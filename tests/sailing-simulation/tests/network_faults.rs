//! Integration: the seeded network fault model on the typed-message bus — per-message
//! latency/jitter/drop/duplicate/reorder. With a lossy, duplicating, reordering bus a healthy
//! majority must STILL reach agreement and make progress (commit the batch), and the run must be
//! deterministic (same seed ⇒ identical applied logs).
//!
//! This is exactly the scenario the conflict-conditional-truncation fix and the deferred-
//! completion model protect against: a duplicate AppendEntries must not double-append, a reordered/
//! duplicated VoteResponse must not double-vote, and a dropped append must be re-replicated. The
//! structural oracles (append-before-ack, one-grant-per-term) stay ENABLED in `Cluster::tick` and
//! run on every SENT message — so if a reorder/dup tripped one, this test would PANIC with the
//! seed, surfacing a real proto bug rather than a loosened oracle.
#![allow(missing_docs)]
use core::time::Duration;
use sailing_simulation::{AppliedLog, Cluster, NetworkFaults};

/// The outcome of a lossy run: per-node applied logs, whether agreement was reached on the batch,
/// and the network fault tallies `(dropped, duplicated)` so a test can assert non-vacuity.
struct LossyRun {
  logs: Vec<AppliedLog>,
  agreed: bool,
  dropped: u64,
  duplicated: u64,
}

/// Run a 3-node cluster under a lossy+duplicating+reordering bus and return its [`LossyRun`] once it
/// has reached agreement on at least `batch` entries (or run out of `steps`). Factored so the
/// determinism test can replay the exact same run from the same seed.
fn run_lossy(seed: u64, batch: u32, steps: usize) -> LossyRun {
  let mut c = Cluster::new(3);
  // Lossy (15% drop) + duplicating (10%) + jittered (30ms on a 100ms heartbeat, 1000ms election —
  // bounded so heartbeats still beat the election timeout ⇒ liveness is achievable) + reordering.
  c.set_network_faults(
    NetworkFaults {
      latency: Duration::from_millis(5),
      jitter: Duration::from_millis(30),
      drop_per_mille: 150,
      duplicate_per_mille: 100,
      reorder: true,
    },
    seed,
  );

  // Elect a leader despite the faults.
  assert!(
    c.run_until(2_000, |c| c.leader_count() >= 1),
    "seed {seed}: a leader must emerge under the lossy/reordering bus"
  );

  // Propose a batch on the leader; re-target the leader each time in case a transient reorder storm
  // forced a re-election between proposals (propose() no-ops when there is momentarily no leader).
  let mut proposed = 0u32;
  for i in 0..batch {
    // Make sure there is a leader to accept the proposal; give it room to settle.
    c.run_until(2_000, |c| c.leader_count() >= 1);
    if c.propose(&i.to_le_bytes()).is_some() {
      proposed += 1;
    }
    // Let it replicate/commit between proposals (drops slow this down).
    c.run_until(400, |_| false);
  }
  assert!(
    proposed >= batch,
    "seed {seed}: every proposal must land on some leader (proposed {proposed}/{batch})"
  );

  // Run to quiescence: agreement on the full batch. Generous budget — drops slow progress.
  let agreed = c.run_until(steps, |c| {
    c.agreement_holds() && c.min_applied_len() >= batch as usize
  });

  let logs = (0..3u64)
    .map(|n| c.applied_entries_of(n))
    .collect::<Vec<_>>();
  LossyRun {
    logs,
    agreed,
    dropped: c.net_dropped(),
    duplicated: c.net_duplicated(),
  }
}

#[test]
fn agreement_under_drops_dups_reorder() {
  // A healthy majority + bounded loss ⇒ the cluster commits the batch DESPITE drops/dups/reorder.
  let seed = 0xA11CE;
  let batch = 12u32;
  let run = run_lossy(seed, batch, 8_000);
  assert!(
    run.agreed,
    "seed {seed:#x}: cluster must reach agreement on the full batch under the fault model"
  );
  // Non-vacuity: the faults must have actually fired (else this is "agreement on a faultless bus").
  assert!(
    run.dropped > 0 && run.duplicated > 0,
    "seed {seed:#x}: the fault model must have dropped AND duplicated messages \
     (dropped={}, duplicated={})",
    run.dropped,
    run.duplicated
  );
  let logs = run.logs;
  // Agreement: every node's applied prefix matches (the longest is the reference).
  let longest = logs.iter().map(|l| l.len()).max().unwrap();
  assert!(
    longest >= batch as usize,
    "seed {seed:#x}: the committed history must include the whole batch (got {longest})"
  );
  for k in 0..longest {
    let mut reference: Option<&(u64, Vec<u8>)> = None;
    for l in &logs {
      if let Some(cell) = l.get(k) {
        match reference {
          None => reference = Some(cell),
          Some(r) => assert_eq!(
            r, cell,
            "seed {seed:#x}: applied-log divergence at position {k} — agreement violated under faults"
          ),
        }
      }
    }
  }
}

#[test]
fn network_fault_model_is_deterministic() {
  // The same seed ⇒ byte-identical applied logs across two independent runs. Proves the model is
  // driven solely by the seeded network PRNG (no wall-clock / `rand`).
  let seed = 0xD37E;
  let a = run_lossy(seed, 8, 8_000);
  let b = run_lossy(seed, 8, 8_000);
  assert_eq!(
    a.agreed, b.agreed,
    "determinism: agreement outcome must match"
  );
  assert_eq!(
    a.logs, b.logs,
    "determinism: same seed must yield identical applied logs across runs"
  );
  // The fault tallies must also match bit-for-bit — proves the SAME schedule replayed (the model is
  // driven solely by the seeded network PRNG, no wall-clock / `rand`).
  assert_eq!(
    (a.dropped, a.duplicated),
    (b.dropped, b.duplicated),
    "determinism: same seed must replay the identical drop/dup schedule"
  );
  // And a DIFFERENT seed should (almost surely) drive a different fault schedule — sanity that the
  // seed actually threads through (not a no-op). We only require both runs are independently valid;
  // the schedules being identical for two distinct seeds would be astronomically unlikely.
  let c = run_lossy(0xBEEF, 8, 8_000);
  assert!(a.logs.iter().map(|l| l.len()).max().unwrap() >= 8);
  assert!(c.logs.iter().map(|l| l.len()).max().unwrap() >= 8);
}

/// Reorder OFF ⇒ deliveries between each (from,to) pair stay FIFO even with jitter. The cluster
/// must still agree (FIFO is the easier case), and — paired with the reorder-on test above — this
/// exercises both branches of the `reorder` knob.
#[test]
fn agreement_with_fifo_jitter_no_reorder() {
  let mut c = Cluster::new(3);
  c.set_network_faults(
    NetworkFaults {
      latency: Duration::from_millis(2),
      jitter: Duration::from_millis(20),
      drop_per_mille: 120,
      duplicate_per_mille: 80,
      reorder: false, // FIFO clamp active
    },
    0x5EED_F1F0,
  );
  assert!(
    c.run_until(2_000, |c| c.leader_count() >= 1),
    "a leader must emerge under FIFO jitter + drops"
  );
  for i in 0..10u32 {
    c.run_until(1_000, |c| c.leader_count() >= 1);
    c.propose(&i.to_le_bytes());
    c.run_until(300, |_| false);
  }
  assert!(
    c.run_until(6_000, |c| c.agreement_holds() && c.min_applied_len() >= 10),
    "FIFO-jittered lossy cluster must still agree on the full batch"
  );
}

/// Non-vacuity: the fault model must actually FIRE (drops + duplicates) over a run, otherwise the
/// agreement tests above would be vacuously passing on a faultless bus. We assert via the cluster's
/// fault counters that both a drop and a duplicate were injected.
#[test]
fn fault_model_actually_fires() {
  let mut c = Cluster::new(3);
  c.set_network_faults(
    NetworkFaults {
      latency: Duration::ZERO,
      jitter: Duration::from_millis(15),
      drop_per_mille: 200,
      duplicate_per_mille: 200,
      reorder: true,
    },
    0xF1FE,
  );
  assert!(c.run_until(2_000, |c| c.leader_count() >= 1));
  for i in 0..10u32 {
    c.run_until(800, |c| c.leader_count() >= 1);
    c.propose(&i.to_le_bytes());
    c.run_until(300, |_| false);
  }
  c.run_until(4_000, |c| c.agreement_holds() && c.min_applied_len() >= 10);
  assert!(
    c.net_dropped() > 0,
    "the drop fault must have fired at least once (else the agreement tests are vacuous)"
  );
  assert!(
    c.net_duplicated() > 0,
    "the duplicate fault must have fired at least once"
  );
  // And agreement still held throughout (the per-tick oracles in `tick` did not trip).
  assert!(
    c.agreement_holds(),
    "agreement holds under a firing fault model"
  );
}
