//! Integration: the per-tick safety-oracle suite ([`sailing_simulation::checker`]) runs on
//! EVERY `Cluster::tick` and must stay GREEN across the full span of scenarios —
//! proving it has NO false positives on correct runs. (Reaching quiescence in any of these without
//! a panic already proves the per-tick suite passed at every step; these tests ALSO call
//! `check_oracles()` explicitly at the end so a green result is asserted, not merely the absence of
//! a panic.)
//!
//! The "teeth" of the suite — that each oracle DETECTS the bug it guards when fed a violating
//! snapshot — is proven by the unit tests in `checker.rs` (one per oracle, incl. the explicit C1
//! durable-prefix test). This file is the COMPLEMENT: the suite must never fire on a legitimate
//! scenario (a removed node, a mid-snapshot-install follower, a crash+restart, a lossy bus).
#![allow(missing_docs)]
use core::time::Duration;
use sailing_proto::Index;
use sailing_simulation::{Cluster, NetworkFaults};

/// Assert the full oracle suite is green right now (no violation), with a helpful message.
fn assert_green(c: &mut Cluster, ctx: &str) {
  if let Err(v) = c.check_oracles() {
    panic!("oracle suite must be green {ctx}, but tripped: {v}");
  }
}

/// Basic replication: a 3-node cluster commits a batch; the suite stays green throughout and at the
/// end. (The implicit per-tick run is the real assertion; the explicit check is a
/// belt-and-suspenders green.)
#[test]
fn suite_green_across_basic_replication() {
  let mut c = Cluster::new(3);
  assert!(c.run_until(200, |c| c.leader_count() == 1));
  for i in 0..12u32 {
    c.propose(&i.to_le_bytes());
    c.run_until(40, |_| false);
  }
  assert!(c.run_until(300, |c| c.agreement_holds() && c.min_applied_len() >= 12));
  assert_green(&mut c, "after basic replication");
  // The commit watermark genuinely advanced (non-vacuity: the oracles ran against real committed
  // state, not an idle cluster).
  let v = c.view();
  let leader_commit = v.nodes.iter().find(|n| n.is_leader).unwrap().commit;
  assert!(leader_commit >= 12, "leader must have committed the batch");
}

/// Crash + restart (the C1 durable-prefix path): a follower crashes and recovers; the
/// durable-prefix and monotonic-commit oracles must NOT false-positive on the legitimate recovery
/// (commit is persisted, so it does not regress and the recovered commit covers the durable prefix).
#[test]
fn suite_green_across_crash_restart() {
  let mut c = Cluster::new(3);
  assert!(c.run_until(100, |c| c.leader_count() == 1));
  for i in 0..5u32 {
    c.propose(&i.to_le_bytes());
    c.run_until(50, |_| false);
  }
  assert!(c.run_until(100, |c| c.agreement_holds() && c.min_applied_len() >= 5));
  assert_green(&mut c, "before crash");

  let leader = c.leader().unwrap();
  let follower = (0..3u64).find(|&n| n != leader).unwrap();
  let commit_before = c.view().nodes[follower as usize].commit;
  c.crash(follower);
  // The very next tick observes the restarted node; its recovered commit must not have regressed
  // below `commit_before` (monotonic-commit / C1) — if it had, the per-tick suite would already
  // have panicked inside `run_until`.
  for i in 5..10u32 {
    c.propose(&i.to_le_bytes());
    c.run_until(50, |_| false);
  }
  assert!(c.run_until(300, |c| c.agreement_holds() && c.min_applied_len() >= 10));
  assert_green(&mut c, "after crash+restart+reconverge");
  let commit_after = c.view().nodes[follower as usize].commit;
  assert!(
    commit_after >= commit_before,
    "recovered commit {commit_after} must not regress below pre-crash commit {commit_before} (C1)"
  );
}

/// Async fsync-window crash: a follower crashes mid-fsync-window (a staged, un-flushed
/// append is lost). The suite must stay green — the lost tail is re-synced, and the recovered
/// commit covers the durable committed prefix (the C1 durability invariant the oracle enforces).
#[test]
fn suite_green_across_async_fsync_window_crash() {
  let mut c = Cluster::new_async(3, 0xC0FFEE);
  assert!(c.run_until(200, |c| c.leader_count() == 1));
  for i in 0..4u32 {
    c.propose(&i.to_le_bytes());
    c.run_until(80, |_| false);
  }
  assert!(c.run_until(200, |c| c.agreement_holds() && c.min_applied_len() >= 4));

  let leader = c.leader().unwrap();
  let follower = (0..3u64).find(|&n| n != leader).unwrap();
  c.propose(b"in-flight");
  assert!(
    c.open_fsync_window(follower, 50),
    "the follower must be sitting in the fsync window (else the scenario is vacuous)"
  );
  c.crash(follower);
  // Reconverge; the per-tick suite ran every tick of `run_until` and must not have fired.
  for i in 4..8u32 {
    c.propose(&i.to_le_bytes());
    c.run_until(80, |_| false);
  }
  assert!(c.run_until(400, |c| c.agreement_holds() && c.min_applied_len() >= 8));
  assert_green(&mut c, "after async fsync-window crash + reconverge");
}

/// Snapshot + compaction: a low snapshot threshold forces compaction. The
/// commit-is-quorum-durable and append-before-ack oracles account for compacted entries (covered by
/// the snapshot boundary), and boundedness checks the in-memory log shrank — none may false-positive.
#[test]
fn suite_green_across_snapshot_and_compaction() {
  let mut c = Cluster::new_with(3, |cfg| cfg.with_snapshot_threshold(5));
  assert!(c.run_until(200, |c| c.leader_count() == 1));
  for i in 0u32..20 {
    c.propose(&i.to_le_bytes());
    c.run_until(30, |_| false);
  }
  assert!(c.run_until(500, |c| c.agreement_holds() && c.min_applied_len() >= 15));
  // Compaction actually happened (non-vacuity for the snapshot-aware oracle paths).
  for id in 0..3u64 {
    assert!(
      c.first_index_of(id) > Index::new(1),
      "node {id} must have compacted (first_index > 1)"
    );
  }
  let v = c.view();
  assert!(
    v.nodes.iter().any(|n| n.snapshot_last_index > 0),
    "at least one node must have a durable snapshot the oracle can attest against"
  );
  assert_green(&mut c, "after snapshot + compaction");
}

/// Membership change with a removed node: removing a node leaves its applied log frozen while
/// the cluster advances. The agreement / no-committed-rewrite oracles must SKIP the removed node
/// (no false positive on its stale tail), which is exactly what `removed` in the view encodes.
#[test]
fn suite_green_across_remove_node() {
  let mut c = Cluster::new(5);
  assert!(c.run_until(400, |c| c.leader_count() == 1));
  for i in 0..6u32 {
    c.propose(&i.to_le_bytes());
    c.run_until(40, |_| false);
  }
  assert!(c.run_until(400, |c| c.agreement_holds() && c.min_applied_len() >= 6));

  // Remove a non-leader voter, then keep committing so the cluster advances past the removed
  // node's frozen applied log.
  let leader = c.leader().unwrap();
  let victim = (0..5u64).find(|&n| n != leader).unwrap();
  c.remove_node(victim);
  for i in 6..14u32 {
    c.propose(&i.to_le_bytes());
    c.run_until(40, |_| false);
  }
  assert!(c.run_until(600, |c| c.agreement_holds() && c.min_applied_len() >= 12));
  // The removed node is flagged in the view (so the cross-node oracles skip it).
  let v = c.view();
  assert!(
    v.nodes.iter().any(|n| n.id == victim && n.removed),
    "the removed node must be flagged `removed` in the view"
  );
  assert_green(&mut c, "after removing a voter and advancing");
}

/// A freshly added learner lags behind before it catches up: a learner is NOT a voter, so it
/// must not affect the quorum-durability oracle, and while it lags its applied log is a short prefix
/// (agreement's prefix form accepts that). No false positive during catch-up.
#[test]
fn suite_green_across_add_learner_catchup() {
  let mut c = Cluster::new(3);
  assert!(c.run_until(200, |c| c.leader_count() == 1));
  for i in 0..8u32 {
    c.propose(&i.to_le_bytes());
    c.run_until(40, |_| false);
  }
  assert!(c.run_until(300, |c| c.agreement_holds() && c.min_applied_len() >= 8));
  // Add a learner (id 3); it starts empty and must catch up while the suite runs every tick.
  c.add_learner(3);
  for i in 8..14u32 {
    c.propose(&i.to_le_bytes());
    c.run_until(40, |_| false);
  }
  assert!(c.run_until(600, |c| c.agreement_holds() && c.applied_len_of(3) >= 6));
  assert_green(&mut c, "after a learner catches up");
}

/// Lossy/duplicating/reordering bus: a healthy majority still agrees; the per-tick suite
/// must stay green under the adversarial schedule (a dropped/duplicated/reordered message must never
/// produce a committed-history rewrite, a double-commit, or a commit that is not quorum-durable).
#[test]
fn suite_green_under_lossy_network() {
  let mut c = Cluster::new(3);
  c.set_network_faults(
    NetworkFaults {
      latency: Duration::from_millis(5),
      jitter: Duration::from_millis(30),
      drop_per_mille: 150,
      duplicate_per_mille: 100,
      reorder: true,
    },
    0x5EED_5EED,
  );
  assert!(c.run_until(2_000, |c| c.leader_count() >= 1));
  for i in 0..8u32 {
    c.run_until(2_000, |c| c.leader_count() >= 1);
    c.propose(&i.to_le_bytes());
    c.run_until(400, |_| false);
  }
  assert!(c.run_until(6_000, |c| c.agreement_holds() && c.min_applied_len() >= 8));
  // The fault model actually fired (non-vacuity).
  assert!(
    c.net_dropped() > 0 || c.net_duplicated() > 0,
    "the lossy bus must have dropped or duplicated at least one message"
  );
  assert_green(&mut c, "after a lossy/reordering run");
}

/// Conf-change that ADDS then the cluster keeps committing — exercises the quorum-durability oracle
/// across a changing membership (the live-node count, hence the quorum threshold, changes mid-run).
/// No false positive across the membership transition.
#[test]
fn suite_green_across_add_voter() {
  let mut c = Cluster::new(3);
  assert!(c.run_until(200, |c| c.leader_count() == 1));
  for i in 0..5u32 {
    c.propose(&i.to_le_bytes());
    c.run_until(40, |_| false);
  }
  assert!(c.run_until(300, |c| c.agreement_holds() && c.min_applied_len() >= 5));
  // Add voter id 3 via the cluster helper (proposes `AddNode(3)` and wires it in), then drive the
  // change to commit and the new voter to catch up.
  c.add_node(3);
  for i in 5..12u32 {
    c.propose(&i.to_le_bytes());
    c.run_until(40, |_| false);
  }
  assert!(c.run_until(800, |c| c.agreement_holds() && c.applied_len_of(3) >= 5));
  assert_green(&mut c, "after adding a voter and advancing");
}
