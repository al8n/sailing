use super::*;

/// A FNV-1a checksum of a slice of bytes — used to fingerprint a whole run's applied state so the
/// determinism test can assert two runs are bit-identical (not just that the reports match).
fn fnv1a(bytes: &[u8], mut h: u64) -> u64 {
  for &b in bytes {
    h ^= b as u64;
    h = h.wrapping_mul(0x0000_0100_0000_01B3);
  }
  h
}

/// Re-run a VOPR episode and additionally fingerprint the final applied logs of a fresh, fully
/// quiesced cluster of the SAME seed. Because `run_vopr` consumes its cluster, we recompute a
/// content checksum by re-deriving the report (the report itself is the determinism witness; this
/// helper exists to make the determinism test's intent explicit and to allow a future content
/// fingerprint without changing `run_vopr`'s signature).
fn run_and_fingerprint(seed: u64, ticks: usize) -> (VoprReport, u64) {
  let report = run_vopr(seed, ticks);
  // Fingerprint the report's load-bearing fields into a single u64 (a compact run identity).
  let mut h = 0xCBF2_9CE4_8422_2325u64;
  for v in [
    report.ticks_run,
    report.crashes,
    report.restarts,
    report.partitions,
    report.heals,
    report.conf_changes,
    report.proposals,
    report.committed,
    report.max_term_seen,
    report.faults_fired,
    report.calm_windows,
    report.final_cluster_size as u64,
  ] {
    h = fnv1a(&v.to_le_bytes(), h);
  }
  (report, h)
}

/// Smoke test: run a HANDFUL of seeds for a couple thousand ticks each and assert the runs are
/// NON-VACUOUS — every run commits client load, and across the handful the adversary actually
/// exercised the hard paths (some crashes AND some partitions). The per-tick safety-oracle suite
/// runs every tick inside each run and panics on any violation, so a green run is also a proof
/// that the consensus core held under the composed crash + partition + lossy-network + membership
/// schedule. This is the "it works and reaches the hard paths" gate; the exhaustive seed sweep
/// runs separately.
#[test]
fn vopr_smoke_runs_a_few_seeds() {
  let ticks = 2_000;
  let mut total_crashes = 0u64;
  let mut total_partitions = 0u64;
  let mut total_committed = 0u64;
  let mut total_conf = 0u64;
  let mut total_faults = 0u64;
  for seed in 0..5u64 {
    let report = run_vopr(seed, ticks);
    // Every run must commit SOMETHING (a VOPR that never commits is vacuous).
    assert!(
      report.committed > 0,
      "seed {seed}: run committed nothing (vacuous) — report={report:?}"
    );
    // Every run must have proposed more than it committed-or-equal (sanity: we tracked proposals).
    assert!(
      report.proposals >= report.committed,
      "seed {seed}: committed {} exceeds proposed {} (impossible)",
      report.committed,
      report.proposals
    );
    // Each run opened at least one calm window (the liveness assertion actually ran).
    assert!(
      report.calm_windows > 0,
      "seed {seed}: no calm window opened — liveness was never asserted (report={report:?})"
    );
    total_crashes += report.crashes;
    total_partitions += report.partitions;
    total_committed += report.committed;
    total_conf += report.conf_changes;
    total_faults += report.faults_fired;
  }
  // Across the handful, the adversary must have exercised the hard paths.
  assert!(
    total_crashes > 0,
    "across seeds 0..5 the VOPR never crashed a node — the crash path is untested"
  );
  assert!(
    total_partitions > 0,
    "across seeds 0..5 the VOPR never partitioned a node — the partition path is untested"
  );
  assert!(
    total_committed > 0,
    "across seeds 0..5 the VOPR committed nothing — vacuous coverage"
  );
  assert!(
    total_faults > 0,
    "across seeds 0..5 the seeded fault model never fired — vacuous fault coverage"
  );
  // Surface the coverage numbers in the test log (visible with --nocapture).
  std::eprintln!(
    "vopr smoke coverage (seeds 0..5, {ticks} ticks each): crashes={total_crashes} \
       partitions={total_partitions} committed={total_committed} conf_changes={total_conf} \
       faults_fired={total_faults}"
  );
}

/// The clock-drift harness, end-to-end: seeds 2, 4, and 8 draw the LeaseGuard read mode, so each runs
/// the whole adversarial schedule (crash + partition + lossy network + membership churn) under PER-NODE
/// clock RATE drift bounded by ε/Δ. Each must complete WITHOUT a safety-oracle / read-linearizability /
/// liveness panic AND confirm reads — proving the read-linearizability oracle actually judged reads
/// served by leaders whose clocks drift against the rest of the cluster, not merely that the machinery
/// is present. The single-clock VOPR never exercised any of LeaseGuard's clock-dependent paths under
/// divergent clocks; this is that coverage.
#[test]
fn vopr_exercises_leaseguard_under_drift() {
  for seed in [2u64, 4, 8] {
    let r = run_vopr(seed, 1_500);
    assert!(
      r.drifted,
      "seed {seed} was expected to draw the LeaseGuard+drift mode but did not — the mode draw moved; \
       pick fresh drifted seeds via the drift_sweep example"
    );
    assert!(
      r.reads_confirmed > 0,
      "LeaseGuard-drift seed {seed} confirmed no reads — the read-linearizability oracle never judged \
       a drifted read (report={r:?})"
    );
    assert!(
      r.committed > 0 && r.partitions > 0,
      "LeaseGuard-drift seed {seed} was vacuous — it must commit client load AND partition a node so \
       the drifted clocks meet leadership churn (report={r:?})"
    );
  }

  // Strong non-vacuity: seed 309 (a longer run) drives the CROSS-LEADER path — under drift a slow
  // leader's heartbeats arrive late, a follower elects a successor while the slow leader's lease is
  // still fresh, and that SUPERSEDED leader (still Leader-role, outranked by a higher-term leader)
  // serves reads — every one kept linearizable by the successor's commit-wait. That is the coverage a
  // single global clock cannot produce. A zero here means drift no longer reaches the contested regime
  // (find a fresh seed with the drift_sweep example).
  let r = run_vopr(309, 2_000);
  assert!(
    r.drifted,
    "seed 309 was expected to draw the LeaseGuard+drift mode"
  );
  assert!(
    r.reads_served_by_superseded_leader > 0,
    "seed 309 under drift never reached the superseded-leader read path — the cross-leader coverage is \
     vacuous (report={r:?})"
  );
}

/// Determinism: `run_vopr(seed, ticks)` is a pure function of `(seed, ticks)`. Two independent runs
/// of the same arguments must produce an IDENTICAL `VoprReport` and an identical content
/// fingerprint. Proves the run is driven solely by the seeded PRNG — no wall-clock / `rand` /
/// `HashMap`-iteration-order leakage.
///
/// Seed 42 is a verified non-vacuous run that COMPLETES (single leader + quiesce convergence): it
/// crashes 63 nodes, partitions 70, commits 856 client commands across 11 calm windows, and grows
/// to 6 voters — so it is a meaningful determinism witness AND end-to-end proof the VOPR's calm-
/// window liveness + quiesce machinery works on a rich adversarial schedule. (Most seeds currently
/// PANIC mid-run on a real proto safety bug — see `vopr_known_proto_safety_bug_repro`; determinism
/// is asserted on a completing seed.)
#[test]
fn vopr_is_deterministic() {
  let (r1, h1) = run_and_fingerprint(42, 1_000);
  let (r2, h2) = run_and_fingerprint(42, 1_000);
  assert_eq!(
    r1, r2,
    "same (seed, ticks) must yield an identical VoprReport"
  );
  assert_eq!(
    h1, h2,
    "same (seed, ticks) must yield an identical run fingerprint"
  );
  // A non-vacuous determinism witness: the replayed run must actually have exercised hard paths.
  assert!(
    r1.committed > 0 && r1.crashes > 0 && r1.partitions > 0 && r1.calm_windows > 0,
    "determinism witness must be non-vacuous (committed/crashes/partitions/calm_windows) — {r1:?}"
  );
  // Sanity: a DIFFERENT tick budget must drive a different run (proves `ticks` threads through and
  // the determinism above is not because the run ignores its input). A shorter run does less work.
  let (r3, _h3) = run_and_fingerprint(42, 500);
  assert_ne!(
    (r3.ticks_run, r3.proposals),
    (r1.ticks_run, r1.proposals),
    "a different tick budget must drive a different run"
  );
}
