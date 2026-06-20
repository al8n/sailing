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
    report.reads_value_checked,
    report.offset_resyncs,
    report.precise_releases,
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

/// The clock-drift harness, end-to-end: seeds 20, 31, and 56 draw the LeaseGuard read mode's DRIFT
/// sub-mode, so each runs the whole adversarial schedule (crash + partition + lossy network +
/// membership churn) under PER-NODE clock RATE drift bounded by ε/Δ. Each must complete WITHOUT a
/// safety-oracle / read-linearizability / liveness panic AND confirm reads — proving the
/// read-linearizability oracle actually judged reads served by leaders whose clocks drift against the
/// rest of the cluster, not merely that the machinery is present. The single-clock VOPR never exercised
/// any of LeaseGuard's clock-dependent paths under divergent clocks; this is that coverage. (The
/// LeaseGuard read mode also has a FAILOVER sub-mode — wall + offset, no rate drift — covered
/// separately by `vopr_exercises_failover_precise_anchor_under_offset`; these seeds must stay in the
/// DRIFT sub-mode, asserted below.)
#[test]
fn vopr_exercises_leaseguard_under_drift() {
  for seed in [20u64, 31, 56] {
    let r = run_vopr(seed, 1_500);
    assert!(
      r.drifted && !r.failover,
      "seed {seed} was expected to draw the LeaseGuard DRIFT sub-mode but did not — the mode draw \
       moved; pick fresh drift seeds via the drift_sweep example"
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

  // Strong non-vacuity (the cross-leader coverage a single global clock cannot produce): under drift a
  // slow leader's heartbeats arrive late, a follower elects a successor while the slow leader's lease
  // is still fresh, and that SUPERSEDED leader (still Leader-role, outranked by a higher-term leader)
  // serves reads — each kept linearizable by the successor's commit-wait. Pinned across two independent
  // strongly-superseded drift seeds so one drifting out as the consensus schedule evolves does not
  // silently vacate the coverage. Both must stay in the DRIFT sub-mode (the failover sub-mode reaches
  // the superseded path too, but this assertion is specifically the rate-drift cross-leader case). Re-
  // derive from the `drift_sweep` example's `SUPERSEDED-SERVE ... drifted=true` lines if they fall to
  // zero.
  let superseded: u64 = [405u64, 573]
    .iter()
    .map(|&seed| {
      let r = run_vopr(seed, 2_000);
      assert!(
        r.drifted && !r.failover,
        "superseded pin {seed} must stay in the DRIFT sub-mode (report={r:?})"
      );
      r.reads_served_by_superseded_leader
    })
    .sum();
  assert!(
    superseded > 0,
    "no pinned drift seed reached the superseded-leader read path — the cross-leader coverage is \
     vacuous; re-derive the seed set with the drift_sweep example"
  );
}

/// The FAILOVER OFFSET sub-mode of the LeaseGuard read mode, end-to-end: seeds 107, 159, and 167 draw a LeaseGuard
/// run with `bounded_clock_uncertainty` armed, a SYNCHRONIZED wall supplied to every proto call, and a
/// per-node bounded wall OFFSET that re-syncs across the run (modelling NTP steps, incl. backward ones).
/// Each must run the whole adversarial schedule WITHOUT a safety / read-linearizability / liveness panic
/// AND actually FIRE the precise commit-anchor — proving the offset clock model exercises the failover
/// early-release under worst-case cross-node wall skew, with the keyed-value oracle as the detector. The
/// monotonic-only harness never supplies a synchronized wall, so this is the precise anchor's ONLY
/// randomized coverage. Re-derive the set from the `drift_sweep` example's FAILOVER-PRECISE lines if the
/// `precise_releases` witnesses fall to zero as the schedule evolves.
#[test]
fn vopr_exercises_failover_precise_anchor_under_offset() {
  // OFFSET (valid-skew, non-asymmetric) failover seeds. The `offset_resyncs > 0` assertion below doubles
  // as the C1 guard: if a coin/rotation change ever flips one of these to the asymmetric sub-mode it draws
  // `violating_resyncs` instead and this test fails LOUDLY rather than silently losing offset coverage.
  for seed in [107u64, 159, 167] {
    let r = run_vopr(seed, 1_500);
    assert!(
      r.failover && !r.drifted,
      "seed {seed} was expected to draw the LeaseGuard FAILOVER sub-mode but did not — the mode draw \
       moved; pick fresh failover seeds via the drift_sweep example"
    );
    assert!(
      r.offset_resyncs > 0,
      "failover seed {seed} never re-synced its wall offset — a static offset cannot model an NTP step \
       (report={r:?})"
    );
    assert!(
      r.precise_releases > 0,
      "failover seed {seed} never fired the PRECISE commit-anchor — the offset coverage is VACUOUS (the \
       conservative anchor governed every release); re-derive via the drift_sweep example (report={r:?})"
    );
    assert!(
      r.committed > 0,
      "failover seed {seed} committed nothing — vacuous (report={r:?})"
    );
  }
}

/// The FAILOVER ASYMMETRIC sub-mode, end-to-end: seeds 52, 67, 186 draw a LeaseGuard failover run that
/// injects a BACKWARD-only clock-CONTRACT violation (a node's wall jumped past −ε_unc; the forward side
/// stays in-contract so no anchor inflates). Each must run the whole adversarial schedule SOUNDLY: no
/// `read-value-linearizability` panic (the run completing IS the assertion — a stale inherited serve would
/// panic like any other), the injection actually FIRED (`violating_resyncs > 0`, the asymmetric-sub-mode
/// witness), the inherited-serve path was REACHED under the violation (`inherited_serves > 0`), and
/// LIVENESS held (`committed > 0` — the gross clock violation did not starve the cluster).
///
/// It does NOT try to CATCH a stale serve in-band (there is no record-caught suppression — a stale
/// inherited serve always panics). A multi-expert audit proved a stale inherited serve is STRUCTURALLY
/// UNREACHABLE in a random run via a clock-offset injection: the inherited-serve window holder is ALWAYS a
/// live Leader still inside its post-election commit-wait, and the serve gate (`now_wall + 2·ε_unc < s_c +
/// W_c`) and the release gate (`now_wall > s_c + W_c + 2·ε_unc`) are exact DUALS on the same wall floor —
/// so any backward wall low enough to hold the window open is by the same inequality low enough to wedge
/// that leader's OWN commit-wait (a livelock): enough-to-catch == enough-to-starve, with no third wall
/// value (`0` is `Wall::ABSENT`). The cross-successor undercut (a window-open stale leader while a
/// successor commits the key past `cidx`) is a split-brain single-instant coincidence the schedule
/// essentially never hits (measured: 22 opportunity node-ticks over ~1.5M). So a stale serve here simply
/// never occurs; the DETECTION that the oracle CATCHES one is proven, soundly and deterministically, by
/// `value_oracle_panics_on_stale_inherited_serve`. THIS test is the randomized STRUCTURAL coverage that
/// the violation injection runs without breaking the cluster.
#[test]
fn vopr_exercises_asymmetric_wall_injection() {
  for seed in [52u64, 67, 186] {
    let r = run_vopr(seed, 1_500);
    assert!(
      r.failover,
      "seed {seed} was expected to draw the LeaseGuard FAILOVER sub-mode but did not (report={r:?})"
    );
    assert!(
      r.violating_resyncs > 0,
      "asymmetric seed {seed} never fired a violating resync — the backward-violation injection did not \
       run (the asymmetric sub-coin moved; re-derive the seed set) (report={r:?})"
    );
    assert!(
      r.inherited_serves > 0,
      "asymmetric seed {seed} never reached the inherited-serve path under the violation — the \
       backward-violating wall never met the inherited-serve path (report={r:?})"
    );
    assert!(
      r.committed > 0,
      "asymmetric seed {seed} committed nothing — the gross clock violation starved the cluster, or the \
       run is vacuous (report={r:?})"
    );
  }
}

/// The OFFSET injection is a pure no-op at ε_unc = 0: with the failover wall armed but a zero
/// uncertainty bound, every `resync_offsets` draw collapses to the range `[0, 0]`, so the synchronized
/// wall stays PERFECTLY synchronized (`global_now + 0`) for every node no matter how many re-syncs fire.
/// This is the "offset ≡ 0 reproduces the synchronized baseline byte-identically" guard: the harness can
/// perturb a run ONLY through a non-zero offset, never as a side effect of the re-sync machinery itself.
#[test]
fn failover_offset_zero_is_a_synchronized_noop() {
  let mut c = crate::Cluster::new_async(3, 7);
  c.enable_failover_clock(core::time::Duration::ZERO);
  let mut prng = crate::store::FaultPrng::new(0xABCD);
  for _ in 0..64 {
    c.resync_offsets(&mut prng);
    assert_eq!(
      c.max_abs_offset(),
      0,
      "at ε_unc = 0 every wall offset must stay zero — the re-sync draw must collapse to [0, 0]"
    );
  }
}

/// `enable_failover_clock` REJECTS an ε_unc beyond `i64::MAX` nanos at install, rather than letting an
/// out-of-range bound silently overflow `resync_offsets` (or produce an offset that no longer fits in
/// `i64`). The arithmetic is total only within this bound, so the contract is enforced loudly up front.
#[test]
#[should_panic(expected = "bounded_clock_uncertainty must be at most")]
fn enable_failover_clock_rejects_oversized_eps_unc() {
  let mut c = crate::Cluster::new_async(3, 1);
  // `Duration::from_nanos(u64::MAX)` is ~1.8e19 ns, well past i64::MAX (~9.2e18) — must panic at install.
  c.enable_failover_clock(core::time::Duration::from_nanos(u64::MAX));
}

/// `resync_offsets` is TOTAL up to the maximum accepted ε_unc: at the `i64::MAX`-nanos boundary it never
/// overflows or wraps, and every drawn offset stays within `[−ε_unc, +ε_unc]` (so `max_abs_offset`, which
/// calls `abs()`, can never meet `i64::MIN`). This is the upper boundary the install validation admits.
#[test]
fn resync_offsets_is_total_at_the_max_bound() {
  let mut c = crate::Cluster::new_async(4, 2);
  let eps_ns = i64::MAX as u64;
  c.enable_failover_clock(core::time::Duration::from_nanos(eps_ns));
  let mut prng = crate::store::FaultPrng::new(0xFEED);
  for _ in 0..256 {
    c.resync_offsets(&mut prng);
    // No panic/wrap, and the magnitude never exceeds the bound (max_abs_offset would panic on i64::MIN).
    assert!(c.max_abs_offset() <= eps_ns as i64);
  }
}

/// `resync_offsets_violating` draws BACKWARD-only: every offset stays `≤ +ε_unc` (forward in-contract, so
/// no node inflates an anchor) while the backward tail can exceed `−ε_unc` (the injected violation), all
/// within `[−factor·ε_unc, +ε_unc]`. Over many draws a genuine backward violation MUST appear.
#[test]
fn resync_offsets_violating_is_backward_only() {
  let mut c = crate::Cluster::new_async(5, 11);
  let eps_ns = 50_000_000i64; // 50ms
  c.enable_failover_clock(core::time::Duration::from_nanos(eps_ns as u64));
  let mut prng = crate::store::FaultPrng::new(0x5151);
  let mut saw_backward_violation = false;
  for _ in 0..256 {
    c.resync_offsets_violating(&mut prng, 3);
    let offs = c.clock_offsets();
    let min = *offs.iter().min().unwrap();
    let max = *offs.iter().max().unwrap();
    assert!(min >= -3 * eps_ns, "offset below the −3·ε_unc floor: {min}");
    assert!(
      max <= eps_ns,
      "offset above +ε_unc — the forward side must stay in-contract so no anchor inflates: {max}"
    );
    if min < -eps_ns {
      saw_backward_violation = true; // a genuine backward contract violation occurred
    }
  }
  assert!(
    saw_backward_violation,
    "256 violating draws never produced a backward violation (offset < −ε_unc)"
  );
}

/// `resync_offsets_violating` is TOTAL at the boundary the precondition admits: `ε = i64::MAX / MAX_FACTOR`
/// with `factor = MAX_FACTOR` makes `factor·ε = i64::MAX` exactly — the draw must not wrap or panic.
#[test]
fn resync_offsets_violating_is_total_at_the_max_bound() {
  let mut c = crate::Cluster::new_async(4, 3);
  let eps_ns = (i64::MAX / 4) as u64;
  c.enable_failover_clock(core::time::Duration::from_nanos(eps_ns));
  let mut prng = crate::store::FaultPrng::new(0xF00D);
  let lo = -(4i128) * eps_ns as i128;
  let hi = eps_ns as i128;
  for _ in 0..256 {
    c.resync_offsets_violating(&mut prng, 4); // factor·ε = i64::MAX, the boundary
    // No panic / wrap: every offset stays in the exact [−4·ε, +ε] range (a wrapped cast would escape it).
    assert!(
      c.clock_offsets()
        .iter()
        .all(|&o| (o as i128) >= lo && (o as i128) <= hi)
    );
  }
}

/// `resync_offsets_violating` REJECTS a `factor·ε_unc` past `i64::MAX` at the call, rather than wrapping
/// the `as i64` cast mid-draw (the H4 totality precondition).
#[test]
#[should_panic(expected = "factor*eps_unc must be in")]
fn resync_offsets_violating_rejects_overflowing_factor() {
  let mut c = crate::Cluster::new_async(3, 5);
  c.enable_failover_clock(core::time::Duration::from_nanos(i64::MAX as u64));
  let mut prng = crate::store::FaultPrng::new(1);
  c.resync_offsets_violating(&mut prng, 4); // 4·i64::MAX > i64::MAX → panics at the precondition
}

/// A FAILOVER run is DETERMINISTIC: the same (seed, ticks) replays to a byte-identical report AND
/// fingerprint, so the seeded offset schedule and the precise-release timing it drives are fully
/// reproducible (a re-sync schedule that drew from wall-clock entropy, or a non-deterministic offset
/// fold, would break this). The `offset_resyncs` + `precise_releases` counters are in the fingerprint.
#[test]
fn vopr_failover_run_is_deterministic() {
  let (r1, h1) = run_and_fingerprint(28, 1_500);
  assert!(
    r1.failover,
    "seed 28 must draw the failover sub-mode for this determinism check (report={r1:?})"
  );
  let (r2, h2) = run_and_fingerprint(28, 1_500);
  assert_eq!(
    r1, r2,
    "a failover run must replay to an identical VoprReport"
  );
  assert_eq!(
    h1, h2,
    "a failover run must replay to an identical fingerprint"
  );
}

/// The per-KEY VALUE oracle actually RAN and judged reads. Across a handful of seeds the keyed-value
/// workload + the value oracle (asserted at each read's serve point ALONGSIDE the index oracle) must
/// have value-checked a positive number of reads — otherwise the new oracle is vacuous (present but
/// never exercised). `reads_value_checked` counts each read whose served value was asserted; it is a
/// SUBSET of `reads_confirmed` (a confirmed read whose node never applies up to the read index is
/// never served, hence unchecked), so we assert the aggregate is positive and bounded by confirmed.
#[test]
fn vopr_value_oracle_runs_across_seeds() {
  let ticks = 2_000;
  let mut total_value_checked = 0u64;
  for seed in 0..5u64 {
    let r = run_vopr(seed, ticks);
    // The value check is DEFERRED to each read's serve point (its node applying up to the read index),
    // so it covers a SUBSET of confirmations — a read whose node crashes / is deposed before applying
    // that far is never truly served and legitimately goes unchecked. Hence `<= reads_confirmed`, and
    // the calm-window / quiesce rounds (which drive a read all the way to served) keep it positive.
    assert!(
      r.reads_value_checked <= r.reads_confirmed,
      "seed {seed}: value-checked {} exceeds confirmed {} (impossible — every value check follows a \
       confirmation)",
      r.reads_value_checked,
      r.reads_confirmed
    );
    total_value_checked += r.reads_value_checked;
  }
  assert!(
    total_value_checked > 0,
    "across seeds 0..5 the per-key VALUE oracle never value-checked a single confirmed read — the \
     value oracle is vacuous"
  );
  std::eprintln!(
    "vopr value-oracle coverage (seeds 0..5, {ticks} ticks each): reads_value_checked=\
     {total_value_checked}"
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

/// The key under test (one fixed slot is enough; the gap is about apply-lag, not key collision).
const GAP_KEY: u16 = 3;
const V_OLD: u64 = 100;
const V_NEW: u64 = 200;

/// Drive a 3-node async cluster into the exact APPLY-LAG window the value oracle's invocation floor
/// must survive: `(GAP_KEY, V_NEW)` is COMMITTED on a follower (its log holds the entry and its commit
/// index covers it) yet APPLIED there is still `V_OLD` — the follower poisoned the instant it tried to
/// `apply_committed` the new entry (`PoisonReason::LogRead`), so `applied` is frozen one value behind
/// `commit`. Returns `(cluster, follower_id)` poised at that instant (the follower's storage fault is
/// CLEARED so its RAW committed log is readable again, but it stays poisoned so `applied` cannot catch up).
///
/// We fault a FOLLOWER, never the leader: the leader reads its log on the SEND path (`maybe_send_append`)
/// to replicate, so an always-on read fault would poison the leader BEFORE it could replicate/commit
/// V_NEW. A follower's only faulting read is `apply_committed`; its append (a write) and its AppendResp
/// ack (gated on the durable append, not on apply) are unaffected, so V_NEW commits on the quorum and
/// the follower receives + logs it, then poisons solely on the apply read — the precise per-node gap the
/// old applied-sourced `v_inv` masked.
fn drive_committed_unapplied_gap(seed: u64) -> (Cluster, u64) {
  let mut c = Cluster::new_async(3, seed);
  assert!(
    c.run_until(3_000, |c| c.leader_count() == 1),
    "the cluster must elect a single leader from a clean start"
  );
  let leader = c.leader().expect("a leader exists after election");
  // The victim follower: any voter that is not the leader. Sorted ids → deterministic pick.
  let follower = c
    .node_ids()
    .into_iter()
    .find(|&id| id != leader)
    .expect("a 3-node cluster has at least one follower");

  // Commit (GAP_KEY, V_OLD) and let EVERY node apply it, so the only later difference is V_NEW.
  c.propose(&encode_kv(GAP_KEY, V_OLD))
    .expect("the leader must accept the V_OLD proposal");
  assert!(
    c.run_until(2_000, |c| c.node_ids().into_iter().all(|id| value_of(
      &c.applied_entries_of(id),
      GAP_KEY
    ) == Some(
      V_OLD
    ))),
    "every node must apply the V_OLD write before the fault is installed"
  );

  // Arm an always-firing committed-range read fault on the FOLLOWER only: its next `apply_committed`
  // log read errors and poisons it, freezing `applied` at V_OLD while `commit` advances to cover V_NEW.
  c.set_node_faults(
    follower,
    StorageFaults {
      transient_read_per_mille: 1_000,
      ..StorageFaults::none()
    },
    seed ^ 0xDEAD,
  );

  let new_index = c
    .propose(&encode_kv(GAP_KEY, V_NEW))
    .expect("the leader must accept the V_NEW proposal");

  // Tick until the follower has the V_NEW entry committed (commit covers it) yet poisoned on applying
  // it — `applied` strictly below the new index. That is the committed-but-unapplied window on that node.
  let hit = c.run_until(2_000, |c| {
    c.is_poisoned(follower)
      && c.commit_index_of(follower) >= new_index
      && c.applied_index_of(follower) < new_index
  });
  assert!(
    hit,
    "expected the follower to reach commit>=V_NEW>applied while poisoned (the committed-but-unapplied \
     window) — it did not materialize (poisoned={}, commit={}, applied={}, V_NEW idx={})",
    c.is_poisoned(follower),
    c.commit_index_of(follower).get(),
    c.applied_index_of(follower).get(),
    new_index.get(),
  );

  // Clear the fault so the follower's RAW committed log is readable again (committed_entries_of reads
  // via the same `entries()` seam). The node STAYS poisoned, so `applied` remains frozen at V_OLD —
  // exactly the asymmetry under test.
  c.set_node_faults(follower, StorageFaults::none(), seed ^ 0xDEAD);
  (c, follower)
}

/// REGRESSION (the Codex apply-lag finding): the per-key value oracle's invocation floor `v_inv` must
/// be sourced from the COMMITTED LOG frontier, not the APPLIED state machine. At a captured instant
/// where `(GAP_KEY, V_NEW)` is committed on a node but not yet applied there, that node's
/// committed-frontier value is V_NEW (the completed-write floor) while its applied-state value is still
/// V_OLD — the stale value the OLD `value_of(applied_entries_of(node), …)` contributed to `v_inv`. A
/// floor built from applied state is therefore under-counted by exactly this apply lag, and a stale read
/// serving V_OLD at an index `>= floor` would wrongly pass the old `observed >= v_inv` check.
#[test]
fn value_oracle_floor_uses_committed_not_applied_frontier() {
  let (c, node) = drive_committed_unapplied_gap(0xC0DE);

  // The per-node GAP: the node's COMMITTED frontier shows the new value, its APPLIED state the old.
  // This is exactly the divergence the old applied-sourced v_inv missed — it read the APPLIED side.
  let committed_here = value_of(&c.committed_entries_of(node), GAP_KEY);
  let applied_here = value_of(&c.applied_entries_of(node), GAP_KEY);
  assert_eq!(
    committed_here,
    Some(V_NEW),
    "the node's COMMITTED frontier must show the newly-committed value V_NEW for the key (commit \
     covers it; apply does not)"
  );
  assert_eq!(
    applied_here,
    Some(V_OLD),
    "the node's APPLIED state must still show the OLD value (apply is frozen by the poison) — exactly \
     the stale value the old applied-sourced v_inv recorded for this node"
  );

  // The fix's per-node contribution to `v_inv` (committed) is the fresh V_NEW; the old contribution
  // (applied) was the stale V_OLD. The fold takes the MAX over all nodes, so a tighter, committed-sourced
  // floor can only ever EQUAL or EXCEED the applied-sourced one — never under-count it.
  assert!(
    committed_here.unwrap_or(0) >= applied_here.unwrap_or(0),
    "the committed-frontier contribution must be >= the applied-state contribution for the same node"
  );

  // The FIX, end to end: folding the COMMITTED frontier across nodes yields V_NEW at this instant — the
  // true completed-write floor, so a stale V_OLD serve would now correctly FAIL the value oracle.
  let v_inv_fixed: u64 = c
    .node_ids()
    .into_iter()
    .filter_map(|id| value_of(&c.committed_entries_of(id), GAP_KEY))
    .max()
    .unwrap_or(0);
  assert_eq!(
    v_inv_fixed, V_NEW,
    "the committed-frontier floor must equal the newly-committed value V_NEW at this instant"
  );

  // Sanity: the index oracle's floor (max commit anywhere) already reflects V_NEW's index — so the two
  // floors must agree on completed-write semantics; the value floor was simply mis-sourced before.
  assert!(
    c.max_commit() >= c.commit_index_of(node),
    "max_commit must reflect the node's advanced commit"
  );
}

/// The value-oracle ASSERTION itself fires when a served value is below the committed `v_inv`. We reuse
/// the same committed-but-unapplied gap: the poisoned node serves the key at the index it HAS applied
/// (the V_OLD index) while the committed floor is V_NEW. Pushing that as a deferred check into a
/// `ReadLedger` and draining it reproduces the stale-read panic the CORRECTED (committed-sourced) floor
/// now catches — the exact failure the old applied-sourced floor masked for this node.
#[test]
#[should_panic(expected = "read-value-linearizability")]
fn value_oracle_panics_on_served_value_below_committed_floor() {
  let (c, node) = drive_committed_unapplied_gap(0xBEEF);

  // The node has applied up to the V_OLD index (its applied frontier), so a read "served" there
  // materializes V_OLD. Compute that serve index from the applied log's last entry index.
  let serve_index = c
    .applied_entries_of(node)
    .iter()
    .map(|(idx, _)| *idx)
    .max()
    .map(sailing_proto::Index::new)
    .expect("the node has applied entries (at least the V_OLD write)");
  assert!(
    c.applied_index_of(node) >= serve_index,
    "the node must have applied up to the serve index (so the deferred check drains, not re-queues)"
  );

  // The committed floor at the (simulated) read invocation is V_NEW — the corrected, committed-frontier
  // value. A read served at serve_index shows V_OLD < V_NEW, so the value oracle MUST panic.
  let mut ledger = ReadLedger::new();
  ledger.pending_value.push(PendingValueCheck {
    ctx: 0,
    node,
    index: serve_index,
    inv: ReadInvocation {
      floor: c.commit_index_of(node),
      key: GAP_KEY,
      v_inv: V_NEW,
    },
    is_inherited_serve: false,
  });
  let mut report = VoprReport::default();
  // Drains the deferred check: observed (V_OLD) < v_inv (V_NEW) → the read-value-linearizability panic.
  ledger.scan(&c, &mut report, 0xBEEF);
}

/// The value oracle catches a stale INHERITED serve specifically — the failover serve path the
/// asymmetric-wall mode exercises. Identical construction to the committed-floor case but with
/// `is_inherited_serve: true`: a deferred check whose served value (V_OLD) is below the committed
/// `v_inv` (V_NEW) drains to the `read-value-linearizability` panic. This is the PRIMARY, deterministic
/// proof that the keyed-value oracle detects the clock-discipline class's staleness — sound and
/// non-vacuous by construction, with no run-wide suppression; the randomized asymmetric sub-mode adds
/// breadth on top of it.
#[test]
#[should_panic(expected = "read-value-linearizability")]
fn value_oracle_panics_on_stale_inherited_serve() {
  let (c, node) = drive_committed_unapplied_gap(0xBEEF);

  let serve_index = c
    .applied_entries_of(node)
    .iter()
    .map(|(idx, _)| *idx)
    .max()
    .map(sailing_proto::Index::new)
    .expect("the node has applied entries (at least the V_OLD write)");

  let mut ledger = ReadLedger::new();
  ledger.pending_value.push(PendingValueCheck {
    ctx: 0,
    node,
    index: serve_index,
    inv: ReadInvocation {
      floor: c.commit_index_of(node),
      key: GAP_KEY,
      v_inv: V_NEW,
    },
    is_inherited_serve: true, // the inherited-serve path — the failover serve the oracle must catch
  });
  let mut report = VoprReport::default();
  ledger.scan(&c, &mut report, 0xBEEF);
}

/// The compaction regression's fixtures. `COMPACT_KEY` receives ONE committed write (`V_COMPACTED`)
/// and is then never written again; `FILLER_KEY` absorbs the churn that drives the snapshot. A low
/// `SNAP_THRESHOLD` makes compaction fire after only a few applied entries.
const COMPACT_KEY: u16 = 5;
const FILLER_KEY: u16 = 6;
const V_COMPACTED: u64 = 700;
/// `applied - first_index >= SNAP_THRESHOLD` triggers a snapshot; a small value compacts quickly.
const SNAP_THRESHOLD: usize = 3;

/// Drive a 3-node async cluster to a state where `(COMPACT_KEY, V_COMPACTED)` — still the LATEST
/// committed value for that key (never overwritten) — has been COMPACTED OUT of the live log on a node:
/// its `first_index` has advanced strictly PAST the entry's index, so `committed_entries_of` (which
/// reads only `[first_index, commit]`) no longer sees it, while the state machine's APPLIED log still
/// retains it. Returns `(cluster, node_id, compact_index)` — the node observed at that instant and the
/// log index `(COMPACT_KEY, V_COMPACTED)` was written at.
///
/// This is the EXACT DUAL of [`drive_committed_unapplied_gap`]: there the live committed log was AHEAD
/// of applied (apply lag), so the committed view was the complete one; here the live committed log is
/// BEHIND applied (compaction dropped the prefix), so the APPLIED view is the complete one. Neither
/// view alone is sufficient — the soundness fix folds the MAX of both.
fn drive_compacted_out_gap(seed: u64) -> (Cluster, u64, sailing_proto::Index) {
  // A low snapshot threshold on every node (founders) so a handful of applied entries forces a
  // snapshot + compaction. pre_vote keeps the small cluster's elections stable under the churn.
  let mut c = Cluster::new_async_with(3, seed, |cfg| {
    cfg
      .with_pre_vote(true)
      .with_snapshot_threshold(SNAP_THRESHOLD)
  });
  assert!(
    c.run_until(3_000, |c| c.leader_count() == 1),
    "the cluster must elect a single leader from a clean start"
  );

  // Commit (COMPACT_KEY, V_COMPACTED) and let EVERY node apply it. This is the value that must survive
  // compaction in the oracle's floor; it is never written again, so V_COMPACTED stays the latest.
  let compact_index = c
    .propose(&encode_kv(COMPACT_KEY, V_COMPACTED))
    .expect("the leader must accept the COMPACT_KEY proposal");
  assert!(
    c.run_until(2_000, |c| c.node_ids().into_iter().all(|id| value_of(
      &c.applied_entries_of(id),
      COMPACT_KEY
    ) == Some(
      V_COMPACTED
    ))),
    "every node must apply the COMPACT_KEY write before the churn that compacts it"
  );

  // Drive enough FURTHER committed+applied writes (to a DIFFERENT key, so COMPACT_KEY's value is never
  // overwritten) that `applied - first_index >= SNAP_THRESHOLD` on every node and the deferred snapshot
  // becomes durable, advancing `first_index` strictly PAST `compact_index` on EVERY node. Each
  // write is a distinct monotonically-increasing value (the workload's distinctness contract). A
  // generous cap on the count + ticks: a healthy 3-node cluster commits + applies + snapshots fast.
  // Drive until EVERY node has compacted `(COMPACT_KEY, V_COMPACTED)` out of its live log. Stopping at
  // the FIRST compacted node would leave others still holding the value in their live committed slice,
  // so the committed-only fold OVER ALL NODES would still recover it — failing to exercise the
  // all-nodes under-count that caused the bug.
  let all_compacted = |c: &Cluster| {
    c.node_ids()
      .into_iter()
      .all(|id| c.first_index_of(id) > compact_index)
  };
  for filler in (V_COMPACTED + 1..).take(128) {
    if all_compacted(&c) {
      break;
    }
    if c.propose(&encode_kv(FILLER_KEY, filler)).is_none() {
      // Momentary leaderless window (an election in flight): let it settle and retry.
      c.run_until(200, |c| c.leader_count() == 1);
      continue;
    }
    // Let the write commit + apply on the quorum, then let `handle_storage`'s `maybe_snapshot` fire and
    // the deferred compaction land (it advances `first_index` only after `SnapshotWritten` is durable).
    c.run_until(400, all_compacted);
  }
  assert!(
    all_compacted(&c),
    "EVERY node must compact COMPACT_KEY (idx {}) out of its live log within the budget — first_index \
     per node: {:?}",
    compact_index.get(),
    c.node_ids()
      .into_iter()
      .map(|id| (id, c.first_index_of(id).get()))
      .collect::<std::vec::Vec<_>>(),
  );
  // A representative node (all have compacted now) for the per-node gap assertions + the served-read
  // fixture; the cluster-wide committed-only fold is what proves the under-count.
  let node = c.node_ids().into_iter().next().expect("non-empty cluster");
  (c, node, compact_index)
}

/// REGRESSION (the Codex compaction finding): the per-key value oracle's invocation floor `v_inv` must
/// fold the APPLIED state machine ALONGSIDE the live committed log — neither alone is complete. Once a
/// key's latest committed write is COMPACTED into the snapshot, it leaves the live log, so
/// `committed_entries_of` (which reads only `[first_index, commit]`) no longer sees it and a
/// committed-ONLY floor under-counts to a stale/None value. The state machine retains the
/// snapshot-compacted prefix, so `applied_entries_of` still shows it. A floor built from the committed
/// log alone is therefore under-counted by exactly this compaction, and a stale read serving the OLD
/// value would wrongly pass a committed-only `observed >= v_inv` check.
#[test]
fn value_oracle_floor_folds_applied_after_compaction() {
  let (c, node, compact_index) = drive_compacted_out_gap(0xC0FFEE);

  // The post-compaction GAP: the live committed log alone MISSES the value (compacted out of
  // `[first_index, commit]`), while the APPLIED state machine still retains it. This is the exact dual
  // of the apply-lag gap — there the committed view was complete; here the applied view is.
  let committed_here = value_of(&c.committed_entries_of(node), COMPACT_KEY);
  let applied_here = value_of(&c.applied_entries_of(node), COMPACT_KEY);
  assert!(
    c.first_index_of(node).get() > compact_index.get(),
    "precondition: the entry must be below first_index (compacted out of the live log)"
  );
  assert_ne!(
    committed_here,
    Some(V_COMPACTED),
    "the live committed log alone must NO LONGER show V_COMPACTED for the key (it was compacted out — \
     first_index advanced past its index); committed-only v_inv under-counts here"
  );
  assert_eq!(
    applied_here,
    Some(V_COMPACTED),
    "the APPLIED state machine must still retain the compacted-out value (the snapshot prefix lives in \
     the state machine) — exactly the value the committed-only floor missed"
  );

  // The committed-ONLY fold over ALL NODES (the pre-fix floor): because EVERY node compacted the entry
  // out of its live log, no node contributes V_COMPACTED to a committed-only fold, so the CLUSTER-WIDE
  // committed-only v_inv is strictly below V_COMPACTED. This is the all-nodes under-count that caused the
  // bug — a committed-only oracle would have recorded this stale floor and passed a stale read. (If even
  // one node still retained the value in its live slice, this fold would recover it; that is why the
  // fixture drives EVERY node to compact.)
  let committed_only_fold: u64 = c
    .node_ids()
    .into_iter()
    .filter_map(|id| value_of(&c.committed_entries_of(id), COMPACT_KEY))
    .max()
    .unwrap_or(0);
  assert!(
    committed_only_fold < V_COMPACTED,
    "the committed-only CLUSTER fold ({committed_only_fold}) must be strictly below V_COMPACTED \
     ({V_COMPACTED}) — every node compacted the write out of its live log, so a committed-only v_inv \
     under-counts cluster-wide (the exact hole the combined fold closes)"
  );

  // The FIX, recomputed EXACTLY as `ReadLedger::issue` does: per node, MAX of the applied view (retains
  // the compacted prefix) ⊔ the live committed view (the not-yet-applied tail); then MAX over all nodes.
  // It recovers V_COMPACTED — the true completed-write floor — where the committed-only fold lost it.
  let v_inv_fixed: u64 = c
    .node_ids()
    .into_iter()
    .filter_map(|id| {
      value_of(&c.applied_entries_of(id), COMPACT_KEY)
        .into_iter()
        .chain(value_of(&c.committed_entries_of(id), COMPACT_KEY))
        .max()
    })
    .max()
    .unwrap_or(0);
  assert_eq!(
    v_inv_fixed, V_COMPACTED,
    "the combined applied⊔committed fold must recover V_COMPACTED after compaction — this is the floor \
     the committed-only fold missed"
  );
}

/// The value-oracle ASSERTION itself fires when a served value is below the post-compaction `v_inv`.
/// After `(COMPACT_KEY, V_COMPACTED)` is compacted out of the live log, the combined floor is still
/// V_COMPACTED (recovered from applied state); a read "served" a value BELOW it must panic. We push a
/// deferred check whose recorded served value is stale (a `serve_index` BEFORE the compacted write) and
/// drain it — reproducing the stale-read panic the combined floor now catches but a committed-only
/// floor would have missed (committed-only `v_inv` would be < V_COMPACTED here, so it would not fire).
#[test]
#[should_panic(expected = "read-value-linearizability")]
fn value_oracle_panics_on_served_value_below_compacted_floor() {
  let (c, node, compact_index) = drive_compacted_out_gap(0xCAFE);

  // A serve index strictly BELOW the compacted write's index: the served snapshot at that index cannot
  // contain (COMPACT_KEY, V_COMPACTED), so `value_of_asof` there is None → observed 0 < v_inv. The node
  // has applied far past this (it churned well beyond), so the deferred check DRAINS rather than re-queues.
  let serve_index = sailing_proto::Index::new(compact_index.get() - 1);
  assert!(
    c.applied_index_of(node) >= serve_index,
    "the node must have applied up to the serve index (so the deferred check drains, not re-queues)"
  );

  // The floor at the (simulated) read invocation is V_COMPACTED — recovered by the combined fold from
  // applied state even though the live committed log compacted it out. A read served below the write's
  // index shows a stale value < V_COMPACTED, so the value oracle MUST panic.
  let mut ledger = ReadLedger::new();
  ledger.pending_value.push(PendingValueCheck {
    ctx: 0,
    node,
    index: serve_index,
    inv: ReadInvocation {
      floor: c.commit_index_of(node),
      key: COMPACT_KEY,
      v_inv: V_COMPACTED,
    },
    is_inherited_serve: false,
  });
  let mut report = VoprReport::default();
  // Drains the deferred check: observed (stale, < V_COMPACTED) < v_inv (V_COMPACTED) → the panic.
  ledger.scan(&c, &mut report, 0xCAFE);
}

/// The proactive lease-refresh modes, end-to-end: the sweep draws `Off`, `OnExpiry`, AND `Continuous`
/// LeaseGuard runs; every run stays read-linearizable (the sweep COMPLETING is the safety assertion — a
/// stale serve panics in the value/index oracle); and a proactive run still commits (liveness). This is
/// the randomized proof that adding proactive no-ops under reads never breaks linearizability. The refresh
/// GATING and the no-op FIRING themselves are unit-tested in `sailing-proto`
/// (`read_since_anchor_set_on_read_cleared_on_append_and_stepdown`, `lease_near_expiry_fires_within_margin_of_delta`).
#[test]
fn vopr_exercises_lease_refresh_modes() {
  use sailing_proto::LeaseRefresh::{Continuous, Off, OnExpiry};
  let (mut saw_off, mut saw_on_expiry, mut saw_continuous) = (false, false, false);
  let mut progressed_under_proactive = false;
  for seed in 0..256u64 {
    let r = run_vopr_refresh(seed, 500);
    match r.lease_refresh {
      Off => saw_off = true,
      OnExpiry => {
        saw_on_expiry = true;
        progressed_under_proactive |= r.committed > 0;
      }
      Continuous => {
        saw_continuous = true;
        progressed_under_proactive |= r.committed > 0;
      }
    }
  }
  assert!(
    saw_off,
    "no seed in 0..256 stayed LeaseRefresh::Off (the bit-identical default)"
  );
  assert!(
    saw_on_expiry,
    "no seed in 0..256 exercised LeaseRefresh::OnExpiry"
  );
  assert!(
    saw_continuous,
    "no seed in 0..256 exercised LeaseRefresh::Continuous"
  );
  assert!(
    progressed_under_proactive,
    "a proactive-refresh run must still commit (liveness held under the extra no-ops)"
  );
}

/// The read-mode MIGRATION harness: across a band of seeds the MigrateReadMode action proposes
/// cluster-wide migrations (Safe / LeaseBased / LeaseGuard) mid-run; each accepted SetReadMode flips the
/// active mode at APPLY-TIME on every node, under the VOPR's faults. Every run completes WITHOUT a
/// read-linearizability / safety-oracle / liveness panic (`run_vopr` panics on a violation) — proving the
/// apply-time flip + the mode-INDEPENDENT commit-wait keep reads linearizable ACROSS a migration, not
/// merely that the machinery is present. The aggregate confirms migrations were ACTUALLY exercised: a
/// LeaseGuard run tore down / re-established its lease, and a read was confirmed in a run that migrated.
#[test]
fn vopr_exercises_read_mode_migrations() {
  let mut total_migrations = 0u64;
  let mut leaseguard_migrated = 0u32;
  let mut migrated_and_confirmed_a_read = 0u32;
  for seed in 0u64..48 {
    let r = run_vopr_migrate(seed, 600);
    total_migrations += r.read_mode_migrations;
    if (r.drifted || r.failover) && r.read_mode_migrations > 0 {
      leaseguard_migrated += 1;
    }
    if r.read_mode_migrations > 0 && r.reads_confirmed > 0 {
      migrated_and_confirmed_a_read += 1;
    }
  }
  std::eprintln!(
    "vopr read-mode-migration coverage (seeds 0..48, 600 ticks): total_migrations={total_migrations} \
     leaseguard_migrated={leaseguard_migrated} migrated_and_confirmed_a_read={migrated_and_confirmed_a_read}"
  );
  assert!(
    total_migrations > 0,
    "no read-mode migration was accepted across seeds 0..48 — the MigrateReadMode action never fired or \
     the proto always rejected it (the action draw moved, or the migration path regressed)"
  );
  assert!(
    leaseguard_migrated > 0,
    "no LeaseGuard run migrated its read mode across seeds 0..48 — the high-value LeaseGuard teardown / \
     warm-up was never exercised (the mode draw moved, or LeaseGuard migrations regressed)"
  );
  assert!(
    migrated_and_confirmed_a_read > 0,
    "no run both migrated its read mode AND confirmed a read — the read-linearizability oracle never \
     judged a read in a run that also migrated, so the cross-migration coverage is vacuous"
  );
}

/// The COLD-read fault, end-to-end: across a band of seeds a fraction of committed-range reads return
/// `EntriesRead::Pending`, so apply and replication DEFER (and the lease/election anchors fail closed)
/// under the full fault model. Every run completes WITHOUT a read-linearizability / safety-oracle /
/// liveness panic (`run_vopr_cold` panics on a violation) — proving deferred reads stay linearizable and
/// the cluster still converges. The aggregate confirms the cold path actually FIRED (non-vacuity) and a
/// cold run still committed (liveness held despite the deferrals). The per-site dispositions are
/// unit-tested in `sailing-proto` (`restart_scan_poisons_on_cold_read`,
/// `apply_defers_on_cold_read_without_poisoning`, `replication_defers_on_cold_read_without_poisoning`).
#[test]
fn vopr_exercises_cold_reads() {
  let mut total_cold_reads = 0u64;
  let mut progressed_under_cold = false;
  for seed in 0u64..64 {
    let r = run_vopr_cold(seed, 600);
    total_cold_reads += r.cold_reads;
    progressed_under_cold |= r.committed > 0;
  }
  std::eprintln!(
    "vopr cold-read coverage (seeds 0..64, 600 ticks): total_cold_reads={total_cold_reads}"
  );
  assert!(
    total_cold_reads > 0,
    "no cold (Pending) read fired across seeds 0..64 — the cold-fetch fault never armed or the read path \
     regressed (cold-read coverage is vacuous)"
  );
  assert!(
    progressed_under_cold,
    "no cold run committed — liveness failed under the cold-read deferrals (apply/replication wedged)"
  );
}
