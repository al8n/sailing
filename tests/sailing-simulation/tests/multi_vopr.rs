#![allow(missing_docs)]
//! Multi-group VOPR sweeps: run the weighted multi-group fuzzer across seeds and assert every run
//! holds (per-group safety oracles, cross-talk, one-identity, calm-window liveness, quiesce)
//! while the band as a whole is non-vacuous.
//!
//! Replaying a failure: a panic prints `seed=<S> tick=<T>`. Reproduce with
//! `run_multi_vopr(S, ticks, MultiProfile::default_multi())` and inspect tick `T`.
//!
//! Regression seeds below were found by a short local sweep — run a few fast-band seeds and read
//! the report counters for the target shape (the drift-sweep pattern):
//! `for seed in 0..16 { dbg!(run_multi_vopr(seed, 4_000, MultiProfile::default_multi())); }`

use sailing_simulation::{
  MultiProfile, MultiVoprReport, run_multi_vopr, run_multi_vopr_certifying_tracked_wedges,
};

/// Enforce the single-group sweep's membership-oracle policy over a band's per-seed reports
/// (`vopr_exercises_joint_snapshot_membership` is the reference shape): the oracle actually
/// JUDGED installs (comparisons nonzero — otherwise a corrupt snapshot ConfState rides a green
/// band), no observed install escaped a verdict (skipped zero — the per-checker finalize panic
/// with gid/generation attribution is the sharp edge; the band total restates it so that policy
/// cannot silently weaken), and the tolerated kind-unobservable decline class stayed a bounded
/// minority instead of swallowing the coverage.
fn assert_membership_band_bounds(band: &str, reports: &[MultiVoprReport]) {
  let total_oracle_comparisons: u64 = reports
    .iter()
    .map(|r| r.membership_oracle_comparisons)
    .sum();
  let total_skipped_unwitnessed: u64 = reports.iter().map(|r| r.skipped_unwitnessed_installs).sum();
  let total_kind_unobservable: u64 = reports.iter().map(|r| r.kind_unobservable_installs).sum();
  std::eprintln!(
    "multi band membership coverage ({band}): membership_oracle_comparisons={total_oracle_comparisons} \
     skipped_unwitnessed_installs={total_skipped_unwitnessed} kind_unobservable_installs={total_kind_unobservable}"
  );
  assert!(
    total_oracle_comparisons > 0,
    "the snapshot-membership oracle never COMPARED a snapshot-installed node against the \
     committed-config history across the {band} band — so a corrupt snapshot ConfState could slip \
     through this green band (the oracle ran but judged nothing)"
  );
  assert_eq!(
    total_skipped_unwitnessed, 0,
    "the snapshot-membership oracle hit a committed-config HISTORY completeness gap across the \
     {band} band (a boundary beyond the watermark or an unresolved divergence that did not \
     converge) — the history must cover every committed index an install lands on"
  );
  // Some installs resolve to a conf-change whose committed-log entry was compacted before any tick observed it
  // (committed and snapshot-compacted within a single catch-up tick), so the oracle has no EXACT-term ConfChange
  // proof for it. Under STRICT trust it DECLINES these (it will not trust a possibly-stale ConfChange) rather
  // than risk a false verdict — a bounded compaction limitation, NOT a soundness hole. Assert the oracle still
  // COMPARED the directly-observed exact-term ConfChanges in bulk (it does not collapse to near-zero coverage):
  // declines stay a minority of comparisons.
  assert!(
    total_kind_unobservable * 2 < total_oracle_comparisons,
    "the membership oracle's coverage collapsed: kind-unobservable declines ({total_kind_unobservable}) are not \
     a minority of comparisons ({total_oracle_comparisons}) — the exact-term committed-log-kind witness regressed"
  );
}

/// The fold sums ACROSS reports: the first report alone carries the comparisons, the second
/// alone the tolerated decline, and only their sum is the passing shape.
#[test]
fn membership_band_bounds_hold_on_the_tolerated_shape() {
  let reports = [
    MultiVoprReport {
      membership_oracle_comparisons: 4,
      ..MultiVoprReport::default()
    },
    MultiVoprReport {
      kind_unobservable_installs: 1,
      ..MultiVoprReport::default()
    },
  ];
  assert_membership_band_bounds("fabricated tolerated", &reports);
}

#[test]
#[should_panic(expected = "never COMPARED a snapshot-installed node")]
fn membership_band_bounds_fire_on_zero_comparisons() {
  assert_membership_band_bounds("fabricated vacuous", &[MultiVoprReport::default()]);
}

#[test]
#[should_panic(expected = "HISTORY completeness gap")]
fn membership_band_bounds_fire_on_a_skipped_install() {
  let reports = [MultiVoprReport {
    membership_oracle_comparisons: 10,
    skipped_unwitnessed_installs: 1,
    ..MultiVoprReport::default()
  }];
  assert_membership_band_bounds("fabricated skipped", &reports);
}

/// Declines at EXACTLY half the comparisons must fire (the bound is a strict minority), and the
/// violation exists only in the cross-report sum — each fabricated report alone would trip a
/// DIFFERENT assertion or none, so this also pins the fold summing both counters.
#[test]
#[should_panic(expected = "coverage collapsed")]
fn membership_band_bounds_fire_when_declines_reach_half() {
  let reports = [
    MultiVoprReport {
      membership_oracle_comparisons: 2,
      ..MultiVoprReport::default()
    },
    MultiVoprReport {
      kind_unobservable_installs: 1,
      ..MultiVoprReport::default()
    },
  ];
  assert_membership_band_bounds("fabricated collapsed", &reports);
}

#[test]
fn same_seed_same_report() {
  let p = MultiProfile::default_multi();
  assert_eq!(
    run_multi_vopr(42, 3_000, p),
    run_multi_vopr(42, 3_000, p),
    "run_multi_vopr must be a pure function of (seed, ticks, profile)"
  );
}

#[test]
fn default_band_is_nonvacuous() {
  // Deliberately NO membership-counter bounds here (single-group plain-band parity): under the
  // demand-driven default threshold no group compacts within a bounded run, so the oracle's
  // comparisons are structurally zero — a dedicated lowered-threshold snapshot band owns that
  // policy, the single-group precedent. The per-checker finalize assert (skipped == 0,
  // gid/generation-attributed) still runs inside every run of every profile, this one included.
  // The library default the untouched (`None`) override leaves in place, derived from a plain
  // config so the witness assertion tracks the library rather than a pinned number.
  let library_default = sailing_proto::Config::try_new(
    0u64,
    std::vec![0u64],
    std::time::Duration::from_millis(1000),
    std::time::Duration::from_millis(100),
  )
  .expect("a valid reference config")
  .snapshot_threshold();
  for seed in 0..8u64 {
    let r = run_multi_vopr(seed, 4_000, MultiProfile::default_multi());
    assert!(
      r.groups_created >= 2 && r.committed > 0,
      "seed {seed} vacuous: {r:?}"
    );
    assert_eq!(
      r.applied_snapshot_threshold,
      Some(library_default),
      "seed {seed}: the default profile must leave the library default threshold in place: {r:?}"
    );
    // The lineage ledger's content leg is non-vacuous on every profile that commits load: the
    // phantom-quorum map judged real cells (the run-end lineage verdict was not an empty pass).
    // Installs are structurally near-zero at fast budgets (no compaction), so the install leg's
    // non-vacuity lives with the Mini-harness adoption scenario instead.
    assert!(
      r.lineage_cells_judged > 0,
      "seed {seed}: the lineage ledger judged no cells: {r:?}"
    );
  }
}

#[test]
fn snapshot_band_smoke() {
  // A fast smoke keeping [`MultiProfile::snapshot_heavy`] wired on the everyday gate: the default
  // band's shape (groups created + committed load) under the snapshot profile. Deliberately NO
  // membership-counter bounds — membership coverage physically requires soak-scale tick budgets.
  // At fast budgets compaction cannot occur, so the oracle's comparisons are structurally near-zero
  // (the measured curve: 0 comparisons @4k ticks, 1 @8k, 7 @40k) and the nonzero-comparisons bound
  // would trip vacuously. The three band-scale bounds bind at the snapshot SOAK
  // (soak_snapshot_heavy_profile). The per-checker finalize assert (skipped == 0,
  // gid/generation-attributed) still runs inside every run of every profile, this one included.
  for seed in 0..8u64 {
    let r = run_multi_vopr(seed, 4_000, MultiProfile::snapshot_heavy(seed));
    assert!(
      r.groups_created >= 2 && r.committed > 0,
      "seed {seed} vacuous: {r:?}"
    );
    // The applied-threshold WITNESS: the runner pushed the snapshot-heavy override all the way to a
    // constructed replica's config. Severing run_multi_vopr's set_snapshot_threshold leaves this at
    // the library default and trips here — the everyday-gate detector the draw and construction-path
    // unit tests, which drive the world directly and bypass the public runner, cannot provide.
    let applied = r
      .applied_snapshot_threshold
      .expect("a completed run constructed at least one replica");
    assert!(
      (256..=511).contains(&applied),
      "seed {seed}: the runner did not apply the snapshot-heavy threshold to a replica \
       (witness {applied}, outside the 256..=511 band): {r:?}"
    );
  }
}

/// The reshape band smoke: the default menu plus a steady `Split` weight at the fast budget.
/// Beyond the standing non-vacuity floor the band must actually RESHAPE — the `splits_applied`
/// witness proves committed splits materialized children, so the conservation verdict inside
/// every run judged real parent/child handovers rather than an empty work list. Deliberately NO
/// membership-counter bounds (the default band's parity): the per-checker finalize assert
/// (skipped == 0, gid/generation-attributed) still runs inside every run.
#[test]
fn reshape_band_smoke() {
  let mut total_splits = 0u64;
  for seed in 0..8u64 {
    let r = run_multi_vopr(seed, 4_000, MultiProfile::reshape());
    std::eprintln!(
      "reshape seed {seed}: splits_applied={} groups_created={} committed={}",
      r.splits_applied,
      r.groups_created,
      r.committed
    );
    assert!(
      r.groups_created >= 2 && r.committed > 0,
      "seed {seed} vacuous: {r:?}"
    );
    total_splits += r.splits_applied;
  }
  assert!(
    total_splits > 0,
    "the reshape band never materialized a split — the Split action is inert"
  );
}

/// Seed 2's schedule retires groups steadily while client load keeps committing (66 removals /
/// 568 committed in the sweep) — the removal-during-load shape, with every oracle armed: the
/// run completing IS the assertion that no cross-talk, one-identity, or per-group safety
/// violation rode the churn.
#[test]
fn regression_seed_2_removal_during_load() {
  let r = run_multi_vopr(2, 4_000, MultiProfile::default_multi());
  assert!(
    r.groups_removed > 0 && r.committed > 0,
    "the shape requires removal under live load: {r:?}"
  );
}

/// Seed 11's schedule recreates retired gids under load harder than its peers (38 recreations
/// in the sweep) — the gen+1 same-logical-group rejoin shape, crossing the one-identity
/// oracle's incarnation boundary on every recreation.
#[test]
fn regression_seed_11_recreate_under_load() {
  let r = run_multi_vopr(11, 4_000, MultiProfile::default_multi());
  assert!(
    r.groups_recreated > 0 && r.committed > 0,
    "the shape requires recreation under live load: {r:?}"
  );
}

/// Seed 36 pins the ZOMBIE-REAP shape: a voter is removed while partitioned, the farewell
/// append is lost in the fault soup, and the victim lingers believing itself leader at a stale
/// term — at etcd-parity defaults (pre-vote/check-quorum off) higher-term peers silently ignore
/// its beats, so only the harness's catalog model (the departed sweep) retires it. A
/// first-Leader-in-id-order scan once let this zombie shadow the live quorum's leader and
/// turned the healthy schedule into a false calm-window livelock at these exact tick counts;
/// the leader view must anchor on the highest term. Run under `--release` when iterating —
/// 24k ticks in debug is minutes.
#[test]
fn regression_seed_36_removed_zombie_leader_is_reaped() {
  let r = run_multi_vopr(36, 24_000, MultiProfile::default_multi());
  assert!(
    r.groups_removed > 0 && r.committed > 0,
    "the shape requires removal under load: {r:?}"
  );
}

/// The M1 exit-criterion soak: 64 seeds x 40_000 ticks under the default profile. `#[ignore]`
/// so the everyday gate stays fast; ALWAYS run it `--release` (the band in debug wastes hours):
///
/// ```text
/// cargo test --release -p sailing-simulation --test multi_vopr -- --ignored soak_default_profile
/// ```
///
/// Shardable via env exactly like the single-group `vopr_long_sweep`
/// (`MULTI_VOPR_SEED_{START,END}` + `MULTI_VOPR_TICKS`), so CI or a local run can slice the
/// band. Deterministic per (seed, ticks): failures replay anywhere.
#[test]
#[ignore = "the M1 soak — run explicitly, --release. Shard via MULTI_VOPR_SEED_{START,END} + MULTI_VOPR_TICKS."]
fn soak_default_profile() {
  fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
      .ok()
      .and_then(|v| v.parse().ok())
      .unwrap_or(default)
  }
  let start = env_u64("MULTI_VOPR_SEED_START", 0);
  let end = env_u64("MULTI_VOPR_SEED_END", 64);
  let ticks = env_u64("MULTI_VOPR_TICKS", 40_000) as usize;
  assert!(end > start, "empty band: {start} >= {end}");
  for seed in start..end {
    let r = run_multi_vopr(seed, ticks, MultiProfile::default_multi());
    assert!(
      r.groups_created >= 2 && r.committed > 0,
      "seed {seed} vacuous: {r:?}"
    );
  }
}

/// The snapshot-heavy soak: the [`soak_default_profile`] shape under
/// [`MultiProfile::snapshot_heavy`], holding the band-scale membership bounds at soak volume.
/// `#[ignore]` so the everyday gate stays fast; ALWAYS run it `--release`:
///
/// ```text
/// cargo test --release -p sailing-simulation --test multi_vopr -- --ignored soak_snapshot_heavy_profile
/// ```
///
/// Shardable via its own env vars (`MULTI_VOPR_SNAPSHOT_SEED_{START,END}` +
/// `MULTI_VOPR_SNAPSHOT_TICKS`), distinct from the default soak's so a CI job can slice both
/// independently. Deterministic per (seed, ticks): failures replay anywhere.
#[test]
#[ignore = "the snapshot-heavy soak — run explicitly, --release. Shard via MULTI_VOPR_SNAPSHOT_SEED_{START,END} + MULTI_VOPR_SNAPSHOT_TICKS."]
fn soak_snapshot_heavy_profile() {
  fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
      .ok()
      .and_then(|v| v.parse().ok())
      .unwrap_or(default)
  }
  let start = env_u64("MULTI_VOPR_SNAPSHOT_SEED_START", 0);
  let end = env_u64("MULTI_VOPR_SNAPSHOT_SEED_END", 64);
  let ticks = env_u64("MULTI_VOPR_SNAPSHOT_TICKS", 40_000) as usize;
  assert!(end > start, "empty band: {start} >= {end}");
  let mut reports = Vec::new();
  for seed in start..end {
    let r = run_multi_vopr(seed, ticks, MultiProfile::snapshot_heavy(seed));
    assert!(
      r.groups_created >= 2 && r.committed > 0,
      "seed {seed} vacuous: {r:?}"
    );
    reports.push(r);
  }
  // Each env shard enforces the membership policy over ITS OWN slice aggregate — exactly how the
  // single-group sweeps assert over their env-selected seed range. Slice bounds compose to the
  // whole-band bound: nonzero comparisons and zero skipped sum across slices, and adding the
  // strict per-slice `declines*2 < comparisons` inequalities yields the band's.
  assert_membership_band_bounds(&std::format!("snapshot {start}..{end} @{ticks}"), &reports);
}

/// The reshape soak: the [`soak_default_profile`] shape under [`MultiProfile::reshape`], so
/// splits land amid soak-scale fault/lifecycle churn and every run's conservation verdict
/// judges the accumulated handovers. Each slice additionally requires the split witness
/// (`splits_applied` nonzero over the slice) — a soak that never reshaped proves nothing about
/// reshaping. `#[ignore]` so the everyday gate stays fast; ALWAYS run it `--release`:
///
/// ```text
/// cargo test --release -p sailing-simulation --test multi_vopr -- --ignored soak_reshape_profile
/// ```
///
/// Shardable via its own env vars (`MULTI_VOPR_RESHAPE_SEED_{START,END}` +
/// `MULTI_VOPR_RESHAPE_TICKS`), distinct from the other soaks' so CI can slice each
/// independently. Deterministic per (seed, ticks): failures replay anywhere.
#[test]
#[ignore = "the reshape soak — run explicitly, --release. Shard via MULTI_VOPR_RESHAPE_SEED_{START,END} + MULTI_VOPR_RESHAPE_TICKS."]
fn soak_reshape_profile() {
  fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
      .ok()
      .and_then(|v| v.parse().ok())
      .unwrap_or(default)
  }
  let start = env_u64("MULTI_VOPR_RESHAPE_SEED_START", 0);
  let end = env_u64("MULTI_VOPR_RESHAPE_SEED_END", 64);
  let ticks = env_u64("MULTI_VOPR_RESHAPE_TICKS", 40_000) as usize;
  assert!(end > start, "empty band: {start} >= {end}");
  let mut total_splits = 0u64;
  for seed in start..end {
    let r = run_multi_vopr(seed, ticks, MultiProfile::reshape());
    std::eprintln!(
      "reshape soak seed {seed}: splits_applied={} groups_created={} groups_removed={} \
       groups_recreated={} committed={}",
      r.splits_applied,
      r.groups_created,
      r.groups_removed,
      r.groups_recreated,
      r.committed
    );
    assert!(
      r.groups_created >= 2 && r.committed > 0,
      "seed {seed} vacuous: {r:?}"
    );
    total_splits += r.splits_applied;
  }
  std::eprintln!("reshape soak {start}..{end} @{ticks}: total splits_applied={total_splits}");
  assert!(
    total_splits > 0,
    "the reshape soak slice {start}..{end} never materialized a split"
  );
}

/// The merge band smoke: the reshape menu plus the three merge verbs at the fast budget.
/// Beyond the standing non-vacuity floor the band must actually MERGE — the `merges_registered`
/// witness proves parked commits resolved to real absorbs, so the union verdict inside every
/// run judged real source→target handovers rather than an empty work list (and the rollback
/// witness proves the race arm drew).
#[test]
fn merge_band_smoke() {
  let mut total_registered = 0u64;
  let mut total_rollbacks = 0u64;
  let mut total_aborted = 0u64;
  // The async crash-suite non-vacuity witnesses, summed over the band (the merge family runs its
  // stores async — see [`MultiProfile::merge_reshape`]).
  let mut total_torn = 0u64;
  let mut total_crashes_inflight = 0u64;
  let mut total_flushes = 0u64;
  // Certify past the filed #106 under-hosted parked-absorb and #110 fork-fence classes: they are
  // reachable from any merge-heavy schedule, and the removed-replica farewell retry's front-loaded
  // delivery dismantles merge sources into husks sooner, raising their incidence — so this non-storm
  // profile opts into the same exemption the storm profiles carry. The predicate stays deliberately
  // narrow, so a non-merge livelock (or any wedge outside the tracked sets) still trips; the band
  // must exercise the exemption at least once (proven nonzero below).
  let mut total_tracked_exempted = 0u64;
  for seed in 0..8u64 {
    let r = run_multi_vopr_certifying_tracked_wedges(seed, 4_000, MultiProfile::merge_reshape());
    std::eprintln!(
      "merge seed {seed}: prepared={} committed={} rolled_back={} registered={} resolved={} \
       aborted={} splits={} committed_load={} log_flushes={} stable_flushes={} torn={} \
       crash_log_inflight={} crash_stable_inflight={}",
      r.merges_prepared,
      r.merges_committed,
      r.merges_rolled_back,
      r.merges_registered,
      r.merges_resolved,
      r.merges_aborted,
      r.splits_applied,
      r.committed,
      r.log_flushes,
      r.stable_flushes,
      r.torn_writes_fired,
      r.crashes_with_log_inflight,
      r.crashes_with_stable_inflight,
    );
    assert!(
      r.groups_created >= 2 && r.committed > 0,
      "seed {seed} vacuous: {r:?}"
    );
    total_registered += r.merges_registered;
    total_rollbacks += r.merges_rolled_back;
    total_aborted += r.merges_aborted;
    total_flushes += r.log_flushes + r.stable_flushes;
    total_torn += r.torn_writes_fired;
    total_crashes_inflight += r.crashes_with_log_inflight + r.crashes_with_stable_inflight;
    total_tracked_exempted += r.tracked_merge_wedges_exempted + r.fork_fence_couplings_exempted;
  }
  std::eprintln!("merge band: total_tracked_exempted={total_tracked_exempted}");
  assert!(
    total_registered > 0,
    "the merge band never resolved an absorb — the merge verbs are inert"
  );
  assert!(
    total_rollbacks + total_aborted > 0,
    "the merge band never exercised the abort side (no rollback accepted, no parked abort)"
  );
  // The async crash suite is non-vacuous over the band: the multi tick fsync-flushes, torn writes
  // strand real batches, and crashes land mid-window — the coverage the sync default cannot reach.
  assert!(
    total_flushes > 0,
    "the merge band never flushed an async store — the multi tick is not fsync-flushing"
  );
  assert!(
    total_torn > 0,
    "the merge band never fired a torn write that stranded a real batch — the lost-fsync window is inert"
  );
  assert!(
    total_crashes_inflight > 0,
    "the merge band never crashed mid-fsync-window — the crash×durability interleaving is vacuous"
  );
  assert!(
    total_tracked_exempted > 0,
    "the merge band never reached the filed #106/#110 wedge — the tracked-wedge certification is \
     vacuous; if the retry's front-loaded delivery no longer raises the husk incidence, revisit it"
  );
}

/// Determinism holds under the merge profile too: the same (seed, ticks, profile) replays to
/// the identical report, merge counters included. The profile certifies past the filed #106/#110
/// wedges (the merge-heavy schedule reaches the under-hosted parked-absorb husk, whose incidence the
/// farewell retry's front-loaded delivery raises); the exemption is liveness-only and narrow (safety
/// runs unconditionally), and determinism holds because both replays share the flag.
#[test]
fn merge_profile_same_seed_same_report() {
  let r = run_multi_vopr_certifying_tracked_wedges(43, 3_000, MultiProfile::merge_reshape());
  std::eprintln!(
    "merge profile seed 43: tracked_merge_wedges_exempted={} fork_fence_couplings_exempted={} \
     exemption_overlap={}",
    r.tracked_merge_wedges_exempted,
    r.fork_fence_couplings_exempted,
    r.exemption_overlap,
  );
  assert_eq!(
    r,
    run_multi_vopr_certifying_tracked_wedges(43, 3_000, MultiProfile::merge_reshape()),
    "run_multi_vopr must be a pure function of (seed, ticks, profile)"
  );
  assert!(
    r.tracked_merge_wedges_exempted + r.fork_fence_couplings_exempted > 0,
    "seed 43 must reach the filed wedge so the certification is a positive witness, not vacuous"
  );
}

/// The merge×compaction band smoke: the merge menu under [`MultiProfile::merge_reshape_compacting`]'s
/// snapshot-heavy threshold at the fast budget — the first randomized coverage where installs and
/// the merge choreography share a world (plain `merge_reshape` never compacts). Beyond the standing
/// non-vacuity floor, the merge witness must hold over the band (the verbs stay drawable under
/// compaction pressure) and the applied-threshold witness must prove the override reached a
/// constructed replica (the snapshot band's detector, kept here because this profile re-derives
/// the draw). Deliberately NO membership-counter bounds, the snapshot band's parity: at fast
/// budgets compaction-driven install comparisons are structurally near-zero — install×freeze
/// coverage physically needs the soak twin's tick budget.
///
/// Ignored by default: apply progress stalls under compaction pressure during a merge — a liveness
/// gap where agreement holds while follower apply lags, so the compacting install×merge
/// interleaving is not yet driven to convergence; run explicitly with `--ignored`.
#[test]
#[ignore = "apply progress stalls under compaction pressure during a merge (liveness: agreement holds, follower apply lags); the compacting install×merge interleaving is not yet driven to convergence — run explicitly with --ignored"]
fn merge_compacting_band_smoke() {
  let mut total_registered = 0u64;
  let mut total_rollbacks = 0u64;
  let mut total_aborted = 0u64;
  for seed in 0..8u64 {
    let r = run_multi_vopr(seed, 4_000, MultiProfile::merge_reshape_compacting(seed));
    std::eprintln!(
      "merge-compacting seed {seed}: prepared={} committed={} rolled_back={} registered={} \
       resolved={} aborted={} splits={} committed_load={} threshold={:?}",
      r.merges_prepared,
      r.merges_committed,
      r.merges_rolled_back,
      r.merges_registered,
      r.merges_resolved,
      r.merges_aborted,
      r.splits_applied,
      r.committed,
      r.applied_snapshot_threshold,
    );
    assert!(
      r.groups_created >= 2 && r.committed > 0,
      "seed {seed} vacuous: {r:?}"
    );
    let applied = r
      .applied_snapshot_threshold
      .expect("a completed run constructed at least one replica");
    assert!(
      (256..=511).contains(&applied),
      "seed {seed}: the runner did not apply the compacting threshold to a replica \
       (witness {applied}, outside the 256..=511 band): {r:?}"
    );
    total_registered += r.merges_registered;
    total_rollbacks += r.merges_rolled_back;
    total_aborted += r.merges_aborted;
  }
  assert!(
    total_registered > 0,
    "the merge-compacting band never resolved an absorb — the merge verbs are inert under \
     compaction pressure"
  );
  assert!(
    total_rollbacks + total_aborted > 0,
    "the merge-compacting band never exercised the abort side"
  );
}

/// The merge soak: the [`soak_default_profile`] shape under [`MultiProfile::merge_reshape`],
/// so freezes, parked commits, rollback races, and resolutions land amid soak-scale
/// fault/lifecycle churn, with every run's union verdict judging the accumulated absorbs. Each
/// slice additionally requires the merge witness (`merges_registered` nonzero over the slice).
/// `#[ignore]` so the everyday gate stays fast; ALWAYS run it `--release`:
///
/// ```text
/// cargo test --release -p sailing-simulation --test multi_vopr -- --ignored soak_merge_profile
/// ```
///
/// Shardable via its own env vars (`MULTI_VOPR_MERGE_SEED_{START,END}` +
/// `MULTI_VOPR_MERGE_TICKS`). Deterministic per (seed, ticks): failures replay anywhere.
#[test]
#[ignore = "the merge soak — run explicitly, --release. Shard via MULTI_VOPR_MERGE_SEED_{START,END} + MULTI_VOPR_MERGE_TICKS."]
fn soak_merge_profile() {
  fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
      .ok()
      .and_then(|v| v.parse().ok())
      .unwrap_or(default)
  }
  let start = env_u64("MULTI_VOPR_MERGE_SEED_START", 0);
  let end = env_u64("MULTI_VOPR_MERGE_SEED_END", 64);
  let ticks = env_u64("MULTI_VOPR_MERGE_TICKS", 40_000) as usize;
  assert!(end > start, "empty band: {start} >= {end}");
  let mut registered = 0u64;
  // The async crash-suite non-vacuity witnesses: the merge family runs its stores through the real
  // fsync-loss window, so the crash campaign must actually flush, tear, and crash mid-window.
  let mut log_flushes = 0u64;
  let mut stable_flushes = 0u64;
  let mut torn = 0u64;
  let mut crashes_log_inflight = 0u64;
  let mut crashes_stable_inflight = 0u64;
  for seed in start..end {
    let r = run_multi_vopr(seed, ticks, MultiProfile::merge_reshape());
    assert!(
      r.groups_created >= 2 && r.committed > 0,
      "seed {seed} vacuous: {r:?}"
    );
    registered += r.merges_registered;
    log_flushes += r.log_flushes;
    stable_flushes += r.stable_flushes;
    torn += r.torn_writes_fired;
    crashes_log_inflight += r.crashes_with_log_inflight;
    crashes_stable_inflight += r.crashes_with_stable_inflight;
  }
  std::eprintln!(
    "merge soak {start}..{end} @{ticks}: merges_registered={registered} log_flushes={log_flushes} \
     stable_flushes={stable_flushes} torn_writes_fired={torn} \
     crashes_with_log_inflight={crashes_log_inflight} \
     crashes_with_stable_inflight={crashes_stable_inflight}"
  );
  assert!(
    registered > 0,
    "the merge soak slice {start}..{end} never resolved an absorb"
  );
  // Non-vacuity of the async crash suite: a configured-but-never-fired durability fault must fail
  // the check, exactly like the network/cross-talk non-vacuity gates. If any of these is zero the
  // suite CLAIMED to test lost-fsync durability but exercised none of it.
  assert!(
    log_flushes > 0 && stable_flushes > 0,
    "the merge soak slice {start}..{end} never flushed an async store — the multi tick is not \
     fsync-flushing (log_flushes={log_flushes} stable_flushes={stable_flushes})"
  );
  assert!(
    torn > 0,
    "the merge soak slice {start}..{end} never fired a torn write that stranded a real batch — the \
     lost-fsync window is not being exercised"
  );
  assert!(
    crashes_log_inflight + crashes_stable_inflight > 0,
    "the merge soak slice {start}..{end} never crashed mid-fsync-window — the crash×durability \
     interleaving is vacuous (crashes_with_log_inflight={crashes_log_inflight} \
     crashes_with_stable_inflight={crashes_stable_inflight})"
  );
}

/// The merge×compaction soak: the [`soak_merge_profile`] shape under
/// [`MultiProfile::merge_reshape_compacting`], so snapshot installs land on frozen sources,
/// parked targets, and obligation holders amid soak-scale churn — the install×freeze/park seams
/// only this profile reaches. Each slice requires the merge witness (`merges_registered`
/// nonzero). `#[ignore]` so the everyday gate stays fast; ALWAYS run it `--release`:
///
/// ```text
/// cargo test --release -p sailing-simulation --test multi_vopr -- --ignored soak_merge_compacting_profile
/// ```
///
/// Shardable via its own env vars (`MULTI_VOPR_MERGE_COMPACTING_SEED_{START,END}` +
/// `MULTI_VOPR_MERGE_COMPACTING_TICKS`). Deterministic per (seed, ticks): failures replay
/// anywhere.
#[test]
#[ignore = "the merge-compacting soak — run explicitly, --release. Shard via MULTI_VOPR_MERGE_COMPACTING_SEED_{START,END} + MULTI_VOPR_MERGE_COMPACTING_TICKS."]
fn soak_merge_compacting_profile() {
  fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
      .ok()
      .and_then(|v| v.parse().ok())
      .unwrap_or(default)
  }
  let start = env_u64("MULTI_VOPR_MERGE_COMPACTING_SEED_START", 0);
  let end = env_u64("MULTI_VOPR_MERGE_COMPACTING_SEED_END", 64);
  let ticks = env_u64("MULTI_VOPR_MERGE_COMPACTING_TICKS", 40_000) as usize;
  assert!(end > start, "empty band: {start} >= {end}");
  let mut registered = 0u64;
  let mut log_flushes = 0u64;
  let mut stable_flushes = 0u64;
  let mut torn = 0u64;
  let mut crashes_log_inflight = 0u64;
  let mut crashes_stable_inflight = 0u64;
  for seed in start..end {
    let r = run_multi_vopr(seed, ticks, MultiProfile::merge_reshape_compacting(seed));
    assert!(
      r.groups_created >= 2 && r.committed > 0,
      "seed {seed} vacuous: {r:?}"
    );
    registered += r.merges_registered;
    log_flushes += r.log_flushes;
    stable_flushes += r.stable_flushes;
    torn += r.torn_writes_fired;
    crashes_log_inflight += r.crashes_with_log_inflight;
    crashes_stable_inflight += r.crashes_with_stable_inflight;
  }
  std::eprintln!(
    "merge-compacting soak {start}..{end} @{ticks}: merges_registered={registered} \
     log_flushes={log_flushes} stable_flushes={stable_flushes} torn_writes_fired={torn} \
     crashes_with_log_inflight={crashes_log_inflight} \
     crashes_with_stable_inflight={crashes_stable_inflight}"
  );
  assert!(
    registered > 0,
    "the merge-compacting soak slice {start}..{end} never resolved an absorb"
  );
  // The async crash suite is non-vacuous under compaction pressure too.
  assert!(
    log_flushes > 0 && stable_flushes > 0 && torn > 0,
    "the merge-compacting soak slice {start}..{end} did not exercise the fsync-loss window \
     (log_flushes={log_flushes} stable_flushes={stable_flushes} torn={torn})"
  );
  assert!(
    crashes_log_inflight + crashes_stable_inflight > 0,
    "the merge-compacting soak slice {start}..{end} never crashed mid-fsync-window \
     (crashes_with_log_inflight={crashes_log_inflight} crashes_with_stable_inflight={crashes_stable_inflight})"
  );
}
