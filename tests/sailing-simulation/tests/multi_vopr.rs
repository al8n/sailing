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

use sailing_simulation::{MultiProfile, MultiVoprReport, run_multi_vopr};

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
