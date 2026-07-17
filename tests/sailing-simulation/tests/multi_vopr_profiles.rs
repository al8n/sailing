#![allow(missing_docs)]
//! Phase-biased reshape soak profiles. The storm/drain profiles ([`MultiProfile::split_storm`],
//! [`MultiProfile::merge_storm`], [`MultiProfile::mixed_reshape`],
//! [`MultiProfile::lifecycle_churn_reshape`]) run through the same [`run_multi_vopr`] as every
//! other profile: the phase seam concentrates the rare reshaping verbs into bursts so a bounded
//! band actually exercises them, and each cycle's DRAIN is the convergence window the run-end
//! quiesce judges. The fast bands here keep the profiles wired on the everyday gate and prove the
//! bursts are non-vacuous; the `#[ignore]` deep bands are the soak twins (env-sharded like the
//! single-group `vopr_long_sweep`); the pinned seeds are named regressions.
//!
//! Replaying a failure: a panic prints `seed=<S>`; reproduce with
//! `run_multi_vopr(S, ticks, MultiProfile::<profile>())` and read the report counters.
//!
//! The tracked under-hosted parked-absorb class (#106) is EXEMPTED by these (phased) profiles'
//! quiesce and calm windows — a seed that reaches run end still carrying that exact shape completes
//! with `tracked_merge_wedges_exempted > 0` rather than panicking, while a NOVEL wedge still
//! panics. The bands assert on the reshape non-vacuity witnesses, never on the oracle thresholds.

use sailing_simulation::{MultiProfile, run_multi_vopr};

/// Env-configured shard bound for the deep bands (mirrors the single-group sweep's shape).
fn env_u64(key: &str, default: u64) -> u64 {
  std::env::var(key)
    .ok()
    .and_then(|v| v.parse().ok())
    .unwrap_or(default)
}

/// The split-storm band actually reshapes: the burst materializes committed splits
/// (`splits_applied`) amid live load and injected faults, so the conservation verdict inside every
/// run judged real parent/child handovers — and the fault witnesses prove the storm's crashes and
/// partitions fired rather than being drawn and skipped.
#[test]
fn split_storm_band_reshapes_under_faults() {
  let mut splits = 0u64;
  let mut crashes = 0u64;
  let mut partitions = 0u64;
  let mut committed = 0u64;
  for seed in 0..8u64 {
    let r = run_multi_vopr(seed, 350, MultiProfile::split_storm());
    assert!(
      r.groups_created >= 2 && r.committed > 0,
      "seed {seed} vacuous: {r:?}"
    );
    splits += r.splits_applied;
    crashes += r.crashes;
    partitions += r.partitions;
    committed += r.committed;
  }
  std::eprintln!(
    "split_storm band: splits_applied={splits} crashes={crashes} partitions={partitions} committed={committed}"
  );
  assert!(splits > 0, "the split storm never materialized a split");
  assert!(crashes > 0, "the split storm never crashed a node");
  assert!(partitions > 0, "the split storm never isolated a node");
}

/// The merge-storm band actually merges AND exercises the abort side: `merges_registered` proves
/// parked commits resolved to real absorbs (the union verdict judged real handovers), the
/// freeze witness (`merges_prepared`) proves the choreography entered, and the abort witness
/// (`merges_rolled_back + merges_aborted`) proves the rollback race drew.
#[test]
fn merge_storm_band_merges_and_aborts() {
  let mut prepared = 0u64;
  let mut registered = 0u64;
  let mut aborted = 0u64;
  for seed in 0..8u64 {
    let r = run_multi_vopr(seed, 450, MultiProfile::merge_storm());
    assert!(
      r.groups_created >= 2 && r.committed > 0,
      "seed {seed} vacuous: {r:?}"
    );
    prepared += r.merges_prepared;
    registered += r.merges_registered;
    aborted += r.merges_rolled_back + r.merges_aborted;
  }
  std::eprintln!(
    "merge_storm band: merges_prepared={prepared} merges_registered={registered} aborts={aborted}"
  );
  assert!(prepared > 0, "the merge storm never entered a freeze");
  assert!(registered > 0, "the merge storm never resolved an absorb");
  assert!(
    aborted > 0,
    "the merge storm never exercised the abort side"
  );
}

/// The mixed-reshape band exercises BOTH families in one run: the split slice mints children and the
/// later merge slice absorbs them back, so `splits_applied` and `merges_registered` are both
/// non-vacuous over the band.
#[test]
fn mixed_reshape_band_exercises_both_families() {
  let mut splits = 0u64;
  let mut registered = 0u64;
  for seed in 0..6u64 {
    let r = run_multi_vopr(seed, 550, MultiProfile::mixed_reshape());
    assert!(
      r.groups_created >= 2 && r.committed > 0,
      "seed {seed} vacuous: {r:?}"
    );
    splits += r.splits_applied;
    registered += r.merges_registered;
  }
  std::eprintln!("mixed_reshape band: splits_applied={splits} merges_registered={registered}");
  assert!(splits > 0, "the mixed band never split");
  assert!(registered > 0, "the mixed band never merged");
}

/// The lifecycle-churn band crosses the floor/tombstone/incarnation boundaries WHILE reshaping:
/// wholesale group retirement (`groups_removed`) and recreation at gen+1 (`groups_recreated`) run
/// beside the reshape verbs, and at least one reshape family (`splits_applied + merges_registered`)
/// materializes amid the churn.
#[test]
fn lifecycle_churn_band_churns_and_reshapes() {
  let mut removed = 0u64;
  let mut recreated = 0u64;
  let mut reshaped = 0u64;
  for seed in 0..6u64 {
    let r = run_multi_vopr(seed, 450, MultiProfile::lifecycle_churn_reshape());
    assert!(
      r.groups_created >= 2 && r.committed > 0,
      "seed {seed} vacuous: {r:?}"
    );
    removed += r.groups_removed;
    recreated += r.groups_recreated;
    reshaped += r.splits_applied + r.merges_registered;
  }
  std::eprintln!(
    "lifecycle_churn band: groups_removed={removed} groups_recreated={recreated} reshaped={reshaped}"
  );
  assert!(removed > 0, "the lifecycle storm never retired a group");
  assert!(
    recreated > 0,
    "the lifecycle storm never recreated a retired gid"
  );
  assert!(
    reshaped > 0,
    "the lifecycle storm never reshaped amid the churn"
  );
}

/// Determinism holds under a PHASED profile too: the same (seed, ticks, profile) replays to the
/// identical report. The phase-biased draw and the tracked-wedge exemption counter are pure
/// functions of the seed — the behavior-identity contract extended to the storm profiles.
#[test]
fn phased_profile_same_seed_same_report() {
  let p = MultiProfile::split_storm();
  assert_eq!(
    run_multi_vopr(7, 400, p),
    run_multi_vopr(7, 400, p),
    "run_multi_vopr must be a pure function of (seed, ticks, profile) under a phased profile"
  );
}

/// The split-storm soak: [`MultiProfile::split_storm`] across a wider band and deeper budget so the
/// bursts stack and every run's conservation verdict judges the accumulated handovers. `#[ignore]`
/// so the everyday gate stays fast; ALWAYS run it `--release`:
///
/// ```text
/// cargo test --release -p sailing-simulation --test multi_vopr_profiles -- --ignored soak_split_storm
/// ```
///
/// Shardable via `MULTI_VOPR_SPLIT_STORM_SEED_{START,END}` + `MULTI_VOPR_SPLIT_STORM_TICKS`.
#[test]
#[ignore = "the split-storm soak — run explicitly, --release. Shard via MULTI_VOPR_SPLIT_STORM_SEED_{START,END} + MULTI_VOPR_SPLIT_STORM_TICKS."]
fn soak_split_storm() {
  let start = env_u64("MULTI_VOPR_SPLIT_STORM_SEED_START", 0);
  let end = env_u64("MULTI_VOPR_SPLIT_STORM_SEED_END", 64);
  let ticks = env_u64("MULTI_VOPR_SPLIT_STORM_TICKS", 4_000) as usize;
  assert!(end > start, "empty band: {start} >= {end}");
  let mut splits = 0u64;
  let mut exempted = 0u64;
  for seed in start..end {
    let r = run_multi_vopr(seed, ticks, MultiProfile::split_storm());
    assert!(
      r.groups_created >= 2 && r.committed > 0,
      "seed {seed} vacuous: {r:?}"
    );
    splits += r.splits_applied;
    exempted += r.tracked_merge_wedges_exempted;
  }
  std::eprintln!(
    "split-storm soak {start}..{end} @{ticks}: splits_applied={splits} tracked_exempted={exempted}"
  );
  assert!(splits > 0, "the split-storm soak slice never split");
}

/// The merge-storm soak — the [`soak_split_storm`] shape under [`MultiProfile::merge_storm`].
/// Shardable via `MULTI_VOPR_MERGE_STORM_SEED_{START,END}` + `MULTI_VOPR_MERGE_STORM_TICKS`.
#[test]
#[ignore = "the merge-storm soak — run explicitly, --release. Shard via MULTI_VOPR_MERGE_STORM_SEED_{START,END} + MULTI_VOPR_MERGE_STORM_TICKS."]
fn soak_merge_storm() {
  let start = env_u64("MULTI_VOPR_MERGE_STORM_SEED_START", 0);
  let end = env_u64("MULTI_VOPR_MERGE_STORM_SEED_END", 64);
  let ticks = env_u64("MULTI_VOPR_MERGE_STORM_TICKS", 4_000) as usize;
  assert!(end > start, "empty band: {start} >= {end}");
  let mut registered = 0u64;
  let mut exempted = 0u64;
  for seed in start..end {
    let r = run_multi_vopr(seed, ticks, MultiProfile::merge_storm());
    assert!(
      r.groups_created >= 2 && r.committed > 0,
      "seed {seed} vacuous: {r:?}"
    );
    registered += r.merges_registered;
    exempted += r.tracked_merge_wedges_exempted;
  }
  std::eprintln!(
    "merge-storm soak {start}..{end} @{ticks}: merges_registered={registered} tracked_exempted={exempted}"
  );
  assert!(
    registered > 0,
    "the merge-storm soak slice never resolved an absorb"
  );
}

/// The mixed-reshape soak — the [`soak_split_storm`] shape under [`MultiProfile::mixed_reshape`].
/// Shardable via `MULTI_VOPR_MIXED_SEED_{START,END}` + `MULTI_VOPR_MIXED_TICKS`.
#[test]
#[ignore = "the mixed-reshape soak — run explicitly, --release. Shard via MULTI_VOPR_MIXED_SEED_{START,END} + MULTI_VOPR_MIXED_TICKS."]
fn soak_mixed_reshape() {
  let start = env_u64("MULTI_VOPR_MIXED_SEED_START", 0);
  let end = env_u64("MULTI_VOPR_MIXED_SEED_END", 64);
  let ticks = env_u64("MULTI_VOPR_MIXED_TICKS", 6_000) as usize;
  assert!(end > start, "empty band: {start} >= {end}");
  let mut splits = 0u64;
  let mut registered = 0u64;
  let mut exempted = 0u64;
  for seed in start..end {
    let r = run_multi_vopr(seed, ticks, MultiProfile::mixed_reshape());
    assert!(
      r.groups_created >= 2 && r.committed > 0,
      "seed {seed} vacuous: {r:?}"
    );
    splits += r.splits_applied;
    registered += r.merges_registered;
    exempted += r.tracked_merge_wedges_exempted;
  }
  std::eprintln!(
    "mixed-reshape soak {start}..{end} @{ticks}: splits_applied={splits} merges_registered={registered} tracked_exempted={exempted}"
  );
  assert!(
    splits > 0 && registered > 0,
    "the mixed-reshape soak slice did not exercise both families (splits={splits} registered={registered})"
  );
}

/// The lifecycle-churn soak — the [`soak_split_storm`] shape under
/// [`MultiProfile::lifecycle_churn_reshape`]. Shardable via
/// `MULTI_VOPR_LIFECYCLE_SEED_{START,END}` + `MULTI_VOPR_LIFECYCLE_TICKS`.
#[test]
#[ignore = "the lifecycle-churn soak — run explicitly, --release. Shard via MULTI_VOPR_LIFECYCLE_SEED_{START,END} + MULTI_VOPR_LIFECYCLE_TICKS."]
fn soak_lifecycle_churn_reshape() {
  let start = env_u64("MULTI_VOPR_LIFECYCLE_SEED_START", 0);
  let end = env_u64("MULTI_VOPR_LIFECYCLE_SEED_END", 64);
  let ticks = env_u64("MULTI_VOPR_LIFECYCLE_TICKS", 4_000) as usize;
  assert!(end > start, "empty band: {start} >= {end}");
  let mut removed = 0u64;
  let mut reshaped = 0u64;
  let mut exempted = 0u64;
  for seed in start..end {
    let r = run_multi_vopr(seed, ticks, MultiProfile::lifecycle_churn_reshape());
    assert!(
      r.groups_created >= 2 && r.committed > 0,
      "seed {seed} vacuous: {r:?}"
    );
    removed += r.groups_removed;
    reshaped += r.splits_applied + r.merges_registered;
    exempted += r.tracked_merge_wedges_exempted;
  }
  std::eprintln!(
    "lifecycle-churn soak {start}..{end} @{ticks}: groups_removed={removed} reshaped={reshaped} tracked_exempted={exempted}"
  );
  assert!(
    removed > 0 && reshaped > 0,
    "the lifecycle-churn soak slice did not churn and reshape (removed={removed} reshaped={reshaped})"
  );
}
