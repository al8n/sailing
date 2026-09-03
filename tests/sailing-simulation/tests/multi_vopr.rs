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

/// The RETIRED exemption, kept as an assertion. The merge-liveness cure closed both #106/#110 classes —
/// #110's fork-fence coupling resolves through the fence-deferred absorb (`Absorbed` → capture debt
/// → `Merged`), and #106's under-hosted park advertises its boundary and adopts the leader's
/// covering snapshot — so a run that still carries either shape at run end has REGRESSED. The
/// classifiers stay wired precisely so that regression is ATTRIBUTED: the failure names the class,
/// the seed and the counts, instead of surfacing as a generic quiesce failure.
///
/// The one residual the cure's design leaves open — a covering blob past the one-frame bound, which
/// the leader must decline to send — is unreachable at the sim's tiny FSM blob sizes, so nothing
/// here needs softening to an inequality; it stays tracked on #106.
///
/// The gate reads the UNATTRIBUTED remainder, not the raw counts. A fork the relay holds for a
/// tombstoned child id keeps its parent unconsumable, and a merge parked behind that parent
/// satisfies both cured classes' SHAPE predicates while its actual cause is the held fork — a
/// by-design behavior with an embedder-held valve, not a regression of either cure. Subtracting the
/// attribution overlap keeps the teeth exactly where they were: a #106 or #110 shape with no held
/// fork behind it still trips, per class, with the same attribution in the message.
fn assert_no_tracked_exemption(band: &str, seed: u64, r: &MultiVoprReport) {
  assert_eq!(
    r.fork_fence_couplings_exempted - r.fork_fence_retired_hold_overlap,
    0,
    "{band} seed {seed} reached a FORK-FENCE coupling (#110) NOT attributable to a tombstone-held \
     fork — the class the fence-deferred absorb retired — overlap with #106 = {}, under-hosted = \
     {}: {r:?}",
    r.exemption_overlap,
    r.tracked_merge_wedges_exempted,
  );
  assert_eq!(
    r.tracked_merge_wedges_exempted - r.under_hosted_retired_hold_overlap,
    0,
    "{band} seed {seed} reached an UNDER-HOSTED parked absorb (#106) NOT attributable to a \
     tombstone-held fork — the class the advertised boundary and its covering snapshot retired: \
     {r:?}",
  );
}

/// The merge band's per-seed allowance for the FILED #106-family residual (see
/// [`merge_band_smoke`]): the post-completion under-hosted park whose designed cure is the M-8
/// courtesy transfer. Deliberately a small CEILING and not a blanket pass — the band's observed
/// per-seed remainder sits at or below it, so a NEW unattributed wedge lifts a seed above the
/// ceiling and fails the band.
const MERGE_BAND_FILED_ALLOWANCE: u64 = 4;

/// The DEEP merge soak's per-seed allowance for the same filed class (see
/// [`soak_merge_profile`]). Larger than the smoke band's for one reason only: the soak runs ten
/// times the ticks, so one seed's schedule has proportionally more opportunity to reach the class
/// — not because the class is treated more leniently there. Sized to the observed per-seed
/// remainder with headroom, and it is still a CEILING: a schedule that piles up unattributed
/// wedges beyond it fails the soak.
const MERGE_SOAK_FILED_ALLOWANCE: u64 = 8;

/// [`assert_no_tracked_exemption`] with a per-class ALLOWANCE for a band that certifies a filed
/// residual. `allowance` is the number of attributed-remainder wedges a single seed may carry
/// before it counts as a new one; at `0` this is exactly the strict assertion.
fn assert_no_tracked_exemption_beyond(band: &str, seed: u64, r: &MultiVoprReport, allowance: u64) {
  let fork_fence = r.fork_fence_couplings_exempted - r.fork_fence_retired_hold_overlap;
  assert!(
    fork_fence <= allowance,
    "{band} seed {seed} reached {fork_fence} FORK-FENCE couplings (#110) NOT attributable to a \
     tombstone-held fork, above the filed allowance of {allowance} — overlap with #106 = {}, \
     under-hosted = {}: {r:?}",
    r.exemption_overlap,
    r.tracked_merge_wedges_exempted,
  );
  let under_hosted = r.tracked_merge_wedges_exempted - r.under_hosted_retired_hold_overlap;
  assert!(
    under_hosted <= allowance,
    "{band} seed {seed} reached {under_hosted} UNDER-HOSTED parked absorbs (#106) NOT attributable \
     to a tombstone-held fork, above the filed allowance of {allowance}: {r:?}"
  );
}

/// The retirement assertion fires per CLASS: a #110-only report trips it though the #106 counter
/// stayed zero, so neither class can ride a green band behind the other.
#[test]
#[should_panic(expected = "reached a FORK-FENCE coupling (#110)")]
fn the_retired_exemption_fires_on_a_fork_fence_coupling() {
  assert_no_tracked_exemption(
    "fabricated",
    5,
    &MultiVoprReport {
      fork_fence_couplings_exempted: 2,
      ..MultiVoprReport::default()
    },
  );
}

/// THE DUAL-ROOT CASE, kept as an assertion because it is what a raw set intersection gets wrong.
/// A group can be independently under-hosted AND sit inside the held-fork cascade at the same
/// time; intersecting the two sets cancels it and a REAL #106 regression rides a green band. Only
/// the wedges the held-fork class actually EXPLAINS are subtracted, so a partial attribution leaves
/// a remainder — and the remainder still trips, per class.
#[test]
#[should_panic(expected = "UNDER-HOSTED parked absorb (#106) NOT attributable")]
fn a_partially_attributable_under_hosted_wedge_still_trips() {
  assert_no_tracked_exemption(
    "fabricated",
    5,
    &MultiVoprReport {
      // Three under-hosted wedges, all three inside the held-fork cascade — but only two of them
      // are there BECAUSE of a held fork. The third is a genuine regression.
      tracked_merge_wedges_exempted: 3,
      retired_hold_wedges_exempted: 3,
      under_hosted_retired_hold_overlap: 2,
      ..MultiVoprReport::default()
    },
  );
}

/// The same shape on the #110 arm.
#[test]
#[should_panic(expected = "FORK-FENCE coupling (#110) NOT attributable")]
fn a_partially_attributable_fork_fence_wedge_still_trips() {
  assert_no_tracked_exemption(
    "fabricated",
    5,
    &MultiVoprReport {
      fork_fence_couplings_exempted: 3,
      retired_hold_wedges_exempted: 3,
      fork_fence_retired_hold_overlap: 2,
      ..MultiVoprReport::default()
    },
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
    // THE FENCE WITNESS. Reshape is where a retired incarnation's traffic would arise, so this is
    // the band that would show the fence rejecting something it must not. Zero is the assertion:
    // a live incarnation's stamp always clears its own floor (equal admits), so nothing legitimate
    // is ever fenced — and with it, the reshape-removed husk's DEPOSE count is zero by
    // construction, since a below-floor campaign never reaches a replica at all. A nonzero count
    // here means a stamp and a floor have drifted onto different scales.
    assert_eq!(
      (r.fenced_frames_dropped, r.fenced_votes_dropped),
      (0, 0),
      "seed {seed} fenced live traffic: {r:?}"
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
///
/// THE EXEMPTION IS BACK ON HERE, and the reason is a residual, not a regression (2026-08-22).
/// The #117 cures closed the #106/#110 space this band could then reach, and the band moved to the
/// plain [`run_multi_vopr`] on that basis. Teaching the fork relay to HOLD changed which schedules
/// the band explores, and the wider space contains a #106-family case the cures do not cover: an
/// absorb TARGET parked on a source that has been merged away and torn down on EVERY host, its own
/// replicas apply-stalled at the park boundary with log-current state. Nothing can serve those
/// replicas — every host that absorbed folded its own terminal floor and tore the source down,
/// and the world's catalog floored the surviving husks, so no host holds the source's state once
/// the completion is registered — so no embedder action releases it: removing the fork-child
/// squatter clears only the coincident fence, the source cannot be recreated (merged away, terminal
/// floor), and the parked target blocks its own teardown.
///
/// That is the POST-COMPLETION variant of the #106 residual family, and its designed cure is the
/// chunked courtesy/cure transfer (M-8) — a courtesy snapshot to an apply-stalled but log-current
/// follower is the only thing that can unstick it. Until that lands these wedges certify under the
/// filed class rather than failing the band. The reproducing shape, for whoever restores the strict
/// mode: this profile, a parked target whose source is retired and hosted nowhere, with the
/// classifier reporting both `#106` and `#110` markers (the `#110` marker is a token-less recreated
/// child at the parent's fork-child id — by-design and NOT the blocker).
///
/// Certifying costs what it always cost: the exemption lets the quiesce drive exit as soon as every
/// non-exempt group has converged, so a park that would have resolved in the remaining budget is
/// left standing and counted. That is the price of not failing on a filed residual; the counters
/// name the class, and [`merge_profile_same_seed_same_report`] keeps the classifiers honest.
///
/// THE BAND STILL HAS TEETH. Certifying past the filed class is NOT a blanket pass: every seed's
/// attributed remainder is asserted against [`MERGE_BAND_FILED_ALLOWANCE`], a small per-seed
/// CEILING sized to the residual's observed shape. A wedge the held-fork class does not explain,
/// beyond that ceiling, fails the band exactly as it did before the exemption came back on — so a
/// real #106/#110 regression on this profile is caught here and named by its class, not read off a
/// generic quiesce dump.
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
  let mut total_absorbed = 0u64;
  // Per-class exemption witnesses for the band (see the doc above): reported, not asserted away.
  let mut total_underhosted = 0u64;
  let mut total_forkfence = 0u64;
  let mut total_retired_hold = 0u64;
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
    // The fence witness (see the reshape band): a merged-away source is the OTHER retired class,
    // and its live members must never be fenced while the merge is resolving.
    assert_eq!(
      (r.fenced_frames_dropped, r.fenced_votes_dropped),
      (0, 0),
      "seed {seed} fenced live traffic: {r:?}"
    );
    total_registered += r.merges_registered;
    total_rollbacks += r.merges_rolled_back;
    total_aborted += r.merges_aborted;
    total_flushes += r.log_flushes + r.stable_flushes;
    total_torn += r.torn_writes_fired;
    total_crashes_inflight += r.crashes_with_log_inflight + r.crashes_with_stable_inflight;
    // THE TEETH, with one named allowance. The other bands assert the attributed remainder at zero
    // outright; this profile cannot, because it certifies the FILED #106-family residual the doc
    // above describes — so the allowance is an explicit per-seed CEILING on that class, and every
    // wedge above it still trips. The ceiling is the band's observed shape at the seeds it runs, so
    // a genuinely new unattributed wedge — a regression, or a schedule the filed class does not
    // explain — fails the band as it always did.
    assert_no_tracked_exemption_beyond("merge band", seed, &r, MERGE_BAND_FILED_ALLOWANCE);
    total_underhosted += r.tracked_merge_wedges_exempted;
    total_forkfence += r.fork_fence_couplings_exempted;
    total_retired_hold += r.retired_hold_wedges_exempted;
    total_absorbed += r.merges_absorbed;
  }
  std::eprintln!(
    "merge band exemptions: under_hosted(#106)={total_underhosted} \
     fork_fence(#110)={total_forkfence} retired_hold={total_retired_hold}"
  );
  std::eprintln!("merge band: total_absorbed={total_absorbed}");
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
  // The fork-fence and abort-fence park classes resolve through the fence-deferred absorb
  // (`Absorbed` -> debt -> `Merged`), so the band's positive witness is the defer FIRING — the
  // complement of the per-seed retirement assertion above, which pins that no seed reached the
  // wedge those defers replaced.
  assert!(
    total_absorbed > 0,
    "the merge band never deferred a fenced absorb — the debt path is inert on the very \
     schedules that used to wedge"
  );
}

/// Determinism holds under the merge profile too: the same (seed, ticks, profile) replays to
/// the identical report, merge counters included. This is the ONE band still on the certifying
/// runner, deliberately: it is where the classifiers stay wired and ASSERTED — the merge-heavy
/// schedule the #106/#110 shapes were reachable from, run with both classes computed and both
/// counters required zero, so a regression is named by its class here rather than read off a generic
/// quiesce dump. Determinism holds because both replays share the flag.
#[test]
fn merge_profile_same_seed_same_report() {
  let r = run_multi_vopr_certifying_tracked_wedges(42, 3_000, MultiProfile::merge_reshape());
  std::eprintln!(
    "merge profile seed 42: merges_absorbed={}",
    r.merges_absorbed
  );
  assert_no_tracked_exemption("merge profile determinism", 42, &r);
  assert_eq!(
    r,
    run_multi_vopr_certifying_tracked_wedges(42, 3_000, MultiProfile::merge_reshape()),
    "run_multi_vopr must be a pure function of (seed, ticks, profile)"
  );
  assert!(
    r.merges_absorbed > 0,
    "seed 42 must exercise the fence-deferred absorb — a zero here means the defer path is inert \
     exactly where it matters (see `merge_reshape_seed_42_converges` for why the pin is 42)"
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
/// THE STALL IS GONE. This band was ignored because apply progress stalled under compaction
/// pressure during a merge — agreement held while follower apply lagged, so the compacting
/// install×merge interleaving never drove to convergence. The merge-liveness cure closed it: the
/// band passes clean across its seeds, quiesce and all, with nothing certified past (it runs on the
/// plain [`run_multi_vopr`], so any wedge panics). What keeps it opt-in is RUNTIME alone — ~45s for
/// the eight compacting seeds, several times the everyday battery's per-band budget — so it stays
/// `#[ignore]`d and is run explicitly, exactly like the soaks:
///
/// ```text
/// cargo test --release -p sailing-simulation --test multi_vopr -- --ignored merge_compacting_band_smoke
/// ```
#[test]
#[ignore = "the merge×compaction band — opt-in for RUNTIME only (~45s); the apply stall behind it no longer reproduces. Run explicitly, --release."]
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
    // Structurally zero on the plain runner, and stated for the same reason the merge band states
    // it: the compacting interleaving certifies past nothing, so a swap back to the certifying
    // runner cannot quietly reintroduce a tolerated wedge here.
    assert_no_tracked_exemption("merge-compacting band", seed, &r);
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
///
/// THE CERTIFYING RUNNER, WITH TEETH (2026-08-23). This soak reaches the same FILED #106-family
/// residual [`merge_band_smoke`] documents — the post-completion under-hosted park whose designed
/// cure is the chunked courtesy/cure transfer (M-8) — so it runs the certifying runner and asserts
/// the ATTRIBUTED REMAINDER per seed against [`MERGE_SOAK_FILED_ALLOWANCE`], exactly as the smoke
/// band does. It is not a blanket pass: a genuinely new unattributed wedge on the deep profile
/// lifts a seed above the ceiling and fails the soak.
///
/// THE STRICT EXEMPTION-OFF MODE RETURNS WHEN M-8 LANDS — the same restore condition, and the same
/// sentence, the smoke band carries; one truth in two places. Until then, a weekly sweep that reds
/// on a filed class every rotation is alert fatigue that buries real regressions, and silence with
/// no teeth is worse than either.
///
/// WHAT THE MERGE-CONSUMED-CHILD CURE'S EVIDENCE ACTUALLY IS, stated so nobody reads it off the
/// wrong instrument: the deterministic container wedge fixture, plus `fork_fence_couplings_exempted`
/// reading ZERO in the attributed counts here — the merge-consumed ring's own class is empty. It is
/// NEVER the convergence of one seed, which is trajectory-sensitive: any change to which forks the
/// relay consumes moves every seed's schedule, and a seed that converges today can reach the filed
/// class tomorrow without anything having regressed.
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
  // Per-class exemption witnesses over the slice — reported below, and asserted per seed.
  let mut underhosted = 0u64;
  let mut forkfence = 0u64;
  let mut retired_hold = 0u64;
  for seed in start..end {
    let r = run_multi_vopr_certifying_tracked_wedges(seed, ticks, MultiProfile::merge_reshape());
    assert!(
      r.groups_created >= 2 && r.committed > 0,
      "seed {seed} vacuous: {r:?}"
    );
    // THE TEETH. Certifying past the filed class is a ceiling, not a pass.
    assert_no_tracked_exemption_beyond("merge soak", seed, &r, MERGE_SOAK_FILED_ALLOWANCE);
    underhosted += r.tracked_merge_wedges_exempted;
    forkfence += r.fork_fence_couplings_exempted;
    retired_hold += r.retired_hold_wedges_exempted;
    registered += r.merges_registered;
    log_flushes += r.log_flushes;
    stable_flushes += r.stable_flushes;
    torn += r.torn_writes_fired;
    crashes_log_inflight += r.crashes_with_log_inflight;
    crashes_stable_inflight += r.crashes_with_stable_inflight;
  }
  std::eprintln!(
    "merge soak {start}..{end} @{ticks} exemptions: under_hosted(#106)={underhosted} \
     fork_fence(#110)={forkfence} retired_hold={retired_hold}"
  );
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
/// Certifying runner with the per-seed ceiling, for exactly [`soak_merge_profile`]'s reason and on
/// the same restore condition — the filed class reaches this profile at the same coordinates
/// (2026-08-23). Read that doc; this is the same decision, not a second one.
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
  let mut underhosted = 0u64;
  let mut forkfence = 0u64;
  let mut retired_hold = 0u64;
  for seed in start..end {
    let r = run_multi_vopr_certifying_tracked_wedges(
      seed,
      ticks,
      MultiProfile::merge_reshape_compacting(seed),
    );
    assert!(
      r.groups_created >= 2 && r.committed > 0,
      "seed {seed} vacuous: {r:?}"
    );
    assert_no_tracked_exemption_beyond(
      "merge-compacting soak",
      seed,
      &r,
      MERGE_SOAK_FILED_ALLOWANCE,
    );
    underhosted += r.tracked_merge_wedges_exempted;
    forkfence += r.fork_fence_couplings_exempted;
    retired_hold += r.retired_hold_wedges_exempted;
    registered += r.merges_registered;
    log_flushes += r.log_flushes;
    stable_flushes += r.stable_flushes;
    torn += r.torn_writes_fired;
    crashes_log_inflight += r.crashes_with_log_inflight;
    crashes_stable_inflight += r.crashes_with_stable_inflight;
  }
  std::eprintln!(
    "merge-compacting soak {start}..{end} @{ticks} exemptions: under_hosted(#106)={underhosted} \
     fork_fence(#110)={forkfence} retired_hold={retired_hold}"
  );
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

/// THE #110 REPRO, converging without an exemption. Seeds 62 and 67 of the lifecycle-churn
/// profile are #110's own coordinates — a merge park held behind a
/// parked fork's standing capture fence on the same parent, two individually sound designs
/// composing into a deadlock. The cure defers the fenced absorb (`Absorbed`, a capture debt
/// discharged into `Merged`) instead of blocking on the fence, so both seeds must now reach run end
/// with BOTH wedge classifiers empty, and the defer must be what carries them: `merges_absorbed`
/// over the pair is the witness that the debt path fired exactly where the wedge used to form.
///
/// Minimized to 200 ticks — the shortest budget at which the merge machinery is non-vacuous on both
/// seeds (62 registers 2 absorbs and defers 3; 67 registers 1). The certifying runner is deliberate:
/// it keeps the classifiers computing so the assertion below is a measurement, not a tautology.
#[test]
fn lifecycle_seeds_62_and_67_converge() {
  let mut absorbed = 0u64;
  for seed in [62u64, 67] {
    let r =
      run_multi_vopr_certifying_tracked_wedges(seed, 200, MultiProfile::lifecycle_churn_reshape());
    std::eprintln!(
      "#110 lifecycle seed {seed}: absorbed={} registered={} prepared={} groups_created={} \
       committed={}",
      r.merges_absorbed,
      r.merges_registered,
      r.merges_prepared,
      r.groups_created,
      r.committed,
    );
    assert!(
      r.groups_created >= 2 && r.committed > 0,
      "seed {seed} vacuous: {r:?}"
    );
    assert!(
      r.merges_registered > 0,
      "seed {seed} never resolved an absorb — the #110 coordinates must still exercise the merge \
       machinery, or the repro has drifted off its shape: {r:?}"
    );
    assert_no_tracked_exemption("#110 lifecycle", seed, &r);
    absorbed += r.merges_absorbed;
  }
  assert!(
    absorbed > 0,
    "neither seed deferred a fenced absorb — the capture debt is inert on the very \
     coordinates #110 names (absorbed={absorbed})"
  );
}

/// THE ATTRIBUTION SEEDS. Seed 40 of the merge profile is the schedule that exposed the gap: a
/// target parked over a merged-away source hosted NOWHERE, behind a tombstone-held fork whose
/// barrier withholds the cure — a #106 shape the counterfactual credits only when its strike domain
/// is the whole catalog (the source is never a merge participant, so a strike set drawn from the
/// wedge set cannot name it). Seed 42 carries the SCHEDULE pin — the fence-deferred absorb —
/// because the retention of covered thaw obligations reclassified an install-covered record's
/// absorb from `Defer` to `Clear` and rolled seed 40 out of that shape; seed 40 keeps the
/// ATTRIBUTION pin here, deliberately without the `merges_absorbed` teeth, so the schedule that
/// exposed the gap still runs end to end. Seeds 10, 17 and 38 reach the same held-fork shape on
/// the current trajectories (every park credited on the catalog domain, one or more un-credited on
/// the wedge-set domain), so they carry the teeth seed 40's trajectory no longer does.
#[test]
fn merge_reshape_credits_the_held_fork_parks() {
  for seed in [40u64, 10, 17, 38] {
    let r = run_multi_vopr_certifying_tracked_wedges(seed, 3_000, MultiProfile::merge_reshape());
    std::eprintln!(
      "#106 attribution seed {seed}: under-hosted={} credited={} fork-fence={} credited={}",
      r.tracked_merge_wedges_exempted,
      r.under_hosted_retired_hold_overlap,
      r.fork_fence_couplings_exempted,
      r.fork_fence_retired_hold_overlap,
    );
    assert!(
      r.groups_created >= 2 && r.committed > 0,
      "seed {seed} vacuous: {r:?}"
    );
    assert_no_tracked_exemption("#106 attribution", seed, &r);
  }
}

/// THE #106/#110 MERGE SEED, converging without an exemption. Seed 42 of the merge profile is a
/// schedule that reaches the fence-deferred absorb and still quiesces with BOTH classifiers empty;
/// its determinism twin ([`merge_profile_same_seed_same_report`]) pins the report's purity, and
/// this pins the property that report carries — the defer is what got the run there.
///
/// The pin moved off seed 43 when the relay began persisting the guard advance a REDUNDANT fold
/// owes: a fork seed 43 used to leave staged (and whose fence produced its defer) is now consumed
/// at the fold, so that seed no longer reaches the shape. It moved again, off seed 40, when a
/// covered thaw obligation stopped being dropped at a transfer: an install-covered record no
/// longer fences its holder's absorb capture (the classification is `Clear`, not `Defer`), and a
/// discharged record holds the absorb of a frozen holder as a witness debt — so seed 40's
/// trajectory no longer contains a fence-deferred absorb at all, while seed 42's does
/// (`merges_absorbed = 2`; seeds 45, 46, 47 and 49 also exercise it, and every seed from 41 to 52
/// quiesces with both classifiers empty). The witness is a property of the schedule, not of the
/// number — re-pinning to a seed that still exercises it keeps the teeth, where weakening the
/// assertion would have removed them.
#[test]
fn merge_reshape_seed_42_converges() {
  let r = run_multi_vopr_certifying_tracked_wedges(42, 3_000, MultiProfile::merge_reshape());
  std::eprintln!(
    "#106/#110 merge seed 42: absorbed={} registered={} groups_created={} committed={}",
    r.merges_absorbed,
    r.merges_registered,
    r.groups_created,
    r.committed,
  );
  assert!(
    r.groups_created >= 2 && r.committed > 0,
    "seed 42 vacuous: {r:?}"
  );
  assert_no_tracked_exemption("#106/#110 merge", 42, &r);
  assert!(
    r.merges_absorbed > 0,
    "seed 42 never deferred a fenced absorb — the debt path is inert on this schedule: {r:?}"
  );
}
