//! The multi-group VOPR: a deterministic, fault-injecting randomized fuzzer over a
//! [`MultiWorld`] of `MultiRaft` container hosts.
//!
//! [`run_multi_vopr`] is a pure function of `(seed, ticks, profile)`: every choice — world size,
//! the per-iteration action, victims, fault intensities, lifecycle churn — draws from one seeded
//! [`FaultPrng`](crate::store::FaultPrng) stream; no wall clock, no `rand`, no map-order
//! nondeterminism. The per-group safety-oracle suites plus the cross-talk and one-identity
//! tripwires run on every world tick (a violation panics with seed + tick); this loop adds the
//! liveness assertions (calm windows and the final quiesce) and the non-vacuity report.
//!
//! The action menu is DATA — [`MultiProfile`] is a named weight table plus per-replica config
//! knobs — which is the M6 seam: reshaping/storm profiles are weight/knob overrides over the
//! same loop.

use crate::{
  multi::{MultiWorld, decode_gkv, encode_gkv},
  store::FaultPrng,
};
use std::{
  collections::{BTreeMap, BTreeSet},
  vec::Vec,
};

/// The smallest voter set a group may be shrunk to via `RemoveNode`.
const MIN_VOTERS: usize = 2;

/// A weighted multi-group action. The lifecycle verbs (`CreateGroup` / `RemoveGroup` /
/// `RecreateGroup`) plus the `(link, group)` mutes are the genuinely multi-group additions; the
/// rest are the single-group bodies parameterized by a seed-picked live group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MultiAction {
  /// Propose 1..=4 gid-tagged keyed-value commands on a live group's leader.
  ClientLoad,
  /// Issue 1..=3 linearizable reads on a live group (leader-direct or member-forwarded).
  ReadIndexLoad,
  /// Isolate a node (ALL its groups) — legal only if EVERY live group keeps a viable quorum.
  Partition,
  /// Heal a currently isolated node.
  Heal,
  /// Crash a node: every hosted replica loses its fsync window and restores from durable state.
  Crash,
  /// Mute one directed `(link, group)` edge — budgeted like Partition, per group.
  MuteGroup,
  /// Unmute one currently muted `(link, group)` edge.
  UnmuteGroup,
  /// Propose an AddNode / AddLearnerNode / RemoveNode on one group (one in flight PER GROUP).
  ConfChange,
  /// Ask one group's leader to transfer leadership to another of its voters.
  TransferLeader,
  /// Propose a read-mode migration on one group (most targets are legitimately refused under
  /// the plain config; the draw stays deterministic either way).
  MigrateReadMode,
  /// Re-roll the network + per-replica storage fault intensities.
  FaultReroll,
  /// Create a fresh group (monotone gid allocator; 3..=5 seed-chosen member nodes).
  CreateGroup,
  /// Retire a live group everywhere (never the last live one).
  RemoveGroup,
  /// Recreate a retired gid as the SAME logical group at gen+1.
  RecreateGroup,
  /// Propose a SPLIT of one live group at a seed-picked population split point (a fresh-minted
  /// child id). Absent from the default menu — the reshape profile weights it in.
  Split,
  /// Propose a merge FREEZE of one live group into an equal-voter-set sibling (the harness
  /// pairing filter keeps proposals mostly-admissible; every typed refusal is a legitimate
  /// no-op tick). Absent from the default menu — the merge profile weights it in.
  PrepareMerge,
  /// Propose the merge ABSORB for a previously accepted freeze (picked from the fuzzer's
  /// pending-merge book; refusals — leader churn, the local source still catching up — no-op
  /// and a later draw retries).
  CommitMerge,
  /// Propose the merge ABORT for a previously accepted freeze — racing the commit is the
  /// point: the source's log settles every race, and the parked applies must all agree.
  RollbackMerge,
}

/// One phase override: a cyclic iteration range and the weighted menu in force across it. The
/// range's `Range<usize>` is not `Copy`, but a `&'static [PhaseMenu]` (how profiles hold these) is.
type PhaseMenu = (core::ops::Range<usize>, &'static [(MultiAction, u32)]);

/// A named action weight table plus per-replica config knobs — the profile seam M6's
/// storm/reshape profiles override (weights AND knobs, e.g. reusing the snapshot-threshold
/// field for their own compaction pressure).
#[derive(Debug, Clone, Copy)]
pub struct MultiProfile {
  /// The weighted action menu.
  weights: &'static [(MultiAction, u32)],
  /// A `Config::snapshot_threshold` override applied at EVERY replica construction the world
  /// performs (bootstrap voters, recreations, observers, resurrections — and crash restores via
  /// the retained per-replica config). `None` leaves the library's demand-driven default
  /// untouched: construction is byte-identical to a world without the seam, so the default
  /// profile's schedules (and its pinned regression seeds) cannot move.
  snapshot_threshold: Option<usize>,
  /// Whether reshaping-participant groups construct their `Config` with pre-vote on — the
  /// removed-replica disruption cure's prevention layer. Scoped PER-GROUP (never the global
  /// `DEFAULT_PRE_VOTE` const: flipping that reseeds the whole VOPR corpus and breaks etcd-library
  /// parity). `false` for the default/snapshot profiles, so their construction stays
  /// byte-identical; `true` only where the profile weaves in the merge reshape verbs, whose
  /// wholesale source-group removal is exactly the steady-state churn a pre-election probe (which
  /// never inflates the real term) keeps from deposing the live leader.
  pre_vote: bool,
  /// Whether reshaping-participant groups construct their `Config` with check-quorum on — the
  /// prevention layer's leader-side half (a leader that has lost quorum contact steps down rather
  /// than shadowing a fresh election). Scoped and defaulted identically to
  /// [`pre_vote`](Self::pre_vote).
  check_quorum: bool,
  /// The [`StoreMode`](crate::StoreMode) every replica the profile's world wires is constructed
  /// under. `Sync` (the default) keeps the stores commit-on-submit — byte-identical to a world
  /// predating the seam, so the non-merge profiles' schedules and pinned seeds cannot move. `Async`
  /// runs them through the staged-write fsync-loss window (submit → `flush` → durable; crash →
  /// `discard_inflight`) so the randomized crash campaign actually EXERCISES lost-fsync durability
  /// (persist-vote-before-grant, append-before-ack, commit persistence, the reshaping lineage across
  /// a crash) rather than claiming to. Async ONLY on the merge family (`merge_reshape` and
  /// `merge_reshape_compacting`), whose whole-source-group teardowns are the churn that makes the
  /// crash×durability seams reachable; the merge runs reseed against their own prior sync runs,
  /// which the standing merge-band decision permits.
  store_mode: crate::StoreMode,
  /// The phase cycle length in ITERATIONS (the loop's `iter` counter, folded `iter % cycle`), or
  /// `0` for an unphased profile. Governs [`phases`](Self::phases): a phased profile draws from a
  /// phase-local menu while `iter % cycle` lands inside a phase range, and from
  /// [`weights`](Self::weights) elsewhere — the storm/drain seam that makes rare reshaping verbs
  /// actually concentrate. `0` on every unphased profile, where [`weights_at`](Self::weights_at)
  /// returns `weights` unconditionally, so the draw is byte-identical to a world predating the seam.
  cycle: usize,
  /// Phase-local weight overrides, each a `(cyclic iteration range, menu)` pair consulted in order
  /// by [`weights_at`](Self::weights_at). EMPTY on every unphased profile (the default/snapshot/
  /// reshape/merge families), so their per-tick draw reads [`weights`](Self::weights) exactly as
  /// before — the behavior-identity contract. A phased profile's ranges are the storm and drain
  /// slices within one `cycle`; a tick outside every range falls back to `weights`. The range's
  /// `Range<usize>` is not `Copy`, but the field is a `&'static` reference (always `Copy`), so the
  /// profile stays `Copy`.
  phases: &'static [PhaseMenu],
}

impl MultiProfile {
  /// The default M1 mix: client load dominates, faults are frequent, lifecycle churn is rare
  /// but steady (every band exercises removal and recreation).
  pub const fn default_multi() -> Self {
    Self {
      weights: &[
        (MultiAction::ClientLoad, 50),
        (MultiAction::ReadIndexLoad, 12),
        (MultiAction::Partition, 8),
        (MultiAction::Heal, 6),
        (MultiAction::Crash, 5),
        (MultiAction::MuteGroup, 6),
        (MultiAction::UnmuteGroup, 4),
        (MultiAction::ConfChange, 5),
        (MultiAction::TransferLeader, 4),
        (MultiAction::MigrateReadMode, 3),
        (MultiAction::FaultReroll, 6),
        (MultiAction::CreateGroup, 3),
        (MultiAction::RemoveGroup, 2),
        (MultiAction::RecreateGroup, 2),
      ],
      snapshot_threshold: None,
      pre_vote: false,
      check_quorum: false,
      store_mode: crate::StoreMode::Sync,
      cycle: 0,
      phases: &[],
    }
  }

  /// The snapshot-heavy band profile: the default menu (weights untouched — the single-group
  /// snapshot runners bias no actions either; the default mix's lifecycle churn is already what
  /// puts installs under removal/recreation) with `snapshot_threshold` lowered to a MODERATE
  /// seed-derived value, so groups compact within a bounded run and a lagging replica
  /// snapshot-installs — the coverage the demand-driven default (10_000) never reaches. The draw
  /// copies the single-group compacting entries (`run_vopr_joint_snapshot`): 256..=511, from a
  /// DEDICATED sub-stream so the master action/topology stream draws nothing of it.
  pub fn snapshot_heavy(seed: u64) -> Self {
    let mut p = FaultPrng::new(seed.rotate_left(28) ^ 0x4D53_4E50_5448_5231); // "MSNPTHR1"
    let threshold = 256 + (p.next_u64() % 256) as usize; // 256..=511
    Self {
      snapshot_threshold: Some(threshold),
      ..Self::default_multi()
    }
  }

  /// The merge-reshape profile: the reshape menu PLUS the three merge verbs, so freezes,
  /// parked commits, rollback races, and resolutions land amid the full fault/lifecycle churn
  /// — and split children (colocated by construction) keep minting equal-voter-set pairs for
  /// the pairing filter to merge back. Commit outweighs prepare so accepted freezes usually
  /// complete; rollback stays rare but steady (the race arm needs real draws).
  pub const fn merge_reshape() -> Self {
    Self {
      weights: &[
        (MultiAction::ClientLoad, 50),
        (MultiAction::ReadIndexLoad, 12),
        (MultiAction::Partition, 8),
        (MultiAction::Heal, 6),
        (MultiAction::Crash, 5),
        (MultiAction::MuteGroup, 6),
        (MultiAction::UnmuteGroup, 4),
        (MultiAction::ConfChange, 5),
        (MultiAction::TransferLeader, 4),
        (MultiAction::MigrateReadMode, 3),
        (MultiAction::FaultReroll, 6),
        (MultiAction::CreateGroup, 3),
        (MultiAction::RemoveGroup, 2),
        (MultiAction::RecreateGroup, 2),
        (MultiAction::Split, 8),
        (MultiAction::PrepareMerge, 6),
        (MultiAction::CommitMerge, 8),
        (MultiAction::RollbackMerge, 2),
      ],
      snapshot_threshold: None,
      // Prevention layer ON for the merge profiles: a merge dissolves a whole source group, so
      // ignorant removed ex-voters are steady-state churn here — pre-vote keeps their election
      // timer from inflating the real term and deposing the live target/sibling leader, and
      // check-quorum makes a partitioned stale leader step down instead of flapping.
      pre_vote: true,
      check_quorum: true,
      // Async stores ON for the merge family: the whole-source-group teardowns are exactly the
      // churn that makes the crash×durability seams reachable, so this is where the crash campaign
      // must run through the real fsync-loss window to stop being vacuous.
      store_mode: crate::StoreMode::Async,
      cycle: 0,
      phases: &[],
    }
  }

  /// The merge×compaction profile: [`merge_reshape`](Self::merge_reshape)'s verbs under
  /// [`snapshot_heavy`](Self::snapshot_heavy)'s compaction pressure. `merge_reshape` leaves
  /// `snapshot_threshold` at the never-compacting default, so snapshot installs never land on a
  /// frozen source, a parked target, or an obligation holder — the install×freeze/park seams
  /// have zero randomized coverage without this profile. The threshold IS snapshot_heavy's own
  /// draw (256..=511 from its dedicated sub-stream), so the master action/topology stream draws
  /// nothing of it and the merge menu's schedules shift only through world evolution.
  pub fn merge_reshape_compacting(seed: u64) -> Self {
    Self {
      snapshot_threshold: Self::snapshot_heavy(seed).snapshot_threshold,
      ..Self::merge_reshape()
    }
  }

  /// The reshape profile: the default menu PLUS a steady [`MultiAction::Split`] weight, so
  /// splits land amid the full fault/lifecycle churn. The split rides the default menu as an
  /// ADDED row rather than a re-weighting — the default mix's schedules stay the reference
  /// point, and the default profile itself stays byte-identical because its own table never
  /// gains the row (an absent action IS weight zero, and the menu-coverage test keeps every
  /// listed row genuinely drawable).
  pub const fn reshape() -> Self {
    Self {
      weights: &[
        (MultiAction::ClientLoad, 50),
        (MultiAction::ReadIndexLoad, 12),
        (MultiAction::Partition, 8),
        (MultiAction::Heal, 6),
        (MultiAction::Crash, 5),
        (MultiAction::MuteGroup, 6),
        (MultiAction::UnmuteGroup, 4),
        (MultiAction::ConfChange, 5),
        (MultiAction::TransferLeader, 4),
        (MultiAction::MigrateReadMode, 3),
        (MultiAction::FaultReroll, 6),
        (MultiAction::CreateGroup, 3),
        (MultiAction::RemoveGroup, 2),
        (MultiAction::RecreateGroup, 2),
        (MultiAction::Split, 8),
      ],
      snapshot_threshold: None,
      pre_vote: false,
      check_quorum: false,
      store_mode: crate::StoreMode::Sync,
      cycle: 0,
      phases: &[],
    }
  }

  /// The weighted action menu in effect at loop iteration `tick`: the phase-local override whose
  /// cyclic range (`tick % cycle`) contains it, or [`weights`](Self::weights) when no phase
  /// applies. An UNPHASED profile (`phases` empty) returns `weights` unconditionally and never
  /// touches `cycle` — the per-tick draw is byte-identical to a world predating the phase seam,
  /// which is the behavior-identity contract every default/snapshot/reshape/merge band and pinned
  /// seed relies on. Ranges are tried in listing order; the first hit wins.
  pub(crate) fn weights_at(&self, tick: usize) -> &'static [(MultiAction, u32)] {
    if self.phases.is_empty() {
      return self.weights;
    }
    let t = if self.cycle == 0 {
      tick
    } else {
      tick % self.cycle
    };
    self
      .phases
      .iter()
      .find(|(range, _)| range.contains(&t))
      .map(|(_, menu)| *menu)
      .unwrap_or(self.weights)
  }

  /// The split-storm reshape profile: a light steady menu punctuated every `cycle` iterations by a
  /// SPLIT STORM (splits, crashes, and partitions all at once) and then a heal-heavy DRAIN. The
  /// storm concentrates the rare split verb — one steady-weight `Split` never stacks the concurrent
  /// split/fault interleavings a burst does — and the drain is the convergence window the soak
  /// asserts every group re-forms a leader and commits through. No merges (so no whole-group
  /// teardown churn), hence the etcd-parity default config, exactly like [`reshape`](Self::reshape).
  pub const fn split_storm() -> Self {
    Self {
      weights: SPLIT_STORM_BASE,
      snapshot_threshold: None,
      pre_vote: false,
      check_quorum: false,
      store_mode: crate::StoreMode::Sync,
      cycle: 300,
      phases: SPLIT_STORM_PHASES,
    }
  }

  /// The merge-storm reshape profile: a steady menu that mints mergeable pairs (a colocated
  /// equal-voter split child beside its sibling) punctuated by a MERGE STORM — freezes, absorbs,
  /// and rollback races fired together amid partitions/crashes — and a merge-FREE drain that lets
  /// the choreography settle to a single union per pair. Commit outweighs prepare in the storm so
  /// accepted freezes usually complete within it; the drain zeroes all three merge verbs so
  /// convergence is judged on what the storm left behind. Prevention layer ON (pre-vote +
  /// check-quorum): a merge dissolves a whole source group, so its ex-voters are steady-state
  /// churn a pre-election probe keeps from deposing the live target/sibling leader.
  pub const fn merge_storm() -> Self {
    Self {
      weights: MERGE_STORM_BASE,
      snapshot_threshold: None,
      pre_vote: true,
      check_quorum: true,
      store_mode: crate::StoreMode::Sync,
      cycle: 300,
      phases: MERGE_STORM_PHASES,
    }
  }

  /// The mixed-reshape profile: one long `cycle` alternating a SPLIT storm, a drain, a MERGE storm,
  /// and a second drain — so both reshape families land in the same run and a split child minted in
  /// the first storm is a merge candidate by the second. The drains between the storms are the
  /// convergence windows; the merge storm carries the same prevention layer as
  /// [`merge_storm`](Self::merge_storm).
  pub const fn mixed_reshape() -> Self {
    Self {
      weights: MIXED_RESHAPE_BASE,
      snapshot_threshold: None,
      pre_vote: true,
      check_quorum: true,
      store_mode: crate::StoreMode::Sync,
      cycle: 600,
      phases: MIXED_RESHAPE_PHASES,
    }
  }

  /// The lifecycle-churn reshape profile: a LIFECYCLE storm layers wholesale group churn (retire,
  /// recreate at gen+1, create fresh) over reshape verbs (split + merge), so floors, tombstones,
  /// and incarnation boundaries are crossed WHILE the reshape choreography runs — the interaction
  /// the single-family storms never reach. The drain recreates what the storm retired and settles
  /// the merges. Prevention layer ON for the whole-group teardown churn (merge + removal alike).
  pub const fn lifecycle_churn_reshape() -> Self {
    Self {
      weights: LIFECYCLE_CHURN_BASE,
      snapshot_threshold: None,
      pre_vote: true,
      check_quorum: true,
      store_mode: crate::StoreMode::Sync,
      cycle: 300,
      phases: LIFECYCLE_CHURN_PHASES,
    }
  }
}

// The phase-biased profile tables, hoisted to `const` items so the storm/drain menus are named
// once and the `Range<usize>` phase bounds live in a const context (a const initializer, where a
// range literal is const-constructible, unlike an rvalue in a non-const position).

/// [`MultiProfile::split_storm`]'s steady menu: client-load-dominant with a token split weight, so
/// the between-storm windows still commit and occasionally reshape without stacking bursts.
const SPLIT_STORM_BASE: &[(MultiAction, u32)] = &[
  (MultiAction::ClientLoad, 50),
  (MultiAction::ReadIndexLoad, 10),
  (MultiAction::Heal, 8),
  (MultiAction::Partition, 6),
  (MultiAction::Crash, 4),
  (MultiAction::Split, 2),
];
/// The split-storm slice: splits, crashes, and partitions fired together, client load kept high so
/// the reshaped groups have live keys to hand across the boundary.
const SPLIT_STORM_STORM: &[(MultiAction, u32)] = &[
  (MultiAction::Split, 25),
  (MultiAction::ClientLoad, 30),
  (MultiAction::Crash, 12),
  (MultiAction::Partition, 12),
];
/// The split-storm drain: no splits, no new faults — heal and read and commit until the storm's
/// forks all re-form a leader and catch up (the convergence the soak asserts at the boundary).
const SPLIT_STORM_DRAIN: &[(MultiAction, u32)] = &[
  (MultiAction::ClientLoad, 40),
  (MultiAction::Heal, 20),
  (MultiAction::ReadIndexLoad, 15),
];
const SPLIT_STORM_PHASES: &[PhaseMenu] =
  &[(100..180, SPLIT_STORM_STORM), (180..300, SPLIT_STORM_DRAIN)];

/// [`MultiProfile::merge_storm`]'s steady menu: split churn keeps minting colocated equal-voter
/// pairs, a light merge trickle keeps a freeze in flight between storms.
const MERGE_STORM_BASE: &[(MultiAction, u32)] = &[
  (MultiAction::ClientLoad, 45),
  (MultiAction::ReadIndexLoad, 8),
  (MultiAction::Heal, 8),
  (MultiAction::Partition, 5),
  (MultiAction::Crash, 4),
  (MultiAction::Split, 8),
  (MultiAction::PrepareMerge, 3),
  (MultiAction::CommitMerge, 4),
  (MultiAction::RollbackMerge, 1),
];
/// The merge-storm slice: freezes, absorbs, and rollback races fired together amid faults, with
/// splits feeding fresh mergeable pairs. Commit outweighs prepare so accepted freezes complete.
const MERGE_STORM_STORM: &[(MultiAction, u32)] = &[
  (MultiAction::CommitMerge, 12),
  (MultiAction::PrepareMerge, 8),
  (MultiAction::Split, 12),
  (MultiAction::RollbackMerge, 3),
  (MultiAction::ClientLoad, 25),
  (MultiAction::Partition, 8),
  (MultiAction::Crash, 6),
];
/// The merge-storm drain: all three merge verbs ZEROED so the world settles what the storm left —
/// the pump resolves parked absorbs and the quiesce teeth drive the residue to a single union.
const MERGE_STORM_DRAIN: &[(MultiAction, u32)] = &[
  (MultiAction::ClientLoad, 40),
  (MultiAction::Heal, 20),
  (MultiAction::ReadIndexLoad, 15),
];
const MERGE_STORM_PHASES: &[PhaseMenu] =
  &[(100..180, MERGE_STORM_STORM), (180..300, MERGE_STORM_DRAIN)];

/// [`MultiProfile::mixed_reshape`]'s steady menu: both reshape families trickle between the storms.
const MIXED_RESHAPE_BASE: &[(MultiAction, u32)] = &[
  (MultiAction::ClientLoad, 45),
  (MultiAction::ReadIndexLoad, 8),
  (MultiAction::Heal, 8),
  (MultiAction::Partition, 5),
  (MultiAction::Crash, 4),
  (MultiAction::Split, 6),
  (MultiAction::PrepareMerge, 2),
  (MultiAction::CommitMerge, 3),
  (MultiAction::RollbackMerge, 1),
];
/// The split slice of the mixed cycle — mints the children the later merge slice absorbs.
const MIXED_RESHAPE_SPLIT: &[(MultiAction, u32)] = &[
  (MultiAction::Split, 25),
  (MultiAction::ClientLoad, 30),
  (MultiAction::ConfChange, 5),
  (MultiAction::Crash, 10),
  (MultiAction::Partition, 10),
];
/// The merge slice of the mixed cycle — absorbs the split children back, commit over prepare.
const MIXED_RESHAPE_MERGE: &[(MultiAction, u32)] = &[
  (MultiAction::CommitMerge, 12),
  (MultiAction::PrepareMerge, 8),
  (MultiAction::Split, 10),
  (MultiAction::RollbackMerge, 4),
  (MultiAction::ClientLoad, 25),
  (MultiAction::Partition, 8),
  (MultiAction::Crash, 6),
];
/// The mixed drain (used after each storm slice): heal and commit to convergence, no reshaping.
const MIXED_RESHAPE_DRAIN: &[(MultiAction, u32)] = &[
  (MultiAction::ClientLoad, 40),
  (MultiAction::Heal, 20),
  (MultiAction::ReadIndexLoad, 15),
];
const MIXED_RESHAPE_PHASES: &[PhaseMenu] = &[
  (100..220, MIXED_RESHAPE_SPLIT),
  (220..340, MIXED_RESHAPE_DRAIN),
  (340..460, MIXED_RESHAPE_MERGE),
  (460..600, MIXED_RESHAPE_DRAIN),
];

/// [`MultiProfile::lifecycle_churn_reshape`]'s steady menu: reshape verbs beside a light lifecycle
/// trickle (create/retire/recreate) so the working set breathes between storms.
const LIFECYCLE_CHURN_BASE: &[(MultiAction, u32)] = &[
  (MultiAction::ClientLoad, 45),
  (MultiAction::ReadIndexLoad, 8),
  (MultiAction::Heal, 8),
  (MultiAction::Partition, 5),
  (MultiAction::Crash, 4),
  (MultiAction::Split, 4),
  (MultiAction::CreateGroup, 2),
  (MultiAction::RemoveGroup, 2),
  (MultiAction::RecreateGroup, 2),
];
/// The lifecycle storm: wholesale group churn (retire / recreate at gen+1 / create fresh) layered
/// over reshape verbs (split + merge), so floors and incarnation boundaries are crossed WHILE the
/// reshape choreography runs.
const LIFECYCLE_CHURN_STORM: &[(MultiAction, u32)] = &[
  (MultiAction::Split, 10),
  (MultiAction::CommitMerge, 8),
  (MultiAction::RemoveGroup, 8),
  (MultiAction::RecreateGroup, 8),
  (MultiAction::CreateGroup, 6),
  (MultiAction::PrepareMerge, 6),
  (MultiAction::ClientLoad, 25),
  (MultiAction::Crash, 6),
  (MultiAction::Partition, 6),
];
/// The lifecycle drain: recreate what the storm retired and settle the merges, no fresh churn.
const LIFECYCLE_CHURN_DRAIN: &[(MultiAction, u32)] = &[
  (MultiAction::ClientLoad, 40),
  (MultiAction::Heal, 20),
  (MultiAction::ReadIndexLoad, 15),
  (MultiAction::RecreateGroup, 4),
];
const LIFECYCLE_CHURN_PHASES: &[PhaseMenu] = &[
  (100..180, LIFECYCLE_CHURN_STORM),
  (180..300, LIFECYCLE_CHURN_DRAIN),
];

/// Non-vacuity counters: what one [`run_multi_vopr`] actually exercised. Derived
/// `PartialEq`/`Eq` so the determinism test can compare whole reports.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MultiVoprReport {
  /// The world seed (echoed for replay).
  pub seed: u64,
  /// The `snapshot_threshold` a genuinely constructed replica was built under, read back off the
  /// retained per-replica config once the world seam applied the profile — the WITNESS that
  /// [`run_multi_vopr`] pushed `MultiProfile`'s override through world construction rather than
  /// leaving it inert in the profile. `None` only if the run wired no replica; a completed run
  /// always creates a group, so it reports `Some`: the library default under the default profile,
  /// a `256..=511` draw under [`MultiProfile::snapshot_heavy`].
  pub applied_snapshot_threshold: Option<usize>,
  /// World ticks executed across the run (main loop + calm windows + quiesce).
  pub ticks_run: u64,
  /// Groups created (the two initial groups plus every `CreateGroup` action).
  pub groups_created: u64,
  /// Groups retired via `RemoveGroup`.
  pub groups_removed: u64,
  /// Retired groups recreated at gen+1 via `RecreateGroup`.
  pub groups_recreated: u64,
  /// Committed splits REGISTERED across the run (a child materialized somewhere, once per
  /// split) — the reshape coverage's non-vacuity witness: nonzero proves the run-end
  /// conservation verdict judged real parent/child handovers rather than an empty work list.
  pub splits_applied: u64,
  /// Merge freezes ACCEPTED by a source leader across the run.
  pub merges_prepared: u64,
  /// Merge absorbs ACCEPTED by a target leader across the run.
  pub merges_committed: u64,
  /// Merge rollbacks ACCEPTED by a source leader across the run.
  pub merges_rolled_back: u64,
  /// Merges REGISTERED (a park resolved to an absorb somewhere, once per merge) — the merge
  /// coverage's non-vacuity witness: nonzero proves the run-end union verdict judged real
  /// absorbs rather than an empty work list.
  pub merges_registered: u64,
  /// Per-host absorb resolutions across the run (every host's park resolution counts).
  pub merges_resolved: u64,
  /// Per-host abort resolutions across the run (the race/duplicate no-op arm).
  pub merges_aborted: u64,
  /// Per-host capture-failed resolutions across the run — a consumed source whose union could not be
  /// made durable. Expected zero under the sim FSM; a non-zero value is a wedge to report.
  pub merges_capture_failed: u64,
  /// Groups (live OR retired frozen husk) the run-end quiesce EXEMPTED from the convergence/
  /// freeze-wedge verdict as the tracked under-hosted parked-absorb class (#106): the merge
  /// component transitively blocked by an under-hosted merge conf
  /// (see `MultiWorld::tracked_merge_wedge_set`). ALWAYS `0` under an unphased profile —
  /// the exemption is scoped to phased (storm) profiles, so the default/snapshot/reshape/merge
  /// families are byte-identical (quiesce panics on ANY wedge, exactly as before). Nonzero only on a
  /// storm-profile seed that reached run end still carrying the tracked shape: the count is the
  /// seed's witness that it hit the FILED class rather than a novel wedge (which panics regardless).
  pub tracked_merge_wedges_exempted: u64,
  /// Groups the run-end quiesce EXEMPTED as the FORK-FENCE COUPLING class (#110), the RAW predicate
  /// count: a group satisfying BOTH this and the under-hosted predicate contributes to both counters
  /// (and both seed lists in the sweep), so neither class can hide the other. A merge park held
  /// behind a parked fork's standing capture fence on the same parent (a composition deadlock of two
  /// individually sound designs — safety intact, see `MultiWorld::fork_fence_wedge_set`). ALWAYS `0`
  /// under an unphased profile. The overlap with #106 is reported separately in
  /// [`exemption_overlap`](Self::exemption_overlap).
  pub fork_fence_couplings_exempted: u64,
  /// Groups counted in BOTH exemption classes above (#106 under-hosted AND #110 fork-fence) — the
  /// explicit overlap, so the two RAW counters are never misread as disjoint. ALWAYS `0` under an
  /// unphased profile.
  pub exemption_overlap: u64,
  /// HOSTED RETIRED HUSKS the run-end quiesce safety pass visited — gids with a lingering replica
  /// (a merged-away source frozen on a lagging host) that are NOT in `live_groups`. The witness that
  /// the unconditional safety worklist reaches beyond the live set; nonzero only when a husk survived
  /// to run end (the merge families), `0` otherwise.
  pub retired_husks_safety_checked: u64,
  /// Live groups at run end.
  pub final_groups: usize,
  /// Client commands accepted by some leader (tracked per group for the quiesce check).
  pub proposals: u64,
  /// Proposed commands confirmed applied on their group at quiesce.
  pub committed: u64,
  /// Reads accepted by a replica.
  pub reads_issued: u64,
  /// Accepted reads whose confirmation passed the index-floor assertion.
  pub reads_confirmed: u64,
  /// Confirmed reads whose per-key VALUE was asserted at the serve point.
  pub reads_value_checked: u64,
  /// Conf-changes accepted by a group leader.
  pub conf_changes: u64,
  /// Leader transfers accepted.
  pub transfers: u64,
  /// Read-mode migrations accepted.
  pub read_mode_migrations: u64,
  /// Node isolations injected.
  pub partitions: u64,
  /// Node heals injected (outside the calm-window bulk heal).
  pub heals: u64,
  /// Node crashes injected.
  pub crashes: u64,
  /// `(link, group)` mutes injected.
  pub mutes: u64,
  /// `(link, group)` unmutes injected (outside the calm-window bulk unmute).
  pub unmutes: u64,
  /// Fault-intensity rerolls.
  pub fault_rerolls: u64,
  /// Calm windows opened (each asserted per-group election + fresh progress).
  pub calm_windows: u64,
  /// The maximum term observed across every replica of every group.
  pub max_term_seen: u64,
  /// Seeded network faults that fired (drops + duplications).
  pub faults_fired: u64,
  /// gid-tagged applied entries the cross-talk sweep judged (the isolation oracle's
  /// non-vacuity witness).
  pub cross_talk_checks: u64,
  /// Membership-coherence comparisons the run-end final pass performed, summed over every
  /// checker the run built (live groups + the retired archive) — how many observed snapshot
  /// installs actually faced the committed-config history verdict.
  pub membership_oracle_comparisons: u64,
  /// Observed installs the run-end final pass could NOT judge due to an incomplete
  /// committed-config HISTORY, summed over live + retired checkers. The finalize policy panics
  /// (with gid/generation attribution) on any nonzero per-checker count — the single-group
  /// sweep's `skipped == 0` policy — so a completed run always reports `0`.
  pub skipped_unwitnessed_installs: u64,
  /// Observed installs the run-end final pass SOUNDLY DECLINED because the resolved conf-change
  /// index is committed-final but its committed-log KIND was compacted before any tick observed
  /// it. Tolerated, exactly as the single-group policy tolerates the class: a bounded coverage
  /// limitation of compaction, not a soundness hole — the net never trusts a possibly-stale
  /// ConfChange.
  pub kind_unobservable_installs: u64,
  /// Async flush-phase witness: log stores the world made durable across the run. `0` under the
  /// sync store mode (the default/snapshot/reshape profiles), where the flush phase never runs;
  /// nonzero under the merge family — the proof the multi tick now fsync-flushes at all.
  pub log_flushes: u64,
  /// Async flush-phase witness: stable stores made durable across the run.
  pub stable_flushes: u64,
  /// Seeded torn writes (fsync failures) that stranded a REAL in-flight batch across the run — the
  /// lost-fsync coverage's non-vacuity witness. `0` under the sync store mode.
  pub torn_writes_fired: u64,
  /// Crashes that rolled back a NON-EMPTY log-store fsync window — proof the crash campaign lands
  /// mid-window (the interleaving lost-fsync durability actually depends on), not only post-flush.
  /// `0` under the sync store mode (nothing is ever in flight).
  pub crashes_with_log_inflight: u64,
  /// Crashes that rolled back a NON-EMPTY stable-store fsync window. `0` under the sync store mode.
  pub crashes_with_stable_inflight: u64,
  /// Applied cells the LINEAGE LEDGER's phantom-quorum leg judged across the run — its non-vacuity
  /// witness. Nonzero on any run that committed keyed load: the lineage-keyed agreement leg ran on
  /// real records rather than an empty world.
  pub lineage_cells_judged: u64,
  /// Snapshot installs the LINEAGE LEDGER's chimera leg examined across the run. `0` under a
  /// no-compaction default band (no transfer installs); nonzero under the snapshot/reshape/merge
  /// families where fork baselines and compaction snapshots transfer.
  pub lineage_installs_observed: u64,
}

/// One accepted-freeze entry in the fuzzer's pending-merge book.
struct PendingMerge {
  /// The target the accepted freeze claims.
  target: u64,
  /// Whether ANY replica of the source has been OBSERVED carrying the freeze (applied or
  /// append-pending) since booking. A booked propose whose entry died un-landed (leader churn
  /// truncated it) never trips this, and the quiesce teeth prune it as undrivable noise; a pair
  /// that DID freeze must resolve by run end or the wedge scan panics.
  saw_frozen: bool,
  /// The world's `(target, source)` abort clock at booking: the pair is resolved-by-abort once
  /// the clock moves past this (the absorb side retires via the source leaving the live set).
  abort_mark: u64,
}

/// The fuzzer's deterministic bookkeeping threaded through the run.
struct MState {
  /// The monotone group-id allocator (starts at 100; NEVER reused for a different logical
  /// group — `RecreateGroup` reuses a retired gid as the SAME logical group at gen+1).
  next_gid: u64,
  /// Accepted merge freezes not yet OBSERVED resolved: source → [`PendingMerge`] — the
  /// commit/rollback actions' pick book AND the quiesce drive's pair lookup. Entries retire on
  /// observed resolution only (the source merged away, or the world drained a `MergeAborted`
  /// for the pair past its booking mark) — NEVER on a rollback's propose-accept: an accepted
  /// abort that never commits leaves the source frozen, and a book that forgot the pair could
  /// neither drive nor attribute the wedge.
  pending_merges: BTreeMap<u64, PendingMerge>,
  /// Per-group journal of every accepted client command (the quiesce expected set).
  expected: BTreeMap<u64, Vec<Vec<u8>>>,
  /// The global monotone client-command counter (distinct commands; per-key values increase).
  cmd_counter: u64,
}

/// Run one deterministic multi-group VOPR episode. Panics (with seed + tick, each a real bug) on
/// a safety-oracle violation, a calm-window livelock, or a quiesce failure.
pub fn run_multi_vopr(seed: u64, ticks: usize, profile: MultiProfile) -> MultiVoprReport {
  let mut prng = FaultPrng::new(seed ^ 0x4D56_4F50_525F_5631); // "MVOPR_V1"
  // The tracked-wedge exemption is scoped to PHASED (storm) profiles: only there may the run-end
  // quiesce and the calm windows certify a group left in the tracked under-hosted parked-absorb
  // class (#106) instead of panicking. Every unphased profile has no phases, so this is `false` and
  // both liveness gates take the exact pre-seam path — the behavior-identity contract.
  let exempt_tracked = !profile.phases.is_empty();
  let nodes = 5 + (prng.next_u64() % 3); // 5..=7 hosts
  let mut w = MultiWorld::new(seed);
  w.set_snapshot_threshold(profile.snapshot_threshold);
  // The prevention layer, per-group (never a global const flip): reshaping-participant profiles
  // construct every replica with pre-vote + check-quorum, funneled through `wire_replica` so all
  // construction paths inherit it. Both `false` on the default profiles ⇒ byte-identical Configs.
  w.set_pre_vote(profile.pre_vote);
  w.set_check_quorum(profile.check_quorum);
  // The store write mode, funneled through the `fresh_stores` chokepoint so every construction path
  // inherits it. `Sync` on the non-merge profiles ⇒ the flush phase never runs and construction is
  // byte-identical; `Async` on the merge family opens the fsync-loss window the crash campaign needs.
  w.set_store_mode(profile.store_mode);
  for n in 0..nodes {
    w.add_node(n);
  }

  let mut st = MState {
    next_gid: 100,
    pending_merges: BTreeMap::new(),
    expected: BTreeMap::new(),
    cmd_counter: 0,
  };
  let mut reads = MultiReadLedger::new();
  let mut report = MultiVoprReport {
    seed,
    ..MultiVoprReport::default()
  };

  // Two initial groups (the non-vacuity floor) elected under a faultless bus, then the seeded
  // baseline faults install and the weighted loop takes over.
  create_group_action(&mut w, &mut st, &mut prng, &mut report);
  create_group_action(&mut w, &mut st, &mut prng, &mut report);
  // Witness the applied threshold off a genuinely constructed replica now that the world seam has
  // wired the initial groups. Read from a real replica config (not `profile.snapshot_threshold`)
  // so severing the `set_snapshot_threshold` call above is caught — a profile read would tautologize.
  report.applied_snapshot_threshold = w.applied_snapshot_threshold();
  for gid in w.live_groups() {
    assert!(
      w.run_until(3_000, |w| w.leader_of(gid).is_some()),
      "MULTI VOPR LIVELOCK (initial-election): group {gid} elected no leader from a clean \
       start\n  seed={seed}"
    );
  }
  let baseline = roll_network_faults(&mut prng);
  w.set_network_faults(baseline, seed.rotate_left(16) ^ 0x004E_4554);
  reroll_storage(&mut w, &mut prng, seed);

  let calm_period = 60 + (prng.next_u64() % 60) as usize;
  let mut next_calm = calm_period;

  for iter in 0..ticks {
    for gid in w.live_groups() {
      w.reconcile_membership(gid);
    }
    let action = pick_action(&mut prng, profile, iter);
    match action {
      MultiAction::ClientLoad => client_load(&mut w, &mut st, &mut prng, &mut report),
      MultiAction::ReadIndexLoad => {
        read_index_load(&mut w, &mut reads, &mut prng, &mut report);
      }
      MultiAction::Partition => partition(&mut w, &mut prng, &mut report),
      MultiAction::Heal => heal_one(&mut w, &mut prng, &mut report),
      MultiAction::Crash => crash_one(&mut w, &mut prng, &mut report),
      MultiAction::MuteGroup => mute_group(&mut w, &mut prng, &mut report),
      MultiAction::UnmuteGroup => unmute_group(&mut w, &mut prng, &mut report),
      MultiAction::ConfChange => conf_change(&mut w, &mut prng, &mut report),
      MultiAction::TransferLeader => transfer_leader(&mut w, &mut prng, &mut report),
      MultiAction::MigrateReadMode => migrate_read_mode(&mut w, &mut prng, &mut report),
      MultiAction::FaultReroll => {
        let net = roll_network_faults(&mut prng);
        let net_seed = prng.next_u64();
        w.set_network_faults(net, net_seed);
        reroll_storage(&mut w, &mut prng, seed);
        report.fault_rerolls += 1;
      }
      MultiAction::CreateGroup => create_group_action(&mut w, &mut st, &mut prng, &mut report),
      MultiAction::RemoveGroup => remove_group_action(&mut w, &mut prng, &mut report),
      MultiAction::RecreateGroup => recreate_group_action(&mut w, &mut prng, &mut report),
      MultiAction::Split => split_group(&mut w, &mut st, &mut prng),
      MultiAction::PrepareMerge => prepare_merge_action(&mut w, &mut st, &mut prng, &mut report),
      MultiAction::CommitMerge => commit_merge_action(&mut w, &mut st, &mut prng, &mut report),
      MultiAction::RollbackMerge => {
        rollback_merge_action(&mut w, &mut st, &mut prng, &mut report);
      }
    }

    let steps = 2 + (prng.next_u64() % 5) as usize; // 2..=6 ticks per iteration
    for _ in 0..steps {
      w.tick();
      report.ticks_run += 1;
    }
    // Track which booked freezes actually LANDED somewhere: the quiesce teeth drive every such
    // pair to resolution, and prune the ones whose accepted propose died un-landed.
    for (s, pm) in st.pending_merges.iter_mut() {
      if !pm.saw_frozen && w.group_freeze_seen(*s) {
        pm.saw_frozen = true;
      }
    }
    reads.scan(&w, &mut report, seed);
    report.max_term_seen = report.max_term_seen.max(w.max_term_all());
    report.faults_fired = w.net_dropped() + w.net_duplicated();

    if iter + 1 >= next_calm {
      calm_window(
        &mut w,
        &mut st,
        &mut prng,
        &mut report,
        seed,
        exempt_tracked,
      );
      report.calm_windows += 1;
      let jitter = (prng.next_u64() % 60) as usize;
      next_calm = iter + 1 + calm_period + jitter;
    }
  }

  quiesce(&mut w, &mut st, &mut report, seed, exempt_tracked);
  reads.scan(&w, &mut report, seed);
  // Membership-oracle VERDICT: the per-tick checks only RECORD snapshot-install observations;
  // the run-end final pass judges every one — across live groups AND retired archives — against
  // its group's FINAL, now-stable committed-config history. Panics on a mismatch AND on any
  // observation left without a verdict (skipped == 0, the single-group sweep's policy, enforced
  // per checker with gid/generation attribution); kind-unobservable declines are the tolerated
  // compaction class and only surface in the report.
  w.finalize_membership_or_panic(seed);
  // The conservation VERDICT: every registered split's key histories, judged against the
  // recorder's independent observations (see `MultiWorld::finalize_conservation_or_panic`).
  // Vacuously green under profiles that never split — zero records, zero cost.
  w.finalize_conservation_or_panic(seed);
  // The union VERDICT: every registered merge's source histories must open the target's copy
  // (see `MultiWorld::finalize_merge_conservation_or_panic`). Vacuously green under profiles
  // that never merge — zero records, zero cost.
  w.finalize_merge_conservation_or_panic(seed);
  // The LINEAGE VERDICT: durable state stayed single-lineage (chimera), every within-lineage
  // quorum agreed byte-for-byte (phantom), and every admitted snapshot transfer terminated
  // (wedge) — see `MultiWorld::finalize_lineage_or_panic`. Fed by the per-tick lineage sweep and
  // the install-event drain, so every seed of every profile faces it.
  w.finalize_lineage_or_panic(seed);
  report.lineage_cells_judged = w.lineage_cells_judged();
  report.lineage_installs_observed = w.lineage_installs_observed();
  report.merges_registered = w.merges_registered();
  report.merges_resolved = w.merges_resolved();
  report.merges_aborted = w.merges_aborted();
  report.merges_capture_failed = w.merges_capture_failed();
  report.membership_oracle_comparisons = w.membership_oracle_comparisons();
  report.skipped_unwitnessed_installs = w.skipped_unwitnessed_installs();
  report.kind_unobservable_installs = w.kind_unobservable_installs();
  report.splits_applied = w.splits_applied();
  report.final_groups = w.live_groups().len();
  report.cross_talk_checks = w.cross_talk_checked();
  report.max_term_seen = report.max_term_seen.max(w.max_term_all());
  report.faults_fired = w.net_dropped() + w.net_duplicated();
  // The async crash-suite non-vacuity witnesses (all 0 under the sync store mode): the flush phase
  // ran, torn writes stranded real batches, and crashes landed mid-window.
  report.log_flushes = w.log_flushes();
  report.stable_flushes = w.stable_flushes();
  report.torn_writes_fired = w.torn_writes_fired();
  report.crashes_with_log_inflight = w.crashes_with_log_inflight();
  report.crashes_with_stable_inflight = w.crashes_with_stable_inflight();
  report
}

/// Open a CALM WINDOW: back the adversary off entirely (heal every partition, clear every mute
/// and fault, restore poisoned nodes) and assert EVERY live group elects and commits fresh load
/// within a generous bound — failure is a livelock, and panics with seed + tick.
fn calm_window(
  w: &mut MultiWorld,
  st: &mut MState,
  prng: &mut FaultPrng,
  report: &mut MultiVoprReport,
  seed: u64,
  exempt_tracked: bool,
) {
  for node in w.isolated_nodes() {
    w.heal(node);
  }
  w.unmute_all();
  w.set_network_faults(crate::NetworkFaults::none(), seed);
  clear_storage_faults(w, seed);
  for node in w.poisoned_nodes() {
    w.crash(node);
  }

  for gid in w.live_groups() {
    // The window's own ticking resolves parked merges: a group absorbed mid-window leaves the
    // live set (its replicas dismantle host by host) — demanding an election or fresh load
    // from it would misread the designed teardown as a livelock.
    if !w.live_groups().contains(&gid) {
      continue;
    }
    w.reconcile_membership(gid);
    let mut elected = false;
    for _ in 0..4_000 {
      if !w.live_groups().contains(&gid) {
        break;
      }
      if w.leader_of(gid).is_some() {
        elected = true;
        break;
      }
      w.tick();
      report.ticks_run += 1;
      w.reconcile_membership(gid);
    }
    if !w.live_groups().contains(&gid) {
      continue;
    }
    if !elected {
      // A FILED merge-liveness class — the under-hosted parked-absorb (#106) or the fork-fence
      // coupling (#110) — can leave a merge participant unable to elect. Under a storm profile these
      // are certified past, not fresh livelocks; skip the ELECTION demand (it stays fully armed for
      // every other group, so a genuine non-merge livelock still trips here). Unphased profiles pass
      // `false` and take the bare panic, byte-identically. SAFETY still runs unconditionally via the
      // full group-safety helper: even a leaderless exempted wedge's hosted replicas must pass
      // agreement, the absorbed cross-watermark per-index own-client-cell agreement, and applied-history
      // integrity — a liveness exemption never gates safety.
      if exempt_tracked
        && (w.tracked_underhosted_merge_wedge(gid) || w.fork_fence_coupled_wedge(gid))
      {
        // The expected set is built exactly as the quiesce sweep builds it, so this checkpoint's
        // integrity verdict is identical to run-end's — the point of catching a divergent replica
        // that is dismantled between here and quiesce and never reaches the run-end sweep.
        let expected: BTreeSet<Vec<u8>> = st
          .expected
          .get(&gid)
          .map(|v| v.iter().cloned().collect())
          .unwrap_or_default();
        assert_group_safety(w, gid, &expected, seed);
        continue;
      }
      panic!(
        "MULTI VOPR LIVELOCK (calm window): group {gid} failed to elect within 4000 ticks after \
         healing everything\n  seed={seed} tick={}\n  {}",
        w.ticks(),
        w.dbg_group(gid),
      );
    }
    w.reconcile_membership(gid);

    // Fresh PROGRESS: a majority of the group's committed voters must commit-and-apply new load.
    let voters: Vec<u64> = w.group_voters(gid).into_iter().collect();
    let quorum_applied = |w: &MultiWorld| -> usize {
      let mut lens: Vec<usize> = voters.iter().map(|&n| w.applied_of(n, gid).len()).collect();
      lens.sort_unstable_by(|a, b| b.cmp(a));
      lens.get(lens.len() / 2).copied().unwrap_or(0)
    };
    let target = quorum_applied(w) + 1 + (prng.next_u64() % 2) as usize;
    let mut budget = 6_000u32;
    while quorum_applied(w) < target {
      // A group FROZEN by a merge refuses writes BY DESIGN — the calm window must not demand
      // fresh progress from it (the refusal is the covered behavior; the merge's own liveness
      // is the resolution/rollback path, not client load). A merely PENDING freeze settles
      // into frozen (or thaws by truncation) within the healed window's ticking below; a group
      // absorbed mid-loop leaves the live set entirely. A storm profile additionally breaks on a
      // FILED merge-liveness class (#106 under-hosted or #110 fork-fence): a parked target holds its
      // apply at the merge boundary and cannot advance until the absorb resolves, so demanding fresh
      // committed load from it would misread the filed liveness gap as a livelock.
      if w.group_frozen(gid)
        || !w.live_groups().contains(&gid)
        || (exempt_tracked
          && (w.tracked_underhosted_merge_wedge(gid) || w.fork_fence_coupled_wedge(gid)))
      {
        break;
      }
      assert!(
        budget > 0,
        "MULTI VOPR LIVELOCK (calm window): group {gid} failed to commit fresh load within the \
         window\n  seed={seed} tick={}\n  {}",
        w.ticks(),
        w.dbg_group(gid),
      );
      w.reconcile_membership(gid);
      if w.leader_of(gid).is_some() {
        // Keyed load from the group's LIVE population (identical to the pre-population pick
        // while it is the full domain). A population emptied by an embedder-driven world verb
        // would still need committable calm load, so it falls back to an un-keyed marker —
        // unreachable under the action, whose split point always leaves both sides a key.
        let keys = w.group_keys_of(gid);
        let payload = if keys.is_empty() {
          st.cmd_counter.to_le_bytes().to_vec()
        } else {
          let key = keys[(st.cmd_counter % keys.len() as u64) as usize];
          encode_gkv(gid, key, st.cmd_counter)
        };
        if w.propose(gid, &payload).is_some() {
          st.expected.entry(gid).or_default().push(payload);
          st.cmd_counter += 1;
          report.proposals += 1;
        }
      }
      w.tick();
      report.ticks_run += 1;
      budget -= 1;
    }
    if w.live_groups().contains(&gid) {
      // The full safety helper (agreement, absorbed cross-watermark per-index own-client-cell
      // agreement, applied-history integrity) at the progress point, expected built exactly as the
      // quiesce sweep builds it: a replica dismantled between this checkpoint and quiesce escapes the
      // run-end sweep, so this is its last judge — agreement alone would let a divergent such replica slip.
      let expected: BTreeSet<Vec<u8>> = st
        .expected
        .get(&gid)
        .map(|v| v.iter().cloned().collect())
        .unwrap_or_default();
      assert_group_safety(w, gid, &expected, seed);
    }
  }
}

/// QUIESCE: fully heal the world and drain every LIVE group to convergence — a single leader,
/// agreement, every member fully caught up and unpoisoned — then assert each group's applied
/// client history is exactly a subset of what the fuzzer proposed FOR THAT GROUP, applied
/// identically on every member. Panics with seed on failure.
fn quiesce(
  w: &mut MultiWorld,
  st: &mut MState,
  report: &mut MultiVoprReport,
  seed: u64,
  exempt_tracked: bool,
) {
  for node in w.isolated_nodes() {
    w.heal(node);
  }
  w.unmute_all();
  w.set_network_faults(crate::NetworkFaults::none(), seed);
  clear_storage_faults(w, seed);
  for node in w.poisoned_nodes() {
    w.crash(node);
  }

  // FREEZE TEETH, phase 1 — the drive: every group still carrying freeze state is driven to
  // RESOLUTION under the healed world, bounded. A freeze is a claim on a specific target; its
  // ONLY exits are the absorb and the abort, both proposed on that target's log — so propose
  // the booked commit (folding the source's expected history into the target's, as the action
  // does) and, when it refuses typed, the rollback release valve; the ticking pumps parks and
  // relays thaws. Anything still frozen when the budget runs out falls through to the wedge
  // scan below. Booked pairs that never landed a freeze anywhere are pruned first — an
  // accepted propose whose entry died with a deposed leader has nothing to resolve.
  st.pending_merges
    .retain(|s, pm| w.live_groups().contains(s) && (pm.saw_frozen || w.group_freeze_seen(*s)));
  let mut budget = 12_000u32;
  while budget > 0 {
    prune_merge_book(w, st);
    let active: Vec<u64> = w
      .live_groups()
      .into_iter()
      .filter(|&g| w.group_freeze_seen(g))
      .collect();
    if active.is_empty() {
      break;
    }
    if budget.is_multiple_of(16) {
      for gid in active {
        let target = st
          .pending_merges
          .get(&gid)
          .map(|pm| pm.target)
          .or_else(|| w.claimed_target_of(gid));
        let Some(target) = target else { continue };
        // A parked target is already deciding this merge — the ticking resolves it.
        if !w.live_groups().contains(&target) || w.group_merge_parked(target) {
          continue;
        }
        if matches!(w.propose_commit_merge(target, gid), Some(Ok(_))) {
          let moved: Vec<Vec<u8>> = st.expected.get(&gid).cloned().unwrap_or_default();
          if !moved.is_empty() {
            st.expected.entry(target).or_default().extend(moved);
          }
          report.merges_committed += 1;
        } else if matches!(w.propose_rollback_merge(target, gid), Some(Ok(_))) {
          report.merges_rolled_back += 1;
        }
      }
    }
    for gid in w.live_groups() {
      w.reconcile_membership(gid);
    }
    w.tick();
    report.ticks_run += 1;
    budget -= 1;
  }

  let converged_group = |w: &MultiWorld, gid: u64| -> bool {
    if w.group_leader_count(gid) != 1 || !w.agreement_holds(gid) {
      return false;
    }
    // A merge-parked target is NOT converged: its apply is pinned at the park boundary while its
    // commit races ahead, so the caught-up check below would read its members as equally applied
    // though the group is wedged. A resolving park clears inside the quiesce ticking; a permanent
    // one is a livelock this refuses to certify.
    if w.group_merge_parked(gid) {
      return false;
    }
    let members: BTreeSet<u64> = w
      .group_voters(gid)
      .into_iter()
      .chain(w.group_learners(gid))
      .collect();
    if members.is_empty() {
      return false;
    }
    let lens: Vec<usize> = members
      .iter()
      .map(|&n| w.applied_of(n, gid).len())
      .collect();
    let caught_up = lens.iter().min() == lens.iter().max();
    caught_up && members.iter().all(|&n| w.hosts_group(n, gid))
  };
  // Recomputed every pass: the quiesce ticking itself resolves parked merges, and an absorbed
  // group leaves the live set as its replicas dismantle.
  let mut live = w.live_groups();
  let mut converged = false;
  for pass in 0..20_000u32 {
    live = w.live_groups();
    for &gid in &live {
      w.reconcile_membership(gid);
    }
    if pass % 64 == 0 {
      for node in w.poisoned_nodes() {
        w.crash(node);
      }
    }
    w.tick();
    report.ticks_run += 1;
    live = w.live_groups();
    // A storm profile certifies convergence with the two FILED merge-liveness classes exempted —
    // the under-hosted parked-absorb (#106) and the fork-fence coupling (#110), each the whole merge
    // component transitively blocked by its root. Unphased profiles pass `exempt_tracked = false`, so
    // both sets are empty (never computed) and this is the bare `all(converged_group)` — byte-identical.
    let wedge = if exempt_tracked {
      let mut s = w.tracked_merge_wedge_set();
      s.extend(w.fork_fence_wedge_set());
      s
    } else {
      BTreeSet::new()
    };
    if live
      .iter()
      .all(|&gid| converged_group(w, gid) || wedge.contains(&gid))
    {
      converged = true;
      break;
    }
  }
  // `live` holds the loop's last pass (no ticks since — current). Recompute both filed components
  // once (live groups AND retired frozen husks): the union is what the convergence certification and
  // the freeze-wedge scan read; the two sizes are the seed's per-class witnesses, counted APART so
  // neither class can hide the other. Empty on a fully-converged run and on every unphased profile.
  let (underhosted, forkfence) = if exempt_tracked {
    (w.tracked_merge_wedge_set(), w.fork_fence_wedge_set())
  } else {
    (BTreeSet::new(), BTreeSet::new())
  };
  let exempted: BTreeSet<u64> = underhosted.union(&forkfence).copied().collect();
  // Count BOTH raw predicate sets INDEPENDENTLY: a group satisfying both predicates contributes to
  // both counters (and both seed lists in the sweep), so neither class can hide the other. The
  // `exempted` union is what the liveness gate certifies past; the raw counters are the per-class
  // witnesses; the overlap is reported explicitly so the two are never read as disjoint.
  report.tracked_merge_wedges_exempted += underhosted.len() as u64;
  report.fork_fence_couplings_exempted += forkfence.len() as u64;
  report.exemption_overlap += underhosted.intersection(&forkfence).count() as u64;
  if !converged {
    // Only a NON-exempt group that failed to converge is a wedge worth panicking on; if every
    // straggler is the tracked class the loop already certified and we never reach here.
    let stuck: Vec<u64> = live
      .iter()
      .copied()
      .filter(|&gid| !converged_group(w, gid) && !exempted.contains(&gid))
      .collect();
    let dumps: Vec<String> = stuck
      .iter()
      .map(|&gid| {
        std::format!(
          "g{gid}: {}\n    merge-block: {}",
          w.dbg_group(gid),
          w.merge_block_dbg(gid)
        )
      })
      .collect();
    panic!(
      "MULTI VOPR QUIESCE FAILURE: a fully-healed world failed to converge every live group \
       within 20000 ticks\n  seed={seed} (replay: run_multi_vopr({seed}, ticks, profile))\n  \
       stuck (non-exempt): {stuck:?}\n  {}",
      dumps.join("\n  "),
    );
  }

  // FREEZE TEETH, phase 2 — the wedge scan: a replica still FROZEN after the drive and full
  // convergence is a stranded merge participant (a frozen group converges — freeze refuses only
  // writes — so the convergence pass above cannot see this class). Every accepted freeze must
  // resolve by run end; anything else is a product wedge, seed-attributed.
  // Frozen replicas whose group is the tracked under-hosted parked-absorb class (#106) are exempt
  // under a storm profile — the source cannot be absorbed while the target conf lacks a live host
  // quorum. Every other frozen replica is still a stranded participant that panics. Unphased
  // profiles filter nothing (the predicate never runs), so the scan is byte-identical.
  let frozen: Vec<(u64, u64)> = w
    .frozen_replicas()
    .into_iter()
    .filter(|&(_, gid)| !(exempt_tracked && exempted.contains(&gid)))
    .collect();
  if !frozen.is_empty() {
    let groups: BTreeSet<u64> = frozen.iter().map(|&(_, gid)| gid).collect();
    let dumps: Vec<String> = groups
      .iter()
      .map(|&gid| {
        std::format!(
          "g{gid}: {}\n    merge-block: {}",
          w.dbg_group(gid),
          w.merge_block_dbg(gid)
        )
      })
      .collect();
    panic!(
      "MULTI VOPR FREEZE WEDGE: replicas still frozen at run end after the quiesce drive\n  \
       seed={seed} (replay: run_multi_vopr({seed}, ticks, profile))\n  frozen (node, gid): \
       {frozen:?}\n  {}",
      dumps.join("\n  "),
    );
  }

  // SAFETY runs UNCONDITIONALLY over every HOSTED gid — a liveness exemption (a tracked #106/#110
  // wedge) gates only the CONVERGENCE demands below, NEVER safety (agreement + absorbed cross-watermark
  // + applied-history integrity; see [`assert_group_safety`]). The worklist is the union over hosts,
  // RETIRED HUSKS INCLUDED: `live_groups` omits a merged-away source's frozen replica lingering after
  // other hosts resolved, but the wedge sets contain it and its hosted replicas must still be judged.
  let live_set: BTreeSet<u64> = live.iter().copied().collect();
  for gid in w.all_hosted_groups() {
    if !live_set.contains(&gid) {
      report.retired_husks_safety_checked += 1;
    }
    let expected: BTreeSet<Vec<u8>> = st
      .expected
      .get(&gid)
      .map(|v| v.iter().cloned().collect())
      .unwrap_or_default();
    assert_group_safety(w, gid, &expected, seed);
  }

  // CONVERGENCE-dependent checks: leader-anchored caught-up equality, over LIVE groups only. An
  // exempted wedge never converged (leaderless / members not caught up by design), so this does not
  // apply to it — it is already counted in the exemption witnesses. On an unphased profile `exempted`
  // is empty and every live group takes this pass, byte-identical to before.
  for &gid in &live {
    if exempted.contains(&gid) {
      continue;
    }
    let leader = w.leader_of(gid).expect("quiesce converged to a leader");
    let leader_applied = w.applied_of(leader, gid);
    let committed = leader_applied
      .iter()
      .filter(|(_, cmd)| !cmd.is_empty())
      .count() as u64;
    report.committed += committed;
    let leader_cmds: Vec<&Vec<u8>> = leader_applied
      .iter()
      .filter(|(_, cmd)| !cmd.is_empty())
      .map(|(_, cmd)| cmd)
      .collect();
    let members: BTreeSet<u64> = w
      .group_voters(gid)
      .into_iter()
      .chain(w.group_learners(gid))
      .collect();
    for member in members {
      let applied = w.applied_of(member, gid);
      let mut cmds: Vec<&Vec<u8>> = applied
        .iter()
        .filter(|(_, cmd)| !cmd.is_empty())
        .map(|(_, cmd)| cmd)
        .collect();
      let mut expected_cmds = leader_cmds.clone();
      // A merged lineage's record ORDER varies by arrival path (live fold vs capture restore);
      // the converged CONTENT is what the product promises — compare as multisets there.
      if w.group_absorbed(gid) {
        cmds.sort();
        expected_cmds.sort();
      }
      assert_eq!(
        cmds, expected_cmds,
        "MULTI VOPR APPLY FAILURE: group {gid} member {member} applied a different committed \
         client history than leader {leader}\n  seed={seed}",
      );
    }
  }
}

/// The UNCONDITIONAL per-group safety pass the quiesce runs on EVERY hosted replica of a group,
/// exempt or not — a liveness exemption (a #106 under-hosted or #110 fork-fence wedge) gates the
/// CONVERGENCE demands, NEVER this. Three legs, none needing a leader (an exempted wedge may be
/// leaderless), so all read the hosting replicas directly:
///   1. AGREEMENT — the hosting replicas' applied records agree (State Machine Safety, aligned
///      across splits/absorbs): positionally over the shared prefix for a live lineage, as sorted
///      multisets at equal watermarks for an absorbed one,
///   2. ABSORBED CROSS-WATERMARK — an absorbed lineage's replicas at UNEQUAL watermarks (where
///      `agreement_holds`' equal-applied absorbed branch compares nothing) must AGREE at every log
///      index they share over their OWN CLIENT (gkv) cells (keyed on index, not position — the arrival
///      path can reorder; cross-watermark non-gkv content is not judged here), and
///   3. INTEGRITY — no hosted replica applied a client command absent from `expected` (the set the
///      fuzzer proposed for the group).
///
/// Panics with `seed` on a violation. Extracted so the exemption-does-not-gate-safety contract is
/// unit-testable directly on a constructed exempted wedge.
pub(crate) fn assert_group_safety(
  w: &MultiWorld,
  gid: u64,
  expected: &BTreeSet<Vec<u8>>,
  seed: u64,
) {
  assert!(
    w.agreement_holds(gid),
    "MULTI VOPR AGREEMENT FAILURE: group {gid} hosted replicas disagree (exempt or not)\n  \
     seed={seed}",
  );
  // ABSORBED-lineage coverage, SCOPED honestly. `agreement_holds`' absorbed branch compares only
  // EQUAL-applied replicas, so an exempted merge wedge (replicas at UNEQUAL watermarks) needs a
  // cross-watermark leg. What runs here is per-index agreement over the group's OWN CLIENT (gkv) cells
  // at shared indices — and a RETIRED source's husk replicas align against their TERMINAL pre-merge
  // population (see `aligned_applied`), so their client cells are judged here rather than blanked by the
  // emptied live set. Standard choreography converges tracked husks to the freeze coordinate — the
  // every-peer barrier acks DURABLE state and the settle loop coalesces commit and apply, so husk pairs
  // sit at EQUAL applied in this world (asserted in the husk regressions). A below-freeze
  // matched-but-not-applied husk is a REAL-SYSTEM shape this model closes, so this leg's unequal-husk
  // coverage is red-proofed at the relation level (the synthetic shared-index divergence and reorder
  // cases), where for an absorbed source it is that shape's only client-content judge. What is
  // DELIBERATELY NOT
  // asserted at the per-replica cross-watermark grain is ABSORBED-content completeness: a record-keyed
  // "every replica past a merge boundary holds the complete absorbed block" form was built and
  // rejected because it false-trips on legitimate world behavior the accumulating ledger tolerates — a
  // target that SPLITS after absorbing sends absorbed cells to a child that RETURN via a later merge,
  // so a lower-watermark (but past-boundary) replica legitimately lacks them until it applies the
  // return, and a COMPACTED replica drops them from its `applied()` view. Per-replica departed/return/
  // compaction state is not cleanly accountable at this grain. The absorbed CONTENT is instead covered
  // by: (1) `agreement_holds`' absorbed branch — replicas at the SAME watermark must hold the same
  // MULTISET of raw cells, compared order-insensitively (the branch sorts each record first; absorbed
  // cells included); (2) the run-end `finalize_merge_conservation_or_panic` — per merge and per
  // absorbed key, every value of the source's run-end history, MINUS those also appearing in the
  // run-end history of any split child of that source that took the key, must appear in the target's
  // (unordered value containment — `assert_union` checks membership, never position). That exemption
  // is a SUPERSET of true departures — the child's accumulated history also holds cells the child
  // originated or inherited after the split — and is deliberately not narrowed (the dedup-by-value
  // ledger cannot tell a genuine return to the FSM from record inheritance; narrowing would re-demand
  // legitimately departed cells — see the ledger's own note). NOT checked, then: an absorbed-suffix
  // divergence between replicas at DIFFERENT watermarks; cross-watermark NON-GKV content (a fold keeps
  // non-gkv source cells at their SOURCE index, so index collisions there are representational, not
  // divergence — covered at equal watermarks by (1) and by the integrity leg); the ORDER of absorbed
  // content anywhere post-absorb (EVERY leg is order-insensitive — (1) and (2) compare unordered and
  // this cross-watermark leg keys on log index over own-gkv cells; cells at an index present on only
  // ONE side are not judged here — the run-end conservation ledger owns loss); and all-replica loss of
  // ANY value sitting in both the source's and a matching
  // child's history when it rides a later merge — the departed → returned-via-merge-back → re-merged
  // chain, and equally a child-born or child-inherited cell that entered the source via a merge-back
  // (exempt from (2)'s demand, invisible to (1) when every replica drops it alike).
  if w.group_absorbed(gid) {
    assert!(
      w.absorbed_lineage_client_cells_agree_at_shared_indices(gid),
      "MULTI VOPR AGREEMENT FAILURE: group {gid} absorbed replicas diverge across watermarks — own \
       cells (exempt or not)\n  seed={seed}",
    );
  }
  for node in w.hosting_nodes(gid) {
    for (_, cmd) in w.applied_of(node, gid) {
      if cmd.is_empty() {
        continue; // empty / conf entries carry no client payload
      }
      assert!(
        expected.contains(&cmd),
        "MULTI VOPR INTEGRITY FAILURE: group {gid} node {node} applied a command {cmd:?} the \
         fuzzer never proposed for it\n  seed={seed}",
      );
    }
  }
}

#[cfg(test)]
mod tests;

// Action bodies + the seeded pick helpers, split by concern.
mod actions;
mod ledger;
use actions::*;
use ledger::MultiReadLedger;
