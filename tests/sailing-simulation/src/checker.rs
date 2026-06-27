//! The per-tick safety-oracle suite — the heart of the VOPR's verification power.
//!
//! An oracle that only checks applied-log agreement is "structurally blind" to durability bugs:
//! it can miss a commit-persistence bug — a node that advances commit without the entry on a
//! quorum of durable logs — entirely. This module fixes that: it consolidates the existing oracles
//! and adds the durability/commit class into a suite of
//! **pure functions** that run on EVERY tick.
//!
//! # Design
//!
//! Each oracle is a pure function of a read-only [`ClusterView`] (a snapshot of the cluster's
//! observable state, copied out by [`Cluster::view`](crate::Cluster) at the end of every tick) and
//! returns `Result<(), Violation>`. The oracles that need cross-time history (commit/term
//! monotonicity, the committed-history high-water, no-committed-rewrite) read and update the
//! [`Checker`]'s small per-node state. A [`Violation`] carries the oracle name + a human-readable
//! detail; the driver [`panic`]s with the violation, the cluster SEED, and the current TICK so the
//! VOPR can replay the exact failing run.
//!
//! # The oracle suite (what each catches)
//!
//! | Oracle | Catches |
//! |---|---|
//! | [`agreement`] | divergent applied logs across nodes (State Machine Safety, prefix form) |
//! | [`append_before_ack`] | a node acking an entry it has not durably stored |
//! | [`commit_is_quorum_durable`] | a node advancing commit without the entry on a quorum of durable logs (the heartbeat class) |
//! | [`monotonic_commit`] | a node's commit going backward across ticks (incl. across restart) |
//! | [`no_committed_rewrite`] | a previously-committed `(index→command)` being overwritten (the strongest State Machine Safety check) |
//! | [`term_monotonic`] | a node's term going backward across ticks |
//! | [`durable_prefix`] | a restarted node silently forgetting the committed prefix it durably stored (the headline durability bug) |
//! | [`boundedness`] | per-node bookkeeping growing unboundedly under compaction (a GC/compaction failure) |
//! | [`snapshot_boundary_coherent`] | an installed snapshot whose boundary term disagrees with the committed log at that index (a corrupt/mis-keyed snapshot transfer) |
//! | [`finalize_membership`] | a snapshot-installed node whose installed membership disagrees with the committed-config history at its snapshot boundary (a phantom voter / missing joiner from a corrupt snapshot `ConfState`) |
//!
//! The suite is a **pure observer**: it never draws from a PRNG and never mutates the simulated
//! nodes/stores, so the run is byte-identical with or without it (determinism preserved).

use std::{
  collections::{BTreeMap, BTreeSet},
  string::String,
  vec::Vec,
};

/// A safety-oracle violation: which oracle tripped and a human-readable detail.
///
/// `oracle` is a stable `&'static str` name (matches the function name) so callers can match on
/// it; `detail` carries the offending node ids / indices / commands for diagnosis. The driver
/// formats this together with the cluster seed + tick into the panic message for VOPR replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
  /// The stable name of the oracle that tripped (e.g. `"agreement"`, `"durable_prefix"`).
  pub oracle: &'static str,
  /// A human-readable description of the violation (offending nodes/indices/commands).
  pub detail: String,
}

impl Violation {
  /// Construct a violation for `oracle` with `detail`.
  fn new(oracle: &'static str, detail: impl Into<String>) -> Self {
    Self {
      oracle,
      detail: detail.into(),
    }
  }
}

impl core::fmt::Display for Violation {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "[{}] {}", self.oracle, self.detail)
  }
}

/// One durable log entry as observed by the checker: `(index, term, command-bytes)`.
///
/// Read from [`MemLog::durable_entries`](crate::MemLog::durable_entries) — the non-faulting seam,
/// so observing it never perturbs the run. Only durable (flushed) entries appear; staged
/// (un-flushed) appends are invisible, exactly as the proto's read view sees them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableEntry {
  /// The entry's 1-based log index.
  pub index: u64,
  /// The entry's term.
  pub term: u64,
  /// The entry's payload bytes.
  pub data: Vec<u8>,
  /// Whether this entry is a `ConfChange`. The authoritative kind the committed-config history's tombstone
  /// needs: a recorded ConfChange transition at an index is valid only if the FINAL committed entry there is
  /// itself a ConfChange. A higher-term NON-ConfChange (Normal/Empty/SetReadMode) that truncated and superseded
  /// an in-memory-applied ConfChange emits NO `ConfChanged` event, so the event-sourced history would otherwise
  /// keep the stale transition (see `finalize_membership`).
  pub is_conf_change: bool,
}

/// A read-only snapshot of one node's observable state at a tick boundary.
///
/// Every field is copied out via a PUBLIC accessor (minimized surface): the proto's
/// `commit_index()`/`applied_index()`/`term()`/`role()`/`state_machine()`/`is_poisoned()`, and the
/// sim store's non-faulting durable-read seams. Nothing here reaches into proto internals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeView {
  /// The node id.
  pub id: u64,
  /// Whether the node has been removed from the cluster (its applied log legitimately stops
  /// advancing at removal, so the cross-node oracles skip it).
  pub removed: bool,
  /// Whether the node considers ITSELF a voter in its own committed configuration
  /// (`Endpoint::conf_state().is_voter(id)`). A learner or a freshly-wired joiner is `false` here.
  ///
  /// This is the per-node self-view. The quorum-durability oracle's PRIMARY denominator is the
  /// cluster-wide [authoritative committed voter set](ClusterView::committed_voters) (the leader's
  /// `conf_state().voters()`); this self-report is the FALLBACK population used only when no
  /// authoritative set is available (an empty/all-removed cluster, or a direct-constructed synthetic
  /// view in the oracle teeth tests). Counting voters — not all live nodes — is what keeps the oracle
  /// sound under reconfiguration: only voters ack toward commit, and a node becomes a voter only by
  /// applying the `AddNode` that adds it (which requires a durable log up to that conf-change's index,
  /// hence covering every earlier committed entry).
  pub is_voter: bool,
  /// Whether the node is poisoned (a fatal storage/apply error made it inert). A poisoned node's
  /// watermarks are frozen; the monotonicity oracles still treat it as a normal observation (a
  /// frozen value never regresses), but it is excluded from liveness-flavored checks.
  pub poisoned: bool,
  /// Whether the node currently believes itself leader.
  pub is_leader: bool,
  /// The node's current term (`Endpoint::term()`).
  pub term: u64,
  /// The node's in-memory commit watermark (`Endpoint::commit_index()`).
  pub commit: u64,
  /// The node's in-memory applied watermark (`Endpoint::applied_index()`).
  pub applied: u64,
  /// The node's applied `(index, command)` sequence (`Endpoint::state_machine().applied()`).
  pub applied_log: Vec<(u64, Vec<u8>)>,
  /// The node's DURABLE (fsync'd) log `first_index` (advances after compaction). In async mode this
  /// is the durable snapshot's first index; a submitted-but-unflushed tail is excluded.
  pub durable_first: u64,
  /// The node's DURABLE (fsync'd) log `last_index`. In async mode this is the durable snapshot's
  /// last index — a submitted-but-unflushed append (visible to the proto's reads) is NOT counted
  /// here, since a crash before flush would lose it.
  pub durable_last: u64,
  /// The node's VISIBLE log `last_index` (`Endpoint`-readable state). In async mode this INCLUDES a
  /// submitted-but-unflushed tail (≥ [`durable_last`](Self::durable_last)); in sync mode the two
  /// coincide. The proto applies committed entries from this visible view (a node can only apply
  /// what it can read), so the `applied <= visible_last` sanity bound uses this, while durability
  /// oracles use `durable_last`.
  pub visible_last: u64,
  /// The node's durable entries (`durable_first..=durable_last`), read via the non-faulting seam.
  pub durable_entries: Vec<DurableEntry>,
  /// The `last_index` of the node's durable snapshot (or `0` if none). Entries `<=` this are
  /// covered by the snapshot even though they are compacted out of `durable_entries`.
  pub snapshot_last_index: u64,
  /// The boundary term of the node's durable snapshot (or `0` if none).
  pub snapshot_last_term: u64,
  /// Whether this node's active membership is SNAPSHOT-DERIVED — it installed a TRANSFERRED snapshot, so
  /// its config came (at least up to the snapshot boundary) from a wire `ConfState` rather than purely by
  /// applying the committed log. STICKY across crash (sourced from `Cluster`'s durable lineage flag, not the
  /// resettable install counter): a restarted node recovers its membership from the durable snapshot, so the
  /// provenance survives. The membership-coherence oracle treats such a node as the one UNDER TEST and
  /// NEVER as the sound (log-built) reference — using a snapshot-derived node as the witness would be
  /// circular and could mask a corrupt snapshot. A locally-COMPACTED node (durable snapshot present but
  /// never transfer-installed) has `false` here — its config is log-built, so it is a valid witness.
  pub installed_snapshot: bool,
  /// The node's ACTIVE incoming voter set (`Endpoint::conf_state().voters()`) — the current-config (or
  /// joint incoming) half. Folded from every membership change the node has applied (a snapshot install
  /// adopts the snapshot's `ConfState` verbatim), so it is the membership the node actually serves with.
  pub conf_voters: BTreeSet<u64>,
  /// The node's ACTIVE outgoing voter set (`conf_state().voters_outgoing()`) — non-empty only mid joint
  /// transition. Compared so a corrupt snapshot can't smuggle a divergent joint half.
  pub conf_voters_outgoing: BTreeSet<u64>,
  /// The node's ACTIVE learner set (`conf_state().learners()`). Compared alongside the voter halves so a
  /// mis-keyed snapshot `ConfState` that drops/adds a learner is also caught.
  pub conf_learners: BTreeSet<u64>,
  /// The node's ACTIVE `learners_next` set (`conf_state().learners_next()`) — the outgoing-only voters
  /// staged for demotion to learner when a joint config LEAVES. Part of the full `ConfState`, so it is
  /// compared too: a snapshot that corrupts the staged demotions while the other halves match would
  /// otherwise slip past.
  pub conf_learners_next: BTreeSet<u64>,
  /// The node's ACTIVE `auto_leave` flag (`conf_state().auto_leave()`) — whether the leader will
  /// auto-append the leave-joint entry. Part of the full `ConfState`, so it is compared too: a snapshot
  /// that flips `auto_leave` while every membership set matches would otherwise slip past.
  pub conf_auto_leave: bool,
  /// The count of `Event::ConfChanged` this node has applied (`Cluster::conf_changed_count`). For a
  /// LOG-BUILT node this exactly counts the applied conf-changes; the committed-config history uses it to
  /// detect a conf-change-free applied delta (the config is then constant across the whole delta, so the
  /// history can be filled for every committed index the node skipped over in one batched apply).
  pub conf_changed: u64,
  /// The node's durable `HardState.commit` — the durably-persisted commit watermark. The
  /// durability invariant relates the recovered in-memory `commit` to this.
  pub hardstate_commit: u64,
  /// The number of staged (un-flushed) store writes (fsync window). Bounded-bookkeeping check.
  pub inflight_staged: usize,
  /// The node's restart count ("incarnation") — bumped each time the node crashes and recovers from
  /// durable storage. A change signals a restart boundary at which the commit/term monotonicity
  /// baseline is legitimately reset: the batched commit/term persist can lose an in-memory advance
  /// still in the fsync window on crash, and the restarted node re-derives it.
  pub incarnation: u64,
}

/// A full cluster configuration snapshot — the five [`ConfState`](sailing_proto::ConfState) fields. The
/// committed-config history records one per committed CONF-CHANGE index (the exact index the new config took
/// effect, taken from the `Event::ConfChanged` of a log-built node); an install record stores the membership a
/// snapshot embedded (from its `SnapshotMeta`); and [`finalize_membership`] compares the install's
/// against the config in effect at the snapshot boundary. Comparing the WHOLE snapshot (not just the voter
/// halves) is what catches a corrupt `learners_next` / `auto_leave`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConfSnapshot {
  voters: BTreeSet<u64>,
  voters_outgoing: BTreeSet<u64>,
  learners: BTreeSet<u64>,
  learners_next: BTreeSet<u64>,
  auto_leave: bool,
}

impl ConfSnapshot {
  /// Capture a [`ConfState`](sailing_proto::ConfState)'s five fields. Used by the cluster to record a
  /// committed conf-change (from an `Event::ConfChanged`) into the committed-config history.
  pub(crate) fn from_conf_state(cs: &sailing_proto::ConfState<u64>) -> Self {
    Self {
      voters: cs.voters().clone(),
      voters_outgoing: cs.voters_outgoing().clone(),
      learners: cs.learners().clone(),
      learners_next: cs.learners_next().clone(),
      auto_leave: cs.auto_leave(),
    }
  }
}

impl NodeView {
  /// This node's active configuration as a [`ConfSnapshot`] (all five `ConfState` fields).
  fn conf_snapshot(&self) -> ConfSnapshot {
    ConfSnapshot {
      voters: self.conf_voters.clone(),
      voters_outgoing: self.conf_voters_outgoing.clone(),
      learners: self.conf_learners.clone(),
      learners_next: self.conf_learners_next.clone(),
      auto_leave: self.conf_auto_leave,
    }
  }

  /// Whether this node's durable state (in-memory entries OR snapshot) covers `index` — i.e. the
  /// node has durably stored the committed entry at `index`. Used by the quorum-durability and
  /// committed-rewrite oracles to account for compaction.
  fn durable_covers(&self, index: u64) -> bool {
    index <= self.snapshot_last_index || (index >= self.durable_first && index <= self.durable_last)
  }

  /// The durable term at `index`, accounting for compaction: a snapshotted entry counts as
  /// durable-present at the snapshot boundary term. Returns `None` if the node does not durably
  /// cover `index`.
  fn durable_term(&self, index: u64) -> Option<u64> {
    if index >= self.durable_first && index <= self.durable_last {
      return self
        .durable_entries
        .iter()
        .find(|e| e.index == index)
        .map(|e| e.term);
    }
    // Compacted-away but covered by the snapshot: the only term we can attest is the snapshot
    // boundary's. A committed entry strictly below the boundary was committed before the snapshot,
    // so we treat the boundary term as its durable witness (the committing node's own term lookup
    // does the same via `term(offset)`).
    if index <= self.snapshot_last_index {
      return Some(self.snapshot_last_term);
    }
    None
  }

  /// The COMMITTED term this node attests at `index`, read STRICTLY from a retained durable log entry
  /// the node has ALSO committed (`index <= self.commit`) — never from a snapshot boundary, and never
  /// from an uncommitted tail. `None` if `index` is outside the retained window
  /// (`durable_first..=durable_last`) or above this node's commit watermark.
  ///
  /// This is the sound, non-circular term witness for [`snapshot_boundary_coherent`]:
  /// - NOT a snapshot boundary, because that boundary's own `last_term` is the value under test (a
  ///   circular witness could rubber-stamp a divergence).
  /// - NOT an uncommitted durable tail, because a node can durably hold an uncommitted entry at an index
  ///   (a stale tail later overwritten by the committed entry at a HIGHER term) — its term is NOT the
  ///   committed term and would false-positive against a correct boundary. Only an entry the node has
  ///   committed carries the one true committed term (a committed index has a unique term, so two
  ///   committed witnesses can never disagree).
  fn committed_log_term(&self, index: u64) -> Option<u64> {
    if index <= self.commit && index >= self.durable_first && index <= self.durable_last {
      return self
        .durable_entries
        .iter()
        .find(|e| e.index == index)
        .map(|e| e.term);
    }
    None
  }
}

/// A read-only snapshot of the whole cluster at a tick boundary, plus the seed/tick for replay.
#[derive(Debug, Clone)]
pub struct ClusterView {
  /// The cluster seed (for VOPR replay).
  pub seed: u64,
  /// The current tick/step number (for VOPR replay).
  pub tick: u64,
  /// The cluster's REAL committed VOTER set — the authoritative quorum denominator for
  /// `commit_is_quorum_durable`, read from the leader's runtime `conf_state().voters()` (or the
  /// plurality committed config when leaderless). Threading the leader's view (rather than each
  /// node's own `is_voter` self-report combined with the sim's `removed` flag) makes the oracle's
  /// denominator the proto's true committed membership: a node the sim prematurely marked removed —
  /// an accepted-but-never-committed RemoveNode — is still a real committed voter and durable
  /// witness, and a learner is correctly excluded.
  ///
  /// `None` only when the cluster could not derive a committed set (empty / all-removed); the oracle
  /// then falls back to the per-node `is_voter & !removed` population. Direct-constructed synthetic
  /// views (the oracle teeth tests) leave this `None` and rely on that fallback.
  pub committed_voters: Option<BTreeSet<u64>>,
  /// This tick's NEW committed conf-changes observed on LOG-BUILT nodes, as `(conf-change index, node term at
  /// apply, resulting ConfState)` taken from `Event::ConfChanged`. The membership oracle folds these into its
  /// persistent committed-config history (a step function keyed by conf-change index). Only log-built nodes
  /// contribute (a snapshot-derived node's config could be wire-corrupt). The term resolves a same-index
  /// conflict: a higher-term observation supersedes a lower-term in-memory apply that was later truncated.
  /// Empty on most ticks and for synthetic views that do not seed the history. Crate-internal.
  pub(crate) committed_transitions: Vec<(u64, u64, ConfSnapshot)>,
  /// This tick's NEW transfer-snapshot installs, as `(node id, snapshot boundary index, install-time
  /// ConfState)` from each `SnapshotInstalled` event's `SnapshotMeta`. The membership oracle accumulates these
  /// into its persistent OBSERVED-install map and compares each STORED install-time ConfState (never the
  /// node's current, drifting one) against the committed-config reference at the boundary. Crate-internal.
  pub(crate) new_installs: Vec<(u64, u64, ConfSnapshot)>,
  /// One [`NodeView`] per node, in node-position order.
  pub nodes: Vec<NodeView>,
}

impl ClusterView {
  /// Iterate the non-removed nodes (the cross-node oracles — agreement, no-committed-rewrite — operate
  /// on these: a learner must also agree on the committed prefix it has applied).
  fn live(&self) -> impl Iterator<Item = &NodeView> {
    self.nodes.iter().filter(|n| !n.removed)
  }

  /// Iterate the committed VOTERS — the quorum-durability denominator/witness population.
  ///
  /// When [`committed_voters`](Self::committed_voters) is `Some` (the production path: a leader
  /// exists or a plurality committed config was derived), membership is taken from that authoritative
  /// committed voter set — a node is a voter iff its id is in the set, regardless of its own
  /// `is_voter` self-report or the sim's `removed` flag. This is what makes the oracle independent of
  /// the harness's optimistic membership bookkeeping (a prematurely-`removed` real voter is still
  /// counted; a learner is excluded because it is not in the committed voter set).
  ///
  /// When `committed_voters` is `None` (an empty/all-removed cluster, or a direct-constructed
  /// synthetic view), it falls back to the per-node `!removed && is_voter` self-report.
  fn voters(&self) -> impl Iterator<Item = &NodeView> {
    self
      .nodes
      .iter()
      .filter(move |n| match &self.committed_voters {
        Some(set) => set.contains(&n.id),
        None => !n.removed && n.is_voter,
      })
  }

  /// The number of committed voters — the denominator for the durable-quorum threshold.
  ///
  /// When the authoritative [`committed_voters`](Self::committed_voters) set is known, the count is
  /// its cardinality (the TRUE voter population), not the number of matching `NodeView`s — so a
  /// momentarily-absent voter view can never shrink the quorum threshold and weaken the oracle's
  /// teeth. Otherwise it falls back to counting the per-node voter self-reports.
  fn voter_count(&self) -> usize {
    match &self.committed_voters {
      Some(set) => set.len(),
      None => self.voters().count(),
    }
  }
}

/// The kind of the committed entry observed at a log index, for the membership oracle's authoritative
/// committed-log record. A given `(index, term)` is a unique committed entry, so all committed-durable
/// observations of it must agree; observing BOTH kinds at the same term means one is a transient/buggy artifact
/// and NEITHER can be trusted, so the record becomes [`Conflicted`](CommittedKind::Conflicted) (order-independent)
/// and [`finalize_membership`] declines there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommittedKind {
  /// The committed entry at the index's recorded term is a `ConfChange`.
  ConfChange,
  /// The committed entry at the index's recorded term is NOT a `ConfChange` (Normal / Empty / SetReadMode).
  NonConfChange,
  /// Two different kinds were observed at the SAME term — an impossible-in-correct-Raft conflict; trust nothing.
  Conflicted,
}

/// The per-tick safety-oracle suite, holding the cross-tick history the monotonicity / committed-
/// history oracles need.
///
/// The driver builds one [`Checker`] per cluster and calls [`check`](Checker::check) at the end of
/// every tick with the freshly-captured [`ClusterView`]; on a violation it panics with seed+tick.
/// The checker is a pure observer of the simulated system (it mutates only its OWN history), so the
/// run is deterministic with or without it.
#[derive(Debug, Default)]
pub struct Checker {
  /// Per-node highest commit watermark ever observed (for [`monotonic_commit`]).
  max_commit_seen: BTreeMap<u64, u64>,
  /// Per-node highest term ever observed (for [`term_monotonic`]).
  max_term_seen: BTreeMap<u64, u64>,
  /// The cluster-wide committed-history high-water: `index -> command`. An entry is recorded the
  /// first time ANY node reports it as applied (applied entries are, by definition, committed).
  /// Used by [`no_committed_rewrite`] (a later conflicting command at a recorded index is a
  /// violation) and to attest the committed prefix for the durability checks.
  committed_hw: BTreeMap<u64, Vec<u8>>,
  /// The highest committed index that was reached under a configuration STRICTLY OLDER than the
  /// current one — raised to the committed high-water each time the authoritative voter set changes.
  /// [`commit_is_quorum_durable`] judges a quorum only for commit indices ABOVE this floor: an entry
  /// committed under a prior config had its quorum defined by that config, so the current voter set
  /// need not durably hold it (a removed voter carried a copy; a freshly-added voter joined later).
  /// Those older entries' safety stays covered by [`agreement`], [`no_committed_rewrite`], and
  /// [`durable_prefix`].
  commit_floor: u64,
  /// The authoritative committed voter set observed on the previous tick — a change signals a
  /// reconfiguration and raises [`commit_floor`](Self::commit_floor).
  last_committed_voters: Option<BTreeSet<u64>>,
  /// Per-node last-observed incarnation ([`NodeView::incarnation`]). When a node's incarnation
  /// changes (it crashed and recovered), its commit/term monotonicity baseline is reset, so a
  /// legitimate watermark drop across the restart boundary is not flagged as a backward step.
  last_incarnation: BTreeMap<u64, u64>,
  /// The INDEPENDENT, PERSISTENT committed-config reference for [`finalize_membership`], as a STEP
  /// FUNCTION keyed by committed conf-change index: `index -> (term, the ConfState that took effect AT that
  /// index)`, folded from LOG-BUILT nodes' `Event::ConfChanged` (carried in
  /// [`ClusterView::committed_transitions`]). The config in effect at any index `i` is the ConfState of the
  /// GREATEST key `<= i` (or [`genesis_conf`] below the first conf-change). It never resets and never shrinks,
  /// so the reference cannot exhaust even when every currently-live node has become snapshot-derived (the
  /// exhaustion the live-peer witness suffered). Each entry is `(term, ConfState, ambiguous)`:
  /// - HIGHEST-TERM-WINS resolves a same-index conflict: a strictly-higher-term observation overwrites (a
  ///   later term truncated and re-applied that entry — the committed truth) AND clears `ambiguous`.
  /// - Two DIFFERENT folds at the SAME `(index, term)` mark the entry `ambiguous`: the ConfChanged ConfState
  ///   is the node's apply-time fold, which an async in-memory apply can transiently diverge, so NEITHER fold
  ///   can be trusted. First-writer-win there would POISON the reference (the install-vs-reference check would
  ///   then compare against the poisoned value and pass a corrupt install), so an ambiguous index is NEVER
  ///   used — an install resolving to it is SKIPPED (counted unwitnessed) until a strictly-higher-term
  ///   transition disambiguates it.
  committed_config_history: BTreeMap<u64, (u64, ConfSnapshot, bool)>,
  /// The FINAL committed-log kind per index, the AUTHORITATIVE source that [`committed_config_history`] (sourced
  /// from transient apply-time `ConfChanged` events) is reconciled against: `index -> (highest committed term,
  /// [`CommittedKind`])`, folded from every node's COMMITTED (`index <= commit`) durable entry. A strictly-higher
  /// term OVERWRITES (resetting the kind); a SAME-term observation whose kind DIFFERS marks the entry
  /// [`Conflicted`](CommittedKind::Conflicted) regardless of arrival order (the committed entry at a given
  /// `(index, term)` is unique, so a same-term kind conflict means one observation is a transient/buggy artifact
  /// — trust neither). An in-memory-applied ConfChange at `(I, t)` can be truncated and superseded by a
  /// strictly-higher-term NON-ConfChange committed at the same index (which emits no `ConfChanged` event, so the
  /// event-sourced history keeps the stale transition); [`finalize_membership`] TOMBSTONES that. Committed entries
  /// are final, so highest-term-wins converges to the committed truth.
  committed_log_kind: BTreeMap<u64, (u64, CommittedKind)>,
  /// EVERY install ever OBSERVED, keyed by identity `(node id, transfer-snapshot boundary)` and storing the
  /// INSTALL-TIME `ConfState` (the exact membership the snapshot installed, captured from the event's
  /// `SnapshotMeta` — carried in [`ClusterView::new_installs`]). The check compares THIS fixed value against
  /// the committed-config reference at the boundary, NEVER the node's CURRENT config (which drifts as the node
  /// applies later entries — a corrupt install could otherwise be "repaired" by a later ConfChange and pass
  /// unexamined). Persistent: an earlier boundary stays even after a later boundary on the same node is
  /// compared, so `observed − compared` never lets a skipped-then-superseded install vanish.
  observed_installs: BTreeMap<(u64, u64), ConfSnapshot>,
  /// The genesis (pre-first-conf-change) configuration, captured ONCE from a log-built node that has applied
  /// no conf-change (`conf_changed == 0` ⇒ its config is the founding membership). The step-function
  /// reference for indices below the first recorded conf-change.
  genesis_conf: Option<ConfSnapshot>,
  /// The highest applied index any LOG-BUILT node has reached — the watermark up to which the step-function
  /// history is COMPLETE (a log-built node at applied `A` has emitted `ConfChanged` for every conf-change
  /// `<= A`, so none is missing). Monotone. An install at applied `> complete_up_to` has no certified
  /// reference (the frontier was only ever reached via snapshot transfer) and is counted as unwitnessed.
  complete_up_to: u64,
  /// How many observed installs the run-end final pass ([`finalize_membership`]) actually COMPARED
  /// against the FINAL committed-config history. Set by `finalize_membership` (0 until it runs). A sweep asserts
  /// this is `> 0` so the oracle is proven non-vacuous.
  membership_comparisons: u64,
  /// How many observed installs the run-end final pass ([`finalize_membership`]) could NOT compare because the
  /// committed-config HISTORY is incomplete at the boundary (`boundary > complete_up_to`, an unresolved
  /// same-term divergence, a higher-term ConfChange of unknown conf, or a resolved index that is neither
  /// committed-final nor genesis). Set by `finalize_membership` (0 until it runs). A sweep asserts this is `0`: a
  /// converged run leaves every boundary's reference complete + non-ambiguous. Distinct from
  /// [`membership_kind_unobservable`](Self::membership_kind_unobservable).
  membership_skipped: u64,
  /// How many observed installs the run-end final pass SOUNDLY declined because the resolved conf-change index is
  /// committed-FINAL (`<= complete_up_to`) but the committed log gives no EXACT-term ConfChange proof for it — its
  /// kind was compacted before any tick observed it as a standalone entry, or only a stale lower-term / unknown
  /// higher-term record exists — so whether the recorded ConfChange is the committed entry cannot be PROVEN. The
  /// net DECLINES to judge (never trust-stale) rather than risk a false verdict; a bounded coverage limitation of
  /// compaction, NOT a soundness hole and NOT a history-completeness gap.
  membership_kind_unobservable: u64,
}

impl Checker {
  /// A fresh checker with empty history.
  pub fn new() -> Self {
    Self::default()
  }

  /// The number of membership-coherence comparisons the run-end final pass performed (see
  /// `finalize_membership`); `0` until it runs. A sweep asserts this is `> 0` so the oracle is proven
  /// non-vacuous — it genuinely compared a snapshot-installed node against the committed-config history.
  pub fn membership_comparisons(&self) -> u64 {
    self.membership_comparisons
  }

  /// The number of observed installs the run-end final pass could NOT witness due to an incomplete committed
  /// -config HISTORY (boundary beyond the completeness watermark, an unresolved divergence, or a resolved index
  /// that is neither committed-final nor genesis); `0` until `finalize_membership` runs. A sweep asserts this is
  /// `0` at run end: after convergence every boundary's reference is complete + non-ambiguous. A compacted
  /// committed-log KIND is NOT counted here — see [`kind_unobservable_installs`](Self::kind_unobservable_installs).
  pub fn skipped_unwitnessed_installs(&self) -> u64 {
    self.membership_skipped
  }

  /// The number of observed installs the run-end final pass SOUNDLY declined because the resolved conf-change
  /// index is committed-FINAL (covered by a durable snapshot) but its committed-log KIND was compacted before any
  /// tick observed it. The net never trusts a possibly-stale ConfChange, so it declines rather than risk a false
  /// verdict — a bounded coverage limitation of compaction, not a soundness hole. `0` until `finalize_membership`
  /// runs.
  pub fn kind_unobservable_installs(&self) -> u64 {
    self.membership_kind_unobservable
  }

  /// Run the ENTIRE oracle suite against `view`, updating cross-tick history.
  ///
  /// Returns `Err(violation)` on the FIRST oracle that trips (the history-updating oracles update
  /// their state before returning, so a non-fatal observation is still recorded). On `Ok(())` the
  /// cluster satisfied every safety property at this tick.
  ///
  /// Ordering note: the history oracles (`no_committed_rewrite`, `monotonic_commit`,
  /// `term_monotonic`) check the NEW observation against stored history and THEN fold it in, so a
  /// regression is caught at the tick it first appears.
  pub fn check(&mut self, view: &ClusterView) -> Result<(), Violation> {
    // A reconfiguration (the authoritative voter set changed) raises the commit floor to the current
    // committed high-water, so entries committed under the prior config are not re-judged against the
    // new voter set. Only a genuine change of an already-known set counts — the first observation
    // (None -> Some, the initial config) does not raise the floor.
    if let (Some(old), Some(new)) = (&self.last_committed_voters, &view.committed_voters)
      && old != new
    {
      // Raise the floor to the high-water commit among the OLD voter set — the configuration under
      // which everything up to here was committed — INCLUDING any voter this change REMOVES. Using
      // only the NEW voters misses entries a removed voter legitimately committed (under the old
      // quorum) that the survivors have not yet caught up to; those entries would then be wrongly
      // re-judged against the smaller new voter set and flagged as non-quorum-durable. They are
      // safe — already validated against their own config before this change, and still covered by
      // agreement / no_committed_rewrite / durable_prefix. (e.g. leader 0 commits index
      // 375 under {0,2,3}; 0 is then removed → {2,3} with voter 2 still behind, so 375 must stay
      // exempt.) A removed voter is still present in `view.nodes` (isolated, not dropped).
      let hw = view
        .nodes
        .iter()
        .filter(|n| old.contains(&n.id))
        .map(|n| n.commit)
        .max()
        .unwrap_or(0);
      self.commit_floor = self.commit_floor.max(hw);
    }
    if view.committed_voters.is_some() {
      self.last_committed_voters = view.committed_voters.clone();
    }

    // A node that crashed and recovered (its incarnation changed) re-derives its commit/term
    // watermark from durable state — the batched commit/term persist can drop an in-memory advance
    // still in the fsync window on crash. Reset that node's monotonicity baseline so the legitimate
    // drop across the restart boundary is not flagged as a backward step within one incarnation.
    for n in view.nodes.iter() {
      let last = self.last_incarnation.get(&n.id).copied().unwrap_or(0);
      if n.incarnation != last {
        self.max_commit_seen.remove(&n.id);
        self.max_term_seen.remove(&n.id);
        self.last_incarnation.insert(n.id, n.incarnation);
      }
    }

    // Stateless cross-node oracles first.
    agreement(view)?;
    append_before_ack(view)?;
    commit_is_quorum_durable(view, self.commit_floor)?;
    durable_prefix(view)?;
    boundedness(view)?;
    snapshot_boundary_coherent(view)?;
    // Record-only: fold this tick's membership observations; the verdict is the run-end `finalize_membership`.
    record_membership_observation(self, view);
    // History oracles (read-then-fold).
    no_committed_rewrite(self, view)?;
    monotonic_commit(self, view)?;
    term_monotonic(self, view)?;
    Ok(())
  }

  /// Run the suite and PANIC on a violation, embedding the seed + tick for exact VOPR replay.
  ///
  /// This is what [`Cluster::tick`](crate::Cluster) calls at the end of every tick.
  pub fn check_or_panic(&mut self, view: &ClusterView) {
    if let Err(v) = self.check(view) {
      panic!(
        "SAFETY ORACLE VIOLATION: {v}\n  seed={} tick={}\n  (replay: run the same scenario / \
         run_vopr(seed, ticks) and inspect tick {})",
        view.seed, view.tick, view.tick,
      );
    }
  }
}

/// **agreement** (applied-prefix): for any two non-removed nodes, the shorter applied log is a
/// prefix of the longer — they agree on `(index→command)` at every shared position.
///
/// This is the core State Machine Safety property in prefix form. Removed nodes are skipped (their
/// applied log stopped advancing at removal while the cluster continued).
pub fn agreement(view: &ClusterView) -> Result<(), Violation> {
  let logs: Vec<&[(u64, Vec<u8>)]> = view.live().map(|n| n.applied_log.as_slice()).collect();
  let ids: Vec<u64> = view.live().map(|n| n.id).collect();
  let longest = logs.iter().map(|l| l.len()).max().unwrap_or(0);
  for k in 0..longest {
    // The first node that has position k pins the expected (index, command); any other node that
    // also has position k must match it.
    let mut pinned: Option<(usize, &(u64, Vec<u8>))> = None;
    for (li, l) in logs.iter().enumerate() {
      if let Some(cell) = l.get(k) {
        match pinned {
          None => pinned = Some((li, cell)),
          Some((pi, p)) => {
            if p != cell {
              return Err(Violation::new(
                "agreement",
                std::format!(
                  "applied[{k}] diverges: node {} has (idx={}, cmd={:?}) but node {} has (idx={}, cmd={:?})",
                  ids[pi],
                  p.0,
                  p.1,
                  ids[li],
                  cell.0,
                  cell.1,
                ),
              ));
            }
          }
        }
      }
    }
  }
  Ok(())
}

/// **append-before-ack** (per-tick form): no node has applied beyond its VISIBLE (readable) log —
/// a node can only apply an entry it can read.
///
/// **The real durability invariant is the send-time tripwire** in
/// [`Cluster::schedule_send`](crate::Cluster): a follower sends a non-reject `AppendResponse{match}`
/// only after its append is DURABLE (the proto defers the ack to `on_log_appended`, which fires on
/// the flush completion), so `durable_last >= match` holds when the ack is sent. That is where
/// "never ack an entry you have not durably stored" is enforced.
///
/// This per-tick form is a weaker companion sanity check: `applied <= max(visible_last,
/// snapshot_last_index)`. It deliberately uses the VISIBLE last index, NOT the durable one, because
/// the proto legitimately applies committed entries from its visible log BEFORE its own fsync —
/// commit advance/apply proceed independently of the local ack since a committed entry is durable
/// on a QUORUM elsewhere and the local state machine is rebuilt from the durable log on restart
/// (see `Endpoint::on_append_entries`). Bounding `applied` by `durable_last` would therefore
/// false-fire on a leader (or any node) that has applied a committed-but-not-yet-locally-flushed
/// tail. Per-entry quorum durability of every committed index is enforced separately by
/// [`commit_is_quorum_durable`]. (A snapshot-install follower has its applied watermark at the
/// snapshot boundary with the entries compacted out of the log, so the snapshot boundary counts.)
pub fn append_before_ack(view: &ClusterView) -> Result<(), Violation> {
  for n in view.nodes.iter() {
    let visible_high = n.visible_last.max(n.snapshot_last_index);
    if n.applied > visible_high {
      return Err(Violation::new(
        "append_before_ack",
        std::format!(
          "node {} applied={} exceeds its visible log high-water {} (visible_last={}, \
           snapshot_last_index={}) — applied beyond readable storage",
          n.id,
          n.applied,
          visible_high,
          n.visible_last,
          n.snapshot_last_index,
        ),
      ));
    }
  }
  Ok(())
}

/// **commit-is-quorum-durable**: for each committed VOTER, the entry at its `commit` index must be
/// present with the SAME term on a quorum of the VOTERS' DURABLE logs.
///
/// Catches a node that advanced commit without the entry being durably replicated to a quorum (the
/// heartbeat class) — one tick BEFORE [`agreement`] would catch the resulting divergence.
/// Compaction is accounted for: an index a voter has snapshotted past counts as a durable witness
/// (the snapshot subsumes its already-applied content), regardless of the snapshot boundary term —
/// a boundary-term match would be wrong, since the boundary outranks the subsumed entry's own term
/// (see [`NodeView::durable_covers`]).
///
/// The committing voter's own durable term at its commit index is the witness term `t`; the oracle
/// then counts how many VOTERS durably hold `(commit, t)`. Fewer than a majority of the EFFECTIVE
/// voter count (the authoritative [`committed_voters`](ClusterView::committed_voters) cardinality minus
/// voters provably on a stale lower-term branch) is a violation. A `commit` of 0 is vacuously fine.
///
/// **Voter-set denominator (reconfiguration soundness):** the quorum is taken over the
/// [authoritative committed voter set](ClusterView::committed_voters) (the leader's
/// `conf_state().voters()`), not all live nodes and not each node's own `is_voter` self-report. Only
/// voters ack toward commit, and a node becomes a voter only by applying the `AddNode` that adds it
/// (which requires its durable log to cover that conf-change's index, hence every earlier committed
/// entry). So a learner or a wired-but-not-yet-voting joiner never inflates the denominator against
/// an entry it could not have witnessed — the false positive an all-live-nodes denominator produced
/// (a 5→6 voter growth). And because the population is the LEADER's committed config,
/// a real committed voter the harness had prematurely marked removed (an accepted-but-never-committed
/// RemoveNode) is still counted as a durable witness, while a behind voter does not crowd it out —
/// the false positive a per-node `is_voter & !removed` denominator produced. A
/// learner's own `commit` watermark is not checked here (it makes no quorum claim; the same entry is
/// checked via the voters), but a learner that holds the entry still does no harm.
pub fn commit_is_quorum_durable(view: &ClusterView, commit_floor: u64) -> Result<(), Violation> {
  for n in view.voters() {
    let c = n.commit;
    if c == 0 {
      continue; // nothing committed
    }
    // The committing voter must itself durably cover its commit index (this is the append-before-
    // ack ordering; a violation here is also a violation, reported precisely).
    let witness_term = match n.durable_term(c) {
      Some(t) => t,
      None => {
        return Err(Violation::new(
          "commit_is_quorum_durable",
          std::format!(
            "node {} has commit={} but does not durably cover it (durable_first={}, \
             durable_last={}, snapshot_last_index={})",
            n.id,
            c,
            n.durable_first,
            n.durable_last,
            n.snapshot_last_index,
          ),
        ));
      }
    };
    // An entry at or below the reconfiguration floor was committed under a configuration older than
    // the current voter set; its quorum was defined by that config, so the current voters need not
    // all hold it. Its safety is covered by agreement / no_committed_rewrite / durable_prefix.
    if c <= commit_floor {
      continue;
    }
    // Quorum DENOMINATOR. Start from the AUTHORITATIVE committed-voter count (`committed_voters.len()`
    // when known — a momentarily-absent voter view must not shrink it and weaken the oracle),
    // then SUBTRACT voters provably on a stale STRICTLY-LOWER-term branch at `c`. Such a voter
    // durably holds a different, older-term entry, so it never acked `(c, witness_term)` — a same-index
    // entry cannot revert to an older term — and it was not in the quorum that committed this entry (the
    // higher-term log will overwrite it). Excluding ONLY lower-term divergence keeps full teeth: a
    // merely-LAGGING voter (no entry at `c`) and a HIGHER-term divergent voter both remain, so a solo /
    // under-replicated commit AND a commit on a LOSING branch still trip. (e.g. a term-3 entry
    // committed under a smaller config while two voters sit on a stale term-2 branch.)
    // A second exclusion handles the deep-churn boundary: a voter whose DURABLE log does not even reach
    // the reconfiguration floor (`durable_last < commit_floor`) is a freshly-added member still catching
    // up to the prior committed config — it could not have witnessed ANY entry above the floor, so it
    // was not in the quorum that committed `c` (which is just above the floor). Counting it demands a
    // phantom ack it could never have given. This is distinct from a merely-LAGGING real voter (which
    // HAS reached the floor and is only missing the latest entries) — that one stays counted, so a true
    // under-replication still trips. (e.g. idx 2056 committed under a small config that
    // then grew to 5 voters, three of which sit far below the floor at 223/490/2034 vs floor 2055.)
    let excluded = view
      .voters()
      .filter(|m| {
        m.durable_term(c).map(|t| t < witness_term).unwrap_or(false)
          || m.durable_last < commit_floor
      })
      .count();
    let effective = view.voter_count().saturating_sub(excluded);
    let quorum = effective / 2 + 1;
    let copies = view
      .voters()
      .filter(|m| {
        // A voter witnesses the committed entry at `c` if it durably holds that entry's content.
        // LIVE (`c` in the retained log): the term must EQUAL the witness term — the teeth that catch a
        // stale-tail / heartbeat commit one tick before `agreement` would. COMPACTED (`c` below the
        // retained log, subsumed by the voter's snapshot): the voter APPLIED `c` before compacting it
        // away (compaction follows apply), so it durably holds the committed content regardless of the
        // snapshot's BOUNDARY term — which is necessarily >= the entry's term and is NOT the entry's own
        // term, so a boundary-term match is the WRONG test. The applied content of a compacted entry is
        // already validated by `agreement`, so a subsumed index counts as a durable witness here.
        // Without this, a committed entry that an ahead voter has snapshotted past is mis-scored as a
        // term-divergent branch and the count drops below quorum (a false positive under frequent
        // compaction, where the snapshot boundary outranks a still-live-elsewhere earlier committed entry).
        if c >= m.durable_first {
          m.durable_term(c) == Some(witness_term)
        } else {
          m.durable_covers(c)
        }
      })
      .count();
    if copies < quorum {
      let per_voter: std::vec::Vec<_> = view
        .voters()
        .map(|m| {
          std::format!(
            "n{}(durable_last={} term@{}={:?} covers={})",
            m.id,
            m.durable_last,
            c,
            m.durable_term(c),
            m.durable_covers(c)
          )
        })
        .collect();
      return Err(Violation::new(
        "commit_is_quorum_durable",
        std::format!(
          "node {} committed index {} (term {}) but only {} of {} voter durable logs hold it with \
           that term (quorum needs {})\n  commit_floor={} committed_voters={:?}\n  voters: {}",
          n.id,
          c,
          witness_term,
          copies,
          effective,
          quorum,
          commit_floor,
          view.committed_voters,
          per_voter.join(" "),
        ),
      ));
    }
  }
  Ok(())
}

/// **durable-prefix-after-restart** (the headline crash-recovery oracle), expressed as a per-tick
/// invariant: a node's in-memory `commit` must be `>=` the committed prefix it durably persisted —
/// concretely, `commit >= min(durable HardState.commit, durable last_index)`.
///
/// # Why this catches the commit-persistence bug
///
/// The durable `HardState.commit` is precisely the committed-prefix length the node had
/// **acknowledged and persisted** before any crash. The bug was that `restart` rebuilt an
/// empty / snapshot-only state machine — recovering `commit = 0` — *despite* a durable
/// `HardState.commit > 0` and a durable log covering it. That trips this oracle the instant the
/// restarted node is observed: `commit (=0) < min(hs.commit, durable_last) (= hs.commit > 0)`.
///
/// # Why it never false-positives
///
/// In correct operation a node persists `commit` only AFTER advancing its in-memory `commit`
/// (the `handle_storage` choke-point writes `self.commit`), so the durable `HardState.commit` is
/// always `<=` the in-memory `commit`. After a restart, the recovery formula sets
/// `commit = min(hs.commit, last_index).max(applied)`, whose lower bound is exactly the
/// `min(hs.commit, last_index)` this oracle requires. The `min` with `durable_last` covers the
/// exotic-but-safe case where a crash lost an in-flight LOG write while the commit-watermark write
/// survived (the entries are re-synced from the leader per the `LogStore::restore` contract); the
/// oracle does not demand the lost entries be present, only the prefix the durable log still covers.
pub fn durable_prefix(view: &ClusterView) -> Result<(), Violation> {
  for n in view.nodes.iter() {
    let durable_committed_prefix = n.hardstate_commit.min(n.durable_last);
    if n.commit < durable_committed_prefix {
      return Err(Violation::new(
        "durable_prefix",
        std::format!(
          "node {} recovered/holds commit={} but its durable committed prefix is {} \
           (HardState.commit={}, durable_last={}) — a restart must not silently forget the \
           committed state it durably stored",
          n.id,
          n.commit,
          durable_committed_prefix,
          n.hardstate_commit,
          n.durable_last,
        ),
      ));
    }
  }
  Ok(())
}

/// **boundedness**: per-node bookkeeping stays bounded under compaction.
///
/// A soft structural check that catches a compaction/GC failure: a node's
/// in-memory durable log must not grow unboundedly while snapshots are taken. The window of
/// in-memory entries is `durable_last - durable_first + 1`; under healthy compaction this stays
/// near `commit - first_index`. We assert the in-memory entry COUNT matches the index window (an
/// off-by-one or stale-offset GC bug would desynchronize them) and that staged writes do not
/// accumulate past a small bound (the fsync window holds at most a handful of in-flight writes; an
/// unbounded `staged` means `flush`/`discard` stopped draining).
///
/// The slack bound on staged writes is generous (`1024`) — this is a tripwire for *unbounded*
/// growth (a leak), not a tight resource assertion.
pub fn boundedness(view: &ClusterView) -> Result<(), Violation> {
  const STAGED_SLACK: usize = 1024;
  for n in view.nodes.iter() {
    // The durable entry count must equal the index window [first..=last]. `durable_last == 0`
    // with `durable_first == 1` is the empty-log base case (count 0).
    let window = if n.durable_last >= n.durable_first {
      (n.durable_last - n.durable_first + 1) as usize
    } else {
      0
    };
    if n.durable_entries.len() != window {
      return Err(Violation::new(
        "boundedness",
        std::format!(
          "node {} durable entry count {} disagrees with its index window {} \
           (durable_first={}, durable_last={}) — a compaction/offset GC bug",
          n.id,
          n.durable_entries.len(),
          window,
          n.durable_first,
          n.durable_last,
        ),
      ));
    }
    if n.inflight_staged > STAGED_SLACK {
      return Err(Violation::new(
        "boundedness",
        std::format!(
          "node {} has {} staged (un-flushed) writes, exceeding the {} bound — the fsync window \
           is not draining (a flush/discard leak)",
          n.id,
          n.inflight_staged,
          STAGED_SLACK,
        ),
      ));
    }
  }
  Ok(())
}

/// **snapshot-boundary-coherent**: for every node carrying an installed durable snapshot with
/// boundary `(last_index, last_term)`, the `last_term` must EQUAL the term the committed log actually
/// recorded at `last_index`. A node can never install a snapshot whose boundary term disagrees with
/// the committed log — that would mean a snapshot transfer delivered a boundary for a different
/// (conflicting) entry than the one committed at that index, corrupting the receiver's compacted
/// prefix below the reach of [`agreement`] (whose applied logs no longer hold the compacted entries).
///
/// # Reference term (the committed log's term at `last_index`)
///
/// A snapshot's boundary entry was a COMMITTED entry on the node that built it (compaction follows
/// apply, apply follows commit), and the chunked-transfer receiver installs that same boundary. The
/// committed term at an index is unique — two nodes that have both COMMITTED `last_index` can never
/// disagree on its term (State Machine Safety) — so the reference is read from any node that still
/// retains `last_index` as a durable LOG entry it has ALSO committed
/// ([`NodeView::committed_log_term`]). The witness is deliberately neither (a) a snapshot boundary
/// (the boundary term is the value under test — circular) nor (b) an UNCOMMITTED durable tail.
///
/// # Why it never false-positives (soundness)
///
/// - The reference comes only from a COMMITTED retained log entry, which carries the one true committed
///   term at that index; if it equals the boundary, the boundary is coherent.
/// - An UNCOMMITTED durable tail is excluded: a lagging node can durably hold a stale entry at
///   `last_index` whose term is LOWER than the committed one (it was overwritten by the higher-term
///   committed entry but not yet truncated). That term is not the committed term — counting it would
///   false-positive against a perfectly correct boundary (the exact term-off-by-one a superseding
///   snapshot race produces: a node still holding the pre-supersede uncommitted tail).
/// - An index NO node retains-and-has-committed is UNWITNESSABLE: there is no sound reference, so the
///   snapshot is SKIPPED, never flagged. Its coherence is still protected at install time (the receiver
///   validates a snapshot's boundary term against its own committed log before installing) and
///   transitively by the monotonic-commit / durable-prefix oracles; this oracle adds teeth for the
///   witnessable case.
///
/// Read-only: draws no PRNG and mutates nothing, so the run stays byte-identical (determinism preserved).
pub fn snapshot_boundary_coherent(view: &ClusterView) -> Result<(), Violation> {
  for n in view.nodes.iter() {
    // No installed snapshot ⇒ no boundary to check (`snapshot_last_index == 0` is the "none" sentinel,
    // matching `Cluster::view`'s capture of an absent durable snapshot).
    if n.snapshot_last_index == 0 {
      continue;
    }
    let last_index = n.snapshot_last_index;
    // The reference committed term at `last_index`, witnessed by any node (incl. this one) that retains
    // it as a durable LOG entry it has ALSO committed. Never a snapshot boundary (circular) nor an
    // uncommitted tail (a stale lower-term entry not yet truncated). The first such witness suffices:
    // a committed index has a unique term, so any committed witness gives the same answer.
    let Some(reference_term) = view
      .nodes
      .iter()
      .find_map(|w| w.committed_log_term(last_index))
    else {
      // Unwitnessable: no node retains-and-has-committed `last_index`, so there is no sound reference.
      // Skip — flagging here would false-positive on a perfectly valid boundary whose committed entry
      // every up-to-date node has compacted away while the laggards have not yet committed it.
      continue;
    };
    if n.snapshot_last_term != reference_term {
      return Err(Violation::new(
        "snapshot_boundary_coherent",
        std::format!(
          "node {} installed a snapshot with boundary (last_index={}, last_term={}) but the committed \
           log records term {} at index {} — a snapshot boundary's term must match the committed entry \
           it subsumes",
          n.id,
          last_index,
          n.snapshot_last_term,
          reference_term,
          last_index,
        ),
      ));
    }
  }
  Ok(())
}

/// **snapshot-membership-coherent (RECORD step)**: fold this tick's observations — every `SnapshotInstalled`
/// (as an install-time ConfState) and every committed conf-change (into the persistent step-function history) —
/// so the run-end [`finalize_membership`] can render the verdict against the FINAL stable history.
///
/// The PROPERTY (verified by `finalize_membership`): the [`ConfState`](sailing_proto::ConfState) a transferred
/// snapshot INSTALLED (captured at install time — every field, not just the obvious sets) must equal the
/// committed configuration in effect at the snapshot's BOUNDARY, so a snapshot transfer can never adopt a
/// membership with a PHANTOM voter (one the committed membership has removed) or a MISSING joiner (one it has
/// added), nor a corrupted joint / learner-promotion state. The verdict is against the INSTALL-TIME ConfState —
/// NOT the node's CURRENT config, which drifts as it applies later entries (a corrupt install could otherwise
/// be silently "repaired" by a later ConfChange and never examined).
///
/// # The reference is an INDEPENDENT, PERSISTENT step-function history (it cannot exhaust)
///
/// A node's active config is the fold of every membership change it has applied, in committed-log (total)
/// order, so the committed config is a STEP FUNCTION of index, changing only at conf-change indices. Rather
/// than depend on a currently-live log-built peer (which can VANISH once every node has become
/// snapshot-derived, since the lineage flag is sticky forever), this oracle folds each committed conf-change
/// — at its EXACT index, from a LOG-BUILT node's `ConfChanged` event — into a PERSISTENT
/// [`committed_config_history`](Checker::committed_config_history), plus the [`genesis_conf`](Checker::genesis_conf)
/// below the first change. The config in effect at index `i` is the value of the greatest key `<= i`. The
/// history never resets and never shrinks, so the reference survives even when no log-built node is currently
/// live. Keying by EXACT conf-change index (not per-applied-index) makes it gap-free regardless of how large a
/// batch an apply advances.
///
/// # Same-index conflicts: highest-term-wins, else AMBIGUOUS (never poisoned)
///
/// A ConfChanged's ConfState is the node's apply-time FOLD, and in async mode an in-memory apply can run ahead
/// of durability and be truncated + superseded by a higher-term entry, so two log-built nodes can report
/// DIFFERENT folds at the same conf-change index. A strictly-higher-term observation is the committed truth
/// (it overwrites); a same-`(index, term)` divergence marks the index AMBIGUOUS — first-writer-win there would
/// POISON the reference, so an ambiguous index is never used until a higher-term transition disambiguates it.
/// Because these resolutions MUTATE the history as the run proceeds, the verdict is deferred to run end (see
/// [`finalize_membership`]) — comparing against an entry before the history stabilises could freeze a verdict
/// against a value a later overwrite or ambiguation supersedes.
///
/// This is a pure observer: it draws no PRNG and never mutates the simulated nodes, so the run stays
/// byte-identical (determinism preserved).
pub fn record_membership_observation(checker: &mut Checker, view: &ClusterView) {
  // RECORD installs: every `SnapshotInstalled` this tick is an OBSERVED install, keyed by (id, transfer
  // boundary) and storing the INSTALL-TIME ConfState (the exact membership the snapshot installed). A repeated
  // observation of the same install keeps the first (same conf).
  for (id, boundary, install_conf) in view.new_installs.iter() {
    checker
      .observed_installs
      .entry((*id, *boundary))
      .or_insert_with(|| install_conf.clone());
  }

  // RECORD transitions: fold this tick's committed conf-changes (from log-built nodes' ConfChanged events)
  // into the step-function history at their EXACT indices.
  //
  // The ConfState in a ConfChanged event is the node's APPLY-TIME FOLD of every conf-change it has applied,
  // and in async mode an in-memory apply can run ahead of durability and later be TRUNCATED + superseded by a
  // higher-term entry at the same log index — so two log-built nodes can legitimately report DIFFERENT folded
  // ConfStates at the same conf-change index. Resolution:
  //   • a STRICTLY-HIGHER term overwrites and clears ambiguity (the later term re-applied the entry — truth);
  //   • a same-`(index, term)` DIVERGENT fold marks the index AMBIGUOUS (neither fold can be trusted —
  //     first-writer-win would poison the reference and let a corrupt install pass the check below);
  //   • a same-`(index, term)` matching fold, or a stale lower term, changes nothing.
  for (idx, term, conf) in view.committed_transitions.iter() {
    if let Some((et, ec, amb)) = checker.committed_config_history.get_mut(idx) {
      if *term > *et {
        *et = *term;
        *ec = conf.clone();
        *amb = false;
      } else if *term == *et && &*ec != conf {
        *amb = true;
      }
    } else {
      checker
        .committed_config_history
        .insert(*idx, (*term, conf.clone(), false));
    }
  }
  for n in view.nodes.iter() {
    if n.removed || n.installed_snapshot {
      continue;
    }
    // Genesis: a log-built node that has applied NO conf-change is serving the founding membership.
    if n.conf_changed == 0 && checker.genesis_conf.is_none() {
      checker.genesis_conf = Some(n.conf_snapshot());
    }
    // A log-built node at applied `A` has emitted ConfChanged for every conf-change `<= A`, so the history is
    // complete up to `A`. Raise the completeness watermark (monotone).
    checker.complete_up_to = checker.complete_up_to.max(n.applied);
  }

  // Record the FINAL committed-log kind per index from the AUTHORITATIVE source — the committed DURABLE log, not
  // the transient apply-time events. For each COMMITTED (`index <= commit`) DURABLE entry of EVERY node, every
  // tick: a strictly-higher term OVERWRITES (resetting the kind to that entry's); a SAME-term observation whose
  // kind DIFFERS marks the index CONFLICTED (order-independent — the committed entry at a given `(index, term)`
  // is unique, so a same-term kind conflict means one observation is a transient/buggy artifact, trust neither);
  // a same-term same-kind or a lower-term observation changes nothing. A durable committed entry is FINAL
  // (committed entries are persisted-before-ack — the durability oracles verify this), unlike a committed-but-
  // unflushed entry an async higher term can still supersede. PERSISTENT (never cleared/compacted), so a captured
  // index is kept FOREVER — capturing across each tick's commit growth observes an entry while still retained,
  // BEFORE compaction removes it.
  for n in view.nodes.iter() {
    for e in n.durable_entries.iter() {
      if e.index > n.commit {
        continue;
      }
      let kind = if e.is_conf_change {
        CommittedKind::ConfChange
      } else {
        CommittedKind::NonConfChange
      };
      match checker.committed_log_kind.get_mut(&e.index) {
        Some((t, k)) if e.term > *t => {
          *t = e.term;
          *k = kind; // strictly-higher term: the superseding committed entry's kind (clears any conflict)
        }
        Some((t, k)) if e.term == *t && *k != kind => {
          *k = CommittedKind::Conflicted; // same term, differing kind: an impossible-in-correct-Raft conflict
        }
        Some(_) => {} // same-term same-kind (incl. already conflicted), or a stale lower-term observation
        None => {
          checker.committed_log_kind.insert(e.index, (e.term, kind));
        }
      }
    }
  }
}

/// **snapshot-membership-coherent (VERDICT step)**: the run-end final pass. Compare EVERY observed install's
/// INSTALL-TIME ConfState against the FINAL committed config in effect at its boundary, exactly once. Run once
/// after the last tick (see [`record_membership_observation`] for the per-tick recording it consumes).
///
/// # Why a run-end pass, not a per-tick verdict
///
/// [`committed_config_history`](Checker::committed_config_history) is MUTABLE while a run proceeds: a
/// strictly-higher-term observation OVERWRITES an entry (the later term re-applied it — the committed truth),
/// and a same-`(index, term)` divergent fold AMBIGUATES it. A per-tick verdict that blessed an install the
/// first time its reference resolved would freeze that judgement against a NON-FINAL reference — a later
/// overwrite or ambiguation of that index would never re-judge the install, so a corrupt install could pass
/// against a value the history later supersedes (the install-vs-stale-reference false-negative class). Deferring
/// to one pass over the now-stable history compares every install against the FINAL committed truth exactly
/// once and removes that whole class.
///
/// # Authoritative source: tombstone superseded ConfChanges
///
/// The history is sourced from transient apply-time `ConfChanged` EVENTS, but the AUTHORITATIVE truth is the
/// committed LOG. An in-memory-applied ConfChange at `(I, t)` can be truncated and superseded by a
/// strictly-higher-term NON-ConfChange (Normal/Empty/SetReadMode) committed at the same index `I` — which emits
/// NO `ConfChanged` event, so the stale ConfChange transition would linger. So the config-in-effect resolution
/// consults [`committed_log_kind`](Checker::committed_log_kind) (the final committed entry per index) and
/// TOMBSTONES a recorded transition at `I` whose final committed entry is a higher-term non-ConfChange: the
/// config does NOT change at `I`, so the resolution walks past it to the prior surviving conf-change index.
///
/// STRICT trust: `committed_log_kind` is the only proof source, and a recorded ConfChange transition is the
/// reference ONLY when the committed-log kind is an EXACT-term ConfChange at that index — which (committed
/// entries being immutable) IS the recorded entry. A known non-ConfChange at exact-or-higher term tombstones
/// (config unchanged there); anything weaker — a kind compacted unobserved, a stale lower-term record, or a
/// higher-term ConfChange of unknown conf — is NEVER trusted (no fall-through to the transient apply-time
/// ConfChange). (An independently-witnessed snapshot boundary would also prove the entry, but a witnessed
/// boundary's committed entry is retained, so its kind is already in `committed_log_kind` — the exact-term check
/// subsumes it; an UN-witnessed boundary is no proof, so there is no separate boundary path.)
///
/// # Verdict
///
/// For each observed install `(node, boundary) -> install-time ConfState`:
/// - boundary beyond [`complete_up_to`](Checker::complete_up_to) (no log-built node certified the history that
///   far), an AMBIGUOUS effective index, or an absent genesis ⇒ a history-completeness gap (SKIPPED — a converged
///   run drives this to `0`);
/// - a consulted index whose committed-log kind is not an exact-term ConfChange proof (compacted unobserved, a
///   stale lower-term record, or a higher-term ConfChange of unknown conf) ⇒ a sound KIND-UNOBSERVABLE decline:
///   the index is committed-final (`<= complete_up_to`) but the oracle cannot prove the recorded ConfChange is
///   the committed entry, so it declines rather than risk a stale verdict;
/// - otherwise resolve the FINAL reference = the conf of the greatest SURVIVING (non-tombstoned) conf-change
///   index `<= boundary`, else [`genesis_conf`](Checker::genesis_conf), and COMPARE the stored install-time
///   ConfState against it: a mismatch is a Violation (a phantom voter, a missing joiner, or a corrupted joint /
///   learner state); a match counts as a comparison.
///
/// Sets [`membership_comparisons`](Checker::membership_comparisons) (installs compared, a sweep asserts `> 0`),
/// [`skipped_unwitnessed_installs`](Checker::skipped_unwitnessed_installs) (history-completeness gaps, a sweep
/// asserts `0` on a converged run), and [`kind_unobservable_installs`](Checker::kind_unobservable_installs)
/// (sound declines for committed-final indices whose kind was compacted — a bounded compaction limitation, not a
/// soundness hole). Idempotent: recomputes from the current history each call. A pure observer — no PRNG, no
/// node mutation.
pub fn finalize_membership(checker: &mut Checker) -> Result<(), Violation> {
  // Snapshot the observed installs so the loop can write the checker's counters without holding a borrow of
  // `observed_installs`. Sorted by (node, boundary) ⇒ a deterministic first-violation choice.
  let observed: Vec<((u64, u64), ConfSnapshot)> = checker
    .observed_installs
    .iter()
    .map(|(id, conf)| (*id, conf.clone()))
    .collect();
  let mut comparisons = 0u64;
  let mut kind_unobservable = 0u64;
  for ((node_id, boundary), install_conf) in observed.iter() {
    // The history is certified complete only up to `complete_up_to` (the highest index a LOG-BUILT node has
    // APPLIED, hence emitted every conf-change for). Beyond it the reference is not final — count it unwitnessed.
    if *boundary > checker.complete_up_to {
      continue;
    }
    // The FINAL committed config in effect at the BOUNDARY = the conf of the greatest SURVIVING conf-change
    // index `<= boundary`. STRICT trust against the AUTHORITATIVE committed log (`committed_log_kind`): a recorded
    // ConfChange transition is used as the reference ONLY on SOLID PROOF — an EXACT-term ConfChange record at the
    // index. Walk the recorded transitions descending:
    //   • `committed_log_kind[idx] == (term_cc, ConfChange)` — the committed entry at idx IS the recorded
    //     ConfChange (committed entries are immutable, so a sampled committed entry at the transition's term is
    //     that entry) ⇒ TRUST (resolve to its conf), unless its fold is AMBIGUOUS;
    //   • a KNOWN non-ConfChange at exact-or-higher term `(>= term_cc, non-ConfChange)` — a same-term or
    //     higher-term non-ConfChange is the committed entry, so the config does NOT change here ⇒ TOMBSTONE:
    //     walk past to the prior surviving index;
    //   • anything else — kind MISSING (compacted unobserved), a STALE lower-term record, or a higher-term
    //     ConfChange of unknown conf — is NOT solid proof ⇒ DECLINE. A declined index is committed-FINAL (it is
    //     `<= complete_up_to`, so a log-built node applied it), so this is a sound KIND-UNOBSERVABLE decline,
    //     NEVER a fall-through to the transient apply-time ConfChange.
    // Below the first surviving conf-change the genesis config applies (a history gap if never captured).
    let mut resolved: Option<ConfSnapshot> = None;
    let mut unwitnessable = false;
    let mut decline_kind = false;
    for (idx, (term_cc, conf, amb)) in checker.committed_config_history.range(..=*boundary).rev() {
      match checker.committed_log_kind.get(idx) {
        // PROVEN: an exact-term committed ConfChange at idx is the recorded transition's entry.
        Some((term_log, CommittedKind::ConfChange)) if *term_log == *term_cc => {}
        // TOMBSTONE: a known non-ConfChange at exact-or-higher term is the committed entry — config unchanged here.
        Some((term_log, CommittedKind::NonConfChange)) if *term_log >= *term_cc => continue,
        // No solid proof — a CONFLICTED same-term record (two kinds observed, trust neither), a missing kind, a
        // stale lower-term record, or a higher-term ConfChange of unknown conf: committed-final but unprovable ⇒
        // a sound kind-unobservable decline, never trust-stale.
        _ => {
          decline_kind = true;
          break;
        }
      }
      if *amb {
        unwitnessable = true; // a proven ConfChange whose apply-time fold is an unresolved same-term divergence
        break;
      }
      resolved = Some(conf.clone());
      break;
    }
    if decline_kind {
      kind_unobservable += 1;
      continue;
    }
    if unwitnessable {
      continue;
    }
    let reference = match resolved {
      Some(conf) => conf,
      None => match checker.genesis_conf.clone() {
        Some(genesis) => genesis,
        None => continue,
      },
    };
    comparisons += 1;
    if install_conf == &reference {
      continue;
    }
    let phantom: BTreeSet<u64> = install_conf
      .voters
      .difference(&reference.voters)
      .copied()
      .collect();
    let missing: BTreeSet<u64> = reference
      .voters
      .difference(&install_conf.voters)
      .copied()
      .collect();
    checker.membership_comparisons = comparisons;
    return Err(Violation::new(
      "snapshot_membership_coherent",
      std::format!(
        "the snapshot installed at node {} boundary {} adopted membership (voters={:?} outgoing={:?} \
         learners={:?} learners_next={:?} auto_leave={}) but the committed config at that boundary is \
         (voters={:?} outgoing={:?} learners={:?} learners_next={:?} auto_leave={}) — phantom voters {:?} \
         (committed-removed yet present), missing joiners {:?} (committed-added yet absent): the installed \
         snapshot's ConfState is inconsistent with the committed membership",
        node_id,
        boundary,
        install_conf.voters,
        install_conf.voters_outgoing,
        install_conf.learners,
        install_conf.learners_next,
        install_conf.auto_leave,
        reference.voters,
        reference.voters_outgoing,
        reference.learners,
        reference.learners_next,
        reference.auto_leave,
        phantom,
        missing,
      ),
    ));
  }
  checker.membership_comparisons = comparisons;
  checker.membership_kind_unobservable = kind_unobservable;
  // Skipped = observed installs that were neither compared nor a sound kind-unobservable decline — i.e. genuine
  // committed-config-history completeness gaps (which a converged run drives to 0).
  checker.membership_skipped = (observed.len() as u64)
    .saturating_sub(comparisons)
    .saturating_sub(kind_unobservable);
  Ok(())
}

/// **no-committed-rewrite**: once index `i` is committed clusterwide with command `c`, no node ever
/// applies a different command at `i` — the strongest State Machine Safety check.
///
/// The cluster-wide committed high-water is tracked in [`Checker::committed_hw`]: every applied
/// entry is, by definition, committed, so the first command seen applied at an index is recorded;
/// any later DIFFERENT command applied at that index is a violation. Re-applying the SAME command
/// (e.g. a follower replaying its durable log after restart) is fine.
pub fn no_committed_rewrite(checker: &mut Checker, view: &ClusterView) -> Result<(), Violation> {
  // First pass: detect a conflict against the recorded high-water.
  for n in view.nodes.iter() {
    // A removed node's frozen applied log already agreed with the high-water while it was live;
    // skip it so a post-removal cluster that legitimately advanced does not re-compare its stale
    // tail. (Its recorded contributions remain in the high-water.)
    if n.removed {
      continue;
    }
    for (idx, cmd) in n.applied_log.iter() {
      if let Some(prev) = checker.committed_hw.get(idx)
        && prev != cmd
      {
        return Err(Violation::new(
          "no_committed_rewrite",
          std::format!(
            "committed index {idx} was applied as {prev:?} but node {} now applies {cmd:?} — a \
               committed entry was overwritten (State Machine Safety violation)",
            n.id,
          ),
        ));
      }
    }
  }
  // Second pass: fold every applied entry into the high-water (no conflict was found).
  for n in view.nodes.iter() {
    for (idx, cmd) in n.applied_log.iter() {
      checker
        .committed_hw
        .entry(*idx)
        .or_insert_with(|| cmd.clone());
    }
  }
  Ok(())
}

/// **monotonic-commit-per-node**: a (healthy) node's `commit` index never decreases across ticks —
/// within an incarnation AND across a restart.
///
/// The durable commit watermark is persisted, so a restart recovers it and commit
/// must NOT regress even across restart. At a tick boundary a HEALTHY node's in-memory commit is
/// durably persisted (the `handle_storage` choke-point ran to quiescence), so the next incarnation
/// recovers `>=` the last observed value; a regression below a previously-observed HEALTHY commit IS
/// a durability bug.
///
/// **Poisoned exception:** the proto gates commit-persistence on `!poisoned`, so a node that
/// advanced commit in-memory and THEN poisoned (e.g. a committed-range read fault during apply,
/// after advancing commit but before persisting it) holds an in-memory commit that was never made
/// durable. Commit-persistence protects only the DURABLE commit, so that un-persisted advance is
/// legitimately lost on restart and must NOT be used as the regression baseline (see the recording
/// pass).
pub fn monotonic_commit(checker: &mut Checker, view: &ClusterView) -> Result<(), Violation> {
  for n in view.nodes.iter() {
    let prev = checker.max_commit_seen.get(&n.id).copied().unwrap_or(0);
    if n.commit < prev {
      return Err(Violation::new(
        "monotonic_commit",
        std::format!(
          "node {} commit regressed from {} to {} across ticks — the durable commit watermark is \
           persisted, so commit must never go backward (even across restart)",
          n.id,
          prev,
          n.commit,
        ),
      ));
    }
  }
  for n in view.nodes.iter() {
    // Do NOT record a poisoned node's in-memory commit as the monotonic baseline. The proto's
    // commit-persistence choke-point is gated on `!poisoned`, so a node that advanced commit
    // in-memory and THEN poisoned (e.g. on a committed-range read fault during apply, before the
    // persist step) carries an in-memory commit that was never durably persisted. Commit-persistence
    // only guarantees the DURABLE commit is recovered on restart, so that un-persisted advance is
    // legitimately lost when the node restarts — recording it here would make the next (healthy,
    // correctly-recovered) incarnation look like a regression. The regression CHECK above still
    // runs for every node (a poisoned node's commit is frozen and can only sit at/above the
    // healthy baseline, never below it), so this only suppresses the false positive, never a real
    // durability regression on a healthy node.
    if n.poisoned {
      continue;
    }
    let e = checker.max_commit_seen.entry(n.id).or_insert(0);
    *e = (*e).max(n.commit);
  }
  Ok(())
}

/// **term-monotonic-per-node**: a node's `term` never decreases across ticks.
///
/// The current term is persisted in `HardState` before the node acts on it, so a restart recovers
/// the durable term and a term must never regress (within an incarnation or across a restart). A
/// regression would mean a node forgot a term it had already entered — which could let it re-grant
/// a vote or re-accept a stale leader.
pub fn term_monotonic(checker: &mut Checker, view: &ClusterView) -> Result<(), Violation> {
  for n in view.nodes.iter() {
    let prev = checker.max_term_seen.get(&n.id).copied().unwrap_or(0);
    if n.term < prev {
      return Err(Violation::new(
        "term_monotonic",
        std::format!(
          "node {} term regressed from {} to {} across ticks — the term is persisted before use, \
           so it must never go backward (even across restart)",
          n.id,
          prev,
          n.term,
        ),
      ));
    }
  }
  for n in view.nodes.iter() {
    let e = checker.max_term_seen.entry(n.id).or_insert(0);
    *e = (*e).max(n.term);
  }
  Ok(())
}

#[cfg(test)]
mod tests;
