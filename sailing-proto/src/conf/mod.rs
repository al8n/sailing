//! Cluster configuration state and membership-change types, including the full
//! joint-consensus extension.
//!
//! This module is the authoritative home for:
//! - [`ConfState`] — the configuration state embedded in snapshots and checkpoints.
//! - [`ConfChangeType`], [`ConfChangeTransition`] — discriminants for membership changes.
//! - [`ConfChangeSingle`] — a single add/remove operation.
//! - [`ConfChange`] — the simple (v1) single-op change entry payload.
//! - [`ConfChangeV2`] — the general (possibly multi-op / joint-consensus) change entry payload.
use crate::CheapClone;
use bytes::Bytes;
use std::{collections::BTreeSet, vec::Vec};

/// The full Raft configuration state. Stored inside [`crate::SnapshotMeta`] and consulted
/// during snapshot install and restart.
///
/// Mirrors etcd `raftpb.ConfState` faithfully:
/// - `voters` — the current (or incoming, during a joint transition) voter set.
/// - `learners` — read-only replicas that do not vote.
/// - `voters_outgoing` — the outgoing voter set during a joint transition (empty in simple
///   config).
/// - `learners_next` — learners that are being promoted to voters; their promotion is
///   deferred until the joint config is committed.
/// - `auto_leave` — when `true` the leader will automatically leave the joint configuration
///   after it is committed (etcd's default behaviour for `ConfChangeV2` with `Auto`
///   transition).
///
/// An empty `ConfState` (all fields default) is valid and represents an empty cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfState<I> {
  voters: BTreeSet<I>,
  learners: BTreeSet<I>,
  voters_outgoing: BTreeSet<I>,
  learners_next: BTreeSet<I>,
  auto_leave: bool,
}

impl<I> Default for ConfState<I> {
  /// An empty configuration (no voters, no learners, not in a joint transition).
  fn default() -> Self {
    Self {
      voters: BTreeSet::new(),
      learners: BTreeSet::new(),
      voters_outgoing: BTreeSet::new(),
      learners_next: BTreeSet::new(),
      auto_leave: false,
    }
  }
}

impl<I> ConfState<I> {
  /// The current (or incoming) voter set.
  #[inline(always)]
  pub fn voters(&self) -> &BTreeSet<I> {
    &self.voters
  }

  /// The learner set.
  #[inline(always)]
  pub fn learners(&self) -> &BTreeSet<I> {
    &self.learners
  }

  /// The outgoing voter set (non-empty only during a joint transition).
  #[inline(always)]
  pub fn voters_outgoing(&self) -> &BTreeSet<I> {
    &self.voters_outgoing
  }

  /// Learners that will be promoted to voters once the joint config is committed.
  #[inline(always)]
  pub fn learners_next(&self) -> &BTreeSet<I> {
    &self.learners_next
  }

  /// Whether the leader should automatically leave the joint configuration after it is
  /// committed.
  #[inline(always)]
  pub fn auto_leave(&self) -> bool {
    self.auto_leave
  }

  /// Whether the cluster is currently in a joint (two-phase) configuration transition.
  ///
  /// A joint transition is active when `voters_outgoing` is non-empty.
  #[inline(always)]
  pub fn is_joint(&self) -> bool {
    !self.voters_outgoing.is_empty()
  }

  /// Number of voters in the current (or incoming) voter set.
  #[inline(always)]
  pub fn len(&self) -> usize {
    self.voters.len()
  }

  /// `true` if the voter set is empty.
  #[inline(always)]
  pub fn is_empty(&self) -> bool {
    self.voters.is_empty()
  }
}

impl<I: Ord> ConfState<I> {
  /// Construct the full configuration state from all five fields.
  ///
  /// The sets are stored as-is; callers are responsible for deduplication if needed (the
  /// `BTreeSet` type deduplicates automatically on insertion, but duplicate inputs in the
  /// iterators are silently dropped).
  pub fn new(
    voters: impl IntoIterator<Item = I>,
    learners: impl IntoIterator<Item = I>,
    voters_outgoing: impl IntoIterator<Item = I>,
    learners_next: impl IntoIterator<Item = I>,
    auto_leave: bool,
  ) -> Self {
    Self {
      voters: voters.into_iter().collect(),
      learners: learners.into_iter().collect(),
      voters_outgoing: voters_outgoing.into_iter().collect(),
      learners_next: learners_next.into_iter().collect(),
      auto_leave,
    }
  }

  /// Convenience constructor for the common single-config (non-joint) case: only the voter
  /// set is populated; all other fields take their default (empty / `false`).
  pub fn from_voters(voters: impl IntoIterator<Item = I>) -> Self {
    Self {
      voters: voters.into_iter().collect(),
      ..Default::default()
    }
  }

  /// Whether `id` is in the current (or incoming) voter set.
  #[inline(always)]
  pub fn is_voter(&self, id: &I) -> bool {
    self.voters.contains(id)
  }

  /// Whether `id` is in the learner set.
  #[inline(always)]
  pub fn is_learner(&self, id: &I) -> bool {
    self.learners.contains(id)
  }

  /// Whether this is a valid, installable *live* configuration — the invariant a
  /// snapshot-delivered or disk-recovered membership MUST satisfy before it is fed into the tracker
  /// (which copies the sets verbatim, performing no checks of its own). These mirror the post-change
  /// invariants the `Changer` enforces internally, applied here at the untrusted-input boundary so a
  /// malformed snapshot cannot install an impossible membership (no quorum, vacuous votes, a broken
  /// joint-leave):
  ///
  /// - at least one incoming voter (a live cluster must be able to form a quorum);
  /// - learners disjoint from BOTH voter halves;
  /// - every `learners_next` member is an outgoing voter and NOT an incoming voter (an
  ///   outgoing-only staged demotion: it is demoted to learner on `leave_joint`);
  /// - non-joint (`voters_outgoing` empty) ⇒ no `learners_next` and `auto_leave == false`.
  ///
  /// NOTE: the empty [`Default`](Self::default) (no voters) is a pre-bootstrap placeholder and is
  /// intentionally NOT valid here — a snapshot of a live cluster always carries at least one voter.
  pub fn is_valid(&self) -> bool {
    if self.voters.is_empty() {
      return false;
    }
    // Learners must not overlap either voter half.
    for id in &self.learners {
      if self.voters.contains(id) || self.voters_outgoing.contains(id) {
        return false;
      }
    }
    // `learners_next` are OUTGOING-ONLY staged demotions: a member leaves on `leave_joint` and
    // becomes a learner, so it must be an outgoing voter AND must NOT also be an incoming voter — it
    // cannot both remain a voter and be demoted. (The local `Changer` removes a node from the
    // incoming half before staging it, so this overlap is impossible from a correct change; reject it
    // here so a malformed snapshot can't smuggle a node that `leave_joint` would turn into a
    // simultaneous voter+learner, poisoning `ConfChangeApply` after the snapshot is already installed.)
    for id in &self.learners_next {
      if !self.voters_outgoing.contains(id) || self.voters.contains(id) {
        return false;
      }
    }
    // Non-joint cleanliness: with no outgoing half there can be no staged demotions or auto-leave.
    if !self.is_joint() && (!self.learners_next.is_empty() || self.auto_leave) {
      return false;
    }
    true
  }
}

/// The operation a [`ConfChangeSingle`] or [`ConfChange`] performs on the cluster.
///
/// Mirrors etcd `raftpb.ConfChangeType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, derive_more::Display, derive_more::IsVariant)]
#[display("{}", self.as_str())]
pub enum ConfChangeType {
  /// Add a voting member. If the node is already a voter this is a no-op.
  AddNode,
  /// Remove a member (voter or learner). If the node is not present this is a no-op.
  RemoveNode,
  /// Add (or keep) a node as a learner. If the node is already a learner this is a
  /// no-op; if it is a voter it becomes a learner.
  AddLearnerNode,
}

impl ConfChangeType {
  /// The stable snake_case name (used for `Display` and serialisation).
  #[inline(always)]
  pub const fn as_str(&self) -> &'static str {
    match self {
      Self::AddNode => "add_node",
      Self::RemoveNode => "remove_node",
      Self::AddLearnerNode => "add_learner_node",
    }
  }
}

/// Governs how a [`ConfChangeV2`] transitions the cluster through the joint-consensus
/// algorithm.
///
/// Mirrors etcd `raftpb.ConfChangeTransition`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, derive_more::Display, derive_more::IsVariant)]
#[display("{}", self.as_str())]
pub enum ConfChangeTransition {
  /// Automatically pick `Implicit` for single-change operations and `Explicit` for
  /// multi-change operations. This is the default.
  Auto,
  /// Use the joint algorithm but leave automatically (the leader writes a second entry to
  /// exit the joint state).
  Implicit,
  /// Use the joint algorithm; the application is responsible for writing the exit entry.
  Explicit,
}

impl Default for ConfChangeTransition {
  #[inline(always)]
  fn default() -> Self {
    Self::Auto
  }
}

impl ConfChangeTransition {
  /// The stable snake_case name.
  #[inline(always)]
  pub const fn as_str(&self) -> &'static str {
    match self {
      Self::Auto => "auto",
      Self::Implicit => "implicit",
      Self::Explicit => "explicit",
    }
  }
}

/// A single membership-change operation: add/remove a node.
///
/// A [`ConfChangeV2`] carries a `Vec<ConfChangeSingle<I>>`; a plain [`ConfChange`] wraps a
/// single one implicitly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfChangeSingle<I> {
  ty: ConfChangeType,
  node: I,
}

impl<I: CheapClone> ConfChangeSingle<I> {
  /// Construct from a change type and the target node id.
  #[inline(always)]
  pub fn new(ty: ConfChangeType, node: I) -> Self {
    Self { ty, node }
  }

  /// The change type.
  #[inline(always)]
  pub fn ty(&self) -> ConfChangeType {
    self.ty
  }

  /// The target node.
  #[inline(always)]
  pub fn node(&self) -> I {
    self.node.cheap_clone()
  }
}

/// A simple (single-operation) membership-change entry payload (v1).
///
/// Corresponds to etcd `raftpb.ConfChange`. Carries exactly one operation (`ty` on `node`)
/// and an opaque `context` blob for the application. Use [`ConfChange::into_v2`] to convert
/// to the general [`ConfChangeV2`] form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfChange<I> {
  ty: ConfChangeType,
  node: I,
  context: Bytes,
}

impl<I: CheapClone> ConfChange<I> {
  /// Construct from a change type, target node, and application context.
  #[inline(always)]
  pub fn new(ty: ConfChangeType, node: I, context: Bytes) -> Self {
    Self { ty, node, context }
  }

  /// The change type.
  #[inline(always)]
  pub fn ty(&self) -> ConfChangeType {
    self.ty
  }

  /// The target node.
  #[inline(always)]
  pub fn node(&self) -> I {
    self.node.cheap_clone()
  }

  /// The opaque application context (empty when unused).
  #[inline(always)]
  pub fn context(&self) -> &Bytes {
    &self.context
  }

  /// Convert to the general [`ConfChangeV2`] form with a single change and `Auto` transition.
  ///
  /// Mirrors etcd's `ConfChange.AsV2()`.
  pub fn into_v2(self) -> ConfChangeV2<I> {
    ConfChangeV2 {
      transition: ConfChangeTransition::Auto,
      changes: std::vec![ConfChangeSingle {
        ty: self.ty,
        node: self.node,
      }],
      context: self.context,
    }
  }
}

/// The general (possibly multi-operation / joint-consensus) membership-change entry payload.
///
/// Corresponds to etcd `raftpb.ConfChangeV2`. A `ConfChangeV2` with a single change and
/// `Auto` transition behaves identically to a [`ConfChange`] (simple config change); with
/// multiple changes or a non-`Auto` transition the joint algorithm is used.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfChangeV2<I> {
  transition: ConfChangeTransition,
  changes: Vec<ConfChangeSingle<I>>,
  context: Bytes,
}

impl<I> ConfChangeV2<I> {
  /// Construct from a transition, change list, and application context.
  #[inline(always)]
  pub fn new(
    transition: ConfChangeTransition,
    changes: Vec<ConfChangeSingle<I>>,
    context: Bytes,
  ) -> Self {
    Self {
      transition,
      changes,
      context,
    }
  }

  /// The transition mode.
  #[inline(always)]
  pub fn transition(&self) -> ConfChangeTransition {
    self.transition
  }

  /// The list of individual membership operations.
  #[inline(always)]
  pub fn changes(&self) -> &[ConfChangeSingle<I>] {
    &self.changes
  }

  /// The opaque application context (empty when unused).
  #[inline(always)]
  pub fn context(&self) -> &Bytes {
    &self.context
  }

  /// Construct the canonical "leave joint" entry: empty changes, `Auto` transition, empty context.
  ///
  /// When decoded at apply time, the empty-changes + Auto combination signals the Changer to
  /// call `leave_joint`, completing the two-phase membership transition.
  pub fn leave_joint() -> Self {
    Self {
      transition: ConfChangeTransition::Auto,
      changes: Vec::new(),
      context: Bytes::new(),
    }
  }
}

#[cfg(test)]
mod tests;
