//! Cluster configuration state and membership-change types, including the full
//! joint-consensus extension.
//!
//! This module is the authoritative home for:
//! - [`ConfState`] — the configuration state embedded in snapshots and checkpoints.
//! - [`ConfChangeType`], [`ConfChangeTransition`] — discriminants for membership changes.
//! - [`ConfChangeSingle`] — a single add/remove operation.
//! - [`ConfChange`] — the simple (v1) single-op change entry payload.
//! - [`ConfChangeV2`] — the general (possibly multi-op / joint-consensus) change entry payload.
use crate::{Data, DecodeError, NodeId};
use bytes::Bytes;
use std::{collections::BTreeSet, vec::Vec};

// ─── ConfState ────────────────────────────────────────────────────────────────

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

impl<I: NodeId> ConfState<I> {
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

  // ── Accessors ───────────────────────────────────────────────────────────────

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

  // ── Predicates ──────────────────────────────────────────────────────────────

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

// ─── ConfChangeType ───────────────────────────────────────────────────────────

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

impl Data for ConfChangeType {
  fn encode(&self, buf: &mut Vec<u8>) {
    buf.push(match self {
      Self::AddNode => 0u8,
      Self::RemoveNode => 1u8,
      Self::AddLearnerNode => 2u8,
    });
  }

  fn decode(buf: &[u8]) -> Result<(usize, Self), DecodeError> {
    match buf.first() {
      Some(0) => Ok((1, Self::AddNode)),
      Some(1) => Ok((1, Self::RemoveNode)),
      Some(2) => Ok((1, Self::AddLearnerNode)),
      Some(_) => Err(DecodeError::Invalid("ConfChangeType")),
      None => Err(DecodeError::UnexpectedEof),
    }
  }
}

// ─── ConfChangeTransition ─────────────────────────────────────────────────────

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

impl Data for ConfChangeTransition {
  fn encode(&self, buf: &mut Vec<u8>) {
    buf.push(match self {
      Self::Auto => 0u8,
      Self::Implicit => 1u8,
      Self::Explicit => 2u8,
    });
  }

  fn decode(buf: &[u8]) -> Result<(usize, Self), DecodeError> {
    match buf.first() {
      Some(0) => Ok((1, Self::Auto)),
      Some(1) => Ok((1, Self::Implicit)),
      Some(2) => Ok((1, Self::Explicit)),
      Some(_) => Err(DecodeError::Invalid("ConfChangeTransition")),
      None => Err(DecodeError::UnexpectedEof),
    }
  }
}

// ─── ConfChangeSingle ─────────────────────────────────────────────────────────

/// A single membership-change operation: add/remove a node.
///
/// A [`ConfChangeV2`] carries a `Vec<ConfChangeSingle<I>>`; a plain [`ConfChange`] wraps a
/// single one implicitly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfChangeSingle<I> {
  ty: ConfChangeType,
  node: I,
}

impl<I: NodeId> ConfChangeSingle<I> {
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
    self.node
  }
}

impl<I: NodeId + Data> Data for ConfChangeSingle<I> {
  fn encode(&self, buf: &mut Vec<u8>) {
    self.ty.encode(buf);
    self.node.encode(buf);
  }

  fn decode(buf: &[u8]) -> Result<(usize, Self), DecodeError> {
    let (n1, ty) = ConfChangeType::decode(buf)?;
    let rest = buf.get(n1..).ok_or(DecodeError::UnexpectedEof)?;
    let (n2, node) = I::decode(rest)?;
    Ok((n1 + n2, Self { ty, node }))
  }
}

// ─── ConfChange (v1) ──────────────────────────────────────────────────────────

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

impl<I: NodeId> ConfChange<I> {
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
    self.node
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

impl<I: NodeId + Data> Data for ConfChange<I> {
  fn encode(&self, buf: &mut Vec<u8>) {
    self.ty.encode(buf);
    self.node.encode(buf);
    self.context.encode(buf);
  }

  fn decode(buf: &[u8]) -> Result<(usize, Self), DecodeError> {
    let (n1, ty) = ConfChangeType::decode(buf)?;
    let rest = buf.get(n1..).ok_or(DecodeError::UnexpectedEof)?;
    let (n2, node) = I::decode(rest)?;
    let rest2 = buf.get(n1 + n2..).ok_or(DecodeError::UnexpectedEof)?;
    let (n3, context) = Bytes::decode(rest2)?;
    Ok((n1 + n2 + n3, Self { ty, node, context }))
  }
}

// ─── ConfChangeV2 ─────────────────────────────────────────────────────────────

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

impl<I: NodeId> ConfChangeV2<I> {
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

impl<I: NodeId + Data> Data for ConfChangeV2<I> {
  fn encode(&self, buf: &mut Vec<u8>) {
    self.transition.encode(buf);
    // Length-prefix the changes vec: encode length as u64 then each element.
    (self.changes.len() as u64).encode(buf);
    for c in &self.changes {
      c.encode(buf);
    }
    self.context.encode(buf);
  }

  fn decode(buf: &[u8]) -> Result<(usize, Self), DecodeError> {
    let mut pos = 0usize;

    // Transition byte.
    let (n, transition) = ConfChangeTransition::decode(buf)?;
    pos += n;

    // Number of changes (u64 length prefix).
    let rest = buf.get(pos..).ok_or(DecodeError::UnexpectedEof)?;
    let (n, count_u64) = u64::decode(rest)?;
    pos += n;
    let count = count_u64 as usize;

    // Decode each change.
    let mut changes = Vec::with_capacity(count);
    for _ in 0..count {
      let rest = buf.get(pos..).ok_or(DecodeError::UnexpectedEof)?;
      let (n, change) = ConfChangeSingle::<I>::decode(rest)?;
      pos += n;
      changes.push(change);
    }

    // Context (length-prefixed bytes).
    let rest = buf.get(pos..).ok_or(DecodeError::UnexpectedEof)?;
    let (n, context) = Bytes::decode(rest)?;
    pos += n;

    Ok((
      pos,
      Self {
        transition,
        changes,
        context,
      },
    ))
  }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
  use super::*;
  use std::vec;

  // ── ConfState ──────────────────────────────────────────────────────────────

  #[test]
  fn conf_state_from_voters_deduplicates() {
    let c = ConfState::from_voters(vec![3u64, 1u64, 2u64, 1u64]);
    assert_eq!(c.voters(), &BTreeSet::from([1u64, 2u64, 3u64]));
    assert_eq!(c.len(), 3);
    assert!(c.is_voter(&2u64));
    assert!(!c.is_voter(&99u64));
    assert!(!c.is_joint());
    assert!(c.learners().is_empty());
  }

  #[test]
  fn conf_state_empty() {
    let c = ConfState::<u64>::from_voters(vec![]);
    assert!(c.is_empty());
  }

  #[test]
  fn conf_state_full_constructor() {
    let c = ConfState::new(
      vec![1u64, 2u64],
      vec![3u64],
      vec![4u64, 5u64],
      vec![6u64],
      true,
    );
    assert_eq!(c.voters(), &BTreeSet::from([1u64, 2u64]));
    assert_eq!(c.learners(), &BTreeSet::from([3u64]));
    assert_eq!(c.voters_outgoing(), &BTreeSet::from([4u64, 5u64]));
    assert_eq!(c.learners_next(), &BTreeSet::from([6u64]));
    assert!(c.auto_leave());
    assert!(c.is_joint());
    assert!(c.is_learner(&3u64));
    assert!(!c.is_learner(&1u64));
  }

  #[test]
  fn conf_state_default_is_empty() {
    let c = ConfState::<u64>::default();
    assert!(c.is_empty());
    assert!(!c.is_joint());
    assert!(!c.auto_leave());
  }

  // ── ConfChangeType ─────────────────────────────────────────────────────────

  #[test]
  fn conf_change_type_roundtrip() {
    for ty in [
      ConfChangeType::AddNode,
      ConfChangeType::RemoveNode,
      ConfChangeType::AddLearnerNode,
    ] {
      let mut buf = std::vec::Vec::new();
      ty.encode(&mut buf);
      assert_eq!(buf.len(), 1);
      let (n, decoded) = ConfChangeType::decode(&buf).unwrap();
      assert_eq!(n, 1);
      assert_eq!(decoded, ty);
    }
  }

  #[test]
  fn conf_change_type_bad_discriminant() {
    assert!(ConfChangeType::decode(&[99u8]).is_err());
  }

  #[test]
  fn conf_change_type_empty_buf() {
    assert!(matches!(
      ConfChangeType::decode(&[]),
      Err(DecodeError::UnexpectedEof)
    ));
  }

  #[test]
  fn conf_change_type_display() {
    assert_eq!(ConfChangeType::AddNode.as_str(), "add_node");
    assert_eq!(ConfChangeType::RemoveNode.as_str(), "remove_node");
    assert_eq!(ConfChangeType::AddLearnerNode.as_str(), "add_learner_node");
    assert_eq!(
      std::format!("{}", ConfChangeType::AddLearnerNode),
      "add_learner_node"
    );
  }

  #[test]
  fn conf_change_type_is_variant() {
    assert!(ConfChangeType::AddNode.is_add_node());
    assert!(!ConfChangeType::AddNode.is_remove_node());
    assert!(ConfChangeType::RemoveNode.is_remove_node());
    assert!(ConfChangeType::AddLearnerNode.is_add_learner_node());
  }

  // ── ConfChangeTransition ───────────────────────────────────────────────────

  #[test]
  fn conf_change_transition_roundtrip() {
    for tr in [
      ConfChangeTransition::Auto,
      ConfChangeTransition::Implicit,
      ConfChangeTransition::Explicit,
    ] {
      let mut buf = std::vec::Vec::new();
      tr.encode(&mut buf);
      assert_eq!(buf.len(), 1);
      let (n, decoded) = ConfChangeTransition::decode(&buf).unwrap();
      assert_eq!(n, 1);
      assert_eq!(decoded, tr);
    }
  }

  #[test]
  fn conf_change_transition_default_is_auto() {
    assert_eq!(ConfChangeTransition::default(), ConfChangeTransition::Auto);
  }

  #[test]
  fn conf_change_transition_bad_discriminant() {
    assert!(ConfChangeTransition::decode(&[99u8]).is_err());
  }

  #[test]
  fn conf_change_transition_empty_buf() {
    assert!(matches!(
      ConfChangeTransition::decode(&[]),
      Err(DecodeError::UnexpectedEof)
    ));
  }

  #[test]
  fn conf_change_transition_display_and_variants() {
    assert_eq!(ConfChangeTransition::Auto.as_str(), "auto");
    assert_eq!(ConfChangeTransition::Implicit.as_str(), "implicit");
    assert_eq!(ConfChangeTransition::Explicit.as_str(), "explicit");
    assert!(ConfChangeTransition::Auto.is_auto());
    assert!(ConfChangeTransition::Implicit.is_implicit());
    assert!(ConfChangeTransition::Explicit.is_explicit());
  }

  // ── ConfChangeSingle ───────────────────────────────────────────────────────

  #[test]
  fn conf_change_single_roundtrip() {
    let c = ConfChangeSingle::new(ConfChangeType::AddNode, 42u64);
    let mut buf = std::vec::Vec::new();
    c.encode(&mut buf);
    let (n, decoded) = ConfChangeSingle::<u64>::decode(&buf).unwrap();
    assert_eq!(n, buf.len());
    assert_eq!(decoded, c);
  }

  #[test]
  fn conf_change_single_truncated() {
    // One byte (type only, missing node).
    assert!(ConfChangeSingle::<u64>::decode(&[0u8]).is_err());
  }

  #[test]
  fn conf_change_single_empty_buf() {
    assert!(ConfChangeSingle::<u64>::decode(&[]).is_err());
  }

  // ── ConfChange ─────────────────────────────────────────────────────────────

  #[test]
  fn conf_change_roundtrip_with_context() {
    let c = ConfChange::new(ConfChangeType::AddNode, 7u64, Bytes::from_static(b"ctx"));
    let mut buf = std::vec::Vec::new();
    c.encode(&mut buf);
    let (n, decoded) = ConfChange::<u64>::decode(&buf).unwrap();
    assert_eq!(n, buf.len());
    assert_eq!(decoded, c);
  }

  #[test]
  fn conf_change_roundtrip_empty_context() {
    let c = ConfChange::new(ConfChangeType::RemoveNode, 3u64, Bytes::new());
    let mut buf = std::vec::Vec::new();
    c.encode(&mut buf);
    let (n, decoded) = ConfChange::<u64>::decode(&buf).unwrap();
    assert_eq!(n, buf.len());
    assert_eq!(decoded, c);
  }

  #[test]
  fn conf_change_truncated() {
    // Truncated after type byte.
    assert!(ConfChange::<u64>::decode(&[0u8]).is_err());
  }

  #[test]
  fn conf_change_into_v2() {
    let c = ConfChange::new(ConfChangeType::AddNode, 5u64, Bytes::from_static(b"x"));
    let v2 = c.clone().into_v2();
    assert_eq!(v2.transition(), ConfChangeTransition::Auto);
    assert_eq!(v2.changes().len(), 1);
    assert_eq!(v2.changes()[0].ty(), ConfChangeType::AddNode);
    assert_eq!(v2.changes()[0].node(), 5u64);
    assert_eq!(v2.context(), &Bytes::from_static(b"x"));
  }

  // ── ConfChangeV2 ───────────────────────────────────────────────────────────

  #[test]
  fn conf_change_v2_roundtrip_empty_changes() {
    let v2 = ConfChangeV2::new(
      ConfChangeTransition::Explicit,
      std::vec![],
      Bytes::from_static(b"empty"),
    );
    let mut buf = std::vec::Vec::new();
    v2.encode(&mut buf);
    let (n, decoded) = ConfChangeV2::<u64>::decode(&buf).unwrap();
    assert_eq!(n, buf.len());
    assert_eq!(decoded, v2);
  }

  #[test]
  fn conf_change_v2_roundtrip_multi_change() {
    let v2 = ConfChangeV2::new(
      ConfChangeTransition::Implicit,
      std::vec![
        ConfChangeSingle::new(ConfChangeType::AddNode, 1u64),
        ConfChangeSingle::new(ConfChangeType::AddLearnerNode, 2u64),
        ConfChangeSingle::new(ConfChangeType::RemoveNode, 3u64),
      ],
      Bytes::from_static(b"multi"),
    );
    let mut buf = std::vec::Vec::new();
    v2.encode(&mut buf);
    let (n, decoded) = ConfChangeV2::<u64>::decode(&buf).unwrap();
    assert_eq!(n, buf.len());
    assert_eq!(decoded, v2);
  }

  #[test]
  fn conf_change_v2_truncated_after_transition() {
    // Only the transition byte, nothing else.
    assert!(ConfChangeV2::<u64>::decode(&[0u8]).is_err());
  }

  #[test]
  fn conf_change_v2_truncated_mid_changes() {
    // transition + length=2 + only 1 full change (9 bytes) then truncated
    let mut buf = std::vec::Vec::new();
    ConfChangeTransition::Auto.encode(&mut buf); // 1 byte
    (2u64).encode(&mut buf); // 8 bytes (len = 2 changes)
    ConfChangeSingle::new(ConfChangeType::AddNode, 1u64).encode(&mut buf); // 1+8 = 9 bytes
    // missing 2nd change → decode must fail, not panic
    assert!(ConfChangeV2::<u64>::decode(&buf).is_err());
  }

  #[test]
  fn conf_change_v2_empty_buf() {
    assert!(ConfChangeV2::<u64>::decode(&[]).is_err());
  }

  #[test]
  fn conf_change_v2_bad_transition_discriminant() {
    assert!(ConfChangeV2::<u64>::decode(&[99u8]).is_err());
  }
}
