//! Majority and joint-quorum primitives. Faithful port of etcd's `quorum/majority.go` and
//! `quorum/joint.go`.
//!
//! These are the building blocks for commit-index advancement and vote tallying under both
//! simple and joint-consensus configurations. Correctness is critical: the committed index
//! and vote result ride directly on the safety of replication and leader election.
use crate::{Index, NodeId};
use std::collections::BTreeSet;

/// The outcome of a quorum vote.
///
/// Matches etcd's `VoteResult` constants. `Pending` means neither side has reached a quorum
/// yet, so the outcome depends on votes not yet received.
#[derive(Debug, Clone, Copy, PartialEq, Eq, derive_more::Display, derive_more::IsVariant)]
#[display("{}", self.as_str())]
pub enum VoteResult {
  /// A quorum has voted yes — the election is won.
  Won,
  /// A quorum has voted no — the election is lost.
  Lost,
  /// Neither outcome has reached a quorum; more votes are needed.
  Pending,
}

impl VoteResult {
  /// Stable snake_case name.
  #[inline(always)]
  pub const fn as_str(&self) -> &'static str {
    match self {
      Self::Won => "won",
      Self::Lost => "lost",
      Self::Pending => "pending",
    }
  }
}

/// A set of node IDs that makes decisions by majority quorum.
///
/// Port of etcd `quorum.MajorityConfig`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MajorityConfig<I> {
  ids: BTreeSet<I>,
}

impl<I: NodeId> MajorityConfig<I> {
  /// Construct from a set of member IDs.
  #[inline(always)]
  pub fn new(ids: BTreeSet<I>) -> Self {
    Self { ids }
  }

  /// The member ID set.
  #[inline(always)]
  pub fn ids(&self) -> &BTreeSet<I> {
    &self.ids
  }

  /// Number of members.
  #[inline(always)]
  #[allow(dead_code, reason = "internal accessor; retained for completeness")]
  pub fn len(&self) -> usize {
    self.ids.len()
  }

  /// `true` if the config is empty.
  #[inline(always)]
  #[allow(dead_code, reason = "internal accessor; retained for completeness")]
  pub fn is_empty(&self) -> bool {
    self.ids.is_empty()
  }

  /// Whether `id` is a member of this config.
  #[inline(always)]
  pub fn contains(&self, id: &I) -> bool {
    self.ids.contains(id)
  }

  /// The largest index that a majority of members have matched (acked).
  ///
  /// Port of etcd `MajorityConfig.CommittedIndex`. Each member's acked index is obtained by
  /// calling `acked(id)`; a member that has not reported yet should return `Index::ZERO`.
  ///
  /// Algorithm: collect all per-member acked indices into a vec, sort ascending, and return
  /// `srt[n - (n/2 + 1)]`. For n=1 that is index 0 (the sole member); for n=3 it is index
  /// 1 (the median, i.e. the 2nd-largest); for n=5 it is index 2. In every case the value
  /// is acked by at least `n/2 + 1` members (a majority).
  ///
  /// An **empty** config returns `Index::new(u64::MAX)` — a "no-constraint" sentinel. This
  /// makes `JointConfig::committed_index` (which takes the `min` of two halves) correctly
  /// defer to the non-empty half when one half is absent.
  pub fn committed_index(&self, acked: impl Fn(I) -> Index) -> Index {
    let n = self.ids.len();
    if n == 0 {
      return Index::new(u64::MAX);
    }
    // The element at position `n - (n/2 + 1)` in ascending order is the highest index held by a
    // majority (n=3 → pos 1, the median; n=5 → pos 2). Only that ONE element is needed, so
    // `select_nth_unstable` (O(n)) replaces a full `sort_unstable` (O(n log n)) — `s[pos]` afterward is
    // exactly the value the sort would have placed there — and a stack buffer for the common
    // small-cluster case avoids the per-ack heap allocation. `pos` depends only on `n`, never the values.
    let pos = n - (n / 2 + 1);
    const STACK_CAP: usize = 16;
    if n <= STACK_CAP {
      let mut buf = [Index::ZERO; STACK_CAP];
      for (slot, id) in buf[..n].iter_mut().zip(self.ids.iter()) {
        *slot = acked(*id);
      }
      let s = &mut buf[..n];
      s.select_nth_unstable(pos);
      s[pos]
    } else {
      let mut srt: std::vec::Vec<Index> = self.ids.iter().map(|id| acked(*id)).collect();
      srt.select_nth_unstable(pos);
      srt[pos]
    }
  }

  /// Whether a majority has voted.
  ///
  /// Port of etcd `MajorityConfig.VoteResult`. `votes(id)` returns `Some(true)` for a
  /// grant, `Some(false)` for a rejection, and `None` for a vote not yet received.
  ///
  /// - Empty config → `Won` (vacuous majority; by convention, as in etcd).
  /// - If grants `>= q` → `Won`.
  /// - Else if grants + missing `>= q` → `Pending` (still winnable).
  /// - Otherwise → `Lost`.
  pub fn vote_result(&self, votes: impl Fn(I) -> Option<bool>) -> VoteResult {
    if self.ids.is_empty() {
      return VoteResult::Won;
    }
    let mut grants = 0usize;
    let mut missing = 0usize;
    for &id in &self.ids {
      match votes(id) {
        Some(true) => grants += 1,
        Some(false) => {}
        None => missing += 1,
      }
    }
    let q = self.ids.len() / 2 + 1;
    if grants >= q {
      VoteResult::Won
    } else if grants + missing >= q {
      VoteResult::Pending
    } else {
      VoteResult::Lost
    }
  }
}

/// A joint (two-majority) quorum configuration used during membership transitions.
///
/// Port of etcd `quorum.JointConfig`. A decision requires both the `incoming` and
/// `outgoing` majorities to agree. When `outgoing` is empty (not in a joint transition),
/// it behaves exactly like the `incoming` half alone, because an empty `MajorityConfig`
/// contributes the `u64::MAX` sentinel to `min` (committed index) and `Won` to the joint
/// vote result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JointConfig<I> {
  incoming: MajorityConfig<I>,
  outgoing: MajorityConfig<I>,
}

impl<I: NodeId> JointConfig<I> {
  /// Construct from explicit incoming and outgoing halves.
  #[inline(always)]
  pub fn new(incoming: MajorityConfig<I>, outgoing: MajorityConfig<I>) -> Self {
    Self { incoming, outgoing }
  }

  /// Construct a non-joint config (outgoing is empty).
  #[inline(always)]
  pub fn from_voters(voters: BTreeSet<I>) -> Self {
    Self {
      incoming: MajorityConfig::new(voters),
      outgoing: MajorityConfig::new(BTreeSet::new()),
    }
  }

  /// The incoming (new/primary) majority config.
  #[inline(always)]
  pub fn incoming(&self) -> &MajorityConfig<I> {
    &self.incoming
  }

  /// The outgoing (old) majority config; empty when not in a joint transition.
  #[inline(always)]
  pub fn outgoing(&self) -> &MajorityConfig<I> {
    &self.outgoing
  }

  /// The union of all member IDs across both halves.
  pub fn ids(&self) -> BTreeSet<I> {
    self
      .incoming
      .ids
      .iter()
      .chain(self.outgoing.ids.iter())
      .copied()
      .collect()
  }

  /// The largest index jointly committed by both halves.
  ///
  /// An index is jointly committed only when it is committed under both the incoming and
  /// outgoing configs. This is the `min` of the two halves' committed indices. Because an
  /// empty half returns `u64::MAX`, the `min` naturally defers to the non-empty half.
  pub fn committed_index(&self, acked: impl Fn(I) -> Index + Copy) -> Index {
    self
      .incoming
      .committed_index(acked)
      .min(self.outgoing.committed_index(acked))
  }

  /// The joint vote result.
  ///
  /// Port of etcd `JointConfig.VoteResult`. `Won` only if BOTH halves are `Won`; `Lost` if
  /// EITHER half is `Lost`; otherwise `Pending`.
  pub fn vote_result(&self, votes: impl Fn(I) -> Option<bool> + Copy) -> VoteResult {
    let r1 = self.incoming.vote_result(votes);
    let r2 = self.outgoing.vote_result(votes);
    if r1 == r2 {
      return r1;
    }
    if r1 == VoteResult::Lost || r2 == VoteResult::Lost {
      return VoteResult::Lost;
    }
    // One side Won, the other Pending → whole outcome is Pending.
    VoteResult::Pending
  }
}

#[cfg(test)]
mod tests;
