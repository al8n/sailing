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
    let mut srt: std::vec::Vec<Index> = self.ids.iter().map(|id| acked(*id)).collect();
    srt.sort_unstable();
    // The element at position `n - (n/2 + 1)` is the highest index held by a majority.
    // Worked example: n=3, q=2, pos=1 → srt[1] is acked by the top-2 members (indices 1
    // and 2 in ascending order), so it is on a majority.
    let pos = n - (n / 2 + 1);
    srt[pos]
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
mod tests {
  use super::*;

  // Helper: build MajorityConfig from a slice of IDs.
  fn mc(ids: &[u64]) -> MajorityConfig<u64> {
    MajorityConfig::new(ids.iter().copied().collect())
  }

  // Helper: build JointConfig with explicit incoming/outgoing.
  fn jc(incoming: &[u64], outgoing: &[u64]) -> JointConfig<u64> {
    JointConfig::new(mc(incoming), mc(outgoing))
  }

  // --- MajorityConfig::committed_index ---

  #[test]
  fn majority_committed_empty_returns_max() {
    let c = mc(&[]);
    assert_eq!(c.committed_index(|_| Index::new(0)), Index::new(u64::MAX));
  }

  #[test]
  fn majority_committed_single_member() {
    // n=1, pos=0: committed = that member's ack.
    let c = mc(&[1]);
    assert_eq!(
      c.committed_index(|id| Index::new(if id == 1 { 7 } else { 0 })),
      Index::new(7)
    );
  }

  #[test]
  fn majority_committed_three_members() {
    // n=3, pos=1 (median): sorted [10,12,14] → srt[1] = 12.
    let c = mc(&[1, 2, 3]);
    let acked = |id| match id {
      1 => Index::new(10),
      2 => Index::new(12),
      3 => Index::new(14),
      _ => Index::ZERO,
    };
    assert_eq!(c.committed_index(acked), Index::new(12));
  }

  #[test]
  fn majority_committed_five_members() {
    // n=5, pos=2: sorted [8,10,12,14,16] → srt[2] = 12.
    let c = mc(&[1, 2, 3, 4, 5]);
    let acked = |id| match id {
      1 => Index::new(10),
      2 => Index::new(12),
      3 => Index::new(14),
      4 => Index::new(16),
      5 => Index::new(8),
      _ => Index::ZERO,
    };
    assert_eq!(c.committed_index(acked), Index::new(12));
  }

  #[test]
  fn majority_committed_zero_drags_down() {
    // n=3, pos=1: sorted [0,12,14] → srt[1] = 12.
    // Member 1 hasn't acked (returns 0), so sorted is [0,12,14] and pos=1 gives 12.
    let c = mc(&[1, 2, 3]);
    let acked = |id| match id {
      1 => Index::ZERO,
      2 => Index::new(12),
      3 => Index::new(14),
      _ => Index::ZERO,
    };
    assert_eq!(c.committed_index(acked), Index::new(12));
  }

  #[test]
  fn majority_committed_all_zero() {
    // n=3, pos=1: sorted [0,0,0] → srt[1] = 0.
    let c = mc(&[1, 2, 3]);
    assert_eq!(c.committed_index(|_| Index::ZERO), Index::ZERO);
  }

  // --- MajorityConfig::vote_result ---

  #[test]
  fn majority_vote_empty_wins() {
    let c = mc(&[]);
    assert_eq!(c.vote_result(|_| None), VoteResult::Won);
  }

  #[test]
  fn majority_vote_won() {
    // n=3, q=2: 2 grants → Won.
    let c = mc(&[1, 2, 3]);
    let votes = |id| match id {
      1 => Some(true),
      2 => Some(true),
      3 => Some(false),
      _ => None,
    };
    assert_eq!(c.vote_result(votes), VoteResult::Won);
  }

  #[test]
  fn majority_vote_lost() {
    // n=3, q=2: 2 rejections → Lost (grants=1, missing=0, grants+missing=1 < 2).
    let c = mc(&[1, 2, 3]);
    let votes = |id| match id {
      1 => Some(true),
      2 => Some(false),
      3 => Some(false),
      _ => None,
    };
    assert_eq!(c.vote_result(votes), VoteResult::Lost);
  }

  #[test]
  fn majority_vote_pending() {
    // n=3, q=2: grants=1, missing=1 → grants+missing=2 >= q → Pending.
    let c = mc(&[1, 2, 3]);
    let votes = |id| match id {
      1 => Some(true),
      2 => None,
      3 => Some(false),
      _ => None,
    };
    assert_eq!(c.vote_result(votes), VoteResult::Pending);
  }

  #[test]
  fn majority_vote_all_missing() {
    // n=3, q=2: grants=0, missing=3 → Pending.
    let c = mc(&[1, 2, 3]);
    assert_eq!(c.vote_result(|_| None), VoteResult::Pending);
  }

  // --- JointConfig::committed_index ---

  #[test]
  fn joint_committed_non_joint_equals_incoming() {
    // outgoing is empty → sentinel u64::MAX; min(incoming, MAX) = incoming.
    let c = JointConfig::from_voters([1u64, 2, 3].iter().copied().collect());
    let acked = |id| match id {
      1 => Index::new(10),
      2 => Index::new(12),
      3 => Index::new(14),
      _ => Index::ZERO,
    };
    assert_eq!(c.committed_index(acked), Index::new(12));
  }

  #[test]
  fn joint_committed_takes_min_of_halves() {
    // incoming {1,2,3} acked {1→10,2→15,3→13} → sorted [10,13,15], pos=1 → 13.
    // outgoing {3,4,5} acked {3→13,4→11,5→12} → sorted [11,12,13], pos=1 → 12.
    // Joint: min(13,12) = 12.
    let c = jc(&[1, 2, 3], &[3, 4, 5]);
    let acked = |id| match id {
      1 => Index::new(10),
      2 => Index::new(15),
      3 => Index::new(13),
      4 => Index::new(11),
      5 => Index::new(12),
      _ => Index::ZERO,
    };
    assert_eq!(c.committed_index(acked), Index::new(12));
  }

  #[test]
  fn joint_committed_classic_etcd_case() {
    // incoming {1,2,3} with acks that give 13, outgoing {4,5,6} that give 11 → joint 11.
    // incoming sorted [11,13,15], pos=1 → 13.
    // outgoing sorted [10,11,12], pos=1 → 11.
    let c = jc(&[1, 2, 3], &[4, 5, 6]);
    let acked = |id| match id {
      1 => Index::new(11),
      2 => Index::new(13),
      3 => Index::new(15),
      4 => Index::new(10),
      5 => Index::new(11),
      6 => Index::new(12),
      _ => Index::ZERO,
    };
    assert_eq!(c.committed_index(acked), Index::new(11));
  }

  // --- JointConfig::vote_result ---

  #[test]
  fn joint_vote_both_won() {
    let c = jc(&[1, 2, 3], &[4, 5, 6]);
    // incoming: 1,2 grant (Won); outgoing: 4,5 grant (Won).
    let votes = |id| match id {
      1 | 2 => Some(true),
      3 => Some(false),
      4 | 5 => Some(true),
      6 => Some(false),
      _ => None,
    };
    assert_eq!(c.vote_result(votes), VoteResult::Won);
  }

  #[test]
  fn joint_vote_one_half_lost() {
    let c = jc(&[1, 2, 3], &[4, 5, 6]);
    // incoming: 1 grant, 2,3 reject → Lost; outgoing: 4,5 grant → Won.
    let votes = |id| match id {
      1 => Some(true),
      2 | 3 => Some(false),
      4 | 5 => Some(true),
      6 => Some(false),
      _ => None,
    };
    assert_eq!(c.vote_result(votes), VoteResult::Lost);
  }

  #[test]
  fn joint_vote_one_won_one_pending() {
    let c = jc(&[1, 2, 3], &[4, 5, 6]);
    // incoming: 1,2 grant → Won; outgoing: 4 grants, 5 missing → Pending.
    let votes = |id| match id {
      1 | 2 => Some(true),
      3 => Some(false),
      4 => Some(true),
      5 => None,
      6 => Some(false),
      _ => None,
    };
    assert_eq!(c.vote_result(votes), VoteResult::Pending);
  }

  #[test]
  fn joint_vote_non_joint_behaves_as_single() {
    // outgoing empty → Won vacuously; result is the incoming result.
    let c = JointConfig::from_voters([1u64, 2, 3].iter().copied().collect());
    let votes = |id| match id {
      1 | 2 => Some(true),
      3 => Some(false),
      _ => None,
    };
    assert_eq!(c.vote_result(votes), VoteResult::Won);
  }
}
