//! Cluster configuration state. M5 ships the minimal voter-set form; M6 extends to full
//! joint-consensus (`ConfChangeV2`).
use crate::NodeId;
use std::{collections::BTreeSet, vec::Vec};

/// The configuration of a Raft cluster (M5: voter set only).
///
/// M6 adds joint-consensus fields (`voters_outgoing`, learners, …). The struct is
/// `#[non_exhaustive]` so adding fields is non-breaking at the pattern level.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ConfState<I> {
  voters: Vec<I>,
}

impl<I: NodeId> ConfState<I> {
  /// Construct from a voter list (de-duplicated and sorted for determinism).
  pub fn new(voters: Vec<I>) -> Self {
    let sorted: BTreeSet<I> = voters.into_iter().collect();
    Self {
      voters: sorted.into_iter().collect(),
    }
  }

  /// The current voter set (sorted).
  #[inline(always)]
  pub fn voters(&self) -> &[I] {
    &self.voters
  }

  /// Whether `id` is a voter.
  #[inline(always)]
  pub fn is_voter(&self, id: &I) -> bool {
    self.voters.contains(id)
  }

  /// Number of voters.
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

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn conf_state_deduplicates_and_sorts() {
    let c = ConfState::new(std::vec![3u64, 1u64, 2u64, 1u64]);
    assert_eq!(c.voters(), &[1u64, 2u64, 3u64]);
    assert_eq!(c.len(), 3);
    assert!(c.is_voter(&2u64));
    assert!(!c.is_voter(&99u64));
  }

  #[test]
  fn conf_state_empty() {
    let c = ConfState::<u64>::new(std::vec![]);
    assert!(c.is_empty());
  }
}
