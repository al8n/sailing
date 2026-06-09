//! The durable Raft metadata: `(term, vote, commit)`, persisted before acting.
use crate::{Index, Term};

/// Durable Raft metadata. `vote` keeps `Option` (the documented `Copy`-scalar exception:
/// `Some(node)` ≠ `None`). Generic params carry no bounds (bounds live on methods).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HardState<I> {
  term: Term,
  vote: Option<I>,
  commit: Index,
}

impl<I> HardState<I> {
  /// The initial durable state of a fresh node.
  #[inline(always)]
  pub const fn initial() -> Self {
    Self {
      term: Term::ZERO,
      vote: None,
      commit: Index::ZERO,
    }
  }

  /// The current term.
  #[inline(always)]
  pub const fn term(&self) -> Term {
    self.term
  }

  /// The committed index.
  #[inline(always)]
  pub const fn commit(&self) -> Index {
    self.commit
  }

  /// Replace the term (consuming builder).
  #[inline(always)]
  #[must_use]
  pub const fn with_term(mut self, term: Term) -> Self {
    self.term = term;
    self
  }

  /// Replace the committed index (consuming builder).
  #[inline(always)]
  #[must_use]
  pub const fn with_commit(mut self, commit: Index) -> Self {
    self.commit = commit;
    self
  }
}

impl<I: Copy> HardState<I> {
  /// Whom this node voted for in `term`, if anyone.
  #[inline(always)]
  pub const fn vote(&self) -> Option<I> {
    self.vote
  }

  /// Replace the vote (consuming builder).
  #[inline(always)]
  #[must_use]
  pub const fn with_vote(mut self, vote: Option<I>) -> Self {
    self.vote = vote;
    self
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn hard_state_defaults_and_accessors() {
    let hs = HardState::<u64>::initial();
    assert_eq!(hs.term(), Term::ZERO);
    assert_eq!(hs.vote(), None);
    assert_eq!(hs.commit(), Index::ZERO);
    let hs = hs
      .with_term(Term::new(3))
      .with_vote(Some(7))
      .with_commit(Index::new(2));
    assert_eq!(hs.term(), Term::new(3));
    assert_eq!(hs.vote(), Some(7));
    assert_eq!(hs.commit(), Index::new(2));
  }
}
