//! Application-facing outputs drained via `Endpoint::poll_event`.
use crate::{Index, Term};

/// A committed `Normal` entry was applied; `response` is the `StateMachine::Response`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Applied<R> {
  index: Index,
  response: R,
}

impl<R> Applied<R> {
  /// Construct.
  pub const fn new(index: Index, response: R) -> Self {
    Self { index, response }
  }

  /// The applied index.
  #[inline(always)]
  pub const fn index(&self) -> Index {
    self.index
  }

  /// The apply result.
  #[inline(always)]
  pub const fn response(&self) -> &R {
    &self.response
  }

  /// Consume into `(index, response)`.
  #[inline(always)]
  pub fn into_parts(self) -> (Index, R) {
    (self.index, self.response)
  }
}

/// The leader changed (soft-state; for routing/observability).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaderChanged<I> {
  term: Term,
  leader: Option<I>,
}

impl<I: Copy> LeaderChanged<I> {
  /// Construct.
  pub const fn new(term: Term, leader: Option<I>) -> Self {
    Self { term, leader }
  }

  /// The term of the change.
  #[inline(always)]
  pub const fn term(&self) -> Term {
    self.term
  }

  /// The new leader, if known.
  #[inline(always)]
  pub const fn leader(&self) -> Option<I> {
    self.leader
  }
}

/// Outputs the application observes. `#[non_exhaustive]` — `ConfChanged`, `ReadState`,
/// `SnapshotInstalled` (design §6.3) are added additively in later milestones.
#[derive(
  Debug, Clone, PartialEq, Eq, derive_more::IsVariant, derive_more::Unwrap, derive_more::TryUnwrap,
)]
#[unwrap(ref, ref_mut)]
#[try_unwrap(ref, ref_mut)]
#[non_exhaustive]
pub enum Event<I, R> {
  /// A committed entry was applied.
  Applied(Applied<R>),
  /// The leader changed.
  LeaderChanged(LeaderChanged<I>),
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn event_construct_and_classify() {
    let e: Event<u64, u32> = Event::Applied(Applied::new(crate::Index::new(3), 99u32));
    assert!(e.is_applied());
    let lc: Event<u64, u32> =
      Event::LeaderChanged(LeaderChanged::new(crate::Term::new(2), Some(1u64)));
    assert!(lc.is_leader_changed());
  }
}
