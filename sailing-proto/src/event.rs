//! Application-facing outputs drained via `Endpoint::poll_event`.
use crate::{CheapClone, ConfState, Index, ReadOnlyOption, ReadState, SnapshotMeta, Term};

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
///
/// Fires on EVERY observable change of the leader belief, including to-`None` transitions: a
/// campaign start, a check-quorum step-down, a higher-term adoption, and a leader's removal by
/// conf change all make a known leader unknown, and they all emit — an embedder routing on the
/// hint never has to infer leader loss from silence. A higher-term message from a leader
/// surfaces an ordered pair in one drain: `(term, None)` when the term is adopted, then
/// `(term, Some(sender))` when the handler installs the sender — the honest transition
/// sequence. Identity-deduplicated: an unchanged belief never re-emits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaderChanged<I> {
  term: Term,
  leader: Option<I>,
}

impl<I: CheapClone> LeaderChanged<I> {
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
  pub fn leader(&self) -> Option<I> {
    self.leader.cheap_clone()
  }
}

/// A `ConfChange` entry was committed and applied; the cluster configuration has changed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfChanged<I> {
  /// The log index of the applied `ConfChange` entry.
  index: Index,
  /// The new (post-change) configuration state.
  conf: ConfState<I>,
}

impl<I: Clone> ConfChanged<I> {
  /// Construct.
  pub fn new(index: Index, conf: ConfState<I>) -> Self {
    Self { index, conf }
  }

  /// The log index of the applied `ConfChange` entry.
  #[inline(always)]
  pub fn index(&self) -> Index {
    self.index
  }

  /// The new configuration state after applying the change.
  #[inline(always)]
  pub fn conf(&self) -> &ConfState<I> {
    &self.conf
  }
}

/// A `SetReadMode` entry was committed and applied; the active read mode changed (a mid-life migration).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadModeChanged {
  /// The log index of the applied `SetReadMode` entry.
  index: Index,
  /// The new active read mode.
  mode: ReadOnlyOption,
}

impl ReadModeChanged {
  /// Construct.
  pub fn new(index: Index, mode: ReadOnlyOption) -> Self {
    Self { index, mode }
  }

  /// The log index of the applied `SetReadMode` entry.
  #[inline(always)]
  pub fn index(&self) -> Index {
    self.index
  }

  /// The new active read mode after applying the change.
  #[inline(always)]
  pub fn mode(&self) -> ReadOnlyOption {
    self.mode
  }
}

/// Outputs the application observes.
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
  /// A snapshot was successfully installed on this node (follower receive path).
  /// The payload is the metadata of the installed snapshot.
  SnapshotInstalled(SnapshotMeta<I>),
  /// A `ConfChange` entry was committed and applied; the cluster membership changed.
  ConfChanged(ConfChanged<I>),
  /// A `SetReadMode` entry was committed and applied; the active read mode changed.
  ReadModeChanged(ReadModeChanged),
  /// A linearizable read index has been confirmed.  The application may serve the
  /// associated read once `applied >= ReadState.index`.
  ReadState(ReadState),
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn event_construct_and_classify() {
    let e: Event<u64, u32> = Event::Applied(Applied::new(Index::new(3), 99u32));
    assert!(e.is_applied());
    let lc: Event<u64, u32> = Event::LeaderChanged(LeaderChanged::new(Term::new(2), Some(1u64)));
    assert!(lc.is_leader_changed());
  }

  #[test]
  fn applied_accessors_and_into_parts() {
    // `response()` borrows the apply result; `into_parts()` decomposes to `(index, response)` in order —
    // the two ways an embedder extracts a committed `Normal` entry's outcome.
    let a = Applied::new(Index::new(3), std::string::String::from("ok"));
    assert_eq!(a.index(), Index::new(3));
    assert_eq!(a.response(), "ok");
    let (index, response) = a.into_parts();
    assert_eq!(index, Index::new(3));
    assert_eq!(response, "ok");
  }

  #[test]
  fn read_state_event_construct_and_classify() {
    use crate::ReadState;
    let rs = ReadState::new(Index::new(7), bytes::Bytes::from_static(b"ctx"));
    let ev: Event<u64, u32> = Event::ReadState(rs.clone());
    assert!(ev.is_read_state());
    assert!(!ev.is_applied());
    assert!(!ev.is_leader_changed());
    // Unwrap gives back the ReadState.
    let rs2 = ev.unwrap_read_state_ref();
    assert_eq!(rs2.index(), Index::new(7));
    assert_eq!(rs2.context().as_ref(), b"ctx");
  }

  #[test]
  fn conf_changed_construct_and_classify() {
    use crate::conf::ConfState;
    let conf = ConfState::from_voters(std::vec![1u64, 2u64, 3u64]);
    let cc = ConfChanged::new(Index::new(5), conf.clone());
    assert_eq!(cc.index(), Index::new(5));
    assert_eq!(cc.conf(), &conf);
    let ev: Event<u64, u32> = Event::ConfChanged(cc);
    assert!(ev.is_conf_changed());
    assert!(!ev.is_applied());
    assert!(!ev.is_leader_changed());
    assert!(!ev.is_snapshot_installed());
  }

  #[test]
  fn snapshot_installed_event_construct_and_classify() {
    use crate::{SnapshotMeta, conf::ConfState};
    let meta = SnapshotMeta::new(
      Index::new(10),
      Term::new(4),
      ConfState::from_voters(std::vec![1u64, 2u64, 3u64]),
    );
    let ev: Event<u64, u32> = Event::SnapshotInstalled(meta.clone());
    assert!(ev.is_snapshot_installed());
    assert!(!ev.is_applied());
    assert!(!ev.is_leader_changed());
    // Unwrap gives back the meta
    assert_eq!(
      ev.unwrap_snapshot_installed_ref().last_index(),
      meta.last_index()
    );
    assert_eq!(
      ev.unwrap_snapshot_installed_ref().last_term(),
      meta.last_term()
    );
  }
}
