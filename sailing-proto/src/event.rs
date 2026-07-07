//! Application-facing outputs drained via `Endpoint::poll_event`.
use crate::{CheapClone, ConfState, Index, ReadOnlyOption, ReadState, SnapshotMeta, Term};
use bytes::Bytes;

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

/// A `Split` entry was committed and applied: the state machine partitioned itself at the
/// deterministic point and the forked half is STAGED for materialization (the multi container
/// relays it; the fork durability barrier holds the parent's snapshots until the child's baseline
/// is locally durable). G-FREE by design — `child` is the child group id's canonical `Data`
/// encoding, because events are drained through the group-unaware core; the typed id surfaces on
/// the drivers' lifecycle tail instead. A single-group embedder that never proposes splits never
/// sees this event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SplitApplied {
  /// The log index of the applied `Split` entry.
  index: Index,
  /// The child group id's canonical `Data` encoding (1..=1024 bytes, the group-tag bound).
  child: Bytes,
}

impl SplitApplied {
  /// Construct.
  pub const fn new(index: Index, child: Bytes) -> Self {
    Self { index, child }
  }

  /// The log index of the applied `Split` entry.
  #[inline(always)]
  pub const fn index(&self) -> Index {
    self.index
  }

  /// The child group id's canonical `Data` encoding (an O(1) shared handle).
  #[inline(always)]
  pub fn child(&self) -> Bytes {
    self.child.clone()
  }
}

/// A committed `Split` entry applied as a DETERMINISTIC NO-OP: its minted lineage
/// (`parent_gen_after`) is not the live counter's successor, so the mint was STALE — a second
/// split proposed before an earlier one applied (both read the same pre-apply counter), or a
/// replayed retry duplicate. The state machine is untouched (`split` is never invoked), no fork
/// is staged, and no snapshot fence is armed; the guard's inputs are replicated state, so every
/// replica no-ops the same entry identically. The embedder observes this and re-proposes the
/// split if it still wants it — the propose-time one-in-flight gate makes the event rare.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SplitStale {
  /// The log index of the no-op'd `Split` entry.
  index: Index,
  /// The child group id's canonical `Data` encoding, as the entry carried it.
  child: Bytes,
  /// The entry's minted parent lineage (`parent_gen_after`).
  minted_gen: u64,
  /// The live lineage counter at the apply point (the mint had to be exactly this + 1).
  shape_gen: u64,
}

impl SplitStale {
  /// Construct.
  pub const fn new(index: Index, child: Bytes, minted_gen: u64, shape_gen: u64) -> Self {
    Self {
      index,
      child,
      minted_gen,
      shape_gen,
    }
  }

  /// The log index of the no-op'd `Split` entry.
  #[inline(always)]
  pub const fn index(&self) -> Index {
    self.index
  }

  /// The child group id's canonical `Data` encoding (an O(1) shared handle).
  #[inline(always)]
  pub fn child(&self) -> Bytes {
    self.child.clone()
  }

  /// The entry's minted parent lineage.
  #[inline(always)]
  pub const fn minted_gen(&self) -> u64 {
    self.minted_gen
  }

  /// The live lineage counter at the apply point.
  #[inline(always)]
  pub const fn shape_gen(&self) -> u64 {
    self.shape_gen
  }
}

/// A `PrepareMerge` entry was committed and applied: this group is now FROZEN as a merge source.
/// Carries the post-freeze lineage so a driver can MIRROR the move into its engine record the
/// moment it applies (INV-LINEAGE: the engine-durable counter tracks every applied lineage move,
/// so the ordinary removal floor covers every generation a freeze ever minted).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MergeFrozen {
  /// The log index of the applied `PrepareMerge` entry.
  index: Index,
  /// The group's lineage counter after the freeze (`source_gen_after`).
  gen_after: u64,
}

impl MergeFrozen {
  /// Construct.
  pub const fn new(index: Index, gen_after: u64) -> Self {
    Self { index, gen_after }
  }

  /// The log index of the applied `PrepareMerge` entry.
  #[inline(always)]
  pub const fn index(&self) -> Index {
    self.index
  }

  /// The group's lineage counter after the freeze.
  #[inline(always)]
  pub const fn gen_after(&self) -> u64 {
    self.gen_after
  }
}

/// A SOURCE-role `RollbackMerge` entry was committed and applied: the freeze is undone. Carries
/// the post-thaw lineage for the same driver mirror as [`MergeFrozen`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MergeRolledBack {
  /// The log index of the applied `RollbackMerge` (unfreeze) entry.
  index: Index,
  /// The group's lineage counter after the thaw (`source_gen_after`).
  gen_after: u64,
}

impl MergeRolledBack {
  /// Construct.
  pub const fn new(index: Index, gen_after: u64) -> Self {
    Self { index, gen_after }
  }

  /// The log index of the applied `RollbackMerge` (unfreeze) entry.
  #[inline(always)]
  pub const fn index(&self) -> Index {
    self.index
  }

  /// The group's lineage counter after the thaw.
  #[inline(always)]
  pub const fn gen_after(&self) -> u64 {
    self.gen_after
  }
}

/// A `CommitMerge` entry RESOLVED: the frozen source group's state machine was absorbed into
/// this group at the parked entry, and this group now serves the union. G-FREE like
/// [`SplitApplied`] — `source` is the absorbed group id's canonical `Data` encoding; the typed id
/// surfaces on the drivers' lifecycle tail. The source group's endpoint is gone the moment this
/// fires (its id is floored terminally at the storage layer). `gen_after` is the TARGET's
/// lineage after the absorb — the driver's engine mirror, as on [`MergeFrozen`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Merged {
  /// The log index of the resolved `CommitMerge` entry.
  index: Index,
  /// The absorbed (source) group id's canonical `Data` encoding.
  source: Bytes,
  /// The absorbing target's lineage counter after the absorb (`target_gen_after`).
  gen_after: u64,
}

impl Merged {
  /// Construct.
  pub const fn new(index: Index, source: Bytes, gen_after: u64) -> Self {
    Self {
      index,
      source,
      gen_after,
    }
  }

  /// The log index of the resolved `CommitMerge` entry.
  #[inline(always)]
  pub const fn index(&self) -> Index {
    self.index
  }

  /// The absorbed (source) group id's canonical `Data` encoding (an O(1) shared handle).
  #[inline(always)]
  pub fn source(&self) -> Bytes {
    self.source.clone()
  }

  /// The absorbing target's lineage counter after the absorb.
  #[inline(always)]
  pub const fn gen_after(&self) -> u64 {
    self.gen_after
  }
}

/// A merge attempt against this group DIED deterministically: a `CommitMerge` no-op'd (the
/// source's log settled the race, or the entry is a replayed duplicate), a parked commit
/// resolved aborted, or a TARGET-role `RollbackMerge` abandoned the merge at its live mint.
/// Nothing was absorbed. `gen_after` is this group's lineage at the event — bumped only by the
/// target-role abort arm (its mint kills the raced commit at the lineage guard), unchanged at
/// the no-op arms; the driver mirrors it unconditionally (monotone-max makes the no-bump arms
/// free).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeAborted {
  /// The log index of the deciding entry.
  index: Index,
  /// The named (source) group id's canonical `Data` encoding.
  source: Bytes,
  /// This group's lineage counter after the event.
  gen_after: u64,
}

impl MergeAborted {
  /// Construct.
  pub const fn new(index: Index, source: Bytes, gen_after: u64) -> Self {
    Self {
      index,
      source,
      gen_after,
    }
  }

  /// The log index of the deciding entry.
  #[inline(always)]
  pub const fn index(&self) -> Index {
    self.index
  }

  /// The named (source) group id's canonical `Data` encoding (an O(1) shared handle).
  #[inline(always)]
  pub fn source(&self) -> Bytes {
    self.source.clone()
  }

  /// This group's lineage counter after the event.
  #[inline(always)]
  pub const fn gen_after(&self) -> u64 {
    self.gen_after
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
  /// A `Split` entry was committed and applied; a forked child half is staged for
  /// materialization.
  SplitApplied(SplitApplied),
  /// A committed `Split` entry no-op'd deterministically: its mint was stale against the live
  /// lineage counter. Nothing forked; re-propose if the split is still wanted.
  SplitStale(SplitStale),
  /// A `PrepareMerge` entry was committed and applied: this group is now FROZEN — it refuses
  /// proposals, conf changes, transfers, and reads (typed) until the merge resolves (the group
  /// is absorbed and removed) or an explicit rollback lands. Replication, elections, and
  /// snapshot sends run unchanged, so the freeze itself propagates and survives leader crashes.
  /// Carries the post-freeze lineage for the drivers' engine mirror.
  MergeFrozen(MergeFrozen),
  /// A `RollbackMerge` entry was committed and applied: the freeze is undone, proposals and
  /// reads resume, and any parked `CommitMerge` naming the old freeze aborts deterministically
  /// (the rollback moved the lineage counter past it). Leases are NOT resurrected — they re-form
  /// from live traffic. Carries the post-thaw lineage for the drivers' engine mirror.
  MergeRolledBack(MergeRolledBack),
  /// A parked `CommitMerge` RESOLVED: the source group was absorbed and this group serves the
  /// union from the resolved index on.
  Merged(Merged),
  /// A parked `CommitMerge` resolved as a deterministic no-op (the source's log settled the
  /// race, or the commit was a replayed duplicate). Nothing was absorbed.
  MergeAborted(MergeAborted),
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
  fn split_stale_construct_and_classify() {
    let s = SplitStale::new(Index::new(5), bytes::Bytes::from_static(b"\x07"), 1, 1);
    assert_eq!(s.index(), Index::new(5));
    assert_eq!(s.child().as_ref(), b"\x07");
    assert_eq!(s.minted_gen(), 1);
    assert_eq!(s.shape_gen(), 1);
    let ev: Event<u64, u32> = Event::SplitStale(s);
    assert!(ev.is_split_stale());
    assert!(!ev.is_split_applied());
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
