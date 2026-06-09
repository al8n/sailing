//! Storage seams. The driver owns the impls and passes them by `&mut`. Reads are
//! synchronous (no durability-ordering constraint); writes are deferred — `submit_*`
//! queues work, `poll()` drains completions (drained by `Endpoint::handle_storage`).
use crate::{Entry, HardState, Index, NodeId, Term};
use core::ops::Range;

/// A storage-submission correlation id, echoed back on completion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct OpId(u64);

impl OpId {
  /// The zero id.
  pub const ZERO: Self = Self(0);

  /// Wrap a raw value.
  #[inline(always)]
  pub const fn new(v: u64) -> Self {
    Self(v)
  }

  /// The raw value.
  #[inline(always)]
  pub const fn get(self) -> u64 {
    self.0
  }

  /// The next id (saturating).
  #[inline(always)]
  pub const fn next(self) -> Self {
    Self(self.0.saturating_add(1))
  }
}

/// A completed log write.
#[derive(
  Debug,
  Clone,
  Copy,
  PartialEq,
  Eq,
  derive_more::IsVariant,
  derive_more::Unwrap,
  derive_more::TryUnwrap,
)]
#[unwrap(ref, ref_mut)]
#[try_unwrap(ref, ref_mut)]
#[non_exhaustive]
pub enum LogDone {
  /// The `submit_append` with this id is durable.
  Appended(OpId),
  /// The log has been compacted below this index.
  Compacted(Index),
}

/// A completed stable-store write.
#[derive(
  Debug,
  Clone,
  Copy,
  PartialEq,
  Eq,
  derive_more::IsVariant,
  derive_more::Unwrap,
  derive_more::TryUnwrap,
)]
#[unwrap(ref, ref_mut)]
#[try_unwrap(ref, ref_mut)]
#[non_exhaustive]
pub enum StableDone {
  /// The `submit_write` with this id is durable.
  Wrote(OpId),
  /// The `submit_snapshot` with this id is durable.
  SnapshotWritten(OpId),
}

/// The replicated-log store. Reads are synchronous; appends are deferred.
pub trait LogStore {
  /// A failure reading the log (fatal to the node).
  type Error;

  /// The first index still present (entries below have been compacted into a snapshot).
  fn first_index(&self) -> Index;

  /// The last index present.
  fn last_index(&self) -> Index;

  /// The term of the entry at `index`.
  fn term(&self, index: Index) -> Result<Term, Self::Error>;

  /// Entries in `range`, capped at roughly `max_bytes` (always at least one if non-empty).
  fn entries(&self, range: Range<Index>, max_bytes: u64) -> Result<&[Entry], Self::Error>;

  /// Queue an append (truncating any conflicting suffix first). Durable on the matching `poll`.
  fn submit_append(&mut self, id: OpId, entries: &[Entry]);

  /// Drop entries at and below `up_to` (post-snapshot GC).
  fn compact(&mut self, up_to: Index);

  /// Drain the next completion, if any.
  fn poll(&mut self) -> Option<Result<LogDone, Self::Error>>;
}

/// The durable-metadata store (term/vote/commit + snapshot blobs).
pub trait StableStore {
  /// The node-id type stored in the vote.
  type NodeId: NodeId;
  /// A failure reading the store (fatal to the node).
  type Error;

  /// The current durable hard state (synchronous read).
  fn hard_state(&self) -> HardState<Self::NodeId>;

  /// Queue a hard-state write. Durable on the matching `poll` (completions are ordered).
  fn submit_write(&mut self, id: OpId, hard_state: HardState<Self::NodeId>);

  /// Drain the next completion, if any.
  fn poll(&mut self) -> Option<Result<StableDone, Self::Error>>;
}

#[cfg(test)]
mod tests {
  use super::*;

  #[allow(dead_code)]
  fn assert_log<L: LogStore>() {}
  #[allow(dead_code)]
  fn assert_stable<S: StableStore>() {}

  #[test]
  fn opid_increments() {
    let mut next = OpId::ZERO;
    let a = next;
    next = next.next();
    assert_ne!(a, next);
    assert_eq!(next.get(), 1);
  }
}
