//! Storage seams. The driver owns the impls and passes them by `&mut`. Reads are
//! synchronous (no durability-ordering constraint); writes are deferred — `submit_*`
//! queues work, `poll()` drains completions (drained by `Endpoint::handle_storage`).
use crate::{Entry, HardState, Index, NodeId, SnapshotMeta, Term};
use bytes::Bytes;
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
  ///
  /// **Durability ordering (NORMATIVE):** `compact` discards committed log entries, so an
  /// implementation MUST NOT make the compaction durable before the snapshot blob covering
  /// `up_to` (persisted via `StableStore::submit_snapshot`) is itself durable — otherwise a crash
  /// in between loses entries that no durable snapshot replaces. The core already enforces this
  /// ordering by deferring the `compact` call until the matching `SnapshotWritten` completion (or,
  /// if that completion is missed, until `StableStore::snapshot()` reports a durable snapshot whose
  /// `last_index >= up_to` — review I9); a disk-backed implementation must not weaken it by
  /// flushing the compaction ahead of the blob.
  fn compact(&mut self, up_to: Index);

  /// Discard the entire log and re-baseline it on an installed snapshot.
  ///
  /// After this call:
  /// - `first_index() == last_index + 1`
  /// - `last_index()  == last_index`
  /// - `term(last_index)` returns `last_term` (the snapshot boundary term)
  ///
  /// This is the receiving-side counterpart to `compact`: whereas `compact` assumes the
  /// entry at `up_to` is present in the log (it reads its term), `restore` accepts an
  /// explicit `last_term` so it works even when the follower never had the entry.
  ///
  /// **Re-baseline is immediate (synchronous):** the updated read-view (`first_index`,
  /// `last_index`, `term`) takes effect before this call returns. This keeps the log
  /// mutually consistent with the caller's already-advanced `commit`/`applied` watermarks,
  /// which `apply_committed` reads synchronously.
  ///
  /// **Completion discipline:** any in-flight `submit_append` completions for indices that
  /// are now below the new `first_index` MUST be dropped (not returned by future `poll`
  /// calls). Returning a stale `Appended` completion for a discarded index would cause the
  /// core to emit a spurious `AppendResp`, potentially advancing the leader's commit past
  /// what the follower has actually stored.
  ///
  /// **Durability ordering (NORMATIVE — disk-backed implementations):** `restore` re-baselines
  /// the log read-view IMMEDIATELY (synchronous, in-memory). A durable (disk-backed)
  /// implementation **MUST NOT** make the re-baseline durable before the corresponding snapshot
  /// blob (persisted separately via `StableStore::submit_snapshot`) is itself durable — otherwise
  /// a crash in the window between the two leaves the log discarded with no snapshot to recover
  /// from, and the node has neither its old entries nor a usable snapshot.
  ///
  /// An implementation that cannot guarantee this ordering MUST instead rely on **restart
  /// re-sync**: on restart, if no durable snapshot is found, the node re-syncs the discarded
  /// entries from the leader. This is safe because every discarded entry was below the leader's
  /// commit (i.e. quorum-committed) at the time of the `restore`, so Leader Completeness
  /// guarantees the leader still holds them and will re-replicate. (This is the path relied upon
  /// by M5-U3.) Combined with commit persistence (review C1), a restart recovers the real commit
  /// watermark and re-syncs correctly even when the blob was not yet durable at crash time.
  fn restore(&mut self, last_index: Index, last_term: Term);

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

  /// Queue a snapshot write. Completes as `StableDone::SnapshotWritten(id)`.
  fn submit_snapshot(&mut self, id: OpId, meta: SnapshotMeta<Self::NodeId>, data: Bytes);

  /// Read the latest persisted snapshot (synchronous). Returns `None` if no snapshot exists.
  fn snapshot(&self) -> Option<(SnapshotMeta<Self::NodeId>, Bytes)>;

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

  #[test]
  fn stable_store_submit_snapshot_and_read_via_noop_stable() {
    // NoopStable has no snapshot; snapshot() returns None
    use crate::testkit::NoopStable;
    let s = NoopStable::default();
    assert!(s.snapshot().is_none());
  }

  #[test]
  fn stable_store_submit_snapshot_roundtrip_via_async_stable() {
    use crate::{SnapshotMeta, conf::ConfState, testkit::AsyncStable};
    let mut s = AsyncStable::default();
    assert!(s.snapshot().is_none()); // no snapshot yet

    let meta = SnapshotMeta::new(
      Index::new(5),
      Term::new(2),
      ConfState::from_voters(std::vec![1u64, 2u64]),
    );
    let data = bytes::Bytes::from_static(b"state");
    s.submit_snapshot(OpId::new(1), meta.clone(), data.clone());

    // completion is enqueued
    assert_eq!(
      s.poll(),
      Some(Ok(StableDone::SnapshotWritten(OpId::new(1))))
    );

    // snapshot is now readable
    let (rmeta, rdata) = s.snapshot().unwrap();
    assert_eq!(rmeta.last_index(), meta.last_index());
    assert_eq!(rmeta.last_term(), meta.last_term());
    assert_eq!(rdata, data);
  }
}
