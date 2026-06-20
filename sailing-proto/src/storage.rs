//! Storage seams. The driver owns the impls and passes them by `&mut`. Reads are
//! synchronous (no durability-ordering constraint); writes are deferred — `submit_*`
//! queues work, `poll()` drains completions (drained by `Endpoint::handle_storage`).
use crate::{Entry, HardState, Index, NodeId, SnapshotMeta, Term};
use bytes::Bytes;
use core::ops::Range;

/// A storage-submission correlation id, echoed back on completion.
///
/// An `OpId` carries the node's BOOT EPOCH (the strictly-increasing per-restart counter the driver
/// supplies to [`Endpoint::restart`](crate::Endpoint::restart)) as its HIGH-ORDER component, with a
/// per-incarnation `seq` as the low-order. This makes a completion enqueued by a PRIOR incarnation — one
/// that survives into a new incarnation because the store did not clear its completion queue on crash —
/// impossible to mistake for a current op: its lower `epoch` makes it UNEQUAL to (so it misses every
/// `pending`/inflight map lookup) and STRICTLY LESS THAN (so it fails every `>=` durability-watermark
/// check) every current-incarnation id. `epoch` is declared before `seq` so the derived `Ord` is
/// epoch-major. (A fresh node uses epoch 0; `restart` seeds `seq=0` of the supplied boot epoch.)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct OpId {
  epoch: u64,
  seq: u64,
}

impl OpId {
  /// The zero id (epoch 0, seq 0) — the fresh-node seed and the watermark sentinel.
  pub const ZERO: Self = Self { epoch: 0, seq: 0 };

  /// An id with sequence `v` in epoch 0 — the store/test constructor (production ids come from
  /// `mint_op_id`, seeded via [`Self::first_of_epoch`] at restart).
  #[inline(always)]
  pub const fn new(v: u64) -> Self {
    Self { epoch: 0, seq: v }
  }

  /// The first id (`seq = 0`) of boot `epoch` — seeded into the op-id counter at
  /// [`Endpoint::restart`](crate::Endpoint::restart) so this incarnation's ids strictly exceed every prior
  /// incarnation's (boot epochs strictly increase).
  #[inline(always)]
  pub const fn first_of_epoch(epoch: u64) -> Self {
    Self { epoch, seq: 0 }
  }

  /// The boot epoch this id belongs to.
  #[inline(always)]
  pub const fn epoch(self) -> u64 {
    self.epoch
  }

  /// The per-incarnation sequence number.
  #[inline(always)]
  pub const fn seq(self) -> u64 {
    self.seq
  }

  /// The per-incarnation sequence number (alias of [`Self::seq`] retained for existing callers).
  #[inline(always)]
  pub const fn get(self) -> u64 {
    self.seq
  }

  /// The next id in the SAME epoch (saturating `seq`).
  #[inline(always)]
  pub const fn next(self) -> Self {
    Self {
      epoch: self.epoch,
      seq: self.seq.saturating_add(1),
    }
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
  ///
  /// **Domain contract (NORMATIVE):** the core routinely probes indices OUTSIDE the retained
  /// entries with PEER-CONTROLLED or pre-quorum values — a stale leader's `prev_log_index` below
  /// the compaction point after a snapshot install, commit-candidate index `0` on a fresh leader
  /// before any ack, a reject hint beyond the log. The full domain an implementation MUST answer
  /// with `Ok` is:
  ///
  /// - `index == first_index() - 1` (the compaction/snapshot boundary): return the boundary term
  ///   retained from the covering snapshot (`Term::ZERO` for the empty-log origin, index 0);
  /// - `first_index() <= index <= last_index()`: the entry's term;
  /// - any OTHER index (compacted below the boundary, or above `last_index()`): return
  ///   `Ok(Term::ZERO)` — **never** `Err`. `Term::ZERO` is unambiguous here: no real entry
  ///   carries it, and the core's consistency checks treat it as "unknown/absent".
  ///
  /// `Err` is reserved for genuine storage faults (I/O error, corruption) and is FATAL: the core
  /// poisons the node (fail-stop) on any term-read error. A store that returns `Err` for routine
  /// out-of-domain probes will be permanently poisoned by ordinary protocol traffic.
  fn term(&self, index: Index) -> Result<Term, Self::Error>;

  /// Entries in `range`, as a CONTIGUOUS borrowed slice beginning exactly at `range.start`.
  ///
  /// **Range-read contract (NORMATIVE):**
  /// - **Contiguous, range-aligned:** when the result is non-empty, `slice[0].index() == range.start`
  ///   and `slice[k].index() == range.start + k`. The result is a PREFIX of `[range.start, range.end)` —
  ///   never a suffix, never reordered, never with a gap. Callers (apply, replication, the restart scans)
  ///   advance by `slice.last().index().next()` and rely on this alignment.
  /// - **May be a prefix (byte cap):** the slice is capped at roughly `max_bytes` (payload bytes), but
  ///   ALWAYS contains at least one entry when the range is non-empty and in view. A caller that needs the
  ///   whole range MUST loop, re-reading `slice.last().index().next()..range.end` until it is drained.
  ///   With `max_bytes == u64::MAX` the cap cannot fire, so the whole in-range portion comes back in one
  ///   call (returning MORE than `max_bytes` is also allowed — "roughly").
  /// - **Empty vs error:** an empty slice means "no entries in view for this range" (e.g.
  ///   `range.start > last_index()`, or a committed entry not yet in the durable read view) — a BENIGN,
  ///   retryable answer, NOT an error. `Err` is reserved for genuine storage faults (I/O, corruption) and
  ///   is FATAL: the core poisons (fail-stop) on any `entries` error.
  ///
  /// **Domain contract:** the core only requests ranges within the retained log
  /// (`first_index() <= range.start` and `range.end <= last_index() + 1`).
  fn entries(&self, range: Range<Index>, max_bytes: u64) -> Result<&[Entry], Self::Error>;

  /// Queue an append (truncating any conflicting suffix first). Durable on the matching `poll`
  /// (a [`LogDone::Appended`] for this `id`).
  ///
  /// **Durability is prefix-ordered (NORMATIVE):** a Raft log is a sequential record, so making the
  /// entry at index `N` durable implies every entry in `first_index()..=N` is also durable. A
  /// [`LogDone::Appended`] completion for an append whose highest index is `N` therefore guarantees
  /// the WHOLE durable prefix through `N` — not merely the entries this one `submit_append` carried.
  /// The core relies on this for persist-before-ack: it tracks a watermark = the highest index any
  /// `Appended` has reported, and a follower acks its match only up to that watermark. An
  /// implementation that reported `Appended` for index `N` while some earlier index `< N` were still
  /// crash-losable would let the leader count a phantom durable replica and commit an entry a crash
  /// could lose (a non-quorum-durable commit). Completions MAY arrive in any order, but each one's
  /// index MUST already be backed by a durable prefix. Disk-backed logs satisfy this automatically by
  /// appending and flushing in index order; an implementation that flushes out of order MUST hold a
  /// completion back until its prefix is durable.
  fn submit_append(&mut self, id: OpId, entries: &[Entry]);

  /// Drop entries at and below `up_to` (post-snapshot GC).
  ///
  /// **Durability ordering (NORMATIVE):** `compact` discards committed log entries, so an
  /// implementation MUST NOT make the compaction durable before the snapshot blob covering
  /// `up_to` (persisted via `StableStore::submit_snapshot`) is itself durable — otherwise a crash
  /// in between loses entries that no durable snapshot replaces. The core already enforces this
  /// ordering by deferring the `compact` call until the matching `SnapshotWritten` completion (or,
  /// if that completion is missed, until `StableStore::snapshot()` reports a durable snapshot whose
  /// `last_index >= up_to`); a disk-backed implementation must not weaken it by
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
  /// This ordering is **REQUIRED for crash-safety, not advisory**. The core does NOT silently
  /// re-sync from a half-installed snapshot: if a crash leaves the log re-baselined
  /// (`first_index() > 1`) with no durable snapshot to baseline the discarded prefix, the committed
  /// entries below `first_index` are gone, so [`Endpoint::restart`](crate::Endpoint::restart)
  /// **fail-stops** — it poisons (`PoisonReason::OrphanedLog`) rather than bootstrap from the static
  /// config and serve a log whose committed prefix is unrecoverable. An implementation that cannot
  /// order the two durabilities cannot crash-safely install snapshots; the node it leaves behind
  /// after such a crash must be re-provisioned by the driver (wiped and re-added), not silently
  /// recovered.
  fn restore(&mut self, last_index: Index, last_term: Term);

  /// Drain the next completion, if any.
  fn poll(&mut self) -> Option<Result<LogDone, Self::Error>>;
}

/// The durable-metadata store (term/vote/commit + snapshot blobs).
///
/// Borrow strength on the command surface follows what each method actually needs:
/// append-only / read-only methods (e.g. `propose`, `read_index`, `transfer_leader`) take
/// `&S`; only methods that write durable term/vote/commit (e.g. `handle_timeout`,
/// `handle_message`, `handle_storage`) take `&mut S`.
pub trait StableStore {
  /// The node-id type stored in the vote.
  type NodeId: NodeId;
  /// A failure reading the store (fatal to the node).
  type Error;

  /// The current durable hard state (synchronous read).
  ///
  /// NORMATIVE for out-of-tree DISK impls: [`HardState::lease_support`] is the three-valued
  /// [`crate::LeaseSupport`]. When (de)serializing, preserve the THREE cases distinctly:
  /// `Recorded(None)` (a current-format node that promised nothing), `Recorded(Some(d))` (a promise of `d`),
  /// and — CRITICALLY — a genuine PRE-`lease_support` (legacy) blob MUST decode to `Unrecorded`, **never**
  /// to `Recorded(None)`. `Unrecorded` triggers the conservative restart fence (and `restart_migrating`'s
  /// operator-supplied prior), so a freshly-upgraded node is never less safe than before; decoding a legacy
  /// blob as `Recorded(None)` would assert "promised nothing" and reopen the disruptive-vote-inside-a-live-
  /// lease hole for one post-upgrade restart of a previously-enforcing node. In-tree impls store `HardState`
  /// by value (no serialization), so they preserve all three cases automatically.
  fn hard_state(&self) -> HardState<Self::NodeId>;

  /// Queue a hard-state write. Durable on the matching `poll` (completions are ordered).
  fn submit_write(&mut self, id: OpId, hard_state: HardState<Self::NodeId>);

  /// Queue a snapshot write. Completes as `StableDone::SnapshotWritten(id)`.
  fn submit_snapshot(&mut self, id: OpId, meta: SnapshotMeta<Self::NodeId>, data: Bytes);

  /// Read the latest SUBMITTED snapshot (synchronous). Returns `None` if no snapshot exists.
  ///
  /// This is the VISIBLE/optimistic slot: `submit_snapshot` makes its blob readable here IMMEDIATELY,
  /// before the write is durable. Use it for serving/streaming, NOT for durability decisions —
  /// see [`durable_snapshot`](Self::durable_snapshot).
  fn snapshot(&self) -> Option<(SnapshotMeta<Self::NodeId>, Bytes)>;

  /// Metadata of the last DURABLE (fsync'd) snapshot — `None` until a submitted snapshot is actually
  /// durable (synchronous read).
  ///
  /// CONTRACT (NORMATIVE): this MUST reflect only blobs that have reached stable storage — it advances
  /// at the same point the matching `StableDone::SnapshotWritten` becomes true, NEVER at `submit_snapshot`
  /// time. It is therefore distinct from [`snapshot`](Self::snapshot) (the submit-visible slot): a store
  /// that has made a blob visible-but-not-durable returns `Some` from `snapshot()` and `None` (or the
  /// PRIOR durable meta) from `durable_snapshot()`. A sync store (every write immediately durable) returns
  /// the same meta from both.
  ///
  /// The core relies on this — NEVER on `snapshot()` — to confirm a snapshot blob is durable before it
  /// runs the destructive log re-baseline of a deferred install (it defers the whole install until the
  /// `SnapshotWritten` completion, or, if that completion is missed/coalesced, until THIS reports the
  /// boundary durable). Returning the visible (pre-fsync) blob here would let a crash orphan the log — the
  /// exact ordering hole this method closes. Returns owned metadata (no `Bytes` — the install needs only
  /// the boundary, and the blob was already handed to the SM at `submit_snapshot`).
  fn durable_snapshot(&self) -> Option<SnapshotMeta<Self::NodeId>>;

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
    let s = NoopStable::<u64>::default();
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
