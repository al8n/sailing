//! Storage seams. The driver owns the impls and passes them by `&mut`. Reads are
//! synchronous (no durability-ordering constraint); writes are deferred — `submit_*`
//! queues work, `poll()` drains completions (drained by `Endpoint::handle_storage`).
use crate::{Entry, HardState, Index, NodeId, SnapshotMeta, Term};
use bytes::Bytes;
use core::ops::Range;
use std::vec::Vec;

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

/// Whether a bounded `handle_storage` call left more storage completions queued.
#[derive(Debug, Clone, Copy, PartialEq, Eq, derive_more::IsVariant)]
pub enum StorageProgress {
  /// Both completion queues drained within the per-call budget — the driver may sleep.
  Drained,
  /// The per-call budget was hit with completions still queued — the driver must re-drive
  /// without sleeping so no single call monopolizes the run loop. The un-processed completions
  /// stay queued (poll() is a stateful FIFO) and are processed next call — never dropped/reordered.
  MorePending,
}

/// The result of a [`LogStore::entries`] range read: resident entries (owned or borrowed) or a
/// cold-read deferral.
///
/// Deliberately NOT `#[non_exhaustive]`: `Ready` and `Pending` require distinct handling at every
/// call site (serve vs defer), so a future variant SHOULD break consumers' matches rather than fall
/// silently into a catch-all — there is no safe default for an unknown read outcome.
pub enum EntriesRead<'a> {
  /// Entries for the requested range (the range-read contract on [`LogStore::entries`]), borrowed
  /// (resident — zero-copy) or owned (the store materialised them, e.g. decoded from cold storage).
  Ready(crate::MaybeOwned<'a, [Entry]>),
  /// The in-domain range is NOT resident; the store has begun fetching it. A BENIGN, retryable
  /// answer (NOT an error): the core makes no progress on this range now and re-reads it on a later
  /// pump. A fully-resident store NEVER returns this.
  Pending,
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

  /// Entries in `range`, as an [`EntriesRead`] holding a CONTIGUOUS run of entries (borrowed when
  /// resident, owned when the store materialised them) beginning exactly at `range.start`.
  ///
  /// **Range-read contract (NORMATIVE)** — the clauses below describe the entries inside
  /// [`EntriesRead::Ready`]:
  /// - **Contiguous, range-aligned:** when the result is non-empty, `slice[0].index() == range.start`
  ///   and `slice[k].index() == range.start + k`. The result is a PREFIX of `[range.start, range.end)` —
  ///   never a suffix, never reordered, never with a gap. Callers (apply, replication, the restart scans)
  ///   advance by `slice.last().index().next()` and rely on this alignment.
  /// - **May be a prefix (byte cap):** the slice is capped at roughly `max_bytes` (payload bytes), but
  ///   ALWAYS contains at least one entry when the range is non-empty and in view. A caller that needs the
  ///   whole range MUST loop, re-reading `slice.last().index().next()..range.end` until it is drained.
  ///   With `max_bytes == u64::MAX` the cap cannot fire, so the whole in-range portion comes back in one
  ///   call (returning MORE than `max_bytes` is also allowed — "roughly").
  /// - **Width-bounded requests:** the byte cap charges PAYLOAD bytes only, so it does not bound the entry
  ///   COUNT for zero-payload entries (no-ops, empty/conf). The core therefore bounds the WIDTH of every
  ///   multi-entry committed-range request it issues (apply, replication, the restart scans) — a store
  ///   materialising an OWNED result allocates a bounded count per call regardless of payload, and the
  ///   caller re-reads the remainder. Single-entry reads (the lease/election anchors) are bounded trivially.
  /// - **Three `Ok` outcomes (NORMATIVE):** `Ready(non-empty)` serves; `Ready(empty)` means "no entries
  ///   in view for this range" (e.g. `range.start > last_index()`, or a committed entry not yet in the
  ///   durable read view) — a BENIGN, retryable answer; [`EntriesRead::Pending`] means the in-domain range
  ///   EXISTS but is NOT resident and the store has begun fetching it — also benign and retryable, but
  ///   DISTINCT from empty (empty asserts there is nothing here; `Pending` asserts there is, just cold).
  ///   A store MUST NOT report a cold range as `Ready(empty)`. `Err` is reserved for genuine storage faults
  ///   (I/O, corruption) and is FATAL: the core poisons (fail-stop, `PoisonReason::LogRead`) on any error.
  /// - **Cold-read obligation (NORMATIVE):** a store that returns `Pending` MUST eventually return `Ready`
  ///   (or `Err` on a genuine fault) for that range, and signal the driver via its storage-ready seam so the
  ///   core re-pumps; a never-resolving `Pending` is a contract violation (the liveness analogue of a store
  ///   that never makes a committed read available). A fully-resident store NEVER returns `Pending`.
  /// - **Restart is resident-only (NORMATIVE):** during [`Endpoint::restart`](crate::Endpoint::restart)
  ///   the store MUST keep `[first_index(), last_index()]` resident — the synchronous lease-floor scans
  ///   cannot defer, and a partial scan would under-size the post-election commit-wait (a stale-read break).
  ///   A `Pending` (or empty) in-range read during restart is treated as a fatal `LogRead` poison.
  ///
  /// **Domain contract:** the core only requests ranges within the retained log
  /// (`first_index() <= range.start` and `range.end <= last_index() + 1`).
  fn entries(&self, range: Range<Index>, max_bytes: u64) -> Result<EntriesRead<'_>, Self::Error>;

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
  /// core to emit a spurious `AppendResponse`, potentially advancing the leader's commit past
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

  /// Whether a subsequent [`poll`](Self::poll) would return `Some` — i.e. at least one completion
  /// is queued and ready to drain RIGHT NOW.
  ///
  /// NORMATIVE: reports the READY-TO-POLL queue depth, NOT un-durable work. `poll()` is a FIFO, so
  /// this is exactly "`poll()` would yield `Some`": an async store that accepted a `submit_*` but has
  /// not yet made it durable (no completion enqueued) MUST return `false` here until its fsync lands
  /// and the completion is queued — else the driver hot-spins (never sleeps). The core checks this at
  /// the END of `handle_storage`, so any enqueue from any site this call (a submission whose
  /// completion lands after its drain phase, a post-drain `compact`, a coordinator bridge-dispatched
  /// submit) is caught by construction. MUST be cheap (no I/O) and side-effect-free. No default — a
  /// store must answer for its own queue (`false`-default would silently stall, `true` would hot-spin).
  fn has_pending(&self) -> bool;
}

/// The result of a [`StableStore::snapshot_chunk`] read: resident chunk bytes or a cold-read deferral.
///
/// Like [`EntriesRead`], deliberately NOT `#[non_exhaustive]`: `Ready` and `Pending` demand distinct
/// handling at the send site (stream the chunk vs defer), so a future variant SHOULD break the match
/// rather than fall silently into a catch-all.
pub enum SnapshotChunkRead {
  /// A contiguous run of the snapshot blob beginning at the requested `offset`, capped at roughly the
  /// requested length. The blob IS `Bytes`, so a resident store returns an O(1) refcount slice and a cold
  /// store returns the materialised bytes; either moves into the outgoing `InstallSnapshot` without a copy.
  /// EMPTY iff `offset >= total_len` (the benign tail an over-cursor degenerates to).
  Ready(Bytes),
  /// The requested run is in-domain but NOT resident; the store has begun fetching it. A BENIGN, retryable
  /// answer (NOT an error): the leader sends nothing now and re-drives on the storage-ready seam. A
  /// fully-resident store NEVER returns this.
  Pending,
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

  /// Read up to `len` bytes of the latest SUBMITTED snapshot starting at byte `offset`, with its
  /// metadata and total length — the bounded-read counterpart of [`snapshot`](Self::snapshot) for
  /// streaming a snapshot to a lagging follower one chunk at a time.
  ///
  /// Returns `None` if no snapshot exists (like [`snapshot`](Self::snapshot)). Otherwise the tuple is
  /// `(meta, total_len, chunk)`: `total_len` is the full blob length and `chunk` is the requested run.
  ///
  /// **Range-read contract (NORMATIVE)** — mirrors [`LogStore::entries`]:
  /// - **Contiguous, offset-aligned:** a non-empty [`Ready`](SnapshotChunkRead::Ready) holds exactly the
  ///   bytes at `[offset, offset + n)` for some `n <= len` — a PREFIX, never a suffix, never with a gap.
  ///   The caller resumes at `offset + n`, driven by the follower's `acked_through`.
  /// - **May be a prefix (cap):** capped at roughly `len`; a non-empty in-range request returns at least
  ///   one byte. `Ready(empty)` iff `offset >= total_len` (the benign tail).
  /// - **Three `Ok` outcomes:** `Ready(non-empty)` streams; `Ready(empty)` means `offset >= total_len`;
  ///   [`Pending`](SnapshotChunkRead::Pending) means the run EXISTS but is not resident and the store began
  ///   fetching it — benign and retryable, but DISTINCT from empty. A store MUST NOT report a cold run as
  ///   `Ready(empty)`. `Err` is a genuine store fault and is FATAL: the core poisons
  ///   (`PoisonReason::SnapshotRead`).
  /// - **Cold-read obligation:** a store that returns `Pending` MUST eventually return `Ready` (or `Err`)
  ///   for that run and signal the storage-ready seam so the core re-pumps; a never-resolving `Pending`
  ///   wedges the transfer. A fully-resident store NEVER returns `Pending`.
  ///
  /// **No default:** every store MUST implement this, so a non-resident (disk/mmap) store cannot SILENTLY
  /// inherit a whole-blob send — which would defeat the bounded-send guarantee at the first send. A
  /// fully-resident store delegates to [`resident_snapshot_chunk`](Self::resident_snapshot_chunk) (an O(1)
  /// `Bytes` slice, never a copy); a non-resident store pages in ONE chunk (returning `Pending` while
  /// cold), so its per-peer send footprint is one chunk, not the whole blob.
  // The `(meta, total_len, chunk-read)` tuple inside `Option<Result<..>>` is the deliberate read
  // contract documented above (snapshot present?, total length, and the resident/cold chunk); naming a
  // type alias for it would obscure rather than clarify the three returned facts.
  #[allow(clippy::type_complexity)]
  fn snapshot_chunk(
    &self,
    offset: u64,
    len: u64,
  ) -> Option<Result<(SnapshotMeta<Self::NodeId>, u64, SnapshotChunkRead), Self::Error>>;

  /// RESIDENT-store helper for [`snapshot_chunk`](Self::snapshot_chunk): reads the whole resident blob via
  /// [`snapshot`](Self::snapshot) and slices it (an O(1) `Bytes` slice, never a copy). A fully-resident
  /// store implements `snapshot_chunk` as `self.resident_snapshot_chunk(offset, len)`. A store whose
  /// snapshot is NOT fully resident MUST NOT use this — it would materialize the whole blob and defeat the
  /// bounded-send guarantee — and should page in one chunk instead.
  #[allow(clippy::type_complexity)]
  fn resident_snapshot_chunk(
    &self,
    offset: u64,
    len: u64,
  ) -> Option<Result<(SnapshotMeta<Self::NodeId>, u64, SnapshotChunkRead), Self::Error>> {
    self.snapshot().map(|(meta, blob)| {
      let total = blob.len() as u64;
      let start = offset.min(total) as usize;
      let end = offset.saturating_add(len).min(total) as usize;
      Ok((
        meta,
        total,
        SnapshotChunkRead::Ready(blob.slice(start..end)),
      ))
    })
  }

  /// Stage one snapshot chunk under the key `meta.last_index()`, writing `data` at byte `offset`.
  /// `Ok` is the highest CONTIGUOUS byte offset now staged (drives `SnapshotResponse.acked_through`).
  ///
  /// Idempotent on a re-delivered `offset`; a STRICTLY-NEWER key (a higher `meta.last_index()`, or the same
  /// boundary with a different `total_len`) supersedes and discards an older partial. The core ALSO discards
  /// explicitly via [`discard_snapshot_staging`](Self::discard_snapshot_staging) when a transfer is replaced
  /// (e.g. a new leader's LOWER snapshot) or becomes redundant. The store bounds its OWN staging (a disk
  /// store by disk, an in-RAM store by RAM) and returns `Err` on capacity exhaustion — the core treats that
  /// as fatal (a node crash → CFT failover), NOT a protocol-level cap.
  ///
  /// The returned contiguous offset drives IN-SESSION resume — a lost chunk re-sends from it, not from `0`.
  /// Staging is VOLATILE across RESTART, however: a store MAY persist it internally, but the core does NOT
  /// resume a partial across a crash (no recovery API restores the transfer identity) — it calls
  /// `discard_snapshot_staging` on restart, which MUST remove any persisted partial. Staging is SEPARATE
  /// from the durable snapshot slot — a crash mid-transfer loses only staging, never the durable log.
  /// [`SnapshotStaging`] is the reference accumulator a store can embed.
  fn accept_snapshot_chunk(
    &mut self,
    meta: &SnapshotMeta<Self::NodeId>,
    total_len: u64,
    offset: u64,
    data: &Bytes,
  ) -> Result<u64, Self::Error>;

  /// CONSUME the fully-staged blob keyed at `meta.last_index()` once its contiguous-staged length
  /// reaches `total_len` — the bytes the core decodes and re-submits via [`submit_snapshot`]. Returns
  /// `None` (leaving any partial staging in place) if no COMPLETE staged blob is keyed there. On `Some`
  /// the store MUST drop its staging accumulator and hand back ownership, so the chunked install never
  /// retains a second full-snapshot buffer past completion.
  ///
  /// [`submit_snapshot`]: Self::submit_snapshot
  fn take_staged_snapshot(&mut self, meta: &SnapshotMeta<Self::NodeId>) -> Option<Bytes>;

  /// DISCARD any in-progress chunked-snapshot staging, freeing the `SnapshotStaging` buffer. The core calls
  /// this when an in-flight transfer becomes REDUNDANT (the recoverable prefix caught up past its boundary),
  /// when a DIFFERENT transfer supersedes it, and on RESTART. A no-op if nothing is staged.
  ///
  /// Chunk staging is VOLATILE: it need NOT survive a process restart. A store MAY persist it, but the core
  /// does not RESUME a durable partial across restart (there is no recovery API to restore the transfer
  /// identity) — it calls this on restart to discard any orphan, so a persisted partial cannot outlive the
  /// `snapshot_recv` that tracks it and block a fresh post-restart transfer.
  fn discard_snapshot_staging(&mut self);

  /// Drain the next completion, if any.
  fn poll(&mut self) -> Option<Result<StableDone, Self::Error>>;

  /// Whether a subsequent [`poll`](Self::poll) would return `Some` — i.e. at least one completion
  /// is queued and ready to drain RIGHT NOW.
  ///
  /// NORMATIVE: reports the READY-TO-POLL queue depth, NOT un-durable work. `poll()` is a FIFO, so
  /// this is exactly "`poll()` would yield `Some`": an async store that accepted a `submit_*` but has
  /// not yet made it durable (no completion enqueued) MUST return `false` here until its fsync lands
  /// and the completion is queued — else the driver hot-spins (never sleeps). The core checks this at
  /// the END of `handle_storage`, so any enqueue from any site this call (a submission whose
  /// completion lands after its drain phase, a post-drain `compact`, a coordinator bridge-dispatched
  /// submit) is caught by construction. MUST be cheap (no I/O) and side-effect-free. No default — a
  /// store must answer for its own queue (`false`-default would silently stall, `true` would hot-spin).
  fn has_pending(&self) -> bool;
}

/// Cap on the number of DISJOINT staged byte-ranges a single transfer may hold. A sane chunked transfer
/// keeps very few runs (sailing sends one chunk per ack and resumes from the contiguous cursor, so the
/// runs collapse as the prefix grows); an adversarial leader flooding tiny out-of-order chunks would
/// otherwise grow the interval metadata to many times the (already bounded) byte buffer and OOM. Past this
/// cap [`SnapshotStaging::accept`] returns `None`, so the embedding store returns an error and the core
/// fail-stops rather than the process aborting.
pub(crate) const MAX_STAGING_RUNS: usize = 1024;

/// A reusable accumulator for chunked snapshot staging — the reference implementation a [`StableStore`]
/// embeds to satisfy [`accept_snapshot_chunk`](StableStore::accept_snapshot_chunk) /
/// [`take_staged_snapshot`](StableStore::take_staged_snapshot). It tracks written byte runs and reports the
/// highest CONTIGUOUS offset; the embedding store keys ONE of these per in-flight transfer and discards
/// it when a strictly-newer boundary supersedes it.
#[derive(Debug)]
pub struct SnapshotStaging {
  boundary: Index,
  buf: Vec<u8>,
  /// Sorted, non-overlapping written byte ranges `[start, end)`.
  runs: Vec<(u64, u64)>,
}

impl SnapshotStaging {
  /// Begin staging a `total_len`-byte blob covered by snapshot boundary `boundary`, bounded by
  /// `max_bytes`. Returns `None` if `total_len` overflows `usize`, exceeds `max_bytes` (WITHOUT
  /// allocating), or the receiver buffer cannot be allocated — so a malformed/forged length OR genuine
  /// memory pressure POISONS (the embedding store maps `None` to its fatal error) rather than panicking or
  /// ABORTING the process. The buffer allocation is FALLIBLE (`try_reserve_exact`): a `len` at or under
  /// `max_bytes` can still exceed available memory, and an infallible `vec![0u8; len]` would abort on OOM,
  /// bypassing the fail-stop path.
  #[must_use]
  pub fn new(boundary: Index, total_len: u64, max_bytes: usize) -> Option<Self> {
    let len = usize::try_from(total_len).ok()?;
    if len > max_bytes {
      return None;
    }
    let mut buf = Vec::new();
    buf.try_reserve_exact(len).ok()?;
    buf.resize(len, 0u8);
    Some(Self {
      boundary,
      buf,
      runs: Vec::new(),
    })
  }

  /// The snapshot boundary (`meta.last_index()`) this staging is keyed on.
  #[inline]
  pub const fn boundary(&self) -> Index {
    self.boundary
  }

  /// The full blob length this staging is accumulating (the `total_len` it was created with) — part of
  /// the transfer identity, so a store can detect a same-boundary different-length supersede.
  #[inline]
  pub fn total_len(&self) -> u64 {
    self.buf.len() as u64
  }

  /// Write `data` at byte `offset` (clamped to the buffer) and return the highest CONTIGUOUS staged
  /// offset. Idempotent on a re-delivered range. Returns `None` if tracking this range would exceed
  /// [`MAX_STAGING_RUNS`] disjoint runs (an adversarially fragmented transfer): the caller maps that to a
  /// store error so the core fail-stops, rather than the interval metadata growing many times the buffer.
  #[must_use]
  pub fn accept(&mut self, offset: u64, data: &[u8]) -> Option<u64> {
    // Clamp in u64 BEFORE narrowing: `offset as usize` would WRAP a > usize::MAX offset to a low value on
    // 32-bit, overwriting the wrong bytes; `usize::try_from` saturates past-the-end (a no-op) instead.
    let start = usize::try_from(offset)
      .unwrap_or(usize::MAX)
      .min(self.buf.len());
    let end = start.saturating_add(data.len()).min(self.buf.len());
    self.buf[start..end].copy_from_slice(&data[..end - start]);
    self.insert_run(start as u64, end as u64)?;
    Some(self.contiguous())
  }

  /// The highest contiguous staged offset (the end of the run starting at `0`, else `0`).
  #[inline]
  pub fn contiguous(&self) -> u64 {
    match self.runs.first() {
      Some(&(0, end)) => end,
      _ => 0,
    }
  }

  /// Whether the whole blob is staged (`contiguous() == total_len`).
  #[inline]
  pub fn is_complete(&self) -> bool {
    self.contiguous() == self.buf.len() as u64
  }

  /// The staged bytes (meaningful once [`is_complete`](Self::is_complete) holds).
  #[inline]
  pub fn bytes(&self) -> &[u8] {
    &self.buf
  }

  /// CONSUME the staging, returning the full blob buffer (meaningful once [`is_complete`] holds) — used
  /// by a store's `take_staged_snapshot` to hand ownership to the core without a copy.
  ///
  /// [`is_complete`]: Self::is_complete
  #[must_use]
  pub fn into_vec(self) -> Vec<u8> {
    self.buf
  }

  fn insert_run(&mut self, start: u64, end: u64) -> Option<()> {
    if start >= end {
      return Some(());
    }
    self.runs.push((start, end));
    self.runs.sort_unstable_by_key(|&(s, _)| s);
    let mut merged: Vec<(u64, u64)> = Vec::with_capacity(self.runs.len());
    for &(s, e) in &self.runs {
      match merged.last_mut() {
        Some(last) if s <= last.1 => last.1 = last.1.max(e),
        _ => merged.push((s, e)),
      }
    }
    // Reject a transfer that stays this fragmented: a sane chunked transfer collapses to a few runs, so
    // exceeding the cap signals an adversarial flood of disjoint chunks. Return None (the caller maps it to
    // a store error → the core fail-stops) rather than letting the interval metadata grow unbounded.
    if merged.len() > MAX_STAGING_RUNS {
      return None;
    }
    self.runs = merged;
    Some(())
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[allow(dead_code)]
  fn assert_log<L: LogStore>() {}
  #[allow(dead_code)]
  fn assert_stable<S: StableStore>() {}

  #[test]
  fn snapshot_staging_tracks_contiguous_runs() {
    let mut s = SnapshotStaging::new(Index::new(10), 6, 1024).unwrap();
    assert_eq!(s.accept(0, b"ab"), Some(2));
    assert_eq!(
      s.accept(4, b"ef"),
      Some(2),
      "a gap at [2,4) holds the contiguous watermark"
    );
    assert!(!s.is_complete());
    assert_eq!(
      s.accept(2, b"cd"),
      Some(6),
      "filling the gap completes the run"
    );
    assert!(s.is_complete());
    assert_eq!(s.bytes(), b"abcdef");
  }

  #[test]
  fn snapshot_staging_handles_overlap_and_clamp() {
    let mut s = SnapshotStaging::new(Index::new(7), 5, 1024).unwrap();
    assert_eq!(s.accept(0, b"abc"), Some(3));
    // An OVERLAPPING range coalesces to the union end (no double-count).
    assert_eq!(s.accept(2, b"XYZ"), Some(5), "[0,3) + [2,5) coalesces to 5");
    assert!(s.is_complete());
    // An idempotent re-delivery does not regress the watermark.
    assert_eq!(s.accept(0, b"a"), Some(5));
    // An offset+len past the buffer CLAMPS (no panic, no growth).
    assert_eq!(s.accept(4, b"OVERLONG"), Some(5));
    // A write entirely past the end is a no-op.
    assert_eq!(s.accept(99, b"z"), Some(5));
    // A huge offset saturates to past-the-end (no wrap to a low offset, no wrong-byte write) — the
    // 32-bit `offset as usize` truncation guard.
    assert_eq!(s.accept(u64::MAX, b"x"), Some(5));
    assert_eq!(s.into_vec().len(), 5);
  }

  #[test]
  fn snapshot_staging_caps_fragmentation() {
    // An adversarially fragmented transfer (many tiny DISJOINT chunks, each separated by a 1-byte gap) is
    // rejected once it would exceed MAX_STAGING_RUNS disjoint runs, so the interval metadata stays bounded
    // and the store returns an error instead of the process OOM-aborting.
    let total = (MAX_STAGING_RUNS as u64 + 1) * 2;
    let mut s = SnapshotStaging::new(Index::new(1), total, total as usize).unwrap();
    // A 1-byte write at every even offset is disjoint from the last (odd-byte gaps), so each adds a run.
    for i in 0..MAX_STAGING_RUNS as u64 {
      assert!(
        s.accept(i * 2, b"x").is_some(),
        "a run within the cap must be accepted"
      );
    }
    // One more disjoint run would exceed the cap → None (the caller maps this to a fatal store error).
    assert_eq!(
      s.accept(MAX_STAGING_RUNS as u64 * 2, b"y"),
      None,
      "exceeding MAX_STAGING_RUNS disjoint runs is rejected, not grown unbounded"
    );
  }

  #[test]
  fn snapshot_staging_new_rejects_oversize() {
    assert!(SnapshotStaging::new(Index::new(1), 8, 16).is_some());
    assert!(
      SnapshotStaging::new(Index::new(1), 17, 16).is_none(),
      "a total_len beyond the cap must be rejected WITHOUT allocating (no OOM on a forged length)"
    );
  }

  #[test]
  fn snapshot_staging_new_returns_none_on_unallocatable_len() {
    // A `total_len` AT OR UNDER `max_bytes` but beyond addressable memory must fail-stop (return None via
    // the fallible `try_reserve_exact`), NOT abort the process on an infallible allocation.
    let huge = 1usize << 60; // ~1 EiB — unallocatable, yet <= usize::MAX so it clears the cap check
    assert!(
      SnapshotStaging::new(Index::new(1), huge as u64, usize::MAX).is_none(),
      "an unallocatable (but under-cap) length must return None, not abort"
    );
  }

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

  /// `has_pending` reports READY-TO-POLL, never un-durable work: a `submit_append` accepted but not
  /// yet flushed (its `Appended` completion HELD, modelling a deferred fsync) must read `false`, and
  /// only once the fsync lands and the completion is enqueued does it read `true`. This is the
  /// anti-hot-spin contract — were `has_pending` to count an un-flushed submit, the driver would
  /// never sleep on a store whose fsync is still in flight.
  #[test]
  fn has_pending_excludes_unflushed_submits() {
    use crate::{Entry, EntryKind, testkit::VecLog};
    let mut log = VecLog::default();
    // No completion is queued, and the (visible-but-)held submit below enqueues none either.
    assert!(!log.has_pending(), "an empty queue has nothing to poll");

    log.hold_appends(true);
    log.submit_append(
      OpId::new(1),
      &[Entry::new(
        Term::new(1),
        Index::new(1),
        EntryKind::Empty,
        bytes::Bytes::new(),
      )],
    );
    assert!(
      !log.has_pending(),
      "a submitted-but-unflushed append has enqueued no completion: nothing to poll yet"
    );

    // The deferred fsync lands: the completion is enqueued and is now ready to drain.
    log.flush_held_appends();
    assert!(
      log.has_pending(),
      "once the fsync completion is enqueued, poll() would yield Some"
    );
    assert!(log.poll().is_some());
    assert!(
      !log.has_pending(),
      "draining the sole completion empties the queue again"
    );
  }

  /// The stable-store companion to [`has_pending_excludes_unflushed_submits`]: `has_pending` tracks the
  /// stable completion queue — true EXACTLY when `poll()` would yield `Some`. AsyncStable enqueues the
  /// completion at `submit_write` (an instant-completion store), so it reads `true` until drained; a
  /// torn-fsync `submit_snapshot` enqueues NO completion, so it correctly stays `false`.
  #[test]
  fn has_pending_tracks_the_stable_completion_queue() {
    use crate::{ConfState, testkit::AsyncStable};
    let mut stable = AsyncStable::default();
    assert!(
      !stable.has_pending(),
      "an empty stable queue has nothing to poll"
    );

    // A normal submit_write enqueues its `Wrote` completion (durability is observed when poll drains it).
    let hs = stable.hard_state().with_term(Term::new(1));
    stable.submit_write(OpId::new(1), hs);
    assert!(
      stable.has_pending(),
      "a submit_write's enqueued completion is ready to poll"
    );
    assert!(stable.poll().is_some());
    assert!(
      !stable.has_pending(),
      "draining the completion empties the queue"
    );

    // A torn fsync makes the blob submit-visible but enqueues NO completion: nothing to poll.
    stable.fail_next_snapshot_durability();
    let meta = SnapshotMeta::new(
      Index::new(1),
      Term::new(1),
      ConfState::from_voters(std::vec![1u64]),
    );
    stable.submit_snapshot(OpId::new(2), meta, bytes::Bytes::new());
    assert!(
      !stable.has_pending(),
      "a torn-fsync submit_snapshot enqueues no completion: nothing to poll"
    );
  }
}
