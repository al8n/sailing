//! Throwaway store impls for proto-level unit tests. Not compiled outside `#[cfg(test)]`.
use crate::{
  EntriesRead, Entry, HardState, Index, LogDone, LogStore, MaybeOwned, NodeId, OpId,
  SnapshotChunkRead, SnapshotMeta, StableDone, StableStore, Term,
};
use bytes::Bytes;
use core::{cell::Cell, convert::Infallible, ops::Range, time::Duration};
use std::{collections::VecDeque, vec::Vec};

/// A no-op log that is always empty — last_index=0, term(any)=Term::ZERO.
///
/// `restore` is a degenerate no-op: the NoopLog carries no real state, so there is nothing
/// to discard.  `first_index`/`last_index`/`term` stay at their fixed degenerate values.
/// Tests that exercise restore behaviour must use `VecLog`.
#[derive(Debug)]
pub(crate) struct NoopLog;

impl LogStore for NoopLog {
  type Error = Infallible;

  fn first_index(&self) -> Index {
    Index::new(1)
  }

  fn last_index(&self) -> Index {
    Index::ZERO
  }

  fn term(&self, _index: Index) -> Result<Term, Self::Error> {
    Ok(Term::ZERO)
  }

  fn entries(&self, _range: Range<Index>, _max_bytes: u64) -> Result<EntriesRead<'_>, Self::Error> {
    Ok(EntriesRead::Ready(MaybeOwned::Borrowed(&[])))
  }

  fn submit_append(&mut self, _id: OpId, _entries: &[crate::Entry]) {}

  fn compact(&mut self, _up_to: Index) {}

  fn restore(&mut self, _last_index: Index, _last_term: Term) {
    // NoopLog carries no real state; the degenerate fixed values remain consistent.
  }

  fn poll(&mut self) -> Option<Result<LogDone, Self::Error>> {
    None
  }

  fn has_pending(&self) -> bool {
    false
  }
}

/// A mutating in-memory log for proto unit tests. Mirrors `sailing_simulation::MemLog`.
///
/// Offset model: `offset` is the compaction boundary (the index before `entries[0]`).
/// Starts at `Index::ZERO`. `compacted_term` is the term at `offset`.
#[derive(Debug, Default)]
pub(crate) struct VecLog {
  entries: Vec<Entry>,
  completions: VecDeque<LogDone>,
  /// Index before entries[0]. Starts at ZERO (no compaction).
  offset: Index,
  /// Term at offset (boundary term kept after compaction).
  compacted_term: Term,
  /// When set, `submit_append` makes entries VISIBLE immediately but HOLDS the `Appended` completion
  /// (models an async log whose fsync is deferred); `flush_held_appends()` releases them. Lets a test
  /// create `commit > durable_index` — a visible-but-unflushed tail.
  hold_appends: bool,
  held: VecDeque<OpId>,
}

impl VecLog {
  /// Seed the log with already-durable entries (no completion enqueued). Used in restart tests.
  pub(crate) fn force_append(&mut self, entries: &[Entry]) {
    for e in entries {
      let offset = self.offset.get();
      let fi = e.index().get();
      let from = if fi <= offset + 1 {
        0usize
      } else {
        (fi - offset - 1) as usize
      };
      self.entries.truncate(from);
      self.entries.push(e.clone());
    }
  }

  /// Hold `Appended` completions on subsequent `submit_append`s (model a deferred fsync) until
  /// `flush_held_appends()`. The appended entries are still VISIBLE immediately, so `commit` can advance
  /// over them while `durable_index` stays behind.
  pub(crate) fn hold_appends(&mut self, on: bool) {
    self.hold_appends = on;
  }

  /// Release all held `Appended` completions (the deferred fsync lands).
  pub(crate) fn flush_held_appends(&mut self) {
    while let Some(id) = self.held.pop_front() {
      self.completions.push_back(LogDone::Appended(id));
    }
  }
}

impl LogStore for VecLog {
  type Error = Infallible;

  fn first_index(&self) -> Index {
    Index::new(self.offset.get() + 1)
  }

  fn last_index(&self) -> Index {
    Index::new(self.offset.get() + self.entries.len() as u64)
  }

  fn term(&self, index: Index) -> Result<Term, Self::Error> {
    if index == self.offset {
      return Ok(self.compacted_term);
    }
    if index < self.offset {
      return Ok(Term::ZERO);
    }
    let last = self.last_index();
    if index > last {
      return Ok(Term::ZERO);
    }
    let pos = (index.get() - self.offset.get() - 1) as usize;
    Ok(self.entries[pos].term())
  }

  fn entries(&self, range: Range<Index>, _max_bytes: u64) -> Result<EntriesRead<'_>, Self::Error> {
    let start = range.start.get();
    let end = range.end.get();
    let offset = self.offset.get();
    let len = self.entries.len() as u64;
    let lo = if start <= offset {
      0usize
    } else {
      (start - offset - 1) as usize
    };
    let hi = if end <= offset {
      0usize
    } else {
      ((end - offset - 1).min(len)) as usize
    };
    let lo = lo.min(self.entries.len());
    let hi = hi.max(lo).min(self.entries.len());
    Ok(EntriesRead::Ready(MaybeOwned::Borrowed(
      &self.entries[lo..hi],
    )))
  }

  fn submit_append(&mut self, id: OpId, entries: &[Entry]) {
    if let Some(first) = entries.first() {
      debug_assert!(
        first.index().get() > self.offset.get(),
        "submit_append below the compaction offset"
      );
      let offset = self.offset.get();
      let fi = first.index().get();
      let from = if fi <= offset + 1 {
        0usize
      } else {
        (fi - offset - 1) as usize
      };
      self.entries.truncate(from);
    }
    self.entries.extend_from_slice(entries);
    if self.hold_appends {
      // Visible now, but the `Appended` completion is HELD (deferred fsync) — see `flush_held_appends`.
      self.held.push_back(id);
    } else {
      self.completions.push_back(LogDone::Appended(id));
    }
  }

  fn compact(&mut self, up_to: Index) {
    if up_to <= self.offset || self.entries.is_empty() {
      return;
    }
    let last = self.last_index();
    let up_to = if up_to > last { last } else { up_to };
    let boundary_term = self.term(up_to).unwrap_or(Term::ZERO);
    let drain_count = ((up_to.get() - self.offset.get()) as usize).min(self.entries.len());
    self.entries.drain(0..drain_count);
    self.offset = up_to;
    self.compacted_term = boundary_term;
    // Enqueue the `Compacted` completion the trait's completion discipline requires (every sibling
    // in-memory store does), so a `poll()` reports the compaction and `has_pending()` is faithful.
    self.completions.push_back(LogDone::Compacted(up_to));
  }

  fn restore(&mut self, last_index: Index, last_term: Term) {
    // Discard all entries: the follower's entire log is replaced by the snapshot.
    // Any pending completions for those appends are also dropped — they will never fire.
    self.entries.clear();
    self.completions.clear();
    self.held.clear();
    // Re-baseline: offset == last_index so that first_index() == last_index + 1
    // and term(last_index) == last_term (the snapshot boundary term).
    self.offset = last_index;
    self.compacted_term = last_term;
  }

  fn poll(&mut self) -> Option<Result<LogDone, Self::Error>> {
    self.completions.pop_front().map(Ok)
  }

  fn has_pending(&self) -> bool {
    // Ready-to-poll only: the `held` deque is un-flushed (no completion enqueued yet), so it is
    // excluded — counting it would make the driver hot-spin on a deferred-fsync tail.
    !self.completions.is_empty()
  }
}

/// A `VecLog` whose [`LogStore::term`] returns `Err(())` for one configurable index, modelling a
/// FATAL storage read failure (not "absent"). Used to prove the core poisons rather than
/// fabricating a default term. Its `Error` is `()` (a distinct, non-`Infallible` error type) so the
/// `Err` arm is actually reachable.
#[derive(Debug, Default)]
pub(crate) struct FailTermLog {
  inner: VecLog,
  /// When `Some(i)`, `term(i)` returns `Err(())`; every other index delegates to `inner`.
  fail_index: Option<Index>,
  /// When `Some(i)`, `entries(range)` returns `Err(())` for any range CONTAINING `i`; otherwise
  /// delegates to `inner`. Proves a fatal log-read mid-scan fail-stops rather than fabricating a default.
  fail_entries_index: Option<Index>,
  /// When `true`, `restore` is a NO-OP: it does NOT re-baseline `first_index` to `last_index + 1`,
  /// modelling a store that violates the restore contract — to prove a snapshot install fail-stops.
  skip_restore_rebaseline: bool,
  /// When `true`, `restore` re-baselines `first_index` correctly BUT retains a stale entry above the
  /// boundary (`last_index > n`) — a store that discards the prefix but not the suffix; proves the full
  /// postcondition check (not just `first_index`) catches a divergent retained suffix.
  restore_keeps_stale_suffix: bool,
  /// When `true`, `entries` drops the FIRST element of every non-empty result — a CONTIGUOUS suffix that
  /// starts ABOVE `range.start` (a non-conforming, gapped read). Folding scans tolerate it (they don't
  /// check contiguity); apply's `entry.index() == applied.next()` guard must fail-stop on it.
  gap_first_entry: bool,
  /// When `true`, `entries` extends every read to `last_index` regardless of `range.end` — a CONTIGUOUS
  /// but OVERLONG read returning entries past the requested range (e.g. past `commit`). Apply's
  /// `idx > commit` guard must fail-stop rather than fold an uncommitted entry into state.
  return_overlong: bool,
  /// When `true`, `entries` returns [`EntriesRead::Pending`] (the range is "cold") for EVERY read —
  /// a store that defers. Proves the per-site Pending dispositions: apply/replication defer without
  /// poisoning, the restart scans poison.
  return_cold: bool,
  /// When `true`, `entries` returns its result OWNED (`Ready(Owned(..))`) instead of borrowed — a
  /// cold/disk store that materialises the range. Exercises the owned apply-iteration path.
  return_owned: bool,
  /// Records the `max_bytes` of the most recent `entries` call, so a test can assert apply reads are
  /// byte-capped (bounded), never `u64::MAX`.
  observed_max_bytes: Cell<u64>,
  /// Records the MAX range width (`end - start`) of any `entries` call, so a test can assert the core
  /// bounds its committed-range requests to `MAX_READ_BATCH_ENTRIES` regardless of payload.
  observed_max_range_width: Cell<u64>,
}

impl FailTermLog {
  /// Seed durable entries (delegates to [`VecLog::force_append`]).
  pub(crate) fn force_append(&mut self, entries: &[Entry]) {
    self.inner.force_append(entries);
  }

  /// Arm the fatal term-read failure at `index` (cleared with `None`).
  pub(crate) fn fail_term_at(&mut self, index: Option<Index>) {
    self.fail_index = index;
  }

  /// Arm the fatal `entries` failure at `index`: any range containing it returns `Err(())`.
  pub(crate) fn fail_entries_at(&mut self, index: Option<Index>) {
    self.fail_entries_index = index;
  }

  /// Make `restore` a no-op (skip the re-baseline) — a contract-violating store, for the install fail-stop.
  pub(crate) fn break_restore_rebaseline(&mut self) {
    self.skip_restore_rebaseline = true;
  }

  /// Make `restore` re-baseline `first_index` correctly but RETAIN a stale entry above the boundary.
  pub(crate) fn break_restore_keeping_suffix(&mut self) {
    self.restore_keeps_stale_suffix = true;
  }

  /// Make `entries` return a gapped (non-contiguous) read by dropping the first element — for apply's
  /// contiguity guard.
  pub(crate) fn gap_first_entry_on_read(&mut self) {
    self.gap_first_entry = true;
  }

  /// Make `entries` return an OVERLONG read (extended to `last_index`, ignoring `range.end`) — for apply's
  /// past-commit guard.
  pub(crate) fn return_overlong_on_read(&mut self) {
    self.return_overlong = true;
  }

  /// Make `entries` return [`EntriesRead::Pending`] (a cold read) for every read.
  pub(crate) fn return_cold_on_read(&mut self) {
    self.return_cold = true;
  }

  /// Stop returning cold reads (the range became resident) — `entries` serves normally again.
  pub(crate) fn clear_cold_on_read(&mut self) {
    self.return_cold = false;
  }

  /// Make `entries` return its result OWNED (`Ready(Owned(..))`) — a cold/disk store that materialises.
  pub(crate) fn return_owned_on_read(&mut self) {
    self.return_owned = true;
  }

  /// The `max_bytes` of the most recent `entries` call (`0` if none yet).
  pub(crate) fn observed_max_bytes(&self) -> u64 {
    self.observed_max_bytes.get()
  }

  /// The largest range WIDTH (`end - start`) requested across all `entries` calls (`0` if none yet).
  pub(crate) fn observed_max_range_width(&self) -> u64 {
    self.observed_max_range_width.get()
  }
}

impl LogStore for FailTermLog {
  type Error = ();

  fn first_index(&self) -> Index {
    self.inner.first_index()
  }

  fn last_index(&self) -> Index {
    self.inner.last_index()
  }

  fn term(&self, index: Index) -> Result<Term, Self::Error> {
    if self.fail_index == Some(index) {
      return Err(());
    }
    Ok(self.inner.term(index).expect("VecLog::term is Infallible"))
  }

  fn entries(&self, range: Range<Index>, max_bytes: u64) -> Result<EntriesRead<'_>, Self::Error> {
    self.observed_max_bytes.set(max_bytes);
    self.observed_max_range_width.set(
      self
        .observed_max_range_width
        .get()
        .max(range.end.get().saturating_sub(range.start.get())),
    );
    if self.return_cold {
      return Ok(EntriesRead::Pending);
    }
    if self.fail_entries_index.is_some_and(|i| range.contains(&i)) {
      return Err(());
    }
    // Overlong: extend the read to last_index, ignoring range.end — a contiguous slice past the range.
    let read = if self.return_overlong {
      range.start..self.inner.last_index().next()
    } else {
      range
    };
    // Destructure `Borrowed` to recover the `&self` lifetime — re-slicing the local MaybeOwned
    // would borrow the local, not the inner store. VecLog always returns Ready(Borrowed).
    let s = match self
      .inner
      .entries(read, max_bytes)
      .expect("VecLog::entries is Infallible")
    {
      EntriesRead::Ready(MaybeOwned::Borrowed(slice)) => slice,
      EntriesRead::Ready(MaybeOwned::Owned(_)) | EntriesRead::Pending => {
        unreachable!("VecLog::entries always returns Ready(Borrowed)")
      }
    };
    // Apply the gap transform, then return borrowed (default) or OWNED (a cold/disk store materialising).
    let out: &[Entry] = if self.gap_first_entry && !s.is_empty() {
      &s[1..] // gapped: a contiguous suffix starting above range.start
    } else {
      s
    };
    if self.return_owned {
      Ok(EntriesRead::Ready(out.to_vec().into()))
    } else {
      Ok(EntriesRead::Ready(MaybeOwned::Borrowed(out)))
    }
  }

  fn submit_append(&mut self, id: OpId, entries: &[Entry]) {
    self.inner.submit_append(id, entries);
  }

  fn compact(&mut self, up_to: Index) {
    self.inner.compact(up_to);
  }

  fn restore(&mut self, last_index: Index, last_term: Term) {
    if self.skip_restore_rebaseline {
      return; // contract-violating store: leave first_index un-rebaselined
    }
    self.inner.restore(last_index, last_term);
    if self.restore_keeps_stale_suffix {
      // Correct re-baseline (first_index = n+1) but leave a divergent entry above n (last_index = n+1):
      // a store that discards the prefix but not the suffix.
      self.inner.force_append(&[Entry::new(
        last_term,
        last_index.next(),
        crate::EntryKind::Empty,
        bytes::Bytes::new(),
      )]);
    }
  }

  fn poll(&mut self) -> Option<Result<LogDone, Self::Error>> {
    self
      .inner
      .poll()
      .map(|r| Ok(r.expect("VecLog::poll is Infallible")))
  }

  fn has_pending(&self) -> bool {
    self.inner.has_pending()
  }
}

/// A counting state machine: `Command = Bytes`, `Response = usize`. Counts applied commands.
#[derive(Debug, Default)]
pub(crate) struct CountSm {
  count: usize,
}

impl CountSm {
  /// How many commands have been applied.
  #[allow(dead_code)]
  pub(crate) fn count(&self) -> usize {
    self.count
  }
}

impl crate::StateMachine for CountSm {
  type Command = Bytes;
  type Response = usize;
  type Snapshot = u64;
  type Error = Infallible;

  fn apply(&mut self, _index: Index, _cmd: Bytes) -> Result<usize, Self::Error> {
    self.count += 1;
    Ok(self.count)
  }

  fn snapshot(&self) -> Result<u64, Self::Error> {
    Ok(self.count as u64)
  }

  fn restore(&mut self, snapshot: u64) -> Result<(), Self::Error> {
    self.count = snapshot as usize;
    Ok(())
  }
}

/// A synchronous in-memory stable store: `submit_write` persists `HardState<u64>` immediately AND
/// enqueues a `StableDone::Wrote` completion (released on `poll`), modeling a store that is durable
/// the moment the write returns. The completion is what the proto's persist-before-act paths need —
/// the candidate `Campaign` (become-leader gated on the self-vote being durable) and the follower
/// `CastVote` — so a driver must call `handle_storage` to drain it.
#[derive(Debug)]
pub(crate) struct NoopStable<I = u64> {
  hard_state: HardState<I>,
  completions: VecDeque<StableDone>,
  snapshot_staging: Option<(SnapshotMeta<I>, crate::SnapshotStaging)>,
}

impl<I> Default for NoopStable<I> {
  fn default() -> Self {
    Self {
      hard_state: HardState::initial(),
      completions: VecDeque::new(),
      snapshot_staging: None,
    }
  }
}

impl<I> NoopStable<I> {
  /// Seed the stable store with a specific (term, vote, commit). Used in restart tests.
  pub(crate) fn force_state(&mut self, term: Term, vote: Option<I>, commit: Index) {
    self.hard_state = HardState::initial()
      .with_term(term)
      .with_vote(vote)
      .with_commit(commit);
  }
}

impl<I: NodeId> StableStore for NoopStable<I> {
  type NodeId = I;
  type Error = Infallible;

  fn hard_state(&self) -> HardState<I> {
    self.hard_state.clone()
  }

  fn submit_write(&mut self, id: OpId, hard_state: HardState<I>) {
    self.hard_state = hard_state;
    self.completions.push_back(StableDone::Wrote(id));
  }

  fn submit_snapshot(&mut self, _id: OpId, _meta: SnapshotMeta<I>, _data: Bytes) {
    // No-op: NoopStable does not persist snapshots and enqueues no completion.
  }

  fn snapshot(&self) -> Option<(SnapshotMeta<I>, Bytes)> {
    None
  }

  fn durable_snapshot(&self) -> Option<SnapshotMeta<I>> {
    None
  }

  fn accept_snapshot_chunk(
    &mut self,
    meta: &SnapshotMeta<I>,
    total_len: u64,
    offset: u64,
    data: &Bytes,
  ) -> Result<u64, Self::Error> {
    let boundary = meta.last_index();
    match &self.snapshot_staging {
      Some((m, _)) if m.last_index() > boundary => return Ok(0),
      Some((m, s)) if !m.identity_eq(meta) || s.total_len() != total_len => {
        self.snapshot_staging = None
      }
      _ => {}
    }
    if self.snapshot_staging.is_none() {
      // A generous cap bounds a forged length without OOM; these test stores see only small snapshots,
      // so an over-cap None is unreachable — treat it as a no-op stage rather than panic.
      match crate::SnapshotStaging::new(boundary, total_len, 1 << 30) {
        Some(s) => self.snapshot_staging = Some((meta.clone(), s)),
        None => return Ok(0),
      }
    }
    Ok(
      self
        .snapshot_staging
        .as_mut()
        .expect("staging set above")
        .1
        .accept(offset, data),
    )
  }

  fn take_staged_snapshot(&mut self, meta: &SnapshotMeta<I>) -> Option<Bytes> {
    let complete = matches!(
      &self.snapshot_staging,
      Some((m, s)) if m.identity_eq(meta) && s.is_complete()
    );
    complete.then(|| {
      let (_, s) = self
        .snapshot_staging
        .take()
        .expect("checked complete above");
      Bytes::from(s.into_vec())
    })
  }

  fn discard_snapshot_staging(&mut self) {
    self.snapshot_staging = None;
  }

  fn poll(&mut self) -> Option<Result<StableDone, Self::Error>> {
    self.completions.pop_front().map(Ok)
  }

  fn has_pending(&self) -> bool {
    !self.completions.is_empty()
  }
}

/// An async-mode stable store: `submit_write` persists the `HardState` AND enqueues a
/// `StableDone::Wrote(opid)` completion that is released only when `poll` is called.
/// Use in tests that verify a granted vote is withheld until the write is durable.
#[derive(Debug)]
pub(crate) struct AsyncStable {
  /// The VISIBLE HardState (reflects a `submit_write` immediately, before its completion is polled).
  hard_state: HardState<u64>,
  /// The DURABLE HardState — the value a crash (`discard_inflight`) rolls back to. Advanced to the visible
  /// value when a `Wrote` completion is polled (models flush-on-poll). Lets tests model a crash in the
  /// fsync window: a `submit_write` whose completion is never polled is LOST on `discard_inflight`.
  durable_hard_state: HardState<u64>,
  completions: VecDeque<StableDone>,
  /// The VISIBLE snapshot slot — `submit_snapshot` sets it immediately (readable via `snapshot()`).
  snapshot: Option<(SnapshotMeta<u64>, Bytes)>,
  /// The DURABLE snapshot slot — what a crash (`discard_inflight`) rolls the visible slot back to.
  /// Advanced to the visible value when a `SnapshotWritten` completion is polled (models flush-on-poll),
  /// so a `submit_snapshot` whose completion is never polled is LOST on a crash (the snapshot-install window).
  durable_snapshot: Option<(SnapshotMeta<u64>, Bytes)>,
  /// When set, the NEXT `submit_snapshot` makes the blob DURABLE but DROPS its `SnapshotWritten`
  /// completion (models a store that coalesces/loses the completion while still fsync'ing the blob) —
  /// so the durable slot advances at submit time but no completion fires. Used by the reconciliation test.
  drop_next_snapshot_completion: bool,
  /// When set, the NEXT `submit_snapshot` makes the blob VISIBLE but NOT durable and enqueues NO
  /// completion (models a torn/failed fsync). `durable_snapshot()` stays `None`, so a missed-completion
  /// fallback keyed on durable evidence must NOT fire the install — the fallback-safety test.
  fail_next_snapshot_durability: bool,
  /// When true, `hard_state()` returns the LAST-DURABLE value (the strict trait contract) rather than
  /// the submit-visible one. Models a conforming store, exposing writers that rebuild HardState from
  /// `hard_state()` while a floor write is in flight.
  last_durable_reads: bool,
  /// The `lease_support` of every HardState actually handed to `submit_write` (post choke-point
  /// stamp). Lets a test assert the durable floor is monotone non-decreasing across all writes.
  submitted_lease: Vec<Option<Duration>>,
  snapshot_staging: Option<(SnapshotMeta<u64>, crate::SnapshotStaging)>,
  /// When true, `snapshot_chunk` reports the resident blob as COLD — it returns the meta and real
  /// `total_len` but `SnapshotChunkRead::Pending`, modelling a disk/mmap store whose blob is not paged
  /// in. The sender must then defer (emit nothing, mutate no progress). When false the default resident
  /// slice is served, so the store behaves byte-identically to a fully-resident one.
  pub cold_snapshot: bool,
}

impl Default for AsyncStable {
  fn default() -> Self {
    Self {
      hard_state: HardState::initial(),
      durable_hard_state: HardState::initial(),
      completions: VecDeque::new(),
      snapshot: None,
      durable_snapshot: None,
      drop_next_snapshot_completion: false,
      fail_next_snapshot_durability: false,
      last_durable_reads: false,
      submitted_lease: Vec::new(),
      snapshot_staging: None,
      cold_snapshot: false,
    }
  }
}

impl AsyncStable {
  /// Seed the stable store with a specific (term, vote, commit) — durable from the start.
  pub(crate) fn force_state(&mut self, term: Term, vote: Option<u64>, commit: Index) {
    let hs = HardState::initial()
      .with_term(term)
      .with_vote(vote)
      .with_commit(commit);
    self.hard_state = hs;
    self.durable_hard_state = hs;
  }

  /// Seed the store with an arbitrary durable `HardState` (e.g. one carrying a `lease_support` floor).
  /// Used by the lease-promise restart tests.
  pub(crate) fn force_hard_state(&mut self, hs: HardState<u64>) {
    self.hard_state = hs;
    self.durable_hard_state = hs;
  }

  /// Model a crash: every `submit_write`/`submit_snapshot` whose completion has not yet been polled is
  /// LOST — the visible HardState AND the visible snapshot roll back to their last durable values and
  /// pending completions are discarded. A snapshot submitted but not yet flushed (its `SnapshotWritten`
  /// unpolled) therefore vanishes, modelling the snapshot-install fsync window.
  pub(crate) fn discard_inflight(&mut self) {
    self.hard_state = self.durable_hard_state;
    self.snapshot.clone_from(&self.durable_snapshot);
    self.completions.clear();
    // An in-RAM store loses chunk staging on a crash — the transfer restarts from offset 0.
    self.snapshot_staging = None;
  }

  /// Make `hard_state()` return the LAST-DURABLE value (strict `StableStore` contract) instead of the
  /// submit-visible one, so writers that rebuild from `hard_state()` see a stale floor while a raise is in
  /// flight — the exact condition Finding 2 needs.
  pub(crate) fn set_last_durable_reads(&mut self, on: bool) {
    self.last_durable_reads = on;
  }

  /// The `lease_support` of every HardState handed to `submit_write`, in order.
  pub(crate) fn submitted_lease_supports(&self) -> &[Option<Duration>] {
    &self.submitted_lease
  }

  /// Number of HardState writes submitted but not yet polled — used to assert the write-amplification
  /// invariant (steady-state heartbeats and same-config restarts add no HardState write).
  pub(crate) fn pending_writes(&self) -> usize {
    self
      .completions
      .iter()
      .filter(|c| matches!(c, StableDone::Wrote(_)))
      .count()
  }

  /// Arm the store so the next `submit_snapshot` persists the durable snapshot but drops its
  /// `SnapshotWritten` completion. The blob remains readable via `snapshot()`.
  pub(crate) fn drop_next_snapshot_completion(&mut self) {
    self.drop_next_snapshot_completion = true;
  }

  /// Arm the store so the next `submit_snapshot` makes the blob VISIBLE but NOT durable and enqueues no
  /// completion (a torn/failed fsync). `durable_snapshot()` stays `None`; a `discard_inflight` then
  /// rolls the visible blob away. Used to prove the deferred-install fallback fires only on DURABLE
  /// evidence, never the visible slot.
  pub(crate) fn fail_next_snapshot_durability(&mut self) {
    self.fail_next_snapshot_durability = true;
  }
}

impl StableStore for AsyncStable {
  type NodeId = u64;
  type Error = Infallible;

  fn hard_state(&self) -> HardState<u64> {
    // A strict store returns the LAST-DURABLE state; the default models a submit-visible store.
    if self.last_durable_reads {
      self.durable_hard_state
    } else {
      self.hard_state
    }
  }

  fn submit_write(&mut self, id: OpId, hard_state: HardState<u64>) {
    self
      .submitted_lease
      .push(hard_state.promised_lease_support());
    self.hard_state = hard_state;
    self.completions.push_back(StableDone::Wrote(id));
  }

  fn submit_snapshot(&mut self, id: OpId, meta: SnapshotMeta<u64>, data: Bytes) {
    // The blob is VISIBLE immediately (readable via `snapshot()`), but NOT yet durable.
    self.snapshot = Some((meta, data));
    if self.fail_next_snapshot_durability {
      // Torn/failed fsync: visible but NOT durable, NO completion. `durable_snapshot()` stays None.
      self.fail_next_snapshot_durability = false;
    } else if self.drop_next_snapshot_completion {
      // Models a store that fsync'd the blob (durable) but coalesced/lost the completion: advance the
      // durable slot NOW but enqueue NO `SnapshotWritten`, so only `durable_snapshot()` reveals it.
      self.drop_next_snapshot_completion = false;
      self.durable_snapshot.clone_from(&self.snapshot);
    } else {
      // Durability is DEFERRED: the durable slot advances when the completion is polled (flush-on-poll).
      self.completions.push_back(StableDone::SnapshotWritten(id));
    }
  }

  fn snapshot(&self) -> Option<(SnapshotMeta<u64>, Bytes)> {
    self.snapshot.clone()
  }

  fn snapshot_chunk(
    &self,
    offset: u64,
    len: u64,
  ) -> Option<Result<(SnapshotMeta<u64>, u64, SnapshotChunkRead), Self::Error>> {
    self.snapshot.as_ref().map(|(meta, blob)| {
      let total = blob.len() as u64;
      let read = if self.cold_snapshot {
        // The blob is resident in this in-RAM store, but report it as COLD so the sender must defer —
        // exactly what a disk/mmap store does when the requested run has not been paged in.
        SnapshotChunkRead::Pending
      } else {
        let start = offset.min(total) as usize;
        let end = offset.saturating_add(len).min(total) as usize;
        SnapshotChunkRead::Ready(blob.slice(start..end))
      };
      Ok((meta.clone(), total, read))
    })
  }

  fn durable_snapshot(&self) -> Option<SnapshotMeta<u64>> {
    self.durable_snapshot.as_ref().map(|(m, _)| m.clone())
  }

  fn accept_snapshot_chunk(
    &mut self,
    meta: &SnapshotMeta<u64>,
    total_len: u64,
    offset: u64,
    data: &Bytes,
  ) -> Result<u64, Self::Error> {
    let boundary = meta.last_index();
    match &self.snapshot_staging {
      Some((m, _)) if m.last_index() > boundary => return Ok(0),
      Some((m, s)) if !m.identity_eq(meta) || s.total_len() != total_len => {
        self.snapshot_staging = None
      }
      _ => {}
    }
    if self.snapshot_staging.is_none() {
      // A generous cap bounds a forged length without OOM; these test stores see only small snapshots,
      // so an over-cap None is unreachable — treat it as a no-op stage rather than panic.
      match crate::SnapshotStaging::new(boundary, total_len, 1 << 30) {
        Some(s) => self.snapshot_staging = Some((meta.clone(), s)),
        None => return Ok(0),
      }
    }
    Ok(
      self
        .snapshot_staging
        .as_mut()
        .expect("staging set above")
        .1
        .accept(offset, data),
    )
  }

  fn take_staged_snapshot(&mut self, meta: &SnapshotMeta<u64>) -> Option<Bytes> {
    let complete = matches!(
      &self.snapshot_staging,
      Some((m, s)) if m.identity_eq(meta) && s.is_complete()
    );
    complete.then(|| {
      let (_, s) = self
        .snapshot_staging
        .take()
        .expect("checked complete above");
      Bytes::from(s.into_vec())
    })
  }

  fn discard_snapshot_staging(&mut self) {
    self.snapshot_staging = None;
  }

  fn poll(&mut self) -> Option<Result<StableDone, Self::Error>> {
    let done = self.completions.pop_front();
    // A polled completion means that write reached stable storage — fold it into the durable value so a
    // later `discard_inflight` (crash) no longer rolls it back.
    match done {
      Some(StableDone::Wrote(_)) => self.durable_hard_state = self.hard_state,
      Some(StableDone::SnapshotWritten(_)) => self.durable_snapshot.clone_from(&self.snapshot),
      _ => {}
    }
    done.map(Ok)
  }

  fn has_pending(&self) -> bool {
    // Only ENQUEUED completions are ready to poll. A `submit_*` whose durability is deferred (no
    // completion yet — the `fail_next_snapshot_durability` torn-fsync slot) is correctly excluded.
    !self.completions.is_empty()
  }
}
