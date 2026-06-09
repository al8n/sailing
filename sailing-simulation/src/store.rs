//! In-memory `LogStore`/`StableStore` impls for the simulator. M0 ships synchronous mode
//! (completions enqueued at submit); M3 adds an async-write mode that re-opens the
//! fsync-in-flight window.
use bytes::Bytes;
use sailing_proto::{
  Entry, HardState, Index, LogDone, LogStore, OpId, SnapshotMeta, StableDone, StableStore, Term,
};
use std::{collections::VecDeque, vec::Vec};

/// In-memory write-ahead log with compaction support.
///
/// Offset model (mirrors etcd `MemoryStorage`):
/// - `offset`: the compaction boundary — the index *before* `entries[0]`; equals the
///   snapshot's `last_index`. Starts at `Index::ZERO`.
/// - `compacted_term`: term at `offset` (the snapshot's last term). Starts at `Term::ZERO`.
/// - `first_index() == offset + 1`; `last_index() == offset + entries.len()`.
#[derive(Debug, Default)]
pub struct MemLog {
  entries: Vec<Entry>,
  completions: VecDeque<LogDone>,
  /// Index before entries[0]. Starts at ZERO (no compaction).
  offset: Index,
  /// Term at offset (boundary term kept for consistency checks after compaction).
  compacted_term: Term,
}

impl MemLog {
  /// Empty log.
  pub fn new() -> Self {
    Self::default()
  }

  /// Drop any in-flight (not-yet-durable) work. In synchronous mode this is a no-op
  /// because `submit_append` commits data immediately; in async mode (M8) this would
  /// drop the staged bytes and their pending completions, modelling fsync loss.
  pub fn discard_inflight(&mut self) {
    // Synchronous mode: nothing is un-flushed; no-op.
  }
}

impl LogStore for MemLog {
  type Error = core::convert::Infallible;

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
      // compacted away — out-of-range read
      return Ok(Term::ZERO);
    }
    let last = self.last_index();
    if index > last {
      return Ok(Term::ZERO);
    }
    // pos = index - offset - 1 (0-based into entries)
    let pos = (index.get() - self.offset.get() - 1) as usize;
    Ok(self.entries[pos].term())
  }

  fn entries(
    &self,
    range: core::ops::Range<Index>,
    _max_bytes: u64,
  ) -> Result<&[Entry], Self::Error> {
    let start = range.start.get();
    let end = range.end.get();
    let offset = self.offset.get();
    let len = self.entries.len() as u64;
    // Convert 1-based absolute index to 0-based vec position: pos = index - offset - 1
    // Saturate to 0 if start <= offset (compacted), clamp to len.
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
    Ok(&self.entries[lo..hi])
  }

  fn submit_append(&mut self, id: OpId, entries: &[Entry]) {
    if let Some(first) = entries.first() {
      debug_assert!(
        first.index().get() > self.offset.get(),
        "submit_append below the compaction offset"
      );
      // Truncate position: first.index() - offset - 1 (saturating to 0)
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
    self.completions.push_back(LogDone::Appended(id));
  }

  fn compact(&mut self, up_to: Index) {
    if up_to <= self.offset || self.entries.is_empty() {
      return; // no-op: already compacted or nothing to compact
    }
    let last = self.last_index();
    let up_to = if up_to > last { last } else { up_to };
    // Read the boundary term before draining
    let boundary_term = self.term(up_to).unwrap_or(Term::ZERO);
    // Number of entries to remove: up_to - offset (= index in entries of up_to's position + 1)
    let drain_count = (up_to.get() - self.offset.get()) as usize;
    let drain_count = drain_count.min(self.entries.len());
    self.entries.drain(0..drain_count);
    self.offset = up_to;
    self.compacted_term = boundary_term;
  }

  fn restore(&mut self, last_index: Index, last_term: Term) {
    // Discard all entries: the follower's entire log is replaced by the snapshot.
    // Drop any pending completions for discarded appends — they will never fire.
    self.entries.clear();
    self.completions.clear();
    // Re-baseline: offset == last_index so that first_index() == last_index + 1
    // and term(last_index) == last_term (the snapshot boundary term).
    self.offset = last_index;
    self.compacted_term = last_term;
  }

  fn poll(&mut self) -> Option<Result<LogDone, Self::Error>> {
    self.completions.pop_front().map(Ok)
  }
}

/// In-memory durable metadata store.
#[derive(Debug)]
pub struct MemStable<I> {
  hard_state: HardState<I>,
  completions: VecDeque<StableDone>,
  snapshot: Option<(SnapshotMeta<I>, Bytes)>,
}

impl<I: sailing_proto::NodeId> MemStable<I> {
  /// Fresh store at the initial hard state.
  pub fn new() -> Self {
    Self {
      hard_state: HardState::initial(),
      completions: VecDeque::new(),
      snapshot: None,
    }
  }

  /// Drop any in-flight (not-yet-durable) work. In synchronous mode this is a no-op
  /// because `submit_write` commits state immediately; in async mode (M8) this would
  /// drop staged writes and their pending completions, modelling fsync loss.
  /// Drops a staged snapshot write if present.
  pub fn discard_inflight(&mut self) {
    // Synchronous mode: completions are enqueued immediately; no staged writes to drop.
    // A snapshot submitted during a crash window is dropped here (the write never completes).
    // In the current synchronous mode this is a no-op, but the field is kept for M8.
  }
}

impl<I: sailing_proto::NodeId> Default for MemStable<I> {
  fn default() -> Self {
    Self::new()
  }
}

impl<I: sailing_proto::NodeId> StableStore for MemStable<I> {
  type NodeId = I;
  type Error = core::convert::Infallible;

  fn hard_state(&self) -> HardState<I> {
    self.hard_state
  }

  fn submit_write(&mut self, id: OpId, hard_state: HardState<I>) {
    self.hard_state = hard_state;
    self.completions.push_back(StableDone::Wrote(id));
  }

  fn submit_snapshot(&mut self, id: OpId, meta: SnapshotMeta<I>, data: Bytes) {
    self.snapshot = Some((meta, data));
    self.completions.push_back(StableDone::SnapshotWritten(id));
  }

  fn snapshot(&self) -> Option<(SnapshotMeta<I>, Bytes)> {
    self.snapshot.clone()
  }

  fn poll(&mut self) -> Option<Result<StableDone, Self::Error>> {
    self.completions.pop_front().map(Ok)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use bytes::Bytes;
  use sailing_proto::{EntryKind, LogDone, LogStore, SnapshotMeta, StableStore, conf::ConfState};

  #[test]
  fn mem_log_append_is_durable_after_poll() {
    let mut log = MemLog::new();
    assert_eq!(log.last_index(), Index::ZERO);
    let e = Entry::new(
      Term::new(1),
      Index::new(1),
      EntryKind::Normal,
      Bytes::from_static(b"a"),
    );
    log.submit_append(OpId::new(1), core::slice::from_ref(&e));
    // synchronous-mode store completes immediately on poll
    assert_eq!(log.poll(), Some(Ok(LogDone::Appended(OpId::new(1)))));
    assert_eq!(log.last_index(), Index::new(1));
    assert_eq!(log.term(Index::new(1)).unwrap(), Term::new(1));
  }

  #[test]
  fn mem_stable_roundtrips_hard_state() {
    let mut s = MemStable::<u64>::new();
    let hs = s.hard_state().with_term(Term::new(4));
    s.submit_write(OpId::new(1), hs);
    let _ = s.poll();
    assert_eq!(s.hard_state().term(), Term::new(4));
  }

  #[test]
  fn mem_stable_roundtrips_snapshot() {
    let mut s = MemStable::<u64>::new();
    assert!(s.snapshot().is_none());

    let meta = SnapshotMeta::new(
      Index::new(10),
      Term::new(3),
      ConfState::new(std::vec![1u64, 2u64, 3u64]),
    );
    let data = Bytes::from_static(b"snapshot-data");
    s.submit_snapshot(OpId::new(42), meta.clone(), data.clone());

    // Completion is enqueued.
    use sailing_proto::{OpId, StableDone};
    assert_eq!(
      s.poll(),
      Some(Ok(StableDone::SnapshotWritten(OpId::new(42))))
    );

    // Snapshot is readable.
    let (rmeta, rdata) = s.snapshot().unwrap();
    assert_eq!(rmeta.last_index(), Index::new(10));
    assert_eq!(rmeta.last_term(), Term::new(3));
    assert_eq!(rdata, data);

    // Second submit_snapshot overwrites the previous one.
    let meta2 = SnapshotMeta::new(
      Index::new(20),
      Term::new(5),
      ConfState::new(std::vec![1u64]),
    );
    s.submit_snapshot(OpId::new(43), meta2.clone(), Bytes::from_static(b"v2"));
    let _ = s.poll();
    let (rmeta2, _) = s.snapshot().unwrap();
    assert_eq!(rmeta2.last_index(), Index::new(20));
  }

  // --- Compaction tests ---

  fn make_entry(term: u64, index: u64) -> Entry {
    Entry::new(
      Term::new(term),
      Index::new(index),
      EntryKind::Normal,
      Bytes::new(),
    )
  }

  #[test]
  fn compact_advances_first_index() {
    let mut log = MemLog::new();
    // append entries 1..=5 all at term 1
    let entries: Vec<Entry> = (1..=5).map(|i| make_entry(1, i)).collect();
    log.submit_append(OpId::new(1), &entries);
    let _ = log.poll();

    assert_eq!(log.first_index(), Index::new(1));
    assert_eq!(log.last_index(), Index::new(5));

    // compact up to index 3 (retain 4, 5)
    log.compact(Index::new(3));

    assert_eq!(log.first_index(), Index::new(4), "first_index must advance");
    assert_eq!(log.last_index(), Index::new(5), "last_index unchanged");
  }

  #[test]
  fn term_at_offset_returns_boundary_term() {
    let mut log = MemLog::new();
    let entries: Vec<Entry> = vec![make_entry(1, 1), make_entry(1, 2), make_entry(2, 3)];
    log.submit_append(OpId::new(1), &entries);
    let _ = log.poll();

    log.compact(Index::new(2)); // compact up through index 2 (term 1)
    assert_eq!(
      log.term(Index::new(2)).unwrap(),
      Term::new(1),
      "term(offset) must return boundary term"
    );
  }

  #[test]
  fn entries_and_term_correct_after_compaction() {
    let mut log = MemLog::new();
    let entries: Vec<Entry> = (1..=5).map(|i| make_entry(i, i)).collect();
    log.submit_append(OpId::new(1), &entries);
    let _ = log.poll();

    log.compact(Index::new(3));

    // entries 4 and 5 still accessible
    let slice = log.entries(Index::new(4)..Index::new(6), u64::MAX).unwrap();
    assert_eq!(slice.len(), 2);
    assert_eq!(slice[0].index(), Index::new(4));
    assert_eq!(slice[0].term(), Term::new(4));
    assert_eq!(slice[1].index(), Index::new(5));
    assert_eq!(slice[1].term(), Term::new(5));

    // term lookups
    assert_eq!(log.term(Index::new(4)).unwrap(), Term::new(4));
    assert_eq!(log.term(Index::new(5)).unwrap(), Term::new(5));
    // below offset → Term::ZERO
    assert_eq!(log.term(Index::new(1)).unwrap(), Term::ZERO);
    assert_eq!(log.term(Index::new(2)).unwrap(), Term::ZERO);
  }

  #[test]
  fn compact_noop_on_already_compacted_range() {
    let mut log = MemLog::new();
    let entries: Vec<Entry> = (1..=5).map(|i| make_entry(1, i)).collect();
    log.submit_append(OpId::new(1), &entries);
    let _ = log.poll();

    log.compact(Index::new(3));
    // compact again with same or lower index — no-op, no panic
    log.compact(Index::new(3));
    log.compact(Index::new(1));

    assert_eq!(log.first_index(), Index::new(4));
    assert_eq!(log.last_index(), Index::new(5));
  }

  #[test]
  fn compact_empty_log_is_noop() {
    let mut log = MemLog::new();
    log.compact(Index::new(5)); // must not panic
    assert_eq!(log.first_index(), Index::new(1));
    assert_eq!(log.last_index(), Index::ZERO);
  }
}
