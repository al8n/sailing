//! Throwaway store impls for proto-level unit tests. Not compiled outside `#[cfg(test)]`.
use crate::{
  Entry, HardState, Index, LogDone, LogStore, OpId, SnapshotMeta, StableDone, StableStore, Term,
};
use bytes::Bytes;
use std::collections::VecDeque;

/// A no-op log that is always empty — last_index=0, term(any)=Term::ZERO.
///
/// `restore` is a degenerate no-op: the NoopLog carries no real state, so there is nothing
/// to discard.  `first_index`/`last_index`/`term` stay at their fixed degenerate values.
/// Tests that exercise restore behaviour must use `VecLog`.
#[derive(Debug)]
pub(crate) struct NoopLog;

impl LogStore for NoopLog {
  type Error = core::convert::Infallible;

  fn first_index(&self) -> Index {
    Index::new(1)
  }

  fn last_index(&self) -> Index {
    Index::ZERO
  }

  fn term(&self, _index: Index) -> Result<Term, Self::Error> {
    Ok(Term::ZERO)
  }

  fn entries(
    &self,
    _range: core::ops::Range<Index>,
    _max_bytes: u64,
  ) -> Result<&[crate::Entry], Self::Error> {
    Ok(&[])
  }

  fn submit_append(&mut self, _id: OpId, _entries: &[crate::Entry]) {}

  fn compact(&mut self, _up_to: Index) {}

  fn restore(&mut self, _last_index: Index, _last_term: Term) {
    // NoopLog carries no real state; the degenerate fixed values remain consistent.
  }

  fn poll(&mut self) -> Option<Result<LogDone, Self::Error>> {
    None
  }
}

/// A mutating in-memory log for proto unit tests. Mirrors `sailing_simulation::MemLog`.
///
/// Offset model: `offset` is the compaction boundary (the index before `entries[0]`).
/// Starts at `Index::ZERO`. `compacted_term` is the term at `offset`.
#[derive(Debug, Default)]
pub(crate) struct VecLog {
  entries: std::vec::Vec<Entry>,
  completions: VecDeque<LogDone>,
  /// Index before entries[0]. Starts at ZERO (no compaction).
  offset: Index,
  /// Term at offset (boundary term kept after compaction).
  compacted_term: Term,
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
}

impl LogStore for VecLog {
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
      return Ok(Term::ZERO);
    }
    let last = self.last_index();
    if index > last {
      return Ok(Term::ZERO);
    }
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
      return;
    }
    let last = self.last_index();
    let up_to = if up_to > last { last } else { up_to };
    let boundary_term = self.term(up_to).unwrap_or(Term::ZERO);
    let drain_count = ((up_to.get() - self.offset.get()) as usize).min(self.entries.len());
    self.entries.drain(0..drain_count);
    self.offset = up_to;
    self.compacted_term = boundary_term;
  }

  fn restore(&mut self, last_index: Index, last_term: Term) {
    // Discard all entries: the follower's entire log is replaced by the snapshot.
    // Any pending completions for those appends are also dropped — they will never fire.
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
  type Error = core::convert::Infallible;

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

/// A minimal stable store that persists `HardState<u64>` in memory.
/// `submit_write` updates state immediately but enqueues NO completion — `poll` always
/// returns `None`. Use when durability of the hard-state write is irrelevant to the test.
#[derive(Debug)]
pub(crate) struct NoopStable {
  hard_state: HardState<u64>,
}

impl Default for NoopStable {
  fn default() -> Self {
    Self {
      hard_state: HardState::initial(),
    }
  }
}

impl NoopStable {
  /// Seed the stable store with a specific (term, vote, commit). Used in restart tests.
  pub(crate) fn force_state(&mut self, term: Term, vote: Option<u64>, commit: Index) {
    self.hard_state = HardState::initial()
      .with_term(term)
      .with_vote(vote)
      .with_commit(commit);
  }
}

impl StableStore for NoopStable {
  type NodeId = u64;
  type Error = core::convert::Infallible;

  fn hard_state(&self) -> HardState<u64> {
    self.hard_state
  }

  fn submit_write(&mut self, _id: OpId, hard_state: HardState<u64>) {
    self.hard_state = hard_state;
  }

  fn submit_snapshot(&mut self, _id: OpId, _meta: SnapshotMeta<u64>, _data: Bytes) {
    // No-op: NoopStable does not persist snapshots and enqueues no completion.
  }

  fn snapshot(&self) -> Option<(SnapshotMeta<u64>, Bytes)> {
    None
  }

  fn poll(&mut self) -> Option<Result<StableDone, Self::Error>> {
    None
  }
}

/// An async-mode stable store: `submit_write` persists the `HardState` AND enqueues a
/// `StableDone::Wrote(opid)` completion that is released only when `poll` is called.
/// Use in tests that verify a granted vote is withheld until the write is durable.
#[derive(Debug)]
pub(crate) struct AsyncStable {
  hard_state: HardState<u64>,
  completions: VecDeque<StableDone>,
  snapshot: Option<(SnapshotMeta<u64>, Bytes)>,
}

impl Default for AsyncStable {
  fn default() -> Self {
    Self {
      hard_state: HardState::initial(),
      completions: VecDeque::new(),
      snapshot: None,
    }
  }
}

impl AsyncStable {
  /// Seed the stable store with a specific (term, vote, commit). Used in restart tests.
  pub(crate) fn force_state(&mut self, term: Term, vote: Option<u64>, commit: Index) {
    self.hard_state = HardState::initial()
      .with_term(term)
      .with_vote(vote)
      .with_commit(commit);
  }
}

impl StableStore for AsyncStable {
  type NodeId = u64;
  type Error = core::convert::Infallible;

  fn hard_state(&self) -> HardState<u64> {
    self.hard_state
  }

  fn submit_write(&mut self, id: OpId, hard_state: HardState<u64>) {
    self.hard_state = hard_state;
    self.completions.push_back(StableDone::Wrote(id));
  }

  fn submit_snapshot(&mut self, id: OpId, meta: SnapshotMeta<u64>, data: Bytes) {
    self.snapshot = Some((meta, data));
    self.completions.push_back(StableDone::SnapshotWritten(id));
  }

  fn snapshot(&self) -> Option<(SnapshotMeta<u64>, Bytes)> {
    self.snapshot.clone()
  }

  fn poll(&mut self) -> Option<Result<StableDone, Self::Error>> {
    self.completions.pop_front().map(Ok)
  }
}
