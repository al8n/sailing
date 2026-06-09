//! Throwaway store impls for proto-level unit tests. Not compiled outside `#[cfg(test)]`.
use crate::{Entry, HardState, Index, LogDone, LogStore, OpId, StableDone, StableStore, Term};
use bytes::Bytes;
use std::collections::VecDeque;

/// A no-op log that is always empty — last_index=0, term(any)=Term::ZERO.
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

  fn poll(&mut self) -> Option<Result<LogDone, Self::Error>> {
    None
  }
}

/// A mutating in-memory log for proto unit tests. Mirrors `sailing_simulation::MemLog`.
#[derive(Debug, Default)]
pub(crate) struct VecLog {
  entries: std::vec::Vec<Entry>,
  completions: VecDeque<LogDone>,
}

impl VecLog {
  /// Seed the log with already-durable entries (no completion enqueued). Used in restart tests.
  pub(crate) fn force_append(&mut self, entries: &[Entry]) {
    for e in entries {
      let from = (e.index().get() as usize).saturating_sub(1);
      self.entries.truncate(from);
      self.entries.push(e.clone());
    }
  }
}

impl LogStore for VecLog {
  type Error = core::convert::Infallible;

  fn first_index(&self) -> Index {
    Index::new(1)
  }

  fn last_index(&self) -> Index {
    Index::new(self.entries.len() as u64)
  }

  fn term(&self, index: Index) -> Result<Term, Self::Error> {
    Ok(
      self
        .entries
        .get((index.get() as usize).wrapping_sub(1))
        .map(Entry::term)
        .unwrap_or(Term::ZERO),
    )
  }

  fn entries(
    &self,
    range: core::ops::Range<Index>,
    _max_bytes: u64,
  ) -> Result<&[Entry], Self::Error> {
    let start = range.start.get() as usize;
    let end = range.end.get() as usize;
    let lo = start.saturating_sub(1).min(self.entries.len());
    let hi = end.saturating_sub(1).min(self.entries.len());
    Ok(&self.entries[lo..hi.max(lo)])
  }

  fn submit_append(&mut self, id: OpId, entries: &[Entry]) {
    if let Some(first) = entries.first() {
      let from = (first.index().get() as usize).saturating_sub(1);
      self.entries.truncate(from);
    }
    self.entries.extend_from_slice(entries);
    self.completions.push_back(LogDone::Appended(id));
  }

  fn compact(&mut self, _up_to: Index) {}

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
  type Snapshot = usize;
  type Error = core::convert::Infallible;

  fn apply(&mut self, _index: Index, _cmd: Bytes) -> Result<usize, Self::Error> {
    self.count += 1;
    Ok(self.count)
  }

  fn snapshot(&self) -> Result<usize, Self::Error> {
    Ok(self.count)
  }

  fn restore(&mut self, snapshot: usize) -> Result<(), Self::Error> {
    self.count = snapshot;
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
}

impl Default for AsyncStable {
  fn default() -> Self {
    Self {
      hard_state: HardState::initial(),
      completions: VecDeque::new(),
    }
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

  fn poll(&mut self) -> Option<Result<StableDone, Self::Error>> {
    self.completions.pop_front().map(Ok)
  }
}
