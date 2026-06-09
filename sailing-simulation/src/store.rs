//! In-memory `LogStore`/`StableStore` impls for the simulator. M0 ships synchronous mode
//! (completions enqueued at submit); M3 adds an async-write mode that re-opens the
//! fsync-in-flight window.
use sailing_proto::{
  Entry, HardState, Index, LogDone, LogStore, OpId, StableDone, StableStore, Term,
};
use std::{collections::VecDeque, vec::Vec};

/// In-memory write-ahead log. `entries[0]` is index 1 (no compaction in M0).
#[derive(Debug, Default)]
pub struct MemLog {
  entries: Vec<Entry>,
  completions: VecDeque<LogDone>,
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
    // 1-based indices: index N is at entries[N-1].
    // Saturating_sub(1) converts to 0-based; clamp to Vec length to avoid panics.
    let lo = start.saturating_sub(1).min(self.entries.len());
    let hi = end.saturating_sub(1).min(self.entries.len());
    Ok(&self.entries[lo..hi.max(lo)])
  }

  fn submit_append(&mut self, id: OpId, entries: &[Entry]) {
    if let Some(first) = entries.first() {
      // Truncate any conflicting suffix at/after the first new index.
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

/// In-memory durable metadata store.
#[derive(Debug)]
pub struct MemStable<I> {
  hard_state: HardState<I>,
  completions: VecDeque<StableDone>,
}

impl<I: sailing_proto::NodeId> MemStable<I> {
  /// Fresh store at the initial hard state.
  pub fn new() -> Self {
    Self {
      hard_state: HardState::initial(),
      completions: VecDeque::new(),
    }
  }

  /// Drop any in-flight (not-yet-durable) work. In synchronous mode this is a no-op
  /// because `submit_write` commits state immediately; in async mode (M8) this would
  /// drop staged writes and their pending completions, modelling fsync loss.
  pub fn discard_inflight(&mut self) {
    // Synchronous mode: nothing is un-flushed; no-op.
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

  fn poll(&mut self) -> Option<Result<StableDone, Self::Error>> {
    self.completions.pop_front().map(Ok)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use bytes::Bytes;
  use sailing_proto::{EntryKind, LogDone, LogStore, StableStore};

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
}
