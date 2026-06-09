//! Throwaway store impls for proto-level unit tests. Not compiled outside `#[cfg(test)]`.
use crate::{HardState, Index, LogDone, LogStore, OpId, StableDone, StableStore, Term};

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

/// A minimal stable store that persists `HardState<u64>` in memory.
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
