//! A minimal test state machine: it records the ordered `(Index, Bytes)` it applied, and
//! returns the applied byte length as the response (enough for agreement + dedup checks).
use bytes::Bytes;
use sailing_proto::{Index, StateMachine};
use std::vec::Vec;

/// Records applied commands in order. `Command = Bytes`, `Response = usize` (byte length).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct LogSm {
  applied: Vec<(Index, Bytes)>,
}

impl LogSm {
  /// Fresh, empty.
  pub fn new() -> Self {
    Self::default()
  }

  /// The ordered applied log.
  pub fn applied(&self) -> &[(Index, Bytes)] {
    &self.applied
  }
}

impl StateMachine for LogSm {
  type Command = Bytes;
  type Response = usize;
  type Snapshot = Vec<(Index, Bytes)>;
  type Error = core::convert::Infallible;

  fn apply(&mut self, index: Index, cmd: Bytes) -> Result<usize, Self::Error> {
    let len = cmd.len();
    self.applied.push((index, cmd)); // moved in — no clone
    Ok(len)
  }

  fn snapshot(&self) -> Result<Self::Snapshot, Self::Error> {
    Ok(self.applied.clone())
  }

  fn restore(&mut self, snapshot: Self::Snapshot) -> Result<(), Self::Error> {
    self.applied = snapshot;
    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use sailing_proto::StateMachine;

  #[test]
  fn log_sm_records_applies_in_order() {
    let mut sm = LogSm::new();
    let r1 = sm
      .apply(Index::new(1), bytes::Bytes::from_static(b"a"))
      .unwrap();
    let r2 = sm
      .apply(Index::new(2), bytes::Bytes::from_static(b"bb"))
      .unwrap();
    assert_eq!(r1, 1); // response = applied byte length
    assert_eq!(r2, 2);
    assert_eq!(
      sm.applied(),
      &[
        (Index::new(1), bytes::Bytes::from_static(b"a")),
        (Index::new(2), bytes::Bytes::from_static(b"bb"))
      ]
    );
  }
}
