//! The application state machine, held inside `Endpoint`. Committed `Normal` entries are
//! decoded and applied here in index order; results surface via `Event::Applied`.
use crate::Index;

/// A deterministic application state machine.
pub trait StateMachine {
  /// The decoded command type (bound to the codec on the `Endpoint` impl, not here).
  type Command;
  /// The result of applying a command (flows out via `Event::Applied`).
  type Response;
  /// A point-in-time snapshot of applied state.
  type Snapshot;
  /// A failure applying/snapshotting/restoring (fatal to the node).
  type Error;

  /// Apply one committed entry in index order. `cmd` is taken **by value** so the SM
  /// moves its contents into state without cloning.
  fn apply(&mut self, index: Index, cmd: Self::Command) -> Result<Self::Response, Self::Error>;
  /// Capture all applied state into a snapshot.
  fn snapshot(&self) -> Result<Self::Snapshot, Self::Error>;
  /// Install a snapshot, replacing all state.
  fn restore(&mut self, snapshot: Self::Snapshot) -> Result<(), Self::Error>;
}

#[cfg(test)]
mod tests {
  use super::*;

  fn assert_sm<S: StateMachine>() {}

  struct Noop;

  impl StateMachine for Noop {
    type Command = ();
    type Response = ();
    type Snapshot = ();
    type Error = core::convert::Infallible;

    fn apply(&mut self, _: crate::Index, _: ()) -> Result<(), Self::Error> {
      Ok(())
    }

    fn snapshot(&self) -> Result<(), Self::Error> {
      Ok(())
    }

    fn restore(&mut self, _: ()) -> Result<(), Self::Error> {
      Ok(())
    }
  }

  #[test]
  fn noop_is_a_state_machine() {
    assert_sm::<Noop>();
  }
}
