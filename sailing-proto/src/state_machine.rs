//! The application state machine, held inside `Endpoint`. Committed `Normal` entries are
//! decoded and applied here in index order; results surface via `Event::Applied`.
use crate::Index;

/// A deterministic application state machine.
///
/// CONTRACT: `apply`/`snapshot`/`restore` must be TOTAL — return `Err(Self::Error)`, never PANIC, on any
/// committed input. The core converts a returned `Err` into a fail-stop (`poison`), but it does NOT
/// `catch_unwind` a panicking method: a panic unwinds through the public `handle_*`/`handle_storage` call
/// and aborts the node (`apply` mutates only node-local state, so a `panic = abort` restart re-applies the
/// committed prefix deterministically). `apply` must also be DETERMINISTIC across replicas — the same
/// `(index, cmd)` yields the same transition on every node — and `snapshot`/`restore` must round-trip; the
/// core relies on both but cannot check them.
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

  /// Split support (the group-split milestone). Default: unsupported (`None`) — an embedder that
  /// never proposes splits never sees a call, and every existing implementation compiles
  /// untouched.
  ///
  /// A supporting FSM partitions ITSELF by the opaque `instruction` (sailing never reads it) and
  /// returns the CHILD's half; `self` continues as the parent's. Runs at the deterministic apply
  /// point of a committed `Split` entry, so it carries `apply`'s determinism contract verbatim:
  /// the same `(instruction, state)` must produce the same partition on every replica — divergent
  /// implementations diverge replicas exactly as a non-deterministic `apply` would. Like `apply`
  /// it must be TOTAL (never panic on a committed input); `None` is the "unsupported /
  /// ill-formed instruction" verdict, not an error channel — a default body cannot mint an
  /// arbitrary `Self::Error`, which is why the signature is `Option`, and a COMMITTED `Split`
  /// entry applied against an FSM that returns `None` POISONS the replica
  /// (`PoisonReason::SplitUnsupported`): fail-stop, never silent divergence.
  fn split(&mut self, instruction: &[u8]) -> Option<Self>
  where
    Self: Sized,
  {
    let _ = instruction;
    None
  }

  /// Merge-absorb support (the group-merge milestone). Default: unsupported (`false`) — the same
  /// contract as [`split`](Self::split): deterministic at the committed apply point, total, and
  /// `bool` rather than `Result` because a default body cannot mint `Self::Error`. A supporting
  /// FSM folds the absorbed `source` into itself and returns `true`.
  fn absorb(&mut self, source: Self) -> bool
  where
    Self: Sized,
  {
    let _ = source;
    false
  }

  /// Whether this FSM supports [`split`](Self::split) — consulted at PROPOSE time (on the leader's
  /// FSM) so a split verb is refused with `SplitError::Unsupported` BEFORE it is appended, rather than
  /// committing and then poisoning every replica at apply (`PoisonReason::SplitUnsupported`). Default
  /// `false`, matching the default `split` (which returns `None`).
  ///
  /// MUST be TYPE-CONSTANT — a fixed property of the type, never of runtime state. Only the leader's
  /// answer gates the propose, so a value that varied with state could let one replica admit a split
  /// another cannot apply; the apply-time poison remains the determinism backstop, but this gate must
  /// not itself introduce divergence.
  fn supports_split(&self) -> bool {
    false
  }

  /// Whether this FSM supports [`absorb`](Self::absorb) — the merge-side mirror of
  /// [`supports_split`](Self::supports_split). Consulted at propose time so a merge verb is refused with
  /// `MergeError::Unsupported` before it is appended, rather than poisoning every replica at apply
  /// (`PoisonReason::MergeUnsupported`). Default `false`; MUST likewise be TYPE-CONSTANT.
  fn supports_absorb(&self) -> bool {
    false
  }
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

  #[test]
  fn split_and_absorb_default_to_unsupported() {
    // The zero-breakage proof: `Noop` predates the reshaping seams and overrides neither, so the
    // defaults answer for it — no split half, no absorb.
    let mut sm = Noop;
    assert_eq!(sm.split(b"anything").map(|_| ()), None);
    assert!(!sm.absorb(Noop));
  }

  /// A partitioned counter: `split` moves `give` units into the returned half; `absorb` folds a
  /// source's units back in. The minimal FSM proving the defaulted seams are overridable.
  struct Partitioned {
    units: u64,
  }

  impl StateMachine for Partitioned {
    type Command = ();
    type Response = ();
    type Snapshot = u64;
    type Error = core::convert::Infallible;

    fn apply(&mut self, _: crate::Index, _: ()) -> Result<(), Self::Error> {
      Ok(())
    }

    fn snapshot(&self) -> Result<u64, Self::Error> {
      Ok(self.units)
    }

    fn restore(&mut self, units: u64) -> Result<(), Self::Error> {
      self.units = units;
      Ok(())
    }

    fn split(&mut self, instruction: &[u8]) -> Option<Self> {
      let give = u64::from(*instruction.first()?);
      let give = give.min(self.units);
      self.units -= give;
      Some(Self { units: give })
    }

    fn absorb(&mut self, source: Self) -> bool {
      self.units += source.units;
      true
    }
  }

  #[test]
  fn overridden_split_partitions_and_absorb_reunites() {
    let mut sm = Partitioned { units: 10 };
    let half = sm.split(&[4]).expect("supported");
    assert_eq!((sm.units, half.units), (6, 4));
    assert!(sm.absorb(half));
    assert_eq!(sm.units, 10);
  }
}
