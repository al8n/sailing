//! Shared helpers for the throughput benches (`pure_core`, `parity`).

use bytes::Bytes;
use sailing_proto::{Data as _, Index, StateMachine};

/// Snapshot decode failure for [`CountSm`].
#[derive(Debug)]
pub struct CountSmError;

impl core::fmt::Display for CountSmError {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.write_str("count snapshot decode error")
  }
}

impl std::error::Error for CountSmError {}

/// A counting state machine with an **O(1) snapshot**.
///
/// It keeps only a running count of applied commands, so `snapshot()` is a fixed ~8 bytes regardless
/// of run length. The simulation's `LogSm` instead records every applied entry and re-encodes its
/// whole (never-truncated) history on each `snapshot()` — an O(n) cost that compounds to O(n²) over a
/// long run. That artifact is why the benches previously had to *disable* snapshotting; doing so also
/// disabled the only compaction trigger, so the log grew unbounded and the measurement drifted into
/// allocator/cache territory.
///
/// With this FSM the benches can leave normal log compaction ON (the default `snapshot_threshold`):
/// the log stays bounded to ~one threshold of entries, snapshots stay O(1), and the put/s is a
/// bounded steady-state read on consensus work — stable across `-n` and comparable to a long
/// openraft run (which also compacts).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CountSm {
  applied: u64,
}

impl CountSm {
  /// A fresh state machine with a zero count.
  pub fn new() -> Self {
    Self::default()
  }

  /// The number of commands applied so far.
  pub fn applied(&self) -> u64 {
    self.applied
  }
}

impl StateMachine for CountSm {
  type Command = Bytes;
  type Response = u64;
  /// The applied count, encoded as a single little-endian `u64`.
  type Snapshot = Bytes;
  type Error = CountSmError;

  fn apply(&mut self, _index: Index, _cmd: Bytes) -> Result<u64, Self::Error> {
    self.applied += 1;
    Ok(self.applied)
  }

  fn snapshot(&self) -> Result<Bytes, Self::Error> {
    // O(1): the entire state is one counter.
    let mut buf = std::vec::Vec::with_capacity(8);
    self.applied.encode(&mut buf);
    Ok(Bytes::from(buf))
  }

  fn restore(&mut self, snapshot: Bytes) -> Result<(), Self::Error> {
    let mut cur = sailing_proto::ByteCursor::new(snapshot);
    let applied = u64::decode(&mut cur).map_err(|_| CountSmError)?;
    if !cur.is_empty() {
      return Err(CountSmError); // trailing bytes: a malformed snapshot
    }
    self.applied = applied;
    Ok(())
  }
}

/// A split/absorb-capable counting state machine for the reshaping benches.
///
/// Like [`CountSm`] it keeps only a running applied count, so its snapshot is O(1) and a group
/// hosting it commits at the same bounded steady-state cost. It ADDITIONALLY overrides the
/// reshaping seams: [`split`](StateMachine::split) hands the child HALF the count (the parent
/// keeps the rest) and [`absorb`](StateMachine::absorb) folds an absorbed source's count back in,
/// so a split-then-merge-back cycle conserves the total. It exists so the reshaping-cost benches
/// (`parity --reshape`, `sharded_host --reshape`) can drive the REAL `propose_split` / merge verbs
/// against an FSM whose per-op cost stays O(1) — keeping the throughput a read on consensus +
/// reshape work rather than on the O(n) snapshot artifact the simulation `LogSm` carries.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ReshapeSm {
  applied: u64,
}

impl ReshapeSm {
  /// A fresh state machine with a zero count.
  pub fn new() -> Self {
    Self::default()
  }

  /// The number of commands applied so far.
  pub fn applied(&self) -> u64 {
    self.applied
  }
}

impl StateMachine for ReshapeSm {
  type Command = Bytes;
  type Response = u64;
  /// The applied count, encoded as a single little-endian `u64` (O(1), as [`CountSm`]).
  type Snapshot = Bytes;
  type Error = CountSmError;

  fn apply(&mut self, _index: Index, _cmd: Bytes) -> Result<u64, Self::Error> {
    self.applied += 1;
    Ok(self.applied)
  }

  fn snapshot(&self) -> Result<Bytes, Self::Error> {
    let mut buf = std::vec::Vec::with_capacity(8);
    self.applied.encode(&mut buf);
    Ok(Bytes::from(buf))
  }

  fn restore(&mut self, snapshot: Bytes) -> Result<(), Self::Error> {
    let mut cur = sailing_proto::ByteCursor::new(snapshot);
    let applied = u64::decode(&mut cur).map_err(|_| CountSmError)?;
    if !cur.is_empty() {
      return Err(CountSmError); // trailing bytes: a malformed snapshot
    }
    self.applied = applied;
    Ok(())
  }

  /// Hand the child HALF the applied count (rounding down); the parent keeps the remainder. The
  /// opaque `instruction` is ignored — a counter has no keys to partition, only a magnitude to
  /// halve, and half is a deterministic function of the pre-split count, so every replica applying
  /// the same committed `Split` partitions identically (the seam's `apply`-grade determinism).
  fn split(&mut self, _instruction: &[u8]) -> Option<Self> {
    let give = self.applied / 2;
    self.applied -= give;
    Some(Self { applied: give })
  }

  /// Fold an absorbed source's count back into this one — the inverse of an earlier
  /// [`split`](Self::split), so a split-then-merge-back cycle restores the pre-split total.
  fn absorb(&mut self, source: Self) -> bool {
    self.applied += source.applied;
    true
  }

  fn supports_split(&self) -> bool {
    true
  }

  fn supports_absorb(&self) -> bool {
    true
  }
}
