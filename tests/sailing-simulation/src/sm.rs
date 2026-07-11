//! A minimal test state machine: it records the ordered `(Index, Bytes)` it applied, and
//! returns the applied byte length as the response (enough for agreement + dedup checks).
use bytes::Bytes;
use sailing_proto::{Data as _, Index, StateMachine};
use std::vec::Vec;

/// Decode failure for `LogSm::restore`.
#[derive(Debug)]
pub struct SmDecodeError;

impl core::fmt::Display for SmDecodeError {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.write_str("snapshot decode error")
  }
}

impl std::error::Error for SmDecodeError {}

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
  /// Serialised as: u64-le entry count, then for each entry: u64-le index, u64-le payload
  /// length, payload bytes.
  type Snapshot = Bytes;
  type Error = SmDecodeError;

  fn apply(&mut self, index: Index, cmd: Bytes) -> Result<usize, Self::Error> {
    let len = cmd.len();
    self.applied.push((index, cmd)); // moved in — no clone
    Ok(len)
  }

  /// THE SIM'S PARTITION CONTRACT: the instruction is a split point — EXACTLY 2 bytes, a `u16`
  /// LE key `p` over the gkv key space. Every applied cell whose command decodes as a gkv
  /// payload with `key >= p` MOVES to the returned child (order and indices preserved); gkv
  /// cells below `p` and every non-gkv command STAY with the parent (an un-keyed command has no
  /// side to move to, and the conservation oracle judges keyed histories only). Any other
  /// instruction length is malformed ⇒ `None`, state untouched — the trait's unsupported arm,
  /// which a committed `Split` entry then converts into a deterministic
  /// `PoisonReason::SplitUnsupported` fail-stop on every replica.
  ///
  /// A pure function of `(state, instruction)`: both sides are order-preserving subsequences of
  /// the applied record, so every replica applying the same committed entry partitions
  /// identically — the `apply`-grade determinism the seam's contract demands.
  fn split(&mut self, instruction: &[u8]) -> Option<Self> {
    let point = u16::from_le_bytes(instruction.try_into().ok()?);
    let (child, parent) = core::mem::take(&mut self.applied)
      .into_iter()
      .partition(|(_, cmd)| crate::multi::decode_gkv(cmd).is_some_and(|(_, key, _)| key >= point));
    self.applied = parent;
    Some(Self { applied: child })
  }

  /// THE SIM'S UNION CONTRACT: append the absorbed source's record onto this one's tail —
  /// order preserved, indices untouched (absorbed cells keep their SOURCE-log indices, exactly
  /// as inherited fork cells keep parent indices). A pure append of two deterministic records
  /// at a log-fixed boundary, so every replica resolving the same commit absorbs identically;
  /// the conservation walk re-admits the cells under the TARGET's ledger id in this order (a
  /// contiguous run trailing the target's own cells), which is what `assert_union` judges.
  fn absorb(&mut self, source: Self) -> bool {
    self.applied.extend(source.applied);
    true
  }

  // Type-constant capability gates consulted at propose. This FSM CAN split and absorb; the
  // per-instruction `None`/`false` verdicts above stay the apply-time determinism backstop (a
  // malformed `Split` instruction still fail-stops via `PoisonReason::SplitUnsupported`).
  fn supports_split(&self) -> bool {
    true
  }

  fn supports_absorb(&self) -> bool {
    true
  }

  fn snapshot(&self) -> Result<Bytes, Self::Error> {
    let mut buf: Vec<u8> = Vec::new();
    // entry count
    (self.applied.len() as u64).encode(&mut buf);
    for (idx, data) in &self.applied {
      idx.get().encode(&mut buf);
      // length-prefixed payload
      (data.len() as u64).encode(&mut buf);
      buf.extend_from_slice(data);
    }
    Ok(Bytes::from(buf))
  }

  fn restore(&mut self, snapshot: Bytes) -> Result<(), Self::Error> {
    // Cursor-based decode over the shared snapshot buffer: payload slices are zero-copy.
    let mut cur = sailing_proto::ByteCursor::new(snapshot);

    let count = u64::decode(&mut cur).map_err(|_| SmDecodeError)?;
    let mut entries: Vec<(Index, Bytes)> = Vec::new();
    for _ in 0..count {
      let raw_idx = u64::decode(&mut cur).map_err(|_| SmDecodeError)?;
      let len = u64::decode(&mut cur).map_err(|_| SmDecodeError)?;
      let len = usize::try_from(len).map_err(|_| SmDecodeError)?;
      let payload = cur.take_bytes(len).map_err(|_| SmDecodeError)?;
      entries.push((Index::new(raw_idx), payload));
    }
    if !cur.is_empty() {
      return Err(SmDecodeError); // trailing bytes: a malformed snapshot
    }
    self.applied = entries;
    Ok(())
  }
}

#[cfg(test)]
mod tests;
