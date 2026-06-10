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
    let buf = &snapshot[..];
    let mut pos = 0usize;

    // decode count — requires ≥ 8 bytes
    let (n, count) = u64::decode(&buf[pos..]).map_err(|_| SmDecodeError)?;
    pos += n;

    let mut entries: Vec<(Index, Bytes)> = Vec::with_capacity(count as usize);
    for _ in 0..count {
      // decode index — requires ≥ 8 bytes
      let (n2, raw_idx) = u64::decode(&buf[pos..]).map_err(|_| SmDecodeError)?;
      pos += n2;
      // decode payload length — requires ≥ 8 bytes
      let (n3, len) = u64::decode(&buf[pos..]).map_err(|_| SmDecodeError)?;
      pos += n3;
      // guard against overflow and truncation
      let end = pos.checked_add(len as usize).ok_or(SmDecodeError)?;
      if end > buf.len() {
        return Err(SmDecodeError);
      }
      let payload = Bytes::copy_from_slice(&buf[pos..end]);
      pos = end;
      entries.push((Index::new(raw_idx), payload));
    }
    self.applied = entries;
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

  #[test]
  fn log_sm_snapshot_restore_roundtrip() {
    let mut sm = LogSm::new();
    sm.apply(Index::new(1), bytes::Bytes::from_static(b"alpha"))
      .unwrap();
    sm.apply(Index::new(2), bytes::Bytes::from_static(b"beta"))
      .unwrap();
    sm.apply(Index::new(3), bytes::Bytes::from_static(b"gamma"))
      .unwrap();

    let snap = sm.snapshot().unwrap();
    let mut sm2 = LogSm::new();
    sm2.restore(snap).unwrap();
    assert_eq!(
      sm.applied(),
      sm2.applied(),
      "restore must reproduce exact state"
    );
  }

  #[test]
  fn log_sm_empty_snapshot_roundtrip() {
    let sm = LogSm::new();
    let snap = sm.snapshot().unwrap();
    let mut sm2 = LogSm::new();
    sm2.restore(snap).unwrap();
    assert!(sm2.applied().is_empty());
  }

  #[test]
  fn restore_malformed_returns_err_never_panics() {
    // empty buffer — can't read the count
    let mut sm = LogSm::new();
    assert!(sm.restore(bytes::Bytes::new()).is_err());

    // declared count=1 but no body follows
    let mut buf: Vec<u8> = Vec::new();
    (1u64).encode(&mut buf); // count = 1
    assert!(sm.restore(bytes::Bytes::from(buf)).is_err());

    // declared count=1 with index+len present but payload absent (len says 100, buf empty after)
    let mut buf2: Vec<u8> = Vec::new();
    (1u64).encode(&mut buf2); // count = 1
    (42u64).encode(&mut buf2); // index = 42
    (100u64).encode(&mut buf2); // payload len = 100, but no payload bytes follow
    assert!(sm.restore(bytes::Bytes::from(buf2)).is_err());

    // absurd length prefix (u64::MAX) — must not overflow/panic
    let mut buf3: Vec<u8> = Vec::new();
    (1u64).encode(&mut buf3); // count = 1
    (1u64).encode(&mut buf3); // index = 1
    (u64::MAX).encode(&mut buf3); // payload len = u64::MAX
    assert!(sm.restore(bytes::Bytes::from(buf3)).is_err());

    // state must be untouched after failed restores
    assert!(sm.applied().is_empty());
  }
}
