//! Bounds the in-flight `AppendEntries` to a peer by message count and total bytes, so the
//! leader never overruns the transport's send buffer (Raft thesis flow control).
use crate::Index;
use std::collections::VecDeque;

/// A bounded window of unacknowledged in-flight appends to one peer.
#[derive(Debug, Clone)]
pub struct Inflights {
  inflight: VecDeque<(Index, u64)>, // (last index of the msg, byte size)
  max_msgs: usize,
  max_bytes: u64, // 0 = no byte cap
  bytes: u64,
}

impl Inflights {
  /// A window capped at `max_msgs` messages and `max_bytes` total bytes (0 = uncapped bytes).
  pub fn new(max_msgs: usize, max_bytes: u64) -> Self {
    Self {
      inflight: VecDeque::new(),
      max_msgs,
      max_bytes,
      bytes: 0,
    }
  }

  /// Whether the window is full (count OR byte cap reached) — the leader must not send more.
  #[inline(always)]
  pub fn full(&self) -> bool {
    self.inflight.len() >= self.max_msgs || (self.max_bytes != 0 && self.bytes >= self.max_bytes)
  }

  /// Record a sent message whose highest index is `index` carrying `bytes`.
  pub fn add(&mut self, index: Index, bytes: u64) {
    self.inflight.push_back((index, bytes));
    self.bytes += bytes;
  }

  /// Free every in-flight entry whose index is `<= to` (the peer acked up to `to`).
  pub fn free_le(&mut self, to: Index) {
    while let Some(&(idx, b)) = self.inflight.front() {
      if idx > to {
        break;
      }
      self.bytes -= b;
      self.inflight.pop_front();
    }
  }

  /// Drop all in-flight tracking (on a `Progress` reset / become-probe).
  pub fn reset(&mut self) {
    self.inflight.clear();
    self.bytes = 0;
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn full_by_count_and_bytes() {
    let mut f = Inflights::new(2, 0); // max 2 msgs, no byte cap
    assert!(!f.full());
    f.add(crate::Index::new(1), 10);
    f.add(crate::Index::new(2), 10);
    assert!(f.full());
    f.free_le(crate::Index::new(1)); // ack of 1 frees one slot
    assert!(!f.full());
    let mut b = Inflights::new(10, 15); // byte cap 15
    b.add(crate::Index::new(1), 10);
    assert!(!b.full());
    b.add(crate::Index::new(2), 10); // 20 bytes > 15
    assert!(b.full());
  }
}
