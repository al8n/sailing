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

  /// Free the single oldest in-flight entry (front of the queue).
  ///
  /// Called by `Progress::free_inflight_on_heartbeat` (etcd `FreeFirstOne`) to let the
  /// leader send one probe per heartbeat round to a `Replicate` peer whose in-flight window
  /// is full because all acks were lost (e.g. a healed partition). No-op if the queue is
  /// empty.
  pub fn free_first_one(&mut self) {
    if let Some((_, b)) = self.inflight.pop_front() {
      self.bytes -= b;
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
    f.add(Index::new(1), 10);
    f.add(Index::new(2), 10);
    assert!(f.full());
    f.free_le(Index::new(1)); // ack of 1 frees one slot
    assert!(!f.full());
    let mut b = Inflights::new(10, 15); // byte cap 15
    b.add(Index::new(1), 10);
    assert!(!b.full());
    b.add(Index::new(2), 10); // 20 bytes > 15
    assert!(b.full());
  }

  #[test]
  fn free_first_one_unblocks_full_window() {
    // Fill to capacity (count cap = 2).
    let mut f = Inflights::new(2, 0);
    f.add(Index::new(1), 7);
    f.add(Index::new(2), 11);
    assert!(f.full(), "window must be full after two adds");
    let bytes_before = f.bytes;

    // Free the oldest (front) entry.
    f.free_first_one();
    assert!(
      !f.full(),
      "window must have one free slot after free_first_one"
    );
    assert_eq!(
      f.bytes,
      bytes_before - 7,
      "bytes must decrease by the freed entry's size"
    );

    // Freeing the second entry drains the window entirely.
    f.free_first_one();
    assert!(!f.full());
    assert_eq!(f.bytes, 0);

    // Calling on an empty queue is a safe no-op.
    f.free_first_one();
    assert_eq!(f.bytes, 0);
    assert!(!f.full());
  }
}
