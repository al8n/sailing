//! ReadIndex tracking: pending read requests, heartbeat-ack bookkeeping, and confirmed read
//! states.  Port of the classical etcd `readOnly` structure (context-keyed variant).
use crate::{Index, NodeId, ReadOnlyOption};
use bytes::Bytes;
use std::{
  collections::{BTreeMap, BTreeSet},
  vec::Vec,
};

// ─── ReadState ────────────────────────────────────────────────────────────────

/// A confirmed linearizable read: the application may serve the read once
/// `applied >= index`.
///
/// Produced by [`crate::Endpoint::poll_event`] as [`crate::Event::ReadState`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadState {
  index: Index,
  context: Bytes,
}

impl ReadState {
  /// Construct a `ReadState`.
  pub fn new(index: Index, context: Bytes) -> Self {
    Self { index, context }
  }

  /// The commit index that was confirmed at the time of the read request.
  ///
  /// The application must wait until `applied >= index` before serving the read.
  #[inline(always)]
  pub const fn index(&self) -> Index {
    self.index
  }

  /// Opaque application context identifying this read request (echoed unchanged).
  #[inline(always)]
  pub fn context(&self) -> &Bytes {
    &self.context
  }
}

// ─── ReadIndexStatus ──────────────────────────────────────────────────────────

/// A pending read-index request: tracks who originated it, the commit index at
/// the time of receipt, and which voters have acked the heartbeat round.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadIndexStatus<I> {
  /// The opaque application context that identifies this read request (echoed back in
  /// `ReadState` / `ReadIndexResp`).
  context: Bytes,
  /// The originator: `None` = local leader request, `Some(follower)` = forwarded.
  req_from: Option<I>,
  /// The leader's commit index at the time the read was received.
  index: Index,
  /// Voters that have acknowledged the heartbeat round carrying this read's context.
  ///
  /// The leader's own ack is included immediately on creation (the leader always
  /// counts toward its own quorum).
  acks: BTreeSet<I>,
}

impl<I: NodeId> ReadIndexStatus<I> {
  /// Construct a new status.  `leader` is included in `acks` immediately (the
  /// leader counts itself toward quorum without waiting for a heartbeat reply).
  pub fn new(context: Bytes, req_from: Option<I>, index: Index, leader: I) -> Self {
    let mut acks = BTreeSet::new();
    acks.insert(leader);
    Self {
      context,
      req_from,
      index,
      acks,
    }
  }

  /// Consume the confirmed read into the `(context, originator, index)` the leader needs to emit the
  /// `ReadState` (local read) or `ReadIndexResp` (forwarded read). `originator` is `None` for a local
  /// leader read and `Some(follower)` for a forwarded one.
  pub fn into_parts(self) -> (Bytes, Option<I>, Index) {
    (self.context, self.req_from, self.index)
  }
}

// ─── ReadOnly ─────────────────────────────────────────────────────────────────

/// Manages in-flight read-index requests for the leader.
///
/// Pending reads are keyed by their opaque `context` bytes (FIFO queue).  When a
/// heartbeat-response quorum acks a context, all reads up to and including it are
/// confirmed and returned via [`ReadOnly::advance`].
#[derive(Debug, Clone)]
pub struct ReadOnly<I> {
  /// How reads are confirmed.
  option: ReadOnlyOption,
  /// Pending reads, keyed by context.
  pending: BTreeMap<Bytes, ReadIndexStatus<I>>,
  /// FIFO queue of context keys (preserves insertion order for batch confirmation).
  queue: Vec<Bytes>,
}

impl<I: NodeId> ReadOnly<I> {
  /// Construct a new `ReadOnly` manager.
  pub fn new(option: ReadOnlyOption) -> Self {
    Self {
      option,
      pending: BTreeMap::new(),
      queue: Vec::new(),
    }
  }

  /// The configured read-only option.
  #[inline(always)]
  #[allow(dead_code, reason = "internal accessor; retained for completeness")]
  pub const fn option(&self) -> ReadOnlyOption {
    self.option
  }

  /// Reset the `ReadOnly` manager, discarding all pending requests.
  ///
  /// Called on step-down and on `become_leader` to ensure stale reads from the
  /// previous term are never confirmed.
  pub fn reset(&mut self, option: ReadOnlyOption) {
    self.option = option;
    self.pending.clear();
    self.queue.clear();
  }

  /// Record a new pending read request.
  ///
  /// `index` is the leader's commit index at receipt.  `context` is an opaque
  /// application token identifying this request.  `from` is `None` for local
  /// (leader-application) reads and `Some(follower_id)` for forwarded reads.
  /// `leader` is the current leader's id — it is included in the ack set
  /// immediately.
  ///
  /// Idempotent: if `context` is already pending this call is a no-op (matches
  /// etcd's early-return-if-present behaviour).
  ///
  /// Returns `true` if the request was newly recorded, or `false` if a request with this
  /// exact `context` was already pending (the duplicate-context case the caller surfaces as
  /// [`crate::ReadIndexError::DuplicateContext`]).
  pub fn add_request(&mut self, index: Index, context: Bytes, from: Option<I>, leader: I) -> bool {
    if self.pending.contains_key(&context) {
      return false;
    }
    let status = ReadIndexStatus::new(context.clone(), from, index, leader);
    self.pending.insert(context.clone(), status);
    self.queue.push(context);
    true
  }

  /// Record that `from` has acknowledged the heartbeat round whose context is
  /// `context`.
  ///
  /// Returns the **total number of acks** (including the self-ack seeded at
  /// creation) for the identified pending request.  Returns `0` if no pending
  /// request with that context exists.
  ///
  /// Calling this multiple times with the same `from` is idempotent (`BTreeSet`
  /// deduplicates).
  pub fn recv_ack(&mut self, from: I, context: &[u8]) -> usize {
    let Some(status) = self.pending.get_mut(context) else {
      return 0;
    };
    status.acks.insert(from);
    status.acks.len()
  }

  /// Confirm all pending reads up to and including the one identified by
  /// `context`.
  ///
  /// Pops (in FIFO order) every pending read whose position in the queue is ≤
  /// the position of `context`, removes them from `pending`, and returns them.
  /// If `context` is not found in the queue, returns an empty `Vec`.
  ///
  /// The FIFO guarantee: a later read is confirmed no earlier than an earlier
  /// one — once the heartbeat round for `context` achieves quorum, all preceding
  /// reads have also been confirmed (they were added at an equal or lower commit
  /// index and their heartbeat rounds were sent first).
  pub fn advance(&mut self, context: &[u8]) -> Vec<ReadIndexStatus<I>> {
    // Find the position of `context` in the queue.
    let Some(pos) = self.queue.iter().position(|c| c.as_ref() == context) else {
      return Vec::new();
    };
    // Drain queue[0..=pos] and remove corresponding pending entries.
    let confirmed: Vec<Bytes> = self.queue.drain(..=pos).collect();
    let mut out = Vec::with_capacity(confirmed.len());
    for ctx in &confirmed {
      if let Some(status) = self.pending.remove(ctx) {
        out.push(status);
      }
    }
    out
  }

  /// The context of the most recently queued read request — used as the heartbeat
  /// context marker so the leader's next heartbeat round covers all pending reads.
  ///
  /// Returns `None` when there are no pending reads.
  pub fn last_pending_request_ctx(&self) -> Option<&Bytes> {
    self.queue.last()
  }

  /// Whether there are no pending reads.
  #[inline(always)]
  #[allow(dead_code, reason = "internal accessor; retained for completeness")]
  pub fn is_empty(&self) -> bool {
    self.queue.is_empty()
  }

  /// Return a reference to the ack set for the pending read identified by `context`,
  /// or `None` if no such request is pending.
  pub fn acks_for(&self, context: &[u8]) -> Option<&std::collections::BTreeSet<I>> {
    self.pending.get(context).map(|s| &s.acks)
  }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
  use super::*;
  use crate::{Index, Term};

  fn idx(n: u64) -> Index {
    Index::new(n)
  }

  fn ctx(b: &'static [u8]) -> Bytes {
    Bytes::from_static(b)
  }

  // ── ReadState accessors ────────────────────────────────────────────────────

  #[test]
  fn read_state_accessors() {
    let rs = ReadState::new(idx(42), ctx(b"hello"));
    assert_eq!(rs.index(), idx(42));
    assert_eq!(rs.context().as_ref(), b"hello");
  }

  // ── add_request / recv_ack / advance ──────────────────────────────────────

  /// Three voters (1=leader, 2, 3).  Context "a" is added, peers 2 and 3 ack →
  /// quorum reached (leader self-ack + peer2 = 2/3, not yet; + peer3 = 3/3).
  #[test]
  fn add_recv_ack_advance_basic() {
    let mut ro: ReadOnly<u64> = ReadOnly::new(ReadOnlyOption::Safe);

    // Leader 1 adds a read at commit index 5.
    ro.add_request(idx(5), ctx(b"ctx_a"), None, 1u64);
    assert_eq!(ro.queue.len(), 1);
    assert!(ro.pending.contains_key(ctx(b"ctx_a").as_ref()));

    // Leader self-ack is already in the set; peer 2 acks.
    let n = ro.recv_ack(2u64, b"ctx_a");
    assert_eq!(n, 2); // leader 1 + peer 2

    // Peer 3 acks → 3 acks now.
    let n = ro.recv_ack(3u64, b"ctx_a");
    assert_eq!(n, 3);

    // advance → returns the one status.
    let confirmed = ro.advance(b"ctx_a");
    assert_eq!(confirmed.len(), 1);
    assert_eq!(confirmed[0].index, idx(5));
    assert!(confirmed[0].req_from.is_none());

    // queue and pending are now empty.
    assert!(ro.is_empty());
  }

  /// Same peer acking twice counts once (BTreeSet dedup).
  #[test]
  fn recv_ack_same_peer_twice_counts_once() {
    let mut ro: ReadOnly<u64> = ReadOnly::new(ReadOnlyOption::Safe);
    ro.add_request(idx(3), ctx(b"ctx_b"), None, 1u64);

    ro.recv_ack(2u64, b"ctx_b");
    let n1 = ro.recv_ack(2u64, b"ctx_b"); // duplicate
    // Still only leader(1) + peer(2) = 2.
    assert_eq!(n1, 2);
  }

  /// Idempotent add: adding the same context twice is a no-op.
  #[test]
  fn add_request_idempotent() {
    let mut ro: ReadOnly<u64> = ReadOnly::new(ReadOnlyOption::Safe);
    // First add is newly recorded → true.
    assert!(ro.add_request(idx(10), ctx(b"ctx_c"), None, 1u64));
    // Second add with the same context is a no-op → false (duplicate context).
    assert!(!ro.add_request(idx(99), ctx(b"ctx_c"), Some(2u64), 1u64));
    assert_eq!(ro.queue.len(), 1);
    // Index must be from the FIRST add (10), not 99.
    assert_eq!(ro.pending[ctx(b"ctx_c").as_ref()].index, idx(10));
  }

  /// `advance` on a middle context confirms it AND all earlier ones (FIFO).
  #[test]
  fn advance_middle_confirms_all_earlier() {
    let mut ro: ReadOnly<u64> = ReadOnly::new(ReadOnlyOption::Safe);
    ro.add_request(idx(1), ctx(b"r1"), None, 1u64);
    ro.add_request(idx(2), ctx(b"r2"), None, 1u64);
    ro.add_request(idx(3), ctx(b"r3"), None, 1u64);

    // Advance "r2" — must return r1 and r2 (FIFO), not r3.
    let confirmed = ro.advance(b"r2");
    assert_eq!(confirmed.len(), 2);
    assert_eq!(confirmed[0].index, idx(1)); // r1
    assert_eq!(confirmed[1].index, idx(2)); // r2

    // r3 is still pending.
    assert_eq!(ro.queue.len(), 1);
    assert_eq!(ro.queue[0], ctx(b"r3"));
  }

  /// `advance` with unknown context returns empty.
  #[test]
  fn advance_unknown_context_is_empty() {
    let mut ro: ReadOnly<u64> = ReadOnly::new(ReadOnlyOption::Safe);
    ro.add_request(idx(5), ctx(b"r1"), None, 1u64);
    let confirmed = ro.advance(b"unknown");
    assert!(confirmed.is_empty());
    // r1 is still there.
    assert_eq!(ro.queue.len(), 1);
  }

  /// `last_pending_request_ctx` tracks the most-recently added context.
  #[test]
  fn last_pending_request_ctx() {
    let mut ro: ReadOnly<u64> = ReadOnly::new(ReadOnlyOption::Safe);
    assert!(ro.last_pending_request_ctx().is_none());

    ro.add_request(idx(1), ctx(b"first"), None, 1u64);
    assert_eq!(ro.last_pending_request_ctx().unwrap().as_ref(), b"first");

    ro.add_request(idx(2), ctx(b"second"), None, 1u64);
    assert_eq!(ro.last_pending_request_ctx().unwrap().as_ref(), b"second");
  }

  /// `reset` discards all pending requests.
  #[test]
  fn reset_clears_state() {
    let mut ro: ReadOnly<u64> = ReadOnly::new(ReadOnlyOption::Safe);
    ro.add_request(idx(1), ctx(b"x"), None, 1u64);
    ro.add_request(idx(2), ctx(b"y"), None, 1u64);
    ro.reset(ReadOnlyOption::LeaseBased);
    assert!(ro.is_empty());
    assert_eq!(ro.option(), ReadOnlyOption::LeaseBased);
  }

  /// Forwarded request: `req_from` is `Some`.
  #[test]
  fn forwarded_request_has_req_from() {
    let mut ro: ReadOnly<u64> = ReadOnly::new(ReadOnlyOption::Safe);
    ro.add_request(idx(7), ctx(b"fwd"), Some(3u64), 1u64);
    let s = &ro.pending[ctx(b"fwd").as_ref()];
    assert_eq!(s.req_from, Some(3u64));
    assert_eq!(s.index, idx(7));
    // Leader self-ack is in the set.
    assert!(s.acks.contains(&1u64));
    assert!(!s.acks.contains(&3u64));
  }

  /// `recv_ack` on a non-existent context returns 0.
  #[test]
  fn recv_ack_nonexistent_returns_zero() {
    let mut ro: ReadOnly<u64> = ReadOnly::new(ReadOnlyOption::Safe);
    assert_eq!(ro.recv_ack(2u64, b"ghost"), 0);
  }

  // ── ReadState: PartialEq / Clone ───────────────────────────────────────────

  #[test]
  fn read_state_clone_and_eq() {
    let rs = ReadState::new(idx(10), Bytes::from_static(b"abc"));
    let rs2 = rs.clone();
    assert_eq!(rs, rs2);
    let rs3 = ReadState::new(idx(11), Bytes::from_static(b"abc"));
    assert_ne!(rs, rs3);
  }

  // Unused import suppressor.
  #[allow(dead_code)]
  fn _use_term() -> Term {
    Term::ZERO
  }
}
