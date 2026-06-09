//! ReadIndex tracking: pending read requests, heartbeat-ack bookkeeping, and confirmed read
//! states.  Adapted from etcd's `readOnly`, but keyed by an INTERNAL per-round token rather than the
//! application context (see [`ReadOnly`]) so the quorum proof is sound even when the application
//! reuses a context across reads.
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
/// Pending reads are keyed by an INTERNAL, monotonically-unique **round token** (an 8-byte
/// counter), NOT by the application's `context`.  This is what makes the heartbeat-quorum proof
/// sound under message duplication/reordering AND application context reuse: each read's heartbeat
/// round carries its own token, so a stale/duplicated `HeartbeatResp` echoing an earlier round's
/// token can never be matched to a later read (even one that reuses the same user `context` after an
/// earlier read with that context completed).  The user `context` rides along inside the status and
/// is echoed back on confirmation.  When a heartbeat-response quorum acks a token, all reads up to
/// and including it are confirmed (FIFO) and returned via [`ReadOnly::advance`].
#[derive(Debug, Clone)]
pub struct ReadOnly<I> {
  /// How reads are confirmed.
  option: ReadOnlyOption,
  /// Pending reads, keyed by their internal round token.
  pending: BTreeMap<Bytes, ReadIndexStatus<I>>,
  /// FIFO queue of round-token keys (preserves insertion order for batch confirmation).
  queue: Vec<Bytes>,
  /// Monotonic source of round tokens.  Never reused for the life of the manager, so two reads —
  /// even with the same user `context` — get distinct tokens and thus independent quorum rounds.
  next_round: u64,
}

impl<I: NodeId> ReadOnly<I> {
  /// Construct a new `ReadOnly` manager.
  pub fn new(option: ReadOnlyOption) -> Self {
    Self {
      option,
      pending: BTreeMap::new(),
      queue: Vec::new(),
      next_round: 0,
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

  /// Record a new pending read request and assign it a fresh, internally-unique **round token**.
  ///
  /// `index` is the leader's commit index at receipt.  `context` is the opaque application token
  /// echoed back on confirmation.  `from` is `None` for local (leader-application) reads and
  /// `Some(follower_id)` for forwarded reads.  `leader` is the current leader's id — included in the
  /// ack set immediately (the leader counts toward its own quorum).
  ///
  /// Returns the round token the caller must seed into the heartbeat round for this read.  The token
  /// is NEVER reused, so the quorum proof is unambiguous: a stale/duplicated `HeartbeatResp` echoing
  /// an earlier read's token cannot be credited to this one, even when the user `context` is reused
  /// after an earlier read with the same context completed.  The caller's own in-flight dedup
  /// ([`Self::context_in_flight`]) surfaces a concurrent same-`context` reuse as
  /// [`crate::ReadIndexError::DuplicateContext`] before this is called.
  pub fn add_request(&mut self, index: Index, context: Bytes, from: Option<I>, leader: I) -> Bytes {
    let token = Bytes::copy_from_slice(&self.next_round.to_be_bytes());
    self.next_round += 1;
    let status = ReadIndexStatus::new(context, from, index, leader);
    self.pending.insert(token.clone(), status);
    self.queue.push(token.clone());
    token
  }

  /// Whether a LOCAL (leader-originated) pending read carries this user `context` — the in-flight
  /// dedup the leader's local read-index guard uses.  Reads are keyed by internal round token, so this
  /// scans the bounded pending set (`<= MAX_LEADER_READS`).  FORWARDED reads (`req_from = Some`) are
  /// excluded: their stored `context` is the forwarding follower's OWN per-follower token, which
  /// collides across followers (each starts at 0) and lives in a different namespace from the leader
  /// application's contexts — the follower owns its own user-context dedup.
  pub fn context_in_flight(&self, context: &[u8]) -> bool {
    self
      .pending
      .values()
      .any(|s| s.req_from.is_none() && s.context.as_ref() == context)
  }

  /// Record that `from` has acknowledged the heartbeat round identified by round `token`.
  ///
  /// Returns the **total number of acks** (including the self-ack seeded at
  /// creation) for the identified pending request.  Returns `0` if no pending
  /// request with that token exists (e.g. a stale ack echoing an already-confirmed,
  /// hence removed, round — which is exactly why a reused user context is safe).
  ///
  /// Calling this multiple times with the same `from` is idempotent (`BTreeSet`
  /// deduplicates).
  pub fn recv_ack(&mut self, from: I, token: &[u8]) -> usize {
    let Some(status) = self.pending.get_mut(token) else {
      return 0;
    };
    status.acks.insert(from);
    status.acks.len()
  }

  /// Confirm all pending reads up to and including the one identified by round `token`.
  ///
  /// Pops (in FIFO order) every pending read whose position in the queue is ≤
  /// the position of `token`, removes them from `pending`, and returns them.
  /// If `token` is not found in the queue, returns an empty `Vec`.
  ///
  /// The FIFO guarantee: a later read is confirmed no earlier than an earlier
  /// one — once the heartbeat round for `token` achieves quorum, all preceding
  /// reads have also been confirmed (they were added at an equal or lower commit
  /// index and their heartbeat rounds were sent first).
  pub fn advance(&mut self, token: &[u8]) -> Vec<ReadIndexStatus<I>> {
    // Find the position of `token` in the queue.
    let Some(pos) = self.queue.iter().position(|c| c.as_ref() == token) else {
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

  /// The round token of the most recently queued read request — seeded as the heartbeat
  /// context so the leader's next heartbeat round covers all pending reads (FIFO).
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

  /// The number of pending reads awaiting heartbeat-quorum confirmation. Bounds the leader's
  /// in-flight read backlog (together with `pending_reads`) so a partitioned leader — one that
  /// never gathers an ack quorum — cannot accumulate contexts without limit.
  #[inline(always)]
  pub fn len(&self) -> usize {
    self.queue.len()
  }

  /// Return a reference to the ack set for the pending read identified by round `token`,
  /// or `None` if no such request is pending.
  pub fn acks_for(&self, token: &[u8]) -> Option<&std::collections::BTreeSet<I>> {
    self.pending.get(token).map(|s| &s.acks)
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

  /// Three voters (1=leader, 2, 3).  A read is added (round token t); peers 2 and 3 ack t →
  /// quorum reached (leader self-ack + peer2 = 2/3, not yet; + peer3 = 3/3).
  #[test]
  fn add_recv_ack_advance_basic() {
    let mut ro: ReadOnly<u64> = ReadOnly::new(ReadOnlyOption::Safe);

    // Leader 1 adds a read at commit index 5 → round token t.
    let t = ro.add_request(idx(5), ctx(b"ctx_a"), None, 1u64);
    assert_eq!(ro.queue.len(), 1);
    assert!(ro.pending.contains_key(&t));
    assert!(ro.context_in_flight(b"ctx_a"));

    // Leader self-ack is already in the set; peer 2 acks the token.
    let n = ro.recv_ack(2u64, &t);
    assert_eq!(n, 2); // leader 1 + peer 2

    // Peer 3 acks → 3 acks now.
    let n = ro.recv_ack(3u64, &t);
    assert_eq!(n, 3);

    // advance → returns the one status, carrying the user context.
    let confirmed = ro.advance(&t);
    assert_eq!(confirmed.len(), 1);
    assert_eq!(confirmed[0].index, idx(5));
    assert!(confirmed[0].req_from.is_none());
    assert_eq!(confirmed[0].context.as_ref(), b"ctx_a");

    // queue and pending are now empty.
    assert!(ro.is_empty());
  }

  /// Same peer acking twice counts once (BTreeSet dedup).
  #[test]
  fn recv_ack_same_peer_twice_counts_once() {
    let mut ro: ReadOnly<u64> = ReadOnly::new(ReadOnlyOption::Safe);
    let t = ro.add_request(idx(3), ctx(b"ctx_b"), None, 1u64);

    ro.recv_ack(2u64, &t);
    let n1 = ro.recv_ack(2u64, &t); // duplicate
    // Still only leader(1) + peer(2) = 2.
    assert_eq!(n1, 2);
  }

  /// Each `add_request` mints a fresh, distinct round token — even for the SAME user context — so a
  /// reused context never collides internally (the property the linearizable-read fix relies on).
  #[test]
  fn add_request_assigns_unique_round_tokens() {
    let mut ro: ReadOnly<u64> = ReadOnly::new(ReadOnlyOption::Safe);
    let t1 = ro.add_request(idx(10), ctx(b"ctx_c"), None, 1u64);
    let t2 = ro.add_request(idx(99), ctx(b"ctx_c"), Some(2u64), 1u64);
    assert_ne!(
      t1, t2,
      "the same user context must still get distinct round tokens"
    );
    assert_eq!(ro.queue.len(), 2);
    // Each token maps to its own status (keyed by token, not context).
    assert_eq!(ro.pending[&t1].index, idx(10));
    assert_eq!(ro.pending[&t2].index, idx(99));
    assert!(ro.context_in_flight(b"ctx_c"));
  }

  /// `advance` on a middle token confirms it AND all earlier ones (FIFO).
  #[test]
  fn advance_middle_confirms_all_earlier() {
    let mut ro: ReadOnly<u64> = ReadOnly::new(ReadOnlyOption::Safe);
    let _t1 = ro.add_request(idx(1), ctx(b"r1"), None, 1u64);
    let t2 = ro.add_request(idx(2), ctx(b"r2"), None, 1u64);
    let t3 = ro.add_request(idx(3), ctx(b"r3"), None, 1u64);

    // Advance t2 — must return r1 and r2 (FIFO), not r3.
    let confirmed = ro.advance(&t2);
    assert_eq!(confirmed.len(), 2);
    assert_eq!(confirmed[0].index, idx(1)); // r1
    assert_eq!(confirmed[1].index, idx(2)); // r2

    // r3 (token t3) is still pending.
    assert_eq!(ro.queue.len(), 1);
    assert_eq!(ro.queue[0], t3);
  }

  /// `advance` with an unknown token returns empty.
  #[test]
  fn advance_unknown_token_is_empty() {
    let mut ro: ReadOnly<u64> = ReadOnly::new(ReadOnlyOption::Safe);
    ro.add_request(idx(5), ctx(b"r1"), None, 1u64);
    let confirmed = ro.advance(b"unknown");
    assert!(confirmed.is_empty());
    // r1 is still there.
    assert_eq!(ro.queue.len(), 1);
  }

  /// `last_pending_request_ctx` tracks the most-recently added read's round token.
  #[test]
  fn last_pending_request_ctx() {
    let mut ro: ReadOnly<u64> = ReadOnly::new(ReadOnlyOption::Safe);
    assert!(ro.last_pending_request_ctx().is_none());

    let t1 = ro.add_request(idx(1), ctx(b"first"), None, 1u64);
    assert_eq!(ro.last_pending_request_ctx().unwrap(), &t1);

    let t2 = ro.add_request(idx(2), ctx(b"second"), None, 1u64);
    assert_eq!(ro.last_pending_request_ctx().unwrap(), &t2);
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
    let t = ro.add_request(idx(7), ctx(b"fwd"), Some(3u64), 1u64);
    let s = &ro.pending[&t];
    assert_eq!(s.req_from, Some(3u64));
    assert_eq!(s.index, idx(7));
    assert_eq!(s.context.as_ref(), b"fwd");
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
