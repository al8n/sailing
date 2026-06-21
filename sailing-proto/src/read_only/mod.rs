//! ReadIndex tracking: pending read requests, heartbeat-ack bookkeeping, and confirmed read
//! states.  Adapted from etcd's `readOnly`, but keyed by an INTERNAL per-round token rather than the
//! application context (see [`ReadOnly`]) so the quorum proof is sound even when the application
//! reuses a context across reads.
use crate::{Index, ReadOnlyOption};
use bytes::Bytes;
use std::{
  collections::{BTreeMap, BTreeSet},
  vec::Vec,
};

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

/// A FAILOVER inherited-read offer, returned by [`crate::Endpoint::failover_read_window`] while a freshly
/// elected leader holds the post-election commit-wait under the LeaseGuard failover tier AND the committed
/// anchor's lease is provably still live.
///
/// It authorizes the application to serve a LINEARIZABLE read on the committed prefix at [`index`](Self::index)
/// — the SOLE LeaseGuard serve against a PRIOR-term commit index — WITHOUT waiting out the commit-wait,
/// PROVIDED the application first confirms its key was not written in the limbo region
/// `(index, limbo_upper]` (it scans/decodes those log entries in its own command format; a coarse
/// application may simply require the limbo region empty). The limbo check AND this lease-live offer
/// together are the linearizability substitute for the current-term-commit gate. Serve at `index` once
/// `applied >= index`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FailoverReadWindow {
  index: Index,
  limbo_upper: Index,
}

impl FailoverReadWindow {
  /// Construct a `FailoverReadWindow`.
  pub const fn new(index: Index, limbo_upper: Index) -> Self {
    Self { index, limbo_upper }
  }

  /// The committed index to serve the inherited read at — the commit index pinned at election. Serve
  /// once `applied >= index`.
  #[inline(always)]
  pub const fn index(&self) -> Index {
    self.index
  }

  /// The inclusive upper end of the limbo region `(index, limbo_upper]` the application must check does
  /// NOT write its key before serving at `index`. `limbo_upper == index` ⇒ empty limbo (nothing to check).
  #[inline(always)]
  pub const fn limbo_upper(&self) -> Index {
    self.limbo_upper
  }
}

/// A pending read-index request: tracks who originated it, the commit index at
/// the time of receipt, and which voters have acked the heartbeat round.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadIndexStatus<I> {
  /// The opaque application context that identifies this read request (echoed back in
  /// `ReadState` / `ReadIndexResponse`).
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

impl<I> ReadIndexStatus<I> {
  /// Consume the confirmed read into the `(context, originator, index)` the leader needs to emit the
  /// `ReadState` (local read) or `ReadIndexResponse` (forwarded read). `originator` is `None` for a local
  /// leader read and `Some(follower)` for a forwarded one.
  pub fn into_parts(self) -> (Bytes, Option<I>, Index) {
    (self.context, self.req_from, self.index)
  }
}

impl<I: Ord> ReadIndexStatus<I> {
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
}

/// Manages in-flight read-index requests for the leader.
///
/// Pending reads are keyed by an INTERNAL, monotonically-unique **round token** (an 8-byte
/// counter), NOT by the application's `context`.  This is what makes the heartbeat-quorum proof
/// sound under message duplication/reordering AND application context reuse: each read's heartbeat
/// round carries its own token, so a stale/duplicated `HeartbeatResponse` echoing an earlier round's
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

impl<I> ReadOnly<I> {
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

  /// Update the read-mode option WITHOUT discarding in-flight accepted reads. Used by the apply-time
  /// read-mode migration: the mode flips, but a read ALREADY accepted (added at its commit index) stays
  /// valid and still confirms under the mode-INDEPENDENT ReadIndex heartbeat quorum. `reset` (used on
  /// step-down / become_leader, where the term changes) instead discards them — which on a mid-term mode
  /// flip would strand the caller or a forwarding follower on a read `read_index` had already accepted.
  pub fn set_option(&mut self, option: ReadOnlyOption) {
    self.option = option;
  }

  #[cfg(test)]
  pub(crate) fn pending_len(&self) -> usize {
    self.pending.len()
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

impl<I: Ord> ReadOnly<I> {
  /// Record a new pending read request and assign it a fresh, internally-unique **round token**.
  ///
  /// `index` is the leader's commit index at receipt.  `context` is the opaque application token
  /// echoed back on confirmation.  `from` is `None` for local (leader-application) reads and
  /// `Some(follower_id)` for forwarded reads.  `leader` is the current leader's id — included in the
  /// ack set immediately (the leader counts toward its own quorum).
  ///
  /// Returns the round token the caller must seed into the heartbeat round for this read.  The token
  /// is NEVER reused, so the quorum proof is unambiguous: a stale/duplicated `HeartbeatResponse` echoing
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
}

#[cfg(test)]
mod tests;
