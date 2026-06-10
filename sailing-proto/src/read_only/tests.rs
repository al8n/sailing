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
