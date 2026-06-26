use super::{super::*, *};
use crate::{
  Heartbeat, HeartbeatResponse, ReadIndexError, ReadIndexResponse, ReadState,
  testkit::{CountSm, NoopStable, VecLog},
};

/// Test 1: Safe read confirmed only after a heartbeat quorum.
///
/// A 3-node leader (with a current-term commit) calls `read_index(ctx)` →
/// broadcasts heartbeats with ctx; NO `ReadState` until a quorum of `HeartbeatResponse`
/// arrive; after the quorum, exactly one `Event::ReadState` is emitted.
#[test]
fn safe_read_confirmed_after_heartbeat_quorum() {
  let (mut ep, mut log, mut stable, d) = make_leader_with_current_term_commit();
  let ctx = bytes::Bytes::from_static(b"read_1");

  ep.read_index(d, &log, &stable, ctx.clone())
    .expect("leader with a current-term commit must accept the read");

  // The leader broadcasts read heartbeats carrying the INTERNAL round token (NOT the user ctx).
  // Capture the token to echo it back in the HeartbeatResponse, exactly as a real follower would.
  let mut round = None;
  let mut ctx_hb_count = 0usize;
  while let Some(out) = ep.poll_message() {
    if let Message::Heartbeat(hb) = out.message()
      && !hb.context().is_empty()
    {
      round = Some(bytes::Bytes::copy_from_slice(hb.context()));
      ctx_hb_count += 1;
    }
  }
  assert_eq!(
    ctx_hb_count, 2,
    "leader must broadcast 2 read heartbeats (to peers 2 and 3)"
  );
  let round = round.expect("a read heartbeat round token");

  // No ReadState yet (need quorum = 2/3 voters, leader already counted itself = 1).
  assert!(
    ep.poll_event().is_none(),
    "ReadState must not be emitted before a quorum of heartbeat acks"
  );

  // One HeartbeatResponse echoing the round token from peer 2 → quorum reached (self + peer2 = 2/3).
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::HeartbeatResponse(HeartbeatResponse::new(Term::new(1), 2u64, round.clone())),
  );
  while ep.poll_message().is_some() {}

  // ReadState must be emitted now.
  let events: Vec<_> = core::iter::from_fn(|| ep.poll_event()).collect();
  assert_eq!(
    events.len(),
    1,
    "exactly one ReadState must be emitted after quorum; got: {:?}",
    events
  );
  let rs = match &events[0] {
    Event::ReadState(rs) => rs.clone(),
    other => panic!("expected ReadState, got {:?}", other),
  };
  assert_eq!(rs.context().as_ref(), ctx.as_ref(), "context must match");
  assert_eq!(
    rs.index(),
    Index::new(1),
    "index must be the commit at receipt"
  );
}

/// Regression (the ReadIndex quorum proof keys on an INTERNAL round token, never the
/// reusable user context): after a read with context X completes, the application may reuse X for a
/// new read. A stale/duplicated `HeartbeatResponse` from the FIRST read's round must NOT confirm the
/// SECOND read. Each read's heartbeat round carries a unique internal token, so the stale ack
/// (echoing the first token) finds no pending entry for the second read and is ignored; only a fresh
/// ack echoing the second round's token confirms it. Without this, a delayed duplicate could confirm
/// the reused read with no fresh quorum — a linearizability break if the leader has since lost quorum.
///
/// MUTATION: stop incrementing `next_round` in `ReadOnly::add_request` (all reads share token 0) →
/// `token1 == token2` and the stale read-#1 ack confirms read #2.
#[test]
fn reused_read_context_is_not_confirmed_by_stale_heartbeat_ack() {
  let (mut ep, mut log, mut stable, d) = make_leader_with_current_term_commit();
  let ctx = bytes::Bytes::from_static(b"reused_ctx");

  // Helper: drain the leader's outgoing messages, returning the round token its read heartbeat
  // carries (the non-empty Heartbeat context).
  fn read_round(ep: &mut Endpoint<u64, CountSm>) -> bytes::Bytes {
    let mut token = None;
    while let Some(out) = ep.poll_message() {
      if let Message::Heartbeat(hb) = out.message()
        && !hb.context().is_empty()
      {
        token = Some(bytes::Bytes::copy_from_slice(hb.context()));
      }
    }
    token.expect("a read heartbeat round token")
  }
  fn read_states(ep: &mut Endpoint<u64, CountSm>) -> Vec<ReadState> {
    core::iter::from_fn(|| ep.poll_event())
      .filter_map(|e| match e {
        Event::ReadState(rs) => Some(rs),
        _ => None,
      })
      .collect()
  }

  // Read #1: register, capture its round token, confirm via a quorum ack.
  ep.read_index(d, &log, &stable, ctx.clone())
    .expect("read #1 accepted");
  let token1 = read_round(&mut ep);
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::HeartbeatResponse(HeartbeatResponse::new(Term::new(1), 2u64, token1.clone())),
  );
  while ep.poll_message().is_some() {}
  assert_eq!(read_states(&mut ep).len(), 1, "read #1 must confirm");

  // Read #2: REUSE the same context (allowed now that #1 completed).
  ep.read_index(d, &log, &stable, ctx.clone())
    .expect("read #2 (reused context) accepted after #1 completed");
  let token2 = read_round(&mut ep);
  assert_ne!(
    token1, token2,
    "the reused context must get a fresh internal round token"
  );

  // The STALE HeartbeatResponse from read #1's round arrives (delayed/duplicated).
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::HeartbeatResponse(HeartbeatResponse::new(Term::new(1), 2u64, token1.clone())),
  );
  while ep.poll_message().is_some() {}
  assert!(
    read_states(&mut ep).is_empty(),
    "a stale ack echoing read #1's token must NOT confirm the reused read #2 (no fresh quorum)"
  );

  // A FRESH ack echoing read #2's token confirms it.
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::HeartbeatResponse(HeartbeatResponse::new(Term::new(1), 2u64, token2.clone())),
  );
  while ep.poll_message().is_some() {}
  let confirmed2 = read_states(&mut ep);
  assert_eq!(
    confirmed2.len(),
    1,
    "read #2 confirms only after a FRESH quorum ack echoing its own round token"
  );
  assert_eq!(confirmed2[0].context().as_ref(), ctx.as_ref());
}

/// Test 2: Stale leader (partitioned from quorum) cannot confirm a read.
///
/// The leader calls `read_index` but only gets heartbeat acks from itself (no quorum)
/// → no `ReadState` is emitted.
#[test]
fn stale_leader_cannot_confirm_read() {
  let (mut ep, log, stable, d) = make_leader_with_current_term_commit();
  let ctx = bytes::Bytes::from_static(b"stale_read");

  ep.read_index(d, &log, &stable, ctx.clone())
    .expect("leader must accept the read (it just cannot confirm without a quorum)");
  while ep.poll_message().is_some() {}
  // No heartbeat acks arrive (partitioned).
  // No ReadState must be emitted.
  let events: Vec<_> = core::iter::from_fn(|| ep.poll_event()).collect();
  assert!(
    events.is_empty(),
    "stale/partitioned leader must not emit ReadState without a heartbeat quorum"
  );
}

/// Test 4: Follower-forwarded read.
///
/// A follower calls `read_index(ctx)` → sends `ReadIndex` to the leader.
/// The leader confirms (heartbeat quorum) and replies `ReadIndexResponse`.
/// The follower emits `Event::ReadState`.
#[test]
fn follower_forwarded_read() {
  use core::time::Duration;

  // Set up a follower (node 2) pointing to leader 1.
  let follower_cfg = Config::try_new(
    2u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut follower = Endpoint::new(follower_cfg, Instant::ORIGIN, 7, CountSm::default());
  let mut follower_log = VecLog::default();
  let mut follower_stable = NoopStable::default();

  // Give the follower a heartbeat so it knows about leader 1.
  follower.handle_message(
    Instant::ORIGIN,
    &mut follower_log,
    &mut follower_stable,
    1u64,
    Message::Heartbeat(Heartbeat::new(
      Term::new(1),
      1u64,
      Index::ZERO,
      bytes::Bytes::new(),
    )),
  );
  while follower.poll_message().is_some() {}
  while follower.poll_event().is_some() {}

  // Follower calls read_index: should forward ReadIndex to leader 1.
  let ctx = bytes::Bytes::from_static(b"fwd_read");
  follower
    .read_index(
      Instant::ORIGIN,
      &follower_log,
      &follower_stable,
      ctx.clone(),
    )
    .expect("follower with a known leader must forward the read");

  let msg = follower
    .poll_message()
    .expect("follower must send ReadIndex to leader");
  assert_eq!(msg.to(), 1u64);
  // The forwarded ReadIndex carries the follower's INTERNAL token, not the user ctx. Capture it to
  // echo back, exactly as the leader would.
  let token = match msg.message() {
    Message::ReadIndex(ri) => bytes::Bytes::copy_from_slice(ri.context()),
    other => panic!("expected ReadIndex, got {other:?}"),
  };

  // Now simulate the leader confirming and replying with ReadIndexResponse echoing the token.
  follower.handle_message(
    Instant::ORIGIN,
    &mut follower_log,
    &mut follower_stable,
    1u64,
    Message::ReadIndexResponse(ReadIndexResponse::new(
      Term::new(1),
      1u64,
      Index::new(5),
      token.clone(),
      false,
    )),
  );

  // Follower must emit ReadState.
  let ev = follower
    .poll_event()
    .expect("follower must emit ReadState on ReadIndexResponse");
  assert!(ev.is_read_state());
  let rs = ev.unwrap_read_state_ref();
  assert_eq!(rs.index(), Index::new(5));
  assert_eq!(rs.context().as_ref(), ctx.as_ref());
}

/// Regression (the FOLLOWER-FORWARDED read correlator must be an internal token, not the
/// reusable user context): the follower-side mirror of the leader-side guard. After a forwarded read with context X
/// completes, the app may reuse X. A delayed/duplicated `ReadIndexResponse` from the FIRST forward must
/// NOT complete the SECOND forwarded read. Each forward carries a unique internal token; the stale
/// response echoes the first token, which `forwarded_reads.remove_by_token` no longer holds, so it is
/// dropped. Only the fresh response for the second forward's token completes it (at the fresh index).
///
/// MUTATION: freeze the follower's forward-token counter (`push` reuses token 0) → the stale response
/// completes the reused read at the STALE index.
#[test]
fn reused_forwarded_read_context_is_not_completed_by_stale_response() {
  use core::time::Duration;
  let cfg = Config::try_new(
    2u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 7, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = NoopStable::default();

  // Learn leader 1 via a heartbeat.
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::Heartbeat(Heartbeat::new(
      Term::new(1),
      1u64,
      Index::ZERO,
      bytes::Bytes::new(),
    )),
  );
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  let ctx = bytes::Bytes::from_static(b"reused_fwd");

  // Forward a read for `ctx` and return the internal token it carried over the wire.
  fn forward(
    ep: &mut Endpoint<u64, CountSm>,
    log: &VecLog,
    stable: &NoopStable,
    ctx: bytes::Bytes,
  ) -> bytes::Bytes {
    ep.read_index(Instant::ORIGIN, log, stable, ctx)
      .expect("forward accepted");
    let mut tok = None;
    while let Some(o) = ep.poll_message() {
      if let Message::ReadIndex(ri) = o.message() {
        tok = Some(bytes::Bytes::copy_from_slice(ri.context()));
      }
    }
    tok.expect("a forwarded ReadIndex")
  }
  fn read_states(ep: &mut Endpoint<u64, CountSm>) -> Vec<ReadState> {
    core::iter::from_fn(|| ep.poll_event())
      .filter_map(|e| match e {
        Event::ReadState(rs) => Some(rs),
        _ => None,
      })
      .collect()
  }

  // Read #1: forward, capture token, complete via the leader's response.
  let token1 = forward(&mut ep, &log, &stable, ctx.clone());
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::ReadIndexResponse(ReadIndexResponse::new(
      Term::new(1),
      1u64,
      Index::new(5),
      token1.clone(),
      false,
    )),
  );
  let s1 = read_states(&mut ep);
  assert_eq!(s1.len(), 1, "read #1 completes");
  assert_eq!(s1[0].index(), Index::new(5));
  assert_eq!(s1[0].context().as_ref(), ctx.as_ref());

  // Read #2: REUSE the context (allowed now that #1 completed).
  let token2 = forward(&mut ep, &log, &stable, ctx.clone());
  assert_ne!(
    token1, token2,
    "the reused forwarded context must get a fresh internal token"
  );

  // A STALE duplicate ReadIndexResponse echoing read #1's token arrives.
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::ReadIndexResponse(ReadIndexResponse::new(
      Term::new(1),
      1u64,
      Index::new(5),
      token1.clone(),
      false,
    )),
  );
  assert!(
    read_states(&mut ep).is_empty(),
    "a stale response echoing read #1's token must NOT complete the reused read #2"
  );

  // The fresh response for read #2's token completes it at the FRESH index.
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::ReadIndexResponse(ReadIndexResponse::new(
      Term::new(1),
      1u64,
      Index::new(8),
      token2.clone(),
      false,
    )),
  );
  let s2 = read_states(&mut ep);
  assert_eq!(
    s2.len(),
    1,
    "read #2 completes only on its own fresh response"
  );
  assert_eq!(
    s2[0].index(),
    Index::new(8),
    "at the FRESH index, not the stale 5"
  );
  assert_eq!(s2[0].context().as_ref(), ctx.as_ref());
}

/// Regression (forwarded-read tokens are unique ACROSS restarts via the durable boot epoch):
/// a follower forwards a read (boot epoch E), crashes, and restarts with a strictly-higher boot epoch.
/// A delayed pre-crash `ReadIndexResponse` (carrying the epoch-E token) must NOT complete a post-restart
/// forwarded read (whose token carries the higher epoch), even at the same term — otherwise it would
/// complete the new read at a stale index (a linearizability break under a transport that redelivers
/// pre-crash messages across a restart).
///
/// MUTATION: drop the `boot_epoch` prefix from the token (`push` uses only the counter) → both
/// incarnations' first tokens are identical and the pre-crash response completes the post-restart read.
#[test]
fn forwarded_read_token_is_unique_across_restart() {
  use crate::{Config, Index, Instant, Message, ReadIndexResponse, Term};
  use core::time::Duration;
  let cfg = Config::try_new(
    2u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut log = VecLog::default();
  let mut stable = NoopStable::default();
  stable.force_state(Term::new(1), None, Index::ZERO); // restart at term 1, no leader yet

  // Restart at `boot_epoch`, learn leader 1 (term 1) via a heartbeat, forward `ctx`, return the token.
  fn restart_and_forward(
    cfg: Config<u64>,
    log: &mut VecLog,
    stable: &mut NoopStable,
    boot_epoch: u64,
    ctx: bytes::Bytes,
  ) -> (Endpoint<u64, CountSm>, bytes::Bytes) {
    let mut ep = Endpoint::restart(
      cfg,
      Instant::ORIGIN,
      7,
      CountSm::default(),
      boot_epoch,
      log,
      stable,
    );
    ep.handle_message(
      Instant::ORIGIN,
      log,
      stable,
      1u64,
      Message::Heartbeat(Heartbeat::new(
        Term::new(1),
        1u64,
        Index::ZERO,
        bytes::Bytes::new(),
      )),
    );
    while ep.poll_message().is_some() {}
    ep.read_index(Instant::ORIGIN, log, stable, ctx)
      .expect("forward accepted");
    let mut tok = None;
    while let Some(o) = ep.poll_message() {
      if let Message::ReadIndex(ri) = o.message() {
        tok = Some(bytes::Bytes::copy_from_slice(ri.context()));
      }
    }
    (ep, tok.expect("forwarded a ReadIndex"))
  }
  fn read_states(ep: &mut Endpoint<u64, CountSm>) -> Vec<ReadState> {
    core::iter::from_fn(|| ep.poll_event())
      .filter_map(|e| match e {
        Event::ReadState(rs) => Some(rs),
        _ => None,
      })
      .collect()
  }

  let ctx = bytes::Bytes::from_static(b"read_x");
  // Incarnation A (boot epoch 1): forward, capture the token (the "pre-crash" one).
  let (_ep_a, token_a) = restart_and_forward(cfg.clone(), &mut log, &mut stable, 1, ctx.clone());
  // Incarnation B (boot epoch 2): restart from the SAME durable stores, forward, capture the token.
  let (mut ep_b, token_b) = restart_and_forward(cfg.clone(), &mut log, &mut stable, 2, ctx.clone());
  assert_ne!(
    token_a, token_b,
    "tokens from different boot epochs must differ"
  );

  // The DELAYED pre-crash ReadIndexResponse (epoch-1 token) must NOT complete B's post-restart read.
  ep_b.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::ReadIndexResponse(ReadIndexResponse::new(
      Term::new(1),
      1u64,
      Index::new(5),
      token_a.clone(),
      false,
    )),
  );
  assert!(
    read_states(&mut ep_b).is_empty(),
    "a pre-crash (lower boot-epoch) token must not complete a post-restart read"
  );

  // The fresh response (B's own epoch-2 token) completes B's read at the fresh index.
  ep_b.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::ReadIndexResponse(ReadIndexResponse::new(
      Term::new(1),
      1u64,
      Index::new(8),
      token_b.clone(),
      false,
    )),
  );
  let s = read_states(&mut ep_b);
  assert_eq!(s.len(), 1, "B's read completes on its own fresh token");
  assert_eq!(s[0].index(), Index::new(8));
  assert_eq!(s[0].context().as_ref(), ctx.as_ref());
}

/// Test 5: FIFO confirmation + index correctness.
///
/// Two reads in order confirm in order; each ReadState.index is the commit recorded
/// at that read's receipt (never less than a prior read's index).
#[test]
fn fifo_confirmation_and_index_correctness() {
  let (mut ep, mut log, mut stable, d) = make_leader_with_current_term_commit();

  let ctx_a = bytes::Bytes::from_static(b"read_a");
  let ctx_b = bytes::Bytes::from_static(b"read_b");

  // Both reads are at commit=1 (nothing new committed between them).
  ep.read_index(d, &log, &stable, ctx_a.clone())
    .expect("first read (ctx_a) must be accepted");
  ep.read_index(d, &log, &stable, ctx_b.clone())
    .expect("second read (ctx_b, distinct context) must be accepted");
  // Capture the LAST read heartbeat's round token (read_b's) — acking it advances through both
  // reads (FIFO). With internal round tokens the heartbeat carries the token, not the user ctx.
  let mut last_round = None;
  while let Some(out) = ep.poll_message() {
    if let Message::Heartbeat(hb) = out.message()
      && !hb.context().is_empty()
    {
      last_round = Some(bytes::Bytes::copy_from_slice(hb.context()));
    }
  }
  let last_round = last_round.expect("two read heartbeat rounds were broadcast");

  // Peer 2 acks the last round token → advance through both read_a and read_b (FIFO).
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::HeartbeatResponse(HeartbeatResponse::new(
      Term::new(1),
      2u64,
      last_round.clone(),
    )),
  );
  while ep.poll_message().is_some() {}

  // Both reads should now be confirmed.
  let events: Vec<_> = core::iter::from_fn(|| ep.poll_event()).collect();
  let read_states: Vec<_> = events
    .iter()
    .filter_map(|e| {
      if let Event::ReadState(rs) = e {
        Some(rs.clone())
      } else {
        None
      }
    })
    .collect();

  assert_eq!(
    read_states.len(),
    2,
    "both reads must be confirmed; got {} ReadStates",
    read_states.len()
  );
  // FIFO: ctx_a before ctx_b.
  assert_eq!(
    read_states[0].context().as_ref(),
    ctx_a.as_ref(),
    "first confirmed must be ctx_a"
  );
  assert_eq!(
    read_states[1].context().as_ref(),
    ctx_b.as_ref(),
    "second confirmed must be ctx_b"
  );
  // Index correctness: both are at commit=1.
  assert_eq!(read_states[0].index(), Index::new(1));
  assert_eq!(read_states[1].index(), Index::new(1));
}

/// Test 6: No-current-term-commit defers.
///
/// A freshly-elected leader whose no-op hasn't committed yet defers a read until
/// the no-op commits, then confirms it.
#[test]
fn no_current_term_commit_defers_read() {
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = NoopStable::default();

  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable);
  ep.handle_storage(d, &mut log, &mut stable);
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResponse(crate::VoteResponse::new(Term::new(1), 2u64, false, false)),
  );
  assert!(ep.role().is_leader());
  // Do NOT drain storage or advance commit yet.
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  // read_index called before the no-op is committed → must be DEFERRED.
  let ctx = bytes::Bytes::from_static(b"deferred_read");
  ep.read_index(d, &log, &stable, ctx.clone())
    .expect("leader must accept the read (deferred until the no-op commits)");

  // No read heartbeats (non-empty context) should have been sent (the read is deferred).
  let mut read_hb_before = false;
  while let Some(out) = ep.poll_message() {
    if let Message::Heartbeat(hb) = out.message()
      && !hb.context().is_empty()
    {
      read_hb_before = true;
    }
  }
  assert!(
    !read_hb_before,
    "deferred read must NOT broadcast a heartbeat round before no-op commits"
  );

  // No ReadState yet.
  assert!(
    ep.poll_event().is_none(),
    "deferred read must NOT emit ReadState before no-op commits"
  );

  // Now drain storage → no-op LeaderAppend fires → self match advances.
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_event().is_some() {} // drain LeaderChanged etc.

  // Peer 2 acks the no-op → commit=1 in current term → deferred read gets flushed.
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendResponse(crate::AppendResponse::new(
      Term::new(1),
      2u64,
      false,
      Index::ZERO,
      Term::ZERO,
      Index::new(1),
    )),
  );
  while ep.poll_event().is_some() {} // drain Applied for no-op (it's Empty, so none)

  // The deferred read should now have been flushed → leader broadcasts a read heartbeat carrying
  // the internal round token. Capture it to echo back.
  let mut round = None;
  while let Some(out) = ep.poll_message() {
    if let Message::Heartbeat(hb) = out.message()
      && !hb.context().is_empty()
    {
      round = Some(bytes::Bytes::copy_from_slice(hb.context()));
    }
  }
  let round = round.expect("deferred read must broadcast heartbeats after no-op commits");

  // Peer 2 acks the round token → quorum → ReadState emitted.
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::HeartbeatResponse(HeartbeatResponse::new(Term::new(1), 2u64, round.clone())),
  );
  while ep.poll_message().is_some() {}

  let events: Vec<_> = core::iter::from_fn(|| ep.poll_event()).collect();
  let read_states: Vec<_> = events
    .iter()
    .filter_map(|e| {
      if let Event::ReadState(rs) = e {
        Some(rs.clone())
      } else {
        None
      }
    })
    .collect();
  assert_eq!(
    read_states.len(),
    1,
    "exactly one ReadState must be emitted after deferred read is confirmed"
  );
  assert_eq!(
    read_states[0].index(),
    Index::new(1),
    "index must be commit at receipt"
  );
  assert_eq!(read_states[0].context().as_ref(), ctx.as_ref());
}

/// A fatal `LogStore::term` failure at a COMMITTED index during an AppendEntries conflict scan
/// must POISON the node (`PoisonReason::LogTerm`) — never silently fabricate a default term and
/// truncate committed state. Regression for the swallowed-`term`-error defect class.
#[test]
fn term_read_failure_at_committed_index_poisons_no_truncation() {
  use crate::{
    AppendEntries, Config, Entry, EntryKind, Index, Instant, Message, PoisonReason, Term,
  };
  use core::time::Duration;
  let cfg = Config::try_new(
    2u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut stable = NoopStable::default();

  // Follower holds two durable, COMMITTED entries at indices 1 and 2 (both term 1). Use Empty
  // entries so commit-and-apply needs no payload decode — this test isolates the term-read path.
  let mut log = crate::testkit::FailTermLog::default();
  log.force_append(&[
    Entry::new(
      Term::new(1),
      Index::new(1),
      EntryKind::Empty,
      bytes::Bytes::new(),
    ),
    Entry::new(
      Term::new(1),
      Index::new(2),
      EntryKind::Empty,
      bytes::Bytes::new(),
    ),
  ]);

  // Drive commit up to index 2 with a benign heartbeat-shaped AppendEntries (prev at the
  // matching tail, no new entries): the consistency check reads term(2)=ok and commit advances.
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(1),
      1u64,
      Index::new(2),
      Term::new(1),
      std::vec![],
      Index::new(2),
    )),
  );
  assert_eq!(ep.commit_index(), Index::new(2), "commit advanced to 2");
  assert!(!ep.is_poisoned(), "healthy after the setup append");
  while ep.poll_message().is_some() {}

  // Now arm a FATAL term-read failure at the committed index 2, and send a conflicting
  // AppendEntries whose suffix overlaps index 2 with a DIFFERENT term. prev_log_index=1 passes
  // the consistency check (term(1) ok); the conflict scan then reads term(2) → Err → poison.
  log.fail_term_at(Some(Index::new(2)));
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(2),
      1u64,
      Index::new(1),
      Term::new(1),
      std::vec![Entry::new(
        Term::new(2),
        Index::new(2),
        EntryKind::Empty,
        bytes::Bytes::new(),
      )],
      Index::new(1),
    )),
  );

  assert!(ep.is_poisoned(), "fatal term-read must poison the node");
  assert_eq!(
    ep.poison_reason(),
    Some(PoisonReason::LogTerm),
    "the swallowed term error must surface as LogTerm, not a fabricated default"
  );
  // NO truncation/append happened: the durable tail is still indices 1..=2 with the ORIGINAL
  // terms (the conflicting suffix at index 2 was never submitted).
  log.fail_term_at(None);
  assert_eq!(log.last_index(), Index::new(2), "no truncation occurred");
  assert_eq!(
    log.term(Index::new(2)),
    Ok(Term::new(1)),
    "the committed entry's term is untouched (no overwrite to term 2)"
  );
}

/// A FOLLOWER forwarding a `ReadIndex` to its leader applies the same duplicate-context guard as
/// the leader: a second `read_index` with an in-flight context returns `DuplicateContext`, and the
/// matching `ReadIndexResponse` clears it so the context can be re-issued. Regression (Class 2).
#[test]
fn duplicate_follower_read_index_is_rejected_then_clears() {
  use crate::{
    AppendEntries, Config, Index, Instant, Message, ReadIndexError, ReadIndexResponse, Term,
  };
  use core::time::Duration;
  let cfg = Config::try_new(
    2u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = NoopStable::default();

  // Establish a known leader (node 1) via a heartbeat-shaped AppendEntries.
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(1),
      1u64,
      Index::ZERO,
      Term::ZERO,
      std::vec![],
      Index::ZERO,
    )),
  );
  assert_eq!(ep.role(), Role::Follower);
  assert_eq!(ep.leader(), Some(1u64));
  while ep.poll_message().is_some() {}

  let ctx = bytes::Bytes::from_static(b"read-ctx");

  // First read forwards to the leader. The forward carries the follower's INTERNAL token (not the
  // user ctx); capture it to echo back in the ReadIndexResponse.
  assert_eq!(
    ep.read_index(Instant::ORIGIN, &log, &stable, ctx.clone()),
    Ok(())
  );
  let token = match ep.poll_message().map(|o| o.message().clone()) {
    Some(Message::ReadIndex(ri)) => bytes::Bytes::copy_from_slice(ri.context()),
    other => panic!("first read must forward as a ReadIndex, got {other:?}"),
  };

  // Second read with the SAME user context — rejected by the follower's dedup, no second forward.
  assert_eq!(
    ep.read_index(Instant::ORIGIN, &log, &stable, ctx.clone()),
    Err(ReadIndexError::DuplicateContext),
    "duplicate forwarded context is rejected"
  );
  assert!(ep.poll_message().is_none(), "no duplicate forward emitted");

  // The matching ReadIndexResponse (echoing the token) confirms the read and clears the in-flight context.
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::ReadIndexResponse(ReadIndexResponse::new(
      Term::new(1),
      1u64,
      Index::ZERO,
      token.clone(),
      false,
    )),
  );
  // Drain the ReadState event.
  while ep.poll_event().is_some() {}

  // Re-issuing the same context now succeeds (the guard cleared).
  assert_eq!(
    ep.read_index(Instant::ORIGIN, &log, &stable, ctx.clone()),
    Ok(()),
    "context is re-issuable after its ReadIndexResponse"
  );
}

/// When the leader is at `MAX_LEADER_READS`, a forwarded `ReadIndex` is DECLINED with a
/// rejecting `ReadIndexResponse` (not silently dropped), so the forwarding follower can clear its
/// `forwarded_reads` strand and re-issue the read.
///
/// FAILS-ON-OLD: with the bare `return` at capacity the leader sends nothing; the follower never
/// learns and its `forwarded_reads` entry is stranded (the context stays a `DuplicateContext`).
#[test]
fn leader_at_capacity_rejects_forwarded_read_and_follower_clears_strand() {
  use crate::{Index, ReadIndex, ReadIndexError};

  // Leader half: at capacity, a forwarded ReadIndex yields a rejecting ReadIndexResponse to ri.from.
  let (mut leader, mut llog, mut lstable, lnow) = make_leader_with_current_term_commit();
  // Saturate the leader's read backlog so `leader_reads_at_capacity()` holds.
  for i in 0..MAX_LEADER_READS {
    leader.reads.pending_reads.push((
      bytes::Bytes::copy_from_slice(&(i as u64).to_le_bytes()),
      None,
    ));
  }
  assert!(leader.leader_reads_at_capacity());
  while leader.poll_message().is_some() {}

  let fwd_ctx = bytes::Bytes::from_static(b"forwarded-at-capacity");
  let leader_term = leader.term();
  leader.handle_message(
    lnow,
    &mut llog,
    &mut lstable,
    2u64,
    Message::ReadIndex(ReadIndex::new(leader_term, 2u64, fwd_ctx.clone())),
  );
  // Exactly one rejecting ReadIndexResponse addressed back to the forwarder (node 2).
  let mut reject_response = None;
  while let Some(out) = leader.poll_message() {
    if out.to() == 2u64
      && let Message::ReadIndexResponse(r) = out.message()
    {
      reject_response = Some(r.clone());
    }
  }
  let reject_response =
    reject_response.expect("leader at capacity must reply with a ReadIndexResponse");
  assert!(
    reject_response.reject(),
    "the at-capacity reply must carry reject=true"
  );
  assert_eq!(
    reject_response.context(),
    fwd_ctx.as_ref(),
    "the rejecting reply echoes the forwarded context"
  );

  // Follower half: receiving a rejecting ReadIndexResponse clears the strand (no ReadState, and the
  // context becomes re-issuable rather than a stuck DuplicateContext).
  use crate::{AppendEntries, Config, Term};
  use core::time::Duration;
  let cfg = Config::try_new(
    2u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut follower = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut flog = VecLog::default();
  let mut fstable = NoopStable::default();
  follower.handle_message(
    Instant::ORIGIN,
    &mut flog,
    &mut fstable,
    1u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(1),
      1u64,
      Index::ZERO,
      Term::ZERO,
      std::vec![],
      Index::ZERO,
    )),
  );
  assert_eq!(follower.leader(), Some(1u64));
  while follower.poll_message().is_some() {}

  let ctx = bytes::Bytes::from_static(b"strand-ctx");
  assert_eq!(
    follower.read_index(Instant::ORIGIN, &flog, &fstable, ctx.clone()),
    Ok(()),
    "the read forwards and records a forwarded_reads strand"
  );
  // Capture the follower's internal token from the forward to echo in the rejecting response.
  let strand_token = match follower.poll_message().map(|o| o.message().clone()) {
    Some(Message::ReadIndex(ri)) => bytes::Bytes::copy_from_slice(ri.context()),
    other => panic!("the read must forward as a ReadIndex, got {other:?}"),
  };
  while follower.poll_message().is_some() {}
  // A re-issue right now is a duplicate (strand still held).
  assert_eq!(
    follower.read_index(Instant::ORIGIN, &flog, &fstable, ctx.clone()),
    Err(ReadIndexError::DuplicateContext),
    "while the strand is held the context is a duplicate"
  );

  // The rejecting ReadIndexResponse from the leader (node 1), echoing the token, clears the strand and
  // emits NO ReadState.
  follower.handle_message(
    Instant::ORIGIN,
    &mut flog,
    &mut fstable,
    1u64,
    Message::ReadIndexResponse(ReadIndexResponse::new(
      Term::new(1),
      1u64,
      Index::ZERO,
      strand_token.clone(),
      true,
    )),
  );
  assert!(
    !follower.poll_all_events_any_read_state(),
    "a rejecting ReadIndexResponse must NOT emit a ReadState"
  );
  // Strand cleared: the SAME context is now accepted again (forwards anew), not DuplicateContext.
  assert_eq!(
    follower.read_index(Instant::ORIGIN, &flog, &fstable, ctx.clone()),
    Ok(()),
    "after a rejecting ReadIndexResponse the context is re-issuable, not stranded"
  );
}

/// A poisoned node's `read_index` returns `Err(Poisoned)` BEFORE any side effect. A poisoned
/// node suppresses `poll_event`, so a `ReadState` would never arrive; returning `Ok(())` would
/// strand the caller waiting on a confirmation that can never come.
///
/// FAILS-ON-OLD: the old `if self.poisoned { return Ok(()) }` short-circuit returns `Ok`.
#[test]
fn poisoned_read_index_reports_poisoned_not_ok() {
  use crate::{PoisonReason, ReadIndexError};
  let (mut leader, log, stable, now) = make_leader_with_current_term_commit();
  leader.poison(PoisonReason::LogTerm);
  assert!(leader.is_poisoned());
  let before = leader.poll_event().is_some();
  assert!(!before, "no pending event before the poisoned read");
  assert_eq!(
    leader.read_index(now, &log, &stable, bytes::Bytes::from_static(b"ctx")),
    Err(ReadIndexError::Poisoned),
    "a poisoned node must reject the read, not falsely accept it"
  );
  // And no ReadState is queued (the poisoned node emits nothing).
  assert!(
    !leader.poll_all_events_any_read_state(),
    "a poisoned read_index must not queue a ReadState"
  );
}

/// A follower completes a forwarded read ONLY for a `ReadIndexResponse` it actually awaits, from
/// its CURRENT leader. An unsolicited / wrong-leader response emits NO `ReadState`; the legitimate response
/// emits exactly one; a delayed duplicate (after the context cleared) emits nothing.
///
/// FAILS-ON-OLD: `on_read_index_response` removed-and-emitted unconditionally, so a spoofed or
/// duplicate response would surface a `ReadState` the application would treat as linearizable.
#[test]
fn read_index_response_validation_rejects_unsolicited_and_duplicate() {
  use crate::{AppendEntries, Config, Index, Instant, Message, ReadIndexResponse, Term};
  use core::time::Duration;
  let cfg = Config::try_new(
    2u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = NoopStable::default();

  // Establish leader = node 1 and forward a read with context "ctx".
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(1),
      1u64,
      Index::ZERO,
      Term::ZERO,
      std::vec![],
      Index::ZERO,
    )),
  );
  assert_eq!(ep.leader(), Some(1u64));
  while ep.poll_message().is_some() {}
  let ctx = bytes::Bytes::from_static(b"ctx");
  assert_eq!(
    ep.read_index(Instant::ORIGIN, &log, &stable, ctx.clone()),
    Ok(())
  );
  // The forward carries the follower's INTERNAL token; capture it (the correlator the response echoes).
  let token = match ep.poll_message().map(|o| o.message().clone()) {
    Some(Message::ReadIndex(ri)) => bytes::Bytes::copy_from_slice(ri.context()),
    other => panic!("the read must forward as a ReadIndex, got {other:?}"),
  };
  while ep.poll_message().is_some() {}

  // (a) Unsolicited token (never forwarded): no ReadState.
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::ReadIndexResponse(ReadIndexResponse::new(
      Term::new(1),
      1u64,
      Index::new(5),
      bytes::Bytes::from_static(b"never-forwarded"),
      false,
    )),
  );
  assert!(
    !ep.poll_all_events_any_read_state(),
    "an unsolicited token must not complete a read"
  );

  // (b) Right token but WRONG leader (from node 3, not our leader node 1): no ReadState, and the
  // in-flight read must remain (so the legitimate response can still complete it below).
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    3u64,
    Message::ReadIndexResponse(ReadIndexResponse::new(
      Term::new(1),
      3u64,
      Index::new(5),
      token.clone(),
      false,
    )),
  );
  assert!(
    !ep.poll_all_events_any_read_state(),
    "a wrong-leader response must not complete the read"
  );

  // (c) The legitimate response from the current leader (echoing the token): exactly one ReadState.
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::ReadIndexResponse(ReadIndexResponse::new(
      Term::new(1),
      1u64,
      Index::new(7),
      token.clone(),
      false,
    )),
  );
  let read_states: Vec<_> = {
    let mut v = Vec::new();
    while let Some(e) = ep.poll_event() {
      if let Event::ReadState(rs) = e {
        v.push(rs);
      }
    }
    v
  };
  assert_eq!(
    read_states.len(),
    1,
    "the legitimate response completes the read exactly once"
  );
  assert_eq!(read_states[0].index(), Index::new(7));

  // (d) A delayed DUPLICATE echoing the same token (already completed/cleared): no second ReadState.
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::ReadIndexResponse(ReadIndexResponse::new(
      Term::new(1),
      1u64,
      Index::new(7),
      token.clone(),
      false,
    )),
  );
  assert!(
    !ep.poll_all_events_any_read_state(),
    "a delayed duplicate response must not re-complete the read"
  );
}

/// A follower whose forwarded reads are never answered (request/response dropped) while the
/// leader stays stable must NOT grow `forwarded_reads` without bound: each new distinct context is
/// FIFO-bounded at `MAX_FORWARDED_READS`.
///
/// FAILS-ON-OLD: the unbounded `BTreeSet` grew one entry per dropped read.
#[test]
fn forwarded_reads_is_bounded() {
  use crate::{AppendEntries, Config, Index, Instant, Message, Term};
  use core::time::Duration;
  let cfg = Config::try_new(
    2u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = NoopStable::default();

  // Establish a stable leader (node 1) so every read FORWARDS (and is then "dropped" — we never
  // deliver a ReadIndexResponse).
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(1),
      1u64,
      Index::ZERO,
      Term::ZERO,
      std::vec![],
      Index::ZERO,
    )),
  );
  while ep.poll_message().is_some() {}

  // Forward far more distinct contexts than the cap, never answering any. The first
  // MAX_FORWARDED_READS are accepted; beyond the cap the follower applies BACK-PRESSURE —
  // rejecting the new read with `TooManyInFlight` rather than evicting an already-accepted one — so
  // the in-flight set saturates exactly at the cap, never grows, and never strands a prior read.
  let total = MAX_FORWARDED_READS * 3 + 17;
  for i in 0..total {
    let ctx = bytes::Bytes::copy_from_slice(&(i as u64).to_be_bytes());
    let result = ep.read_index(Instant::ORIGIN, &log, &stable, ctx);
    if i < MAX_FORWARDED_READS {
      assert_eq!(
        result,
        Ok(()),
        "below the cap each distinct context forwards"
      );
    } else {
      assert_eq!(
        result,
        Err(ReadIndexError::TooManyInFlight),
        "at capacity the follower back-pressures instead of evicting"
      );
    }
    while ep.poll_message().is_some() {}
    assert!(
      ep.reads.forwarded_reads.len() <= MAX_FORWARDED_READS,
      "forwarded_reads must never exceed the cap"
    );
  }
  assert_eq!(
    ep.reads.forwarded_reads.len(),
    MAX_FORWARDED_READS,
    "the set saturates exactly at the cap"
  );
}

/// A forwarded read may be completed only by a `ReadIndexResponse` whose ENVELOPE sender (the transport
/// peer `from` passed to `handle_message`) is the follower's current leader — not merely one whose
/// PAYLOAD `from` claims to be the leader. A wrong peer can forge `response.from()` to the leader's id;
/// validating only the payload would let that spoofed response complete a read the application then
/// treats as linearizable.
///
/// FAILS-ON-OLD: if `on_read_index_response` checks only `self.leader != Some(response.from())` (ignoring
/// the envelope `from`), the spoofed message at step (a) completes the read and a ReadState leaks.
#[test]
fn read_index_response_requires_matching_envelope_sender() {
  use crate::{AppendEntries, Config, Index, Instant, Message, ReadIndexResponse, Term};
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = NoopStable::default();

  // Establish leader = node 2 (via an AppendEntries whose envelope sender is 2) and forward a read.
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(1),
      2u64,
      Index::ZERO,
      Term::ZERO,
      std::vec![],
      Index::ZERO,
    )),
  );
  assert_eq!(ep.leader(), Some(2u64));
  assert_eq!(ep.role(), Role::Follower);
  while ep.poll_message().is_some() {}

  let ctx = bytes::Bytes::from_static(b"read-ctx");
  assert_eq!(
    ep.read_index(Instant::ORIGIN, &log, &stable, ctx.clone()),
    Ok(()),
    "a follower with a known leader forwards the read"
  );
  // The forward is a ReadIndex to the leader (node 2), carrying the follower's internal token.
  let token = {
    let mut tok = None;
    while let Some(o) = ep.poll_message() {
      if let Message::ReadIndex(ri) = o.message() {
        assert_eq!(o.to(), 2u64, "the read forwards to the leader");
        tok = Some(bytes::Bytes::copy_from_slice(ri.context()));
      }
    }
    tok.expect("read_index must forward a ReadIndex to the leader")
  };

  // (a) SPOOFED: payload `from` claims to be the leader (2), but the ENVELOPE sender is node 3 (the
  // transport peer). Must be REJECTED — no ReadState — because the peer that actually delivered it
  // is not our leader, even though the payload lies about being from node 2.
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    3u64,
    Message::ReadIndexResponse(ReadIndexResponse::new(
      Term::new(1),
      2u64,
      Index::new(9),
      token.clone(),
      false,
    )),
  );
  assert!(
    !ep.poll_all_events_any_read_state(),
    "a response whose ENVELOPE sender is not the leader must not complete the read, \
       even if its payload `from` is forged to the leader's id"
  );

  // (b) LEGITIMATE: envelope sender == payload from == leader (2). The read completes: one ReadState
  // at the confirmed index. (The in-flight read survived step (a), proving (a) did not consume it.)
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    2u64,
    Message::ReadIndexResponse(ReadIndexResponse::new(
      Term::new(1),
      2u64,
      Index::new(9),
      token.clone(),
      false,
    )),
  );
  let read_states: Vec<_> = {
    let mut v = Vec::new();
    while let Some(e) = ep.poll_event() {
      if let Event::ReadState(rs) = e {
        v.push(rs);
      }
    }
    v
  };
  assert_eq!(
    read_states.len(),
    1,
    "the legitimately-addressed response completes the read exactly once"
  );
  assert_eq!(read_states[0].index(), Index::new(9));
}

/// Class C regression — leader read backlog is bounded.
///
/// A freshly-elected multi-node leader whose current-term no-op has NOT yet committed defers
/// each read into `pending_reads` (no heartbeat round). That backlog must be capped at
/// `MAX_LEADER_READS`: the first `MAX_LEADER_READS` distinct-context reads are accepted, and the
/// next one is rejected with `TooManyInFlight`. The backlog never exceeds the cap.
///
/// FAILS-ON-OLD: with the `if self.leader_reads_at_capacity() { return Err(..TooManyInFlight) }`
/// check removed from the `read_index` leader branch, the cap+1 read returns `Ok` and
/// `pending_reads` grows past `MAX_LEADER_READS`.
#[test]
fn leader_read_backlog_is_bounded() {
  use crate::{Config, Instant, Message, Term, VoteResponse};
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = NoopStable::default();

  // Elect node 1 leader, but do NOT drain storage / advance commit, so `has_current_term_commit`
  // is false and reads defer into `pending_reads`. Mirrors `no_current_term_commit_defers_read`.
  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable);
  ep.handle_storage(d, &mut log, &mut stable);
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResponse(VoteResponse::new(Term::new(1), 2u64, false, false)),
  );
  assert!(ep.role().is_leader());
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  // First read: deferred (no current-term commit) but accepted.
  ep.read_index(d, &log, &stable, bytes::Bytes::from_static(b"read-0"))
    .expect("a leader with no current-term commit must accept (defer) the first read");

  // Fill the rest of the cap with distinct contexts: indices 1..MAX_LEADER_READS are all Ok.
  for i in 1..MAX_LEADER_READS {
    let ctx = bytes::Bytes::from(std::format!("read-{i}"));
    ep.read_index(d, &log, &stable, ctx)
      .expect("reads up to the cap must be accepted");
    assert!(
      ep.reads.pending_reads.len() <= MAX_LEADER_READS,
      "the deferred backlog must never exceed MAX_LEADER_READS"
    );
  }
  assert_eq!(
    ep.reads.pending_reads.len(),
    MAX_LEADER_READS,
    "exactly MAX_LEADER_READS reads are now in the deferred backlog"
  );

  // One more distinct read: the cap is reached → TooManyInFlight, and the backlog does not grow.
  let overflow = bytes::Bytes::from_static(b"read-overflow");
  assert_eq!(
    ep.read_index(d, &log, &stable, overflow),
    Err(ReadIndexError::TooManyInFlight),
    "the read past the cap must be rejected with TooManyInFlight"
  );
  assert_eq!(
    ep.reads.pending_reads.len(),
    MAX_LEADER_READS,
    "the rejected read must not be added: the backlog stays at the cap"
  );
}

// A fatal LogStore::term read in read_index's current-term-commit gate must REJECT with Poisoned — not
// collapse the fatal error into the ordinary "no current-term commit yet" deferral, which would push a
// pending read that can never complete (a poisoned node emits no events) and report Ok.
#[test]
fn read_index_rejects_when_current_term_gate_term_read_poisons() {
  use crate::{
    AppendResponse, Config, Index, Instant, Message, PoisonReason, Term, VoteResponse,
    testkit::FailTermLog,
  };
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut log = FailTermLog::default();
  let mut stable = NoopStable::default();
  // Elect + commit the election no-op so a current-term anchor exists (term reads succeed: fault unarmed).
  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable);
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResponse(VoteResponse::new(Term::new(1), 2u64, false, false)),
  );
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(ep.role().is_leader());
  ep.handle_storage(d, &mut log, &mut stable);
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendResponse(AppendResponse::new(
      Term::new(1),
      2u64,
      false,
      Index::ZERO,
      Term::ZERO,
      Index::new(1),
    )),
  );
  while ep.poll_event().is_some() {}
  while ep.poll_message().is_some() {}
  assert!(
    ep.poison_reason().is_none(),
    "not poisoned before the fault"
  );

  // Arm the fatal term read at the commit index — has_current_term_commit reads log_term(commit).
  log.fail_term_at(Some(ep.commit_index()));
  let r = ep.read_index(d, &log, &stable, bytes::Bytes::from_static(b"r"));
  assert_eq!(
    r,
    Err(ReadIndexError::Poisoned),
    "a fatal term read in the current-term gate rejects the read, not defer-and-Ok"
  );
  assert_eq!(ep.poison_reason(), Some(PoisonReason::LogTerm));
  assert!(
    ep.reads.pending_reads.is_empty(),
    "no read deferred after the poison"
  );
  assert!(
    ep.poll_event().is_none(),
    "no ReadState event emitted after the poison"
  );
}

// A LeaseGuard read whose anchor `entries` read fails (fatal) poisons the node inside do_leader_read.
// The fail-stop must happen BEFORE the stale-lease branch sets lease_refresh_wanted or registers a Safe
// read — no read/refresh state mutated on a dead node (the egress is already guarded, so the discriminator
// is the internal state). Covers both the local read_index and the shared do_leader_read forwarded path.
#[test]
fn leaseguard_read_fail_stops_before_mutating_state_when_entries_poisons() {
  use crate::{
    AppendResponse, Config, Index, Instant, Message, PoisonReason, ReadOnlyOption, Term,
    VoteResponse, testkit::FailTermLog,
  };
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_read_only(ReadOnlyOption::LeaseGuard)
  .with_lease_duration(Duration::from_millis(300))
  .with_clock_drift_bound(Duration::from_millis(50));
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut log = FailTermLog::default();
  let mut stable = NoopStable::default();
  // Elect + commit the election no-op (a current-term LeaseGuard anchor); term/entries reads succeed here.
  let now = ep.poll_timeout().unwrap();
  ep.handle_timeout(now, &mut log, &mut stable);
  ep.handle_storage(now, &mut log, &mut stable);
  ep.handle_message(
    now,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResponse(VoteResponse::new(Term::new(1), 2u64, false, false)),
  );
  assert!(ep.role().is_leader());
  ep.handle_storage(now, &mut log, &mut stable);
  ep.handle_message(
    now,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendResponse(AppendResponse::new(
      Term::new(1),
      2u64,
      false,
      Index::ZERO,
      Term::ZERO,
      Index::new(1),
    )),
  );
  ep.handle_storage(now, &mut log, &mut stable);
  while ep.poll_event().is_some() {}
  while ep.poll_message().is_some() {}
  assert!(
    ep.poison_reason().is_none(),
    "not poisoned before the fault"
  );

  // Arm the fatal anchor `entries` read; lease_guard_read_live reads entries(commit) and poisons (LogRead).
  log.fail_entries_at(Some(ep.commit_index()));
  let r = ep.read_index(now, &log, &stable, bytes::Bytes::from_static(b"r"));
  assert_eq!(
    r,
    Err(ReadIndexError::Poisoned),
    "a fatal entries read in the LeaseGuard lease check rejects the read"
  );
  assert_eq!(ep.poison_reason(), Some(PoisonReason::LogRead));
  assert!(
    !ep.lease_guard.lease_refresh_wanted,
    "do_leader_read must fail-stop BEFORE recording a refresh demand on a dead node"
  );
  assert!(
    ep.poll_event().is_none(),
    "no ReadState event after the poison"
  );
}

// do_leader_read entry-guards: once the node is poisoned it mutates NO read state — not even the LeaseGuard
// read_since_anchor flag set before the per-mode lease check. So the deferred-read flush loop re-entering
// do_leader_read after an earlier read poisoned is a no-op (the flush loop fail-stop relies on this guard).
#[test]
fn do_leader_read_entry_guards_on_a_poisoned_node() {
  use crate::{
    AppendResponse, Config, Index, Instant, Message, ReadOnlyOption, Term, VoteResponse,
    testkit::FailTermLog,
  };
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_read_only(ReadOnlyOption::LeaseGuard)
  .with_lease_duration(Duration::from_millis(300))
  .with_clock_drift_bound(Duration::from_millis(50));
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut log = FailTermLog::default();
  let mut stable = NoopStable::default();
  let now = ep.poll_timeout().unwrap();
  ep.handle_timeout(now, &mut log, &mut stable);
  ep.handle_storage(now, &mut log, &mut stable);
  ep.handle_message(
    now,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResponse(VoteResponse::new(Term::new(1), 2u64, false, false)),
  );
  assert!(ep.role().is_leader());
  ep.handle_storage(now, &mut log, &mut stable);
  ep.handle_message(
    now,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendResponse(AppendResponse::new(
      Term::new(1),
      2u64,
      false,
      Index::ZERO,
      Term::ZERO,
      Index::new(1),
    )),
  );
  ep.handle_storage(now, &mut log, &mut stable);
  while ep.poll_event().is_some() {}
  while ep.poll_message().is_some() {}
  // A fresh anchor: no read has occurred yet.
  assert!(!ep.lease_guard.read_since_anchor);

  // Poison the node via read_index's gate (fail the term read): it returns Err(Poisoned) WITHOUT reaching
  // do_leader_read, so read_since_anchor stays unset.
  log.fail_term_at(Some(ep.commit_index()));
  assert_eq!(
    ep.read_index(now, &log, &stable, bytes::Bytes::from_static(b"r1")),
    Err(ReadIndexError::Poisoned)
  );
  assert!(ep.poison_reason().is_some());
  assert!(
    !ep.lease_guard.read_since_anchor,
    "the gate poison did not reach do_leader_read"
  );

  // Re-enter do_leader_read on the poisoned node (as the deferred-read flush loop would): the entry guard
  // must no-op — no read_since_anchor mutation, no event.
  ep.do_leader_read(
    crate::Now::monotonic(now),
    &log,
    bytes::Bytes::from_static(b"r2"),
    None,
  );
  assert!(
    !ep.lease_guard.read_since_anchor,
    "do_leader_read must mutate no read state on a poisoned node"
  );
  assert!(
    ep.poll_event().is_none(),
    "no ReadState event on a poisoned node"
  );
}
