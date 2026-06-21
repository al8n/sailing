use super::{super::*, *};
use crate::{
  ProposeError, VoteResponse,
  testkit::{CountSm, FailTermLog, NoopLog, NoopStable, VecLog},
};
use core::time::Duration;

#[test]
fn endpoint_constructs_and_polls_empty() {
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 42, Noop);
  assert_eq!(ep.id(), 1u64);
  assert!(ep.poll_message().is_none());
  assert!(ep.poll_event().is_none());
  // election timer is armed immediately on construction
  assert!(ep.poll_timeout().is_some());
}

/// Regression (no log append at index saturation): `Index::next()` saturates at u64::MAX, so
/// a leader whose `last_index == u64::MAX` (a crafted/recovered log) must NOT allocate a new entry
/// there — `submit_append` is truncate-and-append, so it would replace the existing (possibly
/// committed) entry, breaking log matching. propose / conf-change refuse with `LogIndexExhausted`.
///
/// MUTATION: revert `propose`'s `checked_next()` to `next()` → propose appends at the saturated index
/// (aliasing the entry there) instead of returning `LogIndexExhausted`.
#[test]
fn propose_at_max_index_is_refused_not_truncating() {
  use crate::{Config, Index, Instant, LogStore as _, Message, ProposeError, Term};
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
  // Elect node 1 leader (small log).
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
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}

  // Simulate a crafted/recovered log at the index ceiling: re-baseline so last_index == u64::MAX.
  log.restore(Index::new(u64::MAX), Term::new(1));
  assert_eq!(log.last_index(), Index::new(u64::MAX));

  // A normal proposal must be REFUSED, not appended at the saturated (aliased) index.
  let r = ep.propose(d, &mut log, &stable, &bytes::Bytes::from_static(b"cmd"));
  assert_eq!(
    r,
    Err(ProposeError::LogIndexExhausted),
    "propose at the max index must be refused"
  );
  assert_eq!(
    log.last_index(),
    Index::new(u64::MAX),
    "nothing appended — the entry at the ceiling is untouched"
  );
  assert!(
    ep.poison_reason().is_none(),
    "a refused proposal must not poison the node"
  );
}

/// Regression (reserve u64::MAX as a non-allocatable sentinel index): even ONE BELOW the
/// ceiling, `last_index == u64::MAX - 1`, must NOT allocate `u64::MAX`. An entry there could be
/// committed but never applied or replicated: the half-open log ranges `[i, i.next())` (apply) and
/// `[.., last.next())` (replication) saturate to an EMPTY range at the ceiling. So the usable index
/// space ends at `u64::MAX - 1`; allocation is refused once `last == u64::MAX - 1`.
///
/// MUTATION: drop the `!= u64::MAX` filter in `next_log_index` (bare `checked_next`) → propose at
/// `last == u64::MAX - 1` returns `Ok(u64::MAX)`, allocating the unreadable sentinel.
#[test]
fn propose_reserves_sentinel_max_index() {
  use crate::{Config, Index, Instant, LogStore as _, Message, ProposeError, Term};
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
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}

  // Re-baseline one BELOW the ceiling: last_index == u64::MAX - 1.
  log.restore(Index::new(u64::MAX - 1), Term::new(1));
  assert_eq!(log.last_index(), Index::new(u64::MAX - 1));

  // Allocating the next index (u64::MAX) is refused — it is the reserved sentinel (unreadable by the
  // half-open apply/replication ranges).
  let r = ep.propose(d, &mut log, &stable, &bytes::Bytes::from_static(b"cmd"));
  assert_eq!(
    r,
    Err(ProposeError::LogIndexExhausted),
    "u64::MAX must be reserved (a committed entry there could never be applied/replicated)"
  );
  assert_eq!(
    log.last_index(),
    Index::new(u64::MAX - 1),
    "nothing appended at the sentinel index"
  );
  assert!(ep.poison_reason().is_none());
}

// --- persistence tests ---

#[test]
fn op_ids_are_minted_distinctly() {
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let a = ep.mint_op_id_for_test();
  let b = ep.mint_op_id_for_test();
  assert_ne!(a, b);
  assert_eq!(b.get(), a.get() + 1);
}

/// Persist-before-RESPOND, core-enforced: a follower must not send a SUCCESS `AppendResponse` under
/// a term whose HardState write is not yet durable (Raft §5.1: persist `currentTerm` before responding
/// to RPCs). A higher-term heartbeat (no entries) adopts the term in memory and submits the term write,
/// but the success ack is DEFERRED until that write is durable — then released by `handle_storage`.
/// This isolates the TERM gate from the entry-durability gate (a heartbeat appends nothing), proving
/// the core enforces the ordering itself rather than delegating it to the storage layer.
///
/// MUTATION: drop the `term_is_durable()` check in `send_or_gate_append_ack` (send unconditionally) →
/// the ack is emitted before the term write completes (the deferral and the empty-early-batch fail).
#[test]
fn follower_defers_success_ack_until_term_durable() {
  use crate::{AppendEntries, Index, Instant, Message, Term};
  let (mut ep, mut log, mut stable) = make_follower();

  // Higher-term (5) heartbeat-shaped AppendEntries (no entries): adopts term 5; the term write is
  // submitted (in flight) by the post-dispatch `ensure_term_durable`.
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(5),
      1u64,
      Index::ZERO,
      Term::ZERO,
      std::vec![],
      Index::ZERO,
    )),
  );

  // Term 5 is adopted in memory but NOT durable, so the success ack must be withheld.
  assert!(
    !ep.term_is_durable(),
    "a freshly-adopted term is not durable until its write completes"
  );
  assert!(
    ep.durable.term_gated_append_ack.is_some(),
    "the success ack must be deferred while the term is not durable"
  );
  let early: Vec<_> = core::iter::from_fn(|| ep.poll_message())
    .filter(|o| matches!(o.message(), Message::AppendResponse(a) if !a.reject()))
    .collect();
  assert!(
    early.is_empty(),
    "no success ack may be sent under a non-durable term"
  );

  // The driver drains storage every iteration: completing the term write makes term 5 durable and
  // releases the deferred ack.
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  assert!(
    ep.term_is_durable(),
    "term 5 is durable once its HardState write completes"
  );
  assert!(
    ep.durable.term_gated_append_ack.is_none(),
    "the deferred ack was released"
  );
  let acks: Vec<_> = core::iter::from_fn(|| ep.poll_message())
    .filter(|o| matches!(o.message(), Message::AppendResponse(a) if !a.reject()))
    .collect();
  assert_eq!(
    acks.len(),
    1,
    "the success ack is sent exactly once the term is durable"
  );
}

/// `serviceable_now` mirrors the `handle_timeout` dispatch exactly.
///
/// - Follower: Heartbeat not serviceable; Election serviceable iff voter.
/// - Leader (no CQ, no transfer): only Heartbeat serviceable.
/// - Leader (CQ, no transfer): Heartbeat + Election serviceable.
/// - Leader (CQ + transfer): Heartbeat + Election + Transfer serviceable.
#[test]
fn serviceable_now_mirrors_dispatch() {
  use core::time::Duration;

  // --- Follower (voter) ---
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let ep = Endpoint::new(cfg, Instant::ORIGIN, 42, Noop);
  assert!(ep.role().is_follower());
  assert!(
    !ep.serviceable_now(TimerKind::Heartbeat),
    "follower: Heartbeat not serviceable"
  );
  assert!(
    ep.serviceable_now(TimerKind::Election),
    "follower voter: Election serviceable"
  );
  assert!(
    !ep.serviceable_now(TimerKind::Transfer),
    "follower: Transfer not serviceable"
  );

  // --- Follower (non-voter / observer) ---
  // Use try_new_observer: node 99 joins an existing cluster {1,2,3} as an observer.
  // Its id is not in the voter seed so is_voter(99) = false in its Tracker.
  let cfg_nv = Config::try_new_observer(
    99u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let ep_nv = Endpoint::new(cfg_nv, Instant::ORIGIN, 13, Noop);
  // Node 99 is not in the voter set {1,2,3} so is_voter(99) = false.
  assert!(ep_nv.role().is_follower());
  assert!(
    !ep_nv.serviceable_now(TimerKind::Election),
    "non-voter: Election NOT serviceable"
  );
  assert!(
    !ep_nv.serviceable_now(TimerKind::Heartbeat),
    "non-voter: Heartbeat not serviceable"
  );
  assert!(
    !ep_nv.serviceable_now(TimerKind::Transfer),
    "non-voter: Transfer not serviceable"
  );

  // --- Leader (no check_quorum, no transfer) ---
  let (ep_l, log_leader, stable_leader, _) = make_three_node_leader();
  assert!(ep_l.role().is_leader());
  assert!(!ep_l.config.check_quorum());
  assert!(ep_l.transfer.lead_transferee.is_none());
  assert!(
    ep_l.serviceable_now(TimerKind::Heartbeat),
    "leader: Heartbeat serviceable"
  );
  assert!(
    !ep_l.serviceable_now(TimerKind::Election),
    "leader (no CQ): Election NOT serviceable"
  );
  assert!(
    !ep_l.serviceable_now(TimerKind::Transfer),
    "leader (no transfer): Transfer not serviceable"
  );

  // --- Leader (check_quorum=true, no transfer) ---
  let cfg_cq = Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_check_quorum(true);
  let mut ep_cq = Endpoint::new(cfg_cq, Instant::ORIGIN, 1, CountSm::default());
  let mut log_cq = VecLog::default();
  let mut stable_cq = NoopStable::default();
  let d_cq = ep_cq.poll_timeout().unwrap();
  ep_cq.handle_timeout(d_cq, &mut log_cq, &mut stable_cq);
  ep_cq.handle_storage(d_cq, &mut log_cq, &mut stable_cq);
  ep_cq.handle_message(
    d_cq,
    &mut log_cq,
    &mut stable_cq,
    2u64,
    Message::VoteResponse(VoteResponse::new(Term::new(1), 2u64, false, false)),
  );
  assert!(ep_cq.role().is_leader());
  assert!(ep_cq.config.check_quorum());
  assert!(
    ep_cq.serviceable_now(TimerKind::Heartbeat),
    "leader CQ: Heartbeat serviceable"
  );
  assert!(
    ep_cq.serviceable_now(TimerKind::Election),
    "leader CQ: Election serviceable (CheckQuorum tick)"
  );
  assert!(
    !ep_cq.serviceable_now(TimerKind::Transfer),
    "leader CQ (no transfer): Transfer not serviceable"
  );

  // --- Leader (check_quorum=true, transfer in progress) ---
  let ep_cq_log_ref = &log_cq;
  let ep_cq_stable_ref = &stable_cq;
  ep_cq
    .transfer_leader(d_cq, ep_cq_log_ref, ep_cq_stable_ref, 2u64)
    .expect("transfer_leader must succeed");
  assert!(ep_cq.transfer.lead_transferee.is_some());
  assert!(
    ep_cq.serviceable_now(TimerKind::Transfer),
    "leader CQ + transfer: Transfer serviceable"
  );
  let _ = (ep_l, log_leader, stable_leader);
}

/// `poll_timeout` never surfaces a non-serviceable deadline.
///
/// - A Follower with a stale heartbeat_deadline set returns its election_deadline only.
/// - A non-voter follower returns `None` even if election_deadline is armed.
/// - A Leader without check_quorum returns only heartbeat (not election).
/// - A Leader with check_quorum returns min(heartbeat, election).
/// - A Leader with transfer returns min(heartbeat, election[if CQ], transfer).
#[test]
fn poll_timeout_only_surfaces_serviceable_deadlines() {
  use core::time::Duration;

  let election_timeout = Duration::from_millis(1000);
  let heartbeat_interval = Duration::from_millis(100);

  // --- Follower: stale heartbeat_deadline set, should NOT appear in poll_timeout ---
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    election_timeout,
    heartbeat_interval,
  )
  .unwrap();
  let mut ep_f = Endpoint::new(cfg, Instant::ORIGIN, 42, Noop);
  // Defensively set a stale heartbeat_deadline (should not be serviceable for a follower).
  let stale_hb = Instant::ORIGIN + Duration::from_millis(50);
  ep_f.heartbeat_deadline = Some(stale_hb);
  let election_dl = ep_f.election_deadline.expect("election timer armed");
  let pt = ep_f
    .poll_timeout()
    .expect("poll_timeout must be Some for voter follower");
  assert_eq!(
    pt, election_dl,
    "follower poll_timeout must return election_deadline only"
  );
  assert_ne!(
    pt, stale_hb,
    "follower poll_timeout must NOT return heartbeat_deadline"
  );

  // --- Non-voter: election_deadline armed but not serviceable → poll_timeout returns None ---
  let cfg_nv = Config::try_new_observer(
    99u64,
    std::vec![1u64, 2u64, 3u64], // 99 is not in the voter set
    election_timeout,
    heartbeat_interval,
  )
  .unwrap();
  let ep_nv = Endpoint::new(cfg_nv, Instant::ORIGIN, 7, Noop);
  assert!(
    ep_nv.election_deadline.is_some(),
    "election_deadline is armed on construction"
  );
  assert!(
    ep_nv.poll_timeout().is_none(),
    "non-voter poll_timeout must be None even with election_deadline armed"
  );

  // --- Leader (no CQ): poll_timeout returns heartbeat, NOT election ---
  let (ep_l, _log_l, _stable_l, _d_l) = make_three_node_leader();
  assert!(!ep_l.config.check_quorum());
  // The leader has no election_deadline (cleared on become_leader when CQ=false).
  assert!(ep_l.election_deadline.is_none());
  let hb_dl = ep_l.heartbeat_deadline.expect("heartbeat_deadline armed");
  let pt_l = ep_l
    .poll_timeout()
    .expect("leader poll_timeout must be Some");
  assert_eq!(
    pt_l, hb_dl,
    "leader (no CQ) poll_timeout must return heartbeat_deadline"
  );

  // --- Leader (CQ): poll_timeout returns min(heartbeat, election) ---
  let cfg_cq = Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    election_timeout,
    heartbeat_interval,
  )
  .unwrap()
  .with_check_quorum(true);
  let mut ep_cq = Endpoint::new(cfg_cq, Instant::ORIGIN, 1, CountSm::default());
  let mut log_cq = VecLog::default();
  let mut stable_cq = NoopStable::default();
  let d_cq = ep_cq.poll_timeout().unwrap();
  ep_cq.handle_timeout(d_cq, &mut log_cq, &mut stable_cq);
  ep_cq.handle_storage(d_cq, &mut log_cq, &mut stable_cq);
  ep_cq.handle_message(
    d_cq,
    &mut log_cq,
    &mut stable_cq,
    2u64,
    Message::VoteResponse(VoteResponse::new(Term::new(1), 2u64, false, false)),
  );
  assert!(ep_cq.role().is_leader());
  let hb = ep_cq.heartbeat_deadline.expect("heartbeat armed");
  let el = ep_cq.election_deadline.expect("election (CQ) armed");
  let pt_cq = ep_cq
    .poll_timeout()
    .expect("CQ leader poll_timeout must be Some");
  assert_eq!(
    pt_cq,
    hb.min(el),
    "CQ leader poll_timeout must be min(hb, el)"
  );

  // --- Leader (CQ + transfer): poll_timeout includes transfer ---
  ep_cq
    .transfer_leader(d_cq, &log_cq, &stable_cq, 2u64)
    .expect("transfer_leader must succeed");
  let tr = ep_cq
    .transfer
    .transfer_deadline
    .expect("transfer_deadline armed");
  let pt_cq_tr = ep_cq
    .poll_timeout()
    .expect("CQ+transfer leader poll_timeout must be Some");
  assert_eq!(
    pt_cq_tr,
    hb.min(el).min(tr),
    "CQ+transfer leader poll_timeout must be min(hb, el, tr)"
  );
  let _ = ep_l;
}

/// A POISONED node must surface NO serviceable timer (`poll_timeout` returns None), even with an
/// armed election deadline as a voter. `handle_timeout` (like every `handle_*`) early-returns on
/// poison, so surfacing a deadline wedges the event-driven driver: it advances `now` to that
/// deadline, the timeout fires as a no-op, the deadline stays due, and the clock can NEVER advance
/// past it — freezing the whole cluster (no other node's timer can fire). A poisoned node is
/// revived only by an external `restart`, never by a timer (a poisoned, already-removed voter that
/// froze the simulated clock would starve every election).
///
/// Before fix: `serviceable_now` ignored `poisoned`, so a poisoned voter's election timer was
/// surfaced and `poll_timeout` returned `Some`.
#[test]
fn poisoned_node_surfaces_no_timer() {
  use core::time::Duration;

  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 7, Noop);
  // A healthy voter follower surfaces its (armed) election timer.
  assert!(
    ep.poll_timeout().is_some(),
    "precondition: a healthy voter surfaces its election timer"
  );

  // An unrecoverable storage/apply error poisons the node — every handle_* is now a no-op.
  ep.poison(PoisonReason::LogRead);
  assert!(ep.is_poisoned());

  // It must surface NO timer: it services nothing until an external restart, so the driver must
  // not advance the clock to (and then no-op on) any deadline it holds.
  assert!(
    ep.poll_timeout().is_none(),
    "a poisoned node must surface no serviceable timer (else it freezes the driver clock)"
  );
}

/// `handle_timeout` → `poll_timeout` makes progress (no busy-wakeup wedge).
///
/// For each role/state, arm the relevant deadline(s) to `now` (or just past it), call
/// `handle_timeout(now)`, and assert that `poll_timeout` afterwards is either `None` or
/// strictly `> now` — the serviced timer was re-armed to a future instant or cleared.
#[test]
fn handle_timeout_makes_progress_no_wedge() {
  use core::time::Duration;
  let now = Instant::ORIGIN + Duration::from_millis(5000);

  // --- Follower voter: election timer fires → campaign → election re-armed to future ---
  let cfg_f = Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep_f = Endpoint::new(cfg_f, now, 42, Noop);
  // Force the election deadline to exactly `now` (due).
  ep_f.election_deadline = Some(now);
  let mut log_f = NoopLog;
  let mut stable_f = NoopStable::default();
  ep_f.handle_timeout(now, &mut log_f, &mut stable_f);
  ep_f.handle_storage(now, &mut log_f, &mut stable_f);
  // After: either poll_timeout is None (single-node immediate leader) or > now.
  if let Some(next_dl) = ep_f.poll_timeout() {
    assert!(
      next_dl > now,
      "follower: poll_timeout after timeout must be > now, got {next_dl:?}"
    );
  }

  // --- Non-voter follower: election timer fires silently → poll_timeout becomes None ---
  let cfg_nv = Config::try_new_observer(
    99u64,
    std::vec![1u64, 2u64, 3u64], // 99 is not in the voter set
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep_nv = Endpoint::new(cfg_nv, now, 7, Noop);
  ep_nv.election_deadline = Some(now);
  let mut log_nv = NoopLog;
  let mut stable_nv = NoopStable::default();
  ep_nv.handle_timeout(now, &mut log_nv, &mut stable_nv);
  ep_nv.handle_storage(now, &mut log_nv, &mut stable_nv);
  assert!(
    ep_nv.poll_timeout().is_none(),
    "non-voter: poll_timeout must be None after silent expiry"
  );
  assert!(
    ep_nv.election_deadline.is_none(),
    "non-voter: election_deadline must be cleared after handle_timeout"
  );

  // --- Leader (no CQ): heartbeat fires → re-armed to future ---
  let (mut ep_l, mut log_leader, mut stable_leader, _) = make_three_node_leader();
  assert!(!ep_l.config.check_quorum());
  // Force heartbeat deadline to now.
  ep_l.heartbeat_deadline = Some(now);
  ep_l.handle_timeout(now, &mut log_leader, &mut stable_leader);
  ep_l.handle_storage(now, &mut log_leader, &mut stable_leader);
  while ep_l.poll_message().is_some() {}
  let pt_l = ep_l
    .poll_timeout()
    .expect("leader: poll_timeout must be Some after heartbeat fires");
  assert!(
    pt_l > now,
    "leader: poll_timeout after heartbeat must be > now, got {pt_l:?}"
  );

  // --- Leader (CQ): both heartbeat and election fire, both re-armed ---
  let cfg_cq = Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_check_quorum(true);
  let mut ep_cq = Endpoint::new(cfg_cq, Instant::ORIGIN, 1, CountSm::default());
  let mut log_cq = VecLog::default();
  let mut stable_cq = NoopStable::default();
  let d_cq = ep_cq.poll_timeout().unwrap();
  ep_cq.handle_timeout(d_cq, &mut log_cq, &mut stable_cq);
  ep_cq.handle_storage(d_cq, &mut log_cq, &mut stable_cq);
  ep_cq.handle_message(
    d_cq,
    &mut log_cq,
    &mut stable_cq,
    2u64,
    Message::VoteResponse(VoteResponse::new(Term::new(1), 2u64, false, false)),
  );
  assert!(ep_cq.role().is_leader());
  // Force both timers to now.
  ep_cq.heartbeat_deadline = Some(now);
  ep_cq.election_deadline = Some(now);
  ep_cq.handle_timeout(now, &mut log_cq, &mut stable_cq);
  ep_cq.handle_storage(now, &mut log_cq, &mut stable_cq);
  while ep_cq.poll_message().is_some() {}
  // After: either stepped down (quorum inactive) or both timers re-armed to future.
  if let Some(pt_cq) = ep_cq.poll_timeout() {
    assert!(
      pt_cq > now,
      "CQ leader: poll_timeout after timeout must be > now, got {pt_cq:?}"
    );
  }
  // No serviceable-and-due timer must remain (the debug_assert also guards this).
  for &k in &TimerKind::ALL {
    let still_due = ep_cq.serviceable_now(k) && ep_cq.deadline_of(k).is_some_and(|d| d <= now);
    assert!(
      !still_due,
      "CQ leader: timer {k} is still serviceable-and-due after handle_timeout"
    );
  }

  // --- Leader (transfer): transfer deadline fires → cleared ---
  let (mut ep_tr, mut log_tr, mut stable_tr, d_tr) = make_three_node_leader();
  ep_tr
    .transfer_leader(d_tr, &log_tr, &stable_tr, 2u64)
    .expect("transfer_leader must succeed");
  while ep_tr.poll_message().is_some() {}
  // Force transfer deadline to now.
  ep_tr.transfer.transfer_deadline = Some(now);
  ep_tr.heartbeat_deadline = Some(now + Duration::from_millis(100)); // not due
  ep_tr.handle_timeout(now, &mut log_tr, &mut stable_tr);
  ep_tr.handle_storage(now, &mut log_tr, &mut stable_tr);
  while ep_tr.poll_message().is_some() {}
  assert!(
    ep_tr.transfer.lead_transferee.is_none(),
    "transfer abort: lead_transferee must be cleared"
  );
  assert!(
    ep_tr.transfer.transfer_deadline.is_none(),
    "transfer abort: transfer_deadline must be cleared"
  );
  assert!(
    !ep_tr.serviceable_now(TimerKind::Transfer),
    "transfer abort: Transfer no longer serviceable"
  );
}

/// Regression: a committed Normal entry whose `StateMachine::apply` returns
/// `Err` must POISON the node with `PoisonReason::Apply` — not silently stall apply — and the
/// poisoned node must be inert (all `handle_*` are no-ops).
///
/// FAILS-ON-OLD: with the bare `break` (no `self.poison()`), `is_poisoned()` stays `false`,
/// `applied` stays stuck behind `commit`, and the node keeps serving — so all three asserts
/// (poisoned, reason, inertness) fail.
#[test]
fn failing_fsm_apply_poisons_node() {
  use crate::{AppendEntries, Index, Message, Term};
  use core::time::Duration;

  // Node 2 is a follower in a 3-voter cluster {1, 2, 3}.
  let cfg = Config::try_new(
    2u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 7, FailSm);
  let mut log = VecLog::default();
  let mut stable = NoopStable::default();

  // Leader 1 (term 1) sends one Normal entry carrying the 0xFF sentinel; leader_commit = 1
  // forces the follower to commit and apply it. FailSm::apply will return Err.
  let bad = normal_entry(1, 1, &[0xFFu8]);
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
      std::vec![bad],
      Index::new(1), // leader_commit = 1: the entry is committed
    )),
  );
  // Drain the deferred append completion so apply_committed runs with the durable entry.
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);

  // The FSM apply failed → node poisoned, with the precise cause.
  assert!(
    ep.is_poisoned(),
    "node must be poisoned when StateMachine::apply errors"
  );
  assert_eq!(
    ep.poison_reason(),
    Some(PoisonReason::Apply),
    "poison_reason must record the apply failure"
  );
  // applied is stuck at the pre-apply watermark (the failing entry was never applied).
  assert_eq!(
    ep.applied,
    Index::ZERO,
    "the failing entry must not advance applied"
  );

  // The poisoned node is inert: subsequent handle_* are no-ops.
  let outgoing_before = ep.outputs.outgoing.len();
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
      std::vec![normal_entry(1, 2, b"ok")],
      Index::new(2),
    )),
  );
  ep.handle_timeout(
    Instant::ORIGIN + Duration::from_secs(10),
    &mut log,
    &mut stable,
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  assert_eq!(
    ep.outputs.outgoing.len(),
    outgoing_before,
    "a poisoned node must emit nothing on subsequent handle_*"
  );
  assert_eq!(
    ep.poison_reason(),
    Some(PoisonReason::Apply),
    "poison_reason is first-cause-wins and must not change"
  );
}

/// Regression: a committed Normal entry whose `data` does NOT decode as the
/// SM's `Command` must POISON the node with `PoisonReason::NormalEntryDecode`.
///
/// FAILS-ON-OLD: with the bare `break` the decode error silently stalls apply —
/// `is_poisoned()` stays `false` and `applied` is stuck behind `commit`.
#[test]
fn corrupt_normal_entry_poisons_node() {
  use crate::{AppendEntries, Index, Message, Term};
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

  // A Normal entry whose data is a single byte. `<Bytes as Data>::decode` needs an 8-byte
  // u64 length prefix, so this decodes as UnexpectedEof → corrupt-log decode error.
  let corrupt = crate::Entry::new(
    Term::new(1),
    Index::new(1),
    crate::EntryKind::Normal,
    bytes::Bytes::from_static(&[0x01u8]),
  );
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
      std::vec![corrupt],
      Index::new(1), // leader_commit = 1: the corrupt entry is committed
    )),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);

  assert!(
    ep.is_poisoned(),
    "node must be poisoned when a committed Normal entry fails to decode"
  );
  assert_eq!(
    ep.poison_reason(),
    Some(PoisonReason::NormalEntryDecode),
    "poison_reason must record the decode failure"
  );
  assert_eq!(
    ep.applied,
    Index::ZERO,
    "the undecodable entry must not advance applied"
  );
}

/// `PoisonReason` follows the unit-enum convention (snake_case `as_str` + Display + predicates).
#[test]
fn poison_reason_as_str_display_and_predicate() {
  use crate::PoisonReason;
  assert_eq!(PoisonReason::Apply.as_str(), "apply");
  assert_eq!(
    PoisonReason::NormalEntryDecode.as_str(),
    "normal_entry_decode"
  );
  assert_eq!(PoisonReason::SnapshotRestore.as_str(), "snapshot_restore");
  assert_eq!(
    PoisonReason::CommittedTruncation.as_str(),
    "committed_truncation"
  );
  assert!(PoisonReason::LogRead.is_log_read());
  assert!(!PoisonReason::LogRead.is_apply());
  assert!(PoisonReason::CommittedTruncation.is_committed_truncation());
}

/// A node that POISONS mid-handler must emit NOTHING for the rest of that handler — no
/// `HeartbeatResponse` (the central `send` halt) and no `ReadState`. Here a `Heartbeat` advances commit
/// over a durable-but-undecodable `Normal` entry; `apply_committed` poisons (`NormalEntryDecode`)
/// and the handler would otherwise still queue a `HeartbeatResponse` to the leader.
///
/// FAILS-ON-OLD: without the `send` guard the poisoned follower still replies a `HeartbeatResponse`,
/// acking a heartbeat it can no longer honor.
#[test]
fn poison_after_apply_emits_nothing() {
  use crate::{Entry, EntryKind, Heartbeat, Index, Message, PoisonReason, Term};
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

  // A durable Normal entry at index 1 whose data is a single byte: `<Bytes as Data>::decode`
  // needs an 8-byte length prefix, so applying it fails → poison. Seed it directly (already
  // durable) so the heartbeat only has to advance commit over it.
  log.force_append(&[Entry::new(
    Term::new(1),
    Index::new(1),
    EntryKind::Normal,
    bytes::Bytes::from_static(&[0x01u8]),
  )]);

  // A heartbeat from leader 1 with commit=1 makes the follower advance commit to 1 and apply —
  // the apply poisons. The handler then reaches its tail `send(HeartbeatResponse)`, which must be
  // suppressed by the central `send` poison-guard.
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::Heartbeat(Heartbeat::new(
      Term::new(1),
      1u64,
      Index::new(1),
      bytes::Bytes::new(),
    )),
  );

  assert!(
    ep.is_poisoned(),
    "follower must be poisoned by the undecodable committed entry"
  );
  assert_eq!(ep.poison_reason(), Some(PoisonReason::NormalEntryDecode));
  // No HeartbeatResponse (nor any other message) may leak out of a poisoned node.
  assert!(
    ep.poll_message().is_none(),
    "a poisoned node must emit no message (no HeartbeatResponse ack)"
  );
  // And no ReadState event slipped out either.
  assert!(
    !ep.poll_all_events_any_read_state(),
    "a poisoned node must complete no read (no ReadState event)"
  );
}

/// Follower side: a fatal term-read inside `find_conflict_by_term` during an AppendEntries
/// reject walk must short-circuit — the node poisons and sends NO reject `AppendResponse`.
///
/// On the follower path the no-send guarantee is enforced jointly by FIX 1 (propagate `None`) and
/// the pre-existing `hint_term` guard (the index `find_conflict_by_term` fails on is the same index
/// the follower would re-read for `hint_term`, which fails again). This test locks in the
/// end-to-end behavior; the leader-side sibling test is the one that isolates FIX 1's
/// progress-mutation short-circuit.
#[test]
fn find_conflict_by_term_poison_propagation_follower() {
  use crate::{AppendEntries, Config, Entry, EntryKind, Index, Instant, Message, Term};
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

  // Three durable (uncommitted) entries at term 5 (indices 1..=3).
  let mut log = FailTermLog::default();
  log.force_append(&[
    Entry::new(
      Term::new(5),
      Index::new(1),
      EntryKind::Empty,
      bytes::Bytes::new(),
    ),
    Entry::new(
      Term::new(5),
      Index::new(2),
      EntryKind::Empty,
      bytes::Bytes::new(),
    ),
    Entry::new(
      Term::new(5),
      Index::new(3),
      EntryKind::Empty,
      bytes::Bytes::new(),
    ),
  ]);

  // Send an INCONSISTENT AppendEntries that reaches the reject path WITHOUT poisoning in the
  // consistency check: prev_log_index=3 reads term(3)=5 (NOT armed) which != prev_log_term=2 →
  // inconsistent, no poison. The reject walk then starts at min(3, last=3)=3: term(3)=5 > 2 →
  // step to index 2 → term(2) is ARMED → Err → poison → `find_conflict_by_term` returns None →
  // the handler short-circuits before computing a hint term or sending a reject.
  log.fail_term_at(Some(Index::new(2)));
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(5),
      1u64,
      Index::new(3),
      Term::new(2),
      std::vec![],
      Index::ZERO,
    )),
  );

  assert!(
    ep.is_poisoned(),
    "fatal term-read in the reject walk must poison"
  );
  assert_eq!(ep.poison_reason(), Some(PoisonReason::LogTerm));
  assert!(
    ep.poll_message().is_none(),
    "no reject AppendResponse may be sent on a fabricated conflict index"
  );
}

/// A message ENQUEUED in an earlier dispatch must never reach the wire once the node poisons in a
/// LATER dispatch. The emit-halt lives at the EGRESS (`poll_message`), not only at `send`'s enqueue:
/// a candidate broadcasts `RequestVote`s (queued, not drained), then a follow-up AppendEntries
/// triggers a fatal term-read mid-`on_append_entries` and poisons — those already-queued votes must
/// be SUPPRESSED at the egress, not leak from a dead node.
///
/// FAILS-ON-OLD: with the `if self.poisoned { return None; }` guard removed from `poll_message`, a
/// queued `RequestVote` leaks and the `is_none()` assertion below fires.
#[test]
fn queued_message_is_suppressed_after_later_dispatch_poisons() {
  use crate::{AppendEntries, Config, Entry, EntryKind, Index, Instant, Message, Term};
  use core::time::Duration;
  // pre_vote / check_quorum both default to false, so a fired election timer goes STRAIGHT to
  // `become_candidate` (bumping the term and broadcasting real RequestVotes) — not through a
  // pre-vote probe. A fresh node starts at term 0, so the first campaign is term 1; the term-1
  // AppendEntries below is therefore a SAME-term step-down (the candidate recognizes the leader).
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut stable = NoopStable::default();

  // Two durable entries at indices 1 and 2 (both term 1).
  let mut log = FailTermLog::default();
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

  // Arm a FATAL term-read at index 1, but leave term(2) OK. Dispatch 1 (become_candidate) reads
  // only term(last_index=2) → OK; dispatch 2 (on_append_entries consistency check) reads
  // term(prev_log_index=1) → Err → poison.
  log.fail_term_at(Some(Index::new(1)));

  // DISPATCH 1: fire the election timer. become_candidate bumps term 0→1, reads term(2)=OK, and
  // broadcasts RequestVote{term:1} to voter peers 2 and 3. We do NOT drain `outgoing`, and we do
  // NOT drain `stable` (so the self-vote write stays pending and become_leader never fires) — the
  // node sits as a Candidate with two RequestVotes QUEUED.
  let fire = Instant::ORIGIN + Duration::from_millis(5000);
  ep.handle_timeout(fire, &mut log, &mut stable);
  assert!(!ep.is_poisoned(), "candidate is healthy after dispatch 1");
  assert_eq!(
    ep.role(),
    Role::Candidate,
    "fired timer made us a candidate"
  );
  assert_eq!(ep.term(), Term::new(1), "first campaign is term 1");
  // Sanity (without draining): at least one queued message is a RequestVote at term 1 — proving the
  // votes really are sitting in the egress BEFORE the poison happens.
  assert!(
    ep.outputs
      .outgoing
      .iter()
      .any(|o| matches!(o.message(), Message::RequestVote(rv) if rv.term() == Term::new(1))),
    "become_candidate must have QUEUED a RequestVote(term=1) before any poison"
  );

  // DISPATCH 2: deliver an AppendEntries at the SAME term (1) with prev at index 1. on_append_entries
  // sets role=Follower (candidate recognizes the term-1 leader), then the consistency check reads
  // term(1) → armed Err → poison, and returns before sending anything.
  ep.handle_message(
    fire,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(1),
      2u64,
      Index::new(1),
      Term::new(1),
      std::vec![],
      Index::new(1),
    )),
  );
  assert!(
    ep.is_poisoned(),
    "the fatal term-read in dispatch 2 must poison the node"
  );

  // THE PROPERTY: the RequestVotes queued in dispatch 1 must NOT leak from the now-poisoned node —
  // the egress emit-halt suppresses every queued message, and no ReadState event surfaces either.
  assert!(
    ep.poll_message().is_none(),
    "a message queued BEFORE the poison must be suppressed at the egress"
  );
  assert!(
    !ep.poll_all_events_any_read_state(),
    "a poisoned node surfaces no ReadState event"
  );
}

/// Class A regression — poison effect-boundary on the work-accepting APIs.
///
/// A poisoned node's commit/applied view is no longer trustworthy. `propose`,
/// `propose_conf_change_v2`, and `transfer_leader` must therefore reject with `Poisoned`
/// (not silently `Ok` or `NotLeader`), and — because every durability submit routes through the
/// `submit_*` no-op-when-poisoned wrappers — none of them may advance the durable log.
///
/// FAILS-ON-OLD: with the `if self.poisoned { return Err(ProposeError::Poisoned); }` guard
/// removed from `propose`, a poisoned leader's `propose` returns `Ok`/`NotLeader` instead.
#[test]
fn poisoned_node_rejects_work_and_persists_nothing() {
  use crate::{
    AppendEntries, ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2, Config,
    Entry, EntryKind, Index, Instant, Message, PoisonReason, Term,
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

  // Poison via the same FATAL term-read path as
  // `term_read_failure_at_committed_index_poisons_no_truncation`: two durable committed entries,
  // then a conflicting AppendEntries whose conflict scan reads an armed-to-fail term(2) → poison.
  let mut log = FailTermLog::default();
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
  assert!(!ep.is_poisoned(), "healthy after the setup append");
  while ep.poll_message().is_some() {}

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
  log.fail_term_at(None);
  assert!(ep.is_poisoned(), "fatal term-read must poison the node");
  assert_eq!(ep.poison_reason(), Some(PoisonReason::LogTerm));

  // Snapshot the durable tail BEFORE the work-accepting calls. None of them may advance it.
  let last_before = log.last_index();
  assert_eq!(last_before, Index::new(2));

  // propose → Poisoned.
  let cmd = bytes::Bytes::from_static(b"x");
  assert_eq!(
    ep.propose(Instant::ORIGIN, &mut log, &stable, &cmd),
    Err(ProposeError::Poisoned),
    "a poisoned node must reject propose with Poisoned"
  );
  // propose_conf_change_v2 → Poisoned.
  let cc = ConfChangeV2::new(
    ConfChangeTransition::Auto,
    std::vec![ConfChangeSingle::new(ConfChangeType::AddNode, 4u64)],
    bytes::Bytes::new(),
  );
  assert_eq!(
    ep.propose_conf_change_v2(Instant::ORIGIN, &mut log, &stable, cc),
    Err(ProposeError::Poisoned),
    "a poisoned node must reject propose_conf_change_v2 with Poisoned"
  );
  // transfer_leader → Poisoned.
  assert_eq!(
    ep.transfer_leader(Instant::ORIGIN, &log, &stable, 2u64),
    Err(crate::TransferError::Poisoned),
    "a poisoned node must reject transfer_leader with Poisoned"
  );

  // No durable work was produced by any of those calls.
  assert_eq!(
    log.last_index(),
    last_before,
    "a poisoned node must persist nothing: last_index must not advance across the rejected calls"
  );

  // White-box backstop: even a DIRECT call to the private submit wrapper no-ops when poisoned.
  // (The public-API guards above are the first line of defense; `submit_append` is the
  // structural one that holds for any caller.) `tests` is an inner module of `endpoint`, so the
  // private method is in scope.
  let opid = ep.mint_op_id_for_test();
  let entry = Entry::new(
    Term::new(2),
    log.last_index().next(),
    EntryKind::Empty,
    bytes::Bytes::new(),
  );
  ep.submit_append(&mut log, opid, core::slice::from_ref(&entry));
  assert_eq!(
    log.last_index(),
    last_before,
    "submit_append must no-op when poisoned: the durable tail must not advance"
  );
}
