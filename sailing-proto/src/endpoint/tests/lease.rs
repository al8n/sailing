use super::{super::*, *};

/// Regression (LeaseBased degradation shares the FULL Safe read path): when a LeaseBased
/// read cannot use the lease (here `check_quorum=false`), the fallback must run the Safe single-node
/// self-quorum fast-path, not merely register-and-broadcast. On a ONE-VOTER leader there are no
/// peers to answer, so without the fast-path the read would never emit `ReadState`. Sharing
/// `do_safe_read` makes the degraded read complete immediately.
///
/// MUTATION: revert the degrade arm to `add_request` + `broadcast_heartbeat_with_ctx` (the old
/// partial copy) → no `ReadState` is ever emitted for the single-voter degraded read.
#[test]
fn single_voter_leasebased_degraded_read_completes() {
  use core::time::Duration;
  let cfg = crate::Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_read_only(crate::ReadOnlyOption::LeaseBased)
  .with_check_quorum(false);

  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::NoopStable::default();

  // Single-node self-election: campaign, flush the self-vote (→ leader + no-op), flush the no-op
  // (→ commit the current-term no-op so the read is admissible).
  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable);
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(ep.role().is_leader(), "single voter must self-elect");
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}
  assert_eq!(
    ep.commit_index(),
    crate::Index::new(1),
    "single-node no-op must commit before the read"
  );

  let ctx = bytes::Bytes::from_static(b"single_lease_read");
  ep.read_index(d, &log, &stable, ctx.clone())
    .expect("single-voter leader must accept the read");

  // The degraded LeaseBased read must complete immediately via the self-quorum fast-path.
  let ev = ep
    .poll_event()
    .expect("single-voter degraded LeaseBased read must emit ReadState immediately");
  assert!(ev.is_read_state(), "expected ReadState");
  assert_eq!(ev.unwrap_read_state_ref().context().as_ref(), ctx.as_ref());
}

/// Test CQ-4: Follower lease ignores a disruptive vote request.
///
/// A follower with check_quorum=true, a live leader, and a healthy election timer (deadline
/// in the future) receives `RequestVote{term: self.term+2, leader_transfer: false}` → it
/// does NOT adopt the term, does NOT grant, term unchanged.
///
/// With `leader_transfer=true` (forced) → it IS NOT ignored (proceeds normally: adopts
/// the higher term and steps down, would eventually vote or reject based on log).
#[test]
fn check_quorum_follower_lease_blocks_disruptive_vote() {
  use crate::{Config, Index, Instant, Message, RequestVote, Term};
  use core::time::Duration;

  let cfg = Config::try_new(
    2u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_check_quorum(true);

  // "now" is well within the election timer window so deadline > now.
  let base = Instant::ORIGIN;
  let mut ep = Endpoint::new(cfg, base, 7, Noop);
  let mut log = crate::testkit::NoopLog;
  let mut stable = crate::testkit::NoopStable::default();

  // The follower must believe it has a live leader. Receive a Heartbeat from leader 1
  // to set leader=Some(1) and arm the election timer.
  ep.handle_message(
    base,
    &mut log,
    &mut stable,
    1u64,
    Message::Heartbeat(crate::Heartbeat::new(
      Term::new(1),
      1u64,
      Index::ZERO,
      bytes::Bytes::new(),
    )),
  );
  // Drain the HeartbeatResp.
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  assert_eq!(ep.term(), Term::new(1));
  assert_eq!(ep.leader(), Some(1u64));
  // election_deadline must be in the future (healthy lease).
  let deadline = ep.election_deadline.expect("election timer must be armed");
  assert!(deadline > base, "election deadline must be in the future");

  // --- Case A: non-forced RequestVote at higher term while lease is active ---
  // Simulate a small time advance that is still within the lease window.
  let now_in_lease = base + Duration::from_millis(50); // well before deadline
  ep.handle_message(
    now_in_lease,
    &mut log,
    &mut stable,
    3u64,
    Message::RequestVote(RequestVote::new(
      Term::new(3), // term+2
      3u64,
      Index::ZERO,
      Term::ZERO,
      false, // real vote, NOT pre_vote
      false, // NOT leader_transfer
    )),
  );

  // CRITICAL: term must NOT be adopted (lease blocked the message before the step-down).
  assert_eq!(
    ep.term(),
    Term::new(1),
    "follower lease must block term adoption from disruptive vote"
  );
  // No response sent (we returned early).
  assert!(
    ep.poll_message().is_none(),
    "no reply must be sent while lease blocks disruptive vote"
  );

  // --- Case B: forced (leader_transfer) RequestVote at higher term ---
  // leader_transfer bypasses the lease; this IS processed normally.
  ep.handle_message(
    now_in_lease,
    &mut log,
    &mut stable,
    3u64,
    Message::RequestVote(RequestVote::new(
      Term::new(5), // higher term
      3u64,
      Index::ZERO,
      Term::ZERO,
      false, // real vote
      true,  // leader_transfer → bypass lease
    )),
  );

  // The forced campaign bypasses the lease: the term IS adopted.
  assert_eq!(
    ep.term(),
    Term::new(5),
    "forced leader_transfer vote must bypass lease and adopt the higher term"
  );
}

/// Test 3: LeaseBased confirms immediately.
///
/// With `read_only=LeaseBased` + `check_quorum=true`, `read_index` emits ReadState
/// from `commit` without waiting for heartbeats.
#[test]
fn lease_based_confirms_immediately() {
  use core::time::Duration;
  let cfg = crate::Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_read_only(crate::ReadOnlyOption::LeaseBased)
  .with_check_quorum(true);

  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::NoopStable::default();

  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable);
  ep.handle_storage(d, &mut log, &mut stable);
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResp(crate::VoteResp::new(
      crate::Term::new(1),
      2u64,
      false,
      false,
    )),
  );
  assert!(ep.role().is_leader());
  ep.handle_storage(d, &mut log, &mut stable);
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendResp(crate::AppendResp::new(
      crate::Term::new(1),
      2u64,
      false,
      crate::Index::ZERO,
      crate::Term::ZERO,
      crate::Index::new(1),
    )),
  );
  while ep.poll_event().is_some() {}
  while ep.poll_message().is_some() {}

  // Establish a FRESH lease: tick a heartbeat round (bumps the lease round), then a quorum
  // HeartbeatResp echoing the CURRENT lease round renews `lease_valid_until`. The lease is no longer
  // the spoofable `election_deadline`, so an immediate LeaseBased read requires this fresh confirmation.
  let lease_at = ep.poll_timeout().expect("heartbeat timer armed");
  ep.handle_timeout(lease_at, &mut log, &mut stable);
  let lease_round = {
    let mut lr = None;
    while let Some(out) = ep.poll_message() {
      if let Message::Heartbeat(hb) = out.message() {
        lr = Some(hb.lease_round());
      }
    }
    lr.expect("leader broadcast a heartbeat carrying a lease round")
  };
  ep.handle_message(
    lease_at,
    &mut log,
    &mut stable,
    2u64,
    Message::HeartbeatResp(
      crate::HeartbeatResp::new(crate::Term::new(1), 2u64, bytes::Bytes::new())
          .with_lease_round(lease_round)
          // advertise enforcement (the follower's own election_timeout) so the leader counts it.
          .with_lease_support(Duration::from_millis(1000)),
    ),
  );
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  let ctx = bytes::Bytes::from_static(b"lease_read");
  ep.read_index(lease_at, &log, &stable, ctx.clone())
    .expect("LeaseBased + check_quorum leader must accept the read");

  // No read heartbeats should have been sent for the read round. A read heartbeat carries a
  // non-empty context (the internal round token); the immediate LeaseBased path sends none.
  let mut read_hb_sent = false;
  while let Some(out) = ep.poll_message() {
    if let Message::Heartbeat(hb) = out.message()
      && !hb.context().is_empty()
    {
      read_hb_sent = true;
    }
  }
  assert!(
    !read_hb_sent,
    "LeaseBased must NOT broadcast read-heartbeats"
  );

  // ReadState must be emitted immediately.
  let ev = ep
    .poll_event()
    .expect("LeaseBased must emit ReadState immediately");
  assert!(ev.is_read_state(), "expected ReadState event");
  let rs = ev.unwrap_read_state_ref();
  assert_eq!(rs.index(), crate::Index::new(1));
  assert_eq!(rs.context().as_ref(), ctx.as_ref());
}

/// Regression (the LeaseBased read lease is renewed ONLY by FRESH current-round responses):
/// a stale or duplicated `HeartbeatResp` echoing an EARLIER CheckQuorum round must NOT renew the
/// lease. Otherwise an isolated old leader could keep serving stale lease reads on delayed/duplicated
/// pre-partition traffic (unbounded under duplication), while a new leader commits newer state.
///
/// MUTATION: drop the `resp.lease_round() == self.lease_round` guard in `on_heartbeat_resp` → the
/// stale earlier-round response renews the lease (`lease_acks` gains the peer, `lease_valid_until`
/// extends).
#[test]
fn stale_round_heartbeat_resp_does_not_renew_lease() {
  use core::time::Duration;
  let cfg = crate::Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_read_only(crate::ReadOnlyOption::LeaseBased)
  .with_check_quorum(true);
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::NoopStable::default();
  // Elect node 1 leader.
  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable);
  ep.handle_storage(d, &mut log, &mut stable);
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResp(crate::VoteResp::new(
      crate::Term::new(1),
      2u64,
      false,
      false,
    )),
  );
  assert!(ep.role().is_leader());
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}

  // Tick a heartbeat round and return (time, lease round token).
  fn tick_round(
    ep: &mut Endpoint<u64, crate::testkit::CountSm>,
    log: &mut crate::testkit::VecLog,
    stable: &mut crate::testkit::NoopStable,
  ) -> (crate::Instant, u64) {
    let at = ep.poll_timeout().expect("heartbeat timer armed");
    ep.handle_timeout(at, log, stable);
    let mut lr = None;
    while let Some(out) = ep.poll_message() {
      if let Message::Heartbeat(hb) = out.message() {
        lr = Some(hb.lease_round());
      }
    }
    (at, lr.expect("heartbeat carried a lease round"))
  }
  fn hb_resp(round: u64) -> Message<u64> {
    Message::HeartbeatResp(
      crate::HeartbeatResp::new(crate::Term::new(1), 2u64, bytes::Bytes::new())
          .with_lease_round(round)
          // advertise enforcement (the follower's own election_timeout) so the leader counts it.
          .with_lease_support(Duration::from_millis(1000)),
    )
  }

  // Round 1 (r1): a fresh quorum ack establishes the lease.
  let (t1, r1) = tick_round(&mut ep, &mut log, &mut stable);
  ep.handle_message(t1, &mut log, &mut stable, 2u64, hb_resp(r1));
  assert!(
    ep.check_quorum_lease.lease_valid_until.is_some(),
    "a fresh current-round quorum ack establishes the lease"
  );

  // Round 2 (r2): a new round opens — lease_acks is cleared and round 1 is now STALE.
  let (t2, r2) = tick_round(&mut ep, &mut log, &mut stable);
  assert_ne!(r1, r2, "a new heartbeat round bumps the lease round");
  assert!(
    ep.check_quorum_lease.lease_acks.is_empty(),
    "a new round clears the ack set"
  );
  let lease_before = ep.check_quorum_lease.lease_valid_until;

  // A STALE HeartbeatResp echoing the OLD round (r1) must be IGNORED for the lease.
  ep.handle_message(t2, &mut log, &mut stable, 2u64, hb_resp(r1));
  assert!(
    !ep.check_quorum_lease.lease_acks.contains(&2u64),
    "a stale (old-round) ack must not count toward the lease"
  );
  assert_eq!(
    ep.check_quorum_lease.lease_valid_until, lease_before,
    "a stale ack must not renew the lease"
  );

  // A FRESH HeartbeatResp echoing the CURRENT round (r2) renews the lease.
  ep.handle_message(t2, &mut log, &mut stable, 2u64, hb_resp(r2));
  assert!(
    ep.check_quorum_lease.lease_acks.contains(&2u64),
    "a fresh current-round ack counts toward the lease"
  );
  assert!(
    ep.check_quorum_lease
      .lease_valid_until
      .is_some_and(|d| d >= t2),
    "a fresh quorum ack renews the lease"
  );
}

/// Regression (the lease deadline is bounded by the round's SEND instant, not the response
/// receipt time): a DELAYED current-round HeartbeatResp must renew the lease to
/// `lease_round_start + election_timeout`, NOT `response_receipt + election_timeout`. Followers reset
/// their election timers when they RECEIVED the round (≈ its send instant), so measuring from a
/// delayed response would extend the lease past the quorum's election window and let an isolated
/// leader serve a stale read.
///
/// MUTATION: renew from `now` (response receipt) instead of `lease_round_start` → the lease extends
/// by the response delay.
#[test]
fn delayed_heartbeat_resp_does_not_extend_lease_past_send_window() {
  use core::time::Duration;
  let cfg = crate::Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_read_only(crate::ReadOnlyOption::LeaseBased)
  .with_check_quorum(true);
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::NoopStable::default();
  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable);
  ep.handle_storage(d, &mut log, &mut stable);
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResp(crate::VoteResp::new(
      crate::Term::new(1),
      2u64,
      false,
      false,
    )),
  );
  assert!(ep.role().is_leader());
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}

  // Tick a heartbeat round; capture its SEND instant and lease round token.
  let t_send = ep.poll_timeout().expect("heartbeat timer armed");
  ep.handle_timeout(t_send, &mut log, &mut stable);
  let round = {
    let mut lr = None;
    while let Some(out) = ep.poll_message() {
      if let Message::Heartbeat(hb) = out.message() {
        lr = Some(hb.lease_round());
      }
    }
    lr.expect("heartbeat carried a lease round")
  };

  // The quorum's HeartbeatResp echoing this round arrives MUCH later (delayed in transit).
  let t_late = t_send + Duration::from_millis(500);
  ep.handle_message(
    t_late,
    &mut log,
    &mut stable,
    2u64,
    Message::HeartbeatResp(
      crate::HeartbeatResp::new(crate::Term::new(1), 2u64, bytes::Bytes::new())
          .with_lease_round(round)
          // advertise enforcement (the follower's own election_timeout) so the leader counts it.
          .with_lease_support(Duration::from_millis(1000)),
    ),
  );

  // The lease must be bounded by the SEND instant, NOT the (delayed) receipt instant.
  assert_eq!(
    ep.check_quorum_lease.lease_valid_until,
    Some(t_send + Duration::from_millis(1000)),
    "lease must expire at round_start + election_timeout, not response_receipt + election_timeout"
  );
  assert_ne!(
    ep.check_quorum_lease.lease_valid_until,
    Some(t_late + Duration::from_millis(1000)),
    "a delayed response must not extend the lease by the response delay"
  );
}

/// Self-validating lease: a voter that does NOT enforce the lease window (HeartbeatResp
/// `lease_support == 0`) must NOT renew the lease — even if it freshly acks the current round. This
/// closes the heterogeneous/misconfigured-cluster cooperation hole: a Safe/CQ-disabled voter cannot
/// keep a LeaseBased leader's lease alive.
///
/// MUTATION: drop the `resp.lease_support() > 0` gate in `on_heartbeat_resp` → the non-enforcing ack
/// renews the lease.
#[test]
fn lease_not_renewed_by_non_enforcing_voter() {
  use crate::Message;
  let (mut ep, mut log, mut stable) = leasebased_leader();
  let (at, round) = tick_lease_round(&mut ep, &mut log, &mut stable);
  // Peer 2 acks the CURRENT round but advertises NO enforcement (default lease_support == ZERO).
  ep.handle_message(
    at,
    &mut log,
    &mut stable,
    2u64,
    Message::HeartbeatResp(
      crate::HeartbeatResp::new(crate::Term::new(1), 2u64, bytes::Bytes::new())
        .with_lease_round(round),
    ),
  );
  assert!(
    ep.check_quorum_lease.lease_valid_until.is_none(),
    "a non-enforcing voter must NOT renew the lease (self-validating)"
  );
}

/// The lease deadline is bounded by the quorum's MIN advertised support, so a voter with a
/// SHORTER election_timeout (heterogeneous config) caps the lease at its real election window — the
/// leader cannot out-live the supporter that would time out first.
///
/// MUTATION: renew with `self.config.election_timeout()` instead of `self.lease_min_support` → the
/// lease extends to the leader's 1000ms even though peer 2 only supports 300ms.
#[test]
fn lease_bounded_by_min_support() {
  use crate::Message;
  use core::time::Duration;
  let (mut ep, mut log, mut stable) = leasebased_leader();
  let (at, round) = tick_lease_round(&mut ep, &mut log, &mut stable);
  let lease_start = ep.check_quorum_lease.lease_round_start;
  // Peer 2 enforces but with a SHORTER election_timeout (300ms < the leader's 1000ms).
  ep.handle_message(
    at,
    &mut log,
    &mut stable,
    2u64,
    Message::HeartbeatResp(
      crate::HeartbeatResp::new(crate::Term::new(1), 2u64, bytes::Bytes::new())
        .with_lease_round(round)
        .with_lease_support(Duration::from_millis(300)),
    ),
  );
  assert_eq!(
    ep.check_quorum_lease.lease_valid_until,
    Some(lease_start + Duration::from_millis(300)),
    "the lease must be bounded by the quorum's MIN support (300ms), not the leader's 1000ms"
  );
}

/// A committed MEMBERSHIP change must revoke the
/// lease. The lease's safety rests on quorum OVERLAP, guaranteed only within a single configuration; a
/// new config can have a quorum disjoint from the lease's quorum, so the lease no longer proves "no
/// other leader". Applying a ConfChange revokes the lease → `do_leader_read` degrades to Safe.
///
/// MUTATION: drop the `lease_valid_until = None` on the ConfChange-apply path → the lease survives the
/// membership change.
#[test]
fn membership_change_revokes_lease() {
  use crate::{ConfChange, ConfChangeType, Index, Message, Term};
  use core::time::Duration;
  let (mut ep, mut log, mut stable) = leasebased_leader();
  // Establish a live lease.
  let (at, round) = tick_lease_round(&mut ep, &mut log, &mut stable);
  ep.handle_message(
    at,
    &mut log,
    &mut stable,
    2u64,
    Message::HeartbeatResp(
      crate::HeartbeatResp::new(Term::new(1), 2u64, bytes::Bytes::new())
        .with_lease_round(round)
        .with_lease_support(Duration::from_millis(1000)),
    ),
  );
  assert!(
    ep.check_quorum_lease.lease_valid_until.is_some(),
    "lease established"
  );

  // Propose + commit + apply a ConfChange (add a learner) → membership changes → lease revoked.
  ep.propose_conf_change(
    at,
    &mut log,
    &stable,
    ConfChange::new(ConfChangeType::AddLearnerNode, 4u64, bytes::Bytes::new()),
  )
  .expect("AddLearnerNode(4) must be accepted");
  ep.handle_storage(at, &mut log, &mut stable); // self append durable
  // Peer 2 acks the ConfChange entry (index 2) → commit=2 → apply_committed folds it → tracker change.
  ep.handle_message(
    at,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendResp(crate::AppendResp::new(
      Term::new(1),
      2u64,
      false,
      Index::ZERO,
      Term::ZERO,
      Index::new(2),
    )),
  );
  ep.handle_storage(at, &mut log, &mut stable);
  assert!(
    ep.check_quorum_lease.lease_valid_until.is_none(),
    "a committed membership change must revoke the lease (quorum overlap no longer guaranteed)"
  );
}

/// The decisive persist-before-ADVERTISE gate: a follower advertises ZERO lease support
/// until its floor is DURABLE, then its real election_timeout. So the leader can never float a lease on a
/// promise a crash could erase.
///
/// MUTATION: drop the `&& self.durable_lease_support >= Some(this_run)` gate in `on_heartbeat` (advertise
/// `this_run` unconditionally) → the FIRST response carries 1000ms before the floor is durable.
#[test]
fn advertise_is_zero_until_lease_support_floor_durable_then_full() {
  use core::time::Duration;
  let et = Duration::from_millis(1000);
  let (mut ep, mut log, mut stable) = enforcing_follower(et);
  let now = crate::Instant::ORIGIN;
  let s1 = follower_advertised_support(&mut ep, &mut log, &mut stable, now, 5, 1);
  assert_eq!(
    s1,
    Duration::ZERO,
    "a follower must advertise ZERO until its lease-support floor is durable"
  );
  // Drain the floor write → durable_lease_support advances.
  ep.handle_storage(now, &mut log, &mut stable);
  let s2 = follower_advertised_support(&mut ep, &mut log, &mut stable, now, 5, 2);
  assert_eq!(
    s2, et,
    "once the floor is durable the follower advertises its real election_timeout"
  );
}

/// A crash in the fsync window (the floor write never reaches disk) is HARMLESS because only ZERO
/// was ever advertised — so a restart under weaker config with no durable promise (fence None) is safe.
///
/// MUTATION: ungate the advertise (as above) → the pre-crash advertisement would be 1000ms, the leader
/// would float a lease, and the None post-restart fence would be a stale-read hole.
#[test]
fn persist_before_advertise_survives_crash_in_fsync_window() {
  use crate::{Config, Instant};
  use core::time::Duration;
  let et = Duration::from_millis(1000);
  let (mut ep, mut log, mut stable) = enforcing_follower(et);
  let now = Instant::ORIGIN;
  let s1 = follower_advertised_support(&mut ep, &mut log, &mut stable, now, 5, 1);
  assert_eq!(s1, Duration::ZERO, "pre-durable advertise must be ZERO");
  // CRASH before the floor write drains → the promise was never durable.
  stable.discard_inflight();
  assert_eq!(
    stable.hard_state().promised_lease_support(),
    None,
    "the inflight lease-support write was lost on crash"
  );
  drop(ep);
  // Restart under WEAKER config (enforcement disabled). No durable promise → no fence — and none is
  // needed, since only ZERO was ever advertised (no lease was floated on this node).
  let cfg = Config::try_new(
    2u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(100),
    Duration::from_millis(50),
  )
  .unwrap();
  let r_now = now + Duration::from_millis(50);
  let ep2 = Endpoint::restart(
    cfg,
    r_now,
    7,
    crate::testkit::CountSm::default(),
    1,
    &mut log,
    &mut stable,
  );
  assert_eq!(
    ep2.lease_vote_fence_until, None,
    "no durable promise → no fence (safe: only ZERO was ever advertised)"
  );
}

/// A grow → crash-in-fsync-window → shrink chain must not under-fence. The grown floor that never
/// reached disk is lost; the restart must fence for the last DURABLE promise (run A's 1000ms), not the
/// lost 2000ms grow nor the 100ms shrink.
///
/// MUTATION: drop the monotone-max (`floor = this_run`) → run C fences for 100ms; or fence from
/// `this_run` → same. Either under-fences run A's still-possible 1000ms lease.
#[test]
fn grow_then_crash_then_shrink_does_not_underfence() {
  use crate::{Config, HardState, Instant};
  use core::time::Duration;
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::AsyncStable::default();
  // Run A already persisted+flushed a 1000ms promise.
  stable.force_hard_state(
    HardState::initial().with_lease_support(crate::LeaseSupport::Recorded(Some(
      Duration::from_millis(1000),
    ))),
  );
  // Run B: restart GROWN to et=2000 → submits a floor=2000 write (in flight).
  let cfg_b = Config::try_new(
    2u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(2000),
    Duration::from_millis(50),
  )
  .unwrap()
  .with_check_quorum(true);
  let now_b = Instant::ORIGIN + Duration::from_millis(1000);
  let _ep_b = Endpoint::restart(
    cfg_b,
    now_b,
    7,
    crate::testkit::CountSm::default(),
    1,
    &mut log,
    &mut stable,
  );
  // CRASH in the fsync window → the 2000 write never reached disk; durable rolls back to run A's 1000.
  stable.discard_inflight();
  assert_eq!(
    stable.hard_state().promised_lease_support(),
    Some(Duration::from_millis(1000)),
    "the grown 2000ms floor was lost; durable stays at run A's 1000ms"
  );
  // Run C: restart SHRUNK to et=100. Fence must still cover run A's still-possible 1000ms lease.
  let cfg_c = Config::try_new(
    2u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(100),
    Duration::from_millis(50),
  )
  .unwrap()
  .with_check_quorum(true);
  let now_c = Instant::ORIGIN + Duration::from_millis(1100);
  let ep_c = Endpoint::restart(
    cfg_c,
    now_c,
    7,
    crate::testkit::CountSm::default(),
    1,
    &mut log,
    &mut stable,
  );
  assert_eq!(
    ep_c.lease_vote_fence_until,
    Some(now_c + Duration::from_millis(1000)),
    "fence must honor run A's durable 1000ms, not the lost 2000 grow nor the 100ms shrink"
  );
}

/// A recorded genuine-ZERO floor means "promised nothing" — it must NOT arm a degenerate `now+0`
/// fence. (In-tree we never persist ZERO, but an out-of-tree decoder might, so the fence filters it.)
///
/// MUTATION: drop the `.filter(|d| !d.is_zero())` on `durable_window` → Some(ZERO) arms a `Some(now)`
/// fence instead of None.
#[test]
fn genuine_zero_floor_does_not_force_fence() {
  use crate::{Config, HardState, Instant};
  use core::time::Duration;
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::AsyncStable::default();
  stable.force_hard_state(
    HardState::initial().with_lease_support(crate::LeaseSupport::Recorded(Some(Duration::ZERO))),
  );
  let cfg = Config::try_new(
    2u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(50),
  )
  .unwrap(); // enforcement off
  let now = Instant::ORIGIN + Duration::from_millis(5000);
  let ep = Endpoint::restart(
    cfg,
    now,
    7,
    crate::testkit::CountSm::default(),
    1,
    &mut log,
    &mut stable,
  );
  assert_eq!(
    ep.lease_vote_fence_until, None,
    "a recorded genuine-ZERO floor promised nothing → no fence (must not arm now+0)"
  );
}

/// Choke-point floor preservation: under a strict `StableStore` whose `hard_state()`
/// returns LAST-DURABLE state, writers that rebuild HardState from it (vote grant, commit, campaign) must
/// NOT erase a lease-support floor whose raise is still in flight. `submit_write` stamps the in-memory
/// floor on EVERY write, so the durable floor is monotone non-decreasing regardless of the writer.
///
/// MUTATION: remove the `with_lease_support(max(floor))` stamp in `submit_write` → the vote-grant write,
/// rebuilt from the stale last-durable `hard_state()` (lease None), submits None AFTER the floor's Some →
/// the monotonicity assertion fails.
#[test]
fn lease_floor_never_lowered_by_any_write_under_last_durable_store() {
  use crate::{Index, Message, Term};
  use core::time::Duration;
  let (mut ep, mut log, mut stable) = enforcing_follower(Duration::from_millis(1000));
  stable.set_last_durable_reads(true);
  let now = crate::Instant::ORIGIN;
  // Heartbeat raises the in-memory floor to 1000 and submits the floor write (in flight; durable stays
  // None under last-durable reads).
  let _ = follower_advertised_support(&mut ep, &mut log, &mut stable, now, 5, 1);
  // A higher-term FORCED-TRANSFER RequestVote (transfer bypasses `in_lease`/the fence, so the grant —
  // and its HardState write — actually fires). `on_request_vote` rebuilds HardState from the last-durable
  // `hard_state()` (lease None) WITHOUT stamping lease_support — the choke-point must restore the floor.
  ep.handle_message(
    now,
    &mut log,
    &mut stable,
    3u64,
    Message::RequestVote(crate::RequestVote::new(
      Term::new(9),
      3u64,
      Index::ZERO,
      Term::ZERO,
      false,
      true,
    )),
  );
  // Every submitted write must be monotone non-decreasing in lease_support (None < Some); a Some->None
  // (or Some->smaller) regression means a write erased the durable floor.
  let seq = stable.submitted_lease_supports();
  assert!(
    seq.windows(2).all(|w| w[0] <= w[1]),
    "lease_support floor must never be lowered by any write; got {seq:?}"
  );
  assert!(
    seq.iter().any(|d| *d == Some(Duration::from_millis(1000))),
    "the floor write should have carried Some(1000); got {seq:?}"
  );
}

/// Test: LeaseBased without check_quorum degrades to Safe (all build profiles).
///
/// A leader configured `read_only=LeaseBased` but `check_quorum=false` must
/// NOT confirm the read immediately.  It must behave like Safe: broadcast a
/// heartbeat round and wait for a quorum of acks before emitting ReadState.
/// Construction is infallible and behaves identically in debug and release — the
/// combination is handled by degradation, not rejection.
#[test]
fn lease_based_without_check_quorum_degrades_to_safe() {
  use core::time::Duration;

  // Build a leader with LeaseBased but check_quorum=false (the unsafe combination).
  let cfg = crate::Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_read_only(crate::ReadOnlyOption::LeaseBased)
  .with_check_quorum(false);

  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::NoopStable::default();

  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable);
  ep.handle_storage(d, &mut log, &mut stable);
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResp(crate::VoteResp::new(
      crate::Term::new(1),
      2u64,
      false,
      false,
    )),
  );
  assert!(ep.role().is_leader());
  ep.handle_storage(d, &mut log, &mut stable);
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendResp(crate::AppendResp::new(
      crate::Term::new(1),
      2u64,
      false,
      crate::Index::ZERO,
      crate::Term::ZERO,
      crate::Index::new(1),
    )),
  );
  while ep.poll_event().is_some() {}
  while ep.poll_message().is_some() {}

  let ctx = bytes::Bytes::from_static(b"degraded_lease_read");
  ep.read_index(d, &log, &stable, ctx.clone())
    .expect("leader must accept the read (degraded LeaseBased → Safe)");

  // Must NOT emit ReadState immediately (would be linearizability hazard).
  assert!(
    ep.poll_event().is_none(),
    "LeaseBased without check_quorum must NOT confirm immediately — no ReadState yet"
  );

  // Must have broadcast a read heartbeat (Safe path), carrying the internal round token.
  let mut round = None;
  while let Some(out) = ep.poll_message() {
    if let Message::Heartbeat(hb) = out.message()
      && !hb.context().is_empty()
    {
      round = Some(bytes::Bytes::copy_from_slice(hb.context()));
    }
  }
  let round = round.expect(
    "LeaseBased without check_quorum must fall back to Safe and broadcast a heartbeat round",
  );

  // After a quorum of HeartbeatResp acks (echoing the round token), ReadState is emitted.
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::HeartbeatResp(crate::HeartbeatResp::new(
      crate::Term::new(1),
      2u64,
      round.clone(),
    )),
  );
  while ep.poll_message().is_some() {}

  let ev = ep
    .poll_event()
    .expect("ReadState must be emitted once heartbeat quorum acks");
  assert!(ev.is_read_state(), "expected ReadState");
  let rs = ev.unwrap_read_state_ref();
  assert_eq!(rs.index(), crate::Index::new(1));
  assert_eq!(rs.context().as_ref(), ctx.as_ref());
}

/// Regression (LeaseBased read requires a LIVE lease): with `LeaseBased` + `check_quorum`,
/// the leader may confirm a read immediately ONLY while its quorum-lease window is open. CheckQuorum
/// repurposes `election_deadline` as the lease timer; if the window has lapsed
/// (`election_deadline <= now`) but `handle_timeout` has not yet run for this `now`, the lease is
/// unproven and confirming could serve a read a majority has moved past. Here the leader arms its
/// lease at `d`, then `read_index` is called at `d + 2s` (past the `d + 1s` deadline) BEFORE any
/// timeout — it must degrade to the Safe heartbeat round, not confirm immediately. A subsequent
/// HeartbeatResp quorum then completes the read (liveness preserved).
///
/// MUTATION: drop the `election_deadline > now` conjunct so `use_lease` is `check_quorum()` alone →
/// the leader confirms immediately and the "no immediate ReadState" assertion fails.
#[test]
fn lease_based_expired_lease_degrades_to_safe() {
  use core::time::Duration;
  let cfg = crate::Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_read_only(crate::ReadOnlyOption::LeaseBased)
  .with_check_quorum(true);

  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::NoopStable::default();

  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable);
  ep.handle_storage(d, &mut log, &mut stable);
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResp(crate::VoteResp::new(
      crate::Term::new(1),
      2u64,
      false,
      false,
    )),
  );
  assert!(ep.role().is_leader());
  // The leader armed its quorum lease at `d`: election_deadline = d + election_timeout (1s).
  ep.handle_storage(d, &mut log, &mut stable);
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendResp(crate::AppendResp::new(
      crate::Term::new(1),
      2u64,
      false,
      crate::Index::ZERO,
      crate::Term::ZERO,
      crate::Index::new(1),
    )),
  );
  while ep.poll_event().is_some() {}
  while ep.poll_message().is_some() {}

  // Read at an instant PAST the lease deadline, WITHOUT first calling handle_timeout (so the
  // CheckQuorum tick has not re-confirmed quorum or stepped the leader down).
  let expired = d + Duration::from_millis(2000);
  let ctx = bytes::Bytes::from_static(b"expired_lease_read");
  ep.read_index(expired, &log, &stable, ctx.clone())
    .expect("leader must accept the read (degraded LeaseBased → Safe)");

  // Must NOT confirm immediately — the lease is unproven at this instant.
  assert!(
    ep.poll_event().is_none(),
    "expired LeaseBased lease must NOT confirm immediately — no ReadState yet"
  );
  // Must degrade to Safe: broadcast a read heartbeat round carrying the internal round token.
  let mut round = None;
  while let Some(out) = ep.poll_message() {
    if let Message::Heartbeat(hb) = out.message()
      && !hb.context().is_empty()
    {
      round = Some(bytes::Bytes::copy_from_slice(hb.context()));
    }
  }
  let round =
    round.expect("expired LeaseBased lease must fall back to Safe and broadcast a heartbeat round");

  // A HeartbeatResp quorum (echoing the round token) then completes the read (liveness preserved).
  ep.handle_message(
    expired,
    &mut log,
    &mut stable,
    2u64,
    Message::HeartbeatResp(crate::HeartbeatResp::new(
      crate::Term::new(1),
      2u64,
      round.clone(),
    )),
  );
  while ep.poll_message().is_some() {}
  let ev = ep
    .poll_event()
    .expect("ReadState must be emitted once the Safe heartbeat quorum acks");
  assert!(ev.is_read_state(), "expected ReadState");
  assert_eq!(ev.unwrap_read_state_ref().index(), crate::Index::new(1));
}
