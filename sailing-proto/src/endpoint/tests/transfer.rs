use super::{super::*, *};
use crate::{
  AppendEntries, AppendResponse, ProposeError, TimeoutNow, TransferError, VoteResponse,
  testkit::{CountSm, NoopStable, VecLog},
};

/// A leader transfer revokes LeaseBased read authority: the forced-transfer vote-fence bypass
/// is only safe if the transferring leader actually relinquishes its lease. A leader that arms a
/// transfer authorizes the transferee to become leader (forced campaign), so it must stop serving
/// LeaseBased reads from its old commit — otherwise it could return a stale read while the transferee
/// commits ahead. `transfer_leader` clears the lease, and `do_leader_read` additionally gates the
/// lease shortcut on `lead_transferee.is_none()` (so a heartbeat re-renewing the lease mid-transfer
/// still cannot be used).
///
/// MUTATION: drop the `lead_transferee.is_none()` term from `use_lease` in `do_leader_read` (or the
/// `lease_valid_until = None` clear in `transfer_leader`) → a read during the transfer serves
/// immediately from the lease.
#[test]
fn leader_transfer_revokes_leasebased_read_authority() {
  use crate::{Config, Index, Instant, Message, Term};
  use core::time::Duration;
  let election = Duration::from_millis(1000);
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    election,
    Duration::from_millis(100),
  )
  .unwrap()
  .with_check_quorum(true)
  .with_read_only(ReadOnlyOption::LeaseBased);
  let mut log = VecLog::default();
  let mut stable = NoopStable::default();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());

  // Become leader at term 1 with a current-term commit (campaign → self-vote durable → peer vote →
  // commit the no-op), mirroring `make_leader_with_current_term_commit` but under LeaseBased.
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

  // (1) Live lease, no transfer → a LeaseBased read is served IMMEDIATELY (the lease shortcut works).
  ep.check_quorum_lease.lease_valid_until = Some(d + election);
  ep.read_index(d, &log, &stable, bytes::Bytes::from_static(b"r1"))
    .unwrap();
  assert!(
    ep.poll_all_events_any_read_state(),
    "with a live lease and no transfer, a LeaseBased read emits a ReadState immediately"
  );

  // (2) Arming a transfer REVOKES the lease read authority (immediate clear).
  ep.transfer_leader(d, &log, &stable, 2u64).unwrap();
  assert_eq!(
    ep.check_quorum_lease.lease_valid_until, None,
    "arming a transfer must revoke the read lease"
  );

  // (3) Even if a heartbeat RE-RENEWS the lease during the transfer window, a read must NOT serve from
  //     it — the transferee may already be leader. Re-arm the lease and confirm the read Safe-degrades
  //     (no immediate ReadState; it broadcasts a heartbeat to re-confirm a quorum instead).
  ep.check_quorum_lease.lease_valid_until = Some(d + election);
  while ep.poll_message().is_some() {}
  ep.read_index(d, &log, &stable, bytes::Bytes::from_static(b"r2"))
    .unwrap();
  assert!(
    !ep.poll_all_events_any_read_state(),
    "during a transfer, a LeaseBased read must Safe-degrade — no immediate ReadState"
  );
}

/// A forced handoff disables LeaseBased reads for the rest of the term, even after the transfer
/// aborts): once `TimeoutNow` is sent, the transferee is authorized to campaign FORCED — and under
/// unbounded message delay that campaign (or its already-sent forced `RequestVote`s) can elect a new
/// leader at ANY later point this term, even after the transfer aborts on the deadline. So the old
/// leader must keep LeaseBased reads disabled until re-election, NOT re-enable them when
/// `lead_transferee` clears on abort.
///
/// MUTATION: drop the `!self.transfer.forced_handoff_this_term` term from `use_lease` in `do_leader_read` →
/// after the abort the leader serves an immediate LeaseBased read from a re-renewed lease.
#[test]
fn forced_handoff_disables_leasebased_reads_for_the_term_even_after_abort() {
  use crate::{Config, Index, Instant, Message, Term};
  use core::time::Duration;
  let election = Duration::from_millis(1000);
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    election,
    Duration::from_millis(100),
  )
  .unwrap()
  .with_check_quorum(true)
  .with_read_only(ReadOnlyOption::LeaseBased);
  let mut log = VecLog::default();
  let mut stable = NoopStable::default();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());

  // Become leader at term 1 with a current-term commit (node 2 acks the no-op → match=1).
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

  // Transfer to node 2 (caught up at index 1) → TimeoutNow sent immediately → forced-handoff armed.
  ep.transfer_leader(d, &log, &stable, 2u64).unwrap();
  assert!(
    ep.transfer.forced_handoff_this_term,
    "sending TimeoutNow arms the forced-handoff flag"
  );

  // Abort the transfer on the deadline. Node 2 was recently active (it acked at `d`), so the
  // CheckQuorum check at `after` keeps this node leader; only the transfer is aborted.
  let after = d + election + Duration::from_millis(1);
  ep.handle_timeout(after, &mut log, &mut stable);
  assert!(
    ep.role().is_leader(),
    "leader survives the abort (peer was recently active)"
  );
  assert!(
    ep.transfer.lead_transferee.is_none(),
    "the transfer aborted on the deadline"
  );
  assert!(
    ep.transfer.forced_handoff_this_term,
    "the forced-handoff flag PERSISTS past the abort"
  );

  // Even with the transfer aborted AND the lease re-renewed, a read must NOT serve from the lease:
  // the forced campaign authorized earlier can still elect a new leader at any later point this term.
  ep.check_quorum_lease.lease_valid_until = Some(after + election);
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}
  ep.read_index(after, &log, &stable, bytes::Bytes::from_static(b"r"))
    .unwrap();
  assert!(
    !ep.poll_all_events_any_read_state(),
    "after a TimeoutNow + abort, LeaseBased reads stay disabled for the rest of the term"
  );
}

/// Re-issuing a transfer to the SAME target is idempotent: the second call returns Ok via the
/// `lead_transferee == Some(to)` short-circuit, without re-arming or panicking.
#[test]
fn transfer_to_same_target_is_idempotent() {
  let (mut leader, log, stable) = setup_leader_with_peer2_caught_up();
  leader
    .transfer_leader(Instant::ORIGIN, &log, &stable, 2u64)
    .expect("the first transfer arms");
  assert_eq!(leader.transfer.lead_transferee, Some(2u64));
  leader
    .transfer_leader(Instant::ORIGIN, &log, &stable, 2u64)
    .expect("re-targeting the same node is a no-op success");
  assert_eq!(leader.transfer.lead_transferee, Some(2u64));
}

/// A non-voter (observer) that receives a `TimeoutNow` from its known leader silently ignores it —
/// it can never be elected, so it must NOT campaign (the `is_voter(self)` guard in `on_timeout_now`).
#[test]
fn timeout_now_ignored_by_non_voter() {
  use core::time::Duration;
  let cfg = Config::try_new_observer(
    4u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut obs = Endpoint::new(cfg, Instant::ORIGIN, 7, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = NoopStable::default();
  // Establish leader=1 belief at term 1 via a heartbeat-shaped AppendEntries (so the equal-term
  // TimeoutNow authenticates against the known leader rather than being dropped).
  obs.handle_message(
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
  assert_eq!(obs.leader(), Some(1u64));
  assert!(obs.role().is_follower());
  while obs.poll_message().is_some() {}

  obs.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::TimeoutNow(TimeoutNow::new(Term::new(1), 1u64)),
  );
  assert!(
    obs.role().is_follower(),
    "a non-voter must not campaign on TimeoutNow"
  );
  assert!(
    obs.poll_message().is_none(),
    "an ignored TimeoutNow broadcasts no RequestVote"
  );
}

/// Test 1: transfer_leader to a caught-up follower sends TimeoutNow immediately.
/// When peer 2 receives TimeoutNow it becomes a real Candidate (even with pre_vote=true)
/// and broadcasts RequestVote{leader_transfer:true, pre_vote:false}.
#[test]
fn transfer_to_caught_up_follower_sends_timeout_now_immediately() {
  use core::time::Duration;
  let (mut leader, log, stable) = setup_leader_with_peer2_caught_up();
  // Peer 2 is caught up (match=1, last_index=1): transfer_leader should send TimeoutNow now.
  leader
    .transfer_leader(Instant::ORIGIN, &log, &stable, 2u64)
    .expect("transfer should succeed");

  // Exactly one TimeoutNow to peer 2 must be in the outgoing queue.
  let mut tn_count = 0;
  while let Some(out) = leader.poll_message() {
    if out.to() == 2u64
      && let Message::TimeoutNow(_) = out.message()
    {
      tn_count += 1;
    }
  }
  assert_eq!(tn_count, 1, "exactly one TimeoutNow must be sent to peer 2");

  // Now simulate peer 2 receiving TimeoutNow (with pre_vote=true config, should still
  // do a REAL campaign bypassing PreVote).
  let cfg2 = Config::try_new(
    2u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_pre_vote(true);
  let mut follower = Endpoint::new(cfg2, Instant::ORIGIN, 7, CountSm::default());
  let mut flog = VecLog::default();
  let mut fstable = NoopStable::default();
  // The transfer target must already be a follower of node 1 AT THE LEADER'S TERM for the
  // TimeoutNow to be honored (a TimeoutNow is now authenticated against the current known leader,
  // and a real transfer target is caught up under that leader at its term). Establish leader=1,
  // term=1 via a heartbeat-shaped AppendEntries so the equal-term TimeoutNow does not reset
  // `leader` via higher-term adoption.
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

  // Deliver the TimeoutNow (term=1, from leader=1).
  follower.handle_message(
    Instant::ORIGIN,
    &mut flog,
    &mut fstable,
    1u64,
    Message::TimeoutNow(TimeoutNow::new(Term::new(1), 1u64)),
  );

  // Peer 2 must be a REAL Candidate (not PreCandidate) at term 2.
  assert!(
    follower.role().is_candidate(),
    "TimeoutNow must produce a real Candidate even when pre_vote=true"
  );
  assert_eq!(
    follower.term(),
    Term::new(2),
    "candidate term must be bumped to 2"
  );

  // The RequestVote broadcasts must have pre_vote=false and leader_transfer=true.
  let mut rv_count = 0;
  while let Some(out) = follower.poll_message() {
    if let Message::RequestVote(rv) = out.message() {
      assert!(
        !rv.pre_vote(),
        "TimeoutNow-triggered campaign must be a REAL vote (pre_vote=false)"
      );
      assert!(
        rv.leader_transfer(),
        "TimeoutNow-triggered campaign must set leader_transfer=true"
      );
      rv_count += 1;
    }
  }
  assert!(rv_count > 0, "peer 2 must broadcast RequestVote messages");
}

/// Test 2: transfer_leader to a LAGGING follower does NOT send TimeoutNow yet.
/// TimeoutNow is sent only when on_append_response brings the target to last_index.
#[test]
fn transfer_to_lagging_follower_waits_for_catch_up() {
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

  // Elect node 1.
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
  // Drain storage (no-op append).
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  // Propose a second entry (index 2) to create lag for peer 2.
  ep.propose(d, &mut log, &stable, &bytes::Bytes::from_static(b"x"))
    .unwrap();
  ep.flush_appends(d, &log, &stable);
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  // log.last_index() == 2, but peer 2 match_index == 0 (has NOT acked yet).

  // Initiate transfer to peer 2 (it is lagging).
  ep.transfer_leader(d, &log, &stable, 2u64)
    .expect("transfer should succeed");

  // Must NOT have sent a TimeoutNow yet.
  let mut tn_sent = false;
  while let Some(out) = ep.poll_message() {
    if out.to() == 2u64
      && let Message::TimeoutNow(_) = out.message()
    {
      tn_sent = true;
    }
  }
  assert!(!tn_sent, "TimeoutNow must NOT be sent to a lagging peer");

  // Now simulate peer 2 catching up: ack at match_index=2 (last_index).
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
      Index::new(2), // caught up to last_index=2
    )),
  );

  // Now TimeoutNow MUST have been sent.
  let mut tn_after = false;
  while let Some(out) = ep.poll_message() {
    if out.to() == 2u64
      && let Message::TimeoutNow(_) = out.message()
    {
      tn_after = true;
    }
  }
  assert!(
    tn_after,
    "TimeoutNow must be sent to peer 2 after it catches up"
  );
}

/// Test 3: proposals are refused during transfer and accepted again after abort.
#[test]
fn proposals_refused_during_transfer_allowed_after_abort() {
  use core::time::Duration;
  let (mut ep, mut log, mut stable) = setup_leader_with_peer2_caught_up();

  // Initiate transfer.
  ep.transfer_leader(Instant::ORIGIN, &log, &stable, 2u64)
    .unwrap();

  // Normal propose must be refused.
  let err = ep
    .propose(
      Instant::ORIGIN,
      &mut log,
      &stable,
      &bytes::Bytes::from_static(b"x"),
    )
    .unwrap_err();
  assert!(
    matches!(err, ProposeError::LeaderTransferInProgress),
    "propose must fail with LeaderTransferInProgress during transfer"
  );

  // Conf-change propose must also be refused.
  let cc_err = ep
    .propose_conf_change(
      Instant::ORIGIN,
      &mut log,
      &stable,
      crate::ConfChange::new(crate::ConfChangeType::RemoveNode, 3u64, bytes::Bytes::new()),
    )
    .unwrap_err();
  assert!(
    matches!(cc_err, ProposeError::LeaderTransferInProgress),
    "propose_conf_change must fail with LeaderTransferInProgress during transfer"
  );

  // Advance time past the transfer deadline.
  let deadline = Instant::ORIGIN + Duration::from_millis(1001); // > election_timeout (1000ms)
  ep.handle_timeout(deadline, &mut log, &mut stable);
  ep.handle_storage(deadline, &mut log, &mut stable);
  while ep.poll_message().is_some() {}

  // After abort, propose must succeed again.
  let ok = ep.propose(
    deadline,
    &mut log,
    &stable,
    &bytes::Bytes::from_static(b"after_abort"),
  );
  ep.flush_appends(deadline, &log, &stable);
  assert!(
    ok.is_ok(),
    "propose must succeed after transfer abort; got {ok:?}"
  );
}

/// Test 4: transfer aborts after election timeout with no completion.
#[test]
fn transfer_aborts_on_deadline() {
  use core::time::Duration;
  let (mut ep, mut log, mut stable) = setup_leader_with_peer2_caught_up();

  ep.transfer_leader(Instant::ORIGIN, &log, &stable, 2u64)
    .unwrap();
  // lead_transferee must be set.
  assert!(ep.transfer.lead_transferee.is_some());

  // Fire handle_timeout BEFORE the deadline → still in transfer.
  let before_deadline = Instant::ORIGIN + Duration::from_millis(500);
  ep.handle_timeout(before_deadline, &mut log, &mut stable);
  ep.handle_storage(before_deadline, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  assert!(
    ep.transfer.lead_transferee.is_some(),
    "transfer must still be active before deadline"
  );

  // Fire handle_timeout AFTER the deadline → transfer aborted.
  let after_deadline = Instant::ORIGIN + Duration::from_millis(1001);
  ep.handle_timeout(after_deadline, &mut log, &mut stable);
  ep.handle_storage(after_deadline, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  assert!(
    ep.transfer.lead_transferee.is_none(),
    "transfer must be aborted after deadline"
  );
  assert!(
    ep.transfer.transfer_deadline.is_none(),
    "transfer_deadline must be cleared after abort"
  );

  // Proposals must be accepted again.
  let ok = ep.propose(
    after_deadline,
    &mut log,
    &stable,
    &bytes::Bytes::from_static(b"resumed"),
  );
  ep.flush_appends(after_deadline, &log, &stable);
  assert!(ok.is_ok(), "propose must succeed after abort");
}

/// Test 5: TimeoutNow bypasses PreVote + lease (check_quorum=true, pre_vote=true).
/// The recipient becomes a REAL Candidate (not PreCandidate), bumps its term, and sends
/// RequestVote{leader_transfer:true}. A follower receiving that RequestVote grants it
/// even though the election timer is still healthy (lease bypassed by leader_transfer flag).
#[test]
fn timeout_now_bypasses_prevote_and_lease() {
  use core::time::Duration;

  // Node 2 is the transfer target: pre_vote=true, check_quorum=true.
  let cfg2 = Config::try_new(
    2u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_pre_vote(true)
  .with_check_quorum(true);
  let mut target = Endpoint::new(cfg2, Instant::ORIGIN, 7, CountSm::default());
  let mut tlog = VecLog::default();
  let mut tstable = NoopStable::default();

  // A live leader-1 heartbeat at term 1: this both sets the known leader (so the lease would
  // normally block a vote) AND advances the target to the leader's term, so the equal-term
  // TimeoutNow below is authenticated against `leader == Some(1)` (a higher-term TimeoutNow would
  // instead reset `leader` to None in the term pre-pass). It also arms a healthy election timer.
  target.handle_message(
    Instant::ORIGIN,
    &mut tlog,
    &mut tstable,
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
  assert_eq!(target.leader(), Some(1u64));
  assert_eq!(target.term(), Term::new(1));
  while target.poll_message().is_some() {}

  // Deliver TimeoutNow.
  target.handle_message(
    Instant::ORIGIN,
    &mut tlog,
    &mut tstable,
    1u64,
    Message::TimeoutNow(TimeoutNow::new(Term::new(1), 1u64)),
  );

  // Must be a REAL Candidate (not PreCandidate) despite pre_vote=true.
  assert!(
    target.role().is_candidate(),
    "TimeoutNow must produce Candidate, not PreCandidate"
  );
  assert_eq!(target.term(), Term::new(2), "term must be bumped to 2");

  // All RequestVote messages must have leader_transfer=true and pre_vote=false.
  let mut rv_count = 0;
  while let Some(out) = target.poll_message() {
    if let Message::RequestVote(rv) = out.message() {
      assert!(
        rv.leader_transfer(),
        "RequestVote from TimeoutNow must have leader_transfer=true"
      );
      assert!(
        !rv.pre_vote(),
        "RequestVote from TimeoutNow must have pre_vote=false"
      );
      rv_count += 1;
    }
  }
  assert!(rv_count > 0, "target must broadcast RequestVote messages");

  // Node 3 (a follower with a live leader and healthy election timer) receives the
  // RequestVote{leader_transfer:true}: the lease must NOT block it — it should grant.
  let cfg3 = Config::try_new(
    3u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_pre_vote(true)
  .with_check_quorum(true);
  let mut follower3 = Endpoint::new(cfg3, Instant::ORIGIN, 42, CountSm::default());
  let mut fl3 = VecLog::default();
  let mut fs3 = crate::testkit::AsyncStable::default();

  // Give follower3 a live leader + healthy election timer (same-term as the RequestVote).
  // A real vote from term 2 would normally be blocked by the lease in on_handle_message
  // (RequestVote with term=2 > self.term=1 → term pre-pass would first update term to 2
  // and step down, then on_request_vote grants since voted_for is now None).
  // The CRITICAL test: leader_transfer=true in the higher-term path means the lease guard
  // in the term pre-pass is bypassed, so the request reaches on_request_vote normally.
  follower3.leader = Some(1u64);
  // Make the election timer healthy so the in-lease condition fires if we didn't force it.
  follower3.election_deadline = Some(Instant::ORIGIN + Duration::from_millis(500));

  follower3.handle_message(
    Instant::ORIGIN,
    &mut fl3,
    &mut fs3,
    2u64,
    Message::RequestVote(crate::RequestVote::new(
      Term::new(2), // higher term
      2u64,
      Index::ZERO,
      Term::ZERO,
      false, // real vote
      true,  // leader_transfer — must bypass lease
    )),
  );
  // Drain storage (AsyncStable releases CastVote completion on handle_storage).
  follower3.handle_storage(Instant::ORIGIN, &mut fl3, &mut fs3);

  // follower3 must have granted the vote (not rejected it due to the lease).
  let mut granted = false;
  while let Some(out) = follower3.poll_message() {
    if let Message::VoteResponse(vr) = out.message()
      && !vr.reject()
    {
      granted = true;
    }
  }
  assert!(
    granted,
    "follower3 must grant the leader-transfer RequestVote despite live leader + healthy timer"
  );
}

/// Test 6: transfer_leader to a learner/non-voter is rejected with NotAVoter.
#[test]
fn transfer_to_learner_rejected() {
  use core::time::Duration;
  // Create a cluster where node 4 is a learner (not a voter).
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

  // Elect node 1 as leader.
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

  // Node 4 is not in the voter set — transfer must fail with NotAVoter.
  let err = ep.transfer_leader(d, &log, &stable, 4u64).unwrap_err();
  assert!(
    matches!(err, TransferError::NotAVoter),
    "transfer to non-voter must fail with NotAVoter; got {err:?}"
  );

  // Transferring to self must fail with AlreadyLeader.
  let err2 = ep.transfer_leader(d, &log, &stable, 1u64).unwrap_err();
  assert!(
    matches!(err2, TransferError::AlreadyLeader),
    "transfer to self must fail with AlreadyLeader; got {err2:?}"
  );

  // Non-leader can't initiate transfer at all.
  let cfg_follower = Config::try_new(
    2u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut follower = Endpoint::new(cfg_follower, Instant::ORIGIN, 5, CountSm::default());
  let err3 = follower
    .transfer_leader(d, &log, &stable, 3u64)
    .unwrap_err();
  assert!(
    matches!(err3, TransferError::NotLeader { .. }),
    "non-leader transfer_leader must fail with NotLeader; got {err3:?}"
  );
}

/// Test 7: Removing the transfer target via a conf change aborts the in-flight
/// transfer immediately — proposals must resume without waiting for the deadline.
///
/// Scenario: node 1 is leader of {1, 2, 3}; transfer to node 2 is in flight; then
/// RemoveNode(2) is committed+applied. After apply:
///   - `lead_transferee` must be `None`
///   - `transfer_deadline` must be `None`
///   - a subsequent `propose` must SUCCEED (not `LeaderTransferInProgress`)
#[test]
fn transfer_aborted_when_transferee_removed_by_conf_change() {
  use crate::{AppendResponse, ConfChange, ConfChangeType, Index, Message, ProposeError, Term};
  use core::time::Duration;

  let (mut ep, mut log, mut stable, d) = make_three_node_leader();
  // `d` is the Instant at which the election fired (the value returned by poll_timeout
  // before the election).  All time offsets are anchored to `d` so that the
  // transfer_deadline arithmetic (deadline = now + election_timeout = d + 1000ms) is
  // consistent regardless of what randomised value poll_timeout produced.

  // Start leader transfer to node 2 (caught-up: match=1, last=1 → TimeoutNow sent now).
  ep.transfer_leader(d, &log, &stable, 2u64)
    .expect("transfer_leader must succeed");
  assert!(
    ep.transfer.lead_transferee == Some(2u64),
    "lead_transferee must be Some(2) after transfer_leader"
  );
  // Drain the outgoing TimeoutNow.
  while ep.poll_message().is_some() {}

  // Proposals must be blocked while the transfer is in flight.
  let blocked = ep
    .propose(d, &mut log, &stable, &bytes::Bytes::from_static(b"blocked"))
    .unwrap_err();
  assert!(
    matches!(blocked, ProposeError::LeaderTransferInProgress),
    "propose must fail with LeaderTransferInProgress during transfer; got {blocked:?}"
  );

  // Strategy: abort the in-flight transfer via its deadline (so we can re-issue
  // propose_conf_change without the LeaderTransferInProgress guard firing), propose
  // RemoveNode(2), then re-start the transfer to node 2 (still a voter at that point),
  // and finally commit+apply the RemoveNode.  The fix must abort the re-started transfer
  // when the conf-change is applied, well before its own deadline.

  // Advance time past `d + election_timeout` to trigger the deadline abort.
  let past_first_deadline = d + Duration::from_millis(1001); // > election_timeout (1000ms)
  ep.handle_timeout(past_first_deadline, &mut log, &mut stable);
  ep.handle_storage(past_first_deadline, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  assert!(
    ep.transfer.lead_transferee.is_none(),
    "deadline abort must clear lead_transferee"
  );

  // Propose RemoveNode(2) (no transfer in flight — allowed).
  let cc = ConfChange::new(ConfChangeType::RemoveNode, 2u64, bytes::Bytes::new());
  let cc_idx = ep
    .propose_conf_change(past_first_deadline, &mut log, &stable, cc)
    .expect("propose_conf_change(RemoveNode(2)) must succeed");
  // Drain self-match (leader writes the ConfChange entry).
  ep.handle_storage(past_first_deadline, &mut log, &mut stable);
  while ep.poll_message().is_some() {}

  // Re-start a transfer to node 2 (still a voter until the conf change is applied).
  ep.transfer_leader(past_first_deadline, &log, &stable, 2u64)
    .expect("transfer_leader to node 2 (still a voter) must succeed");
  assert!(
    ep.transfer.lead_transferee == Some(2u64),
    "lead_transferee must be node 2 for the re-started transfer"
  );
  while ep.poll_message().is_some() {}

  // Proposals must be blocked again (new transfer in flight).
  let blocked2 = ep
    .propose(
      past_first_deadline,
      &mut log,
      &stable,
      &bytes::Bytes::from_static(b"blocked2"),
    )
    .unwrap_err();
  assert!(
    matches!(blocked2, ProposeError::LeaderTransferInProgress),
    "propose must be blocked by re-started transfer; got {blocked2:?}"
  );

  // Commit the RemoveNode(2): peer 3 acks up to cc_idx (quorum = leader + peer 3 = 2/3).
  // Leader self-match already happened via handle_storage above.
  ep.handle_message(
    past_first_deadline,
    &mut log,
    &mut stable,
    3u64,
    Message::AppendResponse(AppendResponse::new(
      Term::new(1),
      3u64,
      false,
      Index::ZERO,
      Term::ZERO,
      cc_idx,
    )),
  );
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  // After the conf change applies: the transfer must have been aborted immediately.
  assert!(
    ep.transfer.lead_transferee.is_none(),
    "lead_transferee must be None after the transferee is removed by conf change"
  );
  assert!(
    ep.transfer.transfer_deadline.is_none(),
    "transfer_deadline must be None after transfer aborted on conf-change apply"
  );

  // Proposals must resume immediately — no need to wait for the transfer deadline.
  let ok = ep.propose(
    past_first_deadline,
    &mut log,
    &stable,
    &bytes::Bytes::from_static(b"resumed"),
  );
  ep.flush_appends(past_first_deadline, &log, &stable);
  assert!(
    ok.is_ok(),
    "propose must succeed immediately after transferee is removed; got {ok:?}"
  );
}

/// A forced leader-transfer (`TimeoutNow`) is honored ONLY from this node's current known
/// leader. A `TimeoutNow` from any other (authentic-but-non-leader) peer must be ignored — it must
/// NOT start the disruptive, lease-bypassing forced campaign — while one from the real leader still
/// triggers it.
///
/// FAILS-ON-OLD: without the `self.leader != Some(tn.leader())` guard, the non-leader `TimeoutNow`
/// makes the node a real Candidate at a bumped term (a wrong peer disrupting the cluster).
#[test]
fn timeout_now_is_authenticated_against_current_leader() {
  use crate::{AppendEntries, Index, Term};
  use core::time::Duration;
  let cfg = Config::try_new(
    2u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_pre_vote(true)
  .with_check_quorum(true);
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 7, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = NoopStable::default();

  // Establish leader node 1 at term 1 via a heartbeat-shaped AppendEntries (so the node is a real
  // follower at term 1, not a fresh term-0 node — then an equal-term TimeoutNow triggers no term
  // adoption in the pre-pass and we isolate the campaign-suppression).
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
  assert_eq!(ep.term(), Term::new(1));
  while ep.poll_message().is_some() {}

  // (a) A TimeoutNow from a NON-leader peer (node 3) at the SAME term must be IGNORED: no campaign,
  // term unchanged, still a follower, and no RequestVote emitted.
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    3u64,
    Message::TimeoutNow(TimeoutNow::new(Term::new(1), 3u64)),
  );
  assert_eq!(
    ep.role(),
    Role::Follower,
    "a TimeoutNow from a non-leader must not start a campaign"
  );
  assert_eq!(
    ep.term(),
    Term::new(1),
    "an ignored same-term TimeoutNow must not bump the term"
  );
  assert!(
    ep.poll_message().is_none(),
    "an ignored TimeoutNow must emit no RequestVote"
  );

  // (b) A TimeoutNow from the CURRENT leader (node 1) still triggers the forced campaign: real
  // Candidate, term bumped, leader_transfer RequestVotes broadcast.
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::TimeoutNow(TimeoutNow::new(Term::new(1), 1u64)),
  );
  assert!(
    ep.role().is_candidate(),
    "a TimeoutNow from the current leader must start a real campaign"
  );
  assert_eq!(
    ep.term(),
    Term::new(2),
    "the forced campaign bumps the term"
  );
  let mut saw_transfer_vote = false;
  while let Some(out) = ep.poll_message() {
    if let Message::RequestVote(rv) = out.message() {
      assert!(!rv.pre_vote(), "forced campaign is a real vote");
      assert!(rv.leader_transfer(), "forced campaign sets leader_transfer");
      saw_transfer_vote = true;
    }
  }
  assert!(
    saw_transfer_vote,
    "the leader-authorized TimeoutNow must broadcast RequestVote"
  );
}

/// A leader-transfer target that catches up via an `InstallSnapshot` (not `AppendEntries`) must still
/// be sent `TimeoutNow`: its match jumps to `last_index` on the snapshot ack and never advances again,
/// so the append-path trigger alone would never fire and the transfer would silently abort at its
/// deadline.
///
/// MUTATION: remove the `maybe_hand_off_to_transferee` call from `on_snapshot_response` → no
/// `TimeoutNow` is sent on the snapshot ack, so `forced_handoff_this_term` stays false.
#[test]
fn transfer_to_a_snapshot_target_sends_timeout_now() {
  use crate::{Index, Message, SnapshotResponse, Term};

  let (mut ep, mut log, mut stable, d) = make_three_node_leader();
  // Advance the log tip to 2 so node 2 (match 1) is genuinely behind, then force it into Snapshot
  // state (behind the compaction horizon). Its match stays at 1 < last_index 2.
  ep.propose(d, &mut log, &stable, &bytes::Bytes::from_static(b"x"))
    .expect("leader accepts the proposal");
  assert_eq!(log.last_index(), Index::new(2));
  ep.tracker
    .progress_mut(&2u64)
    .unwrap()
    .become_snapshot(Index::new(2), 9);

  // Transfer to node 2. It is NOT caught up (match 1 < last_index 2), so no TimeoutNow yet — the
  // transfer waits with node 2 as the pending transferee.
  ep.transfer_leader(d, &log, &stable, 2u64).unwrap();
  assert!(
    !ep.transfer.forced_handoff_this_term,
    "no TimeoutNow before the target catches up"
  );
  assert_eq!(
    ep.transfer.lead_transferee,
    Some(2u64),
    "node 2 is the pending transferee"
  );
  while ep.poll_message().is_some() {}

  // Node 2 installs the snapshot and acks a match at the log tip (index 2) — via the SNAPSHOT path.
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::SnapshotResponse(SnapshotResponse::new(
      Term::new(1),
      2u64,
      false,
      Index::new(2),
    )),
  );

  // The handoff fired: TimeoutNow was sent (which arms the forced-handoff flag).
  assert!(
    ep.transfer.forced_handoff_this_term,
    "a transferee that caught up via snapshot must be sent TimeoutNow"
  );
}
