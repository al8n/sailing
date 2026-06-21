use super::{super::*, *};
use crate::{
  Heartbeat, HeartbeatResp, VoteResp,
  testkit::{AsyncStable, CountSm, FailTermLog, NoopLog, NoopStable, VecLog},
};
use core::time::Duration;

#[test]
fn election_timer_is_armed_after_construction() {
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let ep = Endpoint::new(cfg, Instant::ORIGIN, 42, Noop);
  // a fresh follower has an election deadline in (now, now + 2*base]
  let d = ep.poll_timeout().expect("election timer armed");
  assert!(d > crate::Instant::ORIGIN);
}

#[test]
fn election_timeout_starts_a_campaign() {
  use crate::{Config, Instant, Message};
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 42, Noop);
  let mut log = NoopLog;
  let mut stable = NoopStable::default();
  let deadline = ep.poll_timeout().unwrap();
  ep.handle_timeout(deadline, &mut log, &mut stable);
  assert!(ep.role().is_candidate());
  assert_eq!(ep.term(), Term::new(1));
  // two RequestVotes (to peers 2 and 3), each in term 1
  let mut targets = Vec::new();
  while let Some(out) = ep.poll_message() {
    assert!(matches!(out.message(), Message::RequestVote(_)));
    targets.push(out.to());
  }
  targets.sort();
  assert_eq!(targets, std::vec![2u64, 3u64]);
}

/// Regression (election safety at term exhaustion): `Term::next()` saturates at u64::MAX, so
/// a node already at the max term must NOT campaign — that would clear `voted_for` and self-vote in
/// the SAME term (a second vote in a term it may already have voted in → two leaders possible at
/// u64::MAX). A crafted max-term RequestVote pushes the node to term MAX and it grants the vote; a
/// later election timeout must NOT overwrite that vote or make it a candidate.
///
/// MUTATION: drop the `next_term == self.term` guard in `become_candidate` → the timeout overwrites
/// `voted_for` with a self-vote and the node becomes a Candidate in term MAX.
#[test]
fn max_term_node_does_not_campaign_or_double_vote() {
  use crate::{Config, Index, Instant, Message, RequestVote, Term};
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, Noop);
  let mut log = NoopLog;
  let mut stable = NoopStable::default();
  let d = Instant::ORIGIN;

  // A crafted max-term RequestVote pushes this node to term u64::MAX; it grants the vote to node 2.
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::RequestVote(RequestVote::new(
      Term::new(u64::MAX),
      2u64,
      Index::ZERO,
      Term::ZERO,
      false,
      false,
    )),
  );
  ep.handle_storage(d, &mut log, &mut stable);
  assert_eq!(ep.term(), Term::new(u64::MAX), "node adopts the max term");
  assert_eq!(
    ep.voted_for,
    Some(2u64),
    "node granted its term-MAX vote to node 2"
  );
  assert!(ep.role().is_follower());
  while ep.poll_message().is_some() {}

  // An election timeout must NOT let a max-term node campaign (it cannot strictly advance the term).
  let t = ep.poll_timeout().expect("election timer armed");
  ep.handle_timeout(t, &mut log, &mut stable);

  assert!(
    ep.role().is_follower(),
    "a max-term node must not become a candidate"
  );
  assert_eq!(
    ep.term(),
    Term::new(u64::MAX),
    "term must not change (already saturated)"
  );
  assert_eq!(
    ep.voted_for,
    Some(2u64),
    "the term-MAX vote must NOT be overwritten by a self-vote"
  );
  let campaigned = core::iter::from_fn(|| ep.poll_message())
    .any(|o| matches!(o.message(), Message::RequestVote(_)));
  assert!(
    !campaigned,
    "a max-term node must not broadcast RequestVote"
  );
}

#[test]
fn follower_grants_then_rejects_second_candidate() {
  use crate::{Config, Index, Instant, Message, RequestVote, Term};
  use core::time::Duration;
  let cfg = Config::try_new(
    2u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, Noop);
  let mut log = NoopLog;
  // Use AsyncStable so that the VoteResp(grant) is released on handle_storage.
  let mut stable = AsyncStable::default();

  // candidate 1 in term 1, empty log — grant is deferred behind durability
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::RequestVote(RequestVote::new(
      Term::new(1),
      1u64,
      Index::ZERO,
      Term::ZERO,
      false,
      false,
    )),
  );
  // Grant is withheld until the hard-state write is durable.
  assert!(ep.poll_message().is_none(), "no grant before durability");
  // Drain storage → hard-state write completes → grant emitted.
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  let vr = ep.poll_message().unwrap();
  assert!(matches!(vr.message(), Message::VoteResp(v) if !v.reject() && v.from()==2));
  assert_eq!(ep.term(), Term::new(1));

  // candidate 3 in the SAME term — already voted for 1, reject sent immediately
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    3u64,
    Message::RequestVote(RequestVote::new(
      Term::new(1),
      3u64,
      Index::ZERO,
      Term::ZERO,
      false,
      false,
    )),
  );
  let vr = ep.poll_message().unwrap();
  assert!(matches!(vr.message(), Message::VoteResp(v) if v.reject()));
}

/// Regression (same-term leader step-down before a `LeaderAppend` completes): a leader
/// appends entry 1 (LeaderAppend pending), then steps down to follower at the SAME term. When
/// the append completes it hits the `_` arm (role no longer leader), but it still became
/// durable, so `durable_index` must advance via the unconditional advance.
///
/// MUTATION: revert FIX 2 so the advance is only in the arms. Then the post-step-down
/// completion hits `_`, `durable_index` stays at the pre-append value, and the assertion FAILS.
#[test]
fn durable_index_advances_after_same_term_leader_step_down() {
  use crate::{AppendEntries, Config, Index, Instant, Message, Term, VoteResp};
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
  let mut stable = AsyncStable::default();

  // Elect node 1 leader at term 1.
  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable);
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResp(VoteResp::new(Term::new(1), 2u64, false, false)),
  );
  // Self-vote durable → become_leader fires; the no-op append is now in-flight (LeaderAppend).
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(ep.role().is_leader(), "node 1 is leader at term 1");
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}
  // The leader's current-term no-op is pending as a LeaderAppend; durable_index has NOT yet
  // captured it (the completion is still queued in the log).
  let upto_before = ep.durable.durable_index;
  assert!(
    !ep.pending.is_empty(),
    "the leader's no-op append is pending as a LeaderAppend"
  );
  let noop_index = log.last_index();
  assert!(
    noop_index > upto_before,
    "the no-op sits above the durable watermark before it flushes"
  );

  // Step down to follower at the SAME term (1) via an AppendEntries from a same-term peer.
  // (prev_log_index = noop_index, prev_log_term = 1 keeps the log consistent so we step down
  // rather than reject.)
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(1),
      2u64,
      noop_index,
      Term::new(1),
      std::vec![],
      Index::ZERO,
    )),
  );
  assert!(
    ep.role().is_follower(),
    "node 1 stepped down to follower at the same term"
  );
  while ep.poll_message().is_some() {}

  // Drain the no-op append's completion: it now hits the `_` arm (no longer leader), but the
  // append became durable, so the watermark must advance to the no-op index.
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  assert_eq!(
    ep.durable.durable_index, noop_index,
    "the no-op became durable; durable_index advanced despite the same-term step-down (hit `_`)"
  );
}

/// A granted vote must be withheld until the HardState write is durable.
/// Uses `AsyncStable` which releases completions only on `poll`.
#[test]
fn vote_grant_waits_for_durable_hard_state() {
  use crate::{Config, Index, Instant, Message, RequestVote, Term};
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
  let mut stable = AsyncStable::default();

  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::RequestVote(RequestVote::new(
      Term::new(1),
      1u64,
      Index::ZERO,
      Term::ZERO,
      false,
      false,
    )),
  );
  assert!(
    ep.poll_message().is_none(),
    "no grant before the vote is durable"
  );
  // Drain storage → HardState write completes → grant emitted.
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  assert!(
    matches!(ep.poll_message().unwrap().message(), Message::VoteResp(v) if !v.reject()),
    "grant must be emitted after handle_storage"
  );
}

/// Regression (election safety): a candidate must not become leader, or
/// otherwise act on its self-vote, until the term+self-vote hard-state write is DURABLE. The
/// cluster is a single node, so the self-vote alone is a quorum — yet with async storage the
/// leader transition must wait for `StableDone::Wrote`. Without the gate the node leads term 1 on
/// an un-durable self-vote; a crash before that write lands would restart it at term 0 with no
/// vote recorded, free to grant a different candidate in term 1 → two leaders in term 1.
#[test]
fn candidate_does_not_lead_until_self_vote_durable() {
  use crate::{Config, Instant, Term};
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();

  // Campaign at term 1. The self-vote is a single-node quorum, but the hard-state write is in
  // flight (AsyncStable holds the `StableDone::Wrote` until `poll`).
  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable);
  assert_eq!(ep.term(), Term::new(1));
  assert!(
    ep.role().is_candidate() && !ep.role().is_leader(),
    "must not lead while the self-vote write is un-durable"
  );

  // The hard-state write completes → the self-vote is durable → only now is it safe to lead.
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(
    ep.role().is_leader(),
    "leads once the self-vote write is durable"
  );
}

/// Regression (election safety): even when a PEER's grant reaches quorum, the leader transition
/// waits until the candidate's own self-vote write is durable (`on_vote_resp` gates on
/// `self_vote_durable`). Without the gate the peer grant elects the node on an un-durable self-vote.
#[test]
fn quorum_from_peer_vote_waits_for_durable_self_vote() {
  use crate::{Config, Instant, Message, Term, VoteResp};
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
  let mut stable = AsyncStable::default();

  // Campaign at term 1; self-vote write in flight.
  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {} // drain RequestVotes
  assert!(ep.role().is_candidate());

  // Peer 2 grants → 2 of 3 is a quorum. But our own self-vote is not durable yet → must NOT lead.
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResp(VoteResp::new(Term::new(1), 2u64, false, false)),
  );
  assert!(
    ep.role().is_candidate() && !ep.role().is_leader(),
    "quorum met by a peer grant, but the self-vote is not durable: must wait"
  );

  // Self-vote write completes → the deferred quorum elects us.
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(
    ep.role().is_leader(),
    "leads once the self-vote write is durable"
  );
}

/// Regression: A vote grant for term N must NOT be emitted when storage drains
/// if the node has since advanced to a higher term. Without the fix two grants would be
/// emitted — one to candidate 1 (term 5, stale) and one to candidate 3 (term 6) — both
/// stamped term 6, giving two leaders.
#[test]
fn deferred_vote_does_not_leak_across_term_bump() {
  use crate::{Config, Index, Instant, Message, RequestVote, Term};
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
  // AsyncStable: writes complete only when handle_storage / poll is called.
  let mut stable = AsyncStable::default();

  // Step 1: candidate 1 requests a vote in term 5. Follower grants it (deferred).
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::RequestVote(RequestVote::new(
      Term::new(5),
      1u64,
      Index::ZERO,
      Term::ZERO,
      false,
      false,
    )),
  );
  // Grant is withheld — storage not yet drained.
  assert!(
    ep.poll_message().is_none(),
    "no grant before durability (term 5)"
  );

  // Step 2: candidate 3 arrives in term 6. Term pre-pass bumps term, clears pending.
  // on_request_vote then grants 3 and enqueues a NEW CastVote{to:3, term:6}.
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    3u64,
    Message::RequestVote(RequestVote::new(
      Term::new(6),
      3u64,
      Index::ZERO,
      Term::ZERO,
      false,
      false,
    )),
  );
  assert!(
    ep.poll_message().is_none(),
    "no grant before durability (term 6)"
  );

  // Step 3: drain all storage completions (both op1 from term-5 grant and op2 from
  // term-6 step-down write, plus op3 from term-6 grant, all complete here).
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);

  // Step 4: collect all VoteResp messages.
  let mut grants: Vec<(u64, u64)> = Vec::new(); // (from, to/candidate)
  while let Some(out) = ep.poll_message() {
    if let Message::VoteResp(vr) = out.message()
      && !vr.reject()
    {
      // out.to() is the candidate we're replying to
      grants.push((vr.from(), out.to()));
    }
  }

  // There must be AT MOST one grant, and if present it must be to candidate 3 (term 6).
  assert!(
    grants.len() <= 1,
    "double-vote bug: got {} grants (expected at most 1): {:?}",
    grants.len(),
    grants
  );
  if let Some(&(_from, to)) = grants.first() {
    assert_eq!(
      to, 3u64,
      "grant must be to candidate 3 (term-6 vote), not candidate 1 (stale term-5 vote)"
    );
  }
  // There must be exactly one grant (to candidate 3).
  assert_eq!(
    grants.len(),
    1,
    "expected exactly one grant (to candidate 3)"
  );
}

/// Test 3: A non-voter (learner) that has an election timer fire must NOT become a
/// candidate. The term must not change and the role must stay Follower.
#[test]
fn non_voter_does_not_campaign_on_timeout() {
  use core::time::Duration;

  // Node 4 is a learner in {voters: [1,2,3], learners: [4]}.
  // We bootstrap as if 4 is a voter (Config requirement) then manually adjust the Tracker.
  let cfg = Config::try_new(
    4u64,
    std::vec![1u64, 2u64, 3u64, 4u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 99, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = NoopStable::default();

  // Demote node 4 to learner in the Tracker by rebuilding it from a ConfState that has
  // node 4 as a learner, not a voter.
  let learner_cs = crate::ConfState::new([1u64, 2u64, 3u64], [4u64], [], [], false);
  ep.tracker = crate::Tracker::from_conf_state(&learner_cs, Index::ZERO, 256, 0);

  // Sanity: node 4 is NOT a voter.
  assert!(!ep.tracker.is_voter(&4u64), "node 4 must not be a voter");
  assert!(ep.tracker.is_learner(&4u64), "node 4 must be a learner");

  let term_before = ep.term();

  // Arm the election deadline to now (expired).
  ep.election_deadline = Some(Instant::ORIGIN);

  // Fire handle_timeout at now (deadline expired).
  ep.handle_timeout(Instant::ORIGIN, &mut log, &mut stable);

  // Non-voter must NOT have started an election.
  assert!(
    ep.role().is_follower(),
    "non-voter must remain a follower after election timeout"
  );
  assert_eq!(
    ep.term(),
    term_before,
    "non-voter must not bump the term on election timeout"
  );
  // No RequestVote messages emitted.
  assert!(
    ep.poll_message().is_none(),
    "non-voter must not send RequestVote"
  );
}

/// Stepping down to Follower on a higher-term message must ARM a voter's election timer (mirrors
/// etcd's `becomeFollower`). A leader with check_quorum disabled holds `election_deadline = None`; a
/// higher-term RESPONSE (VoteResp / AppendResp — whose handler returns early without arming) would
/// otherwise leave it a voter Follower that can NEVER campaign, wedging the cluster leaderless.
///
/// Before fix: the term pre-pass step-down never armed the timer, so a leader stepping down on a
/// higher-term VoteResp kept `election_deadline = None`.
#[test]
fn step_down_on_higher_term_arms_voter_election_timer() {
  use crate::{Message, Term, VoteResp};

  let (mut ep, mut log, mut stable, d) = make_three_node_leader();
  assert!(ep.role().is_leader(), "precondition: node is the leader");
  // check_quorum is off by default, so a leader holds NO election deadline.
  assert!(
    ep.election_deadline.is_none(),
    "precondition: a leader without check_quorum has no election timer"
  );

  // A higher-term VoteResp — a response whose handler returns early (we are no longer a candidate)
  // without arming — forces the step-down through the term pre-pass.
  let higher = Term::new(ep.term().get() + 5);
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResp(VoteResp::new(higher, 2u64, false, true)),
  );

  assert!(
    ep.role().is_follower(),
    "must step down to Follower on the higher term"
  );
  assert!(
    ep.election_deadline.is_some(),
    "a voter that stepped down must have an ARMED election timer so it can campaign"
  );
}

// ─── PreVote tests ─────────────────────────────────────────────────────────────────────

/// Test 1: A PreCandidate that loses pre-vote stays at the SAME term.
/// A node with pre_vote=true times out → PreCandidate; peers reject (they have a live leader)
/// → the node does NOT advance to Candidate, and self.term is UNCHANGED.
#[test]
fn pre_candidate_loses_stays_at_same_term() {
  use crate::{Config, Instant, Message, Term, VoteResp};
  use core::time::Duration;

  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_pre_vote(true);

  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 42, Noop);
  let mut log = NoopLog;
  let mut stable = NoopStable::default();

  // Trigger the election timer — with pre_vote enabled, node becomes PreCandidate.
  let deadline = ep.poll_timeout().unwrap();
  ep.handle_timeout(deadline, &mut log, &mut stable);
  assert!(ep.role().is_pre_candidate(), "must become PreCandidate");
  assert_eq!(
    ep.term(),
    Term::ZERO,
    "term must NOT be bumped during pre-vote"
  );

  // Drain the RequestVote{pre_vote:true, term:1} messages to peers 2 and 3.
  let mut pre_vote_msgs: Vec<u64> = Vec::new();
  while let Some(out) = ep.poll_message() {
    match out.message() {
      Message::RequestVote(rv) => {
        assert!(rv.pre_vote(), "must be a pre-vote request");
        assert_eq!(
          rv.term(),
          Term::new(1),
          "advertised term must be self.term.next()"
        );
        pre_vote_msgs.push(out.to());
      }
      other => panic!("unexpected message: {other:?}"),
    }
  }
  pre_vote_msgs.sort();
  assert_eq!(
    pre_vote_msgs,
    std::vec![2u64, 3u64],
    "must send pre-vote to both peers"
  );

  // Peers reject: they have a live leader (simulate by sending reject responses at self.term=0).
  // A pre-vote reject carries the responder's term (self.term = 0 here since this is a fresh
  // cluster test; the key invariant is the pre-candidate does NOT advance to Candidate).
  for peer in [2u64, 3u64] {
    ep.handle_message(
      deadline,
      &mut log,
      &mut stable,
      peer,
      Message::VoteResp(VoteResp::new(
        Term::ZERO,
        peer,
        true, /* pre_vote */
        true, /* reject */
      )),
    );
  }

  // Must still be PreCandidate (or return to Follower), NOT Candidate, and term must be 0.
  assert!(
    !ep.role().is_candidate(),
    "pre-candidate that loses must NOT become a real Candidate"
  );
  assert_eq!(
    ep.term(),
    Term::ZERO,
    "term must be unchanged after failed pre-vote"
  );
}

/// A pre-candidate that is BEHIND must adopt the responder's REAL higher term from a pre-vote
/// REJECT. A pre-vote reject carries the responder's current term (not the candidate's advertised
/// one), so it is the candidate's signal that it is stale. Without adopting it, a pair with no third
/// node to bump the term — a 2-voter cluster, or any pair where the peer already self-voted at a
/// higher term — leaves the pre-candidate re-proposing a term the peer keeps rejecting forever: a
/// livelock. (Contrast: a pre-vote GRANT, below, echoes the advertised future term and must NOT be
/// adopted until a quorum lands — the anti-disruption guarantee.)
#[test]
fn pre_vote_reject_at_higher_term_is_adopted() {
  use crate::{Config, Instant, Message, Term, VoteResp};
  use core::time::Duration;

  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2u64], // a 2-voter cluster: no third node can ever bump the term
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_pre_vote(true);

  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 42, Noop);
  let mut log = NoopLog;
  let mut stable = NoopStable::default();

  // Drive to PreCandidate (term 0, pre-voting for term 1) and drain the pre-vote requests.
  let deadline = ep.poll_timeout().unwrap();
  ep.handle_timeout(deadline, &mut log, &mut stable);
  assert!(ep.role().is_pre_candidate(), "must become PreCandidate");
  assert_eq!(ep.term(), Term::ZERO);
  while ep.poll_message().is_some() {}

  // Peer 2 REJECTS the pre-vote, replying at its REAL, higher term 5 (it has moved on / self-voted).
  let higher = Term::new(5);
  ep.handle_message(
    deadline,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResp(VoteResp::new(
      higher, 2u64, true, /* pre_vote */
      true, /* reject */
    )),
  );

  // It must adopt the higher real term and step down — so its NEXT election pre-votes for term 6,
  // high enough to clear the peer's term-5 ballot and finally win. Before the fix it stayed at term
  // 0, re-proposing term 1 the peer rejects forever.
  assert_eq!(
    ep.term(),
    higher,
    "a pre-vote REJECT carries the responder's real higher term and MUST be adopted"
  );
  assert!(
    ep.role().is_follower(),
    "adopting a higher term steps the pre-candidate down to Follower"
  );
}

/// The anti-disruption counterpart: a pre-vote GRANT echoes the candidate's ADVERTISED future term,
/// so a grant SHORT of quorum must NOT raise the receiver's term — only a granted quorum does, via the
/// real campaign. This pins the `!reject` half of the term-adoption condition (a grant is not adopted;
/// a reject is).
#[test]
fn pre_vote_grant_at_higher_term_does_not_raise_term() {
  use crate::{Config, Instant, Message, Term, VoteResp};
  use core::time::Duration;

  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64, 4u64, 5u64], // 5 voters: self + ONE grant = 2, short of the 3-vote quorum
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_pre_vote(true);

  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 42, Noop);
  let mut log = NoopLog;
  let mut stable = NoopStable::default();

  let deadline = ep.poll_timeout().unwrap();
  ep.handle_timeout(deadline, &mut log, &mut stable);
  assert!(ep.role().is_pre_candidate());
  while ep.poll_message().is_some() {}

  // A single pre-vote GRANT carrying the advertised future term 1 — short of the 3-vote quorum.
  ep.handle_message(
    deadline,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResp(VoteResp::new(
      Term::new(1),
      2u64,
      true,  /* pre_vote */
      false, /* grant */
    )),
  );

  assert_eq!(
    ep.term(),
    Term::ZERO,
    "a pre-vote GRANT echoes the advertised term and must not raise our term short of a quorum"
  );
  assert!(
    ep.role().is_pre_candidate(),
    "still PreCandidate: one grant is short of quorum, so no real campaign starts"
  );
}

/// Test 2: A partitioned node's pre-vote requests do NOT cause grantors to adopt the higher
/// advertised term. A follower that receives RequestVote{pre_vote:true, term: self.term+5}
/// must NOT adopt term+5; its term remains unchanged.
#[test]
fn pre_vote_request_does_not_raise_granter_term() {
  use crate::{Config, Index, Instant, Message, RequestVote, Term};
  use core::time::Duration;

  // Follower node 2 with pre_vote=false (it's a stable cluster peer).
  let cfg = Config::try_new(
    2u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 7, Noop);
  let mut log = NoopLog;
  let mut stable = NoopStable::default();

  // Establish a live leader so the lease check blocks the grant.
  // Feed a heartbeat from leader 1 in term 3 — this sets leader=Some(1) and re-arms timer.
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::Heartbeat(Heartbeat::new(
      Term::new(3),
      1u64,
      Index::ZERO,
      bytes::Bytes::new(),
    )),
  );
  while ep.poll_message().is_some() {} // drain HeartbeatResp
  assert_eq!(
    ep.term(),
    Term::new(3),
    "term from heartbeat must be adopted"
  );
  assert_eq!(ep.leader(), Some(1u64), "leader must be known");

  // Now a partitioned node 1 (pre-candidate) sends a pre-vote request at term+5 = 8.
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64, // from
    Message::RequestVote(RequestVote::new(
      Term::new(8), // advertised future term (pre_vote)
      1u64,         // candidate
      Index::ZERO,
      Term::ZERO,
      true,  // pre_vote
      false, // leader_transfer
    )),
  );

  // The node must NOT have adopted term 8.
  assert_eq!(
    ep.term(),
    Term::new(3),
    "pre-vote request must NOT cause the receiver to adopt the advertised term"
  );

  // A response must have been sent (reject, since live leader + healthy election timer).
  let resp = ep.poll_message().expect("must send a VoteResp");
  match resp.message() {
    Message::VoteResp(vr) => {
      assert!(vr.pre_vote(), "response must be a pre-vote response");
      assert!(
        vr.reject(),
        "must reject (live leader + healthy election timer)"
      );
      // Rejection carries self.term (3), not the advertised 8.
      assert_eq!(
        vr.term(),
        Term::new(3),
        "reject response must carry self.term, not the advertised term"
      );
    }
    other => panic!("expected VoteResp, got {other:?}"),
  }
}

/// Test 3: A successful pre-vote quorum transitions to a real Candidate with a term bump
/// and a real RequestVote{pre_vote:false} broadcast.
#[test]
fn successful_pre_vote_quorum_starts_real_campaign() {
  use crate::{Config, Instant, Message, Term, VoteResp};
  use core::time::Duration;

  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_pre_vote(true);

  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 42, Noop);
  let mut log = NoopLog;
  let mut stable = NoopStable::default();

  // Fire election → PreCandidate.
  let deadline = ep.poll_timeout().unwrap();
  ep.handle_timeout(deadline, &mut log, &mut stable);
  assert!(ep.role().is_pre_candidate());
  assert_eq!(ep.term(), Term::ZERO, "term must not bump during pre-vote");
  while ep.poll_message().is_some() {} // drain pre-vote RequestVote msgs

  // Peer 2 grants the pre-vote. Node has no live leader (election timer expired), log
  // is at ZERO (same as ours) → grant. The response carries the advertised term (1).
  ep.handle_message(
    deadline,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResp(VoteResp::new(
      Term::new(1),
      2u64,
      true,  /* pre_vote */
      false, /* grant */
    )),
  );

  // Pre-vote quorum reached (self + peer2 = 2/3 → majority).
  // Node must now be a real Candidate with term bumped to 1.
  assert!(
    ep.role().is_candidate(),
    "must advance to real Candidate after pre-vote quorum"
  );
  assert_eq!(
    ep.term(),
    Term::new(1),
    "term must be bumped on real campaign"
  );

  // Must broadcast real RequestVote{pre_vote:false} to peers.
  let mut real_vote_targets: Vec<u64> = Vec::new();
  while let Some(out) = ep.poll_message() {
    if let Message::RequestVote(rv) = out.message() {
      assert!(!rv.pre_vote(), "real campaign must send pre_vote=false");
      assert_eq!(
        rv.term(),
        Term::new(1),
        "real RequestVote must carry the new term"
      );
      real_vote_targets.push(out.to());
      // Note: other message types (empty-append from become_candidate) are ignored here.
    }
  }
  real_vote_targets.sort();
  assert_eq!(
    real_vote_targets,
    std::vec![2u64, 3u64],
    "real campaign must broadcast to both voter peers"
  );
}

/// Test 4: An up-to-date check still applies to pre-votes. A pre-candidate with a STALE log
/// is rejected even if the lease is open (no live leader).
#[test]
fn pre_vote_rejected_for_stale_log() {
  use crate::{Config, Entry, EntryKind, Index, Instant, Message, RequestVote, Term};
  use core::time::Duration;

  // Follower node 2 with a fresh log (entries up to index 5@term3).
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

  // Seed log with 5 entries in term 3 so our last_log = (5, 3).
  log.force_append(&[
    Entry::new(
      Term::new(3),
      Index::new(1),
      EntryKind::Normal,
      bytes::Bytes::new(),
    ),
    Entry::new(
      Term::new(3),
      Index::new(2),
      EntryKind::Normal,
      bytes::Bytes::new(),
    ),
    Entry::new(
      Term::new(3),
      Index::new(3),
      EntryKind::Normal,
      bytes::Bytes::new(),
    ),
    Entry::new(
      Term::new(3),
      Index::new(4),
      EntryKind::Normal,
      bytes::Bytes::new(),
    ),
    Entry::new(
      Term::new(3),
      Index::new(5),
      EntryKind::Normal,
      bytes::Bytes::new(),
    ),
  ]);

  // No leader known — lease is open. Election timer is expired (use Instant::ORIGIN as now).
  // Pre-vote from node 1 with a STALE log (last_log_index=2, last_log_term=1 < our 5@3).
  // This violates the up-to-date check → must be rejected.
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::RequestVote(RequestVote::new(
      Term::new(4), // advertised term (pre_vote)
      1u64,
      Index::new(2), // stale last_log_index
      Term::new(1),  // stale last_log_term
      true,          // pre_vote
      false,
    )),
  );

  let resp = ep.poll_message().expect("must reply to pre-vote");
  match resp.message() {
    Message::VoteResp(vr) => {
      assert!(vr.pre_vote(), "must be a pre-vote response");
      assert!(
        vr.reject(),
        "must reject pre-vote from a stale-log candidate"
      );
    }
    other => panic!("expected VoteResp, got {other:?}"),
  }
  // The receiver's term must be unchanged (pre-vote never changes term).
  assert_eq!(
    ep.term(),
    Term::ZERO,
    "pre-vote must not change receiver term"
  );
}

/// Test 5: Term pre-pass exemption. A follower receiving RequestVote{pre_vote:true, term:T+5}
/// does NOT adopt T+5. Its term is unchanged, and it replies (grant or reject) immediately
/// without persisting. Specifically: voted_for is not set, and the response is immediate
/// (not deferred behind a storage write).
#[test]
fn term_pre_pass_exemption_for_pre_vote_request() {
  use crate::{Config, Index, Instant, Message, RequestVote, Term};
  use core::time::Duration;

  let cfg = Config::try_new(
    2u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  // Use AsyncStable to confirm that NO storage write is issued for a pre-vote response.
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 7, Noop);
  let mut log = NoopLog;
  let mut stable = AsyncStable::default();

  // Node 2 is at term=0, no known leader, election timer just expired (now=ORIGIN).
  // Receive a pre-vote request at term+5 = 5 from node 1.
  // Log is empty (NoopLog) → log_ok passes (last_log=(0,0) == candidate's).
  // Lease check: no leader known → lease open.
  // term_ok: rv.term()=5 > self.term=0 → passes.
  // All conditions pass → grant.
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::RequestVote(RequestVote::new(
      Term::new(5), // advertised term (T+5)
      1u64,
      Index::ZERO,
      Term::ZERO,
      true, // pre_vote
      false,
    )),
  );

  // CRITICAL: term must NOT have been adopted.
  assert_eq!(
    ep.term(),
    Term::ZERO,
    "pre-vote request must NOT cause receiver to adopt the advertised term T+5"
  );
  // CRITICAL: voted_for must NOT have been set.
  assert!(ep.voted_for.is_none(), "pre-vote must NOT set voted_for");

  // Response must be IMMEDIATE (no persist needed) — it is already in the outgoing queue.
  let resp = ep
    .poll_message()
    .expect("response must be sent immediately, without waiting for storage");
  match resp.message() {
    Message::VoteResp(vr) => {
      assert!(vr.pre_vote(), "must be a pre-vote response");
      // Grant: log_ok + term_ok + lease_open all pass.
      assert!(!vr.reject(), "must grant (log ok, term ok, lease open)");
      // Reply term is the advertised term on grant.
      assert_eq!(
        vr.term(),
        Term::new(5),
        "grant reply must carry the advertised term rv.term()"
      );
    }
    other => panic!("expected VoteResp, got {other:?}"),
  }

  // No storage write must have been submitted (pre-vote grants no-persist invariant).
  // Drain all pending storage → if a write was submitted, AsyncStable would yield it.
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  // No additional messages should appear (a CastVote would have produced a VoteResp here).
  assert!(
    ep.poll_message().is_none(),
    "no additional VoteResp after handle_storage — pre-vote must not persist"
  );
}

// ─── N1: stale-term pre-vote rejection (etcd PreVote fidelity) ───────────────────────────────

/// Regression N1: a follower at term 5 with no voted_for and no live leader receives a
/// pre-vote whose advertised term (3) is BELOW its own term.
///
/// Expected (etcd semantics):
/// - Reply: VoteResp{ pre_vote: true, reject: true, term: 5 } (granter's term in reject)
/// - self.term stays 5
/// - voted_for stays None
///
/// No durable state is touched (pre-vote path).
///
/// Before fix: the `voted_for.is_none()` disjunct in the old `term_ok` incorrectly
/// GRANTED this stale pre-vote (reject: false). The fix adds `rv.term() >= self.term` as
/// a required conjunct so a stale advertised term is rejected regardless of voted_for.
#[test]
fn stale_term_pre_vote_is_rejected() {
  use crate::{Config, Index, Instant, Message, RequestVote, Term};
  use core::time::Duration;

  // Node 2 is a follower at term 5 with no voted_for and no live leader.
  let cfg = Config::try_new(
    2u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 7, Noop);
  let mut log = NoopLog;
  let mut stable = NoopStable::default();

  // Manually set term to 5 (no voted_for, no leader, election timer expired).
  ep.term = Term::new(5);

  // Negative case: stale pre-vote (advertised term 3 < our term 5), up-to-date log.
  // Must be rejected: rv.term() < self.term fails the term_ok >= check.
  ep.handle_message(
    Instant::ORIGIN, // election timer at ORIGIN, so deadline <= now → lease open
    &mut log,
    &mut stable,
    1u64,
    Message::RequestVote(RequestVote::new(
      Term::new(3), // stale advertised term
      1u64,
      Index::ZERO,
      Term::ZERO,
      true, // pre_vote
      false,
    )),
  );

  let resp = ep.poll_message().expect("must reply to stale pre-vote");
  match resp.message() {
    Message::VoteResp(vr) => {
      assert!(vr.pre_vote(), "response must be a pre-vote response");
      assert!(
        vr.reject(),
        "stale-term pre-vote (term 3 < our term 5) must be rejected (N1)"
      );
      assert_eq!(
        vr.term(),
        Term::new(5),
        "reject reply must carry self.term (5) so the pre-candidate learns it is behind"
      );
    }
    other => panic!("expected VoteResp, got {other:?}"),
  }
  // No state mutation: term and voted_for are unchanged.
  assert_eq!(
    ep.term(),
    Term::new(5),
    "self.term must remain 5 after stale pre-vote"
  );
  assert!(ep.voted_for.is_none(), "voted_for must remain None");

  // Positive case: pre-vote with advertised term 6 (> 5), up-to-date log, lease open.
  // Must be granted.
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::RequestVote(RequestVote::new(
      Term::new(6), // rv.term() > self.term → term_ok passes
      1u64,
      Index::ZERO,
      Term::ZERO,
      true, // pre_vote
      false,
    )),
  );

  let resp2 = ep.poll_message().expect("must reply to valid pre-vote");
  match resp2.message() {
    Message::VoteResp(vr) => {
      assert!(vr.pre_vote(), "response must be a pre-vote response");
      assert!(
        !vr.reject(),
        "pre-vote at term 6 > 5, up-to-date, lease open → must grant"
      );
      assert_eq!(
        vr.term(),
        Term::new(6),
        "grant reply must carry the advertised term (6)"
      );
    }
    other => panic!("expected VoteResp, got {other:?}"),
  }
  // Still no state mutation after grant either.
  assert_eq!(
    ep.term(),
    Term::new(5),
    "self.term must remain 5 after granted pre-vote"
  );
  assert!(
    ep.voted_for.is_none(),
    "voted_for must remain None after granted pre-vote"
  );
}

/// Test CQ-1: A leader isolated from a quorum steps down when the CheckQuorum deadline fires.
///
/// Setup: leader of a 3-node cluster. No `recent_active` peers (neither peer 2 nor peer 3
/// has sent any messages). At the CheckQuorum deadline, `quorum_active` is false → step down
/// to Follower (same term, leader=None).
///
/// Conversely: with a quorum active (peer 2 marked), the leader stays and resets the window.
#[test]
fn check_quorum_isolated_leader_steps_down() {
  let cfg = cq_config(1, std::vec![1u64, 2, 3]);
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, Noop);
  let mut log = NoopLog;
  let mut stable = NoopStable::default();

  // Become leader via the normal election path.
  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable); // → Candidate
  ep.handle_storage(d, &mut log, &mut stable);
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResp(VoteResp::new(Term::new(1), 2u64, false, false)),
  );
  assert!(
    ep.role().is_leader(),
    "should be leader after winning election"
  );
  let leader_term = ep.term();

  // Drain all outbound messages (heartbeats, AppendEntries).
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  // The CheckQuorum election_deadline was armed in become_leader.
  // It should be Some (check_quorum is true).
  let cq_deadline = ep
    .election_deadline
    .expect("CQ election_deadline must be armed");

  // No messages received from peers → recent_active is false for peers 2 and 3.
  // Fire the CheckQuorum tick.
  ep.handle_timeout(cq_deadline, &mut log, &mut stable);
  ep.handle_storage(cq_deadline, &mut log, &mut stable);

  // CRITICAL: step down at the SAME term (no term bump).
  assert!(
    ep.role().is_follower(),
    "isolated leader must step down to Follower"
  );
  assert_eq!(
    ep.term(),
    leader_term,
    "step-down must be same-term (no bump)"
  );
  assert!(
    ep.leader().is_none(),
    "leader field must be None after step-down"
  );
  // heartbeat_deadline must be cleared; election timer must be armed (for eventual re-campaign).
  assert!(
    ep.heartbeat_deadline.is_none(),
    "heartbeat_deadline must be cleared after step-down"
  );
  assert!(
    ep.election_deadline.is_some(),
    "election timer must be armed after step-down"
  );
}

/// Test CQ-2: With a quorum active, the leader stays and resets the window.
#[test]
fn check_quorum_active_quorum_stays_leader() {
  let cfg = cq_config(1, std::vec![1u64, 2, 3]);
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, Noop);
  let mut log = NoopLog;
  let mut stable = NoopStable::default();

  // Become leader.
  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable);
  ep.handle_storage(d, &mut log, &mut stable);
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResp(VoteResp::new(Term::new(1), 2u64, false, false)),
  );
  assert!(ep.role().is_leader());
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  let cq_deadline = ep
    .election_deadline
    .expect("CQ election_deadline must be armed");

  // Simulate a HeartbeatResp from peer 2 (marks peer 2 active). Use a time before the
  // CheckQuorum deadline (base + election_timeout / 2 is safely before cq_deadline).
  let before_cq = Instant::ORIGIN + Duration::from_millis(1);
  ep.handle_message(
    before_cq,
    &mut log,
    &mut stable,
    2u64,
    Message::HeartbeatResp(HeartbeatResp::new(Term::new(1), 2u64, bytes::Bytes::new())),
  );
  while ep.poll_message().is_some() {}

  // Peer 2 active + self active = 2 of 3 = quorum. Fire CheckQuorum tick.
  ep.handle_timeout(cq_deadline, &mut log, &mut stable);
  ep.handle_storage(cq_deadline, &mut log, &mut stable);

  // Must remain leader.
  assert!(
    ep.role().is_leader(),
    "leader with active quorum must remain leader"
  );
  // The CheckQuorum window must have been reset (election_deadline re-armed for next window).
  let new_cq_deadline = ep.election_deadline.expect("CQ deadline must be re-armed");
  assert!(
    new_cq_deadline > cq_deadline,
    "re-armed CQ deadline must be in the future"
  );
  // After the reset, peers should be inactive again (except self).
  assert!(
    ep.tracker
      .progress(&2u64)
      .map(|p| !p.recent_active())
      .unwrap_or(false),
    "peer 2 recent_active must be reset to false"
  );
  assert!(
    ep.tracker
      .progress(&1u64)
      .map(|p| p.recent_active())
      .unwrap_or(false),
    "self recent_active must remain true"
  );
}

/// Test CQ-3: `recent_active` is set when the leader receives a message from a peer.
///
/// A leader receiving an AppendResp/HeartbeatResp from a peer marks that peer active.
#[test]
fn check_quorum_recent_active_set_on_inbound_message() {
  let cfg = cq_config(1, std::vec![1u64, 2, 3]);
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, Noop);
  let mut log = VecLog::default();
  let mut stable = NoopStable::default();

  // Become leader.
  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable);
  ep.handle_storage(d, &mut log, &mut stable);
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResp(VoteResp::new(Term::new(1), 2u64, false, false)),
  );
  assert!(ep.role().is_leader());
  // Drain storage (noop write for leader).
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}

  // Initially peer 2 is NOT active.
  assert!(
    !ep
      .tracker
      .progress(&2u64)
      .map(|p| p.recent_active())
      .unwrap_or(true),
    "peer 2 must start inactive"
  );

  // Receive a HeartbeatResp from peer 2.
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::HeartbeatResp(HeartbeatResp::new(Term::new(1), 2u64, bytes::Bytes::new())),
  );

  // Peer 2 must now be active.
  assert!(
    ep.tracker
      .progress(&2u64)
      .map(|p| p.recent_active())
      .unwrap_or(false),
    "peer 2 must be marked active after HeartbeatResp"
  );
}

/// Test CQ-5: `check_quorum=false` default → no CheckQuorum tick, no lease ignore.
///
/// With the default config (check_quorum=false):
/// - A leader's election_deadline is NOT armed (no CheckQuorum window).
/// - A follower does NOT block a higher-term vote request (no lease protection).
#[test]
fn check_quorum_disabled_preserves_m1_m6_behavior() {
  use crate::{Config, Index, Instant, Message, RequestVote, Term};
  use core::time::Duration;

  // --- Part 1: Leader has no CQ election_deadline when check_quorum=false ---
  let cfg_leader = Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  // check_quorum defaults to false
  let mut ep = Endpoint::new(cfg_leader, Instant::ORIGIN, 1, Noop);
  let mut log = NoopLog;
  let mut stable = NoopStable::default();
  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable);
  ep.handle_storage(d, &mut log, &mut stable);
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResp(VoteResp::new(Term::new(1), 2u64, false, false)),
  );
  assert!(ep.role().is_leader(), "should be leader");
  // With check_quorum=false, election_deadline must NOT be armed (arm_heartbeat_timer clears it).
  assert!(
    ep.election_deadline.is_none(),
    "check_quorum=false: election_deadline must not be armed for leader"
  );

  // --- Part 2: Follower with no check_quorum does NOT block higher-term vote ---
  let cfg_follower = Config::try_new(
    2u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  // check_quorum=false AND pre_vote=false
  let base = Instant::ORIGIN;
  let mut ep2 = Endpoint::new(cfg_follower, base, 7, Noop);
  let mut log2 = NoopLog;
  let mut stable2 = NoopStable::default();

  // Give the follower a live leader via Heartbeat.
  ep2.handle_message(
    base,
    &mut log2,
    &mut stable2,
    1u64,
    Message::Heartbeat(Heartbeat::new(
      Term::new(1),
      1u64,
      Index::ZERO,
      bytes::Bytes::new(),
    )),
  );
  while ep2.poll_message().is_some() {}
  while ep2.poll_event().is_some() {}
  assert_eq!(ep2.term(), Term::new(1));
  assert_eq!(ep2.leader(), Some(1u64));

  // A higher-term real vote (non-forced) arrives while the lease *would* apply — but
  // check_quorum=false AND pre_vote=false → lease is NOT active → term IS adopted.
  let now_in_lease = base + Duration::from_millis(50);
  ep2.handle_message(
    now_in_lease,
    &mut log2,
    &mut stable2,
    3u64,
    Message::RequestVote(RequestVote::new(
      Term::new(3),
      3u64,
      Index::ZERO,
      Term::ZERO,
      false, // real vote
      false, // not forced
    )),
  );
  // Without check_quorum or pre_vote, the lease block is inactive → term IS adopted.
  assert_eq!(
    ep2.term(),
    Term::new(3),
    "check_quorum=false: higher-term vote must be processed normally (no lease block)"
  );
}

/// Side-effect-free fail-stop on a higher-term `RequestVote` whose `last_log` read fails.
/// The higher term is adopted in memory and dispatched to `on_request_vote`, which reads `last_log`
/// FIRST; a fatal term-read poisons (`LogTerm`) with NO durable term/vote write — the adopted term
/// is not left on disk by the fail-stop.
///
/// MUTATION: restore the eager `submit_write` in `handle_message`'s higher-term branch → the durable
/// term becomes the vote request's term (5).
#[test]
fn higher_term_request_vote_last_log_failure_poisons_without_persisting_term() {
  use crate::{Config, Entry, EntryKind, Index, Instant, Message, PoisonReason, RequestVote, Term};
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
  // One entry at index 1; arm a fatal term-read at the last index so `last_log` fails.
  let mut log = FailTermLog::default();
  log.force_append(&[Entry::new(
    Term::new(1),
    Index::new(1),
    EntryKind::Empty,
    bytes::Bytes::new(),
  )]);
  log.fail_term_at(Some(Index::new(1)));
  assert_eq!(
    stable.hard_state().term(),
    Term::ZERO,
    "baseline durable term is 0"
  );

  // Higher-term (5) real (non-pre-vote) RequestVote from candidate 1. No known leader → not in
  // lease → the term is adopted and dispatched; `on_request_vote`'s `last_log` read then fails.
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::RequestVote(RequestVote::new(
      Term::new(5),
      1u64,
      Index::new(1),
      Term::new(1),
      false, // pre_vote
      false, // leader_transfer
    )),
  );

  assert!(
    ep.is_poisoned(),
    "a fatal last-log read while granting must poison"
  );
  assert_eq!(ep.poison_reason(), Some(PoisonReason::LogTerm));
  assert_eq!(
    stable.hard_state().term(),
    Term::ZERO,
    "the adopted term must NOT be persisted before the fail-stop (side-effect-free)"
  );
}

/// Side-effect-free fail-stop at election time. `become_candidate` now reads `last_log`
/// BEFORE advancing the term, recording the self-vote, or persisting. A fatal term-read at election
/// time poisons (`LogTerm`) with the durable HardState UNCHANGED — no durable self-vote is left in a
/// term the node never actually campaigned in.
///
/// MUTATION: move the `last_log` read back below the term/self-vote `submit_write` in
/// `become_candidate` → the durable HardState gains (term+1, self) before the poison.
#[test]
fn election_time_last_log_failure_poisons_without_self_vote() {
  use crate::{Config, Entry, EntryKind, Index, Instant, PoisonReason, Term};
  use core::time::Duration;
  // Single-node voter cluster + pre_vote default(false) → the election timeout drives straight into
  // become_candidate (no pre-vote round).
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut stable = NoopStable::default();
  // One entry at index 1; arm a fatal term-read at the last index so `last_log` fails.
  let mut log = FailTermLog::default();
  log.force_append(&[Entry::new(
    Term::new(1),
    Index::new(1),
    EntryKind::Empty,
    bytes::Bytes::new(),
  )]);
  log.fail_term_at(Some(Index::new(1)));
  assert_eq!(stable.hard_state().term(), Term::ZERO);
  assert_eq!(stable.hard_state().vote(), None);

  // Fire the election timeout well past the randomized election timer.
  ep.handle_timeout(
    Instant::ORIGIN + Duration::from_secs(10),
    &mut log,
    &mut stable,
  );

  assert!(
    ep.is_poisoned(),
    "a fatal last-log read at election time must poison"
  );
  assert_eq!(ep.poison_reason(), Some(PoisonReason::LogTerm));
  assert_eq!(
    stable.hard_state().term(),
    Term::ZERO,
    "no term advance persisted before the fail-stop"
  );
  assert_eq!(
    stable.hard_state().vote(),
    None,
    "no durable self-vote left in a term we never campaigned in"
  );
}

/// Leader side: a fatal term-read inside `find_conflict_by_term` while handling a follower
/// reject must short-circuit — the leader must NOT mutate the peer's progress (no `next_index`
/// rewind, no Replicate→Probe flip) and must NOT send a follow-on AppendEntries.
///
/// FAILS-ON-OLD: the old `-> Index` return handed back a fabricated conflict index, so the leader
/// computed `safe_next` (= `min(rejected_prev, conflict+1)`), called `become_probe()` +
/// `set_next_index()`, and `maybe_send_append` on a poisoned node. The peer here is driven to
/// Replicate at next_index=4 first, with the failure armed at the walk's FIRST probe (index 4): the
/// old path would rewind next to 3 and flip the state to Probe — both OBSERVABLE — whereas the fix
/// leaves the full `PeerProgress` untouched.
#[test]
fn find_conflict_by_term_poison_propagation_leader() {
  use crate::{
    AppendResp, Config, Entry, EntryKind, Index, Instant, Message, ProgressState, Term, VoteResp,
  };
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut leader = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut log = FailTermLog::default();
  let mut stable = NoopStable::default();

  // Elect node 1 (term 1, no-op at index 1).
  let d = leader.poll_timeout().unwrap();
  leader.handle_timeout(d, &mut log, &mut stable);
  leader.handle_storage(d, &mut log, &mut stable);
  leader.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResp(VoteResp::new(Term::new(1), 2u64, false, false)),
  );
  assert!(leader.role().is_leader());
  leader.handle_storage(d, &mut log, &mut stable);

  // Seed durable term-1 entries so the leader log is [1@1(noop), 2@1, 3@1, 4@1, 5@1].
  log.force_append(&[
    Entry::new(
      Term::new(1),
      Index::new(2),
      EntryKind::Empty,
      bytes::Bytes::new(),
    ),
    Entry::new(
      Term::new(1),
      Index::new(3),
      EntryKind::Empty,
      bytes::Bytes::new(),
    ),
    Entry::new(
      Term::new(1),
      Index::new(4),
      EntryKind::Empty,
      bytes::Bytes::new(),
    ),
    Entry::new(
      Term::new(1),
      Index::new(5),
      EntryKind::Empty,
      bytes::Bytes::new(),
    ),
  ]);

  // Drive peer 2 to Replicate with match=3, next=4 via a SUCCESS ack at match_index=3.
  leader.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendResp(AppendResp::new(
      Term::new(1),
      2u64,
      false,
      Index::ZERO,
      Term::ZERO,
      Index::new(3),
    )),
  );
  while leader.poll_message().is_some() {}
  while leader.poll_event().is_some() {}
  // The Replicate transition optimistically jumped next to last_index+1 = 6.
  let before = leader.peer_progress(&2u64).expect("peer 2 tracked");
  assert_eq!(
    before.next_index,
    Index::new(6),
    "peer 2 at next_index=6 pre-reject"
  );
  assert!(
    matches!(before.state, ProgressState::Replicate),
    "peer 2 in Replicate pre-reject"
  );

  // Arm a fatal term-read at index 4 (the reject walk's FIRST probe: min(hint_index=4, last=5)=4),
  // then deliver a reject hint (index=4, term=1). `find_conflict_by_term(log, 4, 1)` reads term(4)
  // → Err → poison → `None` → the handler returns before mutating progress or sending. (OLD code
  // would have set next_index = min(rejected_prev=5, conflict+1=5) = 5 and flipped to Probe.)
  log.fail_term_at(Some(Index::new(4)));
  leader.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendResp(AppendResp::new(
      Term::new(1),
      2u64,
      true,
      Index::new(4),
      Term::new(1),
      Index::ZERO,
    )),
  );

  assert!(
    leader.is_poisoned(),
    "fatal term-read in the leader reject walk must poison"
  );
  assert_eq!(leader.poison_reason(), Some(crate::PoisonReason::LogTerm));
  let after = leader.peer_progress(&2u64).expect("peer 2 tracked");
  assert_eq!(
    after.next_index, before.next_index,
    "peer next_index must not be rewound on a poisoned conflict walk"
  );
  assert!(
    matches!(after.state, ProgressState::Replicate),
    "peer state must not flip Replicate→Probe on a poisoned conflict walk"
  );
  assert!(
    leader.poll_message().is_none(),
    "no follow-on AppendEntries may be sent after a poisoned conflict walk"
  );
}

/// Class B regression — sender-authenticity choke-point.
///
/// `handle_message` rejects any message whose self-reported sender (`Message::from()`)
/// disagrees with the transport peer it arrived from. A granting `VoteResp` whose PAYLOAD
/// claims `from = 2` but which actually arrives over the transport from peer `3` must be
/// dropped — so a single hostile peer cannot forge a second node's grant to push a candidate
/// over quorum. The legitimate grant (payload from = 2, transport from = 2) then elects it.
///
/// FAILS-ON-OLD: with the `if msg.from() != from { return; }` choke-point removed, the spoofed
/// grant from peer 3 is tallied as node 2's vote, reaching quorum and electing the candidate
/// before the legitimate grant ever arrives.
#[test]
fn spoofed_sender_vote_resp_is_rejected() {
  use crate::{Config, Instant, Message, Term, VoteResp};
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

  // Node 1 campaigns: become candidate (term 1, self-vote), then make the self-vote durable so
  // the `Campaign` completes (persist-before-act). Mirrors the election-mechanics of
  // `quorum_makes_a_leader_and_heartbeats_follow`.
  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable);
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {} // drain RequestVotes
  assert!(
    ep.role().is_candidate(),
    "node 1 must be a candidate after campaigning"
  );

  // Spoofed grant: PAYLOAD says from = 2 (a peer whose vote WOULD complete quorum), but it
  // arrives over the transport from peer 3. The choke-point must reject it before the tally.
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    3u64,
    Message::VoteResp(VoteResp::new(Term::new(1), 2u64, false, false)),
  );
  assert!(
    ep.role().is_candidate(),
    "a grant whose payload sender (2) disagrees with the transport peer (3) must NOT be \
       counted — the node stays a candidate"
  );
  assert!(
    !ep.role().is_leader(),
    "the spoofed grant must not elect the candidate"
  );

  // Legitimate grant: payload from = 2, transport from = 2 → now quorum (self + 2) → leader.
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResp(VoteResp::new(Term::new(1), 2u64, false, false)),
  );
  assert!(
    ep.role().is_leader(),
    "the legitimate grant from peer 2 must reach quorum and elect the candidate"
  );
}

// --- LeaderChanged event-contract tests ---
// The belief transitions an embedder routes on: every observable change of (term, leader)
// surfaces, INCLUDING to-`None` — leader loss is announced, never inferred from silence.

/// A check-quorum step-down makes a known leader (self) unknown at the SAME term — the
/// embedder sweeping leadership-scoped work must hear it.
#[test]
fn check_quorum_step_down_emits_leader_changed_none() {
  let cfg = cq_config(1, std::vec![1u64, 2, 3]);
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, Noop);
  let mut log = NoopLog;
  let mut stable = NoopStable::default();

  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable);
  ep.handle_storage(d, &mut log, &mut stable);
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResp(VoteResp::new(Term::new(1), 2u64, false, false)),
  );
  assert!(ep.role().is_leader());
  let leader_term = ep.term();
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {} // drain the become-leader LeaderChanged(Some(self))

  let cq_deadline = ep.election_deadline.expect("CQ deadline armed");
  ep.handle_timeout(cq_deadline, &mut log, &mut stable);
  assert!(ep.role().is_follower(), "isolated leader steps down");

  let mut leader_events = Vec::new();
  while let Some(ev) = ep.poll_event() {
    if let Event::LeaderChanged(lc) = ev {
      leader_events.push((lc.term(), lc.leader()));
    }
  }
  assert_eq!(
    leader_events,
    std::vec![(leader_term, None)],
    "the same-term step-down must surface exactly one LeaderChanged(None)"
  );
}

/// A campaign start clears a known leader and bumps the term — the event carries the NEW
/// term with no leader.
#[test]
fn campaign_start_emits_leader_changed_none_at_the_bumped_term() {
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, Noop);
  let mut log = NoopLog;
  let mut stable = NoopStable::default();

  // Learn a leader, then drain its event.
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    2u64,
    Message::Heartbeat(Heartbeat::new(
      Term::new(1),
      2u64,
      Index::ZERO,
      bytes::Bytes::new(),
    )),
  );
  assert_eq!(ep.leader(), Some(2));
  while ep.poll_event().is_some() {}

  // The leader goes silent; the election timeout fires a campaign.
  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable);
  assert!(ep.role().is_candidate());

  let mut leader_events = Vec::new();
  while let Some(ev) = ep.poll_event() {
    if let Event::LeaderChanged(lc) = ev {
      leader_events.push((lc.term(), lc.leader()));
    }
  }
  assert_eq!(
    leader_events,
    std::vec![(Term::new(2), None)],
    "campaign start must announce the lost leader at the bumped term"
  );
}

/// A pre-vote probe clears the leader at the UNCHANGED term; the real campaign that follows
/// a won probe finds the belief already `None` and must NOT re-emit.
#[test]
fn pre_vote_probe_emits_leader_changed_none_once() {
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_pre_vote(true);
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, Noop);
  let mut log = NoopLog;
  let mut stable = NoopStable::default();

  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    2u64,
    Message::Heartbeat(Heartbeat::new(
      Term::new(1),
      2u64,
      Index::ZERO,
      bytes::Bytes::new(),
    )),
  );
  assert_eq!(ep.leader(), Some(2));
  while ep.poll_event().is_some() {}

  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable);
  assert!(ep.role().is_pre_candidate());
  let mut leader_events = Vec::new();
  while let Some(ev) = ep.poll_event() {
    if let Event::LeaderChanged(lc) = ev {
      leader_events.push((lc.term(), lc.leader()));
    }
  }
  assert_eq!(
    leader_events,
    std::vec![(Term::new(1), None)],
    "the probe announces leader loss at the UNBUMPED term"
  );

  // Win the probe: the real campaign bumps the term but the belief is already None —
  // identity dedup means no second event.
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResp(VoteResp::new(Term::new(2), 2u64, true, false)),
  );
  assert!(
    ep.role().is_candidate(),
    "won probe starts the real campaign"
  );
  while let Some(ev) = ep.poll_event() {
    assert!(
      !matches!(ev, Event::LeaderChanged(_)),
      "an unchanged None belief must not re-emit across the term bump"
    );
  }
}

/// A higher-term RequestVote (no lease configured) adopts the term with NO leader — the
/// step-down must say so.
#[test]
fn higher_term_vote_request_emits_leader_changed_none() {
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, Noop);
  let mut log = NoopLog;
  let mut stable = NoopStable::default();

  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    2u64,
    Message::Heartbeat(Heartbeat::new(
      Term::new(1),
      2u64,
      Index::ZERO,
      bytes::Bytes::new(),
    )),
  );
  assert_eq!(ep.leader(), Some(2));
  while ep.poll_event().is_some() {}

  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    3u64,
    Message::RequestVote(crate::RequestVote::new(
      Term::new(5),
      3u64,
      Index::ZERO,
      Term::ZERO,
      false,
      false,
    )),
  );
  assert_eq!(ep.term(), Term::new(5));

  let mut leader_events = Vec::new();
  while let Some(ev) = ep.poll_event() {
    if let Event::LeaderChanged(lc) = ev {
      leader_events.push((lc.term(), lc.leader()));
    }
  }
  assert_eq!(
    leader_events,
    std::vec![(Term::new(5), None)],
    "a leaderless higher-term adoption must announce the unknown leader"
  );
}

/// A higher-term append from a NEW leader surfaces the honest adoption sequence in one
/// drain: `(term, None)` when the term is adopted, then `(term, Some(sender))`.
#[test]
fn higher_term_append_surfaces_the_adoption_pair_in_order() {
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, Noop);
  let mut log = NoopLog;
  let mut stable = NoopStable::default();

  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    2u64,
    Message::Heartbeat(Heartbeat::new(
      Term::new(1),
      2u64,
      Index::ZERO,
      bytes::Bytes::new(),
    )),
  );
  assert_eq!(ep.leader(), Some(2));
  while ep.poll_event().is_some() {}

  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    3u64,
    Message::AppendEntries(crate::AppendEntries::new(
      Term::new(2),
      3u64,
      Index::ZERO,
      Term::ZERO,
      std::vec![],
      Index::ZERO,
    )),
  );

  let mut leader_events = Vec::new();
  while let Some(ev) = ep.poll_event() {
    if let Event::LeaderChanged(lc) = ev {
      leader_events.push((lc.term(), lc.leader()));
    }
  }
  assert_eq!(
    leader_events,
    std::vec![(Term::new(2), None), (Term::new(2), Some(3)),],
    "term adoption then leader installation, in order"
  );
}

/// An unchanged belief never re-emits: the same leader's next heartbeat is event-silent.
#[test]
fn same_leader_reheartbeat_emits_no_leader_changed() {
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, Noop);
  let mut log = NoopLog;
  let mut stable = NoopStable::default();

  let hb = || {
    Message::Heartbeat(Heartbeat::new(
      Term::new(1),
      2u64,
      Index::ZERO,
      bytes::Bytes::new(),
    ))
  };
  ep.handle_message(Instant::ORIGIN, &mut log, &mut stable, 2u64, hb());
  while ep.poll_event().is_some() {}
  ep.handle_message(Instant::ORIGIN, &mut log, &mut stable, 2u64, hb());
  while let Some(ev) = ep.poll_event() {
    assert!(
      !matches!(ev, Event::LeaderChanged(_)),
      "an unchanged leader must not re-emit"
    );
  }
}
