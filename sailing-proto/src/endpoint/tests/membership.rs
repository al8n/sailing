use super::{super::*, *};
use crate::{
  ConfChangeSingle, ConfChangeV2, VoteResponse,
  testkit::{CountSm, NoopStable, VecLog},
};

/// At the log-index ceiling, `propose_conf_change` is refused with `LogIndexExhausted` rather than
/// aliasing the entry at `u64::MAX` (the `append_conf_change` → `next_log_index` None path).
#[test]
fn propose_conf_change_refused_at_index_ceiling() {
  use crate::{ConfChange, ConfChangeType, Index, LogStore as _, ProposeError};
  let (mut ep, mut log, stable, d) = make_single_node_leader();
  log.restore(Index::new(u64::MAX), Term::new(1));
  assert_eq!(log.last_index(), Index::new(u64::MAX));
  let cc = ConfChange::new(ConfChangeType::AddNode, 2u64, bytes::Bytes::new());
  assert_eq!(
    ep.propose_conf_change(d, &mut log, &stable, cc),
    Err(ProposeError::LogIndexExhausted)
  );
}

/// Test 1: One-in-flight refusal.
/// A second `propose_conf_change` before the first is applied → `ConfChangeInFlight`.
/// After apply, a new one is accepted.
#[test]
fn conf_change_in_flight_refusal() {
  use crate::{ConfChange, ConfChangeType, ProposeError};
  let (mut ep, mut log, mut stable, d) = make_single_node_leader();

  // First conf-change: AddNode(2). Should succeed.
  let cc1 = ConfChange::new(ConfChangeType::AddNode, 2u64, bytes::Bytes::new());
  let idx1 = ep
    .propose_conf_change(d, &mut log, &stable, cc1)
    .expect("first conf change must be accepted");
  assert!(idx1 > Index::ZERO);

  // Second conf-change before first is applied: must be refused.
  let cc2 = ConfChange::new(ConfChangeType::AddNode, 3u64, bytes::Bytes::new());
  let err = ep
    .propose_conf_change(d, &mut log, &stable, cc2.clone())
    .expect_err("second conf change must be refused while first is in flight");
  assert_eq!(
    err,
    ProposeError::ConfChangeInFlight,
    "expected ConfChangeInFlight error"
  );

  // Drive the first conf-change to committed+applied (single-node cluster: self-quorum).
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_event().is_some() {}
  while ep.poll_message().is_some() {}

  // Now a new conf-change is accepted.
  let cc3 = ConfChange::new(ConfChangeType::AddNode, 3u64, bytes::Bytes::new());
  let idx3 = ep.propose_conf_change(d, &mut log, &stable, cc3);
  assert!(idx3.is_ok(), "conf change must be accepted after apply");
}

/// Test 2: Simple AddNode applies at commit time.
///
/// Invariants verified:
/// - Tracker is updated ONLY at apply time (not at propose time).
/// - `Event::ConfChanged` is emitted carrying the new `ConfState`.
/// - `F::apply` is NOT called for the ConfChange entry (SM apply-count unchanged).
#[test]
fn simple_add_node_applies_at_commit() {
  use crate::{ConfChange, ConfChangeType};
  let (mut ep, mut log, mut stable, d) = make_single_node_leader();

  let sm_count_before = ep.state_machine().count();

  // Propose AddNode(2) — must NOT immediately change the Tracker.
  let cc = ConfChange::new(ConfChangeType::AddNode, 2u64, bytes::Bytes::new());
  let _idx = ep
    .propose_conf_change(d, &mut log, &stable, cc)
    .expect("propose AddNode must succeed");

  // Tracker must still only have voter 1 — not yet at commit time.
  assert!(
    !ep.tracker.is_voter(&2u64),
    "AddNode must NOT take effect before commit"
  );

  // Drive to committed+applied (single-node: self-quorum on storage drain).
  ep.handle_storage(d, &mut log, &mut stable);

  // Now the Tracker must have node 2 as a voter.
  assert!(
    ep.tracker.is_voter(&2u64),
    "AddNode must take effect after apply"
  );

  // SM apply-count must NOT have increased (ConfChange does not call F::apply).
  assert_eq!(
    ep.state_machine().count(),
    sm_count_before,
    "F::apply must NOT be called for a ConfChange entry"
  );

  // An Event::ConfChanged must have been emitted.
  let events: Vec<_> = core::iter::from_fn(|| ep.poll_event()).collect();
  let conf_changed: Vec<_> = events.iter().filter(|e| e.is_conf_changed()).collect();
  assert!(
    !conf_changed.is_empty(),
    "Event::ConfChanged must be emitted when AddNode is applied"
  );
  // The ConfState must contain voter 2.
  if let Event::ConfChanged(cc_ev) = conf_changed[0] {
    assert!(
      cc_ev.conf().is_voter(&2u64),
      "ConfChanged event must carry a ConfState with voter 2"
    );
  }
}

/// Test 3: Simple RemoveNode applies at commit time.
#[test]
fn simple_remove_node_applies_at_commit() {
  use crate::{ConfChange, ConfChangeType};
  // Start with a 2-voter cluster (1, 2), single-node leader at id=1.
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 42, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = NoopStable::default();

  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable); // become candidate
  ep.handle_storage(d, &mut log, &mut stable);
  // Self-vote is enough if quorum=1 among {1,2} with only self-vote — but actually 2-voter
  // quorum=2. We need to hand-grant ourselves leadership via a VoteResponse.
  use crate::{Message, Term, VoteResponse};
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResponse(VoteResponse::new(Term::new(1), 2u64, false, false)),
  );
  assert!(ep.role().is_leader(), "node 1 must be leader");
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_event().is_some() {}
  while ep.poll_message().is_some() {}

  // Also need to advance commit for the no-op entry. The 2-voter quorum requires peer ack.
  // Simulate peer 2 acking the no-op.
  use crate::{AppendResponse, Index};
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
      Index::new(1), // ack no-op at index 1
    )),
  );
  while ep.poll_event().is_some() {}
  while ep.poll_message().is_some() {}

  // Node 2 must be a voter initially.
  assert!(
    ep.tracker.is_voter(&2u64),
    "node 2 must be a voter before remove"
  );

  // Propose RemoveNode(2).
  let cc = ConfChange::new(ConfChangeType::RemoveNode, 2u64, bytes::Bytes::new());
  let _idx = ep
    .propose_conf_change(d, &mut log, &stable, cc)
    .expect("propose RemoveNode must succeed");

  // Not yet applied — node 2 still a voter.
  assert!(
    ep.tracker.is_voter(&2u64),
    "RemoveNode must NOT take effect before commit"
  );

  // Drive to commit: need quorum. Peer 2 acks the ConfChange entry at index 2.
  ep.handle_storage(d, &mut log, &mut stable); // leader self-match → 2
  // Peer 2 acks up to index 2 → quorum of {1,2} → commit.
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
      Index::new(2), // ack ConfChange at index 2
    )),
  );

  // Node 2 must now be gone from voters.
  assert!(
    !ep.tracker.is_voter(&2u64),
    "RemoveNode must take effect after apply"
  );

  // ConfChanged event.
  let events: Vec<_> = core::iter::from_fn(|| ep.poll_event()).collect();
  assert!(
    events.iter().any(|e| e.is_conf_changed()),
    "Event::ConfChanged must be emitted when RemoveNode is applied"
  );
}

/// Test 4: Non-leader refused.
#[test]
fn non_leader_conf_change_is_refused() {
  use crate::{ConfChange, ConfChangeType, ProposeError};
  use core::time::Duration;
  let cfg = Config::try_new(
    2u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut log = VecLog::default();
  let stable = NoopStable::default();

  assert!(ep.role().is_follower());
  let cc = ConfChange::new(ConfChangeType::AddNode, 4u64, bytes::Bytes::new());
  let err = ep
    .propose_conf_change(Instant::ORIGIN, &mut log, &stable, cc)
    .expect_err("follower must refuse propose_conf_change");
  assert!(
    matches!(err, ProposeError::NotLeader { .. }),
    "expected NotLeader error, got {err:?}"
  );
}

/// Regression: a freshly-elected leader must not accept a new ConfChange while an inherited
/// one is uncommitted.
///
/// Scenario: node 2 is a follower that receives a ConfChange entry from leader 1 but the
/// entry is NOT committed (leader_commit stays at 0). Node 2 then wins an election and
/// becomes leader. Its log contains an uncommitted ConfChange at index 2 (the inherited tail).
/// The one-in-flight guard must fire and refuse a second ConfChange proposal.
///
/// On the OLD code (before the fix): `pending_conf_index` was ZERO on a fresh leader, so
/// `ZERO > applied` is false and the second ConfChange was wrongly accepted → Ok(_).
/// On the FIXED code: `become_leader` sets `pending_conf_index = last_index` (= 2), so
/// `2 > applied(0)` is true → Err(ConfChangeInFlight).
#[test]
fn inherited_uncommitted_conf_change_blocks_new_proposal() {
  use crate::{
    AppendEntries, ConfChange, ConfChangeType, Entry, EntryKind, Index, Message, ProposeError,
    Term, VoteResponse,
  };
  use core::time::Duration;

  // Node 2 is a follower in a 3-voter cluster {1, 2, 3}.
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

  // Step 1: Leader 1 (term 1) sends node 2 an AppendEntries carrying:
  //   - index 1: the leader's no-op (Empty entry)
  //   - index 2: a ConfChange entry (AddNode 4)
  // leader_commit = 0 → neither entry is committed on node 2.
  let cc_payload = {
    let cc = ConfChange::new(ConfChangeType::AddNode, 4u64, bytes::Bytes::new()).into_v2();
    let mut buf = Vec::new();
    crate::wire::encode_conf_change_v2(&cc, &mut buf);
    bytes::Bytes::from(buf)
  };
  let noop = Entry::new(
    Term::new(1),
    Index::new(1),
    EntryKind::Empty,
    bytes::Bytes::new(),
  );
  let conf_entry = Entry::new(
    Term::new(1),
    Index::new(2),
    EntryKind::ConfChange,
    cc_payload,
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
      std::vec![noop, conf_entry],
      Index::ZERO, // leader_commit = 0: nothing committed
    )),
  );
  // Drain the deferred append completion so entries are in the log.
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  while ep.poll_message().is_some() {}

  // Verify: log holds entries at indices 1 and 2; applied and commit are still 0.
  assert_eq!(
    log.last_index(),
    Index::new(2),
    "follower log must hold both entries"
  );
  assert_eq!(ep.applied, Index::ZERO, "nothing applied yet");
  assert_eq!(ep.commit, Index::ZERO, "nothing committed yet");

  // Step 2: A term advance causes node 2 to become a candidate in term 2 and win.
  // Under APPLY-TIME membership (etcd, spec §9), the inherited AddNode(4) at index 2 is UNCOMMITTED,
  // so node 2's config is still {1,2,3} — the change does not take effect until it commits-and-applies.
  // A majority of three is two, so a single peer grant (self + 3) elects node 2.
  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable); // become candidate, term 2
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(ep.role().is_candidate());
  while ep.poll_message().is_some() {}

  // Node 3 grants the vote → self + 3 = two of {1,2,3} → quorum → become_leader.
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    3u64,
    Message::VoteResponse(VoteResponse::new(Term::new(2), 3u64, false, false)),
  );
  assert!(ep.role().is_leader(), "node 2 must be leader after quorum");

  // Step 3: Now call propose_conf_change(AddNode(5)).
  // The inherited tail (index 2: uncommitted ConfChange) must block this.
  // The fix sets pending_conf_index = last (= 2) in become_leader; applied = 0;
  // so 2 > 0 is true → ConfChangeInFlight.
  let cc_new = ConfChange::new(ConfChangeType::AddNode, 5u64, bytes::Bytes::new());
  let result = ep.propose_conf_change(d, &mut log, &stable, cc_new);
  assert_eq!(
    result,
    Err(ProposeError::ConfChangeInFlight),
    "a freshly-elected leader must refuse a new ConfChange while an inherited one is \
       uncommitted"
  );
}

/// Regression: a committed ConfChange that the Changer rejects must poison the node
/// rather than silently stalling apply.
///
/// Scenario: node 2 (follower) receives an AppendEntries that carries a leave-joint
/// ConfChange entry and commits it (leader_commit covers it). The node is NOT in joint
/// config, so Changer::leave_joint returns Err. The fix adds `self.poison()` in that
/// branch so the failure is observable rather than a silent apply stall.
#[test]
fn changer_error_at_apply_poisons_node() {
  use crate::{AppendEntries, Entry, EntryKind, Index, Message, Term};
  use core::time::Duration;

  // Node 2 is a follower in a 3-voter cluster {1, 2, 3}.
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

  // Build a leave-joint ConfChange payload. The node is not in joint config, so
  // when this entry commits the Changer will return Err(NotInJointConfig).
  let leave_payload = {
    let cc = ConfChangeV2::<u64>::leave_joint();
    let mut buf = Vec::new();
    crate::wire::encode_conf_change_v2(&cc, &mut buf);
    bytes::Bytes::from(buf)
  };

  // Leader 1 (term 1) sends two entries: a no-op and the bad leave-joint ConfChange.
  // leader_commit = 2 forces the follower to commit and apply both entries immediately.
  let noop = Entry::new(
    Term::new(1),
    Index::new(1),
    EntryKind::Empty,
    bytes::Bytes::new(),
  );
  let leave_entry = Entry::new(
    Term::new(1),
    Index::new(2),
    EntryKind::ConfChange,
    leave_payload,
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
      std::vec![noop, leave_entry],
      Index::new(2), // leader_commit = 2: both entries committed
    )),
  );
  // Drain the deferred append completion so apply_committed runs with the durable entries.
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);

  // The Changer must have rejected leave_joint (not in joint) → node poisoned.
  assert!(
    ep.is_poisoned(),
    "node must be poisoned when Changer rejects a committed ConfChange at apply time"
  );
}

/// Test 1: A leader that removes itself (RemoveNode(self)) steps down immediately when
/// the ConfChange is committed+applied.
///
/// Invariants:
/// - role → Follower (same term, no term bump)
/// - leader → None
/// - heartbeat_deadline → None (no longer heartbeating)
/// - election_deadline → None (non-voter must not campaign)
/// - is_voter(self) == false in the new Tracker
#[test]
fn leader_steps_down_on_self_removal() {
  use crate::{AppendResponse, ConfChange, ConfChangeType, Index, Message, Term};

  let (mut ep, mut log, mut stable, d) = make_three_node_leader();
  let self_id = ep.id();
  let term_before = ep.term();

  // Propose RemoveNode(self).
  let cc = ConfChange::new(ConfChangeType::RemoveNode, self_id, bytes::Bytes::new());
  let idx = ep
    .propose_conf_change(d, &mut log, &stable, cc)
    .expect("RemoveNode(self) must be accepted");

  // Not yet committed: leader must still be leader.
  assert!(
    ep.role().is_leader(),
    "leader must not step down before commit"
  );

  // Drive to commit: leader self-match via storage drain, then peer 2 acks.
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
      idx,
    )),
  );

  // After apply: leader must have stepped down.
  assert!(
    ep.role().is_follower(),
    "leader must step down after RemoveNode(self) is applied"
  );
  assert_eq!(
    ep.leader(),
    None,
    "leader field must be cleared after step-down"
  );
  assert!(
    ep.heartbeat_deadline.is_none(),
    "heartbeat_deadline must be None after step-down"
  );
  assert!(
    ep.election_deadline.is_none(),
    "election_deadline must be None: a non-voter must not campaign"
  );
  // Step-down is at the same term (no bump).
  assert_eq!(ep.term(), term_before, "step-down must not bump the term");
  // The new Tracker must not have self as a voter.
  assert!(
    !ep.tracker.is_voter(&self_id),
    "self must not be a voter after RemoveNode(self) is applied"
  );
}

/// Test 2: A leader demoted to learner (AddLearnerNode(self)) also steps down.
#[test]
fn leader_steps_down_on_demotion_to_learner() {
  use crate::{AppendResponse, ConfChange, ConfChangeType, Index, Message, Term};

  let (mut ep, mut log, mut stable, d) = make_three_node_leader();
  let self_id = ep.id();
  let term_before = ep.term();

  // Propose AddLearnerNode(self) — demotes the current leader to learner.
  let cc = ConfChange::new(ConfChangeType::AddLearnerNode, self_id, bytes::Bytes::new());
  let idx = ep
    .propose_conf_change(d, &mut log, &stable, cc)
    .expect("AddLearnerNode(self) must be accepted");

  // Not yet committed: leader must still be leader.
  assert!(
    ep.role().is_leader(),
    "leader must not step down before commit"
  );

  // Drive to commit.
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
      idx,
    )),
  );

  // After apply: leader stepped down; self is now a learner (not a voter).
  assert!(
    ep.role().is_follower(),
    "leader must step down after AddLearnerNode(self) is applied"
  );
  assert_eq!(ep.leader(), None, "leader field must be cleared");
  assert!(
    ep.heartbeat_deadline.is_none(),
    "heartbeat_deadline must be None"
  );
  assert!(
    ep.election_deadline.is_none(),
    "election_deadline must be None"
  );
  assert_eq!(ep.term(), term_before, "step-down must not bump the term");
  assert!(
    !ep.tracker.is_voter(&self_id),
    "self must not be a voter after demotion to learner"
  );
  assert!(
    ep.tracker.is_learner(&self_id),
    "self must be a learner after AddLearnerNode(self)"
  );
}

/// A learner PROMOTED to voter must get its election timer ARMED so it can campaign. A non-voter
/// disarms its `election_deadline` when the timer fires (so the event-driven sim clock can advance
/// past it) and never re-arms; without re-arming on promotion the new voter would sit forever with
/// `election_deadline = None` and never start an election — a cluster whose voters were ALL
/// promoted learners would wedge leaderless.
///
/// Before fix: `apply_committed` updated the tracker on promotion but never armed the timer, so
/// `election_deadline` stayed `None` and `is_some()` below was false.
#[test]
fn promoted_learner_arms_election_timer() {
  use crate::{ConfChange, ConfChangeType, Entry, EntryKind, Instant, Term};
  use core::time::Duration;

  // Node 4 starts as a LEARNER in {voters:[1,2,3], learners:[4]}.
  let cfg = Config::try_new(
    4u64,
    std::vec![1u64, 2u64, 3u64, 4u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 99, CountSm::default());
  let mut log = VecLog::default();
  let learner_cs = crate::ConfState::new([1u64, 2u64, 3u64], [4u64], [], [], false);
  ep.tracker = crate::Tracker::from_conf_state(&learner_cs, Index::ZERO, 256, 0);
  assert!(ep.tracker.is_learner(&4u64), "node 4 must start a learner");

  // The non-voter state: the election timer fired once and was cleared to None (never re-armed).
  ep.election_deadline = None;

  // Append a committed AddNode(4) conf-change entry — it promotes node 4 from learner to voter.
  let cc = ConfChange::new(ConfChangeType::AddNode, 4u64, bytes::Bytes::new()).into_v2();
  let mut buf = Vec::new();
  crate::wire::encode_conf_change_v2(&cc, &mut buf);
  let idx = log.last_index().next();
  log.force_append(&[Entry::new(
    Term::new(1),
    idx,
    EntryKind::ConfChange,
    bytes::Bytes::from(buf),
  )]);
  ep.commit = idx;

  ep.apply_committed(&log);
  // The promotion itself does not arm (no per-site patch); the invariant is restored centrally by
  // `reconcile_election_timer`, which every public entry point (handle_message / handle_timeout /
  // handle_storage) runs after applying committed entries. Invoke it directly here to test that
  // central guarantee in isolation.
  assert!(
    ep.tracker.is_voter(&4u64),
    "node 4 must be a voter after AddNode(4) applies"
  );
  assert!(
    ep.election_deadline.is_none(),
    "promotion alone must NOT arm — arming is the reconcile's job, by construction"
  );
  ep.reconcile_election_timer(crate::Now::monotonic(Instant::ORIGIN));

  // Node 4 is now a voter AND the reconcile armed its election timer so it can campaign.
  assert!(
    ep.election_deadline.is_some(),
    "reconcile_election_timer must arm a promoted voter so it can campaign"
  );
}

/// Test 4: With `step_down_on_removal = false`, a leader that removes itself keeps
/// the Leader role (the operator has opted out of the default behavior).
#[test]
fn step_down_disabled_leader_keeps_role_after_self_removal() {
  use crate::{AppendResponse, ConfChange, ConfChangeType, Index, Message, Term};
  use core::time::Duration;

  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_step_down_on_removal(false); // opt out

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

  // Propose and apply RemoveNode(self).
  let cc = ConfChange::new(ConfChangeType::RemoveNode, 1u64, bytes::Bytes::new());
  let idx = ep
    .propose_conf_change(d, &mut log, &stable, cc)
    .expect("RemoveNode(self) must be accepted");
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
      idx,
    )),
  );

  // With step_down_on_removal=false, the leader must keep the Leader role.
  assert!(
    ep.role().is_leader(),
    "leader must keep leadership when step_down_on_removal=false"
  );
}

/// Test 5: Joint phase — a leader still present in the outgoing joint half must NOT
/// step down mid-joint (it must shepherd the joint → simple transition).
///
/// We use `enter_joint` with `auto_leave=false` (Explicit transition) so the leader stays
/// in a joint config where the outgoing half still contains self. `is_voter` checks BOTH
/// halves, so the leader remains a voter and must NOT step down.
#[test]
fn joint_phase_leader_keeps_role_while_still_in_outgoing_half() {
  use crate::{AppendResponse, ConfChangeType, Index, Message, Term};
  use core::time::Duration;

  // 3-voter cluster {1, 2, 3}. We propose a joint change that replaces node 3 with node 4
  // via enter_joint (Explicit transition). Node 1 (leader) is still in both the incoming
  // AND outgoing half → is_voter(1) == true → must not step down.
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
    Message::VoteResponse(VoteResponse::new(Term::new(1), 2u64, false, false)),
  );
  assert!(ep.role().is_leader());
  ep.handle_storage(d, &mut log, &mut stable);
  // Commit the no-op via peer 2.
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

  // Propose an Explicit joint change: add node 4, remove node 3. Node 1 stays in BOTH
  // incoming and outgoing halves, so is_voter(1) == true throughout.
  let ccv2 = ConfChangeV2::new(
    crate::ConfChangeTransition::Explicit,
    std::vec![
      ConfChangeSingle::new(ConfChangeType::AddNode, 4u64),
      ConfChangeSingle::new(ConfChangeType::RemoveNode, 3u64),
    ],
    bytes::Bytes::new(),
  );
  let idx = ep
    .propose_conf_change_v2(d, &mut log, &stable, ccv2)
    .expect("joint conf change must be accepted");

  // Drive to commit: storage drain + peer 2 ack.
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
      idx,
    )),
  );

  // We are now in joint config. Node 1 is still in both halves → is_voter(1) == true.
  assert!(
    ep.tracker.is_joint(),
    "cluster must be in joint configuration"
  );
  assert!(
    ep.tracker.is_voter(&1u64),
    "node 1 must still be a voter in the joint config (outgoing half)"
  );
  // Leader must NOT have stepped down.
  assert!(
    ep.role().is_leader(),
    "leader must not step down mid-joint when still a voter in the outgoing half"
  );
}

/// An invalid ConfChangeV2 is REJECTED at propose time, not committed-then-poisoned.
///
/// A leader NOT in a joint config receives `propose_conf_change_v2(leave_joint())`. `leave_joint`
/// is only valid from a joint config, so the Changer would reject it on apply and poison the node.
/// Pre-validation must turn this into a rejected proposal: `Err(InvalidConfChange)`, nothing
/// appended (`log.last_index()` unchanged), and the node NOT poisoned.
#[test]
fn propose_invalid_conf_change_is_rejected_not_poisoned() {
  let (mut ep, mut log, stable, d) = make_leader_with_current_term_commit();

  // The leader is in a simple (non-joint) config {1,2,3}; leaving a joint config is invalid here.
  let last_before = log.last_index();
  let res = ep.propose_conf_change_v2(d, &mut log, &stable, ConfChangeV2::leave_joint());

  assert!(
    matches!(res, Err(crate::ProposeError::InvalidConfChange)),
    "an invalid conf change must be rejected at propose time, got {res:?}"
  );
  assert_eq!(
    log.last_index(),
    last_before,
    "a rejected conf-change proposal must append nothing"
  );
  assert!(
    ep.poison_reason().is_none(),
    "a rejected conf-change proposal must NOT poison the node"
  );
}

/// A leader removed by its own committed conf change steps down at the same term — the
/// embedder holding leadership-scoped work must hear `LeaderChanged(None)`, exactly as for
/// the check-quorum step-down.
#[test]
fn self_removal_step_down_emits_leader_changed_none() {
  use crate::{AppendResponse, ConfChange, ConfChangeType, Index, Message, Term};
  use core::time::Duration;

  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap(); // step_down_on_removal defaults ON

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

  // Propose and commit RemoveNode(self).
  let cc = ConfChange::new(ConfChangeType::RemoveNode, 1u64, bytes::Bytes::new());
  let idx = ep
    .propose_conf_change(d, &mut log, &stable, cc)
    .expect("RemoveNode(self) must be accepted");
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
      idx,
    )),
  );
  assert!(ep.role().is_follower(), "removed leader steps down");

  let mut leader_events = Vec::new();
  while let Some(ev) = ep.poll_event() {
    if let Event::LeaderChanged(lc) = ev {
      leader_events.push((lc.term(), lc.leader()));
    }
  }
  assert_eq!(
    leader_events,
    std::vec![(Term::new(1), None)],
    "the self-removal step-down must surface exactly one LeaderChanged(None)"
  );
}

/// An id whose `Data` encoding exceeds the 1024-byte wire bound. `NodeId` is
/// blanket-implemented, so nothing stops an embedder from shipping one — the propose
/// path must be the gate.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
struct OverwideId(u64);

impl core::fmt::Display for OverwideId {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "overwide-{}", self.0)
  }
}

impl crate::Data for OverwideId {
  fn encode(&self, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&[0u8; 1020]);
    self.0.encode(buf);
  }
  fn decode(cur: &mut crate::data::ByteCursor) -> Result<Self, crate::DecodeError> {
    let _pad = cur.take_bytes(1020)?;
    Ok(Self(u64::decode(cur)?))
  }
}

impl crate::CheapClone for OverwideId {}

/// A conf change whose target id encodes OUTSIDE the wire bound must be REJECTED AT
/// PROPOSE (`InvalidConfChange`, nothing appended): appended-and-committed, the apply
/// path's envelope decode would reject the id and poison EVERY node applying the entry.
#[test]
fn conf_change_with_overwide_id_is_rejected_at_propose() {
  use crate::{ConfChange, ConfChangeType, ProposeError};
  use core::time::Duration;

  let cfg = Config::try_new(
    OverwideId(1),
    std::vec![OverwideId(1)],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = NoopStable::default();

  // Single voter: elect on the first timeout (the self-vote completes synchronously).
  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable);
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(ep.role().is_leader());
  let before = log.last_index();

  let cc = ConfChange::new(ConfChangeType::AddNode, OverwideId(2), bytes::Bytes::new());
  let err = ep
    .propose_conf_change(d, &mut log, &stable, cc)
    .expect_err("an overwide id must not enter the log");
  assert!(matches!(err, ProposeError::InvalidConfChange));
  assert_eq!(log.last_index(), before, "nothing was appended");
}

/// A node removed by its OWN committed conf change while CAMPAIGNING must abort the candidacy (step
/// down to follower) the instant the removal applies, and — as a backstop for the win-before-apply
/// order — `become_leader` must refuse to lead a non-voter. Together, a decommissioned node can never
/// lead a configuration it is not part of, honouring `step_down_on_removal`.
///
/// MUTATION: restore the `self.role.is_leader()` gate on the apply-time step-down (dropping the
/// candidate case) OR remove the `become_leader` non-voter guard → a removed candidate keeps its role
/// and can assume leadership, failing the follower / not-leader assertions below.
#[test]
fn candidate_removed_by_own_conf_change_steps_down_and_cannot_lead() {
  use super::super::Role;
  use crate::{ConfChange, ConfChangeType, Entry, EntryKind, Index, Instant, Term};

  let (mut ep, mut log, mut stable) = make_follower(); // id = 2, voters {1,2,3}
  let self_id = ep.id();

  // A committed-but-unapplied RemoveNode(self) conf change in the log (the previous leader committed it
  // before dying).
  let cc = ConfChange::new(ConfChangeType::RemoveNode, self_id, bytes::Bytes::new()).into_v2();
  let mut buf = Vec::new();
  crate::wire::encode_conf_change_v2(&cc, &mut buf);
  let entry = Entry::new(
    Term::new(1),
    Index::new(1),
    EntryKind::ConfChange,
    bytes::Bytes::from(buf),
  );
  log.force_append(core::slice::from_ref(&entry));
  ep.term = Term::new(1);
  ep.commit = Index::new(1);
  ep.applied = Index::ZERO;
  ep.durable.durable_index = Index::new(1);

  // The node is campaigning when the removal is about to apply.
  ep.role = Role::Candidate;
  assert!(
    ep.tracker.is_voter(&self_id),
    "self is still a voter before the removal applies"
  );

  // Applying the committed removal (driven by the storage handler) aborts the candidacy.
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  assert!(
    !ep.tracker.is_voter(&self_id),
    "self is removed once the conf change applies"
  );
  assert!(
    ep.role().is_follower(),
    "a candidate removed by its own conf change steps down to follower"
  );
  assert!(
    ep.election_deadline.is_none(),
    "a removed node holds no election timer"
  );

  // Backstop: even forced back into Candidate, a non-voter must not assume leadership.
  ep.role = Role::Candidate;
  ep.become_leader(Instant::ORIGIN.into(), &mut log, &mut stable);
  assert!(
    !ep.role().is_leader(),
    "become_leader must refuse to lead a non-voter"
  );
  assert!(
    ep.role().is_follower(),
    "the refused would-be leader steps down to follower"
  );
}

/// Auto-leave must be FROZEN during a leader transfer: appending the leave-joint entry would advance
/// `last_index` past the caught-up transferee, so its forced `TimeoutNow` campaign loses on `log_ok`
/// and the cluster is leaderless for an election timeout. With a transfer in progress the leader holds
/// the joint config; once the transfer resolves it resumes and appends leave-joint.
///
/// MUTATION: drop the `lead_transferee.is_none()` guard on the auto-leave condition → the leave-joint
/// entry is appended while the transfer is in progress, advancing `last_index` and leaving the joint
/// config early, so the frozen-window assertions below fail.
#[test]
fn auto_leave_is_frozen_during_leader_transfer() {
  use crate::{AppendResponse, ConfChangeTransition, ConfChangeType, Index, Message, Term};
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

  // Elect node 1 and commit its no-op.
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

  // An IMPLICIT joint change (auto_leave = true): add node 4, remove node 3. Node 1 stays in both
  // halves, so it keeps leading through the joint phase.
  let ccv2 = ConfChangeV2::new(
    ConfChangeTransition::Implicit,
    std::vec![
      ConfChangeSingle::new(ConfChangeType::AddNode, 4u64),
      ConfChangeSingle::new(ConfChangeType::RemoveNode, 3u64),
    ],
    bytes::Bytes::new(),
  );
  let idx = ep
    .propose_conf_change_v2(d, &mut log, &stable, ccv2)
    .expect("joint conf change must be accepted");
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
      idx,
    )),
  );
  while ep.poll_message().is_some() {}

  // A leader transfer is now in progress (transferee is node 2, a voter in the new config).
  ep.transfer.lead_transferee = Some(2u64);
  let last_before = log.last_index();
  ep.handle_storage(d, &mut log, &mut stable);
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(
    ep.tracker.is_joint(),
    "auto-leave must be frozen during a transfer — the cluster stays joint"
  );
  assert_eq!(
    log.last_index(),
    last_before,
    "no leave-joint entry is appended while the transfer is in progress"
  );

  // The transfer resolves: auto-leave resumes and appends the leave-joint entry.
  ep.transfer.lead_transferee = None;
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(
    log.last_index() > last_before,
    "auto-leave resumes and appends leave-joint once the transfer clears"
  );
}

/// A conf change whose entry would exceed one transport frame — here via a large caller-supplied
/// `context` — is refused at propose time, just like an oversized `propose`. Without the guard the
/// membership path would append an entry no `AppendEntries` could carry, wedging replication.
///
/// MUTATION: remove the frame-fit check in `propose_conf_change_v2` → the oversized conf change is
/// appended and returns `Ok`, so the refuse-and-not-appended assertions below fail.
#[test]
fn oversized_conf_change_context_is_refused() {
  use crate::{ConfChangeTransition, ConfChangeType, ProposeError};

  let (mut ep, mut log, stable, d) = make_three_node_leader();
  let before = log.last_index();
  let cc = ConfChangeV2::new(
    ConfChangeTransition::Auto,
    std::vec![ConfChangeSingle::new(ConfChangeType::AddNode, 4u64)],
    bytes::Bytes::from(std::vec![0u8; crate::wire::MAX_FRAME_BYTES]), // 64 MiB context
  );
  let r = ep.propose_conf_change_v2(d, &mut log, &stable, cc);
  assert!(
    matches!(r, Err(ProposeError::EntryTooLarge { .. })),
    "an oversized conf-change context must be refused, got {r:?}"
  );
  assert_eq!(
    log.last_index(),
    before,
    "the oversized conf change must NOT be appended"
  );
}
