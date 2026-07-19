use super::{super::*, *};
use crate::{
  ConfChangeSingle, ConfChangeV2, VoteResponse,
  testkit::{AsyncStable, CountSm, NoopStable, VecLog},
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
/// The guard rides `pending_conf_index`, which a fresh leader must SEED from its inherited tail:
/// `become_leader` sets `pending_conf_index = last_index` (= 2), so `2 > applied(0)` is true →
/// Err(ConfChangeInFlight). Left at ZERO, `ZERO > applied` is false and the second ConfChange is
/// wrongly accepted → Ok(_), putting two conf changes in flight at once.
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
/// An `apply_committed` that updates the tracker on promotion but never arms the timer leaves
/// `election_deadline` at `None`, and `is_some()` below is false.
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

/// The commit that REMOVES a follower must still reach that follower: `apply_committed` prunes it
/// from the tracker in the same fused pass that folds the change, and every later send targets only
/// tracked peers — so without the farewell the removed node never learns and retries elections
/// against a cluster that ignores it. This pins the `match >= removal` arm: the peer ACKED the
/// conf entry, so it already holds every entry through the removal and the leader emits ONE
/// farewell Heartbeat whose commit clamp `min(commit, match)` covers it — commit alone suffices,
/// no entries-carrying farewell append for a caught-up peer (mirrors etcd's
/// bcastAppend-before-application ordering). Feeding that beat to the removed node makes it apply
/// its own removal.
///
/// MUTATION: drop the farewell emission in `apply_committed` → no Heartbeat to node 3 below.
#[test]
fn removal_commit_reaches_pruned_follower_via_farewell_heartbeat() {
  use crate::{
    AppendEntries, AppendResponse, ConfChange, ConfChangeType, Entry, EntryKind, Index, Message,
    Term,
  };

  let (mut ep, mut log, mut stable, d) = make_three_node_leader();

  // Propose RemoveNode(3) at index 2 and make the leader's own append durable.
  let cc = ConfChange::new(ConfChangeType::RemoveNode, 3u64, bytes::Bytes::new());
  let idx = ep
    .propose_conf_change(d, &mut log, &stable, cc)
    .expect("RemoveNode(3) must be accepted");
  ep.flush_appends(d, &log, &stable);
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  // Node 3 ACKS the conf entry, and its ack completes the quorum {1, 3}: commit advances to
  // `idx`, the change applies, and node 3 is pruned — all in this one dispatch.
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    3u64,
    Message::AppendResponse(AppendResponse::new(
      Term::new(1),
      3u64,
      false,
      Index::ZERO,
      Term::ZERO,
      idx,
    )),
  );
  assert!(
    !ep.tracker.is_voter(&3u64) && ep.tracker.progress(&3u64).is_none(),
    "node 3 must be pruned once the removal applies"
  );

  // Exactly one farewell Heartbeat to node 3 rides the normal outgoing queue, and its commit
  // clamp min(commit, match) covers the conf entry (node 3 acked it: match == idx).
  let farewells: Vec<_> = core::iter::from_fn(|| ep.poll_message())
    .filter(|o| o.to() == 3u64 && matches!(o.message(), Message::Heartbeat(_)))
    .collect();
  assert_eq!(
    farewells.len(),
    1,
    "the pruned follower must get exactly one farewell Heartbeat"
  );
  let farewell = match farewells.into_iter().next().unwrap().into_parts() {
    (_, Message::Heartbeat(hb)) => {
      assert!(
        hb.commit() >= idx,
        "the farewell's commit ({:?}) must cover the removal entry ({idx:?})",
        hb.commit()
      );
      Message::Heartbeat(hb)
    }
    _ => unreachable!(),
  };

  // Node 3's side: it holds the replicated log [no-op@1, conf@2] (it acked the entry) but has not
  // learned the commit. The farewell delivers it: node 3 applies its own removal and reports a
  // ConfChanged whose config no longer contains it.
  let cfg3 = Config::try_new(
    3u64,
    std::vec![1u64, 2u64, 3u64],
    core::time::Duration::from_millis(1000),
    core::time::Duration::from_millis(100),
  )
  .unwrap();
  let mut c = Endpoint::new(cfg3, Instant::ORIGIN, 9, CountSm::default());
  let mut log3 = VecLog::default();
  let mut stable3 = NoopStable::default();
  let conf_payload = {
    let v2 = ConfChange::new(ConfChangeType::RemoveNode, 3u64, bytes::Bytes::new()).into_v2();
    let mut buf = Vec::new();
    crate::wire::encode_conf_change_v2(&v2, &mut buf);
    bytes::Bytes::from(buf)
  };
  c.handle_message(
    d,
    &mut log3,
    &mut stable3,
    1u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(1),
      1u64,
      Index::ZERO,
      Term::ZERO,
      std::vec![
        Entry::new(
          Term::new(1),
          Index::new(1),
          EntryKind::Empty,
          bytes::Bytes::new()
        ),
        Entry::new(Term::new(1), idx, EntryKind::ConfChange, conf_payload),
      ],
      Index::new(1), // the removal itself is not yet known committed
    )),
  );
  c.handle_storage(d, &mut log3, &mut stable3);
  while c.poll_message().is_some() {}
  while c.poll_event().is_some() {}

  c.handle_message(d, &mut log3, &mut stable3, 1u64, farewell);
  let removed = core::iter::from_fn(|| c.poll_event()).any(
    |e| matches!(&e, Event::ConfChanged(ev) if ev.index() == idx && !ev.conf().is_voter(&3u64)),
  );
  assert!(
    removed,
    "the farewell must make the removed follower apply the removal (ConfChanged without node 3)"
  );
}

/// Drive the `match < removal` farewell shape on a 3-voter leader: node 3 proves only the
/// no-op@1 (match = 1), the RemoveNode(3) entry commits on node 2's ack alone, and node 3 is
/// pruned lagging. Returns the leader's single outgoing farewell to node 3 and the removal index,
/// asserting exactly one message went to the pruned peer.
fn removal_commits_without_node3() -> (Message<u64>, Index) {
  use crate::{AppendResponse, ConfChange, ConfChangeType, Index, Message, Term};

  let (mut ep, mut log, mut stable, d) = make_three_node_leader();

  // Node 3 has proven only the no-op@1 (match = 1); its ack of the conf entry never arrives.
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    3u64,
    Message::AppendResponse(AppendResponse::new(
      Term::new(1),
      3u64,
      false,
      Index::ZERO,
      Term::ZERO,
      Index::new(1),
    )),
  );
  while ep.poll_message().is_some() {}

  let cc = ConfChange::new(ConfChangeType::RemoveNode, 3u64, bytes::Bytes::new());
  let idx = ep
    .propose_conf_change(d, &mut log, &stable, cc)
    .expect("RemoveNode(3) must be accepted");
  ep.flush_appends(d, &log, &stable);
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  // Node 2's ack completes the quorum WITHOUT node 3: the removal commits, the change applies,
  // and node 3 (match = 1 < idx) is pruned lagging — all in this one dispatch.
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
  assert!(ep.tracker.progress(&3u64).is_none(), "node 3 pruned");

  let mut to3: Vec<_> = core::iter::from_fn(|| ep.poll_message())
    .filter(|o| o.to() == 3u64)
    .collect();
  assert_eq!(
    to3.len(),
    1,
    "exactly one farewell must ride out to the pruned peer"
  );
  (to3.pop().unwrap().into_parts().1, idx)
}

/// A follower endpoint for node 3 in the 3-voter cluster, with `entries` (from leader 1, term 1)
/// already appended and durable, and commit at `commit`.
fn node3_with_log(
  entries: Vec<crate::Entry>,
  commit: Index,
) -> (Endpoint<u64, CountSm>, VecLog, NoopStable) {
  use crate::{AppendEntries, Message, Term};

  let cfg3 = Config::try_new(
    3u64,
    std::vec![1u64, 2u64, 3u64],
    core::time::Duration::from_millis(1000),
    core::time::Duration::from_millis(100),
  )
  .unwrap();
  let mut c = Endpoint::new(cfg3, Instant::ORIGIN, 9, CountSm::default());
  let mut log3 = VecLog::default();
  let mut stable3 = NoopStable::default();
  let d = Instant::ORIGIN;
  c.handle_message(
    d,
    &mut log3,
    &mut stable3,
    1u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(1),
      1u64,
      Index::ZERO,
      Term::ZERO,
      entries,
      commit,
    )),
  );
  c.handle_storage(d, &mut log3, &mut stable3);
  while c.poll_message().is_some() {}
  while c.poll_event().is_some() {}
  (c, log3, stable3)
}

/// The RemoveNode(3) conf-change payload, wire-encoded as the leader appends it.
fn remove3_payload() -> bytes::Bytes {
  use crate::{ConfChange, ConfChangeType};
  let v2 = ConfChange::new(ConfChangeType::RemoveNode, 3u64, bytes::Bytes::new()).into_v2();
  let mut buf = Vec::new();
  crate::wire::encode_conf_change_v2(&v2, &mut buf);
  bytes::Bytes::from(buf)
}

/// The ack-in-flight removed voter learns via the farewell APPEND: node 3 HOLDS the conf entry
/// (its ack simply never reached the leader before node 2's completed the quorum), so at the fold
/// `match[3] = 1 < idx` and a clamped farewell Heartbeat (`min(commit, match) = 1`) leaves it
/// IGNORANT — with pre-vote/check-quorum off (the defaults) its election timer then fires a REAL
/// higher-term campaign whose up-to-date log wins, briefly deposing the live leader before it
/// commits its own removal. The farewell AppendEntries closes that window: `prev = match` is the
/// leader's proven-replicated floor for node 3, so the prev-check passes by construction and the
/// removal commit arrives with the message.
///
/// MUTATION: revert `send_farewell_append` to the bare clamped heartbeat → node 3 receives a
/// Heartbeat clamped at 1 and never applies the removal.
#[test]
fn ack_in_flight_removed_voter_learns_via_farewell_append() {
  use crate::{Entry, EntryKind, Index, Message, Term};

  let (farewell, idx) = removal_commits_without_node3();
  let ae = match &farewell {
    Message::AppendEntries(ae) => ae,
    other => panic!("a lagging pruned voter's farewell must be an AppendEntries, got {other:?}"),
  };
  assert_eq!(
    ae.prev_log_index(),
    Index::new(1),
    "the farewell prev must anchor at node 3's proven match"
  );
  assert_eq!(
    ae.leader_commit(),
    idx,
    "the farewell must carry the removal commit"
  );
  assert_eq!(
    ae.entries().iter().map(|e| e.index()).collect::<Vec<_>>(),
    std::vec![idx],
    "the farewell must carry exactly the missing suffix (match, removal]"
  );

  // Node 3's side, the ACK-IN-FLIGHT shape: it holds [no-op@1, conf@2] (it received and acked
  // the entry; the ack was lost) but never learned the commit. The farewell is a pure duplicate
  // append whose commit makes it apply its own removal.
  let (mut c, mut log3, mut stable3) = node3_with_log(
    std::vec![
      Entry::new(
        Term::new(1),
        Index::new(1),
        EntryKind::Empty,
        bytes::Bytes::new()
      ),
      Entry::new(Term::new(1), idx, EntryKind::ConfChange, remove3_payload()),
    ],
    Index::new(1),
  );
  c.handle_message(Instant::ORIGIN, &mut log3, &mut stable3, 1u64, farewell);
  let removed = core::iter::from_fn(|| c.poll_event()).any(
    |e| matches!(&e, Event::ConfChanged(ev) if ev.index() == idx && !ev.conf().is_voter(&3u64)),
  );
  assert!(
    removed,
    "the farewell append must make the ack-in-flight voter apply the removal"
  );
  assert!(!c.tracker.is_voter(&3u64), "node 3 has folded its removal");
}

/// The never-received removed voter learns via the SAME farewell append: node 3 holds only the
/// no-op@1, so the farewell's entries are a REAL suffix it appends before committing — the shape
/// the clamped heartbeat could never deliver (it withheld the commit precisely because the peer
/// had not proven the entries). The prev-check anchors at the proven match, so acceptance is
/// deterministic; the appended suffix commits and applies in the same dispatch.
#[test]
fn never_received_removed_voter_learns_via_farewell_append() {
  use crate::{Entry, EntryKind, Index, Message};

  let (farewell, idx) = removal_commits_without_node3();
  assert!(
    matches!(&farewell, Message::AppendEntries(_)),
    "a lagging pruned voter's farewell must be an AppendEntries"
  );

  // Node 3 never saw the conf entry at all: only the no-op@1, nothing committed.
  let (mut c, mut log3, mut stable3) = node3_with_log(
    std::vec![Entry::new(
      Term::new(1),
      Index::new(1),
      EntryKind::Empty,
      bytes::Bytes::new()
    )],
    Index::ZERO,
  );
  c.handle_message(Instant::ORIGIN, &mut log3, &mut stable3, 1u64, farewell);
  assert_eq!(
    log3.last_index(),
    idx,
    "the farewell must append the missing conf entry"
  );
  let removed = core::iter::from_fn(|| c.poll_event()).any(
    |e| matches!(&e, Event::ConfChanged(ev) if ev.index() == idx && !ev.conf().is_voter(&3u64)),
  );
  assert!(
    removed,
    "the farewell append must make the never-received voter apply the removal"
  );
  assert!(!c.tracker.is_voter(&3u64), "node 3 has folded its removal");
}

/// The one-frame bound: when the pruned peer's missing suffix cannot fit a single
/// `AppendEntries` frame (here a fat entry leaves less headroom than any second entry's minimum
/// wire cost), the farewell falls back to the clamped Heartbeat — `min(commit, match)` — and the
/// peer keeps the documented ignorance residual (it needed snapshot-level catch-up; the
/// embedder's catalog covers it). No multi-frame farewell for a peer being pruned.
#[test]
fn farewell_over_one_frame_falls_back_to_clamped_heartbeat() {
  use crate::{AppendResponse, ConfChange, ConfChangeType, EntriesRead, Index, Message, Term};

  let (mut ep, mut log, mut stable, d) = make_three_node_leader();

  // Node 3 has proven only the no-op@1 (match = 1) and sees nothing further.
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    3u64,
    Message::AppendResponse(AppendResponse::new(
      Term::new(1),
      3u64,
      false,
      Index::ZERO,
      Term::ZERO,
      Index::new(1),
    )),
  );
  while ep.poll_message().is_some() {}

  // A fat entry at index 2: alone it fits one frame (propose enforces that) with only 100 bytes
  // of entry-budget headroom — less than ANY second entry's minimum wire cost — so the suffix
  // (1, removal] cannot make one frame. (The payload arithmetic: the `Bytes` codec adds a 4-byte
  // length prefix and the sizer charges 128 bytes of per-entry overhead.)
  let fat = bytes::Bytes::from(std::vec![
    0xA5u8;
    crate::wire::APPEND_FRAME_ENTRY_BUDGET - 232
  ]);
  let fat_idx = ep
    .propose(d, &mut log, &stable, &fat)
    .expect("the fat entry alone fits one frame");
  ep.flush_appends(d, &log, &stable);
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
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
      fat_idx,
    )),
  );
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  let cc = ConfChange::new(ConfChangeType::RemoveNode, 3u64, bytes::Bytes::new());
  let idx = ep
    .propose_conf_change(d, &mut log, &stable, cc)
    .expect("RemoveNode(3) must be accepted");
  ep.flush_appends(d, &log, &stable);
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  // Sanity: node 3's missing suffix (1, idx] genuinely exceeds the one-frame entry budget.
  {
    let read = log.entries(fat_idx..idx.next(), u64::MAX).unwrap();
    let suffix = match &read {
      EntriesRead::Ready(e) => &**e,
      EntriesRead::Pending => unreachable!(),
    };
    let cost: usize = suffix.iter().map(crate::wire::entry_frame_cost).sum();
    assert!(
      cost > crate::wire::APPEND_FRAME_ENTRY_BUDGET,
      "the suffix must overflow one frame for this test to bite ({cost})"
    );
  }

  // Node 2's ack commits the removal; node 3 (match = 1 < idx) is pruned with an over-frame
  // suffix → the farewell is the clamped Heartbeat, never a multi-frame append.
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
  assert!(ep.tracker.progress(&3u64).is_none(), "node 3 pruned");
  let mut to3: Vec<_> = core::iter::from_fn(|| ep.poll_message())
    .filter(|o| o.to() == 3u64)
    .collect();
  assert_eq!(to3.len(), 1, "exactly one farewell to the pruned peer");
  match to3.pop().unwrap().into_parts() {
    (_, Message::Heartbeat(hb)) => assert_eq!(
      hb.commit(),
      Index::new(1),
      "the fallback farewell must stay clamped at the peer's proven match"
    ),
    (_, other) => panic!("an over-frame suffix must fall back to a Heartbeat, got {other:?}"),
  }
}

/// The compaction bound: when part of the pruned peer's missing suffix lies below `first_index`
/// (compacted into a snapshot), no `AppendEntries` can carry a valid prev/entry range across the
/// boundary and a pruned peer gets no snapshot — the farewell falls back to the clamped
/// Heartbeat and the documented ignorance residual stands.
#[test]
fn farewell_across_compaction_falls_back_to_clamped_heartbeat() {
  use crate::{AppendResponse, ConfChange, ConfChangeType, Index, Message, Term};

  let (mut ep, mut log, mut stable, d) = make_three_node_leader();

  // Node 3 has proven only the no-op@1 (match = 1) and sees nothing further.
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    3u64,
    Message::AppendResponse(AppendResponse::new(
      Term::new(1),
      3u64,
      false,
      Index::ZERO,
      Term::ZERO,
      Index::new(1),
    )),
  );
  while ep.poll_message().is_some() {}

  // Commit an entry at index 2 (via node 2's ack) and compact through it, so node 3's missing
  // suffix starts below `first_index = 3`.
  let cmd = bytes::Bytes::from_static(b"x");
  ep.propose(d, &mut log, &stable, &cmd)
    .expect("the filler entry is accepted");
  ep.flush_appends(d, &log, &stable);
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
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
      Index::new(2),
    )),
  );
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}
  log.compact(Index::new(2));
  assert_eq!(log.first_index(), Index::new(3), "entries 1..=2 compacted");

  let cc = ConfChange::new(ConfChangeType::RemoveNode, 3u64, bytes::Bytes::new());
  let idx = ep
    .propose_conf_change(d, &mut log, &stable, cc)
    .expect("RemoveNode(3) must be accepted");
  ep.flush_appends(d, &log, &stable);
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  // Node 2's ack commits the removal; node 3 (match = 1, suffix start 2 < first_index 3) is
  // pruned behind the compaction boundary → the farewell is the clamped Heartbeat.
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
  assert!(ep.tracker.progress(&3u64).is_none(), "node 3 pruned");
  let mut to3: Vec<_> = core::iter::from_fn(|| ep.poll_message())
    .filter(|o| o.to() == 3u64)
    .collect();
  assert_eq!(to3.len(), 1, "exactly one farewell to the pruned peer");
  match to3.pop().unwrap().into_parts() {
    (_, Message::Heartbeat(hb)) => assert_eq!(
      hb.commit(),
      Index::new(1),
      "the fallback farewell must stay clamped at the peer's proven match"
    ),
    (_, other) => panic!("a compacted suffix must fall back to a Heartbeat, got {other:?}"),
  }
}

// ---------------------------------------------------------------------------------------------
// pending-farewell-retry: the leader re-drives a possibly-lost farewell on a bounded
// blind budget so a single dropped delivery does not strand a removed voter ignorant.
// ---------------------------------------------------------------------------------------------

/// Drive `make_three_node_leader` through a RemoveNode(3) where node 3 proved only match = 1 (below
/// the removal index), so the fold fires an APPEND farewell (shot 1) and schedules blind retries.
/// Returns the leader with its stores, the tick instant `d`, and the removal index; node 3's shot-1
/// append is already drained.
fn leader_after_removing_node3() -> (Endpoint<u64, CountSm>, VecLog, NoopStable, Instant, Index) {
  use crate::{AppendResponse, ConfChange, ConfChangeType, Message, Term};

  let (mut ep, mut log, mut stable, d) = make_three_node_leader();

  // Node 3 proves only the no-op@1 (match = 1); its ack of the conf entry never arrives.
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    3u64,
    Message::AppendResponse(AppendResponse::new(
      Term::new(1),
      3u64,
      false,
      Index::ZERO,
      Term::ZERO,
      Index::new(1),
    )),
  );
  while ep.poll_message().is_some() {}

  let cc = ConfChange::new(ConfChangeType::RemoveNode, 3u64, bytes::Bytes::new());
  let idx = ep
    .propose_conf_change(d, &mut log, &stable, cc)
    .expect("RemoveNode(3) must be accepted");
  ep.flush_appends(d, &log, &stable);
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  // Node 2's ack completes the quorum WITHOUT node 3: the removal commits + applies, node 3
  // (match = 1 < idx) is pruned lagging, and the append farewell (shot 1) rides out.
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
  assert!(ep.tracker.progress(&3u64).is_none(), "node 3 pruned");
  while ep.poll_message().is_some() {} // drain the shot-1 append
  while ep.poll_event().is_some() {}
  (ep, log, stable, d, idx)
}

/// Collect every outgoing message addressed to `to`, draining (and discarding) the rest.
fn drain_to(ep: &mut Endpoint<u64, CountSm>, to: u64) -> std::vec::Vec<Message<u64>> {
  core::iter::from_fn(|| ep.poll_message())
    .filter(|o| o.to() == to)
    .map(|o| o.into_parts().1)
    .collect()
}

/// Populate-on-fire: the append farewell schedules a retry entry keyed on the pruned peer, carrying
/// the full blind budget with `next_at` deferred to the first leader tick.
#[test]
fn farewell_retry_populated_on_append_fire() {
  let (ep, _log, _stable, _d, idx) = leader_after_removing_node3();
  let retry = ep
    .pending_farewells
    .get(&3u64)
    .expect("the append farewell must schedule a retry for the pruned peer");
  assert_eq!(
    retry.matched,
    Index::new(1),
    "prev anchors at node 3's proven match"
  );
  assert_eq!(retry.idx, idx, "the retry re-delivers the removal index");
  assert_eq!(
    retry.shots_left, FAREWELL_RETRY_SHOTS,
    "the initial fire leaves the full retry budget"
  );
  assert!(
    retry.next_at.is_none(),
    "scheduling is deferred to the first leader tick (no `now` at the fire site)"
  );
}

/// Re-drive spacing + budget exhaustion: the first tick FRONT-LOADS the next shot immediately; then
/// each election-timeout-spaced tick re-delivers exactly one append; the entry drops once spent.
#[test]
fn farewell_retry_spacing_and_budget_exhaustion() {
  use core::time::Duration;

  let (mut ep, mut log, mut stable, d, _idx) = leader_after_removing_node3();
  let et = Duration::from_millis(1000); // the config's election_timeout

  // Tick 1: front-loads shot 2 IMMEDIATELY (before the removed peer's election deadline could depose
  // the leader), then schedules the next shot one election timeout out.
  let t1 = d + Duration::from_millis(150);
  ep.handle_timeout(t1, &mut log, &mut stable);
  let to3 = drain_to(&mut ep, 3u64);
  assert_eq!(to3.len(), 1, "the first leader tick front-loads shot 2");
  assert!(
    matches!(to3[0], Message::AppendEntries(_)),
    "the re-delivery is the farewell append, not a heartbeat"
  );
  assert_eq!(
    ep.pending_farewells.get(&3u64).unwrap().shots_left,
    FAREWELL_RETRY_SHOTS - 1,
    "the front-loaded shot decrements the budget"
  );

  // A tick BEFORE the next scheduled shot re-sends nothing (spacing holds).
  ep.handle_timeout(t1 + Duration::from_millis(150), &mut log, &mut stable);
  assert!(
    drain_to(&mut ep, 3u64).is_empty(),
    "no re-send before one election timeout elapses"
  );

  // Tick 2: at t1 + election_timeout, the last shot fires and the budget is exhausted → dropped.
  ep.handle_timeout(t1 + et, &mut log, &mut stable);
  assert_eq!(
    drain_to(&mut ep, 3u64).len(),
    1,
    "the final shot re-delivers once"
  );
  assert!(
    ep.pending_farewells.is_empty(),
    "the budget is spent — the entry is dropped"
  );

  // A later tick re-sends nothing once the budget is gone.
  ep.handle_timeout(t1 + et + et, &mut log, &mut stable);
  assert!(
    drain_to(&mut ep, 3u64).is_empty(),
    "no re-send after budget exhaustion"
  );
}

/// Leadership loss PARKS the budget (it is NOT cleared): a higher-term step-down leaves the entries
/// in the map so a re-election can re-drive them, but the leader-gated `has_pending_farewells` reads
/// false on the resulting follower (a parked follower must not hold the group quiesce-ineligible).
#[test]
fn farewell_retry_parks_across_leadership_loss() {
  use crate::{Heartbeat, Message, Term};

  let (mut ep, mut log, mut stable, d, _idx) = leader_after_removing_node3();
  assert!(ep.has_pending_farewells(), "the leader owes re-deliveries");

  // A higher-term Heartbeat steps the leader down (adopt term, become follower).
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::Heartbeat(Heartbeat::new(
      Term::new(2),
      2u64,
      Index::ZERO,
      bytes::Bytes::new(),
    )),
  );
  assert!(
    !ep.role().is_leader(),
    "the higher term steps the leader down"
  );
  assert!(
    ep.pending_farewells.contains_key(&3u64),
    "the budget PARKS across the step-down"
  );
  assert!(
    !ep.has_pending_farewells(),
    "a parked follower must not hold the group quiesce-ineligible"
  );
}

/// The CheckQuorum step-down (`step_down_to_follower`) PARKS the budget too — it is not cleared, so a
/// later re-election can re-arm and re-drive the surviving shots.
#[test]
fn farewell_retry_survives_check_quorum_step_down() {
  let (mut ep, _log, _stable, d, _idx) = leader_after_removing_node3();
  assert!(ep.has_pending_farewells());
  let shots_before = ep.pending_farewells.get(&3u64).unwrap().shots_left;
  ep.step_down_to_follower(crate::Now::monotonic(d));
  assert!(!ep.role().is_leader());
  let parked = ep
    .pending_farewells
    .get(&3u64)
    .expect("step_down_to_follower parks the budget");
  assert_eq!(
    parked.shots_left, shots_before,
    "the demotion parks the budget WITHOUT spending a shot"
  );
  assert!(
    !ep.has_pending_farewells(),
    "a parked follower reads false through the leader-gated accessor"
  );
}

/// `become_leader` RE-ARMS a parked entry: a surviving shot's wall is reset to `None` so it front-loads
/// at the first post-re-election tick, and the leader-gated accessor flips false→true as leadership
/// returns. The wall is armed to `Some` first so the reset is observable, not a no-op.
#[test]
fn farewell_retry_re_arm_resets_the_wall_and_regains_the_leader_gate() {
  use crate::{Message, VoteResponse};
  use core::time::Duration;

  let (mut ep, mut log, mut stable, d, _idx) = leader_after_removing_node3();

  // One leader tick front-loads a shot and ARMS a wall for the next one (next_at = Some).
  let t1 = d + Duration::from_millis(150);
  ep.handle_timeout(t1, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  assert!(
    ep.pending_farewells.get(&3u64).unwrap().next_at.is_some(),
    "the tick armed a wall for the next shot"
  );

  // Step down: the entry PARKS with its wall intact; the leader gate reads false.
  ep.step_down_to_follower(crate::Now::monotonic(t1));
  assert!(
    ep.pending_farewells.get(&3u64).unwrap().next_at.is_some(),
    "the parked wall is untouched"
  );
  assert!(
    !ep.has_pending_farewells(),
    "the leader gate reads false on a parked follower"
  );

  // Re-elect: become_leader RE-ARMS the wall to None and the leader gate reads true again.
  let rd = ep
    .poll_timeout()
    .expect("the follower re-arms its election timer");
  ep.handle_timeout(rd, &mut log, &mut stable);
  let ct = ep.term();
  ep.handle_message(
    rd,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResponse(VoteResponse::new(ct, 2u64, false, false)),
  );
  ep.handle_storage(rd, &mut log, &mut stable);
  assert!(ep.role().is_leader(), "the endpoint re-wins leadership");
  assert_eq!(
    ep.pending_farewells.get(&3u64).unwrap().next_at,
    None,
    "become_leader re-armed the wall to None so the shot front-loads at the first tick"
  );
  assert!(
    ep.has_pending_farewells(),
    "the leader gate reads true again on re-election"
  );
}

/// Staleness guard: a peer re-admitted by a later conf change is dropped from the retry map.
#[test]
fn farewell_retry_cleared_when_peer_readded() {
  use crate::{AppendResponse, ConfChange, ConfChangeType, Message, Term};

  let (mut ep, mut log, mut stable, d, _idx) = leader_after_removing_node3();
  assert!(ep.pending_farewells.contains_key(&3u64));

  // Re-add node 3 via a committed AddNode(3): the fold sees it re-enter the tracker.
  let cc = ConfChange::new(ConfChangeType::AddNode, 3u64, bytes::Bytes::new());
  let idx2 = ep
    .propose_conf_change(d, &mut log, &stable, cc)
    .expect("AddNode(3) must be accepted");
  ep.flush_appends(d, &log, &stable);
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
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
      idx2,
    )),
  );
  assert!(ep.tracker.progress(&3u64).is_some(), "node 3 is re-added");
  assert!(
    !ep.pending_farewells.contains_key(&3u64),
    "a re-added peer's stale farewell retry is dropped"
  );
}

/// The compacted wall burns a shot without panic: a retry whose suffix has been compacted below the
/// peer's proven match returns false inside `send_farewell_append` and simply consumes a shot.
#[test]
fn farewell_retry_compacted_wall_burns_shot_without_panic() {
  use crate::{Entry, EntryKind, Term};
  use core::time::Duration;

  let (mut ep, mut log, mut stable, d, idx) = leader_after_removing_node3();
  let shots_before = ep.pending_farewells.get(&3u64).unwrap().shots_left;

  // Extend the log with an (uncommitted) tail so compacting past the removal leaves it non-empty,
  // then compact away everything at/below the removal — a retry's append now hits the compacted wall
  // (`next < first_index`). Compact BEFORE the first tick, so the FRONT-LOADED shot hits the wall.
  log.force_append(&[Entry::new(
    Term::new(1),
    idx.next(),
    EntryKind::Empty,
    bytes::Bytes::new(),
  )]);
  log.compact(idx);

  // Tick 1 front-loads the shot, which hits the wall — `send_farewell_append` returns false and the
  // append arm falls back to a clamped Heartbeat (`min(commit, matched) = matched < idx`, so it does
  // NOT carry the removal past the compacted wall). No panic; the shot still burns.
  ep.handle_timeout(d + Duration::from_millis(150), &mut log, &mut stable);
  let to3 = drain_to(&mut ep, 3u64);
  assert!(
    to3
      .iter()
      .all(|m| matches!(m, Message::Heartbeat(hb) if hb.commit() < idx)),
    "a compacted-wall retry falls back to a clamped Heartbeat below the removal, never an append"
  );
  assert_eq!(
    ep.pending_farewells.get(&3u64).unwrap().shots_left,
    shots_before - 1,
    "the shot is still burned at the compacted wall"
  );
}

/// The lost-farewell class, CURED by the retry end-to-end across two endpoints: the leader's shot-1
/// farewell append is DROPPED (never handed to n3); the leader re-drives shot 2 on its next
/// election-timeout beat; n3 — pre-vote ON, still ignorant — even fires its own election timer
/// without inflating its term (a non-disruptive probe), then receives the retry and applies its OWN
/// removal (the `ConfChanged → RemovedSelf` mirror). The removal is always n3's own applied
/// committed state, never a bare assertion.
#[test]
fn farewell_retry_cures_dropped_farewell_without_disruptive_campaign() {
  use crate::{AppendEntries, Config, Entry, EntryKind, Event, Message, Term};
  use core::time::Duration;

  // Leader side: remove n3; the shot-1 farewell append is DROPPED (we never deliver it to n3).
  let (mut ep, mut log, mut stable, d, idx) = leader_after_removing_node3();

  // Re-drive: the FIRST leader tick front-loads shot 2 immediately (before n3's election deadline).
  ep.handle_timeout(d + Duration::from_millis(150), &mut log, &mut stable);
  let mut to3 = drain_to(&mut ep, 3u64);
  assert_eq!(
    to3.len(),
    1,
    "the retry re-delivers exactly one farewell append"
  );
  let retry = to3.pop().unwrap();
  assert!(
    matches!(retry, Message::AppendEntries(_)),
    "the retry re-issues the lost farewell append"
  );

  // n3 side, pre-vote ON, holding [no-op@1, conf@idx] but ignorant of its removal's commit.
  let cfg3 = Config::try_new(
    3u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_pre_vote(true);
  let mut c = Endpoint::new(cfg3, Instant::ORIGIN, 9, CountSm::default());
  let mut log3 = VecLog::default();
  let mut stable3 = NoopStable::default();
  c.handle_message(
    Instant::ORIGIN,
    &mut log3,
    &mut stable3,
    1u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(1),
      1u64,
      Index::ZERO,
      Term::ZERO,
      std::vec![
        Entry::new(
          Term::new(1),
          Index::new(1),
          EntryKind::Empty,
          bytes::Bytes::new()
        ),
        Entry::new(Term::new(1), idx, EntryKind::ConfChange, remove3_payload()),
      ],
      Index::new(1), // commit through the no-op only — n3 stays ignorant of its own removal
    )),
  );
  c.handle_storage(Instant::ORIGIN, &mut log3, &mut stable3);
  while c.poll_message().is_some() {}
  while c.poll_event().is_some() {}
  assert!(
    c.tracker.is_voter(&3u64),
    "n3 is still an (ignorant) voter before the retry lands"
  );

  // n3's election timer fires: pre-vote ON ⇒ a non-inflating probe, never a real campaign.
  let td = c.poll_timeout().unwrap();
  c.handle_timeout(td, &mut log3, &mut stable3);
  assert_eq!(
    c.term(),
    Term::new(1),
    "pre-vote must not inflate n3's term"
  );
  assert!(!c.role().is_leader(), "n3 never wins a disruptive election");
  while c.poll_message().is_some() {}
  while c.poll_event().is_some() {}

  // The retry lands: n3 applies its OWN removal — the ConfChanged → RemovedSelf mirror.
  c.handle_message(td, &mut log3, &mut stable3, 1u64, retry);
  c.handle_storage(td, &mut log3, &mut stable3);
  let removed = core::iter::from_fn(|| c.poll_event())
    .any(|e| matches!(e, Event::ConfChanged(cc) if !cc.conf().voters().contains(&3u64)));
  assert!(
    removed,
    "the retry makes n3 apply its own removal (ConfChanged excludes n3)"
  );
  assert_eq!(
    c.term(),
    Term::new(1),
    "n3 self-removed without ever inflating its term"
  );
}

/// The `has_pending_farewells` quiesce-eligibility read: true while a removal's farewell budget is
/// unspent, false once it drains. A multi-group host consults this so a group cannot quiesce while it
/// still owes blind re-deliveries (the removed peer's ack is unobservable).
#[test]
fn has_pending_farewells_tracks_the_retry_budget() {
  use core::time::Duration;

  let (mut ep, mut log, mut stable, d, _idx) = leader_after_removing_node3();
  assert!(
    ep.has_pending_farewells(),
    "the leader owes farewell re-deliveries right after the removal"
  );

  let et = Duration::from_millis(1000);
  // Tick 1 FRONT-LOADS shot 2 (the budget decrements to one remaining) — still owed.
  let t1 = d + Duration::from_millis(150);
  ep.handle_timeout(t1, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  assert!(
    ep.has_pending_farewells(),
    "still owed while a shot remains in the budget"
  );

  // Tick 2 fires the last shot → the budget drains.
  ep.handle_timeout(t1 + et, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  assert!(
    !ep.has_pending_farewells(),
    "cleared once the blind budget drains"
  );
}

/// Drive a RemoveNode(3) where node 3 is CAUGHT UP (it acked the conf entry, so `match = idx >=
/// removal`): the fold fires the commit-carrying Heartbeat arm and — with the both-arms fix — ALSO
/// populates the retry budget. Returns the leader with its stores, the tick instant, and the removal
/// index; node 3's shot-1 heartbeat is already drained.
fn leader_after_removing_caught_up_node3()
-> (Endpoint<u64, CountSm>, VecLog, NoopStable, Instant, Index) {
  use crate::{AppendResponse, ConfChange, ConfChangeType, Message, Term};

  let (mut ep, mut log, mut stable, d) = make_three_node_leader();
  let cc = ConfChange::new(ConfChangeType::RemoveNode, 3u64, bytes::Bytes::new());
  let idx = ep
    .propose_conf_change(d, &mut log, &stable, cc)
    .expect("RemoveNode(3) must be accepted");
  ep.flush_appends(d, &log, &stable);
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  // Node 3 ACKS the conf entry (match = idx): its ack completes the quorum {1,3}, the removal commits
  // + applies, and node 3 is pruned CAUGHT-UP (match >= idx → the commit-carrying heartbeat arm).
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    3u64,
    Message::AppendResponse(AppendResponse::new(
      Term::new(1),
      3u64,
      false,
      Index::ZERO,
      Term::ZERO,
      idx,
    )),
  );
  assert!(ep.tracker.progress(&3u64).is_none(), "node 3 pruned");
  while ep.poll_message().is_some() {} // drain the shot-1 heartbeat
  while ep.poll_event().is_some() {}
  (ep, log, stable, d, idx)
}

/// Populate-on-fire, the CAUGHT-UP arm: a caught-up removal fires a commit-carrying Heartbeat and
/// still schedules a retry — the both-arms fix, so a lost caught-up farewell is no longer stranded.
#[test]
fn farewell_retry_populated_on_caught_up_fire() {
  let (ep, _log, _stable, _d, idx) = leader_after_removing_caught_up_node3();
  let retry = ep
    .pending_farewells
    .get(&3u64)
    .expect("the caught-up (heartbeat-arm) farewell must ALSO schedule a retry");
  assert!(
    retry.matched >= idx,
    "a caught-up peer freezes matched >= idx, so every re-drive re-derives the heartbeat arm"
  );
  assert_eq!(retry.idx, idx);
  assert_eq!(retry.shots_left, FAREWELL_RETRY_SHOTS);
  assert!(retry.next_at.is_none());
}

/// The caught-up arm's re-drive emits a Heartbeat (not an append) — the arm is derived from the
/// frozen `matched` on every shot.
#[test]
fn farewell_retry_caught_up_arm_redrives_a_heartbeat() {
  use core::time::Duration;

  let (mut ep, mut log, mut stable, d, _idx) = leader_after_removing_caught_up_node3();
  let et = Duration::from_millis(1000);
  let t1 = d + Duration::from_millis(150);
  ep.handle_timeout(t1, &mut log, &mut stable); // schedule
  while ep.poll_message().is_some() {}
  ep.handle_timeout(t1 + et, &mut log, &mut stable); // shot 2
  let to3 = drain_to(&mut ep, 3u64);
  assert_eq!(
    to3.len(),
    1,
    "the caught-up retry re-delivers exactly one farewell"
  );
  assert!(
    matches!(to3[0], Message::Heartbeat(_)),
    "the caught-up arm re-drives a Heartbeat, not an append"
  );
}

/// The lost caught-up-farewell class, CURED by the retry: remove a CAUGHT-UP voter, DROP the shot-1
/// commit-carrying heartbeat, re-drive shot 2, deliver it to n3 — n3 advances its commit, applies its
/// OWN removal (the ConfChanged → RemovedSelf mirror), term unchanged. This is the leg the earlier
/// design stranded (the caught-up arm never populated the budget).
#[test]
fn farewell_retry_cures_dropped_caught_up_heartbeat() {
  use crate::{AppendEntries, Config, Entry, EntryKind, Event, Message, Term};
  use core::time::Duration;

  // Leader side: remove a caught-up n3; the shot-1 heartbeat is DROPPED (never handed to n3).
  let (mut ep, mut log, mut stable, d, idx) = leader_after_removing_caught_up_node3();

  // Re-drive: the FIRST leader tick front-loads shot 2 immediately.
  ep.handle_timeout(d + Duration::from_millis(150), &mut log, &mut stable);
  let mut to3 = drain_to(&mut ep, 3u64);
  assert_eq!(
    to3.len(),
    1,
    "the retry re-delivers exactly one farewell heartbeat"
  );
  let retry = to3.pop().unwrap();
  assert!(
    matches!(retry, Message::Heartbeat(_)),
    "the caught-up retry is the commit-carrying Heartbeat"
  );

  // n3 side: it HOLDS [no-op@1, conf@idx] (it acked the entry) but is ignorant of the commit.
  let cfg3 = Config::try_new(
    3u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut c = Endpoint::new(cfg3, Instant::ORIGIN, 9, CountSm::default());
  let mut log3 = VecLog::default();
  let mut stable3 = NoopStable::default();
  c.handle_message(
    Instant::ORIGIN,
    &mut log3,
    &mut stable3,
    1u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(1),
      1u64,
      Index::ZERO,
      Term::ZERO,
      std::vec![
        Entry::new(
          Term::new(1),
          Index::new(1),
          EntryKind::Empty,
          bytes::Bytes::new()
        ),
        Entry::new(Term::new(1), idx, EntryKind::ConfChange, remove3_payload()),
      ],
      Index::new(1), // n3 holds the conf entry but is ignorant of its commit
    )),
  );
  c.handle_storage(Instant::ORIGIN, &mut log3, &mut stable3);
  while c.poll_message().is_some() {}
  while c.poll_event().is_some() {}
  assert!(
    c.commit_index() < idx,
    "n3 has not yet learned the removal committed"
  );
  let td = c.poll_timeout().unwrap();

  // The retried heartbeat (replacing the dropped shot 1) advances n3's commit and applies the removal.
  c.handle_message(td, &mut log3, &mut stable3, 1u64, retry);
  c.handle_storage(td, &mut log3, &mut stable3);
  assert!(
    c.commit_index() >= idx,
    "the retried heartbeat advances n3's commit past the removal"
  );
  let removed = core::iter::from_fn(|| c.poll_event())
    .any(|e| matches!(e, Event::ConfChanged(cc) if !cc.conf().voters().contains(&3u64)));
  assert!(
    removed,
    "the retried heartbeat makes n3 apply its own removal (ConfChanged excludes n3)"
  );
  assert_eq!(
    c.term(),
    Term::new(1),
    "n3 self-removed without inflating its term"
  );
}

/// The adversarial ordering the front-load + parking cure (the config the retry protects: DEFAULT
/// flags, pre_vote and check_quorum OFF): a removed voter whose shot-1 farewell was DROPPED campaigns
/// at a higher term BEFORE any retry and deposes the live leader. The step-down PARKS the budget (it
/// is not cleared); the old leader re-wins among the live members, `become_leader` re-arms the entry,
/// and the FIRST post-re-election tick front-loads the farewell — the removed peer applies its removal
/// and self-removes, so it can never campaign a second time.
#[test]
fn farewell_retry_survives_deposition_and_cures_on_re_election() {
  use crate::{Entry, EntryKind, Event, Message, Term, VoteResponse};
  use core::time::Duration;

  // Node 1 leads and removes lagging node 3 (default flags); shot 1 is DROPPED (drained by the helper).
  let (mut ep, mut log, mut stable, d, idx) = leader_after_removing_node3();
  assert!(ep.has_pending_farewells());

  // Node 3, ignorant of its removal (DEFAULT flags → no pre-vote), campaigns at a higher term.
  let (mut n3, mut n3log, mut n3stable) = node3_with_log(
    std::vec![Entry::new(
      Term::new(1),
      Index::new(1),
      EntryKind::Empty,
      bytes::Bytes::new()
    )],
    Index::new(1),
  );
  let n3d = n3.poll_timeout().unwrap();
  n3.handle_timeout(n3d, &mut n3log, &mut n3stable);
  let rv = core::iter::from_fn(|| n3.poll_message())
    .find(|o| matches!(o.message(), Message::RequestVote(r) if !r.pre_vote()))
    .expect("node 3 (pre-vote OFF) campaigns with a real vote")
    .into_parts()
    .1;
  assert_eq!(
    n3.term(),
    Term::new(2),
    "node 3 inflated the term (default flags, no pre-vote)"
  );

  // Deliver node 3's RequestVote to the leader: default flags mean NO in_lease block, so the leader
  // adopts the higher term and STEPS DOWN — but the budget PARKS.
  ep.handle_message(d, &mut log, &mut stable, 3u64, rv);
  assert!(
    !ep.role().is_leader(),
    "the removed voter deposed the live leader"
  );
  assert!(
    ep.pending_farewells.contains_key(&3u64),
    "the budget PARKED across the deposition"
  );
  assert!(
    !ep.has_pending_farewells(),
    "parked on a follower — the leader-gated accessor reads false"
  );

  // The live group re-elects node 1 (among {1, 2}); become_leader re-arms the parked entry.
  let rd = ep
    .poll_timeout()
    .expect("the deposed leader re-arms its election timer");
  ep.handle_timeout(rd, &mut log, &mut stable);
  let ct = ep.term();
  ep.handle_message(
    rd,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResponse(VoteResponse::new(ct, 2u64, false, false)),
  );
  ep.handle_storage(rd, &mut log, &mut stable);
  assert!(
    ep.role().is_leader(),
    "the old leader re-wins among the live members"
  );
  assert_eq!(
    ep.pending_farewells.get(&3u64).unwrap().next_at,
    None,
    "become_leader re-armed the parked entry to fire at the first tick"
  );
  ep.handle_storage(rd, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  // The FIRST leader tick front-loads the farewell to node 3.
  let ft = rd + Duration::from_millis(150);
  ep.handle_timeout(ft, &mut log, &mut stable);
  let farewell = drain_to(&mut ep, 3u64)
    .into_iter()
    .next()
    .expect("the first post-re-election tick front-loads the farewell");

  // Node 3 receives it, applies its OWN removal, and self-removes — never a second campaign.
  n3.handle_message(n3d, &mut n3log, &mut n3stable, 1u64, farewell);
  n3.handle_storage(n3d, &mut n3log, &mut n3stable);
  let removed = core::iter::from_fn(|| n3.poll_event())
    .any(|e| matches!(e, Event::ConfChanged(cc) if !cc.conf().voters().contains(&3u64)));
  assert!(
    removed,
    "the front-loaded farewell makes node 3 apply its own removal (RemovedSelf)"
  );
  assert!(
    !n3.tracker.is_voter(&3u64),
    "node 3 is now a non-voter — it can never campaign a second time"
  );
  let _ = idx;
}

/// The round-1 (quiescence gate) × round-3 (parking) seam: a CheckQuorum step-down in the SAME
/// `handle_timeout` tick that a farewell shot is DUE must not spend the shot as a follower. The role
/// guard in `drive_pending_farewells` makes a demoted tick send, decrement, and remove nothing, so the
/// parked budget outlives the demotion and re-election re-arms and delivers it. Without the guard the
/// last shot dies post-demotion and the removed peer is never cured.
#[test]
fn farewell_retry_survives_a_checkquorum_demotion_in_the_same_tick() {
  use crate::{
    AppendResponse, ConfChange, ConfChangeType, Entry, EntryKind, Event, Message, Term,
    VoteResponse,
  };
  use core::time::Duration;

  // A CHECK-QUORUM leader (node 1) removes lagging node 3.
  let cfg = cq_config(1, std::vec![1u64, 2, 3]);
  let mut ep: Endpoint<u64, CountSm> = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = NoopStable::default();
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
  ep.handle_storage(d, &mut log, &mut stable);
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    3u64,
    Message::AppendResponse(AppendResponse::new(
      Term::new(1),
      3u64,
      false,
      Index::ZERO,
      Term::ZERO,
      Index::new(1),
    )),
  );
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
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}
  let cc = ConfChange::new(ConfChangeType::RemoveNode, 3u64, bytes::Bytes::new());
  let idx = ep
    .propose_conf_change(d, &mut log, &stable, cc)
    .expect("RemoveNode(3) accepted");
  ep.flush_appends(d, &log, &stable);
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
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
  assert!(ep.tracker.progress(&3u64).is_none(), "node 3 pruned");
  while ep.poll_message().is_some() {} // drain shot 1
  while ep.poll_event().is_some() {}
  assert!(ep.role().is_leader() && ep.has_pending_farewells());

  // Tick 1, quorum still active from node 2's acks: front-loads shot 2 (one shot remains) and, in the
  // same beat, re-arms the CheckQuorum deadline — so the surviving shot's wall and the CQ deadline now
  // coincide exactly one election timeout out.
  let t1 = ep
    .election_deadline
    .expect("a check-quorum leader arms an election deadline");
  ep.handle_timeout(t1, &mut log, &mut stable);
  while ep.poll_message().is_some() {} // drain shot 2
  assert!(
    ep.role().is_leader(),
    "quorum active at tick 1 keeps the leader"
  );
  assert_eq!(
    ep.pending_farewells.get(&3u64).unwrap().shots_left,
    1,
    "exactly one shot remains"
  );
  let due_at = ep
    .pending_farewells
    .get(&3u64)
    .unwrap()
    .next_at
    .expect("the surviving shot is armed");
  assert_eq!(
    ep.election_deadline,
    Some(due_at),
    "the last shot's wall coincides with the CheckQuorum deadline"
  );

  // Tick 2 at that instant: quorum is now INACTIVE (node 2's activity was reset at tick 1 and no new
  // ack arrived), so the SAME tick takes the CheckQuorum demotion AND the last shot is due. The role
  // guard must keep the demoted tick from spending it.
  ep.handle_timeout(due_at, &mut log, &mut stable);
  assert!(
    ep.role().is_follower(),
    "quorum inactive → CheckQuorum demotes this very tick"
  );
  assert!(
    drain_to(&mut ep, 3u64).is_empty(),
    "the demoted tick sent NO farewell (role guard)"
  );
  let parked = ep
    .pending_farewells
    .get(&3u64)
    .expect("the demoted tick did NOT remove the entry");
  assert_eq!(
    parked.shots_left, 1,
    "the demoted tick did NOT decrement the shot"
  );
  assert!(
    !ep.has_pending_farewells(),
    "the leader-gated accessor reads false on the demoted follower"
  );

  // Re-election re-arms the parked shot; the first leader tick delivers it.
  let rd = ep.poll_timeout().unwrap();
  ep.handle_timeout(rd, &mut log, &mut stable);
  let ct = ep.term();
  ep.handle_message(
    rd,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResponse(VoteResponse::new(ct, 2u64, false, false)),
  );
  ep.handle_storage(rd, &mut log, &mut stable);
  assert!(ep.role().is_leader(), "the endpoint re-wins leadership");
  assert_eq!(
    ep.pending_farewells.get(&3u64).unwrap().next_at,
    None,
    "become_leader re-armed the surviving shot"
  );
  ep.handle_storage(rd, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}
  let ft = rd + Duration::from_millis(150);
  ep.handle_timeout(ft, &mut log, &mut stable);
  let farewell = drain_to(&mut ep, 3u64)
    .into_iter()
    .next()
    .expect("the first post-re-election tick delivers the surviving shot");

  // Node 3 receives it and applies its own removal.
  let (mut n3, mut n3log, mut n3stable) = node3_with_log(
    std::vec![Entry::new(
      Term::new(1),
      Index::new(1),
      EntryKind::Empty,
      bytes::Bytes::new()
    )],
    Index::new(1),
  );
  let n3d = n3.poll_timeout().unwrap();
  n3.handle_message(n3d, &mut n3log, &mut n3stable, 1u64, farewell);
  n3.handle_storage(n3d, &mut n3log, &mut n3stable);
  let removed = core::iter::from_fn(|| n3.poll_event())
    .any(|e| matches!(e, Event::ConfChanged(cc) if !cc.conf().voters().contains(&3u64)));
  assert!(
    removed,
    "the surviving shot, delivered after re-election, cures the removed peer"
  );
  let _ = idx;
}

/// A 3-voter leader (node 1) on an ASYNC stable that removed lagging node 3 (parking a farewell budget)
/// and then stepped down — the parked-follower shape a snapshot install lands on. The async stable is
/// what lets `install_snapshot_now` complete (its blob becomes durable) in these tests.
fn async_parked_removal_of_node3() -> (Endpoint<u64, CountSm>, VecLog, AsyncStable, Instant) {
  use crate::{AppendResponse, ConfChange, ConfChangeType, Message, Term};
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
  ep.handle_storage(d, &mut log, &mut stable);
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    3u64,
    Message::AppendResponse(AppendResponse::new(
      Term::new(1),
      3u64,
      false,
      Index::ZERO,
      Term::ZERO,
      Index::new(1),
    )),
  );
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
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}
  let cc = ConfChange::new(ConfChangeType::RemoveNode, 3u64, bytes::Bytes::new());
  let idx = ep
    .propose_conf_change(d, &mut log, &stable, cc)
    .expect("RemoveNode(3) accepted");
  ep.flush_appends(d, &log, &stable);
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
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
  while ep.poll_event().is_some() {}
  assert!(
    ep.pending_farewells.contains_key(&3u64),
    "the removal parked a farewell budget"
  );
  ep.step_down_to_follower(crate::Now::monotonic(d));
  (ep, log, stable, d)
}

/// Edge 2 (the round-5 staleness-bypass close): a farewell entry whose peer is RE-ADMITTED by a
/// snapshot install while the map is parked on a follower is pruned IN PLACE at the install —
/// `install_snapshot_now` mirrors the log-applied re-add prune against the rebuilt tracker, independent
/// of any re-election.
#[test]
fn farewell_retry_pruned_in_place_when_a_snapshot_readmits_the_peer() {
  use crate::{ConfState, InstallSnapshot, Message, SnapshotMeta, Term};

  let (mut ep, mut log, mut stable, d) = async_parked_removal_of_node3();
  assert!(
    ep.pending_farewells.contains_key(&3u64),
    "the budget parks across the demotion"
  );
  assert!(
    ep.tracker.progress(&3u64).is_none(),
    "node 3 is removed before the install"
  );

  // A newer snapshot whose ConfState RE-ADMITS node 3 installs on the parked follower.
  let meta = SnapshotMeta::new(
    Index::new(5),
    Term::new(2),
    ConfState::from_voters(std::vec![1u64, 2u64, 3u64]),
  );
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::InstallSnapshot(InstallSnapshot::new(
      Term::new(2),
      2u64,
      meta,
      encode_snapshot(9u64),
    )),
  );
  ep.handle_storage(d, &mut log, &mut stable); // SnapshotWritten -> install_snapshot_now
  while ep.poll_message().is_some() {}

  assert!(
    ep.tracker.progress(&3u64).is_some(),
    "the snapshot re-admitted node 3 as a voter"
  );
  assert!(
    !ep.pending_farewells.contains_key(&3u64),
    "install_snapshot_now pruned the obsolete removal IN PLACE"
  );
}

/// The staleness bypass end to end: a removal parked across a demotion, a snapshot that RE-ADMITS its
/// target, then re-election. The install drops the entry (edge 2) and `become_leader`'s reconcile would
/// drop it anyway (edge 1), so no obsolete removal is ever re-armed against the current voter it names —
/// closing the hazard where a rejoiner on the old prefix could commit it and self-remove.
#[test]
fn farewell_retry_dropped_across_a_snapshot_readmit_and_re_election() {
  use crate::{ConfState, InstallSnapshot, Message, SnapshotMeta, Term, VoteResponse};
  use core::time::Duration;

  let (mut ep, mut log, mut stable, d) = async_parked_removal_of_node3();
  assert!(ep.pending_farewells.contains_key(&3u64));

  let meta = SnapshotMeta::new(
    Index::new(5),
    Term::new(2),
    ConfState::from_voters(std::vec![1u64, 2u64, 3u64]),
  );
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::InstallSnapshot(InstallSnapshot::new(
      Term::new(2),
      2u64,
      meta,
      encode_snapshot(9u64),
    )),
  );
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  assert!(ep.tracker.progress(&3u64).is_some(), "node 3 re-admitted");
  assert!(
    !ep.pending_farewells.contains_key(&3u64),
    "the entry was DROPPED at the install (edge 2)"
  );

  // Re-elect node 1 among the re-admitted {1, 2, 3}.
  let rd = ep.poll_timeout().unwrap();
  ep.handle_timeout(rd, &mut log, &mut stable);
  let ct = ep.term();
  ep.handle_message(
    rd,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResponse(VoteResponse::new(ct, 2u64, false, false)),
  );
  ep.handle_storage(rd, &mut log, &mut stable);
  assert!(ep.role().is_leader(), "re-elected");
  assert!(
    ep.pending_farewells.is_empty(),
    "nothing for become_leader to re-arm — no obsolete removal survives"
  );
  assert!(
    ep.tracker.progress(&3u64).is_some(),
    "node 3 remains a tracked voter"
  );

  // The first leader tick emits NO farewell: the map is empty, so `drive_pending_farewells` sends none.
  while ep.poll_message().is_some() {}
  let ft = rd + Duration::from_millis(150);
  ep.handle_timeout(ft, &mut log, &mut stable);
  assert!(
    ep.pending_farewells.is_empty(),
    "still no farewell entry after the first leader tick"
  );
}

/// The companion: the cure must not OVER-prune. A snapshot whose ConfState still EXCLUDES the removed
/// peer leaves the entry intact — it survives the install (edge 2 keeps a still-untracked target),
/// re-arms at re-election, and delivers its surviving shot.
#[test]
fn farewell_retry_survives_a_snapshot_that_keeps_the_peer_removed() {
  use crate::{ConfState, InstallSnapshot, Message, SnapshotMeta, Term, VoteResponse};
  use core::time::Duration;

  let (mut ep, mut log, mut stable, d) = async_parked_removal_of_node3();
  let shots = ep.pending_farewells.get(&3u64).unwrap().shots_left;

  // A snapshot that KEEPS node 3 removed (voters {1, 2}).
  let meta = SnapshotMeta::new(
    Index::new(5),
    Term::new(2),
    ConfState::from_voters(std::vec![1u64, 2u64]),
  );
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::InstallSnapshot(InstallSnapshot::new(
      Term::new(2),
      2u64,
      meta,
      encode_snapshot(9u64),
    )),
  );
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  assert!(
    ep.tracker.progress(&3u64).is_none(),
    "node 3 stays removed under this snapshot"
  );
  let parked = ep
    .pending_farewells
    .get(&3u64)
    .expect("the entry SURVIVES a still-removed snapshot (the cure does not over-prune)");
  assert_eq!(
    parked.shots_left, shots,
    "the surviving entry keeps its budget intact"
  );

  // Re-elect: become_leader re-arms the still-valid entry.
  let rd = ep.poll_timeout().unwrap();
  ep.handle_timeout(rd, &mut log, &mut stable);
  let ct = ep.term();
  ep.handle_message(
    rd,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResponse(VoteResponse::new(ct, 2u64, false, false)),
  );
  ep.handle_storage(rd, &mut log, &mut stable);
  assert!(ep.role().is_leader());
  assert_eq!(
    ep.pending_farewells.get(&3u64).unwrap().next_at,
    None,
    "become_leader re-armed the still-valid entry"
  );

  // The first tick delivers the surviving shot to node 3 (a non-member -> the farewell is the only
  // message it receives).
  while ep.poll_message().is_some() {}
  let ft = rd + Duration::from_millis(150);
  ep.handle_timeout(ft, &mut log, &mut stable);
  assert!(
    !drain_to(&mut ep, 3u64).is_empty(),
    "the surviving shot is delivered to the still-removed peer"
  );
}

/// Edge 1 (the general staleness backstop): `become_leader`'s re-arm RECONCILES against the live tracker
/// — an entry whose peer is currently TRACKED (re-admitted by ANY path while parked) is DROPPED, never
/// re-armed. Constructed directly with a contrived entry for a live voter, because the log-apply and
/// snapshot prunes preempt this at every natural mutation site; this red-proofs the backstop itself.
#[test]
fn become_leader_re_arm_drops_an_entry_whose_peer_is_tracked() {
  use crate::{Message, VoteResponse};

  let (mut ep, mut log, mut stable, d) = make_three_node_leader();
  assert!(
    ep.tracker.progress(&2u64).is_some(),
    "node 2 is a current voter (tracked)"
  );
  // Contrive a STALE parked entry for node 2 — a live voter the tracker still holds. No natural path
  // produces this (the prunes preempt it); edge 1 must drop it at re-arm regardless.
  ep.pending_farewells.insert(
    2u64,
    super::super::FarewellRetry {
      matched: Index::new(1),
      idx: Index::new(2),
      shots_left: 1,
      next_at: None,
    },
  );
  ep.step_down_to_follower(crate::Now::monotonic(d));
  assert!(
    ep.pending_farewells.contains_key(&2u64),
    "the entry parked across the demotion"
  );

  // Re-elect: become_leader's reconcile DROPS the entry because node 2 is tracked.
  let rd = ep.poll_timeout().unwrap();
  ep.handle_timeout(rd, &mut log, &mut stable);
  let ct = ep.term();
  ep.handle_message(
    rd,
    &mut log,
    &mut stable,
    3u64,
    Message::VoteResponse(VoteResponse::new(ct, 3u64, false, false)),
  );
  ep.handle_storage(rd, &mut log, &mut stable);
  assert!(ep.role().is_leader(), "re-elected");
  assert!(
    !ep.pending_farewells.contains_key(&2u64),
    "become_leader dropped the entry for the tracked peer — never re-arming an obsolete removal"
  );
}
