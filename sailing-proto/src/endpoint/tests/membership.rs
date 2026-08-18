use super::{super::*, *};
use crate::{
  ConfChangeSingle, ConfChangeV2, SnapshotMeta, VoteResponse,
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

/// THE ADVERSARIAL ORDERING, at the config the cure protects (DEFAULT flags — pre_vote and
/// check_quorum both OFF, so a removed voter's campaign carries a real, higher term). The honest
/// bound, stated as a test: that campaign DEPOSES the leader, exactly once, and the deposition is
/// self-healing — the removed peer cannot win a quorum of a configuration it is not in, the live
/// members re-elect, and the re-elected leadership holds the debt by the universal mint.
///
/// Nothing suppresses the peer's campaign to avoid that blip. Three successive topologies showed
/// that no local state can license muting another replica's reconciliation traffic: a leader whose
/// configuration history is stale cannot tell a departed peer from a re-added one, and every
/// variant of the shield composed into a permanent wedge. One bounded availability blip is the
/// price, and the first DELIVERED proactive offer is what makes it a one-time price.
#[test]
fn a_removed_voters_real_campaign_costs_exactly_one_deposition() {
  use crate::{Entry, EntryKind, Message, Role, Term};

  let (mut ep, mut log, mut stable, d, _idx) = leader_after_removing_node3();
  assert!(ep.has_pending_farewells());
  assert!(
    ep.has_courtesy_debts(),
    "the committed removal minted the courtesy debt beside the farewell budget"
  );

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
  assert_eq!(n3.term(), Term::new(2), "it inflated its own term doing so");

  // THE ONE DEPOSITION. The leader adopts and steps down — the universal term mechanism, untouched.
  ep.handle_message(d, &mut log, &mut stable, 3u64, rv);
  assert_eq!(
    ep.role(),
    Role::Follower,
    "the campaign deposes, exactly as Raft says"
  );
  assert_eq!(ep.term(), Term::new(2), "adopting the higher term");
  assert!(
    ep.pending_farewells.contains_key(&3u64) && ep.courtesy_owed.contains_key(&3u64),
    "and BOTH cure records park across it, to be re-armed on re-election"
  );

  // The live members re-elect: node 3 cannot win a quorum of a configuration it is not in.
  let rd = ep.poll_timeout().expect("the deposed leader re-arms");
  ep.handle_timeout(rd, &mut log, &mut stable);
  let ct = ep.term();
  ep.handle_message(
    rd,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResponse(crate::VoteResponse::new(ct, 2u64, false, false)),
  );
  ep.handle_storage(rd, &mut log, &mut stable);
  assert!(ep.role().is_leader(), "the live pair re-elects");
  assert!(ep.term() > n3.term(), "at a term above the removed peer's");
  apply_the_term_start_noop(&mut ep, &mut log, &mut stable, rd, &[2u64]);
  while ep.poll_message().is_some() {}

  // The cure lands from the re-elected leadership, and node 3 never campaigns again.
  ep.handle_timeout(rd + Duration::from_millis(150), &mut log, &mut stable);
  let farewell = drain_to(&mut ep, 3u64)
    .into_iter()
    .next()
    .expect("the first post-re-election tick re-drives the cure");
  n3.handle_message(n3d, &mut n3log, &mut n3stable, 1u64, farewell);
  n3.handle_storage(n3d, &mut n3log, &mut n3stable);
  assert!(
    core::iter::from_fn(|| n3.poll_event())
      .any(|e| matches!(e, crate::Event::ConfChanged(cc) if !cc.conf().voters().contains(&3u64))),
    "node 3 applies its own removal"
  );
  let settled = n3.term();
  for _ in 0..4 {
    let Some(t) = n3.poll_timeout() else { break };
    n3.handle_timeout(t, &mut n3log, &mut n3stable);
    n3.handle_storage(t, &mut n3log, &mut n3stable);
  }
  assert_eq!(
    n3.role(),
    Role::Follower,
    "NO SECOND deposition is ever possible"
  );
  assert_eq!(
    n3.term(),
    settled,
    "the cured peer never inflates a term again"
  );
}

/// PARKING across a deposition, driven from the one source that can still depose this leader: a
/// LIVE member at a higher term (the removed peer's own campaign is now dropped, so it cannot).
/// The step-down parks BOTH the farewell budget and the courtesy debt — they are the same peer's
/// unfinished business — the old leader re-wins among the live members, `become_leader` re-arms
/// both, and the FIRST post-re-election tick front-loads the farewell, so the removed peer applies
/// its removal and self-removes.
#[test]
fn farewell_retry_survives_deposition_and_cures_on_re_election() {
  use crate::{Entry, EntryKind, Event, Message, Term, VoteResponse};
  use core::time::Duration;

  let (mut ep, mut log, mut stable, d, idx) = leader_after_removing_node3();
  assert!(ep.has_pending_farewells());

  // A LIVE voter (node 2) campaigns at a higher term: not an owed peer, so the pre-pass applies
  // verbatim and the leader steps down.
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::RequestVote(crate::RequestVote::new(
      Term::new(2),
      2u64,
      Index::new(1),
      Term::new(1),
      false,
      false,
    )),
  );
  assert!(
    !ep.role().is_leader(),
    "a LIVE member's higher-term campaign still deposes (the pre-pass is unchanged for it)"
  );
  assert!(
    ep.pending_farewells.contains_key(&3u64),
    "the budget PARKED across the deposition"
  );
  assert!(
    ep.courtesy_owed.contains_key(&3u64),
    "and so did the courtesy debt"
  );
  assert!(
    !ep.has_pending_farewells() && !ep.has_courtesy_debts(),
    "parked on a follower — the leader-gated accessors read false"
  );

  // The live group re-elects node 1 (among {1, 2}); become_leader re-arms the parked entries.
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
  assert_eq!(
    ep.courtesy_owed.get(&3u64).unwrap().next_at,
    None,
    "and re-armed the courtesy debt on the same predicate"
  );
  ep.handle_storage(rd, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  // The FIRST leader tick front-loads the farewell to node 3.
  // The inherited-tail gate: this fresh leader's own no-op must apply before any cure runs.
  apply_the_term_start_noop(&mut ep, &mut log, &mut stable, rd, &[2u64]);
  let ft = rd + Duration::from_millis(150);
  ep.handle_timeout(ft, &mut log, &mut stable);
  let farewell = drain_to(&mut ep, 3u64)
    .into_iter()
    .next()
    .expect("the first post-re-election tick front-loads the farewell");

  // Node 3 receives it, applies its OWN removal, and self-removes — never a second campaign.
  let (mut n3, mut n3log, mut n3stable) = node3_with_log(
    std::vec![Entry::new(
      Term::new(1),
      Index::new(1),
      EntryKind::Empty,
      bytes::Bytes::new()
    )],
    Index::new(1),
  );
  let n3d = Instant::ORIGIN;
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
  // The inherited-tail gate: this fresh leader's own no-op must apply before any cure runs.
  apply_the_term_start_noop(&mut ep, &mut log, &mut stable, rd, &[2u64]);
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
  // The inherited-tail gate: this fresh leader's own no-op must apply before any cure runs.
  apply_the_term_start_noop(&mut ep, &mut log, &mut stable, rd, &[2u64]);
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
  // The inherited-tail gate: this fresh leader's own no-op must apply before any cure runs.
  apply_the_term_start_noop(&mut ep, &mut log, &mut stable, rd, &[2u64]);
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

// COURTESY SNAPSHOT (#95, the compacted removed peer) — the cure for the one class the farewell
// retry cannot reach.

/// A 3-voter leader on an `AsyncStable` (which has a real snapshot slot) that has COMMITTED and
/// APPLIED RemoveNode(3), with node 3 pruned from the tracker. Returns the leader and its stores.
fn leader_that_removed_node3() -> (Endpoint<u64, CountSm>, VecLog, AsyncStable, Instant, Index) {
  use crate::{AppendResponse, ConfChange, ConfChangeType, Index, Message, Term};
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
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  let cc = ConfChange::new(ConfChangeType::RemoveNode, 3u64, bytes::Bytes::new());
  let idx = ep
    .propose_conf_change(d, &mut log, &stable, cc)
    .expect("RemoveNode(3) must be accepted");
  ep.flush_appends(d, &log, &stable);
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
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(ep.tracker.progress(&3u64).is_none(), "node 3 pruned");
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}
  (ep, log, stable, d, idx)
}

/// Put a durable snapshot carrying the POST-REMOVAL ConfState into the leader's store — the state
/// the compacted class implies exists: a peer can only be past `first_index` because a capture at
/// or beyond the compaction point was taken and persisted.
fn give_leader_a_snapshot(
  ep: &mut Endpoint<u64, CountSm>,
  stable: &mut AsyncStable,
  at: Index,
  blob: bytes::Bytes,
) -> SnapshotMeta<u64> {
  use crate::{StableStore as _, Term};
  let meta = SnapshotMeta::new(at, Term::new(1), ep.tracker.conf_state());
  let opid = ep.mint_op_id_for_test();
  stable.submit_snapshot(opid, meta.clone(), blob);
  meta
}

/// A durable snapshot from BEFORE the removal: it predates the removal index AND its ConfState
/// still names the peer, so it cannot carry the removal to anyone.
fn give_leader_a_pre_removal_snapshot(
  ep: &mut Endpoint<u64, CountSm>,
  stable: &mut AsyncStable,
  at: Index,
) -> SnapshotMeta<u64> {
  use crate::{StableStore as _, Term, conf::ConfState};
  let meta = SnapshotMeta::new(
    at,
    Term::new(1),
    ConfState::from_voters(std::vec![1u64, 2, 3]),
  );
  let opid = ep.mint_op_id_for_test();
  stable.submit_snapshot(opid, meta.clone(), encode_count_snapshot(7));
  meta
}

/// Every courtesy `InstallSnapshot` the endpoint has queued for node 3, draining the rest (the
/// leader also answers the contact below with an ordinary pre-vote rejection).
fn courtesy_offers_to_node3(ep: &mut Endpoint<u64, CountSm>) -> Vec<Message<u64>> {
  core::iter::from_fn(|| ep.poll_message())
    .filter(|o| o.to() == 3u64)
    .map(|o| o.into_parts().1)
    .filter(|m| matches!(m, Message::InstallSnapshot(_)))
    .collect()
}

/// A pristine node-3 follower on an `AsyncStable` (the store that actually persists a blob), the
/// shape a compacted removed peer presents to an install: nothing committed locally.
fn node3_async() -> (Endpoint<u64, CountSm>, VecLog, AsyncStable) {
  use core::time::Duration;
  let cfg = Config::try_new(
    3u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  (
    Endpoint::new(cfg, Instant::ORIGIN, 9, CountSm::default()),
    VecLog::default(),
    AsyncStable::default(),
  )
}

/// Node 2 as a FOLLOWER that has applied the committed RemoveNode(3): the no-op@1 and the conf
/// change@2 arrive from leader node 1 with commit at the change, so node 2's tracker becomes
/// {1, 2} and the universal mint records the debt — all without node 2 ever leading.
fn node2_that_applied_the_removal() -> (Endpoint<u64, CountSm>, VecLog, AsyncStable) {
  use crate::{AppendEntries, Entry, EntryKind, Message, Term};
  use core::time::Duration;
  let cfg = Config::try_new(
    2u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 5, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();
  let entries = std::vec![
    Entry::new(Term::new(1), Index::new(1), EntryKind::Empty, Bytes::new()),
    Entry::new(
      Term::new(1),
      Index::new(2),
      EntryKind::ConfChange,
      remove3_payload()
    ),
  ];
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
      entries,
      Index::new(2),
    )),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  assert!(
    ep.tracker.progress(&3u64).is_none(),
    "node 2 applied the removal"
  );
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}
  (ep, log, stable)
}

/// [`node3_with_log`] on an `AsyncStable` — the store that actually persists a blob, so this
/// node 3 can both CAMPAIGN from a real log and complete a deferred snapshot install.
fn node3_with_log_async(
  entries: Vec<crate::Entry>,
  commit: Index,
) -> (Endpoint<u64, CountSm>, VecLog, AsyncStable) {
  use crate::{AppendEntries, Message, Term};
  use core::time::Duration;
  let cfg = Config::try_new(
    3u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut c = Endpoint::new(cfg, Instant::ORIGIN, 9, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();
  c.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
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
  c.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  while c.poll_message().is_some() {}
  while c.poll_event().is_some() {}
  (c, log, stable)
}

/// Node 3 applying the committed removal OF ITSELF — the self-removal path, for the class-check
/// that a replica never mints a debt naming itself.
fn node3_that_applied_its_own_removal() -> (Endpoint<u64, CountSm>, VecLog, AsyncStable) {
  use crate::{AppendEntries, Entry, EntryKind, Message, Term};
  use core::time::Duration;
  let cfg = Config::try_new(
    3u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 9, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();
  let entries = std::vec![
    Entry::new(Term::new(1), Index::new(1), EntryKind::Empty, Bytes::new()),
    Entry::new(
      Term::new(1),
      Index::new(2),
      EntryKind::ConfChange,
      remove3_payload()
    ),
  ];
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
      entries,
      Index::new(2),
    )),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  (ep, log, stable)
}

/// A removed peer's contact, in the shape an ignorant replica actually produces: a PRE-VOTE
/// campaign at a higher advertised term. The leader neither adopts the term nor steps down (the
/// pre-vote exemption), so the trigger runs while it is still the leader.
fn removed_peer_contact() -> Message<u64> {
  removed_peer_contact_from(3u64)
}

/// [`removed_peer_contact`] from an arbitrary sender id — the rotating-identity probe.
///
/// A PRE-VOTE advertising term 2, which is what a peer stuck at term 1 emits every time it probes:
/// pre-vote never inflates the sender, so its REAL term stays 1 and a term-1 leader's offer is
/// deliverable to it. (A peer that campaigns for REAL is the futility case, exercised separately.)
fn removed_peer_contact_from(from: u64) -> Message<u64> {
  use crate::{Index, RequestVote, Term};
  Message::RequestVote(RequestVote::new(
    Term::new(2),
    from,
    Index::new(2),
    Term::new(1),
    true,
    false,
  ))
}

/// THE COMPACTED-PEER CURE, end to end. Node 3's farewell budget is spent (its suffix is
/// compacted, so every retry burned its shot on the clamped-heartbeat fallback and the map is
/// empty). It then contacts the leader; the leader — whose committed configuration does not name
/// it and whose tracker has no Progress for it — ships ONE whole-blob courtesy `InstallSnapshot`
/// carrying the post-removal ConfState. Node 3 installs it, applies the excluding membership,
/// surfaces its own removal, and never campaigns again. A SECOND contact inside the cooldown ships
/// nothing: the throttle, not a delivery ack (which is unobservable — its Progress is pruned), is
/// what bounds the offer.
///
/// MUTATION: drop the trigger in `handle_message` → node 3 gets no install, stays ignorant, and
/// keeps campaigning; the assertions on the surfaced `ConfChanged` fail.
#[test]
fn a_courtesy_snapshot_cures_the_compacted_removed_peer() {
  use crate::{Event, Message, Role, StableStore as _};
  let (mut ep, mut log, mut stable, d, removal) = leader_that_removed_node3();
  // The farewell budget is spent — the compacted class, per the retry's own residual.
  ep.pending_farewells_clear_for_test();
  let blob = encode_count_snapshot(7);
  let meta = give_leader_a_snapshot(&mut ep, &mut stable, removal, blob.clone());
  assert!(
    !meta.conf().voters().contains(&3u64),
    "the capture carries the post-removal configuration"
  );

  ep.handle_message(d, &mut log, &mut stable, 3u64, removed_peer_contact());
  let mut offers = courtesy_offers_to_node3(&mut ep);
  assert_eq!(offers.len(), 1, "exactly one courtesy offer");
  assert!(ep.role().is_leader(), "a pre-vote probe never deposes");
  let offer = offers.pop().expect("one offer");
  {
    let Message::InstallSnapshot(is) = &offer else {
      unreachable!("filtered above")
    };
    assert_eq!(is.total_len(), 0, "v1 courtesy is the whole-blob shape");
    assert_eq!(is.data(), &blob, "the whole blob rides one frame");
    assert!(
      !is.snapshot().conf().voters().contains(&3u64),
      "the offer carries the ConfState that excludes the peer"
    );
  }

  // The throttle: a second contact inside the cooldown ships nothing.
  ep.handle_message(d, &mut log, &mut stable, 3u64, removed_peer_contact());
  assert!(
    courtesy_offers_to_node3(&mut ep).is_empty(),
    "one courtesy per peer per cooldown"
  );

  // Node 3 applies committed state and self-removes.
  let (mut c, mut log3, mut stable3) = node3_async();
  // Delivered VERBATIM: the exact message the leader emitted, never re-stamped or rebuilt.
  c.handle_message(d, &mut log3, &mut stable3, 1u64, offer);
  c.handle_storage(d, &mut log3, &mut stable3);
  let removed = core::iter::from_fn(|| c.poll_event())
    .any(|ev| matches!(ev, Event::ConfChanged(cc) if !cc.conf().voters().contains(&3u64)));
  assert!(
    removed,
    "the excluding install surfaces the removal to the embedder"
  );
  let before = c.term();
  for _ in 0..4 {
    let Some(t) = c.poll_timeout() else { break };
    c.handle_timeout(t, &mut log3, &mut stable3);
    c.handle_storage(t, &mut log3, &mut stable3);
  }
  assert_eq!(c.role(), Role::Follower, "the cured peer never campaigns");
  assert_eq!(c.term(), before, "and never inflates the term");
  assert!(stable3.durable_snapshot().is_some(), "the blob is durable");
}

/// COMPOSITION with the farewell retry: while node 3's blind budget is still live, the retry OWNS
/// the peer and courtesy ships nothing. Firing both would put a whole blob on the wire beside a
/// cheap append that is very likely to land — the courtesy is the POST-budget cure, not a parallel
/// one.
#[test]
fn a_peer_inside_its_farewell_budget_gets_no_courtesy() {
  let (mut ep, mut log, mut stable, d, removal) = leader_that_removed_node3();
  assert!(
    ep.has_pending_farewells(),
    "the removal armed a blind retry budget"
  );
  give_leader_a_snapshot(&mut ep, &mut stable, removal, encode_count_snapshot(7));

  ep.handle_message(d, &mut log, &mut stable, 3u64, removed_peer_contact());
  assert!(
    courtesy_offers_to_node3(&mut ep).is_empty(),
    "the cheaper cure owns the peer while its budget lasts"
  );
}

/// A blob the whole-blob shape cannot carry SKIPS silently: no send, no state change, no throttle
/// entry burned (so a later, smaller capture is still offered at once). Chunked courtesy would
/// need ack routing for a peer that has no `Progress` to route through, so v1 leaves the oversized
/// residual to the embedder's catalog reap — the golden architecture's own assignment.
#[test]
fn an_oversized_courtesy_blob_skips_without_sending() {
  let (mut ep, mut log, mut stable, d, removal) = leader_that_removed_node3();
  ep.pending_farewells_clear_for_test();
  // One byte past the store's per-call ceiling: the whole blob cannot be read in one call, so it
  // can never ride one frame either.
  let oversized =
    bytes::BytesMut::zeroed(crate::config::MAX_SNAPSHOT_CHUNK_BYTES as usize + 1).freeze();
  give_leader_a_snapshot(&mut ep, &mut stable, removal, oversized);

  ep.handle_message(d, &mut log, &mut stable, 3u64, removed_peer_contact());
  assert!(
    courtesy_offers_to_node3(&mut ep).is_empty(),
    "an unsendable courtesy leaves the wire untouched"
  );
  assert!(ep.role().is_leader(), "and the leader untouched");
}

/// A leader with NOTHING persisted offers nothing: no durable snapshot means nothing has been
/// compacted, so the farewell append can still reach the peer and there is no gap to close.
#[test]
fn no_durable_snapshot_means_no_courtesy() {
  let (mut ep, mut log, mut stable, d, _removal) = leader_that_removed_node3();
  ep.pending_farewells_clear_for_test();
  ep.handle_message(d, &mut log, &mut stable, 3u64, removed_peer_contact());
  assert!(
    courtesy_offers_to_node3(&mut ep).is_empty(),
    "nothing to offer, nothing sent"
  );
}

/// A FOLLOWER never offers a courtesy snapshot: it holds no authority over membership, and a
/// snapshot from a non-leader would be a bare assertion dressed as committed state. The trigger is
/// leader-gated at the dispatch site, so an identical contact on a follower is inert.
#[test]
fn a_follower_never_offers_a_courtesy_snapshot() {
  use crate::Role;
  let (mut ep, mut log, mut stable, d, removal) = leader_that_removed_node3();
  ep.pending_farewells_clear_for_test();
  give_leader_a_snapshot(&mut ep, &mut stable, removal, encode_count_snapshot(7));
  ep.step_down_to_follower_for_test(d);
  assert_eq!(ep.role(), Role::Follower);

  ep.handle_message(d, &mut log, &mut stable, 3u64, removed_peer_contact());
  assert!(
    courtesy_offers_to_node3(&mut ep).is_empty(),
    "a follower offers nothing"
  );
}

/// F1 — AUTHORIZATION IS THE DEBT. A peer this group NEVER removed — a never-member, a stranger
/// that merely authenticated into the cluster, a peer of some other group — is owed nothing, so
/// its traffic buys it nothing: no offer, no state change, and no entry conjured into the debt
/// map. The old trigger authorized by ABSENCE (∉ conf ∧ ∉ tracker), and absence is exactly what a
/// never-member looks like; the debt distinguishes them because only a committed removal mints one.
///
/// MUTATION: restore the absence test (`tracker.progress(from).is_none()`) as the trigger → the
/// stranger is served a whole snapshot and the offer assertion fails.
#[test]
fn a_never_member_is_owed_nothing_and_gets_nothing() {
  let (mut ep, mut log, mut stable, d, removal) = leader_that_removed_node3();
  ep.pending_farewells_clear_for_test();
  give_leader_a_snapshot(&mut ep, &mut stable, removal, encode_count_snapshot(7));
  let (term, role) = (ep.term(), ep.role());

  // Strangers 4..=9: authenticated senders this configuration has never named.
  for stranger in 4u64..=9 {
    ep.handle_message(
      d,
      &mut log,
      &mut stable,
      stranger,
      Message::RequestVote(crate::RequestVote::new(
        Term::new(2),
        stranger,
        Index::new(2),
        Term::new(1),
        true,
        false,
      )),
    );
    assert!(
      core::iter::from_fn(|| ep.poll_message())
        .all(|o| !matches!(o.into_parts().1, Message::InstallSnapshot(_))),
      "a never-member must never be offered a snapshot"
    );
  }
  assert_eq!(
    ep.courtesy_owed.len(),
    1,
    "contact cannot mint a debt — only this leader's own committed removals can"
  );
  assert!(
    ep.courtesy_owed.contains_key(&3u64),
    "and the one debt is the one removal that happened"
  );
  assert_eq!((ep.term(), ep.role()), (term, role), "and moves nothing");
}

/// F1, the rotating-identity form: cooldown bypass by minting fresh sender ids is structurally
/// moot, because the cooldown is not what bounds the work — the debt is. A thousand identities are
/// owed nothing a thousand times over, and the ONE real debt spends its own budget however many
/// identities knock.
#[test]
fn rotating_identities_cannot_bypass_the_courtesy_budget() {
  let (mut ep, mut log, mut stable, d, removal) = leader_that_removed_node3();
  ep.pending_farewells_clear_for_test();
  give_leader_a_snapshot(&mut ep, &mut stable, removal, encode_count_snapshot(7));

  let mut offers = 0usize;
  for id in 100u64..200 {
    ep.handle_message(d, &mut log, &mut stable, id, removed_peer_contact_from(id));
    offers += core::iter::from_fn(|| ep.poll_message())
      .filter(|o| matches!(o.message(), Message::InstallSnapshot(_)))
      .count();
  }
  assert_eq!(offers, 0, "100 fresh identities bought 100 nothings");
  assert_eq!(ep.courtesy_owed.len(), 1, "and grew the map by nothing");
}

/// A debt is discharged by EVIDENCE: the owed peer's own acknowledgement of the blob it was
/// offered. That is the only thing that ends it short of a re-add, self-removal, or the cap — and
/// it is what makes an offer lost in flight harmless, since nothing was consumed by sending it.
///
/// MUTATION: charge the debt at enqueue again → the assertion that a SECOND contact still offers
/// (nothing was spent by the first) fails, or the debt is gone before any ack arrives.
#[test]
fn a_courtesy_debt_is_discharged_by_the_peers_acknowledgement() {
  use crate::{SnapshotResponse, StableStore as _};
  let (mut ep, mut log, mut stable, d, removal) = leader_that_removed_node3();
  ep.pending_farewells_clear_for_test();
  give_leader_a_snapshot(&mut ep, &mut stable, removal, encode_count_snapshot(7));

  // The offer goes out and the debt is RETAINED — sending proves nothing.
  ep.handle_message(d, &mut log, &mut stable, 3u64, removed_peer_contact());
  let offers = courtesy_offers_to_node3(&mut ep);
  assert_eq!(offers.len(), 1, "the contact drew an offer");
  let Message::InstallSnapshot(is) = &offers[0] else {
    unreachable!("filtered above")
  };
  let boundary = is.snapshot().last_index();
  assert_eq!(
    ep.courtesy_owed.get(&3u64).map(|dbt| dbt.offered_index),
    Some(Some(boundary)),
    "the debt records what it offered, and stands until that is acknowledged"
  );

  // The peer installs and acknowledges the boundary. THAT discharges the debt.
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    3u64,
    Message::SnapshotResponse(SnapshotResponse::new(ep.term(), 3u64, false, boundary)),
  );
  assert!(
    !ep.courtesy_owed.contains_key(&3u64),
    "the peer's own acknowledgement is what ends the debt"
  );

  // Nothing more is ever sent to it, however long it keeps knocking.
  let cooldown = 3 * ep.config.election_timeout();
  let mut at = d;
  for _ in 0..4 {
    at = at + cooldown + core::time::Duration::from_millis(1);
    ep.handle_timeout(at, &mut log, &mut stable);
    ep.handle_message(at, &mut log, &mut stable, 3u64, removed_peer_contact());
    assert!(
      courtesy_offers_to_node3(&mut ep).is_empty(),
      "a discharged debt offers nothing further"
    );
  }
  let _ = stable.durable_snapshot();
}

/// A REJECT, and a mid-transfer PROGRESS ack, are not evidence of anything — only a completed
/// install at or past the offered boundary is. Nor is an ack BELOW the boundary.
#[test]
fn only_a_completed_install_ack_discharges_the_debt() {
  use crate::SnapshotResponse;
  let (mut ep, mut log, mut stable, d, removal) = leader_that_removed_node3();
  ep.pending_farewells_clear_for_test();
  give_leader_a_snapshot(&mut ep, &mut stable, removal, encode_count_snapshot(7));
  ep.handle_message(d, &mut log, &mut stable, 3u64, removed_peer_contact());
  let offers = courtesy_offers_to_node3(&mut ep);
  let Message::InstallSnapshot(is) = &offers[0] else {
    unreachable!("filtered above")
  };
  let boundary = is.snapshot().last_index();

  for (what, response) in [
    (
      "a reject",
      SnapshotResponse::new(ep.term(), 3u64, true, boundary),
    ),
    (
      "a mid-transfer progress ack",
      SnapshotResponse::new(ep.term(), 3u64, false, boundary).with_progress(true),
    ),
    (
      "an ack below the offered boundary",
      SnapshotResponse::new(
        ep.term(),
        3u64,
        false,
        Index::new(boundary.get().saturating_sub(1)),
      ),
    ),
  ] {
    ep.handle_message(
      d,
      &mut log,
      &mut stable,
      3u64,
      Message::SnapshotResponse(response),
    );
    assert!(
      ep.courtesy_owed.contains_key(&3u64),
      "{what} is not evidence the peer installed the removal"
    );
  }
}

/// RACE ONE — a DELAYED ack for an EARLIER offer. S1 is delivered and installed, but its ack is
/// slow; the cooldown fires, S2 (a newer capture at a higher boundary) goes out, and S2 is lost.
/// The delayed S1 ack then names a boundary BELOW the latest offer's and must still discharge:
/// every offer of a generation carries a ConfState that excludes the peer, so installing ANY of
/// them cured it. The floor is therefore the EARLIEST boundary offered, never the latest.
///
/// MUTATION: raise the floor to each retry's boundary → the S1 ack is measured against S2's index,
/// fails, and a peer that is already cured stays owed the cure forever.
#[test]
fn a_delayed_ack_for_an_earlier_offer_discharges_the_debt() {
  use crate::SnapshotResponse;
  use core::time::Duration;

  let (mut ep, mut log, mut stable, d, removal) = leader_that_removed_node3();
  ep.pending_farewells_clear_for_test();
  give_leader_a_snapshot(&mut ep, &mut stable, removal, encode_count_snapshot(7));

  // S1 — the generation's first offer. It is delivered and installed; the ack is still in flight.
  ep.handle_message(d, &mut log, &mut stable, 3u64, removed_peer_contact());
  let s1 = courtesy_offers_to_node3(&mut ep);
  assert_eq!(s1.len(), 1, "the contact offers the cure");
  let Message::InstallSnapshot(is1) = &s1[0] else {
    unreachable!("filtered above")
  };
  let b1 = is1.snapshot().last_index();

  // The cooldown elapses with a NEWER capture in the store, so the retry offers a higher boundary.
  give_leader_a_snapshot(
    &mut ep,
    &mut stable,
    Index::new(removal.get() + 5),
    encode_count_snapshot(9),
  );
  let due = ep
    .courtesy_owed
    .get(&3u64)
    .and_then(|dbt| dbt.next_at)
    .expect("the offer started a cooldown");
  let mut at = due + Duration::from_millis(1);
  ep.handle_timeout(at, &mut log, &mut stable);
  let s2 = courtesy_offers_to_node3(&mut ep);
  assert_eq!(s2.len(), 1, "the retained debt re-offers");
  let Message::InstallSnapshot(is2) = &s2[0] else {
    unreachable!("filtered above")
  };
  assert!(
    is2.snapshot().last_index() > b1,
    "and S2 carries the newer capture"
  );
  assert_eq!(
    ep.courtesy_owed.get(&3u64).map(|dbt| dbt.offered_index),
    Some(Some(b1)),
    "yet the discharge floor is still the EARLIEST boundary offered this generation"
  );
  drop(s2); // S2 never arrives

  // The delayed S1 ack lands: below S2's boundary, at S1's — and it is proof of a cure.
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    3u64,
    Message::SnapshotResponse(SnapshotResponse::new(ep.term(), 3u64, false, b1)),
  );
  assert!(
    !ep.courtesy_owed.contains_key(&3u64),
    "an ack for ANY offer of this generation is evidence the peer installed a cure"
  );

  // And the cure is never offered again, contact or tick.
  let window = COURTESY_COOLDOWN_TIMEOUTS * ep.config.election_timeout();
  for _ in 0..4 {
    at = at + window;
    ep.handle_message(at, &mut log, &mut stable, 3u64, removed_peer_contact());
    ep.handle_timeout(at, &mut log, &mut stable);
    assert!(
      courtesy_offers_to_node3(&mut ep).is_empty(),
      "a discharged debt is never re-offered"
    );
  }
}

/// RACE TWO — the ack arrives after a STEP-DOWN. The offer goes out as leader, CheckQuorum steps
/// this replica down at the SAME term, and node 3's ack lands on a follower. The universal mint
/// parks the debt on every replica, so the evidence must be honored on whatever role holds it;
/// discarding it because we are momentarily not the leader re-offers a whole blob to a peer that
/// already installed the cure, once per window, for as long as the debt survives.
///
/// MUTATION: leave the discharge behind the leader gate → the debt outlives the step-down and the
/// first tick after re-election offers again.
#[test]
fn an_ack_after_a_step_down_discharges_the_debt_on_the_follower() {
  use crate::{Role, SnapshotResponse};
  use core::time::Duration;

  let (mut ep, mut log, mut stable, d, removal) = leader_that_removed_node3();
  ep.pending_farewells_clear_for_test();
  give_leader_a_snapshot(&mut ep, &mut stable, removal, encode_count_snapshot(7));
  ep.handle_message(d, &mut log, &mut stable, 3u64, removed_peer_contact());
  let offers = courtesy_offers_to_node3(&mut ep);
  assert_eq!(offers.len(), 1, "the offer goes out as leader");
  let Message::InstallSnapshot(is) = &offers[0] else {
    unreachable!("filtered above")
  };
  let boundary = is.snapshot().last_index();
  let term = ep.term();

  // CheckQuorum's step-down: no term bump, so the peer's ack still matches this term.
  ep.step_down_to_follower(d.into());
  assert_eq!(ep.role(), Role::Follower, "stepped down");
  assert_eq!(ep.term(), term, "at the same term");

  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    3u64,
    Message::SnapshotResponse(SnapshotResponse::new(term, 3u64, false, boundary)),
  );
  assert!(
    !ep.courtesy_owed.contains_key(&3u64),
    "evidence is honored wherever the debt lives, not only at a leader"
  );

  // Re-elected, the first tick has nothing left to offer.
  let rd = re_elect_among_the_live_pair(&mut ep, &mut log, &mut stable);
  ep.handle_timeout(rd + Duration::from_millis(150), &mut log, &mut stable);
  assert!(
    courtesy_offers_to_node3(&mut ep).is_empty(),
    "a debt discharged on the follower is not resurrected by re-election"
  );
}

/// THE FULL TRUTHFUL CHAIN across both replicas. Node 3 is the sharp shape: it already holds the
/// removal entry DURABLY, but below its commit index, so the courtesy snapshot resolves REDUNDANT
/// at the receiver and no blob is installed. The ack that comes back is only evidence because the
/// redundancy arm advances commit and applies through the boundary first — the peer really does
/// process its own removal, and only then does the sender's debt discharge.
///
/// MUTATION: drop the receiver's commit advance → node 3 acks without applying, stays a voter, and
/// the leader discharges the debt on an ack that proves nothing: the cure is lost with both sides
/// believing it landed.
#[test]
fn a_redundant_install_still_cures_the_peer_and_discharges_the_debt() {
  use crate::{Entry, EntryKind, Event};

  let (mut ep, mut log, mut stable, d, removal) = leader_that_removed_node3();
  ep.pending_farewells_clear_for_test();
  give_leader_a_snapshot(&mut ep, &mut stable, removal, encode_count_snapshot(7));

  // Node 3 holds the removal entry durably at `removal`, with commit still at zero.
  let (mut n3, mut n3log, mut n3stable) = node3_with_log_async(
    std::vec![
      Entry::new(
        Term::new(1),
        Index::new(1),
        EntryKind::Empty,
        bytes::Bytes::new()
      ),
      Entry::new(
        Term::new(1),
        removal,
        EntryKind::ConfChange,
        remove3_payload()
      ),
    ],
    Index::ZERO,
  );
  assert!(
    n3.commit < removal,
    "the removal is durable but UNCOMMITTED"
  );
  assert!(n3.tracker.is_voter(&3u64), "so node 3 is still a voter");

  // The offer goes out and reaches it.
  ep.handle_message(d, &mut log, &mut stable, 3u64, removed_peer_contact());
  let offer = courtesy_offers_to_node3(&mut ep)
    .into_iter()
    .next()
    .expect("the contact offers the cure");
  let Message::InstallSnapshot(ref is) = offer else {
    unreachable!("filtered above")
  };
  let boundary = is.snapshot().last_index();
  n3.handle_message(Instant::ORIGIN, &mut n3log, &mut n3stable, 1u64, offer);
  for _ in 0..4 {
    n3.handle_storage(Instant::ORIGIN, &mut n3log, &mut n3stable);
  }

  // Redundant — no blob installed — yet the peer is genuinely cured, and the evidence behind the
  // cure is DURABLE: the raised commit reached stable storage, which is what the ack waits on.
  assert!(
    n3stable.durable_snapshot().is_none(),
    "the receiver short-circuits: it already held the boundary"
  );
  assert_eq!(n3.commit, boundary, "commit advanced to the boundary");
  assert!(
    n3stable.hard_state().commit() >= boundary,
    "and the raise is DURABLE — the crash-surviving half of the shortcut's evidence"
  );
  assert!(
    !n3.tracker.is_voter(&3u64),
    "and the removal applied — the peer is out"
  );
  assert!(
    core::iter::from_fn(|| n3.poll_event())
      .any(|e| matches!(e, Event::ConfChanged(cc) if !cc.conf().voters().contains(&3u64))),
    "the removal surfaced (what RemovedSelf is derived from)"
  );

  // The ack it produces is therefore truthful — and it discharges the debt.
  let ack = core::iter::from_fn(|| n3.poll_message())
    .map(|o| o.into_parts().1)
    .find(|m| matches!(m, Message::SnapshotResponse(r) if !r.reject()))
    .expect("a completed install ack goes back");
  ep.handle_message(d, &mut log, &mut stable, 3u64, ack);
  assert!(
    !ep.courtesy_owed.contains_key(&3u64),
    "and the sender discharges a debt whose cure really landed"
  );
}

/// RACE — ACK, THEN CRASH. The shortcut installs no blob, so the only thing standing between the
/// discharged debt and a revived voter is the DURABLE commit. Node 3 acks a boundary-above-commit
/// cure, the leader discharges, and node 3 then crashes and reboots from its durable state alone: it
/// must come back with commit derived at the boundary, re-apply the removal from the durable log, and
/// never be a voter again. The debt stays rightly discharged because the evidence really did survive.
///
/// The restart derivation is the load-bearing line: `commit = min(hs.commit(), log.last_index())
/// .max(applied)`. Persisting the raise is what makes that resolve to the boundary.
///
/// MUTATION: drop the forced hard-state write → the ack still goes out (the batched choke point has
/// not run), the reboot derives a stale commit, node 3 returns a VOTER with the removal unapplied,
/// and nobody owes it a cure.
#[test]
fn an_acked_shortcut_cure_survives_the_peers_crash() {
  use crate::{Config, Entry, EntryKind, StableStore as _};
  use core::time::Duration;

  let (mut ep, mut log, mut stable, d, removal) = leader_that_removed_node3();
  ep.pending_farewells_clear_for_test();
  give_leader_a_snapshot(&mut ep, &mut stable, removal, encode_count_snapshot(7));
  let (mut n3, mut n3log, mut n3stable) = node3_with_log_async(
    std::vec![
      Entry::new(
        Term::new(1),
        Index::new(1),
        EntryKind::Empty,
        bytes::Bytes::new()
      ),
      Entry::new(
        Term::new(1),
        removal,
        EntryKind::ConfChange,
        remove3_payload()
      ),
    ],
    Index::ZERO,
  );

  ep.handle_message(d, &mut log, &mut stable, 3u64, removed_peer_contact());
  let offer = courtesy_offers_to_node3(&mut ep)
    .into_iter()
    .next()
    .expect("the contact offers the cure");
  n3.handle_message(Instant::ORIGIN, &mut n3log, &mut n3stable, 1u64, offer);
  // Drive only as far as the ack, then CRASH THERE. Driving past it would persist the commit for
  // unrelated reasons and hide the property under test: what matters is that the ack itself never
  // appears before the evidence behind it is durable.
  let mut ack = None;
  for _ in 0..4 {
    while let Some(out) = n3.poll_message() {
      if matches!(out.message(), Message::SnapshotResponse(r) if !r.reject()) {
        ack = Some(out.into_parts().1);
      }
    }
    if ack.is_some() {
      break;
    }
    n3.handle_storage(Instant::ORIGIN, &mut n3log, &mut n3stable);
  }
  while let Some(out) = n3.poll_message() {
    if matches!(out.message(), Message::SnapshotResponse(r) if !r.reject()) {
      ack = Some(out.into_parts().1);
    }
  }
  let ack = ack.expect("the gated ack is released once the raise is durable");
  ep.handle_message(d, &mut log, &mut stable, 3u64, ack);
  assert!(
    !ep.courtesy_owed.contains_key(&3u64),
    "the leader discharges on that ack"
  );

  // CRASH: everything not yet fsynced is lost. The reboot has only the durable log + HardState.
  n3stable.discard_inflight();
  drop(n3);
  let cfg3 = Config::try_new(
    3u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let rebooted = Endpoint::<u64, CountSm>::restart(
    cfg3,
    Instant::ORIGIN,
    9,
    CountSm::default(),
    1,
    &mut n3log,
    &mut n3stable,
  );
  assert!(
    n3stable.durable_snapshot().is_none(),
    "no blob was ever installed — the durable log plus the persisted commit is the whole evidence"
  );
  assert!(
    rebooted.commit >= removal,
    "the reboot derives commit at the boundary from durable HardState"
  );
  assert!(
    !rebooted.tracker.is_voter(&3u64),
    "so the removal re-applies and the peer never revives as a voter"
  );
  assert!(
    !ep.courtesy_owed.contains_key(&3u64),
    "and the discharge was right all along"
  );
}

/// The other half: CRASH BEFORE THE PERSIST. With the commit write held in the fsync window the ack
/// is never emitted, so the leader's debt is never discharged and the cure is simply retried. Losing
/// the window costs a round, never the cure — which is what makes gating the ack (rather than
/// emitting it optimistically) the cheap side of the trade.
///
/// MUTATION: emit the ack without waiting for the raise to land → the debt discharges on evidence
/// that a crash erases, and the peer revives owed nothing.
#[test]
fn a_crash_before_the_persist_emits_no_ack_and_keeps_the_debt() {
  use crate::{Entry, EntryKind};

  let (mut ep, mut log, mut stable, d, removal) = leader_that_removed_node3();
  ep.pending_farewells_clear_for_test();
  give_leader_a_snapshot(&mut ep, &mut stable, removal, encode_count_snapshot(7));
  let (mut n3, mut n3log, mut n3stable) = node3_with_log_async(
    std::vec![
      Entry::new(
        Term::new(1),
        Index::new(1),
        EntryKind::Empty,
        bytes::Bytes::new()
      ),
      Entry::new(
        Term::new(1),
        removal,
        EntryKind::ConfChange,
        remove3_payload()
      ),
    ],
    Index::ZERO,
  );
  // Every subsequent HardState write stays in the fsync window.
  n3stable.hold_writes(true);

  ep.handle_message(d, &mut log, &mut stable, 3u64, removed_peer_contact());
  let offer = courtesy_offers_to_node3(&mut ep)
    .into_iter()
    .next()
    .expect("the contact offers the cure");
  n3.handle_message(Instant::ORIGIN, &mut n3log, &mut n3stable, 1u64, offer);
  for _ in 0..4 {
    n3.handle_storage(Instant::ORIGIN, &mut n3log, &mut n3stable);
  }
  assert!(
    !core::iter::from_fn(|| n3.poll_message())
      .any(|o| matches!(o.message(), Message::SnapshotResponse(_))),
    "no ack while the raise is un-fsynced — the evidence does not exist yet"
  );
  assert!(
    ep.courtesy_owed.contains_key(&3u64),
    "so the leader's debt stands, exactly as a lost offer leaves it"
  );

  // The write lands after all; the ordinary loop releases the ack and the cure completes.
  n3stable.flush_held_writes();
  let mut ack = None;
  for _ in 0..4 {
    n3.handle_storage(Instant::ORIGIN, &mut n3log, &mut n3stable);
    while let Some(out) = n3.poll_message() {
      if matches!(out.message(), Message::SnapshotResponse(r) if !r.reject()) {
        ack = Some(out.into_parts().1);
      }
    }
  }
  let ack = ack.expect("the landed write releases the gated ack");
  ep.handle_message(d, &mut log, &mut stable, 3u64, ack);
  assert!(
    !ep.courtesy_owed.contains_key(&3u64),
    "and only THEN does the debt discharge"
  );
}

/// RACE — DUPLICATE, THEN CRASH AT THE FIRST OBSERVABLE ACK. The sender retries a courtesy offer on
/// its cooldown, so a duplicate landing inside the gate window is the ordinary case, not an exotic
/// one. The duplicate classifies at-or-below against the raised, still-volatile commit; the gate
/// holds it anyway; and whichever ack finally emerges is backed by durable state — so crashing the
/// instant it appears still leaves the peer cured and the discharge right.
///
/// MUTATION: restore the immediate at-or-below ack → the duplicate answers inside the window, the
/// leader discharges, the crash erases the evidence, and node 3 reboots a voter nobody owes.
#[test]
fn a_duplicate_offer_then_a_crash_at_the_first_ack_still_leaves_the_peer_cured() {
  use crate::{Config, Entry, EntryKind, StableStore as _};
  use core::time::Duration;

  let (mut ep, mut log, mut stable, d, removal) = leader_that_removed_node3();
  ep.pending_farewells_clear_for_test();
  give_leader_a_snapshot(&mut ep, &mut stable, removal, encode_count_snapshot(7));
  let (mut n3, mut n3log, mut n3stable) = node3_with_log_async(
    std::vec![
      Entry::new(
        Term::new(1),
        Index::new(1),
        EntryKind::Empty,
        bytes::Bytes::new()
      ),
      Entry::new(
        Term::new(1),
        removal,
        EntryKind::ConfChange,
        remove3_payload()
      ),
    ],
    Index::ZERO,
  );

  ep.handle_message(d, &mut log, &mut stable, 3u64, removed_peer_contact());
  let offer = courtesy_offers_to_node3(&mut ep)
    .into_iter()
    .next()
    .expect("the contact offers the cure");
  let duplicate = offer.clone();

  // First delivery opens the window; the DUPLICATE lands inside it, at-or-below the raised commit.
  n3.handle_message(Instant::ORIGIN, &mut n3log, &mut n3stable, 1u64, offer);
  assert_eq!(n3.commit, removal, "the first delivery raised commit");
  n3.handle_message(Instant::ORIGIN, &mut n3log, &mut n3stable, 1u64, duplicate);
  assert!(
    !core::iter::from_fn(|| n3.poll_message())
      .any(|o| matches!(o.message(), Message::SnapshotResponse(_))),
    "neither delivery acks while the raise is un-fsynced"
  );

  // Drive only as far as the first observable ack, and crash exactly there.
  let mut ack = None;
  for _ in 0..4 {
    n3.handle_storage(Instant::ORIGIN, &mut n3log, &mut n3stable);
    while let Some(out) = n3.poll_message() {
      if matches!(out.message(), Message::SnapshotResponse(r) if !r.reject()) {
        ack = Some(out.into_parts().1);
      }
    }
    if ack.is_some() {
      break;
    }
  }
  let ack = ack.expect("one ack emerges once the evidence is durable");
  ep.handle_message(d, &mut log, &mut stable, 3u64, ack);
  assert!(
    !ep.courtesy_owed.contains_key(&3u64),
    "the leader discharges on it"
  );

  n3stable.discard_inflight();
  drop(n3);
  let cfg3 = Config::try_new(
    3u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let rebooted = Endpoint::<u64, CountSm>::restart(
    cfg3,
    Instant::ORIGIN,
    9,
    CountSm::default(),
    1,
    &mut n3log,
    &mut n3stable,
  );
  assert!(
    n3stable.durable_snapshot().is_none(),
    "still no blob — the durable log plus the persisted commit carried it"
  );
  assert!(
    rebooted.commit >= removal,
    "the reboot derives commit at the boundary"
  );
  assert!(
    !rebooted.tracker.is_voter(&3u64),
    "and the duplicate changed nothing: the peer stays removed"
  );
  assert!(
    !ep.courtesy_owed.contains_key(&3u64),
    "so the discharge was right"
  );
}

/// F2 — the offer must CARRY the removal. A durable snapshot captured BEFORE the removal fails
/// both eligibility legs (its boundary predates the removal index and its ConfState still names
/// the peer), so shipping it would re-baseline the peer onto a configuration that still includes
/// it: ignorant, still armed, and now at the cost of a whole blob. It DEFERS instead — no send,
/// and crucially no budget burned — and a later post-removal capture enables the offer.
///
/// MUTATION: drop the eligibility gate → the pre-removal blob is shipped, and the "no offer yet"
/// assertion fails.
#[test]
fn a_pre_removal_snapshot_defers_without_spending_the_debt() {
  let (mut ep, mut log, mut stable, d, removal) = leader_that_removed_node3();
  ep.pending_farewells_clear_for_test();
  // A capture from before the removal: index below it, and its ConfState still names node 3.
  let stale = give_leader_a_pre_removal_snapshot(&mut ep, &mut stable, Index::new(1));
  assert!(stale.last_index() < removal && stale.conf().voters().contains(&3u64));

  ep.handle_message(d, &mut log, &mut stable, 3u64, removed_peer_contact());
  assert!(
    courtesy_offers_to_node3(&mut ep).is_empty(),
    "a snapshot that predates the removal is not an offer"
  );
  assert_eq!(
    ep.courtesy_owed.get(&3u64).map(|dbt| dbt.offered_index),
    Some(None),
    "nothing was offered, so nothing is outstanding"
  );
  assert_eq!(
    ep.courtesy_owed.get(&3u64).and_then(|dbt| dbt.next_at),
    None,
    "and started no cooldown — the next contact retries at once"
  );

  // A post-removal capture exists: the SAME contact now cures.
  give_leader_a_snapshot(&mut ep, &mut stable, removal, encode_count_snapshot(7));
  ep.handle_message(d, &mut log, &mut stable, 3u64, removed_peer_contact());
  let offers = courtesy_offers_to_node3(&mut ep);
  assert_eq!(offers.len(), 1, "the eligible capture is offered at once");
  let Message::InstallSnapshot(is) = &offers[0] else {
    unreachable!("filtered above")
  };
  assert!(
    is.snapshot().last_index() >= removal && !is.snapshot().conf().voters().contains(&3u64),
    "and what it carries is the removal itself"
  );
}

/// F2's second leg alone: a capture at or past the removal index whose ConfState STILL names the
/// peer (a boundary that happens to sit past the index without covering the change) is equally
/// ineligible. Index alone is not evidence — the installed configuration is.
#[test]
fn a_snapshot_still_naming_the_peer_defers() {
  let (mut ep, mut log, mut stable, d, removal) = leader_that_removed_node3();
  ep.pending_farewells_clear_for_test();
  let meta = give_leader_a_pre_removal_snapshot(&mut ep, &mut stable, removal.next());
  assert!(meta.last_index() > removal, "the index leg passes");

  ep.handle_message(d, &mut log, &mut stable, 3u64, removed_peer_contact());
  assert!(
    courtesy_offers_to_node3(&mut ep).is_empty(),
    "a ConfState that still names the peer cannot carry its removal"
  );
  assert_eq!(
    ep.courtesy_owed.get(&3u64).map(|dbt| dbt.offered_index),
    Some(None),
    "and the debt is untouched — nothing was offered"
  );
}

/// A follower that applies a removal of ANOTHER peer mints the debt too — the mint is universal,
/// so whichever member leads next already owes the departed peer its cure. Without this a leader
/// elected AFTER the removal would owe nothing, would adopt the peer's next campaign in the term
/// pre-pass, and the whole disruption cycle would recur at it.
#[test]
fn a_follower_mints_the_courtesy_debt_when_it_applies_a_removal() {
  use crate::Role;
  let (n2, _log2, _stable2) = node2_that_applied_the_removal();
  assert_eq!(n2.role(), Role::Follower, "node 2 never led");
  assert!(
    n2.courtesy_owed.contains_key(&3u64),
    "a follower's apply-time knowledge is enough to owe the debt"
  );
  assert!(
    !n2.has_courtesy_debts(),
    "but consulting it stays leader-gated — inert on a follower"
  );
  assert!(
    n2.pending_farewells.is_empty(),
    "the FAREWELL stays leader-only: it needs the tracker's proven match, which a follower lacks"
  );
}

/// THE SELF-REMOVAL CLASS-CHECK. A replica applying the removal of ITSELF must take the
/// self-removal path — step down, disarm, surface the change — and must never mint a debt naming
/// itself: nothing could ever spend it, and a later re-add would leave a debt against a current
/// member. The mint's `*peer == me` skip is what makes it impossible, and the self-removal
/// step-down clears the whole map as a belt.
#[test]
fn a_self_removal_never_mints_a_debt_for_self() {
  use crate::{Event, Role};
  let (mut n3, mut log3, mut stable3) = node3_that_applied_its_own_removal();
  assert!(
    core::iter::from_fn(|| n3.poll_event())
      .any(|e| matches!(e, Event::ConfChanged(cc) if !cc.conf().voters().contains(&3u64))),
    "the self-removal surfaces through the ordinary ConfChanged path"
  );
  assert!(
    n3.courtesy_owed.is_empty(),
    "a replica never owes itself a courtesy debt"
  );
  assert_eq!(n3.role(), Role::Follower, "and it is disarmed");
  let _ = (&mut log3, &mut stable3);
}

/// THE DIFFERENT-LEADER HALF of the cure. The removal-era leader is out of the picture entirely:
/// node 2 applied the removal as a FOLLOWER, then won leadership. Because the mint is universal it
/// already owes node 3 the debt, so node 3's contact is answered with the cure from node 2's OWN
/// apply-time evidence — no farewell budget, no help from the leader that did the removing.
///
/// MUTATION: put the mint back inside the leader gate → node 2 owes nothing and offers nothing.
#[test]
fn a_different_leader_cures_from_its_own_apply_time_debt() {
  use crate::{Index, Message, Role, Term, VoteResponse};

  // Node 2: applied the removal as a follower, then elected (its own vote plus node 1's — the
  // post-removal configuration is {1, 2}).
  let (mut n2, mut log2, mut stable2) = node2_that_applied_the_removal();
  let removal = n2
    .courtesy_owed
    .get(&3u64)
    .expect("the follower minted the debt")
    .removal_index;
  let d = n2.poll_timeout().expect("a voter arms its election timer");
  n2.handle_timeout(d, &mut log2, &mut stable2);
  n2.handle_storage(d, &mut log2, &mut stable2);
  let ct = n2.term();
  n2.handle_message(
    d,
    &mut log2,
    &mut stable2,
    1u64,
    Message::VoteResponse(VoteResponse::new(ct, 1u64, false, false)),
  );
  n2.handle_storage(d, &mut log2, &mut stable2);
  assert_eq!(
    n2.role(),
    Role::Leader,
    "node 2 leads without node 1's help"
  );
  // Its own no-op must commit and apply before any cure may act on its applied configuration.
  apply_the_term_start_noop(&mut n2, &mut log2, &mut stable2, d, &[1u64]);
  assert!(
    n2.has_courtesy_debts(),
    "and carries the debt it minted as a follower into leadership"
  );
  while n2.poll_message().is_some() {}
  while n2.poll_event().is_some() {}

  // Node 2 has never sent node 3 a farewell — it was not the removal-era leader — so the courtesy
  // path owns the peer from the first contact.
  assert!(
    n2.pending_farewells.is_empty(),
    "no farewell budget: this leader never pruned node 3 from its own tracker as leader"
  );
  n2.pending_farewells_clear_for_test();
  give_leader_a_snapshot(&mut n2, &mut stable2, removal, encode_count_snapshot(7));

  // Node 3's REAL campaign (default flags: no pre-vote, so a genuine higher term).
  let term_before = n2.term();
  let campaign = Message::RequestVote(crate::RequestVote::new(
    term_before,
    3u64,
    Index::new(2),
    Term::new(1),
    false,
    false,
  ));
  n2.handle_message(d, &mut log2, &mut stable2, 3u64, campaign.clone());

  assert_eq!(
    n2.role(),
    Role::Leader,
    "a same-term campaign does not depose"
  );
  assert_eq!(
    n2.term(),
    term_before,
    "and the same-term campaign moves nothing"
  );
  let mut offers: Vec<Message<u64>> = core::iter::from_fn(|| n2.poll_message())
    .filter(|o| o.to() == 3u64)
    .map(|o| o.into_parts().1)
    .filter(|m| matches!(m, Message::InstallSnapshot(_)))
    .collect();
  assert_eq!(offers.len(), 1, "and the cure is offered from its own debt");
  let offer = offers.pop().expect("one offer");
  {
    let Message::InstallSnapshot(is) = &offer else {
      unreachable!("filtered above")
    };
    assert!(
      !is.snapshot().conf().voters().contains(&3u64) && is.snapshot().last_index() >= removal,
      "carrying the removal node 2 applied as a follower"
    );
  }

  // Node 3 applies it and self-removes — cured with no participation from the removal-era leader.
  // Delivered VERBATIM, exactly as node 2 emitted it.
  let (mut n3, mut log3, mut stable3) = node3_async();
  n3.handle_message(d, &mut log3, &mut stable3, 2u64, offer);
  n3.handle_storage(d, &mut log3, &mut stable3);
  assert!(
    core::iter::from_fn(|| n3.poll_event())
      .any(|e| matches!(e, crate::Event::ConfChanged(cc) if !cc.conf().voters().contains(&3u64))),
    "the removed peer applies the excluding ConfState and surfaces its removal"
  );
  assert!(!n3.tracker.is_voter(&3u64), "and is disarmed for good");
}

/// F2 END TO END, at the config the cure protects (DEFAULT flags). Node 2 is offline across the
/// whole `RemoveNode(3)` conf change and never replays it — it catches up WHOLESALE by snapshot.
/// The install's membership transition is the only evidence of the removal it will ever see, and
/// that is enough: the debt is minted at the boundary, so when node 2 later wins leadership it
/// already owes node 3 the cure, drops node 3's real campaign without stepping down, and pays the
/// debt from the very snapshot that taught it.
///
/// MUTATION: drop the install-edge mint → node 2 owes nothing, adopts node 3's term, and the
/// no-step-down assertions fail.
#[test]
fn a_snapshot_only_removal_cures_the_peer_from_a_later_leader() {
  use crate::{
    Index, InstallSnapshot, Message, Role, SnapshotMeta, Term, VoteResponse, conf::ConfState,
  };
  use core::time::Duration;

  // Node 2, a member of {1, 2, 3}, was offline across the removal.
  let cfg = Config::try_new(
    2u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut n2 = Endpoint::new(cfg, Instant::ORIGIN, 5, CountSm::default());
  let mut log2 = VecLog::default();
  let mut stable2 = AsyncStable::default();
  let d = Instant::ORIGIN;

  // Catch-up by snapshot ALONE: the boundary's ConfState is the post-removal {1, 2}.
  let boundary = Index::new(20);
  let meta = SnapshotMeta::new(
    boundary,
    Term::new(1),
    ConfState::from_voters(std::vec![1u64, 2]),
  );
  n2.handle_message(
    d,
    &mut log2,
    &mut stable2,
    1u64,
    Message::InstallSnapshot(InstallSnapshot::new(
      Term::new(1),
      1u64,
      meta,
      encode_count_snapshot(7),
    )),
  );
  n2.handle_storage(d, &mut log2, &mut stable2);
  assert_eq!(n2.commit, boundary, "node 2 caught up wholesale");
  let debt = n2
    .courtesy_owed
    .get(&3u64)
    .expect("the snapshot-only removal minted the debt");
  assert_eq!(
    debt.removal_index, boundary,
    "at the snapshot boundary, the index this very snapshot pays"
  );
  assert!(
    n2.pending_farewells.is_empty(),
    "and no farewell budget — node 2 never led the removal"
  );
  while n2.poll_message().is_some() {}
  while n2.poll_event().is_some() {}

  // Node 2 wins leadership among the surviving {1, 2}.
  let ed = n2.poll_timeout().expect("a voter arms its election timer");
  n2.handle_timeout(ed, &mut log2, &mut stable2);
  n2.handle_storage(ed, &mut log2, &mut stable2);
  let ct = n2.term();
  n2.handle_message(
    ed,
    &mut log2,
    &mut stable2,
    1u64,
    Message::VoteResponse(VoteResponse::new(ct, 1u64, false, false)),
  );
  n2.handle_storage(ed, &mut log2, &mut stable2);
  assert_eq!(n2.role(), Role::Leader, "node 2 leads");
  apply_the_term_start_noop(&mut n2, &mut log2, &mut stable2, ed, &[1u64]);
  while n2.poll_message().is_some() {}

  // Node 3's REAL higher-term campaign: dropped, counted, and answered with the cure.
  let term_before = n2.term();
  n2.handle_message(
    ed,
    &mut log2,
    &mut stable2,
    3u64,
    Message::RequestVote(crate::RequestVote::new(
      term_before,
      3u64,
      Index::new(2),
      Term::new(1),
      false,
      false,
    )),
  );
  assert_eq!(
    n2.role(),
    Role::Leader,
    "a snapshot-taught leader refuses to be deposed too"
  );
  assert_eq!(
    n2.term(),
    term_before,
    "and the same-term campaign moves nothing"
  );
  let offers: Vec<_> = core::iter::from_fn(|| n2.poll_message())
    .filter_map(|o| match o.into_parts() {
      (3u64, Message::InstallSnapshot(is)) => Some(is),
      _ => None,
    })
    .collect();
  assert_eq!(
    offers.len(),
    1,
    "and the debt is paid from the installed snapshot"
  );
  assert_eq!(
    offers[0].snapshot().last_index(),
    boundary,
    "the offer IS the snapshot that taught node 2 the removal"
  );
  assert!(
    !offers[0].snapshot().conf().voters().contains(&3u64),
    "and it carries the configuration that excludes node 3"
  );
}

/// THE FULL LOOP, at the etcd-parity defaults (pre_vote and check_quorum both OFF) and with NO
/// message ever reconstructed: every message that crosses between the two endpoints here is the
/// exact `Outgoing` the sender emitted, delivered verbatim to the peer it was addressed to.
///
/// A removed peer that campaigns for REAL puts itself above the leader's term, and from that
/// moment every offer that leader could stamp is dead at the peer's own stale-term pre-pass. The
/// futility gate stops it making offers that would be discarded, and the debt survives the
/// deposition the campaign costs; the cure is then delivered by the re-elected leadership, whose
/// first tick offers proactively at a term the peer accepts.
///
/// MUTATION: remove the futility gate → every window's offer burns against a peer whose stale-term
/// pre-pass discards it, and the peer stays uncured for as long as that leader keeps the term.
#[test]
fn the_courtesy_cure_survives_a_real_campaign_and_lands_after_the_term_lift() {
  use crate::{Entry, EntryKind, Event, Index, Message, Role, Term, VoteResponse};
  use core::time::Duration;

  let (mut ep, mut log, mut stable, d, removal) = leader_that_removed_node3();
  ep.pending_farewells_clear_for_test();
  give_leader_a_snapshot(&mut ep, &mut stable, removal, encode_count_snapshot(7));
  while ep.poll_message().is_some() {}

  // Node 3 is a REAL endpoint. Its campaign is its own emitted message, not a hand-built one.
  let (mut n3, mut n3log, mut n3stable) = node3_with_log_async(
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
  let (to, campaign) = core::iter::from_fn(|| n3.poll_message())
    .find(|o| matches!(o.message(), Message::RequestVote(r) if !r.pre_vote()))
    .expect("node 3 campaigns for real (pre-vote off)")
    .into_parts();
  assert_eq!(to, 1u64, "addressed to the leader");
  assert_eq!(
    n3.term(),
    Term::new(2),
    "and it inflated its own term doing so"
  );

  // THE ONE DEPOSITION, and the futility gate doing its own job through it. The campaign is
  // handled normally — Raft's universal term mechanism, untouched — so the leader adopts and steps
  // down. What the cure guarantees is that this costs the BUDGET nothing: every offer this leader
  // could have stamped (term 1) sat below node 3's term 2 and would have died at node 3's own
  // pre-pass, so none was made and none was spent.
  ep.handle_message(d, &mut log, &mut stable, 3u64, campaign);
  assert_eq!(ep.role(), Role::Follower, "the campaign deposes, once");
  assert_eq!(ep.term(), Term::new(2), "adopting the higher term");
  assert!(
    courtesy_offers_to_node3(&mut ep).is_empty(),
    "a futile offer is not emitted"
  );
  assert_eq!(
    ep.courtesy_owed.get(&3u64).unwrap().offered_index,
    None,
    "and nothing was offered, so nothing is outstanding"
  );
  assert_eq!(
    ep.courtesy_owed.get(&3u64).unwrap().next_at,
    None,
    "nor starts a cooldown"
  );

  // THE TERM LIFT. A LIVE member campaigns and node 1 re-wins above node 3's term — node 3 cannot
  // win itself, having no quorum in the configuration that removed it.
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::RequestVote(crate::RequestVote::new(
      Term::new(3),
      2u64,
      Index::new(3),
      Term::new(1),
      false,
      false,
    )),
  );
  let rd = ep.poll_timeout().expect("the deposed leader re-arms");
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
  assert!(ep.role().is_leader(), "node 1 re-wins");
  assert!(ep.term() > n3.term(), "at a term above the removed peer's");
  // The inherited-tail gate: the cure waits for this leader's OWN no-op to apply. Nothing about
  // the assertions below changes — only that the first tick that may offer is the first tick after
  // the leader's own first entry is applied truth.
  apply_the_term_start_noop(&mut ep, &mut log, &mut stable, rd, &[2u64]);
  assert_eq!(
    ep.courtesy_owed.get(&3u64).unwrap().next_at,
    None,
    "become_leader re-armed the surviving debt"
  );
  while ep.poll_message().is_some() {}

  // THE PROACTIVE OFFER: the first leader tick makes it, with no contact from node 3 at all.
  ep.handle_timeout(rd + Duration::from_millis(150), &mut log, &mut stable);
  let (to, offer) = core::iter::from_fn(|| ep.poll_message())
    .find(|o| matches!(o.message(), Message::InstallSnapshot(_)))
    .expect("the first tick after re-election offers the cure proactively")
    .into_parts();
  assert_eq!(to, 3u64, "addressed to the removed peer");

  // Delivered VERBATIM — the very Outgoing the leader emitted, to the endpoint it named.
  n3.handle_message(n3d, &mut n3log, &mut n3stable, 1u64, offer);
  n3.handle_storage(n3d, &mut n3log, &mut n3stable);
  assert!(
    core::iter::from_fn(|| n3.poll_event())
      .any(|e| matches!(e, Event::ConfChanged(cc) if !cc.conf().voters().contains(&3u64))),
    "node 3 installs the offer and surfaces its own removal"
  );
  assert!(!n3.tracker.is_voter(&3u64), "and is disarmed");

  // No further campaigns, ever: a non-voter's timer fires and produces nothing.
  let before = n3.term();
  for _ in 0..4 {
    let Some(t) = n3.poll_timeout() else { break };
    n3.handle_timeout(t, &mut n3log, &mut n3stable);
    n3.handle_storage(t, &mut n3log, &mut n3stable);
  }
  assert_eq!(
    n3.role(),
    Role::Follower,
    "the cured peer never campaigns again"
  );
  assert_eq!(n3.term(), before, "and never inflates the term again");
}

/// Drive a freshly-elected leader's own term-start no-op to COMMIT and APPLY. Every removal cure
/// waits for this (the inherited-tail gate), and it is what a real leader does before it serves
/// anything: until its own first entry applies, its applied configuration may be stale by the tail
/// it inherited. `acks` are the peers whose `AppendResponse` completes the quorum.
fn apply_the_term_start_noop<S: crate::StableStore<NodeId = u64>>(
  ep: &mut Endpoint<u64, CountSm>,
  log: &mut VecLog,
  stable: &mut S,
  now: Instant,
  acks: &[u64],
) {
  use crate::{AppendResponse, LogStore as _, Message, Term};
  ep.handle_storage(now, log, stable);
  let at = log.last_index();
  let term = ep.term();
  for &peer in acks {
    ep.handle_message(
      now,
      log,
      stable,
      peer,
      Message::AppendResponse(AppendResponse::new(
        term,
        peer,
        false,
        Index::ZERO,
        Term::ZERO,
        at,
      )),
    );
  }
  ep.handle_storage(now, log, stable);
}

/// THE FAREWELL-ARM VARIANT of the same race — the half that #112 shipped. Node 1 removed node 3
/// as leader (so it holds a farewell budget AND a debt), was deposed (parking both), then took a
/// committed-but-unknown-committed `AddNode(3)` into its tail from the new leader before re-winning.
/// Its applied configuration still excludes node 3, so `become_leader`'s reconciling retains keep
/// both records — and the front-loaded farewell would deliver a suffix ending at the removal index
/// to a peer the committed configuration has re-admitted.
///
/// MUTATION: remove the gate from `drive_pending_farewells` → the first post-re-election tick
/// front-loads a stale removal to a current member.
#[test]
fn no_farewell_is_front_loaded_while_an_inherited_readd_is_unapplied() {
  use crate::{
    AppendEntries, ConfChange, ConfChangeType, Entry, EntryKind, Message, Role, Term, VoteResponse,
  };
  use core::time::Duration;

  // Node 1 removed node 3 as leader: BOTH cure records exist.
  let (mut ep, mut log, mut stable, d, removal) = leader_after_removing_node3();
  assert!(
    ep.pending_farewells.contains_key(&3u64),
    "the removal armed the budget"
  );
  while ep.poll_message().is_some() {}

  // A live member deposes it; both records PARK.
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::RequestVote(crate::RequestVote::new(
      Term::new(2),
      2u64,
      removal,
      Term::new(1),
      false,
      false,
    )),
  );
  assert!(!ep.role().is_leader(), "deposed by a live member");

  // The new leader replicates its no-op AND a re-add of node 3 — committed globally, but the
  // commit index it carries here still sits at the removal, so neither has applied.
  let readd = ConfChange::new(ConfChangeType::AddNode, 3u64, Bytes::new()).into_v2();
  let mut payload = Vec::new();
  crate::wire::encode_conf_change_v2(&readd, &mut payload);
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(2),
      2u64,
      removal,
      Term::new(1),
      std::vec![
        Entry::new(Term::new(2), removal.next(), EntryKind::Empty, Bytes::new()),
        Entry::new(
          Term::new(2),
          removal.next().next(),
          EntryKind::ConfChange,
          bytes::Bytes::from(payload)
        ),
      ],
      removal,
    )),
  );
  ep.handle_storage(d, &mut log, &mut stable);
  assert_eq!(ep.applied, removal, "the re-add is in the tail, unapplied");
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  // Node 1 re-wins. Both records survive the retains, which read the stale applied view.
  let rd = ep.poll_timeout().expect("the deposed leader re-arms");
  ep.handle_timeout(rd, &mut log, &mut stable);
  ep.handle_storage(rd, &mut log, &mut stable);
  let ct = ep.term();
  ep.handle_message(
    rd,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResponse(VoteResponse::new(ct, 2u64, false, false)),
  );
  ep.handle_storage(rd, &mut log, &mut stable);
  assert_eq!(ep.role(), Role::Leader, "node 1 re-wins");
  assert!(ep.applied < ep.term_start_index, "with an unapplied tail");
  let shots_before = ep
    .pending_farewells
    .get(&3u64)
    .expect("the parked farewell survived the retain")
    .shots_left;
  while ep.poll_message().is_some() {}

  // THE WINDOW: the first ticks front-load NOTHING.
  for i in 0..4 {
    ep.handle_timeout(
      rd + Duration::from_millis(150 * (i + 1)),
      &mut log,
      &mut stable,
    );
    assert!(
      drain_to(&mut ep, 3u64).is_empty(),
      "no stale farewell may reach a re-admitted member"
    );
  }
  assert_eq!(
    ep.pending_farewells.get(&3u64).map(|f| f.shots_left),
    Some(shots_before),
    "the farewell budget is untouched — no shot spent while suppressed"
  );

  // The tail applies; the fold's re-add prune clears both.
  apply_the_term_start_noop(&mut ep, &mut log, &mut stable, rd, &[2u64]);
  assert!(
    ep.tracker.progress(&3u64).is_some(),
    "node 3 is a member again"
  );
  assert!(
    !ep.pending_farewells.contains_key(&3u64),
    "and the apply fold pruned the cure record"
  );
  while ep.poll_message().is_some() {}
  for i in 0..4 {
    ep.handle_timeout(
      rd + Duration::from_millis(900 + 150 * i),
      &mut log,
      &mut stable,
    );
  }
  assert!(
    drain_to(&mut ep, 3u64)
      .into_iter()
      .all(|m| !matches!(m, Message::InstallSnapshot(_))),
    "a re-admitted member is never cured of a removal it no longer has"
  );
}

/// A follower carrying a globally-COMMITTED but locally-unknown-committed `AddNode(3)` in its
/// election-inherited tail, holding BOTH cure records for node 3. This is the race's setup: the
/// old leader committed the re-add on a quorum that did not include this replica's knowledge, so
/// its APPLIED configuration still excludes node 3 while its LOG already re-admits it.
fn follower_with_an_inherited_readd() -> (Endpoint<u64, CountSm>, VecLog, AsyncStable) {
  use crate::{AppendEntries, ConfChange, ConfChangeType, Entry, EntryKind, Message, Term};
  use core::time::Duration;
  let cfg = Config::try_new(
    2u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 5, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();

  // 1..=2 arrive COMMITTED: the no-op and RemoveNode(3). Applying the removal mints both cures.
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
      std::vec![
        Entry::new(Term::new(1), Index::new(1), EntryKind::Empty, Bytes::new()),
        Entry::new(
          Term::new(1),
          Index::new(2),
          EntryKind::ConfChange,
          remove3_payload()
        ),
      ],
      Index::new(2),
    )),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  assert!(
    ep.courtesy_owed.contains_key(&3u64),
    "the removal minted the debt"
  );

  // 3 arrives with commit STILL AT 2 — the AddNode(3) is in the tail, committed globally (the old
  // leader had a quorum for it) but not known-committed here.
  let readd = ConfChange::new(ConfChangeType::AddNode, 3u64, Bytes::new()).into_v2();
  let mut payload = Vec::new();
  crate::wire::encode_conf_change_v2(&readd, &mut payload);
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
      std::vec![Entry::new(
        Term::new(1),
        Index::new(3),
        EntryKind::ConfChange,
        bytes::Bytes::from(payload)
      )],
      Index::new(2),
    )),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  assert_eq!(ep.applied, Index::new(2), "the re-add has NOT applied");
  assert!(
    ep.tracker.progress(&3u64).is_none(),
    "so the applied configuration still excludes node 3 — the stale view"
  );
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}
  (ep, log, stable)
}

/// THE RACE, cured. A fresh leader whose inherited tail holds a committed re-add must not act on
/// its stale applied configuration: both cure records for node 3 survive `become_leader`'s
/// reconciling retains (which read that stale view), and delivering either — the farewell's suffix
/// ending at the removal, or the courtesy's excluding snapshot — would tear down a peer the
/// committed configuration includes. Nothing is sent until the leader's own no-op applies, and by
/// then the tail's own apply fold has pruned both records.
///
/// MUTATION: remove the inherited-tail gate from either drive → a stale cure goes out during the
/// window and the "nothing sent" assertions fail.
#[test]
fn no_removal_cure_is_sent_while_an_inherited_readd_is_unapplied() {
  use crate::{Message, Role, VoteResponse};
  use core::time::Duration;
  let (mut ep, mut log, mut stable) = follower_with_an_inherited_readd();

  // It wins leadership. Its own no-op lands at 4; the inherited AddNode@3 is still unapplied.
  let d = ep.poll_timeout().expect("a voter arms its timer");
  ep.handle_timeout(d, &mut log, &mut stable);
  ep.handle_storage(d, &mut log, &mut stable);
  let ct = ep.term();
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    1u64,
    Message::VoteResponse(VoteResponse::new(ct, 1u64, false, false)),
  );
  ep.handle_storage(d, &mut log, &mut stable);
  assert_eq!(ep.role(), Role::Leader, "the follower won");
  assert!(
    ep.applied < ep.term_start_index,
    "and its inherited tail has not applied"
  );
  // The reconciling retains kept BOTH records — they read the stale applied view.
  assert!(
    ep.courtesy_owed.contains_key(&3u64),
    "the debt survived become_leader's retain"
  );
  // This fixture applied the removal as a FOLLOWER, so it carries the DEBT alone — the farewell is
  // leader-only. The farewell-arm variant below carries a parked budget beside it.
  assert!(
    ep.pending_farewells.is_empty(),
    "the courtesy-arm fixture: debt only"
  );
  give_leader_a_snapshot(
    &mut ep,
    &mut stable,
    Index::new(2),
    encode_count_snapshot(7),
  );
  while ep.poll_message().is_some() {}

  // THE WINDOW. Ticks fire, and node 3 even makes contact — nothing is CURED. Its message is
  // handled normally throughout; only our own sends are withheld.
  for i in 0..4 {
    ep.handle_timeout(
      d + Duration::from_millis(150 * (i + 1)),
      &mut log,
      &mut stable,
    );
    ep.handle_message(d, &mut log, &mut stable, 3u64, removed_peer_contact());
    assert!(
      drain_to(&mut ep, 3u64)
        .into_iter()
        .all(|m| !matches!(m, Message::InstallSnapshot(_))),
      "no stale cure may reach a peer the committed configuration re-admitted"
    );
  }
  assert_eq!(
    ep.courtesy_owed.get(&3u64).map(|dbt| dbt.offered_index),
    Some(None),
    "and nothing was ever offered"
  );
  assert_eq!(
    ep.courtesy_owed.get(&3u64).and_then(|dbt| dbt.next_at),
    None,
    "with no cooldown started"
  );
  assert!(
    ep.courtesy_owed.contains_key(&3u64),
    "the record is still present, waiting"
  );

  // THE TAIL APPLIES: the no-op commits, the inherited AddNode applies with it, and the fold's
  // own re-add prune reconciles both maps. No extra reconciliation code exists or is needed.
  apply_the_term_start_noop(&mut ep, &mut log, &mut stable, d, &[1u64]);
  assert!(
    ep.applied >= ep.term_start_index,
    "the inherited tail applied"
  );
  assert!(
    ep.tracker.progress(&3u64).is_some(),
    "node 3 is a member again per the committed configuration"
  );
  assert!(
    !ep.courtesy_owed.contains_key(&3u64),
    "the apply fold pruned the debt"
  );

  // And still nothing stale ever goes out: later ticks cure nobody.
  while ep.poll_message().is_some() {}
  for i in 0..4 {
    ep.handle_timeout(
      d + Duration::from_millis(900 + 150 * i),
      &mut log,
      &mut stable,
    );
  }
  assert!(
    drain_to(&mut ep, 3u64)
      .into_iter()
      .all(|m| !matches!(m, Message::InstallSnapshot(_))),
    "a re-admitted member is never offered a removal cure"
  );
}

/// A fresh leader whose inherited tail holds `RemoveNode(3)`, optionally `AddNode(3)`, and its own
/// no-op — all committed together and drained in ONE apply pass. This is the fire-site race: the
/// removal's apply is where the farewell's first shot is emitted, and a queued message cannot be
/// retracted by a prune that happens two entries later.
fn leader_draining_an_inherited_removal(
  readd: bool,
) -> (Endpoint<u64, CountSm>, VecLog, NoopStable, Index) {
  use crate::{
    AppendEntries, ConfChange, ConfChangeType, Entry, EntryKind, Message, Role, Term, VoteResponse,
  };
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
  let d = Instant::ORIGIN;

  // The inherited tail, appended while a follower and NOT yet committed: no-op@1, RemoveNode(3)@2,
  // and (optionally) AddNode(3)@3.
  let mut entries = std::vec![
    Entry::new(Term::new(1), Index::new(1), EntryKind::Empty, Bytes::new()),
    Entry::new(
      Term::new(1),
      Index::new(2),
      EntryKind::ConfChange,
      remove3_payload()
    ),
  ];
  if readd {
    let cc = ConfChange::new(ConfChangeType::AddNode, 3u64, Bytes::new()).into_v2();
    let mut payload = Vec::new();
    crate::wire::encode_conf_change_v2(&cc, &mut payload);
    entries.push(Entry::new(
      Term::new(1),
      Index::new(3),
      EntryKind::ConfChange,
      bytes::Bytes::from(payload),
    ));
  }
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(1),
      2u64,
      Index::ZERO,
      Term::ZERO,
      entries,
      Index::ZERO, // nothing known-committed yet
    )),
  );
  ep.handle_storage(d, &mut log, &mut stable);
  assert_eq!(ep.applied, Index::ZERO, "the tail is entirely unapplied");
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  // It wins leadership; its own no-op lands above the tail.
  let ed = ep.poll_timeout().expect("a voter arms its timer");
  ep.handle_timeout(ed, &mut log, &mut stable);
  ep.handle_storage(ed, &mut log, &mut stable);
  let ct = ep.term();
  for peer in [2u64, 3u64] {
    ep.handle_message(
      ed,
      &mut log,
      &mut stable,
      peer,
      Message::VoteResponse(VoteResponse::new(ct, peer, false, false)),
    );
  }
  ep.handle_storage(ed, &mut log, &mut stable);
  assert_eq!(ep.role(), Role::Leader, "it won");
  assert!(ep.applied < ep.term_start_index, "with the tail unapplied");
  while ep.poll_message().is_some() {}
  let term_start = ep.term_start_index;
  (ep, log, stable, term_start)
}

/// F1 — THE FIRE SITE. Draining an inherited `RemoveNode(3)` that a `AddNode(3)` in the SAME tail
/// undoes must emit nothing at all: the farewell's first shot is sent AT APPLY, so gating only the
/// re-drives would leave the initial shot already on the wire when the re-add prunes the entry two
/// entries later. The suppressed shot stays on the budget, and the prune then discards the whole
/// entry — the removed-then-restored peer is never told anything.
///
/// MUTATION: unsuppress the apply-time send → a farewell reaches node 3 before the re-add applies,
/// and node 3 tears itself down as a current member.
#[test]
fn an_inherited_removal_undone_in_the_same_tail_says_nothing_at_all() {
  use crate::Message;
  let (mut ep, mut log, mut stable, term_start) = leader_draining_an_inherited_removal(true);

  // The whole tail commits and applies in ONE pass, removal and re-add together.
  apply_the_term_start_noop(&mut ep, &mut log, &mut stable, Instant::ORIGIN, &[2u64]);
  assert!(ep.applied >= term_start, "the tail applied");
  assert!(
    ep.tracker.progress(&3u64).is_some(),
    "node 3 is a member per the committed configuration"
  );
  assert!(
    !ep.pending_farewells.contains_key(&3u64),
    "the re-add's fold pruned the retry entry"
  );
  assert!(
    drain_to(&mut ep, 3u64)
      .into_iter()
      .all(|m| !matches!(m, Message::AppendEntries(_) | Message::InstallSnapshot(_))),
    "and NO farewell of either arm was ever emitted to a peer that is still a member"
  );
}

/// The other half of the fire-site rule: a suppressed shot is DEFERRED, never lost. With no re-add
/// in the tail the removal stands, and the first post-gate drive delivers shot 1 through the
/// ordinary path — with the full budget, because suppression spent nothing.
///
/// MUTATION: mint the entry at the post-fire remainder instead of the full allowance → the peer
/// gets one fewer delivery attempt than a removal is supposed to buy it.
#[test]
fn a_suppressed_initial_farewell_is_delivered_once_the_tail_applies() {
  use core::time::Duration;
  let (mut ep, mut log, mut stable, term_start) = leader_draining_an_inherited_removal(false);

  // The removal applies; nothing is emitted yet, and the entry holds its FULL allowance.
  apply_the_term_start_noop(&mut ep, &mut log, &mut stable, Instant::ORIGIN, &[2u64]);
  assert!(ep.applied >= term_start, "the tail applied");
  assert!(
    ep.tracker.progress(&3u64).is_none(),
    "node 3 stays removed — no re-add followed"
  );
  let entry = ep
    .pending_farewells
    .get(&3u64)
    .expect("the retry entry was minted");
  assert_eq!(
    entry.shots_left, 3,
    "with the initial shot UNSPENT: the full allowance, not the post-fire remainder"
  );
  assert!(
    drain_to(&mut ep, 3u64).is_empty(),
    "and nothing went out during the window"
  );

  // The first post-gate drive delivers it.
  ep.handle_timeout(
    Instant::ORIGIN + Duration::from_millis(150),
    &mut log,
    &mut stable,
  );
  assert_eq!(
    drain_to(&mut ep, 3u64).len(),
    1,
    "the first drive past the gate delivers the deferred shot"
  );
  assert_eq!(
    ep.pending_farewells.get(&3u64).map(|f| f.shots_left),
    Some(2),
    "and only then is it spent"
  );
}

/// RECOVERY ALWAYS COMPLETES, because an owed sender's traffic is never suppressed. This pins the
/// ABSENCE of the withdrawn drop, and it is deliberately trivial to satisfy — that is the point.
///
/// Three topologies in a row broke every attempt to mute a departed peer's messages. The last one
/// needs no inherited tail at all: the gate is fully open, the re-add commits LATER through a
/// quorum that excludes the stale debt-holder, and the debt-holder simply never learns of it. Any
/// local licence to suppress — term-scoped, tail-scoped, vote-only — leaves such a leader unable
/// to commit and unwilling to be deposed, and the only live current-majority can never form. With
/// no suppression anywhere, the universal term mechanism does what it has always done.
///
/// Here A holds an applied `remove(B)` and its debt, and leads on the configuration it knows. B —
/// re-added by a later commit A never saw — is elected at a higher term by the others, and then a
/// voter fails, leaving exactly A and B alive.
///
/// MUTATION: reintroduce any suppression of an owed sender's messages → A discards B's append,
/// never steps down, and the group is permanently unavailable.
#[test]
fn an_owed_senders_traffic_is_never_suppressed_so_recovery_always_completes() {
  use crate::{
    AppendEntries, ConfChange, ConfChangeType, Entry, EntryKind, Message, Role, Term, VoteResponse,
  };

  // A removed node 3 as leader and has fully applied it — the gate is OPEN, no inherited tail.
  let (mut a, mut alog, mut astable, d, removal) = leader_that_removed_node3();
  apply_the_term_start_noop(&mut a, &mut alog, &mut astable, d, &[2u64]);
  assert!(
    a.applied >= a.term_start_index,
    "membership truth is settled — this is not the inherited-tail case"
  );
  assert!(
    a.courtesy_owed.contains_key(&3u64),
    "and A still owes node 3 a removal cure"
  );
  assert!(a.role().is_leader(), "A leads the configuration it knows");
  while a.poll_message().is_some() {}

  // Node 3 was RE-ADDED by a later commit that A never saw, and is elected at a higher term by the
  // other members. Its replication is the message A must not refuse.
  let higher = Term::new(a.term().get() + 1);
  let readd = {
    let cc = ConfChange::new(ConfChangeType::AddNode, 3u64, Bytes::new()).into_v2();
    let mut payload = Vec::new();
    crate::wire::encode_conf_change_v2(&cc, &mut payload);
    Entry::new(
      higher,
      removal.next(),
      EntryKind::ConfChange,
      bytes::Bytes::from(payload),
    )
  };
  a.handle_message(
    d,
    &mut alog,
    &mut astable,
    3u64,
    Message::AppendEntries(AppendEntries::new(
      higher,
      3u64,
      removal,
      a.term(),
      std::vec![readd],
      removal.next(),
    )),
  );

  // A took the byte-normal path: adopted, stepped down, and followed the current leader.
  assert_eq!(
    a.role(),
    Role::Follower,
    "A stepped down for the current leader"
  );
  assert_eq!(a.term(), higher, "adopting its term");
  a.handle_storage(d, &mut alog, &mut astable);
  assert!(
    a.tracker.progress(&3u64).is_some(),
    "and applied the re-add it had never seen"
  );
  assert!(
    !a.courtesy_owed.contains_key(&3u64),
    "A's own apply of the re-add pruned the debt"
  );

  // Availability: A acknowledges, so the current quorum {A, node 3} commits without the third
  // member — and no cure was ever sent to node 3 at any point.
  let acked = core::iter::from_fn(|| a.poll_message())
    .any(|o| o.to() == 3u64 && matches!(o.message(), Message::AppendResponse(r) if !r.reject()));
  assert!(acked, "A acknowledges the current leader's replication");
  assert!(
    drain_to(&mut a, 3u64)
      .into_iter()
      .all(|m| !matches!(m, Message::InstallSnapshot(_))),
    "and never offered a removal cure to a peer the configuration restored"
  );
  let _ = VoteResponse::new(higher, 2u64, false, false);
}

/// Fire the ignorant peer's election timer until it emits a REAL campaign carrying a term strictly
/// above `above` — the leader's. A removed peer's term lags after each re-election it triggers, so
/// it must time out again before it can depose anyone; that is the cure's own doing, and the loop
/// is what a real ignorant peer does while nobody tells it anything.
fn campaign_above<S: crate::StableStore<NodeId = u64>>(
  n3: &mut Endpoint<u64, CountSm>,
  log: &mut VecLog,
  stable: &mut S,
  above: crate::Term,
) -> (Message<u64>, Instant) {
  for _ in 0..8 {
    let t = n3
      .poll_timeout()
      .expect("the ignorant peer keeps its timer armed");
    n3.handle_timeout(t, log, stable);
    n3.handle_storage(t, log, stable);
    let campaign = core::iter::from_fn(|| n3.poll_message())
      .find(|o| matches!(o.message(), Message::RequestVote(r) if !r.pre_vote()));
    if let Some(o) = campaign
      && o.message().term() > above
    {
      return (o.into_parts().1, t);
    }
  }
  panic!("the removed peer never out-termed the leader");
}

/// Re-elect a deposed leader among the live pair and drive its own no-op to applied, returning the
/// tick at which its first cure may go out. The removed peer takes no part: it cannot win a quorum
/// of a configuration it is not in.
fn re_elect_among_the_live_pair<S: crate::StableStore<NodeId = u64>>(
  ep: &mut Endpoint<u64, CountSm>,
  log: &mut VecLog,
  stable: &mut S,
) -> Instant {
  use crate::{Message, VoteResponse};
  let rd = ep.poll_timeout().expect("the deposed leader re-arms");
  ep.handle_timeout(rd, log, stable);
  ep.handle_storage(rd, log, stable);
  let ct = ep.term();
  ep.handle_message(
    rd,
    log,
    stable,
    2u64,
    Message::VoteResponse(VoteResponse::new(ct, 2u64, false, false)),
  );
  ep.handle_storage(rd, log, stable);
  assert!(ep.role().is_leader(), "the live pair re-elects");
  apply_the_term_start_noop(ep, log, stable, rd, &[2u64]);
  while ep.poll_message().is_some() {}
  rd
}

/// LOSING AN OFFER must cost nothing but time. The frame is dropped on the wire, the debt is still
/// there — sending was never evidence of anything — and the next window offers again, and THAT one
/// lands. Retention is what makes a lossy link merely slow instead of terminal.
///
/// MUTATION: discharge the debt at enqueue → the lost offer retires the cure, and the peer that
/// never received a byte of it is owed nothing by anyone, forever.
#[test]
fn a_lost_offer_leaves_the_debt_standing_for_the_next_leadership() {
  use crate::{Entry, EntryKind, Event, Role, Term};

  let (mut ep, mut log, mut stable, d, removal) = leader_that_removed_node3();
  ep.pending_farewells_clear_for_test();
  give_leader_a_snapshot(&mut ep, &mut stable, removal, encode_count_snapshot(7));
  let (mut n3, mut n3log, mut n3stable) = node3_with_log_async(
    std::vec![Entry::new(
      Term::new(1),
      Index::new(1),
      EntryKind::Empty,
      bytes::Bytes::new()
    )],
    Index::new(1),
  );

  // Cycle 1: node 3 campaigns, deposes, the re-elected leader offers — and the offer is LOST.
  let (campaign, n3d) = campaign_above(&mut n3, &mut n3log, &mut n3stable, ep.term());
  ep.handle_message(d, &mut log, &mut stable, 3u64, campaign);
  assert_eq!(ep.role(), Role::Follower, "deposition one");
  let rd = re_elect_among_the_live_pair(&mut ep, &mut log, &mut stable);
  ep.handle_timeout(rd + Duration::from_millis(150), &mut log, &mut stable);
  let lost = courtesy_offers_to_node3(&mut ep);
  assert_eq!(lost.len(), 1, "the first tick offered the cure");
  drop(lost); // the frame never arrives

  assert!(
    ep.courtesy_owed.contains_key(&3u64),
    "and the debt STANDS: enqueueing is not delivering"
  );

  // The cure is re-offered once the cooldown elapses — the debt was never spent, so the retry is
  // simply the next window's send. (Driving a SECOND natural deposition here would prove the same
  // retention with far more fragile term arithmetic; the persistent-loss regression below runs five
  // such cycles end to end and asserts exactly that.)
  let due = ep
    .courtesy_owed
    .get(&3u64)
    .and_then(|dbt| dbt.next_at)
    .expect("the retained debt is cooling, not spent");
  ep.handle_timeout(due + Duration::from_millis(1), &mut log, &mut stable);
  let offer = courtesy_offers_to_node3(&mut ep)
    .into_iter()
    .next()
    .expect("the retained debt re-offers in the next window");
  let Message::InstallSnapshot(ref is) = offer else {
    unreachable!("filtered above")
  };
  assert!(
    is.term() >= n3.term(),
    "and it is offered at a term the peer accepts"
  );
  n3.handle_message(n3d, &mut n3log, &mut n3stable, 1u64, offer);
  n3.handle_storage(n3d, &mut n3log, &mut n3stable);
  assert!(
    core::iter::from_fn(|| n3.poll_event())
      .any(|e| matches!(e, Event::ConfChanged(cc) if !cc.conf().voters().contains(&3u64))),
    "the cure lands on the first DELIVERED offer"
  );

  // No further deposition is possible: a cured peer is a non-voter.
  let settled = n3.term();
  for _ in 0..4 {
    let Some(t) = n3.poll_timeout() else { break };
    n3.handle_timeout(t, &mut n3log, &mut n3stable);
    n3.handle_storage(t, &mut n3log, &mut n3stable);
  }
  assert_eq!(
    n3.role(),
    Role::Follower,
    "and no further deposition is possible"
  );
  assert_eq!(n3.term(), settled, "and no further term inflation");
}

/// PERSISTENT TARGETED LOSS of every courtesy frame. The degradation is the honest bound: one
/// self-healing deposition per election-timeout window, the debt retained throughout, and the send
/// rate bounded to one whole-blob frame per window — never an uncured peer that nobody owes
/// anything, which is exactly what a charge-on-enqueue budget produced after three lost frames.
///
/// MUTATION: charge the debt at enqueue → the debt is gone by cycle four and the peer disrupts
/// forever with no standing cure.
#[test]
fn persistent_offer_loss_degrades_to_one_deposition_per_window_and_never_loses_the_cure() {
  use crate::{Entry, EntryKind, Role, Term};

  let (mut ep, mut log, mut stable, d, removal) = leader_that_removed_node3();
  ep.pending_farewells_clear_for_test();
  give_leader_a_snapshot(&mut ep, &mut stable, removal, encode_count_snapshot(7));
  let (mut n3, mut n3log, mut n3stable) = node3_with_log_async(
    std::vec![Entry::new(
      Term::new(1),
      Index::new(1),
      EntryKind::Empty,
      bytes::Bytes::new()
    )],
    Index::new(1),
  );

  let mut depositions = 0usize;
  let mut offers = 0usize;
  let mut at = d;
  for _ in 0..5 {
    let (campaign, _) = campaign_above(&mut n3, &mut n3log, &mut n3stable, ep.term());
    ep.handle_message(at, &mut log, &mut stable, 3u64, campaign);
    assert_eq!(ep.role(), Role::Follower, "each campaign deposes");
    depositions += 1;

    at = re_elect_among_the_live_pair(&mut ep, &mut log, &mut stable);
    // A whole window of ticks yields exactly ONE offer — the cooldown is the rate bound.
    let mut in_window = 0usize;
    for k in 0..6 {
      ep.handle_timeout(
        at + Duration::from_millis(150 * (k + 1)),
        &mut log,
        &mut stable,
      );
      in_window += courtesy_offers_to_node3(&mut ep).len(); // every frame is LOST
    }
    assert_eq!(
      in_window, 1,
      "one whole-blob offer per peer per cooldown window"
    );
    offers += in_window;

    assert!(
      ep.courtesy_owed.contains_key(&3u64),
      "the debt is retained through every lost cycle"
    );
  }
  assert_eq!(
    depositions, 5,
    "each deposition is self-healed by re-election"
  );
  assert_eq!(offers, 5, "and each cycle re-offered the cure");
  assert!(
    ep.role().is_leader() && ep.courtesy_owed.contains_key(&3u64),
    "the group is serving and the cure still stands — never uncured-and-debtless"
  );
}

/// The remaining eviction authorities, unchanged by the move to evidence: a committed re-add still
/// discharges a debt, and the map's capacity still evicts the oldest.
#[test]
fn re_add_and_the_cap_remain_eviction_authorities() {
  use crate::{AppendEntries, ConfChange, ConfChangeType, Entry, EntryKind, Message, Term};

  // (a) the re-add edge.
  let (mut ep, mut log, mut stable) = follower_with_an_inherited_readd();
  assert!(
    ep.courtesy_owed.contains_key(&3u64),
    "the removal minted it"
  );
  let readd = {
    let cc = ConfChange::new(ConfChangeType::AddNode, 3u64, Bytes::new()).into_v2();
    let mut payload = Vec::new();
    crate::wire::encode_conf_change_v2(&cc, &mut payload);
    Entry::new(
      Term::new(1),
      Index::new(3),
      EntryKind::ConfChange,
      bytes::Bytes::from(payload),
    )
  };
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
      std::vec![readd],
      Index::new(3),
    )),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  assert!(
    !ep.courtesy_owed.contains_key(&3u64),
    "a committed re-add still discharges the debt"
  );

  // (b) the cap: 64 debts is the ceiling, and the OLDEST goes.
  let (mut ep, _log, _stable, _d, removal) = leader_that_removed_node3();
  for n in 0..70u64 {
    ep.note_courtesy_debt_for_test(100 + n, Index::new(removal.get() + n));
  }
  assert_eq!(ep.courtesy_owed.len(), 64, "the cap holds");
  assert!(
    !ep.courtesy_owed.contains_key(&100u64),
    "and the oldest debt is what was evicted"
  );
  assert!(
    ep.courtesy_owed.contains_key(&169u64),
    "while the newest is retained"
  );
}
