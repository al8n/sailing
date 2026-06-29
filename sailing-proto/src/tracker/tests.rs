use super::*;
use crate::{ConfChangeSingle, ConfChangeType, Index, VoteResult};
use confchange::{Changer, ConfChangeError};
use std::{collections::BTreeMap, vec};

/// Build a `Changer` with sensible defaults for tests.
fn changer(last_index: u64) -> Changer {
  Changer::new(Index::new(last_index), 256, 0)
}

/// Build a simple (non-joint) `Tracker` with voters `ids`.
fn tracker_with_voters(ids: &[u64]) -> Tracker<u64> {
  let cs = ConfState::from_voters(ids.iter().copied());
  Tracker::from_conf_state(&cs, Index::new(1), 256, 0)
}

/// Shorthand `ConfChangeSingle` constructors.
fn add(id: u64) -> ConfChangeSingle<u64> {
  ConfChangeSingle::new(ConfChangeType::AddNode, id)
}
fn remove(id: u64) -> ConfChangeSingle<u64> {
  ConfChangeSingle::new(ConfChangeType::RemoveNode, id)
}
fn add_learner(id: u64) -> ConfChangeSingle<u64> {
  ConfChangeSingle::new(ConfChangeType::AddLearnerNode, id)
}

#[test]
fn tracker_default_is_empty() {
  let t = Tracker::<u64>::new();
  assert!(!t.is_joint());
  assert!(t.ids().is_empty());
  assert!(t.progress_map().is_empty());
}

#[test]
fn tracker_from_conf_state_installs_progress() {
  let cs = ConfState::new(vec![1u64, 2, 3], vec![4u64], vec![], vec![], false);
  let t = Tracker::from_conf_state(&cs, Index::new(10), 256, 0);
  assert_eq!(t.progress_map().len(), 4);
  assert!(t.progress(&1).is_some());
  assert!(t.progress(&4).is_some());
  assert!(t.is_voter(&1));
  assert!(t.is_learner(&4));
  assert!(!t.is_joint());
}

#[test]
fn tracker_conf_state_roundtrip() {
  let cs = ConfState::new(vec![1u64, 2, 3], vec![5u64], vec![4u64, 5u64], vec![], true);
  let t = Tracker::from_conf_state(&cs, Index::new(5), 256, 0);
  let out = t.conf_state();
  assert_eq!(out.voters(), cs.voters());
  assert_eq!(out.learners(), cs.learners());
  assert_eq!(out.voters_outgoing(), cs.voters_outgoing());
  assert_eq!(out.auto_leave(), cs.auto_leave());
}

#[test]
fn quorum_committed_simple() {
  // 3-voter config, match indices 10, 12, 14 → median = 12.
  let mut t = tracker_with_voters(&[1, 2, 3]);
  t.progress_mut(&1).unwrap().maybe_update(Index::new(10));
  t.progress_mut(&2).unwrap().maybe_update(Index::new(12));
  t.progress_mut(&3).unwrap().maybe_update(Index::new(14));
  assert_eq!(t.quorum_committed(), Index::new(12));
}

#[test]
fn quorum_committed_absent_voter_is_zero() {
  // voter 1 has no match (0), 2→5, 3→7 → sorted [0,5,7], pos=1 → 5.
  let mut t = tracker_with_voters(&[1, 2, 3]);
  t.progress_mut(&2).unwrap().maybe_update(Index::new(5));
  t.progress_mut(&3).unwrap().maybe_update(Index::new(7));
  assert_eq!(t.quorum_committed(), Index::new(5));
}

#[test]
fn vote_result_simple_won() {
  let t = tracker_with_voters(&[1, 2, 3]);
  let votes = BTreeMap::from([(1u64, true), (2u64, true), (3u64, false)]);
  assert_eq!(t.vote_result(&votes), VoteResult::Won);
}

#[test]
fn vote_result_simple_lost() {
  let t = tracker_with_voters(&[1, 2, 3]);
  let votes = BTreeMap::from([(1u64, true), (2u64, false), (3u64, false)]);
  assert_eq!(t.vote_result(&votes), VoteResult::Lost);
}

#[test]
fn vote_result_simple_pending() {
  let t = tracker_with_voters(&[1, 2, 3]);
  let votes = BTreeMap::from([(1u64, true)]);
  assert_eq!(t.vote_result(&votes), VoteResult::Pending);
}

#[test]
fn simple_add_voter() {
  // 3→4 voters: add node 4.
  let t = tracker_with_voters(&[1, 2, 3]);
  let next = changer(5).simple(&t, &[add(4)]).unwrap();
  assert!(next.is_voter(&4));
  assert!(next.progress(&4).is_some());
  assert!(!next.is_joint());
  assert_eq!(next.ids().len(), 4);
}

#[test]
fn simple_remove_voter() {
  // {1,2,3} → remove 3 → {1,2}.
  let t = tracker_with_voters(&[1, 2, 3]);
  let next = changer(5).simple(&t, &[remove(3)]).unwrap();
  assert!(!next.is_voter(&3));
  assert!(next.progress(&3).is_none());
  assert_eq!(next.ids().len(), 2);
}

#[test]
fn simple_add_learner() {
  let t = tracker_with_voters(&[1, 2, 3]);
  let next = changer(5).simple(&t, &[add_learner(4)]).unwrap();
  assert!(next.is_learner(&4));
  assert!(!next.is_voter(&4));
  assert!(next.progress(&4).is_some());
}

#[test]
fn simple_rejects_multiple_voter_changes() {
  // Adding 2 voters at once requires joint.
  let t = tracker_with_voters(&[1, 2, 3]);
  let err = changer(5).simple(&t, &[add(4), add(5)]).unwrap_err();
  assert_eq!(err, ConfChangeError::MultipleVoterChanges);
}

#[test]
fn simple_rejects_when_joint() {
  // Pre-condition: put the tracker into a joint state.
  let t = tracker_with_voters(&[1, 2, 3]);
  let joint = changer(5).enter_joint(&t, false, &[add(4)]).unwrap();
  assert!(joint.is_joint());
  let err = changer(5).simple(&joint, &[add(5)]).unwrap_err();
  assert_eq!(err, ConfChangeError::SimpleInJoint);
}

#[test]
fn simple_rejects_all_voters_removed() {
  let t = tracker_with_voters(&[1]);
  let err = changer(5).simple(&t, &[remove(1)]).unwrap_err();
  assert_eq!(err, ConfChangeError::EmptyVoterSet);
}

#[test]
fn enter_joint_basic_swap() {
  // {1,2,3}: swap node 3 for node 4 via joint.
  let t = tracker_with_voters(&[1, 2, 3]);
  let joint = changer(5)
    .enter_joint(&t, true, &[add(4), remove(3)])
    .unwrap();

  assert!(joint.is_joint());
  assert!(joint.auto_leave());
  // Incoming has 4, outgoing has 3.
  assert!(joint.voters().incoming().contains(&4));
  assert!(!joint.voters().incoming().contains(&3));
  assert!(joint.voters().outgoing().contains(&3));
  // Progress for 3 must still exist (it's in the outgoing half).
  assert!(joint.progress(&3).is_some());
  // Progress for 4 was freshly created.
  assert!(joint.progress(&4).is_some());

  // leave_joint → simple config.
  let simple = changer(5).leave_joint(&joint).unwrap();
  assert!(!simple.is_joint());
  assert!(!simple.auto_leave());
  assert!(simple.is_voter(&4));
  assert!(!simple.is_voter(&3));
  // Progress for 3 must be gone (no longer in any set).
  assert!(simple.progress(&3).is_none());
}

#[test]
fn enter_joint_rejects_empty_incoming() {
  // A joint transition snapshots the incoming voters as the outgoing quorum; an empty incoming set
  // would leave the old quorum empty, so it is rejected.
  let empty = tracker_with_voters(&[]);
  let err = changer(5)
    .enter_joint(&empty, false, &[add(4)])
    .unwrap_err();
  assert_eq!(err, ConfChangeError::EmptyIncomingForJoint);
}

#[test]
fn simple_add_learner_is_idempotent() {
  // Re-adding an existing learner is a no-op (the `make_learner` already-a-learner short-circuit).
  let t = tracker_with_voters(&[1, 2, 3]);
  let t = changer(5).simple(&t, &[add_learner(4)]).unwrap();
  assert!(t.is_learner(&4));
  let again = changer(5).simple(&t, &[add_learner(4)]).unwrap();
  assert!(again.is_learner(&4));
  assert!(!again.is_voter(&4));
  assert_eq!(again.progress_map().len(), 4, "no duplicate progress entry");
}

#[test]
fn simple_remove_nonmember_is_noop() {
  // Removing a node that is not in the cluster is a no-op (the `remove` not-in-progress short-circuit),
  // not an error.
  let t = tracker_with_voters(&[1, 2, 3]);
  let next = changer(5).simple(&t, &[remove(99)]).unwrap();
  assert_eq!(next.ids().len(), 3);
  assert!(next.is_voter(&1) && next.is_voter(&2) && next.is_voter(&3));
}

#[test]
fn tracker_progress_map_mutators_and_accessors() {
  use crate::progress::Progress;
  // Default delegates to new() (an empty tracker).
  let d = Tracker::<u64>::default();
  assert!(d.ids().is_empty());
  assert!(!d.is_joint());

  let mut t = tracker_with_voters(&[1, 2, 3]);
  // insert_progress: a NEW id inserts; the SAME id REPLACES in place (the binary-search map's two arms).
  t.insert_progress(9, Progress::new(Index::new(7), 256, 0));
  assert_eq!(t.progress(&9).unwrap().next_index(), Index::new(7));
  t.insert_progress(9, Progress::new(Index::new(11), 256, 0));
  assert_eq!(
    t.progress(&9).unwrap().next_index(),
    Index::new(11),
    "re-inserting an existing id replaces in place"
  );
  // progress_mut on a missing id resolves to None (the map's get_mut Err arm).
  assert!(t.progress_mut(&404).is_none());
  // remove_progress: a present id is removed; an absent id is a no-op.
  t.remove_progress(&9);
  assert!(t.progress(&9).is_none());
  t.remove_progress(&404);
  // learners_next is empty in a simple config.
  assert!(t.learners_next().is_empty());
}

#[test]
fn enter_joint_rejects_already_joint() {
  let t = tracker_with_voters(&[1, 2, 3]);
  let joint = changer(5).enter_joint(&t, false, &[add(4)]).unwrap();
  let err = changer(5)
    .enter_joint(&joint, false, &[add(5)])
    .unwrap_err();
  assert_eq!(err, ConfChangeError::AlreadyJoint);
}

#[test]
fn leave_joint_rejects_not_joint() {
  let t = tracker_with_voters(&[1, 2, 3]);
  let err = changer(5).leave_joint(&t).unwrap_err();
  assert_eq!(err, ConfChangeError::NotJoint);
}

#[test]
fn add_learner_on_outgoing_voter_goes_to_learners_next() {
  // {1,2,3}: demote node 3 to learner via joint.
  // During enter_joint: node 3 is in outgoing, so it should go to learners_next,
  // not learners.
  let t = tracker_with_voters(&[1, 2, 3]);
  let joint = changer(5)
    .enter_joint(&t, false, &[add_learner(3)])
    .unwrap();

  assert!(joint.is_joint());
  // Node 3 is in outgoing voters (needed for old quorum).
  assert!(joint.voters().outgoing().contains(&3));
  // Node 3 is staged in learners_next, not learners.
  assert!(joint.is_learner_next(&3));
  assert!(!joint.is_learner(&3));
  // Progress for 3 must exist (still in outgoing).
  assert!(joint.progress(&3).is_some());

  // After leave_joint: node 3 moves from learners_next to learners.
  let simple = changer(5).leave_joint(&joint).unwrap();
  assert!(!simple.is_joint());
  assert!(simple.is_learner(&3));
  assert!(!simple.is_voter(&3));
  assert!(simple.progress(&3).is_some());
}

#[test]
fn remove_keeps_progress_while_in_outgoing() {
  // enter_joint removes node 3 from incoming but keeps its Progress (still in outgoing).
  let t = tracker_with_voters(&[1, 2, 3]);
  let joint = changer(5).enter_joint(&t, false, &[remove(3)]).unwrap();

  assert!(joint.voters().outgoing().contains(&3));
  assert!(!joint.voters().incoming().contains(&3));
  // Progress must still be present.
  assert!(
    joint.progress(&3).is_some(),
    "Progress must survive while node 3 is in outgoing"
  );

  // leave_joint drops the Progress.
  let simple = changer(5).leave_joint(&joint).unwrap();
  assert!(simple.progress(&3).is_none());
}

// invariant: voters ∩ learners = ∅ after promote/demote

#[test]
fn no_voter_learner_overlap_after_promote_demote() {
  // Start: {1,2,3}, learner {4}.
  let cs = ConfState::new(vec![1u64, 2, 3], vec![4u64], vec![], vec![], false);
  let t = Tracker::from_conf_state(&cs, Index::new(1), 256, 0);

  // Promote learner 4 to voter via simple change.
  let after_promote = changer(5).simple(&t, &[add(4)]).unwrap();
  assert!(after_promote.is_voter(&4));
  assert!(!after_promote.is_learner(&4));

  // Demote voter 3 to learner via joint.
  let joint = changer(5)
    .enter_joint(&after_promote, false, &[add_learner(3)])
    .unwrap();
  // 3 is in outgoing, so it's staged in learners_next, not learners — invariant holds.
  assert!(!joint.is_learner(&3));
  assert!(joint.is_learner_next(&3));

  let simple = changer(5).leave_joint(&joint).unwrap();
  assert!(simple.is_learner(&3));
  assert!(!simple.is_voter(&3));
  // Verify no overlap.
  for id in simple.learners() {
    assert!(!simple.is_voter(id), "voter-learner overlap: {id}");
  }
}

#[test]
fn quorum_committed_joint_blocked_by_outgoing() {
  // Incoming {1,2,3}: match 10,12,14 → committed 12.
  // Outgoing {4,5,6}: match 5,6,7 → committed 6.
  // Joint: min(12,6) = 6 — the outgoing half blocks.
  //
  // Build the tracker directly from a joint ConfState so all six nodes have Progress.
  let cs = ConfState::new(vec![1u64, 2, 3], vec![], vec![4u64, 5, 6], vec![], false);
  let mut jt = Tracker::from_conf_state(&cs, Index::new(1), 256, 0);
  jt.progress_mut(&1).unwrap().maybe_update(Index::new(10));
  jt.progress_mut(&2).unwrap().maybe_update(Index::new(12));
  jt.progress_mut(&3).unwrap().maybe_update(Index::new(14));
  jt.progress_mut(&4).unwrap().maybe_update(Index::new(5));
  jt.progress_mut(&5).unwrap().maybe_update(Index::new(6));
  jt.progress_mut(&6).unwrap().maybe_update(Index::new(7));
  assert_eq!(jt.quorum_committed(), Index::new(6));
}

#[test]
fn vote_result_joint_requires_both_halves() {
  // Joint: incoming {1,2,3}, outgoing {4,5,6}.
  // incoming votes: 1 yes, 2 yes → Won; outgoing: 4 yes, 5 no → Lost.
  // Joint result: Lost (either half Lost → Lost).
  let cs = ConfState::new(vec![1u64, 2, 3], vec![], vec![4u64, 5, 6], vec![], false);
  let jt = Tracker::from_conf_state(&cs, Index::new(1), 256, 0);
  let votes = BTreeMap::from([
    (1u64, true),
    (2u64, true),
    (3u64, false),
    (4u64, true),
    (5u64, false),
    (6u64, false),
  ]);
  assert_eq!(jt.vote_result(&votes), VoteResult::Lost);
}

#[test]
fn vote_result_joint_won() {
  // Both halves win.
  let cs = ConfState::new(vec![1u64, 2, 3], vec![], vec![4u64, 5, 6], vec![], false);
  let jt = Tracker::from_conf_state(&cs, Index::new(1), 256, 0);
  let votes = BTreeMap::from([
    (1u64, true),
    (2u64, true),
    (3u64, false),
    (4u64, true),
    (5u64, true),
    (6u64, false),
  ]);
  assert_eq!(jt.vote_result(&votes), VoteResult::Won);
}

#[test]
fn progress_map_in_sync_after_operations() {
  let t = tracker_with_voters(&[1, 2, 3]);

  // simple add learner 4 → progress has {1,2,3,4}.
  let t = changer(5).simple(&t, &[add_learner(4)]).unwrap();
  assert_eq!(t.progress_map().len(), 4);

  // enter_joint: add 5, remove 3 → progress has {1,2,3,4,5}.
  let t = changer(5)
    .enter_joint(&t, false, &[add(5), remove(3)])
    .unwrap();
  // 3 is in outgoing, so its progress is kept.
  assert!(t.progress(&3).is_some());
  assert!(t.progress(&5).is_some());

  // leave_joint → 3 is gone.
  let t = changer(5).leave_joint(&t).unwrap();
  assert!(t.progress(&3).is_none());
  // All remaining members have progress.
  for id in t.ids() {
    assert!(t.progress(&id).is_some(), "missing progress for {id}");
  }
  // No orphan progress entries.
  let needed = t.ids();
  for (id, _) in t.progress_map() {
    assert!(needed.contains(id), "orphan progress for {id}");
  }
}
