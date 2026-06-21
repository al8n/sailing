use super::*;
use std::vec;

#[test]
fn conf_state_from_voters_deduplicates() {
  let c = ConfState::from_voters(vec![3u64, 1u64, 2u64, 1u64]);
  assert_eq!(c.voters(), &BTreeSet::from([1u64, 2u64, 3u64]));
  assert_eq!(c.len(), 3);
  assert!(c.is_voter(&2u64));
  assert!(!c.is_voter(&99u64));
  assert!(!c.is_joint());
  assert!(c.learners().is_empty());
}

#[test]
fn conf_state_empty() {
  let c = ConfState::<u64>::from_voters(vec![]);
  assert!(c.is_empty());
}

#[test]
fn conf_state_is_valid_accepts_legitimate_configs() {
  // Simple (non-joint) config.
  assert!(ConfState::from_voters(vec![1u64, 2u64, 3u64]).is_valid());
  // Simple config with a learner disjoint from the voters.
  assert!(ConfState::new(vec![1u64, 2u64], vec![3u64], vec![], vec![], false).is_valid());
  // Joint config mid-change (incoming {1,2,3}, outgoing {1,2}), no staged demotions.
  assert!(
    ConfState::new(
      vec![1u64, 2u64, 3u64],
      vec![],
      vec![1u64, 2u64],
      vec![],
      false
    )
    .is_valid()
  );
  // Joint config demoting an outgoing voter to a learner on leave (3 in outgoing + learners_next).
  assert!(
    ConfState::new(
      vec![1u64, 2u64],
      vec![],
      vec![1u64, 2u64, 3u64],
      vec![3u64],
      true
    )
    .is_valid()
  );
}

#[test]
fn conf_state_is_valid_rejects_malformed_configs() {
  // Empty incoming voters — a live cluster cannot form a quorum.
  assert!(!ConfState::<u64>::from_voters(vec![]).is_valid());
  assert!(!ConfState::<u64>::default().is_valid());
  // Learner overlaps an incoming voter.
  assert!(!ConfState::new(vec![1u64, 2u64], vec![1u64], vec![], vec![], false).is_valid());
  // Learner overlaps an outgoing voter.
  assert!(
    !ConfState::new(
      vec![1u64, 2u64],
      vec![3u64],
      vec![3u64, 1u64],
      vec![],
      false
    )
    .is_valid()
  );
  // learners_next member is not an outgoing voter.
  assert!(!ConfState::new(vec![1u64, 2u64], vec![], vec![1u64, 2u64], vec![9u64], true).is_valid());
  // Non-joint (no outgoing) but auto_leave set.
  assert!(!ConfState::new(vec![1u64, 2u64], vec![], vec![], vec![], true).is_valid());
  // Non-joint (no outgoing) but learners_next non-empty.
  assert!(!ConfState::new(vec![1u64, 2u64], vec![], vec![], vec![3u64], false).is_valid());
  // learners_next member is ALSO an incoming voter (cannot both stay a voter and be demoted).
  assert!(
    !ConfState::new(
      vec![1u64, 2u64, 3u64],
      vec![],
      vec![1u64, 2u64, 3u64],
      vec![3u64],
      true
    )
    .is_valid()
  );
}

#[test]
fn conf_state_full_constructor() {
  let c = ConfState::new(
    vec![1u64, 2u64],
    vec![3u64],
    vec![4u64, 5u64],
    vec![6u64],
    true,
  );
  assert_eq!(c.voters(), &BTreeSet::from([1u64, 2u64]));
  assert_eq!(c.learners(), &BTreeSet::from([3u64]));
  assert_eq!(c.voters_outgoing(), &BTreeSet::from([4u64, 5u64]));
  assert_eq!(c.learners_next(), &BTreeSet::from([6u64]));
  assert!(c.auto_leave());
  assert!(c.is_joint());
  assert!(c.is_learner(&3u64));
  assert!(!c.is_learner(&1u64));
}

#[test]
fn conf_state_default_is_empty() {
  let c = ConfState::<u64>::default();
  assert!(c.is_empty());
  assert!(!c.is_joint());
  assert!(!c.auto_leave());
}

#[test]
fn conf_change_type_display() {
  assert_eq!(ConfChangeType::AddNode.as_str(), "add_node");
  assert_eq!(ConfChangeType::RemoveNode.as_str(), "remove_node");
  assert_eq!(ConfChangeType::AddLearnerNode.as_str(), "add_learner_node");
  assert_eq!(
    std::format!("{}", ConfChangeType::AddLearnerNode),
    "add_learner_node"
  );
}

#[test]
fn conf_change_type_is_variant() {
  assert!(ConfChangeType::AddNode.is_add_node());
  assert!(!ConfChangeType::AddNode.is_remove_node());
  assert!(ConfChangeType::RemoveNode.is_remove_node());
  assert!(ConfChangeType::AddLearnerNode.is_add_learner_node());
}

#[test]
fn conf_change_transition_default_is_auto() {
  assert_eq!(ConfChangeTransition::default(), ConfChangeTransition::Auto);
}

#[test]
fn conf_change_transition_display_and_variants() {
  assert_eq!(ConfChangeTransition::Auto.as_str(), "auto");
  assert_eq!(ConfChangeTransition::Implicit.as_str(), "implicit");
  assert_eq!(ConfChangeTransition::Explicit.as_str(), "explicit");
  assert!(ConfChangeTransition::Auto.is_auto());
  assert!(ConfChangeTransition::Implicit.is_implicit());
  assert!(ConfChangeTransition::Explicit.is_explicit());
}

#[test]
fn conf_change_into_v2() {
  let c = ConfChange::new(ConfChangeType::AddNode, 5u64, Bytes::from_static(b"x"));
  let v2 = c.clone().into_v2();
  assert_eq!(v2.transition(), ConfChangeTransition::Auto);
  assert_eq!(v2.changes().len(), 1);
  assert_eq!(v2.changes()[0].ty(), ConfChangeType::AddNode);
  assert_eq!(v2.changes()[0].node(), 5u64);
  assert_eq!(v2.context(), &Bytes::from_static(b"x"));
}
