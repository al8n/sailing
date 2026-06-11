use super::*;
use std::vec;

/// Test helper: exact decode from a plain slice (copies into `Bytes` for the cursor API).
fn dx<T: crate::Data>(buf: &[u8]) -> Result<T, crate::DecodeError> {
  T::decode_exact(bytes::Bytes::copy_from_slice(buf))
}

// ── ConfState ──────────────────────────────────────────────────────────────

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

// ── ConfChangeType ─────────────────────────────────────────────────────────

#[test]
fn conf_change_type_roundtrip() {
  for ty in [
    ConfChangeType::AddNode,
    ConfChangeType::RemoveNode,
    ConfChangeType::AddLearnerNode,
  ] {
    let mut buf = std::vec::Vec::new();
    ty.encode(&mut buf);
    assert_eq!(buf.len(), 1);
    let decoded = dx::<ConfChangeType>(&buf).unwrap();
    assert_eq!(decoded, ty);
  }
}

#[test]
fn conf_change_type_bad_discriminant() {
  assert!(dx::<ConfChangeType>(&[99u8]).is_err());
}

#[test]
fn conf_change_type_empty_buf() {
  assert!(matches!(
    dx::<ConfChangeType>(&[]),
    Err(DecodeError::UnexpectedEof)
  ));
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

// ── ConfChangeTransition ───────────────────────────────────────────────────

#[test]
fn conf_change_transition_roundtrip() {
  for tr in [
    ConfChangeTransition::Auto,
    ConfChangeTransition::Implicit,
    ConfChangeTransition::Explicit,
  ] {
    let mut buf = std::vec::Vec::new();
    tr.encode(&mut buf);
    assert_eq!(buf.len(), 1);
    let decoded = dx::<ConfChangeTransition>(&buf).unwrap();
    assert_eq!(decoded, tr);
  }
}

#[test]
fn conf_change_transition_default_is_auto() {
  assert_eq!(ConfChangeTransition::default(), ConfChangeTransition::Auto);
}

#[test]
fn conf_change_transition_bad_discriminant() {
  assert!(dx::<ConfChangeTransition>(&[99u8]).is_err());
}

#[test]
fn conf_change_transition_empty_buf() {
  assert!(matches!(
    dx::<ConfChangeTransition>(&[]),
    Err(DecodeError::UnexpectedEof)
  ));
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

// ── ConfChangeSingle ───────────────────────────────────────────────────────

#[test]
fn conf_change_single_roundtrip() {
  let c = ConfChangeSingle::new(ConfChangeType::AddNode, 42u64);
  let mut buf = std::vec::Vec::new();
  c.encode(&mut buf);
  let decoded = dx::<ConfChangeSingle<u64>>(&buf).unwrap();
  assert_eq!(decoded, c);
}

#[test]
fn conf_change_single_truncated() {
  // One byte (type only, missing node).
  assert!(dx::<ConfChangeSingle::<u64>>(&[0u8]).is_err());
}

#[test]
fn conf_change_single_empty_buf() {
  assert!(dx::<ConfChangeSingle::<u64>>(&[]).is_err());
}

// ── ConfChange ─────────────────────────────────────────────────────────────

#[test]
fn conf_change_roundtrip_with_context() {
  let c = ConfChange::new(ConfChangeType::AddNode, 7u64, Bytes::from_static(b"ctx"));
  let mut buf = std::vec::Vec::new();
  c.encode(&mut buf);
  let decoded = dx::<ConfChange<u64>>(&buf).unwrap();
  assert_eq!(decoded, c);
}

#[test]
fn conf_change_roundtrip_empty_context() {
  let c = ConfChange::new(ConfChangeType::RemoveNode, 3u64, Bytes::new());
  let mut buf = std::vec::Vec::new();
  c.encode(&mut buf);
  let decoded = dx::<ConfChange<u64>>(&buf).unwrap();
  assert_eq!(decoded, c);
}

#[test]
fn conf_change_truncated() {
  // Truncated after type byte.
  assert!(dx::<ConfChange::<u64>>(&[0u8]).is_err());
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

// ── ConfChangeV2 ───────────────────────────────────────────────────────────

#[test]
fn conf_change_v2_roundtrip_empty_changes() {
  let v2 = ConfChangeV2::new(
    ConfChangeTransition::Explicit,
    std::vec![],
    Bytes::from_static(b"empty"),
  );
  let mut buf = std::vec::Vec::new();
  v2.encode(&mut buf);
  let decoded = dx::<ConfChangeV2<u64>>(&buf).unwrap();
  assert_eq!(decoded, v2);
}

#[test]
fn conf_change_v2_roundtrip_multi_change() {
  let v2 = ConfChangeV2::new(
    ConfChangeTransition::Implicit,
    std::vec![
      ConfChangeSingle::new(ConfChangeType::AddNode, 1u64),
      ConfChangeSingle::new(ConfChangeType::AddLearnerNode, 2u64),
      ConfChangeSingle::new(ConfChangeType::RemoveNode, 3u64),
    ],
    Bytes::from_static(b"multi"),
  );
  let mut buf = std::vec::Vec::new();
  v2.encode(&mut buf);
  let decoded = dx::<ConfChangeV2<u64>>(&buf).unwrap();
  assert_eq!(decoded, v2);
}

#[test]
fn conf_change_v2_truncated_after_transition() {
  // Only the transition byte, nothing else.
  assert!(dx::<ConfChangeV2::<u64>>(&[0u8]).is_err());
}

#[test]
fn conf_change_v2_truncated_mid_changes() {
  // transition + length=2 + only 1 full change (9 bytes) then truncated
  let mut buf = std::vec::Vec::new();
  ConfChangeTransition::Auto.encode(&mut buf); // 1 byte
  (2u64).encode(&mut buf); // 8 bytes (len = 2 changes)
  ConfChangeSingle::new(ConfChangeType::AddNode, 1u64).encode(&mut buf); // 1+8 = 9 bytes
  // missing 2nd change → decode must fail, not panic
  assert!(dx::<ConfChangeV2::<u64>>(&buf).is_err());
}

#[test]
fn conf_change_v2_empty_buf() {
  assert!(dx::<ConfChangeV2::<u64>>(&[]).is_err());
}

#[test]
fn conf_change_v2_bad_transition_discriminant() {
  assert!(dx::<ConfChangeV2::<u64>>(&[99u8]).is_err());
}
