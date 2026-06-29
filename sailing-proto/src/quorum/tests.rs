use super::*;

// Helper: build MajorityConfig from a slice of IDs.
fn mc(ids: &[u64]) -> MajorityConfig<u64> {
  MajorityConfig::new(ids.iter().copied().collect())
}

// Helper: build JointConfig with explicit incoming/outgoing.
fn jc(incoming: &[u64], outgoing: &[u64]) -> JointConfig<u64> {
  JointConfig::new(mc(incoming), mc(outgoing))
}

#[test]
fn vote_result_as_str_and_display() {
  for (r, name) in [
    (VoteResult::Won, "won"),
    (VoteResult::Lost, "lost"),
    (VoteResult::Pending, "pending"),
  ] {
    assert_eq!(r.as_str(), name);
    assert_eq!(std::format!("{r}"), name);
  }
}

#[test]
fn majority_len_and_is_empty() {
  assert_eq!(mc(&[]).len(), 0);
  assert!(mc(&[]).is_empty());
  assert_eq!(mc(&[1, 2, 3]).len(), 3);
  assert!(!mc(&[1, 2, 3]).is_empty());
}

#[test]
fn majority_committed_empty_returns_max() {
  let c = mc(&[]);
  assert_eq!(c.committed_index(|_| Index::new(0)), Index::new(u64::MAX));
}

#[test]
fn majority_committed_single_member() {
  // n=1, pos=0: committed = that member's ack.
  let c = mc(&[1]);
  assert_eq!(
    c.committed_index(|id| Index::new(if id == 1 { 7 } else { 0 })),
    Index::new(7)
  );
}

#[test]
fn majority_committed_three_members() {
  // n=3, pos=1 (median): sorted [10,12,14] → srt[1] = 12.
  let c = mc(&[1, 2, 3]);
  let acked = |id| match id {
    1 => Index::new(10),
    2 => Index::new(12),
    3 => Index::new(14),
    _ => Index::ZERO,
  };
  assert_eq!(c.committed_index(acked), Index::new(12));
}

#[test]
fn majority_committed_five_members() {
  // n=5, pos=2: sorted [8,10,12,14,16] → srt[2] = 12.
  let c = mc(&[1, 2, 3, 4, 5]);
  let acked = |id| match id {
    1 => Index::new(10),
    2 => Index::new(12),
    3 => Index::new(14),
    4 => Index::new(16),
    5 => Index::new(8),
    _ => Index::ZERO,
  };
  assert_eq!(c.committed_index(acked), Index::new(12));
}

#[test]
fn majority_committed_two_members() {
  // n=2, pos=0: a majority of 2 is both, so the committed index is the LOWER ack. sorted [5,9] → srt[0]=5.
  let c = mc(&[1, 2]);
  let acked = |id| match id {
    1 => Index::new(9),
    2 => Index::new(5),
    _ => Index::ZERO,
  };
  assert_eq!(c.committed_index(acked), Index::new(5));
}

#[test]
fn majority_committed_four_members() {
  // n=4, pos=1: a majority of 4 is 3, so the committed index is the 3rd-largest. sorted [10,20,30,40] → 20.
  let c = mc(&[1, 2, 3, 4]);
  let acked = |id| match id {
    1 => Index::new(30),
    2 => Index::new(10),
    3 => Index::new(40),
    4 => Index::new(20),
    _ => Index::ZERO,
  };
  assert_eq!(c.committed_index(acked), Index::new(20));
}

#[test]
fn majority_committed_large_cluster_uses_vec_fallback() {
  // n=17 > STACK_CAP exercises the Vec branch. pos=17-(8+1)=8; acks 1..=17 → sorted srt[8]=9.
  let ids: std::vec::Vec<u64> = (1..=17).collect();
  let c = mc(&ids);
  assert_eq!(c.committed_index(Index::new), Index::new(9));
}

#[test]
fn majority_committed_zero_drags_down() {
  // n=3, pos=1: sorted [0,12,14] → srt[1] = 12.
  // Member 1 hasn't acked (returns 0), so sorted is [0,12,14] and pos=1 gives 12.
  let c = mc(&[1, 2, 3]);
  let acked = |id| match id {
    1 => Index::ZERO,
    2 => Index::new(12),
    3 => Index::new(14),
    _ => Index::ZERO,
  };
  assert_eq!(c.committed_index(acked), Index::new(12));
}

#[test]
fn majority_committed_all_zero() {
  // n=3, pos=1: sorted [0,0,0] → srt[1] = 0.
  let c = mc(&[1, 2, 3]);
  assert_eq!(c.committed_index(|_| Index::ZERO), Index::ZERO);
}

#[test]
fn majority_vote_empty_wins() {
  let c = mc(&[]);
  assert_eq!(c.vote_result(|_| None), VoteResult::Won);
}

#[test]
fn majority_vote_won() {
  // n=3, q=2: 2 grants → Won.
  let c = mc(&[1, 2, 3]);
  let votes = |id| match id {
    1 => Some(true),
    2 => Some(true),
    3 => Some(false),
    _ => None,
  };
  assert_eq!(c.vote_result(votes), VoteResult::Won);
}

#[test]
fn majority_vote_lost() {
  // n=3, q=2: 2 rejections → Lost (grants=1, missing=0, grants+missing=1 < 2).
  let c = mc(&[1, 2, 3]);
  let votes = |id| match id {
    1 => Some(true),
    2 => Some(false),
    3 => Some(false),
    _ => None,
  };
  assert_eq!(c.vote_result(votes), VoteResult::Lost);
}

#[test]
fn majority_vote_pending() {
  // n=3, q=2: grants=1, missing=1 → grants+missing=2 >= q → Pending.
  let c = mc(&[1, 2, 3]);
  let votes = |id| match id {
    1 => Some(true),
    2 => None,
    3 => Some(false),
    _ => None,
  };
  assert_eq!(c.vote_result(votes), VoteResult::Pending);
}

#[test]
fn majority_vote_all_missing() {
  // n=3, q=2: grants=0, missing=3 → Pending.
  let c = mc(&[1, 2, 3]);
  assert_eq!(c.vote_result(|_| None), VoteResult::Pending);
}

#[test]
fn joint_committed_non_joint_equals_incoming() {
  // outgoing is empty → sentinel u64::MAX; min(incoming, MAX) = incoming.
  let c = JointConfig::from_voters([1u64, 2, 3].iter().copied().collect());
  let acked = |id| match id {
    1 => Index::new(10),
    2 => Index::new(12),
    3 => Index::new(14),
    _ => Index::ZERO,
  };
  assert_eq!(c.committed_index(acked), Index::new(12));
}

#[test]
fn joint_committed_takes_min_of_halves() {
  // incoming {1,2,3} acked {1→10,2→15,3→13} → sorted [10,13,15], pos=1 → 13.
  // outgoing {3,4,5} acked {3→13,4→11,5→12} → sorted [11,12,13], pos=1 → 12.
  // Joint: min(13,12) = 12.
  let c = jc(&[1, 2, 3], &[3, 4, 5]);
  let acked = |id| match id {
    1 => Index::new(10),
    2 => Index::new(15),
    3 => Index::new(13),
    4 => Index::new(11),
    5 => Index::new(12),
    _ => Index::ZERO,
  };
  assert_eq!(c.committed_index(acked), Index::new(12));
}

#[test]
fn joint_committed_classic_etcd_case() {
  // incoming {1,2,3} with acks that give 13, outgoing {4,5,6} that give 11 → joint 11.
  // incoming sorted [11,13,15], pos=1 → 13.
  // outgoing sorted [10,11,12], pos=1 → 11.
  let c = jc(&[1, 2, 3], &[4, 5, 6]);
  let acked = |id| match id {
    1 => Index::new(11),
    2 => Index::new(13),
    3 => Index::new(15),
    4 => Index::new(10),
    5 => Index::new(11),
    6 => Index::new(12),
    _ => Index::ZERO,
  };
  assert_eq!(c.committed_index(acked), Index::new(11));
}

#[test]
fn joint_vote_both_won() {
  let c = jc(&[1, 2, 3], &[4, 5, 6]);
  // incoming: 1,2 grant (Won); outgoing: 4,5 grant (Won).
  let votes = |id| match id {
    1 | 2 => Some(true),
    3 => Some(false),
    4 | 5 => Some(true),
    6 => Some(false),
    _ => None,
  };
  assert_eq!(c.vote_result(votes), VoteResult::Won);
}

#[test]
fn joint_vote_one_half_lost() {
  let c = jc(&[1, 2, 3], &[4, 5, 6]);
  // incoming: 1 grant, 2,3 reject → Lost; outgoing: 4,5 grant → Won.
  let votes = |id| match id {
    1 => Some(true),
    2 | 3 => Some(false),
    4 | 5 => Some(true),
    6 => Some(false),
    _ => None,
  };
  assert_eq!(c.vote_result(votes), VoteResult::Lost);
}

#[test]
fn joint_vote_one_won_one_pending() {
  let c = jc(&[1, 2, 3], &[4, 5, 6]);
  // incoming: 1,2 grant → Won; outgoing: 4 grants, 5 missing → Pending.
  let votes = |id| match id {
    1 | 2 => Some(true),
    3 => Some(false),
    4 => Some(true),
    5 => None,
    6 => Some(false),
    _ => None,
  };
  assert_eq!(c.vote_result(votes), VoteResult::Pending);
}

#[test]
fn joint_vote_non_joint_behaves_as_single() {
  // outgoing empty → Won vacuously; result is the incoming result.
  let c = JointConfig::from_voters([1u64, 2, 3].iter().copied().collect());
  let votes = |id| match id {
    1 | 2 => Some(true),
    3 => Some(false),
    _ => None,
  };
  assert_eq!(c.vote_result(votes), VoteResult::Won);
}
