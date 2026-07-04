use super::*;
use std::collections::BTreeSet;

/// `leader_of` must anchor on the HIGHEST-term leader claim. A deposed leader cut off from the
/// group keeps believing itself leader at its stale term (at etcd-parity defaults nothing sends
/// it the deposing higher-term response), and a first-match scan in id order would let that
/// zombie shadow the live quorum's real leader for every consumer that targets "the" leader.
#[test]
fn leader_of_prefers_the_highest_term_leader() {
  let mut w = MultiWorld::new(2); // seed 2 deterministically elects node 0 first
  for n in 0..3 {
    w.add_node(n);
  }
  let all: BTreeSet<u64> = (0..3).collect();
  w.create_group(100, &all);
  assert!(w.run_until(600, |w| w.leader_of(100).is_some()));
  let old = w.leader_of(100).expect("elected");
  let old_term = w.hosts[&old].group(&100).expect("hosted").term();

  // Cut the leader off entirely: it keeps its Leader role at the old term while the survivors
  // elect a successor at a higher one.
  w.isolated.insert(old);
  let successor_role = |w: &MultiWorld| -> Option<u64> {
    w.node_ids
      .iter()
      .find(|&&n| {
        n != old
          && w.hosts[&n]
            .group(&100)
            .is_some_and(|ep| ep.role().is_leader())
      })
      .copied()
  };
  assert!(w.run_until(3_000, |w| successor_role(w).is_some()));
  let successor = successor_role(&w).expect("a survivor elected");
  let new_term = w.hosts[&successor].group(&100).expect("hosted").term();

  // Shape guards: the zombie still claims Leader at a strictly lower term AND sorts first in id
  // order, so a naive first-Leader scan would return it instead of the successor.
  assert!(
    w.hosts[&old]
      .group(&100)
      .is_some_and(|ep| ep.role().is_leader()),
    "the isolated ex-leader must still believe itself leader"
  );
  assert!(new_term > old_term, "the successor rules at a higher term");
  assert!(
    old < successor,
    "the zombie must shadow the successor in id order"
  );

  assert_eq!(
    w.leader_of(100),
    Some(successor),
    "leader_of must return the highest-term leader, not the first Leader role in id order"
  );
}

#[test]
fn two_groups_elect_and_commit_independently() {
  let mut w = MultiWorld::new(7);
  for n in 0..3 {
    w.add_node(n);
  }
  let all: BTreeSet<u64> = (0..3).collect();
  w.create_group(100, &all);
  w.create_group(200, &all);
  assert!(w.run_until(400, |w| w.leader_of(100).is_some()
    && w.leader_of(200).is_some()));
  assert!(w.propose(100, b"a-100").is_some());
  assert!(w.propose(200, b"a-200").is_some());
  assert!(w.run_until(400, |w| {
    w.agreement_holds(100)
      && w.agreement_holds(200)
      && (0..3).all(|n| !w.applied_of(n, 100).is_empty() && !w.applied_of(n, 200).is_empty())
  }));
  // Independence witness: the two groups elected on their own jitter (seeded per group).
  assert!(
    w.applied_of(0, 100)
      .iter()
      .all(|(_, c)| c.ends_with(b"-100"))
  );
}
