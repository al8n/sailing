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

/// Propose `ty(node)` on `gid`, ticking through transient refusals (no leader this instant, the
/// leader's own-term commit gate) until a leader accepts it.
fn propose_conf_change_until_accepted(
  w: &mut MultiWorld,
  gid: u64,
  ty: sailing_proto::ConfChangeType,
  node: u64,
) {
  for _ in 0..2_000 {
    let cc = sailing_proto::ConfChange::new(ty, node, bytes::Bytes::new());
    if w.propose_conf_change(gid, cc).is_some() {
      return;
    }
    w.tick();
  }
  panic!("conf-change {ty:?}({node}) was never accepted");
}

/// Drive group 100 to a committed post-genesis config, then FABRICATE the cheapest corrupt
/// membership observation the checker records: a snapshot install claiming the CURRENT
/// (post-removal) membership at a boundary where the committed config was still the genesis —
/// exactly the phantom/missing-voter ConfState a corrupted snapshot would carry.
fn world_with_corrupt_install() -> MultiWorld {
  let mut w = MultiWorld::new(2);
  for n in 0..3 {
    w.add_node(n);
  }
  let all: BTreeSet<u64> = (0..3).collect();
  w.create_group(100, &all);
  assert!(w.run_until(600, |w| w.leader_of(100).is_some()));
  propose_conf_change_until_accepted(&mut w, 100, sailing_proto::ConfChangeType::RemoveNode, 2);
  assert!(
    w.run_until(2_000, |w| {
      [0u64, 1].iter().all(|n| {
        w.hosts[n]
          .group(&100)
          .is_some_and(|ep| !ep.conf_state().voters().contains(&2))
      })
    }),
    "the removal never applied on the survivors"
  );
  let post_removal =
    checker::ConfSnapshot::from_conf_state(&w.hosts[&0].group(&100).expect("hosted").conf_state());
  // Boundary 1 predates the conf-change entry, so the committed config in effect there is the
  // genesis {0,1,2}; an install claiming {0,1} at that boundary is a corrupt ConfState.
  w.pending_new_installs
    .entry(100)
    .or_default()
    .push((1, 1, post_removal));
  w
}

/// The per-tick suite only RECORDS membership observations (`check_or_panic` defers the
/// verdict), so a corrupt snapshot install must TRIP at the run-end finalize pass — here on a
/// LIVE group's checker.
#[test]
#[should_panic(expected = "snapshot_membership_coherent")]
fn finalize_membership_trips_a_corrupt_install_on_a_live_group() {
  let mut w = world_with_corrupt_install();
  // Record-only: the per-tick pass folds the corrupt observation without judging it.
  w.check_now();
  w.finalize_membership_or_panic(2);
}

/// Retirement archives a checker after one more RECORD-ONLY check, so a corrupt install on a
/// since-removed group must still face the run-end verdict through the retired archive.
#[test]
#[should_panic(expected = "snapshot_membership_coherent")]
fn finalize_membership_trips_a_corrupt_install_archived_by_retirement() {
  let mut w = world_with_corrupt_install();
  // The at-removal check folds the pending corrupt observation, then freezes the checker into
  // the (gid, generation) archive.
  w.remove_group(100);
  assert!(w.checkers.is_empty());
  assert_eq!(w.retired.len(), 1);
  w.finalize_membership_or_panic(2);
}

/// Replica `(node, gid)`'s active `(voters, learners)` pair, for membership asserts.
fn conf_of(w: &MultiWorld, node: u64, gid: u64) -> (BTreeSet<u64>, BTreeSet<u64>) {
  let cs = w.hosts[&node].group(&gid).expect("hosted").conf_state();
  (
    cs.voters().iter().copied().collect(),
    cs.learners().iter().copied().collect(),
  )
}

/// A PARKED stale ex-leader must never be the AUTHORITATIVE committed-config source: with the
/// live leader crashed (no unparked leader anywhere), the committed voter/learner sets must
/// derive from the unparked replicas — not from the zombie still wearing Leader role at its
/// stale term, whose frozen config predates the group's committed changes. The parked exclusion
/// is the same rule `leader_of` and `group_leader_count` already apply.
#[test]
fn parked_stale_leader_never_defines_committed_membership() {
  let mut w = MultiWorld::new(2); // seed 2 deterministically elects node 0 first
  for n in 0..4 {
    w.add_node(n);
  }
  let voters: BTreeSet<u64> = (0..3).collect();
  w.create_group(100, &voters);
  assert!(w.run_until(600, |w| w.leader_of(100) == Some(0)));

  // Cut the leader off: it freezes in Leader role at its stale term with the founding config
  // while the survivors elect a successor at a higher term…
  w.isolate(0);
  assert!(w.run_until(3_000, |w| matches!(w.leader_of(100), Some(l) if l != 0)));

  // …who commits membership the zombie never learns: learner 3 joins, voter 0 is removed.
  w.wire_group_observer(100, 3);
  propose_conf_change_until_accepted(
    &mut w,
    100,
    sailing_proto::ConfChangeType::AddLearnerNode,
    3,
  );
  propose_conf_change_until_accepted(&mut w, 100, sailing_proto::ConfChangeType::RemoveNode, 0);
  let live_conf = (
    [1u64, 2].into_iter().collect::<BTreeSet<u64>>(),
    [3u64].into_iter().collect::<BTreeSet<u64>>(),
  );
  assert!(
    w.run_until(4_000, |w| [1u64, 2, 3]
      .iter()
      .all(|&n| conf_of(w, n, 100) == live_conf)),
    "the committed changes never applied on the live members"
  );

  // Park the ignorant victim (the departed sweep's verdict, applied directly), then crash the
  // live leader so no UNPARKED replica claims leadership at the observation point.
  w.parked.insert((0, 100));
  let live_leader = w.leader_of(100).expect("the live side has a leader");
  w.crash(live_leader);

  // Shape guards: the zombie is a parked Leader whose config is the stale founding one.
  assert!(
    w.hosts[&0]
      .group(&100)
      .is_some_and(|ep| ep.role().is_leader()),
    "the victim must still believe itself leader"
  );
  assert_eq!(w.group_leader_count(100), 0, "no unparked leader remains");
  assert_eq!(conf_of(&w, 0, 100), (voters, BTreeSet::new()));

  assert_eq!(
    w.committed_voters_of(100),
    live_conf.0,
    "a parked stale leader must not be the authoritative committed-voter source"
  );
  assert_eq!(
    w.committed_learners_of(100),
    live_conf.1,
    "a parked stale leader must not be the authoritative committed-learner source"
  );
}

/// The leaderless PLURALITY fallback must not count parked replicas either: a parked stale
/// config that ties the live one in the tally wins the deterministic tie-break (it sorts
/// first), so one zombie vote is enough to hand the checker a committed set anchored on
/// removed members — the committed sets must derive only from unparked replicas.
#[test]
fn parked_replicas_never_vote_in_the_leaderless_plurality() {
  let mut w = MultiWorld::new(2); // seed 2 deterministically elects node 0 first
  for n in 0..4 {
    w.add_node(n);
  }
  let voters: BTreeSet<u64> = (0..3).collect();
  w.create_group(100, &voters);
  assert!(w.run_until(600, |w| w.leader_of(100) == Some(0)));

  // Move leadership off the future victim so it freezes as a NON-leader (the tally path is
  // only reached when the authoritative-leader scan finds nothing).
  let mut moved = false;
  for _ in 0..2_000 {
    if w.leader_of(100) == Some(1) {
      moved = true;
      break;
    }
    w.transfer_group_leader(100, 1);
    w.tick();
  }
  assert!(moved, "leadership never moved to node 1");

  // Commit learner 3 while node 0 still applies it, then cut node 0 off: its frozen view is
  // voters {0,1,2} + learner {3}, diverging from everything committed afterwards.
  w.wire_group_observer(100, 3);
  propose_conf_change_until_accepted(
    &mut w,
    100,
    sailing_proto::ConfChangeType::AddLearnerNode,
    3,
  );
  assert!(
    w.run_until(2_000, |w| [0u64, 1, 2]
      .iter()
      .all(|&n| conf_of(w, n, 100).1.contains(&3))),
    "the learner add never applied on the voters"
  );
  w.isolate(0);

  // Shrink the group to the single voter {1}: remove the frozen victim, the learner, and voter
  // 2 (each self-removes-and-parks when its farewell lands; the embedder tears the replica
  // down). The victim's removal never reaches it — the ignorant-victim shape.
  propose_conf_change_until_accepted(&mut w, 100, sailing_proto::ConfChangeType::RemoveNode, 0);
  propose_conf_change_until_accepted(&mut w, 100, sailing_proto::ConfChangeType::RemoveNode, 3);
  propose_conf_change_until_accepted(&mut w, 100, sailing_proto::ConfChangeType::RemoveNode, 2);
  assert!(
    w.run_until(4_000, |w| conf_of(w, 1, 100)
      == ([1u64].into_iter().collect(), BTreeSet::new())),
    "the shrink to a single voter never applied on node 1"
  );
  w.drop_group_replica(100, 3);
  w.drop_group_replica(100, 2);
  w.parked.insert((0, 100));

  // Crash the last live voter: leaderless, so both accessors fall to the plurality tally over
  // the two hosting replicas — the parked stale one and the crashed-but-live one.
  w.crash(1);
  assert_eq!(w.group_leader_count(100), 0, "no unparked leader remains");
  assert!(
    w.hosts[&0]
      .group(&100)
      .is_some_and(|ep| !ep.role().is_leader()),
    "the parked victim must not hold Leader role (the tally path must be the one judged)"
  );
  assert_eq!(
    conf_of(&w, 0, 100),
    (voters, [3u64].into_iter().collect::<BTreeSet<u64>>()),
    "the victim's frozen view must carry the stale voters + learner"
  );

  assert_eq!(
    w.committed_voters_of(100),
    [1u64].into_iter().collect::<BTreeSet<u64>>(),
    "a parked stale config must not vote in the committed-voter plurality"
  );
  assert_eq!(
    w.committed_learners_of(100),
    BTreeSet::new(),
    "a parked stale config must not vote in the committed-learner plurality"
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
