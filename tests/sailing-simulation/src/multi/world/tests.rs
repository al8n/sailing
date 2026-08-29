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
    .entry((100, 0))
    .or_default()
    .push((1, 1, post_removal));
  w
}

/// A world holding TWO live incarnations of one id: the successor the registry knows about, and a
/// lone replica still bound to the incarnation before it — the island shape. The binding is
/// declared directly because the organic path needs a recreation that EXCLUDES the island's host,
/// which the world's registry-driven recreate cannot express; everything downstream of the binding
/// (routing, views, judging) is the real machinery.
fn world_with_two_live_incarnations() -> (MultiWorld, (u64, u64, checker::ConfSnapshot)) {
  let mut w = MultiWorld::new(2);
  for n in 0..3 {
    w.add_node(n);
  }
  let all: BTreeSet<u64> = (0..3).collect();
  w.create_group(100, &all);
  assert!(w.run_until(600, |w| w.leader_of(100).is_some()));
  // The corrupt observation, built exactly as [`world_with_corrupt_install`] builds its own:
  // commit a removal, then claim the POST-removal config at a boundary that predates the entry
  // carrying it. Captured BEFORE the ceremony below, because once the replicas are rebound the
  // delivery fence stops their frames and nothing can commit.
  propose_conf_change_until_accepted(&mut w, 100, sailing_proto::ConfChangeType::RemoveNode, 1);
  assert!(
    w.run_until(2_000, |w| {
      w.hosts[&0]
        .group(&100)
        .is_some_and(|ep| !ep.conf_state().voters().contains(&1))
    }),
    "the removal never applied"
  );
  let obs = (
    1,
    1,
    checker::ConfSnapshot::from_conf_state(&w.hosts[&0].group(&100).expect("hosted").conf_state()),
  );

  assert!(w.remove_group(100));
  w.recreate_group(100);
  assert!(w.run_until(600, |w| w.leader_of(100).is_some()));
  assert_eq!(w.generation_of(100), 1);
  // Every replica still speaks for the incarnation BEFORE the recreation — the shape the island
  // ceremony leaves behind, where the successor exists in the registry and is hosted nowhere yet.
  // Whole-set rather than one node because an incarnation hosting less than a quorum trips the
  // durability axiom on its own, which would mask the routing property under test.
  for n in 0..3 {
    w.replica_gen.insert((n, 100), 0);
  }
  w.checkers.entry((100, 0)).or_default();
  (w, obs)
}

/// THE STRIKE IS PER `(node, cause)`, NOT PER ID. A group can be fence-coupled on two nodes for
/// two different reasons — a held fork for a tombstoned child on one, an ordinary fence with no
/// tombstone behind it on the other. Cancelling the whole id would hand a clean bill to a wedge
/// only half of which is attributable, so the counterfactual strikes individual edges and the
/// independent one keeps the group a root.
///
/// SCOPE: this pins the strike SET, which is what the granularity change owns — the closure
/// recomputation over the surviving roots is exercised by the band runs. A fully end-to-end
/// dual-cause fixture is not constructible in this harness: no organic shape here leaves a
/// `pending_merge` park standing on two replicas, and fabricating one needs the committed
/// `CommitMerge` encoder, which is crate-private to `sailing-proto`.
#[test]
fn the_held_fork_strike_names_edges_not_whole_ids() {
  let mut w = MultiWorld::new(5);
  for n in 0..3 {
    w.add_node(n);
  }
  let all: BTreeSet<u64> = (0..3).collect();
  w.create_group(100, &all);
  assert!(w.run_until(2_000, |w| w.leader_of(100).is_some()));

  // Two nodes record a fence naming the same child; only ONE of them has that child tombstoned.
  for n in [0u64, 1] {
    w.fork_conflicts
      .entry((n, 100))
      .or_default()
      .insert(sailing_proto::Index::new(1), 9_999);
  }
  w.host_tombstones.insert((0, 9_999));

  assert!(
    w.retired_hold_on(0, 100),
    "node 0's fence is explained by a held fork for a tombstoned child"
  );
  assert!(
    !w.retired_hold_on(1, 100),
    "node 1's is not — same id, different cause"
  );

  let edges = w.held_fork_fence_edges(&[100u64].into_iter().collect());
  assert!(
    edges.contains(&(0, 100)),
    "the explained edge is struck: {edges:?}"
  );
  assert!(
    !edges.contains(&(1, 100)),
    "and the independent one is NOT — an id-level strike would have taken both, cancelling a \
     root no held fork explains: {edges:?}"
  );
  assert!(
    !edges.contains(&(2, 100)),
    "nor an uninvolved node's: {edges:?}"
  );
}

/// OBSERVATION ROUTING, SUCCESSOR SIDE. Two incarnations are judged by two checkers, so the
/// observations that feed them must be keyed by incarnation too. A gid-keyed queue is drained by
/// whichever checker the loop reaches FIRST — BTree order, so the oldest — and clears it; every
/// later incarnation is then starved, and a corrupt install on the successor is judged against the
/// island's history or vanishes entirely. It must trip in the SUCCESSOR's checker, named as such.
#[test]
#[should_panic(expected = "group=100 gen=1")]
fn a_corrupt_successor_install_trips_the_successors_own_checker() {
  let (mut w, obs) = world_with_two_live_incarnations();
  w.pending_new_installs
    .entry((100, 1))
    .or_default()
    .push(obs);
  w.check_now();
  w.finalize_membership_or_panic(2);
}

/// OBSERVATION ROUTING, ISLAND SIDE — the control. The same corrupt install recorded against the
/// OLDER incarnation trips under generation 0, proving the routing is directional rather than one
/// checker quietly swallowing both queues.
#[test]
#[should_panic(expected = "group=100 gen=0")]
fn a_corrupt_island_install_trips_under_its_own_incarnation() {
  let (mut w, obs) = world_with_two_live_incarnations();
  w.pending_new_installs
    .entry((100, 0))
    .or_default()
    .push(obs);
  w.check_now();
  w.finalize_membership_or_panic(2);
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

/// Build the accounting-gap shape: a RECORDED install observation whose boundary lies beyond
/// the committed-config history's completeness watermark, so `checker::finalize_membership`
/// returns `Ok` while counting the install skipped-unwitnessed — never compared. Only the
/// finalize pass's accounting leg stands between this and a run that ends without a membership
/// verdict for the install.
fn world_with_unjudgeable_install() -> MultiWorld {
  let mut w = MultiWorld::new(2);
  for n in 0..3 {
    w.add_node(n);
  }
  let all: BTreeSet<u64> = (0..3).collect();
  w.create_group(100, &all);
  assert!(w.run_until(600, |w| w.leader_of(100).is_some()));
  let conf =
    checker::ConfSnapshot::from_conf_state(&w.hosts[&0].group(&100).expect("hosted").conf_state());
  // No log-built replica is anywhere near applied 1_000_000, so the history is never certified
  // at that boundary and the observation can never be judged.
  w.pending_new_installs
    .entry((100, 0))
    .or_default()
    .push((1, 1_000_000, conf));
  w
}

/// `finalize_membership` returns `Ok` for an install it could not judge; the multi run's
/// finalize policy must still refuse the run — on a LIVE group, with the gid attributed.
#[test]
fn finalize_membership_flags_an_unjudged_install_on_a_live_group() {
  let mut w = world_with_unjudgeable_install();
  // Record-only: the per-tick pass folds the observation without judging it.
  w.check_now();
  let msg = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
    w.finalize_membership_or_panic(2)
  }))
  .expect_err("an unjudged install must fail the run")
  .downcast::<String>()
  .map(|s| *s)
  .unwrap_or_default();
  assert!(msg.contains("MEMBERSHIP ACCOUNTING FAILURE"), "{msg}");
  assert!(msg.contains("group=100 gen=0"), "{msg}");
}

/// The frozen-history case: the group is REMOVED first, so the archived checker's history can
/// never advance to cover the boundary. The accounting policy must trip through the retired
/// leg, attributing gid AND generation.
#[test]
fn finalize_membership_flags_an_unjudged_install_archived_by_retirement() {
  let mut w = world_with_unjudgeable_install();
  // The at-removal check folds the pending observation, then freezes the checker into the
  // (gid, generation) archive.
  w.remove_group(100);
  assert!(w.checkers.is_empty());
  assert_eq!(w.retired.len(), 1);
  let msg = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
    w.finalize_membership_or_panic(2)
  }))
  .expect_err("an unjudged install on a retired group must fail the run")
  .downcast::<String>()
  .map(|s| *s)
  .unwrap_or_default();
  assert!(msg.contains("MEMBERSHIP ACCOUNTING FAILURE"), "{msg}");
  assert!(msg.contains("retired group"), "{msg}");
  assert!(msg.contains("group=100 gen=0"), "{msg}");
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
    w.committed_voters_of(100, w.generation_of(100)),
    live_conf.0,
    "a parked stale leader must not be the authoritative committed-voter source"
  );
  assert_eq!(
    w.committed_learners_of(100, w.generation_of(100)),
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
    w.committed_voters_of(100, w.generation_of(100)),
    [1u64].into_iter().collect::<BTreeSet<u64>>(),
    "a parked stale config must not vote in the committed-voter plurality"
  );
  assert_eq!(
    w.committed_learners_of(100, w.generation_of(100)),
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

/// Propose `payload` on `gid`, ticking through transient leaderless windows until accepted.
fn propose_until_accepted(w: &mut MultiWorld, gid: u64, payload: &[u8]) {
  for _ in 0..2_000 {
    if w.propose(gid, payload).is_some() {
      return;
    }
    w.tick();
  }
  panic!("proposal on g{gid} was never accepted");
}

/// Drive g100 (3 voters, one write per key 0..8) through a split at point 4 into child `child`,
/// to the point where the child is registered and elected. Returns the world.
fn world_after_split(seed: u64, child: u64) -> MultiWorld {
  let mut w = MultiWorld::new(seed);
  for n in 0..3 {
    w.add_node(n);
  }
  let all: BTreeSet<u64> = (0..3).collect();
  w.create_group(100, &all);
  assert!(w.run_until(3_000, |w| w.leader_of(100).is_some()));
  w.reconcile_membership(100);
  for key in 0u16..8 {
    let payload = crate::multi::encode_gkv(100, key, u64::from(key));
    propose_until_accepted(&mut w, 100, &payload);
  }
  assert!(
    w.run_until(2_000, |w| {
      let leader = w.leader_of(100);
      leader.is_some_and(|l| w.applied_of(l, 100).len() >= 8)
    }),
    "the keyed baseline never applied"
  );

  let mut accepted = false;
  for _ in 0..2_000 {
    match w.propose_split(100, child, 4) {
      Some(Ok(_)) => {
        accepted = true;
        break;
      }
      _ => {
        w.tick();
      }
    }
  }
  assert!(accepted, "the split was never accepted");
  // The population flips AT PROPOSE: moved keys are parked before the entry even commits.
  assert_eq!(w.group_keys_of(100), std::vec![0, 1, 2, 3]);

  assert!(
    w.run_until(3_000, |w| w.leader_of(child).is_some()),
    "the forked child never elected: {}",
    w.dbg_group(child)
  );
  w
}

/// The split lifecycle end-to-end in the world: propose → fork pump materialization on every
/// parent voter → child registration (population, checker, catalog) → child election → fresh
/// keyed load on BOTH sides — with the handover visible in the child's applied baseline and
/// the conservation verdict green over independently recorded histories.
#[test]
fn split_reshapes_the_world_end_to_end() {
  let mut w = world_after_split(9, 200);

  assert_eq!(w.splits_applied(), 1);
  assert_eq!(w.group_keys_of(200), std::vec![4, 5, 6, 7]);
  assert_eq!(
    w.hosting_nodes(200),
    std::vec![0, 1, 2],
    "the child bootstraps colocated on the parent's voters"
  );
  assert_eq!(w.generation_of(200), 0, "a forked child starts at gen 0");

  // The handover is the child's OPENING record: the four moved cells, parent-tagged (the gkv
  // tag names the group that ACCEPTED the write), with their parent-log indices intact.
  let baseline: Vec<(u64, u16, u64)> = w
    .applied_of(0, 200)
    .iter()
    .filter_map(|(_, cmd)| crate::multi::decode_gkv(cmd))
    .collect();
  assert_eq!(
    baseline,
    std::vec![(100, 4, 4), (100, 5, 5), (100, 6, 6), (100, 7, 7)],
    "the fork baseline must carry exactly the moved keys' history"
  );

  // Both sides commit fresh keyed load post-split.
  propose_until_accepted(&mut w, 100, &crate::multi::encode_gkv(100, 0, 100));
  propose_until_accepted(&mut w, 200, &crate::multi::encode_gkv(200, 5, 101));
  assert!(
    w.run_until(2_000, |w| {
      (0..3).all(|n| {
        let parent_new = w
          .applied_of(n, 100)
          .iter()
          .any(|(_, c)| crate::multi::decode_gkv(c) == Some((100, 0, 100)));
        let child_new = w
          .applied_of(n, 200)
          .iter()
          .any(|(_, c)| crate::multi::decode_gkv(c) == Some((200, 5, 101)));
        parent_new && child_new
      })
    }),
    "post-split load must commit on both sides"
  );
  assert!(w.agreement_holds(100) && w.agreement_holds(200));

  assert_eq!(w.split_stale_observed(), 0);
  assert_eq!(w.split_conflicts_observed(), 0);
  w.check_now();
  w.finalize_conservation_or_panic(9);
  w.finalize_membership_or_panic(9);
}

/// Crash-replay idempotence: a crashed node restores BOTH sides from durable state, its parent
/// replica's restart replays the committed split entry and re-stages the fork, and the relay
/// folds it against the already-hosted child (the redundant-resolve arm) — never a second
/// registration, never a re-materialization over the child's real durable progress.
#[test]
fn split_replay_after_crash_registers_once() {
  let mut w = world_after_split(11, 200);
  assert_eq!(w.splits_applied(), 1);

  w.crash(0);
  for _ in 0..200 {
    w.tick();
  }
  assert_eq!(
    w.splits_applied(),
    1,
    "a replayed fork against the hosted child must fold, not re-register"
  );
  assert!(
    w.hosts_group(0, 200),
    "the crashed node restores its child replica"
  );

  // Both sides stay live and committing after the replay.
  assert!(w.run_until(3_000, |w| w.leader_of(100).is_some()
    && w.leader_of(200).is_some()));
  propose_until_accepted(&mut w, 100, &crate::multi::encode_gkv(100, 1, 200));
  propose_until_accepted(&mut w, 200, &crate::multi::encode_gkv(200, 6, 201));
  assert!(
    w.run_until(2_000, |w| {
      w.agreement_holds(100)
        && w.agreement_holds(200)
        && (0..3).all(|n| {
          w.applied_of(n, 200)
            .iter()
            .any(|(_, c)| crate::multi::decode_gkv(c) == Some((200, 6, 201)))
        })
    }),
    "post-replay load must commit on both sides"
  );
  w.finalize_conservation_or_panic(11);
  w.finalize_membership_or_panic(11);
}

/// Drive a 2-voter parent through a split whose fork materializes on the LEADER alone, then
/// retire the parent with the fork un-propagated: the child is left registered with committed
/// voters `{0, 1}` but hosted on one node — below quorum, leaderless, campaigning at its
/// manufactured baseline forever.
///
/// The strand needs a LATENCIED bus: on the zero-latency bus the settle loop is a fixpoint, so
/// the lagger's ack, the leader's commit advance, and the lagger's own apply all land within
/// one tick — ack and commit-learn can only be separated while the commit-carrying response is
/// still IN FLIGHT. With latency on, the leader applies (and forks) the tick the quorum ack
/// lands, and both directions of the parent link are muted before the advanced commit index
/// can be delivered (mutes swallow at delivery, so the in-flight response dies too; the
/// lagger's own higher-term campaigns — its log carries the split entry — must not reach the
/// leader either, or a handover would commit-and-fork on the lagger). Retirement then drops
/// the lagger's parent replica with the fork never staged. Returns `(world, holder, lagger)`.
fn world_with_stranded_child(seed: u64, child: u64) -> (MultiWorld, u64, u64) {
  let mut w = MultiWorld::new(seed);
  for n in 0..2 {
    w.add_node(n);
  }
  let voters: BTreeSet<u64> = (0..2).collect();
  w.create_group(100, &voters);
  assert!(w.run_until(3_000, |w| w.leader_of(100).is_some()));
  for key in 0u16..8 {
    let payload = crate::multi::encode_gkv(100, key, u64::from(key));
    propose_until_accepted(&mut w, 100, &payload);
  }
  assert!(
    w.run_until(2_000, |w| (0..2).all(|n| w.applied_of(n, 100).len() >= 8)),
    "the keyed baseline never applied everywhere"
  );
  let leader = w.leader_of(100).expect("elected");
  let lagger = 1 - leader;

  w.set_network_faults(
    crate::NetworkFaults {
      latency: Duration::from_millis(20),
      ..crate::NetworkFaults::none()
    },
    seed,
  );
  let mut accepted = false;
  for _ in 0..2_000 {
    if let Some(Ok(_)) = w.propose_split(100, child, 4) {
      accepted = true;
      break;
    }
    w.tick();
  }
  assert!(accepted, "the split was never accepted");
  for _ in 0..2_000 {
    w.tick();
    if w.splits_applied() == 1 {
      break;
    }
  }
  assert_eq!(w.splits_applied(), 1, "the split never materialized");
  assert_eq!(
    w.hosting_nodes(child),
    std::vec![leader],
    "the fork must land on the leader alone"
  );
  w.mute_group(leader, lagger, 100);
  w.mute_group(lagger, leader, 100);
  for _ in 0..50 {
    w.tick();
  }
  assert_eq!(
    w.hosting_nodes(child),
    std::vec![leader],
    "the starved lagger must never materialize the fork"
  );

  w.remove_group(100);
  w.unmute_all();
  w.set_network_faults(crate::NetworkFaults::none(), seed);
  assert_eq!(
    w.hosting_nodes(child),
    std::vec![leader],
    "retirement tears down the un-forked parent replica with the fork unstaged"
  );
  (w, leader, lagger)
}

/// The stranded fork child completes like the embedder would: the leaderless completion arm
/// wires the missing committed voter as an EMPTY catching-up OBSERVER (the product's
/// solicitation edge under the factory contract's fork-born rule — the blueprint excludes self
/// from the bootstrap voters), the holder's manufactured `(term 1, index 1)` log wins the ONLY
/// possible election (an observer empty grants votes but cannot campaign), the zero-progress
/// joiner arrives by SNAPSHOT carrying the inherited baseline plus the boundary config that
/// promotes it, and the conservation + membership verdicts hold. Without the arm this shape is
/// a permanent calm-window livelock: the only replica-creating repair (the resurrect arm) is
/// leader-gated, and a sub-quorum group can never produce the leader.
#[test]
fn stranded_fork_child_completes_from_the_holder() {
  let (mut w, holder, joiner) = world_with_stranded_child(13, 200);
  assert_eq!(w.group_voters(200), (0..2).collect::<BTreeSet<u64>>());
  assert!(w.leader_of(200).is_none(), "the wedge starts leaderless");

  // One reconcile pass completes the hosting set: the missing voter is wired EMPTY as an
  // OBSERVER (self absent from the bootstrap voters — the fork-born blueprint shape), able to
  // grant the holder's election but never to mount one of its own against the baseline.
  w.reconcile_membership(200);
  assert_eq!(w.hosting_nodes(200), std::vec![0, 1]);
  let (joiner_voters, _) = conf_of(&w, joiner, 200);
  assert!(
    !joiner_voters.contains(&joiner),
    "the completion arm wires the fork-born observer blueprint, never a self-voting empty"
  );
  assert!(
    w.applied_of(joiner, 200).is_empty(),
    "the wired replica is empty — the baseline must arrive by replication"
  );

  let mut elected = false;
  for _ in 0..4_000 {
    w.reconcile_membership(200);
    if w.leader_of(200).is_some() {
      elected = true;
      break;
    }
    w.tick();
  }
  assert!(
    elected,
    "the completed group must elect within the calm budget: {}",
    w.dbg_group(200)
  );
  assert_eq!(
    w.leader_of(200),
    Some(holder),
    "the holder's manufactured baseline wins the ONLY possible election — the observer empty \
     cannot campaign"
  );

  // The joiner's catch-up is structurally a snapshot transfer (the fork baseline pushed
  // first_index to 2), delivering the inherited parent-tagged record.
  assert!(
    w.run_until(4_000, |w| {
      !w.applied_of(holder, 200).is_empty()
        && w.applied_of(joiner, 200) == w.applied_of(holder, 200)
    }),
    "the joiner never converged on the holder's record: {}",
    w.dbg_group(200)
  );
  assert!(
    w.snapshot_lineage.contains(&(joiner, 200)),
    "a zero-progress joiner must arrive by snapshot"
  );
  assert!(
    w.applied_of(joiner, 200)
      .iter()
      .filter_map(|(_, c)| crate::multi::decode_gkv(c))
      .any(|(tag, _, _)| tag == 100),
    "the snapshot must deliver the inherited parent-tagged baseline"
  );
  let (joiner_voters, _) = conf_of(&w, joiner, 200);
  assert!(
    joiner_voters.contains(&joiner),
    "the snapshot's boundary config promotes the observer to voter"
  );
  assert!(w.agreement_holds(200));

  // Fresh keyed load commits on the completed group, and the run-end verdicts hold.
  propose_until_accepted(&mut w, 200, &crate::multi::encode_gkv(200, 5, 900));
  assert!(w.run_until(2_000, |w| (0..2).all(|n| {
    w.applied_of(n, 200)
      .iter()
      .any(|(_, c)| crate::multi::decode_gkv(c) == Some((200, 5, 900)))
  })));
  w.check_now();
  w.finalize_conservation_or_panic(13);
  w.finalize_membership_or_panic(13);
  assert_eq!(
    w.splits_applied(),
    1,
    "completion never re-registers the split"
  );
}

/// CRASH inside the baseline-transfer WINDOW: the stranded child's empty joiner is being caught
/// up by the holder's manufactured-baseline snapshot — the transfer is ADMITTED (the holder's
/// progress to the joiner is `Snapshot`, the install in flight on the latencied bus) — when the
/// joiner crash-restarts from its still-virgin durable stores. The crash devours the in-flight
/// install, so the redrive must RE-RUN the transfer to full adoption: token, inherited state,
/// commit at/past the manufactured baseline, no wedge (the holder exits `Snapshot`), no poison.
/// A second crash AFTER adoption then pins the durable half: the restart reconciles
/// lineage-first off the joiner's own baseline slot, so the token and record survive without any
/// re-transfer.
///
/// The narrower sub-window — blob durable but the install not yet run — is intra-settle-atomic
/// under the world's stores (a submitted blob completes and installs within one settle), so no
/// tick-boundary crash can land inside it; the endpoint tier owns that sub-window (the kin-slot
/// arm of the fork-provenance gate, which completes an interrupted adoption on the retransfer).
/// The world-reachable window pinned here is the ADMITTED, in-flight transfer.
#[test]
fn stranded_joiner_crash_inside_the_transfer_window_completes_on_redrive() {
  let (mut w, holder, joiner) = world_with_stranded_child(13, 200);
  // Latency keeps the install in flight across tick boundaries — on the zero-latency bus the
  // whole transfer settles within one tick and no boundary falls inside the window.
  w.set_network_faults(
    crate::NetworkFaults {
      latency: Duration::from_millis(20),
      ..crate::NetworkFaults::none()
    },
    13,
  );
  w.reconcile_membership(200);
  assert_eq!(w.hosting_nodes(200), std::vec![0, 1]);
  assert!(
    w.applied_of(joiner, 200).is_empty(),
    "the completion arm wires the joiner EMPTY"
  );

  // Run until the transfer is ADMITTED: the holder leads and its progress to the joiner is
  // `Snapshot` at a tick boundary, with the joiner still un-adopted.
  let mut admitted = false;
  for _ in 0..6_000 {
    w.reconcile_membership(200);
    let in_snapshot = w.hosts[&holder].group(&200).is_some_and(|ep| {
      ep.role().is_leader()
        && ep
          .peer_progress(&joiner)
          .is_some_and(|p| matches!(p.state, sailing_proto::ProgressState::Snapshot { .. }))
    });
    if in_snapshot
      && w.hosts[&joiner]
        .group(&200)
        .is_some_and(|ep| ep.fork_id().is_none())
    {
      admitted = true;
      break;
    }
    w.tick();
  }
  assert!(
    admitted,
    "the transfer was never observed admitted: {}",
    w.dbg_group(200)
  );

  // Crash the joiner INSIDE the window: durable stores are still virgin, the in-flight install
  // dies with the bus purge, and the restart re-wires an empty replica.
  w.crash(joiner);
  assert!(
    w.applied_of(joiner, 200).is_empty(),
    "an interrupted transfer leaves no partial state"
  );
  assert!(
    w.hosts[&joiner]
      .group(&200)
      .is_some_and(|ep| ep.fork_id().is_none()),
    "no token before the durable adoption"
  );

  // The redrive: the holder re-sends and the transfer completes to full adoption.
  assert!(
    w.run_until(6_000, |w| {
      w.hosts[&joiner]
        .group(&200)
        .is_some_and(|ep| ep.fork_id().is_some())
        && !w.applied_of(joiner, 200).is_empty()
        && w.applied_of(joiner, 200) == w.applied_of(holder, 200)
    }),
    "the joiner never adopted after the crash: {}",
    w.dbg_group(200)
  );
  assert!(
    w.hosts[&joiner]
      .group(&200)
      .is_some_and(|ep| ep.commit_index() >= sailing_proto::FORK_BASE_INDEX),
    "the adopted commit sits at/past the manufactured baseline"
  );
  assert!(
    w.snapshot_lineage.contains(&(joiner, 200)),
    "the adoption arrived by snapshot transfer"
  );
  // No wedge: the holder exited `Snapshot` toward the joiner.
  assert!(
    w.run_until(2_000, |w| {
      w.hosts[&holder].group(&200).is_some_and(|ep| {
        ep.peer_progress(&joiner)
          .is_none_or(|p| !matches!(p.state, sailing_proto::ProgressState::Snapshot { .. }))
      })
    }),
    "the transfer never terminated: {}",
    w.dbg_group(200)
  );
  assert!(w.poisoned_nodes().is_empty(), "no poison anywhere");

  // The durable half: a crash AFTER adoption restores the lineage from the joiner's own baseline
  // slot — no re-transfer needed for the token to survive.
  w.crash(joiner);
  assert!(
    w.hosts[&joiner]
      .group(&200)
      .is_some_and(|ep| ep.fork_id().is_some()),
    "the restart reconciles lineage-first off the durable baseline"
  );
  assert!(
    w.run_until(4_000, |w| {
      !w.applied_of(holder, 200).is_empty()
        && w.applied_of(joiner, 200) == w.applied_of(holder, 200)
    }),
    "the restarted adopter never re-converged: {}",
    w.dbg_group(200)
  );
  assert!(w.agreement_holds(200));

  // Fresh keyed load commits on the completed group, and the run-end verdicts hold.
  propose_until_accepted(&mut w, 200, &crate::multi::encode_gkv(200, 6, 901));
  assert!(w.run_until(2_000, |w| (0..2).all(|n| {
    w.applied_of(n, 200)
      .iter()
      .any(|(_, c)| crate::multi::decode_gkv(c) == Some((200, 6, 901)))
  })));
  w.check_now();
  w.finalize_conservation_or_panic(13);
  w.finalize_membership_or_panic(13);
}

/// With a LIVE leader the completion arm is out of the path entirely: an under-hosted child
/// that can elect (two of three voters materialized) heals its missing voter through the
/// standing leader-gated resurrect arm — a catching-up OBSERVER whose bootstrap excludes
/// itself, the same shape the completion arm wires under the fork-born rule, reached through
/// the led branch of reconcile rather than the leaderless one. The lagger's own late fork then
/// folds against the already-hosted replica without a second registration.
#[test]
fn under_hosted_group_with_a_live_leader_heals_via_the_observer_arm() {
  let mut w = MultiWorld::new(19);
  for n in 0..3 {
    w.add_node(n);
  }
  let voters: BTreeSet<u64> = (0..3).collect();
  w.create_group(100, &voters);
  assert!(w.run_until(3_000, |w| w.leader_of(100).is_some()));
  for key in 0u16..8 {
    let payload = crate::multi::encode_gkv(100, key, u64::from(key));
    propose_until_accepted(&mut w, 100, &payload);
  }
  assert!(
    w.run_until(2_000, |w| (0..3).all(|n| w.applied_of(n, 100).len() >= 8)),
    "the keyed baseline never applied everywhere"
  );
  let leader = w.leader_of(100).expect("elected");
  let straggler = (0..3).rev().find(|&n| n != leader).expect("a follower");

  // Starve the straggler of the split entry entirely (muted both ways BEFORE the propose): the
  // other two voters carry the quorum, apply, and fork, so the child elects on 2 of 3 while
  // the straggler's parent replica never stages its fork.
  w.mute_group(leader, straggler, 100);
  w.mute_group(straggler, leader, 100);
  let mut accepted = false;
  for _ in 0..2_000 {
    if let Some(Ok(_)) = w.propose_split(100, 300, 4) {
      accepted = true;
      break;
    }
    w.tick();
  }
  assert!(accepted, "the split was never accepted");
  assert!(
    w.run_until(3_000, |w| w.hosting_nodes(300).len() == 2
      && w.leader_of(300).is_some()),
    "the two materialized voters must elect: {}",
    w.dbg_group(300)
  );
  assert_eq!(w.splits_applied(), 1, "one registration for the split");
  assert!(!w.hosts_group(straggler, 300));

  // The led reconcile path: the missing voter comes back as the resurrect arm's OBSERVER
  // (bootstrap excludes itself — it cannot campaign until the log teaches it its own
  // membership). The completion arm wires the same observer shape, but only ever from the
  // LEADERLESS branch; a led group never consults it.
  w.reconcile_membership(300);
  assert!(w.hosts_group(straggler, 300));
  let (wired_voters, _) = conf_of(&w, straggler, 300);
  assert!(
    !wired_voters.contains(&straggler),
    "a led group heals through the observer resurrect arm"
  );

  // Heal the link: the straggler's parent replica applies the split late and its fork folds
  // against the hosted replica — one registration for the run.
  w.unmute_all();
  assert!(
    w.run_until(4_000, |w| {
      !w.applied_of(leader, 300).is_empty()
        && (0..3).all(|n| w.applied_of(n, 300) == w.applied_of(leader, 300))
    }),
    "the group never fully converged: {}",
    w.dbg_group(300)
  );
  assert_eq!(
    w.splits_applied(),
    1,
    "the late fork must fold, not re-register"
  );
  assert!(w.agreement_holds(300));
  w.check_now();
  w.finalize_conservation_or_panic(19);
  w.finalize_membership_or_panic(19);
}

/// A fully-hosted leaderless group is ordinary election territory: reconcile passes during the
/// window wire nothing and re-wire nothing (every replica keeps its founding incarnation), and
/// the election completes without world help. The completion arm's missing-voter set is empty
/// the moment every committed voter hosts, so the default profile — where no verb ever removes
/// a hosting replica without removing its membership — can never reach the arm.
#[test]
fn fully_hosted_leaderless_group_elects_without_world_help() {
  let mut w = MultiWorld::new(5);
  for n in 0..3 {
    w.add_node(n);
  }
  let voters: BTreeSet<u64> = (0..3).collect();
  w.create_group(100, &voters);

  // Freshly created: leaderless with every committed voter hosting.
  assert!(w.leader_of(100).is_none());
  for _ in 0..5 {
    w.reconcile_membership(100);
  }
  assert_eq!(w.hosting_nodes(100), std::vec![0, 1, 2]);
  assert!(
    (0..3).all(|n| w.restarts.get(&(n, 100)) == Some(&1)),
    "no replica may be re-wired while the group is merely electing"
  );

  let mut elected = false;
  for _ in 0..3_000 {
    w.reconcile_membership(100);
    if w.leader_of(100).is_some() {
      elected = true;
      break;
    }
    w.tick();
  }
  assert!(elected, "the ordinary election must complete");
  assert!(
    (0..3).all(|n| w.restarts.get(&(n, 100)) == Some(&1)),
    "the election ran entirely on the founding replicas"
  );
}

/// A retired group is out of reconcile's reach entirely: no pass may ever re-wire a replica
/// for it (recreation is the lifecycle verb that revives a retired gid, at gen+1).
#[test]
fn reconcile_never_rewires_a_retired_group() {
  let mut w = MultiWorld::new(23);
  for n in 0..3 {
    w.add_node(n);
  }
  let voters: BTreeSet<u64> = (0..3).collect();
  w.create_group(100, &voters);
  assert!(w.run_until(3_000, |w| w.leader_of(100).is_some()));
  propose_until_accepted(&mut w, 100, &crate::multi::encode_gkv(100, 0, 1));
  assert!(w.run_until(2_000, |w| w.agreement_holds(100)
    && (0..3).all(|n| !w.applied_of(n, 100).is_empty())));

  w.remove_group(100);
  for _ in 0..5 {
    w.reconcile_membership(100);
    w.tick();
  }
  assert!(
    w.hosting_nodes(100).is_empty(),
    "reconcile must never resurrect a retired group's replicas"
  );
}

/// No unparked solicitation witness, no completion: a parked holder is delivery-isolated — its
/// campaigns reach nobody, so in the product nothing would ever materialize the missing
/// replicas. The arm must hold off until the witness returns (here: the holder unparked, the
/// exact moment solicitation becomes possible again).
#[test]
fn completion_arm_requires_an_unparked_solicitation_witness() {
  let (mut w, holder, _joiner) = world_with_stranded_child(17, 200);

  w.parked.insert((holder, 200));
  for _ in 0..5 {
    w.reconcile_membership(200);
    w.tick();
  }
  assert_eq!(
    w.hosting_nodes(200),
    std::vec![holder],
    "a parked holder solicits nothing — the arm must not fire"
  );

  w.parked.remove(&(holder, 200));
  w.reconcile_membership(200);
  assert_eq!(
    w.hosting_nodes(200),
    std::vec![0, 1],
    "unparking restores the witness and the arm completes the group"
  );
}

/// The completion arm is SHAPE-GENERIC: any under-hosted committed-voter group with a
/// campaigning holder completes, fork-born or not — the product's solicitation edge does not
/// distinguish (any voter-authenticated initial-shape solicitation triggers the factory). A
/// plain group loses one voter's replica to an external teardown, the leader crashes into a
/// leaderless window, and the arm wires the missing voter back as a catching-up OBSERVER
/// (the uniform blueprint shape — an empty must never be able to campaign against state it
/// does not carry) that catches up and converges while the two intact voters elect.
#[test]
fn completion_arm_is_shape_generic_beyond_fork_children() {
  let mut w = MultiWorld::new(29);
  for n in 0..3 {
    w.add_node(n);
  }
  let voters: BTreeSet<u64> = (0..3).collect();
  w.create_group(100, &voters);
  assert!(w.run_until(3_000, |w| w.leader_of(100).is_some()));
  for key in 0u16..4 {
    let payload = crate::multi::encode_gkv(100, key, u64::from(key));
    propose_until_accepted(&mut w, 100, &payload);
  }
  assert!(
    w.run_until(2_000, |w| {
      let lens: Vec<usize> = (0..3).map(|n| w.applied_of(n, 100).len()).collect();
      lens[0] >= 4 && lens.iter().min() == lens.iter().max()
    }),
    "the baseline never equalized"
  );

  let leader = w.leader_of(100).expect("elected");
  let victim = (0..3).rev().find(|&n| n != leader).expect("a follower");
  w.drop_group_replica(100, victim);
  w.crash(leader);
  assert!(
    w.leader_of(100).is_none(),
    "the crashed leader restores as a follower — the group is leaderless"
  );

  w.reconcile_membership(100);
  assert!(
    w.hosts_group(victim, 100),
    "the arm completes any under-hosted group, not only fork children"
  );
  let (wired_voters, _) = conf_of(&w, victim, 100);
  assert!(
    !wired_voters.contains(&victim),
    "completed as a catching-up observer — the empty can grant but never campaign"
  );

  let mut elected = false;
  for _ in 0..4_000 {
    w.reconcile_membership(100);
    if w.leader_of(100).is_some() {
      elected = true;
      break;
    }
    w.tick();
  }
  assert!(
    elected,
    "the completed group must elect: {}",
    w.dbg_group(100)
  );
  assert!(
    w.run_until(4_000, |w| {
      !w.applied_of(0, 100).is_empty()
        && (0..3).all(|n| w.applied_of(n, 100) == w.applied_of(0, 100))
    }),
    "the re-wired voter never converged: {}",
    w.dbg_group(100)
  );
  assert!(w.agreement_holds(100));
  w.check_now();
  w.finalize_membership_or_panic(29);
}

/// A child replica that arrives by the product's OTHER legitimate path — a fresh observer
/// caught up by snapshot transfer (`LogSm::snapshot()` carries the full record, inherited
/// parent-tagged baseline included) — must yield an aligned record identical to a fork-wired
/// sibling's. Alignment is content-derived and the cross-talk floor is the group registration
/// record, so the arrival path is irrelevant to both; deriving either from the fork-wired path
/// alone left this twin unswept — its parent-tagged baseline cells were judged as cross-talk
/// and its unskipped prefix misaligned the positional agreement leg.
#[test]
fn snapshot_wired_child_replica_aligns_with_fork_wired_siblings() {
  let mut w = MultiWorld::new(7);
  for n in 0..3 {
    w.add_node(n);
  }
  let voters: BTreeSet<u64> = [0, 1].into_iter().collect();
  w.create_group(100, &voters);
  assert!(w.run_until(3_000, |w| w.leader_of(100).is_some()));
  for key in 0u16..8 {
    let payload = crate::multi::encode_gkv(100, key, u64::from(key));
    propose_until_accepted(&mut w, 100, &payload);
  }
  assert!(
    w.run_until(2_000, |w| {
      let leader = w.leader_of(100);
      leader.is_some_and(|l| w.applied_of(l, 100).len() >= 8)
    }),
    "the keyed baseline never applied"
  );
  let mut accepted = false;
  for _ in 0..2_000 {
    if let Some(Ok(_)) = w.propose_split(100, 200, 4) {
      accepted = true;
      break;
    }
    w.tick();
  }
  assert!(accepted, "the split was never accepted");
  assert!(w.run_until(3_000, |w| w.leader_of(200).is_some()));
  assert_eq!(
    w.hosting_nodes(200),
    std::vec![0, 1],
    "the child forks only on the parent's voters"
  );

  // Child-own progress past the baseline, so the aligned comparison below is non-vacuous.
  propose_until_accepted(&mut w, 200, &crate::multi::encode_gkv(200, 5, 900));

  // The TWIN arrival: node 2 joins as a fresh observer. The fork-born log starts past the
  // manufactured baseline (`first_index == 2`), so its catch-up is structurally a snapshot
  // transfer carrying the full record.
  w.wire_group_observer(200, 2);
  propose_conf_change_until_accepted(&mut w, 200, sailing_proto::ConfChangeType::AddNode, 2);
  assert!(
    w.run_until(4_000, |w| {
      !w.applied_of(0, 200).is_empty() && w.applied_of(2, 200).len() == w.applied_of(0, 200).len()
    }),
    "the snapshot-wired observer never caught up: {}",
    w.dbg_group(200)
  );

  // The twin path genuinely delivered the inherited baseline (parent-tagged, parent-indexed)…
  let raw = w.applied_of(2, 200);
  assert!(
    raw
      .iter()
      .filter_map(|(_, c)| crate::multi::decode_gkv(c))
      .any(|(tag, _, _)| tag == 100),
    "the transferred snapshot must carry the parent-tagged inherited cells"
  );
  // …and the aligned view discounts it by CONTENT: identical to a fork-wired sibling's,
  // baseline excluded, agreement whole. (The ticks above already cross-talk-swept node 2's
  // record — an unfloored sweep would have tripped the oracle before reaching here.)
  assert_eq!(
    w.aligned_applied(2, 200, w.replica_gen_of(2, 200)),
    w.aligned_applied(0, 200, w.replica_gen_of(0, 200))
  );
  assert!(
    !w.aligned_applied(2, 200, w.replica_gen_of(2, 200))
      .is_empty()
  );
  assert!(
    raw.len() > w.aligned_applied(2, 200, w.replica_gen_of(2, 200)).len(),
    "the raw record must exceed the aligned one by the inherited baseline"
  );
  assert!(w.agreement_holds(200));
  w.check_now();
  w.finalize_conservation_or_panic(7);
  w.finalize_membership_or_panic(7);
}

/// The non-fork pin: a never-split group's oracle-aligned record is the raw record verbatim —
/// the group-level baseline is 0 and the population is the full domain, so alignment is the
/// identity on every node.
#[test]
fn aligned_view_is_the_raw_record_for_never_split_groups() {
  let mut w = MultiWorld::new(3);
  for n in 0..3 {
    w.add_node(n);
  }
  let all: BTreeSet<u64> = (0..3).collect();
  w.create_group(100, &all);
  assert!(w.run_until(3_000, |w| w.leader_of(100).is_some()));
  for key in 0u16..4 {
    let payload = crate::multi::encode_gkv(100, key, 40 + u64::from(key));
    propose_until_accepted(&mut w, 100, &payload);
  }
  assert!(
    w.run_until(2_000, |w| (0..3).all(|n| w.applied_of(n, 100).len() >= 4)),
    "the keyed load never applied everywhere"
  );
  for n in 0..3 {
    let raw = w.applied_of(n, 100);
    assert!(!raw.is_empty());
    assert_eq!(
      w.aligned_applied(n, 100, w.replica_gen_of(n, 100)),
      raw,
      "node {n}: alignment must be the identity for a never-split group"
    );
  }
}

/// The reshape band's re-split mechanism, pinned deterministically: a fork-born group SPLITS
/// ONWARD while one replica lags the split. `LogSm::split` removes moved-key cells from the
/// WHOLE record — inherited baseline included — so the ahead replicas hold FEWER leading
/// inherited cells than the laggard, and a positional discount of the recorded baseline count
/// eats the ahead side's own cells (two honest replicas diverge at aligned position 0).
/// Content alignment must keep the straddling pair prefix-related and converge them on heal.
#[test]
fn aligned_view_survives_a_lag_pair_straddling_an_onward_split() {
  let mut w = world_after_split(13, 200); // 200 owns {4,5,6,7}, hosted on {0,1,2}

  // Child-own load across kept AND moved keys, applied on every replica.
  for (key, value) in [(4u16, 40u64), (5, 50), (6, 60), (7, 70)] {
    propose_until_accepted(&mut w, 200, &crate::multi::encode_gkv(200, key, value));
  }
  assert!(
    w.run_until(2_000, |w| {
      (0..3).all(|n| {
        w.applied_of(n, 200)
          .iter()
          .any(|(_, c)| crate::multi::decode_gkv(c) == Some((200, 7, 70)))
      })
    }),
    "the child-own load never applied everywhere"
  );

  // Node 2 lags the whole onward split: isolate it, re-elect among the survivors, then move
  // {6,7} — and with them the inherited k6/k7 cells — onward to 300.
  w.isolate(2);
  assert!(w.run_until(3_000, |w| w.leader_of(200).is_some_and(|l| l != 2)));
  let mut accepted = false;
  for _ in 0..2_000 {
    if let Some(Ok(_)) = w.propose_split(200, 300, 6) {
      accepted = true;
      break;
    }
    w.tick();
  }
  assert!(accepted, "the onward split was never accepted");
  assert!(
    w.run_until(3_000, |w| w.hosts_group(0, 300) && w.hosts_group(1, 300)),
    "the onward child never materialized on the survivors"
  );
  // One more live-key cell the laggard cannot have, so the ahead views are strictly longer.
  propose_until_accepted(&mut w, 200, &crate::multi::encode_gkv(200, 4, 41));
  assert!(
    w.run_until(2_000, |w| {
      [0u64, 1].iter().all(|&n| {
        w.applied_of(n, 200)
          .iter()
          .any(|(_, c)| crate::multi::decode_gkv(c) == Some((200, 4, 41)))
      })
    }),
    "the post-split load never applied on the survivors"
  );

  // The mechanism is genuinely present: the ahead record's inherited prefix SHRANK below the
  // laggard's, and the laggard still holds a moved-key own cell the ahead record dropped.
  let inherited = |w: &MultiWorld, n: u64| {
    w.applied_of(n, 200)
      .iter()
      .filter_map(|(_, c)| crate::multi::decode_gkv(c))
      .filter(|(tag, _, _)| *tag == 100)
      .count()
  };
  assert_eq!(inherited(&w, 0), 2, "the ahead prefix keeps only k4/k5");
  assert_eq!(inherited(&w, 2), 4, "the laggard keeps the intact baseline");
  let holds_moved_own = |w: &MultiWorld, n: u64| {
    w.applied_of(n, 200)
      .iter()
      .any(|(_, c)| crate::multi::decode_gkv(c) == Some((200, 6, 60)))
  };
  assert!(!holds_moved_own(&w, 0) && holds_moved_own(&w, 2));

  // The straddling pair stays prefix-related: the laggard's aligned view is a strict prefix
  // of the ahead view, and the full agreement predicate holds. (Every tick above already ran
  // the checker's agreement leg over these views — a positional discount panics there before
  // reaching this point.)
  let ahead = w.aligned_applied(0, 200, w.replica_gen_of(0, 200));
  let lagging = w.aligned_applied(2, 200, w.replica_gen_of(2, 200));
  assert_eq!(w.aligned_applied(1, 200, w.replica_gen_of(1, 200)), ahead);
  assert!(lagging.len() < ahead.len());
  assert_eq!(&ahead[..lagging.len()], &lagging[..]);
  assert!(w.agreement_holds(200));

  // Heal: the laggard applies the onward split, its late fork materializes 300 on node 2, and
  // all three views converge.
  w.heal(2);
  assert!(
    w.run_until(4_000, |w| {
      w.hosts_group(2, 300)
        && (0..3).all(|n| {
          w.aligned_applied(n, 200, w.replica_gen_of(n, 200))
            == w.aligned_applied(0, 200, w.replica_gen_of(0, 200))
        })
    }),
    "the healed laggard never converged: {}",
    w.dbg_group(200)
  );
  assert!(w.agreement_holds(200) && w.agreement_holds(300));
  w.check_now();
  w.finalize_conservation_or_panic(13);
  w.finalize_membership_or_panic(13);
}

/// Two onward splits off one fork-born group, the second-generation child inheriting cells
/// tagged by BOTH ancestors. Alignment must reduce every replica to its own live-population
/// cells whatever the inheritance depth — a positional discount of the recorded baseline
/// (4 cells) leaves NOTHING of the parent once both onward splits shrink its record below
/// that count.
#[test]
fn aligned_view_survives_a_double_onward_split_chain() {
  let mut w = world_after_split(17, 200); // 200 owns {4,5,6,7}

  for (key, value) in [(4u16, 40u64), (5, 50), (6, 60), (7, 70)] {
    propose_until_accepted(&mut w, 200, &crate::multi::encode_gkv(200, key, value));
  }
  assert!(
    w.run_until(2_000, |w| {
      (0..3).all(|n| {
        w.applied_of(n, 200)
          .iter()
          .any(|(_, c)| crate::multi::decode_gkv(c) == Some((200, 7, 70)))
      })
    }),
    "the child-own load never applied everywhere"
  );

  // Onward split #1: {6,7} → 300. The moved slice spans BOTH generations — the grandparent's
  // inherited k6/k7 cells travel onward alongside 200's own.
  let mut accepted = false;
  for _ in 0..2_000 {
    if let Some(Ok(_)) = w.propose_split(200, 300, 6) {
      accepted = true;
      break;
    }
    w.tick();
  }
  assert!(accepted, "onward split #1 was never accepted");
  assert!(
    w.run_until(3_000, |w| {
      (0..3).all(|n| w.hosts_group(n, 300)) && w.leader_of(300).is_some()
    }),
    "the first onward child never materialized everywhere: {}",
    w.dbg_group(300)
  );
  propose_until_accepted(&mut w, 300, &crate::multi::encode_gkv(300, 6, 600));

  // Onward split #2 off the SAME fork-born parent: {5} → 400.
  let mut accepted = false;
  for _ in 0..2_000 {
    if let Some(Ok(_)) = w.propose_split(200, 400, 5) {
      accepted = true;
      break;
    }
    w.tick();
  }
  assert!(accepted, "onward split #2 was never accepted");
  assert!(
    w.run_until(3_000, |w| {
      (0..3).all(|n| w.hosts_group(n, 400)) && w.leader_of(400).is_some()
    }),
    "the second onward child never materialized everywhere: {}",
    w.dbg_group(400)
  );
  assert_eq!(w.splits_applied(), 3);

  // The grandchild's opening record carries BOTH ancestor tags with the exact moved history…
  let opening: Vec<(u64, u16, u64)> = w
    .applied_of(0, 300)
    .iter()
    .filter_map(|(_, c)| crate::multi::decode_gkv(c))
    .take(4)
    .collect();
  assert_eq!(
    opening,
    std::vec![(100, 6, 6), (100, 7, 7), (200, 6, 60), (200, 7, 70)],
    "the double-inherited baseline must carry both generations' cells in record order"
  );
  // …and alignment drops the whole multi-generation inheritance on every replica.
  assert!(
    w.run_until(2_000, |w| {
      (0..3).all(|n| {
        w.applied_of(n, 300)
          .iter()
          .any(|(_, c)| crate::multi::decode_gkv(c) == Some((300, 6, 600)))
      })
    }),
    "the grandchild's own load never applied everywhere"
  );
  for n in 0..3 {
    let cells: Vec<(u64, u16, u64)> = w
      .aligned_applied(n, 300, w.replica_gen_of(n, 300))
      .iter()
      .filter_map(|(_, c)| crate::multi::decode_gkv(c))
      .collect();
    assert_eq!(
      cells,
      std::vec![(300, 6, 600)],
      "node {n}: only own live-population cells survive alignment"
    );
  }

  // The doubly-shrunk parent still aligns to its one remaining live-key own cell.
  assert!(
    w.run_until(2_000, |w| {
      (0..3).all(|n| {
        w.aligned_applied(n, 200, w.replica_gen_of(n, 200))
          == w.aligned_applied(0, 200, w.replica_gen_of(0, 200))
      })
    }),
    "the parent replicas never converged: {}",
    w.dbg_group(200)
  );
  let parent_cells: Vec<(u64, u16, u64)> = w
    .aligned_applied(0, 200, w.replica_gen_of(0, 200))
    .iter()
    .filter_map(|(_, c)| crate::multi::decode_gkv(c))
    .collect();
  assert_eq!(parent_cells, std::vec![(200, 4, 40)]);
  assert!(w.agreement_holds(200) && w.agreement_holds(300) && w.agreement_holds(400));
  w.check_now();
  w.finalize_conservation_or_panic(17);
  w.finalize_membership_or_panic(17);
}

/// The arrival-path axis ACROSS an onward split: a fresh observer snapshot-catches-up on a
/// fork-born group AFTER it re-split onward, so the transferred record arrives BORN-SHRUNK —
/// its inherited prefix already below the registered baseline count on the very first sweep.
/// Content alignment and the count-floored cross-talk sweep must both take that record as-is,
/// and the twin must equal its fork-wired siblings, then lag as a strict prefix, never a
/// divergence.
#[test]
fn snapshot_wired_replica_born_after_an_onward_split_aligns() {
  let mut w = MultiWorld::new(19);
  for n in 0..3 {
    w.add_node(n);
  }
  let voters: BTreeSet<u64> = [0, 1].into_iter().collect();
  w.create_group(100, &voters);
  assert!(w.run_until(3_000, |w| w.leader_of(100).is_some()));
  for key in 0u16..8 {
    let payload = crate::multi::encode_gkv(100, key, u64::from(key));
    propose_until_accepted(&mut w, 100, &payload);
  }
  assert!(
    w.run_until(2_000, |w| {
      let leader = w.leader_of(100);
      leader.is_some_and(|l| w.applied_of(l, 100).len() >= 8)
    }),
    "the keyed baseline never applied"
  );
  let mut accepted = false;
  for _ in 0..2_000 {
    if let Some(Ok(_)) = w.propose_split(100, 200, 4) {
      accepted = true;
      break;
    }
    w.tick();
  }
  assert!(accepted, "the split was never accepted");
  assert!(w.run_until(3_000, |w| w.leader_of(200).is_some()));

  // Child-own load, then the onward split that shrinks every existing record.
  propose_until_accepted(&mut w, 200, &crate::multi::encode_gkv(200, 5, 50));
  propose_until_accepted(&mut w, 200, &crate::multi::encode_gkv(200, 6, 60));
  let mut accepted = false;
  for _ in 0..2_000 {
    if let Some(Ok(_)) = w.propose_split(200, 300, 6) {
      accepted = true;
      break;
    }
    w.tick();
  }
  assert!(accepted, "the onward split was never accepted");
  assert!(
    w.run_until(3_000, |w| w.hosts_group(0, 300) && w.hosts_group(1, 300)),
    "the onward child never materialized"
  );
  propose_until_accepted(&mut w, 200, &crate::multi::encode_gkv(200, 4, 41));

  // The twin arrives ONLY NOW: node 2 joins fresh, its record delivered by snapshot transfer.
  w.wire_group_observer(200, 2);
  propose_conf_change_until_accepted(&mut w, 200, sailing_proto::ConfChangeType::AddNode, 2);
  assert!(
    w.run_until(4_000, |w| {
      !w.applied_of(0, 200).is_empty() && w.applied_of(2, 200).len() == w.applied_of(0, 200).len()
    }),
    "the snapshot-wired twin never caught up: {}",
    w.dbg_group(200)
  );
  assert_eq!(w.groups[&200].fork_baseline, 4);
  let inherited: Vec<(u64, u16, u64)> = w
    .applied_of(2, 200)
    .iter()
    .filter_map(|(_, c)| crate::multi::decode_gkv(c))
    .filter(|(tag, _, _)| *tag == 100)
    .collect();
  assert_eq!(
    inherited,
    std::vec![(100, 4, 4), (100, 5, 5)],
    "the transferred record must arrive already shrunk by the onward split"
  );
  assert_eq!(
    w.aligned_applied(2, 200, w.replica_gen_of(2, 200)),
    w.aligned_applied(0, 200, w.replica_gen_of(0, 200))
  );
  assert!(
    !w.aligned_applied(2, 200, w.replica_gen_of(2, 200))
      .is_empty()
  );

  // A plain lag tail on top of the path axis: the twin misses one live-key cell and must
  // become a strict prefix.
  w.isolate(2);
  assert!(w.run_until(3_000, |w| w.leader_of(200).is_some_and(|l| l != 2)));
  propose_until_accepted(&mut w, 200, &crate::multi::encode_gkv(200, 4, 42));
  assert!(
    w.run_until(2_000, |w| {
      [0u64, 1].iter().all(|&n| {
        w.applied_of(n, 200)
          .iter()
          .any(|(_, c)| crate::multi::decode_gkv(c) == Some((200, 4, 42)))
      })
    }),
    "the post-join load never applied on the survivors"
  );
  let ahead = w.aligned_applied(0, 200, w.replica_gen_of(0, 200));
  let lagging = w.aligned_applied(2, 200, w.replica_gen_of(2, 200));
  assert!(lagging.len() < ahead.len());
  assert_eq!(&ahead[..lagging.len()], &lagging[..]);
  assert!(w.agreement_holds(200));
  w.heal(2);
  assert!(
    w.run_until(4_000, |w| w.aligned_applied(
      2,
      200,
      w.replica_gen_of(2, 200)
    ) == w.aligned_applied(
      0,
      200,
      w.replica_gen_of(0, 200)
    )),
    "the healed twin never re-converged"
  );
  w.check_now();
  w.finalize_conservation_or_panic(19);
  w.finalize_membership_or_panic(19);
}

/// The line between modeling and weakening: alignment must never MASK a genuine value
/// disagreement. Two records differing in one own-tagged live-key cell keep that divergence at
/// the same aligned position, and the checker's agreement leg still trips on the aligned views.
#[test]
fn alignment_never_masks_a_genuine_value_divergence() {
  let population: BTreeSet<u16> = [4, 5, 6, 7].into_iter().collect();
  let record = |divergent_value: u64| -> AppliedLog {
    std::vec![
      // A fork-inherited ancestor cell (dropped by tag) alongside the genuine disagreement.
      (9, crate::multi::encode_gkv(100, 4, 4)),
      (2, crate::multi::encode_gkv(200, 5, 50)),
      (3, crate::multi::encode_gkv(200, 4, divergent_value)),
    ]
  };
  let a = MultiWorld::align_record(record(40), 200, &population);
  let b = MultiWorld::align_record(record(41), 200, &population);
  assert_eq!(a.len(), 2);
  assert_eq!(b.len(), 2);
  assert_eq!(a[0], b[0]);
  assert_ne!(
    a[1], b[1],
    "the divergent cell must survive alignment on both sides"
  );

  // End-to-end through the oracle: the aligned views still trip the agreement leg.
  let node = |id: u64, applied_log: AppliedLog| checker::NodeView {
    id,
    removed: false,
    is_voter: true,
    poisoned: false,
    is_leader: id == 0,
    term: 1,
    commit: 3,
    applied: 3,
    applied_log,
    durable_first: 1,
    durable_last: 3,
    visible_last: 3,
    durable_entries: Vec::new(),
    snapshot_last_index: 0,
    snapshot_last_term: 0,
    installed_snapshot: false,
    conf_voters: BTreeSet::new(),
    conf_voters_outgoing: BTreeSet::new(),
    conf_learners: BTreeSet::new(),
    conf_learners_next: BTreeSet::new(),
    conf_auto_leave: false,
    conf_changed: 0,
    hardstate_commit: 3,
    inflight_staged: 0,
    incarnation: 0,
  };
  let view = checker::ClusterView {
    positional_agreement: true,
    seed: 0,
    tick: 0,
    committed_voters: None,
    committed_transitions: Vec::new(),
    new_installs: Vec::new(),
    nodes: std::vec![node(0, a), node(1, b)],
  };
  let v = checker::agreement(&view).unwrap_err();
  assert_eq!(v.oracle, "agreement");
}

/// NEGATIVE — COINCIDENCE IS NOT CAUSALITY. A retired-child fork conflict sitting on a node that
/// has nothing to do with a parked merge must not certify that merge past the liveness gates. The
/// classifier is what stands between a filed residual and a silenced fresh find, so it establishes
/// causality the way the #106/#110 classifiers do — through the parked source, or through this
/// group's own fence at its park boundary — and a bare coincidence satisfies neither.
#[test]
fn a_coincidental_retired_child_conflict_does_not_certify_an_unrelated_wedge() {
  let (mut w, source, target, child) = wedge_a_retired_hold_behind_a_merge(31);
  assert!(
    w.retired_hold_park(target) || w.retired_hold_park(source),
    "the genuine wedge certifies through its causal chain"
  );

  // A SECOND, UNRELATED group with its own retired-child conflict recorded on a node — no merge,
  // no park, no chain to the wedge above.
  let all: BTreeSet<u64> = (0..3).collect();
  w.create_group(77, &all);
  assert!(w.run_until(3_000, |w| w.leader_of(77).is_some()));
  w.inject_fork_conflict_for_child(0, 77, sailing_proto::Index::new(1), child);
  assert!(
    !w.retired_hold_park(77),
    "a retired-child conflict on a group that is no merge participant certifies nothing"
  );

  // And the conflict does not leak into the certified set for anyone else either.
  let certified = w.retired_hold_wedge_set();
  assert!(
    !certified.contains(&77),
    "the unrelated group stayed out of the certified set: {certified:?}"
  );
}

/// NEGATIVE — A MERGED-AWAY CHILD DOES NOT PARK. Production reads the id's floor off the engine,
/// where a merged-away id carries the reserved terminal; the relay therefore ABANDONS its fork
/// rather than holding it. The world's gate must answer the same, or every such fork would park as
/// a retired-hold and manufacture exemptions out of a class that is really terminal.
#[test]
fn a_merged_away_child_terminal_refuses_rather_than_parking() {
  let (mut w, _source, _target, child) = wedge_a_retired_hold_behind_a_merge(37);
  assert_eq!(
    w.split_refused_observed(),
    0,
    "the fork is HELD while the id is merely tombstoned"
  );

  // The id is merged away on the holding node: its floor is the reserved terminal.
  w.merge_floors.insert((2, child));
  assert!(
    w.run_until(6_000, |w| w.split_refused_observed() == 1),
    "a terminal floor abandons the fork instead of parking it"
  );
  assert!(
    !w.retired_hold_park(2),
    "and nothing is left parked to certify"
  );
}

/// Wedge a retired-hold BEHIND A MERGE: a fork held for a tombstoned child keeps its parent
/// unconsumable (`fork_obligations_standing`), so a merge naming that parent as SOURCE parks on the
/// replica holding it. Returns the world with the wedge standing and PROVEN STABLE. `source`
/// encodes above `target` under the LE-byte-string direction rule.
fn wedge_a_retired_hold_behind_a_merge(seed: u64) -> (MultiWorld, u64, u64, u64) {
  let (source, target, child) = (11u64, 10u64, 300u64);
  let mut w = MultiWorld::new(seed);
  for n in 0..3 {
    w.add_node(n);
  }
  let all: BTreeSet<u64> = (0..3).collect();
  w.create_group(source, &all);
  w.create_group(target, &all);
  assert!(w.run_until(3_000, |w| {
    w.leader_of(source).is_some() && w.leader_of(target).is_some()
  }));
  for key in 0u16..8 {
    let payload = crate::multi::encode_gkv(source, key, u64::from(key));
    propose_until_accepted(&mut w, source, &payload);
  }
  propose_until_accepted(&mut w, target, &crate::multi::encode_gkv(target, 0, 900));
  assert!(w.run_until(2_000, |w| {
    (0..3).all(|n| w.applied_of(n, source).len() >= 8)
  }));

  // Node 2 lags the split: isolate it, split on {0,1}, then retire the child while node 2 still
  // has the split entry ahead of it. Healing leaves node 2 holding a fork it cannot land.
  w.isolate(2);
  assert!(w.run_until(3_000, |w| w.leader_of(source).is_some_and(|l| l != 2)));
  propose_split_until_accepted(&mut w, source, child, 4);
  assert!(w.run_until(3_000, |w| w.splits_applied() == 1));
  w.remove_group(child);
  w.heal(2);
  assert!(
    w.run_until(4_000, |w| w.split_conflicts_observed() >= 1),
    "node 2's late fork never signalled its hold"
  );
  assert_eq!(
    w.split_refused_observed(),
    0,
    "a tombstone holds the fork, it does not abandon it"
  );

  // Merge the held fork's PARENT away as the source. The freeze is proposed at the leader, whose
  // own fork resolved at materialization, so it commits — and then node 2, which still owes its
  // held fork, cannot let its replica be consumed.
  colocate_source_onto_target(&mut w, source, target);
  merge_verb_until_accepted(&mut w, 2_000, "the freeze", |w| {
    w.propose_prepare_merge(source, target)
  });
  merge_verb_until_accepted(&mut w, 4_000, "the commit", |w| {
    w.propose_commit_merge(target, source)
  });
  assert!(
    w.run_until(6_000, |w| w.merges_resolved() >= 2),
    "the hosts without a held fork resolve the absorb"
  );
  // STABLE, not merely slow: a long quiet window buys no further progress.
  w.run_until(8_000, |_| false);
  assert_eq!(
    w.merges_resolved(),
    2,
    "the replica owing a held fork must NOT be consumed"
  );
  assert!(
    w.hosting_nodes(source).contains(&2),
    "node 2 still holds the un-consumable source replica: {:?}",
    w.hosting_nodes(source)
  );
  (w, source, target, child)
}

/// THE ISLAND, BUILT DELIBERATELY — the HELD regime. A fork held through its child's removal, then
/// released onto the tombstoned id: what lands is the incarnation the split named, inherited parent
/// cells and all.
///
/// The subject is deliberately a FLOOR-LESS id. A retirement persists an admission floor only for a
/// RESHAPED id (one past its removal ceiling), and this child never reshaped, so its tombstone is
/// the only thing standing between the fork and its id — and a tombstone HOLDS, because consent can
/// lift it. The sibling below takes the other regime, where a recreation makes the id reshaped and
/// its retirement leaves a floor a monotone counter can never clear.
///
/// This is the shape the single-meta oracle misread. Judged against a successor's expectations (no
/// inherited prefix, no tag lineage) every inherited cell reads as a cross-group leak; judged
/// against its OWN, they are exactly what a fork child legitimately opens with. The cure is neither
/// exclusion nor an assert that coexistence cannot happen — it remains reachable through paths this
/// branch does not close — but INCARNATION-BOUND JUDGING: the island gets its own live checker and
/// its own expectations.
#[test]
fn a_late_fork_lands_as_its_own_incarnation_and_is_judged_there() {
  let (source, child) = (11u64, 300u64);
  let mut w = MultiWorld::new(41);
  for n in 0..3 {
    w.add_node(n);
  }
  let all: BTreeSet<u64> = (0..3).collect();
  w.create_group(source, &all);
  assert!(w.run_until(3_000, |w| w.leader_of(source).is_some()));
  for key in 0u16..8 {
    let payload = crate::multi::encode_gkv(source, key, u64::from(key));
    propose_until_accepted(&mut w, source, &payload);
  }
  assert!(w.run_until(2_000, |w| {
    (0..3).all(|n| w.applied_of(n, source).len() >= 8)
  }));

  // Node 2 lags the split: {0,1} materialize the child while node 2 never even sees the entry.
  w.isolate(2);
  assert!(w.run_until(3_000, |w| w.leader_of(source).is_some_and(|l| l != 2)));
  propose_split_until_accepted(&mut w, source, child, 4);
  assert!(w.run_until(3_000, |w| w.splits_applied() == 1));

  // RETIRE IT while node 2 is away. Node 2 is a MEMBER, so the removal tombstones it — and the
  // teardown carries no fork token, so it SPARES the fork rather than abandoning it. No recreation:
  // bringing the id back would make it a reshaped id, and retiring THAT leaves a floor the held
  // fork could never clear — the sibling regime, not this one.
  assert!(w.remove_group(child));
  assert_eq!(
    w.generation_of(child),
    0,
    "the id never moved on: this regime's subject is the incarnation the split named"
  );
  // THE PREMISE, ASSERTED. A never-reshaped id acquires no admission floor at its retirement, so
  // the tombstone is the whole of what stands in the fork's way — and a tombstone is liftable.
  assert_eq!(
    w.removal_floors.get(&child),
    None,
    "the held regime needs a FLOOR-LESS subject; a floor here would abandon the fork instead"
  );

  w.heal(2);
  assert!(
    w.run_until(4_000, |w| w.split_conflicts_observed() >= 1),
    "node 2 replays the split and HOLDS its fork on the tombstone"
  );
  assert_eq!(
    w.split_refused_observed(),
    0,
    "a token-less removal spares the fork — it is held, not abandoned"
  );

  // Consent lifts the tombstone — the one thing standing in the fork's way — and releases it.
  assert!(
    w.clear_tombstone(child),
    "a tombstone was standing to clear"
  );
  assert!(
    w.run_until(6_000, |w| w.hosts_group(2, child)),
    "the held fork lands"
  );
  assert_eq!(
    w.replica_gen_of(2, child),
    0,
    "it lands as the incarnation the SPLIT named, not as whatever the id has become"
  );

  // THE PARENT IS DISCHARGED, exactly as it is on the terminal side — a fork that lands must free
  // its parent as surely as one that is abandoned, or the parent stays capture-fenced behind a
  // fork that has already resolved.
  assert!(
    w.hosts
      .get_mut(&2)
      .expect("node 2 is hosted")
      .peek_yieldable_fork(&sailing_proto::NoHold)
      .is_none(),
    "no fork remains staged: materializing POPS it"
  );
  assert!(
    w.fork_conflicts
      .get(&(2, source))
      .is_none_or(std::collections::BTreeMap::is_empty),
    "no fork barrier still stands on the parent: {:?}",
    w.fork_conflicts.get(&(2, source))
  );
  assert!(
    !w.hosts[&2].split_reserved(&child),
    "the child id is no longer reserved by an in-flight split"
  );
  // AND HERE THE TWO REGIMES PART. A terminal refusal owes NO guard advance — it re-derives from
  // the floor. A held fork that is later RELEASED did relay, so the mirror must have moved with it:
  // a regression that installs the child while leaving the mirror stale would replay this very fork
  // again after a restart and aim a second manufactured baseline at the child's real progress.
  let parent_gen_after = w.hosts[&2]
    .group(&source)
    .expect("node 2 hosts the parent")
    .shape_gen();
  assert_eq!(
    parent_gen_after, 1,
    "the one split bumps the parent's lineage to 1"
  );
  assert_eq!(
    w.relayed_lineage.get(&(2, source)),
    Some(&parent_gen_after),
    "the relay mirror must equal the installed fork's parent generation; it reads {:?}",
    w.relayed_lineage.get(&(2, source))
  );

  // JUDGED under its own incarnation, with a checker of its own. (The coexistence shape — an island
  // hosted beside a LIVE successor of the same id — is built and judged by
  // `island_beside_a_live_successor` and its tests, which reach it without retiring the successor.)
  let judged = w.judged_incarnations();
  assert!(
    judged.contains(&(child, 0)),
    "the island is judged under its own incarnation: {judged:?}"
  );
  // Its expectations are its own: an inherited prefix, and the parent's tag as a LEGAL carrier.
  let island = w
    .meta_at(child, 0)
    .expect("the superseded incarnation's expectations were archived, not destroyed");
  assert!(
    island.fork_baseline > 0,
    "the island opens on the fork's inherited baseline"
  );
  assert!(
    island.carried_tags.contains(&source),
    "and carries the parent's tag legitimately: {:?}",
    island.carried_tags
  );
  // Non-vacuous: the inherited cells really are on the island's replica, and the full oracle suite
  // sweeps them without tripping.
  assert!(
    w.applied_of(2, child).len() >= island.fork_baseline,
    "the inherited cells are present to be judged"
  );
  w.check_now();
  w.finalize_conservation_or_panic(41);
}

/// HARNESS FIDELITY, not a product guarantee. The world's create path mirrors the drivers, and a
/// driver barriers its stores after an admission before the replica takes traffic; without that
/// barrier the founding stamp sits in an async store's in-flight window, and a crash landing
/// between the admission ack and the first tick rolls it back — the replica comes back founded at
/// zero, beneath the generation it was admitted at, and its lineage counter disagrees with every
/// peer's.
///
/// What this pin encodes is that the HARNESS keeps the drivers' barrier. It is deliberately NOT a
/// claim about what an embedder gets: a separate-stores embedder that genuinely crashes inside that
/// window has no stamp to recover, and the product's answer there is the typed
/// `IncarnationUnrecoverable` refusal from `validate_restore`. The world does not exercise that
/// refusal at all — its crash path calls `MultiRaft::restore_group` directly and bypasses the
/// coordinator door `validate_restore` lives behind — so that coverage gap is real and recorded in
/// the PR notes rather than papered over here.
#[test]
fn a_founding_stamp_survives_a_crash_before_the_first_tick() {
  let gid = 700u64;
  let mut w = MultiWorld::new(9);
  // ASYNC stores: the mode where an unflushed write is genuinely rolled back by a crash. Under the
  // sync default nothing is ever in flight and the pin could not fail however the barrier moved.
  w.set_store_mode(crate::StoreMode::Async);
  for n in 0..3 {
    w.add_node(n);
  }
  let all: BTreeSet<u64> = (0..3).collect();
  w.create_group(gid, &all);
  assert!(w.run_until(3_000, |w| w.leader_of(gid).is_some()));

  // Retire and bring it back: the recreation is what founds a replica at a NONZERO generation, and
  // the storeless door cannot serve one — so this is the only path that writes a founding stamp.
  assert!(w.remove_group(gid));
  w.recreate_group(gid);
  let generation = w.generation_of(gid);
  assert_eq!(generation, 1, "the recreation founds the id at 1");

  // CRASH IN THE SAME ITERATION — before any tick, so nothing but the admission's own barrier can
  // have made the stamp durable.
  w.crash(0);

  assert_eq!(
    w.hosts[&0]
      .group(&gid)
      .expect("the crashed node restored its replica")
      .shape_gen(),
    generation,
    "the restored replica must come back founded where it was admitted; a rolled-back stamp \
     brings it up at zero and it mints beneath its own peers"
  );
  for node in [1u64, 2] {
    assert_eq!(
      w.hosts[&node]
        .group(&gid)
        .expect("peer hosts the group")
        .shape_gen(),
      generation,
      "and it agrees with peer {node}, which never crashed"
    );
  }
}

/// THE OTHER REGIME: the same late fork, against an id whose retirement left a FLOOR.
///
/// A recreation makes the id reshaped, so retiring the successor persists an admission floor one
/// past its removal ceiling. The fork the isolated node still owes was minted at generation 0 and
/// names that generation for its child; a floor is monotone, so no consent call and no later event
/// can ever bring it back within reach. That is what separates this from the held regime: a
/// tombstone is a decision the embedder can reverse, a floor is one it cannot, so the fork is
/// TERMINALLY abandoned rather than parked.
///
/// The refusal is only half the claim. The other half — the load-bearing half — is that abandoning
/// it leaves the PARENT clean: the fork is popped rather than left staged, its barrier lifts, the
/// child id stops being reserved, and no relay-guard advance is owed, because a Terminal refusal
/// re-derives from the floor rather than from a recorded bump. A refusal that left any of those
/// behind would wedge the parent's next capture on a fork that will never resolve.
#[test]
fn a_late_fork_below_a_recreated_ids_floor_is_abandoned() {
  let (source, child) = (11u64, 300u64);
  let mut w = MultiWorld::new(41);
  for n in 0..3 {
    w.add_node(n);
  }
  let all: BTreeSet<u64> = (0..3).collect();
  w.create_group(source, &all);
  assert!(w.run_until(3_000, |w| w.leader_of(source).is_some()));
  for key in 0u16..8 {
    let payload = crate::multi::encode_gkv(source, key, u64::from(key));
    propose_until_accepted(&mut w, source, &payload);
  }
  assert!(w.run_until(2_000, |w| {
    (0..3).all(|n| w.applied_of(n, source).len() >= 8)
  }));

  // Node 2 lags the split: {0,1} materialize the child while node 2 never even sees the entry.
  w.isolate(2);
  assert!(w.run_until(3_000, |w| w.leader_of(source).is_some_and(|l| l != 2)));
  propose_split_until_accepted(&mut w, source, child, 4);
  assert!(w.run_until(3_000, |w| w.splits_applied() == 1));

  // THE WHOLE CEREMONY while node 2 is away: retire gen 0, bring the id back as gen 1, retire that
  // too. The recreation is what makes the id reshaped, and retiring a reshaped id is what leaves
  // the floor behind.
  assert!(w.remove_group(child));
  w.recreate_group(child);
  assert!(w.remove_group(child));
  assert_eq!(
    w.generation_of(child),
    1,
    "the id moved on while node 2 was away"
  );
  assert_eq!(
    w.removal_floors.get(&child).copied(),
    Some(2),
    "retiring the recreated incarnation leaves a floor one past its ceiling"
  );

  w.heal(2);
  assert!(
    w.run_until(6_000, |w| w.split_refused_observed() >= 1),
    "node 2 replays the split and its fork is refused below the floor: {}",
    w.dbg_group(child)
  );
  // STABLE, not merely slow: a long quiet window buys no park and no second refusal.
  w.run_until(4_000, |_| false);
  assert_eq!(
    w.split_refused_observed(),
    1,
    "exactly one fork was abandoned"
  );
  assert_eq!(
    w.split_conflicts_observed(),
    0,
    "a below-floor fork is never parked: a park waits for a decision to be reversed, and a \
     monotone floor is the one refusal that provably never lifts"
  );

  // THE PARENT IS DISCHARGED CLEAN — the half that keeps a refusal from wedging the parent.
  assert!(
    w.hosts
      .get_mut(&2)
      .expect("node 2 is hosted")
      .peek_yieldable_fork(&sailing_proto::NoHold)
      .is_none(),
    "no fork remains staged: the refusal POPS it rather than leaving it to be re-examined forever"
  );
  assert!(
    w.fork_conflicts
      .get(&(2, source))
      .is_none_or(std::collections::BTreeMap::is_empty),
    "no fork barrier still stands on the parent: {:?}",
    w.fork_conflicts.get(&(2, source))
  );
  assert!(
    !w.hosts[&2].split_reserved(&child),
    "the child id is no longer reserved by an in-flight split"
  );
  assert_eq!(
    w.relayed_lineage.get(&(2, source)),
    None,
    "a Terminal refusal owes no relay-guard advance — it re-derives from the floor, and mirroring \
     a bump for a fork that never relayed would fold the NEXT one away with it"
  );

  // AND NO ISLAND FORMS. The whole point of the other regime is unreachable here.
  assert!(
    !w.hosts_group(2, child),
    "nothing landed: the fork was abandoned, not held for a consent that could release it"
  );
  w.check_now();
  w.finalize_conservation_or_panic(41);
}

/// THE COEXISTENCE, BUILT DELIBERATELY: a late gen-0 island hosted beside a LIVE gen-1 successor of
/// the same id, the two owning DIFFERENT key populations. Returns `(world, source, child)`.
///
/// A FREE SLOT is what makes the shape reachable. Node 2 lags the split, so it owes a fork it never
/// materialized; the child then sheds node 2 from its membership BEFORE it is retired, so the
/// teardown tombstones only the members it still has and the recreation wires successor replicas
/// over {0,1} alone. Healed, node 2 replays the split and its fork installs the DEAD incarnation on
/// the one node the successor does not occupy — the id is hosted at two incarnations at once.
///
/// The populations diverge by construction: gen 0 was assigned the at-or-above-point slice {4..7}
/// while the recreation hands gen 1 its whole domain back, and gen 0's record is the parent-tagged
/// inherited baseline that gen 1 — carrying nothing — would read as a leak. Every gid-only read of
/// "the group's metadata" resolves to the successor's, which is neither.
fn island_beside_a_live_successor(seed: u64) -> (MultiWorld, u64, u64) {
  let (source, child) = (11u64, 300u64);
  let mut w = MultiWorld::new(seed);
  for n in 0..3 {
    w.add_node(n);
  }
  let all: BTreeSet<u64> = (0..3).collect();
  w.create_group(source, &all);
  assert!(w.run_until(3_000, |w| w.leader_of(source).is_some()));
  for key in 0u16..8 {
    let payload = crate::multi::encode_gkv(source, key, u64::from(key));
    propose_until_accepted(&mut w, source, &payload);
  }
  assert!(w.run_until(2_000, |w| {
    (0..3).all(|n| w.applied_of(n, source).len() >= 8)
  }));

  // Node 2 lags the split: {0,1} materialize the child while node 2 never even sees the entry.
  w.isolate(2);
  assert!(w.run_until(3_000, |w| w.leader_of(source).is_some_and(|l| l != 2)));
  propose_split_until_accepted(&mut w, source, child, 4);
  assert!(w.run_until(3_000, |w| w.splits_applied() == 1));
  assert!(w.run_until(4_000, |w| w.leader_of(child).is_some()));

  // FREE THE SLOT: shed node 2 from the child's membership. COMMITTED first, reconciled second —
  // the registry's unpark/resurrect arm re-wires any committed member that is not hosting, and
  // node 2 is exactly that until the removal commits, so reconciling early would hand it the
  // replica whose absence this fixture is built on.
  propose_conf_change_until_accepted(&mut w, child, sailing_proto::ConfChangeType::RemoveNode, 2);
  assert!(
    w.run_until(4_000, |w| w.leader_of(child).is_some()
      && [0u64, 1]
        .iter()
        .all(|&n| !conf_of(w, n, child).0.contains(&2))),
    "the child's removal of node 2 never committed"
  );
  w.reconcile_membership(child);
  assert_eq!(
    w.meta_at(child, 0).map(|m| m.voters.clone()),
    Some([0u64, 1].into_iter().collect()),
    "the child's registry voters never shrank"
  );
  assert!(
    !w.hosts_group(2, child),
    "node 2 must be off the child, or the teardown below tombstones it and holds the fork"
  );

  // Retire gen 0 and bring the id straight back, all while node 2 is away. Gen 1 is LIVE from here.
  assert!(w.remove_group(child));
  w.recreate_group(child);
  assert_eq!(
    w.generation_of(child),
    1,
    "the id moved on while node 2 was away"
  );
  assert!(
    w.run_until(4_000, |w| w.leader_of(child).is_some()),
    "the successor never elected"
  );

  w.heal(2);
  assert!(
    w.run_until(8_000, |w| w.hosts_group(2, child)),
    "the late fork never landed: {}",
    w.dbg_group(child)
  );
  assert_eq!(
    w.replica_gen_of(2, child),
    0,
    "it lands as the incarnation the SPLIT named, not as whatever the id has become"
  );
  assert!(
    !w.meta_at(child, 1).expect("the successor is live").retired,
    "the successor must still be LIVE beside the island — that is the whole shape"
  );
  (w, source, child)
}

/// DIRECTION ONE — the island is judged, recorded, and conserved under ITS OWN incarnation's
/// metadata while the successor is live at the same id.
///
/// The teeth are the ONWARD SPLIT off the live successor. That split's conservation pair names
/// gen 1's ledger id as the parent, so every cell filed there is demanded of the grandchild. The
/// island's cells belong to gen 0 and were never handed to anyone; filing them under the registry's
/// current generation invents a debt the successor's lineage cannot pay, and the run-end verdict
/// reads that as history lost or continued on both sides. Nothing about the shape is synthetic —
/// the cells are the fork's own inherited baseline, and the ledger id is the only thing in dispute.
///
/// Red-proof: revert `conserve_sweep` to `ledger_id(self.generation_of(gid), gid)` with no
/// replica-generation filter and `finalize_conservation_or_panic` trips on the g1000300->g301
/// partition, naming the island's parent-tagged values as the missing prefix.
#[test]
fn a_coexisting_island_is_conserved_under_its_own_incarnation() {
  let (seed, grandchild) = (41u64, 301u64);
  let (mut w, source, child) = island_beside_a_live_successor(seed);

  // The fixture's premise, asserted: two live incarnations of one id, DISTINCT key populations,
  // and a tag lineage that only the island has.
  let judged = w.judged_incarnations();
  assert!(
    judged.contains(&(child, 0)) && judged.contains(&(child, 1)),
    "both incarnations must be live-judged: {judged:?}"
  );
  let gen0 = w
    .meta_at(child, 0)
    .expect("the island's archived meta")
    .clone();
  let gen1 = w.meta_at(child, 1).expect("the live successor").clone();
  assert_ne!(gen0.keys, gen1.keys, "the populations must differ");
  assert!(
    gen0.carried_tags.contains(&source) && gen1.carried_tags.is_empty(),
    "the parent's tag is legal only under gen 0: {:?} vs {:?}",
    gen0.carried_tags,
    gen1.carried_tags
  );
  // Non-vacuous: the island really holds parent-tagged cells for keys gen 1 also owns.
  let island: Vec<(u64, u16, u64)> = w
    .applied_of(2, child)
    .iter()
    .filter_map(|(_, c)| crate::multi::decode_gkv(c))
    .collect();
  assert!(!island.is_empty(), "the island has no cells to misfile");
  assert!(
    island
      .iter()
      .all(|&(tag, key, _)| tag == source && gen0.keys.contains(&key) && gen1.keys.contains(&key)),
    "the island's cells must be parent-tagged and inside BOTH populations: {island:?}"
  );

  // The live successor writes its own cells over the same keys and then splits them away. The
  // grandchild's conservation pair takes gen 1's ledger id as its parent.
  for key in 4u16..8 {
    let payload = crate::multi::encode_gkv(child, key, 900 + u64::from(key));
    propose_until_accepted(&mut w, child, &payload);
  }
  assert!(
    w.run_until(4_000, |w| [0u64, 1]
      .iter()
      .all(|&n| w.applied_of(n, child).len() >= 4)),
    "the successor's own load never applied: {}",
    w.dbg_group(child)
  );
  propose_split_until_accepted(&mut w, child, grandchild, 4);
  assert!(
    w.run_until(4_000, |w| w.hosting_nodes(grandchild).len() >= 2),
    "the successor's onward split never materialized: {}",
    w.dbg_group(child)
  );
  assert_eq!(
    w.replica_gen_of(2, child),
    0,
    "the island must still be hosted at gen 0 through the onward split"
  );

  w.check_now();
  w.finalize_conservation_or_panic(seed);
  w.finalize_lineage_or_panic(seed);
  w.finalize_membership_or_panic(seed);
}

/// DIRECTION TWO — the incarnation filters must not SILENCE the successor. A cell the successor
/// cannot legally hold still trips, with an island of the same id hosted beside it.
///
/// The corruption is the exact cell the island carries legally: a parent-tagged gkv payload. Gen 0
/// inherited the parent's tag lineage, gen 1 was recreated and inherits nothing, so the same bytes
/// are a legal baseline cell on one incarnation and a cross-group leak on the other. A sweep that
/// filtered the successor's replicas away, or that judged them against the island's expectations,
/// would let this pass in silence.
#[test]
#[should_panic(expected = "cross-group")]
fn a_corrupted_successor_cell_still_trips_beside_the_island() {
  let (mut w, source, child) = island_beside_a_live_successor(41);
  let payload = crate::multi::encode_gkv(source, 2, 4_242);
  propose_until_accepted(&mut w, child, &payload);
  w.run_until(4_000, |_| false);
  panic!("the successor's foreign-tagged cell was never judged");
}

/// THE ISLAND'S ClusterView CARRIES ITS OWN VOTER DENOMINATOR.
///
/// `committed_voters_of` reads the authoritative committed config off the group's replicas, and the
/// island's replicas are not the successor's. Unqualified it returns the live successor's voters,
/// which the island's view then intersects with its own single hosting replica to NOTHING: the
/// quorum-durability leg iterates an empty voter set and certifies vacuously, for the rest of the
/// run. Qualified, the island supplies its own set and the leg has a denominator to judge against.
///
/// Red-proof: drop the `replica_gen_of` filter from `committed_voters_of` and the island's set comes
/// back as the successor's `{0,1}`, which contains none of the island's replicas.
#[test]
fn a_coexisting_island_supplies_its_own_voter_denominator() {
  let (w, _source, child) = island_beside_a_live_successor(41);

  let island = w.committed_voters_of(child, 0);
  let successor = w.committed_voters_of(child, 1);
  assert!(
    island.contains(&2),
    "the island's denominator must contain the node actually hosting it: {island:?}"
  );
  assert_ne!(
    island, successor,
    "the two incarnations must not share a denominator: {island:?}"
  );
  assert!(
    !successor.contains(&2),
    "and the successor's set is the one that would silence the island: {successor:?}"
  );

  // The view the oracle suite actually judges: non-empty voters, and the denominator is the
  // island's own.
  let view = w.group_view(child, 0);
  assert_eq!(
    view.committed_voters.as_ref(),
    Some(&island),
    "the island's view must carry its own committed voters"
  );
  assert_eq!(
    view.nodes.iter().filter(|n| n.id == 2).count(),
    1,
    "the island's single replica is in its own view"
  );
  assert!(
    island.iter().any(|v| view.nodes.iter().any(|n| n.id == *v)),
    "the denominator must intersect the view, or every voter-keyed leg judges nothing"
  );
}

/// A NON-QUORUM ISLAND COMMIT TRIPS THE QUORUM-DURABILITY LEG. Its sibling above proves the
/// denominator is the island's own; this proves the leg that denominator feeds is live rather than
/// merely non-empty.
///
/// The commit is synthesized on the fixture's real island view, and it has to be: the island holds
/// one replica of a three-voter config and the delivery fence keeps its successor's hosts away, so
/// it can never legitimately commit past the manufactured baseline — which is precisely why a
/// missing denominator here would never be noticed by a passing run.
///
/// Red-proof: drop the `replica_gen_of` filter from `committed_voters_of`. The view's denominator
/// becomes the successor's `{0,1}`, `ClusterView::voters()` yields nothing, and the same deliberate
/// violation returns `Ok` — the silent certification this closes.
#[test]
fn a_non_quorum_island_commit_trips_the_quorum_durability_leg() {
  let (w, _source, child) = island_beside_a_live_successor(41);
  let mut view = w.group_view(child, 0);
  assert!(
    view.committed_voters.as_ref().is_some_and(|v| v.len() > 1),
    "the island's own config is multi-voter, or a lone replica IS its own quorum"
  );
  let clean = crate::checker::commit_is_quorum_durable(&view, sailing_proto::FORK_BASE_INDEX.get());
  assert!(
    clean.is_ok(),
    "the untouched island must be clean before the deliberate violation: {clean:?}"
  );

  // The violation: the island's one replica claims a commit far past anything durable anywhere,
  // with no second voter to witness it.
  let island_node = view
    .nodes
    .iter_mut()
    .find(|n| n.id == 2)
    .expect("the island's replica is in its own view");
  island_node.commit = 9_999;
  let verdict =
    crate::checker::commit_is_quorum_durable(&view, sailing_proto::FORK_BASE_INDEX.get());
  assert!(
    verdict.is_err(),
    "a lone replica claiming an unwitnessed commit must trip the quorum-durability leg"
  );
}

/// A MERGE IN ONE INCARNATION MUST NOT DISABLE THE OTHER'S CHECKING.
///
/// The append-only gate and the agreement mode switch were read id-wide, so a union folded into the
/// live successor re-based the successor's record and switched BOTH incarnations out of positional
/// and phantom-quorum checking. The island absorbed nothing; its record is still append-only, and
/// retiring its coverage on someone else's merge loses every violation it would have caught for the
/// rest of the run.
///
/// Red-proof: revert `record_is_append_only` and the `group_view` mode switch to the gid-wide
/// `group_absorbed`/`is_merged`/`group_frozen` reads and the island's two assertions below invert.
#[test]
fn a_successors_merge_leaves_the_islands_checking_armed() {
  let (mut w, _source, child) = island_beside_a_live_successor(41);
  // Above the child's id: the container orients every merge pair source-encodes-above-target.
  let donor = 501u64;

  // Both incarnations start armed.
  assert!(w.record_is_append_only(child, 0) && w.record_is_append_only(child, 1));
  let judged_before = w.lineage_cells_judged();

  // A union folded into the LIVE successor, on the two nodes that host it.
  let pair: BTreeSet<u64> = [0u64, 1].into_iter().collect();
  w.create_group(donor, &pair);
  assert!(w.run_until(4_000, |w| w.leader_of(donor).is_some()));
  for key in 0u16..3 {
    let payload = crate::multi::encode_gkv(donor, key, 500 + u64::from(key));
    propose_until_accepted(&mut w, donor, &payload);
  }
  assert!(w.run_until(3_000, |w| {
    [0u64, 1].iter().all(|&n| w.applied_of(n, donor).len() >= 3)
  }));
  colocate_source_onto_target(&mut w, donor, child);
  merge_verb_until_accepted(&mut w, 3_000, "the freeze", |w| {
    w.propose_prepare_merge(donor, child)
  });
  merge_verb_until_accepted(&mut w, 4_000, "the commit", |w| {
    w.propose_commit_merge(child, donor)
  });
  assert!(
    w.run_until(8_000, |w| w.group_absorbed_at(child, 1)),
    "the successor never absorbed the donor: {}",
    w.dbg_group(child)
  );

  // THE SEPARATION. The successor is out of positional/append-only checking, as a merge target must
  // be; the island — which absorbed nothing — is still fully armed.
  assert!(
    !w.record_is_append_only(child, 1),
    "the absorbing successor must leave the append-only leg"
  );
  assert!(
    w.record_is_append_only(child, 0),
    "but the island absorbed nothing and must stay armed"
  );
  assert!(
    !w.group_absorbed_at(child, 0),
    "the union belongs to the successor's ledger id, not the island's"
  );
  assert!(
    w.group_view(child, 0).positional_agreement,
    "the island keeps the positional comparison its own record still satisfies"
  );
  assert!(
    !w.group_view(child, 1).positional_agreement,
    "and the successor gives it up — the mode switch is per incarnation, not per id"
  );
  assert_eq!(
    w.replica_gen_of(2, child),
    0,
    "the island stood through the whole choreography"
  );

  // NON-VACUOUS: the island's cells are still being fed to the ledger after the merge.
  w.run_until(400, |_| false);
  assert!(
    w.lineage_cells_judged() > judged_before,
    "the lineage ledger stopped judging cells once the successor merged"
  );
  w.check_now();
  w.finalize_lineage_or_panic(41);
}

/// VALVE ONE — CONSENT. `clear_tombstone` is the production release for a fork held on a tombstone,
/// and it is the gentle one: it lifts only the volatile consent gate, leaving the id's floor alone.
/// The relay's gate stops reporting the child spoken-for, so the held fork LANDS — blob intact, the
/// partition delivered — the parent's obligation discharges, and the merge that was parked behind
/// it completes. Nothing is dropped anywhere on this path.
#[test]
fn a_retired_hold_releases_its_merge_when_the_tombstone_clears() {
  let (mut w, source, target, child) = wedge_a_retired_hold_behind_a_merge(23);

  assert!(
    w.clear_tombstone(child),
    "a tombstone was standing to clear"
  );
  assert!(
    w.run_until(6_000, |w| w.hosts_group(2, child)),
    "consent alone lets the held fork LAND: the child materializes where it was held"
  );
  assert_eq!(
    w.split_refused_observed(),
    0,
    "nothing was abandoned — this valve delivers the partition, it does not drop it"
  );
  assert!(
    w.run_until(8_000, |w| w.hosting_nodes(source).is_empty()),
    "the discharged obligation lets the last host give up its source replica: {:?}",
    w.hosting_nodes(source)
  );
  assert!(
    !w.group_merge_parked(target),
    "the target is no longer parked on the absorbed source: {}",
    w.merge_block_dbg(target)
  );
  w.finalize_conservation_or_panic(23);
}

/// VALVE TWO — a TERMINAL floor. The other real product path: once the child id carries a floor the
/// fork's generation can never clear, the relay abandons it deliberately and the blob is dropped as
/// the embedder's own declaration. The merge behind it completes either way, which is the point of
/// covering both valves — the wedge is releasable, and by more than one act.
///
/// The floor used here is the merged-away sentinel, modelled the way the husk-dissolve suite models
/// it: a completed merge floors the id terminally on this node. (Under the node-local gate a plain
/// `recreate_group` no longer reaches this arm — it bumps the incarnation counter, which is not a
/// floor; a never-reshaped child's teardown writes none.)
#[test]
fn a_retired_hold_is_abandoned_when_its_child_id_is_floored_terminally() {
  let (mut w, source, target, child) = wedge_a_retired_hold_behind_a_merge(29);

  // MODEL THE TERMINAL FLOOR on the node holding the fork: the id was merged away there.
  w.merge_floors.insert((2, child));
  assert!(
    w.run_until(6_000, |w| w.split_refused_observed() == 1),
    "a floor the fork can never clear settles it terminally"
  );
  assert!(
    !w.hosts_group(2, child),
    "a terminally-refused fork materializes nothing"
  );
  assert!(
    w.run_until(8_000, |w| w.hosting_nodes(source).is_empty()),
    "the discharged obligation lets the last host give up its source replica: {:?}",
    w.hosting_nodes(source)
  );
  assert!(
    !w.group_merge_parked(target),
    "the target is no longer parked on the absorbed source: {}",
    w.merge_block_dbg(target)
  );
}

/// A LATE fork — a lagging parent replica applying the committed split after the child's
/// incarnation was retired — must resolve REFUSED at the world's materialization edge, exactly
/// as the product's coordinator admission (floor → tombstone) refuses it at the driver's
/// fork-drain: no materialization, the parent's fence lifted, and the id left to the ordinary
/// lifecycle. Materializing instead squats a replica under a retired gid, and the next
/// recreation trips container admission with `Exists` on that node.
#[test]
fn late_fork_for_a_retired_child_holds_and_recreation_admits() {
  let mut w = MultiWorld::new(5);
  for n in 0..3 {
    w.add_node(n);
  }
  let all: BTreeSet<u64> = (0..3).collect();
  w.create_group(100, &all);
  assert!(w.run_until(3_000, |w| w.leader_of(100).is_some()));
  for key in 0u16..8 {
    let payload = crate::multi::encode_gkv(100, key, u64::from(key));
    propose_until_accepted(&mut w, 100, &payload);
  }
  assert!(
    w.run_until(2_000, |w| (0..3).all(|n| w.applied_of(n, 100).len() >= 8)),
    "the keyed baseline never applied everywhere"
  );

  // Node 2's parent replica lags the whole split: isolate it, then commit the split on {0,1}.
  w.isolate(2);
  assert!(w.run_until(3_000, |w| w.leader_of(100).is_some_and(|l| l != 2)));
  let mut accepted = false;
  for _ in 0..2_000 {
    if let Some(Ok(_)) = w.propose_split(100, 300, 4) {
      accepted = true;
      break;
    }
    w.tick();
  }
  assert!(accepted, "the split was never accepted");
  assert!(
    w.run_until(3_000, |w| w.leader_of(300).is_some()),
    "the forked child never elected: {}",
    w.dbg_group(300)
  );
  assert_eq!(
    w.hosting_nodes(300),
    std::vec![0, 1],
    "the isolated straggler must not have materialized yet"
  );
  assert_eq!(w.splits_applied(), 1);

  // Retire the child while the straggler still has the split entry ahead of it.
  w.remove_group(300);

  // Heal: the straggler catches up, applies the split, and its late fork HOLDS against the
  // tombstone. A tombstone is a window — `recreate_group` lifts it — so it is never grounds to
  // drop the fork's blob; the relay parks it and says so once.
  w.heal(2);
  assert!(
    w.run_until(4_000, |w| w.split_conflicts_observed() >= 1),
    "the late fork never signalled its hold"
  );
  assert_eq!(
    w.split_refused_observed(),
    0,
    "a tombstone must hold the fork, never abandon it"
  );
  assert!(!w.hosts_group(2, 300), "a held fork must not materialize");
  assert_eq!(w.splits_applied(), 1, "a held fork registers nothing");

  // The straggler's fence resolved with the refusal: fresh parent load applies EVERYWHERE.
  propose_until_accepted(&mut w, 100, &crate::multi::encode_gkv(100, 0, 500));
  assert!(
    w.run_until(3_000, |w| {
      (0..3).all(|n| {
        w.applied_of(n, 100)
          .iter()
          .any(|(_, c)| crate::multi::decode_gkv(c) == Some((100, 0, 500)))
      })
    }),
    "the parent must keep applying past a refused fork on every replica"
  );

  // The lifecycle keeps the id: recreation admits cleanly on EVERY voter (a materialized late
  // fork would trip container admission with Exists on the straggler here) and the recreated
  // incarnation elects and commits.
  w.recreate_group(300);
  assert_eq!(w.generation_of(300), 1);
  assert!(
    w.run_until(4_000, |w| w.leader_of(300).is_some()),
    "the recreated child never elected: {}",
    w.dbg_group(300)
  );
  assert_eq!(
    w.split_refused_observed(),
    0,
    "the fork was held throughout, never abandoned"
  );
  propose_until_accepted(&mut w, 300, &crate::multi::encode_gkv(300, 0, 600));
  assert!(
    w.run_until(3_000, |w| {
      let leader = w.leader_of(300);
      leader.is_some_and(|l| {
        w.applied_of(l, 300)
          .iter()
          .any(|(_, c)| crate::multi::decode_gkv(c) == Some((300, 0, 600)))
      })
    }),
    "the recreated incarnation must commit fresh load"
  );
  w.check_now();
  w.finalize_conservation_or_panic(5);
  w.finalize_membership_or_panic(5);
}

/// One settle window exactly as [`MultiWorld::tick`] runs it — flush the coalesced replication
/// batches, then drain outgoing/deliveries/storage/forks until quiescent — WITHOUT the
/// end-of-tick oracle pass, so a test can stage a multi-apply window and observe what the FIRST
/// sweep after it sees.
fn settle_without_sweeping(w: &mut MultiWorld) {
  for node in w.node_list() {
    let host = w.hosts.get_mut(&node).expect("host exists");
    let gids: Vec<u64> = host.group_ids().copied().collect();
    for gid in gids {
      let host = w.hosts.get_mut(&node).expect("host exists");
      let log = w.logs.get(&(node, gid)).expect("replica log");
      let stable = w.stables.get(&(node, gid)).expect("replica stable");
      host
        .flush_appends(&gid, w.now, log, stable)
        .expect("hosted group flushes");
    }
  }
  loop {
    let any_new = w.drain_outgoing_all();
    let delivered = w.deliver_due();
    let storage = w.drain_storage_all();
    let forked = w.pump_forks();
    if !(any_new || delivered || storage || forked) {
      break;
    }
  }
}

/// The conservation walk trusts VALUES, never positions — the reshape band's displaced-cell
/// mechanism pinned deterministically. `LogSm::split` removes moved-key cells record-wide, so a
/// kept-key burst applied in the SAME settle window as the split-apply lands at positions an
/// earlier sweep already passed. A positional resume watermark (even one clamped to the record
/// length) skips those cells forever — an interior hole in the parent's recorded history that a
/// later fork baseline exposes as a false conservation verdict; the full value-deduped walk
/// records them completely.
#[test]
fn conservation_walk_records_cells_displaced_by_a_split_apply() {
  let mut w = MultiWorld::new(23);
  for n in 0..3 {
    w.add_node(n);
  }
  let all: BTreeSet<u64> = (0..3).collect();
  w.create_group(100, &all);
  assert!(w.run_until(3_000, |w| w.leader_of(100).is_some()));
  for key in 0u16..8 {
    let payload = crate::multi::encode_gkv(100, key, u64::from(key));
    propose_until_accepted(&mut w, 100, &payload);
  }
  // Converge every replica onto the identical record so the pre-window length is one number.
  assert!(
    w.run_until(2_000, |w| {
      let a0 = w.applied_of(0, 100);
      let full = a0
        .iter()
        .filter_map(|(_, c)| crate::multi::decode_gkv(c))
        .count()
        >= 8;
      full && (1..3).all(|n| w.applied_of(n, 100) == a0)
    }),
    "the keyed baseline never applied identically everywhere"
  );
  let pre_len = w.applied_of(0, 100).len();

  // ONE window, no sweep inside: the split entry and a kept-key burst behind it commit and
  // apply everywhere before the next sweep looks. Every sweep so far ended at `pre_len`.
  assert!(matches!(w.propose_split(100, 200, 4), Some(Ok(_))));
  for value in 100..104u64 {
    let payload = crate::multi::encode_gkv(100, 0, value);
    assert!(w.propose(100, &payload).is_some(), "burst propose refused");
  }
  for _ in 0..50 {
    settle_without_sweeping(&mut w);
    let burst_applied = |n: u64| {
      w.applied_of(n, 100)
        .iter()
        .any(|(_, c)| crate::multi::decode_gkv(c) == Some((100, 0, 103)))
    };
    if (0..3).all(burst_applied) {
      break;
    }
  }

  // The mechanism armed: on every replica the burst sits BELOW the pre-window record length —
  // the split-apply vacated the moved cells' span and the burst landed inside it.
  for n in 0..3 {
    let applied = w.applied_of(n, 100);
    let pos = applied
      .iter()
      .position(|(_, c)| crate::multi::decode_gkv(c) == Some((100, 0, 100)))
      .unwrap_or_else(|| panic!("node {n}: the burst never applied in the window"));
    assert!(
      pos < pre_len,
      "node {n}: the displaced-burst mechanism did not arm (pos {pos} >= pre_len {pre_len})"
    );
  }

  // The first sweep after the window must record the displaced burst COMPLETELY.
  w.check_now();
  let values: Vec<u64> = w
    .conservation
    .history(100, 0)
    .iter()
    .map(|(_, v)| *v)
    .collect();
  assert_eq!(
    values,
    std::vec![0, 100, 101, 102, 103],
    "key 0's recorded history must hold the full displaced burst"
  );
  w.finalize_conservation_or_panic(23);
  w.finalize_membership_or_panic(23);
}

/// Propose a split of `parent` at `point` into `child`, ticking through transient refusals
/// (leaderless instants, a not-yet-settled prior verb) until a leader accepts it.
fn propose_split_until_accepted(w: &mut MultiWorld, parent: u64, child: u64, point: u16) {
  for _ in 0..2_000 {
    if let Some(Ok(_)) = w.propose_split(parent, child, point) {
      return;
    }
    w.tick();
  }
  panic!("the split of g{parent} at {point} was never accepted");
}

/// Drive g100 (3 voters, one write per key 0..8) into the ACCEPTED-BUT-LOST split shape: a
/// fully isolated leader accepts a split at point 5, the survivors depose it, and healing
/// truncates the entry away. The world's population stays conservatively shrunk — keys 5..8
/// are PARKED, unroutable from then on — while their cells remain in every parent record.
fn world_with_parked_keys(seed: u64) -> MultiWorld {
  let mut w = MultiWorld::new(seed);
  for n in 0..3 {
    w.add_node(n);
  }
  let all: BTreeSet<u64> = (0..3).collect();
  w.create_group(100, &all);
  assert!(w.run_until(3_000, |w| w.leader_of(100).is_some()));
  for key in 0u16..8 {
    let payload = crate::multi::encode_gkv(100, key, u64::from(key));
    propose_until_accepted(&mut w, 100, &payload);
  }
  assert!(
    w.run_until(2_000, |w| {
      (0..3).all(|n| {
        w.applied_of(n, 100)
          .iter()
          .filter_map(|(_, c)| crate::multi::decode_gkv(c))
          .count()
          >= 8
      })
    }),
    "the keyed baseline never applied everywhere"
  );

  // The doomed split: accepted by a leader the world has just fully isolated, so the entry
  // exists only in its log and can never commit.
  let doomed = w.leader_of(100).expect("elected");
  w.isolate(doomed);
  assert!(matches!(w.propose_split(100, 900, 5), Some(Ok(_))));
  assert_eq!(
    w.group_keys_of(100),
    std::vec![0, 1, 2, 3, 4],
    "the population flips at accept"
  );

  // Depose and truncate: the survivors elect a higher term, then fresh committed load lands
  // everywhere — including on the healed ex-leader, over the truncated entry's slot.
  assert!(
    w.run_until(4_000, |w| w.leader_of(100).is_some_and(|l| l != doomed)),
    "the survivors never deposed the isolated leader"
  );
  w.heal(doomed);
  propose_until_accepted(&mut w, 100, &crate::multi::encode_gkv(100, 0, 40));
  assert!(
    w.run_until(3_000, |w| {
      (0..3).all(|n| {
        w.applied_of(n, 100)
          .iter()
          .any(|(_, c)| crate::multi::decode_gkv(c) == Some((100, 0, 40)))
      })
    }),
    "post-depose load never applied everywhere"
  );
  assert!((0..3).all(|n| !w.hosts_group(n, 900)));
  assert_eq!(w.splits_applied(), 0, "a lost split must never register");
  assert!(
    w.pending_splits.contains_key(&900),
    "the lost split's keys stay parked"
  );
  w
}

/// An accepted-but-lost split parks its keys, but their CELLS stay in the parent's record — and
/// `LogSm::split` partitions the RECORD by the instruction, not by the propose-time population.
/// A later split at a lower point therefore moves the parked cells into its child, and the
/// registered assignment must follow that instruction rule: an assignment derived from the
/// population slice alone misreads the parked handover as an unassigned key surfacing in the
/// child (a false conservation verdict against a correct partition).
#[test]
fn split_assignment_follows_parked_keys_through_a_lost_entry() {
  let mut w = world_with_parked_keys(29);

  propose_split_until_accepted(&mut w, 100, 901, 2);
  assert!(
    w.run_until(3_000, |w| w.splits_applied() == 1),
    "the later split never registered"
  );
  assert!(w.run_until(3_000, |w| w.leader_of(901).is_some()));

  // Routing keeps the propose-time slices on both sides; the parked keys stay unroutable.
  assert_eq!(w.group_keys_of(100), std::vec![0, 1]);
  assert_eq!(w.group_keys_of(901), std::vec![2, 3, 4]);
  // The conservation assignment covers every key whose cells the instruction moved.
  assert_eq!(
    w.splits[&901]
      .child_keys
      .iter()
      .copied()
      .collect::<Vec<u16>>(),
    std::vec![2, 3, 4, 5, 6, 7],
    "the assignment must include the parked keys the instruction moved"
  );
  // The parked cells genuinely rode the fork into the child's inherited baseline.
  assert!(
    w.applied_of(0, 901)
      .iter()
      .any(|(_, c)| crate::multi::decode_gkv(c) == Some((100, 5, 5))),
    "the parked key's cell must surface in the child's baseline"
  );

  w.check_now();
  w.finalize_conservation_or_panic(29);
  w.finalize_membership_or_panic(29);
}

/// Parked cells CASCADE: they ride the later child's record, so an onward split moves them
/// AGAIN, and the grandchild's assignment must follow the instruction rule at every generation
/// — the parked handover is re-derived from each fork's own record, never remembered one-shot.
#[test]
fn parked_keys_cascade_through_an_onward_split() {
  let mut w = world_with_parked_keys(31);

  propose_split_until_accepted(&mut w, 100, 901, 2);
  assert!(
    w.run_until(3_000, |w| w.splits_applied() == 1),
    "the intermediate split never registered"
  );
  assert!(w.run_until(3_000, |w| w.leader_of(901).is_some()));

  // Onward: the parked 5/6/7 cells sit at or above point 3 in 901's record, so they move a
  // second time while 901's routing slice hands over {3, 4}.
  propose_split_until_accepted(&mut w, 901, 902, 3);
  assert!(
    w.run_until(3_000, |w| w.splits_applied() == 2),
    "the onward split never registered"
  );
  assert!(w.run_until(3_000, |w| w.leader_of(902).is_some()));

  assert_eq!(w.group_keys_of(901), std::vec![2]);
  assert_eq!(w.group_keys_of(902), std::vec![3, 4]);
  assert_eq!(
    w.splits[&902]
      .child_keys
      .iter()
      .copied()
      .collect::<Vec<u16>>(),
    std::vec![3, 4, 5, 6, 7],
    "the parked keys must flow through the onward assignment"
  );
  // The cascading cell itself: the grandparent-tagged parked write arrived two forks deep.
  assert!(
    w.applied_of(0, 902)
      .iter()
      .any(|(_, c)| crate::multi::decode_gkv(c) == Some((100, 5, 5))),
    "the parked key's cell must cascade into the grandchild's baseline"
  );

  w.check_now();
  w.finalize_conservation_or_panic(31);
  w.finalize_membership_or_panic(31);
}

/// The cross-talk leg closes over the parent-merged-into-child REUNION — specifically over a key
/// the source OWNED but never WROTE. The parent keeps keys 0..=3 (never writing key 0), splits
/// keys 4..=7 to the child, then MERGES BACK INTO that child; the child now owns key 0 and
/// writes it for the first time anywhere. The split never assigned key 0, but the union handed
/// the child the parent's whole POPULATION, so the never-assigned leg exempts it. The exemption
/// reads the merge's transferred population, not its written history: a `keys_of` set would MISS
/// key 0 (no ancestor ever wrote it) and false-trip — this run is green only because the closure
/// reads populations, so a revert to a written-history exemption re-reddens it here.
#[test]
fn partition_exempts_an_owned_unwritten_key_a_union_carried_into_the_child() {
  let mut w = MultiWorld::new(41);
  for n in 0..3 {
    w.add_node(n);
  }
  let all: BTreeSet<u64> = (0..3).collect();
  w.create_group(200, &all);
  assert!(w.run_until(3_000, |w| w.leader_of(200).is_some()));
  w.reconcile_membership(200);
  // Write ONLY keys 4..=7 — the parent keeps 0..=3 in its population but never writes key 0.
  for key in 4u16..8 {
    propose_until_accepted(
      &mut w,
      200,
      &crate::multi::encode_gkv(200, key, u64::from(key)),
    );
  }
  assert!(w.run_until(2_000, |w| {
    w.leader_of(200)
      .is_some_and(|l| w.applied_of(l, 200).len() >= 4)
  }));

  // Split keys 4..=7 to the child; 0..=3 stay on the parent, key 0 still unwritten.
  propose_split_until_accepted(&mut w, 200, 100, 4);
  assert_eq!(w.group_keys_of(200), std::vec![0, 1, 2, 3]);
  assert!(w.run_until(3_000, |w| w.leader_of(100).is_some()));
  assert_eq!(w.group_keys_of(100), std::vec![4, 5, 6, 7]);

  // The parent MERGES BACK into its own child (voter sets align by construction): the child
  // absorbs the whole parent population, key 0 included. Colocate the parent's leadership onto
  // the child's leader first, so the commit barrier reads the source off the target's leader.
  colocate_source_onto_target(&mut w, 200, 100);
  merge_verb_until_accepted(&mut w, 3_000, "the freeze", |w| {
    w.propose_prepare_merge(200, 100)
  });
  merge_verb_until_accepted(&mut w, 4_000, "the commit", |w| {
    w.propose_commit_merge(100, 200)
  });
  assert!(
    w.run_until(8_000, |w| !w.live_groups().contains(&200)),
    "the parent absorbs into its child"
  );
  assert_eq!(w.merges_registered(), 1);

  // The child now OWNS key 0 and writes it for the first time anywhere.
  propose_until_accepted(&mut w, 100, &crate::multi::encode_gkv(100, 0, 900));
  assert!(w.run_until(2_000, |w| {
    w.leader_of(100).is_some_and(|l| {
      w.applied_of(l, 100)
        .iter()
        .any(|(_, c)| crate::multi::decode_gkv(c) == Some((100, 0, 900)))
    })
  }));

  w.check_now();
  w.finalize_conservation_or_panic(41);
  w.finalize_merge_conservation_or_panic(41);
}

/// Tick until `verb` lands on some leader (transient refusals — leaderless instants, catch-up
/// gates — are ticked through), panicking with `what` if the budget runs out.
fn merge_verb_until_accepted(
  w: &mut MultiWorld,
  budget: u32,
  what: &str,
  mut verb: impl FnMut(
    &mut MultiWorld,
  ) -> Option<Result<sailing_proto::Index, sailing_proto::MergeError<u64>>>,
) {
  for _ in 0..budget {
    if matches!(verb(w), Some(Ok(_))) {
      return;
    }
    w.tick();
  }
  panic!("{what} was never accepted");
}

/// Colocate `source`'s leadership onto `target`'s current leader. The commit barrier is
/// observable only on the source LEADER's tracker, so `commit_merge` — proposed on the target's
/// leader — demands that host also lead the source; the band's mergeable pairs are split children
/// colocated by construction, and the standalone teeth arrange it with an explicit transfer. The
/// catch-up to every source voter matching the freeze then rides the commit verb's own retry
/// loop. Idempotent (returns at once when already colocated); panics if the transfer never lands.
fn colocate_source_onto_target(w: &mut MultiWorld, source: u64, target: u64) {
  let host = w
    .leader_of(target)
    .expect("target has a leader to colocate onto");
  for _ in 0..2_000 {
    if w.leader_of(source) == Some(host) {
      return;
    }
    w.transfer_group_leader(source, host);
    w.tick();
  }
  panic!("g{source} leadership never colocated onto g{target}'s leader");
}

/// A claimed merge target cannot itself freeze as a SOURCE — the world form of the stranding
/// class: S (10) freezes claiming T (11); with nothing refusing T source-role, T freezes into T2
/// (12), is absorbed away, and S's release verbs — `commit_merge` and `rollback_merge` both ride
/// T's retired log — return `None` forever: S stranded frozen with no release valve. The gate
/// refuses T's freeze TYPED while S's claim stands (deterministic on the colocated leader, applied
/// or append-pending); S's own choreography then resolves normally, T merges onward freely, and no
/// replica is frozen at the end. Without the `SourceClaimedAsTarget` refusal T's freeze admits and
/// the run strands S.
#[test]
fn a_claimed_merge_target_cannot_freeze_as_a_source() {
  let mut w = MultiWorld::new(13);
  for n in 0..3 {
    w.add_node(n);
  }
  let all: BTreeSet<u64> = (0..3).collect();
  w.create_group(12, &all); // S — the claiming source
  w.create_group(11, &all); // T — S's claimed target
  w.create_group(10, &all); // T2 — the target T tries to dissolve into
  assert!(w.run_until(3_000, |w| {
    w.leader_of(12).is_some() && w.leader_of(11).is_some() && w.leader_of(10).is_some()
  }));
  for key in 0u16..2 {
    propose_until_accepted(
      &mut w,
      12,
      &crate::multi::encode_gkv(12, key, u64::from(key)),
    );
    propose_until_accepted(
      &mut w,
      11,
      &crate::multi::encode_gkv(11, key, 100 + u64::from(key)),
    );
  }

  // S freezes claiming T (leadership colocated so the commit barrier reads off one host).
  colocate_source_onto_target(&mut w, 12, 11);
  merge_verb_until_accepted(&mut w, 3_000, "the claiming freeze", |w| {
    w.propose_prepare_merge(12, 11)
  });

  // THE GATE: T is a claimed target — its own freeze refuses while the claim stands. Equal
  // voter sets colocate S wherever T's propose can run, so the claim is locally visible
  // (applied or still append-pending) and the refusal needs no settling.
  assert!(
    matches!(
      w.propose_prepare_merge(11, 10),
      Some(Err(sailing_proto::MergeError::SourceClaimedAsTarget))
    ),
    "a claimed target must refuse source-role while the claim stands"
  );

  // S's choreography resolves normally: T absorbs S.
  merge_verb_until_accepted(&mut w, 4_000, "the claiming commit", |w| {
    w.propose_commit_merge(11, 12)
  });
  assert!(
    w.run_until(8_000, |w| !w.live_groups().contains(&12)),
    "S absorbs into T"
  );

  // The claim dissolved with S: T now freezes into T2 freely and merges onward.
  colocate_source_onto_target(&mut w, 11, 10);
  merge_verb_until_accepted(&mut w, 3_000, "the freed freeze", |w| {
    w.propose_prepare_merge(11, 10)
  });
  merge_verb_until_accepted(&mut w, 4_000, "the freed commit", |w| {
    w.propose_commit_merge(10, 11)
  });
  assert!(
    w.run_until(8_000, |w| !w.live_groups().contains(&11)),
    "T absorbs into T2 once the claim resolved"
  );

  // NOTHING STRANDED: the absorbed lineages dismantle everywhere and the union is not frozen.
  assert!(
    w.run_until(8_000, |w| {
      (0..3u64).all(|n| w.hosts[&n].group(&12).is_none() && w.hosts[&n].group(&11).is_none())
    }),
    "the absorbed lineages dismantle on every host — nothing lingers frozen"
  );
  for n in 0..3u64 {
    assert!(
      w.hosts[&n]
        .group(&10)
        .is_none_or(|ep| !sailing_proto::Endpoint::is_frozen(ep)),
      "the surviving union is not frozen on n{n}"
    );
  }
  assert_eq!(w.merges_registered(), 2, "both merges resolved as absorbs");
  w.finalize_merge_conservation_or_panic(13);
}

/// A 2-CYCLE IS UNCONSTRUCTIBLE. The direction rule refuses the wrong-direction claim at propose
/// (`DirectionInverted`), so two equal-voter groups can never both freeze claiming EACH OTHER — the
/// mutual-`AlreadyFrozen` deadlock in which every release valve is wedged. The verdict is a
/// constant of the id pair, decided locally with no network round, so it holds under any concurrent
/// admission pressure. Without the direction rule both freezes admit and each group is frozen
/// claiming the other, with no valve to release either.
#[test]
fn no_two_cycle_can_form_under_concurrent_admission() {
  let mut w = MultiWorld::new(51);
  for n in 0..3 {
    w.add_node(n);
  }
  let all: BTreeSet<u64> = (0..3).collect();
  w.create_group(20, &all);
  w.create_group(21, &all);
  assert!(w.run_until(3_000, |w| w.leader_of(20).is_some()
    && w.leader_of(21).is_some()));
  // The UP-order claim (encoding-smaller source) refuses TYPED, independent of the network — a
  // constant of the pair. This is the leg that makes the 2-cycle unconstructible.
  assert!(
    matches!(
      w.propose_prepare_merge(20, 21),
      Some(Err(sailing_proto::MergeError::DirectionInverted))
    ),
    "the up-order claim refuses DirectionInverted"
  );
  // The oriented (down-order) claim is the only admissible one; only one side can ever freeze.
  colocate_source_onto_target(&mut w, 21, 20);
  merge_verb_until_accepted(&mut w, 3_000, "the oriented freeze", |w| {
    w.propose_prepare_merge(21, 20)
  });
  assert!(
    !w.group_frozen(20),
    "the target never froze claiming the source — no mutual freeze, no 2-cycle"
  );
  // The reverse claim STILL refuses by direction (checked ahead of every state gate).
  assert!(
    matches!(
      w.propose_prepare_merge(20, 21),
      Some(Err(sailing_proto::MergeError::DirectionInverted))
    ),
    "the reverse claim stays refused — the cycle can never close"
  );
}

/// A 3-CYCLE IS UNCONSTRUCTIBLE. A cycle needs every edge to strictly DECREASE one total order, which
/// none can close: over three equal-voter groups the two up-order edges of the cyclic claim set
/// 10→11→12→10 refuse `DirectionInverted` at propose, and only the single down-order edge (12→10) is
/// admissible. Whatever the schedule (even a host that admitted an earlier edge without observing the
/// others), the closing up-order edge is refused — so the deadlock-every-valve cycle cannot form.
#[test]
fn no_three_cycle_can_form() {
  let mut w = MultiWorld::new(53);
  for n in 0..3 {
    w.add_node(n);
  }
  let all: BTreeSet<u64> = (0..3).collect();
  for g in [10u64, 11, 12] {
    w.create_group(g, &all);
  }
  assert!(w.run_until(3_000, |w| {
    [10u64, 11, 12].iter().all(|&g| w.leader_of(g).is_some())
  }));
  // Two of the three cyclic edges point UP the order — both refuse DirectionInverted (a local
  // constant), so the cycle can never close no matter which host proposes which edge when.
  assert!(matches!(
    w.propose_prepare_merge(10, 11),
    Some(Err(sailing_proto::MergeError::DirectionInverted))
  ));
  assert!(matches!(
    w.propose_prepare_merge(11, 12),
    Some(Err(sailing_proto::MergeError::DirectionInverted))
  ));
  // Only the single DOWN-order edge is admissible; the cycle has no closing edge.
  colocate_source_onto_target(&mut w, 12, 10);
  merge_verb_until_accepted(&mut w, 3_000, "the one down-order edge", |w| {
    w.propose_prepare_merge(12, 10)
  });
  assert!(
    w.run_until(3_000, |w| w.group_frozen(12)),
    "only the down-order claim froze — no cyclic claim set could form"
  );
}

/// THE COMMIT BARRIER ELIMINATES THE CROSS-HOST ROLLBACK RACE. The source cannot dissolve until
/// every source voter has matched the freeze, so a commit is never admitted while a voter lags —
/// the exact window a live generation read once used to SPLIT the hosts (caught-up hosts absorbed
/// while the lagged host aborted — committed divergence) is now unreachable. The teeth: with a
/// voter lagged the commit is HELD (refused), so no committed coordinate exists for the hosts to
/// diverge over; once it heals the commit admits and, racing an abort onto the very next slot,
/// every host reads that one coordinate and un-parks on the SAME side (aborted), after which the
/// relayed thaw unfreezes the source everywhere and a fresh attempt merges for real.
#[test]
fn merge_rollback_race_decides_identically_across_hosts() {
  let mut w = MultiWorld::new(9);
  for n in 0..3 {
    w.add_node(n);
  }
  let all: BTreeSet<u64> = (0..3).collect();
  w.create_group(11, &all); // source
  w.create_group(10, &all); // target
  assert!(w.run_until(2_000, |w| {
    w.leader_of(11).is_some() && w.leader_of(10).is_some()
  }));
  for i in 0..2u8 {
    assert!(w.run_until(500, |w| w.leader_of(11).is_some()));
    let gid_cmd = [b's', i];
    w.propose(11, &gid_cmd);
    let gid_cmd = [b't', i];
    w.propose(10, &gid_cmd);
    w.run_until(200, |_| false);
  }
  // The barrier is read off the source LEADER, which must sit on the target's leader.
  colocate_source_onto_target(&mut w, 11, 10);

  // Lag one host's SOURCE replica only (a host leading neither group, so the freeze still commits
  // and the colocated leader still reaches the boundary): the freeze will not reach it until the
  // heal. Only group 11 is muted, so the target's leadership — the colocation anchor — never moves.
  let lagged = (0..3u64)
    .find(|n| Some(*n) != w.leader_of(11) && Some(*n) != w.leader_of(10))
    .expect("three nodes, at most two leaders");
  for n in 0..3 {
    if n != lagged {
      w.mute_group(n, lagged, 11);
      w.mute_group(lagged, n, 11);
    }
  }
  merge_verb_until_accepted(&mut w, 2_000, "the freeze", |w| {
    w.propose_prepare_merge(11, 10)
  });
  assert!(
    w.run_until(4_000, |w| {
      (0..3).filter(|&n| n != lagged).all(|n| {
        w.hosts[&n]
          .group(&11)
          .is_some_and(sailing_proto::Endpoint::is_frozen)
      })
    }),
    "the connected majority freezes; the lagged replica stays behind"
  );
  assert!(
    !w.hosts[&lagged]
      .group(&11)
      .is_some_and(sailing_proto::Endpoint::is_frozen),
    "the lagged source replica must not have seen the freeze"
  );

  // THE NEW GUARANTEE that retires the race: with a source voter short of the boundary the commit
  // is HELD — refused on the colocated, frozen-applied leader — so the source can never dissolve
  // with a voter still behind, and there is no committed coordinate for the hosts to split over.
  assert!(
    matches!(
      w.propose_commit_merge(10, 11),
      Some(Err(sailing_proto::MergeError::SourceBarrierPending))
    ),
    "the commit is refused while a source voter lags the freeze"
  );

  // Heal: the lagged voter catches up, the (idempotent) colocation re-settles group 11 onto the
  // still-stable target leader, and the commit admits — then the abort races onto the very next
  // slot BEFORE any tick can seal (the leader accepted the commit, so its gates pass here too).
  w.unmute_all();
  colocate_source_onto_target(&mut w, 11, 10);
  merge_verb_until_accepted(&mut w, 4_000, "the commit", |w| {
    w.propose_commit_merge(10, 11)
  });
  assert!(
    matches!(w.propose_rollback_merge(10, 11), Some(Ok(_))),
    "the abort races onto the coordinate right after the commit"
  );

  // Settle: every park must take the ABORT side (one committed coordinate, one verdict), the
  // relayed thaw must unfreeze the source everywhere, and nothing may absorb.
  assert!(
    w.run_until(6_000, |w| {
      (0..3).all(|n| {
        w.hosts[&n]
          .group(&11)
          .is_some_and(|ep| !ep.is_frozen() && ep.shape_gen() == 2)
          && w.hosts[&n]
            .group(&10)
            .is_some_and(|ep| ep.pending_merge().is_none() && ep.shape_gen() == 1)
      })
    }),
    "every host un-parks aborted and the relayed thaw lands everywhere"
  );
  assert_eq!(w.merges_registered(), 0, "nothing absorbed anywhere");
  assert!(w.agreement_holds(11) && w.agreement_holds(10));

  // The state is clean enough to merge for real: the same pair completes end to end.
  colocate_source_onto_target(&mut w, 11, 10);
  merge_verb_until_accepted(&mut w, 2_000, "the fresh freeze", |w| {
    w.propose_prepare_merge(11, 10)
  });
  merge_verb_until_accepted(&mut w, 4_000, "the fresh commit", |w| {
    w.propose_commit_merge(10, 11)
  });
  assert!(
    w.run_until(8_000, |w| (0..3).all(|n| !w.hosts_group(n, 11))),
    "the second attempt absorbs on every host"
  );
  assert_eq!(w.merges_registered(), 1);
  assert!(w.is_merged(11), "the source id is terminally merged away");
}

/// THE LATE ABORT NO-OPS AFTER THE SEAL, and the barrier keeps the seal honest. A source voter
/// held below the freeze can no longer let the absorb begin — the commit is HELD until it catches
/// up — so once the commit finally admits and a first host resolves, the window is sealed: a late
/// abort proposed past it changes NOTHING and every host converges on the absorb. Under the old
/// live-generation read a landed late abort could still split the hosts; the barrier forecloses it.
#[test]
fn merge_late_abort_no_ops_and_every_host_absorbs() {
  let mut w = MultiWorld::new(11);
  for n in 0..3 {
    w.add_node(n);
  }
  let all: BTreeSet<u64> = (0..3).collect();
  w.create_group(11, &all);
  w.create_group(10, &all);
  assert!(w.run_until(2_000, |w| {
    w.leader_of(11).is_some() && w.leader_of(10).is_some()
  }));
  w.propose(11, b"s0");
  w.propose(10, b"t0");
  w.run_until(200, |_| false);
  colocate_source_onto_target(&mut w, 11, 10);

  let lagged = (0..3u64)
    .find(|n| Some(*n) != w.leader_of(11) && Some(*n) != w.leader_of(10))
    .expect("three nodes, at most two leaders");
  for n in 0..3 {
    if n != lagged {
      w.mute_group(n, lagged, 11);
      w.mute_group(lagged, n, 11);
    }
  }
  merge_verb_until_accepted(&mut w, 2_000, "the freeze", |w| {
    w.propose_prepare_merge(11, 10)
  });
  assert!(w.run_until(4_000, |w| {
    (0..3).filter(|&n| n != lagged).all(|n| {
      w.hosts[&n]
        .group(&11)
        .is_some_and(sailing_proto::Endpoint::is_frozen)
    })
  }));
  // The barrier HOLDS the commit while a source voter lags: the absorb cannot begin behind a
  // straggler, so no half-sealed window exists for a late abort to race into divergence.
  assert!(
    matches!(
      w.propose_commit_merge(10, 11),
      Some(Err(sailing_proto::MergeError::SourceBarrierPending))
    ),
    "the commit is refused while a source voter lags the freeze"
  );

  // Heal, re-colocate, and let the commit admit; run until the absorb has begun somewhere (a
  // first resolution) — the window is sealed and the merge is past its last abortable coordinate.
  w.unmute_all();
  colocate_source_onto_target(&mut w, 11, 10);
  merge_verb_until_accepted(&mut w, 4_000, "the commit", |w| {
    w.propose_commit_merge(10, 11)
  });
  assert!(
    w.run_until(6_000, |w| w.merges_registered() >= 1),
    "some host absorbs once the barrier admits the commit"
  );
  // The late abort: wherever it is still proposable it lands ABOVE the seal and must no-op;
  // where the local view already resolved, it refuses typed. Either way it changes nothing.
  let late = w.propose_rollback_merge(10, 11);
  assert!(late.is_some(), "the target group is still hosted somewhere");
  assert!(
    w.run_until(8_000, |w| (0..3).all(|n| !w.hosts_group(n, 11))),
    "every host absorbs — the sealed merge was irrevocable"
  );
  assert_eq!(w.merges_registered(), 1, "one absorb, registered once");
  assert!(w.is_merged(11));
  assert!(w.agreement_holds(10));
}

/// The lifecycle churn's "spoken for" predicate tracks a choreography end to end: the source
/// reads active from the freeze's ACCEPTANCE (appended, not yet applied — the unapplied-suffix
/// leg), the target from the commit's acceptance, both until the absorb resolves everywhere —
/// and an uninvolved bystander never does. This is the remove/recreate draw filter's exact
/// contract; the seed-0 band livelock was a removal drawn inside exactly this window.
#[test]
fn choreography_participants_read_active_until_resolution() {
  let mut w = MultiWorld::new(13);
  for n in 0..3 {
    w.add_node(n);
  }
  let all: BTreeSet<u64> = (0..3).collect();
  w.create_group(11, &all); // source
  w.create_group(10, &all); // target
  w.create_group(12, &all); // bystander
  assert!(w.run_until(3_000, |w| {
    w.leader_of(11).is_some() && w.leader_of(10).is_some() && w.leader_of(12).is_some()
  }));
  assert!(
    !w.merge_choreography_active(11) && !w.merge_choreography_active(10),
    "no choreography yet — everything is drawable"
  );
  // Colocate before the freeze so the commit barrier reads the source off the target's leader; a
  // bare leadership transfer speaks for nobody, so the choreography predicate stays quiet.
  colocate_source_onto_target(&mut w, 11, 10);
  merge_verb_until_accepted(&mut w, 2_000, "the freeze", |w| {
    w.propose_prepare_merge(11, 10)
  });
  assert!(
    w.merge_choreography_active(11),
    "an accepted (still unapplied) freeze already speaks for the source"
  );
  assert!(
    w.merge_choreography_active(10),
    "the freeze already CLAIMS target 10 (pre-park, append-pending) — the mirror leg keeps it \
     undrawable, so a removal never trips the container's Claimed gate"
  );
  assert!(
    !w.merge_choreography_active(12),
    "the bystander 12 is claimed by no freeze"
  );
  assert!(w.run_until(4_000, |w| w.group_frozen(11)));
  assert!(
    w.merge_choreography_active(11),
    "a frozen source stays spoken for"
  );
  assert!(
    w.merge_choreography_active(10),
    "the APPLIED freeze still claims 10 through to the commit — no gap in the mirror leg"
  );
  merge_verb_until_accepted(&mut w, 4_000, "the commit", |w| {
    w.propose_commit_merge(10, 11)
  });
  assert!(
    w.merge_choreography_active(10),
    "an accepted commit speaks for the target through park and resolution"
  );
  assert!(
    w.run_until(8_000, |w| (0..3).all(|n| !w.hosts_group(n, 11))),
    "the merge resolves on every host"
  );
  assert!(
    !w.merge_choreography_active(10),
    "a fully resolved target is drawable again"
  );
  assert!(
    !w.merge_choreography_active(11),
    "an absorbed source is past its choreography (and terminally merged besides)"
  );
  assert!(
    !w.merge_choreography_active(12),
    "the bystander was never spoken for"
  );
}

/// The departed sweep respects an active merge choreography. A virgin non-member wired onto a
/// merge participant has no applied membership — the product refuses its conf change for the whole
/// window (a frozen source, a parked target) — so the ungated sweep would PARK it after the grace,
/// sub-quorum-ing the participant into a freeze wedge. The sweep reads `merge_choreography_active`
/// (true across this whole window) and ZEROES the absent-streak instead: the harness keeps a
/// choreography's participants in place until it resolves — the same embedder contract the gated
/// grow draw honors when it skips the observer-wire, extended to the sweep for a replica wired before
/// the freeze. Without the `|| choreography_active` guard in the sweep the virgin crosses the grace
/// and parks.
#[test]
fn a_virgin_non_member_on_a_frozen_participant_is_spared_by_the_sweep() {
  let mut w = MultiWorld::new(7);
  for n in 0..4 {
    w.add_node(n);
  }
  let voters: BTreeSet<u64> = (0..3).collect();
  w.create_group(11, &voters); // the merge source
  w.create_group(10, &voters); // its target
  assert!(w.run_until(3_000, |w| w.leader_of(11).is_some()
    && w.leader_of(10).is_some()));
  merge_verb_until_accepted(&mut w, 2_000, "the freeze", |w| {
    w.propose_prepare_merge(11, 10)
  });
  assert!(w.run_until(4_000, |w| w.group_frozen(11)));
  assert!(
    w.merge_choreography_active(11),
    "the sweep's gate reads the source active across this whole window"
  );
  // Wire a virgin non-member (an orphan the grow draw would refuse) onto the frozen source, then
  // reconcile well past the grace: the sweep must never park it while the choreography is active —
  // and the source's own voters are spared just the same.
  w.wire_group_observer(11, 3);
  for _ in 0..8 {
    w.reconcile_membership(11);
    assert!(
      !w.parked.contains(&(3, 11)),
      "the sweep must not park a frozen participant's replica mid-choreography"
    );
    for n in 0..3u64 {
      assert!(
        !w.parked.contains(&(n, 11)),
        "the frozen source's voter {n} must not park mid-choreography"
      );
    }
  }
  assert!(
    w.merge_choreography_active(11),
    "the window stayed open throughout — the sweep spared the participant for all of it"
  );
}

/// Fixture for the fault-born-refusal teeth: a merge SOURCE (11) frozen-PENDING toward target (10),
/// both led on node 0, beside a single-voter claim-free BYSTANDER (100) hosted only on node 0. The
/// freeze is left APPEND-pending (unapplied) so the container's `Claimed` gate reaches it through
/// the append-pending log SCAN — the one leg that reads a faultable store path — when the bystander
/// is torn down on node 0. `merge_choreography_active(100)` reads false: no freeze names 100.
fn freeze_pending_beside_bystander(seed: u64) -> MultiWorld {
  let mut w = MultiWorld::new(seed);
  for n in 0..3 {
    w.add_node(n);
  }
  let all: BTreeSet<u64> = (0..3).collect();
  w.create_group(10, &all); // target
  w.create_group(11, &all); // source
  let just_zero: BTreeSet<u64> = std::iter::once(0).collect();
  w.create_group(100, &just_zero); // single-voter bystander, hosted only on node 0
  assert!(w.run_until(3_000, |w| w.leader_of(10).is_some()
    && w.leader_of(11).is_some()
    && w.leader_of(100).is_some()));
  // Land both participants' leadership on node 0, so the append-pending freeze appends on the
  // bystander's only host. 100 is single-voter on 0, so these transfers never unseat it.
  for gid in [10u64, 11] {
    let mut landed = false;
    for _ in 0..3_000 {
      if w.leader_of(gid) == Some(0) {
        landed = true;
        break;
      }
      w.transfer_group_leader(gid, 0);
      w.tick();
    }
    assert!(landed, "g{gid} leadership never landed on node 0");
  }
  // Freeze 11 -> 10, accepted (append-pending). NO ticks afterward: it stays unapplied, so the
  // container reads its claim through the append-pending `Claimed` leg's faultable log scan.
  merge_verb_until_accepted(&mut w, 3_000, "the freeze", |w| {
    w.propose_prepare_merge(11, 10)
  });
  assert_eq!(
    w.leader_of(11),
    Some(0),
    "the freeze must remain led by (and appended on) node 0"
  );
  assert!(
    w.merge_choreography_active(11) && !w.group_frozen(11),
    "source 11 must be freeze-PENDING (append-observed, not yet applied)"
  );
  assert!(
    !w.merge_choreography_active(100),
    "the bystander 100 is claimed by no freeze"
  );
  w
}

/// Land source 11's append-pending freeze claiming target 10 on node 2 ONLY, with 10 a 3-voter group
/// on {0,1,2}. Leadership of 11 is transferred to node 2 and the freeze accepted with NO ticks after,
/// so the `PrepareMerge` stays unapplied AND unreplicated — only node 2's source replica carries the
/// claim. A teardown of 10 then ADMITS on nodes 0 and 1 (their 11 replica has no freeze) and REFUSES
/// on node 2: the partial-admit topology the soak's seed-18 residual hits, exercising the atomic
/// rollback.
fn freeze_pending_led_on_node2(seed: u64) -> MultiWorld {
  let mut w = MultiWorld::new(seed);
  for n in 0..3 {
    w.add_node(n);
  }
  let all: BTreeSet<u64> = (0..3).collect();
  w.create_group(10, &all); // target
  w.create_group(11, &all); // source
  assert!(w.run_until(3_000, |w| w.leader_of(10).is_some()
    && w.leader_of(11).is_some()));
  // Land 11's leadership on node 2 so its append-pending freeze appends there and nowhere else.
  let mut landed = false;
  for _ in 0..3_000 {
    if w.leader_of(11) == Some(2) {
      landed = true;
      break;
    }
    w.transfer_group_leader(11, 2);
    w.tick();
  }
  assert!(landed, "g11 leadership never landed on node 2");
  // Freeze 11 -> 10, accepted (append-pending on node 2). NO ticks afterward: unapplied AND
  // unreplicated, so only node 2's source replica carries the claim.
  merge_verb_until_accepted(&mut w, 3_000, "the freeze", |w| {
    w.propose_prepare_merge(11, 10)
  });
  assert_eq!(
    w.leader_of(11),
    Some(2),
    "the freeze must remain led by (and appended on) node 2"
  );
  assert!(
    w.merge_choreography_active(11) && !w.group_frozen(11),
    "source 11 must be freeze-PENDING (append-observed, not yet applied)"
  );
  w
}

/// A transient read fault fires on a co-hosted freeze-pending source's log while a claim-free
/// bystander is torn down, so the container's `Claimed` scan fails CLOSED (correct product
/// conservatism). The WORLD must read its OWN teardown scan FAULT-FREE: a harness that lets the
/// armed fault reach that scan takes the closed verdict for a teardown failure and panics in its
/// `.expect`. Reading it fault-free, the removal lands ATOMICALLY despite the armed fault — never a
/// partial teardown that would drop committed voters' durable logs and strand the survivors below
/// quorum. The scan proves no claim on the bystander (source 11 names target 10, not 100).
#[test]
fn a_claim_free_teardown_reads_fault_free_and_lands() {
  let mut w = freeze_pending_beside_bystander(13);
  let armed = crate::StorageFaults {
    transient_read_per_mille: 1000, // every read of source 11's log fails closed
    ..crate::StorageFaults::none()
  };
  w.logs
    .get_mut(&(0, 11))
    .expect("source 11 log on node 0")
    .set_faults(armed, 0xF);
  // A harness whose teardown scan is itself subject to the armed fault panics inside the
  // container-removal `.expect` when that scan fails closed.
  assert!(
    w.remove_group(100),
    "the removal lands atomically — the fault-free scan admits the claim-free teardown"
  );
  assert!(!w.hosts_group(0, 100), "the bystander replica is gone");
  assert_eq!(w.retired_checkers(), 1, "the bystander retired");
}

/// Teeth (real claim): a genuinely claimed target — an APPLIED freeze names it — is NEVER swallowed.
/// `merge_choreography_active(10)` reads TRUE, so the removal is the legitimate choreography-active
/// case the draw filters upstream: the container refuses and the world ABANDONS as a retryable no-op
/// (resetting the tie streak), never tearing down the claimed target. (The removal DRAW excludes such
/// a target; this calls it directly to prove the guard is not silent about the refusal.)
#[test]
fn a_genuinely_claimed_target_is_never_swallowed() {
  let mut w = MultiWorld::new(13);
  for n in 0..3 {
    w.add_node(n);
  }
  let all: BTreeSet<u64> = (0..3).collect();
  w.create_group(11, &all); // source
  w.create_group(10, &all); // target
  assert!(w.run_until(3_000, |w| w.leader_of(11).is_some()
    && w.leader_of(10).is_some()));
  colocate_source_onto_target(&mut w, 11, 10);
  merge_verb_until_accepted(&mut w, 3_000, "the freeze", |w| {
    w.propose_prepare_merge(11, 10)
  });
  assert!(
    w.run_until(4_000, |w| w.group_frozen(11)),
    "the freeze applies (the in-memory `frozen_for` claim leg, no faultable scan)"
  );
  assert!(
    w.merge_choreography_active(10),
    "target 10 is genuinely claimed by the frozen source"
  );
  assert!(
    !w.remove_group(10),
    "the claimed target's removal is abandoned, not committed"
  );
  for n in 0..3 {
    assert!(
      w.hosts_group(n, 10),
      "node {n}'s claimed target replica survives untouched"
    );
  }
}

/// A forgotten claim is a RETRYABLE NO-OP, not a panic: the world's
/// `active_freezes` record is dropped so `merge_choreography_active(10)` reads FALSE (the
/// replication-lag residual — a co-hosted source's stale log still claims 10 after the book moved
/// on), yet the fault-free append-pending scan still decodes source 11's genuine claim on target 10.
/// One draw abandons and leaves 10 fully hosted; a real embedder retries once the lag clears.
#[test]
fn a_forgotten_claim_is_a_retryable_no_op() {
  let mut w = freeze_pending_beside_bystander(13);
  w.active_freezes.remove(&11);
  assert!(
    !w.merge_choreography_active(10),
    "the world has FORGOTTEN the freeze — its superset now misses the claim"
  );
  // The container's fault-free scan still decodes source 11's append-pending claim on 10.
  assert!(
    !w.remove_group(10),
    "the residual refusal abandons the draw"
  );
  for n in 0..3 {
    assert!(
      w.hosts_group(n, 10),
      "node {n}'s target replica is untouched by the no-op"
    );
  }
  // The rolled-back world stays quorum-durable (nothing was dropped).
  w.check_now();
}

/// Teeth (a persistent forgotten claim): the escalation bound keeps the tooth. In this frozen fixture
/// no tick ever clears the claim, so it is NOT transient — each draw abandons and bumps the per-gid
/// streak until it passes `TEARDOWN_TIE_BUDGET` and trips with both verdicts, exactly as a genuine
/// world-predicate hole would.
#[test]
#[should_panic(expected = "a REAL teardown-gate tie")]
fn a_persistent_forgotten_claim_escalates_and_trips() {
  let mut w = freeze_pending_beside_bystander(13);
  w.active_freezes.remove(&11);
  for _ in 0..=super::lifecycle::TEARDOWN_TIE_BUDGET {
    assert!(
      !w.remove_group(10),
      "each residual draw abandons until the budget is passed"
    );
  }
  unreachable!("the budget-passing draw panics");
}

/// The atomic-rollback teeth (the soak's seed-18 residual topology): source 11's append-pending
/// freeze claiming target 10 lands on node 2 ONLY, so tearing 10 down ADMITS on nodes 0 and 1 (their
/// 11 replica carries no freeze) and REFUSES on node 2. The world restores the two admitted teardowns
/// from their retained durable stores and abandons — never a PARTIAL teardown that would strand 10's
/// survivors below quorum. `merge_choreography_active(10)` is FORGOTTEN so the residual escalation
/// path (not the choreography-active branch) drives the rollback.
#[test]
fn a_partial_admit_teardown_rolls_back_atomically() {
  let mut w = freeze_pending_led_on_node2(13);
  w.active_freezes.remove(&11);
  assert!(
    !w.merge_choreography_active(10),
    "the world's superset misses the node-2 append-pending claim"
  );
  assert!(
    !w.remove_group(10),
    "the partial-admit teardown rolls back and abandons"
  );
  for n in 0..3 {
    assert!(
      w.hosts_group(n, 10),
      "node {n}'s target replica survives the atomic rollback"
    );
  }
  // Every replica kept its durable log, so the quorum-durability oracle stays green.
  w.check_now();
}

/// A ROLLBACK RESTORE CONSUMES A BOOT EPOCH, exactly as a crash restore and a nonzero founding do.
///
/// The rollback is the THIRD writer of this counter, and reusing the outgoing epoch here would be
/// wrong for a reason that is not about in-flight messages: it is about OP IDS. The partial
/// teardown below aborts at node
/// 2's `Claimed` refusal and rolls nodes 0 and 1 back — potentially before their founding
/// completion drained. `discard_inflight` drops only the UNFLUSHED window, so a flushed completion
/// survives it; restored at the same epoch, that retained acknowledgment sits at exactly the
/// `(epoch, 0)` the rebuilt endpoint's first submission mints, and a torn next flush lets a dead
/// incarnation's completion satisfy a live undurable write.
///
/// The pin is the counter, in both halves: the rolled-back nodes must have consumed an epoch, and
/// the node that never gave one up must not have.
#[test]
fn a_rollback_restore_consumes_a_boot_epoch() {
  let mut w = freeze_pending_led_on_node2(13);
  w.active_freezes.remove(&11);
  let before: Vec<u64> = (0..3)
    .map(|n| w.boot_epochs.get(&n).copied().unwrap_or(0))
    .collect();
  assert!(
    !w.remove_group(10),
    "the partial-admit teardown rolls back and abandons"
  );
  let after: Vec<u64> = (0..3)
    .map(|n| w.boot_epochs.get(&n).copied().unwrap_or(0))
    .collect();
  for n in 0..2usize {
    assert!(
      after[n] > before[n],
      "node {n}'s replica was torn down and restored from its retained stores, so the restore owes \
       a NEW epoch; the counter went {} -> {}. An epoch handed out twice lets the outgoing \
       incarnation's retained completion sit at the same (epoch, 0) the rebuilt endpoint's first \
       submission mints",
      before[n],
      after[n]
    );
  }
  assert_eq!(
    after[2], before[2],
    "node 2 REFUSED the teardown, so its replica was never removed and never restored — nothing \
     there may consume an epoch"
  );
}

/// A DEAD-END merge-abort obligation clears CLUSTER-WIDE off a committed `ThawDischarged` witness.
/// Source S owes a target-role thaw to an upstream group U (S was the aborted TARGET of U→S, so it
/// carries `abandoned[U]`). On a host that holds S but NEVER U, that host can neither drive U's thaw
/// (no local U stores) nor observe U's lineage advance (U unhosted, floor 0, lineage 0) — the ghost
/// obligation. Once U thaws, S's LEADER (an observer: it hosts U and sees `shape_gen(U) > expected`,
/// a GLOBAL proof) appends a `ThawDischarged` witness on S's own log; every S replica's apply clears
/// `abandoned[U]` — so node 2 reaches `!has_abandoned()` WITHOUT ever hosting U, the observer-led
/// self-heal. (The drivability belt — dissolving S drops a dead-end obligation the absorb never
/// needed — remains the fallback when no observer leads S; either mechanism keeps the co-hosted
/// S→T absorb from wedging.)
///
/// The construction is the only one that reaches the bug — every single-container path purges or
/// discharges the obligation first. U and S on {0,1} freeze+abort so S carries `abandoned[U]` on
/// both; U is muted so its thaw cannot run and the abort entry stays uncompacted on S's leader; node
/// 2 then JOINS S as a voter, catching that abort entry up by append and re-deriving `abandoned[U]`
/// though it never hosts U; U is unmuted and thaws, discharging the obligation on {0,1} but never on
/// node 2 (unhosted source, floor 0, lineage 0 — the dead end). S then freezes into T on a discharged
/// leader (clearing the `SourceOwesThaw` gate) and every host must resolve the absorb, node 2 too.
#[test]
fn a_dead_end_obligation_does_not_wedge_a_co_hosted_absorb() {
  // Ids chosen so each claim points down the id order: U (source) > S > T (final target).
  const U: u64 = 32;
  const S: u64 = 31;
  const T: u64 = 30;
  let mut w = MultiWorld::new(9);
  for n in 0..3 {
    w.add_node(n);
  }
  let uf: BTreeSet<u64> = [0, 1].into_iter().collect();
  let all: BTreeSet<u64> = (0..3).collect();
  w.create_group(U, &uf); // the upstream source, on {0,1} only — never hosted on node 2
  w.create_group(S, &uf); // its target; grows to {0,1,2}, carrying the obligation
  w.create_group(T, &all); // the eventual absorb target, on every node
  assert!(w.run_until(3_000, |w| {
    w.leader_of(U).is_some() && w.leader_of(S).is_some() && w.leader_of(T).is_some()
  }));

  // U freezes into S (equal voter sets {0,1}); capture U's frozen generation for the thaw check.
  merge_verb_until_accepted(&mut w, 2_000, "the U→S freeze", |w| {
    w.propose_prepare_merge(U, S)
  });
  assert!(w.run_until(4_000, |w| {
    [0u64, 1]
      .iter()
      .all(|&n| w.hosts[&n].group(&U).is_some_and(|ep| ep.is_frozen()))
  }));
  let frozen_gen = w.hosts[&0].group(&U).expect("U hosted on 0").shape_gen();

  // Mute U so its thaw cannot commit: the obligation stays live and the abort entry stays uncompacted
  // on S's leader (its compaction fence held by the live obligation), so the joiner below catches the
  // entry up by APPEND and re-derives abandoned[U] rather than installing a snapshot past it.
  w.mute_group(0, 1, U);
  w.mute_group(1, 0, U);

  // S aborts the U→S merge as the TARGET: every S replica records abandoned[U].
  merge_verb_until_accepted(&mut w, 2_000, "the U→S abort", |w| {
    w.propose_rollback_merge(S, U)
  });
  assert!(w.run_until(4_000, |w| {
    [0u64, 1]
      .iter()
      .all(|&n| w.hosts[&n].group(&S).is_some_and(|ep| ep.has_abandoned()))
  }));

  // Node 2 JOINS S as a voter, catching up the abort entry and re-deriving abandoned[U] though it
  // never hosts U — the dead-end host the bug needs (holds S and T, but not U).
  w.wire_group_observer(S, 2);
  propose_conf_change_until_accepted(&mut w, S, sailing_proto::ConfChangeType::AddNode, 2);
  assert!(
    w.run_until(6_000, |w| {
      w.hosts_group(2, S) && w.hosts[&2].group(&S).is_some_and(|ep| ep.has_abandoned())
    }),
    "node 2 must join S and re-derive the abandoned[U] obligation"
  );
  w.reconcile_membership(S);

  // Unmute: U thaws on {0,1}, and S's observer leader appends a `ThawDischarged` witness that clears
  // `abandoned[U]` on EVERY S replica — node 2 included, which can neither drive nor observe U. The
  // ghost self-heals cluster-wide.
  w.unmute_all();
  assert!(
    w.run_until(8_000, |w| {
      [0u64, 1]
        .iter()
        .all(|&n| w.hosts[&n].group(&U).is_some_and(|ep| !ep.is_frozen()))
        && (0..3).all(|n| w.hosts[&n].group(&S).is_some_and(|ep| !ep.has_abandoned()))
    }),
    "U must thaw and the witness must discharge the obligation on EVERY S replica"
  );

  // THE CLUSTER-WIDE CLEAR the witness delivers — node 2 discharges WITHOUT ever hosting U.
  assert!(w.hosts_group(2, S), "node 2 hosts S");
  assert!(
    !w.hosts_group(2, U),
    "node 2 never hosts U — the obligation was a local dead end there"
  );
  assert!(
    !w.hosts[&2].group(&S).expect("S on 2").has_abandoned(),
    "node 2's ghost obligation cleared off the committed witness, not any local observation"
  );
  assert!(
    [0u64, 1]
      .iter()
      .all(|&n| w.hosts[&n].group(&U).expect("U hosted").shape_gen() > frozen_gen),
    "U advanced past its frozen generation on its own hosts"
  );

  // S freezes into T and every host commits the absorb. Pin S's leader onto a discharged host (via
  // T's leader) so the freeze clears the SourceOwesThaw gate and the commit barrier reads the source
  // leader off the target's leader.
  for _ in 0..2_000 {
    if w.leader_of(T) == Some(0) {
      break;
    }
    w.transfer_group_leader(T, 0);
    w.tick();
  }
  colocate_source_onto_target(&mut w, S, T);
  merge_verb_until_accepted(&mut w, 3_000, "the S→T freeze", |w| {
    w.propose_prepare_merge(S, T)
  });
  merge_verb_until_accepted(&mut w, 4_000, "the S→T commit", |w| {
    w.propose_commit_merge(T, S)
  });

  // THE PIN: with node 2's ghost already cleared by the witness, S owes no thaw anywhere and the
  // co-hosted absorb resolves on EVERY host — node 2 included. (Pre-witness, node 2's park wedged
  // forever on the dead-end obligation while {0,1} resolved and raced ahead; the belt was the only
  // exit.)
  assert!(
    w.run_until(8_000, |w| (0..3).all(|n| !w.hosts_group(n, S))),
    "the S→T absorb must resolve on EVERY host — a dead-end obligation must not wedge node 2's park"
  );
  assert_eq!(w.merges_registered(), 1, "one absorb, registered once");
  assert!(w.is_merged(S), "S is terminally merged away");
}

/// A dissolved husk does not RESURRECT across a crash — the `Retired` fold's co-barriered terminal
/// floor is durable, so a restore-from-durable-state never re-admits the id. A frozen source is
/// floored terminally on its host (modeling a merge that resolved elsewhere while no park formed
/// here); the husk-dissolve arm reclaims it; a crash then rebuilds every hosted replica from durable
/// state and the source is not among them, its floor intact.
#[test]
fn a_dissolved_husk_does_not_resurrect_across_a_crash() {
  // The source encodes above the target so the freeze claim points down the id order.
  const S: u64 = 41;
  const T: u64 = 40;
  let mut w = MultiWorld::new(11);
  w.add_node(0);
  let solo: BTreeSet<u64> = [0].into_iter().collect();
  w.create_group(S, &solo);
  w.create_group(T, &solo);
  assert!(w.run_until(2_000, |w| w.leader_of(S).is_some()
    && w.leader_of(T).is_some()));
  // Freeze S into T — S is now a frozen source.
  merge_verb_until_accepted(&mut w, 2_000, "the S→T freeze", |w| {
    w.propose_prepare_merge(S, T)
  });
  assert!(w.run_until(4_000, |w| {
    w.hosts[&0].group(&S).is_some_and(|ep| ep.is_frozen())
  }));
  // MODEL THE HUSK: the catalog floors S terminally on node 0 (its merge resolved elsewhere) while S
  // is still frozen+hosted and no park formed here — the exact husk the dissolve reclaims.
  w.merge_floors.insert((0, S));
  // THE DISSOLVE (driven off the oracle path, as the container reclaims a source the world registry
  // still tracks as live): the husk-dissolve arm retires S locally.
  assert!(w.pump_merges(), "the husk dissolved");
  assert!(!w.hosts_group(0, S), "S's husk replica is gone");
  assert!(
    w.merge_floors.contains(&(0, S)),
    "its terminal floor was re-written durably, co-barriered with the teardown"
  );
  // CRASH + RESTORE: the node rebuilds every hosted replica from durable state. S is NOT among them,
  // and the durable terminal floor persists — so it never re-admits.
  w.crash(0);
  assert!(
    !w.hosts_group(0, S),
    "S does not resurrect across the crash — the durable floor held"
  );
  assert!(
    w.merge_floors.contains(&(0, S)),
    "the terminal floor survives the crash"
  );
}

/// Drive `source → target` on a fresh 3-node world through freeze, commit, and every host's
/// resolution (the merge-test preamble shared by the teardown pins).
fn drive_merge_to_full_resolution(seed: u64, source: u64, target: u64) -> MultiWorld {
  let mut w = MultiWorld::new(seed);
  for n in 0..3 {
    w.add_node(n);
  }
  let all: BTreeSet<u64> = (0..3).collect();
  w.create_group(source, &all);
  w.create_group(target, &all);
  assert!(w.run_until(2_000, |w| {
    w.leader_of(source).is_some() && w.leader_of(target).is_some()
  }));
  w.propose(source, b"s0");
  w.propose(target, b"t0");
  w.run_until(200, |_| false);
  // The commit barrier reads the source leader off the target's leader — colocate them.
  colocate_source_onto_target(&mut w, source, target);
  merge_verb_until_accepted(&mut w, 2_000, "the freeze", |w| {
    w.propose_prepare_merge(source, target)
  });
  merge_verb_until_accepted(&mut w, 4_000, "the commit", |w| {
    w.propose_commit_merge(target, source)
  });
  assert!(
    w.run_until(8_000, |w| w.merges_resolved() >= 3),
    "every host resolves the absorb"
  );
  w
}

/// The teardown sweep RECORDS what the resolver extracted: once a host's absorb capture is
/// durable, its `(node, source)` terminal floor and the source store drop land together — on
/// EVERY resolved host. The sweep must decide completion from the resolutions it was handed
/// (the world's floor state), never from post-resolution hosting: the resolver itself extracts
/// the source endpoint, so a hosting check is always false by then and silently drops the
/// whole batch — zero floors recorded, zero source stores dropped, the absorbed source left
/// restorable on every host forever.
#[test]
fn merge_teardown_records_floors_and_drops_source_stores() {
  let mut w = drive_merge_to_full_resolution(17, 11, 10);
  assert!(
    w.run_until(2_000, |w| w.pending_merge_teardowns.is_empty()),
    "every staged teardown completes"
  );
  for n in 0..3u64 {
    assert!(
      w.merge_floors.contains(&(n, 11)),
      "node {n}: the terminal merge floor is recorded"
    );
    assert!(
      !w.logs.contains_key(&(n, 11)) && !w.stables.contains_key(&(n, 11)),
      "node {n}: the absorbed source's stores are dropped"
    );
    assert!(!w.hosts_group(n, 11));
  }
  assert_eq!(w.merges_resolved(), 3);
  assert_eq!(w.merges_registered(), 1);
  assert!(w.is_merged(11));
  w.check_now();
  w.finalize_merge_conservation_or_panic(17);
}

/// An ABSORBED retired-source husk, converged to EQUAL applied, passes the full safety helper
/// (agreement's sorted absorbed branch on RAW records + integrity) with the HONEST expected set — and
/// the terminal-population fallback keeps its aligned record NON-VACUOUS. A merge empties the source's
/// LIVE key population at resolution, but a lagging husk replica stays hosted inside the safety sweep;
/// without the fallback in `aligned_applied` every husk record would align gkv-EMPTY. The every-peer
/// freeze barrier (`peers_matched_through`) converges the tracked source replicas to the freeze
/// coordinate, so these husks sit at the SAME watermark (asserted below), where the absorbed agreement
/// branch judges their client content on raw records. The aligned-record fallback's live consumer is
/// `agreement_holds`' non-absorbed positional branch, pinned by the plain-source sibling test; this test
/// proves the absorbed husk's equal-applied full-helper pass. Red-proof: revert `aligned_applied` to
/// live-only and the `>= 2 gkv-non-empty` assert fails.
#[test]
fn retired_husk_aligns_against_its_terminal_population() {
  // The gkv (client) cells an aligned record retains — what the aligned consumers judge (non-gkv conf
  // cells survive alignment regardless, so they cannot stand in for client coverage).
  let gkv_cells = |w: &MultiWorld, n: u64, gid: u64| -> usize {
    w.aligned_applied(n, gid, w.replica_gen_of(n, gid))
      .iter()
      .filter(|(_, c)| crate::multi::decode_gkv(c).is_some())
      .count()
  };

  let mut w = MultiWorld::new(29);
  for n in 0..5 {
    w.add_node(n);
  }
  let all: BTreeSet<u64> = (0..5).collect();
  w.create_group(10, &all); // the final target
  w.create_group(11, &all); // absorbs 12, then merges into 10
  w.create_group(12, &all); // the first source
  assert!(w.run_until(3_000, |w| {
    w.leader_of(10).is_some() && w.leader_of(11).is_some() && w.leader_of(12).is_some()
  }));
  // Client (gkv) load so 11 carries own gkv cells the husk records must retain.
  for (key, val) in [(0u16, 100u64), (1, 101)] {
    propose_until_accepted(&mut w, 11, &crate::multi::encode_gkv(11, key, val));
  }
  propose_until_accepted(&mut w, 12, &crate::multi::encode_gkv(12, 0, 120));

  // Merge 12 into 11 and resolve fully — 11 is now an ABSORBED lineage.
  colocate_source_onto_target(&mut w, 12, 11);
  merge_verb_until_accepted(&mut w, 2_000, "freeze 12", |w| {
    w.propose_prepare_merge(12, 11)
  });
  merge_verb_until_accepted(&mut w, 4_000, "commit 12", |w| {
    w.propose_commit_merge(11, 12)
  });
  assert!(w.run_until(8_000, |w| w.is_merged(12)), "12 merges away");
  assert!(w.group_absorbed(11), "11 absorbed 12");
  // More 11-own client load after the absorb, then confirm every replica carries gkv content.
  for (key, val) in [(2u16, 102u64), (3, 103)] {
    propose_until_accepted(&mut w, 11, &crate::multi::encode_gkv(11, key, val));
  }
  assert!(
    w.run_until(2_000, |w| (0..5)
      .all(|n| w.hosts_group(n, 11) && gkv_cells(w, n, 11) > 0)),
    "every 11 replica holds gkv content before the second merge"
  );

  // Put 10's (and colocated 11's) leadership on {0,1,2} so isolating {3,4} later cannot remove it.
  assert!(w.run_until(3_000, |w| w.leader_of(10).is_some()));
  if w.leader_of(10).is_some_and(|l| l >= 3) {
    w.transfer_group_leader(10, 0);
    assert!(w.run_until(3_000, |w| w.leader_of(10).is_some_and(|l| l < 3)));
  }
  // Freeze then commit 11 into 10 with all FIVE source voters reachable — the commit barrier waits for
  // EVERY source voter to match the freeze, so {3,4} cannot be isolated before this.
  colocate_source_onto_target(&mut w, 11, 10);
  merge_verb_until_accepted(&mut w, 2_000, "freeze 11", |w| {
    w.propose_prepare_merge(11, 10)
  });
  merge_verb_until_accepted(&mut w, 4_000, "commit 11", |w| {
    w.propose_commit_merge(10, 11)
  });
  // Isolate {3,4} BEFORE the CommitMerge reaches them: the {0,1,2} quorum applies and resolves (retiring
  // 11, emptying its live keys) while {3,4} stay hosted as husks. The every-peer freeze barrier put all
  // tracked voters at the freeze coordinate, so these husks sit at the SAME watermark (the deferred
  // `Merged` teardown drains fast; the two isolated husks are what keep the aligned records non-vacuous).
  w.isolate(3);
  w.isolate(4);
  assert!(
    w.run_until(8_000, |w| w.is_merged(11)),
    "11 retires — its live population is emptied"
  );

  // The retired source's LIVE population is empty, but the terminal set was stashed at resolution.
  assert!(
    w.groups[&11].keys.is_empty(),
    "the retired source's live population is emptied"
  );
  assert!(
    w.groups[&11].terminal_keys.is_some(),
    "its terminal population was stashed at resolution"
  );
  // THE FIX: hosted husks keep their gkv content, so the aligned consumers are NON-VACUOUS.
  let hosts = w.hosting_nodes(11);
  let with_gkv: Vec<u64> = hosts
    .iter()
    .copied()
    .filter(|&n| gkv_cells(&w, n, 11) > 0)
    .collect();
  assert!(
    with_gkv.len() >= 2,
    "the husks' aligned records must be non-vacuous: >=2 hosted husks keep gkv content, got \
     {with_gkv:?} of hosts {hosts:?}"
  );
  assert!(
    gkv_cells(&w, 3, 11) > 0 && gkv_cells(&w, 4, 11) > 0,
    "both lagging husks (nodes 3,4) retain gkv content via the terminal population"
  );
  // Equal-applied by construction (see the coverage note): the durable-ack every-peer barrier makes
  // the freeze durable on every voter and the settle loop coalesces commit+apply, so the sim cannot
  // host a below-freeze husk — every surviving husk sits at the same applied index.
  let applied3 = w.applied_index_of(3, 11).get();
  for &n in &hosts {
    assert_eq!(
      w.applied_index_of(n, 11).get(),
      applied3,
      "surviving husk {n} must be applied-equal (== node 3's {applied3})"
    );
  }
  // The full safety helper passes over the husks with the HONEST expected set (11's own load plus the
  // 12 cell it absorbed) — the wrapper wiring end-to-end, not just the relation in isolation.
  let expected: BTreeSet<Vec<u8>> = [
    crate::multi::encode_gkv(11, 0, 100),
    crate::multi::encode_gkv(11, 1, 101),
    crate::multi::encode_gkv(11, 2, 102),
    crate::multi::encode_gkv(11, 3, 103),
    crate::multi::encode_gkv(12, 0, 120),
  ]
  .into_iter()
  .collect();
  crate::multi::vopr::assert_group_safety(&w, 11, &expected, 29);
}

/// The plain-source variant — the fix's DETERMINISTICALLY-reachable value. A NEVER-absorbed source
/// merged away routes agreement to the NON-absorbed positional branch (`group_absorbed` is false), which
/// reads `aligned_applied`; without the terminal-population fallback its retained husks align gkv-EMPTY
/// and that branch judges empty records (vacuous). The every-peer freeze barrier pins the husks at the
/// SAME watermark, as in the chained test. Red-proof: revert `aligned_applied` to live-only and the
/// gkv-non-empty assert fails — `agreement_holds` passes either way (empty == empty is a vacuous pass),
/// so the non-vacuity is the load-bearing assert here.
#[test]
fn plain_source_husk_aligns_via_the_non_absorbed_branch() {
  let gkv_cells = |w: &MultiWorld, n: u64, gid: u64| -> usize {
    w.aligned_applied(n, gid, w.replica_gen_of(n, gid))
      .iter()
      .filter(|(_, c)| crate::multi::decode_gkv(c).is_some())
      .count()
  };

  let mut w = MultiWorld::new(31);
  for n in 0..5 {
    w.add_node(n);
  }
  let all: BTreeSet<u64> = (0..5).collect();
  w.create_group(10, &all); // the target
  w.create_group(11, &all); // a PLAIN (never-absorbed) source
  assert!(w.run_until(3_000, |w| w.leader_of(10).is_some()
    && w.leader_of(11).is_some()));
  for (key, val) in [(0u16, 200u64), (1, 201), (2, 202)] {
    propose_until_accepted(&mut w, 11, &crate::multi::encode_gkv(11, key, val));
  }
  assert!(
    w.run_until(2_000, |w| (0..5)
      .all(|n| w.hosts_group(n, 11) && gkv_cells(w, n, 11) > 0)),
    "every 11 replica holds gkv content before the merge"
  );
  assert!(
    !w.group_absorbed(11),
    "11 never absorbed anything — agreement routes to the non-absorbed positional branch"
  );

  // Leadership off {3,4}; freeze+commit with all five voters reachable; then isolate {3,4} before the
  // CommitMerge reaches them so they stay hosted as husks at the freeze coordinate.
  assert!(w.run_until(3_000, |w| w.leader_of(10).is_some()));
  if w.leader_of(10).is_some_and(|l| l >= 3) {
    w.transfer_group_leader(10, 0);
    assert!(w.run_until(3_000, |w| w.leader_of(10).is_some_and(|l| l < 3)));
  }
  colocate_source_onto_target(&mut w, 11, 10);
  merge_verb_until_accepted(&mut w, 2_000, "freeze 11", |w| {
    w.propose_prepare_merge(11, 10)
  });
  merge_verb_until_accepted(&mut w, 4_000, "commit 11", |w| {
    w.propose_commit_merge(10, 11)
  });
  w.isolate(3);
  w.isolate(4);
  assert!(
    w.run_until(8_000, |w| w.is_merged(11)),
    "11 retires — its live population is emptied"
  );

  assert!(
    w.groups[&11].keys.is_empty(),
    "the retired source's live population is emptied"
  );
  assert!(
    w.groups[&11].terminal_keys.is_some(),
    "the terminal population was stashed at resolution"
  );
  assert!(
    !w.group_absorbed(11),
    "still non-absorbed — agreement routes to the positional branch that reads aligned_applied"
  );
  // THE FIX: the husks align NON-VACUOUS via the terminal population; without it the non-absorbed
  // positional branch would compare empty records.
  let hosts = w.hosting_nodes(11);
  let with_gkv: Vec<u64> = hosts
    .iter()
    .copied()
    .filter(|&n| gkv_cells(&w, n, 11) > 0)
    .collect();
  assert!(
    with_gkv.len() >= 2,
    "the non-absorbed positional branch must be non-vacuous: >=2 husks keep gkv content, got \
     {with_gkv:?} of hosts {hosts:?}"
  );
  assert!(
    gkv_cells(&w, 3, 11) > 0 && gkv_cells(&w, 4, 11) > 0,
    "both husks retain gkv content via the terminal population"
  );
  // Equal-applied by construction (see the coverage note): the durable-ack every-peer barrier makes
  // the freeze durable on every voter and the settle loop coalesces commit+apply, so the sim cannot
  // host a below-freeze husk — every surviving husk sits at the same applied index.
  let applied3 = w.applied_index_of(3, 11).get();
  for &n in &hosts {
    assert_eq!(
      w.applied_index_of(n, 11).get(),
      applied3,
      "surviving husk {n} must be applied-equal (== node 3's {applied3})"
    );
  }
  assert!(
    w.agreement_holds(11),
    "the non-absorbed positional branch passes over the husks"
  );
}

/// A crash AFTER the absorb's barrier landed must not bring the source back in any form: the
/// floor and store drop are terminal, so the restored host rebuilds the target alone and no
/// frozen source replica reappears anywhere. Under the dead hosting-check sweep the source's
/// durable stores lingered on every host — absorbed state held restorable forever, one
/// registry walk away from a zombie frozen replica.
#[test]
fn crash_after_full_absorb_does_not_restore_the_source() {
  let mut w = drive_merge_to_full_resolution(17, 11, 10);
  assert!(
    w.run_until(2_000, |w| w.pending_merge_teardowns.is_empty()),
    "every staged teardown completes"
  );
  for n in 0..3u64 {
    w.crash(n);
    assert!(
      !w.hosts_group(n, 11),
      "node {n}: the crash must not rebuild the absorbed source"
    );
    assert!(
      !w.logs.contains_key(&(n, 11)) && !w.stables.contains_key(&(n, 11)),
      "node {n}: no source stores survive to restore from"
    );
    assert!(w.hosts_group(n, 10), "node {n}: the target restores");
  }
  assert!(!w.group_frozen(11), "no zombie frozen source replica");
  // The restored union keeps serving: fresh load commits and applies on every host.
  assert!(w.run_until(4_000, |w| w.leader_of(10).is_some()));
  let mut idx = None;
  for _ in 0..2_000 {
    if let Some(i) = w.propose(10, b"t9") {
      idx = Some(i);
      break;
    }
    w.tick();
  }
  let idx = idx.expect("the restored target accepts fresh load");
  assert!(
    w.run_until(4_000, |w| {
      (0..3).all(|n| {
        w.hosts[&n]
          .group(&10)
          .is_some_and(|ep| ep.applied_index() >= idx)
      })
    }),
    "post-crash load applies on every restored host"
  );
}

/// The absent-source park discrimination on a GENUINE replayed park: a follower's target
/// stable gets a real fsync window, so its absorb capture dies in a crash and the restored
/// replica replays the commit and re-parks — under-hosted, since the consumed source never
/// re-hosts. The floor gate must hold BOTH ways: the durable hosts' floors are already
/// recorded, while the follower's stays honestly ABSENT — its barrier never landed. The park
/// itself no longer waits for the ordinary snapshot route: the resolver classifies it locally
/// unresolvable, the follower ADVERTISES its boundary, and the leader's covering blob adopts
/// in place of the impossible fold — the full cure pipeline, exercised through a real crash.
/// What stays pinned is the barrier's honesty across the cure: no floor may land ahead of a
/// durable capture covering the boundary, the retained source stores outlive the adopt until
/// that capture completes, and the agreement oracle convicts any divergent union within a tick.
#[test]
fn a_replayed_under_hosted_park_is_cured_while_the_floor_waits_for_durability() {
  let mut w = MultiWorld::new(19);
  for n in 0..3 {
    w.add_node(n);
  }
  let all: BTreeSet<u64> = (0..3).collect();
  w.create_group(11, &all);
  w.create_group(10, &all);
  assert!(w.run_until(2_000, |w| {
    w.leader_of(11).is_some() && w.leader_of(10).is_some()
  }));
  w.propose(11, b"s0");
  w.propose(10, b"t0");
  w.run_until(200, |_| false);
  // Colocate before the freeze so the commit barrier reads the source off the target's leader;
  // the follower picked below is then a non-leader of both, its target capture free to lag.
  colocate_source_onto_target(&mut w, 11, 10);
  let follower = (0..3u64)
    .find(|n| Some(*n) != w.leader_of(11) && Some(*n) != w.leader_of(10))
    .expect("three nodes, at most two leaders");
  // A real fsync window on the follower's TARGET stable only: the absorb capture will sit in
  // flight there while the (sync) log keeps the parked commit itself durable.
  w.stables
    .get_mut(&(follower, 10))
    .expect("target stable")
    .set_mode(crate::StoreMode::Async);
  merge_verb_until_accepted(&mut w, 2_000, "the freeze", |w| {
    w.propose_prepare_merge(11, 10)
  });
  merge_verb_until_accepted(&mut w, 4_000, "the commit", |w| {
    w.propose_commit_merge(10, 11)
  });
  assert!(
    w.run_until(8_000, |w| w.merges_resolved() >= 3),
    "every host resolves the absorb"
  );
  // The sync hosts' barriers landed at resolution; the follower's capture honestly pends.
  for n in (0..3u64).filter(|n| *n != follower) {
    assert!(
      w.merge_floors.contains(&(n, 11)),
      "node {n}: the durable hosts' floors are recorded"
    );
  }
  assert!(
    w.stables[&(follower, 10)].has_inflight(),
    "the follower's capture sits in the fsync window"
  );
  assert!(
    !w.merge_floors.contains(&(follower, 11)),
    "no floor may be recorded ahead of the capture's durability"
  );

  // The crash collapses the window: the capture is lost, the durable log still holds the
  // commit — the restored target replays it and re-parks, under-hosted. The re-wired replica
  // carries a low threshold so post-cure load reaches an ordinary capture in test time (the
  // crash restores off the STORED per-replica config).
  {
    let c = w.configs.get_mut(&(follower, 10)).expect("stored config");
    *c = c.clone().with_snapshot_threshold(8);
  }
  w.crash(follower);
  assert!(
    w.run_until(4_000, |w| {
      w.hosts[&follower]
        .group(&10)
        .is_some_and(|ep| ep.pending_merge().is_some())
    }),
    "the restored target replays the commit and re-parks"
  );
  // At the re-park instant the barrier is honest: no floor, the retained source stores intact.
  assert!(
    !w.merge_floors.contains(&(follower, 11)),
    "the floor may not land while the capture is unrecoverable"
  );
  assert!(
    w.logs.contains_key(&(follower, 11)),
    "the pending teardown retains the source stores until the barrier lands"
  );
  // The cure pipeline: the resolver classifies the park locally unresolvable, the follower
  // advertises, and the leader's covering blob adopts in place of the impossible fold.
  assert!(
    w.run_until(8_000, |w| {
      w.hosts[&follower]
        .group(&10)
        .is_some_and(|ep| ep.pending_merge().is_none())
    }),
    "the advertised park is cured by the leader's covering snapshot"
  );
  assert_eq!(w.merges_aborted(), 0);
  // The barrier stays honest THROUGH the cure: the adopt persisted no blob, so the floor may
  // not land until an ordinary capture covers the boundary locally — under idle the retained
  // stores simply wait, the conservative direction. Post-cure load drives the threshold
  // capture, the pending teardown's gate releases, and the floor lands.
  assert!(
    !w.merge_floors.contains(&(follower, 11)),
    "no floor lands off the adopt alone: durability, not adoption, releases the teardown"
  );
  // The fsync window served its purpose (it killed the original capture); a stable that never
  // flushes would hold the teardown forever by construction, so restore sync completions for
  // the post-cure half.
  w.stables
    .get_mut(&(follower, 10))
    .expect("target stable")
    .set_mode(crate::StoreMode::Sync);
  for i in 0..200u32 {
    w.propose(10, &i.to_be_bytes());
    w.tick();
  }
  assert!(
    w.run_until(8_000, |w| w.merge_floors.contains(&(follower, 11))),
    "an ordinary capture covers the boundary and the deferred teardown completes"
  );
}

/// The world-side floor plumbing the sweep now feeds: a recorded `(node, source)` floor
/// reaches [`sailing_proto::FloorStore::floor`] as the terminal [`sailing_proto::MERGED_FLOOR`]
/// sentinel — the absent-arm discriminator `service_merge_applies` reads — while every
/// unfloored id stays at the working floor. The arm's verdict on the sentinel (a replayed
/// duplicate no-ops past the park) is pinned in the product's own service tests; this seam is
/// what the dead hosting-check sweep left permanently empty, making that arm unreachable from
/// the world. The non-terminal removal leg is folded in beside it: a per-gid `removal_floors`
/// entry (a reshaped id the world stopped hosting) surfaces at every node, and the two legs take
/// their MAX — so a target re-deriving a torn-down source's abort obligation discharges it off
/// the removal floor even where the terminal per-host sentinel was never recorded.
#[test]
fn recorded_floors_reach_the_service_as_the_terminal_sentinel() {
  use sailing_proto::FloorStore as _;
  let mut logs: BTreeMap<(u64, u64), MemLog> = BTreeMap::new();
  let mut stables: BTreeMap<(u64, u64), MemStable<u64>> = BTreeMap::new();
  let mut floored: BTreeSet<(u64, u64)> = BTreeSet::new();
  floored.insert((1, 10));
  // A reshaped id the world tore down without merging: floored ABOVE its removal ceiling (gen 3),
  // cluster-wide (every node reads it), never the terminal sentinel.
  let mut removal_floors: BTreeMap<u64, u64> = BTreeMap::new();
  removal_floors.insert(12, 3);
  let no_husks = BTreeSet::new();
  let stores = super::merge::NodeStores {
    node: 1,
    logs: &mut logs,
    stables: &mut stables,
    floored: &floored,
    removal_floors: &removal_floors,
    husk_floors: &no_husks,
  };
  assert_eq!(stores.floor(&10), sailing_proto::MERGED_FLOOR);
  assert_eq!(
    stores.floor(&11),
    0,
    "an unfloored id keeps the working floor"
  );
  assert_eq!(
    stores.floor(&12),
    3,
    "a non-terminal removal floor surfaces at its recorded gen (not the sentinel)"
  );
  assert!(
    !sailing_proto::floor_admits(stores.floor(&12), 2),
    "the removal floor fences a below-ceiling gen — the re-derived abort obligation discharges"
  );
  let other = super::merge::NodeStores {
    node: 2,
    logs: &mut logs,
    stables: &mut stables,
    floored: &floored,
    removal_floors: &removal_floors,
    husk_floors: &no_husks,
  };
  assert_eq!(
    other.floor(&10),
    0,
    "the TERMINAL floor is per-host: another node's teardown does not floor this one"
  );
  assert_eq!(
    other.floor(&12),
    3,
    "the REMOVAL floor is cluster-wide: the catalog is one fact, read at every node"
  );
}

/// Drive an already-live `target` (absorbing an already-live `source`, both on nodes {0,1,2}) to a
/// HELD merge PARK on a non-leader follower — the async-capture crash-replay shape
/// [`replayed_park_holds_while_the_capture_barrier_is_open`] pins: the follower's absorb capture dies
/// in the crash, its durable log replays the commit, and it re-parks with the barrier open (no
/// snapshot route arrives to supersede it here). Returns the parked follower node.
fn drive_park_on_existing(w: &mut MultiWorld, source: u64, target: u64) -> u64 {
  colocate_source_onto_target(w, source, target);
  let follower = (0..3u64)
    .find(|n| Some(*n) != w.leader_of(source) && Some(*n) != w.leader_of(target))
    .expect("three nodes, at most two leaders");
  w.stables
    .get_mut(&(follower, target))
    .expect("target stable")
    .set_mode(crate::StoreMode::Async);
  merge_verb_until_accepted(w, 2_000, "the freeze", |w| {
    w.propose_prepare_merge(source, target)
  });
  merge_verb_until_accepted(w, 4_000, "the commit", |w| {
    w.propose_commit_merge(target, source)
  });
  assert!(
    w.run_until(8_000, |w| w.merges_resolved() >= 3),
    "every host resolves the absorb"
  );
  w.crash(follower);
  assert!(
    w.run_until(4_000, |w| {
      w.hosts[&follower]
        .group(&target)
        .is_some_and(|ep| ep.pending_merge().is_some())
    }),
    "the restored follower replays the commit and re-parks"
  );
  follower
}

/// Create `source` and `target` on {0,1,2} with a little committed client load (so the target has an
/// applied history to judge), then drive `target` to a HELD merge park via [`drive_park_on_existing`].
pub(crate) fn held_park_target(seed: u64, source: u64, target: u64) -> (MultiWorld, u64) {
  let mut w = MultiWorld::new(seed);
  for n in 0..3 {
    w.add_node(n);
  }
  let all: BTreeSet<u64> = (0..3).collect();
  w.create_group(source, &all);
  w.create_group(target, &all);
  assert!(w.run_until(2_000, |w| {
    w.leader_of(source).is_some() && w.leader_of(target).is_some()
  }));
  propose_until_accepted(&mut w, source, b"s0");
  propose_until_accepted(&mut w, target, b"t0");
  w.run_until(200, |_| false);
  let follower = drive_park_on_existing(&mut w, source, target);
  (w, follower)
}

/// The fork-fence coupling compares against the PARK's coordinate (`applied_index + 1`), NOT the
/// moving commit (#110). A parked target pins its apply at `k-1` while its commit races ahead, so a
/// fence strictly ABOVE the park coordinate but at-or-below the racing commit must NOT couple — the
/// boundary the earlier commit-based compare over-coupled.
#[test]
fn fork_fence_coupled_park_uses_the_park_coordinate_not_commit() {
  let (mut w, follower) = held_park_target(23, 11, 10);
  // Race the target's COMMIT past the park's pinned apply: the leader commits fresh load, which the
  // parked follower appends and acks (its commit advances) while its apply stays pinned at k-1.
  for _ in 0..4 {
    propose_until_accepted(&mut w, 10, b"more");
  }
  assert!(
    w.run_until(4_000, |w| {
      w.hosts[&follower].group(&10).is_some_and(|ep| {
        ep.pending_merge().is_some() && ep.commit_index().get() >= ep.applied_index().get() + 2
      })
    }),
    "the parked follower's commit must race >= 2 past its pinned apply"
  );
  let applied = w.hosts[&follower]
    .group(&10)
    .expect("parked")
    .applied_index()
    .get();
  // A fence AT the park coordinate (applied + 1) couples — the deadlock boundary is inclusive.
  w.fork_conflicts.clear();
  w.inject_fork_conflict(follower, 10, sailing_proto::Index::new(applied + 1));
  assert!(
    w.fork_fence_coupled_park(10),
    "a fence at the park coordinate (applied + 1) couples"
  );
  // A fence ABOVE the park coordinate but at-or-below the RACING COMMIT must NOT couple: the fix
  // compares against applied + 1, not the moving commit. Under the old commit-based compare this
  // fence (<= commit) would have falsely certified the group as coupled.
  w.fork_conflicts.clear();
  w.inject_fork_conflict(follower, 10, sailing_proto::Index::new(applied + 2));
  assert!(
    !w.fork_fence_coupled_park(10),
    "a fence above the park coordinate must not couple, even at-or-below the racing commit"
  );
  assert!(
    w.fork_fence_wedge_set().is_empty(),
    "no coupling => empty #110 set"
  );
}

/// The REFUSE arm clears the fence: a real parked conflict whose child is then REMOVED resolves
/// through the refusal arm (the late fork hits the tombstone), and a LATER merge park on the same
/// parent must NOT be exempted (#110). The conflict is recorded on the lagging node (the world cannot
/// mint one organically) and the refusal is driven end-to-end — not an injected record beside a
/// conflict-free fork.
#[test]
fn fork_fence_clears_on_the_refuse_arm_so_a_later_park_is_not_exempted() {
  let mut w = MultiWorld::new(43);
  for n in 0..3 {
    w.add_node(n);
  }
  let all: BTreeSet<u64> = (0..3).collect();
  w.create_group(10, &all); // the parent (later the merge target)
  w.create_group(11, &all); // the merge source
  assert!(w.run_until(3_000, |w| {
    w.leader_of(10).is_some() && w.leader_of(11).is_some()
  }));
  for key in 0u16..8 {
    propose_until_accepted(
      &mut w,
      10,
      &crate::multi::encode_gkv(10, key, u64::from(key)),
    );
  }
  assert!(w.run_until(2_000, |w| (0..3).all(|n| w.applied_of(n, 10).len() >= 8)));

  // Node 2 lags the whole split: isolate it, commit the split on {0,1}, materialize child 200 there.
  w.isolate(2);
  assert!(w.run_until(3_000, |w| w.leader_of(10).is_some_and(|l| l != 2)));
  propose_split_until_accepted(&mut w, 10, 200, 4);
  assert!(w.run_until(3_000, |w| w.splits_applied() == 1));
  // A REAL standing conflict on the lagging node 2 for child 200 — recorded naming the child, so the
  // refuse arm clears it exactly as an organic conflict would.
  let fence = w.split_fence_index[&200];
  w.inject_fork_conflict_for_child(2, 10, fence, 200);
  assert!(w.has_fork_fence_below(2, 10, sailing_proto::Index::new(u64::MAX)));

  // Retire the child while node 2 still has the split entry ahead of it, then heal: node 2 applies
  // the split and its late fork HOLDS on the tombstone. Recreation raises the id's floor past the
  // fork's generation, which IS a verdict about the fork — that is the refuse arm, and the fence's
  // resolution arm with it.
  w.remove_group(200);
  w.heal(2);
  assert!(
    w.run_until(4_000, |w| w.split_conflicts_observed() >= 1),
    "the late fork never signalled its hold"
  );
  // Now floor the child TERMINALLY on this node — the state a completed merge of that id leaves,
  // modelled as the husk-dissolve suite models it. A floor the fork's generation can never clear
  // is a verdict about the fork rather than about the id, so this is the arm that abandons — and
  // the arm that resolves the fence.
  w.merge_floors.insert((2, 200));
  assert!(
    w.run_until(4_000, |w| w.split_refused_observed() == 1),
    "the terminal floor never refused the stale fork"
  );
  assert!(
    !w.hosts_group(2, 200),
    "a refused fork must not materialize"
  );
  assert!(
    !w.has_fork_fence_below(2, 10, sailing_proto::Index::new(u64::MAX)),
    "the refuse arm cleared the standing fence"
  );

  // A LATER merge park on the same parent must NOT be exempted — the resolved conflict left no fence.
  let follower = drive_park_on_existing(&mut w, 11, 10);
  assert!(
    w.hosts[&follower]
      .group(&10)
      .is_some_and(|ep| ep.pending_merge().is_some()),
    "the parent is parked mid-absorb"
  );
  assert!(
    !w.fork_fence_coupled_park(10),
    "a park after the refuse resolved must not be certified as fork-fence coupled"
  );
  assert!(
    w.fork_fence_wedge_set().is_empty(),
    "no standing fence => empty #110 set"
  );
}

/// The REDUNDANT fold clears the fence: a real parked conflict whose child is already
/// provenance-matched on the node resolves through the container's internal redundant arm (a crashed
/// parent replays its split and folds the fork against its own restored child, yielding NO fork), and
/// a LATER merge park on the same parent must NOT be exempted (#110). The pump can't see that internal
/// fold, so it reconciles it off the hosted-child fact — driven end-to-end through the crash-replay.
#[test]
fn fork_fence_clears_on_the_redundant_fold_so_a_later_park_is_not_exempted() {
  let mut w = world_after_split(47, 200); // 100 split into 200, materialized on all nodes
  assert_eq!(w.splits_applied(), 1);
  // A merge source that encodes ABOVE the parent (the direction rule), for the later park.
  let all: BTreeSet<u64> = (0..3).collect();
  w.create_group(101, &all);
  assert!(w.run_until(3_000, |w| w.leader_of(101).is_some()));

  // A REAL standing conflict on node 0 for child 200, recorded naming the child.
  let fence = w.split_fence_index[&200];
  w.inject_fork_conflict_for_child(0, 100, fence, 200);
  assert!(w.has_fork_fence_below(0, 100, sailing_proto::Index::new(u64::MAX)));

  // Crash node 0: it restores its 200 replica (carrying the fork token) and the parent replays the
  // split — the replayed fork resolves REDUNDANT against the provenance-matched twin, yielding no
  // fork. The pump reconciles the fence off node 0 hosting the resolved child.
  w.crash(0);
  assert!(
    w.run_until(2_000, |w| w.hosts_group(0, 200)),
    "the crashed node restores its child replica"
  );
  for _ in 0..80 {
    w.tick();
  }
  assert_eq!(
    w.splits_applied(),
    1,
    "the replayed fork folds redundant, not re-registered"
  );
  assert!(
    !w.has_fork_fence_below(0, 100, sailing_proto::Index::new(u64::MAX)),
    "the redundant fold cleared the standing fence"
  );

  // A LATER merge park on the same parent must NOT be exempted.
  let follower = drive_park_on_existing(&mut w, 101, 100);
  assert!(
    w.hosts[&follower]
      .group(&100)
      .is_some_and(|ep| ep.pending_merge().is_some()),
    "the parent is parked mid-absorb"
  );
  assert!(
    !w.fork_fence_coupled_park(100),
    "a park after the redundant fold must not be certified as fork-fence coupled"
  );
  assert!(
    w.fork_fence_wedge_set().is_empty(),
    "no standing fence => empty #110 set"
  );
}

/// The fork-fence record is cleared when the parent replica is torn down — active state, not
/// append-only history (#110). The shared `purge_group_stores` chokepoint fires for both
/// `drop_group_replica` and `remove_group`.
#[test]
fn fork_fence_clears_when_the_parent_replica_is_torn_down() {
  let mut w = MultiWorld::new(31);
  for n in 0..3 {
    w.add_node(n);
  }
  let all: BTreeSet<u64> = (0..3).collect();
  w.create_group(100, &all);
  assert!(w.run_until(2_000, |w| w.leader_of(100).is_some()));
  w.inject_fork_conflict(1, 100, sailing_proto::Index::new(3));
  assert!(
    w.has_fork_fence_below(1, 100, sailing_proto::Index::new(3)),
    "the fence is recorded"
  );
  // Tear the parent replica down on that node — the record must go with it.
  w.drop_group_replica(100, 1);
  assert!(
    !w.has_fork_fence_below(1, 100, sailing_proto::Index::new(u64::MAX)),
    "tearing down the parent replica cleared its fork-fence record"
  );
}

/// The two exemption classes are counted and overlapped INDEPENDENTLY (#106 under-hosted AND #110
/// fork-fence): a group satisfying BOTH predicates lands in both wedge sets, so both raw counters and
/// the overlap are positive. The earlier `difference`-based #110 count dropped exactly this group,
/// zeroing its counter and its seed-list entry.
#[test]
fn both_wedge_classes_count_and_overlap_independently() {
  let (mut w, follower) = held_park_target(37, 11, 10);
  // Strip the target's host quorum: drop every OTHER hosting replica (each resolved the absorb, so
  // none is a merge participant), leaving only the parked follower — a merge participant with no live
  // host quorum (the #106 under-hosted root).
  for n in 0..3u64 {
    if n != follower && w.hosts_group(n, 10) {
      w.drop_group_replica(10, n);
    }
  }
  assert!(
    !w.has_live_host_quorum(10),
    "only the parked follower hosts the target"
  );
  // Record a standing fence at-or-below the follower's park coordinate (the #110 fork-fence root).
  let applied = w.applied_index_of(follower, 10).get();
  w.inject_fork_conflict(follower, 10, sailing_proto::Index::new(applied + 1));

  let underhosted = w.tracked_merge_wedge_set();
  let forkfence = w.fork_fence_wedge_set();
  assert!(
    underhosted.contains(&10),
    "the parked under-hosted target is the #106 class"
  );
  assert!(
    forkfence.contains(&10),
    "the same target is the #110 fork-fence class"
  );
  assert!(!underhosted.is_empty() && !forkfence.is_empty());
  let overlap: BTreeSet<u64> = underhosted.intersection(&forkfence).copied().collect();
  assert!(
    overlap.contains(&10),
    "a group in both predicates is the explicit overlap"
  );
  // The earlier `difference`-based #110 attribution dropped exactly this group, zeroing its counter.
  assert!(
    forkfence.difference(&underhosted).count() < forkfence.len(),
    "the old difference-based #110 count would miss the overlapping group"
  );
}

/// A liveness exemption must NEVER gate safety (#106/#110): the quiesce's per-group safety pass runs
/// on EVERY hosted replica of every group, exempt or not. Here a group is driven into the exempted
/// fork-fence wedge, then the integrity leg is shown to STILL trip on a hosted replica whose applied
/// record carries a command outside the proposed set — before the fix the group was skipped whole.
#[test]
#[should_panic(expected = "INTEGRITY FAILURE")]
fn exemption_does_not_gate_safety_on_a_wedged_group() {
  let (mut w, follower) = held_park_target(41, 11, 10);
  let applied = w.applied_index_of(follower, 10).get();
  w.inject_fork_conflict(follower, 10, sailing_proto::Index::new(applied + 1));
  // The target is now the exempted fork-fence wedge (#110).
  assert!(
    w.fork_fence_wedge_set().contains(&10),
    "the parked target is exempted"
  );
  // Its hosted replicas DID apply the target's real client load. Present an `expected` set that omits
  // it (a divergent applied record); the unconditional safety pass must still panic on the exempted
  // group — the exemption gates only convergence, never this.
  let expected: BTreeSet<Vec<u8>> = BTreeSet::new();
  crate::multi::vopr::assert_group_safety(&w, 10, &expected, 41);
}

/// An EXEMPTED absorbed wedge sitting at UNEQUAL watermarks still passes the safety helper after the
/// per-index cross-watermark leg was retired (indices are not cell identities — see
/// `assert_group_safety`): the parked follower is certified without a leader and without equal
/// watermarks by the surviving legs. Its load is non-gkv (`t0`/`s0`), so the integrity leg carries its
/// client content; its UNEQUAL watermarks come from the PARK COORDINATE (the follower pinned at k-1 while
/// the resolved hosts moved past k), NOT the merge barrier's match/apply gap, so the two unequal shapes
/// are not conflated.
#[test]
fn exempted_absorbed_wedge_at_unequal_watermarks_passes_safety() {
  let (mut w, follower) = held_park_target(53, 11, 10);
  assert!(w.group_absorbed(10), "the target absorbed the source");
  // The parked follower sits at k-1; the resolved hosts are past k — unequal watermarks.
  let lens: Vec<usize> = (0..3u64).map(|n| w.applied_of(n, 10).len()).collect();
  assert!(
    lens.iter().min() != lens.iter().max(),
    "the wedge must straddle unequal watermarks: {lens:?}"
  );
  // Make it the exempted fork-fence wedge (#110) so the safety pass runs on it EXEMPT.
  let applied = w.applied_index_of(follower, 10).get();
  w.inject_fork_conflict(follower, 10, sailing_proto::Index::new(applied + 1));
  assert!(
    w.fork_fence_wedge_set().contains(&10),
    "the parked target is exempted"
  );
  // The safety pass certifies the exempted wedge across unequal watermarks via the surviving legs (the
  // target's own load plus the source's load folded on the resolved hosts are the expected set).
  let expected: BTreeSet<Vec<u8>> = [b"t0".to_vec(), b"s0".to_vec()].into_iter().collect();
  crate::multi::vopr::assert_group_safety(&w, 10, &expected, 53);
}

/// The redundant-fold reconciliation keys on the MINT TOKEN, not bare hostedness (#110): a TOKEN-LESS
/// hosted child at a fork-child id is a STANDING squatter (a plain create/recreate incarnation — the
/// lifecycle-churn #110 mechanism), so its recorded fence MUST survive the pump; only a TOKEN-BEARING
/// child (a materialized fork or a redundant-fold twin that adopted the token) clears it. The
/// hostedness-only form cleared the squatter's fence in the very pump that recorded it, un-exempting a
/// real standing wedge.
#[test]
fn redundant_fold_clear_keys_on_the_mint_token_not_hostedness() {
  // TOKEN-LESS squatter: its fence SURVIVES the pump/reconciliation.
  let mut w = MultiWorld::new(71);
  for n in 0..3 {
    w.add_node(n);
  }
  let all: BTreeSet<u64> = (0..3).collect();
  w.create_group(10, &all); // the parent
  w.create_group(200, &all); // a PLAIN create at a fork-child id — token-less by construction
  assert!(w.run_until(3_000, |w| w.leader_of(10).is_some()
    && w.leader_of(200).is_some()));
  assert!(
    w.hosts[&0]
      .group(&200)
      .is_some_and(|ep| ep.fork_id().is_none()),
    "the plain-created squatter is token-less"
  );
  w.inject_fork_conflict_for_child(0, 10, sailing_proto::Index::new(3), 200);
  w.pump_forks();
  assert!(
    w.has_fork_fence_below(0, 10, sailing_proto::Index::new(u64::MAX)),
    "a token-less standing squatter's fence must SURVIVE the pump — the wedge is real"
  );

  // TOKEN-BEARING fork child: its fence CLEARS via the reconciliation (the redundant-fold arm).
  let mut w2 = world_after_split(73, 200); // 100 split into 200 — a real materialized fork
  assert!(
    w2.hosts[&0]
      .group(&200)
      .is_some_and(|ep| ep.fork_id().is_some()),
    "the materialized fork carries the mint token"
  );
  w2.inject_fork_conflict_for_child(0, 100, w2.split_fence_index[&200], 200);
  w2.pump_forks();
  assert!(
    !w2.has_fork_fence_below(0, 100, sailing_proto::Index::new(u64::MAX)),
    "a token-bearing fork child's fence CLEARS — the fork resolved"
  );
}

/// THE DELIVERY-SEAM GENERATION FENCE, the sim's model of the product's demux fence. A frame
/// stamped with a generation BELOW the receiver's persisted admission floor speaks for a RETIRED
/// incarnation: it is dropped, counted, and never reaches the endpoint. A frame stamped AT the
/// floor is a live member's and delivers — the equal-admits line, which is what keeps a live gid's
/// reshape skew from re-creating the apply-time staleness bug.
///
/// The husk is constructed directly on the bus: the world's own retirement purges a removed gid's
/// in-flight traffic, so the shape has to be injected to be observed at all.
#[test]
fn the_delivery_seam_fences_a_retired_incarnations_frames() {
  use sailing_proto::{Index, Message, RequestVote, Term};
  let mut w = MultiWorld::new(2);
  for n in 0..3 {
    w.add_node(n);
  }
  let all: BTreeSet<u64> = (0..3).collect();
  w.create_group(100, &all);
  // The catalog retired every incarnation of 100 below generation 3; this live incarnation IS
  // generation 3 (registry scale), so its own traffic clears the floor while anything stamped
  // lower speaks for an incarnation that is gone.
  w.incarnation_floors.insert(100, 3);
  w.set_generation_for_test(100, 3);
  assert!(w.run_until(600, |w| w.leader_of(100).is_some()));
  assert_eq!(
    w.fenced_dropped(),
    0,
    "the live incarnation's own traffic clears its floor"
  );
  let before_term = w.hosts[&2].group(&100).expect("hosted").term();
  let husk_vote = || {
    Message::RequestVote(RequestVote::new(
      Term::new(before_term.get() + 5),
      1u64,
      Index::new(1),
      Term::new(1),
      false,
      false,
    ))
  };

  let fenced_before = w.fenced_dropped();
  w.inject_for_test(1, 100, 0, 2, husk_vote());
  w.tick();
  assert_eq!(
    w.fenced_dropped(),
    fenced_before + 1,
    "a below-floor stamp is fenced at the delivery seam"
  );
  assert_eq!(
    w.fenced_votes_dropped(),
    1,
    "and counted in the disruption subset"
  );
  assert_eq!(
    w.hosts[&2].group(&100).expect("hosted").term(),
    before_term,
    "the retired incarnation's campaign never reached the endpoint"
  );

  // AT the floor: a live member's frame, delivered.
  w.inject_for_test(1, 100, 3, 2, husk_vote());
  w.tick();
  assert_eq!(
    w.fenced_dropped(),
    fenced_before + 1,
    "at-floor is not below-floor"
  );
  assert!(
    w.hosts[&2].group(&100).expect("hosted").term() > before_term,
    "the at-floor campaign reached the endpoint"
  );
}

/// THE HUSK-MINORITY PROTECTION with a QUORUM of adopt-superseded replicas. A single follower
/// adopting the leader's covering blob is a straggler; a MAJORITY adopting is the case the adopt's
/// soundness argument actually has to survive, since a divergent blob would then carry the quorum
/// rather than be outvoted by it. Two of the target's three replicas are driven into the
/// under-hosted park at once — their absorb captures die in a crash, their durable logs replay the
/// commit, and neither can fold a source whose endpoint resolution already removed — so both
/// ADVERTISE their boundary and both are superseded in place by the one blob the leader sends.
///
/// The world's agreement, cross-talk and lineage oracles run at the end of EVERY tick over every
/// hosted replica, so the convergence assertions below are only half the verdict: the other half is
/// that no tick between the two adopts convicted the union.
///
/// SHAPE DELTA. The natural statement of this — a source hosted on exactly one node, with the other
/// two target replicas absorbing a group they never held — is not constructible: a merge demands
/// identical voter sets (`MergeError::VoterSetsDiffer`), so the source cannot be given a solo voter
/// set, and under-hosting it after the fact is refused too (the container will not tear down an
/// unresolved merge participant, and a source that lost its quorum before the freeze can never
/// commit one). The crash-replay park is the constructible route to the same endpoint — source
/// endpoint gone, floor honestly absent, no local fold possible — and this widens it from the single
/// follower [`a_replayed_under_hosted_park_is_cured_while_the_floor_waits_for_durability`] drives to
/// a quorum of them.
#[test]
fn a_quorum_of_adopt_superseded_replicas_converges_under_the_oracles() {
  let mut w = MultiWorld::new(59);
  for n in 0..3 {
    w.add_node(n);
  }
  let all: BTreeSet<u64> = (0..3).collect();
  w.create_group(11, &all);
  w.create_group(10, &all);
  assert!(w.run_until(2_000, |w| {
    w.leader_of(11).is_some() && w.leader_of(10).is_some()
  }));
  propose_until_accepted(&mut w, 11, b"s0");
  propose_until_accepted(&mut w, 10, b"t0");
  w.run_until(200, |_| false);

  // Colocate first: with both leaderships on one host, the OTHER TWO nodes are non-leaders of both
  // groups — the pair whose target captures are free to lag, and the pair this test parks together.
  colocate_source_onto_target(&mut w, 11, 10);
  let leader = w.leader_of(10).expect("target leader");
  let followers: Vec<u64> = (0..3u64).filter(|&n| n != leader).collect();
  assert_eq!(followers.len(), 2, "three nodes, one colocated leader");
  // A real fsync window on each follower's TARGET stable: the absorb capture will sit in flight
  // there while the (sync) log keeps the parked commit itself durable.
  for &f in &followers {
    w.stables
      .get_mut(&(f, 10))
      .expect("target stable")
      .set_mode(crate::StoreMode::Async);
  }

  merge_verb_until_accepted(&mut w, 2_000, "the freeze", |w| {
    w.propose_prepare_merge(11, 10)
  });
  merge_verb_until_accepted(&mut w, 4_000, "the commit", |w| {
    w.propose_commit_merge(10, 11)
  });
  assert!(
    w.run_until(8_000, |w| w.merges_resolved() >= 3),
    "every host resolves the absorb before the crashes"
  );
  for &f in &followers {
    assert!(
      w.stables[&(f, 10)].has_inflight(),
      "node {f}: the absorb capture must sit in the fsync window"
    );
    assert!(
      !w.merge_floors.contains(&(f, 11)),
      "node {f}: no floor may be recorded ahead of the capture's durability"
    );
  }

  // Collapse BOTH windows in the same instant: each restored target replays its durable commit and
  // re-parks under-hosted (the source endpoint resolution removed, its floor never landed), leaving
  // the target's apply pinned on a MAJORITY of its replicas at once.
  for &f in &followers {
    w.crash(f);
  }
  assert!(
    w.run_until(6_000, |w| followers.iter().all(|&f| w.hosts[&f]
      .group(&10)
      .is_some_and(|ep| ep.pending_merge().is_some()))),
    "both restored followers replay the commit and re-park TOGETHER"
  );
  // Both park UNRESOLVABLE — the advertisement, not an ordinary wait on a fold that is merely late.
  assert!(
    followers.iter().all(|&f| w.hosts[&f]
      .group(&10)
      .is_some_and(|ep| ep.merge_park_unresolvable().is_some())),
    "both parked followers must ADVERTISE a boundary: no local fold can ever land on either"
  );
  assert!(
    followers.iter().all(|&f| !w.hosts_group(f, 11)),
    "the parks are under-hosted BY CONSTRUCTION: no source endpoint survives on either follower to \
     fold, only the stores the pending teardown retains (hosting {:?})",
    w.hosting_nodes(11)
  );
  assert!(
    w.hosts[&leader]
      .group(&10)
      .is_some_and(|ep| ep.pending_merge().is_none()),
    "the folding leader is NOT parked — the parked set is exactly the two-replica MAJORITY, which \
     is what makes this the husk-minority argument's hard case"
  );

  // The cure: ONE covering blob from the leader supersedes BOTH parks. Nothing else can clear them
  // — neither follower can fold a source it no longer hosts, and an abort would skip a committed
  // union on a quorum of replicas.
  assert!(
    w.run_until(12_000, |w| followers.iter().all(|&f| w.hosts[&f]
      .group(&10)
      .is_some_and(|ep| ep.pending_merge().is_none()))),
    "both advertised parks are cured by the leader's covering snapshot"
  );
  assert_eq!(
    w.merges_aborted(),
    0,
    "an adopt is not an abort: the union is never skipped on a replica"
  );
  assert!(
    w.agreement_holds(10),
    "the union agrees across the adopting quorum and the leader that folded it"
  );

  // The adopt is observable as CONVERGENCE: the superseded majority holds the leader's applied
  // record at equal watermarks, source content folded in.
  assert!(
    w.run_until(8_000, |w| {
      let lens: Vec<usize> = (0..3u64).map(|n| w.applied_of(n, 10).len()).collect();
      lens.iter().min() == lens.iter().max()
    }),
    "the adopting quorum converges to the leader's applied record"
  );

  // Post-cure load drains everywhere: the adopted replicas are ordinary followers again, not
  // replicas pinned at a boundary they can only leave by installing. Sync completions are restored
  // first — a stable that never flushes would hold their teardowns forever by construction.
  for &f in &followers {
    w.stables
      .get_mut(&(f, 10))
      .expect("target stable")
      .set_mode(crate::StoreMode::Sync);
  }
  for i in 0..40u32 {
    propose_until_accepted(&mut w, 10, &i.to_be_bytes());
  }
  assert!(
    w.run_until(8_000, |w| (0..3u64).all(|n| {
      w.hosts[&n]
        .group(&10)
        .is_some_and(|ep| ep.applied_index() == ep.commit_index())
    })),
    "every live member drains the post-cure load to applied == commit"
  );
  w.check_now();
}

/// A FOUNDING INCARNATION CONSUMES A BOOT EPOCH, so the next crash cannot hand out the same one.
///
/// The nonzero-founding door takes an epoch exactly as a restore does. Reading `counter + 1`
/// without storing it left the counter untouched, and the very next crash then restored every
/// group with the value the founding incarnation had already minted under. Flushed completions
/// deliberately survive a simulation crash here, so two incarnations sharing an epoch is not a
/// cosmetic collision: the founding incarnation's queued acknowledgment stops sorting strictly
/// below the restored one and can alias its first submission — the aliasing the boot epoch exists
/// to prevent, and the property this world is supposed to be evidence FOR.
///
/// The pin is the counter itself. Recreate at a nonzero generation, then crash the same node in
/// the same iteration, and require the recreation to have advanced the counter and the restore's
/// epoch to sit strictly above the founding one.
#[test]
fn founding_at_a_nonzero_generation_consumes_a_boot_epoch() {
  let mut w = MultiWorld::new(2);
  for n in 0..3 {
    w.add_node(n);
  }
  let all: BTreeSet<u64> = (0..3).collect();
  w.create_group(100, &all);
  assert!(w.run_until(600, |w| w.leader_of(100).is_some()));

  // A gen-0 founding goes through the storeless door and takes no epoch at all.
  assert_eq!(
    w.boot_epochs.get(&0).copied().unwrap_or(0),
    0,
    "the storeless door writes nothing and must consume no epoch"
  );

  assert!(w.remove_group(100));
  w.recreate_group(100);
  assert_eq!(
    w.generation_of(100),
    1,
    "the recreation must land at a nonzero generation, or the founded door is never taken"
  );

  let founding = w.boot_epochs.get(&0).copied().unwrap_or(0);
  assert!(
    founding > 0,
    "the recreation founded node 0's replica at generation 1 through the door that TAKES an \
     epoch, so the counter must record the epoch it consumed; it still reads {founding}"
  );

  // The crash lands in the same iteration, before anything else can advance the counter — the
  // narrowest window in which a collision is possible, and the one a real deployment hits when a
  // host dies right after a recreation.
  w.crash(0);
  let restored = w.boot_epochs.get(&0).copied().unwrap_or(0);
  assert!(
    restored > founding,
    "the restored incarnation's boot epoch is {restored} and the founding one minted under \
     {founding}. An epoch handed out twice folds two incarnations onto one identity: the founding \
     incarnation's flushed completion survives the crash and is no longer strictly below the ids \
     the restored incarnation mints"
  );
}
