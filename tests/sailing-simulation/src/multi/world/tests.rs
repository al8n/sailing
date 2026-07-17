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
    .entry(100)
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
  assert_eq!(w.aligned_applied(2, 200), w.aligned_applied(0, 200));
  assert!(!w.aligned_applied(2, 200).is_empty());
  assert!(
    raw.len() > w.aligned_applied(2, 200).len(),
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
      w.aligned_applied(n, 100),
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
  let ahead = w.aligned_applied(0, 200);
  let lagging = w.aligned_applied(2, 200);
  assert_eq!(w.aligned_applied(1, 200), ahead);
  assert!(lagging.len() < ahead.len());
  assert_eq!(&ahead[..lagging.len()], &lagging[..]);
  assert!(w.agreement_holds(200));

  // Heal: the laggard applies the onward split, its late fork materializes 300 on node 2, and
  // all three views converge.
  w.heal(2);
  assert!(
    w.run_until(4_000, |w| {
      w.hosts_group(2, 300)
        && (0..3).all(|n| w.aligned_applied(n, 200) == w.aligned_applied(0, 200))
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
      .aligned_applied(n, 300)
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
      (0..3).all(|n| w.aligned_applied(n, 200) == w.aligned_applied(0, 200))
    }),
    "the parent replicas never converged: {}",
    w.dbg_group(200)
  );
  let parent_cells: Vec<(u64, u16, u64)> = w
    .aligned_applied(0, 200)
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
  assert_eq!(w.aligned_applied(2, 200), w.aligned_applied(0, 200));
  assert!(!w.aligned_applied(2, 200).is_empty());

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
  let ahead = w.aligned_applied(0, 200);
  let lagging = w.aligned_applied(2, 200);
  assert!(lagging.len() < ahead.len());
  assert_eq!(&ahead[..lagging.len()], &lagging[..]);
  assert!(w.agreement_holds(200));
  w.heal(2);
  assert!(
    w.run_until(4_000, |w| w.aligned_applied(2, 200)
      == w.aligned_applied(0, 200)),
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

/// A LATE fork — a lagging parent replica applying the committed split after the child's
/// incarnation was retired — must resolve REFUSED at the world's materialization edge, exactly
/// as the product's coordinator admission (floor → tombstone) refuses it at the driver's
/// fork-drain: no materialization, the parent's fence lifted, and the id left to the ordinary
/// lifecycle. Materializing instead squats a replica under a retired gid, and the next
/// recreation trips container admission with `Exists` on that node.
#[test]
fn late_fork_for_a_retired_child_refuses_and_recreation_admits() {
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

  // Heal: the straggler catches up, applies the split, and its late fork refuses against the
  // tombstone.
  w.heal(2);
  assert!(
    w.run_until(4_000, |w| w.split_refused_observed() == 1),
    "the late fork never resolved refused"
  );
  assert!(
    !w.hosts_group(2, 300),
    "a refused fork must not materialize"
  );
  assert_eq!(w.splits_applied(), 1, "a refused fork registers nothing");

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
/// replica replays the commit and re-parks. The floor gate must hold BOTH ways: the durable
/// hosts' floors are already recorded (the sweep landed their barriers at resolution), while
/// the follower's floor stays honestly ABSENT — its barrier never landed — so the replayed
/// park WAITS for the snapshot route instead of skipping the union it does not have. The
/// floor-present half of the discrimination lives at the service seam
/// ([`recorded_floors_reach_the_service_as_the_terminal_sentinel`] plus the product's own
/// absent-arm test): in the world's one-barrier model a durable floor implies a durable
/// capture, which restores PAST the commit — floor-with-replay cannot conformingly coexist,
/// and the agreement oracle convicts the divergent union-skip within a tick if it is forced.
#[test]
fn replayed_park_holds_while_the_capture_barrier_is_open() {
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
  // commit — the restored target replays it and re-parks.
  w.crash(follower);
  assert!(
    w.run_until(4_000, |w| {
      w.hosts[&follower]
        .group(&10)
        .is_some_and(|ep| ep.pending_merge().is_some())
    }),
    "the restored target replays the commit and re-parks"
  );
  for _ in 0..50 {
    w.tick();
  }
  assert!(
    w.hosts[&follower]
      .group(&10)
      .is_some_and(|ep| ep.pending_merge().is_some()),
    "without the floor the replayed park holds for the snapshot route"
  );
  assert_eq!(w.merges_aborted(), 0);
  // The open barrier keeps the whole batch honest: no floor, the extracted source's stores
  // retained for the pending teardown, and the entry itself still staged.
  assert!(
    !w.merge_floors.contains(&(follower, 11)),
    "the floor may not land while the capture is unrecoverable"
  );
  assert!(
    w.logs.contains_key(&(follower, 11)),
    "the pending teardown retains the source stores until the barrier lands"
  );
  assert!(
    w.pending_merge_teardowns
      .iter()
      .any(|(n, s, _, _)| *n == follower && *s == 11),
    "the follower's teardown entry stays staged"
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
fn held_park_target(seed: u64, source: u64, target: u64) -> (MultiWorld, u64) {
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

/// A fork conflict that RESOLVES (its fork materializes) must clear the standing fence, so a LATER
/// merge park on the same parent is NOT exempted (#110). Real machinery for the resolution: a real
/// split materializes the child through `pump_forks`, which drops the recorded fence; a real merge
/// then parks the same parent, and the coupling must read FALSE because the fence is gone.
#[test]
fn fork_fence_clears_on_materialization_so_a_later_park_is_not_exempted() {
  let mut w = MultiWorld::new(29);
  for n in 0..3 {
    w.add_node(n);
  }
  let all: BTreeSet<u64> = (0..3).collect();
  w.create_group(10, &all); // the parent (later the merge target)
  w.create_group(11, &all); // the merge source
  assert!(w.run_until(2_000, |w| {
    w.leader_of(10).is_some() && w.leader_of(11).is_some()
  }));
  // Load the parent so the split has an interior point with keys on both sides.
  for key in 0u16..4 {
    propose_until_accepted(
      &mut w,
      10,
      &crate::multi::encode_gkv(10, key, u64::from(key)),
    );
  }
  // Propose a real split, then RECORD a standing fence at the child's real split index on every
  // hosting node — the squatter conflict the world cannot itself mint, injected at exactly the
  // coordinate the real materialization will clear.
  propose_split_until_accepted(&mut w, 10, 200, 2);
  let fence = w.split_fence_index[&200];
  for n in 0..3u64 {
    w.inject_fork_conflict(n, 10, fence);
  }
  // Materialize the fork through the real drain: each node's `pump_forks` wires its child replica
  // and, on doing so, drops the fence its child contributed.
  assert!(
    w.run_until(3_000, |w| w.splits_applied() == 1),
    "the split materializes on the quorum"
  );
  for n in 0..3u64 {
    assert!(
      !w.has_fork_fence_below(n, 10, sailing_proto::Index::new(u64::MAX)),
      "node {n}: the materialized fork cleared its standing fence"
    );
  }
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
    "a park after the fork resolved must not be certified as fork-fence coupled"
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
