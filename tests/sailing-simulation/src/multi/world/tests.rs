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

/// A child replica that arrives by the product's OTHER legitimate path — a fresh observer
/// caught up by snapshot transfer (`LogSm::snapshot()` carries the full record, inherited
/// parent-tagged baseline included) — must yield an aligned record identical to a fork-wired
/// sibling's. The seeding is GROUP-level (the registration record), so the arrival path is
/// irrelevant; seeding only on the fork-wired path left this twin unswept — its parent-tagged
/// baseline cells were judged as cross-talk and its unskipped prefix misaligned the positional
/// agreement leg.
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
  // …and the aligned view discounts it off the GROUP record: identical to a fork-wired
  // sibling's, baseline excluded, agreement whole. (The ticks above already cross-talk-swept
  // node 2's record — an unseeded view would have tripped the oracle before reaching here.)
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
