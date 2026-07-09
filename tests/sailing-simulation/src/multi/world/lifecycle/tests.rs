use super::super::MultiWorld;
use crate::multi::oracles::encode_gkv;
use std::collections::BTreeSet;

/// Build a world with `groups` created over nodes `0..nodes`, elected and past one committed
/// entry each — the common lifecycle fixture.
fn settled_world(seed: u64, nodes: u64, groups: &[u64]) -> MultiWorld {
  let mut w = MultiWorld::new(seed);
  for n in 0..nodes {
    w.add_node(n);
  }
  let all: BTreeSet<u64> = (0..nodes).collect();
  for &gid in groups {
    w.create_group(gid, &all);
  }
  for &gid in groups {
    assert!(
      w.run_until(600, |w| w.leader_of(gid).is_some()),
      "group {gid} must elect"
    );
    assert!(w.propose(gid, &encode_gkv(gid, 0, gid)).is_some());
    assert!(
      w.run_until(600, |w| w.agreement_holds(gid)
        && (0..nodes).all(|n| !w.applied_of(n, gid).is_empty())),
      "group {gid} must commit"
    );
  }
  w
}

/// (a) `remove_group` retires the group everywhere: later ticks deliver nothing for the gid (the
/// bus is purged and stragglers hit the unhosted drop), the retired checker archive holds its
/// frozen history, and the sibling group keeps committing.
#[test]
fn remove_group_retires_and_the_sibling_keeps_committing() {
  let mut w = settled_world(21, 3, &[100, 200]);
  w.remove_group(100);
  assert_eq!(w.retired_checkers(), 1, "the checker moved to the archive");
  assert!(w.leader_of(100).is_none(), "no replica hosts the group");
  assert!(w.applied_of(0, 100).is_empty(), "replica state is gone");

  // Post-removal ticks stay green (the per-group suite no longer judges the retired gid).
  for _ in 0..50 {
    w.tick();
  }
  w.check_now();

  // The sibling group is untouched: it still elects and commits fresh load.
  assert!(w.run_until(600, |w| w.leader_of(200).is_some()));
  let before = w.applied_of(0, 200).len();
  assert!(w.propose(200, &encode_gkv(200, 1, 7)).is_some());
  assert!(w.run_until(600, |w| w.applied_of(0, 200).len() > before));
}

/// (b) `recreate_group` rejoins the SAME logical group at gen+1 with fresh stores: the election
/// completes and the one-identity oracle stays quiet across the incarnation boundary even though
/// terms restart (the live form of the oracle teeth test).
#[test]
fn recreate_group_bumps_gen_and_elects_cleanly() {
  let mut w = settled_world(23, 3, &[100]);
  assert_eq!(w.generation_of(100), 0);
  w.remove_group(100);
  w.recreate_group(100);
  assert_eq!(w.generation_of(100), 1, "recreation bumps the incarnation");
  assert!(
    w.run_until(800, |w| w.leader_of(100).is_some()),
    "the recreated incarnation must elect (grants re-keyed under gen 1)"
  );
  assert!(w.propose(100, &encode_gkv(100, 2, 9)).is_some());
  assert!(w.run_until(600, |w| w.agreement_holds(100)
    && (0..3).all(|n| !w.applied_of(n, 100).is_empty())));
}

/// (c) A conf-change is PER GROUP: adding a learner to one group leaves the sibling group's
/// committed membership untouched, and the reconcile reads committed state (never propose-time
/// bookkeeping).
#[test]
fn conf_change_scopes_to_one_group() {
  let mut w = settled_world(25, 3, &[100, 200]);
  w.add_node(3);
  w.wire_group_observer(100, 3);
  let cc = sailing_proto::ConfChange::new(
    sailing_proto::ConfChangeType::AddLearnerNode,
    3,
    bytes::Bytes::new(),
  );
  assert!(w.propose_conf_change(100, cc).is_some());
  let mut joined = false;
  for _ in 0..800 {
    w.tick();
    w.reconcile_membership(100);
    if w.group_learners(100).contains(&3) {
      joined = true;
      break;
    }
  }
  assert!(
    joined,
    "the learner must join group 100's committed membership"
  );
  w.reconcile_membership(200);
  assert_eq!(
    w.group_voters(200),
    (0..3).collect::<BTreeSet<u64>>(),
    "group 200's committed voters are untouched"
  );
  assert!(
    w.group_learners(200).is_empty(),
    "group 200 gained no learner"
  );
  // The learner replica exists only under group 100.
  assert!(w.run_until(600, |w| !w.applied_of(3, 100).is_empty()));
  assert!(w.applied_of(3, 200).is_empty());
}

/// A quorum mis-parked by the sweep must be able to UN-PARK once membership is re-derived.
/// `complete_under_hosted` reads a parked replica as still-hosting (missing = ∅), so a leaderless
/// group whose own voters were parked can never repair itself — the unpark arm must run on the
/// leaderless path too, not only behind a leader. GREEN: reconcile un-parks the mis-parked
/// majority and the group recovers a leader. RED (restore the leaderless early-return above the
/// unpark arm): the parked voters stay parked and the group is leaderless forever.
#[test]
fn a_mis_parked_quorum_un_parks_and_recovers() {
  let mut w = settled_world(43, 5, &[100]);
  let all: BTreeSet<u64> = (0..5).collect();
  assert_eq!(w.group_voters(100), all, "five committed voters");
  let leader = w.leader_of(100).expect("elected");

  // Mis-park a MAJORITY including the leader — the aftermath of the sweep over-parking a quorum.
  // Seed `parked` directly (the sweep's own effect); no ticks, so the minority never re-elects.
  let mut mis_parked: BTreeSet<u64> = BTreeSet::new();
  mis_parked.insert(leader);
  for n in 0..5u64 {
    if mis_parked.len() >= 3 {
      break;
    }
    mis_parked.insert(n);
  }
  for &n in &mis_parked {
    w.parked.insert((n, 100));
  }
  assert!(
    w.leader_of(100).is_none(),
    "a mis-parked quorum (leader among the parked) is leaderless"
  );

  // ONE reconcile re-derives the committed set off the two still-unparked voters (both list all
  // five) and un-parks the mis-parked majority.
  w.reconcile_membership(100);
  for &n in &mis_parked {
    assert!(
      !w.parked.contains(&(n, 100)),
      "reconcile must un-park mis-parked committed voter {n}"
    );
  }

  // Liveness restored: the group re-elects and commits fresh load (the wedge denied this forever).
  let mut elected = false;
  for _ in 0..5_000 {
    w.reconcile_membership(100);
    if w.leader_of(100).is_some() {
      elected = true;
      break;
    }
    w.tick();
  }
  assert!(
    elected,
    "the group recovers a leader once the mis-parked quorum un-parks"
  );
  assert!(w.propose(100, &encode_gkv(100, 1, 5)).is_some());
  assert!(
    w.run_until(2_000, |w| w.agreement_holds(100)),
    "the recovered group commits fresh load"
  );
}
