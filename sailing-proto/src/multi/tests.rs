use super::*;
use crate::{
  Config, Instant, PoisonReason,
  testkit::{AsyncStable, CountSm, VecLog},
};
use bytes::Bytes;
use core::time::Duration;

/// Encode `n` as a CountSm snapshot blob (the `Data` encoding of its `u64` snapshot type).
fn fork_blob(n: u64) -> Bytes {
  let mut v = Vec::new();
  Data::encode(&n, &mut v);
  Bytes::from(v)
}

/// A CountSm vessel preloaded to count `n` — the state a fork's caller derived locally.
fn preloaded_sm(n: u64) -> CountSm {
  let mut sm = CountSm::default();
  StateMachine::restore(&mut sm, n).unwrap();
  sm
}

/// Drain `gid`'s storage completions to quiescence on one host.
fn drain_storage(
  m: &mut MultiRaft<u64, u64, CountSm>,
  gid: u64,
  now: Instant,
  log: &mut VecLog,
  stable: &mut AsyncStable,
) {
  while matches!(
    m.handle_storage(&gid, now, log, stable),
    Some(StorageProgress::MorePending)
  ) {}
}

/// Route every queued message of `gid` between two single-group hosts until neither side moves,
/// draining storage after every dispatch — the deterministic two-node message loop the fork
/// election/joiner pins drive. `tap_a_to_b` observes each a→b message before delivery (the
/// replication-transcript capture).
#[allow(clippy::too_many_arguments)]
fn route_until_quiescent(
  a: &mut MultiRaft<u64, u64, CountSm>,
  a_id: u64,
  la: &mut VecLog,
  sa: &mut AsyncStable,
  b: &mut MultiRaft<u64, u64, CountSm>,
  b_id: u64,
  lb: &mut VecLog,
  sb: &mut AsyncStable,
  now: Instant,
  gid: u64,
  tap_a_to_b: &mut dyn FnMut(&Message<u64>),
) {
  loop {
    let mut moved = false;
    while let Some((g, out)) = a.poll_message() {
      let (to, msg) = out.into_parts();
      if g == gid && to == b_id {
        tap_a_to_b(&msg);
        b.handle_message(&g, now, lb, sb, a_id, msg).unwrap();
        drain_storage(b, g, now, lb, sb);
        moved = true;
      }
    }
    while let Some((g, out)) = b.poll_message() {
      let (to, msg) = out.into_parts();
      if g == gid && to == a_id {
        a.handle_message(&g, now, la, sa, b_id, msg).unwrap();
        drain_storage(a, g, now, la, sa);
        moved = true;
      }
    }
    if !moved {
      break;
    }
  }
}

#[test]
fn fork_boots_at_the_synthetic_baseline() {
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  let blob = fork_blob(3);
  m.create_group_from_fork(
    7,
    0,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    preloaded_sm(3),
    blob.clone(),
    None,
    1,
    &mut log,
    &mut stable,
  )
  .unwrap();

  let ep = m.group(&7).unwrap();
  assert_eq!(ep.applied_index(), FORK_BASE_INDEX);
  assert_eq!(ep.commit_index(), FORK_BASE_INDEX);
  assert!(ep.role().is_follower());
  assert!(!ep.is_poisoned());
  assert_eq!(ep.term(), FORK_BASE_TERM, "boot term reads the baseline");
  assert_eq!(ep.state_machine().count(), 3, "restored from the blob");

  // The stores hold the exact post-install shape: log compacted through the baseline, the
  // authoritative blob persisted at (1, 1), HardState at the baseline term.
  assert_eq!(log.first_index().get(), 2, "compacted through the baseline");
  assert_eq!(log.last_index().get(), 1);
  assert_eq!(
    log.term(FORK_BASE_INDEX).unwrap(),
    FORK_BASE_TERM,
    "the boundary term is retained"
  );
  let (meta, stored) = stable.snapshot().expect("baseline blob persisted");
  assert_eq!(
    (meta.last_index(), meta.last_term()),
    (FORK_BASE_INDEX, FORK_BASE_TERM)
  );
  assert_eq!(stored, blob, "the stored blob IS the preloaded state");
  assert_eq!(stable.hard_state().term(), FORK_BASE_TERM);

  assert!(m.poll_message().is_none(), "a fork boots silent");
  assert!(m.poll_event().is_none(), "and surfaces no replay events");
}

#[test]
fn fork_admission_matches_create_group() {
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  m.create_group(
    1,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    CountSm::default(),
  )
  .unwrap();

  // Refusals precede EVERY store write: after each one the fresh stores are untouched.
  let untouched = |log: &VecLog, stable: &AsyncStable| {
    assert_eq!(log.first_index().get(), 1, "log untouched on refusal");
    assert_eq!(log.last_index().get(), 0);
    assert!(stable.snapshot().is_none(), "snapshot slot untouched");
    assert_eq!(stable.hard_state().term(), Term::ZERO);
  };

  assert_eq!(
    m.create_group_from_fork(
      1,
      0,
      single_node_cfg(1),
      Instant::ORIGIN,
      42,
      preloaded_sm(3),
      fork_blob(3),
      None,
      1,
      &mut log,
      &mut stable
    ),
    Err(CreateGroupError::Exists)
  );
  untouched(&log, &stable);

  assert_eq!(
    m.create_group_from_fork(
      2,
      0,
      single_node_cfg(2),
      Instant::ORIGIN,
      42,
      preloaded_sm(3),
      fork_blob(3),
      None,
      1,
      &mut log,
      &mut stable
    ),
    Err(CreateGroupError::NodeIdMismatch)
  );
  untouched(&log, &stable);

  let mut sized: MultiRaft<SizedId, u64, CountSm> = MultiRaft::new();
  for bad in [SizedId(0), SizedId(1025)] {
    assert_eq!(
      sized.create_group_from_fork(
        bad,
        0,
        single_node_cfg(1),
        Instant::ORIGIN,
        42,
        preloaded_sm(3),
        fork_blob(3),
        None,
        1,
        &mut log,
        &mut stable
      ),
      Err(CreateGroupError::InvalidGroupId)
    );
    untouched(&log, &stable);
  }
}

/// A fork refuses `boot_epoch == 0` — on BOTH constructor variants — before touching anything:
/// the manufactured baseline's writes ride the prior epoch, which epoch 0 does not have, so
/// admitting it would issue the baseline's completions in the child's own first live epoch (see
/// the aliasing regression below for the wrongness that releases). The refusal precedes every
/// store write AND every container mutation: the stores stay byte-pristine (no queued
/// completion) and the host identity is never latched.
#[test]
fn fork_refuses_boot_epoch_zero() {
  let pristine = |log: &VecLog, stable: &AsyncStable| {
    assert_eq!(log.first_index().get(), 1, "log untouched on refusal");
    assert_eq!(log.last_index().get(), 0);
    assert!(!log.has_pending(), "no log completion was ever queued");
    assert!(stable.snapshot().is_none(), "snapshot slot untouched");
    assert_eq!(stable.hard_state().term(), Term::ZERO);
    assert!(
      !stable.has_pending(),
      "no stable completion was ever queued"
    );
  };

  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  assert_eq!(
    m.create_group_from_fork(
      7,
      0,
      single_node_cfg(1),
      Instant::ORIGIN,
      42,
      preloaded_sm(3),
      fork_blob(3),
      None,
      0,
      &mut log,
      &mut stable
    ),
    Err(CreateGroupError::InvalidBootEpoch)
  );
  pristine(&log, &stable);
  assert!(m.is_empty(), "nothing was admitted");
  assert!(m.host_id().is_none(), "the refusal precedes the id latch");

  assert_eq!(
    m.create_group_from_fork_with_rng(
      7,
      0,
      single_node_cfg(1),
      Instant::ORIGIN,
      Prng::new(42),
      preloaded_sm(3),
      fork_blob(3),
      None,
      0,
      &mut log,
      &mut stable
    ),
    Err(CreateGroupError::InvalidBootEpoch)
  );
  pristine(&log, &stable);
  assert!(m.is_empty());

  // The floor itself admits: the SAME stores and container accept the fork at epoch 1.
  m.create_group_from_fork(
    7,
    0,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    preloaded_sm(3),
    fork_blob(3),
    None,
    1,
    &mut log,
    &mut stable,
  )
  .unwrap();
  assert_eq!(m.group(&7).unwrap().applied_index(), FORK_BASE_INDEX);
}

#[test]
fn fork_refuses_used_stores_before_any_write() {
  // The manufactured baseline OVERWRITES whatever the stores hold, so a fork is only ever
  // written over VIRGIN stores. Each leg of the used-storage probe refuses on its own — a
  // visible hard state, an occupied snapshot slot, log content, and a compacted (re-baselined)
  // log — and the refusal precedes every store write, so the held state survives untouched.
  let fork = |m: &mut MultiRaft<u64, u64, CountSm>, log: &mut VecLog, stable: &mut AsyncStable| {
    m.create_group_from_fork(
      7,
      0,
      single_node_cfg(1),
      Instant::ORIGIN,
      42,
      preloaded_sm(3),
      fork_blob(3),
      None,
      1,
      log,
      stable,
    )
  };

  // A visible hard state alone (term 1, nothing else).
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  stable.force_state(Term::new(1), None, Index::ZERO);
  assert_eq!(
    fork(&mut m, &mut log, &mut stable),
    Err(CreateGroupError::StorageInUse)
  );
  assert_eq!(
    stable.hard_state().term(),
    Term::new(1),
    "the held state survives the refusal"
  );
  assert!(m.is_empty(), "nothing was admitted");

  // An occupied snapshot slot alone.
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  stable.force_snapshot(
    crate::SnapshotMeta::new(
      Index::new(2),
      Term::new(1),
      crate::ConfState::from_voters(std::vec![1u64]),
    ),
    fork_blob(9),
  );
  assert_eq!(
    fork(&mut m, &mut log, &mut stable),
    Err(CreateGroupError::StorageInUse)
  );
  let (_, held) = stable.snapshot().expect("the held snapshot survives");
  assert_eq!(held, fork_blob(9), "the refusal never touched the slot");

  // Log content alone.
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  log.force_append(&[crate::Entry::new(
    Term::new(1),
    Index::new(1),
    crate::EntryKind::Normal,
    Bytes::new(),
  )]);
  assert_eq!(
    fork(&mut m, &mut log, &mut stable),
    Err(CreateGroupError::StorageInUse)
  );
  assert_eq!(log.last_index().get(), 1, "the held log survives");

  // A compacted (re-baselined) log alone, and the `_with_rng` twin walks the same gate.
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  log.restore(Index::new(3), Term::new(1));
  assert_eq!(
    m.create_group_from_fork_with_rng(
      7,
      0,
      single_node_cfg(1),
      Instant::ORIGIN,
      Prng::new(42),
      preloaded_sm(3),
      fork_blob(3),
      None,
      1,
      &mut log,
      &mut stable
    ),
    Err(CreateGroupError::StorageInUse)
  );
  assert_eq!(log.first_index().get(), 4, "the held boundary survives");
  assert!(m.is_empty(), "no leg admitted anything");

  // The same container and shape over VIRGIN stores admits — the probe fences used storage
  // only, so the legitimate crash-before-flush replay (nothing durable) re-forks freely.
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  fork(&mut m, &mut log, &mut stable).unwrap();
  assert_eq!(m.group(&7).unwrap().applied_index(), FORK_BASE_INDEX);
}

/// WHY the guard exists, demonstrated at the endpoint level. The pre-guard fork shape at
/// `boot_epoch = 0` — reproduced by calling `write_fork_baseline` directly, exactly what
/// `create_group_from_fork` did before refusing 0 — collapses the baseline's prior-epoch write
/// ids into epoch 0, the same epoch the child's op counter is seeded with. Observed pre-fix
/// failure mode (the red proof this test pins): the campaign's self-vote write is minted at
/// `(0, 0)` — the id of the QUEUED baseline HardState write — so draining the BASELINE's
/// `Wrote(0, 0)` matched the pending `Campaign` action and fired `become_leader` while the
/// self-vote's own fsync was still in flight: leadership on a phantom durable self-vote (a
/// crash in that window forgets the vote, and a revote could grant the same term elsewhere).
/// At the enforced floor (`boot_epoch >= 1`) the identical drive stays a candidate until the
/// REAL completion lands — the baseline's completions release nothing.
#[test]
fn epoch_zero_baseline_completions_alias_the_childs_first_live_ops() {
  // The collision, stated on the ids themselves: at boot epoch 0 the "prior epoch" the baseline
  // rides IS the child's own first epoch.
  assert_eq!(
    OpId::first_of_epoch(0u64.saturating_sub(1)),
    OpId::first_of_epoch(0)
  );

  // The pre-guard shape: baseline manufactured at epoch 0, its completions still queued; every
  // write the child itself submits from here has its fsync in flight (completion held).
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  write_fork_baseline(
    &single_node_cfg(1),
    fork_blob(3),
    0,
    None,
    0,
    &mut log,
    &mut stable,
  );
  let mut ep = Endpoint::restart(
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    CountSm::default(),
    0,
    &mut log,
    &mut stable,
  );
  stable.hold_writes(true);
  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable);
  assert!(ep.role().is_candidate(), "the self-vote is not yet durable");
  ep.handle_storage(d, &mut log, &mut stable);
  assert_eq!(
    stable.held_write_count(),
    1,
    "the self-vote's own completion never arrived"
  );
  assert!(
    ep.role().is_leader(),
    "the aliased baseline Wrote(0,0) released become_leader without the vote's durability — \
     the wrongness the fork constructors' epoch guard forecloses"
  );

  // The enforced floor: the identical drive at boot_epoch 1 keeps the persist-before-act gate —
  // the baseline's epoch-0 completions miss every live pending op and every >= watermark.
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  write_fork_baseline(
    &single_node_cfg(1),
    fork_blob(3),
    0,
    None,
    1,
    &mut log,
    &mut stable,
  );
  let mut ep = Endpoint::restart(
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    CountSm::default(),
    1,
    &mut log,
    &mut stable,
  );
  stable.hold_writes(true);
  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable);
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(
    ep.role().is_candidate(),
    "the baseline's completions release nothing at epoch >= 1"
  );
  stable.flush_held_writes();
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(
    ep.role().is_leader(),
    "leadership waits for the vote write's OWN durability"
  );
}

#[test]
fn fork_blob_is_authoritative_over_the_vessel() {
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  // The vessel disagrees with the blob: the blob must win identically everywhere.
  m.create_group_from_fork(
    7,
    0,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    preloaded_sm(5),
    fork_blob(9),
    None,
    1,
    &mut log,
    &mut stable,
  )
  .unwrap();
  assert_eq!(
    m.group(&7).unwrap().state_machine().count(),
    9,
    "the BLOB's content is the post-boot state; the vessel's is absorbed"
  );

  // Live continuity off the blob baseline: a one-node elect + commit applies ON TOP of 9.
  let d = m.group(&7).unwrap().poll_timeout().unwrap();
  m.handle_timeout(&7, d, &mut log, &mut stable).unwrap();
  drain_storage(&mut m, 7, d, &mut log, &mut stable);
  assert!(m.group(&7).unwrap().role().is_leader());
  let cmd = Bytes::from_static(b"x");
  m.propose(&7, d, &mut log, &stable, &cmd).unwrap().unwrap();
  m.flush_appends(&7, d, &log, &stable).unwrap();
  drain_storage(&mut m, 7, d, &mut log, &mut stable);
  assert_eq!(m.group(&7).unwrap().state_machine().count(), 10);
}

#[test]
fn fork_with_a_corrupt_blob_poisons_only_that_group() {
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  m.create_group(
    1,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    CountSm::default(),
  )
  .unwrap();

  // A blob that cannot decode as the FSM snapshot: the fork constructor still returns Ok (the
  // restart discipline — construction is infallible, the GROUP is poisoned), and only group 7
  // is dead.
  let (mut log7, mut stable7) = (VecLog::default(), AsyncStable::default());
  m.create_group_from_fork(
    7,
    0,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    CountSm::default(),
    Bytes::from_static(&[1, 2, 3]),
    None,
    1,
    &mut log7,
    &mut stable7,
  )
  .unwrap();
  let ep = m.group(&7).unwrap();
  assert!(ep.is_poisoned(), "a corrupt blob poisons at construction");
  assert_eq!(ep.poison_reason(), Some(PoisonReason::SnapshotDecode));

  // The sibling group on the same container keeps working.
  let (mut log1, mut stable1) = (VecLog::default(), AsyncStable::default());
  let d = m.group(&1).unwrap().poll_timeout().unwrap();
  m.handle_timeout(&1, d, &mut log1, &mut stable1).unwrap();
  drain_storage(&mut m, 1, d, &mut log1, &mut stable1);
  assert!(m.group(&1).unwrap().role().is_leader());
  let cmd = Bytes::from_static(b"x");
  m.propose(&1, d, &mut log1, &stable1, &cmd)
    .unwrap()
    .unwrap();
  m.flush_appends(&1, d, &log1, &stable1).unwrap();
  drain_storage(&mut m, 1, d, &mut log1, &mut stable1);
  assert_eq!(m.group(&1).unwrap().state_machine().count(), 1);
}

#[test]
fn forked_group_elects_off_the_baseline() {
  fn voter_pair_cfg(id: u64) -> Config<u64> {
    Config::try_new(
      id,
      std::vec![1, 2],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap()
  }
  let mut a: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let mut b: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let (mut la, mut sa) = (VecLog::default(), AsyncStable::default());
  let (mut lb, mut sb) = (VecLog::default(), AsyncStable::default());
  // Both replicas fork the SAME blob — the fork contract for a multi-replica child.
  a.create_group_from_fork(
    7,
    0,
    voter_pair_cfg(1),
    Instant::ORIGIN,
    42,
    preloaded_sm(4),
    fork_blob(4),
    None,
    1,
    &mut la,
    &mut sa,
  )
  .unwrap();
  b.create_group_from_fork(
    7,
    0,
    voter_pair_cfg(2),
    Instant::ORIGIN,
    43,
    preloaded_sm(4),
    fork_blob(4),
    None,
    1,
    &mut lb,
    &mut sb,
  )
  .unwrap();

  // Node 1 campaigns: the vote's up-to-date checks read (1, 1) off the meta on BOTH sides —
  // the election succeeding IS the last-entry derivation pin.
  let d = a.group(&7).unwrap().poll_timeout().unwrap();
  a.handle_timeout(&7, d, &mut la, &mut sa).unwrap();
  drain_storage(&mut a, 7, d, &mut la, &mut sa);
  route_until_quiescent(
    &mut a,
    1,
    &mut la,
    &mut sa,
    &mut b,
    2,
    &mut lb,
    &mut sb,
    d,
    7,
    &mut |_| {},
  );
  let leader = a.group(&7).unwrap();
  assert!(leader.role().is_leader(), "the forked pair elects");
  assert!(
    leader.term() >= Term::new(2),
    "the first campaign runs ABOVE the baseline term, got {:?}",
    leader.term()
  );
  assert!(
    leader.commit_index() >= Index::new(2),
    "the no-op above the baseline commits at index 2"
  );

  // One heartbeat round propagates the commit to the follower.
  let hb = a.group(&7).unwrap().poll_timeout().unwrap();
  a.handle_timeout(&7, hb, &mut la, &mut sa).unwrap();
  drain_storage(&mut a, 7, hb, &mut la, &mut sa);
  route_until_quiescent(
    &mut a,
    1,
    &mut la,
    &mut sa,
    &mut b,
    2,
    &mut lb,
    &mut sb,
    hb,
    7,
    &mut |_| {},
  );
  assert!(
    b.group(&7).unwrap().commit_index() >= Index::new(2),
    "the follower commits the same index"
  );
  assert_eq!(b.group(&7).unwrap().state_machine().count(), 4);
}

#[test]
fn fork_then_store_roundtrip_equals_restore() {
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  m.create_group_from_fork(
    7,
    0,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    preloaded_sm(6),
    fork_blob(6),
    None,
    1,
    &mut log,
    &mut stable,
  )
  .unwrap();

  // A SECOND container restoring a FRESH vessel from the same stores is indistinguishable:
  // the fork IS a manufactured install, so restore recovers it as one.
  let mut r: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  r.restore_group(
    7,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    CountSm::default(),
    2,
    &mut log,
    &mut stable,
  )
  .unwrap();

  let forked = m.group(&7).unwrap();
  let restored = r.group(&7).unwrap();
  assert_eq!(restored.term(), forked.term());
  assert_eq!(restored.applied_index(), forked.applied_index());
  assert_eq!(restored.commit_index(), forked.commit_index());
  assert_eq!(
    restored.state_machine().count(),
    forked.state_machine().count()
  );
  assert!(restored.role().is_follower());
  assert!(!restored.is_poisoned());
}

fn single_node_cfg(id: u64) -> Config<u64> {
  Config::try_new(
    id,
    std::vec![id],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
}

#[test]
fn host_identity_is_latched_across_group_removal() {
  let mut mr = MultiRaft::<u64, u64, CountSm>::new();
  assert!(mr.host_id().is_none(), "no identity before any admission");
  mr.create_group(
    1,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    CountSm::default(),
  )
  .unwrap();
  assert_eq!(mr.host_id(), Some(&1));

  // Removing the LAST group must not un-latch the identity: live transport connections stay
  // authenticated under it, so a re-created group with a different id would silently wedge.
  assert!(mr.remove_group(&1).is_some());
  assert!(mr.is_empty());
  assert_eq!(mr.host_id(), Some(&1), "identity survives an empty host");
  assert_eq!(
    mr.create_group(
      2,
      single_node_cfg(2),
      Instant::ORIGIN,
      42,
      CountSm::default()
    ),
    Err(CreateGroupError::NodeIdMismatch)
  );
  // The latched id re-admits.
  mr.create_group(
    2,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    CountSm::default(),
  )
  .unwrap();
  assert_eq!(mr.host_id(), Some(&1));
}

#[test]
fn two_groups_are_isolated() {
  let mut mr = MultiRaft::<u64, u64, CountSm>::new();
  mr.create_group(
    1,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    CountSm::default(),
  )
  .unwrap();
  mr.create_group(
    2,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    CountSm::default(),
  )
  .unwrap();
  let (mut l1, mut s1) = (VecLog::default(), AsyncStable::default());

  // Drive group 1 (single voter {1}) to leadership, then commit one command. Group 2 is never
  // touched. A single fixed `now` mirrors the single-node drive in `testkit`.
  let d = mr.group(&1).unwrap().poll_timeout().unwrap();
  mr.handle_timeout(&1, d, &mut l1, &mut s1).unwrap(); // campaign
  mr.handle_storage(&1, d, &mut l1, &mut s1).unwrap(); // self-vote durable -> leader
  mr.handle_storage(&1, d, &mut l1, &mut s1).unwrap(); // drain the leader no-op append
  while let Some((g, _)) = mr.poll_message() {
    assert_eq!(g, 1, "only group 1 was driven");
  }
  while let Some((g, _)) = mr.poll_event() {
    assert_eq!(g, 1, "every event is stamped with the originating group");
  }
  assert!(mr.group(&1).unwrap().role().is_leader());

  let cmd = Bytes::copy_from_slice(&[7u8]);
  mr.propose(&1, d, &mut l1, &s1, &cmd).unwrap().unwrap();
  mr.flush_appends(&1, d, &l1, &s1).unwrap();
  mr.handle_storage(&1, d, &mut l1, &mut s1).unwrap(); // quorum=1 auto-commits + applies
  while let Some((g, _)) = mr.poll_message() {
    assert_eq!(g, 1);
  }
  while let Some((g, _)) = mr.poll_event() {
    assert_eq!(g, 1);
  }

  // Group 1 applied at least the command; group 2 is pristine and never emitted output.
  assert!(mr.group(&1).unwrap().state_machine().count() >= 1);
  assert_eq!(mr.group(&2).unwrap().state_machine().count(), 0);
  assert!(mr.group(&2).unwrap().role().is_follower());
}

#[test]
fn unknown_group_is_none() {
  let mut mr = MultiRaft::<u64, u64, CountSm>::new();
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  assert!(
    mr.handle_timeout(&99, Instant::ORIGIN, &mut log, &mut stable)
      .is_none()
  );
  assert!(
    mr.handle_storage(&99, Instant::ORIGIN, &mut log, &mut stable)
      .is_none()
  );
  assert!(mr.poll_message().is_none());
  assert!(mr.poll_event().is_none());
  assert!(mr.poll_timeout().is_none());
}

#[test]
fn create_dup_errors_and_remove_returns_the_group() {
  let mut mr = MultiRaft::<u64, u64, CountSm>::new();
  mr.create_group(
    1,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    CountSm::default(),
  )
  .unwrap();
  assert_eq!(
    mr.create_group(
      1,
      single_node_cfg(1),
      Instant::ORIGIN,
      42,
      CountSm::default()
    ),
    Err(CreateGroupError::Exists)
  );
  assert_eq!(mr.len(), 1);
  assert!(mr.remove_group(&1).is_some());
  assert!(mr.is_empty());
  assert!(mr.remove_group(&1).is_none());
}

#[test]
fn mismatched_node_id_is_rejected() {
  let mut mr = MultiRaft::<u64, u64, CountSm>::new();
  mr.create_group(
    1,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    CountSm::default(),
  )
  .unwrap();
  // A second group configured with a DIFFERENT local node id: refused (a multi-Raft host is one
  // physical node), and the hosted set is untouched.
  assert_eq!(
    mr.create_group(
      2,
      single_node_cfg(2),
      Instant::ORIGIN,
      42,
      CountSm::default()
    ),
    Err(CreateGroupError::NodeIdMismatch)
  );
  assert_eq!(mr.len(), 1);
  // The same id on a new group id is admitted.
  mr.create_group(
    2,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    CountSm::default(),
  )
  .unwrap();
  assert_eq!(mr.len(), 2);
}

/// A test-only group id whose `Data` encoding is exactly `self.0` bytes — exercises the wire
/// bound on the encoded group id (`0` = empty, large = oversized).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
struct SizedId(usize);

impl core::fmt::Display for SizedId {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "sized-{}", self.0)
  }
}

impl CheapClone for SizedId {}

impl Data for SizedId {
  fn encode(&self, buf: &mut Vec<u8>) {
    buf.extend(core::iter::repeat_n(0xAB, self.0));
  }
  fn decode(_: &mut crate::ByteCursor) -> Result<Self, crate::DecodeError> {
    Err(crate::DecodeError::Invalid("test-only id is never decoded"))
  }
}

#[test]
fn out_of_bound_group_id_encodings_are_rejected() {
  let mut mr = MultiRaft::<SizedId, u64, CountSm>::new();
  for bad in [SizedId(0), SizedId(1025)] {
    assert_eq!(
      mr.create_group(
        bad,
        single_node_cfg(1),
        Instant::ORIGIN,
        42,
        CountSm::default()
      ),
      Err(CreateGroupError::InvalidGroupId)
    );
  }
  assert!(mr.is_empty());
  // The bound is inclusive: exactly 1024 bytes is admitted.
  mr.create_group(
    SizedId(1024),
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    CountSm::default(),
  )
  .unwrap();
}

#[test]
fn group_seed_decorrelates_co_located_groups() {
  // Different group ids under the same base seed must yield different election seeds; the base
  // seed still matters for a fixed id.
  assert_ne!(group_seed(42, &1u64), group_seed(42, &2u64));
  assert_ne!(group_seed(0, &1u64), group_seed(0, &2u64));
  assert_ne!(group_seed(42, &7u64), group_seed(43, &7u64));
}

fn two_voter_cfg() -> Config<u64> {
  Config::try_new(
    1,
    std::vec![1, 2],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
}

/// Restoring a group from EMPTY stores mirrors `Endpoint::restart`: replay surfaces no events and
/// no messages, and the restored group then campaigns like a fresh one.
#[test]
fn restore_from_empty_stores_surfaces_no_events() {
  let mut mr = MultiRaft::<u64, u64, CountSm>::new();
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  mr.restore_group(
    1,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    CountSm::default(),
    1,
    &mut log,
    &mut stable,
  )
  .unwrap();
  assert!(mr.poll_event().is_none(), "replay surfaces no events");
  assert!(mr.poll_message().is_none(), "and no messages");
  assert!(mr.group(&1).unwrap().role().is_follower());

  // The restored single-voter group campaigns normally (the same drive as the isolation test).
  let d = mr.group(&1).unwrap().poll_timeout().unwrap();
  mr.handle_timeout(&1, d, &mut log, &mut stable).unwrap(); // campaign
  mr.handle_storage(&1, d, &mut log, &mut stable).unwrap(); // self-vote durable -> leader
  mr.handle_storage(&1, d, &mut log, &mut stable).unwrap(); // drain the leader no-op append
  assert!(mr.group(&1).unwrap().role().is_leader());
}

/// Removing a group whose outbound queue is non-empty leaves a stale dirty entry behind: the next
/// drain must skip it silently (no panic, no cross-group leak), and the id is admissible again.
#[test]
fn remove_group_with_queued_output_is_safe() {
  let mut mr = MultiRaft::<u64, u64, CountSm>::new();
  mr.create_group(1, two_voter_cfg(), Instant::ORIGIN, 42, CountSm::default())
    .unwrap();
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());

  // A 2-voter campaign queues vote traffic to peer 2 — output pending, deliberately undrained.
  let d = mr.group(&1).unwrap().poll_timeout().unwrap();
  mr.handle_timeout(&1, d, &mut log, &mut stable).unwrap();

  let mut ep = mr.remove_group(&1).expect("the group is returned");
  assert!(
    ep.poll_message().is_some(),
    "output WAS queued at removal (the dirty entry is genuinely stale)"
  );
  assert!(
    mr.poll_message().is_none(),
    "the stale dirty entry is skipped"
  );
  assert!(mr.poll_event().is_none());
  assert!(mr.remove_group(&1).is_none());

  // The same gid is admissible again.
  mr.create_group(1, two_voter_cfg(), Instant::ORIGIN, 42, CountSm::default())
    .unwrap();
  assert!(mr.contains_group(&1));
}

/// Two groups dispatched back-to-back drain in GROUP BATCHES: every queued group-100 message
/// surfaces (stamped 100) before any group-200 message, each group's fan-out is complete, and
/// nothing is lost or re-stamped.
#[test]
fn interleaved_groups_drain_in_group_batches() {
  fn three_voter_cfg() -> Config<u64> {
    Config::try_new(
      1,
      std::vec![1, 2, 3],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap()
  }

  let mut mr = MultiRaft::<u64, u64, CountSm>::new();
  mr.create_group(
    100,
    three_voter_cfg(),
    Instant::ORIGIN,
    42,
    CountSm::default(),
  )
  .unwrap();
  mr.create_group(
    200,
    three_voter_cfg(),
    Instant::ORIGIN,
    42,
    CountSm::default(),
  )
  .unwrap();
  let (mut l1, mut s1) = (VecLog::default(), AsyncStable::default());
  let (mut l2, mut s2) = (VecLog::default(), AsyncStable::default());

  // Fire both elections BEFORE draining anything: each 3-voter campaign fans out to peers 2 and 3.
  let d1 = mr.group(&100).unwrap().poll_timeout().unwrap();
  let d2 = mr.group(&200).unwrap().poll_timeout().unwrap();
  let now = d1.max(d2);
  mr.handle_timeout(&100, now, &mut l1, &mut s1).unwrap();
  mr.handle_storage(&100, now, &mut l1, &mut s1).unwrap();
  mr.handle_timeout(&200, now, &mut l2, &mut s2).unwrap();
  mr.handle_storage(&200, now, &mut l2, &mut s2).unwrap();

  let mut order = Vec::new();
  let mut dests_100 = std::collections::BTreeSet::new();
  let mut dests_200 = std::collections::BTreeSet::new();
  while let Some((g, out)) = mr.poll_message() {
    order.push(g);
    let (to, _) = out.into_parts();
    match g {
      100 => dests_100.insert(to),
      200 => dests_200.insert(to),
      other => panic!("a message stamped with an unknown group {other}"),
    };
  }
  assert!(mr.poll_message().is_none(), "fully drained");

  // Group-batched order: all of group 100 contiguously, then all of group 200.
  let boundary = order
    .iter()
    .position(|&g| g == 200)
    .expect("group 200 emitted something");
  assert!(
    order[..boundary].iter().all(|&g| g == 100),
    "every group-100 message precedes group 200's: {order:?}"
  );
  assert!(
    order[boundary..].iter().all(|&g| g == 200),
    "group 200's batch is contiguous: {order:?}"
  );
  assert!(boundary >= 2, "the 3-voter campaign fanned out: {order:?}");
  // Nothing lost: each campaign reached BOTH peers.
  assert_eq!(dests_100, [2, 3].into_iter().collect());
  assert_eq!(dests_200, [2, 3].into_iter().collect());
}

/// T2's shared drive: fork a single-voter group at a preloaded count of 3, commit a two-command
/// tail above the baseline, AddNode(2), then replicate toward a FRESH zero-progress joiner until
/// it catches the leader — returning both hosts and the full leader→joiner message transcript.
struct ForkJoin {
  a: MultiRaft<u64, u64, CountSm>,
  b: MultiRaft<u64, u64, CountSm>,
  transcript: Vec<Message<u64>>,
}

fn drive_forked_leader_and_fresh_joiner() -> ForkJoin {
  use crate::{ConfChangeType, conf::ConfChange};

  let mut a: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let (mut la, mut sa) = (VecLog::default(), AsyncStable::default());
  a.create_group_from_fork(
    7,
    0,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    preloaded_sm(3),
    fork_blob(3),
    None,
    1,
    &mut la,
    &mut sa,
  )
  .unwrap();

  // Elect the single voter and commit a tail above the baseline, then add node 2.
  let mut now = a.group(&7).unwrap().poll_timeout().unwrap();
  a.handle_timeout(&7, now, &mut la, &mut sa).unwrap();
  drain_storage(&mut a, 7, now, &mut la, &mut sa);
  assert!(a.group(&7).unwrap().role().is_leader());
  for payload in [&b"t1"[..], &b"t2"[..]] {
    let cmd = Bytes::copy_from_slice(payload);
    a.propose(&7, now, &mut la, &sa, &cmd).unwrap().unwrap();
  }
  a.flush_appends(&7, now, &la, &sa).unwrap();
  drain_storage(&mut a, 7, now, &mut la, &mut sa);
  a.propose_conf_change(
    &7,
    now,
    &mut la,
    &sa,
    ConfChange::new(ConfChangeType::AddNode, 2u64, Bytes::new()),
  )
  .unwrap()
  .unwrap();
  a.flush_appends(&7, now, &la, &sa).unwrap();
  drain_storage(&mut a, 7, now, &mut la, &mut sa);
  let leader_applied = a.group(&7).unwrap().applied_index();

  // The joiner materializes EMPTY (the factory shape): zero progress, nothing preloaded.
  let mut b: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let (mut lb, mut sb) = (VecLog::default(), AsyncStable::default());
  b.create_group(
    7,
    Config::try_new(
      2u64,
      std::vec![1, 2],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap(),
    Instant::ORIGIN,
    43,
    CountSm::default(),
  )
  .unwrap();

  let mut transcript: Vec<Message<u64>> = Vec::new();
  for _ in 0..300 {
    route_until_quiescent(
      &mut a,
      1,
      &mut la,
      &mut sa,
      &mut b,
      2,
      &mut lb,
      &mut sb,
      now,
      7,
      &mut |m| transcript.push(m.clone()),
    );
    if b.group(&7).unwrap().applied_index() >= leader_applied {
      break;
    }
    now = a
      .group(&7)
      .unwrap()
      .poll_timeout()
      .expect("the leader keeps a heartbeat deadline armed");
    a.handle_timeout(&7, now, &mut la, &mut sa).unwrap();
    drain_storage(&mut a, 7, now, &mut la, &mut sa);
  }
  ForkJoin { a, b, transcript }
}

#[test]
fn a_zero_progress_joiner_is_forced_onto_the_snapshot_path() {
  let fj = drive_forked_leader_and_fresh_joiner();

  // The first message that CAN land payload on a zero-progress joiner must be the snapshot. An
  // AppendEntries attaches at match 0 only with prev_log_index 0 (the log walk the manufactured
  // install exists to make structurally impossible); the leader's optimistic new-peer append of
  // its freshest entry (prev at the tail) bounces off the empty log and is harmless.
  let first_attachable = fj.transcript.iter().find(|m| match m {
    Message::InstallSnapshot(_) => true,
    Message::AppendEntries(ae) => ae.prev_log_index() == Index::ZERO,
    _ => false,
  });
  assert!(
    matches!(first_attachable, Some(Message::InstallSnapshot(_))),
    "the first zero-progress-attachable payload must be InstallSnapshot, got {first_attachable:?}"
  );

  let mut installs = 0usize;
  for m in &fj.transcript {
    match m {
      Message::AppendEntries(ae) => assert!(
        ae.prev_log_index() >= FORK_BASE_INDEX,
        "a forked leader must never serve the log below the baseline (prev {:?})",
        ae.prev_log_index()
      ),
      Message::InstallSnapshot(is) => {
        installs += 1;
        assert!(is.snapshot().last_index() >= FORK_BASE_INDEX);
        if !is.data().is_empty() {
          assert_eq!(is.data(), &fork_blob(3), "the served blob IS the fork blob");
        }
      }
      _ => {}
    }
  }
  assert!(installs > 0, "the snapshot path was actually exercised");
}

#[test]
fn the_joiner_lands_on_the_preloaded_state_plus_tail() {
  let fj = drive_forked_leader_and_fresh_joiner();
  let leader = fj.a.group(&7).unwrap();
  let joiner = fj.b.group(&7).unwrap();
  assert_eq!(
    joiner.applied_index(),
    leader.applied_index(),
    "the joiner catches the leader"
  );
  // 3 preloaded + 2 tail commands: an empty-booted joiner replaying only the tail would sit at
  // 2 — equality proves the preloaded baseline arrived through the snapshot.
  assert_eq!(leader.state_machine().count(), 5);
  assert_eq!(
    joiner.state_machine().count(),
    5,
    "preloaded state AND the tail are both present on the joiner"
  );
}

// ───────────────────────────── committed splits: propose gates + the fork relay ──────────────

/// A splitting counter: `split` gives away `min(units, instruction[0])` units as the child half;
/// `apply` adds one unit per command. `Snapshot = u64` keeps the blob the `Data` encoding the
/// container relay round-trips.
#[derive(Debug, Default, PartialEq, Eq)]
struct SplitSm {
  units: u64,
}

impl crate::StateMachine for SplitSm {
  type Command = Bytes;
  type Response = u64;
  type Snapshot = u64;
  type Error = core::convert::Infallible;

  fn apply(&mut self, _index: crate::Index, _cmd: Bytes) -> Result<u64, Self::Error> {
    self.units += 1;
    Ok(self.units)
  }

  fn snapshot(&self) -> Result<u64, Self::Error> {
    Ok(self.units)
  }

  fn restore(&mut self, snapshot: u64) -> Result<(), Self::Error> {
    self.units = snapshot;
    Ok(())
  }

  fn split(&mut self, instruction: &[u8]) -> Option<Self> {
    let give = u64::from(*instruction.first()?).min(self.units);
    self.units -= give;
    Some(Self { units: give })
  }
}

/// Drive `gid` (single voter) to leadership on `m` under one fixed instant.
fn lead_single_split(
  m: &mut MultiRaft<u64, u64, SplitSm>,
  gid: u64,
  log: &mut VecLog,
  stable: &mut AsyncStable,
) -> Instant {
  let d = m.group(&gid).unwrap().poll_timeout().unwrap();
  m.handle_timeout(&gid, d, log, stable).unwrap();
  m.handle_storage(&gid, d, log, stable).unwrap();
  m.handle_storage(&gid, d, log, stable).unwrap();
  while m.poll_message().is_some() {}
  while m.poll_event().is_some() {}
  assert!(m.group(&gid).unwrap().role().is_leader());
  d
}

/// Commit one command on a single-voter leader (append + flush + storage drain).
fn commit_one_split(
  m: &mut MultiRaft<u64, u64, SplitSm>,
  gid: u64,
  d: Instant,
  log: &mut VecLog,
  stable: &mut AsyncStable,
) {
  let cmd = Bytes::from_static(b"c");
  m.propose(&gid, d, log, stable, &cmd).unwrap().unwrap();
  m.flush_appends(&gid, d, log, stable).unwrap();
  while matches!(
    m.handle_storage(&gid, d, log, stable),
    Some(StorageProgress::MorePending)
  ) {}
  while m.poll_message().is_some() {}
  while m.poll_event().is_some() {}
}

/// The `Data` encoding of a committed `Split` entry's payload, as `propose_split` would build it.
fn split_entry_bytes(child: u64, child_gen: u64, parent_gen_after: u64, give: u8) -> Bytes {
  let mut child_bytes = Vec::new();
  Data::encode(&child, &mut child_bytes);
  let payload = crate::SplitPayload::new(
    Bytes::from(child_bytes),
    child_gen,
    parent_gen_after,
    Bytes::copy_from_slice(&[give]),
  );
  let mut buf = Vec::new();
  crate::wire::encode_split_payload(&payload, &mut buf);
  Bytes::from(buf)
}

#[test]
fn propose_split_delegator_gates() {
  // Unknown gid → None (the house delegator shape).
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  assert!(
    m.propose_split(
      &99,
      Instant::ORIGIN,
      &mut log,
      &stable,
      &200,
      0,
      Bytes::from_static(b"\x01")
    )
    .is_none()
  );

  // A follower refuses with the leader hint (none known here).
  let two_voters = Config::try_new(
    1u64,
    std::vec![1u64, 2],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  m.create_group(7, two_voters, Instant::ORIGIN, 42, SplitSm::default())
    .unwrap();
  assert_eq!(
    m.propose_split(
      &7,
      Instant::ORIGIN,
      &mut log,
      &stable,
      &200,
      0,
      Bytes::from_static(b"\x01")
    ),
    Some(Err(SplitError::NotLeader { leader: None }))
  );

  // A hosted child id refuses (the parent itself is hosted, so `child == gid` is covered too).
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  m.create_group(
    7,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    SplitSm::default(),
  )
  .unwrap();
  m.create_group(
    8,
    single_node_cfg(1),
    Instant::ORIGIN,
    43,
    SplitSm::default(),
  )
  .unwrap();
  let d = lead_single_split(&mut m, 7, &mut log, &mut stable);
  assert_eq!(
    m.propose_split(&7, d, &mut log, &stable, &8, 0, Bytes::from_static(b"\x01")),
    Some(Err(SplitError::ChildExists))
  );
  assert_eq!(
    m.propose_split(&7, d, &mut log, &stable, &7, 0, Bytes::from_static(b"\x01")),
    Some(Err(SplitError::ChildExists)),
    "the parent id itself is a hosted child id"
  );
}

#[test]
fn propose_split_refuses_a_joint_parent() {
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  m.create_group(
    7,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    SplitSm::default(),
  )
  .unwrap();
  let d = lead_single_split(&mut m, 7, &mut log, &mut stable);

  // Enter an EXPLICIT joint config (add voter 2; no auto-leave), committed by the single voter.
  let ccv2 = crate::ConfChangeV2::new(
    crate::ConfChangeTransition::Explicit,
    std::vec![crate::ConfChangeSingle::new(
      crate::ConfChangeType::AddNode,
      2u64
    )],
    Bytes::new(),
  );
  m.propose_conf_change_v2(&7, d, &mut log, &stable, ccv2)
    .unwrap()
    .unwrap();
  m.flush_appends(&7, d, &log, &stable).unwrap();
  while matches!(
    m.handle_storage(&7, d, &mut log, &mut stable),
    Some(StorageProgress::MorePending)
  ) {}
  while m.poll_message().is_some() {}
  while m.poll_event().is_some() {}
  assert!(
    m.group(&7).unwrap().conf_state().is_joint(),
    "the parent is mid-joint"
  );

  assert_eq!(
    m.propose_split(
      &7,
      d,
      &mut log,
      &stable,
      &200,
      0,
      Bytes::from_static(b"\x01")
    ),
    Some(Err(SplitError::JointConfig)),
    "a joint-config parent refuses to split (one-line rule: no joint interleaving)"
  );
}

#[test]
fn propose_split_refuses_an_over_bound_child_id() {
  /// A group id whose encoding length is its value — `1` is a valid 1-byte tag, `2000` is over
  /// the 1024-byte wire bound.
  #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
  struct WideId(usize);
  impl core::fmt::Display for WideId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
      write!(f, "{}", self.0)
    }
  }
  impl cheap_clone::CheapClone for WideId {}
  impl Data for WideId {
    fn encode(&self, buf: &mut Vec<u8>) {
      buf.extend(core::iter::repeat_n(7u8, self.0));
    }

    fn decode(cursor: &mut crate::ByteCursor) -> Result<Self, crate::DecodeError> {
      let n = cursor.remaining();
      cursor.take_bytes(n)?;
      Ok(Self(n))
    }
  }

  let mut m: MultiRaft<WideId, u64, SplitSm> = MultiRaft::new();
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  m.create_group(
    WideId(1),
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    SplitSm::default(),
  )
  .unwrap();
  let d = m.group(&WideId(1)).unwrap().poll_timeout().unwrap();
  m.handle_timeout(&WideId(1), d, &mut log, &mut stable)
    .unwrap();
  m.handle_storage(&WideId(1), d, &mut log, &mut stable)
    .unwrap();
  m.handle_storage(&WideId(1), d, &mut log, &mut stable)
    .unwrap();
  assert!(m.group(&WideId(1)).unwrap().role().is_leader());

  // An over-bound child id refuses AT PROPOSE: were it appended, every replica's relay decode
  // would poison the parent on a committed entry — a self-inflicted cluster-wide fail-stop.
  assert_eq!(
    m.propose_split(
      &WideId(1),
      d,
      &mut log,
      &stable,
      &WideId(2000),
      0,
      Bytes::from_static(b"\x01")
    ),
    Some(Err(SplitError::InvalidChild))
  );
}

#[test]
fn committed_split_relays_a_group_fork() {
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  // A distinctive knob (snapshot_threshold 1) proves the child inherits the parent's LOCAL
  // config, and arms the parent's own snapshot cadence for the barrier assertions below.
  let cfg = single_node_cfg(1)
    .with_snapshot_threshold(1)
    .with_pre_vote(true);
  m.create_group(7, cfg, Instant::ORIGIN, 42, SplitSm::default())
    .unwrap();
  let d = lead_single_split(&mut m, 7, &mut log, &mut stable);
  commit_one_split(&mut m, 7, d, &mut log, &mut stable);
  commit_one_split(&mut m, 7, d, &mut log, &mut stable);
  commit_one_split(&mut m, 7, d, &mut log, &mut stable);
  assert_eq!(m.group(&7).unwrap().state_machine().units, 3);

  // Propose the split: give 2 of the 3 units to child 200.
  let idx = m
    .propose_split(
      &7,
      d,
      &mut log,
      &stable,
      &200,
      0,
      Bytes::from_static(b"\x02"),
    )
    .unwrap()
    .unwrap();
  m.flush_appends(&7, d, &log, &stable).unwrap();
  while matches!(
    m.handle_storage(&7, d, &mut log, &mut stable),
    Some(StorageProgress::MorePending)
  ) {}

  // The G-free event surfaced through the container drain, stamped with the PARENT group.
  let mut saw = false;
  while let Some((g, ev)) = m.poll_event() {
    if let Event::SplitApplied(sa) = ev {
      assert_eq!(g, 7);
      assert_eq!(sa.index(), idx);
      saw = true;
    }
  }
  assert!(saw, "Event::SplitApplied surfaced through poll_event");

  // The relay: a typed GroupFork with the parent's voters remapped and the apply-derived blob.
  let fork = m.poll_pending_fork().expect("one fork relayed");
  assert_eq!(fork.parent, 7);
  assert_eq!(fork.child, 200);
  assert_eq!(fork.child_gen, 0);
  assert_eq!(
    fork.parent_gen_after, 1,
    "first split bumps the lineage to 1"
  );
  assert_eq!(fork.split_index, idx);
  assert_eq!(fork.fsm.units, 2, "the forked half");
  assert_eq!(
    fork.blob,
    fork_blob(2),
    "the apply-derived blob matches the half"
  );
  assert_eq!(fork.config.id(), 1);
  assert_eq!(fork.config.voters(), &[1u64]);
  assert_eq!(
    fork.config.snapshot_threshold(),
    1,
    "the child inherits the parent's local knobs"
  );
  assert!(fork.config.pre_vote());
  assert_eq!(
    fork.read_only, None,
    "a never-migrated parent hands the child no explicit mode"
  );
  assert!(m.poll_pending_fork().is_none(), "exactly one fork");
  assert_eq!(m.group_gen(&7), 1, "the container's lineage view bumped");

  // THE FORK DURABILITY BARRIER, container-to-endpoint: the threshold (1) is long crossed and a
  // post-split entry commits, yet no capture lands AT-OR-PAST the split until the driver reports
  // the fork behind its engine barrier (the pre-split capture, below the split index, stands).
  commit_one_split(&mut m, 7, d, &mut log, &mut stable);
  let pre = stable.snapshot().map(|(m2, _)| m2.last_index());
  assert!(
    pre.is_none_or(|boundary| boundary < idx),
    "no capture at-or-past the unresolved split (boundary {pre:?}, split {idx:?})"
  );
  m.lift_fork_barrier(&7, idx);
  commit_one_split(&mut m, 7, d, &mut log, &mut stable);
  let (meta, _blob) = stable
    .snapshot()
    .expect("the lifted parent snapshots again");
  assert!(
    meta.last_index() > idx,
    "the lifted parent's capture crosses the split boundary"
  );
  assert_eq!(
    meta.shape_gen(),
    1,
    "the parent's meta carries the bumped lineage"
  );
}

#[test]
fn same_mint_split_noops_at_apply_and_conserves_state() {
  // A follower container receives TWO NONZERO committed splits carrying the SAME
  // parent_gen_after (a stale mint forced past the propose gate — a deposed leader's retry, or
  // a crafted entry). Pre-guard this was the DATA-LOSS shape: fsm.split ran for BOTH (the
  // parent gave up two halves) while the relay dropped the second fork as a same-gen duplicate
  // — the second child's partition was given up and never materialized. The apply-time lineage
  // guard now no-ops the stale entry BEFORE fsm.split: zero parent mutation, nothing staged, no
  // snapshot fence, and the `SplitStale` event surfaces for the embedder to re-propose.
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_snapshot_threshold(1);
  m.create_group(7, cfg, Instant::ORIGIN, 42, SplitSm::default())
    .unwrap();

  // Three units of load, then the two same-mint splits, each giving 2 units away.
  let cmd = {
    let mut buf = Vec::new();
    Bytes::from_static(b"c").encode(&mut buf);
    Bytes::from(buf)
  };
  let entries = std::vec![
    crate::Entry::new(
      Term::new(1),
      Index::new(1),
      crate::EntryKind::Normal,
      cmd.clone()
    ),
    crate::Entry::new(
      Term::new(1),
      Index::new(2),
      crate::EntryKind::Normal,
      cmd.clone()
    ),
    crate::Entry::new(Term::new(1), Index::new(3), crate::EntryKind::Normal, cmd),
    crate::Entry::new(
      Term::new(1),
      Index::new(4),
      crate::EntryKind::Split,
      split_entry_bytes(200, 0, 1, 2),
    ),
    crate::Entry::new(
      Term::new(1),
      Index::new(5),
      crate::EntryKind::Split,
      split_entry_bytes(201, 0, 1, 2),
    ),
  ];
  m.handle_message(
    &7,
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendEntries(crate::AppendEntries::new(
      Term::new(1),
      2u64,
      Index::ZERO,
      Term::ZERO,
      entries,
      Index::new(5),
    )),
  )
  .unwrap();
  while matches!(
    m.handle_storage(&7, Instant::ORIGIN, &mut log, &mut stable),
    Some(StorageProgress::MorePending)
  ) {}

  // ZERO parent mutation on the stale entry: the parent gave up exactly ONE half.
  assert!(
    !m.group(&7).unwrap().is_poisoned(),
    "a stale mint never poisons"
  );
  assert_eq!(
    m.group(&7).unwrap().state_machine().units,
    1,
    "3 - 2: the parent shrank ONCE (the stale mint must not shrink it again)"
  );
  let fork = m.poll_pending_fork().expect("the first fork relays");
  assert_eq!((fork.child, fork.parent_gen_after), (200, 1));
  assert_eq!(fork.fsm.units, 2, "child 1 holds the single given-up half");
  assert!(
    m.poll_pending_fork().is_none(),
    "the stale mint staged NO fork"
  );
  // Conservation: every unit is in exactly one of parent / child 1.
  assert_eq!(
    m.group(&7).unwrap().state_machine().units + fork.fsm.units,
    3
  );

  // The stale entry surfaced as the deterministic no-op event (the embedder's re-propose cue).
  let mut stale = None;
  while let Some((g, ev)) = m.poll_event() {
    if let Event::SplitStale(s) = ev {
      assert_eq!(g, 7);
      stale = Some(s);
    }
  }
  let stale = stale.expect("Event::SplitStale surfaced through poll_event");
  assert_eq!(stale.index(), Index::new(5));
  assert_eq!(
    u64::decode_exact(stale.child()).expect("child id decodes"),
    201
  );
  assert_eq!((stale.minted_gen(), stale.shape_gen()), (1, 1));

  // The real fork still fences snapshots; the stale entry contributed NO fence: lifting the
  // real split index alone frees the cadence, and the capture crosses the stale index.
  while matches!(
    m.handle_storage(&7, Instant::ORIGIN, &mut log, &mut stable),
    Some(StorageProgress::MorePending)
  ) {}
  assert!(
    stable.snapshot().is_none(),
    "the real fork's barrier holds until the driver reports it durable"
  );
  m.lift_fork_barrier(&7, Index::new(4));
  m.handle_message(
    &7,
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    2u64,
    Message::Heartbeat(crate::Heartbeat::new(
      Term::new(1),
      2u64,
      Index::new(5),
      Bytes::new(),
    )),
  )
  .unwrap();
  while matches!(
    m.handle_storage(&7, Instant::ORIGIN, &mut log, &mut stable),
    Some(StorageProgress::MorePending)
  ) {}
  let (meta, _blob) = stable
    .snapshot()
    .expect("no orphaned fence from the no-op'd stale entry");
  assert_eq!(
    meta.last_index(),
    Index::new(5),
    "the capture crosses the stale index"
  );
}

#[test]
fn back_to_back_split_proposals_are_gated_until_apply() {
  // The propose-time UX leg of the same defect: a leader appending two splits before the first
  // APPLIES would mint the same parent_gen_after twice (the mint reads the live counter, whose
  // sole bump site is apply). Pre-gate, both entries committed, the parent shrank twice, and
  // the relay dropped the second fork as a same-gen duplicate — the second child's half was
  // LOST. The gate refuses the second proposal while the first is unapplied and self-heals by
  // derivation (index-vs-applied), so the retry after apply chains onto the bumped counter.
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  let cfg = single_node_cfg(1).with_snapshot_threshold(1);
  m.create_group(7, cfg, Instant::ORIGIN, 42, SplitSm::default())
    .unwrap();
  let d = lead_single_split(&mut m, 7, &mut log, &mut stable);
  for _ in 0..5 {
    commit_one_split(&mut m, 7, d, &mut log, &mut stable);
  }
  assert_eq!(m.group(&7).unwrap().state_machine().units, 5);

  // First split appended (durable-pending, NOT yet applied) …
  let idx1 = m
    .propose_split(
      &7,
      d,
      &mut log,
      &stable,
      &200,
      0,
      Bytes::from_static(b"\x02"),
    )
    .unwrap()
    .unwrap();
  // … so a second split must refuse NOW: its mint would duplicate the first's.
  assert_eq!(
    m.propose_split(
      &7,
      d,
      &mut log,
      &stable,
      &201,
      0,
      Bytes::from_static(b"\x02")
    ),
    Some(Err(SplitError::SplitInFlight)),
    "a second split is refused while the first is unapplied"
  );

  // Apply the first: the gate opens by derivation (applied caught up) and the counter bumped.
  m.flush_appends(&7, d, &log, &stable).unwrap();
  while matches!(
    m.handle_storage(&7, d, &mut log, &mut stable),
    Some(StorageProgress::MorePending)
  ) {}
  let fork1 = m.poll_pending_fork().expect("the first fork relays");
  assert_eq!((fork1.child, fork1.parent_gen_after), (200, 1));
  assert_eq!(fork1.fsm.units, 2);
  assert_eq!(fork1.split_index, idx1);

  // The retry now chains onto the bumped lineage instead of duplicating the first mint.
  let idx2 = m
    .propose_split(
      &7,
      d,
      &mut log,
      &stable,
      &201,
      0,
      Bytes::from_static(b"\x02"),
    )
    .unwrap()
    .expect("the gate self-heals once the first split applied");
  m.flush_appends(&7, d, &log, &stable).unwrap();
  while matches!(
    m.handle_storage(&7, d, &mut log, &mut stable),
    Some(StorageProgress::MorePending)
  ) {}
  let fork2 = m.poll_pending_fork().expect("the second fork relays");
  assert_eq!((fork2.child, fork2.parent_gen_after), (201, 2));
  assert_eq!(fork2.fsm.units, 2);
  assert_eq!(fork2.split_index, idx2);

  // Conservation across the chained pair: every unit lives in exactly one of the three.
  assert_eq!(m.group(&7).unwrap().state_machine().units, 1);
  assert_eq!(
    m.group(&7).unwrap().state_machine().units + fork1.fsm.units + fork2.fsm.units,
    5
  );
  assert_eq!(m.group_gen(&7), 2, "the lineage chained 0 → 1 → 2");

  // Exact-index barrier semantics on the CHAINED pair: resolving the NEWER fork must not free
  // the older, still-unflushed one — the snapshot fence is the minimum outstanding index.
  commit_one_split(&mut m, 7, d, &mut log, &mut stable);
  m.lift_fork_barrier(&7, idx2);
  commit_one_split(&mut m, 7, d, &mut log, &mut stable);
  assert!(
    stable.snapshot().map(|(m2, _)| m2.last_index()) < Some(idx1),
    "resolving the newer fork leaves the older fork's fence standing"
  );
  m.lift_fork_barrier(&7, idx1);
  commit_one_split(&mut m, 7, d, &mut log, &mut stable);
  let (meta, _blob) = stable
    .snapshot()
    .expect("both forks resolved: cadence resumes");
  assert!(meta.last_index() > idx2, "the capture crosses both splits");
  assert_eq!(meta.shape_gen(), 2);
}

/// Feed follower group 7 (peer 2 leading at term 1) three units of load and a NONZERO split
/// giving 2 of them to `child`, then drain storage to the applied state. `commit` covers the
/// whole batch. Returns the split entry's index.
fn follower_load_and_split(
  m: &mut MultiRaft<u64, u64, SplitSm>,
  log: &mut VecLog,
  stable: &mut AsyncStable,
  child: u64,
) -> Index {
  let cmd = {
    let mut buf = Vec::new();
    Bytes::from_static(b"c").encode(&mut buf);
    Bytes::from(buf)
  };
  let entries = std::vec![
    crate::Entry::new(
      Term::new(1),
      Index::new(1),
      crate::EntryKind::Normal,
      cmd.clone()
    ),
    crate::Entry::new(
      Term::new(1),
      Index::new(2),
      crate::EntryKind::Normal,
      cmd.clone()
    ),
    crate::Entry::new(Term::new(1), Index::new(3), crate::EntryKind::Normal, cmd),
    crate::Entry::new(
      Term::new(1),
      Index::new(4),
      crate::EntryKind::Split,
      split_entry_bytes(child, 0, 1, 2),
    ),
  ];
  m.handle_message(
    &7,
    Instant::ORIGIN,
    log,
    stable,
    2u64,
    Message::AppendEntries(crate::AppendEntries::new(
      Term::new(1),
      2u64,
      Index::ZERO,
      Term::ZERO,
      entries,
      Index::new(4),
    )),
  )
  .unwrap();
  while matches!(
    m.handle_storage(&7, Instant::ORIGIN, log, stable),
    Some(StorageProgress::MorePending)
  ) {}
  Index::new(4)
}

/// Deliver one committed Normal entry to follower group 7 at `index` (prev = `index - 1`,
/// term 1 throughout) and drain storage — the fence probes' commit source.
fn follower_commit_next(
  m: &mut MultiRaft<u64, u64, SplitSm>,
  log: &mut VecLog,
  stable: &mut AsyncStable,
  index: u64,
) {
  let cmd = {
    let mut buf = Vec::new();
    Bytes::from_static(b"x").encode(&mut buf);
    Bytes::from(buf)
  };
  m.handle_message(
    &7,
    Instant::ORIGIN,
    log,
    stable,
    2u64,
    Message::AppendEntries(crate::AppendEntries::new(
      Term::new(1),
      2u64,
      Index::new(index - 1),
      Term::new(1),
      std::vec![crate::Entry::new(
        Term::new(1),
        Index::new(index),
        crate::EntryKind::Normal,
        cmd,
      )],
      Index::new(index),
    )),
  )
  .unwrap();
  while matches!(
    m.handle_storage(&7, Instant::ORIGIN, log, stable),
    Some(StorageProgress::MorePending)
  ) {}
}

#[test]
fn hosted_child_fork_parks_and_materializes_after_removal() {
  // The committed split names a child THIS host already hosts, and the split is NONZERO — the
  // parent gave up real state at apply, so the staged blob is the partition's only local copy.
  // The relay must PARK the fork (blob held, guard unmoved, fence standing, one conflict
  // signal), never resolve it as a no-op: the pre-park drop arm silently lost the partition.
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_snapshot_threshold(1);
  m.create_group(7, cfg, Instant::ORIGIN, 42, SplitSm::default())
    .unwrap();
  // The squatter: hosted under the child id, zero progress (its timers never fire here).
  m.create_group(
    200,
    single_node_cfg(1),
    Instant::ORIGIN,
    43,
    SplitSm::default(),
  )
  .unwrap();

  let idx = follower_load_and_split(&mut m, &mut log, &mut stable, 200);

  // PARKED: nothing yields, nothing resolves — and the conflict surfaces exactly once.
  assert!(
    m.poll_pending_fork().is_none(),
    "a hosted child id parks the fork instead of yielding it"
  );
  assert_eq!(
    m.poll_split_conflict(),
    Some((7, 200)),
    "the park surfaces one (parent, child) conflict signal"
  );
  assert_eq!(
    m.poll_split_conflict(),
    None,
    "the signal is deduped until the park resolves"
  );
  assert!(m.poll_pending_fork().is_none(), "still parked");
  assert_eq!(
    m.poll_split_conflict(),
    None,
    "re-examination does not re-emit the conflict"
  );

  // Conservation WHILE PARKED: the parent shrank exactly once and the given-up half survives
  // in the staged fork's blob — nothing is lost, it is merely not yet placeable.
  assert_eq!(
    m.group(&7).unwrap().state_machine().units,
    1,
    "3 - 2: fsm.split ran at apply on every replica identically"
  );
  assert_eq!(
    m.group(&7)
      .unwrap()
      .peek_pending_fork()
      .expect("the parked fork stays staged")
      .blob,
    fork_blob(2),
    "the partition's blob is retained while parked"
  );
  assert!(
    m.split_reserved(&200),
    "a parked fork keeps its child id reserved at the coordinators' admission gates"
  );

  // The fence does NOT lift while parked: the threshold (1) is long crossed and a post-split
  // entry commits, yet no capture lands at-or-past the split — the entry stays replayable, so
  // recovery survives arbitrarily late embedder action.
  follower_commit_next(&mut m, &mut log, &mut stable, 5);
  let pre = stable.snapshot().map(|(meta, _)| meta.last_index());
  assert!(
    pre.is_none_or(|boundary| boundary < idx),
    "the parked fork's fence holds (boundary {pre:?}, split {idx:?})"
  );

  // Arm (a): the squatter is removed — the fork now materializes NORMALLY, with the full half.
  m.remove_group(&200);
  let fork = m
    .poll_pending_fork()
    .expect("removal unparks the fork for materialization");
  assert_eq!((fork.parent, fork.child), (7, 200));
  assert_eq!(fork.split_index, idx);
  assert_eq!(fork.parent_gen_after, 1);
  assert_eq!(fork.fsm.units, 2, "the parked half materializes intact");
  assert_eq!(fork.blob, fork_blob(2));
  assert!(
    !m.split_reserved(&200),
    "the yield releases the reservation — this fork is now the id's one admitted writer"
  );
  // Conservation across the resolution: every unit is in exactly one of parent / child — the
  // pre-split 3 plus the one the fence probe committed while parked.
  assert_eq!(
    m.group(&7).unwrap().state_machine().units + fork.fsm.units,
    4
  );

  // The driver reports the child durable; the fence releases and the cadence resumes.
  m.lift_fork_barrier(&7, idx);
  follower_commit_next(&mut m, &mut log, &mut stable, 6);
  let (meta, _blob) = stable.snapshot().expect("the resolved parent captures");
  assert!(
    meta.last_index() >= idx,
    "the capture crosses the resolved split"
  );
  assert_eq!(meta.shape_gen(), 1, "and carries the bumped lineage");
}

#[test]
fn parked_fork_resolves_redundant_when_the_twin_catches_up() {
  // Arm (b), post-park: the hosted child reaches applied >= FORK_BASE_INDEX under the fork's
  // own lineage — under single-incarnation ids that IS this fork (a twin materialized from a
  // sibling whose blob was flush-durable before it could transmit) — so the parked fork
  // resolves as redundant: fence lifts, guard advances, the local blob is discarded safely.
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  let (mut log200, mut stable200) = (VecLog::default(), AsyncStable::default());
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_snapshot_threshold(1);
  m.create_group(7, cfg, Instant::ORIGIN, 42, SplitSm::default())
    .unwrap();
  m.create_group(
    200,
    single_node_cfg(1),
    Instant::ORIGIN,
    43,
    SplitSm::default(),
  )
  .unwrap();
  let idx = follower_load_and_split(&mut m, &mut log, &mut stable, 200);
  assert!(m.poll_pending_fork().is_none(), "parked on the conflict");
  assert_eq!(m.poll_split_conflict(), Some((7, 200)));

  // The hosted child advances to applied >= the fork baseline at lineage 0 == child_gen.
  let d = lead_single_split(&mut m, 200, &mut log200, &mut stable200);
  commit_one_split(&mut m, 200, d, &mut log200, &mut stable200);
  assert!(m.group(&200).unwrap().applied_index() >= FORK_BASE_INDEX);

  // The next drain resolves the park as REDUNDANT: nothing yields, the staged fork is gone,
  // and the fence releases without any lift call.
  assert!(
    m.poll_pending_fork().is_none(),
    "a caught-up twin resolves the park without yielding"
  );
  assert!(
    m.group(&7).unwrap().peek_pending_fork().is_none(),
    "the redundant fork is consumed"
  );
  follower_commit_next(&mut m, &mut log, &mut stable, 5);
  let (meta, _blob) = stable
    .snapshot()
    .expect("the redundant resolution released the fence");
  assert!(meta.last_index() >= idx, "the capture crosses the split");
  assert_eq!(meta.shape_gen(), 1);
}

/// Build the standard park shape shared by the conflict-signal tests: two-voter group 7 as a
/// follower, a zero-progress squatter hosted under child id 200, and a committed split into
/// 200 — parked, with the one conflict signal queued and deliberately NOT consumed.
fn park_with_queued_conflict(
  log: &mut VecLog,
  stable: &mut AsyncStable,
) -> MultiRaft<u64, u64, SplitSm> {
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  m.create_group(7, cfg, Instant::ORIGIN, 42, SplitSm::default())
    .unwrap();
  m.create_group(
    200,
    single_node_cfg(1),
    Instant::ORIGIN,
    43,
    SplitSm::default(),
  )
  .unwrap();
  follower_load_and_split(&mut m, log, stable, 200);
  assert!(m.poll_pending_fork().is_none(), "parked on the conflict");
  m
}

#[test]
fn peek_split_conflict_leaves_the_signal_queued() {
  // The DELIVERED-BEFORE-CONSUMED half: a driver publishing on a bounded lifecycle tail peeks,
  // publishes, and consumes only on acceptance — so a refused send must find the one-shot cue
  // still queued on the next drain, and a successful one must consume it exactly once.
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  let mut m = park_with_queued_conflict(&mut log, &mut stable);

  assert_eq!(m.peek_split_conflict(), Some((7, 200)));
  assert_eq!(
    m.peek_split_conflict(),
    Some((7, 200)),
    "peek repeats until a successful publish consumes — a full tail loses nothing"
  );
  assert_eq!(m.poll_split_conflict(), Some((7, 200)));
  assert_eq!(
    m.peek_split_conflict(),
    None,
    "consumption empties the queue"
  );
  assert_eq!(m.poll_split_conflict(), None, "one signal per park episode");
  assert!(m.poll_pending_fork().is_none(), "still parked");
  assert_eq!(
    m.peek_split_conflict(),
    None,
    "re-examination does not re-arm a consumed episode"
  );
}

#[test]
fn squatter_removal_purges_an_undelivered_conflict() {
  // Arm (a) with the signal still QUEUED (a full driver tail had deferred it): the squatter's
  // removal materializes the fork and the episode is over, so the queued signal dies with it —
  // delivered later it would be stale, capable of goading the embedder into removing the very
  // child the resolution just materialized.
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  let mut m = park_with_queued_conflict(&mut log, &mut stable);
  assert_eq!(
    m.peek_split_conflict(),
    Some((7, 200)),
    "queued, undelivered"
  );

  m.remove_group(&200);
  let fork = m
    .poll_pending_fork()
    .expect("removal unparks the fork for materialization");
  assert_eq!((fork.parent, fork.child), (7, 200));
  assert_eq!(
    m.peek_split_conflict(),
    None,
    "the resolved episode purged its undelivered signal"
  );
  assert_eq!(m.poll_split_conflict(), None);
}

#[test]
fn twin_catch_up_purges_an_undelivered_conflict() {
  // Arm (b) with the signal still queued: the hosted twin catches up, the parked fork resolves
  // as redundant, and the stale cue must not surface after the episode silently healed.
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  let (mut log200, mut stable200) = (VecLog::default(), AsyncStable::default());
  let mut m = park_with_queued_conflict(&mut log, &mut stable);
  assert_eq!(
    m.peek_split_conflict(),
    Some((7, 200)),
    "queued, undelivered"
  );

  let d = lead_single_split(&mut m, 200, &mut log200, &mut stable200);
  commit_one_split(&mut m, 200, d, &mut log200, &mut stable200);
  assert!(m.group(&200).unwrap().applied_index() >= FORK_BASE_INDEX);
  assert!(
    m.poll_pending_fork().is_none(),
    "a caught-up twin resolves the park without yielding"
  );
  assert!(
    m.group(&7).unwrap().peek_pending_fork().is_none(),
    "the redundant fork is consumed"
  );
  assert_eq!(
    m.peek_split_conflict(),
    None,
    "the silently-healed episode purged its undelivered signal"
  );
  assert_eq!(m.poll_split_conflict(), None);
}

#[test]
fn removing_the_parked_parent_purges_its_undelivered_conflict() {
  // The parent's removal is the embedder's explicit destruction of this replica: the staged
  // forks die with the endpoint, so the park bookkeeping — a still-queued conflict signal a
  // full driver tail had deferred included — dies too, never surfacing for a group that no
  // longer exists here.
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  let mut m = park_with_queued_conflict(&mut log, &mut stable);
  assert_eq!(
    m.peek_split_conflict(),
    Some((7, 200)),
    "queued, undelivered"
  );

  assert!(m.remove_group(&7).is_some());
  assert_eq!(
    m.peek_split_conflict(),
    None,
    "the park bookkeeping died with the endpoint"
  );
  assert_eq!(m.poll_split_conflict(), None);
}

#[test]
fn pre_hosted_twin_resolves_redundant_without_parking() {
  // Arm (b) at FIRST examination: the twin was already hosted at-or-past the baseline under
  // the fork's lineage when the split relayed (the factory materialized it from a sibling
  // before this replica's own fork drained), so the relay resolves it as redundant outright —
  // no park episode, no conflict signal — and the twin's state carries the partition.
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  let (mut log200, mut stable200) = (VecLog::default(), AsyncStable::default());
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_snapshot_threshold(1);
  m.create_group(7, cfg, Instant::ORIGIN, 42, SplitSm::default())
    .unwrap();
  // The twin: fork-born at the manufactured baseline (applied == FORK_BASE_INDEX, lineage 0)
  // holding exactly the half the split gives away.
  m.create_group_from_fork(
    200,
    0,
    single_node_cfg(1),
    Instant::ORIGIN,
    43,
    SplitSm::default(),
    fork_blob(2),
    None,
    1,
    &mut log200,
    &mut stable200,
  )
  .unwrap();

  let idx = follower_load_and_split(&mut m, &mut log, &mut stable, 200);

  assert!(
    m.poll_pending_fork().is_none(),
    "the redundant fork never yields"
  );
  assert_eq!(
    m.poll_split_conflict(),
    None,
    "an already-caught-up twin is no conflict — nothing parked"
  );
  assert_eq!(m.group_gen(&7), 1, "the relay guard advanced");
  // Conservation THROUGH the twin: parent half + twin's preloaded half == the original units.
  assert_eq!(m.group(&7).unwrap().state_machine().units, 1);
  assert_eq!(
    m.group(&200).unwrap().state_machine().units,
    2,
    "the twin carries the partition, so discarding the local blob loses nothing"
  );
  // The fence resolved with the redundant fold: the cadence resumes unaided.
  follower_commit_next(&mut m, &mut log, &mut stable, 5);
  let (meta, _blob) = stable
    .snapshot()
    .expect("no orphaned fence from the redundant fold");
  assert!(meta.last_index() >= idx);
  assert_eq!(meta.shape_gen(), 1);
}

#[test]
fn split_admission_race_parks_instead_of_dropping() {
  // THE propose-window race, leader-shaped: propose_split's ChildExists gate passes (200 is
  // not hosted), the child id is then admitted BEFORE the entry applies (at the pure container
  // there is no reservation — this is exactly the coordinator-level pre-reservation window),
  // and the split commits against a now-hosted child. Pre-park this was the data-loss shape:
  // the parent shrank at apply and the relay dropped the fork — the partition vanished. Now it
  // parks: conserved while parked, surfaced, and materialized once the squatter leaves.
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  let cfg = single_node_cfg(1).with_snapshot_threshold(1);
  m.create_group(7, cfg, Instant::ORIGIN, 42, SplitSm::default())
    .unwrap();
  let d = lead_single_split(&mut m, 7, &mut log, &mut stable);
  for _ in 0..3 {
    commit_one_split(&mut m, 7, d, &mut log, &mut stable);
  }
  assert_eq!(m.group(&7).unwrap().state_machine().units, 3);

  // Propose (gate passes: 200 unhosted), THEN admit 200 in the propose→apply window.
  let idx = m
    .propose_split(
      &7,
      d,
      &mut log,
      &stable,
      &200,
      0,
      Bytes::from_static(b"\x02"),
    )
    .unwrap()
    .unwrap();
  m.create_group(200, single_node_cfg(1), d, 43, SplitSm::default())
    .unwrap();

  // Apply the split: the parent shrinks deterministically (apply is replica-identical and
  // cannot consult hosted-ness), and the relay PARKS the fork against the squatter.
  m.flush_appends(&7, d, &log, &stable).unwrap();
  while matches!(
    m.handle_storage(&7, d, &mut log, &mut stable),
    Some(StorageProgress::MorePending)
  ) {}
  assert!(m.poll_pending_fork().is_none(), "parked, not dropped");
  assert_eq!(m.poll_split_conflict(), Some((7, 200)));
  assert_eq!(
    m.group(&7).unwrap().state_machine().units,
    1,
    "the parent gave the half up at apply"
  );
  assert_eq!(
    m.group(&7)
      .unwrap()
      .peek_pending_fork()
      .expect("the half is staged, not lost")
      .blob,
    fork_blob(2)
  );

  // No silent drop: removing the squatter materializes the full half — conservation exact.
  m.remove_group(&200);
  let fork = m.poll_pending_fork().expect("the fork survives the race");
  assert_eq!((fork.parent, fork.child, fork.split_index), (7, 200, idx));
  assert_eq!(fork.fsm.units, 2);
  assert_eq!(
    m.group(&7).unwrap().state_machine().units + fork.fsm.units,
    3
  );
}

#[test]
fn restore_seeds_the_replay_guard_from_durable_lineage() {
  // Crash shape A (row 1): the durable snapshot PRECEDES the split (shape_gen 0); the tail
  // replays it. The re-staged fork must relay again — the child may never have flushed.
  let cfg = || {
    Config::try_new(
      1u64,
      std::vec![1u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap()
  };
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();
  log.force_append(&[crate::Entry::new(
    Term::new(1),
    Index::new(1),
    crate::EntryKind::Split,
    split_entry_bytes(200, 0, 1, 0),
  )]);
  stable.force_state(Term::new(1), Some(1u64), Index::new(1));
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  m.restore_group(
    7,
    cfg(),
    Instant::ORIGIN,
    42,
    SplitSm::default(),
    1,
    &mut log,
    &mut stable,
  )
  .unwrap();
  let fork = m
    .poll_pending_fork()
    .expect("a replayed, never-relayed fork relays again");
  assert_eq!((fork.child, fork.parent_gen_after), (200, 1));

  // Crash shape B (row 4): the durable snapshot ALREADY CARRIES the split's bump (shape_gen 1
  // at a boundary past it), and the tail replays a stale same-gen retry duplicate. The
  // recovered lineage seeds the live counter at 1, so replay's APPLY-TIME lineage guard no-ops
  // the entry outright — nothing is staged (no barrier to resolve), and the restored parent's
  // state is untouched by the duplicate.
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();
  let meta = crate::SnapshotMeta::new(
    Index::new(2),
    Term::new(1),
    crate::ConfState::from_voters(std::vec![1u64]),
  )
  .with_shape_gen(1);
  stable.force_snapshot(meta, fork_blob(1));
  stable.force_state(Term::new(1), Some(1u64), Index::new(3));
  log.restore(Index::new(2), Term::new(1));
  log.force_append(&[crate::Entry::new(
    Term::new(1),
    Index::new(3),
    crate::EntryKind::Split,
    split_entry_bytes(201, 0, 1, 1),
  )]);
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  m.restore_group(
    7,
    cfg().with_snapshot_threshold(1),
    Instant::ORIGIN,
    42,
    SplitSm::default(),
    1,
    &mut log,
    &mut stable,
  )
  .unwrap();
  assert_eq!(m.group_gen(&7), 1);
  assert!(
    m.poll_pending_fork().is_none(),
    "a replayed duplicate below the durable lineage is dropped"
  );
  assert_eq!(
    m.group(&7).unwrap().state_machine().units,
    1,
    "the no-op'd duplicate gave nothing away from the restored parent"
  );
}

#[test]
fn fork_baseline_meta_carries_lineage_and_read_mode() {
  // The manufactured install stamps the child's incarnation and its inherited read mode into
  // the baseline meta, so the child boots knowing both (exactly as a restart recovers them).
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  m.create_group_from_fork(
    7,
    3,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    preloaded_sm(5),
    fork_blob(5),
    Some(crate::ReadOnlyOption::Safe),
    1,
    &mut log,
    &mut stable,
  )
  .unwrap();

  let (meta, _) = stable.snapshot().expect("baseline persisted");
  assert_eq!(
    meta.shape_gen(),
    3,
    "the child's incarnation rides its baseline meta"
  );
  assert_eq!(
    meta.read_only(),
    Some(crate::ReadOnlyOption::Safe),
    "the inherited mode rides the baseline meta (explicit-Safe stays distinguishable)"
  );
  assert_eq!(m.group_gen(&7), 3);
  assert!(!m.group(&7).unwrap().is_poisoned());
}

// ───────────────────────────── merge verbs + the per-crank service ─────────────────────────────

/// Per-group `(VecLog, AsyncStable)` pairs behind the service's store seam, with the host's
/// terminal merge floors beside them (the absent arm's discriminator).
struct MapStores(
  std::collections::BTreeMap<u64, (VecLog, AsyncStable)>,
  std::collections::BTreeSet<u64>,
);

impl crate::GroupStores<u64, VecLog, AsyncStable> for MapStores {
  fn stores(&mut self, group: &u64) -> Option<(&mut VecLog, &mut AsyncStable)> {
    self.0.get_mut(group).map(|(l, s)| (l, s))
  }
}

impl crate::FloorStore<u64> for MapStores {
  fn floor(&self, gid: &u64) -> u64 {
    if self.1.contains(gid) {
      MERGED_FLOOR
    } else {
      0
    }
  }

  fn lineage(&self, _gid: &u64) -> u64 {
    0
  }
}

/// A host with two single-voter groups (1 = source with `src_count` applied commands, 2 = target
/// with `tgt_count`), each elected and fully drained.
fn merge_host(src_count: usize, tgt_count: usize) -> (MultiRaft<u64, u64, CountSm>, MapStores) {
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let mut stores = MapStores(
    std::collections::BTreeMap::new(),
    std::collections::BTreeSet::new(),
  );
  for (gid, n) in [(1u64, src_count), (2u64, tgt_count)] {
    stores
      .0
      .insert(gid, (VecLog::default(), AsyncStable::default()));
    m.create_group(
      gid,
      single_node_cfg(1),
      Instant::ORIGIN,
      7,
      CountSm::default(),
    )
    .unwrap();
    let (log, stable) = stores.0.get_mut(&gid).unwrap();
    let d = m.group(&gid).unwrap().poll_timeout().unwrap();
    m.handle_timeout(&gid, d, log, stable).unwrap();
    drain_storage(&mut m, gid, d, log, stable);
    assert!(m.group(&gid).unwrap().role().is_leader());
    for i in 0..n {
      let cmd = Bytes::copy_from_slice(&[i as u8]);
      m.propose(&gid, d, log, stable, &cmd).unwrap().unwrap();
      drain_storage(&mut m, gid, d, log, stable);
    }
  }
  while m.poll_message().is_some() {}
  while m.poll_event().is_some() {}
  (m, stores)
}

/// Freeze group 1 into group 2 and park group 2's commit, fully drained: the state every
/// resolution arm starts from. Returns the parked index k.
fn freeze_and_park(m: &mut MultiRaft<u64, u64, CountSm>, stores: &mut MapStores) -> Index {
  let now = Instant::ORIGIN;
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.prepare_merge(&1, now, log, stable, &2).unwrap().unwrap();
    drain_storage(m, 1, now, log, stable);
  }
  assert!(m.group(&1).unwrap().is_frozen());
  let k = {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    let k = m.commit_merge(&2, now, log, stable, &1).unwrap().unwrap();
    drain_storage(m, 2, now, log, stable);
    k
  };
  assert!(m.group(&2).unwrap().pending_merge().is_some(), "parked");
  k
}

/// Close the parked commit's abort window: the first service pass resolves nothing — it
/// appends the leader's seal no-op at the coordinate after the parked entry — and the drain
/// commits it (single-voter shape: commit advances at the local storage drain).
fn seal_window(m: &mut MultiRaft<u64, u64, CountSm>, stores: &mut MapStores) {
  assert!(
    m.service_merge_applies(Instant::ORIGIN, stores).is_empty(),
    "the first pass only seals the window"
  );
  let (log, stable) = stores.0.get_mut(&2).unwrap();
  drain_storage(m, 2, Instant::ORIGIN, log, stable);
}

/// Arm 1 end-to-end inside one container: freeze → park → resolve. The source endpoint is
/// extracted and absorbed, the target serves the union, the forced absorb capture is staged
/// through the store seam, and the resolution surfaces for the driver's floor/teardown fold.
#[test]
fn service_resolves_a_ready_merge() {
  let (mut m, mut stores) = merge_host(2, 3);
  let k = freeze_and_park(&mut m, &mut stores);
  seal_window(&mut m, &mut stores);
  let resolutions = m.service_merge_applies(Instant::ORIGIN, &mut stores);
  assert_eq!(
    resolutions,
    std::vec![MergeResolution::Merged {
      source: 1,
      target: 2
    }]
  );
  assert!(!m.contains_group(&1), "the source endpoint is gone");
  let tep = m.group(&2).unwrap();
  assert_eq!(tep.applied_index(), k, "the parked entry applied");
  assert!(tep.pending_merge().is_none());
  assert_eq!(
    tep.state_machine().count(),
    2 + 3,
    "the target serves the union"
  );
  assert_eq!(tep.shape_gen(), 1, "target lineage bumped");
  let mut merged = false;
  while let Some((gid, ev)) = m.poll_event() {
    if let Event::Merged(e) = ev {
      assert_eq!(gid, 2);
      assert_eq!(e.index(), k);
      merged = true;
    }
  }
  assert!(merged, "Event::Merged surfaced group-stamped");
  // The forced absorb capture is staged: draining the target's storage lands the blob and the
  // deferred compaction — no replica can ever be log-walked across the absorb point again.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, Instant::ORIGIN, log, stable);
    assert!(stable.snapshot().is_some(), "absorb capture persisted");
    assert!(log.first_index() > k, "compacted through the absorb");
  }
  // The park is consumed: the next crank has nothing to service.
  assert!(
    m.service_merge_applies(Instant::ORIGIN, &mut stores)
      .is_empty()
  );
}

/// The abort races the parked commit ON THE TARGET'S OWN LOG and lands at the coordinate right
/// after it: every replica's park un-parks ABORTED off that one committed coordinate — never
/// off observation timing of the source's mutable state (the proven cross-log divergence). The
/// drain then applies the abort itself (the target's lineage bumps — the guard that kills any
/// same-base commit) and the relayed thaw unfreezes the source. Never a half-merge.
#[test]
fn rollback_races_commit() {
  let (mut m, mut stores) = merge_host(2, 3);
  let k = freeze_and_park(&mut m, &mut stores);
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    let a = m
      .rollback_merge(&2, Instant::ORIGIN, log, stable, &1)
      .unwrap()
      .unwrap();
    assert_eq!(
      a,
      k.next(),
      "the abort is the window's next resolution input"
    );
    drain_storage(&mut m, 2, Instant::ORIGIN, log, stable);
  }
  let resolutions = m.service_merge_applies(Instant::ORIGIN, &mut stores);
  assert_eq!(
    resolutions,
    std::vec![MergeResolution::Aborted {
      source: 1,
      target: 2
    }]
  );
  // The resumed drain applies the abort entry: lineage bump + the staged thaw relay.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, Instant::ORIGIN, log, stable);
  }
  assert!(m.contains_group(&1), "the source lives on");
  let tep = m.group(&2).unwrap();
  assert!(tep.pending_merge().is_none());
  assert_eq!(tep.applied_index(), k.next(), "past the park and the abort");
  assert_eq!(tep.state_machine().count(), 3, "nothing absorbed");
  assert_eq!(tep.shape_gen(), 1, "the abort bumped the target's lineage");
  let mut aborted = false;
  while let Some((gid, ev)) = m.poll_event() {
    aborted |= gid == 2 && matches!(ev, Event::MergeAborted(_));
  }
  assert!(aborted, "Event::MergeAborted surfaced");
  // Exactly one relayed thaw, and it lands on the source's own log.
  let relay = m
    .poll_pending_merge_abort()
    .expect("one relay per abort apply");
  assert_eq!((relay.target, relay.source), (2, 1));
  assert_eq!(relay.source_gen_after, 1);
  assert!(m.poll_pending_merge_abort().is_none(), "exactly once");
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.propose_merge_unfreeze(&1, Instant::ORIGIN, log, stable, &2)
      .unwrap()
      .unwrap();
    drain_storage(&mut m, 1, Instant::ORIGIN, log, stable);
  }
  let sep = m.group(&1).unwrap();
  assert!(!sep.is_frozen(), "the relayed thaw unfroze the source");
  assert_eq!(sep.shape_gen(), 2, "0 -> 1 (freeze) -> 2 (thaw)");
}

/// A LATE abort — proposed after the window sealed — no-ops at its stale mint once the merge
/// resolves: nothing un-merges, no thaw relays. The "commit first" ordering pin: whichever of
/// the two rides the target's log first wins on every replica.
#[test]
fn late_abort_no_ops_after_the_merge_resolved() {
  let (mut m, mut stores) = merge_host(2, 3);
  let _k = freeze_and_park(&mut m, &mut stores);
  seal_window(&mut m, &mut stores);
  // Proposed while the source is still frozen (the verb accepts) but ABOVE the seal: by the
  // time it applies, the resolved absorb has moved the target's counter past its mint.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.rollback_merge(&2, Instant::ORIGIN, log, stable, &1)
      .unwrap()
      .unwrap();
  }
  let resolutions = m.service_merge_applies(Instant::ORIGIN, &mut stores);
  assert_eq!(
    resolutions,
    std::vec![MergeResolution::Merged {
      source: 1,
      target: 2
    }]
  );
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, Instant::ORIGIN, log, stable);
  }
  let tep = m.group(&2).unwrap();
  assert_eq!(tep.state_machine().count(), 5, "the union stands");
  assert_eq!(tep.shape_gen(), 1, "the stale abort moved nothing");
  assert!(
    m.poll_pending_merge_abort().is_none(),
    "a stale abort relays no thaw"
  );
}

/// The absent arms, floor-discriminated: with the TERMINAL floor the commit is a replayed
/// duplicate for an already-absorbed source — a no-op past the park; WITHOUT the floor the
/// union was never materialized here, and aborting would silently skip it on this replica
/// alone — the park WAITS instead (the resolved quorum's post-merge snapshot supersedes it).
#[test]
fn absent_source_aborts_only_under_the_terminal_floor() {
  let (mut m, mut stores) = merge_host(2, 3);
  let k = freeze_and_park(&mut m, &mut stores);
  assert!(m.remove_group(&1).is_some());
  seal_window(&mut m, &mut stores);
  // No floor: never-held — the park holds for the snapshot route.
  assert!(
    m.service_merge_applies(Instant::ORIGIN, &mut stores)
      .is_empty(),
    "absent without the floor must WAIT, not skip the union"
  );
  assert!(m.group(&2).unwrap().pending_merge().is_some());
  // The terminal floor lands (this host absorbed in a prior incarnation of the park): no-op.
  stores.1.insert(1);
  let resolutions = m.service_merge_applies(Instant::ORIGIN, &mut stores);
  assert_eq!(
    resolutions,
    std::vec![MergeResolution::Aborted {
      source: 1,
      target: 2
    }]
  );
  let tep = m.group(&2).unwrap();
  assert_eq!(tep.applied_index(), k);
  assert_eq!(tep.state_machine().count(), 3, "nothing to absorb");
}

/// Arm 5: a park whose source has not reached the expected generation WAITS — and resolves on a
/// later crank once the source's freeze applies (the behind-source shape, single-host form: the
/// park was restored from a log naming a freeze the local source had not yet performed).
#[test]
fn park_waits_for_the_source_then_resolves() {
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let mut stores = MapStores(
    std::collections::BTreeMap::new(),
    std::collections::BTreeSet::new(),
  );
  // Group 1: a fresh, UNFROZEN single-voter source (gen 0).
  stores
    .0
    .insert(1, (VecLog::default(), AsyncStable::default()));
  m.create_group(
    1,
    single_node_cfg(1),
    Instant::ORIGIN,
    7,
    CountSm::default(),
  )
  .unwrap();
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    let d = m.group(&1).unwrap().poll_timeout().unwrap();
    m.handle_timeout(&1, d, log, stable).unwrap();
    drain_storage(&mut m, 1, d, log, stable);
  }
  // Group 2: restored from durable state whose committed log already carries the CommitMerge —
  // the park re-derives at restore replay, expecting the source at gen 1.
  let mut source_bytes = Vec::new();
  Data::encode(&1u64, &mut source_bytes);
  let payload = crate::CommitMergePayload::new(Bytes::from(source_bytes), Index::new(2), 1, 1);
  let mut buf = Vec::new();
  crate::wire::encode_commit_merge_payload(&payload, &mut buf);
  let mut log2 = VecLog::default();
  log2.force_append(&[crate::Entry::new(
    Term::new(1),
    Index::new(1),
    crate::EntryKind::CommitMerge,
    Bytes::from(buf),
  )]);
  let mut stable2 = crate::testkit::NoopStable::<u64>::default();
  stable2.force_state(Term::new(1), Some(1u64), Index::new(1));
  m.restore_group(
    2,
    single_node_cfg(1),
    Instant::ORIGIN,
    7,
    CountSm::default(),
    1,
    &mut log2,
    &mut stable2,
  )
  .unwrap();
  assert!(m.group(&2).unwrap().pending_merge().is_some(), "re-parked");

  // The source is behind the expectation (gen 0 < 1) AND the window is open: the park WAITS.
  assert!(
    m.service_merge_applies(Instant::ORIGIN, &mut stores)
      .is_empty()
  );
  assert!(
    m.group(&2).unwrap().pending_merge().is_some(),
    "still parked"
  );

  // The source's freeze lands (gen reaches 1, frozen at its boundary): the next crank resolves.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.prepare_merge(&1, Instant::ORIGIN, log, stable, &2)
      .unwrap()
      .unwrap();
    drain_storage(&mut m, 1, Instant::ORIGIN, log, stable);
  }
  // Group 2's own stores must be reachable through the seam for the absorb capture.
  stores.0.insert(2, (log2, AsyncStable::default()));
  // The restored target elects; its election no-op is the window's seal (any committed entry
  // at the coordinate that is not a matching abort closes the window for good).
  {
    let d = m.group(&2).unwrap().poll_timeout().unwrap();
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.handle_timeout(&2, d, log, stable).unwrap();
    drain_storage(&mut m, 2, d, log, stable);
    assert!(m.group(&2).unwrap().role().is_leader());
  }
  let resolutions = m.service_merge_applies(Instant::ORIGIN, &mut stores);
  assert_eq!(
    resolutions,
    std::vec![MergeResolution::Merged {
      source: 1,
      target: 2
    }]
  );
  assert!(!m.contains_group(&1));
  assert!(m.group(&2).unwrap().pending_merge().is_none());
}

/// Every propose-time precondition maps to its typed refusal, with nothing appended.
#[test]
fn merge_verb_preconditions_refuse_typed() {
  let (mut m, mut stores) = merge_host(1, 1);
  let now = Instant::ORIGIN;

  // A 2-voter leaderless group (3) and a LeaseBased group (4) for the comparison arms.
  stores
    .0
    .insert(3, (VecLog::default(), AsyncStable::default()));
  m.create_group(3, two_voter_cfg(), now, 7, CountSm::default())
    .unwrap();
  stores
    .0
    .insert(4, (VecLog::default(), AsyncStable::default()));
  let lease_cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_read_only(crate::ReadOnlyOption::LeaseBased)
  .with_check_quorum(true);
  m.create_group(4, lease_cfg, now, 7, CountSm::default())
    .unwrap();
  {
    let (log, stable) = stores.0.get_mut(&4).unwrap();
    let d = m.group(&4).unwrap().poll_timeout().unwrap();
    m.handle_timeout(&4, d, log, stable).unwrap();
    drain_storage(&mut m, 4, d, log, stable);
  }

  macro_rules! src {
    () => {
      stores.0.get_mut(&1).map(|(l, s)| (l, s)).unwrap()
    };
  }

  {
    let (log, stable) = src!();
    assert!(matches!(
      m.prepare_merge(&1, now, log, stable, &1).unwrap(),
      Err(MergeError::SelfMerge)
    ));
    assert!(matches!(
      m.prepare_merge(&1, now, log, stable, &9).unwrap(),
      Err(MergeError::TargetMissing)
    ));
    assert!(matches!(
      m.prepare_merge(&1, now, log, stable, &3).unwrap(),
      Err(MergeError::VoterSetsDiffer)
    ));
    assert!(matches!(
      m.prepare_merge(&1, now, log, stable, &4).unwrap(),
      Err(MergeError::ReadModesDiffer)
    ));
    assert!(m.prepare_merge(&9, now, log, stable, &2).is_none());
  }
  {
    // NotLeader: group 3 never elected — as the freeze's proposer, as the abort's target
    // leader, and as the relayed thaw's source leader.
    let (log, stable) = stores.0.get_mut(&3).map(|(l, s)| (l, s)).unwrap();
    assert!(matches!(
      m.prepare_merge(&3, now, log, stable, &1).unwrap(),
      Err(MergeError::NotLeader { .. })
    ));
    assert!(matches!(
      m.rollback_merge(&3, now, log, stable, &1).unwrap(),
      Err(MergeError::NotLeader { .. })
    ));
    assert!(matches!(
      m.propose_merge_unfreeze(&3, now, log, stable, &1).unwrap(),
      Err(MergeError::NotLeader { .. })
    ));
  }
  {
    // Commit before any freeze: the local source is not ready; rollback with nothing to undo.
    let (log, stable) = stores.0.get_mut(&2).map(|(l, s)| (l, s)).unwrap();
    assert!(matches!(
      m.commit_merge(&2, now, log, stable, &1).unwrap(),
      Err(MergeError::SourceNotReady)
    ));
    assert!(matches!(
      m.commit_merge(&2, now, log, stable, &9).unwrap(),
      Err(MergeError::SourceMissing)
    ));
    assert!(matches!(
      m.commit_merge(&2, now, log, stable, &2).unwrap(),
      Err(MergeError::SelfMerge)
    ));
  }
  {
    // A conf change in flight on the source refuses the freeze (the comparison would race it).
    let (log, stable) = src!();
    m.propose_conf_change(
      &1,
      now,
      log,
      stable,
      crate::ConfChange::new(crate::ConfChangeType::AddLearnerNode, 5u64, Bytes::new()),
    )
    .unwrap()
    .unwrap();
    assert!(matches!(
      m.prepare_merge(&1, now, log, stable, &2).unwrap(),
      Err(MergeError::ConfChangeInFlight)
    ));
    drain_storage(&mut m, 1, now, log, stable);
  }
  {
    // Freeze, then: a second freeze refuses; a parked commit refuses a second commit.
    let (log, stable) = src!();
    m.prepare_merge(&1, now, log, stable, &2).unwrap().unwrap();
    drain_storage(&mut m, 1, now, log, stable);
    assert!(matches!(
      m.prepare_merge(&1, now, log, stable, &2).unwrap(),
      Err(MergeError::AlreadyFrozen)
    ));
  }
  {
    let (log, stable) = stores.0.get_mut(&2).map(|(l, s)| (l, s)).unwrap();
    m.commit_merge(&2, now, log, stable, &1).unwrap().unwrap();
    drain_storage(&mut m, 2, now, log, stable);
    assert!(matches!(
      m.commit_merge(&2, now, log, stable, &1).unwrap(),
      Err(MergeError::AlreadyPending)
    ));
  }
  {
    // The abort's own gates, off group 4's leader: self-abort, an unhosted source, an
    // unfrozen source, and — with group 1 frozen FOR group 2 — the claim refusal (only the
    // claimed target may abort or thaw the merge; a foreign thaw would move the source's
    // counter under the claimed target's parked commit).
    let (log, stable) = stores.0.get_mut(&4).map(|(l, s)| (l, s)).unwrap();
    assert!(matches!(
      m.rollback_merge(&4, now, log, stable, &4).unwrap(),
      Err(MergeError::SelfMerge)
    ));
    assert!(matches!(
      m.rollback_merge(&4, now, log, stable, &9).unwrap(),
      Err(MergeError::SourceMissing)
    ));
    assert!(matches!(
      m.rollback_merge(&4, now, log, stable, &3).unwrap(),
      Err(MergeError::NotFrozen)
    ));
    assert!(matches!(
      m.rollback_merge(&4, now, log, stable, &1).unwrap(),
      Err(MergeError::SourceClaimed)
    ));
    assert!(matches!(
      m.propose_merge_unfreeze(&4, now, log, stable, &2).unwrap(),
      Err(MergeError::NotFrozen)
    ));
    assert!(matches!(
      m.commit_merge(&4, now, log, stable, &1).unwrap(),
      Err(MergeError::SourceClaimed)
    ));
  }
  {
    // The claim gate on the thaw itself: group 1 is frozen for 2, so a thaw riding any other
    // target's abort refuses.
    let (log, stable) = stores.0.get_mut(&1).map(|(l, s)| (l, s)).unwrap();
    assert!(matches!(
      m.propose_merge_unfreeze(&1, now, log, stable, &4).unwrap(),
      Err(MergeError::SourceClaimed)
    ));
  }
}

/// The abort names an APPLIED freeze: while the freeze is only PENDING (appended, unapplied)
/// the rollback refuses typed — its generation and claim are unreadable until it applies, and
/// a freeze that never commits self-heals through truncation instead. Once applied, the abort
/// lands on the target and the relayed thaw walks the source's counter past the freeze.
#[test]
fn rollback_refuses_a_pending_freeze_then_lands() {
  let (mut m, mut stores) = merge_host(1, 1);
  let now = Instant::ORIGIN;
  {
    let (log, stable) = stores.0.get_mut(&1).map(|(l, s)| (l, s)).unwrap();
    m.prepare_merge(&1, now, log, stable, &2).unwrap().unwrap();
  }
  // No drain: the freeze is pending, not applied — the abort refuses.
  {
    let (log, stable) = stores.0.get_mut(&2).map(|(l, s)| (l, s)).unwrap();
    assert!(matches!(
      m.rollback_merge(&2, now, log, stable, &1).unwrap(),
      Err(MergeError::NotFrozen)
    ));
  }
  {
    let (log, stable) = stores.0.get_mut(&1).map(|(l, s)| (l, s)).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(m.group(&1).unwrap().is_frozen());
  // Applied: the abort lands on the target's log and stages exactly one thaw relay.
  {
    let (log, stable) = stores.0.get_mut(&2).map(|(l, s)| (l, s)).unwrap();
    m.rollback_merge(&2, now, log, stable, &1).unwrap().unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert_eq!(m.group(&2).unwrap().shape_gen(), 1, "the abort bumped");
  let relay = m
    .poll_pending_merge_abort()
    .expect("the abort relays the thaw");
  assert_eq!(
    (relay.target, relay.source, relay.source_gen_after),
    (2, 1, 1)
  );
  {
    let (log, stable) = stores.0.get_mut(&1).map(|(l, s)| (l, s)).unwrap();
    m.propose_merge_unfreeze(&1, now, log, stable, &2)
      .unwrap()
      .unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  let ep = m.group(&1).unwrap();
  assert!(!ep.is_frozen(), "thawed");
  assert_eq!(ep.shape_gen(), 2, "0 -> 1 (freeze) -> 2 (thaw)");
  assert!(!ep.merge_freeze_active());
}

/// The source LEADER's host resolves LAST — waitForApplication, container-local: while any
/// source peer's match sits below the freeze boundary, the host whose local source replica IS
/// the source leader keeps its park (and thereby keeps the source leader alive and feeding F
/// to the stragglers); it resolves only once every peer matched through the boundary. Without
/// this leg, the early teardown of the source leader strands every slow source follower below
/// F forever — and their hosts' parks with them.
#[test]
fn source_leader_host_resolves_last() {
  use crate::{AppendResponse, Message, VoteResponse};
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let mut stores = MapStores(
    std::collections::BTreeMap::new(),
    std::collections::BTreeSet::new(),
  );
  let three_voters = || {
    Config::try_new(
      1u64,
      std::vec![1u64, 2, 3],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap()
  };
  // Feed one peer ack for `upto` on `gid` from `peer`.
  fn ack(
    m: &mut MultiRaft<u64, u64, CountSm>,
    stores: &mut MapStores,
    gid: u64,
    peer: u64,
    upto: Index,
  ) {
    let (log, stable) = stores.0.get_mut(&gid).unwrap();
    m.handle_message(
      &gid,
      Instant::ORIGIN,
      log,
      stable,
      peer,
      Message::AppendResponse(AppendResponse::new(
        Term::new(1),
        peer,
        false,
        Index::ZERO,
        Term::ZERO,
        upto,
      )),
    )
    .unwrap();
    let (log, stable) = stores.0.get_mut(&gid).unwrap();
    drain_storage(m, gid, Instant::ORIGIN, log, stable);
  }
  // Source (1) and target (2): 3-voter groups led locally (peer 2's vote elects; peer 3 lags).
  for gid in [1u64, 2] {
    stores
      .0
      .insert(gid, (VecLog::default(), AsyncStable::default()));
    m.create_group(gid, three_voters(), Instant::ORIGIN, 7, CountSm::default())
      .unwrap();
    let d = m.group(&gid).unwrap().poll_timeout().unwrap();
    let (log, stable) = stores.0.get_mut(&gid).unwrap();
    m.handle_timeout(&gid, d, log, stable).unwrap();
    drain_storage(&mut m, gid, d, log, stable);
    m.handle_message(
      &gid,
      d,
      log,
      stable,
      2u64,
      Message::VoteResponse(VoteResponse::new(Term::new(1), 2u64, false, false)),
    )
    .unwrap();
    drain_storage(&mut m, gid, d, log, stable);
    assert!(m.group(&gid).unwrap().role().is_leader());
    // Commit the no-op via peer 2's ack (quorum 2 of 3); peer 3 never acks anything.
    ack(&mut m, &mut stores, gid, 2, Index::new(1));
  }
  let now = Instant::ORIGIN;

  // Freeze the source: peer 2's ack commits the PrepareMerge; peer 3's match stays at 0.
  let f = {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.prepare_merge(&1, now, log, stable, &2).unwrap().unwrap()
  };
  ack(&mut m, &mut stores, 1, 2, f);
  assert!(m.group(&1).unwrap().is_frozen());

  // Park the target the same way.
  let k = {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.commit_merge(&2, now, log, stable, &1).unwrap().unwrap()
  };
  ack(&mut m, &mut stores, 2, 2, k);
  assert!(m.group(&2).unwrap().pending_merge().is_some(), "parked");

  // First pass: the window is open — the target leader seals it (a no-op at k+1) and waits.
  assert!(
    m.service_merge_applies(now, &mut stores).is_empty(),
    "the open window seals and holds"
  );
  // Peer 2's ack commits the seal; peer 3's SOURCE match still sits below the boundary: the
  // source-leader host must keep WAITING — park intact, source leader alive and still able to
  // feed the straggler.
  ack(&mut m, &mut stores, 2, 2, k.next());
  assert!(
    m.service_merge_applies(now, &mut stores).is_empty(),
    "the source leader's host waits for its peers to match the boundary"
  );
  assert!(m.contains_group(&1), "the source leader survives the wait");
  assert!(m.group(&2).unwrap().pending_merge().is_some());

  // The straggler catches up (its ack reaches F): the wait releases and the host resolves.
  ack(&mut m, &mut stores, 1, 3, f);
  let resolutions = m.service_merge_applies(now, &mut stores);
  assert_eq!(
    resolutions,
    std::vec![MergeResolution::Merged {
      source: 1,
      target: 2
    }]
  );
  assert!(!m.contains_group(&1));
}

/// The freeze gates cover the WHOLE admin propose family: a frozen (or freezing) group refuses
/// a split (forking would mutate the FSM above the freeze boundary), refuses to be a merge
/// TARGET (absorbing above its own boundary), and a source mid-absorb refuses a fresh freeze —
/// every arm typed, nothing appended.
#[test]
fn freeze_gates_cover_split_and_target_verbs() {
  let (mut m, mut stores) = merge_host(2, 3);
  let now = Instant::ORIGIN;
  // Freeze group 1 (into 2, the ordinary pairing).
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.prepare_merge(&1, now, log, stable, &2).unwrap().unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(m.group(&1).unwrap().is_frozen());
  // A frozen parent refuses a split, typed as the propose family does.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    assert!(matches!(
      m.propose_split(&1, now, log, stable, &7, 0, Bytes::new())
        .unwrap(),
      Err(SplitError::Propose(crate::ProposeError::Frozen))
    ));
  }
  // A frozen group can be neither a prepare target nor a commit target.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    assert!(matches!(
      m.prepare_merge(&2, now, log, stable, &1).unwrap(),
      Err(MergeError::AlreadyFrozen)
    ));
  }
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    assert!(matches!(
      m.commit_merge(&1, now, log, stable, &2).unwrap(),
      Err(MergeError::AlreadyFrozen)
    ));
  }
  // A target mid-absorb (parked) refuses a fresh freeze of ITSELF.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.commit_merge(&2, now, log, stable, &1).unwrap().unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(m.group(&2).unwrap().pending_merge().is_some());
  // A third, unfrozen group to name as the target: the mid-absorb SOURCE gate is what must
  // fire (naming the frozen group 1 would trip its target-frozen gate first).
  stores
    .0
    .insert(3, (VecLog::default(), AsyncStable::default()));
  m.create_group(3, single_node_cfg(1), now, 7, CountSm::default())
    .unwrap();
  {
    let (log, stable) = stores.0.get_mut(&3).unwrap();
    let d = m.group(&3).unwrap().poll_timeout().unwrap();
    m.handle_timeout(&3, d, log, stable).unwrap();
    drain_storage(&mut m, 3, d, log, stable);
  }
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    assert!(matches!(
      m.prepare_merge(&2, now, log, stable, &3).unwrap(),
      Err(MergeError::AlreadyPending)
    ));
  }
}

/// An FSM that refuses the absorb POISONS the target (the deterministic fail-stop) — and the
/// service must surface NO `Merged` resolution for it: the driver would otherwise floor the
/// source terminally and tear its stores down behind the fail-stop, destroying the union's
/// only copy. The fail-stop stands alone; the source's storage half stays untouched.
#[test]
fn poisoned_absorb_surfaces_no_resolution() {
  #[derive(Default)]
  struct NoAbsorbSm(usize);
  impl crate::StateMachine for NoAbsorbSm {
    type Command = Bytes;
    type Response = usize;
    type Snapshot = u64;
    type Error = core::convert::Infallible;
    fn apply(&mut self, _: Index, _: Bytes) -> Result<usize, Self::Error> {
      self.0 += 1;
      Ok(self.0)
    }
    fn snapshot(&self) -> Result<u64, Self::Error> {
      Ok(self.0 as u64)
    }
    fn restore(&mut self, s: u64) -> Result<(), Self::Error> {
      self.0 = s as usize;
      Ok(())
    }
  }
  fn drain(
    m: &mut MultiRaft<u64, u64, NoAbsorbSm>,
    gid: u64,
    now: Instant,
    log: &mut VecLog,
    stable: &mut AsyncStable,
  ) {
    while matches!(
      m.handle_storage(&gid, now, log, stable),
      Some(StorageProgress::MorePending)
    ) {}
  }
  let mut m: MultiRaft<u64, u64, NoAbsorbSm> = MultiRaft::new();
  let mut stores = MapStores(
    std::collections::BTreeMap::new(),
    std::collections::BTreeSet::new(),
  );
  for gid in [1u64, 2] {
    stores
      .0
      .insert(gid, (VecLog::default(), AsyncStable::default()));
    m.create_group(
      gid,
      single_node_cfg(1),
      Instant::ORIGIN,
      7,
      NoAbsorbSm::default(),
    )
    .unwrap();
    let (log, stable) = stores.0.get_mut(&gid).unwrap();
    let d = m.group(&gid).unwrap().poll_timeout().unwrap();
    m.handle_timeout(&gid, d, log, stable).unwrap();
    drain(&mut m, gid, d, log, stable);
    assert!(m.group(&gid).unwrap().role().is_leader());
  }
  let now = Instant::ORIGIN;
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.prepare_merge(&1, now, log, stable, &2).unwrap().unwrap();
    drain(&mut m, 1, now, log, stable);
  }
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.commit_merge(&2, now, log, stable, &1).unwrap().unwrap();
    drain(&mut m, 2, now, log, stable);
  }
  assert!(m.group(&2).unwrap().pending_merge().is_some());
  assert!(
    m.service_merge_applies(now, &mut stores).is_empty(),
    "the first pass only seals"
  );
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain(&mut m, 2, now, log, stable);
  }
  let resolutions = m.service_merge_applies(now, &mut stores);
  assert!(
    resolutions.is_empty(),
    "a poisoned absorb must not hand the driver a Merged to floor and tear down: {resolutions:?}"
  );
  let tep = m.group(&2).unwrap();
  assert!(tep.is_poisoned(), "the deterministic fail-stop stands");
  assert!(
    !m.contains_group(&1),
    "the extracted source endpoint is consumed either way (its stores are not)"
  );
}
