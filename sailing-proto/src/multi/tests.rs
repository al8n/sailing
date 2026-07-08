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
fn drain_storage<F>(
  m: &mut MultiRaft<u64, u64, F>,
  gid: u64,
  now: Instant,
  log: &mut VecLog,
  stable: &mut AsyncStable,
) where
  F: crate::StateMachine<Command = Bytes, Snapshot = u64>,
  F::Error: core::error::Error,
{
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
    0,
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
    0,
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
    0,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    CountSm::default(),
  )
  .unwrap();
  assert_eq!(mr.host_id(), Some(&1));

  // Removing the LAST group must not un-latch the identity: live transport connections stay
  // authenticated under it, so a re-created group with a different id would silently wedge.
  assert!(mr.remove_group(&1, &mut empty_stores()).unwrap().is_some());
  assert!(mr.is_empty());
  assert_eq!(mr.host_id(), Some(&1), "identity survives an empty host");
  assert_eq!(
    mr.create_group(
      2,
      0,
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
    0,
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
    0,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    CountSm::default(),
  )
  .unwrap();
  mr.create_group(
    2,
    0,
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
    0,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    CountSm::default(),
  )
  .unwrap();
  assert_eq!(
    mr.create_group(
      1,
      0,
      single_node_cfg(1),
      Instant::ORIGIN,
      42,
      CountSm::default()
    ),
    Err(CreateGroupError::Exists)
  );
  assert_eq!(mr.len(), 1);
  assert!(mr.remove_group(&1, &mut empty_stores()).unwrap().is_some());
  assert!(mr.is_empty());
  assert!(mr.remove_group(&1, &mut empty_stores()).unwrap().is_none());
}

#[test]
fn mismatched_node_id_is_rejected() {
  let mut mr = MultiRaft::<u64, u64, CountSm>::new();
  mr.create_group(
    1,
    0,
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
      0,
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
    0,
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
        0,
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
    0,
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
  mr.create_group(
    1,
    0,
    two_voter_cfg(),
    Instant::ORIGIN,
    42,
    CountSm::default(),
  )
  .unwrap();
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());

  // A 2-voter campaign queues vote traffic to peer 2 — output pending, deliberately undrained.
  let d = mr.group(&1).unwrap().poll_timeout().unwrap();
  mr.handle_timeout(&1, d, &mut log, &mut stable).unwrap();

  let mut ep = mr
    .remove_group(&1, &mut empty_stores())
    .unwrap()
    .expect("the group is returned");
  assert!(
    ep.poll_message().is_some(),
    "output WAS queued at removal (the dirty entry is genuinely stale)"
  );
  assert!(
    mr.poll_message().is_none(),
    "the stale dirty entry is skipped"
  );
  assert!(mr.poll_event().is_none());
  assert!(mr.remove_group(&1, &mut empty_stores()).unwrap().is_none());

  // The same gid is admissible again.
  mr.create_group(
    1,
    0,
    two_voter_cfg(),
    Instant::ORIGIN,
    42,
    CountSm::default(),
  )
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
    0,
    three_voter_cfg(),
    Instant::ORIGIN,
    42,
    CountSm::default(),
  )
  .unwrap();
  mr.create_group(
    200,
    0,
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
    0,
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

  fn absorb(&mut self, source: Self) -> bool {
    self.units += source.units;
    true
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
  m.create_group(7, 0, two_voters, Instant::ORIGIN, 42, SplitSm::default())
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
    0,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    SplitSm::default(),
  )
  .unwrap();
  m.create_group(
    8,
    0,
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
    0,
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
    0,
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
  m.create_group(7, 0, cfg, Instant::ORIGIN, 42, SplitSm::default())
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
  m.create_group(7, 0, cfg, Instant::ORIGIN, 42, SplitSm::default())
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
  m.create_group(7, 0, cfg, Instant::ORIGIN, 42, SplitSm::default())
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
  m.create_group(7, 0, cfg, Instant::ORIGIN, 42, SplitSm::default())
    .unwrap();
  // The squatter: hosted under the child id, zero progress (its timers never fire here).
  m.create_group(
    200,
    0,
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
  m.remove_group(&200, &mut empty_stores()).unwrap();
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
  m.create_group(7, 0, cfg, Instant::ORIGIN, 42, SplitSm::default())
    .unwrap();
  m.create_group(
    200,
    0,
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
  m.create_group(7, 0, cfg, Instant::ORIGIN, 42, SplitSm::default())
    .unwrap();
  m.create_group(
    200,
    0,
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

  m.remove_group(&200, &mut empty_stores()).unwrap();
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

  assert!(m.remove_group(&7, &mut empty_stores()).unwrap().is_some());
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
  m.create_group(7, 0, cfg, Instant::ORIGIN, 42, SplitSm::default())
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
  m.create_group(7, 0, cfg, Instant::ORIGIN, 42, SplitSm::default())
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
  m.create_group(200, 0, single_node_cfg(1), d, 43, SplitSm::default())
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
  m.remove_group(&200, &mut empty_stores()).unwrap();
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

#[test]
fn reshaped_twin_breaks_the_fork_merge_capture_cycle() {
  // Arm (b) at-or-past lineage, the reshape-cycle shape: the parent's crash-replay re-stages a
  // committed fork whose child is hosted AND has reshaped since birth (here: the child froze
  // for a merge, bumping its lineage past the fork's `child_gen`). Under lineage EQUALITY the
  // fork parked forever, and the park's standing fence closed a true dependency cycle — arm
  // (a) needed the child gone, the child leaves only when the merge into the parent resolves,
  // the resolve arm needs the parent's absorb capture, and the capture is exactly what the
  // fence blocks (fence → child-gone → merge → capture → fence). At-or-past lineage is the
  // cycle's one breakable link: a hosted twin past the baseline under the fork's own lineage
  // EVOLVED is still this fork's data, so the blob is redundant and the fence lifts.
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let (mut clog, mut cstable) = (VecLog::default(), AsyncStable::default());
  let (mut plog, mut pstable) = (VecLog::default(), AsyncStable::default());
  let now = Instant::ORIGIN;
  let drain =
    |m: &mut MultiRaft<u64, u64, SplitSm>, gid: u64, log: &mut VecLog, stable: &mut AsyncStable| {
      while matches!(
        m.handle_storage(&gid, now, log, stable),
        Some(StorageProgress::MorePending)
      ) {}
    };

  // The child (gid 1), hosted BEFORE the parent's replay re-stages the fork naming it.
  m.create_group(1, 0, single_node_cfg(1), now, 43, SplitSm::default())
    .unwrap();

  // The parent (gid 2) restores from a durable log whose committed tail holds the split naming
  // child 1 — the crash-replay staging: the pre-crash relay never resolved, so it relays again.
  let cmd = {
    let mut buf = Vec::new();
    Bytes::from_static(b"c").encode(&mut buf);
    Bytes::from(buf)
  };
  plog.force_append(&[
    crate::Entry::new(
      Term::new(1),
      Index::new(1),
      crate::EntryKind::Normal,
      cmd.clone(),
    ),
    crate::Entry::new(
      Term::new(1),
      Index::new(2),
      crate::EntryKind::Normal,
      cmd.clone(),
    ),
    crate::Entry::new(Term::new(1), Index::new(3), crate::EntryKind::Normal, cmd),
    crate::Entry::new(
      Term::new(1),
      Index::new(4),
      crate::EntryKind::Split,
      split_entry_bytes(1, 0, 1, 2),
    ),
  ]);
  pstable.force_state(Term::new(1), Some(1u64), Index::new(4));
  m.restore_group(
    2,
    single_node_cfg(1),
    now,
    42,
    SplitSm::default(),
    1,
    &mut plog,
    &mut pstable,
  )
  .unwrap();
  assert_eq!(
    m.group(&2).unwrap().state_machine().units,
    1,
    "the replayed split gave the half up again"
  );

  // The child reshapes AFTER birth: it leads, applies load past the manufactured baseline, and
  // freezes for the merge into the parent — the freeze apply bumps its lineage to 1 > child_gen.
  let d = lead_single_split(&mut m, 1, &mut clog, &mut cstable);
  commit_one_split(&mut m, 1, d, &mut clog, &mut cstable);
  let mut stores = MapStores(std::collections::BTreeMap::new(), Default::default());
  stores.0.insert(1, (clog, cstable));
  stores.0.insert(2, (plog, pstable));
  m.prepare_merge(&1, d, &mut stores, &2).unwrap().unwrap();
  {
    let (clog, cstable) = stores.0.get_mut(&1).unwrap();
    drain(&mut m, 1, clog, cstable);
  }
  assert!(m.group(&1).unwrap().is_frozen());
  assert_eq!(m.group(&1).unwrap().shape_gen(), 1);
  assert!(m.group(&1).unwrap().applied_index() >= FORK_BASE_INDEX);

  // The reshaped twin resolves the fork as redundant at FIRST examination: no park, no
  // conflict signal, the staged blob discarded, the fence gone.
  assert!(
    m.poll_pending_fork().is_none(),
    "a reshaped twin resolves the fork without yielding"
  );
  assert!(
    m.group(&2).unwrap().peek_pending_fork().is_none(),
    "the redundant fork is consumed — lineage equality parked here forever"
  );
  assert_eq!(m.peek_split_conflict(), None, "nothing parked, no cue");
  assert_eq!(m.group_gen(&2), 1, "the relay guard advanced");

  // The dependent merge now completes: park, seal, resolve — the absorb capture the fence was
  // blocking lands, the child departs, and the parent serves the union.
  let dp = {
    let (plog, pstable) = stores.0.get_mut(&2).unwrap();
    lead_single_split(&mut m, 2, plog, pstable)
  };
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.commit_merge(&2, dp, log, stable, &1).unwrap().unwrap();
    while matches!(
      m.handle_storage(&2, dp, log, stable),
      Some(StorageProgress::MorePending)
    ) {}
  }
  assert!(m.group(&2).unwrap().pending_merge().is_some(), "parked");
  assert!(
    m.service_merge_applies(dp, &mut stores).is_empty(),
    "the first pass only seals the window"
  );
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    while matches!(
      m.handle_storage(&2, dp, log, stable),
      Some(StorageProgress::MorePending)
    ) {}
  }
  let resolutions = m.service_merge_applies(dp, &mut stores);
  assert_eq!(
    resolutions,
    std::vec![MergeResolution::Merged {
      source: 1,
      target: 2
    }],
    "the cycle is broken: the absorb capture is no longer fence-blocked"
  );
  assert!(!m.contains_group(&1), "the child departed via the merge");
  assert_eq!(
    m.group(&2).unwrap().state_machine().units,
    2,
    "the parent serves the union (its own half plus the absorbed twin)"
  );
}

#[test]
fn recreated_squatter_at_higher_lineage_resolves_redundant() {
  // Arm (b) above equality, the recreated-squatter shape: the id under the staged fork is
  // hosted by an incarnation that has MOVED PAST the fork's lineage (here it reshaped with its
  // own split). Under single-incarnation ids and the two-act rejoin — an id returns only
  // through retire-then-recreate, and retirement certifies the fork's incarnation was
  // registered somewhere first — same-id-at-higher-lineage means the gen-0 fork is superseded
  // history: its blob is redundant, and holding the park (as equality did) deadlocks the
  // parent's fence instead of protecting anything.
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  let (mut log200, mut stable200) = (VecLog::default(), AsyncStable::default());
  let cfg = single_node_cfg(1).with_snapshot_threshold(1);
  m.create_group(7, 0, cfg, Instant::ORIGIN, 42, SplitSm::default())
    .unwrap();
  let d = lead_single_split(&mut m, 7, &mut log, &mut stable);
  for _ in 0..3 {
    commit_one_split(&mut m, 7, d, &mut log, &mut stable);
  }

  // Propose (gate passes: 200 unhosted), then the squatter arrives in the propose→apply
  // window and lives a life of its own: load past the baseline, then its OWN split — the
  // lineage bump that broke equality.
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
  m.create_group(200, 0, single_node_cfg(1), d, 43, SplitSm::default())
    .unwrap();
  let d200 = lead_single_split(&mut m, 200, &mut log200, &mut stable200);
  commit_one_split(&mut m, 200, d200, &mut log200, &mut stable200);
  m.propose_split(
    &200,
    d200,
    &mut log200,
    &stable200,
    &300,
    0,
    Bytes::from_static(b"\x01"),
  )
  .unwrap()
  .unwrap();
  while matches!(
    m.handle_storage(&200, d200, &mut log200, &mut stable200),
    Some(StorageProgress::MorePending)
  ) {}
  assert_eq!(m.group(&200).unwrap().shape_gen(), 1);

  // Apply the parent's split against the now-hosted, now-reshaped squatter.
  m.flush_appends(&7, d, &log, &stable).unwrap();
  while matches!(
    m.handle_storage(&7, d, &mut log, &mut stable),
    Some(StorageProgress::MorePending)
  ) {}

  // The drain resolves the parent's fork as redundant and flows on to the squatter's own
  // staged fork — later forks are not wedged behind superseded history.
  let fork = m
    .poll_pending_fork()
    .expect("the squatter's own fork still yields");
  assert_eq!((fork.parent, fork.child), (200, 300));
  assert!(m.poll_pending_fork().is_none());
  assert!(
    m.group(&7).unwrap().peek_pending_fork().is_none(),
    "the superseded fork is consumed — lineage equality parked here forever"
  );
  assert_eq!(m.peek_split_conflict(), None, "nothing parked, no cue");
  assert_eq!(m.group_gen(&7), 1, "the relay guard advanced");

  // The fence lifted with the redundant fold: the next committed entry captures past the split.
  commit_one_split(&mut m, 7, d, &mut log, &mut stable);
  let (meta, _blob) = stable
    .snapshot()
    .expect("no orphaned fence from the redundant fold");
  assert!(meta.last_index() >= idx, "the capture crosses the split");
}

#[test]
fn below_lineage_squatter_stays_parked() {
  // The negative pin on arm (b)'s lower edge: a hosted child BELOW the fork's `child_gen`
  // parks — never resolves redundant. That state predates the fork's mint, so it cannot
  // contain the handover; discarding the blob against it would lose the partition's only
  // local copy. The shape needs a skewed catalog to exist at all — the fork minted at a
  // floor the squatting host never learned (its stale incarnation survived a retirement it
  // was partitioned through) — so the coordinators make it remote, but the floor-free
  // container must hold the conservative park and let the embedder resolve it through arm (a).
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  let (mut log200, mut stable200) = (VecLog::default(), AsyncStable::default());
  let cfg = single_node_cfg(1).with_snapshot_threshold(1);
  m.create_group(7, 0, cfg, Instant::ORIGIN, 42, SplitSm::default())
    .unwrap();
  let d = lead_single_split(&mut m, 7, &mut log, &mut stable);
  for _ in 0..3 {
    commit_one_split(&mut m, 7, d, &mut log, &mut stable);
  }

  // The fork mints at child_gen 1; the squatter arrives at lineage 0 and catches up past the
  // baseline, so the applied gate passes and lineage alone decides.
  let idx = m
    .propose_split(
      &7,
      d,
      &mut log,
      &stable,
      &200,
      1,
      Bytes::from_static(b"\x02"),
    )
    .unwrap()
    .unwrap();
  m.create_group(200, 0, single_node_cfg(1), d, 43, SplitSm::default())
    .unwrap();
  let d200 = lead_single_split(&mut m, 200, &mut log200, &mut stable200);
  commit_one_split(&mut m, 200, d200, &mut log200, &mut stable200);
  assert!(m.group(&200).unwrap().applied_index() >= FORK_BASE_INDEX);
  assert_eq!(m.group(&200).unwrap().shape_gen(), 0);

  m.flush_appends(&7, d, &log, &stable).unwrap();
  while matches!(
    m.handle_storage(&7, d, &mut log, &mut stable),
    Some(StorageProgress::MorePending)
  ) {}

  assert!(m.poll_pending_fork().is_none(), "parked, not resolved");
  assert_eq!(m.poll_split_conflict(), Some((7, 200)));
  assert!(
    m.group(&7).unwrap().peek_pending_fork().is_some(),
    "the below-lineage hold keeps the blob staged"
  );
  // The fence stands while parked: committed load past the threshold captures nothing at or
  // past the split.
  commit_one_split(&mut m, 7, d, &mut log, &mut stable);
  let pre = stable.snapshot().map(|(meta, _)| meta.last_index());
  assert!(
    pre.is_none_or(|boundary| boundary < idx),
    "the parked fork's fence holds (boundary {pre:?}, split {idx:?})"
  );

  // Arm (a) is the shape's designed exit: the embedder removes the stale squatter and the
  // fork materializes with its minted lineage intact.
  m.remove_group(&200, &mut empty_stores()).unwrap();
  let fork = m
    .poll_pending_fork()
    .expect("removal unparks the fork for materialization");
  assert_eq!((fork.parent, fork.child), (7, 200));
  assert_eq!(fork.child_gen, 1, "the minted lineage rides the yield");
  assert_eq!(fork.fsm.units, 2, "the held half materializes intact");
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

/// An empty store seam for a [`MultiRaft::remove_group`] whose participant gate resolves on
/// in-memory state alone — a non-merge teardown never reaches the `Claimed` leg's append-pending
/// scan, so it has no source log to read.
fn empty_stores() -> MapStores {
  MapStores(
    std::collections::BTreeMap::new(),
    std::collections::BTreeSet::new(),
  )
}

/// A store seam whose PERSISTED floor/lineage is settable per group — for exercising the unhosted
/// merge-obligation discharge against a source that was torn down and FLOORED (not terminally
/// merged) rather than observed thawed in place. Storage resolution delegates to an inner
/// [`MapStores`]; only the durable lineage/floor record diverges.
struct LineageStores {
  inner: MapStores,
  floors: std::collections::BTreeMap<u64, u64>,
  lineages: std::collections::BTreeMap<u64, u64>,
}

impl crate::GroupStores<u64, VecLog, AsyncStable> for LineageStores {
  fn stores(&mut self, group: &u64) -> Option<(&mut VecLog, &mut AsyncStable)> {
    self.inner.stores(group)
  }
}

impl crate::FloorStore<u64> for LineageStores {
  fn floor(&self, gid: &u64) -> u64 {
    self
      .floors
      .get(gid)
      .copied()
      .unwrap_or_else(|| self.inner.floor(gid))
  }

  fn lineage(&self, gid: &u64) -> u64 {
    self.lineages.get(gid).copied().unwrap_or(0)
  }
}

/// A host with two single-voter groups (1 = source with `src_count` applied commands, 2 = target
/// with `tgt_count`), each seeded with the given state machine, elected and fully drained.
fn merge_host_with<F>(
  src_fsm: F,
  src_count: usize,
  tgt_fsm: F,
  tgt_count: usize,
) -> (MultiRaft<u64, u64, F>, MapStores)
where
  F: crate::StateMachine<Command = Bytes, Snapshot = u64>,
  F::Error: core::error::Error,
{
  let mut m: MultiRaft<u64, u64, F> = MultiRaft::new();
  let mut stores = MapStores(
    std::collections::BTreeMap::new(),
    std::collections::BTreeSet::new(),
  );
  for (gid, n, fsm) in [(1u64, src_count, src_fsm), (2u64, tgt_count, tgt_fsm)] {
    stores
      .0
      .insert(gid, (VecLog::default(), AsyncStable::default()));
    m.create_group(gid, 0, single_node_cfg(1), Instant::ORIGIN, 7, fsm)
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

/// A host with two single-voter [`CountSm`] groups (1 = source with `src_count` applied commands,
/// 2 = target with `tgt_count`), each elected and fully drained.
fn merge_host(src_count: usize, tgt_count: usize) -> (MultiRaft<u64, u64, CountSm>, MapStores) {
  merge_host_with(CountSm::default(), src_count, CountSm::default(), tgt_count)
}

/// Freeze group 1 into group 2 and park group 2's commit, fully drained: the state every
/// resolution arm starts from. Returns the parked index k.
fn freeze_and_park<F>(m: &mut MultiRaft<u64, u64, F>, stores: &mut MapStores) -> Index
where
  F: crate::StateMachine<Command = Bytes, Snapshot = u64>,
  F::Error: core::error::Error,
{
  let now = Instant::ORIGIN;
  {
    m.prepare_merge(&1, now, stores, &2).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&1).unwrap();
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

/// Step the leader of single-voter group `gid` down to a follower by injecting a heartbeat from a
/// phantom peer at a strictly higher term — the source-leader loss the delivery-based retirement
/// must tolerate. Term adoption is unconditional, so the applied state and `shape_gen` are
/// untouched (a heartbeat truncates nothing); the group re-campaigns on its next timeout.
fn step_down<F>(
  m: &mut MultiRaft<u64, u64, F>,
  gid: u64,
  log: &mut VecLog,
  stable: &mut AsyncStable,
) where
  F: crate::StateMachine<Command = Bytes, Snapshot = u64>,
  F::Error: core::error::Error,
{
  let now = Instant::ORIGIN;
  let higher = Term::new(m.group(&gid).unwrap().term().get() + 5);
  m.handle_message(
    &gid,
    now,
    log,
    stable,
    2u64,
    Message::Heartbeat(crate::Heartbeat::new(
      higher,
      2u64,
      Index::ZERO,
      Bytes::new(),
    )),
  )
  .unwrap();
  drain_storage(m, gid, now, log, stable);
  assert!(
    m.group(&gid).unwrap().role().is_follower(),
    "the higher term stepped the leader down"
  );
}

/// Re-elect single-voter group `gid` (a follower after [`step_down`]) back to leader — its next
/// campaign self-votes and wins.
fn re_elect<F>(m: &mut MultiRaft<u64, u64, F>, gid: u64, log: &mut VecLog, stable: &mut AsyncStable)
where
  F: crate::StateMachine<Command = Bytes, Snapshot = u64>,
  F::Error: core::error::Error,
{
  let d = m.group(&gid).unwrap().poll_timeout().unwrap();
  m.handle_timeout(&gid, d, log, stable).unwrap();
  while matches!(
    m.handle_storage(&gid, d, log, stable),
    Some(StorageProgress::MorePending)
  ) {}
  assert!(
    m.group(&gid).unwrap().role().is_leader(),
    "the single voter re-elects"
  );
}

/// Close the parked commit's abort window: the first service pass resolves nothing — it
/// appends the leader's seal no-op at the coordinate after the parked entry — and the drain
/// commits it (single-voter shape: commit advances at the local storage drain).
fn seal_window<F>(m: &mut MultiRaft<u64, u64, F>, stores: &mut MapStores)
where
  F: crate::StateMachine<Command = Bytes, Snapshot = u64>,
  F::Error: core::error::Error,
{
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

/// The lineage-serialization fence on `commit_merge`'s target side. A `Split` appended-and-unapplied
/// on the target bumps its `shape_gen` when it drains, so a `CommitMerge` proposed over it mints a
/// generation the split immediately stales — the parked apply no-ops at its lineage guard and emits
/// `MergeAborted` WITHOUT recording the source's thaw obligation, leaving the source `frozen_for` a
/// target that owes it nothing (a permanent strand). RED before the gate: `commit_merge` ADMITTED.
/// GREEN: refused `SplitInFlight` while the split is in flight; once the split applies the same
/// `commit_merge` mints from the post-split counter and absorbs — the source is never stranded.
#[test]
fn commit_merge_defers_a_target_reshaping_by_a_split() {
  let (mut m, mut stores) = merge_host_with(SplitSm::default(), 1, SplitSm::default(), 3);
  let now = Instant::ORIGIN;

  // Freeze source 1 into target 2: 1 is frozen_for 2, its barrier trivially met (single voter).
  {
    m.prepare_merge(&1, now, &mut stores, &2).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    m.group(&1).unwrap().is_frozen(),
    "source frozen for the target"
  );

  // Append a Split on the TARGET without draining it: split_in_flight is armed, unapplied.
  let split_idx = {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.propose_split(&2, now, log, stable, &3, 0, Bytes::from_static(b"\x01"))
      .unwrap()
      .unwrap()
  };
  assert!(
    m.group(&2).unwrap().split_in_flight(),
    "the target has a split appended-unapplied"
  );

  // THE FENCE: the absorb defers while the target is reshaping (RED: it ADMITTED, then the
  // drained split staled the CommitMerge into an obligation-less MergeAborted).
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    assert_eq!(
      m.commit_merge(&2, now, log, stable, &1),
      Some(Err(MergeError::SplitInFlight)),
      "a target mid-split defers the absorb"
    );
  }
  // The source is untouched — still frozen_for the target, never stranded by a stale abort.
  assert!(m.group(&1).unwrap().is_frozen());
  assert!(
    !m.group(&2).unwrap().has_abandoned(),
    "no obligation-less abort was recorded on the target"
  );

  // Let the split resolve: it applies, `shape_gen` bumps, the fork relays out, the barrier lifts.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.flush_appends(&2, now, log, stable).unwrap();
    while matches!(
      m.handle_storage(&2, now, log, stable),
      Some(StorageProgress::MorePending)
    ) {}
  }
  assert!(!m.group(&2).unwrap().split_in_flight(), "the split applied");
  let fork = m.poll_pending_fork().expect("the fork relays out");
  assert_eq!(fork.child, 3);
  m.lift_fork_barrier(&2, split_idx);
  while m.poll_event().is_some() {}

  // Re-propose against the post-split lineage: now it admits and PARKS (not stranded).
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.commit_merge(&2, now, log, stable, &1).unwrap().unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    m.group(&2).unwrap().pending_merge().is_some(),
    "the absorb parks against the post-split target"
  );

  // Drive it to resolution: the source is absorbed and removed — the opposite of a strand.
  seal_window(&mut m, &mut stores);
  let resolutions = m.service_merge_applies(now, &mut stores);
  assert_eq!(
    resolutions,
    std::vec![MergeResolution::Merged {
      source: 1,
      target: 2
    }]
  );
  assert!(
    !m.contains_group(&1),
    "the source is absorbed, not left frozen forever"
  );
}

/// The `prepare_merge` dual of the same fence, on the SOURCE side. A source mid-split must not
/// freeze: the freeze mints `source_gen_after` from the source's live `shape_gen`, but the pending
/// split applies first and bumps it, so the freeze's generation COLLIDES with the split's on the
/// one lineage counter. Symmetric to `propose_split` refusing a freezing parent. RED before the
/// gate: the freeze ADMITTED mid-split. GREEN: refused `SplitInFlight`; the same freeze admits once
/// the split applies.
#[test]
fn prepare_merge_defers_a_source_reshaping_by_a_split() {
  let (mut m, mut stores) = merge_host_with(SplitSm::default(), 3, SplitSm::default(), 1);
  let now = Instant::ORIGIN;

  // Append a Split on the SOURCE (group 1) without draining it.
  let split_idx = {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.propose_split(&1, now, log, stable, &3, 0, Bytes::from_static(b"\x01"))
      .unwrap()
      .unwrap()
  };
  assert!(m.group(&1).unwrap().split_in_flight());

  // THE FENCE: the freeze defers while the source is reshaping (RED: it froze, colliding the
  // freeze generation with the split's on one counter).
  {
    assert_eq!(
      m.prepare_merge(&1, now, &mut stores, &2),
      Some(Err(MergeError::SplitInFlight)),
      "a source mid-split defers the freeze"
    );
  }
  assert!(
    !m.group(&1).unwrap().merge_freeze_active(),
    "the source never froze mid-split — nothing was appended"
  );

  // Resolve the split; then the same freeze admits against the post-split counter.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.flush_appends(&1, now, log, stable).unwrap();
    while matches!(
      m.handle_storage(&1, now, log, stable),
      Some(StorageProgress::MorePending)
    ) {}
  }
  assert!(!m.group(&1).unwrap().split_in_flight());
  let fork = m.poll_pending_fork().expect("the fork relays out");
  assert_eq!(fork.child, 3);
  m.lift_fork_barrier(&1, split_idx);
  {
    m.prepare_merge(&1, now, &mut stores, &2).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    m.group(&1).unwrap().is_frozen(),
    "the freeze admits once the split has applied"
  );
}

/// The SAME fence closing the CROSS-SOURCE fan-in case the split gate does not reach: a target-role
/// abort (`RollbackMerge`) in flight bumps the target's `shape_gen` when it applies, exactly like a
/// split. Two sources (1, 3) freeze into one target (2); 2 aborts 1's freeze as a release valve
/// (appended, unapplied) then tries to commit 3 while that abort is in flight. RED before the fence:
/// `commit_merge(3 -> 2)` ADMITTED, then draining 2 applied the abort (bumping the counter and
/// recording `abandoned[1]`) and stale-aborted 3's commit WITHOUT recording `abandoned[3]` — 3 left
/// frozen_for 2 forever while 1 correctly thawed (verified: `has_abandoned` cleared 1 but never
/// held 3). GREEN: refused `RollbackInFlight`; once 1's abort applies the same commit admits and 3
/// absorbs. (The abort of the SAME merge being committed is caught earlier by `AlreadyPending`.)
#[test]
fn commit_merge_defers_a_target_with_a_fanin_abort_in_flight() {
  let (mut m, mut stores) = merge_host_triple(1, 2, 1);
  let now = Instant::ORIGIN;
  for src in [1u64, 3] {
    m.prepare_merge(&src, now, &mut stores, &2)
      .unwrap()
      .unwrap();
    let (log, stable) = stores.0.get_mut(&src).unwrap();
    drain_storage(&mut m, src, now, log, stable);
    assert!(m.group(&src).unwrap().is_frozen(), "fan-in source frozen");
  }
  // Abort 1's freeze (release valve) — append but DO NOT drain: the abort is in flight on 2's log.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.rollback_merge(&2, now, log, stable, &1).unwrap().unwrap();
  }
  assert!(
    m.group(&2).unwrap().rollback_in_flight(),
    "1's abort is in flight"
  );

  // THE FENCE: committing a DIFFERENT frozen source into the same target defers while that abort is
  // in flight (RED: it admitted, then the drained abort staled 3's commit into an obligation-less
  // MergeAborted, stranding 3).
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    assert_eq!(
      m.commit_merge(&2, now, log, stable, &3),
      Some(Err(MergeError::RollbackInFlight)),
      "a target with a fan-in abort in flight defers the absorb"
    );
  }

  // Let 1's abort apply: it records abandoned[1] and clears the fence. 3 is untouched (still frozen).
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    !m.group(&2).unwrap().rollback_in_flight(),
    "the abort applied"
  );
  assert!(m.group(&3).unwrap().is_frozen(), "3 was never disturbed");

  // The same commit now admits against the post-abort lineage and parks — 3 is not stranded.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.commit_merge(&2, now, log, stable, &3).unwrap().unwrap();
  }
  // Drive the service + thaw pass to quiescence: 1 thaws out of its aborted freeze, 3 absorbs into 2.
  for _ in 0..16 {
    m.service_merge_applies(now, &mut stores);
    for g in [1u64, 2, 3] {
      if let Some((log, stable)) = stores.0.get_mut(&g) {
        drain_storage(&mut m, g, now, log, stable);
      }
    }
  }
  assert!(
    !m.group(&1).unwrap().is_frozen(),
    "1's aborted freeze thawed — its obligation was honored"
  );
  assert!(
    !m.contains_group(&3),
    "3 absorbed into 2 — not left frozen forever by a stale abort"
  );
}

/// The lineage-serialization fence on `rollback_merge`'s OWN proposer side — the fan-in strand the
/// abort verb closes on itself. Two sources (1, 3) freeze into one target (2); 2 aborts 1 (release
/// valve, appended, UNAPPLIED) then tries to abort 3 while that first abort is in flight. Both mint
/// `target_gen_after` from the SAME live `shape_gen`; RED before the fence: the second abort ADMITTED,
/// then draining 2 applied 1's abort (bumping the counter, recording `abandoned[1]`) and stale-no-oped
/// 3's abort at the strict apply-time guard WITHOUT recording `abandoned[3]` — 3 left frozen_for 2
/// forever, owed a thaw no obligation names. GREEN: the second abort defers `RollbackInFlight` until
/// 1's applies; re-proposed against the post-abort lineage it records its OWN `abandoned[3]` and 3
/// thaws. (The abort of the SAME merge as an in-flight commit is deliberately RACED, not fenced — see
/// `rollback_merge_races_an_in_flight_commit_of_the_same_merge`.)
#[test]
fn rollback_merge_defers_a_target_with_a_fanin_abort_in_flight() {
  let (mut m, mut stores) = merge_host(2, 3);
  let now = Instant::ORIGIN;
  // A second single-voter source (3), colocated, elected and drained.
  stores
    .0
    .insert(3, (VecLog::default(), AsyncStable::default()));
  m.create_group(3, 0, single_node_cfg(1), now, 7, CountSm::default())
    .unwrap();
  {
    let (log, stable) = stores.0.get_mut(&3).unwrap();
    let d = m.group(&3).unwrap().poll_timeout().unwrap();
    m.handle_timeout(&3, d, log, stable).unwrap();
    drain_storage(&mut m, 3, d, log, stable);
    assert!(m.group(&3).unwrap().role().is_leader());
  }
  while m.poll_message().is_some() {}
  while m.poll_event().is_some() {}

  // Both sources freeze toward target 2 — the concurrent fan-in.
  for src in [1u64, 3u64] {
    m.prepare_merge(&src, now, &mut stores, &2)
      .unwrap()
      .unwrap();
    let (log, stable) = stores.0.get_mut(&src).unwrap();
    drain_storage(&mut m, src, now, log, stable);
    assert!(m.group(&src).unwrap().is_frozen(), "source {src} froze");
  }

  // Abort 1's freeze — append but DO NOT drain: the abort is in flight on 2's log, its mint live.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.rollback_merge(&2, now, log, stable, &1).unwrap().unwrap();
  }
  assert!(
    m.group(&2).unwrap().rollback_in_flight(),
    "1's abort is in flight, unapplied"
  );

  // THE FENCE: aborting a DIFFERENT frozen source while the first abort is in flight defers (RED: it
  // admitted, minted the SAME gen as 1's abort, and stale-no-oped on apply WITHOUT recording
  // abandoned[3] — 3 stranded frozen forever).
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    assert_eq!(
      m.rollback_merge(&2, now, log, stable, &3),
      Some(Err(MergeError::RollbackInFlight)),
      "a second fan-in abort defers while the first is in flight"
    );
  }
  assert!(m.group(&3).unwrap().is_frozen(), "3 is untouched");
  assert!(
    m.group(&2).unwrap().abandoned_obligations().is_empty(),
    "neither abort has applied yet — no obligation recorded (3 not stale-no-oped into a strand)"
  );

  // Let 1's abort apply: it records abandoned[1] and clears the fence.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    !m.group(&2).unwrap().rollback_in_flight(),
    "1's abort applied"
  );
  assert_eq!(
    m.group(&2).unwrap().abandoned_obligations().len(),
    1,
    "only 1's obligation recorded so far"
  );

  // The same abort now admits against the post-abort lineage and records 3's OWN obligation.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.rollback_merge(&2, now, log, stable, &3).unwrap().unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert_eq!(
    m.group(&2).unwrap().abandoned_obligations().len(),
    2,
    "3's abort recorded its own obligation once serialized after 1's — RED stranded 3 with none"
  );

  // The service drives BOTH source thaws; draining each commits+applies its unfreeze.
  m.service_merge_applies(now, &mut stores);
  for src in [1u64, 3u64] {
    let (log, stable) = stores.0.get_mut(&src).unwrap();
    drain_storage(&mut m, src, now, log, stable);
  }
  assert!(!m.group(&1).unwrap().is_frozen(), "source 1 thawed");
  assert!(
    !m.group(&3).unwrap().is_frozen(),
    "source 3 thawed — not stranded by a stale abort"
  );
  // The observing leader defers each obligation's clear to a WITNESS, minting ONE at a time (the
  // in-flight guard serializes the fan-in); a few service+apply cycles discharge both.
  for _ in 0..4 {
    m.service_merge_applies(now, &mut stores);
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    !m.group(&2).unwrap().has_abandoned(),
    "both obligations discharged on the observed advances"
  );
}

/// The `rollback_merge` analogue of `commit_merge_defers_a_target_reshaping_by_a_split`. A `Split`
/// appended-and-unapplied on the target bumps its `shape_gen` when it drains, so an abort proposed
/// over it mints a generation the split immediately stales — the abort no-ops at its strict apply-time
/// guard and records NO `abandoned` obligation, leaving the frozen source owed a thaw nothing names.
/// RED before the gate: `rollback_merge` ADMITTED mid-split. GREEN: refused `SplitInFlight`; once the
/// split applies the same abort mints from the post-split counter and records the obligation.
#[test]
fn rollback_merge_defers_a_target_reshaping_by_a_split() {
  let (mut m, mut stores) = merge_host_with(SplitSm::default(), 1, SplitSm::default(), 3);
  let now = Instant::ORIGIN;

  // Freeze source 1 into target 2.
  {
    m.prepare_merge(&1, now, &mut stores, &2).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    m.group(&1).unwrap().is_frozen(),
    "source frozen for the target"
  );

  // Append a Split on the TARGET without draining it: split_in_flight is armed, unapplied.
  let split_idx = {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.propose_split(&2, now, log, stable, &3, 0, Bytes::from_static(b"\x01"))
      .unwrap()
      .unwrap()
  };
  assert!(
    m.group(&2).unwrap().split_in_flight(),
    "the target has a split appended-unapplied"
  );

  // THE FENCE: the abort defers while the target is reshaping (RED: it ADMITTED, then the drained
  // split staled its mint into an obligation-less no-op that strands the frozen source).
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    assert_eq!(
      m.rollback_merge(&2, now, log, stable, &1),
      Some(Err(MergeError::SplitInFlight)),
      "a target mid-split defers the abort"
    );
  }
  assert!(m.group(&1).unwrap().is_frozen(), "the source is untouched");
  assert!(
    !m.group(&2).unwrap().has_abandoned(),
    "no obligation-less abort was recorded on the target"
  );

  // Let the split resolve: it applies, `shape_gen` bumps, the fork relays out, the barrier lifts.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.flush_appends(&2, now, log, stable).unwrap();
    while matches!(
      m.handle_storage(&2, now, log, stable),
      Some(StorageProgress::MorePending)
    ) {}
  }
  assert!(!m.group(&2).unwrap().split_in_flight(), "the split applied");
  let fork = m.poll_pending_fork().expect("the fork relays out");
  assert_eq!(fork.child, 3);
  m.lift_fork_barrier(&2, split_idx);
  while m.poll_event().is_some() {}

  // Re-propose against the post-split lineage: now it admits and records the obligation.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.rollback_merge(&2, now, log, stable, &1).unwrap().unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    m.group(&2).unwrap().has_abandoned(),
    "the abort recorded its obligation post-split — the source is never stranded"
  );
}

/// THE INTENTIONAL RACE that MUST SURVIVE the fences above (#22). A `CommitMerge` of the SAME merge
/// parked (in flight) on the target must NOT block its abort: the abort is the release valve for a
/// park that can never complete, so fencing `commit_merge_in_flight` here would deadlock it. The abort
/// mints from the SAME base as the parked commit and the target's own log totally-orders the two — the
/// abort lands at the coordinate right after the park and un-parks every replica ABORTED. The pin: the
/// abort is ADMITTED (`Ok`), never refused `RollbackInFlight`/`AlreadyPending`. (End-to-end resolution
/// is `rollback_races_commit`.)
#[test]
fn rollback_merge_races_an_in_flight_commit_of_the_same_merge() {
  let (mut m, mut stores) = merge_host(2, 3);
  let now = Instant::ORIGIN;
  let k = freeze_and_park(&mut m, &mut stores);
  assert!(
    m.group(&2).unwrap().commit_merge_in_flight(),
    "the SAME merge's commit is parked, in flight"
  );
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    assert_eq!(
      m.rollback_merge(&2, now, log, stable, &1),
      Some(Ok(k.next())),
      "the abort races the parked commit at the next coordinate — not fenced by the in-flight commit"
    );
  }
}

/// A concrete, `core::error::Error` snapshot failure for [`SnapFailSm`].
#[derive(Debug)]
struct SnapErr;

impl core::fmt::Display for SnapErr {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.write_str("snapshot capture failed")
  }
}

impl core::error::Error for SnapErr {}

/// A counting state machine whose `snapshot()` fails once its shared flag is armed — the
/// absorb-SUCCEEDED-but-capture-FAILED shape. `absorb` always folds and returns `true`; arming the
/// flag makes the forced absorb capture's `snapshot()` error, so `pending_compact` can never stage.
/// The flag is shared so a test arms it AFTER the merge parks, leaving the drive's own snapshots
/// (if any) succeeding and failing only the resolve arm's forced capture.
#[derive(Debug, Default)]
struct SnapFailSm {
  count: u64,
  fail: std::sync::Arc<core::sync::atomic::AtomicBool>,
}

impl crate::StateMachine for SnapFailSm {
  type Command = Bytes;
  type Response = u64;
  type Snapshot = u64;
  type Error = SnapErr;

  fn apply(&mut self, _index: Index, _cmd: Bytes) -> Result<u64, Self::Error> {
    self.count += 1;
    Ok(self.count)
  }

  fn snapshot(&self) -> Result<u64, Self::Error> {
    if self.fail.load(core::sync::atomic::Ordering::Relaxed) {
      Err(SnapErr)
    } else {
      Ok(self.count)
    }
  }

  fn restore(&mut self, snapshot: u64) -> Result<(), Self::Error> {
    self.count = snapshot;
    Ok(())
  }

  fn absorb(&mut self, source: Self) -> bool {
    self.count += source.count;
    true
  }
}

/// The absorb SUCCEEDS but the forced durable capture FAILS (`snapshot()` errors): the resolve arm
/// must NOT surface `Merged` — the driver's permission to floor the source terminally and drop its
/// stores — over a union no durable target snapshot covers. Without the guard the source would be
/// floored and torn down with no absorbed anchor, and a restart would find neither a re-absorbable
/// source nor a durable absorbed target (data loss). The target fail-stops instead; the source's
/// stores stay untouched so a restart re-parks against them.
#[test]
fn capture_failure_withholds_merged_and_keeps_the_source_recoverable() {
  let fail = std::sync::Arc::new(core::sync::atomic::AtomicBool::new(false));
  let (mut m, mut stores) = merge_host_with(
    SnapFailSm::default(),
    2,
    SnapFailSm {
      count: 0,
      fail: fail.clone(),
    },
    3,
  );
  let _k = freeze_and_park(&mut m, &mut stores);
  seal_window(&mut m, &mut stores);
  // Arm the target's forced capture to fail: the absorb still folds, but snapshot() errors, so no
  // durable anchor for the union can ever stage.
  fail.store(true, core::sync::atomic::Ordering::Relaxed);
  let resolutions = m.service_merge_applies(Instant::ORIGIN, &mut stores);
  assert!(
    resolutions.is_empty(),
    "a failed absorb capture must not surface a Merged teardown resolution: {resolutions:?}"
  );
  let tep = m.group(&2).unwrap();
  assert!(
    tep.is_poisoned(),
    "the failed capture fail-stops the target rather than advertising a phantom merge"
  );
  assert_eq!(tep.poison_reason(), Some(PoisonReason::SnapshotCapture));
  // The source stays recoverable: its stores are untouched and its id was never floored, so a
  // restart re-parks against the restored source and the merge re-resolves.
  assert!(stores.0.contains_key(&1), "the source's stores are intact");
  assert_eq!(stores.floor(&1), 0, "the source id was never floored");
}

/// The negative pin: with the forced capture SUCCEEDING, the resolve arm still emits exactly one
/// `Merged` and stages the durable anchor — the withholding fires ONLY on a failed capture.
#[test]
fn capture_success_still_emits_merged_once() {
  let (mut m, mut stores) = merge_host_with(SnapFailSm::default(), 2, SnapFailSm::default(), 3);
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
  assert!(!m.group(&2).unwrap().is_poisoned());
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, Instant::ORIGIN, log, stable);
    assert!(stable.snapshot().is_some(), "absorb capture persisted");
    assert!(log.first_index() > k, "compacted through the absorb");
  }
  assert!(
    m.service_merge_applies(Instant::ORIGIN, &mut stores)
      .is_empty(),
    "the resolution fires exactly once"
  );
}

/// The abort races the parked commit ON THE TARGET'S OWN LOG and lands at the coordinate right
/// after it: every replica's park un-parks ABORTED off that one committed coordinate — never
/// off observation timing of the source's mutable state (the proven cross-log divergence). The
/// drain then applies the abort itself (the target's lineage bumps — the guard that kills any
/// same-base commit; the durable `abandoned` obligation is recorded), and the per-crank service
/// DERIVES the source thaw from it — no volatile relay. Never a half-merge.
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
  // The resumed drain applies the abort entry: lineage bump + the durable `abandoned` record.
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
  assert_eq!(
    tep.abandoned_obligations().first().map(|(_, g, _)| *g),
    Some(1),
    "the target recorded exactly the abandoned freeze generation"
  );
  let mut aborted = false;
  while let Some((gid, ev)) = m.poll_event() {
    aborted |= gid == 2 && matches!(ev, Event::MergeAborted(_));
  }
  assert!(aborted, "Event::MergeAborted surfaced");
  // The per-crank service DRIVES the source thaw from the target's durable `abandoned` — it appends
  // the source-side RollbackMerge on the source's OWN log; draining commits+applies it.
  m.service_merge_applies(Instant::ORIGIN, &mut stores);
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, Instant::ORIGIN, log, stable);
  }
  let sep = m.group(&1).unwrap();
  assert!(
    !sep.is_frozen(),
    "the service-driven thaw unfroze the source"
  );
  assert_eq!(sep.shape_gen(), 2, "0 -> 1 (freeze) -> 2 (thaw)");
  // The next crank OBSERVES the source advanced past the abandoned freeze and DISCHARGES the
  // obligation — the discharge-gated durability release.
  m.service_merge_applies(Instant::ORIGIN, &mut stores);
  // The observing leader deferred its clear to the witness — apply it on the holder.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, Instant::ORIGIN, log, stable);
  }
  assert!(
    !m.group(&2).unwrap().has_abandoned(),
    "the observed source advance discharged the obligation"
  );
}

/// FIX 2 leg (a): `commit_merge` refuses `TargetOwesThaw` when the target already owes THIS source
/// incarnation an aborted-merge thaw — the same merge's abort applied, the source still frozen at
/// the aborted generation, its thaw not yet discharged. Re-parking there would wedge on the freeze
/// generation the thaw pass drives past. GENERATION-EXACT: once the thaw discharges and the source
/// re-freezes fresh, the same target admits the new commit. RED before the gate: the re-propose
/// ADMITS and the resulting park wedges.
#[test]
fn commit_merge_refuses_a_target_owing_this_source_a_thaw() {
  let (mut m, mut stores) = merge_host(2, 3);
  let now = Instant::ORIGIN;
  // 1 freezes into 2 (1 frozen at gen 1, claim = 2).
  {
    m.prepare_merge(&1, now, &mut stores, &2).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(m.group(&1).unwrap().is_frozen());
  // 2 aborts the 1 -> 2 merge and APPLIES it: 2 now owes 1 a thaw at freeze generation 1, and 1 is
  // still frozen at gen 1 (its relayed thaw has not been driven).
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.rollback_merge(&2, now, log, stable, &1).unwrap().unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(m.group(&2).unwrap().has_abandoned(), "2 owes 1 a thaw");
  assert!(
    m.group(&1).unwrap().is_frozen(),
    "1 is still frozen at gen 1"
  );
  // The re-propose is refused GEN-EXACT — parking here would wedge on the freeze generation the
  // thaw pass drives past.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    assert_eq!(
      m.commit_merge(&2, now, log, stable, &1),
      Some(Err(MergeError::TargetOwesThaw)),
      "a target owing this source incarnation a thaw refuses the re-commit"
    );
  }
  // Drive the thaw: 1 unfreezes, then the observed advance discharges 2's obligation.
  m.service_merge_applies(now, &mut stores);
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(!m.group(&1).unwrap().is_frozen(), "the thaw unfroze 1");
  m.service_merge_applies(now, &mut stores);
  // The observing leader deferred its clear to the witness — apply it on the holder.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    !m.group(&2).unwrap().has_abandoned(),
    "the obligation discharged"
  );
  // GEN-EXACTNESS: 1 re-freezes FRESH (a strictly higher generation) and the same target now ADMITS
  // — the spent obligation named a dead incarnation, so its discharge cleared the refusal.
  {
    m.prepare_merge(&1, now, &mut stores, &2).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    assert!(
      matches!(m.commit_merge(&2, now, log, stable, &1), Some(Ok(_))),
      "the fresh freeze admits — generation-exact"
    );
  }
}

/// FIX 2 leg (b): the apply-time belt for the in-flight order the gate cannot see — a `CommitMerge`
/// with a FRESH mint appended ABOVE the same merge's already-committed abort. The lineage guard
/// admits the fresh mint, so without the belt it would PARK at the aborted freeze generation and
/// wedge once the thaw pass drives the source past it. The belt reads `abandoned` at apply and
/// aborts the dead commit instead: no park, no lineage bump, `MergeAborted` surfaced, drain resumes.
/// RED before the belt: the fresh-mint commit parks and the drain wedges below it.
#[test]
fn a_committed_abort_below_a_fresh_commit_kills_it_at_apply() {
  let (mut m, mut stores) = merge_host(2, 3);
  let now = Instant::ORIGIN;
  // 1 freezes into 2 (1 frozen at gen 1, claim = 2) — the real source the dead commit names.
  {
    m.prepare_merge(&1, now, &mut stores, &2).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  let mut source_bytes = Vec::new();
  Data::encode(&1u64, &mut source_bytes);
  let source_bytes = Bytes::from(source_bytes);
  // Append directly on target 2, bypassing the propose gate to reproduce the in-flight order the
  // gate cannot see: the same merge's abort BELOW, then a FRESH-mint commit ABOVE, then drain both.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    // Target-role abort at the live mint (target_gen_after = 1 against base 0): its apply records
    // abandoned[1] = freeze generation 1.
    let abort = crate::RollbackMergePayload::abort(source_bytes.clone(), 1, 1);
    let mut abuf = Vec::new();
    crate::wire::encode_rollback_merge_payload(&abort, &mut abuf);
    let a = m
      .group_mut(&2)
      .unwrap()
      .propose_merge_entry(now, log, crate::EntryKind::RollbackMerge, Bytes::from(abuf))
      .unwrap();
    // A FRESH-mint commit for the SAME (source, freeze generation): target_gen_after = 2, one past
    // the abort's own bump, so the lineage guard ADMITS it — only the belt can kill it.
    let commit =
      crate::CommitMergePayload::new(source_bytes.clone(), Index::new(2), Term::new(1), 1, 2);
    let mut cbuf = Vec::new();
    crate::wire::encode_commit_merge_payload(&commit, &mut cbuf);
    let k = m
      .group_mut(&2)
      .unwrap()
      .propose_merge_entry(now, log, crate::EntryKind::CommitMerge, Bytes::from(cbuf))
      .unwrap();
    assert_eq!(k, a.next(), "the commit sits directly above the abort");
    drain_storage(&mut m, 2, now, log, stable);
  }
  let tep = m.group(&2).unwrap();
  assert!(tep.pending_merge().is_none(), "the dead commit never parks");
  assert_eq!(
    tep.shape_gen(),
    1,
    "only the abort's bump — the dead commit does not move the lineage"
  );
  assert!(m.contains_group(&1), "the source is not absorbed");
  let mut aborted = false;
  while let Some((gid, ev)) = m.poll_event() {
    aborted |= gid == 2 && matches!(ev, Event::MergeAborted(_));
  }
  assert!(aborted, "the dead commit surfaced MergeAborted");
  // NOT WEDGED: the drain ran straight through the belt-aborted commit, so a fresh proposal on 2
  // applies (a park would have stopped the drain below it forever).
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    let before = m.group(&2).unwrap().applied_index();
    m.propose(&2, now, log, stable, &Bytes::from_static(b"z"))
      .unwrap()
      .unwrap();
    drain_storage(&mut m, 2, now, log, stable);
    assert!(
      m.group(&2).unwrap().applied_index() > before,
      "the drain is not wedged at a park"
    );
  }
}

/// A target legitimately absorbs a FAN-IN of sources, so its abort obligations are a per-source
/// COLLECTION. Two sources frozen toward one target (the second from the window BEFORE the first
/// abort applied) each record their OWN obligation when aborted, and BOTH thaw. RED with the old
/// single-slot keep-first record: the second abort silently DROPPED the first source's obligation,
/// stranding it frozen forever. This also retires the old `prepare_merge` one-abort-at-a-time freeze
/// guard, which forbade this supported shape — and was insufficient anyway, since a source frozen
/// before the first abort applied slipped past it entirely.
#[test]
fn both_fanned_in_aborts_thaw_neither_dropped() {
  let (mut m, mut stores) = merge_host(2, 3);
  let now = Instant::ORIGIN;
  // A second single-voter source (3), colocated with 1 and 2, elected and drained.
  stores
    .0
    .insert(3, (VecLog::default(), AsyncStable::default()));
  m.create_group(3, 0, single_node_cfg(1), now, 7, CountSm::default())
    .unwrap();
  {
    let (log, stable) = stores.0.get_mut(&3).unwrap();
    let d = m.group(&3).unwrap().poll_timeout().unwrap();
    m.handle_timeout(&3, d, log, stable).unwrap();
    drain_storage(&mut m, 3, d, log, stable);
    assert!(m.group(&3).unwrap().role().is_leader());
  }
  while m.poll_message().is_some() {}
  while m.poll_event().is_some() {}

  // BOTH sources freeze toward target 2 — the concurrent fan-in (neither abort has applied yet, so
  // the retired freeze guard could not have caught the second one regardless).
  for src in [1u64, 3u64] {
    m.prepare_merge(&src, now, &mut stores, &2)
      .unwrap()
      .unwrap();
    let (log, stable) = stores.0.get_mut(&src).unwrap();
    drain_storage(&mut m, src, now, log, stable);
    assert!(m.group(&src).unwrap().is_frozen(), "source {src} froze");
  }

  // Target 2 aborts BOTH merges (draining between so each abort's mint is live). The SECOND abort
  // must not drop the FIRST source's obligation.
  for src in [1u64, 3u64] {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.rollback_merge(&2, now, log, stable, &src)
      .unwrap()
      .unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert_eq!(
    m.group(&2).unwrap().abandoned_obligations().len(),
    2,
    "both fanned-in aborts recorded their own obligation — RED (keep-first) kept only one"
  );

  // The service drives BOTH source thaws; draining each commits+applies its unfreeze.
  m.service_merge_applies(now, &mut stores);
  for src in [1u64, 3u64] {
    let (log, stable) = stores.0.get_mut(&src).unwrap();
    drain_storage(&mut m, src, now, log, stable);
  }
  assert!(!m.group(&1).unwrap().is_frozen(), "source 1 thawed");
  assert!(
    !m.group(&3).unwrap().is_frozen(),
    "source 3 thawed too — the second obligation was NOT silently dropped"
  );
  // The observing leader defers each obligation's clear to a WITNESS, minting ONE at a time (the
  // in-flight guard serializes the fan-in); a few service+apply cycles discharge both.
  for _ in 0..4 {
    m.service_merge_applies(now, &mut stores);
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    !m.group(&2).unwrap().has_abandoned(),
    "both obligations discharged on the observed advances"
  );
}

/// A target legitimately fans in TWO sources (1, 3), then COMMITS source 1 — parking it. A
/// concurrent abort of the OTHER source (3) MUST defer `AlreadyPending`: admitting it would land
/// source 3's abort at source 1's `k+1`, where `merge_abort_window` reads a different-source
/// rollback as `Closed`. Source 1 would then absorb and bump the lineage, and source 3's abort would
/// stale-no-op WITHOUT recording `abandoned[3]` — source 3 stranded frozen forever, and source 1's
/// release valve consumed. The fence is SAME-MERGE-EXACT (racing THIS merge's own park is #22's
/// purpose) and self-clearing: once source 1's park resolves (`Merged`, lineage bumped, park gone),
/// the SAME source-3 abort admits, records `abandoned[3]`, and the service thaws source 3. RED with
/// the cross-source arms neutered: the abort ADMITS `Some(Ok(_))` and source 3 strands.
#[test]
fn rollback_merge_defers_a_target_committing_a_different_source() {
  let (mut m, mut stores) = merge_host(2, 3);
  let now = Instant::ORIGIN;
  // A second single-voter source (3), colocated with 1 and 2, elected and drained.
  stores
    .0
    .insert(3, (VecLog::default(), AsyncStable::default()));
  m.create_group(3, 0, single_node_cfg(1), now, 7, CountSm::default())
    .unwrap();
  {
    let (log, stable) = stores.0.get_mut(&3).unwrap();
    let d = m.group(&3).unwrap().poll_timeout().unwrap();
    m.handle_timeout(&3, d, log, stable).unwrap();
    drain_storage(&mut m, 3, d, log, stable);
    assert!(m.group(&3).unwrap().role().is_leader());
  }
  while m.poll_message().is_some() {}
  while m.poll_event().is_some() {}

  // BOTH sources freeze toward target 2 — the fan-in.
  for src in [1u64, 3u64] {
    m.prepare_merge(&src, now, &mut stores, &2)
      .unwrap()
      .unwrap();
    let (log, stable) = stores.0.get_mut(&src).unwrap();
    drain_storage(&mut m, src, now, log, stable);
    assert!(m.group(&src).unwrap().is_frozen(), "source {src} froze");
  }

  // COMMIT source 1 into 2 and PARK it — do NOT resolve.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.commit_merge(&2, now, log, stable, &1).unwrap().unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    m.group(&2).unwrap().pending_merge().is_some(),
    "source 1's commit is parked"
  );
  assert!(
    m.group(&2).unwrap().commit_merge_in_flight(),
    "the parked commit is still in flight (applied held at k-1)"
  );

  // RED-PROOF: the CROSS-source abort (source 3) defers to source 1's parked commit.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    assert_eq!(
      m.rollback_merge(&2, now, log, stable, &3),
      Some(Err(MergeError::AlreadyPending)),
      "a cross-source abort must NOT race a parked commit of a DIFFERENT source"
    );
  }
  // The fence appended nothing: source 1's park stands and source 3 stays frozen.
  assert!(m.group(&2).unwrap().pending_merge().is_some());
  assert!(m.group(&3).unwrap().is_frozen(), "source 3 stayed frozen");

  // SELF-CLEARING: resolve source 1's parked commit (seal the window, then absorb).
  seal_window(&mut m, &mut stores);
  let resolutions = m.service_merge_applies(now, &mut stores);
  assert_eq!(
    resolutions,
    std::vec![MergeResolution::Merged {
      source: 1,
      target: 2
    }],
    "source 1 absorbed"
  );
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    m.group(&2).unwrap().pending_merge().is_none(),
    "the park cleared"
  );
  assert!(!m.group(&2).unwrap().commit_merge_in_flight());

  // The SAME source-3 abort now ADMITS off the bumped live lineage.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    let admitted = m.rollback_merge(&2, now, log, stable, &3);
    assert!(
      matches!(admitted, Some(Ok(_))),
      "with source 1's park resolved the source-3 abort admits: {admitted:?}"
    );
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert_eq!(
    m.group(&2).unwrap().abandoned_obligations().len(),
    1,
    "the admitted abort recorded source 3's thaw obligation"
  );

  // The service drives source 3's thaw; draining commits+applies its unfreeze.
  m.service_merge_applies(now, &mut stores);
  {
    let (log, stable) = stores.0.get_mut(&3).unwrap();
    drain_storage(&mut m, 3, now, log, stable);
  }
  assert!(
    !m.group(&3).unwrap().is_frozen(),
    "the service-driven thaw unfroze source 3 — never stranded"
  );
}

/// The IN-FLIGHT-UNPARKED twin of [`rollback_merge_defers_a_target_committing_a_different_source`]:
/// the cross-source fence must discriminate BEFORE the commit parks, when there is no in-memory
/// `pending_merge` to compare. A target appends a `CommitMerge` of source 1 but does NOT drain it —
/// `commit_merge_in_flight()` is true, `pending_merge()` is still `None` — so the fence DECODES the
/// in-flight commit's source off the log at `pending_commit_index`. Both directions of that decode
/// are pinned here:
///
/// - A concurrent abort of the OTHER source (3) sees `1 != 3` and DEFERS `AlreadyPending`; nothing
///   is appended and source 3 stays frozen. Admitting it would land source 3's abort at source 1's
///   `k + 1` and strand source 3, exactly as the parked case does.
/// - An abort of the SAME source (1) sees `1 == 1` and RACES (`Ok`) — the #22 release valve holds
///   in the pre-park window too. THIS is the assertion the coarse `commit_merge_in_flight` defer
///   this fix replaced got wrong (it deferred every unparked abort, same-source included); the sim
///   band pins the same race end-to-end. RED if the in-flight arm blanket-admits (source 3 strands)
///   or blanket-defers (source 1 cannot release its own stuck commit).
#[test]
fn rollback_merge_source_discriminates_an_in_flight_unparked_commit() {
  let (mut m, mut stores) = merge_host(2, 3);
  let now = Instant::ORIGIN;
  // A second single-voter source (3), colocated with 1 and 2, elected and drained.
  stores
    .0
    .insert(3, (VecLog::default(), AsyncStable::default()));
  m.create_group(3, 0, single_node_cfg(1), now, 7, CountSm::default())
    .unwrap();
  {
    let (log, stable) = stores.0.get_mut(&3).unwrap();
    let d = m.group(&3).unwrap().poll_timeout().unwrap();
    m.handle_timeout(&3, d, log, stable).unwrap();
    drain_storage(&mut m, 3, d, log, stable);
    assert!(m.group(&3).unwrap().role().is_leader());
  }
  while m.poll_message().is_some() {}
  while m.poll_event().is_some() {}

  // BOTH sources freeze toward target 2 — the fan-in.
  for src in [1u64, 3u64] {
    m.prepare_merge(&src, now, &mut stores, &2)
      .unwrap()
      .unwrap();
    let (log, stable) = stores.0.get_mut(&src).unwrap();
    drain_storage(&mut m, src, now, log, stable);
    assert!(m.group(&src).unwrap().is_frozen(), "source {src} froze");
  }

  // COMMIT source 1 into 2 — APPEND ONLY, no drain: the commit stays in flight and UNPARKED.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.commit_merge(&2, now, log, stable, &1).unwrap().unwrap();
  }
  assert!(
    m.group(&2).unwrap().commit_merge_in_flight(),
    "source 1's commit is in flight"
  );
  assert!(
    m.group(&2).unwrap().pending_merge().is_none(),
    "and NOT yet parked — only the log decode, not `pending_merge`, can name its source"
  );

  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    // CROSS-source abort (3): decoded `1 != 3` → defer, append nothing.
    assert_eq!(
      m.rollback_merge(&2, now, log, stable, &3),
      Some(Err(MergeError::AlreadyPending)),
      "a cross-source abort must not race an IN-FLIGHT commit of a DIFFERENT source"
    );
    // SAME-source abort (1): decoded `1 == 1` → race the in-flight commit's release valve.
    assert!(
      matches!(m.rollback_merge(&2, now, log, stable, &1), Some(Ok(_))),
      "the SAME-source abort races the in-flight commit — the #22 valve, pre-park"
    );
  }
  assert!(
    m.group(&3).unwrap().is_frozen(),
    "the deferred cross-source abort left source 3 frozen"
  );
}

/// A host with three colocated single-voter [`CountSm`] groups (1, 2, 3), each with the given
/// applied-command count, elected and fully drained — the substrate for the obligation-holder
/// lifecycle, where group 2 plays a merge TARGET (of 1) that later tries to become a merge SOURCE
/// (into 3).
fn merge_host_triple(c1: usize, c2: usize, c3: usize) -> (MultiRaft<u64, u64, CountSm>, MapStores) {
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let mut stores = MapStores(
    std::collections::BTreeMap::new(),
    std::collections::BTreeSet::new(),
  );
  for (gid, n) in [(1u64, c1), (2u64, c2), (3u64, c3)] {
    stores
      .0
      .insert(gid, (VecLog::default(), AsyncStable::default()));
    m.create_group(
      gid,
      0,
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

/// LEG alpha (liveness, the obligation-holder lifecycle): a group that owes an aborted upstream
/// source its thaw must NOT dissolve as a fresh merge's SOURCE. Group 2 is the TARGET of a 1 -> 2
/// merge that 2 aborts (recording `abandoned[1]`); that thaw is still undischarged. RED current:
/// `prepare_merge(2 -> 3)` ADMITS, 2 later dissolves into 3, and its `abandoned[1]` vanishes with
/// the endpoint — 1 is stranded frozen forever. GREEN: the freeze is refused `SourceOwesThaw`, and
/// once the thaw pass discharges 1 the SAME freeze admits (the self-clearing pin).
#[test]
fn a_source_owing_a_thaw_cannot_freeze_as_a_source() {
  let (mut m, mut stores) = merge_host_triple(2, 3, 4);
  let now = Instant::ORIGIN;
  // 1 freezes into 2, then 2 aborts the merge and APPLIES it: 2 now owes 1 a thaw.
  {
    m.prepare_merge(&1, now, &mut stores, &2).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(m.group(&1).unwrap().is_frozen(), "source 1 froze into 2");
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.rollback_merge(&2, now, log, stable, &1).unwrap().unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    m.group(&2).unwrap().has_abandoned(),
    "2 recorded its target-role abort obligation for 1"
  );

  // The gate: 2 cannot freeze as a source while it still owes 1 a thaw.
  {
    assert!(
      matches!(
        m.prepare_merge(&2, now, &mut stores, &3),
        Some(Err(MergeError::SourceOwesThaw))
      ),
      "a source owing a thaw is refused SourceOwesThaw — never admitted to dissolve"
    );
  }
  assert!(
    !m.group(&2).unwrap().merge_freeze_active(),
    "the refusal appended nothing"
  );

  // Drive the thaw pass: it unfreezes 1 from 2's obligation; draining 1 commits the thaw and 1
  // advances past its freeze generation; the next pass discharges the obligation.
  m.service_merge_applies(now, &mut stores);
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(!m.group(&1).unwrap().is_frozen(), "the thaw unfroze 1");
  m.service_merge_applies(now, &mut stores);
  // The observing leader deferred its clear to the witness — apply it on the holder.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    !m.group(&2).unwrap().has_abandoned(),
    "the observed advance discharged 2's obligation"
  );

  // SELF-CLEARING PIN: with the obligation discharged, the SAME freeze now admits.
  {
    assert!(
      m.prepare_merge(&2, now, &mut stores, &3).unwrap().is_ok(),
      "once the thaw discharges, 2 freezes into 3 exactly as a clean source would"
    );
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(m.group(&2).unwrap().is_frozen(), "2 froze into 3");
}

/// The EXPLICIT-TEARDOWN door twin of `a_source_owing_a_thaw_cannot_freeze_as_a_source`: the public
/// `remove_group` refuses a hosted group that still owes an aborted upstream source its thaw. Group
/// 2 is the TARGET of a 1 -> 2 merge that 2 aborts (recording `abandoned[1]`), still undischarged.
/// RED current: `remove_group(&2)` SUCCEEDS, 2's endpoint + stores drop, and 1 is stranded frozen
/// forever with no holder left to run the thaw pass. GREEN: the removal is REFUSED `OwesThaw`,
/// tearing nothing down; once the thaw pass discharges 1 the SAME `remove_group(&2)` admits (the
/// self-clearing pin). Removing a group that owes nothing (3) is unchanged — `Ok(Some(endpoint))`.
#[test]
fn teardown_refuses_a_group_that_still_owes_a_thaw() {
  let (mut m, mut stores) = merge_host_triple(2, 3, 4);
  let now = Instant::ORIGIN;
  // 1 freezes into 2, then 2 aborts the merge and APPLIES it: 2 now owes 1 a thaw.
  {
    m.prepare_merge(&1, now, &mut stores, &2).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(m.group(&1).unwrap().is_frozen(), "source 1 froze into 2");
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.rollback_merge(&2, now, log, stable, &1).unwrap().unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    m.group(&2).unwrap().has_abandoned(),
    "2 recorded its target-role abort obligation for 1"
  );

  // NEGATIVE PIN: removing a group that owes nothing is unchanged — Ok(Some(endpoint)).
  assert!(
    m.remove_group(&3, &mut stores).unwrap().is_some(),
    "a group with no thaw obligation tears down exactly as before"
  );

  // THE GATE: 2 cannot be torn down while it still owes 1 a thaw.
  assert!(
    matches!(m.remove_group(&2, &mut stores), Err(RemoveError::OwesThaw)),
    "a holder of an undischarged thaw is refused OwesThaw — never torn down"
  );
  // The refusal tore NOTHING down — the strand the RED path would have created is absent: 2 is still
  // hosted with its obligation, and 1 is still frozen but still HAS a holder (2) to run the thaw.
  assert!(m.contains_group(&2), "the refused removal left 2 hosted");
  assert!(
    m.group(&2).unwrap().has_abandoned(),
    "2 still owes 1 the thaw"
  );
  assert!(
    m.group(&1).unwrap().is_frozen(),
    "1 is still frozen, but 2 survives to thaw it"
  );

  // Drive the thaw pass: it unfreezes 1 from 2's obligation; the next pass discharges it.
  m.service_merge_applies(now, &mut stores);
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(!m.group(&1).unwrap().is_frozen(), "the thaw unfroze 1");
  m.service_merge_applies(now, &mut stores);
  // The observing leader deferred its clear to the witness — apply it on the holder.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    !m.group(&2).unwrap().has_abandoned(),
    "the observed advance discharged 2's obligation"
  );

  // SELF-CLEARING PIN: with the obligation discharged, the SAME removal now admits.
  assert!(
    m.remove_group(&2, &mut stores).unwrap().is_some(),
    "once the thaw discharges, 2 tears down exactly as a clean group would"
  );
  assert!(!m.contains_group(&2), "2 is gone");
}

/// The PUBLIC teardown gate refuses EVERY unresolved merge participant, not just a thaw-ower. Group
/// 1 freezes into 2 and 2 parks its `CommitMerge`: 1 is a frozen SOURCE, 2 a parked TARGET. RED
/// current: `remove_group(&1)` and `remove_group(&2)` BOTH succeed, each stranding the other half —
/// 1 torn out leaves 2's park with no source to absorb or abort against; 2 torn out leaves 1 frozen
/// with no decider. GREEN: 1 refuses `Frozen`, 2 refuses `MergeParked`, and neither refusal touches
/// a thing (the group stays hosted with its exact merge state). Once the merge resolves — here by
/// abort + thaw — the SAME removals admit (the self-clearing pin). A non-participant (3) is
/// byte-for-byte unchanged.
#[test]
fn teardown_refuses_a_frozen_source_and_a_parked_target() {
  let (mut m, mut stores) = merge_host_triple(2, 3, 4);
  let now = Instant::ORIGIN;
  let k = freeze_and_park(&mut m, &mut stores);

  // NEGATIVE PIN: a non-participant tears down exactly as before — Ok(Some(endpoint)).
  assert!(
    m.remove_group(&3, &mut stores).unwrap().is_some(),
    "a non-participant removal is byte-for-byte unchanged"
  );

  // THE GATE, leg 2: a frozen source cannot leave — its target's park resolves against this freeze.
  assert!(
    matches!(m.remove_group(&1, &mut stores), Err(RemoveError::Frozen)),
    "a frozen merge source is refused Frozen"
  );
  // THE GATE, leg 3: a parked target cannot leave — its frozen source needs the decider.
  assert!(
    matches!(
      m.remove_group(&2, &mut stores),
      Err(RemoveError::MergeParked)
    ),
    "a target parked on a commit is refused MergeParked"
  );
  // NO SIDE EFFECTS: both refusals left the choreography fully intact — the strand the RED path
  // would have created is absent.
  assert!(
    m.contains_group(&1) && m.group(&1).unwrap().is_frozen(),
    "1 is still a frozen source"
  );
  assert!(
    m.contains_group(&2) && m.group(&2).unwrap().pending_merge().is_some(),
    "2 is still parked on its commit"
  );
  assert!(
    !m.group(&2).unwrap().has_abandoned(),
    "no obligation was fabricated by the refusal"
  );

  // Resolve by ABORT + THAW: the abort lands at k+1 on 2's own log and un-parks it aborted; the
  // per-crank thaw pass then unfreezes 1 and discharges 2's obligation. Both become clean groups.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    let a = m.rollback_merge(&2, now, log, stable, &1).unwrap().unwrap();
    assert_eq!(a, k.next(), "the abort is the window's resolution input");
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert_eq!(
    m.service_merge_applies(now, &mut stores),
    std::vec![MergeResolution::Aborted {
      source: 1,
      target: 2
    }],
    "the park resolves aborted"
  );
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  m.service_merge_applies(now, &mut stores);
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(!m.group(&1).unwrap().is_frozen(), "the thaw unfroze 1");
  m.service_merge_applies(now, &mut stores);
  // The observing leader deferred its clear to the witness — apply it on the holder.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    !m.group(&2).unwrap().has_abandoned(),
    "the observed advance discharged 2's obligation"
  );

  // SELF-CLEARING PIN: with the merge resolved, the SAME two removals now admit.
  assert!(
    m.remove_group(&1, &mut stores).unwrap().is_some(),
    "the thawed former source tears down as a clean group"
  );
  assert!(
    m.remove_group(&2, &mut stores).unwrap().is_some(),
    "the resolved former target tears down as a clean group"
  );
}

/// The ONE deliberate teardown ESCAPE: an OWED source — a frozen source a hosted target already owes
/// a thaw — is removable, because the removal PURGE binds every holder's obligation to the departing
/// incarnation (and the driver floors the id — the catalog recovery for a genuinely-dead frozen
/// participant). Group 1 freezes into 2 and 2 ABORTS, recording `abandoned[1]` while 1 stays frozen.
/// `remove_group(&1)` ADMITS despite the active freeze — leg 2 steps aside for exactly this — and the
/// purge clears 2's now-danging obligation so no stale record can ever back a recreate's thaw.
#[test]
fn teardown_admits_an_owed_frozen_source_and_purges_the_obligation() {
  let (mut m, mut stores) = merge_host_triple(2, 3, 4);
  let now = Instant::ORIGIN;
  let k = freeze_and_park(&mut m, &mut stores);
  // 2 aborts the merge and APPLIES it: 2 now owes 1 a thaw, and 1 is STILL a frozen source.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    let a = m.rollback_merge(&2, now, log, stable, &1).unwrap().unwrap();
    assert_eq!(a, k.next());
    drain_storage(&mut m, 2, now, log, stable);
  }
  m.service_merge_applies(now, &mut stores);
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    m.group(&1).unwrap().is_frozen(),
    "1 is still a frozen source"
  );
  assert!(m.group(&2).unwrap().has_abandoned(), "2 owes 1 the thaw");

  // THE ESCAPE: removing the OWED source ADMITS even though it is frozen (leg 2 suppressed).
  assert!(
    m.remove_group(&1, &mut stores).unwrap().is_some(),
    "an owed frozen source is the designed catalog escape — the removal admits"
  );
  // The removal purge discharged the obligation the departed source can no longer thaw.
  assert!(
    !m.group(&2).unwrap().has_abandoned(),
    "the R2 purge cleared 2's obligation for the departed incarnation"
  );
}

/// The GENERATION-EXACT boundary of that escape: a STALE obligation must NOT bypass the frozen gate.
/// Source 1 freezes into 2 at gen 1 and 2 ABORTS (recording `abandoned[1]` for gen 1); the thaw pass
/// then DELIVERS — 1 unfreezes and advances to gen 2 — but the discharge pass is deliberately NOT
/// run, so 2's obligation lingers naming the now-SPENT gen 1. 1 is then re-frozen for a FRESH merge
/// into 3 (gen 3), whose target has not yet parked. RED current (id-only escape): `remove_group(&1)`
/// admits — the lingering `abandoned[1]` suppresses `Frozen` — tearing down the newly-frozen source
/// and stranding 3's forming park. GREEN: the escape is generation-exact (obligation gen 1 ≠ live gen
/// 3), so the stale record suppresses NOTHING and leg 2 refuses `Frozen`, leaving the merge intact.
#[test]
fn a_stale_obligation_does_not_bypass_the_frozen_gate() {
  let (mut m, mut stores) = merge_host_triple(2, 3, 4);
  let now = Instant::ORIGIN;
  // 1 freezes into 2 at gen 1, then 2 aborts and APPLIES: 2 owes 1 a gen-1 thaw, 1 is still frozen.
  {
    m.prepare_merge(&1, now, &mut stores, &2).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert_eq!(
    m.group(&1).unwrap().shape_gen(),
    1,
    "1 froze into 2 at gen 1"
  );
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.rollback_merge(&2, now, log, stable, &1).unwrap().unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    m.group(&2).unwrap().has_abandoned(),
    "2 owes 1 the gen-1 thaw"
  );

  // Drive the thaw so it DELIVERS — 1 unfreezes and advances to gen 2 — but do NOT run the discharge
  // pass, so 2's obligation lingers naming the now-SPENT gen 1.
  m.service_merge_applies(now, &mut stores);
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(!m.group(&1).unwrap().is_frozen(), "the thaw unfroze 1");
  assert_eq!(
    m.group(&1).unwrap().shape_gen(),
    2,
    "1 advanced past the spent freeze generation"
  );
  assert!(
    m.group(&2).unwrap().has_abandoned(),
    "the discharge pass has NOT run — 2 still carries the spent gen-1 obligation"
  );

  // Re-freeze 1 for a FRESH merge into 3 at gen 3. Its new target 3 has not parked yet, so leg 2
  // (Frozen) is the only strand-preventing refusal available in this window.
  {
    m.prepare_merge(&1, now, &mut stores, &3).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    m.group(&1).unwrap().is_frozen(),
    "1 is a fresh frozen source for 3"
  );
  assert_eq!(
    m.group(&1).unwrap().shape_gen(),
    3,
    "the fresh freeze minted gen 3"
  );

  // THE GATE, generation-exact: 2's obligation names gen 1, 1 is live at gen 3 — the stale record
  // suppresses NOTHING, so leg 2 refuses the newly-frozen source (RED under the id-only escape).
  assert!(
    matches!(m.remove_group(&1, &mut stores), Err(RemoveError::Frozen)),
    "a freshly-frozen source is refused Frozen — a stale obligation cannot bypass the gate"
  );
  // NO SIDE EFFECTS: the refusal tore nothing down — 1 is still the frozen source for its new merge.
  assert!(
    m.contains_group(&1) && m.group(&1).unwrap().is_frozen(),
    "the refused removal left 1 hosted and frozen for its 3-bound merge"
  );
  // And 2's spent obligation is untouched — the gate is a pure read, fabricating and clearing nothing.
  assert!(
    m.group(&2).unwrap().has_abandoned(),
    "the refusal left 2's lingering obligation exactly as it was"
  );
}

/// Leg 4 in isolation: a source is refused `SpokenFor` PURELY because another hosted endpoint's park
/// names it — even when its own replica contributes no freeze signal (the window where a target has
/// committed its `CommitMerge` but this node has not observed the source's freeze). Group 2 parks
/// naming source 1; the source replica is then dropped through the UNGATED inner teardown (the very
/// strand the public door now refuses to create). `remove_group(&1)` is refused `SpokenFor`, never a
/// silent no-op — leg 2 cannot fire (no source endpoint), so only the cross-endpoint park scan
/// catches it.
#[test]
fn teardown_refuses_a_spoken_for_source_with_no_local_freeze() {
  let (mut m, mut stores) = merge_host(2, 3);
  freeze_and_park(&mut m, &mut stores);
  // Strand 2's park: drop the source replica WITHOUT resolving the merge (the ungated inner teardown
  // — exactly the strand `remove_group`'s participant gate exists to refuse). 2 stays parked on 1.
  assert!(
    m.remove_group_inner(&1).is_some(),
    "the source replica is dropped through the ungated inner path"
  );
  assert!(
    !m.contains_group(&1),
    "no local source endpoint contributes a freeze signal"
  );
  assert!(
    m.group(&2).unwrap().pending_merge().is_some(),
    "2 is still parked naming the now-absent source 1"
  );
  // Leg 4 alone catches it: the park names 1, so removing 1 is SpokenFor even with no local freeze.
  assert!(
    matches!(m.remove_group(&1, &mut stores), Err(RemoveError::SpokenFor)),
    "a source a live park names is refused SpokenFor, never a silent no-op"
  );
}

/// Leg 5, the CLAIMED-TARGET pre-park window (the last leg of the participant lattice). Source 1
/// freezes into 2 and APPLIES it (1 is `frozen_for` 2), but 2 never proposes its `CommitMerge` — so
/// 2 has no `pending_merge` (`MergeParked` misses) and no park names 2 (`SpokenFor` reads the mirror
/// direction and misses). RED current: `remove_group(&2)` ADMITS, stranding 1 frozen for a target
/// that no longer exists — 1's absorb AND its abort both ride 2's log, so neither can be proposed,
/// and 1's own removal then refuses `Frozen` (it owes no thaw). GREEN: leg 5 refuses `Claimed`,
/// touching nothing. THE ESCAPE: roll the merge back on 2 (still hosted pre-park), which thaws 1 and
/// discharges 2's obligation, after which the SAME removal admits. A non-participant (3) is unchanged.
#[test]
fn teardown_refuses_a_claimed_target_before_the_park() {
  let (mut m, mut stores) = merge_host_triple(2, 3, 4);
  let now = Instant::ORIGIN;
  // 1 freezes into 2 and APPLIES — 1 is frozen_for 2 — but 2 never commits, so it never parks.
  {
    m.prepare_merge(&1, now, &mut stores, &2).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    m.group(&1).unwrap().is_frozen(),
    "1 is an applied frozen source claiming 2"
  );
  assert!(
    m.group(&2).unwrap().pending_merge().is_none(),
    "2 never proposed a commit, so it never parked"
  );

  // NEGATIVE PIN: a non-claimed group tears down exactly as before.
  assert!(
    m.remove_group(&3, &mut stores).unwrap().is_some(),
    "a group no frozen source claims is byte-for-byte unchanged"
  );

  // THE GATE, leg 5: 2 is claimed by 1's applied freeze. `MergeParked` cannot see it (2 has no park)
  // and `SpokenFor` cannot (no park names 2) — only the mirror scan catches the claim.
  assert!(
    matches!(m.remove_group(&2, &mut stores), Err(RemoveError::Claimed)),
    "a target a frozen source claims is refused Claimed before it parks"
  );
  // NO SIDE EFFECTS: the choreography is intact — the strand the RED path would create is absent.
  assert!(
    m.contains_group(&2) && m.group(&1).unwrap().is_frozen(),
    "the refused removal left 2 hosted and 1 frozen for it"
  );
  // NEGATIVE PIN: the SOURCE side is still refused `Frozen` (its own role), not `Claimed`.
  assert!(
    matches!(m.remove_group(&1, &mut stores), Err(RemoveError::Frozen)),
    "the frozen source itself is refused Frozen"
  );

  // THE ESCAPE: roll the merge back on 2 (hosted pre-park) — 1 thaws, 2's obligation discharges.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.rollback_merge(&2, now, log, stable, &1).unwrap().unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  m.service_merge_applies(now, &mut stores);
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(!m.group(&1).unwrap().is_frozen(), "the rollback thawed 1");
  m.service_merge_applies(now, &mut stores);
  // The observing leader deferred its clear to the witness — apply it on the holder.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    !m.group(&2).unwrap().has_abandoned(),
    "the observed thaw discharged 2's obligation"
  );
  // SELF-CLEARING PIN: with the claiming merge rolled back, the SAME removal now admits.
  assert!(
    m.remove_group(&2, &mut stores).unwrap().is_some(),
    "once the claim is rolled back, the former target tears down as a clean group"
  );
}

/// Leg 5's APPEND-PENDING window: the claim is refused even before the freeze applies. Source 1's
/// `PrepareMerge` is APPENDED (its append-observed lease kill is live) but NOT yet folded, so its
/// target claim is still undecoded in-memory (`frozen_for` is `None`) — the applied leg cannot see
/// it. RED current: `remove_group(&2)` ADMITS, stranding 1 identically once the freeze applies.
/// GREEN: the gate DECODES the claim from 1's own unapplied log suffix and refuses `Claimed`. A
/// DIFFERENT target (3) still tears down — the decode reads the exact claim (2), never over-refusing.
#[test]
fn teardown_refuses_a_claimed_target_from_the_append_pending_freeze() {
  let (mut m, mut stores) = merge_host_triple(2, 3, 4);
  let now = Instant::ORIGIN;
  // 1's PrepareMerge is APPENDED but deliberately NOT drained — freeze-pending, claim undecoded.
  {
    m.prepare_merge(&1, now, &mut stores, &2).unwrap().unwrap();
  }
  assert!(
    m.group(&1).unwrap().merge_freeze_active() && !m.group(&1).unwrap().is_frozen(),
    "1 observes its freeze at append but has not applied — the claim is undecoded in-memory"
  );

  // NEGATIVE PIN: the decode reads the EXACT claim — a different target is not over-refused.
  assert!(
    m.remove_group(&3, &mut stores).unwrap().is_some(),
    "the pending freeze claims 2, not 3 — removing 3 is unchanged"
  );

  // THE GATE, leg 5 pending sub-case: the append-pending claim is decoded from 1's log suffix.
  assert!(
    matches!(m.remove_group(&2, &mut stores), Err(RemoveError::Claimed)),
    "an append-pending freeze's decoded claim refuses the target before the freeze folds"
  );
  assert!(
    m.contains_group(&2),
    "the refused removal left the claimed target hosted"
  );

  // CONTINUITY: once the freeze APPLIES, the claim is refused through the applied leg (frozen_for) —
  // the window is closed end to end.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(m.group(&1).unwrap().is_frozen(), "the freeze folded");
  assert!(
    matches!(m.remove_group(&2, &mut stores), Err(RemoveError::Claimed)),
    "the applied claim keeps refusing the target — no gap between the two windows"
  );
}

/// The PROPOSE-TIME twin of teardown leg 5, at the freeze door: a group another hosted source's
/// APPLIED freeze claims as its TARGET must not freeze as a fresh merge's SOURCE. 1 freezes into 2
/// (1 is `frozen_for` 2); 2 then tries to freeze into 3. RED current: `prepare_merge(2 -> 3)`
/// ADMITS — a later absorb dissolves 2, and 1's release verbs (`commit_merge`, `rollback_merge`)
/// both ride 2's dead log: `None` forever, 1 stranded frozen with no release valve. GREEN: refused
/// `SourceClaimedAsTarget`, appending nothing. THE RELEASE (abort path): rolling 1's merge back on
/// 2 thaws 1 — clearing its claim — and discharges 2's obligation, after which the SAME freeze
/// admits.
#[test]
fn a_claimed_merge_target_cannot_freeze_as_a_source() {
  let (mut m, mut stores) = merge_host_triple(2, 3, 4);
  let now = Instant::ORIGIN;
  // 1 freezes into 2 and APPLIES — 1 is frozen_for 2.
  {
    m.prepare_merge(&1, now, &mut stores, &2).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(m.group(&1).unwrap().is_frozen(), "1 claims 2");

  // THE GATE: 2 is a claimed target — it must not dissolve as a source while 1's claim stands.
  assert!(
    matches!(
      m.prepare_merge(&2, now, &mut stores, &3),
      Some(Err(MergeError::SourceClaimedAsTarget))
    ),
    "a claimed target is refused source-role, typed"
  );
  assert!(
    !m.group(&2).unwrap().merge_freeze_active(),
    "the refusal appended nothing"
  );

  // THE RELEASE, abort path: 2 aborts 1's merge; the relayed thaw unfreezes 1 (clearing its
  // claim) and the observed advance discharges 2's obligation.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.rollback_merge(&2, now, log, stable, &1).unwrap().unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  m.service_merge_applies(now, &mut stores);
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(!m.group(&1).unwrap().is_frozen(), "the rollback thawed 1");
  m.service_merge_applies(now, &mut stores);
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    !m.group(&2).unwrap().has_abandoned(),
    "the observed thaw discharged 2's obligation"
  );

  // SELF-CLEARING PIN: with the claim discharged, the SAME freeze now admits.
  {
    assert!(
      m.prepare_merge(&2, now, &mut stores, &3).unwrap().is_ok(),
      "the released target freezes into 3 exactly as a clean source would"
    );
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(m.group(&2).unwrap().is_frozen(), "2 froze into 3");
}

/// The freeze door's APPEND-PENDING window, mirroring teardown leg 5's: the claim refuses
/// source-role even before the claiming freeze applies. 1's `PrepareMerge` is appended, not
/// folded — its claim is undecoded in-memory (`frozen_for` is `None`), so only the log scan can
/// see it. RED current: `prepare_merge(2 -> 3)` ADMITS in the window and the strand forms
/// identically once the freeze folds. GREEN: the claim is decoded from 1's unapplied suffix and
/// refused; once the freeze APPLIES the refusal continues through the applied leg — no gap
/// between the windows. The decode is EXACT: candidate 3, which 1's claim does not name, freezes
/// toward 2 unrefused (fan-in onto one target is the designed `abandoned` fan-in).
#[test]
fn an_append_pending_claim_refuses_source_role_too() {
  let (mut m, mut stores) = merge_host_triple(2, 3, 4);
  let now = Instant::ORIGIN;
  // 1's PrepareMerge is APPENDED but deliberately NOT drained — freeze-pending, claim undecoded.
  m.prepare_merge(&1, now, &mut stores, &2).unwrap().unwrap();
  assert!(
    m.group(&1).unwrap().merge_freeze_active() && !m.group(&1).unwrap().is_frozen(),
    "1 observes its freeze at append but has not applied"
  );

  // THE GATE, pending sub-case: 2 is refused source-role off the decoded suffix claim.
  assert!(
    matches!(
      m.prepare_merge(&2, now, &mut stores, &3),
      Some(Err(MergeError::SourceClaimedAsTarget))
    ),
    "an append-pending claim refuses the target source-role before the freeze folds"
  );

  // CONTINUITY: once the freeze APPLIES, the applied leg (frozen_for) keeps refusing.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(m.group(&1).unwrap().is_frozen(), "the freeze folded");
  assert!(
    matches!(
      m.prepare_merge(&2, now, &mut stores, &3),
      Some(Err(MergeError::SourceClaimedAsTarget))
    ),
    "the applied claim keeps refusing source-role — no gap between the two windows"
  );

  // EXACTNESS: the claim names 2, not 3 — 3 freezes toward 2 unrefused.
  assert!(
    m.prepare_merge(&3, now, &mut stores, &2).unwrap().is_ok(),
    "a candidate the claim does not name is never over-refused"
  );
}

/// The gate FAILS CLOSED, the teardown leg's read discipline verbatim: an append-pending claim
/// the scan cannot READ (the claimant's suffix is cold) is treated as a claim it cannot rule
/// out, and the freeze refuses — never risking a dissolve that would strand an uninspectable
/// claimant. Candidate 3 is NOT the pending claim's target (1 claims 2), so a READABLE scan
/// admits the same propose — the warm re-run proves the refusal was the fail-closed arm, not a
/// claim match.
#[test]
fn an_unreadable_claim_scan_fails_closed_at_the_freeze_door() {
  use crate::testkit::FailTermLog;
  struct ColdStores(std::collections::BTreeMap<u64, (FailTermLog, AsyncStable)>);
  impl crate::GroupStores<u64, FailTermLog, AsyncStable> for ColdStores {
    fn stores(&mut self, group: &u64) -> Option<(&mut FailTermLog, &mut AsyncStable)> {
      self.0.get_mut(group).map(|(l, s)| (l, s))
    }
  }
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let mut stores = ColdStores(std::collections::BTreeMap::new());
  let now = Instant::ORIGIN;
  for gid in [1u64, 2, 3] {
    stores
      .0
      .insert(gid, (FailTermLog::default(), AsyncStable::default()));
    m.create_group(gid, 0, single_node_cfg(1), now, 7, CountSm::default())
      .unwrap();
    let (log, stable) = stores.0.get_mut(&gid).unwrap();
    let d = m.group(&gid).unwrap().poll_timeout().unwrap();
    m.handle_timeout(&gid, d, log, stable).unwrap();
    while matches!(
      m.handle_storage(&gid, d, log, stable),
      Some(StorageProgress::MorePending)
    ) {}
    assert!(m.group(&gid).unwrap().role().is_leader());
  }
  while m.poll_message().is_some() {}
  while m.poll_event().is_some() {}

  // 1's freeze toward 2 is APPENDED, not applied: the claim lives only in 1's log suffix.
  m.prepare_merge(&1, now, &mut stores, &2).unwrap().unwrap();
  // The claimant's suffix goes COLD: the scan for candidate 3 cannot rule the claim out.
  stores.0.get_mut(&1).unwrap().0.return_cold_on_read();
  assert!(
    matches!(
      m.prepare_merge(&3, now, &mut stores, &2),
      Some(Err(MergeError::SourceClaimedAsTarget))
    ),
    "an unreadable pending claim refuses fail-closed — even a candidate it may not name"
  );
  // Warm again, the decode is exact: 1's claim names 2, so candidate 3 admits.
  stores.0.get_mut(&1).unwrap().0.clear_cold_on_read();
  assert!(
    m.prepare_merge(&3, now, &mut stores, &2).unwrap().is_ok(),
    "the same propose admits once the claim is readable — the refusal was the fail-closed arm"
  );
}

/// LEG beta (liveness, the residual window the propose doors cannot fully close): an abort
/// committed on 2 BELOW its own freeze materializes `abandoned[1]` only after the freeze already
/// landed — the freeze fold is an unguarded max, so 2 freezes for 3 while carrying the fresh
/// obligation. The colocated form is now DOOR-REFUSED (`SourceClaimedAsTarget`, asserted below);
/// the window survives cross-host, where the proposing 2-leader's local replica of 1 has not
/// observed 1's freeze — reproduced here past the door. RED current: the Resolve arm dissolves 2
/// with the live obligation, stranding 1 frozen forever. GREEN: the absorb is HELD; the thaw pass
/// (which does NOT skip the frozen holder 2) discharges 1 first, and only then is 2 absorbed into 3.
#[test]
fn a_late_obligation_holds_the_absorb_until_the_thaw_discharges() {
  let (mut m, mut stores) = merge_host_triple(2, 3, 4);
  let now = Instant::ORIGIN;
  // 1 freezes into 2 (1 frozen, claim = 2).
  {
    m.prepare_merge(&1, now, &mut stores, &2).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(m.group(&1).unwrap().is_frozen());
  // 2 aborts the 1 -> 2 merge but the abort is NOT drained — appended+committed below where 2's own
  // freeze will land. `has_abandoned` reads APPLIED state, so it is still false here: the prepare
  // gate below cannot see the obligation.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.rollback_merge(&2, now, log, stable, &1).unwrap().unwrap();
  }
  assert!(
    !m.group(&2).unwrap().has_abandoned(),
    "the abort has not applied — no obligation yet"
  );
  // The freeze DOOR now refuses this colocated shape outright: 1's applied claim on 2 is locally
  // visible, so `prepare_merge(2 -> 3)` is `SourceClaimedAsTarget` — the window no longer opens
  // through the propose here.
  assert!(
    matches!(
      m.prepare_merge(&2, now, &mut stores, &3),
      Some(Err(MergeError::SourceClaimedAsTarget))
    ),
    "the colocated form of the window is door-refused"
  );
  // The window REMAINS reachable cross-host (a distributed 2-leader whose local replica of 1 has
  // not observed 1's freeze yet sees no claim), so the belt below must still hold. Reproduce it
  // past the door with a direct endpoint append: 2's PrepareMerge for 3 ABOVE the still-unapplied
  // abort. Draining then applies the abort (recording `abandoned[1]`) THEN the freeze (2 frozen
  // for 3, the unguarded max-fold keeps both at the same generation the real mint would use).
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    let mut tbytes = Vec::new();
    Data::encode(&3u64, &mut tbytes);
    let freeze = crate::PrepareMergePayload::new(Bytes::from(tbytes), 1);
    let mut fbuf = Vec::new();
    crate::wire::encode_prepare_merge_payload(&freeze, &mut fbuf);
    m.group_mut(&2)
      .unwrap()
      .propose_merge_entry(now, log, crate::EntryKind::PrepareMerge, Bytes::from(fbuf))
      .unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    m.group(&2).unwrap().has_abandoned(),
    "the abort applied — 2 now owes 1 a thaw"
  );
  assert!(m.group(&2).unwrap().is_frozen(), "and 2 is frozen for 3");
  // 3 commits the absorb of 2 and parks; seal 3's abort window.
  {
    let (log, stable) = stores.0.get_mut(&3).unwrap();
    m.commit_merge(&3, now, log, stable, &2).unwrap().unwrap();
    drain_storage(&mut m, 3, now, log, stable);
  }
  assert!(m.group(&3).unwrap().pending_merge().is_some(), "3 parked");
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  {
    let (log, stable) = stores.0.get_mut(&3).unwrap();
    drain_storage(&mut m, 3, now, log, stable);
  }

  // The absorb is HELD while 2 still owes 1 a thaw — 2 is NOT dissolved, no Merged surfaces.
  let held = m.service_merge_applies(now, &mut stores);
  assert!(
    held.is_empty(),
    "the absorb is held while the obligation stands: {held:?}"
  );
  assert!(m.contains_group(&2), "2 is NOT dissolved this crank");
  // The thaw pass ran on that SAME crank and drove 1's unfreeze despite 2 being FROZEN — the frozen
  // holder is not skipped. Draining 1 commits the thaw; 1 advances past its freeze generation.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    !m.group(&1).unwrap().is_frozen(),
    "1 thawed while 2's absorb waited"
  );

  // Next crank mints 2's discharge witness (the frozen holder is NOT skipped — the witness rides above
  // 2's own freeze, FSM-non-mutating); applying it on 2 discharges the obligation, and the crank after
  // completes 3's absorb of 2.
  m.service_merge_applies(now, &mut stores);
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    !m.group(&2).unwrap().has_abandoned(),
    "2's obligation discharged on the observed advance"
  );
  let done = m.service_merge_applies(now, &mut stores);
  assert_eq!(
    done,
    std::vec![MergeResolution::Merged {
      source: 2,
      target: 3
    }],
    "with the obligation cleared, 2 is finally absorbed into 3"
  );
  assert!(!m.contains_group(&2), "2 dissolved");
  assert!(
    !m.group(&1).unwrap().is_frozen(),
    "and 1 stayed thawed — never stranded"
  );
  assert_eq!(
    m.group(&3).unwrap().state_machine().count(),
    4 + 3,
    "3 serves the 2 + 3 union"
  );
}

/// PIN B(a): a hosted FROZEN source at the TERMINAL floor is the husk of a lineage absorbed away
/// ELSEWHERE (this host's target caught up via a snapshot install and never parked here). It is
/// otherwise unremovable (`Frozen`) and capture-fenced forever; the husk-dissolve arm reclaims it.
#[test]
fn a_hosted_husk_at_the_terminal_floor_dissolves() {
  let (mut m, mut stores) = merge_host(0, 0);
  let now = Instant::ORIGIN;
  {
    m.prepare_merge(&1, now, &mut stores, &2).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(m.group(&1).unwrap().is_frozen(), "1 is a frozen source");
  // The catalog floors 1 terminally — its merge resolved elsewhere while no park formed here. 1 is now
  // a husk; no live park names it. Pre-mechanism it stays forever.
  stores.1.insert(1);
  let resolutions = m.service_merge_applies(now, &mut stores);
  assert_eq!(
    resolutions,
    std::vec![MergeResolution::Retired { source: 1 }],
    "the frozen husk at the terminal floor dissolved locally"
  );
  assert!(!m.contains_group(&1), "the husk is gone");
  assert_eq!(
    stores.floor(&1),
    MERGED_FLOOR,
    "its terminal floor still fences the id"
  );
}

/// PIN B(b): the dissolve keys on the EXACT terminal `MERGED_FLOOR`. A frozen source one below it —
/// the closest non-terminal floor — is a live participant, not a husk, and must NOT dissolve.
#[test]
fn a_frozen_source_one_below_the_terminal_does_not_dissolve() {
  let (mut m, map_stores) = merge_host(0, 0);
  let now = Instant::ORIGIN;
  let mut stores = LineageStores {
    inner: map_stores,
    floors: std::collections::BTreeMap::from([(1u64, u64::MAX - 1)]),
    lineages: std::collections::BTreeMap::new(),
  };
  {
    m.prepare_merge(&1, now, &mut stores, &2).unwrap().unwrap();
    let (log, stable) = stores.inner.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(m.group(&1).unwrap().is_frozen(), "1 is a frozen source");
  assert_eq!(stores.floor(&1), u64::MAX - 1);
  let resolutions = m.service_merge_applies(now, &mut stores);
  assert!(
    resolutions.is_empty(),
    "only the EXACT terminal floor dissolves a husk — a floor one below does not"
  );
  assert!(
    m.contains_group(&1),
    "the frozen source is untouched below the terminal"
  );
}

/// PIN B(c), the B1 red-proof: a hosted park still NAMING the husk as its source HOLDS the dissolve —
/// reclaiming it first would hand the resolver a MANUFACTURED absence and skip the union (committed
/// divergence). The park absorbs it instead (Merged, never Retired), union intact.
#[test]
fn a_park_naming_the_husk_holds_the_dissolve_then_absorbs() {
  let (mut m, mut stores) = merge_host(2, 3);
  let now = Instant::ORIGIN;
  let k = freeze_and_park(&mut m, &mut stores);
  // The catalog floors 1 terminally, RACING the local absorb: 1 is frozen + MERGED_FLOOR AND 2's park
  // names it as source. The husk arm must HOLD (the park gate).
  stores.1.insert(1);
  let sealed = m.service_merge_applies(now, &mut stores);
  assert!(sealed.is_empty(), "the seal pass resolves nothing");
  assert!(
    m.contains_group(&1),
    "the park gate held the dissolve — 1 is not reclaimed as a husk"
  );
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  let resolved = m.service_merge_applies(now, &mut stores);
  assert_eq!(
    resolved,
    std::vec![MergeResolution::Merged {
      source: 1,
      target: 2
    }],
    "the park absorbs the husk (Merged), never the dissolve (Retired)"
  );
  assert!(!m.contains_group(&1), "1 absorbed into 2");
  let tep = m.group(&2).unwrap();
  assert_eq!(tep.applied_index(), k, "the parked entry applied");
  assert_eq!(
    tep.state_machine().count(),
    2 + 3,
    "the union is intact — 1's content folded in"
  );
}

/// PIN B(d), the belt: a husk that still owes a LOCALLY-DRIVABLE thaw must NOT dissolve — dropping the
/// obligation would strand the upstream source frozen forever. Construction mirrors the late-obligation
/// belt: 2 aborts 1→2 below its own freeze into 3, so 2 owes 1 a thaw (1 hosted) AND is frozen.
#[test]
fn the_belt_holds_a_husk_owing_a_locally_drivable_thaw() {
  let (mut m, mut stores) = merge_host_triple(2, 3, 4);
  let now = Instant::ORIGIN;
  {
    m.prepare_merge(&1, now, &mut stores, &2).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.rollback_merge(&2, now, log, stable, &1).unwrap().unwrap();
  }
  // The freeze door refuses the colocated form (`SourceClaimedAsTarget` — 1's applied claim on 2
  // is locally visible); the window is cross-host residue now, so build it past the door with a
  // direct endpoint append, exactly as the late-obligation belt does.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    let mut tbytes = Vec::new();
    Data::encode(&3u64, &mut tbytes);
    let freeze = crate::PrepareMergePayload::new(Bytes::from(tbytes), 1);
    let mut fbuf = Vec::new();
    crate::wire::encode_prepare_merge_payload(&freeze, &mut fbuf);
    m.group_mut(&2)
      .unwrap()
      .propose_merge_entry(now, log, crate::EntryKind::PrepareMerge, Bytes::from(fbuf))
      .unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    m.group(&2).unwrap().has_abandoned() && m.group(&2).unwrap().is_frozen(),
    "2 owes 1 a drivable thaw AND is frozen for 3"
  );
  // The catalog (adversarially) floors 2 terminally; the belt must hold the dissolve while 2 owes a
  // thaw THIS host can drive (1 is hosted).
  stores.1.insert(2);
  let resolutions = m.service_merge_applies(now, &mut stores);
  assert!(
    !resolutions
      .iter()
      .any(|r| matches!(r, MergeResolution::Retired { .. })),
    "the belt held — a husk owing a locally-drivable thaw is NOT dissolved"
  );
  assert!(
    m.contains_group(&2),
    "2 is untouched while its drivable obligation stands"
  );
  assert!(
    m.group(&2).unwrap().has_abandoned(),
    "the obligation is intact — the dissolve did not drop it"
  );
}

/// The negative pin: a source that owes NO thaw dissolves in the ordinary cadence — the residual
/// belt never over-fires. 2 freezes into 3 with no outstanding obligation and is absorbed in the
/// same single resolve pass a clean source always was.
#[test]
fn a_source_without_an_obligation_absorbs_at_once() {
  let (mut m, mut stores) = merge_host_triple(2, 3, 4);
  let now = Instant::ORIGIN;
  assert!(!m.group(&2).unwrap().has_abandoned(), "2 owes nothing");
  {
    m.prepare_merge(&2, now, &mut stores, &3).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  {
    let (log, stable) = stores.0.get_mut(&3).unwrap();
    m.commit_merge(&3, now, log, stable, &2).unwrap().unwrap();
    drain_storage(&mut m, 3, now, log, stable);
  }
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  {
    let (log, stable) = stores.0.get_mut(&3).unwrap();
    drain_storage(&mut m, 3, now, log, stable);
  }
  let done = m.service_merge_applies(now, &mut stores);
  assert_eq!(
    done,
    std::vec![MergeResolution::Merged {
      source: 2,
      target: 3
    }],
    "a clean source is absorbed with no extra crank"
  );
  assert!(!m.contains_group(&2));
  assert_eq!(m.group(&3).unwrap().state_machine().count(), 4 + 3);
}

/// FIX 4: an obligation whose owed id will NOT decode is committed-corrupt — the same `MergeDecode`
/// class the thaw pass and park decode raise. The drivability belt must HOLD the park and poison the
/// SOURCE (the deterministic fail-stop every host reaches), never treat the undecodable id as "not
/// drivable" and AUTHORIZE the dissolve — which would diverge hosts between fail-stop and progress by
/// crank order. RED before the fix: the absorb proceeds (Merged) and the corrupt obligation drops.
#[test]
fn a_corrupt_owed_id_holds_the_park_and_poisons_the_source() {
  let (mut m, mut stores) = merge_host_triple(2, 3, 4);
  let now = Instant::ORIGIN;
  // A corrupt obligation on 2, then 2 freezes into 3 ABOVE it (the unguarded-max ordering the
  // `SourceOwesThaw` gate cannot see): draining applies the abort (abandoned[corrupt]) then the
  // freeze. 3 bytes never decode as the `u64` group id.
  let corrupt = Bytes::from_static(&[0xFF, 0xFF, 0xFF]);
  {
    let (log, _stable) = stores.0.get_mut(&2).unwrap();
    let abort = crate::RollbackMergePayload::abort(corrupt.clone(), 1, 1);
    let mut buf = Vec::new();
    crate::wire::encode_rollback_merge_payload(&abort, &mut buf);
    m.group_mut(&2)
      .unwrap()
      .propose_merge_entry(now, log, crate::EntryKind::RollbackMerge, Bytes::from(buf))
      .unwrap();
  }
  {
    m.prepare_merge(&2, now, &mut stores, &3).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(m.group(&2).unwrap().is_frozen(), "2 is frozen for 3");
  assert!(
    m.group(&2).unwrap().has_abandoned(),
    "2 carries the corrupt obligation"
  );
  // 3 commits the absorb of 2 and parks; seal 3's window.
  {
    let (log, stable) = stores.0.get_mut(&3).unwrap();
    m.commit_merge(&3, now, log, stable, &2).unwrap().unwrap();
    drain_storage(&mut m, 3, now, log, stable);
  }
  assert!(m.group(&3).unwrap().pending_merge().is_some(), "3 parked");
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  {
    let (log, stable) = stores.0.get_mut(&3).unwrap();
    drain_storage(&mut m, 3, now, log, stable);
  }
  // The belt HOLDS the park and poisons the source — never a silent dissolve.
  let held = m.service_merge_applies(now, &mut stores);
  assert!(
    held.is_empty(),
    "the corrupt obligation holds the absorb: {held:?}"
  );
  assert!(m.contains_group(&2), "2 is NOT dissolved");
  assert!(
    m.group(&3).unwrap().pending_merge().is_some(),
    "3 is still parked"
  );
  assert!(
    m.group(&2).unwrap().is_poisoned(),
    "the source is poisoned MergeDecode"
  );
}

/// FIX 4 contrast (kept green): an obligation whose owed id DECODES but is not hosted here is a local
/// dead-end — a co-hosting replica drives that thaw, so the absorb PROCEEDS and dropping the dead-end
/// obligation strands nothing. Distinguishes the corrupt-id poison above from the belt's ordinary
/// dead-end drop; both share the resolve arm's decode.
#[test]
fn a_decodable_unhosted_owed_id_lets_the_absorb_proceed() {
  let (mut m, mut stores) = merge_host_triple(2, 3, 4);
  let now = Instant::ORIGIN;
  // 999 is a decodable `u64` that is NOT a hosted group — a local dead-end obligation.
  let mut unhosted = Vec::new();
  Data::encode(&999u64, &mut unhosted);
  let unhosted = Bytes::from(unhosted);
  {
    let (log, _stable) = stores.0.get_mut(&2).unwrap();
    let abort = crate::RollbackMergePayload::abort(unhosted.clone(), 1, 1);
    let mut buf = Vec::new();
    crate::wire::encode_rollback_merge_payload(&abort, &mut buf);
    m.group_mut(&2)
      .unwrap()
      .propose_merge_entry(now, log, crate::EntryKind::RollbackMerge, Bytes::from(buf))
      .unwrap();
  }
  {
    m.prepare_merge(&2, now, &mut stores, &3).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    m.group(&2).unwrap().has_abandoned(),
    "2 owes a dead-end thaw"
  );
  {
    let (log, stable) = stores.0.get_mut(&3).unwrap();
    m.commit_merge(&3, now, log, stable, &2).unwrap().unwrap();
    drain_storage(&mut m, 3, now, log, stable);
  }
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  {
    let (log, stable) = stores.0.get_mut(&3).unwrap();
    drain_storage(&mut m, 3, now, log, stable);
  }
  let done = m.service_merge_applies(now, &mut stores);
  assert_eq!(
    done,
    std::vec![MergeResolution::Merged {
      source: 2,
      target: 3
    }],
    "a decodable dead-end obligation does not hold the absorb"
  );
  assert!(
    !m.contains_group(&2),
    "2 dissolved — the dead-end obligation dropped by design"
  );
  assert!(
    !m.group(&3).unwrap().is_poisoned(),
    "no poison for a decodable id"
  );
}

/// FINDING-1 RED (safety, structural): a source thaw is REFUSED with NO append unless the claimed
/// target hosts a matching committed abort obligation. A frozen source claimed by target 2 — but with
/// NO abort ever applied on 2 — must not thaw: appending it would move the source's counter out from
/// under a target that never abandoned it (the #22 cross-log race). The gate is intrinsic to the thaw
/// path (belt) and the helper is private with no coordinator delegator (suspenders), so the only
/// driver of a thaw is the container service, which derives the drive FROM the obligation — there is
/// no reachable path to append a thaw with no matching target `abandoned`.
#[test]
fn thaw_without_a_committed_target_abort_is_refused() {
  let (mut m, mut stores) = merge_host(2, 3);
  let now = Instant::ORIGIN;
  // Source 1 frozen-applied at gen 1, claimed by target 2, leader — but 2 NEVER aborted, so it holds
  // no `abandoned` obligation for source 1.
  {
    m.prepare_merge(&1, now, &mut stores, &2).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(m.group(&1).unwrap().is_frozen() && m.group(&1).unwrap().shape_gen() == 1);
  assert!(
    !m.group(&2).unwrap().has_abandoned(),
    "target 2 holds no abort obligation — nothing authorizes a thaw"
  );
  let last_before = stores.0.get(&1).unwrap().0.last_index();

  // The constructed thaw naming the exact frozen incarnation is REFUSED with NO append — the invariant
  // `unfreeze(source) ⟹ ∃ committed target-abort(source, gen)` is structural, not advisory. RED
  // without the gate: the source-local checks all pass and the thaw APPENDS, unfreezing a source no
  // target abandoned.
  let result = {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.propose_merge_unfreeze(&1, now, log, stable, &2, 1)
  };
  assert!(
    matches!(result, Some(Err(MergeError::UnbackedThaw))),
    "a thaw with no committed target abort is refused: {result:?}"
  );
  assert_eq!(
    stores.0.get(&1).unwrap().0.last_index(),
    last_before,
    "the refused thaw appended nothing"
  );
  assert!(
    m.group(&1).unwrap().is_frozen(),
    "the source stays frozen — never thawed out from under a target that never abandoned it"
  );

  // NEGATIVE PIN: once target 2 commits a real abort, the SAME thaw is authorized and appends.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.rollback_merge(&2, now, log, stable, &1).unwrap().unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  let authorized = {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.propose_merge_unfreeze(&1, now, log, stable, &2, 1)
  };
  assert!(
    matches!(authorized, Some(Ok(_))),
    "the committed abort authorizes the thaw: {authorized:?}"
  );
}

/// INCARNATION RED (safety, the #22 race across a source remove/recreate): a target's `abandoned`
/// obligation is keyed by the source's LOCAL freeze gen, which a P5 remove/recreate RESETS. Were the
/// removed source's obligation left behind, a fresh incarnation that re-froze the SAME pair at the
/// SAME repeated gen would find the stale record still backing a thaw the target never aborted for
/// THIS incarnation — a frozen source thawed with no committed target-abort, reopening the cross-log
/// race. The removal choke point PURGES the obligation, so a recreate can never reuse it.
#[test]
fn removed_source_obligation_cannot_back_a_recreates_thaw() {
  let (mut m, mut stores) = merge_host(2, 3);
  let now = Instant::ORIGIN;
  // Freeze source 1 -> target 2 (gen 1) and abort on the target: the obligation records gen 1.
  {
    m.prepare_merge(&1, now, &mut stores, &2).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.rollback_merge(&2, now, log, stable, &1).unwrap().unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert_eq!(
    m.group(&2)
      .unwrap()
      .abandoned_obligations()
      .first()
      .map(|(_, g, _)| *g),
    Some(1),
    "target 2 owes source 1 a thaw for freeze gen 1"
  );

  // REMOVE source 1, non-terminally (no `MERGED_FLOOR`), and drop its store. The choke point purges
  // the obligation — RED here without the purge: the removed source's record strands on the target.
  assert!(m.remove_group(&1, &mut stores).unwrap().is_some());
  stores.0.remove(&1);
  assert!(
    !m.group(&2).unwrap().has_abandoned(),
    "the removal purged the target's obligation for the departed source"
  );

  // RECREATE source 1 at genesis — its LOCAL shape_gen resets to 0 — then elect and drain it.
  stores
    .0
    .insert(1, (VecLog::default(), AsyncStable::default()));
  m.create_group(1, 0, single_node_cfg(1), now, 7, CountSm::default())
    .unwrap();
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    let d = m.group(&1).unwrap().poll_timeout().unwrap();
    m.handle_timeout(&1, d, log, stable).unwrap();
    drain_storage(&mut m, 1, d, log, stable);
    assert!(m.group(&1).unwrap().role().is_leader());
  }
  while m.poll_message().is_some() {}
  while m.poll_event().is_some() {}

  // RE-FREEZE the SAME pair: the reset counter mints gen 1 again — the exact value the OLD obligation
  // named, but a NEW incarnation the target never aborted.
  {
    m.prepare_merge(&1, now, &mut stores, &2).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    m.group(&1).unwrap().is_frozen() && m.group(&1).unwrap().shape_gen() == 1,
    "the recreated source froze at the repeated gen 1"
  );

  // The stale obligation must NOT authorize this incarnation's thaw: the derived-from-abort gate
  // finds no matching obligation and refuses with NO append. RED without the purge: the stale record
  // still matches `(1, 1)` and the thaw APPENDS, unfreezing a source no target abandoned.
  let last_before = stores.0.get(&1).unwrap().0.last_index();
  let result = {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.propose_merge_unfreeze(&1, now, log, stable, &2, 1)
  };
  assert!(
    matches!(result, Some(Err(MergeError::UnbackedThaw))),
    "the recreate's thaw is unbacked — the stale obligation was purged: {result:?}"
  );
  assert_eq!(
    stores.0.get(&1).unwrap().0.last_index(),
    last_before,
    "the refused thaw appended nothing"
  );

  // And the service — the only production driver of the thaw — leaves the recreate frozen: no target
  // owes this incarnation a thaw, so the target-log `k + 1` decider and the incarnation gate stand.
  m.service_merge_applies(now, &mut stores);
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    m.group(&1).unwrap().is_frozen(),
    "the recreated source stays frozen — the removed incarnation's abort can never thaw it"
  );
}

/// STRUCTURAL DEFENSE (liveness): an obligation RE-DERIVED after a restart (replayed from the target's
/// still-durable abort entry) for a source that was torn down and FLOORED — not terminally merged —
/// must still discharge, or that abort entry stays capture-fenced forever. The unhosted discharge binds
/// to the PERSISTED lineage/floor: a floor that no longer admits `expected` proves the frozen-at-
/// `expected` incarnation is gone for good. RED under the old `floor == MERGED_FLOOR` discharge — a
/// non-terminal floor never equals the sentinel, so the re-derived obligation wedges the target's
/// compaction fence.
#[test]
fn a_floored_sources_rederived_obligation_discharges() {
  let (mut m, mut base) = merge_host(2, 3);
  let now = Instant::ORIGIN;
  // Freeze 1 -> 2 (gen 1) and abort: target 2 records the obligation; capture the abort index.
  {
    m.prepare_merge(&1, now, &mut base, &2).unwrap().unwrap();
    let (log, stable) = base.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  {
    let (log, stable) = base.0.get_mut(&2).unwrap();
    m.rollback_merge(&2, now, log, stable, &1).unwrap().unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  let abort_index = m.group(&2).unwrap().abandoned_obligations()[0].2;

  // Tear the source down (non-terminal) and drop its store. The removal purges the obligation (the
  // race-free leg), so re-INSERT it to model the restart replay that re-derives it from the surviving
  // abort entry while the source is gone.
  assert!(m.remove_group(&1, &mut base).unwrap().is_some());
  base.0.remove(&1);
  let mut key = Vec::new();
  Data::encode(&1u64, &mut key);
  m.group_mut(&2)
    .unwrap()
    .note_abandoned(Bytes::from(key), 1, abort_index);
  assert!(
    m.group(&2).unwrap().has_abandoned(),
    "the obligation is re-derived, as a restart replay would"
  );

  // The persisted floor fences the removed incarnation at gen 2 (> the freeze gen 1): a recreate can
  // only land above it, so the frozen-at-gen-1 incarnation is gone. The service discharges the
  // re-derived obligation off that record, lifting the target's compaction fence.
  let mut stores = LineageStores {
    inner: base,
    floors: std::collections::BTreeMap::from([(1u64, 2u64)]),
    lineages: std::collections::BTreeMap::new(),
  };
  m.service_merge_applies(now, &mut stores);
  assert!(
    !m.group(&2).unwrap().has_abandoned(),
    "the persisted floor past the freeze gen discharged the re-derived obligation"
  );
}

/// DURABLE INCARNATION FENCE (the crash-replay layer of the #22 remove/recreate race): the in-memory
/// purge on removal binds an abort obligation to its incarnation SYNCHRONOUSLY, but the obligation is
/// RE-DERIVED from the target's still-durable committed abort entry on a restart that has not yet
/// compacted past it — so a crash between the removal and that compaction resurrects it, and a
/// squatter recreated at reset gen 0 that re-freezes the SAME pair at the SAME repeated gen would
/// find the re-derived record still backing a thaw the target never aborted for THIS incarnation.
///
/// The cure is the ORDINARY removal floor off the UNIFIED lineage counter: the freeze bumped the
/// source's own lineage, so `group_gen + 1` (what a driver's `removal_floor` ceiling persists —
/// mirrored eagerly by the merge lineage events, re-derivable from the stores) already fences one
/// past every generation an obligation could name, with NO target scan. On replay the re-derived
/// obligation discharges off the persisted floor (`!floor_admits`), and a recreate must admit
/// strictly above `expected`. This test plays the driver's removal-floor discipline against a REAL
/// settable `FloorStore` and proves BOTH legs close the race.
///
/// RED without the fence (no floor persisted on removal): the re-derived obligation is NOT discharged,
/// and the recreated source's re-freeze at the repeated gen is thawed — `abandoned_matches` still
/// backs `propose_merge_unfreeze`, reopening the cross-log race across a crash.
#[test]
fn a_removed_sources_durable_floor_fences_the_rederived_abort_across_a_recreate() {
  let (mut m, base) = merge_host(2, 3);
  let now = Instant::ORIGIN;
  let mut stores = LineageStores {
    inner: base,
    floors: std::collections::BTreeMap::new(),
    lineages: std::collections::BTreeMap::new(),
  };
  // Freeze 1 -> 2 (gen 1) and abort on the target: obligation `abandoned[1] = (1, abort_index)`.
  {
    m.prepare_merge(&1, now, &mut stores, &2).unwrap().unwrap();
    let (log, stable) = stores.inner.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  {
    let (log, stable) = stores.inner.0.get_mut(&2).unwrap();
    m.rollback_merge(&2, now, log, stable, &1).unwrap().unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  let abort_index = m.group(&2).unwrap().abandoned_obligations()[0].2;

  // THE REMOVAL-FLOOR DISCIPLINE, played as a driver's `remove_group` wiring does: one past the
  // source's OWN lineage — the freeze rode the unified counter, so no target scan is needed.
  // Persist it durably, THEN purge and drop the store.
  let fence = m.group_gen(&1).saturating_add(1);
  assert_eq!(
    fence, 2,
    "the frozen source's own lineage fences one past the freeze gen (1) it owes 2 a thaw for"
  );
  stores.floors.insert(1, fence);
  assert!(m.remove_group(&1, &mut stores).unwrap().is_some());
  stores.inner.0.remove(&1);

  // RESTART REPLAY: the still-durable abort entry re-derives the purged obligation while the source
  // is absent (modeled by re-inserting it, as `a_floored_sources_rederived_obligation_discharges`).
  let mut key = Vec::new();
  Data::encode(&1u64, &mut key);
  m.group_mut(&2)
    .unwrap()
    .note_abandoned(Bytes::from(key), 1, abort_index);
  assert!(
    m.group(&2).unwrap().has_abandoned(),
    "replay re-derived the obligation from the surviving abort entry"
  );

  // THE FIRST LEG: the durable floor discharges the re-derived obligation (the source is absent and
  // floored past `expected`), lifting the target's compaction fence. RED without the persisted floor.
  m.service_merge_applies(now, &mut stores);
  assert!(
    !m.group(&2).unwrap().has_abandoned(),
    "the removal-persisted floor discharged the re-derived obligation across the restart"
  );

  // RECREATE source 1 as a BELOW-FLOOR SQUATTER at genesis gen 0 — the floor-free container
  // admits what a coordinator's `validate_floor` would refuse. Deliberate adversarial belt: even
  // an improperly-admitted repeat incarnation must find no authorization left to ride.
  stores
    .inner
    .0
    .insert(1, (VecLog::default(), AsyncStable::default()));
  m.create_group(1, 0, single_node_cfg(1), now, 7, CountSm::default())
    .unwrap();
  {
    let (log, stable) = stores.inner.0.get_mut(&1).unwrap();
    let d = m.group(&1).unwrap().poll_timeout().unwrap();
    m.handle_timeout(&1, d, log, stable).unwrap();
    drain_storage(&mut m, 1, d, log, stable);
    assert!(m.group(&1).unwrap().role().is_leader());
  }
  while m.poll_message().is_some() {}
  while m.poll_event().is_some() {}

  // RE-FREEZE the SAME pair: the reset counter mints gen 1 again — the exact value the removed
  // incarnation's abort named, but a new incarnation 2 never abandoned.
  {
    m.prepare_merge(&1, now, &mut stores, &2).unwrap().unwrap();
    let (log, stable) = stores.inner.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    m.group(&1).unwrap().is_frozen() && m.group(&1).unwrap().shape_gen() == 1,
    "the recreated source froze at the repeated gen 1"
  );

  // THE SECOND LEG: no thaw appends. The re-derived obligation was discharged off the floor, so the
  // derived-from-abort gate finds no match and refuses with NO append; the service leaves the
  // recreate frozen. RED without the fence: the stale obligation still matches `(1, 1)` and the thaw
  // appends, unfreezing a source no target abandoned this incarnation.
  let last_before = stores.inner.0.get(&1).unwrap().0.last_index();
  let result = {
    let (log, stable) = stores.inner.0.get_mut(&1).unwrap();
    m.propose_merge_unfreeze(&1, now, log, stable, &2, 1)
  };
  assert!(
    matches!(result, Some(Err(MergeError::UnbackedThaw))),
    "the recreate's thaw is unbacked — the re-derived obligation discharged off the floor: {result:?}"
  );
  assert_eq!(
    stores.inner.0.get(&1).unwrap().0.last_index(),
    last_before,
    "the refused thaw appended nothing"
  );
  m.service_merge_applies(now, &mut stores);
  {
    let (log, stable) = stores.inner.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    m.group(&1).unwrap().is_frozen(),
    "the recreated source stays frozen — the removed incarnation's abort can never thaw it"
  );
}

/// THE SEEDED-COUNTER INCARNATION KILL (a recreated HOSTED source): a floor-validated recreate
/// used to reset its endpoint counter to 0, so (a) a stale obligation re-derived from the old
/// incarnation's abort entry could never discharge off the HOSTED arm (`shape_gen > expected`
/// read `0 > 1`), and (b) the recreate's first freeze re-minted EXACTLY the abandoned generation
/// — the stale record then backed a thaw the target never aborted for THIS incarnation.
/// `create_group` seeding the counter from the ADMITTED generation kills both by construction:
/// the recreate lives strictly above its floor, the re-derived obligation discharges off the
/// live counter with no floor consult (the source is hosted), and every fresh freeze mints
/// strictly above the old `expected`.
///
/// RED without the seed (a recreate at reset gen 0): the re-derived obligation survives the
/// service crank on the hosted arm, the re-freeze mints the repeated gen 1, and the stale drive
/// naming the old incarnation APPENDS the thaw.
#[test]
fn a_recreated_hosted_source_discharges_the_stale_obligation_off_its_seeded_counter() {
  let (mut m, base) = merge_host(2, 3);
  let now = Instant::ORIGIN;
  let mut stores = LineageStores {
    inner: base,
    floors: std::collections::BTreeMap::new(),
    lineages: std::collections::BTreeMap::new(),
  };
  // Freeze 1 -> 2 (gen 1) and abort on the target: obligation `abandoned[1] = (1, abort_index)`.
  {
    m.prepare_merge(&1, now, &mut stores, &2).unwrap().unwrap();
    let (log, stable) = stores.inner.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  {
    let (log, stable) = stores.inner.0.get_mut(&2).unwrap();
    m.rollback_merge(&2, now, log, stable, &1).unwrap().unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  let abort_index = m.group(&2).unwrap().abandoned_obligations()[0].2;

  // The removal-floor discipline off the UNIFIED counter: the frozen source's OWN lineage (1)
  // already covers the obligation's generation — the floor needs no target scan. Persist it,
  // then remove and drop the store.
  let floor = m.group_gen(&1).saturating_add(1);
  assert_eq!(
    floor, 2,
    "the frozen source's own lineage covers the obligation"
  );
  stores.floors.insert(1, floor);
  assert!(m.remove_group(&1, &mut stores).unwrap().is_some());
  stores.inner.0.remove(&1);

  // Crash-replay: the target's surviving abort entry re-derives the purged obligation.
  let mut key = Vec::new();
  Data::encode(&1u64, &mut key);
  m.group_mut(&2)
    .unwrap()
    .note_abandoned(Bytes::from(key), 1, abort_index);

  // RECREATE the source HOSTED, at the generation its floor admits, and elect it.
  assert!(
    crate::floor_admits(*stores.floors.get(&1).unwrap(), 2),
    "the recreate admits at its floor"
  );
  stores
    .inner
    .0
    .insert(1, (VecLog::default(), AsyncStable::default()));
  m.create_group(1, 2, single_node_cfg(1), now, 7, CountSm::default())
    .unwrap();
  {
    let (log, stable) = stores.inner.0.get_mut(&1).unwrap();
    let d = m.group(&1).unwrap().poll_timeout().unwrap();
    m.handle_timeout(&1, d, log, stable).unwrap();
    drain_storage(&mut m, 1, d, log, stable);
    assert!(m.group(&1).unwrap().role().is_leader());
  }
  while m.poll_message().is_some() {}
  while m.poll_event().is_some() {}
  assert_eq!(
    m.group(&1).unwrap().shape_gen(),
    2,
    "the created counter starts at the admitted generation, not 0"
  );

  // THE HOSTED DISCHARGE: the live counter (2) is past the abandoned freeze (1), so the service
  // clears the re-derived obligation off the source's own lineage. RED without the seed: the
  // hosted arm reads 0 > 1 and the stale record survives.
  m.service_merge_applies(now, &mut stores);
  // The observing leader deferred its clear to the witness — apply it on the holder.
  {
    let (log, stable) = stores.inner.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    !m.group(&2).unwrap().has_abandoned(),
    "the seeded counter discharged the re-derived obligation on the hosted arm"
  );

  // The fresh freeze mints ABOVE the old expected — the recreate can never repeat gen 1.
  {
    m.prepare_merge(&1, now, &mut stores, &2).unwrap().unwrap();
    let (log, stable) = stores.inner.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    m.group(&1).unwrap().is_frozen() && m.group(&1).unwrap().shape_gen() == 3,
    "the re-freeze minted strictly above the removed incarnation's generations"
  );

  // And a stale drive naming the OLD incarnation is terminally refused with NO append.
  let last_before = stores.inner.0.get(&1).unwrap().0.last_index();
  let result = {
    let (log, stable) = stores.inner.0.get_mut(&1).unwrap();
    m.propose_merge_unfreeze(&1, now, log, stable, &2, 1)
  };
  assert!(
    matches!(
      result,
      Some(Err(MergeError::StaleThaw {
        expected: 1,
        seen: 3
      }))
    ),
    "the old incarnation's authorization is spent: {result:?}"
  );
  assert_eq!(
    stores.inner.0.get(&1).unwrap().0.last_index(),
    last_before,
    "the refused thaw appended nothing"
  );
  assert!(
    m.group(&1).unwrap().is_frozen(),
    "the new incarnation's freeze stands"
  );
}

/// THE SOURCE-LINEAGE FLOOR NEEDS NO TARGET (the re-derived-obligation escape): an obligation
/// lives in its TARGET's endpoint and is re-derived by replaying that target's durable abort entry
/// after a restore — so once the source is gone, a floor derived by scanning hosted targets is the
/// wrong tool. Under the unified counter the source's removal floor comes from its OWN lineage
/// (`group_gen + 1` — the freeze rode the same counter), so it covers every minted `expected` with
/// no knowledge of any target, and a re-derived obligation with no live source discharges off it.
/// (The teardown gate also forbids tearing the target down WHILE it owes, so this scenario reaches
/// an unhosted-then-restored target only after the obligation is off the holder — see below.)
///
/// RED under the old target-scan discipline (no hosted target owes the freeze's gen once the source
/// is gone → no floor): the re-derived obligation survives, the gen-0 recreate re-mints the
/// repeated gen, and the service's own drive thaws the new incarnation's freeze off the dead
/// incarnation's abort (the codex-R4 critical).
#[test]
fn an_unhosted_targets_rederived_obligation_discharges_off_the_sources_own_floor() {
  let (mut m, base) = merge_host(2, 3);
  let now = Instant::ORIGIN;
  let mut stores = LineageStores {
    inner: base,
    floors: std::collections::BTreeMap::new(),
    lineages: std::collections::BTreeMap::new(),
  };
  // Freeze 1 -> 2 (gen 1) and abort on the target: obligation `abandoned[1] = (1, _)`.
  {
    m.prepare_merge(&1, now, &mut stores, &2).unwrap().unwrap();
    let (log, stable) = stores.inner.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  {
    let (log, stable) = stores.inner.0.get_mut(&2).unwrap();
    m.rollback_merge(&2, now, log, stable, &1).unwrap().unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(m.group(&2).unwrap().has_abandoned());

  // Remove the SOURCE with the unified-counter discipline: its OWN lineage covers the freeze — the
  // removal floor is derived with NO knowledge of any target. Removing the source also PURGES the
  // still-hosted target's LIVE obligation for it (the synchronous fast path), but the target's
  // durable abort entry survives to re-derive it after a restore.
  let fence = m.group_gen(&1).saturating_add(1);
  assert_eq!(fence, 2, "the source's own counter carries the freeze");
  stores.floors.insert(1, fence);
  assert!(m.remove_group(&1, &mut stores).unwrap().is_some());
  stores.inner.0.remove(&1);
  assert!(
    !m.group(&2).unwrap().has_abandoned(),
    "removing the source purged the target's live obligation for it"
  );

  // The target now owes nothing, so the teardown gate admits it (self-cleared); its durable stores
  // survive. RESTORE it later: replaying the committed abort entry RE-DERIVES the obligation, now
  // with NO live source anywhere to observe a thaw.
  assert!(m.remove_group(&2, &mut stores).unwrap().is_some());
  {
    let (log, stable) = stores.inner.0.get_mut(&2).unwrap();
    m.restore_group(
      2,
      single_node_cfg(1),
      now,
      7,
      CountSm::default(),
      1,
      log,
      stable,
    )
    .unwrap();
  }
  assert!(
    m.group(&2).unwrap().has_abandoned(),
    "the restored target re-derived the obligation from its durable abort entry"
  );

  // THE DISCHARGE, off the source's own floor — no target scan produced it, and no live source
  // exists to observe: `!floor_admits(2, 1)` proves the frozen-at-1 incarnation is gone forever.
  m.service_merge_applies(now, &mut stores);
  assert!(
    !m.group(&2).unwrap().has_abandoned(),
    "the re-derived obligation discharged off the source's removal floor"
  );

  // RECREATE the source at its admitted generation and re-freeze the SAME pair: the fresh
  // freeze mints above every generation the dead incarnation ever named.
  stores
    .inner
    .0
    .insert(1, (VecLog::default(), AsyncStable::default()));
  assert!(crate::floor_admits(*stores.floors.get(&1).unwrap(), 2));
  m.create_group(1, 2, single_node_cfg(1), now, 7, CountSm::default())
    .unwrap();
  {
    let (log, stable) = stores.inner.0.get_mut(&1).unwrap();
    let d = m.group(&1).unwrap().poll_timeout().unwrap();
    m.handle_timeout(&1, d, log, stable).unwrap();
    drain_storage(&mut m, 1, d, log, stable);
    assert!(m.group(&1).unwrap().role().is_leader());
  }
  while m.poll_message().is_some() {}
  while m.poll_event().is_some() {}
  {
    m.prepare_merge(&1, now, &mut stores, &2).unwrap().unwrap();
    let (log, stable) = stores.inner.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    m.group(&1).unwrap().is_frozen() && m.group(&1).unwrap().shape_gen() == 3,
    "the recreate's freeze minted above the dead incarnation"
  );

  // NO thaw is backed: the discharged obligation authorizes nothing (UnbackedThaw at the new
  // gen; StaleThaw at the old), and the service leaves the new freeze standing.
  let last_before = stores.inner.0.get(&1).unwrap().0.last_index();
  let unbacked = {
    let (log, stable) = stores.inner.0.get_mut(&1).unwrap();
    m.propose_merge_unfreeze(&1, now, log, stable, &2, 3)
  };
  assert!(
    matches!(unbacked, Some(Err(MergeError::UnbackedThaw))),
    "no committed abort backs the new incarnation's thaw: {unbacked:?}"
  );
  let stale = {
    let (log, stable) = stores.inner.0.get_mut(&1).unwrap();
    m.propose_merge_unfreeze(&1, now, log, stable, &2, 1)
  };
  assert!(
    matches!(
      stale,
      Some(Err(MergeError::StaleThaw {
        expected: 1,
        seen: 3
      }))
    ),
    "the dead incarnation's expected gen is spent: {stale:?}"
  );
  m.service_merge_applies(now, &mut stores);
  {
    let (log, stable) = stores.inner.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert_eq!(
    stores.inner.0.get(&1).unwrap().0.last_index(),
    last_before,
    "nothing appended a thaw"
  );
  assert!(
    m.group(&1).unwrap().is_frozen(),
    "the new incarnation's freeze survives the dead incarnation's abort"
  );
}

/// One driver-shaped storage crank for an engine-hosted group: barrier, then drain completions
/// (the reactor/compio crank loop's shape).
fn engine_crank(
  m: &mut MultiRaft<u64, u64, CountSm>,
  engine: &mut GroupEngine<u64, u64>,
  gid: u64,
  now: Instant,
) {
  for _ in 0..4 {
    engine.flush();
    let Some((log, stable)) = engine.stores(&gid) else {
      break;
    };
    let _ = m.handle_storage(&gid, now, log, stable);
  }
}

/// The drivers' event-pump lineage fold — the mirror discipline under test: every merge lineage
/// event (and an install's monotone catch-up) lands in the engine's per-id record.
fn fold_lineage_events(m: &mut MultiRaft<u64, u64, CountSm>, engine: &mut GroupEngine<u64, u64>) {
  while let Some((g, ev)) = m.poll_event() {
    let lineage_move = match &ev {
      Event::MergeFrozen(f) => f.gen_after(),
      Event::MergeRolledBack(r) => r.gen_after(),
      Event::MergeAborted(a) => a.gen_after(),
      Event::Merged(mg) => mg.gen_after(),
      Event::SnapshotInstalled(meta) => meta.shape_gen(),
      _ => 0,
    };
    if lineage_move > 0 {
      engine.set_group_gen(&g, lineage_move);
    }
  }
}

/// INV-LINEAGE: every hosted group's live counter equals the engine's lineage record once the
/// crank's folds land — the doctrine pin, so the two counters can never silently diverge again.
fn assert_inv_lineage(m: &MultiRaft<u64, u64, CountSm>, engine: &GroupEngine<u64, u64>) {
  for gid in m.group_ids() {
    assert_eq!(
      m.group(gid).unwrap().shape_gen(),
      engine.group_gen(gid),
      "INV-LINEAGE drift on group {gid}: hosted counter != engine record"
    );
  }
}

/// Admit + elect one engine-hosted single-voter group, recording the admitted generation in the
/// engine as the drivers do after a create `Ok`.
fn engine_group(
  m: &mut MultiRaft<u64, u64, CountSm>,
  engine: &mut GroupEngine<u64, u64>,
  gid: u64,
  generation: u64,
  now: Instant,
) {
  assert!(engine.add_group(gid));
  m.create_group(
    gid,
    generation,
    single_node_cfg(1),
    now,
    7,
    CountSm::default(),
  )
  .unwrap();
  engine.set_group_gen(&gid, generation);
  let d = m.group(&gid).unwrap().poll_timeout().unwrap();
  {
    let (log, stable) = engine.stores(&gid).unwrap();
    m.handle_timeout(&gid, d, log, stable).unwrap();
  }
  engine_crank(m, engine, gid, now);
  assert!(m.group(&gid).unwrap().role().is_leader());
}

/// THE INV-LINEAGE PIN: across the whole merge choreography — freeze, target abort, service-driven
/// thaw, re-freeze, parked absorb — every applied lineage move folds into the engine record within
/// its crank, so a hosted group's live counter and the engine's durable lineage NEVER disagree at
/// a crank boundary. This is the doctrine ("one lineage counter, gen ≡ shape gen") pinned as a
/// test: any future lineage move that forgets its mirror trips it immediately.
#[test]
fn hosted_lineage_equals_the_engine_record_after_every_fold() {
  let mut engine: GroupEngine<u64, u64> = GroupEngine::new();
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let now = Instant::ORIGIN;
  engine_group(&mut m, &mut engine, 1, 0, now);
  engine_group(&mut m, &mut engine, 2, 0, now);
  fold_lineage_events(&mut m, &mut engine);
  assert_inv_lineage(&m, &engine);

  // FREEZE 1 -> 2: the source's counter moves; the MergeFrozen fold mirrors it.
  m.prepare_merge(&1, now, &mut engine, &2).unwrap().unwrap();
  engine_crank(&mut m, &mut engine, 1, now);
  fold_lineage_events(&mut m, &mut engine);
  assert!(m.group(&1).unwrap().is_frozen());
  assert_eq!(engine.group_gen(&1), 1, "the freeze mirrored eagerly");
  assert_inv_lineage(&m, &engine);

  // TARGET-role ABORT on 2: the target's counter moves; the MergeAborted fold mirrors it.
  {
    let (log, stable) = engine.stores(&2).unwrap();
    m.rollback_merge(&2, now, log, stable, &1).unwrap().unwrap();
  }
  engine_crank(&mut m, &mut engine, 2, now);
  fold_lineage_events(&mut m, &mut engine);
  assert_eq!(engine.group_gen(&2), 1, "the abort mint mirrored eagerly");
  assert_inv_lineage(&m, &engine);

  // The service drives the SOURCE thaw; the MergeRolledBack fold mirrors it, and the follow-up
  // crank discharges the obligation off the observed advance.
  m.service_merge_applies(now, &mut engine);
  engine_crank(&mut m, &mut engine, 1, now);
  fold_lineage_events(&mut m, &mut engine);
  assert!(!m.group(&1).unwrap().is_frozen(), "thawed");
  assert_eq!(engine.group_gen(&1), 2, "the thaw mirrored eagerly");
  assert_inv_lineage(&m, &engine);
  m.service_merge_applies(now, &mut engine);
  // The observing leader deferred its clear to the witness — apply it on the holder (no lineage move).
  engine_crank(&mut m, &mut engine, 2, now);
  assert!(!m.group(&2).unwrap().has_abandoned());

  // RE-FREEZE and ABSORB: park, seal, resolve — the Merged fold mirrors the target's bump.
  m.prepare_merge(&1, now, &mut engine, &2).unwrap().unwrap();
  engine_crank(&mut m, &mut engine, 1, now);
  fold_lineage_events(&mut m, &mut engine);
  assert_inv_lineage(&m, &engine);
  {
    let (log, stable) = engine.stores(&2).unwrap();
    m.commit_merge(&2, now, log, stable, &1).unwrap().unwrap();
  }
  engine_crank(&mut m, &mut engine, 2, now);
  assert!(m.group(&2).unwrap().pending_merge().is_some(), "parked");
  // First service pass seals the abort window; the crank commits the seal; the second resolves.
  let mut resolutions = m.service_merge_applies(now, &mut engine);
  engine_crank(&mut m, &mut engine, 2, now);
  resolutions.extend(m.service_merge_applies(now, &mut engine));
  assert_eq!(
    resolutions,
    std::vec![MergeResolution::Merged {
      source: 1,
      target: 2
    }]
  );
  // The driver's storage half of a Merged resolution, then the fold: floor + teardown.
  engine.set_group_floor(&1, MERGED_FLOOR);
  engine.remove_group(&1);
  engine_crank(&mut m, &mut engine, 2, now);
  fold_lineage_events(&mut m, &mut engine);
  assert!(!m.contains_group(&1), "the source was absorbed");
  assert_eq!(engine.group_gen(&2), 2, "the absorb mirrored eagerly");
  assert_inv_lineage(&m, &engine);
}

/// THE CRASH-WINDOW HEAL (restore re-sync): a freeze applies and the crash eats its engine
/// mirror — the staged lineage record never reached a barrier, and restart replay surfaces NO
/// events to re-fold. The drivers' restore re-sync (`set_group_gen(gid, live counter)` after the
/// restore) heals the record from the replayed endpoint state, so the ordinary removal floor
/// still lands above every generation the lost mirror covered.
#[test]
fn a_lost_freeze_mirror_heals_on_the_restore_resync() {
  let mut engine: GroupEngine<u64, u64> = GroupEngine::new();
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let now = Instant::ORIGIN;
  engine_group(&mut m, &mut engine, 1, 0, now);
  engine_group(&mut m, &mut engine, 2, 0, now);
  m.prepare_merge(&1, now, &mut engine, &2).unwrap().unwrap();
  engine_crank(&mut m, &mut engine, 1, now);
  assert!(m.group(&1).unwrap().is_frozen());
  // THE CRASH: the freeze is durable in the source's log, but its event-time mirror is LOST —
  // the events die undrained with the process, the engine record still reads 0.
  drop(m);
  assert_eq!(engine.group_gen(&1), 0, "the mirror never landed");

  // RESTORE from the surviving engine stores; replay re-freezes the endpoint (no events), and
  // the driver's re-sync folds the LIVE counter back into the engine record.
  let mut m2: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let epoch = engine.next_boot_epoch(&1).unwrap();
  {
    let (log, stable) = engine.stores(&1).unwrap();
    m2.restore_group(
      1,
      single_node_cfg(1),
      now,
      7,
      CountSm::default(),
      epoch,
      log,
      stable,
    )
    .unwrap();
  }
  let live = m2.group(&1).unwrap().shape_gen();
  assert_eq!(live, 1, "replay re-derived the freeze bump");
  engine.set_group_gen(&1, live);
  assert_eq!(
    m2.group(&1).unwrap().shape_gen(),
    engine.group_gen(&1),
    "the re-sync healed INV-LINEAGE across the crash"
  );

  // The removal now floors ABOVE the obligation's expected gen (1) — the crash window is shut.
  assert_eq!(engine.removal_floor(&1), 2);
}

/// THE STORES-ONLY CRASH WINDOW (the ceiling helper's log and meta legs): the group is never
/// re-hosted after the crash, so no restore re-sync runs — the removal must derive its floor
/// from the stores alone. Every lineage move rides the group's own log (the shape-kind entries
/// carry the generation they set) or its snapshot meta, so the ceiling covers the un-mirrored
/// freeze with no target knowledge. RED under the record-only discipline (`group_gen + 1` with
/// a lost mirror reads 0 → no floor at all).
#[test]
fn removal_floor_reads_the_stores_when_the_mirror_never_landed() {
  let mut engine: GroupEngine<u64, u64> = GroupEngine::new();
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let now = Instant::ORIGIN;
  engine_group(&mut m, &mut engine, 1, 0, now);
  engine_group(&mut m, &mut engine, 2, 0, now);
  m.prepare_merge(&1, now, &mut engine, &2).unwrap().unwrap();
  engine_crank(&mut m, &mut engine, 1, now);
  assert!(m.group(&1).unwrap().is_frozen());
  drop(m);
  assert_eq!(engine.group_gen(&1), 0, "the mirror never landed");

  // THE LOG LEG: the durable PrepareMerge entry names source_gen_after = 1 → ceiling 1 → floor 2.
  assert_eq!(
    engine.removal_floor(&1),
    2,
    "the log scan floors one past the un-mirrored freeze"
  );

  // THE META LEG: once compaction folds the entries away, the snapshot meta carries the lineage
  // (every capture stamps the live counter) — model a post-freeze snapshot boundary and re-check.
  {
    let (log, stable) = engine.stores(&1).unwrap();
    let last = log.last_index();
    let meta = crate::SnapshotMeta::new(
      last,
      Term::new(1),
      crate::ConfState::from_voters(std::vec![1u64]),
    )
    .with_shape_gen(1);
    crate::StableStore::submit_snapshot(stable, OpId::first_of_epoch(9), meta, fork_blob(0));
    log.restore(last, Term::new(1));
  }
  assert_eq!(
    engine.removal_floor(&1),
    2,
    "the meta leg floors one past the un-mirrored freeze once the log is compacted away"
  );
}

/// A committed abort must thaw its frozen source even across source-leader churn. While the source
/// has NO leader the service's thaw drive refuses (`NotLeader`) and appends nothing, so the durable
/// `abandoned` obligation must STAY SET — dropping it would wedge the source frozen forever once a
/// leader is later elected. Once a source leader exists the service lands the thaw, and the observed
/// advance discharges the obligation.
#[test]
fn abort_relay_survives_a_leaderless_source() {
  let (mut m, mut stores) = merge_host(2, 3);
  let now = Instant::ORIGIN;
  let _k = freeze_and_park(&mut m, &mut stores);
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.rollback_merge(&2, now, log, stable, &1).unwrap().unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert_eq!(
    m.service_merge_applies(now, &mut stores),
    std::vec![MergeResolution::Aborted {
      source: 1,
      target: 2
    }]
  );
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    m.group(&1).unwrap().is_frozen() && m.group(&2).unwrap().has_abandoned(),
    "the source is frozen and the obligation is recorded"
  );

  // Step the source leader down: the service's thaw drive now refuses `NotLeader` and appends
  // nothing, so the obligation is RETAINED for a later crank rather than consumed-and-lost.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    step_down(&mut m, 1, log, stable);
  }
  m.service_merge_applies(now, &mut stores);
  assert!(
    m.group(&1).unwrap().is_frozen() && m.group(&2).unwrap().has_abandoned(),
    "a leaderless source keeps the obligation — nothing thawed, nothing dropped"
  );

  // A source leader now exists: the service lands the thaw (appended + applied), and the next crank
  // OBSERVES the advance and discharges the obligation — not permanently wedged.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    re_elect(&mut m, 1, log, stable);
  }
  m.service_merge_applies(now, &mut stores);
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    !m.group(&1).unwrap().is_frozen(),
    "the service-driven thaw unfroze the source once a leader existed"
  );
  m.service_merge_applies(now, &mut stores);
  // The observing leader deferred its clear to the witness — apply it on the holder.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    !m.group(&2).unwrap().has_abandoned(),
    "the observed source advance discharged the obligation"
  );
}

/// The negative pin: a normal abort with a LIVE source leader thaws via the service, and discharge is
/// by OBSERVATION — the append alone does not clear `abandoned`; the SUBSEQUENT crank that observes
/// the source past the freeze does. No infinite retry: the observed advance is terminal.
#[test]
fn accepted_thaw_retires_on_the_observed_advance() {
  let (mut m, mut stores) = merge_host(2, 3);
  let now = Instant::ORIGIN;
  let _k = freeze_and_park(&mut m, &mut stores);
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.rollback_merge(&2, now, log, stable, &1).unwrap().unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  m.service_merge_applies(now, &mut stores);
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    m.group(&2).unwrap().has_abandoned(),
    "the abort recorded the obligation"
  );
  // The service DRIVES the thaw: it APPENDS the source-side RollbackMerge on the source leader's
  // log. The append alone does NOT discharge — the obligation is still set right after.
  m.service_merge_applies(now, &mut stores);
  assert!(
    m.group(&2).unwrap().has_abandoned(),
    "the append is only a leg of delivery — the obligation is not yet discharged"
  );
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    !m.group(&1).unwrap().is_frozen(),
    "the thaw committed+applied on the first drive"
  );
  // The next crank OBSERVES the source past the freeze (seen > expected) and DISCHARGES — terminal,
  // no infinite retry.
  m.service_merge_applies(now, &mut stores);
  // The observing leader deferred its clear to the witness — apply it on the holder.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    !m.group(&2).unwrap().has_abandoned(),
    "the observed advance discharged the obligation — no infinite requeue"
  );
}

/// THE INCARNATION GATE (the stale-obligation hazard): a thaw drive bound to the freeze generation it
/// authorized must thaw NOTHING against a source RE-FROZEN at a new generation. A peer's thaw lands and
/// the service discharges the obligation — which the abort-pending gate REQUIRES before the pair may
/// re-freeze; the SAME source→target pair then FREEZES AGAIN at a new gen with its own parked commit,
/// and a stale drive naming the OLD gen (1) is issued against a source now at gen 3. The `seen >
/// expected` guard refuses it TERMINALLY (`StaleThaw`) and the new freeze survives. Neuter the guard
/// and this regresses — the stale drive thaws the new freeze (`is_frozen()` flips), aborting the new
/// merge out of order.
#[test]
fn stale_abort_relay_does_not_thaw_a_refrozen_source() {
  let (mut m, mut stores) = merge_host(2, 3);
  let now = Instant::ORIGIN;
  freeze_and_park(&mut m, &mut stores);
  // Abort the first merge: the target's log lands the abort in the park's window, the park
  // resolves aborted, and the resumed drain records the source's thaw obligation (recorded gen 1).
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.rollback_merge(&2, now, log, stable, &1).unwrap().unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert_eq!(
    m.service_merge_applies(now, &mut stores),
    std::vec![MergeResolution::Aborted {
      source: 1,
      target: 2
    }]
  );
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert_eq!(
    m.group(&2)
      .unwrap()
      .abandoned_obligations()
      .first()
      .map(|(_, g, _)| *g),
    Some(1),
    "the obligation is recorded at gen 1"
  );

  // Another host's thaw lands here at the recorded generation and unfreezes the source (driven
  // DIRECTLY, modelling a peer). The service then OBSERVES the advance and discharges the
  // obligation — the abort-pending gate REQUIRES this discharge before the pair may re-freeze.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.propose_merge_unfreeze(&1, now, log, stable, &2, 1)
      .unwrap()
      .unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    !m.group(&1).unwrap().is_frozen(),
    "the original thaw landed"
  );
  assert_eq!(
    m.group(&1).unwrap().shape_gen(),
    2,
    "0 -> 1 freeze -> 2 thaw"
  );
  m.service_merge_applies(now, &mut stores);
  // The observing leader deferred its clear to the witness — apply it on the holder.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    !m.group(&2).unwrap().has_abandoned(),
    "the observed advance discharged the obligation, freeing the target to absorb again"
  );

  // The SAME pair freezes AGAIN — a brand-new merge with its own parked commit at a new gen, admitted
  // only because the prior obligation discharged above (the abort-pending gate).
  {
    m.prepare_merge(&1, now, &mut stores, &2).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    m.group(&1).unwrap().is_frozen(),
    "re-frozen for a new merge"
  );
  assert_eq!(m.group(&1).unwrap().shape_gen(), 3, "2 -> 3 re-freeze");
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.commit_merge(&2, now, log, stable, &1).unwrap().unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    m.group(&2).unwrap().pending_merge().is_some(),
    "the new merge parks a commit expecting the new generation"
  );

  // THE GATE DIRECTLY: a retained/relayed thaw drive naming the OLD generation (1) against a source
  // now at gen 3 is TERMINAL `StaleThaw` and thaws nothing — the new freeze must survive.
  let result = {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    let r = m.propose_merge_unfreeze(&1, now, log, stable, &2, 1);
    drain_storage(&mut m, 1, now, log, stable);
    r
  };
  assert!(
    matches!(
      result,
      Some(Err(MergeError::StaleThaw {
        expected: 1,
        seen: 3
      }))
    ),
    "the stale obligation is a spent authorization, not a thaw: {result:?}"
  );
  assert!(
    m.group(&1).unwrap().is_frozen(),
    "the new freeze still stands — the stale obligation did not thaw it"
  );
  assert_eq!(
    m.group(&1).unwrap().shape_gen(),
    3,
    "the source is not moved past the new park's expected generation"
  );
  assert!(
    m.group(&2).unwrap().pending_merge().is_some(),
    "the new merge's park is intact"
  );
}

/// The incarnation gate's TRANSIENT arm: a drive naming a freeze generation the local source leader
/// has not APPLIED yet (its lineage sits below the recorded generation — a fresh leader mid-catch-up)
/// refuses with `SourceBehindFreeze` and appends nothing. The service keeps the obligation set
/// through it (no discharge, no thaw), distinguished from the terminal `StaleThaw` the sibling pins.
#[test]
fn thaw_behind_freeze_generation_is_transient() {
  let (mut m, mut stores) = merge_host(2, 3);
  let now = Instant::ORIGIN;
  freeze_and_park(&mut m, &mut stores);
  // The source leads, frozen at gen 1; a drive naming gen 2 sits ahead of its applied lineage.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    let r = m.propose_merge_unfreeze(&1, now, log, stable, &2, 2);
    assert!(
      matches!(
        r,
        Some(Err(MergeError::SourceBehindFreeze {
          expected: 2,
          seen: 1
        }))
      ),
      "behind the recorded freeze generation: {r:?}"
    );
  }
  assert!(
    m.group(&1).unwrap().is_frozen(),
    "nothing appended while behind the generation"
  );
}

/// THE COMMITTED-BUT-UNAPPLIED WEDGE: the gate driven while the source LEADER holds its freeze
/// committed but NOT yet applied (`freeze_pending` armed at append, `is_frozen() == false`, the
/// lineage still below the abort's recorded generation). The abort was minted off a colocated
/// replica that already applied the freeze, so the recorded generation is `1` while this fresh
/// leader still reads `0`. It must answer TRANSIENT `SourceBehindFreeze` (the service keeps the
/// obligation set through it) — the prior gate read only the applied bit and answered terminal
/// `NotFrozen`, after which the leader applied the freeze and stayed frozen with no way to thaw it
/// (a permanent frozen-source wedge). Then the leader applies the freeze and a later thaw lands.
#[test]
fn committed_but_unapplied_freeze_thaw_is_retained_not_dropped() {
  let (mut m, mut stores) = merge_host(2, 3);
  let now = Instant::ORIGIN;
  // Drive the source into the committed-but-unapplied SIGNATURE: the `PrepareMerge` is on the
  // leader's log (the append arms `freeze_pending`) but deliberately NOT drained, so it has not
  // folded — `is_frozen() == false`, the lineage still pre-freeze. The relay classifier reads
  // exactly these predicates; it never consults commit or persistence, so this is the endpoint a
  // freshly elected source leader presents while its apply trails the abort's recorded freeze.
  {
    m.prepare_merge(&1, now, &mut stores, &2).unwrap().unwrap();
  }
  {
    let src = m.group(&1).unwrap();
    assert!(
      src.merge_freeze_active(),
      "the freeze is pending on the log"
    );
    assert!(!src.is_frozen(), "but not yet applied");
    assert_eq!(
      src.shape_gen(),
      0,
      "the lineage has not reached the freeze gen"
    );
  }

  // RED (pre-fix): the gate answered terminal `NotFrozen`; it must instead answer transient
  // `SourceBehindFreeze` so the committed abort's obligation can still thaw the source later.
  let result = {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.propose_merge_unfreeze(&1, now, log, stable, &2, 1)
  };
  assert!(
    matches!(
      result,
      Some(Err(MergeError::SourceBehindFreeze {
        expected: 1,
        seen: 0
      }))
    ),
    "a committed-but-unapplied freeze at gen < expected is behind, not NotFrozen: {result:?}"
  );
  {
    // Nothing was appended while behind — the freeze signature is unchanged.
    let src = m.group(&1).unwrap();
    assert!(src.merge_freeze_active() && !src.is_frozen());
    assert_eq!(src.shape_gen(), 0);
  }

  // GREEN: the leader applies the freeze, and the retained obligation's thaw now lands.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(m.group(&1).unwrap().is_frozen(), "the freeze folded");
  assert_eq!(
    m.group(&1).unwrap().shape_gen(),
    1,
    "the lineage is now at the freeze gen"
  );
  // The committed target abort backs the thaw — the obligation the SourceBehindFreeze retention
  // exists to protect: target 2 abandons source 1's frozen merge, recording `abandoned` for (1, 1).
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.rollback_merge(&2, now, log, stable, &1).unwrap().unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  let result = {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    let r = m.propose_merge_unfreeze(&1, now, log, stable, &2, 1);
    drain_storage(&mut m, 1, now, log, stable);
    r
  };
  assert!(
    matches!(result, Some(Ok(_))),
    "the thaw lands once the freeze is applied: {result:?}"
  );
  assert!(
    !m.group(&1).unwrap().is_frozen(),
    "the source thawed — never permanently wedged"
  );
  assert_eq!(
    m.group(&1).unwrap().shape_gen(),
    2,
    "0 -> 1 freeze -> 2 thaw"
  );
}

/// The INCARNATION GATE is EXHAUSTIVE over every frozen-source state, pinning each row of the
/// generation-bound table to its verdict. Completeness is the property under test — no reachable
/// frozen-source or freeze-pending state may map to the wrong verdict — so this walks the whole
/// table in the gate's order: the terminal-dedupe (`StaleThaw`, `NotFrozen`) precedes the leadership
/// gate, and the accept arm APPENDS (idempotently) without asserting delivery. The container's
/// service discharges the abort obligation by OBSERVING the source past the freeze, not by
/// classifying this verdict — so the gate's job is only to append correctly and refuse a re-frozen
/// pair, which every row below pins.
#[test]
fn frozen_source_relay_classification_is_exhaustive() {
  let (mut m, mut stores) = merge_host(2, 3);
  let now = Instant::ORIGIN;

  // --- unhosted source (`None`): the source is not on this host to thaw. ---
  let unhosted = {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.propose_merge_unfreeze(&999, now, log, stable, &1, 1)
  };
  assert!(unhosted.is_none(), "an unhosted source is None");

  // --- a never-frozen source is NotFrozen at and below the named gen, BEFORE the leadership gate —
  //     a leader (group 2) and an un-elected follower (group 3) both answer NotFrozen, since the
  //     terminal-dedupe does not depend on role. ---
  m.create_group(3, 0, single_node_cfg(1), now, 7, CountSm::default())
    .unwrap();
  for (gid, expected, why) in [
    (2u64, 0u64, "leader, seen == expected, not frozen"),
    (2, 5, "leader, seen < expected, no freeze"),
    (3, 1, "un-elected follower, seen < expected, no freeze"),
  ] {
    let r = {
      let (log, stable) = stores.0.get_mut(&2).unwrap();
      m.propose_merge_unfreeze(&gid, now, log, stable, &1, expected)
    };
    assert!(
      matches!(r, Some(Err(MergeError::NotFrozen))),
      "never-frozen ({why}) is NotFrozen: {r:?}"
    );
  }

  // --- the committed-but-unapplied freeze (freeze-pending, seen < expected): SourceBehindFreeze,
  //     the catch-up the service keeps `abandoned` set through. ---
  {
    m.prepare_merge(&1, now, &mut stores, &2).unwrap().unwrap();
  }
  let pending_behind = {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.propose_merge_unfreeze(&1, now, log, stable, &2, 1)
  };
  assert!(
    matches!(
      pending_behind,
      Some(Err(MergeError::SourceBehindFreeze {
        expected: 1,
        seen: 0
      }))
    ),
    "freeze-pending below the gen is SourceBehindFreeze: {pending_behind:?}"
  );
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }

  // Group 1 is now frozen-applied at gen 1 for target 2.
  assert!(m.group(&1).unwrap().is_frozen() && m.group(&1).unwrap().shape_gen() == 1);

  // --- applied freeze BELOW a later-named gen (mid-catch-up): SourceBehindFreeze. ---
  let applied_behind = {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.propose_merge_unfreeze(&1, now, log, stable, &2, 2)
  };
  assert!(
    matches!(
      applied_behind,
      Some(Err(MergeError::SourceBehindFreeze {
        expected: 2,
        seen: 1
      }))
    ),
    "applied freeze below the gen is SourceBehindFreeze: {applied_behind:?}"
  );

  // --- frozen at the EXACT incarnation but NOT the leader — the genuine NotLeader row (a follower
  //     frozen at `expected` reaches the leadership gate, having passed the terminal-dedupe). Step
  //     group 1 down, then re-elect it for the rows below. ---
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    step_down(&mut m, 1, log, stable);
  }
  let not_leader = {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.propose_merge_unfreeze(&1, now, log, stable, &2, 1)
  };
  assert!(
    matches!(not_leader, Some(Err(MergeError::NotLeader { .. }))),
    "a follower frozen at the exact incarnation is NotLeader: {not_leader:?}"
  );
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    re_elect(&mut m, 1, log, stable);
  }
  assert!(m.group(&1).unwrap().is_frozen() && m.group(&1).unwrap().shape_gen() == 1);

  // --- the exact incarnation claimed by a DIFFERENT target: SourceClaimed — a relay riding a
  //     foreign target's abort must not thaw it. ---
  let claimed = {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.propose_merge_unfreeze(&1, now, log, stable, &9, 1)
  };
  assert!(
    matches!(claimed, Some(Err(MergeError::SourceClaimed))),
    "the exact incarnation claimed elsewhere is SourceClaimed: {claimed:?}"
  );

  // Back the ACCEPT rows with a committed target abort — the derived-from-abort gate authorizes a
  // thaw ONLY when the claimed target owes this obligation: target 2 abandons source 1 at gen 1.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.rollback_merge(&2, now, log, stable, &1).unwrap().unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }

  // --- ACCEPT: the exact incarnation with the matching claim APPENDS the thaw. IDEMPOTENT: a
  //     second drive while the thaw is in flight appends NO duplicate (the `thaw_in_flight` guard). ---
  let accepted = {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.propose_merge_unfreeze(&1, now, log, stable, &2, 1)
  };
  assert!(
    matches!(accepted, Some(Ok(_))),
    "the exact incarnation with the matching claim appends the thaw: {accepted:?}"
  );
  let after_append = stores.0.get(&1).unwrap().0.last_index();
  let in_flight = {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.propose_merge_unfreeze(&1, now, log, stable, &2, 1)
  };
  assert!(
    matches!(in_flight, Some(Ok(_))),
    "a thaw already in flight is a no-op Ok: {in_flight:?}"
  );
  assert_eq!(
    stores.0.get(&1).unwrap().0.last_index(),
    after_append,
    "the idempotent guard appended no duplicate RollbackMerge"
  );
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(!m.group(&1).unwrap().is_frozen() && m.group(&1).unwrap().shape_gen() == 2);

  // --- advanced PAST the named incarnation (the delivered thaw is OBSERVED): StaleThaw, the
  //     incarnation gate's terminal refusal — leadership-independent, so the service's discharge
  //     check reads the same source advance to clear `abandoned` on every host. ---
  let stale = {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.propose_merge_unfreeze(&1, now, log, stable, &2, 1)
  };
  assert!(
    matches!(
      stale,
      Some(Err(MergeError::StaleThaw {
        expected: 1,
        seen: 2
      }))
    ),
    "a source advanced past the gen is StaleThaw: {stale:?}"
  );
}

/// RED-first for the [MEDIUM] finding: a FOLLOWER host that OBSERVES the source past the freeze must
/// discharge its own obligation — the gate answers terminal `StaleThaw` — WITHOUT ever leading. The
/// source is thawed here (modelling another host's leader delivering the thaw, `seen == 2`), then
/// this host's source replica is stepped down to a follower. Pre-reorder the `NotLeader` check
/// shadowed the lineage dedupe, so a follower answered `NotLeader` (transient) forever and could
/// never discharge; the reorder puts the observed-advance verdict BEFORE the leadership gate — the
/// same advance the service's leadership-independent discharge check reads.
#[test]
fn a_follower_retires_the_relay_on_the_observed_advance() {
  let (mut m, mut stores) = merge_host(2, 3);
  let now = Instant::ORIGIN;
  // Freeze group 1 for target 2 (seen == 1), then thaw it as leader so the source lineage advances
  // PAST the freeze (seen == 2) — the delivery this host will merely OBSERVE. A committed target
  // abort backs the thaw (target 2 abandons source 1 at gen 1).
  {
    m.prepare_merge(&1, now, &mut stores, &2).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.rollback_merge(&2, now, log, stable, &1).unwrap().unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.propose_merge_unfreeze(&1, now, log, stable, &2, 1)
      .unwrap()
      .unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(!m.group(&1).unwrap().is_frozen() && m.group(&1).unwrap().shape_gen() == 2);
  // This host's source replica is now a FOLLOWER; it never leads the thaw.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    step_down(&mut m, 1, log, stable);
  }
  let result = {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.propose_merge_unfreeze(&1, now, log, stable, &2, 1)
  };
  assert!(
    m.group(&1).unwrap().role().is_follower(),
    "the host never led"
  );
  assert!(
    matches!(
      result,
      Some(Err(MergeError::StaleThaw {
        expected: 1,
        seen: 2
      }))
    ),
    "a follower observing the advance is StaleThaw, not NotLeader: {result:?}"
  );
}

/// RED-first for the [HIGH] finding: an appended thaw is only APPENDED, not delivered — a source
/// leader that appends the thaw then loses leadership before it commits has that entry TRUNCATED by
/// the next leader. A design that treated the append as delivered would drop the obligation and the
/// committed abort would have no path left to thaw — the source wedged frozen. The durable obligation
/// PERSISTS across the truncation, a new source leader re-appends (the `become_leader` guard reset
/// frees it), the thaw commits and the lineage advances, and only THEN — on the observed advance —
/// does the gate answer `StaleThaw` (the service's discharge signal) on every host.
#[test]
fn a_truncated_thaw_is_retained_and_re_driven() {
  let (mut m, mut stores) = merge_host(2, 3);
  let now = Instant::ORIGIN;
  // Freeze group 1 for target 2 (seen == 1), leader.
  {
    m.prepare_merge(&1, now, &mut stores, &2).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(m.group(&1).unwrap().is_frozen() && m.group(&1).unwrap().shape_gen() == 1);
  // A committed target abort backs every thaw drive below (target 2 abandons source 1 at gen 1); it
  // persists across the truncation, so the re-driven thaw stays authorized until it finally commits.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.rollback_merge(&2, now, log, stable, &1).unwrap().unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  let leader_term = m.group(&1).unwrap().term();

  // ACCEPT: append the thaw but do NOT commit it (leave it durable-pending, uncommitted).
  let (thaw_index, appended) = {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    let r = m.propose_merge_unfreeze(&1, now, log, stable, &2, 1);
    (log.last_index(), r)
  };
  assert!(
    matches!(appended, Some(Ok(_))),
    "the thaw appended: {appended:?}"
  );
  // An appended-but-uncommitted thaw is only a LEG of delivery: `abandoned` stays set (the service
  // re-drives it), because a leadership loss can still truncate it before it commits.
  assert!(
    matches!(appended, Some(Ok(_))),
    "the thaw appended: {appended:?}"
  );
  assert!(
    m.group(&1).unwrap().is_frozen(),
    "still frozen — the thaw has not committed"
  );

  // LEADERSHIP LOSS + §5.3 TRUNCATION: a new leader at a higher term overwrites the uncommitted
  // thaw at its index. The applied freeze below it survives (seen stays 1).
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    let higher = Term::new(leader_term.get() + 5);
    let prev = Index::new(thaw_index.get() - 1);
    let replace = crate::Entry::new(higher, thaw_index, crate::EntryKind::Normal, {
      let mut b = Vec::new();
      Bytes::from_static(b"x").encode(&mut b);
      Bytes::from(b)
    });
    m.handle_message(
      &1,
      now,
      log,
      stable,
      2u64,
      Message::AppendEntries(crate::AppendEntries::new(
        higher,
        2u64,
        prev,
        leader_term,
        std::vec![replace],
        Index::ZERO,
      )),
    )
    .unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    m.group(&1).unwrap().role().is_follower(),
    "the higher term stepped the leader down"
  );
  assert!(
    m.group(&1).unwrap().is_frozen() && m.group(&1).unwrap().shape_gen() == 1,
    "the freeze survived the truncation; the thaw is gone"
  );

  // As a follower, the drive is transient `NotLeader` — the obligation is held, not delivered.
  let as_follower = {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.propose_merge_unfreeze(&1, now, log, stable, &2, 1)
  };
  assert!(
    matches!(as_follower, Some(Err(MergeError::NotLeader { .. }))),
    "the follower cannot append — the obligation is held: {as_follower:?}"
  );

  // A NEW source leader re-appends the thaw (the `become_leader` reset frees the guard) and commits
  // it: the lineage advances past the freeze.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    re_elect(&mut m, 1, log, stable);
  }
  let reappended = {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    let r = m.propose_merge_unfreeze(&1, now, log, stable, &2, 1);
    drain_storage(&mut m, 1, now, log, stable);
    r
  };
  assert!(
    matches!(reappended, Some(Ok(_))),
    "the new leader re-appends the thaw: {reappended:?}"
  );
  assert!(
    !m.group(&1).unwrap().is_frozen() && m.group(&1).unwrap().shape_gen() == 2,
    "the re-driven thaw committed and delivered — the source is not wedged"
  );

  // Every host now OBSERVES the advance — the gate refuses terminally (StaleThaw), and the service's
  // discharge check reads the same advance to clear `abandoned`.
  let retired = {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.propose_merge_unfreeze(&1, now, log, stable, &2, 1)
  };
  assert!(
    matches!(
      retired,
      Some(Err(MergeError::StaleThaw {
        expected: 1,
        seen: 2
      }))
    ),
    "the observed advance is StaleThaw: {retired:?}"
  );
}

/// The canonical `Data` encoding of a `u64` group id — the bytes the container keys an `abandoned`
/// obligation and a `ThawDischarged` witness on, so a test injects an obligation and mints a witness
/// against the SAME key the apply's gen-exact clear compares.
fn gid_key(g: u64) -> Bytes {
  let mut v = Vec::new();
  Data::encode(&g, &mut v);
  Bytes::from(v)
}

/// Count the `ThawDischarged` witnesses on a log — the mint/idempotence pins' assertion.
fn witness_count(log: &VecLog) -> usize {
  let last = log.last_index();
  match log.entries(Index::new(1)..last.next(), u64::MAX) {
    Ok(crate::EntriesRead::Ready(es)) => es
      .iter()
      .filter(|e| e.kind() == EntryKind::ThawDischarged)
      .count(),
    _ => 0,
  }
}

/// A container hosting ONLY target group 2 (single-voter leader) that owes source 1 a thaw at freeze
/// generation `gen` — the obligation injected directly (source 1 never hosted here), the dead-end
/// shape the witness closes. Returns the container, its store seam (whose floor/terminal set the pins
/// tune), and the source key.
fn target_only_owing(generation: u64) -> (MultiRaft<u64, u64, CountSm>, MapStores, Bytes) {
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let mut stores = MapStores(
    std::collections::BTreeMap::new(),
    std::collections::BTreeSet::new(),
  );
  stores
    .0
    .insert(2, (VecLog::default(), AsyncStable::default()));
  m.create_group(
    2,
    0,
    single_node_cfg(1),
    Instant::ORIGIN,
    7,
    CountSm::default(),
  )
  .unwrap();
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    let d = m.group(&2).unwrap().poll_timeout().unwrap();
    m.handle_timeout(&2, d, log, stable).unwrap();
    drain_storage(&mut m, 2, d, log, stable);
    assert!(m.group(&2).unwrap().role().is_leader());
  }
  while m.poll_message().is_some() {}
  while m.poll_event().is_some() {}
  let source_key = gid_key(1);
  m.group_mut(&2)
    .unwrap()
    .note_abandoned(source_key.clone(), generation, Index::new(1));
  assert!(m.group(&2).unwrap().has_abandoned());
  (m, stores, source_key)
}

/// PIN (a): the dead-end obligation class the witness exists to close. Target 2 owes an UNHOSTED
/// source (floor 0, lineage 0) a thaw — the container can neither observe its advance nor drive its
/// thaw, so the pass leaves the obligation forever and mints NOTHING (no global proof). A committed
/// `ThawDischarged` (minted by an observer elsewhere, replicated here) is the only thing that clears
/// it — pre-mechanism, this replica wedged permanently.
#[test]
fn a_witness_apply_clears_an_unobservable_dead_end_obligation() {
  let (mut m, mut stores, source_key) = target_only_owing(5);
  let now = Instant::ORIGIN;
  // THE WEDGE: the pass cannot discharge (unhosted, floor 0, lineage 0 — no proof anywhere) nor mint
  // (no global proof), and the drive finds no local source. The obligation persists across cranks.
  for _ in 0..8 {
    m.service_merge_applies(now, &mut stores);
  }
  assert!(
    m.group(&2).unwrap().has_abandoned(),
    "no proof anywhere — the obligation wedges"
  );
  assert_eq!(
    witness_count(&stores.0.get(&2).unwrap().0),
    0,
    "a replica with no global proof mints nothing"
  );
  // A witness ARRIVES (append it as replication delivers it, then commit + apply).
  {
    let mut buf = Vec::new();
    crate::wire::encode_thaw_discharged_payload(
      &ThawDischargedPayload::new(source_key.clone(), 5),
      &mut buf,
    );
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.group_mut(&2)
      .unwrap()
      .propose_merge_entry(now, log, EntryKind::ThawDischarged, Bytes::from(buf))
      .unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    !m.group(&2).unwrap().has_abandoned(),
    "the committed witness apply cleared the dead-end obligation"
  );
}

/// PIN (b), the A1 red-proof: a leader whose ONLY proof is a NON-terminal floor clears LOCALLY and
/// mints NO witness. A non-terminal floor is a HOST-LOCAL fact (this host stopped hosting at/below the
/// abandoned gen); witnessing it would clear a LIVE obligation on a co-hosting holder whose source is
/// still frozen, so the mint predicate excludes it — direction matters.
#[test]
fn a1_a_non_terminal_floor_clears_locally_and_mints_no_witness() {
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let mut inner = MapStores(
    std::collections::BTreeMap::new(),
    std::collections::BTreeSet::new(),
  );
  inner
    .0
    .insert(2, (VecLog::default(), AsyncStable::default()));
  m.create_group(
    2,
    0,
    single_node_cfg(1),
    Instant::ORIGIN,
    7,
    CountSm::default(),
  )
  .unwrap();
  let now = Instant::ORIGIN;
  {
    let (log, stable) = inner.0.get_mut(&2).unwrap();
    let d = m.group(&2).unwrap().poll_timeout().unwrap();
    m.handle_timeout(&2, d, log, stable).unwrap();
    drain_storage(&mut m, 2, d, log, stable);
    assert!(m.group(&2).unwrap().role().is_leader());
  }
  while m.poll_message().is_some() {}
  while m.poll_event().is_some() {}
  let source_key = gid_key(1);
  m.group_mut(&2)
    .unwrap()
    .note_abandoned(source_key, 1, Index::new(1));
  // A NON-terminal removal floor above the abandoned gen (source 1 removed at gen 1, floored to 2):
  // it no longer admits gen 1 (local discharge), but it is NOT the terminal MERGED_FLOOR.
  let mut stores = LineageStores {
    inner,
    floors: std::collections::BTreeMap::from([(1u64, 2u64)]),
    lineages: std::collections::BTreeMap::new(),
  };
  m.service_merge_applies(now, &mut stores);
  assert!(
    !m.group(&2).unwrap().has_abandoned(),
    "a non-terminal floor discharges the obligation LOCALLY"
  );
  assert_eq!(
    witness_count(&stores.inner.0.get(&2).unwrap().0),
    0,
    "A1: a non-terminal floor is host-local — it mints NO witness"
  );
}

/// PIN (c): the apply is GEN-EXACT. A witness at generation `g` no-ops against a fresh obligation at
/// `g' != g` (the source re-froze for a new merge) — the stale witness cannot clear the live record.
#[test]
fn a_stale_witness_no_ops_against_a_fresh_obligation() {
  let (mut m, mut stores, source_key) = target_only_owing(9);
  let now = Instant::ORIGIN;
  // Apply a witness for the SAME source at a DIFFERENT (older) generation.
  {
    let mut buf = Vec::new();
    crate::wire::encode_thaw_discharged_payload(
      &ThawDischargedPayload::new(source_key.clone(), 4),
      &mut buf,
    );
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.group_mut(&2)
      .unwrap()
      .propose_merge_entry(now, log, EntryKind::ThawDischarged, Bytes::from(buf))
      .unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    m.group(&2).unwrap().has_abandoned(),
    "the gen-mismatched witness no-op'd — the fresh obligation is untouched"
  );
  assert_eq!(
    m.group(&2)
      .unwrap()
      .abandoned_obligations()
      .first()
      .map(|(_, g, _)| *g),
    Some(9),
    "the live generation stands"
  );
}

/// PIN (d): the mint is IDEMPOTENT. Two cranks with a standing global proof (terminal floor) append
/// exactly ONE witness — the second sees it in flight and holds, mirroring the source-thaw relay's
/// `thaw_in_flight` guard. The committed apply then clears.
#[test]
fn two_cranks_with_a_global_proof_append_one_witness() {
  let (mut m, mut stores, _source_key) = target_only_owing(1);
  let now = Instant::ORIGIN;
  // The terminal floor is the global proof the mint rests on.
  stores.1.insert(1);
  m.service_merge_applies(now, &mut stores);
  m.service_merge_applies(now, &mut stores);
  assert_eq!(
    witness_count(&stores.0.get(&2).unwrap().0),
    1,
    "the in-flight witness is not re-appended crank after crank"
  );
  assert!(
    m.group(&2).unwrap().has_abandoned(),
    "the leader deferred its clear to the witness apply — the obligation is still the re-append trigger"
  );
  // The committed apply clears it, leader included.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    !m.group(&2).unwrap().has_abandoned(),
    "the committed witness discharged the obligation"
  );
}

/// PIN (e): a truncated-uncommitted witness is re-appended by the next observing leader — the
/// `become_leader` witness-guard reset, the exact twin of the source-thaw relay's reseat. The
/// obligation persists across the truncation (durable-derived), so re-drive stays authorized.
#[test]
fn a_truncated_witness_is_re_appended_by_the_next_observing_leader() {
  let (mut m, mut stores, source_key) = target_only_owing(1);
  let now = Instant::ORIGIN;
  stores.1.insert(1); // terminal floor — the global proof.
  let leader_term = m.group(&2).unwrap().term();
  // MINT: append the witness but do NOT commit it (durable-pending, uncommitted).
  let witness_index = {
    let (log, _stable) = stores.0.get_mut(&2).unwrap();
    let r = m.propose_thaw_witness(&2, &source_key, 1, now, log);
    assert!(matches!(r, Some(Ok(_))), "the witness appended: {r:?}");
    stores.0.get(&2).unwrap().0.last_index()
  };
  assert_eq!(witness_count(&stores.0.get(&2).unwrap().0), 1);
  assert!(
    m.group(&2).unwrap().has_abandoned(),
    "obligation held — the witness has not committed"
  );

  // LEADERSHIP LOSS + §5.3 TRUNCATION: a higher-term leader overwrites the uncommitted witness.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    let higher = Term::new(leader_term.get() + 5);
    let prev = Index::new(witness_index.get() - 1);
    let replace = crate::Entry::new(higher, witness_index, crate::EntryKind::Normal, {
      let mut b = Vec::new();
      Bytes::from_static(b"x").encode(&mut b);
      Bytes::from(b)
    });
    m.handle_message(
      &2,
      now,
      log,
      stable,
      2u64,
      Message::AppendEntries(crate::AppendEntries::new(
        higher,
        2u64,
        prev,
        leader_term,
        std::vec![replace],
        Index::ZERO,
      )),
    )
    .unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(m.group(&2).unwrap().role().is_follower());
  assert_eq!(
    witness_count(&stores.0.get(&2).unwrap().0),
    0,
    "the witness was truncated"
  );
  assert!(
    m.group(&2).unwrap().has_abandoned(),
    "the obligation survived the truncation"
  );

  // RE-ELECT + re-drive: the new leader re-appends (the become_leader reset freed the guard), and the
  // committed apply finally discharges the obligation.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    re_elect(&mut m, 2, log, stable);
  }
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    let r = m.propose_thaw_witness(&2, &source_key, 1, now, log);
    assert!(matches!(r, Some(Ok(_))), "the new leader re-appends: {r:?}");
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    !m.group(&2).unwrap().has_abandoned(),
    "the re-driven witness committed and discharged the obligation — not wedged"
  );
}

/// A LATE abort — proposed after the window sealed — no-ops at its stale mint once the merge
/// resolves: nothing un-merges, no thaw obligation. The "commit first" ordering pin: whichever of
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
    !tep.has_abandoned(),
    "a stale abort records no thaw obligation"
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
  // Drop the source through the UNGATED inner teardown: the public gate now refuses to strand a
  // live park (`SpokenFor`/`Frozen`), so this stands in for the churn / never-hosted-source shape
  // the park's absent arm exists to resolve.
  assert!(m.remove_group_inner(&1).is_some());
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
    0,
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
  let payload =
    crate::CommitMergePayload::new(Bytes::from(source_bytes), Index::new(2), Term::new(1), 1, 1);
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
    m.prepare_merge(&1, Instant::ORIGIN, &mut stores, &2)
      .unwrap()
      .unwrap();
    let (log, stable) = stores.0.get_mut(&1).unwrap();
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

/// Restore a host holding a parked single-voter TARGET (its committed log carries a
/// `CommitMerge` naming the freeze identity `(2, boundary_term)` on source group 1, sealed by
/// its own election no-op) and a 3-voter FOLLOWER source replica whose durable log ends with
/// `(boundary_entry)` at index 2 while its commit floor stops at 1 — the lost-final-heartbeat
/// shape's deciding host, distilled: the replica MATCHED the freeze (it holds the entry) but
/// was never TOLD it committed, and no source leader exists anywhere to tell it.
fn wedged_host(boundary_entry: crate::Entry) -> (MultiRaft<u64, u64, CountSm>, MapStores, Index) {
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let mut stores = MapStores(
    std::collections::BTreeMap::new(),
    std::collections::BTreeSet::new(),
  );
  let mut src_log = VecLog::default();
  let mut cmd_buf = Vec::new();
  Data::encode(&Bytes::from_static(&[7u8]), &mut cmd_buf);
  src_log.force_append(&[
    crate::Entry::new(
      Term::new(1),
      Index::new(1),
      crate::EntryKind::Normal,
      Bytes::from(cmd_buf),
    ),
    boundary_entry,
  ]);
  let mut src_stable = crate::testkit::NoopStable::<u64>::default();
  src_stable.force_state(Term::new(2), None, Index::new(1));
  let three = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  m.restore_group(
    1,
    three,
    Instant::ORIGIN,
    7,
    CountSm::default(),
    1,
    &mut src_log,
    &mut src_stable,
  )
  .unwrap();
  {
    let sep = m.group(&1).unwrap();
    assert!(!sep.is_frozen(), "nothing above the commit floor applied");
    assert_eq!(sep.applied_index(), Index::new(1));
  }
  stores.0.insert(1, (src_log, AsyncStable::default()));

  let mut source_bytes = Vec::new();
  Data::encode(&1u64, &mut source_bytes);
  let payload =
    crate::CommitMergePayload::new(Bytes::from(source_bytes), Index::new(2), Term::new(1), 1, 1);
  let mut buf = Vec::new();
  crate::wire::encode_commit_merge_payload(&payload, &mut buf);
  let mut tgt_log = VecLog::default();
  tgt_log.force_append(&[crate::Entry::new(
    Term::new(1),
    Index::new(1),
    crate::EntryKind::CommitMerge,
    Bytes::from(buf),
  )]);
  let mut tgt_stable = crate::testkit::NoopStable::<u64>::default();
  tgt_stable.force_state(Term::new(1), Some(1u64), Index::new(1));
  m.restore_group(
    2,
    single_node_cfg(1),
    Instant::ORIGIN,
    7,
    CountSm::default(),
    1,
    &mut tgt_log,
    &mut tgt_stable,
  )
  .unwrap();
  assert!(m.group(&2).unwrap().pending_merge().is_some(), "re-parked");
  stores.0.insert(2, (tgt_log, AsyncStable::default()));
  // The target elects; its election no-op is the abort window's seal.
  let d = m.group(&2).unwrap().poll_timeout().unwrap();
  let (log, stable) = stores.0.get_mut(&2).unwrap();
  m.handle_timeout(&2, d, log, stable).unwrap();
  drain_storage(&mut m, 2, d, log, stable);
  assert!(m.group(&2).unwrap().role().is_leader());
  (m, stores, Index::new(1))
}

/// THE LOST-FINAL-HEARTBEAT WEDGE: a source follower at match = F but commit < F is legally
/// stranded once the absorb consumes the rest of the source's quorum (the leader-last
/// discipline tears the source leader down only after every peer MATCHED the boundary — match
/// is not commit KNOWLEDGE, and the heartbeat that would have carried the commit index can be
/// lost). Under a pace-only arm this host's park waited forever: leaderless, under-hosted,
/// unelectable, nobody left to raise the stranded replica's commit. The identity leg breaks
/// the wedge: the committed `CommitMerge` proves `(F, freeze_term)` committed in the source,
/// the local log CONTAINS that exact entry, so the host advances its source to the boundary
/// locally and absorbs — the union intact.
#[test]
fn parked_host_advances_a_wedged_source_on_freeze_identity() {
  let mut prep = Vec::new();
  {
    let mut tbytes = Vec::new();
    Data::encode(&2u64, &mut tbytes);
    let p = crate::PrepareMergePayload::new(Bytes::from(tbytes), 1);
    crate::wire::encode_prepare_merge_payload(&p, &mut prep);
  }
  let freeze = crate::Entry::new(
    Term::new(1),
    Index::new(2),
    crate::EntryKind::PrepareMerge,
    Bytes::from(prep),
  );
  let (mut m, mut stores, k) = wedged_host(freeze);

  // First pass: the identity leg advances the stranded source through its own log — commit and
  // apply reach the boundary, the freeze folds, nothing resolves yet.
  assert!(
    m.service_merge_applies(Instant::ORIGIN, &mut stores)
      .is_empty(),
    "the advance pass resolves nothing yet"
  );
  {
    let sep = m.group(&1).unwrap();
    assert!(
      sep.is_frozen(),
      "the identity leg advanced the stranded source to its boundary"
    );
    assert_eq!(sep.applied_index(), Index::new(2));
    assert_eq!(sep.shape_gen(), 1);
  }
  // Second pass: the ordinary resolve arm absorbs (follower-hosted source — no pace leg).
  let resolutions = m.service_merge_applies(Instant::ORIGIN, &mut stores);
  assert_eq!(
    resolutions,
    std::vec![MergeResolution::Merged {
      source: 1,
      target: 2
    }]
  );
  assert!(!m.contains_group(&1), "the source replica was extracted");
  let tep = m.group(&2).unwrap();
  assert!(tep.pending_merge().is_none());
  assert_eq!(tep.applied_index(), k, "the parked entry applied");
  assert_eq!(
    tep.state_machine().count(),
    1,
    "the union carries the source's one committed command"
  );
  assert_eq!(tep.shape_gen(), 1);
  let mut merged = false;
  while let Some((gid, ev)) = m.poll_event() {
    assert!(
      !matches!(ev, Event::MergeAborted(_)),
      "the wedge must resolve to the union, never an abort"
    );
    merged |= gid == 2 && matches!(ev, Event::Merged(_));
  }
  assert!(merged, "Event::Merged surfaced");
}

/// THE NEGATIVE PIN: the identity leg advances on `(index, term)` — never on index alone. A
/// source whose log holds a DIFFERENT entry at the boundary (a divergent uncommitted suffix
/// from a dead leader) proves nothing about its prefix: the park must WAIT (the resolved
/// quorum's post-merge snapshot is its route), and the divergent replica's commit/apply must
/// not move an inch.
#[test]
fn a_divergent_source_boundary_never_advances_on_index_alone() {
  let mut cmd_buf = Vec::new();
  Data::encode(&Bytes::from_static(&[9u8]), &mut cmd_buf);
  let divergent = crate::Entry::new(
    Term::new(2),
    Index::new(2),
    crate::EntryKind::Normal,
    Bytes::from(cmd_buf),
  );
  let (mut m, mut stores, _k) = wedged_host(divergent);
  for _ in 0..5 {
    assert!(
      m.service_merge_applies(Instant::ORIGIN, &mut stores)
        .is_empty(),
      "a divergent boundary entry keeps the park waiting"
    );
  }
  let sep = m.group(&1).unwrap();
  assert!(!sep.is_frozen());
  assert_eq!(
    sep.applied_index(),
    Index::new(1),
    "commit/apply never advanced on index alone"
  );
  assert!(
    m.group(&2).unwrap().pending_merge().is_some(),
    "still parked"
  );
}

/// THE WEDGE THE ADMISSION BARRIER PREVENTS (the log-behind dual of the freeze-identity
/// red-proof): a source replica whose LOG never reached the freeze boundary, cut off from any
/// source leader, can neither advance on identity (its log lacks the boundary entry) nor be
/// snapshotted past it here — so its co-located, log-complete parked target wedges FOREVER. This
/// state is exactly what `commit_merge`'s all-source-voters barrier makes unconstructible: the
/// `CommitMerge` is never proposed while any voter sits below the boundary, so a legitimately
/// admitted merge can never leave a voter in this shape.
#[test]
fn a_log_behind_source_park_never_self_resolves() {
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let mut stores = MapStores(
    std::collections::BTreeMap::new(),
    std::collections::BTreeSet::new(),
  );
  // A 3-voter source whose log ENDS at index 1 — below the freeze boundary at index 2, which it
  // never received. Leaderless (restored, no incoming replication), so it can never catch up.
  let mut src_log = VecLog::default();
  let mut cmd_buf = Vec::new();
  Data::encode(&Bytes::from_static(&[7u8]), &mut cmd_buf);
  src_log.force_append(&[crate::Entry::new(
    Term::new(1),
    Index::new(1),
    crate::EntryKind::Normal,
    Bytes::from(cmd_buf),
  )]);
  let mut src_stable = crate::testkit::NoopStable::<u64>::default();
  src_stable.force_state(Term::new(1), None, Index::new(1));
  let three = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  m.restore_group(
    1,
    three,
    Instant::ORIGIN,
    7,
    CountSm::default(),
    1,
    &mut src_log,
    &mut src_stable,
  )
  .unwrap();
  assert_eq!(m.group(&1).unwrap().applied_index(), Index::new(1));
  stores.0.insert(1, (src_log, AsyncStable::default()));

  // A target parked at a CommitMerge naming the source's boundary at index 2, term 1.
  let mut source_bytes = Vec::new();
  Data::encode(&1u64, &mut source_bytes);
  let payload =
    crate::CommitMergePayload::new(Bytes::from(source_bytes), Index::new(2), Term::new(1), 1, 1);
  let mut buf = Vec::new();
  crate::wire::encode_commit_merge_payload(&payload, &mut buf);
  let mut tgt_log = VecLog::default();
  tgt_log.force_append(&[crate::Entry::new(
    Term::new(1),
    Index::new(1),
    crate::EntryKind::CommitMerge,
    Bytes::from(buf),
  )]);
  let mut tgt_stable = crate::testkit::NoopStable::<u64>::default();
  tgt_stable.force_state(Term::new(1), Some(1u64), Index::new(1));
  m.restore_group(
    2,
    single_node_cfg(1),
    Instant::ORIGIN,
    7,
    CountSm::default(),
    1,
    &mut tgt_log,
    &mut tgt_stable,
  )
  .unwrap();
  assert!(m.group(&2).unwrap().pending_merge().is_some(), "parked");
  stores.0.insert(2, (tgt_log, AsyncStable::default()));
  let d = m.group(&2).unwrap().poll_timeout().unwrap();
  let (log, stable) = stores.0.get_mut(&2).unwrap();
  m.handle_timeout(&2, d, log, stable).unwrap();
  drain_storage(&mut m, 2, d, log, stable);

  // The identity leg cannot advance a log that never reached the boundary; nothing ever resolves.
  for _ in 0..8 {
    assert!(
      m.service_merge_applies(Instant::ORIGIN, &mut stores)
        .is_empty(),
      "a log-behind source can never be advanced or absorbed locally"
    );
  }
  assert_eq!(
    m.group(&1).unwrap().applied_index(),
    Index::new(1),
    "the source stayed below the boundary"
  );
  assert!(
    m.contains_group(&1) && m.group(&2).unwrap().pending_merge().is_some(),
    "the target is wedged: parked forever with an unadvanceable local source"
  );
}

/// A group admitted at the highest working generation — one below the reserved `MERGED_FLOOR`
/// terminal — can never reshape: the next lineage mint would BE the sentinel, which every
/// downstream reader treats as merged-away. Every shape verb refuses `LineageExhausted` at propose
/// (nothing appended, the group stays serviceable) rather than minting the sentinel as a live
/// generation or, on a second move, wrapping the counter past the terminal to `0`. Off the
/// boundary the guard is invisible — small generations chain unchanged.
#[test]
fn lineage_mint_stops_short_of_the_reserved_terminal() {
  // The pure mint helper: it chains strictly below the terminal, and refuses AT it (the negative
  // pin — a normal small generation is untouched, the ceiling is `None`).
  assert_eq!(next_lineage(5), Some(6));
  assert_eq!(next_lineage(MERGED_FLOOR - 2), Some(MERGED_FLOOR - 1));
  assert_eq!(
    next_lineage(MERGED_FLOOR - 1),
    None,
    "the next mint would be the reserved terminal itself"
  );
  assert_eq!(next_lineage(MERGED_FLOOR), None);

  // ---- prepare_merge at the ceiling ----
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let (mut log1, mut stable1) = (VecLog::default(), AsyncStable::default());
  let (mut log2, mut stable2) = (VecLog::default(), AsyncStable::default());
  // Source (1) admitted one below the terminal; target (2) at genesis. Identical single voter, so
  // every merge precondition passes and the freeze reaches its `source_gen_after` mint.
  m.create_group(
    1,
    MERGED_FLOOR - 1,
    single_node_cfg(1),
    Instant::ORIGIN,
    7,
    SplitSm::default(),
  )
  .unwrap();
  m.create_group(
    2,
    0,
    single_node_cfg(1),
    Instant::ORIGIN,
    7,
    SplitSm::default(),
  )
  .unwrap();
  let d1 = lead_single_split(&mut m, 1, &mut log1, &mut stable1);
  lead_single_split(&mut m, 2, &mut log2, &mut stable2);
  let before = log1.last_index();
  assert_eq!(
    m.prepare_merge(&1, d1, &mut empty_stores(), &2),
    Some(Err(MergeError::LineageExhausted)),
    "a freeze mint at the ceiling would be the reserved terminal"
  );
  assert_eq!(
    log1.last_index(),
    before,
    "the refused freeze appended nothing"
  );
  assert!(
    !m.group(&1).unwrap().is_frozen(),
    "the refusal left the source live, not frozen"
  );
  // Serviceable: a normal write still commits on the refused source.
  let applied_before = m.group(&1).unwrap().applied_index();
  commit_one_split(&mut m, 1, d1, &mut log1, &mut stable1);
  assert!(
    m.group(&1).unwrap().applied_index() > applied_before,
    "a normal write still commits after the freeze refusal"
  );

  // ---- propose_split at the ceiling ----
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  m.create_group(
    7,
    MERGED_FLOOR - 1,
    single_node_cfg(1),
    Instant::ORIGIN,
    7,
    SplitSm::default(),
  )
  .unwrap();
  let d = lead_single_split(&mut m, 7, &mut log, &mut stable);
  let before = log.last_index();
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
    Some(Err(SplitError::LineageExhausted)),
    "a split mint at the ceiling would be the reserved terminal"
  );
  assert_eq!(
    log.last_index(),
    before,
    "the refused split appended nothing"
  );
  // Serviceable: a normal write still commits on the refused parent.
  let applied_before = m.group(&7).unwrap().applied_index();
  commit_one_split(&mut m, 7, d, &mut log, &mut stable);
  assert!(
    m.group(&7).unwrap().applied_index() > applied_before,
    "a normal write still commits after the split refusal"
  );
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
  m.create_group(3, 0, two_voter_cfg(), now, 7, CountSm::default())
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
  m.create_group(4, 0, lease_cfg, now, 7, CountSm::default())
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
    assert!(matches!(
      m.prepare_merge(&1, now, &mut stores, &1).unwrap(),
      Err(MergeError::SelfMerge)
    ));
    assert!(matches!(
      m.prepare_merge(&1, now, &mut stores, &9).unwrap(),
      Err(MergeError::TargetMissing)
    ));
    assert!(matches!(
      m.prepare_merge(&1, now, &mut stores, &3).unwrap(),
      Err(MergeError::VoterSetsDiffer)
    ));
    assert!(matches!(
      m.prepare_merge(&1, now, &mut stores, &4).unwrap(),
      Err(MergeError::ReadModesDiffer)
    ));
    assert!(m.prepare_merge(&9, now, &mut stores, &2).is_none());
  }
  {
    // NotLeader: group 3 never elected — as the freeze's proposer and as the abort's target
    // leader. The relayed thaw refuses EARLIER: its terminal-dedupe precedes the leadership gate,
    // so a never-frozen source (seen == expected, not frozen) is NotFrozen regardless of role.
    assert!(matches!(
      m.prepare_merge(&3, now, &mut stores, &1).unwrap(),
      Err(MergeError::NotLeader { .. })
    ));
    let (log, stable) = stores.0.get_mut(&3).map(|(l, s)| (l, s)).unwrap();
    assert!(matches!(
      m.rollback_merge(&3, now, log, stable, &1).unwrap(),
      Err(MergeError::NotLeader { .. })
    ));
    assert!(matches!(
      m.propose_merge_unfreeze(&3, now, log, stable, &1, 0)
        .unwrap(),
      Err(MergeError::NotFrozen)
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
    // The applied learner is then removed again: a source that still carried it would refuse
    // the freeze with `LearnersPresent`, which the next block exercises the clean path of.
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
      m.prepare_merge(&1, now, &mut stores, &2).unwrap(),
      Err(MergeError::ConfChangeInFlight)
    ));
    let (log, stable) = src!();
    drain_storage(&mut m, 1, now, log, stable);
    m.propose_conf_change(
      &1,
      now,
      log,
      stable,
      crate::ConfChange::new(crate::ConfChangeType::RemoveNode, 5u64, Bytes::new()),
    )
    .unwrap()
    .unwrap();
    drain_storage(&mut m, 1, now, log, stable);
    assert!(m.group(&1).unwrap().conf_state().learners().is_empty());
  }
  {
    // Freeze, then: a second freeze refuses; a parked commit refuses a second commit.
    m.prepare_merge(&1, now, &mut stores, &2).unwrap().unwrap();
    {
      let (log, stable) = src!();
      drain_storage(&mut m, 1, now, log, stable);
    }
    assert!(matches!(
      m.prepare_merge(&1, now, &mut stores, &2).unwrap(),
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
      m.propose_merge_unfreeze(&4, now, log, stable, &2, 0)
        .unwrap(),
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
      m.propose_merge_unfreeze(&1, now, log, stable, &4, 1)
        .unwrap(),
      Err(MergeError::SourceClaimed)
    ));
  }
}

/// A learner on EITHER participant refuses the FREEZE gate. A merge hands off on VOTER replicas
/// only and the relay never parks a live absorb on a learner host, so aligned replica sets are a
/// precondition — promote or remove the learners first (the CRDB doctrine). Voter sets still
/// match here; the learner alone is the refusal.
#[test]
fn merge_propose_refuses_learner_carrying_participants() {
  let now = Instant::ORIGIN;
  // The TARGET grew a learner: freezing a clean source into it refuses.
  {
    let (mut m, mut stores) = merge_host(1, 1);
    {
      let (log, stable) = stores.0.get_mut(&2).unwrap();
      m.propose_conf_change(
        &2,
        now,
        log,
        stable,
        crate::ConfChange::new(crate::ConfChangeType::AddLearnerNode, 3u64, Bytes::new()),
      )
      .unwrap()
      .unwrap();
      drain_storage(&mut m, 2, now, log, stable);
    }
    assert!(
      m.group(&2).unwrap().conf_state().learners().contains(&3),
      "the learner applied on the target"
    );
    assert!(matches!(
      m.prepare_merge(&1, now, &mut stores, &2).unwrap(),
      Err(MergeError::LearnersPresent)
    ));
  }
  // The SOURCE carries a learner: freezing it refuses too (a frozen learner host could never
  // hand its half off).
  {
    let (mut m, mut stores) = merge_host(1, 1);
    {
      let (log, stable) = stores.0.get_mut(&1).unwrap();
      m.propose_conf_change(
        &1,
        now,
        log,
        stable,
        crate::ConfChange::new(crate::ConfChangeType::AddLearnerNode, 3u64, Bytes::new()),
      )
      .unwrap()
      .unwrap();
      drain_storage(&mut m, 1, now, log, stable);
    }
    assert!(
      m.group(&1).unwrap().role().is_leader(),
      "the sole voter still leads"
    );
    assert!(matches!(
      m.prepare_merge(&1, now, &mut stores, &2).unwrap(),
      Err(MergeError::LearnersPresent)
    ));
  }
}

/// The VOPR seed-0 shape, distilled: a target whose committed configuration lists learners
/// {1, 3}. The freeze is refused at propose — the randomized reshape band would otherwise place
/// a live absorb on a learner host that parks forever.
#[test]
fn seed0_target_learner_pair_refused_at_propose() {
  let now = Instant::ORIGIN;
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let (mut slog, mut sstable) = (VecLog::default(), AsyncStable::default());
  let (mut tlog, mut tstable) = (VecLog::default(), AsyncStable::default());
  // Source (10) and target (11), both single-voter {2}, colocated on host 2.
  m.create_group(10, 0, single_node_cfg(2), now, 7, CountSm::default())
    .unwrap();
  m.create_group(11, 0, single_node_cfg(2), now, 7, CountSm::default())
    .unwrap();
  let ds = m.group(&10).unwrap().poll_timeout().unwrap();
  m.handle_timeout(&10, ds, &mut slog, &mut sstable).unwrap();
  drain_storage(&mut m, 10, ds, &mut slog, &mut sstable);
  let dt = m.group(&11).unwrap().poll_timeout().unwrap();
  m.handle_timeout(&11, dt, &mut tlog, &mut tstable).unwrap();
  drain_storage(&mut m, 11, dt, &mut tlog, &mut tstable);
  assert!(m.group(&10).unwrap().role().is_leader() && m.group(&11).unwrap().role().is_leader());
  // The target grows learners 1 and 3, one committed change at a time.
  for learner in [1u64, 3u64] {
    m.propose_conf_change(
      &11,
      dt,
      &mut tlog,
      &tstable,
      crate::ConfChange::new(crate::ConfChangeType::AddLearnerNode, learner, Bytes::new()),
    )
    .unwrap()
    .unwrap();
    drain_storage(&mut m, 11, dt, &mut tlog, &mut tstable);
  }
  let learners = m.group(&11).unwrap().conf_state().learners().clone();
  assert!(
    learners.contains(&1) && learners.contains(&3),
    "the committed conf lists learners {{1, 3}}"
  );
  assert!(matches!(
    m.prepare_merge(&10, ds, &mut empty_stores(), &11).unwrap(),
    Err(MergeError::LearnersPresent)
  ));
}

/// The COMMIT gate re-checks the alignment defensively — a learner that lands on either side
/// after the freeze would strand the absorb on a learner host. The target case is the real race
/// (the absorbing target is a live leader that can still take conf changes); the source case is
/// belt-and-suspenders, reached here by restoring a frozen source whose replayed log already
/// carries the learner.
#[test]
fn merge_commit_refuses_learner_carrying_participants() {
  let now = Instant::ORIGIN;
  // The target grows a learner AFTER the freeze applied, then the absorb refuses.
  {
    let (mut m, mut stores) = merge_host(1, 1);
    {
      m.prepare_merge(&1, now, &mut stores, &2).unwrap().unwrap();
      let (log, stable) = stores.0.get_mut(&1).unwrap();
      drain_storage(&mut m, 1, now, log, stable);
    }
    assert!(m.group(&1).unwrap().is_frozen(), "the source froze");
    {
      let (log, stable) = stores.0.get_mut(&2).unwrap();
      m.propose_conf_change(
        &2,
        now,
        log,
        stable,
        crate::ConfChange::new(crate::ConfChangeType::AddLearnerNode, 3u64, Bytes::new()),
      )
      .unwrap()
      .unwrap();
      drain_storage(&mut m, 2, now, log, stable);
    }
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    assert!(
      matches!(
        m.commit_merge(&2, now, log, stable, &1).unwrap(),
        Err(MergeError::LearnersPresent)
      ),
      "the source is frozen-ready and claims 2, but 2 grew a learner"
    );
  }
  // A frozen source that carries a learner (restore-crafted: a ConfChange then the freeze in its
  // durable log) is refused at the absorb too.
  {
    let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
    let mut cc_buf = Vec::new();
    crate::wire::encode_conf_change_v2(
      &crate::ConfChange::new(crate::ConfChangeType::AddLearnerNode, 3u64, Bytes::new()).into_v2(),
      &mut cc_buf,
    );
    let mut prep = Vec::new();
    {
      let mut tb = Vec::new();
      Data::encode(&2u64, &mut tb);
      crate::wire::encode_prepare_merge_payload(
        &crate::PrepareMergePayload::new(Bytes::from(tb), 1),
        &mut prep,
      );
    }
    let mut slog = VecLog::default();
    slog.force_append(&[
      crate::Entry::new(
        Term::new(1),
        Index::new(1),
        crate::EntryKind::ConfChange,
        Bytes::from(cc_buf),
      ),
      crate::Entry::new(
        Term::new(1),
        Index::new(2),
        crate::EntryKind::PrepareMerge,
        Bytes::from(prep),
      ),
    ]);
    let mut sstable = AsyncStable::default();
    sstable.force_state(Term::new(1), Some(1u64), Index::new(2));
    m.restore_group(
      1,
      single_node_cfg(1),
      Instant::ORIGIN,
      7,
      CountSm::default(),
      1,
      &mut slog,
      &mut sstable,
    )
    .unwrap();
    assert!(
      m.group(&1).unwrap().is_frozen(),
      "the crafted freeze applied on restore"
    );
    assert!(
      m.group(&1).unwrap().conf_state().learners().contains(&3),
      "the crafted learner applied on restore"
    );
    let (mut tlog, mut tstable) = (VecLog::default(), AsyncStable::default());
    m.create_group(
      2,
      0,
      single_node_cfg(1),
      Instant::ORIGIN,
      7,
      CountSm::default(),
    )
    .unwrap();
    let d = m.group(&2).unwrap().poll_timeout().unwrap();
    m.handle_timeout(&2, d, &mut tlog, &mut tstable).unwrap();
    drain_storage(&mut m, 2, d, &mut tlog, &mut tstable);
    assert!(m.group(&2).unwrap().role().is_leader());
    assert!(
      matches!(
        m.commit_merge(&2, d, &mut tlog, &tstable, &1).unwrap(),
        Err(MergeError::LearnersPresent)
      ),
      "the frozen source claims 2 and is ready, but carries a learner"
    );
  }
}

/// The abort names an APPLIED freeze: while the freeze is only PENDING (appended, unapplied)
/// the rollback refuses typed — its generation and claim are unreadable until it applies, and
/// a freeze that never commits self-heals through truncation instead. Once applied, the abort
/// lands on the target and the service-driven thaw walks the source's counter past the freeze.
#[test]
fn rollback_refuses_a_pending_freeze_then_lands() {
  let (mut m, mut stores) = merge_host(1, 1);
  let now = Instant::ORIGIN;
  {
    m.prepare_merge(&1, now, &mut stores, &2).unwrap().unwrap();
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
  // Applied: the abort lands on the target's log and records the durable thaw obligation.
  {
    let (log, stable) = stores.0.get_mut(&2).map(|(l, s)| (l, s)).unwrap();
    m.rollback_merge(&2, now, log, stable, &1).unwrap().unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert_eq!(m.group(&2).unwrap().shape_gen(), 1, "the abort bumped");
  assert_eq!(
    m.group(&2)
      .unwrap()
      .abandoned_obligations()
      .first()
      .map(|(_, g, _)| *g),
    Some(1),
    "the abort recorded the thaw obligation at the freeze generation"
  );
  // The per-crank service DRIVES the source thaw from the obligation; draining commits+applies it.
  m.service_merge_applies(now, &mut stores);
  {
    let (log, stable) = stores.0.get_mut(&1).map(|(l, s)| (l, s)).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  let ep = m.group(&1).unwrap();
  assert!(!ep.is_frozen(), "thawed");
  assert_eq!(ep.shape_gen(), 2, "0 -> 1 (freeze) -> 2 (thaw)");
  assert!(!ep.merge_freeze_active());
}

/// THE MERGE-ORPHAN WEDGE, PREVENTED AT ADMISSION (the dual of the freeze-identity red-proof):
/// `commit_merge` must not dissolve a source until EVERY source voter has matched the freeze
/// boundary. A source voter left log-behind below the boundary while the source leader is later
/// lost is orphaned — the other hosts floor+dismantle the source out from under it (the
/// leader-local resolve-last discipline is defeated by source-leader loss), and its co-located,
/// log-complete TARGET LEADER parks forever with a local source it can neither advance nor be
/// snapshotted past. Admitting the `CommitMerge` while a voter lags is what seeds that wedge, so
/// the barrier refuses here: the committed `CommitMerge` then certifies the whole voter set holds
/// the freeze, and dissolution rides it safely on every host.
#[test]
fn commit_merge_refuses_until_every_source_voter_reaches_the_freeze() {
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
  // Source (1) and target (2) both led locally by node 1; peer 2 acks (quorum), peer 3 lags.
  for gid in [1u64, 2] {
    stores
      .0
      .insert(gid, (VecLog::default(), AsyncStable::default()));
    m.create_group(
      gid,
      0,
      three_voters(),
      Instant::ORIGIN,
      7,
      CountSm::default(),
    )
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
    ack(&mut m, &mut stores, gid, 2, Index::new(1));
  }
  let now = Instant::ORIGIN;

  // Freeze the source: peer 2's ack commits and applies the PrepareMerge; peer 3's match stays
  // at 0 — log-behind, below the freeze boundary F.
  let f = { m.prepare_merge(&1, now, &mut stores, &2).unwrap().unwrap() };
  ack(&mut m, &mut stores, 1, 2, f);
  assert!(m.group(&1).unwrap().is_frozen());
  assert!(
    !m.group(&1).unwrap().peers_matched_through(f),
    "peer 3 has NOT reached the freeze boundary"
  );

  // The barrier refuses: admitting now would seed the wedge (source-leader loss orphans peer 3).
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    assert!(
      matches!(
        m.commit_merge(&2, now, log, stable, &1).unwrap(),
        Err(MergeError::SourceBarrierPending)
      ),
      "a lagging source voter must block the absorb at admission"
    );
  }
  assert!(
    m.group(&2).unwrap().pending_merge().is_none(),
    "nothing parked — no CommitMerge was proposed"
  );

  // Peer 3 catches up to the boundary: the barrier clears and the absorb admits, its committed
  // CommitMerge now a certificate that every source voter holds the freeze.
  ack(&mut m, &mut stores, 1, 3, f);
  assert!(m.group(&1).unwrap().peers_matched_through(f));
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    assert!(
      m.commit_merge(&2, now, log, stable, &1).unwrap().is_ok(),
      "with every voter at the boundary the absorb admits"
    );
  }
}

/// Dissolution rides the committed `CommitMerge`, uniformly on every host — there is no
/// resolve-last discipline. The all-source-voters barrier at admission already proved every
/// voter holds the freeze, so once the `CommitMerge` commits and its abort window closes the
/// host resolves straight away; it never waits on a peer's match at resolve time. To make the
/// leader-loss resilience concrete the source LEADER is stepped down before the resolve: the
/// old discipline hinged on the source leader staying alive to feed stragglers, and its early
/// loss is exactly what orphaned them — now the resolve completes regardless, because the
/// barrier put the freeze in every voter's own log before the `CommitMerge` was ever proposed.
#[test]
fn dissolution_rides_the_committed_commit_merge_after_source_leader_loss() {
  use crate::{AppendResponse, Message, RequestVote, VoteResponse};
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
  // Source (1) and target (2): 3-voter groups led locally (peer 2's vote elects).
  for gid in [1u64, 2] {
    stores
      .0
      .insert(gid, (VecLog::default(), AsyncStable::default()));
    m.create_group(
      gid,
      0,
      three_voters(),
      Instant::ORIGIN,
      7,
      CountSm::default(),
    )
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
    ack(&mut m, &mut stores, gid, 2, Index::new(1));
  }
  let now = Instant::ORIGIN;

  // Freeze the source, then bring EVERY source voter to the boundary — the admission barrier.
  let f = { m.prepare_merge(&1, now, &mut stores, &2).unwrap().unwrap() };
  ack(&mut m, &mut stores, 1, 2, f);
  ack(&mut m, &mut stores, 1, 3, f);
  assert!(m.group(&1).unwrap().is_frozen());
  assert!(
    m.group(&1).unwrap().peers_matched_through(f),
    "every source voter matched the boundary — the barrier is met"
  );

  // The barrier admits the absorb; peer 2's ack commits it and the target parks.
  let k = {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.commit_merge(&2, now, log, stable, &1).unwrap().unwrap()
  };
  ack(&mut m, &mut stores, 2, 2, k);
  assert!(m.group(&2).unwrap().pending_merge().is_some(), "parked");

  // The source LEADER is lost: a higher-term vote request steps node 1's source down to a
  // follower. Under the old resolve-last discipline this loss is what stranded stragglers.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.handle_message(
      &1,
      now,
      log,
      stable,
      3u64,
      Message::RequestVote(RequestVote::new(
        Term::new(2),
        3u64,
        f,
        Term::new(1),
        false,
        false,
      )),
    )
    .unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    !m.group(&1).unwrap().role().is_leader(),
    "the source leader stepped down"
  );

  // Seal and resolve: dissolution completes on this host even though the source is no longer
  // led here — the committed CommitMerge is the certificate, not a live source leader.
  assert!(
    m.service_merge_applies(now, &mut stores).is_empty(),
    "the open window seals and holds"
  );
  ack(&mut m, &mut stores, 2, 2, k.next());
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
    m.prepare_merge(&1, now, &mut stores, &2).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&1).unwrap();
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
    assert!(matches!(
      m.prepare_merge(&2, now, &mut stores, &1).unwrap(),
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
  m.create_group(3, 0, single_node_cfg(1), now, 7, CountSm::default())
    .unwrap();
  {
    let (log, stable) = stores.0.get_mut(&3).unwrap();
    let d = m.group(&3).unwrap().poll_timeout().unwrap();
    m.handle_timeout(&3, d, log, stable).unwrap();
    drain_storage(&mut m, 3, d, log, stable);
  }
  {
    assert!(matches!(
      m.prepare_merge(&2, now, &mut stores, &3).unwrap(),
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
      0,
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
    m.prepare_merge(&1, now, &mut stores, &2).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&1).unwrap();
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
