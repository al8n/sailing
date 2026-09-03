use super::*;
use crate::{
  Config, Instant, PoisonReason,
  endpoint::Cover,
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
fn dirty_queues_dedup_interleaved_marks_by_membership() {
  // Interleaved dispatches across groups (A,B,A,B,…) must not grow the dirty queues without
  // bound: the membership mirror caps each queue at one entry per distinct dirty group. 2*n
  // alternating marks leave at most the two ids queued (consecutive-dedup alone kept all 2*n,
  // because each mark's predecessor is the OTHER group).
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let (a, b) = (1u64, 2u64);
  for _ in 0..64 {
    m.mark_dirty(&a);
    m.mark_dirty(&b);
  }
  assert!(
    m.dirty_msgs.len() <= 2,
    "dirty_msgs grew to {}",
    m.dirty_msgs.len()
  );
  assert!(
    m.dirty_events.len() <= 2,
    "dirty_events grew to {}",
    m.dirty_events.len()
  );
  assert!(
    m.dirty_forks.len() <= 2,
    "dirty_forks grew to {}",
    m.dirty_forks.len()
  );
  // Each mirror stays exact with its queue.
  assert_eq!(m.dirty_msgs_set.len(), m.dirty_msgs.len());
  assert_eq!(m.dirty_events_set.len(), m.dirty_events.len());
  assert_eq!(m.dirty_forks_set.len(), m.dirty_forks.len());
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
fn create_rejects_the_reserved_terminal_generation() {
  // #101-F8: the CORE constructors refuse the reserved MERGED_FLOOR sentinel — a group born at it
  // reads as merged-away to every downstream consumer (a MERGED_FLOOR floor admits nothing;
  // `next_lineage` never mints it) — even without a coordinator floor validator in the path.
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  assert_eq!(
    m.create_group(
      1,
      MERGED_FLOOR,
      single_node_cfg(1),
      Instant::ORIGIN,
      42,
      CountSm::default()
    ),
    Err(CreateGroupError::ReservedGeneration)
  );
  assert!(m.is_empty(), "nothing admitted at the sentinel");
  assert!(m.host_id().is_none(), "the refusal precedes the id latch");
  assert_eq!(
    m.create_group_from_fork(
      2,
      MERGED_FLOOR,
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
    Err(CreateGroupError::ReservedGeneration)
  );
  assert!(
    stable.snapshot().is_none(),
    "the fork's baseline write never happened — refused before any store write"
  );

  // A working generation just below the sentinel admits normally — through the door that can
  // persist it, the only one a nonzero founding value may use.
  m.create_group_founded_at(
    3,
    MERGED_FLOOR - 2,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    CountSm::default(),
    1,
    &log,
    &mut stable,
  )
  .unwrap();
  assert_eq!(m.group_gen(&3), MERGED_FLOOR - 2);
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

/// WHY the guard exists, demonstrated at the endpoint level. The forbidden fork shape at
/// `boot_epoch = 0` — reachable here only by calling `write_fork_baseline` directly, since
/// `create_group_from_fork` refuses epoch 0 — collapses the baseline's prior-epoch write
/// ids into epoch 0, the same epoch the child's op counter is seeded with. The two id spaces
/// then ALIAS: the campaign's self-vote write is minted at
/// `(0, 0)` — the id of the QUEUED baseline HardState write — so draining the BASELINE's
/// `Wrote(0, 0)` matches the pending `Campaign` action and fires `become_leader` while the
/// self-vote's own fsync is still in flight: leadership on a phantom durable self-vote (a
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

/// A committed `Split` entry whose CHILD id bytes will NOT decode as `G` — the committed-corrupt
/// shape the relay drain fail-stops as `SplitDecode` (`[0xFF; 3]` is not a valid `u64`).
fn corrupt_child_split_entry() -> Bytes {
  let payload = crate::SplitPayload::new(
    Bytes::from_static(&[0xFF, 0xFF, 0xFF]),
    0,
    1,
    Bytes::copy_from_slice(&[1]),
  );
  let mut buf = Vec::new();
  crate::wire::encode_split_payload(&payload, &mut buf);
  Bytes::from(buf)
}

/// Drive follower group `gid` (leader `2`) to apply one committed corrupt-child split, then relay
/// it: the relay decode fail-stops the parent inside the drain (which does NOT route through
/// `mark_dirty`, so the poison signal is latched at that site). Leaves the group hosted-and-poisoned
/// with its signal pending.
fn poison_via_corrupt_fork(
  m: &mut MultiRaft<u64, u64, SplitSm>,
  gid: u64,
  log: &mut VecLog,
  stable: &mut AsyncStable,
) {
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  m.create_group(gid, 0, cfg, Instant::ORIGIN, 42, SplitSm::default())
    .unwrap();
  let entries = std::vec![crate::Entry::new(
    Term::new(1),
    Index::new(1),
    crate::EntryKind::Split,
    corrupt_child_split_entry(),
  )];
  m.handle_message(
    &gid,
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
      Index::new(1),
    )),
  )
  .unwrap();
  while matches!(
    m.handle_storage(&gid, Instant::ORIGIN, log, stable),
    Some(StorageProgress::MorePending)
  ) {}
  assert!(
    m.peek_yieldable_fork(&NoHold).is_none(),
    "the corrupt child yields no fork"
  );
  assert!(
    m.group(&gid).unwrap().is_poisoned(),
    "the relay decode fail-stops the parent"
  );
}

/// A fail-stopped group surfaces on the aggregate lifecycle tail exactly ONCE — however many cranks
/// re-observe the poison, the seen-set caps it at one signal per hosted incarnation.
#[test]
fn poll_poisoned_reports_a_fail_stopped_group_once() {
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  poison_via_corrupt_fork(&mut m, 7, &mut log, &mut stable);

  // The post-dispatch choke point re-checks on every mark; the signal still fires once.
  m.mark_dirty(&7);
  m.mark_dirty(&7);

  assert_eq!(
    m.poll_poisoned(),
    Some(7),
    "the fail-stop surfaces on the aggregate tail"
  );
  assert_eq!(m.poll_poisoned(), None, "exactly once");
}

/// A removed group's un-consumed poison signal dies with the incarnation — never delivered stale
/// after teardown.
#[test]
fn removing_a_group_purges_its_pending_poison_signal() {
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  poison_via_corrupt_fork(&mut m, 7, &mut log, &mut stable);

  let mut stores = MapStores(std::collections::BTreeMap::new(), Default::default());
  m.remove_group(&7, &mut stores).unwrap();
  assert_eq!(
    m.poll_poisoned(),
    None,
    "a removed group's stale signal is never delivered"
  );
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
  r.restore_group_unchecked(
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
  mr.restore_group_unchecked(
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
/// container relay round-trips. `refuse_restore` models a state machine that cannot take an
/// installed blob: every `restore` fails, so a deferred install's completion poisons
/// `SnapshotRestore` instead of re-baselining.
#[derive(Debug, Default, PartialEq, Eq)]
struct SplitSm {
  units: u64,
  refuse_restore: bool,
}

/// The one fault [`SplitSm`] raises: a refused `restore`.
#[derive(Debug)]
struct RestoreRefused;

impl core::fmt::Display for RestoreRefused {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.write_str("restore refused")
  }
}

impl core::error::Error for RestoreRefused {}

impl crate::StateMachine for SplitSm {
  type Command = Bytes;
  type Response = u64;
  type Snapshot = u64;
  type Error = RestoreRefused;

  fn apply(&mut self, _index: crate::Index, _cmd: Bytes) -> Result<u64, Self::Error> {
    self.units += 1;
    Ok(self.units)
  }

  fn snapshot(&self) -> Result<u64, Self::Error> {
    Ok(self.units)
  }

  fn restore(&mut self, snapshot: u64) -> Result<(), Self::Error> {
    if self.refuse_restore {
      return Err(RestoreRefused);
    }
    self.units = snapshot;
    Ok(())
  }

  fn split(&mut self, instruction: &[u8]) -> Option<Self> {
    let give = u64::from(*instruction.first()?).min(self.units);
    self.units -= give;
    Some(Self {
      units: give,
      refuse_restore: false,
    })
  }

  fn absorb(&mut self, source: Self) -> bool {
    self.units += source.units;
    true
  }

  fn supports_split(&self) -> bool {
    true
  }

  fn supports_absorb(&self) -> bool {
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

/// A split or merge against a state machine that does not support the verb is refused at PROPOSE
/// (nothing appended), rather than committing and then poisoning every replica at apply. A supporting
/// FSM still admits the split.
///
/// MUTATION: drop the `supports_split`/`supports_absorb` gate in `propose_split`/`prepare_merge` →
/// the default-FSM verbs are appended and the log grows, deferring the fail-stop to apply.
#[test]
fn split_and_merge_refuse_an_unsupported_fsm_at_propose() {
  // A plain FSM: overrides neither `split` nor `absorb`, so both `supports_*` default to false.
  #[derive(Default)]
  struct PlainSm(u64);
  impl crate::StateMachine for PlainSm {
    type Command = Bytes;
    type Response = u64;
    type Snapshot = u64;
    type Error = core::convert::Infallible;
    fn apply(&mut self, _: Index, _: Bytes) -> Result<u64, Self::Error> {
      self.0 += 1;
      Ok(self.0)
    }
    fn snapshot(&self) -> Result<u64, Self::Error> {
      Ok(self.0)
    }
    fn restore(&mut self, s: u64) -> Result<(), Self::Error> {
      self.0 = s;
      Ok(())
    }
  }

  // SPLIT: drive a single-voter PlainSm group to leadership, then a split is refused at propose and
  // the log is untouched.
  let mut m: MultiRaft<u64, u64, PlainSm> = MultiRaft::new();
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();
  m.create_group(
    1,
    0,
    single_node_cfg(1),
    Instant::ORIGIN,
    7,
    PlainSm::default(),
  )
  .unwrap();
  let d = m.group(&1).unwrap().poll_timeout().unwrap();
  m.handle_timeout(&1, d, &mut log, &mut stable).unwrap();
  while matches!(
    m.handle_storage(&1, d, &mut log, &mut stable),
    Some(StorageProgress::MorePending)
  ) {}
  assert!(m.group(&1).unwrap().role().is_leader());
  let last_before = log.last_index();
  let err = m
    .propose_split(
      &1,
      d,
      &mut log,
      &stable,
      &200,
      0,
      Bytes::from_static(b"\x01"),
    )
    .unwrap()
    .unwrap_err();
  assert!(matches!(err, SplitError::Unsupported), "got {err:?}");
  assert_eq!(
    log.last_index(),
    last_before,
    "a refused split appends nothing"
  );

  // MERGE: two colocated PlainSm groups; the non-absorbing target refuses at prepare_merge (the
  // gate sits before the source-leader check, so no leadership setup is needed).
  let mut mm: MultiRaft<u64, u64, PlainSm> = MultiRaft::new();
  let mut stores = MapStores(
    std::collections::BTreeMap::new(),
    std::collections::BTreeSet::new(),
  );
  for gid in [1u64, 2] {
    stores
      .0
      .insert(gid, (VecLog::default(), AsyncStable::default()));
    mm.create_group(
      gid,
      0,
      single_node_cfg(1),
      Instant::ORIGIN,
      7,
      PlainSm::default(),
    )
    .unwrap();
  }
  // source = 2 (encodes strictly above target = 1), the required merge direction.
  let merr = mm
    .prepare_merge(&2, Instant::ORIGIN, &mut stores, &1)
    .unwrap()
    .unwrap_err();
  assert!(matches!(merr, MergeError::Unsupported), "got {merr:?}");

  // A SUPPORTING FSM (`SplitSm` overrides split + supports_split) still admits the split.
  let mut ms: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let (mut slog, mut sstable) = (VecLog::default(), AsyncStable::default());
  ms.create_group(
    1,
    0,
    single_node_cfg(1),
    Instant::ORIGIN,
    7,
    SplitSm::default(),
  )
  .unwrap();
  let sd = lead_single_split(&mut ms, 1, &mut slog, &mut sstable);
  commit_one_split(&mut ms, 1, sd, &mut slog, &mut sstable);
  ms.propose_split(
    &1,
    sd,
    &mut slog,
    &sstable,
    &200,
    0,
    Bytes::from_static(b"\x01"),
  )
  .unwrap()
  .expect("a supporting FSM admits the split");
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

  // The relay: a borrowed view of the head fork, carrying the split coordinates and the derived
  // boot config. The fork itself has not moved — it is still staged behind this decision.
  let fork = m.peek_yieldable_fork(&NoHold).expect("one fork relayed");
  assert_eq!((*fork.parent()), 7);
  assert_eq!((*fork.child()), 200);
  assert_eq!(fork.child_gen(), 0);
  assert_eq!(
    fork.parent_gen_after(),
    1,
    "first split bumps the lineage to 1"
  );
  assert_eq!(fork.split_index(), idx);
  assert_eq!(fork.config().id(), 1);
  assert_eq!(fork.config().voters(), &[1u64]);
  assert_eq!(
    fork.config().snapshot_threshold(),
    1,
    "the child inherits the parent's local knobs"
  );
  assert!(fork.config().pre_vote());

  // The forked half never leaves the container, so it is observable exactly where it lands: the
  // installed child's state machine and its manufactured baseline.
  let mut engine: GroupEngine<u64, u64> = GroupEngine::new();
  assert!(matches!(
    m.install_yieldable_fork(&7, &200, &mut engine, &NoHold, d, 43),
    InstallOutcome::Installed {
      parent: 7,
      child: 200,
      child_gen: 0,
      parent_gen_after: 1,
      ..
    }
  ));
  assert_eq!(
    m.group(&200).unwrap().state_machine().units,
    2,
    "the forked half"
  );
  let (_, fstable) = engine.stores(&200).expect("the child's stores");
  let (meta, blob) = fstable.snapshot().expect("the manufactured baseline");
  assert_eq!(
    blob,
    fork_blob(2),
    "the apply-derived blob matches the half"
  );
  assert_eq!(
    meta.read_only(),
    None,
    "a never-migrated parent hands the child no explicit mode"
  );
  assert!(m.peek_yieldable_fork(&NoHold).is_none(), "exactly one fork");
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
  {
    let fork = m
      .peek_yieldable_fork(&NoHold)
      .expect("the first fork relays");
    assert_eq!(((*fork.child()), fork.parent_gen_after()), (200, 1));
  }
  let mut engine: GroupEngine<u64, u64> = GroupEngine::new();
  assert!(matches!(
    m.install_yieldable_fork(&7, &200, &mut engine, &NoHold, Instant::ORIGIN, 43),
    InstallOutcome::Installed { .. }
  ));
  assert_eq!(
    m.group(&200).unwrap().state_machine().units,
    2,
    "child 1 holds the single given-up half"
  );
  assert!(
    m.peek_yieldable_fork(&NoHold).is_none(),
    "the stale mint staged NO fork"
  );
  // Conservation: every unit is in exactly one of parent / child 1.
  assert_eq!(
    m.group(&7).unwrap().state_machine().units + m.group(&200).unwrap().state_machine().units,
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
  {
    let fork1 = m
      .peek_yieldable_fork(&NoHold)
      .expect("the first fork relays");
    assert_eq!(((*fork1.child()), fork1.parent_gen_after()), (200, 1));
    assert_eq!(fork1.split_index(), idx1);
  }
  let mut engine: GroupEngine<u64, u64> = GroupEngine::new();
  assert!(matches!(
    m.install_yieldable_fork(&7, &200, &mut engine, &NoHold, d, 43),
    InstallOutcome::Installed { .. }
  ));
  assert_eq!(m.group(&200).unwrap().state_machine().units, 2);

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
  {
    let fork2 = m
      .peek_yieldable_fork(&NoHold)
      .expect("the second fork relays");
    assert_eq!(((*fork2.child()), fork2.parent_gen_after()), (201, 2));
    assert_eq!(fork2.split_index(), idx2);
  }
  assert!(matches!(
    m.install_yieldable_fork(&7, &201, &mut engine, &NoHold, d, 44),
    InstallOutcome::Installed { .. }
  ));

  // Conservation across the chained pair: every unit lives in exactly one of the three.
  assert_eq!(m.group(&7).unwrap().state_machine().units, 1);
  assert_eq!(
    m.group(&7).unwrap().state_machine().units
      + m.group(&200).unwrap().state_machine().units
      + m.group(&201).unwrap().state_machine().units,
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

/// A [`ForkGate`] answering from fixed sets — the container's stand-in for a driver's engine.
#[derive(Default)]
struct TestGate {
  occupied: BTreeSet<u64>,
  floors: BTreeMap<u64, u64>,
}

impl TestGate {
  fn occupying(child: u64) -> Self {
    Self {
      occupied: [child].into_iter().collect(),
      floors: BTreeMap::new(),
    }
  }

  fn flooring(child: u64, floor: u64) -> Self {
    Self {
      occupied: BTreeSet::new(),
      floors: [(child, floor)].into_iter().collect(),
    }
  }
}

impl ForkGate<u64> for TestGate {
  fn contains_group(&self, gid: &u64) -> bool {
    self.occupied.contains(gid)
  }

  fn floor(&self, gid: &u64) -> u64 {
    self.floors.get(gid).copied().unwrap_or(0)
  }
}

/// Build a host whose group 7 has a NONZERO committed split staged for `child`, ready to relay.
fn host_with_staged_fork(child: u64) -> (MultiRaft<u64, u64, SplitSm>, VecLog, AsyncStable) {
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
  follower_load_and_split(&mut m, &mut log, &mut stable, child);
  (m, log, stable)
}

/// The driver's install seam in one line: install the head fork for `child` through a scratch
/// engine and hand back the split index the driver then reports flush-durable. The fixtures that
/// use this exercise the PARENT's barrier and obligations, so the child's own storage is
/// incidental — what matters is that the install CONSUMES the staged fork, exactly as a driver's
/// `fork_drain` does, before the barrier is lifted.
fn install_head_fork(
  m: &mut MultiRaft<u64, u64, SplitSm>,
  parent: u64,
  child: u64,
  now: Instant,
) -> Index {
  let mut engine: GroupEngine<u64, u64> = GroupEngine::new();
  match m.install_yieldable_fork(&parent, &child, &mut engine, &NoHold, now, 43) {
    InstallOutcome::Installed { split_index, .. } => split_index,
    other => panic!("the staged fork {parent}->{child} must install, got {other:?}"),
  }
}

/// OCCUPIED CALLER STORAGE HOLDS. Occupancy says the id is spoken for, never that the stores ARE
/// this fork's child — so the fork stays staged with its blob, its fence, and its reservation,
/// and lands the moment the gate stops answering yes. The parent's guard never moves meanwhile.
#[test]
fn an_occupied_child_id_holds_the_fork_until_the_gate_clears() {
  let (mut m, _log, _stable) = host_with_staged_fork(200);
  let gate = TestGate::occupying(200);

  assert!(
    m.peek_yieldable_fork(&gate).is_none(),
    "occupied stores hold the fork instead of yielding it"
  );
  assert!(
    m.group(&7).unwrap().peek_pending_fork().is_some(),
    "the held fork is still STAGED — blob, fence and reservation intact"
  );
  assert!(
    m.group(&7).unwrap().fork_obligations_standing(),
    "a held fork is an outstanding obligation by construction"
  );
  assert_eq!(
    m.poll_split_conflict(),
    Some((7, 200)),
    "the hold surfaces one (parent, child) cue"
  );
  assert_eq!(m.poll_split_conflict(), None, "one cue per episode");
  assert!(
    m.peek_yieldable_fork(&gate).is_none(),
    "still held, and re-examination does not re-cue"
  );
  assert_eq!(m.poll_split_conflict(), None);

  // The gate stops answering yes and the very same fork lands, half and all.
  {
    let fork = m
      .peek_yieldable_fork(&NoHold)
      .expect("the released fork materializes");
    assert_eq!(((*fork.parent()), (*fork.child())), (7, 200));
  }
  let mut engine: GroupEngine<u64, u64> = GroupEngine::new();
  assert!(matches!(
    m.install_yieldable_fork(&7, &200, &mut engine, &NoHold, Instant::ORIGIN, 43),
    InstallOutcome::Installed { .. }
  ));
  assert_eq!(
    m.group(&200).unwrap().state_machine().units,
    2,
    "the partition rode the hold intact"
  );
}

/// MEMBERSHIP COMES FROM THE COMMITTED SPLIT. The install takes no config, so two replicas
/// installing the same fork cannot land the child under different voter sets — the shape that would
/// let one id carry two conflicting committed histories with every other input matching. Both
/// containers derive the same config from the staged fork, and the only divergence from the
/// committed one is the reshape-born knob forcing, applied inside the container on both.
#[test]
fn two_replicas_install_one_fork_under_identical_voter_sets() {
  let voters = |m: &MultiRaft<u64, u64, SplitSm>| -> Vec<u64> {
    m.group(&200)
      .unwrap()
      .conf_state()
      .voters()
      .iter()
      .copied()
      .collect()
  };
  let mut installed = Vec::new();
  for seed in [43u64, 99] {
    let (mut m, _log, _stable) = host_with_staged_fork(200);
    // What the committed split says the child's membership is — identical on both replicas
    // because the split rode the parent's totally-ordered log.
    let committed: Vec<u64> = m
      .peek_yieldable_fork(&NoHold)
      .expect("relays")
      .config()
      .voters()
      .to_vec();
    let mut engine: GroupEngine<u64, u64> = GroupEngine::new();
    assert!(matches!(
      m.install_yieldable_fork(&7, &200, &mut engine, &NoHold, Instant::ORIGIN, seed),
      InstallOutcome::Installed { .. }
    ));
    assert_eq!(
      voters(&m),
      committed,
      "the installed child's voters are the committed split's, not any caller's"
    );
    installed.push(voters(&m));
  }
  assert_eq!(
    installed[0], installed[1],
    "and both replicas agree — there is no caller-shaped input left to diverge on"
  );
  assert!(
    installed[0].len() > 1,
    "non-vacuous: the committed voter set is the parent's, not a sole-voter default"
  );
}

/// THE RESERVATION IS CONTIGUOUS, because the fork never leaves home. The staged leg runs from the
/// committed split right up to the pop inside the install, and the pop is followed by the child's
/// own admission with nothing in between — so there is no window to leak through and no second
/// bookkeeping leg to keep. A peek does not shorten it (nothing is consumed), and a HOLD does not
/// either: the fork stays staged, reserving, until it lands.
#[test]
fn a_staged_forks_child_stays_reserved_right_through_the_install() {
  let (mut m, _log, _stable) = host_with_staged_fork(200);
  assert!(m.split_reserved(&200), "staged, so reserved");

  // Peeked but not installed: still staged, still reserving, and the public door still refuses.
  let peeked = m
    .peek_yieldable_fork(&NoHold)
    .expect("the committed split is yieldable")
    .split_index();
  assert!(
    m.group(&7).unwrap().peek_pending_fork().is_some(),
    "a peek consumes nothing — the staged leg never ends early"
  );
  assert!(m.split_reserved(&200));

  // Held on the gate: same answer, for as long as the hold stands.
  assert!(
    m.peek_yieldable_fork(&TestGate::occupying(200)).is_none(),
    "held"
  );
  assert!(m.split_reserved(&200), "a held fork reserves its child too");

  let (mut clog, mut cstable) = (VecLog::default(), AsyncStable::default());
  assert_eq!(
    m.create_group_from_fork(
      200,
      0,
      single_node_cfg(1),
      Instant::ORIGIN,
      43,
      SplitSm::default(),
      Bytes::from_static(b"\x00"),
      None,
      1,
      &mut clog,
      &mut cstable,
    ),
    Err(CreateGroupError::SplitReserved),
    "the public door refuses the id the staged fork owns"
  );
  assert_eq!(
    <VecLog as crate::LogStore>::last_index(&clog),
    Index::ZERO,
    "and refuses before any store write"
  );

  // The install is the split claiming its own id, so it is the one door the reservation does not
  // fence — and the reservation ends with the pop it makes, not before it.
  let mut engine: GroupEngine<u64, u64> = GroupEngine::new();
  assert!(matches!(
    m.install_yieldable_fork(&7, &200, &mut engine, &NoHold, Instant::ORIGIN, 44),
    InstallOutcome::Installed { split_index, .. } if split_index == peeked
  ));
  assert!(
    !m.split_reserved(&200),
    "consumed, so released — a leaked leg would squat the id for good"
  );
  assert!(m.contains_group(&200), "the child is hosted");
}

/// THE DOUBLE-YIELD DIES AS A CLASS. When the forked half left the container as a value, one
/// (parent, child) could be presented twice — a second yield minting a second ticket for the same
/// partition, or a second install replaying the first over the child's own progress. Neither
/// surface survives: a peek CONSUMES NOTHING, so peeking twice describes the SAME staged fork and
/// mints nothing either time; and the install consumes it, so a second install finds no head fork
/// for that child at all. Nothing doubles, and the child's own state is what proves it.
#[test]
fn one_committed_fork_cannot_be_yielded_or_installed_twice() {
  let (mut m, _log, _stable) = host_with_staged_fork(200);
  let coords = |m: &mut MultiRaft<u64, u64, SplitSm>| {
    let v = m
      .peek_yieldable_fork(&NoHold)
      .expect("the committed split is yieldable");
    (
      (*v.parent()),
      (*v.child()),
      v.child_gen(),
      v.parent_gen_after(),
      v.split_index(),
    )
  };
  assert_eq!(
    coords(&mut m),
    coords(&mut m),
    "two peeks, one fork: a peek describes the head, it does not take it"
  );
  assert!(
    m.group(&7).unwrap().peek_pending_fork().is_some(),
    "and the fork is still exactly where it was"
  );

  // The install names its child, so a peek for one id can never install another.
  let mut engine: GroupEngine<u64, u64> = GroupEngine::new();
  assert_eq!(
    m.install_yieldable_fork(&7, &201, &mut engine, &NoHold, Instant::ORIGIN, 43),
    InstallOutcome::NotYieldable,
    "7's head fork names 200; an install for 201 is not this call's business"
  );
  assert!(
    !engine.contains_group(&201),
    "and it made no storage for it"
  );

  // ONE install lands the partition …
  assert!(matches!(
    m.install_yieldable_fork(&7, &200, &mut engine, &NoHold, Instant::ORIGIN, 43),
    InstallOutcome::Installed { child: 200, .. }
  ));
  let units = m.group(&200).unwrap().state_machine().units;
  assert_eq!(units, 2, "the forked half, once");

  // … and the second install has nothing to take: the staged queue is empty, so there is no head
  // fork for this child or any other, and the child that landed is untouched.
  assert_eq!(
    m.install_yieldable_fork(&7, &200, &mut engine, &NoHold, Instant::ORIGIN, 44),
    InstallOutcome::Empty,
    "the first install consumed the fork; nothing replays it over the child's own progress"
  );
  assert_eq!(m.group(&200).unwrap().state_machine().units, units);
  assert_eq!(m.group_gen(&7), 1, "one install, one relay-guard advance");
}

/// A fork abandoned TERMINALLY was never installed, and nothing is left standing for it: the
/// container consumed it inside its own drain, so the id ends up claimed by nothing.
#[test]
fn an_abandoned_fork_leaves_no_reservation_behind() {
  let (mut m, _log, _stable) = host_with_staged_fork(200);
  let floored = TestGate::flooring(200, crate::MERGED_FLOOR);
  assert!(m.peek_yieldable_fork(&floored).is_none());
  assert_eq!(m.poll_split_refusal(), Some((7, 200)));
  assert!(
    !m.split_reserved(&200),
    "a terminally abandoned fork holds nothing"
  );
}

/// THE HELD → TERMINAL TRANSITION. A hold is not a verdict, and the fork proves it by surviving one
/// and then answering the next gate honestly: held while the id is merely occupied, abandoned the
/// moment the id's floor rises to the reserved terminal. This is the pin for the below-floor
/// abandonment, because a wholly-refused child never registers a conservation pair — the sweep
/// cannot see this, so the assertion has to.
#[test]
fn a_held_fork_terminalizes_when_its_floor_rises() {
  let (mut m, _log, _stable) = host_with_staged_fork(200);

  // HELD: the caller's stores are occupied, which says the id is spoken for and nothing more.
  let occupied = TestGate::occupying(200);
  assert!(m.peek_yieldable_fork(&occupied).is_none(), "held");
  assert!(
    m.group(&7).unwrap().peek_pending_fork().is_some(),
    "the blob, the fence and the reservation all stand through a hold"
  );
  assert!(m.group(&7).unwrap().fork_obligations_standing());
  assert_eq!(
    m.poll_split_conflict(),
    Some((7, 200)),
    "one cue per episode"
  );
  assert_eq!(m.poll_split_conflict(), None);

  // The SAME child id now answers at the reserved terminal — a floor that can never lift.
  let floored = TestGate::flooring(200, crate::MERGED_FLOOR);
  assert!(
    m.peek_yieldable_fork(&floored).is_none(),
    "a terminal floor yields nothing"
  );
  assert!(
    m.group(&7).unwrap().peek_pending_fork().is_none(),
    "and consumes the fork: the verdict is about the fork, not the moment"
  );
  assert!(
    !m.group(&7).unwrap().fork_obligations_standing(),
    "the parent's barrier resolved with it rather than standing forever"
  );
  assert_eq!(
    m.poll_split_refusal(),
    Some((7, 200)),
    "exactly one refusal is queued"
  );
  assert_eq!(m.poll_split_refusal(), None, "and only one");
  assert_eq!(
    m.poll_split_conflict(),
    None,
    "the transition emits NO second cue — the park ended, it did not re-form"
  );
}

/// A CHILD BELOW ITS FLOOR is a verdict about the fork — the generation is fixed at the split and
/// the floor only rises — so the relay abandons it deliberately: popped, its barrier resolved, and
/// the refusal queued for the embedder rather than swallowed.
#[test]
fn a_child_below_its_floor_is_abandoned_deliberately() {
  let (mut m, _log, _stable) = host_with_staged_fork(200);
  let gate = TestGate::flooring(200, 9);

  assert!(
    m.peek_yieldable_fork(&gate).is_none(),
    "a below-floor child yields nothing"
  );
  assert!(
    m.group(&7).unwrap().peek_pending_fork().is_none(),
    "the fork was consumed, not held"
  );
  assert!(
    !m.group(&7).unwrap().fork_obligations_standing(),
    "the parent's obligation resolved with the abandonment"
  );
  assert_eq!(
    m.poll_split_refusal(),
    Some((7, 200)),
    "the embedder is told the child will never arrive by this route"
  );
  assert_eq!(m.poll_split_refusal(), None, "one refusal per fork");
  assert_eq!(
    m.poll_split_conflict(),
    None,
    "an abandonment is not a park: no cue"
  );
}

/// ARM ORDER IS AN INVARIANT. A hosted child always has engine stores, so a gate consulted BEFORE
/// the hosted-child branch would swallow every park into a stores-hold and make the ForkId
/// redundant exit unreachable — a legitimately-arrived twin would wedge its parent forever. With
/// an occupancy-answering gate active, a provenance-matched twin must still resolve REDUNDANT.
#[test]
fn a_hosted_child_still_resolves_redundant_with_the_gate_active() {
  let (mut m, _log, _stable) = host_with_staged_fork(200);
  let token = staged_fork_id(&m, 7);
  // The twin arrives carrying THIS split's token — the child is this fork, already materialized.
  m.create_group(
    200,
    0,
    single_node_cfg(1),
    Instant::ORIGIN,
    43,
    SplitSm::default(),
  )
  .unwrap();
  m.group_mut(&200).unwrap().seed_fork_id_for_test(token);
  let gate = TestGate::occupying(200);

  assert!(
    m.peek_yieldable_fork(&gate).is_none(),
    "a redundant fork yields nothing"
  );
  assert!(
    m.group(&7).unwrap().peek_pending_fork().is_none(),
    "the redundant arm ran: the fork resolved rather than holding forever"
  );
  assert_eq!(
    m.poll_split_conflict(),
    None,
    "a redundant resolution is not a conflict"
  );
  assert_eq!(m.poll_split_refusal(), None, "nor a refusal");
}

/// Removing the parent takes its held fork and the cue with it: the obligation is gone, so a cue
/// delivered afterwards could only goad the embedder into clearing a wedge that no longer exists.
#[test]
fn removing_the_parent_purges_a_held_fork_and_its_cue() {
  let (mut m, _log, _stable) = host_with_staged_fork(200);
  let gate = TestGate::occupying(200);
  assert!(m.peek_yieldable_fork(&gate).is_none(), "held");
  // The cue is queued but NOT yet consumed — a driver's bounded tail deferred it.
  m.remove_group(&7, &mut empty_stores()).unwrap();
  assert_eq!(
    m.poll_split_conflict(),
    None,
    "the parent's removal purged its undelivered cue"
  );
  assert!(
    m.peek_yieldable_fork(&gate).is_none(),
    "and left no fork behind to re-examine"
  );
}

/// Deliver one committed Split entry to follower group 7 at `index` (prev = `index - 1`, term 1),
/// giving `give` units to `child` at parent lineage `parent_gen_after`, and drain storage — the
/// SECOND split a parent stages after its first one has been dealt with.
fn follower_split_next(
  m: &mut MultiRaft<u64, u64, SplitSm>,
  log: &mut VecLog,
  stable: &mut AsyncStable,
  index: u64,
  child: u64,
  parent_gen_after: u64,
  give: u8,
) {
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
        crate::EntryKind::Split,
        split_entry_bytes(child, 0, parent_gen_after, give),
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

/// The durable log of a parent that took 3 units of load and then committed a split giving 2 of
/// them to `child` — the crash-recovery source a restart replays the staged fork out of.
fn split_survivor_log(child: u64) -> (VecLog, AsyncStable) {
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  let cmd = {
    let mut b = Vec::new();
    Bytes::from_static(b"c").encode(&mut b);
    Bytes::from(b)
  };
  log.force_append(&[
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
      split_entry_bytes(child, 0, 1, 2),
    ),
  ]);
  stable.force_state(Term::new(1), Some(1u64), Index::new(4));
  (log, stable)
}

/// THE REMOVAL-TIME ABANDONMENT: a host holding a fork for `C` that then removes its OWN `C` has
/// ended the very story this fork began, and the removed replica's [`ForkId`] PROVES the descent.
/// The fork is abandoned deliberately — otherwise it survives the removal and lands on the clean
/// slate the embedder's next consent makes, resurrecting a dead incarnation's baseline under an id
/// that has moved on.
#[test]
fn removing_a_child_descended_from_a_held_fork_abandons_it() {
  let (mut m, mut log, mut stable) = host_with_staged_fork(200);
  // The fork HOLDS first: the caller's stores already hold 200 (a sibling replica's transferred
  // baseline landed there), so the parent parks with one cue outstanding and undelivered.
  let gate = TestGate::occupying(200);
  assert!(
    m.peek_yieldable_fork(&gate).is_none(),
    "occupied stores hold the fork"
  );
  // That transferred baseline registers in the container carrying THIS split's token — the
  // provenance a snapshot transfer preserves exactly as a local materialization would.
  let token = staged_fork_id(&m, 7);
  m.create_group(
    200,
    0,
    single_node_cfg(1),
    Instant::ORIGIN,
    43,
    SplitSm::default(),
  )
  .unwrap();
  m.group_mut(&200).unwrap().seed_fork_id_for_test(token);

  m.remove_group(&200, &mut empty_stores()).unwrap();

  assert!(
    m.group(&7).unwrap().peek_pending_fork().is_none(),
    "the held fork died with the incarnation it produced"
  );
  assert!(
    !m.group(&7).unwrap().fork_obligations_standing(),
    "its durability barrier resolved with it — the parent is not left fenced"
  );
  assert!(
    !m.split_reserved(&200),
    "and the child id's reservation released"
  );
  assert_eq!(
    m.poll_split_refusal(),
    Some((7, 200)),
    "the abandonment is deliberate, so the embedder is told"
  );
  assert_eq!(m.poll_split_refusal(), None, "one refusal per fork");
  assert_eq!(
    m.poll_split_conflict(),
    None,
    "the park ended with the fork: its undelivered cue died with the episode"
  );
  assert!(
    m.peek_yieldable_fork(&NoHold).is_none(),
    "the id is free now and nothing is left to take it"
  );
  assert_eq!(
    m.poll_relay_guard_advance(),
    Some((7, 1)),
    "the parent's guard advanced past the abandoned head fork, for the caller to mirror durably"
  );
  assert_eq!(m.poll_relay_guard_advance(), None, "one advance per fork");

  // THE NEXT CHILD IS UNDAMAGED. Had the abandonment left the parent parked, this second fork's
  // park would dedupe against the dead episode and its cue would never surface.
  follower_commit_next(&mut m, &mut log, &mut stable, 5);
  follower_split_next(&mut m, &mut log, &mut stable, 6, 201, 2, 1);
  let gate = TestGate::occupying(201);
  assert!(
    m.peek_yieldable_fork(&gate).is_none(),
    "the parent's next fork holds on its own occupied child id"
  );
  assert_eq!(
    m.poll_split_conflict(),
    Some((7, 201)),
    "and cues for it — the dead park suppressed nothing"
  );
}

/// The abandonment's guard advance is what makes it SURVIVE a crash. A parent restored from the
/// durable log replays its split and re-stages the fork; fed the lineage record the caller
/// mirrored at the removal, the container folds that re-staged fork to a resolved no-op instead of
/// resurrecting the very fork the removal killed.
#[test]
fn the_mirrored_guard_folds_a_replayed_abandoned_fork() {
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let (mut log, mut stable) = split_survivor_log(200);
  m.restore_group_unchecked(
    7,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    SplitSm::default(),
    1,
    &mut log,
    &mut stable,
  )
  .unwrap();
  assert!(
    m.group(&7).unwrap().peek_pending_fork().is_some(),
    "the crash-replay re-staged the fork"
  );
  // The coordinators' restore path, replayed: the durable lineage record the removal's mirror
  // advanced is fed back to the relay guard.
  m.raise_relay_guard(&7, 1);

  assert!(
    m.peek_yieldable_fork(&NoHold).is_none(),
    "the guard folds the replayed fork instead of relaying it a second time"
  );
  assert!(
    m.group(&7).unwrap().peek_pending_fork().is_none(),
    "and consumes it: the abandonment stands across the crash"
  );
  assert!(
    !m.split_reserved(&200),
    "the child id is not re-reserved by a dead fork"
  );
  assert_eq!(m.poll_split_conflict(), None, "a folded fork is not a park");
  assert_eq!(
    m.poll_split_refusal(),
    None,
    "nor a fresh refusal — the embedder was told once, at the removal"
  );
}

/// The RED PROOF of the guard advance: the identical replay WITHOUT the mirrored record
/// resurrects the abandoned fork and aims it at the free child id. This is what the removal-time
/// abandonment would amount to if its guard bump were volatile only.
#[test]
fn a_replayed_abandoned_fork_resurrects_without_the_mirrored_guard() {
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let (mut log, mut stable) = split_survivor_log(200);
  m.restore_group_unchecked(
    7,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    SplitSm::default(),
    1,
    &mut log,
    &mut stable,
  )
  .unwrap();

  {
    let fork = m
      .peek_yieldable_fork(&NoHold)
      .expect("no guard, no defense: the dead fork relays again");
    assert_eq!(((*fork.parent()), (*fork.child())), (7, 200));
  }
  let mut engine: GroupEngine<u64, u64> = GroupEngine::new();
  assert!(matches!(
    m.install_yieldable_fork(&7, &200, &mut engine, &NoHold, Instant::ORIGIN, 43),
    InstallOutcome::Installed { .. }
  ));
  assert_eq!(
    m.group(&200).unwrap().state_machine().units,
    2,
    "carrying the dead incarnation's half onto the id's clean slate"
  );
}

/// The ForkId of `parent`'s staged fork at split index `index` — the below-head twin of
/// [`staged_fork_id`], for a queue with more than one fork in it.
fn staged_fork_id_at(m: &MultiRaft<u64, u64, SplitSm>, parent: u64, index: u64) -> ForkId {
  let f = m
    .group(&parent)
    .unwrap()
    .staged_forks()
    .find(|f| f.index == Index::new(index))
    .expect("a fork is staged at that index");
  mint_fork_id(
    &parent,
    f.parent_gen_after,
    f.index,
    f.split_term,
    f.child_bytes.clone(),
    f.child_gen,
  )
}

/// The relay guard's PROVENANCE: no hosted group's guard stands above the live lineage counter
/// ⊔ whatever durable fork record was fed to it. `guard <= live` alone is FALSE in an ordinary
/// window — a restore seeds the guard from a record whose backing entry is durable but not yet
/// re-committed — so the honest bound admits the record and refuses anything beyond it.
///
/// INV-MINT-ABOVE-GUARD, the property that actually protects a fresh fork, is asserted at the mint
/// site itself (`propose_split`), where the propose gate guarantees it.
fn assert_guard_provenance<F: StateMachine>(m: &MultiRaft<u64, u64, F>, record: u64) {
  for gid in m.group_ids() {
    let live = m
      .group(gid)
      .expect("group_ids yields hosted ids")
      .shape_gen();
    let guard = m.lineage.get(gid).copied().unwrap_or(0);
    assert!(
      guard <= live.max(record),
      "relay guard {guard} on group {gid} exceeds live counter {live} and record {record}"
    );
  }
}

/// A committed log carrying only ordinary traffic — NO lineage move, so a restart replay leaves the
/// live lineage counter exactly where the (absent) snapshot meta put it: zero.
fn lineage_free_committed_log() -> (VecLog, AsyncStable) {
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  let cmd = {
    let mut b = Vec::new();
    Bytes::from_static(b"c").encode(&mut b);
    Bytes::from(b)
  };
  log.force_append(&[
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
  ]);
  stable.force_state(Term::new(1), Some(1u64), Index::new(3));
  (log, stable)
}

/// A CATALOG generation never reaches the relay guard, so it can never fold the group's next fork.
///
/// The guard is seeded from the host's durable FORK record. When a restore was also allowed to
/// write its admitted generation into that record, the pair went INVERTED — live 0, guard N — and
/// the group's very next split minted 1, the relay read `1 <= N` as already-relayed, and the fork
/// folded to a resolved no-op: the staged baseline is the child partition's ONLY local copy on this
/// host, so the partition was silently discarded. Nothing errored and nothing diverged visibly —
/// the child simply never existed.
///
/// The record carries evidence only, so an id admitted at generation 5 over stores that account for
/// nothing leaves the guard where its own forks put it: at zero.
#[test]
fn a_catalog_generation_never_reaches_the_relay_guard() {
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let (mut log, mut stable) = lineage_free_committed_log();
  m.restore_group_unchecked(
    7,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    SplitSm::default(),
    1,
    &mut log,
    &mut stable,
  )
  .unwrap();
  assert_eq!(
    m.group(&7).unwrap().shape_gen(),
    0,
    "the replayed tail carries no lineage move, so replay evidence is zero"
  );

  // The restore door's guard raise, fed the id's DURABLE FORK RECORD. These stores have relayed no
  // fork and applied no lineage move, so that record reads zero however high the catalog's claim
  // was: the claim reaches admission and stops there.
  m.raise_relay_guard(&7, 0);
  assert_eq!(
    m.group(&7).unwrap().shape_gen(),
    0,
    "a catalog claim is not evidence this group applied anything"
  );
  assert_eq!(m.group_gen(&7), 0, "and the guard has nothing to stand on");
  assert_guard_provenance(&m, 0);

  // The next split mints one past the evidence, and the clamped guard is below it.
  let d = lead_single_split(&mut m, 7, &mut log, &mut stable);
  m.propose_split(
    &7,
    d,
    &mut log,
    &stable,
    &200,
    0,
    Bytes::from_static(b"\x01"),
  )
  .unwrap()
  .unwrap();
  m.flush_appends(&7, d, &log, &stable).unwrap();
  while matches!(
    m.handle_storage(&7, d, &mut log, &mut stable),
    Some(StorageProgress::MorePending)
  ) {}
  {
    let fork = m
      .peek_yieldable_fork(&NoHold)
      .expect("the fresh fork is relayed, never folded away as a replay duplicate");
    assert_eq!(*fork.child(), 200);
    assert_eq!(
      fork.parent_gen_after(),
      1,
      "the mint is one past the replay evidence, and clears the guard"
    );
  }

  // THE LOSS THIS PREVENTS, shown directly: a fork at or below the guard folds to a resolved
  // no-op and its staged blob goes with it. That is what a laundered catalog generation of 5
  // manufactured for every fresh fork this incarnation could ever mint.
  m.raise_relay_guard(&7, 1);
  assert!(
    m.peek_yieldable_fork(&NoHold).is_none(),
    "a fork at or below the guard is folded away — the shape a laundered catalog generation \
     manufactured for every fresh fork"
  );
}

/// Raising the relay guard never moves the live lineage counter. The two answer different questions
/// from different sources: the live counter is replicated state that every apply-time lineage guard
/// reads for EXACT equality, the guard is volatile per-host bookkeeping seeded from a durable fork
/// record. The guard may legitimately stand ahead of the counter — its record's backing entry can be
/// durable but not yet re-committed — and what protects a fresh fork is not an ordering between them
/// but the propose gate, which blocks every mint until the counter has absorbed that entry.
#[test]
fn raising_the_relay_guard_never_moves_the_live_lineage_counter() {
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let (log, mut stable) = (VecLog::default(), AsyncStable::default());
  m.create_group_founded_at(
    7,
    4,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    SplitSm::default(),
    1,
    &log,
    &mut stable,
  )
  .unwrap();
  assert_eq!(m.group(&7).unwrap().shape_gen(), 4);

  // A record ahead of the counter raises the GUARD and nothing else.
  m.raise_relay_guard(&7, 9);
  assert_eq!(
    m.group(&7).unwrap().shape_gen(),
    4,
    "a durable record never enters the replicated apply counter"
  );
  assert_eq!(m.group_gen(&7), 9, "the guard alone took the record");
  assert_guard_provenance(&m, 9);

  // A record BELOW the standing guard never lowers it (monotone), and still moves no counter.
  m.raise_relay_guard(&7, 2);
  assert_eq!(m.group(&7).unwrap().shape_gen(), 4);
  assert_eq!(m.group_gen(&7), 9);
  assert_guard_provenance(&m, 9);
}

/// A restored group's live lineage counter is EXACTLY what its replay produced — no per-host record
/// raises it, and none lowers it. This is INV-APPLY-LINEAGE at the restore door, the one place a
/// value from outside the replicated log has ever been offered to the counter.
#[test]
fn a_restore_leaves_the_live_counter_at_its_replay_evidence() {
  // A committed tail with NO lineage move replays to zero.
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let (mut log, mut stable) = lineage_free_committed_log();
  m.restore_group_unchecked(
    7,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    SplitSm::default(),
    1,
    &mut log,
    &mut stable,
  )
  .unwrap();
  assert_eq!(m.group(&7).unwrap().shape_gen(), 0);
  m.raise_relay_guard(&7, 9);
  assert_eq!(
    m.group(&7).unwrap().shape_gen(),
    0,
    "a durable record does not move the counter the apply guards read"
  );
  assert_guard_provenance(&m, 9);

  // A committed tail that DOES carry a split replays to that split's generation — and a record
  // below it is just as inert as one above.
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let (mut log, mut stable) = split_survivor_log(200);
  m.restore_group_unchecked(
    7,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    SplitSm::default(),
    1,
    &mut log,
    &mut stable,
  )
  .unwrap();
  assert_eq!(
    m.group(&7).unwrap().shape_gen(),
    1,
    "the replayed split is the counter's only mover"
  );
  m.raise_relay_guard(&7, 0);
  assert_eq!(m.group(&7).unwrap().shape_gen(), 1);
  assert_guard_provenance(&m, 0);
}

/// ONE COMMITTED SPLIT, ONE ARM, EVERY REPLICA. The apply-time lineage guard admits an entry only at
/// `shape_gen + 1` exactly: a match PARTITIONS the state machine, a mismatch is a no-op that leaves
/// it untouched. Two replicas that disagree on that comparison therefore disagree about a committed
/// entry's effect on their state machines — permanently, and silently.
///
/// The comparison is safe only while its input is replicated. Here one replica's host holds a
/// durable lineage record its own log and snapshot cannot account for; the other holds none. Feeding
/// that record into the live counter would put the two replicas one comparison apart on the very
/// next committed split.
#[test]
fn one_committed_split_takes_the_same_arm_on_every_replica() {
  let (mut a, mut la, mut sa) = host_with_staged_fork(200);
  let (mut b, mut lb, mut sb) = host_with_staged_fork(200);
  assert_eq!(a.group(&7).unwrap().shape_gen(), 1);
  assert_eq!(b.group(&7).unwrap().shape_gen(), 1);
  let units_before = a.group(&7).unwrap().state_machine().units;
  assert_eq!(b.group(&7).unwrap().state_machine().units, units_before);

  // Only replica A's host carries the above-evidence record.
  a.raise_relay_guard(&7, 5);

  // The leader mints its next split from the lineage every replica shares, and the SAME committed
  // entry reaches both.
  follower_commit_next(&mut a, &mut la, &mut sa, 5);
  follower_commit_next(&mut b, &mut lb, &mut sb, 5);
  follower_split_next(&mut a, &mut la, &mut sa, 6, 201, 2, 1);
  follower_split_next(&mut b, &mut lb, &mut sb, 6, 201, 2, 1);

  assert_eq!(
    a.group(&7).unwrap().state_machine().units,
    b.group(&7).unwrap().state_machine().units,
    "the committed split partitioned both state machines or neither — a per-host record must \
     never decide which"
  );
  assert_eq!(
    a.group(&7).unwrap().shape_gen(),
    b.group(&7).unwrap().shape_gen(),
    "and both replicas' lineage counters moved together"
  );
  assert_eq!(
    a.group(&7).unwrap().staged_forks().count(),
    2,
    "the fresh split staged its fork on the replica whose host held the record"
  );
  assert_eq!(
    b.group(&7).unwrap().staged_forks().count(),
    2,
    "and on the replica whose host held none"
  );
  assert_guard_provenance(&a, 5);
  assert_guard_provenance(&b, 0);
}

/// The relay guard's provenance holds at every door that admits a group or moves the guard: a
/// genesis create, a restore under a durable record, a fork-born child, a relayed fork's guard
/// advance, and a removal-time abandonment's advance. No door ever puts the guard beyond the live
/// counter ⊔ the record it was fed.
#[test]
fn the_relay_guard_keeps_its_provenance_at_every_door() {
  // CREATE: the genesis seed puts both at the admitted generation.
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let (founding_log, mut founding_stable) = (VecLog::default(), AsyncStable::default());
  m.create_group_founded_at(
    7,
    3,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    SplitSm::default(),
    1,
    &founding_log,
    &mut founding_stable,
  )
  .unwrap();
  assert_guard_provenance(&m, 0);

  // RESTORE, under a record standing above the replay evidence.
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let (mut log, mut stable) = lineage_free_committed_log();
  m.restore_group_unchecked(
    7,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    SplitSm::default(),
    1,
    &mut log,
    &mut stable,
  )
  .unwrap();
  m.raise_relay_guard(&7, 8);
  assert_guard_provenance(&m, 8);

  // FORK-CREATE: the child's baseline meta carries its own generation, so the counter is seeded
  // from replicated state and the guard is seeded with it.
  let (mut clog, mut cstable) = (VecLog::default(), AsyncStable::default());
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
    &mut clog,
    &mut cstable,
  )
  .unwrap();
  assert_guard_provenance(&m, 8);

  // RELAY: a materialized fork advances the guard to the applied split's generation.
  let (mut m, _log, _stable) = host_with_staged_fork(200);
  assert_guard_provenance(&m, 0);
  install_head_fork(&mut m, 7, 200, Instant::ORIGIN);
  assert_guard_provenance(&m, 0);

  // ABANDONMENT: a removal that ends the incarnation a staged fork produced advances the guard
  // past that fork.
  let (mut m, _log, _stable) = host_with_staged_fork(201);
  let gate = TestGate::occupying(201);
  assert!(m.peek_yieldable_fork(&gate).is_none(), "the fork holds");
  let token = staged_fork_id(&m, 7);
  m.create_group(
    201,
    0,
    single_node_cfg(1),
    Instant::ORIGIN,
    44,
    SplitSm::default(),
  )
  .unwrap();
  m.group_mut(&201).unwrap().seed_fork_id_for_test(token);
  assert_guard_provenance(&m, 0);
  m.remove_group(&201, &mut empty_stores()).unwrap();
  assert_eq!(
    m.poll_relay_guard_advance(),
    Some((7, 1)),
    "the abandonment advanced the guard past the fork it killed"
  );
  assert_guard_provenance(&m, 0);
}

/// A parent's durable log carrying TWO committed splits — 3 units of load, a split giving 2 to
/// `first`, one more unit, then a split giving 1 to `second`. The crash-recovery source for the
/// ordered-abandonment replay.
fn two_split_survivor_log(first: u64, second: u64) -> (VecLog, AsyncStable) {
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  let cmd = {
    let mut b = Vec::new();
    Bytes::from_static(b"c").encode(&mut b);
    Bytes::from(b)
  };
  log.force_append(&[
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
    crate::Entry::new(
      Term::new(1),
      Index::new(3),
      crate::EntryKind::Normal,
      cmd.clone(),
    ),
    crate::Entry::new(
      Term::new(1),
      Index::new(4),
      crate::EntryKind::Split,
      split_entry_bytes(first, 0, 1, 2),
    ),
    crate::Entry::new(Term::new(1), Index::new(5), crate::EntryKind::Normal, cmd),
    crate::Entry::new(
      Term::new(1),
      Index::new(6),
      crate::EntryKind::Split,
      split_entry_bytes(second, 0, 2, 1),
    ),
  ]);
  stable.force_state(Term::new(1), Some(1u64), Index::new(6));
  (log, stable)
}

/// ORDERED ABANDONMENT BELOW THE HEAD. A lagging host applies a batch carrying two splits while a
/// sibling has already materialized the LATER child and transferred its token-bearing baseline
/// here. The first fork parks on an occupied id; the second's child is hosted-then-REMOVED, so the
/// removal condemns a fork that is not at the head. It must not be consumed out of turn — that
/// would reorder the queue and advance the guard past the fork still in front of it — and it must
/// not be left alive either, or it heads later and resurrects the removed incarnation.
#[test]
fn a_removal_condemns_a_fork_below_the_head_and_the_drain_takes_it_in_order() {
  let (mut m, mut log, mut stable) = host_with_staged_fork(200);
  follower_commit_next(&mut m, &mut log, &mut stable, 5);
  follower_split_next(&mut m, &mut log, &mut stable, 6, 201, 2, 1);
  assert_eq!(
    m.group(&7).unwrap().staged_forks().count(),
    2,
    "two forks are queued, in apply order"
  );

  // The FIRST fork parks: its child id is occupied by caller storage.
  let gate = TestGate::occupying(200);
  assert!(m.peek_yieldable_fork(&gate).is_none(), "the head parks");
  // The SECOND fork's child arrived here by transfer, carrying that split's own token, and the
  // embedder then tore it down.
  let token = staged_fork_id_at(&m, 7, 6);
  m.create_group(
    201,
    0,
    single_node_cfg(1),
    Instant::ORIGIN,
    44,
    SplitSm::default(),
  )
  .unwrap();
  m.group_mut(&201).unwrap().seed_fork_id_for_test(token);
  m.remove_group(&201, &mut empty_stores()).unwrap();

  // CONDEMNED, NOT CONSUMED. The queue is untouched, so the head keeps its place, and nothing about
  // the second fork has been announced or advanced yet.
  assert_eq!(
    m.group(&7).unwrap().staged_forks().count(),
    2,
    "the marked fork stays queued behind the head"
  );
  assert_eq!(
    m.group(&7).unwrap().peek_pending_fork().map(|f| f.index),
    Some(Index::new(4)),
    "and the head is still the FIRST fork"
  );
  assert_eq!(
    m.poll_relay_guard_advance(),
    None,
    "the guard must not advance past a fork still staged in front of the condemned one"
  );
  assert_eq!(m.poll_split_refusal(), None, "nor is the refusal owed yet");
  assert!(
    m.split_reserved(&201),
    "the condemned fork still reserves its child id while it is queued"
  );

  // The first fork's conflict clears and it materializes normally.
  {
    let first = m
      .peek_yieldable_fork(&NoHold)
      .expect("the head fork lands once its id frees");
    assert_eq!(((*first.parent()), (*first.child())), (7, 200));
  }
  let mut engine: GroupEngine<u64, u64> = GroupEngine::new();
  assert!(matches!(
    m.install_yieldable_fork(&7, &200, &mut engine, &NoHold, Instant::ORIGIN, 43),
    InstallOutcome::Installed { .. }
  ));

  // NOW the mark is at the head, and the very next drain takes it — before any verdict.
  assert!(
    m.peek_yieldable_fork(&NoHold).is_none(),
    "the condemned fork is consumed, never yielded onto the id it would resurrect"
  );
  assert!(
    m.group(&7).unwrap().peek_pending_fork().is_none(),
    "the queue drained in order"
  );
  assert_eq!(
    m.poll_split_refusal(),
    Some((7, 201)),
    "and the refusal surfaces at consumption"
  );
  assert_eq!(
    m.poll_relay_guard_advance(),
    Some((7, 2)),
    "with the guard advance owed to the caller's durable record"
  );
  assert!(!m.split_reserved(&201), "the reservation released with it");
}

/// The replay half of the ordered abandonment: a parent restored from a durable log carrying BOTH
/// splits re-stages both forks, and the mirrored guard folds both — the one that legitimately
/// materialized and the one the removal condemned.
#[test]
fn the_mirrored_guard_folds_both_replayed_forks_after_an_ordered_abandonment() {
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let (mut log, mut stable) = two_split_survivor_log(200, 201);
  m.restore_group_unchecked(
    7,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    SplitSm::default(),
    1,
    &mut log,
    &mut stable,
  )
  .unwrap();
  assert_eq!(
    m.group(&7).unwrap().staged_forks().count(),
    2,
    "the crash-replay re-staged both forks"
  );
  m.raise_relay_guard(&7, 2);

  assert!(
    m.peek_yieldable_fork(&NoHold).is_none(),
    "the guard folds both replayed forks"
  );
  assert!(
    m.group(&7).unwrap().peek_pending_fork().is_none(),
    "and consumes them: neither the materialized fork nor the abandoned one comes back"
  );
  assert!(!m.split_reserved(&200));
  assert!(!m.split_reserved(&201));
}

/// A REDUNDANT RESOLUTION IS A CONSUMPTION, so it persists its guard advance. The fork's blob is
/// discarded because the transferred twin already carries this split's baseline — but if only the
/// VOLATILE guard moved, the discard would not survive the restart that follows it. Crash after the
/// redundant fold and the child's removal, before a parent snapshot covers the split: the replayed
/// split re-stages the fork against a durable guard that never learned of the fold, and once the
/// embedder consents to re-admitting the id the fork reinstalls the very baseline the removal ended.
/// The queued advance is what the caller mirrors durably, and it is what folds the replay.
#[test]
fn a_redundant_resolution_persists_its_guard_advance_across_a_crash() {
  let (mut m, mut log, mut stable) = host_with_staged_fork(200);

  // The transferred twin: a sibling replica's manufactured baseline arrived here carrying THIS
  // split's exact token, which is what authorizes the redundant discard.
  let token = staged_fork_id(&m, 7);
  let (mut log200, mut stable200) = (VecLog::default(), AsyncStable::default());
  m.create_group_from_fork_unreserved(
    200,
    0,
    single_node_cfg(1),
    Instant::ORIGIN,
    43,
    SplitSm::default(),
    fork_blob(2),
    None,
    Some(token),
    1,
    &mut log200,
    &mut stable200,
  )
  .unwrap();

  assert!(
    m.peek_yieldable_fork(&NoHold).is_none(),
    "the twin's token resolves the fork as redundant"
  );
  assert!(
    m.group(&7).unwrap().peek_pending_fork().is_none(),
    "and CONSUMES it — the blob is gone"
  );

  // THE DURABLE HALF the consumption owes. A driver mirrors this into its lineage record beside
  // the writes it was already making; here the test plays that mirror.
  let durable_guard = m
    .poll_relay_guard_advance()
    .expect("a redundant consumption owes its caller a durable guard advance");
  assert_eq!(durable_guard, (7, 1));

  // The embedder ends the child's local story. Nothing about the parent's log changed, and no
  // parent snapshot has covered the split yet — this is exactly the crash window.
  m.remove_group(&200, &mut empty_stores()).unwrap();
  drop(m);

  // CRASH + REPLAY. The restored parent replays its split entry and re-stages the fork; the guard
  // is seeded from the DURABLE mirror, which is the only place the fold was recorded.
  let mut m2: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  m2.restore_group_unchecked(
    7,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    SplitSm::default(),
    2,
    &mut log,
    &mut stable,
  )
  .unwrap();
  assert_eq!(
    m2.group(&7).unwrap().staged_forks().count(),
    1,
    "the replay re-staged the fork"
  );
  m2.raise_relay_guard(&durable_guard.0, durable_guard.1);

  // The re-admission consent the embedder would give next: nothing occupies 200 any more.
  let mut engine: GroupEngine<u64, u64> = GroupEngine::new();
  assert!(
    m2.peek_yieldable_fork(&NoHold).is_none(),
    "the mirrored advance folds the replayed fork to a duplicate"
  );
  assert_eq!(
    m2.install_yieldable_fork(&7, &200, &mut engine, &NoHold, Instant::ORIGIN, 44),
    InstallOutcome::Empty,
    "so nothing reinstalls the baseline the removal ended"
  );
  assert!(!m2.contains_group(&200), "the child stays gone");
  assert!(
    !engine.contains_group(&200),
    "and no storage was made for it"
  );
}

/// THE RELAY GUARD IS MONOTONE, and the removal-time abandonment is the site that proved it has to
/// be. The guard legitimately runs AHEAD of the staged forks: a restore raises it from the caller's
/// DURABLE lineage mirror ([`MultiRaft::raise_relay_guard`]), which is what folds a replayed tail's
/// forks to resolved no-ops. Writing the abandoned fork's own bump over that higher guard REGRESSED
/// it, and the next replayed fork — minted between the abandoned one and the true guard — stopped
/// reading as a duplicate and installed a dead incarnation's baseline over the child's real durable
/// progress. The ordered-abandonment test cannot see this: it condemns BOTH forks, so nothing is
/// left behind to be let through.
#[test]
fn abandoning_a_below_guard_fork_never_regresses_the_relay_guard() {
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let (mut log, mut stable) = two_split_survivor_log(200, 201);
  m.restore_group_unchecked(
    7,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    SplitSm::default(),
    1,
    &mut log,
    &mut stable,
  )
  .unwrap();
  assert_eq!(
    m.group(&7).unwrap().staged_forks().count(),
    2,
    "the crash-replay re-staged both forks"
  );
  // The durable mirror already covers BOTH: fork 1 bumps the lineage to 1, fork 2 to 2.
  m.raise_relay_guard(&7, 2);

  // The FIRST fork's child is hosted here carrying that split's own token — it arrived by transfer
  // before the crash — and the embedder tears it down. The (d) abandonment fires on the HEAD fork,
  // whose own bump (1) is BELOW the guard the restore raised.
  let token = staged_fork_id_at(&m, 7, 4);
  m.create_group(
    200,
    0,
    single_node_cfg(1),
    Instant::ORIGIN,
    44,
    SplitSm::default(),
  )
  .unwrap();
  m.group_mut(&200).unwrap().seed_fork_id_for_test(token);
  m.remove_group(&200, &mut empty_stores()).unwrap();

  // THE SECOND FORK IS STILL A DUPLICATE. Under a regressed guard it yields instead, and the
  // install manufactures a baseline for an incarnation the durable record says is long past.
  assert!(
    m.peek_yieldable_fork(&NoHold).is_none(),
    "the guard still covers the second replayed fork — the abandonment raised it, never lowered it"
  );
  let mut engine: GroupEngine<u64, u64> = GroupEngine::new();
  assert_eq!(
    m.install_yieldable_fork(&7, &201, &mut engine, &NoHold, Instant::ORIGIN, 45),
    InstallOutcome::Empty,
    "the guard consumed both forks, so nothing installs the dead incarnation's half"
  );
  assert!(!m.contains_group(&201), "child 201 was never materialized");
  assert!(
    !engine.contains_group(&201),
    "and no storage was made for it"
  );
  assert!(
    m.group(&7).unwrap().peek_pending_fork().is_none(),
    "both forks are finished with: one abandoned, one folded"
  );
}

/// PEEK AND INSTALL AGREE, because the install decides on the PAIR the peek named rather than on
/// whatever a second global drain would reach. The drain's own arms MUTATE its walk — the parked
/// sweep unparks and re-dirties the parent it hands back before returning it — so a re-draining
/// install starts from a different place than the peek did. With two parents' forks releasing on
/// one crank the second drain legitimately reached the OTHER parent, and the install for the
/// peeked child answered `NotYieldable` on a perfectly legal state; the three simulation harnesses
/// and the parity bench turn that answer into a panic.
#[test]
fn two_parks_releasing_together_install_the_pair_the_peek_named() {
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let (mut log7, mut stable7) = (VecLog::default(), AsyncStable::default());
  let (mut log8, mut stable8) = (VecLog::default(), AsyncStable::default());
  let cfg = || {
    Config::try_new(
      1u64,
      std::vec![1u64, 2],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap()
    .with_snapshot_threshold(1)
  };
  m.create_group(7, 0, cfg(), Instant::ORIGIN, 42, SplitSm::default())
    .unwrap();
  m.create_group(8, 0, cfg(), Instant::ORIGIN, 43, SplitSm::default())
    .unwrap();
  follower_load_and_split_on(&mut m, 7, &mut log7, &mut stable7, 200);
  follower_load_and_split_on(&mut m, 8, &mut log8, &mut stable8, 201);

  // BOTH PARK, on their own children's occupied storage, and both release on the same crank.
  let occupied = TestGate {
    occupied: [200u64, 201].into_iter().collect(),
    floors: BTreeMap::new(),
  };
  assert!(m.peek_yieldable_fork(&occupied).is_none(), "both hold");
  assert_eq!(
    core::iter::from_fn(|| m.poll_split_conflict()).count(),
    2,
    "two parents parked, one cue each"
  );

  // The peek names one pair; the install must land THAT pair, not the one a second sweep reaches.
  let mut engine: GroupEngine<u64, u64> = GroupEngine::new();
  let (parent, child) = {
    let view = m
      .peek_yieldable_fork(&NoHold)
      .expect("the gate cleared, so a fork is yieldable");
    ((*view.parent()), (*view.child()))
  };
  assert!(
    matches!(
      m.install_yieldable_fork(&parent, &child, &mut engine, &NoHold, Instant::ORIGIN, 44),
      InstallOutcome::Installed { .. }
    ),
    "the install landed the pair the peek named ({parent} -> {child})"
  );
  assert!(m.contains_group(&child));

  // And the OTHER parent's fork lands on the next turn of the same loop.
  let (parent2, child2) = {
    let view = m
      .peek_yieldable_fork(&NoHold)
      .expect("the second parent's fork is yieldable too");
    ((*view.parent()), (*view.child()))
  };
  assert_ne!((parent, child), (parent2, child2), "a different pair");
  assert!(matches!(
    m.install_yieldable_fork(&parent2, &child2, &mut engine, &NoHold, Instant::ORIGIN, 45),
    InstallOutcome::Installed { .. }
  ));
  assert!(m.contains_group(&child2));
  assert!(
    m.peek_yieldable_fork(&NoHold).is_none(),
    "both partitions landed; nothing is left staged"
  );
}

/// THE CORE API'S OWN DOOR. Reached directly — no coordinator anywhere — a caller-driven fork
/// install must refuse a child id a split owns, in BOTH windows: between propose and apply, and
/// while the committed fork is staged. Without that fence a caller installs a group of its own at
/// the reserved id carrying a token it chose; the genuine fork then finds the id hosted, matches
/// that token, and resolves REDUNDANT — discarding the child partition's only local copy.
#[test]
fn the_exported_containers_public_fork_door_is_fenced_and_token_less() {
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
  m.propose_split(
    &7,
    d,
    &mut log,
    &stable,
    &200,
    0,
    Bytes::from_static(b"\x01"),
  )
  .unwrap()
  .unwrap();

  // A token of the caller's own choosing is no longer even EXPRESSIBLE at this door: the parameter
  // is gone, so the public install is token-less by construction and nothing a caller supplies can
  // wear a genuine fork's identity. What remains to prove is the reservation, in both windows.
  let (mut clog, mut cstable) = (VecLog::default(), AsyncStable::default());
  let refuse =
    |m: &mut MultiRaft<u64, u64, SplitSm>, clog: &mut VecLog, cstable: &mut AsyncStable| {
      m.create_group_from_fork(
        200,
        0,
        single_node_cfg(1),
        Instant::ORIGIN,
        43,
        SplitSm::default(),
        Bytes::from_static(b"\x00"),
        None,
        1,
        clog,
        cstable,
      )
    };
  assert_eq!(
    refuse(&mut m, &mut clog, &mut cstable),
    Err(CreateGroupError::SplitReserved),
    "the PROPOSE window refuses: the id is spoken for before the split even applies"
  );
  assert_eq!(
    <VecLog as crate::LogStore>::last_index(&clog),
    Index::ZERO,
    "and refuses before any store write"
  );

  // Apply the split: the propose window closes and the STAGED window opens behind it.
  while matches!(
    m.handle_storage(&7, d, &mut log, &mut stable),
    Some(StorageProgress::MorePending)
  ) {}
  while m.poll_message().is_some() {}
  while m.poll_event().is_some() {}
  assert!(
    m.group(&7).unwrap().peek_pending_fork().is_some(),
    "the committed fork is staged"
  );
  assert_eq!(
    refuse(&mut m, &mut clog, &mut cstable),
    Err(CreateGroupError::SplitReserved),
    "the STAGED window refuses too"
  );
  assert_eq!(
    <VecLog as crate::LogStore>::last_index(&clog),
    Index::ZERO,
    "still nothing written"
  );

  // The genuine fork then lands, its blob intact — nothing was discarded against a forged twin.
  let fork = m
    .peek_yieldable_fork(&NoHold)
    .expect("the real fork materializes");
  assert_eq!((*fork.parent(), *fork.child()), (7, 200));
  assert!(
    m.split_reserved(&200),
    "and the reservation OUTLIVES the yield: the pop ended the staged leg, not the id's claim, so \
     the window between the yield and the sealed install is fenced too"
  );
}

/// One group's durable log that both SPLITS to `child` at index 4 and later COMMITS a merge
/// absorbing `child` as its source at index 6 — the split-then-reunite shape, with the fork staged
/// BELOW the park coordinate exactly as the drain-stop guarantees. `closer` is what sits at the
/// park's `k + 1` (index 7): `None` leaves the window OPEN (commit stops at 6).
fn split_then_absorb_log(child: u64, closer: Option<crate::EntryKind>) -> (VecLog, AsyncStable) {
  let cmd = {
    let mut b = Vec::new();
    Bytes::from_static(b"c").encode(&mut b);
    Bytes::from(b)
  };
  let mut source_bytes = Vec::new();
  Data::encode(&child, &mut source_bytes);
  // `target_gen_after` must clear the bump the split at index 4 already made, or the apply-time
  // lineage guard reads the commit as a stale mint and no-ops it instead of parking.
  let payload = crate::CommitMergePayload::new(
    Bytes::from(source_bytes.clone()),
    Index::new(2),
    Term::new(1),
    1,
    2,
  );
  let mut cbuf = Vec::new();
  crate::wire::encode_commit_merge_payload(&payload, &mut cbuf);
  let mut entries = std::vec![
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
    crate::Entry::new(
      Term::new(1),
      Index::new(3),
      crate::EntryKind::Normal,
      cmd.clone()
    ),
    crate::Entry::new(
      Term::new(1),
      Index::new(4),
      crate::EntryKind::Split,
      split_entry_bytes(child, 0, 1, 2),
    ),
    crate::Entry::new(Term::new(1), Index::new(5), crate::EntryKind::Normal, cmd),
    crate::Entry::new(
      Term::new(1),
      Index::new(6),
      crate::EntryKind::CommitMerge,
      Bytes::from(cbuf),
    ),
  ];
  let commit = match closer {
    None => Index::new(6),
    Some(kind) => {
      let data = if kind == crate::EntryKind::RollbackMerge {
        let p = crate::RollbackMergePayload::abort(Bytes::from(source_bytes), 1, 1);
        let mut b = Vec::new();
        crate::wire::encode_rollback_merge_payload(&p, &mut b);
        Bytes::from(b)
      } else {
        Bytes::new()
      };
      entries.push(crate::Entry::new(Term::new(1), Index::new(7), kind, data));
      Index::new(7)
    }
  };
  let mut log = VecLog::default();
  log.force_append(&entries);
  let mut stable = AsyncStable::default();
  stable.force_state(Term::new(1), Some(1u64), commit);
  (log, stable)
}

/// A container whose group 7 is BOTH the parent of a staged fork for `child` and the target parked
/// on a committed `CommitMerge` absorbing that same `child` — the only shape in which abandoning
/// the fork loses nothing. The child itself is not hosted: C was removed before the freeze (a park
/// formed after a removal is refused `SpokenFor`/`Frozen`), so the committed entries are replayed
/// in rather than proposed.
fn fork_and_park_on_same_child(
  child: u64,
  closer: Option<crate::EntryKind>,
) -> (MultiRaft<u64, u64, SplitSm>, MapStores) {
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let mut stores = MapStores(
    std::collections::BTreeMap::new(),
    std::collections::BTreeSet::new(),
  );
  let (mut log, mut stable) = split_then_absorb_log(child, closer);
  m.restore_group_unchecked(
    7,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    SplitSm::default(),
    1,
    &mut log,
    &mut stable,
  )
  .unwrap();
  assert!(
    m.group(&7).unwrap().peek_pending_fork().is_some(),
    "the parent re-staged its fork"
  );
  assert!(
    m.group(&7).unwrap().pending_merge().is_some(),
    "and re-parked on the replayed CommitMerge absorbing that same child"
  );
  stores.0.insert(7, (log, stable));
  (m, stores)
}

/// THE COMMITTED-CONSUMED CHILD, abandoned. A parent stages a fork for `C` while a co-hosted target
/// is parked absorbing `C`, and that park's abort window has LATCHED CLOSED — `k + 1` is committed
/// and is not this merge's abort, so no abort can ever resurrect `C`. The fork must be abandoned
/// TERMINALLY: the union subsumes its blob, and holding it instead keeps the parent's fence
/// standing, which suppresses the cure advertisement that would deliver the covering snapshot the
/// park is waiting for — the ring this closes.
#[test]
fn a_fork_for_a_latched_closed_parks_source_is_abandoned() {
  let (mut m, mut stores) = fork_and_park_on_same_child(200, Some(crate::EntryKind::Empty));
  assert!(
    !m.group(&7)
      .unwrap()
      .pending_merge()
      .unwrap()
      .window_closed(),
    "a freshly replayed park is minted undecided"
  );
  assert!(
    m.service_merge_applies(Instant::ORIGIN, &mut stores)
      .is_empty(),
    "the source is unhosted, so nothing resolves — the pass only reads the window"
  );
  assert!(
    m.group(&7)
      .unwrap()
      .pending_merge()
      .unwrap()
      .window_closed(),
    "the CLOSED verdict latched onto the park"
  );

  // The parent's ONLY outstanding barrier in this fixture is this fork, so the fence emptying is
  // observable directly.
  assert!(m.group(&7).unwrap().fork_barrier_standing());
  // Occupancy is asserted TOO: the consumed source's stores are retained in reality, and the
  // refusal must win over them — that is why it is an Err-arm verdict.
  let gate = TestGate::occupying(200);
  assert!(
    m.peek_yieldable_fork(&gate).is_none(),
    "the fork is abandoned, not yielded"
  );
  assert!(
    m.group(&7).unwrap().peek_pending_fork().is_none(),
    "and consumed rather than held"
  );
  assert_eq!(
    m.poll_split_refusal(),
    Some((7, 200)),
    "the embedder is told the child will never arrive by this route"
  );
  assert!(
    !m.group(&7).unwrap().fork_barrier_standing(),
    "the parent's fence empties — the advertisement gate's fork leg clears"
  );
}

/// THE UNRELATED PARENT. The subsumption argument that licenses abandoning a fork is narrow: it
/// rests on the park stopping THIS endpoint's drain at `k - 1`, so a fork staged here below the
/// park predates the freeze and the absorbed union contains its half. An unrelated parent's
/// committed split merely NAMES the same id — its child-half was never in that union and can never
/// be recovered from it. Terminal there is data loss, so the arm HOLDS: the blob stays staged,
/// diagnosably wedged, and every unit the split moved is still accounted for.
#[test]
fn an_unrelated_parents_fork_for_a_consumed_id_holds_rather_than_dropping_its_half() {
  let (mut m, mut stores) = fork_and_park_on_same_child(200, Some(crate::EntryKind::Empty));
  // Q: a DIFFERENT parent whose own committed split gives a NONZERO half to the same child id.
  let (mut qlog, mut qstable) = split_survivor_log(200);
  m.restore_group_unchecked(
    9,
    single_node_cfg(1),
    Instant::ORIGIN,
    11,
    SplitSm::default(),
    1,
    &mut qlog,
    &mut qstable,
  )
  .unwrap();
  stores.0.insert(9, (qlog, qstable));
  let q_half = m
    .group(&9)
    .unwrap()
    .peek_pending_fork()
    .expect("Q staged its own fork")
    .blob
    .clone();
  assert_eq!(q_half, fork_blob(2), "Q's half carries two units");
  let q_units = m.group(&9).unwrap().state_machine().units;

  assert!(
    m.service_merge_applies(Instant::ORIGIN, &mut stores)
      .is_empty()
  );
  assert!(
    m.group(&7)
      .unwrap()
      .pending_merge()
      .unwrap()
      .window_closed()
  );

  // Group 7 IS the absorbing target and its fork predates the park: subsumed, so abandoned.
  // Group 9 is not, so its fork is HELD — the union cannot contain what Q split away.
  assert!(m.peek_yieldable_fork(&NoHold).is_none());
  assert!(
    m.group(&7).unwrap().peek_pending_fork().is_none(),
    "the target's own fork was subsumed and abandoned"
  );
  assert_eq!(m.poll_split_refusal(), Some((7, 200)));
  assert_eq!(
    m.poll_split_refusal(),
    None,
    "and ONLY that one — Q's fork was not refused"
  );
  let held = m
    .group(&9)
    .unwrap()
    .peek_pending_fork()
    .expect("Q's fork is still staged, blob intact");
  assert_eq!(held.blob, q_half, "with every unit it split away");
  assert_eq!(
    m.group(&9).unwrap().state_machine().units + 2,
    q_units + 2,
    "conservation across the verdict: Q kept its remainder and its half is still held"
  );
  assert!(
    m.group(&9).unwrap().fork_obligations_standing(),
    "the hold is diagnosable — Q still owes its staged fork"
  );
}

/// THE M1 NEGATIVE. A park whose window is still OPEN is UNDECIDED: rollback deliberately races a
/// parked commit, and an abort resurrects the source. Nothing may be abandoned on that evidence.
/// Composed with the abort actually landing at `k + 1`: the fork is still staged when the source
/// thaws, which is the only reason the child can still be materialized.
#[test]
fn an_open_park_abandons_nothing_and_the_abort_leaves_the_fork_staged() {
  let (mut m, mut stores) = fork_and_park_on_same_child(200, None);
  assert!(
    m.service_merge_applies(Instant::ORIGIN, &mut stores)
      .is_empty(),
    "an open window resolves nothing"
  );
  assert!(
    !m.group(&7)
      .unwrap()
      .pending_merge()
      .unwrap()
      .window_closed(),
    "an OPEN window must not latch — the merge is undecided"
  );
  // The consumed source's stores are retained in reality, so the gate reports the id occupied —
  // and occupancy is a HOLD, the conservative direction. (With no stores at all the fork yields
  // and installs a child the open park is absorbing: a pre-existing residual, reachable on main
  // and not closed here — the absorb-pending leg deliberately does not fire on an undecided park.)
  let gate = TestGate::occupying(200);
  assert!(
    m.peek_yieldable_fork(&gate).is_none(),
    "the child id is spoken for by an undecided park, so the fork holds"
  );
  assert!(
    m.group(&7).unwrap().peek_pending_fork().is_some(),
    "STAGED, not abandoned: an abort would resurrect this child"
  );
  assert_eq!(m.poll_split_refusal(), None, "and nothing was refused");

  // Now the abort lands at k + 1 — the race the abort-window design is built on.
  let (mut m2, mut stores2) =
    fork_and_park_on_same_child(200, Some(crate::EntryKind::RollbackMerge));
  let _ = m2.service_merge_applies(Instant::ORIGIN, &mut stores2);
  assert!(
    !m2
      .group(&7)
      .unwrap()
      .pending_merge()
      .is_some_and(|p| p.window_closed()),
    "an ABORT window never latches closed"
  );
  assert!(
    m2.group(&7).unwrap().peek_pending_fork().is_some(),
    "the fork survives the abort — the child is alive again and still materializable"
  );
}

/// THE STALE-LATCH LIFECYCLE, and the reason the latch lives on the PARK. A target resolves one
/// absorb and immediately parks on the next: the second park must start UNDECIDED. A latch stored
/// beside the endpoint's merge state instead would carry the first park's CLOSED verdict onto the
/// second and refuse a source no committed coordinate has decided — a source that is still
/// abortable, and whose id an embedder may legitimately be admitting right now.
#[test]
fn a_new_park_does_not_inherit_the_previous_parks_latch() {
  let now = Instant::ORIGIN;
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let mut stores = MapStores(
    std::collections::BTreeMap::new(),
    // Park one's source is merged away here: absent WITH the terminal floor, which resolves that
    // park by abort on the very pass that latches its window.
    [90u64].into_iter().collect(),
  );
  let mut log = VecLog::default();
  let mut entries = Vec::new();
  for (idx, source) in [(1u64, Some(90u64)), (2, None), (3, Some(99))] {
    entries.push(match source {
      None => crate::Entry::new(
        Term::new(1),
        Index::new(idx),
        crate::EntryKind::Empty,
        Bytes::new(),
      ),
      Some(src) => {
        let mut sb = Vec::new();
        Data::encode(&src, &mut sb);
        let payload =
          crate::CommitMergePayload::new(Bytes::from(sb), Index::new(2), Term::new(1), 1, 1);
        let mut buf = Vec::new();
        crate::wire::encode_commit_merge_payload(&payload, &mut buf);
        crate::Entry::new(
          Term::new(1),
          Index::new(idx),
          crate::EntryKind::CommitMerge,
          Bytes::from(buf),
        )
      }
    });
  }
  log.force_append(&entries);
  let mut stable = AsyncStable::default();
  // Committed through index 3: park one's `k + 1` (index 2) is decided, park two's (index 4) is not.
  stable.force_state(Term::new(1), Some(1u64), Index::new(3));
  m.restore_group_unchecked(
    1,
    single_node_cfg(1),
    now,
    7,
    SplitSm::default(),
    1,
    &mut log,
    &mut stable,
  )
  .unwrap();
  stores.0.insert(1, (log, stable));
  let first = m.group(&1).unwrap().pending_merge().expect("park one");
  assert_eq!(first.at(), Index::new(1));
  assert!(!first.window_closed(), "minted undecided");

  // The pass latches park one (its window is CLOSED) and resolves it by abort; the drain then
  // reaches index 3 and parks again, this time with nothing committed at `k + 1`.
  for _ in 0..4 {
    let _ = m.service_merge_applies(now, &mut stores);
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
    if m
      .group(&1)
      .unwrap()
      .pending_merge()
      .is_some_and(|p| p.at() == Index::new(3))
    {
      break;
    }
  }
  let second = m.group(&1).unwrap().pending_merge().expect("park two");
  assert_eq!(second.at(), Index::new(3), "a NEW park, on the next commit");
  assert!(
    !second.window_closed(),
    "and it starts undecided — nothing about park one carried over"
  );

  // The door agrees: park two's source is unhosted and undecided, so admitting it is allowed.
  // With an endpoint-scoped latch this is where the stale CLOSED verdict would refuse it.
  m.create_group(99, 0, single_node_cfg(1), now, 9, SplitSm::default())
    .expect("an undecided park must not refuse its own source's admission");
}

/// ARM ORDER IS AN INVARIANT: a hosted child carrying THIS fork's exact
/// provenance token still resolves REDUNDANT, because the hosted-child branch runs before the gate
/// and before any refusal is computed. The absorb evidence stands throughout.
#[test]
fn a_matching_token_still_resolves_redundant_while_the_absorb_leg_stands() {
  let (mut m, mut stores) = fork_and_park_on_same_child(200, Some(crate::EntryKind::Empty));
  // The child materialized here BEFORE the window closed — a sibling replica's transferred
  // baseline, carrying this very split's token. (Admission afterwards is refused `AbsorbPending`
  // by the absorb-pending leg, which is the point: the arrival has to predate it, exactly as it does in the
  // shape this pins.)
  let token = staged_fork_id(&m, 7);
  m.create_group(
    200,
    0,
    single_node_cfg(1),
    Instant::ORIGIN,
    43,
    SplitSm::default(),
  )
  .unwrap();
  m.group_mut(&200).unwrap().seed_fork_id_for_test(token);

  assert!(
    m.service_merge_applies(Instant::ORIGIN, &mut stores)
      .is_empty()
  );
  assert!(
    m.group(&7)
      .unwrap()
      .pending_merge()
      .unwrap()
      .window_closed(),
    "the absorb leg is live for this id"
  );

  assert!(
    m.peek_yieldable_fork(&NoHold).is_none(),
    "a redundant fork yields nothing"
  );
  assert!(
    m.group(&7).unwrap().peek_pending_fork().is_none(),
    "the REDUNDANT arm ran — the fork resolved against its own materialization"
  );
  assert_eq!(
    m.poll_split_refusal(),
    None,
    "redundant is not a refusal: the child exists, it was not abandoned"
  );
}

/// BOOT ORDER. The latch is written by `service_merge_applies` alone, and restore replay does not
/// run it — so a target restored from a durable log whose replayed applies re-park comes back
/// UNDECIDED, whatever the log says about `k + 1`. Ordinary restart recovery is unaffected by the
/// new leg, and the first service pass after boot is what decides the window.
#[test]
fn a_restored_park_comes_back_undecided() {
  let (mut m, mut stores) = fork_and_park_on_same_child(200, Some(crate::EntryKind::Empty));
  assert!(
    !m.group(&7)
      .unwrap()
      .pending_merge()
      .unwrap()
      .window_closed(),
    "restore replay re-parked WITHOUT latching, though k + 1 is committed in the log"
  );
  // ...and the fork is untouched until a service pass has actually read the window.
  assert!(m.group(&7).unwrap().peek_pending_fork().is_some());
  assert!(
    m.service_merge_applies(Instant::ORIGIN, &mut stores)
      .is_empty()
  );
  assert!(
    m.group(&7)
      .unwrap()
      .pending_merge()
      .unwrap()
      .window_closed(),
    "the first service pass is what decides it"
  );
}

/// The ForkId the container mints for `parent`'s currently-staged head fork — the exact
/// provenance token a genuine twin (a sibling replica's transferred baseline) carries. Peeks the
/// staged `PendingFork` and mints through the same `mint_fork_id` the relay uses, so a test can
/// materialize (or seed) a child that resolves the parked fork as redundant.
fn staged_fork_id(m: &MultiRaft<u64, u64, SplitSm>, parent: u64) -> ForkId {
  let f = m
    .group(&parent)
    .unwrap()
    .peek_pending_fork()
    .expect("a fork is staged on the parent");
  mint_fork_id(
    &parent,
    f.parent_gen_after,
    f.index,
    f.split_term,
    f.child_bytes.clone(),
    f.child_gen,
  )
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
  follower_load_and_split_on(m, 7, log, stable, child)
}

/// [`follower_load_and_split`] on a named parent — the two-parent fixtures' entry point.
fn follower_load_and_split_on(
  m: &mut MultiRaft<u64, u64, SplitSm>,
  parent: u64,
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
    &parent,
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
    m.handle_storage(&parent, Instant::ORIGIN, log, stable),
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
    m.peek_yieldable_fork(&NoHold).is_none(),
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
  assert!(m.peek_yieldable_fork(&NoHold).is_none(), "still parked");
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
  {
    let fork = m
      .peek_yieldable_fork(&NoHold)
      .expect("removal unparks the fork for materialization");
    assert_eq!(((*fork.parent()), (*fork.child())), (7, 200));
    assert_eq!(fork.split_index(), idx);
    assert_eq!(fork.parent_gen_after(), 1);
  }
  assert!(
    m.split_reserved(&200),
    "un-parked but still STAGED: this fork is the id's one admitted writer until it installs"
  );
  let mut engine: GroupEngine<u64, u64> = GroupEngine::new();
  assert!(matches!(
    m.install_yieldable_fork(&7, &200, &mut engine, &NoHold, Instant::ORIGIN, 43),
    InstallOutcome::Installed { .. }
  ));
  assert_eq!(
    m.group(&200).unwrap().state_machine().units,
    2,
    "the parked half materializes intact"
  );
  let (_, fstable) = engine.stores(&200).expect("the child's stores");
  assert_eq!(fstable.snapshot().expect("the baseline").1, fork_blob(2));
  // Conservation across the resolution: every unit is in exactly one of parent / child — the
  // pre-split 3 plus the one the fence probe committed while parked.
  assert_eq!(
    m.group(&7).unwrap().state_machine().units + m.group(&200).unwrap().state_machine().units,
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
fn parked_fork_stays_parked_when_an_independent_twin_catches_up() {
  // An INDEPENDENTLY-created child at the fork's id crosses the fork baseline and
  // the fork's lineage by its OWN commits — but it never installed this fork's baseline, so it
  // carries no matching ForkId. Progress is NOT provenance: the parked fork must NOT resolve
  // redundant against it (that discard loses the child partition), it stays PARKED, and the staged
  // blob — the partition's only local copy — survives untouched.
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
  // An independent group at the child id — never fork-born, so no ForkId.
  m.create_group(
    200,
    0,
    single_node_cfg(1),
    Instant::ORIGIN,
    43,
    SplitSm::default(),
  )
  .unwrap();
  let _idx = follower_load_and_split(&mut m, &mut log, &mut stable, 200);
  assert!(
    m.peek_yieldable_fork(&NoHold).is_none(),
    "parked on the conflict"
  );
  assert_eq!(m.poll_split_conflict(), Some((7, 200)));

  // The independent child advances well past the fork baseline and past the fork's lineage.
  let d = lead_single_split(&mut m, 200, &mut log200, &mut stable200);
  commit_one_split(&mut m, 200, d, &mut log200, &mut stable200);
  assert!(m.group(&200).unwrap().applied_index() >= FORK_BASE_INDEX);
  assert!(m.group(&200).unwrap().fork_id().is_none(), "no provenance");

  // The next drain leaves the fork PARKED: no ForkId match, so progress alone cannot authorize
  // the discard. The blob is retained and the fence stands.
  assert!(
    m.peek_yieldable_fork(&NoHold).is_none(),
    "an independent child's progress must not yield or discard the fork"
  );
  let staged = m
    .group(&7)
    .unwrap()
    .peek_pending_fork()
    .expect("the fork is still staged — never discarded against an unrelated child");
  assert_eq!(
    staged.blob,
    fork_blob(2),
    "the partition's only copy survives"
  );
  assert_eq!(
    m.poll_split_conflict(),
    None,
    "the episode's one signal was already consumed; the park did not re-arm"
  );
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
  assert!(
    m.peek_yieldable_fork(&NoHold).is_none(),
    "parked on the conflict"
  );
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
  assert!(m.peek_yieldable_fork(&NoHold).is_none(), "still parked");
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
    .peek_yieldable_fork(&NoHold)
    .expect("removal unparks the fork for materialization");
  assert_eq!(((*fork.parent()), (*fork.child())), (7, 200));
  assert_eq!(
    m.peek_split_conflict(),
    None,
    "the resolved episode purged its undelivered signal"
  );
  assert_eq!(m.poll_split_conflict(), None);
}

#[test]
fn twin_catch_up_purges_an_undelivered_conflict() {
  // Arm (b) with the signal still queued: the parked child GAINS this fork's ForkId (a sibling
  // replica's baseline installed here — modeled by the seam), the parked fork resolves as
  // redundant, and the stale cue must not surface after the episode silently healed.
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  let mut m = park_with_queued_conflict(&mut log, &mut stable);
  assert_eq!(
    m.peek_split_conflict(),
    Some((7, 200)),
    "queued, undelivered"
  );

  // The genuine token the parent's staged fork mints — what a transferred twin baseline carries.
  let f = staged_fork_id(&m, 7);
  m.group_mut(&200).unwrap().seed_fork_id_for_test(f);
  assert!(
    m.peek_yieldable_fork(&NoHold).is_none(),
    "a twin carrying the fork's token resolves the park without yielding"
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
fn a_destructive_install_refuses_foreign_fork_provenance() {
  // The park shape, then the squatter becomes the fork's genuine twin (a sibling's baseline —
  // the seam `twin_catch_up_purges_an_undelivered_conflict` models): the child now BEARS the
  // fork's token, and every snapshot of its lineage carries that token (own captures stamp it).
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  let mut m = park_with_queued_conflict(&mut log, &mut stable);
  let f = staged_fork_id(&m, 7);
  m.group_mut(&200).unwrap().seed_fork_id_for_test(f.clone());

  let (mut log200, mut stable200) = (VecLog::default(), AsyncStable::default());

  // Drive the twin into the ASYNC-FOLLOWER shape the REDUNDANT short-circuit serves: a durable
  // log (indices 1..=3 at term 3) that outran `commit` (held at 1). Now a snapshot at boundary 1
  // is redundant-by-committed and one at boundary 3 is redundant-by-Log-Matching — both classify
  // BEFORE the fork-provenance gate, the exact ordering a foreign leader can ride to be acked out
  // of Snapshot state on state it does not own.
  let cmd = {
    let mut buf = Vec::new();
    Bytes::from_static(b"c").encode(&mut buf);
    Bytes::from(buf)
  };
  m.handle_message(
    &200,
    Instant::ORIGIN,
    &mut log200,
    &mut stable200,
    2u64,
    Message::AppendEntries(crate::AppendEntries::new(
      Term::new(3),
      2u64,
      Index::ZERO,
      Term::ZERO,
      std::vec![
        crate::Entry::new(
          Term::new(3),
          Index::new(1),
          crate::EntryKind::Normal,
          cmd.clone()
        ),
        crate::Entry::new(
          Term::new(3),
          Index::new(2),
          crate::EntryKind::Normal,
          cmd.clone()
        ),
        crate::Entry::new(Term::new(3), Index::new(3), crate::EntryKind::Normal, cmd),
      ],
      Index::new(1),
    )),
  )
  .unwrap();
  while matches!(
    m.handle_storage(&200, Instant::ORIGIN, &mut log200, &mut stable200),
    Some(StorageProgress::MorePending)
  ) {}
  assert_eq!(
    m.group(&200).unwrap().commit_index(),
    Index::new(1),
    "async-follower setup: durable log outran commit, held at 1"
  );
  let applied_after_setup = m.group(&200).unwrap().applied_index();

  // Drains group-200 outbound and reports whether a SnapshotResponse reached the foreign leader —
  // the ack that would lift it out of Snapshot. It is the LAST effect of the redundant branch,
  // right after the staging discard, so its absence witnesses that the branch never ran.
  let foreign_acked = |m: &mut MultiRaft<u64, u64, SplitSm>, leader: u64| -> bool {
    let mut acked = false;
    while let Some((g, out)) = m.poll_message() {
      let (to, msg) = out.into_parts();
      if g == 200 && to == leader && matches!(msg, Message::SnapshotResponse(_)) {
        acked = true;
      }
    }
    acked
  };
  // Clear the setup's AppendResponse (destined for node 2) before probing for a foreign ack.
  let _ = foreign_acked(&mut m, 9);

  let alien_probe = ForkId::new(
    Bytes::from_static(&[9u8]),
    1,
    Index::new(4),
    Term::new(1),
    Bytes::from_static(&[200u8]),
    7,
  );
  // Token-less AND different-token, each at the committed boundary (1) AND the Log-Matching
  // boundary (3): every arm reaches the redundant classifier first, so the gate must precede it
  // or the foreign leader is acked and this replica's staging discarded before provenance is ever
  // consulted.
  for (probe, boundary, shape) in [
    (None, Index::new(1), "token-less at a committed boundary"),
    (None, Index::new(3), "token-less at a Log-Matching boundary"),
    (
      Some(alien_probe.clone()),
      Index::new(1),
      "alien-token at a committed boundary",
    ),
    (
      Some(alien_probe.clone()),
      Index::new(3),
      "alien-token at a Log-Matching boundary",
    ),
  ] {
    let mut meta = crate::SnapshotMeta::new(
      boundary,
      Term::new(3),
      crate::ConfState::from_voters(std::vec![1u64]),
    );
    if let Some(tok) = probe {
      meta = meta.with_fork_id(tok);
    }
    m.handle_message(
      &200,
      Instant::ORIGIN,
      &mut log200,
      &mut stable200,
      9u64,
      Message::InstallSnapshot(crate::InstallSnapshot::new(
        Term::new(3),
        9u64,
        meta,
        fork_blob(77),
      )),
    )
    .unwrap();
    while matches!(
      m.handle_storage(&200, Instant::ORIGIN, &mut log200, &mut stable200),
      Some(StorageProgress::MorePending)
    ) {}
    assert!(
      !foreign_acked(&mut m, 9),
      "{shape}: a redundant foreign install must not ack the foreign leader"
    );
    let child = m.group(&200).unwrap();
    assert_eq!(
      child.applied_index(),
      applied_after_setup,
      "{shape}: nothing installed"
    );
    assert_eq!(
      child.commit_index(),
      Index::new(1),
      "{shape}: commit unmoved"
    );
    assert!(!child.is_poisoned(), "{shape}: refusal, not fail-stop");
    assert_eq!(
      child.fork_id(),
      Some(f.clone()),
      "{shape}: provenance intact"
    );
  }

  let applied_before = m.group(&200).unwrap().applied_index();

  // An authenticated foreign leader ships a TOKEN-LESS destructive install. Landing it would
  // replace the twin's state wholesale while the keep-if-set adoption retains the token — the
  // replica would impersonate the fork on foreign state, and the parked parent could resolve
  // its fork redundant against it. It must be REFUSED (never fail-stop), leaving state, token,
  // and the durable store untouched.
  let foreign = crate::SnapshotMeta::new(
    Index::new(5),
    Term::new(3),
    crate::ConfState::from_voters(std::vec![1u64, 9]),
  );
  m.handle_message(
    &200,
    Instant::ORIGIN,
    &mut log200,
    &mut stable200,
    9u64,
    Message::InstallSnapshot(crate::InstallSnapshot::new(
      Term::new(3),
      9u64,
      foreign,
      fork_blob(99),
    )),
  )
  .unwrap();
  while matches!(
    m.handle_storage(&200, Instant::ORIGIN, &mut log200, &mut stable200),
    Some(StorageProgress::MorePending)
  ) {}
  {
    let child = m.group(&200).unwrap();
    assert_eq!(
      child.applied_index(),
      applied_before,
      "a token-less install over an established provenance must not land"
    );
    assert!(!child.is_poisoned(), "refusal, not fail-stop");
    assert_eq!(child.fork_id(), Some(f.clone()), "provenance intact");
  }

  // A DIFFERENT fork's token refuses identically: adoption must never overwrite provenance.
  let alien = ForkId::new(
    Bytes::from_static(&[9u8]),
    1,
    Index::new(4),
    Term::new(1),
    Bytes::from_static(&[200u8]),
    7,
  );
  let foreign2 = crate::SnapshotMeta::new(
    Index::new(5),
    Term::new(3),
    crate::ConfState::from_voters(std::vec![1u64, 9]),
  )
  .with_fork_id(alien);
  m.handle_message(
    &200,
    Instant::ORIGIN,
    &mut log200,
    &mut stable200,
    9u64,
    Message::InstallSnapshot(crate::InstallSnapshot::new(
      Term::new(3),
      9u64,
      foreign2,
      fork_blob(98),
    )),
  )
  .unwrap();
  while matches!(
    m.handle_storage(&200, Instant::ORIGIN, &mut log200, &mut stable200),
    Some(StorageProgress::MorePending)
  ) {}
  {
    let child = m.group(&200).unwrap();
    assert_eq!(child.applied_index(), applied_before, "still refused");
    assert!(!child.is_poisoned(), "refusal, not fail-stop");
    assert_eq!(
      child.fork_id(),
      Some(f.clone()),
      "provenance not overwritten"
    );
  }

  // The SAME token still installs — the twin's genuine retransfer at a higher boundary (the
  // e2e fork-transfer pins: `a_zero_progress_joiner_is_forced_onto_the_snapshot_path` and
  // `the_joiner_lands_on_the_preloaded_state_plus_tail` cover the None→Some adoption leg).
  let twin = crate::SnapshotMeta::new(
    Index::new(6),
    Term::new(3),
    crate::ConfState::from_voters(std::vec![1u64]),
  )
  .with_fork_id(f.clone());
  m.handle_message(
    &200,
    Instant::ORIGIN,
    &mut log200,
    &mut stable200,
    2u64,
    Message::InstallSnapshot(crate::InstallSnapshot::new(
      Term::new(3),
      2u64,
      twin,
      fork_blob(2),
    )),
  )
  .unwrap();
  while matches!(
    m.handle_storage(&200, Instant::ORIGIN, &mut log200, &mut stable200),
    Some(StorageProgress::MorePending)
  ) {}
  {
    let child = m.group(&200).unwrap();
    assert_eq!(
      child.applied_index(),
      Index::new(6),
      "the matching-token retransfer lands"
    );
    assert_eq!(child.state_machine().units, 2, "the twin's state arrived");
    assert_eq!(child.fork_id(), Some(f), "provenance carried through");
  }

  // The park resolves redundant against the COHERENT twin — never against foreign state.
  assert!(
    m.peek_yieldable_fork(&NoHold).is_none(),
    "the twin resolves the park without yielding"
  );
  assert!(
    m.group(&7).unwrap().peek_pending_fork().is_none(),
    "the redundant fork is consumed"
  );
}

#[test]
fn a_same_identity_remint_is_refused_not_superseded() {
  // Cross-mint installs NEVER supersede an established provenance — not even a plausible
  // "successor" of the same fork identity. A genuine retry re-mints under a strictly higher
  // parent incarnation (every committed split bumps the parent's lineage counter), and the
  // coordinator's admission floor tears the stale incarnation down before the re-mint exists;
  // a stale-token replica meeting a re-minted lineage is a lifecycle breach the embedder
  // resolves by placement. Admitting any not-exact token would let an authenticated but
  // mis-lineaged leader wholesale-replace a token-bearing replica — the child-partition loss
  // the receipt gate exists to prevent.
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  let mut m = park_with_queued_conflict(&mut log, &mut stable);
  let stale = staged_fork_id(&m, 7);
  m.group_mut(&200)
    .unwrap()
    .seed_fork_id_for_test(stale.clone());
  let (mut log200, mut stable200) = (VecLog::default(), AsyncStable::default());
  let applied_before = m.group(&200).unwrap().applied_index();

  let install = |m: &mut MultiRaft<u64, u64, SplitSm>,
                 log200: &mut VecLog,
                 stable200: &mut AsyncStable,
                 fork_id: ForkId| {
    let meta = crate::SnapshotMeta::new(
      Index::new(5),
      Term::new(3),
      crate::ConfState::from_voters(std::vec![1u64]),
    )
    .with_fork_id(fork_id);
    m.handle_message(
      &200,
      Instant::ORIGIN,
      log200,
      stable200,
      2u64,
      Message::InstallSnapshot(crate::InstallSnapshot::new(
        Term::new(3),
        2u64,
        meta,
        fork_blob(9),
      )),
    )
    .unwrap();
    while matches!(
      m.handle_storage(&200, Instant::ORIGIN, log200, stable200),
      Some(StorageProgress::MorePending)
    ) {}
  };

  // A delayed stale frame; a rival same-generation mint; a forged same-incarnation successor
  // (structurally impossible for a real retry — the parent's counter moves); and the shape a
  // real retry WOULD carry (incarnation and generation both advanced). One rule covers all
  // four: not this token, not this lineage.
  let older = ForkId::new(
    stale.parent().clone(),
    stale.parent_incarnation(),
    Index::new(1),
    Term::new(1),
    stale.child().clone(),
    stale.child_gen().saturating_sub(1),
  );
  let rival = ForkId::new(
    stale.parent().clone(),
    stale.parent_incarnation(),
    Index::new(9),
    Term::new(2),
    stale.child().clone(),
    stale.child_gen(),
  );
  let forged = ForkId::new(
    stale.parent().clone(),
    stale.parent_incarnation(),
    Index::new(9),
    Term::new(2),
    stale.child().clone(),
    stale.child_gen() + 1,
  );
  let remint = ForkId::new(
    stale.parent().clone(),
    stale.parent_incarnation() + 1,
    Index::new(9),
    Term::new(2),
    stale.child().clone(),
    stale.child_gen() + 1,
  );
  for (mint, shape) in [
    (older, "a lower-generation mint"),
    (rival, "an equal-generation rival mint"),
    (forged, "a same-incarnation higher-generation mint"),
    (remint, "a genuine re-mint's higher-incarnation shape"),
  ] {
    install(&mut m, &mut log200, &mut stable200, mint);
    let child = m.group(&200).unwrap();
    assert_eq!(
      child.applied_index(),
      applied_before,
      "{shape} must not land"
    );
    assert!(!child.is_poisoned(), "refusal, not fail-stop");
    assert_eq!(child.fork_id(), Some(stale.clone()), "provenance intact");
  }
}

#[test]
fn an_absorb_capture_preserves_fork_provenance() {
  // THE SHED (multi-VOPR merge band, seed 1): a fork-child TARGET absorbing a merge source
  // anchors the union with the FORCED capture — and that capture's durable meta is what a
  // restart re-derives `fork_id` from and what every later snapshot send advertises. It must
  // re-stamp the child's token exactly as the ordinary capture does; a token-less union anchor
  // sheds provenance, and a stale fork sibling then refuses the lineage's every snapshot as
  // foreign — pinned behind its healed quorum forever.
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  let mut m = park_with_queued_conflict(&mut log, &mut stable);
  let f = staged_fork_id(&m, 7);
  m.group_mut(&200).unwrap().seed_fork_id_for_test(f.clone());
  let (mut log200, mut stable200) = (VecLog::default(), AsyncStable::default());

  // Real applied state below the capture boundary: the twin's retransfer at index 6.
  let twin = crate::SnapshotMeta::new(
    Index::new(6),
    Term::new(3),
    crate::ConfState::from_voters(std::vec![1u64]),
  )
  .with_fork_id(f.clone());
  m.handle_message(
    &200,
    Instant::ORIGIN,
    &mut log200,
    &mut stable200,
    2u64,
    Message::InstallSnapshot(crate::InstallSnapshot::new(
      Term::new(3),
      2u64,
      twin,
      fork_blob(2),
    )),
  )
  .unwrap();
  while matches!(
    m.handle_storage(&200, Instant::ORIGIN, &mut log200, &mut stable200),
    Some(StorageProgress::MorePending)
  ) {}
  assert_eq!(m.group(&200).unwrap().applied_index(), Index::new(6));

  // The container's resolve arm runs the forced capture with the target's stores in hand.
  assert!(
    m.group_mut(&200)
      .unwrap()
      .capture_absorb_snapshot(&log200, &mut stable200),
    "the union anchor stages"
  );
  while matches!(
    m.handle_storage(&200, Instant::ORIGIN, &mut log200, &mut stable200),
    Some(StorageProgress::MorePending)
  ) {}
  let (meta, _) = stable200.snapshot().expect("the union anchor is durable");
  assert_eq!(
    meta.fork_id(),
    Some(&f),
    "the absorb capture re-stamps fork provenance"
  );

  // The contract the stamp protects: a rebuilt container re-derives the token from exactly
  // this durable meta.
  let mut m2: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  m2.restore_group_unchecked(
    200,
    single_node_cfg(1),
    Instant::ORIGIN,
    43,
    SplitSm::default(),
    1,
    &mut log200,
    &mut stable200,
  )
  .unwrap();
  assert_eq!(
    m2.group(&200).unwrap().fork_id(),
    Some(f),
    "restart re-derives provenance from the absorb anchor"
  );
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
  // Arm (b) at FIRST examination: the twin was already hosted from THIS fork's manufactured
  // baseline (it carries the matching ForkId — a sibling materialized it before this replica's own
  // fork drained), so the relay resolves it as redundant outright — no park episode, no conflict
  // signal — and the twin's state carries the partition.
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

  // Stage the split so the fork's token is knowable, then materialize the twin FROM that token —
  // fork-born at the manufactured baseline (applied == FORK_BASE_INDEX) holding exactly the half
  // the split gives away, and carrying this fork's exact provenance.
  let idx = follower_load_and_split(&mut m, &mut log, &mut stable, 200);
  let f = staged_fork_id(&m, 7);
  // The RELAY's own path, not the caller-driven door: a sibling materialized this fork and its
  // baseline arrived here, which is how a token-bearing twin reaches an id a staged fork reserves.
  // The public door refuses exactly that id, by design.
  m.create_group_from_fork_unreserved(
    200,
    0,
    single_node_cfg(1),
    Instant::ORIGIN,
    43,
    SplitSm::default(),
    fork_blob(2),
    None,
    Some(f),
    1,
    &mut log200,
    &mut stable200,
  )
  .unwrap();

  assert!(
    m.peek_yieldable_fork(&NoHold).is_none(),
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
  assert!(
    m.peek_yieldable_fork(&NoHold).is_none(),
    "parked, not dropped"
  );
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
  {
    let fork = m
      .peek_yieldable_fork(&NoHold)
      .expect("the fork survives the race");
    assert_eq!(
      ((*fork.parent()), (*fork.child()), fork.split_index()),
      (7, 200, idx)
    );
  }
  let mut engine: GroupEngine<u64, u64> = GroupEngine::new();
  assert!(matches!(
    m.install_yieldable_fork(&7, &200, &mut engine, &NoHold, Instant::ORIGIN, 43),
    InstallOutcome::Installed { .. }
  ));
  assert_eq!(m.group(&200).unwrap().state_machine().units, 2);
  assert_eq!(
    m.group(&7).unwrap().state_machine().units + m.group(&200).unwrap().state_machine().units,
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
  m.restore_group_unchecked(
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
    .peek_yieldable_fork(&NoHold)
    .expect("a replayed, never-relayed fork relays again");
  assert_eq!(((*fork.child()), fork.parent_gen_after()), (200, 1));

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
  m.restore_group_unchecked(
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
    m.peek_yieldable_fork(&NoHold).is_none(),
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
  // Arm (b), the reshape-cycle shape: the parent's crash-replay re-stages a committed fork whose
  // child is hosted AND has reshaped since birth (here: the child froze for a merge, bumping its
  // lineage). A lineage-EQUALITY resolution parked forever once the child reshaped, and the park's
  // standing fence closed a true dependency cycle — arm (a) needed the child gone, the child leaves
  // only when the merge into the parent resolves, the resolve arm needs the parent's absorb
  // capture, and the capture is exactly what the fence blocks (fence → child-gone → merge → capture
  // → fence). The ForkId breaks it structurally: the token is fixed at the split, so reshaping
  // NEVER changes it — the twin still carries this fork's exact token, the blob is redundant, and
  // the fence lifts.
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
  m.create_group(2, 0, single_node_cfg(1), now, 43, SplitSm::default())
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
      split_entry_bytes(2, 0, 1, 2),
    ),
  ]);
  pstable.force_state(Term::new(1), Some(1u64), Index::new(4));
  m.restore_group_unchecked(
    1,
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
    m.group(&1).unwrap().state_machine().units,
    1,
    "the replayed split gave the half up again"
  );

  // The child reshapes AFTER birth: it leads, applies load past the manufactured baseline, and
  // freezes for the merge into the parent — the freeze apply bumps its lineage to 1 > child_gen.
  let d = lead_single_split(&mut m, 2, &mut clog, &mut cstable);
  commit_one_split(&mut m, 2, d, &mut clog, &mut cstable);
  let mut stores = MapStores(std::collections::BTreeMap::new(), Default::default());
  stores.0.insert(2, (clog, cstable));
  stores.0.insert(1, (plog, pstable));
  m.prepare_merge(&2, d, &mut stores, &1).unwrap().unwrap();
  {
    let (clog, cstable) = stores.0.get_mut(&2).unwrap();
    drain(&mut m, 2, clog, cstable);
  }
  assert!(m.group(&2).unwrap().is_frozen());
  assert_eq!(m.group(&2).unwrap().shape_gen(), 1);
  assert!(m.group(&2).unwrap().applied_index() >= FORK_BASE_INDEX);

  // The child carries THIS fork's provenance (its baseline arrived from a sibling — the seam),
  // fixed at the split and unchanged by the reshape above.
  let f = staged_fork_id(&m, 1);
  m.group_mut(&2).unwrap().seed_fork_id_for_test(f);

  // The reshaped twin resolves the fork as redundant at FIRST examination: no park, no
  // conflict signal, the staged blob discarded, the fence gone.
  assert!(
    m.peek_yieldable_fork(&NoHold).is_none(),
    "a reshaped twin resolves the fork without yielding"
  );
  assert!(
    m.group(&1).unwrap().peek_pending_fork().is_none(),
    "the redundant fork is consumed — a reshaping twin's token never changed"
  );
  assert_eq!(m.peek_split_conflict(), None, "nothing parked, no cue");
  assert_eq!(m.group_gen(&1), 1, "the relay guard advanced");

  // The dependent merge now completes: park, seal, resolve — the absorb capture the fence was
  // blocking lands, the child departs, and the parent serves the union.
  let dp = {
    let (plog, pstable) = stores.0.get_mut(&1).unwrap();
    lead_single_split(&mut m, 1, plog, pstable)
  };
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.commit_merge(&1, dp, log, stable, &2).unwrap().unwrap();
    while matches!(
      m.handle_storage(&1, dp, log, stable),
      Some(StorageProgress::MorePending)
    ) {}
  }
  assert!(m.group(&1).unwrap().pending_merge().is_some(), "parked");
  assert!(
    m.service_merge_applies(dp, &mut stores).is_empty(),
    "the first pass only seals the window"
  );
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    while matches!(
      m.handle_storage(&1, dp, log, stable),
      Some(StorageProgress::MorePending)
    ) {}
  }
  let resolutions = m.service_merge_applies(dp, &mut stores);
  assert_eq!(
    resolutions,
    std::vec![MergeResolution::Merged {
      source: 2,
      target: 1
    }],
    "the cycle is broken: the absorb capture is no longer fence-blocked"
  );
  assert!(!m.contains_group(&2), "the child departed via the merge");
  assert_eq!(
    m.group(&1).unwrap().state_machine().units,
    2,
    "the parent serves the union (its own half plus the absorbed twin)"
  );
}

#[test]
fn recreated_squatter_at_higher_lineage_stays_parked() {
  // The recreated-squatter shape, now PARKED: the id under the staged fork is hosted by an
  // incarnation that reshaped past the fork's lineage with its OWN split. It was created
  // independently, so it never installed this fork's baseline and carries no matching ForkId — a
  // higher lineage is not provenance. The parent's fork stays PARKED (its blob retained, its fence
  // standing); only the squatter's OWN staged fork, a separate parent, still yields.
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
  // lineage bump that would have broken equality.
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
  assert!(m.group(&200).unwrap().fork_id().is_none(), "no provenance");

  // Apply the parent's split against the now-hosted, now-reshaped squatter.
  m.flush_appends(&7, d, &log, &stable).unwrap();
  while matches!(
    m.handle_storage(&7, d, &mut log, &mut stable),
    Some(StorageProgress::MorePending)
  ) {}

  // The drain PARKS the parent's fork (no ForkId match) and flows on to the squatter's own
  // staged fork — a parked fork wedges only its OWN parent's later forks, never another group's.
  {
    let fork = m
      .peek_yieldable_fork(&NoHold)
      .expect("the squatter's own fork still yields");
    assert_eq!(((*fork.parent()), (*fork.child())), (200, 300));
  }
  install_head_fork(&mut m, 200, 300, d);
  assert!(m.peek_yieldable_fork(&NoHold).is_none());
  assert!(
    m.group(&7).unwrap().peek_pending_fork().is_some(),
    "the parent's fork stays parked — a higher lineage is not this fork's provenance"
  );
  assert_eq!(
    m.poll_split_conflict(),
    Some((7, 200)),
    "the park surfaced its conflict cue"
  );

  // The fence still stands: no capture crosses the unresolved split.
  commit_one_split(&mut m, 7, d, &mut log, &mut stable);
  assert!(
    stable
      .snapshot()
      .is_none_or(|(meta, _)| meta.last_index() < idx),
    "the standing fence holds the parent below the split"
  );
}

#[test]
fn below_lineage_squatter_stays_parked() {
  // A hosted child that never installed this fork's baseline PARKS — it carries no matching
  // ForkId, so it cannot contain the handover, and discarding the blob against it would lose the
  // partition's only local copy. Here the squatter also sits below the fork's minted lineage (the
  // fork mints at child_gen 1, the squatter caught up only at lineage 0) — a shape the coordinators
  // make remote via a skewed catalog, but the floor-free container must hold the conservative park
  // regardless and let the embedder resolve it through arm (a).
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

  assert!(
    m.peek_yieldable_fork(&NoHold).is_none(),
    "parked, not resolved"
  );
  assert_eq!(m.poll_split_conflict(), Some((7, 200)));
  assert!(
    m.group(&7).unwrap().peek_pending_fork().is_some(),
    "the no-provenance hold keeps the blob staged"
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
  {
    let fork = m
      .peek_yieldable_fork(&NoHold)
      .expect("removal unparks the fork for materialization");
    assert_eq!(((*fork.parent()), (*fork.child())), (7, 200));
    assert_eq!(fork.child_gen(), 1, "the minted lineage rides the plan");
  }
  let mut engine: GroupEngine<u64, u64> = GroupEngine::new();
  assert!(matches!(
    m.install_yieldable_fork(&7, &200, &mut engine, &NoHold, Instant::ORIGIN, 43),
    InstallOutcome::Installed { child_gen: 1, .. }
  ));
  assert_eq!(
    m.group(&200).unwrap().state_machine().units,
    2,
    "the held half materializes intact"
  );
}

#[test]
fn only_matching_provenance_resolves_a_parked_fork() {
  // The unified fix in one pin: an INDEPENDENT child at the fork's id — advanced well past the
  // baseline AND the fork's lineage by its own commits — PARKS, and only when that same child
  // carries THIS fork's exact ForkId (a sibling's baseline installed here) does the fork resolve
  // redundant. Progress is never provenance; nothing an unrelated child does can discard the blob.
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  let (mut log200, mut stable200) = (VecLog::default(), AsyncStable::default());
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
  follower_load_and_split(&mut m, &mut log, &mut stable, 200);
  assert!(
    m.peek_yieldable_fork(&NoHold).is_none(),
    "parked on the independent occupant"
  );
  assert_eq!(m.poll_split_conflict(), Some((7, 200)));

  // Advance the independent child past the fork baseline — still no resolution, still no yield.
  let d = lead_single_split(&mut m, 200, &mut log200, &mut stable200);
  commit_one_split(&mut m, 200, d, &mut log200, &mut stable200);
  assert!(m.group(&200).unwrap().applied_index() >= FORK_BASE_INDEX);
  assert!(
    m.peek_yieldable_fork(&NoHold).is_none(),
    "progress alone never resolves the park"
  );
  assert!(
    m.group(&7).unwrap().peek_pending_fork().is_some(),
    "the blob is retained against the unrelated child"
  );

  // The genuine token arrives (a sibling's fork baseline installed at this replica) — NOW it
  // resolves redundant, and the guard advances.
  let f = staged_fork_id(&m, 7);
  m.group_mut(&200).unwrap().seed_fork_id_for_test(f);
  assert!(
    m.peek_yieldable_fork(&NoHold).is_none(),
    "still nothing to yield"
  );
  assert!(
    m.group(&7).unwrap().peek_pending_fork().is_none(),
    "the matching token — and only it — resolves the fork"
  );
  assert_eq!(
    m.group_gen(&7),
    1,
    "the guard advanced on the genuine resolution"
  );
}

#[test]
fn parked_fork_conserves_units_across_a_crash() {
  // Conservation across a crash: a parent splits 2 of its 3 units into a child id an INDEPENDENT
  // group already occupies AND has advanced past the fork's baseline and lineage.
  // A crash/restart replays the durable split and re-stages the fork; because the advanced occupant
  // carries no matching ForkId, the fork PARKS instead of resolving redundant, so no unit is
  // discarded across the crash — the parent keeps 1, the staged blob holds 2, all three conserved.
  // Resolving on the occupant's PROGRESS rather than its provenance discards the staged 2 units.
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let (mut log200, mut stable200) = (VecLog::default(), AsyncStable::default());
  // The independent occupant, advanced past the fork baseline (applied >= FORK_BASE_INDEX) at the
  // fork's own lineage — the exact `applied >= baseline && shape_gen >= child_gen` shape a
  // progress-based resolver mistakes for this fork's own child, discarding the blob against it.
  m.create_group(
    200,
    0,
    single_node_cfg(1),
    Instant::ORIGIN,
    43,
    SplitSm::default(),
  )
  .unwrap();
  let d200 = lead_single_split(&mut m, 200, &mut log200, &mut stable200);
  commit_one_split(&mut m, 200, d200, &mut log200, &mut stable200);
  assert!(m.group(&200).unwrap().applied_index() >= FORK_BASE_INDEX);
  assert!(m.group(&200).unwrap().fork_id().is_none(), "no provenance");

  // The parent's durable log after the crash: 3 units of load and a committed split giving 2 to
  // child 200 — recovered by the restart path exactly as crash recovery would.
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  let cmd = {
    let mut b = Vec::new();
    Bytes::from_static(b"c").encode(&mut b);
    Bytes::from(b)
  };
  log.force_append(&[
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
      split_entry_bytes(200, 0, 1, 2),
    ),
  ]);
  stable.force_state(Term::new(1), Some(1u64), Index::new(4));
  m.restore_group_unchecked(
    7,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    SplitSm::default(),
    1,
    &mut log,
    &mut stable,
  )
  .unwrap();

  // The re-staged fork PARKS against the advanced occupant — no discard.
  assert!(
    m.peek_yieldable_fork(&NoHold).is_none(),
    "the advanced independent occupant parks the re-staged fork"
  );
  assert_eq!(m.poll_split_conflict(), Some((7, 200)));
  let parent_units = m.group(&7).unwrap().state_machine().units;
  let staged = m
    .group(&7)
    .unwrap()
    .peek_pending_fork()
    .expect("the fork survives the crash — never discarded against an unrelated child");
  assert_eq!(parent_units, 1, "the parent kept its half across the crash");
  assert_eq!(
    staged.blob,
    fork_blob(2),
    "the child half is intact in the staged blob"
  );
  assert_eq!(
    parent_units + 2,
    3,
    "conservation across the crash: parent 1 + staged 2 = all three units"
  );
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

/// A host with two single-voter groups (2 = source with `src_count` applied commands, 1 = target
/// with `tgt_count`), each seeded with the given state machine, elected and fully drained. The
/// source encodes above the target so the claim points down the id order.
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
  // Group 2 is the SOURCE (encoding-larger → claims down the id order), group 1 the TARGET/survivor.
  for (gid, n, fsm) in [(2u64, src_count, src_fsm), (1u64, tgt_count, tgt_fsm)] {
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

/// A host with two single-voter [`CountSm`] groups (2 = source with `src_count` applied commands,
/// 1 = target with `tgt_count`), each elected and fully drained.
fn merge_host(src_count: usize, tgt_count: usize) -> (MultiRaft<u64, u64, CountSm>, MapStores) {
  merge_host_with(CountSm::default(), src_count, CountSm::default(), tgt_count)
}

/// Freeze group 2 into group 1 and park group 1's commit, fully drained: the state every
/// resolution arm starts from. Returns the parked index k.
fn freeze_and_park<F>(m: &mut MultiRaft<u64, u64, F>, stores: &mut MapStores) -> Index
where
  F: crate::StateMachine<Command = Bytes, Snapshot = u64>,
  F::Error: core::error::Error,
{
  let now = Instant::ORIGIN;
  {
    m.prepare_merge(&2, now, stores, &1).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(m, 2, now, log, stable);
  }
  assert!(m.group(&2).unwrap().is_frozen());
  let k = {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    let k = m.commit_merge(&1, now, log, stable, &2).unwrap().unwrap();
    drain_storage(m, 1, now, log, stable);
    k
  };
  assert!(m.group(&1).unwrap().pending_merge().is_some(), "parked");
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
  let (log, stable) = stores.0.get_mut(&1).unwrap();
  drain_storage(m, 1, Instant::ORIGIN, log, stable);
}

/// The refusals a RECOVERY PIN raises after `CaptureFailed { source, target }`: the consumed
/// source's id is named (what the demux fence and the factory gate read), refuses removal
/// (`SpokenFor` — no tombstone, no store teardown) and admission (`AbsorbPending` — no create, and
/// no live restore off the very stores the restart needs), and the poisoned holder refuses its own
/// removal (`OwesRecovery` — the teardown would shed the pin). The source's stores and floor are
/// exactly as the resolution left them, and the pin is not a debt: further cranks emit nothing,
/// mint nothing and floor nothing.
fn assert_recovery_pinned<F>(
  m: &mut MultiRaft<u64, u64, F>,
  stores: &mut MapStores,
  source: u64,
  target: u64,
  fsm: impl Fn() -> F,
) where
  F: crate::StateMachine<Command = Bytes, Snapshot = u64>,
  F::Error: core::error::Error,
{
  let now = Instant::ORIGIN;
  assert!(
    m.debt_names(&source),
    "the pin names the consumed source {source}"
  );
  assert!(
    matches!(m.remove_group(&source, stores), Err(RemoveError::SpokenFor)),
    "the pinned id {source} refuses removal: no tombstone, no store teardown"
  );
  assert!(
    matches!(
      m.create_group(source, 0, single_node_cfg(1), now, 99, fsm()),
      Err(CreateGroupError::AbsorbPending)
    ),
    "the pinned id {source} refuses admission"
  );
  {
    let (log, stable) = stores.0.get_mut(&source).unwrap();
    assert!(
      matches!(
        m.restore_group_unchecked(source, single_node_cfg(1), now, 99, fsm(), 1, log, stable),
        Err(CreateGroupError::AbsorbPending)
      ),
      "a live restore of {source} off the preserved stores refuses: beside a park-less poisoned \
       target it would be a frozen husk claiming a dead target"
    );
  }
  assert!(
    matches!(
      m.remove_group(&target, stores),
      Err(RemoveError::OwesRecovery)
    ),
    "the poisoned holder {target} refuses its own removal: the teardown would shed the pin"
  );
  assert!(
    stores.0.contains_key(&source),
    "the source's stores are intact"
  );
  assert_eq!(stores.floor(&source), 0, "the source id was never floored");
  for _ in 0..3 {
    assert!(
      m.service_merge_applies(now, stores).is_empty(),
      "a pin is not a debt: no crank discharges it"
    );
  }
  assert!(
    m.group(&target).unwrap().capture_debt().is_none(),
    "no debt was minted for the failed capture"
  );
  assert!(
    stores.0.contains_key(&source) && stores.floor(&source) == 0,
    "the cranks touched neither the stores nor the floor"
  );
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
      source: 2,
      target: 1
    }]
  );
  assert!(!m.contains_group(&2), "the source endpoint is gone");
  let tep = m.group(&1).unwrap();
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
      assert_eq!(gid, 1);
      assert_eq!(e.index(), k);
      merged = true;
    }
  }
  assert!(merged, "Event::Merged surfaced group-stamped");
  // The forced absorb capture is staged: draining the target's storage lands the blob and the
  // deferred compaction — no replica can ever be log-walked across the absorb point again.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, Instant::ORIGIN, log, stable);
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
/// target that owes it nothing (a permanent strand). So `commit_merge` is refused `SplitInFlight`
/// while the split is in flight; once the split applies the same `commit_merge` mints from the
/// post-split counter and absorbs — the source is never stranded.
#[test]
fn commit_merge_defers_a_target_reshaping_by_a_split() {
  let (mut m, mut stores) = merge_host_with(SplitSm::default(), 1, SplitSm::default(), 3);
  let now = Instant::ORIGIN;

  // Freeze source 1 into target 2: 1 is frozen_for 2, its barrier trivially met (single voter).
  {
    m.prepare_merge(&2, now, &mut stores, &1).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    m.group(&2).unwrap().is_frozen(),
    "source frozen for the target"
  );

  // Append a Split on the TARGET without draining it: split_in_flight is armed, unapplied.
  let split_idx = {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.propose_split(&1, now, log, stable, &3, 0, Bytes::from_static(b"\x01"))
      .unwrap()
      .unwrap()
  };
  assert!(
    m.group(&1).unwrap().split_in_flight(),
    "the target has a split appended-unapplied"
  );

  // THE FENCE: the absorb defers while the target is reshaping. Admitting it here lets the drained
  // split stale the CommitMerge into an obligation-less MergeAborted.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    assert_eq!(
      m.commit_merge(&1, now, log, stable, &2),
      Some(Err(MergeError::SplitInFlight)),
      "a target mid-split defers the absorb"
    );
  }
  // The source is untouched — still frozen_for the target, never stranded by a stale abort.
  assert!(m.group(&2).unwrap().is_frozen());
  assert!(
    !m.group(&1).unwrap().owes_live_thaw(),
    "no obligation-less abort was recorded on the target"
  );

  // Let the split resolve: it applies, `shape_gen` bumps, the fork relays out, the barrier lifts.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.flush_appends(&1, now, log, stable).unwrap();
    while matches!(
      m.handle_storage(&1, now, log, stable),
      Some(StorageProgress::MorePending)
    ) {}
  }
  assert!(!m.group(&1).unwrap().split_in_flight(), "the split applied");
  let fork = m.peek_yieldable_fork(&NoHold).expect("the fork relays out");
  assert_eq!((*fork.child()), 3);
  m.lift_fork_barrier(&1, split_idx);
  while m.poll_event().is_some() {}

  // Re-propose against the post-split lineage: now it admits and PARKS (not stranded).
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.commit_merge(&1, now, log, stable, &2).unwrap().unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    m.group(&1).unwrap().pending_merge().is_some(),
    "the absorb parks against the post-split target"
  );

  // Drive it to resolution: the source is absorbed and removed — the opposite of a strand.
  seal_window(&mut m, &mut stores);
  let resolutions = m.service_merge_applies(now, &mut stores);
  assert_eq!(
    resolutions,
    std::vec![MergeResolution::Merged {
      source: 2,
      target: 1
    }]
  );
  assert!(
    !m.contains_group(&2),
    "the source is absorbed, not left frozen forever"
  );
}

/// The `prepare_merge` dual of the same fence, on the SOURCE side. A source mid-split must not
/// freeze: the freeze mints `source_gen_after` from the source's live `shape_gen`, but the pending
/// split applies first and bumps it, so the freeze's generation COLLIDES with the split's on the
/// one lineage counter. Symmetric to `propose_split` refusing a freezing parent. The freeze is
/// refused `SplitInFlight` and admits once the split applies; admitting it mid-split is what
/// collides the two generations.
#[test]
fn prepare_merge_defers_a_source_reshaping_by_a_split() {
  let (mut m, mut stores) = merge_host_with(SplitSm::default(), 3, SplitSm::default(), 1);
  let now = Instant::ORIGIN;

  // Append a Split on the SOURCE (group 1) without draining it.
  let split_idx = {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.propose_split(&2, now, log, stable, &3, 0, Bytes::from_static(b"\x01"))
      .unwrap()
      .unwrap()
  };
  assert!(m.group(&2).unwrap().split_in_flight());

  // THE FENCE: the freeze defers while the source is reshaping. Admitting it collides the freeze's
  // generation with the split's on the one lineage counter.
  {
    assert_eq!(
      m.prepare_merge(&2, now, &mut stores, &1),
      Some(Err(MergeError::SplitInFlight)),
      "a source mid-split defers the freeze"
    );
  }
  assert!(
    !m.group(&2).unwrap().merge_freeze_active(),
    "the source never froze mid-split — nothing was appended"
  );

  // Resolve the split; then the same freeze admits against the post-split counter.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.flush_appends(&2, now, log, stable).unwrap();
    while matches!(
      m.handle_storage(&2, now, log, stable),
      Some(StorageProgress::MorePending)
    ) {}
  }
  assert!(!m.group(&2).unwrap().split_in_flight());
  {
    let fork = m.peek_yieldable_fork(&NoHold).expect("the fork relays out");
    assert_eq!((*fork.child()), 3);
  }
  install_head_fork(&mut m, 2, 3, now);
  // THE BARRIER HALF, between the fork's materialization and its baseline's durability: the
  // source's log is still the child's only local recovery derivation, and the absorb behind this
  // freeze would consume it. Same refusal — the source's split machinery is not finished.
  assert_eq!(
    m.prepare_merge(&2, now, &mut stores, &1),
    Some(Err(MergeError::SplitInFlight)),
    "a source whose staged fork is not yet durable defers the freeze"
  );
  m.lift_fork_barrier(&2, split_idx);
  {
    m.prepare_merge(&2, now, &mut stores, &1).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    m.group(&2).unwrap().is_frozen(),
    "the freeze admits once the split has applied"
  );
}

/// The SAME fence closing the CROSS-SOURCE fan-in case the split gate does not reach: a target-role
/// abort (`RollbackMerge`) in flight bumps the target's `shape_gen` when it applies, exactly like a
/// split. Two sources (1, 3) freeze into one target (2); 2 aborts 1's freeze as a release valve
/// (appended, unapplied) then tries to commit 3 while that abort is in flight. The commit is refused
/// `RollbackInFlight`; once 1's abort applies the same commit admits and 3 absorbs. Admitting it
/// under the in-flight abort strands 3: draining 2 applies the abort (bumping the counter and
/// recording `abandoned[1]`) and stale-aborts 3's commit WITHOUT recording `abandoned[3]`, leaving 3
/// frozen_for 2 forever while 1 thaws (`owes_live_thaw` clears 1 but never held 3).
/// (The abort of the SAME merge being committed is caught earlier by `AlreadyPending`.)
#[test]
fn commit_merge_defers_a_target_with_a_fanin_abort_in_flight() {
  let (mut m, mut stores) = merge_host_triple(2, 1, 1);
  let now = Instant::ORIGIN;
  for src in [2u64, 3] {
    m.prepare_merge(&src, now, &mut stores, &1)
      .unwrap()
      .unwrap();
    let (log, stable) = stores.0.get_mut(&src).unwrap();
    drain_storage(&mut m, src, now, log, stable);
    assert!(m.group(&src).unwrap().is_frozen(), "fan-in source frozen");
  }
  // Abort 1's freeze (release valve) — append but DO NOT drain: the abort is in flight on 2's log.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.rollback_merge(&1, now, log, stable, &2).unwrap().unwrap();
  }
  assert!(
    m.group(&1).unwrap().rollback_in_flight(),
    "1's abort is in flight"
  );

  // THE FENCE: committing a DIFFERENT frozen source into the same target defers while that abort is
  // in flight. Admitting it lets the drained abort stale 3's commit into an obligation-less
  // MergeAborted, stranding 3.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    assert_eq!(
      m.commit_merge(&1, now, log, stable, &3),
      Some(Err(MergeError::RollbackInFlight)),
      "a target with a fan-in abort in flight defers the absorb"
    );
  }

  // Let 1's abort apply: it records abandoned[1] and clears the fence. 3 is untouched (still frozen).
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    !m.group(&1).unwrap().rollback_in_flight(),
    "the abort applied"
  );
  assert!(m.group(&3).unwrap().is_frozen(), "3 was never disturbed");

  // The same commit now admits against the post-abort lineage and parks — 3 is not stranded.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.commit_merge(&1, now, log, stable, &3).unwrap().unwrap();
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
    !m.group(&2).unwrap().is_frozen(),
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
/// `target_gen_after` from the SAME live `shape_gen`, so the second abort defers `RollbackInFlight`
/// until 1's applies; re-proposed against the post-abort lineage it records its OWN `abandoned[3]`
/// and 3 thaws. Minting both off one counter strands 3: draining 2 applies 1's abort (bumping the
/// counter, recording `abandoned[1]`) and stale-no-ops 3's abort at the strict apply-time guard
/// WITHOUT recording `abandoned[3]` — 3 left frozen_for 2 forever, owed a thaw no obligation names.
/// (The abort of the SAME merge as an in-flight commit is deliberately RACED, not fenced — see
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
  for src in [2u64, 3u64] {
    m.prepare_merge(&src, now, &mut stores, &1)
      .unwrap()
      .unwrap();
    let (log, stable) = stores.0.get_mut(&src).unwrap();
    drain_storage(&mut m, src, now, log, stable);
    assert!(m.group(&src).unwrap().is_frozen(), "source {src} froze");
  }

  // Abort 1's freeze — append but DO NOT drain: the abort is in flight on 2's log, its mint live.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.rollback_merge(&1, now, log, stable, &2).unwrap().unwrap();
  }
  assert!(
    m.group(&1).unwrap().rollback_in_flight(),
    "1's abort is in flight, unapplied"
  );

  // THE FENCE: aborting a DIFFERENT frozen source while the first abort is in flight defers.
  // Admitting it mints the SAME gen as 1's abort, which stale-no-ops on apply WITHOUT recording
  // abandoned[3] — 3 stranded frozen forever.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    assert_eq!(
      m.rollback_merge(&1, now, log, stable, &3),
      Some(Err(MergeError::RollbackInFlight)),
      "a second fan-in abort defers while the first is in flight"
    );
  }
  assert!(m.group(&3).unwrap().is_frozen(), "3 is untouched");
  assert!(
    m.group(&1).unwrap().abandoned_obligations().is_empty(),
    "neither abort has applied yet — no obligation recorded (3 not stale-no-oped into a strand)"
  );

  // Let 1's abort apply: it records abandoned[1] and clears the fence.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    !m.group(&1).unwrap().rollback_in_flight(),
    "1's abort applied"
  );
  assert_eq!(
    m.group(&1).unwrap().abandoned_obligations().len(),
    1,
    "only 1's obligation recorded so far"
  );

  // The same abort now admits against the post-abort lineage and records 3's OWN obligation.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.rollback_merge(&1, now, log, stable, &3).unwrap().unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert_eq!(
    m.group(&1).unwrap().abandoned_obligations().len(),
    2,
    "3's abort recorded its own obligation once serialized after 1's — RED stranded 3 with none"
  );

  // The service drives BOTH source thaws; draining each commits+applies its unfreeze.
  m.service_merge_applies(now, &mut stores);
  for src in [2u64, 3u64] {
    let (log, stable) = stores.0.get_mut(&src).unwrap();
    drain_storage(&mut m, src, now, log, stable);
  }
  assert!(!m.group(&2).unwrap().is_frozen(), "source 1 thawed");
  assert!(
    !m.group(&3).unwrap().is_frozen(),
    "source 3 thawed — not stranded by a stale abort"
  );
  // The observing leader defers each obligation's clear to a WITNESS, minting ONE at a time (the
  // in-flight guard serializes the fan-in); a few service+apply cycles discharge both.
  for _ in 0..4 {
    m.service_merge_applies(now, &mut stores);
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    !m.group(&1).unwrap().owes_live_thaw(),
    "both obligations discharged on the observed advances"
  );
}

/// The `rollback_merge` analogue of `commit_merge_defers_a_target_reshaping_by_a_split`. A `Split`
/// appended-and-unapplied on the target bumps its `shape_gen` when it drains, so an abort proposed
/// over it mints a generation the split immediately stales — the abort no-ops at its strict apply-time
/// guard and records NO `abandoned` obligation, leaving the frozen source owed a thaw nothing names.
/// The abort is refused `SplitInFlight`; once the split applies it mints from the post-split counter
/// and records the obligation. Admitting it mid-split is what drops the obligation.
#[test]
fn rollback_merge_defers_a_target_reshaping_by_a_split() {
  let (mut m, mut stores) = merge_host_with(SplitSm::default(), 1, SplitSm::default(), 3);
  let now = Instant::ORIGIN;

  // Freeze source 1 into target 2.
  {
    m.prepare_merge(&2, now, &mut stores, &1).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    m.group(&2).unwrap().is_frozen(),
    "source frozen for the target"
  );

  // Append a Split on the TARGET without draining it: split_in_flight is armed, unapplied.
  let split_idx = {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.propose_split(&1, now, log, stable, &3, 0, Bytes::from_static(b"\x01"))
      .unwrap()
      .unwrap()
  };
  assert!(
    m.group(&1).unwrap().split_in_flight(),
    "the target has a split appended-unapplied"
  );

  // THE FENCE: the abort defers while the target is reshaping. Admitting it lets the drained split
  // stale its mint into an obligation-less no-op that strands the frozen source.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    assert_eq!(
      m.rollback_merge(&1, now, log, stable, &2),
      Some(Err(MergeError::SplitInFlight)),
      "a target mid-split defers the abort"
    );
  }
  assert!(m.group(&2).unwrap().is_frozen(), "the source is untouched");
  assert!(
    !m.group(&1).unwrap().owes_live_thaw(),
    "no obligation-less abort was recorded on the target"
  );

  // Let the split resolve: it applies, `shape_gen` bumps, the fork relays out, the barrier lifts.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.flush_appends(&1, now, log, stable).unwrap();
    while matches!(
      m.handle_storage(&1, now, log, stable),
      Some(StorageProgress::MorePending)
    ) {}
  }
  assert!(!m.group(&1).unwrap().split_in_flight(), "the split applied");
  let fork = m.peek_yieldable_fork(&NoHold).expect("the fork relays out");
  assert_eq!((*fork.child()), 3);
  m.lift_fork_barrier(&1, split_idx);
  while m.poll_event().is_some() {}

  // Re-propose against the post-split lineage: now it admits and records the obligation.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.rollback_merge(&1, now, log, stable, &2).unwrap().unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    m.group(&1).unwrap().owes_live_thaw(),
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
    m.group(&1).unwrap().commit_merge_in_flight(),
    "the SAME merge's commit is parked, in flight"
  );
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    assert_eq!(
      m.rollback_merge(&1, now, log, stable, &2),
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

  fn supports_absorb(&self) -> bool {
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
  // The failed capture surfaces a CaptureFailed resolution — NOT a Merged teardown (that would floor
  // and drop the source) and NOT nothing (the source endpoint is already consumed, so its parked
  // callers would hang without a resolution telling the driver to fail them). CaptureFailed is the
  // driver's cue to fail the source routing typed while PRESERVING its stores/floor.
  assert_eq!(
    resolutions,
    std::vec![MergeResolution::CaptureFailed {
      source: 2,
      target: 1
    }],
    "a failed absorb capture surfaces CaptureFailed, never a Merged teardown: {resolutions:?}"
  );
  let tep = m.group(&1).unwrap();
  assert!(
    tep.is_poisoned(),
    "the failed capture fail-stops the target rather than advertising a phantom merge"
  );
  assert_eq!(tep.poison_reason(), Some(PoisonReason::SnapshotCapture));
  // The source endpoint is consumed, but it stays recoverable: its stores are untouched and its id
  // was never floored, so a restart re-parks against the restored source and the merge re-resolves.
  assert!(!m.contains_group(&2), "the source endpoint was consumed");
  assert!(stores.0.contains_key(&2), "the source's stores are intact");
  assert_eq!(stores.floor(&2), 0, "the source id was never floored");
  // The gated event: a withheld resolution must surface NO `Event::Merged`. The driver folds a
  // Merged's gen into its lineage mirror and lets the application retire external state on it, so
  // an event queued ahead of the capture would leak a false durable-union claim even though the
  // target poisoned. `poll_event` does not filter poisoned groups, so drain the whole queue and
  // prove nothing merged slipped out.
  let mut merged_events = 0;
  while let Some((_, ev)) = m.poll_event() {
    if matches!(ev, Event::Merged(_)) {
      merged_events += 1;
    }
  }
  assert_eq!(
    merged_events, 0,
    "a failed absorb capture must surface no Event::Merged"
  );
  // THE RECOVERY PIN: the union lives only in the poisoned target's volatile state machine, so
  // the consumed source's preserved stores are its only restart derivation. Every naming surface
  // refuses the id and the holder refuses its own removal, until the restart re-parks.
  assert_recovery_pinned(&mut m, &mut stores, 2, 1, SnapFailSm::default);
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
      source: 2,
      target: 1
    }]
  );
  assert!(!m.group(&1).unwrap().is_poisoned());
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, Instant::ORIGIN, log, stable);
    assert!(stable.snapshot().is_some(), "absorb capture persisted");
    assert!(log.first_index() > k, "compacted through the absorb");
  }
  assert!(
    m.service_merge_applies(Instant::ORIGIN, &mut stores)
      .is_empty(),
    "the resolution fires exactly once"
  );
}

/// The second shape at the poisoned-target exit: the target was poisoned BEFORE the resolving
/// crank (the parked worklist has no poison filter), so its absorb SUCCEEDS — the parked entry
/// applies and the union folds — into a fail-stopped state machine whose `Merged` the arm drops.
/// The union then lives nowhere durable, exactly as at the refused fold: the consumed source's
/// preserved stores are its only restart derivation, and the pin lands identically.
#[test]
fn a_target_poisoned_before_the_arm_pins_the_source_its_absorb_consumed() {
  let (mut m, mut stores) = merge_host_with(SnapFailSm::default(), 2, SnapFailSm::default(), 3);
  let k = freeze_and_park(&mut m, &mut stores);
  seal_window(&mut m, &mut stores);
  m.group_mut(&1)
    .unwrap()
    .poison(PoisonReason::ReservedShapeGen);
  let resolutions = m.service_merge_applies(Instant::ORIGIN, &mut stores);
  assert_eq!(
    resolutions,
    std::vec![MergeResolution::CaptureFailed {
      source: 2,
      target: 1
    }],
    "a pre-poisoned target's absorb surfaces CaptureFailed, never a Merged teardown: {resolutions:?}"
  );
  let tep = m.group(&1).unwrap();
  assert_eq!(
    tep.applied_index(),
    k,
    "the absorb SUCCEEDED: the parked entry applied"
  );
  assert_eq!(
    tep.state_machine().count,
    2 + 3,
    "the union folded into the fail-stopped state machine"
  );
  assert!(tep.pending_merge().is_none(), "the park was consumed");
  assert!(!m.contains_group(&2), "the source endpoint was consumed");
  let mut merged_events = 0;
  while let Some((_, ev)) = m.poll_event() {
    if matches!(ev, Event::Merged(_)) {
      merged_events += 1;
    }
  }
  assert_eq!(merged_events, 0, "the dropped Merged surfaces no event");
  assert_recovery_pinned(&mut m, &mut stores, 2, 1, SnapFailSm::default);
}

/// The pin covers the WHOLE chain a consumed source carried. S0 (3) froze into S1 (2), whose
/// absorb a standing abort fence DEFERRED — S1 holds the debt naming S0 — and S1 then froze into
/// T (1), whose forced capture FAULTS. The consumption drained S1's chain; had it died there, S0's
/// preserved stores — which S1's own restart replays its `CommitMerge` against, re-parking — would
/// be removable and re-admittable beside a union no snapshot covers. T pins BOTH.
#[test]
fn a_failed_capture_pins_the_consumed_sources_whole_debt_chain() {
  let now = Instant::ORIGIN;
  let fail = std::sync::Arc::new(core::sync::atomic::AtomicBool::new(false));
  // T = 1 (its forced capture armed to fault later) and S1 = 2, both led and drained.
  let (mut m, mut stores) = merge_host_with(
    SnapFailSm::default(),
    2,
    SnapFailSm {
      count: 0,
      fail: fail.clone(),
    },
    3,
  );
  // S0 = 3, led.
  stores
    .0
    .insert(3, (VecLog::default(), AsyncStable::default()));
  m.create_group(3, 0, single_node_cfg(1), now, 7, SnapFailSm::default())
    .unwrap();
  {
    let (l, s) = stores.0.get_mut(&3).unwrap();
    let d = m.group(&3).unwrap().poll_timeout().unwrap();
    m.handle_timeout(&3, d, l, s).unwrap();
    drain_storage(&mut m, 3, d, l, s);
    assert!(m.group(&3).unwrap().role().is_leader());
  }
  while m.poll_message().is_some() {}
  while m.poll_event().is_some() {}

  // S1's capture fence: an abort record for the unhosted 8, whose clearing rides 8's floor — the
  // embedder's timescale — so S1's absorb of S0 defers into a debt. It is a local dead-end (8 is
  // hosted nowhere), so it never holds S1's own later consumption.
  {
    let (l, s) = stores.0.get_mut(&2).unwrap();
    let abort = crate::RollbackMergePayload::abort(gid_key(8), 1, 1);
    let mut buf = Vec::new();
    crate::wire::encode_rollback_merge_payload(&abort, &mut buf);
    m.group_mut(&2)
      .unwrap()
      .propose_merge_entry(now, l, crate::EntryKind::RollbackMerge, Bytes::from(buf))
      .unwrap();
    drain_storage(&mut m, 2, now, l, s);
  }
  assert!(
    m.group(&2).unwrap().owes_live_thaw(),
    "the abort fence stands on S1"
  );
  // S0 freezes for S1 at the endpoint seam (the container's propose gate refuses the fenced
  // target; a foreign leader's gate ran unfenced).
  let freeze0 = {
    let (l, s) = stores.0.get_mut(&3).unwrap();
    let mut fbuf = Vec::new();
    crate::wire::encode_prepare_merge_payload(
      &crate::PrepareMergePayload::new(gid_key(2), 1),
      &mut fbuf,
    );
    m.group_mut(&3)
      .unwrap()
      .propose_merge_entry(now, l, crate::EntryKind::PrepareMerge, Bytes::from(fbuf))
      .unwrap();
    let idx = l.last_index();
    drain_storage(&mut m, 3, now, l, s);
    idx
  };
  assert!(m.group(&3).unwrap().is_frozen(), "S0 froze for S1");
  // S1 commits the absorb of S0 at its next lineage (the abort bumped S1 to 1) and parks; the
  // sealed window resolves it DEFERRED behind the fence — S1 holds the debt naming S0.
  {
    let (l, s) = stores.0.get_mut(&2).unwrap();
    m.group_mut(&2)
      .unwrap()
      .propose_merge_entry(
        now,
        l,
        crate::EntryKind::CommitMerge,
        commit_merge_bytes(3, freeze0, 1, 2),
      )
      .unwrap();
    drain_storage(&mut m, 2, now, l, s);
  }
  assert!(m.group(&2).unwrap().pending_merge().is_some(), "S1 parked");
  assert!(
    m.service_merge_applies(now, &mut stores).is_empty(),
    "the first pass only seals S1's window"
  );
  {
    let (l, s) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, l, s);
  }
  assert_eq!(
    m.service_merge_applies(now, &mut stores),
    std::vec![MergeResolution::Absorbed {
      source: 3,
      target: 2
    }],
    "S1's absorb of S0 defers behind the abort fence"
  );
  {
    let (l, s) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, l, s);
  }
  assert_eq!(
    m.group(&2).unwrap().capture_debt().map(|d| d.source()),
    Some(gid_key(3)),
    "S1 holds the debt naming S0"
  );
  assert!(!m.contains_group(&3) && stores.0.contains_key(&3));
  while m.poll_merge_blocked().is_some() {}

  // S1 freezes for T at the seam (a debtor refuses THIS host's propose gate; a foreign leader's
  // ran debt-free), at its next lineage (the absorb bumped S1 to 2).
  let freeze1 = {
    let (l, s) = stores.0.get_mut(&2).unwrap();
    let mut fbuf = Vec::new();
    crate::wire::encode_prepare_merge_payload(
      &crate::PrepareMergePayload::new(gid_key(1), 3),
      &mut fbuf,
    );
    m.group_mut(&2)
      .unwrap()
      .propose_merge_entry(now, l, crate::EntryKind::PrepareMerge, Bytes::from(fbuf))
      .unwrap();
    let idx = l.last_index();
    drain_storage(&mut m, 2, now, l, s);
    idx
  };
  assert!(m.group(&2).unwrap().is_frozen(), "S1 froze for T");
  // T commits the absorb of S1 and parks; the window seals; then T's forced capture is armed to
  // fault, so the resolving crank consumes S1 — its chain drained — folds the union, and fails.
  {
    let (l, s) = stores.0.get_mut(&1).unwrap();
    m.group_mut(&1)
      .unwrap()
      .propose_merge_entry(
        now,
        l,
        crate::EntryKind::CommitMerge,
        commit_merge_bytes(2, freeze1, 3, 1),
      )
      .unwrap();
    drain_storage(&mut m, 1, now, l, s);
  }
  assert!(m.group(&1).unwrap().pending_merge().is_some(), "T parked");
  assert!(
    m.service_merge_applies(now, &mut stores).is_empty(),
    "the first pass only seals T's window"
  );
  {
    let (l, s) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, l, s);
  }
  fail.store(true, core::sync::atomic::Ordering::Relaxed);
  let resolutions = m.service_merge_applies(now, &mut stores);
  assert_eq!(
    resolutions,
    std::vec![MergeResolution::CaptureFailed {
      source: 2,
      target: 1
    }],
    "the faulting capture surfaces CaptureFailed for the consumed debtor: {resolutions:?}"
  );
  assert_eq!(
    m.group(&1).unwrap().poison_reason(),
    Some(PoisonReason::SnapshotCapture)
  );
  assert!(!m.contains_group(&2), "S1 was consumed");

  // BOTH links are pinned: the consumed debtor S1, and S0 through the chain S1 carried.
  assert_recovery_pinned(&mut m, &mut stores, 2, 1, SnapFailSm::default);
  assert!(
    m.debt_names(&3),
    "the pin reaches S0, named by the chain the consumption drained"
  );
  assert!(
    matches!(m.remove_group(&3, &mut stores), Err(RemoveError::SpokenFor)),
    "S0's id refuses removal: its stores are the restored S1's own re-park derivation"
  );
  assert!(
    matches!(
      m.create_group(3, 0, single_node_cfg(1), now, 99, SnapFailSm::default()),
      Err(CreateGroupError::AbsorbPending)
    ),
    "S0's id refuses admission"
  );
  assert!(
    stores.0.contains_key(&3) && stores.floor(&3) == 0,
    "S0's stores and floor are untouched"
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
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    let a = m
      .rollback_merge(&1, Instant::ORIGIN, log, stable, &2)
      .unwrap()
      .unwrap();
    assert_eq!(
      a,
      k.next(),
      "the abort is the window's next resolution input"
    );
    drain_storage(&mut m, 1, Instant::ORIGIN, log, stable);
  }
  let resolutions = m.service_merge_applies(Instant::ORIGIN, &mut stores);
  assert_eq!(
    resolutions,
    std::vec![MergeResolution::Aborted {
      source: 2,
      target: 1
    }]
  );
  // The resumed drain applies the abort entry: lineage bump + the durable `abandoned` record.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, Instant::ORIGIN, log, stable);
  }
  assert!(m.contains_group(&2), "the source lives on");
  let tep = m.group(&1).unwrap();
  assert!(tep.pending_merge().is_none());
  assert_eq!(tep.applied_index(), k.next(), "past the park and the abort");
  assert_eq!(tep.state_machine().count(), 3, "nothing absorbed");
  assert_eq!(tep.shape_gen(), 1, "the abort bumped the target's lineage");
  assert_eq!(
    tep
      .abandoned_obligations()
      .first()
      .map(|(_, m)| m.generation),
    Some(1),
    "the target recorded exactly the abandoned freeze generation"
  );
  let mut aborted = false;
  while let Some((gid, ev)) = m.poll_event() {
    aborted |= gid == 1 && matches!(ev, Event::MergeAborted(_));
  }
  assert!(aborted, "Event::MergeAborted surfaced");
  // The per-crank service DRIVES the source thaw from the target's durable `abandoned` — it appends
  // the source-side RollbackMerge on the source's OWN log; draining commits+applies it.
  m.service_merge_applies(Instant::ORIGIN, &mut stores);
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, Instant::ORIGIN, log, stable);
  }
  let sep = m.group(&2).unwrap();
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
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, Instant::ORIGIN, log, stable);
  }
  assert!(
    !m.group(&1).unwrap().owes_live_thaw(),
    "the observed source advance discharged the obligation"
  );
}

/// The propose-time gate: `commit_merge` refuses `TargetOwesThaw` when the target already owes THIS
/// source incarnation an aborted-merge thaw — the same merge's abort applied, the source still frozen
/// at the aborted generation, its thaw not yet discharged. Re-parking there wedges on the freeze
/// generation the thaw pass drives past. GENERATION-EXACT: once the thaw discharges and the source
/// re-freezes fresh, the same target admits the new commit.
#[test]
fn commit_merge_refuses_a_target_owing_this_source_a_thaw() {
  let (mut m, mut stores) = merge_host(2, 3);
  let now = Instant::ORIGIN;
  // 1 freezes into 2 (1 frozen at gen 1, claim = 2).
  {
    m.prepare_merge(&2, now, &mut stores, &1).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(m.group(&2).unwrap().is_frozen());
  // 2 aborts the 1 -> 2 merge and APPLIES it: 2 now owes 1 a thaw at freeze generation 1, and 1 is
  // still frozen at gen 1 (its relayed thaw has not been driven).
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.rollback_merge(&1, now, log, stable, &2).unwrap().unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(m.group(&1).unwrap().owes_live_thaw(), "2 owes 1 a thaw");
  assert!(
    m.group(&2).unwrap().is_frozen(),
    "1 is still frozen at gen 1"
  );
  // The re-propose is refused GEN-EXACT — parking here would wedge on the freeze generation the
  // thaw pass drives past.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    assert_eq!(
      m.commit_merge(&1, now, log, stable, &2),
      Some(Err(MergeError::TargetOwesThaw)),
      "a target owing this source incarnation a thaw refuses the re-commit"
    );
  }
  // Drive the thaw: 1 unfreezes, then the observed advance discharges 2's obligation.
  m.service_merge_applies(now, &mut stores);
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(!m.group(&2).unwrap().is_frozen(), "the thaw unfroze 1");
  m.service_merge_applies(now, &mut stores);
  // The observing leader deferred its clear to the witness — apply it on the holder.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    !m.group(&1).unwrap().owes_live_thaw(),
    "the obligation discharged"
  );
  // GEN-EXACTNESS: 1 re-freezes FRESH (a strictly higher generation) and the same target now ADMITS
  // — the spent obligation named a dead incarnation, so its discharge cleared the refusal.
  {
    m.prepare_merge(&2, now, &mut stores, &1).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    assert!(
      matches!(m.commit_merge(&1, now, log, stable, &2), Some(Ok(_))),
      "the fresh freeze admits — generation-exact"
    );
  }
}

/// The apply-time belt for the in-flight order the propose-time gate cannot see — a `CommitMerge`
/// with a FRESH mint appended ABOVE the same merge's already-committed abort. The lineage guard
/// admits the fresh mint, so without the belt it PARKS at the aborted freeze generation and the
/// drain wedges below it once the thaw pass drives the source past that generation. The belt reads
/// `abandoned` at apply and aborts the dead commit instead: no park, no lineage bump, `MergeAborted`
/// surfaced, drain resumes.
#[test]
fn a_committed_abort_below_a_fresh_commit_kills_it_at_apply() {
  let (mut m, mut stores) = merge_host(2, 3);
  let now = Instant::ORIGIN;
  // 1 freezes into 2 (1 frozen at gen 1, claim = 2) — the real source the dead commit names.
  {
    m.prepare_merge(&2, now, &mut stores, &1).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  let mut source_bytes = Vec::new();
  Data::encode(&1u64, &mut source_bytes);
  let source_bytes = Bytes::from(source_bytes);
  // Append directly on target 2, bypassing the propose gate to reproduce the in-flight order the
  // gate cannot see: the same merge's abort BELOW, then a FRESH-mint commit ABOVE, then drain both.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    // Target-role abort at the live mint (target_gen_after = 1 against base 0): its apply records
    // abandoned[1] = freeze generation 1.
    let abort = crate::RollbackMergePayload::abort(source_bytes.clone(), 1, 1);
    let mut abuf = Vec::new();
    crate::wire::encode_rollback_merge_payload(&abort, &mut abuf);
    let a = m
      .group_mut(&1)
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
      .group_mut(&1)
      .unwrap()
      .propose_merge_entry(now, log, crate::EntryKind::CommitMerge, Bytes::from(cbuf))
      .unwrap();
    assert_eq!(k, a.next(), "the commit sits directly above the abort");
    drain_storage(&mut m, 1, now, log, stable);
  }
  let tep = m.group(&1).unwrap();
  assert!(tep.pending_merge().is_none(), "the dead commit never parks");
  assert_eq!(
    tep.shape_gen(),
    1,
    "only the abort's bump — the dead commit does not move the lineage"
  );
  assert!(m.contains_group(&2), "the source is not absorbed");
  let mut aborted = false;
  while let Some((gid, ev)) = m.poll_event() {
    aborted |= gid == 1 && matches!(ev, Event::MergeAborted(_));
  }
  assert!(aborted, "the dead commit surfaced MergeAborted");
  // NOT WEDGED: the drain ran straight through the belt-aborted commit, so a fresh proposal on 2
  // applies (a park would have stopped the drain below it forever).
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    let before = m.group(&1).unwrap().applied_index();
    m.propose(&1, now, log, stable, &Bytes::from_static(b"z"))
      .unwrap()
      .unwrap();
    drain_storage(&mut m, 1, now, log, stable);
    assert!(
      m.group(&1).unwrap().applied_index() > before,
      "the drain is not wedged at a park"
    );
  }
}

/// A target legitimately absorbs a FAN-IN of sources, so its abort obligations are a per-source
/// COLLECTION. Two sources frozen toward one target (the second from the window BEFORE the first
/// abort applied) each record their OWN obligation when aborted, and BOTH thaw. A single-slot
/// keep-first record cannot express this: the second abort silently DROPS the first source's
/// obligation, stranding it frozen forever. A `prepare_merge` one-abort-at-a-time freeze guard is no
/// substitute — it forbids this supported shape, and is insufficient anyway, since a source frozen
/// before the first abort applied slips past it entirely.
#[test]
fn both_fanned_in_aborts_thaw_neither_dropped() {
  // Fan-in of sources 2 and 3 into target 1 (each source encodes above the target).
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

  // BOTH sources freeze toward target 1 — the concurrent fan-in (neither abort has applied yet, so
  // the retired freeze guard could not have caught the second one regardless).
  for src in [2u64, 3u64] {
    m.prepare_merge(&src, now, &mut stores, &1)
      .unwrap()
      .unwrap();
    let (log, stable) = stores.0.get_mut(&src).unwrap();
    drain_storage(&mut m, src, now, log, stable);
    assert!(m.group(&src).unwrap().is_frozen(), "source {src} froze");
  }

  // Target 1 aborts BOTH merges (draining between so each abort's mint is live). The SECOND abort
  // must not drop the FIRST source's obligation.
  for src in [2u64, 3u64] {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.rollback_merge(&1, now, log, stable, &src)
      .unwrap()
      .unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert_eq!(
    m.group(&1).unwrap().abandoned_obligations().len(),
    2,
    "both fanned-in aborts recorded their own obligation — RED (keep-first) kept only one"
  );

  // The service drives BOTH source thaws; draining each commits+applies its unfreeze.
  m.service_merge_applies(now, &mut stores);
  for src in [2u64, 3u64] {
    let (log, stable) = stores.0.get_mut(&src).unwrap();
    drain_storage(&mut m, src, now, log, stable);
  }
  assert!(!m.group(&2).unwrap().is_frozen(), "source 2 thawed");
  assert!(
    !m.group(&3).unwrap().is_frozen(),
    "source 3 thawed too — the second obligation was NOT silently dropped"
  );
  // The observing leader defers each obligation's clear to a WITNESS, minting ONE at a time (the
  // in-flight guard serializes the fan-in); a few service+apply cycles discharge both.
  for _ in 0..4 {
    m.service_merge_applies(now, &mut stores);
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    !m.group(&1).unwrap().owes_live_thaw(),
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
/// the SAME source-3 abort admits, records `abandoned[3]`, and the service thaws source 3. Without
/// the cross-source arms the abort ADMITS `Some(Ok(_))` and source 3 strands.
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
  for src in [2u64, 3u64] {
    m.prepare_merge(&src, now, &mut stores, &1)
      .unwrap()
      .unwrap();
    let (log, stable) = stores.0.get_mut(&src).unwrap();
    drain_storage(&mut m, src, now, log, stable);
    assert!(m.group(&src).unwrap().is_frozen(), "source {src} froze");
  }

  // COMMIT source 1 into 2 and PARK it — do NOT resolve.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.commit_merge(&1, now, log, stable, &2).unwrap().unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    m.group(&1).unwrap().pending_merge().is_some(),
    "source 1's commit is parked"
  );
  assert!(
    m.group(&1).unwrap().commit_merge_in_flight(),
    "the parked commit is still in flight (applied held at k-1)"
  );

  // THE FENCE: the CROSS-source abort (source 3) defers to source 1's parked commit.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    assert_eq!(
      m.rollback_merge(&1, now, log, stable, &3),
      Some(Err(MergeError::AlreadyPending)),
      "a cross-source abort must NOT race a parked commit of a DIFFERENT source"
    );
  }
  // The fence appended nothing: source 1's park stands and source 3 stays frozen.
  assert!(m.group(&1).unwrap().pending_merge().is_some());
  assert!(m.group(&3).unwrap().is_frozen(), "source 3 stayed frozen");

  // SELF-CLEARING: resolve source 1's parked commit (seal the window, then absorb).
  seal_window(&mut m, &mut stores);
  let resolutions = m.service_merge_applies(now, &mut stores);
  assert_eq!(
    resolutions,
    std::vec![MergeResolution::Merged {
      source: 2,
      target: 1
    }],
    "source 1 absorbed"
  );
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    m.group(&1).unwrap().pending_merge().is_none(),
    "the park cleared"
  );
  assert!(!m.group(&1).unwrap().commit_merge_in_flight());

  // The SAME source-3 abort now ADMITS off the bumped live lineage.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    let admitted = m.rollback_merge(&1, now, log, stable, &3);
    assert!(
      matches!(admitted, Some(Ok(_))),
      "with source 1's park resolved the source-3 abort admits: {admitted:?}"
    );
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert_eq!(
    m.group(&1).unwrap().abandoned_obligations().len(),
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
///   in the pre-park window too; the sim band pins the same race end-to-end. The in-flight arm must
///   discriminate BY SOURCE: a blanket admit strands source 3, and a blanket defer — what a coarse
///   `commit_merge_in_flight` check does, deferring every unparked abort, same-source included —
///   leaves source 1 unable to release its own stuck commit.
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
  for src in [2u64, 3u64] {
    m.prepare_merge(&src, now, &mut stores, &1)
      .unwrap()
      .unwrap();
    let (log, stable) = stores.0.get_mut(&src).unwrap();
    drain_storage(&mut m, src, now, log, stable);
    assert!(m.group(&src).unwrap().is_frozen(), "source {src} froze");
  }

  // COMMIT source 1 into 2 — APPEND ONLY, no drain: the commit stays in flight and UNPARKED.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.commit_merge(&1, now, log, stable, &2).unwrap().unwrap();
  }
  assert!(
    m.group(&1).unwrap().commit_merge_in_flight(),
    "source 1's commit is in flight"
  );
  assert!(
    m.group(&1).unwrap().pending_merge().is_none(),
    "and NOT yet parked — only the log decode, not `pending_merge`, can name its source"
  );

  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    // CROSS-source abort (3): decoded `1 != 3` → defer, append nothing.
    assert_eq!(
      m.rollback_merge(&1, now, log, stable, &3),
      Some(Err(MergeError::AlreadyPending)),
      "a cross-source abort must not race an IN-FLIGHT commit of a DIFFERENT source"
    );
    // SAME-source abort (1): decoded `1 == 1` → race the in-flight commit's release valve.
    assert!(
      matches!(m.rollback_merge(&1, now, log, stable, &2), Some(Ok(_))),
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
/// merge that 2 aborts (recording `abandoned[1]`); that thaw is still undischarged. The freeze is
/// refused `SourceOwesThaw`, and once the thaw pass discharges 1 the SAME freeze admits (the
/// self-clearing pin). Admitting `prepare_merge(2 -> 3)` dissolves 2 into 3 and its `abandoned[1]`
/// vanishes with the endpoint — 1 stranded frozen forever.
#[test]
fn a_source_owing_a_thaw_cannot_freeze_as_a_source() {
  let (mut m, mut stores) = merge_host_triple(4, 3, 2);
  let now = Instant::ORIGIN;
  // 1 freezes into 2, then 2 aborts the merge and APPLIES it: 2 now owes 1 a thaw.
  {
    m.prepare_merge(&3, now, &mut stores, &2).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&3).unwrap();
    drain_storage(&mut m, 3, now, log, stable);
  }
  assert!(m.group(&3).unwrap().is_frozen(), "source 1 froze into 2");
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.rollback_merge(&2, now, log, stable, &3).unwrap().unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    m.group(&2).unwrap().owes_live_thaw(),
    "2 recorded its target-role abort obligation for 1"
  );

  // The gate: 2 cannot freeze as a source while it still owes 1 a thaw.
  {
    assert!(
      matches!(
        m.prepare_merge(&2, now, &mut stores, &1),
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
    let (log, stable) = stores.0.get_mut(&3).unwrap();
    drain_storage(&mut m, 3, now, log, stable);
  }
  assert!(!m.group(&3).unwrap().is_frozen(), "the thaw unfroze 1");
  m.service_merge_applies(now, &mut stores);
  // The observing leader deferred its clear to the witness — apply it on the holder.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    !m.group(&2).unwrap().owes_live_thaw(),
    "the observed advance discharged 2's obligation"
  );

  // SELF-CLEARING PIN: with the obligation discharged, the SAME freeze now admits.
  {
    assert!(
      m.prepare_merge(&2, now, &mut stores, &1).unwrap().is_ok(),
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
/// The removal is REFUSED `OwesThaw`, tearing nothing down; once the thaw pass discharges 1 the SAME
/// `remove_group(&2)` admits (the self-clearing pin). Letting it through drops 2's endpoint and
/// stores, stranding 1 frozen forever with no holder left to run the thaw pass. Removing a group
/// that owes nothing (3) is unaffected — `Ok(Some(endpoint))`.
#[test]
fn teardown_refuses_a_group_that_still_owes_a_thaw() {
  let (mut m, mut stores) = merge_host_triple(3, 2, 4);
  let now = Instant::ORIGIN;
  // 1 freezes into 2, then 2 aborts the merge and APPLIES it: 2 now owes 1 a thaw.
  {
    m.prepare_merge(&2, now, &mut stores, &1).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(m.group(&2).unwrap().is_frozen(), "source 1 froze into 2");
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.rollback_merge(&1, now, log, stable, &2).unwrap().unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    m.group(&1).unwrap().owes_live_thaw(),
    "2 recorded its target-role abort obligation for 1"
  );

  // NEGATIVE PIN: removing a group that owes nothing is unchanged — Ok(Some(endpoint)).
  assert!(
    m.remove_group(&3, &mut stores).unwrap().is_some(),
    "a group with no thaw obligation tears down exactly as before"
  );

  // THE GATE: 2 cannot be torn down while it still owes 1 a thaw.
  assert!(
    matches!(m.remove_group(&1, &mut stores), Err(RemoveError::OwesThaw)),
    "a holder of an undischarged thaw is refused OwesThaw — never torn down"
  );
  // The refusal tore NOTHING down, so nothing is stranded: 2 is still hosted with its obligation,
  // and 1 is still frozen but still HAS a holder (2) to run the thaw.
  assert!(m.contains_group(&1), "the refused removal left 2 hosted");
  assert!(
    m.group(&1).unwrap().owes_live_thaw(),
    "2 still owes 1 the thaw"
  );
  assert!(
    m.group(&2).unwrap().is_frozen(),
    "1 is still frozen, but 2 survives to thaw it"
  );

  // Drive the thaw pass: it unfreezes 1 from 2's obligation; the next pass discharges it.
  m.service_merge_applies(now, &mut stores);
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(!m.group(&2).unwrap().is_frozen(), "the thaw unfroze 1");
  m.service_merge_applies(now, &mut stores);
  // The observing leader deferred its clear to the witness — apply it on the holder.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    !m.group(&1).unwrap().owes_live_thaw(),
    "the observed advance discharged 2's obligation"
  );

  // SELF-CLEARING PIN: with the obligation discharged, the SAME removal now admits.
  assert!(
    m.remove_group(&1, &mut stores).unwrap().is_some(),
    "once the thaw discharges, 2 tears down exactly as a clean group would"
  );
  assert!(!m.contains_group(&1), "2 is gone");
}

/// The PUBLIC teardown gate refuses EVERY unresolved merge participant, not just a thaw-ower. Group
/// 1 freezes into 2 and 2 parks its `CommitMerge`: 1 is a frozen SOURCE, 2 a parked TARGET. 1 refuses
/// `Frozen`, 2 refuses `MergeParked`, and neither refusal touches a thing (the group stays hosted
/// with its exact merge state). Letting either removal through strands the other half — 1 torn out
/// leaves 2's park with no source to absorb or abort against; 2 torn out leaves 1 frozen with no
/// decider. Once the merge resolves — here by
/// abort + thaw — the SAME removals admit (the self-clearing pin). A non-participant (3) is
/// byte-for-byte unchanged.
#[test]
fn teardown_refuses_a_frozen_source_and_a_parked_target() {
  let (mut m, mut stores) = merge_host_triple(3, 2, 4);
  let now = Instant::ORIGIN;
  let k = freeze_and_park(&mut m, &mut stores);

  // NEGATIVE PIN: a non-participant tears down exactly as before — Ok(Some(endpoint)).
  assert!(
    m.remove_group(&3, &mut stores).unwrap().is_some(),
    "a non-participant removal is byte-for-byte unchanged"
  );

  // THE GATE, leg 2: a frozen source cannot leave — its target's park resolves against this freeze.
  assert!(
    matches!(m.remove_group(&2, &mut stores), Err(RemoveError::Frozen)),
    "a frozen merge source is refused Frozen"
  );
  // THE GATE, leg 3: a parked target cannot leave — its frozen source needs the decider.
  assert!(
    matches!(
      m.remove_group(&1, &mut stores),
      Err(RemoveError::MergeParked)
    ),
    "a target parked on a commit is refused MergeParked"
  );
  // NO SIDE EFFECTS: both refusals left the choreography fully intact — neither half is stranded.
  assert!(
    m.contains_group(&2) && m.group(&2).unwrap().is_frozen(),
    "1 is still a frozen source"
  );
  assert!(
    m.contains_group(&1) && m.group(&1).unwrap().pending_merge().is_some(),
    "2 is still parked on its commit"
  );
  assert!(
    !m.group(&1).unwrap().owes_live_thaw(),
    "no obligation was fabricated by the refusal"
  );

  // Resolve by ABORT + THAW: the abort lands at k+1 on 2's own log and un-parks it aborted; the
  // per-crank thaw pass then unfreezes 1 and discharges 2's obligation. Both become clean groups.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    let a = m.rollback_merge(&1, now, log, stable, &2).unwrap().unwrap();
    assert_eq!(a, k.next(), "the abort is the window's resolution input");
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert_eq!(
    m.service_merge_applies(now, &mut stores),
    std::vec![MergeResolution::Aborted {
      source: 2,
      target: 1
    }],
    "the park resolves aborted"
  );
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  m.service_merge_applies(now, &mut stores);
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(!m.group(&2).unwrap().is_frozen(), "the thaw unfroze 1");
  m.service_merge_applies(now, &mut stores);
  // The observing leader deferred its clear to the witness — apply it on the holder.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    !m.group(&1).unwrap().owes_live_thaw(),
    "the observed advance discharged 2's obligation"
  );

  // SELF-CLEARING PIN: with the merge resolved, the SAME two removals now admit.
  assert!(
    m.remove_group(&2, &mut stores).unwrap().is_some(),
    "the thawed former source tears down as a clean group"
  );
  assert!(
    m.remove_group(&1, &mut stores).unwrap().is_some(),
    "the resolved former target tears down as a clean group"
  );
}

/// The ONE deliberate teardown ESCAPE: an OWED source — a frozen source a hosted target already owes
/// a thaw — is removable, because the removal PURGE binds every holder's obligation to the departing
/// incarnation and the driver FLOORS the id (the catalog recovery for a genuinely-dead frozen
/// participant). Source 2 freezes into target 1 and 1 ABORTS, recording `abandoned[2]` while 2 stays
/// frozen. `remove_group(&2)` ADMITS despite the active freeze — leg 2 steps aside for exactly this —
/// and the purge clears 1's now-dangling obligation so no stale record can ever back a recreate's
/// thaw.
///
/// THE FLOOR is the escape's other half, so it runs here on the engine the drivers actually read: an
/// owed frozen source ALWAYS carries a nonzero removal ceiling, which is what forbids the drivers'
/// `if floor > 0` guard from skipping an escaped removal and leaving the departed incarnation
/// re-admittable at the very generation the purged obligation named. The ceiling stands on the LOG
/// alone in the mirror-lost crash window: the applied `PrepareMerge` names the generation the freeze
/// minted, and a frozen source can never capture, so nothing compacts that entry away.
#[test]
fn teardown_admits_an_owed_frozen_source_and_purges_the_obligation() {
  let mut engine: GroupEngine<u64, u64> = GroupEngine::new();
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let now = Instant::ORIGIN;
  engine_group(&mut m, &mut engine, 1, 0, now);
  engine_group(&mut m, &mut engine, 2, 0, now);

  // 2 freezes into 1, and 1 parks the absorb at k.
  m.prepare_merge(&2, now, &mut engine, &1).unwrap().unwrap();
  engine_crank(&mut m, &mut engine, 2, now);
  assert!(m.group(&2).unwrap().is_frozen());
  let k = {
    let (log, stable) = engine.stores(&1).unwrap();
    m.commit_merge(&1, now, log, stable, &2).unwrap().unwrap()
  };
  engine_crank(&mut m, &mut engine, 1, now);
  assert!(m.group(&1).unwrap().pending_merge().is_some(), "parked");

  // 1 aborts the merge and APPLIES it: 1 now owes 2 a thaw, and 2 is STILL a frozen source.
  {
    let (log, stable) = engine.stores(&1).unwrap();
    let a = m.rollback_merge(&1, now, log, stable, &2).unwrap().unwrap();
    assert_eq!(a, k.next(), "the abort resolves the parked window");
  }
  engine_crank(&mut m, &mut engine, 1, now);
  m.service_merge_applies(now, &mut engine);
  engine_crank(&mut m, &mut engine, 1, now);
  assert!(
    m.group(&2).unwrap().is_frozen(),
    "2 is still a frozen source"
  );
  assert!(m.group(&1).unwrap().owes_live_thaw(), "1 owes 2 the thaw");

  // THE FLOOR, MIRROR-LOST: no lineage mirror landed for 2, so its ceiling rests entirely on the
  // `PrepareMerge` still resident in its own log.
  assert_eq!(
    engine.group_gen(&2),
    0,
    "the source carries no lineage record — the crash window the stores leg exists for"
  );
  assert!(
    engine.removal_floor(&2) > 0,
    "an owed frozen source fences off its own log alone — the drivers' `if floor > 0` guard must \
     never skip flooring an escaped removal"
  );

  // THE FLOOR, MIRRORED: the drivers' event fold lands the freeze's generation in the record, which
  // joins the same ceiling rather than replacing it.
  fold_lineage_events(&mut m, &mut engine);
  assert!(engine.group_gen(&2) > 0, "the fold mirrored the freeze");
  assert!(
    engine.removal_floor(&2) > m.group(&2).unwrap().shape_gen(),
    "the floor sits one past the incarnation's ceiling, so the generation the purged obligation \
     named can never be re-admitted"
  );

  // THE FENCE, before the escape: the undischarged obligation fences the holder's own capture.
  let applied = m.group(&1).unwrap().applied_index();
  assert!(
    m.group(&1).unwrap().capture_blocked_at(applied),
    "the outstanding obligation fences the holder's capture at its applied index"
  );

  // THE ESCAPE: removing the OWED source ADMITS even though it is frozen (leg 2 suppressed).
  assert!(
    m.remove_group(&2, &mut engine).unwrap().is_some(),
    "an owed frozen source is the designed catalog escape — the removal admits"
  );
  // THE PURGE ALONE LIFTS THE FENCE: nothing thawed anywhere, yet this leader may now capture past
  // the abort — so a co-hosting replica that later installs that capture must read no thaw into
  // its boundary (the covered obligation's whole rationale).
  assert!(
    !m.group(&1).unwrap().capture_blocked_at(applied),
    "the escape's purge lifts the holder's capture fence with no thaw anywhere"
  );
  // The removal purge discharged the obligation the departed source can no longer thaw.
  assert!(
    !m.group(&1).unwrap().owes_live_thaw(),
    "the purge cleared the holder's obligation for the departed incarnation"
  );
}

/// A single host whose target 1 and source 2 both restart from durable logs mid-choreography — the
/// shape a lagging co-hosting replica pair is in when the transferring leader's snapshot arrives:
/// 1 holds a committed+applied TARGET-role abort of 2's freeze at generation 1 (its obligation
/// re-derived by replay: `abandoned[2]`, abort entry at index 1) and 2 restarts FROZEN at exactly
/// that generation with its claim on 1 (its `PrepareMerge` re-applied). Neither has fired a
/// timeout, so both are followers — which is what lets 1 INSTALL.
fn restored_holder_and_frozen_source() -> (MultiRaft<u64, u64, CountSm>, MapStores) {
  restored_pair_with_holder_cfg(single_node_cfg(1))
}

/// [`restored_holder_and_frozen_source`] with the holder's config settable (a snapshot threshold
/// for the capture pins).
fn restored_pair_with_holder_cfg(
  holder_cfg: Config<u64>,
) -> (MultiRaft<u64, u64, CountSm>, MapStores) {
  let now = Instant::ORIGIN;
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let mut stores = MapStores(std::collections::BTreeMap::new(), Default::default());
  // Target 1: the abort at index 1, committed and applied.
  let abort = {
    let p = crate::RollbackMergePayload::abort(gid_key(2), 1, 1);
    let mut buf = Vec::new();
    crate::wire::encode_rollback_merge_payload(&p, &mut buf);
    Bytes::from(buf)
  };
  let mut tlog = VecLog::default();
  let mut tstable = AsyncStable::default();
  tlog.force_append(&[crate::Entry::new(
    Term::new(1),
    Index::new(1),
    crate::EntryKind::RollbackMerge,
    abort,
  )]);
  tstable.force_state(Term::new(1), Some(1u64), Index::new(1));
  m.restore_group_unchecked(
    1,
    holder_cfg,
    now,
    7,
    CountSm::default(),
    1,
    &mut tlog,
    &mut tstable,
  )
  .unwrap();
  stores.0.insert(1, (tlog, tstable));
  // Source 2: the freeze naming 1 at generation 1, committed and applied.
  let mut slog = VecLog::default();
  let mut sstable = AsyncStable::default();
  slog.force_append(&[crate::Entry::new(
    Term::new(1),
    Index::new(1),
    crate::EntryKind::PrepareMerge,
    prepare_merge_bytes(1, 1),
  )]);
  sstable.force_state(Term::new(1), Some(1u64), Index::new(1));
  m.restore_group_unchecked(
    2,
    single_node_cfg(1),
    now,
    8,
    CountSm::default(),
    1,
    &mut slog,
    &mut sstable,
  )
  .unwrap();
  stores.0.insert(2, (slog, sstable));
  while m.poll_event().is_some() {}
  while m.poll_message().is_some() {}
  let t = m.group(&1).unwrap();
  assert!(t.role().is_follower(), "1 has not campaigned");
  assert_eq!(
    t.owes_live_thaw_for(&gid_key(2)),
    Some(1),
    "replay re-derived 1's obligation for 2's freeze at generation 1"
  );
  assert!(
    t.abandoned_record(&gid_key(2))
      .is_some_and(|r| r.cover == Cover::None && !r.discharged),
    "re-derived from the entry: uncovered and live"
  );
  let sp = m.group(&2).unwrap();
  assert!(
    sp.role().is_follower() && sp.is_frozen(),
    "2 restarted frozen"
  );
  assert_eq!(sp.shape_gen(), 1, "at exactly the abandoned generation");
  assert_eq!(sp.frozen_for(), Some(&gid_key(1)), "claiming 1");
  (m, stores)
}

/// Install the transferring leader's snapshot into restored target 1 — boundary 5, past the abort
/// entry at 1: the destructive deferred install, drained to completion.
fn install_past_the_abort(m: &mut MultiRaft<u64, u64, CountSm>, stores: &mut MapStores) {
  let now = Instant::ORIGIN;
  let meta = crate::SnapshotMeta::new(
    Index::new(5),
    Term::new(1),
    crate::conf::ConfState::from_voters(std::vec![1u64]),
  )
  .with_shape_gen(1);
  let (log, stable) = stores.0.get_mut(&1).unwrap();
  m.handle_message(
    &1,
    now,
    log,
    stable,
    9u64,
    Message::InstallSnapshot(crate::InstallSnapshot::new(
      Term::new(1),
      9u64,
      meta,
      fork_blob(3),
    )),
  )
  .unwrap();
  drain_storage(m, 1, now, log, stable);
  assert_eq!(
    m.group(&1).unwrap().applied_index(),
    Index::new(5),
    "the install landed past the abort"
  );
}

/// THE INSTALL RETAINS A LIVE OBLIGATION (#132). The transferring leader captured past the abort
/// because ITS host removed the source through the owed-source escape — a host-local purge, no
/// thaw anywhere — and this lagging holder installs that capture with the source still frozen
/// RIGHT HERE. The boundary crossed the abort entry, so the obligation is marked install-covered
/// and KEPT, live: it stands through a crank on which the frozen source cannot lead (no local
/// discharge — a cover is not a proof for a hosted source), it drives the thaw the moment the
/// source leads, and it retires only once the source is observed past the abandoned generation —
/// DISCHARGED, not cleared: this follower keeps the record as the witness trigger. What the install
/// does lift is the capture fence — the entry it protected is gone. A drop at the install erases
/// the only drive of that thaw, holder by holder, and strands the source frozen for good.
#[test]
fn an_install_past_the_abort_retains_the_live_obligation_until_the_source_thaws() {
  let (mut m, mut stores) = restored_holder_and_frozen_source();
  let now = Instant::ORIGIN;
  let source = gid_key(2);
  install_past_the_abort(&mut m, &mut stores);
  let t = m.group(&1).unwrap();
  assert!(
    t.owes_live_thaw(),
    "RETAINED: the boundary proves the leader's fence lifted, not that 2 thawed — 2 is frozen here"
  );
  let record = t.abandoned_record(&source).expect("retained");
  assert_eq!(
    (record.cover, record.discharged),
    (Cover::Install, false),
    "marked install-covered, still live"
  );
  assert!(
    !t.abort_relay_fences(t.applied_index()) && !t.capture_blocked_at(t.applied_index()),
    "the install lifted the capture fence: the entry it protected is gone"
  );
  assert!(m.group(&2).unwrap().is_frozen(), "2 is still frozen");

  // The pass while 2 is a FOLLOWER: hosted and frozen, so no local discharge — the record STANDS
  // (a cover is no proof for a hosted source), and the drive answers a transient refusal.
  m.service_merge_applies(now, &mut stores);
  assert!(
    m.group(&1).unwrap().owes_live_thaw(),
    "a hosted frozen source is not discharged by a cover — the obligation stands until the thaw"
  );
  assert!(m.group(&2).unwrap().is_frozen(), "nothing thawed 2 yet");

  // 2 elects itself (single voter); the pass now drives its thaw off the retained record.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    let d = m.group(&2).unwrap().poll_timeout().unwrap();
    m.handle_timeout(&2, d, log, stable).unwrap();
    drain_storage(&mut m, 2, d, log, stable);
    assert!(m.group(&2).unwrap().role().is_leader(), "2 leads");
  }
  m.service_merge_applies(now, &mut stores);
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    !m.group(&2).unwrap().is_frozen(),
    "the retained obligation drove 2's thaw"
  );
  assert_eq!(
    m.group(&2).unwrap().shape_gen(),
    2,
    "past the abandoned generation"
  );

  // The next pass OBSERVES 2 past `expected` — a global proof — and RETIRES the record: discharged,
  // owed by no live gate, but kept as this follower's witness trigger.
  m.service_merge_applies(now, &mut stores);
  let t = m.group(&1).unwrap();
  assert!(
    !t.owes_live_thaw(),
    "observed past the abandoned generation — no live obligation remains"
  );
  assert!(
    t.abandoned_record(&source).is_some_and(|m| m.discharged),
    "the record is KEPT, discharged — the trigger a later leader term mints the witness from"
  );
  assert!(
    !t.capture_blocked_at(t.applied_index()),
    "nothing fences the capture"
  );
  assert_eq!(
    witness_count(&stores.0.get(&1).unwrap().0),
    0,
    "1 is a follower: it mints no witness"
  );
}

/// THE ESCAPE STAYS OPEN (#132): after the install covered the obligation, the holder still OWES
/// the frozen source at its live generation, so `remove_group(2)` ADMITS through the owed-source
/// escape — the operator's door for a source nothing can drive — and the purge lifts the holder's
/// fence. A drop at the install closes that door: with no holder owing it, the frozen source's
/// removal refuses `Frozen`, and the pair is stuck for good.
#[test]
fn the_owed_source_escape_admits_after_a_covering_install() {
  let (mut m, mut stores) = restored_holder_and_frozen_source();
  install_past_the_abort(&mut m, &mut stores);
  let applied = m.group(&1).unwrap().applied_index();
  assert_eq!(
    m.remove_group(&2, &mut stores).map(|r| r.is_some()),
    Ok(true),
    "the retained record keeps 2 an OWED source — the escape admits despite the freeze"
  );
  assert!(
    !m.group(&1).unwrap().owes_live_thaw(),
    "the purge cleared the covered record"
  );
  assert!(
    !m.group(&1).unwrap().capture_blocked_at(applied),
    "and lifted the fence"
  );
}

/// ABSENCE IS TRANSIENT — the restore-and-recommit hazard (#132, #138). A holder's record was
/// install-covered while its source was absent from this container; nothing here can observe or
/// drive that source, and the cranks must leave the record STANDING. The source then comes back —
/// restored from preserved stores, still frozen at exactly the abandoned generation — and leads.
/// `commit_merge(1, 2)` for that generation must refuse `TargetOwesThaw`: a holder that had disposed
/// of its record on absence alone would pass the gate and mint a fresh commit for the ABORTED
/// generation, which every replica still holding the record no-ops at the same-merge abort belt
/// while this one parks and absorbs — one committed target log, divergent lineage and state.
#[test]
fn a_restored_source_frozen_at_the_covered_generation_cannot_be_recommitted() {
  let now = Instant::ORIGIN;
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let mut stores = MapStores(std::collections::BTreeMap::new(), Default::default());
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
  // The record as a transfer left it while 2 was absent: abort at 1, install-covered.
  let source = gid_key(2);
  {
    let ep = m.group_mut(&1).unwrap();
    ep.note_abandoned(source.clone(), 1, Index::new(1));
    ep.note_abort_covered(Index::new(1), Cover::Install);
  }
  for _ in 0..3 {
    m.service_merge_applies(now, &mut stores);
  }
  assert!(
    m.group(&1).unwrap().owes_live_thaw()
      && m
        .group(&1)
        .unwrap()
        .abandoned_record(&source)
        .is_some_and(|r| !r.discharged),
    "absence disposes of nothing: the covered record stands, live, across cranks"
  );
  assert_eq!(
    witness_count(&stores.0.get(&1).unwrap().0),
    0,
    "and a cover is no global proof — nothing was witnessed"
  );

  // 2 comes back from preserved stores, still frozen at the abandoned generation, claiming 1.
  {
    let mut slog = VecLog::default();
    let mut sstable = AsyncStable::default();
    slog.force_append(&[crate::Entry::new(
      Term::new(1),
      Index::new(1),
      crate::EntryKind::PrepareMerge,
      prepare_merge_bytes(1, 1),
    )]);
    sstable.force_state(Term::new(1), Some(1u64), Index::new(1));
    m.restore_group_unchecked(
      2,
      single_node_cfg(1),
      now,
      8,
      CountSm::default(),
      1,
      &mut slog,
      &mut sstable,
    )
    .unwrap();
    stores.0.insert(2, (slog, sstable));
    // It leads (single voter), so the freeze barrier is met and the commit reaches the gate.
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    let d = m.group(&2).unwrap().poll_timeout().unwrap();
    m.handle_timeout(&2, d, log, stable).unwrap();
    drain_storage(&mut m, 2, d, log, stable);
  }
  let sp = m.group(&2).unwrap();
  assert!(
    sp.role().is_leader() && sp.is_frozen() && sp.shape_gen() == 1,
    "2 leads, frozen at exactly the abandoned generation"
  );
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    assert!(
      matches!(
        m.commit_merge(&1, now, log, stable, &2),
        Some(Err(MergeError::TargetOwesThaw))
      ),
      "the retained record refuses the dead commit for the aborted generation"
    );
  }
}

/// THE `OwesThaw` STEP-ASIDE (#132): the teardown leg refuses a holder of a LIVE obligation, but
/// steps aside when every live record it holds is covered and names a source not hosted here — such
/// a record drives nothing on this host (the drive needs the source's own stores), so the removal
/// strands no thaw. It keeps refusing for an UNCOVERED dead end (its entry is the obligation's
/// replay source, still fencing), for a covered record whose source IS hosted here (that record
/// drives the source and keeps its own removal admissible through the owed-source escape), and for
/// a DISCHARGED record — a witness debt: the observation that discharged it may be knowledge no
/// other replica can reproduce, so the record is the only future witness trigger and no step-aside
/// applies.
#[test]
fn the_owes_thaw_leg_steps_aside_for_covered_dead_ends_only() {
  let now = Instant::ORIGIN;
  let holder = |cover: Option<Cover>| {
    let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
    let mut stores = MapStores(std::collections::BTreeMap::new(), Default::default());
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
    }
    while m.poll_message().is_some() {}
    while m.poll_event().is_some() {}
    let ep = m.group_mut(&1).unwrap();
    ep.note_abandoned(gid_key(2), 1, Index::new(1));
    if let Some(cover) = cover {
      ep.note_abort_covered(Index::new(1), cover);
    }
    (m, stores)
  };
  // Install-covered, source unhosted: the step-aside admits.
  let (mut m, mut stores) = holder(Some(Cover::Install));
  assert_eq!(
    m.remove_group(&1, &mut stores).map(|r| r.is_some()),
    Ok(true),
    "an install-covered dead end drives nothing here — the holder's removal admits"
  );
  // Uncovered, source unhosted: refused — the entry is the obligation's replay source.
  let (mut m, mut stores) = holder(None);
  assert_eq!(
    m.remove_group(&1, &mut stores).map(|r| r.is_some()),
    Err(RemoveError::OwesThaw),
    "an uncovered obligation keeps refusing"
  );
  // Install-covered, source HOSTED (restored frozen at the generation): refused — it drives.
  let (mut m, mut stores) = holder(Some(Cover::Install));
  {
    let mut slog = VecLog::default();
    let mut sstable = AsyncStable::default();
    slog.force_append(&[crate::Entry::new(
      Term::new(1),
      Index::new(1),
      crate::EntryKind::PrepareMerge,
      prepare_merge_bytes(1, 1),
    )]);
    sstable.force_state(Term::new(1), Some(1u64), Index::new(1));
    m.restore_group_unchecked(
      2,
      single_node_cfg(1),
      now,
      8,
      CountSm::default(),
      1,
      &mut slog,
      &mut sstable,
    )
    .unwrap();
    stores.0.insert(2, (slog, sstable));
  }
  assert!(m.group(&2).unwrap().is_frozen());
  assert_eq!(
    m.remove_group(&1, &mut stores).map(|r| r.is_some()),
    Err(RemoveError::OwesThaw),
    "a covered record whose source is hosted here drives it — the holder keeps refusing"
  );
  // A DISCHARGED record is a WITNESS DEBT: the holder keeps refusing until the witness applies.
  let (mut m, mut stores) = holder(None);
  m.group_mut(&1).unwrap().note_discharged(&gid_key(2));
  assert_eq!(
    m.remove_group(&1, &mut stores).map(|r| r.is_some()),
    Err(RemoveError::OwesThaw),
    "a discharged record is the only future witness trigger — no step-aside for it"
  );
}

/// A FOLLOWER OBSERVER KEEPS ITS WITNESS TRIGGER (#137). Target 1, a follower, observes source 2
/// thawed past the abandoned generation — a global proof — and RETIRES its record without erasing
/// it: discharged, owed by no live gate, no witness (a follower mints none). When 1 later leads, the
/// kept record is exactly what it mints the `ThawDischarged` witness from, and the committed apply
/// clears the record. A follower that cleared its record on the observation took the trigger with
/// it: on its first term it would have found nothing to witness, and every replica that could not
/// observe 2 itself would have kept the ghost.
#[test]
fn a_follower_that_observed_the_thaw_keeps_its_witness_trigger() {
  let (mut m, mut stores) = restored_holder_and_frozen_source();
  let now = Instant::ORIGIN;
  let source = gid_key(2);
  // 2 leads; the pass drives its thaw off 1's live record; 2 applies it.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    let d = m.group(&2).unwrap().poll_timeout().unwrap();
    m.handle_timeout(&2, d, log, stable).unwrap();
    drain_storage(&mut m, 2, d, log, stable);
  }
  m.service_merge_applies(now, &mut stores);
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert_eq!(
    m.group(&2).unwrap().shape_gen(),
    2,
    "2 thawed past the generation"
  );
  // 1, a FOLLOWER, observes the global proof: discharged, kept, unwitnessed.
  m.service_merge_applies(now, &mut stores);
  let t = m.group(&1).unwrap();
  assert!(t.role().is_follower());
  assert!(!t.owes_live_thaw(), "no live obligation remains");
  assert!(
    t.abandoned_record(&source).is_some_and(|r| r.discharged),
    "the record is kept, discharged — the witness trigger"
  );
  assert_eq!(
    witness_count(&stores.0.get(&1).unwrap().0),
    0,
    "a follower mints nothing"
  );
  // 1 leads: the kept record is the trigger — the witness is minted and its apply clears it.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    let d = m.group(&1).unwrap().poll_timeout().unwrap();
    m.handle_timeout(&1, d, log, stable).unwrap();
    drain_storage(&mut m, 1, d, log, stable);
    assert!(m.group(&1).unwrap().role().is_leader());
  }
  m.service_merge_applies(now, &mut stores);
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    assert_eq!(
      witness_count(log),
      1,
      "the leader minted the witness off the record it kept as a follower"
    );
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    m.group(&1).unwrap().abandoned_obligations().is_empty(),
    "the committed witness apply cleared the record"
  );
}

/// THE WITNESS DEBT AT THE TEARDOWN DOOR (#137): target 1, a follower, observed source 2 thawed and
/// holds its record discharged — the only future `ThawDischarged` trigger while the leader cannot
/// observe 2. `remove_group(1)` refuses `OwesThaw` with no step-aside: the observation may be
/// knowledge no other replica can reproduce, and removing the holder would destroy the trigger
/// while every other replica keeps its live obligation and fence forever. Once 1 leads, mints, and
/// the witness applies, the same removal admits.
#[test]
fn a_discharged_holders_removal_refuses_until_the_witness_applies() {
  let (mut m, mut stores) = restored_holder_and_frozen_source();
  let now = Instant::ORIGIN;
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    let d = m.group(&2).unwrap().poll_timeout().unwrap();
    m.handle_timeout(&2, d, log, stable).unwrap();
    drain_storage(&mut m, 2, d, log, stable);
  }
  m.service_merge_applies(now, &mut stores);
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  m.service_merge_applies(now, &mut stores);
  let t = m.group(&1).unwrap();
  assert!(
    t.role().is_follower() && !t.owes_live_thaw() && t.holds_witness_debt(),
    "a follower observer: no live obligation, one witness debt"
  );
  assert_eq!(
    m.remove_group(&1, &mut stores).map(|r| r.is_some()),
    Err(RemoveError::OwesThaw),
    "the witness debt refuses the holder's removal — its record is the only future trigger"
  );
  // 1 leads, mints, and the committed witness retires the debt; the removal admits.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    let d = m.group(&1).unwrap().poll_timeout().unwrap();
    m.handle_timeout(&1, d, log, stable).unwrap();
    drain_storage(&mut m, 1, d, log, stable);
  }
  m.service_merge_applies(now, &mut stores);
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    !m.group(&1).unwrap().holds_witness_debt(),
    "the committed witness apply retired the debt"
  );
  assert_eq!(
    m.remove_group(&1, &mut stores).map(|r| r.is_some()),
    Ok(true),
    "with the witness applied, the same removal admits"
  );
}

/// A POISONED HOLDER'S DEBT FENCES ITS REMOVAL (#138, residual 13): it cannot mint and a peer's
/// witness cannot apply on it, but admitting the removal would delete, with the storage the drivers
/// tear down, a proof no other replica may hold — the healthy unobserving peers would keep live
/// records and raised fences with no witness producer. Refusing wedges only a replica that serves
/// nothing. THE PURGE EXIT still works on it: once the named source is co-hosted and live past the
/// generation, removing the source admits, and its purge reaches every hosted endpoint — the
/// poisoned holder included — so the debt clears and the holder's own removal then admits.
#[test]
fn a_poisoned_holders_witness_debt_fences_its_removal() {
  let (mut m, mut stores, source_key) = target_only_owing(1);
  let now = Instant::ORIGIN;
  m.group_mut(&2).unwrap().note_discharged(&source_key);
  m.group_mut(&2)
    .unwrap()
    .poison(PoisonReason::ReservedShapeGen);
  assert!(
    m.group(&2).unwrap().is_poisoned() && m.group(&2).unwrap().holds_witness_debt(),
    "poisoned, holding a debt it can never pay itself"
  );
  assert_eq!(
    m.remove_group(&2, &mut stores).map(|r| r.is_some()),
    Err(RemoveError::OwesThaw),
    "the debt fences a poisoned holder's removal too — the proof would go with the storage"
  );
  // THE EXIT: the named source comes to be hosted here, live past the generation (founded at 2).
  stores
    .0
    .insert(1, (VecLog::default(), AsyncStable::default()));
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.create_group_founded_at(
      1,
      2,
      single_node_cfg(1),
      now,
      9,
      CountSm::default(),
      1,
      &*log,
      stable,
    )
    .unwrap();
  }
  assert_eq!(
    m.remove_group(&1, &mut stores).map(|r| r.is_some()),
    Ok(true),
    "a source live past the generation is removable — nothing gates it"
  );
  assert!(
    !m.group(&2).unwrap().holds_witness_debt()
      && m.group(&2).unwrap().abandoned_obligations().is_empty(),
    "the purge reached the poisoned holder and cleared its debt"
  );
  assert_eq!(
    m.remove_group(&2, &mut stores).map(|r| r.is_some()),
    Ok(true),
    "with the debt purged, the poisoned holder's removal admits"
  );
}

/// THE WITNESS DEBT AT THE MERGE-SOURCE DOOR (#137): a discharged holder cannot dissolve as a
/// fresh merge's SOURCE — `prepare_merge` refuses `SourceOwesThaw`: a frozen holder cannot propose
/// the witness (proposes are refused while frozen), and the absorb's dissolve would drop the
/// record, the only future trigger. Once the witness applies, the same freeze admits.
#[test]
fn a_discharged_holder_cannot_freeze_as_a_source_until_the_witness_applies() {
  let (mut m, mut stores) = restored_holder_and_frozen_source();
  let now = Instant::ORIGIN;
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    let d = m.group(&2).unwrap().poll_timeout().unwrap();
    m.handle_timeout(&2, d, log, stable).unwrap();
    drain_storage(&mut m, 2, d, log, stable);
  }
  m.service_merge_applies(now, &mut stores);
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  m.service_merge_applies(now, &mut stores);
  assert!(
    m.group(&1).unwrap().holds_witness_debt(),
    "a follower observer: one witness debt"
  );
  // 1 leads (a freeze proposes on the source leader) but has not cranked: the debt stands. The
  // fresh merge's target is group 0 — a merge points down the id order.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    let d = m.group(&1).unwrap().poll_timeout().unwrap();
    m.handle_timeout(&1, d, log, stable).unwrap();
    drain_storage(&mut m, 1, d, log, stable);
    assert!(m.group(&1).unwrap().role().is_leader());
  }
  stores
    .0
    .insert(0, (VecLog::default(), AsyncStable::default()));
  m.create_group(0, 0, single_node_cfg(1), now, 9, CountSm::default())
    .unwrap();
  {
    let (log, stable) = stores.0.get_mut(&0).unwrap();
    let d = m.group(&0).unwrap().poll_timeout().unwrap();
    m.handle_timeout(&0, d, log, stable).unwrap();
    drain_storage(&mut m, 0, d, log, stable);
  }
  let verdict = m.prepare_merge(&1, now, &mut stores, &0);
  assert!(
    matches!(verdict, Some(Err(MergeError::SourceOwesThaw))),
    "the witness debt refuses 1 as a merge source — frozen, it could never mint: {verdict:?}"
  );
  // The crank mints; the committed witness retires the debt; the same freeze admits.
  m.service_merge_applies(now, &mut stores);
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(!m.group(&1).unwrap().holds_witness_debt());
  let verdict = m.prepare_merge(&1, now, &mut stores, &0);
  assert!(
    matches!(verdict, Some(Ok(_))),
    "with the witness applied, the same freeze admits: {verdict:?}"
  );
}

/// THE PURGE EXIT FOR A WITNESS DEBT (#137): source 2 is hosted here and live past the abandoned
/// generation, so `remove_group(2)` admits, and its purge clears every holder's record for 2 —
/// the discharged one included. The holder's own removal then admits.
#[test]
fn a_discharged_record_retires_when_its_source_leaves_the_host() {
  let (mut m, mut stores) = restored_holder_and_frozen_source();
  let now = Instant::ORIGIN;
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    let d = m.group(&2).unwrap().poll_timeout().unwrap();
    m.handle_timeout(&2, d, log, stable).unwrap();
    drain_storage(&mut m, 2, d, log, stable);
  }
  m.service_merge_applies(now, &mut stores);
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  m.service_merge_applies(now, &mut stores);
  assert!(
    m.group(&1).unwrap().holds_witness_debt(),
    "one witness debt"
  );
  assert_eq!(
    m.remove_group(&1, &mut stores).map(|r| r.is_some()),
    Err(RemoveError::OwesThaw),
    "refused while the debt stands"
  );
  assert_eq!(
    m.remove_group(&2, &mut stores).map(|r| r.is_some()),
    Ok(true),
    "the thawed source is removable — it is live past the generation, nothing gates it"
  );
  assert!(
    m.group(&1).unwrap().abandoned_obligations().is_empty(),
    "the purge cleared the discharged record with the departing source"
  );
  assert_eq!(
    m.remove_group(&1, &mut stores).map(|r| r.is_some()),
    Ok(true),
    "and the holder's removal admits"
  );
}

/// A store seam that WITHHOLDS the stores of chosen groups — `stores()` answers `None` for them
/// while floors and lineages stay readable — the crank on which the service cannot reach a
/// holder's stores. Resolution and records delegate to an inner [`LineageStores`].
struct WithheldStores {
  inner: LineageStores,
  withheld: std::collections::BTreeSet<u64>,
}

impl crate::GroupStores<u64, VecLog, AsyncStable> for WithheldStores {
  fn stores(&mut self, group: &u64) -> Option<(&mut VecLog, &mut AsyncStable)> {
    if self.withheld.contains(group) {
      return None;
    }
    crate::GroupStores::stores(&mut self.inner, group)
  }
}

impl crate::FloorStore<u64> for WithheldStores {
  fn floor(&self, gid: &u64) -> u64 {
    self.inner.floor(gid)
  }

  fn lineage(&self, gid: &u64) -> u64 {
    self.inner.lineage(gid)
  }
}

/// THE LEADER'S WITNESS DEBT (#137): a covered-dead-end holder — target 2 leads, source 1 is
/// unhosted, the record install-covered — observes a GLOBAL proof (the persisted lineage mirror
/// past the generation; the terminal floor alike) and marks the record DISCHARGED before it
/// appends the witness. So while the witness is appended but not yet committed the record reads as
/// a WITNESS DEBT and `remove_group(2)` refuses `OwesThaw`. A record left LIVE there reads as a
/// removable covered dead end instead, and if this host held the sole reproducible proof, removing
/// it would leave every other replica with a live record, a fence, and no future trigger. Once the
/// witness applies, the removal admits.
#[test]
fn a_leading_holders_observation_is_a_witness_debt_until_its_witness_applies() {
  let now = Instant::ORIGIN;
  // The persisted lineage mirror as the proof.
  let (mut m, inner, _) = target_only_owing(1);
  m.group_mut(&2)
    .unwrap()
    .note_abort_covered(Index::new(1), Cover::Install);
  let mut stores = LineageStores {
    inner,
    floors: std::collections::BTreeMap::new(),
    lineages: std::collections::BTreeMap::from([(1u64, 2u64)]),
  };
  m.service_merge_applies(now, &mut stores);
  assert_eq!(
    witness_count(&stores.inner.0.get(&2).unwrap().0),
    1,
    "the leader appended the witness"
  );
  assert_eq!(
    m.remove_group(&2, &mut stores).map(|r| r.is_some()),
    Err(RemoveError::OwesThaw),
    "the appended-but-uncommitted witness is a debt — no step-aside for it"
  );
  let t = m.group(&2).unwrap();
  assert!(
    !t.owes_live_thaw() && t.holds_witness_debt(),
    "the leader marked the debt BEFORE appending the witness"
  );
  {
    let (log, stable) = stores.inner.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    m.group(&2).unwrap().abandoned_obligations().is_empty(),
    "the committed witness apply retired the debt"
  );
  assert_eq!(
    m.remove_group(&2, &mut stores).map(|r| r.is_some()),
    Ok(true),
    "and the removal admits"
  );
  // The terminal floor as the proof: the same debt.
  let (mut m, mut stores, _) = target_only_owing(1);
  m.group_mut(&2)
    .unwrap()
    .note_abort_covered(Index::new(1), Cover::Install);
  stores.1.insert(1);
  m.service_merge_applies(now, &mut stores);
  assert_eq!(witness_count(&stores.0.get(&2).unwrap().0), 1);
  assert_eq!(
    m.remove_group(&2, &mut stores).map(|r| r.is_some()),
    Err(RemoveError::OwesThaw),
    "the terminal-floor proof is a debt until its witness applies"
  );
}

/// THE LEADER'S WITNESS DEBT WITH ITS STORES WITHHELD (#137): the same holder observes the proof on
/// a crank where the service cannot reach its stores — nothing can be appended — and the record is
/// marked DISCHARGED all the same: a debt, the removal refused, with nothing in the log. The next
/// crank with the stores back appends the witness exactly once, off the latched arm, and its apply
/// retires the debt.
#[test]
fn a_leading_holder_without_stores_still_takes_the_witness_debt() {
  let now = Instant::ORIGIN;
  let (mut m, inner, _) = target_only_owing(1);
  m.group_mut(&2)
    .unwrap()
    .note_abort_covered(Index::new(1), Cover::Install);
  let mut stores = WithheldStores {
    inner: LineageStores {
      inner,
      floors: std::collections::BTreeMap::new(),
      lineages: std::collections::BTreeMap::from([(1u64, 2u64)]),
    },
    withheld: std::collections::BTreeSet::from([2u64]),
  };
  m.service_merge_applies(now, &mut stores);
  assert_eq!(
    witness_count(&stores.inner.inner.0.get(&2).unwrap().0),
    0,
    "nothing could be appended without the stores"
  );
  let t = m.group(&2).unwrap();
  assert!(
    !t.owes_live_thaw() && t.holds_witness_debt(),
    "yet the observation is recorded as a debt"
  );
  assert_eq!(
    m.remove_group(&2, &mut stores).map(|r| r.is_some()),
    Err(RemoveError::OwesThaw),
    "the unappended witness is a debt — the removal refuses"
  );
  // The stores return: exactly one witness is appended, off the latched arm.
  stores.withheld.clear();
  m.service_merge_applies(now, &mut stores);
  m.service_merge_applies(now, &mut stores);
  assert_eq!(
    witness_count(&stores.inner.inner.0.get(&2).unwrap().0),
    1,
    "the next crank with stores appended the witness, once"
  );
  {
    let (log, stable) = stores.inner.inner.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    m.group(&2).unwrap().abandoned_obligations().is_empty(),
    "the committed witness apply retired the debt"
  );
  assert_eq!(
    m.remove_group(&2, &mut stores).map(|r| r.is_some()),
    Ok(true),
    "and the removal admits"
  );
}

/// A DISCHARGED FOLLOWER KEEPS FENCING ITS ENTRY UNTIL THE WITNESS APPLIES (#137). Target 1, a
/// follower, observes source 2 thawed and retires its record as discharged — and the record still
/// FENCES the abort entry it re-derives from: while a non-observer leads, this record is the only
/// future witness trigger, and a threshold capture past the entry followed by a crash would lose
/// it for good, leaving every replica that never observed 2 holding its live obligation forever.
/// So the capture at `applied` is refused; the witness applies; the fence lifts and the capture
/// proceeds.
#[test]
fn a_discharged_follower_keeps_fencing_its_entry_until_the_witness_applies() {
  let (mut m, mut stores) =
    restored_pair_with_holder_cfg(single_node_cfg(1).with_snapshot_threshold(1));
  let now = Instant::ORIGIN;
  let source = gid_key(2);
  // 2 leads and thaws off 1's record; 1 (a follower) observes and retires the record, discharged.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    let d = m.group(&2).unwrap().poll_timeout().unwrap();
    m.handle_timeout(&2, d, log, stable).unwrap();
    drain_storage(&mut m, 2, d, log, stable);
  }
  m.service_merge_applies(now, &mut stores);
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  m.service_merge_applies(now, &mut stores);
  let t = m.group(&1).unwrap();
  assert!(
    t.role().is_follower() && t.abandoned_record(&source).is_some_and(|r| r.discharged),
    "a follower observer: discharged, kept"
  );
  assert!(
    t.abort_relay_fences(t.applied_index()),
    "and still fencing its abort entry — the only future witness trigger"
  );
  // The leader replicates a command above the abort: applied(2) - first_index(1) reaches the
  // threshold, and the capture is REFUSED by the discharged record's fence.
  let cmd = {
    let mut buf = Vec::new();
    Bytes::from_static(b"c").encode(&mut buf);
    Bytes::from(buf)
  };
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.handle_message(
      &1,
      now,
      log,
      stable,
      9u64,
      Message::AppendEntries(crate::AppendEntries::new(
        Term::new(1),
        9u64,
        Index::new(1),
        Term::new(1),
        std::vec![crate::Entry::new(
          Term::new(1),
          Index::new(2),
          crate::EntryKind::Normal,
          cmd.clone(),
        )],
        Index::new(2),
      )),
    )
    .unwrap();
    drain_storage(&mut m, 1, now, log, stable);
    drain_storage(&mut m, 1, now, log, stable);
    assert_eq!(m.group(&1).unwrap().applied_index(), Index::new(2));
    assert!(
      stable.snapshot().is_none(),
      "the threshold capture is refused: compacting the entry would risk the trigger"
    );
  }
  // The witness arrives by replication; its apply clears the record, and the capture proceeds.
  let witness = {
    let mut buf = Vec::new();
    crate::wire::encode_thaw_discharged_payload(
      &ThawDischargedPayload::new(source.clone(), 1),
      &mut buf,
    );
    Bytes::from(buf)
  };
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.handle_message(
      &1,
      now,
      log,
      stable,
      9u64,
      Message::AppendEntries(crate::AppendEntries::new(
        Term::new(1),
        9u64,
        Index::new(2),
        Term::new(1),
        std::vec![crate::Entry::new(
          Term::new(1),
          Index::new(3),
          crate::EntryKind::ThawDischarged,
          witness,
        )],
        Index::new(3),
      )),
    )
    .unwrap();
    drain_storage(&mut m, 1, now, log, stable);
    drain_storage(&mut m, 1, now, log, stable);
    assert!(
      m.group(&1).unwrap().abandoned_obligations().is_empty(),
      "the committed witness apply cleared the record"
    );
    assert_eq!(
      stable.snapshot().map(|(meta, _)| meta.last_index()),
      Some(Index::new(3)),
      "the fence lifted with the record — the capture landed"
    );
  }
}

/// THE CRASH VARIANT (#137): a follower observer retires its record as discharged, then restarts
/// before any witness exists. Its abort entry is still in the log — the fence kept it there — so
/// the restart re-derives an UNDISCHARGED record; the restarted holder observes the thawed source
/// again, retires it again, and when it leads it mints the witness that clears every replica's
/// record. Had the entry been compacted, the restart would derive nothing and this holder could
/// never mint.
#[test]
fn a_restarted_discharged_follower_re_derives_its_record_and_mints_when_leading() {
  let (mut m, mut stores) = restored_holder_and_frozen_source();
  let now = Instant::ORIGIN;
  let source = gid_key(2);
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    let d = m.group(&2).unwrap().poll_timeout().unwrap();
    m.handle_timeout(&2, d, log, stable).unwrap();
    drain_storage(&mut m, 2, d, log, stable);
  }
  m.service_merge_applies(now, &mut stores);
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  m.service_merge_applies(now, &mut stores);
  assert!(
    m.group(&1)
      .unwrap()
      .abandoned_record(&source)
      .is_some_and(|r| r.discharged),
    "the follower observed and retired the record, discharged"
  );
  // THE RESTART: both groups come back from their preserved stores.
  drop(m);
  let mut m2: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m2.restore_group_unchecked(
      1,
      single_node_cfg(1),
      now,
      7,
      CountSm::default(),
      2,
      log,
      stable,
    )
    .unwrap();
  }
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m2.restore_group_unchecked(
      2,
      single_node_cfg(1),
      now,
      8,
      CountSm::default(),
      2,
      log,
      stable,
    )
    .unwrap();
  }
  let t = m2.group(&1).unwrap();
  assert!(
    t.owes_live_thaw() && t.abandoned_record(&source).is_some_and(|r| !r.discharged),
    "the kept entry re-derived an UNDISCHARGED record — the volatile mark is gone, the trigger is not"
  );
  assert_eq!(
    m2.group(&2).unwrap().shape_gen(),
    2,
    "2 restarted thawed, past the generation"
  );
  // It observes again (a follower: discharged again), then leads and mints.
  m2.service_merge_applies(now, &mut stores);
  assert!(
    m2.group(&1)
      .unwrap()
      .abandoned_record(&source)
      .is_some_and(|r| r.discharged),
    "re-observed: discharged again, still held"
  );
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    let d = m2.group(&1).unwrap().poll_timeout().unwrap();
    m2.handle_timeout(&1, d, log, stable).unwrap();
    drain_storage(&mut m2, 1, d, log, stable);
    assert!(m2.group(&1).unwrap().role().is_leader());
  }
  m2.service_merge_applies(now, &mut stores);
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    assert_eq!(
      witness_count(log),
      1,
      "leading, it minted the witness off the re-derived record"
    );
    drain_storage(&mut m2, 1, now, log, stable);
  }
  assert!(
    m2.group(&1).unwrap().abandoned_obligations().is_empty(),
    "the committed witness apply cleared the record"
  );
}

/// A POISONED HUSK KEEPS ITS HOLDER'S RECORD AND THE ESCAPE OPEN (#138, residual 7): source 2 is
/// hosted, frozen at the abandoned generation, at the terminal `MERGED_FLOOR` — and poisoned. The
/// husk dissolve skips a poisoned husk and the drive answers `Poisoned`, so retiring the record off
/// the terminal floor would leave a poisoned frozen source nobody owes: unremovable, with no
/// escape. Fenced off poison, the terminal floor retires nothing here, the record stays live, and
/// `remove_group(2)` admits through the owed-source escape, whose purge clears the record.
#[test]
fn a_poisoned_husk_keeps_its_holders_record_and_the_escape_open() {
  let (mut m, mut stores) = leading_holder_with(admit_frozen_source);
  let now = Instant::ORIGIN;
  m.group_mut(&2)
    .unwrap()
    .poison(PoisonReason::ReservedShapeGen);
  stores.1.insert(2);
  for _ in 0..2 {
    m.service_merge_applies(now, &mut stores);
  }
  assert!(m.contains_group(&2), "the dissolve skips a poisoned husk");
  assert!(
    m.group(&1).unwrap().owes_live_thaw()
      && m
        .group(&1)
        .unwrap()
        .abandoned_record(&gid_key(2))
        .is_some_and(|r| !r.discharged),
    "the terminal floor is no proof off a poisoned source — the record stays live"
  );
  assert_eq!(
    witness_count(&stores.0.get(&1).unwrap().0),
    0,
    "and nothing is witnessed off it"
  );
  assert_eq!(
    m.remove_group(&2, &mut stores).map(|r| r.is_some()),
    Ok(true),
    "the live record keeps 2 an OWED source — its removal admits despite the freeze"
  );
  assert!(
    m.group(&1).unwrap().abandoned_obligations().is_empty(),
    "the escape's purge cleared the record"
  );
}

/// The GENERATION-EXACT boundary of that escape: a STALE obligation must NOT bypass the frozen gate.
/// Source 1 freezes into 2 at gen 1 and 2 ABORTS (recording `abandoned[1]` for gen 1); the thaw pass
/// then DELIVERS — 1 unfreezes and advances to gen 2 — but the discharge pass is deliberately NOT
/// run, so 2's obligation lingers naming the now-SPENT gen 1. 1 is then re-frozen for a FRESH merge
/// into 3 (gen 3), whose target has not yet parked. The escape is generation-exact (obligation gen 1
/// ≠ live gen 3), so the stale record suppresses NOTHING and leg 2 refuses `Frozen`, leaving the
/// merge intact. An id-only escape admits `remove_group(&1)` — the lingering `abandoned[1]`
/// suppressing `Frozen` — tearing down the newly-frozen source and stranding 3's forming park.
#[test]
fn a_stale_obligation_does_not_bypass_the_frozen_gate() {
  let (mut m, mut stores) = merge_host_triple(4, 3, 2);
  let now = Instant::ORIGIN;
  // 1 freezes into 2 at gen 1, then 2 aborts and APPLIES: 2 owes 1 a gen-1 thaw, 1 is still frozen.
  {
    m.prepare_merge(&3, now, &mut stores, &2).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&3).unwrap();
    drain_storage(&mut m, 3, now, log, stable);
  }
  assert_eq!(
    m.group(&3).unwrap().shape_gen(),
    1,
    "1 froze into 2 at gen 1"
  );
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.rollback_merge(&2, now, log, stable, &3).unwrap().unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    m.group(&2).unwrap().owes_live_thaw(),
    "2 owes 1 the gen-1 thaw"
  );

  // Drive the thaw so it DELIVERS — 1 unfreezes and advances to gen 2 — but do NOT run the discharge
  // pass, so 2's obligation lingers naming the now-SPENT gen 1.
  m.service_merge_applies(now, &mut stores);
  {
    let (log, stable) = stores.0.get_mut(&3).unwrap();
    drain_storage(&mut m, 3, now, log, stable);
  }
  assert!(!m.group(&3).unwrap().is_frozen(), "the thaw unfroze 1");
  assert_eq!(
    m.group(&3).unwrap().shape_gen(),
    2,
    "1 advanced past the spent freeze generation"
  );
  assert!(
    m.group(&2).unwrap().owes_live_thaw(),
    "the discharge pass has NOT run — 2 still carries the spent gen-1 obligation"
  );

  // Re-freeze 1 for a FRESH merge into 3 at gen 3. Its new target 3 has not parked yet, so leg 2
  // (Frozen) is the only strand-preventing refusal available in this window.
  {
    m.prepare_merge(&3, now, &mut stores, &1).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&3).unwrap();
    drain_storage(&mut m, 3, now, log, stable);
  }
  assert!(
    m.group(&3).unwrap().is_frozen(),
    "1 is a fresh frozen source for 3"
  );
  assert_eq!(
    m.group(&3).unwrap().shape_gen(),
    3,
    "the fresh freeze minted gen 3"
  );

  // THE GATE, generation-exact: 2's obligation names gen 1, 1 is live at gen 3 — the stale record
  // suppresses NOTHING, so leg 2 refuses the newly-frozen source (an id-only escape would admit it).
  assert!(
    matches!(m.remove_group(&3, &mut stores), Err(RemoveError::Frozen)),
    "a freshly-frozen source is refused Frozen — a stale obligation cannot bypass the gate"
  );
  // NO SIDE EFFECTS: the refusal tore nothing down — 1 is still the frozen source for its new merge.
  assert!(
    m.contains_group(&3) && m.group(&3).unwrap().is_frozen(),
    "the refused removal left 1 hosted and frozen for its 3-bound merge"
  );
  // And 2's spent obligation is untouched — the gate is a pure read, fabricating and clearing nothing.
  assert!(
    m.group(&2).unwrap().owes_live_thaw(),
    "the refusal left 2's lingering obligation exactly as it was"
  );
}

/// The container refuses a conf change on a target another hosted source's APPLIED freeze CLAIMS —
/// moving the target's voters off the frozen source's hosts would strand the source (`commit_merge`
/// then refuses `VoterSetsDiffer`, `rollback_merge` `SourceMissing`, no release valve). The
/// endpoint's own fence cannot see the cross-group claim; the container surfaces the same
/// `MergeInFlight` class. Once the claim is released (the merge aborted and the thaw discharged) the
/// same conf change admits.
#[test]
fn conf_change_on_a_claimed_merge_target_is_refused() {
  let (mut m, mut stores) = merge_host(2, 3);
  let now = Instant::ORIGIN;
  // 1 freezes into 2 (1 frozen, claim = 2). 2 itself is clean — only the cross-group claim fences it.
  {
    m.prepare_merge(&2, now, &mut stores, &1).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(m.group(&2).unwrap().is_frozen());
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    assert!(
      matches!(
        m.propose_conf_change_v2(
          &1,
          now,
          log,
          stable,
          crate::ConfChange::new(crate::ConfChangeType::AddNode, 9u64, Bytes::new()).into_v2(),
        ),
        Some(Err(crate::ProposeError::MergeInFlight))
      ),
      "a claimed merge target refuses a voter change"
    );
  }
  // Release the claim: abort the merge, then drive and discharge the source's thaw.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.rollback_merge(&1, now, log, stable, &2).unwrap().unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  m.service_merge_applies(now, &mut stores);
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    !m.group(&2).unwrap().is_frozen(),
    "1 thawed — it no longer claims 2"
  );
  m.service_merge_applies(now, &mut stores);
  // The observing leader deferred its clear to the witness — apply it on the holder.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    !m.group(&1).unwrap().owes_live_thaw(),
    "2's obligation discharged"
  );
  // With the claim gone and no obligation, the same conf change now ADMITS.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    assert!(
      matches!(
        m.propose_conf_change_v2(
          &1,
          now,
          log,
          stable,
          crate::ConfChange::new(crate::ConfChangeType::AddNode, 9u64, Bytes::new()).into_v2(),
        ),
        Some(Ok(_))
      ),
      "the released target admits the voter change"
    );
  }
}

/// The claimed-target fence's exemption holds for a COVERED obligation (#132). Target 1 aborted 2's
/// merge and its install then crossed the abort entry; the record is kept (covered) while 2 is
/// still frozen here with its claim on 1 — so a voter change on 1 is exactly as safe as before the
/// install: 2 thaws off the retained obligation, not off 1's voters. A record dropped at the
/// install re-fences the change (the claim reads unresolved again) with nothing left on 1 to
/// resolve it.
#[test]
fn a_covered_obligation_keeps_the_claimed_target_conf_change_exempt() {
  let (mut m, mut stores) = restored_holder_and_frozen_source();
  let now = Instant::ORIGIN;
  install_past_the_abort(&mut m, &mut stores);
  // 1 elects itself (single voter) so it can propose; 2 still claims it, frozen.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    let d = m.group(&1).unwrap().poll_timeout().unwrap();
    m.handle_timeout(&1, d, log, stable).unwrap();
    drain_storage(&mut m, 1, d, log, stable);
    assert!(m.group(&1).unwrap().role().is_leader());
  }
  assert!(m.group(&2).unwrap().is_frozen(), "2 still claims 1");
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    assert!(
      matches!(
        m.propose_conf_change_v2(
          &1,
          now,
          log,
          stable,
          crate::ConfChange::new(crate::ConfChangeType::AddNode, 9u64, Bytes::new()).into_v2(),
        ),
        Some(Ok(_))
      ),
      "the retained obligation exempts the claim: 2 thaws off it, not off 1's voters"
    );
  }
  assert!(
    m.group(&1)
      .unwrap()
      .abandoned_record(&gid_key(2))
      .is_some_and(|r| r.is_covered() && !r.discharged),
    "the exemption rode the covered, live record, which the election and the change left standing"
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
    m.remove_group_inner(&2).is_some(),
    "the source replica is dropped through the ungated inner path"
  );
  assert!(
    !m.contains_group(&2),
    "no local source endpoint contributes a freeze signal"
  );
  assert!(
    m.group(&1).unwrap().pending_merge().is_some(),
    "2 is still parked naming the now-absent source 1"
  );
  // Leg 4 alone catches it: the park names 1, so removing 1 is SpokenFor even with no local freeze.
  assert!(
    matches!(m.remove_group(&2, &mut stores), Err(RemoveError::SpokenFor)),
    "a source a live park names is refused SpokenFor, never a silent no-op"
  );
}

/// Leg 5, the CLAIMED-TARGET pre-park window (the last leg of the participant lattice). Source 1
/// freezes into 2 and APPLIES it (1 is `frozen_for` 2), but 2 never proposes its `CommitMerge` — so
/// 2 has no `pending_merge` (`MergeParked` misses) and no park names 2 (`SpokenFor` reads the mirror
/// direction and misses). Only leg 5's mirror scan catches the claim: `remove_group(&2)` refuses
/// `Claimed`, touching nothing. Admitting it strands 1 frozen for a target that no longer exists —
/// 1's absorb AND its abort both ride 2's log, so neither can be proposed, and 1's own removal then
/// refuses `Frozen` (it owes no thaw). THE ESCAPE: roll the merge back on 2 (still hosted pre-park),
/// which thaws 1 and discharges 2's obligation, after which the SAME removal admits. A
/// non-participant (3) is unchanged.
#[test]
fn teardown_refuses_a_claimed_target_before_the_park() {
  let (mut m, mut stores) = merge_host_triple(3, 2, 4);
  let now = Instant::ORIGIN;
  // 1 freezes into 2 and APPLIES — 1 is frozen_for 2 — but 2 never commits, so it never parks.
  {
    m.prepare_merge(&2, now, &mut stores, &1).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    m.group(&2).unwrap().is_frozen(),
    "1 is an applied frozen source claiming 2"
  );
  assert!(
    m.group(&1).unwrap().pending_merge().is_none(),
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
    matches!(m.remove_group(&1, &mut stores), Err(RemoveError::Claimed)),
    "a target a frozen source claims is refused Claimed before it parks"
  );
  // NO SIDE EFFECTS: the choreography is intact — the source is not stranded.
  assert!(
    m.contains_group(&1) && m.group(&2).unwrap().is_frozen(),
    "the refused removal left 2 hosted and 1 frozen for it"
  );
  // NEGATIVE PIN: the SOURCE side is still refused `Frozen` (its own role), not `Claimed`.
  assert!(
    matches!(m.remove_group(&2, &mut stores), Err(RemoveError::Frozen)),
    "the frozen source itself is refused Frozen"
  );

  // THE ESCAPE: roll the merge back on 2 (hosted pre-park) — 1 thaws, 2's obligation discharges.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.rollback_merge(&1, now, log, stable, &2).unwrap().unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  m.service_merge_applies(now, &mut stores);
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(!m.group(&2).unwrap().is_frozen(), "the rollback thawed 1");
  m.service_merge_applies(now, &mut stores);
  // The observing leader deferred its clear to the witness — apply it on the holder.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    !m.group(&1).unwrap().owes_live_thaw(),
    "the observed thaw discharged 2's obligation"
  );
  // SELF-CLEARING PIN: with the claiming merge rolled back, the SAME removal now admits.
  assert!(
    m.remove_group(&1, &mut stores).unwrap().is_some(),
    "once the claim is rolled back, the former target tears down as a clean group"
  );
}

/// Leg 5's APPEND-PENDING window: the claim is refused even before the freeze applies. Source 1's
/// `PrepareMerge` is APPENDED (its append-observed lease kill is live) but NOT yet folded, so its
/// target claim is still undecoded in-memory (`frozen_for` is `None`) — the applied leg cannot see
/// it. The gate DECODES the claim from 1's own unapplied log suffix and refuses `Claimed`; an
/// applied-only gate admits `remove_group(&2)` here, stranding 1 identically once the freeze applies.
/// A DIFFERENT target (3) still tears down — the decode reads the exact claim (2), never
/// over-refusing.
#[test]
fn teardown_refuses_a_claimed_target_from_the_append_pending_freeze() {
  let (mut m, mut stores) = merge_host_triple(3, 2, 4);
  let now = Instant::ORIGIN;
  // 1's PrepareMerge is APPENDED but deliberately NOT drained — freeze-pending, claim undecoded.
  {
    m.prepare_merge(&2, now, &mut stores, &1).unwrap().unwrap();
  }
  assert!(
    m.group(&2).unwrap().merge_freeze_active() && !m.group(&2).unwrap().is_frozen(),
    "1 observes its freeze at append but has not applied — the claim is undecoded in-memory"
  );

  // NEGATIVE PIN: the decode reads the EXACT claim — a different target is not over-refused.
  assert!(
    m.remove_group(&3, &mut stores).unwrap().is_some(),
    "the pending freeze claims 2, not 3 — removing 3 is unchanged"
  );

  // THE GATE, leg 5 pending sub-case: the append-pending claim is decoded from 1's log suffix.
  assert!(
    matches!(m.remove_group(&1, &mut stores), Err(RemoveError::Claimed)),
    "an append-pending freeze's decoded claim refuses the target before the freeze folds"
  );
  assert!(
    m.contains_group(&1),
    "the refused removal left the claimed target hosted"
  );

  // CONTINUITY: once the freeze APPLIES, the claim is refused through the applied leg (frozen_for) —
  // the window is closed end to end.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(m.group(&2).unwrap().is_frozen(), "the freeze folded");
  assert!(
    matches!(m.remove_group(&1, &mut stores), Err(RemoveError::Claimed)),
    "the applied claim keeps refusing the target — no gap between the two windows"
  );
}

/// The PROPOSE-TIME twin of teardown leg 5, at the freeze door: a group another hosted source's
/// APPLIED freeze claims as its TARGET must not freeze as a fresh merge's SOURCE. 1 freezes into 2
/// (1 is `frozen_for` 2); 2 then tries to freeze into 3. `prepare_merge(2 -> 3)` is refused
/// `SourceClaimedAsTarget`, appending nothing. Admitting it lets a later absorb dissolve 2, after
/// which 1's release verbs (`commit_merge`, `rollback_merge`) both ride 2's dead log: `None`
/// forever, 1 stranded frozen with no release valve. THE RELEASE (abort path): rolling 1's merge back on
/// 2 thaws 1 — clearing its claim — and discharges 2's obligation, after which the SAME freeze
/// admits.
#[test]
fn a_claimed_merge_target_cannot_freeze_as_a_source() {
  let (mut m, mut stores) = merge_host_triple(4, 3, 2);
  let now = Instant::ORIGIN;
  // 1 freezes into 2 and APPLIES — 1 is frozen_for 2.
  {
    m.prepare_merge(&3, now, &mut stores, &2).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&3).unwrap();
    drain_storage(&mut m, 3, now, log, stable);
  }
  assert!(m.group(&3).unwrap().is_frozen(), "1 claims 2");

  // THE GATE: 2 is a claimed target — it must not dissolve as a source while 1's claim stands.
  assert!(
    matches!(
      m.prepare_merge(&2, now, &mut stores, &1),
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
    m.rollback_merge(&2, now, log, stable, &3).unwrap().unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  m.service_merge_applies(now, &mut stores);
  {
    let (log, stable) = stores.0.get_mut(&3).unwrap();
    drain_storage(&mut m, 3, now, log, stable);
  }
  assert!(!m.group(&3).unwrap().is_frozen(), "the rollback thawed 1");
  m.service_merge_applies(now, &mut stores);
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    !m.group(&2).unwrap().owes_live_thaw(),
    "the observed thaw discharged 2's obligation"
  );

  // SELF-CLEARING PIN: with the claim discharged, the SAME freeze now admits.
  {
    assert!(
      m.prepare_merge(&2, now, &mut stores, &1).unwrap().is_ok(),
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
/// see it. The claim is decoded from 1's unapplied suffix and `prepare_merge(2 -> 3)` refused;
/// once the freeze APPLIES the refusal continues through the applied leg — no gap between the
/// windows, which an applied-only gate leaves open (the strand forms identically once the freeze
/// folds). The decode is EXACT: candidate 3, which 1's claim does not name, freezes
/// toward 2 unrefused (fan-in onto one target is the designed `abandoned` fan-in).
#[test]
fn an_append_pending_claim_refuses_source_role_too() {
  // Four single-voter groups: 4 claims 2 (append-pending), 2 attempts a source-role freeze into the
  // lower 1 (refused), and candidate 3 — which 4's claim does NOT name — freezes into 2 unrefused.
  // Every claim is direction-valid, so the SourceClaimedAsTarget gate is what fires.
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let mut stores = MapStores(
    std::collections::BTreeMap::new(),
    std::collections::BTreeSet::new(),
  );
  let now = Instant::ORIGIN;
  for gid in [1u64, 2, 3, 4] {
    stores
      .0
      .insert(gid, (VecLog::default(), AsyncStable::default()));
    m.create_group(gid, 0, single_node_cfg(1), now, 7, CountSm::default())
      .unwrap();
    let (log, stable) = stores.0.get_mut(&gid).unwrap();
    let d = m.group(&gid).unwrap().poll_timeout().unwrap();
    m.handle_timeout(&gid, d, log, stable).unwrap();
    drain_storage(&mut m, gid, d, log, stable);
    assert!(m.group(&gid).unwrap().role().is_leader());
  }
  while m.poll_message().is_some() {}
  while m.poll_event().is_some() {}
  // 4's PrepareMerge (into 2) is APPENDED but deliberately NOT drained — freeze-pending, claim undecoded.
  m.prepare_merge(&4, now, &mut stores, &2).unwrap().unwrap();
  assert!(
    m.group(&4).unwrap().merge_freeze_active() && !m.group(&4).unwrap().is_frozen(),
    "4 observes its freeze at append but has not applied"
  );

  // THE GATE, pending sub-case: 2 is refused source-role (into the lower 1) off the decoded suffix claim.
  assert!(
    matches!(
      m.prepare_merge(&2, now, &mut stores, &1),
      Some(Err(MergeError::SourceClaimedAsTarget))
    ),
    "an append-pending claim refuses the target source-role before the freeze folds"
  );

  // CONTINUITY: once the freeze APPLIES, the applied leg (frozen_for) keeps refusing.
  {
    let (log, stable) = stores.0.get_mut(&4).unwrap();
    drain_storage(&mut m, 4, now, log, stable);
  }
  assert!(m.group(&4).unwrap().is_frozen(), "the freeze folded");
  assert!(
    matches!(
      m.prepare_merge(&2, now, &mut stores, &1),
      Some(Err(MergeError::SourceClaimedAsTarget))
    ),
    "the applied claim keeps refusing source-role — no gap between the two windows"
  );

  // EXACTNESS: the claim names 2, not 3 — candidate 3 freezes toward 2 (a fan-in) unrefused.
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
  m.prepare_merge(&2, now, &mut stores, &1).unwrap().unwrap();
  // The claimant's suffix goes COLD: the scan for candidate 3 cannot rule the claim out.
  stores.0.get_mut(&2).unwrap().0.return_cold_on_read();
  assert!(
    matches!(
      m.prepare_merge(&3, now, &mut stores, &1),
      Some(Err(MergeError::SourceClaimedAsTarget))
    ),
    "an unreadable pending claim refuses fail-closed — even a candidate it may not name"
  );
  // Warm again, the decode is exact: 1's claim names 2, so candidate 3 admits.
  stores.0.get_mut(&2).unwrap().0.clear_cold_on_read();
  assert!(
    m.prepare_merge(&3, now, &mut stores, &1).unwrap().is_ok(),
    "the same propose admits once the claim is readable — the refusal was the fail-closed arm"
  );
}

/// LEG beta (liveness, the residual window the propose doors cannot fully close): an abort
/// committed on 2 BELOW its own freeze materializes `abandoned[1]` only after the freeze already
/// landed — the freeze fold is an unguarded max, so 2 freezes for 3 while carrying the fresh
/// obligation. The colocated form is now DOOR-REFUSED (`SourceClaimedAsTarget`, asserted below);
/// the window survives cross-host, where the proposing 2-leader's local replica of 1 has not
/// observed 1's freeze — reproduced here past the door. The absorb is HELD; the thaw pass (which
/// does NOT skip the frozen holder 2) discharges 1 first, and only then is 2 absorbed into 3. A
/// Resolve arm that dissolved 2 with the live obligation would strand 1 frozen forever.
#[test]
fn a_late_obligation_holds_the_absorb_until_the_thaw_discharges() {
  let (mut m, mut stores) = merge_host_triple(4, 3, 2);
  let now = Instant::ORIGIN;
  // 1 freezes into 2 (1 frozen, claim = 2).
  {
    m.prepare_merge(&3, now, &mut stores, &2).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&3).unwrap();
    drain_storage(&mut m, 3, now, log, stable);
  }
  assert!(m.group(&3).unwrap().is_frozen());
  // 2 aborts the 1 -> 2 merge but the abort is NOT drained — appended+committed below where 2's own
  // freeze will land. `owes_live_thaw` reads APPLIED state, so it is still false here: the prepare
  // gate below cannot see the obligation.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.rollback_merge(&2, now, log, stable, &3).unwrap().unwrap();
  }
  assert!(
    !m.group(&2).unwrap().owes_live_thaw(),
    "the abort has not applied — no obligation yet"
  );
  // The freeze DOOR now refuses this colocated shape outright: 1's applied claim on 2 is locally
  // visible, so `prepare_merge(2 -> 3)` is `SourceClaimedAsTarget` — the window no longer opens
  // through the propose here.
  assert!(
    matches!(
      m.prepare_merge(&2, now, &mut stores, &1),
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
    Data::encode(&1u64, &mut tbytes);
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
    m.group(&2).unwrap().owes_live_thaw(),
    "the abort applied — 2 now owes 1 a thaw"
  );
  assert!(m.group(&2).unwrap().is_frozen(), "and 2 is frozen for 3");
  // 3 commits the absorb of 2 and parks; seal 3's abort window.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.commit_merge(&1, now, log, stable, &2).unwrap().unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(m.group(&1).unwrap().pending_merge().is_some(), "3 parked");
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
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
    let (log, stable) = stores.0.get_mut(&3).unwrap();
    drain_storage(&mut m, 3, now, log, stable);
  }
  assert!(
    !m.group(&3).unwrap().is_frozen(),
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
    !m.group(&2).unwrap().owes_live_thaw(),
    "2's obligation discharged on the observed advance"
  );
  let done = m.service_merge_applies(now, &mut stores);
  assert_eq!(
    done,
    std::vec![MergeResolution::Merged {
      source: 2,
      target: 1
    }],
    "with the obligation cleared, 2 is finally absorbed into 3"
  );
  assert!(!m.contains_group(&2), "2 dissolved");
  assert!(
    !m.group(&3).unwrap().is_frozen(),
    "and 1 stayed thawed — never stranded"
  );
  assert_eq!(
    m.group(&1).unwrap().state_machine().count(),
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
    m.prepare_merge(&2, now, &mut stores, &1).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(m.group(&2).unwrap().is_frozen(), "1 is a frozen source");
  // The catalog floors 1 terminally — its merge resolved elsewhere while no park formed here. 1 is now
  // a husk; no live park names it. Pre-mechanism it stays forever.
  stores.1.insert(2);
  let resolutions = m.service_merge_applies(now, &mut stores);
  assert_eq!(
    resolutions,
    std::vec![MergeResolution::Retired { source: 2 }],
    "the frozen husk at the terminal floor dissolved locally"
  );
  assert!(!m.contains_group(&2), "the husk is gone");
  assert_eq!(
    stores.floor(&2),
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
    floors: std::collections::BTreeMap::from([(2u64, u64::MAX - 1)]),
    lineages: std::collections::BTreeMap::new(),
  };
  {
    m.prepare_merge(&2, now, &mut stores, &1).unwrap().unwrap();
    let (log, stable) = stores.inner.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(m.group(&2).unwrap().is_frozen(), "1 is a frozen source");
  assert_eq!(stores.floor(&2), u64::MAX - 1);
  let resolutions = m.service_merge_applies(now, &mut stores);
  assert!(
    resolutions.is_empty(),
    "only the EXACT terminal floor dissolves a husk — a floor one below does not"
  );
  assert!(
    m.contains_group(&2),
    "the frozen source is untouched below the terminal"
  );
}

/// THE DEAD-TARGET THAW FAIL-SAFE BELT: the mint refuses while any hosted target's parked commit
/// still names the source. A live park means an absorb of this source may still be resolving locally,
/// so moving the counter underneath it is refused (`SourceAbsorbParked`). `freeze_and_park` leaves
/// source 2 frozen-for-1 AND target 1 parked naming 2, so the belt fires when the mint is driven.
#[test]
fn dead_target_thaw_belt_refuses_while_a_park_names_the_source() {
  let (mut m, mut stores) = merge_host(1, 1);
  freeze_and_park(&mut m, &mut stores);
  let now = Instant::ORIGIN;
  let (log, stable) = stores.0.get_mut(&2).unwrap();
  assert!(
    matches!(
      m.propose_dead_target_thaw(&2, now, log, stable, &1),
      Some(Err(MergeError::SourceAbsorbParked))
    ),
    "the belt refuses the dead-target mint while a hosted park still names the source"
  );
}

/// A host whose only group is source 2, crafted FROZEN for the never-hosted target 1 (a restored
/// durable log holding the `PrepareMerge` at index 1) and elected, with the stores reading
/// `target_floor` for 1: the chain strand, where no local verb can ever release the freeze.
fn source_frozen_for_unhosted_target(
  target_floor: u64,
) -> (MultiRaft<u64, u64, CountSm>, LineageStores) {
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let mut slog = VecLog::default();
  slog.force_append(&[crate::Entry::new(
    Term::new(1),
    Index::new(1),
    crate::EntryKind::PrepareMerge,
    prepare_merge_bytes(1, 1),
  )]);
  let mut sstable = AsyncStable::default();
  sstable.force_state(Term::new(1), Some(1u64), Index::new(1));
  m.restore_group_unchecked(
    2,
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
    m.group(&2).unwrap().is_frozen(),
    "the crafted freeze applied"
  );
  // Elect group 2 (leader-only mint); a frozen group still campaigns.
  let d = m.group(&2).unwrap().poll_timeout().unwrap();
  m.handle_timeout(&2, d, &mut slog, &mut sstable).unwrap();
  drain_storage(&mut m, 2, d, &mut slog, &mut sstable);
  assert!(m.group(&2).unwrap().role().is_leader());
  while m.poll_message().is_some() {}
  while m.poll_event().is_some() {}

  let mut inner = MapStores(
    std::collections::BTreeMap::new(),
    std::collections::BTreeSet::new(),
  );
  inner.0.insert(2, (slog, sstable));
  let stores = LineageStores {
    inner,
    floors: std::collections::BTreeMap::from([(1u64, target_floor)]),
    lineages: std::collections::BTreeMap::new(),
  };
  (m, stores)
}

/// THE TERMINAL-FLOOR-ONLY TRIGGER: a source frozen for an UNHOSTED target self-thaws ONLY when the
/// target reads the terminal `MERGED_FLOOR` — a NON-terminal floor is a host-local fact and must mint
/// NOTHING (the witness-mint discipline, second edition). A crafted frozen source (2) claims target 1,
/// which is never hosted; under a non-terminal floor the source stays frozen forever, and only the
/// terminal floor derives its thaw. What the non-terminal floor DOES yield is the strand's
/// observation: one `StrandedSource` naming the dead target, the source and the source's freeze
/// index, deduped across the cranks that re-derive it, with the source left exactly as it was —
/// hosted, and refusing removal as any frozen source does — and retired the crank the terminal
/// floor hands the source to the dead-target thaw.
#[test]
fn dead_target_thaw_needs_the_terminal_floor_not_a_non_terminal_one() {
  let now = Instant::ORIGIN;
  // Target 1 is UNHOSTED with a NON-terminal floor (5): the trigger must NOT mint.
  let (mut m, mut stores) = source_frozen_for_unhosted_target(5);
  for _ in 0..4 {
    m.service_merge_applies(now, &mut stores);
    let (log, stable) = stores.inner.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    m.group(&2).unwrap().is_frozen(),
    "a non-terminal floor is host-local — it mints NO dead-target thaw"
  );
  assert_eq!(
    m.poll_merge_blocked(),
    Some(MergeBlocked {
      target: 1,
      source: 2,
      boundary: Index::new(1),
      cause: MergeBlockedCause::StrandedSource,
    }),
    "a source frozen for an unhosted, unfloored target is reported stranded, at its freeze index"
  );
  assert_eq!(
    m.poll_merge_blocked(),
    None,
    "one signal for the standing strand, however many cranks re-derive it"
  );
  assert!(
    matches!(m.remove_group(&2, &mut stores), Err(RemoveError::Frozen)),
    "the stranded source is a live frozen replica, not a husk: it stays hosted and unremovable"
  );

  // Flip the target's floor to the TERMINAL sentinel: now the source derives its own thaw, and
  // the observation retires with the strand — nothing further is signalled.
  stores.floors.insert(1, crate::MERGED_FLOOR);
  for _ in 0..4 {
    m.service_merge_applies(now, &mut stores);
    let (log, stable) = stores.inner.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    !m.group(&2).unwrap().is_frozen(),
    "the terminal floor on the dead target derived the source's own thaw"
  );
  assert_eq!(
    m.poll_merge_blocked(),
    None,
    "the observation retired with the strand"
  );
  assert!(
    m.merge_blocked_seen.is_empty(),
    "and its edge retired with it"
  );
}

/// Re-hosting the dead target — the observation's own remedy — RETIRES the strand: the next crank
/// finds the target hosted, the edge and any undelivered signal go with it, and a later strand of
/// the same pair (the target gone again) signals afresh instead of being deduped against the
/// retired edge.
#[test]
fn a_re_hosted_target_retires_the_strand_and_a_re_strand_signals_afresh() {
  let now = Instant::ORIGIN;
  let (mut m, mut stores) = source_frozen_for_unhosted_target(0);
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  assert_eq!(
    m.poll_merge_blocked()
      .map(|b| (b.target, b.source, b.cause)),
    Some((1, 2, MergeBlockedCause::StrandedSource))
  );

  // The embedder re-hosts the target: a fresh incarnation admits (no floor below it, no pin).
  m.create_group(1, 0, single_node_cfg(1), now, 9, CountSm::default())
    .unwrap();
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  assert_eq!(
    m.poll_merge_blocked(),
    None,
    "a hosted target is no strand: the observation retired"
  );
  assert!(m.merge_blocked_seen.is_empty(), "and its edge with it");
  assert!(
    m.group(&2).unwrap().is_frozen(),
    "the source now waits on the re-hosted target's own resolution"
  );

  // The target dies again through the ungated inner teardown (the public door refuses a target
  // a hosted source claims, `Claimed`): the same pair strands afresh, and signals afresh.
  assert!(m.remove_group_inner(&1).is_some());
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  assert_eq!(
    m.poll_merge_blocked(),
    Some(MergeBlocked {
      target: 1,
      source: 2,
      boundary: Index::new(1),
      cause: MergeBlockedCause::StrandedSource,
    }),
    "a legitimate re-strand is a fresh transition"
  );
}

/// A POISONED frozen source is left to its own fail-stop, as every pass leaves it: the strand is
/// not observed on it.
#[test]
fn a_poisoned_stranded_source_is_not_observed() {
  let now = Instant::ORIGIN;
  let (mut m, mut stores) = source_frozen_for_unhosted_target(0);
  m.group_mut(&2).unwrap().poison(PoisonReason::MergeDecode);
  for _ in 0..3 {
    assert!(m.service_merge_applies(now, &mut stores).is_empty());
  }
  assert_eq!(
    m.poll_merge_blocked(),
    None,
    "a poisoned source's strand is its poison signal's business"
  );
}

/// An ORDINARY in-flight merge is not a strand: the freeze applied, the target is hosted and has
/// simply not parked its `CommitMerge` yet — an arbitrarily long window on a follower host — and
/// naming a live source to an embedder would invite exactly the floor write the cause forbids.
#[test]
fn an_ordinary_merge_freeze_with_a_hosted_target_is_not_a_strand() {
  let now = Instant::ORIGIN;
  let (mut m, mut stores) = merge_host(1, 1);
  m.prepare_merge(&2, now, &mut stores, &1).unwrap().unwrap();
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(m.group(&2).unwrap().is_frozen());
  assert!(
    m.group(&1).unwrap().pending_merge().is_none(),
    "the target has not parked"
  );
  for _ in 0..3 {
    assert!(m.service_merge_applies(now, &mut stores).is_empty());
  }
  assert_eq!(
    m.poll_merge_blocked(),
    None,
    "a hosted target owns the merge: nothing is signalled"
  );
}

/// Add a led single-voter [`CountSm`] group `gid` to a [`merge_host`].
fn add_led_count_group(m: &mut MultiRaft<u64, u64, CountSm>, stores: &mut MapStores, gid: u64) {
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
  let (l, s) = stores.0.get_mut(&gid).unwrap();
  let d = m.group(&gid).unwrap().poll_timeout().unwrap();
  m.handle_timeout(&gid, d, l, s).unwrap();
  drain_storage(m, gid, d, l, s);
  assert!(m.group(&gid).unwrap().role().is_leader());
  while m.poll_message().is_some() {}
  while m.poll_event().is_some() {}
}

/// Freeze the led single-voter `source` for `target` at the ENDPOINT seam — a freeze committed
/// by a leader whose door ran on another host's view — at the source's next lineage.
fn freeze_at_seam<F>(
  m: &mut MultiRaft<u64, u64, F>,
  stores: &mut MapStores,
  source: u64,
  target: u64,
) where
  F: crate::StateMachine<Command = Bytes, Snapshot = u64>,
  F::Error: core::error::Error,
{
  let next_gen = m.group(&source).unwrap().shape_gen() + 1;
  let (l, s) = stores.0.get_mut(&source).unwrap();
  m.group_mut(&source)
    .unwrap()
    .propose_merge_entry(
      Instant::ORIGIN,
      l,
      crate::EntryKind::PrepareMerge,
      prepare_merge_bytes(target, next_gen),
    )
    .unwrap();
  drain_storage(m, source, Instant::ORIGIN, l, s);
}

/// A standing park NAMING the source is that park's own observation, not a strand: while a
/// hosted target's `CommitMerge` names the source nothing is signalled for it, and only once the
/// park is gone — aborted deterministically at the closed window here, the freeze being a claim
/// by a different target — does the strand surface, keyed on the dead target.
#[test]
fn a_park_naming_the_stranded_source_holds_the_observation_until_it_resolves() {
  let now = Instant::ORIGIN;
  // X = 1, T = 2, S = 3: S freezes for T through the door, then T dies unresolved.
  let (mut m, mut stores) = merge_host(1, 1);
  add_led_count_group(&mut m, &mut stores, 3);
  m.prepare_merge(&3, now, &mut stores, &2).unwrap().unwrap();
  {
    let (l, s) = stores.0.get_mut(&3).unwrap();
    drain_storage(&mut m, 3, now, l, s);
  }
  let (freeze_idx, expected) = {
    let sep = m.group(&3).unwrap();
    assert!(sep.is_frozen(), "S froze for T");
    (sep.freeze_index().unwrap(), sep.shape_gen())
  };
  assert!(
    m.remove_group_inner(&2).is_some(),
    "T dies through the ungated teardown"
  );
  // A foreign-led `CommitMerge(3→1)` parks X naming S (the seam: the door's gates ran elsewhere).
  {
    let (l, s) = stores.0.get_mut(&1).unwrap();
    m.group_mut(&1)
      .unwrap()
      .propose_merge_entry(
        now,
        l,
        crate::EntryKind::CommitMerge,
        commit_merge_bytes(3, freeze_idx, expected, 1),
      )
      .unwrap();
    drain_storage(&mut m, 1, now, l, s);
  }
  assert!(
    m.group(&1).unwrap().pending_merge().is_some(),
    "X parked naming S"
  );
  assert!(
    m.service_merge_applies(now, &mut stores).is_empty(),
    "the first pass only seals the window"
  );
  assert_eq!(
    m.poll_merge_blocked(),
    None,
    "the park names the source: the hold is the park's to report, not a strand"
  );
  {
    let (l, s) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, l, s);
  }
  assert_eq!(
    m.service_merge_applies(now, &mut stores),
    std::vec![MergeResolution::Aborted {
      source: 3,
      target: 1
    }],
    "a park under a foreign claim aborts deterministically"
  );
  assert_eq!(
    m.poll_merge_blocked(),
    Some(MergeBlocked {
      target: 2,
      source: 3,
      boundary: freeze_idx,
      cause: MergeBlockedCause::StrandedSource,
    }),
    "with the park gone the strand surfaces, keyed on the dead target"
  );
  assert!(
    m.group(&3).unwrap().is_frozen(),
    "X's aborted park released nothing on S: its claim is T's"
  );
}

/// The `Clear` arm's window: the legal co-hosted chain S→T→U. U's absorb consumes T through the
/// ungated teardown, leaving S frozen for a target that is unhosted and — for the rest of this
/// crank — unfloored: the driver writes T's terminal floor only after the call returns. A target
/// named as the SOURCE of a resolution pushed this crank is therefore no strand, and the next
/// crank's terminal floor hands S to the dead-target thaw.
#[test]
fn a_target_consumed_this_crank_is_not_a_stranded_sources_target() {
  let now = Instant::ORIGIN;
  // U = 1, T = 2, S = 3, all led; S freezes for T, then T for U.
  let (mut m, mut stores) = merge_host(1, 1);
  add_led_count_group(&mut m, &mut stores, 3);
  m.prepare_merge(&3, now, &mut stores, &2).unwrap().unwrap();
  {
    let (l, s) = stores.0.get_mut(&3).unwrap();
    drain_storage(&mut m, 3, now, l, s);
  }
  assert!(m.group(&3).unwrap().is_frozen(), "S froze for T");
  // T's own freeze for U arrives committed from a leader that never observed S's claim (the
  // door here refuses it `SourceClaimedAsTarget`): the seam.
  freeze_at_seam(&mut m, &mut stores, 2, 1);
  assert!(m.group(&2).unwrap().is_frozen(), "T froze for U");
  {
    let (l, s) = stores.0.get_mut(&1).unwrap();
    m.commit_merge(&1, now, l, s, &2).unwrap().unwrap();
    drain_storage(&mut m, 1, now, l, s);
  }
  assert!(m.group(&1).unwrap().pending_merge().is_some(), "U parked");
  seal_window(&mut m, &mut stores);
  assert_eq!(
    m.service_merge_applies(now, &mut stores),
    std::vec![MergeResolution::Merged {
      source: 2,
      target: 1
    }],
    "U absorbed T"
  );
  assert!(
    !m.contains_group(&2) && m.group(&3).unwrap().is_frozen(),
    "S is frozen for a target consumed this crank"
  );
  assert_eq!(
    m.poll_merge_blocked(),
    None,
    "the consumed target's floor is the driver's next write, not a strand"
  );
  // The driver folds the `Merged`: T's terminal floor. The dead-target thaw takes over.
  stores.1.insert(2);
  for _ in 0..4 {
    assert!(m.service_merge_applies(now, &mut stores).is_empty());
    let (l, s) = stores.0.get_mut(&3).unwrap();
    drain_storage(&mut m, 3, now, l, s);
  }
  assert!(
    !m.group(&3).unwrap().is_frozen(),
    "the terminal floor on the dead target derived S's own thaw"
  );
  assert_eq!(m.poll_merge_blocked(), None, "nothing was ever a strand");
}

/// The `Absorbed` window: U's absorb of T DEFERS behind a fence, and T's floor is DELIBERATELY
/// left unwritten until the debt discharges — T's stores are the union's only restart derivation.
/// A source frozen for that T is no strand while the debt names T, nor in the crank the discharge
/// surfaces `Merged`; the driver's floor then hands it to the dead-target thaw.
#[test]
fn a_target_in_the_absorbed_debt_window_is_not_a_stranded_sources_target() {
  let (mut m, mut stores, _k, _split_idx, d, _ds) = fork_fenced_park_fixture();
  // S = 3 freezes for T = 2 at the seam (T is already frozen for U, which the door refuses).
  let (mut log3, mut stable3) = (VecLog::default(), AsyncStable::default());
  m.create_group(3, 0, single_node_cfg(1), d, 45, SplitSm::default())
    .unwrap();
  let d3 = lead_single_split(&mut m, 3, &mut log3, &mut stable3);
  stores.0.insert(3, (log3, stable3));
  {
    let (l, s) = stores.0.get_mut(&3).unwrap();
    m.group_mut(&3)
      .unwrap()
      .propose_merge_entry(
        d3,
        l,
        crate::EntryKind::PrepareMerge,
        prepare_merge_bytes(2, 1),
      )
      .unwrap();
    drain_storage(&mut m, 3, d3, l, s);
  }
  assert!(m.group(&3).unwrap().is_frozen(), "S froze for T");
  defer_to_absorbed(&mut m, &mut stores, d);
  assert!(
    !m.contains_group(&2) && m.debt_names(&2),
    "T is consumed and named by U's debt"
  );
  let mut observed = std::vec::Vec::new();
  for _ in 0..3 {
    assert!(m.service_merge_applies(d, &mut stores).is_empty());
    while let Some(b) = m.poll_merge_blocked() {
      observed.push(b);
    }
  }
  assert!(
    observed
      .iter()
      .all(|b| b.cause != MergeBlockedCause::StrandedSource),
    "the debt window is no strand: {observed:?}"
  );
  assert!(
    observed
      .iter()
      .any(|b| b.cause == MergeBlockedCause::ForkFence),
    "the debt's own fence is what is reported: {observed:?}"
  );
  // The fence lifts; the discharge surfaces `Merged` — the same crank names T as its source.
  m.remove_group(&200, &mut empty_stores()).unwrap();
  let split = install_head_fork(&mut m, 1, 200, d);
  m.lift_fork_barrier(&1, split);
  assert_eq!(
    m.service_merge_applies(d, &mut stores),
    std::vec![MergeResolution::Merged {
      source: 2,
      target: 1
    }],
    "the debt discharged"
  );
  assert_eq!(
    m.poll_merge_blocked(),
    None,
    "the discharge crank is no strand either: the floor is the driver's next write"
  );
  stores.1.insert(2);
  for _ in 0..4 {
    assert!(m.service_merge_applies(d, &mut stores).is_empty());
    let (l, s) = stores.0.get_mut(&3).unwrap();
    drain_storage(&mut m, 3, d3, l, s);
  }
  assert!(
    !m.group(&3).unwrap().is_frozen(),
    "the terminal floor on the dead target derived S's own thaw"
  );
  assert_eq!(m.poll_merge_blocked(), None, "nothing was ever a strand");
}

/// The `CaptureFailed` pin: U's absorb consumed T but its capture FAULTED, so T's stores and floor
/// are pinned untouched until the restart that re-parks against them. A source frozen for the
/// pinned T is no strand — the pin names T — across every crank until that restart.
#[test]
fn a_target_pinned_by_a_failed_capture_is_not_a_stranded_sources_target() {
  let now = Instant::ORIGIN;
  let fail = std::sync::Arc::new(core::sync::atomic::AtomicBool::new(false));
  // U = 1 (its forced capture armed to fault later), T = 2, S = 3.
  let (mut m, mut stores) = merge_host_with(
    SnapFailSm::default(),
    2,
    SnapFailSm {
      count: 0,
      fail: fail.clone(),
    },
    3,
  );
  stores
    .0
    .insert(3, (VecLog::default(), AsyncStable::default()));
  m.create_group(3, 0, single_node_cfg(1), now, 7, SnapFailSm::default())
    .unwrap();
  {
    let (l, s) = stores.0.get_mut(&3).unwrap();
    let d = m.group(&3).unwrap().poll_timeout().unwrap();
    m.handle_timeout(&3, d, l, s).unwrap();
    drain_storage(&mut m, 3, d, l, s);
    assert!(m.group(&3).unwrap().role().is_leader());
  }
  while m.poll_message().is_some() {}
  while m.poll_event().is_some() {}
  m.prepare_merge(&3, now, &mut stores, &2).unwrap().unwrap();
  {
    let (l, s) = stores.0.get_mut(&3).unwrap();
    drain_storage(&mut m, 3, now, l, s);
  }
  assert!(m.group(&3).unwrap().is_frozen(), "S froze for T");
  // T's own freeze for U arrives committed from a leader that never observed S's claim (the
  // door here refuses it `SourceClaimedAsTarget`): the seam. U then commits and parks.
  freeze_at_seam(&mut m, &mut stores, 2, 1);
  assert!(m.group(&2).unwrap().is_frozen(), "T froze for U");
  {
    let (l, s) = stores.0.get_mut(&1).unwrap();
    m.commit_merge(&1, now, l, s, &2).unwrap().unwrap();
    drain_storage(&mut m, 1, now, l, s);
  }
  assert!(m.group(&1).unwrap().pending_merge().is_some(), "U parked");
  seal_window(&mut m, &mut stores);
  fail.store(true, core::sync::atomic::Ordering::Relaxed);
  assert_eq!(
    m.service_merge_applies(now, &mut stores),
    std::vec![MergeResolution::CaptureFailed {
      source: 2,
      target: 1
    }],
    "U consumed T and its capture faulted"
  );
  assert!(
    !m.contains_group(&2) && m.debt_names(&2),
    "T is consumed and pinned"
  );
  for _ in 0..3 {
    assert!(m.service_merge_applies(now, &mut stores).is_empty());
  }
  assert_eq!(
    m.poll_merge_blocked(),
    None,
    "a pinned target is no strand: the restart re-parks against it"
  );
  assert!(m.group(&3).unwrap().is_frozen(), "S waits on that restart");
}

/// Distinct pairs coexist in one crank: a park's own observation (its unhosted source) and a
/// stranded source's, each keyed by its `(target, source)` pair and each retained.
#[test]
fn a_park_observation_and_a_strand_observation_coexist() {
  let now = Instant::ORIGIN;
  let (mut m, mut stores) = under_hosted_park_host();
  // S = 3, crafted frozen for the never-hosted T = 2.
  let mut slog = VecLog::default();
  slog.force_append(&[crate::Entry::new(
    Term::new(1),
    Index::new(1),
    crate::EntryKind::PrepareMerge,
    prepare_merge_bytes(2, 1),
  )]);
  let mut sstable = AsyncStable::default();
  sstable.force_state(Term::new(1), Some(1u64), Index::new(1));
  m.restore_group_unchecked(
    3,
    single_node_cfg(1),
    now,
    7,
    SplitSm::default(),
    1,
    &mut slog,
    &mut sstable,
  )
  .unwrap();
  assert!(m.group(&3).unwrap().is_frozen());
  stores.0.insert(3, (slog, sstable));
  for _ in 0..2 {
    assert!(m.service_merge_applies(now, &mut stores).is_empty());
  }
  let mut observed = std::vec::Vec::new();
  while let Some(b) = m.poll_merge_blocked() {
    observed.push(b);
  }
  observed.sort_by_key(|b| b.target);
  assert_eq!(
    observed,
    std::vec![
      MergeBlocked {
        target: 1,
        source: 42,
        boundary: Index::new(2),
        cause: MergeBlockedCause::SourceUnhosted,
      },
      MergeBlocked {
        target: 2,
        source: 3,
        boundary: Index::new(1),
        cause: MergeBlockedCause::StrandedSource,
      },
    ],
    "two pairs, two observations, once each"
  );
}

/// Observations are keyed by their `(target, source)` PAIR, not by the target alone: a follower
/// target that is PARKED (a foreign-led second absorb — `AlreadyPending` is host-local) while it
/// holds a DEBT reports both holds — the park's unhosted source and the debt's fence — where a
/// target-keyed edge let the debt pass overwrite the park's signal every crank.
#[test]
fn a_parked_target_holding_a_debt_reports_both_holds() {
  let (mut m, mut stores, _k, _split_idx, d, _ds) = fork_fenced_park_fixture();
  let debt_at = defer_to_absorbed(&mut m, &mut stores, d);
  // A debt-free foreign leader committed a second absorb into this target, of the unhosted 3.
  {
    let (l, s) = stores.0.get_mut(&1).unwrap();
    m.group_mut(&1)
      .unwrap()
      .propose_merge_entry(
        d,
        l,
        crate::EntryKind::CommitMerge,
        commit_merge_bytes(3, Index::new(9), 1, 3),
      )
      .unwrap();
    drain_storage(&mut m, 1, d, l, s);
  }
  let park = m
    .group(&1)
    .unwrap()
    .pending_merge()
    .expect("the second absorb parked")
    .at();
  for _ in 0..3 {
    assert!(
      !m.service_merge_applies(d, &mut stores)
        .iter()
        .any(|r| matches!(r, MergeResolution::Absorbed { .. })),
      "the standing debt holds the second park"
    );
    let (l, s) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, d, l, s);
  }
  let mut observed = std::vec::Vec::new();
  while let Some(b) = m.poll_merge_blocked() {
    observed.push(b);
  }
  observed.sort_by_key(|b| b.source);
  assert_eq!(
    observed,
    std::vec![
      MergeBlocked {
        target: 1,
        source: 2,
        boundary: debt_at,
        cause: MergeBlockedCause::ForkFence,
      },
      MergeBlocked {
        target: 1,
        source: 3,
        boundary: park,
        cause: MergeBlockedCause::SourceUnhosted,
      },
    ],
    "both holds on the one target are reported, once each"
  );
}

/// PIN B(c): a hosted park still NAMING the husk as its source HOLDS the dissolve — reclaiming it
/// first would hand the resolver a MANUFACTURED absence and skip the union (committed divergence).
/// The park absorbs it instead (Merged, never Retired), union intact.
#[test]
fn a_park_naming_the_husk_holds_the_dissolve_then_absorbs() {
  let (mut m, mut stores) = merge_host(2, 3);
  let now = Instant::ORIGIN;
  let k = freeze_and_park(&mut m, &mut stores);
  // The catalog floors 1 terminally, RACING the local absorb: 1 is frozen + MERGED_FLOOR AND 2's park
  // names it as source. The husk arm must HOLD (the park gate).
  stores.1.insert(2);
  let sealed = m.service_merge_applies(now, &mut stores);
  assert!(sealed.is_empty(), "the seal pass resolves nothing");
  assert!(
    m.contains_group(&2),
    "the park gate held the dissolve — 1 is not reclaimed as a husk"
  );
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  let resolved = m.service_merge_applies(now, &mut stores);
  assert_eq!(
    resolved,
    std::vec![MergeResolution::Merged {
      source: 2,
      target: 1
    }],
    "the park absorbs the husk (Merged), never the dissolve (Retired)"
  );
  assert!(!m.contains_group(&2), "1 absorbed into 2");
  let tep = m.group(&1).unwrap();
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
  let (mut m, mut stores) = merge_host_triple(3, 2, 4);
  let now = Instant::ORIGIN;
  {
    m.prepare_merge(&2, now, &mut stores, &1).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.rollback_merge(&1, now, log, stable, &2).unwrap().unwrap();
  }
  // The freeze door refuses the colocated form (`SourceClaimedAsTarget` — 1's applied claim on 2
  // is locally visible); the window is cross-host residue now, so build it past the door with a
  // direct endpoint append, exactly as the late-obligation belt does.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    let mut tbytes = Vec::new();
    Data::encode(&3u64, &mut tbytes);
    let freeze = crate::PrepareMergePayload::new(Bytes::from(tbytes), 1);
    let mut fbuf = Vec::new();
    crate::wire::encode_prepare_merge_payload(&freeze, &mut fbuf);
    m.group_mut(&1)
      .unwrap()
      .propose_merge_entry(now, log, crate::EntryKind::PrepareMerge, Bytes::from(fbuf))
      .unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    m.group(&1).unwrap().owes_live_thaw() && m.group(&1).unwrap().is_frozen(),
    "2 owes 1 a drivable thaw AND is frozen for 3"
  );
  // The catalog (adversarially) floors 2 terminally; the belt must hold the dissolve while 2 owes a
  // thaw THIS host can drive (1 is hosted).
  stores.1.insert(1);
  let resolutions = m.service_merge_applies(now, &mut stores);
  assert!(
    !resolutions
      .iter()
      .any(|r| matches!(r, MergeResolution::Retired { .. })),
    "the belt held — a husk owing a locally-drivable thaw is NOT dissolved"
  );
  assert!(
    m.contains_group(&1),
    "2 is untouched while its drivable obligation stands"
  );
  assert!(
    m.group(&1).unwrap().owes_live_thaw(),
    "the obligation is intact — the dissolve did not drop it"
  );
}

/// The negative pin: a source that owes NO thaw dissolves in the ordinary cadence — the residual
/// belt never over-fires. 2 freezes into 3 with no outstanding obligation and is absorbed in the
/// same single resolve pass a clean source always was.
#[test]
fn a_source_without_an_obligation_absorbs_at_once() {
  let (mut m, mut stores) = merge_host_triple(2, 4, 3);
  let now = Instant::ORIGIN;
  assert!(!m.group(&3).unwrap().owes_live_thaw(), "2 owes nothing");
  {
    m.prepare_merge(&3, now, &mut stores, &2).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&3).unwrap();
    drain_storage(&mut m, 3, now, log, stable);
  }
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.commit_merge(&2, now, log, stable, &3).unwrap().unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  let done = m.service_merge_applies(now, &mut stores);
  assert_eq!(
    done,
    std::vec![MergeResolution::Merged {
      source: 3,
      target: 2
    }],
    "a clean source is absorbed with no extra crank"
  );
  assert!(!m.contains_group(&3));
  assert_eq!(m.group(&2).unwrap().state_machine().count(), 4 + 3);
}

/// An obligation whose owed id will NOT decode is committed-corrupt — the same `MergeDecode` class
/// the thaw pass and park decode raise. The drivability belt must HOLD the park and poison the
/// SOURCE (the deterministic fail-stop every host reaches), never treat the undecodable id as "not
/// drivable" and AUTHORIZE the dissolve: that lets the absorb proceed (Merged) and drops the corrupt
/// obligation, diverging hosts between fail-stop and progress by crank order.
#[test]
fn a_corrupt_owed_id_holds_the_park_and_poisons_the_source() {
  let (mut m, mut stores) = merge_host_triple(2, 4, 3);
  let now = Instant::ORIGIN;
  // A corrupt obligation on 2, then 2 freezes into 3 ABOVE it (the unguarded-max ordering the
  // `SourceOwesThaw` gate cannot see): draining applies the abort (abandoned[corrupt]) then the
  // freeze. 3 bytes never decode as the `u64` group id.
  let corrupt = Bytes::from_static(&[0xFF, 0xFF, 0xFF]);
  {
    let (log, _stable) = stores.0.get_mut(&3).unwrap();
    let abort = crate::RollbackMergePayload::abort(corrupt.clone(), 1, 1);
    let mut buf = Vec::new();
    crate::wire::encode_rollback_merge_payload(&abort, &mut buf);
    m.group_mut(&3)
      .unwrap()
      .propose_merge_entry(now, log, crate::EntryKind::RollbackMerge, Bytes::from(buf))
      .unwrap();
  }
  {
    m.prepare_merge(&3, now, &mut stores, &2).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&3).unwrap();
    drain_storage(&mut m, 3, now, log, stable);
  }
  assert!(m.group(&3).unwrap().is_frozen(), "2 is frozen for 3");
  assert!(
    m.group(&3).unwrap().owes_live_thaw(),
    "2 carries the corrupt obligation"
  );
  // 3 commits the absorb of 2 and parks; seal 3's window.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.commit_merge(&2, now, log, stable, &3).unwrap().unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(m.group(&2).unwrap().pending_merge().is_some(), "3 parked");
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  // The belt HOLDS the park and poisons the source — never a silent dissolve.
  let held = m.service_merge_applies(now, &mut stores);
  assert!(
    held.is_empty(),
    "the corrupt obligation holds the absorb: {held:?}"
  );
  assert!(m.contains_group(&3), "2 is NOT dissolved");
  assert!(
    m.group(&2).unwrap().pending_merge().is_some(),
    "3 is still parked"
  );
  assert!(
    m.group(&3).unwrap().is_poisoned(),
    "the source is poisoned MergeDecode"
  );
}

/// The CONTRAST to the corrupt-id poison above: an obligation whose owed id DECODES but is not hosted
/// here is a local dead-end — a co-hosting replica drives that thaw, so the absorb PROCEEDS and
/// dropping the dead-end obligation strands nothing. This is what separates the corrupt-id poison
/// from the belt's ordinary dead-end drop; both share the resolve arm's decode.
#[test]
fn a_decodable_unhosted_owed_id_lets_the_absorb_proceed() {
  let (mut m, mut stores) = merge_host_triple(2, 4, 3);
  let now = Instant::ORIGIN;
  // 999 is a decodable `u64` that is NOT a hosted group — a local dead-end obligation.
  let mut unhosted = Vec::new();
  Data::encode(&999u64, &mut unhosted);
  let unhosted = Bytes::from(unhosted);
  {
    let (log, _stable) = stores.0.get_mut(&3).unwrap();
    let abort = crate::RollbackMergePayload::abort(unhosted.clone(), 1, 1);
    let mut buf = Vec::new();
    crate::wire::encode_rollback_merge_payload(&abort, &mut buf);
    m.group_mut(&3)
      .unwrap()
      .propose_merge_entry(now, log, crate::EntryKind::RollbackMerge, Bytes::from(buf))
      .unwrap();
  }
  {
    m.prepare_merge(&3, now, &mut stores, &2).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&3).unwrap();
    drain_storage(&mut m, 3, now, log, stable);
  }
  assert!(
    m.group(&3).unwrap().owes_live_thaw(),
    "2 owes a dead-end thaw"
  );
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.commit_merge(&2, now, log, stable, &3).unwrap().unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  let done = m.service_merge_applies(now, &mut stores);
  assert_eq!(
    done,
    std::vec![MergeResolution::Merged {
      source: 3,
      target: 2
    }],
    "a decodable dead-end obligation does not hold the absorb"
  );
  assert!(
    !m.contains_group(&3),
    "2 dissolved — the dead-end obligation dropped by design"
  );
  assert!(
    !m.group(&2).unwrap().is_poisoned(),
    "no poison for a decodable id"
  );
}

/// Group 3 (a former target) commits a TARGET-role abort for unhosted source 999, freezes into
/// group 2, and 2 parks its commit — the frozen holder shape of the drivability belt — with 3's
/// record then DISCHARGED (the proof observed after the freeze). Returns the container and seam
/// after the park's seal crank, one crank short of the absorb.
fn frozen_holder_with_a_witness_debt() -> (MultiRaft<u64, u64, CountSm>, MapStores, Bytes) {
  let (mut m, mut stores) = merge_host_triple(2, 4, 3);
  let now = Instant::ORIGIN;
  let unhosted = gid_key(999);
  {
    let (log, _stable) = stores.0.get_mut(&3).unwrap();
    let abort = crate::RollbackMergePayload::abort(unhosted.clone(), 1, 1);
    let mut buf = Vec::new();
    crate::wire::encode_rollback_merge_payload(&abort, &mut buf);
    m.group_mut(&3)
      .unwrap()
      .propose_merge_entry(now, log, crate::EntryKind::RollbackMerge, Bytes::from(buf))
      .unwrap();
  }
  {
    m.prepare_merge(&3, now, &mut stores, &2).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&3).unwrap();
    drain_storage(&mut m, 3, now, log, stable);
  }
  assert!(m.group(&3).unwrap().is_frozen() && m.group(&3).unwrap().owes_live_thaw());
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.commit_merge(&2, now, log, stable, &3).unwrap().unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(m.group(&2).unwrap().pending_merge().is_some(), "parked");
  // The proof, observed after the freeze: the record retires as a witness debt.
  m.group_mut(&3).unwrap().note_discharged(&unhosted);
  assert!(m.group(&3).unwrap().holds_witness_debt());
  (m, stores, unhosted)
}

/// THE WITNESS DEBT HOLDS THE ABSORB — THE LEADING HOLDER (#137): frozen holder 3 leads, so the
/// seal crank appends its witness, but the witness is not yet applied when the resolving crank
/// reaches 2's park. The Resolve arm HOLDS: absorbing would destroy the only future trigger while
/// the witness is in flight. Once the witness applies, the absorb proceeds.
#[test]
fn the_absorb_holds_on_a_witness_debt_until_the_appended_witness_applies() {
  let (mut m, mut stores, _) = frozen_holder_with_a_witness_debt();
  let now = Instant::ORIGIN;
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  assert_eq!(
    witness_count(&stores.0.get(&3).unwrap().0),
    1,
    "the leading holder appended its witness on the seal crank"
  );
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    m.service_merge_applies(now, &mut stores).is_empty(),
    "the absorb HOLDS on the witness debt — the witness is appended but not applied"
  );
  assert!(
    m.contains_group(&3) && m.group(&2).unwrap().pending_merge().is_some(),
    "the holder stands and the park waits"
  );
  // The witness applies on the holder's own drain; the debt retires; the absorb proceeds.
  {
    let (log, stable) = stores.0.get_mut(&3).unwrap();
    drain_storage(&mut m, 3, now, log, stable);
  }
  assert!(
    !m.group(&3).unwrap().holds_witness_debt(),
    "the committed witness apply retired the debt"
  );
  assert_eq!(
    m.service_merge_applies(now, &mut stores),
    std::vec![MergeResolution::Merged {
      source: 3,
      target: 2
    }],
    "with the debt retired, the absorb proceeds"
  );
  assert!(!m.contains_group(&3));
}

/// THE WITNESS DEBT HOLDS THE ABSORB — THE FOLLOWER HOLDER (#137): frozen holder 3 has stepped
/// down, so no witness exists anywhere; the Resolve arm HOLDS all the same. A peer's witness,
/// delivered by replication, retires the debt, and the absorb proceeds.
#[test]
fn the_absorb_holds_on_a_witness_debt_until_a_peers_witness_applies() {
  let (mut m, mut stores, unhosted) = frozen_holder_with_a_witness_debt();
  let now = Instant::ORIGIN;
  {
    let (log, stable) = stores.0.get_mut(&3).unwrap();
    step_down(&mut m, 3, log, stable);
  }
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert_eq!(
    witness_count(&stores.0.get(&3).unwrap().0),
    0,
    "a follower holder minted nothing"
  );
  assert!(
    m.service_merge_applies(now, &mut stores).is_empty(),
    "the absorb HOLDS on the witness debt — no witness exists yet"
  );
  assert!(m.contains_group(&3) && m.group(&2).unwrap().pending_merge().is_some());
  // A peer's witness arrives by replication from the leader the holder now follows.
  {
    let (log, stable) = stores.0.get_mut(&3).unwrap();
    let last = log.last_index();
    let last_term = log.term(last).unwrap();
    let term = m.group(&3).unwrap().term();
    let mut buf = Vec::new();
    crate::wire::encode_thaw_discharged_payload(&ThawDischargedPayload::new(unhosted, 1), &mut buf);
    m.handle_message(
      &3,
      now,
      log,
      stable,
      2u64,
      Message::AppendEntries(crate::AppendEntries::new(
        term,
        2u64,
        last,
        last_term,
        std::vec![crate::Entry::new(
          term,
          last.next(),
          crate::EntryKind::ThawDischarged,
          Bytes::from(buf),
        )],
        last.next(),
      )),
    )
    .unwrap();
    drain_storage(&mut m, 3, now, log, stable);
  }
  assert!(
    !m.group(&3).unwrap().holds_witness_debt(),
    "the peer's witness retired the debt"
  );
  assert_eq!(
    m.service_merge_applies(now, &mut stores),
    std::vec![MergeResolution::Merged {
      source: 3,
      target: 2
    }],
    "with the debt retired, the absorb proceeds"
  );
}

/// THE WITNESS DEBT HOLDS THE HUSK DISSOLVE (#137): source 2 is a hosted frozen husk at the
/// terminal floor (absorbed away elsewhere), a follower, carrying a discharged record for an
/// unhosted source. The dissolve would destroy the only future trigger, so it HOLDS until a peer's
/// witness applies; then it retires the husk.
#[test]
fn the_husk_dissolve_holds_on_a_witness_debt_until_a_peers_witness_applies() {
  let now = Instant::ORIGIN;
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let mut stores = MapStores(std::collections::BTreeMap::new(), Default::default());
  stores
    .0
    .insert(1, (VecLog::default(), AsyncStable::default()));
  m.create_group(1, 0, single_node_cfg(1), now, 7, CountSm::default())
    .unwrap();
  admit_frozen_source(&mut m, &mut stores);
  let unhosted = gid_key(999);
  {
    let ep = m.group_mut(&2).unwrap();
    ep.note_abandoned(unhosted.clone(), 1, Index::new(1));
    ep.note_discharged(&unhosted);
  }
  stores.1.insert(2);
  for _ in 0..2 {
    assert!(
      !m.service_merge_applies(now, &mut stores)
        .contains(&MergeResolution::Retired { source: 2 }),
      "the dissolve HOLDS on the witness debt"
    );
  }
  assert!(
    m.contains_group(&2) && m.group(&2).unwrap().is_frozen(),
    "the husk stands"
  );
  // A peer's witness arrives by replication; its apply retires the debt; the dissolve proceeds.
  {
    let mut buf = Vec::new();
    crate::wire::encode_thaw_discharged_payload(&ThawDischargedPayload::new(unhosted, 1), &mut buf);
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.handle_message(
      &2,
      now,
      log,
      stable,
      9u64,
      Message::AppendEntries(crate::AppendEntries::new(
        Term::new(1),
        9u64,
        Index::new(1),
        Term::new(1),
        std::vec![crate::Entry::new(
          Term::new(1),
          Index::new(2),
          crate::EntryKind::ThawDischarged,
          Bytes::from(buf),
        )],
        Index::new(2),
      )),
    )
    .unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    !m.group(&2).unwrap().holds_witness_debt(),
    "the peer's witness retired the debt"
  );
  assert!(
    m.service_merge_applies(now, &mut stores)
      .contains(&MergeResolution::Retired { source: 2 }),
    "with the debt retired, the husk dissolves"
  );
  assert!(!m.contains_group(&2));
}

/// THE PURGE EXIT FOR A FROZEN HOLDER'S DEBT (#137): frozen holder 3 (a follower) observed source
/// 4 — hosted here, live past the abandoned generation — and holds the debt with no witness. Its
/// absorb HOLDS; removing 4 admits (nothing gates a thawed, unowed source), the purge clears 3's
/// record with it, and the absorb proceeds on the next crank.
#[test]
fn a_frozen_holders_debt_retires_through_the_purge_and_the_absorb_proceeds() {
  let (mut m, mut stores) = merge_host_triple(2, 4, 3);
  let now = Instant::ORIGIN;
  // Source 4, hosted and live past the generation 3's abort abandoned (founded at 2).
  stores
    .0
    .insert(4, (VecLog::default(), AsyncStable::default()));
  {
    let (log, stable) = stores.0.get_mut(&4).unwrap();
    m.create_group_founded_at(
      4,
      2,
      single_node_cfg(1),
      now,
      9,
      CountSm::default(),
      1,
      &*log,
      stable,
    )
    .unwrap();
  }
  assert_eq!(
    m.group(&4).unwrap().shape_gen(),
    2,
    "live past the generation"
  );
  {
    let (log, _stable) = stores.0.get_mut(&3).unwrap();
    let abort = crate::RollbackMergePayload::abort(gid_key(4), 1, 1);
    let mut buf = Vec::new();
    crate::wire::encode_rollback_merge_payload(&abort, &mut buf);
    m.group_mut(&3)
      .unwrap()
      .propose_merge_entry(now, log, crate::EntryKind::RollbackMerge, Bytes::from(buf))
      .unwrap();
  }
  {
    m.prepare_merge(&3, now, &mut stores, &2).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&3).unwrap();
    drain_storage(&mut m, 3, now, log, stable);
  }
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.commit_merge(&2, now, log, stable, &3).unwrap().unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(m.group(&2).unwrap().pending_merge().is_some(), "parked");
  // The holder steps down (the barrier was certified on its leader's tracker at the commit), so
  // it will mint nothing. The seal crank: 3 observes 4 past the generation — discharged, unwitnessed.
  {
    let (log, stable) = stores.0.get_mut(&3).unwrap();
    step_down(&mut m, 3, log, stable);
  }
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  assert!(
    m.group(&3).unwrap().holds_witness_debt() && witness_count(&stores.0.get(&3).unwrap().0) == 0,
    "the follower holder retired the record as a debt, unwitnessed"
  );
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    m.service_merge_applies(now, &mut stores).is_empty(),
    "the absorb HOLDS on the debt"
  );
  // THE EXIT: the thawed source is removable, and its purge clears the debt with it.
  assert_eq!(
    m.remove_group(&4, &mut stores).map(|r| r.is_some()),
    Ok(true),
    "a source live past the generation is removable — nothing gates it"
  );
  assert!(
    m.group(&3).unwrap().abandoned_obligations().is_empty(),
    "the purge cleared the debt"
  );
  assert_eq!(
    m.service_merge_applies(now, &mut stores),
    std::vec![MergeResolution::Merged {
      source: 3,
      target: 2
    }],
    "with the debt purged, the absorb proceeds"
  );
}

/// SAFETY, structural: a source thaw is REFUSED with NO append unless the claimed
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
    m.prepare_merge(&2, now, &mut stores, &1).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(m.group(&2).unwrap().is_frozen() && m.group(&2).unwrap().shape_gen() == 1);
  assert!(
    !m.group(&1).unwrap().owes_live_thaw(),
    "target 2 holds no abort obligation — nothing authorizes a thaw"
  );
  let last_before = stores.0.get(&2).unwrap().0.last_index();

  // The constructed thaw naming the exact frozen incarnation is REFUSED with NO append — the invariant
  // `unfreeze(source) ⟹ ∃ committed target-abort(source, gen)` is structural, not advisory. Without
  // the gate the source-local checks all pass and the thaw APPENDS, unfreezing a source no target
  // abandoned.
  let result = {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.propose_merge_unfreeze(&2, now, log, stable, &1, 1)
  };
  assert!(
    matches!(result, Some(Err(MergeError::UnbackedThaw))),
    "a thaw with no committed target abort is refused: {result:?}"
  );
  assert_eq!(
    stores.0.get(&2).unwrap().0.last_index(),
    last_before,
    "the refused thaw appended nothing"
  );
  assert!(
    m.group(&2).unwrap().is_frozen(),
    "the source stays frozen — never thawed out from under a target that never abandoned it"
  );

  // NEGATIVE PIN: once target 2 commits a real abort, the SAME thaw is authorized and appends.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.rollback_merge(&1, now, log, stable, &2).unwrap().unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  let authorized = {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.propose_merge_unfreeze(&2, now, log, stable, &1, 1)
  };
  assert!(
    matches!(authorized, Some(Ok(_))),
    "the committed abort authorizes the thaw: {authorized:?}"
  );
}

/// INCARNATION SAFETY (the #22 race across a source remove/recreate): a target's `abandoned`
/// obligation is keyed by the source's LOCAL freeze gen, which a remove/recreate RESETS. Were the
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
    m.prepare_merge(&2, now, &mut stores, &1).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.rollback_merge(&1, now, log, stable, &2).unwrap().unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert_eq!(
    m.group(&1)
      .unwrap()
      .abandoned_obligations()
      .first()
      .map(|(_, m)| m.generation),
    Some(1),
    "target 2 owes source 1 a thaw for freeze gen 1"
  );

  // REMOVE source 1, non-terminally (no `MERGED_FLOOR`), and drop its store. The choke point purges
  // the obligation; without the purge the removed source's record strands on the target.
  assert!(m.remove_group(&2, &mut stores).unwrap().is_some());
  stores.0.remove(&2);
  assert!(
    !m.group(&1).unwrap().owes_live_thaw(),
    "the removal purged the target's obligation for the departed source"
  );

  // RECREATE source 1 at genesis — its LOCAL shape_gen resets to 0 — then elect and drain it.
  stores
    .0
    .insert(2, (VecLog::default(), AsyncStable::default()));
  m.create_group(2, 0, single_node_cfg(1), now, 7, CountSm::default())
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

  // RE-FREEZE the SAME pair: the reset counter mints gen 1 again — the exact value the OLD obligation
  // named, but a NEW incarnation the target never aborted.
  {
    m.prepare_merge(&2, now, &mut stores, &1).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    m.group(&2).unwrap().is_frozen() && m.group(&2).unwrap().shape_gen() == 1,
    "the recreated source froze at the repeated gen 1"
  );

  // The stale obligation must NOT authorize this incarnation's thaw: the derived-from-abort gate
  // finds no matching obligation and refuses with NO append. Without the purge the stale record
  // still matches `(1, 1)` and the thaw APPENDS, unfreezing a source no target abandoned.
  let last_before = stores.0.get(&2).unwrap().0.last_index();
  let result = {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.propose_merge_unfreeze(&2, now, log, stable, &1, 1)
  };
  assert!(
    matches!(result, Some(Err(MergeError::UnbackedThaw))),
    "the recreate's thaw is unbacked — the stale obligation was purged: {result:?}"
  );
  assert_eq!(
    stores.0.get(&2).unwrap().0.last_index(),
    last_before,
    "the refused thaw appended nothing"
  );

  // And the service — the only production driver of the thaw — leaves the recreate frozen: no target
  // owes this incarnation a thaw, so the target-log `k + 1` decider and the incarnation gate stand.
  m.service_merge_applies(now, &mut stores);
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    m.group(&2).unwrap().is_frozen(),
    "the recreated source stays frozen — the removed incarnation's abort can never thaw it"
  );
}

/// STRUCTURAL DEFENSE (liveness): an obligation RE-DERIVED after a restart (replayed from the target's
/// still-durable abort entry) for a source that was torn down and FLOORED — not terminally merged —
/// must still discharge, or that abort entry stays capture-fenced forever. The unhosted discharge binds
/// to the PERSISTED lineage/floor: a floor that no longer admits `expected` proves the frozen-at-
/// `expected` incarnation is gone for good. A discharge keyed on `floor == MERGED_FLOOR` cannot see
/// this — a non-terminal floor never equals the sentinel — and the re-derived obligation wedges the
/// target's compaction fence.
#[test]
fn a_floored_sources_rederived_obligation_discharges() {
  let (mut m, mut base) = merge_host(2, 3);
  let now = Instant::ORIGIN;
  // Freeze 1 -> 2 (gen 1) and abort: target 2 records the obligation; capture the abort index.
  {
    m.prepare_merge(&2, now, &mut base, &1).unwrap().unwrap();
    let (log, stable) = base.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  {
    let (log, stable) = base.0.get_mut(&1).unwrap();
    m.rollback_merge(&1, now, log, stable, &2).unwrap().unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  let abort_index = m.group(&1).unwrap().abandoned_obligations()[0]
    .1
    .abort_index;

  // Tear the source down (non-terminal) and drop its store. The removal purges the obligation (the
  // race-free leg), so re-INSERT it to model the restart replay that re-derives it from the surviving
  // abort entry while the source is gone.
  assert!(m.remove_group(&2, &mut base).unwrap().is_some());
  base.0.remove(&2);
  let mut key = Vec::new();
  Data::encode(&2u64, &mut key);
  m.group_mut(&1)
    .unwrap()
    .note_abandoned(Bytes::from(key), 1, abort_index);
  assert!(
    m.group(&1).unwrap().owes_live_thaw(),
    "the obligation is re-derived, as a restart replay would"
  );

  // The persisted floor fences the removed incarnation at gen 2 (> the freeze gen 1): a recreate can
  // only land above it, so the frozen-at-gen-1 incarnation is gone. The service discharges the
  // re-derived obligation off that record, lifting the target's compaction fence.
  let mut stores = LineageStores {
    inner: base,
    floors: std::collections::BTreeMap::from([(2u64, 2u64)]),
    lineages: std::collections::BTreeMap::new(),
  };
  m.service_merge_applies(now, &mut stores);
  assert!(
    !m.group(&1).unwrap().owes_live_thaw(),
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
/// Without the fence (no floor persisted on removal) the re-derived obligation is NOT discharged,
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
    m.prepare_merge(&2, now, &mut stores, &1).unwrap().unwrap();
    let (log, stable) = stores.inner.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  {
    let (log, stable) = stores.inner.0.get_mut(&1).unwrap();
    m.rollback_merge(&1, now, log, stable, &2).unwrap().unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  let abort_index = m.group(&1).unwrap().abandoned_obligations()[0]
    .1
    .abort_index;

  // THE REMOVAL-FLOOR DISCIPLINE, played as a driver's `remove_group` wiring does: one past the
  // source's OWN lineage — the freeze rode the unified counter, so no target scan is needed.
  // Persist it durably, THEN purge and drop the store.
  let fence = m.group_gen(&2).saturating_add(1);
  assert_eq!(
    fence, 2,
    "the frozen source's own lineage fences one past the freeze gen (1) it owes 2 a thaw for"
  );
  stores.floors.insert(2, fence);
  assert!(m.remove_group(&2, &mut stores).unwrap().is_some());
  stores.inner.0.remove(&2);

  // RESTART REPLAY: the still-durable abort entry re-derives the purged obligation while the source
  // is absent (modeled by re-inserting it, as `a_floored_sources_rederived_obligation_discharges`).
  let mut key = Vec::new();
  Data::encode(&2u64, &mut key);
  m.group_mut(&1)
    .unwrap()
    .note_abandoned(Bytes::from(key), 1, abort_index);
  assert!(
    m.group(&1).unwrap().owes_live_thaw(),
    "replay re-derived the obligation from the surviving abort entry"
  );

  // THE FIRST LEG: the durable floor discharges the re-derived obligation (the source is absent and
  // floored past `expected`), lifting the target's compaction fence. Without a floor persisted at
  // removal the fence never lifts.
  m.service_merge_applies(now, &mut stores);
  assert!(
    !m.group(&1).unwrap().owes_live_thaw(),
    "the removal-persisted floor discharged the re-derived obligation across the restart"
  );

  // RECREATE source 1 as a BELOW-FLOOR SQUATTER at genesis gen 0 — the floor-free container
  // admits what a coordinator's `validate_floor` would refuse. Deliberate adversarial belt: even
  // an improperly-admitted repeat incarnation must find no authorization left to ride.
  stores
    .inner
    .0
    .insert(2, (VecLog::default(), AsyncStable::default()));
  m.create_group(2, 0, single_node_cfg(1), now, 7, CountSm::default())
    .unwrap();
  {
    let (log, stable) = stores.inner.0.get_mut(&2).unwrap();
    let d = m.group(&2).unwrap().poll_timeout().unwrap();
    m.handle_timeout(&2, d, log, stable).unwrap();
    drain_storage(&mut m, 2, d, log, stable);
    assert!(m.group(&2).unwrap().role().is_leader());
  }
  while m.poll_message().is_some() {}
  while m.poll_event().is_some() {}

  // RE-FREEZE the SAME pair: the reset counter mints gen 1 again — the exact value the removed
  // incarnation's abort named, but a new incarnation 2 never abandoned.
  {
    m.prepare_merge(&2, now, &mut stores, &1).unwrap().unwrap();
    let (log, stable) = stores.inner.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    m.group(&2).unwrap().is_frozen() && m.group(&2).unwrap().shape_gen() == 1,
    "the recreated source froze at the repeated gen 1"
  );

  // THE SECOND LEG: no thaw appends. The re-derived obligation was discharged off the floor, so the
  // derived-from-abort gate finds no match and refuses with NO append; the service leaves the
  // recreate frozen. Without the fence the stale obligation still matches `(1, 1)` and the thaw
  // appends, unfreezing a source no target abandoned this incarnation.
  let last_before = stores.inner.0.get(&2).unwrap().0.last_index();
  let result = {
    let (log, stable) = stores.inner.0.get_mut(&2).unwrap();
    m.propose_merge_unfreeze(&2, now, log, stable, &1, 1)
  };
  assert!(
    matches!(result, Some(Err(MergeError::UnbackedThaw))),
    "the recreate's thaw is unbacked — the re-derived obligation discharged off the floor: {result:?}"
  );
  assert_eq!(
    stores.inner.0.get(&2).unwrap().0.last_index(),
    last_before,
    "the refused thaw appended nothing"
  );
  m.service_merge_applies(now, &mut stores);
  {
    let (log, stable) = stores.inner.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    m.group(&2).unwrap().is_frozen(),
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
/// Without the seed (a recreate at reset gen 0) the re-derived obligation survives the
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
    m.prepare_merge(&2, now, &mut stores, &1).unwrap().unwrap();
    let (log, stable) = stores.inner.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  {
    let (log, stable) = stores.inner.0.get_mut(&1).unwrap();
    m.rollback_merge(&1, now, log, stable, &2).unwrap().unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  let abort_index = m.group(&1).unwrap().abandoned_obligations()[0]
    .1
    .abort_index;

  // The removal-floor discipline off the UNIFIED counter: the frozen source's OWN lineage (1)
  // already covers the obligation's generation — the floor needs no target scan. Persist it,
  // then remove and drop the store.
  let floor = m.group_gen(&2).saturating_add(1);
  assert_eq!(
    floor, 2,
    "the frozen source's own lineage covers the obligation"
  );
  stores.floors.insert(2, floor);
  assert!(m.remove_group(&2, &mut stores).unwrap().is_some());
  stores.inner.0.remove(&2);

  // Crash-replay: the target's surviving abort entry re-derives the purged obligation.
  let mut key = Vec::new();
  Data::encode(&2u64, &mut key);
  m.group_mut(&1)
    .unwrap()
    .note_abandoned(Bytes::from(key), 1, abort_index);

  // RECREATE the source HOSTED, at the generation its floor admits, and elect it.
  assert!(
    crate::floor_admits(*stores.floors.get(&2).unwrap(), 2),
    "the recreate admits at its floor"
  );
  stores
    .inner
    .0
    .insert(2, (VecLog::default(), AsyncStable::default()));
  {
    let (log, stable) = stores.inner.0.get_mut(&2).unwrap();
    m.create_group_founded_at(
      2,
      2,
      single_node_cfg(1),
      now,
      7,
      CountSm::default(),
      1,
      &*log,
      stable,
    )
    .unwrap();
  }
  {
    let (log, stable) = stores.inner.0.get_mut(&2).unwrap();
    let d = m.group(&2).unwrap().poll_timeout().unwrap();
    m.handle_timeout(&2, d, log, stable).unwrap();
    drain_storage(&mut m, 2, d, log, stable);
    assert!(m.group(&2).unwrap().role().is_leader());
  }
  while m.poll_message().is_some() {}
  while m.poll_event().is_some() {}
  assert_eq!(
    m.group(&2).unwrap().shape_gen(),
    2,
    "the created counter starts at the admitted generation, not 0"
  );

  // THE HOSTED DISCHARGE: the live counter (2) is past the abandoned freeze (1), so the service
  // clears the re-derived obligation off the source's own lineage. Without the seed the hosted arm
  // reads 0 > 1 and the stale record survives.
  m.service_merge_applies(now, &mut stores);
  // The observing leader deferred its clear to the witness — apply it on the holder.
  {
    let (log, stable) = stores.inner.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    !m.group(&1).unwrap().owes_live_thaw(),
    "the seeded counter discharged the re-derived obligation on the hosted arm"
  );

  // The fresh freeze mints ABOVE the old expected — the recreate can never repeat gen 1.
  {
    m.prepare_merge(&2, now, &mut stores, &1).unwrap().unwrap();
    let (log, stable) = stores.inner.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    m.group(&2).unwrap().is_frozen() && m.group(&2).unwrap().shape_gen() == 3,
    "the re-freeze minted strictly above the removed incarnation's generations"
  );

  // And a stale drive naming the OLD incarnation is terminally refused with NO append.
  let last_before = stores.inner.0.get(&2).unwrap().0.last_index();
  let result = {
    let (log, stable) = stores.inner.0.get_mut(&2).unwrap();
    m.propose_merge_unfreeze(&2, now, log, stable, &1, 1)
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
    stores.inner.0.get(&2).unwrap().0.last_index(),
    last_before,
    "the refused thaw appended nothing"
  );
  assert!(
    m.group(&2).unwrap().is_frozen(),
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
/// Under a target-scan discipline (no hosted target owes the freeze's gen once the source is gone →
/// no floor) the re-derived obligation survives, the gen-0 recreate re-mints the repeated gen, and
/// the service's own drive thaws the new incarnation's freeze off the DEAD incarnation's abort — a
/// source unfrozen by an abort no live target ever issued for it.
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
    m.prepare_merge(&2, now, &mut stores, &1).unwrap().unwrap();
    let (log, stable) = stores.inner.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  {
    let (log, stable) = stores.inner.0.get_mut(&1).unwrap();
    m.rollback_merge(&1, now, log, stable, &2).unwrap().unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(m.group(&1).unwrap().owes_live_thaw());

  // Remove the SOURCE with the unified-counter discipline: its OWN lineage covers the freeze — the
  // removal floor is derived with NO knowledge of any target. Removing the source also PURGES the
  // still-hosted target's LIVE obligation for it (the synchronous fast path), but the target's
  // durable abort entry survives to re-derive it after a restore.
  let fence = m.group_gen(&2).saturating_add(1);
  assert_eq!(fence, 2, "the source's own counter carries the freeze");
  stores.floors.insert(2, fence);
  assert!(m.remove_group(&2, &mut stores).unwrap().is_some());
  stores.inner.0.remove(&2);
  assert!(
    !m.group(&1).unwrap().owes_live_thaw(),
    "removing the source purged the target's live obligation for it"
  );

  // The target now owes nothing, so the teardown gate admits it (self-cleared); its durable stores
  // survive. RESTORE it later: replaying the committed abort entry RE-DERIVES the obligation, now
  // with NO live source anywhere to observe a thaw.
  assert!(m.remove_group(&1, &mut stores).unwrap().is_some());
  {
    let (log, stable) = stores.inner.0.get_mut(&1).unwrap();
    m.restore_group_unchecked(
      1,
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
    m.group(&1).unwrap().owes_live_thaw(),
    "the restored target re-derived the obligation from its durable abort entry"
  );

  // THE DISCHARGE, off the source's own floor — no target scan produced it, and no live source
  // exists to observe: `!floor_admits(2, 1)` proves the frozen-at-1 incarnation is gone forever.
  m.service_merge_applies(now, &mut stores);
  assert!(
    !m.group(&1).unwrap().owes_live_thaw(),
    "the re-derived obligation discharged off the source's removal floor"
  );

  // RECREATE the source at its admitted generation and re-freeze the SAME pair: the fresh
  // freeze mints above every generation the dead incarnation ever named.
  stores
    .inner
    .0
    .insert(2, (VecLog::default(), AsyncStable::default()));
  assert!(crate::floor_admits(*stores.floors.get(&2).unwrap(), 2));
  {
    let (log, stable) = stores.inner.0.get_mut(&2).unwrap();
    m.create_group_founded_at(
      2,
      2,
      single_node_cfg(1),
      now,
      7,
      CountSm::default(),
      1,
      &*log,
      stable,
    )
    .unwrap();
  }
  {
    let (log, stable) = stores.inner.0.get_mut(&2).unwrap();
    let d = m.group(&2).unwrap().poll_timeout().unwrap();
    m.handle_timeout(&2, d, log, stable).unwrap();
    drain_storage(&mut m, 2, d, log, stable);
    assert!(m.group(&2).unwrap().role().is_leader());
  }
  while m.poll_message().is_some() {}
  while m.poll_event().is_some() {}
  {
    m.prepare_merge(&2, now, &mut stores, &1).unwrap().unwrap();
    let (log, stable) = stores.inner.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    m.group(&2).unwrap().is_frozen() && m.group(&2).unwrap().shape_gen() == 3,
    "the recreate's freeze minted above the dead incarnation"
  );

  // NO thaw is backed: the discharged obligation authorizes nothing (UnbackedThaw at the new
  // gen; StaleThaw at the old), and the service leaves the new freeze standing.
  let last_before = stores.inner.0.get(&2).unwrap().0.last_index();
  let unbacked = {
    let (log, stable) = stores.inner.0.get_mut(&2).unwrap();
    m.propose_merge_unfreeze(&2, now, log, stable, &1, 3)
  };
  assert!(
    matches!(unbacked, Some(Err(MergeError::UnbackedThaw))),
    "no committed abort backs the new incarnation's thaw: {unbacked:?}"
  );
  let stale = {
    let (log, stable) = stores.inner.0.get_mut(&2).unwrap();
    m.propose_merge_unfreeze(&2, now, log, stable, &1, 1)
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
    let (log, stable) = stores.inner.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert_eq!(
    stores.inner.0.get(&2).unwrap().0.last_index(),
    last_before,
    "nothing appended a thaw"
  );
  assert!(
    m.group(&2).unwrap().is_frozen(),
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
  m.prepare_merge(&2, now, &mut engine, &1).unwrap().unwrap();
  engine_crank(&mut m, &mut engine, 2, now);
  fold_lineage_events(&mut m, &mut engine);
  assert!(m.group(&2).unwrap().is_frozen());
  assert_eq!(engine.group_gen(&2), 1, "the freeze mirrored eagerly");
  assert_inv_lineage(&m, &engine);

  // TARGET-role ABORT on 2: the target's counter moves; the MergeAborted fold mirrors it.
  {
    let (log, stable) = engine.stores(&1).unwrap();
    m.rollback_merge(&1, now, log, stable, &2).unwrap().unwrap();
  }
  engine_crank(&mut m, &mut engine, 1, now);
  fold_lineage_events(&mut m, &mut engine);
  assert_eq!(engine.group_gen(&1), 1, "the abort mint mirrored eagerly");
  assert_inv_lineage(&m, &engine);

  // The service drives the SOURCE thaw; the MergeRolledBack fold mirrors it, and the follow-up
  // crank discharges the obligation off the observed advance.
  m.service_merge_applies(now, &mut engine);
  engine_crank(&mut m, &mut engine, 2, now);
  fold_lineage_events(&mut m, &mut engine);
  assert!(!m.group(&2).unwrap().is_frozen(), "thawed");
  assert_eq!(engine.group_gen(&2), 2, "the thaw mirrored eagerly");
  assert_inv_lineage(&m, &engine);
  m.service_merge_applies(now, &mut engine);
  // The observing leader deferred its clear to the witness — apply it on the holder (no lineage move).
  engine_crank(&mut m, &mut engine, 1, now);
  assert!(!m.group(&1).unwrap().owes_live_thaw());

  // RE-FREEZE and ABSORB: park, seal, resolve — the Merged fold mirrors the target's bump.
  m.prepare_merge(&2, now, &mut engine, &1).unwrap().unwrap();
  engine_crank(&mut m, &mut engine, 2, now);
  fold_lineage_events(&mut m, &mut engine);
  assert_inv_lineage(&m, &engine);
  {
    let (log, stable) = engine.stores(&1).unwrap();
    m.commit_merge(&1, now, log, stable, &2).unwrap().unwrap();
  }
  engine_crank(&mut m, &mut engine, 1, now);
  assert!(m.group(&1).unwrap().pending_merge().is_some(), "parked");
  // First service pass seals the abort window; the crank commits the seal; the second resolves.
  let mut resolutions = m.service_merge_applies(now, &mut engine);
  engine_crank(&mut m, &mut engine, 1, now);
  resolutions.extend(m.service_merge_applies(now, &mut engine));
  assert_eq!(
    resolutions,
    std::vec![MergeResolution::Merged {
      source: 2,
      target: 1
    }]
  );
  // The driver's storage half of a Merged resolution, then the fold: floor + teardown.
  engine.set_group_floor(&2, MERGED_FLOOR);
  engine.remove_group(&2);
  engine_crank(&mut m, &mut engine, 1, now);
  fold_lineage_events(&mut m, &mut engine);
  assert!(!m.contains_group(&2), "the source was absorbed");
  assert_eq!(engine.group_gen(&1), 2, "the absorb mirrored eagerly");
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
  m.prepare_merge(&2, now, &mut engine, &1).unwrap().unwrap();
  engine_crank(&mut m, &mut engine, 2, now);
  assert!(m.group(&2).unwrap().is_frozen());
  // THE CRASH: the freeze is durable in the source's log, but its event-time mirror is LOST —
  // the events die undrained with the process, the engine record still reads 0.
  drop(m);
  assert_eq!(engine.group_gen(&2), 0, "the mirror never landed");

  // RESTORE from the surviving engine stores; replay re-freezes the endpoint (no events), and
  // the driver's re-sync folds the LIVE counter back into the engine record.
  let mut m2: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let epoch = engine.next_boot_epoch(&2).unwrap();
  {
    let (log, stable) = engine.stores(&2).unwrap();
    m2.restore_group_unchecked(
      2,
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
  let live = m2.group(&2).unwrap().shape_gen();
  assert_eq!(live, 1, "replay re-derived the freeze bump");
  engine.set_group_gen(&2, live);
  assert_eq!(
    m2.group(&2).unwrap().shape_gen(),
    engine.group_gen(&2),
    "the re-sync healed INV-LINEAGE across the crash"
  );

  // The removal now floors ABOVE the obligation's expected gen (1) — the crash window is shut.
  assert_eq!(engine.removal_floor(&2), 2);
}

/// THE STORES-ONLY CRASH WINDOW (the ceiling helper's log and meta legs): the group is never
/// re-hosted after the crash, so no restore re-sync runs — the removal must derive its floor
/// from the stores alone. Every lineage move rides the group's own log (the shape-kind entries
/// carry the generation they set) or its snapshot meta, so the ceiling covers the un-mirrored
/// freeze with no target knowledge. A record-only discipline (`group_gen + 1`) reads 0 when the
/// mirror is lost — no floor at all.
#[test]
fn removal_floor_reads_the_stores_when_the_mirror_never_landed() {
  let mut engine: GroupEngine<u64, u64> = GroupEngine::new();
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let now = Instant::ORIGIN;
  engine_group(&mut m, &mut engine, 1, 0, now);
  engine_group(&mut m, &mut engine, 2, 0, now);
  m.prepare_merge(&2, now, &mut engine, &1).unwrap().unwrap();
  engine_crank(&mut m, &mut engine, 2, now);
  assert!(m.group(&2).unwrap().is_frozen());
  drop(m);
  assert_eq!(engine.group_gen(&2), 0, "the mirror never landed");

  // THE LOG LEG: the durable PrepareMerge entry names source_gen_after = 1 → ceiling 1 → floor 2.
  assert_eq!(
    engine.removal_floor(&2),
    2,
    "the log scan floors one past the un-mirrored freeze"
  );

  // THE META LEG: once compaction folds the entries away, the snapshot meta carries the lineage
  // (every capture stamps the live counter) — model a post-freeze snapshot boundary and re-check.
  {
    let (log, stable) = engine.stores(&2).unwrap();
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
    engine.removal_floor(&2),
    2,
    "the meta leg floors one past the un-mirrored freeze once the log is compacted away"
  );
}

/// One way a `PrepareMerge` fails the apply path's own admission — the forms a hosted replica can
/// meet in its log, none of which a conforming leader mints: the target id EMPTY (the decoder
/// refuses the group-tag bound), a valid encode cut short (a torn record), and a well-formed
/// payload whose `source_gen_after` sits in the reserved band.
#[derive(Debug, Clone, Copy)]
enum RefusedFreeze {
  EmptyTarget,
  TruncatedEncoding,
  ReservedSourceGen,
}

const REFUSED_FREEZE_FORMS: [RefusedFreeze; 3] = [
  RefusedFreeze::EmptyTarget,
  RefusedFreeze::TruncatedEncoding,
  RefusedFreeze::ReservedSourceGen,
];

impl RefusedFreeze {
  /// The poison the apply arm answers this form with.
  fn poison(self) -> PoisonReason {
    match self {
      Self::EmptyTarget | Self::TruncatedEncoding => PoisonReason::MergeDecode,
      Self::ReservedSourceGen => PoisonReason::ReservedShapeGen,
    }
  }
}

/// A term-1 `PrepareMerge` at `index` the apply path will refuse, naming `target` where the form
/// carries a target at all. `source_gen_after` is nonzero in every form so the truncated encode
/// loses a byte of a field that is PRESENT — cut from an absent trailing field, the bytes would
/// still decode.
fn refused_prepare_merge(form: RefusedFreeze, index: u64, target: u64) -> crate::Entry {
  let mut target_bytes = Vec::new();
  Data::encode(&target, &mut target_bytes);
  let target_bytes = Bytes::from(target_bytes);
  let payload = match form {
    RefusedFreeze::EmptyTarget => crate::PrepareMergePayload::new(Bytes::new(), 1),
    RefusedFreeze::TruncatedEncoding => crate::PrepareMergePayload::new(target_bytes, 1),
    RefusedFreeze::ReservedSourceGen => {
      crate::PrepareMergePayload::new(target_bytes, crate::HIGHEST_WORKING_GENERATION)
    }
  };
  let mut data = Vec::new();
  crate::wire::encode_prepare_merge_payload(&payload, &mut data);
  if matches!(form, RefusedFreeze::TruncatedEncoding) {
    data.truncate(data.len() - 1);
  }
  let entry = crate::Entry::new(
    Term::new(1),
    Index::new(index),
    crate::EntryKind::PrepareMerge,
    Bytes::from(data),
  );
  assert!(
    matches!(shape_entry_move(&entry), ShapeMove::Invalid(_)),
    "{form:?}: the fixture must be an entry the apply path refuses"
  );
  entry
}

/// Node 1 with phantom peer 2 as the other voter: never elected, so the replica's log is fed by
/// `AppendEntries` alone.
fn follower_cfg() -> Config<u64> {
  Config::try_new(
    1u64,
    std::vec![1u64, 2],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
}

/// Admit one engine-hosted group whose local replica FOLLOWS phantom peer 2 — the only way an
/// entry no conforming leader would mint reaches a hosted replica through the engine's own append
/// path, where the removal ceiling's fault latch folds.
fn engine_follower(
  m: &mut MultiRaft<u64, u64, CountSm>,
  engine: &mut GroupEngine<u64, u64>,
  gid: u64,
  now: Instant,
) {
  assert!(engine.add_group(gid));
  m.create_group(gid, 0, follower_cfg(), now, 7, CountSm::default())
    .unwrap();
  engine.set_group_gen(&gid, 0);
  assert!(m.group(&gid).unwrap().role().is_follower());
}

/// Deliver one `AppendEntries` from phantom leader 2 at `term` to engine-hosted follower `gid` —
/// `entries` after `(prev, prev_term)`, the commit advertised at `leader_commit` — WITHOUT
/// cranking: the follower's own message-path work runs (the append, the commit advance, the apply
/// drain up to its budget) and nothing else, which is how a fixture parks a replica mid-drain.
#[allow(clippy::too_many_arguments)]
fn engine_follower_deliver(
  m: &mut MultiRaft<u64, u64, CountSm>,
  engine: &mut GroupEngine<u64, u64>,
  gid: u64,
  term: Term,
  prev: Index,
  prev_term: Term,
  entries: Vec<crate::Entry>,
  leader_commit: Index,
  now: Instant,
) {
  let (log, stable) = engine.stores(&gid).unwrap();
  m.handle_message(
    &gid,
    now,
    log,
    stable,
    2u64,
    Message::AppendEntries(crate::AppendEntries::new(
      term,
      2u64,
      prev,
      prev_term,
      entries,
      leader_commit,
    )),
  )
  .unwrap();
}

/// [`engine_follower_deliver`], then a crank of the engine.
#[allow(clippy::too_many_arguments)]
fn engine_follower_append(
  m: &mut MultiRaft<u64, u64, CountSm>,
  engine: &mut GroupEngine<u64, u64>,
  gid: u64,
  term: Term,
  prev: Index,
  prev_term: Term,
  entries: Vec<crate::Entry>,
  leader_commit: Index,
  now: Instant,
) {
  engine_follower_deliver(
    m,
    engine,
    gid,
    term,
    prev,
    prev_term,
    entries,
    leader_commit,
    now,
  );
  engine_crank(m, engine, gid, now);
}

/// ONE driver crank of an engine-hosted group — a barrier, then a single `handle_storage`, which
/// re-drives the apply backlog by exactly one budget: the granularity a fixture uses to park a
/// replica between two committed entries.
fn engine_step(
  m: &mut MultiRaft<u64, u64, CountSm>,
  engine: &mut GroupEngine<u64, u64>,
  gid: u64,
  now: Instant,
) {
  engine.flush();
  let (log, stable) = engine.stores(&gid).unwrap();
  let _ = m.handle_storage(&gid, now, log, stable);
}

/// Ordinary term-1 `Normal` entries `[from, to]`, one `CountSm` command each — the load that
/// exhausts one apply drain's budget, so an entry delivered committed behind it is COMMITTED but
/// not yet APPLIED when the drain cuts.
fn normal_load(from: u64, to: u64) -> Vec<crate::Entry> {
  let cmd = {
    let mut buf = Vec::new();
    Bytes::from_static(b"c").encode(&mut buf);
    Bytes::from(buf)
  };
  (from..=to)
    .map(|i| {
      crate::Entry::new(
        Term::new(1),
        Index::new(i),
        crate::EntryKind::Normal,
        cmd.clone(),
      )
    })
    .collect()
}

/// A VALID `PrepareMerge` at `index` in `term`, naming `target` and moving the source's counter to
/// `source_gen_after` — what a conforming leader mints.
fn valid_prepare_merge(term: Term, index: u64, target: u64, source_gen_after: u64) -> crate::Entry {
  let payload = crate::PrepareMergePayload::new(gid_key(target), source_gen_after);
  let mut data = Vec::new();
  crate::wire::encode_prepare_merge_payload(&payload, &mut data);
  let entry = crate::Entry::new(
    term,
    Index::new(index),
    crate::EntryKind::PrepareMerge,
    Bytes::from(data),
  );
  assert_eq!(shape_entry_move(&entry), ShapeMove::Valid(source_gen_after));
  entry
}

/// A term-1 SOURCE-side unfreeze at `index`, moving the source's counter to `source_gen_after`.
fn unfreeze_entry(index: u64, source_gen_after: u64) -> crate::Entry {
  let payload = crate::RollbackMergePayload::unfreeze(source_gen_after);
  let mut data = Vec::new();
  crate::wire::encode_rollback_merge_payload(&payload, &mut data);
  crate::Entry::new(
    Term::new(1),
    Index::new(index),
    crate::EntryKind::RollbackMerge,
    Bytes::from(data),
  )
}

/// A COMMITTED `PrepareMerge` the apply path refuses leaves its source POISONED and REMOVABLE,
/// under the release cap. The kind-only append arm armed the lease kill before anything judged
/// the payload; the apply arm then refuses the entry — a payload that will not decode, or a
/// reserved `source_gen_after` — and must release that kill as it poisons. Left armed, the
/// container's teardown gate reads the poisoned replica as an active merge source and refuses
/// `Frozen` for as long as the committed entry sits in the log, which is forever: the operator's
/// one recovery, removing the id under the ceiling's release cap, is exactly the door that stays
/// shut. The cap is read BEFORE the removal, in the order every driver runs (coordinator gate,
/// then floor, then the floor write): `HIGHEST_WORKING_GENERATION`, admitting no working
/// generation and never the terminal. The negative pin — a VALID committed freeze still refuses
/// `Frozen` — is `teardown_refuses_a_frozen_source_and_a_parked_target`.
#[test]
fn a_refused_prepare_merge_leaves_its_poisoned_source_removable_under_the_cap() {
  let mut fenced: Vec<(RefusedFreeze, Result<bool, RemoveError>)> = Vec::new();
  for form in REFUSED_FREEZE_FORMS {
    let mut engine: GroupEngine<u64, u64> = GroupEngine::new();
    let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
    let now = Instant::ORIGIN;
    engine_follower(&mut m, &mut engine, 2, now);
    // The entry reaches the engine's log through the follower's append path — the fault latch
    // folds there — and the advertised commit drives it to apply.
    engine_follower_append(
      &mut m,
      &mut engine,
      2,
      Term::new(1),
      Index::ZERO,
      Term::ZERO,
      std::vec![refused_prepare_merge(form, 1, 1)],
      Index::new(1),
      now,
    );
    let ep = m.group(&2).unwrap();
    assert_eq!(
      ep.poison_reason(),
      Some(form.poison()),
      "{form:?}: the committed entry is refused at apply"
    );
    assert!(!ep.is_frozen(), "{form:?}: a refused freeze never folds");
    // THE CAP, read before the removal as the drivers do.
    let floor = engine.removal_floor(&2);
    assert_eq!(
      floor,
      crate::HIGHEST_WORKING_GENERATION,
      "{form:?}: the resident refused entry caps the removal fence at the release cap"
    );
    assert_ne!(
      floor, MERGED_FLOOR,
      "{form:?}: the cap is never the terminal"
    );
    // THE RULE: a refused entry armed no freeze, so the teardown gate has nothing to refuse.
    // COLLECTED, not asserted in place: every form must show failing on a red run, and a loop
    // that stopped at the first would look identical whether one form wedged or all of them.
    let removed = m.remove_group(&2, &mut engine).map(|ep| ep.is_some());
    if removed != Ok(true) {
      fenced.push((form, removed));
    }
  }
  assert!(
    fenced.is_empty(),
    "these refused freezes fenced their poisoned source instead of releasing it: {fenced:?}"
  );
}

/// A COMMITTED but not yet APPLIED `PrepareMerge` the apply path will refuse CLAIMS NO TARGET —
/// the claim scan's committed arm. Source 2's freeze rides an `AppendEntries` whose commit covers
/// it behind exactly one apply budget of ordinary load, so the follower's commit advance applies
/// the load, cuts the drain, and leaves the entry committed, unapplied, and judged by nothing
/// (freeze-pending, not frozen, not poisoned). The teardown gate's claim leg reads the payload off
/// 2's log: a committed refused entry can only poison 2 when the drain resumes, so no claim can
/// ever materialize from it, and `remove_group(&1)` admits — the resumed drain then refuses the
/// entry and poisons 2, confirming the answer. The budget cut is how the harness reaches the
/// window (a commit delivered within budget applies in the same step); the product reaches it the
/// same way, or through a cold committed read. The uncommitted twin refuses —
/// `an_uncommitted_refused_prepare_merge_still_fences_its_target`.
#[test]
fn a_refused_pending_prepare_merge_claims_no_target() {
  let load = crate::endpoint::APPLY_BUDGET_ENTRIES;
  let freeze_at = Index::new(load + 1);
  let mut fenced: Vec<(RefusedFreeze, Result<bool, RemoveError>)> = Vec::new();
  for form in REFUSED_FREEZE_FORMS {
    let mut engine: GroupEngine<u64, u64> = GroupEngine::new();
    let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
    let now = Instant::ORIGIN;
    engine_group(&mut m, &mut engine, 1, 0, now);
    engine_follower(&mut m, &mut engine, 2, now);
    let mut entries = normal_load(1, load);
    entries.push(refused_prepare_merge(form, load + 1, 1));
    // Delivered committed and NOT cranked: the drain applies the load, cuts at its budget, and
    // leaves the freeze committed but unapplied.
    engine_follower_deliver(
      &mut m,
      &mut engine,
      2,
      Term::new(1),
      Index::ZERO,
      Term::ZERO,
      entries,
      freeze_at,
      now,
    );
    let source = m.group(&2).unwrap();
    assert_eq!(
      (source.commit_index(), source.applied_index()),
      (freeze_at, Index::new(load)),
      "{form:?}: the drain cut at its budget with the freeze committed and unapplied"
    );
    assert!(
      source.merge_freeze_active() && !source.is_frozen() && !source.is_poisoned(),
      "{form:?}: the source is freeze-pending, unapplied, unjudged"
    );
    // COLLECTED, not asserted in place, so a red run shows every form that fenced the target.
    let removed = m.remove_group(&1, &mut engine).map(|ep| ep.is_some());
    if removed != Ok(true) {
      fenced.push((form, removed));
      continue;
    }
    // The resumed drain refuses the entry: the gate's answer is the one apply confirms.
    engine_crank(&mut m, &mut engine, 2, now);
    assert_eq!(
      m.group(&2).unwrap().poison_reason(),
      Some(form.poison()),
      "{form:?}: the committed entry was refused once the drain reached it"
    );
  }
  assert!(
    fenced.is_empty(),
    "these committed refused freezes fenced the target they can never absorb into: {fenced:?}"
  );
}

/// An UNCOMMITTED `PrepareMerge` the apply path will refuse is NOT "no claim" — it is a claim
/// the gate cannot rule out yet. Above the source's commit the entry has no fixed fate: a
/// conflicting append from a newer leader may truncate it and put a VALID freeze at the same
/// index, re-arming the kill and naming a target, and a teardown taken on "no claim" would
/// already have removed that target — the claimant then freezes for a log that no longer exists,
/// with no home for its commit or its rollback. So the claim scan fails closed on it exactly as
/// on an unreadable page, and `remove_group(&1)` refuses `Claimed`. Once the leader's commit
/// reaches the entry its fate is fixed: the drain refuses it, poisons 2, releases the kill, and
/// the SAME removal admits.
#[test]
fn an_uncommitted_refused_prepare_merge_still_fences_its_target() {
  let mut admitted: Vec<(RefusedFreeze, Result<bool, RemoveError>)> = Vec::new();
  for form in REFUSED_FREEZE_FORMS {
    let mut engine: GroupEngine<u64, u64> = GroupEngine::new();
    let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
    let now = Instant::ORIGIN;
    engine_group(&mut m, &mut engine, 1, 0, now);
    engine_follower(&mut m, &mut engine, 2, now);
    // Appended, not committed: the freeze is pending and the entry's fate is open.
    engine_follower_append(
      &mut m,
      &mut engine,
      2,
      Term::new(1),
      Index::ZERO,
      Term::ZERO,
      std::vec![refused_prepare_merge(form, 1, 1)],
      Index::ZERO,
      now,
    );
    let source = m.group(&2).unwrap();
    assert!(
      source.merge_freeze_active() && !source.is_frozen() && !source.is_poisoned(),
      "{form:?}: the source is freeze-pending, uncommitted, unjudged"
    );
    // COLLECTED, not asserted in place, so a red run shows every form that was read as no claim.
    let removed = m.remove_group(&1, &mut engine).map(|ep| ep.is_some());
    if removed != Err(RemoveError::Claimed) {
      admitted.push((form, removed));
      continue;
    }
    assert!(
      m.contains_group(&1),
      "{form:?}: the refusal left the target hosted"
    );
    // The commit reaches the entry: refused at apply, the source poisons and the kill releases.
    engine_follower_append(
      &mut m,
      &mut engine,
      2,
      Term::new(1),
      Index::new(1),
      Term::new(1),
      std::vec![],
      Index::new(1),
      now,
    );
    let source = m.group(&2).unwrap();
    assert_eq!(
      source.poison_reason(),
      Some(form.poison()),
      "{form:?}: the committed entry was refused at apply"
    );
    assert!(
      !source.merge_freeze_active(),
      "{form:?}: the refusal released the kill"
    );
    assert_eq!(
      m.remove_group(&1, &mut engine).map(|ep| ep.is_some()),
      Ok(true),
      "{form:?}: committed and refused, the entry fences nothing"
    );
  }
  assert!(
    admitted.is_empty(),
    "these uncommitted refused freezes were read as no claim: {admitted:?}"
  );
}

/// THE REPLACEMENT RACE the uncommitted refusal guards against, run to its end. Source 2 holds an
/// uncommitted refused `PrepareMerge` at index 1 (term 1); a newer leader's conflicting append
/// truncates it and puts a VALID freeze naming target 1 at the same index (term 2). Had the gate
/// read the refused entry as "no claim", a removal of 1 taken on it would have stood, and the
/// replacement's claim — the one that actually materializes — would name a target that no longer
/// exists. The gate refuses 1 throughout: on the uncommitted refused entry (fail-closed), on the
/// valid pending claim (decoded), and once the freeze commits and applies (the applied leg); 2
/// freezes normally, refused `Frozen` at its own removal with its counter moved. The engine's
/// fence follows the log: the release cap while the refused entry is resident, one past the valid
/// freeze's generation once it is replaced.
#[test]
fn a_replaced_refused_prepare_merge_claims_the_target_its_replacement_names() {
  let mut engine: GroupEngine<u64, u64> = GroupEngine::new();
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let now = Instant::ORIGIN;
  engine_group(&mut m, &mut engine, 1, 0, now);
  engine_follower(&mut m, &mut engine, 2, now);
  engine_follower_append(
    &mut m,
    &mut engine,
    2,
    Term::new(1),
    Index::ZERO,
    Term::ZERO,
    std::vec![refused_prepare_merge(RefusedFreeze::EmptyTarget, 1, 1)],
    Index::ZERO,
    now,
  );
  assert_eq!(
    engine.removal_floor(&2),
    crate::HIGHEST_WORKING_GENERATION,
    "the resident refused entry caps the fence"
  );
  assert_eq!(
    m.remove_group(&1, &mut engine).map(|ep| ep.is_some()),
    Err(RemoveError::Claimed),
    "uncommitted and refused, the entry is a claim the gate cannot rule out"
  );

  // The newer leader's conflicting append: the refused entry is truncated away and a valid freeze
  // naming 1 takes its index.
  let valid = valid_prepare_merge(Term::new(2), 1, 1, 1);
  engine_follower_append(
    &mut m,
    &mut engine,
    2,
    Term::new(2),
    Index::ZERO,
    Term::ZERO,
    std::vec![valid],
    Index::ZERO,
    now,
  );
  let source = m.group(&2).unwrap();
  assert!(
    source.merge_freeze_active() && !source.is_frozen() && !source.is_poisoned(),
    "the replacement re-armed the pending freeze"
  );
  assert_eq!(
    engine.removal_floor(&2),
    2,
    "the fence retracted with the discarded entry and folds the valid freeze's move instead"
  );
  assert_eq!(
    m.remove_group(&1, &mut engine).map(|ep| ep.is_some()),
    Err(RemoveError::Claimed),
    "the replacement's claim is real, decoded off the pending entry"
  );

  // Commit and apply: the freeze folds, and both participants are refused through the applied
  // legs.
  engine_follower_append(
    &mut m,
    &mut engine,
    2,
    Term::new(2),
    Index::new(1),
    Term::new(2),
    std::vec![],
    Index::new(1),
    now,
  );
  let source = m.group(&2).unwrap();
  assert!(
    source.is_frozen() && !source.is_poisoned(),
    "the source froze normally"
  );
  assert_eq!(
    source.shape_gen(),
    1,
    "the freeze moved the lineage counter"
  );
  assert_eq!(
    m.remove_group(&1, &mut engine).map(|ep| ep.is_some()),
    Err(RemoveError::Claimed),
    "the applied claim keeps refusing the target"
  );
  assert_eq!(
    m.remove_group(&2, &mut engine).map(|ep| ep.is_some()),
    Err(RemoveError::Frozen),
    "a valid frozen source is refused Frozen — leg 2 unchanged"
  );
}

/// `frozen` AND `freeze_pending` AT ONCE — the lagging replica the claim gate must read both of.
/// Source 9 applied a freeze naming target 1; behind three apply budgets of load sit a committed
/// unfreeze and, above it, a `PrepareMerge` naming target 3 — first a refused uncommitted entry
/// at that index, then its valid replacement from a newer leader (every delivery re-drives one
/// budget of the committed backlog, so the thaw must still be ahead after the third). The pending
/// kill is armed by
/// kind while `is_frozen()` is still true, so a gate that ran the pending scan only for an
/// UNFROZEN source never read the claim on 3: the applied leg saw only 1, and both doors on 3
/// stood open — `remove_group(&3)` and a freeze of 3 as another merge's SOURCE — until the drain
/// caught up and 9 was frozen for a target already gone or dissolving. Keyed on the pending
/// freeze alone, both doors refuse on the pending claim; once the follower drains (the unfreeze
/// thaws 9 and re-derives the kill, the replacement folds) they refuse on the applied claim, and
/// target 1's claim is released with the thaw.
#[test]
fn a_pending_claim_behind_an_applied_freeze_is_still_read() {
  let budget = crate::endpoint::APPLY_BUDGET_ENTRIES;
  let load = 3 * budget;
  let unfreeze_at = load + 2;
  let refreeze_at = load + 3;
  let mut engine: GroupEngine<u64, u64> = GroupEngine::new();
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let now = Instant::ORIGIN;
  engine_group(&mut m, &mut engine, 1, 0, now);
  engine_group(&mut m, &mut engine, 3, 0, now);
  engine_group(&mut m, &mut engine, 2, 0, now);
  engine_follower(&mut m, &mut engine, 9, now);
  // The old freeze, applied: 9 is frozen for 1.
  engine_follower_append(
    &mut m,
    &mut engine,
    9,
    Term::new(1),
    Index::ZERO,
    Term::ZERO,
    std::vec![valid_prepare_merge(Term::new(1), 1, 1, 1)],
    Index::new(1),
    now,
  );
  let source = m.group(&9).unwrap();
  assert!(
    source.is_frozen() && source.frozen_for() == Some(&gid_key(1)),
    "9 applied its freeze for 1"
  );
  // Behind the budgets: the load, then the committed unfreeze — delivered, not cranked, so this
  // delivery's drain cuts one budget in, far short of the thaw.
  let mut entries = normal_load(2, load + 1);
  entries.push(unfreeze_entry(unfreeze_at, 2));
  engine_follower_deliver(
    &mut m,
    &mut engine,
    9,
    Term::new(1),
    Index::new(1),
    Term::new(1),
    entries,
    Index::new(unfreeze_at),
    now,
  );
  let source = m.group(&9).unwrap();
  assert_eq!(
    (source.commit_index(), source.applied_index()),
    (Index::new(unfreeze_at), Index::new(1 + budget)),
    "the drain cut with the unfreeze committed and unapplied"
  );
  assert!(
    source.is_frozen() && source.freeze_pending().is_none(),
    "still frozen for 1, nothing pending yet"
  );
  // The new freeze's index first carries a refused uncommitted entry, then its valid replacement
  // from a newer leader, naming 3.
  engine_follower_deliver(
    &mut m,
    &mut engine,
    9,
    Term::new(1),
    Index::new(unfreeze_at),
    Term::new(1),
    std::vec![refused_prepare_merge(
      RefusedFreeze::EmptyTarget,
      refreeze_at,
      3
    )],
    Index::new(unfreeze_at),
    now,
  );
  engine_follower_deliver(
    &mut m,
    &mut engine,
    9,
    Term::new(2),
    Index::new(unfreeze_at),
    Term::new(1),
    std::vec![valid_prepare_merge(Term::new(2), refreeze_at, 3, 3)],
    Index::new(unfreeze_at),
    now,
  );
  let source = m.group(&9).unwrap();
  assert_eq!(
    (source.commit_index(), source.applied_index()),
    (Index::new(unfreeze_at), Index::new(unfreeze_at - 1)),
    "two more deliveries re-drove two more budgets; the thaw is the next entry to apply"
  );
  assert!(
    source.is_frozen() && source.freeze_pending() == Some(Index::new(refreeze_at)),
    "frozen for 1 AND freeze-pending toward 3, at once"
  );
  // BOTH DOORS on 3 refuse on the pending claim, read past the applied freeze on 1.
  assert_eq!(
    m.remove_group(&3, &mut engine).map(|ep| ep.is_some()),
    Err(RemoveError::Claimed),
    "the pending claim on 3 is read past the applied freeze on 1"
  );
  assert!(
    matches!(
      m.prepare_merge(&3, now, &mut engine, &2),
      Some(Err(MergeError::SourceClaimedAsTarget))
    ),
    "the freeze door reads the same pending claim"
  );
  // The follower drains: the unfreeze thaws 9 and re-derives the kill, the replacement folds.
  engine_follower_append(
    &mut m,
    &mut engine,
    9,
    Term::new(2),
    Index::new(refreeze_at),
    Term::new(2),
    std::vec![],
    Index::new(refreeze_at),
    now,
  );
  let source = m.group(&9).unwrap();
  assert!(
    source.is_frozen()
      && source.frozen_for() == Some(&gid_key(3))
      && source.freeze_pending().is_none(),
    "9 is frozen for 3"
  );
  assert_eq!(
    source.shape_gen(),
    3,
    "thaw and re-freeze both moved the counter"
  );
  assert_eq!(
    m.remove_group(&3, &mut engine).map(|ep| ep.is_some()),
    Err(RemoveError::Claimed),
    "the applied claim on 3 keeps the teardown door shut"
  );
  assert!(
    matches!(
      m.prepare_merge(&3, now, &mut engine, &2),
      Some(Err(MergeError::SourceClaimedAsTarget))
    ),
    "and the freeze door"
  );
  assert_eq!(
    m.remove_group(&1, &mut engine).map(|ep| ep.is_some()),
    Ok(true),
    "target 1's claim was released by the thaw"
  );
}

/// A `GroupStores` over the engine that answers `None` for one hosted group — the starved shape
/// the trait's contract names ("returning `None` for a hosted group starves it").
struct Starved<'a> {
  engine: &'a mut GroupEngine<u64, u64>,
  withheld: u64,
}

impl crate::GroupStores<u64, EngineLog, EngineStable<u64>> for Starved<'_> {
  fn stores(&mut self, group: &u64) -> Option<(&mut EngineLog, &mut EngineStable<u64>)> {
    if *group == self.withheld {
      None
    } else {
      self.engine.stores(group)
    }
  }
}

/// A freeze-pending source whose stores CANNOT BE RESOLVED is a claim the gate cannot rule out.
/// `GroupStores::stores` may answer `None` for a hosted group — the contract names that shape, a
/// starved group — and the pending claim lives only in that group's log, so a gate that skipped
/// the source on `None` read "unreadable" as "claims nothing" and let the target go. Source 5's
/// pending freeze names 3, not 1: with 5's stores withheld, `remove_group(&1)` REFUSES all the
/// same (fail-closed, exactly as on an unreadable page); with them resolvable the readable claim
/// refuses 3 and admits 1 — the refusal was the missing read, not a match.
#[test]
fn an_unresolvable_freeze_pending_source_fences_every_target() {
  let mut engine: GroupEngine<u64, u64> = GroupEngine::new();
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let now = Instant::ORIGIN;
  engine_group(&mut m, &mut engine, 1, 0, now);
  engine_group(&mut m, &mut engine, 3, 0, now);
  engine_follower(&mut m, &mut engine, 5, now);
  engine_follower_append(
    &mut m,
    &mut engine,
    5,
    Term::new(1),
    Index::ZERO,
    Term::ZERO,
    std::vec![valid_prepare_merge(Term::new(1), 1, 3, 1)],
    Index::ZERO,
    now,
  );
  assert!(
    m.group(&5).unwrap().freeze_pending().is_some(),
    "5's freeze toward 3 is pending"
  );
  assert_eq!(
    m.remove_group(
      &1,
      &mut Starved {
        engine: &mut engine,
        withheld: 5,
      },
    )
    .map(|ep| ep.is_some()),
    Err(RemoveError::Claimed),
    "a claim that cannot be read cannot be ruled out"
  );
  assert!(m.contains_group(&1), "the refusal left the target hosted");
  // Readable, the claim is exact.
  assert_eq!(
    m.remove_group(&3, &mut engine).map(|ep| ep.is_some()),
    Err(RemoveError::Claimed),
    "the readable pending claim refuses the target it names"
  );
  assert_eq!(
    m.remove_group(&1, &mut engine).map(|ep| ep.is_some()),
    Ok(true),
    "and admits the target it does not"
  );
}

/// EVERY QUEUED CLAIM, NOT THE FIRST. Source 9 applied a freeze for target 1; behind its apply
/// budgets sit two more freeze cycles, committed: `Unfreeze0`, `Prepare1` (naming 3), `Unfreeze1`,
/// `Prepare2` (naming 5), each boundary placed one budget apart so the drain can be parked exactly
/// after any of them. A gate that read the LOWEST pending claim answered 3 and left 5 undefended
/// through the whole backlog; and a fold that CLEARED the pending state at `Prepare1`'s apply
/// left the gate not even scanning until `Unfreeze1` applied. Both doors on 5 —
/// `remove_group(&5)` and a freeze of 5 as another merge's SOURCE — refuse at every stage: on the
/// collected pending claims while the drain is parked before `Unfreeze0`, still with `Prepare1`
/// applied (the fold re-derived the pending state from `Prepare2`), still through `Unfreeze1`,
/// and on the applied claim once `Prepare2` folds. The released targets admit as each cycle
/// resolves: 1 after `Prepare1` replaces its freeze, 3 after `Unfreeze1` thaws it.
#[test]
fn every_queued_claim_is_read_across_an_apply_starved_backlog() {
  let budget = crate::endpoint::APPLY_BUDGET_ENTRIES;
  // Phase B's own message-path pass applies one budget from index 2; each `engine_step` applies one
  // more. The boundaries below land the second pass exactly on `Prepare1` and the third exactly
  // on `Unfreeze1`.
  let unfreeze0_at = 2 * budget;
  let prepare1_at = 2 * budget + 1;
  let unfreeze1_at = 3 * budget + 1;
  let prepare2_at = 3 * budget + 2;
  let mut engine: GroupEngine<u64, u64> = GroupEngine::new();
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let now = Instant::ORIGIN;
  for target in [1u64, 3, 4, 5] {
    engine_group(&mut m, &mut engine, target, 0, now);
  }
  engine_follower(&mut m, &mut engine, 9, now);
  engine_follower_append(
    &mut m,
    &mut engine,
    9,
    Term::new(1),
    Index::ZERO,
    Term::ZERO,
    std::vec![valid_prepare_merge(Term::new(1), 1, 1, 1)],
    Index::new(1),
    now,
  );
  assert!(
    m.group(&9).unwrap().frozen_for() == Some(&gid_key(1)),
    "9 applied its freeze for 1"
  );
  // The committed backlog, delivered and not cranked: the drain parks one budget in, before
  // `Unfreeze0`.
  let mut entries = normal_load(2, unfreeze0_at - 1);
  entries.push(unfreeze_entry(unfreeze0_at, 2));
  entries.push(valid_prepare_merge(Term::new(1), prepare1_at, 3, 3));
  entries.extend(normal_load(prepare1_at + 1, unfreeze1_at - 1));
  entries.push(unfreeze_entry(unfreeze1_at, 4));
  entries.push(valid_prepare_merge(Term::new(1), prepare2_at, 5, 5));
  engine_follower_deliver(
    &mut m,
    &mut engine,
    9,
    Term::new(1),
    Index::new(1),
    Term::new(1),
    entries,
    Index::new(prepare2_at),
    now,
  );
  let source = m.group(&9).unwrap();
  assert_eq!(
    (source.commit_index(), source.applied_index()),
    (Index::new(prepare2_at), Index::new(1 + budget)),
    "the drain parked one budget in, before Unfreeze0"
  );
  assert!(
    source.is_frozen() && source.freeze_pending() == Some(Index::new(prepare1_at)),
    "frozen for 1, the lowest pending freeze is Prepare1"
  );
  let doors_refuse_5 =
    |m: &mut MultiRaft<u64, u64, CountSm>, engine: &mut GroupEngine<u64, u64>, stage: &str| {
      assert_eq!(
        m.remove_group(&5, engine).map(|ep| ep.is_some()),
        Err(RemoveError::Claimed),
        "{stage}: the teardown door reads the queued claim on 5"
      );
      assert!(
        matches!(
          m.prepare_merge(&5, now, engine, &4),
          Some(Err(MergeError::SourceClaimedAsTarget))
        ),
        "{stage}: the freeze door reads the queued claim on 5"
      );
    };
  doors_refuse_5(&mut m, &mut engine, "parked before Unfreeze0");

  // Exactly through `Prepare1`'s apply: the fold must re-derive the pending state from `Prepare2`.
  engine_step(&mut m, &mut engine, 9, now);
  let source = m.group(&9).unwrap();
  assert_eq!(
    source.applied_index(),
    Index::new(prepare1_at),
    "the step applied exactly through Prepare1"
  );
  assert!(
    source.is_frozen() && source.frozen_for() == Some(&gid_key(3)),
    "9 is frozen for 3 now"
  );
  assert_eq!(
    source.freeze_pending(),
    Some(Index::new(prepare2_at)),
    "Prepare1's fold re-derived the pending state from the queued Prepare2"
  );
  doors_refuse_5(&mut m, &mut engine, "Prepare1 applied");
  assert_eq!(
    m.remove_group(&1, &mut engine).map(|ep| ep.is_some()),
    Ok(true),
    "1's claim was replaced by Prepare1"
  );

  // Exactly through `Unfreeze1`: thawed, the pending state re-derived once more.
  engine_step(&mut m, &mut engine, 9, now);
  let source = m.group(&9).unwrap();
  assert_eq!(
    source.applied_index(),
    Index::new(unfreeze1_at),
    "the step applied exactly through Unfreeze1"
  );
  assert!(
    !source.is_frozen() && source.freeze_pending() == Some(Index::new(prepare2_at)),
    "thawed, with Prepare2 still pending"
  );
  doors_refuse_5(&mut m, &mut engine, "Unfreeze1 applied");
  assert_eq!(
    m.remove_group(&3, &mut engine).map(|ep| ep.is_some()),
    Ok(true),
    "3's claim was released by Unfreeze1"
  );

  // Through `Prepare2`: frozen for 5, and both doors refuse on the applied claim.
  engine_step(&mut m, &mut engine, 9, now);
  let source = m.group(&9).unwrap();
  assert!(
    source.is_frozen()
      && source.frozen_for() == Some(&gid_key(5))
      && source.freeze_pending().is_none(),
    "9 is frozen for 5, nothing pending"
  );
  assert_eq!(source.shape_gen(), 5, "every cycle moved the counter");
  doors_refuse_5(&mut m, &mut engine, "Prepare2 applied");
}

/// THE WALK ENDS AT A COMMITTED REFUSAL. Source 5's committed suffix holds a refused `PrepareMerge`
/// and, above it, a valid one naming target 1; the drain is parked below both. The refused entry
/// will poison 5 the moment the drain reaches it, so the valid freeze above it never applies and
/// its claim must NOT be reported: `remove_group(&1)` admits, and the resumed drain poisons 5 as
/// promised. A walk that skipped the refusal and read on would fence 1 forever against a source
/// that can never absorb into it. The UNCOMMITTED twin is the opposite edge: a refused entry above
/// the commit is indeterminate, so the walk fails closed at it — `remove_group(&1)` refuses even
/// though the only readable claim above names 3, not 1 — a walk that ended with "no claim" there,
/// or read past it, would let 1 go.
#[test]
fn the_claim_walk_ends_at_a_committed_refusal_and_fails_closed_at_an_uncommitted_one() {
  let budget = crate::endpoint::APPLY_BUDGET_ENTRIES;
  // COMMITTED: the refused entry and the valid freeze above it both sit behind one budget of load.
  {
    let mut engine: GroupEngine<u64, u64> = GroupEngine::new();
    let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
    let now = Instant::ORIGIN;
    engine_group(&mut m, &mut engine, 1, 0, now);
    engine_follower(&mut m, &mut engine, 5, now);
    let mut entries = normal_load(1, budget);
    entries.push(refused_prepare_merge(
      RefusedFreeze::EmptyTarget,
      budget + 1,
      1,
    ));
    entries.push(valid_prepare_merge(Term::new(1), budget + 2, 1, 2));
    engine_follower_deliver(
      &mut m,
      &mut engine,
      5,
      Term::new(1),
      Index::ZERO,
      Term::ZERO,
      entries,
      Index::new(budget + 2),
      now,
    );
    let source = m.group(&5).unwrap();
    assert!(
      source.applied_index() == Index::new(budget)
        && source.freeze_pending() == Some(Index::new(budget + 1))
        && !source.is_poisoned(),
      "parked below the committed refusal, freeze-pending, unjudged"
    );
    assert_eq!(
      m.remove_group(&1, &mut engine).map(|ep| ep.is_some()),
      Ok(true),
      "the claim queued above a committed refusal is never reported"
    );
    engine_crank(&mut m, &mut engine, 5, now);
    assert_eq!(
      m.group(&5).unwrap().poison_reason(),
      Some(PoisonReason::MergeDecode),
      "the resumed drain refused the entry, as the walk promised"
    );
  }
  // UNCOMMITTED: the refused entry is above the commit; the valid freeze above it names 3.
  {
    let mut engine: GroupEngine<u64, u64> = GroupEngine::new();
    let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
    let now = Instant::ORIGIN;
    engine_group(&mut m, &mut engine, 1, 0, now);
    engine_group(&mut m, &mut engine, 3, 0, now);
    engine_follower(&mut m, &mut engine, 5, now);
    engine_follower_append(
      &mut m,
      &mut engine,
      5,
      Term::new(1),
      Index::ZERO,
      Term::ZERO,
      std::vec![
        refused_prepare_merge(RefusedFreeze::EmptyTarget, 1, 1),
        valid_prepare_merge(Term::new(1), 2, 3, 2),
      ],
      Index::ZERO,
      now,
    );
    assert_eq!(
      m.remove_group(&1, &mut engine).map(|ep| ep.is_some()),
      Err(RemoveError::Claimed),
      "an uncommitted refusal below the queue fails closed for every target"
    );
    assert!(m.contains_group(&1), "the refusal left the target hosted");
  }
}

/// `GroupStores` over paged `FailTermLog`s — the store whose reads a test can count, cold, or
/// alternate, per group.
struct PagedStores(std::collections::BTreeMap<u64, (crate::testkit::FailTermLog, AsyncStable)>);

impl crate::GroupStores<u64, crate::testkit::FailTermLog, AsyncStable> for PagedStores {
  fn stores(
    &mut self,
    group: &u64,
  ) -> Option<(&mut crate::testkit::FailTermLog, &mut AsyncStable)> {
    self.0.get_mut(group).map(|(l, s)| (l, s))
  }
}

/// Admit and elect single-voter group `gid` over paged stores.
fn paged_leader(
  m: &mut MultiRaft<u64, u64, CountSm>,
  stores: &mut PagedStores,
  gid: u64,
  now: Instant,
) {
  stores.0.insert(
    gid,
    (
      crate::testkit::FailTermLog::default(),
      AsyncStable::default(),
    ),
  );
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

/// Admit follower group `gid` (node 1 following phantom peer 2) over paged stores.
fn paged_follower(
  m: &mut MultiRaft<u64, u64, CountSm>,
  stores: &mut PagedStores,
  gid: u64,
  now: Instant,
) {
  stores.0.insert(
    gid,
    (
      crate::testkit::FailTermLog::default(),
      AsyncStable::default(),
    ),
  );
  m.create_group(gid, 0, follower_cfg(), now, 7, CountSm::default())
    .unwrap();
}

/// Deliver one term-1 `AppendEntries` from phantom leader 2 to paged follower `gid` — `entries`
/// after `prev`, the commit advertised at `commit` — WITHOUT draining its storage afterwards.
fn paged_deliver(
  m: &mut MultiRaft<u64, u64, CountSm>,
  stores: &mut PagedStores,
  gid: u64,
  prev: Index,
  entries: Vec<crate::Entry>,
  commit: Index,
  now: Instant,
) {
  let (log, stable) = stores.0.get_mut(&gid).unwrap();
  let prev_term = if prev == Index::ZERO {
    Term::ZERO
  } else {
    Term::new(1)
  };
  m.handle_message(
    &gid,
    now,
    log,
    stable,
    2u64,
    Message::AppendEntries(crate::AppendEntries::new(
      Term::new(1),
      2u64,
      prev,
      prev_term,
      entries,
      commit,
    )),
  )
  .unwrap();
}

/// How many `entries` reads paged group `gid`'s log has served.
fn paged_reads(stores: &PagedStores, gid: u64) -> u64 {
  stores.0.get(&gid).unwrap().0.observed_entries_calls()
}

/// THE CLAIM GATE READS EXACTLY THE QUEUED ENTRIES, EACH ONCE. Source 5 holds three uncommitted
/// freezes among ordinary entries — at 1 (naming 1), 3 (naming 3) and 5 (naming 4) — and nothing
/// is committed, so no apply read runs on its log. With the page at 3 cold, the gate reads the
/// first entry, meets the cold page at the second, and refuses: the third is never read — bounded
/// by the queue, never a walk restarted from its origin. Warm again, it reads only the two entries
/// it could not before, each as a single-entry range (a suffix walk read the whole range in one
/// wide chunk), and answers; from then on every attempt answers from the cached verdicts with no
/// read at all.
#[test]
fn the_claim_gate_reads_exactly_the_queued_entries() {
  use crate::testkit::FailTermLog;
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let mut stores = PagedStores(std::collections::BTreeMap::new());
  let now = Instant::ORIGIN;
  for gid in [1u64, 3, 4, 6] {
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
  stores
    .0
    .insert(5, (FailTermLog::default(), AsyncStable::default()));
  m.create_group(5, 0, follower_cfg(), now, 7, CountSm::default())
    .unwrap();
  {
    let (log, stable) = stores.0.get_mut(&5).unwrap();
    let mut entries = std::vec![valid_prepare_merge(Term::new(1), 1, 1, 1)];
    entries.extend(normal_load(2, 2));
    entries.push(valid_prepare_merge(Term::new(1), 3, 3, 2));
    entries.extend(normal_load(4, 4));
    entries.push(valid_prepare_merge(Term::new(1), 5, 4, 3));
    m.handle_message(
      &5,
      now,
      log,
      stable,
      2u64,
      Message::AppendEntries(crate::AppendEntries::new(
        Term::new(1),
        2u64,
        Index::ZERO,
        Term::ZERO,
        entries,
        Index::ZERO,
      )),
    )
    .unwrap();
    while matches!(
      m.handle_storage(&5, now, log, stable),
      Some(StorageProgress::MorePending)
    ) {}
  }
  assert_eq!(
    m.group(&5).unwrap().freeze_queue().collect::<Vec<_>>(),
    std::vec![Index::new(1), Index::new(3), Index::new(5)],
    "three freezes queued"
  );
  while m.poll_message().is_some() {}

  // COLD at the second queued entry: the first is read and cached, the walk stops at the cold
  // page, and the third is never read.
  stores
    .0
    .get_mut(&5)
    .unwrap()
    .0
    .cold_entries_at(Some(Index::new(3)));
  let before = paged_reads(&stores, 5);
  assert_eq!(
    m.remove_group(&4, &mut stores).map(|ep| ep.is_some()),
    Err(RemoveError::Claimed),
    "an unreadable queued entry refuses every target"
  );
  assert_eq!(
    paged_reads(&stores, 5) - before,
    2,
    "the gate stopped at the cold page — the entry above it was never read"
  );

  // WARM: a target nobody names admits after exactly the two still-uncached entries were read,
  // each as a single-entry range.
  stores.0.get_mut(&5).unwrap().0.cold_entries_at(None);
  let before = paged_reads(&stores, 5);
  assert_eq!(
    m.remove_group(&6, &mut stores).map(|ep| ep.is_some()),
    Ok(true),
    "6 is named by no queued freeze"
  );
  let log5 = &stores.0.get(&5).unwrap().0;
  assert_eq!(
    log5.observed_entries_calls() - before,
    2,
    "exactly the two entries the cold attempt could not read"
  );
  assert_eq!(
    log5.observed_max_range_width(),
    1,
    "each as a single-entry range — never a suffix walk"
  );

  // CACHED: no further attempt reads.
  let before = paged_reads(&stores, 5);
  assert_eq!(
    m.remove_group(&4, &mut stores).map(|ep| ep.is_some()),
    Err(RemoveError::Claimed),
    "4 is named by the third queued freeze"
  );
  assert_eq!(
    paged_reads(&stores, 5) - before,
    0,
    "every queued index needed exactly one Ready read, ever"
  );
}

/// A ONE-PAGE CACHE THAT EVICTS WHAT IT JUST SERVED cannot livelock the claim gate. Source 5 holds
/// two queued freezes (naming 1 and 3), nothing committed, and every read of its log flips the
/// page: a walk that re-read every queued entry after a cold page answered a standing refusal
/// forever — each attempt's first read evicted the page its second needed. The gate caches each
/// Ready verdict on the source and reads only the uncached indices: attempt 1 reads both (the
/// second cold) and refuses; attempt 2 reads only the entry attempt 1 could not and reaches the
/// terminal verdict; from then on no attempt reads at all.
#[test]
fn the_claim_gate_resumes_across_a_one_page_cache() {
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let mut stores = PagedStores(std::collections::BTreeMap::new());
  let now = Instant::ORIGIN;
  for gid in [1u64, 3, 6] {
    paged_leader(&mut m, &mut stores, gid, now);
  }
  paged_follower(&mut m, &mut stores, 5, now);
  let mut entries = std::vec![valid_prepare_merge(Term::new(1), 1, 1, 1)];
  entries.extend(normal_load(2, 2));
  entries.push(valid_prepare_merge(Term::new(1), 3, 3, 2));
  paged_deliver(
    &mut m,
    &mut stores,
    5,
    Index::ZERO,
    entries,
    Index::ZERO,
    now,
  );
  while m.poll_message().is_some() {}
  stores.0.get_mut(&5).unwrap().0.alternate_cold_on_read();

  let r0 = paged_reads(&stores, 5);
  assert_eq!(
    m.remove_group(&6, &mut stores).map(|ep| ep.is_some()),
    Err(RemoveError::Claimed),
    "attempt 1: the second queued entry's page is cold — refused, fail-closed"
  );
  assert_eq!(
    paged_reads(&stores, 5) - r0,
    2,
    "attempt 1 read both queued entries"
  );
  let r1 = paged_reads(&stores, 5);
  assert_eq!(
    m.remove_group(&6, &mut stores).map(|ep| ep.is_some()),
    Ok(true),
    "attempt 2: the terminal verdict — 6 is named by no queued freeze"
  );
  assert_eq!(
    paged_reads(&stores, 5) - r1,
    1,
    "attempt 2 read only the entry attempt 1 could not"
  );
  let r2 = paged_reads(&stores, 5);
  assert_eq!(
    m.remove_group(&1, &mut stores).map(|ep| ep.is_some()),
    Err(RemoveError::Claimed),
    "the cached claims answer 1"
  );
  assert_eq!(
    paged_reads(&stores, 5) - r2,
    0,
    "every queued index needed exactly one Ready read, ever"
  );
}

/// THE CLAIM CACHE FOLLOWS THE LOG. A cached claim is a fact about ONE entry: when a conflicting
/// append replaces that entry with a freeze naming a different target, the verdict goes with it
/// and the gate re-reads — the stale claim is never answered. And a refusal judged ABOVE the
/// commit is re-read once the commit reaches it: the same bytes are then a committed refusal,
/// which ends the walk instead of failing closed.
#[test]
fn the_claim_cache_follows_truncation_and_commit() {
  let now = Instant::ORIGIN;
  // TRUNCATION: the cached claim on 1 is replaced by a claim on 3.
  {
    let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
    let mut stores = PagedStores(std::collections::BTreeMap::new());
    paged_leader(&mut m, &mut stores, 1, now);
    paged_leader(&mut m, &mut stores, 3, now);
    paged_follower(&mut m, &mut stores, 5, now);
    paged_deliver(
      &mut m,
      &mut stores,
      5,
      Index::ZERO,
      std::vec![valid_prepare_merge(Term::new(1), 1, 1, 1)],
      Index::ZERO,
      now,
    );
    while m.poll_message().is_some() {}
    assert_eq!(
      m.remove_group(&1, &mut stores).map(|ep| ep.is_some()),
      Err(RemoveError::Claimed),
      "the claim on 1 is read and cached"
    );
    // A newer leader's suffix replaces index 1 with a freeze naming 3.
    {
      let (log, stable) = stores.0.get_mut(&5).unwrap();
      m.handle_message(
        &5,
        now,
        log,
        stable,
        2u64,
        Message::AppendEntries(crate::AppendEntries::new(
          Term::new(2),
          2u64,
          Index::ZERO,
          Term::ZERO,
          std::vec![valid_prepare_merge(Term::new(2), 1, 3, 1)],
          Index::ZERO,
        )),
      )
      .unwrap();
    }
    while m.poll_message().is_some() {}
    let r = paged_reads(&stores, 5);
    assert_eq!(
      m.remove_group(&1, &mut stores).map(|ep| ep.is_some()),
      Ok(true),
      "the replaced entry was re-read: the stale claim on 1 is never answered"
    );
    assert_eq!(
      paged_reads(&stores, 5) - r,
      1,
      "one re-read of the replaced index"
    );
    assert_eq!(
      m.remove_group(&3, &mut stores).map(|ep| ep.is_some()),
      Err(RemoveError::Claimed),
      "the new claim is answered from that re-read"
    );
  }
  // COMMIT: a refusal judged above the commit is re-read once the commit reaches it.
  {
    let budget = crate::endpoint::APPLY_BUDGET_ENTRIES;
    let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
    let mut stores = PagedStores(std::collections::BTreeMap::new());
    paged_leader(&mut m, &mut stores, 1, now);
    paged_follower(&mut m, &mut stores, 5, now);
    let mut entries = normal_load(1, budget);
    entries.push(refused_prepare_merge(
      RefusedFreeze::EmptyTarget,
      budget + 1,
      1,
    ));
    entries.push(valid_prepare_merge(Term::new(1), budget + 2, 1, 2));
    paged_deliver(
      &mut m,
      &mut stores,
      5,
      Index::ZERO,
      entries,
      Index::ZERO,
      now,
    );
    while m.poll_message().is_some() {}
    let r = paged_reads(&stores, 5);
    assert_eq!(
      m.remove_group(&1, &mut stores).map(|ep| ep.is_some()),
      Err(RemoveError::Claimed),
      "judged above the commit: fail-closed"
    );
    assert_eq!(paged_reads(&stores, 5) - r, 1, "the refusal was read once");
    // The leader's commit reaches past the refusal: the drain applies exactly one budget of load
    // and parks below it, the refusal committed and unapplied.
    paged_deliver(
      &mut m,
      &mut stores,
      5,
      Index::new(budget + 2),
      std::vec![],
      Index::new(budget + 2),
      now,
    );
    let source = m.group(&5).unwrap();
    assert_eq!(
      (source.commit_index(), source.applied_index()),
      (Index::new(budget + 2), Index::new(budget)),
      "parked one budget in, below the committed refusal"
    );
    let r = paged_reads(&stores, 5);
    assert_eq!(
      m.remove_group(&1, &mut stores).map(|ep| ep.is_some()),
      Ok(true),
      "re-read as a committed refusal: the walk ends there, and 1 is claimed by nothing that can apply"
    );
    assert_eq!(paged_reads(&stores, 5) - r, 1, "exactly the re-read");
    // The drain confirms the verdict.
    {
      let (log, stable) = stores.0.get_mut(&5).unwrap();
      while matches!(
        m.handle_storage(&5, now, log, stable),
        Some(StorageProgress::MorePending)
      ) {}
    }
    assert_eq!(
      m.group(&5).unwrap().poison_reason(),
      Some(PoisonReason::MergeDecode),
      "the resumed drain refused the entry, as the verdict promised"
    );
  }
}

/// The rule holds across a RESTART, at both crash points the same durable log can be recovered
/// from. Two sources carry the identical committed refused entry. Source 3's commit floor is
/// durable past the entry — the persist a budget-cut apply crank writes before its drain reaches
/// the entry — so boot replay meets the entry, poisons, and restart lands where the live refusal
/// did: no kill armed, removable under the cap. Source 2 is the state the live refusal actually
/// leaves behind: the poison stopped the batched commit persist, so nothing durable committed the
/// entry, boot re-arms the kill by kind (unjudged, exactly as the live append did), and the
/// leader's re-delivered commit drives the refused apply — which must release what the boot scan
/// armed, or the restarted replica wedges `Frozen` on an entry that never leaves its log. The
/// engine's cap is a function of the surviving log, so it holds through both.
#[test]
fn a_refused_prepare_merge_arms_no_freeze_across_a_restart_either() {
  let mut engine: GroupEngine<u64, u64> = GroupEngine::new();
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let now = Instant::ORIGIN;
  for gid in [2u64, 3] {
    engine_follower(&mut m, &mut engine, gid, now);
    engine_follower_append(
      &mut m,
      &mut engine,
      gid,
      Term::new(1),
      Index::ZERO,
      Term::ZERO,
      std::vec![refused_prepare_merge(RefusedFreeze::EmptyTarget, 1, 1)],
      Index::new(1),
      now,
    );
    assert!(m.group(&gid).unwrap().is_poisoned(), "{gid}: refused live");
  }
  // Source 3's crash point: the commit floor persisted past the entry before the drain reached it.
  {
    let (_, stable) = engine.stores(&3).unwrap();
    let hs = stable.hard_state().with_commit(Index::new(1));
    crate::StableStore::submit_write(stable, OpId::first_of_epoch(9), hs);
  }
  engine.flush();
  // Source 2's crash point is the one the live refusal leaves: the poison blocked the persist.
  assert_eq!(
    engine.stores(&2).unwrap().1.hard_state().commit(),
    Index::ZERO,
    "the poisoned drain never persisted the commit floor"
  );
  drop(m);

  let mut m2: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  for gid in [2u64, 3] {
    let epoch = engine.next_boot_epoch(&gid).unwrap();
    let (log, stable) = engine.stores(&gid).unwrap();
    m2.restore_group_unchecked(
      gid,
      follower_cfg(),
      now,
      7,
      CountSm::default(),
      epoch,
      log,
      stable,
    )
    .unwrap();
  }
  // Source 2: nothing durable committed the entry, so boot re-arms the kill by kind, unjudged.
  let ep = m2.group(&2).unwrap();
  assert!(
    !ep.is_poisoned(),
    "2: no durable commit reached the entry at boot"
  );
  assert!(
    ep.merge_freeze_active() && !ep.is_frozen(),
    "2: the boot scan re-armed the append-observed kill by kind"
  );
  // The leader re-delivers the commit; the refused apply must release what the boot scan armed.
  engine_follower_append(
    &mut m2,
    &mut engine,
    2,
    Term::new(1),
    Index::new(1),
    Term::new(1),
    std::vec![],
    Index::new(1),
    now,
  );
  let ep = m2.group(&2).unwrap();
  assert_eq!(
    ep.poison_reason(),
    Some(PoisonReason::MergeDecode),
    "2: the re-delivered commit met the refusal"
  );
  assert_eq!(
    engine.removal_floor(&2),
    crate::HIGHEST_WORKING_GENERATION,
    "2: the cap still holds"
  );
  assert_eq!(
    m2.remove_group(&2, &mut engine).map(|ep| ep.is_some()),
    Ok(true),
    "2: the restarted source tears down — the boot-armed kill released at the refusal"
  );
  // Source 3: boot replay reached the entry — poisoned at boot, and no kill survives the refusal.
  let ep = m2.group(&3).unwrap();
  assert_eq!(
    ep.poison_reason(),
    Some(PoisonReason::MergeDecode),
    "3: replay refused the entry at boot"
  );
  assert!(
    !ep.merge_freeze_active(),
    "3: restart lands where the live refusal did — no freeze armed"
  );
  assert_eq!(
    engine.removal_floor(&3),
    crate::HIGHEST_WORKING_GENERATION,
    "3: the cap survives the restart"
  );
  assert_eq!(
    m2.remove_group(&3, &mut engine).map(|ep| ep.is_some()),
    Ok(true),
    "3: removable after boot"
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
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.rollback_merge(&1, now, log, stable, &2).unwrap().unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert_eq!(
    m.service_merge_applies(now, &mut stores),
    std::vec![MergeResolution::Aborted {
      source: 2,
      target: 1
    }]
  );
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    m.group(&2).unwrap().is_frozen() && m.group(&1).unwrap().owes_live_thaw(),
    "the source is frozen and the obligation is recorded"
  );

  // Step the source leader down: the service's thaw drive now refuses `NotLeader` and appends
  // nothing, so the obligation is RETAINED for a later crank rather than consumed-and-lost.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    step_down(&mut m, 2, log, stable);
  }
  m.service_merge_applies(now, &mut stores);
  assert!(
    m.group(&2).unwrap().is_frozen() && m.group(&1).unwrap().owes_live_thaw(),
    "a leaderless source keeps the obligation — nothing thawed, nothing dropped"
  );

  // A source leader now exists: the service lands the thaw (appended + applied), and the next crank
  // OBSERVES the advance and discharges the obligation — not permanently wedged.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    re_elect(&mut m, 2, log, stable);
  }
  m.service_merge_applies(now, &mut stores);
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    !m.group(&2).unwrap().is_frozen(),
    "the service-driven thaw unfroze the source once a leader existed"
  );
  m.service_merge_applies(now, &mut stores);
  // The observing leader deferred its clear to the witness — apply it on the holder.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    !m.group(&1).unwrap().owes_live_thaw(),
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
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.rollback_merge(&1, now, log, stable, &2).unwrap().unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  m.service_merge_applies(now, &mut stores);
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    m.group(&1).unwrap().owes_live_thaw(),
    "the abort recorded the obligation"
  );
  // The service DRIVES the thaw: it APPENDS the source-side RollbackMerge on the source leader's
  // log. The append alone does NOT discharge — the obligation is still set right after.
  m.service_merge_applies(now, &mut stores);
  assert!(
    m.group(&1).unwrap().owes_live_thaw(),
    "the append is only a leg of delivery — the obligation is not yet discharged"
  );
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    !m.group(&2).unwrap().is_frozen(),
    "the thaw committed+applied on the first drive"
  );
  // The next crank OBSERVES the source past the freeze (seen > expected) and DISCHARGES — terminal,
  // no infinite retry.
  m.service_merge_applies(now, &mut stores);
  // The observing leader deferred its clear to the witness — apply it on the holder.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    !m.group(&1).unwrap().owes_live_thaw(),
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
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.rollback_merge(&1, now, log, stable, &2).unwrap().unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert_eq!(
    m.service_merge_applies(now, &mut stores),
    std::vec![MergeResolution::Aborted {
      source: 2,
      target: 1
    }]
  );
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert_eq!(
    m.group(&1)
      .unwrap()
      .abandoned_obligations()
      .first()
      .map(|(_, m)| m.generation),
    Some(1),
    "the obligation is recorded at gen 1"
  );

  // Another host's thaw lands here at the recorded generation and unfreezes the source (driven
  // DIRECTLY, modelling a peer). The service then OBSERVES the advance and discharges the
  // obligation — the abort-pending gate REQUIRES this discharge before the pair may re-freeze.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.propose_merge_unfreeze(&2, now, log, stable, &1, 1)
      .unwrap()
      .unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    !m.group(&2).unwrap().is_frozen(),
    "the original thaw landed"
  );
  assert_eq!(
    m.group(&2).unwrap().shape_gen(),
    2,
    "0 -> 1 freeze -> 2 thaw"
  );
  m.service_merge_applies(now, &mut stores);
  // The observing leader deferred its clear to the witness — apply it on the holder.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    !m.group(&1).unwrap().owes_live_thaw(),
    "the observed advance discharged the obligation, freeing the target to absorb again"
  );

  // The SAME pair freezes AGAIN — a brand-new merge with its own parked commit at a new gen, admitted
  // only because the prior obligation discharged above (the abort-pending gate).
  {
    m.prepare_merge(&2, now, &mut stores, &1).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    m.group(&2).unwrap().is_frozen(),
    "re-frozen for a new merge"
  );
  assert_eq!(m.group(&2).unwrap().shape_gen(), 3, "2 -> 3 re-freeze");
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.commit_merge(&1, now, log, stable, &2).unwrap().unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    m.group(&1).unwrap().pending_merge().is_some(),
    "the new merge parks a commit expecting the new generation"
  );

  // THE GATE DIRECTLY: a retained/relayed thaw drive naming the OLD generation (1) against a source
  // now at gen 3 is TERMINAL `StaleThaw` and thaws nothing — the new freeze must survive.
  let result = {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    let r = m.propose_merge_unfreeze(&2, now, log, stable, &1, 1);
    drain_storage(&mut m, 2, now, log, stable);
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
    m.group(&2).unwrap().is_frozen(),
    "the new freeze still stands — the stale obligation did not thaw it"
  );
  assert_eq!(
    m.group(&2).unwrap().shape_gen(),
    3,
    "the source is not moved past the new park's expected generation"
  );
  assert!(
    m.group(&1).unwrap().pending_merge().is_some(),
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
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    let r = m.propose_merge_unfreeze(&2, now, log, stable, &1, 2);
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
    m.group(&2).unwrap().is_frozen(),
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
    m.prepare_merge(&2, now, &mut stores, &1).unwrap().unwrap();
  }
  {
    let src = m.group(&2).unwrap();
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

  // A source whose LOG has not reached its own freeze must answer transient `SourceBehindFreeze`,
  // never terminal `NotFrozen`: a terminal verdict discharges the committed abort's obligation, and
  // the source could then never be thawed.
  let result = {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.propose_merge_unfreeze(&2, now, log, stable, &1, 1)
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
    let src = m.group(&2).unwrap();
    assert!(src.merge_freeze_active() && !src.is_frozen());
    assert_eq!(src.shape_gen(), 0);
  }

  // Once the leader applies the freeze, the retained obligation's thaw lands.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(m.group(&2).unwrap().is_frozen(), "the freeze folded");
  assert_eq!(
    m.group(&2).unwrap().shape_gen(),
    1,
    "the lineage is now at the freeze gen"
  );
  // The committed target abort backs the thaw — the obligation the SourceBehindFreeze retention
  // exists to protect: target 2 abandons source 1's frozen merge, recording `abandoned` for (1, 1).
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.rollback_merge(&1, now, log, stable, &2).unwrap().unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  let result = {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    let r = m.propose_merge_unfreeze(&2, now, log, stable, &1, 1);
    drain_storage(&mut m, 2, now, log, stable);
    r
  };
  assert!(
    matches!(result, Some(Ok(_))),
    "the thaw lands once the freeze is applied: {result:?}"
  );
  assert!(
    !m.group(&2).unwrap().is_frozen(),
    "the source thawed — never permanently wedged"
  );
  assert_eq!(
    m.group(&2).unwrap().shape_gen(),
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
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.propose_merge_unfreeze(&999, now, log, stable, &2, 1)
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
      let (log, stable) = stores.0.get_mut(&1).unwrap();
      m.propose_merge_unfreeze(&gid, now, log, stable, &2, expected)
    };
    assert!(
      matches!(r, Some(Err(MergeError::NotFrozen))),
      "never-frozen ({why}) is NotFrozen: {r:?}"
    );
  }

  // --- the committed-but-unapplied freeze (freeze-pending, seen < expected): SourceBehindFreeze,
  //     the catch-up the service keeps `abandoned` set through. ---
  {
    m.prepare_merge(&2, now, &mut stores, &1).unwrap().unwrap();
  }
  let pending_behind = {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.propose_merge_unfreeze(&2, now, log, stable, &1, 1)
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
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }

  // Group 1 is now frozen-applied at gen 1 for target 2.
  assert!(m.group(&2).unwrap().is_frozen() && m.group(&2).unwrap().shape_gen() == 1);

  // --- applied freeze BELOW a later-named gen (mid-catch-up): SourceBehindFreeze. ---
  let applied_behind = {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.propose_merge_unfreeze(&2, now, log, stable, &1, 2)
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
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    step_down(&mut m, 2, log, stable);
  }
  let not_leader = {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.propose_merge_unfreeze(&2, now, log, stable, &1, 1)
  };
  assert!(
    matches!(not_leader, Some(Err(MergeError::NotLeader { .. }))),
    "a follower frozen at the exact incarnation is NotLeader: {not_leader:?}"
  );
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    re_elect(&mut m, 2, log, stable);
  }
  assert!(m.group(&2).unwrap().is_frozen() && m.group(&2).unwrap().shape_gen() == 1);

  // --- the exact incarnation claimed by a DIFFERENT target: SourceClaimed — a relay riding a
  //     foreign target's abort must not thaw it. ---
  let claimed = {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.propose_merge_unfreeze(&2, now, log, stable, &9, 1)
  };
  assert!(
    matches!(claimed, Some(Err(MergeError::SourceClaimed))),
    "the exact incarnation claimed elsewhere is SourceClaimed: {claimed:?}"
  );

  // Back the ACCEPT rows with a committed target abort — the derived-from-abort gate authorizes a
  // thaw ONLY when the claimed target owes this obligation: target 2 abandons source 1 at gen 1.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.rollback_merge(&1, now, log, stable, &2).unwrap().unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }

  // --- ACCEPT: the exact incarnation with the matching claim APPENDS the thaw. IDEMPOTENT: a
  //     second drive while the thaw is in flight appends NO duplicate (the `thaw_in_flight` guard). ---
  let accepted = {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.propose_merge_unfreeze(&2, now, log, stable, &1, 1)
  };
  assert!(
    matches!(accepted, Some(Ok(_))),
    "the exact incarnation with the matching claim appends the thaw: {accepted:?}"
  );
  let after_append = stores.0.get(&2).unwrap().0.last_index();
  let in_flight = {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.propose_merge_unfreeze(&2, now, log, stable, &1, 1)
  };
  assert!(
    matches!(in_flight, Some(Ok(_))),
    "a thaw already in flight is a no-op Ok: {in_flight:?}"
  );
  assert_eq!(
    stores.0.get(&2).unwrap().0.last_index(),
    after_append,
    "the idempotent guard appended no duplicate RollbackMerge"
  );
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(!m.group(&2).unwrap().is_frozen() && m.group(&2).unwrap().shape_gen() == 2);

  // --- advanced PAST the named incarnation (the delivered thaw is OBSERVED): StaleThaw, the
  //     incarnation gate's terminal refusal — leadership-independent, so the service's discharge
  //     check reads the same source advance to clear `abandoned` on every host. ---
  let stale = {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.propose_merge_unfreeze(&2, now, log, stable, &1, 1)
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

/// A FOLLOWER host that OBSERVES the source past the freeze must discharge its own obligation — the
/// gate answers terminal `StaleThaw` — WITHOUT ever leading. The source is thawed here (modelling
/// another host's leader delivering the thaw, `seen == 2`), then this host's source replica is
/// stepped down to a follower. The observed-advance verdict is therefore ordered BEFORE the
/// leadership gate: a `NotLeader` check ahead of the lineage dedupe shadows it, so a follower answers
/// transient `NotLeader` forever and can never discharge. This is the same advance the service's
/// leadership-independent discharge check reads.
#[test]
fn a_follower_retires_the_relay_on_the_observed_advance() {
  let (mut m, mut stores) = merge_host(2, 3);
  let now = Instant::ORIGIN;
  // Freeze group 1 for target 2 (seen == 1), then thaw it as leader so the source lineage advances
  // PAST the freeze (seen == 2) — the delivery this host will merely OBSERVE. A committed target
  // abort backs the thaw (target 2 abandons source 1 at gen 1).
  {
    m.prepare_merge(&2, now, &mut stores, &1).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.rollback_merge(&1, now, log, stable, &2).unwrap().unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.propose_merge_unfreeze(&2, now, log, stable, &1, 1)
      .unwrap()
      .unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(!m.group(&2).unwrap().is_frozen() && m.group(&2).unwrap().shape_gen() == 2);
  // This host's source replica is now a FOLLOWER; it never leads the thaw.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    step_down(&mut m, 2, log, stable);
  }
  let result = {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.propose_merge_unfreeze(&2, now, log, stable, &1, 1)
  };
  assert!(
    m.group(&2).unwrap().role().is_follower(),
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

/// SAFETY of the discharge signal: an appended thaw is only APPENDED, not delivered — a source
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
    m.prepare_merge(&2, now, &mut stores, &1).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(m.group(&2).unwrap().is_frozen() && m.group(&2).unwrap().shape_gen() == 1);
  // A committed target abort backs every thaw drive below (target 2 abandons source 1 at gen 1); it
  // persists across the truncation, so the re-driven thaw stays authorized until it finally commits.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.rollback_merge(&1, now, log, stable, &2).unwrap().unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  let leader_term = m.group(&2).unwrap().term();

  // ACCEPT: append the thaw but do NOT commit it (leave it durable-pending, uncommitted).
  let (thaw_index, appended) = {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    let r = m.propose_merge_unfreeze(&2, now, log, stable, &1, 1);
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
    m.group(&2).unwrap().is_frozen(),
    "still frozen — the thaw has not committed"
  );

  // LEADERSHIP LOSS + §5.3 TRUNCATION: a new leader at a higher term overwrites the uncommitted
  // thaw at its index. The applied freeze below it survives (seen stays 1).
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    let higher = Term::new(leader_term.get() + 5);
    let prev = Index::new(thaw_index.get() - 1);
    let replace = crate::Entry::new(higher, thaw_index, crate::EntryKind::Normal, {
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
  assert!(
    m.group(&2).unwrap().role().is_follower(),
    "the higher term stepped the leader down"
  );
  assert!(
    m.group(&2).unwrap().is_frozen() && m.group(&2).unwrap().shape_gen() == 1,
    "the freeze survived the truncation; the thaw is gone"
  );

  // As a follower, the drive is transient `NotLeader` — the obligation is held, not delivered.
  let as_follower = {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.propose_merge_unfreeze(&2, now, log, stable, &1, 1)
  };
  assert!(
    matches!(as_follower, Some(Err(MergeError::NotLeader { .. }))),
    "the follower cannot append — the obligation is held: {as_follower:?}"
  );

  // A NEW source leader re-appends the thaw (the `become_leader` reset frees the guard) and commits
  // it: the lineage advances past the freeze.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    re_elect(&mut m, 2, log, stable);
  }
  let reappended = {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    let r = m.propose_merge_unfreeze(&2, now, log, stable, &1, 1);
    drain_storage(&mut m, 2, now, log, stable);
    r
  };
  assert!(
    matches!(reappended, Some(Ok(_))),
    "the new leader re-appends the thaw: {reappended:?}"
  );
  assert!(
    !m.group(&2).unwrap().is_frozen() && m.group(&2).unwrap().shape_gen() == 2,
    "the re-driven thaw committed and delivered — the source is not wedged"
  );

  // Every host now OBSERVES the advance — the gate refuses terminally (StaleThaw), and the service's
  // discharge check reads the same advance to clear `abandoned`.
  let retired = {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.propose_merge_unfreeze(&2, now, log, stable, &1, 1)
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
  assert!(m.group(&2).unwrap().owes_live_thaw());
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
    m.group(&2).unwrap().owes_live_thaw(),
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
    !m.group(&2).unwrap().owes_live_thaw(),
    "the committed witness apply cleared the dead-end obligation"
  );
}

/// A COVERED DEAD END STANDS UNTIL A WITNESS, AND ITS INSTALL-COVER LIFTS THE CAPTURE FENCE (#132,
/// #138). Target 2 owes UNHOSTED source 1 (floor 0, lineage 0: nothing this host could ever observe
/// or drive) and an install's boundary crossed the abort entry. Absence is no proof, so the record
/// STANDS across cranks — live, unwitnessed (a cover is no global proof) — and the capture wedge the
/// dead end would otherwise be is cured by the FENCE, not by disposal: the install discarded the
/// entry the fence protected, so the threshold capture proceeds with the record standing. The
/// record's exit is a witness some observer mints elsewhere: its committed apply clears it.
#[test]
fn a_covered_dead_end_stands_until_a_witness_and_its_install_cover_lifts_the_fence() {
  let now = Instant::ORIGIN;
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let mut stores = MapStores(std::collections::BTreeMap::new(), Default::default());
  stores
    .0
    .insert(2, (VecLog::default(), AsyncStable::default()));
  m.create_group(
    2,
    0,
    single_node_cfg(1).with_snapshot_threshold(1),
    now,
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
  // The obligation as an install left it: abort entry at 1, install-covered.
  {
    let ep = m.group_mut(&2).unwrap();
    ep.note_abandoned(source_key.clone(), 5, Index::new(1));
    ep.note_abort_covered(Index::new(1), Cover::Install);
  }
  // A command past the threshold: the capture fires — the install-covered record fences nothing.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.propose(&2, now, log, stable, &Bytes::from_static(b"x"))
      .unwrap()
      .unwrap();
    drain_storage(&mut m, 2, now, log, stable);
    assert_eq!(m.group(&2).unwrap().applied_index(), Index::new(2));
    assert_eq!(
      stable.snapshot().map(|(meta, _)| meta.last_index()),
      Some(Index::new(2)),
      "the threshold capture landed: an install-covered record does not fence it"
    );
  }
  // Cranks: the record STANDS — absence is no proof — and nothing is minted off a cover.
  for _ in 0..4 {
    m.service_merge_applies(now, &mut stores);
  }
  assert!(
    m.group(&2).unwrap().owes_live_thaw()
      && m
        .group(&2)
        .unwrap()
        .abandoned_record(&source_key)
        .is_some_and(|r| !r.discharged),
    "a covered dead end stands, live, until a global fact — absence is not one"
  );
  assert_eq!(
    witness_count(&stores.0.get(&2).unwrap().0),
    0,
    "a cover is no global proof — nothing minted"
  );
  // A witness ARRIVES (append it as replication delivers it, then commit + apply): the exit.
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
    m.group(&2).unwrap().abandoned_obligations().is_empty(),
    "the committed witness apply cleared the covered dead end"
  );
}

/// The witness route disposes of a COVERED record too (#132): a co-hosting holder with a GLOBAL
/// proof mints `ThawDischarged`, and its committed apply clears every replica's record at the
/// witnessed generation — the mark neither blocks that clear nor is needed for it. Here the witness
/// arrives by replication on a holder whose transfer covered its record before any crank ran.
#[test]
fn a_witness_apply_clears_a_covered_obligation() {
  let (mut m, mut stores, source_key) = target_only_owing(5);
  let now = Instant::ORIGIN;
  m.group_mut(&2)
    .unwrap()
    .note_abort_covered(Index::new(1), Cover::Install);
  assert_eq!(
    m.group(&2)
      .unwrap()
      .abandoned_record(&source_key)
      .map(|r| r.cover),
    Some(Cover::Install),
    "the transfer's boundary covered the abort entry (1)"
  );
  assert!(
    m.group(&2).unwrap().owes_live_thaw(),
    "the covered record stands"
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
    m.group(&2).unwrap().abandoned_obligations().is_empty(),
    "the committed witness apply cleared the covered record"
  );
}

/// A hosted target and a hosted source 2 for the witness-mint pins: target 1 leads and holds a
/// LIVE record for 2's freeze at generation 1; 2 is admitted by the caller's `admit`. Returns the
/// container and its plain store seam (floor 0, lineage 0 unless the pin says otherwise).
fn leading_holder_with(
  admit: impl FnOnce(&mut MultiRaft<u64, u64, CountSm>, &mut MapStores),
) -> (MultiRaft<u64, u64, CountSm>, MapStores) {
  let now = Instant::ORIGIN;
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let mut stores = MapStores(std::collections::BTreeMap::new(), Default::default());
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
  admit(&mut m, &mut stores);
  while m.poll_message().is_some() {}
  while m.poll_event().is_some() {}
  m.group_mut(&1)
    .unwrap()
    .note_abandoned(gid_key(2), 1, Index::new(1));
  (m, stores)
}

/// Restore group 2 FROZEN at generation 1 claiming target 1 into `m`, from a log holding only its
/// applied `PrepareMerge`.
fn admit_frozen_source(m: &mut MultiRaft<u64, u64, CountSm>, stores: &mut MapStores) {
  let mut slog = VecLog::default();
  let mut sstable = AsyncStable::default();
  slog.force_append(&[crate::Entry::new(
    Term::new(1),
    Index::new(1),
    crate::EntryKind::PrepareMerge,
    prepare_merge_bytes(1, 1),
  )]);
  sstable.force_state(Term::new(1), Some(1u64), Index::new(1));
  m.restore_group_unchecked(
    2,
    single_node_cfg(1),
    Instant::ORIGIN,
    8,
    CountSm::default(),
    1,
    &mut slog,
    &mut sstable,
  )
  .unwrap();
  stores.0.insert(2, (slog, sstable));
  assert!(m.group(&2).unwrap().is_frozen() && m.group(&2).unwrap().shape_gen() == 1);
}

/// THE MIRROR LEG WITNESSES A HOSTED, UNFROZEN, LAGGING SOURCE (#137): source 2 is hosted at
/// generation 0 — a restore that trails its own history — while the engine's lineage mirror, the
/// driver's global record of 2's committed counter, reads past the abandoned generation. Read off
/// an UNFROZEN source the mirror is a global proof, so the leading holder MINTS the witness and
/// defers its clear to the committed apply. Under the hosted arm's old predicate only the live
/// counter could witness, and this shape cleared locally and silently instead.
#[test]
fn the_mirror_leg_witnesses_a_hosted_unfrozen_lagging_source() {
  let (mut m, inner) = leading_holder_with(|m, stores| {
    stores
      .0
      .insert(2, (VecLog::default(), AsyncStable::default()));
    m.create_group(
      2,
      0,
      single_node_cfg(1),
      Instant::ORIGIN,
      8,
      CountSm::default(),
    )
    .unwrap();
  });
  let now = Instant::ORIGIN;
  assert!(!m.group(&2).unwrap().is_frozen() && m.group(&2).unwrap().shape_gen() == 0);
  let mut stores = LineageStores {
    inner,
    floors: std::collections::BTreeMap::new(),
    lineages: std::collections::BTreeMap::from([(2u64, 2u64)]),
  };
  m.service_merge_applies(now, &mut stores);
  {
    let (log, stable) = stores.inner.0.get_mut(&1).unwrap();
    assert_eq!(
      witness_count(log),
      1,
      "the mirror past the generation, read off an unfrozen source, is a global proof — witnessed"
    );
    assert!(
      !m.group(&1).unwrap().owes_live_thaw() && m.group(&1).unwrap().holds_witness_debt(),
      "the leader marked the debt before appending — the discharged record is the re-append trigger"
    );
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    m.group(&1).unwrap().abandoned_obligations().is_empty(),
    "the committed witness apply cleared the record"
  );
}

/// THE MIRROR LEG IS FENCED OFF A SOURCE FROZEN AT THE ABANDONED GENERATION. The mirror is
/// monotone-max and survives removal: after a removal at a higher generation and a floor-less
/// recreation re-frozen at the abandoned one, it reads past a LIVE freeze. Read off that frozen
/// source it would retire a live obligation here and — witnessed — cluster-wide: a replica-local
/// drop dressed as a global proof. So a hosted source frozen at the generation is neither
/// discharged nor witnessed off the mirror; its own counter discharges it once it thaws.
#[test]
fn the_mirror_leg_is_fenced_off_a_source_frozen_at_the_abandoned_generation() {
  let (mut m, inner) = leading_holder_with(admit_frozen_source);
  let now = Instant::ORIGIN;
  let mut stores = LineageStores {
    inner,
    floors: std::collections::BTreeMap::new(),
    lineages: std::collections::BTreeMap::from([(2u64, 5u64)]),
  };
  for _ in 0..3 {
    m.service_merge_applies(now, &mut stores);
  }
  let t = m.group(&1).unwrap();
  assert!(
    t.owes_live_thaw()
      && t
        .abandoned_record(&gid_key(2))
        .is_some_and(|r| !r.discharged),
    "a stale mirror proves nothing about a source frozen at the generation — the record stays live"
  );
  assert_eq!(
    witness_count(&stores.inner.0.get(&1).unwrap().0),
    0,
    "and nothing is witnessed off it"
  );
  assert!(
    m.group(&2).unwrap().is_frozen(),
    "the live freeze is untouched"
  );
}

/// A COVERED DEAD END IS NOT CLEARED OFF A NON-TERMINAL FLOOR (#138): target 2 owes UNHOSTED
/// source 1, whose persisted removal floor sits past the abandoned generation — a host-local fact
/// ("this host stopped hosting the incarnation"). For an UNCOVERED record that floor clears
/// locally, as it always has: the entry is still in the log and re-derives on restart. For a
/// COVERED record the leg is withdrawn — the record's whole value is the abort belt's uniformity
/// and the owed-source escape, and neither survives a local drop — so it stands, live, and nothing
/// is minted off a local fact.
#[test]
fn a_covered_dead_end_is_not_cleared_off_a_non_terminal_floor() {
  let now = Instant::ORIGIN;
  // COVERED: stands.
  let (mut m, inner, source_key) = target_only_owing(1);
  m.group_mut(&2)
    .unwrap()
    .note_abort_covered(Index::new(1), Cover::Install);
  let mut stores = LineageStores {
    inner,
    floors: std::collections::BTreeMap::from([(1u64, 2u64)]),
    lineages: std::collections::BTreeMap::new(),
  };
  for _ in 0..3 {
    m.service_merge_applies(now, &mut stores);
  }
  assert!(
    m.group(&2).unwrap().owes_live_thaw()
      && m
        .group(&2)
        .unwrap()
        .abandoned_record(&source_key)
        .is_some_and(|r| !r.discharged),
    "a non-terminal floor is a local fact — it disposes of no covered record"
  );
  assert_eq!(
    witness_count(&stores.inner.0.get(&2).unwrap().0),
    0,
    "and mints nothing"
  );
  // UNCOVERED twin: clears locally, as before.
  let (mut m, inner, _) = target_only_owing(1);
  let mut stores = LineageStores {
    inner,
    floors: std::collections::BTreeMap::from([(1u64, 2u64)]),
    lineages: std::collections::BTreeMap::new(),
  };
  m.service_merge_applies(now, &mut stores);
  assert!(
    m.group(&2).unwrap().abandoned_obligations().is_empty(),
    "an uncovered record's entry re-derives on restart — the local floor still clears it"
  );
  assert_eq!(
    witness_count(&stores.inner.0.get(&2).unwrap().0),
    0,
    "a local proof mints nothing"
  );
}

/// A COVERED HOSTED SQUATTER IS NOT CLEARED OFF A NON-TERMINAL FLOOR (#138, residual 6): source 2
/// is hosted, idle, at generation 0 — recreated below the removal floor that sits past the
/// abandoned generation, growing toward it — and the holder's record is install-covered. The floor
/// is a local fact, so the hosted arm withholds its clear exactly as the unhosted arm does; the
/// record stands, live, unwitnessed — a HOLD that is recoverable rather than a divergence: the
/// squatter is hosted and unfrozen, so its removal admits and the purge clears the record.
#[test]
fn a_covered_hosted_squatter_is_not_cleared_off_a_non_terminal_floor() {
  let (mut m, inner) = leading_holder_with(|m, stores| {
    stores
      .0
      .insert(2, (VecLog::default(), AsyncStable::default()));
    m.create_group(
      2,
      0,
      single_node_cfg(1),
      Instant::ORIGIN,
      8,
      CountSm::default(),
    )
    .unwrap();
  });
  let now = Instant::ORIGIN;
  let source_key = gid_key(2);
  m.group_mut(&1)
    .unwrap()
    .note_abort_covered(Index::new(1), Cover::Install);
  let mut stores = LineageStores {
    inner,
    floors: std::collections::BTreeMap::from([(2u64, 2u64)]),
    lineages: std::collections::BTreeMap::new(),
  };
  for _ in 0..3 {
    m.service_merge_applies(now, &mut stores);
  }
  assert!(
    m.group(&1).unwrap().owes_live_thaw()
      && m
        .group(&1)
        .unwrap()
        .abandoned_record(&source_key)
        .is_some_and(|r| !r.discharged),
    "the hosted squatter's local floor disposes of no covered record"
  );
  assert_eq!(
    witness_count(&stores.inner.0.get(&1).unwrap().0),
    0,
    "and mints nothing"
  );
  // THE EXIT: the squatter is hosted and unfrozen, so its removal admits; the purge clears.
  assert_eq!(
    m.remove_group(&2, &mut stores).map(|r| r.is_some()),
    Ok(true),
    "an idle squatter is removable — no freeze gate, no owed-source question"
  );
  assert!(
    m.group(&1).unwrap().abandoned_obligations().is_empty(),
    "the purge cleared the holder's record"
  );
}

/// AN APPEND-PENDING FREEZE IS NOT IDLE (#138, residual 5): source 2's `PrepareMerge` toward 1 is
/// appended and observed but not yet applied — one apply away from freezing at the abandoned
/// generation — while a stale-high lineage mirror and a removal floor past the generation both
/// sit in the engine. Neither may retire the record: read now, the mirror would witness the live
/// freeze cluster-wide the instant before it applies, and the floor would clear it locally. Once
/// the freeze applies, the record drives the thaw as usual and the source's own counter retires it.
#[test]
fn an_append_pending_freeze_at_the_generation_is_neither_cleared_nor_witnessed() {
  let (mut m, mut inner) = merge_host(0, 0);
  let now = Instant::ORIGIN;
  // 2 appends its freeze toward 1 — observed at append, NOT applied (no drain).
  m.prepare_merge(&2, now, &mut inner, &1).unwrap().unwrap();
  let sp = m.group(&2).unwrap();
  assert!(
    sp.merge_freeze_active() && !sp.is_frozen() && sp.shape_gen() == 0,
    "freeze-pending: appended, unapplied"
  );
  let source_key = gid_key(2);
  m.group_mut(&1)
    .unwrap()
    .note_abandoned(source_key.clone(), 1, Index::new(1));
  let mut stores = LineageStores {
    inner,
    floors: std::collections::BTreeMap::from([(2u64, 3u64)]),
    lineages: std::collections::BTreeMap::from([(2u64, 5u64)]),
  };
  m.service_merge_applies(now, &mut stores);
  assert!(
    m.group(&1).unwrap().owes_live_thaw()
      && m
        .group(&1)
        .unwrap()
        .abandoned_record(&source_key)
        .is_some_and(|r| !r.discharged),
    "a freeze-pending source is not idle: neither the mirror nor the floor retires the record"
  );
  assert_eq!(
    witness_count(&stores.inner.0.get(&1).unwrap().0),
    0,
    "and nothing is witnessed off the stale mirror"
  );
  // The freeze applies; the record drives the thaw; the source's own counter retires it.
  {
    let (log, stable) = stores.inner.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(m.group(&2).unwrap().is_frozen() && m.group(&2).unwrap().shape_gen() == 1);
  m.service_merge_applies(now, &mut stores);
  {
    let (log, stable) = stores.inner.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert_eq!(
    m.group(&2).unwrap().shape_gen(),
    2,
    "the retained record drove the thaw"
  );
  m.service_merge_applies(now, &mut stores);
  {
    let (log, stable) = stores.inner.0.get_mut(&1).unwrap();
    assert_eq!(
      witness_count(log),
      1,
      "observed past the generation — the leader witnessed it"
    );
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    m.group(&1).unwrap().abandoned_obligations().is_empty(),
    "the committed witness apply cleared the record"
  );
}

/// THE TERMINAL FLOOR WITNESSES A HOSTED HUSK (#137): source 2 is hosted, frozen at the abandoned
/// generation, and its floor is the terminal `MERGED_FLOOR` — it was absorbed away, globally, and
/// what is hosted here is a husk. The terminal floor needs no freeze fence (terminal means no later
/// incarnation), so the leading holder mints the witness off it in the thaw pass; the husk dissolve
/// behind it in the same crank then retires the husk, and its purge takes the holder's record with
/// it. The witness is what every OTHER replica's record clears on; here it applies to nothing.
#[test]
fn the_terminal_floor_witnesses_a_hosted_husk() {
  let (mut m, mut stores) = leading_holder_with(admit_frozen_source);
  let now = Instant::ORIGIN;
  stores.1.insert(2);
  let resolutions = m.service_merge_applies(now, &mut stores);
  assert_eq!(
    witness_count(&stores.0.get(&1).unwrap().0),
    1,
    "the terminal floor is a global proof for a hosted frozen husk — the leading holder witnessed it"
  );
  assert!(
    resolutions.contains(&MergeResolution::Retired { source: 2 }) && !m.contains_group(&2),
    "the husk dissolve retired the source in the same crank, behind the mint"
  );
  assert!(
    m.group(&1).unwrap().abandoned_obligations().is_empty(),
    "the dissolve's purge cleared the holder's record"
  );
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    m.group(&1).unwrap().abandoned_obligations().is_empty() && !m.group(&1).unwrap().is_poisoned(),
    "the committed witness applies to nothing here — its work is on the other replicas"
  );
}

/// THE LATCHED MINT ARM IS GUARDED (#138, residual 10): `discharged` remembers a proof that a
/// floor-less recreation of source 2, re-frozen at the abandoned generation here, invalidates —
/// its fresh abort applies later and re-arms the record live, and a witness minted off the stale
/// flag would then clear that live obligation and strand the freeze. So the leading holder mints
/// nothing off the latched flag while 2 is freeze-active at a generation at-or-below the abandoned
/// one; once the source is idle, the latched flag mints as intended.
#[test]
fn a_stale_discharge_mark_is_not_witnessed_beside_a_source_refrozen_at_the_generation() {
  let now = Instant::ORIGIN;
  // Re-frozen at the generation here: NOT witnessed off the latched flag.
  let (mut m, mut stores) = leading_holder_with(admit_frozen_source);
  m.group_mut(&1).unwrap().note_discharged(&gid_key(2));
  for _ in 0..3 {
    m.service_merge_applies(now, &mut stores);
  }
  assert_eq!(
    witness_count(&stores.0.get(&1).unwrap().0),
    0,
    "a source freeze-active at the generation invalidates the latched proof — no witness"
  );
  assert!(
    m.group(&1).unwrap().holds_witness_debt() && m.group(&2).unwrap().is_frozen(),
    "the record waits, discharged; the freeze is untouched"
  );
  // Idle (a squatter at generation 0, nothing frozen): the latched flag mints.
  let (mut m, mut stores) = leading_holder_with(|m, stores| {
    stores
      .0
      .insert(2, (VecLog::default(), AsyncStable::default()));
    m.create_group(
      2,
      0,
      single_node_cfg(1),
      Instant::ORIGIN,
      8,
      CountSm::default(),
    )
    .unwrap();
  });
  m.group_mut(&1).unwrap().note_discharged(&gid_key(2));
  m.service_merge_applies(now, &mut stores);
  assert_eq!(
    witness_count(&stores.0.get(&1).unwrap().0),
    1,
    "beside an idle source the latched flag is the trigger it was kept to be"
  );
}

/// PIN (b): a leader whose ONLY proof is a NON-terminal floor clears LOCALLY and
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
    !m.group(&2).unwrap().owes_live_thaw(),
    "a non-terminal floor discharges the obligation LOCALLY"
  );
  assert_eq!(
    witness_count(&stores.inner.0.get(&2).unwrap().0),
    0,
    "A1: a non-terminal floor is host-local — it mints NO witness"
  );
}

/// A hosted incarnation at a LOWER gen than the abandoned freeze — a legal squatter recreated above a
/// non-terminal floor at a fresh gen — must not SHADOW the durable host-local proof that the NAMED
/// (dead) incarnation is discharged. The hosted arm ORs the persisted floor/lineage legs into the
/// LOCAL clear, so the squatter's gen-0 counter does not pin the obligation. A hosted arm reading
/// only the live counter (`shape_gen(0) > 1 = false`) and consulting no persisted leg holds the
/// re-derived obligation forever — the calm-window livelock.
#[test]
fn a_lower_gen_squatter_does_not_shadow_the_floor_discharge() {
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let mut inner = MapStores(
    std::collections::BTreeMap::new(),
    std::collections::BTreeSet::new(),
  );
  // The target holder (group 2) and the gen-0 SQUATTER source (group 1, recreated hosted BELOW the
  // abandoned freeze's gen 1). Both single-voter, elected.
  for gid in [2u64, 1] {
    inner
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
    let (log, stable) = inner.0.get_mut(&gid).unwrap();
    let d = m.group(&gid).unwrap().poll_timeout().unwrap();
    m.handle_timeout(&gid, d, log, stable).unwrap();
    drain_storage(&mut m, gid, d, log, stable);
    assert!(m.group(&gid).unwrap().role().is_leader());
  }
  while m.poll_message().is_some() {}
  while m.poll_event().is_some() {}
  let now = Instant::ORIGIN;
  assert_eq!(
    m.group(&1).unwrap().shape_gen(),
    0,
    "the squatter sits BELOW the abandoned freeze's generation"
  );
  // Crash-replay re-derives the target's obligation on source 1 at expected gen 1.
  m.group_mut(&2)
    .unwrap()
    .note_abandoned(gid_key(1), 1, Index::new(1));
  // The removal floor of the DEAD gen-1 incarnation: floored to 2 (past expected), NON-terminal.
  let mut stores = LineageStores {
    inner,
    floors: std::collections::BTreeMap::from([(1u64, 2u64)]),
    lineages: std::collections::BTreeMap::new(),
  };
  m.service_merge_applies(now, &mut stores);
  assert!(
    !m.group(&2).unwrap().owes_live_thaw(),
    "the persisted floor discharged the obligation off the durable proof, not the squatter's counter"
  );
  // A lower-gen hosted counter is HOST-LOCAL, never a global proof: no witness is minted (the A1
  // discipline — only the local clear fires).
  assert_eq!(
    witness_count(&stores.inner.0.get(&2).unwrap().0),
    0,
    "a lower-gen hosted incarnation mints NO witness — only the local clear fires"
  );
}

/// A LIVE source hosted AND FROZEN at the abandoned generation is NOT prematurely discharged by the
/// hosted floor/lineage legs: its own admission guaranteed `floor_admits(floor, expected)`, so the
/// floor leg is false, and a frozen source's lineage has not passed `expected` either. The obligation
/// stands until the source actually thaws past it — the no-over-reach bound on the hosted arm.
#[test]
fn a_live_frozen_source_at_expected_is_not_prematurely_discharged() {
  let (mut m, base) = merge_host(2, 3);
  let now = Instant::ORIGIN;
  let mut stores = LineageStores {
    inner: base,
    floors: std::collections::BTreeMap::new(),
    lineages: std::collections::BTreeMap::new(),
  };
  // Freeze source 2 into target 1 (source 2 frozen at gen 1) and abort on the target: it records
  // abandoned[2] = (1, idx). Source 2 stays HOSTED and frozen at gen 1 — the LIVE obligation.
  {
    m.prepare_merge(&2, now, &mut stores, &1).unwrap().unwrap();
    let (log, stable) = stores.inner.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  {
    let (log, stable) = stores.inner.0.get_mut(&1).unwrap();
    m.rollback_merge(&1, now, log, stable, &2).unwrap().unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    m.group(&2).unwrap().is_frozen() && m.group(&2).unwrap().shape_gen() == 1,
    "source 2 is the live obligation, frozen at the abandoned generation"
  );
  assert!(
    m.group(&1).unwrap().owes_live_thaw(),
    "the target owes 2 a thaw"
  );
  // The floor at the source's OWN admission value (a gen-1 incarnation admits iff `floor <= 1`):
  // `floor_admits(1, 1)` holds, so the floor leg is false and cannot clear the LIVE obligation.
  stores.floors.insert(2, 1);
  m.service_merge_applies(now, &mut stores);
  assert!(
    m.group(&1).unwrap().owes_live_thaw(),
    "a live frozen source AT the abandoned gen is not prematurely discharged by the hosted floor leg"
  );
}

/// A hosted FROZEN source whose STALE removal floor sits ABOVE the abandoned generation is a LIVE
/// obligation, not a dead squatter: the id was removed (floored) and its fresh incarnation legally
/// re-froze BELOW that floor at a colliding generation. The floor leg is FENCED off a frozen source,
/// so only the source-side thaw drive may clear it. Without the `is_frozen` fence `!floor_admits`
/// discharges the live freeze and strands the source frozen forever — the merge-freeze wedge.
#[test]
fn a_frozen_source_below_a_stale_floor_is_not_floor_discharged() {
  let (mut m, base) = merge_host(2, 3);
  let now = Instant::ORIGIN;
  let mut stores = LineageStores {
    inner: base,
    floors: std::collections::BTreeMap::new(),
    lineages: std::collections::BTreeMap::new(),
  };
  // Source 2 freezes into target 1 at gen 1; target 1 aborts and records abandoned[2] = (1, idx).
  // Source 2 stays HOSTED and frozen at gen 1 — a LIVE re-freeze below the stale floor below.
  {
    m.prepare_merge(&2, now, &mut stores, &1).unwrap().unwrap();
    let (log, stable) = stores.inner.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  {
    let (log, stable) = stores.inner.0.get_mut(&1).unwrap();
    m.rollback_merge(&1, now, log, stable, &2).unwrap().unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(m.group(&2).unwrap().is_frozen() && m.group(&2).unwrap().shape_gen() == 1);
  assert!(m.group(&1).unwrap().owes_live_thaw());
  // A STALE removal floor ABOVE the freeze gen (the id's PRIOR incarnation was floored to 3): it does
  // NOT admit gen 1, so the bare floor leg WOULD fire — but the source is FROZEN, so the fence holds.
  stores.floors.insert(2, 3);
  assert!(
    !crate::floor_admits(3, 1),
    "the stale floor fences gen 1 — the bare floor leg would discharge the live freeze"
  );
  m.service_merge_applies(now, &mut stores);
  assert!(
    m.group(&1).unwrap().owes_live_thaw(),
    "a FROZEN source below a stale floor is a live obligation — the floor leg must not discharge it"
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
    m.group(&2).unwrap().owes_live_thaw(),
    "the gen-mismatched witness no-op'd — the fresh obligation is untouched"
  );
  assert_eq!(
    m.group(&2)
      .unwrap()
      .abandoned_obligations()
      .first()
      .map(|(_, m)| m.generation),
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
    !m.group(&2).unwrap().owes_live_thaw() && m.group(&2).unwrap().holds_witness_debt(),
    "the leader marked the debt before appending — the discharged record is the re-append trigger"
  );
  // The committed apply clears it, leader included.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    !m.group(&2).unwrap().owes_live_thaw(),
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
    m.group(&2).unwrap().owes_live_thaw(),
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
    m.group(&2).unwrap().owes_live_thaw(),
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
    !m.group(&2).unwrap().owes_live_thaw(),
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
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.rollback_merge(&1, Instant::ORIGIN, log, stable, &2)
      .unwrap()
      .unwrap();
  }
  let resolutions = m.service_merge_applies(Instant::ORIGIN, &mut stores);
  assert_eq!(
    resolutions,
    std::vec![MergeResolution::Merged {
      source: 2,
      target: 1
    }]
  );
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, Instant::ORIGIN, log, stable);
  }
  let tep = m.group(&1).unwrap();
  assert_eq!(tep.state_machine().count(), 5, "the union stands");
  assert_eq!(tep.shape_gen(), 1, "the stale abort moved nothing");
  assert!(
    !tep.owes_live_thaw(),
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
  assert!(m.remove_group_inner(&2).is_some());
  seal_window(&mut m, &mut stores);
  // No floor: never-held — the park holds for the snapshot route.
  assert!(
    m.service_merge_applies(Instant::ORIGIN, &mut stores)
      .is_empty(),
    "absent without the floor must WAIT, not skip the union"
  );
  assert!(m.group(&1).unwrap().pending_merge().is_some());
  // The terminal floor lands (this host absorbed in a prior incarnation of the park): no-op.
  stores.1.insert(2);
  let resolutions = m.service_merge_applies(Instant::ORIGIN, &mut stores);
  assert_eq!(
    resolutions,
    std::vec![MergeResolution::Aborted {
      source: 2,
      target: 1
    }]
  );
  let tep = m.group(&1).unwrap();
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
  }
  // The target (group 1): restored from durable state whose committed log already carries the
  // CommitMerge naming source 2 — the park re-derives at restore replay, expecting source 2 at gen 1.
  let mut source_bytes = Vec::new();
  Data::encode(&2u64, &mut source_bytes);
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
  m.restore_group_unchecked(
    1,
    single_node_cfg(1),
    Instant::ORIGIN,
    7,
    CountSm::default(),
    1,
    &mut log2,
    &mut stable2,
  )
  .unwrap();
  assert!(m.group(&1).unwrap().pending_merge().is_some(), "re-parked");

  // The source is behind the expectation (gen 0 < 1) AND the window is open: the park WAITS.
  assert!(
    m.service_merge_applies(Instant::ORIGIN, &mut stores)
      .is_empty()
  );
  assert!(
    m.group(&1).unwrap().pending_merge().is_some(),
    "still parked"
  );

  // The source's freeze lands (gen reaches 1, frozen at its boundary): the next crank resolves.
  {
    m.prepare_merge(&2, Instant::ORIGIN, &mut stores, &1)
      .unwrap()
      .unwrap();
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, Instant::ORIGIN, log, stable);
  }
  // Group 2's own stores must be reachable through the seam for the absorb capture.
  stores.0.insert(1, (log2, AsyncStable::default()));
  // The restored target elects; its election no-op is the window's seal (any committed entry
  // at the coordinate that is not a matching abort closes the window for good).
  {
    let d = m.group(&1).unwrap().poll_timeout().unwrap();
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.handle_timeout(&1, d, log, stable).unwrap();
    drain_storage(&mut m, 1, d, log, stable);
    assert!(m.group(&1).unwrap().role().is_leader());
  }
  let resolutions = m.service_merge_applies(Instant::ORIGIN, &mut stores);
  assert_eq!(
    resolutions,
    std::vec![MergeResolution::Merged {
      source: 2,
      target: 1
    }]
  );
  assert!(!m.contains_group(&2));
  assert!(m.group(&1).unwrap().pending_merge().is_none());
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
  m.restore_group_unchecked(
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
  m.restore_group_unchecked(
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
/// wedge): a source replica whose LOG never reached the freeze boundary, cut off from any
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
  m.restore_group_unchecked(
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
  m.restore_group_unchecked(
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
  assert_eq!(next_lineage(MERGED_FLOOR - 3), Some(MERGED_FLOOR - 2));
  assert_eq!(
    next_lineage(MERGED_FLOOR - 2),
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
  m.create_group_founded_at(
    2,
    MERGED_FLOOR - 2,
    single_node_cfg(1),
    Instant::ORIGIN,
    7,
    SplitSm::default(),
    1,
    &log2,
    &mut stable2,
  )
  .unwrap();
  m.create_group(
    1,
    0,
    single_node_cfg(1),
    Instant::ORIGIN,
    7,
    SplitSm::default(),
  )
  .unwrap();
  let d1 = lead_single_split(&mut m, 2, &mut log1, &mut stable1);
  lead_single_split(&mut m, 1, &mut log2, &mut stable2);
  let before = log1.last_index();
  assert_eq!(
    m.prepare_merge(&2, d1, &mut empty_stores(), &1),
    Some(Err(MergeError::LineageExhausted)),
    "a freeze mint at the ceiling would be the reserved terminal"
  );
  assert_eq!(
    log1.last_index(),
    before,
    "the refused freeze appended nothing"
  );
  assert!(
    !m.group(&2).unwrap().is_frozen(),
    "the refusal left the source live, not frozen"
  );
  // Serviceable: a normal write still commits on the refused source.
  let applied_before = m.group(&2).unwrap().applied_index();
  commit_one_split(&mut m, 2, d1, &mut log1, &mut stable1);
  assert!(
    m.group(&2).unwrap().applied_index() > applied_before,
    "a normal write still commits after the freeze refusal"
  );

  // ---- propose_split at the ceiling ----
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  m.create_group_founded_at(
    7,
    MERGED_FLOOR - 2,
    single_node_cfg(1),
    Instant::ORIGIN,
    7,
    SplitSm::default(),
    1,
    &log,
    &mut stable,
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

/// Pin the byte order the direction rule actually compares — the CANONICAL `Data` encoding, NOT
/// numeric value — so nobody silently assumes numeric order. `u64` encodes little-endian, so for ids
/// that fit in one byte (all the merge tests use) byte order EQUALS numeric order; 256 vs 1 shows the
/// two diverge in general (256's encoding sorts BELOW 1's).
#[test]
fn direction_order_matches_encoded_bytes() {
  fn enc(x: u64) -> Vec<u8> {
    let mut b = Vec::new();
    Data::encode(&x, &mut b);
    b
  }
  assert!(
    enc(1) < enc(2),
    "single-byte ids: byte order == numeric order"
  );
  assert!(enc(2) < enc(3));
  assert!(enc(10) < enc(11));
  assert!(
    enc(256) < enc(1),
    "little-endian: 256's encoding sorts BELOW 1's — byte order is NOT numeric order in general"
  );
}

/// Every propose-time precondition maps to its typed refusal, with nothing appended.
#[test]
fn merge_verb_preconditions_refuse_typed() {
  // A sparse id scheme so every prepare_merge claim is direction-valid (source encodes above
  // target) and the INTENDED precondition gate fires, never the direction rule: clean source 10,
  // clean target 8, a 2-voter group 6 (VoterSetsDiffer as a target, NotLeader as a source into the
  // lower 4), a lease-based group 4 (ReadModesDiffer). 7 is an unhosted target, 99 an unhosted source.
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let mut stores = MapStores(
    std::collections::BTreeMap::new(),
    std::collections::BTreeSet::new(),
  );
  let now = Instant::ORIGIN;
  for gid in [10u64, 8u64] {
    stores
      .0
      .insert(gid, (VecLog::default(), AsyncStable::default()));
    m.create_group(gid, 0, single_node_cfg(1), now, 7, CountSm::default())
      .unwrap();
    let (log, stable) = stores.0.get_mut(&gid).unwrap();
    let d = m.group(&gid).unwrap().poll_timeout().unwrap();
    m.handle_timeout(&gid, d, log, stable).unwrap();
    drain_storage(&mut m, gid, d, log, stable);
    assert!(m.group(&gid).unwrap().role().is_leader());
  }
  while m.poll_message().is_some() {}
  while m.poll_event().is_some() {}

  // A 2-voter leaderless group (6) and a LeaseBased group (4) for the comparison arms.
  stores
    .0
    .insert(6, (VecLog::default(), AsyncStable::default()));
  m.create_group(6, 0, two_voter_cfg(), now, 7, CountSm::default())
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
      stores.0.get_mut(&10).map(|(l, s)| (l, s)).unwrap()
    };
  }

  {
    assert!(matches!(
      m.prepare_merge(&10, now, &mut stores, &10).unwrap(),
      Err(MergeError::SelfMerge)
    ));
    // The direction rule: a claim UP the id order refuses before any state-dependent gate.
    assert!(matches!(
      m.prepare_merge(&8, now, &mut stores, &10).unwrap(),
      Err(MergeError::DirectionInverted)
    ));
    assert!(matches!(
      m.prepare_merge(&10, now, &mut stores, &7).unwrap(),
      Err(MergeError::TargetMissing)
    ));
    assert!(matches!(
      m.prepare_merge(&10, now, &mut stores, &6).unwrap(),
      Err(MergeError::VoterSetsDiffer)
    ));
    assert!(matches!(
      m.prepare_merge(&10, now, &mut stores, &4).unwrap(),
      Err(MergeError::ReadModesDiffer)
    ));
    assert!(m.prepare_merge(&99, now, &mut stores, &8).is_none());
  }
  {
    // NotLeader: group 6 never elected — as the freeze's proposer (into the lower 4) and as the
    // abort's target leader. The relayed thaw refuses EARLIER: its terminal-dedupe precedes the
    // leadership gate, so a never-frozen source (seen == expected, not frozen) is NotFrozen
    // regardless of role.
    assert!(matches!(
      m.prepare_merge(&6, now, &mut stores, &4).unwrap(),
      Err(MergeError::NotLeader { .. })
    ));
    let (log, stable) = stores.0.get_mut(&6).map(|(l, s)| (l, s)).unwrap();
    assert!(matches!(
      m.rollback_merge(&6, now, log, stable, &8).unwrap(),
      Err(MergeError::NotLeader { .. })
    ));
    assert!(matches!(
      m.propose_merge_unfreeze(&6, now, log, stable, &8, 0)
        .unwrap(),
      Err(MergeError::NotFrozen)
    ));
  }
  {
    // Commit before any freeze: the local source is not ready; rollback with nothing to undo.
    let (log, stable) = stores.0.get_mut(&8).map(|(l, s)| (l, s)).unwrap();
    assert!(matches!(
      m.commit_merge(&8, now, log, stable, &10).unwrap(),
      Err(MergeError::SourceNotReady)
    ));
    assert!(matches!(
      m.commit_merge(&8, now, log, stable, &99).unwrap(),
      Err(MergeError::SourceMissing)
    ));
    assert!(matches!(
      m.commit_merge(&8, now, log, stable, &8).unwrap(),
      Err(MergeError::SelfMerge)
    ));
  }
  {
    // A conf change in flight on the source refuses the freeze (the comparison would race it).
    // The applied learner is then removed again: a source that still carried it would refuse
    // the freeze with `LearnersPresent`, which the next block exercises the clean path of.
    let (log, stable) = src!();
    m.propose_conf_change(
      &10,
      now,
      log,
      stable,
      crate::ConfChange::new(crate::ConfChangeType::AddLearnerNode, 5u64, Bytes::new()),
    )
    .unwrap()
    .unwrap();
    assert!(matches!(
      m.prepare_merge(&10, now, &mut stores, &8).unwrap(),
      Err(MergeError::ConfChangeInFlight)
    ));
    let (log, stable) = src!();
    drain_storage(&mut m, 10, now, log, stable);
    m.propose_conf_change(
      &10,
      now,
      log,
      stable,
      crate::ConfChange::new(crate::ConfChangeType::RemoveNode, 5u64, Bytes::new()),
    )
    .unwrap()
    .unwrap();
    drain_storage(&mut m, 10, now, log, stable);
    assert!(m.group(&10).unwrap().conf_state().learners().is_empty());
  }
  {
    // Freeze, then: a second freeze refuses; a parked commit refuses a second commit.
    m.prepare_merge(&10, now, &mut stores, &8).unwrap().unwrap();
    {
      let (log, stable) = src!();
      drain_storage(&mut m, 10, now, log, stable);
    }
    assert!(matches!(
      m.prepare_merge(&10, now, &mut stores, &8).unwrap(),
      Err(MergeError::AlreadyFrozen)
    ));
  }
  {
    let (log, stable) = stores.0.get_mut(&8).map(|(l, s)| (l, s)).unwrap();
    m.commit_merge(&8, now, log, stable, &10).unwrap().unwrap();
    drain_storage(&mut m, 8, now, log, stable);
    assert!(matches!(
      m.commit_merge(&8, now, log, stable, &10).unwrap(),
      Err(MergeError::AlreadyPending)
    ));
  }
  {
    // The abort's own gates, off group 4's leader: self-abort, an unhosted source, an
    // unfrozen source, and — with group 10 frozen FOR group 8 — the claim refusal (only the
    // claimed target may abort or thaw the merge; a foreign thaw would move the source's
    // counter under the claimed target's parked commit).
    let (log, stable) = stores.0.get_mut(&4).map(|(l, s)| (l, s)).unwrap();
    assert!(matches!(
      m.rollback_merge(&4, now, log, stable, &4).unwrap(),
      Err(MergeError::SelfMerge)
    ));
    assert!(matches!(
      m.rollback_merge(&4, now, log, stable, &99).unwrap(),
      Err(MergeError::SourceMissing)
    ));
    assert!(matches!(
      m.rollback_merge(&4, now, log, stable, &6).unwrap(),
      Err(MergeError::NotFrozen)
    ));
    assert!(matches!(
      m.rollback_merge(&4, now, log, stable, &10).unwrap(),
      Err(MergeError::SourceClaimed)
    ));
    assert!(matches!(
      m.propose_merge_unfreeze(&4, now, log, stable, &8, 0)
        .unwrap(),
      Err(MergeError::NotFrozen)
    ));
    assert!(matches!(
      m.commit_merge(&4, now, log, stable, &10).unwrap(),
      Err(MergeError::SourceClaimed)
    ));
  }
  {
    // The claim gate on the thaw itself: group 10 is frozen for 8, so a thaw riding any other
    // target's abort refuses.
    let (log, stable) = stores.0.get_mut(&10).map(|(l, s)| (l, s)).unwrap();
    assert!(matches!(
      m.propose_merge_unfreeze(&10, now, log, stable, &4, 1)
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
      m.group(&1).unwrap().conf_state().learners().contains(&3),
      "the learner applied on the target"
    );
    assert!(matches!(
      m.prepare_merge(&2, now, &mut stores, &1).unwrap(),
      Err(MergeError::LearnersPresent)
    ));
  }
  // The SOURCE carries a learner: freezing it refuses too (a frozen learner host could never
  // hand its half off).
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
      m.group(&2).unwrap().role().is_leader(),
      "the sole voter still leads"
    );
    assert!(matches!(
      m.prepare_merge(&2, now, &mut stores, &1).unwrap(),
      Err(MergeError::LearnersPresent)
    ));
  }
}

/// A target whose committed configuration lists learners {1, 3}. The freeze is refused at propose —
/// admitting it places a live absorb on a learner host, which parks forever (a learner never leads,
/// so the park has no decider). The randomized reshape band reaches this shape end-to-end.
#[test]
fn seed0_target_learner_pair_refused_at_propose() {
  let now = Instant::ORIGIN;
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let (mut slog, mut sstable) = (VecLog::default(), AsyncStable::default());
  let (mut tlog, mut tstable) = (VecLog::default(), AsyncStable::default());
  // Source (11) and target (10), both single-voter {2}, colocated on host 2 — the source encodes
  // above the target so the claim points down the id order.
  m.create_group(11, 0, single_node_cfg(2), now, 7, CountSm::default())
    .unwrap();
  m.create_group(10, 0, single_node_cfg(2), now, 7, CountSm::default())
    .unwrap();
  let ds = m.group(&11).unwrap().poll_timeout().unwrap();
  m.handle_timeout(&11, ds, &mut slog, &mut sstable).unwrap();
  drain_storage(&mut m, 11, ds, &mut slog, &mut sstable);
  let dt = m.group(&10).unwrap().poll_timeout().unwrap();
  m.handle_timeout(&10, dt, &mut tlog, &mut tstable).unwrap();
  drain_storage(&mut m, 10, dt, &mut tlog, &mut tstable);
  assert!(m.group(&11).unwrap().role().is_leader() && m.group(&10).unwrap().role().is_leader());
  // The target grows learners 1 and 3, one committed change at a time.
  for learner in [1u64, 3u64] {
    m.propose_conf_change(
      &10,
      dt,
      &mut tlog,
      &tstable,
      crate::ConfChange::new(crate::ConfChangeType::AddLearnerNode, learner, Bytes::new()),
    )
    .unwrap()
    .unwrap();
    drain_storage(&mut m, 10, dt, &mut tlog, &mut tstable);
  }
  let learners = m.group(&10).unwrap().conf_state().learners().clone();
  assert!(
    learners.contains(&1) && learners.contains(&3),
    "the committed conf lists learners {{1, 3}}"
  );
  assert!(matches!(
    m.prepare_merge(&11, ds, &mut empty_stores(), &10).unwrap(),
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
      m.prepare_merge(&2, now, &mut stores, &1).unwrap().unwrap();
      let (log, stable) = stores.0.get_mut(&2).unwrap();
      drain_storage(&mut m, 2, now, log, stable);
    }
    assert!(m.group(&2).unwrap().is_frozen(), "the source froze");
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
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    assert!(
      matches!(
        m.commit_merge(&1, now, log, stable, &2).unwrap(),
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
      Data::encode(&1u64, &mut tb);
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
    m.restore_group_unchecked(
      2,
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
      m.group(&2).unwrap().is_frozen(),
      "the crafted freeze applied on restore"
    );
    assert!(
      m.group(&2).unwrap().conf_state().learners().contains(&3),
      "the crafted learner applied on restore"
    );
    let (mut tlog, mut tstable) = (VecLog::default(), AsyncStable::default());
    m.create_group(
      1,
      0,
      single_node_cfg(1),
      Instant::ORIGIN,
      7,
      CountSm::default(),
    )
    .unwrap();
    let d = m.group(&1).unwrap().poll_timeout().unwrap();
    m.handle_timeout(&1, d, &mut tlog, &mut tstable).unwrap();
    drain_storage(&mut m, 1, d, &mut tlog, &mut tstable);
    assert!(m.group(&1).unwrap().role().is_leader());
    assert!(
      matches!(
        m.commit_merge(&1, d, &mut tlog, &tstable, &2).unwrap(),
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
    m.prepare_merge(&2, now, &mut stores, &1).unwrap().unwrap();
  }
  // No drain: the freeze is pending, not applied — the abort refuses.
  {
    let (log, stable) = stores.0.get_mut(&1).map(|(l, s)| (l, s)).unwrap();
    assert!(matches!(
      m.rollback_merge(&1, now, log, stable, &2).unwrap(),
      Err(MergeError::NotFrozen)
    ));
  }
  {
    let (log, stable) = stores.0.get_mut(&2).map(|(l, s)| (l, s)).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(m.group(&2).unwrap().is_frozen());
  // Applied: the abort lands on the target's log and records the durable thaw obligation.
  {
    let (log, stable) = stores.0.get_mut(&1).map(|(l, s)| (l, s)).unwrap();
    m.rollback_merge(&1, now, log, stable, &2).unwrap().unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert_eq!(m.group(&1).unwrap().shape_gen(), 1, "the abort bumped");
  assert_eq!(
    m.group(&1)
      .unwrap()
      .abandoned_obligations()
      .first()
      .map(|(_, m)| m.generation),
    Some(1),
    "the abort recorded the thaw obligation at the freeze generation"
  );
  // The per-crank service DRIVES the source thaw from the obligation; draining commits+applies it.
  m.service_merge_applies(now, &mut stores);
  {
    let (log, stable) = stores.0.get_mut(&2).map(|(l, s)| (l, s)).unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  let ep = m.group(&2).unwrap();
  assert!(!ep.is_frozen(), "thawed");
  assert_eq!(ep.shape_gen(), 2, "0 -> 1 (freeze) -> 2 (thaw)");
  assert!(!ep.merge_freeze_active());
}

/// THE MERGE-ORPHAN WEDGE, PREVENTED AT ADMISSION (the dual of the freeze-identity wedge):
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
    ack(&mut m, &mut stores, gid, 1, Index::new(1));
  }
  let now = Instant::ORIGIN;

  // Freeze the source: peer 2's ack commits and applies the PrepareMerge; peer 3's match stays
  // at 0 — log-behind, below the freeze boundary F.
  let f = { m.prepare_merge(&2, now, &mut stores, &1).unwrap().unwrap() };
  ack(&mut m, &mut stores, 2, 2, f);
  assert!(m.group(&2).unwrap().is_frozen());
  assert!(
    !m.group(&2).unwrap().peers_matched_through(f),
    "peer 3 has NOT reached the freeze boundary"
  );

  // The barrier refuses: admitting now would seed the wedge (source-leader loss orphans peer 3).
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    assert!(
      matches!(
        m.commit_merge(&1, now, log, stable, &2).unwrap(),
        Err(MergeError::SourceBarrierPending)
      ),
      "a lagging source voter must block the absorb at admission"
    );
  }
  assert!(
    m.group(&1).unwrap().pending_merge().is_none(),
    "nothing parked — no CommitMerge was proposed"
  );

  // Peer 3 catches up to the boundary: the barrier clears and the absorb admits, its committed
  // CommitMerge now a certificate that every source voter holds the freeze.
  ack(&mut m, &mut stores, 2, 3, f);
  assert!(m.group(&2).unwrap().peers_matched_through(f));
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    assert!(
      m.commit_merge(&1, now, log, stable, &2).unwrap().is_ok(),
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
    ack(&mut m, &mut stores, gid, 1, Index::new(1));
  }
  let now = Instant::ORIGIN;

  // Freeze the source, then bring EVERY source voter to the boundary — the admission barrier.
  let f = { m.prepare_merge(&2, now, &mut stores, &1).unwrap().unwrap() };
  ack(&mut m, &mut stores, 2, 2, f);
  ack(&mut m, &mut stores, 2, 3, f);
  assert!(m.group(&2).unwrap().is_frozen());
  assert!(
    m.group(&2).unwrap().peers_matched_through(f),
    "every source voter matched the boundary — the barrier is met"
  );

  // The barrier admits the absorb; peer 2's ack commits it and the target parks.
  let k = {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.commit_merge(&1, now, log, stable, &2).unwrap().unwrap()
  };
  ack(&mut m, &mut stores, 1, 2, k);
  assert!(m.group(&1).unwrap().pending_merge().is_some(), "parked");

  // The source LEADER is lost: a higher-term vote request steps node 1's source down to a
  // follower. A leader-local resolve-last discipline is defeated by exactly this loss, which is why
  // the straggler strand has to be prevented at ADMISSION rather than at resolve order.
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.handle_message(
      &2,
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
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(
    !m.group(&2).unwrap().role().is_leader(),
    "the source leader stepped down"
  );

  // Seal and resolve: dissolution completes on this host even though the source is no longer
  // led here — the committed CommitMerge is the certificate, not a live source leader.
  assert!(
    m.service_merge_applies(now, &mut stores).is_empty(),
    "the open window seals and holds"
  );
  ack(&mut m, &mut stores, 1, 2, k.next());
  let resolutions = m.service_merge_applies(now, &mut stores);
  assert_eq!(
    resolutions,
    std::vec![MergeResolution::Merged {
      source: 2,
      target: 1
    }]
  );
  assert!(!m.contains_group(&2));
}

/// The freeze gates cover the WHOLE admin propose family: a frozen (or freezing) group refuses
/// a split (forking would mutate the FSM above the freeze boundary), refuses to be a merge
/// TARGET (absorbing above its own boundary), and a source mid-absorb refuses a fresh freeze —
/// every arm typed, nothing appended.
#[test]
fn freeze_gates_cover_split_and_target_verbs() {
  // Four single-voter groups so every claim in the test is direction-valid (source encodes above
  // target) and the FROZEN/PENDING gate is what fires, never the direction rule: the frozen group
  // (3) is named as a target from the higher source (4), and the parked target (2) attempts a
  // fresh freeze into the lower group (1).
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let mut stores = MapStores(
    std::collections::BTreeMap::new(),
    std::collections::BTreeSet::new(),
  );
  let now = Instant::ORIGIN;
  for gid in [1u64, 2, 3, 4] {
    stores
      .0
      .insert(gid, (VecLog::default(), AsyncStable::default()));
    m.create_group(gid, 0, single_node_cfg(1), now, 7, CountSm::default())
      .unwrap();
    let (log, stable) = stores.0.get_mut(&gid).unwrap();
    let d = m.group(&gid).unwrap().poll_timeout().unwrap();
    m.handle_timeout(&gid, d, log, stable).unwrap();
    drain_storage(&mut m, gid, d, log, stable);
    assert!(m.group(&gid).unwrap().role().is_leader());
  }
  while m.poll_message().is_some() {}
  while m.poll_event().is_some() {}
  // Freeze source 3 into target 2 (the ordinary pairing; 3 encodes above 2).
  {
    m.prepare_merge(&3, now, &mut stores, &2).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&3).unwrap();
    drain_storage(&mut m, 3, now, log, stable);
  }
  assert!(m.group(&3).unwrap().is_frozen());
  // A frozen parent refuses a split, typed as the propose family does.
  {
    let (log, stable) = stores.0.get_mut(&3).unwrap();
    assert!(matches!(
      m.propose_split(&3, now, log, stable, &7, 0, Bytes::new())
        .unwrap(),
      Err(SplitError::Propose(crate::ProposeError::Frozen))
    ));
  }
  // A frozen group can be neither a prepare target (named from the higher source 4, direction-valid)
  // nor a commit target — the target-frozen gate fires, not the direction rule.
  assert!(matches!(
    m.prepare_merge(&4, now, &mut stores, &3).unwrap(),
    Err(MergeError::AlreadyFrozen)
  ));
  {
    let (log, stable) = stores.0.get_mut(&3).unwrap();
    assert!(matches!(
      m.commit_merge(&3, now, log, stable, &2).unwrap(),
      Err(MergeError::AlreadyFrozen)
    ));
  }
  // The real absorb parks the target (2); a parked target mid-absorb refuses a fresh freeze of
  // ITSELF into the lower group 1 (the mid-absorb SOURCE gate, direction-valid at 2 > 1).
  {
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    m.commit_merge(&2, now, log, stable, &3).unwrap().unwrap();
    drain_storage(&mut m, 2, now, log, stable);
  }
  assert!(m.group(&2).unwrap().pending_merge().is_some());
  assert!(matches!(
    m.prepare_merge(&2, now, &mut stores, &1).unwrap(),
    Err(MergeError::AlreadyPending)
  ));
}

/// An FSM that refuses the absorb POISONS the target (the deterministic fail-stop) — and the
/// service must surface NO `Merged` resolution for it: the driver would otherwise floor the
/// source terminally and tear its stores down behind the fail-stop, destroying the union's
/// only copy. The fail-stop stands alone; the source's storage half stays untouched.
///
/// The FSM ADVERTISES `supports_absorb` (so the propose gate admits the merge) but its `absorb`
/// returns `false` — the mixed-version / lying-implementation shape the apply-time poison backstops.
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
    // Advertise support so the propose-time gate admits the merge; `absorb` still returns the
    // default `false`, so the apply-time poison (the backstop this test exercises) fires.
    fn supports_absorb(&self) -> bool {
      true
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
    m.prepare_merge(&2, now, &mut stores, &1).unwrap().unwrap();
    let (log, stable) = stores.0.get_mut(&2).unwrap();
    drain(&mut m, 2, now, log, stable);
  }
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.commit_merge(&1, now, log, stable, &2).unwrap().unwrap();
    drain(&mut m, 1, now, log, stable);
  }
  assert!(m.group(&1).unwrap().pending_merge().is_some());
  assert!(
    m.service_merge_applies(now, &mut stores).is_empty(),
    "the first pass only seals"
  );
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain(&mut m, 1, now, log, stable);
  }
  let resolutions = m.service_merge_applies(now, &mut stores);
  // A refused absorb consumes the source endpoint too, so it surfaces CaptureFailed — the driver's
  // cue to fail the stranded source routing typed while PRESERVING its stores — never a Merged (which
  // would floor and drop the source) and never nothing (which would hang the source's callers).
  assert_eq!(
    resolutions,
    std::vec![MergeResolution::CaptureFailed {
      source: 2,
      target: 1
    }],
    "a poisoned absorb surfaces CaptureFailed, never a Merged teardown: {resolutions:?}"
  );
  let tep = m.group(&1).unwrap();
  assert!(tep.is_poisoned(), "the deterministic fail-stop stands");
  assert!(
    !m.contains_group(&2),
    "the extracted source endpoint is consumed either way (its stores are not)"
  );
  // THE RECOVERY PIN: nothing was absorbed, so the consumed source's preserved stores hold the
  // only copy of its state. A debt would discharge into a `Merged` that floors and tears the
  // source down; the pin never does — no capture covered the fold — and it holds every naming
  // surface and the holder's own removal until the restart re-parks against the restored source.
  assert_recovery_pinned(&mut m, &mut stores, 2, 1, NoAbsorbSm::default);
}

/// A backlogged group cannot starve its co-hosted neighbours. Each group's apply is INDEPENDENTLY
/// budget-bounded per crank: a group whose commit leapt far ahead of applied (a catch-up follower —
/// the classic budget-cut source, since a steady leader's commit paces at the storage-drain budget so
/// apply keeps up) applies at most one budget per `handle_storage` crank and reports `MorePending` to
/// be re-driven, while a co-hosted group with a small backlog applies to completion in its OWN crank —
/// the fairness witness. The backlogged group then fully drains over repeated cranks — the liveness
/// witness. Driven through the container's public message + storage path: the >budget backlog arrives
/// as ONE large committed AppendEntries (the driver's per-crank storage pass visits every group, so a
/// budget-cut group yields after its own budget rather than monopolizing the crank).
#[test]
fn a_backlogged_group_cannot_starve_its_co_hosted_neighbors() {
  use crate::endpoint::{APPLY_BUDGET_ENTRIES, MAX_READ_BATCH_ENTRIES};

  // A committed Normal entry the CountSm decodes and applies.
  let mut buf = Vec::new();
  Data::encode(&Bytes::from_static(b"x"), &mut buf);
  let payload = Bytes::from(buf);
  let entry = |i: u64| {
    crate::Entry::new(
      Term::new(1),
      Index::new(i),
      crate::EntryKind::Normal,
      payload.clone(),
    )
  };
  // Snapshot threshold ABOVE the whole backlog so no capture/compaction perturbs the drain counters.
  let cfg = || {
    Config::try_new(
      1u64,
      std::vec![1u64, 2],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap()
    .with_snapshot_threshold((10 * APPLY_BUDGET_ENTRIES) as usize)
  };

  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let (mut la, mut sa) = (VecLog::default(), AsyncStable::default());
  let (mut lb, mut sb) = (VecLog::default(), AsyncStable::default());
  m.create_group(100, 0, cfg(), Instant::ORIGIN, 42, CountSm::default())
    .unwrap();
  m.create_group(200, 0, cfg(), Instant::ORIGIN, 43, CountSm::default())
    .unwrap();
  let now = Instant::ORIGIN;

  // Group A (100): a committed backlog of THREE budgets, delivered as ONE AppendEntries from leader 2.
  // `on_append_entries` itself applies one budget then the budget cuts, so even at delivery A is far
  // from drained — a re-crank per remaining budget is required.
  let big = 3 * APPLY_BUDGET_ENTRIES;
  let a_entries: Vec<crate::Entry> = (1..=big).map(entry).collect();
  m.handle_message(
    &100,
    now,
    &mut la,
    &mut sa,
    2u64,
    Message::AppendEntries(crate::AppendEntries::new(
      Term::new(1),
      2u64,
      Index::ZERO,
      Term::ZERO,
      a_entries,
      Index::new(big),
    )),
  )
  .unwrap();
  // Group B (200): one command, delivered the same way.
  m.handle_message(
    &200,
    now,
    &mut lb,
    &mut sb,
    2u64,
    Message::AppendEntries(crate::AppendEntries::new(
      Term::new(1),
      2u64,
      Index::ZERO,
      Term::ZERO,
      std::vec![entry(1)],
      Index::new(1),
    )),
  )
  .unwrap();

  // The delivery applied one budget and the budget cut — A holds a multi-budget backlog still.
  assert!(
    m.group(&100).unwrap().applied_index() < m.group(&100).unwrap().commit_index(),
    "A's delivery cannot drain a 3-budget backlog — the budget cut it"
  );

  // B applies to completion in a bounded number of cranks WHILE A's backlog is untouched — B's
  // progress does not wait on A's backlog (the fairness witness).
  let mut b_cranks = 0u32;
  while matches!(
    m.handle_storage(&200, now, &mut lb, &mut sb),
    Some(StorageProgress::MorePending)
  ) {
    b_cranks += 1;
    assert!(b_cranks < 100, "B applies its command in bounded passes");
  }
  assert_eq!(
    m.group(&200).unwrap().applied_index(),
    m.group(&200).unwrap().commit_index(),
    "B fully applied — its completion does not wait on A's backlog"
  );
  assert!(
    m.group(&100).unwrap().applied_index() < m.group(&100).unwrap().commit_index(),
    "A is STILL draining while B is already done (the fairness witness)"
  );

  // A fully drains over repeated cranks (the liveness witness); each crank advances applied by at
  // most one budget + one batch — a backlogged group can never monopolize a crank.
  let mut cranks = 0u32;
  loop {
    let before = m.group(&100).unwrap().applied_index().get();
    let progress = m.handle_storage(&100, now, &mut la, &mut sa);
    let after = m.group(&100).unwrap().applied_index().get();
    assert!(
      after - before <= APPLY_BUDGET_ENTRIES + MAX_READ_BATCH_ENTRIES,
      "a crank applied {} entries — must be at most one budget + one batch",
      after - before
    );
    cranks += 1;
    assert!(
      cranks < 1000,
      "the budgeted drain completes in bounded cranks"
    );
    if !matches!(progress, Some(StorageProgress::MorePending)) {
      break;
    }
  }
  assert_eq!(
    m.group(&100).unwrap().applied_index(),
    m.group(&100).unwrap().commit_index(),
    "A fully drains to applied == commit"
  );
}

/// `parent` alone, carrying nothing but a STANDING fork durability barrier: a squatter created at
/// `child` parks its staged fork, so the barrier never lifts on its own and the split entry stays
/// the child's only local recovery derivation. Only the parent gets a store — a test that needs the
/// squatter to lead supplies its own. Returns the container, the stores, the split index, and the
/// parent leader's instant.
fn fork_fenced_source_fixture(
  parent: u64,
  child: u64,
) -> (MultiRaft<u64, u64, SplitSm>, MapStores, Index, Instant) {
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let now = Instant::ORIGIN;
  let (mut plog, mut pstable) = (VecLog::default(), AsyncStable::default());
  m.create_group(
    parent,
    0,
    single_node_cfg(1).with_snapshot_threshold(1),
    now,
    42,
    SplitSm::default(),
  )
  .unwrap();
  let d = lead_single_split(&mut m, parent, &mut plog, &mut pstable);
  for _ in 0..3 {
    commit_one_split(&mut m, parent, d, &mut plog, &mut pstable);
  }
  let split_idx = m
    .propose_split(
      &parent,
      d,
      &mut plog,
      &pstable,
      &child,
      0,
      Bytes::from_static(b"\x02"),
    )
    .unwrap()
    .unwrap();
  m.create_group(child, 0, single_node_cfg(1), d, 43, SplitSm::default())
    .unwrap();
  m.flush_appends(&parent, d, &plog, &pstable).unwrap();
  while matches!(
    m.handle_storage(&parent, d, &mut plog, &mut pstable),
    Some(StorageProgress::MorePending)
  ) {}
  assert!(
    m.peek_yieldable_fork(&NoHold).is_none(),
    "parked on the squatter"
  );
  assert_eq!(m.poll_split_conflict(), Some((parent, child)));
  let mut stores = MapStores(std::collections::BTreeMap::new(), Default::default());
  stores.0.insert(parent, (plog, pstable));
  (m, stores, split_idx, d)
}

/// The fork-parked-then-merge composition: a squatter at the child id parks the fork on group 1
/// (its capture fence stands at the split index), then source 2 freezes for the merge into 1 and
/// the `CommitMerge` parks above the standing fence. Returns the container, the stores, the
/// park index, and the two leaders' instants.
fn fork_fenced_park_fixture() -> (
  MultiRaft<u64, u64, SplitSm>,
  MapStores,
  Index,
  Index,
  Instant,
  Instant,
) {
  fork_fenced_park_fixture_with_target(SplitSm::default())
}

/// [`fork_fenced_park_fixture`] with the TARGET's state machine supplied — the poisoning-install
/// shape seeds one that refuses `restore`.
fn fork_fenced_park_fixture_with_target(
  target: SplitSm,
) -> (
  MultiRaft<u64, u64, SplitSm>,
  MapStores,
  Index,
  Index,
  Instant,
  Instant,
) {
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let now = Instant::ORIGIN;
  let (mut plog, mut pstable) = (VecLog::default(), AsyncStable::default());
  m.create_group(
    1,
    0,
    single_node_cfg(1).with_snapshot_threshold(1),
    now,
    42,
    target,
  )
  .unwrap();
  let d = lead_single_split(&mut m, 1, &mut plog, &mut pstable);
  for _ in 0..3 {
    commit_one_split(&mut m, 1, d, &mut plog, &mut pstable);
  }
  let split_idx = m
    .propose_split(
      &1,
      d,
      &mut plog,
      &pstable,
      &200,
      0,
      Bytes::from_static(b"\x02"),
    )
    .unwrap()
    .unwrap();
  m.create_group(200, 0, single_node_cfg(1), d, 43, SplitSm::default())
    .unwrap();
  m.flush_appends(&1, d, &plog, &pstable).unwrap();
  while matches!(
    m.handle_storage(&1, d, &mut plog, &mut pstable),
    Some(StorageProgress::MorePending)
  ) {}
  assert!(
    m.peek_yieldable_fork(&NoHold).is_none(),
    "parked on the squatter"
  );
  assert_eq!(m.poll_split_conflict(), Some((1, 200)));

  let (mut slog, mut sstable) = (VecLog::default(), AsyncStable::default());
  m.create_group(2, 0, single_node_cfg(1), now, 44, SplitSm::default())
    .unwrap();
  let ds = lead_single_split(&mut m, 2, &mut slog, &mut sstable);
  commit_one_split(&mut m, 2, ds, &mut slog, &mut sstable);
  let mut stores = MapStores(std::collections::BTreeMap::new(), Default::default());
  stores.0.insert(1, (plog, pstable));
  stores.0.insert(2, (slog, sstable));
  m.prepare_merge(&2, ds, &mut stores, &1).unwrap().unwrap();
  {
    let (l, s) = stores.0.get_mut(&2).unwrap();
    while matches!(
      m.handle_storage(&2, ds, l, s),
      Some(StorageProgress::MorePending)
    ) {}
  }
  assert!(m.group(&2).unwrap().is_frozen());

  let k = {
    let (l, s) = stores.0.get_mut(&1).unwrap();
    let k = m.commit_merge(&1, d, l, s, &2).unwrap().unwrap();
    while matches!(
      m.handle_storage(&1, d, l, s),
      Some(StorageProgress::MorePending)
    ) {}
    k
  };
  assert!(m.group(&1).unwrap().pending_merge().is_some(), "parked");
  (m, stores, k, split_idx, d, ds)
}

/// Advance a fork-fenced park to the fence-deferred absorb: seal, drain, resolve. Returns the
/// park index.
fn defer_to_absorbed(
  m: &mut MultiRaft<u64, u64, SplitSm>,
  stores: &mut MapStores,
  d: Instant,
) -> Index {
  assert!(
    m.service_merge_applies(d, stores).is_empty(),
    "the first pass only seals the window"
  );
  {
    let (l, s) = stores.0.get_mut(&1).unwrap();
    while matches!(
      m.handle_storage(&1, d, l, s),
      Some(StorageProgress::MorePending)
    ) {}
  }
  let resolutions = m.service_merge_applies(d, stores);
  assert_eq!(
    resolutions,
    std::vec![MergeResolution::Absorbed {
      source: 2,
      target: 1
    }],
    "a standing fork fence defers the capture instead of wedging the park"
  );
  m.group(&1)
    .unwrap()
    .capture_debt()
    .expect("the union owes its capture")
    .index()
}

/// The debt is HOST-LOCAL, so a foreign-led freeze can commit a debtor's consumption while the
/// debt still stands here — the propose-time refusal ran on a debt-less replica. Reproduced past
/// the door with a direct freeze append (the cross-host shape): the Resolve arm must consume the
/// debtor WITHOUT dropping the held `Merged` — the earlier absorbed source's only terminal-floor
/// permission — by discharging it into the same one-crank capture barrier, whose snapshot covers
/// the debtor's state machine and therefore the prior union it has carried since that absorb.
#[test]
fn a_foreign_led_absorb_of_a_debtor_discharges_the_inherited_debt() {
  let (mut m, mut stores, _k, _split_idx, d, _ds) = fork_fenced_park_fixture();
  defer_to_absorbed(&mut m, &mut stores, d);
  assert!(m.group(&1).unwrap().capture_debt().is_some());

  let (mut log3, mut stable3) = (VecLog::default(), AsyncStable::default());
  m.create_group(3, 0, single_node_cfg(1), d, 45, SplitSm::default())
    .unwrap();
  let d3 = lead_single_split(&mut m, 3, &mut log3, &mut stable3);
  stores.0.insert(3, (log3, stable3));

  // The foreign-led freeze on the debtor, appended past the propose gate exactly as a
  // cross-host commit arrives.
  let freeze_idx = {
    let (l, s) = stores.0.get_mut(&1).unwrap();
    let mut tb = Vec::new();
    Data::encode(&3u64, &mut tb);
    let mut fbuf = Vec::new();
    crate::wire::encode_prepare_merge_payload(
      &crate::PrepareMergePayload::new(Bytes::from(tb), 2),
      &mut fbuf,
    );
    m.group_mut(&1)
      .unwrap()
      .propose_merge_entry(d, l, crate::EntryKind::PrepareMerge, Bytes::from(fbuf))
      .unwrap();
    let idx = l.last_index();
    drain_storage(&mut m, 1, d, l, s);
    idx
  };
  assert!(m.group(&1).unwrap().is_frozen());

  {
    let (l, s) = stores.0.get_mut(&3).unwrap();
    m.group_mut(&3)
      .unwrap()
      .propose_merge_entry(
        d3,
        l,
        crate::EntryKind::CommitMerge,
        commit_merge_bytes(1, freeze_idx, 2, 1),
      )
      .unwrap();
    drain_storage(&mut m, 3, d3, l, s);
  }
  assert!(m.group(&3).unwrap().pending_merge().is_some(), "parked");

  assert!(
    m.service_merge_applies(d3, &mut stores).is_empty(),
    "the first pass only seals the window"
  );
  {
    let (l, s) = stores.0.get_mut(&3).unwrap();
    drain_storage(&mut m, 3, d3, l, s);
  }
  // The debtor's own fork barrier holds its consumption until the child's baseline is durable —
  // its log is that child's only local recovery derivation. Release it: the squatter leaves, the
  // fork yields, and the driver's flush report lifts the barrier.
  m.remove_group(&200, &mut empty_stores()).unwrap();
  let split = install_head_fork(&mut m, 1, 200, d3);
  m.lift_fork_barrier(&1, split);
  assert!(
    m.group(&1).unwrap().capture_debt().is_some(),
    "the debt is still live at the consumption: the resolver runs ahead of the debt pass"
  );
  let resolutions = m.service_merge_applies(d3, &mut stores);
  assert_eq!(
    resolutions,
    std::vec![
      MergeResolution::Merged {
        source: 1,
        target: 3
      },
      MergeResolution::Merged {
        source: 2,
        target: 3
      },
    ],
    "the inherited debt discharges into the consuming absorb's own barrier"
  );
  assert!(!m.contains_group(&1), "the debtor was consumed");
  assert_eq!(
    m.group(&3).unwrap().state_machine().units,
    2,
    "the transitive union: the debtor carried the prior absorb"
  );
  // The proto layer never touches stores; both teardowns are the driver's, off the resolutions.
  assert!(stores.0.contains_key(&1) && stores.0.contains_key(&2));
}

/// The husk twin: a foreign-led freeze husks the debtor (target unhosted) and the embedder's
/// catalog floors it terminally — the claimant's own fold retired the lineage elsewhere, and no
/// floor travels between hosts — so the retirement must surface the inherited debt alongside
/// `Retired`: the catalog's `MERGED_FLOOR` asserts the claimant's durable capture of this husk's
/// state machine, which has carried the prior union since that absorb applied.
#[test]
fn a_husked_debtor_retires_with_its_inherited_debt_discharged() {
  let (mut m, mut stores, _k, _split_idx, d, _ds) = fork_fenced_park_fixture();
  defer_to_absorbed(&mut m, &mut stores, d);
  assert!(m.group(&1).unwrap().capture_debt().is_some());

  {
    let (l, s) = stores.0.get_mut(&1).unwrap();
    let mut tb = Vec::new();
    Data::encode(&7u64, &mut tb);
    let mut fbuf = Vec::new();
    crate::wire::encode_prepare_merge_payload(
      &crate::PrepareMergePayload::new(Bytes::from(tb), 2),
      &mut fbuf,
    );
    m.group_mut(&1)
      .unwrap()
      .propose_merge_entry(d, l, crate::EntryKind::PrepareMerge, Bytes::from(fbuf))
      .unwrap();
    drain_storage(&mut m, 1, d, l, s);
  }
  assert!(m.group(&1).unwrap().is_frozen());

  stores.1.insert(1);
  // The husk's own fork barrier holds its retirement until the child's baseline is durable — its
  // log is that child's only local recovery derivation. Release it as the driver would.
  m.remove_group(&200, &mut empty_stores()).unwrap();
  let split = install_head_fork(&mut m, 1, 200, d);
  m.lift_fork_barrier(&1, split);
  assert!(
    m.group(&1).unwrap().capture_debt().is_some(),
    "the debt is still live at the retirement: the husk dissolve runs ahead of the debt pass"
  );
  let resolutions = m.service_merge_applies(d, &mut stores);
  assert_eq!(
    resolutions,
    std::vec![
      MergeResolution::Merged {
        source: 2,
        target: 1
      },
      MergeResolution::Retired { source: 1 },
    ],
    "the retirement surfaces the inherited debt on the catalog's terminal-floor evidence"
  );
  assert!(!m.contains_group(&1), "the husk dissolved");
  assert!(stores.0.contains_key(&1) && stores.0.contains_key(&2));
}

/// The fork durability barrier is HOST-LOCAL, so a sibling whose staged child's baseline already
/// flushed sees no barrier and can legally propose and commit this source's consumption — the
/// local propose-time refusal never ran. Reproduced past the door with a direct freeze append (the
/// cross-host shape): consuming the source here would drop the `Split` entry that is the staged
/// child's only local recovery derivation, so the Resolve arm HOLDS the park instead. The hold is
/// LIVE — the barrier lifts on the child's own baseline flush, which nothing the park blocks can
/// delay.
#[test]
fn a_sources_standing_fork_barrier_holds_its_consumption() {
  let (mut m, mut stores, split_idx, d) = fork_fenced_source_fixture(1, 200);
  assert!(
    m.group(&1).unwrap().capture_debt().is_none(),
    "the source carries the fork barrier and nothing else"
  );

  let (mut log3, mut stable3) = (VecLog::default(), AsyncStable::default());
  m.create_group(3, 0, single_node_cfg(1), d, 45, SplitSm::default())
    .unwrap();
  let d3 = lead_single_split(&mut m, 3, &mut log3, &mut stable3);
  stores.0.insert(3, (log3, stable3));

  // The foreign-led freeze on the fork-fenced source, appended past the propose gate exactly as a
  // cross-host commit arrives.
  let freeze_idx = {
    let (l, s) = stores.0.get_mut(&1).unwrap();
    let mut tb = Vec::new();
    Data::encode(&3u64, &mut tb);
    let mut fbuf = Vec::new();
    crate::wire::encode_prepare_merge_payload(
      &crate::PrepareMergePayload::new(Bytes::from(tb), 2),
      &mut fbuf,
    );
    m.group_mut(&1)
      .unwrap()
      .propose_merge_entry(d, l, crate::EntryKind::PrepareMerge, Bytes::from(fbuf))
      .unwrap();
    let idx = l.last_index();
    drain_storage(&mut m, 1, d, l, s);
    idx
  };
  assert!(m.group(&1).unwrap().is_frozen());

  {
    let (l, s) = stores.0.get_mut(&3).unwrap();
    m.group_mut(&3)
      .unwrap()
      .propose_merge_entry(
        d3,
        l,
        crate::EntryKind::CommitMerge,
        commit_merge_bytes(1, freeze_idx, 2, 1),
      )
      .unwrap();
    drain_storage(&mut m, 3, d3, l, s);
  }
  assert!(m.group(&3).unwrap().pending_merge().is_some(), "parked");

  assert!(
    m.service_merge_applies(d3, &mut stores).is_empty(),
    "the first pass only seals the window"
  );
  {
    let (l, s) = stores.0.get_mut(&3).unwrap();
    drain_storage(&mut m, 3, d3, l, s);
  }
  // THE HOLD: everything else about this absorb is ready, and the standing barrier alone keeps the
  // source's log — the staged child's recovery derivation — from being torn down.
  assert!(
    m.service_merge_applies(d3, &mut stores).is_empty(),
    "the standing fork barrier holds the source's consumption"
  );
  assert!(m.contains_group(&1), "the fork-fenced source stands");
  assert!(
    m.group(&3).unwrap().pending_merge().is_some(),
    "the park stands with it"
  );

  // The barrier lifts: the squatter leaves, the parked fork yields, and the driver's flush report
  // releases it — the very next crank consumes the source.
  m.remove_group(&200, &mut empty_stores()).unwrap();
  let split = install_head_fork(&mut m, 1, 200, d3);
  assert_eq!(split, split_idx);
  m.lift_fork_barrier(&1, split);
  let resolutions = m.service_merge_applies(d3, &mut stores);
  assert_eq!(
    resolutions,
    std::vec![MergeResolution::Merged {
      source: 1,
      target: 3
    }],
    "the lifted barrier releases the absorb"
  );
  assert!(!m.contains_group(&1), "the source was consumed");
}

/// The husk twin of the same host-local barrier: the absorb resolved ELSEWHERE and the embedder's
/// catalog floored the husk here (no floor travels between hosts), so no local park ever names
/// this frozen source. Retiring it while the barrier stands would destroy the staged child's only
/// local recovery derivation, so the dissolve holds — and releases on the very next crank once
/// the child's baseline is durable.
#[test]
fn a_husks_standing_fork_barrier_holds_its_retirement() {
  let (mut m, mut stores, split_idx, d) = fork_fenced_source_fixture(1, 200);

  // The foreign-led freeze toward an UNHOSTED target: nothing here can ever park against it.
  {
    let (l, s) = stores.0.get_mut(&1).unwrap();
    let mut tb = Vec::new();
    Data::encode(&7u64, &mut tb);
    let mut fbuf = Vec::new();
    crate::wire::encode_prepare_merge_payload(
      &crate::PrepareMergePayload::new(Bytes::from(tb), 2),
      &mut fbuf,
    );
    m.group_mut(&1)
      .unwrap()
      .propose_merge_entry(d, l, crate::EntryKind::PrepareMerge, Bytes::from(fbuf))
      .unwrap();
    drain_storage(&mut m, 1, d, l, s);
  }
  assert!(m.group(&1).unwrap().is_frozen());

  stores.1.insert(1);
  assert!(
    m.service_merge_applies(d, &mut stores).is_empty(),
    "the standing fork barrier holds the husk's retirement"
  );
  assert!(m.contains_group(&1), "the fork-fenced husk stands");

  m.remove_group(&200, &mut empty_stores()).unwrap();
  let split = install_head_fork(&mut m, 1, 200, d);
  assert_eq!(split, split_idx);
  m.lift_fork_barrier(&1, split);
  assert_eq!(
    m.service_merge_applies(d, &mut stores),
    std::vec![MergeResolution::Retired { source: 1 }],
    "the lifted barrier releases the retirement"
  );
  assert!(!m.contains_group(&1), "the husk dissolved");
}

/// The barrier is not the whole obligation. A covering install rebaselines past the split entry and
/// CLEARS the still-queued fork's capture barrier — the log replay it fenced is already gone — while
/// deliberately keeping the queue entry, whose in-memory blob is then the child's ONLY local
/// derivation. A barrier-keyed hold would read clear here and let the consumption destroy exactly
/// that blob, so the hold keys on the queue too.
#[test]
fn a_rebaselined_queued_fork_still_holds_its_parents_consumption() {
  let (mut m, mut stores, split_idx, d) = fork_fenced_source_fixture(1, 200);

  // The covering install, then its completion: the restore rebaselines past the split entry.
  let meta = crate::SnapshotMeta::new(
    Index::new(40),
    Term::new(2),
    crate::conf::ConfState::from_voters(std::vec![1u64]),
  )
  .with_shape_gen(1);
  {
    let (l, s) = stores.0.get_mut(&1).unwrap();
    m.handle_message(
      &1,
      d,
      l,
      s,
      9u64,
      Message::InstallSnapshot(crate::InstallSnapshot::new(
        Term::new(2),
        9u64,
        meta,
        fork_blob(9),
      )),
    )
    .unwrap();
    drain_storage(&mut m, 1, d, l, s);
    assert!(
      l.first_index() > split_idx,
      "the completion rebaselined past the split entry"
    );
  }
  let ep = m.group(&1).unwrap();
  assert!(
    !ep.fork_barrier_standing(),
    "the rebaseline retired the replay derivation and cleared its barrier"
  );
  assert!(
    ep.peek_pending_fork().is_some(),
    "the queue entry is KEPT: its blob still materializes the child"
  );
  assert!(
    ep.fork_obligations_standing(),
    "the consumption predicate still stands on the queued fork alone"
  );

  // The install deposed the parent; re-lead it so the cross-host freeze can be appended, and the
  // freeze then rides the NEW term.
  let d1 = {
    let (l, s) = stores.0.get_mut(&1).unwrap();
    lead_single_split(&mut m, 1, l, s)
  };
  let (mut log3, mut stable3) = (VecLog::default(), AsyncStable::default());
  m.create_group(3, 0, single_node_cfg(1), d1, 45, SplitSm::default())
    .unwrap();
  let d3 = lead_single_split(&mut m, 3, &mut log3, &mut stable3);
  stores.0.insert(3, (log3, stable3));

  let freeze_idx = {
    let (l, s) = stores.0.get_mut(&1).unwrap();
    let mut tb = Vec::new();
    Data::encode(&3u64, &mut tb);
    let mut fbuf = Vec::new();
    crate::wire::encode_prepare_merge_payload(
      &crate::PrepareMergePayload::new(Bytes::from(tb), 2),
      &mut fbuf,
    );
    m.group_mut(&1)
      .unwrap()
      .propose_merge_entry(d1, l, crate::EntryKind::PrepareMerge, Bytes::from(fbuf))
      .unwrap();
    let idx = l.last_index();
    drain_storage(&mut m, 1, d1, l, s);
    idx
  };
  let freeze_term = m
    .group(&1)
    .unwrap()
    .freeze_term()
    .expect("the applied freeze recorded its term");
  {
    let (l, s) = stores.0.get_mut(&3).unwrap();
    m.group_mut(&3)
      .unwrap()
      .propose_merge_entry(
        d3,
        l,
        crate::EntryKind::CommitMerge,
        commit_merge_bytes_at(1, freeze_idx, freeze_term, 2, 1),
      )
      .unwrap();
    drain_storage(&mut m, 3, d3, l, s);
  }
  assert!(m.group(&3).unwrap().pending_merge().is_some(), "parked");

  assert!(
    m.service_merge_applies(d3, &mut stores).is_empty(),
    "the first pass only seals the window"
  );
  {
    let (l, s) = stores.0.get_mut(&3).unwrap();
    drain_storage(&mut m, 3, d3, l, s);
  }
  // THE HOLD, on a source whose capture barrier reads CLEAR.
  assert!(
    m.service_merge_applies(d3, &mut stores).is_empty(),
    "the queued fork holds the consumption though its barrier was rebaselined away"
  );
  assert!(m.contains_group(&1), "the source stands");
  assert!(
    m.group(&1).unwrap().peek_pending_fork().is_some(),
    "the blob was not destroyed"
  );

  // The release: the squatter leaves and the install consumes the fork, emptying the queue.
  // Nothing is left to lift — the rebaseline already took the barrier — so the very next crank
  // consumes the source.
  m.remove_group(&200, &mut empty_stores()).unwrap();
  assert_eq!(install_head_fork(&mut m, 1, 200, d3), split_idx);
  assert!(
    !m.group(&1).unwrap().fork_obligations_standing(),
    "the relay emptied the queue"
  );
  assert_eq!(
    m.service_merge_applies(d3, &mut stores),
    std::vec![MergeResolution::Merged {
      source: 1,
      target: 3
    }],
    "the emptied queue releases the absorb"
  );
  assert!(!m.contains_group(&1), "the source was consumed");
}

/// WHAT SURVIVES WHAT, pinned. A HELD fork's retained blob is the child's only local derivation once
/// a covering install has crossed this host's fences, and that blob is IN-MEMORY: it buys process
/// lifetime, not durability. Every arm exercised here is intended behaviour rather than a defect
/// awaiting a fix, so no future reader has to re-derive the durability story from the arms.
///
/// PHASE ONE, live process. The fork is held on an occupied child id, so its capture fence stands on
/// its own log replay. The covering install rebaselines past the split entry and clears that fence
/// while KEEPING the queue entry, so the consumption predicate still stands on the queue alone and
/// [`MultiRaft::fork_derivation_volatile`] reports the window open.
///
/// PHASE TWO, the restart. Restoring the SAME durable stores into a fresh container replays a log
/// the rebaseline already truncated past the split entry, so nothing re-stages: the parent owes
/// nothing, fences nothing, and signals nothing. That is the loss composite's last term — a held
/// fork, a holder behind the covering boundary, an unfenced voter covering it, then a crash — and it
/// is why the predicate names a deadline rather than a condition to wait out. (TRUE loss of the
/// partition needs the child to have materialized nowhere; a sibling that installed it holds the
/// same content.)
#[test]
fn a_held_forks_derivation_is_process_lifetime_only_after_a_covering_install() {
  let (mut m, mut stores, split_idx, d) = fork_fenced_source_fixture(1, 200);
  assert!(
    m.group(&1).unwrap().fork_barrier_standing(),
    "the held fork's capture fence stands on its own log replay"
  );
  assert!(
    !m.fork_derivation_volatile(&1),
    "while that fence stands the derivation survives a restart, so no window is open"
  );

  // The covering install, then its completion: the restore rebaselines past the split entry.
  let meta = crate::SnapshotMeta::new(
    Index::new(40),
    Term::new(2),
    crate::conf::ConfState::from_voters(std::vec![1u64]),
  )
  .with_shape_gen(1);
  {
    let (l, s) = stores.0.get_mut(&1).unwrap();
    m.handle_message(
      &1,
      d,
      l,
      s,
      9u64,
      Message::InstallSnapshot(crate::InstallSnapshot::new(
        Term::new(2),
        9u64,
        meta,
        fork_blob(9),
      )),
    )
    .unwrap();
    drain_storage(&mut m, 1, d, l, s);
    assert!(
      l.first_index() > split_idx,
      "the completion rebaselined past the split entry"
    );
  }
  {
    let ep = m.group(&1).unwrap();
    assert!(
      !ep.fork_barrier_standing(),
      "the rebaseline retired the replay derivation and cleared its barrier"
    );
    assert!(
      ep.peek_pending_fork().is_some(),
      "the queue entry is KEPT: its in-memory blob still materializes the child"
    );
    assert!(
      ep.fork_obligations_standing(),
      "so the consumption predicate stands on the queued fork alone"
    );
  }
  assert!(
    m.fork_derivation_volatile(&1),
    "obligations standing with the barrier gone IS the volatile window"
  );

  // PHASE TWO. The restart path: the same durable stores, a fresh container.
  drop(m);
  let (mut log, mut stable) = stores.0.remove(&1).expect("the parent's durable stores");
  let mut m2: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  m2.restore_group_unchecked(
    1,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    SplitSm::default(),
    2,
    &mut log,
    &mut stable,
  )
  .unwrap();
  let ep = m2.group(&1).unwrap();
  assert_eq!(
    ep.staged_forks().count(),
    0,
    "the split entry left the log with the rebaseline, so the replay re-stages nothing"
  );
  assert!(
    !ep.fork_obligations_standing(),
    "the restarted parent owes no partition — the blob did not survive the process"
  );
  assert!(
    !ep.fork_barrier_standing(),
    "and fences nothing, so a later capture is free"
  );
  assert!(
    !m2.fork_derivation_volatile(&1),
    "the predicate reports no window because there is no fork left to lose"
  );
  assert_eq!(
    m2.poll_split_conflict(),
    None,
    "and no cue survives to suggest otherwise"
  );
}

/// The composition with NO local release: the parked fork's child id IS the merge target. The fork
/// waits on the occupant, the occupant is `MergeParked` on this very absorb, and the absorb waits on
/// the fork — a three-way wait no crank can break. The hold is still the right answer: without it
/// this shape SILENTLY DROPPED the split-away half at consumption, and a loud, signalled wedge an
/// embedder can act on is strictly better than silent data loss. The `ForkFence` observation is that
/// signal; the release protocol for the composition is tracked separately.
#[test]
fn a_fork_whose_child_is_the_merge_target_wedges_loudly() {
  // Source 5 splits child 2 and 2 is occupied, so the fork parks; 5 → 2 is direction-valid.
  let (mut m, mut stores, _split_idx, d) = fork_fenced_source_fixture(5, 2);
  let (mut log2, mut stable2) = (VecLog::default(), AsyncStable::default());
  let d2 = lead_single_split(&mut m, 2, &mut log2, &mut stable2);
  commit_one_split(&mut m, 2, d2, &mut log2, &mut stable2);
  stores.0.insert(2, (log2, stable2));

  // The foreign-led freeze of the parent toward its own child id, appended past the propose gate
  // (which refuses it locally — the source's staged fork stands) exactly as a cross-host commit
  // arrives.
  let freeze_idx = {
    let (l, s) = stores.0.get_mut(&5).unwrap();
    let mut tb = Vec::new();
    Data::encode(&2u64, &mut tb);
    let mut fbuf = Vec::new();
    crate::wire::encode_prepare_merge_payload(
      &crate::PrepareMergePayload::new(Bytes::from(tb), 2),
      &mut fbuf,
    );
    m.group_mut(&5)
      .unwrap()
      .propose_merge_entry(d, l, crate::EntryKind::PrepareMerge, Bytes::from(fbuf))
      .unwrap();
    let idx = l.last_index();
    drain_storage(&mut m, 5, d, l, s);
    idx
  };
  assert!(m.group(&5).unwrap().is_frozen());
  {
    let (l, s) = stores.0.get_mut(&2).unwrap();
    m.group_mut(&2)
      .unwrap()
      .propose_merge_entry(
        d2,
        l,
        crate::EntryKind::CommitMerge,
        commit_merge_bytes(5, freeze_idx, 2, 1),
      )
      .unwrap();
    drain_storage(&mut m, 2, d2, l, s);
  }
  let park = m.group(&2).unwrap().pending_merge().expect("parked").at();

  assert!(
    m.service_merge_applies(d2, &mut stores).is_empty(),
    "the first pass only seals the window"
  );
  {
    let (l, s) = stores.0.get_mut(&2).unwrap();
    drain_storage(&mut m, 2, d2, l, s);
  }
  // The wedge, re-derived every crank and never resolving on its own.
  for _ in 0..4 {
    assert!(
      m.service_merge_applies(d2, &mut stores).is_empty(),
      "nothing resolves: the fork and the park each wait on the other"
    );
  }
  assert!(m.contains_group(&5), "the source stands, blob intact");
  assert!(
    m.group(&5).unwrap().peek_pending_fork().is_some(),
    "the split-away half was not dropped — the whole point of the hold"
  );
  assert!(
    m.group(&2).unwrap().pending_merge().is_some(),
    "the park stands with it"
  );

  // The loud half: ONE observation naming the pair, deduped across every repeat crank.
  assert_eq!(
    m.poll_merge_blocked(),
    Some(crate::MergeBlocked {
      target: 2,
      source: 5,
      boundary: park,
      cause: MergeBlockedCause::ForkFence,
    }),
    "the wedge is signalled with an actionable identity"
  );
  assert_eq!(
    m.poll_merge_blocked(),
    None,
    "the edge dedupe absorbs the per-crank repeats"
  );
}

/// The Defer twin: the consuming target's own capture is fenced by an ABORT obligation whose
/// clearing rides its embedder timescale, so holding the park for a Clear classification would
/// be a circular wait — the fence stands exactly as long as the park does. A deferred absorb of
/// a debt-carrying source therefore proceeds, CHAINING the consumed debtor's debts onto the
/// target's own minted debt, and one later covering capture discharges the entire chain: the
/// fold just absorbed has carried every prior union since its absorb applied.
#[test]
fn a_deferred_absorb_chains_the_consumed_debtors_debts() {
  let (mut m, mut stores, _k, _split_idx, d, _ds) = fork_fenced_park_fixture();
  defer_to_absorbed(&mut m, &mut stores, d);
  assert!(m.group(&1).unwrap().capture_debt().is_some());

  // The consuming target 3, its capture fenced by an undischarged abort obligation for the
  // unhosted 8 (its clearing waits on 8's floor — the embedder's timescale, not this park's).
  let (mut log3, mut stable3) = (VecLog::default(), AsyncStable::default());
  m.create_group(3, 0, single_node_cfg(1), d, 45, SplitSm::default())
    .unwrap();
  let d3 = lead_single_split(&mut m, 3, &mut log3, &mut stable3);
  stores.0.insert(3, (log3, stable3));
  {
    let (l, s) = stores.0.get_mut(&3).unwrap();
    let mut sb = Vec::new();
    Data::encode(&8u64, &mut sb);
    let abort = crate::RollbackMergePayload::abort(Bytes::from(sb), 1, 1);
    let mut buf = Vec::new();
    crate::wire::encode_rollback_merge_payload(&abort, &mut buf);
    m.group_mut(&3)
      .unwrap()
      .propose_merge_entry(d3, l, crate::EntryKind::RollbackMerge, Bytes::from(buf))
      .unwrap();
    drain_storage(&mut m, 3, d3, l, s);
  }
  assert!(
    m.group(&3).unwrap().owes_live_thaw(),
    "the abort fence stands"
  );

  // The foreign-led freeze on the debtor, then its committed absorb into 3.
  let freeze_idx = {
    let (l, s) = stores.0.get_mut(&1).unwrap();
    let mut tb = Vec::new();
    Data::encode(&3u64, &mut tb);
    let mut fbuf = Vec::new();
    crate::wire::encode_prepare_merge_payload(
      &crate::PrepareMergePayload::new(Bytes::from(tb), 2),
      &mut fbuf,
    );
    m.group_mut(&1)
      .unwrap()
      .propose_merge_entry(d, l, crate::EntryKind::PrepareMerge, Bytes::from(fbuf))
      .unwrap();
    let idx = l.last_index();
    drain_storage(&mut m, 1, d, l, s);
    idx
  };
  assert!(m.group(&1).unwrap().is_frozen());
  {
    let (l, s) = stores.0.get_mut(&3).unwrap();
    m.group_mut(&3)
      .unwrap()
      .propose_merge_entry(
        d3,
        l,
        crate::EntryKind::CommitMerge,
        commit_merge_bytes(1, freeze_idx, 2, 2),
      )
      .unwrap();
    drain_storage(&mut m, 3, d3, l, s);
  }
  let park = m.group(&3).unwrap().pending_merge().expect("parked").at();

  assert!(
    m.service_merge_applies(d3, &mut stores).is_empty(),
    "the first pass only seals the window"
  );
  {
    let (l, s) = stores.0.get_mut(&3).unwrap();
    drain_storage(&mut m, 3, d3, l, s);
  }
  // The debtor's own fork barrier holds its consumption until the child's baseline is durable;
  // release it so the abort fence on the TARGET is the only fence this arm still classifies on.
  m.remove_group(&200, &mut empty_stores()).unwrap();
  let split = install_head_fork(&mut m, 1, 200, d3);
  m.lift_fork_barrier(&1, split);
  assert!(
    m.group(&1).unwrap().capture_debt().is_some(),
    "the debt is still live at the consumption: the resolver runs ahead of the debt pass"
  );
  let resolutions = m.service_merge_applies(d3, &mut stores);
  assert_eq!(
    resolutions,
    std::vec![MergeResolution::Absorbed {
      source: 1,
      target: 3
    }],
    "the deferred absorb proceeds — holding here would wait circularly on its own park"
  );
  {
    let (l, s) = stores.0.get_mut(&3).unwrap();
    drain_storage(&mut m, 3, d3, l, s);
  }
  let tep = m.group(&3).unwrap();
  assert!(tep.pending_merge().is_none(), "unparked: the drain resumed");
  assert!(tep.applied_index() >= park, "applies moved past the park");
  assert_eq!(tep.state_machine().units, 2, "the transitive union folded");
  assert_eq!(
    tep.capture_debt().expect("the own debt minted").source(),
    {
      let mut b = Vec::new();
      Data::encode(&1u64, &mut b);
      Bytes::from(b)
    },
    "the own debt names the consumed debtor"
  );
  assert!(!m.contains_group(&1), "the debtor was consumed");
  assert!(
    m.service_merge_applies(d3, &mut stores).is_empty(),
    "the chain waits: the abort fence still stands"
  );
  // Every chained source is admission-fenced while the chain stands — create, restore, fork,
  // and factory all route through the same shared refusal.
  for pinned in [1u64, 2] {
    assert!(
      matches!(
        m.create_group(pinned, 0, single_node_cfg(1), d3, 99, SplitSm::default()),
        Err(crate::multi::CreateGroupError::AbsorbPending)
      ),
      "a chained debt pins {pinned}'s admission"
    );
  }

  // The obligation clears on the embedder's floor, THROUGH the committed thaw witness: the
  // leader appends it, and the apply — possible at all only because the deferred absorb
  // unparked the drain — clears the map. ONE covering capture then discharges the ENTIRE
  // chain, the own debt and the inherited one.
  stores.1.insert(8);
  assert!(
    m.service_merge_applies(d3, &mut stores).is_empty(),
    "the witness appends first"
  );
  {
    let (l, s) = stores.0.get_mut(&3).unwrap();
    m.flush_appends(&3, d3, l, s).unwrap();
    drain_storage(&mut m, 3, d3, l, s);
  }
  assert!(
    !m.group(&3).unwrap().owes_live_thaw(),
    "the applied witness cleared the obligation"
  );
  let resolutions = m.service_merge_applies(d3, &mut stores);
  assert_eq!(
    resolutions,
    std::vec![
      MergeResolution::Merged {
        source: 1,
        target: 3
      },
      MergeResolution::Merged {
        source: 2,
        target: 3
      },
    ],
    "one covering capture discharges the whole chain"
  );
  assert!(m.group(&3).unwrap().capture_debt().is_none());
  assert!(stores.0.contains_key(&1) && stores.0.contains_key(&2));
  // The discharge self-releases the admission fence.
  m.create_group(2, 0, single_node_cfg(1), d3, 99, SplitSm::default())
    .unwrap();
}

/// THE FENCED CAPTURE DEBT IS NAMED BY ITS FENCE (#132): target 1 holds an abort record for
/// source 8 — hosted nowhere — and absorbs frozen source 2 deferred behind that record's fence,
/// minting a capture debt. The debt pass reports the wait as `AbortFence` naming 8, the record
/// that fences (whose thaw's witness is the exit) — never 2, the source the union consumed —
/// exactly as the adopt pass names its own fenced capture.
#[test]
fn a_fenced_capture_debt_is_reported_by_its_fencing_source() {
  let now = Instant::ORIGIN;
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let mut stores = MapStores(std::collections::BTreeMap::new(), Default::default());
  // Target 1 leads and commits the abort for 8.
  let (mut log1, mut stable1) = (VecLog::default(), AsyncStable::default());
  m.create_group(1, 0, single_node_cfg(1), now, 45, SplitSm::default())
    .unwrap();
  let d = lead_single_split(&mut m, 1, &mut log1, &mut stable1);
  stores.0.insert(1, (log1, stable1));
  {
    let (l, s) = stores.0.get_mut(&1).unwrap();
    let abort = crate::RollbackMergePayload::abort(gid_key(8), 1, 1);
    let mut buf = Vec::new();
    crate::wire::encode_rollback_merge_payload(&abort, &mut buf);
    m.group_mut(&1)
      .unwrap()
      .propose_merge_entry(d, l, crate::EntryKind::RollbackMerge, Bytes::from(buf))
      .unwrap();
    drain_storage(&mut m, 1, d, l, s);
  }
  assert!(
    m.group(&1).unwrap().owes_live_thaw(),
    "the abort fence stands"
  );
  // Source 2 leads and freezes for 1.
  let (mut log2, mut stable2) = (VecLog::default(), AsyncStable::default());
  m.create_group(2, 0, single_node_cfg(1), d, 46, SplitSm::default())
    .unwrap();
  let d2 = lead_single_split(&mut m, 2, &mut log2, &mut stable2);
  stores.0.insert(2, (log2, stable2));
  let freeze_idx = {
    let (l, s) = stores.0.get_mut(&2).unwrap();
    let mut fbuf = Vec::new();
    crate::wire::encode_prepare_merge_payload(
      &crate::PrepareMergePayload::new(gid_key(1), 1),
      &mut fbuf,
    );
    m.group_mut(&2)
      .unwrap()
      .propose_merge_entry(d2, l, crate::EntryKind::PrepareMerge, Bytes::from(fbuf))
      .unwrap();
    let idx = l.last_index();
    drain_storage(&mut m, 2, d2, l, s);
    idx
  };
  assert!(m.group(&2).unwrap().is_frozen());
  // The committed absorb parks 1; the resolver absorbs it DEFERRED — the abort fence stands at
  // the boundary — minting the debt.
  {
    let (l, s) = stores.0.get_mut(&1).unwrap();
    m.group_mut(&1)
      .unwrap()
      .propose_merge_entry(
        d,
        l,
        crate::EntryKind::CommitMerge,
        commit_merge_bytes(2, freeze_idx, 1, 2),
      )
      .unwrap();
    drain_storage(&mut m, 1, d, l, s);
  }
  let park = m.group(&1).unwrap().pending_merge().expect("parked").at();
  assert!(
    m.service_merge_applies(d, &mut stores).is_empty(),
    "the first pass only seals the window"
  );
  {
    let (l, s) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, d, l, s);
  }
  while m.poll_merge_blocked().is_some() {}
  assert_eq!(
    m.service_merge_applies(d, &mut stores),
    std::vec![MergeResolution::Absorbed {
      source: 2,
      target: 1
    }],
    "the deferred absorb proceeds behind the fence"
  );
  {
    let (l, s) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, d, l, s);
  }
  assert_eq!(
    m.group(&1)
      .unwrap()
      .capture_debt()
      .map(|debt| debt.source()),
    Some(gid_key(2)),
    "the debt names the source it consumed"
  );

  // The debt pass: the capture is refused behind the abort record, and the wait is reported by
  // the record that fences — once, however many cranks re-derive it.
  assert!(m.service_merge_applies(d, &mut stores).is_empty());
  assert_eq!(
    m.poll_merge_blocked(),
    Some(crate::MergeBlocked {
      target: 1,
      source: 8,
      boundary: park,
      cause: MergeBlockedCause::AbortFence,
    }),
    "named by the fencing record — its thaw's witness is the exit — not by the consumed source"
  );
  assert_eq!(
    m.poll_merge_blocked(),
    None,
    "one signal for the standing wait"
  );
  assert!(
    m.group(&1).unwrap().capture_debt().is_some(),
    "the debt stands behind the fence"
  );
}

/// A crash replays the chain: the inner `CommitMerge` re-parks while the LATER committed
/// `PrepareMerge` re-arms only as PENDING — its apply sits above the park, which is exactly
/// what the park keeps from draining. A pending freeze ABOVE the fold's boundary must not Hold
/// the absorb (the entry survives the capture's compaction untouched); holding would be a
/// permanent circular wait, with the downstream target parked against this very freeze.
#[test]
fn a_replayed_chain_resolves_the_inner_park_beneath_a_pending_freeze() {
  let now = Instant::ORIGIN;
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let cmd = {
    let mut buf = Vec::new();
    Bytes::from_static(b"c").encode(&mut buf);
    Bytes::from(buf)
  };
  // The consumed-source replica, replaying to its freeze.
  let (mut log2, mut stable2) = (VecLog::default(), AsyncStable::default());
  {
    let mut tb = Vec::new();
    Data::encode(&1u64, &mut tb);
    let mut fbuf = Vec::new();
    crate::wire::encode_prepare_merge_payload(
      &crate::PrepareMergePayload::new(Bytes::from(tb), 1),
      &mut fbuf,
    );
    log2.force_append(&[
      crate::Entry::new(
        Term::new(1),
        Index::new(1),
        crate::EntryKind::Normal,
        cmd.clone(),
      ),
      crate::Entry::new(
        Term::new(1),
        Index::new(2),
        crate::EntryKind::PrepareMerge,
        Bytes::from(fbuf),
      ),
    ]);
    stable2.force_state(Term::new(1), Some(1u64), Index::new(2));
  }
  m.restore_group_unchecked(
    2,
    single_node_cfg(1),
    now,
    8,
    SplitSm::default(),
    1,
    &mut log2,
    &mut stable2,
  )
  .unwrap();
  assert!(
    m.group(&2).unwrap().is_frozen(),
    "the source replays frozen"
  );

  // The target replica: the inner absorb parks it, the later freeze re-arms pending above.
  let (mut log1, mut stable1) = (VecLog::default(), AsyncStable::default());
  {
    let mut tb = Vec::new();
    Data::encode(&0u64, &mut tb);
    let mut fbuf = Vec::new();
    crate::wire::encode_prepare_merge_payload(
      &crate::PrepareMergePayload::new(Bytes::from(tb), 2),
      &mut fbuf,
    );
    log1.force_append(&[
      crate::Entry::new(
        Term::new(1),
        Index::new(1),
        crate::EntryKind::Normal,
        cmd.clone(),
      ),
      crate::Entry::new(
        Term::new(1),
        Index::new(2),
        crate::EntryKind::CommitMerge,
        commit_merge_bytes(2, Index::new(2), 1, 1),
      ),
      crate::Entry::new(
        Term::new(1),
        Index::new(3),
        crate::EntryKind::PrepareMerge,
        Bytes::from(fbuf),
      ),
    ]);
    stable1.force_state(Term::new(1), Some(1u64), Index::new(3));
  }
  m.restore_group_unchecked(
    1,
    single_node_cfg(1),
    now,
    7,
    SplitSm::default(),
    1,
    &mut log1,
    &mut stable1,
  )
  .unwrap();
  let tep = m.group(&1).unwrap();
  assert!(
    tep.pending_merge().is_some(),
    "re-parked at the inner absorb"
  );
  assert!(
    tep.merge_freeze_active(),
    "the later freeze re-armed as pending"
  );

  let mut stores = MapStores(std::collections::BTreeMap::new(), Default::default());
  stores.0.insert(1, (log1, stable1));
  stores.0.insert(2, (log2, stable2));
  let resolutions = m.service_merge_applies(now, &mut stores);
  assert_eq!(
    resolutions,
    std::vec![MergeResolution::Merged {
      source: 2,
      target: 1
    }],
    "the pending freeze above the boundary leaves the inner fold free"
  );
  {
    let (l, s) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, l, s);
  }
  let tep = m.group(&1).unwrap();
  assert!(tep.pending_merge().is_none(), "the park resolved");
  assert!(
    tep.is_frozen(),
    "the drain resumed and the later freeze applied — no circular wait"
  );
  assert_eq!(tep.state_machine().units, 2, "the union folded first");
}

/// Deliver a covering destructive install at index 40 to group `gid` (a single-voter host at
/// term 1 in these fixtures) and drain its completion: the blob is durable, the destructive body
/// ran, the log is re-baselined past the absorb point.
fn covering_install_completes(
  m: &mut MultiRaft<u64, u64, SplitSm>,
  stores: &mut MapStores,
  gid: u64,
  d: Instant,
) {
  let meta = crate::SnapshotMeta::new(
    Index::new(40),
    Term::new(2),
    crate::conf::ConfState::from_voters(std::vec![1u64]),
  )
  .with_shape_gen(1);
  let (l, s) = stores.0.get_mut(&gid).unwrap();
  m.handle_message(
    &gid,
    d,
    l,
    s,
    9u64,
    Message::InstallSnapshot(crate::InstallSnapshot::new(
      Term::new(2),
      9u64,
      meta,
      fork_blob(9),
    )),
  )
  .unwrap();
  drain_storage(m, gid, d, l, s);
}

/// A covering destructive install DISCHARGES the debt chain instead of dropping it. The blob
/// at-or-past the absorb boundary is the union's durable form on this host, and the terminal
/// floor is this host's own write — nothing propagates it — so the held `Merged` is
/// dischargeable here and only here: the chain survives the re-baseline, and the same crank's
/// debt pass surfaces exactly one `Merged` (with the app-visible event) on the install's own
/// durable evidence. Nothing surfaces again, and the naming ends with the discharge.
#[test]
fn a_covering_install_discharges_the_debt_chain() {
  let (mut m, mut stores, _k, _split_idx, d, _ds) = fork_fenced_park_fixture();
  defer_to_absorbed(&mut m, &mut stores, d);
  while m.poll_event().is_some() {}
  let units_before = m.group(&1).unwrap().state_machine().units;
  let meta = crate::SnapshotMeta::new(
    Index::new(40),
    Term::new(2),
    crate::conf::ConfState::from_voters(std::vec![1u64]),
  )
  .with_shape_gen(1);
  {
    let (l, s) = stores.0.get_mut(&1).unwrap();
    m.handle_message(
      &1,
      d,
      l,
      s,
      9u64,
      Message::InstallSnapshot(crate::InstallSnapshot::new(
        Term::new(2),
        9u64,
        meta,
        fork_blob(9),
      )),
    )
    .unwrap();
  }
  let tep = m.group(&1).unwrap();
  assert_eq!(
    tep.state_machine().units,
    units_before,
    "the deferral holds the destructive body until the blob is durable"
  );
  assert!(
    tep.capture_debt().is_some(),
    "the chain stands until the completion actually re-baselines"
  );
  {
    let (l, s) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, d, l, s);
    assert!(
      l.first_index() > Index::new(1),
      "the completion re-baselined past the window"
    );
  }
  let tep = m.group(&1).unwrap();
  assert!(
    tep.capture_debt().is_some(),
    "the chain survives the re-baseline: the debt pass discharges it, the install never drops it"
  );
  assert_eq!(tep.state_machine().units, 9, "the blob IS the new baseline");
  assert!(m.debt_names(&2), "the naming stands until the discharge");
  let resolutions = m.service_merge_applies(d, &mut stores);
  assert_eq!(
    resolutions,
    std::vec![MergeResolution::Merged {
      source: 2,
      target: 1
    }],
    "the install's own durable evidence discharges the debt in the same crank"
  );
  let tep = m.group(&1).unwrap();
  assert!(tep.capture_debt().is_none(), "the debt is discharged");
  assert!(!m.debt_names(&2), "the naming dies with the discharge");
  let mut merged_events = 0;
  while let Some((gid, ev)) = m.poll_event() {
    if let Event::Merged(e) = ev {
      assert_eq!(gid, 1);
      assert_eq!(e.source(), gid_key(2));
      merged_events += 1;
    }
  }
  assert_eq!(
    merged_events, 1,
    "the union event rides the discharge, exactly once"
  );
  assert!(
    m.service_merge_applies(d, &mut stores).is_empty(),
    "nothing surfaces twice"
  );
  // The proto layer never touches stores; the teardown is the driver's, off the resolution.
  assert!(stores.0.contains_key(&2));
}

/// Consume `holder` — a live debtor — into a NEW led target `target` through a fence-DEFERRED
/// absorb, chaining the holder's whole debt chain onto the target: the target is created and led,
/// fenced by an undischarged abort obligation for the unhosted `dead_source`, then a foreign-led
/// freeze on the holder and the target's committed absorb resolve `Absorbed`. `instants` carries
/// each leader's instant (the holder's is read, the target's recorded). The fixture's debtor (1)
/// carries the fork barrier that holds its consumption; it is released here, at its consumption.
fn defer_holder_into_fenced_target(
  m: &mut MultiRaft<u64, u64, SplitSm>,
  stores: &mut MapStores,
  instants: &mut std::collections::BTreeMap<u64, Instant>,
  holder: u64,
  target: u64,
  dead_source: u64,
) {
  // Every target is created at the fixture's own instant, the debtor's.
  let d = instants[&1];
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  m.create_group(target, 0, single_node_cfg(1), d, 45, SplitSm::default())
    .unwrap();
  let dt = lead_single_split(m, target, &mut log, &mut stable);
  instants.insert(target, dt);
  stores.0.insert(target, (log, stable));
  {
    let (l, s) = stores.0.get_mut(&target).unwrap();
    let abort = crate::RollbackMergePayload::abort(gid_key(dead_source), 1, 1);
    let mut buf = Vec::new();
    crate::wire::encode_rollback_merge_payload(&abort, &mut buf);
    m.group_mut(&target)
      .unwrap()
      .propose_merge_entry(dt, l, crate::EntryKind::RollbackMerge, Bytes::from(buf))
      .unwrap();
    drain_storage(m, target, dt, l, s);
  }
  assert!(
    m.group(&target).unwrap().owes_live_thaw(),
    "the abort fence stands on {target}"
  );

  // The foreign-led freeze on the holder, then its committed absorb into the target.
  let dh = instants[&holder];
  let freeze_gen = m.group(&holder).unwrap().shape_gen() + 1;
  let freeze_idx = {
    let (l, s) = stores.0.get_mut(&holder).unwrap();
    let mut fbuf = Vec::new();
    crate::wire::encode_prepare_merge_payload(
      &crate::PrepareMergePayload::new(gid_key(target), freeze_gen),
      &mut fbuf,
    );
    m.group_mut(&holder)
      .unwrap()
      .propose_merge_entry(dh, l, crate::EntryKind::PrepareMerge, Bytes::from(fbuf))
      .unwrap();
    let idx = l.last_index();
    drain_storage(m, holder, dh, l, s);
    idx
  };
  assert!(m.group(&holder).unwrap().is_frozen());
  {
    let target_gen = m.group(&target).unwrap().shape_gen() + 1;
    let (l, s) = stores.0.get_mut(&target).unwrap();
    m.group_mut(&target)
      .unwrap()
      .propose_merge_entry(
        dt,
        l,
        crate::EntryKind::CommitMerge,
        commit_merge_bytes(holder, freeze_idx, freeze_gen, target_gen),
      )
      .unwrap();
    drain_storage(m, target, dt, l, s);
  }
  assert!(
    m.group(&target).unwrap().pending_merge().is_some(),
    "parked on {target}"
  );
  assert!(
    m.service_merge_applies(dt, stores).is_empty(),
    "the first pass only seals the window"
  );
  {
    let (l, s) = stores.0.get_mut(&target).unwrap();
    drain_storage(m, target, dt, l, s);
  }
  if holder == 1 {
    // The fixture's debtor carries the fork barrier that holds its consumption; release it
    // only now, so the resolver (which runs ahead of the debt pass) consumes the debtor with
    // its debt still live.
    m.remove_group(&200, &mut empty_stores()).unwrap();
    let split = install_head_fork(m, 1, 200, dt);
    m.lift_fork_barrier(&1, split);
  }
  assert!(
    m.group(&holder).unwrap().capture_debt().is_some(),
    "the holder's debt is live at its consumption"
  );
  assert_eq!(
    m.service_merge_applies(dt, stores),
    std::vec![MergeResolution::Absorbed {
      source: holder,
      target
    }],
    "the abort fence defers the capture, chaining the holder's debts onto {target}"
  );
  {
    let (l, s) = stores.0.get_mut(&target).unwrap();
    drain_storage(m, target, dt, l, s);
  }
  assert!(!m.contains_group(&holder), "the holder was consumed");
}

/// Two levels up a chain: the debtor is consumed by a fence-deferred absorb (its debt is
/// inherited), then that holder by another (both are inherited), so the final holder owns one
/// debt and carries two. One covering install discharges every level on the same evidence —
/// one `Merged` per source, each exactly once, none the crank after.
#[test]
fn a_covering_install_discharges_every_level_of_an_inherited_chain() {
  let (mut m, mut stores, _k, _split_idx, d, _ds) = fork_fenced_park_fixture();
  defer_to_absorbed(&mut m, &mut stores, d);
  let mut instants = std::collections::BTreeMap::from([(1u64, d)]);

  // Each consuming target's capture is fenced by an undischarged abort obligation for an
  // unhosted source, so its absorb DEFERS and inherits the consumed holder's chain (the
  // abort-fenced twin of the fork-fenced shape the fixture starts from).
  for (holder, target, dead_source) in [(1u64, 3u64, 8u64), (3, 4, 9)] {
    defer_holder_into_fenced_target(
      &mut m,
      &mut stores,
      &mut instants,
      holder,
      target,
      dead_source,
    );
  }
  let d4 = instants[&4];
  let tep = m.group(&4).unwrap();
  assert_eq!(tep.state_machine().units, 2, "the transitive union");
  assert_eq!(
    tep.capture_debt().expect("the own debt").source(),
    gid_key(3),
    "the own debt names the consumed holder"
  );
  for named in [1u64, 2, 3] {
    assert!(m.debt_names(&named), "the chain names {named}");
  }
  assert!(
    m.service_merge_applies(d4, &mut stores).is_empty(),
    "the chain waits: the abort fence still stands"
  );

  covering_install_completes(&mut m, &mut stores, 4, d4);
  let tep = m.group(&4).unwrap();
  assert_eq!(tep.state_machine().units, 9, "the blob IS the new baseline");
  assert!(
    tep.capture_debt().is_some(),
    "the whole chain survives the re-baseline"
  );
  let resolutions = m.service_merge_applies(d4, &mut stores);
  assert_eq!(
    resolutions,
    std::vec![
      MergeResolution::Merged {
        source: 3,
        target: 4
      },
      MergeResolution::Merged {
        source: 1,
        target: 4
      },
      MergeResolution::Merged {
        source: 2,
        target: 4
      },
    ],
    "one Merged per source on the install's evidence: the own debt, then every inherited level"
  );
  assert!(m.group(&4).unwrap().capture_debt().is_none());
  for named in [1u64, 2, 3] {
    assert!(
      !m.debt_names(&named),
      "the naming of {named} dies with the discharge"
    );
    assert!(
      stores.0.contains_key(&named),
      "the proto layer never touches {named}'s stores"
    );
  }
  assert!(
    m.service_merge_applies(d4, &mut stores).is_empty(),
    "nothing surfaces twice"
  );
}

/// A host whose debt holder carries a THREE-level inherited chain: the fixture's debtor 1 (owing
/// 2's capture) is consumed by 3, 3 by 4 and 4 by 5, every absorb fence-deferred, so 5 owns the
/// debt for 4 and carries 3's, 1's and 2's — four consumed, unhosted sources whose floors stay
/// deliberately unwritten until the discharge. Returns the container, its stores and each leader's
/// instant, with the chain standing (5's abort fence holds it).
fn three_level_debt_chain_host() -> (
  MultiRaft<u64, u64, SplitSm>,
  MapStores,
  std::collections::BTreeMap<u64, Instant>,
) {
  let (mut m, mut stores, _k, _split_idx, d, _ds) = fork_fenced_park_fixture();
  defer_to_absorbed(&mut m, &mut stores, d);
  let mut instants = std::collections::BTreeMap::from([(1u64, d)]);
  for (holder, target, dead_source) in [(1u64, 3u64, 8u64), (3, 4, 9), (4, 5, 10)] {
    defer_holder_into_fenced_target(
      &mut m,
      &mut stores,
      &mut instants,
      holder,
      target,
      dead_source,
    );
  }
  let tep = m.group(&5).unwrap();
  assert_eq!(
    tep.capture_debt().expect("the own debt").source(),
    gid_key(4),
    "the own debt names the consumed holder"
  );
  for named in [1u64, 2, 3, 4] {
    assert!(m.debt_names(&named), "the chain names {named}");
  }
  assert!(
    m.service_merge_applies(instants[&5], &mut stores)
      .is_empty(),
    "the chain waits: the abort fence still stands"
  );
  (m, stores, instants)
}

/// Craft `source` hosted and FROZEN for `target` — a restored durable log holding the
/// `PrepareMerge` at index 1 — without hosting `target`: the strand's shape at the endpoint seam.
fn restore_frozen_split_source(
  m: &mut MultiRaft<u64, u64, SplitSm>,
  stores: &mut MapStores,
  source: u64,
  target: u64,
) {
  let mut slog = VecLog::default();
  slog.force_append(&[crate::Entry::new(
    Term::new(1),
    Index::new(1),
    crate::EntryKind::PrepareMerge,
    prepare_merge_bytes(target, 1),
  )]);
  let mut sstable = AsyncStable::default();
  sstable.force_state(Term::new(1), Some(1u64), Index::new(1));
  m.restore_group_unchecked(
    source,
    single_node_cfg(1),
    Instant::ORIGIN,
    7,
    SplitSm::default(),
    1,
    &mut slog,
    &mut sstable,
  )
  .unwrap();
  assert!(
    m.group(&source).unwrap().is_frozen(),
    "the crafted freeze applied"
  );
  stores.0.insert(source, (slog, sstable));
}

/// The strand's target-side naming check reaches the holder's INHERITED chain, not only its own
/// debt: with 5 owning the debt for 4 and carrying 3's, 1's and 2's, a source frozen for the
/// deepest inherited level (2 — consumed, unhosted, its floor deliberately unwritten until the
/// discharge) is no strand for as long as the chain stands. The host's only observation is the
/// chain's own fence hold.
#[test]
fn a_target_named_deep_in_an_inherited_chain_is_not_a_stranded_sources_target() {
  let (mut m, mut stores, instants) = three_level_debt_chain_host();
  let d = instants[&5];
  // S = 60, crafted frozen for T = 2, the chain's deepest inherited source.
  restore_frozen_split_source(&mut m, &mut stores, 60, 2);
  let mut observed = std::vec::Vec::new();
  for _ in 0..3 {
    assert!(m.service_merge_applies(d, &mut stores).is_empty());
    while let Some(b) = m.poll_merge_blocked() {
      observed.push(b);
    }
  }
  assert!(
    observed
      .iter()
      .all(|b| b.cause != MergeBlockedCause::StrandedSource),
    "an inherited debt's window is no strand: {observed:?}"
  );
  assert!(
    observed.iter().any(|b| b.target == 5),
    "the chain's own hold is what is reported: {observed:?}"
  );
  assert!(
    m.group(&60).unwrap().is_frozen(),
    "the source waits on the discharge, then the floor"
  );
}

/// The naming check is exact, not a blanket suppression while a chain stands: beside the same
/// three-level chain, a source frozen for an id the chain does NOT name (50, never hosted, never
/// consumed) IS a strand, reported beside the chain's own hold.
#[test]
fn a_target_the_chain_does_not_name_is_a_strand_beside_the_chain() {
  let (mut m, mut stores, instants) = three_level_debt_chain_host();
  let d = instants[&5];
  // S = 60, crafted frozen for T = 50, which nothing on this host names.
  restore_frozen_split_source(&mut m, &mut stores, 60, 50);
  let mut observed = std::vec::Vec::new();
  for _ in 0..3 {
    assert!(m.service_merge_applies(d, &mut stores).is_empty());
    while let Some(b) = m.poll_merge_blocked() {
      observed.push(b);
    }
  }
  assert_eq!(
    observed
      .iter()
      .filter(|b| b.cause == MergeBlockedCause::StrandedSource)
      .cloned()
      .collect::<std::vec::Vec<_>>(),
    std::vec![MergeBlocked {
      target: 50,
      source: 60,
      boundary: Index::new(1),
      cause: MergeBlockedCause::StrandedSource,
    }],
    "an unnamed dead target is a strand, once: {observed:?}"
  );
  assert!(
    observed.iter().any(|b| b.target == 5),
    "reported beside the chain's own hold: {observed:?}"
  );
}

/// The strand derivation at scale: the same three-level chain (5 the one naming holder), TWO
/// crafted strands — 60 frozen for 2, which the holder's inherited chain names, and 61 frozen for
/// 50, which nothing names — beside 24 idle single-voter groups that hold no park and no naming.
/// Exactly one `StrandedSource` stands, for the unnamed target; none for the named one, whose
/// naming the walk over the holders alone must still reach through the inherited chain; and none
/// keyed on any of the idle groups, which the once-per-crank pass visits without ever admitting
/// them into the holder list.
#[test]
fn many_idle_groups_beside_two_strands_yield_exactly_the_unnamed_strand() {
  let (mut m, mut stores, instants) = three_level_debt_chain_host();
  let d = instants[&5];
  let idle: std::vec::Vec<u64> = (100u64..124).collect();
  for gid in &idle {
    m.create_group(*gid, 0, single_node_cfg(1), d, 9, SplitSm::default())
      .unwrap();
  }
  restore_frozen_split_source(&mut m, &mut stores, 60, 2);
  restore_frozen_split_source(&mut m, &mut stores, 61, 50);
  let mut observed = std::vec::Vec::new();
  for _ in 0..3 {
    assert!(m.service_merge_applies(d, &mut stores).is_empty());
    while let Some(b) = m.poll_merge_blocked() {
      observed.push(b);
    }
  }
  assert_eq!(
    observed
      .iter()
      .filter(|b| b.cause == MergeBlockedCause::StrandedSource)
      .cloned()
      .collect::<std::vec::Vec<_>>(),
    std::vec![MergeBlocked {
      target: 50,
      source: 61,
      boundary: Index::new(1),
      cause: MergeBlockedCause::StrandedSource,
    }],
    "one strand, the unnamed target's, once: {observed:?}"
  );
  assert!(
    observed
      .iter()
      .all(|b| !idle.contains(&b.target) && !idle.contains(&b.source)),
    "an idle group is neither a strand's target nor its source: {observed:?}"
  );
  assert!(
    observed.iter().any(|b| b.target == 5),
    "reported beside the chain's own hold: {observed:?}"
  );
  for source in [60u64, 61] {
    assert!(
      m.group(&source).unwrap().is_frozen(),
      "{source} stays frozen: the named one waits on the discharge, the strand on its remedy"
    );
  }
}

/// A poisoning completion — the state machine refuses the blob, `SnapshotRestore` — discharges
/// NOTHING: the poison returns before the chain is reached, the pass skips a poisoned debtor, so
/// the debt and its naming stand exactly as before the install: no `Merged`, the consumed source
/// still spoken for, its stores intact.
#[test]
fn a_poisoning_install_completion_leaves_the_debt_chain_standing() {
  let (mut m, mut stores, _k, _split_idx, d, _ds) = fork_fenced_park_fixture_with_target(SplitSm {
    refuse_restore: true,
    ..SplitSm::default()
  });
  defer_to_absorbed(&mut m, &mut stores, d);
  while m.poll_event().is_some() {}
  let units_before = m.group(&1).unwrap().state_machine().units;

  covering_install_completes(&mut m, &mut stores, 1, d);
  let tep = m.group(&1).unwrap();
  assert!(
    tep.is_poisoned(),
    "the refused restore fail-stops the completion"
  );
  assert_eq!(tep.poison_reason(), Some(PoisonReason::SnapshotRestore));
  assert_eq!(
    tep.state_machine().units,
    units_before,
    "nothing was restored"
  );
  assert!(tep.capture_debt().is_some(), "the chain stands");
  for _ in 0..3 {
    assert!(
      m.service_merge_applies(d, &mut stores).is_empty(),
      "a poisoned debtor discharges nothing"
    );
  }
  assert!(m.debt_names(&2), "the naming stands with the chain");
  assert!(
    matches!(
      m.remove_group(&2, &mut empty_stores()),
      Err(RemoveError::SpokenFor)
    ),
    "the consumed source is still spoken for: no tombstone, no store teardown"
  );
  while let Some((_gid, ev)) = m.poll_event() {
    assert!(
      !matches!(ev, Event::Merged(_)),
      "no union event without a discharge"
    );
  }
  assert!(stores.0.contains_key(&2));
}

/// THE CRASH WINDOW (#134, recorded, not fixed). The blob is durable before the destructive body
/// runs, so a crash after its fsync and before the debt pass's discharge restarts the target from
/// the blob with the chain gone: the consumed source's stores stand redundant under a
/// non-terminal floor, un-named and re-admittable beside the union. The DESIRED behaviour is
/// pinned here — the naming survives the restart, the source stays un-admittable, and the
/// discharge still surfaces exactly once — and the durable-engine program is what delivers it.
#[test]
#[ignore = "the install-then-crash window drops the debt chain before its discharge (#134): the restarted target re-derives no debt, so this fails when run until the durable-engine program closes the window"]
fn a_crash_between_the_install_and_the_discharge_keeps_the_sources_naming() {
  let (mut m, mut stores, _k, _split_idx, d, _ds) = fork_fenced_park_fixture();
  defer_to_absorbed(&mut m, &mut stores, d);
  covering_install_completes(&mut m, &mut stores, 1, d);
  assert!(
    m.group(&1).unwrap().capture_debt().is_some(),
    "the completion ran with the chain intact and not yet discharged"
  );

  // Crash: the container dies with the volatile chain; the durable blob and the preserved
  // source stores survive.
  drop(m);
  let now = Instant::ORIGIN;
  let mut m2: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  {
    let (plog, pstable) = stores.0.get_mut(&1).unwrap();
    m2.restore_group_unchecked(
      1,
      single_node_cfg(1),
      now,
      42,
      SplitSm::default(),
      2,
      plog,
      pstable,
    )
    .unwrap();
  }
  assert_eq!(
    m2.group(&1).unwrap().state_machine().units,
    9,
    "the restart recovers the union from the durable blob"
  );
  assert!(
    m2.debt_names(&2),
    "the consumed source stays spoken for across the crash"
  );
  {
    let (l, s) = stores.0.get_mut(&2).unwrap();
    assert!(
      matches!(
        m2.restore_group_unchecked(2, single_node_cfg(1), now, 44, SplitSm::default(), 2, l, s),
        Err(CreateGroupError::AbsorbPending)
      ),
      "the redundant source is not re-admittable beside the union"
    );
  }
  assert_eq!(
    m2.service_merge_applies(now, &mut stores),
    std::vec![MergeResolution::Merged {
      source: 2,
      target: 1
    }],
    "the discharge surfaces exactly once after the restart"
  );
  assert!(m2.service_merge_applies(now, &mut stores).is_empty());
}

/// Deliver a covering destructive install at `boundary` to the deposed single-voter host `gid`
/// (a follower after [`step_down`]) and drain its completion. The blob's term is stamped one
/// past the host's, and the boundary entry's term differs from the meta's, so the completion
/// classifies NOT redundant and runs the destructive body: the tail above `boundary` is gone.
fn destructive_install_completes(
  m: &mut MultiRaft<u64, u64, SplitSm>,
  gid: u64,
  d: Instant,
  log: &mut VecLog,
  stable: &mut AsyncStable,
  boundary: Index,
) {
  let term = m.group(&gid).unwrap().term();
  let meta = crate::SnapshotMeta::new(
    boundary,
    Term::new(2),
    crate::conf::ConfState::from_voters(std::vec![1u64]),
  );
  m.handle_message(
    &gid,
    d,
    log,
    stable,
    9u64,
    Message::InstallSnapshot(crate::InstallSnapshot::new(
      Term::new(term.get() + 1),
      9u64,
      meta,
      fork_blob(9),
    )),
  )
  .unwrap();
  drain_storage(m, gid, d, log, stable);
  assert_eq!(
    log.last_index(),
    boundary,
    "the restore discarded the deposed leader's tail"
  );
  let ep = m.group(&gid).unwrap();
  assert_eq!(ep.applied_index(), boundary);
  assert_eq!(ep.state_machine().units, 9, "the blob IS the new baseline");
  assert!(
    ep.role().is_follower(),
    "still a follower: no election re-seated anything"
  );
}

/// A FORMER LEADER's stale commit fence must not refuse the install's own evidence. The debt
/// holder led, appended a `CommitMerge` above what it committed, and was deposed with that tail
/// unapplied; a covering install then discards the tail. Only a follower installs, and a
/// follower has no proposals of its own in flight, so the seat above the boundary names an
/// entry the restore discarded — the value a restart zeroes. Left standing it holds
/// `merge_conf_fence` for good, and with the popped fork's barrier still fencing a fresh capture
/// (the driver's pop→flush→lift window) the install's durable evidence is the debt's only
/// same-crank discharge: the re-seat is what lets it land.
#[test]
fn a_former_leaders_stale_commit_fence_does_not_refuse_the_installs_evidence() {
  let (mut m, mut stores, _k, split_idx, d, _ds) = fork_fenced_park_fixture();
  defer_to_absorbed(&mut m, &mut stores, d);
  // Pop the fork (the squatter leaves) but do NOT lift its barrier: a fresh capture at the
  // boundary stays fenced, so the discharge can only ride the install's evidence.
  m.remove_group(&200, &mut empty_stores()).unwrap();
  assert_eq!(install_head_fork(&mut m, 1, 200, d), split_idx);
  assert!(
    m.group(&1).unwrap().fork_barrier_standing(),
    "the popped fork keeps its barrier until the flush report"
  );
  while m.poll_event().is_some() {}

  // The leader's uncommitted tail: an ordinary entry, then a CommitMerge above it, neither
  // drained (the propose gate refuses a debt holder, so the entry goes through the raw seam).
  let boundary = {
    let (l, s) = stores.0.get_mut(&1).unwrap();
    let normal = m
      .propose(&1, d, l, s, &Bytes::from_static(b"c"))
      .unwrap()
      .unwrap();
    let commit_idx = m
      .group_mut(&1)
      .unwrap()
      .propose_merge_entry(
        d,
        l,
        crate::EntryKind::CommitMerge,
        commit_merge_bytes(7, Index::new(1), 1, 1),
      )
      .unwrap();
    assert_eq!(commit_idx, normal.next());
    step_down(&mut m, 1, l, s);
    normal
  };
  let tep = m.group(&1).unwrap();
  assert!(tep.commit_index() < boundary, "the tail never committed");
  assert!(
    tep.commit_merge_in_flight(),
    "the deposed leader's seat stands over its tail"
  );

  {
    let (l, s) = stores.0.get_mut(&1).unwrap();
    destructive_install_completes(&mut m, 1, d, l, s, boundary);
  }
  let tep = m.group(&1).unwrap();
  assert!(
    tep.capture_debt().is_some(),
    "the chain survives the re-baseline"
  );
  assert!(
    tep.fork_barrier_standing(),
    "the popped fork's barrier stands across the install: a fresh capture is still fenced"
  );
  let resolutions = m.service_merge_applies(d, &mut stores);
  assert_eq!(
    resolutions,
    std::vec![MergeResolution::Merged {
      source: 2,
      target: 1
    }],
    "the debt discharges in the install's own crank, on the install's evidence"
  );
  let tep = m.group(&1).unwrap();
  assert!(
    !tep.commit_merge_in_flight(),
    "the seat over the discarded tail is re-seated to the restart's value"
  );
  assert!(
    tep.pending_compact_boundary().is_none(),
    "no fresh capture was staged: the discharge rode the install"
  );
  assert!(tep.capture_debt().is_none());
  assert!(!m.debt_names(&2));
}

/// A FORMER LEADER's stale conf fence must not survive a destructive install: `prepare_merge`
/// reads the TARGET's `conf_change_in_flight` with no leader gate, so a follower target whose
/// deposed seat names a `ConfChange` the restore discarded would refuse every merge into it
/// until it next led — a wedge a plain restart from the same durable state never has.
#[test]
fn a_former_leaders_stale_conf_fence_does_not_survive_a_destructive_install() {
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let now = Instant::ORIGIN;
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  m.create_group(1, 0, single_node_cfg(1), now, 42, SplitSm::default())
    .unwrap();
  let d = lead_single_split(&mut m, 1, &mut log, &mut stable);
  for _ in 0..3 {
    commit_one_split(&mut m, 1, d, &mut log, &mut stable);
  }
  // The leader's uncommitted tail: an ordinary entry, then a ConfChange above it (through the
  // seam the propose path appends with), neither drained.
  let boundary = m
    .propose(&1, d, &mut log, &stable, &Bytes::from_static(b"c"))
    .unwrap()
    .unwrap();
  let cc = crate::ConfChange::new(crate::ConfChangeType::AddNode, 4u64, Bytes::new()).into_v2();
  let conf_idx = m
    .group_mut(&1)
    .unwrap()
    .append_conf_change(d.into(), &mut log, &stable, cc)
    .unwrap();
  assert_eq!(conf_idx, boundary.next());
  step_down(&mut m, 1, &mut log, &mut stable);
  let tep = m.group(&1).unwrap();
  assert!(tep.commit_index() < boundary, "the tail never committed");
  assert!(
    tep.conf_change_in_flight(),
    "the deposed leader's seat stands over its tail"
  );

  destructive_install_completes(&mut m, 1, d, &mut log, &mut stable, boundary);
  let mut stores = MapStores(
    std::collections::BTreeMap::new(),
    std::collections::BTreeSet::new(),
  );
  stores.0.insert(1, (log, stable));
  // A source proposing a merge INTO the follower target reads that gate, ungated by role.
  let (mut log3, mut stable3) = (VecLog::default(), AsyncStable::default());
  m.create_group(3, 0, single_node_cfg(1), now, 45, SplitSm::default())
    .unwrap();
  let d3 = lead_single_split(&mut m, 3, &mut log3, &mut stable3);
  stores.0.insert(3, (log3, stable3));
  match m.prepare_merge(&3, d3, &mut stores, &1) {
    Some(Ok(_)) => {}
    other => panic!("a merge into the follower target must admit after the install, got {other:?}"),
  }
  assert!(
    !m.group(&1).unwrap().conf_change_in_flight(),
    "the seat over the discarded tail is re-seated to the restart's value"
  );
}

/// A FORMER LEADER's stale split reservation must not survive a destructive install:
/// `split_reserves` feeds the public, role-independent `MultiRaft::split_reserved`, the
/// coordinators' admission methods and the drivers' factory gates, so a follower whose deposed
/// seat names a `Split` the restore discarded would reserve the child id on this host until it
/// next led — a reservation a plain restart from the same durable state never has.
#[test]
fn a_former_leaders_stale_split_reservation_does_not_survive_a_destructive_install() {
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let now = Instant::ORIGIN;
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  m.create_group(1, 0, single_node_cfg(1), now, 42, SplitSm::default())
    .unwrap();
  let d = lead_single_split(&mut m, 1, &mut log, &mut stable);
  for _ in 0..3 {
    commit_one_split(&mut m, 1, d, &mut log, &mut stable);
  }
  // The leader's uncommitted tail: an ordinary entry, then a Split above it, neither drained.
  let boundary = m
    .propose(&1, d, &mut log, &stable, &Bytes::from_static(b"c"))
    .unwrap()
    .unwrap();
  let split_idx = m
    .propose_split(
      &1,
      d,
      &mut log,
      &stable,
      &200,
      0,
      Bytes::from_static(b"\x02"),
    )
    .unwrap()
    .unwrap();
  assert_eq!(split_idx, boundary.next());
  assert!(
    m.split_reserved(&200),
    "the propose window reserves the child id"
  );
  step_down(&mut m, 1, &mut log, &mut stable);
  assert!(
    m.group(&1).unwrap().commit_index() < boundary,
    "the tail never committed"
  );
  assert!(
    m.split_reserved(&200),
    "the deposed proposer's seat still reserves it"
  );

  destructive_install_completes(&mut m, 1, d, &mut log, &mut stable, boundary);
  assert!(
    !m.split_reserved(&200),
    "the reservation dies with the discarded tail: the seat is re-seated to the restart's value"
  );
  let tep = m.group(&1).unwrap();
  assert!(!tep.split_in_flight());
  assert!(
    !tep.fork_obligations_standing(),
    "no fork was staged: the split never applied"
  );
}

/// A parked fork's standing capture fence no longer wedges a later merge into the same parent:
/// the absorb resolves as `Absorbed` — the union applies and serves, the source endpoint is
/// consumed with its stores preserved, and the forced capture becomes a debt the per-crank
/// service discharges into `Merged` once the fence lifts.
#[test]
fn a_parked_fork_defers_the_absorb_capture_as_a_debt() {
  let (mut m, mut stores, k, split_idx, d, _ds) = fork_fenced_park_fixture();
  let boundary = defer_to_absorbed(&mut m, &mut stores, d);
  assert_eq!(boundary, k);

  assert!(!m.contains_group(&2), "the source endpoint was consumed");
  let tep = m.group(&1).unwrap();
  assert!(tep.pending_merge().is_none(), "unparked: the drain resumed");
  assert!(tep.applied_index() >= k, "the union applied");
  assert_eq!(tep.state_machine().units, 2, "one half kept + one absorbed");
  {
    let (l, _s) = stores.0.get_mut(&1).unwrap();
    assert!(
      l.first_index() <= split_idx,
      "the split entry stays replayable under the debt"
    );
  }
  // The fence still stands: further cranks hold the debt, never a premature `Merged`.
  assert!(
    m.service_merge_applies(d, &mut stores).is_empty(),
    "the debt waits for the fence"
  );

  // The fence lifts: the squatter leaves, the fork yields, and the driver's flush report
  // releases the barrier — the very next crank stages the capture and surfaces `Merged`.
  m.remove_group(&200, &mut empty_stores()).unwrap();
  let split = install_head_fork(&mut m, 1, 200, d);
  m.lift_fork_barrier(&1, split);
  let resolutions = m.service_merge_applies(d, &mut stores);
  assert_eq!(
    resolutions,
    std::vec![MergeResolution::Merged {
      source: 2,
      target: 1
    }],
    "the lifted fence discharges the debt"
  );
  assert!(m.group(&1).unwrap().capture_debt().is_none());
  let mut saw_merged = false;
  while let Some((_gid, ev)) = m.poll_event() {
    if matches!(ev, crate::Event::Merged(_)) {
      saw_merged = true;
    }
  }
  assert!(
    saw_merged,
    "the union event rides the discharge, not the defer"
  );
  {
    let (l, s) = stores.0.get_mut(&1).unwrap();
    while matches!(
      m.handle_storage(&1, d, l, s),
      Some(StorageProgress::MorePending)
    ) {}
    assert!(
      l.first_index() > k,
      "the discharged capture compacted through the absorb"
    );
  }
}

/// A crash inside the debt window loses nothing: the target's log still holds the `CommitMerge`
/// (compaction past it required exactly the capture still owed) and the source's stores were
/// preserved, so a restart re-parks, re-folds deterministically, and re-forms the debt — and the
/// fence's later lift discharges it exactly as in the uncrashed run.
#[test]
fn a_crash_inside_the_debt_window_re_parks_and_converges() {
  let (mut m, mut stores, k, _split_idx, d, _ds) = fork_fenced_park_fixture();
  defer_to_absorbed(&mut m, &mut stores, d);
  assert_eq!(m.group(&1).unwrap().state_machine().units, 2);

  // Crash: the container dies with the volatile fold and debt; the stores survive.
  drop(m);
  let now = Instant::ORIGIN;
  let mut m2: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  {
    let (plog, pstable) = stores.0.get_mut(&1).unwrap();
    m2.restore_group_unchecked(
      1,
      single_node_cfg(1),
      now,
      42,
      SplitSm::default(),
      2,
      plog,
      pstable,
    )
    .unwrap();
  }
  {
    let (slog, sstable) = stores.0.get_mut(&2).unwrap();
    m2.restore_group_unchecked(
      2,
      single_node_cfg(1),
      now,
      44,
      SplitSm::default(),
      2,
      slog,
      sstable,
    )
    .unwrap();
  }
  assert!(
    m2.group(&1).unwrap().pending_merge().is_some(),
    "the replayed CommitMerge re-parked"
  );
  assert!(
    m2.group(&2).unwrap().is_frozen(),
    "the restored source re-derived its freeze"
  );

  // The replayed split re-staged the fork (the squatter is gone in this incarnation, so it
  // yields rather than parks), and its barrier re-derived with it — the re-resolve defers again,
  // deterministically re-folding the same union.
  let resolutions = m2.service_merge_applies(now, &mut stores);
  assert_eq!(
    resolutions,
    std::vec![MergeResolution::Absorbed {
      source: 2,
      target: 1
    }],
    "the restart re-derives the debt, not a wedge and not a premature teardown"
  );
  assert_eq!(
    m2.group(&1).unwrap().state_machine().units,
    2,
    "the re-fold is deterministic"
  );

  // The fork yields in this incarnation; the flush report lifts the barrier and the debt
  // discharges — the crashed and uncrashed runs converge on the same end state.
  let split = install_head_fork(&mut m2, 1, 200, now);
  m2.lift_fork_barrier(&1, split);
  let resolutions = m2.service_merge_applies(now, &mut stores);
  assert_eq!(
    resolutions,
    std::vec![MergeResolution::Merged {
      source: 2,
      target: 1
    }]
  );
  assert!(!m2.contains_group(&2));
  assert_eq!(m2.group(&1).unwrap().state_machine().units, 2);
  let _ = k;
}

/// The one-absorb-at-a-time posture holds across the debt window: a second reshape verb into
/// (or out of) a debt-holding target refuses until the debt discharges.
#[test]
fn a_debt_holding_target_refuses_further_reshape_verbs() {
  let (mut m, mut stores, _k, _split_idx, d, _ds) = fork_fenced_park_fixture();
  defer_to_absorbed(&mut m, &mut stores, d);
  // Release the fork barrier first: it refuses a freeze on its own (`SplitInFlight`), and the
  // probe below must reach the DEBT's refusal.
  m.remove_group(&200, &mut empty_stores()).unwrap();
  let split = install_head_fork(&mut m, 1, 200, d);
  m.lift_fork_barrier(&1, split);

  // Named counterparts for the refused verbs: 3 as a would-be source into 1, and 0 as a
  // would-be target of 1 (merges run higher-id into lower-id, so the source-side probe needs
  // the debt-holder on the high side).
  m.create_group(
    3,
    0,
    single_node_cfg(1),
    Instant::ORIGIN,
    45,
    SplitSm::default(),
  )
  .unwrap();
  m.create_group(
    0,
    0,
    single_node_cfg(1),
    Instant::ORIGIN,
    46,
    SplitSm::default(),
  )
  .unwrap();
  {
    let (l, s) = stores.0.get_mut(&1).unwrap();
    assert!(
      matches!(
        m.commit_merge(&1, d, l, s, &3),
        Some(Err(MergeError::AlreadyPending))
      ),
      "a debt-holding target absorbs nothing further"
    );
  }
  assert!(
    matches!(
      m.prepare_merge(&1, d, &mut stores, &0),
      Some(Err(MergeError::AlreadyPending))
    ),
    "a debt-holding group refuses to freeze as a source"
  );
}

/// The staged-capture discharge: an ordinary threshold capture staged after the fence lifts is
/// adopted as the debt's own discharge — same-crank `Merged` while the transient is still
/// staged, where the forced-capture leg alone would have waited out the transient.
#[test]
fn an_ordinary_staged_capture_discharges_the_debt() {
  let (mut m, mut stores, k, _split_idx, d, _ds) = fork_fenced_park_fixture();
  defer_to_absorbed(&mut m, &mut stores, d);

  m.remove_group(&200, &mut empty_stores()).unwrap();
  let split = install_head_fork(&mut m, 1, 200, d);
  m.lift_fork_barrier(&1, split);

  // One storage crank after the lift: the threshold capture stages (threshold 1, applied >= k)
  // and its completion is still pending when the service runs.
  {
    let (l, s) = stores.0.get_mut(&1).unwrap();
    let _ = m.handle_storage(&1, d, l, s);
  }
  assert!(
    m.group(&1)
      .unwrap()
      .pending_compact_boundary()
      .is_some_and(|b| b >= k),
    "an ordinary capture covering the boundary is staged"
  );
  let resolutions = m.service_merge_applies(d, &mut stores);
  assert_eq!(
    resolutions,
    std::vec![MergeResolution::Merged {
      source: 2,
      target: 1
    }],
    "the staged capture is adopted as the discharge in the same crank"
  );
  assert!(m.group(&1).unwrap().capture_debt().is_none());
}

/// The debt window's naming holds at every container lifecycle surface: the consumed source can
/// be neither tombstoned nor re-admitted (create OR restore — either would revive a husk beside
/// the absorbed union), and the debt-holding target cannot be torn down (its discharge is the
/// source's only exit to the terminal floor). Every refusal releases at the discharge.
#[test]
fn a_debt_names_its_source_at_every_lifecycle_surface() {
  let (mut m, mut stores, _k, _split_idx, d, _ds) = fork_fenced_park_fixture();
  defer_to_absorbed(&mut m, &mut stores, d);
  assert!(m.debt_names(&2));

  assert!(
    matches!(
      m.remove_group(&2, &mut empty_stores()),
      Err(RemoveError::SpokenFor)
    ),
    "tombstoning the consumed source strands the union's only restart derivation"
  );
  assert!(
    matches!(
      m.remove_group(&1, &mut empty_stores()),
      Err(RemoveError::OwesCapture)
    ),
    "tearing down the debt holder strands the source's stores forever"
  );
  assert!(
    matches!(
      m.create_group(
        2,
        1,
        single_node_cfg(1),
        Instant::ORIGIN,
        9,
        SplitSm::default()
      ),
      Err(CreateGroupError::AbsorbPending)
    ),
    "a fresh incarnation at the debt-named id revives a husk beside the union"
  );
  {
    let (l, s) = stores.0.get_mut(&2).unwrap();
    assert!(
      matches!(
        m.restore_group_unchecked(
          2,
          single_node_cfg(1),
          Instant::ORIGIN,
          9,
          SplitSm::default(),
          3,
          l,
          s
        ),
        Err(CreateGroupError::AbsorbPending)
      ),
      "a restore over the preserved stores re-hosts the husk mid-window"
    );
  }

  // The discharge releases every surface.
  m.remove_group(&200, &mut empty_stores()).unwrap();
  let split = install_head_fork(&mut m, 1, 200, d);
  m.lift_fork_barrier(&1, split);
  assert_eq!(
    m.service_merge_applies(d, &mut stores),
    std::vec![MergeResolution::Merged {
      source: 2,
      target: 1
    }]
  );
  assert!(!m.debt_names(&2), "the naming dies with the discharge");
}

/// Whether `stable` holds a durable snapshot at-or-past `boundary`.
fn sailing_proto_durable_covers(stable: &AsyncStable, boundary: Index) -> bool {
  crate::StableStore::durable_snapshot(stable).is_some_and(|m| m.last_index() >= boundary)
}

/// A raw `CommitMerge` entry payload naming a typed source id — the force-append form the
/// restore fixtures use (the propose path derives the same bytes from live state).
fn commit_merge_bytes(
  source: u64,
  freeze_index: Index,
  source_gen_after: u64,
  target_gen_after: u64,
) -> Bytes {
  commit_merge_bytes_at(
    source,
    freeze_index,
    Term::new(1),
    source_gen_after,
    target_gen_after,
  )
}

/// A raw `PrepareMerge` entry payload naming a typed target id at `source_gen_after` — the
/// force-append form a restore fixture uses to restart a source FROZEN.
fn prepare_merge_bytes(target: u64, source_gen_after: u64) -> Bytes {
  let p = crate::PrepareMergePayload::new(gid_key(target), source_gen_after);
  let mut buf = Vec::new();
  crate::wire::encode_prepare_merge_payload(&p, &mut buf);
  Bytes::from(buf)
}

/// [`commit_merge_bytes`] with the freeze's TERM settable — for a source whose freeze was appended
/// under a later term than its first (an install deposed it before the freeze).
fn commit_merge_bytes_at(
  source: u64,
  freeze_index: Index,
  freeze_term: Term,
  source_gen_after: u64,
  target_gen_after: u64,
) -> Bytes {
  let mut sb = Vec::new();
  source.encode(&mut sb);
  let p = crate::CommitMergePayload::new(
    Bytes::from(sb),
    freeze_index,
    freeze_term,
    source_gen_after,
    target_gen_after,
  );
  let mut buf = Vec::new();
  crate::wire::encode_commit_merge_payload(&p, &mut buf);
  Bytes::from(buf)
}

/// The under-hosted park's cure, end to end at the container: the resolver's unresolvable arm
/// sets the advertisement hint (no local fold can ever land — the source is unhosted with a
/// non-terminal floor), a covering blob then ADOPTS in place of the fold — state to the
/// boundary, park cleared, the LOG kept — and the completion ack gates on the persisted commit
/// exactly like every no-blob exit. The hosted twin of the same shape keeps today's behavior
/// bit-for-bit: no hint, no adopt, the park held for the local resolution.
#[test]
fn an_under_hosted_park_advertises_and_adopts_the_cure() {
  let now = Instant::ORIGIN;
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let cmd = {
    let mut buf = Vec::new();
    Bytes::from_static(b"c").encode(&mut buf);
    Bytes::from(buf)
  };
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();
  log.force_append(&[
    crate::Entry::new(
      Term::new(1),
      Index::new(1),
      crate::EntryKind::Normal,
      cmd.clone(),
    ),
    crate::Entry::new(
      Term::new(1),
      Index::new(2),
      crate::EntryKind::CommitMerge,
      commit_merge_bytes(42, Index::new(5), 1, 1),
    ),
    // The k+1 coordinate: committed content above the park closes the abort window, so the
    // resolver reaches the source lookup instead of waiting on an unsealed window.
    crate::Entry::new(Term::new(1), Index::new(3), crate::EntryKind::Normal, cmd),
  ]);
  stable.force_state(Term::new(1), Some(1u64), Index::new(3));
  m.restore_group_unchecked(
    1,
    single_node_cfg(1),
    now,
    7,
    SplitSm::default(),
    1,
    &mut log,
    &mut stable,
  )
  .unwrap();
  assert!(m.group(&1).unwrap().pending_merge().is_some(), "parked");
  let mut stores = MapStores(std::collections::BTreeMap::new(), Default::default());
  stores.0.insert(1, (log, stable));

  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  assert_eq!(
    m.group(&1).unwrap().merge_park_unresolvable(),
    Some(Index::new(2)),
    "the unresolvable arm advertises its boundary"
  );

  // The cure blob covers the park; the receipt-time redundancy arm adopts it.
  let meta = crate::SnapshotMeta::new(
    Index::new(3),
    Term::new(1),
    crate::conf::ConfState::from_voters(std::vec![1u64]),
  )
  .with_shape_gen(1);
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.handle_message(
      &1,
      now,
      log,
      stable,
      9u64,
      Message::InstallSnapshot(crate::InstallSnapshot::new(
        Term::new(1),
        9u64,
        meta,
        fork_blob(5),
      )),
    )
    .unwrap();
  }
  let tep = m.group(&1).unwrap();
  assert!(tep.pending_merge().is_none(), "the adopt cleared the park");
  assert_eq!(tep.merge_park_unresolvable(), None);
  assert_eq!(
    tep.applied_index(),
    Index::new(3),
    "state moved to the boundary"
  );
  assert_eq!(tep.state_machine().units, 5, "the blob IS the union");
  // Nothing acked before the commit persist lands; the gated ack releases on the crank.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    while matches!(
      m.handle_storage(&1, now, log, stable),
      Some(StorageProgress::MorePending)
    ) {}
  }
  let mut acked = false;
  while let Some((_gid, out)) = m.poll_message() {
    if matches!(out.message(), Message::SnapshotResponse(r) if r.match_index() == Index::new(3) && !r.reject())
    {
      acked = true;
    }
  }
  assert!(acked, "the completion ack rides the three-leg gate");

  // The adopt owes one forced, threshold-independent capture: the service stages it, and its
  // compaction releases the absorb membership fence — so an idle adopter that later leads has a
  // durable blob covering the boundary and can cure the next parked voter.
  assert!(m.group(&1).unwrap().adopt_capture_owed());
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    while matches!(
      m.handle_storage(&1, now, log, stable),
      Some(StorageProgress::MorePending)
    ) {}
    assert!(
      sailing_proto_durable_covers(stable, Index::new(3)),
      "the owed capture is durable at the boundary"
    );
    assert!(
      log.first_index() > Index::new(2),
      "its compaction released the absorb membership fence"
    );
  }
  assert!(!m.group(&1).unwrap().adopt_capture_owed());

  // The hosted twin: same log shape, but the source IS hosted — the resolvable arm keeps
  // today's behavior exactly (no hint, no adopt, the park held for the local resolution).
  let mut m2: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let cmd2 = {
    let mut buf = Vec::new();
    Bytes::from_static(b"c").encode(&mut buf);
    Bytes::from(buf)
  };
  let mut log2 = VecLog::default();
  let mut stable2 = AsyncStable::default();
  log2.force_append(&[
    crate::Entry::new(
      Term::new(1),
      Index::new(1),
      crate::EntryKind::Normal,
      cmd2.clone(),
    ),
    crate::Entry::new(
      Term::new(1),
      Index::new(2),
      crate::EntryKind::CommitMerge,
      commit_merge_bytes(42, Index::new(5), 1, 1),
    ),
    crate::Entry::new(Term::new(1), Index::new(3), crate::EntryKind::Normal, cmd2),
  ]);
  stable2.force_state(Term::new(1), Some(1u64), Index::new(3));
  m2.restore_group_unchecked(
    1,
    single_node_cfg(1),
    now,
    7,
    SplitSm::default(),
    1,
    &mut log2,
    &mut stable2,
  )
  .unwrap();
  m2.create_group(42, 0, single_node_cfg(1), now, 8, SplitSm::default())
    .unwrap();
  let mut stores2 = MapStores(std::collections::BTreeMap::new(), Default::default());
  stores2.0.insert(1, (log2, stable2));
  let _ = m2.service_merge_applies(now, &mut stores2);
  assert_eq!(
    m2.group(&1).unwrap().merge_park_unresolvable(),
    None,
    "a hosted source is resolvable — no advertisement"
  );
  let meta2 = crate::SnapshotMeta::new(
    Index::new(3),
    Term::new(1),
    crate::conf::ConfState::from_voters(std::vec![1u64]),
  )
  .with_shape_gen(1);
  {
    let (log2, stable2) = stores2.0.get_mut(&1).unwrap();
    m2.handle_message(
      &1,
      now,
      log2,
      stable2,
      9u64,
      Message::InstallSnapshot(crate::InstallSnapshot::new(
        Term::new(1),
        9u64,
        meta2,
        fork_blob(5),
      )),
    )
    .unwrap();
  }
  assert!(
    m2.group(&1).unwrap().pending_merge().is_some(),
    "no hint, no adopt: the covered blob is redundancy, exactly as before the cure"
  );
  assert_eq!(m2.group(&1).unwrap().applied_index(), Index::new(1));

  // The crash story, both regimes. AFTER the owed capture lands, a restart is adopt-equivalent
  // — the durable blob restores past the park, no re-cure needed. (BEFORE it lands — the
  // one-crank window — nothing durable covers the boundary: the restart re-parks off the
  // surviving CommitMerge and the advertisement loop re-cures; nothing was over-acked, since
  // the completion response certified exactly the persisted commit the restart recovers. That
  // regime is the crash-window residual the design records.)
  drop(m);
  let mut m3: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m3.restore_group_unchecked(
      1,
      single_node_cfg(1),
      now,
      7,
      SplitSm::default(),
      2,
      log,
      stable,
    )
    .unwrap();
  }
  let restored = m3.group(&1).unwrap();
  assert!(
    restored.pending_merge().is_none(),
    "post-capture, the restart restores past the park off the durable blob"
  );
  assert_eq!(
    restored.state_machine().units,
    5,
    "adopt-equivalent: the same union, no re-cure"
  );
}

/// A restored target 1 parked at index 2 on a `CommitMerge` naming UNHOSTED source 42 — the cure
/// shape, before any crank.
fn parked_target_over_an_unhosted_source() -> (MultiRaft<u64, u64, SplitSm>, MapStores) {
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let mut stores = MapStores(std::collections::BTreeMap::new(), Default::default());
  park_target_over_an_unhosted_source(&mut m, &mut stores, 1);
  (m, stores)
}

/// [`parked_target_over_an_unhosted_source`] for a chosen `target` id, restored into a container
/// beside whatever it already hosts — the shape's building block when a test needs several such
/// targets in one host.
fn park_target_over_an_unhosted_source(
  m: &mut MultiRaft<u64, u64, SplitSm>,
  stores: &mut MapStores,
  target: u64,
) {
  let now = Instant::ORIGIN;
  let cmd = {
    let mut buf = Vec::new();
    Bytes::from_static(b"c").encode(&mut buf);
    Bytes::from(buf)
  };
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();
  log.force_append(&[
    crate::Entry::new(
      Term::new(1),
      Index::new(1),
      crate::EntryKind::Normal,
      cmd.clone(),
    ),
    crate::Entry::new(
      Term::new(1),
      Index::new(2),
      crate::EntryKind::CommitMerge,
      commit_merge_bytes(42, Index::new(5), 1, 1),
    ),
    crate::Entry::new(Term::new(1), Index::new(3), crate::EntryKind::Normal, cmd),
  ]);
  stable.force_state(Term::new(1), Some(1u64), Index::new(3));
  m.restore_group_unchecked(
    target,
    single_node_cfg(1),
    now,
    7,
    SplitSm::default(),
    1,
    &mut log,
    &mut stable,
  )
  .unwrap();
  assert!(
    m.group(&target).unwrap().pending_merge().is_some(),
    "parked"
  );
  stores.0.insert(target, (log, stable));
}

/// The cure blob covering `boundary` at lineage `shape_gen`, as the target leader (node 9) ships
/// it.
fn cure_blob_at_gen(boundary: Index, shape_gen: u64) -> Message<u64> {
  let meta = crate::SnapshotMeta::new(
    boundary,
    Term::new(1),
    crate::conf::ConfState::from_voters(std::vec![1u64]),
  )
  .with_shape_gen(shape_gen);
  Message::InstallSnapshot(crate::InstallSnapshot::new(
    Term::new(1),
    9u64,
    meta,
    fork_blob(5),
  ))
}

/// The cure blob covering `boundary`, as the target leader (node 9) ships it.
fn cure_blob_to(boundary: Index) -> Message<u64> {
  let meta = crate::SnapshotMeta::new(
    boundary,
    Term::new(1),
    crate::conf::ConfState::from_voters(std::vec![1u64]),
  )
  .with_shape_gen(1);
  Message::InstallSnapshot(crate::InstallSnapshot::new(
    Term::new(1),
    9u64,
    meta,
    fork_blob(5),
  ))
}

/// THE ADOPTION RETAINS AND MARKS (#132): the parked-union adoption is the second transfer that
/// crosses abort entries — state moves to the blob's boundary while the LOG is kept — and it marks
/// the obligations it covers adopt-covered, removing none. Unlike an install-cover, an adopt-cover
/// KEEPS FENCING: the kept log still carries the entry, the record's only restart re-derivation.
/// The covered record is a dead end here (its source is unhosted) and absence is no proof, so it
/// stands across cranks, and the adopt's owed capture is DEFERRED behind its fence — not dropped —
/// until a witness clears the record; the next crank then stages the capture.
#[test]
fn the_adopt_marks_a_covered_obligation_and_its_owed_capture_waits_for_the_disposal() {
  let (mut m, mut stores) = parked_target_over_an_unhosted_source();
  let now = Instant::ORIGIN;
  // An EARLIER abort's obligation, below the park, naming a source no one hosts.
  let source_key = gid_key(7);
  m.group_mut(&1)
    .unwrap()
    .note_abandoned(source_key.clone(), 1, Index::new(1));
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  assert_eq!(
    m.group(&1).unwrap().merge_park_unresolvable(),
    Some(Index::new(2)),
    "an unhosted counterparty withholds nothing — the park advertises"
  );
  assert!(
    m.group(&1)
      .unwrap()
      .abandoned_record(&source_key)
      .is_some_and(|r| r.cover == Cover::None),
    "a parked holder disposes of nothing over an unproven dead end — the record stands, uncovered"
  );
  // The cure blob adopts.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.handle_message(&1, now, log, stable, 9u64, cure_blob_to(Index::new(3)))
      .unwrap();
  }
  let tep = m.group(&1).unwrap();
  assert!(tep.pending_merge().is_none(), "the adopt cleared the park");
  assert_eq!(tep.applied_index(), Index::new(3), "state at the boundary");
  assert!(tep.owes_live_thaw(), "RETAINED across the adopt");
  assert_eq!(
    tep
      .abandoned_record(&source_key)
      .map(|r| (r.cover, r.discharged)),
    Some((Cover::Adopt, false)),
    "and MARKED adopt-covered: the boundary (3) crossed the abort entry (1), the log kept"
  );
  assert!(
    tep.adopt_capture_owed(),
    "the adopt owes its forced capture"
  );
  assert!(
    tep.capture_blocked_at(tep.applied_index()),
    "which the adopt-covered record fences — its entry is in the kept log"
  );
  // The gated ack drains; cranks pass; the record stands (absence is no proof) and the owed
  // capture waits behind it, deferred, not dropped.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  for _ in 0..3 {
    assert!(m.service_merge_applies(now, &mut stores).is_empty());
  }
  assert!(
    m.group(&1).unwrap().adopt_capture_owed() && m.group(&1).unwrap().owes_live_thaw(),
    "the owed capture waits, deferred, while the covered dead end stands"
  );
  assert!(
    m.group(&1)
      .unwrap()
      .abandoned_record(&source_key)
      .is_some_and(|r| !r.discharged),
    "nothing retired it — a cover is no proof"
  );
  // A witness ARRIVES by replication from the leader that cured this park, above the boundary;
  // its committed apply clears the record and lifts the fence.
  {
    let mut buf = Vec::new();
    crate::wire::encode_thaw_discharged_payload(
      &ThawDischargedPayload::new(source_key.clone(), 1),
      &mut buf,
    );
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.handle_message(
      &1,
      now,
      log,
      stable,
      9u64,
      Message::AppendEntries(crate::AppendEntries::new(
        Term::new(1),
        9u64,
        Index::new(3),
        Term::new(1),
        std::vec![crate::Entry::new(
          Term::new(1),
          Index::new(4),
          crate::EntryKind::ThawDischarged,
          Bytes::from(buf),
        )],
        Index::new(4),
      )),
    )
    .unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    m.group(&1).unwrap().abandoned_obligations().is_empty(),
    "the committed witness apply cleared the covered dead end"
  );
  // The next crank finds the fence lifted and stages the owed capture.
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  assert!(
    !m.group(&1).unwrap().adopt_capture_owed(),
    "the owed capture staged the moment the fence lifted"
  );
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
    assert!(
      sailing_proto_durable_covers(stable, Index::new(4)),
      "the owed capture is durable at-or-past the boundary"
    );
    assert!(
      log.first_index() > Index::new(2),
      "its compaction released the absorb membership fence"
    );
  }
}

/// THE FENCED ADOPT DEBT IS NAMED (#132): an adopt-covered record whose source no one hosts fences
/// the owed capture, and the container reports that wait exactly as it reports a fenced capture
/// debt — one `MergeBlocked { AbortFence }` naming the FENCING source at the adopt's boundary,
/// visible across every crank it stands, deduped against the per-crank repeats, and gone the
/// crank the witness applies, the fence lifts, and the capture stages.
#[test]
fn a_fenced_adopt_capture_is_reported_as_an_abort_fence_until_the_witness_lifts_it() {
  let (mut m, mut stores) = parked_target_over_an_unhosted_source();
  let now = Instant::ORIGIN;
  // An EARLIER abort's obligation, below the park, naming a source no one hosts.
  let source_key = gid_key(7);
  m.group_mut(&1)
    .unwrap()
    .note_abandoned(source_key.clone(), 1, Index::new(1));
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  assert_eq!(
    m.poll_merge_blocked().map(|b| (b.source, b.cause)),
    Some((42, MergeBlockedCause::SourceUnhosted)),
    "the park's own hold is the resolver's signal, not the debt's"
  );
  // The cure adopts; the record is adopt-covered and fences the owed capture.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.handle_message(&1, now, log, stable, 9u64, cure_blob_to(Index::new(3)))
      .unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  let tep = m.group(&1).unwrap();
  assert!(
    tep.pending_merge().is_none() && tep.adopt_capture_owed(),
    "adopted, the forced capture owed"
  );
  assert!(
    tep.capture_blocked_at(tep.applied_index()),
    "and fenced by the adopt-covered record"
  );
  let observation = crate::MergeBlocked {
    target: 1,
    source: 7,
    boundary: Index::new(3),
    cause: MergeBlockedCause::AbortFence,
  };
  // The first crank names the wait; it stays visible, un-consumed, across the cranks it stands.
  for crank in 0..4 {
    assert!(m.service_merge_applies(now, &mut stores).is_empty());
    assert_eq!(
      m.peek_merge_blocked().as_ref(),
      Some(&observation),
      "crank {crank}: the fenced adopt debt is reported by its fencing source at its boundary"
    );
  }
  assert_eq!(m.poll_merge_blocked(), Some(observation.clone()));
  assert_eq!(
    m.poll_merge_blocked(),
    None,
    "one signal for the standing wait — the per-crank repeats deduped"
  );
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  assert_eq!(
    m.peek_merge_blocked(),
    None,
    "a consumed signal is not re-armed while its cause stands unchanged"
  );
  assert!(
    m.group(&1).unwrap().adopt_capture_owed(),
    "the owed capture still waits"
  );
  // The witness ARRIVES by replication from the leader that cured this park; its committed apply
  // clears the record and lifts the fence.
  {
    let mut buf = Vec::new();
    crate::wire::encode_thaw_discharged_payload(
      &ThawDischargedPayload::new(source_key.clone(), 1),
      &mut buf,
    );
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.handle_message(
      &1,
      now,
      log,
      stable,
      9u64,
      Message::AppendEntries(crate::AppendEntries::new(
        Term::new(1),
        9u64,
        Index::new(3),
        Term::new(1),
        std::vec![crate::Entry::new(
          Term::new(1),
          Index::new(4),
          crate::EntryKind::ThawDischarged,
          Bytes::from(buf),
        )],
        Index::new(4),
      )),
    )
    .unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(
    m.group(&1).unwrap().abandoned_obligations().is_empty(),
    "the committed witness apply cleared the covered dead end"
  );
  // The next crank stages the owed capture and reports nothing — then and after.
  for crank in 0..2 {
    assert!(m.service_merge_applies(now, &mut stores).is_empty());
    assert!(
      !m.group(&1).unwrap().adopt_capture_owed(),
      "crank {crank}: the owed capture staged the moment the fence lifted"
    );
    assert_eq!(
      m.peek_merge_blocked(),
      None,
      "crank {crank}: nothing stands, nothing is reported"
    );
  }
}

/// ONE OBSERVATION PER TARGET PER CRANK, held across MANY fenced adopt owners at once (#135): the
/// adopt-capture pass withholds its own fence signal exactly for a target an earlier pass of the
/// same crank already named — a re-formed park here, the more actionable identity — and emits it
/// for every other owner, however many the crank services; and the crank's attempt evidence, the
/// target set the suppression reads included, is gone once the crank retires it, so the next crank
/// derives every hold afresh.
#[test]
fn many_fenced_adopt_owners_keep_one_observation_per_target_per_crank() {
  let now = Instant::ORIGIN;
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let mut stores = MapStores(std::collections::BTreeMap::new(), Default::default());
  let targets = [1u64, 2, 3, 4, 5];
  // Every target: parked over unhosted 42, holding an EARLIER abort's obligation below the park
  // that names a source no one hosts (70 + the target's id, distinct per target).
  for &t in &targets {
    park_target_over_an_unhosted_source(&mut m, &mut stores, t);
    m.group_mut(&t)
      .unwrap()
      .note_abandoned(gid_key(70 + t), 1, Index::new(1));
  }
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  while m.poll_merge_blocked().is_some() {}
  // The cure adopts each; every record is adopt-covered and fences its owner's owed capture.
  for &t in &targets {
    let (log, stable) = stores.0.get_mut(&t).unwrap();
    m.handle_message(&t, now, log, stable, 9u64, cure_blob_to(Index::new(3)))
      .unwrap();
    drain_storage(&mut m, t, now, log, stable);
    let tep = m.group(&t).unwrap();
    assert!(
      tep.pending_merge().is_none()
        && tep.adopt_capture_owed()
        && tep.capture_blocked_at(tep.applied_index()),
      "target {t}: adopted, the forced capture owed and fenced"
    );
  }
  // A foreign-led second absorb, of the unhosted 43, RE-PARKS the middle owner above the adopt
  // boundary — with its `k + 1` committed, so the abort window is decided and the park pass
  // names that wait first in every crank from here on.
  {
    let cmd = {
      let mut buf = Vec::new();
      Bytes::from_static(b"c").encode(&mut buf);
      Bytes::from(buf)
    };
    let (log, stable) = stores.0.get_mut(&3).unwrap();
    m.handle_message(
      &3,
      now,
      log,
      stable,
      9u64,
      Message::AppendEntries(crate::AppendEntries::new(
        Term::new(1),
        9u64,
        Index::new(3),
        Term::new(1),
        std::vec![
          crate::Entry::new(
            Term::new(1),
            Index::new(4),
            crate::EntryKind::CommitMerge,
            commit_merge_bytes(43, Index::new(5), 1, 2),
          ),
          crate::Entry::new(Term::new(1), Index::new(5), crate::EntryKind::Normal, cmd),
        ],
        Index::new(5),
      )),
    )
    .unwrap();
    drain_storage(&mut m, 3, now, log, stable);
  }
  let tep = m.group(&3).unwrap();
  assert_eq!(
    tep.pending_merge().map(|p| p.at()),
    Some(Index::new(4)),
    "re-parked above the adopt"
  );
  assert!(
    tep.adopt_capture_owed() && tep.capture_blocked_at(tep.applied_index()),
    "still owing its fenced capture"
  );
  // ONE crank services all five owners.
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  let mut observed = std::vec::Vec::new();
  while let Some(b) = m.poll_merge_blocked() {
    observed.push(b);
  }
  observed.sort_by_key(|b| (b.target, b.source));
  let fenced = |t: u64| MergeBlocked {
    target: t,
    source: 70 + t,
    boundary: Index::new(3),
    cause: MergeBlockedCause::AbortFence,
  };
  assert_eq!(
    observed,
    std::vec![
      fenced(1),
      fenced(2),
      MergeBlocked {
        target: 3,
        source: 43,
        boundary: Index::new(4),
        cause: MergeBlockedCause::SourceUnhosted,
      },
      fenced(4),
      fenced(5),
    ],
    "each unnamed owner reports its own fence; the re-parked one reports its park alone"
  );
  assert!(
    m.merge_blocked_attempts.is_empty() && m.merge_blocked_targets.is_empty(),
    "the crank's attempt evidence retired with the crank"
  );
  // The next crank starts clean and re-derives the same five holds — deduped against the edge,
  // nothing new is queued, and the evidence retires again.
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  assert_eq!(
    m.poll_merge_blocked(),
    None,
    "the standing holds, unchanged, signal once"
  );
  assert!(m.merge_blocked_targets.is_empty());
}

/// THE LIVE CO-HOSTED SHAPE NEVER ADOPTS (#132): a parked target whose obligation names a source
/// hosted-and-frozen RIGHT HERE is doubly protected — the resolver withholds the cure
/// advertisement, and the receipt-time gate drops a cure blob that rides a stale hint anyway — so
/// no transfer ever stands between that source and its drive: the park and the uncovered record
/// stand, the source untouched.
#[test]
fn a_cure_blob_never_adopts_over_a_hosted_frozen_counterparty() {
  let (mut m, mut stores) = parked_target_over_an_unhosted_source();
  let now = Instant::ORIGIN;
  // Source 2 restarts FROZEN at generation 1 with its claim on 1, which owes it the thaw (an
  // earlier abort at index 1, below the park).
  {
    let mut slog = VecLog::default();
    let mut sstable = AsyncStable::default();
    slog.force_append(&[crate::Entry::new(
      Term::new(1),
      Index::new(1),
      crate::EntryKind::PrepareMerge,
      prepare_merge_bytes(1, 1),
    )]);
    sstable.force_state(Term::new(1), Some(1u64), Index::new(1));
    m.restore_group_unchecked(
      2,
      single_node_cfg(1),
      now,
      8,
      SplitSm::default(),
      1,
      &mut slog,
      &mut sstable,
    )
    .unwrap();
    stores.0.insert(2, (slog, sstable));
  }
  assert!(m.group(&2).unwrap().is_frozen(), "2 restarted frozen");
  let source_key = gid_key(2);
  m.group_mut(&1)
    .unwrap()
    .note_abandoned(source_key.clone(), 1, Index::new(1));
  // The resolver withholds the advertisement: the counterparty is hosted and unadvanced.
  let _ = m.service_merge_applies(now, &mut stores);
  assert_eq!(
    m.group(&1).unwrap().merge_park_unresolvable(),
    None,
    "no cure is advertised over a hosted frozen counterparty"
  );
  // The stale-hint window: a hint derived before the counterparty was observed here, the blob
  // already in flight. The receipt gate keys on the PARK and drops the whole message.
  m.group_mut(&1).unwrap().note_merge_park_unresolvable(true);
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.handle_message(&1, now, log, stable, 9u64, cure_blob_to(Index::new(3)))
      .unwrap();
  }
  let tep = m.group(&1).unwrap();
  assert!(
    tep.pending_merge().is_some(),
    "the park stands — nothing adopted"
  );
  assert_eq!(tep.applied_index(), Index::new(1), "state untouched");
  assert_eq!(
    tep.merge_park_unresolvable(),
    None,
    "the gate consumed the stale hint"
  );
  assert!(
    tep
      .abandoned_record(&source_key)
      .is_some_and(|r| r.cover == Cover::None && !r.discharged),
    "the record stands, uncovered and live"
  );
  assert!(
    m.group(&2).unwrap().is_frozen(),
    "the source is untouched, its drive intact"
  );
}

/// A restored target 1 parked at index 2 whose log carries a TARGET-role abort for unhosted source 7
/// BELOW the park (index 1, so the record is live at the park) and, ABOVE it inside the interval a
/// cure would adopt across, the committed `ThawDischarged` witness for that very record — or, with
/// `witness` supplied, an arbitrary payload in its place. Not yet cranked.
fn parked_target_with_a_witness_above_the_park(
  witness: Bytes,
) -> (MultiRaft<u64, u64, CountSm>, MapStores) {
  let now = Instant::ORIGIN;
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let abort = {
    let p = crate::RollbackMergePayload::abort(gid_key(7), 1, 1);
    let mut buf = Vec::new();
    crate::wire::encode_rollback_merge_payload(&p, &mut buf);
    Bytes::from(buf)
  };
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();
  log.force_append(&[
    crate::Entry::new(
      Term::new(1),
      Index::new(1),
      crate::EntryKind::RollbackMerge,
      abort,
    ),
    // The parked commit mints against the abort's bump: target generation 2 after 1.
    crate::Entry::new(
      Term::new(1),
      Index::new(2),
      crate::EntryKind::CommitMerge,
      commit_merge_bytes(42, Index::new(5), 1, 2),
    ),
    // Committed content above the park closes the abort window; here it is the witness itself.
    crate::Entry::new(
      Term::new(1),
      Index::new(3),
      crate::EntryKind::ThawDischarged,
      witness,
    ),
  ]);
  stable.force_state(Term::new(1), Some(1u64), Index::new(3));
  m.restore_group_unchecked(
    1,
    single_node_cfg(1),
    now,
    7,
    CountSm::default(),
    1,
    &mut log,
    &mut stable,
  )
  .unwrap();
  let t = m.group(&1).unwrap();
  assert!(t.pending_merge().is_some(), "parked");
  assert!(
    t.abandoned_record(&gid_key(7))
      .is_some_and(|r| !r.discharged && r.cover == Cover::None),
    "the abort below the park re-derived a live record"
  );
  let mut stores = MapStores(std::collections::BTreeMap::new(), Default::default());
  stores.0.insert(1, (log, stable));
  (m, stores)
}

/// THE ADOPTED INTERVAL'S WITNESS IS APPLIED (#137): target 1 applied an abort for unhosted source 7
/// below its park, and the committed `ThawDischarged` that retires it sits inside the interval the
/// cure blob adopts across. The crossing walk PLANS that witness (it names a held record), and the
/// adoption folds the plan with the gen-exact clear, so the record is CLEARED, nothing fences the
/// owed adopt capture, and the capture stages and compacts — the absorb membership fence releases.
/// Skipping the witness would leave the record adopt-covered forever: the global witness already
/// exists, in the kept log, so no new one is ever minted, and the mandatory capture stays fenced.
#[test]
fn a_witness_inside_the_adopted_interval_clears_the_record() {
  let now = Instant::ORIGIN;
  let witness = {
    let mut buf = Vec::new();
    crate::wire::encode_thaw_discharged_payload(
      &ThawDischargedPayload::new(gid_key(7), 1),
      &mut buf,
    );
    Bytes::from(buf)
  };
  let (mut m, mut stores) = parked_target_with_a_witness_above_the_park(witness);
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  let tep = m.group(&1).unwrap();
  assert_eq!(
    tep.merge_park_unresolvable(),
    Some(Index::new(2)),
    "the park advertises"
  );
  assert_eq!(
    tep.planned_witnesses().keys().cloned().collect::<Vec<_>>(),
    std::vec![(gid_key(7), 1u64)],
    "the walk planned the one witness that names a held record"
  );
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.handle_message(&1, now, log, stable, 9u64, cure_blob_to(Index::new(3)))
      .unwrap();
  }
  let tep = m.group(&1).unwrap();
  assert!(
    tep.pending_merge().is_none() && tep.applied_index() == Index::new(3),
    "adopted"
  );
  assert!(
    tep.abandoned_obligations().is_empty() && tep.planned_witnesses().is_empty(),
    "the planned witness cleared the record at the adopt, and the plan went with the park"
  );
  assert!(
    tep.adopt_capture_owed() && !tep.capture_blocked_at(tep.applied_index()),
    "the owed capture is unfenced"
  );
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  assert!(
    !m.group(&1).unwrap().adopt_capture_owed(),
    "the owed capture staged"
  );
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
    assert!(
      sailing_proto_durable_covers(stable, Index::new(3)),
      "the owed capture is durable at the boundary"
    );
    assert!(
      log.first_index() > Index::new(2),
      "its compaction released the absorb membership fence"
    );
  }
}

/// A committed witness above the park that will not decode is committed-corrupt: the crossing
/// WALK fail-stops `MergeDecode` the crank it reads it — before any cure is advertised, so no adopt
/// is ever admitted across it — exactly as it does for a malformed crossing, and as the apply arm
/// would have.
#[test]
fn a_malformed_witness_inside_the_adopted_interval_poisons() {
  let now = Instant::ORIGIN;
  let (mut m, mut stores) =
    parked_target_with_a_witness_above_the_park(Bytes::from_static(b"\xff\xff\xff"));
  let _ = m.service_merge_applies(now, &mut stores);
  let tep = m.group(&1).unwrap();
  assert!(
    tep.is_poisoned(),
    "a committed-corrupt witness above the park fail-stops the crossing walk"
  );
  assert_eq!(
    tep.merge_park_unresolvable(),
    None,
    "and no cure was advertised off the partial walk"
  );
  assert!(
    tep.pending_merge().is_some() && tep.applied_index() == Index::new(1),
    "nothing moved: the park stands"
  );
}

/// A log seam that COUNTS every `entries` read (its requested range), can answer exactly one
/// designated read with a cold page (`EntriesRead::Pending`), and can model a ONE-PAGE cache —
/// the crossing walk's per-read bound, its watermark resumption, and the adopt's read-free
/// consumption of what the walk carried, observed.
struct CountingLog {
  inner: VecLog,
  reads: core::cell::RefCell<Vec<(Index, Index)>>,
  cold_on_read: core::cell::Cell<Option<usize>>,
  /// The one-page cache policy: a multi-entry read is served only when it asks for the page
  /// loaded last; any other page misses (`Pending`), LOADS — so its retry is served — and evicts
  /// the page before it, so a re-read of an earlier page is always cold. Single-entry reads (the
  /// resolver's abort-window probe) bypass it: the seam models the interval's pages, not the probe.
  one_page: bool,
  cached_page: core::cell::Cell<Option<Index>>,
  served: core::cell::RefCell<Vec<(Index, Index)>>,
}

impl CountingLog {
  fn new(inner: VecLog) -> Self {
    Self {
      inner,
      reads: core::cell::RefCell::new(Vec::new()),
      cold_on_read: core::cell::Cell::new(None),
      one_page: false,
      cached_page: core::cell::Cell::new(None),
      served: core::cell::RefCell::new(Vec::new()),
    }
  }

  fn cold_on_read(self, nth: Option<usize>) -> Self {
    self.cold_on_read.set(nth);
    self
  }

  fn one_page_cache(mut self) -> Self {
    self.one_page = true;
    self
  }

  fn read_ranges(&self) -> Vec<(Index, Index)> {
    self.reads.borrow().clone()
  }

  /// The multi-entry reads the one-page cache SERVED (its misses are in `read_ranges` only).
  fn served_pages(&self) -> Vec<(Index, Index)> {
    self.served.borrow().clone()
  }
}

impl crate::LogStore for CountingLog {
  type Error = core::convert::Infallible;

  fn first_index(&self) -> Index {
    self.inner.first_index()
  }

  fn last_index(&self) -> Index {
    self.inner.last_index()
  }

  fn term(&self, index: Index) -> Result<Term, Self::Error> {
    self.inner.term(index)
  }

  fn entries(
    &self,
    range: core::ops::Range<Index>,
    max_bytes: u64,
  ) -> Result<crate::EntriesRead<'_>, Self::Error> {
    let nth = self.reads.borrow().len();
    self.reads.borrow_mut().push((range.start, range.end));
    if self.cold_on_read.get() == Some(nth) {
      return Ok(crate::EntriesRead::Pending);
    }
    if self.one_page && range.end.get() - range.start.get() > 1 {
      if self.cached_page.get() != Some(range.start) {
        self.cached_page.set(Some(range.start));
        return Ok(crate::EntriesRead::Pending);
      }
      self.served.borrow_mut().push((range.start, range.end));
    }
    self.inner.entries(range, max_bytes)
  }

  fn submit_append(&mut self, id: crate::OpId, entries: &[crate::Entry]) {
    self.inner.submit_append(id, entries)
  }

  fn compact(&mut self, up_to: Index) {
    self.inner.compact(up_to)
  }

  fn restore(&mut self, last_index: Index, last_term: Term) {
    self.inner.restore(last_index, last_term)
  }

  fn poll(&mut self) -> Option<Result<crate::LogDone, Self::Error>> {
    self.inner.poll()
  }

  fn has_pending(&self) -> bool {
    self.inner.has_pending()
  }
}

struct CountingStores(std::collections::BTreeMap<u64, (CountingLog, AsyncStable)>);

impl crate::GroupStores<u64, CountingLog, AsyncStable> for CountingStores {
  fn stores(&mut self, group: &u64) -> Option<(&mut CountingLog, &mut AsyncStable)> {
    self.0.get_mut(group).map(|(l, s)| (l, s))
  }
}

impl crate::FloorStore<u64> for CountingStores {
  fn floor(&self, _gid: &u64) -> u64 {
    0
  }

  fn lineage(&self, _gid: &u64) -> u64 {
    0
  }
}

/// THE WITNESS PLAN IS BOUNDED AND INCREMENTAL (#137): target 1 is parked at index 2 over a
/// LONG tail of committed witnesses for sources it does not hold, with the one witness that names
/// its held record last. The crossing walk reads the tail in bounded chunks, plans exactly ONE
/// witness (the tail is skipped, not retained), resumes from its watermark after a cold page
/// rather than re-reading the interval, and the adoption — which walks nothing of its own for
/// witnesses — folds the plan and clears the record, so the owed capture stages and compacts.
#[test]
fn the_witness_plan_is_bounded_by_the_map_and_the_walk_resumes_from_its_watermark() {
  use crate::endpoint::MAX_READ_BATCH_ENTRIES;
  let now = Instant::ORIGIN;
  let tail: u64 = MAX_READ_BATCH_ENTRIES + 1_800;
  let last = Index::new(2 + tail + 1);
  let encode_witness = |source: u64, generation: u64| {
    let mut buf = Vec::new();
    crate::wire::encode_thaw_discharged_payload(
      &ThawDischargedPayload::new(gid_key(source), generation),
      &mut buf,
    );
    Bytes::from(buf)
  };
  let abort = {
    let p = crate::RollbackMergePayload::abort(gid_key(7), 1, 1);
    let mut buf = Vec::new();
    crate::wire::encode_rollback_merge_payload(&p, &mut buf);
    Bytes::from(buf)
  };
  let mut entries = std::vec![
    crate::Entry::new(
      Term::new(1),
      Index::new(1),
      crate::EntryKind::RollbackMerge,
      abort
    ),
    crate::Entry::new(
      Term::new(1),
      Index::new(2),
      crate::EntryKind::CommitMerge,
      commit_merge_bytes(42, Index::new(5), 1, 2),
    ),
  ];
  // The tail: witnesses for sources this target never held — nothing to plan.
  for i in 0..tail {
    entries.push(crate::Entry::new(
      Term::new(1),
      Index::new(3 + i),
      crate::EntryKind::ThawDischarged,
      encode_witness(1_000 + i, 1),
    ));
  }
  // The one that matters, last.
  entries.push(crate::Entry::new(
    Term::new(1),
    last,
    crate::EntryKind::ThawDischarged,
    encode_witness(7, 1),
  ));
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();
  log.force_append(&entries);
  stable.force_state(Term::new(1), Some(1u64), last);
  m.restore_group_unchecked(
    1,
    single_node_cfg(1),
    now,
    7,
    CountSm::default(),
    1,
    &mut log,
    &mut stable,
  )
  .unwrap();
  assert!(m.group(&1).unwrap().pending_merge().is_some(), "parked");
  let mut stores = CountingStores(std::collections::BTreeMap::new());
  stores.0.insert(
    1,
    (
      // The resolver reads the abort-window coordinate first (one entry), then the walk's
      // first bounded chunk lands; the walk's SECOND chunk answers cold.
      CountingLog::new(log).cold_on_read(Some(2)),
      stable,
    ),
  );

  // Crank 1: the resolver reads the abort-window coordinate (one entry), the walk's first
  // bounded chunk lands, and its second read answers cold — the walk stops at its watermark and
  // withholds the hint (fail-closed).
  let window = (Index::new(3), Index::new(4));
  let chunk_end = Index::new(3 + MAX_READ_BATCH_ENTRIES);
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  let reads = stores.0.get(&1).unwrap().0.read_ranges();
  assert_eq!(
    reads,
    std::vec![window, (Index::new(3), chunk_end), (chunk_end, last.next())],
    "the window read, one bounded chunk, then the cold page"
  );
  assert!(
    reads
      .iter()
      .all(|(s, e)| e.get() - s.get() <= MAX_READ_BATCH_ENTRIES),
    "every read of the walk is bounded: {reads:?}"
  );
  assert_eq!(
    m.group(&1).unwrap().merge_park_unresolvable(),
    None,
    "no hint off a partial walk"
  );
  assert!(
    m.group(&1).unwrap().planned_witnesses().is_empty(),
    "the plan is empty so far: the first chunk held only witnesses for unheld sources"
  );

  // Crank 2: the walk RESUMES from its watermark — the retry reads only what the cold page hid.
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  let reads = stores.0.get(&1).unwrap().0.read_ranges();
  assert_eq!(
    &reads[3..],
    &[window, (chunk_end, last.next())],
    "the retry re-reads the window coordinate and resumes the walk exactly where the first \
     chunk ended — never at the park: {reads:?}"
  );
  let tep = m.group(&1).unwrap();
  assert_eq!(
    tep.merge_park_unresolvable(),
    Some(Index::new(2)),
    "the walk is current — the park advertises"
  );
  assert_eq!(
    tep.planned_witnesses().keys().cloned().collect::<Vec<_>>(),
    std::vec![(gid_key(7), 1u64)],
    "the plan holds exactly the one witness naming a held record — the tail was skipped"
  );

  // The adopt folds the plan; the record clears; the owed capture stages and compacts — and the
  // adopt itself reads NOTHING: the walk carried its absorb point along with the plan.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.handle_message(&1, now, log, stable, 9u64, cure_blob_to(last))
      .unwrap();
  }
  assert_eq!(
    stores.0.get(&1).unwrap().0.read_ranges().len(),
    reads.len(),
    "the adopt performed no log read of its own over the warm interval"
  );
  let tep = m.group(&1).unwrap();
  assert!(
    tep.pending_merge().is_none() && tep.applied_index() == last,
    "adopted"
  );
  assert!(
    tep.abandoned_obligations().is_empty() && tep.planned_witnesses().is_empty(),
    "the planned witness cleared the record; the plan went with the park"
  );
  assert!(
    !tep.capture_blocked_at(last),
    "nothing fences the owed capture"
  );
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    while matches!(
      m.handle_storage(&1, now, log, stable),
      Some(StorageProgress::MorePending)
    ) {}
  }
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  assert!(
    !m.group(&1).unwrap().adopt_capture_owed(),
    "the owed capture staged"
  );
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    while matches!(
      m.handle_storage(&1, now, log, stable),
      Some(StorageProgress::MorePending)
    ) {}
    assert!(
      log.first_index() > Index::new(2),
      "its compaction released the absorb membership fence"
    );
  }
}

/// THE WALK IS BUDGETED PER CRANK (#137): a warm committed tail longer than
/// `CROSSING_SCAN_CHUNKS_PER_CRANK` chunks is walked across cranks — exactly the budgeted reads per
/// crank, the watermark advancing after each chunk, the hint withheld until the walk reaches the
/// frontier — and only then is the adopt admitted. The budget bounds the crank, not the walk.
#[test]
fn the_crossing_walk_spends_its_chunk_budget_per_crank_and_resumes() {
  use crate::endpoint::{CROSSING_SCAN_CHUNKS_PER_CRANK, MAX_READ_BATCH_ENTRIES};
  let now = Instant::ORIGIN;
  let budget = CROSSING_SCAN_CHUNKS_PER_CRANK as u64;
  let tail: u64 = budget * MAX_READ_BATCH_ENTRIES + 1_800;
  let last = Index::new(2 + tail + 1);
  let encode_witness = |source: u64, generation: u64| {
    let mut buf = Vec::new();
    crate::wire::encode_thaw_discharged_payload(
      &ThawDischargedPayload::new(gid_key(source), generation),
      &mut buf,
    );
    Bytes::from(buf)
  };
  let abort = {
    let p = crate::RollbackMergePayload::abort(gid_key(7), 1, 1);
    let mut buf = Vec::new();
    crate::wire::encode_rollback_merge_payload(&p, &mut buf);
    Bytes::from(buf)
  };
  let mut entries = std::vec![
    crate::Entry::new(
      Term::new(1),
      Index::new(1),
      crate::EntryKind::RollbackMerge,
      abort
    ),
    crate::Entry::new(
      Term::new(1),
      Index::new(2),
      crate::EntryKind::CommitMerge,
      commit_merge_bytes(42, Index::new(5), 1, 2),
    ),
  ];
  for i in 0..tail {
    entries.push(crate::Entry::new(
      Term::new(1),
      Index::new(3 + i),
      crate::EntryKind::ThawDischarged,
      encode_witness(1_000 + i, 1),
    ));
  }
  entries.push(crate::Entry::new(
    Term::new(1),
    last,
    crate::EntryKind::ThawDischarged,
    encode_witness(7, 1),
  ));
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();
  log.force_append(&entries);
  stable.force_state(Term::new(1), Some(1u64), last);
  m.restore_group_unchecked(
    1,
    single_node_cfg(1),
    now,
    7,
    CountSm::default(),
    1,
    &mut log,
    &mut stable,
  )
  .unwrap();
  let mut stores = CountingStores(std::collections::BTreeMap::new());
  stores.0.insert(1, (CountingLog::new(log), stable));
  let window = (Index::new(3), Index::new(4));
  let chunk = |i: u64| {
    (
      Index::new(3 + i * MAX_READ_BATCH_ENTRIES),
      Index::new(3 + (i + 1) * MAX_READ_BATCH_ENTRIES),
    )
  };

  // Crank 1: the window read, then exactly the budgeted chunks — the frontier still ahead, the
  // hint withheld, nothing planned yet (every chunk so far named only unheld sources).
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  let reads = stores.0.get(&1).unwrap().0.read_ranges();
  let mut expected = std::vec![window];
  expected.extend((0..budget).map(chunk));
  assert_eq!(
    reads, expected,
    "crank 1 spent exactly its chunk budget and stopped short of the frontier"
  );
  assert_eq!(
    m.group(&1).unwrap().merge_park_unresolvable(),
    None,
    "no hint off a budget-bounded partial walk"
  );
  assert!(m.group(&1).unwrap().planned_witnesses().is_empty());

  // Crank 2: the window read, then the walk resumes from its watermark and reaches the frontier
  // in one remaining chunk — the hint and the plan follow.
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  let reads = stores.0.get(&1).unwrap().0.read_ranges();
  assert_eq!(
    &reads[expected.len()..],
    &[window, (chunk(budget - 1).1, last.next())],
    "crank 2 resumed at the watermark and read only the remainder: {reads:?}"
  );
  let tep = m.group(&1).unwrap();
  assert_eq!(
    tep.merge_park_unresolvable(),
    Some(Index::new(2)),
    "the walk reached the frontier — the park advertises"
  );
  assert_eq!(
    tep.planned_witnesses().keys().cloned().collect::<Vec<_>>(),
    std::vec![(gid_key(7), 1u64)],
    "the plan holds the one witness naming a held record"
  );
  // The adopt is admitted only now, and folds the plan.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.handle_message(&1, now, log, stable, 9u64, cure_blob_to(last))
      .unwrap();
  }
  let tep = m.group(&1).unwrap();
  assert!(
    tep.pending_merge().is_none() && tep.abandoned_obligations().is_empty(),
    "adopted past the walked tail, the record cleared off the plan"
  );
}

/// THE ADOPT WALKS NOTHING (#137): the resumable, budgeted crossing walk carries the absorb point
/// an adoption engages its membership fence at, so the adopt performs no log read of its own.
/// Under a ONE-PAGE cache — a multi-entry read is served only for the page loaded last; any other
/// page misses, loads, and evicts — the walk crosses a three-page interval once (each page cold,
/// then served, never revisited), the cure is admitted behind it, and the adoption completes on
/// its first delivery with zero reads. An adopt-time re-walk from the park could never land: its
/// first page misses (the walk's last page evicted it), the retry's second page misses, and the
/// retry after that misses the first page again — the cure re-sent forever.
#[test]
fn the_adopt_reads_no_page_the_walk_already_crossed() {
  use crate::endpoint::MAX_READ_BATCH_ENTRIES;
  let now = Instant::ORIGIN;
  let tail: u64 = 2 * MAX_READ_BATCH_ENTRIES + 1_800;
  let last = Index::new(2 + tail + 1);
  let encode_witness = |source: u64, generation: u64| {
    let mut buf = Vec::new();
    crate::wire::encode_thaw_discharged_payload(
      &ThawDischargedPayload::new(gid_key(source), generation),
      &mut buf,
    );
    Bytes::from(buf)
  };
  let abort = {
    let p = crate::RollbackMergePayload::abort(gid_key(7), 1, 1);
    let mut buf = Vec::new();
    crate::wire::encode_rollback_merge_payload(&p, &mut buf);
    Bytes::from(buf)
  };
  let mut entries = std::vec![
    crate::Entry::new(
      Term::new(1),
      Index::new(1),
      crate::EntryKind::RollbackMerge,
      abort
    ),
    crate::Entry::new(
      Term::new(1),
      Index::new(2),
      crate::EntryKind::CommitMerge,
      commit_merge_bytes(42, Index::new(5), 1, 2),
    ),
  ];
  for i in 0..tail {
    entries.push(crate::Entry::new(
      Term::new(1),
      Index::new(3 + i),
      crate::EntryKind::ThawDischarged,
      encode_witness(1_000 + i, 1),
    ));
  }
  entries.push(crate::Entry::new(
    Term::new(1),
    last,
    crate::EntryKind::ThawDischarged,
    encode_witness(7, 1),
  ));
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();
  log.force_append(&entries);
  stable.force_state(Term::new(1), Some(1u64), last);
  m.restore_group_unchecked(
    1,
    single_node_cfg(1),
    now,
    7,
    CountSm::default(),
    1,
    &mut log,
    &mut stable,
  )
  .unwrap();
  let mut stores = CountingStores(std::collections::BTreeMap::new());
  stores
    .0
    .insert(1, (CountingLog::new(log).one_page_cache(), stable));
  let window = (Index::new(3), Index::new(4));
  let page = |i: u64| {
    (
      Index::new(3 + i * MAX_READ_BATCH_ENTRIES),
      last
        .next()
        .min(Index::new(3 + (i + 1) * MAX_READ_BATCH_ENTRIES)),
    )
  };

  // Four cranks: every crank probes the window (a single entry, outside the cache's model),
  // then the walk resumes at its watermark — the page it missed last crank is served, the next
  // one misses — until the third page is served and the walk reaches the frontier. No page is
  // ever requested again once served.
  let mut expected = Vec::new();
  for crank in 0..4u64 {
    assert!(m.service_merge_applies(now, &mut stores).is_empty());
    expected.push(window);
    if crank > 0 {
      expected.push(page(crank - 1));
    }
    if crank < 3 {
      expected.push(page(crank));
    }
    assert_eq!(
      stores.0.get(&1).unwrap().0.read_ranges(),
      expected,
      "crank {crank}: the walk resumed at its watermark and re-requested only the page it missed"
    );
    assert_eq!(
      m.group(&1).unwrap().merge_park_unresolvable(),
      (crank == 3).then_some(Index::new(2)),
      "crank {crank}: the hint waits for the frontier"
    );
  }
  assert_eq!(
    stores.0.get(&1).unwrap().0.served_pages(),
    std::vec![page(0), page(1), page(2)],
    "each page served exactly once, in order — the walk crossed the interval once"
  );
  assert_eq!(
    m.group(&1)
      .unwrap()
      .planned_witnesses()
      .keys()
      .cloned()
      .collect::<Vec<_>>(),
    std::vec![(gid_key(7), 1u64)],
    "the plan holds the one witness naming a held record"
  );

  // The cure's FIRST delivery adopts, and the adopt requests no page at all.
  let before = stores.0.get(&1).unwrap().0.read_ranges();
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.handle_message(&1, now, log, stable, 9u64, cure_blob_to(last))
      .unwrap();
  }
  assert_eq!(
    stores.0.get(&1).unwrap().0.read_ranges(),
    before,
    "the adopt performed no log read: the walk carried its absorb point"
  );
  let tep = m.group(&1).unwrap();
  assert!(
    tep.pending_merge().is_none() && tep.applied_index() == last,
    "adopted on the first delivery"
  );
  assert!(
    tep.abandoned_obligations().is_empty() && tep.planned_witnesses().is_empty(),
    "the planned witness cleared the record; the plan went with the park"
  );
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    while matches!(
      m.handle_storage(&1, now, log, stable),
      Some(StorageProgress::MorePending)
    ) {}
  }
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  assert!(
    !m.group(&1).unwrap().adopt_capture_owed(),
    "the owed capture staged"
  );
}

/// THE PLAN IS BOUNDARY-SCOPED (#137): the walk's frontier W sits above the boundary B a cure
/// ships; a witness for a held record lies at W' with B < W' <= W, and a belt-dependent
/// `CommitMerge` for that record's source at D with B < D < W'. The adoption folds nothing above
/// B: the record STANDS adopt-covered, the drain then applies D through the same-merge abort belt
/// as a no-op (`MergeAborted`, no park, the lineage untouched) and applies W' at its own index,
/// clearing the record. Folding W' at the adopt would have cleared the record early, parked D
/// over a source no one hosts, and trapped W' behind that park — committed apply stalled.
#[test]
fn the_adopt_folds_no_witness_above_its_boundary() {
  let now = Instant::ORIGIN;
  let cmd = {
    let mut buf = Vec::new();
    Bytes::from_static(b"c").encode(&mut buf);
    Bytes::from(buf)
  };
  let abort = {
    let p = crate::RollbackMergePayload::abort(gid_key(7), 1, 1);
    let mut buf = Vec::new();
    crate::wire::encode_rollback_merge_payload(&p, &mut buf);
    Bytes::from(buf)
  };
  let witness = {
    let mut buf = Vec::new();
    crate::wire::encode_thaw_discharged_payload(
      &ThawDischargedPayload::new(gid_key(7), 1),
      &mut buf,
    );
    Bytes::from(buf)
  };
  let (boundary, d, w, last) = (Index::new(6), Index::new(8), Index::new(10), Index::new(11));
  let mut entries = std::vec![
    crate::Entry::new(
      Term::new(1),
      Index::new(1),
      crate::EntryKind::RollbackMerge,
      abort
    ),
    crate::Entry::new(
      Term::new(1),
      Index::new(2),
      crate::EntryKind::CommitMerge,
      commit_merge_bytes(42, Index::new(5), 1, 2),
    ),
  ];
  for i in 3..=last.get() {
    let at = Index::new(i);
    let (kind, data) = if at == d {
      // A FRESH mint for the aborted freeze generation: only the belt keeps it from parking.
      (
        crate::EntryKind::CommitMerge,
        commit_merge_bytes(7, Index::new(9), 1, 3),
      )
    } else if at == w {
      (crate::EntryKind::ThawDischarged, witness.clone())
    } else {
      (crate::EntryKind::Normal, cmd.clone())
    };
    entries.push(crate::Entry::new(Term::new(1), at, kind, data));
  }
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();
  log.force_append(&entries);
  stable.force_state(Term::new(1), Some(1u64), last);
  m.restore_group_unchecked(
    1,
    single_node_cfg(1),
    now,
    7,
    SplitSm::default(),
    1,
    &mut log,
    &mut stable,
  )
  .unwrap();
  let mut stores = MapStores(std::collections::BTreeMap::new(), Default::default());
  stores.0.insert(1, (log, stable));
  assert!(
    m.group(&1).unwrap().pending_merge().is_some(),
    "parked at 2 over a source hosted nowhere"
  );

  // The walk reaches the frontier W: the crossing at D recorded, the witness at W' planned.
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  let tep = m.group(&1).unwrap();
  assert_eq!(
    tep.merge_park_unresolvable(),
    Some(Index::new(2)),
    "the walk reached the frontier — the park advertises"
  );
  assert_eq!(
    tep.planned_witnesses().get(&(gid_key(7), 1u64)),
    Some(&w),
    "the witness above the boundary is planned at its index"
  );

  // The cure covers only B < D < W': the adopt folds nothing above B — the record stands.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.handle_message(&1, now, log, stable, 9u64, cure_blob_at_gen(boundary, 2))
      .unwrap();
  }
  let tep = m.group(&1).unwrap();
  assert!(
    tep.pending_merge().is_none() && tep.applied_index() == boundary,
    "adopted to the boundary"
  );
  assert_eq!(
    tep
      .abandoned_record(&gid_key(7))
      .map(|r| (r.cover, r.discharged)),
    Some((Cover::Adopt, false)),
    "the record STANDS: the witness at W' > B was not folded"
  );
  assert!(
    tep.planned_witnesses().is_empty(),
    "the plan went with the park"
  );
  assert_eq!(tep.shape_gen(), 2, "the lineage folded from the blob");

  // The drain resumes above the boundary: D applies through the belt as a no-op — no park, the
  // lineage untouched — and W' clears the record at its own index.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    drain_storage(&mut m, 1, now, log, stable);
  }
  let mut aborted_at = None;
  while let Some((gid, ev)) = m.poll_event() {
    if gid == 1
      && let Event::MergeAborted(e) = ev
    {
      aborted_at = Some(e.index());
    }
  }
  assert_eq!(
    aborted_at,
    Some(d),
    "D no-oped through the same-merge abort belt"
  );
  let tep = m.group(&1).unwrap();
  assert!(tep.pending_merge().is_none(), "D did not park");
  assert_eq!(
    tep.applied_index(),
    last,
    "the drain ran through W' to the frontier"
  );
  assert!(
    tep.abandoned_obligations().is_empty(),
    "W' cleared the record at its own index"
  );
  assert_eq!(tep.shape_gen(), 2, "the belt bumps nothing");
}

/// THE PLAN IS BOUNDED BY THE HELD RECORDS (#137): two committed witnesses for the same
/// `(source, generation)` inside the interval yield ONE plan entry, at the lower index — the first
/// clears the record, the duplicate no-ops — so the plan grows with the records this target
/// holds, never with the witnesses the range repeats.
#[test]
fn duplicate_witnesses_plan_once_at_the_lower_index() {
  let now = Instant::ORIGIN;
  let abort = {
    let p = crate::RollbackMergePayload::abort(gid_key(7), 1, 1);
    let mut buf = Vec::new();
    crate::wire::encode_rollback_merge_payload(&p, &mut buf);
    Bytes::from(buf)
  };
  let witness = {
    let mut buf = Vec::new();
    crate::wire::encode_thaw_discharged_payload(
      &ThawDischargedPayload::new(gid_key(7), 1),
      &mut buf,
    );
    Bytes::from(buf)
  };
  let last = Index::new(4);
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();
  log.force_append(&[
    crate::Entry::new(
      Term::new(1),
      Index::new(1),
      crate::EntryKind::RollbackMerge,
      abort,
    ),
    crate::Entry::new(
      Term::new(1),
      Index::new(2),
      crate::EntryKind::CommitMerge,
      commit_merge_bytes(42, Index::new(5), 1, 2),
    ),
    crate::Entry::new(
      Term::new(1),
      Index::new(3),
      crate::EntryKind::ThawDischarged,
      witness.clone(),
    ),
    crate::Entry::new(
      Term::new(1),
      last,
      crate::EntryKind::ThawDischarged,
      witness,
    ),
  ]);
  stable.force_state(Term::new(1), Some(1u64), last);
  m.restore_group_unchecked(
    1,
    single_node_cfg(1),
    now,
    7,
    CountSm::default(),
    1,
    &mut log,
    &mut stable,
  )
  .unwrap();
  let mut stores = MapStores(std::collections::BTreeMap::new(), Default::default());
  stores.0.insert(1, (log, stable));
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  assert_eq!(
    m.group(&1)
      .unwrap()
      .planned_witnesses()
      .iter()
      .map(|(pair, at)| (pair.clone(), *at))
      .collect::<Vec<_>>(),
    std::vec![((gid_key(7), 1u64), Index::new(3))],
    "one entry for the pair, at the lower index"
  );
}

/// THE ABSORB POINT IS PER-BOUNDARY (#137): a park at k with crossings at k+3, k+7 and k+11 —
/// driven through the crossing walk itself — answers each boundary with the highest crossing
/// at-or-below it, never one above it, and answers nothing at all for a boundary the walk has
/// not examined.
#[test]
fn the_crossing_absorb_point_is_the_highest_crossing_at_or_below_the_boundary() {
  let now = Instant::ORIGIN;
  let k = 2u64;
  let last = Index::new(k + 12);
  let cmd = {
    let mut buf = Vec::new();
    Bytes::from_static(b"c").encode(&mut buf);
    Bytes::from(buf)
  };
  let mut entries = std::vec![
    crate::Entry::new(
      Term::new(1),
      Index::new(1),
      crate::EntryKind::Normal,
      cmd.clone()
    ),
    crate::Entry::new(
      Term::new(1),
      Index::new(k),
      crate::EntryKind::CommitMerge,
      commit_merge_bytes(42, Index::new(5), 1, 1),
    ),
  ];
  for i in (k + 1)..=last.get() {
    let (kind, data) = match i - k {
      3 | 7 | 11 => (
        crate::EntryKind::CommitMerge,
        commit_merge_bytes(40 + i, Index::new(1), 1, 2),
      ),
      _ => (crate::EntryKind::Normal, cmd.clone()),
    };
    entries.push(crate::Entry::new(Term::new(1), Index::new(i), kind, data));
  }
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();
  log.force_append(&entries);
  stable.force_state(Term::new(1), Some(1u64), last);
  m.restore_group_unchecked(
    1,
    single_node_cfg(1),
    now,
    7,
    SplitSm::default(),
    1,
    &mut log,
    &mut stable,
  )
  .unwrap();
  assert_eq!(
    m.group(&1).unwrap().pending_merge().map(|p| p.at()),
    Some(Index::new(k)),
    "parked at k"
  );
  let ep = m.group_mut(&1).unwrap();
  ep.advance_crossing_scan(&log);
  assert!(
    ep.crossing_walk_covers(last),
    "one warm crank walks the whole tail"
  );
  let at = |i: u64| Index::new(k + i);
  assert_eq!(
    ep.crossing_absorb_at(at(2)),
    None,
    "k+2: no crossing at-or-below"
  );
  assert_eq!(
    ep.crossing_absorb_at(at(3)),
    Some(at(3)),
    "k+3: the crossing itself"
  );
  assert_eq!(
    ep.crossing_absorb_at(at(7)),
    Some(at(7)),
    "k+7: the crossing itself"
  );
  assert_eq!(
    ep.crossing_absorb_at(at(10)),
    Some(at(7)),
    "in [k+7, k+11): the highest at-or-below, never the one above"
  );
  assert_eq!(
    ep.crossing_absorb_at(at(12)),
    Some(at(11)),
    "the frontier: the highest crossing at-or-below"
  );
  assert_eq!(
    ep.crossing_absorb_at(at(13)),
    None,
    "above the walk's frontier: unexamined, no answer"
  );
}

/// THE DESTRUCTIVE-INSTALL RESIDUAL, PINNED (#138, residual 12): target 1 applied an abort for
/// source 2, then installed the leader's snapshot past it — a leader that had applied the witness
/// and captured beyond it, so no witness will ever reach this replica. The install-covered record
/// stays LIVE here with nothing to retire it short of a new global proof: it fences no capture (the
/// entry is gone), its holder is removable through the step-aside while the source is unhosted, and
/// `SourceOwesThaw` holds the holder as a merge source. This is the recorded outcome, not a fix; the
/// retention itself is pinned by the install tests (`an_install_past_the_abort_retains…`,
/// `snapshot_install_marks_the_covered_abort_relay`) whose red is the old drop.
#[test]
fn an_install_covered_dead_end_is_the_recorded_residual() {
  let now = Instant::ORIGIN;
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let mut stores = MapStores(std::collections::BTreeMap::new(), Default::default());
  let abort = {
    let p = crate::RollbackMergePayload::abort(gid_key(2), 1, 1);
    let mut buf = Vec::new();
    crate::wire::encode_rollback_merge_payload(&p, &mut buf);
    Bytes::from(buf)
  };
  let mut tlog = VecLog::default();
  let mut tstable = AsyncStable::default();
  tlog.force_append(&[crate::Entry::new(
    Term::new(1),
    Index::new(1),
    crate::EntryKind::RollbackMerge,
    abort,
  )]);
  tstable.force_state(Term::new(1), Some(1u64), Index::new(1));
  m.restore_group_unchecked(
    1,
    single_node_cfg(1).with_snapshot_threshold(1),
    now,
    7,
    CountSm::default(),
    1,
    &mut tlog,
    &mut tstable,
  )
  .unwrap();
  stores.0.insert(1, (tlog, tstable));
  install_past_the_abort(&mut m, &mut stores);
  let t = m.group(&1).unwrap();
  assert!(
    t.owes_live_thaw()
      && t
        .abandoned_record(&gid_key(2))
        .is_some_and(|r| r.cover == Cover::Install && !r.discharged),
    "the install-covered record stands, live — the witness that would retire it never arrives"
  );
  assert!(
    !t.capture_blocked_at(t.applied_index()),
    "it fences no capture: the entry it protected is gone"
  );
  // The holder leads and refuses to freeze as a merge source; its captures proceed freely.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    let d = m.group(&1).unwrap().poll_timeout().unwrap();
    m.handle_timeout(&1, d, log, stable).unwrap();
    drain_storage(&mut m, 1, d, log, stable);
    assert!(m.group(&1).unwrap().role().is_leader());
  }
  stores
    .0
    .insert(0, (VecLog::default(), AsyncStable::default()));
  m.create_group(0, 0, single_node_cfg(1), now, 9, CountSm::default())
    .unwrap();
  let verdict = m.prepare_merge(&1, now, &mut stores, &0);
  assert!(
    matches!(verdict, Some(Err(MergeError::SourceOwesThaw))),
    "the live record holds the holder as a merge source: {verdict:?}"
  );
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.propose(&1, now, log, stable, &Bytes::from_static(b"x"))
      .unwrap()
      .unwrap();
    drain_storage(&mut m, 1, now, log, stable);
    assert!(
      stable
        .snapshot()
        .is_some_and(|(meta, _)| meta.last_index() > Index::new(5)),
      "the threshold capture landed past the install with the record standing"
    );
  }
  assert_eq!(
    m.remove_group(&1, &mut stores).map(|r| r.is_some()),
    Ok(true),
    "and the holder is removable through the step-aside: its source is unhosted"
  );
}

/// A log whose TERM read faults at one index — the AdvanceSource identity read's fatal seam.
struct FaultTermLog(VecLog, Option<Index>);

#[derive(Debug)]
struct TermErr;

impl core::fmt::Display for TermErr {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.write_str("term read fault")
  }
}

impl core::error::Error for TermErr {}

impl crate::LogStore for FaultTermLog {
  type Error = TermErr;

  fn first_index(&self) -> Index {
    self.0.first_index()
  }

  fn last_index(&self) -> Index {
    self.0.last_index()
  }

  fn term(&self, index: Index) -> Result<Term, TermErr> {
    if self.1 == Some(index) {
      return Err(TermErr);
    }
    self.0.term(index).map_err(|e| match e {})
  }

  fn entries(
    &self,
    range: core::ops::Range<Index>,
    max_bytes: u64,
  ) -> Result<crate::EntriesRead<'_>, TermErr> {
    self.0.entries(range, max_bytes).map_err(|e| match e {})
  }

  fn submit_append(&mut self, id: crate::OpId, entries: &[crate::Entry]) {
    self.0.submit_append(id, entries)
  }

  fn compact(&mut self, up_to: Index) {
    self.0.compact(up_to)
  }

  fn restore(&mut self, last_index: Index, last_term: Term) {
    self.0.restore(last_index, last_term)
  }

  fn poll(&mut self) -> Option<Result<crate::LogDone, TermErr>> {
    self.0.poll().map(|r| r.map_err(|e| match e {}))
  }

  fn has_pending(&self) -> bool {
    self.0.has_pending()
  }
}

struct FaultTermStores(std::collections::BTreeMap<u64, (FaultTermLog, AsyncStable)>);

impl crate::GroupStores<u64, FaultTermLog, AsyncStable> for FaultTermStores {
  fn stores(&mut self, group: &u64) -> Option<(&mut FaultTermLog, &mut AsyncStable)> {
    self.0.get_mut(group).map(|(l, s)| (l, s))
  }
}

impl crate::FloorStore<u64> for FaultTermStores {
  fn floor(&self, _gid: &u64) -> u64 {
    0
  }

  fn lineage(&self, _gid: &u64) -> u64 {
    0
  }
}

/// The AdvanceSource identity read can POISON the source (a fatal term fault) while answering
/// false — and the crank's earlier lifecycle drain has already run, so without the latch the
/// fail-stop would sit invisible until unrelated traffic. It must reach `poll_poisoned` in the
/// faulting crank itself.
#[test]
fn a_faulting_advance_source_identity_read_latches_in_the_same_crank() {
  let now = Instant::ORIGIN;
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let cmd = {
    let mut buf = Vec::new();
    Bytes::from_static(b"c").encode(&mut buf);
    Bytes::from(buf)
  };
  // The source: its log CONTAINS the freeze pair but commit lags behind it, the AdvanceSource
  // shape. The term read at the boundary faults.
  let mut log2 = FaultTermLog(VecLog::default(), Some(Index::new(2)));
  let mut stable2 = AsyncStable::default();
  {
    let mut tb = Vec::new();
    Data::encode(&1u64, &mut tb);
    let mut fbuf = Vec::new();
    crate::wire::encode_prepare_merge_payload(
      &crate::PrepareMergePayload::new(Bytes::from(tb), 1),
      &mut fbuf,
    );
    log2.0.force_append(&[
      crate::Entry::new(
        Term::new(1),
        Index::new(1),
        crate::EntryKind::Normal,
        cmd.clone(),
      ),
      crate::Entry::new(
        Term::new(1),
        Index::new(2),
        crate::EntryKind::PrepareMerge,
        Bytes::from(fbuf),
      ),
    ]);
    stable2.force_state(Term::new(1), Some(1u64), Index::new(1));
  }
  m.restore_group_unchecked(
    2,
    single_node_cfg(1),
    now,
    8,
    SplitSm::default(),
    1,
    &mut log2,
    &mut stable2,
  )
  .unwrap();
  assert!(
    !m.group(&2).unwrap().is_frozen(),
    "the freeze is appended, not applied — commit lags"
  );

  let mut log1 = FaultTermLog(VecLog::default(), None);
  let mut stable1 = AsyncStable::default();
  log1.0.force_append(&[
    crate::Entry::new(
      Term::new(1),
      Index::new(1),
      crate::EntryKind::Normal,
      cmd.clone(),
    ),
    crate::Entry::new(
      Term::new(1),
      Index::new(2),
      crate::EntryKind::CommitMerge,
      commit_merge_bytes(2, Index::new(2), 1, 1),
    ),
    crate::Entry::new(Term::new(1), Index::new(3), crate::EntryKind::Normal, cmd),
  ]);
  stable1.force_state(Term::new(1), Some(1u64), Index::new(3));
  m.restore_group_unchecked(
    1,
    single_node_cfg(1),
    now,
    7,
    SplitSm::default(),
    1,
    &mut log1,
    &mut stable1,
  )
  .unwrap();
  assert!(m.group(&1).unwrap().pending_merge().is_some(), "parked");

  let mut stores = FaultTermStores(std::collections::BTreeMap::new());
  stores.0.insert(1, (log1, stable1));
  stores.0.insert(2, (log2, stable2));
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  let sep = m.group(&2).unwrap();
  assert!(
    sep.is_poisoned(),
    "the identity read fail-stopped the source"
  );
  assert_eq!(sep.poison_reason(), Some(PoisonReason::LogTerm));
  assert_eq!(
    m.poll_poisoned(),
    Some(2),
    "latched in the faulting crank itself"
  );
}

/// The held-merge signal is DELIVERED-BEFORE-CONSUMED at the container seam: `peek` exposes the
/// head without consuming (however many times), and only `poll` retires it — what lets a driver
/// facing a full lifecycle tail leave the signal queued instead of losing the one edge its
/// dedupe will never re-arm.
#[test]
fn a_peeked_merge_blocked_signal_survives_until_polled() {
  let now = Instant::ORIGIN;
  let (mut m, mut stores) = under_hosted_park_host();
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  let head = m.peek_merge_blocked().expect("the hold signalled");
  assert_eq!(
    m.peek_merge_blocked().as_ref(),
    Some(&head),
    "peek consumes nothing"
  );
  assert_eq!(m.poll_merge_blocked(), Some(head), "poll retires the head");
  assert_eq!(m.peek_merge_blocked(), None);
}

/// A signal still QUEUED when its hold resolves is PURGED, never delivered late: a full
/// lifecycle tail can hold the observation undelivered across the hold's whole life, and
/// publishing it after the cure would prompt the placement layer to act on a hold that no
/// longer exists — re-hosting an absorbed source beside the cured union.
#[test]
fn a_resolved_holds_queued_signal_is_purged() {
  let now = Instant::ORIGIN;
  let (mut m, mut stores) = under_hosted_park_host();
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  assert!(
    m.peek_merge_blocked().is_some(),
    "the hold signalled and nobody drained it"
  );

  // The cure adopts in place of the impossible fold; the hold is gone.
  let meta = crate::SnapshotMeta::new(
    Index::new(3),
    Term::new(1),
    crate::conf::ConfState::from_voters(std::vec![1u64]),
  )
  .with_shape_gen(1);
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.handle_message(
      &1,
      now,
      log,
      stable,
      9u64,
      Message::InstallSnapshot(crate::InstallSnapshot::new(
        Term::new(1),
        9u64,
        meta,
        fork_blob(5),
      )),
    )
    .unwrap();
  }
  assert!(m.group(&1).unwrap().pending_merge().is_none(), "adopted");
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  assert_eq!(
    m.poll_merge_blocked(),
    None,
    "the undelivered observation retired with its hold"
  );
}

/// The SAME-CRANK twin: the hold resolves inside the very service call whose end the drivers
/// drain after — an entry-time retirement would still see the park standing and deliver the
/// stale signal. The retirement runs at the END of the crank, keyed on what the crank still
/// derived.
#[test]
fn a_same_crank_park_resolution_purges_the_queued_signal() {
  let now = Instant::ORIGIN;
  let (mut m, mut stores) = under_hosted_park_host();
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  assert!(
    m.peek_merge_blocked().is_some(),
    "the hold signalled and nobody drained it"
  );

  // A terminal floor on the source — the absent arm's replayed-duplicate reading — resolves the
  // park INSIDE the next service call.
  stores.1.insert(42);
  let resolutions = m.service_merge_applies(now, &mut stores);
  assert!(
    !resolutions.is_empty(),
    "the terminal floor resolved the park this crank: {resolutions:?}"
  );
  assert!(m.group(&1).unwrap().pending_merge().is_none());
  assert_eq!(
    m.poll_merge_blocked(),
    None,
    "the queued signal retired with the same-crank resolution"
  );
}

/// The debt flavor of the same-crank twin: the fence signal sits queued undelivered, the fence
/// lifts, and the discharge lands inside one service call — nothing stale may survive to the
/// drivers' immediate post-service drain.
#[test]
fn a_same_crank_debt_discharge_purges_the_queued_signal() {
  let (mut m, mut stores, _k, _split_idx, d, _ds) = fork_fenced_park_fixture();
  defer_to_absorbed(&mut m, &mut stores, d);
  assert!(
    m.service_merge_applies(d, &mut stores).is_empty(),
    "the debt waits for the fence"
  );
  assert!(
    m.peek_merge_blocked().is_some(),
    "the fence hold signalled and nobody drained it"
  );

  m.remove_group(&200, &mut empty_stores()).unwrap();
  let split = install_head_fork(&mut m, 1, 200, d);
  m.lift_fork_barrier(&1, split);
  let resolutions = m.service_merge_applies(d, &mut stores);
  assert!(
    resolutions.iter().any(|r| matches!(
      r,
      MergeResolution::Merged {
        source: 2,
        target: 1
      }
    )),
    "the discharge landed this crank: {resolutions:?}"
  );
  assert_eq!(
    m.poll_merge_blocked(),
    None,
    "the queued fence signal retired with the same-crank discharge"
  );
}

/// An under-hosted park on a single-node host, with the `k+1` coordinate committed so the abort
/// window is closed and the resolver reaches the source lookup. The source (42) is unhosted at a
/// non-terminal floor — the locally-unresolvable shape.
fn under_hosted_park_host() -> (MultiRaft<u64, u64, SplitSm>, MapStores) {
  let now = Instant::ORIGIN;
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let cmd = {
    let mut buf = Vec::new();
    Bytes::from_static(b"c").encode(&mut buf);
    Bytes::from(buf)
  };
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();
  log.force_append(&[
    crate::Entry::new(
      Term::new(1),
      Index::new(1),
      crate::EntryKind::Normal,
      cmd.clone(),
    ),
    crate::Entry::new(
      Term::new(1),
      Index::new(2),
      crate::EntryKind::CommitMerge,
      commit_merge_bytes(42, Index::new(5), 1, 1),
    ),
    crate::Entry::new(Term::new(1), Index::new(3), crate::EntryKind::Normal, cmd),
  ]);
  stable.force_state(Term::new(1), Some(1u64), Index::new(3));
  m.restore_group_unchecked(
    1,
    single_node_cfg(1),
    now,
    7,
    SplitSm::default(),
    1,
    &mut log,
    &mut stable,
  )
  .unwrap();
  assert!(m.group(&1).unwrap().pending_merge().is_some(), "parked");
  let mut stores = MapStores(std::collections::BTreeMap::new(), Default::default());
  stores.0.insert(1, (log, stable));
  (m, stores)
}

/// A completion-time redundant install — staged while `commit` was behind its boundary, covered
/// by the time its blob turned durable — raises the DURABLE snapshot index and deliberately
/// keeps the log: durability without compaction. The owed adopt capture must NOT discharge on
/// that evidence: the absorb membership fence waits on `first_index` passing the absorb point,
/// so shedding the obligation there would wedge every voter change until unrelated threshold
/// traffic. The service stages the forced capture anyway; its compaction releases the fence and
/// a voter add is admitted again.
#[test]
fn a_redundant_install_does_not_shed_the_adopts_compaction_obligation() {
  let now = Instant::ORIGIN;
  let (mut m, mut stores) = under_hosted_park_host();
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  assert_eq!(
    m.group(&1).unwrap().merge_park_unresolvable(),
    Some(Index::new(2))
  );

  // The cure blob adopts, leaving the one owed forced capture.
  let meta = crate::SnapshotMeta::new(
    Index::new(3),
    Term::new(1),
    crate::conf::ConfState::from_voters(std::vec![1u64]),
  )
  .with_shape_gen(1);
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.handle_message(
      &1,
      now,
      log,
      stable,
      9u64,
      Message::InstallSnapshot(crate::InstallSnapshot::new(
        Term::new(1),
        9u64,
        meta,
        fork_blob(5),
      )),
    )
    .unwrap();
    while matches!(
      m.handle_storage(&1, now, log, stable),
      Some(StorageProgress::MorePending)
    ) {}
  }
  while m.poll_message().is_some() {}
  assert!(m.group(&1).unwrap().adopt_capture_owed());

  // Manufacture the redundant completion: stage an install one past commit (nothing local
  // covers it, so it defers on blob durability), then let the leader's append carry that entry
  // and the commit over it — the completion finds itself covered, records durability, and
  // keeps the log.
  let cmd = {
    let mut buf = Vec::new();
    Bytes::from_static(b"c").encode(&mut buf);
    Bytes::from(buf)
  };
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    let meta4 = crate::SnapshotMeta::new(
      Index::new(4),
      Term::new(1),
      crate::conf::ConfState::from_voters(std::vec![1u64]),
    )
    .with_shape_gen(1);
    m.handle_message(
      &1,
      now,
      log,
      stable,
      9u64,
      Message::InstallSnapshot(crate::InstallSnapshot::new(
        Term::new(1),
        9u64,
        meta4,
        fork_blob(9),
      )),
    )
    .unwrap();
    m.handle_message(
      &1,
      now,
      log,
      stable,
      9u64,
      Message::AppendEntries(crate::AppendEntries::new(
        Term::new(1),
        9u64,
        Index::new(3),
        Term::new(1),
        std::vec![crate::Entry::new(
          Term::new(1),
          Index::new(4),
          crate::EntryKind::Normal,
          cmd
        )],
        Index::new(4),
      )),
    )
    .unwrap();
    while matches!(
      m.handle_storage(&1, now, log, stable),
      Some(StorageProgress::MorePending)
    ) {}
    assert_eq!(
      m.group(&1).unwrap().state_machine().units,
      6,
      "the covered completion restored nothing — entry 4 applied on top of the union"
    );
    assert!(
      sailing_proto_durable_covers(stable, Index::new(4)),
      "durability was recorded"
    );
    assert_eq!(
      log.first_index(),
      Index::new(1),
      "and the log was deliberately kept"
    );
  }
  while m.poll_message().is_some() {}
  assert!(
    m.group(&1).unwrap().adopt_capture_owed(),
    "durability without compaction does not discharge the obligation"
  );

  // The service still stages the forced capture; its compaction is what the fence awaits.
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    while matches!(
      m.handle_storage(&1, now, log, stable),
      Some(StorageProgress::MorePending)
    ) {}
    assert!(
      log.first_index() > Index::new(2),
      "the owed compaction released the fence"
    );
  }
  assert!(!m.group(&1).unwrap().adopt_capture_owed());
  while m.poll_message().is_some() {}
  while m.poll_event().is_some() {}

  // The membership fence has genuinely released: a voter add is admitted.
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    let d = m.group(&1).unwrap().poll_timeout().unwrap();
    m.handle_timeout(&1, d, log, stable).unwrap();
    while matches!(
      m.handle_storage(&1, d, log, stable),
      Some(StorageProgress::MorePending)
    ) {}
    assert!(m.group(&1).unwrap().role().is_leader());
    m.propose_conf_change(
      &1,
      d,
      log,
      stable,
      crate::conf::ConfChange::new(crate::ConfChangeType::AddNode, 2u64, Bytes::new()),
    )
    .unwrap()
    .unwrap();
  }
}

/// The owed adopt capture FAULTS (`snapshot()` errors): the endpoint fail-stops, and the poison
/// must LATCH for `poll_poisoned` in the same crank — the ower is filtered from every later
/// pass and a poisoned endpoint arms no timer, so without the latch an idle adopter\'s fail-stop
/// stays invisible until unrelated traffic touches the group. No `CaptureFailed` surfaces: that
/// contract names a consumed source\'s stranded routing, and the adopt has none.
#[test]
fn an_adopts_faulting_capture_fail_stops_and_latches() {
  let now = Instant::ORIGIN;
  let mut m: MultiRaft<u64, u64, SnapFailSm> = MultiRaft::new();
  let cmd = {
    let mut buf = Vec::new();
    Bytes::from_static(b"c").encode(&mut buf);
    Bytes::from(buf)
  };
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();
  log.force_append(&[
    crate::Entry::new(
      Term::new(1),
      Index::new(1),
      crate::EntryKind::Normal,
      cmd.clone(),
    ),
    crate::Entry::new(
      Term::new(1),
      Index::new(2),
      crate::EntryKind::CommitMerge,
      commit_merge_bytes(42, Index::new(5), 1, 1),
    ),
    crate::Entry::new(Term::new(1), Index::new(3), crate::EntryKind::Normal, cmd),
  ]);
  stable.force_state(Term::new(1), Some(1u64), Index::new(3));
  let fail = std::sync::Arc::new(core::sync::atomic::AtomicBool::new(false));
  m.restore_group_unchecked(
    1,
    single_node_cfg(1),
    now,
    7,
    SnapFailSm {
      count: 0,
      fail: fail.clone(),
    },
    1,
    &mut log,
    &mut stable,
  )
  .unwrap();
  assert!(m.group(&1).unwrap().pending_merge().is_some(), "parked");
  let mut stores = MapStores(std::collections::BTreeMap::new(), Default::default());
  stores.0.insert(1, (log, stable));
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  assert_eq!(
    m.group(&1).unwrap().merge_park_unresolvable(),
    Some(Index::new(2))
  );
  let meta = crate::SnapshotMeta::new(
    Index::new(3),
    Term::new(1),
    crate::conf::ConfState::from_voters(std::vec![1u64]),
  )
  .with_shape_gen(1);
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.handle_message(
      &1,
      now,
      log,
      stable,
      9u64,
      Message::InstallSnapshot(crate::InstallSnapshot::new(
        Term::new(1),
        9u64,
        meta,
        fork_blob(5),
      )),
    )
    .unwrap();
    while matches!(
      m.handle_storage(&1, now, log, stable),
      Some(StorageProgress::MorePending)
    ) {}
  }
  while m.poll_message().is_some() {}
  assert!(m.group(&1).unwrap().adopt_capture_owed());
  assert_eq!(m.group(&1).unwrap().state_machine().count, 5);

  fail.store(true, core::sync::atomic::Ordering::Relaxed);
  assert!(
    m.service_merge_applies(now, &mut stores).is_empty(),
    "no CaptureFailed: no consumed source stands behind the obligation"
  );
  let ep = m.group(&1).unwrap();
  assert!(ep.is_poisoned());
  assert_eq!(ep.poison_reason(), Some(PoisonReason::SnapshotCapture));
  assert!(ep.adopt_capture_owed(), "nothing was discharged");
  assert_eq!(
    m.poll_poisoned(),
    Some(1),
    "the fail-stop latched in the faulting crank"
  );
  assert_eq!(m.poll_poisoned(), None);
}

/// The structural-hold signal is EDGE-triggered: an under-hosted park names its cause ONCE and
/// then stays silent however many cranks re-derive it, and a genuine change of cause — the source
/// arriving, still below its freeze generation — signals exactly once more.
#[test]
fn a_structurally_held_park_signals_its_cause_once_per_transition() {
  use crate::{MergeBlocked, MergeBlockedCause};
  let now = Instant::ORIGIN;
  let (mut m, mut stores) = under_hosted_park_host();

  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  assert_eq!(
    m.poll_merge_blocked(),
    Some(MergeBlocked {
      target: 1,
      source: 42,
      boundary: Index::new(2),
      cause: MergeBlockedCause::SourceUnhosted,
    }),
    "the hold names its cause and the park's own coordinate"
  );
  assert_eq!(
    m.poll_merge_blocked(),
    None,
    "one signal, not one per crank"
  );

  for _ in 0..5 {
    assert!(m.service_merge_applies(now, &mut stores).is_empty());
  }
  assert_eq!(
    m.poll_merge_blocked(),
    None,
    "the edge holds for as long as the cause does"
  );

  // The source can no longer be ADMITTED while this park stands: its window has latched CLOSED,
  // so the id is committed-consumed and every admission door refuses it. Reviving a husk beside a
  // union whose consumption no abort can contest is exactly what that refusal exists to stop —
  // the cause-transition this used to drive is unreachable from here by design, and the sibling
  // test below covers it from a source hosted before the window closed.
  assert_eq!(
    m.create_group(42, 0, single_node_cfg(1), now, 9, SplitSm::default()),
    Err(CreateGroupError::AbsorbPending),
  );
}

/// The other cause on the same edge machinery: a source hosted here but BEHIND its freeze
/// expectation holds the park for a different reason, and the change of cause is a fresh
/// transition. The source is admitted before the first service pass — while the park is still
/// undecided — because a latched-CLOSED park refuses the admission outright.
#[test]
fn a_park_held_by_a_behind_source_signals_its_own_cause() {
  let now = Instant::ORIGIN;
  let (mut m, mut stores) = under_hosted_park_host();
  m.create_group(42, 0, single_node_cfg(1), now, 9, SplitSm::default())
    .unwrap();
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  assert_eq!(
    m.poll_merge_blocked(),
    Some(MergeBlocked {
      target: 1,
      source: 42,
      boundary: Index::new(2),
      cause: MergeBlockedCause::SourceBehind,
    })
  );
  for _ in 0..3 {
    assert!(m.service_merge_applies(now, &mut stores).is_empty());
  }
  assert_eq!(m.poll_merge_blocked(), None, "and then silence again");
}

/// Resolution RETIRES the edge rather than remembering it: the cure blob adopts in place of the
/// fold, the next crank drops the target's remembered cause, and nothing stale is left queued —
/// so a later hold on the same target signals afresh instead of being deduped forever.
#[test]
fn a_resolved_park_retires_its_blocked_edge() {
  let now = Instant::ORIGIN;
  let (mut m, mut stores) = under_hosted_park_host();
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  assert!(m.poll_merge_blocked().is_some(), "held, and signalled");

  let meta = crate::SnapshotMeta::new(
    Index::new(3),
    Term::new(1),
    crate::conf::ConfState::from_voters(std::vec![1u64]),
  )
  .with_shape_gen(1);
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.handle_message(
      &1,
      now,
      log,
      stable,
      9u64,
      Message::InstallSnapshot(crate::InstallSnapshot::new(
        Term::new(1),
        9u64,
        meta,
        fork_blob(5),
      )),
    )
    .unwrap();
  }
  assert!(
    m.group(&1).unwrap().pending_merge().is_none(),
    "the adopt cleared the park"
  );
  let _ = m.service_merge_applies(now, &mut stores);
  assert_eq!(
    m.poll_merge_blocked(),
    None,
    "a resolved hold signals nothing further"
  );
  assert!(
    m.merge_blocked_seen.is_empty(),
    "the edge is retired with the hold that justified it"
  );
}

/// One debt at a time is a HOST-LOCAL invariant: a debt-free foreign leader's propose gates
/// cannot see this host's standing fences, so a second committed absorb can legally park here
/// mid-window — and it must HOLD, never defer, or the second mint would overwrite the first
/// debt's held `Merged` and strand that source's stores forever. The hold releases on the first
/// debt's own discharge.
#[test]
fn a_second_committed_absorb_holds_behind_a_standing_debt() {
  let (mut m, mut stores, _k, _split_idx, d, _ds) = fork_fenced_park_fixture();
  defer_to_absorbed(&mut m, &mut stores, d);
  assert!(m.group(&1).unwrap().capture_debt().is_some());

  // A debt-free foreign leader committed a second absorb into this target; model its arrival by
  // proposing at the ENDPOINT seam (the container's own gates would refuse — that is exactly the
  // point: they run on the proposer, not on this host).
  {
    let (l, s) = stores.0.get_mut(&1).unwrap();
    let ep = m.group_mut(&1).unwrap();
    ep.propose_merge_entry(
      d,
      l,
      crate::EntryKind::CommitMerge,
      commit_merge_bytes(3, Index::new(9), 1, 3),
    )
    .unwrap();
    m.flush_appends(&1, d, l, s).unwrap();
    while matches!(
      m.handle_storage(&1, d, l, s),
      Some(StorageProgress::MorePending)
    ) {}
  }
  assert!(
    m.group(&1).unwrap().pending_merge().is_some(),
    "the second absorb parked"
  );
  let first_debt_source = m.group(&1).unwrap().capture_debt().unwrap().source();
  // The resolver HOLDS: no second Absorbed, no overwritten debt.
  let resolutions = m.service_merge_applies(d, &mut stores);
  assert!(
    !resolutions
      .iter()
      .any(|r| matches!(r, MergeResolution::Absorbed { .. })),
    "a standing debt holds the next park — deferring would overwrite the held Merged"
  );
  assert!(m.group(&1).unwrap().pending_merge().is_some());
  assert_eq!(
    m.group(&1).unwrap().capture_debt().unwrap().source(),
    first_debt_source,
    "the first debt survives byte-identical"
  );

  // The first debt's discharge releases the hold; the second park then takes its own course.
  m.remove_group(&200, &mut empty_stores()).unwrap();
  let split = install_head_fork(&mut m, 1, 200, d);
  m.lift_fork_barrier(&1, split);
  let resolutions = m.service_merge_applies(d, &mut stores);
  assert!(
    resolutions.iter().any(|r| matches!(
      r,
      MergeResolution::Merged {
        source: 2,
        target: 1
      }
    )),
    "the first union floors and tears down"
  );
  assert!(m.group(&1).unwrap().capture_debt().is_none());
}

/// The discharge fence is keyed at the CAPTURE POINT, never the absorb boundary: a Split
/// applied INSIDE the debt window sits above the boundary — invisible to a boundary-keyed leg —
/// while the forced capture's compaction at `applied` would erase exactly the entry that is the
/// staged fork's only recovery source. The discharge waits for that fork too.
#[test]
fn a_split_inside_the_debt_window_fences_the_discharge() {
  let (mut m, mut stores, _k, _split_idx, d, _ds) = fork_fenced_park_fixture();
  defer_to_absorbed(&mut m, &mut stores, d);

  // A second split lands INSIDE the window (above the absorb boundary).
  {
    let (l, s) = stores.0.get_mut(&1).unwrap();
    m.propose_split(&1, d, l, s, &300, 0, Bytes::from_static(b"\x03"))
      .unwrap()
      .unwrap();
    m.flush_appends(&1, d, l, s).unwrap();
    while matches!(
      m.handle_storage(&1, d, l, s),
      Some(StorageProgress::MorePending)
    ) {}
  }
  // The ORIGINAL fence lifts; the in-window fork's barrier must keep fencing the discharge.
  m.remove_group(&200, &mut empty_stores()).unwrap();
  let mut engine: GroupEngine<u64, u64> = GroupEngine::new();
  let InstallOutcome::Installed { split_index, .. } =
    m.install_yieldable_fork(&1, &200, &mut engine, &NoHold, d, 43)
  else {
    panic!("the original fork installs once its squatter leaves")
  };
  m.lift_fork_barrier(&1, split_index);
  assert!(
    m.service_merge_applies(d, &mut stores).is_empty(),
    "the in-window fork's replay entry sits above the boundary; a boundary-keyed fence would \
     miss it and the compaction would erase the staged fork's recovery source"
  );
  assert!(m.group(&1).unwrap().capture_debt().is_some());

  // The in-window fork resolves; the discharge follows.
  let split2 = m
    .peek_yieldable_fork(&NoHold)
    .expect("the in-window fork")
    .split_index();
  m.lift_fork_barrier(&1, split2);
  assert!(
    m.service_merge_applies(d, &mut stores)
      .iter()
      .any(|r| matches!(
        r,
        MergeResolution::Merged {
          source: 2,
          target: 1
        }
      )),
    "both replay entries safe, the union floors"
  );
}

/// A committed CommitMerge ABOVE an unresolvable park, naming a LOCALLY HOSTED source,
/// withholds the cure advertisement: an adopt would cross that entry without resolving it,
/// leaving the hosted replica a live-voting husk of an absorbed-away (or stale-no-op) lineage —
/// a classification only the full apply machinery can make, so the refusal is outcome-blind.
/// The hint appears the moment the hosted crossing leaves.
#[test]
fn a_crossing_with_a_hosted_source_withholds_the_cure() {
  let now = Instant::ORIGIN;
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  // The crossing's source, hosted here. The PARK's own source (41) stays unhosted.
  m.create_group(42, 0, single_node_cfg(1), now, 8, SplitSm::default())
    .unwrap();
  let cmd = {
    let mut buf = Vec::new();
    Bytes::from_static(b"c").encode(&mut buf);
    Bytes::from(buf)
  };
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();
  log.force_append(&[
    crate::Entry::new(
      Term::new(1),
      Index::new(1),
      crate::EntryKind::Normal,
      cmd.clone(),
    ),
    crate::Entry::new(
      Term::new(1),
      Index::new(2),
      crate::EntryKind::CommitMerge,
      commit_merge_bytes(41, Index::new(5), 1, 1),
    ),
    crate::Entry::new(
      Term::new(1),
      Index::new(3),
      crate::EntryKind::CommitMerge,
      commit_merge_bytes(42, Index::new(6), 1, 2),
    ),
    crate::Entry::new(Term::new(1), Index::new(4), crate::EntryKind::Normal, cmd),
  ]);
  stable.force_state(Term::new(1), Some(1u64), Index::new(4));
  m.restore_group_unchecked(
    1,
    single_node_cfg(1),
    now,
    7,
    SplitSm::default(),
    1,
    &mut log,
    &mut stable,
  )
  .unwrap();
  assert!(
    m.group(&1).unwrap().pending_merge().is_some(),
    "parked on unhosted 41"
  );
  let mut stores = MapStores(std::collections::BTreeMap::new(), Default::default());
  stores.0.insert(1, (log, stable));

  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  assert_eq!(
    m.group(&1).unwrap().merge_park_unresolvable(),
    None,
    "the hosted crossing withholds the hint, outcome-blind"
  );
  // ONE composed signal per transition, carrying the ACTIONABLE identity: the hosted
  // crossing's id (whose lifecycle releases the wedge) and the park coordinate — the generic
  // unhosted-source cause is suppressed while the crossing outranks it, and further cranks
  // emit nothing new.
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  let mut blocked = std::vec::Vec::new();
  while let Some(b) = m.poll_merge_blocked() {
    blocked.push(b);
  }
  assert_eq!(
    blocked.len(),
    1,
    "one emission for a stable hold: {blocked:?}"
  );
  assert!(
    matches!(
      &blocked[0],
      b if b.cause == MergeBlockedCause::CrossedHostedSource
        && b.source == 42
        && b.boundary == Index::new(2)
    ),
    "the signal names the hosted crossing and the park coordinate: {blocked:?}"
  );

  // The hosted crossing leaves; the hint appears on the next crank.
  m.remove_group(&42, &mut empty_stores()).unwrap();
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  assert_eq!(
    m.group(&1).unwrap().merge_park_unresolvable(),
    Some(Index::new(2)),
    "with no hosted crossing the park advertises"
  );
}

/// A cure blob beyond the locally proven commit never adopts — the crossing walk is
/// committed-capped, so entries between local commit and the blob's boundary were never
/// examined and one could be a hosted crossing. The delivery is not wasted: the redundancy
/// arm's raise advances commit off the blob's own evidence, the walk catches up next crank,
/// and the hint re-evaluates against the newly visible crossing.
#[test]
fn a_blob_beyond_local_commit_defers_until_the_walk_catches_up() {
  let now = Instant::ORIGIN;
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  m.create_group(42, 0, single_node_cfg(1), now, 8, SplitSm::default())
    .unwrap();
  let cmd = {
    let mut buf = Vec::new();
    Bytes::from_static(b"c").encode(&mut buf);
    Bytes::from(buf)
  };
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();
  log.force_append(&[
    crate::Entry::new(
      Term::new(1),
      Index::new(1),
      crate::EntryKind::Normal,
      cmd.clone(),
    ),
    crate::Entry::new(
      Term::new(1),
      Index::new(2),
      crate::EntryKind::CommitMerge,
      commit_merge_bytes(41, Index::new(5), 1, 1),
    ),
    crate::Entry::new(Term::new(1), Index::new(3), crate::EntryKind::Normal, cmd),
    // Durable but locally UNPROVEN: a crossing naming a HOSTED source, above commit.
    crate::Entry::new(
      Term::new(1),
      Index::new(4),
      crate::EntryKind::CommitMerge,
      commit_merge_bytes(42, Index::new(6), 1, 2),
    ),
  ]);
  stable.force_state(Term::new(1), Some(1u64), Index::new(3));
  m.restore_group_unchecked(
    1,
    single_node_cfg(1),
    now,
    7,
    SplitSm::default(),
    1,
    &mut log,
    &mut stable,
  )
  .unwrap();
  let mut stores = MapStores(std::collections::BTreeMap::new(), Default::default());
  stores.0.insert(1, (log, stable));
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  assert_eq!(
    m.group(&1).unwrap().merge_park_unresolvable(),
    Some(Index::new(2)),
    "the capped walk sees no crossing yet — the hint stands"
  );

  // The cure blob covers the unproven tail: it must NOT adopt across the unexamined crossing.
  let meta = crate::SnapshotMeta::new(
    Index::new(4),
    Term::new(1),
    crate::conf::ConfState::from_voters(std::vec![1u64]),
  )
  .with_shape_gen(2);
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.handle_message(
      &1,
      now,
      log,
      stable,
      9u64,
      Message::InstallSnapshot(crate::InstallSnapshot::new(
        Term::new(1),
        9u64,
        meta,
        fork_blob(9),
      )),
    )
    .unwrap();
  }
  let tep = m.group(&1).unwrap();
  assert!(
    tep.pending_merge().is_some(),
    "no adopt beyond the proven commit — the unexamined crossing stays protected"
  );
  assert!(
    tep.commit_index() >= Index::new(4),
    "the delivery still advanced commit off the blob's evidence"
  );
  assert!(
    m.contains_group(&42),
    "the hosted crossing's source is untouched"
  );

  // THE BACK-TO-BACK DUPLICATE: commit is raised to the boundary now, but the walk still stops
  // at the old frontier — a commit-keyed gate would admit this duplicate and adopt across the
  // unexamined crossing. The watermark bind refuses until the walk itself has moved.
  let dup = crate::SnapshotMeta::new(
    Index::new(4),
    Term::new(1),
    crate::conf::ConfState::from_voters(std::vec![1u64]),
  )
  .with_shape_gen(2);
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.handle_message(
      &1,
      now,
      log,
      stable,
      9u64,
      Message::InstallSnapshot(crate::InstallSnapshot::new(
        Term::new(1),
        9u64,
        dup,
        fork_blob(9),
      )),
    )
    .unwrap();
  }
  assert!(
    m.group(&1).unwrap().pending_merge().is_some(),
    "a duplicate between cranks must not outrun the walk"
  );
  assert!(m.contains_group(&42));

  // The walk catches up on the next crank and the hint re-gates on the now-visible crossing.
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  assert_eq!(
    m.group(&1).unwrap().merge_park_unresolvable(),
    None,
    "the caught-up walk finds the hosted crossing and withholds"
  );
}

/// The back-to-back freeze race: a source group's append arms its freeze at OBSERVATION, and a
/// cure blob for the target can follow in the same message batch with no resolver crank
/// between — the crank-derived hint is stale exactly then. Receipt-time revalidation at the
/// container's dispatch edge clears it, so the adopt never erases the live thaw obligation the
/// freeze-active predicate protects.
#[test]
fn a_hosted_unadvanced_counterparty_withholds_the_cure() {
  let now = Instant::ORIGIN;
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  // The obligation's counterparty, hosted and (initially) unfrozen.
  m.create_group(42, 0, single_node_cfg(1), now, 8, SplitSm::default())
    .unwrap();
  let cmd = {
    let mut buf = Vec::new();
    Bytes::from_static(b"c").encode(&mut buf);
    Bytes::from(buf)
  };
  let abort = {
    let p = crate::RollbackMergePayload::abort(
      {
        let mut b = Vec::new();
        42u64.encode(&mut b);
        Bytes::from(b)
      },
      1,
      1,
    );
    let mut buf = Vec::new();
    crate::wire::encode_rollback_merge_payload(&p, &mut buf);
    Bytes::from(buf)
  };
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();
  log.force_append(&[
    crate::Entry::new(
      Term::new(1),
      Index::new(1),
      crate::EntryKind::Normal,
      cmd.clone(),
    ),
    crate::Entry::new(
      Term::new(1),
      Index::new(2),
      crate::EntryKind::RollbackMerge,
      abort,
    ),
    crate::Entry::new(
      Term::new(1),
      Index::new(3),
      crate::EntryKind::CommitMerge,
      commit_merge_bytes(41, Index::new(5), 1, 2),
    ),
    crate::Entry::new(Term::new(1), Index::new(4), crate::EntryKind::Normal, cmd),
  ]);
  stable.force_state(Term::new(1), Some(1u64), Index::new(4));
  m.restore_group_unchecked(
    1,
    single_node_cfg(1),
    now,
    7,
    SplitSm::default(),
    1,
    &mut log,
    &mut stable,
  )
  .unwrap();
  assert!(m.group(&1).unwrap().pending_merge().is_some(), "parked");
  assert!(
    m.group(&1).unwrap().owes_live_thaw(),
    "the abort armed the obligation"
  );
  let mut stores = MapStores(std::collections::BTreeMap::new(), Default::default());
  stores.0.insert(1, (log, stable));

  // 42 is hosted at a generation NOT past the owed one: the cure admission is withheld from
  // the very first crank — an unfrozen counterparty at-or-below the owed generation is one
  // delayed PrepareMerge away from freezing at it, and the adopt's boundary clear would erase
  // that freeze's only local thaw driver.
  assert!(m.service_merge_applies(now, &mut stores).is_empty());
  assert_eq!(
    m.group(&1).unwrap().merge_park_unresolvable(),
    None,
    "hosted-unadvanced withholds the hint outright"
  );

  // SAME BATCH, no crank: 42's freeze arms at append-observation…
  let mut slog = VecLog::default();
  let mut sstable = AsyncStable::default();
  stores
    .0
    .insert(42, (VecLog::default(), AsyncStable::default()));
  {
    let (sl, ss) = stores.0.get_mut(&42).unwrap();
    core::mem::swap(sl, &mut slog);
    core::mem::swap(ss, &mut sstable);
  }
  {
    let (sl, ss) = stores.0.get_mut(&42).unwrap();
    m.handle_message(
      &42,
      now,
      sl,
      ss,
      9u64,
      Message::AppendEntries(crate::AppendEntries::new(
        Term::new(1),
        9u64,
        Index::ZERO,
        Term::ZERO,
        std::vec![crate::Entry::new(
          Term::new(1),
          Index::new(1),
          crate::EntryKind::PrepareMerge,
          {
            let p = crate::PrepareMergePayload::new(
              {
                let mut b = Vec::new();
                40u64.encode(&mut b);
                Bytes::from(b)
              },
              1,
            );
            let mut buf = Vec::new();
            crate::wire::encode_prepare_merge_payload(&p, &mut buf);
            Bytes::from(buf)
          },
        )],
        Index::ZERO,
      )),
    )
    .unwrap();
  }
  assert!(
    m.group(&42).unwrap().merge_freeze_active(),
    "the freeze armed at observation"
  );

  // …and the cure blob follows immediately. The receipt edge revalidates and refuses.
  let meta = crate::SnapshotMeta::new(
    Index::new(4),
    Term::new(1),
    crate::conf::ConfState::from_voters(std::vec![1u64]),
  )
  .with_shape_gen(2);
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.handle_message(
      &1,
      now,
      log,
      stable,
      9u64,
      Message::InstallSnapshot(crate::InstallSnapshot::new(
        Term::new(1),
        9u64,
        meta,
        fork_blob(9),
      )),
    )
    .unwrap();
  }
  assert!(
    m.group(&1).unwrap().pending_merge().is_some(),
    "the stale hint was cleared at receipt: no adopt, the obligation survives"
  );
  // A genuinely NEWER blob in the same invalidated state is dropped WHOLE — with the hint
  // gone it would otherwise fall through to the ordinary destructive install, whose
  // completion supersedes the park and clears covered obligations, exactly what the sibling
  // state proved unsafe.
  let newer = crate::SnapshotMeta::new(
    Index::new(6),
    Term::new(1),
    crate::conf::ConfState::from_voters(std::vec![1u64]),
  )
  .with_shape_gen(2);
  {
    let (log, stable) = stores.0.get_mut(&1).unwrap();
    m.handle_message(
      &1,
      now,
      log,
      stable,
      9u64,
      Message::InstallSnapshot(crate::InstallSnapshot::new(
        Term::new(1),
        9u64,
        newer,
        fork_blob(9),
      )),
    )
    .unwrap();
    drain_storage(&mut m, 1, now, log, stable);
    assert!(
      stable.snapshot().is_none(),
      "the invalidated receipt drops the whole message: nothing staged"
    );
  }
  assert!(
    m.group(&1).unwrap().pending_merge().is_some(),
    "no destructive completion can supersede the park the sibling state protects"
  );
  assert!(
    m.group(&1).unwrap().owes_live_thaw(),
    "the live thaw obligation is intact"
  );
}

/// A committed crossing whose source id does not decode as the configured GroupId is
/// committed-corrupt — the resolver's own park-decode fail-stop class. Fail-stopping is the
/// only consistent read: advertising would loop the park through advertise-then-refuse forever
/// (the receipt edge blocks the same bytes fail-closed), shipping whole blobs at a wedge no
/// cure can fix.
#[test]
fn an_undecodable_crossing_id_fail_stops_the_target() {
  let now = Instant::ORIGIN;
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let cmd = {
    let mut buf = Vec::new();
    Bytes::from_static(b"c").encode(&mut buf);
    Bytes::from(buf)
  };
  let corrupt_commit = {
    // A structurally valid payload whose source bytes are an INCOMPLETE varint — length-valid,
    // never decodable as u64.
    let p = crate::CommitMergePayload::new(
      Bytes::from_static(&[0x80]),
      Index::new(6),
      Term::new(1),
      1,
      2,
    );
    let mut buf = Vec::new();
    crate::wire::encode_commit_merge_payload(&p, &mut buf);
    Bytes::from(buf)
  };
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();
  log.force_append(&[
    crate::Entry::new(
      Term::new(1),
      Index::new(1),
      crate::EntryKind::Normal,
      cmd.clone(),
    ),
    crate::Entry::new(
      Term::new(1),
      Index::new(2),
      crate::EntryKind::CommitMerge,
      commit_merge_bytes(41, Index::new(5), 1, 1),
    ),
    crate::Entry::new(
      Term::new(1),
      Index::new(3),
      crate::EntryKind::CommitMerge,
      corrupt_commit,
    ),
    crate::Entry::new(Term::new(1), Index::new(4), crate::EntryKind::Normal, cmd),
  ]);
  stable.force_state(Term::new(1), Some(1u64), Index::new(4));
  m.restore_group_unchecked(
    1,
    single_node_cfg(1),
    now,
    7,
    SplitSm::default(),
    1,
    &mut log,
    &mut stable,
  )
  .unwrap();
  assert!(m.group(&1).unwrap().pending_merge().is_some(), "parked");
  let mut stores = MapStores(std::collections::BTreeMap::new(), Default::default());
  stores.0.insert(1, (log, stable));

  let _ = m.service_merge_applies(now, &mut stores);
  let tep = m.group(&1).unwrap();
  assert!(
    tep.is_poisoned(),
    "committed-corrupt fails stop, never loops"
  );
  assert_eq!(
    tep.merge_park_unresolvable(),
    None,
    "a poisoned park never advertises"
  );
}

/// A structurally malformed committed `CommitMerge` above the park fail-stops the target at the
/// first resolver crank: the parked drain can never reach the entry to poison it, and any
/// answer short of the fail-stop wedges the park forever behind a withheld cure while
/// misreporting an unhosted-source hold.
#[test]
fn a_malformed_crossing_payload_fail_stops_the_target() {
  let now = Instant::ORIGIN;
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let cmd = {
    let mut buf = Vec::new();
    Bytes::from_static(b"c").encode(&mut buf);
    Bytes::from(buf)
  };
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();
  log.force_append(&[
    crate::Entry::new(
      Term::new(1),
      Index::new(1),
      crate::EntryKind::Normal,
      cmd.clone(),
    ),
    crate::Entry::new(
      Term::new(1),
      Index::new(2),
      crate::EntryKind::CommitMerge,
      commit_merge_bytes(41, Index::new(5), 1, 1),
    ),
    // Structurally malformed payload bytes: not a decodable CommitMergePayload at all.
    crate::Entry::new(
      Term::new(1),
      Index::new(3),
      crate::EntryKind::CommitMerge,
      Bytes::from_static(&[0xFF, 0xFF, 0xFF]),
    ),
    crate::Entry::new(Term::new(1), Index::new(4), crate::EntryKind::Normal, cmd),
  ]);
  stable.force_state(Term::new(1), Some(1u64), Index::new(4));
  m.restore_group_unchecked(
    1,
    single_node_cfg(1),
    now,
    7,
    SplitSm::default(),
    1,
    &mut log,
    &mut stable,
  )
  .unwrap();
  assert!(m.group(&1).unwrap().pending_merge().is_some(), "parked");
  let mut stores = MapStores(std::collections::BTreeMap::new(), Default::default());
  stores.0.insert(1, (log, stable));

  let _ = m.service_merge_applies(now, &mut stores);
  let tep = m.group(&1).unwrap();
  assert!(
    tep.is_poisoned(),
    "first-crank fail-stop, never a silent wedge"
  );
  assert_eq!(
    tep.merge_park_unresolvable(),
    None,
    "a poisoned park never advertises"
  );
}

/// A group FOUNDED above zero keeps that generation across a crash that caught no capture — and a
/// replica that never crashed judges the mint it then makes by the same yardstick.
///
/// The founding value is the lineage counter's first term and the ceremony is its only source. It
/// reaches the endpoint through the store-taking door, and until the group's first capture the
/// durable hard state is the only place it lives: a restart in that window that could not read it
/// back would rebuild the counter at zero on this replica alone, mint one past THAT, and hand every
/// replica still standing at the founding value an entry their exact-match apply guard reads as
/// stale — one committed shape entry, two verdicts, permanently divergent state machines. The same
/// mint would also land at or below the relay guard the restore seeds from the durable record, so
/// the child's staged baseline would be folded away with it.
#[test]
fn a_founding_generation_survives_a_crash_without_a_capture() {
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  m.create_group_founded_at(
    7,
    5,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    SplitSm::default(),
    1,
    &log,
    &mut stable,
  )
  .unwrap();
  // Ordinary operation only: a term, a vote, committed entries — and no capture, so the hard
  // state is the founding value's only durable home.
  let d = lead_single_split(&mut m, 7, &mut log, &mut stable);
  commit_one_split(&mut m, 7, d, &mut log, &mut stable);
  assert!(
    stable.snapshot().is_none(),
    "no capture happened, so no snapshot meta carries the founding generation"
  );

  // CRASH: the container dies, the stores survive.
  drop(m);
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  m.restore_group_unchecked(
    7,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    SplitSm::default(),
    // A NEW EPOCH, because this is a new incarnation. The founding one minted under epoch 1 and
    // its completions can still be sitting in the surviving stores; restoring at 1 again would let
    // one of them sort at or above an id this incarnation is about to mint. Reusing the epoch here
    // modelled the very collision the counter exists to prevent.
    2,
    &mut log,
    &mut stable,
  )
  .unwrap();
  assert_eq!(
    m.group(&7).unwrap().shape_gen(),
    5,
    "the ceremony's founding generation came back from the durable hard state"
  );

  // The restored replica leads and mints — one past the founding value, which is exactly what a
  // replica that never crashed would mint and admit.
  let d = lead_single_split(&mut m, 7, &mut log, &mut stable);
  m.propose_split(
    &7,
    d,
    &mut log,
    &stable,
    &200,
    0,
    Bytes::from_static(b"\x01"),
  )
  .unwrap()
  .unwrap();
  m.flush_appends(&7, d, &log, &stable).unwrap();
  while matches!(
    m.handle_storage(&7, d, &mut log, &mut stable),
    Some(StorageProgress::MorePending)
  ) {}
  assert_eq!(
    m.group(&7).unwrap().shape_gen(),
    6,
    "the mint is one past the founding value"
  );
  {
    let fork = m
      .peek_yieldable_fork(&NoHold)
      .expect("the fresh fork is relayed, never folded away beneath the restore's relay guard");
    assert_eq!(((*fork.child()), fork.parent_gen_after()), (200, 6));
  }

  // THE UNIFORM ARM. A replica of the same incarnation that never crashed — still holding the
  // founding value it was created with — applies that very entry, and partitions its state
  // machine exactly as the restored one did.
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut peer: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let (mut plog, mut pstable) = (VecLog::default(), AsyncStable::default());
  peer
    .create_group_founded_at(
      7,
      5,
      cfg,
      Instant::ORIGIN,
      43,
      SplitSm::default(),
      1,
      &plog,
      &mut pstable,
    )
    .unwrap();
  for idx in 1..=3 {
    follower_commit_next(&mut peer, &mut plog, &mut pstable, idx);
  }
  let units_before = peer.group(&7).unwrap().state_machine().units;
  follower_split_next(&mut peer, &mut plog, &mut pstable, 4, 200, 6, 2);
  assert_eq!(
    peer.group(&7).unwrap().shape_gen(),
    6,
    "the peer admitted the same mint at the same generation"
  );
  assert_eq!(
    peer.group(&7).unwrap().state_machine().units,
    units_before - 2,
    "and PARTITIONED its state machine — the arm the restored replica took"
  );
  assert_eq!(peer.group(&7).unwrap().staged_forks().count(), 1);
}

/// The founding door REFUSES storage that already holds an incarnation's state. It writes the
/// founding stamp and builds a term-0 endpoint, so reused storage would carry an old term, vote,
/// commit, snapshot, lineage token — or another incarnation's founding generation — into a fresh
/// incarnation and stamp over it. The precondition is enforced through the same predicate the fork
/// door uses, and refusal precedes every store write.
#[test]
fn the_founding_door_refuses_storage_that_is_not_virgin() {
  // A store the door itself founded is no longer virgin: the stamp it wrote is state.
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let (log, mut stable) = (VecLog::default(), AsyncStable::default());
  m.create_group_founded_at(
    7,
    4,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    SplitSm::default(),
    1,
    &log,
    &mut stable,
  )
  .unwrap();
  let mut other: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  assert_eq!(
    other.create_group_founded_at(
      8,
      4,
      single_node_cfg(1),
      Instant::ORIGIN,
      42,
      SplitSm::default(),
      1,
      &log,
      &mut stable
    ),
    Err(CreateGroupError::StorageInUse),
    "a store already carrying a founding generation is refused"
  );
  assert!(other.is_empty(), "nothing admitted over used storage");

  // A VOTED-BUT-EMPTY store is refused too. Hard state is written on a vote, so its presence is
  // not evidence of a log — but it IS evidence that some incarnation already used this storage,
  // and a fresh term-0 endpoint founded over it would inherit that vote.
  let (voted_log, mut voted_stable) = (VecLog::default(), AsyncStable::default());
  voted_stable.submit_write(
    crate::OpId::new(1),
    crate::HardState::initial()
      .with_term(Term::new(3))
      .with_vote(Some(9u64)),
  );
  let mut m3: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  assert_eq!(
    m3.create_group_founded_at(
      9,
      4,
      single_node_cfg(1),
      Instant::ORIGIN,
      42,
      SplitSm::default(),
      1,
      &voted_log,
      &mut voted_stable
    ),
    Err(CreateGroupError::StorageInUse),
    "a residual hard state is refused, log or no log"
  );

  // THE FRESH PATH IS UNCHANGED, and the stamp is EXACT: a virgin pair founds, and the record the
  // door leaves differs from the initial hard state in the founding generation alone.
  let (fresh_log, mut fresh_stable) = (VecLog::default(), AsyncStable::default());
  let mut m4: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  m4.create_group_founded_at(
    10,
    4,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    SplitSm::default(),
    1,
    &fresh_log,
    &mut fresh_stable,
  )
  .expect("a virgin pair founds");
  assert_eq!(
    fresh_stable.hard_state(),
    crate::HardState::initial().with_founding_gen(4),
    "the stamp moved the founding generation and nothing else"
  );
}

/// A restore whose stores hold state but no INCARNATION refuses typed.
///
/// The founding generation lives in the hard state until the first capture, and the two stores have
/// no cross-store durability ordering — so a crash can leave durable log content beside a hard state
/// that never landed. Recovering there would rebuild the lineage counter at zero on this replica
/// while its peers stand at the founding value, and one committed shape entry would then be judged
/// by two different yardsticks. Nothing observable is lost by refusing: entries that survived
/// without a durable term were never acked.
#[test]
fn a_restore_refuses_stores_that_hold_state_but_no_incarnation() {
  let cmd = {
    let mut b = Vec::new();
    Bytes::from_static(b"c").encode(&mut b);
    Bytes::from(b)
  };
  // The window: log content survived, no stable write did.
  let mut log = VecLog::default();
  log.force_append(&[crate::Entry::new(
    Term::new(1),
    Index::new(1),
    crate::EntryKind::Normal,
    cmd,
  )]);
  let stable = AsyncStable::default();
  assert_eq!(stable.hard_state(), crate::HardState::initial());
  assert_eq!(
    crate::validate_restore(5, 0, 5, &log, &stable),
    Err(CreateGroupError::IncarnationUnrecoverable { record: 5 }),
    "an id the record knows above zero cannot be rebuilt from stores that name no incarnation"
  );

  // THE GEN-0 WORLD IS UNTOUCHED: a record of zero never reaches the arm.
  assert_eq!(crate::validate_restore(0, 0, 0, &log, &stable), Ok(()));

  // A TORN FORK CHILD — its baseline meta lost, its record standing at the founding generation —
  // refuses here rather than booting as incarnation zero. The child is re-delivered, not adopted.
  assert_eq!(
    crate::validate_restore(3, 0, 3, &log, &stable),
    Err(CreateGroupError::IncarnationUnrecoverable { record: 3 })
  );

  // NoStoredState still owns the EMPTY case, and the two refusals partition cleanly.
  let empty_log = VecLog::default();
  assert_eq!(
    crate::validate_restore(5, 0, 5, &empty_log, &stable),
    Err(CreateGroupError::NoStoredState),
    "nothing survived at all is the other refusal, not this one"
  );

  // A surviving hard state carries the incarnation, so the arm stands down.
  let mut recorded = AsyncStable::default();
  recorded.submit_write(
    crate::OpId::new(1),
    crate::HardState::initial()
      .with_term(Term::new(1))
      .with_founding_gen(5),
  );
  assert_eq!(crate::validate_restore(5, 0, 5, &log, &recorded), Ok(()));
}

/// THE UNCHECKED DOOR'S CONTRACT, pinned so a rename cannot quietly become a validated door and a
/// validated door cannot quietly become this one.
///
/// The container holds no catalog generation, no floor, and no lineage record — floors are a
/// coordinator-and-driver concern and no container door takes a `FloorStore` — so it cannot make
/// the incarnation judgements the coordinators make. It admits the two shapes they refuse, and the
/// name is what says so. The caller composing this door owes those checks; `validate_restore` is
/// the public seam that performs them.
#[test]
fn the_unchecked_restore_door_admits_what_a_validated_one_refuses() {
  let cmd = {
    let mut b = Vec::new();
    Bytes::from_static(b"c").encode(&mut b);
    Bytes::from(b)
  };

  // SHAPE ONE — stores that hold nothing. A validated door refuses a KNOWN id here.
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let (mut empty_log, mut empty_stable) = (VecLog::default(), AsyncStable::default());
  assert_eq!(
    crate::validate_restore(5, 0, 5, &empty_log, &empty_stable),
    Err(CreateGroupError::NoStoredState),
    "the validated seam refuses it"
  );
  m.restore_group_unchecked(
    7,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    SplitSm::default(),
    1,
    &mut empty_log,
    &mut empty_stable,
  )
  .expect("the unchecked door admits it — it has no record to know the id by");
  assert_eq!(m.group(&7).unwrap().shape_gen(), 0);

  // SHAPE TWO — state in the stores, no incarnation in them: the shape a lost founding stamp
  // leaves. A validated door refuses; this one recovers at zero, which is precisely why the caller
  // owes the check.
  let mut log = VecLog::default();
  log.force_append(&[crate::Entry::new(
    Term::new(1),
    Index::new(1),
    crate::EntryKind::Normal,
    cmd,
  )]);
  let mut stable = AsyncStable::default();
  assert_eq!(
    crate::validate_restore(5, 0, 5, &log, &stable),
    Err(CreateGroupError::IncarnationUnrecoverable { record: 5 }),
    "the validated seam refuses it"
  );
  let mut m2: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  m2.restore_group_unchecked(
    7,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    SplitSm::default(),
    1,
    &mut log,
    &mut stable,
  )
  .expect("the unchecked door admits it too");
  assert_eq!(
    m2.group(&7).unwrap().shape_gen(),
    0,
    "recovered at zero — the incarnation the stores could not name"
  );

  // The STRUCTURAL checks it does keep still fire.
  assert_eq!(
    m2.restore_group_unchecked(
      7,
      single_node_cfg(1),
      Instant::ORIGIN,
      42,
      SplitSm::default(),
      1,
      &mut log,
      &mut stable
    ),
    Err(CreateGroupError::Exists),
    "structural admission is still the container's"
  );
}

/// A hard state that SURVIVED carries the founding generation, so a zero there is exact and the
/// restore refusal deliberately stands down — the counter heals by replaying the moves the record
/// counts. The refusal fires on the shape where the hard state did NOT survive; these two readings
/// are what make the arm's predicate honest rather than merely conservative.
#[test]
fn a_surviving_hard_state_names_its_incarnation_and_is_admitted() {
  let cmd = {
    let mut b = Vec::new();
    Bytes::from_static(b"c").encode(&mut b);
    Bytes::from(b)
  };
  let mut log = VecLog::default();
  log.force_append(&[crate::Entry::new(
    Term::new(1),
    Index::new(1),
    crate::EntryKind::Normal,
    cmd,
  )]);

  // Founded at zero and since reshaped: the record counts applied moves, the hard state honestly
  // reads zero, and replay is what heals the counter.
  let mut moved = AsyncStable::default();
  moved.submit_write(
    crate::OpId::new(1),
    crate::HardState::initial().with_term(Term::new(4)),
  );
  assert_eq!(moved.hard_state().founding_gen(), 0);
  assert_eq!(
    crate::validate_restore(3, 0, 3, &log, &moved),
    Ok(()),
    "a founded-at-zero group that reshaped is recoverable, and the arm must not brick it"
  );

  // Founded above zero: the surviving hard state carries it, so nothing is ambiguous.
  let mut founded = AsyncStable::default();
  founded.submit_write(
    crate::OpId::new(1),
    crate::HardState::initial()
      .with_term(Term::new(4))
      .with_founding_gen(3),
  );
  assert_eq!(crate::validate_restore(3, 0, 3, &log, &founded), Ok(()));
}

/// A COMPLETION LEFT OVER FROM A PRIOR INCARNATION CREDITS NOTHING HERE.
///
/// The founding door writes into the store it is handed and builds a fresh endpoint, so without an
/// op-id floor that endpoint would mint from epoch zero — exactly where a prior incarnation of the
/// same id minted. A stale `Wrote` landing afterwards would then alias this incarnation's own
/// write and release what that write gates: a vote grant, or a campaign's `become_leader`, on
/// durability it does not have. The epoch is what makes the stale id sort below everything this
/// incarnation mints.
#[test]
fn a_founding_incarnation_mints_above_a_prior_incarnations_completions() {
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let (log, mut stable) = (VecLog::default(), AsyncStable::default());
  m.create_group_founded_at(
    7,
    4,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    SplitSm::default(),
    9,
    &log,
    &mut stable,
  )
  .expect("a virgin pair founds at epoch 9");

  // Everything this incarnation submits sorts strictly above every id epoch 8 and below could
  // mint — the stamp included, which is this endpoint's very first write.
  let stale = crate::OpId::first_of_epoch(8);
  let mine = m.group_mut(&7).unwrap().mint_op_id_for_test();
  assert_eq!(
    mine.epoch(),
    9,
    "this incarnation mints in the epoch the door was given, from its very first id"
  );
  assert!(
    stale < mine,
    "a prior incarnation's completion sorts below it: {stale:?} vs {mine:?}"
  );
}

/// Epoch zero is refused at both store-writing doors. It is where an unseeded endpoint starts, so
/// founding or forking there hands a prior incarnation's leftover completions ids that can alias
/// this one's — the floor is enforced rather than documented because these doors can see the store.
#[test]
fn the_store_writing_doors_refuse_boot_epoch_zero() {
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let (log, mut stable) = (VecLog::default(), AsyncStable::default());
  assert_eq!(
    m.create_group_founded_at(
      7,
      4,
      single_node_cfg(1),
      Instant::ORIGIN,
      42,
      SplitSm::default(),
      0,
      &log,
      &mut stable
    ),
    Err(CreateGroupError::InvalidBootEpoch),
    "founding at epoch zero has no separation from a prior incarnation"
  );
  assert!(m.is_empty(), "nothing admitted at the floor");
  assert_eq!(
    stable.hard_state(),
    crate::HardState::initial(),
    "and the refusal precedes every store write"
  );

  // The fork door's identical floor, unchanged.
  let (mut flog, mut fstable) = (VecLog::default(), AsyncStable::default());
  assert_eq!(
    m.create_group_from_fork(
      8,
      0,
      single_node_cfg(1),
      Instant::ORIGIN,
      42,
      SplitSm::default(),
      fork_blob(3),
      None,
      0,
      &mut flog,
      &mut fstable
    ),
    Err(CreateGroupError::InvalidBootEpoch)
  );
}

/// NO REMOVAL CAN FORGE THE TERMINAL SENTINEL. A floor of `MERGED_FLOOR` is read as a GLOBAL proof
/// that a lineage was absorbed away — the thaw witness's own global-proof leg rests on it — so an
/// ordinary local removal producing one would clear a live thaw obligation on every replica and
/// strand a still-frozen source. The fence needs a value strictly above the ceiling it fences, and
/// at the top of the space there is none to spare, so the top TWO generations are reserved: the
/// sentinel, and the headroom its fence needs.
#[test]
fn a_removal_fence_never_reaches_the_reserved_terminal() {
  // The boundary is one value, shared by every gate that judges a generation.
  assert_eq!(crate::HIGHEST_WORKING_GENERATION, MERGED_FLOOR - 1);
  assert_eq!(
    next_lineage(crate::HIGHEST_WORKING_GENERATION - 1),
    None,
    "the mint stops short of the reserved band"
  );
  assert_eq!(
    next_lineage(crate::HIGHEST_WORKING_GENERATION - 2),
    Some(crate::HIGHEST_WORKING_GENERATION - 1),
    "and reaches the highest working generation"
  );
  assert!(!crate::floor_admits(0, crate::HIGHEST_WORKING_GENERATION));
  assert!(crate::floor_admits(
    0,
    crate::HIGHEST_WORKING_GENERATION - 1
  ));

  // The admission doors agree with the mint: the reserved band is refused whatever floor applies.
  let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
  let (log, mut stable) = (VecLog::default(), AsyncStable::default());
  for reserved in [crate::HIGHEST_WORKING_GENERATION, MERGED_FLOOR] {
    assert_eq!(
      m.create_group_founded_at(
        7,
        reserved,
        single_node_cfg(1),
        Instant::ORIGIN,
        42,
        SplitSm::default(),
        1,
        &log,
        &mut stable
      ),
      Err(CreateGroupError::ReservedGeneration),
      "a reserved generation is refused at admission: {reserved}"
    );
  }

  // The fold over the highest generation an id can actually reach lands on the headroom, and the
  // headroom is distinguishable from the terminal by the reader that matters.
  let mut engine: GroupEngine<u64, u64> = GroupEngine::new();
  engine.set_group_gen(&7, crate::HIGHEST_WORKING_GENERATION - 1);
  let fence = engine.removal_floor(&7);
  assert_eq!(fence, crate::HIGHEST_WORKING_GENERATION);
  assert_ne!(
    fence, MERGED_FLOOR,
    "the fence is an ordinary high floor, not a global absorbed-away verdict"
  );
  assert!(
    !crate::floor_admits(fence, crate::HIGHEST_WORKING_GENERATION - 1),
    "and it still fences the incarnation it retired"
  );
}

/// One case PER GUARDED FIELD: the named field carries the reserved value and EVERY other
/// generation in the payload is valid, so each case can only be caught by that field's operand.
///
/// A fixture that corrupts one field per kind cannot tell a guard that reads all of them from one
/// that reads a single operand — removing the others would leave it green. `valid` is the
/// successor the harness's live counter expects, so the non-corrupted fields never trip the
/// stale-mint arm and the fail-stop is attributable to the band alone.
fn reserved_case(case: &'static str, index: u64, reserved: u64, valid: u64) -> crate::Entry {
  let mut peer = Vec::new();
  Data::encode(&200u64, &mut peer);
  let peer = Bytes::from(peer);
  let (kind, data) = match case {
    "split/parent_gen_after" => (
      crate::EntryKind::Split,
      split_entry_bytes(200, 0, reserved, 1),
    ),
    "split/child_gen" => (
      crate::EntryKind::Split,
      split_entry_bytes(200, reserved, valid, 1),
    ),
    "prepare/source_gen_after" => {
      let p = crate::PrepareMergePayload::new(peer, reserved);
      let mut b = Vec::new();
      crate::wire::encode_prepare_merge_payload(&p, &mut b);
      (crate::EntryKind::PrepareMerge, Bytes::from(b))
    }
    "commit/source_gen_after" | "commit/target_gen_after" => {
      let (src, tgt) = if case.ends_with("source_gen_after") {
        (reserved, valid)
      } else {
        (1, reserved)
      };
      let p = crate::CommitMergePayload::new(peer, Index::new(2), Term::new(1), src, tgt);
      let mut b = Vec::new();
      crate::wire::encode_commit_merge_payload(&p, &mut b);
      (crate::EntryKind::CommitMerge, Bytes::from(b))
    }
    "rollback-abort/source_gen_after" | "rollback-abort/target_gen_after" => {
      let (src, tgt) = if case.ends_with("source_gen_after") {
        (reserved, valid)
      } else {
        (1, reserved)
      };
      let p = crate::RollbackMergePayload::abort(peer, src, tgt);
      let mut b = Vec::new();
      crate::wire::encode_rollback_merge_payload(&p, &mut b);
      (crate::EntryKind::RollbackMerge, Bytes::from(b))
    }
    "rollback-unfreeze/source_gen_after" => {
      let p = crate::RollbackMergePayload::unfreeze(reserved);
      let mut b = Vec::new();
      crate::wire::encode_rollback_merge_payload(&p, &mut b);
      (crate::EntryKind::RollbackMerge, Bytes::from(b))
    }
    other => panic!("unknown reserved case: {other}"),
  };
  crate::Entry::new(Term::new(1), Index::new(index), kind, data)
}

/// Every generation field the apply guard claims, one case each — including the fields a
/// kind-shaped fixture leaves valid by accident. The `split/child_gen` case is the sharpest: a
/// VALID parent mint beside a reserved child would partition the parent's state machine, and child
/// admission would then refuse the reserved generation, discarding the fork's only child half.
const RESERVED_CASES: [&str; 8] = [
  "split/parent_gen_after",
  "split/child_gen",
  "prepare/source_gen_after",
  "commit/source_gen_after",
  "commit/target_gen_after",
  "rollback-abort/source_gen_after",
  "rollback-abort/target_gen_after",
  "rollback-unfreeze/source_gen_after",
];

/// A committed shape entry naming a generation in the RESERVED band is a FAIL-STOP, for every
/// generation-bearing field, live and on restart replay alike.
///
/// A committed entry is agreed state, so the verdict must be identical on every replica — and it
/// is: the inputs are the entry's own bytes, so all replicas halt together at the same index. That
/// is the doctrine the other impossible-entry arms already follow (an undecodable payload, a
/// committed split against an FSM that cannot split) and deliberately not the stale-mint no-op: a
/// stale mint is a legal value that lost a race, a reserved generation is one no conforming
/// replica can produce, so skipping it would carry a log known to be corrupt forward as sound.
#[test]
fn a_committed_shape_entry_in_the_reserved_band_fail_stops_on_live_apply() {
  use crate::{HIGHEST_WORKING_GENERATION, MERGED_FLOOR};

  let mut unguarded: Vec<(&str, u64)> = Vec::new();
  for reserved in [HIGHEST_WORKING_GENERATION, MERGED_FLOOR] {
    for case in RESERVED_CASES {
      let (mut m, mut log, mut stable) = host_with_staged_fork(300);
      let before = m.group(&7).unwrap().state_machine().units;
      // The staged split at index 4 left the counter at 1, so 2 is the successor every
      // non-corrupted field must carry to clear the stale-mint arm.
      m.handle_message(
        &7,
        Instant::ORIGIN,
        &mut log,
        &mut stable,
        2u64,
        Message::AppendEntries(crate::AppendEntries::new(
          Term::new(1),
          2u64,
          Index::new(4),
          Term::new(1),
          std::vec![reserved_case(case, 5, reserved, 2)],
          Index::new(5),
        )),
      )
      .unwrap();
      while matches!(
        m.handle_storage(&7, Instant::ORIGIN, &mut log, &mut stable),
        Some(StorageProgress::MorePending)
      ) {}

      let ep = m.group(&7).unwrap();
      // COLLECTED, not asserted in place: the guard claims every field below, so a red-proof must
      // show every field failing — a loop that stops at the first would look identical whether one
      // operand is read or all of them.
      if ep.poison_reason() != Some(crate::PoisonReason::ReservedShapeGen)
        || ep.shape_gen() >= HIGHEST_WORKING_GENERATION
        || ep.state_machine().units != before
      {
        unguarded.push((case, reserved));
      }
    }
  }
  assert!(
    unguarded.is_empty(),
    "these generation fields did not fail-stop on a reserved value: {unguarded:?}"
  );
}

#[test]
fn a_reserved_shape_entry_fail_stops_on_restart_replay_too() {
  use crate::{HIGHEST_WORKING_GENERATION, MERGED_FLOOR};

  let mut unguarded: Vec<(&str, u64)> = Vec::new();
  for reserved in [HIGHEST_WORKING_GENERATION, MERGED_FLOOR] {
    for case in RESERVED_CASES {
      // A durable log a pre-reservation build could have written, replayed from disk. Replay
      // starts the counter at 0, so 1 is the successor the valid fields carry.
      let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
      let cmd = {
        let mut b = Vec::new();
        Bytes::from_static(b"c").encode(&mut b);
        Bytes::from(b)
      };
      log.force_append(&[
        crate::Entry::new(Term::new(1), Index::new(1), crate::EntryKind::Normal, cmd),
        reserved_case(case, 2, reserved, 1),
      ]);
      stable.force_state(Term::new(1), Some(1u64), Index::new(2));

      let mut m: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
      m.restore_group_unchecked(
        7,
        single_node_cfg(1),
        Instant::ORIGIN,
        42,
        SplitSm::default(),
        1,
        &mut log,
        &mut stable,
      )
      .unwrap();
      let ep = m.group(&7).unwrap();
      if ep.poison_reason() != Some(crate::PoisonReason::ReservedShapeGen)
        || ep.shape_gen() >= HIGHEST_WORKING_GENERATION
      {
        unguarded.push((case, reserved));
      }
    }
  }
  assert!(
    unguarded.is_empty(),
    "these fields replayed a reserved generation without fail-stopping: {unguarded:?}"
  );
}
/// The MINT side of the same boundary: a caller's reserved `child_gen` is refused at propose, so
/// no conforming replica can ever append what the apply guard above defends against.
#[test]
fn propose_split_refuses_a_reserved_child_generation() {
  use crate::{HIGHEST_WORKING_GENERATION, MERGED_FLOOR};

  for reserved in [HIGHEST_WORKING_GENERATION, MERGED_FLOOR] {
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
    let before = log.last_index();
    assert!(
      matches!(
        m.propose_split(
          &7,
          d,
          &mut log,
          &stable,
          &200,
          reserved,
          Bytes::from_static(b"\x01")
        ),
        Some(Err(SplitError::ReservedGeneration))
      ),
      "a reserved child generation {reserved} must be refused at propose"
    );
    assert_eq!(
      log.last_index(),
      before,
      "and nothing was appended at {reserved}"
    );
  }
}

/// STORES MUST CLEAR THE FLOOR THEMSELVES — the caller's claim is not evidence about them.
///
/// The floor gate judges `generation`; the doors deliberately never install it, so the live counter
/// comes from the stores. A claim at the floor over stores founded below it therefore admits the
/// fenced incarnation AND recovers it: it campaigns, serves retired state, and judges
/// current-generation traffic by a dead incarnation's lineage guards.
#[test]
fn a_restore_refuses_stores_whose_lineage_cannot_reach_the_floor() {
  let cmd = {
    let mut b = Vec::new();
    Bytes::from_static(b"c").encode(&mut b);
    Bytes::from(b)
  };
  let mut log = VecLog::default();
  log.force_append(&[crate::Entry::new(
    Term::new(1),
    Index::new(1),
    crate::EntryKind::Normal,
    cmd,
  )]);
  // Coherent stores whose every lineage reading is zero: a term, no founding, no meta.
  let mut fenced = AsyncStable::default();
  fenced.submit_write(
    crate::OpId::new(1),
    crate::HardState::initial().with_term(Term::new(4)),
  );

  // The claim clears the floor; the evidence does not.
  assert_eq!(
    crate::validate_restore(0, 2, 2, &log, &fenced),
    Err(CreateGroupError::StoredStateBelowFloor {
      floor: 2,
      recoverable: 0
    }),
    "an at-floor claim over below-floor stores resurrects the fenced incarnation"
  );

  // EACH of the three evidence readings alone lifts it to the floor — the bound is their max, so
  // a legitimate store is admitted by whichever one it happens to carry.
  let mut founded = AsyncStable::default();
  founded.submit_write(
    crate::OpId::new(1),
    crate::HardState::initial()
      .with_term(Term::new(4))
      .with_founding_gen(2),
  );
  assert_eq!(crate::validate_restore(0, 2, 2, &log, &founded), Ok(()));

  let mut captured = AsyncStable::default();
  captured.submit_write(
    crate::OpId::new(1),
    crate::HardState::initial().with_term(Term::new(4)),
  );
  captured.force_snapshot(
    crate::SnapshotMeta::new(
      Index::new(1),
      Term::new(1),
      crate::ConfState::from_voters(std::vec![1u64]),
    )
    .with_shape_gen(2),
    Bytes::from_static(b"\x00"),
  );
  assert_eq!(crate::validate_restore(0, 2, 2, &log, &captured), Ok(()));
  // The record is the driver's mirror of applied lineage moves, so it too satisfies the bound.
  assert_eq!(crate::validate_restore(2, 2, 2, &log, &fenced), Ok(()));

  // THE GEN-0 WORLD NEVER REACHES THE ARM: no floor, nothing to clear.
  assert_eq!(crate::validate_restore(0, 0, 0, &log, &fenced), Ok(()));
}

/// A POISONED CHILD IS NOT A MATERIALIZATION. Token equality proves the hosted group was born from
/// this fork's baseline; it does not prove the host can serve it. Consuming the parent's staged
/// blob against an inert child destroys the partition's only clean derivation and leaves nothing
/// usable behind, so the fork PARKS on the fork-hold default and re-materializes once the bad child
/// is removed.
#[test]
fn a_poisoned_child_does_not_resolve_its_parents_fork_redundant() {
  let (mut m, _log, _stable) = host_with_staged_fork(200);
  let token = staged_fork_id(&m, 7);
  m.create_group(
    200,
    0,
    single_node_cfg(1),
    Instant::ORIGIN,
    44,
    SplitSm::default(),
  )
  .unwrap();
  m.group_mut(&200).unwrap().seed_fork_id_for_test(token);

  // With a HEALTHY twin carrying the token the fork resolves redundant and is consumed.
  {
    let mut healthy = m.group(&7).unwrap().staged_forks().count();
    assert_eq!(healthy, 1, "one fork staged");
    assert!(
      m.peek_yieldable_fork(&NoHold).is_none(),
      "resolved, not yielded"
    );
    healthy = m.group(&7).unwrap().staged_forks().count();
    assert_eq!(healthy, 0, "the healthy twin consumed the parent's copy");
  }

  // Now the same shape with a POISONED twin: the blob must survive.
  let (mut m, _log2, _stable2) = host_with_staged_fork(200);
  let token = staged_fork_id(&m, 7);
  m.create_group(
    200,
    0,
    single_node_cfg(1),
    Instant::ORIGIN,
    44,
    SplitSm::default(),
  )
  .unwrap();
  m.group_mut(&200).unwrap().seed_fork_id_for_test(token);
  m.group_mut(&200)
    .unwrap()
    .poison(crate::PoisonReason::ReservedShapeGen);

  assert!(
    m.peek_yieldable_fork(&NoHold).is_none(),
    "the id is occupied, so the fork does not yield either way"
  );
  assert_eq!(
    m.group(&7).unwrap().staged_forks().count(),
    1,
    "but the staged blob SURVIVES: an inert child certifies no materialization"
  );
  assert!(
    m.group(&7).unwrap().fork_obligations_standing(),
    "and the parent still owes the partition, so nothing lifted its barrier"
  );
}

/// AN UNADOPTED SNAPSHOT IS NOT EVIDENCE. The blob reached the slot but the install never ran, so
/// the hard state carries no token while the meta does and the log was never re-baselined at its
/// boundary — the leftover shape a restart deliberately ignores. Crediting its `shape_gen` would
/// clear a floor with a boundary the boot then discards, admitting a fenced incarnation that comes
/// up blank and, on a single voter, can campaign and serve below the fence.
#[test]
fn an_unadopted_snapshot_does_not_clear_the_floor() {
  let token = crate::ForkId::new(
    Bytes::from_static(b"p"),
    1,
    Index::new(1),
    Term::new(2),
    Bytes::from_static(b"c"),
    0,
  );
  let meta = || {
    crate::SnapshotMeta::new(
      Index::new(1),
      Term::new(2),
      crate::ConfState::from_voters(std::vec![1u64]),
    )
    .with_shape_gen(2)
    .with_fork_id(token.clone())
  };

  // A VIRGIN log: the blob is durable, the destructive re-baseline never ran.
  let log = VecLog::default();
  let mut stable = AsyncStable::default();
  stable.force_hard_state(crate::HardState::initial().with_term(Term::new(4)));
  stable.force_snapshot(meta(), Bytes::from_static(b"\x00"));
  assert_eq!(
    crate::validate_restore(0, 2, 2, &log, &stable),
    Err(CreateGroupError::StoredStateBelowFloor {
      floor: 2,
      recoverable: 0
    }),
    "an ignored slot must not raise the recoverable lineage"
  );

  // THE ADOPTED TWIN, one field apart: the hard state vouches for the same lineage, so the restart
  // WILL boot on this slot and its generation is real evidence.
  let mut adopted = AsyncStable::default();
  adopted.force_hard_state(
    crate::HardState::initial()
      .with_term(Term::new(4))
      .with_lineage(Some(token.clone())),
  );
  adopted.force_snapshot(meta(), Bytes::from_static(b"\x00"));
  assert_eq!(
    crate::validate_restore(0, 2, 2, &log, &adopted),
    Ok(()),
    "an adopted slot's generation clears the floor"
  );

  // And the OTHER adoption leg: a token-less hard state adopts a token-bearing meta only when the
  // log is baselined AT its boundary — the install having already run, only the stamp missing.
  let mut baselined = VecLog::default();
  baselined.force_append(&[crate::Entry::new(
    Term::new(2),
    Index::new(1),
    crate::EntryKind::Empty,
    Bytes::new(),
  )]);
  baselined.compact(Index::new(1));
  let mut tokenless = AsyncStable::default();
  tokenless.force_hard_state(crate::HardState::initial().with_term(Term::new(4)));
  tokenless.force_snapshot(meta(), Bytes::from_static(b"\x00"));
  assert_eq!(
    crate::validate_restore(0, 2, 2, &baselined, &tokenless),
    Ok(()),
    "a baselined-at-boundary log adopts the slot, so it counts"
  );
}

/// A CHILD POISONED AFTER ADMISSION CERTIFIES NOTHING AT REMOVAL EITHER. Removal reads the removed
/// endpoint's provenance token as proof the parent's staged fork descends from the incarnation being
/// torn down, and abandons that fork — destroying the partition's only clean copy. An inert replica
/// cannot back that claim, so the token is gone before removal ever asks, the fork survives, its
/// replay guard does not advance, and the id re-materializes once it is free again.
#[test]
fn a_poisoned_child_does_not_let_its_removal_abandon_the_parents_fork() {
  let (mut m, _log, _stable) = host_with_staged_fork(200);
  let token = staged_fork_id(&m, 7);
  m.create_group(
    200,
    0,
    single_node_cfg(1),
    Instant::ORIGIN,
    44,
    SplitSm::default(),
  )
  .unwrap();
  m.group_mut(&200).unwrap().seed_fork_id_for_test(token);
  // Poisoned AFTER admission — the late-replay/final-scan shape, where the token is already held.
  m.group_mut(&200)
    .unwrap()
    .poison(crate::PoisonReason::ReservedShapeGen);

  m.remove_group(&200, &mut empty_stores()).unwrap();

  assert_eq!(
    m.group(&7).unwrap().staged_forks().count(),
    1,
    "the parent's staged fork survives the removal of an inert child"
  );
  assert!(
    m.group(&7).unwrap().fork_obligations_standing(),
    "and the parent still owes the partition"
  );
  assert_eq!(
    m.poll_relay_guard_advance(),
    None,
    "no guard advance: nothing was consumed, so a replay must re-stage it"
  );
  assert_eq!(
    m.poll_split_refusal(),
    None,
    "and nothing was refused — this is a deferral, not a verdict about the fork"
  );

  // The id is free again, so the partition re-materializes from the copy that survived.
  let fork = m
    .peek_yieldable_fork(&NoHold)
    .expect("the fork re-materializes once the bad child is gone");
  assert_eq!(*fork.child(), 200);
}

/// A CHILD POISONED BY A RESTART LOG-READ FAULT certifies nothing at removal.
///
/// The child's stores DO carry the fork's provenance token — the positive control below proves it —
/// so this is the shape where a removal could read that certificate as proof the parent's staged
/// fork descended from it and abandon the fork, destroying the partition's last clean derivation.
///
/// SCOPE, stated because the fixture cannot narrow it: the fault lands in one of restart's
/// construction-time log scans, which are ungated and read the whole log, so `scan_freeze_pending`'s
/// own post-construction arm cannot be reached in isolation through this seam — any faulting index
/// is hit by an earlier scan first. That arm's routing through `poison` is swept and correct, but it
/// is this test's neighbour rather than its subject; isolating it needs a fault seam scoped to a
/// range rather than to any read at an index.
#[test]
fn a_child_poisoned_during_restart_does_not_let_its_removal_abandon_the_parents_fork() {
  let (mut m, _plog, _pstable) = host_with_staged_fork(200);
  let token = staged_fork_id(&m, 7);

  // The child's durable state: a snapshot whose meta carries THIS fork's token, vouched for by the
  // hard state so the restart adopts it and the token reaches the endpoint.
  let cmd = {
    let mut b = Vec::new();
    Bytes::from_static(b"c").encode(&mut b);
    Bytes::from(b)
  };
  let mut clog = crate::testkit::FailTermLog::default();
  let mut cstable = AsyncStable::default();
  cstable.force_hard_state(
    crate::HardState::initial()
      .with_term(Term::new(2))
      .with_lineage(Some(token.clone())),
  );
  cstable.force_snapshot(
    crate::SnapshotMeta::new(
      Index::new(1),
      Term::new(2),
      crate::ConfState::from_voters(std::vec![1u64]),
    )
    .with_fork_id(token.clone()),
    fork_blob(2),
  );
  // A suffix ABOVE the snapshot boundary, and the scan of it faults. The scan reads from
  // `applied + 1`, so the fault index is 2 — the boundary itself is never the scanned range.
  clog.force_append(&[
    crate::Entry::new(
      Term::new(2),
      Index::new(1),
      crate::EntryKind::Normal,
      cmd.clone(),
    ),
    crate::Entry::new(
      Term::new(2),
      Index::new(2),
      crate::EntryKind::Normal,
      cmd.clone(),
    ),
  ]);
  clog.fail_entries_at(Some(Index::new(2)));

  // POSITIVE CONTROL, first: the very same stores WITHOUT the fault must restore a child that
  // HOLDS the token. Without this the `is_none()` assertion below passes for a store that never
  // adopted a token at all, and the whole regression is vacuous.
  {
    let mut healthy_log = crate::testkit::FailTermLog::default();
    healthy_log.force_append(&[
      crate::Entry::new(
        Term::new(2),
        Index::new(1),
        crate::EntryKind::Normal,
        cmd.clone(),
      ),
      crate::Entry::new(
        Term::new(2),
        Index::new(2),
        crate::EntryKind::Normal,
        cmd.clone(),
      ),
    ]);
    let mut healthy_stable = AsyncStable::default();
    healthy_stable.force_hard_state(
      crate::HardState::initial()
        .with_term(Term::new(2))
        .with_lineage(Some(token.clone())),
    );
    healthy_stable.force_snapshot(
      crate::SnapshotMeta::new(
        Index::new(1),
        Term::new(2),
        crate::ConfState::from_voters(std::vec![1u64]),
      )
      .with_fork_id(token.clone()),
      fork_blob(2),
    );
    let mut control: MultiRaft<u64, u64, SplitSm> = MultiRaft::new();
    control
      .restore_group_unchecked(
        200,
        single_node_cfg(1),
        Instant::ORIGIN,
        44,
        SplitSm::default(),
        1,
        &mut healthy_log,
        &mut healthy_stable,
      )
      .unwrap();
    let healthy = control.group(&200).expect("hosted");
    assert!(!healthy.is_poisoned(), "the control must not be poisoned");
    assert_eq!(
      healthy.fork_id(),
      Some(token.clone()),
      "these stores DO carry the token, so the fault case's None is the clear at work"
    );
  }

  m.restore_group_unchecked(
    200,
    single_node_cfg(1),
    Instant::ORIGIN,
    44,
    SplitSm::default(),
    1,
    &mut clog,
    &mut cstable,
  )
  .unwrap();

  let child = m.group(&200).expect("the child is hosted");
  // By name: a log-read fault, not some unrelated poison the fixture stumbled into.
  assert_eq!(
    child.poison_reason(),
    Some(crate::PoisonReason::LogRead),
    "the fixture must poison through a log-read fault"
  );
  assert!(
    child.fork_id().is_none(),
    "and the poisoned restart took the provenance token with it — the control above shows these \
     stores carry one"
  );

  m.remove_group(&200, &mut empty_stores()).unwrap();
  assert_eq!(
    m.group(&7).unwrap().staged_forks().count(),
    1,
    "the parent's staged fork survives the inert child's removal"
  );
  assert_eq!(
    m.poll_relay_guard_advance(),
    None,
    "and its replay guard did not advance, so a replay still re-stages it"
  );
  let fork = m
    .peek_yieldable_fork(&NoHold)
    .expect("the partition re-materializes from the copy that survived");
  assert_eq!(*fork.child(), 200);
}

/// THE CLAIM IS EVIDENCE OF HISTORY, NOT EVIDENCE OF LINEAGE.
///
/// A catalog naming a nonzero incarnation asserts the id HAS a history, so empty stores — or stores
/// that name no incarnation at all — contradict it exactly as they contradict a nonzero record.
/// Gating those two refusals on the record alone let a claim recover a blank endpoint under a name
/// it never reached: on a single voter it campaigns and serves that blank state, and its
/// generation-0 frames pass any peer whose floor is still zero.
///
/// What the claim is NOT is a bound the stores must reach. That is the discriminator this test
/// pins in both directions.
#[test]
fn a_nonzero_claim_needs_stores_that_name_an_incarnation() {
  let cmd = {
    let mut b = Vec::new();
    Bytes::from_static(b"c").encode(&mut b);
    Bytes::from(b)
  };
  let with_content = || {
    let mut l = VecLog::default();
    l.force_append(&[crate::Entry::new(
      Term::new(1),
      Index::new(1),
      crate::EntryKind::Normal,
      cmd.clone(),
    )]);
    l
  };

  // (record 0, floor 0, claim N) over NOTHING: the claim alone now trips the empty-stores refusal.
  let empty_log = VecLog::default();
  let empty_stable = AsyncStable::default();
  assert_eq!(
    crate::validate_restore(0, 0, 7, &empty_log, &empty_stable),
    Err(CreateGroupError::NoStoredState),
    "a claimed incarnation over empty stores has nothing to recover"
  );
  // …and the gen-0 world is untouched: no claim, no record, no floor, nothing asserted.
  assert_eq!(
    crate::validate_restore(0, 0, 0, &empty_log, &empty_stable),
    Ok(())
  );

  // (record 0, floor 0, claim N) over content whose hard state names NO incarnation.
  let initial_hs = AsyncStable::default();
  assert_eq!(initial_hs.hard_state(), crate::HardState::initial());
  assert_eq!(
    crate::validate_restore(0, 0, 7, &with_content(), &initial_hs),
    Err(CreateGroupError::IncarnationUnrecoverable { record: 0 }),
    "state that names no incarnation cannot be recovered as one the catalog asserts"
  );

  // THE DISCRIMINATOR, and the reason the claim is not a lower bound on the stores: a NON-INITIAL
  // hard state means some incarnation really did write here, so these stores are that
  // incarnation's — and a claim may legitimately run ahead of everything readable when the evidence
  // is a retained, still-uncommitted shape entry that will carry the counter the rest of the way.
  let mut wrote = AsyncStable::default();
  wrote.force_state(Term::new(1), Some(1u64), Index::new(1));
  assert_eq!(
    crate::validate_restore(0, 0, 7, &with_content(), &wrote),
    Ok(()),
    "a claim ahead of readable lineage is the reopen this discipline exists for, not a fault"
  );

  // EVERY LEGITIMATE DOOR'S TUPLE, admitted. `(record, floor, generation)` against stores that
  // carry the matching evidence — none of the refusals above may catch one of these.
  for (record, floor, generation, founding) in [
    (0u64, 0u64, 0u64, 0u64),
    (2, 2, 2, 2),
    (5, 0, 5, 0),
    (1, 0, 1, 0),
  ] {
    let mut st = AsyncStable::default();
    st.force_state(Term::new(1), Some(1u64), Index::new(1));
    if founding > 0 {
      st.force_hard_state(
        crate::HardState::initial()
          .with_term(Term::new(1))
          .with_founding_gen(founding),
      );
    }
    assert_eq!(
      crate::validate_restore(record, floor, generation, &with_content(), &st),
      Ok(()),
      "legitimate door refused: record {record}, floor {floor}, generation {generation}"
    );
  }
}
