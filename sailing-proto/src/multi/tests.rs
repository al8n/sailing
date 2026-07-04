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
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    preloaded_sm(3),
    blob.clone(),
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
      single_node_cfg(1),
      Instant::ORIGIN,
      42,
      preloaded_sm(3),
      fork_blob(3),
      1,
      &mut log,
      &mut stable,
    ),
    Err(CreateGroupError::Exists)
  );
  untouched(&log, &stable);

  assert_eq!(
    m.create_group_from_fork(
      2,
      single_node_cfg(2),
      Instant::ORIGIN,
      42,
      preloaded_sm(3),
      fork_blob(3),
      1,
      &mut log,
      &mut stable,
    ),
    Err(CreateGroupError::NodeIdMismatch)
  );
  untouched(&log, &stable);

  let mut sized: MultiRaft<SizedId, u64, CountSm> = MultiRaft::new();
  for bad in [SizedId(0), SizedId(1025)] {
    assert_eq!(
      sized.create_group_from_fork(
        bad,
        single_node_cfg(1),
        Instant::ORIGIN,
        42,
        preloaded_sm(3),
        fork_blob(3),
        1,
        &mut log,
        &mut stable,
      ),
      Err(CreateGroupError::InvalidGroupId)
    );
    untouched(&log, &stable);
  }
}

#[test]
fn fork_blob_is_authoritative_over_the_vessel() {
  let mut m: MultiRaft<u64, u64, CountSm> = MultiRaft::new();
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  // The vessel disagrees with the blob: the blob must win identically everywhere.
  m.create_group_from_fork(
    7,
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    preloaded_sm(5),
    fork_blob(9),
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
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    CountSm::default(),
    Bytes::from_static(&[1, 2, 3]),
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
    voter_pair_cfg(1),
    Instant::ORIGIN,
    42,
    preloaded_sm(4),
    fork_blob(4),
    1,
    &mut la,
    &mut sa,
  )
  .unwrap();
  b.create_group_from_fork(
    7,
    voter_pair_cfg(2),
    Instant::ORIGIN,
    43,
    preloaded_sm(4),
    fork_blob(4),
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
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    preloaded_sm(6),
    fork_blob(6),
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
    single_node_cfg(1),
    Instant::ORIGIN,
    42,
    preloaded_sm(3),
    fork_blob(3),
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
