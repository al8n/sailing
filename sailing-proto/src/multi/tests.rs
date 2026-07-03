use super::*;
use crate::{
  Config, Instant,
  testkit::{AsyncStable, CountSm, VecLog},
};
use bytes::Bytes;
use core::time::Duration;

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
