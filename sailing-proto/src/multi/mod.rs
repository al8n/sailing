//! Multi-Raft: hosting many single-group [`Endpoint`]s in one process.
//!
//! [`MultiRaft`] is the Sans-I/O super-state-machine — a container of independent single-group
//! [`Endpoint`]s keyed by [`GroupId`]. You drive it exactly like an [`Endpoint`] (inject peer
//! messages, timer deadlines, and storage completions; drain outbound messages and events), except
//! every call is addressed to a group and the output and scheduling surface is aggregated across
//! all groups. The consensus core stays completely group-unaware; this layer owns only the routing
//! and the aggregate drains.
//!
//! See `MULTI_RAFT.md` for the architecture and the phased roadmap. This is the **Phase 0
//! scaffold**: a static, append-only group set over caller-injected per-group storage (each group
//! is handed its own `LogStore`/`StableStore` per call, mirroring [`Endpoint`]). The shared storage
//! engine, the group-tagged wire, the threaded reactor, heartbeat coalescing, and dynamic group
//! lifecycle are later phases that consume this surface without reshaping it. The read/transfer/
//! conf-change routing methods (`read_index`, `transfer_leader`, `propose_conf_change_v2`) delegate
//! identically to the ones here and are added as the layer grows.

mod group_id;
pub use group_id::GroupId;

use crate::{
  Config, CreateGroupError, Data, Endpoint, Event, Index, Instant, LogStore, Message, NodeId, Now,
  Outgoing, Prng, ProposeError, StableStore, StateMachine, StorageProgress,
};
use cheap_clone::CheapClone;
use std::{
  collections::{BTreeMap, VecDeque},
  vec::Vec,
};

/// The create/restore admission check shared by every group constructor: group-id uniqueness, the
/// wire bound on the encoded group id, and cross-group node-id agreement (a multi-Raft host is one
/// physical node, so every group must be configured with the same local id — the transport
/// authenticates exactly one identity per connection).
fn validate_new_group<G, I, F, R>(
  groups: &BTreeMap<G, Endpoint<I, F, R>>,
  gid: &G,
  config: &Config<I>,
) -> Result<(), CreateGroupError>
where
  G: GroupId,
  I: NodeId,
  F: StateMachine,
{
  if groups.contains_key(gid) {
    return Err(CreateGroupError::Exists);
  }
  let mut encoded = Vec::new();
  gid.encode(&mut encoded);
  if encoded.is_empty() || encoded.len() > crate::wire::MAX_GROUP_ID_LEN {
    return Err(CreateGroupError::InvalidGroupId);
  }
  if let Some(existing) = groups.values().next()
    && existing.id() != config.id()
  {
    return Err(CreateGroupError::NodeIdMismatch);
  }
  Ok(())
}

/// A container of single-group [`Endpoint`]s multiplexed by [`GroupId`].
///
/// Generic over the group id `G`, the node id `I`, the application state machine `F`, and the
/// election RNG `R` (defaulting to the deterministic [`Prng`], as [`Endpoint`] does). See the
/// module-level documentation for the driving model and `MULTI_RAFT.md` for the architecture.
pub struct MultiRaft<G, I, F, R = Prng>
where
  F: StateMachine,
{
  groups: BTreeMap<G, Endpoint<I, F, R>>,
  /// Groups that may have a pending outbound message to drain (see [`poll_message`](Self::poll_message)).
  /// Enqueued after every dispatch and removed lazily once the group's message queue is exhausted.
  dirty_msgs: VecDeque<G>,
  /// Groups that may have a pending event to drain (see [`poll_event`](Self::poll_event)).
  dirty_events: VecDeque<G>,
}

impl<G, I, F, R> MultiRaft<G, I, F, R>
where
  G: GroupId,
  I: NodeId,
  F: StateMachine,
{
  /// An empty host with no groups.
  #[must_use]
  pub fn new() -> Self {
    Self {
      groups: BTreeMap::new(),
      dirty_msgs: VecDeque::new(),
      dirty_events: VecDeque::new(),
    }
  }

  /// The number of hosted groups.
  #[must_use]
  pub fn len(&self) -> usize {
    self.groups.len()
  }

  /// Whether no groups are hosted.
  #[must_use]
  pub fn is_empty(&self) -> bool {
    self.groups.is_empty()
  }

  /// Whether a group with `gid` is hosted.
  #[must_use]
  pub fn contains_group(&self, gid: &G) -> bool {
    self.groups.contains_key(gid)
  }

  /// A shared reference to one group's [`Endpoint`], for observability (role, term, commit,
  /// applied index, leader, poison, the state machine). `None` if no such group.
  #[must_use]
  pub fn group(&self, gid: &G) -> Option<&Endpoint<I, F, R>> {
    self.groups.get(gid)
  }

  /// The hosted group ids, ascending.
  pub fn group_ids(&self) -> impl Iterator<Item = &G> {
    self.groups.keys()
  }

  /// Remove and return a group's [`Endpoint`]. Stale drain-queue entries for it are skipped on the
  /// next poll. This is the teardown seam the dynamic-lifecycle phase builds on.
  pub fn remove_group(&mut self, gid: &G) -> Option<Endpoint<I, F, R>> {
    self.groups.remove(gid)
  }

  /// The earliest serviceable timer deadline across all groups, or `None` if no group has one.
  ///
  /// This is the pure-core convenience: an `O(N)` minimum. The Phase-3 reactor keeps an aggregate
  /// timing wheel over [`deadlines`](Self::deadlines) instead, waking only the due group.
  #[must_use]
  pub fn poll_timeout(&self) -> Option<Instant> {
    self
      .groups
      .values()
      .filter_map(Endpoint::poll_timeout)
      .min()
  }

  /// Each group's next serviceable deadline — the reactor's input for building its timing wheel.
  pub fn deadlines(&self) -> impl Iterator<Item = (G, Instant)> + '_ {
    self
      .groups
      .iter()
      .filter_map(|(gid, ep)| ep.poll_timeout().map(|d| (gid.cheap_clone(), d)))
  }

  /// The next outbound message from any group, stamped with its group id. Drain fully between
  /// drives (the per-group queues are unbounded, as [`Endpoint::poll_message`] is).
  pub fn poll_message(&mut self) -> Option<(G, Outgoing<I>)> {
    while let Some(gid) = self.dirty_msgs.front().map(CheapClone::cheap_clone) {
      match self.groups.get_mut(&gid).and_then(Endpoint::poll_message) {
        Some(msg) => return Some((gid, msg)),
        None => {
          self.dirty_msgs.pop_front();
        }
      }
    }
    None
  }

  /// The next application event from any group, stamped with its group id.
  pub fn poll_event(&mut self) -> Option<(G, Event<I, F::Response>)> {
    while let Some(gid) = self.dirty_events.front().map(CheapClone::cheap_clone) {
      match self.groups.get_mut(&gid).and_then(Endpoint::poll_event) {
        Some(ev) => return Some((gid, ev)),
        None => {
          self.dirty_events.pop_front();
        }
      }
    }
    None
  }

  /// Enqueue a group for output draining after a dispatch. Consecutive-deduped so a burst of
  /// dispatches to one group between drains does not grow the queues unboundedly.
  fn mark_dirty(&mut self, gid: &G) {
    if self.dirty_msgs.back() != Some(gid) {
      self.dirty_msgs.push_back(gid.cheap_clone());
    }
    if self.dirty_events.back() != Some(gid) {
      self.dirty_events.push_back(gid.cheap_clone());
    }
  }
}

// Default-`Prng` constructors, mirroring `Endpoint::new`/`restart` (which are `Prng`-only; the
// generic-RNG entry points live on `Endpoint` and a `MultiRaft` follow-on).
impl<G, I, F> MultiRaft<G, I, F, Prng>
where
  G: GroupId,
  I: NodeId,
  F: StateMachine,
{
  /// Create a fresh group (Follower, term 0, empty log view). The group's election RNG is seeded by
  /// `seed` folded with `gid`, so co-located groups never draw identical election-timeout jitter
  /// (which would correlate their elections into a host-wide storm).
  ///
  /// # Errors
  /// [`CreateGroupError::Exists`] if a group with `gid` is already hosted,
  /// [`CreateGroupError::NodeIdMismatch`] if `config`'s id differs from the hosted groups' shared
  /// node id, and [`CreateGroupError::InvalidGroupId`] if `gid`'s encoding is outside the wire
  /// bound (1..=1024 bytes). Hosted groups are untouched in every case; on `Err` the moved-in
  /// `fsm` is dropped — pre-check [`contains_group`](Self::contains_group) to preserve it.
  pub fn create_group(
    &mut self,
    gid: G,
    config: Config<I>,
    now: impl Into<Now>,
    seed: u64,
    fsm: F,
  ) -> Result<(), CreateGroupError> {
    validate_new_group(&self.groups, &gid, &config)?;
    let ep = Endpoint::new(config, now, group_seed(seed, &gid), fsm);
    self.groups.insert(gid, ep);
    Ok(())
  }

  /// Recover a group from durable storage, replaying its committed tail (which may enqueue
  /// `Applied` events to drain). Same `gid`-folded seeding as [`create_group`](Self::create_group).
  ///
  /// # Errors
  /// The same admission checks as [`create_group`](Self::create_group) — see
  /// [`CreateGroupError`]. Refusal happens BEFORE any store is read.
  #[allow(clippy::too_many_arguments)]
  pub fn restore_group<L, S>(
    &mut self,
    gid: G,
    config: Config<I>,
    now: impl Into<Now>,
    seed: u64,
    fsm: F,
    boot_epoch: u64,
    log: &mut L,
    stable: &mut S,
  ) -> Result<(), CreateGroupError>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
    F::Command: Data,
    F::Snapshot: Data,
    F::Error: core::error::Error,
    I: Data,
  {
    validate_new_group(&self.groups, &gid, &config)?;
    let ep = Endpoint::restart(
      config,
      now,
      group_seed(seed, &gid),
      fsm,
      boot_epoch,
      log,
      stable,
    );
    self.groups.insert(gid.cheap_clone(), ep);
    self.mark_dirty(&gid);
    Ok(())
  }
}

// The per-group driving surface. Delegates to the group then marks it for an output drain. Each
// method mirrors the same-named `Endpoint` method and returns `None` when no group `gid` is hosted.
// The `F::Command`/`F::Error` bounds are the apply-path bounds the `Endpoint` driving methods carry.
impl<G, I, F, R> MultiRaft<G, I, F, R>
where
  G: GroupId,
  I: NodeId,
  F: StateMachine,
  R: rand::Rng,
  F::Command: Data,
  F::Error: core::error::Error,
{
  /// Route an inbound peer message to `gid`. `None` if no such group.
  pub fn handle_message<L, S>(
    &mut self,
    gid: &G,
    now: impl Into<Now>,
    log: &mut L,
    stable: &mut S,
    from: I,
    msg: Message<I>,
  ) -> Option<()>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
    F::Snapshot: Data,
  {
    self
      .groups
      .get_mut(gid)?
      .handle_message(now, log, stable, from, msg);
    self.mark_dirty(gid);
    Some(())
  }

  /// Fire `gid`'s due timers. `None` if no such group.
  pub fn handle_timeout<L, S>(
    &mut self,
    gid: &G,
    now: impl Into<Now>,
    log: &mut L,
    stable: &mut S,
  ) -> Option<()>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    self.groups.get_mut(gid)?.handle_timeout(now, log, stable);
    self.mark_dirty(gid);
    Some(())
  }

  /// Drain `gid`'s storage completions. `None` if no such group, else that group's
  /// [`StorageProgress`] (`MorePending` asks to be re-driven without sleeping).
  pub fn handle_storage<L, S>(
    &mut self,
    gid: &G,
    now: impl Into<Now>,
    log: &mut L,
    stable: &mut S,
  ) -> Option<StorageProgress>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
    F::Snapshot: Data,
  {
    let progress = self.groups.get_mut(gid)?.handle_storage(now, log, stable);
    self.mark_dirty(gid);
    Some(progress)
  }

  /// Propose a command to `gid`'s leader. `None` if no such group, else the append result. Call
  /// [`flush_appends`](Self::flush_appends) for the group once after a burst of proposals.
  pub fn propose<L, S>(
    &mut self,
    gid: &G,
    now: impl Into<Now>,
    log: &mut L,
    stable: &S,
    cmd: &F::Command,
  ) -> Option<Result<Index, ProposeError<I>>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let result = self.groups.get_mut(gid)?.propose(now, log, stable, cmd);
    self.mark_dirty(gid);
    Some(result)
  }

  /// Flush `gid`'s coalesced replication fan-out (once per drive, after a propose burst). `None`
  /// if no such group.
  pub fn flush_appends<L, S>(
    &mut self,
    gid: &G,
    now: impl Into<Now>,
    log: &L,
    stable: &S,
  ) -> Option<()>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    self.groups.get_mut(gid)?.flush_appends(now, log, stable);
    self.mark_dirty(gid);
    Some(())
  }
}

impl<G, I, F, R> Default for MultiRaft<G, I, F, R>
where
  G: GroupId,
  I: NodeId,
  F: StateMachine,
{
  fn default() -> Self {
    Self::new()
  }
}

/// Fold the group id into the base election seed so co-located groups draw distinct
/// election-timeout jitter. FNV-1a over the id's [`Data`] encoding, perturbed by the base seed.
fn group_seed<G: GroupId>(base: u64, gid: &G) -> u64 {
  let mut buf = Vec::new();
  gid.encode(&mut buf);
  let mut h = 0xcbf2_9ce4_8422_2325_u64 ^ base;
  for b in &buf {
    h ^= u64::from(*b);
    h = h.wrapping_mul(0x0000_0100_0000_01b3);
  }
  h
}

#[cfg(test)]
mod tests {
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
}
