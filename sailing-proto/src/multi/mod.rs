//! Multi-Raft: hosting many single-group [`Endpoint`]s in one process.
//!
//! [`MultiRaft`] is the Sans-I/O super-state-machine — a container of independent single-group
//! [`Endpoint`]s keyed by [`GroupId`]. You drive it exactly like an [`Endpoint`] (inject peer
//! messages, timer deadlines, and storage completions; drain outbound messages and events), except
//! every call is addressed to a group and the output and scheduling surface is aggregated across
//! all groups. The consensus core stays completely group-unaware; this layer owns only the routing
//! and the aggregate drains.
//!
//! See `MULTI_RAFT.md` for the architecture and the phased roadmap. Groups are created and
//! removed at runtime over caller-injected per-group storage (each group is handed its own
//! `LogStore`/`StableStore` per call, mirroring [`Endpoint`]); `MultiRaft` itself stays the PURE
//! container — the dynamic-lifecycle mechanics around it (removal tombstones, unknown-group
//! surfacing, removed-self notification) live in the multi coordinators and the drivers, and the
//! placement POLICY — where a group should live, when to tear a removed replica down — is
//! explicitly the embedder's (no auto-create, no auto-teardown). The group-tagged wire (the
//! multi-group coordinators) and the shared storage engine ([`GroupEngine`] — every co-located
//! group's stores behind ONE batched durability barrier) consume this surface without reshaping
//! it. Every client-facing routing method of the single-group [`Endpoint`] — propose, conf
//! changes (v1/v2), read-index, leader transfer, read-mode migration — has a group-keyed
//! delegate here.

mod engine;
pub use engine::{EngineLog, EngineStable, EngineStorageError, GroupEngine};

mod group_id;
pub use group_id::GroupId;

use crate::{
  ConfChange, ConfChangeV2, Config, CreateGroupError, Data, Endpoint, Event, Index, Instant,
  LogStore, Message, NodeId, Now, Outgoing, Prng, ProposeError, ReadIndexError, ReadOnlyOption,
  StableStore, StateMachine, StorageProgress, TransferError,
};
use bytes::Bytes;
use cheap_clone::CheapClone;
use core::time::Duration;
use std::{
  collections::{BTreeMap, VecDeque},
  vec::Vec,
};

/// The create/restore admission check shared by every group constructor: group-id uniqueness, the
/// wire bound on the encoded group id, and agreement with the LATCHED host identity (a multi-Raft
/// host is one physical node for its whole lifetime — the transport authenticates exactly one
/// identity per connection, and live connections outlast group removal, so the check must not
/// relax when the map empties).
fn validate_new_group<G, I, F, R>(
  groups: &BTreeMap<G, Endpoint<I, F, R>>,
  host_id: &Option<I>,
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
  if let Some(host) = host_id
    && *host != config.id()
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
  /// The host's node identity, latched by the FIRST admitted group and retained for the
  /// container's whole lifetime — including across [`remove_group`](Self::remove_group) emptying
  /// the map. A multi-Raft host is one physical node: live transport connections stay
  /// authenticated under this id, so a later admission must never change it (see
  /// [`CreateGroupError::NodeIdMismatch`]).
  host_id: Option<I>,
}

// No `I` (or `R`) bound: the container and drain surface neither keys by, encodes, nor clones a
// node id — the delegated `Endpoint` polls live in its bound-free block.
impl<G, I, F, R> MultiRaft<G, I, F, R>
where
  G: GroupId,
  F: StateMachine,
{
  /// An empty host with no groups.
  #[must_use]
  pub fn new() -> Self {
    Self {
      groups: BTreeMap::new(),
      dirty_msgs: VecDeque::new(),
      dirty_events: VecDeque::new(),
      host_id: None,
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

  /// The host's node identity — latched by the first admitted group and retained across group
  /// removals ([`CreateGroupError::NodeIdMismatch`] enforces it). `None` until any group has ever
  /// been admitted.
  #[must_use]
  pub fn host_id(&self) -> Option<&I> {
    self.host_id.as_ref()
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
  /// next poll. This is the dynamic-lifecycle teardown seam: the container stays PURE (no
  /// tombstone here — the multi coordinators tombstone the id so the wire's stragglers drop
  /// silently). The host identity is NOT cleared — even removing the last group leaves it
  /// latched, so a re-created group must carry the same node id (live transport connections stay
  /// authenticated under it).
  pub fn remove_group(&mut self, gid: &G) -> Option<Endpoint<I, F, R>> {
    self.groups.remove(gid)
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

// The aggregate scheduling surface, split from the block above because `Endpoint::poll_timeout`
// carries the full node-id bound.
impl<G, I, F, R> MultiRaft<G, I, F, R>
where
  G: GroupId,
  I: NodeId,
  F: StateMachine,
{
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
}

// Default-`Prng` constructors, mirroring `Endpoint::new`/`restart` (which are `Prng`-only; the
// generic-RNG entry points live on `Endpoint`'s and this type's `*_with_rng` family).
impl<G, I, F> MultiRaft<G, I, F, Prng>
where
  G: GroupId,
  I: NodeId,
  F: StateMachine,
{
  /// Create a fresh group (Follower, term 0, empty log view). The group's election RNG is seeded by
  /// `seed` folded with `gid`, so co-located groups draw decorrelated election-timeout jitter
  /// (identical jitter would correlate their elections into a host-wide storm).
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
    validate_new_group(&self.groups, &self.host_id, &gid, &config)?;
    self.host_id.get_or_insert(config.id());
    let ep = Endpoint::new(config, now, group_seed(seed, &gid), fsm);
    self.groups.insert(gid, ep);
    Ok(())
  }

  /// Recover a group from durable storage, replaying its committed tail into the state machine.
  /// Replay surfaces NO events (mirroring [`Endpoint::restart`], which deliberately clears them —
  /// replay is not new work); the restore MAY leave one pending stable write (a grown lease
  /// floor), drained by the driver's normal `handle_storage` cadence. Same `gid`-folded seeding as
  /// [`create_group`](Self::create_group).
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
    validate_new_group(&self.groups, &self.host_id, &gid, &config)?;
    self.host_id.get_or_insert(config.id());
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
    // Defensive only: `Endpoint::restart` currently surfaces no output (replay events are
    // deliberately cleared), so this marks an empty queue. The restore variants below mirror it.
    self.mark_dirty(&gid);
    Ok(())
  }

  /// Recover a group from a pre-format store, wrapping [`Endpoint::restart_migrating`] — the
  /// ONE-TIME upgrade path for a node that persisted no lease-support floor. See that method for
  /// the `assume_prior_lease_support` contract.
  ///
  /// # Errors
  /// The same admission checks as [`create_group`](Self::create_group) — see
  /// [`CreateGroupError`]. Refusal happens BEFORE any store is read.
  #[allow(clippy::too_many_arguments)]
  pub fn restore_group_migrating<L, S>(
    &mut self,
    gid: G,
    config: Config<I>,
    now: impl Into<Now>,
    seed: u64,
    fsm: F,
    boot_epoch: u64,
    assume_prior_lease_support: Option<Duration>,
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
    validate_new_group(&self.groups, &self.host_id, &gid, &config)?;
    self.host_id.get_or_insert(config.id());
    let ep = Endpoint::restart_migrating(
      config,
      now,
      group_seed(seed, &gid),
      fsm,
      boot_epoch,
      assume_prior_lease_support,
      log,
      stable,
    );
    self.groups.insert(gid.cheap_clone(), ep);
    self.mark_dirty(&gid);
    Ok(())
  }
}

// Caller-supplied-RNG constructors, mirroring `Endpoint`'s `*_with_rng` family. The seed-taking
// constructors fold the group id into the seed automatically; here the CALLER owns cross-group
// decorrelation — supply a distinctly-seeded RNG per group, or co-located groups draw correlated
// election jitter.
impl<G, I, F, R> MultiRaft<G, I, F, R>
where
  G: GroupId,
  I: NodeId,
  F: StateMachine,
  R: rand::Rng,
{
  /// Create a fresh group driven by a caller-supplied RNG (see [`Endpoint::new_with_rng`]).
  ///
  /// # Errors
  /// The admission checks of [`CreateGroupError`], as on the seed-taking constructors.
  pub fn create_group_with_rng(
    &mut self,
    gid: G,
    config: Config<I>,
    now: impl Into<Now>,
    rng: R,
    fsm: F,
  ) -> Result<(), CreateGroupError> {
    validate_new_group(&self.groups, &self.host_id, &gid, &config)?;
    self.host_id.get_or_insert(config.id());
    let ep = Endpoint::new_with_rng(config, now, rng, fsm);
    self.groups.insert(gid, ep);
    Ok(())
  }

  /// Recover a group from durable storage with a caller-supplied RNG (see
  /// [`Endpoint::restart_with_rng`]).
  ///
  /// # Errors
  /// The admission checks of [`CreateGroupError`]; refusal happens BEFORE any store is read.
  #[allow(clippy::too_many_arguments)]
  pub fn restore_group_with_rng<L, S>(
    &mut self,
    gid: G,
    config: Config<I>,
    now: impl Into<Now>,
    rng: R,
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
    validate_new_group(&self.groups, &self.host_id, &gid, &config)?;
    self.host_id.get_or_insert(config.id());
    let ep = Endpoint::restart_with_rng(config, now, rng, fsm, boot_epoch, log, stable);
    self.groups.insert(gid.cheap_clone(), ep);
    self.mark_dirty(&gid);
    Ok(())
  }

  /// Recover a group from a pre-format store with a caller-supplied RNG (see
  /// [`Endpoint::restart_migrating_with_rng`]).
  ///
  /// # Errors
  /// The admission checks of [`CreateGroupError`]; refusal happens BEFORE any store is read.
  #[allow(clippy::too_many_arguments)]
  pub fn restore_group_migrating_with_rng<L, S>(
    &mut self,
    gid: G,
    config: Config<I>,
    now: impl Into<Now>,
    rng: R,
    fsm: F,
    boot_epoch: u64,
    assume_prior_lease_support: Option<Duration>,
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
    validate_new_group(&self.groups, &self.host_id, &gid, &config)?;
    self.host_id.get_or_insert(config.id());
    let ep = Endpoint::restart_migrating_with_rng(
      config,
      now,
      rng,
      fsm,
      boot_epoch,
      assume_prior_lease_support,
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
  #[must_use = "`None` means no group with this id is hosted — the call did nothing"]
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
  #[must_use = "`None` means no group with this id is hosted — the call did nothing"]
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
  #[must_use = "`None` means no group with this id is hosted — the call did nothing"]
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

  /// Propose a command on `gid`, which must be the leader. `None` if no such group, else the
  /// append result. Call
  /// [`flush_appends`](Self::flush_appends) for the group once after a burst of proposals.
  #[must_use = "`None` means no group with this id is hosted — the call did nothing"]
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
  #[must_use = "`None` means no group with this id is hosted — the call did nothing"]
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

  /// Propose a membership change (single-step) on `gid`, which must be the leader. `None` if no
  /// such group. As with [`propose`](Self::propose), call [`flush_appends`](Self::flush_appends)
  /// for the group once after a burst.
  #[must_use = "`None` means no group with this id is hosted — nothing was proposed"]
  pub fn propose_conf_change<L, S>(
    &mut self,
    gid: &G,
    now: impl Into<Now>,
    log: &mut L,
    stable: &S,
    cc: ConfChange<I>,
  ) -> Option<Result<Index, ProposeError<I>>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let result = self
      .groups
      .get_mut(gid)?
      .propose_conf_change(now, log, stable, cc);
    self.mark_dirty(gid);
    Some(result)
  }

  /// Propose a membership change (joint-consensus capable) on `gid`, which must be the leader.
  /// `None` if no such group. As with [`propose`](Self::propose), call
  /// [`flush_appends`](Self::flush_appends) for the group once after a burst.
  #[must_use = "`None` means no group with this id is hosted — nothing was proposed"]
  pub fn propose_conf_change_v2<L, S>(
    &mut self,
    gid: &G,
    now: impl Into<Now>,
    log: &mut L,
    stable: &S,
    cc: ConfChangeV2<I>,
  ) -> Option<Result<Index, ProposeError<I>>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let result = self
      .groups
      .get_mut(gid)?
      .propose_conf_change_v2(now, log, stable, cc);
    self.mark_dirty(gid);
    Some(result)
  }

  /// Propose a cluster-wide read-mode migration on `gid`, which must be the leader. `None` if no
  /// such group. As with [`propose`](Self::propose), call [`flush_appends`](Self::flush_appends)
  /// for the group once after a burst.
  #[must_use = "`None` means no group with this id is hosted — nothing was proposed"]
  pub fn propose_read_mode_change<L, S>(
    &mut self,
    gid: &G,
    now: impl Into<Now>,
    log: &mut L,
    stable: &S,
    mode: ReadOnlyOption,
  ) -> Option<Result<Index, ProposeError<I>>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let result = self
      .groups
      .get_mut(gid)?
      .propose_read_mode_change(now, log, stable, mode);
    self.mark_dirty(gid);
    Some(result)
  }

  /// Initiate a linearizable read on `gid`; the resulting `ReadState` surfaces via
  /// [`poll_event`](Self::poll_event) stamped with the group. `None` if no such group.
  #[must_use = "`None` means no group with this id is hosted — no read was initiated"]
  pub fn read_index<L, S>(
    &mut self,
    gid: &G,
    now: impl Into<Now>,
    log: &L,
    stable: &S,
    context: Bytes,
  ) -> Option<Result<(), ReadIndexError>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let result = self
      .groups
      .get_mut(gid)?
      .read_index(now, log, stable, context);
    self.mark_dirty(gid);
    Some(result)
  }

  /// Begin transferring `gid`'s leadership to `to`. `None` if no such group.
  #[must_use = "`None` means no group with this id is hosted — no transfer was initiated"]
  pub fn transfer_leader<L, S>(
    &mut self,
    gid: &G,
    now: impl Into<Now>,
    log: &L,
    stable: &S,
    to: I,
  ) -> Option<Result<(), TransferError<I>>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let result = self
      .groups
      .get_mut(gid)?
      .transfer_leader(now, log, stable, to);
    self.mark_dirty(gid);
    Some(result)
  }
}

impl<G, I, F, R> Default for MultiRaft<G, I, F, R>
where
  G: GroupId,
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
mod tests;
