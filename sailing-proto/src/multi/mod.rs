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
  ConfChange, ConfChangeV2, ConfState, Config, CreateGroupError, Data, Endpoint, Event, HardState,
  Index, Instant, LogStore, Message, NodeId, Now, OpId, Outgoing, PoisonReason, Prng, ProposeError,
  ReadIndexError, ReadOnlyOption, SnapshotMeta, SplitError, SplitPayload, StableStore,
  StateMachine, StorageProgress, Term, TransferError,
};
use bytes::Bytes;
use cheap_clone::CheapClone;
use core::time::Duration;
use std::{
  collections::{BTreeMap, VecDeque},
  vec::Vec,
};

/// Read-only lineage lookup consulted at group admission (and, from M4/M5 on, at the
/// split/merge propose-time gates). [`GroupEngine`] implements it over its lineage records — the
/// driver hands its engine in at every admission call; the deterministic sim brings an in-memory
/// impl; a coordinator-embedder that brings its own storage brings its own impl, whose
/// durability is that storage's job. [`NoFloors`] is the gen-0 convenience: every lookup is 0,
/// which makes admission behave exactly as it did before floors existed (no fence).
pub trait FloorStore<G> {
  /// The admission floor for `gid` (0 = never floored).
  fn floor(&self, gid: &G) -> u64;
  /// The id's current incarnation/shape counter (0 = unreshaped).
  fn lineage(&self, gid: &G) -> u64;
}

/// The no-fence store: a gen-0 world (all of today's embedders and tests). Both lookups return
/// 0, so no incarnation is ever below its floor and the volatile tombstone remains the only
/// re-admission gate.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NoFloors;

impl<G> FloorStore<G> for NoFloors {
  fn floor(&self, _gid: &G) -> u64 {
    0
  }

  fn lineage(&self, _gid: &G) -> u64 {
    0
  }
}

/// The admission floor a merge writes for the absorbed id (the merge milestone is its ONLY
/// writer): `u64::MAX` fences every future incarnation, so a merged-away id never returns.
/// Consequently `u64::MAX` is NOT a usable working generation — it is reserved as this fence,
/// and [`floor_admits`] refuses it at ANY floor. That reservation is what makes this fence
/// terminal BY CONSTRUCTION: the floor leg alone (`generation >= floor`) would wave the
/// sentinel generation through its own fence, since `u64::MAX < u64::MAX` is false.
pub const MERGED_FLOOR: u64 = u64::MAX;

/// The floor-admission predicate every admission gate applies — the multi coordinators'
/// create/restore checks and the drivers' factory pre-build gate: `generation` is admissible
/// against `floor` iff it clears the floor AND is not the reserved [`MERGED_FLOOR`] sentinel
/// (never a working incarnation). The second leg makes a `MERGED_FLOOR` fence refuse EVERY
/// generation, the sentinel itself included.
pub const fn floor_admits(floor: u64, generation: u64) -> bool {
  generation < MERGED_FLOOR && generation >= floor
}

/// The refusing form of [`floor_admits`] the coordinators' admission methods report through:
/// the reserved sentinel under a lower floor maps to [`CreateGroupError::ReservedGeneration`]
/// (the caller supplied a generation that can never work), everything else to
/// [`CreateGroupError::BelowFloor`] — under a `MERGED_FLOOR` fence the fence itself stays the
/// truthful terminal verdict at every generation, sentinel included. Gated with its only
/// consumers, the transport coordinators (`quic` implies `tcp`).
#[cfg(feature = "tcp")]
pub(crate) const fn validate_floor(floor: u64, generation: u64) -> Result<(), CreateGroupError> {
  if floor_admits(floor, generation) {
    Ok(())
  } else if generation == MERGED_FLOOR && floor < MERGED_FLOOR {
    Err(CreateGroupError::ReservedGeneration)
  } else {
    Err(CreateGroupError::BelowFloor { floor })
  }
}

/// The engine IS the drivers' floor store: lookups delegate to the freshest-read lineage
/// accessors, so a floor staged this crank already fences before the barrier lands (monotone-max
/// staging makes that early visibility safe).
impl<G, I> FloorStore<G> for GroupEngine<G, I>
where
  G: Ord,
{
  fn floor(&self, gid: &G) -> u64 {
    self.group_floor(gid)
  }

  fn lineage(&self, gid: &G) -> u64 {
    self.group_gen(gid)
  }
}

/// The log index of the synthetic snapshot baseline every forked group boots at. Index 1, not 0:
/// meta index 0 is the no-snapshot sentinel, and the baseline must push `first_index` to 2 so a
/// zero-progress joiner (match 0, next 1) satisfies `next < first_index` and is structurally
/// forced onto the snapshot path — an uncompacted fork would LOG-WALK the joiner, replaying only
/// post-fork entries onto its EMPTY state machine (silent, permanent divergence from the
/// preloaded replicas).
pub const FORK_BASE_INDEX: Index = Index::new(1);

/// The term of the fork baseline entry. Term 1, not 0: a well-formed store never holds an entry
/// above its own durable term, so the manufactured HardState carries this term and the baseline
/// meta claims it — the exact shape a real snapshot install leaves behind.
pub const FORK_BASE_TERM: Term = Term::new(1);

/// Manufacture the fork baseline in a child's fresh stores — the exact store shape a completed
/// snapshot install leaves behind, so [`Endpoint::restart`] recovers it with the install
/// machinery's own validation: HardState at the baseline term (commit at the boundary), the
/// AUTHORITATIVE blob persisted at `(FORK_BASE_INDEX, FORK_BASE_TERM)` under the boot voters,
/// and the log re-baselined so `first_index == 2` (blob-then-rebaseline, the install order).
///
/// The write ids ride the PRIOR boot epoch (`boot_epoch - 1`): restart seeds the endpoint's op
/// counter at `first_of_epoch(boot_epoch)`, so a completion from these manufacture-time writes is
/// epoch-strictly-below every id the child ever mints — it can never alias a live op in the
/// pending maps or falsely satisfy a `>=` durability watermark (the OpId epoch-major contract).
/// Same-epoch ids would collide with the child's first ops: a vote write pending at
/// `(boot_epoch, 0)` would see OUR completion and ack a not-yet-durable vote. That is exactly
/// what `boot_epoch == 0` would produce — the saturating subtraction has no prior epoch to land
/// in — so the fork constructors refuse it ([`validate_fork_boot_epoch`]) before reaching here;
/// the subtraction never actually saturates.
fn write_fork_baseline<I, L, S>(
  config: &Config<I>,
  snapshot: Bytes,
  generation: u64,
  read_only: Option<ReadOnlyOption>,
  boot_epoch: u64,
  log: &mut L,
  stable: &mut S,
) where
  I: NodeId,
  L: LogStore,
  S: StableStore<NodeId = I>,
{
  let opid = OpId::first_of_epoch(boot_epoch.saturating_sub(1));
  stable.submit_write(
    opid,
    HardState::initial()
      .with_term(FORK_BASE_TERM)
      .with_commit(FORK_BASE_INDEX),
  );
  let conf = ConfState::from_voters(config.voters().iter().map(CheapClone::cheap_clone));
  // The baseline meta carries the child's own lineage (its incarnation under the unified
  // counter, absent at 0) and — when the parent had a committed migration — the inherited read
  // mode, exactly as a real install's meta would: the restart boot below then recovers both.
  let mut meta =
    SnapshotMeta::new(FORK_BASE_INDEX, FORK_BASE_TERM, conf).with_shape_gen(generation);
  if let Some(mode) = read_only {
    meta = meta.with_read_only(mode);
  }
  stable.submit_snapshot(opid.next(), meta, snapshot);
  log.restore(FORK_BASE_INDEX, FORK_BASE_TERM);
}

/// The fork constructors' boot-epoch admission check, run BEFORE any store write: a fork must
/// boot at epoch >= 1 because [`write_fork_baseline`] issues the manufactured baseline's store
/// writes at the PRIOR epoch. At `boot_epoch == 0` there is no prior epoch — the baseline ids
/// would collapse into epoch 0, the very epoch [`Endpoint::restart`] seeds the child's own op
/// counter with, so a queued baseline `Wrote(0, 0)` completion could release a live vote or
/// campaign action whose durability it does not prove (leadership on a phantom durable
/// self-vote). Restart already CONTRACTS epochs strictly above every prior incarnation; a fork
/// additionally reserves the prior epoch for its baseline, so this floor is enforced, not
/// merely documented.
const fn validate_fork_boot_epoch(boot_epoch: u64) -> Result<(), CreateGroupError> {
  if boot_epoch == 0 {
    return Err(CreateGroupError::InvalidBootEpoch);
  }
  Ok(())
}

/// One committed, container-relayed fork: everything a driver needs to materialize the child
/// group behind its engine barrier. Yielded by [`MultiRaft::poll_pending_fork`] after the typed
/// child id decoded, the replay guard passed, and the child config was rebuilt from the parent's
/// local tuning under the fork's voter set. The fields are deliberately public — this is a
/// transfer record the driver destructures into
/// [`create_group_from_fork`](MultiRaft::create_group_from_fork).
pub struct GroupFork<G, I, F> {
  /// The parent group (the split entry rode its log).
  pub parent: G,
  /// The child group id, decoded from the committed payload.
  pub child: G,
  /// The child's incarnation under the unified lineage counter (normally 0).
  pub child_gen: u64,
  /// The parent's lineage counter after this split — already folded into the container's relay
  /// guard when this fork is yielded.
  pub parent_gen_after: u64,
  /// The child's boot config: the parent's LOCAL tuning with voters := the parent's voter set
  /// at the split entry (this host keeps its own node id).
  pub config: Config<I>,
  /// The forked state-machine half, handed to the child as its restore vessel.
  pub fsm: F,
  /// The child's authoritative recovery blob, derived at the parent's apply point
  /// (`encode(fsm.snapshot())` of the half — the two correspond by construction).
  pub blob: Bytes,
  /// The read mode the child inherits through its baseline meta (`None` for a never-migrated
  /// parent: the child falls back to its config, exactly as a restart would).
  pub read_only: Option<ReadOnlyOption>,
  /// The split entry's index in the PARENT's log — the fork durability barrier's anchor, handed
  /// back through [`MultiRaft::lift_fork_barrier`] once the child's baseline is flush-durable.
  pub split_index: Index,
}

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
  /// Groups that may have a staged pending fork to relay (see
  /// [`poll_pending_fork`](Self::poll_pending_fork)).
  dirty_forks: VecDeque<G>,
  /// The RELAY-TIME lineage view, one entry per admitted id: seeded at every admission (0 at
  /// genesis create, the DURABLE restored lineage at restore/fork) and bumped to
  /// `parent_gen_after` when a fork is relayed. This is the replay guard: a fork whose bump is
  /// at-or-below this view was already relayed (a same-gen retry duplicate, or a tail replayed
  /// under a snapshot that already carries the bump) and folds to a resolved no-op. Deliberately
  /// DISTINCT from the endpoints' live `shape_gen` (bumped at APPLY, before the relay — guarding
  /// on it would drop every first relay).
  lineage: BTreeMap<G, u64>,
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
      dirty_forks: VecDeque::new(),
      lineage: BTreeMap::new(),
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
    if self.dirty_forks.back() != Some(gid) {
      self.dirty_forks.push_back(gid.cheap_clone());
    }
  }

  /// The group's lineage counter under the unified per-id scheme (incarnation ⊔ shape), as this
  /// container knows it: the LIVE endpoint counter when hosted (it includes every applied
  /// split), else the relay-time view (a removed id's last relayed bump). `0` for an id never
  /// admitted or reshaped.
  #[must_use]
  pub fn group_gen(&self, gid: &G) -> u64 {
    let live = self.groups.get(gid).map(Endpoint::shape_gen).unwrap_or(0);
    let relayed = self.lineage.get(gid).copied().unwrap_or(0);
    live.max(relayed)
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

  /// The next committed, relay-ready fork from any group — the driver drains this every crank
  /// (BEFORE its storage crank, so the same crank's engine flush covers the materialization).
  /// Decodes the typed child id, applies the replay guard, rebuilds the child's config from the
  /// parent's local tuning, and yields a [`GroupFork`]. Folded to a RESOLVED no-op (the fork's
  /// barrier contribution is released, nothing yielded) when: the bump is at-or-below the relay
  /// guard (a retry duplicate / an already-covered replay), the child id is already hosted (the
  /// factory raced the fork — safe because any solicitation that materialized it can only have
  /// come from an already-forked member whose blob was flush-durable before it could transmit),
  /// or this host is not in the fork's voter set (a parent LEARNER applies the split — its
  /// parent half shrinks identically — but does not place the child; the embedder adds it by
  /// conf change later if wanted). A committed child id that does not decode as `G` poisons the
  /// parent (`SplitDecode` — committed-corrupt, the apply-arm's own decode class) and drops its
  /// remaining staged forks.
  pub fn poll_pending_fork(&mut self) -> Option<GroupFork<G, I, F>> {
    while let Some(gid) = self.dirty_forks.front().map(CheapClone::cheap_clone) {
      let Some(ep) = self.groups.get_mut(&gid) else {
        self.dirty_forks.pop_front();
        continue;
      };
      let Some((fork, fsm)) = ep.pop_pending_fork() else {
        self.dirty_forks.pop_front();
        continue;
      };
      let Ok(child) = G::decode_exact(fork.child_bytes.clone()) else {
        ep.poison(PoisonReason::SplitDecode);
        while ep.pop_pending_fork().is_some() {}
        self.dirty_forks.pop_front();
        continue;
      };
      let in_voters = self
        .host_id
        .as_ref()
        .is_some_and(|host| fork.voters.contains(host));
      if !in_voters {
        if let Some(ep) = self.groups.get_mut(&gid) {
          ep.resolve_fork(fork.index);
        }
        continue;
      }
      if fork.parent_gen_after <= self.lineage.get(&gid).copied().unwrap_or(0) {
        if let Some(ep) = self.groups.get_mut(&gid) {
          ep.resolve_fork(fork.index);
        }
        continue;
      }
      self
        .lineage
        .insert(gid.cheap_clone(), fork.parent_gen_after);
      if self.groups.contains_key(&child) {
        if let Some(ep) = self.groups.get_mut(&gid) {
          ep.resolve_fork(fork.index);
        }
        continue;
      }
      // Rebuild the child's boot config: the parent's local tuning under the fork's voter set.
      // The voter-membership check above makes `IdNotAVoter` unreachable; the arm is defensive
      // (resolve rather than wedge the queue).
      let Some(config) = self
        .groups
        .get(&gid)
        .and_then(|ep| ep.config().with_voter_set(fork.voters.clone()).ok())
      else {
        if let Some(ep) = self.groups.get_mut(&gid) {
          ep.resolve_fork(fork.index);
        }
        continue;
      };
      return Some(GroupFork {
        parent: gid,
        child,
        child_gen: fork.child_gen,
        parent_gen_after: fork.parent_gen_after,
        config,
        fsm,
        blob: fork.blob,
        read_only: fork.read_only,
        split_index: fork.index,
      });
    }
    None
  }

  /// Resolve the fork staged at exactly `split_index` on `parent`: the driver reports the
  /// child's baseline flush-durable behind its engine barrier (or a relayed fork it abandoned),
  /// and the parent's snapshot fence over that index releases. Exact-index semantics — see
  /// [`GroupFork::split_index`]; resolving one fork never frees an older, still-pending one.
  pub fn lift_fork_barrier(&mut self, parent: &G, split_index: Index) {
    if let Some(ep) = self.groups.get_mut(parent) {
      ep.resolve_fork(split_index);
    }
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
    // Genesis: reset the relay-time lineage view (a stale entry from an earlier same-uptime
    // incarnation must not shadow this admission — every admission reseeds it).
    self.lineage.insert(gid.cheap_clone(), 0);
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
    // Seed the relay guard from the DURABLE lineage (the restored snapshot meta's `shape_gen`),
    // NOT the live counter: the restart replay may have re-staged a not-yet-materialized fork,
    // re-bumping the live counter — the guard must relay that fork again, not drop it.
    self
      .lineage
      .insert(gid.cheap_clone(), ep.restored_lineage());
    self.groups.insert(gid.cheap_clone(), ep);
    // The dirty marks cover the replayed forks (message/event replay is deliberately cleared by
    // `Endpoint::restart`, so those two queues mark empty).
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
    self
      .lineage
      .insert(gid.cheap_clone(), ep.restored_lineage());
    self.groups.insert(gid.cheap_clone(), ep);
    self.mark_dirty(&gid);
    Ok(())
  }

  /// Create a group born from LOCALLY-FORKED state: a manufactured snapshot install. The
  /// baseline — meta `(`[`FORK_BASE_INDEX`]`, `[`FORK_BASE_TERM`]`)`, the caller's `snapshot`
  /// blob, the log compacted through the boundary — is written into the FRESH `log`/`stable`
  /// first, and the group then boots through the [`Endpoint::restart`] path, inheriting its
  /// boundary validation, poison discipline, and applied/commit derivation wholesale. Because
  /// `first_index` starts at 2, a zero-progress joiner added later is structurally forced onto
  /// the snapshot path and receives exactly the persisted blob — never a log walk onto its empty
  /// state machine.
  ///
  /// **The blob is authoritative.** `fsm` is the restore VESSEL — restart overwrites it from the
  /// blob, so a caller-supplied fsm/blob mismatch cannot diverge replicas (the blob wins
  /// identically everywhere; their equality is an efficiency contract, not a safety one). A blob
  /// that fails boundary/decode/restore POISONS the group exactly as a corrupt durable snapshot
  /// would at crash-restart — construction still returns `Ok`, and co-hosted groups are
  /// untouched.
  ///
  /// A fork is a LOCAL act by an already-authorized replica: it is never solicited over the
  /// wire, and no factory path reaches it — a catalog that marks ids fork-born should DECLINE
  /// solicitations for them. Same `boot_epoch` contract as
  /// [`restore_group`](Self::restore_group) (strictly above every prior incarnation of this
  /// group on this node — a re-fork after removal is a later incarnation), with its floor
  /// ENFORCED here: `boot_epoch == 0` is refused, because the manufactured baseline's store
  /// writes ride the prior epoch and epoch 0 has none — the baseline's completions would land
  /// in the child's own first live epoch and could release a vote/campaign action they do not
  /// prove durable. The stores must be FRESH: the baseline overwrites whatever they hold. On a
  /// stable store whose `hard_state()` lags submitted writes to a durability barrier, the child
  /// boots at the store's PRIOR durable term (the baseline meta alone drives the applied/commit
  /// derivation, so the boot is unchanged otherwise) and the manufactured term becomes durable
  /// at the next barrier — the crash-recovery shape is the spec'd one either way.
  ///
  /// # Errors
  /// The same admission checks as [`create_group`](Self::create_group) — see
  /// [`CreateGroupError`] — plus [`CreateGroupError::InvalidBootEpoch`] when `boot_epoch == 0`
  /// (a fork's baseline needs the prior epoch to itself). Refusal happens BEFORE any store
  /// write.
  #[allow(clippy::too_many_arguments)]
  pub fn create_group_from_fork<L, S>(
    &mut self,
    gid: G,
    generation: u64,
    config: Config<I>,
    now: impl Into<Now>,
    seed: u64,
    fsm: F,
    snapshot: Bytes,
    read_only: Option<ReadOnlyOption>,
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
    validate_fork_boot_epoch(boot_epoch)?;
    validate_new_group(&self.groups, &self.host_id, &gid, &config)?;
    self.host_id.get_or_insert(config.id());
    // `generation` (the child's incarnation under the unified lineage counter) and the
    // inherited `read_only` provenance ride the baseline meta, so the restart boot below — and
    // every later restart from the child's own stores — recovers both exactly as it would from
    // a real install (absent at 0 / `None`: byte-identical to the pre-reshaping baseline).
    write_fork_baseline(
      &config, snapshot, generation, read_only, boot_epoch, log, stable,
    );
    let ep = Endpoint::restart(
      config,
      now,
      group_seed(seed, &gid),
      fsm,
      boot_epoch,
      log,
      stable,
    );
    self
      .lineage
      .insert(gid.cheap_clone(), ep.restored_lineage());
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
    self.lineage.insert(gid.cheap_clone(), 0);
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
    self
      .lineage
      .insert(gid.cheap_clone(), ep.restored_lineage());
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
    self
      .lineage
      .insert(gid.cheap_clone(), ep.restored_lineage());
    self.groups.insert(gid.cheap_clone(), ep);
    self.mark_dirty(&gid);
    Ok(())
  }

  /// Create a group from locally forked state with a caller-supplied RNG (see
  /// [`create_group_from_fork`](Self::create_group_from_fork) for the manufactured-install
  /// contract and [`Endpoint::restart_with_rng`] for the RNG one).
  ///
  /// # Errors
  /// The admission checks of [`CreateGroupError`], including
  /// [`CreateGroupError::InvalidBootEpoch`] when `boot_epoch == 0` (a fork's baseline needs the
  /// prior epoch to itself); refusal happens BEFORE any store write.
  #[allow(clippy::too_many_arguments)]
  pub fn create_group_from_fork_with_rng<L, S>(
    &mut self,
    gid: G,
    generation: u64,
    config: Config<I>,
    now: impl Into<Now>,
    rng: R,
    fsm: F,
    snapshot: Bytes,
    read_only: Option<ReadOnlyOption>,
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
    validate_fork_boot_epoch(boot_epoch)?;
    validate_new_group(&self.groups, &self.host_id, &gid, &config)?;
    self.host_id.get_or_insert(config.id());
    write_fork_baseline(
      &config, snapshot, generation, read_only, boot_epoch, log, stable,
    );
    let ep = Endpoint::restart_with_rng(config, now, rng, fsm, boot_epoch, log, stable);
    self
      .lineage
      .insert(gid.cheap_clone(), ep.restored_lineage());
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
    F::Snapshot: Data,
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

  /// Propose a group SPLIT on `gid` (the parent): a committed `Split` entry deterministically
  /// forks `child` out of the parent's state machine on every replica. Leader-only, on the
  /// parent's own log; the payload carries the child id (G-free on the wire), `child_gen` (the
  /// child's incarnation — normally 0), the parent's bumped lineage (computed here: the live
  /// counter + 1, the replay-guard/idempotence anchor), and the embedder's opaque
  /// `instruction`, bounded by the single-frame append check. `None` if no such group.
  ///
  /// Gate order: poisoned → leader → joint config → hosted child → child-id wire bound → the
  /// ordinary admin append (whose refusals pass through as [`SplitError::Propose`]).
  /// `BelowFloor` is produced by the COORDINATOR delegators through their floor seam, and
  /// `CrossPlane` by a sharded host's handle — the container stays floor- and plane-free.
  #[must_use = "`None` means no group with this id is hosted — nothing was proposed"]
  #[allow(clippy::too_many_arguments)]
  pub fn propose_split<L, S>(
    &mut self,
    gid: &G,
    now: impl Into<Now>,
    log: &mut L,
    // Vestigial, as on the whole propose family: kept so the delegators thread `&stable`.
    _stable: &S,
    child: &G,
    child_gen: u64,
    instruction: Bytes,
  ) -> Option<Result<Index, SplitError<I>>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    {
      let ep = self.groups.get(gid)?;
      if ep.is_poisoned() {
        return Some(Err(SplitError::Propose(ProposeError::Poisoned)));
      }
      if !ep.role().is_leader() {
        return Some(Err(SplitError::NotLeader {
          leader: ep.leader(),
        }));
      }
      // A joint parent would hand the child an ambiguous bootstrap voter set: refuse at
      // propose (the one-line rule that removes the hairiest interleaving).
      if ep.conf_state().is_joint() {
        return Some(Err(SplitError::JointConfig));
      }
    }
    // A hosted child id (the parent's own included) can never be forked into existence here.
    if self.groups.contains_key(child) {
      return Some(Err(SplitError::ChildExists));
    }
    // Refuse an out-of-bound child encoding BEFORE it can be committed: every replica's relay
    // decode would otherwise poison the parent on a committed entry.
    let mut child_bytes = Vec::new();
    child.encode(&mut child_bytes);
    if child_bytes.is_empty() || child_bytes.len() > crate::wire::MAX_GROUP_ID_LEN {
      return Some(Err(SplitError::InvalidChild));
    }
    let ep = self.groups.get_mut(gid)?;
    // The bump is computed from the LIVE counter so back-to-back splits chain (each apply
    // bumps it before the next propose reads it). Lineage exhaustion is unreachable before
    // log-index exhaustion — every bump consumes a log index — so no ceiling check rides here.
    let parent_gen_after = ep.shape_gen() + 1;
    let payload = SplitPayload::new(
      Bytes::from(child_bytes),
      child_gen,
      parent_gen_after,
      instruction,
    );
    let mut buf = Vec::new();
    crate::wire::encode_split_payload(&payload, &mut buf);
    let result = ep
      .propose_split_entry(now, log, Bytes::from(buf))
      .map_err(SplitError::Propose);
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
