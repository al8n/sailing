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
  CommitMergePayload, ConfChange, ConfChangeV2, ConfState, Config, CreateGroupError, Data,
  Endpoint, EntryKind, Event, HardState, Index, Instant, LogStore, MergeError, Message, NodeId,
  Now, OpId, Outgoing, PoisonReason, PrepareMergePayload, Prng, ProposeError, ReadIndexError,
  ReadOnlyOption, RollbackMergePayload, SnapshotMeta, SplitError, SplitPayload, StableStore,
  StateMachine, StorageProgress, Term, TransferError, endpoint::MergeWindow,
};
use bytes::Bytes;
use cheap_clone::CheapClone;
use core::time::Duration;
use std::{
  collections::{BTreeMap, BTreeSet, VecDeque},
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

/// Per-group storage a [`MultiStreamCoordinator`] uses to drive each group's endpoint when inbound
/// bytes span multiple groups. The caller implements it over its own per-group store table.
///
/// CONTRACT: resolution must be STABLE (the same group always resolves to the same stores) and
/// NON-ALIASING (two groups must never share a store — a shared log is a safety violation).
/// Returning `Some` for a group the `MultiRaft` does not host is a harmless per-message drop;
/// returning `None` for a hosted group starves it.
pub trait GroupStores<G, L, S> {
  /// The `(log, stable)` stores for `group`, or `None` if this host has no storage for it — an
  /// inbound message for an unknown group is then dropped (the sender retries on its own cadence).
  fn stores(&mut self, group: &G) -> Option<(&mut L, &mut S)>;
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

/// The fork constructors' used-storage admission check, run BEFORE any store write:
/// [`write_fork_baseline`] OVERWRITES whatever the stores hold, so a fork is only ever written
/// over VIRGIN stores — no visible hard state, no snapshot slot, no log content (an empty,
/// never-re-baselined log). A used incarnation's stores here mean a replayed fork raced the
/// child's real durable progress (a crash restored the parent while the child stayed unhosted);
/// overwriting would destroy that progress, so the fork refuses and the child reaches this host
/// by restore instead. The legitimate crash-BEFORE-flush replay is untouched: nothing of the
/// child ever became durable, the stores are virgin, and re-materialization stays idempotent.
fn validate_virgin_stores<L, S>(log: &L, stable: &S) -> Result<(), CreateGroupError>
where
  L: LogStore,
  S: StableStore,
  S::NodeId: PartialEq,
{
  let used = stable.hard_state() != HardState::initial()
    || stable.snapshot().is_some()
    || log.last_index() > Index::ZERO
    || log.first_index() > Index::new(1);
  if used {
    return Err(CreateGroupError::StorageInUse);
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

/// One committed, container-relayed merge ABORT: a target-side abort entry applied on `target`,
/// and the frozen `source` it names must now be THAWED on its own log — the driver proposes the
/// source-side `RollbackMerge` there ([`MultiRaft::propose_merge_unfreeze`]), so the thaw stays
/// log-borne for restart re-derivation. Yielded by
/// [`MultiRaft::poll_pending_merge_abort`] once per abort apply per host; the relay is
/// best-effort BY DESIGN (only the host whose local source replica leads can propose — every
/// other host's attempt dies typed at the source's own gates), and a relay lost to lifecycle
/// churn is recovered by re-proposing the target-side abort (a fresh entry, a fresh relay).
pub struct MergeAbortRelay<G> {
  /// The target group whose log carried the abort entry.
  pub target: G,
  /// The frozen source group to thaw, decoded from the abort's payload.
  pub source: G,
  /// The freeze generation the abort abandoned (observability; the thaw's own mint re-reads
  /// the live frozen state at propose).
  pub source_gen_after: u64,
}

/// One resolved parked merge from a [`MultiRaft::service_merge_applies`] crank — what the
/// DRIVER folds into its storage engine and lifecycle teardown. The container already did the
/// consensus-side work (the absorb or the deterministic abort, the events, the source
/// endpoint's removal on a merge); the driver owns the storage half: persist
/// `floor(source) = `[`MERGED_FLOOR`] and drop the source's stores for a `Merged`, nothing for
/// an `Aborted` (the source group is still live — its log settled the race).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeResolution<G> {
  /// The target ABSORBED the source: the source endpoint is gone from this container, the
  /// target's forced absorb capture is staged, and the source id must now be floored terminally
  /// and its stores dropped — behind the SAME barrier the capture rides.
  Merged {
    /// The absorbed source group.
    source: G,
    /// The absorbing target group.
    target: G,
  },
  /// The parked commit resolved as a deterministic NO-OP (the source's log settled the race, or
  /// the commit was a replayed duplicate). Both groups remain exactly as they were.
  Aborted {
    /// The named source group.
    source: G,
    /// The target group whose parked apply no-op'd.
    target: G,
  },
}

/// The outcome of one head-fork examination (see `MultiRaft::poll_pending_fork`): the parent's
/// queue was empty (or the parent gone / poisoned on a corrupt child id), the head fork was
/// consumed by a resolution arm, it parked on a hosted-child conflict, or it yielded for
/// materialization.
// Transient: matched and consumed on the stack within one drain step, never stored — the
// unit-vs-`GroupFork` size spread costs nothing, and boxing would allocate per relayed fork.
#[allow(clippy::large_enum_variant)]
enum HeadFork<G, I, F> {
  Empty,
  Resolved,
  Parked,
  Yield(GroupFork<G, I, F>),
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
  /// TARGETS that may have a staged source-unfreeze relay to drain (see
  /// [`poll_pending_merge_abort`](Self::poll_pending_merge_abort)).
  dirty_aborts: VecDeque<G>,
  /// Parents whose HEAD fork is PARKED on a hosted-child conflict (see
  /// [`poll_pending_fork`](Self::poll_pending_fork)): the fork stays staged (blob retained, the
  /// snapshot fence armed, the relay guard unmoved) and is re-examined at the top of every relay
  /// drain — the resolution triggers are CHILD-side (removal, catch-up), so no parent dispatch
  /// re-marks these. Membership doubles as the conflict-signal dedupe: one
  /// [`poll_split_conflict`](Self::poll_split_conflict) signal per park episode.
  parked: BTreeSet<G>,
  /// Pending `(parent, child)` split-conflict signals, pushed once per park episode and HELD
  /// until consumed: a driver publishing on a bounded tail peeks
  /// ([`peek_split_conflict`](Self::peek_split_conflict)), publishes, and consumes only on
  /// acceptance ([`poll_split_conflict`](Self::poll_split_conflict)), so backpressure defers
  /// the episode's only cue instead of erasing it. Every arm that ends a park purges its
  /// still-queued signal ([`unpark`](Self::unpark)) — delivered after resolution it would be
  /// stale — so queued signals always name currently-parked parents.
  conflicts: VecDeque<(G, G)>,
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
      dirty_aborts: VecDeque::new(),
      parked: BTreeSet::new(),
      conflicts: VecDeque::new(),
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
    // A parked PARENT's staged forks die with its endpoint (removal is the embedder's explicit
    // destruction of this replica), so the park bookkeeping — a still-queued conflict signal
    // included — dies too. Removing a parked fork's CHILD needs nothing here: the next relay
    // drain re-examines the park and materializes.
    self.unpark(gid);
    self.groups.remove(gid)
  }

  /// End `gid`'s park episode: leave the parked set and purge any still-queued conflict
  /// signal. Every arm that resolves a park routes through here, so an UNDELIVERED signal (one
  /// a full driver tail deferred) dies with its episode — delivered afterwards it would be
  /// stale, capable of goading the embedder into removing the very child the resolution just
  /// materialized or blessed.
  fn unpark(&mut self, gid: &G) {
    // Signals are queued only while their parent is parked (the queue invariant this helper
    // maintains), so a no-op removal proves there is nothing to purge.
    if self.parked.remove(gid) {
      self.conflicts.retain(|(parent, _)| parent != gid);
    }
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
    if self.dirty_aborts.back() != Some(gid) {
      self.dirty_aborts.push_back(gid.cheap_clone());
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

  /// Whether a split IN FLIGHT on this host reserves `gid` as its child id: some hosted group
  /// has a proposed-but-unapplied split naming it (the leader's propose→apply window), or a
  /// committed fork naming it staged in the relay queue — parked conflicts included. The
  /// coordinators refuse create/restore/fork admission of a reserved id and the drivers'
  /// factory pre-build gate declines it, closing the window the propose-time `ChildExists`
  /// check cannot see; an id admitted anyway (before the split arrived) is the parked-conflict
  /// case [`poll_pending_fork`](Self::poll_pending_fork) holds safe. Purely derived from live
  /// consensus state, so it releases by construction at every resolution: a stale-mint apply
  /// ends the propose window, a resolution arm consumes the staged fork, a yield hands it to
  /// the driver — whose materialization of that very fork therefore passes this predicate —
  /// and a park keeps it held until the conflict resolves.
  #[must_use]
  pub fn split_reserved(&self, gid: &G) -> bool {
    let mut bytes = Vec::new();
    gid.encode(&mut bytes);
    self.groups.values().any(|ep| ep.split_reserves(&bytes))
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
  /// guard (a retry duplicate / an already-covered replay), or this host is not in the fork's
  /// voter set (a parent LEARNER applies the split — its parent half shrinks identically — but
  /// does not place the child; the embedder adds it by conf change later if wanted). A committed
  /// child id that does not decode as `G` poisons the parent (`SplitDecode` — committed-corrupt,
  /// the apply-arm's own decode class) and drops its remaining staged forks.
  ///
  /// A fork whose child id is ALREADY HOSTED here is PARKED, never dropped: the parent's
  /// `fsm.split` already ran at apply — the parent SHRANK — so the staged blob is the
  /// partition's only local copy, and the pre-park behavior (resolve as a no-op) silently lost
  /// it whenever the child was admitted between the propose-time `ChildExists` gate and this
  /// relay (the coordinators' reservation narrows that window, but a child admitted BEFORE the
  /// split applied remains reachable). Parked means: the fork stays queued at the head (its
  /// parent's later forks wait behind it — relaying past it would advance the replay guard over
  /// it and fold it to a duplicate), the relay guard does not advance, the snapshot fence does
  /// not lift, and one `(parent, child)` conflict signal surfaces via
  /// [`poll_split_conflict`](Self::poll_split_conflict). Every drain re-examines parked forks
  /// first and resolves by exactly one of: (a) the hosted child is REMOVED — the fork
  /// materializes normally; (b) the hosted child reaches `applied >=` [`FORK_BASE_INDEX`] with
  /// lineage equal to the fork's `child_gen` — the same-logical-fork twin (materialized from a
  /// sibling replica whose own blob was flush-durable before it could transmit), so the fork
  /// data provably exists cluster-wide and this fork resolves as redundant (fence lifts, guard
  /// advances, blob discarded — now safe); (c) otherwise it stays parked. Parking cannot
  /// deadlock recovery: the conflict signal is the embedder's cue, and the standing fence means
  /// the parent cannot compact past the split entry while parked — the fork's replay source
  /// survives indefinitely, so resolution stays possible no matter how late the embedder acts.
  pub fn poll_pending_fork(&mut self) -> Option<GroupFork<G, I, F>> {
    // Parked parents first: their resolution triggers are child-side, so the dirty queue —
    // marked only by parent dispatches — cannot be relied on to revisit them.
    let parked: Vec<G> = self.parked.iter().map(CheapClone::cheap_clone).collect();
    for gid in parked {
      match self.examine_head_fork(&gid) {
        HeadFork::Empty => {
          self.unpark(&gid);
        }
        HeadFork::Resolved => {
          // Arm (b): the head fork resolved as redundant — later forks of this parent flow
          // through the ordinary drain below.
          self.unpark(&gid);
          self.dirty_forks.push_back(gid);
        }
        HeadFork::Parked => {}
        HeadFork::Yield(fork) => {
          // Arm (a): the squatter is gone and the fork materializes normally.
          self.unpark(&gid);
          self.dirty_forks.push_back(gid);
          return Some(fork);
        }
      }
    }
    while let Some(gid) = self.dirty_forks.front().map(CheapClone::cheap_clone) {
      // A parked parent's queue is owned by the sweep above (head-of-line by design).
      if self.parked.contains(&gid) {
        self.dirty_forks.pop_front();
        continue;
      }
      match self.examine_head_fork(&gid) {
        HeadFork::Empty | HeadFork::Parked => {
          self.dirty_forks.pop_front();
        }
        // Re-examine the same parent: its next staged fork (if any) is now at the head.
        HeadFork::Resolved => {}
        HeadFork::Yield(fork) => return Some(fork),
      }
    }
    None
  }

  /// Examine (and where possible resolve or yield) `gid`'s HEAD staged fork — the one shared
  /// arm evaluation both [`poll_pending_fork`](Self::poll_pending_fork) phases run. The head
  /// fork is consumed only on a resolution or a yield; a park leaves it staged untouched.
  fn examine_head_fork(&mut self, gid: &G) -> HeadFork<G, I, F> {
    enum Verdict<G> {
      Poison,
      /// Resolve the head fork's barrier and consume it (duplicate / non-member arms).
      Resolve,
      /// Park on a hosted-child conflict (arm (c)), surfacing the signal on a fresh park.
      Park(G),
      /// Resolve as redundant — the hosted twin provably carries the fork data (arm (b)).
      Redundant,
      /// No conflict: advance the guard and yield (the config rebuild may still refuse).
      Yield(G),
    }
    let verdict = {
      let Some(ep) = self.groups.get(gid) else {
        return HeadFork::Empty;
      };
      let Some(fork) = ep.peek_pending_fork() else {
        return HeadFork::Empty;
      };
      if let Ok(child) = G::decode_exact(fork.child_bytes.clone()) {
        let in_voters = self
          .host_id
          .as_ref()
          .is_some_and(|host| fork.voters.contains(host));
        if !in_voters || fork.parent_gen_after <= self.lineage.get(gid).copied().unwrap_or(0) {
          Verdict::Resolve
        } else if let Some(hosted) = self.groups.get(&child) {
          // Arm (b): a twin at-or-past the manufactured baseline under the fork's own lineage
          // IS this fork (single-incarnation ids), materialized from a sibling whose blob was
          // flush-durable before it could transmit — discarding the local copy loses nothing.
          if hosted.applied_index() >= FORK_BASE_INDEX && hosted.shape_gen() == fork.child_gen {
            Verdict::Redundant
          } else {
            Verdict::Park(child)
          }
        } else {
          Verdict::Yield(child)
        }
      } else {
        Verdict::Poison
      }
    };
    match verdict {
      Verdict::Poison => {
        if let Some(ep) = self.groups.get_mut(gid) {
          ep.poison(PoisonReason::SplitDecode);
          while ep.pop_pending_fork().is_some() {}
        }
        HeadFork::Empty
      }
      Verdict::Resolve => {
        if let Some(ep) = self.groups.get_mut(gid)
          && let Some((fork, _fsm)) = ep.pop_pending_fork()
        {
          ep.resolve_fork(fork.index);
        }
        HeadFork::Resolved
      }
      Verdict::Redundant => {
        if let Some(ep) = self.groups.get_mut(gid)
          && let Some((fork, _fsm)) = ep.pop_pending_fork()
        {
          ep.resolve_fork(fork.index);
          self
            .lineage
            .insert(gid.cheap_clone(), fork.parent_gen_after);
        }
        HeadFork::Resolved
      }
      Verdict::Park(child) => {
        // Set-insert is the dedupe: one conflict signal per park episode, re-armed only after
        // a resolution removes the parent from the parked set.
        if self.parked.insert(gid.cheap_clone()) {
          self.conflicts.push_back((gid.cheap_clone(), child));
        }
        HeadFork::Parked
      }
      Verdict::Yield(child) => {
        // Rebuild the child's boot config: the parent's local tuning under the fork's voter
        // set. The voter-membership check above makes `IdNotAVoter` unreachable; the arm is
        // defensive (resolve rather than wedge the queue).
        let config = self.groups.get(gid).and_then(|ep| {
          let voters = ep.peek_pending_fork()?.voters.clone();
          ep.config().with_voter_set(voters).ok()
        });
        let Some(ep) = self.groups.get_mut(gid) else {
          return HeadFork::Empty;
        };
        let Some((fork, fsm)) = ep.pop_pending_fork() else {
          return HeadFork::Empty;
        };
        let Some(config) = config else {
          ep.resolve_fork(fork.index);
          self
            .lineage
            .insert(gid.cheap_clone(), fork.parent_gen_after);
          return HeadFork::Resolved;
        };
        self
          .lineage
          .insert(gid.cheap_clone(), fork.parent_gen_after);
        HeadFork::Yield(GroupFork {
          parent: gid.cheap_clone(),
          child,
          child_gen: fork.child_gen,
          parent_gen_after: fork.parent_gen_after,
          config,
          fsm,
          blob: fork.blob,
          read_only: fork.read_only,
          split_index: fork.index,
        })
      }
    }
  }

  /// The next `(parent, child)` SPLIT-CONFLICT signal, left queued — the DELIVERED-BEFORE-
  /// CONSUMED half for a driver publishing on a bounded tail: peek, publish, and only on
  /// acceptance consume via [`poll_split_conflict`](Self::poll_split_conflict). The signal is
  /// one-shot per park episode, so consuming it ahead of a refusable send would let a
  /// momentarily-full tail erase the embedder's only cue while the fence stands and the child
  /// id stays reserved; peeking leaves it queued for the next drain instead.
  #[must_use]
  pub fn peek_split_conflict(&self) -> Option<(G, G)> {
    self
      .conflicts
      .front()
      .map(|(parent, child)| (parent.cheap_clone(), child.cheap_clone()))
  }

  /// Drain the next `(parent, child)` SPLIT-CONFLICT signal: a committed fork PARKED because
  /// its child id is already hosted here (see [`poll_pending_fork`](Self::poll_pending_fork)).
  /// One signal per park episode, held until consumed HERE. A synchronous embedder (the sim
  /// worlds) consumes directly — its consumption IS delivery; a driver publishing on a BOUNDED
  /// tail must [`peek_split_conflict`](Self::peek_split_conflict) first and consume only after
  /// the tail accepts, so backpressure defers the one-shot cue rather than erasing it. A park
  /// that resolves before consumption purges its queued signal (a later delivery would be
  /// stale); the parked fork itself, not this signal, is the load-bearing state. The embedder
  /// resolves by removing the hosted child (the fork then materializes) or by letting the twin
  /// catch up (the fork then resolves as redundant); until then the parent's snapshot fence
  /// holds its replay source.
  pub fn poll_split_conflict(&mut self) -> Option<(G, G)> {
    self.conflicts.pop_front()
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

  /// The next committed, relay-ready merge ABORT from any target (see [`MergeAbortRelay`]):
  /// drained by the driver every crank, which then proposes the SOURCE-side thaw via
  /// [`propose_merge_unfreeze`](Self::propose_merge_unfreeze) over the source's own stores.
  /// Restart replay re-stages these (an already-thawed source refuses the duplicate typed); a
  /// committed source id that does not decode as `G` poisons the target — the committed-corrupt
  /// class every relay decode shares.
  pub fn poll_pending_merge_abort(&mut self) -> Option<MergeAbortRelay<G>> {
    while let Some(gid) = self.dirty_aborts.front().map(CheapClone::cheap_clone) {
      let Some(ep) = self.groups.get_mut(&gid) else {
        self.dirty_aborts.pop_front();
        continue;
      };
      let Some(relay) = ep.pop_pending_abort() else {
        self.dirty_aborts.pop_front();
        continue;
      };
      match G::decode_exact(relay.source_bytes.clone()) {
        Ok(source) => {
          return Some(MergeAbortRelay {
            target: gid,
            source,
            source_gen_after: relay.source_gen_after,
          });
        }
        Err(_) => {
          ep.poison(PoisonReason::MergeDecode);
          while ep.pop_pending_abort().is_some() {}
        }
      }
    }
    None
  }

  /// Raise `gid`'s relay-time replay guard to at least `lineage` — the restore arms' seam for
  /// the DURABLE lineage record the host's engine keeps beside the group's stores. The restore
  /// constructors seed the guard from the restored snapshot meta alone, but that meta can LAG
  /// what this host already materialized: a driver flushes the parent's lineage record together
  /// with each fork's child baseline (one barrier), while the parent's next snapshot — the only
  /// thing that folds the bump into the meta — may never have happened before the crash. A
  /// parent restored in that window replays its split entries and re-stages their forks; under
  /// the meta-only seed the container would relay them again, and materializing one against an
  /// unhosted child would aim a manufactured baseline at the child's REAL durable progress.
  /// Feeding the engine's record here folds those already-durable forks to resolved no-ops
  /// instead. Monotone (never lowers) and a no-op for an unhosted `gid`, so a lineage-less
  /// floor store leaves the snapshot-seeded guard exactly as it was.
  pub fn raise_relay_guard(&mut self, gid: &G, lineage: u64) {
    if !self.groups.contains_key(gid) {
      return;
    }
    if let Some(guard) = self.lineage.get_mut(gid) {
      *guard = (*guard).max(lineage);
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
  /// prove durable. The stores must be VIRGIN, and that too is ENFORCED
  /// ([`CreateGroupError::StorageInUse`]): the baseline overwrites whatever the stores hold, so
  /// a fork over a used incarnation's storage — a replayed split racing the child's real
  /// durable progress after a parent-only restore — would destroy that progress; only the
  /// crash-before-flush replay, whose stores hold nothing durable, may re-fork (idempotently).
  /// On a stable store whose `hard_state()` lags submitted writes to a durability barrier, the
  /// child boots at the store's PRIOR durable term (the baseline meta alone drives the
  /// applied/commit derivation, so the boot is unchanged otherwise) and the manufactured term
  /// becomes durable at the next barrier — the crash-recovery shape is the spec'd one either
  /// way.
  ///
  /// # Errors
  /// The same admission checks as [`create_group`](Self::create_group) — see
  /// [`CreateGroupError`] — plus [`CreateGroupError::InvalidBootEpoch`] when `boot_epoch == 0`
  /// (a fork's baseline needs the prior epoch to itself) and
  /// [`CreateGroupError::StorageInUse`] when the stores already hold state (a fork never
  /// overwrites used storage). Refusal happens BEFORE any store write.
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
    validate_virgin_stores(log, stable)?;
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
  /// prior epoch to itself) and [`CreateGroupError::StorageInUse`] over non-virgin stores;
  /// refusal happens BEFORE any store write.
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
    validate_virgin_stores(log, stable)?;
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
  /// Gate order: poisoned → leader → joint config → split in flight → hosted child → child-id
  /// wire bound → the ordinary admin append (whose refusals pass through as
  /// [`SplitError::Propose`]). `BelowFloor` is produced by the COORDINATOR delegators through
  /// their floor seam, and `CrossPlane` by a sharded host's handle — the container stays floor-
  /// and plane-free.
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
      // A frozen (or freezing) parent must not fork: the split would mutate the FSM above the
      // freeze boundary, breaking the absorb's nothing-above-the-freeze determinism — the same
      // gate class as propose/conf-change/read, applied to the one admin verb that had slipped
      // it (the split's own lineage guard already no-ops the pre-freeze in-flight shapes).
      if ep.merge_freeze_active() {
        return Some(Err(SplitError::Propose(ProposeError::Frozen)));
      }
      // A joint parent would hand the child an ambiguous bootstrap voter set: refuse at
      // propose (the one-line rule that removes the hairiest interleaving).
      if ep.conf_state().is_joint() {
        return Some(Err(SplitError::JointConfig));
      }
      // One split in flight at a time: the mint below reads the LIVE counter, which bumps only
      // when the earlier split APPLIES — a second mint before then duplicates it, and the
      // duplicate can only no-op at the apply-time lineage guard (its child's half would
      // otherwise be given up by the parent and never materialized). See
      // [`SplitError::SplitInFlight`] for the self-healing derivation.
      if ep.split_in_flight() {
        return Some(Err(SplitError::SplitInFlight));
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
    // The bump is computed from the LIVE counter, whose sole bump site is the earlier split's
    // APPLY — the in-flight gate above holds new proposals until then, so consecutive mints
    // chain instead of duplicating. Lineage exhaustion is unreachable before log-index
    // exhaustion — every bump consumes a log index — so no ceiling check rides here.
    let parent_gen_after = ep.shape_gen() + 1;
    let child_bytes = Bytes::from(child_bytes);
    let payload = SplitPayload::new(
      child_bytes.clone(),
      child_gen,
      parent_gen_after,
      instruction,
    );
    let mut buf = Vec::new();
    crate::wire::encode_split_payload(&payload, &mut buf);
    let result = ep
      .propose_split_entry(now, log, child_bytes, Bytes::from(buf))
      .map_err(SplitError::Propose);
    self.mark_dirty(gid);
    Some(result)
  }

  /// Propose a merge FREEZE on `source` (the group that will be absorbed): a committed
  /// `PrepareMerge` freezes it on every replica so `target` can absorb it at the boundary.
  /// Leader-proposed on the SOURCE's own log. The preconditions are checked against the LOCAL
  /// replicas (colocation makes them representative; every parked apply re-checks the facts
  /// that matter from its own log): identical voter sets, both non-joint, same active read
  /// mode, no membership change in flight on either side, and the source not already frozen or
  /// freezing. `None` if no group `source` is hosted.
  ///
  /// Floor refusals (`MergeError::BelowFloor`) are the COORDINATOR delegators' leg through
  /// their per-call floor seam, and `CrossPlane` the sharded handle's — the container stays
  /// floor- and plane-free, exactly as it is for splits.
  #[must_use = "`None` means no group with this id is hosted — nothing was proposed"]
  pub fn prepare_merge<L, S>(
    &mut self,
    source: &G,
    now: impl Into<Now>,
    log: &mut L,
    // Vestigial, as on the whole propose family: kept so the delegators thread `&stable`.
    _stable: &S,
    target: &G,
  ) -> Option<Result<Index, MergeError<I>>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    if !self.groups.contains_key(source) {
      return None;
    }
    if source == target {
      return Some(Err(MergeError::SelfMerge));
    }
    let Some(tep) = self.groups.get(target) else {
      return Some(Err(MergeError::TargetMissing));
    };
    // A frozen (or freezing) target is being dissolved itself — it can absorb nothing.
    if tep.merge_freeze_active() {
      return Some(Err(MergeError::AlreadyFrozen));
    }
    let target_conf = tep.conf_state();
    let target_mode = tep.active_read_mode();
    let target_conf_in_flight = tep.conf_change_in_flight();
    let sep = self.groups.get(source).expect("checked hosted above");
    if sep.is_poisoned() {
      return Some(Err(MergeError::Propose(ProposeError::Poisoned)));
    }
    if !sep.role().is_leader() {
      return Some(Err(MergeError::NotLeader {
        leader: sep.leader(),
      }));
    }
    if sep.merge_freeze_active() {
      return Some(Err(MergeError::AlreadyFrozen));
    }
    // A source that is itself mid-ABSORB (a CommitMerge in flight or parked as a target)
    // must finish that first: freezing it would mint a source generation the pending absorb
    // is about to move, and the two verbs' entries would race on one counter.
    if sep.commit_merge_in_flight() || sep.pending_merge().is_some() {
      return Some(Err(MergeError::AlreadyPending));
    }
    let source_conf = sep.conf_state();
    if source_conf.is_joint() || target_conf.is_joint() {
      return Some(Err(MergeError::JointConfig));
    }
    if sep.conf_change_in_flight() || target_conf_in_flight {
      return Some(Err(MergeError::ConfChangeInFlight));
    }
    if source_conf.voters() != target_conf.voters() {
      return Some(Err(MergeError::VoterSetsDiffer));
    }
    if sep.active_read_mode() != target_mode {
      return Some(Err(MergeError::ReadModesDiffer));
    }
    // The mint reads the live counter; it bumps only when THIS freeze applies, and a second
    // freeze cannot be proposed while this one is pending or applied (AlreadyFrozen above).
    let source_gen_after = sep.shape_gen() + 1;
    let mut target_bytes = Vec::new();
    target.encode(&mut target_bytes);
    let payload = PrepareMergePayload::new(Bytes::from(target_bytes), source_gen_after);
    let mut buf = Vec::new();
    crate::wire::encode_prepare_merge_payload(&payload, &mut buf);
    let ep = self.groups.get_mut(source).expect("checked hosted above");
    let result = ep
      .propose_merge_entry(now, log, EntryKind::PrepareMerge, Bytes::from(buf))
      .map_err(MergeError::Propose);
    self.mark_dirty(source);
    Some(result)
  }

  /// Propose the merge ABSORB on `target`: a committed `CommitMerge` parks every target
  /// replica's apply until its LOCAL source replica is frozen-applied at the boundary, then the
  /// per-crank [`service_merge_applies`](Self::service_merge_applies) resolves it. Leader-
  /// proposed on the TARGET's own log, only once the LOCAL source replica is already
  /// frozen-applied (the cheap local gate; every replica's park re-checks the same facts). The
  /// carried `source_gen_after` is read off the frozen source, so the parked applies' decision
  /// inputs are fully log-determined. `None` if no group `target` is hosted.
  #[must_use = "`None` means no group with this id is hosted — nothing was proposed"]
  pub fn commit_merge<L, S>(
    &mut self,
    target: &G,
    now: impl Into<Now>,
    log: &mut L,
    // Vestigial, as on the whole propose family: kept so the delegators thread `&stable`.
    _stable: &S,
    source: &G,
  ) -> Option<Result<Index, MergeError<I>>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    if !self.groups.contains_key(target) {
      return None;
    }
    if source == target {
      return Some(Err(MergeError::SelfMerge));
    }
    let Some(sep) = self.groups.get(source) else {
      return Some(Err(MergeError::SourceMissing));
    };
    let source_conf = sep.conf_state();
    let source_mode = sep.active_read_mode();
    let source_frozen_ready =
      sep.is_frozen() && sep.freeze_index().is_some_and(|f| sep.applied_index() >= f);
    let source_claim_mismatch = {
      let mut target_bytes = Vec::new();
      target.encode(&mut target_bytes);
      sep.frozen_for().is_none_or(|t| *t != target_bytes)
    };
    let freeze_index = sep.freeze_index();
    let source_gen_after = sep.shape_gen();
    let tep = self.groups.get(target).expect("checked hosted above");
    if tep.is_poisoned() {
      return Some(Err(MergeError::Propose(ProposeError::Poisoned)));
    }
    if !tep.role().is_leader() {
      return Some(Err(MergeError::NotLeader {
        leader: tep.leader(),
      }));
    }
    if tep.commit_merge_in_flight() || tep.pending_merge().is_some() {
      return Some(Err(MergeError::AlreadyPending));
    }
    // A frozen (or freezing) target must not absorb: the CommitMerge would land above its own
    // freeze boundary and mutate the FSM there — the absorb determinism its own merge's
    // target depends on.
    if tep.merge_freeze_active() {
      return Some(Err(MergeError::AlreadyFrozen));
    }
    // The local readiness gate: the source must be frozen-applied at its boundary. `>=` rather
    // than `==` deliberately — a post-freeze election lands an FSM-no-op above the boundary on
    // every replica, and only FSM-no-ops can follow a surviving freeze, so applied-past-F is
    // the SAME state as applied-at-F (an equality gate would wedge every merge whose source
    // ever re-elected while frozen).
    if !source_frozen_ready {
      return Some(Err(MergeError::SourceNotReady));
    }
    if source_claim_mismatch {
      // The freeze names ONE absorbing target for its whole generation; a commit from any
      // other target could only park and abort at the service's claim leg — refuse it here
      // with the truthful verdict instead.
      return Some(Err(MergeError::SourceClaimed));
    }
    let target_conf = tep.conf_state();
    if source_conf.is_joint() || target_conf.is_joint() {
      return Some(Err(MergeError::JointConfig));
    }
    if tep.conf_change_in_flight() {
      return Some(Err(MergeError::ConfChangeInFlight));
    }
    if source_conf.voters() != target_conf.voters() {
      return Some(Err(MergeError::VoterSetsDiffer));
    }
    if source_mode != tep.active_read_mode() {
      return Some(Err(MergeError::ReadModesDiffer));
    }
    let target_gen_after = tep.shape_gen() + 1;
    let mut source_bytes = Vec::new();
    source.encode(&mut source_bytes);
    let payload = CommitMergePayload::new(
      Bytes::from(source_bytes),
      freeze_index.expect("frozen-ready implies a boundary"),
      source_gen_after,
      target_gen_after,
    );
    let mut buf = Vec::new();
    crate::wire::encode_commit_merge_payload(&payload, &mut buf);
    let ep = self.groups.get_mut(target).expect("checked hosted above");
    let result = ep
      .propose_merge_entry(now, log, EntryKind::CommitMerge, Bytes::from(buf))
      .map_err(MergeError::Propose);
    self.mark_dirty(target);
    Some(result)
  }

  /// Propose the merge ABORT on `target`: a committed target-side `RollbackMerge` abandons the
  /// merge of `source` into it, TOTALLY ORDERED against `CommitMerge` on the target's own log —
  /// landing below the commit it kills it at the commit's own lineage guard (parks never form);
  /// landing at the coordinate right after a parked commit it un-parks every replica aborted;
  /// landing later it no-ops (the merge already resolved — the abort's mint is stale by then).
  /// The applied abort then RELAYS the source's thaw
  /// ([`poll_pending_merge_abort`](Self::poll_pending_merge_abort)): the driver proposes the
  /// source-side `RollbackMerge` on the source's own log, and a relay lost to churn is
  /// recovered by simply re-proposing this abort. The release valve — there is deliberately no
  /// timeout-based auto-unfreeze. `None` if no group `target` is hosted.
  ///
  /// The gates are best-effort truthfulness (the apply-time lineage guard is the decider): the
  /// TARGET leader proposes; the LOCAL source must exist and be frozen or freezing (the mint
  /// names its freeze generation); a frozen target refuses (its own dissolution outranks —
  /// aborting through it would bump its lineage above its own boundary).
  #[must_use = "`None` means no group with this id is hosted — nothing was proposed"]
  pub fn rollback_merge<L, S>(
    &mut self,
    target: &G,
    now: impl Into<Now>,
    log: &mut L,
    // Vestigial, as on the whole propose family: kept so the delegators thread `&stable`.
    _stable: &S,
    source: &G,
  ) -> Option<Result<Index, MergeError<I>>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    if !self.groups.contains_key(target) {
      return None;
    }
    if source == target {
      return Some(Err(MergeError::SelfMerge));
    }
    if !self.groups.contains_key(source) {
      return Some(Err(MergeError::SourceMissing));
    }
    let tep = self.groups.get(target).expect("checked hosted above");
    if tep.is_poisoned() {
      return Some(Err(MergeError::Propose(ProposeError::Poisoned)));
    }
    if !tep.role().is_leader() {
      return Some(Err(MergeError::NotLeader {
        leader: tep.leader(),
      }));
    }
    if tep.merge_freeze_active() {
      return Some(Err(MergeError::AlreadyFrozen));
    }
    // The abort names an APPLIED freeze: its generation and claim are read off the frozen
    // source. A merely pending freeze refuses — unreadable claim, and a freeze that never
    // commits self-heals through truncation rather than through an abort.
    let sep = self.groups.get(source).expect("checked hosted above");
    if !sep.is_frozen() {
      return Some(Err(MergeError::NotFrozen));
    }
    let mut target_bytes = Vec::new();
    target.encode(&mut target_bytes);
    if sep.frozen_for().is_none_or(|t| *t != target_bytes) {
      // Only the claimed target may abort the merge — a foreign abort's relayed thaw would
      // move the source's counter under the claimed target's parked commit (the wedge the
      // claim exists to prevent).
      return Some(Err(MergeError::SourceClaimed));
    }
    let source_gen_after = sep.shape_gen();
    let mut source_bytes = Vec::new();
    source.encode(&mut source_bytes);
    // The mint reads the target's live counter — deliberately NOT gated on an in-flight or
    // parked commit: racing one is this verb's whole purpose, and the shared base is exactly
    // what makes the race resolve to one log-ordered winner.
    let target_gen_after = tep.shape_gen() + 1;
    let payload = RollbackMergePayload::abort(
      Bytes::from(source_bytes),
      source_gen_after,
      target_gen_after,
    );
    let mut buf = Vec::new();
    crate::wire::encode_rollback_merge_payload(&payload, &mut buf);
    let ep = self.groups.get_mut(target).expect("checked hosted above");
    let result = ep
      .propose_merge_entry(now, log, EntryKind::RollbackMerge, Bytes::from(buf))
      .map_err(MergeError::Propose);
    self.mark_dirty(target);
    Some(result)
  }

  /// Propose the SOURCE-side thaw on `source` — the relay leg of a committed target-side abort
  /// (see [`poll_pending_merge_abort`](Self::poll_pending_merge_abort)): invoked by the
  /// DRIVER's relay drain with the relay's own target as `claimed_by`, and by NOTHING else —
  /// the embedder's verb is [`rollback_merge`](Self::rollback_merge). A direct thaw that
  /// bypasses the abort would move the source's counter under the claimed target's parked
  /// commit, wedging it (debug builds assert; release builds hold the park rather than
  /// diverge). The ONE entry proposable on a frozen group. Refusals are the relay's dedupe: an
  /// already-thawed source answers `NotFrozen`, a non-leader replica `NotLeader`, a claim
  /// mismatch `SourceClaimed` — every host relays once per abort apply, and exactly the host
  /// whose local source replica leads under the matching claim can land the thaw. `None` if no
  /// group `source` is hosted.
  #[must_use = "`None` means no group with this id is hosted — nothing was proposed"]
  pub fn propose_merge_unfreeze<L, S>(
    &mut self,
    source: &G,
    now: impl Into<Now>,
    log: &mut L,
    // Vestigial, as on the whole propose family: kept so the delegators thread `&stable`.
    _stable: &S,
    claimed_by: &G,
  ) -> Option<Result<Index, MergeError<I>>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let ep = self.groups.get(source)?;
    if ep.is_poisoned() {
      return Some(Err(MergeError::Propose(ProposeError::Poisoned)));
    }
    if !ep.role().is_leader() {
      return Some(Err(MergeError::NotLeader {
        leader: ep.leader(),
      }));
    }
    // An APPLIED freeze only: a pending one's claim is unreadable (and a freeze that never
    // commits self-heals through truncation, not through a thaw).
    if !ep.is_frozen() {
      return Some(Err(MergeError::NotFrozen));
    }
    let mut claimed_bytes = Vec::new();
    claimed_by.encode(&mut claimed_bytes);
    if ep.frozen_for().is_none_or(|t| *t != claimed_bytes) {
      // A relay riding a foreign target's abort must not thaw a source claimed elsewhere —
      // the claimed target's parked commit gates on this counter staying put.
      return Some(Err(MergeError::SourceClaimed));
    }
    let source_gen_after = ep.shape_gen() + 1;
    let payload = RollbackMergePayload::unfreeze(source_gen_after);
    let mut buf = Vec::new();
    crate::wire::encode_rollback_merge_payload(&payload, &mut buf);
    let ep = self.groups.get_mut(source).expect("checked hosted above");
    let result = ep
      .propose_merge_entry(now, log, EntryKind::RollbackMerge, Bytes::from(buf))
      .map_err(MergeError::Propose);
    self.mark_dirty(source);
    Some(result)
  }

  /// Resolve every parked `CommitMerge` that the TARGET's log and local facts now decide —
  /// called ONCE PER CRANK by every driver, after the per-group apply drains.
  ///
  /// The ABORT side is decided by target-log order alone: the park's **abort window** is the
  /// single committed coordinate `k + 1` right after the parked entry. Until something commits
  /// there EVERY arm holds (resolving while the coordinate is undecided would race an abort
  /// landing at it — one host absorbed, another aborted, committed divergence; the proven
  /// cross-log race, one log removed), and the target LEADER seals a quiet window with a no-op
  /// so it cannot stay open forever. A committed matching abort there un-parks ABORTED on
  /// every replica; anything else closes the window for good — from then on the park waits
  /// only on the local source gate. NEVER a live read of the source's mutable state on the
  /// abort side: the source's counter cannot move while the park stands (the thaw is relayed
  /// by the abort itself, which this very park blocks above `k`).
  ///
  /// With the window closed the arms are: **resolve** (source hosted, frozen at the expected
  /// gen FOR THIS TARGET, applied past the boundary, and the target free to stage its absorb
  /// capture) — the source endpoint is removed, its state machine absorbed, and the forced
  /// capture staged through `stores` so the union's durability anchor rides the SAME barrier
  /// as the driver's floor/teardown; **abort** (the frozen source's claim names a DIFFERENT
  /// target — log-pinned for the freeze's whole generation, so identical on every replica; or
  /// source absent WITH the terminal floor — a replayed duplicate, the union already here);
  /// **keep parked** otherwise (a behind source keeps replicating while frozen, entirely
  /// independent of this target; an absent source without the floor waits for the resolved
  /// quorum's post-merge snapshot, whose install supersedes the park).
  ///
  /// Liveness needs exactly one guarantee from the driver: a parked target is never
  /// quiesce-eligible, so this service keeps being reached. The park waits on a local monotone
  /// condition (the local source's applied reaching a fixed committed index); the abort valve
  /// races the seal for the window, and once sealed the merge is as decided as any committed
  /// entry — a source wedged below its boundary forever is the group-is-dead liveness class,
  /// recovered at the embedder's catalog like any dead group.
  ///
  /// Returns the crank's resolutions for the DRIVER to fold: floor + teardown for `Merged`,
  /// nothing for `Aborted`.
  pub fn service_merge_applies<L, S, St>(
    &mut self,
    now: impl Into<Now>,
    stores: &mut St,
  ) -> Vec<MergeResolution<G>>
  where
    St: crate::GroupStores<G, L, S> + FloorStore<G>,
    L: LogStore,
    S: StableStore<NodeId = I>,
    F::Snapshot: Data,
  {
    let now: Now = now.into();
    let mut resolutions = Vec::new();
    let parked: Vec<G> = self
      .groups
      .iter()
      .filter(|(_, ep)| ep.pending_merge().is_some())
      .map(|(gid, _)| gid.cheap_clone())
      .collect();
    for tgid in parked {
      let Some(tep) = self.groups.get(&tgid) else {
        continue;
      };
      let Some(pending) = tep.pending_merge() else {
        continue;
      };
      let expected = pending.source_gen_after();
      let boundary = pending.freeze_index();
      let source_bytes = pending.source_bytes();
      let Ok(source) = G::decode_exact(source_bytes) else {
        // A committed source id that does not decode as G is committed-corrupt — the split
        // relay's own decode class, fail-stopped identically.
        if let Some(tep) = self.groups.get_mut(&tgid) {
          tep.poison(PoisonReason::MergeDecode);
        }
        continue;
      };
      enum Verdict {
        Resolve,
        Abort,
        Wait,
      }
      // THE ABORT WINDOW, first and unconditionally: no arm below may fire until the target's
      // own log has decided the `k + 1` coordinate (see the method doc). The read is of this
      // group's committed content — identical bytes on every replica.
      let window = match stores.stores(&tgid) {
        Some((log, stable)) => {
          let w = tep.merge_abort_window(&*log);
          if w == MergeWindow::Open {
            // Seal a quiet window (leader-only, idempotent per park), flushing the fan-out
            // inline so sealing rides this crank rather than the next heartbeat cadence.
            if let Some(tep) = self.groups.get_mut(&tgid)
              && tep.ensure_merge_seal(now, log)
            {
              tep.flush_appends(now, &*log, &*stable);
              self.mark_dirty(&tgid);
            }
          }
          w
        }
        None => continue,
      };
      let verdict = match window {
        MergeWindow::Abort => Verdict::Abort,
        MergeWindow::Open | MergeWindow::Stall => Verdict::Wait,
        MergeWindow::Closed => match self.groups.get(&source) {
          Some(sep) => {
            let seen = sep.shape_gen();
            if seen == expected {
              // gen == expected implies the freeze applied (that apply is the counter's only
              // path to this value) — frozen and applied-past-boundary ride along; the explicit
              // checks document the gate and catch a broken counter in debug.
              debug_assert!(sep.is_frozen() && sep.applied_index() >= boundary);
              let mut tgid_bytes = Vec::new();
              tgid.encode(&mut tgid_bytes);
              if sep.frozen_for().is_none_or(|t| *t != tgid_bytes) {
                // The freeze is a CLAIM by exactly one target, pinned for this whole
                // generation on the source's log: a park under a foreign claim can never
                // absorb, and every replica sees the same claim — abort deterministically
                // (two targets naming one frozen source is the same committed-divergence
                // class as the cross-log rollback, and the claim is what closes it).
                Verdict::Abort
              } else if sep.role().is_leader() && !sep.peers_matched_through(boundary) {
                // waitForApplication, container-local: the host whose local source replica
                // LEADS the source resolves LAST. Resolving would consume (and tear down) the
                // source leader while slow source followers still sit below the boundary —
                // with no leader left to feed them the freeze, their hosts' parks wedge
                // forever. Holding THIS park keeps the source leader alive and replicating
                // exactly until every peer provably matched through the boundary (the capture
                // fence keeps the freeze replayable, so catch-up below it never needs a
                // snapshot the source cannot send). A source peer that never catches up
                // (dead, unreachable) holds only this one host's park.
                Verdict::Wait
              } else {
                Verdict::Resolve
              }
            } else {
              // Behind the expectation: still catching up (its own replication keeps running
              // while frozen). PAST it is structurally unreachable while parked — the thaw is
              // relayed only by the abort entry this park blocks above `k`, and a replayed
              // commit against an already-moved counter no-ops at its own lineage guard
              // before ever parking — so a moved counter here is a broken-counter bug, not an
              // abort signal: hold rather than diverge.
              debug_assert!(
                seen < expected,
                "a parked commit observed the source PAST its freeze generation"
              );
              Verdict::Wait
            }
          }
          // Absent WITH the terminal floor: this host already absorbed the source (the floor
          // lands in the same barrier as an absorb) — the commit is a replayed duplicate and
          // the union is already in the target; no-op past it. Absent WITHOUT the floor: this
          // replica never held the source (lifecycle churn tore it down, or the replica
          // joined after the source dissolved) and the union is NOT materializable here — it
          // must WAIT for the resolved quorum's post-merge snapshot, whose install supersedes
          // the park (the forced capture compacts the leader through the absorb, so a parked
          // straggler is structurally on the snapshot path). Aborting instead would skip the
          // union on this replica alone — silent, permanent divergence from every replica
          // that absorbed.
          None => {
            if stores.floor(&source) == crate::MERGED_FLOOR {
              Verdict::Abort
            } else {
              Verdict::Wait
            }
          }
        },
      };
      match verdict {
        Verdict::Wait => {}
        Verdict::Abort => {
          if let Some(tep) = self.groups.get_mut(&tgid) {
            tep.resolve_pending_merge_aborted();
          }
          self.mark_dirty(&tgid);
          resolutions.push(MergeResolution::Aborted {
            source,
            target: tgid,
          });
        }
        Verdict::Resolve => {
          // The absorb capture must be stageable NOW: absorb + capture + (driver-side) floor
          // and teardown ride one crank, one barrier. A busy target (a capture or install
          // already staged, a fork barrier at the absorb point) stays parked this crank.
          if self
            .groups
            .get(&tgid)
            .is_some_and(Endpoint::absorb_capture_blocked)
          {
            continue;
          }
          let Some(sep) = self.remove_group(&source) else {
            continue;
          };
          let fsm = sep.into_state_machine();
          let Some(tep) = self.groups.get_mut(&tgid) else {
            continue;
          };
          tep.resolve_pending_merge(fsm);
          if let Some((log, stable)) = stores.stores(&tgid)
            && let Some(tep) = self.groups.get_mut(&tgid)
          {
            tep.capture_absorb_snapshot(log, stable);
          }
          self.mark_dirty(&tgid);
          resolutions.push(MergeResolution::Merged {
            source,
            target: tgid,
          });
        }
      }
    }
    resolutions
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
