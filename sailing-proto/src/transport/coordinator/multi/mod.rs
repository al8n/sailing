//! `MultiStreamCoordinator<G, I, F, R>`: the multi-group stream-transport super state machine.
//!
//! It composes a [`MultiRaft`] of single-group endpoints with a [`PeerRouter`]: inbound frames are
//! demuxed by their group tag and fed to the owning group's endpoint, and each group's outbound
//! messages are routed back stamped with that group's id. One connection per peer carries every
//! co-located group's traffic. Storage is per group: [`handle_conn_data`](MultiStreamCoordinator::handle_conn_data)
//! resolves each decoded frame's group store through a caller-supplied
//! [`GroupStores`](crate::GroupStores), while the
//! single-group driving methods take the target group's store directly.
use super::super::{
  CoalescedEntry, ConnId, TransportError, frame::COALESCED_FLAG_QUIESCE, router::PeerRouter,
  stream::RecordIo,
};
use crate::{
  Config, CreateGroupError, Data, Endpoint, Event, FloorStore, GroupId, GroupStores, Index,
  Instant, LogStore, Message, MultiRaft, NodeId, Now, ProposeError, RemoveError, StableStore,
  StateMachine, StorageProgress, multi::validate_floor,
};
use bytes::Bytes;
use std::{
  collections::{BTreeMap, BTreeSet, VecDeque},
  vec::Vec,
};

/// A group-scoped scheduling signal a multi-group coordinator surfaces to its driver (drained via
/// `poll_group_control`, like `poll_event`). Only the LATEST signal per group survives to the
/// driver — a group's net state is its last control in stream order — so a quiesce-flagged beat
/// followed by an append in the same read collapses to a single `Wake`, landing the group awake.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum GroupControl {
  /// The group's leader quiesced it after the beat that carried this flag: the peer promised
  /// heartbeat silence, so the driver may stop arming/sweeping the group's timers — safe because
  /// any traffic surfaces a [`Wake`](Self::Wake) and a connection loss is the driver's own signal
  /// to wake everything.
  Quiesce,
  /// WAKE-class inbound traffic was dispatched to the group. The absorbed (non-waking) complement
  /// is one message kind in one shape — a `HeartbeatResponse` advertising no wedged merge park —
  /// because a quiescing group's FINAL flagged round
  /// exchanges precisely `Heartbeat` + `HeartbeatResponse` and nothing else, and
  /// waking on that response would re-arm the leader's timers and keep the round-trip alive
  /// forever. That the final round is exactly the pair rests on two gates: quiesce eligibility
  /// (every tracked peer — learners included — caught up and replicating, commit applied) means
  /// no responder is behind, probing, or awaiting a snapshot, and the gated heartbeat-response
  /// append pump sends nothing to a caught-up, replicating responder — so the round has no
  /// empty-append tail to absorb. Absorbing the
  /// response is safe: it can only echo a beat the quiesced side itself sent pre-quiesce (a
  /// quiesced leader emits no new beats), and eligibility ensured no straggler ack carries new
  /// progress. The one exception is a response carrying a nonzero `stuck_boundary`: that is a peer
  /// reporting a merge park it cannot cure alone, state the eligibility sweep is structurally blind
  /// to (a parked replica's log is fully caught up; only its apply drain is stalled), so it wakes —
  /// see `is_wake_class`.
  /// Everything else wakes — a `Heartbeat` tells a quiesced follower its leader is
  /// active again (restoring the silent-wedge detection its swept election timer provides), a
  /// non-empty `AppendEntries` is live replication, votes/transfers/reads/snapshots are live
  /// consensus, and an empty `AppendEntries` or `AppendResponse` no longer belongs to an idle
  /// round at all, so a spurious one costs one wake instead of riding the absorb trust surface.
  /// A connection loss, the liveness oracle, is the driver's own wake signal — and a silent
  /// blackhole (a peer that stops sending without closing the socket) becomes exactly such a loss:
  /// the transport idle-probe/reap closes the quiet connection, so even a fully-quiesced plane wakes.
  Wake,
}

/// Bound on DISTINCT groups a coordinator queues as unknown-group placement signals; beyond it
/// new unknown groups drop silently until the embedder drains the queue. The signal is an
/// optimization, never load-bearing (the sender's retry cadence re-delivers), so a small fixed
/// bound caps the state a scanner spraying group ids can pin.
pub(crate) const UNKNOWN_GROUP_SIGNAL_CAP: usize = 64;

/// Whether an inbound message is INITIAL-SHAPED — a kind that legitimately SOLICITS state
/// creation on a host that does not carry its group: a campaigner's vote request (any,
/// pre-vote included), or a leader's first-contact heartbeat (the per-peer commit clamp
/// `min(commit, match)` makes a genuinely-initial beat carry commit 0 by construction). Mirrors
/// TiKV's `is_initial_msg` gate (only these kinds may create a peer): surfacing arbitrary
/// traffic would let a delayed straggler — an old append or snapshot chunk from a removed
/// group's past life — prompt the embedder's placement brain to resurrect a group it just
/// destroyed, while an initial-shaped message means a live peer is actively soliciting this
/// node, and the sender's own retry loop re-delivers everything else once the group exists.
pub(crate) fn is_initial_shaped<I: crate::CheapClone>(msg: &Message<I>) -> bool {
  match msg {
    Message::RequestVote(_) => true,
    Message::Heartbeat(hb) => hb.commit() == Index::ZERO,
    _ => false,
  }
}

/// A multi-group consensus node speaking over framed reliable connections (`R` is the record layer,
/// e.g. `Labeled<Passthrough>` for TCP or `Labeled<TlsRecords>` for TLS).
pub struct MultiStreamCoordinator<G, I, F, R>
where
  F: StateMachine,
{
  multi: MultiRaft<G, I, F>,
  router: PeerRouter<I, R>,
  next_conn_id: u64,
  /// Heartbeat/HeartbeatResponse batches diverted per peer by [`flush`](Self::flush), shipped as
  /// coalesced frames at the [`poll_transmit`](Self::poll_transmit) chokepoint. Batching across
  /// flushes (rather than per flush) is what coalesces a driver's PER-GROUP `handle_timeout` sweep
  /// — each call flushes separately, but all of a crank's beats leave in that crank's transmit
  /// drain as one frame per peer.
  hb_batches: BTreeMap<I, Vec<CoalescedEntry<I>>>,
  /// Groups whose NEXT heartbeat broadcast carries the QUIESCE flag (set by
  /// [`mark_quiescing`](Self::mark_quiescing), consumed by the flush that stamps the beats).
  quiesce_intents: BTreeSet<G>,
  /// The LATEST group-scoped scheduling signal per group. A group's net driver state is its last
  /// control in stream order (`Wake` re-arms its timers, `Quiesce` parks them), and cross-group
  /// order carries no invariant, so keeping only the latest per group delivers the same net state
  /// as the full stream would — a burst collapses to the one control that matters.
  control_state: BTreeMap<G, GroupControl>,
  /// Groups with a pending scheduling signal, in first-signal order, membership-deduped by
  /// `control_state` (see [`poll_group_control`](Self::poll_group_control)). A gid whose map entry
  /// was purged (removal/merge) stays queued but inert and is skipped at poll.
  controls: VecDeque<G>,
  /// Tombstoned group ids: REMOVED groups whose inbound frames drop silently and whose
  /// create/restore refuses until an explicit [`clear_tombstone`](Self::clear_tombstone) (see
  /// [`remove_group`](Self::remove_group)). In-memory and volatile — a restart starts clean; the
  /// embedder's group catalog owns removal persistence.
  retired: BTreeSet<G>,
  /// Unknown-group placement signals, in arrival order: `(group, authenticated sender)` for
  /// initial-shaped traffic whose group is neither hosted, nor store-resolvable, nor tombstoned
  /// (see [`poll_unknown_group`](Self::poll_unknown_group)).
  unknown_pending: VecDeque<(G, I)>,
  /// The groups currently queued in `unknown_pending` — the dedupe set (one signal per group
  /// until polled off), bounded by [`UNKNOWN_GROUP_SIGNAL_CAP`].
  unknown_seen: BTreeSet<G>,
  /// Monotone count of frames the generation fence dropped (see
  /// [`fenced_frames_dropped`](Self::fenced_frames_dropped)).
  fenced_dropped: u64,
}

impl<G, I, F, R> MultiStreamCoordinator<G, I, F, R>
where
  G: GroupId,
  I: NodeId,
  F: StateMachine,
  F::Command: Data,
  F::Snapshot: Data,
  F::Error: core::error::Error,
  R: RecordIo,
{
  /// A coordinator hosting no groups and no connections.
  #[must_use]
  pub fn new() -> Self {
    Self {
      multi: MultiRaft::new(),
      router: PeerRouter::new(),
      next_conn_id: 1,
      hb_batches: BTreeMap::new(),
      quiesce_intents: BTreeSet::new(),
      control_state: BTreeMap::new(),
      controls: VecDeque::new(),
      retired: BTreeSet::new(),
      unknown_pending: VecDeque::new(),
      unknown_seen: BTreeSet::new(),
      fenced_dropped: 0,
    }
  }

  /// Create a fresh group (see [`MultiRaft::create_group`]). Admission checks run FLOOR FIRST,
  /// then the tombstone, then the container: `generation` — the id's incarnation under the
  /// single-incarnation contract, 0 unless the embedder reshapes ids — is compared against the
  /// persisted admission floor read through `floors`, and an under-floor incarnation is refused
  /// before anything volatile is consulted (the durable fence outranks in-session state). The
  /// ADMITTED generation then SEEDS the container's lineage counter — the created incarnation
  /// mints strictly above its floor, so it can never repeat a fenced predecessor's generations —
  /// and the driver records the same value in its engine after the `Ok`. The driver hands its
  /// engine as `floors`; the deterministic sim and proto-level embedders hand their own store —
  /// durability is the store-owner's job. A tombstoned id still REFUSES creation at ANY
  /// generation until an explicit [`clear_tombstone`](Self::clear_tombstone) consents to
  /// re-admission — the clear-then-create pair is the supported rejoin path (see
  /// [`remove_group`](Self::remove_group)); the floor narrows what MAY be admitted, never
  /// widens it.
  ///
  /// # Errors
  /// [`CreateGroupError::BelowFloor`] when `generation` is below the id's admission floor
  /// (terminal for that incarnation — no consent call cures it; a
  /// [`MERGED_FLOOR`](crate::MERGED_FLOOR) fence refuses every generation),
  /// [`CreateGroupError::ReservedGeneration`] when `generation` is the reserved `u64::MAX`
  /// sentinel itself, [`CreateGroupError::Retired`] when the id is tombstoned by a removal,
  /// [`CreateGroupError::SplitReserved`] when the id is reserved as an in-flight split's
  /// child (self-releasing when the fork resolves); otherwise the admission checks of [`MultiRaft::create_group`] — see [`CreateGroupError`].
  #[allow(clippy::too_many_arguments)]
  pub fn create_group(
    &mut self,
    gid: G,
    config: Config<I>,
    now: impl Into<Now>,
    seed: u64,
    fsm: F,
    generation: u64,
    floors: &impl FloorStore<G>,
  ) -> Result<(), CreateGroupError> {
    validate_floor(floors.floor(&gid), generation)?;
    if self.retired.contains(&gid) {
      return Err(CreateGroupError::Retired);
    }
    // The split reservation: an in-flight split's child id refuses admission from EVERY other
    // path (embedder create/restore, a local fork, the factory's gate at the drivers), so the
    // committed fork's materialization never finds its id occupied by a same-session admission.
    // Derived from live consensus state — it releases on its own when the fork resolves.
    if self.multi.split_reserved(&gid) {
      return Err(CreateGroupError::SplitReserved);
    }
    let key = gid.cheap_clone();
    self
      .multi
      .create_group(gid, generation, config, now, seed, fsm)?;
    self.purge_unknown_signal(&key);
    Ok(())
  }

  /// Recover a group from durable storage (see [`MultiRaft::restore_group`]). Admission is
  /// gated exactly as [`create_group`](Self::create_group): floor first (via the caller's
  /// `floors` seam), then the tombstone, then the container. The embedder's catalog supplies
  /// `generation` — the cross-restart incarnation authority: a restore that lies about it
  /// collapses two incarnations into one identity for every gen-keyed observer (the multi-VOPR
  /// one-identity oracle keys on it), voiding what the fence exists to distinguish.
  ///
  /// The `floors` seam feeds the fork replay guard too: the admitted group's guard is raised to
  /// `floors.lineage(&gid)`, the DURABLE lineage record the driver flushed with each of this
  /// id's materialized forks — the restored snapshot meta alone can lag it, and a meta-seeded
  /// guard would re-relay a replayed fork whose child baseline is already durable (see
  /// [`MultiRaft::raise_relay_guard`]).
  ///
  /// # Errors
  /// [`CreateGroupError::BelowFloor`] when `generation` is below the id's admission floor (a
  /// [`MERGED_FLOOR`](crate::MERGED_FLOOR) fence refuses every generation),
  /// [`CreateGroupError::ReservedGeneration`] when `generation` is the reserved `u64::MAX`
  /// sentinel itself, [`CreateGroupError::Retired`] when the id is tombstoned by a removal,
  /// [`CreateGroupError::SplitReserved`] when the id is reserved as an in-flight split's
  /// child (self-releasing when the fork resolves); otherwise the admission checks of [`MultiRaft::restore_group`] — see [`CreateGroupError`].
  #[allow(clippy::too_many_arguments)]
  pub fn restore_group<L, S>(
    &mut self,
    gid: G,
    config: Config<I>,
    now: impl Into<Now>,
    seed: u64,
    fsm: F,
    boot_epoch: u64,
    generation: u64,
    floors: &impl FloorStore<G>,
    log: &mut L,
    stable: &mut S,
  ) -> Result<(), CreateGroupError>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
    I: Data,
  {
    validate_floor(floors.floor(&gid), generation)?;
    if self.retired.contains(&gid) {
      return Err(CreateGroupError::Retired);
    }
    // The split reservation: an in-flight split's child id refuses admission from EVERY other
    // path (embedder create/restore, a local fork, the factory's gate at the drivers), so the
    // committed fork's materialization never finds its id occupied by a same-session admission.
    // Derived from live consensus state — it releases on its own when the fork resolves.
    if self.multi.split_reserved(&gid) {
      return Err(CreateGroupError::SplitReserved);
    }
    let key = gid.cheap_clone();
    self
      .multi
      .restore_group(gid, config, now, seed, fsm, boot_epoch, log, stable)?;
    self.multi.raise_relay_guard(&key, floors.lineage(&key));
    self.purge_unknown_signal(&key);
    Ok(())
  }

  /// Create a group from locally forked state (see [`MultiRaft::create_group_from_fork`] for
  /// the manufactured-snapshot-install contract; `generation` and `read_only` ride the baseline
  /// meta as the child's lineage and inherited read-mode provenance). Admission is gated exactly as
  /// [`create_group`](Self::create_group): floor first (via the caller's `floors` seam), then
  /// the tombstone, then the container — a fork NEVER clears a tombstone, and it is never
  /// factory-reachable (a local act by an already-authorized replica, never solicited over the
  /// wire). A successful fork purges any queued unknown-group signal for the id, as every
  /// admission does.
  ///
  /// # Errors
  /// [`CreateGroupError::BelowFloor`] when `generation` is below the id's admission floor (a
  /// [`MERGED_FLOOR`](crate::MERGED_FLOOR) fence refuses every generation),
  /// [`CreateGroupError::ReservedGeneration`] when `generation` is the reserved `u64::MAX`
  /// sentinel itself, [`CreateGroupError::Retired`] when the id is tombstoned by a removal,
  /// [`CreateGroupError::SplitReserved`] when the id is reserved as an in-flight split's
  /// child (self-releasing when the fork resolves); otherwise the admission checks of [`MultiRaft::create_group_from_fork`] — see
  /// [`CreateGroupError`] — including [`CreateGroupError::InvalidBootEpoch`] when
  /// `boot_epoch == 0` (a fork's manufactured baseline needs the prior epoch to itself) and
  /// [`CreateGroupError::StorageInUse`] when the handed stores already hold state (a fork
  /// never overwrites used storage). Refusal happens BEFORE any store write.
  #[allow(clippy::too_many_arguments)]
  pub fn create_group_from_fork<L, S>(
    &mut self,
    gid: G,
    config: Config<I>,
    now: impl Into<Now>,
    seed: u64,
    fsm: F,
    snapshot: Bytes,
    read_only: Option<crate::ReadOnlyOption>,
    fork_id: Option<crate::ForkId>,
    boot_epoch: u64,
    generation: u64,
    floors: &impl FloorStore<G>,
    log: &mut L,
    stable: &mut S,
  ) -> Result<(), CreateGroupError>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
    I: Data,
  {
    validate_floor(floors.floor(&gid), generation)?;
    if self.retired.contains(&gid) {
      return Err(CreateGroupError::Retired);
    }
    // THE RESERVATION FENCE STANDS ON THIS DOOR. This is the public, caller-driven fork install —
    // anyone holding a coordinator can call it with a blob and a token of their choosing — so a
    // child id an in-flight or staged split owns must refuse here. The relay's own materialization
    // does not come through this door: it goes through
    // [`create_group_from_relayed_fork`](Self::create_group_from_relayed_fork), whose ticket is a
    // container-minted fork nobody outside this crate can construct.
    if self.multi.split_reserved(&gid) {
      return Err(CreateGroupError::SplitReserved);
    }
    let key = gid.cheap_clone();
    self.multi.create_group_from_fork(
      gid, generation, config, now, seed, fsm, snapshot, read_only, fork_id, boot_epoch, log,
      stable,
    )?;
    self.purge_unknown_signal(&key);
    Ok(())
  }

  /// Materialize a child from a fork the CONTAINER yielded — the relay's door, and the only one
  /// that skips the split reservation.
  ///
  /// The ticket is possession of a [`GroupFork`](crate::GroupFork): the container mints them, only
  /// for a committed split whose coordinates it verified, and nothing outside this crate can
  /// construct one. That is what makes skipping the reservation safe here and nowhere else — the
  /// reservation fences the OTHER admission doors from squatting an id a split owns, and this call
  /// IS that split claiming its own id. Consulting the predicate here would refuse the very
  /// admission it exists to protect: the fork's own parent reserves the id while it is staged, and
  /// a sibling parent that named the same child reserves it too. Under the relay's hold such a
  /// refusal has nowhere to go but the caller's fail-closed floor, which in release drops the
  /// partition. Two committed forks racing one child need no fence between them either — the first
  /// installs, and the second parks on the hosted child under provenance.
  ///
  /// `config` is the child's boot config after the caller's reshape-birth transform; every other
  /// input is taken from the fork itself, so no caller-supplied token can reach the baseline.
  #[allow(clippy::too_many_arguments)]
  pub fn create_group_from_relayed_fork<L, S>(
    &mut self,
    fork: crate::GroupFork<G, I, F>,
    config: Config<I>,
    now: impl Into<Now>,
    seed: u64,
    boot_epoch: u64,
    floors: &impl FloorStore<G>,
    log: &mut L,
    stable: &mut S,
  ) -> Result<(), CreateGroupError>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
    I: Data,
  {
    validate_floor(floors.floor(&fork.child), fork.child_gen)?;
    if self.retired.contains(&fork.child) {
      return Err(CreateGroupError::Retired);
    }
    let key = fork.child.cheap_clone();
    self.multi.create_group_from_fork_unreserved(
      fork.child,
      fork.child_gen,
      config,
      now,
      seed,
      fork.fsm,
      fork.blob,
      fork.read_only,
      Some(fork.fork_id),
      boot_epoch,
      log,
      stable,
    )?;
    self.purge_unknown_signal(&key);
    Ok(())
  }

  /// Remove a group, returning its endpoint if present. Drops the group's pending quiesce intent
  /// and queued controls with it (its per-peer batched beats, if any, still ship — a removed
  /// group's in-flight heartbeat is indistinguishable from one that left just before removal, and
  /// the receiver's unhosted-entry drop absorbs it) — and TOMBSTONES the id: inbound frames tagged
  /// with it drop silently, before store resolution and WITHOUT an unknown-group signal, and a
  /// create/restore of the id refuses ([`CreateGroupError::Retired`]). Re-admission is EXPLICIT:
  /// [`clear_tombstone`](Self::clear_tombstone), then create/restore — so a stale unknown-group
  /// advisory consumed after this removal can never resurrect the id through a naive re-create.
  ///
  /// The tombstone is IN-MEMORY and GROUP-keyed — deliberately weaker than the references, which
  /// persist replica-keyed tombstones for exactly this straggler problem (TiKV a region-epoch +
  /// peer-id tombstone, CockroachDB a NextReplicaID floor, each a monotonic incarnation
  /// discriminator): sailing group ids carry no incarnation number, so SINGLE-INCARNATION ids are
  /// the embedder contract — re-creating the SAME group id (an explicit clear, then a
  /// create/restore) is the supported rejoin path, and reusing an id for a DIFFERENT logical
  /// group is unsound without epochs (matching the NodeId reuse rules). Across a restart the
  /// tombstone is gone; the embedder's placement catalog is the persistent record of what must
  /// not live here.
  ///
  /// REFUSES every UNRESOLVED merge participant (inherited verbatim from the container's teardown
  /// gate: [`OwesThaw`](RemoveError::OwesThaw), [`Frozen`](RemoveError::Frozen),
  /// [`MergeParked`](RemoveError::MergeParked), [`SpokenFor`](RemoveError::SpokenFor),
  /// [`Claimed`](RemoveError::Claimed)) — nothing is torn down and, crucially, the id is NOT
  /// tombstoned, so a refused removal is a clean no-op the caller retries once the choreography
  /// resolves. The gate runs FIRST, before any side-state is cleared, so the refusal leaves the
  /// group and this coordinator's bookkeeping fully intact. `stores` is the per-group seam the
  /// container reads a freeze-pending source's log through (the `Claimed` leg's append-pending
  /// window); a refusal touches nothing through it.
  pub fn remove_group<L, S, St>(
    &mut self,
    gid: &G,
    stores: &mut St,
  ) -> Result<Option<Endpoint<I, F>>, RemoveError>
  where
    St: GroupStores<G, L, S>,
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let removed = self.multi.remove_group(gid, stores)?;
    self.quiesce_intents.remove(gid);
    // Drop the group's latest control; a still-queued gid goes inert and is skipped at poll.
    self.control_state.remove(gid);
    self.retired.insert(gid.cheap_clone());
    self.purge_unknown_signal(gid);
    Ok(removed)
  }

  /// Lift `gid`'s tombstone, returning whether one existed. The EXPLICIT re-admission consent —
  /// the ONLY way a retired id becomes creatable again (a subsequent create/restore then admits
  /// it): admission itself never lifts a tombstone, so consenting is always a deliberate act of
  /// the embedder's placement brain, never the side effect of replaying a stale advisory.
  /// Clearing lifts ONLY this volatile consent gate: the id's persisted admission floor (if a
  /// reshaping removal wrote one through the caller's [`FloorStore`] seam) is untouched — no
  /// consent call ever re-admits an under-floor incarnation.
  pub fn clear_tombstone(&mut self, gid: &G) -> bool {
    self.retired.remove(gid)
  }

  /// Whether `gid` is currently RESERVED as the child id of a split in flight on this host
  /// (see [`MultiRaft::split_reserved`]) — the predicate the admission methods refuse on
  /// ([`CreateGroupError::SplitReserved`]) and a driver's factory pre-build gate declines on.
  /// Self-releasing derived state: no clear call exists.
  #[must_use]
  pub fn is_split_reserved(&self, gid: &G) -> bool {
    self.multi.split_reserved(gid)
  }

  /// Whether `gid` is TOMBSTONED: removed and not explicitly cleared since — its inbound frames
  /// dropping silently and its re-creation refusing (see [`remove_group`](Self::remove_group)).
  /// Volatile — a restart starts clean.
  #[must_use]
  pub fn is_retired(&self, gid: &G) -> bool {
    self.retired.contains(gid)
  }

  /// Every gate [`create_group_from_fork`](Self::create_group_from_fork) applies BEFORE it
  /// consumes the child's state machine and blob — floor, tombstone, split reservation, then the
  /// container's own — run against the same state in the same order. A relay asks this FIRST so a
  /// refusal reaches it while the fork is still whole: `Ok`, with nothing mutated in between,
  /// means the admission cannot refuse.
  pub fn fork_admission_check(
    &self,
    gid: &G,
    generation: u64,
    config: &Config<I>,
    floors: &impl FloorStore<G>,
  ) -> Result<(), CreateGroupError> {
    validate_floor(floors.floor(gid), generation)?;
    if self.retired.contains(gid) {
      return Err(CreateGroupError::Retired);
    }
    if self.multi.split_reserved(gid) {
      return Err(CreateGroupError::SplitReserved);
    }
    self.multi.fork_admission_check(gid, generation, config)
  }

  /// Fail-stop the addressed group because a user QUERY closure panicked mid-read against its state
  /// machine, LATCHING the poison for the lifecycle tail (see
  /// [`MultiRaft::fail_stop_query_panicked`]). A driver caught the unwind to keep its plane and every
  /// co-located group alive, then routes here so this group joins the poison surface
  /// ([`poll_poisoned`](Self::poll_poisoned)) and stops serving possibly-torn replicated state.
  pub fn fail_stop_query_panicked(&mut self, gid: &G) {
    self.multi.fail_stop_query_panicked(gid);
  }

  /// Fail-stop EVERY hosted group because a completion caught a user-closure(-drop) panic that names
  /// no group — the verdict a refusal addressed to a group this host does not carry reports (see
  /// [`MultiRaft::fail_stop_plane_unattributable_panic`] for why an unattributable tear is PLANE-fatal
  /// and why that is the safe trade). Every group poisons and surfaces on the lifecycle tail
  /// ([`poll_poisoned`](Self::poll_poisoned)), so the plane fails LOUDLY, never silently.
  pub fn fail_stop_plane_unattributable_panic(&mut self) {
    self.multi.fail_stop_plane_unattributable_panic();
  }

  /// Drain the next UNKNOWN-GROUP placement signal: `(group, authenticated sender)` for
  /// well-formed INITIAL-SHAPED traffic — a vote request, or a first-contact heartbeat carrying
  /// commit 0 — whose group this host neither hosts, nor resolves stores for, nor has
  /// tombstoned. The embedder's PLACEMENT BRAIN decides what to do with it: create/restore the
  /// group here (the soliciting peer's retry then completes the join) or ignore it (the
  /// coordinator keeps dropping the frames). Placement policy is deliberately NOT the
  /// coordinator's job — no auto-create, ever.
  ///
  /// One signal per group until polled off; polling re-arms the group for a fresh signal. At
  /// most 64 distinct groups are queued — beyond the cap new unknown groups drop silently (the
  /// signal is an optimization; the sender retries on its own cadence).
  pub fn poll_unknown_group(&mut self) -> Option<(G, I)> {
    let (group, from) = self.unknown_pending.pop_front()?;
    self.unknown_seen.remove(&group);
    Some((group, from))
  }

  /// The node's host identity — LATCHED by the first admitted group for the container's
  /// lifetime (a multi-Raft host is one physical node), stable across group removals and
  /// zero-group windows. `None` only before any group has ever been admitted.
  #[must_use]
  pub fn host_id(&self) -> Option<&I> {
    self.multi.host_id()
  }

  /// Queue an unknown-group placement signal, deduped by group until polled off and dropped
  /// beyond [`UNKNOWN_GROUP_SIGNAL_CAP`] pending groups (see
  /// [`poll_unknown_group`](Self::poll_unknown_group)).
  fn note_unknown_group(&mut self, group: G, from: I) {
    if self.unknown_seen.contains(&group) || self.unknown_seen.len() >= UNKNOWN_GROUP_SIGNAL_CAP {
      return;
    }
    self.unknown_seen.insert(group.cheap_clone());
    self.unknown_pending.push_back((group, from));
  }

  /// Drop any queued unknown-group signal for `gid`: after an admission or a removal the queued
  /// claim is stale — polling it would hand the placement brain a lie (an "unknown" group that
  /// is now hosted, or one the embedder just retired).
  fn purge_unknown_signal(&mut self, gid: &G) {
    if self.unknown_seen.remove(gid) {
      self.unknown_pending.retain(|(g, _)| g != gid);
    }
  }

  /// Register a freshly DIALED connection (the driver dialed `expected`), returning the
  /// coordinator-assigned [`ConnId`] the driver keys its socket by. A hello authenticating as any
  /// other id closes the connection ([`TransportError::UnexpectedPeer`]).
  pub fn on_dial_open(&mut self, expected: I, record: R, now: Instant) -> ConnId {
    self.refresh_local_id();
    let id = self.alloc_conn_id();
    self.router.register_dial(id, expected, record, now);
    id
  }

  /// Register a freshly ACCEPTED connection (an inbound socket — no dial expectation), returning the
  /// coordinator-assigned [`ConnId`] the driver keys its socket by.
  pub fn on_accept_open(&mut self, record: R, now: Instant) -> ConnId {
    self.refresh_local_id();
    let id = self.alloc_conn_id();
    self.router.register_accept(id, record, now);
    id
  }

  /// Allocate the next [`ConnId`] from the monotonic counter.
  ///
  /// # Panics
  /// Panics if the `u64` connection-id space is exhausted (unreachable in practice).
  fn alloc_conn_id(&mut self) -> ConnId {
    let id = ConnId(self.next_conn_id);
    self.next_conn_id = self
      .next_conn_id
      .checked_add(1)
      .expect("connection id space exhausted");
    id
  }

  /// Hand the router this host's own id once a group exists — before then a multi host has no
  /// identity, so the self-ID gate stays a no-op (matching the QUIC coordinator).
  fn refresh_local_id(&mut self) {
    if let Some(me) = self.multi.host_id() {
      let me = me.cheap_clone();
      self.router.set_local_id(me);
    }
  }

  /// Tear down a driver-closed connection.
  pub fn on_conn_close(&mut self, conn: ConnId) {
    self.router.remove(conn);
  }

  /// The next connection the transport closed on its own initiative, with the fault if any.
  pub fn poll_conn_closed(&mut self) -> Option<(ConnId, Option<TransportError>)> {
    self.router.poll_conn_closed()
  }

  /// Feed inbound bytes from `conn`: decode each frame, resolve its group's store through `stores`,
  /// feed the owning group's endpoint, then flush every group's resulting outbound messages. A frame
  /// whose group has no store is dropped (the sender retries on its own cadence) — and when it is
  /// initial-shaped for a group neither hosted nor tombstoned, surfaced once via
  /// [`poll_unknown_group`](Self::poll_unknown_group); a group tag that does not decode as `G`
  /// closes the connection as integrity-suspect (reported via
  /// [`poll_conn_closed`](Self::poll_conn_closed)).
  ///
  /// THE GENERATION FENCE: each frame carries its sender's incarnation stamp for the tagged gid,
  /// checked against this host's DURABLE admission floor read through `stores` — a below-floor
  /// stamp means the sender speaks for a RETIRED incarnation, so the frame drops exactly as a
  /// tombstoned gid's does, counted by [`fenced_frames_dropped`](Self::fenced_frames_dropped).
  /// Reading the floor is why `stores` is also a [`FloorStore`]: the fence must survive a restart,
  /// which the volatile tombstone set does not.
  pub fn handle_conn_data<L, S, St>(
    &mut self,
    conn: ConnId,
    bytes: &[u8],
    eof: bool,
    now: impl Into<Now>,
    stores: &mut St,
  ) where
    L: LogStore,
    S: StableStore<NodeId = I>,
    St: GroupStores<G, L, S> + FloorStore<G>,
  {
    let now: Now = now.into();
    let mut decoded = Vec::new();
    let _ = self
      .router
      .handle_conn_data(conn, bytes, eof, now.mono(), &mut decoded);
    for (group_bytes, flags, generation, from, msg) in decoded {
      let Ok(group) = G::decode_exact(group_bytes) else {
        // A well-framed tag that is not a valid `G` is a systematic peer fault (a different
        // group-id type, or a single-group node on a multi-group cluster): every frame reproduces
        // it, and dropping silently would black-hole consensus traffic on a healthy-looking
        // connection. Close as integrity-suspect — the connection's remaining frames are equally
        // suspect.
        self.router.close(conn, Some(TransportError::Decode));
        break;
      };
      // Receive-side gate on the quiesce bit: it is only ever STAMPED on a leader's own
      // Heartbeat broadcast, so a flagged anything-else is a protocol violation (a buggy or
      // stale-version peer) — and honoring it would freeze this group on a message class that
      // deliberately emits no Wake. Close as integrity-suspect, the uniform violation policy.
      if flags & COALESCED_FLAG_QUIESCE != 0 && !msg.is_heartbeat() {
        self.router.close(conn, Some(TransportError::Decode));
        break;
      }
      // A tombstoned (removed, not cleared since) group's frame is a straggler from the group's
      // past life on this host: drop it silently — never a close (the shared connection carries
      // the live groups' traffic), never a control, and never an unknown-group signal (the
      // embedder retired the id; resurrecting it on a straggler's say-so would undo the
      // removal). Ordered AFTER the integrity gates (a malformed tag or violating flag still
      // closes) and BEFORE store resolution.
      if self.retired.contains(&group) {
        continue;
      }
      // THE DEBT-WINDOW FENCE: a fence-deferred absorb consumed this id's endpoint while its
      // stores await the union's capture — not yet tombstoned (the floor moves only at the
      // discharge), but every frame for it is exactly as moot, and an unknown-group advisory
      // here would prompt the embedder (or the factory, whose own gate also refuses) to revive
      // a husk beside the absorbed union. Same posture as the tombstone: drop silently.
      //
      // DELIBERATELY DEBT-ONLY. The admission doors also refuse a source named by a
      // latched-CLOSED park, but this fence must NOT: a park in that state is waiting for the
      // covering snapshot that resolves it, and the cure chain rides the CONSUMED SOURCE's own
      // frames. Dropping them here would starve the very cure the park is waiting on.
      if self.multi.debt_names(&group) {
        continue;
      }
      // THE GENERATION FENCE (see the method doc): the durable, restart-surviving counterpart of
      // the tombstone above. Ordered right beside it — after the integrity gates, before store
      // resolution — so a retired incarnation's frames of EVERY class go equally inert.
      if !crate::floor_admits(stores.floor(&group), generation) {
        self.note_fenced_frame(&group, generation, &from);
        continue;
      }
      if let Some((log, stable)) = stores.stores(&group) {
        let wake = Self::is_wake_class(&msg);
        let beat_term = msg.term();
        let sender = from.cheap_clone();
        // The core's own sender-authenticity rule, mirrored pre-dispatch: a payload naming a
        // different node than the authenticated transport peer is dropped by the endpoint, so its
        // quiesce flag must drop with it (the post-dispatch gate alone cannot see this case when
        // the transport peer IS the current leader relaying a foreign payload).
        let flags = if msg.from() == from {
          flags
        } else {
          flags & !COALESCED_FLAG_QUIESCE
        };
        // `None` here means `stores` resolved a group `MultiRaft` does not host — the same
        // unhosted-group drop as a missing store; an unhosted entry's flags drop with it.
        if self
          .multi
          .handle_message(&group, now, log, stable, from, msg)
          .is_some()
        {
          let flags = self.accepted_flags(&group, flags, beat_term, &sender);
          self.push_dispatch_controls(&group, wake, flags);
        }
      } else if is_initial_shaped(&msg) && !self.multi.contains_group(&group) {
        // Neither store-resolvable nor hosted (nor tombstoned — gated above): a live peer is
        // actively soliciting a group this host does not carry. Surface it ONCE to the
        // embedder's placement brain; every other kind for the group drops silently.
        self.note_unknown_group(group, from);
      }
    }
    self.flush();
  }

  /// How many inbound frames the generation fence has dropped — a below-floor incarnation stamp
  /// for a gid this host has retired. Purely observational: nothing consumes it for control flow,
  /// and the cure for the sender is the embedder's catalog reap, never a reply.
  #[must_use]
  pub fn fenced_frames_dropped(&self) -> u64 {
    self.fenced_dropped
  }

  /// Record one fence drop: the counter plus, under the `tracing` feature, the
  /// `(gid, generation, peer)` event the design's observability signal names.
  fn note_fenced_frame(&mut self, group: &G, generation: u64, from: &I) {
    self.fenced_dropped = self.fenced_dropped.saturating_add(1);
    #[cfg(feature = "tracing")]
    tracing::debug!(
      target: "sailing::transport",
      group = %group,
      generation,
      peer = ?from,
      "retired-incarnation frame fenced at demux"
    );
    #[cfg(not(feature = "tracing"))]
    let _ = (group, generation, from);
  }

  /// Strip the quiesce flag unless the dispatched beat was ACCEPTED as current-leader contact:
  /// after the dispatch this group must be a follower of exactly `sender` at exactly the beat's
  /// term. A hosted-group dispatch verdict alone proves nothing — the core silently drops a
  /// sender-mismatched payload and a stale-term beat, and freezing timers on a message Raft threw
  /// away would quiesce a group on a rejected input's say-so. `Wake` is deliberately NOT gated:
  /// waking on a rejected message is the conservative direction.
  fn accepted_flags(&self, group: &G, flags: u8, beat_term: crate::Term, sender: &I) -> u8 {
    if flags & COALESCED_FLAG_QUIESCE == 0 {
      return flags;
    }
    let accepted = self.multi.group(group).is_some_and(|ep| {
      !ep.role().is_leader() && ep.term() == beat_term && ep.leader().as_ref() == Some(sender)
    });
    if accepted {
      flags
    } else {
      flags & !COALESCED_FLAG_QUIESCE
    }
  }

  /// Propose a command on `group`'s leader, replicating immediately. `None` if no such group.
  #[must_use = "`None` means no group with this id is hosted — the call did nothing"]
  pub fn submit_propose<L, S>(
    &mut self,
    group: &G,
    now: impl Into<Now>,
    log: &mut L,
    stable: &S,
    cmd: &F::Command,
  ) -> Option<Result<Index, ProposeError<I>>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let now: Now = now.into();
    let r = self.multi.propose(group, now, log, stable, cmd)?;
    let _ = self.multi.flush_appends(group, now, log, stable);
    self.flush();
    Some(r)
  }

  /// Append a proposal on `group` WITHOUT fanning out its `AppendEntries` now; the caller drives
  /// replication afterward via [`flush_appends`](Self::flush_appends), once per crank, so a burst
  /// coalesces into one broadcast per peer. Direct callers should prefer
  /// [`submit_propose`](Self::submit_propose). `None` if no such group.
  #[must_use = "`None` means no group with this id is hosted — the call did nothing"]
  pub fn submit_propose_deferred<L, S>(
    &mut self,
    group: &G,
    now: impl Into<Now>,
    log: &mut L,
    stable: &S,
    cmd: &F::Command,
  ) -> Option<Result<Index, ProposeError<I>>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let now: Now = now.into();
    let r = self.multi.propose(group, now, log, stable, cmd)?;
    self.flush();
    Some(r)
  }

  /// Ship `group`'s coalesced replication batch and flush. `None` if no such group.
  #[must_use = "`None` means no group with this id is hosted — the call did nothing"]
  pub fn flush_appends<L, S>(
    &mut self,
    group: &G,
    now: impl Into<Now>,
    log: &L,
    stable: &S,
  ) -> Option<()>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let now: Now = now.into();
    self.multi.flush_appends(group, now, log, stable)?;
    self.flush();
    Some(())
  }

  /// Propose a membership change (single-step) on `group`, replicating immediately. `None` if no
  /// such group.
  #[must_use = "`None` means no group with this id is hosted — the call did nothing"]
  pub fn propose_conf_change<L, S>(
    &mut self,
    group: &G,
    now: impl Into<Now>,
    log: &mut L,
    stable: &S,
    cc: crate::ConfChange<I>,
  ) -> Option<Result<Index, ProposeError<I>>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let now: Now = now.into();
    let r = self
      .multi
      .propose_conf_change(group, now, log, stable, cc)?;
    let _ = self.multi.flush_appends(group, now, log, stable);
    self.flush();
    Some(r)
  }

  /// Propose a membership change (joint-consensus capable) on `group`, replicating immediately.
  /// `None` if no such group.
  #[must_use = "`None` means no group with this id is hosted — the call did nothing"]
  pub fn propose_conf_change_v2<L, S>(
    &mut self,
    group: &G,
    now: impl Into<Now>,
    log: &mut L,
    stable: &S,
    cc: crate::ConfChangeV2<I>,
  ) -> Option<Result<Index, ProposeError<I>>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let now: Now = now.into();
    let r = self
      .multi
      .propose_conf_change_v2(group, now, log, stable, cc)?;
    let _ = self.multi.flush_appends(group, now, log, stable);
    self.flush();
    Some(r)
  }

  /// Propose a cluster-wide read-mode migration on `group`, replicating immediately. `None` if no
  /// such group.
  #[must_use = "`None` means no group with this id is hosted — the call did nothing"]
  pub fn propose_read_mode_change<L, S>(
    &mut self,
    group: &G,
    now: impl Into<Now>,
    log: &mut L,
    stable: &S,
    mode: crate::ReadOnlyOption,
  ) -> Option<Result<Index, ProposeError<I>>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let now: Now = now.into();
    let r = self
      .multi
      .propose_read_mode_change(group, now, log, stable, mode)?;
    let _ = self.multi.flush_appends(group, now, log, stable);
    self.flush();
    Some(r)
  }

  /// Propose a group SPLIT on `group` (the parent), replicating immediately (see
  /// [`MultiRaft::propose_split`] for the container gates). The coordinator adds the
  /// PROPOSE-TIME leg of the two-point floor check through the caller's `floors` seam: a child
  /// incarnation below its persisted admission floor — or the reserved `u64::MAX` sentinel —
  /// refuses BEFORE anything is appended (the drivers' materialization edge keeps the
  /// authoritative recheck, where a follower's local removal history may differ). `None` if no
  /// such group.
  #[must_use = "`None` means no group with this id is hosted — nothing was proposed"]
  #[allow(clippy::too_many_arguments)]
  pub fn propose_split<L, S>(
    &mut self,
    group: &G,
    now: impl Into<Now>,
    log: &mut L,
    stable: &S,
    child: &G,
    child_gen: u64,
    instruction: bytes::Bytes,
    floors: &impl FloorStore<G>,
  ) -> Option<Result<Index, crate::SplitError<I>>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    if !self.multi.contains_group(group) {
      return None;
    }
    // A locally-tombstoned child could never materialize its fork here (admission refuses
    // `Retired`), so refuse the split at propose — the entry is never appended. The floor leg
    // fences a below-floor incarnation; this leg fences a removed one, beside it.
    if self.retired.contains(child) {
      return Some(Err(crate::SplitError::ChildRetired));
    }
    if let Err(e) = validate_floor(floors.floor(child), child_gen) {
      return Some(Err(match e {
        CreateGroupError::BelowFloor { floor } => crate::SplitError::BelowFloor { floor },
        _ => crate::SplitError::ReservedGeneration,
      }));
    }
    let now: Now = now.into();
    let r = self
      .multi
      .propose_split(group, now, log, stable, child, child_gen, instruction)?;
    let _ = self.multi.flush_appends(group, now, log, stable);
    self.flush();
    Some(r)
  }

  /// Propose a merge FREEZE of `source` into `target` (see [`MultiRaft::prepare_merge`] for the
  /// container gates), replicating immediately. The source's log resolves through the caller's
  /// `stores` seam — the container's claimed-target gate reads co-hosted claimants' logs, not
  /// just the source's own. The coordinator adds the merge's floor leg through the caller's
  /// `floors` seam: a participant whose CURRENT incarnation sits below its persisted admission
  /// floor is a stale survivor of a fenced incarnation — refused BEFORE anything is appended,
  /// exactly as the split delegator fences its child. `None` if no group `source` is hosted.
  #[must_use = "`None` means no group with this id is hosted — nothing was proposed"]
  pub fn prepare_merge<L, S, St>(
    &mut self,
    source: &G,
    now: impl Into<Now>,
    stores: &mut St,
    target: &G,
    floors: &impl FloorStore<G>,
  ) -> Option<Result<Index, crate::MergeError<I>>>
  where
    St: GroupStores<G, L, S>,
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    if !self.multi.contains_group(source) {
      return None;
    }
    if let Err(e) = self.merge_floor_check(source, target, floors) {
      return Some(Err(e));
    }
    let now: Now = now.into();
    let r = self.multi.prepare_merge(source, now, stores, target)?;
    if let Some((log, stable)) = stores.stores(source) {
      let _ = self.multi.flush_appends(source, now, log, stable);
    }
    self.flush();
    Some(r)
  }

  /// Propose the merge ABSORB on `target` (see [`MultiRaft::commit_merge`]), replicating
  /// immediately, with the same per-call floor leg as
  /// [`prepare_merge`](Self::prepare_merge). `None` if no group `target` is hosted.
  #[must_use = "`None` means no group with this id is hosted — nothing was proposed"]
  pub fn commit_merge<L, S>(
    &mut self,
    target: &G,
    now: impl Into<Now>,
    log: &mut L,
    stable: &S,
    source: &G,
    floors: &impl FloorStore<G>,
  ) -> Option<Result<Index, crate::MergeError<I>>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    if !self.multi.contains_group(target) {
      return None;
    }
    if let Err(e) = self.merge_floor_check(source, target, floors) {
      return Some(Err(e));
    }
    let now: Now = now.into();
    let r = self.multi.commit_merge(target, now, log, stable, source)?;
    let _ = self.multi.flush_appends(target, now, log, stable);
    self.flush();
    Some(r)
  }

  /// Propose the merge ABORT on `target` (see [`MultiRaft::rollback_merge`]): the target-side
  /// abort entry, totally ordered against `CommitMerge` on the target's own log, replicating
  /// immediately. No floor leg: aborting is always legitimate on groups this host still runs.
  /// `None` if no group `target` is hosted.
  #[must_use = "`None` means no group with this id is hosted — nothing was proposed"]
  pub fn rollback_merge<L, S>(
    &mut self,
    target: &G,
    now: impl Into<Now>,
    log: &mut L,
    stable: &S,
    source: &G,
  ) -> Option<Result<Index, crate::MergeError<I>>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let now: Now = now.into();
    let r = self
      .multi
      .rollback_merge(target, now, log, stable, source)?;
    let _ = self.multi.flush_appends(target, now, log, stable);
    self.flush();
    Some(r)
  }

  /// The merge verbs' floor leg: BOTH participants' current incarnations must clear their
  /// persisted admission floors — an under-floor participant is a fenced incarnation's stale
  /// survivor, and anchoring a merge on it would resurrect exactly what the floor buried.
  fn merge_floor_check(
    &self,
    source: &G,
    target: &G,
    floors: &impl FloorStore<G>,
  ) -> Result<(), crate::MergeError<I>> {
    for gid in [source, target] {
      let floor = floors.floor(gid);
      if !crate::floor_admits(floor, self.multi.group_gen(gid)) {
        return Err(crate::MergeError::BelowFloor { floor });
      }
    }
    Ok(())
  }

  /// Resolve every parked merge that local facts now decide (see
  /// [`MultiRaft::service_merge_applies`]) — called once per crank by the driver after the
  /// per-group storage drains. On a resolved ABSORB or a husk RETIREMENT the coordinator TOMBSTONES
  /// the source id: its straggler frames drop silently from here on (the P5 wire story, unchanged),
  /// while the terminal floor the DRIVER persists from the returned resolutions is what makes the
  /// refusal survive restarts. Aborted resolutions touch nothing here — the source group is still live.
  pub fn service_merge_applies<L, S, St>(
    &mut self,
    now: impl Into<Now>,
    stores: &mut St,
  ) -> Vec<crate::MergeResolution<G>>
  where
    St: crate::GroupStores<G, L, S> + FloorStore<G>,
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let resolutions = self.multi.service_merge_applies(now, stores);
    for r in &resolutions {
      // A `Merged` source (absorbed) and a `Retired` source (husk dissolved) both LEAVE the container
      // terminally floored — tombstone each id. `Aborted` leaves both groups live. `CaptureFailed`
      // also removed the source endpoint, but does NOT floor it — its stores are preserved for a
      // restart re-park — so it is NOT tombstoned here: a terminal refusal would fence exactly the
      // incarnation the restart must restore.
      let source = match r {
        crate::MergeResolution::Merged { source, .. }
        | crate::MergeResolution::Retired { source } => source,
        // `Absorbed` is the CaptureFailed posture wire-side: the source endpoint is gone but its
        // stores and floor are preserved for the debt's discharge (or a crash re-park), so no
        // tombstone — the demux fence covers the window through the debt itself, not `retired`.
        crate::MergeResolution::Aborted { .. }
        | crate::MergeResolution::CaptureFailed { .. }
        | crate::MergeResolution::Absorbed { .. } => {
          continue;
        }
      };
      self.quiesce_intents.remove(source);
      // Drop the source's latest control; a still-queued gid goes inert and is skipped at poll.
      self.control_state.remove(source);
      self.retired.insert(source.cheap_clone());
      self.purge_unknown_signal(source);
    }
    resolutions
  }

  /// Whether any hosted target's outstanding capture debt names `gid` as its absorbed source
  /// (see [`MultiRaft::debt_names`]) — the drivers' factory and lifecycle gates consult this.
  ///
  /// The FACTORY needs no separate park leg: a solicited build goes through `create_group`, whose
  /// `validate_new_group` already refuses a committed-consumed source on both legs — the debt this
  /// predicate reports and the latched-CLOSED park beside it.
  pub fn debt_names(&self, gid: &G) -> bool {
    self.multi.debt_names(gid)
  }

  /// The next committed, relay-ready fork from any hosted group (see
  /// [`MultiRaft::poll_pending_fork`]) — the driver drains this every crank BEFORE its storage
  /// crank, so the same crank's engine flush covers the materialization.
  pub fn poll_pending_fork(&mut self) -> Option<crate::GroupFork<G, I, F>> {
    self.poll_pending_fork_with(&crate::NoHold)
  }

  /// [`poll_pending_fork`](Self::poll_pending_fork) over the caller's [`ForkGate`](crate::ForkGate), AUGMENTED
  /// with this coordinator's tombstone set before the container sees it — never merely
  /// forwarded, because `retired` is state the container does not hold.
  pub fn poll_pending_fork_with(
    &mut self,
    gate: &impl crate::ForkGate<G>,
  ) -> Option<crate::GroupFork<G, I, F>> {
    self.multi.poll_pending_fork_with(&CoordGate {
      retired: &self.retired,
      outer: gate,
    })
  }

  /// Resolve the fork staged at exactly `split_index` on `parent` (see
  /// [`MultiRaft::lift_fork_barrier`]): the driver reports the child's baseline flush-durable,
  /// and the parent's snapshot fence over that index releases.
  pub fn lift_fork_barrier(&mut self, parent: &G, split_index: Index) {
    self.multi.lift_fork_barrier(parent, split_index);
  }

  /// The next `(parent, child)` SPLIT-CONFLICT signal, left queued (see
  /// [`MultiRaft::peek_split_conflict`]): the driver publishes it on its bounded lifecycle
  /// tail and consumes via [`poll_split_conflict`](Self::poll_split_conflict) only once the
  /// tail accepts — a full tail must defer the one-shot cue, never erase it.
  #[must_use]
  pub fn peek_split_conflict(&self) -> Option<(G, G)> {
    self.multi.peek_split_conflict()
  }

  /// Drain the next `(parent, child)` SPLIT-CONFLICT signal — a committed fork parked because
  /// its child id is already hosted (see [`MultiRaft::poll_split_conflict`]); the driver
  /// surfaces it on its lifecycle tail for the placement brain, consuming here only after the
  /// tail accepted the peeked event.
  pub fn poll_split_conflict(&mut self) -> Option<(G, G)> {
    self.multi.poll_split_conflict()
  }

  /// The next `(parent, child)` SPLIT-REFUSAL signal, left queued (see
  /// [`MultiRaft::peek_split_refusal`]): the driver publishes it on its bounded lifecycle tail and
  /// consumes only once the tail accepts.
  #[must_use]
  pub fn peek_split_refusal(&self) -> Option<(G, G)> {
    self.multi.peek_split_refusal()
  }

  /// Drain the next `(parent, child)` SPLIT-REFUSAL signal — a committed fork the relay abandoned
  /// deliberately (see [`MultiRaft::poll_split_refusal`]).
  pub fn poll_split_refusal(&mut self) -> Option<(G, G)> {
    self.multi.poll_split_refusal()
  }

  /// Drain the next `(parent, lineage)` relay-guard advance owed to the caller's DURABLE lineage
  /// record (see [`MultiRaft::poll_relay_guard_advance`]). Removal-time fork abandonment is its
  /// only source; mirror it beside the removal's floor write.
  pub fn poll_relay_guard_advance(&mut self) -> Option<(G, u64)> {
    self.multi.poll_relay_guard_advance()
  }

  /// Whether `gid`'s fork relay still owes something — a held head fork or an undelivered
  /// conflict/refusal signal (see [`MultiRaft::fork_relay_pending`]).
  #[must_use]
  pub fn fork_relay_pending(&self, gid: &G) -> bool {
    self.multi.fork_relay_pending(gid)
  }

  /// Drain the next FAIL-STOPPED group id (see [`MultiRaft::poll_poisoned`]); the driver surfaces
  /// it on its lifecycle tail for the placement brain. Best-effort, like the other observations.
  pub fn poll_poisoned(&mut self) -> Option<G> {
    self.multi.poll_poisoned()
  }

  /// Drain the next STRUCTURALLY HELD merge (see [`MultiRaft::poll_merge_blocked`]); the driver
  /// surfaces it on its lifecycle tail for the placement brain. Best-effort, like the other
  /// observations: the container re-derives the hold every crank, so a dropped signal costs a
  /// notification and nothing else.
  pub fn poll_merge_blocked(&mut self) -> Option<crate::MergeBlocked<G>> {
    self.multi.poll_merge_blocked()
  }

  /// The held-merge queue's head WITHOUT consuming it (see [`MultiRaft::peek_merge_blocked`]) —
  /// the drivers' delivered-before-consumed read: publish first, consume only on acceptance.
  pub fn peek_merge_blocked(&self) -> Option<crate::MergeBlocked<G>> {
    self.multi.peek_merge_blocked()
  }

  /// Initiate a linearizable read on `group`; the resulting `ReadState` surfaces via
  /// [`poll_event`](Self::poll_event) stamped with the group. `None` if no such group.
  #[must_use = "`None` means no group with this id is hosted — the call did nothing"]
  pub fn read_index<L, S>(
    &mut self,
    group: &G,
    now: impl Into<Now>,
    log: &L,
    stable: &S,
    context: bytes::Bytes,
  ) -> Option<Result<(), crate::ReadIndexError>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let now: Now = now.into();
    let r = self.multi.read_index(group, now, log, stable, context)?;
    self.flush();
    Some(r)
  }

  /// Begin transferring `group`'s leadership to `to`. `None` if no such group.
  #[must_use = "`None` means no group with this id is hosted — the call did nothing"]
  pub fn transfer_leader<L, S>(
    &mut self,
    group: &G,
    now: impl Into<Now>,
    log: &L,
    stable: &S,
    to: I,
  ) -> Option<Result<(), crate::TransferError<I>>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let now: Now = now.into();
    let r = self.multi.transfer_leader(group, now, log, stable, to)?;
    self.flush();
    Some(r)
  }

  /// The bound connection for `peer`, if any — the driver's input for its redial policy.
  pub fn conn_of(&self, peer: &I) -> Option<ConnId> {
    self.router.conn_of(peer)
  }

  /// Fire `group`'s timers (and the transport's handshake reaping), then flush. `None` if no such
  /// group.
  #[must_use = "`None` means no group with this id is hosted — the call did nothing"]
  pub fn handle_timeout<L, S>(
    &mut self,
    group: &G,
    now: impl Into<Now>,
    log: &mut L,
    stable: &mut S,
  ) -> Option<()>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let now: Now = now.into();
    self.router.reap_handshakes(now.mono());
    self.multi.handle_timeout(group, now, log, stable)?;
    self.flush();
    Some(())
  }

  /// Drain `group`'s storage completions, then flush. `None` if no such group.
  #[must_use = "`None` means no group with this id is hosted — the call did nothing"]
  pub fn handle_storage<L, S>(
    &mut self,
    group: &G,
    now: impl Into<Now>,
    log: &mut L,
    stable: &mut S,
  ) -> Option<StorageProgress>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let now: Now = now.into();
    let progress = self.multi.handle_storage(group, now, log, stable)?;
    self.flush();
    Some(progress)
  }

  /// Drain queued outbound wire bytes as `(conn, bytes)` pairs for the driver to write. This is
  /// the drain-end chokepoint where the crank's batched heartbeats ship (one coalesced frame per
  /// peer — see `flush`), so every `handle_*` call's beats leave with that call's
  /// transmit drain.
  pub fn poll_transmit(&mut self) -> Vec<(ConnId, Vec<u8>)> {
    self.ship_heartbeats();
    self.router.poll_transmit()
  }

  /// Record the intent to QUIESCE `group`: its next heartbeat broadcast is stamped with the
  /// quiesce flag (every copy in that broadcast — all followers hear the promise), after which the
  /// intent clears and [`is_quiescing`](Self::is_quiescing) reports `false`. The driver then stops
  /// arming the group's timers; each follower surfaces [`GroupControl::Quiesce`] to its own driver.
  /// A no-op for an unhosted group.
  pub fn mark_quiescing(&mut self, group: &G) {
    if self.multi.contains_group(group) {
      self.quiesce_intents.insert(group.cheap_clone());
    }
  }

  /// Whether `group`'s quiesce intent is still pending (its flagged beat has not yet been stamped).
  /// The driver's cue to move the group into its quiesced set once this flips to `false`.
  #[must_use]
  pub fn is_quiescing(&self, group: &G) -> bool {
    self.quiesce_intents.contains(group)
  }

  /// Cancel a pending quiesce intent for `group` (a no-op if none). The driver calls this on
  /// EVERY un-quiesce trigger — a wake control, a local command, a connection loss, a leadership
  /// change — so an intent recorded before the wake can never be stamped onto a later beat: the
  /// eligibility that justified it no longer holds.
  pub fn cancel_quiescing(&mut self, group: &G) {
    self.quiesce_intents.remove(group);
  }

  /// Drain the next group-scoped scheduling signal — each group's LATEST control, in first-signal
  /// order (see [`GroupControl`]). Skips a queued gid whose control was purged (removal/merge).
  pub fn poll_group_control(&mut self) -> Option<(G, GroupControl)> {
    while let Some(gid) = self.controls.pop_front() {
      if let Some(ctrl) = self.control_state.remove(&gid) {
        return Some((gid, ctrl));
      }
    }
    None
  }

  /// The transport's OWN earliest deadline — handshake reaping AND validated-connection liveness
  /// (idle-peer keep-alive probes and silent-peer reaping) — without any group's consensus deadline.
  /// This is the [`poll_timeout`](Self::poll_timeout) decomposition a quiescing driver needs: it
  /// folds this with the non-quiesced subset of [`deadlines`](Self::deadlines) instead of the
  /// all-groups aggregate, so a fully-quiesced host still wakes to keep its links alive and detect a
  /// blackhole.
  #[must_use]
  pub fn transport_timeout(&self) -> Option<Instant> {
    match (
      self.router.next_handshake_deadline(),
      self.router.next_liveness_deadline(),
    ) {
      (Some(a), Some(b)) => Some(a.min(b)),
      (a, None) => a,
      (None, b) => b,
    }
  }

  /// The earliest deadline the driver must wake for: the minimum over every group's consensus
  /// deadline AND the transport's earliest deadline ([`transport_timeout`](Self::transport_timeout):
  /// handshake reaping plus validated-connection liveness). The transport half matters when no group
  /// surfaces a deadline at all (zero hosted groups, every group poisoned, or a host of non-voter
  /// learners) — and when every group is quiesced — so un-validated connections are reaped and idle
  /// links are probed / blackhole-reaped even then. On expiry call
  /// [`handle_transport_timeout`](Self::handle_transport_timeout) unconditionally and
  /// [`handle_timeout`](Self::handle_timeout) for whichever groups are due.
  #[must_use]
  pub fn poll_timeout(&self) -> Option<Instant> {
    match (self.multi.poll_timeout(), self.transport_timeout()) {
      (Some(a), Some(b)) => Some(a.min(b)),
      (a, None) => a,
      (None, b) => b,
    }
  }

  /// Fire the transport's own housekeeping — handshake-deadline reaping AND validated-connection
  /// liveness (probing idle peers, reaping silent/blackholed ones) — without touching any group.
  /// Call at every [`poll_timeout`](Self::poll_timeout) expiry: the surfaced deadline may be a
  /// transport deadline with no group due.
  pub fn handle_transport_timeout(&mut self, now: impl Into<Now>) {
    let now: Now = now.into();
    self.router.reap_handshakes(now.mono());
    self.router.service_liveness(now.mono());
  }

  /// Each group's next deadline — a driver's input for an aggregate timing wheel.
  pub fn deadlines(&self) -> impl Iterator<Item = (G, Instant)> + '_ {
    self.multi.deadlines()
  }

  /// Drain the next application event, stamped with its originating group.
  pub fn poll_event(&mut self) -> Option<(G, Event<I, F::Response>)> {
    self.multi.poll_event()
  }

  /// A group's endpoint, for observability (role, term, commit, the state machine). `None` if no
  /// such group.
  pub fn group(&self, gid: &G) -> Option<&Endpoint<I, F>> {
    self.multi.group(gid)
  }

  /// Route every group's queued outbound messages, stamping each frame with its group tag.
  /// `Heartbeat`/`HeartbeatResponse` are DIVERTED into per-peer batches (shipped coalesced at the
  /// [`poll_transmit`](Self::poll_transmit) chokepoint); everything else routes immediately as its
  /// own frame. Reordering a batched beat behind a same-crank `AppendEntries` is safe by
  /// construction: a heartbeat's `commit` is clamped to the follower's acked match, so no beat can
  /// reference state its follower has not durably acknowledged.
  ///
  /// A group with a pending quiesce intent has EVERY beat copy in this drain stamped with the
  /// quiesce flag (the whole broadcast — each follower must hear the promise), and the intent is
  /// consumed at drain end.
  fn flush(&mut self) {
    let mut group_bytes = Vec::new();
    let mut stamped: BTreeSet<G> = BTreeSet::new();
    while let Some((group, out)) = self.multi.poll_message() {
      let (to, msg) = out.into_parts();
      if msg.is_heartbeat() || msg.is_heartbeat_response() {
        // The quiesce flag rides ONLY the leader's own Heartbeat broadcast. Stamping a
        // HeartbeatResponse would leak a STALE intent: a leader that marked quiescing and was
        // then woken or deposed before its next beat still responds to the NEW leader's beats —
        // a flagged response would freeze the very leader that never chose to quiesce.
        let flags = if msg.is_heartbeat() && self.quiesce_intents.contains(&group) {
          stamped.insert(group.cheap_clone());
          COALESCED_FLAG_QUIESCE
        } else {
          0
        };
        let mut gb = Vec::new();
        group.encode(&mut gb);
        let generation = self.multi.group_gen(&group);
        self
          .hb_batches
          .entry(to)
          .or_default()
          .push((flags, gb, generation, msg));
      } else {
        group_bytes.clear();
        group.encode(&mut group_bytes);
        let generation = self.multi.group_gen(&group);
        self.router.route(&group_bytes, generation, to, &msg);
      }
    }
    for group in &stamped {
      self.quiesce_intents.remove(group);
    }
  }

  /// Ship the batched heartbeats: a batch of ONE unflagged beat goes as a normal single-message
  /// frame (no format change for the trivial case); anything else — many beats, or a flagged one
  /// (only a coalesced entry has a flags byte) — ships as coalesced frames.
  ///
  /// An entry whose encoded size ALONE exceeds the coalesced budget is diverted to a normal
  /// single-message frame: the receiver enforces the budget on coalesced frames, so an oversized
  /// coalesced emission would be rejected on every delivery and churn the shared connection.
  /// Real heartbeat entries are bounded BY CONSTRUCTION (the wire heartbeat context is the read
  /// machinery's 8-byte internal round token, never the caller's context), so this divert is
  /// defense-in-depth — send/receive symmetry that holds whatever rides the batch in the future.
  /// A flagged oversized beat additionally RE-ARMS the quiesce intent instead of shipping the
  /// flag (a normal frame carries none): the promise waits for a beat that fits.
  fn ship_heartbeats(&mut self) {
    if self.hb_batches.is_empty() {
      return;
    }
    let mut scratch = Vec::new();
    for (to, batch) in core::mem::take(&mut self.hb_batches) {
      let mut fitting: Vec<CoalescedEntry<I>> = Vec::with_capacity(batch.len());
      for (flags, group_bytes, generation, msg) in batch {
        scratch.clear();
        crate::wire::encode_message(&msg, &mut scratch);
        let entry_len = crate::transport::frame::coalesced_entry_len(
          group_bytes.len(),
          generation,
          scratch.len(),
        );
        if entry_len > crate::transport::frame::COALESCED_FRAME_BUDGET {
          // Re-arm only for a group still hosted: a lifecycle removal between the stamp and this
          // divert must not leave a dormant intent behind for a re-created id.
          if flags & COALESCED_FLAG_QUIESCE != 0
            && let Ok(gid) = G::decode_exact(bytes::Bytes::from(group_bytes.clone()))
            && self.multi.contains_group(&gid)
          {
            self.quiesce_intents.insert(gid);
          }
          self
            .router
            .route(&group_bytes, generation, to.cheap_clone(), &msg);
        } else {
          fitting.push((flags, group_bytes, generation, msg));
        }
      }
      match fitting.as_slice() {
        [] => {}
        [(0, group_bytes, generation, msg)] => {
          self.router.route(group_bytes, *generation, to, msg);
        }
        _ => {
          self.router.route_coalesced(to, &fitting);
        }
      }
    }
  }

  /// Queue the dispatch-driven [`GroupControl`]s for one delivered message: a `Wake` for every
  /// wake-class kind (see [`GroupControl::Wake`] — an idle heartbeat response is absorbed), then a
  /// `Quiesce` if the entry carried the flag — flag AFTER wake, so a flagged
  /// beat nets quiesced. Same-group controls collapse to the latest (a burst of appends is one `Wake`).
  fn push_dispatch_controls(&mut self, group: &G, wake: bool, flags: u8) {
    if wake {
      self.push_control(group, GroupControl::Wake);
    }
    if flags & COALESCED_FLAG_QUIESCE != 0 {
      self.push_control(group, GroupControl::Quiesce);
    }
  }

  /// Whether a delivered message is WAKE-class for its group. The absorbed complement is a
  /// `HeartbeatResponse` carrying NO wedged-park boundary — with the heartbeat-response append pump
  /// gated and quiesce eligibility excluding lagging peers, a quiescing group's FINAL flagged round
  /// is precisely `Heartbeat` + `HeartbeatResponse`, so absorbing that one response is all it takes
  /// for the round to die out instead of re-waking either side (see [`GroupControl::Wake`] for the
  /// safety argument).
  ///
  /// A response whose `stuck_boundary` is nonzero is NOT that response. What the absorbed class
  /// admits is the zero-information idle ack — a peer reporting that nothing has changed and that
  /// nothing is owed. A boundary is the opposite: actionable non-idle state that the leader cannot
  /// see by any other means, since a parked replica is not log-lagging (its park sits above a
  /// fully-replicated log, so the pump predicate reads it as caught up) and its stalled apply drain
  /// is purely local. Absorbing it would recreate exactly the blind spot the field exists to
  /// pierce: the group would settle with a replica wedged forever and the only party able to cure
  /// it asleep. The cost is bounded by the advertiser's slow-tick cadence — one response per
  /// election timeout — and by the park's own lifetime, since the advertisement dies with it.
  fn is_wake_class(msg: &Message<I>) -> bool {
    match msg {
      Message::HeartbeatResponse(hbr) => hbr.stuck_boundary() != Index::ZERO,
      _ => true,
    }
  }

  fn push_control(&mut self, group: &G, ctrl: GroupControl) {
    // Latest-wins: overwrite the group's control, enqueuing the gid only when it was absent so the
    // queue holds each dirty group once. A later push in the same drain cycle just replaces the
    // value (Quiesce after Wake nets Quiesce; a Wake after Quiesce re-arms), which the single
    // queued visit then delivers.
    if self
      .control_state
      .insert(group.cheap_clone(), ctrl)
      .is_none()
    {
      self.controls.push_back(group.cheap_clone());
    }
  }
}

impl<G, I, F, R> Default for MultiStreamCoordinator<G, I, F, R>
where
  G: GroupId,
  I: NodeId,
  F: StateMachine,
  F::Command: Data,
  F::Snapshot: Data,
  F::Error: core::error::Error,
  R: RecordIo,
{
  fn default() -> Self {
    Self::new()
  }
}

/// The driver's [`ForkGate`](crate::ForkGate) AUGMENTED with the one fact only this coordinator holds: the
/// tombstone. `retired` is coordinator state — the container cannot see it, and a driver would
/// have to reach across the seam for it — so a relayed fork for a tombstoned id must learn its
/// answer here. It reads as OCCUPANCY, not refusal: `clear_tombstone` lifts a tombstone, so the
/// two-act rejoin is a window, never a verdict.
struct CoordGate<'a, G, Outer> {
  retired: &'a BTreeSet<G>,
  outer: &'a Outer,
}

impl<G, Outer> crate::ForkGate<G> for CoordGate<'_, G, Outer>
where
  G: GroupId,
  Outer: crate::ForkGate<G>,
{
  fn contains_group(&self, gid: &G) -> bool {
    self.retired.contains(gid) || self.outer.contains_group(gid)
  }

  fn floor(&self, gid: &G) -> u64 {
    self.outer.floor(gid)
  }
}

#[cfg(test)]
mod tests;
