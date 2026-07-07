//! `MultiQuicCoordinator<G, I, F, ID>`: the QUIC-transport multi-group super state machine.
//!
//! It composes a [`MultiRaft`] of single-group endpoints with the quinn-proto [`Bridge`], the
//! QUIC counterpart of the stream-transport `MultiStreamCoordinator`: inbound datagrams decode to
//! group-tagged frames demuxed to the owning group's endpoint, and each group's outbound messages
//! ride the shared per-peer QUIC streams back out stamped with that group's id. One connection per
//! peer carries every co-located group's traffic. Storage is per group: the datagram/transport
//! pumps resolve each decoded frame's group store through a caller-supplied [`GroupStores`], while
//! the single-group driving methods take the target group's store directly.

use core::error::Error;
use std::{
  collections::{BTreeMap, BTreeSet, VecDeque},
  net::SocketAddr,
  vec::Vec,
};

use quinn_proto::{ConnectionHandle, EcnCodepoint};

use super::{
  super::{
    Hello, IdentityCtx, IdentityOutcome, IdentitySource,
    bridge::{Bridge, DialError},
    crypto::{QuicOptions, mesh_connection_floor},
  },
  sni_for,
};
use crate::{
  CheapClone, Config, CreateGroupError, Data, Endpoint, Event, FloorStore, GroupControl, GroupId,
  GroupStores, Index, Instant, LogStore, Message, MultiRaft, NodeId, Now, ProposeError,
  StableStore, StateMachine, StorageProgress,
  multi::validate_floor,
  transport::{
    ClusterId, CoalescedEntry,
    coordinator::{UNKNOWN_GROUP_SIGNAL_CAP, is_initial_shaped},
    frame::COALESCED_FLAG_QUIESCE,
  },
};

/// A multi-group consensus node speaking QUIC: a [`MultiRaft`] composed with the quinn-proto bridge
/// and an [`IdentitySource`] (`ID`, the provided [`Hello`] by default).
///
/// # Identity
///
/// Identity is a NODE property, not a group one — a multi-Raft host is one physical node hosting
/// many groups, and the transport authenticates the node once per connection. The binding policy is
/// the single-group coordinator's verbatim: an unconditional `cluster == our cluster` cross-check, a
/// never-bind-our-own-id gate, then dialed→match-or-abort / accepted→adopt. The node's own id (the
/// preface self-claim and the self-id gate operand) is the host identity LATCHED by the first
/// admitted group — stable across group removals and zero-group windows, enforced at admission.
///
/// # Clock
///
/// The coordinator owns the sailing↔std clock adapter exactly as the single-group one does: its
/// surface speaks the crate [`Instant`], converting to `std::time::Instant` at every quinn boundary,
/// with the anchor captured LAZILY on the first call.
pub struct MultiQuicCoordinator<G, I, F, ID = Hello>
where
  F: StateMachine,
{
  multi: MultiRaft<G, I, F>,
  bridge: Bridge<I>,
  /// The identity source: extracts the candidate the coordinator's binding policy then checks.
  identity: ID,
  /// The cluster this coordinator authenticates for (the cross-check operand and the SNI component).
  cluster: ClusterId,
  /// The sailing↔std clock anchor `(crate_base, std_base)`, captured lazily on the first
  /// [`Self::quinn_now`].
  clock_anchor: Option<(Instant, std::time::Instant)>,
  /// The most recent `now` the driver passed in — the deterministic "immediate deadline"
  /// [`Self::poll_timeout`] returns while the bridge holds deferred work.
  last_now: Instant,
  /// The CONFIGURED connection cap from the options, kept so the live mesh floor can be recomputed
  /// against it each pump: committed configuration changes across the hosted groups grow the tracked
  /// peer set long after construction, and the effective cap must grow with it (see [`Self::pump`]).
  configured_max_connections: usize,
  /// Heartbeat/HeartbeatResponse batches diverted per peer by [`Self::pump`], shipped as coalesced
  /// frames at the [`Self::poll_transmit`] chokepoint — batching ACROSS pumps is what coalesces a
  /// driver's per-group `handle_timeout` sweep into one frame per peer per crank (the stream
  /// coordinator's discipline exactly).
  hb_batches: BTreeMap<I, Vec<CoalescedEntry<I>>>,
  /// Groups whose NEXT heartbeat broadcast carries the QUIESCE flag (set by
  /// [`Self::mark_quiescing`], consumed by the pump that stamps the beats).
  quiesce_intents: BTreeSet<G>,
  /// Group-scoped scheduling signals for the driver, in dispatch order (see [`GroupControl`]).
  controls: VecDeque<(G, GroupControl)>,
  /// Tombstoned group ids: REMOVED groups whose inbound entries drop silently and whose
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
}

impl<G, I, F> MultiQuicCoordinator<G, I, F, Hello>
where
  G: GroupId,
  I: NodeId,
  F: StateMachine,
  F::Command: Data,
  F::Snapshot: Data,
  F::Error: Error,
{
  /// A coordinator hosting no groups and no connections, authenticating peers with the provided
  /// [`Hello`] preface scheme. Create groups with [`create_group`](Self::create_group) /
  /// [`restore_group`](Self::restore_group).
  ///
  /// # Panics
  ///
  /// Panics if `opts` was not built with mandatory client-certificate authentication (a
  /// [`ClusterTls::build`](super::super::ClusterTls::build) bundle) — see
  /// [`with_identity`](Self::with_identity).
  #[must_use]
  pub fn new(opts: QuicOptions, cluster: ClusterId) -> Self {
    Self::with_identity(opts, None, cluster)
  }

  /// A coordinator using the provided [`Hello`] identity scheme, seeding quinn's connection-ID /
  /// token RNG with `rng_seed` (`None` = OS entropy; simulations pass a fixed seed).
  ///
  /// # Panics
  ///
  /// Panics if `opts` lacks mandatory client auth: the `Hello` self-claim is trustworthy only
  /// because mandatory mTLS already proved the peer holds a cluster cert. Arbitrary/no-auth options
  /// belong only behind [`dangerous_custom_identity`](Self::dangerous_custom_identity).
  #[must_use]
  pub fn with_identity(opts: QuicOptions, rng_seed: Option<[u8; 32]>, cluster: ClusterId) -> Self {
    assert!(
      opts.requires_client_auth(),
      "the provided Hello identity requires mandatory mTLS: build the options with \
       ClusterTls::build (so requires_client_auth() is true). Without mandatory client auth a \
       hello preface is a self-claim with no cryptographic backstop; arbitrary/no-auth options \
       belong only behind dangerous_custom_identity",
    );
    let identity = Hello::new(cluster);
    Self::build(opts, rng_seed, identity, cluster)
  }
}

impl<G, I, F, ID> MultiQuicCoordinator<G, I, F, ID>
where
  G: GroupId,
  I: NodeId,
  F: StateMachine,
  F::Command: Data,
  F::Snapshot: Data,
  F::Error: Error,
  ID: IdentitySource<I>,
{
  /// A coordinator with a CALLER-SUPPLIED [`IdentitySource`].
  ///
  /// # Hazard
  ///
  /// The embedder owns the identity-binding correctness of `src`, INCLUDING the attested cluster —
  /// see the single-group `dangerous_custom_identity`. Prefer the provided scheme unless a custom
  /// one is genuinely required.
  #[must_use]
  pub fn dangerous_custom_identity(
    opts: QuicOptions,
    rng_seed: Option<[u8; 32]>,
    src: ID,
    cluster: ClusterId,
  ) -> Self {
    Self::build(opts, rng_seed, src, cluster)
  }

  /// Shared constructor body. The connection cap starts at the empty-membership
  /// [`mesh_connection_floor`] (no groups are hosted yet) and is raised to the live union floor
  /// each pump as groups are created and their memberships commit — see [`Self::pump`].
  fn build(
    opts: QuicOptions,
    rng_seed: Option<[u8; 32]>,
    identity: ID,
    cluster: ClusterId,
  ) -> Self {
    let configured = opts.max_connections();
    let effective_cap = configured.max(mesh_connection_floor(0));
    let opts = opts.with_max_connections(effective_cap);
    Self {
      multi: MultiRaft::new(),
      bridge: Bridge::new(&opts, rng_seed),
      identity,
      cluster,
      clock_anchor: None,
      last_now: Instant::ORIGIN,
      configured_max_connections: configured,
      hb_batches: BTreeMap::new(),
      quiesce_intents: BTreeSet::new(),
      controls: VecDeque::new(),
      retired: BTreeSet::new(),
      unknown_pending: VecDeque::new(),
      unknown_seen: BTreeSet::new(),
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
    // A fresh group can widen the tracked-peer union; raise the connection cap now rather than at
    // the next pump, so accepts arriving in the gap are not statelessly refused.
    self.bridge.raise_max_connections(self.effective_cap());
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
    // Same cap-raise as `create_group`: the restored group widens the tracked-peer union.
    self.bridge.raise_max_connections(self.effective_cap());
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
    snapshot: bytes::Bytes,
    read_only: Option<crate::ReadOnlyOption>,
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
    self.multi.create_group_from_fork(
      gid, generation, config, now, seed, fsm, snapshot, read_only, boot_epoch, log, stable,
    )?;
    self.purge_unknown_signal(&key);
    // Same cap-raise as `create_group`: the forked group widens the tracked-peer union.
    self.bridge.raise_max_connections(self.effective_cap());
    Ok(())
  }

  /// Remove a group, returning its endpoint if present. Drops the group's pending quiesce intent
  /// and queued controls with it (its per-peer batched beats, if any, still ship — the receiver's
  /// unhosted-entry drop absorbs them) — and TOMBSTONES the id: inbound entries tagged with it
  /// drop silently, before store resolution and WITHOUT an unknown-group signal, and a
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
  pub fn remove_group(&mut self, gid: &G) -> Option<Endpoint<I, F>> {
    self.quiesce_intents.remove(gid);
    self.controls.retain(|(g, _)| g != gid);
    self.retired.insert(gid.cheap_clone());
    self.purge_unknown_signal(gid);
    self.multi.remove_group(gid)
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

  /// The durable admission floor a removal of `source` must persist to fence its outstanding
  /// merge-abort thaw obligation — see [`MultiRaft::abort_thaw_floor`]. A driver reads it BEFORE
  /// [`remove_group`](Self::remove_group) (which purges the obligation) and writes it through its
  /// `FloorStore`, so a crash-replay re-derivation discharges off the floor and a recreate can never
  /// repeat the frozen incarnation.
  #[must_use]
  pub fn abort_thaw_floor(&self, source: &G) -> Option<u64> {
    self.multi.abort_thaw_floor(source)
  }

  /// Whether `gid` is TOMBSTONED: removed and not explicitly cleared since — its inbound entries
  /// dropping silently and its re-creation refusing (see [`remove_group`](Self::remove_group)).
  /// Volatile — a restart starts clean.
  #[must_use]
  pub fn is_retired(&self, gid: &G) -> bool {
    self.retired.contains(gid)
  }

  /// Drain the next UNKNOWN-GROUP placement signal: `(group, authenticated sender)` for
  /// well-formed INITIAL-SHAPED traffic — a vote request, or a first-contact heartbeat carrying
  /// commit 0 — whose group this host neither hosts, nor resolves stores for, nor has
  /// tombstoned. The embedder's PLACEMENT BRAIN decides what to do with it: create/restore the
  /// group here (the soliciting peer's retry then completes the join) or ignore it (the
  /// coordinator keeps dropping the entries). Placement policy is deliberately NOT the
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

  /// Dial the node `peer` at `remote` — the shared connection carries every co-located group's
  /// traffic. Records `peer` as the connection's expectation (match-or-abort at validation) and
  /// derives the SNI server-name from `peer` + the cluster.
  ///
  /// # Errors
  /// The typed [`DialError`] when the dial is refused (the connection cap, or a peer id whose
  /// encoding exceeds the SNI scheme's 29-byte bound — [`connect_with_server_name`](Self::connect_with_server_name)
  /// is the escape hatch).
  pub fn connect(&mut self, now: Instant, remote: SocketAddr, peer: I) -> Result<(), DialError> {
    let server_name = sni_for(&peer, &self.cluster);
    self.connect_with_server_name(now, remote, peer, &server_name)
  }

  /// Dial `peer` at `remote` presenting an EXPLICIT SNI `server_name` — the dial-side counterpart of
  /// [`dangerous_custom_identity`](Self::dangerous_custom_identity), for a deployment whose id
  /// encodings exceed the derived label bound or whose certs use their own naming.
  ///
  /// # Errors
  /// The typed [`DialError`] when the dial is refused (see [`connect`](Self::connect)).
  pub fn connect_with_server_name(
    &mut self,
    now: Instant,
    remote: SocketAddr,
    peer: I,
    server_name: &str,
  ) -> Result<(), DialError> {
    let std_now = self.quinn_now(now);
    self.bridge.connect(std_now, remote, server_name, peer)?;
    Ok(())
  }

  /// Feed one inbound UDP datagram from `remote` into the QUIC stack, decode its group-tagged frames
  /// into consensus messages routed to their owning group's endpoint (resolved through `stores`),
  /// then pump every group's resulting outbound messages back out. A frame whose group has no store
  /// is dropped (the sender retries) — and when it is initial-shaped for a group neither hosted nor
  /// tombstoned, surfaced once via [`poll_unknown_group`](Self::poll_unknown_group); a group tag
  /// that does not decode as `G`, or an undecodable message body, closes the connection as
  /// integrity-suspect.
  ///
  /// `ecn` is the received ECN codepoint when the driver's socket reports one (`None` is always
  /// safe).
  pub fn handle_udp<L, S, St>(
    &mut self,
    now: impl Into<Now>,
    remote: SocketAddr,
    ecn: Option<EcnCodepoint>,
    data: &[u8],
    stores: &mut St,
  ) where
    L: LogStore,
    S: StableStore<NodeId = I>,
    St: GroupStores<G, L, S>,
  {
    let now: Now = now.into();
    let std_now = self.quinn_now(now.mono());
    self.bridge.handle_datagram(std_now, remote, ecn, data);
    self.drain_bridge(now, stores);
    self.pump(now.mono());
  }

  /// Fire all QUIC transport timers (quinn's connection timers plus the authentication deadline),
  /// drain the bridge into the owning groups (resolved through `stores`), then pump. The per-group
  /// CONSENSUS timers are fired separately via [`handle_timeout`](Self::handle_timeout); this is the
  /// shared transport half. Call it at EVERY [`poll_timeout`](Self::poll_timeout) expiry — the
  /// surfaced deadline may be a pure transport deadline (a quinn retransmit or the auth reap) with
  /// no group due — as well as whenever `poll_timeout` reports the bridge's immediate deferred
  /// work.
  pub fn handle_transport_timeout<L, S, St>(&mut self, now: impl Into<Now>, stores: &mut St)
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
    St: GroupStores<G, L, S>,
  {
    let now: Now = now.into();
    let std_now = self.quinn_now(now.mono());
    self.bridge.handle_timeout(std_now);
    self.drain_bridge(now, stores);
    self.pump(now.mono());
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
    self.pump(now.mono());
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
    self.pump(now.mono());
    Some(r)
  }

  /// Ship `group`'s coalesced replication batch and pump. `None` if no such group.
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
    self.pump(now.mono());
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
    self.pump(now.mono());
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
    self.pump(now.mono());
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
    self.pump(now.mono());
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
    self.pump(now.mono());
    Some(r)
  }

  /// Propose a merge FREEZE of `source` into `target` (see [`MultiRaft::prepare_merge`] for the
  /// container gates), replicating immediately. The coordinator adds the merge's floor leg
  /// through the caller's `floors` seam: a participant whose CURRENT incarnation sits below its
  /// persisted admission floor is a stale survivor of a fenced incarnation — refused BEFORE
  /// anything is appended, exactly as the split delegator fences its child. `None` if no group
  /// `source` is hosted.
  #[must_use = "`None` means no group with this id is hosted — nothing was proposed"]
  pub fn prepare_merge<L, S>(
    &mut self,
    source: &G,
    now: impl Into<Now>,
    log: &mut L,
    stable: &S,
    target: &G,
    floors: &impl FloorStore<G>,
  ) -> Option<Result<Index, crate::MergeError<I>>>
  where
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
    let r = self.multi.prepare_merge(source, now, log, stable, target)?;
    let _ = self.multi.flush_appends(source, now, log, stable);
    self.pump(now.mono());
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
    self.pump(now.mono());
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
    self.pump(now.mono());
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
  /// per-group storage drains. On a resolved ABSORB the coordinator TOMBSTONES the source id:
  /// its straggler frames drop silently from here on (the P5 wire story, unchanged), while the
  /// terminal floor the DRIVER persists from the returned resolutions is what makes the refusal
  /// survive restarts. Aborted resolutions touch nothing here — the source group is still live.
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
      if let crate::MergeResolution::Merged { source, .. } = r {
        self.quiesce_intents.remove(source);
        self.controls.retain(|(g, _)| g != source);
        self.retired.insert(source.cheap_clone());
        self.purge_unknown_signal(source);
      }
    }
    resolutions
  }

  /// The next committed, relay-ready fork from any hosted group (see
  /// [`MultiRaft::poll_pending_fork`]) — the driver drains this every crank BEFORE its storage
  /// crank, so the same crank's engine flush covers the materialization.
  pub fn poll_pending_fork(&mut self) -> Option<crate::GroupFork<G, I, F>> {
    self.multi.poll_pending_fork()
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
    self.pump(now.mono());
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
    self.pump(now.mono());
    Some(r)
  }

  /// Fire `group`'s CONSENSUS timers, then pump. `None` if no such group. The shared QUIC transport
  /// timers are fired by [`handle_transport_timeout`](Self::handle_transport_timeout).
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
    self.multi.handle_timeout(group, now, log, stable)?;
    self.pump(now.mono());
    Some(())
  }

  /// Drain `group`'s storage completions, then pump. `None` if no such group.
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
    self.pump(now.mono());
    Some(progress)
  }

  /// Pop one outbound datagram (destination + owned bytes), or `None` when the queue is empty. The
  /// driver drains this to exhaustion after every `handle_*` call — the drain-end chokepoint where
  /// the crank's batched heartbeats ship (one coalesced frame per peer, see [`Self::pump`]), so
  /// every call's beats leave with that call's transmit drain.
  pub fn poll_transmit(&mut self) -> Option<(SocketAddr, Vec<u8>)> {
    self.ship_heartbeats();
    self.bridge.poll_transmit()
  }

  /// Record the intent to QUIESCE `group`: its next heartbeat broadcast is stamped with the
  /// quiesce flag (every copy in that broadcast — all followers hear the promise), after which the
  /// intent clears and [`Self::is_quiescing`] reports `false`. The driver then stops arming the
  /// group's timers; each follower surfaces [`GroupControl::Quiesce`] to its own driver. A no-op
  /// for an unhosted group.
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

  /// Drain the next group-scoped scheduling signal, in dispatch order (see [`GroupControl`]).
  pub fn poll_group_control(&mut self) -> Option<(G, GroupControl)> {
    self.controls.pop_front()
  }

  /// The TRANSPORT's own earliest deadline — quinn's timers/auth reap, or `now` immediately while
  /// the bridge holds deferred work — without any group's consensus deadline: the
  /// [`Self::poll_timeout`] decomposition a quiescing driver needs (it folds this with the
  /// non-quiesced subset of [`Self::deadlines`] instead of the all-groups aggregate).
  pub fn transport_timeout(&mut self) -> Option<Instant> {
    if self.bridge.has_pending_work() {
      return Some(self.last_now);
    }
    self
      .bridge
      .min_timeout()
      .and_then(|d| self.crate_instant(d))
  }

  /// The next deadline the driver should service: the earlier of the aggregate consensus deadline
  /// (across all groups) and the QUIC stack's — or `now` IMMEDIATELY when the bridge holds deferred
  /// work that progresses without any inbound datagram. The immediate deadline is the last `now` the
  /// driver passed in, NOT a wall-clock read, so simulations stay deterministic.
  pub fn poll_timeout(&mut self) -> Option<Instant> {
    let quic = self.transport_timeout();
    match (self.multi.poll_timeout(), quic) {
      (Some(a), Some(b)) => Some(a.min(b)),
      (a, None) => a,
      (None, b) => b,
    }
  }

  /// Each group's next consensus deadline — a driver's input for an aggregate timing wheel. The QUIC
  /// transport's own deadline is folded into [`poll_timeout`](Self::poll_timeout), not here.
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

  /// Whether a BOUND (identity-validated) connection to `peer` currently exists — the shared link
  /// every co-located group's outbound frames route over. A driver polls this to redial a configured
  /// peer whose connection idled out or was lost.
  pub fn has_bound_conn(&self, peer: &I) -> bool {
    self.bridge.handle_for(peer).is_some()
  }

  /// The number of outgoing messages the send path refused because their encoded frame would exceed
  /// the transport frame limit. A non-zero, growing count signals an oversized snapshot or command
  /// payload.
  pub fn oversized_outbound_dropped(&self) -> u64 {
    self.bridge.oversized_dropped()
  }

  /// The node's transport identity — the host id latched by the first admitted group (a
  /// multi-Raft host is one physical node), stable across group removals and zero-group windows.
  /// `None` only before ANY group has ever been admitted, in which case there is no identity to
  /// advertise: our preface cannot be staged, and an inbound preface closes its connection (the
  /// peer redials once admission happens).
  fn node_id(&self) -> Option<I> {
    self.multi.host_id().map(CheapClone::cheap_clone)
  }

  /// The mesh floor against the UNION of every hosted group's tracked peers: co-located groups share
  /// the transport connections, so the cap must cover the union (voters in both joint halves,
  /// learners, and incoming learners), not any one group. Excludes this node itself.
  fn effective_cap(&self) -> usize {
    let mut peers: BTreeSet<I> = BTreeSet::new();
    for gid in self.multi.group_ids() {
      if let Some(ep) = self.multi.group(gid) {
        let conf = ep.conf_state();
        peers.extend(conf.voters().iter().map(CheapClone::cheap_clone));
        peers.extend(conf.voters_outgoing().iter().map(CheapClone::cheap_clone));
        peers.extend(conf.learners().iter().map(CheapClone::cheap_clone));
        peers.extend(conf.learners_next().iter().map(CheapClone::cheap_clone));
      }
    }
    if let Some(me) = self.node_id() {
      peers.remove(&me);
    }
    self
      .configured_max_connections
      .max(mesh_connection_floor(peers.len()))
  }

  /// `now` mapped onto quinn's `std::time::Instant` clock through the lazily-captured anchor, so
  /// `quinn_now(first_now) == std_base` regardless of the driver's epoch. `&mut self` because the
  /// first call sets the anchor.
  fn quinn_now(&mut self, now: Instant) -> std::time::Instant {
    self.last_now = now;
    let (base, std_base) = *self
      .clock_anchor
      .get_or_insert_with(|| (now, std::time::Instant::now()));
    std_base + now.duration_since(base)
  }

  /// Reverse-map a quinn deadline back into crate time through the same anchor. `None` before the
  /// first `quinn_now` (no anchor — nothing has been fed to quinn either).
  fn crate_instant(&self, std_deadline: std::time::Instant) -> Option<Instant> {
    let (base, std_base) = self.clock_anchor?;
    Some(base + std_deadline.saturating_duration_since(std_base))
  }

  /// Drain the bridge's connection-event queues into the owning groups (the QUIC counterpart of the
  /// stream coordinator's inbound path). Mirrors the single-group `drain_bridge`, but a validated
  /// frame's group-demux tag is decoded and its store resolved through `stores` before dispatch:
  /// - `connected` → write the node's identity preface, run the cert-only authenticate probe;
  /// - stream-ready → retry staged sends, then decode each frame — an `Authenticating` connection's
  ///   first frame authenticates; a `Validated` connection's frame splits its group tag, decodes to
  ///   `G` + a consensus `Message`, and (if the group is hosted) feeds that group's endpoint. An
  ///   unknown-but-well-formed group drops the frame (the connection stays up for other groups),
  ///   surfacing an unknown-group signal when it is initial-shaped and untombstoned; a malformed
  ///   tag, an undecodable message, or a framing violation closes the connection;
  /// - `lost` → reap the closed connection from routing.
  // Takes the full `Now`: a decoded consensus message dispatched below can drive a network election
  // whose leader no-op must stamp the SYNCHRONIZED wall. Only the quinn/bridge timers use `now.mono()`.
  fn drain_bridge<L, S, St>(&mut self, now: Now, stores: &mut St)
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
    St: GroupStores<G, L, S>,
  {
    let std_now = self.quinn_now(now.mono());
    while let Some(h) = self.bridge.take_connected() {
      let Some(me) = self.node_id() else {
        // No identity latched yet (no group has ever been admitted): the Connected event fires
        // exactly once, so this was the connection's ONLY chance to stage our preface — it can
        // never validate, no matter what is admitted later. Close it here, at the source,
        // strictly BEFORE any of the peer's preface bytes are examined: a group admitted between
        // this event and the (possibly trickling) preface's arrival must not let the connection
        // bind half-duplex. The peer sees the close and redials after admission.
        self.bridge.close_local(std_now, h);
        continue;
      };
      let mut preface = Vec::new();
      self.identity.write_control_preface(&me, &mut preface);
      self.bridge.open_send_and_preface(std_now, h, &preface);
      let certs = self.bridge.peer_certs(h);
      let outcome = self
        .identity
        .authenticate(&IdentityCtx::new(&certs, None, self.cluster));
      self.apply_outcome(std_now, h, outcome);
    }
    for h in self.bridge.take_ready_unique() {
      self.bridge.flush_stream(std_now, h);
      if self.bridge.ingest_recv(std_now, h) {
        continue;
      }
      loop {
        let frame = match self.bridge.next_frame(h) {
          Ok(Some(f)) => f,
          Ok(None) => break,
          Err(_) => {
            self.bridge.close_local(std_now, h);
            break;
          }
        };
        if self.bridge.is_authenticating(h) {
          if self.node_id().is_none() {
            // Backstop only: an identity-less connection is closed at its Connected event above,
            // and the identity latch never clears, so a frame cannot legitimately reach this arm
            // with no identity. If one does, the invariant still holds — our preface was never
            // staged, so binding would wedge half-duplex: close, never bind.
            self.bridge.close_local(std_now, h);
            break;
          }
          let certs = self.bridge.peer_certs(h);
          let outcome =
            self
              .identity
              .authenticate(&IdentityCtx::new(&certs, Some(&frame), self.cluster));
          self.apply_outcome(std_now, h, outcome);
          if !self.bridge.is_validated(h) {
            break;
          }
        } else if self.bridge.is_validated(h) {
          let Some(from) = self.bridge.bound_peer_of(h) else {
            self.bridge.close_local(std_now, h);
            break;
          };
          // A coalesced control frame expands to per-entry (flags, group, message) dispatches;
          // a single-message frame is the one-entry, flags-0 case of the same shape.
          let entries = if crate::transport::frame::is_coalesced_frame(&frame) {
            crate::transport::frame::split_coalesced(frame).ok()
          } else {
            crate::transport::frame::split_group_header(frame)
              .ok()
              .map(|(group_bytes, message)| std::vec![(0u8, group_bytes, message)])
          };
          let Some(entries) = entries else {
            self.bridge.close_local(std_now, h);
            break;
          };
          let mut malformed = false;
          for (flags, group_bytes, message) in entries {
            let parsed = G::decode_exact(group_bytes).ok().and_then(|group| {
              let msg = crate::wire::decode_message::<I>(message).ok()?;
              Some((group, msg))
            });
            // A malformed tag or message poisons the whole frame (integrity-suspect close),
            // exactly as on a single-message frame.
            let Some((group, msg)) = parsed else {
              malformed = true;
              break;
            };
            // Receive-side gate on the quiesce bit: only a leader's own Heartbeat broadcast ever
            // stamps it, so a flagged anything-else is a protocol violation — and honoring it
            // would freeze this group on a class that deliberately emits no Wake (see the stream
            // sibling).
            if flags & COALESCED_FLAG_QUIESCE != 0 && !msg.is_heartbeat() {
              malformed = true;
              break;
            }
            // A tombstoned (removed, not cleared since) group's entry is a straggler from the
            // group's past life on this host: drop the ENTRY silently — per entry, like the
            // unhosted drop, so the shared frame's other groups still dispatch, and never an
            // unknown-group signal (the embedder retired the id; resurrecting it on a
            // straggler's say-so would undo the removal). Ordered AFTER the integrity gates (a
            // malformed tag or violating flag still closes) and BEFORE store resolution.
            if self.retired.contains(&group) {
              continue;
            }
            // A well-formed entry for a group this host does not carry is dropped — entry by
            // entry, never the frame or the connection: the link is shared, so one unhosted
            // group must not cost the others their frames. Its flags drop with it.
            if let Some((log, stable)) = stores.stores(&group) {
              let wake = Self::is_wake_class(&msg);
              let beat_term = msg.term();
              // The core's sender-authenticity rule mirrored pre-dispatch (see the stream
              // sibling): a payload naming another node is dropped, so its flag drops too.
              let flags = if msg.from() == from {
                flags
              } else {
                flags & !COALESCED_FLAG_QUIESCE
              };
              if self
                .multi
                .handle_message(&group, now, log, stable, from.cheap_clone(), msg)
                .is_some()
              {
                let flags = self.accepted_flags(&group, flags, beat_term, &from);
                self.push_dispatch_controls(&group, wake, flags);
              }
            } else if is_initial_shaped(&msg) && !self.multi.contains_group(&group) {
              // Neither store-resolvable nor hosted (nor tombstoned — gated above): a live peer
              // is actively soliciting a group this host does not carry. Surface it ONCE to the
              // embedder's placement brain; every other kind for the group drops silently.
              self.note_unknown_group(group, from.cheap_clone());
            }
          }
          if malformed {
            self.bridge.close_local(std_now, h);
            break;
          }
        } else {
          break;
        }
      }
      if self.bridge.fin_received(h) {
        self.bridge.close_local(std_now, h);
      }
    }
    while let Some(h) = self.bridge.take_lost() {
      self.bridge.reap(h);
    }
  }

  /// Apply the coordinator-owned binding policy to an [`IdentityOutcome`] for connection `h` (the
  /// single-group policy verbatim, with the self-id gate keyed on the shared node id):
  /// cluster cross-check, never-bind-our-own-id, then dialed→match-or-abort / accepted→adopt.
  fn apply_outcome(
    &mut self,
    std_now: std::time::Instant,
    h: ConnectionHandle,
    outcome: IdentityOutcome<I>,
  ) {
    let identified = match outcome {
      IdentityOutcome::Identified(id) => id,
      IdentityOutcome::Pending => return,
      IdentityOutcome::Rejected => {
        self.bridge.close_local(std_now, h);
        return;
      }
    };
    if *identified.cluster() != self.cluster {
      self.bridge.close_local(std_now, h);
      return;
    }
    let candidate = identified.into_who();
    if self.node_id().is_some_and(|me| candidate == me) {
      self.bridge.close_local(std_now, h);
      return;
    }
    match self.bridge.dialed_expectation_of(h) {
      Some(expected) if candidate != expected => {
        self.bridge.close_local(std_now, h);
      }
      _ => self.bridge.bind_validated(std_now, h, candidate),
    }
  }

  /// Recompute the live-membership connection floor, drain every group's outbound backlog, route
  /// each message over the resolved peer's shared stream stamped with its group tag, then run ONE
  /// unconditional bridge `service`. The pump-end `service` is the single wakeup mechanism for the
  /// QUIC transport (see the single-group coordinator); every coordinator pass ends here.
  ///
  /// A message to a peer with NO bound connection is dropped: consensus retransmission re-drives it,
  /// and the driver's redial policy ([`has_bound_conn`](Self::has_bound_conn)) restores the link.
  fn pump(&mut self, now: Instant) {
    // Track the LIVE union membership: a committed configuration change in any hosted group can grow
    // the tracked peer set, and a cap frozen at construction would refuse the new members' shared
    // mesh connections. Recompute against the configured cap each pump — monotone (the bridge only
    // raises), positioned where every pass already ends.
    let cap = self.effective_cap();
    self.bridge.raise_max_connections(cap);
    let mut outgoing = Vec::new();
    while let Some(o) = self.multi.poll_message() {
      outgoing.push(o);
    }
    let std_now = self.quinn_now(now);
    let mut group_bytes = Vec::new();
    // `Heartbeat`/`HeartbeatResponse` divert into per-peer batches (shipped coalesced at the
    // `poll_transmit` chokepoint); everything else writes immediately as its own frame. The
    // reorder of a batched beat behind a same-crank `AppendEntries` is safe by construction — a
    // heartbeat's `commit` is clamped to the follower's acked match. A group with a pending
    // quiesce intent has EVERY beat copy in this drain stamped (the whole broadcast), and the
    // intent is consumed at drain end.
    let mut stamped: BTreeSet<G> = BTreeSet::new();
    for (group, o) in outgoing {
      let (to, msg) = o.into_parts();
      if msg.is_heartbeat() || msg.is_heartbeat_response() {
        // Stamp ONLY the leader's own Heartbeat broadcast, never a HeartbeatResponse — a stale
        // intent on a response would freeze the NEW leader (see the stream sibling).
        let flags = if msg.is_heartbeat() && self.quiesce_intents.contains(&group) {
          stamped.insert(group.cheap_clone());
          COALESCED_FLAG_QUIESCE
        } else {
          0
        };
        let mut gb = Vec::new();
        group.encode(&mut gb);
        self
          .hb_batches
          .entry(to)
          .or_default()
          .push((flags, gb, msg));
      } else if let Some(h) = self.bridge.handle_for(&to) {
        group_bytes.clear();
        group.encode(&mut group_bytes);
        self.bridge.write_framed(std_now, h, &group_bytes, &msg);
      }
    }
    for group in &stamped {
      self.quiesce_intents.remove(group);
    }
    self.bridge.service(std_now);
  }

  /// Ship the batched heartbeats: a batch of ONE unflagged beat goes as a normal single-message
  /// frame (no format change for the trivial case); anything else — many beats, or a flagged one
  /// (only a coalesced entry has a flags byte) — ships as coalesced frames. A peer with no bound
  /// connection drops its batch, exactly as `pump` drops a message (retries re-drive it). Runs one
  /// service pass when anything shipped, so the staged bytes reach the datagram queue the caller
  /// is about to drain.
  fn ship_heartbeats(&mut self) {
    if self.hb_batches.is_empty() {
      return;
    }
    let std_now = self.quinn_now(self.last_now);
    let mut scratch = Vec::new();
    for (to, batch) in core::mem::take(&mut self.hb_batches) {
      let Some(h) = self.bridge.handle_for(&to) else {
        continue;
      };
      // An entry alone over the coalesced budget diverts to a normal frame, re-arming a flagged
      // one's intent — the receiver enforces the budget, so an oversized coalesced emission would
      // reject on every delivery (see the stream sibling).
      let mut fitting: Vec<crate::transport::CoalescedEntry<I>> = Vec::with_capacity(batch.len());
      for (flags, group_bytes, msg) in batch {
        scratch.clear();
        crate::wire::encode_message(&msg, &mut scratch);
        let entry_len = 1 + 2 + group_bytes.len() + 4 + scratch.len();
        if entry_len > crate::transport::frame::COALESCED_FRAME_BUDGET {
          // Re-arm only for a group still hosted (see the stream sibling): a lifecycle removal
          // between the stamp and this divert must not leave a dormant intent behind.
          if flags & COALESCED_FLAG_QUIESCE != 0
            && let Ok(gid) = G::decode_exact(bytes::Bytes::from(group_bytes.clone()))
            && self.multi.contains_group(&gid)
          {
            self.quiesce_intents.insert(gid);
          }
          self.bridge.write_framed(std_now, h, &group_bytes, &msg);
        } else {
          fitting.push((flags, group_bytes, msg));
        }
      }
      match fitting.as_slice() {
        [] => {}
        [(0, group_bytes, msg)] => {
          self.bridge.write_framed(std_now, h, group_bytes, msg);
        }
        _ => {
          self.bridge.write_coalesced(std_now, h, &fitting);
        }
      }
    }
    self.bridge.service(std_now);
  }

  /// Strip the quiesce flag unless the dispatched beat was ACCEPTED as current-leader contact —
  /// after the dispatch this group must be a follower of exactly `sender` at exactly the beat's
  /// term (the core silently drops sender-mismatched and stale-term beats, and a rejected input
  /// must not freeze timers; see the stream sibling). `Wake` is deliberately not gated.
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

  /// Queue the dispatch-driven [`GroupControl`]s for one delivered message: a `Wake` for every
  /// wake-class kind (see [`GroupControl::Wake`] — the heartbeat response is absorbed), then a
  /// `Quiesce` if the entry carried the flag — flag AFTER wake, so a flagged
  /// beat nets quiesced. Consecutive duplicates collapse (a burst of appends is one `Wake`).
  fn push_dispatch_controls(&mut self, group: &G, wake: bool, flags: u8) {
    if wake {
      self.push_control(group, GroupControl::Wake);
    }
    if flags & COALESCED_FLAG_QUIESCE != 0 {
      self.push_control(group, GroupControl::Quiesce);
    }
  }

  /// Whether a delivered message is WAKE-class for its group. The absorbed complement is exactly
  /// `HeartbeatResponse` — with the heartbeat-response append pump gated and quiesce eligibility
  /// excluding lagging peers, a quiescing group's FINAL flagged round is precisely
  /// `Heartbeat` + `HeartbeatResponse`, so absorbing that one response is all it takes for the
  /// round to die out instead of re-waking either side (see [`GroupControl::Wake`] for the
  /// safety argument).
  fn is_wake_class(msg: &Message<I>) -> bool {
    !msg.is_heartbeat_response()
  }

  fn push_control(&mut self, group: &G, ctrl: GroupControl) {
    if self
      .controls
      .back()
      .is_some_and(|(g, c)| g == group && *c == ctrl)
    {
      return;
    }
    self.controls.push_back((group.cheap_clone(), ctrl));
  }
}

impl<G, I, F, ID> core::fmt::Debug for MultiQuicCoordinator<G, I, F, ID>
where
  G: GroupId,
  F: StateMachine,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("MultiQuicCoordinator")
      .finish_non_exhaustive()
  }
}

#[cfg(test)]
mod tests;
