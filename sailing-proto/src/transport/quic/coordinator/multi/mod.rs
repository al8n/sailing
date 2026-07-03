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
  CheapClone, Config, CreateGroupError, Data, Endpoint, Event, GroupControl, GroupId, GroupStores,
  Index, Instant, LogStore, Message, MultiRaft, NodeId, Now, ProposeError, StableStore,
  StateMachine, StorageProgress,
  transport::{ClusterId, CoalescedEntry, frame::COALESCED_FLAG_QUIESCE},
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
  /// Tombstoned group ids: REMOVED groups whose inbound entries drop silently until a
  /// create/restore re-admits the id (see [`remove_group`](Self::remove_group)). In-memory and
  /// volatile — a restart starts clean; the embedder's group catalog owns removal persistence.
  retired: BTreeSet<G>,
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
    }
  }

  /// Create a fresh group (see [`MultiRaft::create_group`]). Successful admission lifts the id's
  /// tombstone, if any — re-creating the SAME group id is the supported rejoin path (see
  /// [`remove_group`](Self::remove_group)).
  ///
  /// # Errors
  /// The admission checks of [`MultiRaft::create_group`] — see [`CreateGroupError`].
  pub fn create_group(
    &mut self,
    gid: G,
    config: Config<I>,
    now: impl Into<Now>,
    seed: u64,
    fsm: F,
  ) -> Result<(), CreateGroupError> {
    let key = gid.cheap_clone();
    self.multi.create_group(gid, config, now, seed, fsm)?;
    self.retired.remove(&key);
    // A fresh group can widen the tracked-peer union; raise the connection cap now rather than at
    // the next pump, so accepts arriving in the gap are not statelessly refused.
    self.bridge.raise_max_connections(self.effective_cap());
    Ok(())
  }

  /// Recover a group from durable storage (see [`MultiRaft::restore_group`]). Successful
  /// admission lifts the id's tombstone, if any, exactly as [`create_group`](Self::create_group).
  ///
  /// # Errors
  /// The admission checks of [`MultiRaft::restore_group`] — see [`CreateGroupError`].
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
    I: Data,
  {
    let key = gid.cheap_clone();
    self
      .multi
      .restore_group(gid, config, now, seed, fsm, boot_epoch, log, stable)?;
    self.retired.remove(&key);
    // Same cap-raise as `create_group`: the restored group widens the tracked-peer union.
    self.bridge.raise_max_connections(self.effective_cap());
    Ok(())
  }

  /// Remove a group, returning its endpoint if present. Drops the group's pending quiesce intent
  /// and queued controls with it (its per-peer batched beats, if any, still ship — the receiver's
  /// unhosted-entry drop absorbs them) — and TOMBSTONES the id: inbound entries tagged with it
  /// drop silently, before store resolution, until a create/restore re-admits it.
  ///
  /// The tombstone is IN-MEMORY and GROUP-keyed — deliberately weaker than the references, which
  /// persist replica-keyed tombstones for exactly this straggler problem (TiKV a region-epoch +
  /// peer-id tombstone, CockroachDB a NextReplicaID floor, each a monotonic incarnation
  /// discriminator): sailing group ids carry no incarnation number, so SINGLE-INCARNATION ids are
  /// the embedder contract — re-creating the SAME group id is the supported rejoin path, and
  /// reusing an id for a DIFFERENT logical group is unsound without epochs (matching the NodeId
  /// reuse rules). Across a restart the tombstone is gone; the embedder's placement catalog is
  /// the persistent record of what must not live here.
  pub fn remove_group(&mut self, gid: &G) -> Option<Endpoint<I, F>> {
    self.quiesce_intents.remove(gid);
    self.controls.retain(|(g, _)| g != gid);
    self.retired.insert(gid.cheap_clone());
    self.multi.remove_group(gid)
  }

  /// Whether `gid` is TOMBSTONED: removed and not re-admitted since, its inbound entries dropping
  /// silently (see [`remove_group`](Self::remove_group)). Volatile — a restart starts clean.
  #[must_use]
  pub fn is_retired(&self, gid: &G) -> bool {
    self.retired.contains(gid)
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
  /// is dropped (the sender retries); a group tag that does not decode as `G`, or an undecodable
  /// message body, closes the connection as integrity-suspect.
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
  ///   unknown-but-well-formed group drops the frame (the connection stays up for other groups); a
  ///   malformed tag, an undecodable message, or a framing violation closes the connection;
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
            // A tombstoned (removed, not re-admitted) group's entry is a straggler from the
            // group's past life on this host: drop the ENTRY silently — per entry, like the
            // unhosted drop, so the shared frame's other groups still dispatch. Ordered AFTER
            // the integrity gates (a malformed tag or violating flag still closes) and BEFORE
            // store resolution.
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
          if flags & COALESCED_FLAG_QUIESCE != 0
            && let Ok(gid) = G::decode_exact(bytes::Bytes::from(group_bytes.clone()))
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
  /// wake-class kind (see [`GroupControl::Wake`] — the heartbeat round's response tail is
  /// absorbed), then a `Quiesce` if the entry carried the flag — flag AFTER wake, so a flagged
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
  /// the response tail of one heartbeat round in this codebase — `HeartbeatResponse`, the empty
  /// `AppendEntries` the leader pumps back at each responder, and that probe's `AppendResponse` —
  /// so a quiescing group's FINAL flagged round dies out instead of re-waking either side (see
  /// [`GroupControl::Wake`] for the safety argument).
  fn is_wake_class(msg: &Message<I>) -> bool {
    match msg {
      Message::HeartbeatResponse(_) | Message::AppendResponse(_) => false,
      Message::AppendEntries(ae) => !ae.entries().is_empty(),
      _ => true,
    }
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
