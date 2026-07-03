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
use std::{collections::BTreeSet, net::SocketAddr, vec::Vec};

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
  CheapClone, Config, CreateGroupError, Data, Endpoint, Event, GroupId, GroupStores, Index,
  Instant, LogStore, MultiRaft, NodeId, Now, ProposeError, StableStore, StateMachine,
  StorageProgress, transport::ClusterId,
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
/// preface self-claim and the self-id gate operand) is the id every hosted group shares; it is read
/// from any hosted group.
///
/// # Clock
///
/// The coordinator owns the sailing↔std clock adapter exactly as the single-group one does: its
/// surface speaks the crate [`Instant`], converting to `std::time::Instant` at every quinn boundary,
/// with the anchor captured LAZILY on the first call.
pub struct MultiQuicCoordinator<G, I, F, ID = Hello>
where
  G: GroupId,
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
    }
  }

  /// Create a fresh group (see [`MultiRaft::create_group`]).
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
    self.multi.create_group(gid, config, now, seed, fsm)?;
    // A fresh group can widen the tracked-peer union; raise the connection cap now rather than at
    // the next pump, so accepts arriving in the gap are not statelessly refused.
    self.bridge.raise_max_connections(self.effective_cap());
    Ok(())
  }

  /// Recover a group from durable storage (see [`MultiRaft::restore_group`]).
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
    self
      .multi
      .restore_group(gid, config, now, seed, fsm, boot_epoch, log, stable)?;
    // Same cap-raise as `create_group`: the restored group widens the tracked-peer union.
    self.bridge.raise_max_connections(self.effective_cap());
    Ok(())
  }

  /// Remove a group, returning its endpoint if present.
  pub fn remove_group(&mut self, gid: &G) -> Option<Endpoint<I, F>> {
    self.multi.remove_group(gid)
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
  /// then pump every group's resulting outbound messages back out. A frame whose group tag fails to
  /// decode, or whose group has no store, is dropped (the sender retries).
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
  /// shared transport half, and the method the driver calls while [`poll_timeout`](Self::poll_timeout)
  /// reports the bridge's immediate deferred work.
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
    self.multi.flush_appends(group, now, log, stable);
    self.pump(now.mono());
    Some(r)
  }

  /// Fire `group`'s CONSENSUS timers, then pump. `None` if no such group. The shared QUIC transport
  /// timers are fired by [`handle_transport_timeout`](Self::handle_transport_timeout).
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
  /// driver drains this to exhaustion after every `handle_*` call.
  pub fn poll_transmit(&mut self) -> Option<(SocketAddr, Vec<u8>)> {
    self.bridge.poll_transmit()
  }

  /// The next deadline the driver should service: the earlier of the aggregate consensus deadline
  /// (across all groups) and the QUIC stack's — or `now` IMMEDIATELY when the bridge holds deferred
  /// work that progresses without any inbound datagram. The immediate deadline is the last `now` the
  /// driver passed in, NOT a wall-clock read, so simulations stay deterministic.
  pub fn poll_timeout(&mut self) -> Option<Instant> {
    if self.bridge.has_pending_work() {
      return Some(self.last_now);
    }
    let quic = self
      .bridge
      .min_timeout()
      .and_then(|d| self.crate_instant(d));
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

  /// The node's transport identity — the id every hosted group shares (a multi-Raft host is one
  /// physical node). Read from any hosted group; `None` only when no group is hosted yet, in which
  /// case there is no identity to advertise (the preface step and the self-id gate are skipped, and
  /// an un-authenticatable connection is reaped by the bridge's auth deadline).
  fn node_id(&self) -> Option<I> {
    self
      .multi
      .group_ids()
      .next()
      .and_then(|gid| self.multi.group(gid))
      .map(|ep| ep.id())
  }

  /// The mesh floor against the UNION of every hosted group's tracked peers: co-located groups share
  /// the transport connections, so the cap must cover the union (voters in both joint halves,
  /// learners, and incoming learners), not any one group. Excludes this node itself.
  fn effective_cap(&self) -> usize {
    let mut peers: BTreeSet<I> = BTreeSet::new();
    let mut me: Option<I> = None;
    for gid in self.multi.group_ids() {
      if let Some(ep) = self.multi.group(gid) {
        let conf = ep.conf_state();
        peers.extend(conf.voters().iter().map(CheapClone::cheap_clone));
        peers.extend(conf.voters_outgoing().iter().map(CheapClone::cheap_clone));
        peers.extend(conf.learners().iter().map(CheapClone::cheap_clone));
        peers.extend(conf.learners_next().iter().map(CheapClone::cheap_clone));
        me = Some(ep.id());
      }
    }
    if let Some(me) = me {
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
        continue; // no group hosted: nothing to authenticate as; the connection auth-times out
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
          let from = self.bridge.bound_peer_of(h);
          let parsed = crate::transport::frame::split_group_header(frame)
            .ok()
            .and_then(|(group_bytes, message)| {
              let group = G::decode_exact(group_bytes).ok()?;
              let msg = crate::wire::decode_message::<I>(message).ok()?;
              Some((group, msg))
            });
          match (from, parsed) {
            (Some(from), Some((group, msg))) => {
              // A well-formed frame for a group this host does not carry is dropped, not fatal: the
              // connection is shared, so one unhosted group must not tear it down for the others.
              if let Some((log, stable)) = stores.stores(&group) {
                self
                  .multi
                  .handle_message(&group, now, log, stable, from, msg);
              }
            }
            _ => {
              self.bridge.close_local(std_now, h);
              break;
            }
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
    for (group, o) in outgoing {
      let (to, msg) = o.into_parts();
      if let Some(h) = self.bridge.handle_for(&to) {
        group_bytes.clear();
        group.encode(&mut group_bytes);
        self.bridge.write_framed(std_now, h, &group_bytes, &msg);
      }
    }
    self.bridge.service(std_now);
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
mod tests {
  use super::*;
  use crate::{
    testkit::{AsyncStable, CountSm, VecLog},
    transport::quic::{QuicTuning, crypto::tests::TestClusterCa},
  };
  use core::time::Duration;
  use std::{collections::BTreeMap, string::String};

  /// The SAN the coordinator's `sni_for` derives for node `id` in `c` — certs are minted with it.
  fn san(id: u64, c: &ClusterId) -> String {
    use core::fmt::Write as _;
    let mut s = String::from("node-");
    let mut enc = Vec::new();
    Data::encode(&id, &mut enc);
    for b in &enc {
      let _ = write!(s, "{b:02x}");
    }
    s.push('.');
    for b in &c.0 {
      let _ = write!(s, "{b:02x}");
    }
    s.push_str(".sailing");
    s
  }

  struct Stores {
    map: BTreeMap<u64, (VecLog, AsyncStable)>,
  }

  impl GroupStores<u64, VecLog, AsyncStable> for Stores {
    fn stores(&mut self, group: &u64) -> Option<(&mut VecLog, &mut AsyncStable)> {
      self.map.get_mut(group).map(|(l, s)| (l, s))
    }
  }

  fn single_voter(id: u64) -> Config<u64> {
    Config::try_new(
      id,
      std::vec![id],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap()
  }

  #[test]
  fn coordinator_drives_isolated_groups() {
    let ca = TestClusterCa::generate();
    let cluster = ClusterId([7u8; 16]);
    let opts = ca
      .cluster_tls(&san(1, &cluster))
      .tuning(QuicTuning::new().with_keep_alive_interval_millis(0))
      .build();
    let mut seed = [0u8; 32];
    seed[0] = 1;
    let mut coord =
      MultiQuicCoordinator::<u64, u64, CountSm>::with_identity(opts, Some(seed), cluster);
    coord
      .create_group(100, single_voter(1), Instant::ORIGIN, 1, CountSm::default())
      .unwrap();
    coord
      .create_group(200, single_voter(1), Instant::ORIGIN, 1, CountSm::default())
      .unwrap();

    let mut stores = Stores {
      map: BTreeMap::new(),
    };
    stores
      .map
      .insert(100, (VecLog::default(), AsyncStable::default()));
    stores
      .map
      .insert(200, (VecLog::default(), AsyncStable::default()));

    // Drive group 100 to leadership through the coordinator's per-group storage. Group 200 is never
    // touched. Single-voter groups need no peer connection, so this exercises the coordinator's group
    // threading + store routing without the wire.
    let d = coord.group(&100).unwrap().poll_timeout().unwrap();
    {
      let (l, s) = stores.stores(&100).unwrap();
      coord.handle_timeout(&100, d, l, s); // campaign
    }
    for _ in 0..2 {
      // First drain: the self-vote becomes durable and the group becomes leader (appending a no-op);
      // second drain: the no-op append completes, so quorum=1 commits and applies it.
      let (l, s) = stores.stores(&100).unwrap();
      coord.handle_storage(&100, d, l, s);
    }
    assert!(coord.group(&100).unwrap().role().is_leader());
    assert!(coord.group(&200).unwrap().role().is_follower());

    // Propose a command on group 100 and let quorum=1 commit + apply it.
    let cmd = bytes::Bytes::copy_from_slice(&[7u8]);
    {
      let (l, s) = stores.stores(&100).unwrap();
      coord.submit_propose(&100, d, l, s, &cmd).unwrap().unwrap();
    }
    {
      let (l, s) = stores.stores(&100).unwrap();
      coord.handle_storage(&100, d, l, s);
    }
    while let Some((g, _)) = coord.poll_event() {
      assert_eq!(g, 100, "events are stamped with the originating group");
    }
    // Group 100 applied the command; group 200 is pristine.
    assert!(coord.group(&100).unwrap().state_machine().count() >= 1);
    assert_eq!(coord.group(&200).unwrap().state_machine().count(), 0);
  }
}
