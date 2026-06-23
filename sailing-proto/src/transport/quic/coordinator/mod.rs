//! `QuicCoordinator<I, F, ID>`: the QUIC-transport "super state machine".
//!
//! It composes the pure consensus [`Endpoint`] with the quinn-proto [`Bridge`]: inbound UDP
//! datagrams become decoded `Message`s fed to the endpoint, and the endpoint's outbound messages
//! ride per-peer bidi streams back out as datagrams. The driver supplies UDP sockets, timers, and
//! storage; this type is fully deterministic and Sans-I/O.

use std::{net::SocketAddr, string::String, vec::Vec};

use quinn_proto::{ConnectionHandle, EcnCodepoint};

use super::{
  Hello, IdentityCtx, IdentityOutcome, IdentitySource,
  bridge::{Bridge, DialError},
  crypto::{QuicOptions, mesh_connection_floor},
};
use crate::{
  Config, Data, Endpoint, Event, Index, Instant, LogStore, NodeId, Now, ProposeError, StableStore,
  StateMachine, StorageProgress, TransferError, transport::ClusterId,
};
use core::error::Error;
use std::collections::BTreeSet;

/// Derive the SNI server-name a dial presents for `peer` in `cluster`, matching the per-node cert
/// SAN minted by a [`ClusterTls`](super::ClusterTls) deployment:
/// `node-<id-hex>.<cluster-hex>.sailing`, where `id-hex` is the lowercase hex of the peer id's
/// `Data` encoding. The stock `WebPkiServerVerifier` validates this against the server cert's
/// SANs, so it is part of the cluster-separation guarantee, not cosmetic.
///
/// **Bound:** DNS limits one label to 63 octets, so the `node-<id-hex>` label admits id encodings
/// of at most 29 bytes — comfortably past `u64`/`u128`/UUID node ids, but far below the hello
/// layer's 1024-byte cap. A longer id produces an invalid server name, which rustls rejects and
/// [`connect`](QuicCoordinator::connect) surfaces as a typed [`DialError`] (never a panic); a
/// deployment with larger ids dials through
/// [`connect_with_server_name`](QuicCoordinator::connect_with_server_name) (paired with
/// [`dangerous_custom_identity`](QuicCoordinator::dangerous_custom_identity) when the identity
/// scheme changes too) and certs minted to match.
fn sni_for<I: Data>(peer: &I, cluster: &ClusterId) -> String {
  use core::fmt::Write as _;
  let mut id = Vec::new();
  peer.encode(&mut id);
  let mut name = String::with_capacity(5 + id.len() * 2 + 1 + 32 + 8);
  name.push_str("node-");
  for b in &id {
    let _ = write!(name, "{b:02x}");
  }
  name.push('.');
  for b in &cluster.0 {
    let _ = write!(name, "{b:02x}");
  }
  name.push_str(".sailing");
  name
}

/// The number of TRACKED peers this node replicates with, from the committed configuration:
/// the union of voters (both joint halves), learners, and incoming learners, excluding this
/// node itself. The transport meshes with every one of them, so the connection-cap floor is
/// sized from this — not from the incoming voter set alone, which would undercount learners and
/// joint transitions (and a node that is itself a learner).
fn tracked_peer_count<I: Ord>(conf: &crate::ConfState<I>, me: &I) -> usize {
  let mut peers: BTreeSet<&I> = BTreeSet::new();
  peers.extend(conf.voters().iter());
  peers.extend(conf.voters_outgoing().iter());
  peers.extend(conf.learners().iter());
  peers.extend(conf.learners_next().iter());
  peers.remove(me);
  peers.len()
}

/// A consensus node speaking QUIC: the [`Endpoint`] composed with the quinn-proto bridge and an
/// [`IdentitySource`] (`ID`, the provided [`Hello`] by default).
///
/// # Identity
///
/// `ID` extracts an UNTRUSTED candidate peer from post-handshake material (the control-stream
/// preface, or a certificate extension). The COORDINATOR — never the source — owns the binding
/// policy: an unconditional `cluster == our cluster` cross-check, a never-bind-our-own-id gate,
/// then dialed→match-or-abort / accepted→adopt. Only after that does a connection reach
/// `Validated` and carry consensus frames.
///
/// Deliberately ABSENT from the policy: a membership gate. Sailing's membership is dynamic
/// (joint-consensus conf changes apply at commit), so a not-yet-committed joiner legitimately
/// connects — and catches up — before it is a voter; admission of its MESSAGES is governed by the
/// endpoint's own sender checks, which are membership-aware where it matters. This mirrors the
/// stream transport, whose hello binds any authenticated cluster member.
///
/// # Clock
///
/// The coordinator owns the sailing↔std clock adapter: its surface speaks the crate
/// [`Instant`] (matching the consensus endpoint), converting to `std::time::Instant` at every
/// quinn boundary. The anchor is captured LAZILY on the first call (the driver's actual first
/// `now`, not construction time), so a driver whose monotonic clock does not start at zero maps
/// quinn deadlines as small offsets rather than epoch-shifted ones.
pub struct QuicCoordinator<I, F, ID = Hello>
where
  F: StateMachine,
{
  endpoint: Endpoint<I, F>,
  bridge: Bridge<I>,
  /// The identity source: extracts the candidate the coordinator's binding policy then checks.
  identity: ID,
  /// The cluster this coordinator authenticates for (the cross-check operand and the SNI
  /// component).
  cluster: ClusterId,
  /// The sailing↔std clock anchor `(crate_base, std_base)`, captured lazily on the first
  /// [`Self::quinn_now`].
  clock_anchor: Option<(Instant, std::time::Instant)>,
  /// The most recent `now` the driver passed in — the deterministic "immediate deadline" value
  /// [`Self::poll_timeout`] returns when the bridge holds deferred work (real wall-clock reads
  /// would break simulation determinism).
  last_now: Instant,
  /// The CONFIGURED connection cap from the options, kept so the live mesh floor can be
  /// recomputed against it each pump: committed configuration changes grow the tracked peer set
  /// long after construction, and the effective cap must grow with it (see [`Self::pump`]).
  configured_max_connections: usize,
}

impl<I, F> QuicCoordinator<I, F, Hello>
where
  I: NodeId,
  F: StateMachine,
  F::Command: Data,
  F::Snapshot: Data,
  F::Error: Error,
{
  /// Create a coordinator wrapping a fresh [`Endpoint`], authenticating peers with the provided
  /// [`Hello`] preface scheme.
  ///
  /// # Panics
  ///
  /// Panics if `opts` was not built with mandatory client-certificate authentication (a
  /// [`ClusterTls::build`](super::ClusterTls::build) bundle): the `Hello` self-claim is
  /// trustworthy only because mandatory mTLS already proved the peer holds a cluster cert.
  /// Arbitrary/no-auth options belong only behind [`Self::dangerous_custom_identity`], where the
  /// embedder owns the trust boundary.
  pub fn new(
    config: Config<I>,
    now: impl Into<Now>,
    seed: u64,
    fsm: F,
    opts: QuicOptions,
    cluster: ClusterId,
  ) -> Self {
    let now: Now = now.into();
    Self::with_identity(Endpoint::new(config, now, seed, fsm), opts, None, cluster)
  }

  /// Wrap an already-constructed endpoint (restart and migration paths construct their endpoint
  /// through [`Endpoint`]'s own restart constructors first) with the provided [`Hello`] identity
  /// scheme. `rng_seed` seeds quinn's connection-ID/token RNG (`None` = OS entropy; simulations
  /// pass a fixed seed).
  ///
  /// # Panics
  ///
  /// Panics if `opts` lacks mandatory client auth — see [`Self::new`].
  pub fn with_identity(
    endpoint: Endpoint<I, F>,
    opts: QuicOptions,
    rng_seed: Option<[u8; 32]>,
    cluster: ClusterId,
  ) -> Self {
    assert!(
      opts.requires_client_auth(),
      "the provided Hello identity requires mandatory mTLS: build the options with \
       ClusterTls::build (so requires_client_auth() is true). Without mandatory client auth a \
       hello preface is a self-claim with no cryptographic backstop; arbitrary/no-auth options \
       belong only behind dangerous_custom_identity",
    );
    let identity = Hello::new(cluster);
    Self::build(endpoint, opts, rng_seed, identity, cluster)
  }
}

impl<I, F, ID> QuicCoordinator<I, F, ID>
where
  I: NodeId,
  F: StateMachine,
  F::Command: Data,
  F::Snapshot: Data,
  F::Error: Error,
  ID: IdentitySource<I>,
{
  /// Wrap a consensus endpoint with a CALLER-SUPPLIED [`IdentitySource`].
  ///
  /// # Hazard
  ///
  /// The embedder owns the identity-binding correctness of `src`, INCLUDING the attested
  /// cluster: `authenticate` must derive its candidate from genuine handshake material and
  /// report the cluster it actually attested. The coordinator re-runs its cross-checks, but for
  /// a custom source the cluster check only re-confirms what the source asserts — a source that
  /// mints an `Identified` with this cluster for a foreign peer passes it. Prefer the provided
  /// scheme unless a custom one (e.g. certificate-extension identity) is genuinely required.
  pub fn dangerous_custom_identity(
    endpoint: Endpoint<I, F>,
    opts: QuicOptions,
    rng_seed: Option<[u8; 32]>,
    src: ID,
    cluster: ClusterId,
  ) -> Self {
    Self::build(endpoint, opts, rng_seed, src, cluster)
  }

  /// Shared constructor body. The connection cap is raised to the membership-sized
  /// [`mesh_connection_floor`] (each peer pair keeps two mutual-dial connections plus reconnect
  /// headroom), so the cap can never refuse a legitimate steady-state mesh connection while
  /// still bounding an untrusted-network flood. The floor counts EVERY tracked peer — voters in
  /// both joint halves, learners, and incoming learners — because the endpoint replicates to all
  /// of them and the transport must mesh with all of them; counting only incoming voters would
  /// refuse legitimate learner / joint-transition connections under a low configured cap.
  fn build(
    endpoint: Endpoint<I, F>,
    opts: QuicOptions,
    rng_seed: Option<[u8; 32]>,
    identity: ID,
    cluster: ClusterId,
  ) -> Self {
    let configured = opts.max_connections();
    let peers = tracked_peer_count(&endpoint.conf_state(), &endpoint.id());
    let effective_cap = configured.max(mesh_connection_floor(peers));
    let opts = opts.with_max_connections(effective_cap);
    Self {
      endpoint,
      bridge: Bridge::new(&opts, rng_seed),
      identity,
      cluster,
      clock_anchor: None,
      last_now: Instant::ORIGIN,
      configured_max_connections: configured,
    }
  }

  /// A reference to the underlying consensus endpoint (read-only observers: role, term,
  /// commit/applied indexes, …).
  pub const fn endpoint(&self) -> &Endpoint<I, F> {
    &self.endpoint
  }

  /// This node's current consensus role.
  pub const fn role(&self) -> crate::Role {
    self.endpoint.role()
  }

  /// Read-only access to the application state machine.
  pub const fn state_machine(&self) -> &F {
    self.endpoint.state_machine()
  }

  /// `now` mapped onto quinn's `std::time::Instant` clock: the std anchor plus the crate-time
  /// elapsed since the crate anchor. The anchor is captured LAZILY on the first call, so
  /// `quinn_now(first_now) == std_base` regardless of the driver's epoch — a fixed anchor at
  /// [`Instant::ORIGIN`] would shift every quinn deadline by the driver's absolute epoch.
  /// Saturating on the crate side clamps a `now` before the anchor; `&mut self` because the
  /// first call sets the anchor.
  fn quinn_now(&mut self, now: Instant) -> std::time::Instant {
    self.last_now = now;
    let (base, std_base) = *self
      .clock_anchor
      .get_or_insert_with(|| (now, std::time::Instant::now()));
    std_base + now.duration_since(base)
  }

  /// Reverse-map a quinn deadline back into crate time through the same anchor. `None` before
  /// the first `quinn_now` (no anchor — nothing has been fed to quinn either).
  fn crate_instant(&self, std_deadline: std::time::Instant) -> Option<Instant> {
    let (base, std_base) = self.clock_anchor?;
    Some(base + std_deadline.saturating_duration_since(std_base))
  }

  /// Dial the node `peer` at `remote`. The dial records `peer` as the connection's expectation
  /// (the binding policy later requires the authenticated identity to match it — match-or-abort)
  /// and derives the SNI server-name from `peer` + the cluster so the dialer's verifier matches
  /// it against the server cert's SAN. On success the handshake Initial is queued for the next
  /// [`Self::poll_transmit`].
  ///
  /// Returns the typed [`DialError`] when the dial is refused — the connection cap, or a peer id
  /// whose encoding exceeds the SNI scheme's 29-byte bound (the derived DNS label would be
  /// invalid; see `sni_for` — [`Self::connect_with_server_name`] is the escape hatch) — so a
  /// driver can back off or report the configuration error instead of mistaking it for a
  /// scheduled dial.
  pub fn connect(&mut self, now: Instant, remote: SocketAddr, peer: I) -> Result<(), DialError> {
    let server_name = sni_for(&peer, &self.cluster);
    self.connect_with_server_name(now, remote, peer, &server_name)
  }

  /// Dial `peer` at `remote` presenting an EXPLICIT SNI `server_name` instead of the derived
  /// `node-<id-hex>.<cluster-hex>.sailing` form — the dial-side counterpart of
  /// [`Self::dangerous_custom_identity`]: a deployment whose id encodings exceed the derived
  /// scheme's 29-byte label bound (or whose certificates use its own naming) supplies the name
  /// its server certs are actually minted for. Everything else is identical to
  /// [`Self::connect`], including the dialed-expectation match-or-abort on `peer`: the SNI
  /// names the certificate to validate against, never the consensus identity — that still comes
  /// from the identity source's post-handshake authentication.
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

  /// Feed one inbound UDP datagram from `remote` into the QUIC stack, then drain the bridge's
  /// connection events (bind newly-connected peers, decode readable streams into consensus
  /// messages) and pump the endpoint's resulting outgoing messages back out over the streams.
  ///
  /// `ecn` is the received ECN codepoint when the driver's socket reports one (`None` is always
  /// safe).
  pub fn handle_udp<L, S>(
    &mut self,
    now: impl Into<Now>,
    remote: SocketAddr,
    ecn: Option<EcnCodepoint>,
    data: &[u8],
    log: &mut L,
    stable: &mut S,
  ) where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let now: Now = now.into();
    let std_now = self.quinn_now(now.mono());
    self.bridge.handle_datagram(std_now, remote, ecn, data);
    self.drain_bridge(now, log, stable);
    self.pump(now.mono());
  }

  /// Fire all QUIC + consensus timers at `now`, then drain the bridge and pump.
  pub fn handle_timeout<L, S>(&mut self, now: impl Into<Now>, log: &mut L, stable: &mut S)
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let now: Now = now.into();
    let std_now = self.quinn_now(now.mono());
    self.bridge.handle_timeout(std_now);
    self.endpoint.handle_timeout(now, log, stable);
    self.drain_bridge(now, log, stable);
    self.pump(now.mono());
  }

  /// Drain storage completions into the consensus endpoint, then drain the bridge and pump. The
  /// [`StorageProgress`] is RE-DERIVED from the stores AFTER the bridge dispatch, so the driver
  /// re-drives without sleeping while a completion is queued — including one a bridge-dispatched
  /// inbound handler submitted, which the endpoint's own (pre-bridge) verdict could not see.
  pub fn handle_storage<L, S>(
    &mut self,
    now: impl Into<Now>,
    log: &mut L,
    stable: &mut S,
  ) -> StorageProgress
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let now: Now = now.into();
    let _ = self.endpoint.handle_storage(now, log, stable);
    self.drain_bridge(now, log, stable); // inbound handlers may submit storage after the drain above
    self.pump(now.mono());
    // Re-derive from the stores AFTER the bridge dispatch — catches a bridge-submitted append / vote
    // persist the endpoint's own (pre-bridge) verdict could not see.
    if log.has_pending() || stable.has_pending() {
      StorageProgress::MorePending
    } else {
      StorageProgress::Drained
    }
  }

  /// Pop one outbound datagram (destination + owned bytes), or `None` when the queue is empty.
  /// The driver drains this to exhaustion after every `handle_*` call.
  pub fn poll_transmit(&mut self) -> Option<(SocketAddr, Vec<u8>)> {
    self.bridge.poll_transmit()
  }

  /// The next deadline the driver should fire [`Self::handle_timeout`] at: the earlier of the
  /// consensus endpoint's deadline and the QUIC stack's (quinn's connection timers with the
  /// authentication deadline folded in) — or `now` IMMEDIATELY when the bridge holds deferred
  /// work that progresses without any inbound datagram (the one-tick endpoint-event feedback, a
  /// queued connection event, a half-drained receive stream). The immediate deadline is the last
  /// `now` the driver passed in, NOT a wall-clock read, so simulations stay deterministic.
  pub fn poll_timeout(&mut self) -> Option<Instant> {
    if self.bridge.has_pending_work() {
      return Some(self.last_now);
    }
    let quic = self
      .bridge
      .min_timeout()
      .and_then(|d| self.crate_instant(d));
    match (self.endpoint.poll_timeout(), quic) {
      (Some(a), Some(b)) => Some(a.min(b)),
      (a, None) => a,
      (None, b) => b,
    }
  }

  /// Pop the next consensus application [`Event`] (a committed entry, a read state, …), or
  /// `None` when the queue is empty.
  pub fn poll_event(&mut self) -> Option<Event<I, F::Response>> {
    self.endpoint.poll_event()
  }

  /// Whether a BOUND (identity-validated) connection to `peer` currently exists — the link
  /// outbound consensus frames route over. A driver polls this to redial a configured peer whose
  /// connection idled out or was lost: without a redial a dead mesh edge stays dead (messages to
  /// it are dropped; consensus retransmission re-drives them once the link returns) until the
  /// peer happens to dial back. Also `false` while a dial/handshake is still in flight, so a
  /// redialing driver must pace itself rather than treat every `false` as dead-link proof.
  pub fn has_bound_conn(&self, peer: &I) -> bool {
    self.bridge.handle_for(peer).is_some()
  }

  /// The number of outgoing messages the send path refused because their encoded frame would
  /// exceed the transport frame limit (the peer's decoder would fatally reject the declared
  /// length, so such a message can never deliver; consensus retransmission only re-drops it). A
  /// non-zero, growing count signals an oversized snapshot or command payload.
  pub fn oversized_outbound_dropped(&self) -> u64 {
    self.bridge.oversized_dropped()
  }

  /// Propose a client command on this node (must be the leader). Mirrors
  /// [`Endpoint::propose`]; the resulting replication messages are pumped out immediately.
  pub fn submit_propose<L, S>(
    &mut self,
    now: impl Into<Now>,
    log: &mut L,
    stable: &S,
    cmd: &F::Command,
  ) -> Result<Index, ProposeError<I>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let now: Now = now.into();
    let r = self.endpoint.propose(now, log, stable, cmd);
    self.pump(now.mono());
    r
  }

  /// Propose a membership change (single-step). Mirrors [`Endpoint::propose_conf_change`].
  pub fn propose_conf_change<L, S>(
    &mut self,
    now: impl Into<Now>,
    log: &mut L,
    stable: &S,
    cc: crate::ConfChange<I>,
  ) -> Result<Index, ProposeError<I>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
    I: Data,
  {
    let now: Now = now.into();
    let r = self.endpoint.propose_conf_change(now, log, stable, cc);
    self.pump(now.mono());
    r
  }

  /// Propose a joint-consensus membership change. Mirrors [`Endpoint::propose_conf_change_v2`].
  pub fn propose_conf_change_v2<L, S>(
    &mut self,
    now: impl Into<Now>,
    log: &mut L,
    stable: &S,
    cc: crate::ConfChangeV2<I>,
  ) -> Result<Index, ProposeError<I>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
    I: Data,
  {
    let now: Now = now.into();
    let r = self.endpoint.propose_conf_change_v2(now, log, stable, cc);
    self.pump(now.mono());
    r
  }

  /// Propose a cluster-wide read-mode migration. Mirrors [`Endpoint::propose_read_mode_change`].
  pub fn propose_read_mode_change<L, S>(
    &mut self,
    now: impl Into<Now>,
    log: &mut L,
    stable: &S,
    mode: crate::ReadOnlyOption,
  ) -> Result<Index, ProposeError<I>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
    I: Data,
  {
    let now: Now = now.into();
    let r = self
      .endpoint
      .propose_read_mode_change(now, log, stable, mode);
    self.pump(now.mono());
    r
  }

  /// Initiate a linearizable read; the resulting `ReadState` surfaces via [`Self::poll_event`].
  pub fn read_index<L, S>(
    &mut self,
    now: impl Into<Now>,
    log: &L,
    stable: &S,
    context: bytes::Bytes,
  ) -> Result<(), crate::ReadIndexError>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let now: Now = now.into();
    let r = self.endpoint.read_index(now, log, stable, context);
    self.pump(now.mono());
    r
  }

  /// Begin transferring leadership to `to`. Mirrors [`Endpoint::transfer_leader`].
  pub fn transfer_leader<L, S>(
    &mut self,
    now: impl Into<Now>,
    log: &L,
    stable: &S,
    to: I,
  ) -> Result<(), TransferError<I>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let now: Now = now.into();
    let r = self.endpoint.transfer_leader(now, log, stable, to);
    self.pump(now.mono());
    r
  }

  /// Drain the bridge's connection-event queues:
  /// - `connected` → open the send stream + write the identity preface as its FIRST frame, then
  ///   attempt the cert-only `authenticate` probe (a certificate-based scheme binds here; the
  ///   provided `Hello` stays `Authenticating` until the peer's preface frame arrives);
  /// - stream-ready → retry staged sends, read frames off the recv stream, and route each by the
  ///   connection's phase: an `Authenticating` connection's first frame goes to `authenticate`;
  ///   a `Validated` connection's frames decode to consensus messages fed to the endpoint. A
  ///   framing violation closes the connection; a latched graceful FIN closes it AFTER the
  ///   already-decoded frames are delivered (deliver-before-close);
  /// - `lost` → reap the closed connection from routing (the entry drains to quinn's `Drained`).
  // Takes the full `Now` (not a bare `Instant`): a decoded consensus message dispatched below reaches
  // `endpoint.handle_message`, and a network-driven election there (VoteResponse → become_leader → no-op)
  // must stamp the SYNCHRONIZED wall. Only the quinn/bridge timers use the monotonic `now.mono()`.
  fn drain_bridge<L, S>(&mut self, now: Now, log: &mut L, stable: &mut S)
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let std_now = self.quinn_now(now.mono());
    while let Some(h) = self.bridge.take_connected() {
      // Open the send stream and write our preface as frame zero. The send stream is empty here
      // (consensus frames are gated until `Validated`), so the preface leads the stream.
      let me = self.endpoint.id();
      let mut preface = Vec::new();
      self.identity.write_control_preface(&me, &mut preface);
      self.bridge.open_send_and_preface(std_now, h, &preface);
      // The cert-only probe: NO control frame has been delivered yet (`None`), so a
      // certificate-based source can bind now and a preface source reports `Pending` and waits
      // for the peer's first frame below.
      let certs = self.bridge.peer_certs(h);
      let outcome = self
        .identity
        .authenticate(&IdentityCtx::new(&certs, None, self.cluster));
      self.apply_outcome(std_now, h, outcome);
    }
    for h in self.bridge.take_ready_unique() {
      // A `Writable` signal means a formerly-blocked send may now progress; a `Readable` means
      // new bytes. Retry the staged send first, then read.
      self.bridge.flush_stream(std_now, h);
      if self.bridge.ingest_recv(std_now, h) {
        // The read closed the connection inline (peer RESET / pre-auth bound violation) with
        // nothing queued to deliver — skip the provably-empty frame drain.
        continue;
      }
      loop {
        let frame = match self.bridge.next_frame(h) {
          Ok(Some(f)) => f,
          Ok(None) => break,
          // A framing violation (over-cap declared length): terminal for the connection, never
          // for the consensus core.
          Err(_) => {
            self.bridge.close_local(std_now, h);
            break;
          }
        };
        if self.bridge.is_authenticating(h) {
          // The first frame on an authenticating connection is the peer's identity preface —
          // authenticate, NOT decode. It is a COMPLETE popped frame, so this is the sole hello
          // opportunity: a malformed one rejects (closing the connection), never defers.
          let certs = self.bridge.peer_certs(h);
          let outcome =
            self
              .identity
              .authenticate(&IdentityCtx::new(&certs, Some(&frame), self.cluster));
          self.apply_outcome(std_now, h, outcome);
          if !self.bridge.is_validated(h) {
            // Rejected (or candidate mismatch): the connection is closed and queued on `lost`;
            // stop pulling frames from it.
            break;
          }
        } else if self.bridge.is_validated(h) {
          // A validated connection: the frame is a consensus message, decoded zero-copy from the
          // frame's shared bytes. A frame that fails to decode is a peer fault: close the
          // connection (consensus retransmission re-drives anything lost); the endpoint is never
          // poisoned by transport input.
          let from = self.bridge.bound_peer_of(h);
          match (from, crate::wire::decode_message::<I>(frame)) {
            (Some(from), Ok(msg)) => {
              self.endpoint.handle_message(now, log, stable, from, msg);
            }
            _ => {
              self.bridge.close_local(std_now, h);
              break;
            }
          }
        } else {
          // Closed (or otherwise no longer routable): drop the remaining frames.
          break;
        }
      }
      // Deliver-before-close: a gracefully finished stream's complete frames were just drained;
      // only now tear the connection down.
      if self.bridge.fin_received(h) {
        self.bridge.close_local(std_now, h);
      }
    }
    while let Some(h) = self.bridge.take_lost() {
      self.bridge.reap(h);
    }
  }

  /// Apply the coordinator-owned binding policy to an [`IdentityOutcome`] for connection `h`:
  ///
  /// - `Identified(candidate)`: the attested cluster MUST equal this coordinator's cluster (for
  ///   the provided source this is un-bypassable — it reports the genuine parsed cluster); the
  ///   candidate must NOT be this node's own id (an accepted connection has no dialed
  ///   expectation, so without this gate a duplicate-id member with a valid cluster cert would
  ///   bind AS us and its frames would carry our sender id); then a DIALED connection requires
  ///   `candidate == dialed_expectation` (match-or-abort) while an ACCEPTED one adopts the
  ///   candidate. On acceptance the connection is promoted to `Validated`.
  /// - `Pending`: more control input is needed — leave the connection `Authenticating`.
  /// - `Rejected`: close.
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
    if candidate == self.endpoint.id() {
      self.bridge.close_local(std_now, h);
      return;
    }
    match self.bridge.dialed_expectation_of(h) {
      // Dialed: the authenticated identity must be exactly the peer we meant to reach.
      Some(expected) if candidate != expected => {
        self.bridge.close_local(std_now, h);
      }
      // Dialed-and-matched, or accepted (adopt): bind.
      _ => self.bridge.bind_validated(std_now, h, candidate),
    }
  }

  /// Drain the endpoint's outgoing backlog into an owned `Vec` (releasing the endpoint borrow),
  /// route each message over the resolved peer's stream, then run ONE unconditional bridge
  /// `service` over the whole table.
  ///
  /// **The pump-end `service` is the single wakeup mechanism for the QUIC transport.** Every
  /// coordinator pass — `handle_udp`, `handle_timeout`, `handle_storage`, and the `submit_*`
  /// proxies — ends here, AFTER all of `drain_bridge`'s connection mutations and this pump's own
  /// routed writes. quinn collects a connection's queued output (datagrams, STREAM data,
  /// credit/control frames) only when `service` polls it, so this guarantees every frame any
  /// mutation queued THIS pass reaches the outbound queue THIS pass — by construction, not
  /// per-operation: a future mutation needs no `service` plumbing of its own to be wakeup-safe.
  ///
  /// A message to a peer with NO bound connection is dropped: consensus retransmission re-drives
  /// it, and the driver's redial policy ([`Self::has_bound_conn`]) restores the link.
  fn pump(&mut self, now: Instant) {
    // Track the LIVE membership: a committed configuration change (applied inside the endpoint
    // during this very pass) can grow the tracked peer set, and a cap frozen at construction
    // would refuse the new members' mesh connections. Recompute the floor against the configured
    // cap each pump — monotone (the bridge only raises), cheap (four set iterations over the
    // committed config), and positioned where every pass already ends.
    let peers = tracked_peer_count(&self.endpoint.conf_state(), &self.endpoint.id());
    self.bridge.raise_max_connections(
      self
        .configured_max_connections
        .max(mesh_connection_floor(peers)),
    );
    let mut outgoing = Vec::new();
    while let Some(o) = self.endpoint.poll_message() {
      outgoing.push(o);
    }
    let std_now = self.quinn_now(now);
    for o in outgoing {
      let (to, msg) = o.into_parts();
      if let Some(h) = self.bridge.handle_for(&to) {
        self.bridge.write_framed(std_now, h, &msg);
      }
    }
    self.bridge.service(std_now);
  }
}

#[cfg(test)]
impl<I, F, ID> QuicCoordinator<I, F, ID>
where
  I: NodeId,
  F: StateMachine,
{
  /// Test observable: the bridge's current effective connection cap (the live-membership floor
  /// regression watches it grow across a committed configuration change).
  pub(crate) fn effective_max_connections(&self) -> usize {
    self.bridge.max_connections_for_test()
  }
}

impl<I, F, ID> core::fmt::Debug for QuicCoordinator<I, F, ID>
where
  F: StateMachine,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("QuicCoordinator").finish_non_exhaustive()
  }
}

#[cfg(test)]
mod tests;
