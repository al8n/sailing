//! The `Bridge`: wraps one `quinn_proto::Endpoint`, owns the [`ConnTable`], and runs the
//! Endpoint↔Connection service pump that drives every QUIC connection to fixpoint each tick.
//!
//! quinn-proto's polling contract requires the caller to drain ALL of a connection's poll
//! surfaces every progress step: `poll()` (application events), `poll_endpoint_events()`
//! (endpoint-facing feedback), and `poll_transmit()` (outbound datagrams), plus `handle_timeout`.
//! Omitting any one stalls the connection — `poll_endpoint_events()` carries `NeedIdentifiers` /
//! `RetireConnectionId` feedback that must round-trip through `Endpoint::handle_event` for CID
//! rotation to keep working.
//!
//! **One-tick deferral.** Endpoint events drained from a connection are NOT fed back into
//! `Endpoint::handle_event` in the same pass — they are queued in
//! [`Bridge::pending_endpoint_events`] and applied at the TOP of the next `service` call,
//! mirroring quinn-proto's reference driver (a connection task drains to a channel; the endpoint
//! task applies on its own iteration). Feeding them back inline would hold `&mut endpoint` and
//! `&mut connection` simultaneously and, once connections are reaped, reintroduce the
//! connection-ID slab-reuse race the deferral avoids.
//!
//! The terminal `EndpointEvent::Drained` rides the SAME deferral: queued in step (2), applied in
//! step (1) — where it is forwarded to `Endpoint::handle_event` (freeing quinn's slab slot and
//! CID indexes), the local entry is reaped, and residual queued events for the freed handle are
//! purged. This preserves quinn's per-connection event FIFO across the slab-free; handling
//! `Drained` inline would free the slot this pass and replay a still-deferred earlier event
//! against a reused handle next pass.
//!
//! The `Bridge` works natively in [`std::time::Instant`] — quinn's time currency. The sailing
//! [`Instant`](crate::Instant) adapter lives one layer up (the coordinator).

use std::{
  collections::VecDeque,
  net::SocketAddr,
  time::{Duration, Instant},
  vec::Vec,
};

use quinn_proto::{
  ClientConfig, ConnectError, ConnectionHandle, DatagramEvent, Dir, EcnCodepoint, Endpoint,
  EndpointEvent, Event, StreamEvent, VarInt, WriteError,
};
use rustls::pki_types::CertificateDer;

use super::{
  super::frame::{MAX_FRAME_LEN, encode_frame},
  MAX_HELLO_LEN,
  conn::{ConnEntry, ConnTable, Phase},
  crypto::QuicOptions,
};
use crate::{Data, Message, NodeId, TransportError};

/// Maximum number of LIVE connections the bridge keeps for any ONE peer. On validation the bridge
/// closes the OLDEST same-peer connections beyond this bound, so a flapping or crash-looping
/// valid-cert member cannot accumulate unbounded same-peer connections and consume every
/// `max_connections` slot — a connection-table-exhaustion DoS reachable within the non-Byzantine
/// threat model (a buggy but valid-cert member re-validating fresh connections passes the
/// [`AUTH_DEADLINE`] gate each time).
///
/// **Value = 3:** the 2 steady-state mutual-dial connections (each side dials the other and BOTH
/// are kept — both deliver inbound frames; tearing the "displaced" one down would break the mesh)
/// plus 1 reconnect slot (a new dial/accept briefly overlapping the old one). Reaping is by
/// creation recency ([`ConnEntry::seq`]): the just-validated connection is excluded outright and
/// its mutual-dial sibling is the newest survivor, so the steady-state pair is never reaped.
///
/// Consistent with the global cap by construction: the coordinator raises `max_connections` to
/// `mesh_connection_floor(n) = max(4, 3*(n-1))`, exactly `n-1` peers times this per-peer bound.
const PER_PEER_CONN_LIMIT: usize = 3;

/// `max_datagrams` per `poll_transmit` call: quinn packs up to this many equal-size datagrams
/// into ONE transmit (GSO segments; the last may be shorter), so a congestion window drains in a
/// few calls instead of one per datagram. The bridge splits the segments back into per-datagram
/// payloads, so drivers send one UDP datagram per popped entry — GSO is not required of them.
const MAX_TRANSMIT_DATAGRAMS: usize = 10;

/// Per-pass receive budget: `ingest_recv` reads at most this many stream bytes per connection per
/// pump. A multi-megabyte receive window packed with tiny frames would otherwise push its whole
/// content through the decoder in ONE pump before any frame is delivered; bounding the read
/// bounds the ready queue to one budget's worth, and a read that stops with bytes still readable
/// defers the connection to the NEXT pump (`deferred_ready`), draining the window one budget per
/// pump.
const READ_BUDGET: usize = 256 * 1024;

/// Outbound staging cap per connection. When `outbound` would exceed this, the peer has stopped
/// consuming consensus traffic — close the connection rather than buffer without bound (the same
/// constant and rationale as the stream transport's `MAX_CONN_OUT_BUF`: one maximum frame in
/// flight plus one more arriving).
const MAX_CONN_OUT_BUF: usize = 2 * MAX_FRAME_LEN;

/// Application error code on a locally-issued connection close.
const CONNECTION_CLOSE_CODE: u32 = 1;

/// Application error code on a per-stream RESET (the retire of a violating peer-opened stream).
const STREAM_RESET_CODE: u32 = 2;

/// How long a connection may sit in [`Phase::Authenticating`] (QUIC handshake done, identity not
/// yet bound) before the bridge tears it down. A peer that completes mTLS with a valid cluster
/// cert but never sends a valid hello would otherwise pin a connection slot forever — quinn's
/// idle timeout is refreshed by the peer's keep-alive PINGs, so it never trips, and N such peers
/// exhaust `max_connections`.
///
/// 5 s is comfortably above any legitimate authentication (the QUIC handshake has ALREADY
/// completed; the window covers only the one-round-trip preface exchange, ~1 RTT even across
/// several PTO-driven retransmits) yet bounded, so a silent valid-cert peer frees its slot in
/// seconds rather than holding it for the connection's keep-alive-extended lifetime.
const AUTH_DEADLINE: Duration = Duration::from_secs(5);

/// Why an outbound dial did not produce a live connection. Surfaced (not swallowed) so a caller
/// can back off or report saturation.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DialError {
  /// The bridge holds `max_connections` live connections already; this dial was skipped (no quinn
  /// `Connection` allocated, nothing inserted) to keep dialed + accepted connections within the
  /// cap — the same bound the inbound accept path enforces statelessly.
  #[error("connection cap reached ({cap}); dial refused")]
  AtCapacity {
    /// The configured live-connection cap that was hit.
    cap: usize,
  },
  /// quinn refused the dial (e.g. no client config, or an invalid server name).
  #[error("quinn refused the dial: {0}")]
  Connect(#[from] ConnectError),
}

/// Split one transmit's contents into its on-the-wire UDP datagrams: every chunk is exactly
/// `segment_size` bytes except the last (quinn's GSO segment layout). `max(1)` keeps `chunks`
/// panic-free for a `segment_size` quinn never actually emits.
fn transmit_segments(contents: &[u8], segment_size: usize) -> core::slice::Chunks<'_, u8> {
  contents.chunks(segment_size.max(1))
}

/// The earlier of two optional instants — folds the auth deadline in alongside quinn's earliest
/// connection timer without either masking the other.
fn min_opt(a: Option<Instant>, b: Option<Instant>) -> Option<Instant> {
  match (a, b) {
    (Some(a), Some(b)) => Some(a.min(b)),
    (a, None) => a,
    (None, b) => b,
  }
}

/// Wraps a `quinn_proto::Endpoint` plus its connection table and runs the service pump. The
/// coordinator that owns this bridge drains the `connected` / `stream_ready` / `lost` event
/// queues (via the `take_*` accessors) and the outbound datagram queue ([`Self::poll_transmit`]).
pub(crate) struct Bridge<I> {
  /// The owned quinn endpoint. Single source of new/accepted connections.
  endpoint: Endpoint,
  /// Pool of live connections keyed by `ConnectionHandle`, plus the per-peer routing index.
  table: ConnTable<I>,
  /// The client config for outbound dials, snapshotted from the options at construction. `None`
  /// if the options carried no client config (accept-only).
  client: Option<ClientConfig>,
  /// Cap on live connections (dialed + accepted). An inbound attempt that would exceed it is
  /// REFUSED (a stateless close) instead of allocating a `Connection`, bounding an
  /// untrusted-network flood of foreign-CA / no-cert Initials before identity validation.
  max_connections: usize,
  /// Outbound datagrams awaiting the driver's `poll_transmit` drain.
  out: VecDeque<(SocketAddr, Vec<u8>)>,
  /// Endpoint events drained from connections on the PREVIOUS `service` pass, applied at the top
  /// of the next one (the one-tick deferral; see the module docs). The terminal `Drained` rides
  /// this queue like any other event so quinn's per-connection FIFO survives the slab-free.
  pending_endpoint_events: VecDeque<(ConnectionHandle, EndpointEvent)>,
  /// Count of outbound messages refused because their encoded frame would exceed
  /// [`MAX_FRAME_LEN`]. The receive side closes a connection on an over-cap declared length, so
  /// such a message could never deliver; the send path drops it visibly here instead.
  oversized_dropped: u64,
  /// Connections that just reached `Event::Connected`, drained via [`Self::take_connected`].
  connected: VecDeque<ConnectionHandle>,
  /// Connections with a readable / newly-writable / newly-peer-opened stream, drained via
  /// [`Self::take_ready_unique`]. quinn pushes a `Readable` per received STREAM frame, so a
  /// handle can sit here many times; the drain collapses duplicates so each connection is read at
  /// most one budget per pump.
  stream_ready: VecDeque<ConnectionHandle>,
  /// Connections whose read stopped on its per-pass budget with stream bytes still readable,
  /// queued by [`Self::ingest_recv`] for the NEXT pump (not the one draining `stream_ready` now —
  /// landing there would let one pump drain the whole window). [`Self::take_ready_unique`] folds
  /// this in at the start of each drain; [`Self::has_pending_work`] counts it so a
  /// `poll_timeout`-driven driver re-pumps immediately while leftover remains.
  deferred_ready: VecDeque<ConnectionHandle>,
  /// Connections that just emitted `Event::ConnectionLost` (or were locally closed), drained via
  /// [`Self::take_lost`].
  lost: VecDeque<ConnectionHandle>,
  /// Deferred-service marker for the per-message write path: [`Self::write_framed`] sets this
  /// instead of running a whole-table `service` per message; the coordinator's single pump-end
  /// `service` consumes it (an inline pass per message would be O(messages × connections) of
  /// redundant quinn polling).
  needs_service: bool,
  /// Test-only: how many service passes have run — the observable that pins "every connect exit
  /// services" (a drained deferral queue alone is also true of an exit that skipped the pass).
  #[cfg(test)]
  services_run: u64,
}

impl<I: NodeId> Bridge<I> {
  /// Build a bridge from `opts`. `rng_seed` seeds the endpoint's connection-ID / token RNG
  /// (`None` = OS entropy; the deterministic tests pass a fixed seed).
  ///
  /// MTU discovery is enabled: consensus traffic routinely exceeds the 1200-byte initial MTU and
  /// cluster links are datacenter-grade, so the default probe schedule converges immediately; a
  /// black-holed probe merely keeps the connection at the floor.
  pub(crate) fn new(opts: &QuicOptions, rng_seed: Option<[u8; 32]>) -> Self {
    let endpoint = Endpoint::new(
      opts.endpoint_config(),
      opts.server_config(),
      /* allow_mtud = */ true,
      rng_seed,
    );
    Self {
      endpoint,
      table: ConnTable::new(),
      client: opts.client_config(),
      max_connections: opts.max_connections(),
      out: VecDeque::new(),
      pending_endpoint_events: VecDeque::new(),
      oversized_dropped: 0,
      connected: VecDeque::new(),
      stream_ready: VecDeque::new(),
      deferred_ready: VecDeque::new(),
      lost: VecDeque::new(),
      needs_service: false,
      #[cfg(test)]
      services_run: 0,
    }
  }

  /// Whether the live-connection cap is reached — the SINGLE cap predicate both the inbound
  /// accept and the outbound dial consult before allocating a `Connection`.
  fn at_capacity(&self) -> bool {
    self.table.len() >= self.max_connections
  }

  /// Raise the live-connection cap to `to` if it is higher (never lowers). The coordinator
  /// recomputes the membership-sized mesh floor each pump — committed configuration changes can
  /// GROW the tracked peer set long after construction, and a cap frozen at build time would
  /// refuse the new members' legitimate mesh connections. Monotone within a process lifetime: a
  /// membership shrink does not reclaim flood budget until restart (lowering mid-flight could
  /// refuse a still-draining peer's reconnect during the transition).
  pub(crate) fn raise_max_connections(&mut self, to: usize) {
    if to > self.max_connections {
      self.max_connections = to;
    }
  }

  /// Dial `remote`, validating its certificate against `server_name`. `expected` is the peer this
  /// dial is meant to reach — recorded on the entry so the coordinator's binding policy can
  /// require the authenticated identity to match it (match-or-abort). Runs one service pass so
  /// the initial handshake datagram is queued for the next `poll_transmit`.
  pub(crate) fn connect(
    &mut self,
    now: Instant,
    remote: SocketAddr,
    server_name: &str,
    expected: I,
  ) -> Result<ConnectionHandle, DialError> {
    // A queued Drained may be holding a freed slot hostage (the one-tick deferral): apply the
    // feedback FIRST so a reconnect is never refused for capacity that is already released.
    self.apply_deferred();
    // Every exit below — refusals included — runs a service pass: `apply_deferred` may have fed
    // CID-rotation feedback (`NewIdentifiers`) into a connection, queueing NEW_CONNECTION_ID
    // frames that only a `poll_transmit` after a service makes visible. An early return without
    // one would strand that output (the deferral queue is drained, so `has_pending_work` no
    // longer reports it) until unrelated activity.
    if self.at_capacity() {
      self.service(now);
      return Err(DialError::AtCapacity {
        cap: self.max_connections,
      });
    }
    let cfg = match self.client.clone() {
      Some(cfg) => cfg,
      None => {
        self.service(now);
        return Err(ConnectError::NoDefaultClientConfig.into());
      }
    };
    let (h, conn) = match self.endpoint.connect(now, cfg, remote, server_name) {
      Ok(pair) => pair,
      Err(e) => {
        self.service(now);
        return Err(e.into());
      }
    };
    self.table.insert(h, ConnEntry::new(conn, Some(expected)));
    self.service(now);
    Ok(h)
  }

  /// Feed one inbound UDP datagram from `remote` into the endpoint, routing the resulting
  /// [`DatagramEvent`], then run a service pass.
  ///
  /// - `ConnectionEvent` → delivered to the addressed connection.
  /// - `NewConnection` → accepted while under the connection cap, else REFUSED (a stateless
  ///   close) so an untrusted-network flood cannot allocate unbounded connection state.
  /// - `Response` → a stateless endpoint reply (Retry / version negotiation / stateless reset)
  ///   forwarded outbound.
  pub(crate) fn handle_datagram(
    &mut self,
    now: Instant,
    remote: SocketAddr,
    ecn: Option<EcnCodepoint>,
    data: &[u8],
  ) {
    // Same slot-release ordering as `connect`: a queued Drained frees capacity BEFORE this
    // datagram's accept decision consults the cap.
    self.apply_deferred();
    let mut scratch = Vec::new();
    let ev = self.endpoint.handle(
      now,
      remote,
      /* local_ip = */ None,
      ecn,
      bytes::BytesMut::from(data),
      &mut scratch,
    );
    match ev {
      Some(DatagramEvent::ConnectionEvent(h, conn_ev)) => {
        if let Some(e) = self.table.entry(h) {
          e.conn.handle_event(conn_ev);
        }
      }
      Some(DatagramEvent::NewConnection(incoming)) => {
        if self.at_capacity() {
          // REFUSE at the cap (a stateless close) rather than allocate a `Connection`. `refuse`
          // returns a single close transmit written into `rbuf`.
          let mut rbuf = Vec::new();
          let t = self.endpoint.refuse(incoming, &mut rbuf);
          rbuf.truncate(t.size);
          self.out.push_back((t.destination, rbuf));
          self.service(now);
          return;
        }
        let mut abuf = Vec::new();
        match self
          .endpoint
          .accept(incoming, now, &mut abuf, /* server_config = */ None)
        {
          // An accepted connection has no dialed expectation: the coordinator adopts whatever
          // identity authenticates (subject to the unconditional cluster cross-check).
          Ok((h, conn)) => self.table.insert(h, ConnEntry::new(conn, None)),
          Err(e) => {
            // quinn attaches a refusal/close transmit whenever it owes the peer an immediate
            // close; surface it so the peer sees the close at once instead of waiting out its
            // retransmit budget.
            if let Some(t) = e.response {
              abuf.truncate(t.size);
              self.out.push_back((t.destination, abuf));
            }
          }
        }
      }
      Some(DatagramEvent::Response(t)) => {
        scratch.truncate(t.size);
        self.out.push_back((t.destination, scratch));
      }
      None => {}
    }
    self.service(now);
  }

  /// The fixpoint service pump. Drives every connection one progress step:
  ///
  /// 1. Apply the previous pass's deferred endpoint-event feedback FIRST (a deferred `Drained`
  ///    here frees the endpoint slab, reaps the local entry, and purges residual events for the
  ///    freed handle).
  /// 2. Drain each connection's `poll_endpoint_events()`, deferring the feedback to the next
  ///    pass — `Drained` included (the FIFO-preserving deferral; see the module docs).
  /// 3. Drain each connection's application `poll()` events.
  /// 4. Collect each connection's outbound transmits into `out` — the ONLY place quinn's queued
  ///    output (datagrams, STREAM data, credit/control frames) becomes visible, so every
  ///    quinn-mutating operation must be followed by a `service` this pump; the coordinator's
  ///    single pump-end `service` provides that systematically.
  ///
  /// Finally, reap any connection that has sat `Authenticating` past its [`AUTH_DEADLINE`]
  /// (handshake done, identity never bound). `close_local` is a non-recursive state mutation, so
  /// a mass simultaneous expiry does at most this one pass; the CONNECTION_CLOSE bytes are
  /// collected by the next pass (`lost` makes [`Self::has_pending_work`] true, so a
  /// `poll_timeout`-driven driver re-pumps at once).
  pub(crate) fn service(&mut self, now: Instant) {
    #[cfg(test)]
    {
      self.services_run += 1;
    }
    // Consume the per-message write deferral: this pass collects everything `write_framed`
    // staged since the last one.
    self.needs_service = false;
    // Step 1: the previous pass's deferred feedback.
    self.apply_deferred();

    for h in self.table.handles() {
      // Fire any expired timers first so the resulting events/transmits are polled this pass.
      if let Some(e) = self.table.entry(h) {
        e.conn.handle_timeout(now);
      }

      // Step 2: drain endpoint-facing events into the deferral queue (collected into a local
      // first so the entry borrow is released before the queue push).
      let mut events = Vec::new();
      if let Some(e) = self.table.entry(h) {
        while let Some(ev) = e.conn.poll_endpoint_events() {
          events.push(ev);
        }
      }
      for ev in events {
        self.pending_endpoint_events.push_back((h, ev));
      }

      // Step 3: drain application events (`poll()` pulled into a local so the entry borrow is
      // released before `on_app_event` re-borrows `self`).
      loop {
        let next = self.table.entry(h).and_then(|e| e.conn.poll());
        match next {
          Some(ev) => self.on_app_event(now, h, ev),
          None => break,
        }
      }

      // Step 4: collect outbound transmits. quinn packs up to MAX_TRANSMIT_DATAGRAMS equal-size
      // datagrams per call (`segment_size` is `Some` exactly when more than one was packed); the
      // queue stays one-UDP-datagram-per-entry, so a single-datagram transmit hands its buffer
      // through owned and a multi-segment one is split.
      let mut tbuf = Vec::new();
      while let Some(e) = self.table.entry(h) {
        let Some(t) = e.conn.poll_transmit(now, MAX_TRANSMIT_DATAGRAMS, &mut tbuf) else {
          break;
        };
        debug_assert!(t.size <= tbuf.len(), "quinn wrote within its buffer");
        match t.segment_size {
          None => {
            tbuf.truncate(t.size);
            self
              .out
              .push_back((t.destination, core::mem::take(&mut tbuf)));
          }
          Some(seg) => {
            for datagram in transmit_segments(&tbuf[..t.size], seg) {
              self.out.push_back((t.destination, datagram.to_vec()));
            }
            tbuf.clear();
          }
        }
      }
    }

    // Auth-deadline reap: collect the expired handles first (releasing the `handles()` borrow),
    // then close them through the shared choke-point.
    let expired: Vec<ConnectionHandle> = self
      .table
      .handles()
      .into_iter()
      .filter(|h| {
        self
          .table
          .entry(*h)
          .is_some_and(|e| e.phase.is_authenticating() && e.auth_deadline.is_some_and(|d| now >= d))
      })
      .collect();
    for h in expired {
      self.close_local(now, h);
    }
  }

  /// Apply the deferred endpoint-event feedback queue, in FIFO order — `service`'s step 1, also
  /// run by [`Self::connect`] and [`Self::handle_datagram`] BEFORE their capacity checks: a
  /// queued terminal `Drained` frees a connection slot, so a reconnect racing the previous
  /// connection's drain must not be refused for capacity the deferral has already released.
  fn apply_deferred(&mut self) {
    while let Some((h, ev)) = self.pending_endpoint_events.pop_front() {
      if ev.is_drained() {
        // Forwarding `Drained` frees quinn's slab slot + CID indexes; the local entry is then
        // reaped. FIFO order means any earlier same-handle event was already applied above while
        // the handle was live. Purge EVERY per-handle queue for the freed handle — not just the
        // deferral queue: quinn REUSES slab handles after `Drained`, so a stale entry surviving
        // into a reused handle's lifetime would mis-target the NEW connection. A stale `lost` is
        // the sharp edge: the coordinator's end-of-drain `reap` would unbind the reused handle's
        // freshly validated route. The stale entries carry no live work — the old generation's
        // table entry is gone (its unbind already ran via `remove`), and quinn emits nothing
        // further for it.
        let _ = self.endpoint.handle_event(h, ev);
        self.table.remove(h);
        self.pending_endpoint_events.retain(|(qh, _)| *qh != h);
        self.connected.retain(|qh| *qh != h);
        self.stream_ready.retain(|qh| *qh != h);
        self.deferred_ready.retain(|qh| *qh != h);
        self.lost.retain(|qh| *qh != h);
        continue;
      }
      // Nested rather than a let-chain: the crate's MSRV (1.85) predates stabilized let-chains.
      if let Some(conn_ev) = self.endpoint.handle_event(h, ev) {
        if let Some(e) = self.table.entry(h) {
          e.conn.handle_event(conn_ev);
        }
      }
    }
  }

  /// Route one connection-level application [`Event`] to the per-connection phase and the
  /// coordinator-facing event queues. `now` stamps the [`AUTH_DEADLINE`] when a connection enters
  /// `Authenticating`.
  fn on_app_event(&mut self, now: Instant, h: ConnectionHandle, ev: Event) {
    match ev {
      Event::Connected => {
        // The QUIC handshake is complete, but identity is NOT yet bound: `Authenticating`, not
        // `Validated`. The coordinator opens the send stream, writes the preface, and runs the
        // binding policy; only that promotes the connection. The deadline stamped here is what
        // reaps a valid-cert peer that never sends a valid hello (its keep-alives refresh the
        // idle timeout, so nothing else would).
        if let Some(e) = self.table.entry(h) {
          e.phase = Phase::Authenticating;
          e.auth_deadline = Some(now + AUTH_DEADLINE);
        }
        self.connected.push_back(h);
      }
      Event::Stream(StreamEvent::Opened { dir: Dir::Bi }) => {
        // The peer opened its bidi stream. quinn emits `Opened` (not `Readable`) for the FIRST
        // frame on a freshly peer-initiated stream, so this is what schedules the preface read.
        self.stream_ready.push_back(h);
      }
      Event::Stream(StreamEvent::Readable { .. }) => {
        self.stream_ready.push_back(h);
      }
      Event::Stream(StreamEvent::Writable { .. }) => {
        // A formerly write-blocked stream may now accept writes; same queue, the drain retries
        // the staged send.
        self.stream_ready.push_back(h);
      }
      Event::Stream(StreamEvent::Available { dir: Dir::Bi }) => {
        // The peer raised its bidi-stream limit, so an `open` that previously returned `None`
        // can now succeed (a stream reopen with staged bytes waiting).
        self.stream_ready.push_back(h);
      }
      Event::ConnectionLost { .. } => {
        // Peer-initiated loss: quinn is already draining toward `Drained`, so no local `close`
        // is re-issued; run the shared teardown tail so the connection is unrouteable atomically.
        self.mark_closed_unbind_push(h);
      }
      Event::Stream(StreamEvent::Stopped { id, .. }) => {
        // The peer sent STOP_SENDING on a send stream of OURS, keyed to that EXACT id.
        // - Our CURRENT consensus send stream → the peer stopped consuming consensus: close the
        //   connection (the single stream IS the consensus channel; a redial reopens it cleanly).
        // - Any other id → a peer-opened stream's unused send half (the peer retired its stream;
        //   its retire `stop`s our never-written send direction). `reset` that exact half so it
        //   reaches `ResetSent` and frees on ack — without this the peer-opened stream never
        //   fully retires in quinn's accounting and the peer never re-grants `MAX_STREAMS`.
        let is_current = self.table.entry(h).is_some_and(|e| e.send == Some(id));
        if is_current {
          self.close_local(now, h);
        } else if let Some(e) = self.table.entry(h) {
          let _ = e
            .conn
            .send_stream(id)
            .reset(VarInt::from_u32(STREAM_RESET_CODE));
        }
      }
      // Not consumed: Uni-stream opens/credit (the transport config advertises a 0 uni limit, so
      // these are unreachable on a conformant path), `Finished` (the transport never `finish`es
      // its consensus send stream), `HandshakeDataReady`, and DATAGRAM events (receive disabled
      // in the transport config). Defensive ignores.
      _ => {}
    }
  }

  /// Pop one outbound datagram (destination + owned bytes), or `None` when the queue is empty.
  pub(crate) fn poll_transmit(&mut self) -> Option<(SocketAddr, Vec<u8>)> {
    self.out.pop_front()
  }

  /// Whether the bridge holds DEFERRED work the next pump must apply WITHOUT waiting on an
  /// inbound datagram: the one-tick endpoint-event feedback (a deferred `Drained` frees the slab
  /// and cap slot), the coordinator-facing `connected`/`stream_ready`/`lost` queues, and the
  /// `deferred_ready` leftover reads (those bytes are already buffered in the connection). A
  /// `poll_timeout`-driven driver re-pumps immediately while this is true; staged OUTBOUND bytes
  /// are deliberately excluded (they only progress on a peer signal, which arrives as an inbound
  /// datagram and lands on `stream_ready`).
  pub(crate) fn has_pending_work(&self) -> bool {
    !self.pending_endpoint_events.is_empty()
      || !self.connected.is_empty()
      || !self.stream_ready.is_empty()
      || !self.deferred_ready.is_empty()
      || !self.lost.is_empty()
  }

  /// The earliest quinn timer across all connections, with the earliest [`AUTH_DEADLINE`] folded
  /// in as a connection timer (a future wake-up, not immediate work — without the fold-in a
  /// sleeping driver would never wake to reap a silent `Authenticating` connection, since the
  /// peer's keep-alives refresh quinn's own idle timer indefinitely). Does NOT account for
  /// deferred immediate work — the coordinator consults [`Self::has_pending_work`] for that.
  pub(crate) fn min_timeout(&mut self) -> Option<Instant> {
    min_opt(
      self.table.min_conn_timeout(),
      self.table.earliest_auth_deadline(),
    )
  }

  /// Fire every connection's timers at `now`, then run a service pass so the resulting events
  /// and retransmits are collected.
  pub(crate) fn handle_timeout(&mut self, now: Instant) {
    for h in self.table.handles() {
      if let Some(e) = self.table.entry(h) {
        e.conn.handle_timeout(now);
      }
    }
    self.service(now);
  }

  /// Count of outbound messages dropped because their encoded frame would exceed
  /// [`MAX_FRAME_LEN`] (forwarded to the coordinator's public counter).
  pub(crate) fn oversized_dropped(&self) -> u64 {
    self.oversized_dropped
  }

  /// Pop the next connection that just reached `Connected`, for the coordinator to write its
  /// preface and start the identity-binding step.
  pub(crate) fn take_connected(&mut self) -> Option<ConnectionHandle> {
    self.connected.pop_front()
  }

  /// Drain this pump's stream-ready work as an ORDER-PRESERVING UNIQUE list: the reads deferred
  /// on the previous pump are folded in first, then `stream_ready`, with duplicate handles
  /// collapsed. quinn pushes a `Readable` per received STREAM frame, so a connection that took N
  /// datagrams sits here N times; without the dedup one pump would read N budgets — proportional
  /// to the buffered window instead of one budget per pump.
  pub(crate) fn take_ready_unique(&mut self) -> Vec<ConnectionHandle> {
    // Deferred leftover reads genuinely come FIRST (matching the doc above): a connection
    // mid-window-drain takes its next budget ahead of this pump's fresh readiness signals, so
    // a large in-flight transfer keeps draining steadily. Every unique handle still runs
    // exactly once per pump either way.
    let mut queue = core::mem::take(&mut self.deferred_ready);
    queue.append(&mut self.stream_ready);
    let mut seen: Vec<ConnectionHandle> = Vec::new();
    while let Some(h) = queue.pop_front() {
      if !seen.contains(&h) {
        seen.push(h);
      }
    }
    seen
  }

  /// Pop the next connection that was lost / locally closed, for the coordinator to reap.
  pub(crate) fn take_lost(&mut self) -> Option<ConnectionHandle> {
    self.lost.pop_front()
  }

  /// The routed connection for `peer`, if a validated one is bound.
  pub(crate) fn handle_for(&self, peer: &I) -> Option<ConnectionHandle> {
    self.table.handle_for(peer)
  }

  /// The peer bound to connection `h`, if it has validated.
  pub(crate) fn bound_peer_of(&mut self, h: ConnectionHandle) -> Option<I> {
    self.table.entry(h).and_then(|e| e.peer)
  }

  /// Whether `h` is in the `Authenticating` window (preface exchange).
  pub(crate) fn is_authenticating(&mut self, h: ConnectionHandle) -> bool {
    self.table.entry(h).is_some_and(|e| e.is_authenticating())
  }

  /// Whether `h` has validated (identity bound; consensus frames flow).
  pub(crate) fn is_validated(&mut self, h: ConnectionHandle) -> bool {
    self.table.entry(h).is_some_and(|e| e.is_validated())
  }

  /// The peer `h` was dialed to reach (`None` for an accepted connection).
  pub(crate) fn dialed_expectation_of(&mut self, h: ConnectionHandle) -> Option<I> {
    self.table.entry(h).and_then(|e| e.dialed_expectation)
  }

  /// The peer's TLS certificate chain for `h`, as validated by the handshake (empty when none —
  /// e.g. a no-client-auth test config).
  pub(crate) fn peer_certs(&mut self, h: ConnectionHandle) -> Vec<CertificateDer<'static>> {
    self
      .table
      .entry(h)
      .and_then(|e| e.conn.crypto_session().peer_identity())
      .and_then(|id| id.downcast::<Vec<CertificateDer<'static>>>().ok())
      .map(|certs| *certs)
      .unwrap_or_default()
  }

  /// Open the consensus send stream for a freshly `Connected` connection and stage the identity
  /// preface as its FIRST frame. An empty preface (a cert-only identity scheme) frames nothing;
  /// an over-budget preface closes the connection here (it could never authenticate — the peer's
  /// pre-auth intake bound rejects it) rather than opening streams on a doomed connection.
  pub(crate) fn open_send_and_preface(
    &mut self,
    now: Instant,
    h: ConnectionHandle,
    preface: &[u8],
  ) {
    let framed_preface = if preface.is_empty() {
      None
    } else {
      if preface.len() > MAX_HELLO_LEN {
        self.close_local(now, h);
        return;
      }
      let mut framed = Vec::new();
      encode_frame(preface, &mut framed);
      Some(framed)
    };
    let opened = {
      let Some(e) = self.table.entry(h) else {
        return;
      };
      if e.preface_done {
        return;
      }
      if e.send.is_none() {
        // `open` returns `None` only when the concurrent-stream limit is exhausted, which cannot
        // happen for the first stream on a fresh connection.
        match e.conn.streams().open(Dir::Bi) {
          Some(sid) => e.send = Some(sid),
          None => return,
        }
      }
      // The send stream is empty here (consensus frames are gated until `Validated`), so the
      // preface frame leads the stream.
      if let Some(framed) = framed_preface {
        e.outbound.extend(framed);
      }
      e.preface_done = true;
      true
    };
    if opened {
      self.flush_outbound(now, h);
      self.service(now);
    }
  }

  /// Bind the authenticated `peer` to connection `h` and promote it to [`Phase::Validated`],
  /// then flush any consensus frames that staged while it was authenticating. The coordinator
  /// calls this once its binding policy accepts the candidate; only after this do routing
  /// lookups see the peer.
  ///
  /// **Last-established-wins, the mutual-dial pair kept, older excess closed.** Under mutual dial
  /// `peer` validates on BOTH the connection this side dialed and the one it accepted; the
  /// routing index re-points at the most-recently-validated handle, the sibling keeps delivering
  /// inbound frames, and neither is torn down. What IS bounded is the per-peer live COUNT
  /// ([`PER_PEER_CONN_LIMIT`]): the OLDEST live same-peer connections beyond it are closed, with
  /// the just-validated `h` excluded outright (a slow hello can validate late — insertion recency
  /// does not track validation recency).
  pub(crate) fn bind_validated(&mut self, now: Instant, h: ConnectionHandle, peer: I) {
    // Idempotency guard, symmetric with `close_local`'s: a handle already `Validated` or torn
    // down is not re-validated — a duplicate hello frame, or a validate racing a close, is a
    // no-op rather than re-running the reap/flush or resurrecting a closed connection.
    if self
      .table
      .entry(h)
      .is_none_or(|e| e.is_validated() || e.phase.is_closed())
    {
      return;
    }
    let stale = self.table.validate_routing(h, &peer, PER_PEER_CONN_LIMIT);
    if let Some(e) = self.table.entry(h) {
      e.phase = Phase::Validated;
      // Validated: clear the authentication deadline so this connection is never a reap
      // candidate, and the deadline-present-iff-authenticating invariant holds.
      e.auth_deadline = None;
      debug_assert!(
        e.auth_deadline.is_some() == e.is_authenticating(),
        "validate: a validated entry carries no auth deadline"
      );
    }
    for stale_h in stale {
      self.close_local(now, stale_h);
    }
    debug_assert!(
      self.table.live_peer_count(&peer) <= PER_PEER_CONN_LIMIT,
      "validate: the per-peer live connection count is within its bound after the reap"
    );
    // Flush any consensus frames that staged while authenticating.
    if self.flush_outbound(now, h) {
      self.service(now);
    }
    // Schedule a post-validation read: the pre-auth intake bound may have left buffered stream
    // bytes unread (a peer that validated US first can pipeline consensus frames behind its
    // hello), and the readiness edge that delivered them is already consumed. Without this
    // enqueue those bytes would sit unread until unrelated traffic woke the connection.
    self.stream_ready.push_back(h);
  }

  /// The shared teardown tail run by BOTH the local-fatal close ([`Self::close_local`]) and the
  /// peer-initiated loss: mark the entry `Closed`, clear its authentication deadline, unbind
  /// routing, RECOVER routing to a live same-peer sibling if one remains, and queue the handle
  /// for the reap. Consolidating these coupled mutations keeps the close-time invariants holding
  /// by construction: a closed connection is unrouteable the instant it is torn down (the entry
  /// is KEPT for the drain — only the routing slot clears), and a peer holding a still-validated
  /// mutual-dial sibling keeps an outbound route across the loss.
  fn mark_closed_unbind_push(&mut self, h: ConnectionHandle) {
    let peer = self.table.entry(h).and_then(|e| e.peer);
    if let Some(e) = self.table.entry(h) {
      e.phase = Phase::Closed;
      e.auth_deadline = None;
    }
    self.table.unbind(h);
    if let Some(p) = peer {
      self.table.promote_routing_if_unbound(&p);
    }
    self.lost.push_back(h);
  }

  /// Tear down connection `h` for a LOCAL fatal decision: issue the quinn `close` at `now`, then
  /// run the shared teardown tail. A subsequent `service` pass flushes the CONNECTION_CLOSE into
  /// the outbound queue.
  ///
  /// The SINGLE choke-point for every local-fatal teardown (binding rejection, framing error,
  /// outbound overflow, dead send stream, the auth-deadline reap). Issuing the quinn `close` is
  /// load-bearing: it arms the drain timer, so the service pump later drives the connection to
  /// `EndpointEvent::Drained` and the endpoint frees its slab slot and connection-cap slot — a
  /// teardown that only unbound routing would never drain (the peer's keep-alives hold it),
  /// pinning that state indefinitely.
  ///
  /// **Non-recursive: state mutation only — it does NOT call `service`** (it is reached from
  /// inside `service`'s own reap loop; the systematic pump-end `service` collects the close).
  /// Idempotent: a second call on an already-`Closed` entry is a no-op.
  pub(crate) fn close_local(&mut self, now: Instant, h: ConnectionHandle) {
    if let Some(e) = self.table.entry(h) {
      if e.phase.is_closed() {
        return;
      }
      e.conn.close(
        now,
        VarInt::from_u32(CONNECTION_CLOSE_CODE),
        bytes::Bytes::new(),
      );
    } else {
      return;
    }
    self.mark_closed_unbind_push(h);
  }

  /// Retire connection `h` from ROUTING (the coordinator's reap of a `lost` handle): the routing
  /// slot is cleared, but the quinn `Connection` is KEPT in the table so the service pump can
  /// drive it to `Drained` — only then is the endpoint slab freed and the entry removed.
  /// Removing the entry here would drop the `Connection` before it emits `Drained`, leaking the
  /// endpoint slab. Idempotent (an already-drained handle is simply absent).
  pub(crate) fn reap(&mut self, h: ConnectionHandle) {
    self.table.unbind(h);
  }

  /// Frame-encode `msg` and write it to connection `h`'s send stream. The framed bytes are
  /// appended to the BACK of the strict-FIFO `outbound` buffer, then a single front-draining
  /// flush pushes into the stream — appending first keeps on-wire frame order equal to call
  /// order under partial/blocked writes. The service pass that turns written stream bytes into
  /// datagrams is DEFERRED to the coordinator's pump-end `service`.
  ///
  /// Consensus frames are gated behind identity: a frame is staged ONLY on a `Validated`
  /// connection (the router cannot even resolve a non-validated one — this is defense in depth).
  /// An outbound buffer that would exceed [`MAX_CONN_OUT_BUF`] means the peer stopped consuming
  /// consensus traffic: the connection is closed (a redial recovers; consensus retransmission
  /// re-drives the dropped messages). An encoded message over [`MAX_FRAME_LEN`] is counted and
  /// dropped — the peer's decoder would fatally reject its declared length, so it could never
  /// deliver.
  pub(crate) fn write_framed(&mut self, now: Instant, h: ConnectionHandle, msg: &Message<I>) {
    if !self.is_validated(h) {
      return;
    }
    let mut payload = Vec::new();
    msg.encode(&mut payload);
    if payload.len() > MAX_FRAME_LEN {
      self.oversized_dropped = self.oversized_dropped.saturating_add(1);
      return;
    }
    let mut framed = Vec::new();
    encode_frame(&payload, &mut framed);
    {
      let Some(e) = self.table.entry(h) else {
        return;
      };
      if e.outbound.len().saturating_add(framed.len()) > MAX_CONN_OUT_BUF {
        self.close_local(now, h);
        return;
      }
      e.outbound.extend(framed);
    }
    self.flush_outbound(now, h);
    self.needs_service = true;
  }

  /// Run a deferred `service` pass iff the per-message write path marked one — the white-box
  /// stand-in for the coordinator's pump-end `service`, for tests that drive [`Self::write_framed`]
  /// directly and then inspect the outbound queue.
  #[cfg(test)]
  pub(crate) fn service_if_deferred(&mut self, now: Instant) {
    if self.needs_service {
      self.service(now);
    }
  }

  /// Adopt the peer-opened bidi stream and read it into the frame decoder, classifying any fatal
  /// close.
  ///
  /// Returns `true` ONLY when this call closed the connection INLINE with nothing queued to
  /// deliver (a peer RESET / already-closed stream, whose bytes quinn discarded; or a
  /// pre-authentication intake-bound violation, before any frame was admitted). A graceful FIN
  /// instead records [`ConnEntry::fin_received`] and returns `false`, so the coordinator's frame
  /// drain delivers the complete frames read BEFORE the FIN and only then closes
  /// (deliver-before-close). A non-fatal read (would-block, or progress made) also returns
  /// `false`.
  pub(crate) fn ingest_recv(&mut self, now: Instant, h: ConnectionHandle) -> bool {
    let Some(e) = self.table.entry(h) else {
      return false;
    };
    if e.phase.is_closed() || e.phase.is_handshaking() {
      return false;
    }
    // Adopt pending peer-opened bidi streams. The transport opens exactly ONE consensus stream
    // per side per connection, so the FIRST adoption is the consensus recv stream and any LATER
    // peer-opened stream is a protocol violation: the peer's send stream lives for the
    // connection's lifetime (its death is a connection-fatal on the peer's side), so a second
    // stream cannot be a legitimate reopen. Close rather than adopt a surface we never agreed to
    // read.
    let mut violation = false;
    while let Some(sid) = e.conn.streams().accept(Dir::Bi) {
      if e.recv.is_some() && e.recv != Some(sid) {
        violation = true;
        break;
      }
      e.recv = Some(sid);
    }
    if violation {
      self.close_local(now, h);
      return true;
    }
    let Some(sid) = e.recv else {
      return false;
    };

    // Read discipline by phase. POST-validation: at most one READ_BUDGET per pass (leftover
    // defers the connection to the next pump). While AUTHENTICATING: read EXACTLY the first
    // frame and not a byte beyond it — the only legitimate pre-validation frame is the peer's
    // preface, so the read is steered by the frame's own length prefix (header first, then
    // precisely the declared remainder). A pipelined consensus tail behind a short hello is
    // therefore NEVER read pre-auth (it stays backpressured in quinn, never dropped) until
    // validation, when `bind_validated` re-schedules the read; and a first frame DECLARING more
    // than one hello's worth (`MAX_HELLO_LEN`) can never be a hello, so the connection closes
    // the moment its header arrives. An unvalidated (but mTLS'd) peer therefore never has more
    // than one framed hello buffered on its behalf.
    let authenticating = e.phase.is_authenticating();
    // The first frame's header bytes seen in PRIOR passes (a hello can trickle in arbitrarily
    // small chunks); combined with this pass's scratch below to learn the declared length.
    let fed_prior = e.preauth_fed;
    let hdr_prior = e.preauth_hdr;

    /// How one read pass ended (the fatal variants dispose of the read bytes differently).
    enum RecvFault {
      /// Data accumulated or a non-fatal would-block: the stream stays live.
      Open,
      /// The peer GRACEFULLY finished its send half: the bytes before the FIN are complete
      /// frames, delivered before the close.
      Graceful,
      /// The peer ABANDONED its send half (RESET, or the stream was already finished/reset):
      /// the bytes are gone; close at once.
      Abandoned,
    }

    let mut scratch: Vec<u8> = Vec::new();
    let mut fault = RecvFault::Open;
    let mut leftover = false;
    let mut oversized_preface = false;
    {
      let mut recv = e.conn.recv_stream(sid);
      match recv.read(/* ordered = */ true) {
        Ok(mut chunks) => {
          loop {
            // How many more bytes this pass may read.
            let want = if authenticating {
              let have = fed_prior + scratch.len();
              if have < 4 {
                // Finish the first frame's 4-byte length prefix first.
                4 - have
              } else {
                // The header is complete across prior passes + this scratch: steer the read to
                // exactly the frame's end.
                let mut hdr = [0u8; 4];
                for (i, b) in hdr.iter_mut().enumerate() {
                  *b = if i < fed_prior {
                    hdr_prior[i]
                  } else {
                    scratch[i - fed_prior]
                  };
                }
                // Compare the DECLARED length before adding the prefix: `4 + declared` could wrap
                // usize on a 32-bit target for a hostile 0xFFFF_FFFF header, defeating the
                // close-at-header. `declared <= MAX_HELLO_LEN` keeps the sum trivially in range
                // and is exactly the one-framed-hello bound.
                let declared = u32::from_be_bytes(hdr) as usize;
                if declared > MAX_HELLO_LEN {
                  // Declares more than one hello's worth: it can never be a preface. Close now —
                  // the over-declared frame is never buffered at all.
                  oversized_preface = true;
                  break;
                }
                (4 + declared) - have // 0 = the frame is complete; stop at its boundary
              }
            } else if scratch.len() >= READ_BUDGET {
              // Budget spent with neither end-of-data nor a fault: the stream may still hold
              // readable bytes; reschedule below.
              leftover = true;
              break;
            } else {
              READ_BUDGET - scratch.len()
            };
            if want == 0 {
              break;
            }
            match chunks.next(want) {
              Ok(Some(chunk)) => scratch.extend_from_slice(&chunk.bytes),
              // FIN: the peer gracefully finished its send half. The data read before it is
              // complete; deliver it, THEN close (the coordinator's drain runs the close).
              Ok(None) => {
                fault = RecvFault::Graceful;
                break;
              }
              // Would-block: no data right now, stream still open.
              Err(quinn_proto::ReadError::Blocked) => break,
              // RESET: the peer abandoned its send half; whatever it sent before is gone.
              Err(quinn_proto::ReadError::Reset(_)) => {
                fault = RecvFault::Abandoned;
                break;
              }
            }
          }
          // `finalize` releases the read bytes from the flow-control window and queues the
          // resulting MAX_DATA / MAX_STREAM_DATA; the pump-end `service` (or the one below)
          // carries them to the wire.
          let _ = chunks.finalize();
        }
        // The stream was already finished/reset/stopped: nothing to read, nothing to recover.
        Err(_) => fault = RecvFault::Abandoned,
      }
    }

    if oversized_preface || matches!(fault, RecvFault::Abandoned) {
      // Oversized preface: the first frame can never authenticate — nothing legitimate to
      // deliver. Abandoned: the peer threw those bytes away (a RESET guarantees an empty read);
      // the decoder holds no undelivered frame from previous passes (each pump fully drains it).
      // Both close inline with nothing queued.
      self.close_local(now, h);
      return true;
    }

    let Some(e) = self.table.entry(h) else {
      return false;
    };
    e.decoder.push(&scratch);
    if authenticating {
      // Retain the first frame's header bytes for the next pass's steering (a no-op once all
      // four have been seen).
      for (i, b) in scratch.iter().enumerate() {
        let pos = fed_prior + i;
        if pos >= 4 {
          break;
        }
        e.preauth_hdr[pos] = *b;
      }
      e.preauth_fed = e.preauth_fed.saturating_add(scratch.len());
    }
    if matches!(fault, RecvFault::Graceful) {
      // Deliver-before-close: the coordinator's frame drain pops the complete frames, sees the
      // latched FIN, and closes the connection after the drain.
      e.fin_received = true;
      return false;
    }
    if leftover {
      // Bytes past this pass's budget remain readable: defer to the NEXT pump (not
      // `stream_ready`, which the current drain is consuming — that would drain the whole
      // window in one pump).
      self.deferred_ready.push_back(h);
    }
    false
  }

  /// Whether `h`'s peer gracefully finished its send half (the FIN latch the coordinator's drain
  /// consults after popping frames — deliver-before-close).
  pub(crate) fn fin_received(&mut self, h: ConnectionHandle) -> bool {
    self.table.entry(h).is_some_and(|e| e.fin_received)
  }

  /// Pop the next complete frame payload off `h`'s decoder. `Ok(None)` when no complete frame is
  /// buffered; `Err` once the decoder latched a framing violation (an over-cap declared length) —
  /// the coordinator closes the connection on it.
  pub(crate) fn next_frame(
    &mut self,
    h: ConnectionHandle,
  ) -> Result<Option<bytes::Bytes>, TransportError> {
    match self.table.entry(h) {
      Some(e) => e.decoder.poll(),
      None => Ok(None),
    }
  }

  /// Retry `h`'s staged sends after a `Writable`/`Available` signal reopened a window. Bytes
  /// reaching the stream are turned into datagrams by a service pass.
  pub(crate) fn flush_stream(&mut self, now: Instant, h: ConnectionHandle) {
    if self.flush_outbound(now, h) {
      self.service(now);
    }
  }

  /// Front-drain `h`'s staged outbound buffer into its send stream, returning whether bytes
  /// reached the stream (the caller turns progress into a service pass). A no-op when the buffer
  /// is empty. If staged bytes exist but no send stream is open (a fresh post-`Connected` write
  /// racing the preface step), a stream is opened first. quinn accepts a contiguous slice, so
  /// the buffer is made contiguous and written from the front: a partial write drops only the
  /// written prefix; `Blocked` leaves everything staged for the `Writable` retry; a terminal
  /// `Stopped`/`ClosedStream` means the peer stopped consuming consensus — the connection is
  /// closed (`close_local` services itself via the pump-end pass).
  fn flush_outbound(&mut self, now: Instant, h: ConnectionHandle) -> bool {
    {
      let Some(e) = self.table.entry(h) else {
        return false;
      };
      if e.outbound.is_empty() {
        return false;
      }
      if e.send.is_none() {
        match e.conn.streams().open(Dir::Bi) {
          Some(sid) => e.send = Some(sid),
          None => return false,
        }
      }
    }
    let Some(e) = self.table.entry(h) else {
      return false;
    };
    let Some(sid) = e.send else {
      return false;
    };
    let mut progressed = false;
    loop {
      // `make_contiguous` returns a front-anchored slice over the remaining buffer; the write
      // holds a disjoint re-borrow of `e.conn` (separate fields of the same entry).
      let bytes: &[u8] = e.outbound.make_contiguous();
      if bytes.is_empty() {
        break;
      }
      match e.conn.send_stream(sid).write(bytes) {
        Ok(0) => break,
        Ok(n) => {
          e.outbound.drain(..n);
          progressed = true;
        }
        // The flow-control window is exhausted; quinn registered the stream for the next
        // `Writable`. Leave the rest staged.
        Err(WriteError::Blocked) => break,
        // The send half is dead (peer STOP / already closed): the peer stopped consuming
        // consensus traffic — close the connection; a redial reopens the stream cleanly.
        Err(_) => {
          self.close_local(now, h);
          return false;
        }
      }
    }
    progressed
  }

  /// Test-only: stage raw pre-framed bytes directly onto `h`'s outbound buffer, bypassing the
  /// `Validated` gate — the white-box stand-in for a peer that PIPELINES consensus frames behind
  /// its hello in one flight (its own gate is its validation of US, which the test cannot see).
  #[cfg(test)]
  pub(crate) fn stage_outbound_for_test(&mut self, h: ConnectionHandle, bytes: &[u8]) {
    if let Some(e) = self.table.entry(h) {
      e.outbound.extend(bytes.iter().copied());
    }
  }

  // ── test observables ─────────────────────────────────────────────────────────

  /// The number of connections quinn's endpoint still tracks in its slab (the reconnect-churn
  /// regression asserts `Drained` connections actually free it).
  #[cfg(test)]
  pub(crate) fn endpoint_open_connections(&self) -> usize {
    self.endpoint.open_connections()
  }

  /// The number of live entries in the local connection table.
  #[cfg(test)]
  pub(crate) fn table_len(&self) -> usize {
    self.table.len()
  }

  /// The number of deferred endpoint events awaiting the next pass (the reconnect-vs-drain
  /// capacity regression observes the window where a queued `Drained` still holds a table slot).
  #[cfg(test)]
  pub(crate) fn pending_endpoint_events_len(&self) -> usize {
    self.pending_endpoint_events.len()
  }

  /// The number of service passes run so far (see the `services_run` field).
  #[cfg(test)]
  pub(crate) fn services_run(&self) -> u64 {
    self.services_run
  }

  /// The current effective connection cap (the live-membership floor regression watches it).
  #[cfg(test)]
  pub(crate) fn max_connections_for_test(&self) -> usize {
    self.max_connections
  }
}

#[cfg(test)]
mod tests;
