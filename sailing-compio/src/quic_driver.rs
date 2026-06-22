//! [`CompioQuicDriver`]: one task owning a [`QuicCoordinator`], the embedder's stores, and a UDP
//! socket, driving consensus over real datagrams.

use std::{collections::BTreeMap, net::SocketAddr, time::Duration};

use bytes::Bytes;
use compio::net::UdpSocket;
use sailing_proto::{
  ClusterId, Config, Instant, LogStore, Now, ProposeError, StableStore, StateMachine,
  quic::{QuicCoordinator, QuicOptions},
};

use crate::{
  BindError, DriverError, Monotonic, WallClock,
  clock::{Clock, jittered},
  config::DriverConfig,
  handle::{Command, Handle},
  shared::{InflightBudget, ParkedFailover, ParkedQuery, Pending, Routing},
};

/// IP-layer maximum UDP payload — the persistent receive buffer's size.
const RECV_BUF_LEN: usize = 65_507;
/// Backoff before retrying a failed `recv_from`, bounding the retry rate under a persistent
/// synchronously-resolving error so the thread always makes progress.
const RECV_ERROR_BACKOFF: Duration = Duration::from_millis(20);

/// The persistent datagram-receive task: owns a clone of the driver's socket (compio sockets
/// share one fd across clones) plus ONE receive buffer for its whole life, looping `recv_from`
/// and forwarding each datagram — copied exact-sized — into the bounded channel the run loop
/// selects on.
///
/// Keeping the read in its own task is what makes the run loop's recv arm a plain channel wait:
/// on a proactor, DROPPING a not-yet-finished op future (what a losing select arm does) submits
/// an asynchronous CANCEL and forfeits the op's buffer, so a loop that re-armed `recv_from` per
/// iteration would pay a cancel syscall plus a zeroed 64 KiB allocation on every
/// submit/timer/storage wake. Here the op is never dropped while the driver runs; each completed
/// read hands the buffer back in its `BufResult` and it is re-lent forever.
///
/// A receive error is transient for an unconnected UDP socket (anything lost under it is QUIC
/// loss recovery's to repair), so the loop keeps receiving after a paced backoff. The task exits
/// when the driver drops the channel receiver; the driver also OWNS the task's `JoinHandle`,
/// whose drop cancels the task on every run-loop exit path.
async fn recv_datagrams(socket: UdpSocket, inbound: lochan::mpsc::Sender<(Vec<u8>, SocketAddr)>) {
  let mut buf = vec![0u8; RECV_BUF_LEN];
  loop {
    let compio::buf::BufResult(res, returned) = socket.recv_from(buf).await;
    buf = returned;
    match res {
      Ok((n, from)) => {
        // Exact-sized copy so the long-lived receive buffer is immediately re-lent; a full
        // channel parks here, leaving NO receive in flight — arrivals then queue in (and
        // overflow) the kernel socket buffer, which is exactly UDP backpressure.
        if inbound.send((buf[..n].to_vec(), from)).await.is_err() {
          return; // the driver dropped its receiver: tear down
        }
      }
      Err(_) => {
        compio::time::sleep(RECV_ERROR_BACKOFF).await;
      }
    }
  }
}

/// Per-peer redial state: the next attempt instant and the current (pre-jitter) backoff.
struct Redial {
  at: std::time::Instant,
  backoff: Duration,
}

/// A consensus node over QUIC on compio: the driver owns the coordinator, the stores, and the
/// socket; [`Handle`]s own the conversation with it.
///
/// Construct AND run on the same thread (see the crate docs): the socket attaches to the
/// constructing thread's proactor.
pub struct CompioQuicDriver<I, F, L, S, W = Monotonic>
where
  I: sailing_proto::NodeId,
  F: StateMachine,
{
  coord: QuicCoordinator<I, F>,
  log: L,
  stable: S,
  socket: UdpSocket,
  clock: Clock<W>,
  /// Byte cap on the failover inherited-read limbo scan (see
  /// [`DriverConfig::max_failover_limbo_bytes`]).
  max_failover_limbo_bytes: usize,
  commands: flume::Receiver<Command<I, F>>,
  routing: Routing<I, F::Response, F>,
  storage_ready: flume::Receiver<()>,
  /// Keeps a `None`-seam storage channel parked forever (a sender-less receiver would resolve
  /// `Err` immediately and busy-loop the select arm).
  _storage_ready_keepalive: Option<flume::Sender<()>>,
  /// The configured peer book: every OTHER node's address, dialed and redialed as needed.
  peers: Vec<(I, SocketAddr)>,
  redial: BTreeMap<I, Redial>,
  cmd_budget: usize,
  recv_cap: usize,
  redial_base: Duration,
  redial_cap: Duration,
  /// Latched when every storage-ready sender has dropped: a disconnected flume receiver
  /// resolves `recv_async` immediately (and forever), so without the latch the dead channel
  /// would turn the storage arm into an always-ready select winner and the loop into a hot
  /// spin. The notifier is a wake-latency optimization, not a liveness dependency —
  /// `handle_storage` runs every iteration regardless — so the latch only downgrades storage
  /// completions to timer/I/O cadence.
  storage_closed: bool,
  /// Leadership as of the END of the last pass: the sweep backstop, DEFENSE-IN-DEPTH. The
  /// proto announces every leader-belief transition with `LeaderChanged` — including the
  /// to-`None` ones (check-quorum stepdown, campaign start, higher-term adoption, self-
  /// removal) — so the event-driven supersede covers every loss; this edge-detect stays as a
  /// second, event-independent witness that parked completions (and their budget) can never
  /// be stranded by an event path regression.
  was_leader: bool,
}

impl<I, F, L, S> CompioQuicDriver<I, F, L, S, Monotonic>
where
  I: sailing_proto::NodeId + Send,
  F: StateMachine,
  F::Command: sailing_proto::Data + Send,
  F::Snapshot: sailing_proto::Data,
  F::Response: Clone + Send,
  F::Error: core::error::Error,
  L: LogStore,
  S: StableStore<NodeId = I>,
{
  /// Bind with the default monotonic-only clock — the failover tier stays inert. For a failover
  /// deployment, use [`bind_with_wall_clock`](Self::bind_with_wall_clock) with a synchronized source.
  #[allow(clippy::too_many_arguments)]
  pub async fn bind(
    addr: SocketAddr,
    config: Config<I>,
    seed: u64,
    fsm: F,
    opts: QuicOptions,
    cluster: ClusterId,
    peers: Vec<(I, SocketAddr)>,
    log: L,
    stable: S,
    driver_cfg: DriverConfig,
  ) -> Result<(Self, Handle<I, F>), BindError> {
    Self::bind_with_wall_clock(
      addr, config, seed, fsm, opts, cluster, peers, log, stable, Monotonic, driver_cfg,
    )
    .await
  }
}

impl<I, F, L, S, W> CompioQuicDriver<I, F, L, S, W>
where
  I: sailing_proto::NodeId + Send,
  F: StateMachine,
  F::Command: sailing_proto::Data + Send,
  F::Snapshot: sailing_proto::Data,
  F::Response: Clone + Send,
  F::Error: core::error::Error,
  L: LogStore,
  S: StableStore<NodeId = I>,
  W: WallClock,
{
  /// Bind `addr` and build the driver plus its [`Handle`].
  ///
  /// `peers` is the static peer book (every other node's id + address): the driver dials each at
  /// startup and REDIALS (jittered exponential backoff) whenever a peer has no bound connection.
  /// `opts` must be a [`ClusterTls`](sailing_proto::quic::ClusterTls) bundle (the provided
  /// identity scheme requires mandatory mTLS); `seed` seeds the consensus endpoint's election
  /// jitter. Storage is the embedder's; a genuinely-async store wires
  /// [`DriverConfig::storage_ready`].
  #[allow(clippy::too_many_arguments)]
  pub async fn bind_with_wall_clock(
    addr: SocketAddr,
    config: Config<I>,
    seed: u64,
    fsm: F,
    opts: QuicOptions,
    cluster: ClusterId,
    peers: Vec<(I, SocketAddr)>,
    log: L,
    stable: S,
    wall: W,
    driver_cfg: DriverConfig,
  ) -> Result<(Self, Handle<I, F>), BindError> {
    // Reject an out-of-range programmatic `DriverConfig` UP FRONT (before the socket binds). The
    // serde/clap parse paths validate, but a programmatic config bypasses that; this keeps the
    // channel-sizing knobs under their ceilings before any channel is built — in particular the
    // eager-ring `recv_cap` (a `lochan::mpsc::bounded` ring is allocated in full at `cap` slots) under
    // `MAX_BOUNDED_QUEUE_DEPTH`, so an astronomical value cannot OOM at bind.
    driver_cfg.validate()?;
    // Validate + capture ε_unc (the sole copy of the wall-gate threshold) BEFORE the socket binds,
    // rejecting an invalid Config and the silent failover wedge (a failover tier with a non-supplying
    // source).
    let eps_unc_ns = crate::clock::validate_and_capture_eps::<I, W>(&config)?;
    let socket = UdpSocket::bind(addr).await?;
    let mut clock = Clock::new(eps_unc_ns, wall);
    let endpoint = sailing_proto::Endpoint::new(config, clock.now(), seed, fsm);
    let coord = QuicCoordinator::with_identity(endpoint, opts, None, cluster);

    // Unbounded: the submit BUDGET — not the channel — is the binding bound on in-flight
    // operations (see the memory model), and an unbounded queue keeps shutdown from ever blocking
    // on a full channel.
    let (cmd_tx, cmd_rx) = flume::unbounded();
    let (event_tx, event_rx) = flume::bounded(driver_cfg.events_cap);
    let budget = InflightBudget::new(driver_cfg.max_inflight, driver_cfg.max_pending_bytes);
    let handle = Handle::new(cmd_tx, event_rx, budget);

    let (storage_ready, keepalive) = match driver_cfg.storage_ready {
      Some(rx) => (rx, None),
      None => {
        // No async store: hold the sender so the receiver parks forever instead of erroring.
        let (tx, rx) = flume::bounded(1);
        (rx, Some(tx))
      }
    };

    Ok((
      Self {
        coord,
        log,
        stable,
        socket,
        clock,
        commands: cmd_rx,
        routing: Routing::new(event_tx),
        storage_ready,
        _storage_ready_keepalive: keepalive,
        peers,
        redial: BTreeMap::new(),
        // Clamped to at least one: the iter-top drain is the only flood-independent command
        // path, and shutdown's stoppable-under-load guarantee rides on it.
        cmd_budget: driver_cfg.cmd_budget.max(1),
        recv_cap: driver_cfg.recv_cap,
        max_failover_limbo_bytes: driver_cfg.max_failover_limbo_bytes,
        redial_base: driver_cfg.redial_base,
        redial_cap: driver_cfg.redial_cap,
        storage_closed: false,
        was_leader: false,
      },
      handle,
    ))
  }

  /// The count of times this node released its post-election commit-wait EARLY via the precise
  /// wall-clock anchor (vs the conservative monotonic deadline) — the observable witness that the
  /// LeaseGuard failover tier is live end-to-end. `0` outside the failover tier or under a
  /// monotonic-only clock.
  #[must_use]
  pub fn precise_releases(&self) -> u64 {
    self.coord.endpoint().precise_releases()
  }

  /// The count of times an inherited walled-lease floor could not be proven (no synchronized wall, or
  /// no bounded clock uncertainty) and the commit-wait was held conservatively. A nonzero value in a
  /// configured-failover deployment signals a node OUTSIDE the synchronized-clock contract — the
  /// intended backstop, not a wiring fault.
  #[must_use]
  pub fn unprovable_floor_holds(&self) -> u64 {
    self.coord.endpoint().unprovable_floor_holds()
  }

  /// Drive consensus until shutdown (or until every `Handle` clone has dropped AND the buffered
  /// commands are drained — a driver nobody can talk to has no reason to run).
  pub async fn run(mut self) {
    use futures_util::{FutureExt, select_biased};

    let (recv_tx, mut recv_rx) = lochan::mpsc::bounded(self.recv_cap);
    // The recv task's JoinHandle is OWNED by this scope — never detached — so every exit path
    // drops it, cancelling the task with its in-flight recv and its socket clone. The cancel is
    // mark-and-schedule, not synchronous teardown: the orderly exits below follow it with the
    // socket close().await as the true fd-release barrier.
    let recv_task = compio::runtime::spawn(recv_datagrams(self.socket.clone(), recv_tx));

    let now = self.clock.now();
    self.reconcile_peer_links(now.mono());
    let mut poisoned = self.pump().await;

    let mut shutdown_ack: Option<futures_channel::oneshot::Sender<()>> = None;
    while !poisoned {
      let now = self.clock.now();

      // (1) Fairness: drain up to the command budget before the biased I/O select, so a
      // continuous recv backlog cannot starve Shutdown/Submit.
      let mut exit = false;
      for _ in 0..self.cmd_budget {
        match self.commands.try_recv() {
          Ok(cmd) => {
            if self.handle_command(now, cmd, &mut shutdown_ack) {
              exit = true;
              break;
            }
          }
          Err(e) => {
            // Disconnected = every Handle clone dropped AND the buffer drained: the command
            // stream has ENDED for good — exit (a continuously-readable socket would otherwise
            // keep the task and the socket alive forever). Empty just falls through to the select.
            if matches!(e, flume::TryRecvError::Disconnected) {
              exit = true;
            }
            break;
          }
        }
      }
      if exit {
        break;
      }

      // (2) Fairness: fire an already-due deadline before the select, so a recv flood cannot
      // suppress heartbeats/elections. The coordinator's poll_timeout already folds the
      // consensus deadline, quinn's timers, and the auth deadline into ONE crate instant.
      if self
        .coord
        .poll_timeout()
        .is_some_and(|d| d <= self.clock.mono())
      {
        self
          .coord
          .handle_timeout(now, &mut self.log, &mut self.stable);
      }
      // Redial any configured peer with no bound connection BEFORE the pump, so a fresh dial's
      // handshake Initial transmits this iteration rather than after the next wake.
      self.reconcile_peer_links(now.mono());
      if self.pump().await {
        break;
      }

      // Recomputed AFTER the iter-top fire so it reflects the NEXT deadline.
      let deadline = self
        .coord
        .poll_timeout()
        .map(|d| self.clock.to_std(d))
        .unwrap_or_else(|| std::time::Instant::now() + Duration::from_secs(3600));

      // The select arms are plain channel/timer waits — the socket I/O lives in the recv task
      // and the pump — so a losing arm never cancels an in-flight socket op. The pinned futures
      // are confined to this scope; each arm only writes a captured local.
      let (inbound, fire_timeout, command, ended) = {
        // `recv_rx` is a run-loop local, so its lochan `recv` (`&mut self`) is pre-pinnable.
        let recv_fut = recv_rx.recv();
        let timer_fut = compio::time::sleep_until(deadline).fuse();
        // Once every notifier sender has dropped, the channel is dead (recv resolves Err
        // immediately, forever) and would win the select every iteration — when latched, this
        // arm becomes PENDING instead, parking it for good. (An always-ready placeholder like a
        // resolved Option future would itself re-create the hot spin the latch closes.)
        let storage_closed = self.storage_closed;
        let storage_rx = &self.storage_ready;
        let storage_fut = async move {
          if storage_closed {
            std::future::pending::<Result<(), flume::RecvError>>().await
          } else {
            storage_rx.recv_async().await
          }
        }
        .fuse();
        let cmd_fut = self.commands.recv_async().fuse();
        futures_util::pin_mut!(recv_fut, timer_fut, storage_fut, cmd_fut);

        let mut inbound: Option<(Vec<u8>, SocketAddr)> = None;
        let mut fire_timeout = false;
        let mut command: Option<Command<I, F>> = None;
        let mut ended = false;
        let mut storage_disconnected = false;

        select_biased! {
          // `None` (a closed channel) is unreachable while this scope holds recv_task: the task
          // only exits when the receiver it sends to drops.
          got = recv_fut => {
            if let Some(datagram) = got { inbound = Some(datagram); }
          }
          _ = timer_fut => { fire_timeout = true; }
          // flume `recv_async` yields `Ok` while any sender lives and `Err` once every `Handle`
          // clone has dropped (the buffer already drained) — the end-of-stream signal.
          cmd = cmd_fut => {
            match cmd { Ok(c) => command = Some(c), Err(_) => ended = true }
          }
          got = storage_fut => {
            if got.is_err() { storage_disconnected = true; }
          }
        }
        if storage_disconnected {
          self.storage_closed = true;
        }
        (inbound, fire_timeout, command, ended)
      };
      // Coalesce any burst of storage signals: handle_storage below drains ALL completions.
      while self.storage_ready.try_recv().is_ok() {}
      if ended {
        break;
      }

      let now = self.clock.now();
      if let Some((datagram, from)) = inbound {
        self
          .coord
          .handle_udp(now, from, None, &datagram, &mut self.log, &mut self.stable);
      }
      if fire_timeout {
        self
          .coord
          .handle_timeout(now, &mut self.log, &mut self.stable);
      }
      // ALWAYS drain storage completions: synchronous stores complete inline with the calls
      // above, async ones signalled the arm we just coalesced.
      self
        .coord
        .handle_storage(now, &mut self.log, &mut self.stable);
      if let Some(cmd) = command
        && self.handle_command(now, cmd, &mut shutdown_ack)
      {
        break;
      }
      poisoned = self.pump().await;
    }

    // Teardown. Fail everything parked (each entry's reservation releases on drop), cancel the
    // recv task, then make the command queue airtight: close-then-drain refuses a racing
    // try_send WITH its command (the handle's own rollback runs) while everything already
    // buffered is drained and dropped here — no command, queued or in flight, survives the ack.
    // Classify the fail-stop FIRST: an exit that raced a poison (a Shutdown command winning
    // the select after the poisoning storage drain) must still fail parked work with the typed
    // verdict; the ShuttingDown sweep below is then a no-op on the emptied maps.
    if self.coord.endpoint().is_poisoned() {
      self.routing.fail_all(&DriverError::Poisoned);
    }
    self.routing.fail_all(&DriverError::ShuttingDown);
    drop(recv_task);
    drop(recv_rx);
    // Drain everything already buffered, then DROP the receiver: a racing `try_send` then sees a
    // disconnected channel WITH its command (the handle's own rollback runs) — no command, queued
    // or in flight, survives the ack.
    while let Ok(cmd) = self.commands.try_recv() {
      drop(cmd);
    }
    drop(self.commands);
    // The fd-release barrier: close() parks until every other reference to the socket's fd —
    // the recv task's clone and its cancelled-but-unprocessed op — has dropped, then closes the
    // fd with a real close op. Once this await returns the bound address is free, which is what
    // makes the ack an immediate-rebind contract.
    let _ = self.socket.close().await;
    if let Some(ack) = shutdown_ack {
      let _ = ack.send(());
    }
  }

  /// Handle one command; returns `true` when the loop should exit (a `Shutdown`).
  fn handle_command(
    &mut self,
    now: Now,
    cmd: Command<I, F>,
    shutdown_ack: &mut Option<futures_channel::oneshot::Sender<()>>,
  ) -> bool {
    match cmd {
      Command::Submit {
        cmd,
        reply,
        reservation,
      } => {
        match self
          .coord
          .submit_propose(now, &mut self.log, &self.stable, &cmd)
        {
          Ok(index) => {
            self.routing.pending.insert(
              index,
              Pending::Submit {
                reply,
                _reservation: reservation,
              },
            );
          }
          Err(e) => {
            let _ = reply.send(Err(map_propose_err(e)));
          }
        }
      }
      Command::Conf {
        cc,
        reply,
        reservation,
      } => {
        match self
          .coord
          .propose_conf_change(now, &mut self.log, &self.stable, cc)
        {
          Ok(index) => {
            self.routing.pending.insert(
              index,
              Pending::Conf {
                reply,
                _reservation: reservation,
              },
            );
          }
          Err(e) => {
            let _ = reply.send(Err(map_propose_err(e)));
          }
        }
      }
      Command::ConfV2 {
        cc,
        reply,
        reservation,
      } => {
        match self
          .coord
          .propose_conf_change_v2(now, &mut self.log, &self.stable, cc)
        {
          Ok(index) => {
            self.routing.pending.insert(
              index,
              Pending::Conf {
                reply,
                _reservation: reservation,
              },
            );
          }
          Err(e) => {
            let _ = reply.send(Err(map_propose_err(e)));
          }
        }
      }
      Command::Query {
        complete,
        reservation,
      } => {
        let ctx = self.routing.mint_query_ctx();
        match self.coord.read_index(
          now,
          &self.log,
          &self.stable,
          Bytes::copy_from_slice(&ctx.to_be_bytes()),
        ) {
          Ok(()) => {
            self.routing.queries.insert(
              ctx,
              ParkedQuery {
                ready_at: None,
                complete,
                _reservation: reservation,
              },
            );
          }
          Err(e) => {
            complete(Err(map_read_err(e)));
          }
        }
      }
      Command::FailoverWindow {
        complete,
        reservation,
      } => {
        self.routing.failovers.push(ParkedFailover {
          complete,
          _reservation: reservation,
        });
      }
      Command::Transfer {
        to,
        reply,
        reservation,
      } => {
        let r = self
          .coord
          .transfer_leader(now, &self.log, &self.stable, to)
          .map_err(map_transfer_err);
        let _ = reply.send(r);
        // A transfer parks nothing (the verdict is immediate); release with the reply.
        drop(reservation);
      }
      Command::Shutdown { ack } => {
        *shutdown_ack = Some(ack);
        return true;
      }
    }
    false
  }

  /// Dial every configured peer that has no bound connection and whose backoff has elapsed; a
  /// peer that re-binds resets its backoff.
  fn reconcile_peer_links(&mut self, now: Instant) {
    let std_now = std::time::Instant::now();
    for (peer, addr) in self.peers.clone() {
      if self.coord.has_bound_conn(&peer) {
        self.redial.remove(&peer);
        continue;
      }
      let due = self.redial.get(&peer).is_none_or(|r| std_now >= r.at);
      if !due {
        continue;
      }
      // A refused dial (cap, config) is just retried on the schedule; the typed error matters
      // to interactive callers, not to the background reconciler.
      let _ = self.coord.connect(now, addr, peer.cheap_clone());
      let backoff = self
        .redial
        .get(&peer)
        .map(|r| (r.backoff * 2).min(self.redial_cap))
        .unwrap_or(self.redial_base);
      self.redial.insert(
        peer,
        Redial {
          at: std_now + jittered(backoff),
          backoff,
        },
      );
    }
  }

  /// Serve (or fall back) the parked failover inherited-read queries, re-deriving the serve window from
  /// `now` each pass: `None` (commit-wait lifted, off-tier, inherited lease expired, poisoned) falls
  /// every query back to a normal read (`Ok(None)`); a live window whose committed prefix has applied
  /// serves the whole batch against the FSM with the limbo region; otherwise the queries stay parked for
  /// next pass. Returns `true` on a FATAL limbo storage fault (the caller fails the parked work
  /// `Poisoned` and stops the driver — a corrupt committed-range log is unrecoverable).
  fn run_failover_serve(&mut self) -> bool {
    if self.routing.failovers.is_empty() {
      return false;
    }
    // A FRESH wall: the loop-top `now` is stale by here (it predates this pass's pump and callbacks)
    // and the proto lease gate is strict at the boundary.
    let now = self.clock.now();
    match self.coord.endpoint().failover_read_window(now) {
      None => {
        for p in std::mem::take(&mut self.routing.failovers) {
          (p.complete)(Ok(None));
        }
      }
      Some(window) if self.routing.applied >= window.index() => {
        match crate::shared::read_limbo(&self.log, &window, self.max_failover_limbo_bytes as u64) {
          Ok(Some(limbo)) => {
            let parked = std::mem::take(&mut self.routing.failovers);
            let fsm = self.coord.state_machine();
            // Re-check the lease with a FRESH wall before EACH completion — the scan and each closure
            // burn wall time, so the window can expire mid-batch.
            crate::shared::serve_failover_batch(parked, fsm, &limbo, window, || {
              self
                .coord
                .endpoint()
                .failover_read_window(self.clock.now())
                .is_some()
            });
          }
          // A SAFE fallback (truncated / over-budget / incomplete / index-ceiling limbo): fall the
          // batch back to a normal read.
          Ok(None) => {
            for p in std::mem::take(&mut self.routing.failovers) {
              (p.complete)(Ok(None));
            }
          }
          // A FATAL limbo storage fault (corrupt/unreadable committed-range log): leave the reads
          // parked for the pump to fail `Poisoned` and stop the driver.
          Err(_) => return true,
        }
      }
      // The window is armed but the committed prefix has not applied yet — keep parked, re-check
      // next pass.
      Some(_) => {}
    }
    false
  }

  /// Drain the coordinator's outputs: transmits to the socket, events into the routing (and any
  /// queries whose read index the apply watermark now covers, run against the state machine).
  async fn pump(&mut self) -> bool {
    // Drain the coordinator's queued datagrams FIRST. These awaited sends precede the failover serve
    // below BY DESIGN — parity with the normal-query serve: user-closure serves follow the consensus
    // output, never the reverse, so unbounded user read closures cannot stall outbound consensus
    // traffic. The drain is a finite fire-and-forget UDP batch and the inherited-lease window carries a
    // 2·ε_unc margin, so a bounded send phase cannot expire it; only a pathological send stall could,
    // which equally stalls the normal-read fallback — the failover path is never worse off than the
    // read it falls back to.
    while let Some((dest, bytes)) = self.coord.poll_transmit() {
      // A send error is transient for UDP (the peer redials / QUIC retransmits); dropping the
      // datagram is the same observable as the network dropping it.
      let _ = self.socket.send_to(bytes, dest).await;
    }
    let mut run_queries = false;
    while let Some(ev) = self.coord.poll_event() {
      run_queries |= self.routing.route_event(ev);
    }
    // Leadership-loss backstop, BEFORE the serve: defense-in-depth for a loss NOT carried by a routed
    // `LeaderChanged` (which `route_event` already swept). Sweeping ahead of the serve voids parked
    // inherited-reads `Err(Superseded)` — the serve's None arm can never drain them `Ok(None)` first.
    // Normally a no-op (the routed event already emptied the map).
    let is_leader = self.coord.role().is_leader();
    if self.was_leader && !is_leader {
      self.routing.fail_all(&DriverError::Superseded);
    }
    self.was_leader = is_leader;
    // Serve parked failover inherited-reads HERE: after the `route_event` drain (it advanced
    // `routing.applied` and swept on a routed `LeaderChanged`) AND the leadership backstop above — so the
    // serve runs only on a still-live tier — and BEFORE the UNBOUNDED `take_runnable_queries` user
    // closures, so the strict-wall serve cannot expire behind them. Skip on a poisoned node so the
    // `fail_all(Poisoned)` sweep below owns the parked reads.
    if !self.coord.endpoint().is_poisoned() && self.run_failover_serve() {
      // A FATAL limbo storage fault: a corrupt/unreadable committed-range log is unrecoverable, not a
      // safe normal-read fallback — fail all parked work `Poisoned` and stop the driver.
      self.routing.fail_all(&DriverError::Poisoned);
      return true;
    }
    if run_queries {
      for q in self.routing.take_runnable_queries() {
        (q.complete)(Ok(self.coord.state_machine()));
      }
    }
    // The fail-stop check: a poisoned endpoint suppresses poll_event and poll_timeout by
    // design, so anything parked would otherwise wait forever holding its reservation. Fail it
    // all with the typed verdict and tell the run loop to exit — the NODE is dead; an operator
    // restart (or re-provisioning) is the only recovery, and keeping the socket bound would
    // only mislead peers.
    if self.coord.endpoint().is_poisoned() {
      self.routing.fail_all(&DriverError::Poisoned);
      return true;
    }
    false
  }
}

/// Map the proto's propose-time error to the driver's typed surface.
fn map_propose_err<I: core::fmt::Debug>(e: ProposeError<I>) -> DriverError<I> {
  match e {
    ProposeError::NotLeader { leader } => DriverError::NotLeader { leader },
    ProposeError::Poisoned => DriverError::Poisoned,
    other => DriverError::Rejected {
      reason: format!("{other:?}"),
    },
  }
}

/// Map the proto's transfer-time error, preserving the redirect hint.
fn map_transfer_err<I: core::fmt::Debug>(e: sailing_proto::TransferError<I>) -> DriverError<I> {
  match e {
    sailing_proto::TransferError::NotLeader { leader } => DriverError::NotLeader { leader },
    sailing_proto::TransferError::Poisoned => DriverError::Poisoned,
    other => DriverError::Rejected {
      reason: format!("{other:?}"),
    },
  }
}

/// Map the proto's read-index error: a missing leader is the same redirect signal as a propose
/// rejection (retry once a leader is known), the rest carry their reason.
fn map_read_err<I>(e: sailing_proto::ReadIndexError) -> DriverError<I> {
  match e {
    sailing_proto::ReadIndexError::NoLeader => DriverError::NotLeader { leader: None },
    sailing_proto::ReadIndexError::Poisoned => DriverError::Poisoned,
    other => DriverError::Rejected {
      reason: other.to_string(),
    },
  }
}
