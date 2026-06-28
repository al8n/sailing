//! [`ReactorQuicDriver`]: one `Send` task owning a [`QuicCoordinator`], the embedder's stores, and a
//! UDP socket, driving consensus over real datagrams on any [`agnostic::Runtime`].
//!
//! The readiness sibling of `CompioQuicDriver`: the consensus logic is byte-for-byte identical, only
//! the I/O model differs. The single datagram-receive task lives inline here (QUIC is connectionless
//! at the driver edge — there is no per-conn bridge); the channels and the receive plumbing are the
//! `Send` work-stealing set (`flume`), each spawned task is owned as an [`AbortOnDrop`] handle, and the
//! socket is shared by `Arc` (a readiness UDP socket is borrowed `&self` for both halves, never split).

use std::{collections::BTreeMap, net::SocketAddr, sync::Arc, time::Duration};

use agnostic::{
  Runtime,
  net::{Net, UdpSocket},
};
use bytes::Bytes;
use sailing_proto::{
  ClusterId, Config, Instant, LogStore, Now, StableStore, StateMachine,
  quic::{QuicCoordinator, QuicOptions},
};

use sailing_driver::{
  Command, Handle, Node, Status, jittered,
  shared::{InflightBudget, ParkedFailover, ParkedQuery, Pending, Routing},
  validate_and_capture_eps,
};

use crate::{BindError, Clock, DriverConfig, DriverError, Monotonic, WallClock, task::AbortOnDrop};

use super::{map_propose_err, map_read_err, map_transfer_err};

/// IP-layer maximum UDP payload — the persistent receive buffer's size.
const RECV_BUF_LEN: usize = 65_507;
/// Backoff before retrying a failed `recv_from`, bounding the retry rate under a persistent
/// synchronously-resolving error so the task always makes progress.
const RECV_ERROR_BACKOFF: Duration = Duration::from_millis(20);
/// Backstop wake cadence while configured peers exist: a periodic reconciler wake so a redial that
/// armed no consensus timer still fires on schedule. A bounded 1 Hz idle cap, NOT load-bearing for
/// safety — quinn's connection timers ride `poll_timeout()`, so this only paces the link reconciler
/// on an otherwise-idle node.
const HOUSEKEEPING_INTERVAL: Duration = Duration::from_secs(1);
/// The per-iteration bound on each channel drain at the loop top (datagrams, storage coalesce): the
/// signal-free arms make guaranteed progress per pass INDEPENDENT of the biased select below, so a
/// perpetually-ready arm (a flooding peer) cannot starve the arms after it.
const IO_BUDGET: usize = 256;

/// The single datagram-receive task: owns a clone of the driver's `Arc` socket plus ONE receive
/// buffer for its whole life, looping `recv_from` and forwarding each datagram — copied exact-sized —
/// into the bounded channel the run loop selects on.
///
/// Keeping the read in its own task is what makes the run loop's recv arm a plain channel wait. On a
/// readiness socket the borrowed-buffer read is cancel-safe to drop, but the dedicated task still
/// keeps the buffer resident and re-lent for the driver's life rather than re-allocating one per
/// select wake, and the `Arc` clone is the second owner that keeps the fd alive until BOTH this task
/// and the run loop drop it.
///
/// A receive error is transient for an unconnected UDP socket (anything lost under it is QUIC loss
/// recovery's to repair), so the loop keeps receiving after a paced backoff. The task returns on the
/// teardown shutdown signal (or when the channel receiver drops), releasing its socket clone; the
/// driver's teardown block awaits that join to make the final socket drop the fd-release barrier (see
/// `run`). The [`AbortOnDrop`] handle the driver also owns is the panic-path abort backstop.
async fn recv_datagrams<R: Runtime>(
  socket: Arc<<R::Net as Net>::UdpSocket>,
  inbound: flume::Sender<(Vec<u8>, SocketAddr)>,
  mut shutdown: futures_channel::oneshot::Receiver<()>,
) {
  let mut buf = vec![0u8; RECV_BUF_LEN];
  loop {
    // Race the read against the shutdown signal in an inner scope, so the read future — which borrows
    // `buf` — is dropped before the chunk is copied out below. A teardown signal (fired, or the sender
    // dropped) returns the task, releasing its `Arc` socket clone; the driver awaits that return so the
    // final socket drop is the fd-release barrier (see the teardown block in `run`).
    let received = {
      let recv = socket.recv_from(&mut buf);
      futures_util::pin_mut!(recv);
      match futures_util::future::select(recv, &mut shutdown).await {
        futures_util::future::Either::Left((result, _)) => result,
        futures_util::future::Either::Right(_) => return,
      }
    };
    match received {
      Ok((n, from)) => {
        // Exact-sized copy so the long-lived receive buffer is immediately re-lent; a full channel
        // parks here, leaving NO receive in flight — arrivals then queue in (and overflow) the
        // kernel socket buffer, which is exactly UDP backpressure.
        if inbound.send_async((buf[..n].to_vec(), from)).await.is_err() {
          return; // the driver dropped its receiver: tear down
        }
      }
      Err(_) => {
        R::sleep(RECV_ERROR_BACKOFF).await;
      }
    }
  }
}

/// Per-peer redial state: the next attempt instant and the current (pre-jitter) backoff.
struct Redial {
  at: std::time::Instant,
  backoff: Duration,
}

/// A consensus node over QUIC on a readiness runtime. `R` is the [`agnostic::Runtime`]; the driver
/// owns the coordinator, the stores, and the socket, and [`Handle`]s own the conversation with it.
///
/// The `run()` future is `Send` (given `Send` state-machine/storage types), so it rides a
/// work-stealing multi-thread runtime; a consensus group stays serial because it is ONE task, not by
/// thread-pinning. The channels are therefore `flume`, never compio's `lochan`, and the socket is
/// shared by `Arc` rather than a compio fd-sharing clone.
pub struct ReactorQuicDriver<R, I, F, L, S, W = Monotonic>
where
  R: Runtime,
  I: sailing_proto::NodeId,
  F: StateMachine,
{
  coord: QuicCoordinator<I, F>,
  log: L,
  stable: S,
  socket: Arc<<R::Net as Net>::UdpSocket>,
  clock: Clock<W>,
  /// Byte cap on the failover inherited-read limbo scan (see
  /// [`DriverConfig::max_failover_limbo_bytes`]).
  max_failover_limbo_bytes: usize,
  commands: flume::Receiver<Command<I, F>>,
  routing: Routing<I, F::Response, F>,
  storage_ready: flume::Receiver<()>,
  /// Keeps a `None`-seam storage channel parked forever (a sender-less receiver would resolve `Err`
  /// immediately and busy-loop the select arm).
  _storage_ready_keepalive: Option<flume::Sender<()>>,
  /// The configured peer book: every OTHER node's address, dialed and redialed as needed.
  peers: Vec<Node<I, SocketAddr>>,
  redial: BTreeMap<I, Redial>,
  cmd_budget: usize,
  recv_cap: usize,
  redial_base: Duration,
  redial_cap: Duration,
  /// Latched when every storage-ready sender has dropped: a disconnected flume receiver resolves
  /// `recv_async` immediately (and forever), so without the latch the dead channel would turn the
  /// storage arm into an always-ready select winner and the loop into a hot spin. The notifier is a
  /// wake-latency optimization, not a liveness dependency — `handle_storage` runs every iteration
  /// regardless — so the latch only downgrades storage completions to timer/I/O cadence.
  storage_closed: bool,
  /// Leadership as of the END of the last pass: the sweep backstop, DEFENSE-IN-DEPTH. The proto
  /// announces every leader-belief transition with `LeaderChanged` — including the to-`None` ones —
  /// so the event-driven supersede covers every loss; this edge-detect stays as a second,
  /// event-independent witness that parked completions can never be stranded by an event-path
  /// regression.
  was_leader: bool,
  /// The teardown-completion signal: fired (or dropped) AFTER the socket's fd-release barrier on
  /// every run-loop exit, fanning to every `Handle` via the shared receiver so each
  /// `shutdown().await` resolves only once the bound address is rebindable. `Option` so the fire is
  /// an explicit, ordered `take().send(())` right after the socket drop — never an implicit field
  /// drop whose ordering against the socket release is not guaranteed.
  teardown_tx: Option<futures_channel::oneshot::Sender<()>>,
}

impl<R, I, F, L, S> ReactorQuicDriver<R, I, F, L, S, Monotonic>
where
  R: Runtime,
  I: sailing_proto::NodeId + Send,
  F: StateMachine + Send,
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
    peers: Vec<Node<I, SocketAddr>>,
    log: L,
    stable: S,
    driver_cfg: DriverConfig,
  ) -> Result<(Self, Handle<I, F>), BindError> {
    Self::bind_with_wall_clock(
      addr, config, seed, fsm, opts, cluster, peers, log, stable, Monotonic, driver_cfg,
    )
    .await
  }

  /// Restart from durable storage with the default monotonic-only clock. Like [`bind`](Self::bind)
  /// but reconciles the durable stores instead of booting a fresh endpoint — see
  /// [`bind_restart_with_wall_clock`](Self::bind_restart_with_wall_clock).
  #[allow(clippy::too_many_arguments)]
  pub async fn bind_restart(
    addr: SocketAddr,
    config: Config<I>,
    seed: u64,
    fsm: F,
    boot_epoch: u64,
    opts: QuicOptions,
    cluster: ClusterId,
    peers: Vec<Node<I, SocketAddr>>,
    log: L,
    stable: S,
    driver_cfg: DriverConfig,
  ) -> Result<(Self, Handle<I, F>), BindError> {
    Self::bind_restart_with_wall_clock(
      addr, config, seed, fsm, boot_epoch, opts, cluster, peers, log, stable, Monotonic, driver_cfg,
    )
    .await
  }

  /// One-time MIGRATION restart from a pre-format store with the default monotonic-only clock — see
  /// [`bind_restart_migrating_with_wall_clock`](Self::bind_restart_migrating_with_wall_clock).
  #[allow(clippy::too_many_arguments)]
  pub async fn bind_restart_migrating(
    addr: SocketAddr,
    config: Config<I>,
    seed: u64,
    fsm: F,
    boot_epoch: u64,
    assume_prior_lease_support: Option<Duration>,
    opts: QuicOptions,
    cluster: ClusterId,
    peers: Vec<Node<I, SocketAddr>>,
    log: L,
    stable: S,
    driver_cfg: DriverConfig,
  ) -> Result<(Self, Handle<I, F>), BindError> {
    Self::bind_restart_migrating_with_wall_clock(
      addr,
      config,
      seed,
      fsm,
      boot_epoch,
      assume_prior_lease_support,
      opts,
      cluster,
      peers,
      log,
      stable,
      Monotonic,
      driver_cfg,
    )
    .await
  }
}

impl<R, I, F, L, S, W> ReactorQuicDriver<R, I, F, L, S, W>
where
  R: Runtime,
  I: sailing_proto::NodeId + Send,
  F: StateMachine + Send,
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
  /// `opts` must be a [`ClusterTls`](sailing_proto::quic::ClusterTls) bundle (the provided identity
  /// scheme requires mandatory mTLS); `seed` seeds the consensus endpoint's election jitter. Storage
  /// is the embedder's; a genuinely-async store wires [`DriverConfig::storage_ready`].
  #[allow(clippy::too_many_arguments)]
  pub async fn bind_with_wall_clock(
    addr: SocketAddr,
    config: Config<I>,
    seed: u64,
    fsm: F,
    opts: QuicOptions,
    cluster: ClusterId,
    peers: Vec<Node<I, SocketAddr>>,
    log: L,
    stable: S,
    wall: W,
    driver_cfg: DriverConfig,
  ) -> Result<(Self, Handle<I, F>), BindError> {
    // Reject an out-of-range programmatic `DriverConfig` UP FRONT (before the socket binds). The
    // serde/clap parse paths validate, but a programmatic config bypasses that; this keeps the
    // channel-sizing knobs under their ceilings before any channel is built — in particular the
    // bounded `recv_cap` ring under `MAX_BOUNDED_QUEUE_DEPTH`.
    driver_cfg.validate()?;
    // Validate + capture ε_unc (the sole copy of the wall-gate threshold) BEFORE the socket binds,
    // rejecting an invalid Config and the silent failover wedge (a failover tier with a non-supplying
    // source).
    let eps_unc_ns = validate_and_capture_eps::<I, W>(&config)?;
    let socket = Arc::new(<R::Net as Net>::UdpSocket::bind(addr).await?);
    let mut clock = Clock::new(eps_unc_ns, wall);
    let endpoint = sailing_proto::Endpoint::new(config, clock.now(), seed, fsm);
    let coord = QuicCoordinator::with_identity(endpoint, opts, None, cluster);
    Ok(Self::from_parts(
      coord, log, stable, socket, clock, peers, driver_cfg,
    ))
  }

  /// Restart the socket and driver from DURABLE storage after a crash, plus its [`Handle`].
  ///
  /// The crash-recovery sibling of [`bind_with_wall_clock`](Self::bind_with_wall_clock): instead of a
  /// fresh endpoint it builds the coordinator over
  /// [`Endpoint::restart`](sailing_proto::Endpoint::restart), which RECONCILES the durable
  /// [`LogStore`]/[`StableStore`] — recovering the persisted term/vote/commit, replaying the committed
  /// tail, and re-arming the lease/vote fences — so a restarting node never double-votes by booting at
  /// term 0. `boot_epoch` MUST be strictly greater than every prior incarnation's and persisted
  /// durably BEFORE this call (a fresh node uses 0, so the first restart passes at least 1).
  #[allow(clippy::too_many_arguments)]
  pub async fn bind_restart_with_wall_clock(
    addr: SocketAddr,
    config: Config<I>,
    seed: u64,
    fsm: F,
    boot_epoch: u64,
    opts: QuicOptions,
    cluster: ClusterId,
    peers: Vec<Node<I, SocketAddr>>,
    mut log: L,
    mut stable: S,
    wall: W,
    driver_cfg: DriverConfig,
  ) -> Result<(Self, Handle<I, F>), BindError> {
    driver_cfg.validate()?;
    let eps_unc_ns = validate_and_capture_eps::<I, W>(&config)?;
    let socket = Arc::new(<R::Net as Net>::UdpSocket::bind(addr).await?);
    let mut clock = Clock::new(eps_unc_ns, wall);
    let endpoint = sailing_proto::Endpoint::restart(
      config,
      clock.now(),
      seed,
      fsm,
      boot_epoch,
      &mut log,
      &mut stable,
    );
    let coord = QuicCoordinator::with_identity(endpoint, opts, None, cluster);
    Ok(Self::from_parts(
      coord, log, stable, socket, clock, peers, driver_cfg,
    ))
  }

  /// One-time MIGRATION restart from a pre-format store (one that persisted no `lease_support` floor),
  /// plus its [`Handle`]. Wraps
  /// [`Endpoint::restart_migrating`](sailing_proto::Endpoint::restart_migrating):
  /// `assume_prior_lease_support` upper-bounds the read-lease window this node may have advertised
  /// before the crash so the post-restart vote fence honors it. Pass `None` (or just use
  /// [`bind_restart_with_wall_clock`](Self::bind_restart_with_wall_clock)) once an enforcing restart
  /// has recorded a real durable floor.
  #[allow(clippy::too_many_arguments)]
  pub async fn bind_restart_migrating_with_wall_clock(
    addr: SocketAddr,
    config: Config<I>,
    seed: u64,
    fsm: F,
    boot_epoch: u64,
    assume_prior_lease_support: Option<Duration>,
    opts: QuicOptions,
    cluster: ClusterId,
    peers: Vec<Node<I, SocketAddr>>,
    mut log: L,
    mut stable: S,
    wall: W,
    driver_cfg: DriverConfig,
  ) -> Result<(Self, Handle<I, F>), BindError> {
    driver_cfg.validate()?;
    let eps_unc_ns = validate_and_capture_eps::<I, W>(&config)?;
    let socket = Arc::new(<R::Net as Net>::UdpSocket::bind(addr).await?);
    let mut clock = Clock::new(eps_unc_ns, wall);
    let endpoint = sailing_proto::Endpoint::restart_migrating(
      config,
      clock.now(),
      seed,
      fsm,
      boot_epoch,
      assume_prior_lease_support,
      &mut log,
      &mut stable,
    );
    let coord = QuicCoordinator::with_identity(endpoint, opts, None, cluster);
    Ok(Self::from_parts(
      coord, log, stable, socket, clock, peers, driver_cfg,
    ))
  }

  /// Assemble the driver + [`Handle`] from an already-constructed coordinator, clock, and bound
  /// socket. Shared by the fresh-`bind` and crash-`restart` entry points — they differ ONLY in how the
  /// endpoint inside `coord` is built (fresh vs. reconciled from the durable stores), so the
  /// channel/budget/handle wiring lives here once.
  fn from_parts(
    coord: QuicCoordinator<I, F>,
    log: L,
    stable: S,
    socket: Arc<<R::Net as Net>::UdpSocket>,
    clock: Clock<W>,
    peers: Vec<Node<I, SocketAddr>>,
    driver_cfg: DriverConfig,
  ) -> (Self, Handle<I, F>) {
    // Unbounded: the submit BUDGET — not the channel — is the binding bound on in-flight operations
    // (see the memory model), and an unbounded queue keeps shutdown from ever blocking on a full
    // channel.
    let (cmd_tx, cmd_rx) = flume::unbounded();
    let (event_tx, event_rx) = flume::bounded(driver_cfg.events_cap);
    let budget = InflightBudget::new(driver_cfg.max_inflight, driver_cfg.max_pending_bytes);
    // The teardown-completion oneshot: the driver keeps the sender and fires it after the socket's
    // fd-release barrier; every `Handle` clone awaits the shared receiver, so a coalesced shutdown
    // caller that does not itself enqueue still observes real teardown.
    let (teardown_tx, teardown_rx) = futures_channel::oneshot::channel();
    let handle = Handle::new(cmd_tx, event_rx, budget, teardown_rx);

    let (storage_ready, keepalive) = match driver_cfg.storage_ready {
      Some(rx) => (rx, None),
      None => {
        // No async store: hold the sender so the receiver parks forever instead of erroring.
        let (tx, rx) = flume::bounded(1);
        (rx, Some(tx))
      }
    };

    (
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
        // Clamped to at least one: the iter-top drain is the only flood-independent command path,
        // and shutdown's stoppable-under-load guarantee rides on it.
        cmd_budget: driver_cfg.cmd_budget.max(1),
        recv_cap: driver_cfg.recv_cap,
        max_failover_limbo_bytes: driver_cfg.max_failover_limbo_bytes,
        redial_base: driver_cfg.redial_base,
        redial_cap: driver_cfg.redial_cap,
        storage_closed: false,
        was_leader: false,
        teardown_tx: Some(teardown_tx),
      },
      handle,
    )
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

    let (recv_tx, recv_rx) = flume::bounded(self.recv_cap);
    let (recv_shutdown_tx, recv_shutdown_rx) = futures_channel::oneshot::channel();
    // The recv task's handle is OWNED by this scope — never detached — so the teardown block can signal
    // it to stop and await its join (the fd-release barrier; see below). The [`AbortOnDrop`] is the
    // panic-path abort backstop.
    let recv_task = AbortOnDrop::<R>::new(R::spawn(recv_datagrams::<R>(
      self.socket.clone(),
      recv_tx,
      recv_shutdown_rx,
    )));

    let now = self.clock.now();
    self.reconcile_peer_links(now.mono());
    let mut poisoned = self.pump(now).await;

    while !poisoned {
      let now = self.clock.now();

      // (1) Fairness: drain up to the command budget before the biased I/O select, so a continuous
      // recv backlog cannot starve Shutdown/Submit.
      let mut exit = false;
      for _ in 0..self.cmd_budget {
        match self.commands.try_recv() {
          Ok(cmd) => {
            if self.handle_command(now, cmd) {
              exit = true;
              break;
            }
          }
          Err(e) => {
            // Disconnected = every Handle clone dropped AND the buffer drained: the command stream
            // has ENDED for good — exit (a continuously-readable socket would otherwise keep the
            // task and the socket alive forever). Empty just falls through to the select.
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

      // (2) Fairness across the biased select: drain the datagram channel to a bounded budget HERE,
      // at the loop top, so it makes guaranteed per-iteration progress INDEPENDENT of the select
      // bias below. The compio driver handles only ONE datagram per iteration (its recv arm is the
      // select's first arm); draining a budget here keeps a recv flood from outpacing the loop's
      // per-pass timer/storage/command work.
      for _ in 0..IO_BUDGET {
        match recv_rx.try_recv() {
          Ok((datagram, from)) => self.coord.handle_udp(
            self.clock.now(),
            from,
            None,
            &datagram,
            &mut self.log,
            &mut self.stable,
          ),
          Err(_) => break,
        }
      }

      // (3) Fairness: fire an already-due deadline before the select, so a recv flood cannot
      // suppress heartbeats/elections. The coordinator's poll_timeout already folds the consensus
      // deadline, quinn's timers, and the auth deadline into ONE crate instant.
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
      if self.pump(now).await {
        break;
      }

      // Recomputed AFTER the iter-top fire so it reflects the NEXT deadline. The housekeeping wake
      // (while peers exist) caps the idle sleep so the link reconciler still runs roughly per second
      // on a node with no consensus timer (see HOUSEKEEPING_INTERVAL).
      let housekeeping =
        (!self.peers.is_empty()).then(|| std::time::Instant::now() + HOUSEKEEPING_INTERVAL);
      // An already-due instant when EITHER store still has a completion queued — derived from the
      // stores' LIVE state here, so it catches storage queued by a command (the loop-top fairness
      // drain OR the selected command) as well as a budget cutoff, not just the prior
      // `handle_storage`. So the timer fires immediately and the loop re-drives `handle_storage`
      // next pass WITHOUT sleeping.
      let storage_redrive =
        (self.log.has_pending() || self.stable.has_pending()).then(std::time::Instant::now);
      let deadline = self
        .coord
        .poll_timeout()
        .map(|d| self.clock.to_std(d))
        .into_iter()
        .chain(housekeeping)
        .chain(storage_redrive)
        .min()
        .unwrap_or_else(|| std::time::Instant::now() + Duration::from_secs(3600));

      // The select arms are plain channel/timer waits — the socket I/O lives in the recv task and
      // the pump — so a losing arm never cancels an in-flight socket op. NO accept arm: QUIC is
      // connectionless at the driver edge.
      enum Wake<I, F: StateMachine> {
        Datagram((Vec<u8>, SocketAddr)),
        Timer,
        Command(Option<Command<I, F>>),
        Storage,
        StorageClosed,
      }
      let wake = {
        let recv_fut = recv_rx.recv_async().fuse();
        let timer_fut =
          R::sleep(deadline.saturating_duration_since(std::time::Instant::now())).fuse();
        // Parked once every notifier sender has dropped (the `storage_closed` latch): a dead channel
        // resolves immediately forever and would hot-spin the loop, so the latched arm becomes
        // PENDING for good (an always-ready placeholder would re-create the spin).
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

        select_biased! {
          // `Err` (the recv task dropped its sender) is unreachable while this scope holds
          // recv_task: the task only exits when the receiver it sends to drops.
          got = recv_fut => Wake::Datagram(got.expect("recv_task outlives the loop")),
          _ = timer_fut => Wake::Timer,
          // flume `recv_async` yields `Ok` while any sender lives and `Err` once every `Handle`
          // clone has dropped (the buffer already drained) — the end-of-stream signal.
          cmd = cmd_fut => Wake::Command(cmd.ok()),
          got = storage_fut => {
            if got.is_err() { Wake::StorageClosed } else { Wake::Storage }
          }
        }
      };
      // Coalesce storage-ready wakes to a BOUNDED count: the signal carries no data and
      // `handle_storage` below does the real work every pass regardless, so an unbounded drain would
      // let a noisy notifier trap the loop and starve every other arm.
      for _ in 0..IO_BUDGET {
        if self.storage_ready.try_recv().is_err() {
          break;
        }
      }

      let now = self.clock.now();
      match wake {
        Wake::Datagram((datagram, from)) => {
          self
            .coord
            .handle_udp(now, from, None, &datagram, &mut self.log, &mut self.stable);
        }
        Wake::Timer => {
          self
            .coord
            .handle_timeout(now, &mut self.log, &mut self.stable);
        }
        Wake::Command(Some(cmd)) => {
          if self.handle_command(now, cmd) {
            break;
          }
        }
        Wake::Command(None) => break,
        Wake::Storage => {}
        Wake::StorageClosed => self.storage_closed = true,
      }
      // ALWAYS drain storage completions: synchronous stores complete inline with the calls above,
      // async ones signalled the arm we just coalesced.
      self
        .coord
        .handle_storage(now, &mut self.log, &mut self.stable);
      poisoned = self.pump(now).await;
    }

    // Teardown. Fail everything parked (each entry's reservation releases on drop), abort the recv
    // task, then make the command queue airtight: close-then-drain refuses a racing try_send WITH
    // its command (the handle's own rollback runs) while everything already buffered is drained and
    // dropped here — no command, queued or in flight, survives teardown. Classify the fail-stop
    // FIRST: an exit that raced a poison (a Shutdown command winning the select after the poisoning
    // storage drain) must still fail parked work with the typed verdict; the ShuttingDown sweep
    // below is then a no-op on the emptied maps.
    if self.coord.endpoint().is_poisoned() {
      self.routing.fail_all(&DriverError::Poisoned);
    }
    self.routing.fail_all(&DriverError::ShuttingDown);
    // Stop the recv task and AWAIT its join so its socket-Arc clone is released BEFORE we drop ours:
    // `AbortOnDrop`'s abort only SCHEDULES cancellation, so a `recv_from` parked in the task could
    // otherwise outlive `drop(self.socket)` and keep the fd bound past `shutdown().await` — an
    // immediate-rebind `AddrInUse` flake. The shutdown signal stops a recv-parked task; dropping the
    // receiver stops a send-parked one; the join then confirms the clone is gone.
    let _ = recv_shutdown_tx.send(());
    drop(recv_rx);
    if let Some(handle) = recv_task.into_handle() {
      let _ = handle.await;
    }
    // Drain everything already buffered, then DROP the receiver: a racing `try_send` then sees a
    // disconnected channel WITH its command (the handle's own rollback runs) — no command, queued or
    // in flight, survives teardown.
    while let Ok(cmd) = self.commands.try_recv() {
      drop(cmd);
    }
    drop(self.commands);
    // The fd-release point: the driver holds the LAST socket-Arc once the recv task's clone is
    // dropped (above), so this drop releases the UDP fd synchronously — the readiness analog of
    // compio's `socket.close().await`. Once it returns the bound address is free, what makes the
    // teardown signal an immediate-rebind contract.
    drop(self.socket);
    // Fire teardown so every parked `shutdown().await` (winner, swap-loser, disconnected path)
    // resolves and an immediate rebind is safe. Explicit AFTER the socket drop rather than a field
    // drop, whose ordering against it is not guaranteed. Dropping the sender instead of sending would
    // also satisfy the awaiters (`Canceled`), but the explicit send keeps the success path
    // observable.
    if let Some(tx) = self.teardown_tx.take() {
      let _ = tx.send(());
    }
  }

  /// Handle one command; returns `true` when the loop should exit (a `Shutdown`). Teardown
  /// completion is signalled by the run loop after the socket release, not here, so this carries no
  /// ack.
  fn handle_command(&mut self, now: Now, cmd: Command<I, F>) -> bool {
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
      Command::SetReadMode {
        mode,
        reply,
        reservation,
      } => {
        let r = self
          .coord
          .propose_read_mode_change(now, &mut self.log, &self.stable, mode)
          .map_err(map_propose_err);
        let _ = reply.send(r);
        // The migration applies cluster-wide once the entry commits; the verdict here is immediate, so
        // nothing parks — release with the reply.
        drop(reservation);
      }
      Command::Status { reply, reservation } => {
        let ep = self.coord.endpoint();
        let status = Status {
          role: ep.role(),
          term: ep.term(),
          leader: ep.leader(),
          commit_index: ep.commit_index(),
          applied_index: ep.applied_index(),
          active_read_mode: ep.active_read_mode(),
          conf_state: ep.conf_state(),
          is_poisoned: ep.is_poisoned(),
          precise_releases: ep.precise_releases(),
          unprovable_floor_holds: ep.unprovable_floor_holds(),
        };
        let _ = reply.send(status);
        drop(reservation);
      }
      Command::Shutdown => return true,
    }
    false
  }

  /// Dial every configured peer that has no bound connection and whose backoff has elapsed; a peer
  /// that re-binds resets its backoff.
  fn reconcile_peer_links(&mut self, now: Instant) {
    let std_now = std::time::Instant::now();
    for node in self.peers.clone() {
      let (peer, addr) = node.into_parts();
      if self.coord.has_bound_conn(&peer) {
        self.redial.remove(&peer);
        continue;
      }
      let due = self.redial.get(&peer).is_none_or(|r| std_now >= r.at);
      if !due {
        continue;
      }
      // A refused dial (cap, config) is just retried on the schedule; the typed error matters to
      // interactive callers, not to the background reconciler.
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
        match sailing_driver::shared::read_limbo(
          &self.log,
          &window,
          self.max_failover_limbo_bytes as u64,
        ) {
          Ok(Some(limbo)) => {
            let parked = std::mem::take(&mut self.routing.failovers);
            let fsm = self.coord.state_machine();
            // Re-check the lease with a FRESH wall before EACH completion — the scan and each closure
            // burn wall time, so the window can expire mid-batch.
            sailing_driver::shared::serve_failover_batch(parked, fsm, &limbo, window, || {
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
  async fn pump(&mut self, now: Now) -> bool {
    // Coalesce replication BEFORE the transmit drain so the batch leaves on this pass.
    self.coord.flush_appends(now, &self.log, &self.stable);
    // Drain the coordinator's queued datagrams FIRST. These awaited sends precede the failover serve
    // below BY DESIGN — parity with the normal-query serve: user-closure serves follow the consensus
    // output, never the reverse, so unbounded user read closures cannot stall outbound consensus
    // traffic. The drain is a finite fire-and-forget UDP batch and the inherited-lease window carries a
    // 2·ε_unc margin, so a bounded send phase cannot expire it; only a pathological send stall could,
    // which equally stalls the normal-read fallback — the failover path is never worse off than the
    // read it falls back to.
    while let Some((dest, bytes)) = self.coord.poll_transmit() {
      // A send error is transient for UDP (the peer redials / QUIC retransmits); dropping the
      // datagram is the same observable as the network dropping it. Borrowed `&bytes`: the readiness
      // socket sends from a slice, never an owned buffer.
      let _ = self.socket.send_to(&bytes, dest).await;
    }
    let mut run_queries = false;
    while let Some(ev) = self.coord.poll_event() {
      run_queries |= self.routing.route_event(ev);
    }
    // Eventless applies advance the endpoint's applied index with NO routed event — a fresh leader's
    // `Empty` no-op, and the committed prefix a restart replays (its events are cleared) — so reconcile
    // the driver watermark to the endpoint HERE, or a read confirmed at such an index never becomes
    // runnable. Skipped on a poisoned node: the fail-stop sweep below owns its parked work.
    if !self.coord.endpoint().is_poisoned() {
      run_queries |= self
        .routing
        .sync_applied(self.coord.endpoint().applied_index());
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
    // The fail-stop check: a poisoned endpoint suppresses poll_event and poll_timeout by design, so
    // anything parked would otherwise wait forever holding its reservation. Fail it all with the
    // typed verdict and tell the run loop to exit — the NODE is dead; an operator restart (or
    // re-provisioning) is the only recovery, and keeping the socket bound would only mislead peers.
    if self.coord.endpoint().is_poisoned() {
      self.routing.fail_all(&DriverError::Poisoned);
      return true;
    }
    false
  }
}
