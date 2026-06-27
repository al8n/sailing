//! [`CompioStreamDriver`]: one task owning a [`StreamCoordinator`], the embedder's stores, and a
//! TCP listener, driving consensus over framed reliable streams (plain TCP or TLS, by the record
//! layer the factories build).

use std::{cell::Cell, collections::BTreeMap, net::SocketAddr, rc::Rc, time::Duration};

use bytes::Bytes;
use compio::net::{TcpListener, TcpStream};
use sailing_proto::{
  Config, ConnId, Instant, LogStore, Now, RecordIo, StableStore, StateMachine, StreamCoordinator,
};

use sailing_driver::{
  Command, Handle, jittered,
  shared::{InflightBudget, ParkedFailover, ParkedQuery, Pending, Routing},
  validate_and_capture_eps,
};

use crate::{
  BindError, Clock, DriverConfig, DriverError, Monotonic, WallClock,
  bridge::{BridgeInbound, BridgeOut, DialReady, bridge_read, bridge_write},
};

use super::{map_propose_err, map_read_err, map_transfer_err};

/// Builds the record layer for an OUTBOUND connection to the given peer (the peer parameter
/// carries the dial target so a TLS dialer can derive its SNI). Infallibility is not assumed:
/// a failed construction (a bad local id, a TLS config error) surfaces as an `io::Error` and the
/// dial is retried by the link reconciler like any other failure.
pub type DialerFactory<I, R> = Rc<dyn Fn(&I) -> std::io::Result<R>>;
/// Builds the record layer for an ACCEPTED connection.
pub type AcceptorFactory<R> = Rc<dyn Fn() -> std::io::Result<R>>;

/// The persistent accept task: owns a listener clone, forwarding each accepted socket into the
/// bounded channel the run loop selects on. A full channel parks the task — further connections
/// queue in (then overflow) the kernel listen backlog, which is exactly TCP accept backpressure.
/// An accept error is transient (a refused/reset in-flight connection); the loop keeps accepting
/// after a paced backoff.
async fn accept_conns(
  listener: TcpListener,
  accepted: lochan::mpsc::Sender<(TcpStream, SocketAddr)>,
) {
  loop {
    match listener.accept().await {
      Ok((socket, from)) => {
        if accepted.send((socket, from)).await.is_err() {
          return; // the run loop dropped its receiver: teardown
        }
      }
      Err(_) => compio::time::sleep(Duration::from_millis(20)).await,
    }
  }
}

/// The live task(s) the driver holds for one connection. Dropping it cancels whatever runs:
/// compio aborts a non-detached task when its `JoinHandle` drops, so dropping a `Connecting`
/// cancels the dial and dropping a `Bridged` cancels BOTH split-half tasks — aborting a stuck
/// write and dropping the socket. The handles are held ONLY for that drop-cancel.
enum ConnTask {
  /// The dial/connect task, until it completes.
  #[allow(dead_code)]
  Connecting(compio::runtime::JoinHandle<()>),
  /// The two independent split-half tasks (each owns one half via `into_split`, so a large write
  /// never starves the reader).
  #[allow(dead_code)]
  Bridged {
    read: compio::runtime::JoinHandle<()>,
    write: compio::runtime::JoinHandle<()>,
  },
}

/// Per-peer link-repair state. An entry records FAILURE HISTORY and persists until the peer's
/// binding proves STABLE — bound continuously for at least `redial_base` — because a binding
/// that merely EXISTS for a moment proves nothing: the symmetric mutual-dial race produces
/// validated, bound survivors that die within an RTT, and resetting the backoff on sight of
/// one would restart every round from base, erasing the doubling that makes the race converge.
struct Redial {
  /// The next dial attempt may fire at/after this instant.
  at: std::time::Instant,
  /// The un-jittered delay the NEXT attempt will wait (doubles per attempt, capped).
  backoff: Duration,
  /// When the current binding was first observed, while one exists. Observation runs at loop
  /// cadence (a bound peer means consensus traffic, so passes run at least per heartbeat); a
  /// binding observed bound for `redial_base` is stable and clears the entry.
  bound_since: Option<std::time::Instant>,
}

/// Everything the driver owns for one connection. Dropping it tears the connection down:
/// the task drop cancels the live compio task(s), and dropping `out_tx` signals a still-running
/// writer to flush-then-exit.
struct Conn<I> {
  tasks: ConnTask,
  /// Outbound wire bytes to the writer (the per-conn FIFO).
  out_tx: lochan::mpsc::Sender<BridgeOut>,
  /// Bytes enqueued toward the socket and not yet written — the per-conn memory bound.
  queued_bytes: Rc<Cell<usize>>,
  /// `Some(peer)` for a dialed conn — the reconciler's dial-in-flight marker — and `None` for
  /// an accepted one. Repair scheduling itself lives in the reconciler, never on the conn.
  dialed_to: Option<I>,
}

/// A consensus node over framed reliable streams on compio. `R` is the record layer the
/// factories build: `Labeled<Passthrough>` for plain TCP, `Labeled<TlsRecords>` for TLS.
///
/// Construct AND run on the same thread (see the crate docs); the `Rc` factories make this
/// driver structurally `!Send`, enforcing that pinning.
pub struct CompioStreamDriver<I, F, R, L, S, W = Monotonic>
where
  I: sailing_proto::NodeId,
  F: StateMachine,
  R: RecordIo,
{
  coord: StreamCoordinator<I, F, R>,
  log: L,
  stable: S,
  listener: TcpListener,
  clock: Clock<W>,
  /// Byte cap on the failover inherited-read limbo scan (see
  /// [`DriverConfig::max_failover_limbo_bytes`]).
  max_failover_limbo_bytes: usize,
  commands: flume::Receiver<Command<I, F>>,
  routing: Routing<I, F::Response, F>,
  storage_ready: flume::Receiver<()>,
  _storage_ready_keepalive: Option<flume::Sender<()>>,
  conns: BTreeMap<ConnId, Conn<I>>,
  /// The link reconciler's per-peer backoff state (failure history; see [`Redial`]).
  redial: BTreeMap<I, Redial>,
  /// The earliest instant the reconciler needs a wake for (recomputed every pass). Folding raw
  /// `Redial::at` values into the select deadline would HOT-SPIN: a bound or dial-in-flight
  /// peer legitimately carries a stale past `at`, and a past deadline fires the timer
  /// instantly, every iteration, for the whole stability window.
  redial_wake: Option<std::time::Instant>,
  peers: Vec<(I, SocketAddr)>,
  dialer: DialerFactory<I, R>,
  acceptor: AcceptorFactory<R>,
  inbound_tx: lochan::mpsc::Sender<BridgeInbound>,
  inbound_rx: lochan::mpsc::Receiver<BridgeInbound>,
  dial_ready_tx: flume::Sender<DialReady>,
  dial_ready_rx: flume::Receiver<DialReady>,
  cmd_budget: usize,
  accept_cap: usize,
  max_outbound_backlog: usize,
  max_conns: usize,
  redial_base: Duration,
  redial_cap: Duration,
  /// Latched when every storage-ready sender has dropped (see the QUIC driver's twin field): a
  /// dead channel would win the select forever and hot-spin the loop; the latch parks the arm,
  /// downgrading storage completions to timer/I/O cadence — `handle_storage` runs every
  /// iteration regardless.
  storage_closed: bool,
  /// Leadership as of the END of the last pass — the supersede backstop, defense-in-depth
  /// behind the event-driven sweep (see the QUIC driver's twin field): the proto announces
  /// every leadership loss with `LeaderChanged(None)`, so this edge-detect is a second,
  /// event-independent witness, not the primary path.
  was_leader: bool,
  /// The teardown-completion signal: fired (or dropped) AFTER the listener's `close().await`
  /// fd-release barrier on every run-loop exit, fanning to every `Handle` via the shared receiver
  /// so each `shutdown().await` resolves only once the bound address is rebindable. `Option` so the
  /// fire is an explicit, ordered `take().send(())` right after `close()` — never an implicit field
  /// drop whose ordering against the socket close is not guaranteed.
  teardown_tx: Option<futures_channel::oneshot::Sender<()>>,
}

impl<I, F, R, L, S> CompioStreamDriver<I, F, R, L, S, Monotonic>
where
  I: sailing_proto::NodeId + Send,
  F: StateMachine,
  F::Command: sailing_proto::Data + Send,
  F::Snapshot: sailing_proto::Data,
  F::Response: Clone + Send,
  F::Error: core::error::Error,
  R: RecordIo,
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
    peers: Vec<(I, SocketAddr)>,
    dialer: DialerFactory<I, R>,
    acceptor: AcceptorFactory<R>,
    log: L,
    stable: S,
    driver_cfg: DriverConfig,
  ) -> Result<(Self, Handle<I, F>), BindError> {
    Self::bind_with_wall_clock(
      addr, config, seed, fsm, peers, dialer, acceptor, log, stable, Monotonic, driver_cfg,
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
    peers: Vec<(I, SocketAddr)>,
    dialer: DialerFactory<I, R>,
    acceptor: AcceptorFactory<R>,
    log: L,
    stable: S,
    driver_cfg: DriverConfig,
  ) -> Result<(Self, Handle<I, F>), BindError> {
    Self::bind_restart_with_wall_clock(
      addr, config, seed, fsm, boot_epoch, peers, dialer, acceptor, log, stable, Monotonic,
      driver_cfg,
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
    peers: Vec<(I, SocketAddr)>,
    dialer: DialerFactory<I, R>,
    acceptor: AcceptorFactory<R>,
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
      peers,
      dialer,
      acceptor,
      log,
      stable,
      Monotonic,
      driver_cfg,
    )
    .await
  }
}

impl<I, F, R, L, S, W> CompioStreamDriver<I, F, R, L, S, W>
where
  I: sailing_proto::NodeId + Send,
  F: StateMachine,
  F::Command: sailing_proto::Data + Send,
  F::Snapshot: sailing_proto::Data,
  F::Response: Clone + Send,
  F::Error: core::error::Error,
  R: RecordIo,
  L: LogStore,
  S: StableStore<NodeId = I>,
  W: WallClock,
{
  /// Bind the listener and build the driver plus its [`Handle`]. The configured peers are dialed
  /// at `run()` start and redialed (jittered exponential backoff) whenever their connection
  /// dies; handshake reaping and duplicate tie-breaks are the coordinator's, surfaced through
  /// its close reporting.
  #[allow(clippy::too_many_arguments)]
  pub async fn bind_with_wall_clock(
    addr: SocketAddr,
    config: Config<I>,
    seed: u64,
    fsm: F,
    peers: Vec<(I, SocketAddr)>,
    dialer: DialerFactory<I, R>,
    acceptor: AcceptorFactory<R>,
    log: L,
    stable: S,
    wall: W,
    driver_cfg: DriverConfig,
  ) -> Result<(Self, Handle<I, F>), BindError> {
    // Reject an out-of-range programmatic `DriverConfig` UP FRONT (before the socket binds). The
    // serde/clap parse paths validate, but a programmatic config bypasses that; this keeps the
    // channel-sizing knobs under their ceilings before any channel is built — in particular the
    // eager-ring `inbound_cap`/`accept_cap` (each a `lochan::mpsc::bounded` ring allocated in full at
    // `cap` slots) under `MAX_BOUNDED_QUEUE_DEPTH`, so an astronomical value cannot OOM at bind.
    driver_cfg.validate()?;
    // Validate + capture ε_unc (the sole copy of the wall-gate threshold) BEFORE the socket binds,
    // rejecting an invalid Config and the silent failover wedge (a failover tier with a non-supplying
    // source).
    let eps_unc_ns = validate_and_capture_eps::<I, W>(&config)?;
    let listener = TcpListener::bind(addr).await?;
    let mut clock = Clock::new(eps_unc_ns, wall);
    let coord = StreamCoordinator::new(config, clock.now(), seed, fsm);
    Ok(Self::from_parts(
      coord, log, stable, listener, clock, peers, dialer, acceptor, driver_cfg,
    ))
  }

  /// Restart the listener and driver from DURABLE storage after a crash, plus its [`Handle`].
  ///
  /// The crash-recovery sibling of [`bind_with_wall_clock`](Self::bind_with_wall_clock): instead of a
  /// fresh endpoint it builds the coordinator through [`StreamCoordinator::restart`], which RECONCILES
  /// the durable [`LogStore`]/[`StableStore`] — recovering the persisted term/vote/commit, replaying
  /// the committed tail, and re-arming the lease/vote fences — so a restarting node never double-votes
  /// by booting at term 0. `boot_epoch` MUST be strictly greater than every prior incarnation's and
  /// persisted durably BEFORE this call (see [`StreamCoordinator::restart`]); a fresh node uses 0, so
  /// the first restart passes at least 1. The connection table starts empty (peers are re-dialed/-
  /// accepted).
  #[allow(clippy::too_many_arguments)]
  pub async fn bind_restart_with_wall_clock(
    addr: SocketAddr,
    config: Config<I>,
    seed: u64,
    fsm: F,
    boot_epoch: u64,
    peers: Vec<(I, SocketAddr)>,
    dialer: DialerFactory<I, R>,
    acceptor: AcceptorFactory<R>,
    mut log: L,
    mut stable: S,
    wall: W,
    driver_cfg: DriverConfig,
  ) -> Result<(Self, Handle<I, F>), BindError> {
    driver_cfg.validate()?;
    let eps_unc_ns = validate_and_capture_eps::<I, W>(&config)?;
    let listener = TcpListener::bind(addr).await?;
    let mut clock = Clock::new(eps_unc_ns, wall);
    let coord = StreamCoordinator::restart(
      config,
      clock.now(),
      seed,
      fsm,
      boot_epoch,
      &mut log,
      &mut stable,
    );
    Ok(Self::from_parts(
      coord, log, stable, listener, clock, peers, dialer, acceptor, driver_cfg,
    ))
  }

  /// One-time MIGRATION restart from a pre-format store (one that persisted no `lease_support` floor),
  /// plus its [`Handle`]. Wraps [`StreamCoordinator::restart_migrating`]:
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
    peers: Vec<(I, SocketAddr)>,
    dialer: DialerFactory<I, R>,
    acceptor: AcceptorFactory<R>,
    mut log: L,
    mut stable: S,
    wall: W,
    driver_cfg: DriverConfig,
  ) -> Result<(Self, Handle<I, F>), BindError> {
    driver_cfg.validate()?;
    let eps_unc_ns = validate_and_capture_eps::<I, W>(&config)?;
    let listener = TcpListener::bind(addr).await?;
    let mut clock = Clock::new(eps_unc_ns, wall);
    let coord = StreamCoordinator::restart_migrating(
      config,
      clock.now(),
      seed,
      fsm,
      boot_epoch,
      assume_prior_lease_support,
      &mut log,
      &mut stable,
    );
    Ok(Self::from_parts(
      coord, log, stable, listener, clock, peers, dialer, acceptor, driver_cfg,
    ))
  }

  /// Assemble the driver + [`Handle`] from an already-constructed coordinator, clock, and bound
  /// listener. Shared by the fresh-`bind` and crash-`restart` entry points — they differ ONLY in how
  /// `coord` is built (a fresh endpoint vs. one reconciled from the durable stores), so the
  /// channel/budget/handle wiring lives here once.
  #[allow(clippy::too_many_arguments)]
  fn from_parts(
    coord: StreamCoordinator<I, F, R>,
    log: L,
    stable: S,
    listener: TcpListener,
    clock: Clock<W>,
    peers: Vec<(I, SocketAddr)>,
    dialer: DialerFactory<I, R>,
    acceptor: AcceptorFactory<R>,
    driver_cfg: DriverConfig,
  ) -> (Self, Handle<I, F>) {
    // Unbounded: the submit BUDGET is the binding bound on in-flight operations, so the channel
    // carries no cap of its own and shutdown can never block on a full queue.
    let (cmd_tx, cmd_rx) = flume::unbounded();
    let (event_tx, event_rx) = flume::bounded(driver_cfg.events_cap);
    let budget = InflightBudget::new(driver_cfg.max_inflight, driver_cfg.max_pending_bytes);
    // The teardown-completion oneshot: the driver keeps the sender and fires it after the listener's
    // fd-release barrier; every `Handle` clone awaits the shared receiver, so a coalesced shutdown
    // caller that does not itself enqueue still observes real teardown.
    let (teardown_tx, teardown_rx) = futures_channel::oneshot::channel();
    let handle = Handle::new(cmd_tx, event_rx, budget, teardown_rx);

    let (storage_ready, keepalive) = match driver_cfg.storage_ready {
      Some(rx) => (rx, None),
      None => {
        let (tx, rx) = flume::bounded(1);
        (rx, Some(tx))
      }
    };
    let (inbound_tx, inbound_rx) = lochan::mpsc::bounded(driver_cfg.inbound_cap);
    // Bounded by construction at the live dial count (one task, one completion each), itself
    // bounded by the peer book + the reconciler's in-flight dedup; `unbounded` here means
    // "never parks a dial task".
    let (dial_ready_tx, dial_ready_rx) = flume::unbounded();
    // The cap must admit the full mutual-dial mesh (a dialed AND an accepted conn per peer) —
    // mesh dials are never refused (consensus liveness), so a configured cap below the mesh
    // would let the documented bound be exceeded silently instead of sizing it honestly.
    let max_conns = driver_cfg.max_conns.max(2 * peers.len());
    let max_failover_limbo_bytes = driver_cfg.max_failover_limbo_bytes;

    (
      Self {
        coord,
        log,
        stable,
        listener,
        clock,
        max_failover_limbo_bytes,
        commands: cmd_rx,
        routing: Routing::new(event_tx),
        storage_ready,
        _storage_ready_keepalive: keepalive,
        conns: BTreeMap::new(),
        redial: BTreeMap::new(),
        redial_wake: None,
        peers,
        dialer,
        acceptor,
        inbound_tx,
        inbound_rx,
        dial_ready_tx,
        dial_ready_rx,
        // Clamped to at least one: the iter-top drain is the only flood-independent command
        // path, and shutdown's stoppable-under-load guarantee rides on it.
        cmd_budget: driver_cfg.cmd_budget.max(1),
        accept_cap: driver_cfg.accept_cap,
        max_outbound_backlog: driver_cfg.max_outbound_backlog,
        max_conns,
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

  /// Drive consensus until shutdown (or until every `Handle` clone has dropped and the buffered
  /// commands drained).
  pub async fn run(mut self) {
    use futures_util::{FutureExt, select_biased};

    let (accept_tx, mut accept_rx) =
      lochan::mpsc::bounded::<(TcpStream, SocketAddr)>(self.accept_cap);
    let accept_task = compio::runtime::spawn(accept_conns(self.listener.clone(), accept_tx));

    // The first reconciler pass dials the full configured mesh (nothing is bound yet).
    let now = self.clock.now();
    self.reconcile_peer_links(now.mono());
    let mut poisoned = self.pump().await;

    while !poisoned {
      let now = self.clock.now();

      // Fairness: a bounded command drain before the biased select.
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

      // Fire an already-due deadline before the select (an inbound flood cannot suppress
      // elections/heartbeats), then reconcile peer links, then pump.
      if self
        .coord
        .poll_timeout()
        .is_some_and(|d| d <= self.clock.mono())
      {
        self
          .coord
          .handle_timeout(now, &mut self.log, &mut self.stable);
      }
      self.reconcile_peer_links(now.mono());
      if self.pump().await {
        break;
      }

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
        .chain(self.redial_wake)
        .chain(storage_redrive)
        .min()
        .unwrap_or_else(|| std::time::Instant::now() + Duration::from_secs(3600));

      enum Wake<I, F: StateMachine> {
        Inbound(BridgeInbound),
        Accepted(TcpStream),
        DialReady(DialReady),
        Timer,
        Command(Option<Command<I, F>>),
        Storage,
        StorageClosed,
      }
      let wake = {
        // `accept_rx` is a run-loop local, so its lochan `recv` (`&mut self`) is pre-pinnable.
        let accept_fut = accept_rx.recv();
        let dial_fut = self.dial_ready_rx.recv_async().fuse();
        let timer_fut = compio::time::sleep_until(deadline).fuse();
        // Parked once every notifier sender has dropped (the `storage_closed` latch): a dead
        // channel resolves immediately forever and would hot-spin the loop, so the latched arm
        // becomes PENDING for good (an always-ready placeholder would re-create the spin).
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
        // `inbound_rx` is a lochan receiver (`&mut self` recv), so it is recv'd INLINE in its arm
        // below — never pre-pinned — so the `&mut self.inbound_rx` borrow ends the instant the
        // select resolves. (`accept_fut`/`dial_fut`/`storage_fut`/`cmd_fut` borrow disjoint fields
        // or locals, so they stay pre-created.) lochan `recv` is `Unpin + FusedFuture`; pinning it
        // via `pin_mut!` gives `select_biased!` the `&mut` place it polls (no `.fuse()` needed).
        futures_util::pin_mut!(accept_fut, dial_fut, timer_fut, storage_fut, cmd_fut);

        select_biased! {
          // lochan `recv` yields `Some` while the producing tasks live; `None` (every bridge
          // dropped its sender) is unreachable while the loop is alive — the senders outlive it.
          got = self.inbound_rx.recv() => Wake::Inbound(got.expect("inbound_tx outlives the loop")),
          got = accept_fut => {
            let (s, _from) = got.expect("accept task outlives the loop");
            Wake::Accepted(s)
          }
          got = dial_fut => Wake::DialReady(got.expect("dial_ready_tx outlives the loop")),
          _ = timer_fut => Wake::Timer,
          // flume `recv_async` yields `Ok` while any sender lives and `Err` once every
          // `Handle` clone has dropped (the buffer already drained) — the end-of-stream signal.
          cmd = cmd_fut => Wake::Command(cmd.ok()),
          got = storage_fut => {
            if got.is_err() { Wake::StorageClosed } else { Wake::Storage }
          }
        }
      };
      while self.storage_ready.try_recv().is_ok() {}

      let now = self.clock.now();
      match wake {
        Wake::Inbound(inbound) => self.handle_inbound(now, inbound),
        Wake::Accepted(socket) => self.handle_accept(now.mono(), socket),
        Wake::DialReady(ready) => self.handle_dial_ready(ready),
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
      self
        .coord
        .handle_storage(now, &mut self.log, &mut self.stable);
      poisoned = self.pump().await;
    }

    // Teardown: fail everything parked, cancel the accept task and every connection's tasks
    // (their JoinHandle drops cancel them; out_tx drops flush-and-exit the writers), make the
    // command queue airtight, and close the listener — the fd-release barrier the teardown signal
    // fires behind. Classify the fail-stop FIRST: an exit that raced a poison (a Shutdown command
    // winning the select after the poisoning storage drain) must still fail parked work with the
    // typed verdict; the ShuttingDown sweep below is then a no-op on the emptied maps.
    if self.coord.endpoint().is_poisoned() {
      self.routing.fail_all(&DriverError::Poisoned);
    }
    self.routing.fail_all(&DriverError::ShuttingDown);
    drop(accept_task);
    drop(accept_rx);
    // Dropping every Conn cancels its tasks; queued frames are discarded (consensus
    // retransmission re-drives them — see close_conn for why bounded teardown wins).
    self.conns.clear();
    // Drain everything already buffered, then DROP the receiver: a racing `try_send` then sees a
    // disconnected channel and the handle's own rollback runs — no command survives teardown.
    while let Ok(cmd) = self.commands.try_recv() {
      drop(cmd);
    }
    drop(self.commands);
    let _ = self.listener.close().await;
    // The listener fd is now released — fire teardown so every parked `shutdown().await` (winner,
    // swap-loser, disconnected path) resolves and an immediate rebind is safe. Explicit AFTER
    // `close()` rather than a field drop, whose ordering against the close await is not guaranteed.
    // Dropping the sender instead of sending would also satisfy the awaiters (`Canceled`), but the
    // explicit send keeps the success path observable.
    if let Some(tx) = self.teardown_tx.take() {
      let _ = tx.send(());
    }
  }

  /// One inbound bridge frame: feed bytes/EOF to the coordinator (errors close the conn).
  fn handle_inbound(&mut self, now: Now, inbound: BridgeInbound) {
    match inbound {
      BridgeInbound::Bytes { id, bytes } => {
        self
          .coord
          .handle_conn_data(id, &bytes, false, now, &mut self.log, &mut self.stable);
      }
      BridgeInbound::Eof { id } => {
        self
          .coord
          .handle_conn_data(id, &[], true, now, &mut self.log, &mut self.stable);
      }
      BridgeInbound::Error { id } => self.close_conn(id),
    }
  }

  /// One accepted socket: admission control, record-layer construction, registration, bridging.
  fn handle_accept(&mut self, now: Instant, socket: TcpStream) {
    if self.conns.len() >= self.max_conns {
      // At the cap: refuse by dropping the socket. Mesh DIALS are never refused (consensus
      // liveness); only unsolicited accepts are bounded here.
      return;
    }
    let record = match (self.acceptor)() {
      Ok(r) => r,
      Err(_) => return, // a mis-built record layer cannot serve this socket
    };
    let id = self.coord.on_conn_open(record, now);
    let (out_tx, out_rx) = lochan::mpsc::unbounded();
    let queued = Rc::new(Cell::new(0usize));
    let (read_half, write_half) = socket.into_split();
    let read = compio::runtime::spawn(bridge_read(read_half, id, self.inbound_tx.clone()));
    let write = compio::runtime::spawn(bridge_write(
      write_half,
      id,
      out_rx,
      queued.clone(),
      self.inbound_tx.clone(),
    ));
    self.conns.insert(
      id,
      Conn {
        tasks: ConnTask::Bridged { read, write },
        out_tx,
        queued_bytes: queued,
        dialed_to: None,
      },
    );
  }

  /// Reconcile every configured peer link against CURRENT state, once per loop iteration.
  ///
  /// Link repair is a STANDING RECONCILER (the same shape as the QUIC driver's), never a
  /// close-time decision: a bound peer suppresses dialing now (and, once STABLE for
  /// `redial_base`, clears its failure history); a dial already in flight suppresses;
  /// otherwise, once the per-peer backoff allows, one dial fires and the backoff doubles.
  /// Close-time scheduling is wrong in both directions — done
  /// unconditionally, a duplicate tie-break close redials and the fresh higher `ConnId` evicts
  /// the bound survivor (steady churn); gated on close-time bound state, the SYMMETRIC
  /// tie-break (both sides dialed within one SYN flight, so each side's accepted conn outranks
  /// its dialed one) has each side keep the very socket the other is closing — both survivors
  /// die moments later and nobody reschedules: a permanently dead edge. The reconciler is
  /// immune to both because it re-derives from `conn_of` and the live-conn table every pass: a
  /// dead edge is re-discovered no matter how it died.
  ///
  /// Convergence of the symmetric race itself: the per-peer backoff doubles on every attempt
  /// and is reset ONLY by a binding that stays bound for the stability window (`redial_base`)
  /// — the race's doomed survivors die within close-propagation time (an RTT-scale bound, BELOW
  /// `redial_base` by the knob's contract), so while the race repeats, the doubling is
  /// monotone, the jittered spread between the two sides' next dials widens, and once it
  /// exceeds a SYN flight both routers rank the SAME (later) dial highest and one socket
  /// survives on both ends. A dial against an accepted-but-not-yet-validated conn (`conn_of`
  /// still `None`) can still mint one transient duplicate — the tie-break resolves it in one
  /// round. Asymmetric peer books are tolerated: repair responsibility follows the BOOK
  /// (whoever lists the peer redials it), not conn provenance.
  fn reconcile_peer_links(&mut self, now: Instant) {
    let std_now = std::time::Instant::now();
    let mut wake: Option<std::time::Instant> = None;
    for (peer, addr) in self.peers.clone() {
      if self.coord.conn_of(&peer).is_some() {
        // Bound. Failure history is cleared only once the binding proves stable; a doomed
        // tie-break survivor (dead within an RTT) never reaches the window, so its round
        // keeps — and keeps doubling — the backoff. Eviction needs no timer: a bound peer
        // means consensus traffic, so passes run at least per heartbeat.
        let stable = match self.redial.get_mut(&peer) {
          None => false, // steady state: bound with no failure history
          Some(r) => {
            let since = *r.bound_since.get_or_insert(std_now);
            std_now.duration_since(since) >= self.redial_base
          }
        };
        if stable {
          self.redial.remove(&peer);
        }
        continue;
      }
      if let Some(r) = self.redial.get_mut(&peer) {
        r.bound_since = None; // whatever binding existed died before proving stable
      }
      if self
        .conns
        .values()
        .any(|c| c.dialed_to.as_ref() == Some(&peer))
      {
        continue; // a dialed socket for this peer is already connecting/validating
      }
      if let Some(r) = self.redial.get(&peer)
        && std_now < r.at
      {
        wake = Some(wake.map_or(r.at, |w| w.min(r.at))); // backing off: wake to retry
        continue;
      }
      let delay = self
        .redial
        .get(&peer)
        .map_or(self.redial_base, |r| r.backoff);
      let at = std_now + jittered(delay);
      self.redial.insert(
        peer.cheap_clone(),
        Redial {
          at,
          backoff: (delay * 2).min(self.redial_cap),
          bound_since: None,
        },
      );
      self.dial(now, peer, addr);
      // Cover the corner where this attempt produced NO conn and NO future event (a record-
      // factory failure): the armed `at` still gets a wake. When the attempt did produce a
      // socket, the wake is benign — the next pass sees the in-flight conn and skips.
      wake = Some(wake.map_or(at, |w| w.min(at)));
    }
    self.redial_wake = wake;
  }

  /// Register + start one dial attempt. The coordinator registration happens NOW (its handshake
  /// bytes queue against the conn id immediately); the socket connects asynchronously and the
  /// bridge halves spawn on completion. A record-factory failure abandons the attempt — the
  /// reconciler retries it on the backoff already armed for this attempt.
  fn dial(&mut self, now: Instant, peer: I, addr: SocketAddr) {
    let record = match (self.dialer)(&peer) {
      Ok(r) => r,
      Err(_) => return,
    };
    let id = self.coord.on_conn_open(record, now);
    let (out_tx, out_rx) = lochan::mpsc::unbounded();
    let queued = Rc::new(Cell::new(0usize));
    let dial_ready = self.dial_ready_tx.clone();
    let task = compio::runtime::spawn({
      let queued = queued.clone();
      async move {
        let result = TcpStream::connect(addr).await;
        let _ = dial_ready
          .send_async(DialReady {
            id,
            result,
            out_rx,
            queued_bytes: queued,
          })
          .await;
      }
    });
    self.conns.insert(
      id,
      Conn {
        tasks: ConnTask::Connecting(task),
        out_tx,
        queued_bytes: queued,
        dialed_to: Some(peer),
      },
    );
  }

  /// One dial completion: bridge the socket, or close (the reconciler retries).
  fn handle_dial_ready(&mut self, ready: DialReady) {
    let DialReady {
      id,
      result,
      out_rx,
      queued_bytes,
    } = ready;
    match result {
      Ok(socket) => {
        if let Some(conn) = self.conns.get_mut(&id) {
          let (read_half, write_half) = socket.into_split();
          let read = compio::runtime::spawn(bridge_read(read_half, id, self.inbound_tx.clone()));
          let write = compio::runtime::spawn(bridge_write(
            write_half,
            id,
            out_rx,
            queued_bytes,
            self.inbound_tx.clone(),
          ));
          conn.tasks = ConnTask::Bridged { read, write };
        }
        // A conn the coordinator already closed (handshake reap racing the connect): the entry
        // is gone; dropping out_rx/halves here tears the socket down.
      }
      Err(_) => self.close_conn(id),
    }
  }

  /// Tear one connection down: tell the coordinator and drop the `Conn` — CANCELLING both
  /// bridge halves (or the dial task). NO repair decision is made here: the standing
  /// reconciler re-derives every peer's link state each iteration (close-time decisions are
  /// wrong in both directions — see [`Self::reconcile_peer_links`]).
  ///
  /// Frames still queued toward the socket are DISCARDED with the cancel, deliberately:
  /// consensus retransmission re-drives anything that mattered, so the loss is benign — while
  /// the alternative (detaching the writer to drain them) has UNBOUNDED lifetime: a peer that
  /// keeps its TCP window closed parks `write_all` forever, and a detached, table-removed task
  /// counts against no cap. Bounded teardown wins over best-effort delivery the protocol
  /// already guarantees by other means.
  fn close_conn(&mut self, id: ConnId) {
    self.coord.on_conn_close(id);
    drop(self.conns.remove(&id));
  }

  /// Handle one command (same dispatch as the QUIC driver). Returns `true` when the loop should
  /// exit (a `Shutdown`); teardown completion is signalled by the run loop after `close().await`,
  /// not here, so this carries no ack.
  fn handle_command(&mut self, now: Now, cmd: Command<I, F>) -> bool {
    match cmd {
      Command::Submit {
        cmd,
        reply,
        reservation,
      } => match self
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
      },
      Command::Conf {
        cc,
        reply,
        reservation,
      } => match self
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
      },
      Command::ConfV2 {
        cc,
        reply,
        reservation,
      } => match self
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
      },
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
          Err(e) => complete(Err(map_read_err(e))),
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
      Command::Shutdown => return true,
    }
    false
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

  /// Drain the coordinator's outputs: wire bytes to each conn's writer (byte-budgeted), internal
  /// closes into teardown (the reconciler repairs), events into completions and queries.
  async fn pump(&mut self) -> bool {
    for (id, bytes) in self.coord.poll_transmit() {
      let Some(conn) = self.conns.get(&id) else {
        continue; // already closed; the coordinator's stale bytes die with it
      };
      let projected = conn.queued_bytes.get() + bytes.len();
      if projected > self.max_outbound_backlog {
        // The peer has stopped consuming: close (consensus retransmission re-drives).
        self.close_conn(id);
        continue;
      }
      conn.queued_bytes.set(conn.queued_bytes.get() + bytes.len());
      // lochan unbounded `try_send` never returns `Full`; only `Closed` (the writer task already
      // exited), and a stale enqueue onto a dying conn is benign (consensus retransmits).
      let _ = conn.out_tx.try_send(BridgeOut(Bytes::from(bytes)));
    }
    // Coordinator-initiated closes (handshake reap, duplicate tie-break, faults): tear down
    // the bridge side; the link reconciler repairs whatever ends up unbound.
    while let Some((id, _err)) = self.coord.poll_conn_closed() {
      self.close_conn(id);
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
