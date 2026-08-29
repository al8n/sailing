//! [`CompioMultiStreamDriver`]: one thread-pinned task owning a [`MultiStreamCoordinator`], a
//! shared [`GroupEngine`], and a TCP listener, driving N co-located consensus groups over framed
//! reliable streams on compio.
//!
//! The single-group [`CompioStreamDriver`](crate::CompioStreamDriver) loop generalized to N
//! groups — the reactor multi driver's crank over compio idioms. The socket, accept, redial, and
//! command plumbing is the single compio driver's verbatim (one connection per peer carries
//! EVERY group's traffic; `Rc` factories and `lochan` channels keep the driver structurally
//! `!Send`, enforcing the construct-and-run-on-one-thread pinning), the per-iteration crank
//! order is the reactor multi loop's, and only the consensus-facing steps fan out — the one
//! armed deadline folds every group's earliest deadline quiesce-aware, a timer fire sweeps the
//! DUE groups, and the single `handle_storage` call becomes the storage crank: ONE engine flush
//! (the batched durability barrier all groups share) followed by every hosted group's completion
//! drain. Groups arrive via [`MultiCommand`] lifecycle commands — the driver binds EMPTY, and
//! the host identity latches from the first admitted group.
//!
//! Fault scope is PER GROUP for an apply/storage poison: it fails its own parked work with the typed
//! verdict and the co-located groups keep serving. The exception is a caught user-closure (completion
//! or factory) panic — UNATTRIBUTABLE, since the closure can alias any group's FSM — which fail-stops
//! the whole plane. Either way the driver survives: only driver-level events (shutdown, every handle
//! dropping) end `run()`.

use std::{
  cell::Cell,
  collections::{BTreeMap, BTreeSet},
  net::SocketAddr,
  rc::Rc,
  time::Duration,
};

use bytes::Bytes;
use compio::net::{TcpListener, TcpStream};
use sailing_proto::{
  Config, ConnId, Endpoint, Event, GroupControl, GroupEngine, GroupId, Index, InstallOutcome,
  Instant, MultiEngine, MultiStreamCoordinator, Now, ReadOnlyOption, RecordIo, StateMachine,
  StorageProgress, floor_admits,
};

use sailing_driver::{
  BoxedGroupFactory, GroupFactory, LifecycleEvent, MultiCommand, MultiHandle, Node, Status,
  jittered,
  shared::{
    CompletionOutcome, EngineGate, ParkedFailover, ParkedQuery, Pending, PendingAck, Routing,
  },
  validate_and_capture_eps,
};

use crate::{
  BindError, Clock, DriverConfig, DriverError, Monotonic,
  bridge::{BridgeInbound, BridgeOut, DialReady, bridge_read, bridge_write},
  driver::{
    map_propose_err, map_read_err, map_transfer_err,
    stream::{AcceptorFactory, Conn, ConnTask, DialerFactory, Redial, accept_conns},
  },
};

use super::{
  EngineMetrics, FloorSnapshot, GroupActivity, PairFloors, STORAGE_REDRIVES, SharedTails,
  blueprint_names, conf_names, group_idle, host_seed, map_merge_err, map_split_err, no_such_group,
  rejected, reshape_born_factory_config,
};

/// Backstop wake cadence while connections exist, exactly as on the reactor multi loop: with
/// ZERO hosted groups (or only timerless ones) the group-deadline fold can be `None` while
/// un-handshaked connections still need reaping.
const HOUSEKEEPING_INTERVAL: Duration = Duration::from_secs(1);

/// How long to wait before offering a refused one-shot relay cue to the lifecycle tail again (see
/// [`CompioMultiStreamDriver::rearm_cue_retry`]). Fixed, and deliberately coarse relative to the
/// crank: the retry is a peek and a refusable send, so a tail that stays full costs one cheap wake
/// per interval instead of a hot loop, while a tail that drains gets the cue within one interval.
const CUE_RETRY_INTERVAL: Duration = Duration::from_millis(50);

/// Per-iteration bound on each loop-top channel drain (the reactor multi loop's fairness note:
/// the loop-top drains make guaranteed progress independent of the biased select).
const IO_BUDGET: usize = 256;

/// A multi-group consensus host over framed reliable streams on compio. `G` is the group id; `R`
/// the record layer the factories build: `Labeled<Passthrough>` for plain TCP,
/// `Labeled<TlsRecords>` for TLS — the single driver's `Rc` factory pattern verbatim.
///
/// Construct AND run on the same thread (see the crate docs); the `Rc` factories and `lochan`
/// channels make this driver structurally `!Send`, enforcing that pinning — the whole HOST stays
/// serial because it is ONE task on ONE thread. Storage is an OWNED engine behind the
/// [`MultiEngine`] seam — `E` defaults to the in-memory [`GroupEngine`] every constructor builds
/// — and each crank runs one engine-wide barrier over every hosted group.
///
/// This driver is the compio/reactor multi PARITY point on one core: the multi crank, the
/// quiesce scheduler, the lifecycle mechanics, and the group factory are the reactor multi
/// driver's logic verbatim — a plane of the sharded host IS one of these drivers, so every
/// multi feature works per-plane unchanged.
///
/// The host is monotonic-only in v1: a group config that demands the LeaseGuard FAILOVER tier
/// (`bounded_clock_uncertainty`) is rejected loudly at admission, exactly as a single-group
/// `bind` under the default [`Monotonic`] clock rejects it — never a silently-inert tier. The
/// wall-clock generalization is a later seam.
///
/// # Durability is the ENGINE's, and the default engine is in-memory
///
/// The DEFAULT constructors build a [`GroupEngine`]: the shared IN-MEMORY reference engine, so a
/// process crash loses ALL consensus state under that default — for tests, single-process
/// deployments, and as the reference a persistent engine is validated against. Because a plane of
/// the sharded host IS one of these drivers, that default is per-plane too.
/// [`bind_with_engine`](Self::bind_with_engine) takes the caller's own [`MultiEngine`], which is
/// what the seam exists for. The recovery half is what is still missing: `restore_group` reconnects
/// a group within the SAME live process, not a recover-from-disk path. The lifecycle and event
/// tails are best-effort TELEMETRY for observability, never a correctness feed.
pub struct CompioMultiStreamDriver<G, I, F, R, E = GroupEngine<G, I>>
where
  I: sailing_proto::NodeId,
  F: StateMachine,
  R: RecordIo,
{
  coord: MultiStreamCoordinator<G, I, F, R>,
  engine: E,
  listener: TcpListener,
  clock: Clock,
  /// Byte cap on each group's failover inherited-read limbo scan.
  max_failover_limbo_bytes: usize,
  commands: flume::Receiver<MultiCommand<G, I, F>>,
  /// The single-group routing state instantiated PER GROUP: pending completions, parked queries,
  /// and the apply watermark are group-scoped, so one group's supersede/poison sweep never
  /// touches a co-located group's parked work.
  routing: BTreeMap<G, Routing<I, F::Response, F>>,
  /// The driver-owned, group-stamped events tail (the per-group `Routing`'s own tail is a stub —
  /// see `from_parts`).
  events_tx: flume::Sender<(G, Event<I, F::Response>)>,
  /// The dead-end sender each per-group `Routing` is built over: its receiver dropped at bind, so
  /// the single-group type's own best-effort forward is a no-op and the driver forwards the
  /// group-stamped copy itself.
  stub_events_tx: flume::Sender<Event<I, F::Response>>,
  /// The driver-owned lifecycle tail: unknown-group placement signals and removed-self
  /// notifications (see [`LifecycleEvent`]), bounded and best-effort like the events tail.
  lifecycle_tx: flume::Sender<LifecycleEvent<G, I>>,
  /// The registered auto-materialization hook (see [`GroupFactory`]), consulted on each polled
  /// unknown-group signal BEFORE the lifecycle tail. `None` = every signal falls through to the
  /// tail — the factory-less driver, byte for byte.
  factory: Option<BoxedGroupFactory<G, I, F>>,
  /// Relayed forks materialized THIS crank, awaiting the engine barrier: after `engine.flush()`
  /// covers their staged baselines, each lifts its parent's fork barrier and surfaces the typed
  /// `LifecycleEvent::SplitApplied` — registration, blob, and lineage become durable atomically
  /// before any lift.
  forks_pending_flush: Vec<(G, Index, G)>,
  /// Applied merges' `Event::Merged`, withheld from the application until the engine barrier
  /// (the fork queue's idiom): when the endpoint surfaces the event, the absorb's forced
  /// capture, the source's terminal floor, and its removal are only STAGED — forwarding it
  /// earlier would let a consumer retire the source's external state on the strength of a
  /// union a crash still loses (recovery re-parks the merge). Queuing arms `flush_pending`,
  /// so the next crank's `engine.flush()` covers those writes before any queued event drains.
  merges_pending_flush: Vec<(G, Event<I, F::Response>)>,
  /// Lifecycle replies withheld until the barrier that covers their commands' engine writes —
  /// the persist-before-reply queue stated on [`Self::handle_command`]. Drained by the storage
  /// crank immediately after `engine.flush()`, and again at teardown behind the final barrier,
  /// on the fork/merge queues' idiom.
  acks_pending_flush: Vec<PendingAck<I>>,
  storage_ready: flume::Receiver<()>,
  _storage_ready_keepalive: Option<flume::Sender<()>>,
  conns: BTreeMap<ConnId, Conn<I>>,
  redial: BTreeMap<I, Redial>,
  redial_wake: Option<std::time::Instant>,
  /// When to run the relay's cue pumps again after a bounded lifecycle tail refused a one-shot
  /// signal (see [`rearm_cue_retry`](Self::rearm_cue_retry)). `None` when nothing is queued.
  cue_retry_at: Option<std::time::Instant>,
  peers: Vec<Node<I, SocketAddr>>,
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
  /// Latched when every storage-ready sender has dropped (see the single driver's twin field):
  /// a dead channel would win the select forever and hot-spin the loop.
  storage_closed: bool,
  /// Per-group leadership as of the END of the last pass — the group-scoped supersede backstop
  /// (the single driver's `was_leader` bool, one edge-detect per hosted group).
  was_leader: BTreeMap<G, bool>,
  /// Whether any dispatch since the last barrier may have STAGED engine work. The engine's
  /// `has_pending` reports READY completions only (staged work is invisible BY DESIGN), so this
  /// flag stands in for the single loop's live-store `has_pending` check when deriving the
  /// immediate storage-redrive deadline; every staging dispatch sets it and each crank recomputes
  /// it (a crank that released nothing proves quiescence).
  flush_pending: bool,
  /// Published engine counters (see [`EngineMetrics`]).
  metrics: EngineMetrics,
  /// Groups whose consensus deadlines the driver neither arms nor sweeps (see
  /// [`Self::quiesce_sweep`]). Everything else — inbound dispatch, output drains, the storage
  /// crank, client commands — flows normally for a quiesced group.
  quiesced: BTreeSet<G>,
  /// Whether ANY connection was lost in the current loop pass (set by [`Self::wake_all`]).
  /// A queued `GroupControl::Quiesce` decoded earlier in the same pass predates the loss, and
  /// honoring it would consume the liveness signal — the control drain drops `Quiesce` while this
  /// is set, so a same-pass close always wins. Cleared at the top of each pass.
  link_lost_in_pass: bool,
  /// Leader groups marked quiescing whose FLAGGED beat has not yet been stamped: they keep being
  /// swept until the coordinator consumes the intent, then move into `quiesced`.
  quiesce_pending: BTreeSet<G>,
  /// Per-group observed consensus state + the instant it last changed — the idle clock feeding
  /// quiesce eligibility (see [`Self::quiesce_sweep`] for why observation, not dispatch, is the
  /// activity signal).
  activity: BTreeMap<G, GroupActivity>,
  /// Each group's election timeout, captured at admission — the quiesce-eligibility idle window.
  election: BTreeMap<G, Duration>,
  teardown_tx: Option<futures_channel::oneshot::Sender<()>>,
}

/// The constructors that name the engine CONCRETELY: a defaulted type parameter does not infer in
/// expression position, so binding these to [`GroupEngine`] is what keeps every call site
/// turbofish-free. The engine-taking form and everything a bound host does live on the generic
/// block below.
impl<G, I, F, R> CompioMultiStreamDriver<G, I, F, R, GroupEngine<G, I>>
where
  G: GroupId + Send,
  I: sailing_proto::NodeId + Send,
  F: StateMachine,
  F::Command: sailing_proto::Data + Send,
  F::Snapshot: sailing_proto::Data,
  F::Response: Clone + Send,
  F::Error: core::error::Error,
  R: RecordIo,
{
  /// Bind the listener and build an EMPTY host plus its [`MultiHandle`]: no group exists yet —
  /// they arrive via [`MultiHandle::create_group`] / [`MultiHandle::restore_group`] commands, and
  /// the host identity latches from the first admitted group's config. The configured peers are
  /// dialed at `run()` start and redialed exactly as on the single-group driver (the shared
  /// connections are group-agnostic).
  pub async fn bind(
    addr: SocketAddr,
    peers: Vec<Node<I, SocketAddr>>,
    dialer: DialerFactory<I, R>,
    acceptor: AcceptorFactory<R>,
    driver_cfg: DriverConfig,
  ) -> Result<(Self, MultiHandle<G, I, F>), BindError> {
    // Validate BEFORE the tails are sized from the config's caps (the same up-front rejection
    // the single-group bind runs before any channel is built).
    driver_cfg.validate()?;
    let tails = SharedTails::new(&driver_cfg);
    Self::bind_with_tails_in(
      addr,
      peers,
      dialer,
      acceptor,
      driver_cfg,
      tails,
      GroupEngine::new(),
    )
    .await
  }
}

/// Everything a bound host does, over ANY engine behind the [`MultiEngine`] seam.
impl<G, I, F, R, E> CompioMultiStreamDriver<G, I, F, R, E>
where
  G: GroupId + Send,
  I: sailing_proto::NodeId + Send,
  F: StateMachine,
  F::Command: sailing_proto::Data + Send,
  F::Snapshot: sailing_proto::Data,
  F::Response: Clone + Send,
  F::Error: core::error::Error,
  R: RecordIo,
  E: MultiEngine<G, I>,
{
  /// [`bind`](Self::bind) over a CALLER-SUPPLIED engine — the seam a durable engine enters this
  /// host through. Identical in every other respect: the host binds EMPTY, groups arrive by
  /// lifecycle command, and each crank runs one barrier over the engine handed in here. The
  /// engine MOVES into the driver and is never shared — this host drives it from its own single
  /// task, which is what lets a group's `(log, stable)` pair be lent mutably across a whole
  /// drive call.
  pub async fn bind_with_engine(
    addr: SocketAddr,
    peers: Vec<Node<I, SocketAddr>>,
    dialer: DialerFactory<I, R>,
    acceptor: AcceptorFactory<R>,
    driver_cfg: DriverConfig,
    engine: E,
  ) -> Result<(Self, MultiHandle<G, I, F>), BindError> {
    // Validate BEFORE the tails are sized from the config's caps, exactly as `bind` does.
    driver_cfg.validate()?;
    let tails = SharedTails::new(&driver_cfg);
    Self::bind_with_tails_in(addr, peers, dialer, acceptor, driver_cfg, tails, engine).await
  }

  /// Bind with CALLER-SUPPLIED fan-in tails and a CALLER-SUPPLIED engine — the sharded host's
  /// per-plane constructor. K planes each get a clone of ONE [`SharedTails`], so their events,
  /// lifecycle signals, and in-flight budget fan into a single set by construction, while each
  /// gets its OWN engine (a shared one would put two cores behind one barrier). The returned
  /// [`MultiHandle`] is standard — it holds clones of the shared receivers and the shared budget.
  /// Both public constructors build their own tails and delegate here.
  #[allow(clippy::too_many_arguments)]
  pub(crate) async fn bind_with_tails_in(
    addr: SocketAddr,
    peers: Vec<Node<I, SocketAddr>>,
    dialer: DialerFactory<I, R>,
    acceptor: AcceptorFactory<R>,
    driver_cfg: DriverConfig,
    tails: SharedTails<G, I, F>,
    engine: E,
  ) -> Result<(Self, MultiHandle<G, I, F>), BindError> {
    driver_cfg.validate()?;
    let listener = TcpListener::bind(addr).await?;
    let coord = MultiStreamCoordinator::new();
    Ok(Self::from_parts(
      coord, listener, peers, dialer, acceptor, driver_cfg, tails, engine,
    ))
  }

  /// Assemble the driver + [`MultiHandle`] from the bound listener (the single driver's
  /// `from_parts`, with the per-group maps and the shared engine in place of one group's stores,
  /// and the fan-in tails injected rather than built).
  #[allow(clippy::too_many_arguments)]
  fn from_parts(
    coord: MultiStreamCoordinator<G, I, F, R>,
    listener: TcpListener,
    peers: Vec<Node<I, SocketAddr>>,
    dialer: DialerFactory<I, R>,
    acceptor: AcceptorFactory<R>,
    driver_cfg: DriverConfig,
    tails: SharedTails<G, I, F>,
    engine: E,
  ) -> (Self, MultiHandle<G, I, F>) {
    let SharedTails {
      events_tx,
      events_rx,
      lifecycle_tx,
      lifecycle_rx,
      budget,
    } = tails;
    // Unbounded: the submit BUDGET is the binding bound on in-flight operations, so the channel
    // carries no cap of its own and shutdown can never block on a full queue.
    let (cmd_tx, cmd_rx) = flume::unbounded();
    let (teardown_tx, teardown_rx) = futures_channel::oneshot::channel();
    let handle = MultiHandle::new(cmd_tx, events_rx, lifecycle_rx, budget, teardown_rx);

    // Each per-group `Routing` needs an events sender, but the GROUP-STAMPED tail is driver-owned
    // (the shared single-group type cannot stamp a group id): the stub's receiver is dropped here,
    // so `route_event`'s own best-effort forward is a no-op and `pump` forwards the stamped copy.
    let (stub_events_tx, _) = flume::bounded(0);

    let (storage_ready, keepalive) = match driver_cfg.storage_ready {
      Some(rx) => (rx, None),
      None => {
        let (tx, rx) = flume::bounded(1);
        (rx, Some(tx))
      }
    };
    let (inbound_tx, inbound_rx) = lochan::mpsc::bounded(driver_cfg.inbound_cap);
    let (dial_ready_tx, dial_ready_rx) = flume::unbounded();
    let max_conns = driver_cfg.max_conns.max(2 * peers.len());

    (
      Self {
        coord,
        engine,
        listener,
        clock: Clock::new(None, Monotonic),
        max_failover_limbo_bytes: driver_cfg.max_failover_limbo_bytes,
        commands: cmd_rx,
        routing: BTreeMap::new(),
        events_tx,
        stub_events_tx,
        lifecycle_tx,
        factory: None,
        forks_pending_flush: Vec::new(),
        merges_pending_flush: Vec::new(),
        acks_pending_flush: Vec::new(),
        storage_ready,
        _storage_ready_keepalive: keepalive,
        conns: BTreeMap::new(),
        redial: BTreeMap::new(),
        redial_wake: None,
        cue_retry_at: None,
        peers,
        dialer,
        acceptor,
        inbound_tx,
        inbound_rx,
        dial_ready_tx,
        dial_ready_rx,
        cmd_budget: driver_cfg.cmd_budget.max(1),
        accept_cap: driver_cfg.accept_cap,
        max_outbound_backlog: driver_cfg.max_outbound_backlog,
        max_conns,
        redial_base: driver_cfg.redial_base,
        redial_cap: driver_cfg.redial_cap,
        storage_closed: false,
        was_leader: BTreeMap::new(),
        flush_pending: false,
        metrics: EngineMetrics::default(),
        quiesced: BTreeSet::new(),
        link_lost_in_pass: false,
        quiesce_pending: BTreeSet::new(),
        activity: BTreeMap::new(),
        election: BTreeMap::new(),
        teardown_tx: Some(teardown_tx),
      },
      handle,
    )
  }

  /// The shared engine's cross-thread batching counters — clone BEFORE spawning `run()`.
  #[must_use]
  pub fn engine_metrics(&self) -> EngineMetrics {
    self.metrics.clone()
  }

  /// Cap the engine's chunked-snapshot staging buffers (the engine-wide knob, applied to this
  /// host's engine). Set between `bind` and `run()`.
  #[must_use]
  pub fn with_snapshot_staging_cap(mut self, cap: usize) -> Self {
    self.engine.set_snapshot_staging_cap(cap);
    self
  }

  /// Register a [`GroupFactory`] — the auto-materialization hook consulted on every polled
  /// unknown-group signal (see the trait for the full admission-edge contract; assemble one
  /// from its two phases as closures with [`factory_fn`](sailing_driver::factory_fn)). The
  /// driver builds the state machine ([`GroupFactory::build`]) only AFTER the sender gate
  /// admitted the materialized blueprint. Set between `bind` and `run()`; without it the
  /// driver forwards every signal to the lifecycle tail exactly as before. `Send + 'static`
  /// because a factory is typically CONSTRUCTED off-thread and shipped to the driver's thread
  /// (the sharded host builds one per plane on the spawning thread); it is only ever CALLED
  /// from the one driver task.
  #[must_use]
  pub fn with_group_factory<Fac>(mut self, factory: Fac) -> Self
  where
    Fac: GroupFactory<G, I, F> + Send + 'static,
  {
    self.factory = Some(Box::new(factory));
    self
  }

  /// Register an already-boxed [`GroupFactory`] — the dynamic sibling of
  /// [`with_group_factory`](Self::with_group_factory) for callers that select a factory at
  /// runtime (the sharded host's per-plane factory slot hands each plane its own boxed
  /// instance).
  #[must_use]
  pub fn with_boxed_group_factory(mut self, factory: BoxedGroupFactory<G, I, F>) -> Self {
    self.factory = Some(factory);
    self
  }

  /// Drive every hosted group until shutdown (or until every `MultiHandle` clone has dropped and
  /// the buffered commands drained). Per-iteration crank order is the reactor multi loop's, with
  /// the single compio driver's socket/timer plumbing.
  pub async fn run(mut self) {
    use futures_util::{FutureExt, select_biased};

    let (accept_tx, mut accept_rx) =
      lochan::mpsc::bounded::<(TcpStream, SocketAddr)>(self.accept_cap);
    let accept_task = compio::runtime::spawn(accept_conns(self.listener.clone(), accept_tx));

    // The first reconciler pass dials the full configured mesh (nothing is bound yet).
    let now = self.clock.now();
    self.reconcile_peer_links(now.mono());
    self.pump(now).await;

    loop {
      let now = self.clock.now();
      self.link_lost_in_pass = false;

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

      // Fairness across the biased select: bounded loop-top drains (the reactor multi loop's).
      for _ in 0..IO_BUDGET {
        match self.inbound_rx.try_recv() {
          Ok(inbound) => self.handle_inbound(now, inbound),
          Err(_) => break,
        }
      }
      for _ in 0..IO_BUDGET {
        match self.dial_ready_rx.try_recv() {
          Ok(ready) => self.handle_dial_ready(ready),
          Err(_) => break,
        }
      }

      // Fire an already-due deadline before the select; the housekeeping condition mirrors the
      // reactor multi loop (a timerless host with live connections still reaps handshakes). The
      // armed deadline is the QUIESCE-AWARE fold ([`Self::armed_deadline`]), never the
      // all-groups `poll_timeout`.
      let armed = self.armed_deadline();
      if armed.is_some_and(|d| d <= self.clock.mono())
        || (armed.is_none() && !self.conns.is_empty())
      {
        self.fire_timeouts(now);
      }
      self.reconcile_peer_links(now.mono());
      self.pump(now).await;
      self.quiesce_sweep();

      let housekeeping =
        (!self.conns.is_empty()).then(|| std::time::Instant::now() + HOUSEKEEPING_INTERVAL);
      // The multi analog of the single loop's live-store `has_pending` deadline: staged engine
      // work is invisible until the barrier (READY-only `has_pending` by design), so the
      // pending-flush flag — set by every staging dispatch, recomputed by every crank — derives
      // the immediate re-drive wake instead.
      let storage_redrive = self.flush_pending.then(std::time::Instant::now);
      let deadline = self
        .armed_deadline()
        .map(|d| self.clock.to_std(d))
        .into_iter()
        .chain(self.redial_wake)
        .chain(self.cue_retry_at)
        .chain(housekeeping)
        .chain(storage_redrive)
        .min()
        .unwrap_or_else(|| std::time::Instant::now() + Duration::from_secs(3600));

      // A transient select result, matched and consumed on the stack within this iteration —
      // never stored — so the variant-size spread (a lifecycle command's Config + fork blob vs
      // a unit timer wake) costs nothing; boxing would add an allocation per hot-path command.
      #[allow(clippy::large_enum_variant)]
      enum Wake<G, I, F: StateMachine> {
        Inbound(BridgeInbound),
        Accepted(TcpStream),
        DialReady(DialReady),
        Timer,
        Command(Option<MultiCommand<G, I, F>>),
        Storage,
        StorageClosed,
      }
      let wake = {
        // `accept_rx` is a run-loop local, so its lochan `recv` (`&mut self`) is pre-pinnable;
        // `inbound_rx` is recv'd INLINE in its arm so the `&mut self.inbound_rx` borrow ends the
        // instant the select resolves (see the single driver's borrow notes).
        let accept_fut = accept_rx.recv();
        let dial_fut = self.dial_ready_rx.recv_async().fuse();
        let timer_fut = compio::time::sleep_until(deadline).fuse();
        // Parked once every notifier sender has dropped (the `storage_closed` latch): a dead
        // channel resolves immediately forever and would hot-spin the loop.
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
        futures_util::pin_mut!(accept_fut, dial_fut, timer_fut, storage_fut, cmd_fut);

        select_biased! {
          got = self.inbound_rx.recv() => Wake::Inbound(got.expect("inbound_tx outlives the loop")),
          got = accept_fut => {
            let (s, _from) = got.expect("accept task outlives the loop");
            Wake::Accepted(s)
          }
          got = dial_fut => Wake::DialReady(got.expect("dial_ready_tx outlives the loop")),
          _ = timer_fut => Wake::Timer,
          cmd = cmd_fut => Wake::Command(cmd.ok()),
          got = storage_fut => {
            if got.is_err() { Wake::StorageClosed } else { Wake::Storage }
          }
        }
      };
      // Coalesce storage-ready wakes to a bounded count (the reactor multi loop's).
      for _ in 0..IO_BUDGET {
        if self.storage_ready.try_recv().is_err() {
          break;
        }
      }

      let now = self.clock.now();
      match wake {
        Wake::Inbound(inbound) => self.handle_inbound(now, inbound),
        Wake::Accepted(socket) => self.handle_accept(now.mono(), socket),
        Wake::DialReady(ready) => self.handle_dial_ready(ready),
        Wake::Timer => self.fire_timeouts(now),
        Wake::Command(Some(cmd)) => {
          if self.handle_command(now, cmd) {
            break;
          }
        }
        Wake::Command(None) => break,
        Wake::Storage => {}
        Wake::StorageClosed => self.storage_closed = true,
      }
      self.storage_crank(now);
      self.pump(now).await;
    }

    // THE TEARDOWN BARRIER. Every exit path lands here — a `Shutdown`, a disconnected command
    // channel, the last handle dropped — and any of them can be reached with engine writes still
    // STAGED: a removal's floor above all, whose loss lets a retired incarnation return on a
    // durable engine's next boot. The bounded command drain makes that ordinary, not exotic: a
    // removal and the `Shutdown` behind it are handled in the SAME pass, and the loop exits before
    // the crank that would have flushed. One final barrier makes those writes durable; only then
    // do the verdicts they gate become observable. Nothing past this point stages engine work.
    if self.engine.has_staged() {
      self.engine.flush();
    }
    for ack in self.acks_pending_flush.drain(..) {
      ack.send();
    }

    // Teardown. Classify each group's fail-stop FIRST (a poisoned group's parked work fails with
    // the typed verdict even when a Shutdown raced the poisoning crank), then the ShuttingDown
    // sweep — a no-op on the already-emptied groups.
    let hosted: Vec<G> = self.routing.keys().map(|g| g.cheap_clone()).collect();
    for g in &hosted {
      if self.coord.group(g).is_some_and(|ep| ep.is_poisoned())
        && let Some(routing) = self.routing.get_mut(g)
      {
        routing.fail_all(&DriverError::Poisoned);
      }
    }
    for routing in self.routing.values_mut() {
      routing.fail_all(&DriverError::ShuttingDown);
      // THE ONE SOUND DISCARD, and it turns on the PLANE's lifecycle, not on any group's: the run loop
      // has EXITED. This sweep has failed the group's parked work with its typed verdict, no crank
      // follows, and no group outlives the loop — so a swept completion's dropped guard has no serve
      // left to corrupt anywhere on this plane and there is nothing to fail-stop. (On a LIVE plane the
      // same latch is plane-fatal: siblings keep serving, and the tear could be in any of them.) Drain
      // the latch the sweep may have just set; a dropped `Routing` still carrying one is the
      // un-routed-verdict bug its `Drop` asserts against.
      let _ = routing.take_completion_panicked();
    }
    drop(accept_task);
    drop(accept_rx);
    self.conns.clear();
    while let Ok(cmd) = self.commands.try_recv() {
      drop(cmd);
    }
    drop(self.commands);
    // The fd-release point, then the teardown signal — the single driver's ordering verbatim
    // (explicit AFTER `close()` rather than a field drop, whose ordering against the close
    // await is not guaranteed).
    let _ = self.listener.close().await;
    if let Some(tx) = self.teardown_tx.take() {
      let _ = tx.send(());
    }
  }

  /// Fire the transport's shared housekeeping (handshake reaping), then every DUE group's
  /// consensus timers. This is the v1 aggregate-timer dispatch: ONE armed deadline, an O(groups)
  /// due sweep on fire; an indexed timing wheel over [`MultiStreamCoordinator::deadlines`] is the
  /// scale refinement seam.
  fn fire_timeouts(&mut self, now: Now) {
    self.coord.handle_transport_timeout(now);
    let mono = self.clock.mono();
    // A QUIESCED group is skipped: its deadline is stale by design (frozen, not cancelled — the
    // wake path re-admits it to the fold, where being long past due makes it fire immediately).
    let due: Vec<G> = self
      .coord
      .deadlines()
      .filter(|(g, d)| *d <= mono && !self.quiesced.contains(g))
      .map(|(g, _)| g)
      .collect();
    for g in &due {
      if let Some((log, stable)) = self.engine.stores(g) {
        let _ = self.coord.handle_timeout(g, now, log, stable);
      }
    }
    if !due.is_empty() {
      // A fired election/heartbeat can stage vote/append writes: barrier them this crank.
      self.flush_pending = true;
    }
  }

  /// Install the container's committed, relay-ready forks — BEFORE the barrier below, so the SAME
  /// crank's `engine.flush()` covers every staged baseline before `pump` can transmit anything for
  /// the child (a child that can solicit peers is therefore always locally blob-durable first; the
  /// drain also front-runs the factory drain, so a local fork wins any same-id solicitation race).
  ///
  /// THE FORK NEVER LEAVES THE CONTAINER. This loop peeks at a decision, validates the config the
  /// container would boot with, and asks the container to install in place; the partition itself
  /// stays in the staged queue until the install's own pop, so no step here can lose, mis-target
  /// or double-present it. The driver owns only the seam: the two lineage records, the child's
  /// election timer, its routing, and the durability barrier the pending-flush entry rides.
  ///
  /// The outcomes divide the way the container's verdicts do. `Refused` is a DELIBERATE
  /// abandonment the container already resolved and queued — a floored or terminally-consumed
  /// child id, a verdict no host state will change — so this loop does nothing and comes round
  /// again; the child still reaches this host by the ordinary lifecycle paths (restore over its
  /// own storage, solicitation → factory/embedder → snapshot from a live member, whose own blob
  /// went durable before it could transmit). `Held` means the child id is merely SPOKEN FOR —
  /// occupied engine stores, a tombstone mid-rejoin, a hosted twin — so the fork stays staged with
  /// its blob, its fence and its reservation intact, and the conflict pump below relays the
  /// one-shot [`LifecycleEvent::SplitConflict`] to the embedder, consumed from the coordinator only
  /// once the bounded lifecycle tail accepts it so backpressure defers the cue instead of erasing
  /// it. `NotYieldable` and `Empty` end the pass.
  fn fork_drain(&mut self, now: Now) {
    loop {
      // The view borrows the container, so it is read out and dropped before the install: the
      // decision travels, the partition does not.
      let peeked = {
        let gate = EngineGate::new(&self.engine);
        self.coord.peek_yieldable_fork(&gate).map(|fork| {
          (
            fork.parent().cheap_clone(),
            fork.child().cheap_clone(),
            fork.config().clone(),
          )
        })
      };
      let Some((peeked_parent, child, config)) = peeked else {
        break;
      };
      // FAIL-CLOSED FLOOR, validating the EXACT config the container will boot the child with: the
      // container derives it internally from the fork's own committed config, so this reads the
      // same derivation rather than a locally-shaped one — a driver that transformed the config
      // here could no longer influence what installs. The assert is load-bearing rather than
      // decorative: the container ran this same validation before it offered the fork, and a fork
      // child cannot reach the wall-clock leg at all (`Endpoint::config` has no mutation path, and
      // the parent passed the identical check at admission on this driver). A failure means one of
      // those two facts stopped being true, which must be loud in a test run rather than a
      // silently dropped partition.
      //
      // AND IT STOPS THE PASS RATHER THAN SKIPPING THE FORK. Nothing was consumed, so continuing
      // would re-peek this identical fork forever; breaking leaves it STAGED and still fenced,
      // which is the correct fail-closed state — the partition is held, not lost.
      if let Err(e) =
        validate_and_capture_eps::<I, Monotonic>(&sailing_proto::reshape_born_prevention(config))
      {
        debug_assert!(false, "a staged fork's config failed validation: {e}");
        break;
      }
      let seed = host_seed(self.coord.host_id());
      // The install decides on the PAIR the peek named — never on whatever a second drain would
      // reach, which with two parks releasing on one crank is a different parent entirely.
      match self
        .coord
        .install_yieldable_fork(&peeked_parent, &child, &mut self.engine, now, seed)
      {
        InstallOutcome::Installed {
          parent,
          child,
          child_gen,
          parent_gen_after,
          split_index,
          config,
        } => {
          // Both lineage records advance with the child's registration, all behind the ONE barrier
          // the pending-flush entry below waits on.
          self.engine.set_group_gen(&parent, parent_gen_after);
          self.engine.set_group_gen(&child, child_gen);
          self.election.insert(
            child.cheap_clone(),
            <sailing_proto::Config<I>>::election_timeout(&config),
          );
          self.admit_group(child.cheap_clone());
          self.flush_pending = true;
          self.forks_pending_flush.push((parent, split_index, child));
        }
        // The container resolved this fork's barrier and queued the refusal itself; the pumps
        // below deliver it. Come round for the next staged fork.
        InstallOutcome::Refused => {}
        InstallOutcome::Held | InstallOutcome::NotYieldable | InstallOutcome::Empty => break,
      }
    }
    // The relay's own guard advances: the removal-time abandonment defers a fork it condemned
    // below the head, and the drain above consumes it when it reaches the front — so the advance
    // that keeps a crash-replay from re-staging it is queued HERE, not at the removal. Mirrored on
    // the same barrier as every other fork write this pass made.
    while let Some((parent, generation)) = self.coord.poll_relay_guard_advance() {
      self.engine.set_group_gen(&parent, generation);
      self.flush_pending = true;
    }
    // DELIVERED-BEFORE-CONSUMED: unlike its best-effort siblings, the conflict signal is
    // one-shot per park episode, so popping it ahead of a refusable send would let a
    // momentarily-full tail erase the embedder's only cue while the parent fence stands and
    // the child id stays reserved. Consume only on acceptance; on a full tail the signal
    // stays queued at the coordinator (the fork stays parked) and this drain retries next
    // crank — a park that resolves first purges it there, so nothing stale ever surfaces.
    while let Some((parent, child)) = self.coord.peek_split_conflict() {
      if self
        .lifecycle_tx
        .try_send(LifecycleEvent::SplitConflict { parent, child })
        .is_err()
      {
        break;
      }
      let _ = self.coord.poll_split_conflict();
    }
    // The relay's DELIBERATE abandonments, on the same discipline: a refusal is one-shot too, and
    // it is the embedder's only record that a promised child will never arrive by this route.
    while let Some((parent, child)) = self.coord.peek_split_refusal() {
      if self
        .lifecycle_tx
        .try_send(LifecycleEvent::SplitRefused { parent, child })
        .is_err()
      {
        break;
      }
      let _ = self.coord.poll_split_refusal();
    }
    // RE-ARM, on the cue's own bounded schedule — never on `flush_pending`, which would couple an
    // undelivered signal to the storage barrier and spin (see
    // [`rearm_cue_retry`](Self::rearm_cue_retry)).
    self.rearm_cue_retry();
    // A fail-stopped group surfaces once on the same best-effort tail; a full tail drops the
    // observation and the embedder still finds the poison by direct inspection.
    while let Some(group) = self.coord.poll_poisoned() {
      let _ = self
        .lifecycle_tx
        .try_send(LifecycleEvent::Poisoned { group });
    }
    // Held-merge observations deliberately do NOT publish here: this drain runs BEFORE the
    // service pass, and a hold can resolve OUTSIDE any service crank (a cure blob adopts at
    // receipt, an admission drifts the cause) — publishing first would ship an observation the
    // end-of-crank retirement is about to prove stale. The post-service drain, which reads
    // strictly after that retirement, is the ONLY publication point.
  }

  /// The per-crank storage step, replacing the single loop's one `handle_storage` call:
  /// (a) ONE engine flush — the batched in-memory visibility barrier every hosted group's staged
  /// writes share, the cross-group batching point — gated on the pending-flush flag so an idle pass
  /// never burns a barrier on a knowably-empty batch; then (b) every hosted group's completion drain,
  /// re-driving a budget-cut group at most [`STORAGE_REDRIVES`] times (the remainder rides the
  /// next crank). v1 deliberately iterates ALL hosted groups — an idle group's drain is a cheap
  /// no-op poll — with dirty-set tracking as the scale refinement.
  /// Re-arm — or disarm — the relay's cue-retry deadline from what the pumps left behind.
  ///
  /// A one-shot cue the bounded lifecycle tail refused has no other prompt: the container has no
  /// timer, and a quiet peerless parent generates no traffic. It gets its OWN bounded schedule
  /// rather than riding `flush_pending`, and the distinction is the whole point — `flush_pending`
  /// means "staged storage work is owed a barrier", so folding an undelivered cue into it would
  /// make every crank both re-drive immediately AND run `engine.flush()`, and a permanently full
  /// or disconnected tail would spin that loop forever: a burned core here, an fsync storm on a
  /// durable engine. This wake runs the pumps again and nothing else; the barrier still fires only
  /// for real staged work.
  ///
  /// FIXED interval, not exponential: the retry is a peek and a refusable send, and a cue's
  /// delivery latency must stay bounded rather than growing without limit while the embedder is
  /// briefly behind. It cannot starve the cue either — the retry re-arms for as long as the signal
  /// is queued, and a parent holding one is quiesce-INELIGIBLE
  /// ([`Self::owes_merge_cure_ticks`] reads `fork_relay_pending`), so the group keeps its own
  /// timers armed and the loop keeps turning independently of this deadline.
  fn rearm_cue_retry(&mut self) {
    let pending =
      self.coord.peek_split_conflict().is_some() || self.coord.peek_split_refusal().is_some();
    self.cue_retry_at = pending.then(|| std::time::Instant::now() + CUE_RETRY_INTERVAL);
  }

  fn storage_crank(&mut self, now: Now) {
    self.fork_drain(now);
    let flushed = self.flush_pending;
    if flushed {
      self.engine.flush();
    }
    // Registration + blob + lineage became durable in the flush above (ONE barrier): only now
    // may each fork's parent barrier lift and the typed lifecycle event fire.
    for (parent, split_index, child) in self.forks_pending_flush.drain(..) {
      self.coord.lift_fork_barrier(&parent, split_index);
      let _ = self
        .lifecycle_tx
        .try_send(LifecycleEvent::SplitApplied { parent, child });
    }
    // The fork lift's merge twin: a queued `Merged` drains only behind a barrier that ran, and
    // queuing armed `flush_pending`, so a non-empty queue always finds one — the absorb's
    // capture, the terminal floor, and the source's removal are covered before the application
    // can act on the union. `route_event` moves no waiter or watermark for `Merged`; both the
    // per-group copy and the stamped tail copy are the deferred app-visible surfaces.
    // PERSIST-BEFORE-REPLY: a lifecycle verdict whose command staged engine state becomes
    // observable only behind the barrier that made that state durable. Queuing armed
    // `flush_pending`, so a non-empty queue always finds a barrier here.
    if flushed {
      for ack in self.acks_pending_flush.drain(..) {
        ack.send();
      }
    }
    if flushed {
      for (g, ev) in self.merges_pending_flush.drain(..) {
        if let Some(routing) = self.routing.get_mut(&g) {
          let _ = routing.route_event(ev.clone());
        }
        let _ = self.events_tx.try_send((g, ev));
      }
    }
    let mut more = false;
    let hosted: Vec<G> = self.engine.group_ids().map(|g| g.cheap_clone()).collect();
    for g in &hosted {
      let mut redrives = 0;
      while let Some((log, stable)) = self.engine.stores(g) {
        match self.coord.handle_storage(g, now, log, stable) {
          Some(StorageProgress::MorePending) => {
            redrives += 1;
            if redrives >= STORAGE_REDRIVES {
              more = true;
              break;
            }
          }
          _ => break,
        }
      }
    }
    // Completions drained above can STAGE follow-up writes (a commit's HardState write submitted
    // by the core's storage tail) that stay invisible to has_pending until the next barrier — so
    // the re-arm predicate is the engine's own staged-work signal, measured AFTER the drains: it
    // is exact, where a release-count inference would miss a write staged by a crank whose
    // barrier released nothing.
    // Resolve every parked merge that local facts now decide, then fold each ABSORB's storage
    // half: the terminal floor and the source teardown ride the SAME crank as the absorb and
    // its forced capture, so the next barrier lands them together — a crash either rewinds to
    // re-park or restarts the target past the absorb, never in between. (The coordinator
    // already tombstoned the source, so stragglers drop at the wire; an Aborted resolution
    // needs nothing here — the source group is still live.)
    let resolutions = self.coord.service_merge_applies(now, &mut self.engine);
    if !resolutions.is_empty() {
      self.flush_pending = true;
    }
    for r in resolutions {
      // `Merged` (absorbed) and `Retired` (a hosted frozen husk dissolved off its terminal floor)
      // fold the SAME source half — floor terminally, drop the stores and engine record, and tear
      // down the routing. A `Retired` has no capture, but the floor re-write is STILL mandatory: a
      // `FloorStore` may serve a STAGED floor, and dropping the stores off it before the flush would
      // re-admit the id below its gen. `Aborted` needs nothing — the source is still live.
      //
      // `CaptureFailed` is the crucial ASYMMETRY: the absorb consumed the source endpoint but the
      // union could not be made durable (the target refused the absorb, or its forced capture
      // faulted), so the target is POISONED and the source's stores and floor MUST be PRESERVED —
      // they hold the union's only copy, and a restart re-parks the merge against the restored source.
      // Fail the source's stranded routing with the typed `Poisoned` (its callers park on the vanished
      // endpoint's oneshots and would otherwise HANG FOREVER — the events that would have answered
      // them vanished with the endpoint), drain its completion latch to fail-stop the absorbing target
      // when it fired, and surface a lifecycle signal so the embedder restarts.
      //
      // A completion(-drop) panic in the teardown sweep below is UNATTRIBUTABLE: the source's parked
      // completion captured arbitrary state whose `Drop` can alias ANY hosted group's replicated FSM,
      // not only the absorbing target's, so the tear could be anywhere on the plane. The reaction is
      // the same plane-fatal fail-stop the refusals raise — there is no group to name — so the target
      // is `..`-discarded and the `Merged`/`Retired` split no longer carries it.
      let source = match r {
        sailing_proto::MergeResolution::Merged { source, .. } => source,
        sailing_proto::MergeResolution::Retired { source } => source,
        sailing_proto::MergeResolution::Aborted { .. } => continue,
        sailing_proto::MergeResolution::Absorbed { source, target } => {
          // The fence-deferred absorb: the union is applied and serving in the target, the source
          // endpoint is gone, and the source's stores and floor stay DELIBERATELY untouched — they
          // remain the union's only restart derivation until the capture debt discharges into the
          // later `Merged` (whose arm then floors and tears down as usual). Fail the source's
          // stranded routing typed — its callers would hang forever on the vanished endpoint's
          // oneshots — and drain the completion latch exactly as the other consuming arms do.
          self.was_leader.remove(&source);
          self.quiesced.remove(&source);
          self.quiesce_pending.remove(&source);
          self.activity.remove(&source);
          self.election.remove(&source);
          if let Some(mut routing) = self.routing.remove(&source) {
            routing.fail_all(&DriverError::SourceAbsorbed);
            // The source's DYING latch fires PLANE-FATAL, as on every consuming arm: the swept
            // completion's dropped guard could have torn ANY hosted group's FSM.
            if routing.take_completion_panicked() {
              self.coord.fail_stop_plane_unattributable_panic();
            }
          }
          let _ = self
            .lifecycle_tx
            .try_send(LifecycleEvent::MergeAbsorbed { source, target });
          continue;
        }
        sailing_proto::MergeResolution::CaptureFailed { source, target } => {
          self.was_leader.remove(&source);
          self.quiesced.remove(&source);
          self.quiesce_pending.remove(&source);
          self.activity.remove(&source);
          self.election.remove(&source);
          if let Some(mut routing) = self.routing.remove(&source) {
            routing.fail_all(&DriverError::Poisoned);
            // The source's DYING latch fires PLANE-FATAL: the swept completion's dropped guard could
            // have torn ANY hosted group's FSM, not only the absorbing target's, so every hosted group
            // fail-stops — the same UNATTRIBUTABLE reaction the refusals raise.
            if routing.take_completion_panicked() {
              self.coord.fail_stop_plane_unattributable_panic();
            }
          }
          let _ = self
            .lifecycle_tx
            .try_send(LifecycleEvent::MergeCaptureFailed { source, target });
          continue;
        }
      };
      self
        .engine
        .set_group_floor(&source, sailing_proto::MERGED_FLOOR);
      self.engine.remove_group(&source);
      self.was_leader.remove(&source);
      self.quiesced.remove(&source);
      self.quiesce_pending.remove(&source);
      self.activity.remove(&source);
      self.election.remove(&source);
      if let Some(mut routing) = self.routing.remove(&source) {
        routing.fail_all(&DriverError::ShuttingDown);
        // The DETACHED routing dies at the end of this block, and its completion-panic latch with it:
        // the pump's per-group tail reads latches through `self.routing`, which no longer holds this
        // source. Drain it HERE or lose it — and a fired latch is PLANE-FATAL. The swept completion's
        // dropped guard captured arbitrary state that can alias ANY hosted group's FSM: a `Merged`
        // target that absorbed the source, OR any live sibling — a `Retired` husk's completion is no
        // exception, since its guard can tear a group the husk never owned. The tear is UNATTRIBUTABLE,
        // so every hosted group fail-stops. Raised in the storage crank, ahead of the pump, so each is
        // poisoned before anything serves or pumps it this crank (the tail's `is_poisoned` gate then
        // skips its query serve and fails its parked work `Poisoned`).
        if routing.take_completion_panicked() {
          self.coord.fail_stop_plane_unattributable_panic();
        }
      }
    }
    // `service_merge_applies` can latch a fail-stop with NO resolution and NO staged storage
    // work — an owed adopt capture faulting, a committed-corrupt decode in the resolver — and
    // this crank's earlier drain already ran, so without a second drain an otherwise idle
    // plane would sit on the queued poison until an unrelated wake. Same best-effort tail
    // semantics as the pre-storage drain; the hold signal drains here too for the same reason.
    while let Some(group) = self.coord.poll_poisoned() {
      let _ = self
        .lifecycle_tx
        .try_send(LifecycleEvent::Poisoned { group });
    }
    while let Some(blocked) = self.coord.peek_merge_blocked() {
      if self
        .lifecycle_tx
        .try_send(LifecycleEvent::MergeBlocked {
          target: blocked.target,
          source: blocked.source,
          boundary: blocked.boundary,
          cause: blocked.cause,
        })
        .is_err()
      {
        break;
      }
      let _ = self.coord.poll_merge_blocked();
    }
    self.flush_pending = self.engine.has_staged() || more;
    self
      .metrics
      .record(self.engine.barriers(), self.engine.ops_batched());
    self
      .metrics
      .record_fenced(self.coord.fenced_frames_dropped());
  }

  /// One inbound bridge frame: feed bytes/EOF to the coordinator, which demuxes each frame's
  /// group tag through the engine's per-group stores (errors close the conn).
  fn handle_inbound(&mut self, now: Now, inbound: BridgeInbound) {
    match inbound {
      BridgeInbound::Bytes { id, bytes } => {
        self
          .coord
          .handle_conn_data(id, &bytes, false, now, &mut self.engine);
        self.flush_pending = true;
      }
      BridgeInbound::Eof { id } => {
        self
          .coord
          .handle_conn_data(id, &[], true, now, &mut self.engine);
        self.flush_pending = true;
      }
      BridgeInbound::Error { id } => self.close_conn(id),
    }
  }

  /// One accepted socket (the single-group driver's admission + bridging verbatim).
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
    let id = self.coord.on_accept_open(record, now);
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

  /// The standing per-peer link reconciler (the single-group driver's verbatim — the shared
  /// connections are group-agnostic, so link repair is too; see that driver for the full
  /// convergence argument).
  fn reconcile_peer_links(&mut self, now: Instant) {
    let std_now = std::time::Instant::now();
    let mut wake: Option<std::time::Instant> = None;
    for node in self.peers.clone() {
      let (peer, addr) = node.into_parts();
      if self.coord.conn_of(&peer).is_some() {
        let stable = match self.redial.get_mut(&peer) {
          None => false,
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
        r.bound_since = None;
      }
      if self
        .conns
        .values()
        .any(|c| c.dialed_to.as_ref() == Some(&peer))
      {
        continue;
      }
      if let Some(r) = self.redial.get(&peer)
        && std_now < r.at
      {
        wake = Some(wake.map_or(r.at, |w| w.min(r.at)));
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
      wake = Some(wake.map_or(at, |w| w.min(at)));
    }
    self.redial_wake = wake;
  }

  /// Register + start one dial attempt (single-group verbatim).
  fn dial(&mut self, now: Instant, peer: I, addr: SocketAddr) {
    let record = match (self.dialer)(&peer) {
      Ok(r) => r,
      Err(_) => return,
    };
    let id = self.coord.on_dial_open(peer.cheap_clone(), record, now);
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
      }
      Err(_) => self.close_conn(id),
    }
  }

  /// Tear one connection down (single-group verbatim: no repair decision here) — and wake EVERY
  /// quiesced group: connection health is the quiesce liveness oracle (a dead leader's
  /// connections die, so the followers' stale election deadlines re-enter the fold and fire
  /// immediately — exactly the desired leader-failure election). Clearing the whole set on any
  /// close is deliberately conservative; conn churn is rare enough that the cost is noise, and a
  /// per-leader scoping (waking only the groups routed over the lost peer's connection) is the
  /// refinement seam.
  fn close_conn(&mut self, id: ConnId) {
    self.coord.on_conn_close(id);
    drop(self.conns.remove(&id));
    self.wake_all();
  }

  /// Admit a group's driver-side client state (routing + the leadership edge-detect).
  fn admit_group(&mut self, gid: G) {
    self
      .routing
      .insert(gid.cheap_clone(), Routing::new(self.stub_events_tx.clone()));
    self.was_leader.insert(gid, false);
  }

  /// Create a fresh group: engine storage + coordinator endpoint + driver routing, admitted
  /// together (engine admission rolled back if the coordinator refuses). The coordinator's
  /// admission consults THE ENGINE as its floor store — the driver is seam wiring plus
  /// persistence: it records the admitted incarnation after the `Ok`.
  fn create_group(
    &mut self,
    now: Now,
    gid: G,
    config: Config<I>,
    seed: u64,
    fsm: F,
    generation: u64,
  ) -> Result<(), DriverError<I>> {
    // The single drivers validate the Config — and loudly reject a walled failover tier on a
    // monotonic host — at bind; group admission is the same gate, per group.
    validate_and_capture_eps::<I, Monotonic>(&config).map_err(rejected)?;
    let election = config.election_timeout();
    // OCCUPIED STORES ARE RECOVERED STATE. A caller-supplied engine can arrive holding a
    // previous process's stores; a fresh term-0 endpoint built over them would ack success and
    // then truncate that durable history on its first append. Refuse, touching nothing — the
    // restart path is how existing state re-enters. A live SAME-PROCESS duplicate is a different
    // fact and keeps its own verdict: the container's `Exists` refusal answers below.
    let added = self.engine.add_group(gid.cheap_clone());
    if !added && self.coord.group(&gid).is_none() {
      return Err(DriverError::StoredStateExists);
    }
    // A NONZERO FOUNDING GENERATION IS PERSISTED, not merely seeded: the counter has no other
    // durable home until the group's first capture, so it goes through the store-taking door and
    // its stamp rides the barrier below, before this admission is acknowledged. Generation zero
    // is the value every replica reconstructs by default and needs no stamp, so it keeps the
    // storeless door untouched. The floors are snapshotted first — the door needs the engine
    // mutably for the stores, and the gate reads it immutably.
    let outcome = if generation == 0 {
      self.coord.create_group(
        gid.cheap_clone(),
        config,
        now,
        seed,
        fsm,
        generation,
        &self.engine,
      )
    } else {
      let floors = FloorSnapshot {
        floor: sailing_proto::FloorStore::floor(&self.engine, &gid),
        lineage: sailing_proto::FloorStore::lineage(&self.engine, &gid),
      };
      // The op-id floor for a founding incarnation, from the same per-group counter a restore or
      // a fork draws on: a completion left over from a prior incarnation of this id then sorts
      // below everything this one mints.
      let Some(epoch) = self.engine.next_boot_epoch(&gid) else {
        return Err(rejected("boot epoch counter exhausted for this group"));
      };
      match self.engine.stores(&gid) {
        Some((log, stable)) => self.coord.create_group_founded_at(
          gid.cheap_clone(),
          config,
          now,
          seed,
          fsm,
          generation,
          &floors,
          epoch,
          &*log,
          stable,
        ),
        // `add_group` above admitted the storage, so this is unreachable; refuse rather than
        // found an incarnation whose seed nothing recorded.
        None => Err(sailing_proto::CreateGroupError::NoStoredState),
      }
    };
    match outcome {
      Ok(()) => {
        self.engine.set_group_gen(&gid, generation);
        if generation > 0 {
          // The staged lineage record and the founding stamp ride the next barrier together.
          self.flush_pending = true;
        }
        self.election.insert(gid.cheap_clone(), election);
        self.admit_group(gid);
        Ok(())
      }
      Err(e) => {
        // Roll back ONLY an admission this call made; pre-existing storage is not ours to drop.
        if added {
          self.engine.remove_group(&gid);
        }
        Err(rejected(e))
      }
    }
  }

  /// Recover a group from the engine's storage, the driver deriving the boot epoch from the
  /// engine's per-group counter. The floor check reads a pre-call [`FloorSnapshot`] of the
  /// engine's lineage (the engine itself is lent to the restore as `(log, stable)`).
  ///
  /// This reconnects a group against the SAME live in-memory engine (after a driver-level teardown),
  /// NOT a recovery from durable storage: the in-memory engine keeps no state across a process crash.
  /// Real crash recovery is the planned persistent-engine seam's job.
  fn restore_group(
    &mut self,
    now: Now,
    gid: G,
    config: Config<I>,
    seed: u64,
    fsm: F,
    generation: u64,
  ) -> Result<(), DriverError<I>> {
    validate_and_capture_eps::<I, Monotonic>(&config).map_err(rejected)?;
    let election = config.election_timeout();
    let lineage = FloorSnapshot {
      floor: sailing_proto::FloorStore::floor(&self.engine, &gid),
      lineage: sailing_proto::FloorStore::lineage(&self.engine, &gid),
    };
    if self.engine.add_group(gid.cheap_clone()) {
      // `add_group` CREATED the store: the host has no stored state for this group (it was never
      // staged, or was torn down and its volatile state died with the in-memory engine). Restoring
      // now would fabricate a blank index-0 incarnation masquerading as recovered state — fail
      // closed after rolling the fresh store back out, rather than silently returning Ok. A durable
      // engine is the roadmap cure.
      self.engine.remove_group(&gid);
      return Err(DriverError::NoStoredState);
    }
    let epoch = match self.engine.next_boot_epoch(&gid) {
      Some(epoch) => epoch,
      // Storage was admitted just above (a fresh store already failed closed with `NoStoredState`),
      // so `None` here means the per-group boot-epoch counter is EXHAUSTED — refuse rather than
      // wrap onto a colliding incarnation identity. A pre-existing store is never rolled back on
      // refusal (it held real state before this call).
      None => return Err(rejected("boot epoch counter exhausted for this group")),
    };
    let result = {
      let (log, stable) = self.engine.stores(&gid).expect("storage admitted above");
      self.coord.restore_group(
        gid.cheap_clone(),
        config,
        now,
        seed,
        fsm,
        epoch,
        generation,
        &lineage,
        log,
        stable,
      )
    };
    match result {
      Ok(()) => {
        // Re-sync to the LIVE restored counter, and to NOTHING ELSE (see the reactor hosts):
        // replay re-applies lineage moves whose event-time mirror may have died with the crash, and
        // the admitted `generation` is a catalog claim the evidence record must never carry.
        let live = self.coord.group(&gid).map_or(0, Endpoint::shape_gen);
        self.engine.set_group_gen(&gid, live);
        // The restore may leave one staged stable write (a recovered lease floor): barrier it.
        self.flush_pending = true;
        self.election.insert(gid.cheap_clone(), election);
        self.admit_group(gid);
        Ok(())
      }
      // A pre-existing store is never removed on refusal (it held real state before this call); the
      // fresh-store case already failed closed above, so nothing here rolls a store back out.
      Err(e) => Err(rejected(e)),
    }
  }

  #[allow(clippy::too_many_arguments)]
  fn create_group_from_fork(
    &mut self,
    now: Now,
    gid: G,
    config: Config<I>,
    seed: u64,
    fsm: F,
    snapshot: Bytes,
    read_only: Option<ReadOnlyOption>,
    generation: u64,
  ) -> Result<(), DriverError<I>> {
    validate_and_capture_eps::<I, Monotonic>(&config).map_err(rejected)?;
    let election = config.election_timeout();
    let lineage = FloorSnapshot {
      floor: sailing_proto::FloorStore::floor(&self.engine, &gid),
      lineage: sailing_proto::FloorStore::lineage(&self.engine, &gid),
    };
    // Occupied child stores mean this child's baseline is ALREADY durable, so re-installing the
    // manufactured snapshot over them would overwrite a group that exists. Refusing is the
    // correct answer and costs the parent nothing: its fork fence releases off that durable
    // baseline, not off this install. Same live-duplicate carve-out as `create_group`.
    let added = self.engine.add_group(gid.cheap_clone());
    if !added && self.coord.group(&gid).is_none() {
      return Err(DriverError::StoredStateExists);
    }
    let epoch = match self.engine.next_boot_epoch(&gid) {
      Some(epoch) => epoch,
      // Storage was admitted just above, so `None` here means the per-group boot-epoch counter is
      // EXHAUSTED — refuse rather than wrap onto a colliding incarnation identity, rolling a
      // freshly-added store back out (create's rollback discipline).
      None => {
        if added {
          self.engine.remove_group(&gid);
        }
        return Err(rejected("boot epoch counter exhausted for this group"));
      }
    };
    let result = {
      let (log, stable) = self.engine.stores(&gid).expect("storage admitted above");
      self.coord.create_group_from_fork(
        gid.cheap_clone(),
        config,
        now,
        seed,
        fsm,
        snapshot,
        read_only,
        epoch,
        generation,
        &lineage,
        log,
        stable,
      )
    };
    match result {
      Ok(()) => {
        self.engine.set_group_gen(&gid, generation);
        // The manufactured baseline is STAGED in the group's stores: barrier it, so blob and
        // log re-baseline become flush-durable together with everything else this crank staged.
        self.flush_pending = true;
        self.election.insert(gid.cheap_clone(), election);
        self.admit_group(gid);
        Ok(())
      }
      Err(e) => {
        if added {
          self.engine.remove_group(&gid);
        }
        Err(rejected(e))
      }
    }
  }

  /// Remove a group: coordinator endpoint, engine storage, and driver routing torn down together;
  /// the group's parked work fails with the group-scoped teardown verdict. `Ok` carries whether the
  /// group was hosted; an UNRESOLVED merge participant refuses TRANSIENTLY ([`DriverError::Rejected`],
  /// the coordinator's inherited container gate — a thaw owed, a frozen source, a parked target, or a
  /// group a park names), tearing nothing down.
  fn remove_group(&mut self, gid: &G) -> Result<bool, DriverError<I>> {
    // THE TEARDOWN GATE FIRST: the coordinator inherits the container's refusal of a group that
    // still owes a thaw, so gate here BEFORE any floor write or teardown — a refusal must leave the
    // group, its floor, its stores, and its routing untouched. Self-clearing off the thaw pass.
    let existed = self
      .coord
      .remove_group(gid, &mut self.engine)
      .map_err(rejected)?
      .is_some();
    // Floors are the OPT-IN reshaping fence: a gen-0 id keeps the P5 volatile-tombstone rejoin;
    // a reshaped id is fenced one past its removal CEILING — every generation this incarnation
    // could have minted rides the unified counter the lineage mirrors track, so the floor
    // covers every outstanding gen-keyed authorization with no knowledge of any other group
    // (see the reactor hosts).
    let floor = self.engine.removal_floor(gid);
    if floor > 0 {
      self.engine.set_group_floor(gid, floor);
      self.flush_pending = true;
    }
    // The removal-time fork abandonment's guard advance: the container bumped its volatile relay
    // guard for a parent whose staged fork it just killed, and only a durable mirror keeps that
    // kill from being undone by a crash — the replayed split would re-stage the fork against the
    // clean slate this very removal made. Rides the same barrier as the floor write above.
    while let Some((parent, generation)) = self.coord.poll_relay_guard_advance() {
      self.engine.set_group_gen(&parent, generation);
      self.flush_pending = true;
    }
    let had_storage = self.engine.remove_group(gid);
    self.was_leader.remove(gid);
    self.quiesced.remove(gid);
    self.quiesce_pending.remove(gid);
    self.activity.remove(gid);
    self.election.remove(gid);
    // The DETACHED routing dies at the end of this block, and its completion-panic latch with it: the
    // pump's tails read latches through `self.routing`, which no longer holds this group. Drain it HERE
    // or lose it — and a fired latch is PLANE-FATAL. The sweep drops each parked completion's unused
    // closure, and a captured guard's `Drop` can tear ANY hosted group's FSM, not merely the one being
    // destroyed. Removal is not a teardown of the plane: the loop runs on and every sibling keeps
    // serving, so an unattributable tear must stop them all — the same verdict the merge fold's source
    // teardown raises. (The removed group is already out of the coordinator, so it is not among the
    // groups poisoned: nothing of it is left to serve.)
    if let Some(mut routing) = self.routing.remove(gid) {
      routing.fail_all(&DriverError::ShuttingDown);
      if routing.take_completion_panicked() {
        self.coord.fail_stop_plane_unattributable_panic();
      }
    }
    Ok(existed || had_storage)
  }

  /// Reply to a lifecycle command that WROTE to the engine, honoring persist-before-reply: while
  /// the write is still staged the verdict waits for the barrier that makes it durable; when the
  /// engine staged nothing the verdict is already covered and goes now. Queuing arms the barrier,
  /// so a queued reply always has one coming.
  fn ack_engine_write(&mut self, ack: PendingAck<I>) {
    if self.engine.has_staged() {
      self.flush_pending = true;
      self.acks_pending_flush.push(ack);
    } else {
      ack.send();
    }
  }

  /// Handle one command. Returns `true` when the loop should exit (a `Shutdown`).  ///
  /// # Persist-before-reply
  ///
  /// Every lifecycle command that writes engine state — create, create-from-fork, restore,
  /// remove — releases its verdict through [`ack_engine_write`](Self::ack_engine_write) rather
  /// than replying inline, so a success is observable only once the barrier covering that write
  /// has run. The rule is UNIFORM across those commands on purpose, though the stakes are not
  /// symmetric: a lost create is re-runnable by the caller, while a lost removal floor is a
  /// SAFETY rollback — a retired incarnation returning on the next boot. Making every one of them
  /// wait costs a caller nothing (the covering barrier runs in the same crank) and spares every
  /// future reader the case analysis about which verdicts may be trusted early.
  ///
  /// Client operations are outside the rule and need nothing from it: they do not ack on
  /// acceptance at all — their replies park until the entry commits, which is already strictly
  /// after the append carrying it became durable.
  fn handle_command(&mut self, now: Now, cmd: MultiCommand<G, I, F>) -> bool {
    // Any group-addressed CLIENT operation un-quiesces its group BEFORE dispatch, so the
    // operation's outputs flow and the re-armed deadlines are swept again. Pure observability
    // (`Status`) and the lifecycle commands do not wake — polling a status must not defeat
    // quiescence.
    match &cmd {
      MultiCommand::Submit { group, .. }
      | MultiCommand::Conf { group, .. }
      | MultiCommand::ConfV2 { group, .. }
      | MultiCommand::Query { group, .. }
      | MultiCommand::FailoverWindow { group, .. }
      | MultiCommand::Transfer { group, .. }
      | MultiCommand::SetReadMode { group, .. }
      | MultiCommand::ProposeSplit { group, .. } => {
        let group = group.cheap_clone();
        self.wake_group(&group);
      }
      _ => {}
    }
    match cmd {
      MultiCommand::Submit {
        group,
        cmd,
        reply,
        reservation,
      } => {
        let Some((log, stable)) = self.engine.stores(&group) else {
          let _ = reply.send(Err(no_such_group()));
          return false;
        };
        match self
          .coord
          .submit_propose_deferred(&group, now, log, stable, &cmd)
        {
          Some(Ok(index)) => {
            self.flush_pending = true;
            if let Some(routing) = self.routing.get_mut(&group) {
              routing.pending.insert(
                index,
                Pending::Submit {
                  reply,
                  _reservation: reservation,
                },
              );
            }
          }
          Some(Err(e)) => {
            let _ = reply.send(Err(map_propose_err(e)));
          }
          None => {
            let _ = reply.send(Err(no_such_group()));
          }
        }
      }
      MultiCommand::Conf {
        group,
        cc,
        reply,
        reservation,
      } => {
        let Some((log, stable)) = self.engine.stores(&group) else {
          let _ = reply.send(Err(no_such_group()));
          return false;
        };
        match self.coord.propose_conf_change(&group, now, log, stable, cc) {
          Some(Ok(index)) => {
            self.flush_pending = true;
            if let Some(routing) = self.routing.get_mut(&group) {
              routing.pending.insert(
                index,
                Pending::Conf {
                  reply,
                  _reservation: reservation,
                },
              );
            }
          }
          Some(Err(e)) => {
            let _ = reply.send(Err(map_propose_err(e)));
          }
          None => {
            let _ = reply.send(Err(no_such_group()));
          }
        }
      }
      MultiCommand::ConfV2 {
        group,
        cc,
        reply,
        reservation,
      } => {
        let Some((log, stable)) = self.engine.stores(&group) else {
          let _ = reply.send(Err(no_such_group()));
          return false;
        };
        match self
          .coord
          .propose_conf_change_v2(&group, now, log, stable, cc)
        {
          Some(Ok(index)) => {
            self.flush_pending = true;
            if let Some(routing) = self.routing.get_mut(&group) {
              routing.pending.insert(
                index,
                Pending::Conf {
                  reply,
                  _reservation: reservation,
                },
              );
            }
          }
          Some(Err(e)) => {
            let _ = reply.send(Err(map_propose_err(e)));
          }
          None => {
            let _ = reply.send(Err(no_such_group()));
          }
        }
      }
      MultiCommand::Query {
        group,
        complete,
        reservation,
      } => {
        // THE NOT-HOSTED REFUSALS — the two below (no routing, no stores) and the coordinator's own
        // further down. The library handed this closure no state machine, and that bounds NOTHING:
        // `Q` is `Send + 'static` and captures whatever it likes, `StateMachine` imposes no ownership
        // or isolation constraint, so a guard captured for a group this host does not carry can alias
        // state a HOSTED group's replicated FSM shares — tear it in `Drop`, and panic there, inside
        // the completion's `catch_unwind`. The driver cannot see what a closure captured, so the tear
        // is UNATTRIBUTABLE: it could be in ANY hosted group, and fail-stopping none leaves a torn
        // group serving committed state that has silently diverged from the replicas that never ran
        // the closure. A caught panic here is therefore PLANE-FATAL — every hosted group fail-stops,
        // each surfacing its own poison and failing its parked work with the typed verdict (see
        // `fail_stop_plane_unattributable_panic`). Availability loses to safety by design: a plane of
        // fail-stopped groups restarts from durable state, a divergent group is unrecoverable. Only a
        // panicking `Drop` — an abort-level anti-pattern the `query` contract forbids — reaches this.
        let Some(routing) = self.routing.get_mut(&group) else {
          if complete(Err(no_such_group())) == CompletionOutcome::Panicked {
            self.coord.fail_stop_plane_unattributable_panic();
          }
          return false;
        };
        let ctx = routing.mint_query_ctx();
        let Some((log, stable)) = self.engine.stores(&group) else {
          if complete(Err(no_such_group())) == CompletionOutcome::Panicked {
            self.coord.fail_stop_plane_unattributable_panic();
          }
          return false;
        };
        match self.coord.read_index(
          &group,
          now,
          log,
          stable,
          Bytes::copy_from_slice(&ctx.to_be_bytes()),
        ) {
          Some(Ok(())) => {
            routing.queries.insert(
              ctx,
              ParkedQuery {
                ready_at: None,
                complete,
                _reservation: reservation,
              },
            );
          }
          // An immediate refusal on a group this host DOES hold. The `Err` arm does not CALL the
          // user closure — it DROPS it unused inside the completion's `catch_unwind` — so a guard
          // the closure captured runs its `Drop` there. That `Drop` captured arbitrary state and can
          // alias state ANY hosted group's replicated FSM shares, not only this addressed one, so a
          // caught panic is UNATTRIBUTABLE and fail-stops the WHOLE plane (uniform policy) — the same
          // reaction the not-hosted refusal below raises.
          Some(Err(e)) => {
            if complete(Err(map_read_err(e))) == CompletionOutcome::Panicked {
              self.coord.fail_stop_plane_unattributable_panic();
            }
          }
          // The coordinator does not hold the group: the third not-hosted refusal, plane-fatal on a
          // caught panic for the reason above.
          None => {
            if complete(Err(no_such_group())) == CompletionOutcome::Panicked {
              self.coord.fail_stop_plane_unattributable_panic();
            }
          }
        }
      }
      MultiCommand::FailoverWindow {
        group,
        complete,
        reservation,
      } => {
        if let Some(routing) = self.routing.get_mut(&group) {
          routing.failovers.push(ParkedFailover {
            complete,
            _reservation: reservation,
          });
        } else if complete(Err(no_such_group())) == CompletionOutcome::Panicked {
          // Not hosted: the same UNATTRIBUTABLE tear the `Query` arm's refusals describe — the
          // dropped closure's captured guard could have torn ANY hosted group's state machine, and
          // this one names none — so the refusal is plane-fatal here too.
          self.coord.fail_stop_plane_unattributable_panic();
        }
      }
      MultiCommand::Transfer {
        group,
        to,
        reply,
        reservation,
      } => {
        let verdict = match self.engine.stores(&group) {
          None => Err(no_such_group()),
          Some((log, stable)) => match self.coord.transfer_leader(&group, now, log, stable, to) {
            Some(r) => r.map_err(map_transfer_err),
            None => Err(no_such_group()),
          },
        };
        let _ = reply.send(verdict);
        drop(reservation);
      }
      MultiCommand::SetReadMode {
        group,
        mode,
        reply,
        reservation,
      } => {
        let verdict = match self.engine.stores(&group) {
          None => Err(no_such_group()),
          Some((log, stable)) => {
            match self
              .coord
              .propose_read_mode_change(&group, now, log, stable, mode)
            {
              Some(r) => r.map_err(map_propose_err),
              None => Err(no_such_group()),
            }
          }
        };
        if verdict.is_ok() {
          self.flush_pending = true;
        }
        let _ = reply.send(verdict);
        drop(reservation);
      }
      MultiCommand::ProposeSplit {
        group,
        child,
        child_gen,
        instruction,
        reply,
        reservation,
      } => {
        // The propose-time floor leg reads a pre-call snapshot of the CHILD id's lineage (the
        // engine is lent to the propose as `(log, stable)`); the drain above keeps the
        // authoritative materialization-edge recheck.
        let floors = FloorSnapshot {
          floor: sailing_proto::FloorStore::floor(&self.engine, &child),
          lineage: sailing_proto::FloorStore::lineage(&self.engine, &child),
        };
        let verdict = match self.engine.stores(&group) {
          None => Err(no_such_group()),
          Some((log, stable)) => {
            match self.coord.propose_split(
              &group,
              now,
              log,
              stable,
              &child,
              child_gen,
              instruction,
              &floors,
            ) {
              Some(r) => r.map_err(map_split_err),
              None => Err(no_such_group()),
            }
          }
        };
        if verdict.is_ok() {
          self.flush_pending = true;
        }
        let _ = reply.send(verdict);
        drop(reservation);
      }
      MultiCommand::PrepareMerge {
        source,
        target,
        reply,
        reservation,
      } => {
        // The merge floor leg reads a pre-call snapshot of BOTH participants' lineage (the
        // engine is lent WHOLE as the propose's store seam: the container's claimed-target
        // gate reads co-hosted claimants' logs, not just the source's own).
        let floors = PairFloors::snapshot(&self.engine, &source, &target);
        let verdict =
          match self
            .coord
            .prepare_merge(&source, now, &mut self.engine, &target, &floors)
          {
            Some(r) => r.map_err(map_merge_err),
            None => Err(no_such_group()),
          };
        if verdict.is_ok() {
          self.flush_pending = true;
        }
        let _ = reply.send(verdict);
        drop(reservation);
      }
      MultiCommand::CommitMerge {
        target,
        source,
        reply,
        reservation,
      } => {
        let floors = PairFloors::snapshot(&self.engine, &source, &target);
        let verdict = match self.engine.stores(&target) {
          None => Err(no_such_group()),
          Some((log, stable)) => {
            match self
              .coord
              .commit_merge(&target, now, log, stable, &source, &floors)
            {
              Some(r) => r.map_err(map_merge_err),
              None => Err(no_such_group()),
            }
          }
        };
        if verdict.is_ok() {
          self.flush_pending = true;
        }
        let _ = reply.send(verdict);
        drop(reservation);
      }
      MultiCommand::RollbackMerge {
        target,
        source,
        reply,
        reservation,
      } => {
        let floors = PairFloors::snapshot(&self.engine, &source, &target);
        let verdict = match self.engine.stores(&target) {
          None => Err(no_such_group()),
          Some((log, stable)) => {
            match self
              .coord
              .rollback_merge(&target, now, log, stable, &source, &floors)
            {
              Some(r) => r.map_err(map_merge_err),
              None => Err(no_such_group()),
            }
          }
        };
        if verdict.is_ok() {
          self.flush_pending = true;
        }
        let _ = reply.send(verdict);
        drop(reservation);
      }
      MultiCommand::Status {
        group,
        reply,
        reservation,
      } => {
        let status = self.coord.group(&group).map(|ep| Status {
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
          unprovable_floor_campaigns: ep.unprovable_floor_campaigns(),
          frozen: ep.is_frozen(),
          shape_gen: ep.shape_gen(),
        });
        let _ = reply.send(status.ok_or_else(no_such_group));
        drop(reservation);
      }
      MultiCommand::CreateGroup {
        gid,
        config,
        seed,
        fsm,
        generation,
        reply,
        reservation,
      } => {
        let verdict = self.create_group(now, gid, config, seed, fsm, generation);
        self.ack_engine_write(PendingAck::Admission {
          reply,
          verdict,
          reservation,
        });
      }
      MultiCommand::CreateGroupFromFork {
        gid,
        config,
        seed,
        fsm,
        snapshot,
        generation,
        reply,
        reservation,
      } => {
        let verdict = self.create_group_from_fork(
          now, gid, config, seed, fsm, snapshot,
          // An embedder-driven fork inherits no parent mode: the child's config supplies
          // it, which is exactly the absent-provenance meaning of `None`.
          None, generation,
        );
        self.ack_engine_write(PendingAck::Admission {
          reply,
          verdict,
          reservation,
        });
      }
      MultiCommand::RestoreGroup {
        gid,
        config,
        seed,
        fsm,
        generation,
        reply,
        reservation,
      } => {
        let verdict = self.restore_group(now, gid, config, seed, fsm, generation);
        self.ack_engine_write(PendingAck::Admission {
          reply,
          verdict,
          reservation,
        });
      }
      MultiCommand::RemoveGroup {
        gid,
        reply,
        reservation,
      } => {
        let verdict = self.remove_group(&gid);
        self.ack_engine_write(PendingAck::Removal {
          reply,
          verdict,
          reservation,
        });
      }
      MultiCommand::ClearTombstone {
        gid,
        reply,
        reservation,
      } => {
        let _ = reply.send(self.coord.clear_tombstone(&gid));
        drop(reservation);
      }
      MultiCommand::Shutdown => return true,
    }
    false
  }

  /// Serve (or fall back) ONE group's parked failover inherited-read queries — the single-group
  /// `run_failover_serve` scoped to `gid`. Structurally inert on this monotonic-only v1 host
  /// (no group can arm a serve window without the failover tier), but kept whole so the wall-clock
  /// generalization does not reshape the loop.
  ///
  /// Returns `true` when the pass POISONED the group: a FATAL limbo storage fault (GROUP-scoped — the
  /// co-located groups and the driver keep running), or a caught user-closure panic in the served batch
  /// (UNATTRIBUTABLE, so it has already fail-stopped the whole plane). Either way the caller fails THAT
  /// group's parked work with the typed verdict. A DECLINE returns `false` and can still latch a caught
  /// drop-panic in the routing — the caller's pre-serve verdict read is what routes it.
  fn run_failover_serve(&mut self, gid: &G) -> bool {
    let Some(routing) = self.routing.get_mut(gid) else {
      return false;
    };
    // Nothing parked — and this check is also THE OTHER HALF of the leadership-backstop coupling. The
    // backstop's `fail_all(Superseded)` runs EARLIER in this same group's pre-serve step and can latch a
    // tear the step only reads AFTERWARDS, which reads like a same-group tear-then-serve window. It is
    // closed by construction: `Routing::fail_all` DRAINS `failovers` with `mem::take`, so whenever the
    // backstop could have latched, the batch this serve would run is ALREADY EMPTY and this returns before
    // any FSM is touched. Break either half — this check, or that drain — and the window re-opens inside
    // one group's step, where no plane-wide phase of the crank can see it.
    if routing.failovers.is_empty() {
      return false;
    }
    let now = self.clock.now();
    let Some(ep) = self.coord.group(gid) else {
      return false;
    };
    match ep.failover_read_window(now) {
      None => routing.decline_failovers(),
      Some(window) if routing.applied >= window.index() => {
        let Some((log, _stable)) = self.engine.stores(gid) else {
          return false;
        };
        match sailing_driver::shared::read_limbo(log, &window, self.max_failover_limbo_bytes as u64)
        {
          Ok(Some(limbo)) => {
            let parked = std::mem::take(&mut routing.failovers);
            let fsm = ep.state_machine();
            let panicked =
              sailing_driver::shared::serve_failover_batch(parked, fsm, &limbo, window, || {
                self
                  .coord
                  .group(gid)
                  .is_some_and(|e| e.failover_read_window(self.clock.now()).is_some())
              });
            if panicked {
              // A served inherited-read's user closure panicked: interior mutability could have torn
              // its FSM, and the closure's captured guard can alias state ANY hosted group shares, so
              // the caught panic is UNATTRIBUTABLE and fail-stops the WHOLE plane (uniform policy).
              self.coord.fail_stop_plane_unattributable_panic();
              return true;
            }
          }
          Ok(None) => routing.decline_failovers(),
          Err(_) => return true,
        }
      }
      Some(_) => {}
    }
    false
  }

  /// Drain the coordinator's aggregate outputs: wire bytes to each conn's writer (byte-budgeted),
  /// transport closes into teardown, group-stamped events into each group's routing plus the
  /// driver-owned stamped tail — then each hosted group's completion tail.
  async fn pump(&mut self, now: Now) {
    // Coalesce replication BEFORE the transmit drain, per hosted group. v1 flushes every group
    // unconditionally: `flush_appends` is idempotent and cheap when nothing is pending (the
    // proto's replication_pending flag), so proposed-this-crank tracking is a later refinement.
    let hosted: Vec<G> = self.engine.group_ids().map(|g| g.cheap_clone()).collect();
    for g in &hosted {
      if let Some((log, stable)) = self.engine.stores(g) {
        let _ = self.coord.flush_appends(g, now, log, stable);
      }
    }
    for (id, bytes) in self.coord.poll_transmit() {
      let Some(conn) = self.conns.get(&id) else {
        continue;
      };
      let projected = conn.queued_bytes.get() + bytes.len();
      if projected > self.max_outbound_backlog {
        self.close_conn(id);
        continue;
      }
      conn.queued_bytes.set(conn.queued_bytes.get() + bytes.len());
      // lochan unbounded `try_send` never returns `Full`; only `Closed` (the writer task already
      // exited), and a stale enqueue onto a dying conn is benign (consensus retransmits).
      let _ = conn.out_tx.try_send(BridgeOut(Bytes::from(bytes)));
    }
    while let Some((id, _err)) = self.coord.poll_conn_closed() {
      self.close_conn(id);
    }
    // Route each group-stamped event into ITS group's routing; the driver forwards the stamped
    // copy to the shared tail itself (the per-group Routing's own tail is the bind-time stub).
    let mut run_queries: BTreeSet<G> = BTreeSet::new();
    while let Some((g, ev)) = self.coord.poll_event() {
      // A `Merged` is withheld until the barrier covering the absorb's staged capture, floor,
      // and source removal (see `merges_pending_flush`): fold the lineage mirror NOW — the same
      // barrier must cover it — arm the flush, and defer both app-visible copies to the
      // post-barrier drain in the storage crank.
      if let Event::Merged(m) = &ev {
        let gen_after = m.gen_after();
        if gen_after > 0 {
          self.engine.set_group_gen(&g, gen_after);
        }
        self.flush_pending = true;
        self.merges_pending_flush.push((g, ev));
        continue;
      }
      if let Some(routing) = self.routing.get_mut(&g)
        && routing.route_event(ev.clone())
      {
        run_queries.insert(g.cheap_clone());
      }
      // REMOVED-SELF: a committed configuration change whose NEW configuration no longer names
      // this host in any role. An observation only — no auto-teardown: the replica keeps running,
      // harmlessly (the committed change already excluded it from every quorum), until the
      // application drains its reads and calls remove_group.
      if let Event::ConfChanged(cc) = &ev
        && self
          .coord
          .host_id()
          .is_some_and(|me| !conf_names(cc.conf(), me))
      {
        let _ = self.lifecycle_tx.try_send(LifecycleEvent::RemovedSelf {
          group: g.cheap_clone(),
        });
      }
      // THE LINEAGE MIRROR (see the reactor hosts): fold each applied merge move — and an
      // install's monotone catch-up — into the engine's per-id record (INV-LINEAGE).
      let lineage_move = match &ev {
        Event::MergeFrozen(f) => f.gen_after(),
        Event::MergeRolledBack(r) => r.gen_after(),
        Event::MergeAborted(a) => a.gen_after(),
        Event::SnapshotInstalled(meta) => meta.shape_gen(),
        _ => 0,
      };
      if lineage_move > 0 {
        self.engine.set_group_gen(&g, lineage_move);
        self.flush_pending = true;
      }
      let _ = self.events_tx.try_send((g, ev));
    }
    // Fold the coordinator's group-scoped scheduling signals IN DISPATCH ORDER: a flagged beat
    // quiesces its group (the leader promised heartbeat silence — the follower-side entry), and
    // any wake-class dispatch un-freezes it (the core re-armed its timers during the dispatch,
    // so the next sweep sees fresh deadlines — no stale fire).
    while let Some((g, ctrl)) = self.coord.poll_group_control() {
      match ctrl {
        GroupControl::Quiesce => {
          // A loss in this pass supersedes a Quiesce queued before it was known — and so does
          // standing merge-cure work, whose only carrier is the tick this entry would silence
          // (see [`Self::owes_merge_cure_ticks`]).
          if !self.link_lost_in_pass && !self.owes_merge_cure_ticks(&g) {
            self.quiesced.insert(g);
          }
        }
        GroupControl::Wake => self.wake_group(&g),
        _ => {}
      }
    }
    // UNKNOWN-GROUP placement signals: a registered factory is consulted FIRST, in THIS crank —
    // poll, materialize, admit run synchronously with every lifecycle mutation (one driver task),
    // so no removal or tombstone can interleave between the signal and the admission. The order
    // within one signal is the resource-safety line: materialize (the cheap catalog phase:
    // config + seed) → the sender gate ([`blueprint_names`]) → build (the state machine, the
    // factory's first real resource work) → the exact CreateGroup command path (engine +
    // coordinator + routing, same rollback). An admitted build consumes the signal — the
    // soliciting peer's retry completes the join. A decline, a blueprint that does NOT name the
    // solicitor (refused BEFORE build, so an unauthorized valid-cert solicitor can never force
    // state-machine construction), a build abort (`None`), or a create REFUSAL (the admission
    // gate applies to factory blueprints too — identity mismatch, invalid config, the tombstone
    // fail-closed) falls through to the lifecycle tail, so the embedder sees what the factory
    // could not place; without a factory every signal falls through, exactly as before.
    while let Some((group, from)) = self.coord.poll_unknown_group() {
      // A caught factory panic — `catch_unwind` returns `Err`, distinct from a legitimate `Ok(None)`
      // decline — QUARANTINES the factory: `self.factory` is permanently removed, so this signal and
      // every later one falls through to `UnknownGroup` exactly as on a driver that never had a
      // factory. WHY the panic (not a plain decline) is quarantined: the factory is the admission
      // authority for a group's consensus voter set, and `&mut GroupFactory` is not unwind-safe — one
      // that mutates internal state then panics could, on a LATER call, return a valid-LOOKING
      // blueprint that names the solicitor but carries a WRONG voter set, which clears every gate
      // below (none checks voter-set semantics) and admits a broken quorum. AssertUnwindSafe is sound
      // because the driver acts only on the returned Option and a torn factory is never reached
      // again. A legitimate `Ok(None)` decline keeps the factory; `Ok(Some(bp))` runs the gates.
      let mut quarantine = false;
      let mut admitted = false;
      let blueprint = match self.factory.as_mut() {
        Some(factory) => match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
          factory.materialize(&group, &from)
        })) {
          Ok(blueprint) => blueprint,
          Err(_) => {
            quarantine = true;
            None
          }
        },
        None => None,
      };
      // The gates run on the same seams the create below consults authoritatively, in the
      // resource-safety order: solicitor-naming first (BEFORE build, so an unauthorized valid-cert
      // solicitor never forces state-machine construction), then floors, then split-reservation.
      if let Some(blueprint) = blueprint
        && blueprint_names(&blueprint, &from)
        // The floors gate: a fenced id (or the reserved `u64::MAX` sentinel, never a working
        // incarnation) is refused before the factory's build phase can be asked for a state machine.
        && floor_admits(self.engine.floor(&group), blueprint.generation())
        // The split-reservation gate: a solicited id that an in-flight split reserves declines
        // BEFORE build, so the local fork stays the id's one materializer (the solicitation falls to
        // the lifecycle tail and the sender retries).
        && !self.coord.is_split_reserved(&group)
        // The debt-window gate: a gid named as an outstanding capture debt's absorbed source
        // must not be re-materialized beside the union its stores still back.
        && !self.coord.debt_names(&group)
      {
        // The resource phase, on a fresh borrow so a caught build panic can quarantine the factory
        // released above. Reached only after the blueprint cleared every gate.
        let fsm = match self.factory.as_mut() {
          Some(factory) => {
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| factory.build(&group))) {
              Ok(fsm) => fsm,
              Err(_) => {
                quarantine = true;
                None
              }
            }
          }
          None => None,
        };
        if let Some(fsm) = fsm {
          let generation = blueprint.generation();
          let (config, seed) = blueprint.into_parts();
          // A factory materialization is reshape-born only for the recreated/reshaped
          // (generation > 0) or fork-born (observer-shaped) subset; a fresh day-0 full-voter
          // blueprint keeps the caller's config. The two-legged gate lives in the helper.
          let config = reshape_born_factory_config(generation, config);
          admitted = self
            .create_group(now, group.cheap_clone(), config, seed, fsm, generation)
            .is_ok();
        }
      }
      if quarantine {
        // A caught factory panic QUARANTINES the factory AND fail-stops the whole plane: the factory
        // is arbitrary user code with no group isolation, so its panic (or a captured guard's `Drop`)
        // could have torn an aliased hosted FSM. Quarantine prevents REUSE but cannot UNDO a tear, so
        // consensus safety fail-stops every hosted group; the signal still falls through to
        // `UnknownGroup` below.
        self.factory = None;
        self.coord.fail_stop_plane_unattributable_panic();
      }
      if admitted {
        continue;
      }
      let _ = self
        .lifecycle_tx
        .try_send(LifecycleEvent::UnknownGroup { group, from });
    }
    // The single pump's completion tail, split PLANE-WIDE into a TEAR phase and a SERVE phase.
    //
    // Detection is per group — each group's caught completion(-drop) panic latches in ITS routing — but
    // the REACTION is plane-fatal, because the tear is unattributable. Running one group's whole tail
    // (sweep, decline, THEN serve) before the next group's would let a sibling that sorts EARLIER serve
    // a read this crank against a state machine a LATER group's dropped guard has already torn: the
    // sibling's own latch is clear and its endpoint is not yet poisoned, so nothing stops it. Whether a
    // torn read is served would then depend on the numeric order of the group ids — accidental safety.
    //
    // So the crank is phased instead:
    //   (1) HARVEST every routing's latch, plane-wide, before any group's completion step runs — the
    //       sweeps `route_event` ran above (a `LeaderChanged` fails the parked work of the group that
    //       changed) latch here, and a fired latch fail-stops the plane BEFORE anything serves;
    //   (2) the PRE-SERVE step of every group — watermark sync, the leadership-loss backstop, the
    //       failover serve/decline — each routing its OWN latch at the end of its step, so a tear in
    //       one group's step poisons the plane before the NEXT group's step can serve an inherited read;
    //   (3) the SERVE step of every group, each gated on a FRESH `is_poisoned()` read.
    // Every tear is therefore converted into a plane-wide poison before any subsequent serve, in
    // program order: no group serves a read after a caught panic, whatever the id order.
    self.harvest_completion_panics();
    let with_routing: Vec<G> = self.routing.keys().map(|g| g.cheap_clone()).collect();
    for g in &with_routing {
      self.pump_group_pre_serve(g, &mut run_queries);
    }
    for g in &with_routing {
      self.pump_group_tail(g, run_queries.contains(g));
    }
  }

  /// Take EVERY hosted routing's completion-panic latch and fail-stop the plane ONCE if any fired —
  /// the crank's authoritative pre-serve harvest.
  ///
  /// A latch is set by a completion that caught a user-closure(-drop) panic while DROPPING an unused
  /// closure (a `fail_all` sweep, a failover decline). The tear is unattributable — the closure's guard
  /// can alias any hosted group's FSM — so reading only the latched group's latch before only that
  /// group's serve is not enough: every OTHER group must be stopped too, and stopped BEFORE it serves.
  /// This runs ahead of every per-group step in the pump, so a latch set anywhere earlier in the crank
  /// (above all a `route_event` sweep) poisons the plane while every group's parked reads are still
  /// parked. Draining every routing — not stopping at the first — keeps the `Drop` invariant: no
  /// `Routing` outlives its verdict.
  ///
  /// WHAT ONLY THIS CALL CAN STOP. A latch already SET when the per-group steps begin — `route_event`'s
  /// `LeaderChanged` arm sweeps the changed group with `fail_all(Superseded)` during the event drain above
  /// — must poison the plane before any group's PHASE-2 failover SERVE, not merely before the phase-3
  /// query serves. Without this call, a group sorting BEFORE the latched one reaches `run_failover_serve`
  /// in its own pre-serve step with its latch clear and its endpoint unpoisoned, and serves inherited
  /// reads against the FSM the sibling's swept guard already tore; the latched group's own pre-serve does
  /// route its latch, but one step too late. The phase-3 QUERY serves are covered WITHOUT this call — the
  /// latched group's pre-serve poisons the plane before any tail runs — so they are not what it is for.
  ///
  /// THAT PATH IS UNREACHABLE ON THIS HOST, so nothing can red-proof the CALL: delete it and the suite
  /// stays green. `run_failover_serve`'s serve arm is inert here — `failover_read_window` returns `None`
  /// without the LeaseGuard failover tier, so the pass can only DECLINE — and that inertness is a property
  /// of this monotonic-only v1 host, not of this code. The moment the synchronized-`WallClock` failover
  /// tier reaches the multi drivers (the generalization `run_failover_serve` is kept whole for), the serve
  /// arm arms and this call becomes the only thing between a sibling's tear and an inherited read. It must
  /// therefore NOT be deleted on the strength of a green suite. Its BODY is pinned directly, by the
  /// reactor stream twin's `a_harvest_of_one_latched_routing_fail_stops_every_hosted_group`.
  fn harvest_completion_panics(&mut self) {
    let mut torn = false;
    for routing in self.routing.values_mut() {
      torn |= routing.take_completion_panicked();
    }
    if torn {
      self.coord.fail_stop_plane_unattributable_panic();
    }
  }

  /// The deadline actually ARMED: the earliest NON-quiesced group deadline folded with the
  /// transport's own ([`MultiStreamCoordinator::transport_timeout`], handshake reaping) — the
  /// `poll_timeout()` decomposition quiescing requires. A quiesced group's deadline is excluded
  /// here AND skipped in [`Self::fire_timeouts`]'s due sweep; the core still records it, so a
  /// wake re-admits it and a long-stale one fires immediately.
  fn armed_deadline(&self) -> Option<Instant> {
    let group = self
      .coord
      .deadlines()
      .filter(|(g, _)| !self.quiesced.contains(g))
      .map(|(_, d)| d)
      .min();
    match (group, self.coord.transport_timeout()) {
      (Some(a), Some(b)) => Some(a.min(b)),
      (a, None) => a,
      (None, b) => b,
    }
  }

  /// Un-quiesce one group: it re-enters the armed fold (a stale deadline fires immediately), its
  /// idle clock restarts so it cannot re-mark before a fresh full election timeout, and any
  /// not-yet-stamped quiesce intent is cancelled at the coordinator — the eligibility that
  /// justified the intent no longer holds, so it must never reach a later beat.
  fn wake_group(&mut self, g: &G) {
    self.quiesced.remove(g);
    self.quiesce_pending.remove(g);
    self.coord.cancel_quiescing(g);
    if let Some(a) = self.activity.get_mut(g) {
      a.at = std::time::Instant::now();
    }
  }

  /// Wake EVERY quiesced (and pending) group — the conn-loss path (see [`Self::close_conn`]).
  fn wake_all(&mut self) {
    self.link_lost_in_pass = true;
    let woken: Vec<G> = self
      .quiesced
      .iter()
      .chain(self.quiesce_pending.iter())
      .map(|g| g.cheap_clone())
      .collect();
    for g in &woken {
      self.wake_group(g);
    }
  }

  /// The per-iteration quiesce scheduler.
  ///
  /// PHASE B (enter): a pending group whose quiesce intent the coordinator has consumed — its
  /// flagged beat is stamped and ships with this crank's transmit drain — moves into the
  /// quiesced set; until then it keeps being swept, so the flagged beat actually goes out.
  ///
  /// ELIGIBILITY (mark): a leader group becomes quiesce-eligible when its consensus state is
  /// idle ([`group_idle`]), it has no parked driver work, and its OBSERVED state — the
  /// `(term, commit, applied)` tuple — has not changed for a full election timeout. Observation
  /// (a state diff per iteration) rather than dispatch counting is deliberate: an idle leader's
  /// steady heartbeat/response exchange dispatches forever without changing anything, so a
  /// dispatch-based activity clock would never expire; every consensus-visible advance (an
  /// election, an append, a commit, an apply) moves the tuple, and client commands reset the
  /// clock directly ([`Self::wake_group`]).
  /// Whether `g` carries merge-cure work whose ONLY carrier is the slow tick.
  ///
  /// THE CARRIER ARGUMENT. A wedged park's boundary advertisement rides `handle_timeout`
  /// (`drive_stuck_advertisement`, one unsolicited response per election timeout), the leader's
  /// cure and capture-debt sweeps ride the same beat, and a quiesced group's deadlines are
  /// EXCLUDED from the armed fold and skipped in the due sweep. So a group admitted to the
  /// quiesced set while any of these stand loses the exact cadence that would end them, until
  /// unrelated traffic happens to wake it — and the wedge is precisely the state in which no such
  /// traffic is coming.
  ///
  /// [`group_idle`] already refuses the LEADER side of all three. This covers the FOLLOWER-side
  /// entry, which is driven by the leader's flagged beat and gated by no eligibility check of
  /// ours — and a follower is where the advertisement lives, the leader being its consumer.
  fn owes_merge_cure_ticks(&self, g: &G) -> bool {
    // The FORK leg of the same doctrine: a parent whose head fork is held, or whose one-shot cue
    // is still queued, owes the embedder a prompt it can only get from a running crank — so it is
    // no more quiesce-eligible than a parked merge participant, and evicts if already quiesced.
    self.coord.fork_relay_pending(g)
      || self.coord.group(g).is_some_and(|ep| {
        ep.merge_park_unresolvable().is_some() || ep.capture_debt().is_some() || ep.has_cure_debts()
      })
  }

  fn quiesce_sweep(&mut self) {
    let now_std = std::time::Instant::now();
    let stamped: Vec<G> = self
      .quiesce_pending
      .iter()
      .filter(|g| !self.coord.is_quiescing(g))
      .map(|g| g.cheap_clone())
      .collect();
    for g in stamped {
      self.quiesce_pending.remove(&g);
      self.quiesced.insert(g);
    }
    let hosted: Vec<G> = self.engine.group_ids().map(|g| g.cheap_clone()).collect();
    for g in &hosted {
      let Some(ep) = self.coord.group(g) else {
        continue;
      };
      let (term, commit, applied) = (ep.term(), ep.commit_index(), ep.applied_index());
      let entry = self
        .activity
        .entry(g.cheap_clone())
        .or_insert(GroupActivity {
          term,
          commit,
          applied,
          at: now_std,
        });
      if (entry.term, entry.commit, entry.applied) != (term, commit, applied) {
        *entry = GroupActivity {
          term,
          commit,
          applied,
          at: now_std,
        };
      }
      let idle_since = entry.at;
      // EVICT, not merely refuse: the resolver's unresolvable classification is re-derived every
      // crank and can first hold on a crank AFTER the group entered the quiesced set, so the
      // entry gate alone would leave the group silent with the hint standing.
      if self.owes_merge_cure_ticks(g) {
        if self.quiesced.contains(g) || self.quiesce_pending.contains(g) {
          self.wake_group(g);
        }
        continue;
      }
      if self.quiesced.contains(g) || self.quiesce_pending.contains(g) {
        continue;
      }
      let Some(window) = self.election.get(g).copied() else {
        continue;
      };
      if now_std.duration_since(idle_since) < window {
        continue;
      }
      let parked = self
        .routing
        .get(g)
        .is_some_and(|r| !r.pending.is_empty() || !r.queries.is_empty() || !r.failovers.is_empty());
      if parked {
        continue;
      }
      if self.coord.group(g).is_some_and(group_idle) {
        // Mark the intent; the group's NEXT heartbeat broadcast carries the flag, and the
        // stamped-intent check above moves it into the quiesced set on a later sweep.
        self.coord.mark_quiescing(g);
        self.quiesce_pending.insert(g.cheap_clone());
      }
    }
    self.metrics.record_quiesced(self.quiesced.len() as u64);
  }

  /// One group's PRE-SERVE step: watermark sync, the leadership-loss backstop, and the failover
  /// serve/decline — every completion this group runs in a crank that is NOT the parked-query serve.
  /// The pump runs it for EVERY group before ANY group serves, and it routes this group's
  /// completion-panic latch at its END: the sweeps and declines here drop unused user closures, a
  /// captured guard's `Drop` can tear any hosted group's FSM, so the verdict must become a plane-wide
  /// poison before the NEXT group's step (whose failover serve would otherwise read a torn state
  /// machine) and before every group's query serve.
  ///
  /// Records the group in `run_queries` when its apply watermark advanced — the reads it confirmed are
  /// now runnable — and REMOVES it when the failover pass poisoned the group: the typed sweep drained
  /// its parked queries, so nothing of it is left for the serve step (the early return this replaces).
  fn pump_group_pre_serve(&mut self, gid: &G, run_queries: &mut BTreeSet<G>) {
    let Some(ep) = self.coord.group(gid) else {
      return;
    };
    let poisoned = ep.is_poisoned();
    let applied = ep.applied_index();
    let is_leader = ep.role().is_leader();
    if !poisoned
      && let Some(routing) = self.routing.get_mut(gid)
      && routing.sync_applied(applied)
    {
      run_queries.insert(gid.cheap_clone());
    }
    let was = self
      .was_leader
      .insert(gid.cheap_clone(), is_leader)
      .unwrap_or(false);
    if was && !is_leader {
      // Deposition can be TIMER-driven (a CheckQuorum step-down) with no inbound wake control,
      // so the role edge itself must funnel the wake: a quiesce intent recorded under the old
      // leadership dies here, never to be stamped onto a re-elected term's beat before fresh
      // eligibility is proven.
      self.wake_group(gid);
      if let Some(routing) = self.routing.get_mut(gid) {
        routing.fail_all(&DriverError::Superseded);
      }
    }
    if !poisoned && self.run_failover_serve(gid) {
      // A FATAL limbo storage fault (group-scoped), or a caught panic in the served inherited-read
      // batch (which has already fail-stopped the plane). Either way this group's parked work fails
      // with the typed verdict, and that sweep leaves nothing for the serve step to run.
      if let Some(routing) = self.routing.get_mut(gid) {
        routing.fail_all(&DriverError::Poisoned);
      }
      run_queries.remove(gid);
    }
    // THE STEP'S OWN VERDICT. The backstop's sweep, the failover decline, and the typed sweep above all
    // drop unused user closures inside `catch_unwind`; a caught guard-`Drop` panic latches in this
    // group's routing. Route it NOW — the tear is unattributable, so it fail-stops the plane, and every
    // group whose step or serve is still to come reads that poison before it runs. (The fault arm
    // returned early without this read: a latch it set was deferred a whole crank — every sibling
    // serving meanwhile — and lost outright if the group was removed before the next one.)
    if self
      .routing
      .get_mut(gid)
      .is_some_and(|routing| routing.take_completion_panicked())
    {
      self.coord.fail_stop_plane_unattributable_panic();
    }
  }

  /// One group's SERVE step: the poison gate, the runnable parked queries, and the poison sweep.
  ///
  /// Reached only after the plane-wide harvest and EVERY group's [`Self::pump_group_pre_serve`], so a
  /// completion(-drop) panic caught anywhere earlier in this crank has already poisoned this group and
  /// the gate below skips the serve. A poisoned group fails ITS parked work with the typed verdict.
  fn pump_group_tail(&mut self, gid: &G, run_queries: bool) {
    let Some(ep) = self.coord.group(gid) else {
      return;
    };
    // FRESH: a group whose serve step ran earlier in this loop may have fail-stopped the plane after the
    // harvest — this read is what makes that stop bind here, before this group serves.
    let poisoned = ep.is_poisoned();
    // A DEFENSIVE BELT — provably clear under the current phasing, NOT a live path. The harvest took
    // EVERY routing's latch at the top of the crank, this group's own pre-serve step took whatever its
    // completions latched, and no group's step touches a SIBLING's routing — so nothing between that take
    // and this one can set this latch, and it always reads `false` today. It is kept because it is the
    // last read before the serve: a future latch source inside the tail (a new completion here, a
    // tear-capable step reordered into it) needs exactly this gate and must not have to reinvent it. When
    // it IS set the serve is SKIPPED: the fail-stop and the `fail_all(Poisoned)` below fail the parked
    // queries `Poisoned` instead. A query that panics DURING the serve still reports through
    // `query_panicked`; one read-and-clear both gates and decides.
    let completion_panicked = self
      .routing
      .get_mut(gid)
      .is_some_and(|routing| routing.take_completion_panicked());
    let mut query_panicked = false;
    // A POISONED group never serves a parked read, and this gate is what makes a fail-stop raised
    // EARLIER in the crank actually stop the serve: an immediate refusal's dropped closure panicked in
    // `handle_command`, a merge teardown or a `remove_group` fail-stopped the plane in `storage_crank`,
    // or another group's pre-serve step tore and fail-stopped it moments ago — none of them routes
    // through THIS group's completion latch. `is_poisoned` is the one surface every fail-stop funnels
    // through, so gating on it covers every present and future poison source.
    if !poisoned
      && !completion_panicked
      && run_queries
      && let Some(ep) = self.coord.group(gid)
      && let Some(routing) = self.routing.get_mut(gid)
    {
      // A caught user-closure panic fail-stops the WHOLE plane (the closure captured arbitrary state
      // aliasing any group's FSM, so the tear is unattributable): the batch completes its remainder
      // `Poisoned` (already drained from routing, so the `fail_all` below cannot reach them); the
      // fail-stop runs once the `ep` borrow releases — before any later group's serve step.
      query_panicked = sailing_driver::shared::serve_query_batch(
        routing.take_runnable_queries(),
        ep.state_machine(),
      );
    }
    if query_panicked || completion_panicked {
      // A query that panicked mid-serve, or a completion(-drop) panic latched before it, is
      // UNATTRIBUTABLE — the closure and its captured guard can alias ANY hosted group's FSM — so the
      // WHOLE plane fail-stops (uniform policy). Each group poisons and surfaces once on the lifecycle
      // tail via `poll_poisoned`, then fails its parked work `Poisoned`.
      self.coord.fail_stop_plane_unattributable_panic();
    }
    let mut swept_panicked = false;
    if (poisoned || query_panicked || completion_panicked)
      && let Some(routing) = self.routing.get_mut(gid)
    {
      routing.fail_all(&DriverError::Poisoned);
      swept_panicked = routing.take_completion_panicked();
    }
    if swept_panicked {
      // The typed sweep's OWN dropped closures tear too, and a GROUP-scoped poison (an apply or storage
      // fault) reaches it with the plane otherwise healthy — so this latch must fail-stop the plane
      // here, or the groups whose serve step has not run yet would serve after the tear.
      self.coord.fail_stop_plane_unattributable_panic();
    }
  }
}

#[cfg(test)]
#[path = "stream_tests.rs"]
mod stream_tests;
