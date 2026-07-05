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
//! Fault scope is PER GROUP: a poisoned group fails its own parked work with the typed verdict
//! and the driver keeps serving the co-located groups; only driver-level events (shutdown, every
//! handle dropping) end `run()`.

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
  Config, ConnId, Event, FloorStore, GroupControl, GroupEngine, GroupId, Index, Instant,
  MultiStreamCoordinator, Now, ReadOnlyOption, RecordIo, StateMachine, StorageProgress,
  floor_admits,
};

use sailing_driver::{
  BoxedGroupFactory, GroupFactory, LifecycleEvent, MultiCommand, MultiHandle, Node, Status,
  jittered,
  shared::{ParkedFailover, ParkedQuery, Pending, Routing},
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
  rejected,
};

/// Backstop wake cadence while connections exist, exactly as on the reactor multi loop: with
/// ZERO hosted groups (or only timerless ones) the group-deadline fold can be `None` while
/// un-handshaked connections still need reaping.
const HOUSEKEEPING_INTERVAL: Duration = Duration::from_secs(1);

/// Per-iteration bound on each loop-top channel drain (the reactor multi loop's fairness note:
/// the loop-top drains make guaranteed progress independent of the biased select).
const IO_BUDGET: usize = 256;

/// A multi-group consensus host over framed reliable streams on compio. `G` is the group id; `R`
/// the record layer the factories build: `Labeled<Passthrough>` for plain TCP,
/// `Labeled<TlsRecords>` for TLS — the single driver's `Rc` factory pattern verbatim.
///
/// Construct AND run on the same thread (see the crate docs); the `Rc` factories and `lochan`
/// channels make this driver structurally `!Send`, enforcing that pinning — the whole HOST stays
/// serial because it is ONE task on ONE thread. Storage is the owned [`GroupEngine`]: v1 hosts
/// the concrete shared in-memory engine (a disk engine motivates the storage-trait seam later),
/// and each crank runs one engine-wide barrier.
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
pub struct CompioMultiStreamDriver<G, I, F, R>
where
  I: sailing_proto::NodeId,
  F: StateMachine,
  R: RecordIo,
{
  coord: MultiStreamCoordinator<G, I, F, R>,
  engine: GroupEngine<G, I>,
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
  storage_ready: flume::Receiver<()>,
  _storage_ready_keepalive: Option<flume::Sender<()>>,
  conns: BTreeMap<ConnId, Conn<I>>,
  redial: BTreeMap<I, Redial>,
  redial_wake: Option<std::time::Instant>,
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

impl<G, I, F, R> CompioMultiStreamDriver<G, I, F, R>
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
    Self::bind_with_tails(addr, peers, dialer, acceptor, driver_cfg, tails).await
  }

  /// Bind with CALLER-SUPPLIED fan-in tails — the sharded host's constructor: K planes each get
  /// a clone of ONE [`SharedTails`], so their events, lifecycle signals, and in-flight budget
  /// fan into a single set by construction. The returned [`MultiHandle`] is standard (it holds
  /// clones of the shared receivers and the shared budget); a single-plane `bind` builds its own
  /// tails and delegates here.
  pub(crate) async fn bind_with_tails(
    addr: SocketAddr,
    peers: Vec<Node<I, SocketAddr>>,
    dialer: DialerFactory<I, R>,
    acceptor: AcceptorFactory<R>,
    driver_cfg: DriverConfig,
    tails: SharedTails<G, I, F>,
  ) -> Result<(Self, MultiHandle<G, I, F>), BindError> {
    driver_cfg.validate()?;
    let listener = TcpListener::bind(addr).await?;
    let coord = MultiStreamCoordinator::new();
    Ok(Self::from_parts(
      coord, listener, peers, dialer, acceptor, driver_cfg, tails,
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
        engine: GroupEngine::new(),
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

  /// Cap the engine's chunked-snapshot staging buffers (the [`GroupEngine`] knob, applied to
  /// this host's engine). Set between `bind` and `run()`.
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

  /// The per-crank storage step, replacing the single loop's one `handle_storage` call:
  /// (a) ONE engine flush — the batched durability barrier every hosted group's staged writes
  /// share, the fsync-amortization point — gated on the pending-flush flag so an idle pass never
  /// burns a barrier on a knowably-empty batch; then (b) every hosted group's completion drain,
  /// re-driving a budget-cut group at most [`STORAGE_REDRIVES`] times (the remainder rides the
  /// next crank). v1 deliberately iterates ALL hosted groups — an idle group's drain is a cheap
  /// no-op poll — with dirty-set tracking as the scale refinement.
  /// Drain the container's committed, relay-ready forks into materializations — BEFORE the
  /// barrier below, so the SAME crank's `engine.flush()` covers every staged baseline before
  /// `pump` can transmit anything for the child (a child that can solicit peers is therefore
  /// always locally blob-durable first; the drain also front-runs the factory drain, so a
  /// local fork wins any same-id solicitation race). A refused materialization — a floored or
  /// tombstoned child id, an invalid config, used child storage (which a fork never
  /// overwrites) — abandons THIS fork: its barrier resolves (the parent must not stay fenced
  /// for a fork that will never land here), the refusal surfaces as
  /// [`LifecycleEvent::SplitRefused`] for the placement brain, and the driver survives; the
  /// child still reaches this host by the ordinary lifecycle paths (restore over its own
  /// storage, solicitation → factory/embedder → snapshot from a live member, whose own blob
  /// went durable before it could transmit). A fork whose child id is ALREADY HOSTED is
  /// neither yielded nor abandoned: the container PARKS it (blob held, the parent's fence
  /// standing, re-examined every crank) and the conflict pump below relays the one-shot
  /// [`LifecycleEvent::SplitConflict`] to the embedder — consumed from the coordinator only
  /// once the bounded lifecycle tail accepts it, so backpressure defers the cue instead of
  /// erasing it — whose removal/catch-up resolves the park.
  fn fork_drain(&mut self, now: Now) {
    while let Some(fork) = self.coord.poll_pending_fork() {
      let parent = fork.parent;
      let split_index = fork.split_index;
      let child = fork.child.cheap_clone();
      let seed = host_seed(self.coord.host_id());
      match self.create_group_from_fork(
        now,
        fork.child,
        fork.config,
        seed,
        fork.fsm,
        fork.blob,
        fork.read_only,
        fork.child_gen,
      ) {
        Ok(()) => {
          // The parent's lineage record advances with the child's registration, all behind the
          // ONE barrier the pending-flush entry below waits on.
          self.engine.set_group_gen(&parent, fork.parent_gen_after);
          self.forks_pending_flush.push((parent, split_index, child));
        }
        Err(_) => {
          self.coord.lift_fork_barrier(&parent, split_index);
          let _ = self
            .lifecycle_tx
            .try_send(LifecycleEvent::SplitRefused { parent, child });
        }
      }
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
  }

  fn storage_crank(&mut self, now: Now) {
    self.fork_drain(now);
    if self.flush_pending {
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
    let resolutions = self.coord.service_merge_applies(&mut self.engine);
    if !resolutions.is_empty() {
      self.flush_pending = true;
    }
    for r in resolutions {
      if let sailing_proto::MergeResolution::Merged { source, .. } = r {
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
        }
      }
    }
    self.flush_pending = self.engine.has_staged() || more;
    self
      .metrics
      .record(self.engine.flushes(), self.engine.ops_flushed());
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
    let added = self.engine.add_group(gid.cheap_clone());
    match self.coord.create_group(
      gid.cheap_clone(),
      config,
      now,
      seed,
      fsm,
      generation,
      &self.engine,
    ) {
      Ok(()) => {
        self.engine.set_group_gen(&gid, generation);
        if generation > 0 {
          // The staged lineage record rides the next barrier like any stable write.
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
      floor: self.engine.group_floor(&gid),
      lineage: self.engine.group_gen(&gid),
    };
    let added = self.engine.add_group(gid.cheap_clone());
    let epoch = self
      .engine
      .next_boot_epoch(&gid)
      .expect("storage admitted above");
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
        self.engine.set_group_gen(&gid, generation);
        // The restore may leave one staged stable write (a recovered lease floor): barrier it.
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

  /// Create a group from LOCALLY-FORKED state (the reactor hosts' manufactured-baseline flow
  /// on the compio plane): [`FloorSnapshot`] pre-read, engine boot epoch, the baseline staged
  /// behind the next barrier, and create's rollback discipline on refusal.
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
      floor: self.engine.group_floor(&gid),
      lineage: self.engine.group_gen(&gid),
    };
    let added = self.engine.add_group(gid.cheap_clone());
    let epoch = self
      .engine
      .next_boot_epoch(&gid)
      .expect("storage admitted above");
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
  /// the group's parked work fails with the group-scoped teardown verdict. Returns whether the
  /// group was hosted.
  fn remove_group(&mut self, gid: &G) -> bool {
    // Floors are the OPT-IN reshaping fence: a gen-0 id keeps the P5 volatile-tombstone rejoin;
    // a reshaped id is fenced below its next incarnation forever.
    let generation = self.engine.group_gen(gid);
    if generation > 0 {
      self
        .engine
        .set_group_floor(gid, generation.saturating_add(1));
      self.flush_pending = true;
    }
    let existed = self.coord.remove_group(gid).is_some();
    let had_storage = self.engine.remove_group(gid);
    self.was_leader.remove(gid);
    self.quiesced.remove(gid);
    self.quiesce_pending.remove(gid);
    self.activity.remove(gid);
    self.election.remove(gid);
    if let Some(mut routing) = self.routing.remove(gid) {
      routing.fail_all(&DriverError::ShuttingDown);
    }
    existed || had_storage
  }

  /// Handle one command. Returns `true` when the loop should exit (a `Shutdown`).
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
        let Some(routing) = self.routing.get_mut(&group) else {
          complete(Err(no_such_group()));
          return false;
        };
        let ctx = routing.mint_query_ctx();
        let Some((log, stable)) = self.engine.stores(&group) else {
          complete(Err(no_such_group()));
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
          Some(Err(e)) => complete(Err(map_read_err(e))),
          None => complete(Err(no_such_group())),
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
        } else {
          complete(Err(no_such_group()));
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
          floor: self.engine.group_floor(&child),
          lineage: self.engine.group_gen(&child),
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
        // engine is lent to the propose as `(log, stable)`).
        let floors = PairFloors::snapshot(&self.engine, &source, &target);
        let verdict = match self.engine.stores(&source) {
          None => Err(no_such_group()),
          Some((log, stable)) => {
            match self
              .coord
              .prepare_merge(&source, now, log, stable, &target, &floors)
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
        source,
        reply,
        reservation,
      } => {
        let verdict = match self.engine.stores(&source) {
          None => Err(no_such_group()),
          Some((log, stable)) => match self.coord.rollback_merge(&source, now, log, stable) {
            Some(r) => r.map_err(map_merge_err),
            None => Err(no_such_group()),
          },
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
        let _ = reply.send(self.create_group(now, gid, config, seed, fsm, generation));
        drop(reservation);
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
        let _ = reply.send(self.create_group_from_fork(
          now, gid, config, seed, fsm, snapshot,
          // An embedder-driven fork inherits no parent mode: the child's config supplies
          // it, which is exactly the absent-provenance meaning of `None`.
          None, generation,
        ));
        drop(reservation);
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
        let _ = reply.send(self.restore_group(now, gid, config, seed, fsm, generation));
        drop(reservation);
      }
      MultiCommand::RemoveGroup {
        gid,
        reply,
        reservation,
      } => {
        let _ = reply.send(self.remove_group(&gid));
        drop(reservation);
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
  /// generalization does not reshape the loop. Returns `true` on a FATAL limbo storage fault; the
  /// caller then fails THAT group's parked work `Poisoned` (group-scoped, never the driver).
  fn run_failover_serve(&mut self, gid: &G) -> bool {
    let Some(routing) = self.routing.get_mut(gid) else {
      return false;
    };
    if routing.failovers.is_empty() {
      return false;
    }
    let now = self.clock.now();
    let Some(ep) = self.coord.group(gid) else {
      return false;
    };
    match ep.failover_read_window(now) {
      None => {
        for p in std::mem::take(&mut routing.failovers) {
          (p.complete)(Ok(None));
        }
      }
      Some(window) if routing.applied >= window.index() => {
        let Some((log, _stable)) = self.engine.stores(gid) else {
          return false;
        };
        match sailing_driver::shared::read_limbo(log, &window, self.max_failover_limbo_bytes as u64)
        {
          Ok(Some(limbo)) => {
            let parked = std::mem::take(&mut routing.failovers);
            let fsm = ep.state_machine();
            sailing_driver::shared::serve_failover_batch(parked, fsm, &limbo, window, || {
              self
                .coord
                .group(gid)
                .is_some_and(|e| e.failover_read_window(self.clock.now()).is_some())
            });
          }
          Ok(None) => {
            for p in std::mem::take(&mut routing.failovers) {
              (p.complete)(Ok(None));
            }
          }
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
      let _ = self.events_tx.try_send((g, ev));
    }
    // Fold the coordinator's group-scoped scheduling signals IN DISPATCH ORDER: a flagged beat
    // quiesces its group (the leader promised heartbeat silence — the follower-side entry), and
    // any wake-class dispatch un-freezes it (the core re-armed its timers during the dispatch,
    // so the next sweep sees fresh deadlines — no stale fire).
    while let Some((g, ctrl)) = self.coord.poll_group_control() {
      match ctrl {
        GroupControl::Quiesce => {
          // A loss in this pass supersedes a Quiesce queued before it was known.
          if !self.link_lost_in_pass {
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
      if let Some(factory) = self.factory.as_mut()
        && let Some(blueprint) = factory.materialize(&group, &from)
        && blueprint_names(&blueprint, &from)
        // The floors gate, on the same seam the create below consults authoritatively: the
        // cheap PRE-BUILD refusal keeps the resource-phase ordering — a fenced id (or the
        // reserved `u64::MAX` sentinel, never a working incarnation) is refused before the
        // factory's build phase can be asked for a state machine.
        && floor_admits(self.engine.floor(&group), blueprint.generation())
        // The split-reservation gate, same seam: a solicited id that an in-flight split
        // reserves declines BEFORE build, so the local fork stays the id's one materializer
        // (the solicitation falls to the lifecycle tail and the sender retries).
        && !self.coord.is_split_reserved(&group)
        && let Some(fsm) = factory.build(&group)
      {
        let generation = blueprint.generation();
        let (config, seed) = blueprint.into_parts();
        if self
          .create_group(now, group.cheap_clone(), config, seed, fsm, generation)
          .is_ok()
        {
          continue;
        }
      }
      let _ = self
        .lifecycle_tx
        .try_send(LifecycleEvent::UnknownGroup { group, from });
    }
    // The single pump's completion tail, PER GROUP — one group's supersede or fail-stop never
    // touches a co-located group's parked work.
    let with_routing: Vec<G> = self.routing.keys().map(|g| g.cheap_clone()).collect();
    for g in &with_routing {
      self.pump_group_tail(g, run_queries.contains(g));
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

  /// One group's per-pass completion tail: watermark sync, the leadership-loss backstop, the
  /// failover serve, runnable queries, and the group-scoped poison sweep — the single-group
  /// pump's tail with every step keyed to `gid`. A poisoned group fails ITS parked work with the
  /// typed verdict; the driver keeps running for the co-located groups.
  fn pump_group_tail(&mut self, gid: &G, mut run_queries: bool) {
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
      run_queries = true;
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
      // A FATAL limbo storage fault is GROUP-scoped on a multi host: fail that group's parked
      // work with the typed verdict; co-located groups (and the driver) keep running.
      if let Some(routing) = self.routing.get_mut(gid) {
        routing.fail_all(&DriverError::Poisoned);
      }
      return;
    }
    if run_queries
      && let Some(ep) = self.coord.group(gid)
      && let Some(routing) = self.routing.get_mut(gid)
    {
      for q in routing.take_runnable_queries() {
        (q.complete)(Ok(ep.state_machine()));
      }
    }
    if poisoned && let Some(routing) = self.routing.get_mut(gid) {
      routing.fail_all(&DriverError::Poisoned);
    }
  }
}
