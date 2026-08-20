//! [`MultiReactorQuicDriver`]: one `Send` task owning a [`MultiQuicCoordinator`], a shared
//! [`GroupEngine`], and a UDP socket, driving N co-located consensus groups over real QUIC
//! datagrams on any [`agnostic::Runtime`].
//!
//! The single-group [`ReactorQuicDriver`](crate::ReactorQuicDriver) loop generalized exactly as
//! the stream sibling ([`MultiReactorStreamDriver`](super::MultiReactorStreamDriver)) generalizes
//! its own: one recv task, awaited sends in the pump, the recv-task join as the fd-release
//! barrier — with the consensus steps fanned out per group and the storage crank's one engine
//! barrier per pass. Two QUIC-specific deltas: the shared transport timeout takes the ENGINE as
//! stores (the quinn bridge drain delivers consensus messages, unlike the stream transport's
//! stores-less handshake reap), and the node's transport identity latches from the FIRST admitted
//! group — a zero-group host closes inbound identity-less connections until then, so peers bind
//! via their standing redial once admission happens.

use std::{
  collections::{BTreeMap, BTreeSet},
  net::SocketAddr,
  sync::Arc,
  time::Duration,
};

use agnostic::{
  Runtime,
  net::{Net, UdpSocket},
};
use bytes::Bytes;
use sailing_proto::{
  ClusterId, Config, Endpoint, Event, FloorStore, ForkId, GroupControl, GroupEngine, GroupId,
  Index, Instant, MultiQuicCoordinator, Now, ReadOnlyOption, StateMachine, StorageProgress,
  floor_admits, quic::QuicOptions,
};

use sailing_driver::{
  BoxedGroupFactory, GroupFactory, LifecycleEvent, MultiCommand, MultiHandle, Node, Status,
  jittered,
  shared::{CompletionOutcome, InflightBudget, ParkedFailover, ParkedQuery, Pending, Routing},
  validate_and_capture_eps,
};

use crate::{
  BindError, Clock, DriverConfig, DriverError, Monotonic, driver::quic::recv_datagrams,
  task::AbortOnDrop,
};

use crate::driver::{map_propose_err, map_read_err, map_transfer_err};

use super::{
  EngineMetrics, FloorSnapshot, GroupActivity, PairFloors, STORAGE_REDRIVES, blueprint_names,
  conf_names, group_idle, host_seed, map_merge_err, map_split_err, no_such_group, rejected,
  reshape_born_factory_config, reshape_born_prevention,
};

/// Backstop wake cadence while configured peers exist (the link reconciler's pacing on an
/// otherwise-idle node — see the single-group QUIC driver).
const HOUSEKEEPING_INTERVAL: Duration = Duration::from_secs(1);
/// Per-iteration bound on each loop-top channel drain (datagrams, storage coalesce).
const IO_BUDGET: usize = 256;

/// Per-peer redial state (the single-group QUIC driver's verbatim).
struct Redial {
  at: std::time::Instant,
  backoff: Duration,
}

/// A multi-group consensus host over QUIC on a readiness runtime — the datagram sibling of
/// [`MultiReactorStreamDriver`](super::MultiReactorStreamDriver): same lifecycle commands, same
/// per-group routing and fault scope, same storage crank over the shared engine; only the I/O
/// model differs. Monotonic-only in v1, exactly as the stream sibling (a walled failover-tier
/// group config is rejected loudly at admission).
///
/// # Storage is in-memory — NOT crash-durable
///
/// Like its stream sibling, this host OWNS its [`GroupEngine`] (`GroupEngine::new`): the shared
/// IN-MEMORY reference engine, with no store-injection seam in v1, so a PROCESS CRASH loses ALL
/// consensus state — for tests, single-process deployments, and as the reference a persistent engine
/// is validated against, NOT for crash recovery. `restore_group` reconnects a group within the SAME
/// live process, not a recover-from-disk path. The lifecycle and event tails are best-effort
/// TELEMETRY for observability, never a correctness feed.
pub struct MultiReactorQuicDriver<R, G, I, F>
where
  R: Runtime,
  I: sailing_proto::NodeId,
  F: StateMachine,
{
  coord: MultiQuicCoordinator<G, I, F>,
  engine: GroupEngine<G, I>,
  socket: Arc<<R::Net as Net>::UdpSocket>,
  clock: Clock,
  /// Byte cap on each group's failover inherited-read limbo scan.
  max_failover_limbo_bytes: usize,
  commands: flume::Receiver<MultiCommand<G, I, F>>,
  /// Per-group routing state (see the stream sibling — group-scoped completions and sweeps).
  routing: BTreeMap<G, Routing<I, F::Response, F>>,
  /// The driver-owned, group-stamped events tail.
  events_tx: flume::Sender<(G, Event<I, F::Response>)>,
  /// The dead-end sender each per-group `Routing` is built over (receiver dropped at bind).
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
  storage_ready: flume::Receiver<()>,
  _storage_ready_keepalive: Option<flume::Sender<()>>,
  peers: Vec<Node<I, SocketAddr>>,
  redial: BTreeMap<I, Redial>,
  /// The earliest pending-redial instant, recomputed each reconcile pass and folded into the loop
  /// deadline so a due retry wakes on its own backoff schedule instead of on the ~1s housekeeping
  /// backstop. Snapshotting the minimum (rather than folding the raw `redial` map into the
  /// deadline) avoids a HOT-SPIN: an unbound peer whose backoff just elapsed carries a past `at`
  /// that would fire the timer every iteration until the next pass re-arms it.
  redial_wake: Option<std::time::Instant>,
  cmd_budget: usize,
  recv_cap: usize,
  redial_base: Duration,
  redial_cap: Duration,
  storage_closed: bool,
  /// Per-group leadership edge-detect (the supersede backstop, one entry per hosted group).
  was_leader: BTreeMap<G, bool>,
  /// The possibly-staged-engine-work flag deriving the immediate storage-redrive deadline (see
  /// the stream sibling's field — the engine's `has_pending` is READY-only by design).
  flush_pending: bool,
  /// Published engine counters (see [`EngineMetrics`]).
  metrics: EngineMetrics,
  /// Groups whose consensus deadlines the driver neither arms nor sweeps (see
  /// [`Self::quiesce_sweep`]); everything else flows normally for a quiesced group.
  quiesced: BTreeSet<G>,
  /// Whether ANY connection was lost in the current loop pass (set by [`Self::wake_all`]).
  /// A queued `GroupControl::Quiesce` decoded earlier in the same pass predates the loss, and
  /// honoring it would consume the liveness signal — the control drain drops `Quiesce` while this
  /// is set, so a same-pass close always wins. Cleared at the top of each pass.
  link_lost_in_pass: bool,
  /// Leader groups marked quiescing whose FLAGGED beat has not yet been stamped (they keep being
  /// swept until the coordinator consumes the intent, then move into `quiesced`).
  quiesce_pending: BTreeSet<G>,
  /// Per-group observed consensus state + the instant it last changed (the idle clock).
  activity: BTreeMap<G, GroupActivity>,
  /// Each group's election timeout, captured at admission — the quiesce-eligibility idle window.
  election: BTreeMap<G, Duration>,
  /// Configured peers observed BOUND on the last reconcile — the falling edge of
  /// [`MultiQuicCoordinator::has_bound_conn`] is this driver's connection-loss signal (QUIC has
  /// no socket EOF; quinn surfaces a loss by dropping the binding after its idle/keep-alive
  /// machinery gives up).
  bound_peers: BTreeSet<I>,
  teardown_tx: Option<futures_channel::oneshot::Sender<()>>,
}

impl<R, G, I, F> MultiReactorQuicDriver<R, G, I, F>
where
  R: Runtime,
  G: GroupId + Send,
  I: sailing_proto::NodeId + Send,
  F: StateMachine + Send,
  F::Command: sailing_proto::Data + Send,
  F::Snapshot: sailing_proto::Data,
  F::Response: Clone + Send,
  F::Error: core::error::Error,
{
  /// Bind `addr` and build an EMPTY host plus its [`MultiHandle`]: groups arrive via
  /// [`MultiHandle::create_group`] / [`MultiHandle::restore_group`], and the node's transport
  /// identity latches from the FIRST admitted group's config. Until then an inbound connection
  /// cannot be answered with an identity preface and is closed — a configured peer binds via its
  /// standing redial once the first group exists, so admit groups promptly after `run()` starts.
  ///
  /// `opts` must be a [`ClusterTls`](sailing_proto::quic::ClusterTls) bundle (the provided
  /// identity scheme requires mandatory mTLS), as on the single-group driver.
  pub async fn bind(
    addr: SocketAddr,
    opts: QuicOptions,
    cluster: ClusterId,
    peers: Vec<Node<I, SocketAddr>>,
    driver_cfg: DriverConfig,
  ) -> Result<(Self, MultiHandle<G, I, F>), BindError> {
    driver_cfg.validate()?;
    let socket = Arc::new(<R::Net as Net>::UdpSocket::bind(addr).await?);
    let coord = MultiQuicCoordinator::with_identity(opts, None, cluster);
    Ok(Self::from_parts(coord, socket, peers, driver_cfg))
  }

  /// Assemble the driver + [`MultiHandle`] from the bound socket.
  fn from_parts(
    coord: MultiQuicCoordinator<G, I, F>,
    socket: Arc<<R::Net as Net>::UdpSocket>,
    peers: Vec<Node<I, SocketAddr>>,
    driver_cfg: DriverConfig,
  ) -> (Self, MultiHandle<G, I, F>) {
    let (cmd_tx, cmd_rx) = flume::unbounded();
    let (event_tx, event_rx) = flume::bounded(driver_cfg.events_cap);
    let (lifecycle_tx, lifecycle_rx) = flume::bounded(driver_cfg.events_cap);
    let budget = InflightBudget::new(driver_cfg.max_inflight, driver_cfg.max_pending_bytes);
    let (teardown_tx, teardown_rx) = futures_channel::oneshot::channel();
    let handle = MultiHandle::new(cmd_tx, event_rx, lifecycle_rx, budget, teardown_rx);

    // The per-group Routing's stub tail (receiver dropped): the driver forwards the
    // group-stamped copies itself — see the stream sibling.
    let (stub_events_tx, _) = flume::bounded(0);

    let (storage_ready, keepalive) = match driver_cfg.storage_ready {
      Some(rx) => (rx, None),
      None => {
        let (tx, rx) = flume::bounded(1);
        (rx, Some(tx))
      }
    };

    (
      Self {
        coord,
        engine: GroupEngine::new(),
        socket,
        clock: Clock::new(None, Monotonic),
        max_failover_limbo_bytes: driver_cfg.max_failover_limbo_bytes,
        commands: cmd_rx,
        routing: BTreeMap::new(),
        events_tx: event_tx,
        stub_events_tx,
        lifecycle_tx,
        factory: None,
        forks_pending_flush: Vec::new(),
        merges_pending_flush: Vec::new(),
        storage_ready,
        _storage_ready_keepalive: keepalive,
        peers,
        redial: BTreeMap::new(),
        redial_wake: None,
        cmd_budget: driver_cfg.cmd_budget.max(1),
        recv_cap: driver_cfg.recv_cap,
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
        bound_peers: BTreeSet::new(),
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
  /// because the factory rides the `Send` `run()` future across a work-stealing runtime's
  /// threads.
  #[must_use]
  pub fn with_group_factory<Fac>(mut self, factory: Fac) -> Self
  where
    Fac: GroupFactory<G, I, F> + Send + 'static,
  {
    self.factory = Some(Box::new(factory));
    self
  }

  /// Drive every hosted group until an exit, then tear down (join the recv task before the socket
  /// drop — the fd-release barrier, exactly as on the single driver). `run` returns `()` with NO
  /// reason on a `shutdown()` command or every `MultiHandle` clone dropping (the command channel
  /// disconnects and the buffered commands drain). Fault scope is PER GROUP: a poisoned group does
  /// NOT exit `run` — it fails only its own parked work `Poisoned` while the other groups keep
  /// running. A typed `run() -> Result` exit is future work. The single-group QUIC loop's iteration
  /// order with the multi-group storage crank.
  pub async fn run(mut self) {
    use futures_util::{FutureExt, select_biased};

    let (recv_tx, recv_rx) = flume::bounded(self.recv_cap);
    let (recv_shutdown_tx, recv_shutdown_rx) = futures_channel::oneshot::channel();
    let recv_task = AbortOnDrop::<R>::new(R::spawn(recv_datagrams::<R>(
      self.socket.clone(),
      recv_tx,
      recv_shutdown_rx,
    )));

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

      // Fairness: a bounded datagram drain at the loop top (see the single-group driver).
      for _ in 0..IO_BUDGET {
        match recv_rx.try_recv() {
          Ok((datagram, from)) => {
            self
              .coord
              .handle_udp(self.clock.now(), from, None, &datagram, &mut self.engine);
            self.flush_pending = true;
          }
          Err(_) => break,
        }
      }

      // Fire an already-due deadline before the select. The armed deadline is the QUIESCE-AWARE
      // fold ([`Self::armed_deadline`]): the non-quiesced groups' earliest consensus deadline
      // plus the coordinator's TRANSPORT-only deadline (quinn's timers, the auth deadline, and
      // the bridge's deferred-work immediate deadline).
      if self
        .armed_deadline()
        .is_some_and(|d| d <= self.clock.mono())
      {
        self.fire_timeouts(now);
      }
      self.reconcile_peer_links(now.mono());
      self.pump(now).await;
      self.quiesce_sweep();

      let housekeeping =
        (!self.peers.is_empty()).then(|| std::time::Instant::now() + HOUSEKEEPING_INTERVAL);
      // The pending-flush flag stands in for a live staged-store check (see the stream sibling).
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

      enum Wake<G, I, F: StateMachine> {
        Datagram((Vec<u8>, SocketAddr)),
        Timer,
        Command(Option<MultiCommand<G, I, F>>),
        Storage,
        StorageClosed,
      }
      let wake = {
        let recv_fut = recv_rx.recv_async().fuse();
        let timer_fut =
          R::sleep(deadline.saturating_duration_since(std::time::Instant::now())).fuse();
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
          got = recv_fut => Wake::Datagram(got.expect("recv_task outlives the loop")),
          _ = timer_fut => Wake::Timer,
          cmd = cmd_fut => Wake::Command(cmd.ok()),
          got = storage_fut => {
            if got.is_err() { Wake::StorageClosed } else { Wake::Storage }
          }
        }
      };
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
            .handle_udp(now, from, None, &datagram, &mut self.engine);
          self.flush_pending = true;
        }
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

    // Teardown. Classify each group's fail-stop FIRST, then the ShuttingDown sweep; then stop the
    // recv task and AWAIT its join so the final socket drop is the fd-release barrier (see the
    // single-group driver's teardown notes).
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
    let _ = recv_shutdown_tx.send(());
    drop(recv_rx);
    if let Some(handle) = recv_task.into_handle() {
      let _ = handle.await;
    }
    while let Ok(cmd) = self.commands.try_recv() {
      drop(cmd);
    }
    drop(self.commands);
    drop(self.socket);
    if let Some(tx) = self.teardown_tx.take() {
      let _ = tx.send(());
    }
  }

  /// Fire the shared QUIC transport timers — WITH the engine as stores: the quinn bridge drain
  /// delivers group-tagged consensus messages, unlike the stream transport's stores-less reap —
  /// then every DUE group's consensus timers (the v1 aggregate-timer dispatch; the indexed wheel
  /// over [`MultiQuicCoordinator::deadlines`] is the scale refinement seam).
  fn fire_timeouts(&mut self, now: Now) {
    self.coord.handle_transport_timeout(now, &mut self.engine);
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
    // The transport drain itself can deliver consensus messages that stage writes (not just the
    // due-group sweep), so the flag is set unconditionally here.
    self.flush_pending = true;
  }

  /// The per-crank storage step (the stream sibling's verbatim): one gated engine barrier, then
  /// every hosted group's bounded completion drain.
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
      // Reshape-born prevention: a split child is a reshaping participant, so it is FORCED
      // to run pre-vote + check-quorum — an ignorant removed voter must not depose a live leader
      // while reshaping keeps membership churn steady-state. Embedder `with_group` groups keep their
      // configured (etcd-parity) defaults; this force applies at reshape birth only.
      let child_config = reshape_born_prevention(fork.config);
      match self.create_group_from_fork(
        now,
        fork.child,
        child_config,
        seed,
        fork.fsm,
        fork.blob,
        fork.read_only,
        // The child's provenance token rides its manufactured baseline so every replica reports
        // the same origin and the parent's parked fork resolves redundant only against a match.
        Some(fork.fork_id),
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
    // A fail-stopped group surfaces once on the same best-effort tail; a full tail drops the
    // observation and the embedder still finds the poison by direct inspection.
    while let Some(group) = self.coord.poll_poisoned() {
      let _ = self
        .lifecycle_tx
        .try_send(LifecycleEvent::Poisoned { group });
    }
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
    // The engine's staged-work signal, measured AFTER the drains, is the exact re-arm predicate —
    // a release-count inference would miss a write the storage tail staged during a crank whose
    // barrier released nothing (see the stream sibling).
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
    self.flush_pending = self.engine.has_staged() || more;
    self
      .metrics
      .record(self.engine.barriers(), self.engine.ops_batched());
    self
      .metrics
      .record_fenced(self.coord.fenced_frames_dropped());
  }

  /// Dial every configured peer with no bound connection whose backoff has elapsed (the
  /// single-group QUIC reconciler verbatim — the shared connections are group-agnostic).
  fn reconcile_peer_links(&mut self, now: Instant) {
    let std_now = std::time::Instant::now();
    let mut wake: Option<std::time::Instant> = None;
    for node in self.peers.clone() {
      let (peer, addr) = node.into_parts();
      if self.coord.has_bound_conn(&peer) {
        self.redial.remove(&peer);
        self.bound_peers.insert(peer);
        continue;
      }
      // The FALLING edge of an established binding is this transport's connection-loss signal:
      // wake every quiesced group — connection health is the quiesce liveness oracle (a dead
      // leader's connection idles out here, the followers' stale election deadlines re-enter the
      // fold and fire immediately — the desired leader-failure election). Whole-set clearing is
      // deliberately conservative; per-leader scoping is the refinement seam.
      if self.bound_peers.remove(&peer) {
        self.wake_all();
      }
      if let Some(r) = self.redial.get(&peer)
        && std_now < r.at
      {
        wake = Some(wake.map_or(r.at, |w| w.min(r.at))); // backing off: wake to retry
        continue;
      }
      let _ = self.coord.connect(now, addr, peer.cheap_clone());
      let backoff = self
        .redial
        .get(&peer)
        .map(|r| (r.backoff * 2).min(self.redial_cap))
        .unwrap_or(self.redial_base);
      let at = std_now + jittered(backoff);
      self.redial.insert(peer, Redial { at, backoff });
      // Arm the wake for this attempt even if it produced no future event, so the next retry fires
      // on its own schedule rather than waiting for the housekeeping backstop.
      wake = Some(wake.map_or(at, |w| w.min(at)));
    }
    self.redial_wake = wake;
  }

  /// Admit a group's driver-side client state (routing + the leadership edge-detect).
  fn admit_group(&mut self, gid: G) {
    self
      .routing
      .insert(gid.cheap_clone(), Routing::new(self.stub_events_tx.clone()));
    self.was_leader.insert(gid, false);
  }

  /// Create a fresh group: engine storage + coordinator endpoint + driver routing together (see
  /// the stream sibling — same admission gate, same rollback; the coordinator consults THE
  /// ENGINE as its floor store, and the driver records the admitted incarnation after the `Ok`).
  fn create_group(
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
        if added {
          self.engine.remove_group(&gid);
        }
        Err(rejected(e))
      }
    }
  }

  /// Recover a group from the engine's storage, the driver deriving the boot epoch. The floor
  /// check reads a pre-call [`FloorSnapshot`] of the engine's lineage (the engine itself is lent
  /// to the restore as `(log, stable)`).
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
      floor: self.engine.group_floor(&gid),
      lineage: self.engine.group_gen(&gid),
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
        // Re-sync to the LIVE restored counter (see the stream sibling): replay re-applies
        // lineage moves whose event-time mirror may have died with the crash.
        let live = self.coord.group(&gid).map_or(0, Endpoint::shape_gen);
        self.engine.set_group_gen(&gid, generation.max(live));
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

  /// Create a group from LOCALLY-FORKED state (the stream sibling's manufactured-baseline flow
  /// verbatim over the QUIC coordinator): [`FloorSnapshot`] pre-read, engine boot epoch, the
  /// baseline staged behind the next barrier, and create's rollback discipline on refusal.
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
    fork_id: Option<ForkId>,
    generation: u64,
  ) -> Result<(), DriverError<I>> {
    validate_and_capture_eps::<I, Monotonic>(&config).map_err(rejected)?;
    let election = config.election_timeout();
    let lineage = FloorSnapshot {
      floor: self.engine.group_floor(&gid),
      lineage: self.engine.group_gen(&gid),
    };
    let added = self.engine.add_group(gid.cheap_clone());
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
        fork_id,
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

  /// Remove a group: endpoint, storage, and routing torn down together; the group's parked work
  /// fails with the group-scoped teardown verdict. `Ok` carries whether the group was hosted; an
  /// UNRESOLVED merge participant refuses TRANSIENTLY ([`DriverError::Rejected`], the coordinator's
  /// inherited container gate — a thaw owed, a frozen source, a parked target, or a group a park
  /// names), tearing nothing down.
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
    // (see the stream sibling).
    let floor = self.engine.removal_floor(gid);
    if floor > 0 {
      self.engine.set_group_floor(gid, floor);
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

  /// Handle one command (the stream sibling's dispatch verbatim over the QUIC coordinator).
  /// Returns `true` when the loop should exit (a `Shutdown`).
  fn handle_command(&mut self, now: Now, cmd: MultiCommand<G, I, F>) -> bool {
    // Any group-addressed CLIENT operation un-quiesces its group BEFORE dispatch (see the stream
    // sibling); pure observability (`Status`) and the lifecycle commands do not wake.
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
        let verdict = match self.engine.stores(&target) {
          None => Err(no_such_group()),
          Some((log, stable)) => {
            match self
              .coord
              .rollback_merge(&target, now, log, stable, &source)
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
          None,
          // ...and it carries no split provenance token — only a committed split's fork does.
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

  /// Serve (or fall back) ONE group's parked failover inherited-read queries (the stream
  /// sibling's verbatim — structurally inert on this monotonic-only v1 host, kept whole for the
  /// wall-clock generalization).
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

  /// Drain the coordinator's aggregate outputs: datagrams to the socket (awaited sends, exactly
  /// as on the single-group driver), group-stamped events into each group's routing plus the
  /// driver-owned stamped tail — then each hosted group's completion tail. No conn-closed drain:
  /// QUIC connection teardown is the coordinator's own.
  async fn pump(&mut self, now: Now) {
    let hosted: Vec<G> = self.engine.group_ids().map(|g| g.cheap_clone()).collect();
    for g in &hosted {
      if let Some((log, stable)) = self.engine.stores(g) {
        let _ = self.coord.flush_appends(g, now, log, stable);
      }
    }
    while let Some((dest, bytes)) = self.coord.poll_transmit() {
      let _ = self.socket.send_to(&bytes, dest).await;
    }
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
      // THE LINEAGE MIRROR (see the stream sibling): fold each applied merge move — and an
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
    // quiesces its group (the follower-side entry); any wake-class dispatch un-freezes it (the
    // core re-armed its timers during the dispatch — no stale fire).
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
    // so no removal or tombstone can interleave between the signal and the admission (see the
    // stream sibling). The order within one signal is the resource-safety line: materialize (the
    // cheap catalog phase) → the sender gate ([`blueprint_names`], enforced BEFORE build so an
    // unauthorized valid-cert solicitor can never force state-machine construction) → build →
    // the exact CreateGroup command path. An admitted build consumes the signal; a decline, a
    // refused blueprint, a build abort (`None`), or a create refusal falls through to the
    // lifecycle tail as before.
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
    // The single pump's completion tail, split PLANE-WIDE into a TEAR phase and a SERVE phase (the
    // stream sibling's phasing verbatim).
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
  /// the crank's authoritative pre-serve harvest (the stream sibling's verbatim).
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
  /// stream sibling's `a_harvest_of_one_latched_routing_fail_stops_every_hosted_group`.
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
  /// coordinator's TRANSPORT-only deadline ([`MultiQuicCoordinator::transport_timeout`]: quinn's
  /// timers, the auth reap, and the bridge's deferred-work immediate deadline) — the
  /// `poll_timeout()` decomposition quiescing requires. A quiesced group's deadline is excluded
  /// here AND skipped in [`Self::fire_timeouts`]'s due sweep; the core still records it, so a
  /// wake re-admits it and a long-stale one fires immediately.
  fn armed_deadline(&mut self) -> Option<Instant> {
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

  /// Wake EVERY quiesced (and pending) group — the connection-loss path (see
  /// [`Self::reconcile_peer_links`]).
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

  /// The per-iteration quiesce scheduler (the stream sibling's verbatim — see its notes on the
  /// two phases and on why OBSERVED-state diffing, not dispatch counting, is the idle clock).
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
        self.coord.mark_quiescing(g);
        self.quiesce_pending.insert(g.cheap_clone());
      }
    }
    self.metrics.record_quiesced(self.quiesced.len() as u64);
  }

  /// One group's PRE-SERVE step (the stream sibling's verbatim): watermark sync, the leadership-loss
  /// backstop, and the failover serve/decline — every completion this group runs in a crank that is NOT
  /// the parked-query serve. The pump runs it for EVERY group before ANY group serves, and it routes
  /// this group's completion-panic latch at its END: the sweeps and declines here drop unused user
  /// closures, a captured guard's `Drop` can tear any hosted group's FSM, so the verdict must become a
  /// plane-wide poison before the NEXT group's step (whose failover serve would otherwise read a torn
  /// state machine) and before every group's query serve.
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
      // Deposition can be TIMER-driven (a CheckQuorum step-down) with no inbound wake control —
      // the role edge funnels the wake so a stale quiesce intent dies with the old leadership
      // (see the stream sibling).
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
