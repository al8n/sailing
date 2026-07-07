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
  ClusterId, Config, Endpoint, Event, FloorStore, GroupControl, GroupEngine, GroupId, Index,
  Instant, MultiQuicCoordinator, Now, ReadOnlyOption, StateMachine, StorageProgress, floor_admits,
  quic::QuicOptions,
};

use sailing_driver::{
  BoxedGroupFactory, GroupFactory, LifecycleEvent, MultiCommand, MultiHandle, Node, Status,
  jittered,
  shared::{InflightBudget, ParkedFailover, ParkedQuery, Pending, Routing},
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
  storage_ready: flume::Receiver<()>,
  _storage_ready_keepalive: Option<flume::Sender<()>>,
  peers: Vec<Node<I, SocketAddr>>,
  redial: BTreeMap<I, Redial>,
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
        storage_ready,
        _storage_ready_keepalive: keepalive,
        peers,
        redial: BTreeMap::new(),
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

  /// Drive every hosted group until shutdown (or until every `MultiHandle` clone has dropped and
  /// the buffered commands drained). The single-group QUIC loop's iteration order with the
  /// multi-group storage crank; teardown joins the recv task before dropping the socket (the
  /// fd-release barrier), exactly as on the single driver.
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

  /// Dial every configured peer with no bound connection whose backoff has elapsed (the
  /// single-group QUIC reconciler verbatim — the shared connections are group-agnostic).
  fn reconcile_peer_links(&mut self, now: Instant) {
    let std_now = std::time::Instant::now();
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
      let due = self.redial.get(&peer).is_none_or(|r| std_now >= r.at);
      if !due {
        continue;
      }
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
        // Re-sync to the LIVE restored counter (see the stream sibling): replay re-applies
        // lineage moves whose event-time mirror may have died with the crash.
        let live = self.coord.group(&gid).map_or(0, Endpoint::shape_gen);
        self.engine.set_group_gen(&gid, generation.max(live));
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
    if let Some(mut routing) = self.routing.remove(gid) {
      routing.fail_all(&DriverError::ShuttingDown);
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

  /// Serve (or fall back) ONE group's parked failover inherited-read queries (the stream
  /// sibling's verbatim — structurally inert on this monotonic-only v1 host, kept whole for the
  /// wall-clock generalization). Returns `true` on a FATAL limbo storage fault (group-scoped).
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
        Event::Merged(m) => m.gen_after(),
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
    let with_routing: Vec<G> = self.routing.keys().map(|g| g.cheap_clone()).collect();
    for g in &with_routing {
      self.pump_group_tail(g, run_queries.contains(g));
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

  /// One group's per-pass completion tail (the stream sibling's verbatim): watermark sync, the
  /// leadership-loss backstop, the failover serve, runnable queries, and the GROUP-scoped poison
  /// sweep — the driver keeps running for the co-located groups.
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
      // Deposition can be TIMER-driven (a CheckQuorum step-down) with no inbound wake control —
      // the role edge funnels the wake so a stale quiesce intent dies with the old leadership
      // (see the stream sibling).
      self.wake_group(gid);
      if let Some(routing) = self.routing.get_mut(gid) {
        routing.fail_all(&DriverError::Superseded);
      }
    }
    if !poisoned && self.run_failover_serve(gid) {
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
