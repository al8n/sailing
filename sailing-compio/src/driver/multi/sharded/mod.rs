//! The sharded thread-per-core multi-raft host: K PARALLEL PLANES, each a complete
//! [`CompioMultiStreamDriver`] on its own core, behind one group-routing
//! [`ShardedMultiHandle`].
//!
//! # The plane model
//!
//! Every core runs a FULL compio multi-group driver — its own fused coordinator, its own
//! [`GroupEngine`](sailing_proto::GroupEngine) (a per-core WAL barrier: zero cross-core fsync
//! contention), and its own TCP listener on a per-shard port — hosting the disjoint subset of
//! groups a cluster-wide-consistent [`ShardMap`] assigns it. Because every node runs the SAME
//! map (same K, same mapping), group `g`'s replicas always talk `shard(g)` ↔ `shard(g)`: the
//! cluster decomposes into K independent meshes, one connection per peer PER PLANE, and the
//! router's one-connection-per-peer dedup holds within each plane. NO cross-core hop exists
//! anywhere on the hot path — a frame lands on the plane that owns its group, and
//! conn → consensus → storage all stay core-local. Every multi feature (heartbeat coalescing,
//! quiescence, tombstones, the lifecycle tail, the group factory) works per-plane UNCHANGED,
//! because a plane IS a full multi driver — with ONE host-added gate: each plane's factory is
//! shard-guarded, so a group the map assigns to a different plane can never materialize on it
//! (mis-routed solicitations surface as unknown-group on the shared tail; mis-routed
//! non-initial frames drop as unhosted). A connection loss wakes exactly the one plane that
//! owned the connection.
//!
//! What is shared is only the CLIENT surface: one events tail, one lifecycle tail, and one
//! [`InflightBudget`](sailing_driver::shared::InflightBudget) — every plane publishes into the
//! same channels (fan-in by construction, no merge task), and the sharded handle routes each
//! group-keyed operation to its plane's command channel through the shard map.

#[cfg(test)]
mod tests;

use std::{net::SocketAddr, sync::Arc};

use sailing_proto::{Config, Event, GroupId, RecordIo, StateMachine};

use sailing_driver::{
  BindError, BoxedGroupFactory, DriverConfigError, GroupBlueprint, GroupFactory, GroupHandle,
  LifecycleEvent, MultiHandle, Node,
};

use crate::{
  DriverConfig, DriverError,
  driver::stream::{AcceptorFactory, DialerFactory},
};

use super::{CompioMultiStreamDriver, EngineMetrics, SharedTails};

/// Builds one plane's record-layer factory pair, ON that plane's thread. `Send + Sync` because
/// the ONE provider is shared across every plane thread; the returned factories are `Rc` and
/// stay on the thread that called it (the compio construct-where-you-run pinning), so the
/// provider must capture only `Send` material (DER bytes, cluster ids) and build the actual
/// rustls/`Rc` state per call. A failure aborts that plane's spawn with a typed
/// [`SpawnError::RecordLayers`].
pub type ShardRecordLayers<I, R> =
  Arc<dyn Fn(usize) -> std::io::Result<(DialerFactory<I, R>, AcceptorFactory<R>)> + Send + Sync>;

/// The per-plane group-factory slots: consulted once per shard at [`ShardedCompioHost::spawn`]
/// (on the spawning thread; the boxed factory then moves into its plane), so each plane gets its
/// OWN instance — factories are stateful (`&mut self` phases) and a plane must never share one.
/// `None` leaves that plane factory-less.
type FactorySlots<G, I, F> = Box<dyn FnMut(usize) -> Option<BoxedGroupFactory<G, I, F>>>;

/// FNV-1a over one group id's canonical [`Data`](sailing_proto::Data) encoding — deterministic
/// across processes, platforms, and runs (no `RandomState`, no pointer identity), which is what
/// lets the uniform map be CLUSTER-WIDE consistent by construction.
fn fnv1a(bytes: &[u8]) -> u64 {
  let mut hash = 0xcbf2_9ce4_8422_2325u64;
  for byte in bytes {
    hash ^= u64::from(*byte);
    hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
  }
  hash
}

/// The cluster-wide group → plane assignment. **The contract: every node of the cluster runs the
/// SAME map — same shard count, same mapping.** Replication traffic for group `g` flows only
/// between the planes named `shard(g)` on each node (plane `i` listens on and dials per-shard
/// ports), so two nodes disagreeing on the map would dial each other's WRONG planes. The host
/// enforces the contract fail-closed on the automatic path: a plane consults its
/// [`GroupFactory`] only for groups this map assigns to it, so a mis-routed solicitation
/// surfaces as [`LifecycleEvent::UnknownGroup`] on the lifecycle tail (and a mis-routed
/// non-initial frame drops as unhosted) instead of materializing a replica on a plane no
/// correctly-configured peer ever dials. The default mapping is uniform FNV-1a over the group
/// id's canonical `Data` encoding — a fixed algorithm with no per-process state — and a custom
/// mapping must be equally deterministic and deployed cluster-wide.
pub struct ShardMap<G> {
  shards: usize,
  mapping: Mapping<G>,
}

enum Mapping<G> {
  Uniform,
  /// The embedder's override (e.g. a locality-aware assignment). Folded `% shards` on use, so a
  /// wild return value can never index out of the plane vector.
  Custom(Arc<dyn Fn(&G) -> usize + Send + Sync>),
}

impl<G> Clone for ShardMap<G> {
  fn clone(&self) -> Self {
    Self {
      shards: self.shards,
      mapping: match &self.mapping {
        Mapping::Uniform => Mapping::Uniform,
        Mapping::Custom(f) => Mapping::Custom(f.clone()),
      },
    }
  }
}

impl<G> ShardMap<G>
where
  G: GroupId,
{
  /// The uniform default: `fnv1a(g.encode()) % shards`. `shards` is clamped to at least 1 (the
  /// same silent floor the drivers apply to `cmd_budget`).
  #[must_use]
  pub fn uniform(shards: usize) -> Self {
    Self {
      shards: shards.max(1),
      mapping: Mapping::Uniform,
    }
  }

  /// An embedder-supplied assignment in place of the uniform hash. The returned shard is folded
  /// `% shards`; the cluster-wide-consistency contract on the type applies to `f` verbatim.
  #[must_use]
  pub fn with_mapping<M>(shards: usize, f: M) -> Self
  where
    M: Fn(&G) -> usize + Send + Sync + 'static,
  {
    Self {
      shards: shards.max(1),
      mapping: Mapping::Custom(Arc::new(f)),
    }
  }

  /// The plane count K.
  #[must_use]
  pub const fn shards(&self) -> usize {
    self.shards
  }

  /// The plane hosting `group` — on every node of the cluster, by the consistency contract.
  #[must_use]
  pub fn shard(&self, group: &G) -> usize {
    match &self.mapping {
      Mapping::Uniform => {
        let mut key = Vec::new();
        group.encode(&mut key);
        (fnv1a(&key) % self.shards as u64) as usize
      }
      Mapping::Custom(f) => f(group) % self.shards,
    }
  }
}

/// The host-installed guard around one plane's embedder factory: [`materialize`] consults the
/// [`ShardMap`] FIRST and declines — without ever asking the inner factory — any group the map
/// assigns to a different plane. [`ShardedCompioHost::spawn`] wraps every factory the per-shard
/// slots yield, OUTSIDE the embedder's code, so nothing a factory implementation does can
/// bypass it. The hazard it closes: under a K/map/addressing skew between nodes, a peer's
/// solicitation for a group lands on the WRONG plane here, and a catalog-backed factory (the
/// natural embedder shape — catalogs are not plane-aware) would materialize it there; a later
/// correctly-routed create then leaves TWO local replicas of the group under this node's ONE
/// identity, on independent WAL barriers — one voter that can ack and vote twice. With the
/// guard the decline falls into the driver's ordinary path: no build, no create, and the
/// solicitation surfaces as [`LifecycleEvent::UnknownGroup`] on the shared tail, so the
/// embedder OBSERVES the skew instead of the cluster silently splitting a group. The contract
/// this hands the embedder: a plane's factory only ever sees its own plane's groups, so a
/// catalog-backed factory needs no shard awareness of its own.
///
/// [`materialize`]: GroupFactory::materialize
struct ShardGuardedFactory<G, I, F> {
  inner: BoxedGroupFactory<G, I, F>,
  map: ShardMap<G>,
  plane: usize,
}

impl<G, I, F> GroupFactory<G, I, F> for ShardGuardedFactory<G, I, F>
where
  G: GroupId,
{
  fn materialize(&mut self, group: &G, from: &I) -> Option<GroupBlueprint<I>> {
    if self.map.shard(group) != self.plane {
      // Fail closed BEFORE the embedder's catalog is even asked: a group that does not belong
      // to this plane must never materialize here, whatever the inner factory would say.
      return None;
    }
    self.inner.materialize(group, from)
  }

  fn build(&mut self, group: &G) -> Option<F> {
    // Delegating without a re-check is sound: the drivers' two-phase protocol calls `build`
    // only immediately after a `Some` from `materialize` for the same group within the same
    // drain step, and the guard above already scoped that `Some` to this plane.
    self.inner.build(group)
  }
}

/// A typed refusal from [`ShardedCompioHost::spawn`]. Everything here is a STARTUP error — once
/// `spawn` returns `Ok`, plane failures are per-group/per-plane runtime concerns surfaced
/// through the ordinary driver verdicts.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SpawnError {
  /// The shared [`DriverConfig`] is invalid (validated once up front; every plane would reject
  /// it identically at bind).
  #[error("invalid driver config: {0}")]
  Config(#[from] DriverConfigError),
  /// An explicit listen-address list does not carry exactly one address per shard.
  #[error("expected {expected} per-shard listen addresses, got {got}")]
  ListenAddrCount {
    /// The shard count the map fixed.
    expected: usize,
    /// The list length actually supplied.
    got: usize,
  },
  /// The `base port + shard` convention overflows a `u16` port for this base address (listen or
  /// peer side).
  #[error("the per-shard port convention overflows u16: base {base} across {shards} shards")]
  PortOverflow {
    /// The base address whose port arithmetic overflowed.
    base: SocketAddr,
    /// The shard count being derived.
    shards: usize,
  },
  /// One plane's compio runtime failed to build.
  #[error("shard {shard}: the compio runtime failed to build")]
  Runtime {
    /// The failed plane.
    shard: usize,
    /// The runtime construction error.
    source: std::io::Error,
  },
  /// One plane's record-layer provider failed (a mis-built TLS config, a bad local id).
  #[error("shard {shard}: the record-layer provider failed")]
  RecordLayers {
    /// The failed plane.
    shard: usize,
    /// The provider's error.
    source: std::io::Error,
  },
  /// One plane's driver failed to bind (port in use, invalid address).
  #[error("shard {shard}: bind failed")]
  Bind {
    /// The failed plane.
    shard: usize,
    /// The bind error.
    source: BindError,
  },
  /// One plane's thread exited without reporting a bind verdict (a panic in the plane's
  /// runtime bring-up).
  #[error("shard {shard}: the plane thread exited before reporting a bind verdict")]
  PlaneFailed {
    /// The failed plane.
    shard: usize,
  },
}

/// One plane's bind verdict, shipped from its thread during [`ShardedCompioHost::spawn`]: the
/// routing ingredients on success, the typed startup refusal otherwise.
type Verdict<G, I, F> = Result<(MultiHandle<G, I, F>, EngineMetrics), SpawnError>;

/// One spawned plane, as the spawner tracks it until its verdict arrives.
type Plane<G, I, F> = (
  std::sync::mpsc::Receiver<Verdict<G, I, F>>,
  std::thread::JoinHandle<()>,
);

/// `base` with its port advanced by `shard`, or `None` on `u16` overflow.
fn shard_addr(base: SocketAddr, shard: usize) -> Option<SocketAddr> {
  let offset = u16::try_from(shard).ok()?;
  let port = base.port().checked_add(offset)?;
  Some(SocketAddr::new(base.ip(), port))
}

/// The builder for the sharded host: K planes, each a complete [`CompioMultiStreamDriver`] on
/// its own `std::thread` + compio runtime, behind one [`ShardedMultiHandle`].
///
/// # The plane model (read the [module docs](self) first)
///
/// - K independent meshes: plane `i` listens on `listen base port + i` and dials every peer's
///   `base port + i` — the ADDRESSING contract mirrors the [`ShardMap`]'s: every node of the
///   cluster runs the same K, the same map, and the same port convention (or the same explicit
///   per-shard lists), so `shard(g)` on one node always reaches `shard(g)` on another.
/// - Per-plane engines: each plane owns its own [`GroupEngine`](sailing_proto::GroupEngine) —
///   K independent durability barriers, no cross-core fsync contention, observable per plane
///   through [`ShardedMultiHandle::engine_metrics`].
/// - Per-plane multi semantics unchanged: coalescing, quiescence, tombstones, lifecycle
///   surfacing, and factories all run inside each plane exactly as on a single multi driver;
///   a connection loss wakes only the plane that owned the connection.
/// - Fail closed under skew: every registered factory is wrapped in a host-installed shard
///   guard, so traffic for a group the [`ShardMap`] assigns to a DIFFERENT plane (a peer
///   running a different K, map, or addressing) can never materialize state on the plane it
///   mistakenly reached — the uniform-map contract is ENFORCED on the automatic path, not
///   merely documented. The skew surfaces as [`LifecycleEvent::UnknownGroup`] on the shared
///   tail instead of a second local replica acking under this node's one identity.
/// - Shared client surface: ONE events tail, ONE lifecycle tail, ONE in-flight budget across
///   all planes (co-located planes share the host's memory, so they share its bound).
///
/// `spawn` blocks briefly: it starts the K plane threads and waits for each plane's bind
/// verdict, so a returned handle means every listener is bound and every plane is running.
pub struct ShardedCompioHost<G, I, F, R>
where
  F: StateMachine,
{
  map: ShardMap<G>,
  listen: ListenAddrs,
  peers: Vec<Node<I, SocketAddr>>,
  record_layers: ShardRecordLayers<I, R>,
  factories: Option<FactorySlots<G, I, F>>,
  snapshot_staging_cap: Option<usize>,
  driver_cfg: DriverConfig,
}

enum ListenAddrs {
  /// Plane `i` listens on `base` with its port advanced by `i` — the port convention.
  Base(SocketAddr),
  /// One explicit address per plane (length must equal the shard count).
  Explicit(Vec<SocketAddr>),
}

impl<G, I, F, R> ShardedCompioHost<G, I, F, R>
where
  G: GroupId + Send,
  I: sailing_proto::NodeId + Send,
  F: StateMachine + Send + 'static,
  F::Command: sailing_proto::Data + Send,
  F::Snapshot: sailing_proto::Data,
  F::Response: Clone + Send,
  F::Error: core::error::Error,
  R: RecordIo + 'static,
{
  /// Start describing a host: the cluster-wide [`ShardMap`], this node's LISTEN base address
  /// (plane `i` binds `base port + i`; override with
  /// [`with_listen_addrs`](Self::with_listen_addrs)), the peer book of BASE addresses (plane `i`
  /// dials each peer's `base port + i` — every node must run the same K and convention), the
  /// per-plane record-layer provider, and the [`DriverConfig`] every plane runs under.
  #[must_use]
  pub fn new(
    map: ShardMap<G>,
    listen_base: SocketAddr,
    peers: Vec<Node<I, SocketAddr>>,
    record_layers: ShardRecordLayers<I, R>,
    driver_cfg: DriverConfig,
  ) -> Self {
    Self {
      map,
      listen: ListenAddrs::Base(listen_base),
      peers,
      record_layers,
      factories: None,
      snapshot_staging_cap: None,
      driver_cfg,
    }
  }

  /// Replace the port convention with one explicit listen address per plane (for hosts whose
  /// shards cannot share one IP or a contiguous port range). The list length must equal the
  /// map's shard count — validated at [`spawn`](Self::spawn). Peer dialing keeps the base + `i`
  /// convention regardless: the CLUSTER-wide addressing contract is per-node, and a node using
  /// explicit local addresses must still be reachable at its advertised convention.
  #[must_use]
  pub fn with_listen_addrs(mut self, addrs: Vec<SocketAddr>) -> Self {
    self.listen = ListenAddrs::Explicit(addrs);
    self
  }

  /// Register per-plane group factories: `per_shard` is consulted once per shard at
  /// [`spawn`](Self::spawn) (on the spawning thread), and each `Some` factory moves into its
  /// plane — one INSTANCE per plane, because a factory is stateful and a plane must never share
  /// one. A plane given `None` runs factory-less (its solicitations fall through to the shared
  /// lifecycle tail). Every installed factory sits behind a host-owned shard guard: a plane
  /// consults its factory only for groups the [`ShardMap`] assigns to that plane and declines
  /// the rest before the factory is even asked — so a catalog-backed factory needs no shard
  /// filtering of its own, and a skewed peer's mis-routed solicitation can never trick a plane
  /// into materializing a group the map places elsewhere (it surfaces as
  /// [`LifecycleEvent::UnknownGroup`] instead).
  #[must_use]
  pub fn with_group_factories<Fac>(mut self, per_shard: Fac) -> Self
  where
    Fac: FnMut(usize) -> Option<BoxedGroupFactory<G, I, F>> + 'static,
  {
    self.factories = Some(Box::new(per_shard));
    self
  }

  /// Cap every plane engine's chunked-snapshot staging buffers (the
  /// [`GroupEngine`](sailing_proto::GroupEngine) knob, applied per plane).
  #[must_use]
  pub fn with_snapshot_staging_cap(mut self, cap: usize) -> Self {
    self.snapshot_staging_cap = Some(cap);
    self
  }

  /// Start the K planes — one `std::thread` each, running a compio runtime whose whole life is
  /// that plane's driver (the driver is CONSTRUCTED on its plane thread: a compio socket
  /// attaches to the proactor of the constructing thread) — and return the routing handle once
  /// every plane reports its listener bound. On any plane's failure the already-started planes
  /// are torn down (their handles drop, their threads are joined) before the typed error
  /// returns, so a failed spawn leaks neither threads nor sockets.
  pub fn spawn(mut self) -> Result<ShardedMultiHandle<G, I, F>, SpawnError> {
    self.driver_cfg.validate()?;
    let shards = self.map.shards();
    let listen: Vec<SocketAddr> = match self.listen {
      ListenAddrs::Explicit(addrs) => {
        if addrs.len() != shards {
          return Err(SpawnError::ListenAddrCount {
            expected: shards,
            got: addrs.len(),
          });
        }
        addrs
      }
      ListenAddrs::Base(base) => (0..shards)
        .map(|shard| shard_addr(base, shard))
        .collect::<Option<Vec<_>>>()
        .ok_or(SpawnError::PortOverflow { base, shards })?,
    };
    // Validate the PEER port arithmetic up front too: a dial-side overflow would otherwise
    // surface as a mysterious per-plane connectivity hole at runtime.
    for node in &self.peers {
      let base = *node.addr_ref();
      if shard_addr(base, shards - 1).is_none() {
        return Err(SpawnError::PortOverflow { base, shards });
      }
    }

    // ONE set of fan-in tails, cloned into every plane: one events channel, one lifecycle
    // channel, one budget — the fan-in is the channel itself, no merge task.
    let tails: SharedTails<G, I, F> = SharedTails::new(&self.driver_cfg);
    let events = tails.events_rx.clone();
    let lifecycle = tails.lifecycle_rx.clone();

    let mut planes: Vec<Plane<G, I, F>> = Vec::with_capacity(shards);
    for (shard, addr) in listen.into_iter().enumerate() {
      let peers: Vec<Node<I, SocketAddr>> = self
        .peers
        .iter()
        .map(|node| {
          let (id, base) = node.clone().into_parts();
          Node::new(
            id,
            shard_addr(base, shard).expect("peer port arithmetic validated above"),
          )
        })
        .collect();
      let record_layers = self.record_layers.clone();
      let tails = tails.clone();
      let driver_cfg = self.driver_cfg.clone();
      // Every factory the slots yield is wrapped in the shard guard HERE, outside the
      // embedder's code: the plane refuses to materialize groups the map assigns elsewhere.
      // The map clone is cheap — a count plus, for a custom mapping, one shared `Arc`'d fn.
      let factory = self
        .factories
        .as_mut()
        .and_then(|slots| slots(shard))
        .map(|inner| {
          Box::new(ShardGuardedFactory {
            inner,
            map: self.map.clone(),
            plane: shard,
          }) as BoxedGroupFactory<G, I, F>
        });
      let staging_cap = self.snapshot_staging_cap;
      let (verdict_tx, verdict_rx) = std::sync::mpsc::channel::<Verdict<G, I, F>>();
      let thread = std::thread::spawn(move || {
        let runtime = match compio::runtime::Runtime::new() {
          Ok(runtime) => runtime,
          Err(source) => {
            let _ = verdict_tx.send(Err(SpawnError::Runtime { shard, source }));
            return;
          }
        };
        runtime.block_on(async move {
          // The record layers are built HERE, on the plane's thread: the returned factories
          // are `Rc` and must never cross threads.
          let (dialer, acceptor) = match (record_layers)(shard) {
            Ok(pair) => pair,
            Err(source) => {
              let _ = verdict_tx.send(Err(SpawnError::RecordLayers { shard, source }));
              return;
            }
          };
          let bound = CompioMultiStreamDriver::bind_with_tails(
            addr, peers, dialer, acceptor, driver_cfg, tails,
          )
          .await;
          let (mut driver, handle) = match bound {
            Ok(pair) => pair,
            Err(source) => {
              let _ = verdict_tx.send(Err(SpawnError::Bind { shard, source }));
              return;
            }
          };
          if let Some(cap) = staging_cap {
            driver = driver.with_snapshot_staging_cap(cap);
          }
          if let Some(factory) = factory {
            driver = driver.with_boxed_group_factory(factory);
          }
          let metrics = driver.engine_metrics();
          if verdict_tx.send(Ok((handle, metrics))).is_err() {
            // The spawner unwound (another plane failed): stop before running.
            return;
          }
          driver.run().await;
        });
      });
      planes.push((verdict_rx, thread));
    }

    // Collect every plane's bind verdict, in shard order.
    let mut handles = Vec::with_capacity(shards);
    let mut metrics = Vec::with_capacity(shards);
    let mut failure: Option<SpawnError> = None;
    for (shard, (verdict_rx, _)) in planes.iter().enumerate() {
      match verdict_rx.recv() {
        Ok(Ok((handle, plane_metrics))) => {
          handles.push(handle);
          metrics.push(plane_metrics);
        }
        Ok(Err(e)) => {
          failure = Some(e);
          break;
        }
        Err(_) => {
          failure = Some(SpawnError::PlaneFailed { shard });
          break;
        }
      }
    }
    if let Some(e) = failure {
      // Unwind: dropping every collected handle (and every verdict still in a channel) drops
      // every command sender, so each running plane's loop exits on command disconnect and runs
      // its ordinary teardown; joining the threads then guarantees every listener is closed
      // before the error returns — a failed spawn leaks nothing.
      drop(handles);
      drop(tails);
      for (verdict_rx, thread) in planes {
        drop(verdict_rx);
        let _ = thread.join();
      }
      return Err(e);
    }
    // Success: the plane threads detach (dropping a `std` JoinHandle detaches). Teardown is
    // observed through the handles — `shutdown()` resolves only after every plane's fd-release
    // barrier — so nothing here needs the threads joinable.
    Ok(ShardedMultiHandle {
      shards: handles,
      map: self.map,
      events,
      lifecycle,
      metrics,
    })
  }
}

/// The routing client surface over the K planes: group-keyed operations go to the plane the
/// [`ShardMap`] assigns, the events/lifecycle tails are the ONE shared pair every plane fans
/// into, and `shutdown` broadcasts to all planes then awaits ALL their teardowns. Cheaply
/// cloneable and `Send + Sync` like the per-plane [`MultiHandle`]s it wraps.
pub struct ShardedMultiHandle<G, I, F>
where
  F: StateMachine,
{
  shards: Vec<MultiHandle<G, I, F>>,
  map: ShardMap<G>,
  events: flume::Receiver<(G, Event<I, F::Response>)>,
  lifecycle: flume::Receiver<LifecycleEvent<G, I>>,
  metrics: Vec<EngineMetrics>,
}

impl<G, I, F> Clone for ShardedMultiHandle<G, I, F>
where
  F: StateMachine,
{
  fn clone(&self) -> Self {
    Self {
      shards: self.shards.clone(),
      map: self.map.clone(),
      events: self.events.clone(),
      lifecycle: self.lifecycle.clone(),
      metrics: self.metrics.clone(),
    }
  }
}

impl<G, I, F> ShardedMultiHandle<G, I, F>
where
  G: GroupId + Send,
  I: sailing_proto::NodeId + Send,
  F: StateMachine,
  F::Command: sailing_proto::Data + Send,
  F::Response: Send,
{
  /// The plane count K.
  #[must_use]
  pub fn shards(&self) -> usize {
    self.shards.len()
  }

  /// The plane hosting `group` under this host's map — the same answer every node of the
  /// cluster computes.
  #[must_use]
  pub fn shard_of(&self, group: &G) -> usize {
    self.map.shard(group)
  }

  /// Project a per-group handle, routed to the group's plane: the returned [`GroupHandle`] is
  /// the ordinary multi-driver projection, bound to `shard(group)`'s command channel.
  #[must_use]
  pub fn group(&self, group: G) -> GroupHandle<G, I, F> {
    self.shards[self.map.shard(&group)].group(group)
  }

  /// Create a fresh group ON ITS MAPPED PLANE and await the admission verdict (the per-plane
  /// [`MultiHandle::create_group`] contract; the host identity latches per plane from its first
  /// admitted group). `generation` forwards to the plane command unchanged — each plane's
  /// engine carries its own lineage records, so the floor check runs at exactly the plane the
  /// shard map routes the id to (per-plane grain, no cross-plane state).
  pub async fn create_group(
    &self,
    gid: G,
    config: Config<I>,
    seed: u64,
    fsm: F,
    generation: u64,
  ) -> Result<(), DriverError<I>> {
    let shard = self.map.shard(&gid);
    self.shards[shard]
      .create_group(gid, config, seed, fsm, generation)
      .await
  }

  /// Create a group from LOCALLY-FORKED state ON ITS MAPPED PLANE and await the admission
  /// verdict (the per-plane [`MultiHandle::create_group_from_fork`] contract): the manufactured
  /// baseline lands in exactly the plane the shard map routes the id to, so an embedder fork —
  /// like the split milestone's in-apply fork, which is plane-local by construction — never
  /// hosts a replica on a plane no peer would ever dial. `generation` forwards unchanged, as on
  /// [`create_group`](Self::create_group).
  pub async fn create_group_from_fork(
    &self,
    gid: G,
    config: Config<I>,
    seed: u64,
    fsm: F,
    snapshot: bytes::Bytes,
    generation: u64,
  ) -> Result<(), DriverError<I>> {
    let shard = self.map.shard(&gid);
    self.shards[shard]
      .create_group_from_fork(gid, config, seed, fsm, snapshot, generation)
      .await
  }

  /// Recover a group from ITS MAPPED PLANE's engine and await the admission verdict.
  /// `generation` forwards to the plane command unchanged, as on
  /// [`create_group`](Self::create_group).
  pub async fn restore_group(
    &self,
    gid: G,
    config: Config<I>,
    seed: u64,
    fsm: F,
    generation: u64,
  ) -> Result<(), DriverError<I>> {
    let shard = self.map.shard(&gid);
    self.shards[shard]
      .restore_group(gid, config, seed, fsm, generation)
      .await
  }

  /// Propose a group SPLIT on `parent`'s mapped plane. v1 CONSTRAINT, refused HERE — typed,
  /// before any command crosses a channel: `shard(child) == shard(parent)`, because the fork
  /// materializes inside one plane's driver (a cross-plane child would need a handoff seam
  /// between plane threads that v1 deliberately lacks; the `ShardMap` override makes any child
  /// id placeable by the embedder instead). Same-plane splits are the per-plane
  /// [`GroupHandle::propose_split`] contract verbatim.
  pub async fn propose_split(
    &self,
    parent: G,
    child: G,
    child_gen: u64,
    instruction: bytes::Bytes,
  ) -> Result<sailing_proto::Index, DriverError<I>> {
    let plane = self.map.shard(&parent);
    if self.map.shard(&child) != plane {
      return Err(DriverError::Rejected {
        reason: sailing_proto::SplitError::<I>::CrossPlane.to_string(),
      });
    }
    self.shards[plane]
      .propose_split(parent, child, child_gen, instruction)
      .await
  }

  /// Remove a group from its mapped plane, awaiting whether it was hosted (the removal
  /// tombstones the id ON THAT PLANE, exactly the per-plane multi semantics).
  pub async fn remove_group(&self, gid: G) -> Result<bool, DriverError<I>> {
    let shard = self.map.shard(&gid);
    self.shards[shard].remove_group(gid).await
  }

  /// Lift a group id's tombstone on its mapped plane, awaiting whether one existed.
  pub async fn clear_tombstone(&self, gid: G) -> Result<bool, DriverError<I>> {
    let shard = self.map.shard(&gid);
    self.shards[shard].clear_tombstone(gid).await
  }

  /// The ONE shared events tail every plane fans into, each event stamped with its originating
  /// group. Consume from a single receiver: the channel is multi-consumer, so concurrent
  /// consumers SPLIT the stream (each event goes to exactly one).
  pub fn events(&self) -> &flume::Receiver<(G, Event<I, F::Response>)> {
    &self.events
  }

  /// The ONE shared lifecycle tail every plane fans into (unknown-group solicitations surface
  /// from the plane the shard map routes the group to; removed-self from the plane hosting the
  /// replica). Same single-consumer guidance as [`events`](Self::events).
  pub fn lifecycle(&self) -> &flume::Receiver<LifecycleEvent<G, I>> {
    &self.lifecycle
  }

  /// One plane's [`MultiHandle`] — the ESCAPE HATCH for plane-scoped work (status polls against
  /// a known plane, plane-local shutdown in tests). Group-keyed operations issued here bypass
  /// the shard map: a create on the wrong plane would host a replica no other node ever dials,
  /// so lifecycle mutations should ride the sharded surface unless the caller re-derives the
  /// map itself. Manual creates also bypass the shard guard — it covers only factory
  /// materialization, and this handle is the deliberate opt-out.
  #[must_use]
  pub fn shard_handle(&self, shard: usize) -> Option<&MultiHandle<G, I, F>> {
    self.shards.get(shard)
  }

  /// One plane's engine counters (its own durability barrier + quiesced-group gauge) — the
  /// per-plane observability that makes the independent barriers visible.
  #[must_use]
  pub fn engine_metrics(&self, shard: usize) -> Option<&EngineMetrics> {
    self.metrics.get(shard)
  }

  /// Ask every plane to stop, then await ALL K teardowns: each plane's shutdown resolves only
  /// after its listener's fd-release barrier, so a resolved call means every per-shard port is
  /// rebindable. The request is broadcast to all planes before any teardown is awaited (the
  /// futures run joined), and the first error — if any — is returned after every plane has been
  /// asked.
  pub async fn shutdown(&self) -> Result<(), DriverError<I>> {
    let verdicts =
      futures_util::future::join_all(self.shards.iter().map(MultiHandle::shutdown)).await;
    verdicts.into_iter().collect()
  }
}
