//! Real-socket integration for the SHARDED compio host: 2 nodes × K=2 planes over loopback TCP,
//! every plane a complete multi driver on its own thread with its own engine barrier and its own
//! per-shard listener. The suites assert the plane model end to end: groups mapped to different
//! shards elect and commit through one routing [`ShardedMultiHandle`] each, replication flows
//! shard(g) ↔ shard(g) (a group is hosted on exactly its mapped plane), the per-plane barriers
//! run independently, one plane's poisoned group leaves the other plane committing, factories
//! materialize solicited groups on the RIGHT plane hands-free, a skewed peer's wrong-plane
//! solicitations are refused fail-closed (never materialized, surfaced as unknown-group), and
//! shutdown releases every plane. The test thread drives the `Send` handles with a plain
//! futures executor — no compio runtime on the caller side, exactly the embedder shape.

mod common;

use std::{
  collections::BTreeSet,
  future::Future,
  net::SocketAddr,
  rc::Rc,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
  },
  time::{Duration, Instant},
};

use bytes::Bytes;
use common::{CountSm, DelegatingEngine, EngineWatch, TrapSm};
use sailing_compio::{
  BoxedGroupFactory, DriverConfig, DriverError, EngineMetrics, GroupBlueprint, GroupHandle,
  LifecycleEvent, Node, ShardMap, ShardedCompioHost, ShardedMultiHandle, SpawnError, factory_fn,
};
use sailing_proto::{
  ClusterId, Config, Data, Index, LabelOptions, Labeled, Passthrough, Role, StateMachine,
};

const ELECTION: Duration = Duration::from_millis(300);
const HEARTBEAT: Duration = Duration::from_millis(60);

fn cluster() -> ClusterId {
  ClusterId([17; 16])
}

fn encoded(id: u64) -> Vec<u8> {
  let mut v = Vec::new();
  id.encode(&mut v);
  v
}

fn config(id: u64, voters: Vec<u64>) -> Config<u64> {
  Config::try_new(id, voters, ELECTION, HEARTBEAT).unwrap()
}

/// The OBSERVER boot shape — `id` absent from the seed voters, so the replica grants votes but
/// cannot campaign until the log/snapshot teaches it its own membership. The mandatory factory
/// blueprint shape for FORK-BORN ids (see the `GroupFactory` fork-born contract paragraph).
fn observer_config(id: u64, current_voters: Vec<u64>) -> Config<u64> {
  Config::try_new_observer(id, current_voters, ELECTION, HEARTBEAT).unwrap()
}

/// Drive one handle future to completion from the plain test thread (the handles are `Send`
/// channel futures; no compio runtime is needed caller-side).
fn bo<T>(fut: impl Future<Output = T>) -> T {
  futures_executor::block_on(fut)
}

/// The per-plane plaintext record layers for node `id`: ONE `Send + Sync` provider shared across
/// the plane threads, each call building that plane's `Rc` factories ON the plane's thread. The
/// node id is plane-independent — peer identity is node-level; the plane separation is the port
/// convention.
fn plain_records(id: u64) -> sailing_compio::ShardRecordLayers<u64, Labeled<Passthrough>> {
  let local = encoded(id);
  Arc::new(move |_shard: usize| {
    let dial_local = local.clone();
    let accept_local = local.clone();
    let dialer: sailing_compio::DialerFactory<u64, Labeled<Passthrough>> =
      Rc::new(move |_: &u64| {
        Labeled::dialer(
          Passthrough::new(),
          &LabelOptions {
            cluster: cluster(),
            local_id: dial_local.clone(),
          },
        )
        .map_err(std::io::Error::other)
      });
    let acceptor: sailing_compio::AcceptorFactory<Labeled<Passthrough>> = Rc::new(move || {
      Labeled::acceptor(
        Passthrough::new(),
        &LabelOptions {
          cluster: cluster(),
          local_id: accept_local.clone(),
        },
      )
      .map_err(std::io::Error::other)
    });
    Ok((dialer, acceptor))
  })
}

/// Two group ids that land on DIFFERENT planes under the DEFAULT uniform map — derived from the
/// map itself (the map is deterministic cluster-wide, so the derivation is too).
fn distinct_shard_gids(map: &ShardMap<u64>) -> (u64, u64) {
  let a = 100u64;
  let shard_a = map.shard(&a);
  let b = (2u64..100)
    .map(|i| i * 100)
    .find(|g| map.shard(g) != shard_a)
    .expect("some group id lands on the other plane");
  (a, b)
}

/// Submit through whichever node is (or redirects to) the group's leader.
fn submit_anywhere<F>(groups: &[GroupHandle<u64, u64, F>], payload: &'static [u8]) -> u64
where
  F: StateMachine<Command = Bytes, Response = u64> + Send,
  F::Command: Data + Send,
{
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  let mut at = 0usize;
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "no commit within the deadline"
    );
    match bo(groups[at].submit(Bytes::from_static(payload))) {
      Ok(response) => return response,
      Err(DriverError::NotLeader { leader }) => {
        at = leader
          .map(|l| (l - 1) as usize)
          .unwrap_or((at + 1) % groups.len());
        std::thread::sleep(Duration::from_millis(50));
      }
      Err(DriverError::Superseded) => {}
      Err(DriverError::Rejected { .. }) => {
        at = (at + 1) % groups.len();
        std::thread::sleep(Duration::from_millis(50));
      }
      Err(e) => panic!("unexpected submit error: {e:?}"),
    }
  }
}

/// Query the group through any node that will serve it (sailing forwards follower reads).
fn query_anywhere(groups: &[GroupHandle<u64, u64, CountSm>]) -> u64 {
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "no query within the deadline"
    );
    for g in groups {
      if let Ok(c) = bo(g.query(|sm: &CountSm| sm.count())) {
        return c;
      }
    }
    std::thread::sleep(Duration::from_millis(50));
  }
}

/// Spawn one node's sharded host: K=2 planes at `base` (ports base, base+1), dialing `peer`'s
/// planes by the same convention, under the DEFAULT uniform shard map.
fn spawn_host<F>(
  id: u64,
  base: SocketAddr,
  peer: Option<(u64, SocketAddr)>,
) -> ShardedMultiHandle<u64, u64, F>
where
  F: StateMachine<Command = Bytes, Response = u64> + Send + Default + 'static,
  F::Command: Data + Send,
  F::Snapshot: Data,
  F::Response: Clone + Send,
  F::Error: core::error::Error,
{
  spawn_host_mapped(id, base, peer, ShardMap::uniform(2))
}

/// The [`spawn_host`] variant taking an EXPLICIT shard map, for the reshaping suites that pin a
/// split/merge pair (and a bystander) to named planes. The map is cluster-wide, so every node's
/// host must be spawned with the same one — the shard-consistency contract the skew suite proves
/// is fatal to violate.
fn spawn_host_mapped<F>(
  id: u64,
  base: SocketAddr,
  peer: Option<(u64, SocketAddr)>,
  map: ShardMap<u64>,
) -> ShardedMultiHandle<u64, u64, F>
where
  F: StateMachine<Command = Bytes, Response = u64> + Send + Default + 'static,
  F::Command: Data + Send,
  F::Snapshot: Data,
  F::Response: Clone + Send,
  F::Error: core::error::Error,
{
  let peers = peer
    .map(|(pid, addr)| Node::new(pid, addr))
    .into_iter()
    .collect();
  ShardedCompioHost::<u64, u64, F, Labeled<Passthrough>>::new(
    map,
    base,
    peers,
    plain_records(id),
    DriverConfig::default(),
  )
  .spawn()
  .expect("the sharded host spawns")
}

/// The index of the node currently LEADING `groups`, waited for — the sync twin of the reactor
/// suite's helper (needed to colocate a merge pair's leadership before the freeze).
fn find_leader<F>(groups: &[GroupHandle<u64, u64, F>], what: &str) -> usize
where
  F: StateMachine<Command = Bytes, Response = u64> + Send,
  F::Command: Data + Send,
{
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "{what}: no leader in time"
    );
    for (i, g) in groups.iter().enumerate() {
      if let Ok(status) = bo(g.status())
        && status.role == Role::Leader
      {
        return i;
      }
    }
    std::thread::sleep(Duration::from_millis(30));
  }
}

/// Move `groups`' leadership onto node `to_node` (1-based) and wait until it settles. The merge's
/// all-source-voters freeze barrier is observable only on the source LEADER, so `commit_merge`
/// certifies it only when the absorbing target's leader also leads the source — the CRDB
/// colocate-then-merge discipline. Move the SOURCE (a frozen source refuses a transfer, so this
/// must precede the freeze).
fn colocate_onto<F>(groups: &[GroupHandle<u64, u64, F>], to_node: u64, what: &str)
where
  F: StateMachine<Command = Bytes, Response = u64> + Send,
  F::Command: Data + Send,
{
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "{what}: colocation never settled onto node {to_node}"
    );
    let at = find_leader(groups, what);
    if at as u64 + 1 == to_node {
      return;
    }
    let _ = bo(groups[at].transfer_leader(to_node));
    std::thread::sleep(Duration::from_millis(40));
  }
}

/// Retry a merge verb across every node until one accepts, failing FAST on the permanent
/// `DirectionInverted` refusal (a property of the id pair, never cleared by retrying) so a
/// re-introduced direction bug is a pointed panic, not a 15s timeout.
fn merge_verb_anywhere<Fut>(what: &str, mut verb: impl FnMut(usize) -> Fut, n: usize)
where
  Fut: Future<Output = Result<Index, DriverError<u64>>>,
{
  let inverted = format!("{:?}", sailing_proto::MergeError::<u64>::DirectionInverted);
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "{what} never accepted"
    );
    for at in 0..n {
      match bo(verb(at)) {
        Ok(_) => return,
        Err(DriverError::Rejected { reason }) if reason == inverted => {
          panic!(
            "{what}: the merge claim is permanently inverted — source must encode above target"
          )
        }
        Err(_) => {}
      }
    }
    std::thread::sleep(Duration::from_millis(40));
  }
}

/// Poll `metrics`' quiesced-group gauge until it reaches `want` (the driver publishes it after
/// every quiesce sweep) — the sync twin of the reactor suite's helper.
fn wait_for_quiesced(metrics: &EngineMetrics, want: u64, what: &str) {
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  while metrics.quiesced_groups() != want {
    assert!(
      std::time::Instant::now() < deadline,
      "{what}: quiesced gauge stuck at {} (want {want})",
      metrics.quiesced_groups()
    );
    std::thread::sleep(Duration::from_millis(30));
  }
}

/// Observe a quiesce WAKE EDGE, not merely a level: first poll until the gauge LEAVES `from` (drops
/// below it — a group left quiescence), then until it settles back at `to`. Unlike
/// [`wait_for_quiesced`], this refuses to accept a STALE reading already sitting at `to` before the
/// wake ever happened — the transition must be witnessed. Bounded by a hard deadline.
fn observe_wake_edge(metrics: &EngineMetrics, from: u64, to: u64, what: &str) {
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  // The WAKE: the gauge must drop below `from` — a group actually left quiescence.
  loop {
    let g = metrics.quiesced_groups();
    if g < from {
      break;
    }
    assert!(
      std::time::Instant::now() < deadline,
      "{what}: quiesced gauge never left {from} (still {g}) — the wake edge was not observed"
    );
    std::thread::sleep(Duration::from_millis(15));
  }
  // The SETTLE: it returns to `to`.
  wait_for_quiesced(metrics, to, what);
}

/// Poll an atomic progress counter until it reaches at least `want` — used to let a background
/// keyed loader accumulate enough committed keys before the reshape.
fn wait_until_at_least(counter: &AtomicU64, want: u64, what: &str) {
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  while counter.load(Ordering::Acquire) < want {
    assert!(
      std::time::Instant::now() < deadline,
      "{what}: stuck at {} (want >= {want})",
      counter.load(Ordering::Acquire)
    );
    std::thread::sleep(Duration::from_millis(30));
  }
}

/// A background thread that keeps submitting DISTINCT monotonically-increasing keys (1, 2, 3, …)
/// into `gid` (redirecting to its leader) until `stop`, advancing to the next key only after the
/// current one COMMITS — so the acked set stays the contiguous range `1..=last_acked`. Publishes
/// the last acked key into `last_acked`, the conservation suite's ground truth.
fn spawn_keyed_loader(
  handles: Vec<ShardedMultiHandle<u64, u64, KeyedSm>>,
  gid: u64,
  stop: Arc<AtomicBool>,
  last_acked: Arc<AtomicU64>,
) -> std::thread::JoinHandle<()> {
  std::thread::spawn(move || {
    let groups: Vec<_> = handles.iter().map(|h| h.group(gid)).collect();
    let mut key = 1u64;
    let mut at = 0usize;
    while !stop.load(Ordering::Relaxed) {
      match bo(groups[at].submit(Bytes::from(key.to_le_bytes().to_vec()))) {
        Ok(_) => {
          last_acked.store(key, Ordering::Release);
          key += 1;
        }
        Err(DriverError::NotLeader { leader }) => {
          at = leader
            .map(|l| (l - 1) as usize)
            .unwrap_or((at + 1) % groups.len());
        }
        Err(DriverError::Superseded) => {}
        Err(_) => {
          at = (at + 1) % groups.len();
          std::thread::sleep(Duration::from_millis(10));
        }
      }
    }
  })
}

/// A keyed-set state machine local to the reshaping suite: each command is an 8-byte little-endian
/// key, `apply` inserts it (returning the running cardinality), and the snapshot is the key set
/// itself — which round-trips through `Data` (`BTreeSet<u64>`), the shape a manufactured fork
/// baseline installs. `split` hands the child every ODD key and keeps the even ones (a disjoint
/// partition), and `absorb` folds the source's keys back; so a split-then-merge-back cycle
/// conserves the whole key set and a post-merge read of the survivor finds exactly the acked keys.
/// A TEST-ONLY barrier inside [`KeyedSm::split`], for `split_on_plane_under_load`'s deterministic
/// isolation proof. Scoped two ways so no other test (`reshape_cycle_under_continuous_load` also
/// splits a KeyedSm) is affected: the split blocks only when `SPLIT_BARRIER_ARMED` is set AND the
/// FSM carries [`SPLIT_BARRIER_KEY`] (present only in that one test's parent). On entry it bumps
/// `SPLIT_BARRIER_ENTERED` (the apply thread is now provably held inside the fork) and spins until
/// `SPLIT_BARRIER_RELEASE`, with a hard timeout so a missed release fails loudly instead of hanging.
static SPLIT_BARRIER_ARMED: AtomicBool = AtomicBool::new(false);
static SPLIT_BARRIER_ENTERED: AtomicUsize = AtomicUsize::new(0);
static SPLIT_BARRIER_RELEASE: AtomicBool = AtomicBool::new(false);
/// A magic EVEN key (so `split` keeps it on the parent side): its presence in the FSM is what scopes
/// the barrier to `split_on_plane_under_load`'s parent, and it sorts far above any real key.
const SPLIT_BARRIER_KEY: u64 = 0xB000_0000_0000_0000;

#[derive(Default)]
struct KeyedSm {
  keys: BTreeSet<u64>,
}

impl KeyedSm {
  fn keys(&self) -> &BTreeSet<u64> {
    &self.keys
  }
}

impl StateMachine for KeyedSm {
  type Command = Bytes;
  type Response = u64;
  type Snapshot = BTreeSet<u64>;
  type Error = std::convert::Infallible;

  fn apply(&mut self, _index: Index, cmd: Bytes) -> Result<u64, Self::Error> {
    // The command is an 8-byte LE key; a short frame zero-pads (defensive — the suite always
    // submits a full key).
    let mut raw = [0u8; 8];
    let n = cmd.len().min(8);
    raw[..n].copy_from_slice(&cmd[..n]);
    self.keys.insert(u64::from_le_bytes(raw));
    Ok(self.keys.len() as u64)
  }

  fn snapshot(&self) -> Result<Self::Snapshot, Self::Error> {
    Ok(self.keys.clone())
  }

  fn restore(&mut self, snapshot: Self::Snapshot) -> Result<(), Self::Error> {
    self.keys = snapshot;
    Ok(())
  }

  fn split(&mut self, _instruction: &[u8]) -> Option<Self> {
    // TEST-ONLY barrier (see the statics above): hold this plane-0 apply thread inside the fork so the
    // test can prove a bystander commit lands on plane 1 while plane 0 is provably frozen here.
    if SPLIT_BARRIER_ARMED.load(Ordering::Acquire) && self.keys.contains(&SPLIT_BARRIER_KEY) {
      SPLIT_BARRIER_ENTERED.fetch_add(1, Ordering::Release);
      let start = Instant::now();
      while !SPLIT_BARRIER_RELEASE.load(Ordering::Acquire) {
        assert!(
          start.elapsed() < Duration::from_secs(20),
          "split barrier: release never came (deadlock guard)"
        );
        std::thread::sleep(Duration::from_millis(1));
      }
    }
    let child: BTreeSet<u64> = self.keys.iter().copied().filter(|k| k & 1 == 1).collect();
    self.keys.retain(|k| k & 1 == 0);
    Some(Self { keys: child })
  }

  fn absorb(&mut self, source: Self) -> bool {
    self.keys.extend(source.keys);
    true
  }

  fn supports_split(&self) -> bool {
    true
  }

  fn supports_absorb(&self) -> bool {
    true
  }
}

/// The sharded gate: two groups on DIFFERENT planes both elect and commit through the routing
/// handle; each group is hosted on exactly its mapped plane (the shard(g) ↔ shard(g) traffic
/// partition); both planes' engine barriers ran independently; one plane's poisoned group
/// leaves the other plane's group committing (and is removable through the routed surface);
/// and shutdown releases every plane — a fresh host re-spawns on the same ports through the
/// same stack.
#[test]
fn sharded_host_commits_and_isolates_planes() {
  let base1: SocketAddr = "127.0.0.1:45100".parse().unwrap();
  let base2: SocketAddr = "127.0.0.1:45110".parse().unwrap();
  let handles: Vec<ShardedMultiHandle<u64, u64, TrapSm>> = vec![
    spawn_host(1, base1, Some((2, base2))),
    spawn_host(2, base2, Some((1, base1))),
  ];
  let map = ShardMap::<u64>::uniform(2);
  let (gid_a, gid_b) = distinct_shard_gids(&map);
  let (shard_a, shard_b) = (map.shard(&gid_a), map.shard(&gid_b));
  assert_ne!(shard_a, shard_b, "the gids land on different planes");

  for gid in [gid_a, gid_b] {
    for (i, h) in handles.iter().enumerate() {
      let id = i as u64 + 1;
      bo(h.create_group(
        gid,
        config(id, vec![1, 2]),
        id * 1000 + gid,
        TrapSm::default(),
        0,
      ))
      .expect("group admission through the routed surface");
    }
  }

  // Both planes' groups elect and commit through the ONE routing handle per node.
  let ga: Vec<_> = handles.iter().map(|h| h.group(gid_a)).collect();
  let gb: Vec<_> = handles.iter().map(|h| h.group(gid_b)).collect();
  assert_eq!(submit_anywhere(&ga, b"a1"), 1);
  assert_eq!(submit_anywhere(&gb, b"b1"), 1);
  assert_eq!(submit_anywhere(&ga, b"a2"), 2);
  assert_eq!(submit_anywhere(&gb, b"b2"), 2);

  // PLANE PLACEMENT: each group is hosted on exactly its mapped plane (the escape-hatch
  // per-plane handles witness it) — a create routed through the sharded surface can land
  // nowhere else, and the peer's replica sits on the SAME plane index (cluster-wide map).
  for h in &handles {
    assert!(
      bo(h.shard_handle(shard_a).unwrap().group(gid_a).status()).is_ok(),
      "gid_a lives on its mapped plane"
    );
    match bo(h.shard_handle(shard_b).unwrap().group(gid_a).status()) {
      Err(DriverError::Rejected { reason }) => {
        assert!(reason.contains("no such group"), "got: {reason}");
      }
      other => panic!("gid_a must not exist on the other plane, got {other:?}"),
    }
    assert!(
      bo(h.shard_handle(shard_b).unwrap().group(gid_b).status()).is_ok(),
      "gid_b lives on its mapped plane"
    );
  }

  // Per-plane engine barriers: BOTH planes flushed (each group's appends rode its own plane's
  // barrier — with one shared engine either group's counter would be the other's too, so two
  // independently-nonzero counters on one node witness the independent WALs).
  for h in &handles {
    assert!(
      h.engine_metrics(shard_a).unwrap().barriers() > 0,
      "plane {shard_a}'s barrier ran"
    );
    assert!(
      h.engine_metrics(shard_b).unwrap().barriers() > 0,
      "plane {shard_b}'s barrier ran"
    );
  }

  // PLANE ISOLATION: poison gid_a (its apply traps on BOOM) — the submit fails with the typed
  // verdict, the poison is observable and sticky on gid_a, and gid_b — on the OTHER plane —
  // keeps committing throughout.
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  let mut at = 0usize;
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "the trapped apply never poisoned"
    );
    match bo(ga[at].submit(Bytes::from_static(b"BOOM"))) {
      Err(DriverError::Poisoned) => break,
      Err(DriverError::NotLeader { leader }) => {
        at = leader.map(|l| (l - 1) as usize).unwrap_or(1 - at);
        std::thread::sleep(Duration::from_millis(50));
      }
      Err(DriverError::Superseded) => {}
      other => panic!("expected Poisoned from the trapped apply, got {other:?}"),
    }
  }
  let status = bo(ga[at].status()).expect("status still answers on the poisoned group");
  assert!(status.is_poisoned, "gid_a fail-stopped");
  assert_eq!(
    submit_anywhere(&gb, b"b3"),
    3,
    "the sibling plane's group rides out the poison"
  );

  // The poisoned group is removable through the routed surface, and the plane survives.
  assert!(bo(handles[at].remove_group(gid_a)).expect("remove resolves"));
  assert_eq!(submit_anywhere(&gb, b"b4"), 4);

  // Shutdown broadcasts to all planes and resolves only after EVERY plane's fd-release
  // barrier: a fresh host re-spawning on the very same per-shard ports through the same stack
  // is the release witness.
  for h in &handles {
    bo(h.shutdown()).expect("the sharded host tears down");
  }
  let fresh: ShardedMultiHandle<u64, u64, TrapSm> = spawn_host(1, base1, None);
  bo(fresh.shutdown()).expect("the re-spawned host tears down");
}

/// THE hands-free materialization flow, sharded: node 2 registers factories on BOTH planes and
/// never calls `create_group`; node 1 creates two groups mapped to DIFFERENT planes and each
/// campaign solicits node 2 over ITS plane's mesh — the factory materializes each replica on
/// exactly the plane the cluster-wide map names (the solicitation can only ARRIVE there), the
/// elections complete, and both groups commit. The consumed signals never reach node 2's shared
/// lifecycle tail.
#[test]
fn sharded_factory_materializes_on_the_mapped_plane() {
  let base1: SocketAddr = "127.0.0.1:45140".parse().unwrap();
  let base2: SocketAddr = "127.0.0.1:45150".parse().unwrap();
  let map = ShardMap::<u64>::uniform(2);
  let (gid_a, gid_b) = distinct_shard_gids(&map);
  let (shard_a, shard_b) = (map.shard(&gid_a), map.shard(&gid_b));

  let node1: ShardedMultiHandle<u64, u64, CountSm> = spawn_host(1, base1, Some((2, base2)));
  // Node 2 is factory-armed on every plane: each plane gets its OWN factory instance, and each
  // sees only the solicitations the shard map routes to it, so one catalog closure serves all
  // planes without shard filtering. Both gids are day-0 BOOTSTRAPPED ids (created explicitly
  // on node 1), so the blueprints keep the full-voter shape — the observer rule binds
  // fork-born ids only.
  let node2: ShardedMultiHandle<u64, u64, CountSm> =
    ShardedCompioHost::<u64, u64, CountSm, Labeled<Passthrough>>::new(
      ShardMap::uniform(2),
      base2,
      vec![Node::new(1u64, base1)],
      plain_records(2),
      DriverConfig::default(),
    )
    .with_group_factories(move |_shard| {
      let catalog = [gid_a, gid_b];
      let factory: BoxedGroupFactory<u64, u64, CountSm> = Box::new(factory_fn(
        move |group: &u64, from: &u64| {
          (catalog.contains(group) && [1u64, 2].contains(from))
            .then(|| GroupBlueprint::new(config(2, vec![1, 2]), *group))
        },
        |_group: &u64| Some(CountSm::default()),
      ));
      Some(factory)
    })
    .spawn()
    .expect("the factory-armed host spawns");

  // Both groups exist only on node 1; each campaign solicits node 2 on its own plane, whose
  // factory materializes the replica hands-free — no create_group is ever issued on node 2.
  for gid in [gid_a, gid_b] {
    bo(node1.create_group(gid, config(1, vec![1, 2]), gid, CountSm::default(), 0))
      .expect("group admission on node 1");
  }
  let ga = [node1.group(gid_a), node2.group(gid_a)];
  let gb = [node1.group(gid_b), node2.group(gid_b)];
  assert_eq!(submit_anywhere(&ga, b"a"), 1);
  assert_eq!(submit_anywhere(&gb, b"b"), 1);
  assert_eq!(query_anywhere(&ga), 1, "the joined group serves reads");

  // Each replica materialized on EXACTLY its mapped plane on node 2.
  assert!(
    bo(node2.shard_handle(shard_a).unwrap().group(gid_a).status()).is_ok(),
    "gid_a materialized on its mapped plane"
  );
  match bo(node2.shard_handle(shard_b).unwrap().group(gid_a).status()) {
    Err(DriverError::Rejected { .. }) => {}
    other => panic!("gid_a must not materialize on the other plane, got {other:?}"),
  }
  assert!(
    bo(node2.shard_handle(shard_b).unwrap().group(gid_b).status()).is_ok(),
    "gid_b materialized on its mapped plane"
  );

  // The factory CONSUMED the solicitations: node 2's shared lifecycle tail never surfaced
  // either group.
  while let Ok(ev) = node2.lifecycle().try_recv() {
    assert!(
      !matches!(ev, LifecycleEvent::UnknownGroup { group, .. } if group == gid_a || group == gid_b),
      "a factory-consumed signal must not reach the tail: {ev:?}"
    );
  }

  for h in [&node1, &node2] {
    bo(h.shutdown()).expect("the sharded host tears down");
  }
}

/// THE fail-closed guard under map skew: node 1 runs the uniform map, node 2 a DELIBERATELY
/// inverted one, so node 1 dials group `g`'s traffic to the plane node 2's own map does NOT
/// assign `g`. Node 2 is factory-armed on every plane with a catalog that vouches for `g`
/// (catalogs are not plane-aware — exactly the shape that would split the group without the
/// guard: a wrong-plane replica now, a correctly-routed one later, both acking under node 2's
/// ONE identity). The host's shard guard must refuse: the receiving plane never hosts `g`, its
/// embedder factory is never even consulted (the guard declines first), the skew surfaces as
/// `UnknownGroup` on node 2's lifecycle tail, NO plane of node 2 ever hosts `g` (the group
/// simply cannot form there until the config is fixed), and node 2's own groups on both planes
/// keep electing and committing throughout.
#[test]
fn skewed_maps_fail_closed_instead_of_materializing_on_the_wrong_plane() {
  let base1: SocketAddr = "127.0.0.1:45200".parse().unwrap();
  let base2: SocketAddr = "127.0.0.1:45210".parse().unwrap();

  let uniform = ShardMap::<u64>::uniform(2);
  let gid = 100u64;
  // Node 1 dials g toward THIS plane index on node 2 — which node 2's inverted map does not
  // assign g, making it node 2's wrong plane.
  let wrong_plane = uniform.shard(&gid);
  let inverted = {
    let u = ShardMap::<u64>::uniform(2);
    ShardMap::with_mapping(2, move |g: &u64| u.shard(g) + 1)
  };
  assert_ne!(
    inverted.shard(&gid),
    wrong_plane,
    "the maps disagree on g by construction"
  );

  // Node 1: the uniform map, no factory — it creates g and campaigns toward node 2.
  let node1: ShardedMultiHandle<u64, u64, CountSm> = spawn_host(1, base1, Some((2, base2)));

  // Node 2: the inverted map + a catalog factory on EVERY plane, each instrumented with a
  // per-plane consultation counter.
  let consults: [Arc<AtomicUsize>; 2] = [Arc::default(), Arc::default()];
  let node2: ShardedMultiHandle<u64, u64, CountSm> =
    ShardedCompioHost::<u64, u64, CountSm, Labeled<Passthrough>>::new(
      inverted.clone(),
      base2,
      vec![Node::new(1u64, base1)],
      plain_records(2),
      DriverConfig::default(),
    )
    .with_group_factories({
      let consults = consults.clone();
      move |shard| {
        let consulted = consults[shard].clone();
        let factory: BoxedGroupFactory<u64, u64, CountSm> = Box::new(factory_fn(
          move |group: &u64, _from: &u64| {
            consulted.fetch_add(1, Ordering::SeqCst);
            (*group == gid).then(|| GroupBlueprint::new(config(2, vec![1, 2]), *group))
          },
          |_group: &u64| Some(CountSm::default()),
        ));
        Some(factory)
      }
    })
    .spawn()
    .expect("the factory-armed host spawns");

  bo(node1.create_group(gid, config(1, vec![1, 2]), gid, CountSm::default(), 0))
    .expect("group admission on node 1");

  // Drive the solicitation window: node 1's campaign keeps soliciting node 2's wrong plane.
  // The guard holding is OBSERVABLE as UnknownGroup on node 2's shared tail; the guard failing
  // is observable as g hosted on the wrong plane — poll for both, sharpest failure first.
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  let mut observed = false;
  while !observed {
    assert!(
      std::time::Instant::now() < deadline,
      "the skew never surfaced on the lifecycle tail"
    );
    match bo(node2.shard_handle(wrong_plane).unwrap().group(gid).status()) {
      Err(DriverError::Rejected { reason }) => {
        assert!(reason.contains("no such group"), "got: {reason}");
      }
      other => panic!("the wrong plane must never host g, got {other:?}"),
    }
    while let Ok(ev) = node2.lifecycle().try_recv() {
      if let LifecycleEvent::UnknownGroup { group, from } = ev
        && group == gid
      {
        assert_eq!(from, 1, "the mis-routed solicitation came from node 1");
        observed = true;
      }
    }
    if !observed {
      std::thread::sleep(Duration::from_millis(50));
    }
  }

  // The guard declined BEFORE the embedder's catalog: no plane's factory was ever consulted
  // (g's solicitations all landed on the wrong plane, and nothing solicits the other plane).
  for (plane, counter) in consults.iter().enumerate() {
    assert_eq!(
      counter.load(Ordering::SeqCst),
      0,
      "plane {plane}'s factory must never be consulted for wrong-plane traffic"
    );
  }

  // Isolation: node 2's OWN groups keep working on BOTH planes throughout the skew
  // (single-voter groups, so no cross-node traffic rides the broken g mesh).
  for plane in 0..2usize {
    let local = (200u64..)
      .find(|g| inverted.shard(g) == plane)
      .expect("some id maps to this plane");
    bo(node2.create_group(local, config(2, vec![2]), local, CountSm::default(), 0))
      .expect("node 2 admits its own group");
    let handle = [node2.group(local)];
    assert_eq!(
      submit_anywhere(&handle, b"local"),
      1,
      "node 2's plane {plane} commits during the skew"
    );
  }

  // Fail closed CLUSTER-WIDE: at the end NO plane of node 2 hosts g — under the skew the group
  // cannot form on node 2 at all, which beats a second replica acking under one node id.
  for plane in 0..2usize {
    match bo(node2.shard_handle(plane).unwrap().group(gid).status()) {
      Err(DriverError::Rejected { reason }) => {
        assert!(reason.contains("no such group"), "got: {reason}");
      }
      other => panic!("no plane of node 2 may host g under the skew, got {other:?}"),
    }
  }

  for h in [&node1, &node2] {
    bo(h.shutdown()).expect("the sharded host tears down");
  }
}

/// Addressing is validated up front, before any thread or socket exists: a wrong-length explicit
/// listen list and a port convention overflowing `u16` (listen side and peer side) are typed
/// spawn refusals.
#[test]
fn spawn_rejects_bad_addressing() {
  let addr: SocketAddr = "127.0.0.1:45190".parse().unwrap();

  let host = ShardedCompioHost::<u64, u64, CountSm, Labeled<Passthrough>>::new(
    ShardMap::uniform(2),
    addr,
    Vec::new(),
    plain_records(1),
    DriverConfig::default(),
  )
  .with_listen_addrs(vec![addr]);
  match host.spawn() {
    Err(SpawnError::ListenAddrCount { expected, got }) => {
      assert_eq!((expected, got), (2, 1));
    }
    Err(other) => panic!("expected the addr-count refusal, got {other:?}"),
    Ok(_) => panic!("expected the addr-count refusal, got a running host"),
  }

  let top: SocketAddr = "127.0.0.1:65534".parse().unwrap();
  let host = ShardedCompioHost::<u64, u64, CountSm, Labeled<Passthrough>>::new(
    ShardMap::uniform(4),
    top,
    Vec::new(),
    plain_records(1),
    DriverConfig::default(),
  );
  assert!(
    matches!(host.spawn(), Err(SpawnError::PortOverflow { .. })),
    "a listen-side port overflow is refused"
  );

  let host = ShardedCompioHost::<u64, u64, CountSm, Labeled<Passthrough>>::new(
    ShardMap::uniform(4),
    addr,
    vec![Node::new(2u64, top)],
    plain_records(1),
    DriverConfig::default(),
  );
  assert!(
    matches!(host.spawn(), Err(SpawnError::PortOverflow { .. })),
    "a peer-side port overflow is refused"
  );
}

/// The split milestone on a K-plane host (v1: same-plane only). A parent on plane A splits a
/// SAME-plane child end to end — both halves elect, commit, and serve through the routed
/// surface, the typed `SplitApplied` fires on every node's shared tail — while a CROSS-plane
/// child id refuses at the HANDLE, typed, before any command is sent: nothing materializes on
/// any plane, and the sibling plane's group commits undisturbed throughout. The factory
/// interplay rides the same cluster: node 2 is factory-armed with a catalog that vouches for
/// the child, yet its build phase is NEVER consulted — both members fork locally in their own
/// drains (the drain front-runs the factory drain), so no solicitation for the child ever
/// exists to consume.
#[test]
fn sharded_split_stays_in_plane_and_refuses_cross_plane() {
  let base1: SocketAddr = "127.0.0.1:45240".parse().unwrap();
  let base2: SocketAddr = "127.0.0.1:45250".parse().unwrap();
  let map = ShardMap::<u64>::uniform(2);
  let (parent, sibling) = distinct_shard_gids(&map);
  let parent_plane = map.shard(&parent);
  // A same-plane child for the split, and a DIFFERENT-plane child for the refusal leg.
  let child = (3u64..100)
    .map(|i| i * 100 + 1)
    .find(|g| *g != parent && map.shard(g) == parent_plane)
    .expect("some id lands on the parent's plane");
  let cross_child = (3u64..100)
    .map(|i| i * 100 + 1)
    .find(|g| *g != sibling && map.shard(g) != parent_plane)
    .expect("some id lands on the other plane");

  let node1: ShardedMultiHandle<u64, u64, CountSm> = spawn_host(1, base1, Some((2, base2)));
  // Node 2's factory vouches for the child — a catalog that also knows fork-born ids, so its
  // blueprint is the mandatory OBSERVER shape (self absent from the seed voters) — with a
  // build counter proving the fork path, not the factory, materializes it.
  let builds = Arc::new(AtomicUsize::new(0));
  let builds_probe = builds.clone();
  let node2: ShardedMultiHandle<u64, u64, CountSm> =
    ShardedCompioHost::<u64, u64, CountSm, Labeled<Passthrough>>::new(
      ShardMap::uniform(2),
      base2,
      vec![Node::new(1u64, base1)],
      plain_records(2),
      DriverConfig::default(),
    )
    .with_group_factories(move |_shard| {
      let builds = builds.clone();
      let factory: BoxedGroupFactory<u64, u64, CountSm> = Box::new(factory_fn(
        move |group: &u64, from: &u64| {
          (*group == child && [1u64, 2].contains(from))
            .then(|| GroupBlueprint::new(observer_config(2, vec![1]), *group))
        },
        move |_group: &u64| {
          builds.fetch_add(1, Ordering::SeqCst);
          Some(CountSm::default())
        },
      ));
      Some(factory)
    })
    .spawn()
    .expect("the factory-armed host spawns");

  // The parent on its plane (both nodes), plus a sibling group on the OTHER plane.
  for (h, id) in [(&node1, 1u64), (&node2, 2u64)] {
    bo(h.create_group(parent, config(id, vec![1, 2]), id, CountSm::default(), 0))
      .expect("parent admission");
    bo(h.create_group(sibling, config(id, vec![1, 2]), id, CountSm::default(), 0))
      .expect("sibling admission");
  }
  let gp = [node1.group(parent), node2.group(parent)];
  let gs = [node1.group(sibling), node2.group(sibling)];
  for i in 0..7u64 {
    assert_eq!(submit_anywhere(&gp, b"load"), i + 1);
  }
  assert_eq!(submit_anywhere(&gs, b"s"), 1);

  // The same-plane split: give 5 of the 7 units to the child.
  {
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let handles = [&node1, &node2];
    let mut at = 0usize;
    loop {
      assert!(
        std::time::Instant::now() < deadline,
        "no split accepted within the deadline"
      );
      match bo(handles[at].propose_split(parent, child, 0, Bytes::from_static(b"\x05"))) {
        Ok(_) => break,
        Err(DriverError::NotLeader { .. }) | Err(DriverError::Rejected { .. }) => {
          at = (at + 1) % handles.len();
          std::thread::sleep(Duration::from_millis(40));
        }
        Err(e) => panic!("unexpected split error: {e:?}"),
      }
    }
  }
  for (name, h) in [("node 1", &node1), ("node 2", &node2)] {
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
      let remaining = deadline.saturating_duration_since(std::time::Instant::now());
      assert!(
        remaining > Duration::ZERO,
        "{name}: no SplitApplied in time"
      );
      match h.lifecycle().recv_timeout(remaining) {
        Ok(LifecycleEvent::SplitApplied {
          parent: p,
          child: c,
        }) => {
          assert_eq!((p, c), (parent, child), "{name}: the typed split event");
          break;
        }
        Ok(_) => {}
        Err(e) => panic!("{name}: the lifecycle tail closed: {e:?}"),
      }
    }
  }

  // Both halves live on the parent's plane; the totals conserve.
  let gc = [node1.group(child), node2.group(child)];
  assert_eq!(query_anywhere(&gp), 2, "the parent kept 7 - 5");
  assert_eq!(submit_anywhere(&gc, b"tail"), 6, "the child preloaded 5");
  assert!(
    bo(
      node1
        .shard_handle(parent_plane)
        .unwrap()
        .group(child)
        .status()
    )
    .is_ok(),
    "the child lives on the parent's plane"
  );
  assert_eq!(
    builds_probe.load(Ordering::SeqCst),
    0,
    "the fork drain front-runs the factory: no solicitation for the child ever existed"
  );

  // The cross-plane leg: typed refusal at the handle, nothing materialized anywhere, and the
  // sibling plane undisturbed.
  match bo(node1.propose_split(parent, cross_child, 0, Bytes::from_static(b"\x01"))) {
    Err(DriverError::Rejected { reason }) => {
      assert!(
        reason.contains("plane"),
        "the typed CrossPlane refusal: {reason}"
      );
    }
    other => panic!("expected the cross-plane rejection, got {other:?}"),
  }
  for h in [&node1, &node2] {
    for plane in 0..2 {
      match bo(h.shard_handle(plane).unwrap().group(cross_child).status()) {
        Err(DriverError::Rejected { .. }) => {}
        other => panic!("the refused child must not exist on any plane, got {other:?}"),
      }
    }
  }
  assert_eq!(
    submit_anywhere(&gs, b"after"),
    2,
    "the sibling plane is undisturbed"
  );
  assert_eq!(
    query_anywhere(&gp),
    2,
    "the parent still serves post-refusal"
  );

  for h in [&node1, &node2] {
    bo(h.shutdown()).expect("the sharded host tears down");
  }
}

/// Merges stay within one plane: a same-plane pair freezes, parks, and resolves through the
/// plane's own crank (the union served, the source floored terminally — the factory/admission
/// edge refuses its id at every generation), while a cross-plane pairing is refused typed at
/// the HANDLE, before any command crosses a channel — the plane cannot even see the other
/// group.
#[test]
fn same_plane_merges_resolve_and_cross_plane_refuses() {
  let base: SocketAddr = "127.0.0.1:45300".parse().unwrap();
  let node: ShardedMultiHandle<u64, u64, CountSm> = spawn_host(1, base, None);
  let map = ShardMap::<u64>::uniform(2);
  // Two gids on ONE plane (the merge pair) + one on the other (the refusal probe).
  let g1 = 100u64;
  let plane = map.shard(&g1);
  let g2 = (2u64..200)
    .map(|i| i * 100)
    .find(|g| map.shard(g) == plane)
    .expect("a same-plane sibling exists");
  let g3 = (2u64..200)
    .map(|i| i * 100)
    .find(|g| map.shard(g) != plane)
    .expect("an other-plane gid exists");
  for gid in [g1, g2, g3] {
    bo(node.create_group(gid, config(1, vec![1]), gid, CountSm::default(), 0))
      .expect("group admission");
  }
  let h1 = node.group(g1);
  let h2 = node.group(g2);
  assert_eq!(submit_anywhere(std::slice::from_ref(&h1), b"s1"), 1);
  assert_eq!(submit_anywhere(std::slice::from_ref(&h1), b"s2"), 2);
  assert_eq!(submit_anywhere(std::slice::from_ref(&h2), b"t1"), 1);

  // Orient the pair by the direction rule: the source must encode strictly ABOVE the target, which
  // for u64 is LE-byte-string order, NOT numeric order — g2 is the first same-plane multiple of
  // 100, which may be a two-byte id (e.g. 300) whose LE encoding sorts BELOW the single-byte g1.
  // The larger-encoding id is the source that dissolves; the union (2 + 1 = 3) is served by the
  // target either way.
  let (msrc, mtgt, h_tgt, h_src) = if g1.to_le_bytes() > g2.to_le_bytes() {
    (g1, g2, &h2, &h1)
  } else {
    (g2, g1, &h1, &h2)
  };
  let inverted = format!("{:?}", sailing_proto::MergeError::<u64>::DirectionInverted);

  // Cross-plane pairings refuse at the handle edge, typed, before ANY propose.
  for verdict in [
    bo(node.prepare_merge(g2, g3)),
    bo(node.commit_merge(g3, g2)),
  ] {
    match verdict {
      Err(DriverError::Rejected { reason }) => {
        assert!(reason.contains("planes"), "got: {reason}");
      }
      other => panic!("cross-plane must refuse at the handle, got {other:?}"),
    }
  }

  // The same-plane merge runs the full choreography inside its plane.
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  loop {
    assert!(std::time::Instant::now() < deadline, "no freeze accepted");
    match bo(node.prepare_merge(msrc, mtgt)) {
      Ok(_) => break,
      Err(DriverError::Rejected { reason }) if reason == inverted => {
        panic!("the freeze is permanently inverted — source must encode above target")
      }
      Err(_) => {}
    }
    std::thread::sleep(Duration::from_millis(30));
  }
  loop {
    assert!(std::time::Instant::now() < deadline, "no commit accepted");
    if bo(node.commit_merge(mtgt, msrc)).is_ok() {
      break;
    }
    std::thread::sleep(Duration::from_millis(30));
  }
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "the union never served"
    );
    if bo(h_tgt.query(|sm: &CountSm| sm.count())) == Ok(3) {
      break;
    }
    std::thread::sleep(Duration::from_millis(30));
  }

  // The merged-away id is floored TERMINALLY on its plane: re-admission refuses at any
  // generation — the same floor_admits gate the factory's pre-build check runs, so a solicited
  // re-materialization can never resurrect it either.
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "the source never died"
    );
    if matches!(bo(h_src.status()), Err(DriverError::Rejected { .. })) {
      break;
    }
    std::thread::sleep(Duration::from_millis(30));
  }
  match bo(node.create_group(msrc, config(1, vec![1]), 9, CountSm::default(), 42)) {
    Err(DriverError::Rejected { reason }) => {
      assert!(reason.contains("floor"), "got: {reason}");
    }
    other => panic!("a merged-away id must never re-admit, got {other:?}"),
  }

  bo(node.shutdown()).expect("every plane tears down");
}

/// Cross-plane ISOLATION under a large split, proven DETERMINISTICALLY (not by timing): an override
/// map pins the split pair to plane 0 and the bystander to plane 1. A test-only barrier inside
/// `KeyedSm::split` holds plane 0's apply thread INSIDE the fork; while it is provably frozen there, a
/// bystander op completes a full consensus round on plane 1 — that ack IS the isolation (separate
/// per-plane engines; a shared crank would deadlock the ack). The 200k-cell parent (installed at zero
/// consensus cost via `create_group`'s initial FSM) is stress that makes the fork do real work. Then
/// the barrier releases, `SplitApplied` fires on both nodes, and both halves keep committing.
#[test]
fn split_on_plane_under_load() {
  let base1: SocketAddr = "127.0.0.1:45400".parse().unwrap();
  let base2: SocketAddr = "127.0.0.1:45410".parse().unwrap();
  let parent = 10u64;
  let child = 11u64;
  let bystander = 20u64;
  // The override map pins the split pair to plane 0 and the bystander to plane 1, cluster-wide.
  let plane_map = || ShardMap::<u64>::with_mapping(2, move |g: &u64| usize::from(*g == bystander));
  assert_eq!(
    (plane_map().shard(&parent), plane_map().shard(&child)),
    (0, 0),
    "the split pair shares plane 0"
  );
  assert_eq!(
    plane_map().shard(&bystander),
    1,
    "the bystander is on plane 1"
  );

  let node1: ShardedMultiHandle<u64, u64, KeyedSm> =
    spawn_host_mapped(1, base1, Some((2, base2)), plane_map());
  let node2: ShardedMultiHandle<u64, u64, KeyedSm> =
    spawn_host_mapped(2, base2, Some((1, base1)), plane_map());

  // The parent boots with a LARGE KeyedSm (`PRELOAD` cells) via `create_group`'s INITIAL FSM value at
  // ZERO consensus cost, PLUS the magic barrier key. The preload is STRESS — the fork's apply-time cost
  // scales with the FSM size (`fsm.split` moves the odd half, then the baseline is encoded and
  // installed), so the split does real work — but the ISOLATION PROOF below is deterministic (the
  // barrier), not a timing argument. The two halves partition the preload exactly (odd -> child, even
  // -> parent; the even barrier key stays with the parent).
  const PRELOAD: u64 = 200_000;
  let preloaded = || KeyedSm {
    keys: (1..=PRELOAD)
      .chain(std::iter::once(SPLIT_BARRIER_KEY))
      .collect(),
  };
  for (h, id) in [(&node1, 1u64), (&node2, 2u64)] {
    bo(h.create_group(parent, config(id, vec![1, 2]), id, preloaded(), 0))
      .expect("parent admission");
    bo(h.create_group(bystander, config(id, vec![1, 2]), id, KeyedSm::default(), 0))
      .expect("bystander admission");
  }
  let gp = [node1.group(parent), node2.group(parent)];
  let gbys = [node1.group(bystander), node2.group(bystander)];
  // Elect the bystander (its first committed key), so plane 1 is already serving before the split.
  assert_eq!(submit_anywhere(&gbys, b"b"), 1);

  // Elect the parent so the split has a leader to propose to (the bystander is elected above).
  let _ = submit_anywhere(&gp, b"pelect00");
  let leader_idx = find_leader(&gp, "parent leader pre-split");

  // A DETERMINISTIC cross-plane ISOLATION proof via the test-only split barrier — no timing argument.
  // Arm the barrier for this parent's fork; propose; wait until the fork drain is provably IN PROGRESS
  // (plane 0's apply thread is held inside `KeyedSm::split`); then submit a bystander op and REQUIRE
  // its ack WHILE plane 0 is still frozen there. A full plane-1 consensus round completing while plane
  // 0's apply thread is blocked inside the fork IS cross-plane isolation, structurally: the planes are
  // separate engines on separate cores by design, and a shared crank would block the bystander's ack
  // too (its `submit_anywhere` deadline would then fail loudly). The block is brief (only until the
  // bystander commits), well under a heartbeat, so plane 0 does not lose leadership.
  SPLIT_BARRIER_ENTERED.store(0, Ordering::Release);
  SPLIT_BARRIER_RELEASE.store(false, Ordering::Release);
  SPLIT_BARRIER_ARMED.store(true, Ordering::Release);

  {
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut at = leader_idx;
    loop {
      assert!(Instant::now() < deadline, "no split accepted");
      match bo([&node1, &node2][at].propose_split(parent, child, 0, Bytes::from_static(b"\x00"))) {
        Ok(_) => break,
        Err(DriverError::NotLeader { .. }) | Err(DriverError::Rejected { .. }) => {
          at = (at + 1) % 2;
          std::thread::sleep(Duration::from_millis(40));
        }
        Err(e) => panic!("unexpected split error: {e:?}"),
      }
    }
  }

  // Wait until plane 0's apply thread is provably HELD inside the fork.
  {
    let deadline = Instant::now() + Duration::from_secs(15);
    while SPLIT_BARRIER_ENTERED.load(Ordering::Acquire) == 0 {
      assert!(
        Instant::now() < deadline,
        "the split never entered the barrier"
      );
      std::thread::sleep(Duration::from_millis(1));
    }
  }
  eprintln!(
    "isolation proof: plane 0 held inside the fork (entered={})",
    SPLIT_BARRIER_ENTERED.load(Ordering::Acquire),
  );

  // THE ISOLATION PROOF: a bystander commit lands on plane 1 while plane 0 is frozen inside the fork.
  let bys_card = submit_anywhere(&gbys, b"isolated");
  assert!(
    bys_card >= 2,
    "the bystander committed on plane 1 while plane 0 was frozen inside the fork ({bys_card} cells)"
  );

  // Release the barrier; plane 0's fork completes.
  SPLIT_BARRIER_RELEASE.store(true, Ordering::Release);
  SPLIT_BARRIER_ARMED.store(false, Ordering::Release);

  // Completion: the typed SplitApplied on BOTH nodes (the large fork takes real time to finish).
  for (name, h) in [("node 1", &node1), ("node 2", &node2)] {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
      let remaining = deadline.saturating_duration_since(Instant::now());
      assert!(
        remaining > Duration::ZERO,
        "{name}: no SplitApplied in time"
      );
      match h.lifecycle().recv_timeout(remaining) {
        Ok(LifecycleEvent::SplitApplied {
          parent: p,
          child: c,
        }) => {
          assert_eq!((p, c), (parent, child), "{name}: the typed split event");
          break;
        }
        Ok(_) => {}
        Err(e) => panic!("{name}: the lifecycle tail closed: {e:?}"),
      }
    }
  }

  // Both halves keep committing on plane 0, keyed on POST-split commits rather than absolute counts:
  // the child inherited the moved ODD half of the preload and the parent kept its EVEN half — each
  // partition holds ~PRELOAD/2 cells and each still accepts a fresh commit (the returned cardinality
  // reflects the inherited partition plus the new key). The bystander still lives on plane 1.
  let gc = [node1.group(child), node2.group(child)];
  let child_card = submit_anywhere(&gc, b"childkey");
  assert!(
    child_card >= PRELOAD / 2,
    "the child inherited the moved half and committed post-split ({child_card} cells)"
  );
  let parent_card = submit_anywhere(&gp, b"parntkey");
  assert!(
    parent_card >= PRELOAD / 2,
    "the parent kept its half and committed post-split ({parent_card} cells)"
  );
  assert!(
    bo(node1.shard_handle(0).unwrap().group(child).status()).is_ok(),
    "the child lives on the parent's plane 0"
  );
  assert!(
    bo(node1.shard_handle(1).unwrap().group(bystander).status()).is_ok(),
    "the bystander lives on plane 1"
  );

  for h in [&node1, &node2] {
    bo(h.shutdown()).expect("the sharded host tears down");
  }
}

/// The cross-plane split refusal, in isolation: with the child id mapped to plane 1 and the parent
/// on plane 0, `propose_split` refuses TYPED at the handle before any command is sent — no fork on
/// any plane, the parent never freezes, both planes keep serving, and nothing for the child id
/// ever reaches the lifecycle tail (the refusal appended nothing).
#[test]
fn cross_plane_split_refused_typed() {
  let base: SocketAddr = "127.0.0.1:45450".parse().unwrap();
  let parent = 10u64; // plane 0
  let cross_child = 11u64; // plane 1 — the cross-plane child id
  let server = 20u64; // plane 1 — proves plane 1 keeps serving
  let map = ShardMap::<u64>::with_mapping(2, move |g: &u64| usize::from(*g != parent));
  assert_eq!(
    (
      map.shard(&parent),
      map.shard(&cross_child),
      map.shard(&server)
    ),
    (0, 1, 1),
    "the parent is on plane 0, the child and server on plane 1"
  );

  let node: ShardedMultiHandle<u64, u64, CountSm> = spawn_host_mapped(1, base, None, map);
  bo(node.create_group(parent, config(1, vec![1]), parent, CountSm::default(), 0))
    .expect("parent admission");
  bo(node.create_group(server, config(1, vec![1]), server, CountSm::default(), 0))
    .expect("server admission");
  let gp = [node.group(parent)];
  let gsrv = [node.group(server)];
  assert_eq!(submit_anywhere(&gp, b"p1"), 1);
  assert_eq!(submit_anywhere(&gsrv, b"s1"), 1);

  // The cross-plane child refuses at the handle, typed, before any propose.
  match bo(node.propose_split(parent, cross_child, 0, Bytes::from_static(b"\x01"))) {
    Err(DriverError::Rejected { reason }) => {
      assert!(
        reason.contains("plane"),
        "the typed CrossPlane refusal: {reason}"
      );
    }
    other => panic!("expected the cross-plane rejection, got {other:?}"),
  }

  // No fork: the child id exists on NEITHER plane.
  for plane in 0..2usize {
    match bo(
      node
        .shard_handle(plane)
        .unwrap()
        .group(cross_child)
        .status(),
    ) {
      Err(DriverError::Rejected { .. }) => {}
      other => panic!("the refused child must not exist on plane {plane}, got {other:?}"),
    }
  }

  // No freeze, both planes serve: the parent (plane 0) and the server (plane 1) keep committing.
  assert_eq!(
    submit_anywhere(&gp, b"p2"),
    2,
    "plane 0 still serves — the parent never froze"
  );
  assert_eq!(submit_anywhere(&gsrv, b"s2"), 2, "plane 1 still serves");

  // Nothing for the child id ever reached the shared lifecycle tail.
  while let Ok(ev) = node.lifecycle().try_recv() {
    let touches_child = matches!(&ev, LifecycleEvent::UnknownGroup { group, .. } if *group == cross_child)
      || matches!(&ev, LifecycleEvent::RemovedSelf { group } if *group == cross_child)
      || matches!(&ev, LifecycleEvent::Poisoned { group } if *group == cross_child)
      || matches!(&ev, LifecycleEvent::SplitApplied { child, .. } if *child == cross_child)
      || matches!(&ev, LifecycleEvent::SplitRefused { child, .. } if *child == cross_child)
      || matches!(&ev, LifecycleEvent::SplitConflict { child, .. } if *child == cross_child)
      || matches!(&ev, LifecycleEvent::MergeCaptureFailed { source, target } if *source == cross_child || *target == cross_child);
    assert!(
      !touches_child,
      "no lifecycle event may name the refused child: {ev:?}"
    );
  }

  bo(node.shutdown()).expect("the sharded host tears down");
}

/// The same-plane merge with QUIESCE CYCLING: a colocated source+target on plane 1 (Safe read
/// mode, the default; quiescence needs a tracked voter, so the pair is 2-node with leadership
/// pinned to node 1) commit, then idle past the eligibility window until BOTH quiesce. The merge
/// cycles them — `prepare_merge` wakes and freezes the source, which, freeze-pending, can no longer
/// quiesce, so the plane settles at just the target; `commit_merge` dissolves the source and the
/// union re-quiesces serving the summed count — while the merged-away id refuses TERMINALLY on its
/// floor and a plane-0 bystander's quiesced gauge is never touched.
#[test]
fn merge_on_plane_with_quiesce_cycling() {
  let base1: SocketAddr = "127.0.0.1:45500".parse().unwrap();
  let base2: SocketAddr = "127.0.0.1:45510".parse().unwrap();
  let target = 30u64; // plane 1
  let source = 31u64; // plane 1 — encodes strictly ABOVE the target, so it is the dissolving source
  let bystander = 40u64; // plane 0
  assert!(
    source.to_le_bytes() > target.to_le_bytes(),
    "the source must encode above the target for the merge direction"
  );
  let plane_map = || ShardMap::<u64>::with_mapping(2, move |g: &u64| usize::from(*g != bystander));
  assert_eq!(
    (
      plane_map().shard(&source),
      plane_map().shard(&target),
      plane_map().shard(&bystander)
    ),
    (1, 1, 0),
    "the merge pair is on plane 1, the bystander on plane 0"
  );

  let node1: ShardedMultiHandle<u64, u64, CountSm> =
    spawn_host_mapped(1, base1, Some((2, base2)), plane_map());
  let node2: ShardedMultiHandle<u64, u64, CountSm> =
    spawn_host_mapped(2, base2, Some((1, base1)), plane_map());
  for (h, id) in [(&node1, 1u64), (&node2, 2u64)] {
    for gid in [source, target, bystander] {
      bo(h.create_group(gid, config(id, vec![1, 2]), id, CountSm::default(), 0))
        .expect("group admission");
    }
  }
  let gsrc = [node1.group(source), node2.group(source)];
  let gtgt = [node1.group(target), node2.group(target)];
  let gbys = [node1.group(bystander), node2.group(bystander)];
  // Source = 2 units, target = 1; the union serves their sum, 3. The bystander commits once.
  assert_eq!(submit_anywhere(&gsrc, b"s1"), 1);
  assert_eq!(submit_anywhere(&gsrc, b"s2"), 2);
  assert_eq!(submit_anywhere(&gtgt, b"t1"), 1);
  assert_eq!(submit_anywhere(&gbys, b"z"), 1);

  // Pin every group's leadership onto node 1: the merge runs there (so the source's freeze barrier
  // certifies on the target leader), and the gauges we watch are that one driver's leader-side.
  colocate_onto(&gsrc, 1, "source onto node 1");
  colocate_onto(&gtgt, 1, "target onto node 1");
  colocate_onto(&gbys, 1, "bystander onto node 1");

  let plane1 = node1.engine_metrics(1).expect("plane 1 metrics");
  let plane0 = node1.engine_metrics(0).expect("plane 0 metrics");

  // Idle past the eligibility window: both plane-1 groups quiesce, and the plane-0 bystander too.
  wait_for_quiesced(plane1, 2, "plane 1: source + target idle");
  wait_for_quiesced(plane0, 1, "plane 0: bystander idle");

  // Freeze wakes the source; frozen, it is no longer quiesce-eligible, so the plane settles at just
  // the quiesced target. Observe the WAKE EDGE (2 -> 1), not the bare level: the source must be
  // seen leaving quiescence, never a stale reading that was already 1.
  merge_verb_anywhere(
    "the freeze",
    |at| {
      let h = [&node1, &node2][at].clone();
      async move { h.prepare_merge(source, target).await }
    },
    2,
  );
  observe_wake_edge(plane1, 2, 1, "plane 1: the freeze woke the source (2 -> 1)");

  // Commit dissolves the source and the target absorbs; the union re-quiesces serving the sum.
  merge_verb_anywhere(
    "the commit",
    |at| {
      let h = [&node1, &node2][at].clone();
      async move { h.commit_merge(target, source).await }
    },
    2,
  );
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "the union never served"
    );
    if bo(gtgt[0].query(|sm: &CountSm| sm.count())) == Ok(3) {
      break;
    }
    std::thread::sleep(Duration::from_millis(30));
  }
  // The absorb WOKE the target (it was the sole quiesced group at 1), which then re-quiesced serving
  // the union — assert the 1 -> 0 -> 1 edge, not the stale 1 the freeze phase already left standing.
  observe_wake_edge(
    plane1,
    1,
    1,
    "plane 1: the absorb woke the target then re-quiesced (1 -> 0 -> 1)",
  );

  // The merged-away source dies and floors terminally: status refuses, and re-admission refuses on
  // the floor at any generation.
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "the source never died"
    );
    if matches!(bo(gsrc[0].status()), Err(DriverError::Rejected { .. })) {
      break;
    }
    std::thread::sleep(Duration::from_millis(30));
  }
  match bo(node1.create_group(source, config(1, vec![1, 2]), 9, CountSm::default(), 42)) {
    Err(DriverError::Rejected { reason }) => {
      assert!(
        reason.contains("floor"),
        "the terminal floor refusal: {reason}"
      );
    }
    other => panic!("a merged-away id must never re-admit, got {other:?}"),
  }

  // Plane 0's gauge was never touched by the plane-1 merge: the bystander is still the one quiesced
  // group.
  assert_eq!(
    plane0.quiesced_groups(),
    1,
    "the plane-1 merge never perturbed plane 0's quiesced gauge"
  );

  for h in [&node1, &node2] {
    bo(h.shutdown()).expect("every plane tears down");
  }
}

/// A whole reshape CYCLE under continuous keyed load: a parent is split into a same-plane child,
/// then the child is merged BACK in, all while a background loop keeps submitting distinct keys to
/// the surviving parent. The fork is materialized EVERYWHERE (SplitApplied on both nodes, the child
/// live) BEFORE the merge is proposed — the realistic sequence, and the one that never wedges a
/// parked fork's fence against the absorb. The e2e invariant is conservation: after the merge-back
/// the survivor holds EXACTLY the acked key set, `1..=last_acked`, every acked key readable.
#[test]
fn reshape_cycle_under_continuous_load() {
  let base1: SocketAddr = "127.0.0.1:45550".parse().unwrap();
  let base2: SocketAddr = "127.0.0.1:45560".parse().unwrap();
  let parent = 10u64; // the merge-back SURVIVOR (target)
  let child = 11u64; // forked out, then dissolves (encodes above the parent, so it is the source)
  assert!(
    child.to_le_bytes() > parent.to_le_bytes(),
    "the child must encode above the parent to be the merge-back source"
  );
  // Both halves share plane 0 (a split is same-plane; the merge-back absorb is in-plane too).
  let plane_map = || ShardMap::<u64>::with_mapping(2, |_g: &u64| 0usize);

  let node1: ShardedMultiHandle<u64, u64, KeyedSm> =
    spawn_host_mapped(1, base1, Some((2, base2)), plane_map());
  let node2: ShardedMultiHandle<u64, u64, KeyedSm> =
    spawn_host_mapped(2, base2, Some((1, base1)), plane_map());

  for (h, id) in [(&node1, 1u64), (&node2, 2u64)] {
    bo(h.create_group(parent, config(id, vec![1, 2]), id, KeyedSm::default(), 0))
      .expect("parent admission");
  }
  let gp = [node1.group(parent), node2.group(parent)];

  // The continuous keyed load on the survivor across the whole cycle: the loader's first submits
  // also elect the parent's leader (it redirects until one emerges).
  let stop = Arc::new(AtomicBool::new(false));
  let last_acked = Arc::new(AtomicU64::new(0));
  let loader = spawn_keyed_loader(
    vec![node1.clone(), node2.clone()],
    parent,
    stop.clone(),
    last_acked.clone(),
  );
  // Let the parent accumulate enough keys that the ODD-key partition hands the child real state.
  wait_until_at_least(&last_acked, 8, "loader warmup before the split");

  // Split the parent into the same-plane child (instruction ignored by the keyed FSM).
  {
    let handles = [&node1, &node2];
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let mut at = 0usize;
    loop {
      assert!(std::time::Instant::now() < deadline, "no split accepted");
      match bo(handles[at].propose_split(parent, child, 0, Bytes::from_static(b"\x01"))) {
        Ok(_) => break,
        Err(DriverError::NotLeader { .. }) | Err(DriverError::Rejected { .. }) => {
          at = (at + 1) % handles.len();
          std::thread::sleep(Duration::from_millis(40));
        }
        Err(e) => panic!("unexpected split error: {e:?}"),
      }
    }
  }

  // Materialize the fork EVERYWHERE before merging: SplitApplied on both nodes, then the child
  // serves (a live group, not a parked fork) — the sequence that keeps a later absorb from wedging
  // behind a parked fork's still-standing capture fence.
  for (name, h) in [("node 1", &node1), ("node 2", &node2)] {
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
      let remaining = deadline.saturating_duration_since(std::time::Instant::now());
      assert!(
        remaining > Duration::ZERO,
        "{name}: no SplitApplied in time"
      );
      match h.lifecycle().recv_timeout(remaining) {
        Ok(LifecycleEvent::SplitApplied {
          parent: p,
          child: c,
        }) => {
          assert_eq!((p, c), (parent, child), "{name}: the typed split event");
          break;
        }
        Ok(_) => {}
        Err(e) => panic!("{name}: the lifecycle tail closed: {e:?}"),
      }
    }
  }
  let gc = [node1.group(child), node2.group(child)];
  {
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
      assert!(
        std::time::Instant::now() < deadline,
        "the child never served its partition"
      );
      let child_live = matches!(bo(gc[0].query(|sm: &KeyedSm| sm.keys().len())), Ok(n) if n >= 1)
        || matches!(bo(gc[1].query(|sm: &KeyedSm| sm.keys().len())), Ok(n) if n >= 1);
      if child_live {
        break;
      }
      std::thread::sleep(Duration::from_millis(30));
    }
  }

  // Merge the child BACK into the parent: colocate the child's leadership onto the parent's leader
  // (the freeze-barrier discipline), then freeze and commit through whichever node leads each verb.
  let t_leader = find_leader(&gp, "parent leader pre-merge");
  colocate_onto(&gc, t_leader as u64 + 1, "child onto parent leader");
  merge_verb_anywhere(
    "the freeze",
    |at| {
      let h = [&node1, &node2][at].clone();
      async move { h.prepare_merge(child, parent).await }
    },
    2,
  );
  merge_verb_anywhere(
    "the commit",
    |at| {
      let h = [&node1, &node2][at].clone();
      async move { h.commit_merge(parent, child).await }
    },
    2,
  );

  // The child dissolves everywhere.
  {
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
      assert!(
        std::time::Instant::now() < deadline,
        "the child never tore down everywhere"
      );
      let gone = [&node1, &node2]
        .iter()
        .filter(|h| {
          matches!(
            bo(h.group(child).status()),
            Err(DriverError::Rejected { .. })
          )
        })
        .count();
      if gone == 2 {
        break;
      }
      std::thread::sleep(Duration::from_millis(30));
    }
  }

  // Stop the load and take the ground truth: the acked keys are exactly `1..=last_acked`.
  stop.store(true, Ordering::Release);
  loader.join().expect("the loader thread joins");
  let acked: BTreeSet<u64> = (1..=last_acked.load(Ordering::Acquire)).collect();
  assert!(acked.len() >= 8, "the load ran ({} keys)", acked.len());

  // CONSERVATION at e2e grain: the survivor holds EXACTLY the acked key set — the child's odd-key
  // partition folded back, nothing lost, nothing invented — every acked key readable.
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "the survivor never converged on the acked key set"
    );
    if let Ok(keys) = bo(gp[t_leader].query(|sm: &KeyedSm| sm.keys().clone()))
      && keys == acked
    {
      break;
    }
    std::thread::sleep(Duration::from_millis(30));
  }

  for h in [&node1, &node2] {
    bo(h.shutdown()).expect("the sharded host tears down");
  }
}

/// THE ENGINE SEAM, from outside the crate. This file is an integration test: it can name only
/// `sailing-compio`'s PUBLIC API, so a host built here over [`DelegatingEngine`] — a foreign
/// `MultiEngine` that is neither a re-export nor an alias of the built-in one — is proof the seam
/// is reachable by a downstream crate, which a `pub(crate)` constructor would not be.
///
/// The factory contract is pinned too: invoked ONCE per plane index, on the spawning thread,
/// its product moving into that plane. Then both planes are driven for real — a sole-voter group
/// on each elects and commits with no network in the path — and the engines' own barrier counter
/// proves the planes ran on the engines handed in, not on ones the host quietly built itself.
#[test]
fn a_foreign_engine_reaches_every_plane_through_the_public_seam() {
  let base: SocketAddr = "127.0.0.1:45370".parse().unwrap();
  let map = ShardMap::<u64>::uniform(2);
  let (g_a, g_b) = distinct_shard_gids(&map);

  let built = Arc::new(AtomicUsize::new(0));
  let planes = Arc::new(std::sync::Mutex::new(Vec::<usize>::new()));
  let watch = Arc::new(EngineWatch::default());

  let node = {
    let built = built.clone();
    let planes = planes.clone();
    let watch = watch.clone();
    ShardedCompioHost::<u64, u64, CountSm, Labeled<Passthrough>>::new(
      map.clone(),
      base,
      Vec::new(),
      plain_records(1),
      DriverConfig::default(),
    )
    .with_engine_factory(move |shard: usize| {
      built.fetch_add(1, Ordering::SeqCst);
      planes.lock().unwrap().push(shard);
      DelegatingEngine::new(watch.clone())
    })
    .spawn()
    .expect("the sharded host spawns over a foreign engine")
  };

  // `spawn` returns only once every plane is bound, so every factory call has already happened.
  assert_eq!(
    built.load(Ordering::SeqCst),
    2,
    "one engine per plane — no more, and none shared"
  );
  let mut seen = planes.lock().unwrap().clone();
  seen.sort_unstable();
  assert_eq!(
    seen,
    vec![0, 1],
    "the factory saw every plane index exactly once"
  );

  // Both planes run for real on their foreign engines: a sole-voter group needs no network to
  // elect or commit, so a failure here is the engine, not the loopback.
  for gid in [g_a, g_b] {
    bo(node.create_group(gid, config(1, vec![1]), gid, CountSm::default(), 0))
      .expect("group admission through the foreign engine");
  }
  for gid in [g_a, g_b] {
    let handle = node.group(gid);
    assert_eq!(
      submit_anywhere(std::slice::from_ref(&handle), b"foreign"),
      1,
      "the group commits through the foreign engine"
    );
  }
  assert!(
    watch.barriers.load(Ordering::SeqCst) > 0,
    "the cranks ran the FOREIGN engine's barrier — the host did not substitute its own"
  );

  bo(node.shutdown()).expect("the sharded host tears down");
  assert!(
    !watch.staged_at_drop.load(Ordering::SeqCst),
    "every plane's teardown ran its final barrier: no staged work died with the engine"
  );
}
