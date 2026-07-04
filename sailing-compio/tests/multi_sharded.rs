//! Real-socket integration for the SHARDED compio host: 2 nodes × K=2 planes over loopback TCP,
//! every plane a complete multi driver on its own thread with its own engine barrier and its own
//! per-shard listener. The suites assert the plane model end to end: groups mapped to different
//! shards elect and commit through one routing [`ShardedMultiHandle`] each, replication flows
//! shard(g) ↔ shard(g) (a group is hosted on exactly its mapped plane), the per-plane barriers
//! run independently, one plane's poisoned group leaves the other plane committing, factories
//! materialize solicited groups on the RIGHT plane hands-free, and shutdown releases every
//! plane. The test thread drives the `Send` handles with a plain futures executor — no compio
//! runtime on the caller side, exactly the embedder shape.

mod common;

use std::{future::Future, net::SocketAddr, rc::Rc, sync::Arc, time::Duration};

use bytes::Bytes;
use common::{CountSm, TrapSm};
use sailing_compio::{
  BoxedGroupFactory, DriverConfig, DriverError, GroupBlueprint, GroupHandle, LifecycleEvent, Node,
  ShardMap, ShardedCompioHost, ShardedMultiHandle, SpawnError, factory_fn,
};
use sailing_proto::{ClusterId, Config, Data, LabelOptions, Labeled, Passthrough, StateMachine};

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
/// planes by the same convention.
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
  let peers = peer
    .map(|(pid, addr)| Node::new(pid, addr))
    .into_iter()
    .collect();
  ShardedCompioHost::<u64, u64, F, Labeled<Passthrough>>::new(
    ShardMap::uniform(2),
    base,
    peers,
    plain_records(id),
    DriverConfig::default(),
  )
  .spawn()
  .expect("the sharded host spawns")
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
      h.engine_metrics(shard_a).unwrap().flushes() > 0,
      "plane {shard_a}'s barrier ran"
    );
    assert!(
      h.engine_metrics(shard_b).unwrap().flushes() > 0,
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
  // planes without shard filtering.
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
    bo(node1.create_group(gid, config(1, vec![1, 2]), gid, CountSm::default()))
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
