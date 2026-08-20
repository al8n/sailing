//! Real-socket integration for the MULTI-GROUP reactor stream driver: loopback-TCP hosts carrying
//! several Raft groups over shared connections and one shared storage engine, on a multi-thread
//! tokio runtime (which proves the `Send` `run()`). Groups arrive at runtime through
//! `MultiHandle::create_group`; the suites assert cross-group isolation, the engine's batched
//! durability barrier, group removal failing exactly its own parked work, and one poisoned group
//! leaving its co-hosted groups committing.

mod common;

use std::{
  net::SocketAddr,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
  },
  time::Duration,
};

use agnostic::tokio::TokioRuntime;
use bytes::Bytes;
use common::{CountSm, TrapSm};
use sailing_proto::{
  ClusterId, ConfChange, ConfChangeType, Config, Data, Event, Index, LabelOptions, Labeled,
  Passthrough, ReadOnlyOption, Role,
};
use sailing_reactor::{
  DriverConfig, DriverError, GroupBlueprint, GroupHandle, LifecycleEvent, MultiHandle,
  MultiReactorStreamDriver, Node, factory_fn,
};

const ELECTION: Duration = Duration::from_millis(300);
const HEARTBEAT: Duration = Duration::from_millis(60);

fn cluster() -> ClusterId {
  ClusterId([11; 16])
}

fn encoded(id: u64) -> Vec<u8> {
  let mut v = Vec::new();
  id.encode(&mut v);
  v
}

fn addrs(base_port: u16, n: u16) -> Vec<SocketAddr> {
  (0..n)
    .map(|i| format!("127.0.0.1:{}", base_port + i).parse().unwrap())
    .collect()
}

/// Plaintext dialer/acceptor factories for node `id` (the single-group suite's, shared here).
fn plain_factories(
  id: u64,
) -> (
  sailing_reactor::DialerFactory<u64, Labeled<Passthrough>>,
  sailing_reactor::AcceptorFactory<Labeled<Passthrough>>,
) {
  let local = encoded(id);
  let dial_local = local.clone();
  let dialer: sailing_reactor::DialerFactory<u64, Labeled<Passthrough>> =
    Arc::new(move |_: &u64| {
      Labeled::dialer(
        Passthrough::new(),
        &LabelOptions {
          cluster: cluster(),
          local_id: dial_local.clone(),
        },
      )
      .map_err(std::io::Error::other)
    });
  let acceptor: sailing_reactor::AcceptorFactory<Labeled<Passthrough>> = Arc::new(move || {
    Labeled::acceptor(
      Passthrough::new(),
      &LabelOptions {
        cluster: cluster(),
        local_id: local.clone(),
      },
    )
    .map_err(std::io::Error::other)
  });
  (dialer, acceptor)
}

type MDriver<F> = MultiReactorStreamDriver<TokioRuntime, u64, u64, F, Labeled<Passthrough>>;

/// Bind one EMPTY multi-group host (groups arrive via commands).
async fn bind_node<F>(
  id: u64,
  addr: SocketAddr,
  peers: Vec<Node<u64, SocketAddr>>,
) -> (MDriver<F>, MultiHandle<u64, u64, F>)
where
  F: sailing_proto::StateMachine + Send,
  F::Command: Data + Send,
  F::Snapshot: Data,
  F::Response: Clone + Send,
  F::Error: core::error::Error,
{
  bind_node_with::<F>(id, addr, peers, DriverConfig::default()).await
}

/// Bind one EMPTY multi-group host with a caller-tuned driver config (the lifecycle
/// backpressure test shrinks the tail to a single slot).
async fn bind_node_with<F>(
  id: u64,
  addr: SocketAddr,
  peers: Vec<Node<u64, SocketAddr>>,
  cfg: DriverConfig,
) -> (MDriver<F>, MultiHandle<u64, u64, F>)
where
  F: sailing_proto::StateMachine + Send,
  F::Command: Data + Send,
  F::Snapshot: Data,
  F::Response: Clone + Send,
  F::Error: core::error::Error,
{
  let (dialer, acceptor) = plain_factories(id);
  MDriver::<F>::bind(addr, peers, dialer, acceptor, cfg)
    .await
    .expect("the empty multi host binds")
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

/// Create group `gid` with the given voters on every handle (node ids are 1-based positions).
async fn create_group_everywhere<F>(handles: &[MultiHandle<u64, u64, F>], gid: u64, voters: &[u64])
where
  F: sailing_proto::StateMachine + Send + Default,
  F::Command: Data + Send,
  F::Response: Send,
{
  for (i, h) in handles.iter().enumerate() {
    let id = i as u64 + 1;
    h.create_group(gid, config(id, voters.to_vec()), id, F::default(), 0)
      .await
      .expect("group admission");
  }
}

/// Submit through whichever node is (or redirects to) the group's leader.
async fn submit_anywhere<F>(groups: &[GroupHandle<u64, u64, F>], payload: &'static [u8]) -> u64
where
  F: sailing_proto::StateMachine<Command = Bytes, Response = u64> + Send,
  F::Command: Data + Send,
{
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  let mut at = 0usize;
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "no commit within the deadline"
    );
    match groups[at].submit(Bytes::from_static(payload)).await {
      Ok(response) => return response,
      Err(DriverError::NotLeader { leader }) => {
        at = leader
          .map(|l| (l - 1) as usize)
          .unwrap_or((at + 1) % groups.len());
        tokio::time::sleep(Duration::from_millis(50)).await;
      }
      Err(DriverError::Superseded) => {}
      // A node that does not host the group (de-hosted mid-test): try the next one.
      Err(DriverError::Rejected { .. }) => {
        at = (at + 1) % groups.len();
        tokio::time::sleep(Duration::from_millis(50)).await;
      }
      Err(e) => panic!("unexpected submit error: {e:?}"),
    }
  }
}

/// Query the group through any node that will serve it (sailing forwards follower reads).
async fn query_anywhere(groups: &[GroupHandle<u64, u64, CountSm>]) -> u64 {
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "no query within the deadline"
    );
    for g in groups {
      if let Ok(c) = g.query(|sm: &CountSm| sm.count()).await {
        return c;
      }
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
  }
}

/// Await a lifecycle event satisfying `pred` on `rx` (draining non-matching ones), within 15s.
async fn await_lifecycle<P>(rx: &flume::Receiver<LifecycleEvent<u64, u64>>, what: &str, mut pred: P)
where
  P: FnMut(&LifecycleEvent<u64, u64>) -> bool,
{
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  loop {
    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
    assert!(
      remaining > Duration::ZERO,
      "{what}: no matching lifecycle event in time"
    );
    match tokio::time::timeout(remaining, rx.recv_async()).await {
      Ok(Ok(ev)) if pred(&ev) => return,
      Ok(Ok(_)) => {}
      Ok(Err(e)) => panic!("{what}: the lifecycle tail closed: {e:?}"),
      Err(_) => panic!("{what}: no matching lifecycle event in time"),
    }
  }
}

/// Poll `group`'s status until it reports poisoned (fail-stopped), within 15s.
async fn await_poisoned(group: &GroupHandle<u64, u64, CountSm>, what: &str) {
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "{what}: the group never fail-stopped"
    );
    if group.status().await.is_ok_and(|s| s.is_poisoned) {
      return;
    }
    tokio::time::sleep(Duration::from_millis(30)).await;
  }
}

/// Group 100's current leader index among `groups` (status-polled, deadline-bounded).
async fn find_leader(groups: &[GroupHandle<u64, u64, CountSm>], what: &str) -> usize {
  let deadline = std::time::Instant::now() + Duration::from_secs(10);
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "{what}: no leader in time"
    );
    for (i, g) in groups.iter().enumerate() {
      if let Ok(status) = g.status().await
        && status.role == Role::Leader
      {
        return i;
      }
    }
    tokio::time::sleep(Duration::from_millis(30)).await;
  }
}

/// The gate: a 3-node host carries groups 100 and 200 over ONE mesh of shared connections; each
/// group elects, commits through redirects, and serves linearizable reads — and the two groups'
/// state machines stay strictly isolated (each counts only its own commits).
#[tokio::test(flavor = "multi_thread")]
async fn three_node_host_isolates_co_located_groups() {
  let addrs = addrs(44_100, 3);
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  for id in 1u64..=3 {
    let peers: Vec<_> = (1u64..=3)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(id, addrs[(id - 1) as usize], peers).await;
    tokio::spawn(driver.run());
    handles.push(handle);
  }
  create_group_everywhere(&handles, 100, &[1, 2, 3]).await;
  create_group_everywhere(&handles, 200, &[1, 2, 3]).await;

  let g100: Vec<_> = handles.iter().map(|h| h.group(100)).collect();
  let g200: Vec<_> = handles.iter().map(|h| h.group(200)).collect();

  // Interleave the two groups' commits: the counts prove per-group apply streams.
  assert_eq!(submit_anywhere(&g100, b"a1").await, 1);
  assert_eq!(submit_anywhere(&g200, b"b1").await, 1);
  assert_eq!(submit_anywhere(&g100, b"a2").await, 2);
  assert_eq!(submit_anywhere(&g200, b"b2").await, 2);
  assert_eq!(submit_anywhere(&g200, b"b3").await, 3);

  // Linearizable reads see exactly their own group's commits — cross-group isolation.
  assert_eq!(
    query_anywhere(&g100).await,
    2,
    "group 100 saw its 2 commits"
  );
  assert_eq!(
    query_anywhere(&g200).await,
    3,
    "group 200 saw its 3 commits"
  );

  // The shared events tail is group-stamped; every stamp names a hosted group.
  let mut stamped = 0;
  while let Ok((g, _ev)) = handles[0].events().try_recv() {
    assert!(g == 100 || g == 200, "a foreign group stamp: {g}");
    stamped += 1;
  }
  assert!(stamped > 0, "the tail observed group-stamped events");
}

/// The shared engine's durability barrier AMORTIZES: a concurrent burst of submits into TWO
/// co-hosted groups on one node rides fewer flushes than it completes operations (the delta over
/// the burst window is the honest witness; total counters include idle barriers).
#[tokio::test(flavor = "multi_thread")]
async fn engine_barrier_amortizes_ops_across_groups() {
  let addr: SocketAddr = "127.0.0.1:44120".parse().unwrap();
  let (driver, handle) = bind_node::<CountSm>(1, addr, Vec::new()).await;
  let metrics = driver.engine_metrics();
  tokio::spawn(driver.run());

  handle
    .create_group(100, config(1, vec![1]), 1, CountSm::default(), 0)
    .await
    .expect("group 100 admitted");
  handle
    .create_group(200, config(1, vec![1]), 2, CountSm::default(), 0)
    .await
    .expect("group 200 admitted");
  let g100 = handle.group(100);
  let g200 = handle.group(200);

  // Warm up: both single-voter groups elect and commit once.
  assert_eq!(submit_anywhere(std::slice::from_ref(&g100), b"w").await, 1);
  assert_eq!(submit_anywhere(std::slice::from_ref(&g200), b"w").await, 1);

  let barriers0 = metrics.barriers();
  let ops0 = metrics.ops_batched();

  // A concurrent 16-submit burst across BOTH groups: the loop-top command drain dispatches the
  // batch, so ONE barrier covers many staged appends across the two groups.
  let mut futs = Vec::new();
  for _ in 0..8 {
    futs.push(g100.submit(Bytes::from_static(b"x")));
  }
  for _ in 0..8 {
    futs.push(g200.submit(Bytes::from_static(b"x")));
  }
  for r in futures_util::future::join_all(futs).await {
    r.expect("every burst submit commits");
  }

  let barriers1 = metrics.barriers();
  let ops1 = metrics.ops_batched();
  assert!(barriers1 > 0, "the barrier ran");
  assert!(
    ops1 - ops0 > barriers1 - barriers0,
    "the burst amortized: {} ops rode {} barriers",
    ops1 - ops0,
    barriers1 - barriers0
  );
}

/// Removing a group fails exactly ITS parked work — and only when the REMOVING node held it. A
/// 2-node group loses its follower (removal there strands the leader without quorum), a submit on
/// the leader parks uncommittable, and removing the group on the leader fails that parked submit
/// with the group-scoped teardown verdict while the co-hosted group keeps committing.
#[tokio::test(flavor = "multi_thread")]
async fn remove_group_fails_parked_ops_and_the_host_survives() {
  let addrs = addrs(44_140, 2);
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  for id in 1u64..=2 {
    let peers: Vec<_> = (1u64..=2)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(id, addrs[(id - 1) as usize], peers).await;
    tokio::spawn(driver.run());
    handles.push(handle);
  }
  create_group_everywhere(&handles, 100, &[1, 2]).await;
  create_group_everywhere(&handles, 300, &[1, 2]).await;

  let g300: Vec<_> = handles.iter().map(|h| h.group(300)).collect();
  assert_eq!(submit_anywhere(&g300, b"seed").await, 1);

  // Find group 300's leader node.
  let deadline = std::time::Instant::now() + Duration::from_secs(10);
  let leader_at = loop {
    assert!(std::time::Instant::now() < deadline, "no leader in time");
    let mut found = None;
    for (i, g) in g300.iter().enumerate() {
      if g.status().await.expect("status").role == Role::Leader {
        found = Some(i);
        break;
      }
    }
    if let Some(i) = found {
      break i;
    }
    tokio::time::sleep(Duration::from_millis(30)).await;
  };
  let follower_at = 1 - leader_at;

  // Remove the group on the FOLLOWER: the leader keeps leading (no check_quorum) but can never
  // commit again — the follower now drops group 300's frames as unhosted.
  assert!(
    handles[follower_at]
      .remove_group(300)
      .await
      .expect("remove resolves"),
    "the follower hosted group 300"
  );

  // A submit on the still-leader node is ACCEPTED and parks forever (quorum 2 is unreachable).
  let parked = tokio::spawn({
    let g = g300[leader_at].clone();
    async move { g.submit(Bytes::from_static(b"parked")).await }
  });
  tokio::time::sleep(Duration::from_millis(400)).await;
  assert!(
    !parked.is_finished(),
    "the submit must park: its quorum no longer hosts the group"
  );

  // Removing the group on the LEADER fails ITS parked work with the group-scoped verdict.
  assert!(
    handles[leader_at]
      .remove_group(300)
      .await
      .expect("remove resolves")
  );
  match tokio::time::timeout(Duration::from_secs(5), parked).await {
    Ok(Ok(Err(DriverError::ShuttingDown))) => {}
    other => panic!("expected the parked submit to fail ShuttingDown, got {other:?}"),
  }

  // Both drivers survived the removals: the co-hosted group still commits across the 2-node mesh,
  // and the removed group now reports no-such-group.
  let g100: Vec<_> = handles.iter().map(|h| h.group(100)).collect();
  assert_eq!(submit_anywhere(&g100, b"alive").await, 1);
  match g300[leader_at].status().await {
    Err(DriverError::Rejected { reason }) => {
      assert!(reason.contains("no such group"), "got: {reason}");
    }
    other => panic!("expected the no-such-group rejection, got {other:?}"),
  }
}

/// One group's fail-stop is GROUP-scoped: an FSM apply fault poisons group 100 (its parked submit
/// fails with the typed verdict and later operations keep reporting it) while co-hosted group 200
/// keeps committing on the SAME driver — one group's poison must not kill the host.
#[tokio::test(flavor = "multi_thread")]
async fn a_poisoned_group_leaves_co_hosted_groups_committing() {
  let addr: SocketAddr = "127.0.0.1:44160".parse().unwrap();
  let (driver, handle) = bind_node::<TrapSm>(1, addr, Vec::new()).await;
  tokio::spawn(driver.run());

  handle
    .create_group(100, config(1, vec![1]), 1, TrapSm::default(), 0)
    .await
    .expect("group 100 admitted");
  handle
    .create_group(200, config(1, vec![1]), 2, TrapSm::default(), 0)
    .await
    .expect("group 200 admitted");
  let g100 = handle.group(100);
  let g200 = handle.group(200);

  // Both groups are healthy first.
  assert_eq!(submit_anywhere(std::slice::from_ref(&g100), b"ok").await, 1);
  assert_eq!(submit_anywhere(std::slice::from_ref(&g200), b"ok").await, 1);

  // The trap payload commits on group 100 and FAILS its apply → that group fail-stops; the
  // waiting submit is failed with the typed verdict by the group-scoped poison sweep.
  match g100.submit(Bytes::from_static(b"BOOM")).await {
    Err(DriverError::Poisoned) => {}
    other => panic!("expected Poisoned from the trapped apply, got {other:?}"),
  }
  // The poison is observable and sticky on group 100 only.
  let status = g100.status().await.expect("status still answers");
  assert!(status.is_poisoned, "group 100 fail-stopped");
  match g100.submit(Bytes::from_static(b"late")).await {
    Err(DriverError::Poisoned) => {}
    other => panic!("expected Poisoned on the dead group, got {other:?}"),
  }

  // The DRIVER is alive and group 200 is untouched: it keeps committing and reading.
  assert_eq!(
    submit_anywhere(std::slice::from_ref(&g200), b"alive").await,
    2
  );
  assert!(!g200.status().await.expect("status").is_poisoned);

  // A failover query off the (inert, monotonic-only) failover tier falls back cleanly per group.
  let out: Option<u64> = g200
    .failover_query(|_fsm: &TrapSm, _win| Some(9))
    .await
    .expect("the failover query resolves");
  assert_eq!(out, None, "no serve window → normal-read fallback");
}

/// Poll `metrics` until its quiesced-group gauge reaches `want` (the driver publishes it after
/// every quiesce sweep).
async fn wait_for_quiesced(metrics: &sailing_reactor::EngineMetrics, want: u64, what: &str) {
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  while metrics.quiesced_groups() != want {
    assert!(
      std::time::Instant::now() < deadline,
      "{what}: quiesced gauge stuck at {} (want {want})",
      metrics.quiesced_groups()
    );
    tokio::time::sleep(Duration::from_millis(30)).await;
  }
}

/// Steady-state quiescence and per-group wake: a 2-node host carries two groups; both commit,
/// then go idle past the eligibility window (one election timeout) — BOTH drivers' quiesced
/// gauges reach 2 (the leader side quiesces by eligibility, the follower side by the flagged
/// beat's `GroupControl` round-trip, the only path that quiesces a follower) and STAY there
/// across a multi-heartbeat-interval quiet window with terms frozen: a quiesced leader emits no
/// beats by construction (the due sweep skips it) and a woken follower would drop the gauge, so
/// the steady gauges + frozen terms are the zero-frame witness (the armed deadline left is the
/// housekeeping backstop, which touches no group). A propose then wakes exactly ITS group,
/// commits, and the sibling stays quiesced; the woken group re-quiesces once idle again.
#[tokio::test(flavor = "multi_thread")]
async fn idle_groups_quiesce_and_a_propose_wakes_only_its_group() {
  let addrs = addrs(44_200, 2);
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  let mut metrics = Vec::new();
  for id in 1u64..=2 {
    let peers: Vec<_> = (1u64..=2)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(id, addrs[(id - 1) as usize], peers).await;
    metrics.push(driver.engine_metrics());
    tokio::spawn(driver.run());
    handles.push(handle);
  }
  create_group_everywhere(&handles, 100, &[1, 2]).await;
  create_group_everywhere(&handles, 200, &[1, 2]).await;

  let g100: Vec<_> = handles.iter().map(|h| h.group(100)).collect();
  let g200: Vec<_> = handles.iter().map(|h| h.group(200)).collect();
  assert_eq!(submit_anywhere(&g100, b"a").await, 1);
  assert_eq!(submit_anywhere(&g200, b"b").await, 1);

  // Idle past the eligibility window: every group quiesces on BOTH sides.
  wait_for_quiesced(&metrics[0], 2, "node 1 both groups").await;
  wait_for_quiesced(&metrics[1], 2, "node 2 both groups").await;

  // The quiet window: gauges hold and terms freeze (status is pure observability — no wake).
  let term100 = g100[0].status().await.expect("status").term;
  let term200 = g200[0].status().await.expect("status").term;
  for _ in 0..10 {
    tokio::time::sleep(HEARTBEAT).await;
    assert_eq!(metrics[0].quiesced_groups(), 2, "node 1 stays quiesced");
    assert_eq!(metrics[1].quiesced_groups(), 2, "node 2 stays quiesced");
  }
  assert_eq!(
    g100[0].status().await.expect("status").term,
    term100,
    "no election disturbed the quiesced group"
  );

  // Wake-on-propose: group 100 wakes, commits, and the SIBLING stays quiesced.
  assert_eq!(submit_anywhere(&g100, b"wake").await, 2);
  let status200 = g200[0].status().await.expect("status");
  assert_eq!(status200.term, term200, "group 200 saw no traffic");
  assert!(
    metrics.iter().all(|m| m.quiesced_groups() >= 1),
    "the sibling group stayed quiesced through the wake"
  );

  // The woken group re-quiesces once idle again — the full cycle.
  wait_for_quiesced(&metrics[0], 2, "node 1 re-quiesce").await;
  wait_for_quiesced(&metrics[1], 2, "node 2 re-quiesce").await;
}

/// Wake-on-connection-loss: a 3-node cluster quiesces its group, then the LEADER's driver shuts
/// down (its sockets close — the liveness oracle fires on the survivors). The survivors'
/// stale election deadlines re-enter the fold, fire immediately, and a NEW leader emerges: a
/// submit through the survivors commits.
#[tokio::test(flavor = "multi_thread")]
async fn conn_loss_wakes_quiesced_followers_and_reelects() {
  let addrs = addrs(44_220, 3);
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  let mut metrics = Vec::new();
  for id in 1u64..=3 {
    let peers: Vec<_> = (1u64..=3)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(id, addrs[(id - 1) as usize], peers).await;
    metrics.push(driver.engine_metrics());
    tokio::spawn(driver.run());
    handles.push(handle);
  }
  create_group_everywhere(&handles, 100, &[1, 2, 3]).await;
  let g100: Vec<_> = handles.iter().map(|h| h.group(100)).collect();
  assert_eq!(submit_anywhere(&g100, b"seed").await, 1);

  for (i, m) in metrics.iter().enumerate() {
    wait_for_quiesced(m, 1, &format!("node {} quiesces", i + 1)).await;
  }

  // Find and kill the leader.
  let deadline = std::time::Instant::now() + Duration::from_secs(10);
  let leader_at = loop {
    assert!(std::time::Instant::now() < deadline, "no leader in time");
    let mut found = None;
    for (i, g) in g100.iter().enumerate() {
      if g.status().await.expect("status").role == Role::Leader {
        found = Some(i);
        break;
      }
    }
    if let Some(i) = found {
      break i;
    }
    tokio::time::sleep(Duration::from_millis(30)).await;
  };
  let survivors: Vec<usize> = (0..3).filter(|&i| i != leader_at).collect();
  let old_term = g100[survivors[0]].status().await.expect("status").term;
  handles[leader_at]
    .shutdown()
    .await
    .expect("the leader driver tears down");

  // The conn-loss wake, observed in ISOLATION: only wake-free status/gauge polling below, so the
  // gauges dropping to 0 is the connection-loss trigger itself (no command ever wakes these
  // groups) — and the drop cannot revert, since re-quiescing needs every voter matched and the
  // dead voter's match can never advance again.
  let deadline = std::time::Instant::now() + Duration::from_secs(20);
  for &i in &survivors {
    while metrics[i].quiesced_groups() != 0 {
      assert!(
        std::time::Instant::now() < deadline,
        "survivor {i} never observed the connection loss"
      );
      tokio::time::sleep(Duration::from_millis(30)).await;
    }
  }

  // The woken survivors' STALE election deadlines fire immediately: a new leader emerges at a
  // higher term, still with no command issued.
  let deadline = std::time::Instant::now() + Duration::from_secs(20);
  'reelected: loop {
    assert!(
      std::time::Instant::now() < deadline,
      "no re-election within the deadline"
    );
    for &i in &survivors {
      let status = g100[i].status().await.expect("status");
      if status.role == Role::Leader && status.term > old_term {
        break 'reelected;
      }
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
  }

  // And the re-elected group commits through the survivors.
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  'committed: loop {
    assert!(
      std::time::Instant::now() < deadline,
      "no post-election commit within the deadline"
    );
    for &i in &survivors {
      if g100[i].submit(Bytes::from_static(b"after")).await.is_ok() {
        break 'committed;
      }
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
  }
}

/// A probing peer blocks quiescence (the Probe leg of the lagging-peer eligibility gate): group
/// 100 adds a learner that never comes up, so its Progress sits in Probe forever (equal-match
/// acks never arrive to promote it), and group 100 must never quiesce — a probing peer still
/// draws the gated heartbeat-response append pump, the traffic the shrunk absorb set (exactly
/// `HeartbeatResponse`) no longer covers. The follower side never quiesces either (its only
/// quiesce path is the leader's flagged beat, which eligibility withholds), so BOTH drivers'
/// gauges settle at exactly 1 — the sibling group — and hold across a long quiet window, with
/// both groups still serving.
#[tokio::test(flavor = "multi_thread")]
async fn a_probing_peer_blocks_quiescence() {
  let addrs = addrs(44_500, 2);
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  let mut metrics = Vec::new();
  for id in 1u64..=2 {
    let peers: Vec<_> = (1u64..=2)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(id, addrs[(id - 1) as usize], peers).await;
    metrics.push(driver.engine_metrics());
    tokio::spawn(driver.run());
    handles.push(handle);
  }
  create_group_everywhere(&handles, 100, &[1, 2]).await;
  create_group_everywhere(&handles, 200, &[1, 2]).await;
  let g100: Vec<_> = handles.iter().map(|h| h.group(100)).collect();
  let g200: Vec<_> = handles.iter().map(|h| h.group(200)).collect();
  assert_eq!(submit_anywhere(&g100, b"a").await, 1);
  assert_eq!(submit_anywhere(&g200, b"b").await, 1);

  // Add learner 9 to group 100; node 9 is never started, so its Progress probes forever.
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "no committed learner add in time"
    );
    let at = find_leader(&g100, "group 100 pre-learner").await;
    let cc = ConfChange::new(ConfChangeType::AddLearnerNode, 9, Bytes::new());
    match g100[at].conf_change(cc).await {
      Ok(_) => break,
      Err(DriverError::NotLeader { .. }) => {
        tokio::time::sleep(Duration::from_millis(50)).await;
      }
      Err(e) => panic!("unexpected conf-change error: {e:?}"),
    }
  }

  // The sibling quiesces on BOTH drivers; group 100 cannot (its learner probes), so each gauge
  // settles at exactly 1 and HOLDS — before the probing gate, group 100 quiesced too and the
  // gauges reached 2.
  wait_for_quiesced(&metrics[0], 1, "node 1 sibling only").await;
  wait_for_quiesced(&metrics[1], 1, "node 2 sibling only").await;
  for _ in 0..10 {
    tokio::time::sleep(HEARTBEAT).await;
    assert_eq!(
      metrics[0].quiesced_groups(),
      1,
      "node 1: the probing-learner group must not quiesce"
    );
    assert_eq!(
      metrics[1].quiesced_groups(),
      1,
      "node 2: the probing-learner group must not quiesce"
    );
  }

  // Both groups still serve.
  assert_eq!(submit_anywhere(&g100, b"a2").await, 2);
  assert_eq!(submit_anywhere(&g200, b"b2").await, 2);
}

/// A lagging LEARNER blocks quiescence (the match leg of the lagging-peer eligibility gate — the
/// Replicate-state sibling of the probing case): a live learner catches up, so its group
/// quiesces on every host; then the learner's node dies and the voters commit past its match, so
/// its tracked Progress sits in Replicate with a stale match. The leader must stay awake — the
/// learner's catch-up appends ride heartbeat responses, and a quiesced leader stops beating, so
/// quiescing here would strand the down learner until an unrelated wake. The voters' gauges
/// re-settle at exactly 1 (the all-voter sibling group re-quiesces after the conn-loss wake) and
/// HOLD past a full election window, with the group still committing.
#[tokio::test(flavor = "multi_thread")]
async fn a_lagging_learner_blocks_quiescence() {
  let addrs = addrs(44_620, 3);
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  let mut metrics = Vec::new();
  // The learner's node DIALS IN (the voters' address books omit it, like a joining observer):
  // the shared one-conn-per-peer link serves the voters' outbound replication while the learner
  // lives, and after its death there is no redial — a redial to a dead address fails into the
  // conn-loss wake every attempt, which would keep waking the sibling group and turn the gauges
  // into noise rather than the eligibility signal this test pins.
  for id in 1u64..=3 {
    let peers: Vec<_> = (1u64..=2)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(id, addrs[(id - 1) as usize], peers).await;
    metrics.push(driver.engine_metrics());
    tokio::spawn(driver.run());
    handles.push(handle);
  }
  // Group 100: voters {1, 2} + node 3 pre-provisioned as the joining OBSERVER replica (its
  // bootstrap voter seed is the existing cluster's, so it cannot campaign). Group 200 is the
  // all-voter sibling on nodes 1 and 2 only.
  for id in 1u64..=2 {
    handles[(id - 1) as usize]
      .create_group(100, config(id, vec![1, 2]), id, CountSm::default(), 0)
      .await
      .expect("group admission");
  }
  handles[2]
    .create_group(
      100,
      Config::try_new_observer(3u64, vec![1, 2], ELECTION, HEARTBEAT).unwrap(),
      3,
      CountSm::default(),
      0,
    )
    .await
    .expect("the observer replica admits");
  create_group_everywhere(&handles[0..2], 200, &[1, 2]).await;
  let g100: Vec<_> = handles.iter().map(|h| h.group(100)).collect();
  let g200: Vec<_> = handles[0..2].iter().map(|h| h.group(200)).collect();
  assert_eq!(submit_anywhere(&g100[0..2], b"seed").await, 1);
  assert_eq!(submit_anywhere(&g200, b"seed").await, 1);

  // Wire in learner 3 via a committed conf change through the leader.
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "no committed learner add in time"
    );
    let at = find_leader(&g100[0..2], "group 100 pre-learner").await;
    let cc = ConfChange::new(ConfChangeType::AddLearnerNode, 3, Bytes::new());
    match g100[at].conf_change(cc).await {
      Ok(_) => break,
      Err(DriverError::NotLeader { .. }) => {
        tokio::time::sleep(Duration::from_millis(50)).await;
      }
      Err(e) => panic!("unexpected conf-change error: {e:?}"),
    }
  }

  // With the LIVE learner fully caught up, everything quiesces: eligibility requires the learner
  // matched, so the voters' gauges reaching 2 witness the catch-up, and the learner host's own
  // gauge is the flagged beat's follower-side round-trip.
  wait_for_quiesced(&metrics[0], 2, "node 1 both groups").await;
  wait_for_quiesced(&metrics[1], 2, "node 2 both groups").await;
  wait_for_quiesced(&metrics[2], 1, "the learner host").await;

  // The learner's node dies (the conn loss wakes every group), and the voters commit PAST the
  // dead learner's match — its Progress now reads Replicate with a stale match.
  handles[2]
    .shutdown()
    .await
    .expect("the learner driver tears down");
  assert_eq!(submit_anywhere(&g100[0..2], b"past").await, 2);

  // The all-voter sibling re-quiesces; the lagging-learner group must NOT — each voter gauge
  // settles at exactly 1 and HOLDS well past a full election window (a re-quiesce would land
  // within one), with the group still serving.
  wait_for_quiesced(&metrics[0], 1, "node 1 sibling only").await;
  wait_for_quiesced(&metrics[1], 1, "node 2 sibling only").await;
  for _ in 0..12 {
    tokio::time::sleep(HEARTBEAT).await;
    assert_eq!(
      metrics[0].quiesced_groups(),
      1,
      "node 1: the lagging-learner group must not quiesce"
    );
    assert_eq!(
      metrics[1].quiesced_groups(),
      1,
      "node 2: the lagging-learner group must not quiesce"
    );
  }
  assert_eq!(submit_anywhere(&g100[0..2], b"again").await, 3);
}

/// Group admission is validated LOUDLY: duplicate ids, a config whose node id contradicts the
/// latched host identity, and a walled failover-tier config on this monotonic-only host are all
/// typed rejections — and a removed id can be re-admitted under the same host identity.
#[tokio::test(flavor = "multi_thread")]
async fn lifecycle_admission_errors_are_typed() {
  let addr: SocketAddr = "127.0.0.1:44180".parse().unwrap();
  let (driver, handle) = bind_node::<CountSm>(1, addr, Vec::new()).await;
  tokio::spawn(driver.run());

  handle
    .create_group(100, config(1, vec![1]), 1, CountSm::default(), 0)
    .await
    .expect("first admission latches the host identity");

  // Duplicate group id.
  match handle
    .create_group(100, config(1, vec![1]), 1, CountSm::default(), 0)
    .await
  {
    Err(DriverError::Rejected { reason }) => {
      assert!(reason.contains("already exists"), "got: {reason}");
    }
    other => panic!("expected the duplicate-id rejection, got {other:?}"),
  }

  // A config whose node id contradicts the latched host identity.
  match handle
    .create_group(500, config(9, vec![9]), 1, CountSm::default(), 0)
    .await
  {
    Err(DriverError::Rejected { reason }) => {
      assert!(reason.contains("identity"), "got: {reason}");
    }
    other => panic!("expected the identity-mismatch rejection, got {other:?}"),
  }

  // A walled failover-tier config is rejected loudly on the monotonic-only multi host — the same
  // no-silent-wedge contract the single drivers enforce at bind.
  let failover = config(1, vec![1])
    .with_read_only(sailing_proto::ReadOnlyOption::LeaseGuard)
    .with_lease_duration(Duration::from_millis(200))
    .with_clock_drift_bound(Duration::from_millis(2))
    .with_bounded_clock_uncertainty(Duration::from_millis(5));
  match handle
    .create_group(600, failover, 1, CountSm::default(), 0)
    .await
  {
    Err(DriverError::Rejected { reason }) => {
      assert!(reason.contains("wall"), "got: {reason}");
    }
    other => panic!("expected the missing-wall-source rejection, got {other:?}"),
  }

  // Removal round-trips the was-hosted bool and TOMBSTONES the id: a re-create is refused with
  // the typed Retired flatten until an explicit clear_tombstone consents to re-admission.
  assert!(!handle.remove_group(999).await.expect("remove resolves"));
  assert!(handle.remove_group(100).await.expect("remove resolves"));
  match handle
    .create_group(100, config(1, vec![1]), 3, CountSm::default(), 0)
    .await
  {
    Err(DriverError::Rejected { reason }) => {
      assert!(reason.contains("tombstoned"), "got: {reason}");
    }
    other => panic!("expected the tombstoned-id rejection, got {other:?}"),
  }
  assert!(
    handle.clear_tombstone(100).await.expect("clear resolves"),
    "a tombstone existed"
  );
  handle
    .create_group(100, config(1, vec![1]), 3, CountSm::default(), 0)
    .await
    .expect("a cleared id is re-admittable under the latched identity");
  assert_eq!(
    submit_anywhere(std::slice::from_ref(&handle.group(100)), b"x").await,
    1,
    "the re-created group is fresh and functional"
  );
}

/// The embedder-driven placement flow, end to end: node 1 creates group 100 (voters {1,2}) and
/// campaigns into the void; node 2 — which does NOT host it — surfaces
/// `UnknownGroup { group: 100, from: 1 }` on its lifecycle tail; the test (playing the placement
/// brain) creates the group there; the campaigner's next election retry completes and BOTH sides
/// commit — while the co-hosted sibling group is untouched by the churn.
#[tokio::test(flavor = "multi_thread")]
async fn unknown_group_event_drives_creation() {
  let addrs = addrs(44_240, 2);
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  for id in 1u64..=2 {
    let peers: Vec<_> = (1u64..=2)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(id, addrs[(id - 1) as usize], peers).await;
    tokio::spawn(driver.run());
    handles.push(handle);
  }
  // The sibling group exists everywhere: it binds the mesh and proves cross-group isolation.
  create_group_everywhere(&handles, 900, &[1, 2]).await;
  let g900: Vec<_> = handles.iter().map(|h| h.group(900)).collect();
  assert_eq!(submit_anywhere(&g900, b"seed").await, 1);

  // Group 100 exists ONLY on node 1; its campaign solicits node 2 over the shared mesh.
  handles[0]
    .create_group(100, config(1, vec![1, 2]), 1, CountSm::default(), 0)
    .await
    .expect("group 100 admitted on node 1");
  await_lifecycle(handles[1].lifecycle(), "node 2 unknown-group", |ev| {
    matches!(
      ev,
      LifecycleEvent::UnknownGroup {
        group: 100,
        from: 1
      }
    )
  })
  .await;

  // The test IS the placement brain: create the solicited group on node 2. The campaigner's
  // next retry then finds its quorum, and the joined group commits and reads on both sides.
  handles[1]
    .create_group(100, config(2, vec![1, 2]), 2, CountSm::default(), 0)
    .await
    .expect("group 100 admitted on node 2");
  let g100: Vec<_> = handles.iter().map(|h| h.group(100)).collect();
  assert_eq!(submit_anywhere(&g100, b"joined").await, 1);
  assert_eq!(query_anywhere(&g100).await, 1);

  // The sibling group was untouched by the lifecycle churn.
  assert_eq!(submit_anywhere(&g900, b"still").await, 2);
}

/// THE stale-advisory regression: an `UnknownGroup` observation consumed AFTER the embedder
/// tombstoned the id must not resurrect the group. The lifecycle tail is asynchronous — the
/// event was captured at emission, and by consumption the embedder has already removed the id —
/// so a naive placement brain replaying it into `create_group` is exactly the implicit
/// resurrection the references forbid: the driver fails it closed (`Rejected`), the group stays
/// un-hosted, and only the deliberate two-act rejoin — `clear_tombstone`, then `create_group` —
/// re-admits the id and lets the solicited election complete.
#[tokio::test(flavor = "multi_thread")]
async fn stale_unknown_group_event_cannot_resurrect() {
  let addrs = addrs(44_300, 2);
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  for id in 1u64..=2 {
    let peers: Vec<_> = (1u64..=2)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(id, addrs[(id - 1) as usize], peers).await;
    tokio::spawn(driver.run());
    handles.push(handle);
  }
  // The sibling group binds the mesh.
  create_group_everywhere(&handles, 900, &[1, 2]).await;
  let g900: Vec<_> = handles.iter().map(|h| h.group(900)).collect();
  assert_eq!(submit_anywhere(&g900, b"seed").await, 1);

  // Group 100 exists only on node 1; its campaign solicits node 2. Wait for the UnknownGroup
  // observation to LAND on node 2's lifecycle tail — but do not consume it yet.
  handles[0]
    .create_group(100, config(1, vec![1, 2]), 1, CountSm::default(), 0)
    .await
    .expect("group 100 admitted on node 1");
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  while handles[1].lifecycle().is_empty() {
    assert!(
      std::time::Instant::now() < deadline,
      "no unknown-group event landed in time"
    );
    tokio::time::sleep(Duration::from_millis(20)).await;
  }

  // The embedder tombstones the id BEFORE consuming the tail (a placement decision racing the
  // queued advisory). Node 2 never hosted 100, so the remove reports false — and still
  // tombstones.
  assert!(
    !handles[1].remove_group(100).await.expect("remove resolves"),
    "node 2 never hosted group 100"
  );

  // NOW the naive brain consumes the stale advisory and replays it into a create: refused —
  // the tombstone fails the resurrection closed, and the group stays un-hosted.
  await_lifecycle(handles[1].lifecycle(), "the stale solicitation", |ev| {
    matches!(
      ev,
      LifecycleEvent::UnknownGroup {
        group: 100,
        from: 1
      }
    )
  })
  .await;
  match handles[1]
    .create_group(100, config(2, vec![1, 2]), 2, CountSm::default(), 0)
    .await
  {
    Err(DriverError::Rejected { reason }) => {
      assert!(reason.contains("tombstoned"), "got: {reason}");
    }
    other => panic!("expected the stale re-create to be rejected, got {other:?}"),
  }
  match handles[1].group(100).status().await {
    Err(DriverError::Rejected { .. }) => {}
    other => panic!("the group must stay un-hosted, got {other:?}"),
  }

  // The legitimate re-admission is two deliberate acts: clear the tombstone, then create — the
  // campaigner's next retry then completes the election and both sides commit.
  assert!(
    handles[1]
      .clear_tombstone(100)
      .await
      .expect("clear resolves"),
    "a tombstone existed"
  );
  handles[1]
    .create_group(100, config(2, vec![1, 2]), 2, CountSm::default(), 0)
    .await
    .expect("group 100 admitted on node 2 after the explicit clear");
  let g100: Vec<_> = handles.iter().map(|h| h.group(100)).collect();
  assert_eq!(submit_anywhere(&g100, b"joined").await, 1);
}

/// A committed remove-node conf change that drops a host from a group surfaces
/// `RemovedSelf { group }` on THAT host's lifecycle tail, and the app-driven teardown leaves
/// everything else standing: the removed node's OTHER group keeps committing, the survivors keep
/// committing the shrunk group, and the removed node's tombstone silently absorbs the stragglers
/// (no unknown-group resurrection prompt ever fires for the removed id, and no connection churn).
///
/// The removed node here is the group's LEADER, removing itself: the node driving the commit
/// applies its own removal directly. The removed-FOLLOWER path — learning the excising commit
/// from the leader's farewell heartbeat — is
/// `removed_follower_learns_via_farewell_and_tears_down`'s.
#[tokio::test(flavor = "multi_thread")]
async fn removed_self_event_and_teardown() {
  let addrs = addrs(44_260, 3);
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  for id in 1u64..=3 {
    let peers: Vec<_> = (1u64..=3)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(id, addrs[(id - 1) as usize], peers).await;
    tokio::spawn(driver.run());
    handles.push(handle);
  }
  create_group_everywhere(&handles, 100, &[1, 2, 3]).await;
  create_group_everywhere(&handles, 200, &[1, 2, 3]).await;
  let g100: Vec<_> = handles.iter().map(|h| h.group(100)).collect();
  let g200: Vec<_> = handles.iter().map(|h| h.group(200)).collect();
  assert_eq!(submit_anywhere(&g100, b"seed").await, 1);
  assert_eq!(submit_anywhere(&g200, b"seed").await, 1);

  // Group 100's leader proposes REMOVING ITSELF (v1 remove-node); retry through pre-propose
  // leadership moves.
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  let removed_at = loop {
    assert!(
      std::time::Instant::now() < deadline,
      "no committed self-removal in time"
    );
    let at = find_leader(&g100, "group 100 pre-removal").await;
    let cc = ConfChange::new(ConfChangeType::RemoveNode, at as u64 + 1, Bytes::new());
    match g100[at].conf_change(cc).await {
      Ok(_) => break at,
      Err(DriverError::NotLeader { .. }) => {
        tokio::time::sleep(Duration::from_millis(50)).await;
      }
      Err(e) => panic!("unexpected conf-change error: {e:?}"),
    }
  };

  // The removed node's lifecycle tail names the group it no longer belongs to.
  await_lifecycle(handles[removed_at].lifecycle(), "removed-self", |ev| {
    matches!(ev, LifecycleEvent::RemovedSelf { group: 100 })
  })
  .await;

  // The app decides: tear the local replica down (until now it kept running, harmlessly — the
  // committed change already excluded it from every quorum).
  assert!(
    handles[removed_at]
      .remove_group(100)
      .await
      .expect("remove resolves"),
    "the removed node hosted group 100"
  );

  // The removed node's OTHER group is untouched, and the SURVIVORS keep committing the shrunk
  // group (electing afresh, since the departed leader fell silent).
  assert_eq!(submit_anywhere(&g200, b"alive").await, 2);
  let survivors: Vec<usize> = (0..3usize).filter(|&i| i != removed_at).collect();
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  'committed: loop {
    assert!(
      std::time::Instant::now() < deadline,
      "no post-removal commit within the deadline"
    );
    for &i in &survivors {
      if let Ok(count) = g100[i].submit(Bytes::from_static(b"after")).await {
        assert_eq!(count, 2, "the shrunk group applied seed + after");
        break 'committed;
      }
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
  }

  // No resurrection prompt and no reconnect churn: the survivors' straggler frames (their
  // election-era votes and beats while their configs still named the removed node) died against
  // the tombstone silently — the removed node's lifecycle tail never solicits group 100 back.
  while let Ok(ev) = handles[removed_at].lifecycle().try_recv() {
    assert!(
      !matches!(ev, LifecycleEvent::UnknownGroup { group: 100, .. }),
      "a tombstoned group must never re-solicit placement: {ev:?}"
    );
  }
}

/// A removed FOLLOWER learns of its removal end-to-end: the leader's farewell heartbeat —
/// emitted at the apply-time fold, before the tracker swap drops the pruned peer — delivers the
/// excising commit to a follower that never drives it, so ITS lifecycle tail yields
/// `RemovedSelf` and the app can tear the replica down (the leader-self-removal path is
/// `removed_self_event_and_teardown`'s).
///
/// The 3-node group is shaped voters {1, 2} + learner 3 (B), with C the non-leader VOTER:
/// requiring C's ack for the commit quorum pins the `match >= removal` farewell arm — the
/// commit-only heartbeat to a peer that already holds the conf entry. (The 3-voter shape,
/// where the loser of the ack race is caught up by the farewell APPEND instead, is
/// `removed_voter_in_full_quorum_learns_via_farewell`'s.)
///
/// Group 100 runs pre-vote + check-quorum so the farewell is the ONLY way C can learn: without
/// them, an ignorant C (still seeing voters {A, C}) eventually campaigns at a higher term, wins
/// A's vote with its up-to-date log, and commits its own removal — learning by usurpation, not
/// by the farewell. Check-quorum's in-lease rejection plus pre-vote's non-disruptive probes
/// wall that path off, so a farewell regression turns this test into a clean timeout.
#[tokio::test(flavor = "multi_thread")]
async fn removed_follower_learns_via_farewell_and_tears_down() {
  let addrs = addrs(44_520, 3);
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  for id in 1u64..=3 {
    let peers: Vec<_> = (1u64..=3)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(id, addrs[(id - 1) as usize], peers).await;
    tokio::spawn(driver.run());
    handles.push(handle);
  }
  // Group 100 lives on all three nodes: voters {1, 2}, node 3 pre-provisioned as the joining
  // OBSERVER replica the AddLearnerNode below wires in (its bootstrap voter seed is the
  // EXISTING cluster's, so it cannot campaign). The sibling 2-node group 300 isolates the churn.
  for id in 1u64..=2 {
    handles[(id - 1) as usize]
      .create_group(
        100,
        config(id, vec![1, 2])
          .with_pre_vote(true)
          .with_check_quorum(true),
        id,
        CountSm::default(),
        0,
      )
      .await
      .expect("group admission");
  }
  handles[2]
    .create_group(
      100,
      Config::try_new_observer(3u64, vec![1, 2], ELECTION, HEARTBEAT)
        .unwrap()
        .with_pre_vote(true)
        .with_check_quorum(true),
      3,
      CountSm::default(),
      0,
    )
    .await
    .expect("the observer replica admits");
  create_group_everywhere(&handles[0..2], 300, &[1, 2]).await;
  let g100: Vec<_> = handles.iter().map(|h| h.group(100)).collect();
  let g300: Vec<_> = handles[0..2].iter().map(|h| h.group(300)).collect();
  assert_eq!(submit_anywhere(&g100, b"seed").await, 1);
  assert_eq!(submit_anywhere(&g300, b"seed").await, 1);

  // Wire in learner 3 (B) via a committed conf change through the leader.
  let voters = &g100[0..2];
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "no committed learner add in time"
    );
    let at = find_leader(voters, "group 100 pre-learner").await;
    let cc = ConfChange::new(ConfChangeType::AddLearnerNode, 3, Bytes::new());
    match g100[at].conf_change(cc).await {
      Ok(_) => break,
      Err(DriverError::NotLeader { .. }) => {
        tokio::time::sleep(Duration::from_millis(50)).await;
      }
      Err(e) => panic!("unexpected conf-change error: {e:?}"),
    }
  }

  // A (the leader) proposes removing C (the OTHER voter). C's ack is required for the commit
  // quorum {1, 2}, so at the apply-time fold C's match covers the conf entry and the farewell
  // heartbeat carries the removal commit — C, which never drives the commit, learns from it.
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  let removed_at = loop {
    assert!(
      std::time::Instant::now() < deadline,
      "no committed follower removal in time"
    );
    let at = find_leader(voters, "group 100 pre-removal").await;
    let c = 1 - at;
    let cc = ConfChange::new(ConfChangeType::RemoveNode, c as u64 + 1, Bytes::new());
    match g100[at].conf_change(cc).await {
      Ok(_) => break c,
      Err(DriverError::NotLeader { .. }) => {
        tokio::time::sleep(Duration::from_millis(50)).await;
      }
      Err(e) => panic!("unexpected conf-change error: {e:?}"),
    }
  };

  // The pruned follower's lifecycle tail names the group it no longer belongs to — reachable
  // only through the farewell (the leader prunes C in the same fused pass and never appends to
  // it again).
  await_lifecycle(handles[removed_at].lifecycle(), "removed-follower", |ev| {
    matches!(ev, LifecycleEvent::RemovedSelf { group: 100 })
  })
  .await;

  // The app tears the local replica down; the removal reports it was hosted.
  assert!(
    handles[removed_at]
      .remove_group(100)
      .await
      .expect("remove resolves"),
    "the removed follower hosted group 100"
  );

  // A + B keep the group: the shrunk single-voter group commits through A, and learner B's
  // apply stream follows it.
  let leader_at = 1 - removed_at;
  assert_eq!(
    submit_anywhere(std::slice::from_ref(&g100[leader_at]), b"after").await,
    2,
    "the shrunk group applied seed + after"
  );
  let a_applied = g100[leader_at]
    .status()
    .await
    .expect("leader status")
    .applied_index;
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  loop {
    let b_applied = g100[2]
      .status()
      .await
      .expect("learner status")
      .applied_index;
    if b_applied >= a_applied {
      break;
    }
    assert!(
      std::time::Instant::now() < deadline,
      "learner B never caught up: {b_applied:?} < {a_applied:?}"
    );
    tokio::time::sleep(Duration::from_millis(30)).await;
  }

  // The sibling group is unaffected — and its quorum NEEDS the torn-down node's driver, so a
  // commit proves C's driver survived its group-scoped teardown.
  assert_eq!(submit_anywhere(&g300, b"still").await, 2);

  // No resurrection prompt on the removed follower: the tombstone absorbs the stragglers.
  while let Ok(ev) = handles[removed_at].lifecycle().try_recv() {
    assert!(
      !matches!(ev, LifecycleEvent::UnknownGroup { group: 100, .. }),
      "a tombstoned group must never re-solicit placement: {ev:?}"
    );
  }
}

/// A removed voter in a FULL 3-voter quorum learns of its removal and the live leader survives
/// REGARDLESS of which farewell arm the ack race selects: the removal commits on whichever of
/// the two follower acks the leader processes first, so whether the pruned voter's own ack made
/// it (`match >= removal` — the commit-only farewell heartbeat) or is still in flight
/// (`match < removal` — the farewell append carrying the missing suffix) is a scheduler coin
/// flip this test deliberately leaves OPEN — it pins the outcome, not the arm. With
/// pre-vote/check-quorum at their defaults (OFF, deliberately, unlike the 2-voter+learner
/// sibling) an ignorant removed voter's election timer fires a REAL higher-term campaign that
/// deposes the live leader before the removal resolves — the churn residual the farewell closes
/// on BOTH arms alike, pinned by the leader's role AND term staying fixed from the removal
/// commit to C's `RemovedSelf` (a deposed-then-reelected leader would carry a higher term). The
/// append arm's DETERMINISTIC pins are the endpoint tests
/// (`ack_in_flight_removed_voter_learns_via_farewell_append`,
/// `never_received_removed_voter_learns_via_farewell_append`), the `confchange_remove`
/// interaction golden, and this suite's learn-from-zero sibling
/// (`never_caught_up_removed_replica_learns_from_zero_via_farewell`).
#[tokio::test(flavor = "multi_thread")]
async fn removed_voter_in_full_quorum_learns_via_farewell() {
  let addrs = addrs(44_760, 3);
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  for id in 1u64..=3 {
    let peers: Vec<_> = (1u64..=3)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(id, addrs[(id - 1) as usize], peers).await;
    tokio::spawn(driver.run());
    handles.push(handle);
  }
  // Group 100 runs the DEFAULT knobs — the farewell append alone must keep the removal
  // churn-free. The sibling group 400 isolates the churn and proves C's driver survives.
  create_group_everywhere(&handles, 100, &[1, 2, 3]).await;
  create_group_everywhere(&handles, 400, &[1, 2, 3]).await;
  let g100: Vec<_> = handles.iter().map(|h| h.group(100)).collect();
  let g400: Vec<_> = handles.iter().map(|h| h.group(400)).collect();
  assert_eq!(submit_anywhere(&g100, b"seed").await, 1);
  assert_eq!(submit_anywhere(&g400, b"seed").await, 1);

  // The leader A removes voter C (a non-leader), and A's term is pinned at the commit.
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  let (leader_at, removed_at, term_at_commit) = loop {
    assert!(
      std::time::Instant::now() < deadline,
      "no committed voter removal in time"
    );
    let at = find_leader(&g100, "group 100 pre-removal").await;
    let c = (at + 1) % 3;
    let cc = ConfChange::new(ConfChangeType::RemoveNode, c as u64 + 1, Bytes::new());
    match g100[at].conf_change(cc).await {
      Ok(_) => {
        let st = g100[at]
          .status()
          .await
          .expect("leader status at the commit");
        break (at, c, st.term);
      }
      Err(DriverError::NotLeader { .. }) => {
        tokio::time::sleep(Duration::from_millis(50)).await;
      }
      Err(e) => panic!("unexpected conf-change error: {e:?}"),
    }
  };

  // C learns from the farewell alone: RemovedSelf on ITS lifecycle tail, C never driving the
  // commit (the change was proposed, committed, and applied on A).
  await_lifecycle(handles[removed_at].lifecycle(), "removed-voter", |ev| {
    matches!(ev, LifecycleEvent::RemovedSelf { group: 100 })
  })
  .await;

  // NO leadership churn across the removal: A is still leader at the SAME term. An ignorant C
  // would have campaigned at a higher term and deposed A with its up-to-date log — exactly the
  // spurious disruption the farewell append pins away.
  let st = g100[leader_at].status().await.expect("leader status");
  assert_eq!(
    st.role,
    Role::Leader,
    "the removed voter must not depose the live leader"
  );
  assert_eq!(
    st.term, term_at_commit,
    "no term bump between the removal commit and the removed voter's RemovedSelf"
  );

  // The app tears the replica down; the survivors keep committing the shrunk group.
  assert!(
    handles[removed_at]
      .remove_group(100)
      .await
      .expect("remove resolves"),
    "the removed voter hosted group 100"
  );
  // `>=`: `submit_anywhere` is at-least-once across a `Superseded` retry, and a contested
  // startup election can double-apply the seed — the liveness claim is only "the shrunk group
  // still commits and applies".
  assert!(
    submit_anywhere(std::slice::from_ref(&g100[leader_at]), b"after").await >= 2,
    "the shrunk group applied seed + after"
  );

  // The sibling group is unaffected — its quorum includes the torn-down node's driver, so a
  // commit proves C's driver survived its group-scoped teardown.
  assert!(submit_anywhere(&g400, b"still").await >= 2);

  // No resurrection prompt on the removed voter: the tombstone absorbs the stragglers.
  while let Ok(ev) = handles[removed_at].lifecycle().try_recv() {
    assert!(
      !matches!(ev, LifecycleEvent::UnknownGroup { group: 100, .. }),
      "a tombstoned group must never re-solicit placement: {ev:?}"
    );
  }
}

/// A replica that NEVER caught up — empty log, applied 0 — learns of its removal from the
/// farewell APPEND alone: the arm is pinned STRUCTURALLY, with no ack-race coin flip (the
/// full-quorum sibling above deliberately leaves the arm open). Node 2 pre-creates group 100 as
/// a NON-MEMBER observer replica (bootstrap voter seed {1}, its own id absent): it must HOST
/// the group — an unhosted node drops the farewell frames — yet the sole voter tracks nobody,
/// so NOTHING ever replicates to it. AddLearnerNode(2) and RemoveNode(2) then commit
/// back-to-back at the one-in-flight conf-change limit, and BOTH commit on node 1's own
/// in-memory durability barrier alone (a single-voter quorum): the barrier is staged before any
/// probe to node 2 even leaves the host, so the removal fold — which prunes node 2 and emits
/// the farewell — runs strictly before node 1 can process more than one response from node 2,
/// and `match[2] = 0 < removal` at the fold under ANY scheduling. The removed replica is
/// deliberately a LEARNER, not a voter: a never-caught-up VOTER's removal cannot commit without
/// either its own ack (`match >= removal` — the farewell heartbeat arm by construction) or
/// another voter's NETWORK ack — and the latter is a cross-connection scheduling race that a
/// probe-rejection walk-back resend can win, handing the removal to the heartbeat arm. The
/// learner shape is the unique one whose removal needs no network at all. The heartbeat arm is
/// thus structurally out of reach — its commit clamp `min(commit, match)` is 0 for a zero-match
/// peer and can teach node 2 nothing — so `RemovedSelf` on node 2's tail proves the append arm
/// delivered, with node 2's applied index jumping 0 → removal in that ONE delivery.
///
/// Belt-and-suspenders guards keep the premise LOUD rather than silently degraded: group 100
/// runs `max_size_per_msg = 1`, so any stray catch-up crawls ~one entry per round trip while
/// the farewell append — bounded by the wire frame budget alone — still ships the whole missing
/// suffix in one shot; group 100's heartbeat interval is stretched to keep any beat (whose
/// lagging response would draw the catch-up pump toward node 2) practically out of the
/// ~millisecond add→remove window (a peerless single-voter leader beats nobody before the add,
/// so there is no quiesce/wake re-arm to lean on); and node 2's applied AND commit are
/// re-asserted ZERO
/// immediately before every remove attempt — a caught-up node 2 fails the run rather than
/// passing through the wrong arm. The observer never campaigns (neither its bootstrap seed nor
/// the learner role is promotable), so the pinned leader role/term is trivially stable —
/// asserted anyway. The deterministic endpoint-level pins of the same arm are
/// `ack_in_flight_removed_voter_learns_via_farewell_append`,
/// `never_received_removed_voter_learns_via_farewell_append`, and the `confchange_remove`
/// interaction golden — this is their end-to-end reactor sibling.
#[tokio::test(flavor = "multi_thread")]
async fn never_caught_up_removed_replica_learns_from_zero_via_farewell() {
  let addrs = addrs(44_840, 2);
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  for id in 1u64..=2 {
    let peers: Vec<_> = (1u64..=2)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(id, addrs[(id - 1) as usize], peers).await;
    tokio::spawn(driver.run());
    handles.push(handle);
  }
  // Group 100: sole voter {1} with the 1-byte per-append cap and the stretched beat; node 2
  // hosts the NON-MEMBER observer replica. The 2-node sibling group 500 needs BOTH drivers, so
  // a post-teardown commit proves node 2's driver survived its group-scoped teardown.
  let solo_election = Duration::from_secs(1);
  let solo_heartbeat = Duration::from_millis(900);
  handles[0]
    .create_group(
      100,
      Config::try_new(1u64, vec![1], solo_election, solo_heartbeat)
        .unwrap()
        .with_max_size_per_msg(1),
      1,
      CountSm::default(),
      0,
    )
    .await
    .expect("group admission");
  handles[1]
    .create_group(
      100,
      Config::try_new_observer(2u64, vec![1], solo_election, solo_heartbeat)
        .unwrap()
        .with_max_size_per_msg(1),
      2,
      CountSm::default(),
      0,
    )
    .await
    .expect("the observer replica admits");
  create_group_everywhere(&handles, 500, &[1, 2]).await;
  let g100: Vec<_> = handles.iter().map(|h| h.group(100)).collect();
  let g500: Vec<_> = handles.iter().map(|h| h.group(500)).collect();
  assert_eq!(submit_anywhere(&g100[0..1], b"seed").await, 1);
  assert_eq!(submit_anywhere(&g500, b"seed").await, 1);

  // The observer has learned NOTHING of the group: the sole voter tracks nobody.
  let st = g100[1].status().await.expect("the observer's status");
  assert_eq!(
    st.applied_index,
    Index::ZERO,
    "the observer must start with an empty apply stream"
  );
  assert_eq!(
    st.commit_index,
    Index::ZERO,
    "the observer must start with an empty commit"
  );
  assert!(
    !st.conf_state.voters().contains(&2),
    "the observer's bootstrap seed must not name it"
  );

  // AddLearnerNode(2), retried through the pre-election window (node 1 is the only possible
  // leader of its single-voter group).
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "no committed learner add in time"
    );
    let cc = ConfChange::new(ConfChangeType::AddLearnerNode, 2, Bytes::new());
    match g100[0].conf_change(cc).await {
      Ok(_) => break,
      Err(DriverError::NotLeader { .. }) => {
        tokio::time::sleep(Duration::from_millis(50)).await;
      }
      Err(e) => panic!("unexpected conf-change error: {e:?}"),
    }
  }
  // RemoveNode(2) IMMEDIATELY: the add resolved at ITS apply, the earliest instant the
  // one-in-flight gate admits the remove. The premise is re-asserted before EVERY attempt:
  // node 2 still holds NOTHING, so its match at the removal fold can only be 0 — the append
  // arm. A trip here means the shape's assumption broke (something caught node 2 up first);
  // fail loudly, never pass through the heartbeat arm.
  let (removal_index, term_at_commit) = loop {
    assert!(
      std::time::Instant::now() < deadline,
      "no committed learner removal in time"
    );
    let st = g100[1].status().await.expect("the observer's status");
    assert_eq!(
      st.applied_index,
      Index::ZERO,
      "the premise broke: node 2 caught up before the removal was proposed"
    );
    assert_eq!(
      st.commit_index,
      Index::ZERO,
      "the premise broke: node 2 caught up before the removal was proposed"
    );
    let cc = ConfChange::new(ConfChangeType::RemoveNode, 2, Bytes::new());
    match g100[0].conf_change(cc).await {
      Ok(idx) => {
        let st = g100[0].status().await.expect("leader status at the commit");
        break (idx, st.term);
      }
      Err(DriverError::NotLeader { .. }) => {
        tokio::time::sleep(Duration::from_millis(50)).await;
      }
      Err(e) => panic!("unexpected conf-change error: {e:?}"),
    }
  };

  // Node 2 learns FROM ZERO: `RemovedSelf` on ITS lifecycle tail, without ever driving or
  // holding a commit beforehand...
  await_lifecycle(handles[1].lifecycle(), "removed-from-zero", |ev| {
    matches!(ev, LifecycleEvent::RemovedSelf { group: 100 })
  })
  .await;
  // ...with its applied index carried from the asserted zero past its own removal in that one
  // farewell delivery — the learn-from-zero signature only the append arm can produce (a
  // heartbeat's commit clamps to the proven match, 0).
  let st = g100[1]
    .status()
    .await
    .expect("the removed observer's status");
  assert!(
    st.applied_index >= removal_index,
    "the farewell append must carry node 2 from zero past its removal: {:?} < {removal_index:?}",
    st.applied_index
  );

  // NO leadership churn across the removal: the leader holds its role at the SAME term.
  let st = g100[0].status().await.expect("leader status");
  assert_eq!(
    st.role,
    Role::Leader,
    "the removed replica must not depose the live leader"
  );
  assert_eq!(
    st.term, term_at_commit,
    "no term bump between the removal commit and the removed replica's RemovedSelf"
  );

  // The app tears the replica down; the survivor keeps committing the shrunk group.
  assert!(
    handles[1].remove_group(100).await.expect("remove resolves"),
    "the removed replica hosted group 100"
  );
  assert!(
    submit_anywhere(&g100[0..1], b"after").await >= 2,
    "the shrunk group applied seed + after"
  );

  // The sibling group is untouched — and its quorum NEEDS node 2's driver, so a commit proves
  // the driver survived the group-scoped teardown.
  assert!(submit_anywhere(&g500, b"still").await >= 2);

  // No resurrection prompt on the removed observer: the tombstone absorbs the stragglers.
  while let Ok(ev) = handles[1].lifecycle().try_recv() {
    assert!(
      !matches!(ev, LifecycleEvent::UnknownGroup { group: 100, .. }),
      "a tombstoned group must never re-solicit placement: {ev:?}"
    );
  }
}
/// Tombstone-then-recreate at the driver level: one follower of a LIVE 3-node group de-hosts its
/// replica (the unaware leader keeps beating; the tombstone absorbs the stragglers with no
/// closes — the shared mesh and the sibling group stay clean), then re-admits the SAME id
/// through the explicit clear-then-create rejoin, and the group keeps committing throughout.
///
/// The group's liveness deliberately rides the REMAINING majority, not the re-created replica's
/// catch-up: a fresh-log replica behind the leader's STALE positive `match` cannot be walked
/// back by the reject path (its rejects echo indexes at or below the old match and are dropped
/// as stale, and the `match + 1` resend floor never reaches its empty log), so full rejoin is
/// the restore/snapshot path's job — this test pins admission + no wedge for everyone else.
#[tokio::test(flavor = "multi_thread")]
async fn tombstoned_id_recreates_cleanly() {
  let addrs = addrs(44_280, 3);
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  for id in 1u64..=3 {
    let peers: Vec<_> = (1u64..=3)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(id, addrs[(id - 1) as usize], peers).await;
    tokio::spawn(driver.run());
    handles.push(handle);
  }
  create_group_everywhere(&handles, 100, &[1, 2, 3]).await;
  create_group_everywhere(&handles, 900, &[1, 2, 3]).await;
  let g100: Vec<_> = handles.iter().map(|h| h.group(100)).collect();
  let g900: Vec<_> = handles.iter().map(|h| h.group(900)).collect();
  assert_eq!(submit_anywhere(&g100, b"seed").await, 1);
  assert_eq!(submit_anywhere(&g900, b"seed").await, 1);

  // ONE follower de-hosts its replica of the live group; the majority keeps the group up.
  let leader_at = find_leader(&g100, "group 100").await;
  let follower_at = (0..3usize).find(|&i| i != leader_at).unwrap();
  assert!(
    handles[follower_at]
      .remove_group(100)
      .await
      .expect("remove resolves")
  );

  // Several heartbeat intervals of straggler beats die against the tombstone; the shared mesh
  // stays healthy — the sibling group keeps committing through EVERY node, and the de-hosted
  // group keeps committing through its remaining majority.
  tokio::time::sleep(HEARTBEAT * 4).await;
  assert_eq!(submit_anywhere(&g900, b"mesh-alive").await, 2);
  assert_eq!(submit_anywhere(&g100, b"majority").await, 2);

  // Re-admission is the deliberate two-act rejoin: clear the tombstone (the explicit consent),
  // then re-create the SAME id (fresh storage, as a driver provisions). The admission is clean
  // and the group keeps committing — no wedge anywhere.
  assert!(
    handles[follower_at]
      .clear_tombstone(100)
      .await
      .expect("clear resolves"),
    "a tombstone existed"
  );
  handles[follower_at]
    .create_group(
      100,
      config(follower_at as u64 + 1, vec![1, 2, 3]),
      9,
      CountSm::default(),
      0,
    )
    .await
    .expect("the cleared id re-admits");
  assert_eq!(submit_anywhere(&g100, b"rejoined").await, 3);
  assert_eq!(submit_anywhere(&g900, b"still").await, 3);
}

/// Bind a 2-node mesh and return its handles (the read-mode suites' shared preamble).
async fn bind_pair(base_port: u16) -> Vec<MultiHandle<u64, u64, CountSm>> {
  let addrs = addrs(base_port, 2);
  let mut handles = Vec::new();
  for id in 1u64..=2 {
    let peers: Vec<_> = (1u64..=2)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(id, addrs[(id - 1) as usize], peers).await;
    tokio::spawn(driver.run());
    handles.push(handle);
  }
  handles
}

/// Poll every replica of `groups` until each reports `want` as its active read mode.
async fn wait_for_mode(
  groups: &[GroupHandle<u64, u64, CountSm>],
  want: ReadOnlyOption,
  what: &str,
) {
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  for g in groups {
    loop {
      let mode = g.status().await.expect("status").active_read_mode;
      if mode == want {
        break;
      }
      assert!(
        std::time::Instant::now() < deadline,
        "{what}: active mode stuck at {mode:?} (want {want:?})"
      );
      tokio::time::sleep(Duration::from_millis(30)).await;
    }
  }
}

/// Per-group read modes are heterogeneous on one multi host: group 100 runs `Safe` and group 200
/// runs `LeaseGuard` on the SAME host pair — the v1 monotonic-only host admits the plain
/// LeaseGuard tier (its lease gate reads each node's own monotonic clock; only the FAILOVER tier,
/// `bounded_clock_uncertainty`, is walled at admission) — and each group serves linearizable
/// reads under ITS mode with the counts strictly isolated: the Safe group confirms on the
/// read-index round while the LeaseGuard group serves lease-fresh reads locally (degrading to
/// the same safe round only when the lease is stale — either way the value is correct).
///
/// The PATH is then pinned behaviorally, not just the configured modes (matching modes + counts
/// would also pass if every read silently rode the Safe round): with both leaders on one node
/// and the peer KILLED, a lease-fresh LeaseGuard read still completes — it serves from the local
/// commit inside the lease window, no quorum round — while the Safe group's read on the very
/// same partitioned leader parks on its unreachable read-index quorum.
#[tokio::test(flavor = "multi_thread")]
async fn co_hosted_groups_serve_heterogeneous_read_modes() {
  let handles = bind_pair(44_540).await;
  for id in 1u64..=2 {
    handles[(id - 1) as usize]
      .create_group(100, config(id, vec![1, 2]), id, CountSm::default(), 0)
      .await
      .expect("the Safe group admits");
    // The wider election window admits a lease window roomy enough for the partition pin below
    // (validation requires the commit-wait window, just past Δ, to fit under the election
    // timeout), so the post-kill read lands comfortably inside the anchor's Δ.
    handles[(id - 1) as usize]
      .create_group(
        200,
        Config::try_new(id, vec![1, 2], Duration::from_millis(1200), HEARTBEAT)
          .unwrap()
          .with_read_only(ReadOnlyOption::LeaseGuard)
          .with_lease_duration(Duration::from_millis(600))
          .with_clock_drift_bound(Duration::from_millis(2)),
        id,
        CountSm::default(),
        0,
      )
      .await
      .expect("the plain LeaseGuard tier admits on the monotonic-only host");
  }
  let g100: Vec<_> = handles.iter().map(|h| h.group(100)).collect();
  let g200: Vec<_> = handles.iter().map(|h| h.group(200)).collect();

  // Interleaved commits: each group's apply stream counts only its own.
  assert_eq!(submit_anywhere(&g100, b"s1").await, 1);
  assert_eq!(submit_anywhere(&g200, b"l1").await, 1);
  assert_eq!(submit_anywhere(&g100, b"s2").await, 2);

  // Every replica reports its group's configured mode — per-group, not per-host.
  wait_for_mode(&g100, ReadOnlyOption::Safe, "group 100 mode").await;
  wait_for_mode(&g200, ReadOnlyOption::LeaseGuard, "group 200 mode").await;

  // Both groups serve reads under their own modes, independently and correctly. The LeaseGuard
  // read lands inside the lease window of the submit above (lease-fresh serve) on a quiet run.
  assert_eq!(query_anywhere(&g100).await, 2, "the Safe group's reads");
  assert_eq!(
    query_anywhere(&g200).await,
    1,
    "the LeaseGuard group's reads"
  );

  // And again after further interleaving — the modes keep serving side by side.
  assert_eq!(submit_anywhere(&g200, b"l2").await, 2);
  assert_eq!(query_anywhere(&g200).await, 2);
  assert_eq!(query_anywhere(&g100).await, 2);

  // ---- The read-PATH pin. ----
  // Co-locate both leaders: move the SAFE group's leadership onto the LeaseGuard leader's node
  // (never the lease group's — a forced handoff would put its fresh leader into the commit-wait
  // and disable its lease reads for the term).
  let lease_at = find_leader(&g200, "group 200 lease leader").await;
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "the Safe group's leadership never co-located"
    );
    let at = find_leader(&g100, "group 100 leader").await;
    if at == lease_at {
      break;
    }
    match g100[at].transfer_leader(lease_at as u64 + 1).await {
      Ok(()) | Err(DriverError::NotLeader { .. }) => {}
      Err(e) => panic!("unexpected transfer error: {e:?}"),
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
  }
  // A commit through each group: the Safe group proves its relocated leader commits (a
  // current-term anchor for its reads), and the LeaseGuard commit re-anchors the lease window
  // right before the partition.
  assert_eq!(submit_anywhere(&g100, b"s3").await, 3);
  assert_eq!(submit_anywhere(&g200, b"l3").await, 3);

  // Sever the leaders from their peer: the quorum is now unreachable for BOTH groups.
  let dead = 1 - lease_at;
  handles[dead].shutdown().await.expect("the peer tears down");

  // LeaseGuard serves the lease-fresh read LOCALLY — no quorum round to park on, so completion
  // itself is the path witness (a read degraded onto the Safe round could never confirm here).
  assert_eq!(
    g200[lease_at]
      .query(|sm: &CountSm| sm.count())
      .await
      .expect("the lease-fresh read serves without the peer"),
    3,
    "the LeaseGuard read serves from the local commit"
  );

  // The Safe read on the SAME partitioned leader parks on its read-index heartbeat round: no
  // quorum ack can arrive, so it must never serve a value inside the window.
  let parked = tokio::time::timeout(
    Duration::from_millis(500),
    g100[lease_at].query(|sm: &CountSm| sm.count()),
  )
  .await;
  assert!(
    !matches!(parked, Ok(Ok(_))),
    "a Safe read must park on the unreachable quorum, got {parked:?}"
  );
}

/// A committed `SetReadMode` migrates exactly ITS group: group 100 migrates Safe -> LeaseBased
/// (apply-time, surfacing as the group-stamped `ReadModeChanged` on the shared events tail) while
/// co-hosted group 200's active mode stays `Safe` on every replica, and both groups keep
/// committing and serving reads through the migration.
///
/// The post-migration read PATH is then pinned behaviorally (mode + count assertions alone would
/// also pass if the migrated group's reads silently stayed on the Safe round): with both leaders
/// on one node and the peer KILLED, the migrated group's read still completes inside the
/// check-quorum lease window — LeaseBased serves from the local commit, no per-read quorum round
/// — while the still-Safe sibling's read on the same partitioned leader never serves.
#[tokio::test(flavor = "multi_thread")]
async fn set_read_mode_migrates_one_group_only() {
  let handles = bind_pair(44_580).await;
  // Both groups start Safe; check_quorum is the LeaseBased migration's validity gate. The wider
  // election window sizes the check-quorum lease residual the partition pin below serves inside
  // (the lease outlives the severed peer by up to `min support ≈ election` from its last
  // heartbeat round, and the leader's own check-quorum step-down lands on the same scale).
  for gid in [100u64, 200] {
    for id in 1u64..=2 {
      handles[(id - 1) as usize]
        .create_group(
          gid,
          Config::try_new(id, vec![1, 2], Duration::from_millis(600), HEARTBEAT)
            .unwrap()
            .with_check_quorum(true),
          id * 10 + gid,
          CountSm::default(),
          0,
        )
        .await
        .expect("group admission");
    }
  }
  let g100: Vec<_> = handles.iter().map(|h| h.group(100)).collect();
  let g200: Vec<_> = handles.iter().map(|h| h.group(200)).collect();
  assert_eq!(submit_anywhere(&g100, b"seed").await, 1);
  assert_eq!(submit_anywhere(&g200, b"seed").await, 1);

  // Migrate ONLY group 100 (through its leader, wherever it is).
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  let (proposed, leader_at) = loop {
    assert!(
      std::time::Instant::now() < deadline,
      "never became leader to migrate"
    );
    let at = find_leader(&g100, "group 100 pre-migration").await;
    match g100[at].set_read_mode(ReadOnlyOption::LeaseBased).await {
      Ok(index) => break (index, at),
      Err(DriverError::NotLeader { .. }) => {
        tokio::time::sleep(Duration::from_millis(50)).await;
      }
      Err(e) => panic!("unexpected set_read_mode error: {e:?}"),
    }
  };

  // The migration takes effect APPLY-TIME: the proposer host's shared events tail surfaces the
  // group-stamped ReadModeChanged for group 100 — and for group 100 only.
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  let changed = loop {
    let mut seen = None;
    while let Ok((gid, ev)) = handles[leader_at].events().try_recv() {
      if let Event::ReadModeChanged(rmc) = ev {
        assert_eq!(gid, 100, "only the migrated group changes mode");
        seen = Some(rmc);
      }
    }
    if let Some(rmc) = seen {
      break rmc;
    }
    assert!(
      std::time::Instant::now() < deadline,
      "the read-mode migration never applied"
    );
    tokio::time::sleep(Duration::from_millis(30)).await;
  };
  assert_eq!(changed.mode(), ReadOnlyOption::LeaseBased);
  assert!(
    changed.index() >= proposed,
    "the migration applied at (or after) the proposed index"
  );

  // Group 100's replicas all migrate; co-hosted group 200's stay Safe on the same hosts.
  wait_for_mode(
    &g100,
    ReadOnlyOption::LeaseBased,
    "group 100 post-migration",
  )
  .await;
  wait_for_mode(&g200, ReadOnlyOption::Safe, "group 200 untouched").await;

  // Both groups keep committing and serving reads after the one-group migration.
  assert_eq!(submit_anywhere(&g100, b"after").await, 2);
  assert_eq!(submit_anywhere(&g200, b"after").await, 2);
  assert_eq!(query_anywhere(&g100).await, 2);
  assert_eq!(query_anywhere(&g200).await, 2);

  // ---- The post-migration read-PATH pin. ----
  // Co-locate both leaders on the MIGRATED group's leader node (transfer the Safe sibling, never
  // the lease group — `forced_handoff_this_term` disables lease reads on a handed-off leader).
  let lease_at = find_leader(&g100, "group 100 post-migration leader").await;
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "the Safe sibling's leadership never co-located"
    );
    let at = find_leader(&g200, "group 200 leader").await;
    if at == lease_at {
      break;
    }
    match g200[at].transfer_leader(lease_at as u64 + 1).await {
      Ok(()) | Err(DriverError::NotLeader { .. }) => {}
      Err(e) => panic!("unexpected transfer error: {e:?}"),
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
  }
  // A commit through each group: the sibling proves its relocated leader commits, and the
  // migrated group's commit gives its LeaseBased reads a current-term anchor right before the
  // partition (the check-quorum lease itself keeps renewing on every heartbeat round).
  assert_eq!(submit_anywhere(&g200, b"pin").await, 3);
  assert_eq!(submit_anywhere(&g100, b"pin").await, 3);

  // Sever the leaders from their peer, then read INSIDE the lease residual: LeaseBased serves
  // from the local commit with no per-read quorum round — completion is the path witness (a
  // read still on the Safe round could never confirm without the peer).
  let dead = 1 - lease_at;
  handles[dead].shutdown().await.expect("the peer tears down");
  assert_eq!(
    g100[lease_at]
      .query(|sm: &CountSm| sm.count())
      .await
      .expect("the LeaseBased read serves without the peer"),
    3,
    "the migrated group's read serves from the local commit"
  );

  // The Safe sibling's read on the SAME partitioned leader must never serve a value: it parks on
  // its unreachable read-index quorum (and past the check-quorum window it would fail, not
  // serve).
  let parked = tokio::time::timeout(
    Duration::from_millis(500),
    g200[lease_at].query(|sm: &CountSm| sm.count()),
  )
  .await;
  assert!(
    !matches!(parked, Ok(Ok(_))),
    "a Safe read must not serve on the unreachable quorum, got {parked:?}"
  );
}

/// THE hands-free materialization flow: node 2 registers a group FACTORY recognizing group 100
/// and never calls `create_group` for it; node 1 creates 100 (voters {1,2}) and campaigns; node
/// 2's driver materializes the replica INSIDE the crank that polled the solicitation and the
/// campaigner's retry completes the election — both sides commit and read. The consumed signal
/// never reaches node 2's lifecycle tail, and the manually-created sibling group is untouched:
/// factory-admitted and command-admitted groups host side by side.
#[tokio::test(flavor = "multi_thread")]
async fn factory_materializes_solicited_group_hands_free() {
  let addrs = addrs(44_660, 2);
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  for id in 1u64..=2 {
    let peers: Vec<_> = (1u64..=2)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(id, addrs[(id - 1) as usize], peers).await;
    if id == 2 {
      // The factory IS the embedder's catalog check, on both legs: the group id against the
      // catalog AND the solicitor against the group's replica set (the driver refuses a
      // blueprint that fails the second leg anyway — checking it here declines instead of
      // burning a doomed materialization). The state machine lives in the separate build
      // phase, which the driver invokes only after admitting the blueprint. Group 100 is a
      // day-0 BOOTSTRAPPED id (created explicitly on node 1), so the blueprint keeps the
      // full-voter shape — the observer rule binds fork-born ids only.
      let driver = driver.with_group_factory(factory_fn(
        |group: &u64, from: &u64| {
          (*group == 100 && [1u64, 2].contains(from))
            .then(|| GroupBlueprint::new(config(2, vec![1, 2]), 2))
        },
        |_group: &u64| Some(CountSm::default()),
      ));
      tokio::spawn(driver.run());
    } else {
      tokio::spawn(driver.run());
    }
    handles.push(handle);
  }
  // The sibling group is created manually everywhere: it binds the mesh and pins that a
  // factory-bearing host still admits ordinary lifecycle-command groups.
  create_group_everywhere(&handles, 900, &[1, 2]).await;
  let g900: Vec<_> = handles.iter().map(|h| h.group(900)).collect();
  assert_eq!(submit_anywhere(&g900, b"seed").await, 1);

  // Group 100 exists only on node 1; its campaign solicits node 2, whose factory materializes
  // the replica hands-free — no create_group is ever issued for 100 on node 2.
  handles[0]
    .create_group(100, config(1, vec![1, 2]), 1, CountSm::default(), 0)
    .await
    .expect("group 100 admitted on node 1");
  let g100: Vec<_> = handles.iter().map(|h| h.group(100)).collect();
  assert_eq!(submit_anywhere(&g100, b"joined").await, 1);
  assert_eq!(query_anywhere(&g100).await, 1);
  assert!(
    handles[1].group(100).status().await.is_ok(),
    "node 2 hosts the materialized replica"
  );

  // The factory CONSUMED the solicitation: node 2's lifecycle tail never surfaced group 100
  // (on a factory-less driver the same solicitation lands there — see
  // `unknown_group_event_drives_creation`).
  while let Ok(ev) = handles[1].lifecycle().try_recv() {
    assert!(
      !matches!(ev, LifecycleEvent::UnknownGroup { group: 100, .. }),
      "a factory-consumed signal must not reach the tail: {ev:?}"
    );
  }

  // The manually-created sibling is unaffected by the factory churn.
  assert_eq!(submit_anywhere(&g900, b"still").await, 2);
}

/// A factory DECLINE falls through byte for byte: node 2's factory recognizes only group 555,
/// so node 1's group-100 solicitation is declined — the `UnknownGroup` event surfaces on the
/// lifecycle tail exactly as on a factory-less driver, nothing materializes, and the embedder's
/// manual `create_group` (the placement brain overruling its own catalog) still completes the
/// join.
#[tokio::test(flavor = "multi_thread")]
async fn factory_decline_falls_through_to_lifecycle_tail() {
  let addrs = addrs(44_680, 2);
  let consulted = Arc::new(AtomicUsize::new(0));
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  for id in 1u64..=2 {
    let peers: Vec<_> = (1u64..=2)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(id, addrs[(id - 1) as usize], peers).await;
    if id == 2 {
      let consulted = consulted.clone();
      let driver = driver.with_group_factory(factory_fn(
        move |group: &u64, from: &u64| {
          if *group == 100 {
            consulted.fetch_add(1, Ordering::SeqCst);
          }
          (*group == 555 && [1u64, 2].contains(from))
            .then(|| GroupBlueprint::new(config(2, vec![1, 2]), 2))
        },
        |_group: &u64| Some(CountSm::default()),
      ));
      tokio::spawn(driver.run());
    } else {
      tokio::spawn(driver.run());
    }
    handles.push(handle);
  }
  // The sibling group binds the mesh.
  create_group_everywhere(&handles, 900, &[1, 2]).await;
  let g900: Vec<_> = handles.iter().map(|h| h.group(900)).collect();
  assert_eq!(submit_anywhere(&g900, b"seed").await, 1);

  // Group 100 exists only on node 1; node 2's factory declines it, so the signal falls through
  // to the lifecycle tail — the factory-less flow, with the factory consulted first.
  handles[0]
    .create_group(100, config(1, vec![1, 2]), 1, CountSm::default(), 0)
    .await
    .expect("group 100 admitted on node 1");
  await_lifecycle(handles[1].lifecycle(), "the declined solicitation", |ev| {
    matches!(
      ev,
      LifecycleEvent::UnknownGroup {
        group: 100,
        from: 1
      }
    )
  })
  .await;
  assert!(
    consulted.load(Ordering::SeqCst) >= 1,
    "the factory was consulted before the tail"
  );
  // A decline materializes nothing.
  match handles[1].group(100).status().await {
    Err(DriverError::Rejected { .. }) => {}
    other => panic!("a declined group must stay un-hosted, got {other:?}"),
  }

  // The placement brain can still place the declined group manually — exactly today's flow.
  handles[1]
    .create_group(100, config(2, vec![1, 2]), 2, CountSm::default(), 0)
    .await
    .expect("group 100 admitted on node 2");
  let g100: Vec<_> = handles.iter().map(|h| h.group(100)).collect();
  assert_eq!(submit_anywhere(&g100, b"joined").await, 1);
}

/// The driver's sender-membership gate on factory blueprints, end to end: node 2's factory
/// RECOGNIZES group 100 but its catalog view seeds voters {2, 3} — the actual solicitor (node
/// 1) is absent — so the driver REFUSES the returned blueprint. Nothing materializes on node 2,
/// the solicitation surfaces on the lifecycle tail as unplaceable (the embedder's manual path),
/// and the co-hosted sibling group keeps serving. Correcting the catalog view to name the
/// solicitor lets the very next solicitation materialize through the SAME factory — the gate
/// refused the blueprint's shape, not the flow. The build (resource) phase is the
/// resource-exhaustion pin: through the whole refused phase the `from`-blind factory's build
/// closure NEVER ran — zero state machines constructed for the unauthorized-shaped
/// solicitations, while the materialize counter proves the cheap phase did run — and the
/// corrected join builds exactly one.
#[tokio::test(flavor = "multi_thread")]
async fn refused_blueprint_not_naming_solicitor_falls_to_lifecycle_tail() {
  let addrs = addrs(44_720, 2);
  let corrected = Arc::new(AtomicBool::new(false));
  let blueprinted = Arc::new(AtomicUsize::new(0));
  let built = Arc::new(AtomicUsize::new(0));
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  for id in 1u64..=2 {
    let peers: Vec<_> = (1u64..=2)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(id, addrs[(id - 1) as usize], peers).await;
    if id == 2 {
      let corrected = corrected.clone();
      let blueprinted = blueprinted.clone();
      let built = built.clone();
      // Deliberately `from`-blind: the point is the DRIVER's gate, not a factory-side decline.
      let driver = driver.with_group_factory(factory_fn(
        move |group: &u64, _from: &u64| {
          (*group == 100).then(|| {
            blueprinted.fetch_add(1, Ordering::SeqCst);
            // The stale catalog view first: the group is real, but the seed voters name a
            // would-be peer (node 3) instead of the actual solicitor (node 1).
            let voters = if corrected.load(Ordering::SeqCst) {
              vec![1, 2]
            } else {
              vec![2, 3]
            };
            GroupBlueprint::new(config(2, voters), 2)
          })
        },
        move |_group: &u64| {
          built.fetch_add(1, Ordering::SeqCst);
          Some(CountSm::default())
        },
      ));
      tokio::spawn(driver.run());
    } else {
      tokio::spawn(driver.run());
    }
    handles.push(handle);
  }
  // The sibling group binds the mesh (and pins driver survival through the refusals below).
  create_group_everywhere(&handles, 900, &[1, 2]).await;
  let g900: Vec<_> = handles.iter().map(|h| h.group(900)).collect();
  assert_eq!(submit_anywhere(&g900, b"seed").await, 1);

  // Group 100 exists only on node 1; its campaign solicits node 2. The factory RETURNS a
  // blueprint and the driver refuses it — the signal falls through to the lifecycle tail
  // exactly like a decline.
  handles[0]
    .create_group(100, config(1, vec![1, 2]), 1, CountSm::default(), 0)
    .await
    .expect("group 100 admitted on node 1");
  await_lifecycle(handles[1].lifecycle(), "the refused blueprint", |ev| {
    matches!(
      ev,
      LifecycleEvent::UnknownGroup {
        group: 100,
        from: 1
      }
    )
  })
  .await;
  assert!(
    blueprinted.load(Ordering::SeqCst) >= 1,
    "the factory returned a blueprint — the DRIVER refused it"
  );
  assert_eq!(
    built.load(Ordering::SeqCst),
    0,
    "a refused blueprint must never reach the build phase — no state machine constructed"
  );
  // The refusal materialized nothing.
  match handles[1].group(100).status().await {
    Err(DriverError::Rejected { .. }) => {}
    other => panic!("a refused blueprint must leave the group un-hosted, got {other:?}"),
  }
  // The co-hosted sibling rides out the refusal churn.
  assert_eq!(submit_anywhere(&g900, b"still").await, 2);
  // Even across the whole refused phase — every retry re-ran the cheap phase, none the build.
  assert_eq!(
    built.load(Ordering::SeqCst),
    0,
    "the unauthorized-shaped solicitations never cost a state machine"
  );

  // Correct the catalog view: the next solicitation's blueprint names the solicitor, and the
  // same factory + gate materialize hands-free.
  corrected.store(true, Ordering::SeqCst);
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  while handles[1].group(100).status().await.is_err() {
    assert!(
      std::time::Instant::now() < deadline,
      "the corrected blueprint never materialized"
    );
    tokio::time::sleep(Duration::from_millis(30)).await;
  }
  assert_eq!(
    built.load(Ordering::SeqCst),
    1,
    "the corrected, admitted solicitation built exactly one state machine"
  );
  let g100: Vec<_> = handles.iter().map(|h| h.group(100)).collect();
  assert_eq!(submit_anywhere(&g100, b"joined").await, 1);
}

/// The factory never overrides a tombstone, end to end. Node 2 hosts group 100 THROUGH its
/// factory, then de-hosts it (`remove_group` → tombstoned) while node 1 keeps soliciting —
/// pre-vote + check-quorum make the solicitation stream unconditional (whichever side led, the
/// survivor ends up a follower pre-campaigning into the void forever). The tombstoned id is
/// never enqueued: the factory is NOT consulted again, nothing surfaces on the lifecycle tail,
/// and the group stays dead. `clear_tombstone` is the explicit re-admission consent, after
/// which the very next solicitation re-materializes through the factory and the group commits
/// again — every rejoin rides a FRESH election (the deposed side can never keep leading without
/// its peer), so the recreated replica's catch-up starts from reset leader-side progress, never
/// a stale match.
#[tokio::test(flavor = "multi_thread")]
async fn factory_never_overrides_a_tombstone() {
  let addrs = addrs(44_700, 2);
  let blueprinted = Arc::new(AtomicUsize::new(0));
  let built = Arc::new(AtomicUsize::new(0));
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  for id in 1u64..=2 {
    let peers: Vec<_> = (1u64..=2)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(id, addrs[(id - 1) as usize], peers).await;
    if id == 2 {
      let blueprinted = blueprinted.clone();
      let built = built.clone();
      // Group 100 is a day-0 BOOTSTRAPPED id (created explicitly on node 1), so the blueprint
      // keeps the full-voter shape — the observer rule binds fork-born ids only.
      let driver = driver.with_group_factory(factory_fn(
        move |group: &u64, from: &u64| {
          (*group == 100 && [1u64, 2].contains(from)).then(|| {
            blueprinted.fetch_add(1, Ordering::SeqCst);
            GroupBlueprint::new(
              config(2, vec![1, 2])
                .with_pre_vote(true)
                .with_check_quorum(true),
              2,
            )
          })
        },
        move |_group: &u64| {
          built.fetch_add(1, Ordering::SeqCst);
          Some(CountSm::default())
        },
      ));
      tokio::spawn(driver.run());
    } else {
      tokio::spawn(driver.run());
    }
    handles.push(handle);
  }
  // The sibling group binds the mesh (and proves it stays clean through the churn below).
  create_group_everywhere(&handles, 900, &[1, 2]).await;
  let g900: Vec<_> = handles.iter().map(|h| h.group(900)).collect();
  assert_eq!(submit_anywhere(&g900, b"seed").await, 1);

  // Act 1: the hands-free join (exactly one materialization — the signal is deduped until
  // polled, and once hosted the group's frames route normally).
  handles[0]
    .create_group(
      100,
      config(1, vec![1, 2])
        .with_pre_vote(true)
        .with_check_quorum(true),
      1,
      CountSm::default(),
      0,
    )
    .await
    .expect("group 100 admitted on node 1");
  let g100: Vec<_> = handles.iter().map(|h| h.group(100)).collect();
  assert_eq!(submit_anywhere(&g100, b"joined").await, 1);
  assert_eq!(blueprinted.load(Ordering::SeqCst), 1, "one clean join");
  assert_eq!(
    built.load(Ordering::SeqCst),
    1,
    "exactly one state machine for the one accepted join"
  );

  // Act 2: node 2 de-hosts its replica — the id is tombstoned. Node 1 keeps soliciting (its
  // leader steps down by check-quorum without the peer, then pre-campaigns every election
  // timeout), and every solicitation dies against the tombstone BEFORE the factory: never
  // enqueued, never consulted, nothing on the tail.
  assert!(
    handles[1].remove_group(100).await.expect("remove resolves"),
    "node 2 hosted the materialized replica"
  );
  tokio::time::sleep(ELECTION * 4).await;
  assert_eq!(
    blueprinted.load(Ordering::SeqCst),
    1,
    "a tombstoned id never reaches the factory"
  );
  assert_eq!(
    built.load(Ordering::SeqCst),
    1,
    "a tombstoned id never costs a state machine"
  );
  match handles[1].group(100).status().await {
    Err(DriverError::Rejected { .. }) => {}
    other => panic!("the tombstoned group must stay dead, got {other:?}"),
  }
  while let Ok(ev) = handles[1].lifecycle().try_recv() {
    assert!(
      !matches!(ev, LifecycleEvent::UnknownGroup { group: 100, .. }),
      "a tombstoned id must never re-solicit placement: {ev:?}"
    );
  }

  // Act 3: the explicit two-act rejoin — clear the tombstone, and the NEXT solicitation
  // re-materializes through the factory (no manual create).
  assert!(
    handles[1]
      .clear_tombstone(100)
      .await
      .expect("clear resolves"),
    "a tombstone existed"
  );
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  while handles[1].group(100).status().await.is_err() {
    assert!(
      std::time::Instant::now() < deadline,
      "the cleared id never re-materialized through the factory"
    );
    tokio::time::sleep(Duration::from_millis(30)).await;
  }
  assert_eq!(
    blueprinted.load(Ordering::SeqCst),
    2,
    "re-materialization went through the factory"
  );
  assert_eq!(
    built.load(Ordering::SeqCst),
    2,
    "the rejoin built exactly one fresh state machine"
  );
  // The rejoined group commits (the fresh replica caught up from reset progress), and the
  // sibling group rode out the whole churn untouched.
  assert_eq!(submit_anywhere(&g100, b"rejoined").await, 2);
  assert_eq!(submit_anywhere(&g900, b"still").await, 2);
}

/// The full-stack cell-1/cell-4 walk: a gen-1 group's removal floors the id at 2 in the host's
/// engine, so even after the explicit tombstone clear the SAME incarnation is refused with the
/// admission-floor rejection — consent cures a tombstone, never the durable fence — while the
/// NEXT incarnation admits cleanly and serves.
#[tokio::test(flavor = "multi_thread")]
async fn removal_at_gen_floors_the_id() {
  let addr: SocketAddr = "127.0.0.1:44860".parse().unwrap();
  let (driver, handle) = bind_node::<CountSm>(1, addr, Vec::new()).await;
  tokio::spawn(driver.run());

  handle
    .create_group(100, config(1, vec![1]), 1, CountSm::default(), 1)
    .await
    .expect("the gen-1 incarnation admits");
  assert!(handle.remove_group(100).await.expect("remove resolves"));
  assert!(
    handle.clear_tombstone(100).await.expect("clear resolves"),
    "a tombstone existed"
  );

  // The tombstone consent is spent — and still the SAME incarnation is refused: the removal
  // floored the id at gen 2, and the fence outranks the (now-cleared) volatile gate.
  match handle
    .create_group(100, config(1, vec![1]), 2, CountSm::default(), 1)
    .await
  {
    Err(DriverError::Rejected { reason }) => {
      assert!(reason.contains("admission floor"), "got: {reason}");
    }
    other => panic!("expected the below-floor rejection, got {other:?}"),
  }

  // The next incarnation is exactly what the floor admits.
  handle
    .create_group(100, config(1, vec![1]), 2, CountSm::default(), 2)
    .await
    .expect("the gen-2 incarnation admits");
  assert_eq!(
    submit_anywhere(std::slice::from_ref(&handle.group(100)), b"x").await,
    1,
    "the re-admitted incarnation is fresh and functional"
  );
}

/// A floored id never re-materializes through the factory: node 2 hosted group 100 at gen 1 and
/// removed it (floor 2 persisted in its engine), the tombstone is explicitly cleared — so the
/// solicitation stream REACHES the factory — and the factory's stale gen-0 blueprint is then
/// refused by the pre-build floor gate: the cheap materialize phase runs, the build (resource)
/// phase NEVER does, and the solicitation surfaces on the lifecycle tail as unplaceable.
#[tokio::test(flavor = "multi_thread")]
async fn floored_id_never_rematerializes_via_factory() {
  let addrs = addrs(44_880, 2);
  let blueprinted = Arc::new(AtomicUsize::new(0));
  let built = Arc::new(AtomicUsize::new(0));
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  for id in 1u64..=2 {
    let peers: Vec<_> = (1u64..=2)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(id, addrs[(id - 1) as usize], peers).await;
    if id == 2 {
      let blueprinted = blueprinted.clone();
      let built = built.clone();
      // The stale catalog view: the factory still vouches for group 100 at its ORIGINAL
      // incarnation (`GroupBlueprint::new` defaults to gen 0) — exactly the advisory replay the
      // floor exists to fence.
      let driver = driver.with_group_factory(factory_fn(
        move |group: &u64, _from: &u64| {
          (*group == 100).then(|| {
            blueprinted.fetch_add(1, Ordering::SeqCst);
            GroupBlueprint::new(config(2, vec![1, 2]), 2)
          })
        },
        move |_group: &u64| {
          built.fetch_add(1, Ordering::SeqCst);
          Some(CountSm::default())
        },
      ));
      tokio::spawn(driver.run());
    } else {
      tokio::spawn(driver.run());
    }
    handles.push(handle);
  }
  // The sibling group binds the mesh (and pins driver survival through the refusals below).
  create_group_everywhere(&handles, 900, &[1, 2]).await;
  let g900: Vec<_> = handles.iter().map(|h| h.group(900)).collect();
  assert_eq!(submit_anywhere(&g900, b"seed").await, 1);

  // Node 2 hosts group 100's gen-1 incarnation, then retires it: the removal floors the id at
  // 2 in node 2's engine, and the explicit clear spends the tombstone so ONLY the floor stands
  // between the id and the factory below.
  handles[1]
    .create_group(100, config(2, vec![1, 2]), 2, CountSm::default(), 1)
    .await
    .expect("the gen-1 incarnation admits on node 2");
  assert!(handles[1].remove_group(100).await.expect("remove resolves"));
  assert!(
    handles[1]
      .clear_tombstone(100)
      .await
      .expect("clear resolves"),
    "a tombstone existed"
  );

  // Node 1 solicits the id (campaigning into the void); node 2's factory vouches — and the
  // pre-build floor gate refuses the stale incarnation, so the signal falls to the tail.
  handles[0]
    .create_group(100, config(1, vec![1, 2]), 1, CountSm::default(), 2)
    .await
    .expect("group 100 admitted on node 1");
  await_lifecycle(handles[1].lifecycle(), "the floored solicitation", |ev| {
    matches!(
      ev,
      LifecycleEvent::UnknownGroup {
        group: 100,
        from: 1
      }
    )
  })
  .await;
  assert!(
    blueprinted.load(Ordering::SeqCst) >= 1,
    "the factory vouched — the FLOOR gate refused it"
  );
  assert_eq!(
    built.load(Ordering::SeqCst),
    0,
    "a floored id must never reach the build phase — no state machine constructed"
  );
  // Nothing materialized; the group stays un-hosted on node 2.
  match handles[1].group(100).status().await {
    Err(DriverError::Rejected { .. }) => {}
    other => panic!("a floored id must leave the group un-hosted, got {other:?}"),
  }
  // The co-hosted sibling rides out the refusal churn.
  assert_eq!(submit_anywhere(&g900, b"still").await, 2);
}

/// The reserved sentinel never materializes through the factory: a buggy catalog vouches for
/// group 100 at `u64::MAX` — the merged-tombstone sentinel, never a working incarnation — with
/// NO fence in the way (the id was never hosted, removed, or floored on node 2), and the
/// pre-build gate still refuses it: the cheap materialize phase runs, the build (resource)
/// phase NEVER does, and the solicitation surfaces on the lifecycle tail as unplaceable.
#[tokio::test(flavor = "multi_thread")]
async fn sentinel_generation_never_materializes_via_factory() {
  let addrs = addrs(44_920, 2);
  let blueprinted = Arc::new(AtomicUsize::new(0));
  let built = Arc::new(AtomicUsize::new(0));
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  for id in 1u64..=2 {
    let peers: Vec<_> = (1u64..=2)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(id, addrs[(id - 1) as usize], peers).await;
    if id == 2 {
      let blueprinted = blueprinted.clone();
      let built = built.clone();
      let driver = driver.with_group_factory(factory_fn(
        move |group: &u64, _from: &u64| {
          (*group == 100).then(|| {
            blueprinted.fetch_add(1, Ordering::SeqCst);
            GroupBlueprint::new(config(2, vec![1, 2]), 2).with_gen(u64::MAX)
          })
        },
        move |_group: &u64| {
          built.fetch_add(1, Ordering::SeqCst);
          Some(CountSm::default())
        },
      ));
      tokio::spawn(driver.run());
    } else {
      tokio::spawn(driver.run());
    }
    handles.push(handle);
  }
  // The sibling group binds the mesh (and pins driver survival through the refusals below).
  create_group_everywhere(&handles, 900, &[1, 2]).await;
  let g900: Vec<_> = handles.iter().map(|h| h.group(900)).collect();
  assert_eq!(submit_anywhere(&g900, b"seed").await, 1);

  // Node 1 solicits the id (campaigning into the void); node 2's factory vouches at the
  // sentinel — and the pre-build gate refuses it, so the signal falls to the tail.
  handles[0]
    .create_group(100, config(1, vec![1, 2]), 1, CountSm::default(), 0)
    .await
    .expect("group 100 admitted on node 1");
  await_lifecycle(handles[1].lifecycle(), "the sentinel solicitation", |ev| {
    matches!(
      ev,
      LifecycleEvent::UnknownGroup {
        group: 100,
        from: 1
      }
    )
  })
  .await;
  assert!(
    blueprinted.load(Ordering::SeqCst) >= 1,
    "the factory vouched — the RESERVED-generation gate refused it"
  );
  assert_eq!(
    built.load(Ordering::SeqCst),
    0,
    "the sentinel must never reach the build phase — no state machine constructed"
  );
  // Nothing materialized; the group stays un-hosted on node 2.
  match handles[1].group(100).status().await {
    Err(DriverError::Rejected { .. }) => {}
    other => panic!("the sentinel must leave the group un-hosted, got {other:?}"),
  }
  // The co-hosted sibling rides out the refusal churn.
  assert_eq!(submit_anywhere(&g900, b"still").await, 2);
}

/// The HONESTY pin: floors survive exactly what the engine survives. In-session the fence holds
/// across remove + clear; a FRESH driver on the same address (the in-memory reference engine's
/// restart) admits gen 0 again — durable floors are the disk-engine mirror's obligation, and the
/// embedder's catalog remains the cross-restart authority until one is in use.
#[tokio::test(flavor = "multi_thread")]
async fn floors_survive_what_the_engine_survives() {
  let addr: SocketAddr = "127.0.0.1:44900".parse().unwrap();
  let (driver, handle) = bind_node::<CountSm>(1, addr, Vec::new()).await;
  tokio::spawn(driver.run());

  handle
    .create_group(100, config(1, vec![1]), 1, CountSm::default(), 1)
    .await
    .expect("the gen-1 incarnation admits");
  assert!(handle.remove_group(100).await.expect("remove resolves"));
  assert!(
    handle.clear_tombstone(100).await.expect("clear resolves"),
    "a tombstone existed"
  );
  // In-session: the fence holds through the spent consent.
  match handle
    .create_group(100, config(1, vec![1]), 2, CountSm::default(), 1)
    .await
  {
    Err(DriverError::Rejected { reason }) => {
      assert!(reason.contains("admission floor"), "got: {reason}");
    }
    other => panic!("expected the below-floor rejection, got {other:?}"),
  }

  // "Restart": tear the host down (releasing the port) and bind a FRESH driver on the same
  // address — a new in-memory engine, as a process restart produces.
  handle.shutdown().await.expect("teardown completes");
  let (driver, handle) = bind_node::<CountSm>(1, addr, Vec::new()).await;
  tokio::spawn(driver.run());

  // The DOCUMENTED volatility: the in-memory reference engine's floors died with it, so the
  // stale gen-0 advisory admits again. A disk engine mirroring the lineage contract would
  // refuse this — that refusal is its obligation, not this host's.
  handle
    .create_group(100, config(1, vec![1]), 1, CountSm::default(), 0)
    .await
    .expect("a fresh engine has no floors — gen 0 admits (the documented volatility)");
  assert_eq!(
    submit_anywhere(std::slice::from_ref(&handle.group(100)), b"x").await,
    1,
    "the post-restart incarnation serves"
  );
}

/// Fork admission failures leave NO engine residue and never touch a live group's storage: a
/// duplicate-gid fork is refused with the hosted group still serving off its own stores
/// (pre-existing storage is not the fork's to roll back), and a tombstoned-id fork — whose
/// engine admission DID happen before the coordinator refused — is rolled back so a later
/// restore sees virgin stores: no manufactured baseline leaks through a refusal.
#[tokio::test(flavor = "multi_thread")]
async fn fork_rollback_leaves_no_engine_residue() {
  let addr: SocketAddr = "127.0.0.1:44940".parse().unwrap();
  let (driver, handle) = bind_node::<CountSm>(1, addr, Vec::new()).await;
  tokio::spawn(driver.run());

  handle
    .create_group(100, config(1, vec![1]), 1, CountSm::default(), 0)
    .await
    .expect("the live group admits");
  let g100 = handle.group(100);
  assert_eq!(
    submit_anywhere(std::slice::from_ref(&g100), b"live").await,
    1
  );

  // Leg 1: a duplicate-gid fork refuses at the container; the live group's storage survives
  // and it keeps serving.
  let blob = encoded(7).into();
  match handle
    .create_group_from_fork(100, config(1, vec![1]), 9, CountSm::default(), blob, 0)
    .await
  {
    Err(DriverError::Rejected { reason }) => {
      assert!(reason.contains("already exists"), "got: {reason}");
    }
    other => panic!("expected the duplicate-id rejection, got {other:?}"),
  }
  assert_eq!(
    submit_anywhere(std::slice::from_ref(&g100), b"still").await,
    2,
    "the live group's storage survived the refused fork"
  );

  // Leg 2: a tombstoned-id fork is refused AFTER engine admission — the rollback must remove
  // the freshly-added storage, or the staged baseline would leak into a later admission.
  assert!(!handle.remove_group(300).await.expect("remove resolves"));
  let blob = encoded(7).into();
  match handle
    .create_group_from_fork(300, config(1, vec![1]), 9, CountSm::default(), blob, 0)
    .await
  {
    Err(DriverError::Rejected { reason }) => {
      assert!(reason.contains("tombstoned"), "got: {reason}");
    }
    other => panic!("expected the tombstoned-id rejection, got {other:?}"),
  }
  assert!(
    handle.clear_tombstone(300).await.expect("clear resolves"),
    "a tombstone existed"
  );
  // The refused fork's rollback removed the freshly-added storage, so there is NO stored state to
  // restore and the host fails closed. A leaked baseline (stores left behind) would instead let
  // the restore succeed against them — so NoStoredState IS the no-leak assertion here.
  match handle
    .restore_group(300, config(1, vec![1]), 9, CountSm::default(), 0)
    .await
  {
    Err(DriverError::NoStoredState) => {}
    other => panic!(
      "the rollback must leave no stored state to restore (a leaked baseline would let restore \
       succeed), got {other:?}"
    ),
  }

  // The driver keeps serving after both refusals.
  assert_eq!(
    submit_anywhere(std::slice::from_ref(&g100), b"after").await,
    3
  );
}

/// THE fork-then-join flow, end to end: nodes 1+2 each fork child 300 from the SAME preloaded
/// blob (voters {1,2}), the child elects and commits a live tail on top of the baseline, and
/// node 3 — whose factory materializes an EMPTY replica on the leader's post-AddNode
/// solicitation — catches up BY SNAPSHOT: the persisted fork blob plus the tail land on its
/// replica (an empty-booted joiner replaying only the tail would sit at 2, not 9). The
/// co-hosted sibling group is untouched throughout.
#[tokio::test(flavor = "multi_thread")]
async fn forked_group_serves_and_snapshots_a_late_joiner() {
  let addrs = addrs(44_960, 3);
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  for id in 1u64..=3 {
    let peers: Vec<_> = (1u64..=3)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(id, addrs[(id - 1) as usize], peers).await;
    if id == 3 {
      // Node 3's catalog knows child 300 and its replica set; the factory materializes an
      // EMPTY replica when the leader's post-AddNode contact solicits it. Child 300 is
      // FORK-BORN, so the blueprint is the mandatory OBSERVER shape (self absent from the
      // seed voters): the empty can never campaign against the manufactured baseline, and
      // the snapshot's boundary config is what promotes it.
      let driver = driver.with_group_factory(factory_fn(
        |group: &u64, from: &u64| {
          (*group == 300 && [1u64, 2].contains(from))
            .then(|| GroupBlueprint::new(observer_config(3, vec![1, 2]), 3))
        },
        |_group: &u64| Some(CountSm::default()),
      ));
      tokio::spawn(driver.run());
    } else {
      tokio::spawn(driver.run());
    }
    handles.push(handle);
  }
  // The sibling group binds the mesh on all three nodes.
  create_group_everywhere(&handles, 900, &[1, 2, 3]).await;
  let g900: Vec<_> = handles.iter().map(|h| h.group(900)).collect();
  assert_eq!(submit_anywhere(&g900, b"seed").await, 1);

  // Nodes 1 and 2 fork the child from the SAME blob: a preloaded count of 7.
  let blob: Bytes = encoded(7).into();
  for (i, h) in handles[..2].iter().enumerate() {
    let id = i as u64 + 1;
    h.create_group_from_fork(
      300,
      config(id, vec![1, 2]),
      id,
      CountSm::default(),
      blob.clone(),
      0,
    )
    .await
    .expect("fork admission");
  }
  let g300: Vec<_> = handles.iter().map(|h| h.group(300)).collect();

  // The forked pair elects and commits a live tail ON TOP of the preloaded baseline.
  assert_eq!(
    submit_anywhere(&g300[..2], b"t1").await,
    8,
    "7 preloaded + 1"
  );
  assert_eq!(submit_anywhere(&g300[..2], b"t2").await, 9);

  // The leader adds node 3, whose replica exists nowhere yet: the factory materializes it
  // EMPTY, and the fork baseline forces the leader onto the snapshot path toward it.
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "no committed AddNode in time"
    );
    let at = find_leader(&g300[..2], "child pre-add").await;
    let cc = ConfChange::new(ConfChangeType::AddNode, 3u64, Bytes::new());
    match g300[at].conf_change(cc).await {
      Ok(_) => break,
      Err(DriverError::NotLeader { .. }) => tokio::time::sleep(Duration::from_millis(50)).await,
      Err(e) => panic!("unexpected conf-change error: {e:?}"),
    }
  }

  // FSM equality across ALL THREE replicas: the preloaded state AND the tail are everywhere.
  // Throughout the joiner's catch-up its observer-booted replica must never campaign — the
  // members hold every election until the snapshot converts it.
  for (i, g) in g300.iter().enumerate() {
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
      assert!(
        std::time::Instant::now() < deadline,
        "node {} never served the joined count",
        i + 1
      );
      if let Ok(st) = g300[2].status().await {
        assert!(
          st.role != Role::Candidate && st.role != Role::PreCandidate && st.role != Role::Leader,
          "an observer-materialized empty must never campaign: {:?}",
          st.role
        );
      }
      if let Ok(c) = g.query(|sm: &CountSm| sm.count()).await {
        assert_eq!(c, 9, "node {}'s replica equals preloaded + tail", i + 1);
        break;
      }
      tokio::time::sleep(Duration::from_millis(50)).await;
    }
  }
  // The snapshot's boundary config converted the observer: node 3's own view now names it a
  // VOTER, with the fork content intact (the count equality above).
  let st = g300[2].status().await.expect("the joined replica answers");
  assert!(
    st.conf_state.voters().contains(&3),
    "the snapshot boundary must promote the observer to voter: {:?}",
    st.conf_state
  );

  // The sibling group is unaffected by the fork/join churn.
  assert_eq!(submit_anywhere(&g900, b"still").await, 2);
}

/// Propose a split through whichever node leads the parent (redirect-following, like
/// `submit_anywhere`), returning the proposed index.
async fn split_anywhere(
  groups: &[GroupHandle<u64, u64, CountSm>],
  child: u64,
  instruction: &'static [u8],
) -> Index {
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  let mut at = 0usize;
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "no split accepted within the deadline"
    );
    match groups[at]
      .propose_split(child, 0, Bytes::from_static(instruction))
      .await
    {
      Ok(idx) => return idx,
      Err(DriverError::NotLeader { .. }) | Err(DriverError::Rejected { .. }) => {
        at = (at + 1) % groups.len();
        tokio::time::sleep(Duration::from_millis(40)).await;
      }
      Err(e) => panic!("unexpected split error: {e:?}"),
    }
  }
}

/// THE split flow, end to end on three live nodes: group 100 commits load, its leader proposes
/// a split of child 200, and EVERY node's drain materializes the child behind its engine
/// barrier — surfacing the typed `LifecycleEvent::SplitApplied` on all three tails — after
/// which BOTH halves elect, commit, and serve linearizable reads with conserved totals.
#[tokio::test(flavor = "multi_thread")]
async fn split_forks_both_halves_live() {
  let addrs = addrs(45_000, 3);
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  for id in 1u64..=3 {
    let peers: Vec<_> = (1u64..=3)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(id, addrs[(id - 1) as usize], peers).await;
    tokio::spawn(driver.run());
    handles.push(handle);
  }
  create_group_everywhere(&handles, 100, &[1, 2, 3]).await;
  let g100: Vec<_> = handles.iter().map(|h| h.group(100)).collect();

  // Load: seven committed commands on the parent.
  for i in 0..7u64 {
    assert_eq!(submit_anywhere(&g100, b"load").await, i + 1);
  }

  // Split: give 5 of the 7 units to child 200.
  split_anywhere(&g100, 200, b"\x05").await;

  // The typed lifecycle event fires on EVERY node's tail — each replica materialized the fork
  // locally behind its own engine barrier.
  for (i, h) in handles.iter().enumerate() {
    await_lifecycle(h.lifecycle(), &format!("node {}", i + 1), |ev| {
      matches!(
        ev,
        LifecycleEvent::SplitApplied {
          parent: 100,
          child: 200
        }
      )
    })
    .await;
  }

  // Both halves serve: the parent shrank to 2 everywhere, the child preloaded 5 and commits a
  // live tail on top of it.
  let g200: Vec<_> = handles.iter().map(|h| h.group(200)).collect();
  assert_eq!(query_anywhere(&g100).await, 2, "the parent kept 7 - 5");
  assert_eq!(
    submit_anywhere(&g200, b"tail").await,
    6,
    "the child preloaded 5 and committed 1 more"
  );
  assert_eq!(query_anywhere(&g200).await, 6);
  // Conservation: post-split parent + pre-tail child == the pre-split total.
  assert_eq!(
    submit_anywhere(&g100, b"more").await,
    3,
    "the parent keeps committing on its own log"
  );
}

/// The fresh-joiner pin: a split-born child's manufactured baseline (`first_index == 2`)
/// structurally forces a zero-progress joiner onto the SNAPSHOT path — the persisted blob plus
/// the child's live tail land on the joiner's replica, never an empty log walk. The parent
/// lives on nodes 1+2 only; node 3's factory materializes the EMPTY child replica when the
/// child leader's post-AddNode contact solicits it.
#[tokio::test(flavor = "multi_thread")]
async fn fresh_joiner_takes_the_snapshot_path() {
  let addrs = addrs(45_010, 3);
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  for id in 1u64..=3 {
    let peers: Vec<_> = (1u64..=3)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(id, addrs[(id - 1) as usize], peers).await;
    if id == 3 {
      // Node 3's catalog knows the split-born child and its replica set; it declines nothing
      // else. A fork-born id reaches a NON-member host only through this ordinary join path,
      // and its blueprint is the mandatory OBSERVER shape (self absent from the seed voters):
      // the empty grants votes but cannot campaign, and the snapshot's boundary config is
      // what promotes it.
      let driver = driver.with_group_factory(factory_fn(
        |group: &u64, from: &u64| {
          (*group == 200 && [1u64, 2].contains(from))
            .then(|| GroupBlueprint::new(observer_config(3, vec![1, 2]), 3))
        },
        |_group: &u64| Some(CountSm::default()),
      ));
      tokio::spawn(driver.run());
    } else {
      tokio::spawn(driver.run());
    }
    handles.push(handle);
  }
  // The parent lives on nodes 1+2 only, so the split-born child does too.
  create_group_everywhere(&handles[..2], 100, &[1, 2]).await;
  let g100: Vec<_> = handles[..2].iter().map(|h| h.group(100)).collect();
  for i in 0..7u64 {
    assert_eq!(submit_anywhere(&g100, b"load").await, i + 1);
  }
  split_anywhere(&g100, 200, b"\x05").await;
  for (i, h) in handles[..2].iter().enumerate() {
    await_lifecycle(h.lifecycle(), &format!("member {}", i + 1), |ev| {
      matches!(
        ev,
        LifecycleEvent::SplitApplied {
          parent: 100,
          child: 200
        }
      )
    })
    .await;
  }

  // The child commits a live tail on top of its preloaded half.
  let g200_members: Vec<_> = handles[..2].iter().map(|h| h.group(200)).collect();
  assert_eq!(submit_anywhere(&g200_members, b"t1").await, 6);

  // AddNode 3: the joiner's replica exists nowhere — the factory materializes it EMPTY, and the
  // manufactured baseline (first_index == 2 > the joiner's next == 1) forces the child leader
  // onto the snapshot path toward it.
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "no committed AddNode in time"
    );
    let at = find_leader(&g200_members, "child pre-add").await;
    let cc = ConfChange::new(ConfChangeType::AddNode, 3u64, Bytes::new());
    match g200_members[at].conf_change(cc).await {
      Ok(_) => break,
      Err(DriverError::NotLeader { .. }) => tokio::time::sleep(Duration::from_millis(50)).await,
      Err(e) => panic!("unexpected conf-change error: {e:?}"),
    }
  }

  // The joiner lands on preloaded + tail: an empty-booted replica replaying only the tail
  // would sit at 1 — equality proves the persisted blob arrived through the snapshot path.
  // Its observer-booted replica must never campaign along the way: the members hold every
  // election until the snapshot converts it.
  let g200_joiner = handles[2].group(200);
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "the joiner never served the joined count"
    );
    if let Ok(st) = g200_joiner.status().await {
      assert!(
        st.role != Role::Candidate && st.role != Role::PreCandidate && st.role != Role::Leader,
        "an observer-materialized empty must never campaign: {:?}",
        st.role
      );
    }
    if let Ok(c) = g200_joiner.query(|sm: &CountSm| sm.count()).await {
      assert_eq!(
        c, 6,
        "preloaded 5 + the 1-entry tail, via the snapshot path"
      );
      break;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
  }
  // The snapshot's boundary config converted the observer to a VOTER, fork content intact.
  let st = g200_joiner.status().await.expect("the joiner answers");
  assert!(
    st.conf_state.voters().contains(&3),
    "the snapshot boundary must promote the observer to voter: {:?}",
    st.conf_state
  );
}

/// The late splitter: a replica DOWN through the split converges by the ordinary lifecycle
/// paths, never by
/// re-forking. Node 3 dies before the split; the surviving parent quorum splits, materializes
/// the child, and compacts past the split entry. The reborn node 3 (fresh stores) receives the
/// parent's POST-split snapshot — its lifecycle tail must never see a `SplitApplied` — and the
/// child reaches it through solicitation → its factory's OBSERVER blueprint (the fork-born
/// rule: the empty cannot campaign, the holders elect) → the child leader's chunked snapshot
/// serving the PERSISTED blob, whose boundary config promotes the joiner to voter.
#[tokio::test(flavor = "multi_thread")]
async fn late_splitter_converges_via_lifecycle() {
  let addrs = addrs(45_020, 3);
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  for id in 1u64..=3 {
    let peers: Vec<_> = (1u64..=3)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(id, addrs[(id - 1) as usize], peers).await;
    tokio::spawn(driver.run());
    handles.push(handle);
  }
  // A tight snapshot threshold so the parent compacts past the split while node 3 is away.
  let parent_cfg = |id: u64| config(id, vec![1, 2, 3]).with_snapshot_threshold(4);
  for (i, h) in handles.iter().enumerate() {
    h.create_group(
      100,
      parent_cfg(i as u64 + 1),
      i as u64 + 1,
      CountSm::default(),
      0,
    )
    .await
    .expect("parent admission");
  }
  let g100: Vec<_> = handles.iter().map(|h| h.group(100)).collect();
  for i in 0..5u64 {
    assert_eq!(submit_anywhere(&g100, b"pre").await, i + 1);
  }

  // Node 3 dies before the split.
  handles[2].shutdown().await.expect("node 3 shuts down");
  let survivors: Vec<_> = handles[..2].iter().map(|h| h.group(100)).collect();

  // The surviving quorum splits (child voters = the parent's full set, INCLUDING the dead
  // node) and materializes the child; more load drives the parent's capture past the split.
  split_anywhere(&survivors, 200, b"\x03").await;
  for (i, h) in handles[..2].iter().enumerate() {
    await_lifecycle(h.lifecycle(), &format!("survivor {}", i + 1), |ev| {
      matches!(
        ev,
        LifecycleEvent::SplitApplied {
          parent: 100,
          child: 200
        }
      )
    })
    .await;
  }
  for _ in 0..6u64 {
    submit_anywhere(&survivors, b"post").await;
  }
  let g200_members: Vec<_> = handles[..2].iter().map(|h| h.group(200)).collect();
  assert_eq!(
    submit_anywhere(&g200_members, b"t1").await,
    4,
    "3 preloaded + 1"
  );

  // Node 3 is REBORN with fresh stores; its factory knows the child (and only the child — the
  // parent is re-created explicitly, the ordinary operator path for a re-provisioned node).
  // The child is FORK-BORN, so the blueprint is the mandatory OBSERVER shape (self absent
  // from the seed voters): the materialized empty cannot campaign against the manufactured
  // baseline — the holders keep every election — and the chunked snapshot serving the
  // persisted blob is what converts it to a voter.
  let peers3: Vec<_> = (1u64..=2)
    .map(|p| Node::new(p, addrs[(p - 1) as usize]))
    .collect();
  let (driver3, handle3) = bind_node::<CountSm>(3, addrs[2], peers3).await;
  let driver3 = driver3.with_group_factory(factory_fn(
    |group: &u64, from: &u64| {
      (*group == 200 && [1u64, 2].contains(from))
        .then(|| GroupBlueprint::new(observer_config(3, vec![1, 2]), 3))
    },
    |_group: &u64| Some(CountSm::default()),
  ));
  tokio::spawn(driver3.run());
  handle3
    .create_group(100, parent_cfg(3), 3, CountSm::default(), 0)
    .await
    .expect("the reborn node re-admits the parent");

  // The parent converges by INSTALL (the entry is compacted away), the child by the factory
  // path — and the reborn node NEVER re-forks: no SplitApplied may reach its lifecycle tail.
  // Throughout the catch-up the child's observer-booted replica must never campaign (the
  // holders keep the election; a promotable full-voter empty here is exactly the fusion
  // hazard `full_voter_blueprint_for_a_fork_born_id_fuses_histories` demonstrates).
  let deadline = std::time::Instant::now() + Duration::from_secs(20);
  let g100_reborn = handle3.group(100);
  let g200_reborn = handle3.group(200);
  let mut parent_ok = false;
  let mut child_ok = false;
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "the late splitter never converged (parent {parent_ok}, child {child_ok})"
    );
    if let Ok(st) = g200_reborn.status().await {
      assert!(
        st.role != Role::Candidate && st.role != Role::PreCandidate && st.role != Role::Leader,
        "an observer-materialized empty must never campaign: {:?}",
        st.role
      );
    }
    if !parent_ok && let Ok(c) = g100_reborn.query(|sm: &CountSm| sm.count()).await {
      // 5 pre + 6 post commits, minus the 3 units given away at the split.
      assert_eq!(c, 8, "the reborn parent replica is post-split");
      parent_ok = true;
    }
    if !child_ok && let Ok(c) = g200_reborn.query(|sm: &CountSm| sm.count()).await {
      assert_eq!(c, 4, "the persisted blob + tail reached the late splitter");
      child_ok = true;
    }
    if parent_ok && child_ok {
      break;
    }
    tokio::time::sleep(Duration::from_millis(60)).await;
  }
  // The chunked snapshot's boundary config converted the observer: the reborn node's own view
  // names it a VOTER of the child, with the fork content intact (the count equality above).
  let st = g200_reborn
    .status()
    .await
    .expect("the reborn child answers");
  assert!(
    st.conf_state.voters().contains(&3),
    "the snapshot boundary must promote the observer to voter: {:?}",
    st.conf_state
  );
  while let Ok(ev) = handle3.lifecycle().try_recv() {
    assert!(
      !matches!(ev, LifecycleEvent::SplitApplied { .. }),
      "a post-compaction rebirth must never re-fork: {ev:?}"
    );
  }
}

/// THE HAZARD DEMONSTRATION — this test asserts the BAD outcome on purpose: it pins,
/// executably, WHY the `GroupFactory` contract forbids full-voter blueprints for fork-born ids
/// (the observer rule), by driving the divergence such a blueprint permits. If a structural
/// product guard ever closes this shape, the test fails loudly and must be flipped into that
/// guard's regression.
///
/// Node 1 forks child 200 (voters {1,2,3}) alone: a manufactured baseline at (index 1, term 1)
/// carrying a preloaded count of 7 behind `first_index == 2`. Its campaign solicits nodes 2+3,
/// whose factories vouch the fork-born id FULL-VOTER — self included, the contract violation —
/// so the materialized empties are promotable with virgin election timers (the soliciting
/// frame itself is consumed by the signal path, so no vote is ever cast from it). The holder's
/// election timeout is slow and the empties' fast (and mutually disjoint, so their first round
/// cannot split): the empty pair elects AMONG ITSELF and commits its no-op at index 1 — the
/// manufactured baseline's exact coordinate — and log-matching fuses the divergent histories
/// silently. The leader believes the holder fully matched, so no snapshot ever flows; the 7
/// preloaded units survive on the holder's replica alone and are neither propagated nor
/// erased. The pinned outcome: every replica applies the same committed tail, yet the holder's
/// linearizable read exceeds the joiners' by exactly the fork baseline — permanently.
#[tokio::test(flavor = "multi_thread")]
async fn full_voter_blueprint_for_a_fork_born_id_fuses_histories() {
  const SLOW_ELECTION: Duration = Duration::from_millis(2500);
  const SLOW_HEARTBEAT: Duration = Duration::from_millis(500);
  let addrs = addrs(45_360, 3);
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  for id in 1u64..=3 {
    let peers: Vec<_> = (1u64..=3)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(id, addrs[(id - 1) as usize], peers).await;
    if id == 1 {
      tokio::spawn(driver.run());
    } else {
      // The contract violation under test: a fork-born id vouched with SELF IN THE VOTERS.
      // Node 3's election window sits disjointly above node 2's so the empty pair's first
      // election resolves in one round, deterministically inside the holder's slow-recampaign
      // gap.
      let election = if id == 2 {
        ELECTION
      } else {
        Duration::from_millis(700)
      };
      let driver = driver.with_group_factory(factory_fn(
        move |group: &u64, from: &u64| {
          (*group == 200 && [1u64, 2, 3].contains(from)).then(|| {
            let full_voter = Config::try_new(id, vec![1, 2, 3], election, HEARTBEAT)
              .expect("a valid full-voter config");
            GroupBlueprint::new(full_voter, id)
          })
        },
        |_group: &u64| Some(CountSm::default()),
      ));
      tokio::spawn(driver.run());
    }
    handles.push(handle);
  }
  // The sibling binds the mesh so the holder's solicitations flow immediately.
  create_group_everywhere(&handles, 900, &[1, 2, 3]).await;
  let g900: Vec<_> = handles.iter().map(|h| h.group(900)).collect();
  assert_eq!(submit_anywhere(&g900, b"seed").await, 1);

  // The fork holder: child 200 exists on node 1 alone, preloaded 7, slow election so its
  // recampaign can never outrun the empties' first election.
  let blob: Bytes = encoded(7).into();
  let slow = Config::try_new(1u64, vec![1, 2, 3], SLOW_ELECTION, SLOW_HEARTBEAT).unwrap();
  handles[0]
    .create_group_from_fork(200, slow, 1, CountSm::default(), blob, 0)
    .await
    .expect("fork admission");

  // The holder's campaign materializes both empties; the empty pair elects among itself and a
  // live tail commits on the FUSED log (the submit resolves through whichever side leads).
  let g200: Vec<_> = handles.iter().map(|h| h.group(200)).collect();
  submit_anywhere(&g200, b"tail").await;

  // THE DIVERGENCE, observably: all three replicas serve the SAME committed log, yet the
  // holder's linearizable read exceeds the empties' by exactly the preloaded baseline — the
  // fork content neither propagated (the leader believes everyone matched) nor was erased.
  let divergence = |q1: u64, q2: u64, q3: u64| q2 == q3 && q2 >= 1 && q1 == q2 + 7;
  let deadline = std::time::Instant::now() + Duration::from_secs(20);
  let mut last = None;
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "the fusion hazard did not reproduce (last counts {last:?}): if a structural guard now \
       forces fork-born empties onto the snapshot path, flip this regression to assert the \
       HEALED outcome"
    );
    if let (Ok(q1), Ok(q2), Ok(q3)) = (
      g200[0].query(|sm: &CountSm| sm.count()).await,
      g200[1].query(|sm: &CountSm| sm.count()).await,
      g200[2].query(|sm: &CountSm| sm.count()).await,
    ) {
      last = Some((q1, q2, q3));
      if divergence(q1, q2, q3) {
        break;
      }
    }
    tokio::time::sleep(Duration::from_millis(60)).await;
  }

  // And it is PERMANENT: log-matching sees every replica matched, so nothing ever heals it —
  // a settle window (many heartbeats, ample snapshot room) later the fused histories still
  // disagree by the whole baseline.
  tokio::time::sleep(Duration::from_millis(1500)).await;
  let deadline = std::time::Instant::now() + Duration::from_secs(10);
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "post-settle re-read never resolved"
    );
    if let (Ok(q1), Ok(q2), Ok(q3)) = (
      g200[0].query(|sm: &CountSm| sm.count()).await,
      g200[1].query(|sm: &CountSm| sm.count()).await,
      g200[2].query(|sm: &CountSm| sm.count()).await,
    ) {
      assert!(
        divergence(q1, q2, q3),
        "the fused divergence must persist: ({q1}, {q2}, {q3})"
      );
      break;
    }
    tokio::time::sleep(Duration::from_millis(60)).await;
  }
}

/// A split into a child id this host has TOMBSTONED is refused at PROPOSE (the coordinator's
/// #97-1 ChildRetired gate): the fork could never materialize onto a retired id, so the entry is
/// never appended and the parent never shrinks — no data loss. The child stays unhosted until the
/// embedder clears the tombstone and recreates.
#[tokio::test(flavor = "multi_thread")]
async fn split_into_a_tombstoned_child_refuses_at_propose() {
  let addr: SocketAddr = "127.0.0.1:45300".parse().unwrap();
  let (driver, handle) = bind_node::<CountSm>(1, addr, Vec::new()).await;
  tokio::spawn(driver.run());

  create_group_everywhere(std::slice::from_ref(&handle), 100, &[1]).await;
  let g100 = handle.group(100);
  for i in 0..3u64 {
    assert_eq!(
      submit_anywhere(std::slice::from_ref(&g100), b"load").await,
      i + 1
    );
  }

  // Tombstone the child id (an unhosted removal still tombstones), then split into it: the
  // coordinator's ChildRetired gate refuses at PROPOSE, before anything is appended.
  assert!(!handle.remove_group(300).await.expect("remove resolves"));
  let err = g100
    .propose_split(300, 0, Bytes::from_static(b"\x02"))
    .await
    .expect_err("a split into a locally-tombstoned child is refused at propose");
  assert!(
    matches!(&err, DriverError::Rejected { reason } if reason.contains("ChildRetired")),
    "the typed ChildRetired refusal, got {err:?}"
  );

  // The parent never shrank — the split was never appended, so no unit was given away or lost —
  // and it keeps committing.
  assert_eq!(query_anywhere(std::slice::from_ref(&g100)).await, 3);
  assert_eq!(
    submit_anywhere(std::slice::from_ref(&g100), b"after").await,
    4
  );
  // The tombstoned child id stays unhosted.
  assert!(handle.group(300).status().await.is_err());
}

/// A hosted-child conflict on a live host PARKS the fork and HEALS: node 2 hosts an empty
/// squatter under the child id before the split (the leader's propose gate cannot see a remote
/// host's groups), so node 2's drain parks the committed fork and surfaces
/// `LifecycleEvent::SplitConflict` — pre-fix this replica silently discarded the child's half
/// and lifted the fence. Node 1 materializes normally; its child replica then leads and
/// snapshots the squatter up to the fork baseline (the manufactured `first_index == 2` forces
/// the install path), at which point node 2's parked fork resolves as REDUNDANT — the twin
/// provably carries the partition — the fence lifts on its own, and both halves serve with
/// conserved totals. No `SplitRefused` may ever fire: the fork was never abandoned.
#[tokio::test(flavor = "multi_thread")]
async fn hosted_child_conflict_parks_then_heals_via_the_twin() {
  let addrs = addrs(45_320, 2);
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  for id in 1u64..=2 {
    let peers: Vec<_> = (1u64..=2)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(id, addrs[(id - 1) as usize], peers).await;
    tokio::spawn(driver.run());
    handles.push(handle);
  }
  create_group_everywhere(&handles, 100, &[1, 2]).await;
  let g100: Vec<_> = handles.iter().map(|h| h.group(100)).collect();
  for i in 0..7u64 {
    assert_eq!(submit_anywhere(&g100, b"load").await, i + 1);
  }

  // The squatter: node 2 hosts an EMPTY group under the child id (same voter set as the child
  // will boot with, zero progress). Admitted before any split is in flight anywhere.
  handles[1]
    .create_group(300, config(2, vec![1, 2]), 9, CountSm::default(), 0)
    .await
    .expect("the squatter admits: no split names this id yet");

  // Propose the split on NODE 1, whose gate cannot see node 2's squatter. Node 2 may hold the
  // parent lease; steer leadership to node 1 until the propose lands there.
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "the split never proposed on node 1"
    );
    match g100[0]
      .propose_split(300, 0, Bytes::from_static(b"\x05"))
      .await
    {
      Ok(_) => break,
      Err(DriverError::NotLeader { .. }) => {
        let _ = g100[1].transfer_leader(1).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
      }
      Err(e) => panic!("unexpected split error: {e:?}"),
    }
  }

  // Node 1 materializes its fork normally; node 2 PARKS and surfaces the typed conflict —
  // pre-fix, node 2 dropped the fork here with no signal at all.
  await_lifecycle(handles[0].lifecycle(), "node 1", |ev| {
    matches!(
      ev,
      LifecycleEvent::SplitApplied {
        parent: 100,
        child: 300
      }
    )
  })
  .await;
  await_lifecycle(handles[1].lifecycle(), "node 2 (parked)", |ev| {
    matches!(
      ev,
      LifecycleEvent::SplitConflict {
        parent: 100,
        child: 300
      }
    )
  })
  .await;

  // The parent shrank once per replica (apply is replica-identical) and keeps serving.
  assert_eq!(query_anywhere(&g100).await, 2, "7 - 5 everywhere");

  // HEAL: node 1's child replica leads {1,2} and the manufactured baseline forces the empty
  // squatter onto the snapshot path — it becomes the twin (applied >= baseline at the fork's
  // lineage), node 2's parked fork resolves as redundant, and BOTH replicas serve the half.
  let g300: Vec<_> = handles.iter().map(|h| h.group(300)).collect();
  assert_eq!(
    submit_anywhere(&g300, b"tail").await,
    6,
    "the child preloaded 5 and committed 1 more"
  );
  assert_eq!(query_anywhere(&g300).await, 6);

  // The redundant fold released node 2's fence silently: the parent commits on, and the fork
  // was never abandoned — no SplitRefused may have reached either tail.
  assert_eq!(submit_anywhere(&g100, b"after").await, 3);
  for (i, h) in handles.iter().enumerate() {
    while let Ok(ev) = h.lifecycle().try_recv() {
      assert!(
        !matches!(ev, LifecycleEvent::SplitRefused { .. }),
        "node {}: a parked fork must resolve, never abandon: {ev:?}",
        i + 1
      );
    }
  }
}

/// The factory-race regression: while a split's child id is RESERVED on a host, the factory
/// pre-build gate DECLINES solicitations for it — the local fork, never a factory-built empty
/// squatter, is what materializes the id. The reserved window is held open deterministically:
/// the parent (and so both fork children) runs a SLOW election tuning, node 2 squats fork #1's
/// child so fork #2 sits staged-and-reserved behind the park (head-of-line), and — only once
/// the park is observed — node 3 admits an EMPTY fast-cadence pre-vote squatter of the second
/// child id whose solicitations hammer node 2 seconds before any slow child election can heal
/// the park. Node 2's factory KNOWS the id and its blueprint names the solicitor, so the
/// reservation is the ONE leg that refuses: consults reach the factory, zero builds pass, the
/// id stays unhosted on node 2 — and when the park heals through the twin, fork #2
/// materializes from its own staged blob (`SplitApplied`, the typed observable a factory build
/// would have silently folded away). No abandonment, no second conflict, conserved totals.
#[tokio::test(flavor = "multi_thread")]
async fn factory_gate_declines_a_reserved_child_until_the_fork_lands() {
  // Slow consensus for the parent — inherited by both fork children — so the parked window
  // (healed only by a child election) outlasts the fast squatter's solicitations by seconds.
  const SLOW_ELECTION: Duration = Duration::from_millis(2500);
  const SLOW_HEARTBEAT: Duration = Duration::from_millis(500);
  let slow =
    |id: u64, voters: Vec<u64>| Config::try_new(id, voters, SLOW_ELECTION, SLOW_HEARTBEAT).unwrap();

  let addrs = addrs(45_340, 3);
  let consults = Arc::new(AtomicUsize::new(0));
  let builds = Arc::new(AtomicUsize::new(0));
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  for id in 1u64..=3 {
    let peers: Vec<_> = (1u64..=3)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(id, addrs[(id - 1) as usize], peers).await;
    if id == 2 {
      // Node 2's catalog knows child 320 and would happily materialize it empty on a
      // solicitation — exactly what the reservation gate must refuse while fork #2 is staged.
      // `materialize` runs BEFORE the gate (consults count solicitations that reached the
      // factory); `build` only ever runs behind it. The blueprint is the fork-born OBSERVER
      // shape (self absent from the seed voters) and names the node-3 solicitor, so the
      // sender leg passes and the reservation is the one refusing leg.
      let consults_ = consults.clone();
      let builds_ = builds.clone();
      let driver = driver.with_group_factory(factory_fn(
        move |group: &u64, from: &u64| {
          (*group == 320 && [1u64, 2, 3].contains(from)).then(|| {
            consults_.fetch_add(1, Ordering::SeqCst);
            GroupBlueprint::new(observer_config(2, vec![1, 3]), 0)
          })
        },
        move |_group: &u64| {
          builds_.fetch_add(1, Ordering::SeqCst);
          Some(CountSm::default())
        },
      ));
      tokio::spawn(driver.run());
    } else {
      tokio::spawn(driver.run());
    }
    handles.push(handle);
  }
  // The parent lives on nodes 1+2 (slow tuning); node 3 carries no parent replica.
  for (i, h) in handles[..2].iter().enumerate() {
    let id = i as u64 + 1;
    h.create_group(100, slow(id, vec![1, 2]), id, CountSm::default(), 0)
      .await
      .expect("parent admission");
  }
  let g100: Vec<_> = handles[..2].iter().map(|h| h.group(100)).collect();
  for i in 0..7u64 {
    assert_eq!(submit_anywhere(&g100, b"load").await, i + 1);
  }

  // The squatter under fork #1's child id, on node 2 only (empty, zero progress).
  handles[1]
    .create_group(310, slow(2, vec![1, 2]), 9, CountSm::default(), 0)
    .await
    .expect("the squatter admits: no split names this id yet");

  // Both splits are proposed on NODE 1 (whose gates see neither the squatter nor node 2's
  // reservation): steer leadership there and ride out SplitInFlight between the two.
  for (child, give) in [(310u64, b"\x02"), (320u64, b"\x03")] {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
      assert!(
        std::time::Instant::now() < deadline,
        "split into {child} never proposed on node 1"
      );
      match g100[0]
        .propose_split(child, 0, Bytes::from_static(give))
        .await
      {
        Ok(_) => break,
        Err(DriverError::NotLeader { .. }) => {
          let _ = g100[1].transfer_leader(1).await;
          tokio::time::sleep(Duration::from_millis(50)).await;
        }
        // An earlier split still unapplied (or a transfer window): retry.
        Err(DriverError::Rejected { .. }) => {
          tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Err(e) => panic!("unexpected split error: {e:?}"),
      }
    }
  }

  // Node 1 materializes both children; node 2 parks fork #1 on the squatter — fork #2 stays
  // staged (and reserved) behind it, and nothing can heal the park before a SLOW child
  // election fires.
  for child in [310u64, 320] {
    await_lifecycle(
      handles[0].lifecycle(),
      "node 1",
      |ev| matches!(ev, LifecycleEvent::SplitApplied { parent: 100, child: c } if *c == child),
    )
    .await;
  }
  await_lifecycle(handles[1].lifecycle(), "node 2 (parked)", |ev| {
    matches!(
      ev,
      LifecycleEvent::SplitConflict {
        parent: 100,
        child: 310
      }
    )
  })
  .await;

  // THE LIVE-WINDOW PIN. Only now — the park observed, fork #2 provably staged-and-reserved —
  // node 3 admits an empty FAST pre-vote squatter of the second child id: its solicitations
  // reach node 2 within ~an election (fast) while the heal is still seconds out (slow). Every
  // consult that lands in the window must die at the gate: zero builds, the id unhosted.
  handles[2]
    .create_group(
      320,
      config(3, vec![2, 3]).with_pre_vote(true),
      11,
      CountSm::default(),
      0,
    )
    .await
    .expect("the fast solicitor admits on node 3");
  let deadline = std::time::Instant::now() + Duration::from_secs(10);
  while consults.load(Ordering::SeqCst) == 0 {
    assert!(
      std::time::Instant::now() < deadline,
      "no solicitation for the reserved id reached node 2's factory in time"
    );
    tokio::time::sleep(Duration::from_millis(30)).await;
  }
  assert_eq!(
    builds.load(Ordering::SeqCst),
    0,
    "a reserved child id must never reach the factory's build phase"
  );
  assert!(
    handles[1].group(320).status().await.is_err(),
    "node 2 must not host the reserved id while its fork is staged"
  );

  // The park heals through the twin (node 1's 310 replica elects and snapshots the squatter),
  // fork #2 yields, and node 2's tail carries the typed SplitApplied — the fork, not the
  // factory, materialized the id (a factory build would have folded it away silently).
  await_lifecycle(handles[1].lifecycle(), "node 2 (fork #2 lands)", |ev| {
    matches!(
      ev,
      LifecycleEvent::SplitApplied {
        parent: 100,
        child: 320
      }
    )
  })
  .await;
  eprintln!(
    "factory race: {} consult(s) for 320 reached node 2's factory, {} build(s) passed the gate",
    consults.load(Ordering::SeqCst),
    builds.load(Ordering::SeqCst)
  );
  assert_eq!(builds.load(Ordering::SeqCst), 0);

  // Both children serve with conserved totals; the parent kept 7 - 2 - 3. The node-3 squatter
  // stays a harmless pre-vote candidate: its candidacy is refused on log freshness without
  // ever bumping the live group's term.
  let g310: Vec<_> = handles[..2].iter().map(|h| h.group(310)).collect();
  let g320: Vec<_> = handles[..2].iter().map(|h| h.group(320)).collect();
  assert_eq!(query_anywhere(&g100).await, 2);
  assert_eq!(submit_anywhere(&g310, b"t").await, 3, "2 forked + 1 tail");
  assert_eq!(submit_anywhere(&g320, b"t").await, 4, "3 forked + 1 tail");

  // No fork was ever abandoned and fork #2 never conflicted: the gate refused the squatter
  // the factory would have planted.
  for (i, h) in handles.iter().enumerate() {
    while let Ok(ev) = h.lifecycle().try_recv() {
      assert!(
        !matches!(ev, LifecycleEvent::SplitRefused { .. }),
        "node {}: no fork may be abandoned here: {ev:?}",
        i + 1
      );
      assert!(
        !matches!(ev, LifecycleEvent::SplitConflict { child: 320, .. }),
        "node {}: the reserved id must never grow a squatter: {ev:?}",
        i + 1
      );
    }
  }
}

/// Steer a split proposal to node 1 (whose gates cannot see node 2's groups), riding out
/// leadership on the other node and a still-unresolved earlier split; returns the split
/// entry's log index.
async fn steer_split_to_node1(
  g100: &[GroupHandle<u64, u64, CountSm>],
  child: u64,
  give: &'static [u8],
) -> Index {
  let deadline = std::time::Instant::now() + Duration::from_secs(20);
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "split into {child} never proposed on node 1"
    );
    match g100[0]
      .propose_split(child, 0, Bytes::from_static(give))
      .await
    {
      Ok(idx) => return idx,
      Err(DriverError::NotLeader { .. }) => {
        let _ = g100[1].transfer_leader(1).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
      }
      Err(DriverError::Rejected { .. }) => {
        tokio::time::sleep(Duration::from_millis(50)).await;
      }
      Err(e) => panic!("unexpected split error: {e:?}"),
    }
  }
}

/// THE BACKPRESSURE PIN: a park's conflict signal survives a momentarily-full lifecycle tail.
/// Node 2 runs a CAPACITY-1 lifecycle tail, pre-filled by split #1's own `SplitApplied` and
/// deliberately not drained. Split #2 then parks on node 2's squatter while the tail is still
/// full — pre-fix the drain popped the coordinator's one-shot signal and `try_send` dropped it,
/// erasing the embedder's only cue for the episode (parent fence standing, child id reserved,
/// fork parked — invisibly and indefinitely); this await timed out with no conflict ever
/// delivered. Post-fix the signal stays queued at the coordinator until the tail has room:
/// draining the stale event must surface the conflict, exactly once for the episode, and the
/// park then heals through the twin exactly as with an unbounded tail. The parent (and so both
/// fork children) runs the SLOW tuning so the twin heal — which would purge an undelivered
/// signal — stays seconds behind the heartbeat-paced delivery.
#[tokio::test(flavor = "multi_thread")]
async fn parked_conflict_survives_a_full_lifecycle_tail() {
  const SLOW_ELECTION: Duration = Duration::from_millis(2500);
  const SLOW_HEARTBEAT: Duration = Duration::from_millis(500);
  let slow =
    |id: u64, voters: Vec<u64>| Config::try_new(id, voters, SLOW_ELECTION, SLOW_HEARTBEAT).unwrap();

  let addrs = addrs(45_380, 2);
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  for id in 1u64..=2 {
    let peers: Vec<_> = (1u64..=2)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    // Node 2 (the parking host): ONE lifecycle slot, so a single undrained event is a full
    // tail at the park instant.
    let cfg = if id == 2 {
      DriverConfig {
        events_cap: 1,
        ..DriverConfig::default()
      }
    } else {
      DriverConfig::default()
    };
    let (driver, handle) =
      bind_node_with::<CountSm>(id, addrs[(id - 1) as usize], peers, cfg).await;
    tokio::spawn(driver.run());
    handles.push(handle);
  }
  for (i, h) in handles.iter().enumerate() {
    let id = i as u64 + 1;
    h.create_group(100, slow(id, vec![1, 2]), id, CountSm::default(), 0)
      .await
      .expect("parent admission");
  }
  let g100: Vec<_> = handles.iter().map(|h| h.group(100)).collect();
  for i in 0..7u64 {
    assert_eq!(submit_anywhere(&g100, b"load").await, i + 1);
  }

  // Split #1 (clean, both nodes materialize): node 2's `SplitApplied` fills its one-slot tail.
  let _ = steer_split_to_node1(&g100, 310, b"\x02").await;
  await_lifecycle(handles[0].lifecycle(), "node 1 (split #1)", |ev| {
    matches!(
      ev,
      LifecycleEvent::SplitApplied {
        parent: 100,
        child: 310
      }
    )
  })
  .await;
  // Node 2's materialization is observed through STATUS, never its tail: the event that fired
  // with it must stay queued so the tail is provably full from here on.
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  while handles[1].group(310).status().await.is_err() {
    assert!(
      std::time::Instant::now() < deadline,
      "node 2 never materialized split #1"
    );
    tokio::time::sleep(Duration::from_millis(30)).await;
  }

  // The squatter under split #2's child id, then the split: node 2 parks with a FULL tail.
  handles[1]
    .create_group(320, slow(2, vec![1, 2]), 9, CountSm::default(), 0)
    .await
    .expect("the squatter admits: no split names this id yet");
  let split2 = steer_split_to_node1(&g100, 320, b"\x03").await;
  await_lifecycle(handles[0].lifecycle(), "node 1 (split #2)", |ev| {
    matches!(
      ev,
      LifecycleEvent::SplitApplied {
        parent: 100,
        child: 320
      }
    )
  })
  .await;

  // The tail must still be FULL at the park instant, so the park is confirmed WITHOUT touching
  // it: node 2's parent replica reports the split applied (the fork is staged), and each of the
  // follow-up status commands drives a full loop pass — a storage crank whose fork drain
  // examines the staged fork, parks it, and publishes the conflict against the full tail.
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  loop {
    let status = handles[1]
      .group(100)
      .status()
      .await
      .expect("node 2 hosts the parent");
    if status.applied_index >= split2 {
      break;
    }
    assert!(
      std::time::Instant::now() < deadline,
      "node 2 never applied split #2"
    );
    tokio::time::sleep(Duration::from_millis(30)).await;
  }
  for _ in 0..3 {
    let _ = handles[1].group(100).status().await;
  }

  // Free the slot and the deferred conflict must land — the lost-signal regression.
  await_lifecycle(handles[1].lifecycle(), "node 2 (deferred conflict)", |ev| {
    matches!(
      ev,
      LifecycleEvent::SplitConflict {
        parent: 100,
        child: 320
      }
    )
  })
  .await;

  // Exactly one signal per episode: consuming the delivered cue re-arms nothing, whether the
  // park still stands or has just healed silently through the twin.
  tokio::time::sleep(SLOW_HEARTBEAT * 2).await;
  while let Ok(ev) = handles[1].lifecycle().try_recv() {
    assert!(
      !matches!(ev, LifecycleEvent::SplitConflict { .. }),
      "duplicate conflict within the episode: {ev:?}"
    );
  }

  // The park heals through the twin as usual; both halves and the parent serve, and no fork
  // was ever abandoned or re-conflicted on either node.
  let g320: Vec<_> = handles.iter().map(|h| h.group(320)).collect();
  assert_eq!(
    submit_anywhere(&g320, b"tail").await,
    4,
    "3 forked + 1 tail"
  );
  let g310: Vec<_> = handles.iter().map(|h| h.group(310)).collect();
  assert_eq!(
    submit_anywhere(&g310, b"tail").await,
    3,
    "2 forked + 1 tail"
  );
  assert_eq!(submit_anywhere(&g100, b"after").await, 3, "7 - 2 - 3 + 1");
  for (i, h) in handles.iter().enumerate() {
    while let Ok(ev) = h.lifecycle().try_recv() {
      assert!(
        !matches!(
          ev,
          LifecycleEvent::SplitRefused { .. } | LifecycleEvent::SplitConflict { .. }
        ),
        "node {}: the episode delivered its one cue and healed: {ev:?}",
        i + 1
      );
    }
  }
}

/// THE HELD-MERGE BACKPRESSURE PIN, the conflict pin's merge twin: a debt window's fence
/// signal survives a momentarily-full lifecycle tail. Node 2 runs a capacity-1 tail,
/// pre-filled by split #1's own `SplitApplied` and deliberately not drained; split #2 parks on
/// node 2's squatter, arming the parent's fork fence there. A committed merge into the parent
/// then resolves on node 2 as the fence-deferred absorb — minting the capture debt whose
/// fence-hold signal is published against the full tail. Pre-fix the drain popped the
/// coordinator's once-per-transition signal and `try_send` dropped it, erasing the embedder's
/// only cue for a window that needs placement action; post-fix it stays queued until the tail
/// has room.
#[tokio::test(flavor = "multi_thread")]
async fn a_debt_windows_fence_signal_survives_a_full_lifecycle_tail() {
  const SLOW_ELECTION: Duration = Duration::from_millis(2500);
  const SLOW_HEARTBEAT: Duration = Duration::from_millis(500);
  let slow =
    |id: u64, voters: Vec<u64>| Config::try_new(id, voters, SLOW_ELECTION, SLOW_HEARTBEAT).unwrap();

  let addrs = addrs(45_420, 2);
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  for id in 1u64..=2 {
    let peers: Vec<_> = (1u64..=2)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let cfg = if id == 2 {
      DriverConfig {
        events_cap: 1,
        ..DriverConfig::default()
      }
    } else {
      DriverConfig::default()
    };
    let (driver, handle) =
      bind_node_with::<CountSm>(id, addrs[(id - 1) as usize], peers, cfg).await;
    tokio::spawn(driver.run());
    handles.push(handle);
  }
  for (i, h) in handles.iter().enumerate() {
    let id = i as u64 + 1;
    h.create_group(100, slow(id, vec![1, 2]), id, CountSm::default(), 0)
      .await
      .expect("parent admission");
    h.create_group(200, config(id, vec![1, 2]), id + 4, CountSm::default(), 0)
      .await
      .expect("source admission");
  }
  let g100: Vec<_> = handles.iter().map(|h| h.group(100)).collect();
  let g200: Vec<_> = handles.iter().map(|h| h.group(200)).collect();
  for i in 0..7u64 {
    assert_eq!(submit_anywhere(&g100, b"load").await, i + 1);
  }
  assert_eq!(submit_anywhere(&g200, b"b1").await, 1);
  assert_eq!(submit_anywhere(&g200, b"b2").await, 2);

  // Split #1 (clean): node 2's `SplitApplied` fills its one-slot tail and stays undrained.
  let _ = steer_split_to_node1(&g100, 310, b"\x02").await;
  await_lifecycle(handles[0].lifecycle(), "node 1 (split #1)", |ev| {
    matches!(
      ev,
      LifecycleEvent::SplitApplied {
        parent: 100,
        child: 310
      }
    )
  })
  .await;
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  while handles[1].group(310).status().await.is_err() {
    assert!(
      std::time::Instant::now() < deadline,
      "node 2 never materialized split #1"
    );
    tokio::time::sleep(Duration::from_millis(30)).await;
  }

  // The squatter, then split #2: node 2's fork parks and the parent's capture fence arms there.
  handles[1]
    .create_group(320, slow(2, vec![1, 2]), 9, CountSm::default(), 0)
    .await
    .expect("the squatter admits: no split names this id yet");
  let split2 = steer_split_to_node1(&g100, 320, b"\x03").await;
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  loop {
    let status = handles[1]
      .group(100)
      .status()
      .await
      .expect("node 2 hosts the parent");
    if status.applied_index >= split2 {
      break;
    }
    assert!(
      std::time::Instant::now() < deadline,
      "node 2 never applied split #2"
    );
    tokio::time::sleep(Duration::from_millis(30)).await;
  }
  for _ in 0..3 {
    let _ = handles[1].group(100).status().await;
  }

  // The merge: node 1 (fence-free once its own fork resolves) absorbs cleanly; node 2 resolves
  // the SAME committed absorb as the fence-deferred debt, publishing its fence hold against
  // the still-full tail.
  merge_verb_anywhere(
    "the freeze",
    |at| {
      let h = handles[at].clone();
      async move { h.prepare_merge(200, 100).await }
    },
    handles.len(),
  )
  .await;
  merge_verb_anywhere(
    "the commit",
    |at| {
      let h = handles[at].clone();
      async move { h.commit_merge(100, 200).await }
    },
    handles.len(),
  )
  .await;
  // Node 2 serves the union: its deferred absorb resolved (2 kept post-splits + 2 absorbed).
  let deadline = std::time::Instant::now() + Duration::from_secs(20);
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "node 2 never resolved the deferred absorb"
    );
    if let Ok(c) = handles[1].group(100).query(|sm: &CountSm| sm.count()).await
      && c == 4
    {
      break;
    }
    tokio::time::sleep(Duration::from_millis(30)).await;
  }

  // Free the slot: the deferred fence-hold signal must land — the lost-signal regression.
  await_lifecycle(
    handles[1].lifecycle(),
    "node 2 (deferred fence hold)",
    |ev| {
      matches!(
        ev,
        LifecycleEvent::MergeBlocked {
          target: 100,
          source: 200,
          ..
        }
      )
    },
  )
  .await;
}

/// Retry a leader-routed merge verb across nodes until some leader accepts it (transient
/// refusals — routing, the local source still catching up to frozen-applied — no-op and retry).
async fn merge_verb_anywhere<Fut>(what: &str, mut verb: impl FnMut(usize) -> Fut, n: usize) -> usize
where
  Fut: core::future::Future<Output = Result<sailing_proto::Index, DriverError<u64>>>,
{
  // A DirectionInverted refusal is a property of the id PAIR, not of transient state, so it can
  // never clear by retrying across nodes or time. Fail on it immediately instead of spinning out
  // the deadline, so a re-introduced direction bug surfaces as a pointed panic, not a 15s timeout.
  let inverted = format!("{:?}", sailing_proto::MergeError::<u64>::DirectionInverted);
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "{what} never accepted"
    );
    for at in 0..n {
      match verb(at).await {
        Ok(_) => return at,
        Err(DriverError::Rejected { reason }) if reason == inverted => {
          panic!(
            "{what}: the merge claim is permanently inverted — source must encode above target"
          )
        }
        Err(_) => {}
      }
    }
    tokio::time::sleep(Duration::from_millis(40)).await;
  }
}

/// Colocate `groups`' leadership onto node `to_node`, waiting until it settles there. The merge's
/// all-source-voters barrier is observable only on the source LEADER's tracker, so `commit_merge`
/// certifies it only when the absorbing target's leader also leads the source — the CRDB
/// colocate-then-merge discipline. Move the source onto the target leader BEFORE freezing (a
/// frozen source refuses a transfer; moving the source leaves the target's leadership pinned).
async fn colocate_onto(groups: &[GroupHandle<u64, u64, CountSm>], to_node: u64, what: &str) {
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "{what}: colocation never settled onto node {to_node}"
    );
    let at = find_leader(groups, what).await;
    if at as u64 + 1 == to_node {
      return;
    }
    let _ = groups[at].transfer_leader(to_node).await;
    tokio::time::sleep(Duration::from_millis(40)).await;
  }
}

/// The T11 stream merge gate: 3 nodes, two loaded groups — freeze, parked commit, per-crank
/// resolution — then the target serves the UNION, the source id refuses forever on its terminal
/// floor, and the target saw NO leadership churn through the whole choreography (same leader,
/// same term: the merge rides ordinary appends and the crank, never an election).
#[tokio::test(flavor = "multi_thread")]
async fn merge_absorbs_and_source_never_returns() {
  let addrs = addrs(44_880, 3);
  let mut handles = Vec::new();
  for id in 1u64..=3 {
    let peers: Vec<_> = (1u64..=3)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(id, addrs[(id - 1) as usize], peers).await;
    tokio::spawn(driver.run());
    handles.push(handle);
  }
  create_group_everywhere::<CountSm>(&handles, 100, &[1, 2, 3]).await;
  create_group_everywhere::<CountSm>(&handles, 200, &[1, 2, 3]).await;
  let g100: Vec<_> = handles.iter().map(|h| h.group(100)).collect();
  let g200: Vec<_> = handles.iter().map(|h| h.group(200)).collect();
  assert_eq!(submit_anywhere(&g100, b"a1").await, 1);
  assert_eq!(submit_anywhere(&g100, b"a2").await, 2);
  assert_eq!(submit_anywhere(&g200, b"b1").await, 1);
  assert_eq!(submit_anywhere(&g200, b"b2").await, 2);
  assert_eq!(submit_anywhere(&g200, b"b3").await, 3);

  // The direction rule makes the encoding-minimal id the survivor: group 100 is the target, group
  // 200 the source that dissolves (200's LE encoding sorts strictly above 100's). Pin the target's
  // leadership through the whole choreography.
  let t_leader = find_leader(&g100, "target pre-merge").await;
  let t_term = g100[t_leader].status().await.expect("status").term;
  // Colocate the source onto the target leader so the absorb can certify the freeze barrier.
  colocate_onto(&g200, t_leader as u64 + 1, "source onto target leader").await;

  merge_verb_anywhere(
    "the freeze",
    |at| {
      let h = handles[at].clone();
      async move { h.prepare_merge(200, 100).await }
    },
    handles.len(),
  )
  .await;
  merge_verb_anywhere(
    "the commit",
    |at| {
      let h = handles[at].clone();
      async move { h.commit_merge(100, 200).await }
    },
    handles.len(),
  )
  .await;

  // Every node's crank resolves its park: the union serves from the target on ALL nodes.
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "the union never served everywhere"
    );
    let mut counts = Vec::new();
    for g in &g100 {
      if let Ok(c) = g.query(|sm: &CountSm| sm.count()).await {
        counts.push(c);
      }
    }
    if counts.len() == 3 && counts.iter().all(|&c| c == 5) {
      break;
    }
    tokio::time::sleep(Duration::from_millis(40)).await;
  }

  // No leadership churn on the target: same leader, same term.
  let status = g100[t_leader].status().await.expect("status");
  assert_eq!(status.role, Role::Leader, "the target leader never moved");
  assert_eq!(status.term, t_term, "the target's term never moved");

  // The source id dies everywhere and its floor is terminal: status refuses on every node and
  // re-admission refuses at ANY generation, forever.
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "the source never tore down everywhere"
    );
    let mut gone = 0;
    for g in &g200 {
      if matches!(g.status().await, Err(DriverError::Rejected { .. })) {
        gone += 1;
      }
    }
    if gone == 3 {
      break;
    }
    tokio::time::sleep(Duration::from_millis(40)).await;
  }
  for (i, h) in handles.iter().enumerate() {
    let id = i as u64 + 1;
    match h
      .create_group(200, config(id, vec![1, 2, 3]), 9, CountSm::default(), 9)
      .await
    {
      Err(DriverError::Rejected { reason }) => {
        assert!(
          reason.contains("floor"),
          "node {id}: the refusal must be the terminal floor, got: {reason}"
        );
      }
      other => panic!("node {id}: a merged-away id must never re-admit, got {other:?}"),
    }
  }
}

/// The T11 rollback gate: the TARGET-side abort abandons a standing freeze, and the DRIVER's
/// relay drain proposes the source's thaw on its own log — the source serves fresh writes
/// again, and a LATER merge of the same pair completes, proving the abort left a clean
/// lineage. (The abort-vs-commit RACE itself is pinned deterministically at the container and
/// world tiers, where log adjacency is controllable; over a real mesh the seal wins or loses
/// by scheduling, so this gate exercises the surface, the relay, and the reuse.)
#[tokio::test(flavor = "multi_thread")]
async fn merge_rollback_unfreezes() {
  let addrs = addrs(44_940, 3);
  let mut handles = Vec::new();
  for id in 1u64..=3 {
    let peers: Vec<_> = (1u64..=3)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(id, addrs[(id - 1) as usize], peers).await;
    tokio::spawn(driver.run());
    handles.push(handle);
  }
  create_group_everywhere::<CountSm>(&handles, 100, &[1, 2, 3]).await;
  create_group_everywhere::<CountSm>(&handles, 200, &[1, 2, 3]).await;
  let g100: Vec<_> = handles.iter().map(|h| h.group(100)).collect();
  let g200: Vec<_> = handles.iter().map(|h| h.group(200)).collect();
  assert_eq!(submit_anywhere(&g100, b"a1").await, 1);
  assert_eq!(submit_anywhere(&g200, b"b1").await, 1);

  // The direction rule makes the encoding-minimal id the survivor: group 100 is the target, group
  // 200 the source that dissolves and thaws (200's LE encoding sorts strictly above 100's).
  merge_verb_anywhere(
    "the freeze",
    |at| {
      let h = handles[at].clone();
      async move { h.prepare_merge(200, 100).await }
    },
    handles.len(),
  )
  .await;
  // The frozen source refuses writes typed.
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "the freeze never took effect"
    );
    let mut frozen = false;
    for g in &g200 {
      if matches!(g.submit(Bytes::from_static(b"x")).await, Err(DriverError::Rejected { reason }) if reason.contains("Frozen"))
      {
        frozen = true;
        break;
      }
    }
    if frozen {
      break;
    }
    tokio::time::sleep(Duration::from_millis(40)).await;
  }

  // Abort the standing freeze from the TARGET side (retrying across nodes for the target
  // leader whose local source has applied the freeze).
  merge_verb_anywhere(
    "the abort",
    |at| {
      let h = handles[at].clone();
      async move { h.rollback_merge(100, 200).await }
    },
    handles.len(),
  )
  .await;

  // The relayed thaw lands on the source's own log: it accepts fresh writes again everywhere.
  assert_eq!(submit_anywhere(&g200, b"a2").await, 2);
  // Both groups intact: nothing was absorbed.
  assert_eq!(query_anywhere(&g100).await, 1, "no union — the merge died");

  // A LATER merge of the same pair completes end to end: the abort left a clean lineage. Colocate
  // the (now thawed) source onto the target leader again so the fresh absorb can certify the barrier.
  let t2_leader = find_leader(&g100, "target pre-second-merge").await;
  colocate_onto(
    &g200,
    t2_leader as u64 + 1,
    "source onto target leader (second)",
  )
  .await;
  merge_verb_anywhere(
    "the fresh freeze",
    |at| {
      let h = handles[at].clone();
      async move { h.prepare_merge(200, 100).await }
    },
    handles.len(),
  )
  .await;
  merge_verb_anywhere(
    "the fresh commit",
    |at| {
      let h = handles[at].clone();
      async move { h.commit_merge(100, 200).await }
    },
    handles.len(),
  )
  .await;
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "the second merge never completed"
    );
    let mut gone = 0;
    for g in &g200 {
      if matches!(g.status().await, Err(DriverError::Rejected { .. })) {
        gone += 1;
      }
    }
    if gone == 3 && query_anywhere(&g100).await == 3 {
      break;
    }
    tokio::time::sleep(Duration::from_millis(40)).await;
  }
}

/// The direction rule is a permanent, state-independent refusal: an inverted claim (source
/// encoding strictly BELOW the target) is rejected typed on the FIRST call, before any leadership
/// consideration and with nothing appended — the id pair, not any mutable state, decides it, so it
/// can never self-clear and must never be retried.
#[tokio::test(flavor = "multi_thread")]
async fn prepare_merge_refuses_an_inverted_claim() {
  let addrs = addrs(44_900, 3);
  let mut handles = Vec::new();
  for id in 1u64..=3 {
    let peers: Vec<_> = (1u64..=3)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(id, addrs[(id - 1) as usize], peers).await;
    tokio::spawn(driver.run());
    handles.push(handle);
  }
  create_group_everywhere::<CountSm>(&handles, 100, &[1, 2, 3]).await;
  create_group_everywhere::<CountSm>(&handles, 200, &[1, 2, 3]).await;

  // source 100 encodes strictly BELOW target 200: the direction gate is the earliest,
  // constant-vs-constant check, so the host of the source refuses on the first call regardless of
  // who leads. `map_merge_err` carries the variant's Debug form as the rejection reason.
  let inverted = format!("{:?}", sailing_proto::MergeError::<u64>::DirectionInverted);
  match handles[0].prepare_merge(100, 200).await {
    Err(DriverError::Rejected { reason }) => {
      assert_eq!(reason, inverted, "the refusal must be DirectionInverted");
    }
    other => panic!("an inverted claim must refuse typed on the first call, got {other:?}"),
  }
}

/// A query closure that PANICS is caught at the handle seam — the caller gets `QueryPanicked` and the
/// driver task does NOT unwind. But the caught panic is UNATTRIBUTABLE: the closure captured arbitrary
/// state that can alias ANY hosted group's FSM, so it FAIL-STOPS THE WHOLE PLANE — the group it read
/// against AND every co-located group poison, rather than risk one serving silently-divergent state.
/// Each poison surfaces on the lifecycle tail; the driver survives (a fail-stop, never an unwind).
#[tokio::test(flavor = "multi_thread")]
async fn a_panicking_query_fails_typed_and_the_driver_survives() {
  let addr: SocketAddr = "127.0.0.1:44800".parse().unwrap();
  let (driver, handle) = bind_node::<CountSm>(1, addr, Vec::new()).await;
  tokio::spawn(driver.run());

  for gid in [100u64, 200] {
    handle
      .create_group(gid, config(1, vec![1]), 1, CountSm::default(), 0)
      .await
      .expect("group admission");
  }
  let g100 = handle.group(100);
  let g200 = handle.group(200);
  assert_eq!(submit_anywhere(std::slice::from_ref(&g100), b"x").await, 1);

  // The panic is caught at the handle seam: the caller gets QueryPanicked, the driver task does
  // NOT unwind.
  match g100
    .query(|_: &CountSm| -> u64 { panic!("boom in query") })
    .await
  {
    Err(DriverError::QueryPanicked) => {}
    other => panic!("expected QueryPanicked, got {other:?}"),
  }

  // Plane-fatal, not group-scoped: the caught panic is UNATTRIBUTABLE, so BOTH the read's group AND the
  // co-located sibling fail-stop. Poll status (which cranks the spawned driver) until each reports
  // poisoned — RED before the fail-stop wiring (a caught panic kept the groups serving, no poison). The
  // driver survives the panic; every hosted group poisons (the reactor unit tests pin the matching
  // `LifecycleEvent::Poisoned` surfacing for both groups).
  await_poisoned(&g100, "the query-panicked group 100").await;
  await_poisoned(&g200, "the co-located sibling group 200").await;
}

/// A caught factory panic QUARANTINES the factory — permanently removed, so the plane then behaves
/// exactly as a driver with no factory — rather than mapping to a one-shot decline that leaves the
/// SAME `&mut` factory installed for the next solicitation. WHY the stronger cure: the factory is
/// the admission authority for a group's consensus voter set, and `&mut GroupFactory` is not
/// unwind-safe. A factory that mutates internal state and THEN panics can, on a LATER call, return a
/// valid-LOOKING blueprint that names the solicitor but carries a wrong voter set — which clears
/// every downstream gate (solicitor-naming, floors, split-reservation, the create admission, none
/// of which check voter-set semantics) and admits a broken quorum. This factory models the torn
/// authority: `materialize` bumps a counter then panics on the FIRST group-100 solicitation, and
/// every SUBSEQUENT call returns a full-voter blueprint naming the solicitor that WOULD admit. After
/// the caught panic the factory is never consulted again (the counter stays 1), group 100 stays
/// un-hosted, and its solicitation surfaces as `UnknownGroup`. The panic is ALSO plane-fatal — a torn
/// factory could have aliased a hosted FSM — so node 2's co-hosted sibling fail-stops (quarantine
/// prevents reuse; the plane fail-stop covers the tear). RED before the quarantine: the retained
/// factory's second call admits group 100.
#[tokio::test(flavor = "multi_thread")]
async fn a_materialize_panic_quarantines_the_factory_and_falls_through() {
  let addrs = addrs(44_820, 2);
  let materialized = Arc::new(AtomicUsize::new(0));
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  for id in 1u64..=2 {
    let peers: Vec<_> = (1u64..=2)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(id, addrs[(id - 1) as usize], peers).await;
    if id == 2 {
      let materialized = materialized.clone();
      let driver = driver.with_group_factory(factory_fn(
        move |group: &u64, from: &u64| -> Option<GroupBlueprint<u64>> {
          if *group != 100 {
            return None;
          }
          // Mutate, THEN tear: the first solicitation bumps the counter and panics; a RETAINED
          // factory's later call returns a valid-looking blueprint naming the solicitor and admits.
          if materialized.fetch_add(1, Ordering::SeqCst) == 0 {
            panic!("boom in materialize");
          }
          [1u64, 2]
            .contains(from)
            .then(|| GroupBlueprint::new(config(2, vec![1, 2]), 2))
        },
        |_group: &u64| Some(CountSm::default()),
      ));
      tokio::spawn(driver.run());
    } else {
      tokio::spawn(driver.run());
    }
    handles.push(handle);
  }
  // The sibling group binds the mesh and proves node 2 is alive throughout.
  create_group_everywhere(&handles, 900, &[1, 2]).await;
  let g900: Vec<_> = handles.iter().map(|h| h.group(900)).collect();
  assert_eq!(submit_anywhere(&g900, b"seed").await, 1);

  // Group 100 exists only on node 1; its first solicitation makes node 2's materialize panic —
  // caught, quarantining the factory — so the signal falls through to the lifecycle tail.
  handles[0]
    .create_group(100, config(1, vec![1, 2]), 1, CountSm::default(), 0)
    .await
    .expect("group 100 admitted on node 1");
  await_lifecycle(
    handles[1].lifecycle(),
    "the panicked-factory solicitation",
    |ev| {
      matches!(
        ev,
        LifecycleEvent::UnknownGroup {
          group: 100,
          from: 1
        }
      )
    },
  )
  .await;

  // The quarantine holds across every retry: node 1 keeps soliciting, but a removed factory is
  // never consulted again, so group 100 never materializes. A RETAINED factory admits here on its
  // second call — which is exactly the RED this asserts against.
  let deadline = std::time::Instant::now() + Duration::from_secs(3);
  while std::time::Instant::now() < deadline {
    assert!(
      handles[1].group(100).status().await.is_err(),
      "a quarantined factory must materialize nothing"
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
  }
  assert_eq!(
    materialized.load(Ordering::SeqCst),
    1,
    "the factory was consulted again after a caught panic — not quarantined"
  );

  // The panic is PLANE-FATAL: node 2's driver survives, but its co-hosted sibling fail-stops (a torn
  // factory could have aliased the sibling's FSM). Node 1's replica stays alive — only node 2's plane
  // poisoned.
  await_poisoned(&handles[1].group(900), "node 2's co-hosted sibling").await;
}

/// The build-phase twin of the materialize quarantine. This factory's cheap `materialize` always
/// returns a valid blueprint naming the solicitor, but its `build` (the resource phase) bumps a
/// counter then panics on the FIRST admitted solicitation. A caught build panic quarantines the
/// factory exactly as a materialize panic does — the torn `&mut` admission authority is the same —
/// so `build` is never reached again (the counter stays 1), group 100 never materializes, and the
/// solicitation surfaces as `UnknownGroup`. The panic is ALSO plane-fatal — a torn factory could have
/// aliased a hosted FSM — so node 2's co-hosted sibling fail-stops. RED before the quarantine: the
/// retained factory's second solicitation builds and admits.
#[tokio::test(flavor = "multi_thread")]
async fn a_build_panic_quarantines_the_factory_and_falls_through() {
  let addrs = addrs(45_400, 2);
  let built = Arc::new(AtomicUsize::new(0));
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  for id in 1u64..=2 {
    let peers: Vec<_> = (1u64..=2)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(id, addrs[(id - 1) as usize], peers).await;
    if id == 2 {
      let built = built.clone();
      let driver = driver.with_group_factory(factory_fn(
        move |group: &u64, from: &u64| {
          (*group == 100 && [1u64, 2].contains(from))
            .then(|| GroupBlueprint::new(config(2, vec![1, 2]), 2))
        },
        move |_group: &u64| -> Option<CountSm> {
          // Mutate, THEN tear: the first admitted solicitation bumps the counter and panics in the
          // resource phase; a RETAINED factory's later build succeeds and admits.
          if built.fetch_add(1, Ordering::SeqCst) == 0 {
            panic!("boom in build");
          }
          Some(CountSm::default())
        },
      ));
      tokio::spawn(driver.run());
    } else {
      tokio::spawn(driver.run());
    }
    handles.push(handle);
  }
  // The sibling group binds the mesh and proves node 2 is alive throughout.
  create_group_everywhere(&handles, 900, &[1, 2]).await;
  let g900: Vec<_> = handles.iter().map(|h| h.group(900)).collect();
  assert_eq!(submit_anywhere(&g900, b"seed").await, 1);

  // Group 100 exists only on node 1; its first solicitation admits the blueprint, then node 2's
  // build panics — caught, quarantining the factory — so the signal falls through to the tail.
  handles[0]
    .create_group(100, config(1, vec![1, 2]), 1, CountSm::default(), 0)
    .await
    .expect("group 100 admitted on node 1");
  await_lifecycle(
    handles[1].lifecycle(),
    "the build-panicked solicitation",
    |ev| {
      matches!(
        ev,
        LifecycleEvent::UnknownGroup {
          group: 100,
          from: 1
        }
      )
    },
  )
  .await;

  // The quarantine holds across every retry: a removed factory's build is never reached again, so
  // group 100 never materializes. A RETAINED factory builds and admits on its second solicitation.
  let deadline = std::time::Instant::now() + Duration::from_secs(3);
  while std::time::Instant::now() < deadline {
    assert!(
      handles[1].group(100).status().await.is_err(),
      "a quarantined factory must materialize nothing"
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
  }
  assert_eq!(
    built.load(Ordering::SeqCst),
    1,
    "the build phase ran again after a caught panic — the factory was not quarantined"
  );

  // The panic is PLANE-FATAL: node 2's driver survives, but its co-hosted sibling fail-stops (a torn
  // factory could have aliased the sibling's FSM). Node 1's replica stays alive — only node 2's plane
  // poisoned.
  await_poisoned(&handles[1].group(900), "node 2's co-hosted sibling").await;
}

/// Integration through the real driver scheduler: a removed voter's farewell budget survives
/// quiescence. A clean 3-voter majority {1,2,3} elects and commits; a present follower is removed.
/// With the both-arms fix, removing a PRESENT voter always populates the retry budget — its farewell
/// (a caught-up commit-carrying heartbeat, or an append if it lagged the commit) is re-driven on a
/// bounded BLIND budget that drains by SHOT COUNT, not by an ack — so the leader holds the group
/// quiesce-INELIGIBLE across the one-election-timeout window it would otherwise have quiesced in, even
/// though the initial farewell was delivered. The removed peer applies its own removal and surfaces
/// RemovedSelf (so it never campaigns), the live pair keeps its term (no disruption), and once the
/// budget drains the group quiesces.
#[tokio::test(flavor = "multi_thread")]
async fn a_removed_voter_holds_quiescence_through_the_farewell_budget() {
  const SLOW_ELECTION: Duration = Duration::from_millis(1500);
  const SLOW_HEARTBEAT: Duration = Duration::from_millis(300);

  let addrs = addrs(46_000, 3);
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  let mut metrics = Vec::new();
  for id in 1u64..=3 {
    let peers: Vec<_> = (1u64..=3)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(id, addrs[(id - 1) as usize], peers).await;
    metrics.push(driver.engine_metrics());
    tokio::spawn(driver.run());
    handles.push(handle);
  }
  for (i, h) in handles.iter().enumerate() {
    h.create_group(
      100,
      Config::try_new(i as u64 + 1, vec![1, 2, 3], SLOW_ELECTION, SLOW_HEARTBEAT).unwrap(),
      i as u64 + 1, // distinct election-jitter seed per node (identical seeds → perpetual split vote)
      CountSm::default(),
      0,
    )
    .await
    .expect("group 100 admitted");
  }
  let g100: Vec<_> = handles.iter().map(|h| h.group(100)).collect();
  assert_eq!(submit_anywhere(&g100, b"a").await, 1);
  let leader = find_leader(&g100, "group 100 leader").await;
  let leader_id = leader as u64 + 1;
  let victim_id = (1u64..=3).find(|&v| v != leader_id).unwrap();
  let victim_idx = (victim_id - 1) as usize;
  let leader_term = g100[leader].status().await.expect("status").term;

  // Remove a present follower. `armed` is the observable origin of the retry budget: the leader's
  // apply-fold arms it in this call's own crank, so measuring the hold from here is at worst one
  // crank early — which understates the hold and can only make the floor below easier to clear.
  let armed = std::time::Instant::now();
  g100[leader]
    .conf_change(ConfChange::new(
      ConfChangeType::RemoveNode,
      victim_id,
      Bytes::new(),
    ))
    .await
    .expect("removal proposed");

  // The removed peer applies its own removal and self-removes (so it never campaigns).
  await_lifecycle(
    handles[victim_idx].lifecycle(),
    "the removed voter self-removes",
    |ev| matches!(ev, LifecycleEvent::RemovedSelf { group } if *group == 100),
  )
  .await;

  // The hold is a STATE invariant, so assert on state: poll until the gauge FIRST flips, then
  // check WHEN it flipped. A premature quiesce breaks this loop early and shows up as a short
  // hold; the deadline is a hang guard, never the assertion.
  //
  // The floor is what the budget is FOR, derived from the driver's own eligibility rule rather
  // than from a chosen sleep. The quiesce sweep's inactivity clock starts at the last
  // `(term, commit, applied)` change — the removal's own commit+apply — so a group with NO pending
  // farewell becomes eligible exactly one election timeout later. `group_idle` refuses while
  // `has_pending_farewells()` holds, and the budget's last shot cannot fire before the leader tick
  // one election timeout after the front-loaded shot 2. So the flip must land at least one further
  // leader tick past the farewell-free baseline: anything at or under `SLOW_ELECTION` would mean
  // the budget extended nothing. Only the direction matters for stability — a slow machine delays
  // the flip, never hastens it, and `armed` is taken before the conf change so it can only
  // overstate the hold.
  let hold_floor = SLOW_ELECTION + SLOW_HEARTBEAT;
  let guard = armed + Duration::from_secs(30);
  while metrics[leader].quiesced_groups() == 0 {
    assert!(
      std::time::Instant::now() < guard,
      "group 100 never quiesced: the farewell budget never drained"
    );
    tokio::time::sleep(Duration::from_millis(20)).await;
  }
  let held = armed.elapsed();
  assert!(
    held >= hold_floor,
    "the budget must hold the group quiesce-ineligible past the farewell-free baseline \
     ({SLOW_ELECTION:?} of inactivity): quiesced after {held:?}, floor {hold_floor:?}"
  );
  // No disruption: the live pair kept its term (the removed voter self-removed, never campaigned).
  assert_eq!(
    g100[leader].status().await.expect("status").term,
    leader_term,
    "the live group kept its term"
  );

  // Once the budget drains, the group becomes idle-eligible and quiesces through the real scheduler.
  wait_for_quiesced(
    &metrics[leader],
    1,
    "group 100 quiesces after the farewell budget drains",
  )
  .await;
}

/// FENCE INERTNESS over a real transport: a retired incarnation's frames are DROPPED at demux on
/// a resolved member — every class, before the endpoint sees them — so the husk cannot depose,
/// cannot replicate, and cannot be heard at all; the observability counter names it.
///
/// Node 2 walks the real re-admission path (create at generation 5 → remove, which persists the
/// removal floor → clear the tombstone → create at 6), so its engine carries a DURABLE floor of 6
/// and a live incarnation at 6. Node 1 hosts the SAME gid at generation 0 — a husk of the
/// incarnation that floor retired — and campaigns hard at a fresh term. Its frames reach node 2's
/// socket (the address book is bind-time and group-agnostic: `reconcile_peer_links` keeps the link
/// up regardless of membership) and are fenced there, which is exactly what makes the husk
/// structurally inert rather than merely eventually reaped.
#[tokio::test(flavor = "multi_thread")]
async fn a_retired_incarnations_frames_are_fenced_at_a_resolved_member() {
  let addrs = addrs(45_040, 2);
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  let mut fenced = None;
  for id in 1u64..=2 {
    let peers: Vec<_> = (1u64..=2)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(id, addrs[(id - 1) as usize], peers).await;
    if id == 2 {
      fenced = Some(driver.engine_metrics());
    }
    tokio::spawn(driver.run());
    handles.push(handle);
  }
  let fenced = fenced.expect("node 2's metrics");
  let election = Duration::from_millis(200);
  let heartbeat = Duration::from_millis(40);

  // Node 2 reaches a floored, re-admitted incarnation through the public lifecycle path. It is a
  // non-member OBSERVER, so it never campaigns and its own term never moves — every term it could
  // ever show would have to have come from the husk.
  let observer = || Config::try_new_observer(2u64, vec![1u64], election, heartbeat).unwrap();
  handles[1]
    .create_group(100, observer(), 2, CountSm::default(), 5)
    .await
    .expect("first incarnation admits");
  assert!(
    handles[1]
      .remove_group(100)
      .await
      .expect("removal resolves"),
    "node 2 hosted the first incarnation"
  );
  assert!(
    handles[1]
      .clear_tombstone(100)
      .await
      .expect("clear resolves"),
    "the tombstone was set by the removal"
  );
  handles[1]
    .create_group(100, observer(), 2, CountSm::default(), 6)
    .await
    .expect("the successor admits above its floor");

  // Node 1 is the HUSK: the same gid at the generation the floor retired, shaped so it keeps
  // campaigning forever (it needs node 2's vote and will never be granted one).
  handles[0]
    .create_group(
      100,
      Config::try_new(1u64, vec![1u64, 2], election, heartbeat).unwrap(),
      1,
      CountSm::default(),
      0,
    )
    .await
    .expect("the husk admits locally");
  let husk = handles[0].group(100);
  let resolved = handles[1].group(100);

  // Every frame the husk emits must die at node 2's demux.
  let deadline = std::time::Instant::now() + Duration::from_secs(10);
  while fenced.fenced_frames_dropped() == 0 {
    assert!(
      std::time::Instant::now() < deadline,
      "the husk's frames never reached the fence — the link or the stamp is wrong"
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
  }
  tokio::time::sleep(Duration::from_millis(600)).await;
  let after = resolved.status().await.expect("status");
  assert_eq!(
    after.term,
    sailing_proto::Term::ZERO,
    "the husk's campaigns never moved the resolved member's term"
  );
  assert!(
    after.leader.is_none(),
    "and never made it follow a retired incarnation"
  );
  assert!(
    fenced.fenced_frames_dropped() > 1,
    "the fence counter keeps naming the husk, beat after beat"
  );
  // The husk campaigns into the void: never granted, never leading.
  assert_ne!(
    husk.status().await.expect("status").role,
    Role::Leader,
    "a fenced husk can never assemble a quorum"
  );
}

/// THE COURTESY CURE end to end over a real transport, PRE-VOTE variant — the announce-without-
/// inflating shape, where the victim's probes reach the leader without ever putting themselves
/// above its term, so the very first offer is deliverable. The default-flags variant, where the
/// victim campaigns for real and the cure has to wait for a term lift, is its sibling below.
///
/// It must be the COURTESY arm that cures: a `RemovedSelf` alone would also be satisfied by the
/// farewell append, a different mechanism with different coverage, so the victim's
/// `SnapshotInstalled` is the assertion — only an install re-baselines it, and the only install it
/// can receive is the offer.
///
/// The transport leg is the other half: the leader still has a route to a peer its configuration
/// no longer names, because the driver's address book is bind-time and group-agnostic
/// (`reconcile_peer_links` keeps the link up regardless of membership).
#[tokio::test(flavor = "multi_thread")]
async fn a_courtesy_snapshot_cures_a_removed_peer_over_tcp() {
  let addrs = addrs(45_050, 3);
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  for id in 1u64..=3 {
    let peers: Vec<_> = (1u64..=3)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(id, addrs[(id - 1) as usize], peers).await;
    tokio::spawn(driver.run());
    handles.push(handle);
  }
  for id in 1u64..=3 {
    let base = Config::try_new(
      id,
      vec![1u64, 2, 3],
      Duration::from_millis(300),
      Duration::from_millis(60),
    )
    .unwrap()
    .with_snapshot_threshold(2)
    .with_pre_vote(true);
    handles[(id - 1) as usize]
      .create_group(100, base, id, CountSm::default(), 0)
      .await
      .expect("group admission");
  }
  let g100: Vec<_> = (0..3).map(|i| handles[i].group(100)).collect();
  let leader = find_leader(&g100, "group 100").await;
  let victim = (0..3).find(|&i| i != leader).expect("a non-leader");

  // Commit load either side of the removal so the leader captures and compacts real snapshots and
  // the offer it makes carries the post-removal configuration.
  for _ in 0..8 {
    let _ = g100[leader].submit(Bytes::from_static(b"load")).await;
  }
  let cc = ConfChange::new(ConfChangeType::RemoveNode, victim as u64 + 1, Bytes::new());
  g100[leader].conf_change(cc).await.expect("removal commits");
  for _ in 0..8 {
    let _ = g100[leader].submit(Bytes::from_static(b"load")).await;
  }

  // The victim learns — and a SNAPSHOT is what taught it.
  await_lifecycle(handles[victim].lifecycle(), "courtesy removed-self", |ev| {
    matches!(ev, LifecycleEvent::RemovedSelf { group: 100 })
  })
  .await;
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  let mut installed = false;
  while !installed {
    while let Ok((gid, ev)) = handles[victim].events().try_recv() {
      if gid == 100 && matches!(ev, Event::SnapshotInstalled(_)) {
        installed = true;
      }
    }
    if installed {
      break;
    }
    assert!(
      std::time::Instant::now() < deadline,
      "the victim was cured, but not by a snapshot install — the courtesy arm did not run"
    );
    tokio::time::sleep(Duration::from_millis(30)).await;
  }
  assert_ne!(
    g100[victim].status().await.expect("status").role,
    Role::Leader,
    "the cured peer never leads the group it was removed from"
  );
}

/// F1 OVER A REAL TRANSPORT: a joiner's catch-up snapshot predates its own `AddNode`, so the
/// ConfState it installs cannot name it — and that must mean HISTORY, not removal. The joiner
/// installs, surfaces NO `RemovedSelf`, replays the entries after the boundary that admit it, and
/// ends a serving member of the group.
///
/// The shape is the ordinary one, not a contrivance: the two-node group compacts (threshold 2)
/// while node 3 is absent, so the leader's durable snapshot sits at a boundary BELOW the
/// `AddNode(3)` that follows — and a zero-progress joiner is structurally forced onto the snapshot
/// path. Node 3 boots as the OBSERVER the factory and fork paths mandate (self absent from its own
/// bootstrap voters), which is exactly the shape whose prior configuration does not name it.
///
/// MUTATION: key the install's removal event on absence from the installed ConfState again → the
/// joiner is told it was removed mid-join and the lifecycle assertion fails.
#[tokio::test(flavor = "multi_thread")]
async fn a_joiner_whose_catch_up_snapshot_predates_its_add_is_not_removed() {
  let addrs = addrs(45_060, 3);
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  for id in 1u64..=3 {
    let peers: Vec<_> = (1u64..=3)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(id, addrs[(id - 1) as usize], peers).await;
    tokio::spawn(driver.run());
    handles.push(handle);
  }
  let election = Duration::from_millis(300);
  let heartbeat = Duration::from_millis(60);
  // The group is {1, 2}; node 3 is not a member yet.
  for id in 1u64..=2 {
    handles[(id - 1) as usize]
      .create_group(
        100,
        Config::try_new(id, vec![1u64, 2], election, heartbeat)
          .unwrap()
          .with_snapshot_threshold(2),
        id,
        CountSm::default(),
        0,
      )
      .await
      .expect("group admission");
  }
  let g12: Vec<_> = (0..2).map(|i| handles[i].group(100)).collect();
  let leader = find_leader(&g12, "group 100").await;

  // Commit and compact while node 3 is absent: the durable snapshot's ConfState is {1, 2}.
  for _ in 0..10 {
    let _ = g12[leader].submit(Bytes::from_static(b"load")).await;
  }

  // Node 3 now boots as a NON-MEMBER observer and is added. Its catch-up snapshot predates the add.
  handles[2]
    .create_group(
      100,
      Config::try_new_observer(3u64, vec![1u64, 2], election, heartbeat)
        .unwrap()
        .with_snapshot_threshold(2),
      3,
      CountSm::default(),
      0,
    )
    .await
    .expect("the observer joiner admits");
  g12[leader]
    .conf_change(ConfChange::new(ConfChangeType::AddNode, 3u64, Bytes::new()))
    .await
    .expect("AddNode(3) commits");

  // It catches up and becomes a serving member — and is never told it was removed. `installed`
  // proves the catch-up really went through the SNAPSHOT path (a log-only catch-up would make the
  // whole regression vacuous, since the install rule would never run).
  let mut installed = false;
  let deadline = std::time::Instant::now() + Duration::from_secs(20);
  loop {
    while let Ok((gid, ev)) = handles[2].events().try_recv() {
      if gid == 100 && matches!(ev, Event::SnapshotInstalled(_)) {
        installed = true;
      }
    }
    if let Ok(st) = handles[2].group(100).status().await
      && installed
      && st.conf_state.voters().contains(&3u64)
      && st.commit_index > Index::ZERO
      && st.applied_index == st.commit_index
    {
      break;
    }
    assert!(
      std::time::Instant::now() < deadline,
      "the joiner never became a serving member"
    );
    while let Ok(ev) = handles[2].lifecycle().try_recv() {
      assert!(
        !matches!(ev, LifecycleEvent::RemovedSelf { group: 100 }),
        "a snapshot that predates the joiner's admission is history, not a removal: {ev:?}"
      );
    }
    tokio::time::sleep(Duration::from_millis(30)).await;
  }
  assert!(
    installed,
    "the joiner caught up by snapshot — the install rule actually ran"
  );
  while let Ok(ev) = handles[2].lifecycle().try_recv() {
    assert!(
      !matches!(ev, LifecycleEvent::RemovedSelf { group: 100 }),
      "no removal may surface even after the joiner is serving: {ev:?}"
    );
  }
  // It really is serving: a fresh commit reaches it.
  let before = handles[2]
    .group(100)
    .status()
    .await
    .expect("status")
    .commit_index;
  let _ = g12[leader].submit(Bytes::from_static(b"after")).await;
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  loop {
    let st = handles[2].group(100).status().await.expect("status");
    if st.commit_index > before {
      break;
    }
    assert!(
      std::time::Instant::now() < deadline,
      "the joined member stopped receiving commits"
    );
    tokio::time::sleep(Duration::from_millis(30)).await;
  }
}

/// THE COURTESY CURE at the etcd-parity DEFAULTS (pre_vote and check_quorum both OFF) over a real
/// transport — the shape the round-3 finding was about. The removed peer campaigns for REAL, so it
/// puts itself above the leader's term and every offer that leader could stamp would die at the
/// peer's own stale-term pre-pass. The leader drops those campaigns without stepping down and
/// without spending its budget; the peer's own campaigns lift the group's term through the LIVE
/// members; and whichever member then leads offers proactively at a term the peer accepts.
///
/// The assertion is the courtesy arm specifically (`SnapshotInstalled`), and the cure must arrive
/// with the group's term having moved — which is the visible signature of the delegation.
#[tokio::test(flavor = "multi_thread")]
async fn a_courtesy_snapshot_cures_a_removed_peer_at_default_flags_over_tcp() {
  let addrs = addrs(45_070, 3);
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  for id in 1u64..=3 {
    let peers: Vec<_> = (1u64..=3)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(id, addrs[(id - 1) as usize], peers).await;
    tokio::spawn(driver.run());
    handles.push(handle);
  }
  for id in 1u64..=3 {
    handles[(id - 1) as usize]
      .create_group(
        100,
        Config::try_new(
          id,
          vec![1u64, 2, 3],
          Duration::from_millis(300),
          Duration::from_millis(60),
        )
        .unwrap()
        .with_snapshot_threshold(2),
        id,
        CountSm::default(),
        0,
      )
      .await
      .expect("group admission");
  }
  let g100: Vec<_> = (0..3).map(|i| handles[i].group(100)).collect();
  let leader = find_leader(&g100, "group 100").await;
  let victim = (0..3).find(|&i| i != leader).expect("a non-leader");

  for _ in 0..8 {
    let _ = g100[leader].submit(Bytes::from_static(b"load")).await;
  }
  let cc = ConfChange::new(ConfChangeType::RemoveNode, victim as u64 + 1, Bytes::new());
  g100[leader].conf_change(cc).await.expect("removal commits");
  for _ in 0..8 {
    let _ = g100[leader].submit(Bytes::from_static(b"load")).await;
  }

  // The cure lands, and a snapshot install is what delivered it.
  await_lifecycle(
    handles[victim].lifecycle(),
    "default-flags removed-self",
    |ev| matches!(ev, LifecycleEvent::RemovedSelf { group: 100 }),
  )
  .await;
  let deadline = std::time::Instant::now() + Duration::from_secs(20);
  let mut installed = false;
  while !installed {
    while let Ok((gid, ev)) = handles[victim].events().try_recv() {
      if gid == 100 && matches!(ev, Event::SnapshotInstalled(_)) {
        installed = true;
      }
    }
    if installed {
      break;
    }
    assert!(
      std::time::Instant::now() < deadline,
      "the victim was cured, but not by a snapshot install — the courtesy arm did not run"
    );
    tokio::time::sleep(Duration::from_millis(30)).await;
  }
  assert_ne!(
    g100[victim].status().await.expect("status").role,
    Role::Leader,
    "the cured peer never leads the group it was removed from"
  );
  // The surviving members kept serving throughout: a fresh command still commits.
  let survivor = (0..3).find(|&i| i != victim).expect("a surviving member");
  let deadline = std::time::Instant::now() + Duration::from_secs(20);
  loop {
    if g100[survivor]
      .submit(Bytes::from_static(b"after"))
      .await
      .is_ok()
    {
      break;
    }
    assert!(
      std::time::Instant::now() < deadline,
      "the live group never resumed committing"
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
  }
}
