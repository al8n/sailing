//! Real-socket integration for the MULTI-GROUP reactor stream driver: loopback-TCP hosts carrying
//! several Raft groups over shared connections and one shared storage engine, on a multi-thread
//! tokio runtime (which proves the `Send` `run()`). Groups arrive at runtime through
//! `MultiHandle::create_group`; the suites assert cross-group isolation, the engine's batched
//! durability barrier, group removal failing exactly its own parked work, and one poisoned group
//! leaving its co-hosted groups committing.

mod common;

use std::{net::SocketAddr, sync::Arc, time::Duration};

use agnostic::tokio::TokioRuntime;
use bytes::Bytes;
use common::{CountSm, TrapSm};
use sailing_proto::{
  ClusterId, ConfChange, ConfChangeType, Config, Data, LabelOptions, Labeled, Passthrough, Role,
};
use sailing_reactor::{
  DriverConfig, DriverError, GroupHandle, LifecycleEvent, MultiHandle, MultiReactorStreamDriver,
  Node,
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
  let (dialer, acceptor) = plain_factories(id);
  MDriver::<F>::bind(addr, peers, dialer, acceptor, DriverConfig::default())
    .await
    .expect("the empty multi host binds")
}

fn config(id: u64, voters: Vec<u64>) -> Config<u64> {
  Config::try_new(id, voters, ELECTION, HEARTBEAT).unwrap()
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
    h.create_group(gid, config(id, voters.to_vec()), id, F::default())
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
    .create_group(100, config(1, vec![1]), 1, CountSm::default())
    .await
    .expect("group 100 admitted");
  handle
    .create_group(200, config(1, vec![1]), 2, CountSm::default())
    .await
    .expect("group 200 admitted");
  let g100 = handle.group(100);
  let g200 = handle.group(200);

  // Warm up: both single-voter groups elect and commit once.
  assert_eq!(submit_anywhere(std::slice::from_ref(&g100), b"w").await, 1);
  assert_eq!(submit_anywhere(std::slice::from_ref(&g200), b"w").await, 1);

  let flushes0 = metrics.flushes();
  let ops0 = metrics.ops_flushed();

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

  let flushes1 = metrics.flushes();
  let ops1 = metrics.ops_flushed();
  assert!(flushes1 > 0, "the barrier ran");
  assert!(
    ops1 - ops0 > flushes1 - flushes0,
    "the burst amortized: {} ops rode {} barriers",
    ops1 - ops0,
    flushes1 - flushes0
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
    .create_group(100, config(1, vec![1]), 1, TrapSm::default())
    .await
    .expect("group 100 admitted");
  handle
    .create_group(200, config(1, vec![1]), 2, TrapSm::default())
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
    .failover_query(|_fsm: &TrapSm, _limbo: &[sailing_proto::Entry], _win| Some(9))
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

/// Group admission is validated LOUDLY: duplicate ids, a config whose node id contradicts the
/// latched host identity, and a walled failover-tier config on this monotonic-only host are all
/// typed rejections — and a removed id can be re-admitted under the same host identity.
#[tokio::test(flavor = "multi_thread")]
async fn lifecycle_admission_errors_are_typed() {
  let addr: SocketAddr = "127.0.0.1:44180".parse().unwrap();
  let (driver, handle) = bind_node::<CountSm>(1, addr, Vec::new()).await;
  tokio::spawn(driver.run());

  handle
    .create_group(100, config(1, vec![1]), 1, CountSm::default())
    .await
    .expect("first admission latches the host identity");

  // Duplicate group id.
  match handle
    .create_group(100, config(1, vec![1]), 1, CountSm::default())
    .await
  {
    Err(DriverError::Rejected { reason }) => {
      assert!(reason.contains("already exists"), "got: {reason}");
    }
    other => panic!("expected the duplicate-id rejection, got {other:?}"),
  }

  // A config whose node id contradicts the latched host identity.
  match handle
    .create_group(500, config(9, vec![9]), 1, CountSm::default())
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
    .create_group(600, failover, 1, CountSm::default())
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
    .create_group(100, config(1, vec![1]), 3, CountSm::default())
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
    .create_group(100, config(1, vec![1]), 3, CountSm::default())
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
    .create_group(100, config(1, vec![1, 2]), 1, CountSm::default())
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
    .create_group(100, config(2, vec![1, 2]), 2, CountSm::default())
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
    .create_group(100, config(1, vec![1, 2]), 1, CountSm::default())
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
    .create_group(100, config(2, vec![1, 2]), 2, CountSm::default())
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
    .create_group(100, config(2, vec![1, 2]), 2, CountSm::default())
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
/// The removed node is the group's LEADER, removing itself: the node driving the commit is the
/// one node guaranteed to APPLY its own removal. A removed FOLLOWER never learns the commit that
/// excised it — the leader prunes it from the tracker in the same fused commit+apply pass, and
/// sailing (unlike etcd's commit-then-bcastAppend) sends no further append to it — so the
/// follower-side RemovedSelf needs a final commit-carrying probe the core does not perform.
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
    )
    .await
    .expect("the cleared id re-admits");
  assert_eq!(submit_anywhere(&g100, b"rejoined").await, 3);
  assert_eq!(submit_anywhere(&g900, b"still").await, 3);
}
