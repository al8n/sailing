//! Real-socket integration for the MULTI-GROUP reactor QUIC driver: loopback-UDP hosts carrying
//! several Raft groups over shared mandatory-mTLS QUIC connections and one shared storage engine,
//! on a multi-thread tokio runtime (which proves the `Send` `run()`). Mirrors the multi stream
//! suite. QUIC-specific: the host's transport identity latches from the FIRST admitted group, so
//! a zero-group host closes inbound identity-less connections — the suites create groups right
//! after spawning `run()` and the standing redial binds the mesh once admission lands.

mod common;

use std::{net::SocketAddr, time::Duration};

use agnostic::tokio::TokioRuntime;
use bytes::Bytes;
use common::{CountSm, TestCa, TrapSm};
use sailing_proto::{ClusterId, ConfChange, ConfChangeType, Config, Data, Role};
use sailing_reactor::{
  DriverConfig, DriverError, GroupHandle, LifecycleEvent, MultiHandle, MultiReactorQuicDriver, Node,
};

const ELECTION: Duration = Duration::from_millis(300);
const HEARTBEAT: Duration = Duration::from_millis(60);

fn cluster() -> ClusterId {
  ClusterId([13; 16])
}

fn addrs(base_port: u16, n: u16) -> Vec<SocketAddr> {
  (0..n)
    .map(|i| format!("127.0.0.1:{}", base_port + i).parse().unwrap())
    .collect()
}

type MDriver<F> = MultiReactorQuicDriver<TokioRuntime, u64, u64, F>;

/// Bind one EMPTY multi-group QUIC host (groups arrive via commands).
async fn bind_node<F>(
  ca: &TestCa,
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
  MDriver::<F>::bind(
    addr,
    ca.options(id, &cluster()),
    cluster(),
    peers,
    DriverConfig::default(),
  )
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

/// The group's current leader index among `groups` (status-polled, deadline-bounded).
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

/// The gate: a 3-node QUIC host carries groups 100 and 200 over ONE shared mTLS mesh; each group
/// elects, commits through redirects, and serves linearizable reads, with the two groups' state
/// machines strictly isolated. Groups are created immediately after spawn — the identity latch —
/// and the redial binds the peers that dialed before admission.
#[tokio::test(flavor = "multi_thread")]
async fn three_node_host_isolates_co_located_groups() {
  let ca = TestCa::new();
  let addrs = addrs(44_300, 3);
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  for id in 1u64..=3 {
    let peers: Vec<_> = (1u64..=3)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(&ca, id, addrs[(id - 1) as usize], peers).await;
    tokio::spawn(driver.run());
    handles.push(handle);
  }
  create_group_everywhere(&handles, 100, &[1, 2, 3]).await;
  create_group_everywhere(&handles, 200, &[1, 2, 3]).await;

  let g100: Vec<_> = handles.iter().map(|h| h.group(100)).collect();
  let g200: Vec<_> = handles.iter().map(|h| h.group(200)).collect();

  assert_eq!(submit_anywhere(&g100, b"a1").await, 1);
  assert_eq!(submit_anywhere(&g200, b"b1").await, 1);
  assert_eq!(submit_anywhere(&g100, b"a2").await, 2);
  assert_eq!(submit_anywhere(&g200, b"b2").await, 2);
  assert_eq!(submit_anywhere(&g200, b"b3").await, 3);

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

  let mut stamped = 0;
  while let Ok((g, _ev)) = handles[0].events().try_recv() {
    assert!(g == 100 || g == 200, "a foreign group stamp: {g}");
    stamped += 1;
  }
  assert!(stamped > 0, "the tail observed group-stamped events");
}

/// The shared engine's durability barrier AMORTIZES on the QUIC host too: a concurrent two-group
/// burst rides fewer barriers than it completes operations (delta over the burst window).
#[tokio::test(flavor = "multi_thread")]
async fn engine_barrier_amortizes_ops_across_groups() {
  let ca = TestCa::new();
  let addr: SocketAddr = "127.0.0.1:44320".parse().unwrap();
  let (driver, handle) = bind_node::<CountSm>(&ca, 1, addr, Vec::new()).await;
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

  assert_eq!(submit_anywhere(std::slice::from_ref(&g100), b"w").await, 1);
  assert_eq!(submit_anywhere(std::slice::from_ref(&g200), b"w").await, 1);

  let flushes0 = metrics.flushes();
  let ops0 = metrics.ops_flushed();

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

/// Removing a group fails exactly ITS parked work while the host survives — the stream suite's
/// scenario over QUIC: de-host the group on the follower (stranding the leader without quorum),
/// park a submit on the leader, then remove there and watch only that work fail.
#[tokio::test(flavor = "multi_thread")]
async fn remove_group_fails_parked_ops_and_the_host_survives() {
  let ca = TestCa::new();
  let addrs = addrs(44_340, 2);
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  for id in 1u64..=2 {
    let peers: Vec<_> = (1u64..=2)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(&ca, id, addrs[(id - 1) as usize], peers).await;
    tokio::spawn(driver.run());
    handles.push(handle);
  }
  create_group_everywhere(&handles, 100, &[1, 2]).await;
  create_group_everywhere(&handles, 300, &[1, 2]).await;

  let g300: Vec<_> = handles.iter().map(|h| h.group(300)).collect();
  assert_eq!(submit_anywhere(&g300, b"seed").await, 1);

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

  assert!(
    handles[follower_at]
      .remove_group(300)
      .await
      .expect("remove resolves"),
    "the follower hosted group 300"
  );

  let parked = tokio::spawn({
    let g = g300[leader_at].clone();
    async move { g.submit(Bytes::from_static(b"parked")).await }
  });
  tokio::time::sleep(Duration::from_millis(400)).await;
  assert!(
    !parked.is_finished(),
    "the submit must park: its quorum no longer hosts the group"
  );

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

  let g100: Vec<_> = handles.iter().map(|h| h.group(100)).collect();
  assert_eq!(submit_anywhere(&g100, b"alive").await, 1);
  match g300[leader_at].status().await {
    Err(DriverError::Rejected { reason }) => {
      assert!(reason.contains("no such group"), "got: {reason}");
    }
    other => panic!("expected the no-such-group rejection, got {other:?}"),
  }
}

/// Poll `metrics` until its quiesced-group gauge reaches `want`.
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

/// The stream suite's quiesce cycle over QUIC: both groups quiesce on BOTH drivers (leader by
/// eligibility, follower by the flagged beat's GroupControl round-trip), hold across a quiet
/// window with terms frozen (the zero-frame witness: a quiesced leader emits no beats by
/// construction and any arriving wake-class frame would drop the follower's gauge), a propose
/// wakes exactly its group while the sibling stays quiesced, and the woken group re-quiesces.
#[tokio::test(flavor = "multi_thread")]
async fn idle_groups_quiesce_and_a_propose_wakes_only_its_group() {
  let ca = TestCa::new();
  let addrs = addrs(44_400, 2);
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  let mut metrics = Vec::new();
  for id in 1u64..=2 {
    let peers: Vec<_> = (1u64..=2)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(&ca, id, addrs[(id - 1) as usize], peers).await;
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

  wait_for_quiesced(&metrics[0], 2, "node 1 both groups").await;
  wait_for_quiesced(&metrics[1], 2, "node 2 both groups").await;

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

  assert_eq!(submit_anywhere(&g100, b"wake").await, 2);
  let status200 = g200[0].status().await.expect("status");
  assert_eq!(status200.term, term200, "group 200 saw no traffic");
  assert!(
    metrics.iter().all(|m| m.quiesced_groups() >= 1),
    "the sibling group stayed quiesced through the wake"
  );

  wait_for_quiesced(&metrics[0], 2, "node 1 re-quiesce").await;
  wait_for_quiesced(&metrics[1], 2, "node 2 re-quiesce").await;
}

/// Wake-on-connection-loss over QUIC: the leader's driver shuts down; the survivors' quinn stacks
/// declare the shared connection lost (keep-alives stop being acknowledged, the idle timeout
/// fires), the has_bound_conn falling edge wakes every quiesced group, the stale election
/// deadlines fire, and a NEW leader emerges — a submit through the survivors commits. Detection
/// rides quinn's idle machinery (seconds), unlike the stream driver's immediate socket EOF, so
/// the re-election deadline here is generous.
#[tokio::test(flavor = "multi_thread")]
async fn conn_loss_wakes_quiesced_followers_and_reelects() {
  let ca = TestCa::new();
  let addrs = addrs(44_420, 3);
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  let mut metrics = Vec::new();
  for id in 1u64..=3 {
    let peers: Vec<_> = (1u64..=3)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(&ca, id, addrs[(id - 1) as usize], peers).await;
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

/// One group's fail-stop stays GROUP-scoped on the QUIC host: a trapped FSM apply poisons group
/// 100 (typed verdicts for its parked and later work) while co-hosted group 200 keeps committing
/// on the same driver.
#[tokio::test(flavor = "multi_thread")]
async fn a_poisoned_group_leaves_co_hosted_groups_committing() {
  let ca = TestCa::new();
  let addr: SocketAddr = "127.0.0.1:44360".parse().unwrap();
  let (driver, handle) = bind_node::<TrapSm>(&ca, 1, addr, Vec::new()).await;
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

  assert_eq!(submit_anywhere(std::slice::from_ref(&g100), b"ok").await, 1);
  assert_eq!(submit_anywhere(std::slice::from_ref(&g200), b"ok").await, 1);

  match g100.submit(Bytes::from_static(b"BOOM")).await {
    Err(DriverError::Poisoned) => {}
    other => panic!("expected Poisoned from the trapped apply, got {other:?}"),
  }
  let status = g100.status().await.expect("status still answers");
  assert!(status.is_poisoned, "group 100 fail-stopped");
  match g100.submit(Bytes::from_static(b"late")).await {
    Err(DriverError::Poisoned) => {}
    other => panic!("expected Poisoned on the dead group, got {other:?}"),
  }

  assert_eq!(
    submit_anywhere(std::slice::from_ref(&g200), b"alive").await,
    2
  );
  assert!(!g200.status().await.expect("status").is_poisoned);

  let out: Option<u64> = g200
    .failover_query(|_fsm: &TrapSm, _limbo: &[sailing_proto::Entry], _win| Some(9))
    .await
    .expect("the failover query resolves");
  assert_eq!(out, None, "no serve window → normal-read fallback");
}

/// The embedder-driven placement flow over QUIC (the stream suite's money test): node 1 creates
/// group 100 (voters {1,2}) and campaigns into the void; node 2 surfaces
/// `UnknownGroup { group: 100, from: 1 }` on its lifecycle tail; the test creates the group
/// there; the campaigner's retry completes and both sides commit, with the sibling untouched.
#[tokio::test(flavor = "multi_thread")]
async fn unknown_group_event_drives_creation() {
  let ca = TestCa::new();
  let addrs = addrs(44_440, 2);
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  for id in 1u64..=2 {
    let peers: Vec<_> = (1u64..=2)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(&ca, id, addrs[(id - 1) as usize], peers).await;
    tokio::spawn(driver.run());
    handles.push(handle);
  }
  // The sibling group exists everywhere: it latches both hosts' transport identities (a
  // zero-group QUIC host cannot bind) and proves the shared mesh.
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

  // The test IS the placement brain: create the solicited group on node 2 and watch the join
  // complete.
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

/// The removed-self flow over QUIC (see the stream suite for the leader-self-removal rationale):
/// the group's leader removes ITSELF via a committed v1 remove-node, its lifecycle tail yields
/// `RemovedSelf { group: 100 }`, the app tears the local replica down, the removed node's other
/// group keeps committing, the survivors keep committing the shrunk group, and the tombstone
/// absorbs every straggler without re-soliciting placement.
#[tokio::test(flavor = "multi_thread")]
async fn removed_self_event_and_teardown() {
  let ca = TestCa::new();
  let addrs = addrs(44_460, 3);
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  for id in 1u64..=3 {
    let peers: Vec<_> = (1u64..=3)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(&ca, id, addrs[(id - 1) as usize], peers).await;
    tokio::spawn(driver.run());
    handles.push(handle);
  }
  create_group_everywhere(&handles, 100, &[1, 2, 3]).await;
  create_group_everywhere(&handles, 200, &[1, 2, 3]).await;
  let g100: Vec<_> = handles.iter().map(|h| h.group(100)).collect();
  let g200: Vec<_> = handles.iter().map(|h| h.group(200)).collect();
  assert_eq!(submit_anywhere(&g100, b"seed").await, 1);
  assert_eq!(submit_anywhere(&g200, b"seed").await, 1);

  // Group 100's leader proposes REMOVING ITSELF; retry through pre-propose leadership moves.
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

  await_lifecycle(handles[removed_at].lifecycle(), "removed-self", |ev| {
    matches!(ev, LifecycleEvent::RemovedSelf { group: 100 })
  })
  .await;

  // The app decides: tear the local replica down.
  assert!(
    handles[removed_at]
      .remove_group(100)
      .await
      .expect("remove resolves"),
    "the removed node hosted group 100"
  );

  // The removed node's OTHER group is untouched, and the survivors keep committing the shrunk
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

  // No resurrection prompt: the tombstone absorbed the stragglers silently.
  while let Ok(ev) = handles[removed_at].lifecycle().try_recv() {
    assert!(
      !matches!(ev, LifecycleEvent::UnknownGroup { group: 100, .. }),
      "a tombstoned group must never re-solicit placement: {ev:?}"
    );
  }
}

/// The removed-FOLLOWER flow over QUIC (see the stream sibling for the full rationale): the
/// leader's farewell heartbeat — emitted at the apply-time fold, before the tracker swap drops
/// the pruned peer — delivers the excising commit to a follower that never drives it, so ITS
/// lifecycle tail yields `RemovedSelf` and the app tears the replica down. Shaped voters {1, 2}
/// + learner 3 so the removed voter's ack is REQUIRED for the commit quorum (the farewell's
/// commit-carrying shape is deterministic), with pre-vote + check-quorum walling off the
/// learn-by-usurpation path — a farewell regression is a clean timeout here.
#[tokio::test(flavor = "multi_thread")]
async fn removed_follower_learns_via_farewell_and_tears_down() {
  let ca = TestCa::new();
  let addrs = addrs(44_560, 3);
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  for id in 1u64..=3 {
    let peers: Vec<_> = (1u64..=3)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(&ca, id, addrs[(id - 1) as usize], peers).await;
    tokio::spawn(driver.run());
    handles.push(handle);
  }
  for id in 1u64..=2 {
    handles[(id - 1) as usize]
      .create_group(
        100,
        config(id, vec![1, 2])
          .with_pre_vote(true)
          .with_check_quorum(true),
        id,
        CountSm::default(),
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

  // A (the leader) removes C (the OTHER voter); C's ack is in the quorum, so the farewell
  // carries the removal commit.
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

  await_lifecycle(handles[removed_at].lifecycle(), "removed-follower", |ev| {
    matches!(ev, LifecycleEvent::RemovedSelf { group: 100 })
  })
  .await;
  assert!(
    handles[removed_at]
      .remove_group(100)
      .await
      .expect("remove resolves"),
    "the removed follower hosted group 100"
  );

  // A + B keep the group; the sibling group's quorum NEEDS the torn-down node's driver, so its
  // commit proves C's driver survived the group-scoped teardown.
  let leader_at = 1 - removed_at;
  assert_eq!(
    submit_anywhere(std::slice::from_ref(&g100[leader_at]), b"after").await,
    2,
    "the shrunk group applied seed + after"
  );
  assert_eq!(submit_anywhere(&g300, b"still").await, 2);
}

/// Tombstone-then-recreate over QUIC: one follower of a live 3-node group de-hosts its replica,
/// the leader's straggler beats die silently against the tombstone (the shared mTLS mesh and the
/// sibling group stay clean), and the explicit clear-then-create rejoin re-admits the SAME id
/// while the group keeps committing through its majority (see the stream sibling for why full
/// rejoin of the fresh replica is the restore/snapshot path's job, not the reject walk-back's).
#[tokio::test(flavor = "multi_thread")]
async fn tombstoned_id_recreates_cleanly() {
  let ca = TestCa::new();
  let addrs = addrs(44_480, 3);
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  for id in 1u64..=3 {
    let peers: Vec<_> = (1u64..=3)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(&ca, id, addrs[(id - 1) as usize], peers).await;
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
