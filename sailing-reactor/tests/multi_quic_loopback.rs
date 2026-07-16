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
use sailing_proto::{ClusterId, ConfChange, ConfChangeType, Config, Data, Index, Role};
use sailing_reactor::{
  DriverConfig, DriverError, GroupBlueprint, GroupHandle, LifecycleEvent, MultiHandle,
  MultiReactorQuicDriver, Node, factory_fn,
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

/// Colocate `groups`' leadership onto node `to_node`, waiting until it settles there. The merge's
/// all-source-voters barrier is observable only on the source LEADER's tracker, so `commit_merge`
/// can only certify it when the absorbing target's leader also leads the source — the CRDB
/// colocate-then-merge discipline. Transfer the source onto the target leader BEFORE freezing:
/// a frozen source refuses a transfer, and moving the source (not the target) leaves the target's
/// leadership pinned through the choreography.
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
    .create_group(100, config(1, vec![1]), 1, CountSm::default(), 0)
    .await
    .expect("group 100 admitted");
  handle
    .create_group(200, config(1, vec![1]), 2, CountSm::default(), 0)
    .await
    .expect("group 200 admitted");
  let g100 = handle.group(100);
  let g200 = handle.group(200);

  assert_eq!(submit_anywhere(std::slice::from_ref(&g100), b"w").await, 1);
  assert_eq!(submit_anywhere(std::slice::from_ref(&g200), b"w").await, 1);

  let barriers0 = metrics.barriers();
  let ops0 = metrics.ops_batched();

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
    .create_group(100, config(1, vec![1]), 1, TrapSm::default(), 0)
    .await
    .expect("group 100 admitted");
  handle
    .create_group(200, config(1, vec![1]), 2, TrapSm::default(), 0)
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
    .failover_query(|_fsm: &TrapSm, _win| Some(9))
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

  // The test IS the placement brain: create the solicited group on node 2 and watch the join
  // complete.
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
/// + learner 3 so the removed voter's ack is REQUIRED for the commit quorum — pinning the
/// `match >= removal` commit-only heartbeat arm (the 3-voter farewell-APPEND arm is
/// `removed_voter_in_full_quorum_learns_via_farewell`'s) — with pre-vote + check-quorum walling
/// off the learn-by-usurpation path, so a farewell regression is a clean timeout here.
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
}

/// The 3-voter removed-voter flow over QUIC (see the stream sibling for the full rationale):
/// the removal commits on whichever follower ack the leader processes first, so WHICH farewell
/// arm delivers — the commit-only heartbeat (`match >= removal`) or the suffix-carrying append
/// (`match < removal`) — is a scheduler coin flip this test deliberately leaves OPEN; it pins
/// that the removed voter learns and the live leader survives EITHER WAY (role and term fixed
/// from the removal commit to `RemovedSelf`), with pre-vote/check-quorum at their defaults
/// (OFF). The append arm's DETERMINISTIC pins are the endpoint farewell-append tests, the
/// `confchange_remove` interaction golden, and this suite's learn-from-zero sibling
/// (`never_caught_up_removed_replica_learns_from_zero_via_farewell`).
#[tokio::test(flavor = "multi_thread")]
async fn removed_voter_in_full_quorum_learns_via_farewell() {
  let ca = TestCa::new();
  let addrs = addrs(44_800, 3);
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

  // C learns from the farewell alone: RemovedSelf on ITS lifecycle tail.
  await_lifecycle(handles[removed_at].lifecycle(), "removed-voter", |ev| {
    matches!(ev, LifecycleEvent::RemovedSelf { group: 100 })
  })
  .await;

  // NO leadership churn across the removal: A is still leader at the SAME term.
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

  // The sibling group is unaffected — its quorum includes the torn-down node's driver.
  assert!(submit_anywhere(&g400, b"still").await >= 2);

  // No resurrection prompt on the removed voter: the tombstone absorbs the stragglers.
  while let Ok(ev) = handles[removed_at].lifecycle().try_recv() {
    assert!(
      !matches!(ev, LifecycleEvent::UnknownGroup { group: 100, .. }),
      "a tombstoned group must never re-solicit placement: {ev:?}"
    );
  }
}

/// The learn-from-zero farewell-append flow over QUIC (see the stream sibling for the full
/// rationale): node 2 pre-creates group 100 as a NON-MEMBER observer replica — hosted, so the
/// farewell can land; untracked by the sole voter, so NOTHING ever replicates to it — and
/// AddLearnerNode(2) / RemoveNode(2) commit back-to-back at the one-in-flight limit, BOTH on
/// node 1's own in-memory durability barrier alone (a single-voter quorum), so the removal fold
/// prunes node 2 at `match[2] = 0 < removal` before any node-2 response can be processed: the
/// farewell can ONLY be the append arm (a heartbeat's commit clamp `min(commit, match)` is 0
/// for a zero-match peer). The removed replica is deliberately a LEARNER: a never-caught-up
/// VOTER's removal needs either its own ack (the heartbeat arm by construction) or another
/// voter's NETWORK ack — a cross-connection scheduling race that QUIC demonstrably lets a
/// probe-rejection walk-back resend win. `max_size_per_msg = 1` and the stretched beat keep
/// even stray catch-up out of the window, and node 2's applied/commit are re-asserted ZERO
/// before every remove attempt — a broken premise fails loudly instead of passing through the
/// heartbeat arm. `RemovedSelf` fires on node 2's tail with its applied index carried from zero
/// past the removal in ONE delivery, and the leader's role and term hold.
#[tokio::test(flavor = "multi_thread")]
async fn never_caught_up_removed_replica_learns_from_zero_via_farewell() {
  let ca = TestCa::new();
  let addrs = addrs(44_860, 2);
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
      0,
    )
    .await
    .expect("the cleared id re-admits");
  assert_eq!(submit_anywhere(&g100, b"rejoined").await, 3);
  assert_eq!(submit_anywhere(&g900, b"still").await, 3);
}

/// The hands-free materialization flow over QUIC (the stream suite's money test): node 2
/// registers a group FACTORY recognizing group 100 and never calls `create_group` for it; node
/// 1 creates 100 (voters {1,2}) and campaigns over the shared mTLS mesh; node 2's driver
/// materializes the replica inside the crank that polled the solicitation and the campaigner's
/// retry completes the election — both sides commit and read, the consumed signal never reaches
/// node 2's lifecycle tail, and the manually-created sibling (which also latched both hosts'
/// transport identities) is untouched.
#[tokio::test(flavor = "multi_thread")]
async fn factory_materializes_solicited_group_hands_free() {
  let ca = TestCa::new();
  let addrs = addrs(44_600, 2);
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  for id in 1u64..=2 {
    let peers: Vec<_> = (1u64..=2)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(&ca, id, addrs[(id - 1) as usize], peers).await;
    if id == 2 {
      // The factory IS the embedder's catalog check, on both legs: the group id against the
      // catalog AND the solicitor against the group's replica set (the driver refuses a
      // blueprint that fails the second leg anyway). The state machine lives in the separate
      // build phase, invoked only after the driver admits the blueprint. Group 100 is a day-0
      // BOOTSTRAPPED id (created explicitly on node 1), so the blueprint keeps the full-voter
      // shape — the observer rule binds fork-born ids only.
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
  // The sibling group exists everywhere: it latches both hosts' transport identities (a
  // zero-group QUIC host cannot bind) and pins that a factory-bearing host still admits
  // ordinary lifecycle-command groups.
  create_group_everywhere(&handles, 900, &[1, 2]).await;
  let g900: Vec<_> = handles.iter().map(|h| h.group(900)).collect();
  assert_eq!(submit_anywhere(&g900, b"seed").await, 1);

  // Group 100 exists only on node 1; its campaign solicits node 2, whose factory materializes
  // the replica hands-free.
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

  // The factory CONSUMED the solicitation: node 2's lifecycle tail never surfaced group 100.
  while let Ok(ev) = handles[1].lifecycle().try_recv() {
    assert!(
      !matches!(ev, LifecycleEvent::UnknownGroup { group: 100, .. }),
      "a factory-consumed signal must not reach the tail: {ev:?}"
    );
  }

  // The manually-created sibling is unaffected by the factory churn.
  assert_eq!(submit_anywhere(&g900, b"still").await, 2);
}

/// The QUIC mirror of the stream suite's fork-then-join flow: nodes 1+2 fork child 300 from the
/// SAME preloaded blob, commit a live tail, then AddNode(3) — node 3's factory materializes an
/// EMPTY replica under the fork-born OBSERVER blueprint (it can never campaign; the snapshot's
/// boundary config promotes it) and it catches up BY SNAPSHOT over the mTLS mesh (an
/// empty-booted joiner replaying only the tail would count 2, not 9). The sibling group is
/// untouched throughout.
#[tokio::test(flavor = "multi_thread")]
async fn forked_group_serves_and_snapshots_a_late_joiner() {
  let ca = TestCa::new();
  let addrs = addrs(44_900, 3);
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  for id in 1u64..=3 {
    let peers: Vec<_> = (1u64..=3)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(&ca, id, addrs[(id - 1) as usize], peers).await;
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
  // The sibling group binds the mesh (and latches every host's transport identity).
  create_group_everywhere(&handles, 900, &[1, 2, 3]).await;
  let g900: Vec<_> = handles.iter().map(|h| h.group(900)).collect();
  assert_eq!(submit_anywhere(&g900, b"seed").await, 1);

  // Nodes 1 and 2 fork the child from the SAME blob: a preloaded count of 7.
  let blob = {
    let mut v = Vec::new();
    Data::encode(&7u64, &mut v);
    Bytes::from(v)
  };
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

/// The T11 QUIC merge gate — the stream suite's absorb shape over the QUIC transport: freeze,
/// parked commit, per-crank resolution, the union served everywhere, the source id refused
/// forever on its terminal floor, and no leadership churn on the target through the whole
/// choreography.
#[tokio::test(flavor = "multi_thread")]
async fn merge_absorbs_and_source_never_returns() {
  let ca = TestCa::new();
  let addrs = addrs(44_960, 3);
  let mut handles = Vec::new();
  for id in 1u64..=3 {
    let peers: Vec<_> = (1u64..=3)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = bind_node::<CountSm>(&ca, id, addrs[(id - 1) as usize], peers).await;
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
  // 200 the source that dissolves (200's LE encoding sorts strictly above 100's).
  let t_leader = find_leader(&g100, "target pre-merge").await;
  let t_term = g100[t_leader].status().await.expect("status").term;
  // Colocate the source's leadership onto the target's leader so the absorb can certify the
  // all-source-voters freeze barrier (the source is moved, so the target leader never churns).
  colocate_onto(&g200, t_leader as u64 + 1, "source onto target leader").await;

  // DirectionInverted is a property of the id pair, never transient — fail fast on it so a
  // re-introduced direction bug is a pointed panic, not a 15s timeout. `map_merge_err` carries the
  // variant's Debug form as the rejection reason.
  let inverted = format!("{:?}", sailing_proto::MergeError::<u64>::DirectionInverted);
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  'freeze: loop {
    assert!(std::time::Instant::now() < deadline, "no freeze accepted");
    for h in &handles {
      match h.prepare_merge(200, 100).await {
        Ok(_) => break 'freeze,
        Err(DriverError::Rejected { reason }) if reason == inverted => {
          panic!("the freeze is permanently inverted — source must encode above target")
        }
        Err(_) => {}
      }
    }
    tokio::time::sleep(Duration::from_millis(40)).await;
  }
  'commit: loop {
    assert!(std::time::Instant::now() < deadline, "no commit accepted");
    for h in &handles {
      if h.commit_merge(100, 200).await.is_ok() {
        break 'commit;
      }
    }
    tokio::time::sleep(Duration::from_millis(40)).await;
  }

  // Every node's crank resolves its park: the union serves from the target on ALL nodes.
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

  // The source id dies everywhere and its floor is terminal.
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

/// A restore for a group the host holds NO stored state for fails closed instead of fabricating a
/// blank incarnation: removing a group drops its in-memory stores, so a later restore of that id
/// finds nothing to recover and returns `NoStoredState` rather than a silent empty `Ok`. The id is
/// not resurrected, and the co-hosted live group keeps committing.
#[tokio::test(flavor = "multi_thread")]
async fn restore_without_stored_state_fails_closed() {
  let ca = TestCa::new();
  let addr: SocketAddr = "127.0.0.1:44380".parse().unwrap();
  let (driver, handle) = bind_node::<CountSm>(&ca, 1, addr, Vec::new()).await;
  tokio::spawn(driver.run());

  handle
    .create_group(100, config(1, vec![1]), 1, CountSm::default(), 0)
    .await
    .expect("the live co-hosted group admits");
  handle
    .create_group(200, config(1, vec![1]), 2, CountSm::default(), 0)
    .await
    .expect("the second group admits");
  let g200 = handle.group(200);
  assert_eq!(submit_anywhere(std::slice::from_ref(&g200), b"x").await, 1);
  assert!(
    handle.remove_group(200).await.expect("remove resolves"),
    "the hosted removal dropped storage"
  );
  assert!(
    handle.clear_tombstone(200).await.expect("clear resolves"),
    "the removal left a tombstone to clear"
  );

  // The tombstone is cleared, so nothing but the ABSENT stores stands between this call and a
  // restore. The removal dropped those in-memory stores, so the restore has nothing to recover:
  // it fails closed rather than silently standing up a blank index-0 incarnation.
  match handle
    .restore_group(200, config(1, vec![1]), 2, CountSm::default(), 0)
    .await
  {
    Err(DriverError::NoStoredState) => {}
    other => panic!("expected NoStoredState for a group with no stored state, got {other:?}"),
  }
  assert!(
    handle.group(200).status().await.is_err(),
    "the group was not resurrected"
  );

  // The co-hosted live group is undisturbed by the refused restore.
  let g100 = handle.group(100);
  assert_eq!(submit_anywhere(std::slice::from_ref(&g100), b"y").await, 1);
}
