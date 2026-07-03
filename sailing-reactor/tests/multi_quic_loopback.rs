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
use sailing_proto::{ClusterId, Config, Data, Role};
use sailing_reactor::{
  DriverConfig, DriverError, GroupHandle, MultiHandle, MultiReactorQuicDriver, Node,
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
