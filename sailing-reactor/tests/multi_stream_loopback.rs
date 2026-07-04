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
      // phase, which the driver invokes only after admitting the blueprint.
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
