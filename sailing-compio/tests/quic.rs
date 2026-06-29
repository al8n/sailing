//! Real-socket integration: three QUIC drivers on loopback UDP, cluster-private mTLS, real
//! quinn-proto datagrams, real timers — the whole stack the simulator cannot exercise.

mod common;

use std::{net::SocketAddr, time::Duration};

use bytes::Bytes;
use common::{CountSm, MemLog, MemStable, SharedLog, SharedStable, TestCa};
use sailing_compio::{CompioQuicDriver, DriverConfig, DriverError, Handle, Node};
use sailing_proto::{
  ClusterId, Config, Event, Index, LogStore, ReadOnlyOption, Role, StableStore, Term,
};

const ELECTION: Duration = Duration::from_millis(300);
const HEARTBEAT: Duration = Duration::from_millis(60);

fn cluster() -> ClusterId {
  ClusterId([7; 16])
}

fn addrs(base_port: u16, n: u16) -> Vec<SocketAddr> {
  (0..n)
    .map(|i| format!("127.0.0.1:{}", base_port + i).parse().unwrap())
    .collect()
}

async fn build_node(
  ca: &TestCa,
  id: u64,
  addr: SocketAddr,
  peers: Vec<Node<u64, SocketAddr>>,
  cfg: DriverConfig,
) -> (
  CompioQuicDriver<u64, CountSm, MemLog, MemStable>,
  Handle<u64, CountSm>,
) {
  let config = Config::try_new(id, vec![1u64, 2, 3], ELECTION, HEARTBEAT).unwrap();
  CompioQuicDriver::bind(
    addr,
    config,
    id, // election-jitter seed: distinct per node
    CountSm::default(),
    ca.options(id, &cluster()),
    cluster(),
    peers,
    MemLog::new(),
    MemStable::new(),
    cfg,
  )
  .await
  .expect("driver binds")
}

/// Spawn a full 3-node cluster, each driver detached on this test's runtime; returns the
/// handles indexed by node id - 1.
async fn spawn_cluster(ca: &TestCa, base_port: u16) -> Vec<Handle<u64, CountSm>> {
  let addrs = addrs(base_port, 3);
  let mut handles = Vec::new();
  for id in 1u64..=3 {
    let peers: Vec<_> = (1u64..=3)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (driver, handle) = build_node(
      ca,
      id,
      addrs[(id - 1) as usize],
      peers,
      DriverConfig::default(),
    )
    .await;
    compio::runtime::spawn(driver.run()).detach();
    handles.push(handle);
  }
  handles
}

/// Submit through whichever node is (or redirects to) the leader, retrying the NotLeader hint
/// until the cluster elects and the command commits.
async fn submit_anywhere(handles: &[Handle<u64, CountSm>], payload: &'static [u8]) -> u64 {
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  let mut at = 0usize;
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "no commit within the deadline"
    );
    match handles[at].submit(Bytes::from_static(payload)).await {
      Ok(response) => return response,
      // Redirect: the hint names the leader; no hint yet means no leader yet — try the next
      // node after a beat.
      Err(DriverError::NotLeader { leader }) => {
        at = leader
          .map(|l| (l - 1) as usize)
          .unwrap_or((at + 1) % handles.len());
        compio::time::sleep(Duration::from_millis(50)).await;
      }
      // A leadership change voided the outcome: retry (the test payload is idempotent).
      Err(DriverError::Superseded) => {}
      Err(e) => panic!("unexpected submit error: {e:?}"),
    }
  }
}

/// `bind` must REJECT an out-of-range programmatic `DriverConfig` rather than build a driver with a
/// pathological submit budget — the QUIC counterpart of the stream driver's identical guard. An
/// over-ceiling `max_inflight` exceeds the submit-budget ceiling and is rejected. The `validate`
/// runs before the UDP socket binds, so the bogus address is never touched.
#[compio::test]
async fn bind_rejects_out_of_range_driver_config() {
  use sailing_compio::{BindError, MAX_CHANNEL_CAPACITY};

  let ca = TestCa::new();
  let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
  let config = Config::try_new(1u64, vec![1u64], ELECTION, HEARTBEAT).unwrap();
  let over_inflight = DriverConfig {
    max_inflight: MAX_CHANNEL_CAPACITY,
    ..DriverConfig::default()
  };
  let res = CompioQuicDriver::bind(
    addr,
    config,
    1,
    CountSm::default(),
    ca.options(1, &cluster()),
    cluster(),
    vec![],
    MemLog::new(),
    MemStable::new(),
    over_inflight,
  )
  .await;
  assert!(
    matches!(res, Err(BindError::DriverConfig(_))),
    "an over-ceiling max_inflight must be rejected at bind, not panic"
  );
}

/// The gate: a real 3-node cluster over mandatory-mTLS QUIC on loopback elects, commits a
/// command submitted with NotLeader redirects, answers through the submitting handle, and
/// serves a linearizable query against the leader's state machine.
#[compio::test]
async fn three_node_cluster_commits_and_queries() {
  let ca = TestCa::new();
  let handles = spawn_cluster(&ca, 42_000).await;

  let response = submit_anywhere(&handles, b"hello").await;
  assert_eq!(response, 1, "the first committed command counts to one");

  // A second command through any node (the redirect loop finds the leader again).
  let response = submit_anywhere(&handles, b"world").await;
  assert_eq!(response, 2);

  // A linearizable query: runs against the FSM on the driver thread at a confirmed read index.
  // Find the leader (the node whose submit succeeds is leader-adjacent; query needs the leader
  // or a forwarding follower — sailing forwards follower reads, so any node serves).
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  let count = loop {
    assert!(
      std::time::Instant::now() < deadline,
      "no query within the deadline"
    );
    let mut served = None;
    for h in &handles {
      match h.query(|sm: &CountSm| sm.count()).await {
        Ok(c) => {
          served = Some(c);
          break;
        }
        Err(_) => continue,
      }
    }
    if let Some(c) = served {
      break c;
    }
    compio::time::sleep(Duration::from_millis(50)).await;
  };
  assert!(
    count >= 2,
    "the linearizable read observes both commits, got {count}"
  );

  // The events tail saw the applies (best-effort, but nothing here overflows it).
  let mut applied = 0;
  while let Ok(ev) = handles[0].events().try_recv() {
    if ev.is_applied() {
      applied += 1;
    }
  }
  assert!(applied >= 1, "the tail observed at least one apply");
}

/// The budget gate is at the HANDLE, before anything queues: a payload larger than the byte
/// budget is Busy synchronously — no cluster, no timing.
#[compio::test]
async fn submit_budget_exhaustion_is_busy() {
  let ca = TestCa::new();
  let addrs = addrs(42_100, 1);
  let cfg = DriverConfig {
    max_pending_bytes: 4,
    ..Default::default()
  };
  let (_driver, handle) = build_node(&ca, 1, addrs[0], Vec::new(), cfg).await;
  // The driver is never run: the budget rejects before the command channel is involved.
  match handle.submit(Bytes::from_static(b"oversized")).await {
    Err(DriverError::Busy) => {}
    other => panic!("expected Busy, got {other:?}"),
  }
}

/// A completed `shutdown().await` is an immediate-rebind contract: it resolves only after the socket
/// fd is fully released, so binding the SAME address again succeeds at once. Crucially this holds for
/// EVERY coalesced caller — here two `Handle` clones shut down concurrently and BOTH must resolve
/// before the rebind, proving a swap-loser awaits real teardown rather than returning an early `Ok`.
#[compio::test]
async fn shutdown_means_immediate_rebind_for_every_coalesced_caller() {
  let ca = TestCa::new();
  let addr: SocketAddr = "127.0.0.1:42200".parse().unwrap();

  let (driver, handle) = build_node(&ca, 1, addr, Vec::new(), DriverConfig::default()).await;
  let clone = handle.clone();
  let task = compio::runtime::spawn(driver.run());
  // Two coalesced callers (only one wins the enqueue swap); JOIN them so BOTH resolve before the
  // rebind. If a loser returned `Ok` before the driver's `close().await`, the rebind below would
  // race the still-open fd and could fail with `AddrInUse`.
  let (a, b) = futures_util::future::join(handle.shutdown(), clone.shutdown()).await;
  a.expect("the winner resolves after teardown");
  b.expect("the loser resolves after teardown");
  // Both resolved ⇒ the fd is RELEASED — not merely that teardown was scheduled.
  let rebound = compio::net::UdpSocket::bind(addr)
    .await
    .expect("the address is immediately rebindable once every shutdown caller has resolved");
  drop(rebound);
  let _ = task.await;

  // Post-shutdown operations fail with the typed teardown error.
  match handle.submit(Bytes::from_static(b"late")).await {
    Err(DriverError::ShuttingDown) => {}
    other => panic!("expected ShuttingDown, got {other:?}"),
  }
}

/// A node with no quorum never leads: submits are NotLeader (no silent parking), and the
/// redirect hint is absent while no leader is known.
#[compio::test]
async fn no_quorum_means_not_leader_not_a_hang() {
  let ca = TestCa::new();
  let addrs = addrs(42_300, 3);
  // Only node 1 runs; 2 and 3 are configured but never started.
  let peers = vec![Node::new(2u64, addrs[1]), Node::new(3u64, addrs[2])];
  let (driver, handle) = build_node(&ca, 1, addrs[0], peers, DriverConfig::default()).await;
  compio::runtime::spawn(driver.run()).detach();

  // Give it a few election timeouts: without quorum it can never win.
  compio::time::sleep(ELECTION * 4).await;
  match handle.submit(Bytes::from_static(b"nope")).await {
    Err(DriverError::NotLeader { leader }) => {
      assert_eq!(leader, None, "no leader is known without a quorum");
    }
    other => panic!("expected NotLeader, got {other:?}"),
  }
}

/// A storage fault fail-stops the endpoint (poison); the driver must fail everything parked
/// with the TYPED verdict — not strand it holding budget — and exit its run loop.
#[compio::test]
async fn storage_fault_poisons_with_a_typed_verdict() {
  use common::PoisonableLog;

  let ca = TestCa::new();
  let addr: SocketAddr = "127.0.0.1:42400".parse().unwrap();
  // A single-voter cluster: elects itself and commits without peers, so the only failure
  // injected is the storage fault.
  let config = Config::try_new(1u64, vec![1u64], ELECTION, HEARTBEAT).unwrap();
  let (log, fail_appends) = PoisonableLog::new();
  let (driver, handle) = CompioQuicDriver::bind(
    addr,
    config,
    1,
    CountSm::default(),
    ca.options(1, &cluster()),
    cluster(),
    Vec::new(),
    log,
    MemStable::new(),
    DriverConfig::default(),
  )
  .await
  .expect("binds");
  let task = compio::runtime::spawn(driver.run());

  // A healthy commit first (the cluster works end to end before the fault).
  let deadline = std::time::Instant::now() + Duration::from_secs(10);
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "no leadership in time"
    );
    match handle.submit(Bytes::from_static(b"ok")).await {
      Ok(1) => break,
      Ok(n) => panic!("unexpected count {n}"),
      Err(DriverError::NotLeader { .. }) => {
        compio::time::sleep(Duration::from_millis(30)).await;
      }
      Err(e) => panic!("unexpected error: {e:?}"),
    }
  }

  // Inject the fault: the NEXT append's completion is a storage error → fail-stop.
  fail_appends.store(true, std::sync::atomic::Ordering::Release);
  match handle.submit(Bytes::from_static(b"doomed")).await {
    Err(DriverError::Poisoned) => {}
    other => panic!("expected Poisoned, got {other:?}"),
  }

  // The run loop exited on the poison: the driver task ends and later operations surface the
  // teardown error.
  let _ = task.await;
  match handle.submit(Bytes::from_static(b"late")).await {
    Err(DriverError::ShuttingDown) => {}
    other => panic!("expected ShuttingDown, got {other:?}"),
  }
}

/// A log whose `poll()` emits a huge burst of `Compacted` completions AHEAD of its real ones before
/// draining: an UNBOUNDED `handle_storage` would process the whole burst in one call and trap the
/// driver's run loop, starving commands/timers. The per-call budget bounds each call instead (the
/// remainder stays queued — `poll()` is a stateful FIFO, nothing dropped), so the loop keeps cycling.
/// `Compacted` is a no-op arm, so flooding it never corrupts log state. Finite (not endless) because
/// the submit's own append completion sits BEHIND the burst in the log FIFO — the per-queue budget
/// drains the burst over several windows before that real append surfaces to commit. (The election is
/// never delayed: the stable queue has its OWN budget, so the durable self-vote can't be starved by a
/// log flood.) The burst is sized to drain well within the timeout.
struct CompactedFloodLog {
  inner: MemLog,
  filler: usize,
}

impl CompactedFloodLog {
  fn new(filler: usize) -> Self {
    Self {
      inner: MemLog::new(),
      filler,
    }
  }
}

impl sailing_proto::LogStore for CompactedFloodLog {
  type Error = std::convert::Infallible;

  fn first_index(&self) -> sailing_proto::Index {
    self.inner.first_index()
  }
  fn last_index(&self) -> sailing_proto::Index {
    self.inner.last_index()
  }
  fn term(&self, index: sailing_proto::Index) -> Result<sailing_proto::Term, Self::Error> {
    self.inner.term(index)
  }
  fn entries(
    &self,
    range: std::ops::Range<sailing_proto::Index>,
    max_bytes: u64,
  ) -> Result<sailing_proto::EntriesRead<'_>, Self::Error> {
    self.inner.entries(range, max_bytes)
  }
  fn submit_append(&mut self, id: sailing_proto::OpId, entries: &[sailing_proto::Entry]) {
    self.inner.submit_append(id, entries);
  }
  fn compact(&mut self, up_to: sailing_proto::Index) {
    self.inner.compact(up_to);
  }
  fn restore(&mut self, last_index: sailing_proto::Index, last_term: sailing_proto::Term) {
    self.inner.restore(last_index, last_term);
  }

  fn poll(&mut self) -> Option<Result<sailing_proto::LogDone, Self::Error>> {
    if self.filler > 0 {
      self.filler -= 1;
      return Some(Ok(sailing_proto::LogDone::Compacted(
        sailing_proto::Index::ZERO,
      )));
    }
    self.inner.poll()
  }

  fn has_pending(&self) -> bool {
    self.filler > 0 || self.inner.has_pending()
  }
}

/// The per-call storage-drain budget keeps a degraded LOG store's flood of `Compacted` completions
/// from trapping the run loop: a lone-voter leader still elects and commits a submit despite the log
/// handing back a huge `Compacted` burst on every drain.
#[compio::test]
async fn storage_log_flood_does_not_trap_the_run_loop() {
  let ca = TestCa::new();
  let addr: SocketAddr = "127.0.0.1:42401".parse().unwrap();
  let config = Config::try_new(1u64, vec![1u64], ELECTION, HEARTBEAT).unwrap();
  // 64 budget windows' worth of `Compacted` completions ahead of any real work.
  let (driver, handle) = CompioQuicDriver::bind(
    addr,
    config,
    1,
    CountSm::default(),
    ca.options(1, &cluster()),
    cluster(),
    Vec::new(),
    CompactedFloodLog::new(64 * 256),
    MemStable::new(),
    DriverConfig::default(),
  )
  .await
  .expect("binds");
  compio::runtime::spawn(driver.run()).detach();
  // Despite the log flood, the lone leader elects and commits the submit within the timeout — the
  // budget bounds each `handle_storage` call so the run loop is never trapped.
  let committed = compio::time::timeout(
    Duration::from_secs(10),
    submit_anywhere(std::slice::from_ref(&handle), b"under-log-flood"),
  )
  .await
  .expect("no livelock: the submit must commit despite the Compacted log flood");
  assert_eq!(committed, 1);
}

/// The QUIC membership + transfer + accessor command paths (lone-voter learner additions, a
/// non-voter transfer rejection, and the driver-side failover accessors).
#[compio::test]
async fn membership_transfer_and_accessor_commands() {
  use sailing_proto::{
    ConfChange, ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2,
  };

  let ca = TestCa::new();
  let addr: SocketAddr = "127.0.0.1:42500".parse().unwrap();
  let config = Config::try_new(1u64, vec![1u64], ELECTION, HEARTBEAT).unwrap();
  let (driver, handle) = CompioQuicDriver::bind(
    addr,
    config,
    1,
    CountSm::default(),
    ca.options(1, &cluster()),
    cluster(),
    Vec::new(),
    MemLog::new(),
    MemStable::new(),
    DriverConfig::default(),
  )
  .await
  .expect("binds");
  assert_eq!(driver.precise_releases(), 0);
  assert_eq!(driver.unprovable_floor_holds(), 0);
  compio::runtime::spawn(driver.run()).detach();

  assert_eq!(
    submit_anywhere(std::slice::from_ref(&handle), b"x").await,
    1
  );

  let deadline = std::time::Instant::now() + Duration::from_secs(10);
  let idx = loop {
    match handle
      .conf_change(ConfChange::new(
        ConfChangeType::AddLearnerNode,
        2u64,
        Bytes::new(),
      ))
      .await
    {
      Ok(i) => break i,
      Err(DriverError::NotLeader { .. }) => compio::time::sleep(Duration::from_millis(30)).await,
      Err(e) => panic!("unexpected conf_change error: {e:?}"),
    }
    assert!(
      std::time::Instant::now() < deadline,
      "never became leader to add the learner"
    );
  };

  let idx2 = loop {
    let v2 = ConfChangeV2::new(
      ConfChangeTransition::Auto,
      vec![ConfChangeSingle::new(ConfChangeType::AddLearnerNode, 3u64)],
      Bytes::new(),
    );
    match handle.conf_change_v2(v2).await {
      Ok(i) => break i,
      Err(DriverError::NotLeader { .. }) => compio::time::sleep(Duration::from_millis(30)).await,
      Err(e) => panic!("unexpected conf_change_v2 error: {e:?}"),
    }
    assert!(
      std::time::Instant::now() < deadline,
      "never became leader to add the second learner"
    );
  };
  assert!(
    idx2 > idx,
    "the joint-form change applied after the v1 change"
  );

  match handle.transfer_leader(99u64).await {
    Err(DriverError::Rejected { .. }) => {}
    other => panic!("expected Rejected for a non-voter transfer target, got {other:?}"),
  }
}

/// `transfer_leader` off the leader maps to `NotLeader`.
#[compio::test]
async fn transfer_on_a_non_leader_is_not_leader() {
  let ca = TestCa::new();
  let addrs = addrs(42_510, 3);
  let peers = vec![Node::new(2u64, addrs[1]), Node::new(3u64, addrs[2])];
  let (driver, handle) = build_node(&ca, 1, addrs[0], peers, DriverConfig::default()).await;
  compio::runtime::spawn(driver.run()).detach();

  compio::time::sleep(ELECTION * 4).await;
  match handle.transfer_leader(2u64).await {
    Err(DriverError::NotLeader { .. }) => {}
    other => panic!("expected NotLeader off the leader, got {other:?}"),
  }
}

/// A `failover_query` off the failover tier resolves `Ok(None)`.
#[compio::test]
async fn failover_query_without_a_window_falls_back() {
  let ca = TestCa::new();
  let addr: SocketAddr = "127.0.0.1:42520".parse().unwrap();
  let config = Config::try_new(1u64, vec![1u64], ELECTION, HEARTBEAT).unwrap();
  let (driver, handle) = CompioQuicDriver::bind(
    addr,
    config,
    1,
    CountSm::default(),
    ca.options(1, &cluster()),
    cluster(),
    Vec::new(),
    MemLog::new(),
    MemStable::new(),
    DriverConfig::default(),
  )
  .await
  .expect("binds");
  compio::runtime::spawn(driver.run()).detach();
  assert_eq!(
    submit_anywhere(std::slice::from_ref(&handle), b"x").await,
    1
  );

  let out: Option<u64> = handle
    .failover_query(|_fsm: &CountSm, _limbo: &[sailing_proto::Entry], _win| Some(123u64))
    .await
    .expect("the failover query resolves");
  assert_eq!(out, None, "no serve window → fall back to a normal read");
}

/// A QUIC node that crashes and RESTARTS from the same durable stores recovers and replays the
/// committed log — the recovered FSM count proves it (a fresh boot would commit 1 next, not 3).
#[compio::test]
async fn bind_restart_recovers_durable_state() {
  let ca = TestCa::new();
  let addr: SocketAddr = "127.0.0.1:42530".parse().unwrap();
  let log = SharedLog::new();
  let stable = SharedStable::new();

  let (driver, handle) = CompioQuicDriver::bind(
    addr,
    Config::try_new(1u64, vec![1u64], ELECTION, HEARTBEAT).unwrap(),
    1,
    CountSm::default(),
    ca.options(1, &cluster()),
    cluster(),
    Vec::new(),
    log.clone(),
    stable.clone(),
    DriverConfig::default(),
  )
  .await
  .expect("fresh bind");
  let task = compio::runtime::spawn(driver.run());
  assert_eq!(
    submit_anywhere(std::slice::from_ref(&handle), b"a").await,
    1
  );
  assert_eq!(
    submit_anywhere(std::slice::from_ref(&handle), b"b").await,
    2
  );
  let deadline = std::time::Instant::now() + Duration::from_secs(5);
  loop {
    let last = log.last_index();
    if stable.hard_state().commit() == last && last >= Index::new(2) {
      break;
    }
    assert!(
      std::time::Instant::now() < deadline,
      "the durable commit never caught up"
    );
    compio::time::sleep(Duration::from_millis(30)).await;
  }
  handle
    .shutdown()
    .await
    .expect("clean teardown frees the addr for restart");
  let _ = task.await;

  let (driver2, handle2) = CompioQuicDriver::bind_restart(
    addr,
    Config::try_new(1u64, vec![1u64], ELECTION, HEARTBEAT).unwrap(),
    1,
    CountSm::default(),
    1,
    ca.options(1, &cluster()),
    cluster(),
    Vec::new(),
    log.clone(),
    stable.clone(),
    DriverConfig::default(),
  )
  .await
  .expect("restart bind");
  let task2 = compio::runtime::spawn(driver2.run());

  let next = submit_anywhere(std::slice::from_ref(&handle2), b"c").await;
  assert_eq!(
    next, 3,
    "the restart replayed the durable committed log (count recovered to 2), not a fresh count-0 boot"
  );
  handle2.shutdown().await.expect("clean teardown");
  let _ = task2.await;
}

/// `bind_restart_migrating` recovers from a PRE-FORMAT QUIC store, upper-bounding the prior advertised
/// read-lease window.
#[compio::test]
async fn bind_restart_migrating_recovers_durable_state() {
  let ca = TestCa::new();
  let addr: SocketAddr = "127.0.0.1:42540".parse().unwrap();
  let log = SharedLog::new();
  let stable = SharedStable::new();

  let (driver, handle) = CompioQuicDriver::bind(
    addr,
    Config::try_new(1u64, vec![1u64], ELECTION, HEARTBEAT).unwrap(),
    1,
    CountSm::default(),
    ca.options(1, &cluster()),
    cluster(),
    Vec::new(),
    log.clone(),
    stable.clone(),
    DriverConfig::default(),
  )
  .await
  .expect("fresh bind");
  let task = compio::runtime::spawn(driver.run());
  assert_eq!(
    submit_anywhere(std::slice::from_ref(&handle), b"a").await,
    1
  );
  let deadline = std::time::Instant::now() + Duration::from_secs(5);
  loop {
    let last = log.last_index();
    if stable.hard_state().commit() == last && last >= Index::new(2) {
      break;
    }
    assert!(
      std::time::Instant::now() < deadline,
      "the durable commit never caught up"
    );
    compio::time::sleep(Duration::from_millis(30)).await;
  }
  handle.shutdown().await.expect("clean teardown");
  let _ = task.await;

  let (driver2, handle2) = CompioQuicDriver::bind_restart_migrating(
    addr,
    Config::try_new(1u64, vec![1u64], ELECTION, HEARTBEAT).unwrap(),
    1,
    CountSm::default(),
    1,
    Some(Duration::from_millis(50)),
    ca.options(1, &cluster()),
    cluster(),
    Vec::new(),
    log.clone(),
    stable.clone(),
    DriverConfig::default(),
  )
  .await
  .expect("migrating restart bind");
  let task2 = compio::runtime::spawn(driver2.run());

  let next = submit_anywhere(std::slice::from_ref(&handle2), b"b").await;
  assert_eq!(
    next, 2,
    "the migrating restart replayed the durable committed log (count recovered to 1)"
  );
  handle2.shutdown().await.expect("clean teardown");
  let _ = task2.await;
}

/// `Handle::status` over QUIC: a lone-voter leader reports Leader/self-hint/term/commit/applied and
/// the default Safe read mode.
#[compio::test]
async fn status_reports_leader_role_term_and_commit() {
  let ca = TestCa::new();
  let addr: SocketAddr = "127.0.0.1:42550".parse().unwrap();
  let config = Config::try_new(1u64, vec![1u64], ELECTION, HEARTBEAT).unwrap();
  let (driver, handle) = CompioQuicDriver::bind(
    addr,
    config,
    1,
    CountSm::default(),
    ca.options(1, &cluster()),
    cluster(),
    Vec::new(),
    MemLog::new(),
    MemStable::new(),
    DriverConfig::default(),
  )
  .await
  .expect("binds");
  compio::runtime::spawn(driver.run()).detach();

  assert_eq!(
    submit_anywhere(std::slice::from_ref(&handle), b"x").await,
    1
  );
  assert_eq!(
    submit_anywhere(std::slice::from_ref(&handle), b"y").await,
    2
  );

  let deadline = std::time::Instant::now() + Duration::from_secs(10);
  let status = loop {
    let st = handle.status().await.expect("status round-trips");
    if st.role == Role::Leader {
      break st;
    }
    assert!(
      std::time::Instant::now() < deadline,
      "the node never reported leadership via status"
    );
    compio::time::sleep(Duration::from_millis(30)).await;
  };
  assert_eq!(status.role, Role::Leader);
  assert_eq!(status.leader, Some(1));
  assert!(status.term >= Term::new(1));
  assert!(status.commit_index >= Index::new(2));
  assert!(status.applied_index >= Index::new(2));
  assert_eq!(status.active_read_mode, ReadOnlyOption::Safe);
  assert!(!status.is_poisoned);
  assert!(status.conf_state.voters().contains(&1));
}

/// A QUIC read-mode migration Safe → LeaseBased surfaces as `Event::ReadModeChanged`.
#[compio::test]
async fn set_read_mode_migrates_the_active_mode() {
  let ca = TestCa::new();
  let addr: SocketAddr = "127.0.0.1:42560".parse().unwrap();
  let config = Config::try_new(1u64, vec![1u64], ELECTION, HEARTBEAT)
    .unwrap()
    .with_check_quorum(true);
  let (driver, handle) = CompioQuicDriver::bind(
    addr,
    config,
    1,
    CountSm::default(),
    ca.options(1, &cluster()),
    cluster(),
    Vec::new(),
    MemLog::new(),
    MemStable::new(),
    DriverConfig::default(),
  )
  .await
  .expect("binds");
  compio::runtime::spawn(driver.run()).detach();

  assert_eq!(
    submit_anywhere(std::slice::from_ref(&handle), b"x").await,
    1
  );

  let deadline = std::time::Instant::now() + Duration::from_secs(10);
  let proposed = loop {
    match handle.set_read_mode(ReadOnlyOption::LeaseBased).await {
      Ok(index) => break index,
      Err(DriverError::NotLeader { .. }) => compio::time::sleep(Duration::from_millis(30)).await,
      Err(e) => panic!("unexpected set_read_mode error: {e:?}"),
    }
    assert!(
      std::time::Instant::now() < deadline,
      "never became leader to migrate"
    );
  };

  let deadline = std::time::Instant::now() + Duration::from_secs(10);
  let changed = loop {
    let mut seen = None;
    while let Ok(ev) = handle.events().try_recv() {
      if let Event::ReadModeChanged(rmc) = ev {
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
    compio::time::sleep(Duration::from_millis(30)).await;
  };
  assert_eq!(changed.mode(), ReadOnlyOption::LeaseBased);
  assert!(changed.index() >= proposed);
}

/// Membership/query commands on a non-leader QUIC node take the propose/read ERROR arms (NotLeader).
#[compio::test]
async fn commands_on_a_non_leader_redirect() {
  use sailing_proto::{
    ConfChange, ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2,
  };

  let ca = TestCa::new();
  let addrs = addrs(42_570, 3);
  let peers = vec![Node::new(2u64, addrs[1]), Node::new(3u64, addrs[2])];
  let (driver, handle) = build_node(&ca, 1, addrs[0], peers, DriverConfig::default()).await;
  compio::runtime::spawn(driver.run()).detach();

  compio::time::sleep(ELECTION * 4).await;
  match handle
    .conf_change(ConfChange::new(
      ConfChangeType::AddLearnerNode,
      4u64,
      Bytes::new(),
    ))
    .await
  {
    Err(DriverError::NotLeader { .. }) => {}
    other => panic!("expected NotLeader for conf_change off the leader, got {other:?}"),
  }
  match handle
    .conf_change_v2(ConfChangeV2::new(
      ConfChangeTransition::Auto,
      vec![ConfChangeSingle::new(ConfChangeType::AddLearnerNode, 5u64)],
      Bytes::new(),
    ))
    .await
  {
    Err(DriverError::NotLeader { .. }) => {}
    other => panic!("expected NotLeader for conf_change_v2 off the leader, got {other:?}"),
  }
  match handle.query(|sm: &CountSm| sm.count()).await {
    Err(DriverError::NotLeader { .. }) => {}
    other => panic!("expected the no-leader redirect for a query off the leader, got {other:?}"),
  }
}

/// A LeaseBased migration WITHOUT `check_quorum` is rejected at the proposer.
#[compio::test]
async fn set_read_mode_without_check_quorum_is_rejected() {
  let ca = TestCa::new();
  let addr: SocketAddr = "127.0.0.1:42580".parse().unwrap();
  let config = Config::try_new(1u64, vec![1u64], ELECTION, HEARTBEAT).unwrap();
  let (driver, handle) = CompioQuicDriver::bind(
    addr,
    config,
    1,
    CountSm::default(),
    ca.options(1, &cluster()),
    cluster(),
    Vec::new(),
    MemLog::new(),
    MemStable::new(),
    DriverConfig::default(),
  )
  .await
  .expect("binds");
  compio::runtime::spawn(driver.run()).detach();
  assert_eq!(
    submit_anywhere(std::slice::from_ref(&handle), b"x").await,
    1
  );

  let deadline = std::time::Instant::now() + Duration::from_secs(10);
  loop {
    match handle.set_read_mode(ReadOnlyOption::LeaseBased).await {
      Err(DriverError::Rejected { .. }) => break,
      Err(DriverError::NotLeader { .. }) => compio::time::sleep(Duration::from_millis(30)).await,
      other => panic!("expected Rejected without check_quorum, got {other:?}"),
    }
    assert!(
      std::time::Instant::now() < deadline,
      "never became leader to attempt the migration"
    );
  }
}
