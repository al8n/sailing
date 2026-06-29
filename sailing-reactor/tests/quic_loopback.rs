//! Real-socket integration for the reactor QUIC driver: three nodes over loopback UDP, cluster-private
//! mTLS, real quinn-proto datagrams and timers, on a multi-thread tokio runtime (which proves the
//! `Send` `run()`). The readiness sibling of the compio QUIC loopback suite — same scenarios, the
//! `agnostic` runtime in place of compio's proactor.

mod common;

use std::{net::SocketAddr, time::Duration};

use agnostic::tokio::TokioRuntime;
use bytes::Bytes;
use common::{CountSm, MemLog, MemStable, SharedLog, SharedStable, TestCa};
use sailing_proto::{ClusterId, Config, Index, LogStore, StableStore};
use sailing_reactor::{DriverConfig, DriverError, Handle, Node, ReactorQuicDriver};

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
  ReactorQuicDriver<TokioRuntime, u64, CountSm, MemLog, MemStable>,
  Handle<u64, CountSm>,
) {
  let config = Config::try_new(id, vec![1u64, 2, 3], ELECTION, HEARTBEAT).unwrap();
  ReactorQuicDriver::<TokioRuntime, _, _, _, _>::bind(
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

/// Spawn a full 3-node cluster, each driver detached on this test's runtime; returns the handles
/// indexed by node id - 1.
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
    tokio::spawn(driver.run());
    handles.push(handle);
  }
  handles
}

/// Submit through whichever node is (or redirects to) the leader, retrying the NotLeader hint until
/// the cluster elects and the command commits.
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
      // Redirect: the hint names the leader; no hint yet means no leader yet — try the next node
      // after a beat.
      Err(DriverError::NotLeader { leader }) => {
        at = leader
          .map(|l| (l - 1) as usize)
          .unwrap_or((at + 1) % handles.len());
        tokio::time::sleep(Duration::from_millis(50)).await;
      }
      // A leadership change voided the outcome: retry (the test payload is idempotent).
      Err(DriverError::Superseded) => {}
      Err(e) => panic!("unexpected submit error: {e:?}"),
    }
  }
}

/// `bind` must REJECT an out-of-range programmatic `DriverConfig` rather than build a driver with a
/// pathological submit budget — the QUIC counterpart of the stream driver's identical guard. An
/// over-ceiling `max_inflight` exceeds the submit-budget ceiling and is rejected. The `validate` runs
/// before the UDP socket binds, so the bogus address is never touched.
#[tokio::test(flavor = "multi_thread")]
async fn bind_rejects_out_of_range_driver_config() {
  use sailing_reactor::{BindError, MAX_CHANNEL_CAPACITY};

  let ca = TestCa::new();
  let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
  let config = Config::try_new(1u64, vec![1u64], ELECTION, HEARTBEAT).unwrap();
  let over_inflight = DriverConfig {
    max_inflight: MAX_CHANNEL_CAPACITY,
    ..DriverConfig::default()
  };
  let res = ReactorQuicDriver::<TokioRuntime, _, _, _, _>::bind(
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

/// The gate: a real 3-node cluster over mandatory-mTLS QUIC on loopback elects, commits a command
/// submitted with NotLeader redirects, answers through the submitting handle, and serves a
/// linearizable query against the leader's state machine.
#[tokio::test(flavor = "multi_thread")]
async fn three_node_cluster_commits_and_queries() {
  let ca = TestCa::new();
  let handles = spawn_cluster(&ca, 43_800).await;

  let response = submit_anywhere(&handles, b"hello").await;
  assert_eq!(response, 1, "the first committed command counts to one");

  // A second command through any node (the redirect loop finds the leader again).
  let response = submit_anywhere(&handles, b"world").await;
  assert_eq!(response, 2);

  // A linearizable query: runs against the FSM on the driver task at a confirmed read index. The
  // node whose submit succeeds is leader-adjacent; sailing forwards follower reads, so any node
  // serves.
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
    tokio::time::sleep(Duration::from_millis(50)).await;
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

/// The budget gate is at the HANDLE, before anything queues: a payload larger than the byte budget is
/// Busy synchronously — no cluster, no timing.
#[tokio::test(flavor = "multi_thread")]
async fn submit_budget_exhaustion_is_busy() {
  let ca = TestCa::new();
  let addrs = addrs(43_810, 1);
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
#[tokio::test(flavor = "multi_thread")]
async fn quic_shutdown_means_immediate_rebind_for_every_coalesced_caller() {
  let ca = TestCa::new();
  let addr: SocketAddr = "127.0.0.1:43820".parse().unwrap();

  let (driver, handle) = build_node(&ca, 1, addr, Vec::new(), DriverConfig::default()).await;
  let clone = handle.clone();
  let task = tokio::spawn(driver.run());
  // Two coalesced callers (only one wins the enqueue swap); JOIN them so BOTH resolve before the
  // rebind. If a loser returned `Ok` before the driver's socket release, the rebind below would race
  // the still-open fd and could fail with `AddrInUse`.
  let (a, b) = futures_util::future::join(handle.shutdown(), clone.shutdown()).await;
  a.expect("the winner resolves after teardown");
  b.expect("the loser resolves after teardown");
  // Both resolved ⇒ the fd is RELEASED — not merely that teardown was scheduled.
  let rebound = std::net::UdpSocket::bind(addr)
    .expect("the address is immediately rebindable once every shutdown caller has resolved");
  drop(rebound);
  let _ = task.await;

  // Post-shutdown operations fail with the typed teardown error.
  match handle.submit(Bytes::from_static(b"late")).await {
    Err(DriverError::ShuttingDown) => {}
    other => panic!("expected ShuttingDown, got {other:?}"),
  }
}

/// A node with no quorum never leads: submits are NotLeader (no silent parking), and the redirect
/// hint is absent while no leader is known.
#[tokio::test(flavor = "multi_thread")]
async fn no_quorum_means_not_leader_not_a_hang() {
  let ca = TestCa::new();
  let addrs = addrs(43_830, 3);
  // Only node 1 runs; 2 and 3 are configured but never started.
  let peers = vec![Node::new(2u64, addrs[1]), Node::new(3u64, addrs[2])];
  let (driver, handle) = build_node(&ca, 1, addrs[0], peers, DriverConfig::default()).await;
  tokio::spawn(driver.run());

  // Give it a few election timeouts: without quorum it can never win.
  tokio::time::sleep(ELECTION * 4).await;
  match handle.submit(Bytes::from_static(b"nope")).await {
    Err(DriverError::NotLeader { leader }) => {
      assert_eq!(leader, None, "no leader is known without a quorum");
    }
    other => panic!("expected NotLeader, got {other:?}"),
  }
}

/// A storage fault fail-stops the endpoint (poison); the driver must fail everything parked with the
/// TYPED verdict — not strand it holding budget — and exit its run loop.
#[tokio::test(flavor = "multi_thread")]
async fn storage_fault_poisons_with_a_typed_verdict() {
  use common::PoisonableLog;

  let ca = TestCa::new();
  let addr: SocketAddr = "127.0.0.1:43840".parse().unwrap();
  // A single-voter cluster: elects itself and commits without peers, so the only failure injected is
  // the storage fault.
  let config = Config::try_new(1u64, vec![1u64], ELECTION, HEARTBEAT).unwrap();
  let (log, fail_appends) = PoisonableLog::new();
  let (driver, handle) = ReactorQuicDriver::<TokioRuntime, _, _, _, _>::bind(
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
  let task = tokio::spawn(driver.run());

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
        tokio::time::sleep(Duration::from_millis(30)).await;
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

  // The run loop exited on the poison: the driver task ends and later operations surface the teardown
  // error.
  let _ = task.await;
  match handle.submit(Bytes::from_static(b"late")).await {
    Err(DriverError::ShuttingDown) => {}
    other => panic!("expected ShuttingDown, got {other:?}"),
  }
}

/// The coalescing `storage_ready` contract (a `flume::bounded(1)` channel written with `try_send`) keeps a
/// noisy notifier from EITHER livelocking the loop OR growing memory: the single slot bounds the queue and
/// the loop's bounded drain coalesces it (`handle_storage` does the real store work each pass regardless).
/// A lone-voter node (it elects itself) must still commit a submit while its notifier is hammered.
#[tokio::test(flavor = "multi_thread")]
async fn storage_ready_flood_does_not_livelock_the_run_loop() {
  let ca = TestCa::new();
  let addr: SocketAddr = "127.0.0.1:43850".parse().unwrap();
  let config = Config::try_new(1u64, vec![1u64], ELECTION, HEARTBEAT).unwrap();
  // The coalescing storage-ready channel — the SUPPORTED contract: a single slot the notifier
  // `try_send`s into (an unbounded one is rejected at bind), wired into the driver through the config
  // seam.
  let (ready_tx, ready_rx) = flume::bounded(1);
  let driver_cfg = DriverConfig {
    storage_ready: Some(ready_rx),
    ..DriverConfig::default()
  };
  let (driver, handle) = ReactorQuicDriver::<TokioRuntime, _, _, _, _>::bind(
    addr,
    config,
    1,
    CountSm::default(),
    ca.options(1, &cluster()),
    cluster(),
    Vec::new(),
    MemLog::new(),
    MemStable::new(),
    driver_cfg,
  )
  .await
  .expect("binds");
  tokio::spawn(driver.run());
  // Hammer the notifier continuously: the single bounded slot coalesces it (`try_send` drops when
  // full), so the channel cannot grow and the loop's bounded drain cannot be trapped — the submit
  // must commit.
  let flood = tokio::spawn(async move {
    loop {
      for _ in 0..512 {
        // Coalesce (drop on a full slot); stop only when the driver drops its receiver.
        if matches!(
          ready_tx.try_send(()),
          Err(flume::TrySendError::Disconnected(_))
        ) {
          return;
        }
      }
      tokio::task::yield_now().await;
    }
  });
  // Despite the flood, the lone leader must still elect and commit the submit — the loop makes
  // progress.
  let committed = tokio::time::timeout(
    Duration::from_secs(10),
    submit_anywhere(std::slice::from_ref(&handle), b"under-flood"),
  )
  .await
  .expect("no livelock: the submit must commit despite the storage_ready flood");
  assert_eq!(committed, 1);
  flood.abort();
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
/// handing back a huge `Compacted` burst on every drain. (Sibling of
/// `storage_ready_flood_does_not_livelock_the_run_loop`, stressing the LOG queue rather than the
/// notifier channel.)
#[tokio::test(flavor = "multi_thread")]
async fn storage_log_flood_does_not_trap_the_run_loop() {
  let ca = TestCa::new();
  let addr: SocketAddr = "127.0.0.1:43852".parse().unwrap();
  let config = Config::try_new(1u64, vec![1u64], ELECTION, HEARTBEAT).unwrap();
  // 64 budget windows' worth of `Compacted` completions ahead of any real work.
  let (driver, handle) = ReactorQuicDriver::<TokioRuntime, _, _, _, _>::bind(
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
  tokio::spawn(driver.run());
  // Despite the log flood, the lone leader elects and commits the submit — the budget bounds each
  // `handle_storage` call so the run loop is never trapped.
  let committed = tokio::time::timeout(
    Duration::from_secs(10),
    submit_anywhere(std::slice::from_ref(&handle), b"under-log-flood"),
  )
  .await
  .expect("no livelock: the submit must commit despite the Compacted log flood");
  assert_eq!(committed, 1);
}

/// The QUIC membership + transfer + accessor command paths. A lone-voter leader applies two learner
/// additions (`conf_change` and `conf_change_v2`), then a transfer to a non-voter is rejected with
/// the typed reason — covering the QUIC driver's Conf/ConfV2/Transfer arms and the error mappers. The
/// driver-side failover counters read zero on a fresh monotonic node.
#[tokio::test(flavor = "multi_thread")]
async fn membership_transfer_and_accessor_commands() {
  use sailing_proto::{
    ConfChange, ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2,
  };

  let ca = TestCa::new();
  let addr: SocketAddr = "127.0.0.1:43870".parse().unwrap();
  let config = Config::try_new(1u64, vec![1u64], ELECTION, HEARTBEAT).unwrap();
  let (driver, handle) = ReactorQuicDriver::<TokioRuntime, _, _, _, _>::bind(
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
  tokio::spawn(driver.run());

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
      Err(DriverError::NotLeader { .. }) => tokio::time::sleep(Duration::from_millis(30)).await,
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
      Err(DriverError::NotLeader { .. }) => tokio::time::sleep(Duration::from_millis(30)).await,
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

/// `transfer_leader` off the leader maps to `NotLeader`: a node with no reachable quorum never leads.
#[tokio::test(flavor = "multi_thread")]
async fn transfer_on_a_non_leader_is_not_leader() {
  let ca = TestCa::new();
  let addrs = addrs(43_872, 3);
  let peers = vec![Node::new(2u64, addrs[1]), Node::new(3u64, addrs[2])];
  let (driver, handle) = build_node(&ca, 1, addrs[0], peers, DriverConfig::default()).await;
  tokio::spawn(driver.run());

  tokio::time::sleep(ELECTION * 4).await;
  match handle.transfer_leader(2u64).await {
    Err(DriverError::NotLeader { .. }) => {}
    other => panic!("expected NotLeader off the leader, got {other:?}"),
  }
}

/// A `failover_query` off the failover tier resolves `Ok(None)` — covering the QUIC driver's
/// FailoverWindow command arm and `run_failover_serve`'s no-window arm.
#[tokio::test(flavor = "multi_thread")]
async fn failover_query_without_a_window_falls_back() {
  let ca = TestCa::new();
  let addr: SocketAddr = "127.0.0.1:43874".parse().unwrap();
  let config = Config::try_new(1u64, vec![1u64], ELECTION, HEARTBEAT).unwrap();
  let (driver, handle) = ReactorQuicDriver::<TokioRuntime, _, _, _, _>::bind(
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
  tokio::spawn(driver.run());
  assert_eq!(
    submit_anywhere(std::slice::from_ref(&handle), b"x").await,
    1
  );

  let out: Option<u64> = handle
    .failover_query(|_fsm: &CountSm, _limbo: &[sailing_proto::Entry], _win| Some(123u64))
    .await
    .expect("the failover query resolves");
  assert_eq!(
    out, None,
    "no serve window off the failover tier → fall back to a normal read"
  );
}

/// A QUIC node that crashes and RESTARTS from the same durable stores recovers its persisted
/// term/vote/commit and replays the committed log — never boots fresh at term 0. The recovered FSM
/// count is the decisive proof: a fresh boot would commit 1 next, not 3.
#[tokio::test(flavor = "multi_thread")]
async fn bind_restart_recovers_durable_state() {
  let ca = TestCa::new();
  let addr: SocketAddr = "127.0.0.1:43876".parse().unwrap();
  let log = SharedLog::new();
  let stable = SharedStable::new();

  // Boot 1: a fresh lone voter commits two entries.
  let (driver, handle) = ReactorQuicDriver::<TokioRuntime, _, _, _, _>::bind(
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
  let task = tokio::spawn(driver.run());
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
    tokio::time::sleep(Duration::from_millis(30)).await;
  }
  handle
    .shutdown()
    .await
    .expect("clean teardown frees the addr for restart");
  let _ = task.await;

  // Boot 2: RESTART from the same durable stores (boot_epoch 1 > the fresh 0).
  let (driver2, handle2) = ReactorQuicDriver::<TokioRuntime, _, _, _, _>::bind_restart(
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
  let task2 = tokio::spawn(driver2.run());

  let next = submit_anywhere(std::slice::from_ref(&handle2), b"c").await;
  assert_eq!(
    next, 3,
    "the restart replayed the durable committed log (count recovered to 2), not a fresh count-0 boot"
  );
  handle2.shutdown().await.expect("clean teardown");
  let _ = task2.await;
}

/// `bind_restart_migrating` recovers from a PRE-FORMAT QUIC store, upper-bounding the prior advertised
/// read-lease window. The recovered FSM count proves the committed log replayed.
#[tokio::test(flavor = "multi_thread")]
async fn bind_restart_migrating_recovers_durable_state() {
  let ca = TestCa::new();
  let addr: SocketAddr = "127.0.0.1:43878".parse().unwrap();
  let log = SharedLog::new();
  let stable = SharedStable::new();

  let (driver, handle) = ReactorQuicDriver::<TokioRuntime, _, _, _, _>::bind(
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
  let task = tokio::spawn(driver.run());
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
    tokio::time::sleep(Duration::from_millis(30)).await;
  }
  handle.shutdown().await.expect("clean teardown");
  let _ = task.await;

  let (driver2, handle2) = ReactorQuicDriver::<TokioRuntime, _, _, _, _>::bind_restart_migrating(
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
  let task2 = tokio::spawn(driver2.run());

  let next = submit_anywhere(std::slice::from_ref(&handle2), b"b").await;
  assert_eq!(
    next, 2,
    "the migrating restart replayed the durable committed log (count recovered to 1)"
  );
  handle2.shutdown().await.expect("clean teardown");
  let _ = task2.await;
}

/// The recv task holds an `Arc` clone of the UDP socket; if teardown only SCHEDULED its abort, a
/// `recv_from` parked with no inbound datagrams could keep the fd bound past `shutdown().await`,
/// flaking an immediate rebind with `AddrInUse`. The driver instead AWAITS the recv task's join before
/// dropping the socket, so the fd-release is synchronous — proven by repeated bind/run/shutdown/rebind
/// cycles on the same address, the recv task parked in `recv_from` each time.
#[tokio::test(flavor = "multi_thread")]
async fn quic_shutdown_releases_the_udp_fd_for_repeated_immediate_rebind() {
  let addr: SocketAddr = "127.0.0.1:43860".parse().unwrap();
  for _ in 0..12 {
    let ca = TestCa::new();
    let (driver, handle) = build_node(&ca, 1, addr, Vec::new(), DriverConfig::default()).await;
    let task = tokio::spawn(driver.run());
    // The recv task is parked in `recv_from` here (no peers, no datagrams).
    handle.shutdown().await.expect("clean shutdown");
    // The fd must be released synchronously: an immediate rebind of the SAME address succeeds.
    std::net::UdpSocket::bind(addr).expect("immediately rebindable after shutdown");
    let _ = task.await;
  }
}
