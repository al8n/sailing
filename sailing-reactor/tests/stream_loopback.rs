//! Real-socket integration for the reactor stream driver: three nodes over loopback TCP — plaintext
//! (`Labeled<Passthrough>`) and TLS (`Labeled<TlsRecords>`) — through real listeners, dials,
//! split-half bridges, and redials, on a multi-thread tokio runtime (which proves the `Send` `run()`).

mod common;

use std::{net::SocketAddr, sync::Arc, time::Duration};

use agnostic::tokio::TokioRuntime;
use bytes::Bytes;
use common::{CountSm, MemLog, MemStable, SharedLog, SharedStable};
use sailing_proto::{
  ClusterId, Config, Data, Event, Index, LabelOptions, Labeled, LogStore, Passthrough,
  ReadOnlyOption, Role, StableStore, Term, TlsRecords,
};
use sailing_reactor::{DriverConfig, DriverError, Handle, ReactorStreamDriver};

const ELECTION: Duration = Duration::from_millis(300);
const HEARTBEAT: Duration = Duration::from_millis(60);

fn cluster() -> ClusterId {
  ClusterId([9; 16])
}

fn encoded(id: u64) -> Vec<u8> {
  let mut v = Vec::new();
  id.encode(&mut v);
  v
}

fn addrs(base_port: u16) -> Vec<SocketAddr> {
  (0..3)
    .map(|i| format!("127.0.0.1:{}", base_port + i).parse().unwrap())
    .collect()
}

/// Submit through whichever node is (or redirects to) the leader.
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
      Err(DriverError::NotLeader { leader }) => {
        at = leader
          .map(|l| (l - 1) as usize)
          .unwrap_or((at + 1) % handles.len());
        tokio::time::sleep(Duration::from_millis(50)).await;
      }
      Err(DriverError::Superseded) => {}
      Err(e) => panic!("unexpected submit error: {e:?}"),
    }
  }
}

/// `bind` must REJECT an out-of-range programmatic `DriverConfig` rather than build a driver with a
/// pathological submit budget — the validation runs before the socket binds, so the bogus address is
/// never touched. Identical contract to the compio driver's.
#[tokio::test(flavor = "multi_thread")]
async fn bind_rejects_out_of_range_driver_config() {
  use sailing_reactor::{BindError, MAX_CHANNEL_CAPACITY};

  let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
  let config = Config::try_new(1u64, vec![1u64], ELECTION, HEARTBEAT).unwrap();
  let dialer: sailing_reactor::DialerFactory<u64, Labeled<Passthrough>> =
    Arc::new(move |_peer: &u64| {
      Labeled::dialer(
        Passthrough::new(),
        &LabelOptions {
          cluster: cluster(),
          local_id: encoded(1),
        },
      )
      .map_err(std::io::Error::other)
    });
  let acceptor: sailing_reactor::AcceptorFactory<Labeled<Passthrough>> = Arc::new(move || {
    Labeled::acceptor(
      Passthrough::new(),
      &LabelOptions {
        cluster: cluster(),
        local_id: encoded(1),
      },
    )
    .map_err(std::io::Error::other)
  });

  let over_inflight = DriverConfig {
    max_inflight: MAX_CHANNEL_CAPACITY,
    ..DriverConfig::default()
  };
  let res = ReactorStreamDriver::<TokioRuntime, _, _, _, _, _>::bind(
    addr,
    config.clone(),
    1,
    CountSm::default(),
    vec![],
    dialer.clone(),
    acceptor.clone(),
    MemLog::new(),
    MemStable::new(),
    over_inflight,
  )
  .await;
  assert!(
    matches!(res, Err(BindError::DriverConfig(_))),
    "an over-ceiling max_inflight must be rejected at bind, not panic"
  );

  let zero_redial = DriverConfig {
    redial_base: Duration::ZERO,
    ..DriverConfig::default()
  };
  let res = ReactorStreamDriver::<TokioRuntime, _, _, _, _, _>::bind(
    addr,
    config,
    1,
    CountSm::default(),
    vec![],
    dialer,
    acceptor,
    MemLog::new(),
    MemStable::new(),
    zero_redial,
  )
  .await;
  assert!(
    matches!(res, Err(BindError::DriverConfig(_))),
    "a zero redial base must be rejected at bind"
  );
}

/// Three plaintext-TCP nodes elect, commit through redirects, and serve a linearizable query — the
/// full reactor stream-driver stack over real sockets on a multi-thread runtime.
#[tokio::test(flavor = "multi_thread")]
async fn three_node_plaintext_cluster_commits_and_queries() {
  let addrs = addrs(43_400);
  let mut handles = Vec::new();
  for id in 1u64..=3 {
    let peers: Vec<_> = (1u64..=3)
      .filter(|&p| p != id)
      .map(|p| (p, addrs[(p - 1) as usize]))
      .collect();
    let config = Config::try_new(id, vec![1u64, 2, 3], ELECTION, HEARTBEAT).unwrap();
    let local = encoded(id);
    let dial_local = local.clone();
    let dialer: sailing_reactor::DialerFactory<u64, Labeled<Passthrough>> =
      Arc::new(move |_peer: &u64| {
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
    let (driver, handle) = ReactorStreamDriver::<TokioRuntime, _, _, _, _, _>::bind(
      addrs[(id - 1) as usize],
      config,
      id,
      CountSm::default(),
      peers,
      dialer,
      acceptor,
      MemLog::new(),
      MemStable::new(),
      DriverConfig::default(),
    )
    .await
    .expect("driver binds");
    tokio::spawn(driver.run());
    handles.push(handle);
  }

  assert_eq!(submit_anywhere(&handles, b"alpha").await, 1);
  assert_eq!(submit_anywhere(&handles, b"beta").await, 2);

  // LATE liveness: the mutual-dial mesh must still carry traffic well after the boot tie-break storm.
  tokio::time::sleep(Duration::from_millis(800)).await;
  assert_eq!(submit_anywhere(&handles, b"gamma").await, 3);

  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  let count = loop {
    assert!(std::time::Instant::now() < deadline, "no query in time");
    let mut served = None;
    for h in &handles {
      if let Ok(c) = h.query(|sm: &CountSm| sm.count()).await {
        served = Some(c);
        break;
      }
    }
    if let Some(c) = served {
      break c;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
  };
  assert!(count >= 3, "the linearizable read sees all three commits");
}

/// The same cluster over TLS: per-node rcgen certs chained to a shared CA, the dialer deriving each
/// peer's server name, the hello riding encrypted inside the session.
#[tokio::test(flavor = "multi_thread")]
async fn three_node_tls_cluster_commits() {
  let _ = rustls::crypto::ring::default_provider().install_default();

  let ca = {
    let mut params = rcgen::CertificateParams::new(Vec::new()).expect("CA params");
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    params.key_usages.push(rcgen::KeyUsagePurpose::KeyCertSign);
    let key = rcgen::KeyPair::generate().expect("CA key");
    let cert = params.self_signed(&key).expect("CA cert");
    (
      cert.der().to_vec(),
      rcgen::Issuer::new(
        rcgen::CertificateParams::new(Vec::new()).expect("issuer params"),
        key,
      ),
    )
  };
  let mut roots = rustls::RootCertStore::empty();
  roots
    .add(rustls::pki_types::CertificateDer::from(ca.0.clone()))
    .expect("CA root");
  let roots = Arc::new(roots);

  let node_name = |id: u64| format!("node-{id}.sailing.test");
  let mint = |id: u64| {
    let mut params = rcgen::CertificateParams::new(vec![node_name(id)]).expect("SAN params");
    params
      .extended_key_usages
      .push(rcgen::ExtendedKeyUsagePurpose::ServerAuth);
    let key = rcgen::KeyPair::generate().expect("leaf key");
    let cert = params.signed_by(&key, &ca.1).expect("leaf");
    (cert.der().to_vec(), key.serialize_der())
  };

  let addrs = addrs(43_500);
  let mut handles = Vec::new();
  for id in 1u64..=3 {
    let peers: Vec<_> = (1u64..=3)
      .filter(|&p| p != id)
      .map(|p| (p, addrs[(p - 1) as usize]))
      .collect();
    let config = Config::try_new(id, vec![1u64, 2, 3], ELECTION, HEARTBEAT).unwrap();

    let (cert_der, key_der) = mint(id);
    let server_cfg = Arc::new(
      rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
          vec![rustls::pki_types::CertificateDer::from(cert_der)],
          rustls::pki_types::PrivateKeyDer::try_from(key_der).expect("key"),
        )
        .expect("server config"),
    );
    let client_cfg = Arc::new(
      rustls::ClientConfig::builder()
        .with_root_certificates(roots.as_ref().clone())
        .with_no_client_auth(),
    );

    let local = encoded(id);
    let dial_local = local.clone();
    let dialer: sailing_reactor::DialerFactory<u64, Labeled<TlsRecords>> = {
      let client_cfg = client_cfg.clone();
      Arc::new(move |peer: &u64| {
        let name = rustls::pki_types::ServerName::try_from(node_name(*peer))
          .map_err(std::io::Error::other)?;
        let tls = TlsRecords::client(client_cfg.clone(), name).map_err(std::io::Error::other)?;
        Labeled::dialer(
          tls,
          &LabelOptions {
            cluster: cluster(),
            local_id: dial_local.clone(),
          },
        )
        .map_err(std::io::Error::other)
      })
    };
    let acceptor: sailing_reactor::AcceptorFactory<Labeled<TlsRecords>> = Arc::new(move || {
      let tls = TlsRecords::server(server_cfg.clone()).map_err(std::io::Error::other)?;
      Labeled::acceptor(
        tls,
        &LabelOptions {
          cluster: cluster(),
          local_id: local.clone(),
        },
      )
      .map_err(std::io::Error::other)
    });
    let (driver, handle) = ReactorStreamDriver::<TokioRuntime, _, _, _, _, _>::bind(
      addrs[(id - 1) as usize],
      config,
      id,
      CountSm::default(),
      peers,
      dialer,
      acceptor,
      MemLog::new(),
      MemStable::new(),
      DriverConfig::default(),
    )
    .await
    .expect("driver binds");
    tokio::spawn(driver.run());
    handles.push(handle);
  }

  assert_eq!(submit_anywhere(&handles, b"tls-op").await, 1);
}

/// The stream shutdown carries the immediate-rebind contract for every coalesced caller: two `Handle`
/// clones shut down concurrently and BOTH must resolve before the listener address is rebindable.
#[tokio::test(flavor = "multi_thread")]
async fn stream_shutdown_means_immediate_rebind_for_every_coalesced_caller() {
  let addr: SocketAddr = "127.0.0.1:43600".parse().unwrap();
  let config = Config::try_new(1u64, vec![1u64, 2, 3], ELECTION, HEARTBEAT).unwrap();
  let local = encoded(1);
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
  let (driver, handle) = ReactorStreamDriver::<TokioRuntime, _, _, _, _, _>::bind(
    addr,
    config,
    1,
    CountSm::default(),
    Vec::new(),
    dialer,
    acceptor,
    MemLog::new(),
    MemStable::new(),
    DriverConfig::default(),
  )
  .await
  .expect("binds");
  let clone = handle.clone();
  let task = tokio::spawn(driver.run());
  // Two coalesced callers JOINED: both must resolve before the rebind, proving the swap-loser awaits
  // the driver's listener fd-release rather than returning early.
  let (a, b) = futures_util::future::join(handle.shutdown(), clone.shutdown()).await;
  a.expect("the winner resolves after teardown");
  b.expect("the loser resolves after teardown");
  let rebound = std::net::TcpListener::bind(addr)
    .expect("immediately rebindable once every shutdown caller has resolved");
  drop(rebound);
  let _ = task.await;
}

/// The coalescing `storage_ready` contract (a `flume::bounded(1)` channel written with `try_send`) keeps a
/// noisy notifier from EITHER livelocking the loop OR growing memory: the single slot bounds the queue and
/// the loop's bounded drain coalesces it (`handle_storage` does the real store work each pass regardless).
/// A lone-voter node (it elects itself) must still commit a submit while its notifier is hammered.
#[tokio::test(flavor = "multi_thread")]
async fn storage_ready_flood_does_not_livelock_the_run_loop() {
  let addr: SocketAddr = "127.0.0.1:43700".parse().unwrap();
  let config = Config::try_new(1u64, vec![1u64], ELECTION, HEARTBEAT).unwrap();
  let local = encoded(1);
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
  // The coalescing storage-ready channel — the SUPPORTED contract: a single slot the notifier `try_send`s
  // into (an unbounded one is rejected at bind), wired into the driver through the config seam.
  let (ready_tx, ready_rx) = flume::bounded(1);
  let driver_cfg = DriverConfig {
    storage_ready: Some(ready_rx),
    ..DriverConfig::default()
  };
  let (driver, handle) = ReactorStreamDriver::<TokioRuntime, _, _, _, _, _>::bind(
    addr,
    config,
    1,
    CountSm::default(),
    Vec::new(),
    dialer,
    acceptor,
    MemLog::new(),
    MemStable::new(),
    driver_cfg,
  )
  .await
  .expect("binds");
  tokio::spawn(driver.run());
  // Hammer the notifier continuously: the single bounded slot coalesces it (`try_send` drops when full),
  // so the channel cannot grow and the loop's bounded drain cannot be trapped — the submit must commit.
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
  // Despite the flood, the lone leader must still elect and commit the submit — the loop makes progress.
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
  let addr: SocketAddr = "127.0.0.1:43702".parse().unwrap();
  let config = Config::try_new(1u64, vec![1u64], ELECTION, HEARTBEAT).unwrap();
  let local = encoded(1);
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
  // 64 budget windows' worth of `Compacted` completions ahead of any real work.
  let (driver, handle) = ReactorStreamDriver::<TokioRuntime, _, _, _, _, _>::bind(
    addr,
    config,
    1,
    CountSm::default(),
    Vec::new(),
    dialer,
    acceptor,
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

/// Plaintext dialer/acceptor factories for node `id` — the smallest stream setup the single-node
/// tests below need (no peers, so the mesh is never actually dialed).
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

/// A node that crashes and RESTARTS from the same durable stores must RECOVER its persisted
/// term/vote/commit and replay the committed log — never boot fresh at term 0 (which would let it
/// double-vote). `bind_restart` reconciles the durable stores through `Endpoint::restart`; a plain
/// `bind` would discard them. The recovered FSM count is the decisive proof the committed log replayed:
/// a fresh boot's count would be 0, so the next commit would return 1, not 4.
#[tokio::test(flavor = "multi_thread")]
async fn restart_recovers_durable_state_instead_of_booting_fresh() {
  let addr: SocketAddr = "127.0.0.1:43800".parse().unwrap();
  let log = SharedLog::new();
  let stable = SharedStable::new();

  // Boot 1: a fresh single-voter node elects itself and commits three entries.
  let (dialer, acceptor) = plain_factories(1);
  let (driver, handle) = ReactorStreamDriver::<TokioRuntime, _, _, _, _, _>::bind(
    addr,
    Config::try_new(1u64, vec![1u64], ELECTION, HEARTBEAT).unwrap(),
    1,
    CountSm::default(),
    Vec::new(),
    dialer,
    acceptor,
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
  assert_eq!(
    submit_anywhere(std::slice::from_ref(&handle), b"c").await,
    3
  );

  // Wait for the durable HardState commit to catch up to the WHOLE committed log, so the restart's
  // recovery is deterministic (every committed Normal entry is replayed, none stranded uncommitted).
  let deadline = std::time::Instant::now() + Duration::from_secs(5);
  loop {
    let last = log.last_index();
    if stable.hard_state().commit() == last && last >= Index::new(3) {
      break;
    }
    assert!(
      std::time::Instant::now() < deadline,
      "the durable commit never caught up to the committed log"
    );
    tokio::time::sleep(Duration::from_millis(30)).await;
  }
  // The durable ground truth the restart must recover: a real term, a self-vote, a non-trivial commit.
  let durable = stable.hard_state();
  assert!(
    durable.term() >= Term::new(1),
    "a leader advanced the durable term"
  );
  assert_eq!(
    durable.vote(),
    Some(1),
    "the single voter persisted its self-vote"
  );
  assert!(durable.commit() >= Index::new(3));

  // Crash: shut driver 1 down, KEEPING the shared stores (our clones outlive it).
  handle
    .shutdown()
    .await
    .expect("clean teardown frees the addr for restart");
  let _ = task.await;

  // Boot 2: RESTART (not a fresh bind) from the same durable stores; boot_epoch = 1 > the fresh 0.
  let (dialer, acceptor) = plain_factories(1);
  let (driver2, handle2) = ReactorStreamDriver::<TokioRuntime, _, _, _, _, _>::bind_restart(
    addr,
    Config::try_new(1u64, vec![1u64], ELECTION, HEARTBEAT).unwrap(),
    1,
    CountSm::default(),
    1,
    Vec::new(),
    dialer,
    acceptor,
    log.clone(),
    stable.clone(),
    DriverConfig::default(),
  )
  .await
  .expect("restart bind");
  let task2 = tokio::spawn(driver2.run());

  // The recovered node replayed the three committed entries (FSM count = 3), so the next commit
  // returns 4. A FRESH boot would have discarded the stores: count 0, so the next commit would be 1.
  let next = submit_anywhere(std::slice::from_ref(&handle2), b"d").await;
  assert_eq!(
    next, 4,
    "the restart replayed the durable committed log (count recovered to 3), not a fresh count-0 boot"
  );

  handle2.shutdown().await.expect("clean teardown");
  let _ = task2.await;
}

/// `Handle::set_read_mode` drives a mid-life read-mode migration the proto already supports but no
/// `Command`/`Handle` path reached before. A leader migrates Safe -> LeaseBased; the change applies
/// cluster-wide once the `SetReadMode` entry commits, surfacing as `Event::ReadModeChanged` on the
/// events tail.
#[tokio::test(flavor = "multi_thread")]
async fn set_read_mode_migrates_the_active_mode() {
  let addr: SocketAddr = "127.0.0.1:43810".parse().unwrap();
  let (dialer, acceptor) = plain_factories(1);
  // LeaseBased requires check_quorum on the proposer (the migration validity gate).
  let config = Config::try_new(1u64, vec![1u64], ELECTION, HEARTBEAT)
    .unwrap()
    .with_check_quorum(true);
  let (driver, handle) = ReactorStreamDriver::<TokioRuntime, _, _, _, _, _>::bind(
    addr,
    config,
    1,
    CountSm::default(),
    Vec::new(),
    dialer,
    acceptor,
    MemLog::new(),
    MemStable::new(),
    DriverConfig::default(),
  )
  .await
  .expect("binds");
  tokio::spawn(driver.run());

  // Establish leadership.
  assert_eq!(
    submit_anywhere(std::slice::from_ref(&handle), b"x").await,
    1
  );

  // Migrate Safe -> LeaseBased (retrying until this node is the leader).
  let deadline = std::time::Instant::now() + Duration::from_secs(10);
  let proposed = loop {
    match handle.set_read_mode(ReadOnlyOption::LeaseBased).await {
      Ok(index) => break index,
      Err(DriverError::NotLeader { .. }) => {
        tokio::time::sleep(Duration::from_millis(30)).await;
      }
      Err(e) => panic!("unexpected set_read_mode error: {e:?}"),
    }
    assert!(
      std::time::Instant::now() < deadline,
      "never became leader to migrate"
    );
  };

  // The migration takes effect apply-time: observe ReadModeChanged for the new mode on the tail.
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
    tokio::time::sleep(Duration::from_millis(30)).await;
  };
  assert_eq!(
    changed.mode(),
    ReadOnlyOption::LeaseBased,
    "the active read mode migrated to LeaseBased"
  );
  assert!(
    changed.index() >= proposed,
    "the migration applied at (or after) the proposed index"
  );
}

/// `Handle::status` surfaces the runtime consensus state — previously unreachable from the cross-thread
/// handle — via a oneshot round-trip. A single-voter leader reports `role = Leader`, the self leader
/// hint, a real term, the committed/applied indices, and the default (Safe) active read mode.
#[tokio::test(flavor = "multi_thread")]
async fn status_reports_leader_role_term_and_commit() {
  let addr: SocketAddr = "127.0.0.1:43820".parse().unwrap();
  let (dialer, acceptor) = plain_factories(1);
  let (driver, handle) = ReactorStreamDriver::<TokioRuntime, _, _, _, _, _>::bind(
    addr,
    Config::try_new(1u64, vec![1u64], ELECTION, HEARTBEAT).unwrap(),
    1,
    CountSm::default(),
    Vec::new(),
    dialer,
    acceptor,
    MemLog::new(),
    MemStable::new(),
    DriverConfig::default(),
  )
  .await
  .expect("binds");
  tokio::spawn(driver.run());

  // Commit two entries so commit/applied are non-trivial.
  assert_eq!(
    submit_anywhere(std::slice::from_ref(&handle), b"x").await,
    1
  );
  assert_eq!(
    submit_anywhere(std::slice::from_ref(&handle), b"y").await,
    2
  );

  // Poll status until this node reports leadership, then assert the full snapshot.
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
    tokio::time::sleep(Duration::from_millis(30)).await;
  };
  assert_eq!(status.role, Role::Leader);
  assert_eq!(status.leader, Some(1), "the leader hint is self");
  assert!(status.term >= Term::new(1));
  assert!(
    status.commit_index >= Index::new(2),
    "both submits committed, got {:?}",
    status.commit_index
  );
  assert!(status.applied_index >= Index::new(2));
  assert_eq!(
    status.active_read_mode,
    ReadOnlyOption::Safe,
    "the default read mode is Safe"
  );
  assert!(!status.is_poisoned);
  assert!(
    status.conf_state.voters().contains(&1),
    "the lone voter is in the membership"
  );
}

/// REGRESSION: a `query` issued AFTER a committed read-mode migration — with NO further entry appended
/// — must COMPLETE. A committed `SetReadMode` reports `Event::ReadModeChanged`, not `Applied`, so
/// unless `route_event` advances the apply watermark on that event, the query confirms at the
/// migration's index and then parks FOREVER (no later `Applied` lifts it), stranding its budget
/// reservation. The per-query timeout turns that hang into a clear failure: before the watermark fix
/// this loops until the deadline; after, the first query returns the read.
#[tokio::test(flavor = "multi_thread")]
async fn query_completes_after_read_mode_migration() {
  let addr: SocketAddr = "127.0.0.1:43830".parse().unwrap();
  let (dialer, acceptor) = plain_factories(1);
  let config = Config::try_new(1u64, vec![1u64], ELECTION, HEARTBEAT)
    .unwrap()
    .with_check_quorum(true);
  let (driver, handle) = ReactorStreamDriver::<TokioRuntime, _, _, _, _, _>::bind(
    addr,
    config,
    1,
    CountSm::default(),
    Vec::new(),
    dialer,
    acceptor,
    MemLog::new(),
    MemStable::new(),
    DriverConfig::default(),
  )
  .await
  .expect("binds");
  tokio::spawn(driver.run());

  // Establish leadership and a known FSM: one committed Normal entry.
  assert_eq!(
    submit_anywhere(std::slice::from_ref(&handle), b"x").await,
    1
  );

  // Migrate to another mode (retrying until leader). The SetReadMode is the LAST entry — nothing is
  // appended after it, so only its ReadModeChanged can advance the watermark.
  let deadline = std::time::Instant::now() + Duration::from_secs(10);
  loop {
    match handle.set_read_mode(ReadOnlyOption::LeaseBased).await {
      Ok(_) => break,
      Err(DriverError::NotLeader { .. }) => tokio::time::sleep(Duration::from_millis(30)).await,
      Err(e) => panic!("unexpected set_read_mode error: {e:?}"),
    }
    assert!(
      std::time::Instant::now() < deadline,
      "never became leader to migrate"
    );
  }
  // Wait until the migration APPLIES (ReadModeChanged on the tail).
  let deadline = std::time::Instant::now() + Duration::from_secs(10);
  loop {
    let mut applied = false;
    while let Ok(ev) = handle.events().try_recv() {
      if matches!(ev, Event::ReadModeChanged(_)) {
        applied = true;
      }
    }
    if applied {
      break;
    }
    assert!(
      std::time::Instant::now() < deadline,
      "the read-mode migration never applied"
    );
    tokio::time::sleep(Duration::from_millis(30)).await;
  }

  // The decisive check: a query AFTER the migration, with no further append. A per-query timeout
  // distinguishes "parked forever" (the bug) from a transient retry; the overall deadline fails the
  // test if the query never completes.
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  let count = loop {
    assert!(
      std::time::Instant::now() < deadline,
      "the post-migration query never completed — a committed read-mode change must advance the apply \
       watermark"
    );
    match tokio::time::timeout(
      Duration::from_secs(2),
      handle.query(|sm: &CountSm| sm.count()),
    )
    .await
    {
      Ok(Ok(c)) => break c,
      // A transient redirect/supersede: retry. A PARKED query (the bug) elapses the per-query timeout
      // and also retries — until the overall deadline above fails the test.
      Ok(Err(_)) | Err(_) => tokio::time::sleep(Duration::from_millis(30)).await,
    }
  };
  assert_eq!(
    count, 1,
    "the linearizable read observes the one committed Normal entry"
  );
}

/// REGRESSION: a `query` on a fresh leader whose ONLY committed entry is its `Empty` no-op must
/// COMPLETE. The no-op advances the endpoint's applied index but emits NO routed event, so without
/// `Routing::sync_applied` the driver watermark stays 0 and the read — confirmed at the no-op index —
/// parks forever. The per-query timeout turns that hang into a clear failure.
#[tokio::test(flavor = "multi_thread")]
async fn query_after_noop_tail() {
  let addr: SocketAddr = "127.0.0.1:43840".parse().unwrap();
  let (dialer, acceptor) = plain_factories(1);
  let (driver, handle) = ReactorStreamDriver::<TokioRuntime, _, _, _, _, _>::bind(
    addr,
    Config::try_new(1u64, vec![1u64], ELECTION, HEARTBEAT).unwrap(),
    1,
    CountSm::default(),
    Vec::new(),
    dialer,
    acceptor,
    MemLog::new(),
    MemStable::new(),
    DriverConfig::default(),
  )
  .await
  .expect("binds");
  tokio::spawn(driver.run());

  // Become leader WITHOUT appending any Normal entry: the only committed entry is the Empty no-op.
  let deadline = std::time::Instant::now() + Duration::from_secs(10);
  loop {
    if handle.status().await.expect("status round-trips").role == Role::Leader {
      break;
    }
    assert!(
      std::time::Instant::now() < deadline,
      "the node never became leader"
    );
    tokio::time::sleep(Duration::from_millis(30)).await;
  }

  // The query confirms at the no-op index and must run. Before the watermark sync it parks (the
  // watermark stays 0 with no Applied event behind the no-op).
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  let count = loop {
    assert!(
      std::time::Instant::now() < deadline,
      "the query never completed — the eventless no-op apply did not advance the driver watermark"
    );
    match tokio::time::timeout(
      Duration::from_secs(2),
      handle.query(|sm: &CountSm| sm.count()),
    )
    .await
    {
      Ok(Ok(c)) => break c,
      Ok(Err(_)) | Err(_) => tokio::time::sleep(Duration::from_millis(30)).await,
    }
  };
  assert_eq!(
    count, 0,
    "no Normal entry committed, so the read sees count 0"
  );
}

/// REGRESSION: a `query` right after `bind_restart`, BEFORE any post-restart write, must COMPLETE.
/// Restart replays the committed log (the endpoint's applied index recovers high) but CLEARS the
/// replay events and starts a ZEROED `Routing`, so without `Routing::sync_applied` the driver
/// watermark stays 0 and the post-restart read parks forever.
#[tokio::test(flavor = "multi_thread")]
async fn query_after_restart_before_write() {
  let addr: SocketAddr = "127.0.0.1:43850".parse().unwrap();
  let log = SharedLog::new();
  let stable = SharedStable::new();

  // Boot 1: commit one Normal entry, then crash (keeping the durable stores).
  let (dialer, acceptor) = plain_factories(1);
  let (driver, handle) = ReactorStreamDriver::<TokioRuntime, _, _, _, _, _>::bind(
    addr,
    Config::try_new(1u64, vec![1u64], ELECTION, HEARTBEAT).unwrap(),
    1,
    CountSm::default(),
    Vec::new(),
    dialer,
    acceptor,
    log.clone(),
    stable.clone(),
    DriverConfig::default(),
  )
  .await
  .expect("fresh bind");
  let task = tokio::spawn(driver.run());
  assert_eq!(
    submit_anywhere(std::slice::from_ref(&handle), b"x").await,
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

  // Boot 2: RESTART, then query BEFORE any post-restart write.
  let (dialer, acceptor) = plain_factories(1);
  let (driver2, handle2) = ReactorStreamDriver::<TokioRuntime, _, _, _, _, _>::bind_restart(
    addr,
    Config::try_new(1u64, vec![1u64], ELECTION, HEARTBEAT).unwrap(),
    1,
    CountSm::default(),
    1,
    Vec::new(),
    dialer,
    acceptor,
    log.clone(),
    stable.clone(),
    DriverConfig::default(),
  )
  .await
  .expect("restart bind");
  tokio::spawn(driver2.run());

  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  let count = loop {
    assert!(
      std::time::Instant::now() < deadline,
      "the post-restart query never completed — the recovered applied index did not advance the \
       driver watermark"
    );
    match tokio::time::timeout(
      Duration::from_secs(2),
      handle2.query(|sm: &CountSm| sm.count()),
    )
    .await
    {
      Ok(Ok(c)) => break c,
      Ok(Err(_)) | Err(_) => tokio::time::sleep(Duration::from_millis(30)).await,
    }
  };
  assert_eq!(count, 1, "the read sees the one recovered committed entry");
}
