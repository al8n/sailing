//! Real-socket integration for the reactor stream driver: three nodes over loopback TCP — plaintext
//! (`Labeled<Passthrough>`) and TLS (`Labeled<TlsRecords>`) — through real listeners, dials,
//! split-half bridges, and redials, on a multi-thread tokio runtime (which proves the `Send` `run()`).

mod common;

use std::{net::SocketAddr, sync::Arc, time::Duration};

use agnostic::tokio::TokioRuntime;
use bytes::Bytes;
use common::{CountSm, MemLog, MemStable};
use sailing_proto::{ClusterId, Config, Data, LabelOptions, Labeled, Passthrough, TlsRecords};
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
