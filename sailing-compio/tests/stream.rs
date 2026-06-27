//! Real-socket integration for the stream driver: three nodes over loopback TCP — plaintext
//! (`Labeled<Passthrough>`) and TLS (`Labeled<TlsRecords>`) — through real listeners, dials,
//! split-half bridges, and redials.

mod common;

use std::{net::SocketAddr, rc::Rc, sync::Arc, time::Duration};

use bytes::Bytes;
use common::{CountSm, MemLog, MemStable, SharedLog, SharedStable};
use sailing_compio::{CompioStreamDriver, DriverConfig, DriverError, Handle};
use sailing_proto::{
  ClusterId, Config, Data, Event, Index, LabelOptions, Labeled, LogStore, Passthrough,
  ReadOnlyOption, Role, StableStore, Term, TlsRecords,
};

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
        compio::time::sleep(Duration::from_millis(50)).await;
      }
      Err(DriverError::Superseded) => {}
      Err(e) => panic!("unexpected submit error: {e:?}"),
    }
  }
}

/// `bind` must REJECT an out-of-range programmatic `DriverConfig` (whose serde/clap parse path would
/// have caught it) rather than build a driver with a pathological submit budget. An over-ceiling
/// `max_inflight` exceeds the submit-budget ceiling; a zero redial base hot-loops. Both must surface
/// as `BindError::DriverConfig`. The validation runs before the socket binds, so the bogus address
/// is never touched.
#[compio::test]
async fn bind_rejects_out_of_range_driver_config() {
  use sailing_compio::{BindError, MAX_CHANNEL_CAPACITY};

  let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
  let config = Config::try_new(1u64, vec![1u64], ELECTION, HEARTBEAT).unwrap();
  let dialer: sailing_compio::DialerFactory<u64, Labeled<Passthrough>> =
    Rc::new(move |_peer: &u64| {
      Labeled::dialer(
        Passthrough::new(),
        &LabelOptions {
          cluster: cluster(),
          local_id: encoded(1),
        },
      )
      .map_err(std::io::Error::other)
    });
  let acceptor: sailing_compio::AcceptorFactory<Labeled<Passthrough>> = Rc::new(move || {
    Labeled::acceptor(
      Passthrough::new(),
      &LabelOptions {
        cluster: cluster(),
        local_id: encoded(1),
      },
    )
    .map_err(std::io::Error::other)
  });

  // `max_inflight` at the channel-capacity ceiling — above the submit-budget cap `validate` enforces.
  let over_inflight = DriverConfig {
    max_inflight: MAX_CHANNEL_CAPACITY,
    ..DriverConfig::default()
  };
  let res = CompioStreamDriver::bind(
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

  // A zero redial base (a hot retry loop) is likewise a startup rejection, not a silent build.
  let zero_redial = DriverConfig {
    redial_base: Duration::ZERO,
    ..DriverConfig::default()
  };
  let res = CompioStreamDriver::bind(
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

/// Three plaintext-TCP nodes elect, commit through redirects, and serve a linearizable query —
/// the full stream-driver stack over real sockets.
#[compio::test]
async fn three_node_plaintext_cluster_commits_and_queries() {
  let addrs = addrs(43_000);
  let mut handles = Vec::new();
  for id in 1u64..=3 {
    let peers: Vec<_> = (1u64..=3)
      .filter(|&p| p != id)
      .map(|p| (p, addrs[(p - 1) as usize]))
      .collect();
    let config = Config::try_new(id, vec![1u64, 2, 3], ELECTION, HEARTBEAT).unwrap();
    let local = encoded(id);
    let dial_local = local.clone();
    let dialer: sailing_compio::DialerFactory<u64, Labeled<Passthrough>> =
      Rc::new(move |_peer: &u64| {
        Labeled::dialer(
          Passthrough::new(),
          &LabelOptions {
            cluster: cluster(),
            local_id: dial_local.clone(),
          },
        )
        .map_err(std::io::Error::other)
      });
    let acceptor: sailing_compio::AcceptorFactory<Labeled<Passthrough>> = Rc::new(move || {
      Labeled::acceptor(
        Passthrough::new(),
        &LabelOptions {
          cluster: cluster(),
          local_id: local.clone(),
        },
      )
      .map_err(std::io::Error::other)
    });
    let (driver, handle) = CompioStreamDriver::bind(
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
    compio::runtime::spawn(driver.run()).detach();
    handles.push(handle);
  }

  assert_eq!(submit_anywhere(&handles, b"alpha").await, 1);
  assert_eq!(submit_anywhere(&handles, b"beta").await, 2);

  // LATE liveness: the mutual-dial mesh must still carry traffic well after the duplicate
  // tie-break storm at boot has resolved. The failure modes this guards — steady redial churn
  // evicting bound survivors, and the symmetric tie-break killing both survivors with no
  // repair scheduled — pass the early submits above and only surface now.
  compio::time::sleep(Duration::from_millis(800)).await;
  assert_eq!(submit_anywhere(&handles, b"gamma").await, 3);

  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  let count = loop {
    assert!(std::time::Instant::now() < deadline, "no query in time");
    if let Some(c) = {
      let mut served = None;
      for h in &handles {
        if let Ok(c) = h.query(|sm: &CountSm| sm.count()).await {
          served = Some(c);
          break;
        }
      }
      served
    } {
      break c;
    }
    compio::time::sleep(Duration::from_millis(50)).await;
  };
  assert!(count >= 3, "the linearizable read sees all three commits");
}

/// The same cluster over TLS: per-node rcgen certs chained to a shared CA, the dialer deriving
/// each peer's server name, the hello riding encrypted inside the session.
#[compio::test]
async fn three_node_tls_cluster_commits() {
  // Pin the process-level provider: with BOTH of the proto's provider features enabled (as an
  // all-features build does), rustls cannot auto-select one and the bare config builders panic.
  let _ = rustls::crypto::ring::default_provider().install_default();

  // A shared CA + per-node cert/key, minted once and shared into the factories.
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

  let addrs = addrs(43_100);
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
    let dialer: sailing_compio::DialerFactory<u64, Labeled<TlsRecords>> = {
      let client_cfg = client_cfg.clone();
      Rc::new(move |peer: &u64| {
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
    let acceptor: sailing_compio::AcceptorFactory<Labeled<TlsRecords>> = Rc::new(move || {
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
    let (driver, handle) = CompioStreamDriver::bind(
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
    compio::runtime::spawn(driver.run()).detach();
    handles.push(handle);
  }

  assert_eq!(submit_anywhere(&handles, b"tls-op").await, 1);
}

/// The stream shutdown carries the same immediate-rebind contract as the QUIC driver's, for every
/// coalesced caller: two `Handle` clones shut down concurrently and BOTH must resolve before the
/// listener address is rebindable — a swap-loser awaits real teardown, never an early `Ok`.
#[compio::test]
async fn stream_shutdown_means_immediate_rebind_for_every_coalesced_caller() {
  let addr: SocketAddr = "127.0.0.1:43200".parse().unwrap();
  let config = Config::try_new(1u64, vec![1u64, 2, 3], ELECTION, HEARTBEAT).unwrap();
  let local = encoded(1);
  let dial_local = local.clone();
  let dialer: sailing_compio::DialerFactory<u64, Labeled<Passthrough>> = Rc::new(move |_: &u64| {
    Labeled::dialer(
      Passthrough::new(),
      &LabelOptions {
        cluster: cluster(),
        local_id: dial_local.clone(),
      },
    )
    .map_err(std::io::Error::other)
  });
  let acceptor: sailing_compio::AcceptorFactory<Labeled<Passthrough>> = Rc::new(move || {
    Labeled::acceptor(
      Passthrough::new(),
      &LabelOptions {
        cluster: cluster(),
        local_id: local.clone(),
      },
    )
    .map_err(std::io::Error::other)
  });
  let (driver, handle) = CompioStreamDriver::bind(
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
  let task = compio::runtime::spawn(driver.run());
  // Two coalesced callers JOINED: both must resolve before the rebind, proving the swap-loser awaits
  // the driver's listener `close().await` rather than returning early.
  let (a, b) = futures_util::future::join(handle.shutdown(), clone.shutdown()).await;
  a.expect("the winner resolves after teardown");
  b.expect("the loser resolves after teardown");
  let rebound = compio::net::TcpListener::bind(addr)
    .await
    .expect("immediately rebindable once every shutdown caller has resolved");
  drop(rebound);
  let _ = task.await;
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
  let addr: SocketAddr = "127.0.0.1:43201".parse().unwrap();
  let config = Config::try_new(1u64, vec![1u64], ELECTION, HEARTBEAT).unwrap();
  let local = encoded(1);
  let dial_local = local.clone();
  let dialer: sailing_compio::DialerFactory<u64, Labeled<Passthrough>> = Rc::new(move |_: &u64| {
    Labeled::dialer(
      Passthrough::new(),
      &LabelOptions {
        cluster: cluster(),
        local_id: dial_local.clone(),
      },
    )
    .map_err(std::io::Error::other)
  });
  let acceptor: sailing_compio::AcceptorFactory<Labeled<Passthrough>> = Rc::new(move || {
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
  let (driver, handle) = CompioStreamDriver::bind(
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

/// Plaintext dialer/acceptor factories for node `id` — the smallest stream setup the single-node
/// tests below need (no peers, so the mesh is never actually dialed).
fn plain_factories(
  id: u64,
) -> (
  sailing_compio::DialerFactory<u64, Labeled<Passthrough>>,
  sailing_compio::AcceptorFactory<Labeled<Passthrough>>,
) {
  let local = encoded(id);
  let dial_local = local.clone();
  let dialer: sailing_compio::DialerFactory<u64, Labeled<Passthrough>> = Rc::new(move |_: &u64| {
    Labeled::dialer(
      Passthrough::new(),
      &LabelOptions {
        cluster: cluster(),
        local_id: dial_local.clone(),
      },
    )
    .map_err(std::io::Error::other)
  });
  let acceptor: sailing_compio::AcceptorFactory<Labeled<Passthrough>> = Rc::new(move || {
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
#[compio::test]
async fn restart_recovers_durable_state_instead_of_booting_fresh() {
  let addr: SocketAddr = "127.0.0.1:43300".parse().unwrap();
  let log = SharedLog::new();
  let stable = SharedStable::new();

  // Boot 1: a fresh single-voter node elects itself and commits three entries.
  let (dialer, acceptor) = plain_factories(1);
  let (driver, handle) = CompioStreamDriver::bind(
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
  let task = compio::runtime::spawn(driver.run());
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
    compio::time::sleep(Duration::from_millis(30)).await;
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
  let (driver2, handle2) = CompioStreamDriver::bind_restart(
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
  let task2 = compio::runtime::spawn(driver2.run());

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

