//! Real-socket integration for the stream driver: three nodes over loopback TCP — plaintext
//! (`Labeled<Passthrough>`) and TLS (`Labeled<TlsRecords>`) — through real listeners, dials,
//! split-half bridges, and redials.

mod common;

use std::{net::SocketAddr, rc::Rc, sync::Arc, time::Duration};

use bytes::Bytes;
use common::{CountSm, MemLog, MemStable};
use sailing_compio::{CompioStreamDriver, DriverConfig, DriverError, Handle};
use sailing_proto::{ClusterId, Config, Data, LabelOptions, Labeled, Passthrough, TlsRecords};

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
/// have caught it) rather than panic deep in the channel sizing. An over-ceiling `max_inflight`
/// trips `futures_channel`'s `MAX_BUFFER` assert at `mpsc::channel(max_inflight + 1)`; a zero redial
/// base hot-loops. Both must surface as `BindError::DriverConfig`. The validation runs before the
/// socket binds, so the bogus address is never touched.
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

  // `max_inflight` whose `+ 1` clears the `usize::MAX` overflow check yet exceeds `MAX_BUFFER` — the
  // exact value that panicked `bind` before the up-front `validate`.
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

/// The stream shutdown ack carries the same immediate-rebind contract as the QUIC driver's.
#[compio::test]
async fn stream_shutdown_ack_means_immediate_rebind() {
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
  let task = compio::runtime::spawn(driver.run());
  handle.shutdown().await.expect("acks");
  let rebound = compio::net::TcpListener::bind(addr)
    .await
    .expect("immediately rebindable");
  drop(rebound);
  let _ = task.await;
}
