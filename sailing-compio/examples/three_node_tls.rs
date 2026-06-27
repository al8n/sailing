//! Three sailing nodes over real loopback TCP with TLS, one driver thread each.
//!
//! ```sh
//! cargo run -p sailing-compio --example three_node_tls
//! ```
//!
//! The stream-driver twin of `three_node_quic`: the same PRODUCTION shape — one thread, one
//! compio `Runtime`, one listener, one set of stores per node, constructed AND run on that
//! thread — but consensus rides framed reliable streams instead of datagrams. The record layer
//! is the embedder's choice through the two factories; this example builds
//! `Labeled<TlsRecords>` (the cluster hello riding encrypted inside per-node-cert TLS), while
//! `Labeled<Passthrough>` would give plaintext TCP with the identical driver.
//!
//! What the embedder supplies beyond the QUIC example:
//! - **A record-layer pair** — the dialer factory receives the PEER, so it derives the server
//!   name to verify (here `node-<id>.sailing.example` per leaf cert); the acceptor factory
//!   builds the server side. Both return `io::Result`: a mis-built layer is retried by the
//!   link reconciler like a failed connect.
//! - **The TLS material** — a real deployment provisions per-node certs from its PKI; this
//!   example mints a throwaway CA and one leaf per node in main, then moves each node's
//!   DER-encoded material into its thread (rustls configs are built ON the node's thread).

use std::{net::SocketAddr, rc::Rc, sync::Arc, time::Duration};

use bytes::Bytes;
use sailing_compio::{CompioStreamDriver, DriverConfig, DriverError, Node};
use sailing_proto::{ClusterId, Config, Data, LabelOptions, Labeled, TlsRecords};

#[path = "../tests/common/mod.rs"]
mod common;
use common::{CountSm, MemLog, MemStable};

fn node_name(id: u64) -> String {
  format!("node-{id}.sailing.example")
}

fn main() {
  // Pin the process-level rustls provider once, up front: when both provider crate features
  // end up enabled (an all-features build), rustls cannot auto-select and the bare config
  // builders panic.
  let _ = rustls::crypto::ring::default_provider().install_default();

  let cluster = ClusterId([42; 16]);
  let addrs: Vec<SocketAddr> = (0..3)
    .map(|i| format!("127.0.0.1:{}", 47_100 + i).parse().unwrap())
    .collect();

  // A throwaway cluster CA and one leaf per node, SAN'd to the name the dialer derives. Minted
  // in main; only DER bytes (plain `Vec<u8>`, `Send`) cross into the node threads.
  let (ca_der, leaves) = {
    let mut params = rcgen::CertificateParams::new(Vec::new()).expect("CA params");
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    params.key_usages.push(rcgen::KeyUsagePurpose::KeyCertSign);
    let key = rcgen::KeyPair::generate().expect("CA key");
    let cert = params.self_signed(&key).expect("CA cert");
    let issuer = rcgen::Issuer::new(
      rcgen::CertificateParams::new(Vec::new()).expect("issuer params"),
      key,
    );
    let leaves: Vec<(Vec<u8>, Vec<u8>)> = (1u64..=3)
      .map(|id| {
        let mut params = rcgen::CertificateParams::new(vec![node_name(id)]).expect("SAN params");
        params
          .extended_key_usages
          .push(rcgen::ExtendedKeyUsagePurpose::ServerAuth);
        let key = rcgen::KeyPair::generate().expect("leaf key");
        let cert = params.signed_by(&key, &issuer).expect("leaf");
        (cert.der().to_vec(), key.serialize_der())
      })
      .collect();
    (cert.der().to_vec(), leaves)
  };

  // One driver thread per node; the handle comes back over a plain std channel.
  let (handle_tx, handle_rx) = std::sync::mpsc::channel();
  let mut threads = Vec::new();
  for id in 1u64..=3 {
    let addrs = addrs.clone();
    let ca_der = ca_der.clone();
    let (cert_der, key_der) = leaves[(id - 1) as usize].clone();
    let handle_tx = handle_tx.clone();
    threads.push(std::thread::spawn(move || {
      compio::runtime::Runtime::new()
        .expect("runtime")
        .block_on(async move {
          let peers: Vec<_> = (1u64..=3)
            .filter(|&p| p != id)
            .map(|p| Node::new(p, addrs[(p - 1) as usize]))
            .collect();
          let config = Config::try_new(
            id,
            vec![1u64, 2, 3],
            Duration::from_millis(300),
            Duration::from_millis(60),
          )
          .expect("config");

          // The rustls configs are built HERE, on the node's thread, from the moved DER bytes.
          let mut roots = rustls::RootCertStore::empty();
          roots
            .add(rustls::pki_types::CertificateDer::from(ca_der))
            .expect("CA root");
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
              .with_root_certificates(roots)
              .with_no_client_auth(),
          );

          // The label layer carries the cluster id + node id hello INSIDE the TLS session.
          let local_id = {
            let mut v = Vec::new();
            id.encode(&mut v);
            v
          };
          let dial_local = local_id.clone();
          let dialer: sailing_compio::DialerFactory<u64, Labeled<TlsRecords>> =
            Rc::new(move |peer: &u64| {
              let name = rustls::pki_types::ServerName::try_from(node_name(*peer))
                .map_err(std::io::Error::other)?;
              let tls =
                TlsRecords::client(client_cfg.clone(), name).map_err(std::io::Error::other)?;
              Labeled::dialer(
                tls,
                &LabelOptions {
                  cluster,
                  local_id: dial_local.clone(),
                },
              )
              .map_err(std::io::Error::other)
            });
          let acceptor: sailing_compio::AcceptorFactory<Labeled<TlsRecords>> = Rc::new(move || {
            let tls = TlsRecords::server(server_cfg.clone()).map_err(std::io::Error::other)?;
            Labeled::acceptor(
              tls,
              &LabelOptions {
                cluster,
                local_id: local_id.clone(),
              },
            )
            .map_err(std::io::Error::other)
          });

          let (driver, handle) = CompioStreamDriver::bind(
            addrs[(id - 1) as usize],
            config,
            id, // election-jitter seed
            CountSm::default(),
            peers,
            dialer,
            acceptor,
            MemLog::new(),
            MemStable::new(),
            DriverConfig::default(),
          )
          .await
          .expect("bind");
          handle_tx.send((id, handle)).expect("hand back the handle");
          // Drop our sender clone now: main collects the channel to its END, and the iterator
          // only ends when every sender is gone — a clone held for the driver's whole life
          // would park main forever.
          drop(handle_tx);
          // The driver runs until shutdown (or every handle clone drops).
          driver.run().await;
        });
    }));
  }
  drop(handle_tx);

  let mut handles: Vec<_> = handle_rx.iter().collect();
  handles.sort_by_key(|(id, _)| *id);
  let handles: Vec<_> = handles.into_iter().map(|(_, h)| h).collect();

  // Submit a few commands from the MAIN thread, following NotLeader redirects — the cluster
  // elects on its own timers underneath.
  let submit = |payload: &'static [u8]| {
    let mut at = 0usize;
    loop {
      match futures_executor::block_on(handles[at].submit(Bytes::from_static(payload))) {
        Ok(count) => return count,
        Err(DriverError::NotLeader { leader }) => {
          at = leader
            .map(|l| (l - 1) as usize)
            .unwrap_or((at + 1) % handles.len());
          std::thread::sleep(Duration::from_millis(50));
        }
        Err(DriverError::Superseded) => {} // retry: the payload is idempotent here
        Err(e) => panic!("submit failed: {e}"),
      }
    }
  };

  for (i, payload) in [&b"alpha"[..], b"beta", b"gamma"].iter().enumerate() {
    let count = submit(payload);
    println!("committed op {} -> applied count {count}", i + 1);
  }

  // A linearizable query: the closure runs on the serving node's driver thread against its
  // state machine, after a confirmed read index is applied.
  let count = handles
    .iter()
    .find_map(|h| futures_executor::block_on(h.query(|sm: &CountSm| sm.count())).ok())
    .expect("some node serves the read");
  println!("linearizable read -> {count}");
  assert_eq!(count, 3);

  // Orderly teardown: each ack means that node's listener is already rebindable.
  for h in &handles {
    let _ = futures_executor::block_on(h.shutdown());
  }
  for t in threads {
    let _ = t.join();
  }
  println!("done");
}
