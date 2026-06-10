use super::*;
use crate::Instant;
use rustls::{
  ClientConfig, RootCertStore, ServerConfig,
  pki_types::{CertificateDer, PrivateKeyDer},
};
use std::{sync::Arc, vec::Vec};

/// Build a client+server config pair backed by a fresh self-signed cert for "localhost".
fn test_configs() -> (Arc<ClientConfig>, Arc<ServerConfig>) {
  let _ = rustls::crypto::ring::default_provider().install_default();
  let cert = rcgen::generate_simple_self_signed(std::vec!["localhost".to_string()]).unwrap();
  let cert_der = CertificateDer::from(cert.cert.der().to_vec());
  let key_der = PrivateKeyDer::try_from(cert.signing_key.serialize_der()).unwrap();

  let server = ServerConfig::builder()
    .with_no_client_auth()
    .with_single_cert(std::vec![cert_der.clone()], key_der)
    .unwrap();

  let mut roots = RootCertStore::empty();
  roots.add(cert_der).unwrap();
  let client = ClientConfig::builder()
    .with_root_certificates(roots)
    .with_no_client_auth();

  (Arc::new(client), Arc::new(server))
}

/// Shuttle ciphertext between the two TLS layers until the handshake settles.
fn pump(c: &mut TlsRecords, s: &mut TlsRecords) {
  for _ in 0..32 {
    let mut wc = Vec::new();
    c.poll_transport_transmit(&mut wc);
    if !wc.is_empty() {
      s.handle_transport_data(&wc, Instant::ORIGIN);
    }
    let mut ws = Vec::new();
    s.poll_transport_transmit(&mut ws);
    if !ws.is_empty() {
      c.handle_transport_data(&ws, Instant::ORIGIN);
    }
    if wc.is_empty() && ws.is_empty() {
      break;
    }
  }
}

#[test]
fn completes_handshake_and_carries_plaintext() {
  let (client_cfg, server_cfg) = test_configs();
  let mut c = TlsRecords::client(client_cfg, "localhost".try_into().unwrap()).unwrap();
  let mut s = TlsRecords::server(server_cfg).unwrap();
  assert!(c.is_handshaking());
  assert!(TlsRecords::is_secure());

  pump(&mut c, &mut s);
  assert!(!c.is_handshaking(), "client handshake completes");
  assert!(!s.is_handshaking(), "server handshake completes");

  // Application plaintext flows, encrypted on the wire.
  c.write_plaintext(b"secret");
  pump(&mut c, &mut s);
  let mut got = Vec::new();
  s.read_plaintext(&mut got);
  assert_eq!(got, b"secret");
}
