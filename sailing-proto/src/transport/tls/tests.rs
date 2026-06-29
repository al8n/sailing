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

/// A consensus-sized payload (well past rustls's ~16 KiB internal received-plaintext cap) must
/// flow through `TlsRecords` via `Intake::Pending` backpressure — NOT kill the connection.
/// rustls signals the full receive buffer as `ErrorKind::Other` from `read_tls`; treating that as
/// fatal closed the connection on every message over ~16 KiB (a permanent redial/kill flap for
/// ordinary AppendEntries batches). The feed loop below mirrors `Conn::handle_data`: on Pending,
/// drain plaintext and re-feed the remainder.
#[test]
fn large_plaintext_backpressures_and_reassembles() {
  let (client_cfg, server_cfg) = test_configs();
  let mut c = TlsRecords::client(client_cfg, "localhost".try_into().unwrap()).unwrap();
  let mut s = TlsRecords::server(server_cfg).unwrap();
  pump(&mut c, &mut s);
  assert!(!c.is_handshaking() && !s.is_handshaking());

  // 64 KiB of patterned plaintext — 4x the rustls receive cap.
  let big: Vec<u8> = (0..64 * 1024).map(|i| (i % 251) as u8).collect();
  let mut received = Vec::new();
  let mut offered = 0;
  while offered < big.len() {
    offered += c.write_plaintext(&big[offered..]);
    // Drain ciphertext to the server in one chunk, honoring Pending by draining plaintext
    // and re-feeding the unconsumed remainder.
    let mut wire = Vec::new();
    c.poll_transport_transmit(&mut wire);
    let mut input = &wire[..];
    let mut got_chunk = Vec::new();
    loop {
      match s.handle_transport_data(input, Instant::ORIGIN) {
        Intake::Done => break,
        Intake::Pending(consumed) => {
          input = &input[consumed..];
          let drained = s.read_plaintext(&mut got_chunk);
          assert!(
            drained > 0 || consumed > 0,
            "backpressure must always be resolvable by draining plaintext"
          );
        }
        Intake::Failed => panic!("a large message must backpressure, never fail the record layer"),
      }
    }
    s.read_plaintext(&mut got_chunk);
    received.extend_from_slice(&got_chunk);
  }
  assert_eq!(received, big, "the full 64 KiB reassembles intact");
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

/// A fatal record fault (garbage ciphertext) latches the session: Failed is sticky and every
/// other method becomes inert (no plaintext out, no writes accepted, nothing transmitted).
#[test]
fn fatal_record_fault_is_terminal_and_inert() {
  let (client_cfg, server_cfg) = test_configs();
  let mut c = TlsRecords::client(client_cfg, "localhost".try_into().unwrap()).unwrap();
  let mut s = TlsRecords::server(server_cfg).unwrap();
  pump(&mut c, &mut s);
  assert!(!s.is_handshaking());

  // Garbage that cannot be a TLS record stream.
  assert_eq!(
    s.handle_transport_data(&[0u8; 64], Instant::ORIGIN),
    Intake::Failed
  );
  // Sticky + inert.
  assert_eq!(
    s.handle_transport_data(b"more", Instant::ORIGIN),
    Intake::Failed
  );
  assert_eq!(s.write_plaintext(b"app"), 0, "writes refused after abort");
  let mut out = Vec::new();
  assert_eq!(
    s.poll_transport_transmit(&mut out),
    0,
    "nothing transmitted after abort"
  );
  assert_eq!(
    s.read_plaintext(&mut out),
    0,
    "no plaintext surfaced after abort"
  );
}

/// A peer's close_notify surfaces via peer_has_closed (the in-band clean close signal).
#[test]
fn close_notify_surfaces_as_peer_has_closed() {
  let (client_cfg, server_cfg) = test_configs();
  let mut c = TlsRecords::client(client_cfg, "localhost".try_into().unwrap()).unwrap();
  let mut s = TlsRecords::server(server_cfg).unwrap();
  pump(&mut c, &mut s);

  c.send_close_notify_for_test();
  let mut wire = Vec::new();
  c.poll_transport_transmit(&mut wire);
  assert_ne!(
    s.handle_transport_data(&wire, Instant::ORIGIN),
    Intake::Failed
  );
  assert!(
    s.peer_has_closed(),
    "close_notify latches the in-band close"
  );
}

/// `buffered_outbound` reflects accepted-but-undrained plaintext, and `TlsRecords` carries no
/// identity of its own (the `Labeled` decorator supplies it).
#[test]
fn buffered_outbound_tracks_plaintext_and_peer_identity_is_none() {
  let (client_cfg, server_cfg) = test_configs();
  let mut c = TlsRecords::client(client_cfg, "localhost".try_into().unwrap()).unwrap();
  let mut s = TlsRecords::server(server_cfg).unwrap();
  pump(&mut c, &mut s);

  let n = c.write_plaintext(b"some application bytes");
  assert!(n > 0, "post-handshake plaintext is accepted");
  assert_eq!(
    c.buffered_outbound(),
    n,
    "buffered_outbound counts the accepted, not-yet-drained plaintext"
  );
  assert_eq!(c.peer_identity(), None, "the TLS layer is anonymous");
}

/// After a peer's clean close_notify: trailing ciphertext is consumed-as-nothing (the `read_tls`
/// `Ok(0)` post-close latch, never a fault), and `read_plaintext` reaches a clean EOF (the reader's
/// `Ok(0)` arm) surfacing no further plaintext.
#[test]
fn post_close_latch_consumes_trailing_bytes_and_read_reaches_eof() {
  let (client_cfg, server_cfg) = test_configs();
  let mut c = TlsRecords::client(client_cfg, "localhost".try_into().unwrap()).unwrap();
  let mut s = TlsRecords::server(server_cfg).unwrap();
  pump(&mut c, &mut s);

  c.send_close_notify_for_test();
  let mut wire = Vec::new();
  c.poll_transport_transmit(&mut wire);
  assert_ne!(
    s.handle_transport_data(&wire, Instant::ORIGIN),
    Intake::Failed
  );
  assert!(s.peer_has_closed());

  // Trailing bytes after close_notify are consumed-as-nothing, never a fault.
  assert_ne!(
    s.handle_transport_data(b"trailing", Instant::ORIGIN),
    Intake::Failed,
    "post-close trailing bytes are discarded, not a fault"
  );

  // The reader is at clean EOF: read_plaintext surfaces nothing.
  let mut out = Vec::new();
  assert_eq!(
    s.read_plaintext(&mut out),
    0,
    "no plaintext after a clean close"
  );
}

/// A TLS record header declaring a fragment past rustls's record-size cap fails at `read_tls` (the
/// record-framing layer, distinct from the decrypt-stage fault), latching the session aborted —
/// never a panic — and the abort is sticky.
#[test]
fn oversized_tls_record_aborts_via_read_tls() {
  let (_c, server_cfg) = test_configs();
  let mut s = TlsRecords::server(server_cfg).unwrap();
  // ApplicationData, TLS 1.2, declared length 0xFFFF (65535) — over the ~16 KiB record cap.
  let oversized = [0x17u8, 0x03, 0x03, 0xFF, 0xFF];
  assert_eq!(
    s.handle_transport_data(&oversized, Instant::ORIGIN),
    Intake::Failed
  );
  assert_eq!(
    s.handle_transport_data(b"x", Instant::ORIGIN),
    Intake::Failed,
    "the abort is sticky"
  );
}
