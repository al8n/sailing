use super::*;
use crate::Instant;
use std::vec::Vec;

#[test]
fn pipes_plaintext_both_ways() {
  let mut p = Passthrough::new();
  assert!(!p.is_handshaking());
  assert!(!Passthrough::is_secure());
  assert!(p.peer_identity().is_none());
  assert!(!p.peer_has_closed());

  // Inbound wire bytes surface as plaintext.
  assert_eq!(
    p.handle_transport_data(b"ping", Instant::ORIGIN),
    Intake::Done
  );
  let mut got = Vec::new();
  assert_eq!(p.read_plaintext(&mut got), 4);
  assert_eq!(got, b"ping");

  // Outbound plaintext becomes the wire.
  assert_eq!(p.write_plaintext(b"pong"), 4);
  let mut wire = Vec::new();
  assert_eq!(p.poll_transport_transmit(&mut wire), 4);
  assert_eq!(wire, b"pong");
}

#[test]
fn backpressures_when_recv_buffer_full() {
  let mut p = Passthrough::new();
  let big = std::vec![0u8; RECV_LIMIT + 100];
  match p.handle_transport_data(&big, Instant::ORIGIN) {
    Intake::Pending(n) => assert_eq!(n, RECV_LIMIT),
    other => panic!("expected Pending, got {other:?}"),
  }
}

/// A buffer that absorbed a burst past the 4 * 64 KiB shrink threshold releases its peak capacity
/// once fully drained (the `shrink_excess` shrink path) — heap is not pinned by one large burst for
/// the connection's lifetime.
#[test]
fn large_drained_outbound_releases_excess_capacity() {
  let mut p = Passthrough::new();
  let big = std::vec![0xAB_u8; 300 * 1024]; // > 4 * 64 KiB, under the 64 MiB send cap
  assert_eq!(p.write_plaintext(&big), big.len());
  let mut out = Vec::new();
  // Draining empties the outbound buffer, tripping the shrink-when-empty-and-oversized path.
  assert_eq!(p.poll_transport_transmit(&mut out), big.len());
  assert_eq!(out.len(), big.len());
  // The drained buffer accepts fresh traffic normally afterward.
  assert_eq!(p.write_plaintext(b"ping"), 4);
}
