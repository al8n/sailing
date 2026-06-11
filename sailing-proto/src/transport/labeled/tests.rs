use super::{super::passthrough::Passthrough, *};
use crate::{Data, Instant};
use std::vec::Vec;

fn enc(id: u64) -> Vec<u8> {
  let mut v = Vec::new();
  id.encode(&mut v);
  v
}

fn opts(cluster: u8, id: u64) -> LabelOptions {
  LabelOptions {
    cluster: ClusterId([cluster; 16]),
    local_id: enc(id),
  }
}

/// Shuttle wire bytes between two record layers until quiescent or one fails.
fn pump(a: &mut Labeled<Passthrough>, b: &mut Labeled<Passthrough>) {
  for _ in 0..8 {
    let mut wa = Vec::new();
    a.poll_transport_transmit(&mut wa);
    if !wa.is_empty() {
      b.handle_transport_data(&wa, Instant::ORIGIN);
    }
    let mut wb = Vec::new();
    b.poll_transport_transmit(&mut wb);
    if !wb.is_empty() {
      a.handle_transport_data(&wb, Instant::ORIGIN);
    }
    if wa.is_empty() && wb.is_empty() {
      break;
    }
  }
}

#[test]
fn validates_matching_cluster_and_binds_peer() {
  let mut a = Labeled::dialer(Passthrough::new(), &opts(1, 7));
  let mut b = Labeled::acceptor(Passthrough::new(), &opts(1, 9));
  assert!(a.is_handshaking() && b.is_handshaking());
  pump(&mut a, &mut b);
  assert_eq!(a.peer_identity(), Some(enc(9).as_slice()));
  assert_eq!(b.peer_identity(), Some(enc(7).as_slice()));
  assert!(!a.is_handshaking());
  assert!(!b.is_handshaking());
}

#[test]
fn rejects_cluster_mismatch() {
  let mut a = Labeled::dialer(Passthrough::new(), &opts(1, 7));
  let mut b = Labeled::acceptor(Passthrough::new(), &opts(2, 9));
  let mut wire = Vec::new();
  a.poll_transport_transmit(&mut wire);
  assert_eq!(
    b.handle_transport_data(&wire, Instant::ORIGIN),
    Intake::Failed
  );
  assert!(b.peer_identity().is_none());
}

/// An inner record layer with a tiny bounded outbound buffer (`cap` total) — writes accept only up
/// to the remaining room, forcing the partial-accept path on the hello itself until the wire side
/// drains the buffer.
struct ThrottledInner {
  cap: usize,
  inbound: Vec<u8>,
  outbound: Vec<u8>,
}

impl ThrottledInner {
  fn new(cap: usize) -> Self {
    Self {
      cap,
      inbound: Vec::new(),
      outbound: Vec::new(),
    }
  }
}

impl super::super::stream::sealed::Sealed for ThrottledInner {}

impl RecordIo for ThrottledInner {
  fn handle_transport_data(&mut self, input: &[u8], _now: Instant) -> Intake {
    self.inbound.extend_from_slice(input);
    Intake::Done
  }
  fn poll_transport_transmit(&mut self, out: &mut Vec<u8>) -> usize {
    let n = self.outbound.len();
    out.extend_from_slice(&self.outbound);
    self.outbound.clear();
    n
  }
  fn read_plaintext(&mut self, out: &mut Vec<u8>) -> usize {
    let n = self.inbound.len();
    out.extend_from_slice(&self.inbound);
    self.inbound.clear();
    n
  }
  fn write_plaintext(&mut self, plaintext: &[u8]) -> usize {
    let room = self.cap.saturating_sub(self.outbound.len());
    let take = plaintext.len().min(room);
    self.outbound.extend_from_slice(&plaintext[..take]);
    take
  }
  fn buffered_outbound(&self) -> usize {
    self.outbound.len()
  }
  fn is_handshaking(&self) -> bool {
    false
  }
  fn peer_identity(&self) -> Option<&[u8]> {
    None
  }
  fn peer_has_closed(&self) -> bool {
    false
  }
  fn is_secure() -> bool {
    false
  }
}

#[test]
fn hello_survives_partial_accepts_and_gates_app_writes() {
  // The inner layer holds at most ONE byte: the dialer's hello drains a byte at a time, and must
  // still reach the wire complete and uncorrupted; app writes are refused (0) until it has.
  let mut a = Labeled::dialer(ThrottledInner::new(1), &opts(1, 7));
  assert_eq!(
    a.write_plaintext(b"app"),
    0,
    "app plaintext is refused while the hello tail is pending"
  );

  // Drain the wire one byte per poll (each poll re-offers the hello tail into the freed room).
  let mut wire = Vec::new();
  for _ in 0..256 {
    let mut chunk = Vec::new();
    a.poll_transport_transmit(&mut chunk);
    wire.extend_from_slice(&chunk);
    if wire.len() >= 28 {
      break; // hello header (20) + 8-byte u64 id
    }
  }

  // A fresh acceptor must validate the dialer from that wire — i.e. the hello arrived intact.
  let mut b = Labeled::acceptor(Passthrough::new(), &opts(1, 9));
  assert_ne!(
    b.handle_transport_data(&wire, Instant::ORIGIN),
    Intake::Failed
  );
  assert_eq!(b.peer_identity(), Some(enc(7).as_slice()));

  // And once the hello is fully out, app writes flow again.
  assert!(a.write_plaintext(b"app") > 0);
}

#[test]
fn gates_plaintext_until_validated_then_strips_the_hello() {
  let mut a = Labeled::dialer(Passthrough::new(), &opts(1, 7));
  let mut b = Labeled::acceptor(Passthrough::new(), &opts(1, 9));
  a.write_plaintext(b"appdata");
  let mut got = Vec::new();
  assert_eq!(
    b.read_plaintext(&mut got),
    0,
    "no plaintext before validation"
  );
  pump(&mut a, &mut b);
  let mut got2 = Vec::new();
  b.read_plaintext(&mut got2);
  assert_eq!(
    got2, b"appdata",
    "the hello is stripped; only app bytes surface"
  );
}

#[test]
fn hello_delivered_byte_at_a_time_still_validates() {
  let mut a = Labeled::dialer(Passthrough::new(), &opts(1, 7));
  let mut b = Labeled::acceptor(Passthrough::new(), &opts(1, 9));
  let mut wire = Vec::new();
  a.poll_transport_transmit(&mut wire);
  for byte in &wire {
    assert!(b.peer_identity().is_none() || *byte == wire[wire.len() - 1]);
    assert_ne!(
      b.handle_transport_data(&[*byte], Instant::ORIGIN),
      Intake::Failed,
      "a split hello must never be rejected mid-delivery"
    );
  }
  assert_eq!(b.peer_identity(), Some(enc(7).as_slice()));
}

#[test]
fn magic_and_version_mismatches_are_rejected() {
  // Wrong magic.
  let mut b = Labeled::acceptor(Passthrough::new(), &opts(1, 9));
  let mut hello = Vec::new();
  Labeled::dialer(Passthrough::new(), &opts(1, 7)).poll_transport_transmit(&mut hello);
  let mut bad_magic = hello.clone();
  bad_magic[0] ^= 0xFF;
  assert_eq!(
    b.handle_transport_data(&bad_magic, Instant::ORIGIN),
    Intake::Failed
  );

  // Wrong version (a future wire format must be rejected at the handshake).
  let mut b2 = Labeled::acceptor(Passthrough::new(), &opts(1, 9));
  let mut bad_ver = hello.clone();
  bad_ver[1] ^= 0xFF;
  assert_eq!(
    b2.handle_transport_data(&bad_ver, Instant::ORIGIN),
    Intake::Failed
  );
}

#[test]
fn zero_length_and_oversized_peer_ids_are_rejected() {
  // Hand-craft a hello with peer_id_len == 0.
  let mut zero = std::vec![0xCA_u8, 1];
  zero.extend_from_slice(&[1u8; 16]); // cluster
  zero.extend_from_slice(&0u16.to_be_bytes());
  let mut b = Labeled::acceptor(Passthrough::new(), &opts(1, 9));
  assert_eq!(
    b.handle_transport_data(&zero, Instant::ORIGIN),
    Intake::Failed,
    "an empty peer id is no identity"
  );

  // And one claiming a 64 KiB id (over MAX_PEER_ID_LEN) — rejected at the header, before any
  // id byte is buffered.
  let mut huge = std::vec![0xCA_u8, 1];
  huge.extend_from_slice(&[1u8; 16]);
  huge.extend_from_slice(&u16::MAX.to_be_bytes());
  let mut b2 = Labeled::acceptor(Passthrough::new(), &opts(1, 9));
  assert_eq!(
    b2.handle_transport_data(&huge, Instant::ORIGIN),
    Intake::Failed,
    "an oversized peer id is unauthenticated buffer growth"
  );
}

/// EXHAUSTIVE split matrix: the inbound hello split into two chunks at every byte boundary must
/// validate identically (the two wait-states of the hello parser are re-entrant at any cut).
#[test]
fn every_two_chunk_hello_split_validates() {
  let mut hello = Vec::new();
  Labeled::dialer(Passthrough::new(), &opts(1, 7)).poll_transport_transmit(&mut hello);
  for cut in 0..=hello.len() {
    let mut b = Labeled::acceptor(Passthrough::new(), &opts(1, 9));
    assert_ne!(
      b.handle_transport_data(&hello[..cut], Instant::ORIGIN),
      Intake::Failed,
      "cut {cut}: prefix must never reject"
    );
    assert_ne!(
      b.handle_transport_data(&hello[cut..], Instant::ORIGIN),
      Intake::Failed,
      "cut {cut}: remainder must validate"
    );
    assert_eq!(
      b.peer_identity(),
      Some(enc(7).as_slice()),
      "cut {cut}: peer bound"
    );
  }
}
