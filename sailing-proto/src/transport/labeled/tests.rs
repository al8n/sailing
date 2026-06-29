use super::{super::passthrough::Passthrough, *};
use crate::{Data, Instant, transport::TransportError};
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
  let mut a = Labeled::dialer(Passthrough::new(), &opts(1, 7)).unwrap();
  let mut b = Labeled::acceptor(Passthrough::new(), &opts(1, 9)).unwrap();
  assert!(a.is_handshaking() && b.is_handshaking());
  pump(&mut a, &mut b);
  assert_eq!(a.peer_identity(), Some(enc(9).as_slice()));
  assert_eq!(b.peer_identity(), Some(enc(7).as_slice()));
  assert!(!a.is_handshaking());
  assert!(!b.is_handshaking());
}

#[test]
fn rejects_cluster_mismatch() {
  let mut a = Labeled::dialer(Passthrough::new(), &opts(1, 7)).unwrap();
  let mut b = Labeled::acceptor(Passthrough::new(), &opts(2, 9)).unwrap();
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
  let mut a = Labeled::dialer(ThrottledInner::new(1), &opts(1, 7)).unwrap();
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
  let mut b = Labeled::acceptor(Passthrough::new(), &opts(1, 9)).unwrap();
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
  let mut a = Labeled::dialer(Passthrough::new(), &opts(1, 7)).unwrap();
  let mut b = Labeled::acceptor(Passthrough::new(), &opts(1, 9)).unwrap();
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
  let mut a = Labeled::dialer(Passthrough::new(), &opts(1, 7)).unwrap();
  let mut b = Labeled::acceptor(Passthrough::new(), &opts(1, 9)).unwrap();
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
  let mut b = Labeled::acceptor(Passthrough::new(), &opts(1, 9)).unwrap();
  let mut hello = Vec::new();
  Labeled::dialer(Passthrough::new(), &opts(1, 7))
    .unwrap()
    .poll_transport_transmit(&mut hello);
  let mut bad_magic = hello.clone();
  bad_magic[0] ^= 0xFF;
  assert_eq!(
    b.handle_transport_data(&bad_magic, Instant::ORIGIN),
    Intake::Failed
  );

  // Wrong version (a future wire format must be rejected at the handshake).
  let mut b2 = Labeled::acceptor(Passthrough::new(), &opts(1, 9)).unwrap();
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
  let mut b = Labeled::acceptor(Passthrough::new(), &opts(1, 9)).unwrap();
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
  let mut b2 = Labeled::acceptor(Passthrough::new(), &opts(1, 9)).unwrap();
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
  Labeled::dialer(Passthrough::new(), &opts(1, 7))
    .unwrap()
    .poll_transport_transmit(&mut hello);
  for cut in 0..=hello.len() {
    let mut b = Labeled::acceptor(Passthrough::new(), &opts(1, 9)).unwrap();
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

/// The OUTBOUND mirror of the inbound peer-id bound: a local id we would reject on receipt must
/// be rejected at construction, for both roles. (An oversized id would wrap through the hello's
/// u16 length field; an empty one is no identity at all.)
#[test]
fn out_of_bounds_local_id_is_rejected_at_construction() {
  let empty = LabelOptions {
    cluster: ClusterId([7; 16]),
    local_id: Vec::new(),
  };
  let oversized = LabelOptions {
    cluster: ClusterId([7; 16]),
    local_id: std::vec![0xAB; MAX_PEER_ID_LEN + 1],
  };
  for bad in [&empty, &oversized] {
    assert_eq!(
      Labeled::dialer(Passthrough::default(), bad).err(),
      Some(TransportError::InvalidLocalId),
      "dialer must reject an out-of-bounds local id"
    );
    assert_eq!(
      Labeled::acceptor(Passthrough::default(), bad).err(),
      Some(TransportError::InvalidLocalId),
      "acceptor must reject an out-of-bounds local id"
    );
  }
  // The boundary values themselves are fine.
  let max = LabelOptions {
    cluster: ClusterId([7; 16]),
    local_id: std::vec![0xAB; MAX_PEER_ID_LEN],
  };
  assert!(Labeled::dialer(Passthrough::default(), &max).is_ok());
  let one = LabelOptions {
    cluster: ClusterId([7; 16]),
    local_id: std::vec![0x01],
  };
  assert!(Labeled::acceptor(Passthrough::default(), &one).is_ok());
}

#[cfg(feature = "serde")]
#[test]
fn label_options_serde_round_trips() {
  let opts = LabelOptions {
    cluster: ClusterId([0xAB; 16]),
    local_id: std::vec![1, 2, 3, 4, 5],
  };
  let json = serde_json::to_string(&opts).unwrap();
  let back: LabelOptions = serde_json::from_str(&json).unwrap();
  assert_eq!(back, opts);
}

#[cfg(feature = "serde")]
#[test]
fn label_options_serde_requires_both_identity_fields() {
  // Identity is REQUIRED — neither field is defaulted, so a missing one is an error (not a silent
  // empty identity). The cluster id is a 16-byte array; a wrong length is likewise rejected.
  assert!(
    serde_json::from_str::<LabelOptions>(r#"{"cluster": [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]}"#)
      .is_err(),
    "a missing local_id is an error"
  );
  assert!(
    serde_json::from_str::<LabelOptions>(r#"{"local_id": [1, 2, 3]}"#).is_err(),
    "a missing cluster is an error"
  );
  assert!(
    serde_json::from_str::<LabelOptions>(r#"{"cluster": [1, 2, 3], "local_id": []}"#).is_err(),
    "a wrong-length cluster array is an error"
  );
}

/// A record layer that always faults its intake — drives the `Labeled` decorator's inner-failure
/// propagation.
struct FailingInner;

impl super::super::stream::sealed::Sealed for FailingInner {}

impl RecordIo for FailingInner {
  fn handle_transport_data(&mut self, _input: &[u8], _now: Instant) -> Intake {
    Intake::Failed
  }
  fn poll_transport_transmit(&mut self, _out: &mut Vec<u8>) -> usize {
    0
  }
  fn read_plaintext(&mut self, _out: &mut Vec<u8>) -> usize {
    0
  }
  fn write_plaintext(&mut self, plaintext: &[u8]) -> usize {
    plaintext.len()
  }
  fn buffered_outbound(&self) -> usize {
    0
  }
  fn is_handshaking(&self) -> bool {
    true
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

/// Once a `Labeled` layer latches `failed` (a foreign cluster), it is terminally inert: every
/// `RecordIo` method short-circuits — no further intake, no transmit (not even the local hello), no
/// accepted plaintext.
#[test]
fn failed_layer_is_terminally_inert() {
  let mut b = Labeled::acceptor(Passthrough::new(), &opts(1, 9)).unwrap();
  // A hello advertising a DIFFERENT cluster latches `failed`.
  let foreign = build_hello(&ClusterId([2; 16]), &enc(7));
  assert_eq!(
    b.handle_transport_data(&foreign, Instant::ORIGIN),
    Intake::Failed
  );
  // Sticky + inert across every method.
  assert_eq!(
    b.handle_transport_data(b"more", Instant::ORIGIN),
    Intake::Failed,
    "intake stays Failed"
  );
  let mut out = Vec::new();
  assert_eq!(
    b.poll_transport_transmit(&mut out),
    0,
    "a rejected stream emits nothing, not even its hello"
  );
  assert_eq!(
    b.write_plaintext(b"app"),
    0,
    "writes are refused after failure"
  );
}

/// A fault from the INNER record layer latches the `Labeled` decorator failed and propagates as
/// `Intake::Failed`, then stays inert.
#[test]
fn inner_layer_failure_propagates_and_latches() {
  let mut l = Labeled::dialer(FailingInner, &opts(1, 7)).unwrap();
  assert_eq!(
    l.handle_transport_data(b"x", Instant::ORIGIN),
    Intake::Failed,
    "an inner-layer fault surfaces as Failed"
  );
  assert_eq!(
    l.handle_transport_data(b"y", Instant::ORIGIN),
    Intake::Failed,
    "the failure is latched"
  );
}

/// The peer-id length bound (`1..=MAX_PEER_ID_LEN`) is enforced by `advance_handshake` on a hello
/// carrying the CURRENT magic + version — built via `build_hello`, then its `u16` length field
/// tampered. (The companion `zero_length_and_oversized_peer_ids_are_rejected` hand-rolls the header
/// and so rejects earlier, at the version check.)
#[test]
fn peer_id_length_bounds_are_enforced_after_the_version_check() {
  // peer_id_len == 0: a valid current-version hello with its length field zeroed.
  let mut zero = build_hello(&ClusterId([1; 16]), &enc(7));
  zero[18] = 0;
  zero[19] = 0;
  let mut b = Labeled::acceptor(Passthrough::new(), &opts(1, 9)).unwrap();
  assert_eq!(
    b.handle_transport_data(&zero, Instant::ORIGIN),
    Intake::Failed,
    "an empty peer id is rejected"
  );

  // peer_id_len > MAX_PEER_ID_LEN: the length field claims more than the cap.
  let mut huge = build_hello(&ClusterId([1; 16]), &enc(7));
  let over = (MAX_PEER_ID_LEN as u16) + 1;
  huge[18..20].copy_from_slice(&over.to_be_bytes());
  let mut b2 = Labeled::acceptor(Passthrough::new(), &opts(1, 9)).unwrap();
  assert_eq!(
    b2.handle_transport_data(&huge, Instant::ORIGIN),
    Intake::Failed,
    "an oversized peer id is rejected before any id byte is buffered"
  );
}

/// `is_secure` reflects the inner layer: `Labeled<Passthrough>` is plaintext.
#[test]
fn is_secure_reflects_the_inner_layer() {
  assert!(!<Labeled<Passthrough> as RecordIo>::is_secure());
}
