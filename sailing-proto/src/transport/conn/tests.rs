use super::*;
use crate::{
  Data as _, Index, Term,
  transport::{
    ClusterId,
    labeled::{LabelOptions, Labeled},
    passthrough::Passthrough,
    stream::{Intake, RecordIo, sealed},
  },
};
use std::vec::Vec;

type C = Conn<u64, Labeled<Passthrough>>;

/// A test record layer that accepts at most `cap` plaintext bytes per `write_plaintext` call (to
/// force the backpressure / partial-accept path), pipes plaintext through, and reports a fixed
/// peer identity (so a `Conn` validates immediately and we can feed it a crafted id).
struct Throttle {
  cap: usize,
  inbound: Vec<u8>,
  outbound: Vec<u8>,
  ident: Option<Vec<u8>>,
  peer_closed: bool,
}

impl Throttle {
  fn new(cap: usize, ident: Option<Vec<u8>>) -> Self {
    Self {
      cap,
      inbound: Vec::new(),
      outbound: Vec::new(),
      ident,
      peer_closed: false,
    }
  }

  /// Simulate an in-band peer close (a TLS close_notify) surfacing from the record layer.
  fn with_peer_closed(mut self) -> Self {
    self.peer_closed = true;
    self
  }
}

impl sealed::Sealed for Throttle {}

impl RecordIo for Throttle {
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
    let take = plaintext.len().min(self.cap);
    self.outbound.extend_from_slice(&plaintext[..take]);
    take
  }
  fn buffered_outbound(&self) -> usize {
    self.outbound.len()
  }
  fn is_handshaking(&self) -> bool {
    self.ident.is_none()
  }
  fn peer_identity(&self) -> Option<&[u8]> {
    self.ident.as_deref()
  }
  fn peer_has_closed(&self) -> bool {
    self.peer_closed
  }
  fn is_secure() -> bool {
    false
  }
}

fn enc_id(id: u64) -> Vec<u8> {
  let mut v = Vec::new();
  id.encode(&mut v);
  v
}

fn opts(id: u64) -> LabelOptions {
  let mut local_id = Vec::new();
  id.encode(&mut local_id);
  LabelOptions {
    cluster: ClusterId([1; 16]),
    local_id,
  }
}

fn dialer(id: u64) -> C {
  Conn::new(Labeled::dialer(Passthrough::new(), &opts(id)).unwrap())
}

fn acceptor(id: u64) -> C {
  Conn::new(Labeled::acceptor(Passthrough::new(), &opts(id)).unwrap())
}

/// Shuttle wire bytes between two conns until quiescent.
fn pump(a: &mut C, b: &mut C) {
  for _ in 0..8 {
    let mut wa = Vec::new();
    a.poll_transmit(&mut wa);
    if !wa.is_empty() {
      b.handle_data(&wa, false, Instant::ORIGIN).unwrap();
    }
    let mut wb = Vec::new();
    b.poll_transmit(&mut wb);
    if !wb.is_empty() {
      a.handle_data(&wb, false, Instant::ORIGIN).unwrap();
    }
    if wa.is_empty() && wb.is_empty() {
      break;
    }
  }
}

fn sample_msg() -> Message<u64> {
  // A 64-byte context keeps the frame comfortably multi-chunk for the tiny-receive tests
  // regardless of how compactly the envelope encodes the scalar fields.
  Message::Heartbeat(crate::message::Heartbeat::new(
    Term::new(2),
    7,
    Index::new(4),
    bytes::Bytes::from_static(&[0xC7; 64]),
  ))
}

#[test]
fn gates_app_frames_until_validated_then_decodes() {
  let mut d = dialer(7);
  let mut a = acceptor(9);
  // A message queued before the handshake settles must not decode on the peer prematurely.
  d.send_message(&sample_msg());
  assert!(d.is_handshaking() && a.is_handshaking());

  pump(&mut d, &mut a);
  assert_eq!(a.peer(), Some(7));
  assert_eq!(d.peer(), Some(9));
  assert!(!a.is_handshaking());

  let mut msgs = Vec::new();
  a.poll_decoded(&mut msgs).unwrap();
  assert_eq!(msgs, std::vec![sample_msg()]);
}

#[test]
fn round_trips_a_message_after_handshake() {
  let mut d = dialer(7);
  let mut a = acceptor(9);
  pump(&mut d, &mut a); // settle the handshake first
  d.send_message(&sample_msg());
  pump(&mut d, &mut a);
  let mut msgs = Vec::new();
  a.poll_decoded(&mut msgs).unwrap();
  assert_eq!(msgs, std::vec![sample_msg()]);
}

#[test]
fn undecodable_frame_closes_the_conn() {
  let mut d = dialer(7);
  let mut a = acceptor(9);
  pump(&mut d, &mut a);
  // Send a framed payload that is NOT a valid Message (bogus tag byte).
  let mut framed = Vec::new();
  crate::transport::frame::encode_frame(&[0xFF], &mut framed);
  d.record_write_for_test(&framed);
  pump(&mut d, &mut a);
  let mut msgs = Vec::new();
  let res = a.poll_decoded(&mut msgs);
  assert!(res.is_err());
  assert!(a.is_closed());
}

#[test]
fn eof_closes_the_conn() {
  let mut a = acceptor(9);
  a.handle_data(&[], true, Instant::ORIGIN).unwrap();
  assert!(a.is_closed());
}

#[test]
fn oversized_frame_closes_the_conn_and_clears_the_encoder_cache() {
  let mut d = dialer(7);
  let mut a = acceptor(9);
  pump(&mut d, &mut a);
  assert_eq!(a.peer(), Some(7));

  // Prime the receiver's OUTBOUND encoder with a cacheable (small, NON-final) snapshot transfer so the
  // meta is held in the cache.
  let meta = crate::SnapshotMeta::new(
    Index::new(5),
    Term::new(2),
    crate::conf::ConfState::from_voters([1u64, 2, 3]),
  );
  a.send_message(&Message::InstallSnapshot(
    crate::InstallSnapshot::new_chunk(
      Term::new(2),
      9,
      meta,
      bytes::Bytes::from_static(&[0xAB; 32]),
      0,
      1_000_000, // non-final (offset + len < total) → the cache is retained, not completion-cleared
    ),
  ));
  assert!(
    a.encoder.cached_body_len().is_some(),
    "the send must populate the encoder cache"
  );

  // Feed a raw oversized frame HEADER (declared length far above MAX_FRAME_LEN); the decoder latches
  // `FrameTooLarge` at the header, before any payload.
  d.record_write_for_test(&[0xFF, 0xFF, 0xFF, 0xFF]);
  pump(&mut d, &mut a);

  // The latched decoder fault must surface AS a close: an error is returned, the conn is Closed, and the
  // close path cleared the encoder cache — so a direct owner that keeps the conn retains no metadata.
  let mut msgs = Vec::new();
  let res = a.poll_decoded(&mut msgs);
  assert!(
    res.is_err(),
    "a latched oversized frame must surface an error"
  );
  assert!(a.is_closed(), "a transport fault must close the conn");
  assert_eq!(
    a.encoder.cached_body_len(),
    None,
    "the close must clear the encoder cache (no retained snapshot metadata)"
  );
}

#[test]
fn backpressured_write_never_truncates_a_frame() {
  // A record layer that accepts only 3 plaintext bytes per write — a hostile backpressure pattern.
  // The full framed message must still reach the wire intact across repeated drains, never a prefix
  // that a later frame could complete into a corrupted-but-valid message.
  let mut sender: Conn<u64, Throttle> = Conn::new(Throttle::new(3, Some(enc_id(7))));
  sender.send_message(&sample_msg());
  sender.send_message(&sample_msg());

  // Drain the wire in many small pulls, exactly as the throttle allows.
  let mut wire = Vec::new();
  for _ in 0..500 {
    let mut chunk = Vec::new();
    if sender.poll_transmit(&mut chunk) == 0 {
      break;
    }
    wire.extend_from_slice(&chunk);
  }

  // Feed the collected wire into a fresh receiver and confirm BOTH messages decode intact.
  let mut receiver: Conn<u64, Throttle> = Conn::new(Throttle::new(usize::MAX, Some(enc_id(7))));
  receiver.handle_data(&wire, false, Instant::ORIGIN).unwrap();
  let mut msgs = Vec::new();
  receiver.poll_decoded(&mut msgs).unwrap();
  assert_eq!(msgs, std::vec![sample_msg(), sample_msg()]);
}

#[test]
fn outbound_cap_exceeded_closes_the_conn() {
  // A record layer that accepts NOTHING (cap 0): out_plain retains every framed byte. Once the cap
  // trips, the connection closes instead of growing without bound.
  let mut conn: Conn<u64, Throttle> = Conn::new(Throttle::new(0, Some(enc_id(7))));
  conn.set_max_out_for_test(16);
  conn.send_message(&sample_msg()); // a heartbeat frame is well over 16 bytes
  assert!(
    conn.is_closed(),
    "exceeding the outbound cap closes the connection"
  );
}

#[test]
fn in_band_peer_close_closes_the_conn() {
  // The record layer reports peer_has_closed (a TLS close_notify) without any socket EOF.
  let mut conn: Conn<u64, Throttle> =
    Conn::new(Throttle::new(usize::MAX, Some(enc_id(7))).with_peer_closed());
  conn.handle_data(b"", false, Instant::ORIGIN).unwrap();
  assert!(
    conn.is_closed(),
    "an in-band close ends the connection like an EOF"
  );
}

#[test]
fn peer_id_with_trailing_bytes_is_rejected() {
  // A valid u64 NodeId encoding followed by a trailing byte is a malformed identity, not a peer.
  let mut ident = enc_id(7);
  ident.push(0xAB);
  let mut conn: Conn<u64, Throttle> = Conn::new(Throttle::new(usize::MAX, Some(ident)));
  let err = conn.handle_data(b"anything", false, Instant::ORIGIN);
  assert!(
    err.is_err(),
    "trailing bytes after the id must fail validation"
  );
  assert!(conn.is_closed());
  assert_eq!(conn.peer(), None);
}

#[test]
fn frames_in_the_final_read_before_eof_still_deliver() {
  let mut d = dialer(7);
  let mut a = acceptor(9);
  pump(&mut d, &mut a); // validated
  d.send_message(&sample_msg());
  let mut wire = Vec::new();
  d.poll_transmit(&mut wire);
  // The frame and the EOF arrive in the SAME read: a clean close retains the peer so the final
  // frames still decode and deliver before the route drops.
  a.handle_data(&wire, true, Instant::ORIGIN).unwrap();
  assert!(a.is_closed());
  let mut msgs = Vec::new();
  a.poll_decoded(&mut msgs).unwrap();
  assert_eq!(
    msgs,
    std::vec![sample_msg()],
    "final-read frames deliver on a clean close"
  );
}

/// A record layer with a tiny receive buffer (forces `Intake::Pending` on intake): `handle_data`'s
/// re-feed loop must drain plaintext between rounds and deliver the full frame stream; and a
/// WEDGED layer (no consumption, no plaintext) must close the connection rather than silently
/// dropping the tail.
struct TinyRecv {
  cap: usize,
  inbound: Vec<u8>,
  outbound: Vec<u8>,
  ident: Option<Vec<u8>>,
  wedged: bool,
}

impl TinyRecv {
  fn new(cap: usize, ident: Option<Vec<u8>>) -> Self {
    Self {
      cap,
      inbound: Vec::new(),
      outbound: Vec::new(),
      ident,
      wedged: false,
    }
  }
}

impl sealed::Sealed for TinyRecv {}

impl RecordIo for TinyRecv {
  fn handle_transport_data(&mut self, input: &[u8], _now: Instant) -> Intake {
    if self.wedged {
      return Intake::Pending(0); // consumes nothing, surfaces nothing — a wedge
    }
    let room = self.cap.saturating_sub(self.inbound.len());
    if input.len() <= room {
      self.inbound.extend_from_slice(input);
      Intake::Done
    } else {
      self.inbound.extend_from_slice(&input[..room]);
      Intake::Pending(room)
    }
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
    self.outbound.extend_from_slice(plaintext);
    plaintext.len()
  }
  fn buffered_outbound(&self) -> usize {
    self.outbound.len()
  }
  fn is_handshaking(&self) -> bool {
    self.ident.is_none()
  }
  fn peer_identity(&self) -> Option<&[u8]> {
    self.ident.as_deref()
  }
  fn peer_has_closed(&self) -> bool {
    false
  }
  fn is_secure() -> bool {
    false
  }
}

impl<I: NodeId> Conn<I, TinyRecv> {
  fn record_wedge_for_test(&mut self) {
    self.record.wedged = true;
  }
}

#[test]
fn pending_refeed_reassembles_across_a_tiny_receive_buffer() {
  // 8-byte receive cap: a multi-hundred-byte read is consumed in many Pending rounds, with
  // plaintext drained between each. Both messages must reassemble exactly.
  let mut sender: Conn<u64, TinyRecv> = Conn::new(TinyRecv::new(usize::MAX, Some(enc_id(7))));
  sender.send_message(&sample_msg());
  sender.send_message(&sample_msg());
  let mut wire = Vec::new();
  sender.poll_transmit(&mut wire);
  assert!(
    wire.len() > 64,
    "two framed heartbeats are well over the cap"
  );

  let mut receiver: Conn<u64, TinyRecv> = Conn::new(TinyRecv::new(8, Some(enc_id(7))));
  receiver.handle_data(&wire, false, Instant::ORIGIN).unwrap();
  let mut msgs = Vec::new();
  receiver.poll_decoded(&mut msgs).unwrap();
  assert_eq!(msgs, std::vec![sample_msg(), sample_msg()]);
}

#[test]
fn wedged_record_layer_closes_instead_of_dropping_the_tail() {
  let mut conn: Conn<u64, TinyRecv> = Conn::new(TinyRecv::new(8, Some(enc_id(7))));
  conn.record_wedge_for_test();
  let res = conn.handle_data(
    b"some bytes that cannot make progress",
    false,
    Instant::ORIGIN,
  );
  assert!(res.is_err(), "a wedged record layer is a transport fault");
  assert!(conn.is_closed());
}
