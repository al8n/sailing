use super::*;
use crate::{
  Index, Term,
  transport::{
    ClusterId,
    labeled::{LabelOptions, Labeled},
    passthrough::Passthrough,
  },
};
use std::vec::Vec;

type C = Conn<u64, Labeled<Passthrough>>;

fn opts(id: u64) -> LabelOptions {
  let mut local_id = Vec::new();
  id.encode(&mut local_id);
  LabelOptions {
    cluster: ClusterId([1; 16]),
    local_id,
  }
}

fn dialer(id: u64) -> C {
  Conn::new(Labeled::dialer(Passthrough::new(), &opts(id)))
}

fn acceptor(id: u64) -> C {
  Conn::new(Labeled::acceptor(Passthrough::new(), &opts(id)))
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
  Message::Heartbeat(crate::message::Heartbeat::new(
    Term::new(2),
    7,
    Index::new(4),
    bytes::Bytes::from_static(b"ctx"),
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
