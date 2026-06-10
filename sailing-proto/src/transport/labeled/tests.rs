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
