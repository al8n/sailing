use super::*;
use crate::{
  Data, Index, Term,
  transport::{
    ClusterId,
    labeled::{LabelOptions, Labeled},
    passthrough::Passthrough,
  },
};
use std::vec::Vec;

type R = PeerRouter<u64, Labeled<Passthrough>>;

fn opts(id: u64) -> LabelOptions {
  let mut local_id = Vec::new();
  id.encode(&mut local_id);
  LabelOptions {
    cluster: ClusterId([1; 16]),
    local_id,
  }
}

fn dialer(id: u64) -> Labeled<Passthrough> {
  Labeled::dialer(Passthrough::new(), &opts(id)).unwrap()
}

fn acceptor(id: u64) -> Labeled<Passthrough> {
  Labeled::acceptor(Passthrough::new(), &opts(id)).unwrap()
}

fn hb(from: u64) -> Message<u64> {
  Message::Heartbeat(crate::message::Heartbeat::new(
    Term::new(1),
    from,
    Index::new(0),
    bytes::Bytes::new(),
  ))
}

/// Shuttle bytes between a router's connection `id` and a standalone peer Conn until quiescent.
fn pump(router: &mut R, id: ConnId, peer: &mut Conn<u64, Labeled<Passthrough>>) {
  for _ in 0..8 {
    let moved = router.poll_transmit();
    let mut any = false;
    for (cid, bytes) in moved {
      if cid == id && !bytes.is_empty() {
        peer.handle_data(&bytes, false, Instant::ORIGIN).unwrap();
        any = true;
      }
    }
    let mut back = Vec::new();
    peer.poll_transmit(&mut back);
    if !back.is_empty() {
      let mut out = Vec::new();
      router
        .handle_conn_data(id, &back, false, Instant::ORIGIN, &mut out)
        .unwrap();
      any = true;
    }
    if !any {
      break;
    }
  }
}

#[test]
fn binds_peer_on_validation_and_routes() {
  let mut router = R::new();
  let id = ConnId(1);
  router.register(id, acceptor(10), Instant::ORIGIN); // local node 10 accepts
  let mut peer = Conn::new(dialer(7)); // remote node 7 dials
  pump(&mut router, id, &mut peer);
  assert_eq!(router.conn_of(&7), Some(id), "peer 7 is bound to its conn");

  // Routing a message to peer 7 reaches that connection.
  assert!(router.route(7, &hb(10)));
  pump(&mut router, id, &mut peer);
  let mut got = Vec::new();
  peer.poll_decoded(&mut got).unwrap();
  assert_eq!(got, std::vec![hb(10)]);
}

#[test]
fn decodes_inbound_messages_with_their_peer() {
  let mut router = R::new();
  let id = ConnId(1);
  router.register(id, acceptor(10), Instant::ORIGIN);
  let mut peer = Conn::new(dialer(7));
  pump(&mut router, id, &mut peer);

  peer.send_message(&hb(7));
  let mut delivered = Vec::new();
  for _ in 0..8 {
    let mut back = Vec::new();
    peer.poll_transmit(&mut back);
    if back.is_empty() {
      break;
    }
    router
      .handle_conn_data(id, &back, false, Instant::ORIGIN, &mut delivered)
      .unwrap();
  }
  assert_eq!(delivered, std::vec![(7, hb(7))]);
}

#[test]
fn route_to_unknown_peer_is_dropped() {
  let mut router = R::new();
  assert!(!router.route(99, &hb(1)), "no conn to peer 99");
}

#[test]
fn eof_clears_the_peer_route() {
  let mut router = R::new();
  let id = ConnId(1);
  router.register(id, acceptor(10), Instant::ORIGIN);
  let mut peer = Conn::new(dialer(7));
  pump(&mut router, id, &mut peer);
  assert_eq!(router.conn_of(&7), Some(id));

  // The peer half-closes: an inbound read with eof must drop the binding, not leave a dead route.
  let mut out = Vec::new();
  router
    .handle_conn_data(id, &[], true, Instant::ORIGIN, &mut out)
    .unwrap();
  assert_eq!(router.conn_of(&7), None, "EOF clears the peer binding");
  assert!(
    !router.route(7, &hb(10)),
    "no route into a closed connection"
  );
}

#[test]
fn newer_connection_wins_duplicate_peer() {
  let mut router = R::new();
  // Two connections both validate as peer 7; the second registered wins.
  let (id1, id2) = (ConnId(1), ConnId(2));
  router.register(id1, acceptor(10), Instant::ORIGIN);
  let mut peer1 = Conn::new(dialer(7));
  pump(&mut router, id1, &mut peer1);
  assert_eq!(router.conn_of(&7), Some(id1));

  router.register(id2, acceptor(10), Instant::ORIGIN);
  let mut peer2 = Conn::new(dialer(7));
  pump(&mut router, id2, &mut peer2);
  assert_eq!(router.conn_of(&7), Some(id2), "newer conn wins");
  assert!(router.route(7, &hb(10)));
}

#[test]
fn older_connection_validating_late_does_not_evict_newer() {
  let mut router = R::new();
  let (id1, id2) = (ConnId(1), ConnId(2));
  // Both registered up front; the NEWER one (id2) completes its handshake first and binds.
  router.register(id1, acceptor(10), Instant::ORIGIN);
  router.register(id2, acceptor(10), Instant::ORIGIN);
  let mut peer1 = Conn::new(dialer(7));
  let mut peer2 = Conn::new(dialer(7));
  pump(&mut router, id2, &mut peer2);
  assert_eq!(router.conn_of(&7), Some(id2), "newer conn bound first");

  // The OLDER connection's hello arrives late: it must be dropped, never evicting the newer one.
  pump(&mut router, id1, &mut peer1);
  assert_eq!(
    router.conn_of(&7),
    Some(id2),
    "a stale older duplicate cannot evict the healthy newer binding"
  );
  assert!(router.route(7, &hb(10)), "the newer route still works");
}

#[test]
fn internal_removals_surface_via_poll_conn_closed() {
  use crate::transport::TransportError;
  // A faulted connection (garbage hello) is removed AND reported with its fault.
  let mut router = R::new();
  let id = ConnId(1);
  router.register(id, acceptor(10), Instant::ORIGIN);
  let mut out = Vec::new();
  let _ = router.handle_conn_data(id, &[0xFF; 32], false, Instant::ORIGIN, &mut out);
  assert_eq!(
    router.poll_conn_closed(),
    Some((id, Some(TransportError::Record))),
    "a transport fault is reported to the driver with its reason"
  );
  assert_eq!(router.poll_conn_closed(), None);
}

#[test]
fn duplicate_eviction_surfaces_via_poll_conn_closed() {
  let mut router = R::new();
  let (id1, id2) = (ConnId(1), ConnId(2));
  router.register(id1, acceptor(10), Instant::ORIGIN);
  let mut peer1 = Conn::new(dialer(7));
  pump(&mut router, id1, &mut peer1);
  assert_eq!(router.conn_of(&7), Some(id1));

  router.register(id2, acceptor(10), Instant::ORIGIN);
  let mut peer2 = Conn::new(dialer(7));
  pump(&mut router, id2, &mut peer2);
  assert_eq!(router.conn_of(&7), Some(id2));
  // The evicted older connection is reported (clean — no fault) so the driver can close its socket.
  assert_eq!(router.poll_conn_closed(), Some((id1, None)));
}

#[test]
fn unvalidated_conns_are_reaped_after_the_handshake_deadline() {
  use crate::transport::TransportError;
  use core::time::Duration;
  let mut router = R::new();
  router.register(ConnId(1), acceptor(10), Instant::ORIGIN);
  // Before the deadline: nothing reaped.
  router.reap_handshakes(Instant::ORIGIN + Duration::from_secs(9));
  assert_eq!(router.poll_conn_closed(), None);
  // Past the deadline: the never-validating connection is reaped and reported.
  router.reap_handshakes(Instant::ORIGIN + Duration::from_secs(11));
  assert_eq!(
    router.poll_conn_closed(),
    Some((ConnId(1), Some(TransportError::NotValidated))),
    "a connection that never validates is reaped so the driver releases the socket"
  );
}

#[test]
fn duplicate_conn_id_registration_is_rejected_not_replaced() {
  use crate::transport::TransportError;
  let mut router = R::new();
  let id = ConnId(1);
  router.register(id, acceptor(10), Instant::ORIGIN);
  let mut peer = Conn::new(dialer(7));
  pump(&mut router, id, &mut peer);
  assert_eq!(router.conn_of(&7), Some(id), "original conn validated");

  // A second registration under the SAME id: rejected + reported; the original is untouched.
  router.register(id, acceptor(10), Instant::ORIGIN);
  assert_eq!(
    router.poll_conn_closed(),
    Some((id, Some(TransportError::DuplicateConnId))),
    "the rejected registration is reported so the driver closes that socket"
  );
  assert_eq!(
    router.conn_of(&7),
    Some(id),
    "the original binding is untouched"
  );
  assert!(router.route(7, &hb(10)), "the original conn still routes");
}

use crate::transport::stream::Intake;

fn enc(id: u64) -> Vec<u8> {
  let mut v = Vec::new();
  id.encode(&mut v);
  v
}

/// A record layer that validates IMMEDIATELY (fixed identity, not handshaking) and reports a huge
/// outbound backlog, so the very first `send_message` trips the connection's outbound cap.
struct HugeBuffered {
  ident: Vec<u8>,
}

impl crate::transport::stream::sealed::Sealed for HugeBuffered {}

impl RecordIo for HugeBuffered {
  fn handle_transport_data(&mut self, _input: &[u8], _now: Instant) -> Intake {
    Intake::Done
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
  /// Far above the 2 * 64 MiB connection outbound cap, but small enough that `occupancy + 4 +
  /// payload` never overflows `usize`.
  fn buffered_outbound(&self) -> usize {
    256 * 1024 * 1024
  }
  fn is_handshaking(&self) -> bool {
    false
  }
  fn peer_identity(&self) -> Option<&[u8]> {
    Some(&self.ident)
  }
  fn peer_has_closed(&self) -> bool {
    false
  }
  fn is_secure() -> bool {
    false
  }
}

/// Inbound bytes for a connection the router has no record of are silently dropped (`Ok`, nothing
/// delivered, no close queued) — a late read for an already-removed connection is benign.
#[test]
fn data_for_an_unknown_conn_is_ignored() {
  let mut router = R::new();
  let mut out = Vec::new();
  router
    .handle_conn_data(ConnId(9), b"bytes", false, Instant::ORIGIN, &mut out)
    .unwrap();
  assert!(out.is_empty());
  assert_eq!(router.poll_conn_closed(), None);
}

/// A `route` whose `send_message` trips the connection's outbound cap (the peer stopped draining)
/// drops the route immediately and reports the clean close, so no later message is queued into a
/// dead connection.
#[test]
fn route_that_trips_the_outbound_cap_drops_the_route() {
  let mut router: PeerRouter<u64, HugeBuffered> = PeerRouter::new();
  let id = ConnId(1);
  router.register(id, HugeBuffered { ident: enc(7) }, Instant::ORIGIN);
  // One inbound read validates peer 7 (the record reports its identity immediately).
  let mut out = Vec::new();
  router
    .handle_conn_data(id, b"x", false, Instant::ORIGIN, &mut out)
    .unwrap();
  assert_eq!(router.conn_of(&7), Some(id), "the peer bound on validation");

  assert!(
    !router.route(7, &hb(10)),
    "a send that trips the outbound cap drops the route"
  );
  assert_eq!(
    router.poll_conn_closed(),
    Some((id, None)),
    "the outbound-cap stall is reported (a clean close, no fault)"
  );
  assert_eq!(router.conn_of(&7), None, "the route is cleared");
}

/// `Default` yields an empty router (no connections, no bindings).
#[test]
fn default_is_an_empty_router() {
  let router: R = PeerRouter::default();
  assert!(router.conn_of(&1).is_none());
}
