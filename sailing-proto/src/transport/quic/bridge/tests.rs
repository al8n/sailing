use std::{
  net::SocketAddr,
  time::{Duration, Instant},
  vec::Vec,
};

use super::{Bridge, DialError};
use crate::{
  Message, Term, TimeoutNow,
  transport::quic::crypto::{QuicTuning, tests::TestClusterCa},
};

fn addr(port: u16) -> SocketAddr {
  SocketAddr::from(([127, 0, 0, 1], port))
}

fn san(node: u64) -> std::string::String {
  std::format!("node-{node:016x}.{:032x}.sailing", 0u128)
}

/// Two mTLS bridges over the same test cluster CA, with deterministic RNG seeds and keep-alive
/// off (timer determinism — the production default arms it).
fn pair(ca: &TestClusterCa) -> (Bridge<u64>, Bridge<u64>) {
  let tuning = QuicTuning::new().with_keep_alive_interval_millis(0);
  let opts_a = ca.cluster_tls(&san(1)).tuning(tuning).build();
  let opts_b = ca.cluster_tls(&san(2)).tuning(tuning).build();
  (
    Bridge::new(&opts_a, Some([1; 32])),
    Bridge::new(&opts_b, Some([2; 32])),
  )
}

/// Shuttle datagrams between the two bridges until both go quiescent. Each round services both
/// (applying the one-tick deferred endpoint-event feedback) then moves every queued datagram;
/// two consecutive rounds with nothing moved means genuinely quiescent (the deferral is one
/// tick deep). The lossless in-memory pipe: a real driver would read/write UDP sockets.
fn pump(now: Instant, a: &mut Bridge<u64>, b: &mut Bridge<u64>) {
  let mut idle_rounds = 0;
  for _ in 0..128 {
    a.service(now);
    b.service(now);
    let mut moved = false;
    while let Some((dest, bytes)) = a.poll_transmit() {
      assert_eq!(dest, addr(2), "a only ever talks to b");
      b.handle_datagram(now, addr(1), None, &bytes);
      moved = true;
    }
    while let Some((dest, bytes)) = b.poll_transmit() {
      assert_eq!(dest, addr(1), "b only ever talks to a");
      a.handle_datagram(now, addr(2), None, &bytes);
      moved = true;
    }
    if moved {
      idle_rounds = 0;
    } else {
      idle_rounds += 1;
      if idle_rounds >= 2 {
        return;
      }
    }
  }
  panic!("the loopback pump did not quiesce");
}

/// Drive the pair to `Connected` on both sides, returning (a's dialed handle, b's accepted
/// handle).
fn handshake(
  now: Instant,
  a: &mut Bridge<u64>,
  b: &mut Bridge<u64>,
) -> (quinn_proto::ConnectionHandle, quinn_proto::ConnectionHandle) {
  let ha = a.connect(now, addr(2), &san(2), 2u64).expect("dial");
  pump(now, a, b);
  let got_a = a.take_connected().expect("dialer reached Connected");
  assert_eq!(got_a, ha);
  let hb = b.take_connected().expect("acceptor reached Connected");
  (ha, hb)
}

#[test]
fn loopback_pair_exchanges_a_framed_message() {
  let ca = TestClusterCa::generate();
  let (mut a, mut b) = pair(&ca);
  let now = Instant::now();

  let (ha, hb) = handshake(now, &mut a, &mut b);

  // White-box: open the send streams (empty preface — the identity layer is the coordinator's
  // job) and bind the peers directly, as the coordinator's binding policy would.
  a.open_send_and_preface(now, ha, &[]);
  b.open_send_and_preface(now, hb, &[]);
  a.bind_validated(now, ha, 2u64);
  b.bind_validated(now, hb, 1u64);
  pump(now, &mut a, &mut b);

  // A framed consensus message from a to b…
  let msg = Message::TimeoutNow(TimeoutNow::new(Term::new(8), 1u64));
  a.write_framed(now, ha, &msg);
  a.service_if_deferred(now);
  pump(now, &mut a, &mut b);

  // …pops out of b's decoder on its stream-ready drain, decoding to the identical value.
  let ready = b.take_ready_unique();
  assert!(ready.contains(&hb), "b's stream became readable");
  assert!(!b.ingest_recv(now, hb), "a live read never closes inline");
  let frame = b
    .next_frame(hb)
    .expect("no framing violation")
    .expect("one complete frame");
  let decoded = crate::wire::decode_message::<u64>(frame).expect("frame decodes");
  assert_eq!(decoded, msg, "the message survives the QUIC round-trip");
  assert!(
    matches!(b.next_frame(hb), Ok(None)),
    "exactly one frame was sent"
  );

  // …and the reverse direction works over the SAME connection (each side writes the stream it
  // opened and reads the stream the peer opened).
  let reply = Message::TimeoutNow(TimeoutNow::new(Term::new(9), 2u64));
  b.write_framed(now, hb, &reply);
  b.service_if_deferred(now);
  pump(now, &mut a, &mut b);
  let ready = a.take_ready_unique();
  assert!(ready.contains(&ha));
  assert!(!a.ingest_recv(now, ha));
  let frame = a.next_frame(ha).expect("ok").expect("one frame");
  assert_eq!(
    crate::wire::decode_message::<u64>(frame).expect("decodes"),
    reply
  );
}

#[test]
fn dial_at_capacity_is_refused_with_a_typed_error() {
  let ca = TestClusterCa::generate();
  let tuning = QuicTuning::new().with_keep_alive_interval_millis(0);
  let opts = ca
    .cluster_tls(&san(1))
    .tuning(tuning)
    .build()
    .with_max_connections(1);
  let mut a: Bridge<u64> = Bridge::new(&opts, Some([1; 32]));
  let now = Instant::now();

  a.connect(now, addr(2), &san(2), 2u64).expect("first dial");
  match a.connect(now, addr(3), &san(2), 3u64) {
    Err(DialError::AtCapacity { cap }) => assert_eq!(cap, 1),
    other => panic!("expected AtCapacity, got {other:?}"),
  }
  assert_eq!(a.table_len(), 1, "the refused dial allocated nothing");
}

#[test]
fn unvalidated_connections_stage_no_consensus_bytes() {
  let ca = TestClusterCa::generate();
  let (mut a, mut b) = pair(&ca);
  let now = Instant::now();
  let (ha, _hb) = handshake(now, &mut a, &mut b);

  a.open_send_and_preface(now, ha, &[]);
  // Still `Authenticating`: a consensus write must stage NOTHING (no byte rides out ahead of the
  // identity preface / before the peer is bound).
  let msg = Message::TimeoutNow(TimeoutNow::new(Term::new(1), 1u64));
  a.write_framed(now, ha, &msg);
  a.service_if_deferred(now);
  pump(now, &mut a, &mut b);
  let ready = b.take_ready_unique();
  for h in ready {
    let _ = b.ingest_recv(now, h);
    assert!(
      matches!(b.next_frame(h), Ok(None)),
      "no consensus frame may arrive from an unvalidated sender"
    );
  }
}

#[test]
fn lost_connection_frees_the_quinn_slab_after_drain() {
  let ca = TestClusterCa::generate();
  let (mut a, mut b) = pair(&ca);
  let mut now = Instant::now();
  let (ha, _hb) = handshake(now, &mut a, &mut b);

  assert_eq!(a.endpoint_open_connections(), 1);
  a.close_local(now, ha);
  assert_eq!(a.take_lost(), Some(ha), "the close is reported");
  a.reap(ha);

  // Drive a's timers through the drain period: the connection reaches `Drained`, the deferred
  // endpoint event frees quinn's slab slot, and the local entry is removed.
  for _ in 0..64 {
    now += Duration::from_millis(500);
    a.handle_timeout(now);
    while a.has_pending_work() {
      a.service(now);
    }
    if a.endpoint_open_connections() == 0 {
      break;
    }
  }
  assert_eq!(
    a.endpoint_open_connections(),
    0,
    "Drained must free quinn's connection slab"
  );
  assert_eq!(a.table_len(), 0, "the local entry is reaped with it");
}

/// FAILS-ON-OLD: under a budget-based pre-auth read (rather than one steered by the first
/// frame's own length prefix), a peer that pipelines a consensus frame behind a SHORT hello in
/// one flight would have up to a full hello-budget of tail bytes pulled into the decoder before
/// validation. Exactly the hello must be readable pre-validation — the tail stays backpressured
/// in quinn until the connection validates, then arrives on the rescheduled read.
#[test]
fn pipelined_frames_behind_the_hello_stay_unread_until_validated() {
  use crate::transport::{ClusterId, labeled};

  let ca = TestClusterCa::generate();
  let (mut a, mut b) = pair(&ca);
  let now = Instant::now();
  let (ha, hb) = handshake(now, &mut a, &mut b);

  // a's preface: a real (short) hello — 8-byte id, far under the hello cap.
  let mut id = Vec::new();
  crate::Data::encode(&1u64, &mut id);
  let hello = labeled::build_hello(&ClusterId([7; 16]), &id);
  a.open_send_and_preface(now, ha, &hello);
  // …with a consensus frame PIPELINED directly behind it, before any validation handshake.
  let msg = Message::TimeoutNow(TimeoutNow::new(Term::new(3), 1u64));
  let mut payload = Vec::new();
  crate::wire::encode_message(&msg, &mut payload);
  let mut framed = Vec::new();
  crate::transport::frame::encode_frame(&payload, &mut framed);
  a.stage_outbound_for_test(ha, &framed);
  a.flush_stream(now, ha);
  pump(now, &mut a, &mut b);

  // Pre-validation, b reads EXACTLY the hello: one complete frame, and not a byte of the tail.
  let ready = b.take_ready_unique();
  assert!(ready.contains(&hb));
  assert!(!b.ingest_recv(now, hb));
  let frame = b.next_frame(hb).expect("ok").expect("the hello frame");
  assert_eq!(
    &frame[..],
    &hello[..],
    "the first frame is the hello, byte-exact"
  );
  assert!(
    matches!(b.next_frame(hb), Ok(None)),
    "the pipelined tail must NOT be readable before validation"
  );
  // Even an explicit re-read pre-validation pulls nothing more (the frame boundary, not a
  // budget, stops the read).
  assert!(!b.ingest_recv(now, hb));
  assert!(matches!(b.next_frame(hb), Ok(None)));

  // Validation re-schedules the read; the tail then arrives intact.
  b.bind_validated(now, hb, 1u64);
  pump(now, &mut a, &mut b);
  let ready = b.take_ready_unique();
  assert!(ready.contains(&hb), "bind_validated reschedules the read");
  assert!(!b.ingest_recv(now, hb));
  let tail = b.next_frame(hb).expect("ok").expect("the pipelined frame");
  assert_eq!(
    crate::wire::decode_message::<u64>(tail).expect("decodes"),
    msg,
    "the backpressured tail is delivered after validation, nothing lost"
  );
}

/// A first frame whose header DECLARES more than one hello's worth can never be a preface: the
/// connection closes the moment the header arrives, and the over-declared body is never
/// buffered.
#[test]
fn over_declared_preface_closes_at_the_header() {
  let ca = TestClusterCa::generate();
  let (mut a, mut b) = pair(&ca);
  let now = Instant::now();
  let (ha, hb) = handshake(now, &mut a, &mut b);

  // a stages a frame declaring 1 MiB — over the framed-hello bound — as its FIRST bytes.
  a.open_send_and_preface(now, ha, &[]);
  let mut framed = Vec::new();
  crate::transport::frame::encode_frame(&[0xAB; 1024 * 1024], &mut framed);
  a.stage_outbound_for_test(ha, &framed);
  a.flush_stream(now, ha);
  pump(now, &mut a, &mut b);

  let ready = b.take_ready_unique();
  assert!(ready.contains(&hb));
  assert!(
    b.ingest_recv(now, hb),
    "an over-declared preface closes inline with nothing queued"
  );
  assert_eq!(b.take_lost(), Some(hb), "the close is reported");
}

/// The maximal hostile header: a first frame declaring 0xFFFF_FFFF bytes. The declared length is
/// compared BEFORE the 4-byte prefix is added, so the close-at-header verdict cannot be defeated
/// by usize wrap-around on a 32-bit target.
#[test]
fn max_declared_preface_closes_at_the_header() {
  let ca = TestClusterCa::generate();
  let (mut a, mut b) = pair(&ca);
  let now = Instant::now();
  let (ha, hb) = handshake(now, &mut a, &mut b);

  a.open_send_and_preface(now, ha, &[]);
  a.stage_outbound_for_test(ha, &[0xFF, 0xFF, 0xFF, 0xFF]);
  a.flush_stream(now, ha);
  pump(now, &mut a, &mut b);

  let ready = b.take_ready_unique();
  assert!(ready.contains(&hb));
  assert!(
    b.ingest_recv(now, hb),
    "the max header closes at the header"
  );
  assert_eq!(b.take_lost(), Some(hb));
}

/// FAILS-ON-OLD: without applying the deferred feedback BEFORE the cap check, a reconnect
/// racing the previous connection's teardown is refused for capacity a QUEUED `Drained` event
/// has already released. The freed slot must be visible to the very dial that needs it.
#[test]
fn reconnect_is_not_refused_while_a_drained_event_holds_the_slot() {
  let ca = TestClusterCa::generate();
  let tuning = QuicTuning::new().with_keep_alive_interval_millis(0);
  let opts = ca
    .cluster_tls(&san(1))
    .tuning(tuning)
    .build()
    .with_max_connections(1);
  let mut a: Bridge<u64> = Bridge::new(&opts, Some([1; 32]));
  let mut now = Instant::now();

  let ha = a.connect(now, addr(2), &san(2), 2u64).expect("first dial");
  a.close_local(now, ha);
  assert_eq!(a.take_lost(), Some(ha));
  a.reap(ha);

  // Drive timers until the terminal `Drained` is QUEUED on the deferral but not yet applied —
  // the window where the table still holds the entry that the next pass would free.
  let mut windowed = false;
  for _ in 0..64 {
    now += Duration::from_millis(500);
    a.handle_timeout(now);
    if a.pending_endpoint_events_len() > 0 && a.table_len() == 1 {
      windowed = true;
      break;
    }
    if a.table_len() == 0 {
      break; // already applied (no observable window this run)
    }
  }
  assert!(
    windowed,
    "the one-tick deferral exposes the queued-Drained window after a drain pass"
  );

  // At the window, the table is nominally AT capacity — but the queued Drained owns the slot.
  // The reconnect must succeed, not bounce with AtCapacity.
  a.connect(now, addr(2), &san(2), 2u64)
    .expect("a reconnect must see the capacity the queued Drained releases");
  assert_eq!(a.table_len(), 1, "the freed slot was reused, not exceeded");

  // And a dial that IS refused still leaves no deferred feedback invisible: every connect exit
  // runs a service pass, so `apply_deferred`'s side effects (CID-rotation frames fed into a
  // connection) reach the transmit queue rather than stranding until unrelated activity. The
  // assert is on the pass COUNTER — a drained deferral queue alone would also hold for an exit
  // that applied the feedback and then skipped the pass.
  let services_before = a.services_run();
  let err = a.connect(now, addr(3), &san(2), 3u64).unwrap_err();
  assert!(matches!(err, DialError::AtCapacity { cap: 1 }));
  assert_eq!(a.pending_endpoint_events_len(), 0);
  assert!(
    a.services_run() > services_before,
    "a REFUSED dial must still run a service pass (the applied feedback's output is otherwise      invisible to poll_transmit and has_pending_work)"
  );
}

/// FAILS-ON-OLD: quinn reuses slab handles after Drained, so a `lost` entry surviving the
/// Drained purge would — once the handle is reused by a NEW connection — make the coordinator's
/// end-of-drain `reap` unbind the new connection's freshly validated route. The Drained arm must
/// purge EVERY per-handle queue.
#[test]
fn drained_purge_clears_stale_queues_before_handle_reuse() {
  let ca = TestClusterCa::generate();
  let (mut a, mut b) = pair(&ca);
  let mut now = Instant::now();
  let (ha, _hb) = handshake(now, &mut a, &mut b);

  // Close locally — `lost` now holds the handle — and deliberately DO NOT drain it before the
  // connection drains (a coordinator pass that closes late in its drain does exactly this).
  a.close_local(now, ha);
  assert_eq!(a.table_len(), 1, "the entry is kept for the drain");

  // Drive timers until the Drained applies (slab + table freed) WITHOUT ever taking `lost`.
  for _ in 0..64 {
    now += Duration::from_millis(500);
    a.handle_timeout(now);
    if a.endpoint_open_connections() == 0 && a.table_len() == 0 {
      break;
    }
  }
  assert_eq!(a.endpoint_open_connections(), 0, "drained");

  // The stale `lost` entry must have been purged WITH the Drained — before any reuse.
  assert_eq!(
    a.take_lost(),
    None,
    "Drained purges the stale lost entry (quinn will reuse this handle)"
  );

  // The slab slot is free: a redial REUSES the same raw handle. The new connection's lifecycle
  // must be untouched by the old generation's queues.
  let ha2 = a.connect(now, addr(2), &san(2), 2u64).expect("redial");
  assert_eq!(
    ha2, ha,
    "quinn reuses the freed slab handle (the hazard precondition)"
  );
  assert_eq!(
    a.take_lost(),
    None,
    "no stale loss mis-targets the reused handle"
  );
}

/// `min_opt` folds the two optional deadlines (quinn's earliest connection timer and the auth
/// deadline) without either masking the other.
#[test]
fn min_opt_folds_both_optional_instants() {
  let t0 = Instant::now();
  let t1 = t0 + Duration::from_secs(1);
  assert_eq!(
    super::min_opt(Some(t0), Some(t1)),
    Some(t0),
    "both present → the earlier"
  );
  assert_eq!(super::min_opt(Some(t1), Some(t0)), Some(t0));
  assert_eq!(super::min_opt(Some(t0), None), Some(t0));
  assert_eq!(super::min_opt(None, Some(t1)), Some(t1));
  assert_eq!(super::min_opt(None, None), None);
}

/// An accept-only bridge (no client config) cannot dial: `connect` surfaces a typed `DialError`
/// (never a panic) after running its service pass.
#[test]
fn dial_without_a_client_config_is_refused() {
  let opts = crate::transport::quic::QuicOptions::new(
    quinn_proto::EndpointConfig::default(),
    None,
    None,
    QuicTuning::new(),
  );
  let mut bridge: Bridge<u64> = Bridge::new(&opts, Some([1; 32]));
  let now = Instant::now();
  assert!(
    bridge.connect(now, addr(2), &san(2), 2u64).is_err(),
    "an accept-only bridge has no client config to dial with"
  );
}
