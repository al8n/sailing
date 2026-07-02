use std::{
  net::SocketAddr,
  time::{Duration, Instant},
  vec::Vec,
};

use quinn_proto::ConnectionHandle;

use super::{Bridge, DialError};
use crate::{
  ConfState, Index, InstallSnapshot, Message, SnapshotMeta, Term, TimeoutNow,
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

/// Drive the pair to a fully validated connection with the consensus stream adopted in BOTH
/// directions (each side opened its send stream and read one frame off the peer-opened stream),
/// returning (a's handle, b's handle). The fault-injection tests below start from here.
fn validated(
  now: Instant,
  a: &mut Bridge<u64>,
  b: &mut Bridge<u64>,
) -> (ConnectionHandle, ConnectionHandle) {
  let (ha, hb) = handshake(now, a, b);
  a.open_send_and_preface(now, ha, &[]);
  b.open_send_and_preface(now, hb, &[]);
  a.bind_validated(now, ha, 2u64);
  b.bind_validated(now, hb, 1u64);
  pump(now, a, b);
  // One frame each way so both sides adopt the peer-opened stream (its recv sid).
  let m = Message::TimeoutNow(TimeoutNow::new(Term::new(1), 1u64));
  a.write_framed(now, ha, &m);
  a.service_if_deferred(now);
  pump(now, a, b);
  let _ = b.take_ready_unique();
  assert!(!b.ingest_recv(now, hb));
  let _ = b.next_frame(hb);
  b.write_framed(now, hb, &m);
  b.service_if_deferred(now);
  pump(now, a, b);
  let _ = a.take_ready_unique();
  assert!(!a.ingest_recv(now, ha));
  let _ = a.next_frame(ha);
  (ha, hb)
}

/// An inbound `NewConnection` while the table is AT the connection cap is REFUSED (a stateless
/// close), allocating no `Connection`.
#[test]
fn handle_datagram_refuses_a_new_connection_at_capacity() {
  let ca = TestClusterCa::generate();
  let tuning = QuicTuning::new().with_keep_alive_interval_millis(0);
  let opts_b = ca
    .cluster_tls(&san(2))
    .tuning(tuning)
    .build()
    .with_max_connections(1);
  let mut b: Bridge<u64> = Bridge::new(&opts_b, Some([2; 32]));
  let opts_a = ca.cluster_tls(&san(1)).tuning(tuning).build();
  let mut a: Bridge<u64> = Bridge::new(&opts_a, Some([1; 32]));
  let opts_c = ca.cluster_tls(&san(3)).tuning(tuning).build();
  let mut c: Bridge<u64> = Bridge::new(&opts_c, Some([3; 32]));
  let now = Instant::now();

  // a dials b; deliver a's Initial so b accepts it, filling its single slot.
  a.connect(now, addr(2), &san(2), 2u64).expect("a dials b");
  let (_, init_a) = a.poll_transmit().expect("a's Initial");
  b.handle_datagram(now, addr(1), None, &init_a);
  assert_eq!(b.table_len(), 1, "b accepted a's connection");
  while b.poll_transmit().is_some() {} // drain b's handshake response

  // c dials b; its Initial arrives while b is at capacity → refused, nothing allocated, a stateless
  // close queued.
  c.connect(now, addr(2), &san(2), 2u64).expect("c dials b");
  let (_, init_c) = c.poll_transmit().expect("c's Initial");
  b.handle_datagram(now, addr(3), None, &init_c);
  assert_eq!(
    b.table_len(),
    1,
    "the over-cap inbound allocated no connection"
  );
  assert!(
    b.poll_transmit().is_some(),
    "b queued a stateless refuse for the over-cap Initial"
  );
}

/// An unroutable garbage datagram warrants no stateless reply and allocates nothing (the
/// `DatagramEvent` `None` arm).
#[test]
fn handle_datagram_drops_unroutable_bytes() {
  let ca = TestClusterCa::generate();
  let (mut a, _b) = pair(&ca);
  let now = Instant::now();
  a.handle_datagram(now, addr(2), None, &[0u8]);
  assert!(a.poll_transmit().is_none(), "garbage drew no transmit");
  assert_eq!(a.table_len(), 0);
}

/// A long-header packet advertising an unsupported QUIC version draws a stateless Version
/// Negotiation reply (the `DatagramEvent::Response` arm), allocating no connection.
#[test]
fn handle_datagram_emits_a_stateless_response() {
  let ca = TestClusterCa::generate();
  let (mut a, _b) = pair(&ca);
  let now = Instant::now();
  let mut dg = std::vec![0xC0u8]; // long header + fixed bit
  dg.extend_from_slice(&[0x0a, 0x0a, 0x0a, 0x0a]); // an unsupported (GREASE) version
  dg.push(8);
  dg.extend_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]); // DCID
  dg.push(8);
  dg.extend_from_slice(&[8, 7, 6, 5, 4, 3, 2, 1]); // SCID
  dg.resize(1200, 0); // pad to the minimum datagram size
  a.handle_datagram(now, addr(2), None, &dg);
  assert!(
    a.poll_transmit().is_some(),
    "an unsupported version draws a stateless VN response"
  );
  assert_eq!(
    a.table_len(),
    0,
    "version negotiation allocates no connection"
  );
}

/// An Initial whose long header parses (so quinn yields `NewConnection`) but whose encrypted body
/// fails authentication takes the accept-ERROR path: no connection is allocated.
#[test]
fn handle_datagram_surfaces_an_accept_error() {
  let ca = TestClusterCa::generate();
  let tuning = QuicTuning::new().with_keep_alive_interval_millis(0);
  let opts_a = ca.cluster_tls(&san(1)).tuning(tuning).build();
  let opts_b = ca.cluster_tls(&san(2)).tuning(tuning).build();
  let mut a: Bridge<u64> = Bridge::new(&opts_a, Some([1; 32]));
  let mut b: Bridge<u64> = Bridge::new(&opts_b, Some([2; 32]));
  let now = Instant::now();
  a.connect(now, addr(2), &san(2), 2u64).expect("dial");
  let (_, mut init) = a.poll_transmit().expect("a's Initial");
  // Corrupt the encrypted-payload tail: the header still parses (→ NewConnection), but accept's
  // AEAD authentication of the Initial fails (the accept-error path, no response queued here).
  let n = init.len();
  init[n - 1] ^= 0xFF;
  b.handle_datagram(now, addr(1), None, &init);
  assert_eq!(
    b.table_len(),
    0,
    "an Initial that fails authentication allocates no connection"
  );
}

/// A connection that completes the QUIC handshake but never validates (sends no valid hello) is
/// reaped once it sits `Authenticating` past the auth deadline. The idle timeout is set ABOVE the
/// auth deadline so the auth-deadline reap — not quinn's idle timeout — is what closes it.
#[test]
fn authenticating_connection_is_reaped_past_the_auth_deadline() {
  let ca = TestClusterCa::generate();
  let tuning = QuicTuning::new()
    .with_keep_alive_interval_millis(0)
    .with_idle_timeout_millis(10_000);
  let opts_a = ca.cluster_tls(&san(1)).tuning(tuning).build();
  let opts_b = ca.cluster_tls(&san(2)).tuning(tuning).build();
  let mut a: Bridge<u64> = Bridge::new(&opts_a, Some([1; 32]));
  let mut b: Bridge<u64> = Bridge::new(&opts_b, Some([2; 32]));
  let now = Instant::now();
  let (ha, _hb) = handshake(now, &mut a, &mut b);
  let later = now + super::AUTH_DEADLINE + Duration::from_secs(1);
  a.handle_timeout(later);
  assert_eq!(
    a.take_lost(),
    Some(ha),
    "the silent Authenticating connection is reaped"
  );
}

/// `open_send_and_preface` is a no-op on an absent handle and closes the connection on a preface
/// that exceeds the hello bound (it could never authenticate).
#[test]
fn open_send_and_preface_rejects_oversized_and_absent() {
  let ca = TestClusterCa::generate();
  let (mut a, mut b) = pair(&ca);
  let now = Instant::now();
  let (ha, _hb) = handshake(now, &mut a, &mut b);
  a.open_send_and_preface(now, ConnectionHandle(99_999), &[1, 2, 3]); // absent → no-op
  let oversized = std::vec![0u8; super::MAX_HELLO_LEN + 1];
  a.open_send_and_preface(now, ha, &oversized);
  assert_eq!(a.take_lost(), Some(ha), "an over-budget preface closes");
}

/// A second `open_send_and_preface` on the same connection is a no-op (`preface_done`).
#[test]
fn open_send_and_preface_is_idempotent() {
  let ca = TestClusterCa::generate();
  let (mut a, mut b) = pair(&ca);
  let now = Instant::now();
  let (ha, _hb) = handshake(now, &mut a, &mut b);
  a.open_send_and_preface(now, ha, &[]);
  let sid = a
    .send_sid_for_test(ha)
    .expect("first call opens the send stream");
  a.open_send_and_preface(now, ha, &[9, 9, 9]); // preface_done → no-op
  assert_eq!(a.send_sid_for_test(ha), Some(sid));
}

/// `bind_validated` is idempotent: a duplicate validate, or one racing a close, is a no-op.
#[test]
fn bind_validated_is_idempotent() {
  let ca = TestClusterCa::generate();
  let (mut a, mut b) = pair(&ca);
  let now = Instant::now();
  let (ha, _hb) = handshake(now, &mut a, &mut b);
  a.open_send_and_preface(now, ha, &[]);
  a.bind_validated(now, ha, 2u64);
  assert!(a.is_validated(ha));
  a.bind_validated(now, ha, 2u64); // already validated → no-op
  assert!(a.is_validated(ha));
  a.close_local(now, ha);
  a.bind_validated(now, ha, 2u64); // closed → no resurrection
}

/// On validate, same-peer connections beyond [`PER_PEER_CONN_LIMIT`] are reaped oldest-first (the
/// just-validated handle excluded).
#[test]
fn bind_validated_reaps_excess_same_peer_connections() {
  let ca = TestClusterCa::generate();
  let opts = ca
    .cluster_tls(&san(1))
    .tuning(QuicTuning::new().with_keep_alive_interval_millis(0))
    .build();
  let mut a: Bridge<u64> = Bridge::new(&opts, Some([1; 32]));
  let now = Instant::now();
  let h1 = a.connect(now, addr(2), &san(2), 2u64).unwrap();
  let h2 = a.connect(now, addr(2), &san(2), 2u64).unwrap();
  let h3 = a.connect(now, addr(2), &san(2), 2u64).unwrap();
  let h4 = a.connect(now, addr(2), &san(2), 2u64).unwrap();
  a.bind_validated(now, h1, 2u64);
  a.bind_validated(now, h2, 2u64);
  a.bind_validated(now, h3, 2u64);
  assert_eq!(a.take_lost(), None, "within the bound nothing is reaped");
  a.bind_validated(now, h4, 2u64);
  assert_eq!(
    a.take_lost(),
    Some(h1),
    "the oldest same-peer connection is reaped past the bound"
  );
}

/// `bind_validated` flushes (and services) consensus frames staged while authenticating, so a
/// pipelined send reaches the peer the moment the connection validates.
#[test]
fn bind_validated_flushes_staged_consensus_frames() {
  let ca = TestClusterCa::generate();
  let (mut a, mut b) = pair(&ca);
  let now = Instant::now();
  let (ha, hb) = handshake(now, &mut a, &mut b);
  a.open_send_and_preface(now, ha, &[]);
  let msg = Message::TimeoutNow(TimeoutNow::new(Term::new(5), 1u64));
  let mut payload = Vec::new();
  crate::wire::encode_message(&msg, &mut payload);
  let mut framed = Vec::new();
  crate::transport::frame::encode_frame(&payload, &mut framed);
  a.stage_outbound_for_test(ha, &framed);
  b.open_send_and_preface(now, hb, &[]);
  b.bind_validated(now, hb, 1u64);
  a.bind_validated(now, ha, 2u64); // flushes the staged frame + services
  pump(now, &mut a, &mut b);
  let ready = b.take_ready_unique();
  assert!(ready.contains(&hb));
  assert!(!b.ingest_recv(now, hb));
  let frame = b
    .next_frame(hb)
    .expect("ok")
    .expect("the staged frame flushed on validate");
  assert_eq!(
    crate::wire::decode_message::<u64>(frame).expect("decodes"),
    msg
  );
}

/// `close_local` is idempotent and a no-op on an absent handle.
#[test]
fn close_local_is_idempotent_and_absent_safe() {
  let ca = TestClusterCa::generate();
  let (mut a, mut b) = pair(&ca);
  let now = Instant::now();
  let (ha, _hb) = handshake(now, &mut a, &mut b);
  a.close_local(now, ha);
  assert_eq!(a.take_lost(), Some(ha));
  a.close_local(now, ha); // already Closed → no second lost
  assert_eq!(a.take_lost(), None);
  a.close_local(now, ConnectionHandle(99_999)); // absent → no-op
  assert_eq!(a.take_lost(), None);
}

/// A message whose encoded frame exceeds `MAX_FRAME_LEN` is counted and dropped (the peer's decoder
/// would fatally reject its declared length).
#[test]
fn write_framed_drops_an_oversized_message() {
  let ca = TestClusterCa::generate();
  let (mut a, mut b) = pair(&ca);
  let now = Instant::now();
  let (ha, _hb) = handshake(now, &mut a, &mut b);
  a.open_send_and_preface(now, ha, &[]);
  a.bind_validated(now, ha, 2u64);
  let data = bytes::Bytes::from(std::vec![0u8; super::MAX_FRAME_LEN + 1]);
  let meta = SnapshotMeta::new(
    Index::new(1),
    Term::new(1),
    ConfState::from_voters(std::vec![1u64, 2]),
  );
  let msg = Message::InstallSnapshot(InstallSnapshot::new(Term::new(1), 1u64, meta, data));
  assert_eq!(a.oversized_dropped(), 0);
  a.write_framed(now, ha, &msg);
  assert_eq!(
    a.oversized_dropped(),
    1,
    "the over-cap message is counted and dropped"
  );
}

/// A staged outbound buffer that would exceed `MAX_CONN_OUT_BUF` means the peer stopped consuming;
/// the next frame closes the connection.
#[test]
fn write_framed_closes_on_outbound_overflow() {
  let ca = TestClusterCa::generate();
  let (mut a, mut b) = pair(&ca);
  let now = Instant::now();
  let (ha, _hb) = handshake(now, &mut a, &mut b);
  a.open_send_and_preface(now, ha, &[]);
  a.bind_validated(now, ha, 2u64);
  a.fill_outbound_for_test(ha, super::MAX_CONN_OUT_BUF);
  let msg = Message::TimeoutNow(TimeoutNow::new(Term::new(1), 1u64));
  a.write_framed(now, ha, &msg);
  assert_eq!(
    a.take_lost(),
    Some(ha),
    "an over-cap outbound buffer closes the connection"
  );
}

/// `ingest_recv` is a no-op on an absent handle and on a still-handshaking connection.
#[test]
fn ingest_recv_skips_absent_and_handshaking() {
  let ca = TestClusterCa::generate();
  let opts = ca
    .cluster_tls(&san(1))
    .tuning(QuicTuning::new().with_keep_alive_interval_millis(0))
    .build();
  let mut a: Bridge<u64> = Bridge::new(&opts, Some([1; 32]));
  let now = Instant::now();
  assert!(!a.ingest_recv(now, ConnectionHandle(99_999)), "absent");
  let ha = a.connect(now, addr(2), &san(2), 2u64).unwrap();
  assert!(!a.ingest_recv(now, ha), "Handshaking is skipped");
}

/// `ingest_recv` returns without reading when the peer has opened no stream yet (no recv sid).
#[test]
fn ingest_recv_returns_when_no_peer_stream_opened() {
  let ca = TestClusterCa::generate();
  let (mut a, mut b) = pair(&ca);
  let now = Instant::now();
  let (_ha, hb) = handshake(now, &mut a, &mut b);
  assert!(!b.ingest_recv(now, hb), "no peer-opened stream yet");
}

/// A SECOND peer-opened bidi stream violates the one-consensus-stream contract: the receiver closes
/// inline on the second adoption.
#[test]
fn second_peer_stream_is_a_violation() {
  let ca = TestClusterCa::generate();
  let (mut a, mut b) = pair(&ca);
  let now = Instant::now();
  let (ha, hb) = validated(now, &mut a, &mut b);
  a.open_extra_stream_for_test(ha);
  a.service(now);
  pump(now, &mut a, &mut b);
  let ready = b.take_ready_unique();
  assert!(ready.contains(&hb));
  assert!(
    b.ingest_recv(now, hb),
    "a second peer-opened stream closes the connection inline"
  );
  assert_eq!(b.take_lost(), Some(hb));
}

/// A frame larger than one `READ_BUDGET`: the first read takes a budget and DEFERS the rest to the
/// next pump, draining the window one budget per pump.
#[test]
fn oversized_read_defers_to_the_next_pump() {
  let ca = TestClusterCa::generate();
  let (mut a, mut b) = pair(&ca);
  let now = Instant::now();
  let (ha, hb) = validated(now, &mut a, &mut b);
  let big = super::READ_BUDGET + 64 * 1024;
  let data = bytes::Bytes::from(std::vec![3u8; big]);
  let meta = SnapshotMeta::new(
    Index::new(1),
    Term::new(1),
    ConfState::from_voters(std::vec![1u64, 2]),
  );
  let msg = Message::InstallSnapshot(InstallSnapshot::new(Term::new(1), 1u64, meta, data));
  a.write_framed(now, ha, &msg);
  a.service_if_deferred(now);
  pump(now, &mut a, &mut b);
  let ready = b.take_ready_unique();
  assert!(ready.contains(&hb));
  assert!(!b.ingest_recv(now, hb), "first read takes one budget");
  assert!(
    b.has_pending_work(),
    "the half-read connection is deferred to the next pump"
  );
  assert!(
    matches!(b.next_frame(hb), Ok(None)),
    "the frame is not yet complete"
  );
  // The deferred read folds in on the next drain and completes the frame.
  for _ in 0..16 {
    let ready = b.take_ready_unique();
    if ready.contains(&hb) {
      b.ingest_recv(now, hb);
    }
    if let Ok(Some(f)) = b.next_frame(hb) {
      assert_eq!(
        crate::wire::decode_message::<u64>(f).expect("decodes"),
        msg,
        "the deferred large frame reassembles intact"
      );
      return;
    }
    pump(now, &mut a, &mut b);
  }
  panic!("the deferred large frame never fully reassembled");
}

/// A peer's graceful FIN delivers the frames read before it, THEN latches the close
/// (deliver-before-close).
#[test]
fn graceful_fin_delivers_then_latches_close() {
  let ca = TestClusterCa::generate();
  let (mut a, mut b) = pair(&ca);
  let now = Instant::now();
  let (ha, hb) = validated(now, &mut a, &mut b);
  let msg = Message::TimeoutNow(TimeoutNow::new(Term::new(7), 2u64));
  b.write_framed(now, hb, &msg);
  b.service_if_deferred(now);
  let b_send = b.send_sid_for_test(hb).expect("b's send stream");
  b.finish_send_for_test(hb, b_send);
  b.service(now);
  pump(now, &mut a, &mut b);
  let ready = a.take_ready_unique();
  assert!(ready.contains(&ha));
  assert!(
    !a.ingest_recv(now, ha),
    "a graceful FIN is not an inline close"
  );
  assert!(a.fin_received(ha), "the FIN latched after the frame");
  let frame = a
    .next_frame(ha)
    .expect("ok")
    .expect("the frame before the FIN");
  assert_eq!(
    crate::wire::decode_message::<u64>(frame).expect("decodes"),
    msg
  );
}

/// A read on a dead recv stream (here, stopped by us) errors: the peer's bytes are unrecoverable,
/// so the connection closes inline.
#[test]
fn ingest_recv_closes_on_a_dead_recv_stream() {
  let ca = TestClusterCa::generate();
  let (mut a, mut b) = pair(&ca);
  let now = Instant::now();
  let (ha, _hb) = validated(now, &mut a, &mut b);
  let a_recv = a.recv_sid_for_test(ha).expect("a adopted the peer stream");
  a.stop_recv_for_test(ha, a_recv);
  assert!(
    a.ingest_recv(now, ha),
    "a dead recv stream closes the connection inline"
  );
  assert_eq!(a.take_lost(), Some(ha));
}

/// A peer RESET abandons its send half: the bytes are gone, so the connection closes inline.
#[test]
fn peer_reset_closes_the_connection() {
  let ca = TestClusterCa::generate();
  let (mut a, mut b) = pair(&ca);
  let now = Instant::now();
  let (ha, hb) = validated(now, &mut a, &mut b);
  let b_send = b.send_sid_for_test(hb).expect("b's send stream");
  b.reset_send_for_test(hb, b_send);
  b.service(now);
  pump(now, &mut a, &mut b);
  let ready = a.take_ready_unique();
  assert!(ready.contains(&ha));
  assert!(
    a.ingest_recv(now, ha),
    "a peer RESET closes the connection inline"
  );
  assert_eq!(a.take_lost(), Some(ha));
}

/// `next_frame` and `flush_stream` on an absent handle are no-ops.
#[test]
fn next_frame_and_flush_on_absent_handles_are_noops() {
  let ca = TestClusterCa::generate();
  let (mut a, _b) = pair(&ca);
  let now = Instant::now();
  let bogus = ConnectionHandle(99_999);
  assert!(matches!(a.next_frame(bogus), Ok(None)));
  a.flush_stream(now, bogus);
}

/// FAILS-ON-OLD: staged bytes with no send stream open must NOT open the stream until the identity
/// preface has been staged. The preface is frame zero on the send stream, so a consensus frame
/// queued behind an un-sent preface has to be held back — on the old code the flush opened the
/// stream and wrote that frame as the stream's first bytes. Once the preface step latches
/// `preface_done`, the flush opens the stream and drains the staged bytes.
#[test]
fn flush_outbound_gates_the_stream_open_on_the_preface() {
  let ca = TestClusterCa::generate();
  let (mut a, mut b) = pair(&ca);
  let now = Instant::now();
  let (ha, _hb) = handshake(now, &mut a, &mut b);
  assert!(a.send_sid_for_test(ha).is_none());

  // A consensus frame staged with `preface_done` still false: the flush leaves the stream unopened.
  let msg = Message::TimeoutNow(TimeoutNow::new(Term::new(4), 1u64));
  let mut payload = Vec::new();
  crate::wire::encode_message(&msg, &mut payload);
  let mut framed = Vec::new();
  crate::transport::frame::encode_frame(&payload, &mut framed);
  a.stage_outbound_for_test(ha, &framed);
  a.flush_stream(now, ha);
  assert_eq!(
    a.send_sid_for_test(ha),
    None,
    "no send stream opens before the preface is staged"
  );

  // The preface step latches `preface_done`, so the flush now opens the stream for the staged bytes.
  a.open_send_and_preface(now, ha, &[]);
  assert!(
    a.send_sid_for_test(ha).is_some(),
    "the preface step opens the send stream"
  );
}

/// A write past the (small) stream flow-control window BLOCKS partway; only the peer's reads
/// (regranting credit) let the sender resume on the Writable signal.
#[test]
fn blocked_write_backpressures_then_resumes_on_writable() {
  let ca = TestClusterCa::generate();
  let tuning = QuicTuning::new()
    .with_keep_alive_interval_millis(0)
    .with_stream_receive_window(16 * 1024)
    .with_connection_receive_window(64 * 1024);
  let opts_a = ca.cluster_tls(&san(1)).tuning(tuning).build();
  let opts_b = ca.cluster_tls(&san(2)).tuning(tuning).build();
  let mut a: Bridge<u64> = Bridge::new(&opts_a, Some([1; 32]));
  let mut b: Bridge<u64> = Bridge::new(&opts_b, Some([2; 32]));
  let now = Instant::now();
  let (ha, hb) = validated(now, &mut a, &mut b);

  let data = bytes::Bytes::from(std::vec![5u8; 200 * 1024]);
  let meta = SnapshotMeta::new(
    Index::new(1),
    Term::new(1),
    ConfState::from_voters(std::vec![1u64, 2]),
  );
  let msg = Message::InstallSnapshot(InstallSnapshot::new(Term::new(1), 1u64, meta, data));
  a.write_framed(now, ha, &msg); // blocks after ~one window
  a.service_if_deferred(now);

  let mut got: Option<bytes::Bytes> = None;
  let mut saw_writable = false;
  for _ in 0..300 {
    pump(now, &mut a, &mut b);
    let ready_b = b.take_ready_unique();
    if ready_b.contains(&hb) {
      b.ingest_recv(now, hb);
      if let Ok(Some(f)) = b.next_frame(hb) {
        got = Some(f);
      }
    }
    pump(now, &mut a, &mut b);
    let ready_a = a.take_ready_unique();
    if ready_a.contains(&ha) {
      saw_writable = true;
      a.flush_stream(now, ha);
    }
    if got.is_some() {
      break;
    }
  }
  assert!(
    saw_writable,
    "a formerly-blocked write resumed on a Writable signal"
  );
  let got = got.expect("the backpressured frame fully transferred");
  assert_eq!(
    crate::wire::decode_message::<u64>(got).expect("decodes"),
    msg
  );
}

/// A flush whose write hits a DEAD send stream closes the connection. Resetting our own send half
/// (the connection itself stays up) makes the next `write` error, exercising the terminal
/// write-error close.
#[test]
fn flush_to_a_dead_send_stream_closes() {
  let ca = TestClusterCa::generate();
  let (mut a, mut b) = pair(&ca);
  let now = Instant::now();
  let (ha, _hb) = validated(now, &mut a, &mut b);
  let sid = a.send_sid_for_test(ha).expect("a's send stream");
  a.reset_send_for_test(ha, sid);
  a.stage_outbound_for_test(ha, &[1, 2, 3, 4]);
  a.flush_stream(now, ha); // write → Err → close_local
  assert_eq!(
    a.take_lost(),
    Some(ha),
    "a write to a dead send stream closes the connection"
  );
}

/// A peer STOP_SENDING on our CURRENT consensus send stream means the peer stopped consuming
/// consensus: close the connection.
#[test]
fn peer_stop_on_the_consensus_stream_closes() {
  let ca = TestClusterCa::generate();
  let (mut a, mut b) = pair(&ca);
  let now = Instant::now();
  let (ha, hb) = validated(now, &mut a, &mut b);
  let b_recv = b.recv_sid_for_test(hb).expect("b adopted a's send stream");
  b.stop_recv_for_test(hb, b_recv);
  b.service(now);
  pump(now, &mut a, &mut b);
  assert_eq!(
    a.take_lost(),
    Some(ha),
    "a STOP on the consensus send stream closes the connection"
  );
}

/// A peer STOP_SENDING on the UNUSED half of a peer-opened stream only resets that idle half; the
/// connection stays up.
#[test]
fn peer_stop_on_an_unused_half_only_resets_it() {
  let ca = TestClusterCa::generate();
  let (mut a, mut b) = pair(&ca);
  let now = Instant::now();
  let (ha, hb) = validated(now, &mut a, &mut b);
  let b_send = b.send_sid_for_test(hb).expect("b's own opened stream");
  b.stop_recv_for_test(hb, b_send);
  b.service(now);
  pump(now, &mut a, &mut b);
  assert_eq!(
    a.take_lost(),
    None,
    "a STOP on an unused half does not close the connection"
  );
  assert!(a.has_entry_for_test(ha));
}
