use super::*;
use crate::{
  Config, Message, Term, TimeoutNow,
  testkit::{AsyncStable, CountSm, VecLog},
  transport::{ClusterId, Labeled, Passthrough, labeled::LabelOptions},
};
use core::time::Duration;
use std::collections::BTreeMap;

type TestRecord = Labeled<Passthrough>;
type MultiCoord = MultiStreamCoordinator<u64, u64, CountSm, TestRecord>;

struct Stores {
  map: BTreeMap<u64, (VecLog, AsyncStable)>,
}

impl GroupStores<u64, VecLog, AsyncStable> for Stores {
  fn stores(&mut self, group: &u64) -> Option<(&mut VecLog, &mut AsyncStable)> {
    self.map.get_mut(group).map(|(l, s)| (l, s))
  }
}

fn single_voter(id: u64) -> Config<u64> {
  Config::try_new(
    id,
    std::vec![id],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
}

#[test]
fn coordinator_drives_isolated_groups() {
  let mut coord = MultiStreamCoordinator::<u64, u64, CountSm, TestRecord>::new();
  coord
    .create_group(100, single_voter(1), Instant::ORIGIN, 1, CountSm::default())
    .unwrap();
  coord
    .create_group(200, single_voter(1), Instant::ORIGIN, 1, CountSm::default())
    .unwrap();

  let mut stores = Stores {
    map: BTreeMap::new(),
  };
  stores
    .map
    .insert(100, (VecLog::default(), AsyncStable::default()));
  stores
    .map
    .insert(200, (VecLog::default(), AsyncStable::default()));

  // Drive group 100 to leadership through the coordinator's per-group storage. Group 200 is never
  // touched. (Single-voter groups need no peer connection, so this exercises the coordinator's
  // group threading + store routing without the wire.)
  let d = coord.group(&100).unwrap().poll_timeout().unwrap();
  {
    let (l, s) = stores.stores(&100).unwrap();
    coord.handle_timeout(&100, d, l, s).unwrap(); // campaign
  }
  for _ in 0..2 {
    // First drain: the self-vote becomes durable and the group becomes leader (appending a no-op);
    // second drain: the no-op append completes, so quorum=1 commits and applies it.
    let (l, s) = stores.stores(&100).unwrap();
    coord.handle_storage(&100, d, l, s).unwrap();
  }
  assert!(coord.group(&100).unwrap().role().is_leader());
  assert!(coord.group(&200).unwrap().role().is_follower());

  // Propose a command on group 100 and let quorum=1 commit + apply it.
  let cmd = bytes::Bytes::copy_from_slice(&[7u8]);
  {
    let (l, s) = stores.stores(&100).unwrap();
    coord.submit_propose(&100, d, l, s, &cmd).unwrap().unwrap();
  }
  {
    let (l, s) = stores.stores(&100).unwrap();
    coord.handle_storage(&100, d, l, s).unwrap();
  }
  while let Some((g, _)) = coord.poll_event() {
    assert_eq!(g, 100, "events are stamped with the originating group");
  }
  // Group 100 applied the command; group 200 is pristine.
  assert!(coord.group(&100).unwrap().state_machine().count() >= 1);
  assert_eq!(coord.group(&200).unwrap().state_machine().count(), 0);
}

fn two_voter(id: u64) -> Config<u64> {
  Config::try_new(
    id,
    std::vec![1, 2],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
}

fn label(id: u64, role_dialer: bool) -> TestRecord {
  let mut local_id = Vec::new();
  id.encode(&mut local_id);
  let opts = LabelOptions {
    cluster: ClusterId([1; 16]),
    local_id,
  };
  if role_dialer {
    Labeled::dialer(Passthrough::new(), &opts).unwrap()
  } else {
    Labeled::acceptor(Passthrough::new(), &opts).unwrap()
  }
}

/// A framed `[group header][Message]` payload, exactly as a peer `Conn::send_message` builds it —
/// for hand-feeding a receiver a frame with a chosen (possibly malformed) group tag.
fn crafted_frame(tag: &[u8], msg: &Message<u64>) -> Vec<u8> {
  let mut payload = Vec::new();
  crate::transport::frame::write_group_header(tag, &mut payload);
  crate::wire::encode_message(msg, &mut payload);
  let mut framed = Vec::new();
  crate::transport::frame::encode_frame(&payload, &mut framed);
  framed
}

/// A two-node multi-group world: node 1 (`a`) dials node 2 (`b`); ONE connection (id 1 on both
/// sides) carries every co-located group's traffic. Only `a`'s timers are ever fired, so
/// elections are deterministic (`b` grants and never campaigns).
struct World {
  a: MultiCoord,
  b: MultiCoord,
  sa: Stores,
  sb: Stores,
  now: Instant,
}

impl World {
  fn new(a_groups: &[u64], b_groups: &[u64]) -> Self {
    let mut a = MultiCoord::new();
    let mut b = MultiCoord::new();
    let mut sa = Stores {
      map: BTreeMap::new(),
    };
    let mut sb = Stores {
      map: BTreeMap::new(),
    };
    for &g in a_groups {
      a.create_group(g, two_voter(1), Instant::ORIGIN, 1, CountSm::default())
        .unwrap();
      sa.map
        .insert(g, (VecLog::default(), AsyncStable::default()));
    }
    for &g in b_groups {
      b.create_group(g, two_voter(2), Instant::ORIGIN, 2, CountSm::default())
        .unwrap();
      sb.map
        .insert(g, (VecLog::default(), AsyncStable::default()));
    }
    let ca = a.on_conn_open(label(1, true), Instant::ORIGIN);
    let cb = b.on_conn_open(label(2, false), Instant::ORIGIN);
    assert_eq!(ca, cb, "first allocation on both sides");
    World {
      a,
      b,
      sa,
      sb,
      now: Instant::ORIGIN,
    }
  }

  /// Move all queued bytes across the wire, draining every hosted group's storage on both sides,
  /// until quiescent.
  fn settle(&mut self) {
    for _ in 0..200 {
      for g in [100u64, 200] {
        if self.a.group(&g).is_some() {
          let (l, s) = self.sa.stores(&g).unwrap();
          let _ = self.a.handle_storage(&g, self.now, l, s);
        }
        if self.b.group(&g).is_some() {
          let (l, s) = self.sb.stores(&g).unwrap();
          let _ = self.b.handle_storage(&g, self.now, l, s);
        }
      }
      let from_a = self.a.poll_transmit();
      let from_b = self.b.poll_transmit();
      let mut moved = false;
      for (_, bytes) in &from_a {
        if !bytes.is_empty() {
          self
            .b
            .handle_conn_data(ConnId(1), bytes, false, self.now, &mut self.sb);
          moved = true;
        }
      }
      for (_, bytes) in &from_b {
        if !bytes.is_empty() {
          self
            .a
            .handle_conn_data(ConnId(1), bytes, false, self.now, &mut self.sa);
          moved = true;
        }
      }
      if !moved {
        break;
      }
    }
  }

  /// Fire `group`'s timers on `a` at (or after) that group's own deadline, then settle.
  fn fire_a(&mut self, group: u64) {
    let d = self.a.group(&group).unwrap().poll_timeout().unwrap();
    self.now = self.now.max(d);
    let now = self.now;
    let (l, s) = self.sa.stores(&group).unwrap();
    self.a.handle_timeout(&group, now, l, s).unwrap();
    self.settle();
  }

  /// Drive `a`'s `group` to leadership by firing ONLY its timers.
  fn elect_a(&mut self, group: u64) {
    for _ in 0..40 {
      if self.a.group(&group).unwrap().role().is_leader() {
        return;
      }
      self.fire_a(group);
    }
    panic!("group {group} did not elect a leader");
  }
}

/// Two co-located groups drive elections over the SAME connection: the group tag demuxes each
/// frame to the owning endpoint on the receiver, and the other group stays pristine.
#[test]
fn demuxes_two_groups_over_one_connection() {
  let mut w = World::new(&[100, 200], &[100, 200]);
  w.settle(); // complete the label handshake
  assert_eq!(w.a.conn_of(&2), Some(ConnId(1)), "node 1 bound peer 2");
  assert_eq!(w.b.conn_of(&1), Some(ConnId(1)), "node 2 bound peer 1");

  w.elect_a(100);
  assert!(
    w.b.group(&100).unwrap().term() >= Term::new(1),
    "b's group 100 heard the election through the wire"
  );
  assert_eq!(
    w.b.group(&200).unwrap().term(),
    Term::ZERO,
    "b's group 200 is pristine — the tag isolated the traffic"
  );
  assert_eq!(
    w.a.conn_of(&2),
    Some(ConnId(1)),
    "still the same connection"
  );

  w.elect_a(200);
  assert!(
    w.b.group(&200).unwrap().term() >= Term::new(1),
    "group 200 demuxed over the SAME connection"
  );
  assert_eq!(w.a.conn_of(&2), Some(ConnId(1)));
  assert_eq!(w.b.conn_of(&1), Some(ConnId(1)));
  assert_eq!(
    w.b.poll_conn_closed(),
    None,
    "the shared connection never closed"
  );
}

/// A well-formed tag for a group the receiver does not host: the frame is dropped but the shared
/// connection SURVIVES — one unhosted group must not sever the link for the hosted ones.
#[test]
fn unhosted_group_frames_drop_but_connection_survives() {
  let mut w = World::new(&[100, 200], &[100]);
  w.settle();
  assert_eq!(w.b.conn_of(&1), Some(ConnId(1)));

  // Fire group 200's election on a: b hosts no group 200, so every frame is dropped on arrival.
  w.fire_a(200);
  w.fire_a(200); // a retry round changes nothing
  assert_eq!(w.b.poll_conn_closed(), None, "the connection stays alive");
  assert_eq!(w.b.conn_of(&1), Some(ConnId(1)));
  assert_eq!(
    w.b.group(&100).unwrap().term(),
    Term::ZERO,
    "the dropped frames leaked into no hosted group"
  );
  assert!(
    !w.a.group(&200).unwrap().role().is_leader(),
    "no quorum without b"
  );

  // The hosted group still flows over the same (surviving) connection.
  w.elect_a(100);
  assert!(w.b.group(&100).unwrap().term() >= Term::new(1));
  assert_eq!(w.b.poll_conn_closed(), None);
}

/// A well-framed frame whose group tag does not decode as the host's `G` (a u64 id is exactly 8
/// bytes) is a systematic peer fault: the receiver closes the connection as integrity-suspect.
#[test]
fn undecodable_tag_closes_the_connection() {
  let mut w = World::new(&[100], &[100]);
  w.settle();
  assert_eq!(
    w.b.conn_of(&1),
    Some(ConnId(1)),
    "validated before the fault"
  );

  let msg = Message::TimeoutNow(TimeoutNow::new(Term::new(3), 1u64));
  let framed = crafted_frame(&[1, 2, 3], &msg); // 3 bytes can never decode as a u64 group id
  w.b
    .handle_conn_data(ConnId(1), &framed, false, w.now, &mut w.sb);
  assert_eq!(
    w.b.poll_conn_closed(),
    Some((ConnId(1), Some(TransportError::Decode))),
    "the malformed tag closes the connection as integrity-suspect"
  );
  assert_eq!(w.b.conn_of(&1), None, "the route is gone");
}

/// An EMPTY tag (a single-group peer) arriving at a multi-group host: `u64::decode_exact` of zero
/// bytes fails, so the deployment-shape mismatch closes the connection rather than guessing a
/// target group.
#[test]
fn single_group_peer_empty_tag_closes_on_a_multi_host() {
  let mut w = World::new(&[100], &[100]);
  w.settle();
  assert_eq!(w.b.conn_of(&1), Some(ConnId(1)));

  let msg = Message::TimeoutNow(TimeoutNow::new(Term::new(3), 1u64));
  let framed = crafted_frame(&[], &msg); // the single-group (empty) tag
  w.b
    .handle_conn_data(ConnId(1), &framed, false, w.now, &mut w.sb);
  assert_eq!(
    w.b.poll_conn_closed(),
    Some((ConnId(1), Some(TransportError::Decode)))
  );
  assert_eq!(w.b.conn_of(&1), None);
}

/// Split a transmit drain's concatenated `[u32 len][payload]` frames back into payloads.
fn frames_of(bytes: &[u8]) -> Vec<Vec<u8>> {
  let mut frames = Vec::new();
  let mut at = 0usize;
  while at < bytes.len() {
    let len = u32::from_be_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
    at += 4;
    frames.push(bytes[at..at + len].to_vec());
    at += len;
  }
  frames
}

/// Every frame of one transmit drain, across its `(conn, bytes)` pairs.
fn transmit_frames(out: &[(ConnId, Vec<u8>)]) -> Vec<Vec<u8>> {
  let mut all = Vec::new();
  for (_, bytes) in out {
    all.extend(frames_of(bytes));
  }
  all
}

/// The `(flags, group, message)` entries of a coalesced payload, with the tags decoded as `u64`.
fn decode_entries(payload: &[u8]) -> Vec<(u8, u64, Message<u64>)> {
  crate::transport::frame::split_coalesced(bytes::Bytes::copy_from_slice(payload))
    .expect("well-formed coalesced payload")
    .into_iter()
    .map(|(flags, group, msg)| {
      (
        flags,
        u64::decode_exact(group).expect("u64 tag"),
        crate::wire::decode_message::<u64>(msg).expect("valid message"),
      )
    })
    .collect()
}

/// Both co-located groups' heartbeats to the shared peer leave in ONE coalesced frame per crank
/// (the poll_transmit chokepoint batches across the per-group timeout calls), and the peer's two
/// responses come back the same way — all beats deliver on both sides.
#[test]
fn heartbeats_coalesce_into_one_frame_per_peer() {
  let mut w = World::new(&[100, 200], &[100, 200]);
  w.settle();
  w.elect_a(100);
  w.elect_a(200);

  // Fire BOTH leaders' heartbeat timers in one crank, then drain the wire once.
  let d100 = w.a.group(&100).unwrap().poll_timeout().unwrap();
  let d200 = w.a.group(&200).unwrap().poll_timeout().unwrap();
  w.now = w.now.max(d100).max(d200);
  let now = w.now;
  {
    let (l, s) = w.sa.stores(&100).unwrap();
    w.a.handle_timeout(&100, now, l, s).unwrap();
  }
  {
    let (l, s) = w.sa.stores(&200).unwrap();
    w.a.handle_timeout(&200, now, l, s).unwrap();
  }
  let out = w.a.poll_transmit();
  let frames = transmit_frames(&out);
  assert_eq!(frames.len(), 1, "one physical frame carries both beats");
  assert!(crate::transport::frame::is_coalesced_frame(&frames[0]));
  let entries = decode_entries(&frames[0]);
  let groups: Vec<u64> = entries.iter().map(|(_, g, _)| *g).collect();
  assert_eq!(groups, std::vec![100, 200], "both groups' beats, one frame");
  assert!(
    entries.iter().all(|(f, _, m)| *f == 0 && m.is_heartbeat()),
    "unflagged heartbeats"
  );

  // Deliver to b: both beats dispatch (no close), and b's two responses ride ONE coalesced frame.
  for (_, bytes) in &out {
    w.b
      .handle_conn_data(ConnId(1), bytes, false, w.now, &mut w.sb);
  }
  assert_eq!(w.b.poll_conn_closed(), None);
  let back = w.b.poll_transmit();
  let frames = transmit_frames(&back);
  assert_eq!(frames.len(), 1, "one frame back");
  let entries = decode_entries(&frames[0]);
  assert_eq!(entries.len(), 2, "both groups' responses");
  assert!(
    entries
      .iter()
      .all(|(f, _, m)| *f == 0 && m.is_heartbeat_response()),
    "unflagged heartbeat responses"
  );
}

/// A LONE unflagged heartbeat keeps the plain single-message frame — no format change for the
/// trivial case.
#[test]
fn a_single_heartbeat_stays_a_plain_frame() {
  let mut w = World::new(&[100, 200], &[100, 200]);
  w.settle();
  w.elect_a(100);

  let d = w.a.group(&100).unwrap().poll_timeout().unwrap();
  w.now = w.now.max(d);
  let now = w.now;
  {
    let (l, s) = w.sa.stores(&100).unwrap();
    w.a.handle_timeout(&100, now, l, s).unwrap();
  }
  let frames = transmit_frames(&w.a.poll_transmit());
  assert_eq!(frames.len(), 1);
  assert!(
    !crate::transport::frame::is_coalesced_frame(&frames[0]),
    "a batch of one unflagged beat is a normal frame"
  );
}

/// The receive side enforces what the send side promises: a quiesce flag on anything but a
/// Heartbeat is a protocol violation (a buggy or stale-version peer) and closes the connection —
/// honoring it would freeze the group on a message class that deliberately emits no `Wake`.
#[test]
fn flagged_response_closes_the_connection() {
  let mut w = World::new(&[100], &[100]);
  w.settle();
  assert_eq!(w.b.conn_of(&1), Some(ConnId(1)));

  // A crafted 1-entry coalesced frame: a QUIESCE-flagged HeartbeatResponse.
  let hbr = Message::HeartbeatResponse(crate::HeartbeatResponse::new(
    Term::new(1),
    1u64,
    bytes::Bytes::new(),
  ));
  let mut msg_bytes = Vec::new();
  crate::wire::encode_message(&hbr, &mut msg_bytes);
  let mut gb = Vec::new();
  sailing_encode_u64(100, &mut gb);
  let mut payload = Vec::new();
  crate::transport::frame::write_coalesced_marker(&mut payload);
  crate::transport::frame::write_coalesced_entry(1, &gb, &msg_bytes, &mut payload);
  let mut framed = Vec::new();
  crate::transport::frame::encode_frame(&payload, &mut framed);

  w.b
    .handle_conn_data(ConnId(1), &framed, false, w.now, &mut w.sb);
  assert_eq!(
    w.b.poll_conn_closed(),
    Some((ConnId(1), Some(TransportError::Decode))),
    "a flagged non-heartbeat is integrity-suspect"
  );
  let mut controls = Vec::new();
  while let Some(c) = w.b.poll_group_control() {
    controls.push(c);
  }
  assert!(
    !controls.contains(&(100, GroupControl::Quiesce)),
    "the violating flag is never honored"
  );
}

/// `Data`-encode a `u64` group id (the test-side mirror of the coordinators' tag stamping).
fn sailing_encode_u64(id: u64, out: &mut Vec<u8>) {
  use crate::Data as _;
  id.encode(out);
}

/// Build a 1-entry coalesced frame carrying a QUIESCE-flagged Heartbeat for group 100.
fn flagged_beat_frame(term: u64, leader: u64) -> Vec<u8> {
  let hb = Message::Heartbeat(crate::Heartbeat::new(
    Term::new(term),
    leader,
    Index::ZERO,
    bytes::Bytes::new(),
  ));
  let mut msg_bytes = Vec::new();
  crate::wire::encode_message(&hb, &mut msg_bytes);
  let mut gb = Vec::new();
  sailing_encode_u64(100, &mut gb);
  let mut payload = Vec::new();
  crate::transport::frame::write_coalesced_marker(&mut payload);
  crate::transport::frame::write_coalesced_entry(1, &gb, &msg_bytes, &mut payload);
  let mut framed = Vec::new();
  crate::transport::frame::encode_frame(&payload, &mut framed);
  framed
}

/// A quiesce flag is honored only when the CORE accepted the beat as current-leader contact: a
/// STALE-term flagged beat (the core rejects it) and a flagged beat whose payload names a node
/// other than the authenticated transport peer (the core's sender-authenticity drop) must both
/// strip the flag — freezing timers on a rejected input's say-so would quiesce a live group.
#[test]
fn rejected_flagged_beats_never_quiesce() {
  let mut w = World::new(&[100], &[100]);
  w.settle();
  w.elect_a(100);
  while w.b.poll_group_control().is_some() {}
  let live_term = w.b.group(&100).unwrap().term().get();
  assert!(live_term >= 1);

  // (a) A stale-term flagged beat from the legitimate leader's connection.
  let stale = flagged_beat_frame(0, 1);
  w.b
    .handle_conn_data(ConnId(1), &stale, false, w.now, &mut w.sb);
  // (b) A current-term flagged beat whose payload names node 3, arriving over node 1's conn.
  let spoofed = flagged_beat_frame(live_term, 3);
  w.b
    .handle_conn_data(ConnId(1), &spoofed, false, w.now, &mut w.sb);

  let mut controls = Vec::new();
  while let Some(c) = w.b.poll_group_control() {
    controls.push(c);
  }
  assert!(
    !controls.contains(&(100, GroupControl::Quiesce)),
    "neither rejected beat quiesces: {controls:?}"
  );
  assert_eq!(
    w.b.poll_conn_closed(),
    None,
    "valid heartbeat kinds do not close"
  );

  // A genuine flagged beat from the live leader still quiesces (the gate passes real traffic).
  w.a.mark_quiescing(&100);
  w.fire_a(100);
  let mut controls = Vec::new();
  while let Some(c) = w.b.poll_group_control() {
    controls.push(c);
  }
  assert!(
    controls.contains(&(100, GroupControl::Quiesce)),
    "the accepted flagged beat quiesces: {controls:?}"
  );
}

/// Heartbeat entries are BOUNDED BY CONSTRUCTION: the wire heartbeat context is the read
/// machinery's internal 8-byte round token, never the caller's context (which rides the
/// non-coalesced ReadIndex/ReadIndexResponse pair) — so even a read with a huge caller context
/// produces only small, budget-conforming coalesced frames, the peer accepts everything, and the
/// coordinators' oversized-entry divert stays pure defense-in-depth.
#[test]
fn huge_caller_context_never_inflates_the_wire_heartbeat() {
  let mut w = World::new(&[100, 200], &[100, 200]);
  w.settle();
  w.elect_a(100);
  w.elect_a(200);
  let big = bytes::Bytes::from(std::vec![0xCD; 70 * 1024]);
  {
    let (l, s) = w.sa.stores(&100).unwrap();
    w.a.read_index(&100, w.now, l, s, big).unwrap().unwrap();
  }
  // Fire group 200's beat by hand (`fire_a` would settle and deliver the frames before they can
  // be inspected) so the drain batches beats for the one peer.
  let d = w.a.group(&200).unwrap().poll_timeout().unwrap();
  w.now = w.now.max(d);
  let now = w.now;
  {
    let (l, s) = w.sa.stores(&200).unwrap();
    w.a.handle_timeout(&200, now, l, s).unwrap();
  }
  let out = w.a.poll_transmit();
  let frames = transmit_frames(&out);
  assert!(!frames.is_empty());
  for f in &frames {
    assert!(
      f.len() <= 2 + crate::transport::frame::COALESCED_FRAME_BUDGET,
      "every frame stays small: the 70 KiB caller context never reaches the wire heartbeat"
    );
  }
  for (_, bytes) in &out {
    w.b
      .handle_conn_data(ConnId(1), bytes, false, w.now, &mut w.sb);
  }
  assert_eq!(w.b.poll_conn_closed(), None, "the peer accepted everything");
}

/// The quiesce flag rides ONLY a leader's own Heartbeat broadcast: an intent marked on a
/// FOLLOWER (or a leader deposed before its next beat) must never leak onto the
/// HeartbeatResponses it keeps sending — a flagged response would freeze the very leader that
/// never chose to quiesce. The intent stays pending (a follower never broadcasts) until
/// explicitly cancelled, the un-quiesce path's job.
#[test]
fn stale_intent_never_rides_a_response() {
  let mut w = World::new(&[100, 200], &[100, 200]);
  w.settle();
  w.elect_a(100);
  while w.a.poll_group_control().is_some() {}
  while w.b.poll_group_control().is_some() {}

  // B is group 100's FOLLOWER; a (stale) intent on B can only ever meet HeartbeatResponses.
  w.b.mark_quiescing(&100);
  w.fire_a(100); // A beats; B responds
  w.settle();

  let mut a_controls = Vec::new();
  while let Some(c) = w.a.poll_group_control() {
    a_controls.push(c);
  }
  assert!(
    !a_controls.contains(&(100, GroupControl::Quiesce)),
    "the follower's responses carried no quiesce flag"
  );
  assert!(
    w.b.is_quiescing(&100),
    "the intent is NOT consumed by a response — only a leader broadcast stamps"
  );

  // The un-quiesce path cancels it, so no later beat can carry the stale promise.
  w.b.cancel_quiescing(&100);
  assert!(!w.b.is_quiescing(&100));
}

/// `mark_quiescing` stamps the group's next heartbeat with the QUIESCE flag (a flagged single
/// rides a one-entry coalesced frame — only a coalesced entry has a flags byte), the intent is
/// consumed by that broadcast, and the receiver surfaces exactly one `GroupControl::Quiesce` for
/// the group — ordered AFTER the beat's own `Wake`, so folding controls in order nets quiesced.
/// The responding `HeartbeatResponse` back at the leader surfaces NO `Wake` (a quiescing leader's
/// final beat draws responses; waking on them would never let the quiesce settle).
#[test]
fn quiesce_flag_round_trips_as_group_control() {
  let mut w = World::new(&[100, 200], &[100, 200]);
  w.settle();
  w.elect_a(100);
  while w.a.poll_group_control().is_some() {} // drop the election-era controls
  while w.b.poll_group_control().is_some() {}

  w.a.mark_quiescing(&100);
  assert!(w.a.is_quiescing(&100));
  let d = w.a.group(&100).unwrap().poll_timeout().unwrap();
  w.now = w.now.max(d);
  let now = w.now;
  {
    let (l, s) = w.sa.stores(&100).unwrap();
    w.a.handle_timeout(&100, now, l, s).unwrap();
  }
  assert!(
    !w.a.is_quiescing(&100),
    "the intent is consumed by the stamped broadcast"
  );
  let out = w.a.poll_transmit();
  let frames = transmit_frames(&out);
  assert_eq!(frames.len(), 1);
  let entries = decode_entries(&frames[0]);
  assert_eq!(
    entries.len(),
    1,
    "a flagged single is a 1-entry coalesced frame"
  );
  assert_eq!(entries[0].0, 1, "bit0 = QUIESCE");
  assert_eq!(entries[0].1, 100);

  for (_, bytes) in &out {
    w.b
      .handle_conn_data(ConnId(1), bytes, false, w.now, &mut w.sb);
  }
  let mut controls = Vec::new();
  while let Some(c) = w.b.poll_group_control() {
    controls.push(c);
  }
  assert_eq!(
    controls,
    std::vec![(100, GroupControl::Wake), (100, GroupControl::Quiesce)],
    "the beat wakes, then the flag quiesces — net quiesced"
  );

  // The response flows back to the leader WITHOUT waking group 100 there.
  let back = w.b.poll_transmit();
  for (_, bytes) in &back {
    w.a
      .handle_conn_data(ConnId(1), bytes, false, w.now, &mut w.sa);
  }
  assert_eq!(
    w.a.poll_group_control(),
    None,
    "a HeartbeatResponse is absorbed: no Wake for the quiesced leader"
  );

  // The round's remaining tail (the leader's empty-append probe and its response) dies out with
  // no wake on EITHER side — the flagged round settles instead of ping-ponging the pair awake.
  w.settle();
  assert_eq!(w.a.poll_group_control(), None, "the leader stays settled");
  assert_eq!(
    w.b.poll_group_control(),
    None,
    "the follower stays settled after the round's tail"
  );
}

/// Heartbeats coalesce while an `AppendEntries` in the same crank keeps its own frame — and both
/// deliver: the proposal commits and applies on both sides after settling.
#[test]
fn mixed_traffic_keeps_appends_in_their_own_frames() {
  let mut w = World::new(&[100, 200], &[100, 200]);
  w.settle();
  w.elect_a(100);
  w.elect_a(200);

  let d100 = w.a.group(&100).unwrap().poll_timeout().unwrap();
  let d200 = w.a.group(&200).unwrap().poll_timeout().unwrap();
  w.now = w.now.max(d100).max(d200);
  let now = w.now;
  let cmd = bytes::Bytes::from_static(b"x");
  {
    let (l, s) = w.sa.stores(&100).unwrap();
    w.a.submit_propose(&100, now, l, s, &cmd).unwrap().unwrap();
  }
  {
    let (l, s) = w.sa.stores(&100).unwrap();
    w.a.handle_timeout(&100, now, l, s).unwrap();
  }
  {
    let (l, s) = w.sa.stores(&200).unwrap();
    w.a.handle_timeout(&200, now, l, s).unwrap();
  }
  let out = w.a.poll_transmit();
  let frames = transmit_frames(&out);
  let (coalesced, plain): (Vec<_>, Vec<_>) = frames
    .iter()
    .partition(|f| crate::transport::frame::is_coalesced_frame(f));
  assert_eq!(coalesced.len(), 1, "the two beats share one frame");
  assert_eq!(decode_entries(coalesced[0]).len(), 2);
  assert!(
    !plain.is_empty(),
    "the AppendEntries keeps its own frame(s)"
  );

  for (_, bytes) in &out {
    w.b
      .handle_conn_data(ConnId(1), bytes, false, w.now, &mut w.sb);
  }
  assert_eq!(w.b.poll_conn_closed(), None);
  w.settle();
  assert_eq!(
    w.a.group(&100).unwrap().state_machine().count(),
    1,
    "the proposal committed and applied through the mixed drain"
  );
  assert_eq!(w.b.group(&100).unwrap().state_machine().count(), 1);
}

/// A coalesced entry for a group the receiver does not host is dropped ENTRY by entry: the other
/// entries still dispatch and the shared connection survives.
#[test]
fn unhosted_coalesced_entry_drops_but_the_frame_delivers() {
  let mut w = World::new(&[100], &[100]);
  w.settle();
  w.elect_a(100);
  while w.b.poll_group_control().is_some() {}

  // Craft a two-entry coalesced frame: a beat for hosted group 100 and one for unhosted 999.
  let term = w.b.group(&100).unwrap().term();
  let hb = |gid: u64| {
    let mut tag = Vec::new();
    gid.encode(&mut tag);
    let msg = Message::Heartbeat(crate::message::Heartbeat::new(
      term,
      1u64,
      crate::Index::new(0),
      bytes::Bytes::new(),
    ));
    let mut msg_bytes = Vec::new();
    crate::wire::encode_message(&msg, &mut msg_bytes);
    (tag, msg_bytes)
  };
  let mut payload = Vec::new();
  crate::transport::frame::write_coalesced_marker(&mut payload);
  let (tag, msg_bytes) = hb(999);
  crate::transport::frame::write_coalesced_entry(0, &tag, &msg_bytes, &mut payload);
  let (tag, msg_bytes) = hb(100);
  crate::transport::frame::write_coalesced_entry(0, &tag, &msg_bytes, &mut payload);
  let mut framed = Vec::new();
  crate::transport::frame::encode_frame(&payload, &mut framed);

  w.b
    .handle_conn_data(ConnId(1), &framed, false, w.now, &mut w.sb);
  assert_eq!(
    w.b.poll_conn_closed(),
    None,
    "an unhosted entry never costs the connection"
  );
  let mut controls = Vec::new();
  while let Some(c) = w.b.poll_group_control() {
    controls.push(c);
  }
  assert_eq!(
    controls,
    std::vec![(100, GroupControl::Wake)],
    "the hosted entry dispatched; the unhosted one dropped silently"
  );
}
