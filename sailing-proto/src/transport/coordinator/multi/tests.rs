use super::*;
use crate::{
  Config, FloorStore, GroupEngine, MERGED_FLOOR, Message, NoFloors, SplitError, Term, TimeoutNow,
  testkit::{AsyncStable, CountSm, VecLog},
  transport::{ClusterId, Labeled, Passthrough, labeled::LabelOptions},
};
use bytes::Bytes;
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
    .create_group(
      100,
      single_voter(1),
      Instant::ORIGIN,
      1,
      CountSm::default(),
      0,
      &NoFloors,
    )
    .unwrap();
  coord
    .create_group(
      200,
      single_voter(1),
      Instant::ORIGIN,
      1,
      CountSm::default(),
      0,
      &NoFloors,
    )
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
      a.create_group(
        g,
        two_voter(1),
        Instant::ORIGIN,
        1,
        CountSm::default(),
        0,
        &NoFloors,
      )
      .unwrap();
      sa.map
        .insert(g, (VecLog::default(), AsyncStable::default()));
    }
    for &g in b_groups {
      b.create_group(
        g,
        two_voter(2),
        Instant::ORIGIN,
        2,
        CountSm::default(),
        0,
        &NoFloors,
      )
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

  // The round has no further tail: the gated heartbeat-response pump sends nothing to a
  // caught-up, replicating responder, so the flagged round is exactly the beat + its absorbed
  // response and both sides settle.
  w.settle();
  assert_eq!(w.a.poll_group_control(), None, "the leader stays settled");
  assert_eq!(
    w.b.poll_group_control(),
    None,
    "the follower stays settled after the round's tail"
  );
}

/// With the heartbeat-response append pump gated (an idle round has no empty-append tail) and
/// quiesce eligibility excluding probing peers, the absorb set shrinks to exactly
/// `HeartbeatResponse`: a delivered empty `AppendEntries` or `AppendResponse` is WAKE-class —
/// the conservative direction, a spurious pair costs one wake instead of riding the absorb
/// trust surface.
#[test]
fn empty_append_and_append_response_are_wake_class() {
  let mut w = World::new(&[100], &[100]);
  w.settle();
  w.elect_a(100);
  while w.a.poll_group_control().is_some() {}
  while w.b.poll_group_control().is_some() {}
  let term = w.b.group(&100).unwrap().term();
  let commit = w.b.group(&100).unwrap().commit_index();
  let mut gb = Vec::new();
  sailing_encode_u64(100, &mut gb);

  // An empty AppendEntries delivered to the follower wakes its group.
  let empty_ae = Message::AppendEntries(crate::AppendEntries::new(
    term,
    1u64,
    Index::ZERO,
    Term::ZERO,
    Vec::new(),
    commit,
  ));
  let frame = crafted_frame(&gb, &empty_ae);
  w.b
    .handle_conn_data(ConnId(1), &frame, false, w.now, &mut w.sb);
  assert_eq!(
    w.b.poll_group_control(),
    Some((100, GroupControl::Wake)),
    "an empty AppendEntries is WAKE-class"
  );

  // An AppendResponse delivered to the leader wakes its group.
  let ack = Message::AppendResponse(crate::AppendResponse::new(
    term,
    2u64,
    false,
    Index::ZERO,
    Term::ZERO,
    commit,
  ));
  let frame = crafted_frame(&gb, &ack);
  w.a
    .handle_conn_data(ConnId(1), &frame, false, w.now, &mut w.sa);
  assert_eq!(
    w.a.poll_group_control(),
    Some((100, GroupControl::Wake)),
    "an AppendResponse is WAKE-class"
  );

  // The heartbeat response stays the sole absorbed kind.
  let hbr = Message::HeartbeatResponse(crate::HeartbeatResponse::new(
    term,
    2u64,
    bytes::Bytes::new(),
  ));
  let frame = crafted_frame(&gb, &hbr);
  w.a
    .handle_conn_data(ConnId(1), &frame, false, w.now, &mut w.sa);
  assert_eq!(
    w.a.poll_group_control(),
    None,
    "a HeartbeatResponse stays absorbed"
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

/// Removal TOMBSTONES the group id at the coordinator: straggler frames for it drop SILENTLY —
/// no close (the shared connection carries the live groups' traffic), no control — and BOTH
/// admission paths refuse the id with `Retired` until an explicit `clear_tombstone` consents to
/// re-admission (the references' tombstone-refuses-creation rule). The supported rejoin is the
/// two deliberate acts — clear, then create — after which traffic reaches the fresh replica
/// again.
#[test]
fn tombstoned_group_refuses_recreation_until_cleared() {
  let mut w = World::new(&[100, 200], &[100, 200]);
  w.settle();
  w.elect_a(100);
  while w.b.poll_group_control().is_some() {}

  assert!(!w.b.is_retired(&100), "a hosted group is not tombstoned");
  assert!(w.b.remove_group(&100).is_some());
  assert!(w.b.is_retired(&100), "removal tombstones the id");

  // The unaware leader keeps beating group 100; b's tombstone absorbs every frame silently.
  w.fire_a(100);
  w.fire_a(100);
  assert_eq!(
    w.b.poll_conn_closed(),
    None,
    "no close on a tombstoned straggler"
  );
  assert_eq!(
    w.b.poll_group_control(),
    None,
    "no control for a tombstoned group"
  );
  assert!(w.b.group(&100).is_none(), "the group stays removed");

  // The co-located group is untouched by the sibling's tombstone.
  w.elect_a(200);
  assert!(w.b.group(&200).unwrap().term() >= Term::new(1));

  // A tombstoned id REFUSES both admission paths: a stale unknown-group advisory replayed into
  // a naive create can never resurrect the id — only an explicit clear consents.
  assert_eq!(
    w.b
      .create_group(
        100,
        two_voter(2),
        w.now,
        2,
        CountSm::default(),
        0,
        &NoFloors
      )
      .unwrap_err(),
    CreateGroupError::Retired,
    "create refuses a tombstoned id"
  );
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  assert_eq!(
    w.b
      .restore_group(
        100,
        two_voter(2),
        w.now,
        2,
        CountSm::default(),
        1,
        0,
        &NoFloors,
        &mut log,
        &mut stable,
      )
      .unwrap_err(),
    CreateGroupError::Retired,
    "restore is gated exactly like create"
  );
  assert!(w.b.is_retired(&100), "a refused admission lifts nothing");

  // The legitimate rejoin is the two deliberate acts — clear, then create. The fresh replica
  // (fresh stores, as a driver would provision) hears the still-beating leader again.
  assert!(w.b.clear_tombstone(&100), "a tombstone existed");
  assert!(
    !w.b.clear_tombstone(&100),
    "the second clear reports no tombstone left"
  );
  assert!(
    !w.b.is_retired(&100),
    "the explicit clear lifts the tombstone"
  );
  w.sb
    .map
    .insert(100, (VecLog::default(), AsyncStable::default()));
  w.b
    .create_group(
      100,
      two_voter(2),
      w.now,
      2,
      CountSm::default(),
      0,
      &NoFloors,
    )
    .unwrap();
  w.fire_a(100);
  assert!(
    w.b.group(&100).unwrap().term() >= Term::new(1),
    "traffic flows to the re-created group"
  );
}

/// The M2 spec's 5-cell admission matrix, walked directly against coordinator admission with an
/// in-memory seam: the durable fence is consulted first, the volatile consent gate still applies
/// at every gen, and a NoFloors world is P5 verbatim.
#[test]
fn admission_checks_floor_first_then_consent_then_existence() {
  struct Floors(u64, u64);
  impl FloorStore<u64> for Floors {
    fn floor(&self, _: &u64) -> u64 {
      self.0
    }

    fn lineage(&self, _: &u64) -> u64 {
      self.1
    }
  }
  let mut c = MultiCoord::new();
  let now = Instant::ORIGIN;
  // cell 1: below the floor — terminal, consent cannot cure
  let e = c
    .create_group(
      100,
      single_voter(1),
      now,
      1,
      CountSm::default(),
      1,
      &Floors(2, 0),
    )
    .unwrap_err();
  assert!(matches!(e, CreateGroupError::BelowFloor { floor: 2 }));
  // cell 4: at the floor — admitted
  c.create_group(
    100,
    single_voter(1),
    now,
    1,
    CountSm::default(),
    2,
    &Floors(2, 0),
  )
  .expect("at-floor admitted");
  // cell 1 under floor-first ordering: hosted + below-floor reports the fence, not Exists
  let e = c
    .create_group(
      100,
      single_voter(1),
      now,
      1,
      CountSm::default(),
      1,
      &Floors(2, 0),
    )
    .unwrap_err();
  assert!(
    matches!(e, CreateGroupError::BelowFloor { .. }),
    "floor precedes existence"
  );
  // cell 3: hosted at a passing gen
  let e = c
    .create_group(
      100,
      single_voter(1),
      now,
      1,
      CountSm::default(),
      2,
      &Floors(2, 0),
    )
    .unwrap_err();
  assert!(matches!(e, CreateGroupError::Exists));
  // cell 2 (the subtlest): tombstoned + HIGHER gen → Retired (consent gate holds at any gen)
  assert!(c.remove_group(&100).is_some());
  let e = c
    .create_group(
      100,
      single_voter(1),
      now,
      1,
      CountSm::default(),
      9,
      &Floors(2, 0),
    )
    .unwrap_err();
  assert!(matches!(e, CreateGroupError::Retired));
  assert!(c.clear_tombstone(&100));
  c.create_group(
    100,
    single_voter(1),
    now,
    1,
    CountSm::default(),
    9,
    &Floors(2, 0),
  )
  .expect("two-act rejoin");
  // cell 5: NoFloors = the P5 world
  let mut p5 = MultiCoord::new();
  p5.create_group(7, single_voter(1), now, 1, CountSm::default(), 0, &NoFloors)
    .expect("gen-0 verbatim");
}

/// `restore_group` walks the same floor-first gate as `create_group` — the catalog-supplied
/// incarnation is what passes or fails the durable fence — and `MERGED_FLOOR` refuses EVERY
/// generation: the reserved `u64::MAX` sentinel is not numerically below the fence, but it is
/// never a working incarnation, so admission refuses it too.
#[test]
fn restore_admission_walks_the_same_floor_gate() {
  struct Floors(u64);
  impl FloorStore<u64> for Floors {
    fn floor(&self, _: &u64) -> u64 {
      self.0
    }

    fn lineage(&self, _: &u64) -> u64 {
      0
    }
  }
  let mut c = MultiCoord::new();
  let now = Instant::ORIGIN;
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  let e = c
    .restore_group(
      100,
      single_voter(1),
      now,
      1,
      CountSm::default(),
      1,
      1,
      &Floors(2),
      &mut log,
      &mut stable,
    )
    .unwrap_err();
  assert!(matches!(e, CreateGroupError::BelowFloor { floor: 2 }));
  let e = c
    .restore_group(
      100,
      single_voter(1),
      now,
      1,
      CountSm::default(),
      1,
      u64::MAX - 1,
      &Floors(MERGED_FLOOR),
      &mut log,
      &mut stable,
    )
    .unwrap_err();
  assert!(matches!(
    e,
    CreateGroupError::BelowFloor { floor: u64::MAX }
  ));
  // The terminal fence refuses its OWN generation too: `u64::MAX` is the one value the
  // `generation < floor` leg alone would wave through, and it must not be.
  let e = c
    .restore_group(
      100,
      single_voter(1),
      now,
      1,
      CountSm::default(),
      1,
      u64::MAX,
      &Floors(MERGED_FLOOR),
      &mut log,
      &mut stable,
    )
    .unwrap_err();
  assert!(matches!(
    e,
    CreateGroupError::BelowFloor { floor: u64::MAX }
  ));
  // Under a lower floor the sentinel reports its reservation, not the floor.
  let e = c
    .restore_group(
      100,
      single_voter(1),
      now,
      1,
      CountSm::default(),
      1,
      u64::MAX,
      &Floors(2),
      &mut log,
      &mut stable,
    )
    .unwrap_err();
  assert!(matches!(e, CreateGroupError::ReservedGeneration));
  c.restore_group(
    100,
    single_voter(1),
    now,
    1,
    CountSm::default(),
    1,
    2,
    &Floors(2),
    &mut log,
    &mut stable,
  )
  .expect("at-floor restore admitted");
}

/// The reserved sentinel is refused at its OWN generation on the create path: `u64::MAX` is the
/// merged-tombstone fence, never a working incarnation, so a buggy catalog supplying it is
/// refused under ANY floor — a lower floor (or none at all) reports the reservation, and the
/// terminal `MERGED_FLOOR` fence keeps reporting itself, so it refuses every generation.
#[test]
fn merged_floor_sentinel_generation_is_reserved() {
  struct Floors(u64);
  impl FloorStore<u64> for Floors {
    fn floor(&self, _: &u64) -> u64 {
      self.0
    }

    fn lineage(&self, _: &u64) -> u64 {
      0
    }
  }
  let mut c = MultiCoord::new();
  let now = Instant::ORIGIN;
  let e = c
    .create_group(
      100,
      single_voter(1),
      now,
      1,
      CountSm::default(),
      u64::MAX,
      &Floors(2),
    )
    .unwrap_err();
  assert!(matches!(e, CreateGroupError::ReservedGeneration));
  let e = c
    .create_group(
      100,
      single_voter(1),
      now,
      1,
      CountSm::default(),
      u64::MAX,
      &NoFloors,
    )
    .unwrap_err();
  assert!(
    matches!(e, CreateGroupError::ReservedGeneration),
    "the sentinel is reserved even in a never-floored world"
  );
  let e = c
    .create_group(
      100,
      single_voter(1),
      now,
      1,
      CountSm::default(),
      u64::MAX,
      &Floors(MERGED_FLOOR),
    )
    .unwrap_err();
  assert!(
    matches!(e, CreateGroupError::BelowFloor { floor: u64::MAX }),
    "under the terminal fence the truthful verdict stays the fence itself"
  );
}

/// A vote request for a group this host neither hosts nor has tombstoned surfaces ONCE via
/// `poll_unknown_group` — keyed to the authenticated sender, deduped until polled, re-armed by
/// polling, and purged by an admission — while hosted and tombstoned groups stay silent.
#[test]
fn unknown_group_traffic_surfaces_once_until_polled() {
  let mut w = World::new(&[100, 200], &[100]);
  w.settle();
  assert_eq!(
    w.b.poll_unknown_group(),
    None,
    "the handshake alone signals nothing"
  );

  // Two campaign rounds for un-hosted group 200 arrive before the embedder polls: ONE signal.
  w.fire_a(200);
  w.fire_a(200);
  assert_eq!(
    w.b.poll_unknown_group(),
    Some((200, 1)),
    "the unknown group surfaces with its soliciting peer"
  );
  assert_eq!(w.b.poll_unknown_group(), None, "deduped until polled");

  // Polling re-arms: the next solicitation surfaces afresh.
  w.fire_a(200);
  assert_eq!(w.b.poll_unknown_group(), Some((200, 1)));
  assert_eq!(w.b.poll_unknown_group(), None);

  // Admission PURGES a stale queued signal: polling after the create must not hand the
  // placement brain an "unknown" claim about a group this host now carries.
  w.fire_a(200);
  w.sb
    .map
    .insert(200, (VecLog::default(), AsyncStable::default()));
  w.b
    .create_group(
      200,
      two_voter(2),
      w.now,
      2,
      CountSm::default(),
      0,
      &NoFloors,
    )
    .unwrap();
  assert_eq!(
    w.b.poll_unknown_group(),
    None,
    "the stale signal died with the admission"
  );

  // Hosted traffic never signals.
  w.elect_a(100);
  assert_eq!(
    w.b.poll_unknown_group(),
    None,
    "a hosted group's traffic is not unknown"
  );

  // A tombstoned id never signals — even for initial-shaped traffic.
  assert!(w.b.remove_group(&100).is_some());
  let mut tag = Vec::new();
  sailing_encode_u64(100, &mut tag);
  let rv = Message::RequestVote(crate::RequestVote::new(
    Term::new(9),
    1u64,
    Index::ZERO,
    Term::ZERO,
    false,
    false,
  ));
  let framed = crafted_frame(&tag, &rv);
  w.b
    .handle_conn_data(ConnId(1), &framed, false, w.now, &mut w.sb);
  assert_eq!(
    w.b.poll_unknown_group(),
    None,
    "tombstoned: silent, not unknown"
  );
  assert_eq!(w.b.poll_conn_closed(), None);
}

/// The unknown-group signal is gated on INITIAL-SHAPED kinds (TiKV's `is_initial_msg`): a vote
/// request or a commit-0 first-contact heartbeat surfaces; an AppendEntries or an established
/// (commit > 0) heartbeat — the shape of a removed group's delayed stragglers — drops with NO
/// signal, so a stale frame can never prompt the placement brain to resurrect a destroyed group.
#[test]
fn only_initial_shaped_traffic_signals_an_unknown_group() {
  let mut w = World::new(&[100], &[100]);
  w.settle();
  let mut tag = Vec::new();
  sailing_encode_u64(999, &mut tag);

  // Non-initial kinds for unknown group 999: an append and an established heartbeat — silent.
  let ae = Message::AppendEntries(crate::AppendEntries::new(
    Term::new(3),
    1u64,
    Index::ZERO,
    Term::ZERO,
    Vec::new(),
    Index::new(5),
  ));
  let framed = crafted_frame(&tag, &ae);
  w.b
    .handle_conn_data(ConnId(1), &framed, false, w.now, &mut w.sb);
  let hb = Message::Heartbeat(crate::Heartbeat::new(
    Term::new(3),
    1u64,
    Index::new(5),
    bytes::Bytes::new(),
  ));
  let framed = crafted_frame(&tag, &hb);
  w.b
    .handle_conn_data(ConnId(1), &framed, false, w.now, &mut w.sb);
  assert_eq!(
    w.b.poll_unknown_group(),
    None,
    "non-initial kinds never signal"
  );

  // Initial-shaped kinds: a first-contact (commit-0) heartbeat, then a (pre-)vote request.
  let hb0 = Message::Heartbeat(crate::Heartbeat::new(
    Term::new(3),
    1u64,
    Index::ZERO,
    bytes::Bytes::new(),
  ));
  let framed = crafted_frame(&tag, &hb0);
  w.b
    .handle_conn_data(ConnId(1), &framed, false, w.now, &mut w.sb);
  assert_eq!(
    w.b.poll_unknown_group(),
    Some((999, 1)),
    "a first-contact beat signals"
  );
  let rv = Message::RequestVote(crate::RequestVote::new(
    Term::new(3),
    1u64,
    Index::ZERO,
    Term::ZERO,
    true,
    false,
  ));
  let framed = crafted_frame(&tag, &rv);
  w.b
    .handle_conn_data(ConnId(1), &framed, false, w.now, &mut w.sb);
  assert_eq!(
    w.b.poll_unknown_group(),
    Some((999, 1)),
    "a (pre-)vote request signals"
  );
  assert_eq!(
    w.b.poll_conn_closed(),
    None,
    "well-formed unknown traffic never closes"
  );
}

/// The pending unknown-group set is CAPPED at 64 distinct groups: beyond it new signals drop
/// silently (the sender retries on its own cadence), and polling frees capacity for fresh ones.
#[test]
fn unknown_group_signals_cap_and_recover_on_poll() {
  let mut w = World::new(&[100], &[100]);
  w.settle();
  let rv_frame = |gid: u64| {
    let mut tag = Vec::new();
    sailing_encode_u64(gid, &mut tag);
    let msg = Message::RequestVote(crate::RequestVote::new(
      Term::new(2),
      1u64,
      Index::ZERO,
      Term::ZERO,
      false,
      false,
    ));
    crafted_frame(&tag, &msg)
  };
  for gid in 0..70u64 {
    let framed = rv_frame(1000 + gid);
    w.b
      .handle_conn_data(ConnId(1), &framed, false, w.now, &mut w.sb);
  }
  let mut drained = Vec::new();
  while let Some(sig) = w.b.poll_unknown_group() {
    drained.push(sig);
  }
  assert_eq!(
    drained.len(),
    UNKNOWN_GROUP_SIGNAL_CAP,
    "the queue holds at most the cap"
  );
  assert_eq!(drained[0], (1000, 1), "FIFO from the first solicitation");

  // Polling freed the set: a fresh unknown group signals again.
  let framed = rv_frame(2000);
  w.b
    .handle_conn_data(ConnId(1), &framed, false, w.now, &mut w.sb);
  assert_eq!(w.b.poll_unknown_group(), Some((2000, 1)));
}

/// Encode `n` as a CountSm snapshot blob for the fork-admission tests.
fn fork_blob(n: u64) -> bytes::Bytes {
  let mut v = Vec::new();
  crate::Data::encode(&n, &mut v);
  bytes::Bytes::from(v)
}

/// A fork NEVER clears a tombstone: the coordinator's Retired gate refuses it exactly as
/// create/restore, the refusal writes nothing into the caller's fresh stores, and the two
/// deliberate acts — clear, then fork — admit a group booted at the manufactured baseline.
#[test]
fn fork_refuses_a_tombstoned_id_until_cleared() {
  let mut c = MultiCoord::new();
  let now = Instant::ORIGIN;
  c.create_group(
    100,
    single_voter(1),
    now,
    1,
    CountSm::default(),
    0,
    &NoFloors,
  )
  .unwrap();
  assert!(c.remove_group(&100).is_some());

  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  let e = c
    .create_group_from_fork(
      100,
      single_voter(1),
      now,
      1,
      CountSm::default(),
      fork_blob(3),
      None,
      1,
      0,
      &NoFloors,
      &mut log,
      &mut stable,
    )
    .unwrap_err();
  assert!(
    matches!(e, CreateGroupError::Retired),
    "a fork never clears a tombstone"
  );
  assert_eq!(log.first_index().get(), 1, "the refusal wrote nothing");
  assert!(stable.snapshot().is_none());
  assert!(c.is_retired(&100), "a refused fork lifts nothing");

  assert!(c.clear_tombstone(&100));
  c.create_group_from_fork(
    100,
    single_voter(1),
    now,
    1,
    CountSm::default(),
    fork_blob(3),
    None,
    1,
    0,
    &NoFloors,
    &mut log,
    &mut stable,
  )
  .unwrap();
  let ep = c.group(&100).unwrap();
  assert_eq!(ep.applied_index(), crate::FORK_BASE_INDEX);
  assert_eq!(ep.state_machine().count(), 3, "booted from the fork blob");
}

/// Fork admission walks the SAME floor-first gate as create/restore — the durable fence
/// precedes the volatile consent gate and the container — and the reserved `u64::MAX`
/// sentinel is refused at every floor.
#[test]
fn fork_admission_walks_the_floor_gate_and_reserves_the_sentinel() {
  struct Floors(u64);
  impl FloorStore<u64> for Floors {
    fn floor(&self, _: &u64) -> u64 {
      self.0
    }

    fn lineage(&self, _: &u64) -> u64 {
      0
    }
  }
  let mut c = MultiCoord::new();
  let now = Instant::ORIGIN;
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  let e = c
    .create_group_from_fork(
      100,
      single_voter(1),
      now,
      1,
      CountSm::default(),
      fork_blob(3),
      None,
      1,
      1,
      &Floors(2),
      &mut log,
      &mut stable,
    )
    .unwrap_err();
  assert!(matches!(e, CreateGroupError::BelowFloor { floor: 2 }));
  let e = c
    .create_group_from_fork(
      100,
      single_voter(1),
      now,
      1,
      CountSm::default(),
      fork_blob(3),
      None,
      1,
      u64::MAX,
      &Floors(2),
      &mut log,
      &mut stable,
    )
    .unwrap_err();
  assert!(matches!(e, CreateGroupError::ReservedGeneration));
  let e = c
    .create_group_from_fork(
      100,
      single_voter(1),
      now,
      1,
      CountSm::default(),
      fork_blob(3),
      None,
      1,
      u64::MAX,
      &NoFloors,
      &mut log,
      &mut stable,
    )
    .unwrap_err();
  assert!(
    matches!(e, CreateGroupError::ReservedGeneration),
    "the sentinel is reserved even in a never-floored world"
  );
  let e = c
    .create_group_from_fork(
      100,
      single_voter(1),
      now,
      1,
      CountSm::default(),
      fork_blob(3),
      None,
      1,
      u64::MAX - 1,
      &Floors(MERGED_FLOOR),
      &mut log,
      &mut stable,
    )
    .unwrap_err();
  assert!(matches!(
    e,
    CreateGroupError::BelowFloor { floor: u64::MAX }
  ));
  let e = c
    .create_group_from_fork(
      100,
      single_voter(1),
      now,
      1,
      CountSm::default(),
      fork_blob(3),
      None,
      1,
      u64::MAX,
      &Floors(MERGED_FLOOR),
      &mut log,
      &mut stable,
    )
    .unwrap_err();
  assert!(
    matches!(e, CreateGroupError::BelowFloor { floor: u64::MAX }),
    "under the terminal fence the truthful verdict stays the fence itself"
  );
  assert_eq!(log.first_index().get(), 1, "refusals wrote nothing");
  assert!(stable.snapshot().is_none());

  c.create_group_from_fork(
    100,
    single_voter(1),
    now,
    1,
    CountSm::default(),
    fork_blob(3),
    None,
    1,
    2,
    &Floors(2),
    &mut log,
    &mut stable,
  )
  .expect("at-floor fork admitted");
  assert_eq!(
    c.group(&100).unwrap().applied_index(),
    crate::FORK_BASE_INDEX
  );
}

/// The delegator surfaces the container's fork boot-epoch guard: `boot_epoch == 0` would issue
/// the manufactured baseline's completions in the child's own first live epoch, so the refusal
/// arrives before any store write — the caller's fresh stores stay pristine.
#[test]
fn fork_refuses_boot_epoch_zero() {
  let mut c = MultiCoord::new();
  let now = Instant::ORIGIN;
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  let e = c
    .create_group_from_fork(
      100,
      single_voter(1),
      now,
      1,
      CountSm::default(),
      fork_blob(3),
      None,
      0,
      0,
      &NoFloors,
      &mut log,
      &mut stable,
    )
    .unwrap_err();
  assert!(matches!(e, CreateGroupError::InvalidBootEpoch));
  assert_eq!(log.first_index().get(), 1, "the refusal wrote nothing");
  assert!(stable.snapshot().is_none());
  assert!(!stable.has_pending(), "no completion was ever queued");
  assert!(c.group(&100).is_none());
}

/// A successful fork PURGES a queued unknown-group signal for its id, exactly as create does:
/// polling after the admission must not hand the placement brain a stale "unknown" claim.
#[test]
fn fork_purges_a_queued_unknown_group_signal() {
  let mut w = World::new(&[100, 200], &[100]);
  w.settle();
  w.fire_a(200);
  w.sb
    .map
    .insert(200, (VecLog::default(), AsyncStable::default()));
  let (l, s) = w.sb.map.get_mut(&200).unwrap();
  w.b
    .create_group_from_fork(
      200,
      two_voter(2),
      w.now,
      2,
      CountSm::default(),
      fork_blob(3),
      None,
      1,
      0,
      &NoFloors,
      l,
      s,
    )
    .unwrap();
  assert_eq!(
    w.b.poll_unknown_group(),
    None,
    "the stale signal died with the fork admission"
  );
}

/// The coordinator's propose-time floor gate for splits (the ratified two-point BelowFloor:
/// this fail-fast leg + the drivers' authoritative materialization-edge recheck): a floored
/// child id refuses with the typed verdict and NOTHING is appended to the parent's log; the
/// reserved `u64::MAX` incarnation refuses as its own class at any floor.
#[test]
fn propose_split_gates_the_child_floor() {
  struct Floors(u64);
  impl FloorStore<u64> for Floors {
    fn floor(&self, _: &u64) -> u64 {
      self.0
    }

    fn lineage(&self, _: &u64) -> u64 {
      0
    }
  }
  let mut c = MultiCoord::new();
  let now = Instant::ORIGIN;
  let (mut log, stable) = (VecLog::default(), AsyncStable::default());
  c.create_group(
    100,
    single_voter(1),
    now,
    1,
    CountSm::default(),
    0,
    &NoFloors,
  )
  .unwrap();
  let last = log.last_index();

  let e = c
    .propose_split(
      &100,
      now,
      &mut log,
      &stable,
      &200,
      1,
      Bytes::from_static(b"i"),
      &Floors(2),
    )
    .expect("the parent is hosted")
    .unwrap_err();
  assert_eq!(e, SplitError::BelowFloor { floor: 2 });
  assert_eq!(log.last_index(), last, "nothing was proposed");

  let e = c
    .propose_split(
      &100,
      now,
      &mut log,
      &stable,
      &200,
      u64::MAX,
      Bytes::from_static(b"i"),
      &Floors(0),
    )
    .expect("the parent is hosted")
    .unwrap_err();
  assert_eq!(
    e,
    SplitError::ReservedGeneration,
    "the sentinel incarnation is refused as its own class"
  );
  assert_eq!(log.last_index(), last, "nothing was proposed");
}

/// A state machine whose `split` gives away `instruction[0]` units — the minimal partitionable
/// FSM the crash-restore pins below need (self-contained, mirroring the container tests').
#[derive(Default, Debug, PartialEq)]
struct SplitSm {
  units: u64,
}

impl crate::StateMachine for SplitSm {
  type Command = Bytes;
  type Response = u64;
  type Snapshot = u64;
  type Error = core::convert::Infallible;

  fn apply(&mut self, _index: Index, _cmd: Bytes) -> Result<u64, Self::Error> {
    self.units += 1;
    Ok(self.units)
  }

  fn snapshot(&self) -> Result<u64, Self::Error> {
    Ok(self.units)
  }

  fn restore(&mut self, snapshot: u64) -> Result<(), Self::Error> {
    self.units = snapshot;
    Ok(())
  }

  fn split(&mut self, instruction: &[u8]) -> Option<Self> {
    let give = u64::from(*instruction.first()?).min(self.units);
    self.units -= give;
    Some(Self { units: give })
  }
}

type SplitCoord = MultiStreamCoordinator<u64, u64, SplitSm, TestRecord>;

/// Flush the engine barrier and drain every listed group's completions until the host is quiet
/// (no completion progress, nothing staged) — the driver's storage crank, inlined.
fn settle_engine(c: &mut SplitCoord, e: &mut GroupEngine<u64, u64>, gids: &[u64], now: Instant) {
  loop {
    e.flush();
    let mut more = false;
    for g in gids {
      let (l, s) = e.stores(g).expect("hosted storage");
      if matches!(
        c.handle_storage(g, now, l, s),
        Some(StorageProgress::MorePending)
      ) {
        more = true;
      }
    }
    if !more && !e.has_staged() {
      break;
    }
  }
}

/// The restore-overwrite regression, end to end over ONE engine (the disk): a fork
/// materializes and goes flush-durable, the child accrues post-fork progress, the host
/// crashes, and the PARENT ALONE is restored — its un-compacted split entry replays and
/// re-stages the fork. Pre-fix, the parent's own snapshot meta was the guard's ONLY seed
/// (zero here: the parent never snapshotted), so the drain re-materialized the fork and the
/// manufactured baseline overwrote the child's real durable progress (stores collapsed to the
/// baseline: last_index 4 -> 1, units 4 -> 2). Both independent stops are pinned: the restore
/// arm seeds the relay guard from the DURABLE engine lineage (the replayed fork folds to a
/// resolved no-op), and — with that seed bypassed through a lineage-blind floor store — the
/// materialization edge itself refuses to write over used storage.
#[test]
fn restored_parent_replay_never_overwrites_the_childs_durable_progress() {
  let now = Instant::ORIGIN;
  let mut engine: GroupEngine<u64, u64> = GroupEngine::new();

  // Genesis: parent 100 leads (single voter) and commits 3 units of load.
  let mut c1 = SplitCoord::new();
  engine.add_group(100);
  c1.create_group(
    100,
    single_voter(1),
    now,
    1,
    SplitSm::default(),
    0,
    &NoFloors,
  )
  .unwrap();
  let d = c1.group(&100).unwrap().poll_timeout().unwrap();
  {
    let (l, s) = engine.stores(&100).unwrap();
    c1.handle_timeout(&100, d, l, s).unwrap();
  }
  settle_engine(&mut c1, &mut engine, &[100], d);
  assert!(c1.group(&100).unwrap().role().is_leader());
  for _ in 0..3 {
    let (l, s) = engine.stores(&100).unwrap();
    c1.submit_propose(&100, d, l, s, &Bytes::from_static(b"c"))
      .unwrap()
      .unwrap();
    settle_engine(&mut c1, &mut engine, &[100], d);
  }
  assert_eq!(c1.group(&100).unwrap().state_machine().units, 3);

  // Split: child 300 takes 2 units; materialize EXACTLY as the drivers do — fork drain, child
  // registration + baseline + the parent's lineage record all behind ONE engine barrier, then
  // the fence lift.
  {
    let (l, s) = engine.stores(&100).unwrap();
    c1.propose_split(
      &100,
      d,
      l,
      s,
      &300,
      0,
      Bytes::from_static(b"\x02"),
      &NoFloors,
    )
    .expect("the parent is hosted")
    .expect("the leader appends the split");
  }
  settle_engine(&mut c1, &mut engine, &[100], d);
  let fork = c1.poll_pending_fork().expect("the committed split relays");
  assert_eq!((fork.child, fork.parent_gen_after), (300, 1));
  engine.add_group(300);
  let epoch = engine.next_boot_epoch(&300).unwrap();
  {
    let (l, s) = engine.stores(&300).unwrap();
    c1.create_group_from_fork(
      300,
      fork.config,
      now,
      1,
      fork.fsm,
      fork.blob,
      fork.read_only,
      epoch,
      fork.child_gen,
      &NoFloors,
      l,
      s,
    )
    .expect("the fork materializes over the fresh stores");
  }
  engine.set_group_gen(&100, fork.parent_gen_after);
  engine.flush();
  c1.lift_fork_barrier(&100, fork.split_index);

  // The child accrues REAL post-fork progress: it elects and commits 2 entries of its own.
  let dc = c1.group(&300).unwrap().poll_timeout().unwrap();
  {
    let (l, s) = engine.stores(&300).unwrap();
    c1.handle_timeout(&300, dc, l, s).unwrap();
  }
  settle_engine(&mut c1, &mut engine, &[100, 300], dc);
  assert!(c1.group(&300).unwrap().role().is_leader());
  for _ in 0..2 {
    let (l, s) = engine.stores(&300).unwrap();
    c1.submit_propose(&300, dc, l, s, &Bytes::from_static(b"c"))
      .unwrap()
      .unwrap();
    settle_engine(&mut c1, &mut engine, &[100, 300], dc);
  }
  assert_eq!(
    c1.group(&300).unwrap().state_machine().units,
    4,
    "2 forked units + 2 live commits"
  );
  let used_last = {
    let (l, _) = engine.stores(&300).unwrap();
    l.last_index()
  };
  assert!(used_last > Index::new(1), "the child outgrew its baseline");

  // CRASH. The engine is the disk; everything above was flushed and drained.
  drop(c1);

  // Leg 2 first, with leg 1 BYPASSED (a lineage-blind floor store, the pre-M2 world): the
  // replayed fork relays, and the materialization edge must refuse to write over the child's
  // USED stores — the driver's Err arm then resolves the fence and moves on.
  let mut c2 = SplitCoord::new();
  let epoch = engine.next_boot_epoch(&100).unwrap();
  {
    let (l, s) = engine.stores(&100).unwrap();
    c2.restore_group(
      100,
      single_voter(1),
      now,
      1,
      SplitSm::default(),
      epoch,
      1,
      &NoFloors,
      l,
      s,
    )
    .unwrap();
  }
  assert_eq!(
    c2.group(&100).unwrap().state_machine().units,
    1,
    "the restored parent replays to its post-split half"
  );
  let fork = c2
    .poll_pending_fork()
    .expect("a lineage-blind guard seed relays the replayed fork");
  assert!(
    !engine.add_group(300),
    "the child's storage is already hosted in the engine"
  );
  let epoch = engine.next_boot_epoch(&300).unwrap();
  let refusal = {
    let (l, s) = engine.stores(&300).unwrap();
    c2.create_group_from_fork(
      300,
      fork.config,
      now,
      1,
      fork.fsm,
      fork.blob,
      fork.read_only,
      epoch,
      fork.child_gen,
      &NoFloors,
      l,
      s,
    )
  };
  assert_eq!(
    refusal,
    Err(CreateGroupError::StorageInUse),
    "a fork never overwrites used storage"
  );
  c2.lift_fork_barrier(&100, fork.split_index);
  {
    let (l, _) = engine.stores(&300).unwrap();
    assert_eq!(l.last_index(), used_last, "the refusal wrote nothing");
  }
  drop(c2);

  // Leg 1, the fix proper: the restore arm consumes the DURABLE engine lineage (the driver's
  // pre-call floor snapshot), so the guard already covers the replayed fork and it folds to a
  // resolved no-op — nothing is relayed at all.
  struct Snapshot {
    floor: u64,
    lineage: u64,
  }
  impl FloorStore<u64> for Snapshot {
    fn floor(&self, _: &u64) -> u64 {
      self.floor
    }

    fn lineage(&self, _: &u64) -> u64 {
      self.lineage
    }
  }
  let mut c3 = SplitCoord::new();
  let floors = Snapshot {
    floor: engine.group_floor(&100),
    lineage: engine.group_gen(&100),
  };
  assert_eq!(
    floors.lineage, 1,
    "the barrier made the fork's bump durable"
  );
  let epoch = engine.next_boot_epoch(&100).unwrap();
  {
    let (l, s) = engine.stores(&100).unwrap();
    c3.restore_group(
      100,
      single_voter(1),
      now,
      1,
      SplitSm::default(),
      epoch,
      1,
      &floors,
      l,
      s,
    )
    .unwrap();
  }
  assert!(
    c3.poll_pending_fork().is_none(),
    "the durable lineage already covers the replayed fork"
  );

  // The child restores from its own stores with every post-fork commit intact.
  let floors = Snapshot {
    floor: engine.group_floor(&300),
    lineage: engine.group_gen(&300),
  };
  let epoch = engine.next_boot_epoch(&300).unwrap();
  {
    let (l, s) = engine.stores(&300).unwrap();
    c3.restore_group(
      300,
      single_voter(1),
      now,
      1,
      SplitSm::default(),
      epoch,
      0,
      &floors,
      l,
      s,
    )
    .unwrap();
  }
  assert_eq!(
    c3.group(&300).unwrap().state_machine().units,
    4,
    "the child keeps its post-fork progress across the parent-only restore"
  );
  assert_eq!(
    c3.group(&100).unwrap().state_machine().units + c3.group(&300).unwrap().state_machine().units,
    5,
    "conservation: every unit lives in exactly one of parent / child"
  );
}
