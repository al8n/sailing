use super::*;
use crate::{
  Config, FloorStore, GroupEngine, InstallOutcome, MERGED_FLOOR, Message, NoFloors, NoHold,
  SplitError, Term, TimeoutNow,
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
  /// Per-gid admission floors, the demux fence's durable input. Empty (floor 0) unless a test
  /// retires an incarnation, so every other test's behavior is unchanged.
  floors: BTreeMap<u64, u64>,
}

impl GroupStores<u64, VecLog, AsyncStable> for Stores {
  fn stores(&mut self, group: &u64) -> Option<(&mut VecLog, &mut AsyncStable)> {
    self.map.get_mut(group).map(|(l, s)| (l, s))
  }
}

impl FloorStore<u64> for Stores {
  fn floor(&self, gid: &u64) -> u64 {
    self.floors.get(gid).copied().unwrap_or(0)
  }

  fn lineage(&self, _gid: &u64) -> u64 {
    0
  }
}

/// An empty store seam for a coordinator teardown whose participant gate resolves on in-memory
/// state alone (no freeze-pending source to scan for the `Claimed` leg).
fn empty_stores() -> Stores {
  Stores {
    map: BTreeMap::new(),
    floors: BTreeMap::new(),
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
    floors: BTreeMap::new(),
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
    coord
      .submit_propose(&100, d, l, s, &cmd, &NoFloors)
      .unwrap()
      .unwrap();
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
  crafted_frame_at(tag, 0, msg)
}

/// [`crafted_frame`] with an explicit incarnation stamp — the fence's hand-fed input.
fn crafted_frame_at(tag: &[u8], generation: u64, msg: &Message<u64>) -> Vec<u8> {
  let mut payload = Vec::new();
  crate::transport::frame::write_group_header(tag, generation, &mut payload);
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
      floors: BTreeMap::new(),
    };
    let mut sb = Stores {
      map: BTreeMap::new(),
      floors: BTreeMap::new(),
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
    let ca = a.on_dial_open(2, label(1, true), Instant::ORIGIN);
    let cb = b.on_accept_open(label(2, false), Instant::ORIGIN);
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

/// A quiesced connection whose peer goes SILENT (a blackhole — socket alive, bytes dropped) is
/// reaped through the transport-timeout seam: past the idle timeout `transport_timeout` surfaces the
/// silence deadline and `handle_transport_timeout` closes the connection — the loss the driver turns
/// into a wake-all → election. A validated connection that surfaces no transport deadline is never
/// reaped, so a quiesced plane that sends nothing would never produce that wake.
#[test]
fn a_silent_quiesced_connection_is_reaped_through_the_transport_timeout() {
  let mut w = World::new(&[100], &[100]);
  w.settle();
  w.elect_a(100);
  assert_eq!(w.a.conn_of(&2), Some(ConnId(1)), "a bound its link to b");
  w.a.mark_quiescing(&100);
  w.settle();

  // Blackhole: no further bytes reach `a`. Past the idle timeout the transport timeout is due.
  let idle = w.now + Duration::from_millis(3001);
  assert!(
    w.a.transport_timeout().is_some_and(|d| d <= idle),
    "the silence deadline is armed and due at the idle horizon"
  );
  w.a.handle_transport_timeout(idle);
  assert_eq!(
    w.a.poll_conn_closed(),
    Some((ConnId(1), Some(TransportError::IdleTimeout))),
    "the silent link is reaped, surfacing the loss the driver wakes on"
  );
  assert_eq!(w.a.conn_of(&2), None, "the route is dropped");
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
    .map(|(flags, group, _generation, msg)| {
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
  crate::transport::frame::write_coalesced_entry(1, &gb, 0, &msg_bytes, &mut payload);
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
  crate::transport::frame::write_coalesced_entry(1, &gb, 0, &msg_bytes, &mut payload);
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
/// the group — the flagged beat's `Wake` and `Quiesce` collapse to the latter, netting quiesced.
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
    std::vec![(100, GroupControl::Quiesce)],
    "the flagged beat collapses to its latest control — net quiesced"
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

/// Interleaved controls across groups (a burst the driver has not drained) leave one queued
/// entry per DISTINCT group, not one per push: the queue is membership-deduped by `control_state`.
#[test]
fn interleaved_controls_dedup_by_membership() {
  let mut coord = MultiCoord::new();
  for _ in 0..64 {
    coord.push_control(&1, GroupControl::Wake);
    coord.push_control(&2, GroupControl::Wake);
  }
  assert!(
    coord.controls.len() <= 2,
    "control queue grew to {}",
    coord.controls.len()
  );
}

/// A group's queued signal collapses to its LATEST control: `Wake`, `Quiesce`, then `Wake` for one
/// group delivers a single `Wake`.
#[test]
fn controls_collapse_to_the_latest_per_group() {
  let mut coord = MultiCoord::new();
  coord.push_control(&7, GroupControl::Wake);
  coord.push_control(&7, GroupControl::Quiesce);
  coord.push_control(&7, GroupControl::Wake);
  let mut drained = Vec::new();
  while let Some(c) = coord.poll_group_control() {
    drained.push(c);
  }
  assert_eq!(drained, std::vec![(7, GroupControl::Wake)]);
}

/// The flagged beat — a `Wake` immediately followed by a `Quiesce` for one group — delivers a
/// single `Quiesce`, net-identical to folding the pair.
#[test]
fn flagged_beat_collapses_to_a_single_quiesce() {
  let mut coord = MultiCoord::new();
  coord.push_control(&7, GroupControl::Wake);
  coord.push_control(&7, GroupControl::Quiesce);
  let mut drained = Vec::new();
  while let Some(c) = coord.poll_group_control() {
    drained.push(c);
  }
  assert_eq!(drained, std::vec![(7, GroupControl::Quiesce)]);
}

/// A purged group's queued gid goes inert: `poll_group_control` skips it and keeps draining the
/// live groups.
#[test]
fn purged_group_control_is_skipped_at_poll() {
  let mut coord = MultiCoord::new();
  coord.push_control(&100, GroupControl::Wake);
  coord.push_control(&200, GroupControl::Wake);
  coord.remove_group(&100, &mut empty_stores()).unwrap();
  assert_eq!(
    coord.poll_group_control(),
    Some((200, GroupControl::Wake)),
    "the live group still drains"
  );
  assert_eq!(
    coord.poll_group_control(),
    None,
    "the purged group yields nothing"
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

  // An idle heartbeat response stays the sole absorbed shape.
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

/// The wedged-park exception, both directions through the real delivery path: an idle
/// `HeartbeatResponse` is absorbed, and the SAME response carrying a nonzero `stuck_boundary`
/// wakes the leader's group. The advertiser is not log-lagging — its park sits above a fully
/// replicated log — so no other leader-side signal can see it; absorbing the advertisement would
/// let the group settle with that replica wedged forever and the only party able to cure it asleep.
#[test]
fn a_wedged_park_advertisement_is_wake_class() {
  let mut w = World::new(&[100], &[100]);
  w.settle();
  w.elect_a(100);
  while w.a.poll_group_control().is_some() {}
  while w.b.poll_group_control().is_some() {}
  let term = w.a.group(&100).unwrap().term();
  let mut gb = Vec::new();
  sailing_encode_u64(100, &mut gb);

  let idle = Message::HeartbeatResponse(crate::HeartbeatResponse::new(
    term,
    2u64,
    bytes::Bytes::new(),
  ));
  let frame = crafted_frame(&gb, &idle);
  w.a
    .handle_conn_data(ConnId(1), &frame, false, w.now, &mut w.sa);
  assert_eq!(
    w.a.poll_group_control(),
    None,
    "a response advertising nothing is absorbed"
  );

  let advertising = Message::HeartbeatResponse(
    crate::HeartbeatResponse::new(term, 2u64, bytes::Bytes::new())
      .with_stuck_boundary(Index::new(7)),
  );
  let frame = crafted_frame(&gb, &advertising);
  w.a
    .handle_conn_data(ConnId(1), &frame, false, w.now, &mut w.sa);
  assert_eq!(
    w.a.poll_group_control(),
    Some((100, GroupControl::Wake)),
    "a response advertising a wedged park wakes the group"
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
    w.a
      .submit_propose(&100, now, l, s, &cmd, &NoFloors)
      .unwrap()
      .unwrap();
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
  crate::transport::frame::write_coalesced_entry(0, &tag, 0, &msg_bytes, &mut payload);
  let (tag, msg_bytes) = hb(100);
  crate::transport::frame::write_coalesced_entry(0, &tag, 0, &msg_bytes, &mut payload);
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
  assert!(
    w.b
      .remove_group(&100, &mut empty_stores())
      .unwrap()
      .is_some()
  );
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
  // cell 4: at the floor — admitted, through the door a nonzero founding value must use
  let (founding_log, mut founding_stable) = (VecLog::default(), AsyncStable::default());
  c.create_group_founded_at(
    100,
    single_voter(1),
    now,
    1,
    CountSm::default(),
    2,
    &Floors(2, 0),
    1,
    &founding_log,
    &mut founding_stable,
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
  assert!(c.remove_group(&100, &mut empty_stores()).unwrap().is_some());
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
  // The rejoin is a FRESH incarnation and gets fresh stores: the founding door refuses storage
  // that already holds an incarnation's state, including the one it stamped a moment ago.
  let (rejoin_log, mut rejoin_stable) = (VecLog::default(), AsyncStable::default());
  c.create_group_founded_at(
    100,
    single_voter(1),
    now,
    1,
    CountSm::default(),
    9,
    &Floors(2, 0),
    1,
    &rejoin_log,
    &mut rejoin_stable,
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
  // A floored id is one with a HISTORY, so an at-floor restore over stores that hold nothing is
  // refused before the floor gate is even the question: there is nothing here to recover.
  let e = c
    .restore_group(
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
    .unwrap_err();
  assert!(matches!(e, CreateGroupError::NoStoredState));

  // STATE ALONE IS NOT ENOUGH. These stores hold a term but account for no lineage at all — they
  // are the FENCED incarnation's — so the at-floor CLAIM must not recover them. Admitting on the
  // claim resurrects exactly what the floor buried.
  stable.submit_write(
    crate::OpId::new(1),
    crate::HardState::initial().with_term(Term::new(1)),
  );
  let e = c
    .restore_group(
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
    .unwrap_err();
  assert!(
    matches!(
      e,
      CreateGroupError::StoredStateBelowFloor {
        floor: 2,
        recoverable: 0
      }
    ),
    "stores that reach no lineage must not be recovered at the floor, got {e:?}"
  );

  // With the stores' OWN lineage at the floor — the shape a legitimate recreate-at-2 leaves, its
  // founding generation stamped before the create was acked — the at-floor restore admits.
  stable.submit_write(
    crate::OpId::new(2),
    crate::HardState::initial()
      .with_term(Term::new(1))
      .with_founding_gen(2),
  );
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
  .expect("at-floor restore admitted once the stores reach the floor");
}

/// A restore's `generation` is compared against the id's DURABLE LINEAGE RECORD, not only its
/// floor: below the record it REFUSES, at or above it admits and the guard's `max` fold stands.
///
/// The record outlives the process that wrote it, so at a restore it is definitionally the
/// better-informed of the two readings. Folding a lower catalog value up by `max` — which is what
/// the relay guard did on its own — hid the disagreement at the one moment it matters, leaving the
/// endpoint seeded at the supplied generation while every gen-keyed door answered to the record.
#[test]
fn restore_below_the_durable_lineage_record_refuses() {
  struct Record(u64);
  impl FloorStore<u64> for Record {
    fn floor(&self, _: &u64) -> u64 {
      0
    }

    fn lineage(&self, _: &u64) -> u64 {
      self.0
    }
  }
  let mut c = MultiCoord::new();
  let now = Instant::ORIGIN;
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  stable.submit_write(
    crate::OpId::new(1),
    crate::HardState::initial().with_term(Term::new(1)),
  );

  let e = c
    .restore_group(
      100,
      single_voter(1),
      now,
      1,
      CountSm::default(),
      1,
      6,
      &Record(7),
      &mut log,
      &mut stable,
    )
    .unwrap_err();
  assert!(
    matches!(e, CreateGroupError::BelowLineageRecord { record: 7 }),
    "a generation below the durable record must refuse typed, got {e:?}"
  );
  assert!(
    c.group(&100).is_none(),
    "a refused restore leaves the container untouched"
  );

  // AT the record admits — and the admitted generation stays OUT of the endpoint: the live counter
  // is the input to every apply-time lineage guard, and these stores replay no lineage move at all.
  c.restore_group(
    100,
    single_voter(1),
    now,
    1,
    CountSm::default(),
    1,
    7,
    &Record(7),
    &mut log,
    &mut stable,
  )
  .expect("an at-record restore is admitted");
  assert_eq!(
    c.group(&100).unwrap().shape_gen(),
    0,
    "the admitted generation gates the door; the live counter still reads the replay evidence"
  );
}

/// A restore ABOVE the record admits — the catalog is entitled to move an id's incarnation forward
/// — and the admitted generation is VALIDATED, never INSTALLED. The live lineage counter is the
/// input to every apply-time lineage guard, and those guards compare for exact equality: a counter
/// carrying a per-host catalog value would have this replica mint generations no other replica can
/// admit, and judge committed shape entries by a yardstick no other replica shares.
#[test]
fn a_restore_above_the_record_validates_but_never_installs_the_admitted_generation() {
  struct Record(u64);
  impl FloorStore<u64> for Record {
    fn floor(&self, _: &u64) -> u64 {
      0
    }

    fn lineage(&self, _: &u64) -> u64 {
      self.0
    }
  }
  let mut c = MultiCoord::new();
  let now = Instant::ORIGIN;
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  stable.submit_write(
    crate::OpId::new(1),
    crate::HardState::initial().with_term(Term::new(1)),
  );
  c.restore_group(
    100,
    single_voter(1),
    now,
    1,
    CountSm::default(),
    1,
    9,
    &Record(7),
    &mut log,
    &mut stable,
  )
  .expect("an above-record restore is admitted");
  assert_eq!(
    c.group(&100).unwrap().shape_gen(),
    0,
    "the endpoint's counter reads its replay evidence, not the generation it was admitted at"
  );
  assert_eq!(
    c.multi.group_gen(&100),
    7,
    "the guard took the durable record, and nothing anywhere took the catalog's 9"
  );
}

/// A restore naming an id the lineage KNOWS, over stores holding nothing, refuses rather than
/// building a blank term-0 endpoint and presenting it as recovered state. An id the lineage does
/// NOT know keeps the gen-0 world's behaviour exactly.
#[test]
fn restore_over_empty_stores_refuses_only_for_a_known_id() {
  struct Record(u64);
  impl FloorStore<u64> for Record {
    fn floor(&self, _: &u64) -> u64 {
      0
    }

    fn lineage(&self, _: &u64) -> u64 {
      self.0
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
      4,
      &Record(4),
      &mut log,
      &mut stable,
    )
    .unwrap_err();
  assert!(
    matches!(e, CreateGroupError::NoStoredState),
    "a known id over empty stores has nothing to recover, got {e:?}"
  );

  // The gen-0 world is untouched: an id with no record and no floor restores off empty stores
  // exactly as it always did.
  c.restore_group(
    101,
    single_voter(1),
    now,
    1,
    CountSm::default(),
    1,
    0,
    &Record(0),
    &mut log,
    &mut stable,
  )
  .expect("an unknown id keeps the gen-0 behaviour");
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

/// A group admitted at the HIGHEST WORKING generation floors PERMANENTLY on removal — and does so
/// with the headroom below the sentinel, never the sentinel itself. A `MERGED_FLOOR` floor is read
/// as a GLOBAL verdict that a lineage was absorbed away; an ordinary local removal must not be able
/// to forge one, which is why the top two generations are reserved rather than the top one.
#[test]
fn a_terminal_edge_group_floors_permanently_on_removal() {
  // The highest working generation is two below the sentinel: the one between is the fence's.
  assert!(crate::floor_admits(
    0,
    crate::HIGHEST_WORKING_GENERATION - 1
  ));
  assert!(!crate::floor_admits(0, crate::HIGHEST_WORKING_GENERATION));
  let mut engine: GroupEngine<u64, u64> = GroupEngine::new();
  engine.set_group_gen(&7, crate::HIGHEST_WORKING_GENERATION - 1);
  let floor = engine.removal_floor(&7);
  assert_eq!(
    floor,
    crate::HIGHEST_WORKING_GENERATION,
    "the ceiling fences with the reserved headroom, and never reaches the terminal"
  );
  assert_ne!(
    floor, MERGED_FLOOR,
    "a local removal forges no global verdict"
  );
  engine.set_group_floor(&7, floor);
  // The fence still admits nothing: every working generation is strictly below it, and the two
  // reserved values are refused for being reserved.
  for generation in [0, crate::HIGHEST_WORKING_GENERATION - 1] {
    assert_eq!(
      crate::multi::validate_floor(engine.group_floor(&7), generation),
      Err(CreateGroupError::BelowFloor {
        floor: crate::HIGHEST_WORKING_GENERATION
      }),
      "the fence refuses recreation at working generation {generation}"
    );
  }
  for generation in [crate::HIGHEST_WORKING_GENERATION, MERGED_FLOOR] {
    assert_eq!(
      crate::multi::validate_floor(engine.group_floor(&7), generation),
      Err(CreateGroupError::ReservedGeneration),
      "and a reserved generation is refused as reserved, at {generation}"
    );
  }
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
  assert!(
    w.b
      .remove_group(&100, &mut empty_stores())
      .unwrap()
      .is_some()
  );
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
  assert!(c.remove_group(&100, &mut empty_stores()).unwrap().is_some());

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
  // Floors the CHILD id only: the parent is judged first and on its own record, so a fixture that
  // answers one floor for every id would fence the parent and never reach the leg under test.
  struct Floors(u64);
  impl FloorStore<u64> for Floors {
    fn floor(&self, gid: &u64) -> u64 {
      if *gid == 200 { self.0 } else { 0 }
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

#[test]
fn propose_split_refuses_a_tombstoned_child() {
  // The #97-1 ChildRetired gate: a split whose child id THIS host has tombstoned is refused at
  // propose (beside the floor leg) — the fork could never materialize onto a retired id (admission
  // refuses `Retired`), so the entry is never appended. Clear-then-recreate is the rejoin path.
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
  c.create_group(
    200,
    single_voter(1),
    now,
    2,
    CountSm::default(),
    0,
    &NoFloors,
  )
  .unwrap();
  assert!(c.remove_group(&200, &mut empty_stores()).unwrap().is_some());
  assert!(c.is_retired(&200), "removal tombstones the child id");
  let last = log.last_index();

  let e = c
    .propose_split(
      &100,
      now,
      &mut log,
      &stable,
      &200,
      0,
      Bytes::from_static(b"i"),
      &NoFloors,
    )
    .expect("the parent is hosted")
    .unwrap_err();
  assert_eq!(e, SplitError::ChildRetired);
  assert_eq!(
    log.last_index(),
    last,
    "nothing was proposed for a tombstoned child"
  );

  // Clearing the tombstone lifts THIS gate (the split then fails for an unrelated reason, never
  // ChildRetired).
  assert!(c.clear_tombstone(&200), "a tombstone existed");
  let e = c
    .propose_split(
      &100,
      now,
      &mut log,
      &stable,
      &200,
      0,
      Bytes::from_static(b"i"),
      &NoFloors,
    )
    .expect("the parent is hosted")
    .unwrap_err();
  assert_ne!(
    e,
    SplitError::ChildRetired,
    "a cleared tombstone no longer gates the split"
  );
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

  fn absorb(&mut self, source: Self) -> bool {
    self.units += source.units;
    true
  }

  fn supports_split(&self) -> bool {
    true
  }

  fn supports_absorb(&self) -> bool {
    true
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
/// re-stages the fork. Seeding the guard from the parent's own snapshot meta ALONE
/// (zero here: the parent never snapshotted) lets the drain re-materialize the fork, and the
/// manufactured baseline overwrites the child's real durable progress (stores collapse to the
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
    c1.submit_propose(&100, d, l, s, &Bytes::from_static(b"c"), &NoFloors)
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
  {
    let fork = c1
      .peek_yieldable_fork(&NoHold)
      .expect("the committed split relays");
    assert_eq!(((*fork.child()), fork.parent_gen_after()), (300, 1));
  }
  let InstallOutcome::Installed {
    parent_gen_after,
    split_index,
    ..
  } = c1.install_yieldable_fork(&100, &300, &mut engine, now, 1)
  else {
    panic!("the fork materializes over the fresh stores")
  };
  engine.set_group_gen(&100, parent_gen_after);
  engine.flush();
  c1.lift_fork_barrier(&100, split_index);

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
    c1.submit_propose(&300, dc, l, s, &Bytes::from_static(b"c"), &NoFloors)
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
  assert!(
    c2.peek_yieldable_fork(&NoHold).is_some(),
    "a lineage-blind guard seed relays the replayed fork"
  );
  assert!(
    !engine.add_group(300),
    "the child's storage is already hosted in the engine"
  );
  assert_eq!(
    c2.install_yieldable_fork(&100, &300, &mut engine, now, 1),
    InstallOutcome::Held,
    "a fork never overwrites used storage — and the answer is HOLD, not abandon: the squatting \
     incarnation can be removed, while abandoning here would destroy the partition's only local \
     copy"
  );
  assert!(
    c2.group(&100).unwrap().peek_pending_fork().is_some(),
    "held means STAGED and still fenced — the correct fail-closed state, so nothing lifts"
  );
  {
    let (l, _) = engine.stores(&300).unwrap();
    assert_eq!(l.last_index(), used_last, "the hold wrote nothing");
  }
  drop(c2);

  // Leg 1: the restore arm consumes the DURABLE engine lineage (the driver's
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
    c3.peek_yieldable_fork(&NoHold).is_none(),
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

/// THE PUBLIC FORK DOOR IS FENCED, AND TOKEN-LESS. `create_group_from_fork` is callable by anyone
/// holding a coordinator, so it is fenced two ways. It no longer ACCEPTS provenance at all — the
/// parameter is gone, so a caller cannot stamp its own content with an identity, which matters
/// because a `ForkId` is minted from PUBLIC, DETERMINISTIC split coordinates and any caller can
/// compute the exact token a genuine fork will carry. And it refuses a RESERVED child id in every
/// window — between propose and apply, while the fork is staged, and (the window the pop used to
/// open) between the yield and the sealed install. Without either fence a caller could install a
/// squatter the committed fork then matches and resolves REDUNDANT against, silently discarding
/// the partition.
#[test]
fn the_public_fork_door_is_fenced_in_every_window_and_takes_no_provenance() {
  let now = Instant::ORIGIN;
  let mut engine: GroupEngine<u64, u64> = GroupEngine::new();
  let mut c = SplitCoord::new();
  engine.add_group(100);
  c.create_group(
    100,
    single_voter(1),
    now,
    1,
    SplitSm::default(),
    0,
    &NoFloors,
  )
  .unwrap();
  let d = c.group(&100).unwrap().poll_timeout().unwrap();
  {
    let (l, s) = engine.stores(&100).unwrap();
    c.handle_timeout(&100, d, l, s).unwrap();
  }
  settle_engine(&mut c, &mut engine, &[100], d);
  for _ in 0..3 {
    let (l, s) = engine.stores(&100).unwrap();
    c.submit_propose(&100, d, l, s, &Bytes::from_static(b"c"), &NoFloors)
      .unwrap()
      .unwrap();
    settle_engine(&mut c, &mut engine, &[100], d);
  }
  {
    let (l, s) = engine.stores(&100).unwrap();
    c.propose_split(
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

  // WINDOW A — proposed, not yet applied. The attacker cannot know the token yet, but the door
  // must refuse regardless of what it is handed.
  let (mut scratch_l, mut scratch_s) = (VecLog::default(), AsyncStable::default());
  assert_eq!(
    c.create_group_from_fork(
      300,
      single_voter(1),
      now,
      9,
      SplitSm::default(),
      fork_blob(1),
      None,
      1,
      0,
      &NoFloors,
      &mut scratch_l,
      &mut scratch_s,
    ),
    Err(CreateGroupError::SplitReserved),
    "window A: the public door refuses a reserved child id"
  );

  // WINDOW B — the split applied and its fork is STAGED. The token is derivable from the committed
  // coordinates now, but there is nowhere to put it: the door takes none. The reservation refuses
  // regardless.
  settle_engine(&mut c, &mut engine, &[100], d);
  assert_eq!(
    c.create_group_from_fork(
      300,
      single_voter(1),
      now,
      9,
      SplitSm::default(),
      fork_blob(1),
      None,
      1,
      0,
      &NoFloors,
      &mut scratch_l,
      &mut scratch_s,
    ),
    Err(CreateGroupError::SplitReserved),
    "window B: an EXACTLY-minted token buys nothing at the public door"
  );
  assert_eq!(
    scratch_l.last_index(),
    Index::ZERO,
    "every refusal wrote nothing"
  );

  // THE GENUINE FORK STILL LANDS, because the relay's materialization is not a door at all: the
  // container installs the child from its own staged queue, with nothing for a caller to supply.
  assert!(matches!(
    c.install_yieldable_fork(&100, &300, &mut engine, now, 1),
    InstallOutcome::Installed { child: 300, .. }
  ));
  assert!(
    c.group(&300).is_some(),
    "the child the attacker could not squat is now the genuine fork's"
  );
}

/// The split reservation walks the whole fork lifecycle at the admission edge: from the
/// leader's propose (window A) through the staged fork (window B), every OTHER admission path —
/// create and restore — refuses the child id with the typed verdict, and the factory-gate
/// predicate reads true. The FORK door is outside the fence by design (see
/// [`MultiRaft::split_reserved`]): the reservation exists to stop other doors squatting an id a
/// split owns, and the fork IS that split claiming it. The reservation releases when the relay
/// yields; thereafter the id refuses as plain `Exists`. Without the reservation every one of
/// these admissions succeeds — planting the squatter whose conflict the relay must then park around.
#[test]
fn admission_refuses_an_in_flight_splits_child_id() {
  let now = Instant::ORIGIN;
  let mut engine: GroupEngine<u64, u64> = GroupEngine::new();
  let mut c = SplitCoord::new();
  engine.add_group(100);
  c.create_group(
    100,
    single_voter(1),
    now,
    1,
    SplitSm::default(),
    0,
    &NoFloors,
  )
  .unwrap();
  let d = c.group(&100).unwrap().poll_timeout().unwrap();
  {
    let (l, s) = engine.stores(&100).unwrap();
    c.handle_timeout(&100, d, l, s).unwrap();
  }
  settle_engine(&mut c, &mut engine, &[100], d);
  assert!(c.group(&100).unwrap().role().is_leader());
  for _ in 0..3 {
    let (l, s) = engine.stores(&100).unwrap();
    c.submit_propose(&100, d, l, s, &Bytes::from_static(b"c"), &NoFloors)
      .unwrap()
      .unwrap();
    settle_engine(&mut c, &mut engine, &[100], d);
  }

  // WINDOW A: the split is proposed (appended, durable-pending) and deliberately NOT settled.
  {
    let (l, s) = engine.stores(&100).unwrap();
    c.propose_split(
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
  assert!(c.is_split_reserved(&300), "reserved from the propose on");
  assert!(
    !c.is_split_reserved(&301),
    "only the named child is reserved"
  );
  let (mut scratch_l, mut scratch_s) = (VecLog::default(), AsyncStable::default());
  assert_eq!(
    c.create_group(
      300,
      single_voter(1),
      now,
      9,
      SplitSm::default(),
      0,
      &NoFloors
    ),
    Err(CreateGroupError::SplitReserved)
  );
  assert_eq!(
    c.restore_group(
      300,
      single_voter(1),
      now,
      9,
      SplitSm::default(),
      1,
      0,
      &NoFloors,
      &mut scratch_l,
      &mut scratch_s,
    ),
    Err(CreateGroupError::SplitReserved)
  );
  // The FORK door is deliberately outside this fence and is not exercised here: a committed
  // fork's materialization is the split CLAIMING its own id, not another door asking for it, so
  // consulting the reservation there would refuse the very admission it exists to protect (the
  // id is reserved BY that fork). See `MultiRaft::split_reserved`.
  assert_eq!(
    scratch_l.last_index(),
    Index::ZERO,
    "every refusal wrote nothing"
  );

  // WINDOW B: the split applies and its fork is STAGED — the reservation carries over.
  settle_engine(&mut c, &mut engine, &[100], d);
  assert!(c.is_split_reserved(&300), "reserved while staged");
  assert_eq!(
    c.create_group(
      300,
      single_voter(1),
      now,
      9,
      SplitSm::default(),
      0,
      &NoFloors
    ),
    Err(CreateGroupError::SplitReserved)
  );

  // A PEEK CONSUMES NOTHING: the fork is still staged, so the id is still reserved and the door
  // stays shut — there is no window between the decision and the install to slip through.
  assert!(
    c.peek_yieldable_fork(&NoHold).is_some(),
    "the committed split relays"
  );
  assert!(
    c.is_split_reserved(&300),
    "still staged, so still this fork's id"
  );
  let InstallOutcome::Installed {
    parent_gen_after,
    split_index,
    ..
  } = c.install_yieldable_fork(&100, &300, &mut engine, now, 1)
  else {
    panic!("the staged fork materializes")
  };
  engine.set_group_gen(&100, parent_gen_after);
  engine.flush();
  c.lift_fork_barrier(&100, split_index);

  // Post-resolution the id is simply hosted: the refusal class hands over to `Exists`.
  assert_eq!(
    c.create_group(
      300,
      single_voter(1),
      now,
      9,
      SplitSm::default(),
      0,
      &NoFloors
    ),
    Err(CreateGroupError::Exists)
  );
  assert_eq!(
    c.group(&300).unwrap().state_machine().units,
    2,
    "the materialized child carries the partition"
  );
}

/// A floor store reporting the terminal MERGED_FLOOR for every id — the coordinator leg's
/// refusal input.
struct MaxFloors;

impl FloorStore<u64> for MaxFloors {
  fn floor(&self, _gid: &u64) -> u64 {
    MERGED_FLOOR
  }

  fn lineage(&self, _gid: &u64) -> u64 {
    0
  }
}

#[test]
fn merge_verbs_ride_the_coordinator() {
  let mut coord = MultiStreamCoordinator::<u64, u64, CountSm, TestRecord>::new();
  let mut stores = Stores {
    map: BTreeMap::new(),
    floors: BTreeMap::new(),
  };
  for gid in [1u64, 2] {
    stores
      .map
      .insert(gid, (VecLog::default(), AsyncStable::default()));
    coord
      .create_group(
        gid,
        single_voter(1),
        Instant::ORIGIN,
        1,
        CountSm::default(),
        0,
        &NoFloors,
      )
      .unwrap();
    let d = coord.group(&gid).unwrap().poll_timeout().unwrap();
    {
      let (l, s) = stores.stores(&gid).unwrap();
      coord.handle_timeout(&gid, d, l, s).unwrap();
    }
    for _ in 0..2 {
      let (l, s) = stores.stores(&gid).unwrap();
      coord.handle_storage(&gid, d, l, s).unwrap();
    }
    assert!(coord.group(&gid).unwrap().role().is_leader());
  }
  let now = Instant::ORIGIN;

  // The coordinator's floor leg refuses a fenced participant BEFORE anything is appended.
  assert!(matches!(
    coord
      .prepare_merge(&2, now, &mut stores, &1, &MaxFloors)
      .unwrap(),
    Err(crate::MergeError::BelowFloor {
      floor: MERGED_FLOOR
    })
  ));
  // The container preconditions surface through the delegator verbatim.
  assert!(matches!(
    coord
      .prepare_merge(&2, now, &mut stores, &2, &NoFloors)
      .unwrap(),
    Err(crate::MergeError::SelfMerge)
  ));

  // Freeze, park, and resolve THROUGH the coordinator.
  coord
    .prepare_merge(&2, now, &mut stores, &1, &NoFloors)
    .unwrap()
    .unwrap();
  {
    let (l, s) = stores.stores(&2).unwrap();
    coord.handle_storage(&2, now, l, s).unwrap();
  }
  assert!(coord.group(&2).unwrap().is_frozen());
  {
    let (l, s) = stores.stores(&1).unwrap();
    coord
      .commit_merge(&1, now, l, s, &2, &NoFloors)
      .unwrap()
      .unwrap();
    coord.handle_storage(&1, now, l, s).unwrap();
  }
  assert!(coord.group(&1).unwrap().pending_merge().is_some());
  // The first pass seals the park's abort window; the drain commits the seal; the next pass
  // absorbs.
  assert!(
    coord.service_merge_applies(now, &mut stores).is_empty(),
    "the first pass only seals"
  );
  {
    let (l, s) = stores.stores(&1).unwrap();
    coord.handle_storage(&1, now, l, s).unwrap();
  }
  let resolutions = coord.service_merge_applies(now, &mut stores);
  assert_eq!(
    resolutions,
    std::vec![crate::MergeResolution::Merged {
      source: 2,
      target: 1
    }]
  );
  // The source id is TOMBSTONED at the coordinator: stragglers drop silently (the P5 wire
  // story), and re-admission refuses until the explicit clear — while the terminal floor the
  // DRIVER persists from the resolution outlives even that.
  assert!(coord.group(&2).is_none());
  assert!(coord.is_retired(&2), "resolved merge tombstones the source");

  // The abort delegator is reachable too, with a typed refusal: the merged-away source is gone
  // (`SourceMissing`). The source-side thaw has NO delegator — it is fully service-driven, so no
  // external path can move a frozen source's counter without a committed target abort.
  {
    let (l, s) = stores.stores(&1).unwrap();
    assert!(matches!(
      coord.rollback_merge(&1, now, l, s, &2, &NoFloors).unwrap(),
      Err(crate::MergeError::SourceMissing)
    ));
  }
  assert!(
    coord.group(&1).is_some_and(|ep| !ep.has_abandoned()),
    "no abort applied — the target records no thaw obligation"
  );
}

/// The container's participant teardown gate is INHERITED verbatim by the coordinator door, and a
/// refusal writes NO side state: the id is left hosted, UN-tombstoned (`retired`), and freely
/// retryable. A frozen source refuses `Frozen` through the delegator; its parked target
/// `MergeParked`. Proves the door threads the new variants (not just `OwesThaw`) and that its
/// tombstone insert stays UNREACHABLE behind the gate's `?`.
#[test]
fn coordinator_teardown_inherits_the_participant_gate_without_tombstoning() {
  let mut coord = MultiStreamCoordinator::<u64, u64, CountSm, TestRecord>::new();
  let mut stores = Stores {
    map: BTreeMap::new(),
    floors: BTreeMap::new(),
  };
  for gid in [1u64, 2] {
    stores
      .map
      .insert(gid, (VecLog::default(), AsyncStable::default()));
    coord
      .create_group(
        gid,
        single_voter(1),
        Instant::ORIGIN,
        1,
        CountSm::default(),
        0,
        &NoFloors,
      )
      .unwrap();
    let d = coord.group(&gid).unwrap().poll_timeout().unwrap();
    {
      let (l, s) = stores.stores(&gid).unwrap();
      coord.handle_timeout(&gid, d, l, s).unwrap();
    }
    for _ in 0..2 {
      let (l, s) = stores.stores(&gid).unwrap();
      coord.handle_storage(&gid, d, l, s).unwrap();
    }
    assert!(coord.group(&gid).unwrap().role().is_leader());
  }
  let now = Instant::ORIGIN;
  // Freeze 1 into 2 and park 2 THROUGH the coordinator.
  coord
    .prepare_merge(&2, now, &mut stores, &1, &NoFloors)
    .unwrap()
    .unwrap();
  {
    let (l, s) = stores.stores(&2).unwrap();
    coord.handle_storage(&2, now, l, s).unwrap();
  }
  assert!(coord.group(&2).unwrap().is_frozen());
  {
    let (l, s) = stores.stores(&1).unwrap();
    coord
      .commit_merge(&1, now, l, s, &2, &NoFloors)
      .unwrap()
      .unwrap();
    coord.handle_storage(&1, now, l, s).unwrap();
  }
  assert!(coord.group(&1).unwrap().pending_merge().is_some());

  // THE DOOR inherits the container gate verbatim — the new variants surface through the delegator.
  assert!(matches!(
    coord.remove_group(&2, &mut stores),
    Err(crate::RemoveError::Frozen)
  ));
  assert!(matches!(
    coord.remove_group(&1, &mut stores),
    Err(crate::RemoveError::MergeParked)
  ));
  // NO SIDE STATE: neither id was tombstoned, and both stay hosted for the retry.
  assert!(
    coord.group(&2).is_some() && !coord.is_retired(&2),
    "the frozen source is left intact and un-tombstoned"
  );
  assert!(
    coord.group(&1).is_some() && !coord.is_retired(&1),
    "the parked target is left intact and un-tombstoned"
  );
}

/// A forked group's manufactured hard state records its lineage FROM BIRTH — alongside the baseline
/// snapshot's token, never trailing it. Restart reconciliation compares the two, so a baseline written
/// with a token-less hard state would read as another lineage's log beside a token-bearing snapshot:
/// the exact ambiguity the record exists to remove. An untokened fork records `None` for the same
/// reason — the record is exact, not conservative.
#[test]
fn a_forked_groups_hard_state_records_its_lineage_from_birth() {
  let mut c = MultiCoord::new();
  let now = Instant::ORIGIN;

  // A TOKEN-BEARING birth comes through the sealed relay door, which is the only path that carries
  // provenance now: the container hands the install its own minted token, and the caller-driven
  // door below installs token-less by construction.
  let mut engine: GroupEngine<u64, u64> = GroupEngine::new();
  let mut sc = SplitCoord::new();
  engine.add_group(100);
  sc.create_group(
    100,
    single_voter(1),
    now,
    1,
    SplitSm::default(),
    0,
    &NoFloors,
  )
  .unwrap();
  let d = sc.group(&100).unwrap().poll_timeout().unwrap();
  {
    let (l, st) = engine.stores(&100).unwrap();
    sc.handle_timeout(&100, d, l, st).unwrap();
  }
  settle_engine(&mut sc, &mut engine, &[100], d);
  for _ in 0..3 {
    let (l, st) = engine.stores(&100).unwrap();
    sc.submit_propose(&100, d, l, st, &Bytes::from_static(b"c"), &NoFloors)
      .unwrap()
      .unwrap();
    settle_engine(&mut sc, &mut engine, &[100], d);
  }
  {
    let (l, st) = engine.stores(&100).unwrap();
    sc.propose_split(
      &100,
      d,
      l,
      st,
      &300,
      0,
      Bytes::from_static(b"\x02"),
      &NoFloors,
    )
    .unwrap()
    .unwrap();
  }
  settle_engine(&mut sc, &mut engine, &[100], d);
  let (gen_after, split_at) = {
    let fork = sc
      .peek_yieldable_fork(&NoHold)
      .expect("the committed split relays");
    (fork.parent_gen_after(), fork.split_index())
  };
  // The token is a pure function of PUBLIC split coordinates, so the expectation is RE-DERIVED
  // here rather than taken from the container: parent id, lineage after the split, the split
  // entry's (index, term), and the child's id and incarnation.
  let split_term = {
    let (l, _) = engine.stores(&100).unwrap();
    crate::LogStore::term(l, split_at).expect("the split entry is in the parent's log")
  };
  let encoded = |gid: u64| {
    let mut v = Vec::new();
    crate::Data::encode(&gid, &mut v);
    bytes::Bytes::from(v)
  };
  let token = crate::ForkId::new(
    encoded(100),
    gen_after,
    split_at,
    split_term,
    encoded(300),
    0,
  );
  assert!(matches!(
    sc.install_yieldable_fork(&100, &300, &mut engine, now, 1),
    InstallOutcome::Installed { child: 300, .. }
  ));
  engine.flush();
  let (_, stable) = engine.stores(&300).unwrap();
  assert_eq!(
    stable.hard_state().lineage(),
    Some(&token),
    "the manufactured hard state carries the child's token"
  );
  assert_eq!(
    stable
      .snapshot()
      .expect("the baseline occupies the slot")
      .0
      .fork_id(),
    Some(&token),
    "hard state and baseline agree on the lineage"
  );

  let (mut log2, mut stable2) = (VecLog::default(), AsyncStable::default());
  c.create_group_from_fork(
    101,
    single_voter(1),
    now,
    1,
    CountSm::default(),
    fork_blob(3),
    None,
    1,
    0,
    &NoFloors,
    &mut log2,
    &mut stable2,
  )
  .unwrap();
  assert!(
    stable2.hard_state().lineage().is_none(),
    "and the caller-driven door records None — token-less by construction, exact rather than \
     conservative"
  );
}

/// THE GENERATION FENCE, below the floor: a frame stamped with a RETIRED incarnation's generation
/// is dropped at demux — the endpoint never sees it, the shared connection survives for the live
/// groups, and the observability counter records the drop. The receiver's group is otherwise
/// perfectly healthy: only the stamp distinguishes the husk's traffic from a live member's.
#[test]
fn a_below_floor_stamp_is_fenced_at_demux() {
  let mut w = World::new(&[100], &[100]);
  w.settle();
  // Retire incarnation 0 of group 100 on the receiver: anything below generation 3 is a husk.
  w.sb.floors.insert(100, 3);

  let before = w.b.group(&100).unwrap().term();
  let vote = Message::RequestVote(crate::RequestVote::new(
    Term::new(9),
    1u64,
    crate::Index::new(1),
    Term::new(1),
    false,
    false,
  ));
  let mut tag = Vec::new();
  crate::Data::encode(&100u64, &mut tag);
  let framed = crafted_frame_at(&tag, 2, &vote);
  w.b
    .handle_conn_data(ConnId(1), &framed, false, w.now, &mut w.sb);

  assert_eq!(w.b.fenced_frames_dropped(), 1, "the fence counted the drop");
  assert_eq!(
    w.b.group(&100).unwrap().term(),
    before,
    "a retired incarnation's term-9 vote never reached the endpoint"
  );
  assert_eq!(
    w.b.poll_conn_closed(),
    None,
    "fencing a frame never costs the shared connection"
  );
  assert_eq!(
    w.b.poll_unknown_group(),
    None,
    "a fenced frame is never a placement signal"
  );
}

/// EQUAL ADMITS (WIRE.md §6): a frame stamped exactly AT the receiver's floor is a live member's,
/// not a husk's, so it dispatches normally and the fence counts nothing. Rejecting equal would
/// reintroduce the membership-apply-time staleness the vote path warns about.
#[test]
fn an_at_floor_stamp_is_admitted() {
  let mut w = World::new(&[100], &[100]);
  w.settle();
  w.sb.floors.insert(100, 3);

  let vote = Message::RequestVote(crate::RequestVote::new(
    Term::new(9),
    1u64,
    crate::Index::new(1),
    Term::new(1),
    false,
    false,
  ));
  let mut tag = Vec::new();
  crate::Data::encode(&100u64, &mut tag);
  let framed = crafted_frame_at(&tag, 3, &vote);
  w.b
    .handle_conn_data(ConnId(1), &framed, false, w.now, &mut w.sb);

  assert_eq!(
    w.b.fenced_frames_dropped(),
    0,
    "at-floor is not below-floor"
  );
  assert_eq!(
    w.b.group(&100).unwrap().term(),
    Term::new(9),
    "the at-floor vote reached the endpoint and raised its term"
  );
}

/// SKEW IMMUNITY: the comparator is the RETIREMENT floor, not the receiver's current shape
/// generation, so a replica trailing a reshape stamps its own lower generation and still ADMITS at
/// an applied sibling — a live gid's floor does not move when its shape generation bumps. Without
/// this the fence would re-create the apply-time staleness bug: a mid-split straggler's campaign
/// would be silently swallowed by every peer that applied the split first.
#[test]
fn a_reshape_trailing_stamp_still_admits_at_an_applied_sibling() {
  let mut w = World::new(&[100], &[100]);
  w.settle();
  // The receiver has applied two reshapes (lineage 2); its floor for the LIVE gid stays 0.
  assert_eq!(
    w.sb.floor(&100),
    0,
    "a live gid's floor is unmoved by reshape"
  );

  let vote = Message::RequestVote(crate::RequestVote::new(
    Term::new(9),
    1u64,
    crate::Index::new(1),
    Term::new(1),
    false,
    false,
  ));
  let mut tag = Vec::new();
  crate::Data::encode(&100u64, &mut tag);
  // The sender trails by one: it stamps generation 1 while the sibling sits at 2.
  let framed = crafted_frame_at(&tag, 1, &vote);
  w.b
    .handle_conn_data(ConnId(1), &framed, false, w.now, &mut w.sb);

  assert_eq!(
    w.b.fenced_frames_dropped(),
    0,
    "a trailing shape generation is not a retired incarnation"
  );
  assert_eq!(
    w.b.group(&100).unwrap().term(),
    Term::new(9),
    "the trailing sibling's vote was delivered"
  );
}

/// The fence covers EVERY message class, not just votes: a retired incarnation's appends are dead
/// writes and its heartbeats are noise, and admitting either would leave the replication plane as
/// a stale-incarnation side channel. Heartbeats ride COALESCED entries, so this also pins the
/// per-entry stamp — the live group's entry in the same frame still dispatches.
#[test]
fn the_fence_covers_every_class_and_spares_the_frames_live_entries() {
  let mut w = World::new(&[100, 200], &[100, 200]);
  w.settle();
  w.sb.floors.insert(100, 3);
  let before = w.b.group(&100).unwrap().term();

  let beat = |gid: u64, generation: u64, payload: &mut Vec<u8>| {
    let hb = Message::Heartbeat(crate::Heartbeat::new(
      Term::new(9),
      1u64,
      Index::ZERO,
      Bytes::new(),
    ));
    let mut msg_bytes = Vec::new();
    crate::wire::encode_message(&hb, &mut msg_bytes);
    let mut gb = Vec::new();
    sailing_encode_u64(gid, &mut gb);
    crate::transport::frame::write_coalesced_entry(0, &gb, generation, &msg_bytes, payload);
  };
  let mut payload = Vec::new();
  crate::transport::frame::write_coalesced_marker(&mut payload);
  beat(100, 2, &mut payload); // the husk's beat — below the floor
  beat(200, 0, &mut payload); // a live group's beat in the SAME frame
  let mut framed = Vec::new();
  crate::transport::frame::encode_frame(&payload, &mut framed);
  w.b
    .handle_conn_data(ConnId(1), &framed, false, w.now, &mut w.sb);

  // An append from the same husk, on its own frame.
  let append = Message::AppendEntries(crate::AppendEntries::new(
    Term::new(9),
    1u64,
    Index::ZERO,
    Term::ZERO,
    std::vec![],
    Index::ZERO,
  ));
  let mut tag = Vec::new();
  crate::Data::encode(&100u64, &mut tag);
  let framed = crafted_frame_at(&tag, 2, &append);
  w.b
    .handle_conn_data(ConnId(1), &framed, false, w.now, &mut w.sb);

  assert_eq!(
    w.b.fenced_frames_dropped(),
    2,
    "the husk's heartbeat AND its append were both fenced"
  );
  assert_eq!(
    w.b.group(&100).unwrap().term(),
    before,
    "no class of the husk's traffic reached the endpoint"
  );
  assert_eq!(
    w.b.group(&200).unwrap().term(),
    Term::new(9),
    "the live group's entry in the fenced frame still dispatched"
  );
  assert_eq!(w.b.poll_conn_closed(), None, "the connection survives");
}

/// Drive a single-voter group on `c` to leadership, then settle its storage.
fn lead_split_group(c: &mut SplitCoord, g: u64, st: &mut Stores, now: Instant) -> Instant {
  let d = c.group(&g).unwrap().poll_timeout().unwrap().max(now);
  {
    let (l, s) = st.stores(&g).unwrap();
    c.handle_timeout(&g, d, l, s).unwrap();
  }
  settle_group(c, g, st, d);
  assert!(c.group(&g).unwrap().role().is_leader());
  d
}

/// Drain one group's storage completions until nothing is pending.
fn settle_group(c: &mut SplitCoord, g: u64, st: &mut Stores, d: Instant) {
  for _ in 0..40 {
    let (l, s) = st.stores(&g).unwrap();
    if !matches!(
      c.handle_storage(&g, d, l, s),
      Some(StorageProgress::MorePending)
    ) {
      break;
    }
  }
  let (l, s) = st.stores(&g).unwrap();
  let _ = c.handle_storage(&g, d, l, s);
}

/// Commit one command on a single-voter leader.
fn commit_one_on(c: &mut SplitCoord, g: u64, st: &mut Stores, d: Instant) {
  {
    let (l, s) = st.stores(&g).unwrap();
    c.submit_propose(&g, d, l, s, &Bytes::from_static(b"c"), &NoFloors)
      .unwrap()
      .unwrap();
  }
  settle_group(c, g, st, d);
}

/// A coordinator (node 2) parked in the DEBT WINDOW: group 1 carries a fork whose child id 200 is
/// already hosted — so its durability barrier stands — and group 2 then freezes into it, which
/// makes the resolve arm ABSORB and defer the union's capture as a debt. Returns the coordinator,
/// its stores, and the instant everything is driven at.
fn debt_window_coord() -> (SplitCoord, Stores, Instant) {
  let now = Instant::ORIGIN;
  let mut c = SplitCoord::new();
  let mut st = Stores {
    map: BTreeMap::new(),
    floors: BTreeMap::new(),
  };
  for g in [1u64, 2, 200] {
    st.map
      .insert(g, (VecLog::default(), AsyncStable::default()));
  }
  for g in [1u64, 2] {
    c.create_group(g, single_voter(2), now, 7, SplitSm::default(), 0, &NoFloors)
      .unwrap();
  }
  let d = lead_split_group(&mut c, 1, &mut st, now);
  for _ in 0..3 {
    commit_one_on(&mut c, 1, &mut st, d);
  }
  {
    let (l, s) = st.stores(&1).unwrap();
    c.propose_split(&1, d, l, s, &200, 0, Bytes::from_static(b"\x02"), &NoFloors)
      .expect("the parent is hosted")
      .expect("the leader appends the split");
  }
  // The squatter is admitted through the CONTAINER, past this coordinator's split reservation.
  // The reservation only ever narrows the LOCAL window (its doc says so): the reachable
  // production shape is a replica that already hosted the child id when the committed split
  // ARRIVED, which no local reservation can see. Everything downstream — the apply, the fork's
  // park, the barrier it leaves standing — is the ordinary path.
  c.multi
    .create_group(200, 0, single_voter(2), now, 43, SplitSm::default())
    .unwrap();
  settle_group(&mut c, 1, &mut st, d);
  assert!(
    c.peek_yieldable_fork(&NoHold).is_none(),
    "the fork parks on the hosted child, leaving its barrier standing"
  );
  assert_eq!(c.poll_split_conflict(), Some((1, 200)));

  let ds = lead_split_group(&mut c, 2, &mut st, now);
  commit_one_on(&mut c, 2, &mut st, ds);
  c.prepare_merge(&2, ds, &mut st, &1, &NoFloors)
    .unwrap()
    .unwrap();
  settle_group(&mut c, 2, &mut st, ds);
  assert!(c.group(&2).unwrap().is_frozen());
  {
    let (l, s) = st.stores(&1).unwrap();
    c.commit_merge(&1, d, l, s, &2, &NoFloors).unwrap().unwrap();
  }
  settle_group(&mut c, 1, &mut st, d);
  assert!(c.group(&1).unwrap().pending_merge().is_some(), "parked");

  // The first pass seals the park's abort window; the drain commits the seal; the next resolves.
  assert!(c.service_merge_applies(d, &mut st).is_empty());
  settle_group(&mut c, 1, &mut st, d);
  assert_eq!(
    c.service_merge_applies(d, &mut st),
    std::vec![crate::MergeResolution::Absorbed {
      source: 2,
      target: 1
    }],
    "the standing fork barrier defers the capture into a debt"
  );
  assert!(c.debt_names(&2), "the debt names its consumed source");
  (c, st, d)
}

/// The demux fence covers the debt window exactly as it covers a tombstone, and — like the
/// tombstone check — it sits BEFORE store resolution, so the outcome does not depend on the
/// embedder's `GroupStores` seam. The consumed source's id is NOT retired (its floor moves only at
/// the discharge), yet every frame addressed to it is equally moot: the endpoint is gone and the
/// preserved stores are the union's restart derivation, not a group.
///
/// Both seam shapes are asserted because they fail differently without the fence. With the source
/// still store-resolvable (the drivers' posture — `Absorbed` deliberately keeps the stores and the
/// engine record), the frame already dies as an unhosted-group dispatch. With the source absent
/// from the seam, the frame reaches the initial-shaped arm, and an `UnknownGroup` advisory there
/// would prompt the embedder — or a factory — to revive a husk beside the absorbed union. The
/// fence is what makes both shapes silent, and neither ever closes the shared connection or
/// disturbs the absorbing target.
#[test]
fn the_demux_fence_drops_a_debt_named_sources_frames_without_signalling() {
  let (mut b, mut sb, d) = debt_window_coord();
  assert!(
    !b.is_retired(&2),
    "an absorbed-pending-capture source is NOT tombstoned — the fence is the debt itself"
  );

  // A peer that still believes group 2 lives: node 1 dials, node 2 accepts, and the label
  // handshake completes so the frames below arrive authenticated.
  let mut a = SplitCoord::new();
  let mut sa = Stores {
    map: BTreeMap::new(),
    floors: BTreeMap::new(),
  };
  a.create_group(
    2,
    single_voter(1),
    Instant::ORIGIN,
    9,
    SplitSm::default(),
    0,
    &NoFloors,
  )
  .unwrap();
  sa.map
    .insert(2, (VecLog::default(), AsyncStable::default()));
  let ca = a.on_dial_open(2, label(1, true), Instant::ORIGIN);
  let cb = b.on_accept_open(label(2, false), Instant::ORIGIN);
  assert_eq!(ca, cb);
  for _ in 0..20 {
    let mut moved = false;
    for (_, bytes) in a.poll_transmit() {
      if !bytes.is_empty() {
        b.handle_conn_data(ConnId(1), &bytes, false, d, &mut sb);
        moved = true;
      }
    }
    for (_, bytes) in b.poll_transmit() {
      if !bytes.is_empty() {
        a.handle_conn_data(ConnId(1), &bytes, false, d, &mut sa);
        moved = true;
      }
    }
    if !moved {
      break;
    }
  }
  assert_eq!(b.conn_of(&1), Some(ConnId(1)), "the peer is authenticated");
  assert_eq!(
    b.poll_unknown_group(),
    None,
    "the handshake alone signals nothing"
  );

  // Initial-shaped traffic for the debt-named id: the one shape that mints an advisory.
  let mut tag = Vec::new();
  sailing_encode_u64(2, &mut tag);
  let rv = Message::RequestVote(crate::RequestVote::new(
    Term::new(9),
    1u64,
    Index::ZERO,
    Term::ZERO,
    false,
    false,
  ));
  let framed = crafted_frame(&tag, &rv);
  let target_term = b.group(&1).unwrap().term();

  // Seam A: the source's stores are still resolvable, as the drivers keep them.
  assert!(sb.map.contains_key(&2));
  b.handle_conn_data(ConnId(1), &framed, false, d, &mut sb);
  b.handle_conn_data(ConnId(1), &framed, false, d, &mut sb);
  assert_eq!(b.poll_unknown_group(), None, "debt-named: silent");
  assert_eq!(b.poll_conn_closed(), None, "no close on the shared link");

  // Seam B: the source has fallen out of the store seam — the arm that would otherwise advertise
  // the id as unhosted-and-solicited, which is exactly the revival the fence exists to prevent.
  sb.map.remove(&2);
  b.handle_conn_data(ConnId(1), &framed, false, d, &mut sb);
  assert_eq!(
    b.poll_unknown_group(),
    None,
    "debt-named: silent, not unknown — an advisory here revives a husk"
  );
  assert_eq!(b.poll_conn_closed(), None, "still no close");
  assert_eq!(
    b.group(&1).unwrap().term(),
    target_term,
    "the absorbing target is untouched by its consumed source's stragglers"
  );
  assert!(b.group(&2).is_none(), "the source endpoint stays consumed");
}

/// The `Data`-encoded payload of a committed `Split` entry, as `propose_split` builds it.
fn split_payload_bytes(child: u64, parent_gen_after: u64, give: u8) -> Bytes {
  let mut child_bytes = Vec::new();
  crate::Data::encode(&child, &mut child_bytes);
  let payload = crate::SplitPayload::new(
    Bytes::from(child_bytes),
    0,
    parent_gen_after,
    Bytes::copy_from_slice(&[give]),
  );
  let mut buf = Vec::new();
  crate::wire::encode_split_payload(&payload, &mut buf);
  Bytes::from(buf)
}

/// A parent's crash image whose `Split` is DURABLE IN THE LOG but NOT YET COMMITTED: three
/// committed commands, then the split of `child` minted at parent generation 1 sitting ABOVE the
/// durable commit index. Every replica reaches this shape whenever an entry's append outlives the
/// hard-state write that would have recorded its commit — apply runs off the in-memory commit, and
/// only the durable one survives a crash.
fn parent_holding_an_uncommitted_split(child: u64) -> (VecLog, AsyncStable) {
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  let cmd = {
    let mut c = Vec::new();
    crate::Data::encode(&Bytes::from_static(b"c"), &mut c);
    Bytes::from(c)
  };
  log.force_append(&[
    crate::Entry::new(
      Term::new(1),
      Index::new(1),
      crate::EntryKind::Normal,
      cmd.clone(),
    ),
    crate::Entry::new(
      Term::new(1),
      Index::new(2),
      crate::EntryKind::Normal,
      cmd.clone(),
    ),
    crate::Entry::new(Term::new(1), Index::new(3), crate::EntryKind::Normal, cmd),
    crate::Entry::new(
      Term::new(1),
      Index::new(4),
      crate::EntryKind::Split,
      split_payload_bytes(child, 1, 2),
    ),
  ]);
  stable.force_state(Term::new(1), Some(1u64), Index::new(3));
  (log, stable)
}

/// A `Split` that was durable but UNCOMMITTED when the host crashed still partitions the state
/// machine when it commits after the reopen — at the generation it was minted with, exactly as it
/// does on every replica that never crashed.
///
/// The apply-time lineage guard admits the entry only at `shape_gen + 1` exactly. This replica
/// reopens with a lineage record its replay cannot account for (the record was flushed with the
/// child's baseline; the commit-index write was not), so an admitted generation or a record folded
/// into the live counter would put it one comparison past the retained entry: the entry would no-op
/// while every replica that stayed up partitioned on it. The counter therefore reads the replay
/// evidence, and the retained entry lands.
#[test]
fn an_uncommitted_split_still_partitions_when_it_commits_after_a_reopen() {
  struct Record(u64);
  impl FloorStore<u64> for Record {
    fn floor(&self, _: &u64) -> u64 {
      0
    }

    fn lineage(&self, _: &u64) -> u64 {
      self.0
    }
  }
  let now = Instant::ORIGIN;
  let (mut log, mut stable) = parent_holding_an_uncommitted_split(300);
  let mut c = SplitCoord::new();
  // The catalog has moved this id to generation 1 — the split committed elsewhere — while this
  // host's own fork record still reads 0: it crashed before materializing anything.
  c.restore_group(
    100,
    single_voter(1),
    now,
    1,
    SplitSm::default(),
    1,
    1,
    &Record(0),
    &mut log,
    &mut stable,
  )
  .expect("the reopen is admitted above the record");

  assert_eq!(
    c.group(&100).unwrap().state_machine().units,
    3,
    "replay stops below the uncommitted split, so the FSM still holds all three units"
  );
  assert_eq!(
    c.group(&100).unwrap().shape_gen(),
    0,
    "and the live counter reads that replay, not the generation the reopen was admitted at"
  );

  // The replica rejoins and the retained entry COMMITS: a fresh term's own entry carries the
  // stale-term tail past the commit point with it.
  let d = c.group(&100).unwrap().poll_timeout().unwrap();
  c.handle_timeout(&100, d, &mut log, &mut stable).unwrap();
  while matches!(
    c.handle_storage(&100, d, &mut log, &mut stable),
    Some(StorageProgress::MorePending)
  ) {}

  assert_eq!(
    c.group(&100).unwrap().shape_gen(),
    1,
    "the committed split applied at the generation it was minted with"
  );
  assert_eq!(
    c.group(&100).unwrap().state_machine().units,
    1,
    "the state machine PARTITIONED: three units less the two it gave away"
  );
  {
    let fork = c
      .peek_yieldable_fork(&NoHold)
      .expect("the applied split staged its fork for the relay");
    assert_eq!(((*fork.child()), fork.parent_gen_after()), (300, 1));
  }

  // THE OTHER LEG. The same reopen on a host whose fork record DOES cover this split — it
  // materialized the child before the crash, and only the commit-index write was lost. The entry
  // still applies (the apply guard reads the replay, which is identical on both hosts), and the
  // re-staged fork now folds as the duplicate it is rather than re-materializing over the child's
  // real durable progress.
  let (mut log, mut stable) = parent_holding_an_uncommitted_split(300);
  let mut c = SplitCoord::new();
  c.restore_group(
    100,
    single_voter(1),
    now,
    1,
    SplitSm::default(),
    1,
    1,
    &Record(1),
    &mut log,
    &mut stable,
  )
  .expect("the reopen is admitted at the record");
  let d = c.group(&100).unwrap().poll_timeout().unwrap();
  c.handle_timeout(&100, d, &mut log, &mut stable).unwrap();
  while matches!(
    c.handle_storage(&100, d, &mut log, &mut stable),
    Some(StorageProgress::MorePending)
  ) {}
  assert_eq!(
    c.group(&100).unwrap().state_machine().units,
    1,
    "the committed split partitioned this replica identically"
  );
  assert!(
    c.peek_yieldable_fork(&NoHold).is_none(),
    "and its fork folded against the record that already covers it"
  );
}

/// The group-header incarnation stamp on every outbound frame reads the group's REPLICATED
/// evidence. The receiver compares it against that id's admission floor — a retirement fact — so a
/// stamp lifted above the sender's own evidence would have a reopened replica speak for
/// generations its stores do not hold, which is the identity collapse the fence exists to refuse.
#[test]
fn the_group_header_stamp_reads_the_replicated_evidence() {
  struct Record(u64);
  impl FloorStore<u64> for Record {
    fn floor(&self, _: &u64) -> u64 {
      0
    }

    fn lineage(&self, _: &u64) -> u64 {
      self.0
    }
  }
  // The peer link is bound and its handshake settled before the group is reopened, so every frame
  // captured below is this group's own tagged traffic.
  let mut w = World::new(&[], &[100]);
  w.settle();
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  stable.submit_write(
    crate::OpId::new(1),
    crate::HardState::initial().with_term(Term::new(1)),
  );
  w.a
    .restore_group(
      100,
      two_voter(1),
      w.now,
      1,
      CountSm::default(),
      1,
      9,
      &Record(7),
      &mut log,
      &mut stable,
    )
    .expect("an above-record restore is admitted");
  w.sa.map.insert(100, (log, stable));
  assert_eq!(
    w.a.group(&100).unwrap().shape_gen(),
    0,
    "the replay evidence is zero"
  );

  // Campaign, capturing what leaves before the peer ever answers.
  let mut frames = Vec::new();
  for _ in 0..40 {
    let d = w.a.group(&100).unwrap().poll_timeout().unwrap();
    w.now = w.now.max(d);
    let now = w.now;
    {
      let (l, s) = w.sa.stores(&100).unwrap();
      w.a.handle_timeout(&100, now, l, s).unwrap();
    }
    for _ in 0..8 {
      let (l, s) = w.sa.stores(&100).unwrap();
      let _ = w.a.handle_storage(&100, now, l, s);
    }
    frames.extend(transmit_frames(&w.a.poll_transmit()));
    if !frames.is_empty() {
      break;
    }
  }
  let mut stamps = Vec::new();
  for f in &frames {
    let bytes = bytes::Bytes::copy_from_slice(f);
    let tagged = if crate::transport::frame::is_coalesced_frame(f) {
      crate::transport::frame::split_coalesced(bytes)
        .expect("well-formed coalesced payload")
        .into_iter()
        .map(|(_, group, generation, _)| (group, generation))
        .collect::<Vec<_>>()
    } else {
      let (group, generation, _) =
        crate::transport::frame::split_group_header(bytes).expect("well-formed group header");
      std::vec![(group, generation)]
    };
    for (group, generation) in tagged {
      assert_eq!(u64::decode_exact(group).expect("u64 tag"), 100);
      stamps.push(generation);
    }
  }
  assert!(
    !stamps.is_empty(),
    "the campaigning group must have emitted at least one tagged frame"
  );
  for s in stamps {
    assert_eq!(
      s, 7,
      "the stamp is the group's own evidence — its durable fork record here — never the 9 the \
       catalog claimed at admission"
    );
  }
}

/// One floor for one id, zero for the rest — so each participant's leg can be fenced alone.
struct FloorFor(u64, u64);

impl FloorStore<u64> for FloorFor {
  fn floor(&self, gid: &u64) -> u64 {
    if *gid == self.0 { self.1 } else { 0 }
  }

  fn lineage(&self, _gid: &u64) -> u64 {
    0
  }
}

/// A frozen source (2) and the target (1) whose log carries the abort, both single-voter leaders —
/// the posture every abort leg below starts from.
fn a_frozen_source_and_its_target() -> (
  MultiStreamCoordinator<u64, u64, CountSm, TestRecord>,
  Stores,
) {
  let mut coord = MultiStreamCoordinator::<u64, u64, CountSm, TestRecord>::new();
  let mut stores = Stores {
    map: BTreeMap::new(),
    floors: BTreeMap::new(),
  };
  for gid in [1u64, 2] {
    stores
      .map
      .insert(gid, (VecLog::default(), AsyncStable::default()));
    coord
      .create_group(
        gid,
        single_voter(1),
        Instant::ORIGIN,
        1,
        CountSm::default(),
        0,
        &NoFloors,
      )
      .unwrap();
    let d = coord.group(&gid).unwrap().poll_timeout().unwrap();
    {
      let (l, s) = stores.stores(&gid).unwrap();
      coord.handle_timeout(&gid, d, l, s).unwrap();
    }
    for _ in 0..2 {
      let (l, s) = stores.stores(&gid).unwrap();
      coord.handle_storage(&gid, d, l, s).unwrap();
    }
    assert!(coord.group(&gid).unwrap().role().is_leader());
  }
  let now = Instant::ORIGIN;
  coord
    .prepare_merge(&2, now, &mut stores, &1, &NoFloors)
    .unwrap()
    .unwrap();
  {
    let (l, s) = stores.stores(&2).unwrap();
    coord.handle_storage(&2, now, l, s).unwrap();
  }
  assert!(coord.group(&2).unwrap().is_frozen(), "the source is frozen");
  (coord, stores)
}

/// Append the abort on target 1 under `floors`, apply it, and answer whether the target now records
/// the source-side thaw obligation. The obligation is the load-bearing observation: an abort that
/// appended but recorded nothing would leave the source frozen with no authorization to thaw it.
fn abort_and_apply(
  coord: &mut MultiStreamCoordinator<u64, u64, CountSm, TestRecord>,
  stores: &mut Stores,
  floors: &impl FloorStore<u64>,
) -> (Result<Index, crate::MergeError<u64>>, bool, bool) {
  let now = Instant::ORIGIN;
  let before = stores.map.get(&1).unwrap().0.last_index();
  let verdict = {
    let (l, s) = stores.stores(&1).unwrap();
    coord.rollback_merge(&1, now, l, s, &2, floors).unwrap()
  };
  if verdict.is_ok() {
    // Persist, commit, apply — the abort must be APPLIED before the target records the debt.
    for _ in 0..3 {
      let (l, s) = stores.stores(&1).unwrap();
      coord.handle_storage(&1, now, l, s).unwrap();
    }
  }
  let after = stores.map.get(&1).unwrap().0.last_index();
  let owed = coord.group(&1).is_some_and(|ep| ep.has_abandoned());
  (verdict, after != before, owed)
}

/// THE ABORT ANSWERS TO THE FLOOR OF THE LOG THAT CARRIES IT, AND ONLY THAT. Its sibling merge
/// verbs gate on both participants; the abort is the merge's RELEASE VALVE — the entry it appends
/// CREATES the source-side thaw obligation, and that committed obligation is the sole authorization
/// for the thaw. Nothing else ever unfreezes the source and there is no timeout behind it, so a
/// source-side floor leg here would strand a frozen source permanently: the merge-freeze wedge the
/// thaw-obligation discharge fences its own floor leg off a frozen source to avoid, arriving one
/// step earlier — at propose, where the obligation would never be created at all.
///
/// A NON-TERMINAL floor on the frozen source: the valve opens, and the obligation assert is the
/// load-bearing half — an abort that appended but recorded nothing would leave the source frozen
/// with nothing authorized to free it.
#[test]
fn the_abort_ignores_a_non_terminal_floor_on_its_frozen_source() {
  let (mut coord, mut stores) = a_frozen_source_and_its_target();
  let (verdict, appended, owed) = abort_and_apply(&mut coord, &mut stores, &FloorFor(2, 7));
  assert!(
    verdict.is_ok(),
    "a floored source must not close the valve, got {verdict:?}"
  );
  assert!(appended, "the abort rides the target's log");
  assert!(
    owed,
    "the appended abort must CREATE the thaw obligation — an append that records nothing leaves \
     the source frozen with nothing authorized to free it"
  );
}

/// The rule is TARGET-ONLY whatever the source floor's value: a source whose lineage resolved away
/// cluster-wide is still a frozen source until its thaw runs, so the terminal sentinel does not
/// close the valve either.
#[test]
fn the_abort_ignores_even_a_terminal_floor_on_its_source() {
  let (mut coord, mut stores) = a_frozen_source_and_its_target();
  let (verdict, appended, owed) =
    abort_and_apply(&mut coord, &mut stores, &FloorFor(2, MERGED_FLOOR));
  assert!(
    verdict.is_ok(),
    "the terminal sentinel on the SOURCE does not gate the abort, got {verdict:?}"
  );
  assert!(appended && owed, "the valve opens and records the debt");
}

/// The half that was right, kept: a floored TARGET still refuses. The abort rides the target's log,
/// so a target below its own floor cannot carry the entry at all — nothing is appended and no
/// obligation is recorded.
#[test]
fn the_abort_still_refuses_a_floored_target() {
  let (mut coord, mut stores) = a_frozen_source_and_its_target();
  let (verdict, appended, owed) = abort_and_apply(&mut coord, &mut stores, &FloorFor(1, 7));
  assert!(
    matches!(verdict, Err(crate::MergeError::BelowFloor { floor: 7 })),
    "a floored target is refused at propose, got {verdict:?}"
  );
  assert!(!appended, "a refused abort appends nothing");
  assert!(!owed, "and records no obligation");
}

/// Both participants clear: the pre-floor behaviour verbatim.
#[test]
fn an_unfloored_abort_appends_and_records_the_debt() {
  let (mut coord, mut stores) = a_frozen_source_and_its_target();
  let (verdict, appended, owed) = abort_and_apply(&mut coord, &mut stores, &NoFloors);
  assert!(
    verdict.is_ok(),
    "an unfloored world appends, got {verdict:?}"
  );
  assert!(appended && owed);
}

/// FOLLOW-THROUGH, END TO END: the valve the propose gate keeps open actually frees the source. A
/// source frozen under a floor that no longer admits its incarnation is aborted, and the relayed
/// source-side thaw UNFREEZES it — the outcome a source-side floor leg at propose would have made
/// unreachable forever.
#[test]
fn the_valve_thaws_a_source_frozen_under_a_floored_lineage() {
  let (mut coord, mut stores) = a_frozen_source_and_its_target();
  let now = Instant::ORIGIN;
  // The world itself floors the source's lineage — the same fact the propose gate must not act on.
  stores.floors.insert(2, 7);

  let (verdict, appended, owed) = abort_and_apply(&mut coord, &mut stores, &FloorFor(2, 7));
  assert!(verdict.is_ok() && appended && owed);
  assert!(
    coord.group(&2).unwrap().is_frozen(),
    "the source is still frozen — only the thaw frees it"
  );

  // Drive the service passes that relay the obligation to the source and let it apply.
  for _ in 0..4 {
    let _ = coord.service_merge_applies(now, &mut stores);
    for gid in [1u64, 2] {
      if let Some((l, s)) = stores.stores(&gid) {
        coord.handle_storage(&gid, now, l, s).unwrap();
      }
    }
  }
  assert!(
    coord.group(&2).is_some_and(|ep| !ep.is_frozen()),
    "the relayed thaw UNFREEZES the source — the valve works end to end"
  );
}

/// A TERMINALLY FLOORED GROUP REFUSES PROPOSALS INSTEAD OF TIMING OUT. Every ordinary propose verb
/// on a husk replicates toward a quorum that went with the incarnation the floor buried, so the
/// caller's only signal is a timeout it cannot distinguish from a slow leader. The floor is the
/// truthful answer and it is already durable, so the refusal costs one comparison.
#[test]
fn the_propose_verbs_refuse_a_floored_group() {
  let mut coord = MultiStreamCoordinator::<u64, u64, CountSm, TestRecord>::new();
  let mut stores = Stores {
    map: BTreeMap::new(),
    floors: BTreeMap::new(),
  };
  stores
    .map
    .insert(1u64, (VecLog::default(), AsyncStable::default()));
  coord
    .create_group(
      1,
      single_voter(1),
      Instant::ORIGIN,
      1,
      CountSm::default(),
      0,
      &NoFloors,
    )
    .unwrap();
  let d = coord.group(&1).unwrap().poll_timeout().unwrap();
  {
    let (l, s) = stores.stores(&1).unwrap();
    coord.handle_timeout(&1, d, l, s).unwrap();
  }
  for _ in 0..2 {
    let (l, s) = stores.stores(&1).unwrap();
    coord.handle_storage(&1, d, l, s).unwrap();
  }
  assert!(coord.group(&1).unwrap().role().is_leader());
  let now = Instant::ORIGIN;
  let before = stores.map.get(&1).unwrap().0.last_index();

  // Each verb, against the terminal sentinel: the typed refusal, and nothing appended.
  macro_rules! refused {
    ($call:expr) => {{
      let verdict = $call;
      assert!(
        matches!(
          verdict,
          Some(Err(ProposeError::BelowFloor {
            floor: MERGED_FLOOR
          }))
        ),
        "expected a typed floor refusal, got {verdict:?}"
      );
      assert_eq!(
        stores.map.get(&1).unwrap().0.last_index(),
        before,
        "a refused proposal appends nothing"
      );
    }};
  }
  {
    let (l, s) = stores.stores(&1).unwrap();
    refused!(coord.submit_propose(&1, now, l, s, &Bytes::from_static(b"c"), &MaxFloors));
  }
  {
    let (l, s) = stores.stores(&1).unwrap();
    refused!(coord.submit_propose_deferred(&1, now, l, s, &Bytes::from_static(b"c"), &MaxFloors));
  }
  {
    let (l, s) = stores.stores(&1).unwrap();
    refused!(coord.propose_conf_change(
      &1,
      now,
      l,
      s,
      crate::ConfChange::new(crate::ConfChangeType::AddNode, 2u64, Bytes::new()),
      &MaxFloors
    ));
  }
  {
    let (l, s) = stores.stores(&1).unwrap();
    refused!(coord.propose_conf_change_v2(
      &1,
      now,
      l,
      s,
      crate::ConfChange::new(crate::ConfChangeType::AddNode, 2u64, Bytes::new()).into_v2(),
      &MaxFloors
    ));
  }
  {
    let (l, s) = stores.stores(&1).unwrap();
    refused!(coord.propose_read_mode_change(
      &1,
      now,
      l,
      s,
      crate::ReadOnlyOption::LeaseBased,
      &MaxFloors
    ));
  }

  // THE NON-REFUSAL BOUNDARY. A live group with no floor at all, and one whose floor it clears,
  // both reach the container — the entry is appended, which is what a false positive here would
  // have cost.
  {
    let (l, s) = stores.stores(&1).unwrap();
    coord
      .submit_propose(&1, now, l, s, &Bytes::from_static(b"c"), &NoFloors)
      .unwrap()
      .expect("an unfloored group proposes");
  }
  {
    let (l, s) = stores.stores(&1).unwrap();
    coord
      .submit_propose(&1, now, l, s, &Bytes::from_static(b"c"), &FloorFor(1, 0))
      .unwrap()
      .expect("a group at floor zero proposes");
  }
  assert!(
    stores.map.get(&1).unwrap().0.last_index() > before,
    "the admitted proposals really were appended"
  );
}

/// THE RECREATED ID IS THE FALSE-POSITIVE BOUNDARY. A group re-admitted ABOVE the floor that
/// fenced its predecessor is exactly what the floor exists to permit, and refusing it would make
/// the fence permanent rather than incarnation-scoped. Its generation clears the floor, so every
/// verb must reach the container.
#[test]
fn a_recreated_group_above_its_floor_still_proposes() {
  let mut coord = MultiStreamCoordinator::<u64, u64, CountSm, TestRecord>::new();
  let mut stores = Stores {
    map: BTreeMap::new(),
    floors: BTreeMap::new(),
  };
  stores
    .map
    .insert(1u64, (VecLog::default(), AsyncStable::default()));
  let (founding_log, mut founding_stable) = (VecLog::default(), AsyncStable::default());
  coord
    .create_group_founded_at(
      1,
      single_voter(1),
      Instant::ORIGIN,
      1,
      CountSm::default(),
      2,
      &FloorFor(1, 2),
      1,
      &founding_log,
      &mut founding_stable,
    )
    .expect("a recreation at the floor is admitted");
  let d = coord.group(&1).unwrap().poll_timeout().unwrap();
  {
    let (l, s) = stores.stores(&1).unwrap();
    coord.handle_timeout(&1, d, l, s).unwrap();
  }
  for _ in 0..2 {
    let (l, s) = stores.stores(&1).unwrap();
    coord.handle_storage(&1, d, l, s).unwrap();
  }
  let now = Instant::ORIGIN;
  let before = stores.map.get(&1).unwrap().0.last_index();
  {
    let (l, s) = stores.stores(&1).unwrap();
    let verdict = coord
      .submit_propose(&1, now, l, s, &Bytes::from_static(b"c"), &FloorFor(1, 2))
      .unwrap();
    assert!(
      !matches!(verdict, Err(ProposeError::BelowFloor { .. })),
      "a generation at its floor is admitted, not fenced: {verdict:?}"
    );
  }
  assert!(
    stores.map.get(&1).unwrap().0.last_index() > before,
    "the recreated incarnation's proposal was appended"
  );
}

/// The split delegator fences its PARENT as well as its child. The child leg judges the caller's
/// claim about a new id; a husk parent proposing a split replicates that entry toward the quorum
/// its own floor buried, which is the ordinary doomed proposal wearing a different verb.
#[test]
fn propose_split_refuses_a_floored_parent() {
  let mut coord = MultiStreamCoordinator::<u64, u64, CountSm, TestRecord>::new();
  let mut stores = Stores {
    map: BTreeMap::new(),
    floors: BTreeMap::new(),
  };
  stores
    .map
    .insert(1u64, (VecLog::default(), AsyncStable::default()));
  coord
    .create_group(
      1,
      single_voter(1),
      Instant::ORIGIN,
      1,
      CountSm::default(),
      0,
      &NoFloors,
    )
    .unwrap();
  let d = coord.group(&1).unwrap().poll_timeout().unwrap();
  {
    let (l, s) = stores.stores(&1).unwrap();
    coord.handle_timeout(&1, d, l, s).unwrap();
  }
  for _ in 0..2 {
    let (l, s) = stores.stores(&1).unwrap();
    coord.handle_storage(&1, d, l, s).unwrap();
  }
  let now = Instant::ORIGIN;
  let before = stores.map.get(&1).unwrap().0.last_index();
  {
    let (l, s) = stores.stores(&1).unwrap();
    let verdict = coord
      .propose_split(
        &1,
        now,
        l,
        s,
        &2,
        0,
        bytes::Bytes::from_static(b"i"),
        &FloorFor(1, MERGED_FLOOR),
      )
      .unwrap();
    assert!(
      matches!(
        verdict,
        Err(crate::SplitError::Propose(ProposeError::BelowFloor {
          floor: MERGED_FLOOR
        }))
      ),
      "a fenced PARENT is the parent's own propose failure — the shape that says reroute or \
       retire it, not raise the child generation. Got {verdict:?}"
    );
  }
  assert_eq!(stores.map.get(&1).unwrap().0.last_index(), before);
}

/// AN ID THIS HOST DOES NOT RUN ANSWERS "NOT MINE", whatever floor it left behind. A floor outlives
/// the incarnation it fenced, so a host that once buried an id still holds a terminal value for it;
/// reading that before establishing placement would answer an unhosted id with a refusal where the
/// contract says `None`, and callers discriminating placement from ownership would then depend on
/// which ids this host happened to have retired.
#[test]
fn the_propose_verbs_answer_none_for_an_id_this_host_does_not_run() {
  let mut coord = MultiStreamCoordinator::<u64, u64, CountSm, TestRecord>::new();
  let mut stores = Stores {
    map: BTreeMap::new(),
    floors: BTreeMap::new(),
  };
  // Storage exists for the id; the COORDINATOR does not host it, which is the shape a resolved
  // merge or a torn-down group leaves behind.
  stores
    .map
    .insert(9u64, (VecLog::default(), AsyncStable::default()));
  let now = Instant::ORIGIN;
  let (l, s) = stores.stores(&9).unwrap();
  assert!(
    coord
      .submit_propose(&9, now, l, s, &Bytes::from_static(b"c"), &MaxFloors)
      .is_none()
  );
  assert!(
    coord
      .submit_propose_deferred(&9, now, l, s, &Bytes::from_static(b"c"), &MaxFloors)
      .is_none()
  );
  assert!(
    coord
      .propose_conf_change(
        &9,
        now,
        l,
        s,
        crate::ConfChange::new(crate::ConfChangeType::AddNode, 2u64, Bytes::new()),
        &MaxFloors
      )
      .is_none()
  );
  assert!(
    coord
      .propose_conf_change_v2(
        &9,
        now,
        l,
        s,
        crate::ConfChange::new(crate::ConfChangeType::AddNode, 2u64, Bytes::new()).into_v2(),
        &MaxFloors
      )
      .is_none()
  );
  assert!(
    coord
      .propose_read_mode_change(&9, now, l, s, crate::ReadOnlyOption::LeaseBased, &MaxFloors)
      .is_none()
  );
}

/// THE SPLIT'S FALSE-REJECTION BOUNDARY. A child recreated above a nonzero floor is exactly what a
/// floor permits, and the parent proposing that split is live at a lower generation. The two legs
/// read DIFFERENT ids, so a seam that answers one floor for both fences a healthy parent behind
/// its child's history — the split must be admitted and the entry appended.
#[test]
fn propose_split_admits_a_live_parent_under_a_recreated_child() {
  let mut coord = MultiStreamCoordinator::<u64, u64, CountSm, TestRecord>::new();
  let mut stores = Stores {
    map: BTreeMap::new(),
    floors: BTreeMap::new(),
  };
  stores
    .map
    .insert(1u64, (VecLog::default(), AsyncStable::default()));
  coord
    .create_group(
      1,
      single_voter(1),
      Instant::ORIGIN,
      1,
      CountSm::default(),
      0,
      &NoFloors,
    )
    .unwrap();
  let d = coord.group(&1).unwrap().poll_timeout().unwrap();
  {
    let (l, s) = stores.stores(&1).unwrap();
    coord.handle_timeout(&1, d, l, s).unwrap();
  }
  for _ in 0..2 {
    let (l, s) = stores.stores(&1).unwrap();
    coord.handle_storage(&1, d, l, s).unwrap();
  }
  let now = Instant::ORIGIN;
  let before = stores.map.get(&1).unwrap().0.last_index();
  {
    let (l, s) = stores.stores(&1).unwrap();
    coord
      .propose_split(
        &1,
        now,
        l,
        s,
        &2,
        7,
        bytes::Bytes::from_static(b"i"),
        &FloorFor(2, 7),
      )
      .unwrap()
      .expect("a live parent splits out a child recreated at its own floor");
  }
  assert!(
    stores.map.get(&1).unwrap().0.last_index() > before,
    "the split entry was appended"
  );
}

/// THE CHILD LEG KEEPS THE CHILD-SCOPED VARIANT. A claim below the child id's floor is cured by
/// raising `child_gen` or recreating the child above it, which is the opposite of what a fenced
/// parent needs — so the two refusals must not share a shape.
#[test]
fn propose_split_reports_a_floored_child_under_its_own_variant() {
  let mut coord = MultiStreamCoordinator::<u64, u64, CountSm, TestRecord>::new();
  let mut stores = Stores {
    map: BTreeMap::new(),
    floors: BTreeMap::new(),
  };
  stores
    .map
    .insert(1u64, (VecLog::default(), AsyncStable::default()));
  coord
    .create_group(
      1,
      single_voter(1),
      Instant::ORIGIN,
      1,
      CountSm::default(),
      0,
      &NoFloors,
    )
    .unwrap();
  let d = coord.group(&1).unwrap().poll_timeout().unwrap();
  {
    let (l, s) = stores.stores(&1).unwrap();
    coord.handle_timeout(&1, d, l, s).unwrap();
  }
  for _ in 0..2 {
    let (l, s) = stores.stores(&1).unwrap();
    coord.handle_storage(&1, d, l, s).unwrap();
  }
  let now = Instant::ORIGIN;
  let before = stores.map.get(&1).unwrap().0.last_index();
  {
    let (l, s) = stores.stores(&1).unwrap();
    // The parent is unfloored; only the child's claim is under its floor.
    let verdict = coord
      .propose_split(
        &1,
        now,
        l,
        s,
        &2,
        3,
        bytes::Bytes::from_static(b"i"),
        &FloorFor(2, 7),
      )
      .unwrap();
    assert!(
      matches!(verdict, Err(crate::SplitError::BelowFloor { floor: 7 })),
      "a below-floor child claim keeps the child-scoped variant, got {verdict:?}"
    );
  }
  assert_eq!(stores.map.get(&1).unwrap().0.last_index(), before);
}

/// THE FENCE QUERY IS THE GATED VERBS' OWN PREDICATE, asked without proposing. A caller that must
/// answer a placement question before dispatching needs the group's own state first, and hosting
/// is established before the floor is read — an id this host merely used to run raises no
/// objection, however terminal the floor it left behind.
#[test]
fn fenced_floor_reports_only_a_hosted_groups_own_fence() {
  let mut coord = MultiStreamCoordinator::<u64, u64, CountSm, TestRecord>::new();
  let mut stores = Stores {
    map: BTreeMap::new(),
    floors: BTreeMap::new(),
  };
  stores
    .map
    .insert(1u64, (VecLog::default(), AsyncStable::default()));
  coord
    .create_group(
      1,
      single_voter(1),
      Instant::ORIGIN,
      1,
      CountSm::default(),
      0,
      &NoFloors,
    )
    .unwrap();

  assert_eq!(
    coord.fenced_floor(&1, &MaxFloors),
    Some(MERGED_FLOOR),
    "a hosted group under a terminal floor reports it"
  );
  assert_eq!(
    coord.fenced_floor(&1, &NoFloors),
    None,
    "a hosted group that clears its floor raises no objection"
  );
  assert_eq!(
    coord.fenced_floor(&9, &MaxFloors),
    None,
    "an id this host does not run raises no objection, whatever floor it left behind"
  );
}
