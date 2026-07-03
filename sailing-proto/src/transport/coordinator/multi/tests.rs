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
