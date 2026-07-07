#![allow(missing_docs)]
//! Tier B — coordinator-tier scenarios: two real `MultiStreamCoordinator`s bridged by an
//! in-memory duplex byte pipe, pinning the lifecycle gates the pure-container Tier A world
//! cannot express (connection-level tombstones, quiesce group controls, the unknown-group
//! advisory). The Tier A `MultiWorld` models these semantics at its bus; these scenarios hold
//! the REAL coordinator code to them.
//!
//! The fixture mirrors the two-node World pattern of the proto crate's own multi-coordinator
//! tests, re-implemented here over the sim crate's stores and state machine (an integration
//! test cannot import another crate's test module, and the independent re-implementation is
//! the point: it pins the public API shape too). Only node 1 (`a`) ever fires timers, so
//! elections are deterministic — node 2 (`b`) grants and never campaigns.

use core::time::Duration;
use sailing_proto::{
  Config, ConnId, CreateGroupError, Data as _, GroupControl, GroupStores, Instant, LabelOptions,
  Labeled, MultiStreamCoordinator, NoFloors, Passthrough, Term,
};
use sailing_simulation::{LogSm, MemLog, MemStable};
use std::collections::BTreeMap;

type TestRecord = Labeled<Passthrough>;
type Coord = MultiStreamCoordinator<u64, u64, LogSm, TestRecord>;

struct Stores {
  map: BTreeMap<u64, (MemLog, MemStable<u64>)>,
}

impl Stores {
  fn fresh(&mut self, gid: u64) {
    self.map.insert(gid, (MemLog::new(), MemStable::new()));
  }
}

impl GroupStores<u64, MemLog, MemStable<u64>> for Stores {
  fn stores(&mut self, group: &u64) -> Option<(&mut MemLog, &mut MemStable<u64>)> {
    self.map.get_mut(group).map(|(l, s)| (l, s))
  }
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
    cluster: sailing_proto::ClusterId([1; 16]),
    local_id,
  };
  if role_dialer {
    Labeled::dialer(Passthrough::new(), &opts).unwrap()
  } else {
    Labeled::acceptor(Passthrough::new(), &opts).unwrap()
  }
}

/// A two-node multi-group world: node 1 (`a`) dials node 2 (`b`); ONE connection (id 1 on both
/// sides) carries every co-located group's traffic.
struct World {
  a: Coord,
  b: Coord,
  sa: Stores,
  sb: Stores,
  /// Every gid either side has ever hosted (the settle pump's iteration set).
  groups: Vec<u64>,
  now: Instant,
}

impl World {
  fn new(a_groups: &[u64], b_groups: &[u64]) -> Self {
    let mut a = Coord::new();
    let mut b = Coord::new();
    let mut sa = Stores {
      map: BTreeMap::new(),
    };
    let mut sb = Stores {
      map: BTreeMap::new(),
    };
    let mut groups = Vec::new();
    for &g in a_groups {
      a.create_group(
        g,
        two_voter(1),
        Instant::ORIGIN,
        1,
        LogSm::new(),
        0,
        &NoFloors,
      )
      .unwrap();
      sa.fresh(g);
      groups.push(g);
    }
    for &g in b_groups {
      b.create_group(
        g,
        two_voter(2),
        Instant::ORIGIN,
        2,
        LogSm::new(),
        0,
        &NoFloors,
      )
      .unwrap();
      sb.fresh(g);
      if !groups.contains(&g) {
        groups.push(g);
      }
    }
    let ca = a.on_conn_open(label(1, true), Instant::ORIGIN);
    let cb = b.on_conn_open(label(2, false), Instant::ORIGIN);
    assert_eq!(ca, cb, "first allocation on both sides");
    World {
      a,
      b,
      sa,
      sb,
      groups,
      now: Instant::ORIGIN,
    }
  }

  /// Note a gid created after construction so the settle pump drains its storage too.
  fn track(&mut self, gid: u64) {
    if !self.groups.contains(&gid) {
      self.groups.push(gid);
    }
  }

  /// Move all queued bytes across the pipe, draining every hosted group's storage on both
  /// sides, until quiescent.
  fn settle(&mut self) {
    for _ in 0..200 {
      for g in self.groups.clone() {
        if self.a.group(&g).is_some() {
          let now = self.now;
          let (l, s) = self.sa.stores(&g).unwrap();
          let _ = self.a.handle_storage(&g, now, l, s);
        }
        if self.b.group(&g).is_some() {
          let now = self.now;
          let (l, s) = self.sb.stores(&g).unwrap();
          let _ = self.b.handle_storage(&g, now, l, s);
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

  /// Propose `cmd` on `a`'s `group` (must be leader) and settle the round.
  fn propose_a(&mut self, group: u64, cmd: &[u8]) {
    let now = self.now;
    let cmd = bytes::Bytes::copy_from_slice(cmd);
    {
      let (l, s) = self.sa.stores(&group).unwrap();
      self
        .a
        .submit_propose(&group, now, l, s, &cmd)
        .unwrap()
        .unwrap();
    }
    self.settle();
  }

  fn drain_controls_a(&mut self) -> Vec<(u64, GroupControl)> {
    let mut out = Vec::new();
    while let Some(c) = self.a.poll_group_control() {
      out.push(c);
    }
    out
  }

  fn drain_controls_b(&mut self) -> Vec<(u64, GroupControl)> {
    let mut out = Vec::new();
    while let Some(c) = self.b.poll_group_control() {
      out.push(c);
    }
    out
  }
}

/// Whether `coord`'s applied log for `group` contains `cmd` (the committed-through-the-pipe
/// witness).
fn applied_contains(coord: &Coord, group: u64, cmd: &[u8]) -> bool {
  coord
    .group(&group)
    .is_some_and(|ep| ep.state_machine().applied().iter().any(|(_, c)| c == cmd))
}

/// Removal TOMBSTONES the group id at the coordinator: straggler frames drop SILENTLY — no
/// close (the shared connection carries the live groups' traffic), no group control, no
/// unknown-group signal — and creation refuses the id with `Retired` until an explicit
/// `clear_tombstone` consents; after clear + create, traffic reaches the fresh replica again.
/// The sim crate's independent re-pin of the tombstone-refuses-recreation class.
#[test]
fn tombstone_refusal_chain() {
  let mut w = World::new(&[100, 200], &[100, 200]);
  w.settle();
  w.elect_a(100);

  // Real load commits THROUGH the pipe on both sides before the removal.
  w.propose_a(100, b"pin");
  w.fire_a(100); // the follow-up beat carries the advanced commit to b
  assert!(applied_contains(&w.a, 100, b"pin"), "leader applied");
  assert!(applied_contains(&w.b, 100, b"pin"), "follower applied");

  assert!(!w.b.is_retired(&100), "a hosted group is not tombstoned");
  assert!(w.b.remove_group(&100, &mut w.sb).unwrap().is_some());
  assert!(w.b.is_retired(&100), "removal tombstones the id");
  let _ = w.drain_controls_b();

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
  assert_eq!(
    w.b.poll_unknown_group(),
    None,
    "tombstoned is silent, never unknown"
  );
  assert!(w.b.group(&100).is_none(), "the group stays removed");

  // The co-located group is untouched by the sibling's tombstone.
  w.elect_a(200);
  assert!(
    w.b.group(&200).unwrap().term() >= Term::new(1),
    "the sibling group's traffic still flows"
  );

  // A tombstoned id refuses re-admission until the explicit clear consents.
  assert_eq!(
    w.b
      .create_group(100, two_voter(2), w.now, 2, LogSm::new(), 0, &NoFloors)
      .unwrap_err(),
    CreateGroupError::Retired,
    "create refuses a tombstoned id"
  );
  assert!(w.b.is_retired(&100), "a refused admission lifts nothing");
  assert!(w.b.clear_tombstone(&100), "a tombstone existed");
  assert!(
    !w.b.is_retired(&100),
    "the explicit clear lifts the tombstone"
  );
  w.sb.fresh(100);
  w.b
    .create_group(100, two_voter(2), w.now, 2, LogSm::new(), 0, &NoFloors)
    .unwrap();
  w.fire_a(100);
  assert!(
    w.b.group(&100).unwrap().term() >= Term::new(1),
    "traffic flows to the re-created group"
  );
}

/// A quiescing leader's final round: `mark_quiescing` stamps the next beat, the receiver folds
/// to net-quiesced (Wake then Quiesce, in order), and the returning `HeartbeatResponse` is
/// ABSORBED at the leader — no `Wake`, or the quiesce could never settle. The absorb set is
/// EXACTLY that response: fresh replication traffic is wake-class on both sides (the
/// `AppendEntries` wakes the follower, its `AppendResponse` wakes the leader). The empty-append
/// arm of the wake class is pinned at the proto tier with crafted frames; this fixture pins the
/// naturally-arising kinds through the public API.
#[test]
fn quiesce_final_round_absorbs_only_the_response() {
  let mut w = World::new(&[100], &[100]);
  w.settle();
  w.elect_a(100);
  w.settle();
  let _ = w.drain_controls_a();
  let _ = w.drain_controls_b();

  // The final round: the flagged beat ships, the intent is consumed by the broadcast.
  w.a.mark_quiescing(&100);
  assert!(w.a.is_quiescing(&100));
  w.fire_a(100);
  assert!(
    !w.a.is_quiescing(&100),
    "the intent is consumed by the stamped broadcast"
  );
  assert_eq!(
    w.drain_controls_b(),
    std::vec![(100, GroupControl::Wake), (100, GroupControl::Quiesce)],
    "the beat wakes, then the flag quiesces — folding in order nets quiesced"
  );
  assert_eq!(
    w.a.poll_group_control(),
    None,
    "the HeartbeatResponse of the final round is absorbed — no Wake at the leader"
  );

  // Fresh replication is WAKE-class on both sides: the append wakes the quiesced follower and
  // the ack wakes the leader (the absorb set never grows past the heartbeat response).
  w.propose_a(100, b"wake");
  assert!(
    w.drain_controls_b().contains(&(100, GroupControl::Wake)),
    "an AppendEntries wakes the follower's group"
  );
  assert!(
    w.drain_controls_a().contains(&(100, GroupControl::Wake)),
    "an AppendResponse wakes the leader's group"
  );
}

/// The unknown-group advisory is gated on INITIAL-SHAPED kinds: a RequestVote for a gid this
/// host neither hosts nor has tombstoned surfaces exactly once via `poll_unknown_group`
/// (deduped until polled), while an AppendEntries for the SAME gid — the shape of a removed
/// group's delayed stragglers — surfaces nothing, so a stale frame can never prompt the
/// placement brain to resurrect a destroyed group.
#[test]
fn unknown_group_gate_is_initial_shaped() {
  let mut w = World::new(&[100, 300], &[100]);
  w.settle();
  assert_eq!(
    w.b.poll_unknown_group(),
    None,
    "the handshake alone signals nothing"
  );

  // Vote kind: a campaigns for 300 (b does not host it). Two rounds before the embedder polls
  // still surface ONE signal, keyed to the authenticated soliciting peer.
  w.fire_a(300);
  w.fire_a(300);
  assert_eq!(
    w.b.poll_unknown_group(),
    Some((300, 1)),
    "a RequestVote for an unhosted gid surfaces the advisory once"
  );
  assert_eq!(w.b.poll_unknown_group(), None, "deduped until polled");

  // Build the established-straggler shape for the SAME gid: admit 300 on b, elect and commit
  // through the pipe, then remove + clear on b — 300 is now neither hosted nor tombstoned
  // there, exactly the state a destroyed group's stragglers meet.
  w.sb.fresh(300);
  w.b
    .create_group(300, two_voter(2), w.now, 2, LogSm::new(), 0, &NoFloors)
    .unwrap();
  w.track(300);
  w.elect_a(300);
  w.propose_a(300, b"est");
  w.fire_a(300);
  assert!(applied_contains(&w.b, 300, b"est"), "established on b");
  assert!(w.b.remove_group(&300, &mut w.sb).unwrap().is_some());
  assert!(w.b.clear_tombstone(&300), "clear leaves 300 truly unknown");
  let _ = w.drain_controls_b();

  // The unaware leader replicates: the AppendEntries reaches b for a gid it no longer knows.
  // Non-initial ⇒ silent: no advisory, no control, no close.
  w.propose_a(300, b"straggler");
  assert_eq!(
    w.b.poll_unknown_group(),
    None,
    "an AppendEntries for the same gid surfaces nothing"
  );
  assert_eq!(w.b.poll_group_control(), None, "no control either");
  assert_eq!(w.b.poll_conn_closed(), None, "and the connection survives");

  // An established (commit > 0) heartbeat is equally non-initial: still silent.
  w.fire_a(300);
  assert_eq!(
    w.b.poll_unknown_group(),
    None,
    "an established heartbeat stays silent too"
  );
}
