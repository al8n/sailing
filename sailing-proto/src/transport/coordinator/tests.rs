use super::*;
use crate::{
  Data,
  testkit::{CountSm, NoopStable, VecLog},
  transport::{
    ClusterId,
    labeled::{LabelOptions, Labeled},
    passthrough::Passthrough,
  },
};
use core::time::Duration;
use std::vec::Vec;

type Rec = Labeled<Passthrough>;
type Coord = StreamCoordinator<u64, CountSm, Rec>;

const ELECTION: Duration = Duration::from_millis(100);
const HEARTBEAT: Duration = Duration::from_millis(30);

fn label(id: u64, role_dialer: bool) -> Rec {
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

fn coord(id: u64) -> Coord {
  let cfg = crate::Config::try_new(id, std::vec![1, 2], ELECTION, HEARTBEAT).unwrap();
  StreamCoordinator::new(cfg, Instant::ORIGIN, id, CountSm::default())
}

/// A two-node world: one connection pair (conn id 1 on both sides), driven by a manual clock.
struct World {
  a: Coord,
  b: Coord,
  la: VecLog,
  sa: NoopStable,
  lb: VecLog,
  sb: NoopStable,
  now: Instant,
}

impl World {
  fn new() -> Self {
    let mut a = coord(1);
    let mut b = coord(2);
    // Node 1 dials node 2; node 2 accepts. Each coordinator assigns its own ConnId; with a single
    // connection both counters yield the same first id.
    let ca = a.on_conn_open(label(1, true), Instant::ORIGIN);
    let cb = b.on_conn_open(label(2, false), Instant::ORIGIN);
    assert_eq!(ca, cb, "first allocation on both sides");
    World {
      a,
      b,
      la: VecLog::default(),
      sa: NoopStable::default(),
      lb: VecLog::default(),
      sb: NoopStable::default(),
      now: Instant::ORIGIN,
    }
  }

  /// Move all queued bytes across the wire, draining storage on both sides, until quiescent.
  fn settle(&mut self) {
    for _ in 0..200 {
      self.a.handle_storage(self.now, &mut self.la, &mut self.sa);
      self.b.handle_storage(self.now, &mut self.lb, &mut self.sb);
      let from_a = self.a.poll_transmit();
      let from_b = self.b.poll_transmit();
      let mut moved = false;
      for (_, bytes) in &from_a {
        if !bytes.is_empty() {
          self.b.handle_conn_data(
            ConnId(1),
            bytes,
            false,
            self.now,
            &mut self.lb,
            &mut self.sb,
          );
          moved = true;
        }
      }
      for (_, bytes) in &from_b {
        if !bytes.is_empty() {
          self.a.handle_conn_data(
            ConnId(1),
            bytes,
            false,
            self.now,
            &mut self.la,
            &mut self.sa,
          );
          moved = true;
        }
      }
      if !moved {
        break;
      }
    }
  }

  /// Advance to the earliest pending deadline across both nodes and fire only the node(s) actually
  /// due, then settle. Firing each node on its OWN (randomized) deadline — rather than both at once
  /// — breaks election symmetry, exactly as a real timer-driven cluster does.
  fn step(&mut self) {
    let da = self.a.poll_timeout();
    let db = self.b.poll_timeout();
    let next = match (da, db) {
      (Some(x), Some(y)) => x.min(y),
      (Some(x), None) => x,
      (None, Some(y)) => y,
      (None, None) => self.now + HEARTBEAT,
    };
    self.now = next;
    if da.is_some_and(|d| d <= self.now) {
      self.a.handle_timeout(self.now, &mut self.la, &mut self.sa);
    }
    if db.is_some_and(|d| d <= self.now) {
      self.b.handle_timeout(self.now, &mut self.lb, &mut self.sb);
    }
    self.settle();
  }

  fn a_is_leader(&self) -> bool {
    self.a.role().is_leader()
  }
}

#[test]
fn two_node_cluster_elects_a_leader_over_the_transport() {
  let mut w = World::new();
  w.settle(); // complete the label handshake first
  assert_eq!(w.a.conn_of(&2), Some(ConnId(1)), "node 1 bound peer 2");
  assert_eq!(w.b.conn_of(&1), Some(ConnId(1)), "node 2 bound peer 1");

  // Drive timers until a leader emerges.
  for _ in 0..40 {
    w.step();
    if w.a_is_leader() || w.b.role().is_leader() {
      break;
    }
  }
  assert!(
    w.a_is_leader() || w.b.role().is_leader(),
    "a leader emerged through the wire"
  );
}

#[test]
fn leader_replicates_a_proposal_over_the_transport() {
  let mut w = World::new();
  w.settle();
  for _ in 0..40 {
    w.step();
    if w.a_is_leader() || w.b.role().is_leader() {
      break;
    }
  }
  assert!(w.a_is_leader() || w.b.role().is_leader());

  // Propose on whichever node is leader; it must commit + apply on BOTH nodes.
  let cmd = bytes::Bytes::from_static(b"x");
  if w.a_is_leader() {
    w.a
      .submit_propose(w.now, &mut w.la, &w.sa, &cmd)
      .expect("propose");
  } else {
    w.b
      .submit_propose(w.now, &mut w.lb, &w.sb, &cmd)
      .expect("propose");
  }
  // Drive heartbeats/commit through to both state machines.
  for _ in 0..40 {
    w.step();
  }

  // Both state machines applied the one committed Normal entry (CountSm counts applies).
  assert!(w.a.state_machine().count() >= 1, "leader applied the entry");
  assert!(
    w.b.state_machine().count() >= 1,
    "follower applied the entry"
  );
}

/// `submit_propose` must replicate the accepted entry IMMEDIATELY. `settle()` only moves bytes and
/// drains storage (no `handle_timeout`, no `flush_appends`), so the entry reaches the follower ONLY if
/// `submit_propose` already fanned out the `AppendEntries` — the deferred-only variant would not.
#[test]
fn submit_propose_replicates_immediately_to_peers() {
  let mut w = World::new();
  w.settle();
  for _ in 0..40 {
    w.step();
    if w.a_is_leader() || w.b.role().is_leader() {
      break;
    }
  }
  assert!(w.a_is_leader() || w.b.role().is_leader());

  // Capture the FOLLOWER's pre-propose log tail (the election no-op already replicated via the
  // step() heartbeats), then propose on the leader and move bytes ONCE with no timer/pump.
  let cmd = bytes::Bytes::from_static(b"x");
  let (idx, before) = if w.a_is_leader() {
    let before = w.lb.last_index();
    let idx = w
      .a
      .submit_propose(w.now, &mut w.la, &w.sa, &cmd)
      .expect("propose on the leader");
    (idx, before)
  } else {
    let before = w.la.last_index();
    let idx = w
      .b
      .submit_propose(w.now, &mut w.lb, &w.sb, &cmd)
      .expect("propose on the leader");
    (idx, before)
  };
  // settle() = move queued bytes + drain storage; NO handle_timeout, NO flush_appends. The only
  // way the entry reaches the follower is the AppendEntries submit_propose queued itself.
  w.settle();

  let follower_last = if w.a_is_leader() {
    w.lb.last_index()
  } else {
    w.la.last_index()
  };
  assert!(
    follower_last > before,
    "submit_propose must emit the AppendEntries immediately — the follower's log did not advance \
     (before={before:?}, after={follower_last:?})"
  );
  assert!(
    follower_last >= idx,
    "the follower received the accepted entry index {idx:?} (log tail {follower_last:?})"
  );
}

/// The coordinator-assigned id counter must never silently wrap into reuse: u64
/// exhaustion is unreachable in practice, but a release-mode wrap would hand a LIVE id to a new
/// connection and break the tie-break's uniqueness assumption — so it is a checked panic.
#[test]
#[should_panic(expected = "connection id space exhausted")]
fn conn_id_exhaustion_panics_instead_of_wrapping() {
  let mut c = coord(1);
  c.next_conn_id = u64::MAX;
  // This open hands out ConnId(u64::MAX) and must refuse to wrap the successor.
  let _ = c.on_conn_open(label(1, true), Instant::ORIGIN);
}

/// The coordinator/driver counterpart of `flush_appends_is_idempotent_for_a_probe_peer`: the driver
/// calls `flush_appends` every crank, so a no-ack PROBE peer must be sent a staged append exactly once.
/// Forces the follower into a sustained `Probe`, stages a proposal via the deferred-propose path, and
/// flushes twice with no intervening timer or ack — the second flush must emit nothing, failing on the
/// un-gated code.
#[test]
fn coordinator_flush_appends_does_not_re_send_a_probe_peer_each_pump() {
  use crate::Index;
  let mut w = World::new();
  w.settle(); // complete the label handshake so peer↔conn is bound both ways
  assert_eq!(w.a.conn_of(&2), Some(ConnId(1)));

  // Elect a leader over the wire, then drive the proposal from WHICHEVER node won (the World's election
  // is randomized per seed). Bind `(leader, follower)` once so the rest is winner-agnostic.
  for _ in 0..40 {
    w.step();
    if w.a_is_leader() || w.b.role().is_leader() {
      break;
    }
  }
  assert!(w.a_is_leader() || w.b.role().is_leader());
  let leader_is_a = w.a_is_leader();
  let follower_id: u64 = if leader_is_a { 2 } else { 1 };

  // Borrow the leader coordinator + its log/stable, winner-agnostic.
  let (leader, llog, lstable): (&mut Coord, &mut VecLog, &NoopStable) = if leader_is_a {
    (&mut w.a, &mut w.la, &w.sa)
  } else {
    (&mut w.b, &mut w.lb, &w.sb)
  };

  // Force the follower into a sustained PROBE at the tail — a complete send leaves next_index UNMOVED,
  // the shape the dirty flag must guard. Its ack is never delivered below, so it stays Probe.
  let tail: Index = llog.last_index();
  leader
    .endpoint_mut()
    .force_peer_probe_for_test(&follower_id, tail.next());
  let _ = leader.poll_transmit(); // clean baseline

  // An idle flush (no propose since the last flush — the election no-op does NOT set the dirty flag)
  // must be a no-op.
  leader.flush_appends(w.now, llog, lstable);
  let idle = leader.poll_transmit();
  assert!(
    idle.iter().all(|(_, b)| b.is_empty()),
    "an idle flush must emit nothing — the election no-op is not re-sent every pump"
  );

  // Stage ONE proposal without fanning out, then flush: the leader sends it to the Probe peer once.
  let cmd = bytes::Bytes::from_static(b"x");
  leader
    .submit_propose_deferred(w.now, llog, lstable, &cmd)
    .expect("propose on the leader");
  leader.flush_appends(w.now, llog, lstable);
  let first = leader.poll_transmit();
  assert!(
    first.iter().any(|(_, b)| !b.is_empty()),
    "the first flush after a propose must send the entry to the Probe peer"
  );

  // SECOND flush: no new propose, no ack, no timer in this window — the only possible output is a
  // replication re-send, which the dirty flag suppresses. It must emit nothing.
  leader.flush_appends(w.now, llog, lstable);
  let second = leader.poll_transmit();
  assert!(
    second.iter().all(|(_, b)| b.is_empty()),
    "a second flush with no new append must emit nothing — a Probe peer must NOT be re-sent every pump"
  );
}
