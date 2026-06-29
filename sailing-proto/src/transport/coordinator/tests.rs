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

/// The crash-recovery constructors wrap [`Endpoint::restart`] / [`Endpoint::restart_migrating`] with
/// an empty connection table. A fresh restart from an empty durable store reconstructs a follower
/// (no leader, election timer armed) — the driver re-dials/re-accepts its peers.
#[test]
fn restart_and_restart_migrating_rebuild_a_follower() {
  let cfg = || crate::Config::try_new(1u64, std::vec![1u64, 2u64], ELECTION, HEARTBEAT).unwrap();

  let mut log = VecLog::default();
  let mut stable = NoopStable::default();
  let c: Coord = StreamCoordinator::restart(
    cfg(),
    Instant::ORIGIN,
    1,
    CountSm::default(),
    1,
    &mut log,
    &mut stable,
  );
  assert!(
    c.role().is_follower(),
    "a fresh restart from an empty store is a follower"
  );
  assert!(
    c.poll_timeout().is_some(),
    "the election timer is armed after restart"
  );

  let mut log2 = VecLog::default();
  let mut stable2 = NoopStable::default();
  let c2: Coord = StreamCoordinator::restart_migrating(
    cfg(),
    Instant::ORIGIN,
    1,
    CountSm::default(),
    1,
    Some(Duration::from_millis(100)),
    &mut log2,
    &mut stable2,
  );
  assert!(c2.role().is_follower());
}

/// `on_conn_close` is a DRIVER-initiated removal: the driver already knows the socket is gone, so it
/// is NOT echoed back through `poll_conn_closed` (unlike a transport-initiated close).
#[test]
fn driver_initiated_close_is_not_echoed() {
  let mut c = coord(1);
  let id = c.on_conn_open(label(1, true), Instant::ORIGIN);
  c.on_conn_close(id);
  assert_eq!(
    c.poll_conn_closed(),
    None,
    "a driver-initiated close is not surfaced back to the driver"
  );
}

/// A connection that never completes its handshake is reaped past the handshake deadline and the
/// transport-initiated close IS surfaced via `poll_conn_closed` so the driver releases the socket.
#[test]
fn unvalidated_conn_reaped_surfaces_via_poll_conn_closed() {
  let mut w = World::new();
  // No `settle()` — the handshake never completes. Fire the coordinator's housekeeping past the
  // 10s handshake deadline.
  let late = Instant::ORIGIN + Duration::from_secs(11);
  w.a.handle_timeout(late, &mut w.la, &mut w.sa);
  assert_eq!(
    w.a.poll_conn_closed(),
    Some((
      ConnId(1),
      Some(crate::transport::TransportError::NotValidated)
    )),
    "an un-validated connection past the deadline is reaped and reported"
  );
}

/// The read / transfer / membership / read-mode proxies each delegate to the wrapped endpoint and
/// run the coordinator's flush. On a fresh follower the endpoint refuses each (not the leader), but
/// the delegation + flush path executes — and `poll_event` / `endpoint()` expose the endpoint.
#[test]
fn coordinator_proxies_delegate_to_the_endpoint() {
  let mut c = coord(1);
  let mut log = VecLog::default();
  let stable = NoopStable::default();
  let now = Instant::ORIGIN;

  assert!(
    c.read_index(now, &log, &stable, bytes::Bytes::new())
      .is_err(),
    "a follower cannot serve a read index"
  );
  assert!(
    c.transfer_leader(now, &log, &stable, 2u64).is_err(),
    "a follower cannot transfer leadership"
  );
  let add3 = crate::ConfChange::new(crate::ConfChangeType::AddNode, 3u64, bytes::Bytes::new());
  assert!(
    c.propose_conf_change(now, &mut log, &stable, add3.clone())
      .is_err(),
    "a follower cannot propose a conf change"
  );
  assert!(
    c.propose_conf_change_v2(now, &mut log, &stable, add3.into_v2())
      .is_err()
  );
  assert!(
    c.propose_read_mode_change(now, &mut log, &stable, crate::ReadOnlyOption::LeaseBased)
      .is_err()
  );

  // `poll_event` drains the application-event queue; `endpoint()` exposes the wrapped endpoint.
  while c.poll_event().is_some() {}
  assert_eq!(c.endpoint().role(), c.role());
}
