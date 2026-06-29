use std::{net::SocketAddr, string::String, time::Duration, vec::Vec};

use super::QuicCoordinator;
use crate::{
  ConfState, Config, Data, Endpoint, Instant,
  testkit::{CountSm, NoopStable, VecLog},
  transport::{
    ClusterId,
    quic::{QuicTuning, crypto::tests::TestClusterCa},
  },
};

type Coord = QuicCoordinator<u64, CountSm>;

const ELECTION: Duration = Duration::from_millis(100);
const HEARTBEAT: Duration = Duration::from_millis(30);

fn addr(node: u64) -> SocketAddr {
  SocketAddr::from(([127, 0, 0, 1], 9000 + node as u16))
}

fn cluster(b: u8) -> ClusterId {
  ClusterId([b; 16])
}

/// The SAN the coordinator's `sni_for` derives for node `id` in `cluster` — certs are minted with
/// it so the stock WebPki verifier matches the dial.
fn san(id: u64, c: &ClusterId) -> String {
  use core::fmt::Write as _;
  let mut s = String::from("node-");
  let mut enc = Vec::new();
  Data::encode(&id, &mut enc);
  for b in &enc {
    let _ = write!(s, "{b:02x}");
  }
  s.push('.');
  for b in &c.0 {
    let _ = write!(s, "{b:02x}");
  }
  s.push_str(".sailing");
  s
}

/// A coordinator for node `id`, authenticating for `c`, with mTLS certs from the shared test CA
/// and a deterministic quinn RNG seed. Keep-alive is off for timer determinism (the production
/// default arms it).
fn coord(ca: &TestClusterCa, id: u64, c: ClusterId) -> Coord {
  coord_with_cap(ca, id, c, None)
}

/// As [`coord`], optionally clamping the CONFIGURED connection cap (the live-floor regression
/// needs the floor — not the 64-connection default — to be the binding constraint).
fn coord_with_cap(ca: &TestClusterCa, id: u64, c: ClusterId, cap: Option<usize>) -> Coord {
  use crate::transport::quic::QuicTuning;
  let cfg = Config::try_new(id, std::vec![1u64, 2u64], ELECTION, HEARTBEAT).unwrap();
  let endpoint = Endpoint::new(cfg, Instant::ORIGIN, id, CountSm::default());
  let mut opts = ca
    .cluster_tls(&san(id, &c))
    .tuning(QuicTuning::new().with_keep_alive_interval_millis(0))
    .build();
  if let Some(cap) = cap {
    opts = opts.with_max_connections(cap);
  }
  let mut seed = [0u8; 32];
  seed[0] = id as u8;
  QuicCoordinator::with_identity(endpoint, opts, Some(seed), c)
}

/// As [`coord`], but a LeaseGuard FAILOVER coordinator (`bounded_clock_uncertainty` set): a
/// network-driven election here stamps the leader no-op with the SYNCHRONIZED wall the driver
/// supplies, and the failover tier debug-asserts a present wall on every endpoint hop. The timing is
/// valid under the module's 100ms election timeout (Δ=30ms, ε=5ms → 30·35/25 = 42ms < 100ms; the
/// uncertainty 20ms < Δ).
fn coord_failover(ca: &TestClusterCa, id: u64, c: ClusterId) -> Coord {
  use crate::transport::quic::QuicTuning;
  let cfg = Config::try_new(id, std::vec![1u64, 2u64], ELECTION, HEARTBEAT)
    .unwrap()
    .with_read_only(crate::ReadOnlyOption::LeaseGuard)
    .with_lease_duration(Duration::from_millis(30))
    .with_clock_drift_bound(Duration::from_millis(5))
    .with_bounded_clock_uncertainty(Duration::from_millis(20));
  let endpoint = Endpoint::new(cfg, Instant::ORIGIN, id, CountSm::default());
  let opts = ca
    .cluster_tls(&san(id, &c))
    .tuning(QuicTuning::new().with_keep_alive_interval_millis(0))
    .build();
  let mut seed = [0u8; 32];
  seed[0] = id as u8;
  QuicCoordinator::with_identity(endpoint, opts, Some(seed), c)
}

/// A two-node world over an in-memory UDP pipe with optional deterministic datagram loss: every
/// `drop_every`-th moved datagram is silently discarded (quinn's loss recovery must retransmit).
struct World {
  a: Coord,
  b: Coord,
  la: VecLog,
  sa: NoopStable,
  lb: VecLog,
  sb: NoopStable,
  now: Instant,
  drop_every: Option<u64>,
  moved: u64,
  dropped: u64,
}

impl World {
  fn new(ca: &TestClusterCa, drop_every: Option<u64>) -> Self {
    Self::with_clusters(ca, cluster(7), cluster(7), drop_every)
  }

  fn with_clusters(
    ca: &TestClusterCa,
    c_a: ClusterId,
    c_b: ClusterId,
    drop_every: Option<u64>,
  ) -> Self {
    let mut a = coord(ca, 1, c_a);
    let b = coord(ca, 2, c_b);
    // Node 1 dials node 2 once; the single connection carries both directions (each side opens
    // its own send stream on it).
    a.connect(Instant::ORIGIN, addr(2), 2u64).expect("dial");
    World {
      a,
      b,
      la: VecLog::default(),
      sa: NoopStable::default(),
      lb: VecLog::default(),
      sb: NoopStable::default(),
      now: Instant::ORIGIN,
      drop_every,
      moved: 0,
      dropped: 0,
    }
  }

  /// Whether this datagram is the deterministically-dropped one.
  fn drops(&mut self) -> bool {
    self.moved += 1;
    if let Some(n) = self.drop_every
      && self.moved.is_multiple_of(n)
    {
      self.dropped += 1;
      return true;
    }
    false
  }

  /// Move all queued datagrams across the pipe (applying the loss schedule), draining storage on
  /// both sides, until quiescent.
  fn settle(&mut self) {
    for _ in 0..400 {
      self.a.handle_storage(self.now, &mut self.la, &mut self.sa);
      self.b.handle_storage(self.now, &mut self.lb, &mut self.sb);
      let mut from_a = Vec::new();
      while let Some(t) = self.a.poll_transmit() {
        from_a.push(t);
      }
      let mut from_b = Vec::new();
      while let Some(t) = self.b.poll_transmit() {
        from_b.push(t);
      }
      let mut progressed = false;
      for (dest, bytes) in from_a {
        assert_eq!(dest, addr(2));
        progressed = true;
        if self.drops() {
          continue;
        }
        self
          .b
          .handle_udp(self.now, addr(1), None, &bytes, &mut self.lb, &mut self.sb);
      }
      for (dest, bytes) in from_b {
        assert_eq!(dest, addr(1));
        progressed = true;
        if self.drops() {
          continue;
        }
        self
          .a
          .handle_udp(self.now, addr(2), None, &bytes, &mut self.la, &mut self.sa);
      }
      if !progressed {
        return;
      }
    }
    panic!("the UDP pipe did not quiesce");
  }

  /// Advance to the earliest pending deadline across both nodes and fire only the node(s)
  /// actually due, then settle. Firing each node on its OWN (randomized) deadline — rather than
  /// both at once — breaks election symmetry, exactly as a real timer-driven cluster does.
  fn step(&mut self) {
    let da = self.a.poll_timeout();
    let db = self.b.poll_timeout();
    let next = match (da, db) {
      (Some(x), Some(y)) => x.min(y),
      (Some(x), None) => x,
      (None, Some(y)) => y,
      (None, None) => self.now + HEARTBEAT,
    };
    // Deferred work reports an immediate (past-or-now) deadline; never run time backwards.
    self.now = self.now.max(next);
    if da.is_some_and(|d| d <= self.now) {
      self.a.handle_timeout(self.now, &mut self.la, &mut self.sa);
    }
    if db.is_some_and(|d| d <= self.now) {
      self.b.handle_timeout(self.now, &mut self.lb, &mut self.sb);
    }
    self.settle();
  }

  fn leader_emerged(&self) -> bool {
    self.a.role().is_leader() || self.b.role().is_leader()
  }
}

/// The happy path the plan pins: connect → handshake (QUIC + hello identity, both directions over
/// ONE connection) → elect → replicate a proposal → both state machines apply it.
#[test]
fn elects_and_replicates_over_quic() {
  let ca = TestClusterCa::generate();
  let mut w = World::new(&ca, None);
  w.settle();

  // The dial validated both ways off the single connection.
  assert!(w.a.has_bound_conn(&2u64), "dialer bound its peer");
  assert!(w.b.has_bound_conn(&1u64), "acceptor bound its peer");

  for _ in 0..100 {
    w.step();
    if w.leader_emerged() {
      break;
    }
  }
  assert!(w.leader_emerged(), "a leader emerged over QUIC");

  let cmd = bytes::Bytes::from_static(b"x");
  if w.a.role().is_leader() {
    w.a
      .submit_propose(w.now, &mut w.la, &w.sa, &cmd)
      .expect("propose on the leader");
  } else {
    w.b
      .submit_propose(w.now, &mut w.lb, &w.sb, &cmd)
      .expect("propose on the leader");
  }
  for _ in 0..60 {
    w.step();
    if w.a.state_machine().count() >= 1 && w.b.state_machine().count() >= 1 {
      break;
    }
  }
  assert!(w.a.state_machine().count() >= 1, "node 1 applied the entry");
  assert!(w.b.state_machine().count() >= 1, "node 2 applied the entry");
}

/// The dropped-datagram case the plan pins: with every 5th datagram silently discarded, quinn's
/// loss recovery retransmits and consensus still elects + replicates — the transport's loss
/// handling, not the consensus layer's, absorbs the UDP loss.
#[test]
fn dropped_datagrams_are_retransmitted() {
  let ca = TestClusterCa::generate();
  let mut w = World::new(&ca, Some(5));
  w.settle();

  for _ in 0..300 {
    w.step();
    if w.leader_emerged() {
      break;
    }
  }
  assert!(w.leader_emerged(), "a leader emerged despite datagram loss");

  let cmd = bytes::Bytes::from_static(b"x");
  if w.a.role().is_leader() {
    w.a
      .submit_propose(w.now, &mut w.la, &w.sa, &cmd)
      .expect("propose");
  } else {
    w.b
      .submit_propose(w.now, &mut w.lb, &w.sb, &cmd)
      .expect("propose");
  }
  for _ in 0..200 {
    w.step();
    if w.a.state_machine().count() >= 1 && w.b.state_machine().count() >= 1 {
      break;
    }
  }
  assert!(
    w.dropped > 0,
    "the loss schedule actually dropped datagrams"
  );
  assert!(
    w.a.state_machine().count() >= 1 && w.b.state_machine().count() >= 1,
    "replication converged through retransmits (dropped {} of {} datagrams)",
    w.dropped,
    w.moved
  );
}

/// A wrong-cluster peer passes TLS (same CA, same SAN form) but its hello advertises a different
/// cluster: the coordinator's cross-check rejects it on BOTH sides and neither binds — cluster
/// separation holds at the identity layer even when the PKI is shared.
#[test]
fn wrong_cluster_hello_never_binds() {
  let ca = TestClusterCa::generate();
  // Mint b's CERT for cluster 7's SAN (so a's dial passes TLS), but configure b's COORDINATOR
  // for cluster 9 — its hello advertises 9, and it expects 9 from a.
  let c7 = cluster(7);
  let c9 = cluster(9);
  let mut a = coord(&ca, 1, c7);
  let b_endpoint = Endpoint::new(
    Config::try_new(2u64, std::vec![1u64, 2u64], ELECTION, HEARTBEAT).unwrap(),
    Instant::ORIGIN,
    2,
    CountSm::default(),
  );
  let b_opts = ca
    .cluster_tls(&san(2, &c7))
    .tuning(QuicTuning::new().with_keep_alive_interval_millis(0))
    .build();
  let mut b: Coord = QuicCoordinator::with_identity(b_endpoint, b_opts, Some([2; 32]), c9);

  let (mut la, mut sa) = (VecLog::default(), NoopStable::default());
  let (mut lb, mut sb) = (VecLog::default(), NoopStable::default());
  let now = Instant::ORIGIN;
  a.connect(now, addr(2), 2u64).expect("dial");
  for _ in 0..200 {
    let mut progressed = false;
    while let Some((_, bytes)) = a.poll_transmit() {
      progressed = true;
      b.handle_udp(now, addr(1), None, &bytes, &mut lb, &mut sb);
    }
    while let Some((_, bytes)) = b.poll_transmit() {
      progressed = true;
      a.handle_udp(now, addr(2), None, &bytes, &mut la, &mut sa);
    }
    if !progressed {
      break;
    }
  }
  assert!(
    !a.has_bound_conn(&2u64),
    "a foreign-cluster hello must never bind on the dialer"
  );
  assert!(
    !b.has_bound_conn(&1u64),
    "a foreign-cluster hello must never bind on the acceptor"
  );
}

/// The provided SNI scheme bounds id encodings at 29 bytes (one DNS label). A wider id must
/// surface as a TYPED dial error — the invalid server name is a configuration error the driver
/// hears about synchronously, never a panic and never a silently-dead dial.
#[test]
fn oversized_node_id_encoding_fails_the_dial_with_a_typed_error() {
  use crate::transport::quic::DialError;

  /// A 32-byte node id: over the 29-byte SNI bound, well under the hello layer's 1024.
  #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
  struct WideId([u8; 32]);

  impl core::fmt::Display for WideId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
      write!(f, "wide")
    }
  }

  impl Data for WideId {
    fn encode(&self, buf: &mut Vec<u8>) {
      buf.extend_from_slice(&self.0);
    }
    fn decode(cur: &mut crate::ByteCursor) -> Result<Self, crate::DecodeError> {
      Ok(Self(cur.take_array::<32>()?))
    }
  }

  impl crate::CheapClone for WideId {}

  let ca = TestClusterCa::generate();
  let cfg = Config::try_new(
    WideId([1; 32]),
    std::vec![WideId([1; 32]), WideId([2; 32])],
    ELECTION,
    HEARTBEAT,
  )
  .unwrap();
  let endpoint = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let opts = ca
    .cluster_tls("node-wide.test.sailing")
    .tuning(QuicTuning::new().with_keep_alive_interval_millis(0))
    .build();
  let mut c: QuicCoordinator<WideId, CountSm> =
    QuicCoordinator::with_identity(endpoint, opts, Some([9; 32]), cluster(7));

  match c.connect(Instant::ORIGIN, addr(2), WideId([2; 32])) {
    Err(DialError::Connect(_)) => {} // rustls rejects the >63-octet label; quinn types it
    other => panic!("a 32-byte id must fail the dial with a typed error, got {other:?}"),
  }

  // The escape hatch is REACHABLE: the same wide-id dial succeeds with an explicit server name
  // (the name the deployment's certs are actually minted for), so wide-id deployments are not
  // locked out of the public dial path.
  c.connect_with_server_name(
    Instant::ORIGIN,
    addr(2),
    WideId([2; 32]),
    "node-wide-2.test.sailing",
  )
  .expect("an explicit server name dials a wide-id peer");
}

/// The connection-cap floor counts EVERY tracked peer — both joint halves, learners, incoming
/// learners — minus this node, not just the incoming voters: the endpoint replicates to all of
/// them, so the transport must admit a mesh edge to each.
#[test]
fn tracked_peer_count_spans_joint_config_and_learners() {
  let simple = ConfState::from_voters(std::vec![1u64, 2, 3]);
  assert_eq!(
    super::tracked_peer_count(&simple, &1u64),
    2,
    "a 3-voter config has 2 peers from node 1"
  );
  // A node OUTSIDE the config (e.g. a learner-to-be counting its own floor) meshes with all 3.
  assert_eq!(super::tracked_peer_count(&simple, &9u64), 3);

  // A joint config mid-transition: incoming {1,2,4}, outgoing {1,2,3}, a learner 5, an incoming
  // learner 6. From node 1 the mesh is {2,3,4,5,6} — the union minus self, every one of which
  // the endpoint replicates to. Counting only incoming voters would size for {2,4} and refuse
  // the rest under a low cap.
  let joint = ConfState::new(
    std::vec![1u64, 2, 4],
    std::vec![5u64],
    std::vec![1u64, 2, 3],
    std::vec![6u64],
    true,
  );
  assert_eq!(super::tracked_peer_count(&joint, &1u64), 5);
  // A LEARNER counts its floor over the same union: from node 5 the peers are {1,2,3,4,6}.
  assert_eq!(super::tracked_peer_count(&joint, &5u64), 5);
}

/// FAILS-ON-OLD: with the connection-cap floor computed once at construction, a committed
/// membership change that grows the tracked peer set outruns the cap — the floor must grow WITH
/// the membership, or the new member's legitimate mesh connections are refused.
#[test]
fn committed_membership_growth_raises_the_connection_cap() {
  use crate::{ConfChange, ConfChangeType};

  let ca = TestClusterCa::generate();
  let mut w = World::new(&ca, None);
  // Make the FLOOR the binding constraint (the 64-connection default would mask the growth).
  w.a = coord_with_cap(&ca, 1, cluster(7), Some(1));
  w.b = coord_with_cap(&ca, 2, cluster(7), Some(1));
  w.a.connect(Instant::ORIGIN, addr(2), 2u64).expect("dial");
  w.settle();
  for _ in 0..100 {
    w.step();
    if w.leader_emerged() {
      break;
    }
  }
  assert!(w.leader_emerged());

  // A 2-node config with a 1-connection configured cap: 1 peer → the MIN floor of 4 binds.
  assert_eq!(w.a.effective_max_connections(), 4);
  assert_eq!(w.b.effective_max_connections(), 4);

  // Commit AddNode(3) through the live cluster.
  let cc = ConfChange::new(ConfChangeType::AddNode, 3u64, bytes::Bytes::new());
  if w.a.role().is_leader() {
    w.a
      .propose_conf_change(w.now, &mut w.la, &w.sa, cc)
      .expect("propose AddNode on the leader");
  } else {
    w.b
      .propose_conf_change(w.now, &mut w.lb, &w.sb, cc)
      .expect("propose AddNode on the leader");
  }
  for _ in 0..80 {
    w.step();
    if w.a.endpoint().conf_state().is_voter(&3u64) && w.b.endpoint().conf_state().is_voter(&3u64) {
      break;
    }
  }
  assert!(
    w.a.endpoint().conf_state().is_voter(&3u64) && w.b.endpoint().conf_state().is_voter(&3u64),
    "the membership change committed and applied on both nodes"
  );

  // 2 peers now → the floor rises to 3*2 = 6 on both nodes, live, without a restart.
  assert_eq!(
    w.a.effective_max_connections(),
    6,
    "the cap grows with the committed membership"
  );
  assert_eq!(w.b.effective_max_connections(), 6);
}

/// FAILS-ON-OLD (FIX 2: `drain_bridge` must forward the full `Now` to `handle_message`): a
/// network-driven election over QUIC, with EVERY coordinator hop driven by a SYNCHRONIZED `Now`,
/// must preserve the synchronized wall onto the elected leader's term-current no-op (Empty) entry.
/// The winning `VoteResponse` rides a QUIC stream into `drain_bridge`, which decodes it and calls
/// `endpoint.handle_message` → `become_leader` → `append_leader_noop`, stamping
/// `lease_wall_stamp(now)`. Under the FAILOVER tier that stamp is `now.wall().as_nanos()`.
///
/// MUTATION (revert FIX 2 — `drain_bridge(now.mono(), ..)`): the decoded `VoteResponse` reaches
/// `handle_message` with the wall STRIPPED (`Now::monotonic`), so the failover tier's
/// `lease_wall_stamp` debug-asserts the absent wall and PANICS the election (and, with the assert
/// compiled out, the no-op would stamp `0`, also failing the `== W` assertion).
#[test]
fn quic_election_preserves_synchronized_wall_on_leader_noop() {
  use crate::{EntryKind, Index, LogStore, Now, Wall};

  // A fixed cluster-epoch wall reading carried on every endpoint hop.
  const W: u64 = 1_700_000_000_000_000_000;
  let synced = |mono: Instant| Now::synchronized(mono, Wall::from_nanos(W));

  let ca = TestClusterCa::generate();
  let c = cluster(7);
  let mut a = coord_failover(&ca, 1, c);
  let mut b = coord_failover(&ca, 2, c);
  let (mut la, mut sa) = (VecLog::default(), NoopStable::default());
  let (mut lb, mut sb) = (VecLog::default(), NoopStable::default());
  let mut now = Instant::ORIGIN;

  // Node 1 dials node 2 once; the single connection carries both directions.
  a.connect(Instant::ORIGIN, addr(2), 2u64).expect("dial");

  // Move every queued datagram across the pipe, draining storage on both sides under a SYNCHRONIZED
  // `Now`, until quiescent — the synchronized-wall analogue of `World::settle`.
  let settle = |a: &mut Coord,
                b: &mut Coord,
                la: &mut VecLog,
                sa: &mut NoopStable,
                lb: &mut VecLog,
                sb: &mut NoopStable,
                now: Instant| {
    for _ in 0..400 {
      a.handle_storage(synced(now), la, sa);
      b.handle_storage(synced(now), lb, sb);
      let mut from_a = Vec::new();
      while let Some(t) = a.poll_transmit() {
        from_a.push(t);
      }
      let mut from_b = Vec::new();
      while let Some(t) = b.poll_transmit() {
        from_b.push(t);
      }
      let mut progressed = false;
      for (_dest, bytes) in from_a {
        progressed = true;
        b.handle_udp(synced(now), addr(1), None, &bytes, lb, sb);
      }
      for (_dest, bytes) in from_b {
        progressed = true;
        a.handle_udp(synced(now), addr(2), None, &bytes, la, sa);
      }
      if !progressed {
        return;
      }
    }
    panic!("the UDP pipe did not quiesce");
  };

  settle(&mut a, &mut b, &mut la, &mut sa, &mut lb, &mut sb, now);
  assert!(
    a.has_bound_conn(&2u64) && b.has_bound_conn(&1u64),
    "both bound"
  );

  // Drive timers (each node on its own randomized deadline) under a SYNCHRONIZED `Now` until a
  // leader emerges — the winning VoteResponse flows over QUIC into `drain_bridge` → `handle_message`.
  for _ in 0..200 {
    let da = a.poll_timeout();
    let db = b.poll_timeout();
    let next = match (da, db) {
      (Some(x), Some(y)) => x.min(y),
      (Some(x), None) => x,
      (None, Some(y)) => y,
      (None, None) => now + HEARTBEAT,
    };
    now = now.max(next);
    if da.is_some_and(|d| d <= now) {
      a.handle_timeout(synced(now), &mut la, &mut sa);
    }
    if db.is_some_and(|d| d <= now) {
      b.handle_timeout(synced(now), &mut lb, &mut sb);
    }
    settle(&mut a, &mut b, &mut la, &mut sa, &mut lb, &mut sb, now);
    if a.role().is_leader() || b.role().is_leader() {
      break;
    }
  }
  assert!(
    a.role().is_leader() || b.role().is_leader(),
    "a leader emerged over QUIC under the failover tier"
  );

  // Inspect the elected leader's log for its term-current no-op (the Empty entry `become_leader`
  // appended) and assert it carries the synchronized wall — proving `drain_bridge` forwarded the
  // full `Now` to `handle_message`.
  let leader_log = if a.role().is_leader() { &la } else { &lb };
  let last = leader_log.last_index();
  let crate::EntriesRead::Ready(entries) = leader_log
    .entries(Index::new(1)..last.next(), u64::MAX)
    .expect("read the leader log")
  else {
    panic!("a resident store never returns Pending");
  };
  let noop = entries
    .iter()
    .find(|e| e.kind() == EntryKind::Empty)
    .expect("the elected leader appended a term-current no-op");
  assert_eq!(
    noop.wall_timestamp(),
    W,
    "the network-driven election must stamp the no-op with the synchronized wall (FIX 2)"
  );
}

/// `QuicCoordinator::new` builds a mutual-mTLS coordinator (the panic-on-no-auth public path) — a
/// fresh follower.
#[test]
fn new_constructs_a_mutual_auth_coordinator() {
  let ca = TestClusterCa::generate();
  let c7 = cluster(7);
  let cfg = Config::try_new(1u64, std::vec![1u64, 2u64], ELECTION, HEARTBEAT).unwrap();
  let opts = ca
    .cluster_tls(&san(1, &c7))
    .tuning(QuicTuning::new().with_keep_alive_interval_millis(0))
    .build();
  let c: Coord = QuicCoordinator::new(cfg, Instant::ORIGIN, 1, CountSm::default(), opts, c7);
  assert!(c.role().is_follower());
}

/// The read / transfer / membership / read-mode / deferred-propose / flush proxies each delegate to
/// the wrapped endpoint and run the coordinator's pump. On a fresh follower the endpoint refuses
/// each, but the delegation + pump executes; the observers expose endpoint state.
#[test]
fn quic_coordinator_proxies_delegate_to_the_endpoint() {
  let ca = TestClusterCa::generate();
  let mut c = coord(&ca, 1, cluster(7));
  let mut log = VecLog::default();
  let stable = NoopStable::default();
  let now = Instant::ORIGIN;
  let cmd = bytes::Bytes::from_static(b"x");

  assert!(
    c.submit_propose_deferred(now, &mut log, &stable, &cmd)
      .is_err()
  );
  c.flush_appends(now, &log, &stable);
  let add3 = crate::ConfChange::new(crate::ConfChangeType::AddNode, 3u64, bytes::Bytes::new());
  assert!(
    c.propose_conf_change_v2(now, &mut log, &stable, add3.into_v2())
      .is_err()
  );
  assert!(
    c.propose_read_mode_change(now, &mut log, &stable, crate::ReadOnlyOption::LeaseBased)
      .is_err()
  );
  assert!(
    c.read_index(now, &log, &stable, bytes::Bytes::new())
      .is_err()
  );
  assert!(c.transfer_leader(now, &log, &stable, 2u64).is_err());

  while c.poll_event().is_some() {}
  assert_eq!(c.oversized_outbound_dropped(), 0);
  assert!(std::format!("{c:?}").contains("QuicCoordinator"));
}

/// Without a quinn clock anchor (nothing fed to quinn yet) the quic side of `poll_timeout` is `None`,
/// so the deadline is the endpoint's election timer (the `(Some, None)` arm).
#[test]
fn poll_timeout_is_the_endpoint_deadline_without_a_quinn_anchor() {
  let ca = TestClusterCa::generate();
  let mut c = coord(&ca, 1, cluster(7));
  assert!(
    c.poll_timeout().is_some(),
    "a fresh follower reports its election deadline"
  );
}

/// A CALLER-SUPPLIED identity source (`dangerous_custom_identity`) cannot bypass the coordinator's
/// binding policy: the cross-checks run on the candidate `authenticate` returns. A candidate that
/// (a) is not the dialed peer, (b) is this node's own id, or (c) attests a foreign cluster is closed
/// — never bound — even over a fully-completed mTLS handshake.
#[test]
fn custom_identity_binding_policy_rejects_bad_candidates() {
  use crate::transport::quic::{Hello, Identified, IdentityCtx, IdentityOutcome, IdentitySource};

  /// Writes a normal `Hello` preface (so the Hello PEER can authenticate us), but its own
  /// `authenticate` returns a scripted candidate the coordinator's policy must reject.
  struct ScriptedId {
    cluster: ClusterId,
    mode: u8,
  }

  impl IdentitySource<u64> for ScriptedId {
    fn write_control_preface(&self, me: &u64, out: &mut Vec<u8>) {
      <Hello as IdentitySource<u64>>::write_control_preface(&Hello::new(self.cluster), me, out);
    }
    fn authenticate(&self, ctx: &IdentityCtx<'_>) -> IdentityOutcome<u64> {
      if ctx.control_frame().is_none() {
        return IdentityOutcome::Pending;
      }
      match self.mode {
        // Not the dialed peer (dialed 2) → dialed-match-or-abort closes.
        0 => IdentityOutcome::Identified(Identified::new(99u64, *ctx.our_cluster())),
        // Our own id → the never-bind-self gate closes.
        1 => IdentityOutcome::Identified(Identified::new(1u64, *ctx.our_cluster())),
        // A foreign cluster → the cluster cross-check closes.
        _ => IdentityOutcome::Identified(Identified::new(2u64, ClusterId([99; 16]))),
      }
    }
  }

  /// Dial a scripted (mode) dialer at a normal Hello acceptor over real mTLS; return whether the
  /// dialer bound peer 2.
  fn dialer_binds_peer2(ca: &TestClusterCa, mode: u8) -> bool {
    let c7 = cluster(7);
    let a_endpoint = Endpoint::new(
      Config::try_new(1u64, std::vec![1u64, 2u64], ELECTION, HEARTBEAT).unwrap(),
      Instant::ORIGIN,
      1,
      CountSm::default(),
    );
    let a_opts = ca
      .cluster_tls(&san(1, &c7))
      .tuning(QuicTuning::new().with_keep_alive_interval_millis(0))
      .build();
    let mut a: QuicCoordinator<u64, CountSm, ScriptedId> =
      QuicCoordinator::dangerous_custom_identity(
        a_endpoint,
        a_opts,
        Some([1; 32]),
        ScriptedId { cluster: c7, mode },
        c7,
      );
    let mut b = coord(ca, 2, c7);
    let (mut la, mut sa) = (VecLog::default(), NoopStable::default());
    let (mut lb, mut sb) = (VecLog::default(), NoopStable::default());
    let now = Instant::ORIGIN;
    a.connect(now, addr(2), 2u64).expect("dial");
    for _ in 0..200 {
      let mut progressed = false;
      while let Some((_, bytes)) = a.poll_transmit() {
        progressed = true;
        b.handle_udp(now, addr(1), None, &bytes, &mut lb, &mut sb);
      }
      while let Some((_, bytes)) = b.poll_transmit() {
        progressed = true;
        a.handle_udp(now, addr(2), None, &bytes, &mut la, &mut sa);
      }
      if !progressed {
        break;
      }
    }
    a.has_bound_conn(&2u64)
  }

  let ca = TestClusterCa::generate();
  assert!(
    !dialer_binds_peer2(&ca, 0),
    "a candidate != the dialed peer must not bind"
  );
  assert!(
    !dialer_binds_peer2(&ca, 1),
    "a candidate == our own id must not bind"
  );
  assert!(
    !dialer_binds_peer2(&ca, 2),
    "a foreign-cluster candidate must not bind"
  );
}

/// `with_identity` requires mandatory mTLS: building it on accept-any options (no client auth) is a
/// programming error and panics — the Hello self-claim has no cryptographic backstop otherwise.
#[test]
#[should_panic(expected = "mandatory mTLS")]
fn with_identity_rejects_options_without_mandatory_client_auth() {
  let cfg = Config::try_new(1u64, std::vec![1u64, 2u64], ELECTION, HEARTBEAT).unwrap();
  let endpoint = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let opts = crate::transport::quic::QuicOptions::new(
    quinn_proto::EndpointConfig::default(),
    None,
    None,
    QuicTuning::new(),
  );
  let _c: Coord = QuicCoordinator::with_identity(endpoint, opts, Some([1; 32]), cluster(7));
}
