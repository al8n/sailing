use super::*;
use crate::{
  Term,
  testkit::{AsyncStable, CountSm, VecLog},
  transport::quic::{QuicTuning, crypto::tests::TestClusterCa},
};
use core::time::Duration;
use std::{collections::BTreeMap, string::String};

type MCoord = MultiQuicCoordinator<u64, u64, CountSm>;

/// The SAN the coordinator's `sni_for` derives for node `id` in `c` — certs are minted with it.
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
  let ca = TestClusterCa::generate();
  let cluster = ClusterId([7u8; 16]);
  let opts = ca
    .cluster_tls(&san(1, &cluster))
    .tuning(QuicTuning::new().with_keep_alive_interval_millis(0))
    .build();
  let mut seed = [0u8; 32];
  seed[0] = 1;
  let mut coord =
    MultiQuicCoordinator::<u64, u64, CountSm>::with_identity(opts, Some(seed), cluster);
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
  // touched. Single-voter groups need no peer connection, so this exercises the coordinator's group
  // threading + store routing without the wire.
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

fn addr(node: u64) -> SocketAddr {
  SocketAddr::from(([127, 0, 0, 1], 9100 + node as u16))
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

/// A multi-group coordinator for node `id` with mTLS certs from the shared test CA and a
/// deterministic quinn RNG seed. Keep-alive is off for timer determinism.
fn multi_coord(ca: &TestClusterCa, id: u64, cluster: ClusterId) -> MCoord {
  let opts = ca
    .cluster_tls(&san(id, &cluster))
    .tuning(QuicTuning::new().with_keep_alive_interval_millis(0))
    .build();
  let mut seed = [0u8; 32];
  seed[0] = id as u8;
  MultiQuicCoordinator::with_identity(opts, Some(seed), cluster)
}

fn group_stores(groups: &[u64]) -> Stores {
  let mut s = Stores {
    map: BTreeMap::new(),
  };
  for &g in groups {
    s.map.insert(g, (VecLog::default(), AsyncStable::default()));
  }
  s
}

/// Move all queued datagrams across the in-memory pipe, draining every hosted group's storage on
/// both sides, until quiescent.
fn settle(a: &mut MCoord, b: &mut MCoord, sa: &mut Stores, sb: &mut Stores, now: Instant) {
  for _ in 0..400 {
    for g in [100u64, 200] {
      if a.group(&g).is_some() {
        let (l, s) = sa.stores(&g).unwrap();
        let _ = a.handle_storage(&g, now, l, s);
      }
      if b.group(&g).is_some() {
        let (l, s) = sb.stores(&g).unwrap();
        let _ = b.handle_storage(&g, now, l, s);
      }
    }
    let mut from_a = Vec::new();
    while let Some(t) = a.poll_transmit() {
      from_a.push(t);
    }
    let mut from_b = Vec::new();
    while let Some(t) = b.poll_transmit() {
      from_b.push(t);
    }
    let mut progressed = false;
    for (dest, bytes) in from_a {
      assert_eq!(dest, addr(2), "a only ever talks to b");
      progressed = true;
      b.handle_udp(now, addr(1), None, &bytes, sb);
    }
    for (dest, bytes) in from_b {
      assert_eq!(dest, addr(1), "b only ever talks to a");
      progressed = true;
      a.handle_udp(now, addr(2), None, &bytes, sa);
    }
    if !progressed {
      return;
    }
  }
  panic!("the UDP pipe did not quiesce");
}

/// Drive `a`'s `group` to leadership by firing ONLY its consensus timers (`b` grants and never
/// campaigns), returning the advanced clock.
fn elect_a(
  a: &mut MCoord,
  b: &mut MCoord,
  sa: &mut Stores,
  sb: &mut Stores,
  group: u64,
  mut now: Instant,
) -> Instant {
  for _ in 0..40 {
    if a.group(&group).unwrap().role().is_leader() {
      return now;
    }
    if let Some(d) = a.group(&group).unwrap().poll_timeout() {
      now = now.max(d);
      let (l, s) = sa.stores(&group).unwrap();
      a.handle_timeout(&group, now, l, s).unwrap();
    }
    settle(a, b, sa, sb, now);
  }
  panic!("group {group} did not elect a leader over QUIC");
}

/// Two co-located groups drive elections over ONE QUIC connection: each group-tagged frame
/// demuxes to the owning endpoint on the receiver while the other group stays pristine.
#[test]
fn demuxes_two_groups_over_one_quic_connection() {
  let ca = TestClusterCa::generate();
  let cluster = ClusterId([7u8; 16]);
  let mut a = multi_coord(&ca, 1, cluster);
  let mut b = multi_coord(&ca, 2, cluster);
  for g in [100u64, 200] {
    a.create_group(g, two_voter(1), Instant::ORIGIN, 1, CountSm::default())
      .unwrap();
    b.create_group(g, two_voter(2), Instant::ORIGIN, 2, CountSm::default())
      .unwrap();
  }
  let mut sa = group_stores(&[100, 200]);
  let mut sb = group_stores(&[100, 200]);
  let mut now = Instant::ORIGIN;

  a.connect(now, addr(2), 2u64).expect("dial");
  settle(&mut a, &mut b, &mut sa, &mut sb, now);
  assert!(a.has_bound_conn(&2u64), "dialer bound its peer");
  assert!(b.has_bound_conn(&1u64), "acceptor bound its peer");

  now = elect_a(&mut a, &mut b, &mut sa, &mut sb, 100, now);
  assert!(
    b.group(&100).unwrap().term() >= Term::new(1),
    "group 100 crossed the QUIC link"
  );
  assert_eq!(
    b.group(&200).unwrap().term(),
    Term::ZERO,
    "group 200 is pristine — the tag isolated the traffic"
  );

  let _ = elect_a(&mut a, &mut b, &mut sa, &mut sb, 200, now);
  assert!(
    b.group(&200).unwrap().term() >= Term::new(1),
    "group 200 demuxed over the SAME connection"
  );
  assert!(
    a.has_bound_conn(&2u64) && b.has_bound_conn(&1u64),
    "the shared connection survived both elections"
  );
}

/// A host with ZERO groups has no identity to advertise: the handshake wedges un-validated on
/// both sides (no bind). After the auth deadline reaps the wedged connection, creating a group
/// and REDIALING binds — and a group election crosses the fresh link.
#[test]
fn zero_group_host_never_binds() {
  let ca = TestClusterCa::generate();
  let cluster = ClusterId([7u8; 16]);
  let mut a = multi_coord(&ca, 1, cluster);
  let mut b = multi_coord(&ca, 2, cluster); // hosts NO groups yet
  a.create_group(100, two_voter(1), Instant::ORIGIN, 1, CountSm::default())
    .unwrap();
  let mut sa = group_stores(&[100]);
  let mut sb = group_stores(&[]);
  let mut now = Instant::ORIGIN;

  a.connect(now, addr(2), 2u64).expect("dial");
  settle(&mut a, &mut b, &mut sa, &mut sb, now);
  assert!(
    !b.has_bound_conn(&1u64),
    "a zero-group host binds no one (nothing to authenticate as)"
  );
  assert!(
    !a.has_bound_conn(&2u64),
    "the peer of a zero-group host never validates either"
  );

  // Advance past the 5s auth deadline and fire the transport timers so both sides reap the
  // wedged connection before the redial.
  now = now + Duration::from_secs(6);
  a.handle_transport_timeout(now, &mut sa);
  b.handle_transport_timeout(now, &mut sb);
  settle(&mut a, &mut b, &mut sa, &mut sb, now);

  // Give b its group; a FRESH dial then authenticates and binds both ways.
  b.create_group(100, two_voter(2), now, 2, CountSm::default())
    .unwrap();
  sb.map
    .insert(100, (VecLog::default(), AsyncStable::default()));
  a.connect(now, addr(2), 2u64).expect("redial");
  settle(&mut a, &mut b, &mut sa, &mut sb, now);
  assert!(
    a.has_bound_conn(&2u64) && b.has_bound_conn(&1u64),
    "both bind once a group exists"
  );

  let _ = elect_a(&mut a, &mut b, &mut sa, &mut sb, 100, now);
  assert!(
    b.group(&100).unwrap().term() >= Term::new(1),
    "the election crossed after the group appeared"
  );
}
