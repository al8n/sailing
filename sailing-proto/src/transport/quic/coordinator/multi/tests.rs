use super::*;
use crate::{
  FloorStore, MERGED_FLOOR, NoFloors, SplitError, Term,
  testkit::{AsyncStable, CountSm, VecLog},
  transport::quic::{QuicTuning, crypto::tests::TestClusterCa},
};
use bytes::Bytes;
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

impl FloorStore<u64> for Stores {
  fn floor(&self, _gid: &u64) -> u64 {
    0
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
  a.create_group(
    100,
    two_voter(1),
    Instant::ORIGIN,
    1,
    CountSm::default(),
    0,
    &NoFloors,
  )
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
  b.create_group(100, two_voter(2), now, 2, CountSm::default(), 0, &NoFloors)
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

/// Drain every queued group control from a coordinator.
fn drain_controls(c: &mut MCoord) -> Vec<(u64, crate::GroupControl)> {
  let mut controls = Vec::new();
  while let Some(ctrl) = c.poll_group_control() {
    controls.push(ctrl);
  }
  controls
}

/// The quiesce round-trip over QUIC: `mark_quiescing` stamps the group's next beat (the intent is
/// consumed by that broadcast), the flag surfaces exactly one `GroupControl::Quiesce` on the
/// follower — AFTER the beat's own `Wake`, so folding in order nets quiesced — the co-located
/// group's beat rides the same crank untouched, and the returning `HeartbeatResponse`s wake
/// NOTHING on the leader (the flap-free property that lets a quiesced leader stay quiesced).
#[test]
fn quiesce_flag_round_trips_over_quic() {
  use crate::GroupControl;
  let ca = TestClusterCa::generate();
  let cluster = ClusterId([7u8; 16]);
  let mut a = multi_coord(&ca, 1, cluster);
  let mut b = multi_coord(&ca, 2, cluster);
  for g in [100u64, 200] {
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
  }
  let mut sa = group_stores(&[100, 200]);
  let mut sb = group_stores(&[100, 200]);
  let mut now = Instant::ORIGIN;

  a.connect(now, addr(2), 2u64).expect("dial");
  settle(&mut a, &mut b, &mut sa, &mut sb, now);
  now = elect_a(&mut a, &mut b, &mut sa, &mut sb, 100, now);
  now = elect_a(&mut a, &mut b, &mut sa, &mut sb, 200, now);
  let _ = drain_controls(&mut a); // drop the election-era controls
  let _ = drain_controls(&mut b);

  a.mark_quiescing(&100);
  assert!(a.is_quiescing(&100));
  // Fire BOTH leaders' heartbeat timers in one crank; the beats coalesce at the transmit drain.
  let d100 = a.group(&100).unwrap().poll_timeout().unwrap();
  let d200 = a.group(&200).unwrap().poll_timeout().unwrap();
  now = now.max(d100).max(d200);
  {
    let (l, s) = sa.stores(&100).unwrap();
    a.handle_timeout(&100, now, l, s).unwrap();
  }
  {
    let (l, s) = sa.stores(&200).unwrap();
    a.handle_timeout(&200, now, l, s).unwrap();
  }
  assert!(
    !a.is_quiescing(&100),
    "the intent is consumed by the stamped broadcast"
  );
  settle(&mut a, &mut b, &mut sa, &mut sb, now);

  let controls = drain_controls(&mut b);
  assert_eq!(
    controls,
    std::vec![
      (100, GroupControl::Wake),
      (100, GroupControl::Quiesce),
      (200, GroupControl::Wake),
    ],
    "both beats delivered; only the flagged group quiesces, ordered flag-last"
  );
  assert_eq!(
    drain_controls(&mut a),
    Vec::new(),
    "HeartbeatResponses are absorbed: no Wake for the quiescing leader"
  );
  assert!(
    a.has_bound_conn(&2u64) && b.has_bound_conn(&1u64),
    "the shared connection survived the coalesced exchange"
  );
}

/// The wake-classification pin on this coordinator's own copy (the stream sibling pins it
/// end-to-end): only a `HeartbeatResponse` is absorbed — an empty `AppendEntries` and an
/// `AppendResponse` wake, since the gated heartbeat-response pump leaves an idle round with no
/// empty-append tail and quiesce eligibility excludes the probing peers that would still draw it.
#[test]
fn only_the_heartbeat_response_is_absorbed() {
  use crate::Index;
  let empty_ae = Message::AppendEntries(crate::AppendEntries::new(
    Term::new(1),
    1u64,
    Index::ZERO,
    Term::ZERO,
    Vec::new(),
    Index::ZERO,
  ));
  assert!(
    MCoord::is_wake_class(&empty_ae),
    "an empty AppendEntries wakes"
  );
  let ack = Message::AppendResponse(crate::AppendResponse::new(
    Term::new(1),
    2u64,
    false,
    Index::ZERO,
    Term::ZERO,
    Index::ZERO,
  ));
  assert!(MCoord::is_wake_class(&ack), "an AppendResponse wakes");
  let hbr = Message::HeartbeatResponse(crate::HeartbeatResponse::new(
    Term::new(1),
    2u64,
    bytes::Bytes::new(),
  ));
  assert!(
    !MCoord::is_wake_class(&hbr),
    "the heartbeat response is absorbed"
  );
}

/// A coalesced entry for a group the receiver no longer hosts is dropped ENTRY by entry over
/// QUIC: the co-located group's beat in the same frame still dispatches and the shared connection
/// survives.
#[test]
fn unhosted_coalesced_entry_drops_over_quic() {
  use crate::GroupControl;
  let ca = TestClusterCa::generate();
  let cluster = ClusterId([7u8; 16]);
  let mut a = multi_coord(&ca, 1, cluster);
  let mut b = multi_coord(&ca, 2, cluster);
  for g in [100u64, 200] {
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
  }
  let mut sa = group_stores(&[100, 200]);
  let mut sb = group_stores(&[100, 200]);
  let mut now = Instant::ORIGIN;

  a.connect(now, addr(2), 2u64).expect("dial");
  settle(&mut a, &mut b, &mut sa, &mut sb, now);
  now = elect_a(&mut a, &mut b, &mut sa, &mut sb, 100, now);
  now = elect_a(&mut a, &mut b, &mut sa, &mut sb, 200, now);
  let _ = drain_controls(&mut b);

  // b de-hosts group 200; a (unaware) keeps beating both groups in one coalesced frame.
  assert!(b.remove_group(&200, &mut empty_stores()).unwrap().is_some());
  let d100 = a.group(&100).unwrap().poll_timeout().unwrap();
  let d200 = a.group(&200).unwrap().poll_timeout().unwrap();
  now = now.max(d100).max(d200);
  {
    let (l, s) = sa.stores(&100).unwrap();
    a.handle_timeout(&100, now, l, s).unwrap();
  }
  {
    let (l, s) = sa.stores(&200).unwrap();
    a.handle_timeout(&200, now, l, s).unwrap();
  }
  settle(&mut a, &mut b, &mut sa, &mut sb, now);

  let controls = drain_controls(&mut b);
  assert_eq!(
    controls,
    std::vec![(100, GroupControl::Wake)],
    "the hosted entry dispatched; the de-hosted one dropped silently"
  );
  assert!(
    a.has_bound_conn(&2u64) && b.has_bound_conn(&1u64),
    "an unhosted entry never costs the shared connection"
  );
}

/// Removal TOMBSTONES the id at the QUIC coordinator: straggler entries for it drop silently —
/// the shared connection and the co-located group's traffic untouched, no control — and a
/// create of the id refuses with `Retired` until an explicit `clear_tombstone` consents to
/// re-admission; the clear-then-create rejoin then lets traffic reach the fresh replica again.
#[test]
fn tombstoned_group_refuses_recreation_until_cleared() {
  use crate::GroupControl;
  let ca = TestClusterCa::generate();
  let cluster = ClusterId([7u8; 16]);
  let mut a = multi_coord(&ca, 1, cluster);
  let mut b = multi_coord(&ca, 2, cluster);
  for g in [100u64, 200] {
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
  }
  let mut sa = group_stores(&[100, 200]);
  let mut sb = group_stores(&[100, 200]);
  let mut now = Instant::ORIGIN;

  a.connect(now, addr(2), 2u64).expect("dial");
  settle(&mut a, &mut b, &mut sa, &mut sb, now);
  now = elect_a(&mut a, &mut b, &mut sa, &mut sb, 100, now);
  now = elect_a(&mut a, &mut b, &mut sa, &mut sb, 200, now);
  let _ = drain_controls(&mut b);

  assert!(!b.is_retired(&100), "a hosted group is not tombstoned");
  assert!(b.remove_group(&100, &mut empty_stores()).unwrap().is_some());
  assert!(b.is_retired(&100), "removal tombstones the id");

  // Both leaders beat in one crank: the coalesced frame carries a tombstoned entry (100) beside
  // a live one (200) — the tombstone absorbs its entry, the sibling's still dispatches.
  let d100 = a.group(&100).unwrap().poll_timeout().unwrap();
  let d200 = a.group(&200).unwrap().poll_timeout().unwrap();
  now = now.max(d100).max(d200);
  {
    let (l, s) = sa.stores(&100).unwrap();
    a.handle_timeout(&100, now, l, s).unwrap();
  }
  {
    let (l, s) = sa.stores(&200).unwrap();
    a.handle_timeout(&200, now, l, s).unwrap();
  }
  settle(&mut a, &mut b, &mut sa, &mut sb, now);
  assert_eq!(
    drain_controls(&mut b),
    std::vec![(200, GroupControl::Wake)],
    "the tombstoned entry surfaced no control; the live sibling dispatched"
  );
  assert!(b.group(&100).is_none(), "the group stays removed");
  assert!(
    a.has_bound_conn(&2u64) && b.has_bound_conn(&1u64),
    "a tombstoned straggler never costs the shared connection"
  );

  // A tombstoned id REFUSES re-creation — a stale unknown-group advisory replayed into a naive
  // create can never resurrect it; only the explicit clear consents to re-admission.
  assert_eq!(
    b.create_group(100, two_voter(2), now, 2, CountSm::default(), 0, &NoFloors)
      .unwrap_err(),
    CreateGroupError::Retired,
    "create refuses a tombstoned id"
  );
  assert!(b.is_retired(&100), "the refused create lifts nothing");
  assert!(b.clear_tombstone(&100), "a tombstone existed");
  assert!(
    !b.is_retired(&100),
    "the explicit clear lifts the tombstone"
  );

  // The clear-then-create rejoin: the fresh replica (fresh stores) hears the still-beating
  // leader again.
  sb.map
    .insert(100, (VecLog::default(), AsyncStable::default()));
  b.create_group(100, two_voter(2), now, 2, CountSm::default(), 0, &NoFloors)
    .unwrap();
  let d = a.group(&100).unwrap().poll_timeout().unwrap();
  now = now.max(d);
  {
    let (l, s) = sa.stores(&100).unwrap();
    a.handle_timeout(&100, now, l, s).unwrap();
  }
  settle(&mut a, &mut b, &mut sa, &mut sb, now);
  assert!(
    b.group(&100).unwrap().term() >= Term::new(1),
    "traffic flows to the re-created group"
  );
}

/// Fire one campaign round of `a`'s `group` (its election timer) and settle — the solicitation
/// generator for the unknown-group tests (`b` does not host the group, so no election completes).
fn campaign_a(
  a: &mut MCoord,
  b: &mut MCoord,
  sa: &mut Stores,
  sb: &mut Stores,
  group: u64,
  mut now: Instant,
) -> Instant {
  let d = a.group(&group).unwrap().poll_timeout().unwrap();
  now = now.max(d);
  {
    let (l, s) = sa.stores(&group).unwrap();
    a.handle_timeout(&group, now, l, s).unwrap();
  }
  settle(a, b, sa, sb, now);
  now
}

/// The unknown-group placement signal over live QUIC: a campaign for a group `b` does not host
/// surfaces `(group, sender)` once until polled, polling re-arms it, a tombstoned id stays
/// silent, and the dedupe set is capped.
#[test]
fn unknown_group_traffic_surfaces_over_quic() {
  let ca = TestClusterCa::generate();
  let cluster = ClusterId([7u8; 16]);
  let mut a = multi_coord(&ca, 1, cluster);
  let mut b = multi_coord(&ca, 2, cluster);
  for g in [100u64, 200] {
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
  }
  b.create_group(
    100,
    two_voter(2),
    Instant::ORIGIN,
    2,
    CountSm::default(),
    0,
    &NoFloors,
  )
  .unwrap();
  let mut sa = group_stores(&[100, 200]);
  let mut sb = group_stores(&[100]);
  let mut now = Instant::ORIGIN;

  a.connect(now, addr(2), 2u64).expect("dial");
  settle(&mut a, &mut b, &mut sa, &mut sb, now);
  now = elect_a(&mut a, &mut b, &mut sa, &mut sb, 100, now);
  assert_eq!(
    b.poll_unknown_group(),
    None,
    "hosted traffic is never unknown"
  );

  // Two campaign rounds for un-hosted group 200 before the embedder polls: ONE deduped signal.
  now = campaign_a(&mut a, &mut b, &mut sa, &mut sb, 200, now);
  now = campaign_a(&mut a, &mut b, &mut sa, &mut sb, 200, now);
  assert_eq!(
    b.poll_unknown_group(),
    Some((200, 1)),
    "the unknown group surfaces with its soliciting peer"
  );
  assert_eq!(b.poll_unknown_group(), None, "deduped until polled");

  // Polling re-arms the group for a fresh signal.
  now = campaign_a(&mut a, &mut b, &mut sa, &mut sb, 200, now);
  assert_eq!(b.poll_unknown_group(), Some((200, 1)));

  // Tombstoning the id silences the solicitations entirely (an unhosted removal still
  // tombstones: the embedder declared the id retired).
  assert!(
    b.remove_group(&200, &mut empty_stores()).unwrap().is_none(),
    "b never hosted 200"
  );
  assert!(b.is_retired(&200));
  let _ = campaign_a(&mut a, &mut b, &mut sa, &mut sb, 200, now);
  assert_eq!(
    b.poll_unknown_group(),
    None,
    "tombstoned: silent, not unknown"
  );
  assert!(
    a.has_bound_conn(&2u64) && b.has_bound_conn(&1u64),
    "unknown and tombstoned solicitations never cost the connection"
  );

  // The pending set is CAPPED at 64 distinct groups (the wire path into the queue is covered
  // above; the bound itself is a container property).
  for gid in 0..(UNKNOWN_GROUP_SIGNAL_CAP as u64 + 6) {
    b.note_unknown_group(3000 + gid, 1u64);
  }
  let mut drained = 0;
  while b.poll_unknown_group().is_some() {
    drained += 1;
  }
  assert_eq!(drained, UNKNOWN_GROUP_SIGNAL_CAP);
}

/// A zero-group host CLOSES a preface-bearing connection immediately (the preface was already
/// consumed, so it could never validate) — admission plus a fresh dial therefore recover well
/// WITHIN the auth-deadline window, with no reap wait.
#[test]
fn group_admitted_before_auth_deadline_recovers() {
  let ca = TestClusterCa::generate();
  let cluster = ClusterId([7u8; 16]);
  let mut a = multi_coord(&ca, 1, cluster);
  let mut b = multi_coord(&ca, 2, cluster); // hosts NO groups yet
  a.create_group(
    100,
    two_voter(1),
    Instant::ORIGIN,
    1,
    CountSm::default(),
    0,
    &NoFloors,
  )
  .unwrap();
  let mut sa = group_stores(&[100]);
  let mut sb = group_stores(&[]);
  let now = Instant::ORIGIN;

  a.connect(now, addr(2), 2u64).expect("dial");
  settle(&mut a, &mut b, &mut sa, &mut sb, now);
  assert!(
    !b.has_bound_conn(&1u64),
    "the identity-less host closed the connection instead of binding"
  );

  // No time passes: the group appears and a fresh dial binds long before the 5s deadline.
  b.create_group(100, two_voter(2), now, 2, CountSm::default(), 0, &NoFloors)
    .unwrap();
  sb.map
    .insert(100, (VecLog::default(), AsyncStable::default()));
  // The stale connection was closed at its Connected event — strictly before any preface byte is
  // examined — so even bytes arriving AFTER admission (a trickling peer preface) cannot bind it.
  settle(&mut a, &mut b, &mut sa, &mut sb, now);
  assert!(
    !b.has_bound_conn(&1u64) && !a.has_bound_conn(&2u64),
    "post-admission traffic cannot resurrect the identity-less connection"
  );
  a.connect(now, addr(2), 2u64).expect("redial");
  settle(&mut a, &mut b, &mut sa, &mut sb, now);
  assert!(
    a.has_bound_conn(&2u64) && b.has_bound_conn(&1u64),
    "both bind with zero elapsed time"
  );
}

/// The QUIC twin of the stream coordinator's 5-cell admission matrix: floor first, the volatile
/// consent gate at every gen, container existence last — and a NoFloors world is P5 verbatim.
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
  let ca = TestClusterCa::generate();
  let cluster = ClusterId([7u8; 16]);
  let mut c = multi_coord(&ca, 1, cluster);
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
  let mut p5 = multi_coord(&ca, 1, cluster);
  p5.create_group(7, single_voter(1), now, 1, CountSm::default(), 0, &NoFloors)
    .expect("gen-0 verbatim");
}

/// The QUIC twin of the reserved-sentinel refusal: `u64::MAX` is the merged-tombstone fence,
/// never a working incarnation, so create refuses it under ANY floor — the reservation under a
/// lower floor, the terminal fence's own verdict under `MERGED_FLOOR`.
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
  let ca = TestClusterCa::generate();
  let cluster = ClusterId([7u8; 16]);
  let mut c = multi_coord(&ca, 1, cluster);
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

/// Encode `n` as a CountSm snapshot blob for the fork-admission tests.
fn fork_blob(n: u64) -> bytes::Bytes {
  let mut v = Vec::new();
  Data::encode(&n, &mut v);
  bytes::Bytes::from(v)
}

/// The QUIC twin of the stream fork-tombstone gate: a fork never clears a tombstone; the
/// refusal writes nothing; clear-then-fork boots the group at the manufactured baseline.
#[test]
fn fork_refuses_a_tombstoned_id_until_cleared() {
  let ca = TestClusterCa::generate();
  let cluster = ClusterId([7u8; 16]);
  let mut c = multi_coord(&ca, 1, cluster);
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
  assert!(c.is_retired(&100));

  assert!(c.clear_tombstone(&100));
  c.create_group_from_fork(
    100,
    single_voter(1),
    now,
    1,
    CountSm::default(),
    fork_blob(3),
    None,
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

/// The QUIC twin of the fork floor gate: floor first, the reserved sentinel refused at every
/// floor, refusals write nothing, and the at-floor fork admits at the baseline.
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
  let ca = TestClusterCa::generate();
  let cluster = ClusterId([7u8; 16]);
  let mut c = multi_coord(&ca, 1, cluster);
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

/// The QUIC twin of the fork boot-epoch guard: `boot_epoch == 0` would issue the manufactured
/// baseline's completions in the child's own first live epoch, so the delegator surfaces the
/// container's refusal before any store write — the caller's fresh stores stay pristine.
#[test]
fn fork_refuses_boot_epoch_zero() {
  let ca = TestClusterCa::generate();
  let cluster = ClusterId([7u8; 16]);
  let mut c = multi_coord(&ca, 1, cluster);
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

/// A successful fork PURGES a queued unknown-group signal for its id on the QUIC coordinator
/// too — polling after the admission must not surface a stale "unknown" claim.
#[test]
fn fork_purges_a_queued_unknown_group_signal() {
  let ca = TestClusterCa::generate();
  let cluster = ClusterId([7u8; 16]);
  let mut c = multi_coord(&ca, 1, cluster);
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
  c.note_unknown_group(200, 2);
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  c.create_group_from_fork(
    200,
    single_voter(1),
    now,
    1,
    CountSm::default(),
    fork_blob(3),
    None,
    None,
    1,
    0,
    &NoFloors,
    &mut log,
    &mut stable,
  )
  .unwrap();
  assert_eq!(
    c.poll_unknown_group(),
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
  let ca = TestClusterCa::generate();
  let cluster = ClusterId([7u8; 16]);
  let mut c = multi_coord(&ca, 1, cluster);
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

/// The QUIC delegator's restore feeds the fork replay guard from the `floors` seam exactly as
/// the stream coordinator's does: a restored parent whose log replays an already-materialized
/// split (its own snapshot meta lags — it never snapshotted) folds the re-staged fork to a
/// resolved no-op when the durable engine lineage covers it, and relays it only under a
/// lineage-blind floor store (the control leg proving the seam is what did it).
#[test]
fn quic_restore_seeds_the_replay_guard_from_the_floor_seam() {
  #[derive(Default)]
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

    fn supports_split(&self) -> bool {
      true
    }
  }

  // A crash image whose split is durable in the LOG but not yet in any snapshot meta: two
  // committed commands, then the committed split of child 300 minted at parent gen 1.
  let stores = || {
    let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
    let mut child_bytes = Vec::new();
    crate::Data::encode(&300u64, &mut child_bytes);
    let payload =
      crate::SplitPayload::new(Bytes::from(child_bytes), 0, 1, Bytes::from_static(b"\x01"));
    let mut buf = Vec::new();
    crate::wire::encode_split_payload(&payload, &mut buf);
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
      crate::Entry::new(Term::new(1), Index::new(2), crate::EntryKind::Normal, cmd),
      crate::Entry::new(
        Term::new(1),
        Index::new(3),
        crate::EntryKind::Split,
        Bytes::from(buf),
      ),
    ]);
    stable.force_state(Term::new(1), Some(1u64), Index::new(3));
    (log, stable)
  };

  struct Floors(u64);
  impl FloorStore<u64> for Floors {
    fn floor(&self, _: &u64) -> u64 {
      0
    }

    fn lineage(&self, _: &u64) -> u64 {
      self.0
    }
  }

  let ca = TestClusterCa::generate();
  let cluster = ClusterId([9u8; 16]);
  let coord = |node: u64| {
    let opts = ca
      .cluster_tls(&san(node, &cluster))
      .tuning(QuicTuning::new().with_keep_alive_interval_millis(0))
      .build();
    let mut seed = [0u8; 32];
    seed[0] = node as u8;
    MultiQuicCoordinator::<u64, u64, SplitSm>::with_identity(opts, Some(seed), cluster)
  };

  // The durable engine lineage (1) covers the replayed fork: nothing relays, and the parent
  // replayed to its post-split half with the fold having resolved the fork's own barrier.
  let mut a = coord(1);
  let (mut log, mut stable) = stores();
  a.restore_group(
    100,
    single_voter(1),
    Instant::ORIGIN,
    1,
    SplitSm::default(),
    1,
    1,
    &Floors(1),
    &mut log,
    &mut stable,
  )
  .unwrap();
  assert!(
    a.poll_pending_fork().is_none(),
    "the durable lineage already covers the replayed fork"
  );
  assert_eq!(a.group(&100).unwrap().state_machine().units, 1, "2 - 1");
  assert!(a.group(&300).is_none(), "no child materialized from replay");

  // The control: a lineage-blind floor store leaves the meta-seeded guard at 0 and the same
  // replayed fork RELAYS — the floors seam, not the meta, is what folded it above.
  let mut b = coord(2);
  let (mut log, mut stable) = stores();
  b.restore_group(
    100,
    single_voter(1),
    Instant::ORIGIN,
    1,
    SplitSm::default(),
    1,
    1,
    &NoFloors,
    &mut log,
    &mut stable,
  )
  .unwrap();
  let fork = b
    .poll_pending_fork()
    .expect("a lineage-blind guard seed relays the replayed fork");
  assert_eq!((fork.child, fork.parent_gen_after), (300, 1));
  assert_eq!(fork.fsm.units, 1, "the re-forked half");
}

/// The QUIC twin of the split-reservation admission gate (window A — the leader's
/// propose→apply window; the full lifecycle walk lives in the stream coordinator's test over
/// the shared container predicate): once a split naming the child id is appended, create,
/// restore, and fork all refuse it with the typed verdict, the factory-gate predicate reads
/// true, and only the named id is reserved. Nothing is applied here, so the reservation is
/// pure derived propose-state.
#[test]
fn quic_admission_refuses_an_in_flight_splits_child_id() {
  let ca = TestClusterCa::generate();
  let cluster = ClusterId([13u8; 16]);
  let opts = ca
    .cluster_tls(&san(1, &cluster))
    .tuning(QuicTuning::new().with_keep_alive_interval_millis(0))
    .build();
  let mut seed = [0u8; 32];
  seed[0] = 1;
  let mut c = MultiQuicCoordinator::<u64, u64, CountSm>::with_identity(opts, Some(seed), cluster);
  let (mut log, mut stable) = (VecLog::default(), AsyncStable::default());
  c.create_group(
    100,
    single_voter(1),
    Instant::ORIGIN,
    1,
    CountSm::default(),
    0,
    &NoFloors,
  )
  .unwrap();
  let d = c.group(&100).unwrap().poll_timeout().unwrap();
  c.handle_timeout(&100, d, &mut log, &mut stable).unwrap();
  for _ in 0..2 {
    c.handle_storage(&100, d, &mut log, &mut stable).unwrap();
  }
  assert!(c.group(&100).unwrap().role().is_leader());

  c.propose_split(
    &100,
    d,
    &mut log,
    &stable,
    &300,
    0,
    Bytes::from_static(b"\x02"),
    &NoFloors,
  )
  .expect("the parent is hosted")
  .expect("the leader appends the split");

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
      Instant::ORIGIN,
      9,
      CountSm::default(),
      0,
      &NoFloors
    ),
    Err(CreateGroupError::SplitReserved)
  );
  assert_eq!(
    c.restore_group(
      300,
      single_voter(1),
      Instant::ORIGIN,
      9,
      CountSm::default(),
      1,
      0,
      &NoFloors,
      &mut scratch_l,
      &mut scratch_s,
    ),
    Err(CreateGroupError::SplitReserved)
  );
  assert_eq!(
    c.create_group_from_fork(
      300,
      single_voter(1),
      Instant::ORIGIN,
      9,
      CountSm::default(),
      fork_blob(1),
      None,
      None,
      1,
      0,
      &NoFloors,
      &mut scratch_l,
      &mut scratch_s,
    ),
    Err(CreateGroupError::SplitReserved)
  );
  assert_eq!(
    scratch_l.last_index(),
    Index::ZERO,
    "every refusal wrote nothing"
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
  let ca = TestClusterCa::generate();
  let cluster = ClusterId([9u8; 16]);
  let opts = ca
    .cluster_tls(&san(1, &cluster))
    .tuning(QuicTuning::new().with_keep_alive_interval_millis(0))
    .build();
  let mut seed = [0u8; 32];
  seed[0] = 9;
  let mut coord =
    MultiQuicCoordinator::<u64, u64, CountSm>::with_identity(opts, Some(seed), cluster);
  let mut stores = Stores {
    map: BTreeMap::new(),
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
      coord.rollback_merge(&1, now, l, s, &2).unwrap(),
      Err(crate::MergeError::SourceMissing)
    ));
  }
  assert!(
    coord.group(&1).is_some_and(|ep| !ep.has_abandoned()),
    "no abort applied — the target records no thaw obligation"
  );
}
