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
  now = elect_a(&mut a, &mut b, &mut sa, &mut sb, 100, now);
  now = elect_a(&mut a, &mut b, &mut sa, &mut sb, 200, now);
  let _ = drain_controls(&mut b);

  // b de-hosts group 200; a (unaware) keeps beating both groups in one coalesced frame.
  assert!(b.remove_group(&200).is_some());
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
/// the shared connection and the co-located group's traffic untouched, no control — until a
/// create re-admits the SAME id, which lifts the tombstone and lets traffic reach the fresh
/// replica again.
#[test]
fn tombstoned_group_drops_entries_silently_until_recreated() {
  use crate::GroupControl;
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
  now = elect_a(&mut a, &mut b, &mut sa, &mut sb, 100, now);
  now = elect_a(&mut a, &mut b, &mut sa, &mut sb, 200, now);
  let _ = drain_controls(&mut b);

  assert!(!b.is_retired(&100), "a hosted group is not tombstoned");
  assert!(b.remove_group(&100).is_some());
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

  // Re-admitting the SAME id lifts the tombstone; the fresh replica (fresh stores) hears the
  // still-beating leader again.
  sb.map
    .insert(100, (VecLog::default(), AsyncStable::default()));
  b.create_group(100, two_voter(2), now, 2, CountSm::default())
    .unwrap();
  assert!(!b.is_retired(&100), "re-admission lifts the tombstone");
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
    a.create_group(g, two_voter(1), Instant::ORIGIN, 1, CountSm::default())
      .unwrap();
  }
  b.create_group(100, two_voter(2), Instant::ORIGIN, 2, CountSm::default())
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
  assert!(b.remove_group(&200).is_none(), "b never hosted 200");
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
  a.create_group(100, two_voter(1), Instant::ORIGIN, 1, CountSm::default())
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
  b.create_group(100, two_voter(2), now, 2, CountSm::default())
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
