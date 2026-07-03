use super::*;
use crate::{
  testkit::{AsyncStable, CountSm, VecLog},
  transport::quic::{QuicTuning, crypto::tests::TestClusterCa},
};
use core::time::Duration;
use std::{collections::BTreeMap, string::String};

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
