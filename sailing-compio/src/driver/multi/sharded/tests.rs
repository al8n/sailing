use super::{ShardGuardedFactory, ShardMap, fnv1a, shard_addr};

/// The uniform map is deterministic (no per-process hash state) and stays inside the plane
/// bound — the two properties the cluster-wide-consistency contract rides on.
#[test]
fn uniform_map_is_deterministic_and_bounded() {
  let a = ShardMap::<u64>::uniform(4);
  let b = ShardMap::<u64>::uniform(4);
  for gid in [0u64, 1, 7, 100, 200, 0xFFFF_FFFF_FFFF_FFFF] {
    let shard = a.shard(&gid);
    assert!(shard < 4, "shard {shard} out of bounds for gid {gid}");
    assert_eq!(shard, b.shard(&gid), "two maps disagree on gid {gid}");
    assert_eq!(shard, a.shard(&gid), "one map disagrees with itself");
  }
}

/// FNV-1a matches the published 64-bit test vectors — the fixed-algorithm half of the
/// cross-process determinism contract (the other half is the id's canonical `Data` encoding).
#[test]
fn fnv1a_matches_the_reference_vectors() {
  assert_eq!(fnv1a(b""), 0xcbf2_9ce4_8422_2325);
  assert_eq!(fnv1a(b"a"), 0xaf63_dc4c_8601_ec8c);
  assert_eq!(fnv1a(b"foobar"), 0x8594_4171_f739_67e8);
}

/// A custom mapping is honored — and folded `% shards`, so a wild return value can never index
/// out of the plane vector.
#[test]
fn custom_mapping_is_used_and_folded() {
  let map = ShardMap::with_mapping(2, |gid: &u64| (*gid / 100) as usize);
  assert_eq!(map.shard(&100), 1);
  assert_eq!(map.shard(&200), 0, "2 % 2 folds back into range");
  assert_eq!(map.shard(&500), 1, "5 % 2 folds back into range");
}

/// A zero shard count is clamped to one (the drivers' `cmd_budget.max(1)` convention), keeping
/// `shard()` total.
#[test]
fn zero_shards_clamps_to_one() {
  let map = ShardMap::<u64>::uniform(0);
  assert_eq!(map.shards(), 1);
  assert_eq!(map.shard(&42), 0);
}

/// The shard guard declines a group the map assigns to a DIFFERENT plane before the embedder's
/// factory is ever consulted (its catalog is never probed with wrong-plane ids), and a group
/// mapped to THIS plane flows through both phases untouched.
#[test]
fn shard_guard_declines_wrong_plane_groups_before_the_factory() {
  use core::time::Duration;
  use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
  };

  use sailing_driver::{BoxedGroupFactory, GroupBlueprint, GroupFactory, factory_fn};

  let consulted = Arc::new(AtomicUsize::new(0));
  let counter = consulted.clone();
  let inner: BoxedGroupFactory<u64, u64, Vec<u8>> = Box::new(factory_fn(
    move |group: &u64, _from: &u64| {
      counter.fetch_add(1, Ordering::SeqCst);
      let config = sailing_proto::Config::try_new(
        2u64,
        vec![1, 2],
        Duration::from_millis(1000),
        Duration::from_millis(100),
      )
      .expect("a valid seed config");
      Some(GroupBlueprint::new(config, *group))
    },
    |_group: &u64| Some(Vec::new()),
  ));
  // The identity map over 2 planes (group 0 → plane 0, group 1 → plane 1); the guard sits on
  // plane 0.
  let mut guard = ShardGuardedFactory {
    inner,
    map: ShardMap::with_mapping(2, |g: &u64| *g as usize),
    plane: 0,
  };

  assert!(
    guard.materialize(&1, &7).is_none(),
    "a wrong-plane group is declined"
  );
  assert_eq!(
    consulted.load(Ordering::SeqCst),
    0,
    "the inner factory was never consulted for the wrong-plane group"
  );

  assert!(
    guard.materialize(&0, &7).is_some(),
    "an own-plane group delegates to the inner factory"
  );
  assert_eq!(consulted.load(Ordering::SeqCst), 1);
  assert!(guard.build(&0).is_some(), "build delegates untouched");
}

/// The port convention advances the port and preserves the IP; overflow is a `None`, never a
/// wrap (a wrapped port would silently dial an unrelated service).
#[test]
fn shard_addr_advances_the_port_and_refuses_overflow() {
  let base: std::net::SocketAddr = "127.0.0.1:45000".parse().unwrap();
  assert_eq!(shard_addr(base, 0), Some(base));
  assert_eq!(
    shard_addr(base, 3),
    Some("127.0.0.1:45003".parse().unwrap())
  );
  let near_top: std::net::SocketAddr = "127.0.0.1:65535".parse().unwrap();
  assert_eq!(shard_addr(near_top, 0), Some(near_top));
  assert_eq!(shard_addr(near_top, 1), None, "u16 overflow is refused");
}
