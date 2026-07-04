use super::{ShardMap, fnv1a, shard_addr};

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
