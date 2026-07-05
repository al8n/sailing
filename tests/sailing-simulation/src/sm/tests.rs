use super::*;
use crate::multi::encode_gkv;
use sailing_proto::StateMachine;

#[test]
fn log_sm_records_applies_in_order() {
  let mut sm = LogSm::new();
  let r1 = sm
    .apply(Index::new(1), bytes::Bytes::from_static(b"a"))
    .unwrap();
  let r2 = sm
    .apply(Index::new(2), bytes::Bytes::from_static(b"bb"))
    .unwrap();
  assert_eq!(r1, 1); // response = applied byte length
  assert_eq!(r2, 2);
  assert_eq!(
    sm.applied(),
    &[
      (Index::new(1), bytes::Bytes::from_static(b"a")),
      (Index::new(2), bytes::Bytes::from_static(b"bb"))
    ]
  );
}

#[test]
fn log_sm_snapshot_restore_roundtrip() {
  let mut sm = LogSm::new();
  sm.apply(Index::new(1), bytes::Bytes::from_static(b"alpha"))
    .unwrap();
  sm.apply(Index::new(2), bytes::Bytes::from_static(b"beta"))
    .unwrap();
  sm.apply(Index::new(3), bytes::Bytes::from_static(b"gamma"))
    .unwrap();

  let snap = sm.snapshot().unwrap();
  let mut sm2 = LogSm::new();
  sm2.restore(snap).unwrap();
  assert_eq!(
    sm.applied(),
    sm2.applied(),
    "restore must reproduce exact state"
  );
}

#[test]
fn log_sm_empty_snapshot_roundtrip() {
  let sm = LogSm::new();
  let snap = sm.snapshot().unwrap();
  let mut sm2 = LogSm::new();
  sm2.restore(snap).unwrap();
  assert!(sm2.applied().is_empty());
}

#[test]
fn restore_malformed_returns_err_never_panics() {
  // empty buffer — can't read the count
  let mut sm = LogSm::new();
  assert!(sm.restore(bytes::Bytes::new()).is_err());

  // declared count=1 but no body follows
  let mut buf: Vec<u8> = Vec::new();
  (1u64).encode(&mut buf); // count = 1
  assert!(sm.restore(bytes::Bytes::from(buf)).is_err());

  // declared count=1 with index+len present but payload absent (len says 100, buf empty after)
  let mut buf2: Vec<u8> = Vec::new();
  (1u64).encode(&mut buf2); // count = 1
  (42u64).encode(&mut buf2); // index = 42
  (100u64).encode(&mut buf2); // payload len = 100, but no payload bytes follow
  assert!(sm.restore(bytes::Bytes::from(buf2)).is_err());

  // absurd length prefix (u64::MAX) — must not overflow/panic
  let mut buf3: Vec<u8> = Vec::new();
  (1u64).encode(&mut buf3); // count = 1
  (1u64).encode(&mut buf3); // index = 1
  (u64::MAX).encode(&mut buf3); // payload len = u64::MAX
  assert!(sm.restore(bytes::Bytes::from(buf3)).is_err());

  // state must be untouched after failed restores
  assert!(sm.applied().is_empty());
}

/// A parent with one write per gkv key 0..8 (indices 1..=8) plus one un-keyed command at 9.
fn keyed_sm() -> LogSm {
  let mut sm = LogSm::new();
  for key in 0u16..8 {
    let cmd = bytes::Bytes::from(encode_gkv(100, key, 1_000 + u64::from(key)));
    sm.apply(Index::new(u64::from(key) + 1), cmd).unwrap();
  }
  sm.apply(Index::new(9), bytes::Bytes::from_static(b"unkeyed"))
    .unwrap();
  sm
}

/// The partition property under the 2-byte LE split-point contract: every gkv cell lands in
/// exactly one side by `key >= point`, un-keyed commands stay with the parent, both sides
/// preserve order and indices, and the union is exactly the original record.
#[test]
fn split_partitions_keys_at_the_point() {
  let original = keyed_sm();
  let mut parent = original.clone();
  let child = parent.split(&4u16.to_le_bytes()).expect("supported");

  let keys = |sm: &LogSm| -> Vec<u16> {
    sm.applied()
      .iter()
      .filter_map(|(_, cmd)| crate::multi::decode_gkv(cmd))
      .map(|(_, k, _)| k)
      .collect()
  };
  assert_eq!(keys(&parent), std::vec![0, 1, 2, 3]);
  assert_eq!(keys(&child), std::vec![4, 5, 6, 7]);
  assert_eq!(
    parent.applied().last(),
    Some(&(Index::new(9), bytes::Bytes::from_static(b"unkeyed"))),
    "an un-keyed command has no side to move to — it stays with the parent"
  );

  // Union exactness: interleave-merge both sides by original index ⇒ the original record.
  let mut union: Vec<(Index, bytes::Bytes)> = parent
    .applied()
    .iter()
    .chain(child.applied().iter())
    .cloned()
    .collect();
  union.sort_by_key(|(idx, _)| *idx);
  assert_eq!(union.as_slice(), original.applied());
}

/// `split` is a pure function of `(state, instruction)`: the same split on a clone yields an
/// identical child AND an identical shrunk parent, and re-splitting the already-shrunk parent at
/// the same point moves nothing more.
#[test]
fn split_is_deterministic_across_clones_and_repeats() {
  let mut a = keyed_sm();
  let mut b = a.clone();
  let child_a = a.split(&4u16.to_le_bytes()).expect("supported");
  let child_b = b.split(&4u16.to_le_bytes()).expect("supported");
  assert_eq!(child_a, child_b, "same state + instruction ⇒ same child");
  assert_eq!(a, b, "same state + instruction ⇒ same shrunk parent");

  let again = a.split(&4u16.to_le_bytes()).expect("supported");
  assert!(
    again.applied().is_empty(),
    "the moved keys are GONE from the parent — a repeat moves nothing"
  );
}

/// Anything but exactly 2 bytes is a malformed instruction: `None`, state untouched. (An empty
/// key population is still a WELL-FORMED split — point 0 moves every keyed cell.)
#[test]
fn split_rejects_malformed_instructions_untouched() {
  let mut sm = keyed_sm();
  let before = sm.clone();
  for bad in [&b""[..], &b"\x04"[..], &b"\x04\x00\x00"[..]] {
    assert!(sm.split(bad).is_none(), "{} bytes must refuse", bad.len());
    assert_eq!(sm, before, "a refused split must not touch state");
  }
  let all = sm
    .split(&0u16.to_le_bytes())
    .expect("point 0 is well-formed");
  assert_eq!(all.applied().len(), 8, "point 0 moves every keyed cell");
}

/// The malformed arm END-TO-END: a COMMITTED `Split` entry carrying a malformed instruction
/// reaches `LogSm::split` at the deterministic apply point, gets `None`, and the endpoint
/// fail-stops with `PoisonReason::SplitUnsupported` — never a silent skip. Driven through the
/// real container machinery on a single-voter group (the world's `propose_split` builds only
/// well-formed instructions, so this pins the arm the fuzzer cannot reach).
#[test]
fn committed_split_with_malformed_instruction_poisons() {
  use sailing_proto::{Config, Instant, MultiRaft, StorageProgress};

  let mut m: MultiRaft<u64, u64, LogSm> = MultiRaft::new();
  let mut log = crate::MemLog::new();
  let mut stable = crate::MemStable::new();
  let config = Config::try_new(
    1u64,
    std::vec![1u64],
    core::time::Duration::from_millis(1000),
    core::time::Duration::from_millis(100),
  )
  .expect("valid config");
  m.create_group(100u64, config, Instant::ORIGIN, 7, LogSm::new())
    .expect("admission");

  // A single voter elects itself on its first due timer.
  let due = m
    .group(&100)
    .expect("hosted")
    .poll_timeout()
    .expect("armed");
  m.handle_timeout(&100, due, &mut log, &mut stable)
    .expect("hosted");
  while matches!(
    m.handle_storage(&100, due, &mut log, &mut stable),
    Some(StorageProgress::MorePending)
  ) {}
  assert!(m.group(&100).expect("hosted").role().is_leader());

  // Propose a split whose instruction is ONE byte — well-formed to the container (opaque), but
  // malformed under the sim FSM's 2-byte contract.
  m.propose_split(
    &100,
    due,
    &mut log,
    &stable,
    &200u64,
    0,
    bytes::Bytes::from_static(b"\x04"),
  )
  .expect("hosted")
  .expect("the container accepts an opaque instruction");
  m.flush_appends(&100, due, &log, &stable).expect("hosted");
  while matches!(
    m.handle_storage(&100, due, &mut log, &mut stable),
    Some(StorageProgress::MorePending)
  ) {}

  let ep = m.group(&100).expect("hosted");
  assert!(ep.is_poisoned(), "a committed unsupported split fail-stops");
  assert_eq!(
    ep.poison_reason(),
    Some(sailing_proto::PoisonReason::SplitUnsupported)
  );
}
