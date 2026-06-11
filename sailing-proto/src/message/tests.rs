use super::*;

#[test]
fn message_construct_and_classify() {
  let rv = RequestVote::new(
    Term::new(2),
    1u64,
    Index::new(5),
    Term::new(1),
    false,
    false,
  );
  let m = Message::RequestVote(rv);
  assert!(m.is_request_vote());
  assert_eq!(m.try_unwrap_request_vote().unwrap().term(), Term::new(2));

  let out = Outgoing::new(
    3u64,
    Message::Heartbeat(Heartbeat::new(
      Term::new(2),
      1u64,
      Index::new(4),
      bytes::Bytes::new(),
    )),
  );
  assert_eq!(out.to(), 3u64);
  assert!(out.message().is_heartbeat());
}

#[test]
fn snapshot_meta_accessors() {
  use crate::conf::ConfState;
  use std::collections::BTreeSet;
  let voters = std::vec![1u64, 2u64, 3u64];
  let conf = ConfState::from_voters(voters.clone());
  let meta = SnapshotMeta::new(Index::new(42), Term::new(5), conf);
  assert_eq!(meta.last_index(), Index::new(42));
  assert_eq!(meta.last_term(), Term::new(5));
  let expected: BTreeSet<u64> = voters.into_iter().collect();
  assert_eq!(meta.conf().voters(), &expected);
}

#[test]
fn install_snapshot_accessors() {
  use crate::conf::ConfState;
  let meta = SnapshotMeta::new(
    Index::new(10),
    Term::new(3),
    ConfState::from_voters(std::vec![1u64]),
  );
  let data = bytes::Bytes::from_static(b"payload");
  let snap = InstallSnapshot::new(Term::new(7), 1u64, meta.clone(), data.clone());
  assert_eq!(snap.term(), Term::new(7));
  assert_eq!(snap.leader(), 1u64);
  assert_eq!(snap.snapshot().last_index(), meta.last_index());
  assert_eq!(snap.data(), &data);

  let m = Message::InstallSnapshot(snap);
  assert!(m.is_install_snapshot());
}

#[test]
fn snapshot_resp_accessors() {
  let resp = SnapshotResp::new(Term::new(4), 2u64, false, Index::new(10));
  assert_eq!(resp.term(), Term::new(4));
  assert_eq!(resp.from(), 2u64);
  assert!(!resp.reject());
  assert_eq!(resp.match_index(), Index::new(10));

  let m = Message::SnapshotResp(resp);
  assert!(m.is_snapshot_resp());
}

#[test]
fn codec_round_trips_every_variant() {
  use crate::{Data, Entry, EntryKind, conf::ConfState};
  use bytes::Bytes;

  fn rt(m: Message<u64>) {
    let mut buf = std::vec::Vec::new();
    m.encode(&mut buf);
    let (n, back) = Message::<u64>::decode(&buf).expect("decode");
    assert_eq!(n, buf.len(), "consumes exactly all bytes: {m:?}");
    assert_eq!(back, m, "round-trips: {m:?}");
  }

  let entries = std::vec![
    Entry::new(
      Term::new(1),
      Index::new(1),
      EntryKind::Normal,
      Bytes::from_static(b"a")
    ),
    Entry::new(Term::new(1), Index::new(2), EntryKind::Empty, Bytes::new()),
  ];
  rt(Message::AppendEntries(AppendEntries::new(
    Term::new(3),
    1,
    Index::new(2),
    Term::new(2),
    entries,
    Index::new(1),
  )));
  rt(Message::AppendResp(AppendResp::new(
    Term::new(3),
    2,
    true,
    Index::new(4),
    Term::new(2),
    Index::new(0),
  )));
  rt(Message::RequestVote(RequestVote::new(
    Term::new(3),
    1,
    Index::new(5),
    Term::new(2),
    true,
    false,
  )));
  rt(Message::VoteResp(VoteResp::new(
    Term::new(3),
    2,
    true,
    false,
  )));
  rt(Message::Heartbeat(
    Heartbeat::new(Term::new(3), 1, Index::new(4), Bytes::from_static(b"ctx")).with_lease_round(9),
  ));
  rt(Message::HeartbeatResp(
    HeartbeatResp::new(Term::new(3), 2, Bytes::from_static(b"ctx"))
      .with_lease_round(9)
      .with_lease_support(core::time::Duration::from_millis(150)),
  ));
  let meta = SnapshotMeta::new(
    Index::new(10),
    Term::new(3),
    ConfState::from_voters(std::vec![1u64, 2u64]),
  );
  rt(Message::InstallSnapshot(InstallSnapshot::new(
    Term::new(3),
    1,
    meta,
    Bytes::from_static(b"blob"),
  )));
  rt(Message::SnapshotResp(SnapshotResp::new(
    Term::new(3),
    2,
    false,
    Index::new(10),
  )));
  rt(Message::TimeoutNow(TimeoutNow::new(Term::new(3), 1)));
  rt(Message::ReadIndex(ReadIndex::new(
    Term::new(3),
    2,
    Bytes::from_static(b"r"),
  )));
  rt(Message::ReadIndexResp(ReadIndexResp::new(
    Term::new(3),
    1,
    Index::new(7),
    Bytes::from_static(b"r"),
    false,
  )));
}

#[test]
fn codec_rejects_truncated_input_without_panic() {
  use crate::Data;
  use bytes::Bytes;
  let m = Message::<u64>::Heartbeat(Heartbeat::new(
    Term::new(3),
    1,
    Index::new(4),
    Bytes::from_static(b"ctx"),
  ));
  let mut buf = std::vec::Vec::new();
  m.encode(&mut buf);
  for cut in 0..buf.len() {
    // Every strict prefix must error — never panic, never decode as a different value.
    assert!(
      Message::<u64>::decode(&buf[..cut]).is_err(),
      "prefix len {cut} must fail"
    );
  }
  assert!(Message::<u64>::decode(&buf).is_ok());
}

#[test]
fn codec_rejects_unknown_tag_and_empty() {
  use crate::Data;
  assert!(Message::<u64>::decode(&[0xFF]).is_err());
  assert!(Message::<u64>::decode(&[]).is_err());
}

#[test]
fn codec_rejects_oversized_collection_length() {
  use crate::Data;
  // AppendEntries claiming u64::MAX entries with no entry bytes must error, not OOM.
  let mut buf = std::vec::Vec::new();
  buf.push(0u8); // AppendEntries tag
  Term::new(1).encode(&mut buf);
  1u64.encode(&mut buf);
  Index::new(0).encode(&mut buf);
  Term::new(0).encode(&mut buf);
  u64::MAX.encode(&mut buf); // entries count = u64::MAX, with nothing following
  assert!(Message::<u64>::decode(&buf).is_err());
}

/// GOLDEN WIRE VECTORS: pin the exact byte encoding of representative messages, so any
/// wire-format drift (field reorder, width change, tag renumber) fails HERE — visibly — instead
/// of silently breaking cross-version clusters. If this test fails because the format was changed
/// ON PURPOSE, bump the transport hello version (`LABEL_VERSION` in transport/labeled.rs) and
/// regenerate the vectors in the same commit.
#[test]
fn codec_golden_byte_vectors() {
  use crate::Data;

  // VoteResp { term: 3, from: 2, pre_vote: true, reject: false } — tag 3, LE u64s, bool bytes.
  let mut buf = std::vec::Vec::new();
  Message::<u64>::VoteResp(VoteResp::new(Term::new(3), 2, true, false)).encode(&mut buf);
  assert_eq!(
    buf,
    [
      3, // tag: VoteResp
      3, 0, 0, 0, 0, 0, 0, 0, // term = 3 (LE u64)
      2, 0, 0, 0, 0, 0, 0, 0, // from = 2 (LE u64)
      1, // pre_vote = true
      0, // reject = false
    ]
  );

  // Heartbeat { term: 1, leader: 9, commit: 4, context: "ab", lease_round: 7 } — tag 4;
  // Bytes carry a LE u64 length prefix.
  let mut buf = std::vec::Vec::new();
  Message::<u64>::Heartbeat(
    Heartbeat::new(
      Term::new(1),
      9,
      Index::new(4),
      bytes::Bytes::from_static(b"ab"),
    )
    .with_lease_round(7),
  )
  .encode(&mut buf);
  assert_eq!(
    buf,
    [
      4, // tag: Heartbeat
      1, 0, 0, 0, 0, 0, 0, 0, // term = 1
      9, 0, 0, 0, 0, 0, 0, 0, // leader = 9
      4, 0, 0, 0, 0, 0, 0, 0, // commit = 4
      2, 0, 0, 0, 0, 0, 0, 0, // context length = 2
      b'a', b'b', // context bytes
      7, 0, 0, 0, 0, 0, 0, 0, // lease_round = 7
    ]
  );

  // TimeoutNow { term: 2, leader: 5 } — tag 8, the smallest message.
  let mut buf = std::vec::Vec::new();
  Message::<u64>::TimeoutNow(TimeoutNow::new(Term::new(2), 5)).encode(&mut buf);
  assert_eq!(buf, [8, 2, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0,]);
}

/// The HeartbeatResp duration decoder rejects an out-of-domain nanos field (>= 1e9): two distinct
/// encodings must never decode to the same Duration.
#[test]
fn codec_rejects_out_of_range_duration_nanos() {
  use crate::Data;
  let mut buf = std::vec::Vec::new();
  Message::<u64>::HeartbeatResp(
    HeartbeatResp::new(Term::new(1), 2, bytes::Bytes::new())
      .with_lease_support(core::time::Duration::from_millis(5)),
  )
  .encode(&mut buf);
  // Locate the nanos field: it is the LAST 8 bytes of the encoding (secs precedes it).
  let n = buf.len();
  buf[n - 8..].copy_from_slice(&1_000_000_000u64.to_le_bytes()); // nanos = 1e9: out of domain
  assert!(
    Message::<u64>::decode(&buf).is_err(),
    "nanos >= 1e9 must be rejected (canonical duration encoding)"
  );
}

/// One representative value of EVERY Message variant (shared by the round-trip and the
/// truncation sweep, so a new variant must be added here to be covered by both).
fn all_variants() -> std::vec::Vec<Message<u64>> {
  use crate::{Entry, EntryKind, conf::ConfState};
  use bytes::Bytes;
  let entries = std::vec![
    Entry::new(
      Term::new(1),
      Index::new(1),
      EntryKind::Normal,
      Bytes::from_static(b"a")
    ),
    Entry::new(Term::new(1), Index::new(2), EntryKind::Empty, Bytes::new()),
  ];
  let meta = SnapshotMeta::new(
    Index::new(10),
    Term::new(3),
    ConfState::from_voters(std::vec![1u64, 2u64]),
  );
  std::vec![
    Message::AppendEntries(AppendEntries::new(
      Term::new(3),
      1,
      Index::new(2),
      Term::new(2),
      entries,
      Index::new(1),
    )),
    Message::AppendResp(AppendResp::new(
      Term::new(3),
      2,
      true,
      Index::new(4),
      Term::new(2),
      Index::new(0),
    )),
    Message::RequestVote(RequestVote::new(
      Term::new(3),
      1,
      Index::new(5),
      Term::new(2),
      true,
      false,
    )),
    Message::VoteResp(VoteResp::new(Term::new(3), 2, true, false)),
    Message::Heartbeat(
      Heartbeat::new(Term::new(3), 1, Index::new(4), Bytes::from_static(b"ctx"))
        .with_lease_round(9),
    ),
    Message::HeartbeatResp(
      HeartbeatResp::new(Term::new(3), 2, Bytes::from_static(b"ctx"))
        .with_lease_round(9)
        .with_lease_support(core::time::Duration::from_millis(150)),
    ),
    Message::InstallSnapshot(InstallSnapshot::new(
      Term::new(3),
      1,
      meta,
      Bytes::from_static(b"blob"),
    )),
    Message::SnapshotResp(SnapshotResp::new(Term::new(3), 2, false, Index::new(10))),
    Message::TimeoutNow(TimeoutNow::new(Term::new(3), 1)),
    Message::ReadIndex(ReadIndex::new(Term::new(3), 2, Bytes::from_static(b"r"))),
    Message::ReadIndexResp(ReadIndexResp::new(
      Term::new(3),
      1,
      Index::new(7),
      Bytes::from_static(b"r"),
      false,
    )),
  ]
}

/// EXHAUSTIVE truncation sweep across EVERY variant: each strict prefix of each encoding must
/// error cleanly — never panic, never decode as a different value.
#[test]
fn codec_truncation_sweep_covers_every_variant() {
  use crate::Data;
  for m in all_variants() {
    let mut buf = std::vec::Vec::new();
    m.encode(&mut buf);
    for cut in 0..buf.len() {
      assert!(
        Message::<u64>::decode(&buf[..cut]).is_err(),
        "{m:?}: prefix of len {cut} must fail"
      );
    }
    let (n, back) = Message::<u64>::decode(&buf).expect("full decode");
    assert_eq!(n, buf.len());
    assert_eq!(back, m);
  }
}
