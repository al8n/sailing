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
