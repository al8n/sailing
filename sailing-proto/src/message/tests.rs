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
