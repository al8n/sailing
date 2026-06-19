use super::*;
use crate::{
  AppendEntries, AppendResp, Heartbeat, HeartbeatResp, InstallSnapshot, ReadIndex, ReadIndexResp,
  RequestVote, SnapshotResp, TimeoutNow, VoteResp, conf::ConfState,
};

fn rt(m: Message<u64>) {
  let mut buf = std::vec::Vec::new();
  encode_message(&m, &mut buf);
  let back = decode_message::<u64>(Bytes::from(buf)).expect("decode");
  assert_eq!(back, m, "round-trips: {m:?}");
}

/// Every Message variant survives encode → decode with value identity.
#[test]
fn round_trips_every_variant() {
  let entries = std::vec![
    Entry::new(
      Term::new(1),
      Index::new(1),
      EntryKind::Normal,
      Bytes::from_static(b"a")
    ),
    Entry::new(Term::new(1), Index::new(2), EntryKind::Empty, Bytes::new()),
    Entry::new(
      Term::new(2),
      Index::new(3),
      EntryKind::ConfChange,
      Bytes::from_static(b"cc")
    ),
    Entry::new(
      Term::new(2),
      Index::new(4),
      EntryKind::SetReadMode,
      Bytes::from_static(b"\x02"),
    ),
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

/// A LeaseGuard entry timestamp round-trips through the envelope (and a zero one is absent on
/// the wire, so non-LeaseGuard entries are byte-identical to before the field existed).
#[test]
fn entry_timestamp_round_trips() {
  let stamped = Entry::new(
    Term::new(2),
    Index::new(3),
    EntryKind::Normal,
    Bytes::from_static(b"x"),
  )
  .with_timestamp(1_234_567_890);
  let m = Message::AppendEntries(AppendEntries::new(
    Term::new(2),
    1,
    Index::ZERO,
    Term::ZERO,
    std::vec![stamped.clone()],
    Index::ZERO,
  ));
  let mut buf = std::vec::Vec::new();
  encode_message(&m, &mut buf);
  let Message::AppendEntries(back) = decode_message::<u64>(Bytes::from(buf)).expect("decode")
  else {
    panic!("variant")
  };
  assert_eq!(back.entries()[0].timestamp(), 1_234_567_890);
  assert_eq!(back.entries()[0], stamped);

  // A zero timestamp is omitted on the wire: the encoding is byte-identical to an entry built
  // without ever touching the timestamp field.
  let plain = Entry::new(
    Term::new(2),
    Index::new(3),
    EntryKind::Normal,
    Bytes::from_static(b"x"),
  );
  let mut a = std::vec::Vec::new();
  let mut b = std::vec::Vec::new();
  encode_message(
    &Message::AppendEntries(AppendEntries::new(
      Term::new(2),
      1,
      Index::ZERO,
      Term::ZERO,
      std::vec![plain.clone()],
      Index::ZERO,
    )),
    &mut a,
  );
  encode_message(
    &Message::AppendEntries(AppendEntries::new(
      Term::new(2),
      1,
      Index::ZERO,
      Term::ZERO,
      std::vec![plain.with_timestamp(0)],
      Index::ZERO,
    )),
    &mut b,
  );
  assert_eq!(a, b, "a zero timestamp must be absent on the wire");
}

/// A LeaseGuard failover wall-timestamp round-trips through the envelope, and a zero one is absent
/// on the wire (so non-failover entries stay byte-identical to before the field existed).
#[test]
fn entry_wall_timestamp_round_trips() {
  let stamped = Entry::new(
    Term::new(2),
    Index::new(3),
    EntryKind::Normal,
    Bytes::from_static(b"x"),
  )
  .with_wall_timestamp(1_700_000_000_000_000_000);
  let m = Message::AppendEntries(AppendEntries::new(
    Term::new(2),
    1,
    Index::ZERO,
    Term::ZERO,
    std::vec![stamped.clone()],
    Index::ZERO,
  ));
  let mut buf = std::vec::Vec::new();
  encode_message(&m, &mut buf);
  let Message::AppendEntries(back) = decode_message::<u64>(Bytes::from(buf)).expect("decode")
  else {
    panic!("variant")
  };
  assert_eq!(
    back.entries()[0].wall_timestamp(),
    1_700_000_000_000_000_000
  );
  assert_eq!(back.entries()[0], stamped);

  // A zero wall_timestamp is omitted on the wire: byte-identical to an entry that never set it.
  let plain = Entry::new(
    Term::new(2),
    Index::new(3),
    EntryKind::Normal,
    Bytes::from_static(b"x"),
  );
  let mut a = std::vec::Vec::new();
  let mut b = std::vec::Vec::new();
  encode_message(
    &Message::AppendEntries(AppendEntries::new(
      Term::new(2),
      1,
      Index::ZERO,
      Term::ZERO,
      std::vec![plain.clone()],
      Index::ZERO,
    )),
    &mut a,
  );
  encode_message(
    &Message::AppendEntries(AppendEntries::new(
      Term::new(2),
      1,
      Index::ZERO,
      Term::ZERO,
      std::vec![plain.with_wall_timestamp(0)],
      Index::ZERO,
    )),
    &mut b,
  );
  assert_eq!(a, b, "a zero wall_timestamp must be absent on the wire");
}

/// SnapshotMeta.max_wall_plus_window round-trips, and a zero is absent on the wire (byte-identical
/// to a snapshot built before the field existed).
#[test]
fn snapshot_meta_max_wall_plus_window_round_trips() {
  let conf = ConfState::new(
    std::vec![1u64, 2, 3],
    std::vec![],
    std::vec![],
    std::vec![],
    false,
  );
  let meta = SnapshotMeta::new(Index::new(10), Term::new(4), conf.clone())
    .with_max_wall_plus_window(1_700_000_000_000_000_999);
  let m = Message::InstallSnapshot(InstallSnapshot::new(
    Term::new(4),
    1,
    meta,
    Bytes::from_static(b"blob"),
  ));
  let mut buf = std::vec::Vec::new();
  encode_message(&m, &mut buf);
  let Message::InstallSnapshot(back) = decode_message::<u64>(Bytes::from(buf)).expect("decode")
  else {
    panic!("variant")
  };
  assert_eq!(
    back.snapshot().max_wall_plus_window(),
    1_700_000_000_000_000_999
  );

  // A zero max_wall_plus_window is omitted on the wire: byte-identical to a snapshot that never set it.
  let plain = SnapshotMeta::new(Index::new(10), Term::new(4), conf);
  let mut a = std::vec::Vec::new();
  let mut b = std::vec::Vec::new();
  encode_message(
    &Message::InstallSnapshot(InstallSnapshot::new(
      Term::new(4),
      1,
      plain.clone(),
      Bytes::from_static(b"blob"),
    )),
    &mut a,
  );
  encode_message(
    &Message::InstallSnapshot(InstallSnapshot::new(
      Term::new(4),
      1,
      plain.with_max_wall_plus_window(0),
      Bytes::from_static(b"blob"),
    )),
    &mut b,
  );
  assert_eq!(
    a, b,
    "a zero max_wall_plus_window must be absent on the wire"
  );
}

/// SnapshotMeta.max_unwalled_lease_window round-trips, and a hand-built ZERO is omitted on the wire
/// (the codec's absent-when-zero rule — note the entry-property fold makes the value NON-zero for any
/// real LeaseGuard snapshot, so this exercises the codec directly, not a non-failover snapshot).
#[test]
fn snapshot_meta_max_unwalled_lease_window_round_trips() {
  let conf = ConfState::new(
    std::vec![1u64, 2, 3],
    std::vec![],
    std::vec![],
    std::vec![],
    false,
  );
  let meta = SnapshotMeta::new(Index::new(10), Term::new(4), conf.clone())
    .with_max_unwalled_lease_window(420_000_000);
  let m = Message::InstallSnapshot(InstallSnapshot::new(
    Term::new(4),
    1,
    meta,
    Bytes::from_static(b"blob"),
  ));
  let mut buf = std::vec::Vec::new();
  encode_message(&m, &mut buf);
  let Message::InstallSnapshot(back) = decode_message::<u64>(Bytes::from(buf)).expect("decode")
  else {
    panic!("variant")
  };
  assert_eq!(back.snapshot().max_unwalled_lease_window(), 420_000_000);

  // A zero max_unwalled_lease_window is omitted on the wire (the codec's absent-when-zero rule; a real
  // LeaseGuard snapshot's value is non-zero under the entry-property fold).
  let plain = SnapshotMeta::new(Index::new(10), Term::new(4), conf);
  let mut a = std::vec::Vec::new();
  let mut b = std::vec::Vec::new();
  encode_message(
    &Message::InstallSnapshot(InstallSnapshot::new(
      Term::new(4),
      1,
      plain.clone(),
      Bytes::from_static(b"blob"),
    )),
    &mut a,
  );
  encode_message(
    &Message::InstallSnapshot(InstallSnapshot::new(
      Term::new(4),
      1,
      plain.with_max_unwalled_lease_window(0),
      Bytes::from_static(b"blob"),
    )),
    &mut b,
  );
  assert_eq!(
    a, b,
    "a zero max_unwalled_lease_window must be absent on the wire"
  );
}

/// Zero-valued scalars (proto3 omits them) round-trip to the same values.
#[test]
fn round_trips_zero_defaults() {
  rt(Message::VoteResp(VoteResp::new(
    Term::ZERO,
    1,
    false,
    false,
  )));
  rt(Message::Heartbeat(Heartbeat::new(
    Term::ZERO,
    1,
    Index::ZERO,
    Bytes::new(),
  )));
}

/// A joint-config ConfState (all four sets + auto_leave) survives the envelope.
#[test]
fn round_trips_joint_conf_state() {
  let conf = ConfState::new(
    std::vec![1u64, 2, 3],
    std::vec![7u64],
    std::vec![1u64, 2],
    std::vec![9u64],
    true,
  );
  let meta = SnapshotMeta::new(Index::new(5), Term::new(2), conf);
  rt(Message::InstallSnapshot(InstallSnapshot::new(
    Term::new(2),
    1,
    meta,
    Bytes::new(),
  )));
}

/// Truncating an encoded message at EVERY byte boundary errors and never panics. (The
/// frame layer normally guarantees whole-message delivery; this pins the decoder's
/// behavior on a corrupt frame.)
#[test]
fn truncation_never_panics_and_never_misdecodes() {
  let m = Message::AppendEntries(AppendEntries::new(
    Term::new(3),
    1u64,
    Index::new(2),
    Term::new(2),
    std::vec![Entry::new(
      Term::new(1),
      Index::new(1),
      EntryKind::Normal,
      Bytes::from_static(b"payload")
    )],
    Index::new(1),
  ));
  let mut buf = std::vec::Vec::new();
  encode_message(&m, &mut buf);
  for cut in 0..buf.len() {
    let r = decode_message::<u64>(Bytes::copy_from_slice(&buf[..cut]));
    if let Ok(decoded) = r {
      // A protobuf prefix can be a VALID shorter message (field-granular truncation);
      // it must never equal the full message.
      assert_ne!(
        decoded, m,
        "a truncation at {cut} must not decode as the full message"
      );
    }
  }
  assert!(
    decode_message::<u64>(Bytes::new()).is_err(),
    "an empty frame has no body and rejects"
  );
}

/// The conversion validations: each crafted envelope rejects.
#[test]
fn conversion_rejections() {
  use buffa::Message as _;

  fn enc(msg: &pb::Message) -> Bytes {
    let mut buf = std::vec::Vec::new();
    msg.encode(&mut buf);
    Bytes::from(buf)
  }
  fn wrap(body: pb::message::Body) -> pb::Message {
    pb::Message {
      body: Some(body),
      ..Default::default()
    }
  }

  // Absent body.
  assert!(
    decode_message::<u64>(enc(&pb::Message::default())).is_err(),
    "absent body rejects"
  );

  // Empty id field.
  let vr = pb::VoteResp {
    term: 3,
    ..Default::default()
  };
  assert!(
    decode_message::<u64>(enc(&wrap(pb::message::Body::from(vr)))).is_err(),
    "an absent/empty id field rejects"
  );

  // Oversized id field (> 1024 bytes).
  let vr = pb::VoteResp {
    from_id: Bytes::from(std::vec![0u8; 1025]),
    ..Default::default()
  };
  assert!(
    decode_message::<u64>(enc(&wrap(pb::message::Body::from(vr)))).is_err(),
    "an oversized id rejects"
  );

  // An id with trailing bytes (a u64 id is exactly 8 bytes).
  let vr = pb::VoteResp {
    from_id: Bytes::from(std::vec![1u8; 9]),
    ..Default::default()
  };
  assert!(
    decode_message::<u64>(enc(&wrap(pb::message::Body::from(vr)))).is_err(),
    "an id that does not consume exactly rejects"
  );

  // Out-of-range lease nanos.
  let hr = pb::HeartbeatResp {
    from_id: encode_id(&2u64),
    lease_support_nanos: 1_000_000_000,
    ..Default::default()
  };
  assert!(
    decode_message::<u64>(enc(&wrap(pb::message::Body::from(hr)))).is_err(),
    "lease nanos >= 1e9 rejects"
  );

  // The uint32-truncation shape: 2^32 + k would truncate to an in-range k under a uint32
  // field (protobuf truncates oversized uint32 varints by spec); the uint64 schema field
  // sees the full value and the bound rejects it.
  let hr = pb::HeartbeatResp {
    from_id: encode_id(&2u64),
    lease_support_nanos: (1u64 << 32) + 999_999_999,
    ..Default::default()
  };
  assert!(
    decode_message::<u64>(enc(&wrap(pb::message::Body::from(hr)))).is_err(),
    "an oversized nanos varint rejects instead of truncating into range"
  );

  // Unknown enum value in an entry kind.
  let e = pb::Entry {
    kind: EnumValue::Unknown(99),
    ..Default::default()
  };
  let ae = pb::AppendEntries {
    leader_id: encode_id(&1u64),
    entries: std::vec![e],
    ..Default::default()
  };
  assert!(
    decode_message::<u64>(enc(&wrap(pb::message::Body::from(ae)))).is_err(),
    "an unknown EntryKind rejects"
  );

  // A snapshot without its meta sub-message.
  let is = pb::InstallSnapshot {
    leader_id: encode_id(&1u64),
    ..Default::default()
  };
  assert!(
    decode_message::<u64>(enc(&wrap(pb::message::Body::from(is)))).is_err(),
    "InstallSnapshot without meta rejects"
  );
}

/// Membership sets must be STRICTLY ASCENDING by decoded value: duplicates and disorder
/// reject — one set has exactly one accepted encoding.
#[test]
fn set_order_discipline() {
  use buffa::Message as _;

  fn snapshot_with_voters(voters: std::vec::Vec<u64>) -> Bytes {
    let cs = pb::ConfState {
      voters: voters.iter().map(encode_id).collect(),
      ..Default::default()
    };
    let meta = pb::SnapshotMeta {
      conf: buffa::MessageField::some(cs),
      ..Default::default()
    };
    let is = pb::InstallSnapshot {
      leader_id: encode_id(&1u64),
      snapshot: buffa::MessageField::some(meta),
      ..Default::default()
    };
    let msg = pb::Message {
      body: Some(pb::message::Body::from(is)),
      ..Default::default()
    };
    let mut buf = std::vec::Vec::new();
    msg.encode(&mut buf);
    Bytes::from(buf)
  }

  assert!(
    decode_message::<u64>(snapshot_with_voters(std::vec![1, 2, 3])).is_ok(),
    "ascending voters accepted"
  );
  assert!(
    decode_message::<u64>(snapshot_with_voters(std::vec![2, 1, 3])).is_err(),
    "disorder rejects"
  );
  assert!(
    decode_message::<u64>(snapshot_with_voters(std::vec![1, 1, 2])).is_err(),
    "duplicates reject"
  );
}

/// ConfChangeV2 entry payloads round-trip and reject unknown discriminants.
#[test]
fn conf_change_v2_payload_round_trip_and_rejects() {
  use crate::{ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2};
  use buffa::Message as _;

  let cc = ConfChangeV2::new(
    ConfChangeTransition::Explicit,
    std::vec![
      ConfChangeSingle::new(ConfChangeType::AddNode, 4u64),
      ConfChangeSingle::new(ConfChangeType::RemoveNode, 2u64),
      ConfChangeSingle::new(ConfChangeType::AddLearnerNode, 9u64),
    ],
    Bytes::from_static(b"ctx"),
  );
  let mut buf = std::vec::Vec::new();
  encode_conf_change_v2(&cc, &mut buf);
  let back = decode_conf_change_v2::<u64>(Bytes::from(buf)).expect("decodes");
  assert_eq!(back, cc);

  // Unknown transition rejects.
  let w = pb::ConfChangeV2 {
    transition: EnumValue::Unknown(77),
    ..Default::default()
  };
  let mut buf = std::vec::Vec::new();
  w.encode(&mut buf);
  assert!(decode_conf_change_v2::<u64>(Bytes::from(buf)).is_err());
}

/// Golden byte vectors: representative envelope encodings pinned byte-for-byte. These
/// double as cross-implementation conformance pins — any conformant protobuf encoder
/// emitting the same fields in field order produces these bytes.
#[test]
fn golden_byte_vectors() {
  fn enc(m: &Message<u64>) -> std::vec::Vec<u8> {
    let mut buf = std::vec::Vec::new();
    encode_message(m, &mut buf);
    buf
  }

  let vote_resp = Message::VoteResp(VoteResp::new(Term::new(3), 2, true, false));
  assert_eq!(
    enc(&vote_resp),
    std::vec![
      0x22, 0x0E, // Message.vote_resp (field 4, length-delimited, 14 bytes)
      0x08, 0x03, // term = 3
      0x12, 0x08, 0x02, 0, 0, 0, 0, 0, 0, 0, // from_id = the u64 id's 8-byte LE encoding
      0x18, 0x01, // pre_vote = true
    ],
    "VoteResp golden encoding"
  );

  let timeout_now = Message::TimeoutNow(TimeoutNow::new(Term::new(1), 1));
  assert_eq!(
    enc(&timeout_now),
    std::vec![
      0x4A, 0x0C, // Message.timeout_now (field 9, length-delimited, 12 bytes)
      0x08, 0x01, // term = 1
      0x12, 0x08, 0x01, 0, 0, 0, 0, 0, 0, 0, // leader_id
    ],
    "TimeoutNow golden encoding"
  );
}

/// The decoded message's Bytes fields alias the frame allocation (zero-copy): the
/// payload's pointer lies INSIDE the frame buffer.
#[test]
fn decode_aliases_the_frame_allocation() {
  let payload = Bytes::from_static(b"zero-copy-payload-zero-copy-payload");
  let m = Message::AppendEntries(AppendEntries::new(
    Term::new(1),
    1u64,
    Index::ZERO,
    Term::ZERO,
    std::vec![Entry::new(
      Term::new(1),
      Index::new(1),
      EntryKind::Normal,
      payload
    )],
    Index::ZERO,
  ));
  let mut buf = std::vec::Vec::new();
  encode_message(&m, &mut buf);
  let frame = Bytes::from(buf);
  let frame_range = frame.as_ptr() as usize..frame.as_ptr() as usize + frame.len();

  let back = decode_message::<u64>(frame.clone()).expect("decodes");
  let Message::AppendEntries(ae) = &back else {
    panic!("variant");
  };
  let data = ae.entries()[0].data();
  let p = data.as_ptr() as usize;
  assert!(
    frame_range.contains(&p),
    "the decoded entry payload must alias the frame allocation (zero-copy)"
  );
}

/// SnapshotMeta.read_only round-trips, and Safe (the genesis default) is absent on the wire
/// (byte-identical to a snapshot built before the field existed).
#[test]
fn snapshot_meta_read_only_round_trips() {
  let conf = ConfState::new(
    std::vec![1u64, 2, 3],
    std::vec![],
    std::vec![],
    std::vec![],
    false,
  );
  let meta = SnapshotMeta::new(Index::new(10), Term::new(4), conf.clone())
    .with_read_only(crate::ReadOnlyOption::LeaseGuard);
  let m = Message::InstallSnapshot(InstallSnapshot::new(
    Term::new(4),
    1,
    meta,
    Bytes::from_static(b"blob"),
  ));
  let mut buf = std::vec::Vec::new();
  encode_message(&m, &mut buf);
  let Message::InstallSnapshot(back) = decode_message::<u64>(Bytes::from(buf)).expect("decode")
  else {
    panic!("variant")
  };
  assert_eq!(
    back.snapshot().read_only(),
    crate::ReadOnlyOption::LeaseGuard
  );

  // Safe (default) is omitted on the wire: byte-identical to a snapshot that never set it.
  let plain = SnapshotMeta::new(Index::new(10), Term::new(4), conf);
  let mut a = std::vec::Vec::new();
  let mut b = std::vec::Vec::new();
  encode_message(
    &Message::InstallSnapshot(InstallSnapshot::new(
      Term::new(4),
      1,
      plain.clone(),
      Bytes::from_static(b"blob"),
    )),
    &mut a,
  );
  encode_message(
    &Message::InstallSnapshot(InstallSnapshot::new(
      Term::new(4),
      1,
      plain.with_read_only(crate::ReadOnlyOption::Safe),
      Bytes::from_static(b"blob"),
    )),
    &mut b,
  );
  assert_eq!(a, b, "a Safe read_only is byte-identical to unset");
}
