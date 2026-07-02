use super::*;
use crate::{
  AppendEntries, AppendResponse, Heartbeat, HeartbeatResponse, InstallSnapshot, ReadIndex,
  ReadIndexResponse, ReadOnlyOption, RequestVote, SnapshotResponse, TimeoutNow, VoteResponse,
  conf::ConfState,
};

fn rt(m: Message<u64>) {
  let mut buf = Vec::new();
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
  rt(Message::AppendResponse(AppendResponse::new(
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
  rt(Message::VoteResponse(VoteResponse::new(
    Term::new(3),
    2,
    true,
    false,
  )));
  rt(Message::Heartbeat(
    Heartbeat::new(Term::new(3), 1, Index::new(4), Bytes::from_static(b"ctx")).with_lease_round(9),
  ));
  rt(Message::HeartbeatResponse(
    HeartbeatResponse::new(Term::new(3), 2, Bytes::from_static(b"ctx"))
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
  rt(Message::SnapshotResponse(SnapshotResponse::new(
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
  rt(Message::ReadIndexResponse(ReadIndexResponse::new(
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
  let mut buf = Vec::new();
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
  let mut a = Vec::new();
  let mut b = Vec::new();
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
  let mut buf = Vec::new();
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
  let mut a = Vec::new();
  let mut b = Vec::new();
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
  let mut buf = Vec::new();
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
  let mut a = Vec::new();
  let mut b = Vec::new();
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
  let mut buf = Vec::new();
  encode_message(&m, &mut buf);
  let Message::InstallSnapshot(back) = decode_message::<u64>(Bytes::from(buf)).expect("decode")
  else {
    panic!("variant")
  };
  assert_eq!(back.snapshot().max_unwalled_lease_window(), 420_000_000);

  // A zero max_unwalled_lease_window is omitted on the wire (the codec's absent-when-zero rule; a real
  // LeaseGuard snapshot's value is non-zero under the entry-property fold).
  let plain = SnapshotMeta::new(Index::new(10), Term::new(4), conf);
  let mut a = Vec::new();
  let mut b = Vec::new();
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
  rt(Message::VoteResponse(VoteResponse::new(
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
  let mut buf = Vec::new();
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
    let mut buf = Vec::new();
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
  let vr = pb::VoteResponse {
    term: 3,
    ..Default::default()
  };
  assert!(
    decode_message::<u64>(enc(&wrap(pb::message::Body::from(vr)))).is_err(),
    "an absent/empty id field rejects"
  );

  // Oversized id field (> 1024 bytes).
  let vr = pb::VoteResponse {
    from_id: Bytes::from(std::vec![0u8; 1025]),
    ..Default::default()
  };
  assert!(
    decode_message::<u64>(enc(&wrap(pb::message::Body::from(vr)))).is_err(),
    "an oversized id rejects"
  );

  // An id with trailing bytes (a u64 id is exactly 8 bytes).
  let vr = pb::VoteResponse {
    from_id: Bytes::from(std::vec![1u8; 9]),
    ..Default::default()
  };
  assert!(
    decode_message::<u64>(enc(&wrap(pb::message::Body::from(vr)))).is_err(),
    "an id that does not consume exactly rejects"
  );

  // Out-of-range lease nanos.
  let hr = pb::HeartbeatResponse {
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
  let hr = pb::HeartbeatResponse {
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

  fn snapshot_with_voters(voters: Vec<u64>) -> Bytes {
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
    let mut buf = Vec::new();
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
  let mut buf = Vec::new();
  encode_conf_change_v2(&cc, &mut buf);
  let back = decode_conf_change_v2::<u64>(Bytes::from(buf)).expect("decodes");
  assert_eq!(back, cc);

  // Unknown transition rejects.
  let w = pb::ConfChangeV2 {
    transition: EnumValue::Unknown(77),
    ..Default::default()
  };
  let mut buf = Vec::new();
  w.encode(&mut buf);
  assert!(decode_conf_change_v2::<u64>(Bytes::from(buf)).is_err());
}

/// Golden byte vectors: representative envelope encodings pinned byte-for-byte. These
/// double as cross-implementation conformance pins — any conformant protobuf encoder
/// emitting the same fields in field order produces these bytes.
#[test]
fn golden_byte_vectors() {
  fn enc(m: &Message<u64>) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_message(m, &mut buf);
    buf
  }

  let vote_response = Message::VoteResponse(VoteResponse::new(Term::new(3), 2, true, false));
  assert_eq!(
    enc(&vote_response),
    std::vec![
      0x22, 0x0E, // Message.vote_response (field 4, length-delimited, 14 bytes)
      0x08, 0x03, // term = 3
      0x12, 0x08, 0x02, 0, 0, 0, 0, 0, 0, 0, // from_id = the u64 id's 8-byte LE encoding
      0x18, 0x01, // pre_vote = true
    ],
    "VoteResponse golden encoding"
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

  let snapshot_response = Message::SnapshotResponse(
    SnapshotResponse::new(Term::new(3), 2, false, Index::new(9)).with_acked_through(1),
  );
  assert_eq!(
    enc(&snapshot_response),
    std::vec![
      0x42, 0x10, // Message.snapshot_response (field 8, length-delimited, 16 bytes)
      0x08, 0x03, // term = 3
      0x12, 0x08, 0x02, 0, 0, 0, 0, 0, 0, 0, // from_id = the u64 id's 8-byte LE encoding
      0x20, 0x09, // match_index = 9 (field 4)
      0x28, 0x01, // acked_through = 1 (field 5)
    ],
    "SnapshotResponse golden encoding (reject=false is absent on the wire)"
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
  let mut buf = Vec::new();
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
    .with_read_only(ReadOnlyOption::LeaseGuard);
  let m = Message::InstallSnapshot(InstallSnapshot::new(
    Term::new(4),
    1,
    meta,
    Bytes::from_static(b"blob"),
  ));
  let mut buf = Vec::new();
  encode_message(&m, &mut buf);
  let Message::InstallSnapshot(back) = decode_message::<u64>(Bytes::from(buf)).expect("decode")
  else {
    panic!("variant")
  };
  assert_eq!(
    back.snapshot().read_only(),
    Some(ReadOnlyOption::LeaseGuard)
  );

  // A snapshot that NEVER set the mode (a pre-migration / legacy snapshot) is ABSENT on the wire and
  // decodes as None; an EXPLICIT Safe is PRESENT (discriminant + 1), so the two are NOT byte-identical —
  // a migrate-to-Safe stays distinguishable from legacy, and recovery falls back to config (not Safe).
  let plain = SnapshotMeta::new(Index::new(10), Term::new(4), conf);
  let mut a = Vec::new();
  let mut b = Vec::new();
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
      plain.with_read_only(ReadOnlyOption::Safe),
      Bytes::from_static(b"blob"),
    )),
    &mut b,
  );
  assert_ne!(
    a, b,
    "an explicit Safe must NOT be byte-identical to a legacy (absent) snapshot"
  );
  let Message::InstallSnapshot(back_legacy) =
    decode_message::<u64>(Bytes::from(a)).expect("decode")
  else {
    panic!("variant")
  };
  assert_eq!(
    back_legacy.snapshot().read_only(),
    None,
    "an unset read_only decodes as None (legacy/pre-migration), not Safe"
  );
  let Message::InstallSnapshot(back_safe) = decode_message::<u64>(Bytes::from(b)).expect("decode")
  else {
    panic!("variant")
  };
  assert_eq!(
    back_safe.snapshot().read_only(),
    Some(ReadOnlyOption::Safe),
    "an explicit Safe round-trips as Some(Safe)"
  );
}

#[test]
fn install_snapshot_chunk_fields_round_trip() {
  let conf = ConfState::from_voters(std::vec![1u64, 2u64]);
  let meta = SnapshotMeta::new(Index::new(10), Term::new(3), conf);
  let m = Message::InstallSnapshot(InstallSnapshot::new_chunk(
    Term::new(3),
    1,
    meta,
    Bytes::from_static(b"chunk"),
    64,
    4096,
  ));
  let mut buf = Vec::new();
  encode_message(&m, &mut buf);
  let Message::InstallSnapshot(back) = decode_message::<u64>(Bytes::from(buf)).expect("decode")
  else {
    panic!("variant")
  };
  assert_eq!(back.offset(), 64);
  assert_eq!(back.total_len(), 4096);
  assert!(!back.is_last());
}

#[test]
fn install_snapshot_legacy_is_byte_identical() {
  let conf = ConfState::from_voters(std::vec![1u64]);
  let meta = SnapshotMeta::new(Index::new(5), Term::new(1), conf);
  let mut a = Vec::new();
  let mut b = Vec::new();
  encode_message(
    &Message::InstallSnapshot(InstallSnapshot::new(
      Term::new(1),
      1,
      meta.clone(),
      Bytes::from_static(b"blob"),
    )),
    &mut a,
  );
  encode_message(
    &Message::InstallSnapshot(InstallSnapshot::new_chunk(
      Term::new(1),
      1,
      meta,
      Bytes::from_static(b"blob"),
      0,
      0,
    )),
    &mut b,
  );
  assert_eq!(
    a, b,
    "offset=0,total_len=0 must be byte-identical to the legacy new()"
  );
}

#[test]
fn snapshot_response_acked_through_round_trips() {
  let m = Message::SnapshotResponse(
    SnapshotResponse::new(Term::new(2), 7, false, Index::new(0)).with_acked_through(2048),
  );
  let mut buf = Vec::new();
  encode_message(&m, &mut buf);
  let Message::SnapshotResponse(back) = decode_message::<u64>(Bytes::from(buf)).expect("decode")
  else {
    panic!("variant")
  };
  assert_eq!(back.acked_through(), 2048);
  let mut c = Vec::new();
  let mut d = Vec::new();
  encode_message(
    &Message::SnapshotResponse(SnapshotResponse::new(Term::new(2), 7, false, Index::new(9))),
    &mut c,
  );
  encode_message(
    &Message::SnapshotResponse(
      SnapshotResponse::new(Term::new(2), 7, false, Index::new(9)).with_acked_through(0),
    ),
    &mut d,
  );
  assert_eq!(c, d, "a zero acked_through must be absent on the wire");
}

/// The closed-form `install_snapshot_encoded_len` must equal — to the byte — the length the real
/// encoder produces for the matching `InstallSnapshot::new_chunk` frame, across the full field
/// space (empty/joint/learner ConfStates, present/absent lease-and-read scalars, present/absent
/// term/offset/total_len, and several data lengths). This agreement is the guarantee the formula
/// stays in lockstep with the encoder.
#[test]
fn install_snapshot_encoded_len_agrees_with_encoder() {
  let metas: std::vec::Vec<(&str, SnapshotMeta<u64>)> = std::vec![
    (
      "3-voter, no extras",
      SnapshotMeta::new(
        Index::new(7),
        Term::new(3),
        ConfState::from_voters([1u64, 2, 3])
      ),
    ),
    (
      "empty conf",
      SnapshotMeta::new(Index::new(0), Term::new(0), ConfState::default()),
    ),
    (
      "joint + learners + outgoing + auto_leave",
      SnapshotMeta::new(
        Index::new(1_000_000),
        Term::new(42),
        ConfState::new(
          [1u64, 2, 300],
          [4u64, 5],
          [6u64, 7, 8],
          [9u64, 128, 16_384],
          true,
        ),
      ),
    ),
    (
      "all lease/read scalars set (Safe)",
      SnapshotMeta::new(
        Index::new(9),
        Term::new(4),
        ConfState::from_voters([10u64, 20])
      )
      .with_max_lease_window(1_234_567)
      .with_max_wall_plus_window(2_000_000_000)
      .with_max_unwalled_lease_window(50_000)
      .with_read_only(ReadOnlyOption::Safe),
    ),
    (
      "lease scalars set (LeaseGuard read mode)",
      SnapshotMeta::new(
        Index::new(u32::MAX as u64),
        Term::new(u16::MAX as u64),
        ConfState::from_voters([u64::MAX, u64::MAX - 1]),
      )
      .with_max_lease_window(u64::MAX)
      .with_max_wall_plus_window(u64::MAX)
      .with_max_unwalled_lease_window(u64::MAX)
      .with_read_only(ReadOnlyOption::LeaseGuard),
    ),
  ];

  for (term_raw, leader, offset, total) in [
    (0u64, 1u64, 0u64, 0u64),
    (5, 7, 0, 0),
    (5, 7, 4096, 60 << 20),
    (u64::MAX, u64::MAX, u64::MAX, u64::MAX),
  ] {
    let term = Term::new(term_raw);
    for (label, meta) in &metas {
      for data_len in [0u64, 1, 100, 65536] {
        let want = {
          let mut v = Vec::new();
          encode_message(
            &Message::InstallSnapshot(InstallSnapshot::new_chunk(
              term,
              leader,
              meta.clone(),
              Bytes::from(std::vec![0u8; data_len as usize]),
              offset,
              total,
            )),
            &mut v,
          );
          v.len()
        };
        let got = install_snapshot_encoded_len(term, &leader, meta, offset, total, data_len);
        assert_eq!(
          got, want,
          "meta={label}, term={term_raw}, leader={leader}, offset={offset}, total={total}, data_len={data_len}"
        );
      }
    }
  }
}

/// [`MessageEncoder`] must produce BYTE-IDENTICAL frames to the stateless [`encode_message`] for every
/// message — exercising the snapshot-meta cache across HITs (consecutive chunks of one transfer),
/// MISSes (a superseding meta, and a same-identity-but-different-bounds meta whose body differs even
/// though `identity_eq` matches), and non-snapshot messages interleaved between chunks (which must leave
/// the cache untouched). This is the guarantee the cached splice stays in lockstep with the encoder.
#[test]
fn encoder_is_byte_identical_to_encode_message() {
  fn assert_same(enc: &mut MessageEncoder<u64>, msg: &Message<u64>) {
    let mut want = Vec::new();
    encode_message(msg, &mut want);
    let mut got = Vec::new();
    enc.encode_message(msg, &mut got);
    assert_eq!(got, want, "encoder must match encode_message for {msg:?}");
  }

  let meta_a = SnapshotMeta::new(
    Index::new(5000),
    Term::new(6),
    ConfState::new([1u64, 2, 3, 4, 5], [9u64], [1u64, 2, 3], [7u64], true),
  );
  // SAME identity as `meta_a` (equal last_index/last_term/conf) but DIFFERENT bounds: `identity_eq`
  // matches yet the encoded body differs, so the cache (keyed on the FULL meta) must miss and re-encode.
  let meta_a_bounded = meta_a.clone().with_max_lease_window(123_456);
  // A genuinely different (superseding) meta.
  let meta_b = SnapshotMeta::new(
    Index::new(9000),
    Term::new(7),
    ConfState::from_voters([10u64, 20, 30]),
  )
  .with_read_only(ReadOnlyOption::LeaseGuard);

  // Single-message agreement across the field space (mirrors the encoded-len corpus); a fresh encoder
  // each time isolates the splice from the cache.
  for (term_raw, leader, offset, total) in [
    (0u64, 1u64, 0u64, 0u64),
    (5, 7, 0, 0),
    (5, 7, 4096, 60 << 20),
    (u64::MAX, u64::MAX, u64::MAX, u64::MAX),
  ] {
    let term = Term::new(term_raw);
    for meta in [&meta_a, &meta_a_bounded, &meta_b] {
      for data_len in [0u64, 1, 100, 65536] {
        let msg = Message::InstallSnapshot(InstallSnapshot::new_chunk(
          term,
          leader,
          meta.clone(),
          Bytes::from(std::vec![0xCDu8; data_len as usize]),
          offset,
          total,
        ));
        assert_same(&mut MessageEncoder::new(), &msg);
      }
    }
  }

  // A realistic transfer through ONE encoder: many chunks of `meta_a` (cache HITs) with a heartbeat
  // interleaved between chunks (the cache must stay untouched), then a supersede.
  let mut enc = MessageEncoder::new();
  let total = 10_000u64;
  let chunk = 2048u64;
  let mut offset = 0u64;
  while offset < total {
    let len = chunk.min(total - offset);
    assert_same(
      &mut enc,
      &Message::InstallSnapshot(InstallSnapshot::new_chunk(
        Term::new(6),
        1u64,
        meta_a.clone(),
        Bytes::from(std::vec![0xABu8; len as usize]),
        offset,
        total,
      )),
    );
    assert_same(
      &mut enc,
      &Message::Heartbeat(Heartbeat::new(
        Term::new(6),
        1u64,
        Index::new(offset),
        Bytes::new(),
      )),
    );
    offset += len;
  }
  // Supersede with a different meta (miss + re-cache), then the same-identity-different-bounds meta
  // (must NOT reuse the stale body), then back to `meta_a`.
  for meta in [&meta_b, &meta_a_bounded, &meta_a] {
    assert_same(
      &mut enc,
      &Message::InstallSnapshot(InstallSnapshot::new_chunk(
        Term::new(7),
        2u64,
        meta.clone(),
        Bytes::from_static(b"tail"),
        0,
        4,
      )),
    );
  }
}

/// The snapshot-meta cache is MEMORY-BOUNDED: a body over [`SNAPSHOT_META_CACHE_MAX_BYTES`] is not
/// cached (it re-encodes per chunk, still byte-identical), the cache RELEASES the moment a transfer
/// completes (the final chunk, or a legacy single-shot), and [`MessageEncoder::clear`] drops it on
/// teardown. The cache is a pure optimization, so output is byte-identical whether cached or not.
#[test]
fn encoder_cache_is_bounded_and_clears() {
  fn chunk(meta: &SnapshotMeta<u64>, offset: u64, len: usize, total: u64) -> Message<u64> {
    Message::InstallSnapshot(InstallSnapshot::new_chunk(
      Term::new(6),
      1u64,
      meta.clone(),
      Bytes::from(std::vec![0xAB; len]),
      offset,
      total,
    ))
  }
  fn assert_same(enc: &mut MessageEncoder<u64>, msg: &Message<u64>) {
    let mut want = Vec::new();
    encode_message(msg, &mut want);
    let mut got = Vec::new();
    enc.encode_message(msg, &mut got);
    assert_eq!(
      got, want,
      "cache state must never change the bytes: {msg:?}"
    );
  }

  let small = SnapshotMeta::new(
    Index::new(7),
    Term::new(6),
    ConfState::from_voters([1u64, 2, 3]),
  );
  // Enough u64 voters that the encoded body comfortably exceeds the threshold (>= 3 bytes per id-entry,
  // so 40_000 entries > 64 KiB regardless of the id's own encoding width).
  let big = SnapshotMeta::new(
    Index::new(7),
    Term::new(6),
    ConfState::from_voters(1..=40_000u64),
  );

  // SIZE BOUND: a small meta is cached (within the threshold); an over-threshold meta is NOT — both
  // byte-identical, and the big one supersedes (clears) the small entry rather than pinning either.
  let mut enc = MessageEncoder::new();
  assert_same(&mut enc, &chunk(&small, 0, 8, 1_000_000));
  assert!(
    enc
      .cached_body_len()
      .is_some_and(|n| n <= SNAPSHOT_META_CACHE_MAX_BYTES),
    "a small meta must be cached within the threshold"
  );
  assert_same(&mut enc, &chunk(&big, 0, 8, 1_000_000));
  assert_eq!(
    enc.cached_body_len(),
    None,
    "an over-threshold meta must not be cached"
  );
  // A repeat of the big meta still re-encodes byte-identically (no cache to hit).
  assert_same(&mut enc, &chunk(&big, 8, 8, 1_000_000));
  assert_eq!(enc.cached_body_len(), None);

  // COMPLETION CLEAR: the final chunk (is_last) of a small transfer releases the cache.
  let mut enc = MessageEncoder::new();
  assert_same(&mut enc, &chunk(&small, 0, 8, 16)); // non-final (8 < 16) → cached
  assert!(enc.cached_body_len().is_some());
  assert_same(&mut enc, &chunk(&small, 8, 8, 16)); // is_last (8 + 8 == 16) → released
  assert_eq!(
    enc.cached_body_len(),
    None,
    "a completed transfer must release the cache"
  );

  // A legacy single-shot (total_len == 0) retains nothing.
  let mut enc = MessageEncoder::new();
  assert_same(
    &mut enc,
    &Message::InstallSnapshot(InstallSnapshot::new(
      Term::new(6),
      1u64,
      small.clone(),
      Bytes::from_static(b"blob"),
    )),
  );
  assert_eq!(
    enc.cached_body_len(),
    None,
    "a single-shot must not retain a cache"
  );

  // TEARDOWN: clear() drops the cache.
  let mut enc = MessageEncoder::new();
  assert_same(&mut enc, &chunk(&small, 0, 8, 1_000_000));
  assert!(enc.cached_body_len().is_some());
  enc.clear();
  assert_eq!(enc.cached_body_len(), None);
}

/// A frame packed with unknown protobuf fields is rejected rather than materializing an unbounded run
/// of `UnknownField` entries: `decode_message` caps the allowance at [`MAX_UNKNOWN_FIELDS`].
///
/// MUTATION: revert `decode_message` to buffa's default allowance (1,000,000) → a frame with far more
/// unknown fields than the cap decodes without error (or allocates megabytes first), failing this.
#[test]
fn decode_rejects_a_frame_stuffed_with_unknown_fields() {
  fn push_varint(buf: &mut Vec<u8>, mut v: u64) {
    loop {
      let b = (v & 0x7f) as u8;
      v >>= 7;
      if v == 0 {
        buf.push(b);
        break;
      }
      buf.push(b | 0x80);
    }
  }

  // Each entry is a `uint64` field at an unused high field number (100), which `pb::Message` does not
  // recognize and so counts as one unknown field. More than the cap must be refused.
  let mut buf = Vec::new();
  for _ in 0..(MAX_UNKNOWN_FIELDS + 8) {
    push_varint(&mut buf, 100u64 << 3); // tag: field 100, wire type 0 (varint)
    push_varint(&mut buf, 0); // its value
  }
  let r = decode_message::<u64>(bytes::Bytes::from(buf));
  assert!(
    r.is_err(),
    "a frame with more than MAX_UNKNOWN_FIELDS unknown fields must be rejected"
  );
}
