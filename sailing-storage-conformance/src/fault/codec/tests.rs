use super::*;

fn fork() -> ForkId {
  ForkId::new(
    Bytes::from_static(b"parent"),
    7,
    Index::new(42),
    Term::new(3),
    Bytes::from_static(b"child"),
    9,
  )
}

#[test]
fn crc32_matches_the_known_ieee_check_value() {
  // The IEEE 802.3 check value for "123456789" — the standard self-test vector.
  assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
  assert_eq!(crc32(b""), 0);
  assert_ne!(crc32(b"abc"), crc32(b"abd"));
}

#[test]
fn a_field_walk_tells_clean_eof_from_a_truncation() {
  let mut w = FieldWriter::default();
  w.u64_field(1, 5);
  w.u64_field(2, 6);
  let full = w.finish();

  // The complete body ends CLEANLY, and the walk says so.
  let mut reader = FieldReader::new(&full);
  assert!(matches!(reader.next_field(), Ok(FieldStep::Field(1, _))));
  assert!(matches!(reader.next_field(), Ok(FieldStep::Field(2, _))));
  assert!(matches!(reader.next_field(), Ok(FieldStep::Done)));

  // A body cut inside the second field's PAYLOAD is malformed, not finished.
  let mut reader = FieldReader::new(&full[..full.len() - 3]);
  assert!(matches!(reader.next_field(), Ok(FieldStep::Field(1, _))));
  assert_eq!(
    reader.next_field().unwrap_err(),
    DecodeFault::TruncatedFieldBody
  );

  // A body cut inside the second field's HEADER is a different fault, and named as one.
  let mut reader = FieldReader::new(&full[..full.len() - 10]);
  assert!(matches!(reader.next_field(), Ok(FieldStep::Field(1, _))));
  assert_eq!(
    reader.next_field().unwrap_err(),
    DecodeFault::TruncatedFieldHeader
  );
}

/// EVERY prefix of an encoded record must be refused with a typed fault — never a value assembled
/// from the fields that survived. The envelope is what makes this decidable: a tagged record with
/// no length of its own cannot tell a prefix ending on a field boundary from a shorter record a
/// writer meant to write.
#[test]
fn every_truncation_of_every_shape_is_refused_by_name() {
  let hs = HardState::<u64>::initial()
    .with_term(Term::new(9))
    .with_commit(Index::new(4))
    .with_vote(Some(77))
    .with_lease_support(LeaseSupport::Recorded(Some(Duration::new(3, 500))))
    .with_lineage(Some(fork()))
    .with_founding_gen(23);
  let entry = Entry::new(
    Term::new(2),
    Index::new(8),
    EntryKind::ConfChange,
    Bytes::from_static(b"payload"),
  )
  .with_timestamp(101)
  .with_lease_window(202)
  .with_wall_timestamp(303);
  let meta = SnapshotMeta::new(
    Index::new(12),
    Term::new(4),
    ConfState::new(vec![1u64, 2], vec![3u64], vec![4u64], vec![5u64], true),
  )
  .with_shape_gen(6)
  .with_fork_id(fork());

  let hs_bytes = ReferenceCodec::encode_hard_state(&hs);
  let entry_bytes = ReferenceCodec::encode_entry(&entry);
  let meta_bytes = ReferenceCodec::encode_snapshot_meta(&meta);

  for cut in 0..hs_bytes.len() {
    let err = ReferenceCodec::decode_hard_state(&hs_bytes[..cut]).unwrap_err();
    assert!(
      matches!(
        err,
        DecodeFault::TruncatedEnvelope | DecodeFault::TruncatedRecord
      ),
      "a hard state cut at {cut} of {} answered {err:?}",
      hs_bytes.len()
    );
  }
  for cut in 0..entry_bytes.len() {
    assert!(
      ReferenceCodec::decode_entry(&entry_bytes[..cut]).is_err(),
      "an entry cut at {cut} decoded to a value"
    );
  }
  for cut in 0..meta_bytes.len() {
    assert!(
      ReferenceCodec::decode_snapshot_meta(&meta_bytes[..cut]).is_err(),
      "a snapshot meta cut at {cut} decoded to a value"
    );
  }
  // The FULL records still decode, so the sweep is not passing by refusing everything.
  assert_eq!(ReferenceCodec::decode_hard_state(&hs_bytes).unwrap(), hs);
  assert_eq!(ReferenceCodec::decode_entry(&entry_bytes).unwrap(), entry);
  assert_eq!(
    ReferenceCodec::decode_snapshot_meta(&meta_bytes)
      .unwrap()
      .fork_id(),
    meta.fork_id()
  );
}

/// A single byte of input: a reader that cannot see its own envelope answers with a
/// default-shaped value.
#[test]
fn a_single_stray_byte_is_a_fault_not_a_default() {
  assert_eq!(
    ReferenceCodec::decode_hard_state(&[1]).unwrap_err(),
    DecodeFault::TruncatedEnvelope
  );
  assert_eq!(
    ReferenceCodec::decode_entry(&[1]).unwrap_err(),
    DecodeFault::TruncatedEnvelope
  );
  assert_eq!(
    ReferenceCodec::decode_snapshot_meta(&[1]).unwrap_err(),
    DecodeFault::TruncatedEnvelope
  );
}

/// A record whose bytes were altered fails its checksum rather than decoding to whatever the
/// altered bytes say.
#[test]
fn a_corrupted_record_fails_its_checksum() {
  let hs = HardState::<u64>::initial().with_term(Term::new(9));
  let mut bytes = ReferenceCodec::encode_hard_state(&hs);
  let at = bytes.len() / 2;
  bytes[at] ^= 0xFF;
  assert_eq!(
    ReferenceCodec::decode_hard_state(&bytes).unwrap_err(),
    DecodeFault::ChecksumMismatch
  );
}

/// A declared count or length is refused BEFORE it sizes an allocation.
#[test]
fn an_implausible_declared_length_is_refused_before_it_allocates() {
  // A conf-state field claiming four billion voters inside a nine-byte body.
  let mut w = FieldWriter::default();
  let mut conf = u32::MAX.to_le_bytes().to_vec();
  conf.extend_from_slice(&[0u8; 5]);
  w.u64_field(1, 12);
  w.u64_field(2, 4);
  w.field(3, &conf);
  let bytes = w.seal();
  assert_eq!(
    ReferenceCodec::decode_snapshot_meta(&bytes).unwrap_err(),
    DecodeFault::ImplausibleLength
  );
}

/// A discriminant no encoder mints is refused by name.
#[test]
fn an_unknown_discriminant_is_refused_by_name() {
  let mut w = FieldWriter::default();
  w.u64_field(1, 2);
  w.u64_field(2, 8);
  w.field(3, &[99u8]);
  w.field(4, b"data");
  let bytes = w.seal();
  assert_eq!(
    ReferenceCodec::decode_entry(&bytes).unwrap_err(),
    DecodeFault::UnknownDiscriminant
  );
}

/// A record missing a field the shape cannot be rebuilt without is refused rather than defaulted.
#[test]
fn a_missing_mandatory_field_is_refused_rather_than_defaulted() {
  let mut w = FieldWriter::default();
  w.u64_field(1, 7); // a term, and nothing else
  let bytes = w.seal();
  assert_eq!(
    ReferenceCodec::decode_hard_state(&bytes).unwrap_err(),
    DecodeFault::MissingField
  );
  assert_eq!(
    ReferenceCodec::decode_entry(&bytes).unwrap_err(),
    DecodeFault::MissingField
  );
}

#[test]
fn hard_state_round_trips_every_field() {
  let hs = HardState::<u64>::initial()
    .with_term(Term::new(9))
    .with_commit(Index::new(4))
    .with_vote(Some(77))
    .with_lease_support(LeaseSupport::Recorded(Some(Duration::new(3, 500))))
    .with_lineage(Some(fork()))
    .with_founding_gen(23);
  let back = ReferenceCodec::decode_hard_state(&ReferenceCodec::encode_hard_state(&hs)).unwrap();
  assert_eq!(back, hs);
  assert_eq!(back.lineage(), hs.lineage(), "lineage verbatim");
  assert_eq!(back.founding_gen(), 23, "founding generation verbatim");
}

/// An ABSENT founding generation reads as zero — the exact meaning the field's contract assigns to
/// a writer that predates it, since the storeless create door admits no other generation.
#[test]
fn an_absent_founding_generation_decodes_as_zero() {
  let hs = HardState::<u64>::initial().with_term(Term::new(3));
  assert_eq!(hs.founding_gen(), 0);
  let bytes = ReferenceCodec::encode_hard_state(&hs);
  let back = ReferenceCodec::decode_hard_state(&bytes).unwrap();
  assert_eq!(back.founding_gen(), 0);
  // A legacy record carries neither the promise nor the founding generation.
  let legacy = ReferenceCodec::encode_legacy_hard_state(
    &HardState::<u64>::initial()
      .with_term(Term::new(3))
      .with_founding_gen(9),
  );
  let back = ReferenceCodec::decode_hard_state(&legacy).unwrap();
  assert_eq!(back.founding_gen(), 0, "a legacy blob founded at zero");
  assert_eq!(back.lease_support(), LeaseSupport::Unrecorded);
}

#[test]
fn a_recorded_none_promise_survives_as_recorded_none() {
  let hs = HardState::<u64>::initial().with_lease_support(LeaseSupport::Recorded(None));
  let back = ReferenceCodec::decode_hard_state(&ReferenceCodec::encode_hard_state(&hs)).unwrap();
  assert_eq!(back.lease_support(), LeaseSupport::Recorded(None));
}

#[test]
fn legacy_bytes_decode_as_unrecorded_never_as_recorded_none() {
  let hs = HardState::<u64>::initial()
    .with_term(Term::new(2))
    .with_lease_support(LeaseSupport::Recorded(Some(Duration::from_secs(5))));
  let legacy = ReferenceCodec::encode_legacy_hard_state(&hs);
  let back = ReferenceCodec::decode_hard_state(&legacy).unwrap();
  assert_eq!(
    back.lease_support(),
    LeaseSupport::Unrecorded,
    "an absent field is the legacy verdict"
  );
  assert_eq!(back.term(), Term::new(2), "the other fields still decode");
}

#[test]
fn snapshot_meta_round_trips_shape_gen_and_fork_id() {
  let meta = SnapshotMeta::new(
    Index::new(12),
    Term::new(4),
    ConfState::new(vec![1u64, 2], vec![3u64], vec![4u64], vec![5u64], true),
  )
  .with_max_lease_window(11)
  .with_max_wall_plus_window(22)
  .with_max_unwalled_lease_window(33)
  .with_read_only(ReadOnlyOption::LeaseGuard)
  .with_shape_gen(6)
  .with_fork_id(fork());
  let back =
    ReferenceCodec::decode_snapshot_meta(&ReferenceCodec::encode_snapshot_meta(&meta)).unwrap();
  assert_eq!(back.shape_gen(), 6);
  assert_eq!(back.fork_id(), meta.fork_id());
  assert!(back.identity_eq(&meta));
  assert_eq!(back.conf(), meta.conf());
  assert_eq!(back.read_only(), Some(ReadOnlyOption::LeaseGuard));
  assert_eq!(back.max_lease_window(), 11);
  assert_eq!(back.max_wall_plus_window(), 22);
  assert_eq!(back.max_unwalled_lease_window(), 33);
}

#[test]
fn a_meta_without_a_fork_id_decodes_without_one() {
  let meta = SnapshotMeta::new(
    Index::new(1),
    Term::new(1),
    ConfState::from_voters(vec![1u64]),
  );
  let back =
    ReferenceCodec::decode_snapshot_meta(&ReferenceCodec::encode_snapshot_meta(&meta)).unwrap();
  assert!(back.fork_id().is_none());
  assert_eq!(back.shape_gen(), 0);
}

#[test]
fn every_entry_kind_round_trips_with_its_self_describing_fields() {
  for kind in [
    EntryKind::Normal,
    EntryKind::ConfChange,
    EntryKind::Empty,
    EntryKind::SetReadMode,
    EntryKind::Split,
    EntryKind::PrepareMerge,
    EntryKind::CommitMerge,
    EntryKind::RollbackMerge,
    EntryKind::ThawDischarged,
  ] {
    let entry = Entry::new(
      Term::new(2),
      Index::new(8),
      kind,
      Bytes::from_static(b"payload"),
    )
    .with_timestamp(101)
    .with_lease_window(202)
    .with_wall_timestamp(303);
    let back = ReferenceCodec::decode_entry(&ReferenceCodec::encode_entry(&entry)).unwrap();
    assert_eq!(back, entry, "{kind:?} must round-trip verbatim");
  }
}

#[test]
fn an_empty_input_is_a_truncated_envelope() {
  assert_eq!(
    ReferenceCodec::decode_snapshot_meta(b"").unwrap_err(),
    DecodeFault::TruncatedEnvelope
  );
}

/// Build a hard-state record whose lease-support promise carries an arbitrary `(secs, nanos)` pair
/// — including pairs no encoder would ever mint.
fn hard_state_with_raw_promise(secs: u64, nanos: u32) -> Vec<u8> {
  let mut promise = std::vec![1u8];
  promise.extend_from_slice(&secs.to_le_bytes());
  promise.extend_from_slice(&nanos.to_le_bytes());
  let mut w = FieldWriter::default();
  w.u64_field(1, 4); // term
  w.u64_field(2, 2); // commit
  w.field(4, &promise);
  w.seal()
}

/// THE CRASH LOOP THIS CLOSES. `Duration::new` PANICS when the nanosecond carry overflows the
/// second count, so a checksum-valid record carrying `u64::MAX` seconds beside a whole second of
/// nanoseconds ABORTED the decode instead of refusing it — and journal recovery, which decodes on
/// every open, would take the panic on every open rather than failing closed once.
///
/// The decode must RETURN, with a typed fault. If it unwound instead, this test would not reach
/// its assertion at all.
#[test]
fn a_carry_overflowing_promise_is_refused_rather_than_panicking() {
  let bytes = hard_state_with_raw_promise(u64::MAX, 1_000_000_000);
  assert_eq!(
    ReferenceCodec::decode_hard_state(&bytes).unwrap_err(),
    DecodeFault::NonCanonicalValue
  );
}

/// The same rule where no carry could overflow anything: a sub-second component at or past a whole
/// second is non-canonical whatever the seconds are, because a canonical writer emits
/// `subsec_nanos()`. Refusing the whole class is what makes the construction total.
#[test]
fn a_non_canonical_nanosecond_component_is_refused_at_any_second_count() {
  for (secs, nanos) in [(0u64, 1_000_000_000u32), (5, 2_500_000_000), (7, u32::MAX)] {
    let bytes = hard_state_with_raw_promise(secs, nanos);
    assert_eq!(
      ReferenceCodec::decode_hard_state(&bytes).unwrap_err(),
      DecodeFault::NonCanonicalValue,
      "({secs}, {nanos}) must be refused"
    );
  }
  // And the canonical boundary still decodes: one nanosecond below a whole second.
  let bytes = hard_state_with_raw_promise(u64::MAX, 999_999_999);
  assert_eq!(
    ReferenceCodec::decode_hard_state(&bytes)
      .expect("a canonical promise decodes")
      .lease_support(),
    LeaseSupport::Recorded(Some(Duration::new(u64::MAX, 999_999_999)))
  );
}

/// A FIELD's declared length is 64 bits wide too, and a declaration past the old 32-bit boundary is
/// REFUSED rather than truncated into one that fits.
///
/// The record envelope was widened first; the fields inside it kept a narrower prefix, so a payload
/// past four gibibytes announced a length far below its own body while the envelope's own length
/// and checksum stayed correct. The reader then walked to a field boundary the writer never wrote.
#[test]
fn a_field_declaring_more_than_four_gibibytes_is_refused() {
  let past_the_old_boundary = u64::from(u32::MAX) + 1;
  let mut buf = std::vec![7u8];
  buf.extend_from_slice(&past_the_old_boundary.to_le_bytes());
  buf.extend_from_slice(b"far fewer bytes than that");
  let mut reader = FieldReader::new(&buf);
  assert!(
    matches!(
      reader.next_field(),
      Err(DecodeFault::ImplausibleLength | DecodeFault::TruncatedFieldBody)
    ),
    "a field declaring more bytes than back it must be refused by name"
  );

  // The writer's half: the prefix a field states is the full length, not its low 32 bits.
  let mut writer = FieldWriter::default();
  writer.field(9, b"payload");
  let bytes = writer.finish();
  assert_eq!(
    u64::from_le_bytes(bytes[1..9].try_into().expect("the length prefix")),
    7,
    "a field's declared length is a full 64-bit value"
  );
}
