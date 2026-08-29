use super::*;
use crate::fault::{CrashClass, SharedVfs, VfsDevice, codec::FieldWriter};

fn open(vfs: &SharedVfs, framing: JournalFraming) -> JournalEngine<VfsDevice> {
  JournalEngine::open_with(
    vfs.open("wal"),
    JournalDefects {
      framing,
      ..JournalDefects::default()
    },
  )
}

#[test]
fn a_barrier_covered_write_survives_an_unsynced_loss() {
  let vfs = SharedVfs::new();
  let mut engine = open(&vfs, JournalFraming::PerBarrier);
  engine.add_group(1);
  engine.set_group_floor(&1, 7);
  engine.flush();
  drop(engine);
  vfs.crash(CrashClass::LoseUnsyncedWrites);
  let engine = open(&vfs, JournalFraming::PerBarrier);
  assert!(engine.contains_group(&1));
  assert_eq!(FloorStore::floor(&engine, &1), 7);
}

#[test]
fn work_past_the_last_barrier_is_lost() {
  let vfs = SharedVfs::new();
  let mut engine = open(&vfs, JournalFraming::PerBarrier);
  engine.add_group(1);
  engine.flush();
  engine.set_group_floor(&1, 9);
  drop(engine);
  vfs.crash(CrashClass::LoseUnsyncedWrites);
  let engine = open(&vfs, JournalFraming::PerBarrier);
  assert!(engine.contains_group(&1), "the flushed admission survives");
  assert_eq!(
    FloorStore::floor(&engine, &1),
    0,
    "a floor written past the last barrier is crash-losable"
  );
}

#[test]
fn a_torn_tail_drops_the_whole_last_barrier() {
  let vfs = SharedVfs::new();
  let mut engine = open(&vfs, JournalFraming::PerBarrier);
  engine.add_group(1);
  engine.set_group_floor(&1, 3);
  engine.flush();
  let after_first = vfs.tail_durable_len();
  engine.set_group_floor(&1, 11);
  engine.set_group_gen(&1, 11);
  engine.flush();
  let after_second = vfs.tail_durable_len();
  drop(engine);
  // Cut in the MIDDLE of the second record.
  vfs.crash(CrashClass::TornTail {
    keep_bytes: after_first + (after_second - after_first) / 2,
  });
  let engine = open(&vfs, JournalFraming::PerBarrier);
  assert_eq!(
    FloorStore::floor(&engine, &1),
    3,
    "a half-written barrier is not a barrier"
  );
  assert_eq!(FloorStore::lineage(&engine, &1), 0);
}

#[test]
fn log_entries_and_the_hard_state_replay_verbatim() {
  use bytes::Bytes;
  use sailing_proto::{ConfState, EntryKind, LeaseSupport, OpId};

  let vfs = SharedVfs::new();
  let mut engine = open(&vfs, JournalFraming::PerBarrier);
  engine.add_group(4);
  {
    let (log, stable) = engine.stores(&4).unwrap();
    log.submit_append(
      OpId::new(1),
      &[
        Entry::new(
          Term::new(2),
          Index::new(1),
          EntryKind::Normal,
          Bytes::from_static(b"a"),
        )
        .with_lease_window(99),
        Entry::new(Term::new(2), Index::new(2), EntryKind::Empty, Bytes::new()),
      ],
    );
    stable.submit_write(
      OpId::new(2),
      HardState::initial()
        .with_term(Term::new(2))
        .with_vote(Some(7))
        .with_lease_support(LeaseSupport::Recorded(Some(
          core::time::Duration::from_millis(5),
        ))),
    );
    stable.submit_snapshot(
      OpId::new(3),
      SnapshotMeta::new(Index::new(1), Term::new(2), ConfState::from_voters([7u64]))
        .with_shape_gen(5),
      Bytes::from_static(b"blob"),
    );
  }
  engine.flush();
  drop(engine);
  vfs.crash(CrashClass::LoseUnsyncedWrites);

  let mut engine = open(&vfs, JournalFraming::PerBarrier);
  {
    let (log, stable) = engine.stores(&4).unwrap();
    assert_eq!(log.last_index(), Index::new(2));
    assert_eq!(log.term(Index::new(1)), Ok(Term::new(2)));
    assert_eq!(
      log
        .entries(Index::new(1)..Index::new(2), u64::MAX)
        .map(|read| match read {
          sailing_proto::EntriesRead::Ready(view) => view[0].lease_window(),
          sailing_proto::EntriesRead::Pending => 0,
        }),
      Ok(99),
      "an entry's self-describing lease window survives the journal"
    );
    let hs = stable.hard_state();
    assert_eq!(hs.term(), Term::new(2));
    assert_eq!(hs.vote(), Some(7));
    assert_eq!(
      hs.lease_support(),
      LeaseSupport::Recorded(Some(core::time::Duration::from_millis(5)))
    );
    let durable = stable.durable_snapshot().expect("the blob was barriered");
    assert_eq!(durable.shape_gen(), 5, "shape_gen survives the journal");
  }
  assert_eq!(engine.removal_floor(&4), 6);
}

#[test]
fn a_lineage_record_outlives_the_group_and_its_crash() {
  let vfs = SharedVfs::new();
  let mut engine = open(&vfs, JournalFraming::PerBarrier);
  engine.add_group(2);
  engine.set_group_gen(&2, 4);
  engine.flush();
  engine.remove_group(&2);
  engine.set_group_floor(&2, 5);
  engine.flush();
  drop(engine);
  vfs.crash(CrashClass::LoseUnsyncedWrites);
  let engine = open(&vfs, JournalFraming::PerBarrier);
  assert!(!engine.contains_group(&2), "the group itself is gone");
  assert_eq!(FloorStore::floor(&engine, &2), 5, "the fence outlives it");
  assert_eq!(FloorStore::lineage(&engine, &2), 4);
}

#[test]
fn boot_epochs_never_repeat_across_a_reopen() {
  let vfs = SharedVfs::new();
  let mut engine = open(&vfs, JournalFraming::PerBarrier);
  engine.add_group(3);
  engine.flush();
  assert_eq!(engine.next_boot_epoch(&3), Some(1));
  assert_eq!(engine.next_boot_epoch(&3), Some(2));
  drop(engine);
  // The epoch is handed out and USED at once, so it carries its own synced record: even a crash
  // with no barrier since must not hand the same epoch out twice.
  vfs.crash(CrashClass::LoseUnsyncedWrites);
  let mut engine = open(&vfs, JournalFraming::PerBarrier);
  assert_eq!(engine.next_boot_epoch(&3), Some(3));
}

#[test]
fn per_operation_framing_lets_half_a_barrier_survive() {
  // The deliberately-wrong framing, shown doing exactly what it is kept to demonstrate.
  let vfs = SharedVfs::new();
  let mut engine = open(&vfs, JournalFraming::PerOperation);
  engine.add_group(1);
  engine.flush();
  let before = vfs.tail_durable_len();
  engine.set_group_floor(&1, 6);
  engine.set_group_gen(&1, 6);
  engine.flush();
  let after = vfs.tail_durable_len();
  drop(engine);
  vfs.crash(CrashClass::TornTail {
    keep_bytes: before + (after - before) / 2,
  });
  let engine = open(&vfs, JournalFraming::PerOperation);
  assert_ne!(
    (
      FloorStore::floor(&engine, &1),
      FloorStore::lineage(&engine, &1)
    ),
    (6, 6),
    "the whole barrier cannot have survived a cut inside it"
  );
  assert!(
    FloorStore::floor(&engine, &1) == 6 || FloorStore::lineage(&engine, &1) == 6,
    "yet one half of it did — which is the failure per-operation framing produces"
  );
}

#[test]
fn a_boot_epoch_counter_is_not_reset_by_a_removal() {
  let vfs = SharedVfs::new();
  let mut engine = open(&vfs, JournalFraming::PerBarrier);
  engine.add_group(5);
  engine.flush();
  assert_eq!(engine.next_boot_epoch(&5), Some(1));
  engine.remove_group(&5);
  engine.flush();
  engine.add_group(5);
  engine.flush();
  assert_eq!(
    engine.next_boot_epoch(&5),
    Some(2),
    "a re-created id must not be handed an epoch a prior incarnation already used"
  );
}

#[test]
fn a_failing_device_latches_a_terminal_fail_stop() {
  use sailing_proto::{LogStore, OpId};
  let vfs = SharedVfs::new();
  // The medium refuses from the very first write on.
  let mut engine = JournalEngine::open(vfs.open_failing("wal", 1));
  engine.add_group(1);
  {
    let (log, _) = engine.stores(&1).unwrap();
    log.submit_append(
      OpId::new(1),
      &[Entry::new(
        Term::new(1),
        Index::new(1),
        sailing_proto::EntryKind::Normal,
        bytes::Bytes::from_static(b"a"),
      )],
    );
  }
  assert_eq!(
    engine.flush(),
    0,
    "a barrier the medium refused releases nothing"
  );
  assert!(engine.failed(), "the fail-stop latches");
  {
    let (log, stable) = engine.stores(&1).unwrap();
    assert!(
      matches!(log.poll(), Some(Err(JournalStorageError::DeviceFailed))),
      "the stores poison on the next poll"
    );
    assert!(matches!(
      stable.poll(),
      Some(Err(JournalStorageError::DeviceFailed))
    ));
    assert!(log.has_pending(), "the poison is drainable");
  }
  // Terminal: a later barrier does not recover, and no durable slot ever advanced.
  assert_eq!(engine.flush(), 0);
  assert!(engine.failed());
}

#[test]
fn a_reopen_manufactures_no_completions() {
  use sailing_proto::{LogStore, OpId};
  let vfs = SharedVfs::new();
  let mut engine = open(&vfs, JournalFraming::PerBarrier);
  engine.add_group(1);
  {
    let (log, stable) = engine.stores(&1).unwrap();
    log.submit_append(
      OpId::new(1),
      &[Entry::new(
        Term::new(1),
        Index::new(1),
        sailing_proto::EntryKind::Normal,
        bytes::Bytes::from_static(b"a"),
      )],
    );
    stable.submit_write(OpId::new(2), HardState::initial().with_term(Term::new(1)));
  }
  engine.flush();
  drop(engine);
  vfs.crash(CrashClass::LoseUnsyncedWrites);

  // Repeated cycles: each reopen replays the same barriers, and none of them may leave an
  // acknowledgment behind for a process that no longer exists.
  for cycle in 0..3 {
    let mut engine = open(&vfs, JournalFraming::PerBarrier);
    {
      let (log, stable) = engine.stores(&1).unwrap();
      assert_eq!(
        log.last_index(),
        Index::new(1),
        "cycle {cycle}: state is back"
      );
      assert!(
        log.poll().is_none(),
        "cycle {cycle}: no manufactured log completion"
      );
      assert!(!log.has_pending(), "cycle {cycle}: nothing to poll");
      assert!(
        stable.poll().is_none(),
        "cycle {cycle}: no manufactured stable completion"
      );
      assert!(!stable.has_pending());
    }
    drop(engine);
    vfs.crash(CrashClass::Clean);
  }
}

/// A CRC-valid record whose SECOND operation does not decode: two ops, the first perfectly good.
fn record_with_a_malformed_second_op(seq: u64) -> Vec<u8> {
  let mut payload = seq.to_le_bytes().to_vec();
  let mut writer = FieldWriter::default();
  encode_op(&Op::AddGroup(1), &mut writer);
  // An append op whose body is shorter than the group id it must start with.
  writer.field(OP_APPEND, &[0u8, 1, 2]);
  payload.extend_from_slice(&writer.finish());
  let mut record = (payload.len() as u64).to_le_bytes().to_vec();
  record.extend_from_slice(&payload);
  record.extend_from_slice(&crc32(&payload).to_le_bytes());
  record
}

/// A checksum proves the bytes are the ones written; it proves nothing about whether they DECODE.
/// A record whose later operation is refused must be voided WHOLE — keeping the earlier ones is
/// half a barrier arriving through the decode path, where no checksum can see it.
#[test]
fn a_record_with_a_malformed_operation_is_voided_whole() {
  let vfs = SharedVfs::new();
  {
    let mut dev = vfs.open("wal");
    dev.append(&record_with_a_malformed_second_op(0)).unwrap();
    dev.sync().unwrap();
  }
  let before = vfs.durable_len("wal");
  assert!(before > 0, "the record is on the medium");

  let engine = open(&vfs, JournalFraming::PerBarrier);
  assert!(
    !engine.contains_group(&1),
    "the record's first operation must not survive a refusal of its second"
  );
  drop(engine);
  assert_eq!(
    vfs.durable_len("wal"),
    0,
    "recovery cuts the medium BEFORE the refused record, so it is never replayed again"
  );
}

/// The same medium under the deliberate defect, so the difference is the check's teeth rather than
/// an assertion about nothing.
#[test]
fn partial_record_replay_keeps_half_a_barrier() {
  let vfs = SharedVfs::new();
  {
    let mut dev = vfs.open("wal");
    dev.append(&record_with_a_malformed_second_op(0)).unwrap();
    dev.sync().unwrap();
  }
  let engine = JournalEngine::open_with(
    vfs.open("wal"),
    JournalDefects {
      partial_records: true,
      ..JournalDefects::default()
    },
  );
  assert!(
    engine.contains_group(&1),
    "the defect keeps what decoded before the refusal — which is what makes the check above real"
  );
}

/// A later record is still reachable after a good one: the void applies to the refused record
/// alone, not to the prefix in front of it.
#[test]
fn a_valid_prefix_survives_a_later_refused_record() {
  let vfs = SharedVfs::new();
  {
    let mut engine = open(&vfs, JournalFraming::PerBarrier);
    engine.add_group(9);
    engine.set_group_floor(&9, 4);
    engine.flush();
  }
  let after_good = vfs.durable_len("wal");
  {
    let mut dev = vfs.open("wal");
    dev.append(&record_with_a_malformed_second_op(1)).unwrap();
    dev.sync().unwrap();
  }
  let engine = open(&vfs, JournalFraming::PerBarrier);
  assert!(engine.contains_group(&9), "the good record still replays");
  assert_eq!(FloorStore::floor(&engine, &9), 4);
  assert!(
    !engine.contains_group(&1),
    "and nothing from the refused record does"
  );
  drop(engine);
  assert_eq!(
    vfs.durable_len("wal"),
    after_good,
    "the medium is cut exactly where the valid prefix ends"
  );
}

/// Every operation a fail-stopped engine must refuse.
fn assert_refuses_everything(engine: &mut JournalEngine<VfsDevice>, what: &str) {
  assert!(engine.failed(), "{what}: the fail-stop must be latched");
  assert!(
    !engine.add_group(1),
    "{what}: admission it can never make durable must be refused"
  );
  assert!(!engine.contains_group(&1), "{what}: nothing is hosted");
  assert_eq!(engine.flush(), 0, "{what}: a barrier releases nothing");
  assert_eq!(
    engine.next_boot_epoch(&1),
    None,
    "{what}: no epoch is handed out"
  );
}

/// A device whose READ fails is not an empty device. Reading the fault as "no journal" is the worst
/// available reading: recovery finds no records, concludes the whole medium is a torn tail, and
/// truncates an intact journal to nothing.
#[test]
fn a_read_failure_at_open_fail_stops_the_engine() {
  let vfs = SharedVfs::new();
  {
    let mut engine = open(&vfs, JournalFraming::PerBarrier);
    engine.add_group(4);
    engine.set_group_floor(&4, 6);
    engine.flush();
  }
  let before = vfs.durable_len("wal");
  assert!(before > 0, "there is a real journal on the medium");

  let mut engine = JournalEngine::open(vfs.open_unreadable("wal"));
  assert_refuses_everything(&mut engine, "an unreadable journal");
  drop(engine);
  assert_eq!(
    vfs.durable_len("wal"),
    before,
    "and the journal it could not read is still there — a failed read must never truncate"
  );

  // The medium was never damaged: a healthy reopen still finds everything.
  let engine = open(&vfs, JournalFraming::PerBarrier);
  assert!(engine.contains_group(&4));
  assert_eq!(FloorStore::floor(&engine, &4), 6);
}

/// A torn tail that cannot be CUT is not recoverable by carrying on: every record the engine went
/// on to acknowledge would sit behind an unreadable tail, durable and permanently invisible.
#[test]
fn a_truncate_failure_at_open_fail_stops_the_engine() {
  let vfs = SharedVfs::new();
  {
    let mut engine = open(&vfs, JournalFraming::PerBarrier);
    engine.add_group(4);
    engine.set_group_floor(&4, 6);
    engine.flush();
  }
  // A torn suffix the next open must cut.
  {
    let mut dev = vfs.open("wal");
    dev.append(&[0xAA; 9]).unwrap();
    dev.sync().unwrap();
  }
  // This device reads fine and refuses its first write, which is the recovery truncation.
  let mut engine = JournalEngine::open(vfs.open_failing("wal", 1));
  assert_refuses_everything(&mut engine, "an uncuttable torn tail");
}

/// A record declaring a payload larger than four gibibytes must be READ AS DECLARED, and a
/// declaration no address space can hold must END the replay.
///
/// Under a 32-bit length field the same declaration wrapped: `u32::MAX + 1` narrowed to zero, so
/// the replay read a phantom zero-length record and walked on into bytes belonging to something
/// else. A barrier carrying a large snapshot blob framed itself that way, acknowledged the write,
/// and came back as an invalid tail — the acknowledged state simply gone.
#[test]
fn a_declared_length_past_four_gibibytes_does_not_wrap() {
  let vfs = SharedVfs::new();
  {
    let mut engine = open(&vfs, JournalFraming::PerBarrier);
    engine.add_group(1);
    engine.set_group_floor(&1, 7);
    engine.flush();
  }
  {
    // A header declaring more than a `u32` can hold, with nothing behind it.
    let mut dev = vfs.open("wal");
    let mut record = (u64::from(u32::MAX) + 1).to_le_bytes().to_vec();
    record.extend_from_slice(&1u64.to_le_bytes());
    dev.append(&record).unwrap();
    dev.sync().unwrap();
  }
  let engine = open(&vfs, JournalFraming::PerBarrier);
  assert!(
    engine.contains_group(&1),
    "the complete record in front of the oversized declaration still replays"
  );
  assert_eq!(FloorStore::floor(&engine, &1), 7);
  assert!(
    !engine.contains_group(&2),
    "the oversized declaration ends the replay; nothing behind it may be invented"
  );
}

/// The INNER declarations survive the boundary the outer record's already did.
///
/// The outer length was widened to 64 bits, but a journalled blob and every operation field kept a
/// 32-bit prefix underneath it. A snapshot blob reaches four gibibytes, and there the inner
/// declaration wrapped: the operation announced a length far below its own body INSIDE an outer
/// record whose length was correct, so the checksum matched, the barrier synced, `SnapshotWritten`
/// released — and the reopen followed the truncated inner boundary and cut away acknowledged
/// state.
///
/// Constructed rather than allocated: four gibibytes of test blob is not something to demand of a
/// machine, and the DECLARATION is what wrapped. Under the old prefix this exact declaration
/// narrowed to zero and decoded as an EMPTY blob, successfully.
#[test]
fn a_nested_blob_declaration_past_four_gibibytes_is_refused_not_truncated() {
  let past_the_old_boundary = u64::from(u32::MAX) + 1;
  let mut payload = past_the_old_boundary.to_le_bytes().to_vec();
  payload.extend_from_slice(b"and only these few bytes behind it");
  let mut at = 0usize;
  assert!(
    take_blob(&payload, &mut at).is_err(),
    "a blob declaring more bytes than back it must be refused; truncating the declaration into \
     one that fits is how a corrupt length becomes a plausible record"
  );

  // The writer's half of the same boundary: the declaration is eight bytes wide, so the length it
  // states is the length it means.
  let mut framed = Vec::new();
  put_blob(&mut framed, b"payload");
  assert_eq!(
    u64::from_le_bytes(framed[..8].try_into().expect("the length prefix")),
    7,
    "the blob's declared length is a full 64-bit value"
  );
  let mut at = 0usize;
  assert_eq!(
    take_blob(&framed, &mut at).expect("round trips"),
    b"payload"
  );
}

/// The same nesting END TO END, through the real encoder rather than a hand-built header: a
/// snapshot blob and a log entry go into one barrier, the engine is reopened from the medium, and
/// both come back byte-identical. Every layer the blob passes through — the operation field, the
/// sealed record, the outer journal frame — declares its own length, and a narrowing at any of
/// them cuts the blob at a boundary the checksum still agrees with.
#[test]
fn a_snapshot_blob_survives_the_encoder_nesting_and_the_reopen() {
  let vfs = SharedVfs::new();
  let blob = Bytes::from(std::vec![0xa5u8; 3 << 20]);
  let meta = SnapshotMeta::new(
    Index::new(4),
    Term::new(2),
    sailing_proto::ConfState::from_voters([1u64]),
  )
  .with_shape_gen(6);
  {
    let mut engine = open(&vfs, JournalFraming::PerBarrier);
    engine.add_group(1);
    let (log, stable) = engine.stores(&1).expect("just admitted");
    log.submit_append(
      sailing_proto::OpId::new(1),
      &[Entry::new(
        Term::new(2),
        Index::new(1),
        sailing_proto::EntryKind::Normal,
        Bytes::from_static(b"beside the blob"),
      )],
    );
    stable.submit_snapshot(sailing_proto::OpId::new(2), meta.clone(), blob.clone());
    engine.flush();
  }
  let mut engine = open(&vfs, JournalFraming::PerBarrier);
  let (log, stable) = engine.stores(&1).expect("the barrier admitted it");
  assert_eq!(
    stable.snapshot(),
    Some((meta, blob)),
    "the snapshot's meta and its bytes come back exactly as the encoder nested them"
  );
  assert_eq!(log.last_index(), Index::new(1));
}

/// A wrapped entry COUNT is refused, never read as an empty append.
///
/// The count was the last 32-bit length in the encoder. At 2^32 entries it wrapped to ZERO, and a
/// zero-count append decodes as a complete, empty operation while the entry bytes behind it are
/// simply not read — inside a record whose own length is right and whose checksum agrees. The
/// reopen then rebuilds a log missing everything that append acknowledged.
///
/// Constructed rather than allocated: the DECLARATION is what wrapped, and four billion entries is
/// not something to ask of a machine.
#[test]
fn an_append_count_past_four_gibibytes_is_refused_not_truncated() {
  let past_the_old_boundary = u64::from(u32::MAX) + 1;
  let mut payload = 7u64.to_le_bytes().to_vec();
  payload.extend_from_slice(&past_the_old_boundary.to_le_bytes());
  put_blob(
    &mut payload,
    &ReferenceCodec::encode_entry(&Entry::new(
      Term::new(1),
      Index::new(1),
      sailing_proto::EntryKind::Normal,
      Bytes::from_static(b"acknowledged"),
    )),
  );
  assert!(
    matches!(
      decode_op(OP_APPEND, &payload),
      Err(DecodeFault::ImplausibleLength)
    ),
    "a count naming more entries than the payload can hold must be refused; narrowing it into one \
     that fits reads an append with real entries behind it as an EMPTY one"
  );
}

/// The allocation gate is written against the header width that is ACTUALLY written.
///
/// Every entry costs at least its own length prefix, and those prefixes were widened to eight
/// bytes while the gate kept multiplying by four — so a count up to twice the truthful ceiling
/// walked through it and sized a `Vec` from a number the payload could never back. The refusal now
/// happens at the gate, before anything is allocated from the declared count at all.
#[test]
fn an_append_count_beyond_the_remaining_payload_is_refused_before_it_allocates() {
  let mut payload = 7u64.to_le_bytes().to_vec();
  // Eight entries claimed with forty bytes behind them: 8 * 4 fits, 8 * 8 cannot.
  payload.extend_from_slice(&8u64.to_le_bytes());
  payload.extend_from_slice(&[0u8; 40]);
  assert!(
    matches!(
      decode_op(OP_APPEND, &payload),
      Err(DecodeFault::ImplausibleLength)
    ),
    "the count must be refused by the length gate rather than sized into a Vec and discovered \
     later, one blob at a time"
  );
}

/// AN OPERATION CONSUMES ITS WHOLE FIELD PAYLOAD. Bytes left unread are the ones the writer wrote —
/// the checksum agrees with them — so nothing downstream can tell a complete operation from one
/// that stopped early, which is exactly the shape a wrapped count takes from the inside.
#[test]
fn an_operation_that_leaves_field_bytes_unread_is_refused() {
  let mut payload = 7u64.to_le_bytes().to_vec();
  payload.extend_from_slice(&9u64.to_le_bytes());
  payload.extend_from_slice(b"bytes no decode reads");
  assert!(
    matches!(
      decode_op(OP_SET_FLOOR, &payload),
      Err(DecodeFault::TrailingBytes)
    ),
    "an operation that decodes without reading its whole payload must be refused"
  );
}
