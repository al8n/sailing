use super::*;
use bytes::Bytes;
use sailing_proto::{EntryKind, LogDone, LogStore, SnapshotMeta, StableStore, conf::ConfState};

#[test]
fn mem_log_append_is_durable_after_poll() {
  let mut log = MemLog::new();
  assert_eq!(log.last_index(), Index::ZERO);
  let e = Entry::new(
    Term::new(1),
    Index::new(1),
    EntryKind::Normal,
    Bytes::from_static(b"a"),
  );
  log.submit_append(OpId::new(1), core::slice::from_ref(&e));
  // synchronous-mode store completes immediately on poll
  assert_eq!(log.poll(), Some(Ok(LogDone::Appended(OpId::new(1)))));
  assert_eq!(log.last_index(), Index::new(1));
  assert_eq!(log.term(Index::new(1)).unwrap(), Term::new(1));
}

#[test]
fn mem_stable_roundtrips_hard_state() {
  let mut s = MemStable::<u64>::new();
  let hs = s.hard_state().with_term(Term::new(4));
  s.submit_write(OpId::new(1), hs);
  let _ = s.poll();
  assert_eq!(s.hard_state().term(), Term::new(4));
}

#[test]
fn mem_stable_roundtrips_snapshot() {
  let mut s = MemStable::<u64>::new();
  assert!(s.snapshot().is_none());

  let meta = SnapshotMeta::new(
    Index::new(10),
    Term::new(3),
    ConfState::from_voters(std::vec![1u64, 2u64, 3u64]),
  );
  let data = Bytes::from_static(b"snapshot-data");
  s.submit_snapshot(OpId::new(42), meta.clone(), data.clone());

  // Completion is enqueued.
  use sailing_proto::{OpId, StableDone};
  assert_eq!(
    s.poll(),
    Some(Ok(StableDone::SnapshotWritten(OpId::new(42))))
  );

  // Snapshot is readable.
  let (rmeta, rdata) = s.snapshot().unwrap();
  assert_eq!(rmeta.last_index(), Index::new(10));
  assert_eq!(rmeta.last_term(), Term::new(3));
  assert_eq!(rdata, data);

  // Second submit_snapshot overwrites the previous one.
  let meta2 = SnapshotMeta::new(
    Index::new(20),
    Term::new(5),
    ConfState::from_voters(std::vec![1u64]),
  );
  s.submit_snapshot(OpId::new(43), meta2.clone(), Bytes::from_static(b"v2"));
  let _ = s.poll();
  let (rmeta2, _) = s.snapshot().unwrap();
  assert_eq!(rmeta2.last_index(), Index::new(20));
}

// --- Compaction tests ---

fn make_entry(term: u64, index: u64) -> Entry {
  Entry::new(
    Term::new(term),
    Index::new(index),
    EntryKind::Normal,
    Bytes::new(),
  )
}

#[test]
fn compact_advances_first_index() {
  let mut log = MemLog::new();
  // append entries 1..=5 all at term 1
  let entries: Vec<Entry> = (1..=5).map(|i| make_entry(1, i)).collect();
  log.submit_append(OpId::new(1), &entries);
  let _ = log.poll();

  assert_eq!(log.first_index(), Index::new(1));
  assert_eq!(log.last_index(), Index::new(5));

  // compact up to index 3 (retain 4, 5)
  log.compact(Index::new(3));

  assert_eq!(log.first_index(), Index::new(4), "first_index must advance");
  assert_eq!(log.last_index(), Index::new(5), "last_index unchanged");
}

#[test]
fn term_at_offset_returns_boundary_term() {
  let mut log = MemLog::new();
  let entries: Vec<Entry> = vec![make_entry(1, 1), make_entry(1, 2), make_entry(2, 3)];
  log.submit_append(OpId::new(1), &entries);
  let _ = log.poll();

  log.compact(Index::new(2)); // compact up through index 2 (term 1)
  assert_eq!(
    log.term(Index::new(2)).unwrap(),
    Term::new(1),
    "term(offset) must return boundary term"
  );
}

#[test]
fn entries_and_term_correct_after_compaction() {
  let mut log = MemLog::new();
  let entries: Vec<Entry> = (1..=5).map(|i| make_entry(i, i)).collect();
  log.submit_append(OpId::new(1), &entries);
  let _ = log.poll();

  log.compact(Index::new(3));

  // entries 4 and 5 still accessible
  let slice = log.entries(Index::new(4)..Index::new(6), u64::MAX).unwrap();
  assert_eq!(slice.len(), 2);
  assert_eq!(slice[0].index(), Index::new(4));
  assert_eq!(slice[0].term(), Term::new(4));
  assert_eq!(slice[1].index(), Index::new(5));
  assert_eq!(slice[1].term(), Term::new(5));

  // term lookups
  assert_eq!(log.term(Index::new(4)).unwrap(), Term::new(4));
  assert_eq!(log.term(Index::new(5)).unwrap(), Term::new(5));
  // below offset → Term::ZERO
  assert_eq!(log.term(Index::new(1)).unwrap(), Term::ZERO);
  assert_eq!(log.term(Index::new(2)).unwrap(), Term::ZERO);
}

#[test]
fn committed_entries_no_fault_bounds_and_ignores_read_fault() {
  let mut log = MemLog::new_async(7);
  let entries: Vec<Entry> = (1..=5).map(|i| make_entry(i, i)).collect();
  log.submit_append(OpId::new(1), &entries);
  log.flush();
  let _ = log.poll();
  // Arm an always-firing committed-range read fault: the trait `entries()` would now ERROR and draw the
  // read PRNG, but `committed_entries_no_fault` must be a pure observer — it neither errors nor perturbs.
  log.set_faults(
    StorageFaults {
      transient_read_per_mille: 1000,
      ..StorageFaults::none()
    },
    42,
  );
  assert!(
    log.entries(Index::new(1)..Index::new(2), u64::MAX).is_err(),
    "the faulting trait read errors (the path the oracle must avoid)"
  );

  // commit covers a prefix [1..=3]: exactly entries 1, 2, 3 (entries above commit excluded).
  let prefix = log.committed_entries_no_fault(Index::new(3));
  let idxs: Vec<u64> = prefix.iter().map(|e| e.index().get()).collect();
  assert_eq!(idxs, vec![1, 2, 3]);
  // commit at the last index returns everything; commit past the last index clamps to it.
  assert_eq!(log.committed_entries_no_fault(Index::new(5)).len(), 5);
  assert_eq!(log.committed_entries_no_fault(Index::new(99)).len(), 5);
  // commit ZERO (nothing committed) → empty.
  assert!(log.committed_entries_no_fault(Index::ZERO).is_empty());

  // After compaction the prefix starts at first_index; a commit at/below the compaction offset is empty.
  log.compact(Index::new(3));
  let after = log.committed_entries_no_fault(Index::new(5));
  let after_idxs: Vec<u64> = after.iter().map(|e| e.index().get()).collect();
  assert_eq!(
    after_idxs,
    vec![4, 5],
    "prefix begins at the post-compaction first_index"
  );
  assert!(
    log.committed_entries_no_fault(Index::new(3)).is_empty(),
    "a commit at the compaction boundary leaves no in-memory committed entries"
  );
}

#[test]
fn compact_noop_on_already_compacted_range() {
  let mut log = MemLog::new();
  let entries: Vec<Entry> = (1..=5).map(|i| make_entry(1, i)).collect();
  log.submit_append(OpId::new(1), &entries);
  let _ = log.poll();

  log.compact(Index::new(3));
  // compact again with same or lower index — no-op, no panic
  log.compact(Index::new(3));
  log.compact(Index::new(1));

  assert_eq!(log.first_index(), Index::new(4));
  assert_eq!(log.last_index(), Index::new(5));
}

#[test]
fn compact_empty_log_is_noop() {
  let mut log = MemLog::new();
  log.compact(Index::new(5)); // must not panic
  assert_eq!(log.first_index(), Index::new(1));
  assert_eq!(log.last_index(), Index::ZERO);
}

// ─── async-write mode (fsync-loss window) ──────────────────────────────

#[test]
fn async_log_submit_then_discard_loses_inflight_append() {
  // Async mode (visible-state + durable-snapshot): submit_append is VISIBLE to reads immediately
  // but its durability is deferred (no completion). A crash (discard_inflight) BEFORE flush rolls
  // the visible state back to the durable snapshot: last_index returns to its durable value and
  // no completion ever fires.
  let mut log = MemLog::new_async(7);
  assert!(log.mode().is_async());
  assert_eq!(log.last_index(), Index::ZERO);

  let e = make_entry(1, 1);
  log.submit_append(OpId::new(1), core::slice::from_ref(&e));
  // Visible to reads immediately (the proto relies on this), but NOT yet durable and NO
  // completion enqueued.
  assert_eq!(
    log.last_index(),
    Index::new(1),
    "submitted append must be VISIBLE to reads (deferred durability, not deferred visibility)"
  );
  assert!(log.has_inflight(), "the append is in the fsync window");
  assert_eq!(
    log.durable_len(),
    0,
    "durable snapshot still empty (append not yet fsync'd)"
  );
  assert_eq!(
    log.poll(),
    None,
    "in-flight append must not enqueue a completion before flush"
  );

  // Crash in the fsync window: roll back to the (empty) durable snapshot.
  log.discard_inflight();
  assert_eq!(
    log.last_index(),
    Index::ZERO,
    "discarded in-flight append must be rolled back"
  );
  assert!(!log.has_inflight(), "fsync window empty after crash");
  assert_eq!(log.poll(), None, "no completion after discard");
}

#[test]
fn async_log_submit_then_flush_is_durable() {
  // Async mode: submit_append is visible immediately; flush makes it DURABLE and releases the
  // deferred completion (preserving the ordered-completion contract).
  let mut log = MemLog::new_async(7);
  let e = make_entry(1, 1);
  log.submit_append(OpId::new(1), core::slice::from_ref(&e));
  // Before flush: visible to reads, but not yet durable and no completion.
  assert_eq!(log.last_index(), Index::new(1), "visible immediately");
  assert_eq!(log.durable_len(), 0, "not yet durable");
  assert_eq!(log.poll(), None);

  log.flush();
  // After flush: durable + completion; survives a subsequent crash.
  assert_eq!(log.last_index(), Index::new(1), "flushed append is durable");
  assert_eq!(log.durable_len(), 1, "durable snapshot now covers it");
  assert_eq!(log.term(Index::new(1)).unwrap(), Term::new(1));
  assert_eq!(log.poll(), Some(Ok(LogDone::Appended(OpId::new(1)))));
  assert_eq!(log.poll(), None);
}

#[test]
fn async_log_discard_preserves_already_flushed_durable_state() {
  // Flush makes the first append durable; a later staged append is then discarded. The
  // durable prefix SURVIVES the crash; only the un-flushed tail is lost.
  let mut log = MemLog::new_async(7);
  log.submit_append(OpId::new(1), core::slice::from_ref(&make_entry(1, 1)));
  log.flush();
  let _ = log.poll();
  assert_eq!(log.last_index(), Index::new(1));

  // Submit a second append (visible immediately), then crash before flushing it.
  log.submit_append(OpId::new(2), core::slice::from_ref(&make_entry(1, 2)));
  assert_eq!(
    log.last_index(),
    Index::new(2),
    "second append visible before the crash"
  );
  log.discard_inflight();

  assert_eq!(
    log.last_index(),
    Index::new(1),
    "durable prefix survives crash; un-flushed tail rolled back"
  );
  assert_eq!(log.poll(), None, "no completion for the discarded tail");
  // The durable entry is still readable.
  assert_eq!(log.term(Index::new(1)).unwrap(), Term::new(1));
}

#[test]
fn async_log_flush_preserves_completion_order() {
  // Multiple staged appends flush in submission order.
  let mut log = MemLog::new_async(1);
  log.submit_append(OpId::new(10), core::slice::from_ref(&make_entry(1, 1)));
  log.submit_append(OpId::new(11), core::slice::from_ref(&make_entry(1, 2)));
  log.submit_append(OpId::new(12), core::slice::from_ref(&make_entry(1, 3)));
  log.flush();
  assert_eq!(log.poll(), Some(Ok(LogDone::Appended(OpId::new(10)))));
  assert_eq!(log.poll(), Some(Ok(LogDone::Appended(OpId::new(11)))));
  assert_eq!(log.poll(), Some(Ok(LogDone::Appended(OpId::new(12)))));
  assert_eq!(log.poll(), None);
  assert_eq!(log.last_index(), Index::new(3));
}

#[test]
fn sync_log_discard_inflight_is_noop() {
  // Sync mode is byte-identical to the original: submit is durable immediately, discard is a
  // no-op, completion is present.
  let mut log = MemLog::new();
  assert_eq!(log.mode(), StoreMode::Sync);
  log.submit_append(OpId::new(1), core::slice::from_ref(&make_entry(1, 1)));
  assert_eq!(
    log.last_index(),
    Index::new(1),
    "sync submit is durable now"
  );
  log.discard_inflight(); // no-op
  assert_eq!(log.last_index(), Index::new(1), "sync discard is a no-op");
  assert_eq!(log.poll(), Some(Ok(LogDone::Appended(OpId::new(1)))));
}

#[test]
fn async_stable_submit_then_discard_loses_inflight_write() {
  let mut s = MemStable::<u64>::new_async(3);
  assert!(s.mode().is_async());
  let hs = s.hard_state().with_term(Term::new(9));
  s.submit_write(OpId::new(1), hs);
  // The trait contract: `hard_state()` is LAST-DURABLE. A submitted-but-unflushed write must NOT
  // read back yet (a real disk store would return the previously-fsynced record here).
  assert_eq!(
    s.hard_state().term(),
    Term::ZERO,
    "an in-flight hard-state write is not durable, so hard_state() must not return it"
  );
  assert!(s.has_inflight());
  assert_eq!(s.poll(), None);

  // Crash before flush: roll back to the durable (initial) snapshot.
  s.discard_inflight();
  assert_eq!(
    s.hard_state().term(),
    Term::ZERO,
    "discarded in-flight write is rolled back to the durable snapshot"
  );
  assert!(!s.has_inflight());
  assert_eq!(s.poll(), None);
}

#[test]
fn async_stable_submit_then_flush_is_durable() {
  use sailing_proto::StableDone;
  let mut s = MemStable::<u64>::new_async(3);
  let hs = s.hard_state().with_term(Term::new(9));
  s.submit_write(OpId::new(1), hs);
  assert_eq!(
    s.hard_state().term(),
    Term::ZERO,
    "not durable until flushed (hard_state() reads the durable record)"
  );

  s.flush();
  assert_eq!(
    s.hard_state().term(),
    Term::new(9),
    "flushed write is durable"
  );
  assert_eq!(s.poll(), Some(Ok(StableDone::Wrote(OpId::new(1)))));
  assert_eq!(s.poll(), None);
  // Durable: a subsequent crash preserves it.
  s.discard_inflight();
  assert_eq!(
    s.hard_state().term(),
    Term::new(9),
    "flushed write survives a later crash"
  );
}

#[test]
fn async_stable_snapshot_is_visible_then_flushes() {
  use sailing_proto::StableDone;
  let mut s = MemStable::<u64>::new_async(5);
  let meta = SnapshotMeta::new(
    Index::new(10),
    Term::new(3),
    ConfState::from_voters(std::vec![1u64]),
  );
  s.submit_snapshot(OpId::new(7), meta, Bytes::from_static(b"snap"));
  // Visible immediately, but no completion before flush.
  assert!(
    s.snapshot().is_some(),
    "submitted snapshot is visible immediately"
  );
  assert_eq!(s.poll(), None);

  s.flush();
  assert!(s.snapshot().is_some(), "flushed snapshot is durable");
  assert_eq!(
    s.poll(),
    Some(Ok(StableDone::SnapshotWritten(OpId::new(7))))
  );
}

// ─── seeded storage faults (faults-as-data, never panics) ──────────────

#[test]
fn transient_read_fault_surfaces_as_error_not_panic() {
  // With transient_read at 100% the committed-range `entries` read returns the store error (a
  // VALUE), which the proto treats as fatal (poison). Never a panic. `term` is
  // deliberately NOT faulted (its proto callers swallow errors), so it keeps succeeding.
  let mut log = MemLog::new_async(7);
  log.submit_append(OpId::new(1), core::slice::from_ref(&make_entry(1, 1)));
  log.flush();
  let _ = log.poll();
  log.set_faults(
    StorageFaults {
      transient_read_per_mille: 1000,
      ..StorageFaults::none()
    },
    42,
  );
  assert_eq!(
    log.entries(Index::new(1)..Index::new(2), u64::MAX),
    Err(MemStoreError::TransientRead)
  );
  assert!(
    log.term(Index::new(1)).is_ok(),
    "term is intentionally never faulted by transient_read"
  );
}

#[test]
fn faults_off_by_default_reads_succeed() {
  // Default store (and async store with no faults) never returns an error from reads.
  let mut log = MemLog::new_async(99);
  log.submit_append(OpId::new(1), core::slice::from_ref(&make_entry(1, 1)));
  log.flush();
  let _ = log.poll();
  assert!(log.faults.is_none());
  for _ in 0..1000 {
    assert!(log.term(Index::new(1)).is_ok());
    assert!(log.entries(Index::new(1)..Index::new(2), u64::MAX).is_ok());
  }
}

#[test]
fn transient_read_fault_is_deterministic_given_seed() {
  // Same seed + same fault config → identical fault schedule (reproducible).
  let outcomes = |seed: u64| -> Vec<bool> {
    let mut log = MemLog::new_async(0);
    log.submit_append(OpId::new(1), core::slice::from_ref(&make_entry(1, 1)));
    log.flush();
    let _ = log.poll();
    log.set_faults(
      StorageFaults {
        transient_read_per_mille: 500,
        ..StorageFaults::none()
      },
      seed,
    );
    (0..64)
      .map(|_| log.entries(Index::new(1)..Index::new(2), u64::MAX).is_err())
      .collect()
  };
  assert_eq!(outcomes(123), outcomes(123), "same seed → same schedule");
  // Sanity: a 50% fault rate produces a mix (not all-true / all-false) — proves it fired.
  let s = outcomes(123);
  assert!(s.iter().any(|&x| x) && s.iter().any(|&x| !x));
}

#[test]
fn torn_write_fault_keeps_visible_undurable_then_retries() {
  // A torn write fails the fsync on flush: nothing becomes durable and no completion fires, but
  // the VISIBLE append survives (page cache intact) and stays in-flight. A LATER successful flush
  // retries the fsync and makes it durable; never a panic, never a visible rollback under the
  // running proc.
  let mut log = MemLog::new_async(0);
  log.set_faults(
    StorageFaults {
      torn_write_per_mille: 1000,
      ..StorageFaults::none()
    },
    11,
  );
  log.submit_append(OpId::new(1), core::slice::from_ref(&make_entry(1, 1)));
  assert_eq!(log.last_index(), Index::new(1), "append visible on submit");
  log.flush(); // torn: fsync fails
  assert_eq!(
    log.last_index(),
    Index::new(1),
    "torn write does NOT roll back the visible (page-cache) tail"
  );
  assert_eq!(log.durable_len(), 0, "but nothing landed durably");
  assert!(
    log.has_inflight(),
    "the write stays in flight (will be retried)"
  );
  assert_eq!(log.poll(), None, "torn write enqueues no completion");

  // Clear the fault and flush again: the retried fsync now lands.
  log.set_faults(StorageFaults::none(), 0);
  log.flush();
  assert_eq!(log.durable_len(), 1, "retried fsync made it durable");
  assert_eq!(log.poll(), Some(Ok(LogDone::Appended(OpId::new(1)))));

  // A crash BEFORE a successful retry would instead lose the torn tail: re-run that path.
  let mut log2 = MemLog::new_async(0);
  log2.set_faults(
    StorageFaults {
      torn_write_per_mille: 1000,
      ..StorageFaults::none()
    },
    11,
  );
  log2.submit_append(OpId::new(2), core::slice::from_ref(&make_entry(1, 1)));
  log2.flush(); // torn
  log2.discard_inflight(); // crash before a successful retry
  assert_eq!(
    log2.last_index(),
    Index::ZERO,
    "a crash before a successful retry loses the torn tail"
  );
  assert_eq!(log2.poll(), None);
}

#[test]
fn async_compact_keeps_durable_snapshot_consistent() {
  // Compaction GCs already-durable entries from BOTH the visible state and the durable snapshot.
  let mut log = MemLog::new_async(0);
  let entries: Vec<Entry> = (1..=5).map(|i| make_entry(1, i)).collect();
  log.submit_append(OpId::new(1), &entries);
  log.flush(); // entries 1..=5 now durable
  let _ = log.poll();
  assert_eq!(log.durable_len(), 5);

  log.compact(Index::new(3)); // retain 4,5 in both visible and durable
  assert_eq!(
    log.first_index(),
    Index::new(4),
    "visible first_index advanced"
  );
  assert_eq!(log.last_index(), Index::new(5));
  assert_eq!(log.durable_len(), 2, "durable snapshot GC'd in lockstep");
  assert_eq!(log.durable_entries().len(), 2);
  assert_eq!(log.durable_entries()[0].index(), Index::new(4));

  // A crash now rolls back to the (compacted) durable snapshot — still consistent.
  log.discard_inflight();
  assert_eq!(log.first_index(), Index::new(4));
  assert_eq!(log.last_index(), Index::new(5));
}

#[test]
fn async_restore_rebaselines_visible_and_durable() {
  // restore() is an immediate durable re-baseline of BOTH visible and durable snapshot.
  let mut log = MemLog::new_async(0);
  log.submit_append(OpId::new(1), core::slice::from_ref(&make_entry(1, 1)));
  log.flush();
  let _ = log.poll();

  log.restore(Index::new(10), Term::new(3));
  assert_eq!(log.first_index(), Index::new(11));
  assert_eq!(log.last_index(), Index::new(10));
  assert_eq!(log.term(Index::new(10)).unwrap(), Term::new(3));
  assert_eq!(
    log.durable_len(),
    0,
    "durable re-baselined to the snapshot point"
  );
  assert!(!log.has_inflight());

  // A crash after restore stays at the re-baselined point (durable == visible).
  log.discard_inflight();
  assert_eq!(log.last_index(), Index::new(10));
  assert_eq!(log.term(Index::new(10)).unwrap(), Term::new(3));
}
