use super::*;
use crate::{ConfState, EntryKind, FloorStore, ForkId, MERGED_FLOOR, NoFloors};

fn empty_entry(term: u64, index: u64) -> Entry {
  Entry::new(
    Term::new(term),
    Index::new(index),
    EntryKind::Empty,
    Bytes::new(),
  )
}

fn voter_meta(last_index: u64, last_term: u64) -> SnapshotMeta<u64> {
  SnapshotMeta::new(
    Index::new(last_index),
    Term::new(last_term),
    ConfState::from_voters(std::vec![1u64]),
  )
}

/// `has_staged` is the driver's exact barrier re-arm signal: it sees work `has_pending` hides
/// (staged pre-barrier), flips false once a flush releases everything, and — the case a
/// release-count predicate misses — flips true again when a write is staged AFTER a flush that
/// released nothing.
#[test]
fn has_staged_tracks_pre_barrier_work_exactly() {
  let mut eng = GroupEngine::<u64, u64>::new();
  assert!(eng.add_group(1));
  assert!(!eng.has_staged());

  {
    let (log, _) = eng.stores(&1).unwrap();
    log.submit_append(OpId::new(1), &[empty_entry(1, 1)]);
    assert!(log.has_staged() && !log.has_pending());
  }
  assert!(eng.has_staged());

  assert_eq!(eng.flush(), 1);
  assert!(!eng.has_staged(), "released work is no longer staged");

  // A flush that releases nothing, THEN a stage (the storage-tail submit while draining a
  // completion): the release count says quiescent, has_staged says re-arm.
  assert_eq!(eng.flush(), 0);
  {
    let (_, stable) = eng.stores(&1).unwrap();
    stable.submit_write(OpId::new(2), HardState::initial().with_term(Term::new(1)));
    assert!(stable.has_staged() && !stable.has_pending());
  }
  assert!(eng.has_staged());
}

/// The two staging-capacity bounds part ways: a declared `total_len` over the cap is a FATAL
/// error (the core poisons — a zero watermark would re-solicit the same declaration forever),
/// while the same store keeps accepting an allocatable transfer afterward.
#[test]
fn over_cap_staging_declaration_is_fatal() {
  let mut eng = GroupEngine::<u64, u64>::new();
  eng.set_snapshot_staging_cap(1024);
  assert!(eng.add_group(1));
  let (_, stable) = eng.stores(&1).unwrap();
  let meta = voter_meta(9, 2);

  let err = stable
    .accept_snapshot_chunk(&meta, 4096, 0, &Bytes::from_static(&[1, 2, 3]))
    .unwrap_err();
  assert_eq!(
    err,
    EngineStorageError::StagingUnallocatable { total_len: 4096 }
  );

  // An allocatable declaration on the same store still stages normally.
  let got = stable
    .accept_snapshot_chunk(&meta, 4, 0, &Bytes::from_static(&[9, 9, 9, 9]))
    .unwrap();
  assert_eq!(got, 4);
}

/// A large-but-allocatable declaration stages and makes progress — the cap defaults to
/// allocator-bound, so a legitimate big snapshot never spins on zero-watermark retries.
#[test]
fn large_allocatable_staging_never_spins() {
  let mut eng = GroupEngine::<u64, u64>::new();
  assert!(eng.add_group(1));
  let (_, stable) = eng.stores(&1).unwrap();
  let meta = voter_meta(9, 2);
  let total = 1u64 << 20;

  let chunk = Bytes::from(std::vec![7u8; 4096]);
  assert_eq!(
    stable
      .accept_snapshot_chunk(&meta, total, 0, &chunk)
      .unwrap(),
    4096,
    "the contiguous watermark advances"
  );
  assert_eq!(
    stable
      .accept_snapshot_chunk(&meta, total, 4096, &chunk)
      .unwrap(),
    8192,
    "and keeps advancing — no zero-watermark restart"
  );
}

/// A pre-barrier §5.3 conflict truncation invalidates the superseded staged completion: releasing
/// it would claim a durable prefix through an index the barrier no longer makes durable.
#[test]
fn conflict_truncation_invalidates_staged_completions() {
  let mut eng = GroupEngine::<u64, u64>::new();
  assert!(eng.add_group(7));
  let (log, _stable) = eng.stores(&7).unwrap();

  // Stage 1..=3, then a conflicting suffix from index 2 BEFORE any barrier: the first append's
  // extent (3) is truncated away, so its completion must never be released.
  log.submit_append(
    OpId::new(1),
    &[empty_entry(1, 1), empty_entry(1, 2), empty_entry(1, 3)],
  );
  log.submit_append(OpId::new(2), &[empty_entry(2, 2)]);
  assert_eq!(log.last_index(), Index::new(2));

  assert_eq!(eng.flush(), 1, "only the surviving completion is released");
  let (log, _stable) = eng.stores(&7).unwrap();
  assert!(matches!(
    log.poll(),
    Some(Ok(LogDone::Appended(id))) if id == OpId::new(2)
  ));
  assert!(log.poll().is_none(), "the superseded completion is gone");
}

/// A PARTIAL overlap drops the superseded completion too: the surviving prefix of the first
/// append is covered by the conflicting append's own completion (its extent is at least the
/// truncation point), so under-reporting never claims false durability.
#[test]
fn partial_overlap_keeps_only_the_superseding_completion() {
  let mut eng = GroupEngine::<u64, u64>::new();
  assert!(eng.add_group(7));
  let (log, _stable) = eng.stores(&7).unwrap();

  log.submit_append(
    OpId::new(1),
    &[
      empty_entry(1, 1),
      empty_entry(1, 2),
      empty_entry(1, 3),
      empty_entry(1, 4),
      empty_entry(1, 5),
    ],
  );
  // Conflicts from index 4: indices 1..=3 of the first batch survive and become durable at the
  // same barrier the superseding append's completion (extent 4) proves.
  log.submit_append(OpId::new(2), &[empty_entry(2, 4)]);
  assert_eq!(log.last_index(), Index::new(4));

  assert_eq!(eng.flush(), 1);
  let (log, _stable) = eng.stores(&7).unwrap();
  assert!(matches!(
    log.poll(),
    Some(Ok(LogDone::Appended(id))) if id == OpId::new(2)
  ));
  assert!(log.poll().is_none());
  assert_eq!(
    log.term(Index::new(3)),
    Ok(Term::new(1)),
    "the surviving prefix is intact"
  );
  assert_eq!(log.term(Index::new(4)), Ok(Term::new(2)));
}

/// The trait's visibility split: `submit_append` updates the log READ VIEW immediately (ahead of
/// durability) while `hard_state()` keeps returning the last-durable value; neither store has
/// anything to poll until the barrier releases the staged completions.
#[test]
fn reads_are_visible_before_durability() {
  let mut eng = GroupEngine::<u64, u64>::new();
  assert!(eng.add_group(7));
  let (log, stable) = eng.stores(&7).unwrap();

  log.submit_append(OpId::new(1), &[empty_entry(1, 1)]);
  assert_eq!(
    log.last_index(),
    Index::new(1),
    "the read view reflects the append immediately"
  );
  assert_eq!(log.term(Index::new(1)), Ok(Term::new(1)));
  match log.entries(Index::new(1)..Index::new(2), u64::MAX).unwrap() {
    EntriesRead::Ready(v) => assert_eq!(v.len(), 1, "the unflushed tail is readable"),
    EntriesRead::Pending => panic!("a resident engine never defers"),
  }

  let hs = HardState::initial().with_term(Term::new(3));
  stable.submit_write(OpId::new(2), hs.clone());
  assert_eq!(
    stable.hard_state(),
    HardState::initial(),
    "a staged write is invisible to hard_state()"
  );

  assert!(
    !log.has_pending() && log.poll().is_none(),
    "no log completion before the barrier"
  );
  assert!(
    !stable.has_pending() && stable.poll().is_none(),
    "no stable completion before the barrier"
  );

  assert_eq!(eng.flush(), 2, "the barrier completes both staged ops");

  let (log, stable) = eng.stores(&7).unwrap();
  assert_eq!(
    stable.hard_state(),
    hs,
    "the barrier advanced the durable HardState"
  );
  assert_eq!(log.poll(), Some(Ok(LogDone::Appended(OpId::new(1)))));
  assert_eq!(stable.poll(), Some(Ok(StableDone::Wrote(OpId::new(2)))));
}

/// The engine's reason to exist: THREE groups stage work independently and ONE barrier makes all
/// of it durable — the returned count is the cross-group total, and every group's FIFO holds its
/// completions afterward.
#[test]
fn one_flush_covers_every_group() {
  let mut eng = GroupEngine::<u64, u64>::new();
  for gid in [1u64, 2, 3] {
    assert!(eng.add_group(gid));
    let (log, stable) = eng.stores(&gid).unwrap();
    log.submit_append(OpId::new(gid), &[empty_entry(1, 1)]);
    stable.submit_write(
      OpId::new(gid + 10),
      HardState::initial().with_term(Term::new(1)),
    );
  }
  assert_eq!((eng.barriers(), eng.ops_batched()), (0, 0));

  assert_eq!(
    eng.flush(),
    6,
    "one barrier covers all three groups' appends and writes"
  );
  assert_eq!((eng.barriers(), eng.ops_batched()), (1, 6));

  for gid in [1u64, 2, 3] {
    let (log, stable) = eng.stores(&gid).unwrap();
    assert_eq!(
      log.poll(),
      Some(Ok(LogDone::Appended(OpId::new(gid)))),
      "group {gid}'s log completion is in ITS fifo"
    );
    assert_eq!(
      stable.poll(),
      Some(Ok(StableDone::Wrote(OpId::new(gid + 10))))
    );
    assert!(log.poll().is_none() && stable.poll().is_none());
  }

  assert_eq!(eng.flush(), 0, "nothing staged: the barrier is a no-op");
  assert_eq!((eng.barriers(), eng.ops_batched()), (2, 6));
}

/// Stable completions are ORDERED (submit order) per group: two hard-state writes and a snapshot
/// submitted in sequence release in exactly that sequence, and the visible/durable snapshot slots
/// split at the barrier.
#[test]
fn stable_completions_are_ordered_per_group() {
  let mut eng = GroupEngine::<u64, u64>::new();
  assert!(eng.add_group(1));
  let (_, stable) = eng.stores(&1).unwrap();

  stable.submit_write(OpId::new(1), HardState::initial().with_term(Term::new(1)));
  stable.submit_write(OpId::new(2), HardState::initial().with_term(Term::new(2)));
  let meta = voter_meta(5, 2);
  stable.submit_snapshot(OpId::new(3), meta.clone(), Bytes::from_static(b"snap"));

  let (got, blob) = stable
    .snapshot()
    .expect("the visible slot reflects the submit immediately");
  assert!(got.identity_eq(&meta));
  assert_eq!(blob, Bytes::from_static(b"snap"));
  assert!(
    stable.durable_snapshot().is_none(),
    "the durable slot waits for the barrier"
  );

  assert_eq!(eng.flush(), 3);
  let (_, stable) = eng.stores(&1).unwrap();
  assert_eq!(stable.poll(), Some(Ok(StableDone::Wrote(OpId::new(1)))));
  assert_eq!(stable.poll(), Some(Ok(StableDone::Wrote(OpId::new(2)))));
  assert_eq!(
    stable.poll(),
    Some(Ok(StableDone::SnapshotWritten(OpId::new(3))))
  );
  assert!(stable.poll().is_none());
  assert!(
    stable
      .durable_snapshot()
      .expect("durable after the barrier")
      .identity_eq(&meta)
  );
  assert_eq!(
    stable.hard_state().term(),
    Term::new(2),
    "the LAST submitted write is the durable value"
  );
}

/// Log durability is prefix-ordered by construction (the barrier completes every staged append at
/// once) and completions may release in any order — the engine picks submit order. Each
/// `Appended` carries its own submit's `OpId`, and a conflicting suffix truncates the READ VIEW
/// at submit time.
#[test]
fn log_prefix_durability_and_any_order() {
  let mut eng = GroupEngine::<u64, u64>::new();
  assert!(eng.add_group(1));
  let (log, _) = eng.stores(&1).unwrap();

  for i in 1u64..=3 {
    log.submit_append(OpId::new(i), &[empty_entry(1, i)]);
  }
  assert_eq!(log.last_index(), Index::new(3));

  log.submit_append(OpId::new(4), &[empty_entry(2, 2)]);
  assert_eq!(
    log.last_index(),
    Index::new(2),
    "the conflicting suffix is gone from the read view ahead of durability"
  );
  assert_eq!(log.term(Index::new(2)), Ok(Term::new(2)));
  assert_eq!(
    log.term(Index::new(3)),
    Ok(Term::ZERO),
    "the truncated index is out of domain"
  );

  assert_eq!(
    eng.flush(),
    2,
    "only the completions whose extents survived the truncation release"
  );
  let (log, _) = eng.stores(&1).unwrap();
  for i in [1u64, 4] {
    assert_eq!(
      log.poll(),
      Some(Ok(LogDone::Appended(OpId::new(i)))),
      "completion {i} carries its submit's id"
    );
  }
  assert!(
    log.poll().is_none(),
    "the truncated-away extents (ids 2 and 3) never complete: released, they would claim a \
     durable prefix through indices the barrier does not make durable"
  );
}

/// The chunk-staging trio: out-of-order chunks hold the contiguous watermark,
/// `take_staged_snapshot` consumes only a COMPLETE blob, `discard_snapshot_staging` frees a
/// partial, and a strictly-newer boundary supersedes while an older one is ignored.
#[test]
fn snapshot_staging_round_trips() {
  let mut eng = GroupEngine::<u64, u64>::new();
  assert!(eng.add_group(1));
  let (_, stable) = eng.stores(&1).unwrap();
  let meta = voter_meta(9, 2);

  assert_eq!(
    stable.accept_snapshot_chunk(&meta, 6, 0, &Bytes::from_static(b"ab")),
    Ok(2)
  );
  assert_eq!(
    stable.accept_snapshot_chunk(&meta, 6, 4, &Bytes::from_static(b"ef")),
    Ok(2),
    "a gap at [2,4) holds the contiguous watermark"
  );
  assert!(
    stable.take_staged_snapshot(&meta).is_none(),
    "an incomplete blob is not consumable"
  );
  assert_eq!(
    stable.accept_snapshot_chunk(&meta, 6, 2, &Bytes::from_static(b"cd")),
    Ok(6),
    "filling the gap completes the run"
  );
  assert_eq!(
    stable.take_staged_snapshot(&meta),
    Some(Bytes::from_static(b"abcdef"))
  );
  assert!(
    stable.take_staged_snapshot(&meta).is_none(),
    "consuming drops the accumulator"
  );

  // A discarded partial frees the single staging slot: the next chunk restarts from nothing.
  assert_eq!(
    stable.accept_snapshot_chunk(&meta, 6, 0, &Bytes::from_static(b"ab")),
    Ok(2)
  );
  stable.discard_snapshot_staging();
  assert_eq!(
    stable.accept_snapshot_chunk(&meta, 6, 2, &Bytes::from_static(b"cd")),
    Ok(0),
    "a fresh transfer after the discard has no contiguous prefix yet"
  );

  // A strictly-newer boundary supersedes the partial; an OLDER boundary cannot displace it.
  let newer = voter_meta(12, 2);
  assert_eq!(
    stable.accept_snapshot_chunk(&newer, 4, 0, &Bytes::from_static(b"wx")),
    Ok(2)
  );
  assert_eq!(
    stable.accept_snapshot_chunk(&meta, 6, 0, &Bytes::from_static(b"ab")),
    Ok(0),
    "an older boundary is ignored while newer staging is in flight"
  );
}

/// The staging slot is owned by ONE snapshot identity, LINEAGE included. Two transfers colliding on
/// `(last_index, last_term, conf)` AND `total_len` but belonging to DIFFERENT lineages are different bytes —
/// `(index, term)` is not a content-identity across a fork boundary — so the second must RESET the slot, and
/// a staged blob must never be handed out for another lineage's meta. Within one lineage, chunks still
/// accumulate normally.
///
/// MUTATION: drop `fork_id` from `SnapshotMeta::identity_eq` → the fork's chunk EXTENDS the tokenless
/// staging (`Ok(6)` instead of `Ok(0)`), and `take_staged_snapshot` hands the resulting MIXED blob
/// (b"AAABBB" — half one lineage, half another) to whichever meta asks. Both assertions FAIL.
#[test]
fn snapshot_staging_never_mixes_two_lineages() {
  let mut eng = GroupEngine::<u64, u64>::new();
  assert!(eng.add_group(1));
  let (_, stable) = eng.stores(&1).unwrap();

  let tokenless = voter_meta(10, 2);
  let token = ForkId::new(
    Bytes::from_static(&[7u8]),
    1,
    Index::new(4),
    Term::new(2),
    Bytes::from_static(&[9u8]),
    1,
  );
  let forked = voter_meta(10, 2).with_fork_id(token);

  assert_eq!(
    stable.accept_snapshot_chunk(&tokenless, 6, 0, &Bytes::from_static(b"AAA")),
    Ok(3),
    "the tokenless transfer stages [0,3)"
  );
  assert_eq!(
    stable.accept_snapshot_chunk(&forked, 6, 3, &Bytes::from_static(b"BBB")),
    Ok(0),
    "the fork's chunk RESETS the slot — it must never extend another lineage's staged bytes"
  );
  assert!(
    stable.take_staged_snapshot(&tokenless).is_none(),
    "the displaced lineage owns nothing"
  );
  assert!(
    stable.take_staged_snapshot(&forked).is_none(),
    "and the fork's own staging is still incomplete — no blob is fabricated from the other's bytes"
  );

  // The fork completes on its OWN bytes alone: same-lineage chunks accumulate exactly as before.
  assert_eq!(
    stable.accept_snapshot_chunk(&forked, 6, 0, &Bytes::from_static(b"BBB")),
    Ok(6)
  );
  assert_eq!(
    stable.take_staged_snapshot(&forked),
    Some(Bytes::from_static(b"BBBBBB")),
    "the installed blob is ONE lineage's bytes end to end"
  );
}

/// `term` is TOTAL over the peer-controlled probe domain — out-of-domain indices answer
/// `Ok(Term::ZERO)`, the boundary retains the compacted term — and `entries` is always `Ready`
/// (a resident engine never defers). Compaction's read-view effect is immediate; its completion
/// waits for the barrier.
#[test]
fn term_is_total_and_entries_ready() {
  let mut eng = GroupEngine::<u64, u64>::new();
  assert!(eng.add_group(1));
  let (log, _) = eng.stores(&1).unwrap();

  assert_eq!(
    log.term(Index::ZERO),
    Ok(Term::ZERO),
    "the empty-log origin boundary"
  );
  assert_eq!(
    log.term(Index::new(9)),
    Ok(Term::ZERO),
    "beyond last_index is Ok(ZERO), never Err"
  );

  for i in 1u64..=4 {
    log.submit_append(OpId::new(i), &[empty_entry(i, i)]);
  }
  log.compact(Index::new(2));
  assert_eq!(
    log.first_index(),
    Index::new(3),
    "compaction re-baselines the read view immediately"
  );
  assert_eq!(
    log.term(Index::new(2)),
    Ok(Term::new(2)),
    "the boundary term is retained"
  );
  assert_eq!(
    log.term(Index::new(1)),
    Ok(Term::ZERO),
    "below the boundary is out of domain"
  );
  assert_eq!(log.term(Index::new(4)), Ok(Term::new(4)));

  match log.entries(Index::new(3)..Index::new(5), u64::MAX).unwrap() {
    EntriesRead::Ready(v) => assert_eq!(v.len(), 2, "the in-range run is resident"),
    EntriesRead::Pending => panic!("a resident engine never returns Pending"),
  }

  assert!(
    !log.has_pending(),
    "the compaction completion is staged, not ready"
  );
  assert_eq!(eng.flush(), 5, "four appends + one compaction");
  let (log, _) = eng.stores(&1).unwrap();
  for i in 1u64..=4 {
    assert_eq!(log.poll(), Some(Ok(LogDone::Appended(OpId::new(i)))));
  }
  assert_eq!(log.poll(), Some(Ok(LogDone::Compacted(Index::new(2)))));
}

/// `restore` is the synchronous re-baseline: the read view moves before it returns, and EVERY
/// queued log completion (staged or already released) is dropped — a stale `Appended` after a
/// re-baseline would ack entries the store no longer holds.
#[test]
fn restore_rebaselines_and_drops_queued_completions() {
  let mut eng = GroupEngine::<u64, u64>::new();
  assert!(eng.add_group(1));
  let (log, _) = eng.stores(&1).unwrap();
  log.submit_append(OpId::new(1), &[empty_entry(1, 1)]);
  assert_eq!(eng.flush(), 1, "one completion is already RELEASED");
  let (log, _) = eng.stores(&1).unwrap();
  log.submit_append(OpId::new(2), &[empty_entry(1, 2)]);

  log.restore(Index::new(10), Term::new(3));
  assert_eq!(
    log.first_index(),
    Index::new(11),
    "first_index == last_index + 1"
  );
  assert_eq!(log.last_index(), Index::new(10));
  assert_eq!(
    log.term(Index::new(10)),
    Ok(Term::new(3)),
    "the snapshot boundary term"
  );
  assert!(
    !log.has_pending() && log.poll().is_none(),
    "released AND staged completions were both dropped"
  );
  assert_eq!(eng.flush(), 0, "nothing staged survived the re-baseline");
}

/// Boot epochs count per group and independently: the restore seam needs each group's incarnation
/// counter to be strictly increasing regardless of its neighbors'.
#[test]
fn boot_epochs_are_per_group_monotonic() {
  let mut eng = GroupEngine::<u64, u64>::new();
  assert!(eng.add_group(1));
  assert!(eng.add_group(2));
  assert_eq!(
    eng.next_boot_epoch(&1),
    Some(1),
    "the first incarnation is epoch 1"
  );
  assert_eq!(eng.next_boot_epoch(&1), Some(2));
  assert_eq!(
    eng.next_boot_epoch(&2),
    Some(1),
    "group 2 counts independently"
  );
  assert_eq!(eng.next_boot_epoch(&1), Some(3));
  assert_eq!(eng.next_boot_epoch(&2), Some(2));
  assert_eq!(eng.next_boot_epoch(&9), None, "no such group");
}

/// The boot-epoch counter is CHECKED: at `u64::MAX` it REFUSES (`None`) rather than wrapping to a
/// colliding epoch. A wrapped epoch restarts at 0 and folds two incarnations onto one
/// `(group, epoch)` identity for every gen-keyed observer — the identity fail-stop class the
/// `OpId`/read-round ceilings also hold.
#[test]
fn next_boot_epoch_refuses_exhaustion() {
  let mut eng = GroupEngine::<u64, u64>::new();
  assert!(eng.add_group(1));
  eng.set_boot_epochs_for_test(&1, u64::MAX);
  assert_eq!(
    eng.next_boot_epoch(&1),
    None,
    "a saturated counter refuses rather than wrapping onto a colliding identity"
  );
  assert_eq!(
    eng.next_boot_epoch(&1),
    None,
    "the refusal is stable — the counter holds at its ceiling, never wrapping to 0"
  );
}

/// `remove_group` is the teardown seam: the storage (including staged work) is dropped, the id is
/// re-admissible with FRESH empty storage, and an unknown group resolves to `None` — the
/// coordinator's deliberate unhosted-drop path.
#[test]
fn remove_group_drops_storage() {
  let mut eng = GroupEngine::<u64, u64>::new();
  assert!(eng.stores(&1).is_none(), "unknown group: no stores");
  assert!(!eng.remove_group(&1));

  assert!(eng.add_group(1));
  assert!(!eng.add_group(1), "a hosted id is not re-admitted");
  assert_eq!(
    (eng.len(), eng.is_empty(), eng.contains_group(&1)),
    (1, false, true)
  );
  assert_eq!(eng.group_ids().copied().collect::<Vec<_>>(), std::vec![1]);

  let (log, _) = eng.stores(&1).unwrap();
  log.submit_append(OpId::new(1), &[empty_entry(1, 1)]);
  assert!(eng.remove_group(&1));
  assert!(eng.is_empty());
  assert!(eng.stores(&1).is_none());
  assert_eq!(
    eng.flush(),
    0,
    "the removed group's staged work vanished with it"
  );

  assert!(eng.add_group(1), "the id is admissible again");
  let (log, _) = eng.stores(&1).unwrap();
  assert_eq!(
    log.last_index(),
    Index::ZERO,
    "re-admission starts from EMPTY storage"
  );
}

/// Lineage records (incarnation gen + admission floor) are the reshaping fence's storage: staged
/// writes read back immediately (monotone-max makes early visibility safe), they arm the barrier,
/// they fold at flush as one op per record — and they deliberately OUTLIVE `remove_group`, which
/// is the whole point of a floor.
#[test]
fn lineage_records_stage_read_fresh_and_survive_removal() {
  let mut eng: GroupEngine<u64, u64> = GroupEngine::new();
  assert_eq!(eng.group_floor(&7), 0, "never floored");
  eng.add_group(7);
  eng.set_group_gen(&7, 3);
  assert_eq!(eng.group_gen(&7), 3, "staged writes read immediately");
  assert!(eng.has_staged(), "pending lineage arms the barrier");
  assert_eq!(eng.flush(), 1, "one folded record = one op");
  assert!(!eng.has_staged());
  eng.set_group_floor(&7, 4);
  eng.set_group_floor(&7, 2); // monotone: lower write ignored
  assert_eq!(eng.group_floor(&7), 4);
  eng.flush();
  assert!(eng.remove_group(&7));
  assert_eq!(
    (eng.group_floor(&7), eng.group_gen(&7)),
    (4, 3),
    "lineage OUTLIVES the group"
  );
  eng.add_group(7);
  assert_eq!(
    eng.group_floor(&7),
    4,
    "re-admission does not reset lineage"
  );
  fn floors<G, S: FloorStore<G>>(s: &S, g: &G) -> (u64, u64) {
    (s.floor(g), s.lineage(g))
  }
  assert_eq!(floors(&eng, &7), (4, 3), "the engine IS the seam");
  assert_eq!(
    floors(&NoFloors, &7u64),
    (0, 0),
    "NoFloors is the gen-0 world"
  );
}

/// The removal ceiling's ORACLE: the SCAN the maintained ceiling stands in for — the surviving
/// resident log, the CURRENT snapshot slots, and the id's lineage record. Kept whole so the
/// maintained value is checked against something other than itself, and read at the moment it is
/// asked, which is exactly where a stale contribution would show.
fn scanned_removal_floor(eng: &GroupEngine<u64, u64>, gid: &u64) -> u64 {
  // Staged ⊔ durable, the same read the maintained answer uses: a removal stages its inherited
  // ceiling, so a scan that saw only the durable slot would model a fence the engine does not have.
  let mut ceiling = eng.group_gen(gid).max(
    eng
      .lineage
      .get(gid)
      .map_or(0, |r| r.ceiling)
      .max(eng.lineage_staged.get(gid).map_or(0, |r| r.ceiling)),
  );
  if let Some(storage) = eng.groups.get(gid) {
    let meta_gen = storage
      .stable
      .snapshot
      .as_ref()
      .map_or(0, |(m, _)| m.shape_gen())
      .max(
        storage
          .stable
          .durable_snapshot
          .as_ref()
          .map_or(0, SnapshotMeta::shape_gen),
      );
    ceiling = ceiling.max(meta_gen);
    for entry in &storage.log.entries {
      let minted = match entry.kind() {
        EntryKind::Split => {
          crate::wire::decode_split_payload(entry.data_bytes()).map_or(0, |p| p.parent_gen_after())
        }
        EntryKind::PrepareMerge => crate::wire::decode_prepare_merge_payload(entry.data_bytes())
          .map_or(0, |p| p.source_gen_after()),
        EntryKind::CommitMerge => crate::wire::decode_commit_merge_payload(entry.data_bytes())
          .map_or(0, |p| p.target_gen_after()),
        EntryKind::RollbackMerge => crate::wire::decode_rollback_merge_payload(entry.data_bytes())
          .map_or(0, |p| {
            if p.is_unfreeze() {
              p.source_gen_after()
            } else {
              p.target_gen_after()
            }
          }),
        _ => 0,
      };
      ceiling = ceiling.max(minted);
    }
  }
  if ceiling == 0 {
    0
  } else {
    ceiling.saturating_add(1)
  }
}

/// The five shape roles a log entry can carry, each naming its OWN group's post-move lineage in
/// a different payload field. The decoy values below make a mis-read field FAIL rather than
/// coincide.
#[derive(Debug, Clone, Copy)]
enum Shape {
  Split,
  Freeze,
  Absorb,
  Abort,
  Unfreeze,
}

/// A shape entry naming `gen_after` for its own group. Every field the extraction must IGNORE
/// carries `gen_after + 7`, so reading the wrong one lands the ceiling above the oracle.
fn shape_entry(index: u64, shape: Shape, gen_after: u64) -> Entry {
  let decoy = gen_after.saturating_add(7);
  let source = Bytes::from_static(&[9]);
  let mut buf = Vec::new();
  let kind = match shape {
    Shape::Split => {
      let p = crate::SplitPayload::new(source, decoy, gen_after, Bytes::new());
      crate::wire::encode_split_payload(&p, &mut buf);
      EntryKind::Split
    }
    Shape::Freeze => {
      let p = crate::PrepareMergePayload::new(source, gen_after);
      crate::wire::encode_prepare_merge_payload(&p, &mut buf);
      EntryKind::PrepareMerge
    }
    Shape::Absorb => {
      let p =
        crate::CommitMergePayload::new(source, Index::new(index), Term::new(1), decoy, gen_after);
      crate::wire::encode_commit_merge_payload(&p, &mut buf);
      EntryKind::CommitMerge
    }
    Shape::Abort => {
      let p = crate::RollbackMergePayload::abort(source, decoy, gen_after);
      crate::wire::encode_rollback_merge_payload(&p, &mut buf);
      EntryKind::RollbackMerge
    }
    Shape::Unfreeze => {
      let p = crate::RollbackMergePayload::unfreeze(gen_after);
      crate::wire::encode_rollback_merge_payload(&p, &mut buf);
      EntryKind::RollbackMerge
    }
  };
  Entry::new(Term::new(1), Index::new(index), kind, Bytes::from(buf))
}

/// One engine mutation in the ceiling differential — every move that can shift either the
/// maintained ceiling or the oracle's answer.
#[derive(Debug, Clone, Copy)]
enum Step {
  /// Append one entry at `index`; `None` is an ordinary command. An `index` at or below the
  /// resident last index is the §5.3 conflict rewind, which retracts the superseded suffix.
  Append {
    index: u64,
    shape: Option<Shape>,
    gen_after: u64,
  },
  Compact {
    up_to: u64,
  },
  /// REPLACE the visible snapshot slot with a meta claiming `shape_gen` at `last_index`.
  Snapshot {
    last_index: u64,
    shape_gen: u64,
  },
  /// Re-baseline the log at `last_index` — the snapshot-install path.
  Restore {
    last_index: u64,
  },
  /// Mirror the group's live lineage counter into the engine record (what a driver does eagerly).
  Mirror {
    generation: u64,
  },
  Flush,
  /// Tear the storage down and re-admit the id: the removal-to-rejoin cycle the floor fences.
  RemoveAndReadmit,
}

fn apply_step(eng: &mut GroupEngine<u64, u64>, step: Step, op: &mut u64) {
  *op += 1;
  match step {
    Step::Append {
      index,
      shape,
      gen_after,
    } => {
      let entry = match shape {
        Some(shape) => shape_entry(index, shape, gen_after),
        None => empty_entry(1, index),
      };
      let (log, _) = eng.stores(&1).unwrap();
      log.submit_append(OpId::new(*op), &[entry]);
    }
    Step::Compact { up_to } => {
      let (log, _) = eng.stores(&1).unwrap();
      log.compact(Index::new(up_to));
    }
    Step::Snapshot {
      last_index,
      shape_gen,
    } => {
      let meta = voter_meta(last_index, 1).with_shape_gen(shape_gen);
      let (_, stable) = eng.stores(&1).unwrap();
      stable.submit_snapshot(OpId::new(*op), meta, Bytes::from_static(&[1, 2, 3]));
    }
    Step::Restore { last_index } => {
      let (log, _) = eng.stores(&1).unwrap();
      log.restore(Index::new(last_index), Term::new(1));
    }
    Step::Mirror { generation } => eng.set_group_gen(&1, generation),
    Step::Flush => {
      eng.flush();
    }
    Step::RemoveAndReadmit => {
      assert!(eng.remove_group(&1));
      assert!(eng.add_group(1));
    }
  }
}

/// Shorthand for the vectors below: an ordinary command at `index`.
const fn plain(index: u64) -> Step {
  Step::Append {
    index,
    shape: None,
    gen_after: 0,
  }
}

/// Shorthand for the vectors below: a shape entry at `index` minting `gen_after`.
const fn mint(index: u64, shape: Shape, gen_after: u64) -> Step {
  Step::Append {
    index,
    shape: Some(shape),
    gen_after,
  }
}

/// THE CEILING DIFFERENTIAL. `removal_floor` maintains its ceiling as state is written instead of
/// scanning the log when a removal asks, and the maintenance is EXACT: every vector below is
/// replayed against the scan it replaced, asserting STRICT EQUALITY at every step — across
/// conflict truncation, compaction, `restore`, snapshot-slot replacement, and the
/// removal-to-rejoin cycle. Where a step also pins an ABSOLUTE answer, the vector carries it,
/// because equality alone cannot catch two legs that drifted together.
#[test]
fn the_removal_ceiling_tracks_the_scan_exactly() {
  // Every field a shape role must ignore is decoyed, so these vectors also pin the extraction:
  // reading `source_gen_after` where `target_gen_after` is meant lands 7 above the oracle.
  let vectors: &[&[(Step, Option<u64>)]] = &[
    // Every shape role in one lineage, nothing dropped, then a MIRRORED teardown.
    &[
      (plain(1), Some(0)),
      (mint(2, Shape::Split, 3), Some(4)),
      (mint(3, Shape::Freeze, 5), Some(6)),
      (Step::Flush, Some(6)),
      (mint(4, Shape::Absorb, 9), Some(10)),
      (mint(5, Shape::Abort, 11), Some(12)),
      (mint(6, Shape::Unfreeze, 12), Some(13)),
      (Step::Flush, Some(13)),
      (Step::Mirror { generation: 12 }, Some(13)),
      (
        Step::Snapshot {
          last_index: 6,
          shape_gen: 12,
        },
        Some(13),
      ),
      (Step::Flush, Some(13)),
      (Step::RemoveAndReadmit, Some(13)),
    ],
    // §5.3: a conflicting append supersedes the shape entry, and the ceiling RETRACTS with it —
    // back to exactly the answer that stood before the doomed mint was ever staged.
    &[
      (mint(1, Shape::Freeze, 2), Some(3)),
      (mint(2, Shape::Split, 6), Some(7)),
      (plain(2), Some(3)),
      (Step::Flush, Some(3)),
      // And again, deeper: the rewind takes BOTH mints when it lands under them.
      (mint(3, Shape::Absorb, 8), Some(9)),
      (plain(1), Some(0)),
      (Step::Flush, Some(0)),
    ],
    // Compaction pops the prefix: the surviving suffix's mint still answers, and once the last
    // shape entry is compacted away the id is back to an unfenced gen-0 rejoin.
    &[
      (mint(1, Shape::Freeze, 2), Some(3)),
      (plain(2), Some(3)),
      (mint(3, Shape::Split, 5), Some(6)),
      (Step::Compact { up_to: 2 }, Some(6)),
      (Step::Compact { up_to: 3 }, Some(0)),
      (Step::Flush, Some(0)),
    ],
    // The realistic compaction order — snapshot first, then drop the prefix: the meta leg picks
    // up exactly what the log leg gives back.
    &[
      (mint(1, Shape::Freeze, 2), Some(3)),
      (plain(2), Some(3)),
      (mint(3, Shape::Split, 4), Some(5)),
      (
        Step::Snapshot {
          last_index: 3,
          shape_gen: 4,
        },
        Some(5),
      ),
      (Step::Compact { up_to: 3 }, Some(5)),
      (Step::Flush, Some(5)),
      (Step::Restore { last_index: 3 }, Some(5)),
      (Step::Mirror { generation: 4 }, Some(5)),
    ],
    // A `restore` with NO covering meta clears the log leg outright — the re-baselined entry is
    // one this log never kept.
    &[
      (mint(1, Shape::Absorb, 8), Some(9)),
      (Step::Restore { last_index: 5 }, Some(0)),
      (Step::Flush, Some(0)),
    ],
    // THE META LEG ALONE: a replica caught up by snapshot INSTALL never appended the shape
    // entries, so the installed meta is the only place its lineage is written.
    &[
      (plain(1), Some(0)),
      (
        Step::Snapshot {
          last_index: 4,
          shape_gen: 6,
        },
        Some(7),
      ),
      (Step::Restore { last_index: 4 }, Some(7)),
      (Step::Flush, Some(7)),
      (Step::Mirror { generation: 6 }, Some(7)),
      (Step::RemoveAndReadmit, Some(7)),
    ],
    // SLOT REPLACEMENT: a meta REPLACES its slot, it does not accumulate into it. While the
    // durable slot still holds the older capture the higher lineage stands; once the barrier
    // advances that slot too, the displaced meta stops counting entirely.
    &[
      (
        Step::Snapshot {
          last_index: 3,
          shape_gen: 9,
        },
        Some(10),
      ),
      (Step::Flush, Some(10)),
      (
        Step::Snapshot {
          last_index: 5,
          shape_gen: 4,
        },
        Some(10),
      ),
      (Step::Flush, Some(5)),
    ],
    // THE STORES-ONLY TEARDOWN: nothing mirrored the freeze, so the record leg is what carries
    // the fence across the teardown — the survival the floor exists for.
    &[
      (mint(1, Shape::Freeze, 2), Some(3)),
      (Step::RemoveAndReadmit, Some(3)),
      (plain(1), Some(3)),
    ],
    // The same teardown for a snapshot-installed replica, whose lineage lives ONLY in its meta.
    &[
      (
        Step::Snapshot {
          last_index: 4,
          shape_gen: 6,
        },
        Some(7),
      ),
      (Step::RemoveAndReadmit, Some(7)),
    ],
    // An id that NEVER reshapes keeps the gen-0 rejoin through every move — no phantom record
    // may appear, or a group that never split would come back fenced.
    &[
      (plain(1), Some(0)),
      (plain(2), Some(0)),
      (
        Step::Snapshot {
          last_index: 2,
          shape_gen: 0,
        },
        Some(0),
      ),
      (Step::Compact { up_to: 2 }, Some(0)),
      (Step::Flush, Some(0)),
      (Step::RemoveAndReadmit, Some(0)),
    ],
    // A SURVIVING mint at the highest working generation does saturate onto the terminal: the id
    // can never reshape again, which is the truthful verdict here.
    &[(mint(1, Shape::Freeze, MERGED_FLOOR - 1), Some(MERGED_FLOOR))],
  ];

  for (v, steps) in vectors.iter().enumerate() {
    let mut eng: GroupEngine<u64, u64> = GroupEngine::new();
    assert!(eng.add_group(1));
    assert_eq!(eng.removal_floor(&1), 0, "vector {v} starts unfenced");
    let mut op = 0;
    for (s, (step, pinned)) in steps.iter().enumerate() {
      apply_step(&mut eng, *step, &mut op);
      let maintained = eng.removal_floor(&1);
      let scanned = scanned_removal_floor(&eng, &1);
      assert_eq!(
        maintained, scanned,
        "[{v}] step {s} ({step:?}): the maintained ceiling and the scan disagree — the \
         maintenance is exact or it is a fence over state that no longer exists"
      );
      if let Some(pinned) = pinned {
        assert_eq!(
          maintained, *pinned,
          "[{v}] step {s} ({step:?}): both legs agree on {maintained}, but the answer this step \
           owes is {pinned}"
        );
      }
    }
  }
}

/// A never-reshaped id keeps its gen-0 rejoin even after a shape entry was STAGED and then
/// superseded. The mint rode an uncommitted suffix this log threw away, so it fences nothing:
/// carrying it would strand the id above a floor no committed entry ever justified, and the
/// volatile tombstone would be the only thing left admitting it back.
#[test]
fn a_superseded_mint_leaves_the_gen_zero_rejoin_intact() {
  let mut eng: GroupEngine<u64, u64> = GroupEngine::new();
  assert!(eng.add_group(1));
  assert_eq!(eng.removal_floor(&1), 0, "never reshaped");

  {
    let (log, _) = eng.stores(&1).unwrap();
    log.submit_append(OpId::new(1), &[shape_entry(1, Shape::Freeze, 1)]);
  }
  assert_eq!(eng.removal_floor(&1), 2, "while it stands, the mint fences");

  // The §5.3 rewind: a conflicting append at the same index supersedes it.
  {
    let (log, _) = eng.stores(&1).unwrap();
    log.submit_append(OpId::new(2), &[empty_entry(1, 1)]);
  }
  assert_eq!(
    eng.removal_floor(&1),
    0,
    "a superseded mint fences nothing — the id never reshaped"
  );

  // The driver's removal order: read the floor, persist it, then drop the stores.
  let floor = eng.removal_floor(&1);
  eng.set_group_floor(&1, floor);
  assert!(eng.remove_group(&1));
  assert_eq!(
    eng.removal_floor(&1),
    0,
    "the teardown inherits no phantom fence"
  );
  assert_eq!(eng.floor(&1), 0, "no floor was persisted");
  assert!(
    crate::floor_admits(eng.floor(&1), 0),
    "the gen-0 volatile-tombstone rejoin stands"
  );
  assert!(eng.add_group(1), "and the id is re-admissible");
}

/// A SUPERSEDED mint at the top of the working range must not forge the reserved terminal. The
/// `+ 1` saturates onto [`MERGED_FLOOR`] — the fence the merge service reads as authoritative
/// when it authorizes a dead target's thaw or retires a husk — so a ceiling that kept a
/// truncated-away generation would manufacture a globally terminal verdict out of an entry no
/// replica ever committed. The retraction must restore the EXACT answer that stood before it.
#[test]
fn a_superseded_mint_cannot_forge_the_terminal_floor() {
  let mut eng: GroupEngine<u64, u64> = GroupEngine::new();
  assert!(eng.add_group(1));

  // A move that genuinely survives — the answer the id must come back to.
  {
    let (log, _) = eng.stores(&1).unwrap();
    log.submit_append(OpId::new(1), &[shape_entry(1, Shape::Freeze, 2)]);
  }
  let before = eng.removal_floor(&1);
  assert_eq!(before, 3);

  // A mint at the highest WORKING generation, staged and then superseded at the same index.
  {
    let (log, _) = eng.stores(&1).unwrap();
    log.submit_append(
      OpId::new(2),
      &[shape_entry(2, Shape::Split, MERGED_FLOOR - 1)],
    );
  }
  assert_eq!(
    eng.removal_floor(&1),
    MERGED_FLOOR,
    "while it stands the saturation is truthful — the id could not reshape again"
  );
  {
    let (log, _) = eng.stores(&1).unwrap();
    log.submit_append(OpId::new(3), &[empty_entry(1, 2)]);
  }

  // The saturation lives in `removal_floor` itself, so the engine's own answer is the assertion
  // point — no driver is needed to observe the forgery.
  assert_ne!(
    eng.removal_floor(&1),
    MERGED_FLOOR,
    "a superseded mint must NOT forge the terminal floor"
  );
  assert_eq!(
    eng.removal_floor(&1),
    before,
    "the retraction restores exactly the answer the surviving state justifies"
  );

  let floor = eng.removal_floor(&1);
  eng.set_group_floor(&1, floor);
  assert!(eng.remove_group(&1));
  assert_eq!(
    eng.removal_floor(&1),
    before,
    "the teardown inherits the SURVIVING ceiling only"
  );
  assert_ne!(eng.floor(&1), MERGED_FLOOR, "no terminal floor persisted");
}

/// The Phase-2 payoff end to end: two multi-group coordinator hosts, each backed by ONE
/// [`GroupEngine`] as its `GroupStores`, drive groups 100 and 200 over a single shared connection
/// with `flush()` as the only durability barrier — and the barrier BATCHES across groups.
#[cfg(feature = "tcp")]
mod engine_backed_cluster {
  use super::*;
  use crate::{
    ClusterId, Config, ConnId, Data, Instant, LabelOptions, Labeled, MultiStreamCoordinator,
    Passthrough, testkit::CountSm,
  };
  use core::time::Duration;
  use std::vec::Vec;

  type MultiCoord = MultiStreamCoordinator<u64, u64, CountSm, Labeled<Passthrough>>;
  type Engine = GroupEngine<u64, u64>;

  fn two_voter(id: u64) -> Config<u64> {
    Config::try_new(
      id,
      std::vec![1, 2],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap()
  }

  fn label(id: u64, role_dialer: bool) -> Labeled<Passthrough> {
    let mut local_id = Vec::new();
    id.encode(&mut local_id);
    let opts = LabelOptions {
      cluster: ClusterId([1; 16]),
      local_id,
    };
    if role_dialer {
      Labeled::dialer(Passthrough::new(), &opts).unwrap()
    } else {
      Labeled::acceptor(Passthrough::new(), &opts).unwrap()
    }
  }

  /// A two-node world where node 1 (`a`) dials node 2 (`b`) and each host's storage is ONE
  /// [`GroupEngine`] shared by groups 100 and 200. Only `a`'s timers fire, so elections are
  /// deterministic (`b` grants and never campaigns).
  struct World {
    a: MultiCoord,
    b: MultiCoord,
    ea: Engine,
    eb: Engine,
    now: Instant,
    /// `ea.flush()` calls that released at least one op (the effective barriers).
    a_effective_flushes: u64,
    /// Total ops those barriers released (must mirror `ea.ops_batched()`).
    a_ops: u64,
  }

  impl World {
    fn new() -> Self {
      let mut a = MultiCoord::new();
      let mut b = MultiCoord::new();
      let mut ea = Engine::new();
      let mut eb = Engine::new();
      for g in [100u64, 200] {
        a.create_group(
          g,
          two_voter(1),
          Instant::ORIGIN,
          1,
          CountSm::default(),
          0,
          &NoFloors,
        )
        .unwrap();
        assert!(ea.add_group(g));
        b.create_group(
          g,
          two_voter(2),
          Instant::ORIGIN,
          2,
          CountSm::default(),
          0,
          &NoFloors,
        )
        .unwrap();
        assert!(eb.add_group(g));
      }
      let ca = a.on_dial_open(2, label(1, true), Instant::ORIGIN);
      let cb = b.on_accept_open(label(2, false), Instant::ORIGIN);
      assert_eq!(ca, cb, "first allocation on both sides");
      World {
        a,
        b,
        ea,
        eb,
        now: Instant::ORIGIN,
        a_effective_flushes: 0,
        a_ops: 0,
      }
    }

    /// Run `a`'s barrier through the metric accounting.
    fn flush_a(&mut self) -> usize {
      let n = self.ea.flush();
      if n > 0 {
        self.a_effective_flushes += 1;
        self.a_ops += n as u64;
      }
      n
    }

    /// One crank = one barrier per host, then drain storage completions, then move wire bytes.
    /// Loops until a full crank releases nothing and moves nothing.
    fn settle(&mut self) {
      for _ in 0..200 {
        let released = self.flush_a() + self.eb.flush();
        for g in [100u64, 200] {
          let now = self.now;
          let (l, s) = self.ea.stores(&g).unwrap();
          let _ = self.a.handle_storage(&g, now, l, s);
          let (l, s) = self.eb.stores(&g).unwrap();
          let _ = self.b.handle_storage(&g, now, l, s);
        }
        let from_a = self.a.poll_transmit();
        let from_b = self.b.poll_transmit();
        let mut moved = false;
        for (_, bytes) in &from_a {
          if !bytes.is_empty() {
            self
              .b
              .handle_conn_data(ConnId(1), bytes, false, self.now, &mut self.eb);
            moved = true;
          }
        }
        for (_, bytes) in &from_b {
          if !bytes.is_empty() {
            self
              .a
              .handle_conn_data(ConnId(1), bytes, false, self.now, &mut self.ea);
            moved = true;
          }
        }
        if !moved && released == 0 {
          break;
        }
      }
    }

    /// Fire `group`'s timers on `a` at (or after) that group's own deadline, then settle.
    fn fire_a(&mut self, group: u64) {
      let d = self.a.group(&group).unwrap().poll_timeout().unwrap();
      self.now = self.now.max(d);
      let now = self.now;
      let (l, s) = self.ea.stores(&group).unwrap();
      self.a.handle_timeout(&group, now, l, s).unwrap();
      self.settle();
    }

    /// Drive `a`'s `group` to leadership by firing ONLY its timers.
    fn elect_a(&mut self, group: u64) {
      for _ in 0..40 {
        if self.a.group(&group).unwrap().role().is_leader() {
          return;
        }
        self.fire_a(group);
      }
      panic!("group {group} did not elect a leader");
    }
  }

  #[test]
  fn engine_backs_two_hosts_and_batches_across_groups() {
    let mut w = World::new();
    w.settle(); // complete the label handshake
    assert_eq!(w.a.conn_of(&2), Some(ConnId(1)), "node 1 bound peer 2");
    assert_eq!(w.b.conn_of(&1), Some(ConnId(1)), "node 2 bound peer 1");

    w.elect_a(100);
    w.elect_a(200);
    assert!(w.a.group(&100).unwrap().role().is_leader());
    assert!(w.a.group(&200).unwrap().role().is_leader());
    assert!(
      w.b.group(&100).unwrap().term() >= Term::new(1),
      "b's group 100 followed the election through the engine-backed drive"
    );
    assert!(
      w.b.group(&200).unwrap().term() >= Term::new(1),
      "b's group 200 followed the election through the engine-backed drive"
    );

    // The cross-group batch, deterministically: stage a proposal in BOTH groups, then release
    // them with ONE barrier.
    let now = w.now;
    let cmd = Bytes::copy_from_slice(&[7u8]);
    {
      let (l, s) = w.ea.stores(&100).unwrap();
      w.a.submit_propose(&100, now, l, s, &cmd).unwrap().unwrap();
    }
    {
      let (l, s) = w.ea.stores(&200).unwrap();
      w.a.submit_propose(&200, now, l, s, &cmd).unwrap().unwrap();
    }
    let batched = w.flush_a();
    assert!(
      batched >= 2,
      "one flush covered both groups' staged appends, got {batched}"
    );
    w.settle();

    assert!(
      w.a.group(&100).unwrap().state_machine().count() >= 1,
      "group 100 committed through the shared engine"
    );
    assert!(
      w.a.group(&200).unwrap().state_machine().count() >= 1,
      "group 200 committed through the shared engine"
    );

    // The batching metric: every effective barrier released at least one op and the cross-group
    // barrier above released more, so completed ops strictly outnumber the barriers that
    // completed them.
    assert_eq!(
      w.ea.ops_batched(),
      w.a_ops,
      "the engine's cumulative metric matches the harness's accounting"
    );
    assert!(
      w.a_ops > w.a_effective_flushes,
      "batching happened: {} ops completed across {} effective flushes",
      w.a_ops,
      w.a_effective_flushes
    );
  }
}
