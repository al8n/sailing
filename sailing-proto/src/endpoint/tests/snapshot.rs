use super::*;

/// Regression (snapshot restore RESETS `durable_index`, never `max`): a follower with a
/// DURABLE divergent tail ABOVE a later, SHORTER snapshot must not keep a stale-high watermark.
///
/// Setup: the follower flushes a durable-but-uncommitted tail (indices 1..=3, term 1), so
/// `durable_index == 3` while `commit == 0`. It then installs a LOWER snapshot (last_index=2,
/// term 2). `restore` re-baselines the log to last_index 2 and DISCARDS the tail, so the durable
/// boundary is now exactly 2. A new entry (index 3, term 2) is appended in-flight (NOT flushed),
/// and a DUPLICATE of it arrives before `handle_storage` drains. The immediate-ack clamp
/// (`last_new.min(durable_index)`) must report 2 (the snapshot boundary), not the unflushed 3.
///
/// MUTATION: revert FIX 1 to `self.durable_index = self.durable_index.max(meta.last_index())`.
/// Then after install `durable_index` stays at the stale-high 3, the duplicate clamps to
/// `min(3, 3) = 3`, and the assertion (duplicate acks 2) FAILS — the follower over-acks an
/// unflushed entry, reopening the phantom-replica commit hole.
#[test]
fn snapshot_install_resets_durable_index_below_divergent_tail() {
  use crate::{
    AppendEntries, Config, Entry, EntryKind, Index, InstallSnapshot, Instant, Message,
    SnapshotMeta, Term, conf::ConfState,
  };
  use core::time::Duration;
  let cfg = Config::try_new(
    2u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::AsyncStable::default();

  // Durable-but-uncommitted tail: entries 1..=3 at term 1, leader_commit=0.
  let tail: std::vec::Vec<Entry> = (1u64..=3)
    .map(|i| {
      Entry::new(
        Term::new(1),
        Index::new(i),
        EntryKind::Normal,
        bytes::Bytes::from_static(b"old"),
      )
    })
    .collect();
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(1),
      1u64,
      Index::ZERO,
      Term::ZERO,
      tail,
      Index::ZERO,
    )),
  );
  // Flush so the divergent tail becomes durable: durable_index == 3, commit still 0.
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  assert_eq!(ep.durable_index, Index::new(3), "tail flushed → durable=3");
  assert_eq!(ep.commit, Index::ZERO, "tail is uncommitted");

  // Install a LOWER snapshot (last_index=2 > commit=0 → install proceeds, discards the tail).
  let meta = SnapshotMeta::new(
    Index::new(2),
    Term::new(2),
    ConfState::from_voters(std::vec![1u64, 2u64, 3u64]),
  );
  let snap_data = encode_snapshot(7u64);
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::InstallSnapshot(InstallSnapshot::new(Term::new(1), 1u64, meta, snap_data)),
  );
  // the install is DEFERRED — drive handle_storage so SnapshotWritten fires `install_snapshot_now`,
  // which runs the destructive re-baseline (the blob is durable by then).
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  assert_eq!(
    log.last_index(),
    Index::new(2),
    "restore re-baselined the log to the snapshot boundary"
  );
  assert_eq!(
    ep.durable_index,
    Index::new(2),
    "RESET: durable boundary IS the snapshot's last index (not the stale-high tail at 3)"
  );

  // Establish term 2 as a DURABLE term first (a follower must not ack under a non-durable term).
  // A higher-term heartbeat (no entries) adopts term 2; draining storage makes that term write durable
  // WITHOUT flushing any log tail — modelling a follower already at a durable term 2 before the
  // in-flight entry arrives. (The divergent tail at 3 was already discarded by the install above.)
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(2),
      1u64,
      Index::new(2),
      Term::new(2),
      std::vec![],
      Index::ZERO,
    )),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  assert_eq!(
    ep.durable_index,
    Index::new(2),
    "the term-2 heartbeat made the term durable without flushing a tail"
  );

  // Append ONE genuinely-new entry (index 3, term 2) in-flight — do NOT flush.
  let e3 = Entry::new(
    Term::new(2),
    Index::new(3),
    EntryKind::Normal,
    bytes::Bytes::from_static(b"new"),
  );
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(2),
      1u64,
      Index::new(2),
      Term::new(2),
      std::vec![e3.clone()],
      Index::ZERO,
    )),
  );
  while ep.poll_message().is_some() {}
  assert_eq!(
    log.last_index(),
    Index::new(3),
    "new entry 3 is visible in-flight"
  );

  // DUPLICATE of entry 3 BEFORE draining → immediate-ack path clamps to durable_index.
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(2),
      1u64,
      Index::new(2),
      Term::new(2),
      std::vec![e3],
      Index::ZERO,
    )),
  );
  let dup = ep
    .poll_message()
    .expect("duplicate emits an immediate AppendResp");
  match dup.message() {
    Message::AppendResp(a) => {
      assert!(!a.reject(), "duplicate is a success ack");
      assert_eq!(
        a.match_index(),
        Index::new(2),
        "persist-before-ack: the duplicate must report the snapshot boundary (2), \
           not the unflushed in-flight entry 3"
      );
    }
    other => panic!("expected AppendResp, got {other:?}"),
  }
}

/// After applying past `snapshot_threshold`, a single `handle_storage` call should:
/// 1. Submit a snapshot to stable (readable via `stable.snapshot().is_some()`).
/// 2. Set `pending_compact` to the deferred (opid, applied) pair.
///
/// The log is NOT yet compacted — the SnapshotWritten completion hasn't fired.
#[test]
fn snapshot_submitted_and_pending_compact_set() {
  // threshold=3 means we snapshot once applied - first_index >= 3.
  // After no-op (idx 1) + 3 Normal entries (idx 2,3,4), applied=4, first_index=1 → gap=3.
  let (ep, log, stable) = make_single_node_leader_with_entries(3, 3);

  // snapshot was persisted in stable
  assert!(
    stable.snapshot().is_some(),
    "stable must hold the persisted snapshot"
  );
  // pending_compact is set (snapshot write in flight, compaction deferred)
  assert!(
    ep.pending_compact().is_some(),
    "pending_compact must be set while snapshot write is in flight"
  );
  // log is NOT yet compacted (compaction deferred until SnapshotWritten)
  assert_eq!(
    log.first_index(),
    Index::new(1),
    "log must not be compacted before SnapshotWritten fires"
  );
}

/// After the `SnapshotWritten` completion fires (second `handle_storage`), the deferred
/// compaction executes: `log.first_index()` advances and `pending_compact` is cleared.
#[test]
fn deferred_compact_fires_on_snapshot_written() {
  let (mut ep, mut log, mut stable) = make_single_node_leader_with_entries(3, 3);

  // Drain the SnapshotWritten completion → deferred compact fires.
  ep.handle_storage(crate::Instant::ORIGIN, &mut log, &mut stable);

  // Log is now compacted: first_index advanced past the initial first_index.
  assert!(
    log.first_index() > Index::new(1),
    "first_index must advance after SnapshotWritten fires (got {:?})",
    log.first_index()
  );
  // pending_compact cleared
  assert!(
    ep.pending_compact().is_none(),
    "pending_compact must be None after compaction fires"
  );
}

/// While `pending_compact` is set, `maybe_snapshot` must not fire again (idempotence guard).
#[test]
fn maybe_snapshot_does_not_refire_while_pending() {
  let (mut ep, mut log, mut stable) = make_single_node_leader_with_entries(3, 3);

  // At this point pending_compact is Some. Drain again without clearing the completion —
  // but since AsyncStable enqueues SnapshotWritten only once, calling handle_storage again
  // before any new completion simply runs maybe_snapshot again. The guard must prevent a
  // second submit_snapshot.
  let snap_count_before = stable.snapshot().map(|_| 1usize).unwrap_or(0);

  // Call handle_storage again — no new completion available yet (already drained above),
  // so maybe_snapshot runs again. With the guard it must be a no-op.
  ep.handle_storage(crate::Instant::ORIGIN, &mut log, &mut stable);
  // We shouldn't have gotten a SECOND snapshot submission — check pending_compact is still set.
  // (It won't be cleared because there's no new SnapshotWritten completion.)
  // The stable still has exactly one snapshot (no double-submit).
  let snap_count_after = stable.snapshot().map(|_| 1usize).unwrap_or(0);
  assert_eq!(
    snap_count_before, snap_count_after,
    "maybe_snapshot must not re-fire while pending_compact is set"
  );
}

/// A dropped `SnapshotWritten` completion must NOT permanently wedge `pending_compact`
/// (and thus all future snapshots/compaction). `handle_storage` reconciles `pending_compact`
/// against the durable snapshot: once the persisted snapshot covers `up_to`, the deferred
/// compaction is performed and the field cleared, even though the completion was never seen.
///
/// FAILS ON OLD CODE (no reconciliation): `pending_compact` stays `Some`, `first_index` never
/// advances, and the `is_some()` guard in `maybe_snapshot` wedges every future snapshot.
#[test]
fn dropped_snapshot_completion_reconciled_against_durable_snapshot() {
  // threshold=3: after no-op (idx 1) + 3 entries (idx 2,3,4), applied=4, first_index=1 → gap=3,
  // so a snapshot is submitted — but its completion is dropped by the armed store.
  let (mut ep, mut log, mut stable) = make_single_node_leader_dropping_snapshot_completion(3, 3);

  // Precondition: the snapshot blob IS durable, but pending_compact is stuck (no completion),
  // and the log was NOT compacted (the deferred compact never ran).
  assert!(
    stable.snapshot().is_some(),
    "the durable snapshot blob must be persisted even though the completion was dropped"
  );
  assert!(
    ep.pending_compact().is_some(),
    "pending_compact must still be set (the SnapshotWritten completion was dropped)"
  );
  assert_eq!(
    log.first_index(),
    Index::new(1),
    "log must not be compacted yet (no completion drained the deferred compact)"
  );

  // Drive handle_storage again. There is NO SnapshotWritten completion to drain, so on OLD code
  // this would be a no-op and the node would stay wedged. The reconciliation must instead
  // notice the durable snapshot covers `up_to`, perform the compaction, and clear pending_compact.
  ep.handle_storage(crate::Instant::ORIGIN, &mut log, &mut stable);

  assert!(
    ep.pending_compact().is_none(),
    "pending_compact must be reconciled to None against the durable snapshot"
  );
  assert!(
    log.first_index() > Index::new(1),
    "the deferred compaction must run via reconciliation (first_index advanced, got {:?})",
    log.first_index()
  );

  // The node is no longer wedged: keep applying until the gap past the (new) first_index reaches
  // the threshold again, and a NEW snapshot must fire (pending_compact set for the fresh point).
  // After reconciliation first_index == 5 (compacted up_to=4); applied must reach 8 for gap >= 3.
  let first_index_after_reconcile = log.first_index();
  let d = crate::Instant::ORIGIN;
  for i in 0..4usize {
    let cmd = bytes::Bytes::copy_from_slice(&[100 + i as u8]);
    let _ = ep.propose(d, &mut log, &stable, &cmd).unwrap();
    ep.handle_storage(d, &mut log, &mut stable);
    while ep.poll_message().is_some() {}
    while ep.poll_event().is_some() {}
  }
  assert!(
    ep.pending_compact().is_some(),
    "after reconciliation the node can snapshot again (not wedged)"
  );
  // And draining the (this time delivered) completion compacts further, proving end-to-end health.
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(
    ep.pending_compact().is_none(),
    "the follow-up snapshot's completion clears pending_compact normally"
  );
  assert!(
    log.first_index() > first_index_after_reconcile,
    "the follow-up compaction advances first_index further (got {:?})",
    log.first_index()
  );
}

/// Test 1: sends InstallSnapshot when next_index < first_index.
#[test]
fn sends_install_snapshot_on_compacted_hole() {
  use crate::{Index, Message};

  let offset = 5u64;
  let (mut ep, log, stable) = make_leader_with_compacted_log(offset, 2);

  // Set peer 2's progress so next_index = 3 < first_index = 6.
  let far_behind = Index::new(3);
  if let Some(p) = ep.tracker.progress_mut(&2u64) {
    p.become_probe();
    p.set_next_index(far_behind);
  }

  // Call maybe_send_append; it should detect next_index < first_index and send snapshot.
  ep.maybe_send_append(2u64, &log, &stable);

  // Exactly one outgoing message to peer 2 must be InstallSnapshot.
  let msgs: std::vec::Vec<_> = core::iter::from_fn(|| ep.poll_message()).collect();
  let snap_msgs: std::vec::Vec<_> = msgs
    .iter()
    .filter(|o| o.to() == 2u64 && matches!(o.message(), Message::InstallSnapshot(_)))
    .collect();
  assert_eq!(
    snap_msgs.len(),
    1,
    "exactly one InstallSnapshot must be sent to peer 2"
  );

  let snap_msg = match snap_msgs[0].message() {
    Message::InstallSnapshot(s) => s,
    _ => unreachable!(),
  };
  // The snapshot must match what stable holds (last_index = offset).
  assert_eq!(
    snap_msg.snapshot().last_index(),
    Index::new(offset),
    "InstallSnapshot must carry the persisted snapshot's last_index"
  );

  // Peer 2's progress must now be in Snapshot state with pending = offset.
  let pr = ep.tracker.progress(&2u64).unwrap();
  assert!(
    pr.state().is_snapshot(),
    "peer 2 must be in Snapshot state after sending InstallSnapshot"
  );
  if let crate::ProgressState::Snapshot(pending) = pr.state() {
    assert_eq!(
      pending,
      Index::new(offset),
      "Snapshot pending index must equal the snapshot's last_index"
    );
  }
}

/// Test 2: no broken AppendEntries (prev_log_term == ZERO) for compacted peer.
#[test]
fn no_broken_append_entries_for_compacted_peer() {
  use crate::{Index, Message, Term};

  let offset = 5u64;
  let (mut ep, log, stable) = make_leader_with_compacted_log(offset, 2);

  // Peer 2 is far behind (next_index < first_index).
  if let Some(p) = ep.tracker.progress_mut(&2u64) {
    p.become_probe();
    p.set_next_index(Index::new(3));
  }

  ep.maybe_send_append(2u64, &log, &stable);

  // Must NOT see any AppendEntries with prev_log_term == ZERO for this peer.
  while let Some(out) = ep.poll_message() {
    if out.to() == 2u64 {
      if let Message::AppendEntries(ae) = out.message() {
        assert_ne!(
          ae.prev_log_term(),
          Term::ZERO,
          "a broken AppendEntries with prev_log_term=ZERO must not be sent to a compacted peer"
        );
      }
    }
  }
}

/// Test 3: after becoming Snapshot-state, peer is paused (no spam).
#[test]
fn snapshot_state_peer_is_paused_no_second_send() {
  use crate::Index;

  let offset = 5u64;
  let (mut ep, log, stable) = make_leader_with_compacted_log(offset, 2);

  // Set peer 2 far behind.
  if let Some(p) = ep.tracker.progress_mut(&2u64) {
    p.become_probe();
    p.set_next_index(Index::new(3));
  }

  // First call: sends the snapshot and transitions peer to Snapshot state.
  ep.maybe_send_append(2u64, &log, &stable);
  while ep.poll_message().is_some() {} // drain

  // Second call: peer is now paused (Snapshot state), must send nothing.
  ep.maybe_send_append(2u64, &log, &stable);
  let msgs: std::vec::Vec<_> = core::iter::from_fn(|| ep.poll_message()).collect();
  assert!(
    msgs.is_empty(),
    "a second maybe_send_append to a Snapshot-state peer must emit nothing (paused)"
  );
}

/// Test 4: a peer at next_index == first_index gets a normal AppendEntries (not a snapshot).
#[test]
fn normal_append_at_boundary_not_snapshot() {
  use crate::{Index, Message};

  let offset = 5u64;
  let (mut ep, log, stable) = make_leader_with_compacted_log(offset, 2);
  // first_index = offset + 1 = 6; set next_index = 6 (the boundary).
  let first = log.first_index();
  assert_eq!(first, Index::new(offset + 1));

  if let Some(p) = ep.tracker.progress_mut(&2u64) {
    p.become_probe();
    p.set_next_index(first); // exactly at boundary
  }

  ep.maybe_send_append(2u64, &log, &stable);

  let msgs: std::vec::Vec<_> = core::iter::from_fn(|| ep.poll_message()).collect();

  // Must NOT send an InstallSnapshot.
  let snap_count = msgs
    .iter()
    .filter(|o| o.to() == 2u64 && matches!(o.message(), Message::InstallSnapshot(_)))
    .count();
  assert_eq!(
    snap_count, 0,
    "must NOT send InstallSnapshot when next_index == first_index"
  );

  // Must send an AppendEntries (normal path — prev_index = offset, boundary term retained).
  let ae_count = msgs
    .iter()
    .filter(|o| o.to() == 2u64 && matches!(o.message(), Message::AppendEntries(_)))
    .count();
  assert_eq!(
    ae_count, 1,
    "must send a normal AppendEntries when next_index == first_index"
  );

  // And the prev_log_term must be the boundary term (Term::new(1)), NOT ZERO.
  for out in &msgs {
    if out.to() == 2u64 {
      if let Message::AppendEntries(ae) = out.message() {
        assert_ne!(
          ae.prev_log_term(),
          crate::Term::ZERO,
          "AppendEntries at the compaction boundary must carry the boundary term, not ZERO"
        );
      }
    }
  }
}

/// A HeartbeatResp from a peer still stuck in Snapshot state (its
/// InstallSnapshot was dropped) must RE-SEND the InstallSnapshot, carrying the same meta.
///
/// FAILS-ON-OLD: without the resend hook the HeartbeatResp produces NO InstallSnapshot
/// (maybe_send_append early-returns on the paused Snapshot peer), so the follower wedges.
#[test]
fn heartbeat_resend_snapshot_to_wedged_follower() {
  use crate::{Index, Instant, Message, Term};

  let offset = 5u64;
  let (mut ep, mut log, mut stable, pending) = wedged_snapshot_follower(offset, 2);
  assert_eq!(pending, Index::new(offset));

  // Peer 2 is still in Snapshot(offset) with match_index = 0 < pending: it has NOT received
  // the snapshot. Deliver a HeartbeatResp (empty context — no ReadIndex involvement).
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    2u64,
    Message::HeartbeatResp(crate::HeartbeatResp::new(
      Term::new(1),
      2u64,
      bytes::Bytes::new(),
    )),
  );

  // A NEW InstallSnapshot to peer 2 must be emitted (the resend), carrying the same meta.
  let msgs: std::vec::Vec<_> = core::iter::from_fn(|| ep.poll_message()).collect();
  let snap_msgs: std::vec::Vec<_> = msgs
    .iter()
    .filter(|o| o.to() == 2u64 && matches!(o.message(), Message::InstallSnapshot(_)))
    .collect();
  assert_eq!(
    snap_msgs.len(),
    1,
    "a HeartbeatResp from a wedged Snapshot-state follower must RE-SEND exactly one InstallSnapshot"
  );
  let resent = match snap_msgs[0].message() {
    Message::InstallSnapshot(s) => s,
    _ => unreachable!(),
  };
  assert_eq!(
    resent.snapshot().last_index(),
    pending,
    "the resent InstallSnapshot must carry the same snapshot meta (last_index = pending)"
  );

  // Peer 2 remains in Snapshot(pending) — the resend does not change progress state.
  let pr = ep.tracker.progress(&2u64).unwrap();
  assert!(pr.state().is_snapshot(), "peer 2 stays in Snapshot state");
  if let crate::ProgressState::Snapshot(p) = pr.state() {
    assert_eq!(
      p, pending,
      "pending snapshot index is unchanged by the resend"
    );
  }

  // BACKOFF: a deferred install legitimately spans many heartbeat intervals, so an immediate
  // second HeartbeatResp must NOT trigger another full-blob resend — the per-peer countdown
  // spaces resends roughly one election timeout apart.
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    2u64,
    Message::HeartbeatResp(crate::HeartbeatResp::new(
      Term::new(1),
      2u64,
      bytes::Bytes::new(),
    )),
  );
  let again: std::vec::Vec<_> = core::iter::from_fn(|| ep.poll_message()).collect();
  let resends: usize = again
    .iter()
    .filter(|o| o.to() == 2u64 && matches!(o.message(), Message::InstallSnapshot(_)))
    .count();
  assert_eq!(
    resends, 0,
    "an immediate second HeartbeatResp must not re-send the blob (resend backoff)"
  );
}

/// The resend STOPS once the follower acks past its pending snapshot index.
/// After a SnapshotResp (match >= pending) the peer leaves Snapshot state (→ Probe), so a
/// subsequent HeartbeatResp must NOT emit another InstallSnapshot (no infinite resend / spam).
#[test]
fn no_snapshot_resend_after_follower_catches_up() {
  use crate::{Instant, Message, Term};

  let offset = 5u64;
  let (mut ep, mut log, mut stable, pending) = wedged_snapshot_follower(offset, 2);

  // First heartbeat round while wedged: resend fires (sanity — same as the test above).
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    2u64,
    Message::HeartbeatResp(crate::HeartbeatResp::new(
      Term::new(1),
      2u64,
      bytes::Bytes::new(),
    )),
  );
  let resent = core::iter::from_fn(|| ep.poll_message())
    .filter(|o| o.to() == 2u64 && matches!(o.message(), Message::InstallSnapshot(_)))
    .count();
  assert_eq!(resent, 1, "resend fires while the follower is still wedged");

  // The follower finally receives a snapshot and acks at pending (SnapshotResp success).
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    2u64,
    Message::SnapshotResp(crate::SnapshotResp::new(Term::new(1), 2u64, false, pending)),
  );
  // It must have left Snapshot state (maybe_update(pending) → Probe).
  assert!(
    !ep.tracker.progress(&2u64).unwrap().state().is_snapshot(),
    "after acking at pending the follower must leave Snapshot state"
  );
  while ep.poll_message().is_some() {} // drain anything the catch-up emitted

  // A subsequent HeartbeatResp must NOT emit another InstallSnapshot (resend has stopped).
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    2u64,
    Message::HeartbeatResp(crate::HeartbeatResp::new(
      Term::new(1),
      2u64,
      bytes::Bytes::new(),
    )),
  );
  let after = core::iter::from_fn(|| ep.poll_message())
    .filter(|o| o.to() == 2u64 && matches!(o.message(), Message::InstallSnapshot(_)))
    .count();
  assert_eq!(
    after, 0,
    "once the follower has caught up, no further InstallSnapshot may be re-sent (no spam)"
  );
}

/// Test 1: a behind follower installs the snapshot and acks correctly.
#[test]
fn install_snapshot_on_behind_follower() {
  use crate::{Index, Instant, Message, Term, conf::ConfState};

  let (mut ep, mut log, mut stable) = make_follower();

  // Build a snapshot: SM state = 42 (CountSm::count = 42), last_index=10, last_term=4.
  let snap_value: u64 = 42;
  let snap_data = encode_snapshot(snap_value);
  let meta = crate::SnapshotMeta::new(
    Index::new(10),
    Term::new(4),
    ConfState::from_voters(std::vec![1u64, 2u64, 3u64]),
  );
  let is = crate::InstallSnapshot::new(Term::new(1), 1u64, meta.clone(), snap_data.clone());

  // Follower commit starts at 0 (< 10) → install path.
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::InstallSnapshot(is),
  );
  // the install adopts term 1 (follower started at term 0), so the post-install SnapshotResp is
  // deferred until that term write is durable. Drain storage (as the driver does each iteration) to
  // complete the term write and release the ack.
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);

  // SM must be restored to the snapshot state.
  assert_eq!(
    ep.state_machine().count() as u64,
    snap_value,
    "state machine must be restored to the snapshot value"
  );

  // commit and applied must both equal last_index.
  assert_eq!(
    ep.commit,
    Index::new(10),
    "commit must equal meta.last_index()"
  );
  assert_eq!(
    ep.applied,
    Index::new(10),
    "applied must equal meta.last_index()"
  );

  // Log must be re-baselined: first_index == 11, term(10) == 4.
  assert_eq!(
    log.first_index(),
    Index::new(11),
    "first_index must be last_index + 1"
  );
  assert_eq!(
    log.last_index(),
    Index::new(10),
    "last_index must equal meta.last_index()"
  );
  assert_eq!(
    log.term(Index::new(10)).unwrap(),
    Term::new(4),
    "term(last_index) must equal last_term after restore"
  );
  // No entries exist above last_index.
  assert!(
    log
      .entries(Index::new(11)..Index::new(11), u64::MAX)
      .unwrap()
      .is_empty(),
    "entries(11..11) must be empty after restore"
  );

  // Exactly one SnapshotInstalled event must be emitted.
  let events: std::vec::Vec<_> = core::iter::from_fn(|| ep.poll_event()).collect();
  let installed: std::vec::Vec<_> = events
    .iter()
    .filter(|e| e.is_snapshot_installed())
    .collect();
  assert_eq!(
    installed.len(),
    1,
    "exactly one SnapshotInstalled event must be emitted"
  );
  assert_eq!(
    installed[0].unwrap_snapshot_installed_ref().last_index(),
    Index::new(10)
  );

  // Exactly one SnapshotResp must be sent to the leader (node 1) with reject=false,
  // match_index=10.
  let msgs: std::vec::Vec<_> = core::iter::from_fn(|| ep.poll_message()).collect();
  let snap_resps: std::vec::Vec<_> = msgs
    .iter()
    .filter(|o| o.to() == 1u64 && matches!(o.message(), Message::SnapshotResp(_)))
    .collect();
  assert_eq!(
    snap_resps.len(),
    1,
    "exactly one SnapshotResp must be sent to the leader"
  );
  let sr = match snap_resps[0].message() {
    Message::SnapshotResp(r) => r,
    _ => unreachable!(),
  };
  assert!(
    !sr.reject(),
    "SnapshotResp must not be a rejection on successful install"
  );
  assert_eq!(
    sr.match_index(),
    Index::new(10),
    "match_index must equal meta.last_index()"
  );

  // stable must have a snapshot persisted (submit_snapshot was called).
  assert!(
    stable.snapshot().is_some(),
    "stable store must hold the persisted snapshot after install"
  );

  // Election timer must be re-armed (poll_timeout is Some).
  assert!(
    ep.poll_timeout().is_some(),
    "election timer must be re-armed after receiving a snapshot"
  );
}

/// A snapshot install must scrub stale outgoing success acks.
///
/// A follower has a queued success `AppendResp(match_index = 3)` still in `outgoing` (it acked
/// index 3, but the ack has not yet been polled). It then installs a snapshot at a LOWER boundary
/// (`last_index = 2`). The truncated entry 3 no longer exists, so emitting that ack would over-ack
/// an entry the follower no longer stores — letting the leader count a phantom replica toward
/// commit. After the install, no success `AppendResp` with `match_index > 2` may be emitted.
#[test]
fn install_snapshot_scrubs_stale_outgoing_ack() {
  use crate::{Index, Instant, Message, Outgoing, Term, conf::ConfState};

  let (mut ep, mut log, mut stable) = make_follower();

  // Queue a success AppendResp(match_index = 3) as if the follower had acked index 3 and the ack
  // is still sitting in `outgoing` (not yet polled). This is the stale ack that must be scrubbed.
  ep.outgoing.push_back(Outgoing::new(
    1u64,
    Message::AppendResp(crate::AppendResp::new(
      Term::new(1),
      2u64,
      false,
      Index::ZERO,
      Term::ZERO,
      Index::new(3),
    )),
  ));

  // Install a snapshot at a LOWER boundary (last_index = 2 > commit = 0 → install proceeds).
  let snap_data = encode_snapshot(7u64);
  let meta = crate::SnapshotMeta::new(
    Index::new(2),
    Term::new(4),
    ConfState::from_voters(std::vec![1u64, 2u64, 3u64]),
  );
  let is = crate::InstallSnapshot::new(Term::new(1), 1u64, meta, snap_data);
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::InstallSnapshot(is),
  );
  // the install (and its ack-scrub) is DEFERRED — drive handle_storage so SnapshotWritten fires
  // `install_snapshot_now`, which runs `scrub_acks_above(2)`.
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);

  // Drain all outgoing messages: NONE may be a success AppendResp with match_index > 2.
  let msgs: std::vec::Vec<_> = core::iter::from_fn(|| ep.poll_message()).collect();
  let over_ack = msgs.iter().any(|o| {
      matches!(o.message(), Message::AppendResp(a) if !a.reject() && a.match_index() > Index::new(2))
    });
  assert!(
    !over_ack,
    "the stale success AppendResp(match_index = 3) must be scrubbed by the snapshot install"
  );
}

/// Test 2: a stale snapshot (last_index <= commit) is a no-op ack, SM not touched.
#[test]
fn stale_snapshot_does_not_install() {
  use crate::{Entry, EntryKind, Index, Instant, Message, Term, conf::ConfState};

  let (mut ep, mut log, mut stable) = make_follower();

  // Seed the follower log with 15 entries so commit can be set to 15.
  let entries: std::vec::Vec<_> = (1u64..=15)
    .map(|i| {
      Entry::new(
        Term::new(1),
        Index::new(i),
        EntryKind::Normal,
        bytes::Bytes::from_static(b"x"),
      )
    })
    .collect();
  log.force_append(&entries);
  // Manually advance commit to 15 (the follower has committed up to 15). `force_append` writes the
  // durable VecLog directly (bypassing submit_append), so also advance `durable_index` to keep the
  // state self-consistent: a follower whose log is durable to 15 has durable_index == 15. Without
  // this the stale-snapshot ack would (correctly) clamp to durable_commit() = min(15, 0) = 0.
  ep.commit = Index::new(15);
  ep.applied = Index::new(15);
  ep.durable_index = Index::new(15);
  // SM count is arbitrary (doesn't matter — must not change).
  let sm_count_before = ep.state_machine().count();

  // Try to install a snapshot with last_index=10 (< commit=15): stale.
  let snap_data = encode_snapshot(7u64);
  let meta = crate::SnapshotMeta::new(
    Index::new(10),
    Term::new(4),
    ConfState::from_voters(std::vec![1u64, 2u64, 3u64]),
  );
  let is = crate::InstallSnapshot::new(Term::new(1), 1u64, meta, snap_data);
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::InstallSnapshot(is),
  );
  // the install adopts term 1 (the follower starts at term 0), so the stale-snapshot success ack
  // is deferred until that term write is durable. Drain storage (the driver does this every iteration)
  // to complete the term write and release the deferred SnapshotResp.
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);

  // SM must NOT have been restored.
  assert_eq!(
    ep.state_machine().count(),
    sm_count_before,
    "SM must not be restored for a stale snapshot"
  );
  // commit must be unchanged.
  assert_eq!(
    ep.commit,
    Index::new(15),
    "commit must not regress for a stale snapshot"
  );

  // No SnapshotInstalled event.
  let events: std::vec::Vec<_> = core::iter::from_fn(|| ep.poll_event())
    .filter(|e| e.is_snapshot_installed())
    .collect();
  assert!(
    events.is_empty(),
    "no SnapshotInstalled event for a stale snapshot"
  );

  // Must still send a SnapshotResp with reject=false and match_index = self.commit.
  let msgs: std::vec::Vec<_> = core::iter::from_fn(|| ep.poll_message()).collect();
  let snap_resps: std::vec::Vec<_> = msgs
    .iter()
    .filter(|o| o.to() == 1u64 && matches!(o.message(), Message::SnapshotResp(_)))
    .collect();
  assert_eq!(
    snap_resps.len(),
    1,
    "stale snapshot must still send a SnapshotResp"
  );
  let sr = match snap_resps[0].message() {
    Message::SnapshotResp(r) => r,
    _ => unreachable!(),
  };
  assert!(!sr.reject(), "stale snapshot ack must have reject=false");
  assert_eq!(
    sr.match_index(),
    Index::new(15),
    "match_index must be self.commit (so leader leaves Snapshot state)"
  );
}

/// Test 3: malformed snapshot data poisons the node; no partial state is applied.
#[test]
fn malformed_snapshot_data_poisons_node() {
  use crate::{Index, Instant, Message, Term, conf::ConfState};

  let (mut ep, mut log, mut stable) = make_follower();

  // Bad data: too short to decode a u64 (only 3 bytes).
  let bad_data = bytes::Bytes::from_static(b"bad");
  let meta = crate::SnapshotMeta::new(
    Index::new(10),
    Term::new(4),
    ConfState::from_voters(std::vec![1u64, 2u64, 3u64]),
  );
  let is = crate::InstallSnapshot::new(Term::new(1), 1u64, meta, bad_data);
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::InstallSnapshot(is),
  );

  // Node must be poisoned.
  assert!(
    ep.is_poisoned(),
    "node must be poisoned after a malformed snapshot"
  );

  // commit and applied must NOT have been touched (no partial state).
  assert_eq!(
    ep.commit,
    Index::ZERO,
    "commit must not be modified on decode failure"
  );
  assert_eq!(
    ep.applied,
    Index::ZERO,
    "applied must not be modified on decode failure"
  );

  // All subsequent handle_message calls are no-ops.
  let good_data = encode_snapshot(1u64);
  let meta2 = crate::SnapshotMeta::new(
    Index::new(10),
    Term::new(4),
    ConfState::from_voters(std::vec![1u64, 2u64, 3u64]),
  );
  let is2 = crate::InstallSnapshot::new(Term::new(1), 1u64, meta2, good_data);
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::InstallSnapshot(is2),
  );
  // commit still zero — poisoned node ignores everything.
  assert_eq!(
    ep.commit,
    Index::ZERO,
    "poisoned node must ignore subsequent messages"
  );
  // No messages or events emitted.
  assert!(
    ep.poll_message().is_none(),
    "poisoned node must not emit messages"
  );
}

/// Test 4: leader processes a successful SnapshotResp — peer leaves Snapshot state.
#[test]
fn leader_processes_snapshot_resp_success_and_reject() {
  use crate::{Index, Instant, Message, Term, VoteResp};
  use core::time::Duration;

  // Build a 3-voter leader (node 1).
  let cfg = crate::Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::AsyncStable::default();

  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable);
  ep.handle_storage(d, &mut log, &mut stable);
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResp(VoteResp::new(Term::new(1), 2u64, false, false)),
  );
  assert!(ep.role().is_leader());
  // Extend the leader's log to index 10 (no-op at 1 + 9 proposals) so a peer's snapshot ack at 10
  // is consistent with the leader's own last_index — a leader never snapshots beyond its log, so
  // the success-ack boundary check (`match_within_log`) requires last_index >= the acked index.
  for _ in 0..9 {
    ep.propose(d, &mut log, &stable, &bytes::Bytes::from_static(b"x"))
      .unwrap();
  }
  ep.handle_storage(d, &mut log, &mut stable);
  assert_eq!(log.last_index(), Index::new(10));
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  // Manually put peer 2 into Snapshot(10) state.
  if let Some(p) = ep.tracker.progress_mut(&2u64) {
    p.become_snapshot(Index::new(10));
  }
  assert!(ep.tracker.progress(&2u64).unwrap().state().is_snapshot());

  // --- Reject case: become_probe, then maybe_send_append re-enters probe ---
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::SnapshotResp(crate::SnapshotResp::new(
      Term::new(1),
      2u64,
      true, // reject
      Index::new(10),
    )),
  );
  // After reject the peer must have transitioned to Probe.
  assert!(
    ep.tracker.progress(&2u64).unwrap().state().is_probe(),
    "reject SnapshotResp must transition peer to Probe"
  );

  // --- Success case: peer has been put back in Snapshot(10). ---
  if let Some(p) = ep.tracker.progress_mut(&2u64) {
    p.become_snapshot(Index::new(10));
  }
  // Drain any messages from the probe that was triggered by the reject.
  while ep.poll_message().is_some() {}

  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::SnapshotResp(crate::SnapshotResp::new(
      Term::new(1),
      2u64,
      false, // success
      Index::new(10),
    )),
  );
  // maybe_update(10) >= pending_snapshot(10) → Probe; match_index == 10.
  let pr = ep.tracker.progress(&2u64).unwrap();
  assert!(
    pr.state().is_probe(),
    "success SnapshotResp must transition peer out of Snapshot state"
  );
  assert_eq!(
    pr.match_index(),
    Index::new(10),
    "match_index must be 10 after successful SnapshotResp"
  );
}

/// Regression (`on_install_snapshot` validates the snapshot `ConfState`): a
/// sender-authentic but malformed snapshot whose membership violates the core invariants (here a
/// learner that is also a voter) must poison the follower BEFORE any state mutation, not install an
/// impossible configuration into the tracker. `Tracker::from_conf_state` copies the sets verbatim,
/// so the boundary check is the only thing standing between malformed input and a corrupt
/// membership (no quorum, vacuous votes).
///
/// MUTATION: drop the Step-0 `meta.conf().is_valid()` gate in `on_install_snapshot` → the follower
/// installs the impossible config and is not poisoned.
#[test]
fn install_snapshot_with_invalid_conf_state_poisons() {
  use crate::{Config, Index, Instant, Message, Term, conf::ConfState};
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::NoopStable::default();

  // Malformed membership: node 1 is BOTH a voter and a learner. last_index=5 > commit=0 passes the
  // staleness guard and reaches the Step-0 membership validation.
  let bad_meta = crate::SnapshotMeta::new(
    Index::new(5),
    Term::new(2),
    ConfState::new(
      std::vec![1u64, 2u64],
      std::vec![1u64],
      std::vec![],
      std::vec![],
      false,
    ),
  );
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    2u64,
    Message::InstallSnapshot(crate::InstallSnapshot::new(
      Term::new(2),
      2u64,
      bad_meta,
      bytes::Bytes::from_static(b"anything"),
    )),
  );

  assert!(
    ep.is_poisoned(),
    "an invalid snapshot ConfState must poison the follower"
  );
  assert_eq!(
    ep.poison_reason().map(|r| r.as_str()),
    Some("invalid_conf_state")
  );
  // No partial install: neither commit nor the state machine advanced.
  assert_eq!(
    ep.commit_index(),
    Index::ZERO,
    "commit must not advance on a rejected snapshot"
  );
  assert_eq!(
    ep.state_machine().count(),
    0,
    "no SM restore on a rejected snapshot"
  );
}

/// Regression (`ConfState::is_valid` rejects a `learners_next` ∩ incoming-voter overlap):
/// a malformed JOINT snapshot where a node is BOTH an incoming voter and staged for demotion
/// (`learners_next`) is impossible from a correct `Changer` (which removes a node from the incoming
/// half before staging it). Installed verbatim, `leave_joint` would later make that node a
/// simultaneous voter+learner and poison `ConfChangeApply` AFTER the snapshot was already restored.
/// The install gate must reject it up front via the tightened validator.
///
/// MUTATION: drop the `|| self.voters.contains(id)` clause added to `ConfState::is_valid`'s
/// `learners_next` loop → the overlap is accepted and the follower installs it instead of poisoning.
#[test]
fn install_snapshot_with_learners_next_voter_overlap_poisons() {
  use crate::{Config, Index, Instant, Message, Term, conf::ConfState};
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::NoopStable::default();

  // Joint config where node 3 is in the incoming voters AND staged in learners_next — impossible.
  let bad_meta = crate::SnapshotMeta::new(
    Index::new(5),
    Term::new(2),
    ConfState::new(
      std::vec![1u64, 2u64, 3u64],
      std::vec![],
      std::vec![1u64, 2u64, 3u64],
      std::vec![3u64],
      true,
    ),
  );
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    2u64,
    Message::InstallSnapshot(crate::InstallSnapshot::new(
      Term::new(2),
      2u64,
      bad_meta,
      bytes::Bytes::from_static(b"anything"),
    )),
  );

  assert!(
    ep.is_poisoned(),
    "a learners_next/incoming-voter overlap must poison the follower"
  );
  assert_eq!(
    ep.poison_reason().map(|r| r.as_str()),
    Some("invalid_conf_state")
  );
  assert_eq!(ep.commit_index(), Index::ZERO, "no commit advance");
}

/// Regression (the install-commit invariant, now preserved BY the deferral): a follower install must never
/// persist `HardState.commit` above durable storage. Under the deferral the install is DEFERRED —
/// `on_install_snapshot` only submits the blob and arms `pending_install`, leaving
/// `commit`/`applied`/`durable_index` at their OLD values; `install_snapshot_now` advances them to the
/// boundary ONLY once the blob is durable. So a follower at committed_persisted=3 installing a snapshot
/// at index 10 keeps `commit`/`durable_commit()` at 3 while the blob is in flight, then advances to 10
/// once `SnapshotWritten` fires — the commit is never persistable above the durable log by construction.
///
/// MUTATION: run the install body eagerly in `on_install_snapshot` (advance `commit` before the blob is
/// durable) → `commit`/`durable_commit()` report 10 while the blob is still in flight.
#[test]
fn install_defers_commit_advance_until_blob_durable() {
  use crate::{
    AppendEntries, Config, Entry, EntryKind, Index, InstallSnapshot, Instant, Message,
    SnapshotMeta, Term, conf::ConfState,
  };
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::AsyncStable::default();
  let d = Instant::ORIGIN;

  // Follower at term 2 with a durable log [1..=3], commit=3, committed_persisted=3.
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(2),
      2u64,
      Index::ZERO,
      Term::ZERO,
      std::vec![
        Entry::new(
          Term::new(2),
          Index::new(1),
          EntryKind::Empty,
          bytes::Bytes::new()
        ),
        Entry::new(
          Term::new(2),
          Index::new(2),
          EntryKind::Empty,
          bytes::Bytes::new()
        ),
        Entry::new(
          Term::new(2),
          Index::new(3),
          EntryKind::Empty,
          bytes::Bytes::new()
        ),
      ],
      Index::new(3),
    )),
  );
  ep.handle_storage(d, &mut log, &mut stable);
  assert_eq!(ep.committed_persisted, Index::new(3));

  // Install a snapshot at index 10 — commit/durable_index jump to 10, but the blob is DEFERRED
  // (AsyncStable), so SnapshotWritten has not fired yet.
  let meta = SnapshotMeta::new(
    Index::new(10),
    Term::new(2),
    ConfState::from_voters(std::vec![1u64, 2u64, 3u64]),
  );
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::InstallSnapshot(InstallSnapshot::new(
      Term::new(2),
      2u64,
      meta,
      encode_count_snapshot(10),
    )),
  );
  // the install is DEFERRED — commit/applied/durable_index stay at their OLD values; only the blob
  // is submitted and `pending_install` is armed. A crash here loses just the in-flight blob.
  assert!(
    ep.pending_install.is_some(),
    "install deferred → pending_install armed"
  );
  assert_eq!(
    ep.commit,
    Index::new(3),
    "commit NOT advanced until the blob is durable"
  );
  assert_eq!(
    ep.durable_index,
    Index::new(3),
    "durable_index NOT advanced until the blob is durable"
  );
  assert_eq!(
    ep.durable_commit(),
    Index::new(3),
    "durable_commit stays at the pre-install durable commit while the blob is in flight"
  );

  // Make the blob durable (SnapshotWritten fires) → `install_snapshot_now` runs: commit/durable_index
  // advance to the boundary together, with the blob already durable.
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(
    ep.pending_install.is_none(),
    "blob durable → deferred install completed"
  );
  assert_eq!(
    ep.commit,
    Index::new(10),
    "commit advances to the boundary once the blob is durable"
  );
  assert_eq!(ep.durable_index, Index::new(10));
  assert_eq!(
    ep.durable_commit(),
    Index::new(10),
    "durable_commit lifts to the snapshot boundary once the blob is durable"
  );
}

/// Regression: persist-before-ack on the STALE-snapshot reply path. The staleness guard in
/// `on_install_snapshot` (last_index <= commit) acks so the leader can transition the peer out of
/// Snapshot state, but it must report `durable_commit()` (the recoverable watermark), NOT raw
/// `self.commit`. An async follower can have `commit > durable_index` — commit advanced over a
/// visible-but-not-yet-durable append — and replying raw commit would over-ack a tail this node
/// cannot recover after a crash, letting the leader count a phantom replica (the same persist-before-
/// ack hole the immediate `AppendResp` clamp closes on the AppendEntries path).
///
/// Setup: durable log [1..=3] (commit/durable 3); a second AppendEntries [4..=5] with leader_commit=5
/// advances commit to 5 but `durable_index` stays 3 (the 4/5 `Appended` is NOT drained). A stale
/// InstallSnapshot(last_index=5) then hits the guard; the reply must report 3 = min(commit 5,
/// durable_index 3), not 5.
///
/// MUTATION: revert the stale-guard ack to `self.commit` → the `SnapshotResp` reports 5, over-acking
/// the non-durable tail [4..=5].
#[test]
fn stale_snapshot_resp_is_clamped_to_durable_watermark() {
  use crate::{
    AppendEntries, Config, Entry, EntryKind, Index, InstallSnapshot, Instant, Message,
    SnapshotMeta, Term, conf::ConfState,
  };
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::AsyncStable::default();
  let d = Instant::ORIGIN;

  // Durable log [1..=3], commit=3, durable_index=3.
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(2),
      2u64,
      Index::ZERO,
      Term::ZERO,
      std::vec![
        Entry::new(
          Term::new(2),
          Index::new(1),
          EntryKind::Empty,
          bytes::Bytes::new()
        ),
        Entry::new(
          Term::new(2),
          Index::new(2),
          EntryKind::Empty,
          bytes::Bytes::new()
        ),
        Entry::new(
          Term::new(2),
          Index::new(3),
          EntryKind::Empty,
          bytes::Bytes::new()
        ),
      ],
      Index::new(3),
    )),
  );
  ep.handle_storage(d, &mut log, &mut stable);
  assert_eq!(ep.durable_index, Index::new(3));
  assert_eq!(ep.commit, Index::new(3));

  // Second AppendEntries [4..=5] with leader_commit=5: commit jumps to 5, but the 4/5 append is NOT
  // yet durable (no handle_storage), so durable_index stays 3 → commit > durable_index.
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(2),
      2u64,
      Index::new(3),
      Term::new(2),
      std::vec![
        Entry::new(
          Term::new(2),
          Index::new(4),
          EntryKind::Empty,
          bytes::Bytes::new()
        ),
        Entry::new(
          Term::new(2),
          Index::new(5),
          EntryKind::Empty,
          bytes::Bytes::new()
        ),
      ],
      Index::new(5),
    )),
  );
  assert_eq!(
    ep.commit,
    Index::new(5),
    "commit advanced to the leader_commit"
  );
  assert_eq!(
    ep.durable_index,
    Index::new(3),
    "but the 4/5 append is not yet durable"
  );
  // Drain the outbox (the immediate AppendResp for the second append) so the next poll is the
  // SnapshotResp under test.
  while ep.poll_message().is_some() {}

  // A stale InstallSnapshot at index 3 (== durable_index, already within ack_watermark) hits the
  // staleness short-circuit — it is redundant, so it acks the clamp immediately (no deferred install).
  let meta = SnapshotMeta::new(
    Index::new(3),
    Term::new(2),
    ConfState::from_voters(std::vec![1u64, 2u64, 3u64]),
  );
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::InstallSnapshot(InstallSnapshot::new(
      Term::new(2),
      2u64,
      meta,
      encode_count_snapshot(3),
    )),
  );

  let resp = ep
    .poll_message()
    .expect("a stale InstallSnapshot emits a SnapshotResp");
  match resp.message() {
    Message::SnapshotResp(s) => {
      assert!(
        !s.reject(),
        "the follower is at/ahead → success ack, not a reject"
      );
      assert_eq!(
        s.match_index(),
        Index::new(3),
        "persist-before-ack: the stale-snapshot ack must report the durable watermark \
           min(commit=5, durable_index=3)=3, not the raw in-memory commit 5"
      );
    }
    other => panic!("expected SnapshotResp, got {other:?}"),
  }
  // The stale path installs nothing: commit must not regress and no deferred install is armed.
  assert_eq!(
    ep.commit,
    Index::new(5),
    "a stale snapshot must not regress commit"
  );
  assert!(
    ep.pending_install.is_none(),
    "the stale path submits no blob → no deferred install armed"
  );
}

/// THE canonical deferred-install regression: a crash in the snapshot-install fsync window (blob submitted, not yet
/// durable) must NOT orphan the log. Under the deferred install the destructive `log.restore` runs only
/// in `install_snapshot_now`, gated on the blob being durable — so a crash before `SnapshotWritten`
/// leaves the durable log UNCHANGED and restart re-syncs, never `OrphanedLog`.
///
/// MUTATION: run the install body eagerly in `on_install_snapshot` (the eager-install behavior) → `log` is
/// re-baselined to 10 here (the `log.last_index()==3` assertion fails) and the restart below poisons
/// `orphaned_log`.
#[test]
fn install_crash_in_window_does_not_orphan_log() {
  use crate::{Index, Instant};
  let (mut ep, mut log, mut stable, cfg) = follower_committed_to_3();
  let d = Instant::ORIGIN;
  assert_eq!(log.last_index(), Index::new(3));

  // Receive InstallSnapshot at boundary 10 — DEFERRED: blob submitted (visible, not yet durable). Do
  // NOT drive `handle_storage`, so `install_snapshot_now` never runs and `log.restore` is not called.
  ep.handle_message(d, &mut log, &mut stable, 1u64, install_at(10));
  assert!(
    ep.pending_install.is_some(),
    "install deferred → pending_install armed"
  );
  assert_eq!(
    log.last_index(),
    Index::new(3),
    "the durable log is UNCHANGED while the blob is in flight (no eager re-baseline)"
  );

  // CRASH before SnapshotWritten: the in-flight blob is lost; the durable log `[1..=3]` survives.
  stable.discard_inflight();
  assert!(
    stable.durable_snapshot().is_none(),
    "no durable snapshot survives the crash"
  );

  // RESTART: reconcile_restart_log sees the pre-install shape (no snapshot, first_index==1) → recover,
  // NOT orphan. The follower re-syncs from the leader.
  let ep2 = Endpoint::restart(
    cfg,
    d,
    1,
    crate::testkit::CountSm::default(),
    2,
    &mut log,
    &mut stable,
  );
  assert!(
    !ep2.is_poisoned(),
    "a crash in the install window must NOT orphan the log"
  );
  assert!(ep2.poison_reason().is_none());
  assert_eq!(
    log.last_index(),
    Index::new(3),
    "durable log intact → normal re-sync"
  );
}

/// The missed-completion fallback fires ONLY on DURABLE evidence (`durable_snapshot()`), never the
/// submit-visible slot — else a torn fsync (blob visible but not durable) would re-baseline the log
/// ahead of a non-durable blob, recreating the orphaned-log bug.
///
/// MUTATION: key the `pending_install` fallback on `stable.snapshot()` (visible) instead of
/// `durable_snapshot()` → the install fires on the torn blob here (`pending_install` becomes None and
/// the log re-baselines), so the `is_some()`/`last_index()==3` assertions fail.
#[test]
fn install_fallback_requires_durable_evidence_not_visible_blob() {
  use crate::{Index, Instant};
  let (mut ep, mut log, mut stable, cfg) = follower_committed_to_3();
  let d = Instant::ORIGIN;
  // Torn fsync: the next snapshot blob is VISIBLE but NOT durable and enqueues no completion.
  stable.fail_next_snapshot_durability();
  ep.handle_message(d, &mut log, &mut stable, 1u64, install_at(10));
  assert!(ep.pending_install.is_some());

  // Drain storage: NO SnapshotWritten arrives (torn); the fallback sees durable_snapshot()==None and
  // must NOT fire the destructive install.
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(
    ep.pending_install.is_some(),
    "the fallback must NOT fire on a visible-but-not-durable blob"
  );
  assert_eq!(
    log.last_index(),
    Index::new(3),
    "no re-baseline while the blob is not durable"
  );
  assert!(stable.durable_snapshot().is_none());

  // Crash + restart: still no orphan (the log was never re-baselined).
  stable.discard_inflight();
  let ep2 = Endpoint::restart(
    cfg,
    d,
    1,
    crate::testkit::CountSm::default(),
    2,
    &mut log,
    &mut stable,
  );
  assert!(
    !ep2.is_poisoned(),
    "a torn snapshot blob + crash must NOT orphan the log"
  );
}

/// The fallback DOES complete the install when the blob is durable but its `SnapshotWritten` completion
/// was dropped/coalesced — `durable_snapshot()` reveals the durable blob, so a missed completion never
/// wedges `pending_install` forever.
///
/// MUTATION: delete the `pending_install` durable-evidence fallback in `handle_storage` → the install
/// stays wedged (`pending_install` never clears, commit never advances), so the assertions fail.
#[test]
fn install_completes_via_durable_fallback_on_missed_completion() {
  use crate::{Index, Instant};
  let (mut ep, mut log, mut stable, _cfg) = follower_committed_to_3();
  let d = Instant::ORIGIN;
  // The blob is fsync'd DURABLE but its SnapshotWritten completion is dropped (coalesced).
  stable.drop_next_snapshot_completion();
  ep.handle_message(d, &mut log, &mut stable, 1u64, install_at(10));
  assert!(ep.pending_install.is_some());
  assert!(
    stable.durable_snapshot().is_some(),
    "blob durable despite the dropped completion"
  );

  // Drain storage: no SnapshotWritten arrives, but the durable_snapshot() fallback completes the install.
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(
    ep.pending_install.is_none(),
    "the durable-evidence fallback completed the install despite the missed completion"
  );
  assert_eq!(ep.commit, Index::new(10));
  assert_eq!(
    log.last_index(),
    Index::new(10),
    "log re-baselined to the boundary"
  );
}

/// Vote-freshness floor: while an install is deferred, the follower must advertise freshness AT LEAST
/// the (already-quorum-committed) snapshot boundary — else it could grant a vote to a candidate whose
/// log is below the committed snapshot prefix (a Leader-Completeness violation).
///
/// MUTATION: drop the `pending_install` floor in `on_request_vote` → freshness is read from the OLD log
/// `[1..=3]`, so the candidate at index 5 looks up-to-date and the vote is GRANTED (assert fails).
#[test]
fn vote_freshness_floored_at_pending_install_boundary() {
  use crate::{Index, Instant, Message, RequestVote, Term};
  let (mut ep, mut log, mut stable, _cfg) = follower_committed_to_3();
  let d = Instant::ORIGIN;
  // Deferred install at boundary 10 (term 2) — pending_install armed, NOT yet completed.
  ep.handle_message(d, &mut log, &mut stable, 1u64, install_at(10));
  assert!(ep.pending_install.is_some());
  while ep.poll_message().is_some() {}

  // A candidate at a HIGHER term (so the follower steps down → lease open) whose log (5/2) is BELOW the
  // committed snapshot boundary (10/2). The freshness floor must REJECT it.
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    3u64,
    Message::RequestVote(RequestVote::new(
      Term::new(3),
      3u64,
      Index::new(5),
      Term::new(2),
      false,
      false,
    )),
  );
  let granted_reject = core::iter::from_fn(|| ep.poll_message()).find_map(|o| match o.message() {
    Message::VoteResp(v) => Some(v.reject()),
    _ => None,
  });
  assert_eq!(
    granted_reject,
    Some(true),
    "the freshness floor at the pending-install boundary must REJECT a candidate below it"
  );
}

/// Duplicate-install guard: a resent (same-boundary) InstallSnapshot while one is already deferred must
/// be a no-op — NOT mint a second blob op (which would orphan the first in-flight blob).
///
/// MUTATION: drop the duplicate guard in `on_install_snapshot` → the second install re-mints a blob op
/// (next_op_id advances) and replaces pending_install, orphaning the first blob (assert fails).
#[test]
fn duplicate_install_in_window_mints_no_second_blob() {
  use crate::{Index, Instant};
  let (mut ep, mut log, mut stable, _cfg) = follower_committed_to_3();
  let d = Instant::ORIGIN;
  ep.handle_message(d, &mut log, &mut stable, 1u64, install_at(10));
  let op_after_first = ep.next_op_id;

  // A DUPLICATE install at the same boundary while pending: a no-op (no second blob op minted).
  ep.handle_message(d, &mut log, &mut stable, 1u64, install_at(10));
  assert_eq!(
    ep.next_op_id, op_after_first,
    "a duplicate install must NOT mint a second blob op"
  );
  assert!(ep.pending_install.is_some());

  // The original install still completes on SnapshotWritten.
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(ep.pending_install.is_none());
  assert_eq!(ep.commit, Index::new(10));
  assert_eq!(log.last_index(), Index::new(10));
}

/// Completion-time staleness re-check: if in-window AppendEntries catch the follower up PAST the boundary
/// while the blob is in flight, the deferred install is DROPPED rather than regressing commit/log.
///
/// MUTATION: drop the `meta.last_index() <= self.commit` re-check in `install_snapshot_now` → the install
/// re-baselines to boundary 4, REGRESSING commit 5→4 and discarding entry 5 (asserts fail).
#[test]
fn completion_time_staleness_drops_superseded_install() {
  use crate::{AppendEntries, Entry, EntryKind, Index, Instant, Message, Term};
  let (mut ep, mut log, mut stable, _cfg) = follower_committed_to_3();
  let d = Instant::ORIGIN;
  // Deferred install at boundary 4.
  ep.handle_message(d, &mut log, &mut stable, 1u64, install_at(4));
  assert!(ep.pending_install.is_some());

  // In-window AppendEntries catch the follower up to commit=5 (PAST the boundary 4) before the blob is
  // durable — appended to the OLD log, which stays live throughout the deferral.
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    1u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(2),
      1u64,
      Index::new(3),
      Term::new(2),
      std::vec![
        Entry::new(
          Term::new(2),
          Index::new(4),
          EntryKind::Empty,
          bytes::Bytes::new()
        ),
        Entry::new(
          Term::new(2),
          Index::new(5),
          EntryKind::Empty,
          bytes::Bytes::new()
        ),
      ],
      Index::new(5),
    )),
  );

  // SnapshotWritten fires → completion-staleness re-check: boundary 4 <= commit 5 → DROP the install,
  // never regressing commit/log.
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(
    ep.pending_install.is_none(),
    "the superseded install is dropped at completion"
  );
  assert_eq!(
    ep.commit,
    Index::new(5),
    "commit must NOT regress to the boundary"
  );
  assert_eq!(
    log.last_index(),
    Index::new(5),
    "the log followed the appends, not a stale snapshot re-baseline"
  );
}

/// When a deferred install is DROPPED as stale because in-window appends advanced `commit` past the
/// boundary over a NOT-YET-FLUSHED tail (so `durable_index < boundary`), the follower must still be able
/// to honestly ack the durable snapshot boundary — `ack_watermark()` = max(durable_index, durable
/// snapshot boundary) — so the leader is not pinned in `ProgressState::Snapshot`. A crash would
/// `reconcile_restart_log::Restore` to that snapshot, so acking it is honest; the boundary is already
/// quorum-committed, so it is phantom-safe.
///
/// MUTATION: revert `ack_watermark()` to `self.durable_index` (drop the durable-snapshot max) → the
/// watermark reports 3 not 5, and the leader would stay pinned in Snapshot until the tail flushes.
#[test]
fn stale_drop_acks_durable_snapshot_boundary() {
  use crate::{AppendEntries, Entry, EntryKind, Index, Instant, Message, Term};
  let (mut ep, mut log, mut stable, _cfg) = follower_committed_to_3();
  let d = Instant::ORIGIN;
  // Deferred install at boundary 5 (blob in flight; commit still 3, so not stale at receipt).
  ep.handle_message(d, &mut log, &mut stable, 1u64, install_at(5));
  assert!(ep.pending_install.is_some());

  // In-window appends [4..=7] (leader_commit=7) advance commit to 7 — but the log HOLDS their `Appended`
  // completions (deferred fsync), so `durable_index` stays 3: a visible-but-unflushed tail.
  log.hold_appends(true);
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    1u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(2),
      1u64,
      Index::new(3),
      Term::new(2),
      std::vec![
        Entry::new(
          Term::new(2),
          Index::new(4),
          EntryKind::Empty,
          bytes::Bytes::new()
        ),
        Entry::new(
          Term::new(2),
          Index::new(5),
          EntryKind::Empty,
          bytes::Bytes::new()
        ),
        Entry::new(
          Term::new(2),
          Index::new(6),
          EntryKind::Empty,
          bytes::Bytes::new()
        ),
        Entry::new(
          Term::new(2),
          Index::new(7),
          EntryKind::Empty,
          bytes::Bytes::new()
        ),
      ],
      Index::new(7),
    )),
  );
  assert_eq!(ep.commit, Index::new(7));
  assert_eq!(
    ep.durable_index,
    Index::new(3),
    "the appended tail is visible but UNFLUSHED"
  );

  // handle_storage drains SnapshotWritten(5) (the log loop drains nothing — appends are held) →
  // install_snapshot_now: 5 <= commit 7 → stale-drop, but it RECORDS the durable snapshot boundary.
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(ep.pending_install.is_none(), "stale install dropped");
  assert_eq!(
    ep.durable_index,
    Index::new(3),
    "no re-baseline → durable_index unchanged"
  );
  // THE FIX: the follower honestly acks the durable snapshot boundary (5) — a crash would Restore to it —
  // instead of the lower durable log tip (3) that would pin the leader in Snapshot(5).
  assert_eq!(
    ep.ack_watermark(),
    Index::new(5),
    "ack_watermark reflects the durable snapshot boundary, not just the unflushed durable log tip"
  );

  // When the held tail finally flushes, durable_index overtakes and ack_watermark rises with it (MAX).
  log.flush_held_appends();
  ep.handle_storage(d, &mut log, &mut stable);
  assert_eq!(ep.durable_index, Index::new(7));
  assert_eq!(
    ep.ack_watermark(),
    Index::new(7),
    "max(durable_index 7, durable snapshot 5) = 7"
  );
}

/// Completes the dropped-stale-install class on the RECEIPT-time stale path: a snapshot that is committed
/// (boundary <= commit) but ABOVE the durable recoverable prefix (boundary > ack_watermark) must NOT be
/// dropped by the receipt-time staleness short-circuit — it is deferred-installed so its durable
/// boundary is RECORDED, raising ack_watermark and un-pinning the leader. The completion-time stale-drop
/// prevents any commit/log regress.
///
/// MUTATION: revert the receipt-time guard to `meta.last_index() <= self.commit` → the snapshot
/// short-circuits without recording, so `pending_install` is never set and ack_watermark stays 3
/// (the leader stays pinned in Snapshot).
#[test]
fn receipt_stale_above_watermark_records_durable_snapshot() {
  use crate::{AppendEntries, Entry, EntryKind, Index, Instant, Message, Term};
  let (mut ep, mut log, mut stable, _cfg) = follower_committed_to_3();
  let d = Instant::ORIGIN;
  // Async appends [4..=12] (leader_commit=12) advance commit to 12 over a HELD (unflushed) tail, so
  // durable_index stays 3 and ack_watermark == 3.
  log.hold_appends(true);
  let tail: std::vec::Vec<Entry> = (4u64..=12)
    .map(|i| {
      Entry::new(
        Term::new(2),
        Index::new(i),
        EntryKind::Empty,
        bytes::Bytes::new(),
      )
    })
    .collect();
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    1u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(2),
      1u64,
      Index::new(3),
      Term::new(2),
      tail,
      Index::new(12),
    )),
  );
  assert_eq!(ep.commit, Index::new(12));
  assert_eq!(
    ep.ack_watermark(),
    Index::new(3),
    "recoverable prefix is the unflushed durable tip"
  );

  // InstallSnapshot at boundary 10: committed (10 <= commit 12) but ABOVE ack_watermark (3). It must
  // NOT be short-circuited — it falls through to the deferred install.
  ep.handle_message(d, &mut log, &mut stable, 1u64, install_at(10));
  assert!(
    ep.pending_install.is_some(),
    "a committed-but-not-yet-recoverable snapshot is deferred, not dropped"
  );

  // On SnapshotWritten the install stale-drops (10 <= commit 12) but RECORDS the durable boundary.
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(ep.pending_install.is_none());
  assert_eq!(ep.commit, Index::new(12), "commit did NOT regress");
  assert_eq!(
    ep.durable_index,
    Index::new(3),
    "no re-baseline of the still-unflushed log"
  );
  assert_eq!(
    ep.ack_watermark(),
    Index::new(10),
    "the durable snapshot boundary is now ackable → the leader is un-pinned from Snapshot"
  );
}
