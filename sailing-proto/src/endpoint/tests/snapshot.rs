use super::{super::snapshot::ChunkSend, *};
use crate::{
  HeartbeatResponse, InstallSnapshot, ProgressState, SnapshotMeta, SnapshotResponse,
  testkit::{AsyncStable, CountSm, FailTermLog, NoopStable, VecLog},
};

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
/// MUTATION: revert FIX 1 to `self.durable.durable_index = self.durable.durable_index.max(meta.last_index())`.
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
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();

  // Durable-but-uncommitted tail: entries 1..=3 at term 1, leader_commit=0.
  let tail: Vec<Entry> = (1u64..=3)
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
  assert_eq!(
    ep.durable.durable_index,
    Index::new(3),
    "tail flushed → durable=3"
  );
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
    ep.durable.durable_index,
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
    ep.durable.durable_index,
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
    .expect("duplicate emits an immediate AppendResponse");
  match dup.message() {
    Message::AppendResponse(a) => {
      assert!(!a.reject(), "duplicate is a success ack");
      assert_eq!(
        a.match_index(),
        Index::new(2),
        "persist-before-ack: the duplicate must report the snapshot boundary (2), \
           not the unflushed in-flight entry 3"
      );
    }
    other => panic!("expected AppendResponse, got {other:?}"),
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
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);

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

/// Regression (storage-drain progress): the deferred compaction that fires when the
/// `SnapshotWritten` completion drains calls `log.compact` AFTER the log drain phase. `compact`
/// mints no op id, so a budget/op-id-only progress check would miss the `LogDone::Compacted` (or a
/// compaction error) it can enqueue and report `Drained` with that completion still queued. This
/// call runs no submission and exhausts no budget, so the compaction is the SOLE progress signal —
/// `handle_storage` must report `MorePending` (this fails against the budget/op-id-only return).
#[test]
fn handle_storage_reports_more_pending_when_a_deferred_compaction_fires() {
  let (mut ep, mut log, mut stable) = make_single_node_leader_with_entries(3, 3);
  assert!(
    ep.pending_compact().is_some(),
    "snapshot write in flight, compaction deferred"
  );

  // The SnapshotWritten completion drains → the deferred `log.compact` fires.
  let progress = ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  assert!(
    log.first_index() > Index::new(1),
    "the deferred compaction ran (first_index advanced past 1)"
  );
  assert_eq!(
    progress,
    crate::StorageProgress::MorePending,
    "a post-drain compaction must report MorePending, not Drained"
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
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  // We shouldn't have gotten a SECOND snapshot submission — check pending_compact is still set.
  // (It won't be cleared because there's no new SnapshotWritten completion.)
  // The stable still has exactly one snapshot (no double-submit).
  let snap_count_after = stable.snapshot().map(|_| 1usize).unwrap_or(0);
  assert_eq!(
    snap_count_before, snap_count_after,
    "maybe_snapshot must not re-fire while pending_compact is set"
  );
}

/// A NON-failover LeaseGuard snapshot carries a non-zero `max_lease_window` but a ZERO
/// `max_wall_plus_window`: every entry such a leader appends has a real `lease_window` (the
/// commit-wait window) yet an ABSENT wall (`wall_timestamp == 0`, the failover tier is off), so the
/// per-entry `wall + window` floor must fold NOTHING. The two floors are independent — a non-failover
/// snapshot must never let `lease_window` alone masquerade as a wall-derived release floor.
///
/// MUTATION (revert FIX 1 — drop the `e.wall_timestamp() != 0` guard in `submit_append`): each
/// `0`-wall entry then folds `0.saturating_add(lease_window) == lease_window` into
/// `max_wall_plus_window`, so it rises to equal `max_lease_window` and the `== 0` assertion FAILS.
#[test]
fn non_failover_leaseguard_snapshot_has_zero_wall_plus_window() {
  use crate::{Config, Index};
  use core::time::Duration;

  // LeaseGuard WITHOUT the failover tier (no `bounded_clock_uncertainty`): a valid window
  // (Δ=300ms, ε=50ms → 300·350/250 = 420ms < the 1000ms election timeout) so every appended entry
  // carries a non-zero `lease_window`, while the wall stamp stays absent (0).
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_read_only(ReadOnlyOption::LeaseGuard)
  .with_lease_duration(Duration::from_millis(300))
  .with_clock_drift_bound(Duration::from_millis(50))
  .with_snapshot_threshold(3);

  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 42, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();

  // Elect the single-node leader (self-vote durable first), drain the stamped no-op.
  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable);
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(ep.role().is_leader());
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  // Apply past `snapshot_threshold`: no-op (idx 1) + 3 Normal entries (idx 2,3,4) → applied=4,
  // first_index=1 → gap=3, so the next `handle_storage` submits a snapshot.
  for i in 0..3 {
    let cmd = bytes::Bytes::copy_from_slice(&[i as u8]);
    let _ = ep.propose(d, &mut log, &stable, &cmd).unwrap();
    ep.handle_storage(d, &mut log, &mut stable);
    while ep.poll_message().is_some() {}
    while ep.poll_event().is_some() {}
  }

  // The snapshot must have been submitted to stable; capture its carried meta.
  let (meta, _data) = stable
    .snapshot()
    .expect("a snapshot crossed the threshold and was persisted");
  assert_eq!(
    log.first_index(),
    Index::new(1),
    "the snapshot is in flight (compaction deferred until SnapshotWritten)"
  );

  // The lease floor IS populated (LeaseGuard stamps a real window on every entry) ...
  assert!(
    meta.max_lease_window() > 0,
    "a LeaseGuard snapshot must carry the inherited commit-wait window"
  );
  // ... but the wall+window floor is ZERO: a `0`-wall entry folds nothing into it. Without FIX 1
  // it would instead equal `max_lease_window` (the bug this regression pins).
  assert_eq!(
    meta.max_wall_plus_window(),
    0,
    "a non-failover (wall-absent) entry must contribute 0 to the wall+window release floor"
  );
}

/// A NON-failover LeaseGuard snapshot carries `max_unwalled_lease_window == max_lease_window`: the
/// unwalled-fallback floor is gated by the ENTRY property (`lease_window > 0 && wall_timestamp == 0`),
/// NOT the local failover tier — the exact dual of the wall floor — so every wall-absent lease entry
/// folds itself on every node and the floor stays complete. On a non-failover cluster every entry is
/// wall-absent, so the floor equals `max_lease_window`; it is INERT here (the sole consumer,
/// `precise_release_ready`, is hard-gated off off-tier).
///
/// MUTATION (re-gate the fold on the local tier): the floor drops to 0 on a non-failover node, and a
/// later failover restart from such a snapshot under-counts its inherited leases — a cross-tier stale
/// read.
#[test]
fn non_failover_leaseguard_snapshot_unwalled_tracks_lease_window() {
  use crate::Config;
  use core::time::Duration;

  // LeaseGuard WITHOUT the failover tier (no `bounded_clock_uncertainty`): every appended entry
  // carries a non-zero `lease_window` while the wall stamp stays absent (0) — i.e. every entry meets
  // the `lease_window > 0 && wall_timestamp == 0` fold condition, so the entry-property fold raises the
  // bound to `max_lease_window` on this non-failover node.
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_read_only(ReadOnlyOption::LeaseGuard)
  .with_lease_duration(Duration::from_millis(300))
  .with_clock_drift_bound(Duration::from_millis(50))
  .with_snapshot_threshold(3);

  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 42, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();

  // Elect the single-node leader (self-vote durable first), drain the stamped no-op.
  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable);
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(ep.role().is_leader());
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  // Apply past `snapshot_threshold` so the next `handle_storage` submits a snapshot.
  for i in 0..3 {
    let cmd = bytes::Bytes::copy_from_slice(&[i as u8]);
    let _ = ep.propose(d, &mut log, &stable, &cmd).unwrap();
    ep.handle_storage(d, &mut log, &mut stable);
    while ep.poll_message().is_some() {}
    while ep.poll_event().is_some() {}
  }

  let (meta, _data) = stable
    .snapshot()
    .expect("a snapshot crossed the threshold and was persisted");

  // The lease window IS populated (every entry has a non-zero `lease_window`) ...
  assert!(
    meta.max_lease_window() > 0,
    "a LeaseGuard snapshot must carry the inherited commit-wait window"
  );
  // ... and the unwalled-lease fallback bound EQUALS it: the entry-property fold folds every wall-absent
  // lease entry, so on a non-failover cluster (all entries wall-absent) the floor is `max_lease_window`.
  // Inert here (the precise anchor is off-tier), but complete by construction so a later failover restart
  // from this snapshot covers every inherited lease.
  assert_eq!(
    meta.max_unwalled_lease_window(),
    meta.max_lease_window(),
    "the entry-property fold folds every wall-absent lease entry, so the bound tracks max_lease_window"
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
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);

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
  let d = Instant::ORIGIN;
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
  ep.maybe_send_append(Now::monotonic(Instant::ORIGIN), 2u64, &log, &stable);

  // Exactly one outgoing message to peer 2 must be InstallSnapshot.
  let msgs: Vec<_> = core::iter::from_fn(|| ep.poll_message()).collect();
  let snap_msgs: Vec<_> = msgs
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
  if let ProgressState::Snapshot { pending, .. } = pr.state() {
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

  ep.maybe_send_append(Now::monotonic(Instant::ORIGIN), 2u64, &log, &stable);

  // Must NOT see any AppendEntries with prev_log_term == ZERO for this peer.
  while let Some(out) = ep.poll_message() {
    if out.to() == 2u64
      && let Message::AppendEntries(ae) = out.message()
    {
      assert_ne!(
        ae.prev_log_term(),
        Term::ZERO,
        "a broken AppendEntries with prev_log_term=ZERO must not be sent to a compacted peer"
      );
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
  ep.maybe_send_append(Now::monotonic(Instant::ORIGIN), 2u64, &log, &stable);
  while ep.poll_message().is_some() {} // drain

  // Second call: peer is now paused (Snapshot state), must send nothing.
  ep.maybe_send_append(Now::monotonic(Instant::ORIGIN), 2u64, &log, &stable);
  let msgs: Vec<_> = core::iter::from_fn(|| ep.poll_message()).collect();
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

  ep.maybe_send_append(Now::monotonic(Instant::ORIGIN), 2u64, &log, &stable);

  let msgs: Vec<_> = core::iter::from_fn(|| ep.poll_message()).collect();

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
    if out.to() == 2u64
      && let Message::AppendEntries(ae) = out.message()
    {
      assert_ne!(
        ae.prev_log_term(),
        crate::Term::ZERO,
        "AppendEntries at the compaction boundary must carry the boundary term, not ZERO"
      );
    }
  }
}

/// A HeartbeatResponse from a peer still stuck in Snapshot state (its
/// InstallSnapshot was dropped) must RE-SEND the InstallSnapshot, carrying the same meta.
///
/// FAILS-ON-OLD: without the resend hook the HeartbeatResponse produces NO InstallSnapshot
/// (maybe_send_append early-returns on the paused Snapshot peer), so the follower wedges.
///
/// PACING (deadline armed AT each send): the initial install (sent at ORIGIN by the helper) arms
/// the deadline, so responses within one election timeout of the SEND must not re-transmit the
/// blob; the first response at/after the deadline re-sends and re-arms.
#[test]
fn heartbeat_resend_snapshot_to_wedged_follower() {
  use crate::{Index, Instant, Message, Term};

  let offset = 5u64;
  let (mut ep, mut log, mut stable, pending) = wedged_snapshot_follower(offset, 2);
  assert_eq!(pending, Index::new(offset));
  let hb_response = || {
    Message::HeartbeatResponse(HeartbeatResponse::new(
      Term::new(1),
      2u64,
      bytes::Bytes::new(),
    ))
  };
  let count_installs = |ep: &mut Endpoint<u64, CountSm>| {
    core::iter::from_fn(|| ep.poll_message())
      .filter(|o| o.to() == 2u64 && matches!(o.message(), Message::InstallSnapshot(_)))
      .count()
  };

  // Peer 2 is still in Snapshot(offset) with match_index = 0 < pending: it has NOT received the
  // snapshot. A response WITHIN one election timeout of the initial send (the helper sent it at
  // ORIGIN) must NOT re-send: the blob just went out, and the deadline armed at that send covers
  // it. (An immediate resend here is exactly the double-blob amplification the pacing prevents.)
  ep.handle_message(Instant::ORIGIN, &mut log, &mut stable, 2u64, hb_response());
  assert_eq!(
    count_installs(&mut ep),
    0,
    "a response within one election timeout of the install send must not re-send the blob"
  );

  // The first response at/after the deadline (one election timeout past the SEND) re-sends,
  // carrying the same meta.
  let later = Instant::ORIGIN + ep.config.election_timeout();
  ep.handle_message(later, &mut log, &mut stable, 2u64, hb_response());
  let msgs: Vec<_> = core::iter::from_fn(|| ep.poll_message()).collect();
  let snap_msgs: Vec<_> = msgs
    .iter()
    .filter(|o| o.to() == 2u64 && matches!(o.message(), Message::InstallSnapshot(_)))
    .collect();
  assert_eq!(
    snap_msgs.len(),
    1,
    "the first HeartbeatResponse at/after the deadline must RE-SEND exactly one InstallSnapshot"
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
  if let ProgressState::Snapshot { pending: p, .. } = pr.state() {
    assert_eq!(
      p, pending,
      "pending snapshot index is unchanged by the resend"
    );
  }

  // BACKOFF: the resend re-armed the deadline, so another response at the SAME instant must not
  // re-send again — regardless of how many responses arrive (ReadIndex Safe rounds elicit extras).
  ep.handle_message(later, &mut log, &mut stable, 2u64, hb_response());
  assert_eq!(
    count_installs(&mut ep),
    0,
    "a response within one election timeout of the RESEND must not re-send again (backoff)"
  );

  // TIME-based pacing repeats: one more election timeout later, the next response re-sends.
  let even_later = later + ep.config.election_timeout();
  ep.handle_message(even_later, &mut log, &mut stable, 2u64, hb_response());
  assert_eq!(
    count_installs(&mut ep),
    1,
    "a response after the re-armed deadline re-sends the blob (liveness repeats)"
  );
}

/// FAILS-ON-OLD: when the heartbeat-response PUMP is what opens the install window
/// (a compacted Probe peer resumes on a heartbeat ack), the same response handling must not send
/// the blob TWICE — once from the pump's compacted-hole branch and once from the resend hook,
/// which previously saw "Snapshot state + no deadline" and fired immediately.
#[test]
fn heartbeat_pump_initial_install_is_not_double_sent() {
  use crate::{Index, Instant, Message, Term};

  let offset = 5u64;
  let (mut ep, mut log, mut stable) = make_leader_with_compacted_log(offset, 2);

  // Peer 2 far behind (next_index < first_index) and still in Probe: the install window is NOT
  // yet open — the heartbeat response below is what opens it.
  if let Some(p) = ep.tracker.progress_mut(&2u64) {
    p.become_probe();
    p.set_next_index(Index::new(2));
  }
  while ep.poll_message().is_some() {} // drop anything emitted during setup

  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    2u64,
    Message::HeartbeatResponse(HeartbeatResponse::new(
      Term::new(1),
      2u64,
      bytes::Bytes::new(),
    )),
  );
  let installs = core::iter::from_fn(|| ep.poll_message())
    .filter(|o| o.to() == 2u64 && matches!(o.message(), Message::InstallSnapshot(_)))
    .count();
  assert_eq!(
    installs, 1,
    "one response handling = one InstallSnapshot: the pump's initial install must arm the \
     pacing deadline so the resend hook does not duplicate it"
  );
  assert!(
    ep.tracker.progress(&2u64).unwrap().state().is_snapshot(),
    "the pump moved peer 2 into Snapshot state"
  );
}

/// FAILS-ON-OLD: a pacing deadline left over from a PREVIOUS install window must not
/// leak into a new one. The peer exits Snapshot via `maybe_update` (no heartbeat observation to
/// clean the map), falls behind a fresh compaction, and re-enters Snapshot — the NEW install send
/// must overwrite the stale (long-expired) deadline, so a response right after the new install
/// does NOT immediately re-send the blob.
#[test]
fn stale_resend_deadline_does_not_leak_across_install_windows() {
  use crate::{Index, Instant, Message, Term};

  let offset = 5u64;
  let (mut ep, mut log, mut stable, pending) = wedged_snapshot_follower(offset, 2);
  let hb_response = || {
    Message::HeartbeatResponse(HeartbeatResponse::new(
      Term::new(1),
      2u64,
      bytes::Bytes::new(),
    ))
  };

  // Window 1: a resend fires at the deadline, re-arming it (deadline now ORIGIN + 2·ET).
  let t1 = Instant::ORIGIN + ep.config.election_timeout();
  ep.handle_message(t1, &mut log, &mut stable, 2u64, hb_response());
  while ep.poll_message().is_some() {}

  // The follower acks at pending: it exits Snapshot via maybe_update (SnapshotResponse path) —
  // NO heartbeat-response observation cleans the pacing map here.
  ep.handle_message(
    t1,
    &mut log,
    &mut stable,
    2u64,
    Message::SnapshotResponse(SnapshotResponse::new(Term::new(1), 2u64, false, pending)),
  );
  assert!(
    !ep.tracker.progress(&2u64).unwrap().state().is_snapshot(),
    "after acking at pending the follower leaves Snapshot state"
  );
  while ep.poll_message().is_some() {}

  // Window 2 opens MUCH later (the stale window-1 deadline is long expired): the peer falls
  // behind the compaction boundary again and a heartbeat response re-opens the install.
  if let Some(p) = ep.tracker.progress_mut(&2u64) {
    p.become_probe();
    p.set_next_index(Index::new(2));
  }
  let t2 = Instant::ORIGIN + ep.config.election_timeout() * 10;
  ep.handle_message(t2, &mut log, &mut stable, 2u64, hb_response());
  let installs = core::iter::from_fn(|| ep.poll_message())
    .filter(|o| o.to() == 2u64 && matches!(o.message(), Message::InstallSnapshot(_)))
    .count();
  assert_eq!(
    installs, 1,
    "the new window's install must overwrite the stale deadline — exactly one blob, not an \
     install plus an immediate stale-deadline resend"
  );

  // And the very next response inside the new window's deadline stays quiet.
  ep.handle_message(t2, &mut log, &mut stable, 2u64, hb_response());
  let extra = core::iter::from_fn(|| ep.poll_message())
    .filter(|o| o.to() == 2u64 && matches!(o.message(), Message::InstallSnapshot(_)))
    .count();
  assert_eq!(
    extra, 0,
    "window 2 paces from ITS send, not window 1's deadline"
  );
}

/// The resend STOPS once the follower acks past its pending snapshot index.
/// After a SnapshotResponse (match >= pending) the peer leaves Snapshot state (→ Probe), so a
/// subsequent HeartbeatResponse must NOT emit another InstallSnapshot (no infinite resend / spam).
#[test]
fn no_snapshot_resend_after_follower_catches_up() {
  use crate::{Instant, Message, Term};

  let offset = 5u64;
  let (mut ep, mut log, mut stable, pending) = wedged_snapshot_follower(offset, 2);

  // First heartbeat round at the deadline while wedged: resend fires (sanity — same as above).
  let t1 = Instant::ORIGIN + ep.config.election_timeout();
  ep.handle_message(
    t1,
    &mut log,
    &mut stable,
    2u64,
    Message::HeartbeatResponse(HeartbeatResponse::new(
      Term::new(1),
      2u64,
      bytes::Bytes::new(),
    )),
  );
  let resent = core::iter::from_fn(|| ep.poll_message())
    .filter(|o| o.to() == 2u64 && matches!(o.message(), Message::InstallSnapshot(_)))
    .count();
  assert_eq!(resent, 1, "resend fires while the follower is still wedged");

  // The follower finally receives a snapshot and acks at pending (SnapshotResponse success).
  ep.handle_message(
    t1,
    &mut log,
    &mut stable,
    2u64,
    Message::SnapshotResponse(SnapshotResponse::new(Term::new(1), 2u64, false, pending)),
  );
  // It must have left Snapshot state (maybe_update(pending) → Probe).
  assert!(
    !ep.tracker.progress(&2u64).unwrap().state().is_snapshot(),
    "after acking at pending the follower must leave Snapshot state"
  );
  while ep.poll_message().is_some() {} // drain anything the catch-up emitted

  // A subsequent HeartbeatResponse — even WAY past every armed deadline — must NOT emit another
  // InstallSnapshot (the resend is gated on Snapshot state, which the peer has left).
  ep.handle_message(
    Instant::ORIGIN + ep.config.election_timeout() * 10,
    &mut log,
    &mut stable,
    2u64,
    Message::HeartbeatResponse(HeartbeatResponse::new(
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
  let meta = SnapshotMeta::new(
    Index::new(10),
    Term::new(4),
    ConfState::from_voters(std::vec![1u64, 2u64, 3u64]),
  );
  let is = InstallSnapshot::new(Term::new(1), 1u64, meta.clone(), snap_data.clone());

  // Follower commit starts at 0 (< 10) → install path.
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::InstallSnapshot(is),
  );
  // the install adopts term 1 (follower started at term 0), so the post-install SnapshotResponse is
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
  let crate::EntriesRead::Ready(entries) = log
    .entries(Index::new(11)..Index::new(11), u64::MAX)
    .unwrap()
  else {
    panic!("a resident store never returns Pending");
  };
  assert!(
    entries.is_empty(),
    "entries(11..11) must be empty after restore"
  );

  // Exactly one SnapshotInstalled event must be emitted.
  let events: Vec<_> = core::iter::from_fn(|| ep.poll_event()).collect();
  let installed: Vec<_> = events
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

  // Exactly one SnapshotResponse must be sent to the leader (node 1) with reject=false,
  // match_index=10.
  let msgs: Vec<_> = core::iter::from_fn(|| ep.poll_message()).collect();
  let snap_responses: Vec<_> = msgs
    .iter()
    .filter(|o| o.to() == 1u64 && matches!(o.message(), Message::SnapshotResponse(_)))
    .collect();
  assert_eq!(
    snap_responses.len(),
    1,
    "exactly one SnapshotResponse must be sent to the leader"
  );
  let sr = match snap_responses[0].message() {
    Message::SnapshotResponse(r) => r,
    _ => unreachable!(),
  };
  assert!(
    !sr.reject(),
    "SnapshotResponse must not be a rejection on successful install"
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
/// A follower has a queued success `AppendResponse(match_index = 3)` still in `outgoing` (it acked
/// index 3, but the ack has not yet been polled). It then installs a snapshot at a LOWER boundary
/// (`last_index = 2`). The truncated entry 3 no longer exists, so emitting that ack would over-ack
/// an entry the follower no longer stores — letting the leader count a phantom replica toward
/// commit. After the install, no success `AppendResponse` with `match_index > 2` may be emitted.
#[test]
fn install_snapshot_scrubs_stale_outgoing_ack() {
  use crate::{Index, Instant, Message, Outgoing, Term, conf::ConfState};

  let (mut ep, mut log, mut stable) = make_follower();

  // Queue a success AppendResponse(match_index = 3) as if the follower had acked index 3 and the ack
  // is still sitting in `outgoing` (not yet polled). This is the stale ack that must be scrubbed.
  ep.outputs.outgoing.push_back(Outgoing::new(
    1u64,
    Message::AppendResponse(crate::AppendResponse::new(
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
  let meta = SnapshotMeta::new(
    Index::new(2),
    Term::new(4),
    ConfState::from_voters(std::vec![1u64, 2u64, 3u64]),
  );
  let is = InstallSnapshot::new(Term::new(1), 1u64, meta, snap_data);
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

  // Drain all outgoing messages: NONE may be a success AppendResponse with match_index > 2.
  let msgs: Vec<_> = core::iter::from_fn(|| ep.poll_message()).collect();
  let over_ack = msgs.iter().any(|o| {
      matches!(o.message(), Message::AppendResponse(a) if !a.reject() && a.match_index() > Index::new(2))
    });
  assert!(
    !over_ack,
    "the stale success AppendResponse(match_index = 3) must be scrubbed by the snapshot install"
  );
}

/// Test 2: a stale snapshot (last_index <= commit) is a no-op ack, SM not touched.
#[test]
fn stale_snapshot_does_not_install() {
  use crate::{Entry, EntryKind, Index, Instant, Message, Term, conf::ConfState};

  let (mut ep, mut log, mut stable) = make_follower();

  // Seed the follower log with 15 entries so commit can be set to 15.
  let entries: Vec<_> = (1u64..=15)
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
  ep.durable.durable_index = Index::new(15);
  // SM count is arbitrary (doesn't matter — must not change).
  let sm_count_before = ep.state_machine().count();

  // Try to install a snapshot with last_index=10 (< commit=15): stale.
  let snap_data = encode_snapshot(7u64);
  let meta = SnapshotMeta::new(
    Index::new(10),
    Term::new(4),
    ConfState::from_voters(std::vec![1u64, 2u64, 3u64]),
  );
  let is = InstallSnapshot::new(Term::new(1), 1u64, meta, snap_data);
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::InstallSnapshot(is),
  );
  // the install adopts term 1 (the follower starts at term 0), so the stale-snapshot success ack
  // is deferred until that term write is durable. Drain storage (the driver does this every iteration)
  // to complete the term write and release the deferred SnapshotResponse.
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
  let events: Vec<_> = core::iter::from_fn(|| ep.poll_event())
    .filter(|e| e.is_snapshot_installed())
    .collect();
  assert!(
    events.is_empty(),
    "no SnapshotInstalled event for a stale snapshot"
  );

  // Must still send a SnapshotResponse with reject=false and match_index = self.commit.
  let msgs: Vec<_> = core::iter::from_fn(|| ep.poll_message()).collect();
  let snap_responses: Vec<_> = msgs
    .iter()
    .filter(|o| o.to() == 1u64 && matches!(o.message(), Message::SnapshotResponse(_)))
    .collect();
  assert_eq!(
    snap_responses.len(),
    1,
    "stale snapshot must still send a SnapshotResponse"
  );
  let sr = match snap_responses[0].message() {
    Message::SnapshotResponse(r) => r,
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
  let meta = SnapshotMeta::new(
    Index::new(10),
    Term::new(4),
    ConfState::from_voters(std::vec![1u64, 2u64, 3u64]),
  );
  let is = InstallSnapshot::new(Term::new(1), 1u64, meta, bad_data);
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
  let meta2 = SnapshotMeta::new(
    Index::new(10),
    Term::new(4),
    ConfState::from_voters(std::vec![1u64, 2u64, 3u64]),
  );
  let is2 = InstallSnapshot::new(Term::new(1), 1u64, meta2, good_data);
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

/// Test 4: leader processes a successful SnapshotResponse — peer leaves Snapshot state.
#[test]
fn leader_processes_snapshot_response_success_and_reject() {
  use crate::{Index, Instant, Message, Term, VoteResponse};
  use core::time::Duration;

  // Build a 3-voter leader (node 1).
  let cfg = crate::Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();

  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable);
  ep.handle_storage(d, &mut log, &mut stable);
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResponse(VoteResponse::new(Term::new(1), 2u64, false, false)),
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

  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::SnapshotResponse(SnapshotResponse::new(
      Term::new(1),
      2u64,
      true, // reject
      Index::new(10),
    )),
  );
  // After reject the peer must have transitioned to Probe.
  assert!(
    ep.tracker.progress(&2u64).unwrap().state().is_probe(),
    "reject SnapshotResponse must transition peer to Probe"
  );

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
    Message::SnapshotResponse(SnapshotResponse::new(
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
    "success SnapshotResponse must transition peer out of Snapshot state"
  );
  assert_eq!(
    pr.match_index(),
    Index::new(10),
    "match_index must be 10 after successful SnapshotResponse"
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
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = NoopStable::default();

  // Malformed membership: node 1 is BOTH a voter and a learner. last_index=5 > commit=0 passes the
  // staleness guard and reaches the Step-0 membership validation.
  let bad_meta = SnapshotMeta::new(
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
    Message::InstallSnapshot(InstallSnapshot::new(
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
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = NoopStable::default();

  // Joint config where node 3 is in the incoming voters AND staged in learners_next — impossible.
  let bad_meta = SnapshotMeta::new(
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
    Message::InstallSnapshot(InstallSnapshot::new(
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
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();
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
  assert_eq!(ep.durable.committed_persisted, Index::new(3));

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
    ep.snapshot.pending_install.is_some(),
    "install deferred → pending_install armed"
  );
  assert_eq!(
    ep.commit,
    Index::new(3),
    "commit NOT advanced until the blob is durable"
  );
  assert_eq!(
    ep.durable.durable_index,
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
    ep.snapshot.pending_install.is_none(),
    "blob durable → deferred install completed"
  );
  assert_eq!(
    ep.commit,
    Index::new(10),
    "commit advances to the boundary once the blob is durable"
  );
  assert_eq!(ep.durable.durable_index, Index::new(10));
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
/// ack hole the immediate `AppendResponse` clamp closes on the AppendEntries path).
///
/// Setup: durable log [1..=3] (commit/durable 3); a second AppendEntries [4..=5] with leader_commit=5
/// advances commit to 5 but `durable_index` stays 3 (the 4/5 `Appended` is NOT drained). A stale
/// InstallSnapshot(last_index=5) then hits the guard; the reply must report 3 = min(commit 5,
/// durable_index 3), not 5.
///
/// MUTATION: revert the stale-guard ack to `self.commit` → the `SnapshotResponse` reports 5, over-acking
/// the non-durable tail [4..=5].
#[test]
fn stale_snapshot_response_is_clamped_to_durable_watermark() {
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
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();
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
  assert_eq!(ep.durable.durable_index, Index::new(3));
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
    ep.durable.durable_index,
    Index::new(3),
    "but the 4/5 append is not yet durable"
  );
  // Drain the outbox (the immediate AppendResponse for the second append) so the next poll is the
  // SnapshotResponse under test.
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

  let response = ep
    .poll_message()
    .expect("a stale InstallSnapshot emits a SnapshotResponse");
  match response.message() {
    Message::SnapshotResponse(s) => {
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
    other => panic!("expected SnapshotResponse, got {other:?}"),
  }
  // The stale path installs nothing: commit must not regress and no deferred install is armed.
  assert_eq!(
    ep.commit,
    Index::new(5),
    "a stale snapshot must not regress commit"
  );
  assert!(
    ep.snapshot.pending_install.is_none(),
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
    ep.snapshot.pending_install.is_some(),
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
  let ep2 = Endpoint::restart(cfg, d, 1, CountSm::default(), 2, &mut log, &mut stable);
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
  assert!(ep.snapshot.pending_install.is_some());

  // Drain storage: NO SnapshotWritten arrives (torn); the fallback sees durable_snapshot()==None and
  // must NOT fire the destructive install.
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(
    ep.snapshot.pending_install.is_some(),
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
  let ep2 = Endpoint::restart(cfg, d, 1, CountSm::default(), 2, &mut log, &mut stable);
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
  assert!(ep.snapshot.pending_install.is_some());
  assert!(
    stable.durable_snapshot().is_some(),
    "blob durable despite the dropped completion"
  );

  // Drain storage: no SnapshotWritten arrives, but the durable_snapshot() fallback completes the install.
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(
    ep.snapshot.pending_install.is_none(),
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
  assert!(ep.snapshot.pending_install.is_some());
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
    Message::VoteResponse(v) => Some(v.reject()),
    _ => None,
  });
  assert_eq!(
    granted_reject,
    Some(true),
    "the freshness floor at the pending-install boundary must REJECT a candidate below it"
  );
}

/// The SAME freshness floor must apply during an in-progress CHUNKED receive — BEFORE the blob completes
/// into `pending_install`. Without it, the multi-chunk receive window would advertise stale-LOW freshness
/// and could help elect a candidate behind the committed snapshot boundary already accepted.
///
/// MUTATION: drop the `snapshot_recv` floor in `on_request_vote` → the below-boundary candidate is granted
/// (assert fails).
#[test]
fn vote_freshness_floored_at_chunked_receive_boundary() {
  use crate::{
    Index, InstallSnapshot, Instant, Message, RequestVote, SnapshotMeta, Term, conf::ConfState,
  };
  let (mut ep, mut log, mut stable, _cfg) = follower_committed_to_3();
  let d = Instant::ORIGIN;
  // Accept only the FIRST chunk of a higher-boundary (10/2) snapshot: `snapshot_recv` is armed, but the
  // blob is incomplete (3 of 100 bytes), so `pending_install` is NOT yet armed.
  let meta = SnapshotMeta::new(
    Index::new(10),
    Term::new(2),
    ConfState::from_voters(std::vec![1u64, 2u64, 3u64]),
  );
  let first_chunk = InstallSnapshot::new_chunk(
    Term::new(2),
    1u64,
    meta,
    bytes::Bytes::from_static(b"abc"),
    0,
    100,
  );
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    1u64,
    Message::InstallSnapshot(first_chunk),
  );
  assert!(
    ep.snapshot.snapshot_recv.is_some(),
    "the first chunk arms snapshot_recv"
  );
  assert!(
    ep.snapshot.pending_install.is_none(),
    "the blob is incomplete — pending_install is NOT yet armed"
  );
  while ep.poll_message().is_some() {}

  // A candidate at a HIGHER term whose log (5/2) is BELOW the committed snapshot boundary (10/2).
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
    Message::VoteResponse(v) => Some(v.reject()),
    _ => None,
  });
  assert_eq!(
    granted_reject,
    Some(true),
    "the freshness floor at the in-progress chunked-receive boundary must REJECT a candidate below it"
  );
}

/// An abandoned chunked receive must be reclaimed once the recoverable prefix catches up past its
/// boundary (a snapshot/AppendEntries race where the live log wins) — freeing `snapshot_recv` AND the
/// store staging, not pinning a full `total_len` buffer until a supersede/restart.
///
/// MUTATION: drop the `reclaim_stale_snapshot_recv` call in `handle_storage` → `snapshot_recv` stays armed
/// after the catch-up (assert fails).
#[test]
fn abandoned_chunked_receive_is_reclaimed_when_log_catches_up() {
  use crate::{
    AppendEntries, Entry, EntryKind, Index, InstallSnapshot, Instant, Message, SnapshotMeta, Term,
    conf::ConfState,
  };
  let (mut ep, mut log, mut stable, _cfg) = follower_committed_to_3();
  let d = Instant::ORIGIN;
  ep.handle_storage(d, &mut log, &mut stable); // make the committed prefix durable (ack_watermark = 3)
  while ep.poll_message().is_some() {}

  // A partial first chunk of a boundary-5 snapshot arms snapshot_recv (5 is above the recoverable prefix 3).
  let meta = SnapshotMeta::new(
    Index::new(5),
    Term::new(2),
    ConfState::from_voters(std::vec![1u64, 2u64, 3u64]),
  );
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    1u64,
    Message::InstallSnapshot(InstallSnapshot::new_chunk(
      Term::new(2),
      1u64,
      meta,
      bytes::Bytes::from_static(b"abc"),
      0,
      100,
    )),
  );
  assert!(
    ep.snapshot.snapshot_recv.is_some(),
    "the partial chunk arms snapshot_recv"
  );
  while ep.poll_message().is_some() {}

  // A snapshot/AppendEntries RACE: the live log catches up to (and commits) boundary 5 before the rest of
  // the snapshot arrives.
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
  ep.handle_storage(d, &mut log, &mut stable); // entries 4,5 durable → ack_watermark = 5, reclaim fires

  assert!(
    ep.snapshot.snapshot_recv.is_none(),
    "an abandoned chunked receive must be reclaimed once the recoverable prefix covers its boundary"
  );
}

/// A new leader must be able to replace an abandoned HIGHER partial from an old leader with a LOWER
/// snapshot — a leader change can legitimately leave the follower below a new leader's first index, which
/// then sends `snapshot(K)+log`. Dropping the lower snapshot by boundary ordering would wedge the follower.
///
/// MUTATION: revert the supersession rule to boundary-only (drop when the partial boundary exceeds the
/// incoming) → the new leader's lower snapshot is dropped and snapshot_recv stays at the old boundary.
#[test]
fn new_leader_lower_snapshot_replaces_abandoned_higher_partial() {
  use crate::{Index, InstallSnapshot, Instant, Message, SnapshotMeta, Term, conf::ConfState};
  let (mut ep, mut log, mut stable, _cfg) = follower_committed_to_3();
  let d = Instant::ORIGIN;
  // Old leader (term 2) leaves a HIGH partial at boundary 100 (3 of 200 bytes).
  let hi = SnapshotMeta::new(
    Index::new(100),
    Term::new(2),
    ConfState::from_voters(std::vec![1u64, 2u64, 3u64]),
  );
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    1u64,
    Message::InstallSnapshot(InstallSnapshot::new_chunk(
      Term::new(2),
      1u64,
      hi,
      bytes::Bytes::from_static(b"abc"),
      0,
      200,
    )),
  );
  assert_eq!(
    ep.snapshot
      .snapshot_recv
      .as_ref()
      .map(|r| r.meta.last_index()),
    Some(Index::new(100)),
    "the old leader's high partial is armed"
  );
  while ep.poll_message().is_some() {}

  // A NEW leader (term 3) catches the follower up via a LOWER snapshot(50). The follower must REPLACE the
  // abandoned 100 partial, not drop the 50 by boundary ordering (which would wedge it).
  let lo = SnapshotMeta::new(
    Index::new(50),
    Term::new(3),
    ConfState::from_voters(std::vec![1u64, 2u64, 3u64]),
  );
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::InstallSnapshot(InstallSnapshot::new_chunk(
      Term::new(3),
      2u64,
      lo,
      bytes::Bytes::from_static(b"de"),
      0,
      80,
    )),
  );
  assert_eq!(
    ep.snapshot
      .snapshot_recv
      .as_ref()
      .map(|r| r.meta.last_index()),
    Some(Index::new(50)),
    "the new leader's lower snapshot REPLACES the abandoned higher partial (not dropped → no wedge)"
  );
}

/// A newer leader's chunk for the SAME snapshot identity (same meta + length) is a DISTINCT capture, not a
/// continuation — the receiver must DISCARD the old leader's partial staging and start fresh, never mix two
/// leaders' bytes into one blob (the StateMachine contract does not promise byte-identical cross-leader
/// encodings of the same applied state).
///
/// MUTATION: drop `r.sender_term == is.term()` from the `continues` check → the newer chunk continues the
/// old partial, leaving contiguous_staged = 6 (a mixed blob) instead of 0 (assert fails).
#[test]
fn newer_leader_same_identity_chunk_discards_old_partial_not_mixes() {
  use crate::{Index, InstallSnapshot, Instant, Message, SnapshotMeta, Term, conf::ConfState};
  let (mut ep, mut log, mut stable, _cfg) = follower_committed_to_3();
  let d = Instant::ORIGIN;
  let conf = ConfState::from_voters(std::vec![1u64, 2u64, 3u64]);
  // Old leader (term 2) stages [0,3) of a boundary-10 snapshot (total_len 6).
  let m = SnapshotMeta::new(Index::new(10), Term::new(2), conf);
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    1u64,
    Message::InstallSnapshot(InstallSnapshot::new_chunk(
      Term::new(2),
      1u64,
      m.clone(),
      bytes::Bytes::from_static(b"AAA"),
      0,
      6,
    )),
  );
  assert_eq!(
    ep.snapshot
      .snapshot_recv
      .as_ref()
      .map(|r| r.contiguous_staged),
    Some(3),
    "the old leader staged [0,3)"
  );
  while ep.poll_message().is_some() {}

  // A NEWER leader (term 3) sends the REMAINING chunk [3,6) for the SAME identity but a different capture.
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::InstallSnapshot(InstallSnapshot::new_chunk(
      Term::new(3),
      2u64,
      m,
      bytes::Bytes::from_static(b"BBB"),
      3,
      6,
    )),
  );
  let recv = ep
    .snapshot
    .snapshot_recv
    .as_ref()
    .expect("a transfer is in progress");
  assert_eq!(
    recv.sender_term,
    Term::new(3),
    "the newer leader's transfer REPLACED the old partial"
  );
  assert_eq!(
    recv.contiguous_staged, 0,
    "the old [0,3) was discarded — the new [3,6) leaves a gap, NOT a mixed [0,6) blob"
  );
}

/// A complete single-shot (legacy / 0-byte) snapshot must supersede an abandoned chunked partial —
/// clearing `snapshot_recv` and discarding the store staging — not install alongside a lingering stale
/// receive that would pin the staging buffer and skew the vote-freshness floor.
///
/// MUTATION: drop the snapshot_recv cleanup from the `total_len == 0` branch → the chunked partial survives
/// the single-shot install (snapshot_recv stays Some, assert fails).
#[test]
fn legacy_single_shot_supersedes_abandoned_chunked_partial() {
  use crate::{Index, InstallSnapshot, Instant, Message, SnapshotMeta, Term, conf::ConfState};
  let (mut ep, mut log, mut stable, _cfg) = follower_committed_to_3();
  let d = Instant::ORIGIN;
  // Old leader (term 2) leaves a HIGH chunked partial at boundary 100.
  let hi = SnapshotMeta::new(
    Index::new(100),
    Term::new(2),
    ConfState::from_voters(std::vec![1u64, 2u64, 3u64]),
  );
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    1u64,
    Message::InstallSnapshot(InstallSnapshot::new_chunk(
      Term::new(2),
      1u64,
      hi,
      bytes::Bytes::from_static(b"abc"),
      0,
      200,
    )),
  );
  assert!(
    ep.snapshot.snapshot_recv.is_some(),
    "the chunked partial is armed"
  );
  while ep.poll_message().is_some() {}

  // A NEW leader (term 3) catches the follower up via a LOWER single-shot snapshot(50) — it must SUPERSEDE
  // the abandoned chunked partial, clearing snapshot_recv and discarding staging.
  let lo = SnapshotMeta::new(
    Index::new(50),
    Term::new(3),
    ConfState::from_voters(std::vec![1u64, 2u64, 3u64]),
  );
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::InstallSnapshot(InstallSnapshot::new(
      Term::new(3),
      2u64,
      lo,
      encode_count_snapshot(50),
    )),
  );
  assert!(
    ep.snapshot.snapshot_recv.is_none(),
    "the legacy single-shot cleared the abandoned chunked partial"
  );
  assert!(
    matches!(&ep.snapshot.pending_install, Some((_, m, ..)) if m.last_index() == Index::new(50)),
    "the legacy single-shot installed at boundary 50"
  );
}

/// Chunk staging is VOLATILE across restart: if a store persisted a higher partial, restart must discard it
/// (snapshot_recv is reset to None, and there is no recovery API), so a post-restart LOWER snapshot from a
/// new leader can stage instead of being blocked forever by the orphaned higher staging key.
///
/// MUTATION: drop the `discard_snapshot_staging` call in `restart_inner` → the orphaned 100 staging survives
/// and the store rejects the lower 50 chunk (contiguous stays 0, assert fails).
#[test]
fn restart_discards_orphaned_durable_staging_so_a_lower_snapshot_stages() {
  use crate::{
    Config, Index, InstallSnapshot, Instant, Message, SnapshotMeta, StableStore, Term,
    conf::ConfState,
  };
  use core::time::Duration;
  let cfg = Config::try_new(
    2u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();

  // Simulate a pre-crash DURABLE partial: stage the first chunk of a boundary-100 snapshot directly in the
  // store, with NO surviving snapshot_recv (the proto is about to restart).
  let hi = SnapshotMeta::new(
    Index::new(100),
    Term::new(2),
    ConfState::from_voters(std::vec![1u64, 2u64, 3u64]),
  );
  stable
    .accept_snapshot_chunk(&hi, 200, 0, &bytes::Bytes::from_static(b"abc"))
    .unwrap();

  // RESTART: the proto must discard the orphaned durable staging.
  let mut ep = Endpoint::restart(
    cfg,
    Instant::ORIGIN,
    7,
    CountSm::default(),
    1,
    &mut log,
    &mut stable,
  );
  let d = Instant::ORIGIN;

  // A new leader (term 3) sends a LOWER snapshot(50): it must STAGE (the old 100 staging is gone), not be
  // rejected by a surviving higher staging key.
  let lo = SnapshotMeta::new(
    Index::new(50),
    Term::new(3),
    ConfState::from_voters(std::vec![1u64, 2u64, 3u64]),
  );
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::InstallSnapshot(InstallSnapshot::new_chunk(
      Term::new(3),
      2u64,
      lo,
      bytes::Bytes::from_static(b"de"),
      0,
      80,
    )),
  );
  assert_eq!(
    ep.snapshot
      .snapshot_recv
      .as_ref()
      .map(|r| (r.meta.last_index(), r.contiguous_staged)),
    Some((Index::new(50), 2)),
    "after the restart discard, the lower snapshot stages fresh — not blocked by the orphaned 100 staging"
  );
}

/// A chunk whose byte range exceeds `total_len` must FAIL-STOP, not be silently clamped by the staging
/// accumulator into a completed-but-TRUNCATED buffer that decodes a valid-looking prefix.
///
/// MUTATION: drop the range check before `accept_snapshot_chunk` → the overlong chunk clamps to [0,6),
/// completes the buffer, and decodes a 6-byte prefix instead of poisoning (assert fails).
#[test]
fn overlong_chunk_is_rejected_not_truncated() {
  use crate::{Index, InstallSnapshot, Instant, Message, SnapshotMeta, Term, conf::ConfState};
  let (mut ep, mut log, mut stable, _cfg) = follower_committed_to_3();
  let d = Instant::ORIGIN;
  let meta = SnapshotMeta::new(
    Index::new(10),
    Term::new(2),
    ConfState::from_voters(std::vec![1u64, 2u64, 3u64]),
  );
  // offset 0 + 10 bytes of data, but total_len is only 6 — out of range.
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    1u64,
    Message::InstallSnapshot(InstallSnapshot::new_chunk(
      Term::new(2),
      1u64,
      meta,
      bytes::Bytes::from_static(b"abcdefghij"),
      0,
      6,
    )),
  );
  assert!(
    ep.is_poisoned(),
    "an overlong chunk (offset + len > total_len) must fail-stop, not truncate-install"
  );
  assert_eq!(
    ep.poison_reason().map(|r| r.as_str()),
    Some("snapshot_decode")
  );
}

/// A STALE resume cursor (a reordered ack from a LARGER superseded snapshot, SET onto a peer whose current
/// blob is smaller) must NOT make the leader encode `offset > total_len` — which the follower's range check
/// would reject as a decode poison of a CORRECT follower. The transmitted offset is the CLAMPED `start`.
///
/// MUTATION: encode the raw `from` instead of `start` in `send_snapshot_chunk` → the emitted offset is
/// 1_000_000 > total_len (assert fails).
#[test]
fn stale_resume_cursor_emits_clamped_offset() {
  use crate::{Instant, Message, Term};
  let (mut ep, mut log, mut stable, _pending) = wedged_snapshot_follower(5u64, 2);
  // Simulate a stale cursor far beyond the current blob (the SET cursor accepts any reported watermark).
  if let Some(p) = ep.tracker.progress_mut(&2u64) {
    p.snapshot_acked(1_000_000);
  }
  // A HeartbeatResponse at/after the resend deadline re-sends from the cursor.
  let later = Instant::ORIGIN + ep.config.election_timeout();
  ep.handle_message(
    later,
    &mut log,
    &mut stable,
    2u64,
    Message::HeartbeatResponse(HeartbeatResponse::new(
      Term::new(1),
      2u64,
      bytes::Bytes::new(),
    )),
  );
  let resent = core::iter::from_fn(|| ep.poll_message())
    .find_map(|o| match o.message() {
      Message::InstallSnapshot(s) if o.to() == 2u64 => Some(s.clone()),
      _ => None,
    })
    .expect("a resend InstallSnapshot");
  assert!(
    resent.offset() <= resent.total_len(),
    "a stale cursor must encode a CLAMPED offset ({}) ≤ total_len ({}), never offset > total",
    resent.offset(),
    resent.total_len(),
  );
}

/// A COLD snapshot read (the store reports the blob NOT resident — `SnapshotChunkRead::Pending`) makes
/// the sender DEFER: it emits no `InstallSnapshot` and mutates no `Progress`, relying on a later re-drive
/// (the storage-ready seam / the heartbeat `resend_snapshot`). Once the read warms (`Ready`), the very
/// same send proceeds, carrying the blob's real `total_len` and the requested `offset`.
///
/// MUTATION: drop the `SnapshotChunkRead::Pending => return` arm in `send_snapshot_chunk` (e.g. fall
/// through to an empty `data`) → the cold call enqueues an InstallSnapshot and/or advances progress
/// (the cold-defer asserts fail).
#[test]
fn cold_snapshot_chunk_read_defers_the_send() {
  use crate::{Index, Message};

  let offset = 5u64;
  let (mut ep, _log, mut stable) = make_leader_with_compacted_log(offset, 2);
  // Drive peer 2 into Snapshot state for the persisted boundary, then drop anything emitted in setup so
  // the next poll observes only what THIS send produces.
  if let Some(p) = ep.tracker.progress_mut(&2u64) {
    p.become_snapshot(Index::new(offset));
  }
  while ep.poll_message().is_some() {}
  let progress_before = ep.tracker.progress(&2u64).unwrap().state();

  // COLD: the store reports the resident blob as not-yet-paged-in. The send must defer AND report NOT sent
  // — so the caller leaves the resend-pacing deadline DUE (the next heartbeat retries immediately) rather
  // than arming it and suppressing retries for a full election timeout per cold chunk.
  stable.cold_snapshot = true;
  assert_eq!(
    ep.send_snapshot_chunk(2u64, &stable, 0),
    ChunkSend::Deferred,
    "a cold (Pending) chunk read reports Deferred so the caller does not arm resend-pacing"
  );
  assert!(
    ep.poll_message().is_none(),
    "a cold snapshot_chunk read (Pending) must emit NO InstallSnapshot — the send defers"
  );
  assert_eq!(
    ep.tracker.progress(&2u64).unwrap().state(),
    progress_before,
    "a deferred (cold) send must NOT mutate the peer's Snapshot progress"
  );

  // WARM: the read resolves. The deferred send now proceeds — exactly one InstallSnapshot, carrying the
  // blob's real total_len (b"snap-data" = 9 bytes) and the requested offset 0. The send reports SENT.
  stable.cold_snapshot = false;
  assert_eq!(
    ep.send_snapshot_chunk(2u64, &stable, 0),
    ChunkSend::Sent,
    "a warm send that emits an InstallSnapshot reports Sent"
  );
  let installs: Vec<_> = core::iter::from_fn(|| ep.poll_message())
    .filter_map(|o| match o.message() {
      Message::InstallSnapshot(s) if o.to() == 2u64 => Some(s.clone()),
      _ => None,
    })
    .collect();
  assert_eq!(
    installs.len(),
    1,
    "once the cold read warms, the send proceeds — exactly one InstallSnapshot"
  );
  assert_eq!(
    installs[0].total_len(),
    9,
    "the warm send carries the blob's real total_len (b\"snap-data\")"
  );
  assert_eq!(installs[0].offset(), 0, "the warm send begins at offset 0");
  assert_eq!(
    installs[0].snapshot().last_index(),
    Index::new(offset),
    "the warm send carries the persisted snapshot boundary"
  );
}

/// The chunk cap bounds the ENCODED FRAME, not just the blob slice: a large but legal `ConfState` plus a
/// full `MAX_SNAPSHOT_CHUNK_BYTES` chunk must still encode below `MAX_FRAME_BYTES`, else the transport
/// refuses the frame and the follower wedges in catch-up.
///
/// MUTATION: drop the frame-budget `.min(frame_budget.max(1))` in `send_snapshot_chunk` → the emitted frame
/// exceeds `MAX_FRAME_BYTES` (assert fails). `#[ignore]` — allocates ~64 MiB; run with `-- --ignored`.
#[test]
#[ignore = "near the 64 MiB frame limit — allocates ~64 MiB; run with cargo test -- --ignored"]
fn chunked_install_frame_stays_under_limit_with_large_metadata() {
  use crate::{
    Config, Index, Instant, Message, OpId, SnapshotMeta, StableStore, Term, conf::ConfState,
  };
  use core::time::Duration;
  let cfg = Config::try_new(
    2u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_snapshot_chunk_bytes(crate::config::MAX_SNAPSHOT_CHUNK_BYTES);
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 7, CountSm::default());
  let mut stable = AsyncStable::default();
  // A LARGE but legal ConfState (a huge voter set, ~5 MiB encoded) + a ~60 MiB blob. Without frame-aware
  // chunk sizing, the encoded InstallSnapshot (metadata + a full MAX_SNAPSHOT_CHUNK_BYTES chunk) would
  // exceed MAX_FRAME_BYTES and the stream/QUIC transport would refuse it.
  let voters: std::vec::Vec<u64> = (0..600_000u64).collect();
  let meta = SnapshotMeta::new(Index::new(10), Term::new(1), ConfState::from_voters(voters));
  let blob = bytes::Bytes::from(std::vec![0u8; 60 * 1024 * 1024]);
  stable.submit_snapshot(OpId::new(1), meta, blob);
  if let Some(p) = ep.tracker.progress_mut(&1u64) {
    p.become_snapshot(Index::new(10));
  }
  let _ = ep.send_snapshot_chunk(1u64, &stable, 0);
  let out = core::iter::from_fn(|| ep.poll_message())
    .find(|o| matches!(o.message(), Message::InstallSnapshot(_)))
    .expect("an InstallSnapshot chunk");
  let mut buf = std::vec::Vec::new();
  crate::wire::encode_message(out.message(), &mut buf);
  assert!(
    buf.len() <= crate::wire::MAX_FRAME_BYTES,
    "the chunked InstallSnapshot frame ({} bytes) must stay within MAX_FRAME_BYTES ({}) even with large metadata",
    buf.len(),
    crate::wire::MAX_FRAME_BYTES,
  );
}

/// When the snapshot METADATA alone exceeds the frame limit (no room for even a 1-byte chunk), the snapshot
/// is UNSENDABLE — `send_snapshot_chunk` must emit NOTHING, never an oversized frame the transport refuses.
///
/// MUTATION: revert the unsendable guard to `frame_budget.max(1)` → a 1-byte chunk is enqueued in an
/// oversized frame (poll_message returns Some, assert fails). `#[ignore]` — allocates a >64 MiB ConfState.
#[test]
#[ignore = "metadata alone over the 64 MiB frame limit — allocates a ~7.5M-voter ConfState; run with -- --ignored"]
fn unsendable_oversized_metadata_emits_no_chunk() {
  use crate::{Config, Index, Instant, OpId, SnapshotMeta, StableStore, Term, conf::ConfState};
  use core::time::Duration;
  let cfg = Config::try_new(
    2u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_snapshot_chunk_bytes(crate::config::MAX_SNAPSHOT_CHUNK_BYTES);
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 7, CountSm::default());
  let mut stable = AsyncStable::default();
  // A ConfState whose ENCODED size alone exceeds MAX_FRAME_BYTES (~8M voters ≈ 72 MiB). The snapshot can
  // never ride a single frame — the metadata-only InstallSnapshot is already oversized.
  let voters: std::vec::Vec<u64> = (0..8_000_000u64).collect();
  let meta = SnapshotMeta::new(Index::new(10), Term::new(1), ConfState::from_voters(voters));
  let blob = bytes::Bytes::from(std::vec![0u8; 1024]); // the BLOB is tiny — the METADATA is what's oversized
  stable.submit_snapshot(OpId::new(1), meta, blob);
  if let Some(p) = ep.tracker.progress_mut(&1u64) {
    p.become_snapshot(Index::new(10));
  }
  let _ = ep.send_snapshot_chunk(1u64, &stable, 0);
  assert!(
    ep.poll_message().is_none(),
    "an InstallSnapshot whose metadata alone exceeds MAX_FRAME_BYTES must NOT be enqueued (no oversized frame)"
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
  assert!(ep.snapshot.pending_install.is_some());

  // The original install still completes on SnapshotWritten.
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(ep.snapshot.pending_install.is_none());
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
  assert!(ep.snapshot.pending_install.is_some());

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
    ep.snapshot.pending_install.is_none(),
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
/// MUTATION: revert `ack_watermark()` to `self.durable.durable_index` (drop the durable-snapshot max) → the
/// watermark reports 3 not 5, and the leader would stay pinned in Snapshot until the tail flushes.
#[test]
fn stale_drop_acks_durable_snapshot_boundary() {
  use crate::{AppendEntries, Entry, EntryKind, Index, Instant, Message, Term};
  let (mut ep, mut log, mut stable, _cfg) = follower_committed_to_3();
  let d = Instant::ORIGIN;
  // Deferred install at boundary 5 (blob in flight; commit still 3, so not stale at receipt).
  ep.handle_message(d, &mut log, &mut stable, 1u64, install_at(5));
  assert!(ep.snapshot.pending_install.is_some());

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
    ep.durable.durable_index,
    Index::new(3),
    "the appended tail is visible but UNFLUSHED"
  );

  // handle_storage drains SnapshotWritten(5) (the log loop drains nothing — appends are held) →
  // install_snapshot_now: 5 <= commit 7 → stale-drop, but it RECORDS the durable snapshot boundary.
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(
    ep.snapshot.pending_install.is_none(),
    "stale install dropped"
  );
  assert_eq!(
    ep.durable.durable_index,
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
  assert_eq!(ep.durable.durable_index, Index::new(7));
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
  let tail: Vec<Entry> = (4u64..=12)
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
    ep.snapshot.pending_install.is_some(),
    "a committed-but-not-yet-recoverable snapshot is deferred, not dropped"
  );

  // On SnapshotWritten the install stale-drops (10 <= commit 12) but RECORDS the durable boundary.
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(ep.snapshot.pending_install.is_none());
  assert_eq!(ep.commit, Index::new(12), "commit did NOT regress");
  assert_eq!(
    ep.durable.durable_index,
    Index::new(3),
    "no re-baseline of the still-unflushed log"
  );
  assert_eq!(
    ep.ack_watermark(),
    Index::new(10),
    "the durable snapshot boundary is now ackable → the leader is un-pinned from Snapshot"
  );
}

/// FAILS-ON-OLD: a peer REMOVED by a committed conf change while still in Snapshot
/// state can never be observed leaving it (its Progress is gone, and a dead peer sends no further
/// responses), so its resend-pacing deadline would linger for the rest of the term — and
/// add/remove churn of lagging peers would grow the map past the live peer set. The apply-time
/// membership fold must prune the map to the new membership.
#[test]
fn conf_change_removal_prunes_snapshot_resend_deadline() {
  use crate::{AppendResponse, ConfChange, ConfChangeType, Index, Instant, Message, Term};

  let offset = 5u64;
  let (mut ep, mut log, mut stable, _pending) = wedged_snapshot_follower(offset, 2);
  assert!(
    ep.snapshot.snapshot_resend_after.contains_key(&2u64),
    "the install send armed peer 2's pacing deadline"
  );
  while ep.poll_message().is_some() {}
  // The helper force-compacted the log without advancing commit/applied (its pacing tests never
  // apply anything). Make the apply cursor coherent with the compacted log — everything up to the
  // seeded tail is "already applied" — so the conf-change entry below is the next apply.
  let tail = log.last_index();
  ep.commit = tail;
  ep.applied = tail;

  // Remove the wedged peer 2. Quorum for the conf-change entry under the OLD config {1,2,3}
  // is leader self-match + peer 3's ack.
  let cc = ConfChange::new(ConfChangeType::RemoveNode, 2u64, bytes::Bytes::new());
  let idx = ep
    .propose_conf_change(Instant::ORIGIN, &mut log, &stable, cc)
    .expect("propose RemoveNode(2)");
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable); // leader self-match
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    3u64,
    Message::AppendResponse(AppendResponse::new(
      Term::new(1),
      3u64,
      false,
      Index::ZERO,
      Term::ZERO,
      idx, // peer 3 acks the ConfChange entry → quorum → commit + apply
    )),
  );
  assert!(
    !ep.tracker.is_voter(&2u64),
    "RemoveNode(2) must have applied"
  );
  assert!(
    !ep.snapshot.snapshot_resend_after.contains_key(&2u64),
    "applying the removal must prune peer 2's resend-pacing deadline (no leak across membership)"
  );
}

/// Installing a snapshot adopts the active read mode carried in its metadata (a SetReadMode compacted
/// into the snapshot) — recovered from replicated state, not the static config.
#[test]
fn install_snapshot_adopts_read_mode() {
  use crate::{Index, Instant, Message, ReadOnlyOption, Term, conf::ConfState};
  let (mut ep, mut log, mut stable) = make_follower();
  assert_eq!(
    ep.active_read_mode(),
    ReadOnlyOption::Safe,
    "the follower starts in the genesis Safe mode"
  );
  let snap_data = encode_snapshot(0);
  let meta = SnapshotMeta::new(
    Index::new(10),
    Term::new(4),
    ConfState::from_voters(std::vec![1u64, 2u64, 3u64]),
  )
  .with_read_only(ReadOnlyOption::LeaseGuard);
  let is = InstallSnapshot::new(Term::new(1), 1u64, meta, snap_data);
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::InstallSnapshot(is),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  assert_eq!(
    ep.active_read_mode(),
    ReadOnlyOption::LeaseGuard,
    "the install adopts the snapshot's carried read mode"
  );
}

/// REGRESSION: a NON-migrated node leaves SnapshotMeta.read_only ABSENT (None), so a restart from its own
/// snapshot falls back to the static config rather than pinning the active mode. The presence bit means
/// "a SetReadMode was compacted", not "whatever mode was active".
#[test]
fn non_migrated_snapshot_leaves_read_mode_absent() {
  let (_ep, _log, stable) = make_single_node_leader_with_entries(3, 3);
  let (meta, _data) = stable.snapshot().expect("a snapshot was submitted");
  assert_eq!(
    meta.read_only(),
    None,
    "a non-migrated node must leave the snapshot read_only absent (falls back to config on restart)"
  );
}

/// A MIGRATED node carries the explicit mode in its snapshot (read_only = Some), so a restart recovers it.
#[test]
fn migrated_snapshot_carries_explicit_read_mode() {
  use crate::{Config, Instant, ReadOnlyOption};
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_lease_duration(Duration::from_millis(300))
  .with_clock_drift_bound(Duration::from_millis(50))
  .with_snapshot_threshold(1);
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();
  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable);
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(ep.role().is_leader());
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}
  // Migrate to LeaseGuard; committing it past the threshold triggers a compaction.
  ep.propose_read_mode_change(d, &mut log, &stable, ReadOnlyOption::LeaseGuard)
    .expect("proposed");
  ep.handle_storage(d, &mut log, &mut stable);
  assert_eq!(ep.active_read_mode(), ReadOnlyOption::LeaseGuard);
  let (meta, _data) = stable
    .snapshot()
    .expect("a snapshot was submitted past the threshold");
  assert_eq!(
    meta.read_only(),
    Some(ReadOnlyOption::LeaseGuard),
    "a migrated node must carry the explicit mode in its snapshot"
  );
}

/// A snapshot install whose LogStore::restore does NOT re-baseline (first_index != last_index + 1
/// afterward) is a storage-contract violation: the read-view would be inconsistent with the advanced
/// commit/applied. The install must fail-stop (poison), not silently serve off a torn boundary — a
/// release-mode check, where the old debug_assert was a no-op.
#[test]
fn install_with_torn_rebaseline_poisons() {
  use crate::{Index, Instant, Message, PoisonReason, Term, conf::ConfState};
  let (mut ep, _vlog, mut stable) = make_follower();
  // A contract-violating log: its restore is a no-op, so first_index stays un-rebaselined.
  let mut log = FailTermLog::default();
  log.break_restore_rebaseline();

  let snap_data = encode_snapshot(7);
  let meta = SnapshotMeta::new(
    Index::new(10),
    Term::new(4),
    ConfState::from_voters(std::vec![1u64, 2u64, 3u64]),
  );
  let is = InstallSnapshot::new(Term::new(1), 1u64, meta, snap_data);
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::InstallSnapshot(is),
  );
  // Drive storage so the deferred install runs install_snapshot_now (where the re-baseline is checked).
  for _ in 0..4 {
    ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  }
  assert_eq!(
    ep.poison_reason(),
    Some(PoisonReason::SnapshotRebaseline),
    "a torn restore re-baseline must fail-stop the install, not be silently accepted"
  );
}

/// A snapshot install whose restore sets first_index correctly but RETAINS a stale suffix above the
/// boundary (last_index > n) must also fail-stop — the full postcondition, not just first_index. A
/// divergent retained suffix could later campaign and commit an entry the snapshot was meant to discard.
#[test]
fn install_with_stale_suffix_after_restore_poisons() {
  use crate::{Index, Instant, Message, PoisonReason, Term, conf::ConfState};
  let (mut ep, _vlog, mut stable) = make_follower();
  let mut log = FailTermLog::default();
  log.break_restore_keeping_suffix();

  let snap_data = encode_snapshot(7);
  let meta = SnapshotMeta::new(
    Index::new(10),
    Term::new(4),
    ConfState::from_voters(std::vec![1u64, 2u64, 3u64]),
  );
  let is = InstallSnapshot::new(Term::new(1), 1u64, meta, snap_data);
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::InstallSnapshot(is),
  );
  for _ in 0..4 {
    ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  }
  assert_eq!(
    ep.poison_reason(),
    Some(PoisonReason::SnapshotRebaseline),
    "a restore that keeps a stale suffix above the boundary must fail-stop the install"
  );
}

/// Receipt-time Log-Matching redundancy: an async follower whose DURABLE log tip outran its in-memory
/// `commit` (it persisted+acked entries before learning they were committed) receives a committed
/// snapshot at a boundary BETWEEN `commit` and `durable_index` whose boundary term MATCHES the durable
/// entry there. `on_install_snapshot` reads the `LogStore` and short-circuits AT RECEIPT via the §5.3
/// redundancy arm (`boundary <= durable_index` AND `term(boundary) == last_term`): the durable
/// `[first..=boundary]` already IS the snapshot's prefix entry-for-entry, so the snapshot is never
/// staged (no transfer, no deferred install) and re-baselining — which would only DESTROY durably-acked
/// entries above the boundary, making a leader's quorum-committed entry non-durable on this replica — is
/// avoided. The follower acks `max(commit, boundary)` so the leader lifts `match` past `pending` and the
/// peer leaves `ProgressState::Snapshot`.
///
/// MUTATION: drop the receipt guard's Log-Matching clause so `redundant` is just `boundary <=
/// min(commit, ack_watermark())`. Boundary 5 > commit 2, so the snapshot is no longer short-circuited at
/// receipt: it STAGES a deferred install (`pending_install.is_some()`) and the receipt ack at 5 is never
/// sent — both assertions FAIL.
#[test]
fn redundant_install_below_durable_tip_keeps_the_log() {
  use crate::{AppendEntries, Entry, EntryKind, Index, Instant, Message, Term};
  let (mut ep, mut log, mut stable) = make_follower();
  let d = Instant::ORIGIN;

  // Durable log tip ABOVE `commit`: entries [1..=9] at the leader's term 2 (consistent with the
  // leader — same terms), but `leader_commit = 2`, so the follower commits only up to 2 while it
  // makes the whole tail durable. (An async follower acks entries before learning they committed.)
  let tail: Vec<Entry> = (1u64..=9)
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
      Index::ZERO,
      Term::ZERO,
      tail,
      Index::new(2),
    )),
  );
  // Flush so the tail becomes durable: durable_index == 9 while commit stays at 2. (This also makes
  // term 2 durable, so the receipt ack below is sent immediately, not term-gated.)
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  assert_eq!(
    ep.durable.durable_index,
    Index::new(9),
    "tail flushed → durable=9"
  );
  assert_eq!(ep.commit, Index::new(2), "leader_commit held commit at 2");
  assert_eq!(ep.applied, Index::new(2), "applied tracks commit");
  assert_eq!(
    log.term(Index::new(5)).unwrap(),
    Term::new(2),
    "the durable entry at the boundary carries the leader's term 2"
  );

  // Install at boundary 5 (commit 2 < 5 <= durable 9), carrying last_term == 2 — the SAME term as the
  // follower's durable entry at 5. At receipt `5 <= durable_index 9` AND `term(5) == 2 == last_term`, so
  // it is SHORT-CIRCUITED: never staged, never reaching `install_snapshot_now`.
  ep.handle_message(d, &mut log, &mut stable, 1u64, install_at(5));
  assert!(
    ep.snapshot.pending_install.is_none(),
    "a snapshot already covered by the durable log is short-circuited at receipt (never staged)"
  );

  // The follower acks `max(commit 2, boundary 5) = 5` — above `pending`, so the leader lifts `match`
  // and the peer leaves Snapshot state. Capture the last SnapshotResponse from the drain.
  let mut acked: Option<crate::SnapshotResponse<u64>> = None;
  while let Some(out) = ep.poll_message() {
    if out.to() == 1u64
      && let Message::SnapshotResponse(sr) = out.message()
    {
      acked = Some(*sr);
    }
  }
  let sr = acked.expect("the short-circuit emits a SnapshotResponse to the leader");
  assert!(
    !sr.reject(),
    "the receipt short-circuit acks, never rejects"
  );
  assert_eq!(
    sr.match_index(),
    Index::new(5),
    "the follower acks max(commit, boundary) = 5 so the leader advances past `pending`"
  );

  // (a) The log is preserved — the durable tail [6..=9] survives (it was never staged or re-baselined).
  assert_eq!(
    log.last_index(),
    Index::new(9),
    "the durable tail above the boundary must survive (no re-baseline)"
  );
  assert_eq!(
    log.first_index(),
    Index::new(1),
    "the log was not re-baselined onto the snapshot boundary"
  );
  // (b) commit/applied are UNCHANGED — the snapshot was short-circuited, not applied.
  assert_eq!(
    ep.commit,
    Index::new(2),
    "a short-circuited snapshot must not move commit"
  );
  assert_eq!(
    ep.applied,
    Index::new(2),
    "a short-circuited snapshot must not move applied"
  );
  // (c) The follower's `ack_watermark()` is still the durable tip 9: nothing raised
  // `durable_snapshot_index` (no install ran), so the recoverable prefix is the durable log tail.
  assert_eq!(
    ep.ack_watermark(),
    Index::new(9),
    "no install ran, so the watermark stays at the durable tip"
  );
}

/// Completion-time Log-Matching redundancy (the IN-WINDOW catch-up case the receipt guard cannot see):
/// the snapshot's boundary is ABOVE the follower's durable tip AT RECEIPT, so the receipt short-circuit
/// does NOT fire and the install is STAGED. Then in-window AppendEntries make the matching prefix durable
/// PAST the boundary while the blob is in flight. `install_snapshot_now` re-checks at completion and DROPS
/// the install via the §5.3 arm (`boundary <= durable_index` AND `term(boundary) == last_term`): the
/// durable `[first..=boundary]` now IS the snapshot's prefix entry-for-entry, so re-baselining would only
/// DESTROY durably-acked entries above the boundary. This is the last line of defense — the receipt guard
/// covers the already-covered case, completion covers the caught-up-mid-transfer case.
///
/// MUTATION: revert BOTH the completion guard (back to `boundary <= self.commit`) AND the receipt
/// Log-Matching clause. The install is staged at receipt (boundary 5 > durable 4) and, since boundary 5 >
/// commit 2 at completion too, `log.restore(5)` truncates the durable tail — `log.last_index()` drops
/// 9 → 5, losing committed-prefix-consistent durably-acked entries — so the safety assertion FAILS.
#[test]
fn redundant_install_caught_up_mid_transfer_dropped_at_completion() {
  use crate::{AppendEntries, Entry, EntryKind, Index, Instant, Message, Term};
  let (mut ep, mut log, mut stable) = make_follower();
  let d = Instant::ORIGIN;

  // Durable prefix [1..=4] at the leader's term 2, with `leader_commit = 2`: the follower commits only
  // up to 2 while making [1..=4] durable. Crucially the durable tip (4) is BELOW the incoming boundary
  // (5), so the receipt-time guard cannot short-circuit — the install must stage.
  let head: Vec<Entry> = (1u64..=4)
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
      Index::ZERO,
      Term::ZERO,
      head,
      Index::new(2),
    )),
  );
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  assert_eq!(
    ep.durable.durable_index,
    Index::new(4),
    "head flushed → durable=4 (BELOW the incoming boundary 5)"
  );
  assert_eq!(ep.commit, Index::new(2), "leader_commit held commit at 2");

  // Install at boundary 5: `5 > durable 4` at receipt, so the receipt guard does NOT fire — the install
  // is STAGED as a deferred install. The blob is submitted but NOT yet drained (no `handle_storage` here).
  ep.handle_message(d, &mut log, &mut stable, 1u64, install_at(5));
  assert!(
    ep.snapshot.pending_install.is_some(),
    "a snapshot above the durable tip is staged at receipt (the receipt guard cannot see it as covered)"
  );

  // In-window AppendEntries extend the durable log to [5..=9] at term 2 — making the snapshot's matching
  // prefix durable PAST the boundary while the blob is still in flight. (prev = (4, term 2) so the
  // consistency check passes; the deferral window keeps the OLD log live and its appends valid.)
  let tail: Vec<Entry> = (5u64..=9)
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
      Index::new(4),
      Term::new(2),
      tail,
      Index::new(2),
    )),
  );
  // ONE drain: `handle_storage` drains the LOG queue first (the [5..=9] appends raise `durable_index` to
  // 9), THEN the STABLE queue (the snapshot's `SnapshotWritten` fires `install_snapshot_now`). So at the
  // moment the install completes, `durable_index == 9 >= boundary 5` and `term(5) == 2 == last_term` →
  // the install is DROPPED, never re-baselining over the durably-acked tail. (If the staged receive were
  // instead reclaimed earlier when the durable prefix advanced, completion would be a no-op — but the
  // SAFETY outcome below is identical regardless of which guard fired.)
  ep.handle_storage(d, &mut log, &mut stable);
  // A redundant install dropped at completion must ack a position at/above the boundary (mirror the
  // receipt-time short-circuit) so the leader leaves `ProgressState::Snapshot` without waiting for a resend.
  let mut snapshot_ack = None;
  while let Some(m) = ep.poll_message() {
    if let Message::SnapshotResponse(r) = m.message() {
      snapshot_ack = Some(r.match_index());
    }
  }
  assert!(
    ep.snapshot.pending_install.is_none(),
    "the install completed (or was reclaimed) and is no longer pending"
  );
  assert_eq!(
    snapshot_ack,
    Some(Index::new(5)),
    "the completion-time drop acks max(commit, boundary) = 5 so the leader leaves Snapshot without a resend"
  );

  // SAFETY (non-negotiable): the durable tail [5..=9] survives — the log is NOT truncated to the boundary.
  assert_eq!(
    log.last_index(),
    Index::new(9),
    "the durable tail above the boundary must survive (no re-baseline, no committed-data loss)"
  );
  assert_eq!(
    log.first_index(),
    Index::new(1),
    "the log was not re-baselined onto the snapshot boundary"
  );
  // commit/applied are UNCHANGED — the install was dropped, not applied.
  assert_eq!(
    ep.commit,
    Index::new(2),
    "a dropped install must not move commit"
  );
  assert_eq!(
    ep.applied,
    Index::new(2),
    "a dropped install must not move applied"
  );
}

/// Over-suppression guard for the redundancy arm (the must-STILL-install direction): same shape —
/// durable tip above `commit`, boundary BETWEEN them — but the follower's durable entry at the boundary
/// carries a DIFFERENT term than the snapshot's `last_term` (a divergent tail). Log Matching does NOT
/// hold, so the install must PROCEED and re-baseline onto the boundary. (`snapshot_install_resets_durable_
/// index_below_divergent_tail` exercises the same divergent-tail re-baseline from the commit==0 angle and
/// focuses on the `durable_index` RESET + the later ack-clamp; this one isolates the commit/applied/
/// first_index/last_index advance for a boundary strictly above a non-zero `commit`.)
///
/// MUTATION: this direction is unaffected by the redundancy test — both forms install — so it stays GREEN
/// under the reverted guard; it fences the redundancy test against over-suppressing a divergent install.
#[test]
fn divergent_install_below_durable_tip_still_rebaselines() {
  use crate::{AppendEntries, Entry, EntryKind, Index, Instant, Message, Term};
  let (mut ep, mut log, mut stable) = make_follower();
  let d = Instant::ORIGIN;

  // Durable tip above `commit`, but the tail is at term 1 — DIVERGENT from the term-2 snapshot below.
  let tail: Vec<Entry> = (1u64..=9)
    .map(|i| {
      Entry::new(
        Term::new(1),
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
      Term::new(1),
      1u64,
      Index::ZERO,
      Term::ZERO,
      tail,
      Index::new(2),
    )),
  );
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  assert_eq!(
    ep.durable.durable_index,
    Index::new(9),
    "tail flushed → durable=9"
  );
  assert_eq!(ep.commit, Index::new(2), "leader_commit held commit at 2");
  assert_eq!(
    log.term(Index::new(5)).unwrap(),
    Term::new(1),
    "the durable entry at the boundary carries term 1 (divergent from the term-2 snapshot)"
  );

  // Deferred install at boundary 5 carrying last_term == 2 (DIFFERENT from the durable entry's term 1).
  ep.handle_message(d, &mut log, &mut stable, 1u64, install_at(5));
  assert!(ep.snapshot.pending_install.is_some());

  // SnapshotWritten fires → redundancy test: 5 <= durable 9 but term(5)==1 != 2 == last_term → NOT
  // redundant → fall through and re-baseline onto the boundary.
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  assert!(
    ep.snapshot.pending_install.is_none(),
    "the divergent install completes (it is not redundant)"
  );

  // The log IS re-baselined to the boundary — first_index == last_index + 1 == 6, last_index == 5 —
  // and commit/applied advance to the boundary.
  assert_eq!(
    log.last_index(),
    Index::new(5),
    "re-baselined to the snapshot boundary"
  );
  assert_eq!(
    log.first_index(),
    Index::new(6),
    "first_index == boundary + 1 after restore"
  );
  assert_eq!(ep.commit, Index::new(5), "commit advances to the boundary");
  assert_eq!(
    ep.applied,
    Index::new(5),
    "applied advances to the boundary"
  );
  assert_eq!(
    ep.durable.durable_index,
    Index::new(5),
    "durable_index RESET to the boundary (the divergent tail was discarded)"
  );
}

// A fatal `term(boundary)` read during the redundancy proof is a STORAGE failure, not a mismatch — it
// must poison (`PoisonReason::LogTerm`), never silently fall through (which at receipt would stage a
// redundant transfer, and at completion would drive the destructive `log.restore` on unreadable state).
#[test]
fn snapshot_redundancy_term_read_failure_poisons_at_receipt() {
  use crate::{
    AppendEntries, Config, Entry, EntryKind, Index, Instant, Message, PoisonReason, Term,
    testkit::{AsyncStable, CountSm, FailTermLog},
  };
  use core::time::Duration;
  let cfg = Config::try_new(
    2u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 7, CountSm::default());
  let mut log = FailTermLog::default();
  let mut stable = AsyncStable::default();
  let d = Instant::ORIGIN;

  // Durable [1..=9] term 2, commit 2: durable_index=9 >= the incoming boundary 5, so the Log-Matching
  // clause is reached. Append + flush BEFORE arming the fault (the append path reads only its prev term).
  let tail: Vec<Entry> = (1u64..=9)
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
      Index::ZERO,
      Term::ZERO,
      tail,
      Index::new(2),
    )),
  );
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  assert_eq!(ep.durable.durable_index, Index::new(9));

  // Arm the fatal boundary term-read, then deliver the install: the receipt redundancy proof read fails.
  log.fail_term_at(Some(Index::new(5)));
  ep.handle_message(d, &mut log, &mut stable, 1u64, install_at(5));
  assert_eq!(
    ep.poison_reason(),
    Some(PoisonReason::LogTerm),
    "a fatal boundary term-read at receipt poisons, never silently 'not redundant'"
  );
}

#[test]
fn snapshot_redundancy_term_read_failure_poisons_at_completion() {
  use crate::{
    AppendEntries, Config, Entry, EntryKind, Index, Instant, Message, OpId, PoisonReason,
    StorageProgress, Term,
    testkit::{AsyncStable, CountSm, FailTermLog},
  };
  use core::time::Duration;
  let cfg = Config::try_new(
    2u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 7, CountSm::default());
  let mut log = FailTermLog::default();
  let mut stable = AsyncStable::default();
  let d = Instant::ORIGIN;

  // Durable [1..=4], commit 2: durable_index=4 < boundary 5 at receipt, so the install STAGES.
  let head: Vec<Entry> = (1u64..=4)
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
      Index::ZERO,
      Term::ZERO,
      head,
      Index::new(2),
    )),
  );
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  ep.handle_message(d, &mut log, &mut stable, 1u64, install_at(5));
  assert!(
    ep.snapshot.pending_install.is_some(),
    "below the boundary at receipt → stages"
  );

  // In-window appends extend the durable log past the boundary; arm the fault before the completion drains.
  let tail: Vec<Entry> = (5u64..=9)
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
      Index::new(4),
      Term::new(2),
      tail,
      Index::new(2),
    )),
  );
  log.fail_term_at(Some(Index::new(5)));
  // A deferred compaction is pending when the install completes. The install poison must FAIL-STOP the
  // storage handler BEFORE its compaction fallback runs — a poisoned node must do no destructive
  // `log.compact`. (op 99 != the install's op, so the in-loop compaction at the completion does not match;
  // only the post-loop fallback could fire, and with the durable snapshot covering up_to it WOULD — so the
  // surviving entry proves the bail skipped it.)
  ep.snapshot.pending_compact = Some((OpId::new(99), Index::new(2)));
  let progress = ep.handle_storage(d, &mut log, &mut stable);
  assert_eq!(
    ep.poison_reason(),
    Some(PoisonReason::LogTerm),
    "a fatal boundary term-read at completion poisons, never falls through to the destructive restore"
  );
  assert_eq!(
    progress,
    StorageProgress::Drained,
    "the completion-time install poison fail-stops handle_storage"
  );
  assert_eq!(
    ep.pending_compact(),
    Some((OpId::new(99), Index::new(2))),
    "the fail-stop skips the compaction fallback — no destructive log.compact after a poison"
  );
}

// A partial chunked receive stages a full `total_len` buffer. If in-window appends make the durable log
// match THROUGH the boundary while `commit` stays below it, the staged receive is redundant — and must be
// reclaimed (its buffer freed), not stranded until restart/supersede. Reclaim uses the same Log-Matching
// proof as the ack-path short-circuit, so it fires even though `boundary > commit`.
#[test]
fn redundant_staged_chunk_reclaimed_when_durable_log_catches_up() {
  use crate::{
    AppendEntries, Entry, EntryKind, Index, InstallSnapshot, Instant, Message, SnapshotMeta, Term,
    conf::ConfState,
  };
  let (mut ep, mut log, mut stable) = make_follower();
  let d = Instant::ORIGIN;

  // Durable [1..=4] term 2, commit 2: durable_index=4 < boundary 5 at receipt, so a partial chunk STAGES.
  let head: Vec<Entry> = (1u64..=4)
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
      Index::ZERO,
      Term::ZERO,
      head,
      Index::new(2),
    )),
  );
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}

  // A PARTIAL chunk at boundary 5 (offset 0, half the blob, total_len = full) stages `snapshot_recv`.
  let blob = encode_count_snapshot(5);
  let half = blob.len() / 2;
  let meta = SnapshotMeta::new(
    Index::new(5),
    Term::new(2),
    ConfState::from_voters(std::vec![1u64, 2, 3]),
  );
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    1u64,
    Message::InstallSnapshot(InstallSnapshot::new_chunk(
      Term::new(2),
      1u64,
      meta,
      blob.slice(0..half),
      0,
      blob.len() as u64,
    )),
  );
  assert!(
    ep.snapshot.snapshot_recv.is_some(),
    "a partial chunk below the durable tip stages a receive"
  );

  // In-window appends extend the durable log THROUGH the boundary at term 2; commit stays at 2.
  let tail: Vec<Entry> = (5u64..=9)
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
      Index::new(4),
      Term::new(2),
      tail,
      Index::new(2),
    )),
  );
  ep.handle_storage(d, &mut log, &mut stable);
  assert_eq!(ep.durable.durable_index, Index::new(9));
  assert_eq!(ep.commit, Index::new(2), "commit stays below the boundary");
  assert!(
    ep.snapshot.snapshot_recv.is_none(),
    "the staged receive is reclaimed once the durable log matches through the boundary (commit < boundary)"
  );
}

// A redundant LOWER-boundary snapshot from a NEWER leader must both ack that leader out of Snapshot AND
// discard an abandoned HIGHER-boundary partial staged by an OLDER leader — not strand its `total_len`
// allocation. (Reclaim alone misses it: the staged boundary is not recoverable; the supersede cleanup is.)
#[test]
fn redundant_lower_snapshot_from_newer_leader_supersedes_older_staged_receive() {
  use crate::{
    AppendEntries, Entry, EntryKind, Index, InstallSnapshot, Instant, Message, SnapshotMeta, Term,
    conf::ConfState,
  };
  let (mut ep, mut log, mut stable) = make_follower();
  let d = Instant::ORIGIN;

  // Durable [1..=9] term 2, commit 2: durable_index=9, so a boundary-5 snapshot is covered (redundant).
  let tail: Vec<Entry> = (1u64..=9)
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
      Index::ZERO,
      Term::ZERO,
      tail,
      Index::new(2),
    )),
  );
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  assert_eq!(ep.durable.durable_index, Index::new(9));

  // Stage a PARTIAL HIGHER-boundary receive from an OLDER leader term (3, boundary 20).
  let blob_hi = encode_count_snapshot(20);
  let meta_hi = SnapshotMeta::new(
    Index::new(20),
    Term::new(3),
    ConfState::from_voters(std::vec![1u64, 2, 3]),
  );
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    1u64,
    Message::InstallSnapshot(InstallSnapshot::new_chunk(
      Term::new(3),
      1u64,
      meta_hi,
      blob_hi.slice(0..blob_hi.len() / 2),
      0,
      blob_hi.len() as u64,
    )),
  );
  assert!(
    ep.snapshot.snapshot_recv.is_some(),
    "a higher-boundary partial from the older leader stages"
  );

  // A REDUNDANT LOWER-boundary install from a NEWER leader term (4, boundary 5, last_term 2 = durable term).
  let blob_lo = encode_count_snapshot(5);
  let meta_lo = SnapshotMeta::new(
    Index::new(5),
    Term::new(2),
    ConfState::from_voters(std::vec![1u64, 2, 3]),
  );
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    1u64,
    Message::InstallSnapshot(InstallSnapshot::new(Term::new(4), 1u64, meta_lo, blob_lo)),
  );

  // The stale higher-boundary staging is discarded IMMEDIATELY, not stranded until a later catch-up to 20
  // / supersede / restart.
  assert!(
    ep.snapshot.snapshot_recv.is_none(),
    "the abandoned older higher-boundary staging is discarded, not stranded"
  );
  // The newer leader is acked out of Snapshot at the boundary. The ack is term-gated (the newer leader's
  // term is not yet durable), so flush to release it, then observe it.
  ep.handle_storage(d, &mut log, &mut stable);
  let mut snapshot_ack = None;
  while let Some(m) = ep.poll_message() {
    if let Message::SnapshotResponse(r) = m.message() {
      snapshot_ack = Some(r.match_index());
    }
  }
  assert_eq!(
    snapshot_ack,
    Some(Index::new(5)),
    "the newer leader is acked out of Snapshot at the redundant boundary"
  );
}

// A chunked replacement transfer must retire a superseded `pending_install` immediately. Otherwise the old
// install's `SnapshotWritten` — delivered while the replacement is still partial — would run
// `install_snapshot_now` for the STALE snapshot, restoring/acking superseded metadata.
#[test]
fn chunked_replacement_retires_the_superseded_pending_install() {
  use crate::{
    AppendEntries, Entry, EntryKind, Index, InstallSnapshot, Instant, Message, SnapshotMeta, Term,
    conf::ConfState,
  };
  let (mut ep, mut log, mut stable) = make_follower();
  let d = Instant::ORIGIN;

  // Committed log [1..=5] term 2, commit 5.
  let entries: Vec<Entry> = (1u64..=5)
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
      Index::ZERO,
      Term::ZERO,
      entries,
      Index::new(5),
    )),
  );
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  let applied_before = ep.applied;

  // A single-shot install at boundary 8 (> commit) stages a DEFERRED pending install (NOT yet drained).
  let meta1 = SnapshotMeta::new(
    Index::new(8),
    Term::new(2),
    ConfState::from_voters(std::vec![1u64, 2, 3]),
  );
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    1u64,
    Message::InstallSnapshot(InstallSnapshot::new(
      Term::new(2),
      1u64,
      meta1,
      encode_count_snapshot(8),
    )),
  );
  assert!(
    ep.snapshot.pending_install.is_some(),
    "the single-shot install stages a deferred pending install"
  );

  // The FIRST chunk of a DIFFERENT, higher-boundary chunked transfer (boundary 10) supersedes it → the
  // pending install is retired NOW, before this replacement completes.
  let blob = encode_count_snapshot(10);
  let meta2 = SnapshotMeta::new(
    Index::new(10),
    Term::new(2),
    ConfState::from_voters(std::vec![1u64, 2, 3]),
  );
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    1u64,
    Message::InstallSnapshot(InstallSnapshot::new_chunk(
      Term::new(2),
      1u64,
      meta2,
      blob.slice(0..blob.len() / 2),
      0,
      blob.len() as u64,
    )),
  );
  assert!(
    ep.snapshot.pending_install.is_none(),
    "the chunked replacement retires the superseded pending install"
  );
  assert!(
    ep.snapshot.snapshot_recv.is_some(),
    "the replacement transfer is staged"
  );

  // The OLD install's SnapshotWritten fires but finds no matching pending install → the stale snapshot at
  // 8 is NOT installed (applied unchanged; the replacement transfer survives).
  ep.handle_storage(d, &mut log, &mut stable);
  assert_eq!(
    ep.applied, applied_before,
    "the superseded install at 8 did not run (no stale restore)"
  );
  assert!(
    ep.snapshot.snapshot_recv.is_some(),
    "the replacement transfer survives the stale completion"
  );
}

// A DEFERRED (cold) snapshot send must not arm the resend-pacing deadline: arming it would let the
// heartbeat suppress retries for a full election timeout, stalling a cold transfer one timeout per chunk.
// The peer still enters Snapshot (it needs the blob), but the deadline stays unset so the next heartbeat
// retries immediately. (The warm-send-arms case is covered by `conf_change_removal_prunes_snapshot_resend_deadline`.)
#[test]
fn cold_snapshot_send_does_not_arm_resend_pacing() {
  use crate::{Index, Instant, Now};
  let offset = 5u64;
  let (mut ep, log, mut stable) = make_leader_with_compacted_log(offset, 2);
  // Peer 2 far behind: next_index < first_index = offset + 1, so maybe_send_append takes the snapshot path.
  if let Some(p) = ep.tracker.progress_mut(&2u64) {
    p.become_probe();
    p.set_next_index(Index::new(2));
  }
  while ep.poll_message().is_some() {}

  // COLD store: the first snapshot chunk read defers (Pending).
  stable.cold_snapshot = true;
  ep.maybe_send_append(Now::monotonic(Instant::ORIGIN), 2u64, &log, &stable);
  assert!(
    ep.poll_message().is_none(),
    "a cold first chunk emits no InstallSnapshot"
  );
  assert!(
    ep.tracker.progress(&2u64).unwrap().state().is_snapshot(),
    "the peer still enters Snapshot state — it needs the blob"
  );
  assert!(
    !ep.snapshot.snapshot_resend_after.contains_key(&2u64),
    "a deferred (cold) snapshot send must NOT arm resend-pacing — the chunk was not sent"
  );
}

// A cold per-ack PUMP (progress ack → the next chunk read defers) must CLEAR the resend-pacing deadline,
// not leave the prior (future) one armed — otherwise the next heartbeat is suppressed for up to a full
// election timeout even though no chunk went out and the peer will send no further progress ack.
#[test]
fn progress_ack_cold_pump_clears_resend_pacing_so_retry_is_immediate() {
  use crate::{Index, Instant, Message, SnapshotResponse, Term};
  let offset = 5u64;
  let (mut ep, mut log, mut stable, _pending) = wedged_snapshot_follower(offset, 2);
  while ep.poll_message().is_some() {}
  assert!(
    ep.snapshot.snapshot_resend_after.contains_key(&2u64),
    "the wedged setup armed peer 2's (future) resend deadline"
  );

  stable.cold_snapshot = true;
  // Mid-transfer progress ack: match_index 0 < pending keeps the peer in Snapshot; acked_through advances
  // the cursor so the per-ack pump tries the NEXT chunk (which defers, cold).
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    2u64,
    Message::SnapshotResponse(
      SnapshotResponse::new(Term::new(1), 2u64, false, Index::ZERO).with_acked_through(1),
    ),
  );
  assert!(
    ep.tracker.progress(&2u64).unwrap().state().is_snapshot(),
    "a progress ack keeps the peer in Snapshot"
  );
  assert!(
    !core::iter::from_fn(|| ep.poll_message())
      .any(|o| o.to() == 2u64 && matches!(o.message(), Message::InstallSnapshot(_))),
    "the cold per-ack pump emits no InstallSnapshot"
  );
  assert!(
    !ep.snapshot.snapshot_resend_after.contains_key(&2u64),
    "a cold per-ack pump CLEARS the resend deadline so the next heartbeat retries immediately"
  );
}

// A fatal `term` read during `reclaim_stale_snapshot_recv`'s Log-Matching proof must FAIL-STOP the whole
// storage handler — `reclaim` poisons (PoisonReason::LogTerm) and returns false, and `handle_storage` bails
// rather than continuing handler work on a poisoned node.
#[test]
fn reclaim_term_read_failure_fail_stops_handle_storage() {
  use crate::{
    AppendEntries, Entry, EntryKind, Index, InstallSnapshot, Instant, Message, PoisonReason,
    SnapshotMeta, Term,
    testkit::{AsyncStable, CountSm, FailTermLog},
  };
  let cfg = crate::Config::try_new(
    2u64,
    std::vec![1u64, 2, 3],
    core::time::Duration::from_millis(1000),
    core::time::Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 7, CountSm::default());
  let mut log = FailTermLog::default();
  let mut stable = AsyncStable::default();
  let d = Instant::ORIGIN;

  // Durable [1..=4] term 2, commit 2: durable_index=4 < boundary 5 at receipt, so a partial chunk STAGES.
  let head: Vec<Entry> = (1u64..=4)
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
      Index::ZERO,
      Term::ZERO,
      head,
      Index::new(2),
    )),
  );
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}

  // Stage a partial chunk at boundary 5 (offset 0, total_len > the partial).
  let blob = encode_count_snapshot(5);
  let meta = SnapshotMeta::new(
    Index::new(5),
    Term::new(2),
    crate::conf::ConfState::from_voters(std::vec![1u64, 2, 3]),
  );
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    1u64,
    Message::InstallSnapshot(InstallSnapshot::new_chunk(
      Term::new(2),
      1u64,
      meta,
      blob.slice(0..blob.len() / 2),
      0,
      blob.len() as u64,
    )),
  );
  assert!(
    ep.snapshot.snapshot_recv.is_some(),
    "the partial chunk staged"
  );

  // In-window appends extend the durable log THROUGH the boundary; arm the fatal boundary term-read so the
  // reclaim Log-Matching proof (boundary 5 <= durable 9) hits it.
  let tail: Vec<Entry> = (5u64..=9)
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
      Index::new(4),
      Term::new(2),
      tail,
      Index::new(2),
    )),
  );
  log.fail_term_at(Some(Index::new(5)));
  ep.handle_storage(d, &mut log, &mut stable);
  assert_eq!(
    ep.poison_reason(),
    Some(PoisonReason::LogTerm),
    "a fatal term-read in reclaim fail-stops the storage handler"
  );
}

// The sender now TRUSTS the snapshot_chunk API (it no longer locally slices a resident blob), so it must
// validate the store's Ready chunk: an in-range Ready(empty) is a contract violation (empty is EOF-only)
// that would wedge on infinite empty resends → poison; overlong bytes (past total_len) are clamped so a
// correct follower never decode-poisons on an out-of-range chunk.
#[test]
fn send_snapshot_chunk_validates_malformed_store_ready() {
  use crate::{Index, Message, PoisonReason};
  let offset = 5u64;
  // In-range Ready(empty) → fatal store-contract violation → Poisoned.
  {
    let (mut ep, _log, mut stable) = make_leader_with_compacted_log(offset, 2);
    if let Some(p) = ep.tracker.progress_mut(&2u64) {
      p.become_snapshot(Index::new(offset));
    }
    while ep.poll_message().is_some() {}
    stable.malformed_in_range_empty = true;
    assert_eq!(
      ep.send_snapshot_chunk(2u64, &stable, 0),
      ChunkSend::Poisoned,
      "an in-range Ready(empty) is a contract violation (empty is EOF-only) and must poison"
    );
    assert_eq!(ep.poison_reason(), Some(PoisonReason::SnapshotRead));
  }
  // Overlong Ready (more bytes than remain) → clamped to within total_len, still Sent.
  {
    let (mut ep, _log, mut stable) = make_leader_with_compacted_log(offset, 2);
    if let Some(p) = ep.tracker.progress_mut(&2u64) {
      p.become_snapshot(Index::new(offset));
    }
    while ep.poll_message().is_some() {}
    stable.malformed_overlong = true;
    assert_eq!(ep.send_snapshot_chunk(2u64, &stable, 0), ChunkSend::Sent);
    let install = core::iter::from_fn(|| ep.poll_message())
      .find_map(|o| match o.message() {
        Message::InstallSnapshot(s) if o.to() == 2u64 => Some(s.clone()),
        _ => None,
      })
      .expect("an InstallSnapshot was sent");
    assert!(
      install.offset() + install.data().len() as u64 <= install.total_len(),
      "the overlong chunk was clamped to within total_len — no out-of-range chunk reaches the follower"
    );
  }
}
