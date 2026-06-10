use super::*;

#[test]
fn restart_replays_committed_log() {
  use crate::{Config, Entry, EntryKind, Index, Instant, Term};
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::NoopStable::default();
  // Seed the stores as if a prior incarnation had committed 2 Normal entries.
  log.force_append(&[
    Entry::new(
      Term::new(1),
      Index::new(1),
      EntryKind::Normal,
      encode_cmd(b"a"),
    ),
    Entry::new(
      Term::new(1),
      Index::new(2),
      EntryKind::Normal,
      encode_cmd(b"b"),
    ),
  ]);
  stable.force_state(Term::new(1), Some(1u64), Index::new(2)); // term=1, vote=1, commit=2

  let ep = Endpoint::restart(
    cfg,
    Instant::ORIGIN,
    7,
    crate::testkit::CountSm::default(),
    1, // boot_epoch (this incarnation > the prior one)
    &mut log,
    &mut stable,
  );
  assert_eq!(ep.term(), Term::new(1));
  assert_eq!(ep.state_machine().count(), 2); // both committed entries replayed
  assert!(ep.role().is_follower());
  // election timer must be armed
  assert!(ep.poll_timeout().is_some());
}

/// Apply-time membership regression (etcd, spec §9): on restart the configuration is reconstructed
/// from the COMMITTED log prefix only — `apply_committed` re-folds the committed ConfChanges — and an
/// UNCOMMITTED ConfChange in the log tail does NOT take effect. So `conf_state()` after restart is
/// exactly the committed voter set, never an uncommitted one (the configuration follows the
/// APPLIED prefix, not the raw log).
///
/// Scenario: genesis is the 5-voter cluster {1,2,3,4,5}. The durable log holds two RemoveNode
/// conf-changes — drop 4 at index 1 (COMMITTED, commit=1) and drop 5 at index 2 (UNCOMMITTED). The
/// reconstructed config must be {1,2,3,5}: drop-4 applied, drop-5 ignored.
#[test]
fn restart_reconstructs_committed_config_ignoring_uncommitted_tail() {
  use crate::{
    ConfChange, ConfChangeType, Config, Data as _, Entry, EntryKind, Index, Instant, Term,
  };
  use core::time::Duration;

  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3, 4, 5],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::NoopStable::default();

  let remove = |node: u64| -> bytes::Bytes {
    let cc = ConfChange::new(ConfChangeType::RemoveNode, node, bytes::Bytes::new()).into_v2();
    let mut buf = std::vec::Vec::new();
    cc.encode(&mut buf);
    bytes::Bytes::from(buf)
  };

  // Durable log: drop 4 (index 1, committed) then drop 5 (index 2, uncommitted).
  log.force_append(&[
    Entry::new(
      Term::new(1),
      Index::new(1),
      EntryKind::ConfChange,
      remove(4),
    ),
    Entry::new(
      Term::new(1),
      Index::new(2),
      EntryKind::ConfChange,
      remove(5),
    ),
  ]);
  // commit = 1: drop-4 is committed; drop-5 is an uncommitted tail entry.
  stable.force_state(Term::new(1), None, Index::new(1));

  let ep = Endpoint::restart(
    cfg,
    Instant::ORIGIN,
    7,
    crate::testkit::CountSm::default(),
    1, // boot_epoch (this incarnation > the prior one)
    &mut log,
    &mut stable,
  );

  // Reconstructed config is {1,2,3,5}: the COMMITTED drop-4 took effect (apply_committed re-folded
  // it); the UNCOMMITTED drop-5 did NOT — apply-time never folds an uncommitted entry.
  assert!(ep.tracker.is_voter(&1u64) && ep.tracker.is_voter(&2u64) && ep.tracker.is_voter(&3u64));
  assert!(
    !ep.tracker.is_voter(&4u64),
    "committed RemoveNode(4) must be reconstructed on restart"
  );
  assert!(
    ep.tracker.is_voter(&5u64),
    "uncommitted RemoveNode(5) must NOT take effect (apply-time: config == committed prefix)"
  );
}

/// A node that commits+applies entries [1..N] through the REAL path
/// (self-elect → propose → handle_storage drains the append, advances commit, applies, AND
/// now persists the commit watermark to HardState) must, after a `restart` from the SAME
/// stores with NO snapshot, recover `commit == N`, `applied == N`, and a state machine that
/// reflects all N applied entries — NOT an empty SM.
///
/// FAILS ON OLD CODE: without the handle_storage commit-persist (and the with_commit stamps),
/// the durable HardState.commit stays Index::ZERO for the node's life, so restart computes
/// `commit = min(0, last_index).max(0) = 0`, the replay loop (0..0] is empty, and the
/// restarted node recovers commit=0 with an EMPTY state machine despite the durable log
/// holding all N committed entries.
#[test]
fn restart_recovers_commit_persisted_via_real_path() {
  use crate::{Config, Index, Instant};
  use core::time::Duration;
  // 1-voter cluster: quorum == 1, so a lone node self-elects and commits on storage drain.
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut log = crate::testkit::VecLog::default();
  // AsyncStable enqueues a Wrote completion for every submit_write, so handle_storage also
  // drains the commit-watermark completion (verifying it passes harmlessly through
  // on_stable_wrote with no Pending entry). Both testkit stores persist synchronously, so
  // the durable HardState reflects each write immediately.
  let mut stable = crate::testkit::AsyncStable::default();
  let mut ep = Endpoint::new(
    cfg.clone(),
    Instant::ORIGIN,
    7,
    crate::testkit::CountSm::default(),
  );

  // Self-elect (quorum == 1) and let the no-op LeaderAppend commit.
  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable);
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(ep.role().is_leader(), "lone voter must self-elect");
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  // Propose N Normal entries through the real path; drain storage after each so it commits
  // and applies (and, with the fix, persists the advanced commit watermark). The command
  // bytes are irrelevant to CountSm (it just counts applies); use fixed distinct payloads.
  let cmds: [&[u8]; 4] = [b"c0", b"c1", b"c2", b"c3"];
  const N: usize = 4;
  for cmd in cmds {
    ep.propose(d, &mut log, &stable, &bytes::Bytes::copy_from_slice(cmd))
      .unwrap();
    ep.handle_storage(d, &mut log, &mut stable);
    while ep.poll_message().is_some() {}
    while ep.poll_event().is_some() {}
  }
  assert!(!ep.is_poisoned(), "node must not be poisoned");
  // SM reflects N applied Normal entries (the leader's term-start no-op is Empty, not counted).
  assert_eq!(
    ep.state_machine().count(),
    N,
    "live leader must have applied all N proposed entries"
  );
  // The durable HardState.commit must now reflect the advanced watermark (the fix). The log
  // holds the no-op at index 1 plus N Normal entries, so commit == N + 1.
  let expected_commit = Index::new(N as u64 + 1);
  assert_eq!(
    stable.hard_state().commit(),
    expected_commit,
    "handle_storage must persist the advanced commit watermark into HardState"
  );

  // Restart from the SAME log + stable with NO snapshot.
  let restarted = Endpoint::restart(
    cfg,
    Instant::ORIGIN,
    9,
    crate::testkit::CountSm::default(),
    1, // boot_epoch (this incarnation > the prior one)
    &mut log,
    &mut stable,
  );
  assert!(
    !restarted.is_poisoned(),
    "restarted node must not be poisoned"
  );
  assert_eq!(
    restarted.commit, expected_commit,
    "restart must recover the durable commit watermark, not collapse to applied/0"
  );
  assert_eq!(
    restarted.applied, expected_commit,
    "restart must replay the committed tail so applied catches up to commit"
  );
  assert_eq!(
    restarted.state_machine().count(),
    N,
    "restarted SM must reflect all N committed entries, not be empty"
  );
}

// ---- LogStore::restore unit tests ----

/// After `restore(10, 4)` on a VecLog with arbitrary prior content, the log has the
/// expected re-baseline invariants.
#[test]
fn veclog_restore_rebaselines_correctly() {
  use crate::{Entry, EntryKind, Index, Term};

  let mut log = crate::testkit::VecLog::default();

  // Seed with entries 1..=5 at term 1.
  let entries: std::vec::Vec<_> = (1u64..=5)
    .map(|i| {
      Entry::new(
        Term::new(1),
        Index::new(i),
        EntryKind::Normal,
        bytes::Bytes::new(),
      )
    })
    .collect();
  log.submit_append(crate::OpId::new(1), &entries);
  let _ = log.poll(); // drain completion

  // Restore to last_index=10, last_term=4 (simulating a received snapshot).
  log.restore(Index::new(10), Term::new(4));

  assert_eq!(
    log.first_index(),
    Index::new(11),
    "first_index must be last_index + 1"
  );
  assert_eq!(
    log.last_index(),
    Index::new(10),
    "last_index must equal the snapshot boundary"
  );
  assert_eq!(
    log.term(Index::new(10)).unwrap(),
    Term::new(4),
    "term(last_index) must equal last_term"
  );
  // No entries above last_index.
  assert!(
    log
      .entries(Index::new(11)..Index::new(11), u64::MAX)
      .unwrap()
      .is_empty(),
    "entries(11..11) must be empty after restore"
  );
  // No stale completions should leak out.
  assert!(log.poll().is_none(), "no pending completions after restore");
}

/// After `restore` a subsequent `submit_append` of index 11 works correctly.
#[test]
fn veclog_submit_append_after_restore() {
  use crate::{Entry, EntryKind, Index, Term};

  let mut log = crate::testkit::VecLog::default();

  // Seed and restore to last_index=10, last_term=4.
  log.restore(Index::new(10), Term::new(4));

  // Append index 11 at term 5.
  let e = Entry::new(
    Term::new(5),
    Index::new(11),
    EntryKind::Normal,
    bytes::Bytes::from_static(b"next"),
  );
  log.submit_append(crate::OpId::new(1), core::slice::from_ref(&e));
  let _ = log.poll(); // drain

  assert_eq!(
    log.last_index(),
    Index::new(11),
    "last_index must be 11 after appending entry 11"
  );
  assert_eq!(
    log.term(Index::new(11)).unwrap(),
    Term::new(5),
    "term(11) must be 5"
  );
  // Boundary term still accessible.
  assert_eq!(
    log.term(Index::new(10)).unwrap(),
    Term::new(4),
    "boundary term must be retained"
  );
}

/// Test 1: restart with a durable snapshot + post-snapshot committed tail.
/// SM must reflect snapshot-baseline PLUS replayed entries 6 and 7.
/// applied==7, commit==7, not poisoned.
#[test]
fn restart_restores_snapshot_then_replays_tail() {
  use crate::{Config, Entry, EntryKind, Index, Instant, Term, conf::ConfState};
  use core::time::Duration;

  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();

  // Build a durable stable: snapshot at last_index=5, last_term=2, SM count=10.
  let mut stable = crate::testkit::AsyncStable::default();
  let snap_count: u64 = 10;
  let snap_data = encode_count_snapshot(snap_count);
  let meta = crate::SnapshotMeta::new(
    Index::new(5),
    Term::new(2),
    ConfState::from_voters(std::vec![1u64]),
  );
  stable.submit_snapshot(crate::OpId::new(1), meta, snap_data);
  // Drain the SnapshotWritten completion so stable.snapshot() is readable.
  while stable.poll().is_some() {}

  // Set HardState: term=2, commit=7 (two entries past the snapshot).
  stable.force_state(Term::new(2), None, Index::new(7));

  // Build a durable log: compacted to baseline 5, entries 6 and 7 present.
  let mut log = crate::testkit::VecLog::default();
  // Restore the log to the snapshot baseline (offset=5, compacted_term=2).
  log.restore(Index::new(5), Term::new(2));
  // Force-append entries 6 and 7 (post-snapshot tail).
  // Entry data must be length-prefixed (the CountSm uses Bytes::decode, which requires
  // an 8-byte LE length prefix followed by the raw payload).
  log.force_append(&[
    Entry::new(
      Term::new(2),
      Index::new(6),
      EntryKind::Normal,
      encode_cmd(b"cmd6"),
    ),
    Entry::new(
      Term::new(2),
      Index::new(7),
      EntryKind::Normal,
      encode_cmd(b"cmd7"),
    ),
  ]);

  // Restart the node.
  let ep = Endpoint::restart(
    cfg,
    Instant::ORIGIN,
    42,
    crate::testkit::CountSm::default(),
    1, // boot_epoch (this incarnation > the prior one)
    &mut log,
    &mut stable,
  );

  // SM must be the snapshot baseline (10) + 2 replayed entries = 12.
  assert_eq!(
    ep.state_machine().count() as u64,
    snap_count + 2,
    "SM must equal snapshot baseline + 2 replayed tail entries"
  );
  assert_eq!(ep.applied, Index::new(7), "applied must be 7");
  assert_eq!(ep.commit, Index::new(7), "commit must be 7");
  assert!(!ep.is_poisoned(), "node must not be poisoned");
}

/// Test 2: restart with snapshot only, no post-snapshot tail.
/// SM == snapshot state, applied==commit==5.
#[test]
fn restart_restores_snapshot_no_tail() {
  use crate::{Config, Index, Instant, Term, conf::ConfState};
  use core::time::Duration;

  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();

  let mut stable = crate::testkit::AsyncStable::default();
  let snap_count: u64 = 7;
  let snap_data = encode_count_snapshot(snap_count);
  let meta = crate::SnapshotMeta::new(
    Index::new(5),
    Term::new(2),
    ConfState::from_voters(std::vec![1u64]),
  );
  stable.submit_snapshot(crate::OpId::new(1), meta, snap_data);
  while stable.poll().is_some() {}
  stable.force_state(Term::new(2), None, Index::new(5));

  // Log baseline = 5, no entries above it.
  let mut log = crate::testkit::VecLog::default();
  log.restore(Index::new(5), Term::new(2));

  let ep = Endpoint::restart(
    cfg,
    Instant::ORIGIN,
    42,
    crate::testkit::CountSm::default(),
    1, // boot_epoch (this incarnation > the prior one)
    &mut log,
    &mut stable,
  );

  assert_eq!(
    ep.state_machine().count() as u64,
    snap_count,
    "SM must equal the snapshot state"
  );
  assert_eq!(ep.applied, Index::new(5), "applied must be 5");
  assert_eq!(ep.commit, Index::new(5), "commit must be 5");
  assert!(!ep.is_poisoned(), "node must not be poisoned");
}

/// Test 3: no snapshot (regression) — replay-from-1 still works when
/// stable.snapshot() is None and the log starts at 1.
///
/// Drives the REAL commit-persist path (a live single-node leader) instead of
/// `force_state`-injecting the durable commit. This makes the no-snapshot restart
/// suite genuinely exercise the handle_storage commit-watermark write: the live leader's
/// `commit` reaches HardState only because of the fix, and the restart reads it back.
#[test]
fn restart_no_snapshot_replays_from_one() {
  use crate::{Config, Index, Instant};
  use core::time::Duration;

  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();

  // No snapshot. Drive a live single-node leader so commit advances and is persisted to
  // HardState by the handle_storage choke-point (no force_state injection).
  let mut stable = crate::testkit::AsyncStable::default();
  let mut log = crate::testkit::VecLog::default();
  let mut ep = Endpoint::new(
    cfg.clone(),
    Instant::ORIGIN,
    42,
    crate::testkit::CountSm::default(),
  );

  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable); // self-elect (quorum == 1)
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(ep.role().is_leader());
  ep.handle_storage(d, &mut log, &mut stable); // no-op LeaderAppend at index 1 commits
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  // Propose 2 Normal entries (indices 2 and 3); drain storage so each commits and applies.
  for b in [b"a".as_slice(), b"b".as_slice()] {
    ep.propose(d, &mut log, &stable, &bytes::Bytes::copy_from_slice(b))
      .unwrap();
    ep.handle_storage(d, &mut log, &mut stable);
    while ep.poll_message().is_some() {}
    while ep.poll_event().is_some() {}
  }
  assert_eq!(
    ep.state_machine().count(),
    2,
    "two Normal entries applied pre-restart"
  );
  assert_eq!(
    ep.commit,
    Index::new(3),
    "commit must reach 3 (no-op + 2 Normal)"
  );
  // The fix: commit watermark is durable, so restart can recover it.
  assert_eq!(stable.hard_state().commit(), Index::new(3));

  let ep = Endpoint::restart(
    cfg,
    Instant::ORIGIN,
    42,
    crate::testkit::CountSm::default(),
    1, // boot_epoch (this incarnation > the prior one)
    &mut log,
    &mut stable,
  );

  // 2 Normal entries applied (entry 1 is Empty/noop).
  assert_eq!(ep.state_machine().count(), 2, "two Normal entries applied");
  assert_eq!(ep.applied, Index::new(3), "applied must be 3");
  assert_eq!(ep.commit, Index::new(3), "commit must be 3");
  assert!(!ep.is_poisoned(), "node must not be poisoned");
}

/// Test 4: corrupt durable snapshot data poisons the node; no partial apply.
#[test]
fn restart_corrupt_snapshot_poisons_node() {
  use crate::{Config, Index, Instant, Term, conf::ConfState};
  use core::time::Duration;

  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();

  let mut stable = crate::testkit::AsyncStable::default();
  // Store garbage — too short to decode a u64 count.
  let bad_data = bytes::Bytes::from_static(b"\x01\x02\x03");
  let meta = crate::SnapshotMeta::new(
    Index::new(5),
    Term::new(2),
    ConfState::from_voters(std::vec![1u64]),
  );
  stable.submit_snapshot(crate::OpId::new(1), meta, bad_data);
  while stable.poll().is_some() {}
  stable.force_state(Term::new(2), None, Index::new(7));

  let mut log = crate::testkit::VecLog::default();
  log.restore(Index::new(5), Term::new(2));

  let ep = Endpoint::restart(
    cfg,
    Instant::ORIGIN,
    42,
    crate::testkit::CountSm::default(),
    1, // boot_epoch (this incarnation > the prior one)
    &mut log,
    &mut stable,
  );

  assert!(
    ep.is_poisoned(),
    "node must be poisoned after corrupt snapshot"
  );
  // Applied must not have advanced past the snapshot boundary (no partial apply).
  assert_eq!(
    ep.state_machine().count(),
    0,
    "SM must be empty after corrupt snapshot (no partial apply)"
  );
}

/// Regression (`restart` validates the durable snapshot `ConfState`): recovering from a
/// corrupt-on-disk or version-skewed snapshot whose membership is impossible (here empty voters)
/// must poison rather than recover into an unquorable configuration. The ConfState is checked
/// before the SM is even decoded, so the data itself is irrelevant.
///
/// MUTATION: drop the `meta.conf().is_valid()` gate in `restart` → the node recovers with an empty
/// voter set and is not poisoned.
#[test]
fn restart_with_invalid_snapshot_conf_state_poisons() {
  use crate::{Config, Index, Instant, Term, conf::ConfState};
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();

  let mut stable = crate::testkit::AsyncStable::default();
  // Durable snapshot with an INVALID ConfState (empty voters); the data is never reached.
  let bad_meta = crate::SnapshotMeta::new(
    Index::new(5),
    Term::new(2),
    ConfState::<u64>::from_voters(std::vec![]),
  );
  stable.submit_snapshot(
    crate::OpId::new(1),
    bad_meta,
    bytes::Bytes::from_static(b"anything"),
  );
  while stable.poll().is_some() {}
  stable.force_state(Term::new(2), None, Index::new(7));

  let mut log = crate::testkit::VecLog::default();
  log.restore(Index::new(5), Term::new(2));

  let ep = Endpoint::restart(
    cfg,
    Instant::ORIGIN,
    42,
    crate::testkit::CountSm::default(),
    1, // boot_epoch (this incarnation > the prior one)
    &mut log,
    &mut stable,
  );

  assert!(
    ep.is_poisoned(),
    "restart from an invalid snapshot ConfState must poison"
  );
  assert_eq!(
    ep.poison_reason().map(|r| r.as_str()),
    Some("invalid_conf_state")
  );
  assert_eq!(
    ep.state_machine().count(),
    0,
    "no SM restore on an invalid-ConfState snapshot"
  );
}

/// Regression (`restart` fail-stops on a reserved-sentinel snapshot WITHOUT mutating the SM):
/// a durable snapshot whose `last_index` is the reserved sentinel `u64::MAX` is corrupt/version-
/// skewed (a correct leader reserves the sentinel and never snapshots at it; followers reject
/// installing it). An earlier guard already poisoned on such a snapshot — but did so in the post-restore
/// reconciliation guard, AFTER `F::Snapshot::decode` + `fsm.restore` had already mutated the state
/// machine. The fail-stop MUST be side-effect-free: the sentinel boundary is now rejected ahead of
/// decode/restore (beside the conf-validity gate), so `restore` never runs. Asserts the node poisons
/// (`log_exhausted`) AND the SM is untouched — `CountSm.count` stays 0; a restore from the blob
/// would have set it to 99.
///
/// MUTATION: move the `meta.last_index().get() == u64::MAX` check back below the decode/restore
/// branch (the post-restore placement) → the node still poisons, but `count() == 99` (restore ran first).
#[test]
fn restart_sentinel_snapshot_poisons_without_restoring_sm() {
  use crate::{Config, Index, Instant, Term, conf::ConfState};
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();

  let mut stable = crate::testkit::AsyncStable::default();
  // Durable snapshot at the RESERVED SENTINEL boundary (last_index == u64::MAX) with a VALID
  // ConfState (so the sentinel gate — not the conf gate — is what poisons) and a blob that decodes
  // to count=99 (so a restore, if it wrongly ran, is observable as count==99).
  let snap_data = encode_count_snapshot(99);
  let bad_meta = crate::SnapshotMeta::new(
    Index::new(u64::MAX),
    Term::new(2),
    ConfState::from_voters(std::vec![1u64]),
  );
  stable.submit_snapshot(crate::OpId::new(1), bad_meta, snap_data);
  while stable.poll().is_some() {}
  stable.force_state(Term::new(2), None, Index::new(0));

  // Empty durable log: the snapshot boundary is rejected before reconciliation, so the log is never
  // consulted here — this isolates the SNAPSHOT-boundary sentinel (the log-boundary sentinel has its
  // own regression).
  let mut log = crate::testkit::VecLog::default();

  let ep = Endpoint::restart(
    cfg,
    Instant::ORIGIN,
    42,
    crate::testkit::CountSm::default(),
    1, // boot_epoch (this incarnation > the prior one)
    &mut log,
    &mut stable,
  );

  assert!(
    ep.is_poisoned(),
    "restart from a reserved-sentinel snapshot boundary must poison"
  );
  assert_eq!(
    ep.poison_reason().map(|r| r.as_str()),
    Some("log_exhausted")
  );
  assert_eq!(
    ep.state_machine().count(),
    0,
    "fail-stop must be side-effect-free: the SM must NOT be restored from a sentinel snapshot"
  );
}

/// Regression (`restart` fail-stops on an orphaned re-baselined log): the snapshot-install
/// durability window. If a crash leaves the log re-baselined (`first_index() > 1`, the `restore`
/// re-baseline reached disk) but the snapshot blob never became durable, the committed prefix below
/// `first_index` is gone. `restart` must NOT bootstrap from the static config and serve that log as
/// if its prefix were intact (which silently discards committed entries and corrupts apply state);
/// it must fail-stop. A conforming `LogStore` orders the re-baseline durability after the blob so
/// this never happens, but the core defends against a contract violation / disk corruption.
///
/// MUTATION: drop the `else if log.first_index() > Index::new(1)` guard in `restart` → the node
/// bootstraps from the static config with an orphaned log and is not poisoned.
#[test]
fn restart_orphaned_log_without_snapshot_poisons() {
  use crate::{Config, Index, Instant, Term};
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();

  // No durable snapshot is submitted (simulating a crash after `restore` reached disk but before
  // the snapshot blob): `stable.snapshot()` is None.
  let mut stable = crate::testkit::AsyncStable::default();
  stable.force_state(Term::new(2), None, Index::new(7));

  // The log was re-baselined to last_index=5 (first_index becomes 6) with nothing backing it.
  let mut log = crate::testkit::VecLog::default();
  log.restore(Index::new(5), Term::new(2));
  assert!(log.first_index() > Index::new(1), "log is re-baselined");

  let ep = Endpoint::restart(
    cfg,
    Instant::ORIGIN,
    42,
    crate::testkit::CountSm::default(),
    1, // boot_epoch (this incarnation > the prior one)
    &mut log,
    &mut stable,
  );

  assert!(
    ep.is_poisoned(),
    "a re-baselined log with no durable snapshot must fail-stop, not bootstrap from static config"
  );
  assert_eq!(ep.poison_reason().map(|r| r.as_str()), Some("orphaned_log"));
}

/// Regression (`restart` REPAIRS the log behind a durable snapshot): the EXPECTED
/// conforming-store crash window. `LogStore::restore` orders the re-baseline durability AFTER the
/// snapshot blob, so a crash can leave the blob durable while the log re-baseline never reached
/// disk — the log is then behind the snapshot (`first_index <= snapshot index`). The snapshot IS
/// durable, so restart must RECOVER (re-run `restore`), not fail-stop: poisoning here would kill a
/// node whose store followed the contract. Here the durable snapshot is at index 5 but the log is
/// fresh (never re-baselined); restart completes the re-baseline and comes up healthy at the
/// snapshot baseline.
///
/// MUTATION: drop the `if log.first_index() <= snap_idx { log.restore(..) }` repair in `restart` →
/// the node comes up with `applied=5`/`commit=5` but a fresh, un-rebaselined log (`first_index=1`),
/// so the `first_index == 6` assertion fails.
#[test]
fn restart_log_behind_durable_snapshot_repairs() {
  use crate::{Config, Index, Instant, Term, conf::ConfState};
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();

  let mut stable = crate::testkit::AsyncStable::default();
  let snap_data = encode_count_snapshot(10);
  let meta = crate::SnapshotMeta::new(
    Index::new(5),
    Term::new(2),
    ConfState::from_voters(std::vec![1u64]),
  );
  stable.submit_snapshot(crate::OpId::new(1), meta, snap_data);
  while stable.poll().is_some() {}
  stable.force_state(Term::new(2), None, Index::new(5));

  // The log re-baseline never reached disk: a FRESH log (first_index=1), behind the snapshot at 5.
  let mut log = crate::testkit::VecLog::default();
  assert!(log.first_index() <= Index::new(5));

  let ep = Endpoint::restart(
    cfg,
    Instant::ORIGIN,
    42,
    crate::testkit::CountSm::default(),
    1, // boot_epoch (this incarnation > the prior one)
    &mut log,
    &mut stable,
  );

  assert!(
    !ep.is_poisoned(),
    "a durable snapshot with a behind log must RECOVER (re-baseline), not fail-stop"
  );
  assert_eq!(
    ep.applied,
    Index::new(5),
    "applied at the snapshot boundary"
  );
  assert_eq!(ep.commit, Index::new(5), "commit at the snapshot boundary");
  assert_eq!(
    ep.state_machine().count(),
    10,
    "SM restored to the snapshot baseline"
  );
  // The log was repaired to the snapshot baseline.
  assert_eq!(
    log.first_index(),
    Index::new(6),
    "log re-baselined to snapshot+1"
  );
}

/// Regression (`restart` fail-stops when the log is compacted PAST a durable snapshot):
/// if the durable snapshot is at index 5 but the log has been compacted beyond it
/// (`first_index > 6`), the committed prefix between the snapshot and the log baseline has no
/// snapshot to cover it — a conforming store keeps a snapshot at or above its compaction point, so
/// this is corruption / a lost newer snapshot. Restart must poison rather than serve a log whose
/// prefix is gone.
///
/// MUTATION: drop the `else if log.first_index() > snap_idx.next()` poison in `restart` → the node
/// comes up serving a log with an uncovered committed prefix.
#[test]
fn restart_log_compacted_past_snapshot_poisons() {
  use crate::{Config, Index, Instant, Term, conf::ConfState};
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();

  let mut stable = crate::testkit::AsyncStable::default();
  let snap_data = encode_count_snapshot(10);
  let meta = crate::SnapshotMeta::new(
    Index::new(5),
    Term::new(2),
    ConfState::from_voters(std::vec![1u64]),
  );
  stable.submit_snapshot(crate::OpId::new(1), meta, snap_data);
  while stable.poll().is_some() {}
  stable.force_state(Term::new(2), None, Index::new(9));

  // The log is compacted to baseline 8 (first_index=9), PAST the snapshot at 5.
  let mut log = crate::testkit::VecLog::default();
  log.restore(Index::new(8), Term::new(2));
  assert!(log.first_index() > Index::new(6));

  let ep = Endpoint::restart(
    cfg,
    Instant::ORIGIN,
    42,
    crate::testkit::CountSm::default(),
    1, // boot_epoch (this incarnation > the prior one)
    &mut log,
    &mut stable,
  );

  assert!(
    ep.is_poisoned(),
    "a log compacted past the durable snapshot must fail-stop"
  );
  assert_eq!(ep.poison_reason().map(|r| r.as_str()), Some("orphaned_log"));
}

/// Regression (the log-behind-snapshot repair must NOT truncate a committed tail): `first_index <= N` also
/// arises in the LOCAL-snapshot compaction window — the node snapshotted its OWN log at `N`, the
/// blob is durable, but the deferred `compact(N)` has not run, so the log still holds the committed
/// tail above `N`. Recovery here must `compact` (preserving the tail), NOT `restore` (which would
/// delete committed entries `N+1..C` — a safety violation). Here the durable snapshot is at 5 with a
/// durable, NOT-yet-compacted log holding entries 1..=7 and `HardState.commit=7`; restart must
/// compact through 5 and replay the committed tail 6,7.
///
/// MUTATION: collapse the recovery branch back to an unconditional `log.restore(snap_idx, ..)` (the
/// repair bug) → `restore(5)` deletes entries 6,7 and the node rolls back to the snapshot boundary
/// (applied=5, count=10, last_index=5) instead of replaying the committed tail.
#[test]
fn restart_local_snapshot_compaction_window_preserves_tail() {
  use crate::{Config, Entry, EntryKind, Index, Instant, Term, conf::ConfState};
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();

  let mut stable = crate::testkit::AsyncStable::default();
  let snap_data = encode_count_snapshot(10);
  let meta = crate::SnapshotMeta::new(
    Index::new(5),
    Term::new(2),
    ConfState::from_voters(std::vec![1u64]),
  );
  stable.submit_snapshot(crate::OpId::new(1), meta, snap_data);
  while stable.poll().is_some() {}
  stable.force_state(Term::new(2), None, Index::new(7));

  // The node snapshotted its OWN log at 5 (blob durable) but the deferred compact has NOT run: the
  // log still holds the FULL committed log 1..=7, INCLUDING the tail 6,7 above the snapshot.
  let mut log = crate::testkit::VecLog::default();
  log.force_append(&[
    Entry::new(
      Term::new(2),
      Index::new(1),
      EntryKind::Empty,
      bytes::Bytes::new(),
    ),
    Entry::new(
      Term::new(2),
      Index::new(2),
      EntryKind::Empty,
      bytes::Bytes::new(),
    ),
    Entry::new(
      Term::new(2),
      Index::new(3),
      EntryKind::Empty,
      bytes::Bytes::new(),
    ),
    Entry::new(
      Term::new(2),
      Index::new(4),
      EntryKind::Empty,
      bytes::Bytes::new(),
    ),
    Entry::new(
      Term::new(2),
      Index::new(5),
      EntryKind::Empty,
      bytes::Bytes::new(),
    ),
    Entry::new(
      Term::new(2),
      Index::new(6),
      EntryKind::Normal,
      encode_cmd(b"cmd6"),
    ),
    Entry::new(
      Term::new(2),
      Index::new(7),
      EntryKind::Normal,
      encode_cmd(b"cmd7"),
    ),
  ]);
  assert_eq!(log.first_index(), Index::new(1), "log NOT yet compacted");
  assert_eq!(log.last_index(), Index::new(7));

  let ep = Endpoint::restart(
    cfg,
    Instant::ORIGIN,
    42,
    crate::testkit::CountSm::default(),
    1, // boot_epoch (this incarnation > the prior one)
    &mut log,
    &mut stable,
  );

  // MUST recover by compacting (preserving the tail), NOT restore (which would truncate 6,7).
  assert!(
    !ep.is_poisoned(),
    "the compaction-window restart must recover, not poison"
  );
  assert_eq!(
    ep.applied,
    Index::new(7),
    "the committed tail 6,7 must be replayed (not truncated)"
  );
  assert_eq!(ep.commit, Index::new(7));
  assert_eq!(
    ep.state_machine().count(),
    12,
    "snapshot baseline 10 + 2 replayed tail entries (NOT rolled back to 10)"
  );
  assert_eq!(
    log.first_index(),
    Index::new(6),
    "compacted through the snapshot boundary"
  );
  assert_eq!(
    log.last_index(),
    Index::new(7),
    "the committed tail is preserved"
  );
}

/// Regression (a fatal boundary term-read at restart must poison, NOT truncate): the
/// compaction/install discriminator reads the boundary term `term(N)`. A `term()` `Err` is a FATAL
/// storage read failure (as everywhere else in the core), NOT evidence the boundary is absent.
/// Collapsing `Err` into "absent" (the old `.unwrap_or(false)`) would take the `restore` branch and
/// DELETE a committed tail that is actually present. Here the local-compaction-window log holds the
/// committed tail 1..=7 but `term(5)` fails; restart must poison `LogTerm` and leave the log intact.
///
/// MUTATION: revert the `Err(_) => poison(LogTerm)` arm to fall through to `restore` (or restore the
/// `.unwrap_or(false)`) → the committed tail 6,7 is truncated instead of fail-stopping.
#[test]
fn restart_boundary_term_read_failure_poisons_not_truncates() {
  use crate::{Config, Entry, EntryKind, Index, Instant, Term, conf::ConfState};
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();

  let mut stable = crate::testkit::AsyncStable::default();
  let snap_data = encode_count_snapshot(10);
  let meta = crate::SnapshotMeta::new(
    Index::new(5),
    Term::new(2),
    ConfState::from_voters(std::vec![1u64]),
  );
  stable.submit_snapshot(crate::OpId::new(1), meta, snap_data);
  while stable.poll().is_some() {}
  stable.force_state(Term::new(2), None, Index::new(7));

  // Local compaction window: durable snapshot at 5, the log still holds the committed tail 1..=7 —
  // but reading the boundary term `term(5)` FAILS (a storage fault).
  let mut log = crate::testkit::FailTermLog::default();
  log.force_append(&[
    Entry::new(
      Term::new(2),
      Index::new(1),
      EntryKind::Empty,
      bytes::Bytes::new(),
    ),
    Entry::new(
      Term::new(2),
      Index::new(2),
      EntryKind::Empty,
      bytes::Bytes::new(),
    ),
    Entry::new(
      Term::new(2),
      Index::new(3),
      EntryKind::Empty,
      bytes::Bytes::new(),
    ),
    Entry::new(
      Term::new(2),
      Index::new(4),
      EntryKind::Empty,
      bytes::Bytes::new(),
    ),
    Entry::new(
      Term::new(2),
      Index::new(5),
      EntryKind::Empty,
      bytes::Bytes::new(),
    ),
    Entry::new(
      Term::new(2),
      Index::new(6),
      EntryKind::Normal,
      encode_cmd(b"cmd6"),
    ),
    Entry::new(
      Term::new(2),
      Index::new(7),
      EntryKind::Normal,
      encode_cmd(b"cmd7"),
    ),
  ]);
  log.fail_term_at(Some(Index::new(5)));

  let ep = Endpoint::restart(
    cfg,
    Instant::ORIGIN,
    42,
    crate::testkit::CountSm::default(),
    1, // boot_epoch (this incarnation > the prior one)
    &mut log,
    &mut stable,
  );

  // A fatal boundary term-read must POISON, not silently restore over the committed tail.
  assert!(ep.is_poisoned(), "a fatal boundary term-read must poison");
  assert_eq!(ep.poison_reason().map(|r| r.as_str()), Some("log_term"));
  // The committed tail must NOT have been truncated — no `restore` ran.
  assert_eq!(
    log.last_index(),
    Index::new(7),
    "the committed tail must not be truncated on a boundary term-read failure"
  );
}

/// Completeness proof for the restart log/snapshot reconciliation: `reconcile_restart_log` is a
/// pure, total function over the durable shape, so we exhaustively map every distinct
/// `(snapshot, committed_in_log, first_index, last_index, boundary_term)` class to its action. This
/// covers EVERY branch of the function — the guarantee the per-shape ad-hoc cases never gave (they
/// missed a case for five review rounds, including a committed-tail truncation).
#[test]
fn reconcile_restart_log_is_exhaustive() {
  use super::{RestartLogAction as A, reconcile_restart_log};
  use crate::{Index, PoisonReason as P, Term};
  let i = Index::new;
  let n = i(5);
  let t = Term::new(2);
  let other = Term::new(3); // a boundary term that disagrees with the snapshot

  // (snap, committed_in_log, first_index, last_index, boundary_term) -> expected action.
  type Case = (
    Option<(Index, Term)>,
    Index,
    Index,
    Index,
    Option<Result<Term, ()>>,
    A,
  );
  let cases: &[Case] = &[
    // ── No durable snapshot ──
    (None, i(0), i(1), i(0), None, A::None), // fresh/empty log
    (None, i(7), i(1), i(7), None, A::None), // uncompacted log with a committed range
    (None, i(0), i(3), i(7), None, A::Poison(P::OrphanedLog)), // compacted, no snapshot → prefix gone
    // ── Durable snapshot at N=5 ──
    (
      Some((n, t)),
      i(0),
      i(7),
      i(9),
      None,
      A::Poison(P::OrphanedLog),
    ), // first_index > N+1 → past
    (Some((n, t)), i(5), i(6), i(9), Some(Ok(t)), A::None), // first_index == N+1, boundary matches → consistent
    (Some((n, t)), i(3), i(1), i(3), None, A::Restore(n, t)), // last_index < N → behind (install)
    (Some((n, t)), i(5), i(3), i(7), Some(Ok(t)), A::Compact(n)), // boundary matches (fi<=N) → compaction
    // first_index == N+1 must ALSO validate the retained baseline term.
    (
      Some((n, t)),
      i(9),
      i(6),
      i(9),
      Some(Ok(other)),
      A::Poison(P::OrphanedLog),
    ), // fi==N+1, boundary MISMATCH, committed tail above N → corruption
    (
      Some((n, t)),
      i(5),
      i(6),
      i(9),
      Some(Ok(other)),
      A::Poison(P::OrphanedLog),
    ), // fi==N+1, boundary MISMATCH, committed AT N (cil==n) → committed-boundary corruption
    (
      Some((n, t)),
      i(4),
      i(6),
      i(9),
      Some(Ok(other)),
      A::Restore(n, t),
    ), // fi==N+1, boundary MISMATCH, boundary uncommitted (cil<n) → re-baseline
    (
      Some((n, t)),
      i(5),
      i(6),
      i(9),
      Some(Err(())),
      A::Poison(P::LogTerm),
    ), // fi==N+1, fatal boundary term-read
    (
      Some((n, t)),
      i(5),
      i(3),
      i(7),
      Some(Ok(other)),
      A::Poison(P::OrphanedLog),
    ), // live boundary, MISMATCH, committed AT N (cil==n) → committed-boundary corruption
    (
      Some((n, t)),
      i(4),
      i(3),
      i(7),
      Some(Ok(other)),
      A::Restore(n, t),
    ), // live boundary, MISMATCH, boundary uncommitted (cil<n) → re-baseline
    (
      Some((n, t)),
      i(7),
      i(3),
      i(7),
      Some(Ok(other)),
      A::Poison(P::OrphanedLog),
    ), // mismatch, committed > N → corruption (would truncate a committed tail)
    (
      Some((n, t)),
      i(5),
      i(3),
      i(7),
      Some(Err(())),
      A::Poison(P::LogTerm),
    ), // fatal boundary term-read
    // ── Log-validity precondition (structural gaps) ──
    (
      Some((n, t)),
      i(0),
      i(6),
      i(4),
      None,
      A::Poison(P::OrphanedLog),
    ), // first_index=N+1 but last_index<N → gap
    (None, i(0), i(6), i(4), None, A::Poison(P::OrphanedLog)), // gap with no snapshot
    // ── Empty log baselined exactly at N (first==last+1, NOT a gap) ──
    (Some((n, t)), i(5), i(6), i(5), Some(Ok(t)), A::None), // snapshot at N, empty log above it, boundary matches
  ];

  for (idx, (snap, cil, fi, li, bt, expected)) in cases.iter().enumerate() {
    assert_eq!(
      reconcile_restart_log(*snap, *cil, *fi, *li, *bt),
      *expected,
      "reconcile case {idx}"
    );
  }
}

/// Regression (boundary-term mismatch over a COMMITTED tail must poison, not truncate):
/// the durable snapshot is at 5 (term 2), but the log holds 1..=7 with `term(5)=3` and
/// `HardState.commit=7`, so 6,7 are committed. A term mismatch at/below a committed index is
/// impossible in correct Raft — restart must poison (`OrphanedLog`) and leave the committed tail
/// intact, NOT re-baseline over it.
///
/// MUTATION: drop the `committed_in_log > n` gate in `reconcile_restart_log` (always `Restore` on
/// mismatch) → restart re-baselines to 5 and truncates the committed tail 6,7.
#[test]
fn restart_committed_tail_boundary_mismatch_poisons_not_truncates() {
  use crate::{Config, Entry, EntryKind, Index, Instant, Term, conf::ConfState};
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();

  let mut stable = crate::testkit::AsyncStable::default();
  let snap_data = encode_count_snapshot(10);
  let meta = crate::SnapshotMeta::new(
    Index::new(5),
    Term::new(2),
    ConfState::from_voters(std::vec![1u64]),
  );
  stable.submit_snapshot(crate::OpId::new(1), meta, snap_data);
  while stable.poll().is_some() {}
  stable.force_state(Term::new(3), None, Index::new(7));

  // Log 1..=7 with the boundary entry at 5 carrying term 3 — DISAGREEING with the snapshot's term 2
  // — while commit=7 makes 6,7 committed.
  let mut log = crate::testkit::VecLog::default();
  log.force_append(&[
    Entry::new(
      Term::new(2),
      Index::new(1),
      EntryKind::Empty,
      bytes::Bytes::new(),
    ),
    Entry::new(
      Term::new(2),
      Index::new(2),
      EntryKind::Empty,
      bytes::Bytes::new(),
    ),
    Entry::new(
      Term::new(2),
      Index::new(3),
      EntryKind::Empty,
      bytes::Bytes::new(),
    ),
    Entry::new(
      Term::new(2),
      Index::new(4),
      EntryKind::Empty,
      bytes::Bytes::new(),
    ),
    Entry::new(
      Term::new(3),
      Index::new(5),
      EntryKind::Empty,
      bytes::Bytes::new(),
    ),
    Entry::new(
      Term::new(3),
      Index::new(6),
      EntryKind::Normal,
      encode_cmd(b"cmd6"),
    ),
    Entry::new(
      Term::new(3),
      Index::new(7),
      EntryKind::Normal,
      encode_cmd(b"cmd7"),
    ),
  ]);

  let ep = Endpoint::restart(
    cfg,
    Instant::ORIGIN,
    42,
    crate::testkit::CountSm::default(),
    1, // boot_epoch (this incarnation > the prior one)
    &mut log,
    &mut stable,
  );

  assert!(
    ep.is_poisoned(),
    "a boundary mismatch over a committed tail is corruption — must poison"
  );
  assert_eq!(ep.poison_reason().map(|r| r.as_str()), Some("orphaned_log"));
  assert_eq!(
    log.last_index(),
    Index::new(7),
    "the committed tail must NOT be truncated"
  );
}

/// Regression (a boundary-term mismatch AT the committed boundary (`committed_in_log == N`)
/// must poison, not re-baseline): the durable snapshot is at 5 (term 2), the log holds 1..=5 with
/// `term(5)=3` (DISAGREEING), and `HardState.commit=5`, so index 5 ITSELF is committed (there is no
/// committed tail ABOVE the boundary — the earlier `> N` gate missed this). The committed boundary and
/// the snapshot are from different histories — restart must poison (`OrphanedLog`), NOT re-baseline
/// the committed boundary 5 onto the snapshot's term-2 history.
///
/// MUTATION: weaken the `reconcile_restart_log` mismatch gate from `committed_in_log >= n` back to
/// `> n` → restart re-baselines (Restore) over the committed boundary instead of poisoning.
#[test]
fn restart_committed_boundary_equality_mismatch_poisons() {
  use crate::{Config, Entry, EntryKind, Index, Instant, Term, conf::ConfState};
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();

  let mut stable = crate::testkit::AsyncStable::default();
  let snap_data = encode_count_snapshot(10);
  let meta = crate::SnapshotMeta::new(
    Index::new(5),
    Term::new(2),
    ConfState::from_voters(std::vec![1u64]),
  );
  stable.submit_snapshot(crate::OpId::new(1), meta, snap_data);
  while stable.poll().is_some() {}
  // commit == 5 == the snapshot boundary: index 5 ITSELF is committed (no committed tail above it).
  stable.force_state(Term::new(3), None, Index::new(5));

  // Log 1..=5 with the boundary entry at 5 carrying term 3 — DISAGREEING with the snapshot's term 2.
  let mut log = crate::testkit::VecLog::default();
  log.force_append(&[
    Entry::new(
      Term::new(2),
      Index::new(1),
      EntryKind::Empty,
      bytes::Bytes::new(),
    ),
    Entry::new(
      Term::new(2),
      Index::new(2),
      EntryKind::Empty,
      bytes::Bytes::new(),
    ),
    Entry::new(
      Term::new(2),
      Index::new(3),
      EntryKind::Empty,
      bytes::Bytes::new(),
    ),
    Entry::new(
      Term::new(2),
      Index::new(4),
      EntryKind::Empty,
      bytes::Bytes::new(),
    ),
    Entry::new(
      Term::new(3),
      Index::new(5),
      EntryKind::Empty,
      bytes::Bytes::new(),
    ),
  ]);

  let ep = Endpoint::restart(
    cfg,
    Instant::ORIGIN,
    42,
    crate::testkit::CountSm::default(),
    1, // boot_epoch
    &mut log,
    &mut stable,
  );

  assert!(
    ep.is_poisoned(),
    "a boundary mismatch AT the committed boundary is corruption — must poison"
  );
  assert_eq!(ep.poison_reason().map(|r| r.as_str()), Some("orphaned_log"));
  assert_eq!(
    log.last_index(),
    Index::new(5),
    "the committed boundary must NOT be re-baselined/truncated"
  );
}

/// LeaseBased crash-safety — a restarted node withholds its vote during the post-restart fence:
/// a node that may have acked a leader's read-lease just before crashing must not, after restart,
/// grant a vote that could elect a new leader inside the old lease window (while the old leader still
/// serves LeaseBased reads). Under LeaseBased, `restart` arms a one-election-timeout vote fence: WITHIN
/// it a non-forced RequestVote is REJECTED (the higher term is still adopted); a FORCED leader-transfer
/// bypasses; PAST it, votes are granted normally.
///
/// MUTATION: drop the `!self.lease_vote_fenced(...)` guard in `on_request_vote` (or stop arming the
/// fence in `restart`) → the restarted node grants the vote within the window.
#[test]
fn restarted_leasebased_node_fences_votes() {
  use crate::{Config, Index, Instant, Message, RequestVote, Term};
  use core::time::Duration;
  let election = Duration::from_millis(1000);
  let cfg = Config::try_new(
    2u64,
    std::vec![1u64, 2u64, 3u64],
    election,
    Duration::from_millis(100),
  )
  .unwrap()
  .with_check_quorum(true)
  .with_read_only(crate::ReadOnlyOption::LeaseBased);
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::AsyncStable::default();
  // Restart under LeaseBased at ORIGIN → arms the post-restart vote fence for one election_timeout.
  let mut ep = Endpoint::restart(
    cfg,
    Instant::ORIGIN,
    1,
    crate::testkit::CountSm::default(),
    1, // boot_epoch
    &mut log,
    &mut stable,
  );
  assert!(
    ep.lease_vote_fence_until.is_some(),
    "a LeaseBased restart arms the post-restart vote fence"
  );

  // WITHIN the fence: a higher-term, non-forced RequestVote is REJECTED — the node may have acked a
  // lease before crashing. The higher term is still adopted (term adoption is always safe).
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    3u64,
    Message::RequestVote(RequestVote::new(
      Term::new(1),
      3u64,
      Index::ZERO,
      Term::ZERO,
      false,
      false,
    )),
  );
  match ep
    .poll_message()
    .expect("a reject VoteResp is sent")
    .message()
  {
    Message::VoteResp(v) => assert!(v.reject(), "a fenced node must REJECT the vote"),
    _ => panic!("expected VoteResp"),
  }
  assert_eq!(ep.voted_for, None, "no vote granted within the fence");
  assert_eq!(ep.term(), Term::new(1), "but the higher term IS adopted");

  // WITHIN the fence: a FORCED leader-transfer RequestVote BYPASSES the fence and is granted (the
  // current leader is voluntarily handing off, relinquishing its lease).
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::RequestVote(RequestVote::new(
      Term::new(2),
      1u64,
      Index::ZERO,
      Term::ZERO,
      false,
      true, // leader_transfer
    )),
  );
  assert_eq!(
    ep.voted_for,
    Some(1u64),
    "a forced leader-transfer vote bypasses the fence"
  );

  // PAST the fence: a non-forced RequestVote is granted normally.
  let past = Instant::ORIGIN + election + election;
  ep.handle_message(
    past,
    &mut log,
    &mut stable,
    3u64,
    Message::RequestVote(RequestVote::new(
      Term::new(3),
      3u64,
      Index::ZERO,
      Term::ZERO,
      false,
      false,
    )),
  );
  assert_eq!(
    ep.voted_for,
    Some(3u64),
    "past the fence, votes are granted normally"
  );
}

/// Regression (the `first_index == N+1` compacted-baseline case must ALSO validate the
/// retained boundary term): a durable snapshot at `(5, 2)` with a log compacted exactly to baseline
/// 5 BUT whose retained `term(5)` is 3 (disagreeing with the snapshot) and a committed tail 6,7.
/// The original code returned healthy (`None`) for `first_index == N+1` without reading `term(5)`,
/// so it would have replayed 6,7 on the WRONG snapshot history. Restart must read the baseline term,
/// see the mismatch over a committed tail, and poison.
///
/// MUTATION: revert `reconcile_restart_log` to map `first_index == N+1` straight to `None` (without
/// the boundary-term match) → the node comes up healthy on a divergent baseline instead of poisoning.
#[test]
fn restart_compacted_baseline_boundary_mismatch_poisons() {
  use crate::{Config, Entry, EntryKind, Index, Instant, Term, conf::ConfState};
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();

  let mut stable = crate::testkit::AsyncStable::default();
  let snap_data = encode_count_snapshot(10);
  let meta = crate::SnapshotMeta::new(
    Index::new(5),
    Term::new(2),
    ConfState::from_voters(std::vec![1u64]),
  );
  stable.submit_snapshot(crate::OpId::new(1), meta, snap_data);
  while stable.poll().is_some() {}
  stable.force_state(Term::new(3), None, Index::new(7));

  // Log compacted EXACTLY to baseline 5 (first_index == 6 == N+1) but with a retained boundary term
  // of 3 — disagreeing with the snapshot's term 2 — plus a committed tail 6,7.
  let mut log = crate::testkit::VecLog::default();
  log.restore(Index::new(5), Term::new(3));
  log.force_append(&[
    Entry::new(
      Term::new(3),
      Index::new(6),
      EntryKind::Normal,
      encode_cmd(b"cmd6"),
    ),
    Entry::new(
      Term::new(3),
      Index::new(7),
      EntryKind::Normal,
      encode_cmd(b"cmd7"),
    ),
  ]);
  assert_eq!(log.first_index(), Index::new(6), "compacted to N+1");
  assert_eq!(
    log.term(Index::new(5)).unwrap(),
    Term::new(3),
    "baseline term mismatches snapshot"
  );

  let ep = Endpoint::restart(
    cfg,
    Instant::ORIGIN,
    42,
    crate::testkit::CountSm::default(),
    1, // boot_epoch (this incarnation > the prior one)
    &mut log,
    &mut stable,
  );

  assert!(
    ep.is_poisoned(),
    "fi==N+1 with a mismatched baseline term over a committed tail must poison"
  );
  assert_eq!(ep.poison_reason().map(|r| r.as_str()), Some("orphaned_log"));
  assert_eq!(
    log.last_index(),
    Index::new(7),
    "committed tail not truncated"
  );
}

/// The post-restart vote fence is armed on the ENFORCEMENT CAPABILITY (`check_quorum||pre_vote`),
/// NOT on the node's own `read_only`. A node whose own reads are Safe but that runs CheckQuorum still
/// upholds a LeaseBased leader's lease (advertises `lease_support`, runs `in_lease`), so it MUST fence
/// post-restart; a node that enforces nothing must not.
///
/// MUTATION: key the fence on `read_only == LeaseBased` → the Safe+CheckQuorum node fails to fence.
#[test]
fn restart_arms_vote_fence_on_enforcement_capability_not_read_mode() {
  use crate::{Config, Instant};
  use core::time::Duration;
  let et = Duration::from_millis(1000);
  let hb = Duration::from_millis(100);
  // Safe reads BUT check_quorum on → enforces a leader's lease → must fence post-restart.
  let cfg = Config::try_new(2u64, std::vec![1u64, 2u64, 3u64], et, hb)
    .unwrap()
    .with_check_quorum(true); // read_only defaults to Safe
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::AsyncStable::default();
  let ep = Endpoint::restart(
    cfg,
    Instant::ORIGIN,
    1,
    crate::testkit::CountSm::default(),
    1,
    &mut log,
    &mut stable,
  );
  assert!(
    ep.lease_vote_fence_until.is_some(),
    "a Safe-reads BUT check_quorum node must fence post-restart (it can uphold a leader's lease)"
  );
  // Neither check_quorum nor pre_vote → enforces nothing → no fence needed.
  let cfg2 = Config::try_new(2u64, std::vec![1u64, 2u64, 3u64], et, hb).unwrap();
  let mut log2 = crate::testkit::VecLog::default();
  let mut stable2 = crate::testkit::AsyncStable::default();
  let ep2 = Endpoint::restart(
    cfg2,
    Instant::ORIGIN,
    1,
    crate::testkit::CountSm::default(),
    1,
    &mut log2,
    &mut stable2,
  );
  assert!(
    ep2.lease_vote_fence_until.is_none(),
    "a node that enforces neither check_quorum nor pre_vote needs no post-restart fence"
  );
}

/// A restart under a config that DISABLES enforcement (neither check_quorum nor pre_vote) must
/// still fence for the DURABLE pre-crash promise — the fence is sized by `hs.lease_support()`, not by the
/// post-restart config.
///
/// MUTATION: size `lease_vote_fence_until` from the config window only (ignore `durable_window`) → the
/// fence becomes None under enforcement-off (the original bug).
#[test]
fn restart_fence_honors_persisted_floor_under_enforcement_disabled() {
  use crate::{Config, HardState, Instant};
  use core::time::Duration;
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::AsyncStable::default();
  stable.force_hard_state(
    HardState::initial().with_lease_support(crate::LeaseSupport::Recorded(Some(
      Duration::from_millis(1000),
    ))),
  );
  // Restart with enforcement OFF (check_quorum and pre_vote both default false).
  let cfg = Config::try_new(
    2u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(100),
    Duration::from_millis(50),
  )
  .unwrap();
  let now = Instant::ORIGIN + Duration::from_millis(5000);
  let ep = Endpoint::restart(
    cfg,
    now,
    7,
    crate::testkit::CountSm::default(),
    1,
    &mut log,
    &mut stable,
  );
  assert_eq!(
    ep.lease_vote_fence_until,
    Some(now + Duration::from_millis(1000)),
    "the durable pre-crash promise must arm the fence even when the post-restart config enforces nothing"
  );
}

/// A restart with a SHORTER election_timeout must still fence for the (longer) durable promise.
///
/// MUTATION: `lease_vote_fence_until = Some(now + config.election_timeout())` (drop the max with the
/// durable floor) → fence becomes now+100ms, shorter than the 1000ms promised.
#[test]
fn restart_fence_honors_persisted_floor_over_shrunk_election_timeout() {
  use crate::{Config, HardState, Instant};
  use core::time::Duration;
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::AsyncStable::default();
  stable.force_hard_state(
    HardState::initial().with_lease_support(crate::LeaseSupport::Recorded(Some(
      Duration::from_millis(1000),
    ))),
  );
  let cfg = Config::try_new(
    2u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(100),
    Duration::from_millis(50),
  )
  .unwrap()
  .with_check_quorum(true);
  let now = Instant::ORIGIN + Duration::from_millis(5000);
  let ep = Endpoint::restart(
    cfg,
    now,
    7,
    crate::testkit::CountSm::default(),
    1,
    &mut log,
    &mut stable,
  );
  assert_eq!(
    ep.lease_vote_fence_until,
    Some(now + Duration::from_millis(1000)),
    "the fence must honor the durable 1000ms promise, not the shrunk 100ms election_timeout"
  );
}

/// Fail-stop: a legacy (pre-format) `Unrecorded` record has an UNKNOWN, possibly-large prior lease
/// promise, so plain `restart` (no operator bound) cannot fence it safely by any finite value and must
/// FAIL-STOP (poison `legacy_lease_unrecoverable`) — under BOTH enforcing and non-enforcing config, since
/// either way the node could grant a disruptive vote inside an old leader's still-live lease. A poisoned
/// node is inert (emits/persists nothing), so it can never grant. `restart_migrating(Some(bound))` is the
/// recovery path and is NOT poisoned (the et-shrink case is fenced by the operator's bound, not under-fenced).
/// Native nodes never hit this (genesis is `Recorded`).
///
/// MUTATION: have the `Unrecorded`-without-bound arm of `reconcile_durable` return a finite fence instead
/// of `Poison` → plain restart under-fences a legacy node → the `is_poisoned` assertion fails.
#[test]
fn legacy_unrecorded_plain_restart_poisons_migrating_recovers() {
  use crate::{Config, HardState, Instant, LeaseSupport, PoisonReason};
  use core::time::Duration;
  let now = Instant::ORIGIN + Duration::from_millis(5000);
  let legacy = || {
    let mut s = crate::testkit::AsyncStable::default();
    s.force_hard_state(HardState::initial().with_lease_support(LeaseSupport::Unrecorded));
    s
  };
  let enforcing = Config::try_new(
    2u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(100), // SHRUNK timeout (the dangerous et-shrink-on-upgrade case)
    Duration::from_millis(50),
  )
  .unwrap()
  .with_check_quorum(true);
  let non_enforcing = Config::try_new(
    2u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(100),
    Duration::from_millis(50),
  )
  .unwrap();
  // Plain restart of a legacy Unrecorded record FAIL-STOPS — both enforcing and non-enforcing.
  for cfg in [enforcing.clone(), non_enforcing] {
    let mut log = crate::testkit::VecLog::default();
    let mut stable = legacy();
    let ep = Endpoint::restart(
      cfg,
      now,
      7,
      crate::testkit::CountSm::default(),
      1,
      &mut log,
      &mut stable,
    );
    assert!(
      ep.is_poisoned(),
      "plain restart of a legacy Unrecorded record must fail-stop (unbounded prior promise)"
    );
    assert_eq!(
      ep.poison_reason(),
      Some(PoisonReason::LegacyLeaseUnrecoverable)
    );
  }
  // restart_migrating with the operator's known pre-upgrade window (2000ms) RECOVERS — fence honors the
  // bound (now+2000), NOT the shrunk 100ms config, and the node is NOT poisoned.
  let mut log = crate::testkit::VecLog::default();
  let mut stable = legacy();
  let ep = Endpoint::restart_migrating(
    enforcing,
    now,
    7,
    crate::testkit::CountSm::default(),
    1,
    Some(Duration::from_millis(2000)),
    &mut log,
    &mut stable,
  );
  assert!(
    !ep.is_poisoned(),
    "restart_migrating with a bound must recover, not poison"
  );
  assert_eq!(
    ep.lease_vote_fence_until,
    Some(now + Duration::from_millis(2000)),
    "restart_migrating fences by the operator's known prior window, not the shrunk config"
  );
}

/// `reconcile_durable` is a pure total fn — a table over
/// (Recorded(None) / Recorded(Some) / Unrecorded) x (enforcing / not) x (assume_prior present / absent)
/// pins every cell. A native `Recorded` is authoritative (assume_prior IGNORED, never poisons); a legacy
/// `Unrecorded` is safe ONLY with an operator bound, else fail-stops (unbounded prior promise).
///
/// MUTATION: make the `Recorded` arm consult assume_prior (over-fences a native node), make the
/// `Unrecorded` arm fence by `this_run` instead of poisoning when assume_prior is None (under-fence),
/// or have `Unrecorded` poison even WITH a bound → a cell mismatches.
#[test]
fn reconcile_durable_is_exhaustive() {
  use crate::LeaseSupport;
  use core::time::Duration;
  let et = Duration::from_millis(1000);
  let p = Some(Duration::from_millis(900)); // an operator-supplied assume_prior bound
  let d700 = Some(Duration::from_millis(700));
  let ok = |floor, window| {
    LeaseReconcile::Ok(DerivedLeaseSafety {
      lease_support_floor: floor,
      fence_window: window,
    })
  };
  // (recovered, enforcing, assume_prior) -> expected LeaseReconcile
  let cases = [
    // Native Recorded(Some(700)): authoritative; enforcing maxes with this_run(1000); assume_prior ignored.
    (
      LeaseSupport::Recorded(d700),
      true,
      p,
      ok(Some(et), Some(et)),
    ),
    (LeaseSupport::Recorded(d700), false, p, ok(d700, d700)),
    // Native Recorded(None): promised nothing; fence only by this_run; assume_prior ignored; never poisons.
    (
      LeaseSupport::Recorded(None),
      true,
      p,
      ok(Some(et), Some(et)),
    ),
    (LeaseSupport::Recorded(None), false, p, ok(None, None)),
    (LeaseSupport::Recorded(None), false, None, ok(None, None)),
    // Legacy Unrecorded WITH an operator bound: safe (this_run.max(assume_prior)).
    (LeaseSupport::Unrecorded, true, p, ok(Some(et), Some(et))), // max(1000,900)=1000
    (LeaseSupport::Unrecorded, false, p, ok(p, p)),
    // Legacy Unrecorded WITHOUT a bound: fail-stop (the prior promise is unbounded).
    (LeaseSupport::Unrecorded, true, None, LeaseReconcile::Poison),
    (
      LeaseSupport::Unrecorded,
      false,
      None,
      LeaseReconcile::Poison,
    ),
  ];
  for (recovered, enforcing, assume_prior, expected) in cases {
    assert_eq!(
      reconcile_durable(recovered, enforcing, et, assume_prior),
      expected,
      "reconcile_durable({recovered:?}, enforcing={enforcing}, assume_prior={assume_prior:?})"
    );
  }
}

/// A NATIVE `Recorded(None)` (a fresh / non-enforcing current-format node) is NOT over-fenced —
/// `reconcile_durable` ignores `assume_prior` for any `Recorded` record (it is authoritative). Only a
/// legacy `Unrecorded` record consults the operator's assumed prior. This pins the by-construction
/// native/legacy distinction the in-tree by-value store must preserve.
///
/// MUTATION: treat `Recorded(None)` as `Unrecorded` (or have the `Recorded` arm consult assume_prior) →
/// the native node is fenced by the (huge) assume_prior it should ignore.
#[test]
fn native_recorded_none_is_not_overfenced() {
  use crate::{Config, HardState, Instant, LeaseSupport};
  use core::time::Duration;
  let now = Instant::ORIGIN + Duration::from_millis(5000);
  let cfg = || {
    Config::try_new(
      2u64,
      std::vec![1u64, 2u64, 3u64],
      Duration::from_millis(100),
      Duration::from_millis(50),
    )
    .unwrap()
  };
  // A NATIVE Recorded(None) under restart_migrating with a HUGE assume_prior: the native record is
  // authoritative → assume_prior is ignored → non-enforcing config → no fence.
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::AsyncStable::default();
  stable.force_hard_state(HardState::initial().with_lease_support(LeaseSupport::Recorded(None)));
  let ep = Endpoint::restart_migrating(
    cfg(),
    now,
    7,
    crate::testkit::CountSm::default(),
    1,
    Some(Duration::from_secs(999)), // huge assume_prior — MUST be ignored for a native record
    &mut log,
    &mut stable,
  );
  assert_eq!(
    ep.lease_vote_fence_until, None,
    "a native Recorded(None) must ignore assume_prior (authoritative no-promise), not over-fence"
  );
  // Contrast: the SAME huge assume_prior on a legacy Unrecorded record IS honored.
  let mut log2 = crate::testkit::VecLog::default();
  let mut stable2 = crate::testkit::AsyncStable::default();
  stable2.force_hard_state(HardState::initial().with_lease_support(LeaseSupport::Unrecorded));
  let ep2 = Endpoint::restart_migrating(
    cfg(),
    now,
    7,
    crate::testkit::CountSm::default(),
    1,
    Some(Duration::from_secs(999)),
    &mut log2,
    &mut stable2,
  );
  assert_eq!(
    ep2.lease_vote_fence_until,
    Some(now + Duration::from_secs(999)),
    "a legacy Unrecorded record DOES honor the operator's assume_prior"
  );
}

/// `restart` seeds the op-id counter at seq 0 of THIS boot epoch, so a post-restart op id strictly
/// exceeds (and is unequal to) every prior incarnation's id — the by-construction basis for ignoring a
/// prior-incarnation storage completion that survives a crash. Fresh `new()` uses epoch 0.
///
/// MUTATION: seed `next_op_id` at restart to `OpId::ZERO` instead of `first_of_epoch(boot_epoch)` → the
/// minted id is `{epoch:0, seq:0}`, colliding with a fresh node's / a prior incarnation's ids.
#[test]
fn restart_mints_epoch_scoped_op_ids() {
  use crate::{Config, Instant, OpId};
  use core::time::Duration;
  let cfg = || {
    Config::try_new(
      1u64,
      std::vec![1u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap()
  };
  // Fresh node: epoch 0.
  let mut fresh = Endpoint::new(
    cfg(),
    Instant::ORIGIN,
    1,
    crate::testkit::CountSm::default(),
  );
  assert_eq!(
    fresh.mint_op_id_for_test(),
    OpId::new(0),
    "fresh node mints epoch-0 ids"
  );
  // Restart at boot_epoch 7: the first minted id is seq 0 of epoch 7.
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::AsyncStable::default();
  let mut ep = Endpoint::restart(
    cfg(),
    Instant::ORIGIN,
    1,
    crate::testkit::CountSm::default(),
    7,
    &mut log,
    &mut stable,
  );
  assert_eq!(
    ep.mint_op_id_for_test(),
    OpId::first_of_epoch(7),
    "a restart must mint ids in its own boot epoch, never colliding with a prior incarnation"
  );
}

/// `OpId` ordering is EPOCH-MAJOR — a higher boot epoch's FIRST id exceeds (and is unequal to) ANY
/// lower epoch's id. This is what makes a prior-incarnation completion (lower epoch) sort below every
/// current op (so it fails every `>=` durability-watermark check) and compare unequal (so it misses
/// every `pending`/inflight map lookup), with no explicit epoch check in the completion handlers.
///
/// MUTATION: order `OpId` by `seq` first (or ignore `epoch`) → a prior incarnation's high-`seq` id can
/// exceed a new incarnation's `first_of_epoch`, reopening the stale-completion collision.
#[test]
fn op_id_is_epoch_major() {
  use crate::OpId;
  // A higher epoch's first id beats any amount of seq in a lower epoch.
  assert!(OpId::first_of_epoch(2) > OpId::new(u64::MAX));
  assert!(OpId::first_of_epoch(8) > OpId::first_of_epoch(7).next().next());
  // Distinct epochs are never equal even at the same seq → map lookups can't collide across incarnations.
  assert_ne!(OpId::new(0), OpId::first_of_epoch(1));
  assert_ne!(OpId::first_of_epoch(1), OpId::first_of_epoch(2));
  // Within an epoch, seq orders as usual.
  assert!(OpId::first_of_epoch(3).next() > OpId::first_of_epoch(3));
}

/// A same-config restart submits NO floor write (the recovered floor already covers this run) and
/// the gate is satisfied immediately (no ZERO advertise stall).
///
/// MUTATION: always submit the floor write in restart (drop the `if floor > durable` guard) → a write
/// appears and the first advertisement stalls at ZERO.
#[test]
fn restart_same_config_no_floor_write_and_no_advertise_stall() {
  use crate::{Config, HardState, Instant, Term};
  use core::time::Duration;
  let et = Duration::from_millis(1000);
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::AsyncStable::default();
  stable.force_hard_state(
    HardState::initial()
      .with_term(Term::new(5))
      .with_lease_support(crate::LeaseSupport::Recorded(Some(et))),
  );
  let cfg = Config::try_new(
    2u64,
    std::vec![1u64, 2u64, 3u64],
    et,
    Duration::from_millis(50),
  )
  .unwrap()
  .with_check_quorum(true)
  .with_read_only(crate::ReadOnlyOption::LeaseBased);
  let now = Instant::ORIGIN;
  let mut ep = Endpoint::restart(
    cfg,
    now,
    7,
    crate::testkit::CountSm::default(),
    1,
    &mut log,
    &mut stable,
  );
  assert_eq!(
    stable.pending_writes(),
    0,
    "a same-config restart must submit no floor write (recovered floor already covers this run)"
  );
  // First heartbeat (same term 5) advertises the real support immediately — no ZERO stall.
  let s1 = follower_advertised_support(&mut ep, &mut log, &mut stable, now, 5, 1);
  assert_eq!(
    s1, et,
    "a same-config restart's first HeartbeatResp must advertise the real support (no stall)"
  );
}

/// Defense-in-depth: the restart fence is computed from the durable floor UNCONDITIONALLY — even a
/// restart that POISONS still arms the fence (a poisoned node grants no vote anyway, but the fence must
/// not depend on the poison decision).
///
/// MUTATION: gate the fence computation behind `if !poisoned { .. } else { None }` → the poisoned
/// restart leaves the fence None.
#[test]
fn poisoned_restart_still_computes_fence_from_floor() {
  use crate::{Config, HardState, Index, Instant, Term};
  use core::time::Duration;
  // An orphaned-log restart (re-baselined log, no durable snapshot) poisons.
  let mut stable = crate::testkit::AsyncStable::default();
  stable.force_hard_state(
    HardState::initial()
      .with_term(Term::new(2))
      .with_commit(Index::new(7))
      .with_lease_support(crate::LeaseSupport::Recorded(Some(Duration::from_millis(
        1000,
      )))),
  );
  let mut log = crate::testkit::VecLog::default();
  log.restore(Index::new(5), Term::new(2));
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(100),
    Duration::from_millis(50),
  )
  .unwrap();
  let now = Instant::ORIGIN + Duration::from_millis(5000);
  let ep = Endpoint::restart(
    cfg,
    now,
    42,
    crate::testkit::CountSm::default(),
    1,
    &mut log,
    &mut stable,
  );
  assert!(ep.is_poisoned(), "the orphaned-log restart must poison");
  assert_eq!(
    ep.lease_vote_fence_until,
    Some(now + Duration::from_millis(1000)),
    "the fence must still be armed from the durable floor on a poisoned restart"
  );
}

/// Migration boundary: `restart_migrating` folds the operator-supplied
/// `assume_prior_lease_support` into the fence, so upgrading from a pre-format binary (no durable floor)
/// under WEAKER config still honors the in-memory-only promise the old node may have made — and persists
/// it so subsequent plain restarts are covered. Plain `restart` (the contrast below) cannot.
///
/// MUTATION: drop `.max(assume_prior_lease_support)` in `restart_inner` → the legacy None record under a
/// non-enforcing config yields a None floor → no fence → the assertion fails.
#[test]
fn restart_migrating_honors_assumed_prior_lease_support() {
  use crate::{Config, HardState, Instant};
  use core::time::Duration;
  let now = Instant::ORIGIN + Duration::from_millis(5000);
  let cfg = || {
    // Upgrade restart under WEAKER config: enforcement OFF (so `this_run` is None) + shorter timeout.
    Config::try_new(
      2u64,
      std::vec![1u64, 2u64, 3u64],
      Duration::from_millis(100),
      Duration::from_millis(50),
    )
    .unwrap()
  };
  // Plain restart of a legacy (Unrecorded) record FAIL-STOPS — the prior promise is unbounded, so no
  // finite fence is safe; the operator must use restart_migrating.
  let mut log0 = crate::testkit::VecLog::default();
  let mut stable0 = crate::testkit::AsyncStable::default();
  stable0
    .force_hard_state(HardState::initial().with_lease_support(crate::LeaseSupport::Unrecorded));
  let ep0 = Endpoint::restart(
    cfg(),
    now,
    7,
    crate::testkit::CountSm::default(),
    1,
    &mut log0,
    &mut stable0,
  );
  assert!(
    ep0.is_poisoned(),
    "plain restart of a legacy Unrecorded record must fail-stop, not silently proceed"
  );
  // restart_migrating with the operator-supplied prior closes it.
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::AsyncStable::default();
  stable.force_hard_state(HardState::initial().with_lease_support(crate::LeaseSupport::Unrecorded)); // legacy/pre-format record
  let ep = Endpoint::restart_migrating(
    cfg(),
    now,
    7,
    crate::testkit::CountSm::default(),
    1,
    Some(Duration::from_millis(1000)), // operator: this node may have promised up to 1000ms pre-crash
    &mut log,
    &mut stable,
  );
  assert_eq!(
    ep.lease_vote_fence_until,
    Some(now + Duration::from_millis(1000)),
    "restart_migrating must honor the assumed prior promise (1000ms), not the weaker post-restart config"
  );
  assert!(
    stable.pending_writes() > 0,
    "the assumed floor must be persisted on the migration restart so later plain restarts are covered"
  );
}

/// Pin the leader-side round-token restart safety. The
/// leader's ReadIndex round token also resets on restart, but it is safe BY CONSTRUCTION: a restarted
/// node returns as a FOLLOWER, and `on_heartbeat_resp` only confirms reads while leader. To confirm
/// reads again it must win a NEW election (strictly higher term), and the term pre-pass drops any
/// pre-crash HeartbeatResp (lower term). Here a restarted follower receives a HeartbeatResp at its
/// current term and emits NO ReadState (it is not leader), so a reset round token cannot complete a
/// read from a stale ack.
#[test]
fn restarted_follower_ignores_heartbeat_resp_read_acks() {
  use crate::{Config, HeartbeatResp, Index, Instant, Message, Term};
  use core::time::Duration;
  let cfg = Config::try_new(
    2u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::NoopStable::default();
  stable.force_state(Term::new(1), None, Index::ZERO);
  let mut ep = Endpoint::restart(
    cfg,
    Instant::ORIGIN,
    7,
    crate::testkit::CountSm::default(),
    1,
    &mut log,
    &mut stable,
  );
  assert!(ep.role().is_follower());
  // A HeartbeatResp (the leader-side read ack) carrying any context must not complete a read on a
  // follower — `on_heartbeat_resp` early-returns when `!is_leader`.
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::HeartbeatResp(HeartbeatResp::new(
      Term::new(1),
      1u64,
      bytes::Bytes::copy_from_slice(&[0u8; 8]),
    )),
  );
  assert!(
    !core::iter::from_fn(|| ep.poll_event()).any(|e| matches!(e, crate::Event::ReadState(_))),
    "a restarted follower must not complete a read from a HeartbeatResp"
  );
}
