use super::*;
use crate::{
  AppendEntries, Config, Entry, EntryKind, Index, Instant, Message, PrepareMergePayload, Term,
  VoteResponse,
};

/// Encode a `PrepareMerge` payload naming target group bytes `t` with `source_gen_after`.
fn prepare_payload(t: &'static [u8], source_gen_after: u64) -> bytes::Bytes {
  let p = PrepareMergePayload::new(bytes::Bytes::from_static(t), source_gen_after);
  let mut buf = Vec::new();
  crate::wire::encode_prepare_merge_payload(&p, &mut buf);
  bytes::Bytes::from(buf)
}

/// A 3-voter leader (node 1) at term 1 with its no-op committed nowhere (peers never ack), so
/// later admin appends stay UNCOMMITTED — the append-window shapes the freeze tests pin.
fn make_three_voter_leader() -> (Endpoint<u64, CountSm>, VecLog, AsyncStable) {
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 42, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();
  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable); // campaign
  ep.handle_storage(d, &mut log, &mut stable); // self-vote durable
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResponse(VoteResponse::new(Term::new(1), 2u64, false, false)),
  );
  assert!(ep.role().is_leader());
  ep.handle_storage(d, &mut log, &mut stable); // no-op append durable locally
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}
  (ep, log, stable)
}

/// A follower (node 2 of {1,2,3}) with empty stores.
fn make_merge_follower() -> (Endpoint<u64, CountSm>, VecLog, NoopStable<u64>) {
  use core::time::Duration;
  let cfg = Config::try_new(
    2u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let ep = Endpoint::new(cfg, Instant::ORIGIN, 7, CountSm::default());
  (ep, VecLog::default(), NoopStable::default())
}

/// The spec's `frozen_source_lease_dies_at_append` leader half, at the STATE level: proposing a
/// `PrepareMerge` arms `freeze_pending` the moment the entry enters the leader's own log —
/// before any replication, commit, or apply — and full `Frozen` stays apply-time.
#[test]
fn leader_propose_arms_freeze_pending_at_append() {
  let (mut ep, mut log, _stable) = make_three_voter_leader();
  let idx = ep
    .propose_merge_entry(
      Instant::ORIGIN,
      &mut log,
      EntryKind::PrepareMerge,
      prepare_payload(b"\x2b", 1),
    )
    .expect("leader appends the freeze");
  assert_eq!(ep.merge.freeze_pending, Some(idx), "armed at append");
  assert!(
    !ep.is_frozen(),
    "full Frozen is apply-time, not append-time"
  );
  assert_eq!(ep.freeze_index(), None);
}

/// The follower half of the append-observed kill: `freeze_pending` arms at AppendEntries
/// ACCEPTANCE by a KIND check alone — the payload is GARBAGE here, and the follower must arm
/// without decoding it (decoding is the apply arm's job; the hot path never pays it).
#[test]
fn follower_arms_freeze_pending_at_accept_without_decoding() {
  let (mut ep, mut log, mut stable) = make_merge_follower();
  let freeze = Entry::new(
    Term::new(1),
    Index::new(1),
    EntryKind::PrepareMerge,
    bytes::Bytes::from_static(b"\xff\xff\xff"),
  );
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
      std::vec![freeze],
      Index::ZERO,
    )),
  );
  assert_eq!(ep.merge.freeze_pending, Some(Index::new(1)));
  assert!(!ep.is_poisoned(), "no payload decode on the accept path");
  assert!(!ep.is_frozen());
}

/// A §5.3 conflict truncation covering the pending freeze releases the kill (the entry no longer
/// exists in this log); a truncation strictly ABOVE it leaves the kill armed.
#[test]
fn conflict_truncation_clears_freeze_pending_only_at_or_below() {
  let (mut ep, mut log, mut stable) = make_merge_follower();
  // Term-1 suffix: Normal@1, PrepareMerge@2, Normal@3 — uncommitted (leader_commit = 0).
  let entries = std::vec![
    Entry::new(
      Term::new(1),
      Index::new(1),
      EntryKind::Normal,
      encode_cmd(b"a")
    ),
    Entry::new(
      Term::new(1),
      Index::new(2),
      EntryKind::PrepareMerge,
      prepare_payload(b"\x2b", 1),
    ),
    Entry::new(
      Term::new(1),
      Index::new(3),
      EntryKind::Normal,
      encode_cmd(b"b")
    ),
  ];
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
      entries,
      Index::ZERO,
    )),
  );
  assert_eq!(ep.merge.freeze_pending, Some(Index::new(2)));

  // A term-2 leader overwrites index 3 ONLY: the freeze at 2 survives, the kill stays armed.
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    3u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(2),
      3u64,
      Index::new(2),
      Term::new(1),
      std::vec![Entry::new(
        Term::new(2),
        Index::new(3),
        EntryKind::Normal,
        encode_cmd(b"c"),
      )],
      Index::ZERO,
    )),
  );
  assert_eq!(
    ep.merge.freeze_pending,
    Some(Index::new(2)),
    "a truncation strictly above the freeze leaves it armed"
  );

  // A term-3 leader overwrites from index 2: the freeze entry itself is truncated — released.
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(3),
      1u64,
      Index::new(1),
      Term::new(1),
      std::vec![Entry::new(
        Term::new(3),
        Index::new(2),
        EntryKind::Normal,
        encode_cmd(b"d"),
      )],
      Index::ZERO,
    )),
  );
  assert_eq!(
    ep.merge.freeze_pending, None,
    "truncating the freeze entry releases the append-observed kill"
  );
}

/// Restart re-derives `freeze_pending` from the UNAPPLIED suffix: a committed-but-unapplied (or
/// merely appended) `PrepareMerge` re-arms the kill before the replica can win an election and
/// form a fresh lease. The applied prefix contributes nothing (its freeze folded into `frozen`).
#[test]
fn restart_rederives_freeze_pending_from_the_unapplied_suffix() {
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut log = VecLog::default();
  let mut stable = NoopStable::default();
  // Durable: Normal@1 committed; PrepareMerge@2 appended but UNCOMMITTED (commit = 1).
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
      EntryKind::PrepareMerge,
      prepare_payload(b"\x2b", 1),
    ),
  ]);
  stable.force_state(Term::new(1), Some(1u64), Index::new(1));

  let ep = Endpoint::restart(
    cfg,
    Instant::ORIGIN,
    7,
    CountSm::default(),
    1,
    &mut log,
    &mut stable,
  );
  assert!(!ep.is_poisoned());
  assert_eq!(ep.applied_index(), Index::new(1), "replay stops at commit");
  assert_eq!(
    ep.merge.freeze_pending,
    Some(Index::new(2)),
    "the unapplied-suffix scan re-arms the kill"
  );
}

/// An election does NOT clear the pending freeze: it is log-derived state, so a follower that
/// campaigns (and would go on to lead) inherits the kill with its log — a new leader with the
/// freeze committed-but-unapplied cannot form or serve a lease during the gap.
#[test]
fn election_does_not_clear_freeze_pending() {
  let (mut ep, mut log, mut stable) = make_merge_follower();
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
      std::vec![Entry::new(
        Term::new(1),
        Index::new(1),
        EntryKind::PrepareMerge,
        prepare_payload(b"\x2b", 1),
      )],
      Index::ZERO,
    )),
  );
  assert_eq!(ep.merge.freeze_pending, Some(Index::new(1)));
  // The election timeout fires: the follower campaigns (term moves, role changes) — the
  // append-observed kill must survive the transition.
  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable);
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(
    !ep.role().is_follower(),
    "the timeout must have started a campaign"
  );
  assert_eq!(
    ep.merge.freeze_pending,
    Some(Index::new(1)),
    "an election never clears the pending freeze"
  );
}

/// A snapshot install re-baselines the log, discarding the tail that held the pending freeze:
/// the kill releases with the entry (a stale flag would kill leases forever on a node whose
/// freeze no longer exists); re-delivery of a still-live freeze re-arms it at accept.
#[test]
fn snapshot_install_clears_a_discarded_freeze_pending() {
  use crate::{InstallSnapshot, SnapshotMeta, conf::ConfState};
  let (mut ep, mut log, mut stable) = make_follower();
  // The follower holds an UNCOMMITTED PrepareMerge@1 (leader_commit = 0): kill armed.
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
      std::vec![Entry::new(
        Term::new(1),
        Index::new(1),
        EntryKind::PrepareMerge,
        prepare_payload(b"\x2b", 1),
      )],
      Index::ZERO,
    )),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  assert_eq!(ep.merge.freeze_pending, Some(Index::new(1)));

  // A divergent-history install at boundary 10 re-baselines the log wholesale — the freeze
  // entry is discarded with the tail, so the kill releases.
  let meta = SnapshotMeta::new(
    Index::new(10),
    Term::new(4),
    ConfState::from_voters(std::vec![1u64, 2u64, 3u64]),
  );
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::InstallSnapshot(InstallSnapshot::new(
      Term::new(4),
      1u64,
      meta,
      encode_snapshot(42),
    )),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  assert_eq!(ep.applied_index(), Index::new(10), "install landed");
  assert_eq!(
    ep.merge.freeze_pending, None,
    "the discarded freeze releases the kill"
  );
}

/// The spec's `frozen_source_lease_dies_at_append` row, LeaseBased half: a leader with a LIVE,
/// freshly-confirmed CheckQuorum lease stops lease-serving the moment it PROPOSES the freeze —
/// before commit, before apply — and the read degrades to the Safe heartbeat round.
#[test]
fn pending_freeze_kills_the_leasebased_lease_at_propose() {
  use crate::{AppendResponse, HeartbeatResponse, ReadOnlyOption};
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_read_only(ReadOnlyOption::LeaseBased)
  .with_check_quorum(true);
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = NoopStable::default();
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
  ep.handle_storage(d, &mut log, &mut stable);
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendResponse(AppendResponse::new(
      Term::new(1),
      2u64,
      false,
      Index::ZERO,
      Term::ZERO,
      Index::new(1),
    )),
  );
  // A fresh lease: heartbeat round + a quorum response echoing it with enforcement advertised.
  let hb_at = ep.poll_timeout().expect("heartbeat timer armed");
  ep.handle_timeout(hb_at, &mut log, &mut stable);
  let lease_round = {
    let mut lr = None;
    while let Some(out) = ep.poll_message() {
      if let Message::Heartbeat(hb) = out.message() {
        lr = Some(hb.lease_round());
      }
    }
    lr.expect("a heartbeat carrying a lease round")
  };
  ep.handle_message(
    hb_at,
    &mut log,
    &mut stable,
    2u64,
    Message::HeartbeatResponse(
      HeartbeatResponse::new(Term::new(1), 2u64, bytes::Bytes::new())
        .with_lease_round(lease_round)
        .with_lease_support(Duration::from_millis(1000)),
    ),
  );
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}
  assert!(
    ep.lease_read_available(hb_at.into()),
    "the lease is live before the freeze"
  );

  let _ = ep
    .propose_merge_entry(
      hb_at,
      &mut log,
      EntryKind::PrepareMerge,
      prepare_payload(b"\x2b", 1),
    )
    .expect("the source leader appends the freeze");
  assert!(
    !ep.lease_read_available(hb_at.into()),
    "the lease dies at the freeze's APPEND, not its commit or apply"
  );
  // Behavioral: the read is still ACCEPTED, but degrades to the Safe heartbeat round.
  ep.read_index(hb_at, &log, &stable, bytes::Bytes::from_static(b"r"))
    .expect("reads stay accepted while the freeze is pending");
  assert!(
    ep.poll_event().is_none(),
    "no immediate ReadState — the lease shortcut is dead"
  );
  let mut read_hb = false;
  while let Some(out) = ep.poll_message() {
    if let Message::Heartbeat(hb) = out.message()
      && !hb.context().is_empty()
    {
      read_hb = true;
    }
  }
  assert!(read_hb, "the read degraded to the Safe heartbeat round");
}

/// The LeaseGuard half of the same row: a live committed current-term anchor stops serving the
/// moment the freeze is proposed (the anchor gate fails closed), same clock-free ordering.
#[test]
fn pending_freeze_kills_the_leaseguard_anchor_at_propose() {
  use crate::{AppendResponse, ReadOnlyOption};
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_read_only(ReadOnlyOption::LeaseGuard)
  .with_lease_duration(Duration::from_millis(500))
  .with_clock_drift_bound(Duration::from_millis(10));
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = NoopStable::default();
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
  ep.handle_storage(d, &mut log, &mut stable);
  // Commit the stamped no-op: the current-term committed anchor the LeaseGuard serve keys on.
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendResponse(AppendResponse::new(
      Term::new(1),
      2u64,
      false,
      Index::ZERO,
      Term::ZERO,
      Index::new(1),
    )),
  );
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}
  assert!(
    ep.lease_guard_read_live(d.into(), &log),
    "the anchor is live before the freeze"
  );

  let _ = ep
    .propose_merge_entry(
      d,
      &mut log,
      EntryKind::PrepareMerge,
      prepare_payload(b"\x2b", 1),
    )
    .expect("the source leader appends the freeze");
  assert!(
    !ep.lease_guard_read_live(d.into(), &log),
    "the anchor serve dies at the freeze's APPEND"
  );
}

/// While a freeze is PENDING no refresh no-op is appended: the pending window's own guards and
/// the merge kill overlap here (the freeze append leaves `last > commit`), so this pins the
/// BEHAVIOR — nothing re-anchors the lease once the freeze enters the log; the frozen-phase
/// variant (where the kill alone carries it) rides the freeze-apply suite.
#[test]
fn pending_freeze_appends_no_proactive_refresh() {
  use crate::{AppendResponse, LeaseRefresh, ReadOnlyOption};
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_read_only(ReadOnlyOption::LeaseGuard)
  .with_lease_duration(Duration::from_millis(500))
  .with_clock_drift_bound(Duration::from_millis(10))
  .with_lease_refresh(LeaseRefresh::Continuous);
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = NoopStable::default();
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
  ep.handle_storage(d, &mut log, &mut stable);
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendResponse(AppendResponse::new(
      Term::new(1),
      2u64,
      false,
      Index::ZERO,
      Term::ZERO,
      Index::new(1),
    )),
  );
  // A served read arms `read_since_anchor` (the proactive gate's demand signal).
  ep.read_index(d, &log, &stable, bytes::Bytes::from_static(b"r"))
    .expect("leaseguard read accepted");
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  let _ = ep
    .propose_merge_entry(
      d,
      &mut log,
      EntryKind::PrepareMerge,
      prepare_payload(b"\x2b", 1),
    )
    .expect("freeze appended");
  let last_before = log.last_index();
  // Fire the next heartbeat tick: with the freeze pending, Continuous must append NOTHING.
  let hb_at = ep.poll_timeout().expect("heartbeat timer armed");
  ep.handle_timeout(hb_at, &mut log, &mut stable);
  assert_eq!(
    log.last_index(),
    last_before,
    "no refresh no-op while a freeze is pending"
  );
  assert_eq!(ep.lease_refreshes(), 0, "the proactive counter never moved");
}

/// Encode a SOURCE-role `RollbackMerge` payload (the relayed thaw).
fn rollback_payload(source_gen_after: u64) -> bytes::Bytes {
  let p = crate::RollbackMergePayload::unfreeze(source_gen_after);
  let mut buf = Vec::new();
  crate::wire::encode_rollback_merge_payload(&p, &mut buf);
  bytes::Bytes::from(buf)
}

/// Encode a TARGET-role `RollbackMerge` payload (the abort).
fn abort_payload(
  source: &'static [u8],
  source_gen_after: u64,
  target_gen_after: u64,
) -> bytes::Bytes {
  let p = crate::RollbackMergePayload::abort(
    bytes::Bytes::from_static(source),
    source_gen_after,
    target_gen_after,
  );
  let mut buf = Vec::new();
  crate::wire::encode_rollback_merge_payload(&p, &mut buf);
  bytes::Bytes::from(buf)
}

/// Commit-and-apply everything through `upto` on a 3-voter leader by acking from node 2.
fn ack_through(
  ep: &mut Endpoint<u64, CountSm>,
  log: &mut VecLog,
  stable: &mut AsyncStable,
  upto: Index,
) {
  use crate::AppendResponse;
  ep.handle_storage(Instant::ORIGIN, log, stable);
  ep.handle_message(
    Instant::ORIGIN,
    log,
    stable,
    2u64,
    Message::AppendResponse(AppendResponse::new(
      Term::new(1),
      2u64,
      false,
      Index::ZERO,
      Term::ZERO,
      upto,
    )),
  );
  ep.handle_storage(Instant::ORIGIN, log, stable);
}

/// The freeze fold: a committed `PrepareMerge` applies as full `Frozen` — the boundary recorded,
/// the lineage bumped to the minted gen, the pending kill subsumed, `Event::Frozen` emitted.
#[test]
fn prepare_merge_apply_freezes_and_bumps_gen() {
  let (mut ep, mut log, mut stable) = make_three_voter_leader();
  let f = ep
    .propose_merge_entry(
      Instant::ORIGIN,
      &mut log,
      EntryKind::PrepareMerge,
      prepare_payload(b"\x2b", 1),
    )
    .unwrap();
  ack_through(&mut ep, &mut log, &mut stable, f);
  assert!(ep.is_frozen(), "the committed freeze applied");
  assert_eq!(ep.freeze_index(), Some(f));
  assert_eq!(ep.shape_gen(), 1, "lineage bumped to the minted gen");
  assert_eq!(
    ep.merge.freeze_pending, None,
    "pending subsumed into frozen"
  );
  let mut saw_frozen = false;
  while let Some(ev) = ep.poll_event() {
    saw_frozen |= matches!(ev, crate::Event::Frozen);
  }
  assert!(saw_frozen, "Event::Frozen surfaced");
}

/// The spec's §4 gate table: every typed surface refuses on a FROZEN group — proposals, conf
/// changes (the `conf_change_frozen_rejected` row's source half), read-mode migrations, leader
/// transfers, and reads on both roles; forwarded reads draw a REJECTING reply, not a black hole.
#[test]
fn conf_change_frozen_rejected() {
  use crate::{ConfChange, ConfChangeType, ProposeError, ReadIndexError, TransferError};
  let (mut ep, mut log, mut stable) = make_three_voter_leader();
  let f = ep
    .propose_merge_entry(
      Instant::ORIGIN,
      &mut log,
      EntryKind::PrepareMerge,
      prepare_payload(b"\x2b", 1),
    )
    .unwrap();
  ack_through(&mut ep, &mut log, &mut stable, f);
  assert!(ep.is_frozen());
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  let cmd = bytes::Bytes::from_static(b"w");
  assert!(matches!(
    ep.propose(Instant::ORIGIN, &mut log, &stable, &cmd),
    Err(ProposeError::Frozen)
  ));
  assert!(matches!(
    ep.propose_conf_change(
      Instant::ORIGIN,
      &mut log,
      &stable,
      ConfChange::new(ConfChangeType::AddNode, 4u64, bytes::Bytes::new()),
    ),
    Err(ProposeError::Frozen)
  ));
  assert!(matches!(
    ep.propose_read_mode_change(
      Instant::ORIGIN,
      &mut log,
      &stable,
      crate::ReadOnlyOption::Safe
    ),
    Err(ProposeError::Frozen)
  ));
  assert!(matches!(
    ep.transfer_leader(Instant::ORIGIN, &log, &stable, 2u64),
    Err(TransferError::Frozen)
  ));
  assert!(matches!(
    ep.read_index(
      Instant::ORIGIN,
      &log,
      &stable,
      bytes::Bytes::from_static(b"r")
    ),
    Err(ReadIndexError::Frozen)
  ));
  // A forwarded read draws a rejecting ReadIndexResponse (never a silent drop).
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    2u64,
    Message::ReadIndex(crate::ReadIndex::new(
      Term::new(1),
      2u64,
      bytes::Bytes::from_static(b"tok"),
    )),
  );
  let mut rejected = false;
  while let Some(out) = ep.poll_message() {
    if let Message::ReadIndexResponse(r) = out.message() {
      rejected |= r.reject();
    }
  }
  assert!(rejected, "a frozen leader rejects forwarded reads typed");

  // A frozen FOLLOWER fails local reads closed too.
  let (mut fep, mut flog, mut fstable) = make_merge_follower();
  fep.handle_message(
    Instant::ORIGIN,
    &mut flog,
    &mut fstable,
    1u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(1),
      1u64,
      Index::ZERO,
      Term::ZERO,
      std::vec![Entry::new(
        Term::new(1),
        Index::new(1),
        EntryKind::PrepareMerge,
        prepare_payload(b"\x2b", 1),
      )],
      Index::new(1),
    )),
  );
  assert!(
    fep.is_frozen(),
    "the committed freeze applied on the follower"
  );
  assert!(matches!(
    fep.read_index(
      Instant::ORIGIN,
      &flog,
      &fstable,
      bytes::Bytes::from_static(b"r")
    ),
    Err(ReadIndexError::Frozen)
  ));
}

/// The absorb-determinism gate: proposals refuse from the freeze's APPEND, not only its apply.
/// Every target replica absorbs its LOCAL source at its own apply progress, so an entry accepted
/// in the append→apply window would ride above the freeze on every log — present in some hosts'
/// absorbed state, missing from others' — or vanish from the union outright.
#[test]
fn pending_freeze_blocks_proposals_before_apply() {
  use crate::ProposeError;
  let (mut ep, mut log, _stable) = make_three_voter_leader();
  let stable = NoopStable::default();
  let _ = ep
    .propose_merge_entry(
      Instant::ORIGIN,
      &mut log,
      EntryKind::PrepareMerge,
      prepare_payload(b"\x2b", 1),
    )
    .unwrap();
  assert!(!ep.is_frozen(), "still only pending");
  let cmd = bytes::Bytes::from_static(b"w");
  assert!(
    matches!(
      ep.propose(Instant::ORIGIN, &mut log, &stable, &cmd),
      Err(ProposeError::Frozen)
    ),
    "the append-window gate holds before apply"
  );
}

/// The spec's §4 "UNCHANGED" half: a frozen group stays LIVE — its leader heartbeats and pumps
/// a behind follower the freeze suffix (catch-up to the boundary), and a frozen node still
/// grants votes (leader crashes survive the freeze).
#[test]
fn frozen_replication_and_elections_run_unchanged() {
  use crate::{HeartbeatResponse, RequestVote};
  let (mut ep, mut log, mut stable) = make_three_voter_leader();
  let f = ep
    .propose_merge_entry(
      Instant::ORIGIN,
      &mut log,
      EntryKind::PrepareMerge,
      prepare_payload(b"\x2b", 1),
    )
    .unwrap();
  ack_through(&mut ep, &mut log, &mut stable, f);
  assert!(ep.is_frozen());
  while ep.poll_message().is_some() {}

  // Heartbeats still broadcast on the frozen leader.
  let hb_at = ep.poll_timeout().expect("heartbeat timer armed");
  ep.handle_timeout(hb_at, &mut log, &mut stable);
  let mut beats = 0;
  while let Some(out) = ep.poll_message() {
    if matches!(out.message(), Message::Heartbeat(_)) {
      beats += 1;
    }
  }
  assert!(beats >= 2, "a frozen leader keeps heartbeating its peers");

  // A behind responder (node 3, match 0) still draws the catch-up append carrying the freeze.
  ep.handle_message(
    hb_at,
    &mut log,
    &mut stable,
    3u64,
    Message::HeartbeatResponse(HeartbeatResponse::new(
      Term::new(1),
      3u64,
      bytes::Bytes::new(),
    )),
  );
  let mut freeze_pumped = false;
  while let Some(out) = ep.poll_message() {
    if let Message::AppendEntries(ae) = out.message() {
      freeze_pumped |= ae
        .entries()
        .iter()
        .any(|e| e.kind() == EntryKind::PrepareMerge);
    }
  }
  assert!(
    freeze_pumped,
    "a frozen leader still replicates the freeze suffix to a behind peer"
  );

  // A frozen FOLLOWER still grants a legitimate higher-term vote.
  let (mut fep, mut flog, mut fstable) = make_merge_follower();
  fep.handle_message(
    Instant::ORIGIN,
    &mut flog,
    &mut fstable,
    1u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(1),
      1u64,
      Index::ZERO,
      Term::ZERO,
      std::vec![Entry::new(
        Term::new(1),
        Index::new(1),
        EntryKind::PrepareMerge,
        prepare_payload(b"\x2b", 1),
      )],
      Index::new(1),
    )),
  );
  assert!(fep.is_frozen());
  fep.handle_message(
    Instant::ORIGIN,
    &mut flog,
    &mut fstable,
    3u64,
    Message::RequestVote(RequestVote::new(
      Term::new(5),
      3u64,
      Index::new(9),
      Term::new(4),
      false,
      false,
    )),
  );
  fep.handle_storage(Instant::ORIGIN, &mut flog, &mut fstable);
  let mut granted = false;
  while let Some(out) = fep.poll_message() {
    if let Message::VoteResponse(v) = out.message() {
      granted |= !v.reject();
    }
  }
  assert!(granted, "a frozen follower still votes");
}

/// `RollbackMerge` is the release valve: it applies as unfreeze (gen moved past the freeze,
/// `Event::MergeRolledBack`), proposals resume, and leases are NOT resurrected — the lease
/// machinery re-forms from live traffic.
#[test]
fn rollback_merge_apply_unfreezes() {
  let (mut ep, mut log, mut stable) = make_three_voter_leader();
  let f = ep
    .propose_merge_entry(
      Instant::ORIGIN,
      &mut log,
      EntryKind::PrepareMerge,
      prepare_payload(b"\x2b", 1),
    )
    .unwrap();
  ack_through(&mut ep, &mut log, &mut stable, f);
  assert!(ep.is_frozen());
  let r = ep
    .propose_merge_entry(
      Instant::ORIGIN,
      &mut log,
      EntryKind::RollbackMerge,
      rollback_payload(2),
    )
    .expect("RollbackMerge is the one proposable entry while frozen");
  ack_through(&mut ep, &mut log, &mut stable, r);
  assert!(!ep.is_frozen(), "the rollback thawed the group");
  assert_eq!(ep.freeze_index(), None);
  assert_eq!(ep.shape_gen(), 2, "gen moved PAST the freeze generation");
  assert!(!ep.merge_freeze_active(), "lease formation may resume");
  let mut saw = false;
  while let Some(ev) = ep.poll_event() {
    saw |= matches!(ev, crate::Event::MergeRolledBack);
  }
  assert!(saw, "Event::MergeRolledBack surfaced");
  // Proposals resume.
  let cmd = bytes::Bytes::from_static(b"w");
  assert!(ep.propose(Instant::ORIGIN, &mut log, &stable, &cmd).is_ok());
}

/// A rollback's clear is a RE-DERIVATION, not a flag drop: a LATER freeze already appended
/// above the rollback keeps the append-observed kill armed through the thaw.
#[test]
fn rollback_rederives_a_later_pending_freeze() {
  let (mut ep, mut log, mut stable) = make_three_voter_leader();
  let f1 = ep
    .propose_merge_entry(
      Instant::ORIGIN,
      &mut log,
      EntryKind::PrepareMerge,
      prepare_payload(b"\x2b", 1),
    )
    .unwrap();
  ack_through(&mut ep, &mut log, &mut stable, f1);
  assert!(ep.is_frozen());
  let r = ep
    .propose_merge_entry(
      Instant::ORIGIN,
      &mut log,
      EntryKind::RollbackMerge,
      rollback_payload(2),
    )
    .unwrap();
  // A SECOND freeze lands above the rollback before the rollback commits (the entry plumbing
  // permits it; the container's verb gates make it rare). Fold order is total either way.
  let f2 = ep
    .propose_merge_entry(
      Instant::ORIGIN,
      &mut log,
      EntryKind::PrepareMerge,
      prepare_payload(b"\x2b", 3),
    )
    .unwrap();
  // Commit through the ROLLBACK only: the thaw must re-arm the pending kill at f2.
  ack_through(&mut ep, &mut log, &mut stable, r);
  assert!(!ep.is_frozen(), "thawed at the rollback");
  assert_eq!(
    ep.merge.freeze_pending,
    Some(f2),
    "the rollback re-derived the LATER pending freeze"
  );
  assert!(ep.merge_freeze_active(), "the kill never lapsed");
}

/// Replay idempotence: a restart whose durable log holds the committed freeze re-applies it to
/// the identical fold — same frozen state, same boundary, same lineage — however many times.
#[test]
fn freeze_replay_is_idempotent_across_restart() {
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut log = VecLog::default();
  let mut stable = NoopStable::default();
  log.force_append(&[Entry::new(
    Term::new(1),
    Index::new(1),
    EntryKind::PrepareMerge,
    prepare_payload(b"\x2b", 1),
  )]);
  stable.force_state(Term::new(1), Some(1u64), Index::new(1));
  for boot in 1..=2u64 {
    let ep = Endpoint::restart(
      cfg.clone(),
      Instant::ORIGIN,
      7,
      CountSm::default(),
      boot,
      &mut log,
      &mut stable,
    );
    assert!(!ep.is_poisoned());
    assert!(ep.is_frozen(), "replay re-froze (boot {boot})");
    assert_eq!(ep.freeze_index(), Some(Index::new(1)));
    assert_eq!(ep.shape_gen(), 1, "gen max-fold is idempotent");
    assert_eq!(ep.merge.freeze_pending, None);
  }
}

/// THE MERGE REPLAY FENCE: a frozen (or freezing) endpoint captures NO snapshot — a capture at
/// or past the freeze would compact the very entry whose replay re-derives `frozen` at restart,
/// and the replica would come back thawed while a target still holds a parked absorb of it. The
/// fence lifts with an explicit rollback.
#[test]
fn frozen_source_captures_no_snapshot() {
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_snapshot_threshold(1);
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 42, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();
  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable);
  ep.handle_storage(d, &mut log, &mut stable); // single voter: leader, no-op@1 commits+applies
  assert!(ep.role().is_leader());
  let f = ep
    .propose_merge_entry(
      d,
      &mut log,
      EntryKind::PrepareMerge,
      prepare_payload(b"\x2b", 1),
    )
    .unwrap();
  ep.handle_storage(d, &mut log, &mut stable); // commits + applies the freeze (quorum of one)
  assert!(ep.is_frozen());
  assert_eq!(f, Index::new(2));
  // applied(2) - first_index(1) >= threshold(1): the capture WOULD fire — the fence refuses.
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(
    stable.snapshot().is_none(),
    "no capture while frozen: the freeze entry must stay replayable"
  );
  // The rollback lifts the fence; the very next crank captures.
  let r = ep
    .propose_merge_entry(d, &mut log, EntryKind::RollbackMerge, rollback_payload(2))
    .unwrap();
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(!ep.is_frozen());
  assert_eq!(r, Index::new(3));
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(
    stable.snapshot().is_some(),
    "the fence lifted with the rollback"
  );
}

/// THE ABORT REPLAY FENCE (the merge/split fence family, abort edition): a TARGET-side abort records
/// its `abandoned` obligation durable-derived from the abort entry, re-derivable solely by replaying
/// it. A capture at-or-past that entry compacts it, and a restart from the snapshot then re-derives
/// no obligation with the source possibly still frozen — a permanent frozen-source wedge.
/// `maybe_snapshot` refuses while the obligation is outstanding; the fence lifts once the service
/// discharges it (the source observed thawed, modeled here by `clear_abandoned`).
///
/// RED without the fence: the capture below lands, compaction erases the abort entry, and a later
/// restart re-derives no `abandoned` — the frozen-source wedge.
#[test]
fn outstanding_abort_relay_captures_no_snapshot() {
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_snapshot_threshold(1);
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 42, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();
  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable);
  ep.handle_storage(d, &mut log, &mut stable); // single voter: leader, no-op@1 commits+applies
  assert!(ep.role().is_leader());
  // A TARGET-side abort at the live mint (target_gen_after = 1 against base 0): it applies, bumps
  // the lineage, and records exactly one `abandoned` obligation durable-derived from the entry.
  let a = ep
    .propose_merge_entry(
      d,
      &mut log,
      EntryKind::RollbackMerge,
      abort_payload(b"\x2a", 1, 1),
    )
    .unwrap();
  ep.handle_storage(d, &mut log, &mut stable); // commits + applies the abort (quorum of one)
  assert_eq!(a, Index::new(2));
  assert_eq!(ep.shape_gen(), 1, "the abort bumped the lineage");
  // applied(2) - first_index(1) >= threshold(1): the capture WOULD fire — the fence refuses while
  // the abort obligation is outstanding, so the abort entry stays replayable.
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(
    stable.snapshot().is_none(),
    "no capture while an abort obligation is outstanding: the abort entry must stay replayable"
  );
  // The service discharges it (the source observed thawed past the abandoned freeze), modeled by
  // clearing it here. THE NEGATIVE PIN: with no outstanding obligation the fence does not
  // over-block — the very next crank captures.
  assert!(ep.has_abandoned());
  ep.clear_abandoned(&bytes::Bytes::from_static(b"\x2a"));
  assert!(!ep.has_abandoned());
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(
    stable.snapshot().is_some(),
    "the fence lifted once the obligation discharged — compaction proceeds normally"
  );
}

/// The `abandoned` obligation's restart derivation — the recovery the fence PROTECTS. A restart whose
/// durable log still holds the committed abort entry re-applies it and RE-DERIVES `abandoned` (with
/// its abandoned freeze generation intact), exactly like `frozen_for`, so the source can still be
/// thawed. Had a capture compacted past the abort — which the fence forbids — the entry would be gone
/// and the obligation lost: the permanent frozen-source wedge.
#[test]
fn restart_re_derives_the_abort_relay() {
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut log = VecLog::default();
  let mut stable = NoopStable::default();
  // A durable log holding one committed TARGET-side abort at index 1 (mint = 1 against base 0),
  // abandoning source freeze generation 4.
  log.force_append(&[Entry::new(
    Term::new(1),
    Index::new(1),
    EntryKind::RollbackMerge,
    abort_payload(b"\x2a", 4, 1),
  )]);
  stable.force_state(Term::new(1), Some(1u64), Index::new(1));
  let ep = Endpoint::restart(
    cfg,
    Instant::ORIGIN,
    7,
    CountSm::default(),
    1,
    &mut log,
    &mut stable,
  );
  assert!(!ep.is_poisoned());
  assert_eq!(ep.shape_gen(), 1, "replay re-bumped the abort's lineage");
  // The obligation is BACK: replaying the surviving abort entry re-derived it, so the service can
  // re-drive the source thaw — the source is never wedged frozen.
  let (_source, source_gen_after, abort_index) = ep
    .abandoned_obligations()
    .first()
    .cloned()
    .expect("replay re-derived the abandoned obligation from the surviving entry");
  assert_eq!(
    source_gen_after, 4,
    "the abandoned freeze generation survived the restart"
  );
  assert_eq!(
    abort_index,
    Index::new(1),
    "the fence boundary re-derives to the replayed entry's index"
  );
}

/// THE ABORT INSTALL CLEAR (the fence family, install edition): a snapshot install re-baselines a
/// follower's log to a LEADER's boundary — a floor-advance NO local fenced capture produced —
/// discarding an abort entry at-or-below it. That entry is the `abandoned` obligation's ONLY restart
/// re-derivation, and with the source thawed and gone the service can never observe it advance to
/// discharge it. So the install CLEARS an obligation its boundary covers: the boundary sits past the
/// committed+applied abort, proving the source thawed past the abandoned freeze (the capturing
/// leader's own service drove it). Without the clear the stranded obligation pins `abort_relay_fences`
/// on a boundary the install already crossed — a permanent target-capture wedge with the abort entry
/// gone.
///
/// RED without the clear: after the install `abandoned` stays set with its entry compacted, so a
/// LATER `maybe_snapshot` is fenced forever and never captures.
#[test]
fn snapshot_install_retires_the_covered_abort_relay() {
  use crate::{InstallSnapshot, SnapshotMeta, conf::ConfState};
  use core::time::Duration;
  let cfg = Config::try_new(
    2u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_snapshot_threshold(1);
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 7, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();
  // The follower applies a TARGET-side abort at index 2 (mint 1 against base 0): it records exactly
  // one `abandoned` obligation (abort_index = 2) and bumps the lineage.
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
      std::vec![
        Entry::new(
          Term::new(1),
          Index::new(1),
          EntryKind::Normal,
          encode_cmd(b"a")
        ),
        Entry::new(
          Term::new(1),
          Index::new(2),
          EntryKind::RollbackMerge,
          abort_payload(b"\x2a", 3, 1),
        ),
      ],
      Index::new(2),
    )),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  assert_eq!(ep.applied_index(), Index::new(2), "the abort applied");
  assert_eq!(ep.shape_gen(), 1, "the abort bumped the lineage");
  assert!(
    ep.abort_relay_fences(ep.applied_index()),
    "the outstanding abort obligation fences target compaction"
  );

  // The target leader's post-abort snapshot lands (boundary 5 > commit 2 — a non-redundant install),
  // the source ABSENT (this endpoint hosts none to thaw). The re-baseline discards the abort entry
  // AND must clear the now-moot obligation.
  let meta = SnapshotMeta::new(
    Index::new(5),
    Term::new(4),
    ConfState::from_voters(std::vec![1u64, 2u64, 3u64]),
  );
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::InstallSnapshot(InstallSnapshot::new(
      Term::new(4),
      1u64,
      meta,
      encode_snapshot(42),
    )),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  assert_eq!(ep.applied_index(), Index::new(5), "the install landed");
  // GREEN: the boundary (5 >= abort_index 2) cleared the obligation — the fence lifts for every later
  // boundary. RED (no clear): the obligation is stranded with its entry compacted, so this stays true.
  assert!(
    !ep.abort_relay_fences(Index::new(1_000)),
    "the covering install cleared the obligation — the fence lifts"
  );

  // END TO END: the fence really is gone — a LATER maybe_snapshot captures. Append and apply two
  // entries ABOVE the boundary (threshold 1), then a storage crank captures. RED: still fenced, so
  // maybe_snapshot refuses and no snapshot is ever written.
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(4),
      1u64,
      Index::new(5),
      Term::new(4),
      std::vec![
        Entry::new(
          Term::new(4),
          Index::new(6),
          EntryKind::Normal,
          encode_cmd(b"b")
        ),
        Entry::new(
          Term::new(4),
          Index::new(7),
          EntryKind::Normal,
          encode_cmd(b"c")
        ),
      ],
      Index::new(7),
    )),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  assert_eq!(
    ep.applied_index(),
    Index::new(7),
    "the post-install tail applied"
  );
  assert!(
    stable.snapshot().is_some(),
    "the cleared obligation no longer fences — the later capture proceeds"
  );
}

/// SYMMETRY (the negative pin): a covering install CLEARS `abandoned` only when its boundary spans
/// the abort entry. An obligation whose abort entry sits ABOVE the boundary is RETAINED — the install
/// does not prove the source past THAT freeze, so its fence correctly still holds (the
/// `abort_index <= boundary` test). The real install path never carries an above-boundary abort (a
/// non-redundant install re-baselines strictly above `commit >= applied >= abort_index`); this pins
/// the clear predicate directly so a refactor cannot silently clear an uncovered obligation.
#[test]
fn install_retires_only_the_covered_abort_relays() {
  let (mut ep, _log, _stable) = make_merge_follower();
  // COVERED: boundary 5 spans the abort entry at 3 → cleared, the fence lifts.
  ep.note_abandoned(bytes::Bytes::from_static(b"\x2a"), 1, Index::new(3));
  ep.note_abort_rebaselined(Index::new(5));
  assert!(
    !ep.has_abandoned(),
    "the covered obligation (abort_index 3) cleared"
  );
  assert!(
    !ep.abort_relay_fences(Index::new(4)),
    "nothing fences once the covered obligation cleared"
  );
  // UNCOVERED: boundary 5 does NOT span the abort entry at 8 → retained, the fence still holds.
  ep.note_abandoned(bytes::Bytes::from_static(b"\x2b"), 1, Index::new(8));
  ep.note_abort_rebaselined(Index::new(5));
  assert_eq!(
    ep.abandoned_obligations().first().map(|(_, _, at)| *at),
    Some(Index::new(8)),
    "the uncovered obligation (abort_index 8) is retained"
  );
  assert!(
    ep.abort_relay_fences(Index::new(8)),
    "the uncovered obligation still fences"
  );
}

/// The `abandoned` COLLECTION's per-source semantics — the concurrent-fan-in fix and its replay
/// idempotence, pinned at the endpoint. A target that aborts several sources keeps ONE obligation per
/// source (a single-slot record silently dropped all but one, wedging the rest frozen), and a replayed
/// abort — the same source and generation re-applied on restart — must NOT double-insert, while a
/// re-freeze of that source (a higher generation) replaces the spent obligation LAST-WINS.
#[test]
fn note_abandoned_is_per_source_and_replay_idempotent() {
  let (mut ep, _log, _stable) = make_merge_follower();
  // Two DISTINCT sources abort into this one target — both obligations coexist (fan-in).
  ep.note_abandoned(bytes::Bytes::from_static(b"\x2a"), 1, Index::new(3));
  ep.note_abandoned(bytes::Bytes::from_static(b"\x2b"), 1, Index::new(4));
  assert_eq!(
    ep.abandoned_obligations().len(),
    2,
    "each source keeps its own obligation — neither dropped"
  );
  // NEGATIVE PIN — replay idempotence: re-applying the SAME source's abort (same generation, same
  // entry index, as a restart replay would) does NOT grow the collection.
  ep.note_abandoned(bytes::Bytes::from_static(b"\x2a"), 1, Index::new(3));
  assert_eq!(
    ep.abandoned_obligations().len(),
    2,
    "a replayed duplicate abort does not double-insert"
  );
  // LAST-WINS on a re-freeze: the same source, a HIGHER generation and a later entry, replaces the
  // spent obligation in place (its earlier one was discharged before the re-freeze could exist).
  ep.note_abandoned(bytes::Bytes::from_static(b"\x2a"), 3, Index::new(9));
  let obligations = ep.abandoned_obligations();
  assert_eq!(obligations.len(), 2, "still one obligation per source");
  let a = obligations
    .iter()
    .find(|(s, ..)| *s == bytes::Bytes::from_static(b"\x2a"))
    .expect("source 2a still tracked");
  assert_eq!(
    (a.1, a.2),
    (3, Index::new(9)),
    "the re-freeze's generation and abort index won last"
  );
  // Discharge is per-source: clearing one leaves the other's obligation and fence intact.
  ep.clear_abandoned(&bytes::Bytes::from_static(b"\x2a"));
  assert_eq!(ep.abandoned_obligations().len(), 1, "only 2a discharged");
  assert!(
    ep.abort_relay_fences(Index::new(4)),
    "source 2b's obligation still fences its abort entry"
  );
}

/// Encode a `CommitMerge` payload (freeze term pinned at 1 — the endpoint alone never reads
/// the identity pair; the container's service does).
fn commit_payload(
  source: &'static [u8],
  freeze_index: Index,
  source_gen_after: u64,
  target_gen_after: u64,
) -> bytes::Bytes {
  let p = crate::CommitMergePayload::new(
    bytes::Bytes::from_static(source),
    freeze_index,
    Term::new(1),
    source_gen_after,
    target_gen_after,
  );
  let mut buf = Vec::new();
  crate::wire::encode_commit_merge_payload(&p, &mut buf);
  bytes::Bytes::from(buf)
}

/// A parked 3-voter target leader: no-op@1 + N normal entries + CommitMerge@k, all committed —
/// the drain stops at k−1. Returns `(ep, log, stable, k)`.
fn make_parked_target(n: usize) -> (Endpoint<u64, CountSm>, VecLog, AsyncStable, Index) {
  let (mut ep, mut log, mut stable) = make_three_voter_leader();
  for i in 0..n {
    let cmd = bytes::Bytes::copy_from_slice(&[i as u8]);
    let _ = ep
      .propose(Instant::ORIGIN, &mut log, &stable, &cmd)
      .unwrap();
  }
  let k = ep
    .propose_merge_entry(
      Instant::ORIGIN,
      &mut log,
      EntryKind::CommitMerge,
      commit_payload(b"\x2a", Index::new(5), 1, 1),
    )
    .unwrap();
  ack_through(&mut ep, &mut log, &mut stable, k);
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}
  (ep, log, stable, k)
}

/// The park: a committed `CommitMerge` at k stops the drain at k−1 and stays parked across
/// storage cranks — while replication, reads, and ordinary proposals keep running (the target
/// is not frozen; entries land above k and apply after the resolution).
#[test]
fn commit_merge_apply_parks_at_k_minus_1() {
  use crate::ProposeError;
  let (mut ep, mut log, mut stable, k) = make_parked_target(2);
  assert_eq!(ep.applied_index(), Index::new(k.get() - 1), "parked at k-1");
  let pending = ep.pending_merge().expect("the park is recorded");
  assert_eq!(pending.at(), k);
  assert_eq!(pending.freeze_index(), Index::new(5));
  assert_eq!(pending.source_gen_after(), 1);
  assert_eq!(pending.source_bytes().as_ref(), b"\x2a");
  // The park holds across cranks (no busy-loop re-park, no progress).
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  assert_eq!(ep.applied_index(), Index::new(k.get() - 1));
  assert!(ep.pending_merge().is_some());
  // The target is NOT frozen: reads confirm and proposals land (above k).
  assert!(
    ep.read_index(
      Instant::ORIGIN,
      &log,
      &stable,
      bytes::Bytes::from_static(b"r")
    )
    .is_ok(),
    "reads confirm while parked"
  );
  let cmd = bytes::Bytes::from_static(b"w");
  assert!(
    ep.propose(Instant::ORIGIN, &mut log, &stable, &cmd).is_ok(),
    "ordinary proposals keep landing above the park"
  );
  // But membership is FENCED while parked (the log-walk hazard).
  assert!(matches!(
    ep.propose_conf_change(
      Instant::ORIGIN,
      &mut log,
      &stable,
      crate::ConfChange::new(crate::ConfChangeType::AddNode, 4u64, bytes::Bytes::new()),
    ),
    Err(ProposeError::MergeInFlight)
  ));
}

/// The membership fence's IN-FLIGHT leg: a proposed-but-uncommitted CommitMerge already fences
/// conf changes on its proposer (nothing else marks the window before the park exists).
#[test]
fn in_flight_commit_merge_fences_membership() {
  use crate::ProposeError;
  let (mut ep, mut log, _stable) = make_three_voter_leader();
  let stable = NoopStable::default();
  let _ = ep
    .propose_merge_entry(
      Instant::ORIGIN,
      &mut log,
      EntryKind::CommitMerge,
      commit_payload(b"\x2a", Index::new(5), 1, 1),
    )
    .unwrap();
  assert!(ep.pending_merge().is_none(), "not yet committed, no park");
  assert!(matches!(
    ep.propose_conf_change(
      Instant::ORIGIN,
      &mut log,
      &stable,
      crate::ConfChange::new(crate::ConfChangeType::AddNode, 4u64, bytes::Bytes::new()),
    ),
    Err(ProposeError::MergeInFlight)
  ));
}

/// The resolve: absorbing the extracted source FSM applies the parked entry — state folded,
/// lineage bumped, `Event::Merged` surfaced — and the drain resumes on the next crank.
#[test]
fn resolve_pending_merge_absorbs_and_resumes() {
  let (mut ep, mut log, mut stable, k) = make_parked_target(2);
  let before = ep.state_machine().count();
  let mut source = CountSm::default();
  for i in 0..3 {
    let _ = crate::StateMachine::apply(&mut source, Index::new(i + 1), bytes::Bytes::new());
  }
  ep.resolve_pending_merge(source);
  assert!(!ep.is_poisoned());
  assert_eq!(ep.applied_index(), k, "the parked entry applied");
  assert!(ep.pending_merge().is_none());
  assert_eq!(
    ep.state_machine().count(),
    before + 3,
    "the union folded in"
  );
  assert_eq!(ep.shape_gen(), 1, "lineage bumped to target_gen_after");
  let mut merged = false;
  while let Some(ev) = ep.poll_event() {
    if let crate::Event::Merged(m) = ev {
      merged = true;
      assert_eq!(m.index(), k);
      assert_eq!(m.source().as_ref(), b"\x2a");
    }
  }
  assert!(merged, "Event::Merged surfaced");
  // The drain RESUMES: a later committed entry applies on the next crank.
  let cmd = bytes::Bytes::from_static(b"after");
  let idx = ep
    .propose(Instant::ORIGIN, &mut log, &stable, &cmd)
    .unwrap();
  ack_through(&mut ep, &mut log, &mut stable, idx);
  assert_eq!(ep.applied_index(), idx, "post-merge entries apply normally");
}

/// The abort resolution: a no-op past k — FSM untouched, lineage untouched,
/// `Event::MergeAborted` surfaced.
#[test]
fn resolve_pending_merge_aborted_no_ops_past_k() {
  let (mut ep, mut log, mut stable, k) = make_parked_target(2);
  let before = ep.state_machine().count();
  ep.resolve_pending_merge_aborted();
  assert_eq!(ep.applied_index(), k);
  assert!(ep.pending_merge().is_none());
  assert_eq!(ep.state_machine().count(), before, "no absorb on abort");
  assert_eq!(ep.shape_gen(), 0, "no lineage bump on abort");
  let mut aborted = false;
  while let Some(ev) = ep.poll_event() {
    if let crate::Event::MergeAborted(a) = ev {
      aborted = true;
      assert_eq!(a.index(), k);
      assert_eq!(a.source().as_ref(), b"\x2a");
    }
  }
  assert!(aborted, "Event::MergeAborted surfaced");
  // The drain resumes here too.
  let cmd = bytes::Bytes::from_static(b"after");
  let idx = ep
    .propose(Instant::ORIGIN, &mut log, &stable, &cmd)
    .unwrap();
  ack_through(&mut ep, &mut log, &mut stable, idx);
  assert_eq!(ep.applied_index(), idx);
}

/// A restart with the commit-merge committed re-parks deterministically: the entry is
/// re-encountered by the replay drain and the pending apply is rebuilt from its payload.
#[test]
fn restart_reparks_a_committed_commit_merge() {
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut log = VecLog::default();
  let mut stable = NoopStable::default();
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
      EntryKind::CommitMerge,
      // A LIVE mint: the replayed counter walks to 0 here, so only target_gen_after 1 parks
      // (a replayed stale mint no-ops at the lineage guard instead of re-parking).
      commit_payload(b"\x2a", Index::new(7), 3, 1),
    ),
  ]);
  stable.force_state(Term::new(1), Some(1u64), Index::new(2));
  let ep = Endpoint::restart(
    cfg,
    Instant::ORIGIN,
    7,
    CountSm::default(),
    1,
    &mut log,
    &mut stable,
  );
  assert!(!ep.is_poisoned());
  assert_eq!(ep.applied_index(), Index::new(1), "re-parked at k-1");
  let pending = ep.pending_merge().expect("the park re-derived");
  assert_eq!(pending.at(), Index::new(2));
  assert_eq!(pending.freeze_index(), Index::new(7));
  assert_eq!(pending.source_gen_after(), 3);
  assert_eq!(pending.target_gen_after(), 1);
}

/// An FSM without `absorb` support poisons at resolve — deterministic on every replica, never a
/// silent skip that diverges absorbed replicas from refusing ones (mirror `SplitUnsupported`).
#[test]
fn absorb_unsupported_poisons() {
  struct NoAbsorbSm(usize);
  impl crate::StateMachine for NoAbsorbSm {
    type Command = bytes::Bytes;
    type Response = usize;
    type Snapshot = u64;
    type Error = core::convert::Infallible;
    fn apply(&mut self, _: Index, _: bytes::Bytes) -> Result<usize, Self::Error> {
      self.0 += 1;
      Ok(self.0)
    }
    fn snapshot(&self) -> Result<u64, Self::Error> {
      Ok(self.0 as u64)
    }
    fn restore(&mut self, s: u64) -> Result<(), Self::Error> {
      self.0 = s as usize;
      Ok(())
    }
  }
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut log = VecLog::default();
  let mut stable = NoopStable::default();
  log.force_append(&[Entry::new(
    Term::new(1),
    Index::new(1),
    EntryKind::CommitMerge,
    commit_payload(b"\x2a", Index::new(7), 1, 1),
  )]);
  stable.force_state(Term::new(1), Some(1u64), Index::new(1));
  let mut ep = Endpoint::restart(
    cfg,
    Instant::ORIGIN,
    7,
    NoAbsorbSm(0),
    1,
    &mut log,
    &mut stable,
  );
  assert!(ep.pending_merge().is_some(), "parked");
  ep.resolve_pending_merge(NoAbsorbSm(9));
  assert!(ep.is_poisoned(), "unsupported absorb fail-stops");
  assert_eq!(
    ep.poison_reason(),
    Some(crate::PoisonReason::MergeUnsupported)
  );
}

/// The membership fence's COMPACTION leg: after the resolve, conf changes stay fenced until the
/// forced absorb capture's compaction moves `first_index` past the absorb point — from then on
/// no replica can ever be log-walked across it (a fresh joiner is structurally snapshot-forced).
#[test]
fn merge_conf_fence_releases_with_the_capture() {
  use crate::ProposeError;
  let (mut ep, mut log, mut stable, k) = make_parked_target(1);
  assert!(!ep.absorb_capture_blocked(), "nothing else is staged");
  ep.resolve_pending_merge(CountSm::default());
  assert!(
    ep.capture_absorb_snapshot(&log, &mut stable),
    "the capture stages the durable anchor"
  );
  // Absorbed but not yet compacted: still fenced.
  assert!(matches!(
    ep.propose_conf_change(
      Instant::ORIGIN,
      &mut log,
      &stable,
      crate::ConfChange::new(crate::ConfChangeType::AddNode, 4u64, bytes::Bytes::new()),
    ),
    Err(ProposeError::MergeInFlight)
  ));
  // Drain the capture completion: blob durable → deferred compact runs → first_index > k.
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  assert!(
    log.first_index() > k,
    "the absorb capture compacted through the parked entry"
  );
  assert!(
    ep.propose_conf_change(
      Instant::ORIGIN,
      &mut log,
      &stable,
      crate::ConfChange::new(crate::ConfChangeType::AddNode, 4u64, bytes::Bytes::new()),
    )
    .is_ok(),
    "the fence releases once no log walk can cross the absorb"
  );
}

/// A parked 3-voter target leader that ALSO carries an OUTSTANDING `abandoned` obligation from a
/// DIFFERENT merge's abort committed BELOW the park: no-op@1, a TARGET-side abort@2 (applied below
/// the park, `abandoned` recorded), a CommitMerge@3 for another source (parked at k−1). Returns
/// `(ep, log, stable, abort_at, k)`.
fn make_parked_target_with_pending_abort()
-> (Endpoint<u64, CountSm>, VecLog, AsyncStable, Index, Index) {
  let (mut ep, mut log, mut stable) = make_three_voter_leader();
  let abort_at = ep
    .propose_merge_entry(
      Instant::ORIGIN,
      &mut log,
      EntryKind::RollbackMerge,
      abort_payload(b"\x2b", 3, 1),
    )
    .unwrap();
  let k = ep
    .propose_merge_entry(
      Instant::ORIGIN,
      &mut log,
      EntryKind::CommitMerge,
      commit_payload(b"\x2a", Index::new(5), 1, 2),
    )
    .unwrap();
  ack_through(&mut ep, &mut log, &mut stable, k);
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}
  (ep, log, stable, abort_at, k)
}

/// THE FORCED-ABSORB COMPACTION FENCE (the abort fence, absorb-capture edition — the site
/// `maybe_snapshot`'s fence alone missed): a target holding an OUTSTANDING `abandoned` obligation from
/// one merge, then resolving a DIFFERENT parked merge into the SAME target, runs the forced absorb
/// capture OUTSIDE `maybe_snapshot`. That capture stages `pending_compact` at the absorb boundary
/// `pending.at()` — PAST the earlier abort entry — so with no fence here it compacts the abort
/// entry while its obligation is still outstanding, erasing the obligation's only restart source and
/// wedging the source frozen forever. The absorb capture shares `maybe_snapshot`'s abort fence via
/// `abort_relay_fences`, so `absorb_capture_blocked` holds the park until the obligation discharges.
///
/// RED without the leg: `absorb_capture_blocked` returns false, the container's resolve arm captures,
/// and the deferred compaction moves `first_index` PAST the abort entry with the obligation live —
/// the entry (its only restart re-derivation) is gone and the park is consumed.
#[test]
fn outstanding_abort_relay_blocks_the_forced_absorb_capture() {
  let (mut ep, mut log, mut stable, abort_at, k) = make_parked_target_with_pending_abort();
  assert!(abort_at < k, "the abort committed below the park");
  assert_eq!(ep.pending_merge().expect("parked").at(), k);

  // Model the container's resolve arm EXACTLY: it resolves + forces the absorb capture ONLY when
  // the capture is not blocked. The fence must hold the park while the abort relay is outstanding.
  if !ep.absorb_capture_blocked() {
    ep.resolve_pending_merge(CountSm::default());
    assert!(ep.capture_absorb_snapshot(&log, &mut stable));
  }
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  // GREEN: the fence held the park, nothing compacted — the abort entry is RETAINED, so a
  // crash-restart re-derives `abandoned` (see `restart_re_derives_the_abort_relay`) and the source
  // stays thawable. RED (no leg): the arm resolved+captured and the deferred compaction crossed
  // `abort_at`, erasing the obligation's only restart source and consuming the park.
  assert!(
    log.first_index() <= abort_at,
    "the abort entry must survive: an absorb capture past it erases the obligation's restart source"
  );
  assert!(
    ep.pending_merge().is_some(),
    "the park is still held — the absorb waits for the obligation to discharge"
  );

  // NEGATIVE PIN: discharge the obligation (the service clears it once the source is observed
  // thawed, modeled here). The fence lifts and the forced absorb capture proceeds and compacts
  // through the park — exactly as it does for a target with no outstanding abort (no over-block).
  assert!(ep.has_abandoned());
  ep.clear_abandoned(&bytes::Bytes::from_static(b"\x2b"));
  assert!(
    !ep.absorb_capture_blocked(),
    "the fence lifts once the obligation discharges — no over-block"
  );
  ep.resolve_pending_merge(CountSm::default());
  assert!(
    ep.capture_absorb_snapshot(&log, &mut stable),
    "the capture stages the durable anchor once no abort relay is outstanding"
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  assert!(
    log.first_index() > k,
    "the absorb capture compacted through the parked entry once the fence lifted"
  );
}

/// A snapshot install at-or-past the parked entry SUPERSEDES the park: the union state arrives
/// wholesale in the blob (the target leader's forced absorb capture guarantees one exists past
/// every resolution), so a log-behind straggler that parked is caught up without ever touching
/// its local source. An install below the park clears it too — the replay re-encounters the
/// entry and re-parks from log-fixed data. Without the clear, the stale park wedges the apply
/// drain forever below a boundary it can never reach again.
#[test]
fn snapshot_install_supersedes_a_parked_commit_merge() {
  use crate::{InstallSnapshot, SnapshotMeta, conf::ConfState};
  let (mut ep, mut log, mut stable) = make_follower();
  // The follower parks: CommitMerge@2 committed (leader_commit = 2).
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
      std::vec![
        Entry::new(
          Term::new(1),
          Index::new(1),
          EntryKind::Normal,
          encode_cmd(b"a")
        ),
        Entry::new(
          Term::new(1),
          Index::new(2),
          EntryKind::CommitMerge,
          commit_payload(b"\x2a", Index::new(7), 1, 1),
        ),
      ],
      Index::new(2),
    )),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  assert!(ep.pending_merge().is_some(), "parked at k-1");
  assert_eq!(ep.applied_index(), Index::new(1));

  // The target leader's post-merge snapshot lands (boundary 10 >= k): the union arrives
  // wholesale — the park must clear and the boundary must apply.
  let meta = SnapshotMeta::new(
    Index::new(10),
    Term::new(4),
    ConfState::from_voters(std::vec![1u64, 2u64, 3u64]),
  );
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::InstallSnapshot(InstallSnapshot::new(
      Term::new(4),
      1u64,
      meta,
      encode_snapshot(42),
    )),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  assert_eq!(ep.applied_index(), Index::new(10), "the install landed");
  assert!(
    ep.pending_merge().is_none(),
    "the park is superseded by the installed union"
  );
  // The drain runs again: a later committed entry applies normally.
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(4),
      1u64,
      Index::new(10),
      Term::new(4),
      std::vec![Entry::new(
        Term::new(4),
        Index::new(11),
        EntryKind::Normal,
        encode_cmd(b"b"),
      )],
      Index::new(11),
    )),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  assert_eq!(ep.applied_index(), Index::new(11), "the drain resumed");
}

/// The freeze retains its CLAIM (the named target) for the whole frozen generation, and only
/// the thaw clears it — the claim is what lets exactly one target absorb or abort this freeze,
/// read host-order-independently off the frozen source.
#[test]
fn freeze_retains_its_claim_until_the_thaw() {
  let (mut ep, mut log, mut stable) = make_three_voter_leader();
  assert_eq!(ep.frozen_for(), None);
  let f = ep
    .propose_merge_entry(
      Instant::ORIGIN,
      &mut log,
      EntryKind::PrepareMerge,
      prepare_payload(b"\x2b", 1),
    )
    .unwrap();
  ack_through(&mut ep, &mut log, &mut stable, f);
  assert!(ep.is_frozen());
  assert_eq!(
    ep.frozen_for().map(|t| t.as_ref().to_vec()),
    Some(b"\x2b".to_vec()),
    "the claim is the freeze's named target"
  );
  let r = ep
    .propose_merge_entry(
      Instant::ORIGIN,
      &mut log,
      EntryKind::RollbackMerge,
      rollback_payload(2),
    )
    .unwrap();
  ack_through(&mut ep, &mut log, &mut stable, r);
  assert!(!ep.is_frozen());
  assert_eq!(ep.frozen_for(), None, "the thaw clears the claim");
}

/// Abort-before-commit, one log: the abort applies first (bumping the target's lineage), so
/// the later commit's mint is STALE at its own apply — it no-ops with `Event::MergeAborted`
/// and NO PARK EVER FORMS. The core "parks never form" ordering pin.
#[test]
fn abort_below_a_commit_kills_it_at_apply() {
  let (mut ep, mut log, mut stable) = make_three_voter_leader();
  // Both minted against base 0 (target_gen_after = 1): the log orders the abort first.
  let a = ep
    .propose_merge_entry(
      Instant::ORIGIN,
      &mut log,
      EntryKind::RollbackMerge,
      abort_payload(b"\x2a", 1, 1),
    )
    .unwrap();
  let k = ep
    .propose_merge_entry(
      Instant::ORIGIN,
      &mut log,
      EntryKind::CommitMerge,
      commit_payload(b"\x2a", Index::new(5), 1, 1),
    )
    .unwrap();
  assert_eq!(k, a.next());
  ack_through(&mut ep, &mut log, &mut stable, k);
  assert!(
    ep.pending_merge().is_none(),
    "the killed commit never parks"
  );
  assert_eq!(ep.applied_index(), k, "the drain ran straight through");
  assert_eq!(
    ep.shape_gen(),
    1,
    "exactly the abort's bump, not the commit's"
  );
  let aborted = core::iter::from_fn(|| ep.poll_event())
    .filter(|ev| matches!(ev, Event::MergeAborted(_)))
    .count();
  assert_eq!(
    aborted, 2,
    "the abort's own signal plus the killed commit's"
  );
  // Exactly one abandoned obligation — the applied abort's.
  assert!(ep.has_abandoned());
}

/// A TARGET-role abort with a STALE mint is a silent deterministic no-op: no lineage move, no
/// abandoned obligation, no event — the winner of its base already surfaced the definitive signal.
#[test]
fn stale_abort_is_a_silent_no_op() {
  let (mut ep, mut log, mut stable) = make_three_voter_leader();
  // Minted against base 2 (target_gen_after = 3) while the live counter sits at 0.
  let a = ep
    .propose_merge_entry(
      Instant::ORIGIN,
      &mut log,
      EntryKind::RollbackMerge,
      abort_payload(b"\x2a", 1, 3),
    )
    .unwrap();
  ack_through(&mut ep, &mut log, &mut stable, a);
  assert_eq!(ep.applied_index(), a, "applied as a no-op");
  assert_eq!(ep.shape_gen(), 0, "no lineage move");
  assert!(!ep.has_abandoned(), "no abandoned obligation");
  assert!(
    !core::iter::from_fn(|| ep.poll_event()).any(|ev| matches!(ev, Event::MergeAborted(_))),
    "no signal — the base's winner already spoke"
  );
}

/// The abort window reads the ONE committed coordinate after the parked entry: OPEN while
/// nothing committed there, ABORT on this merge's own abort, CLOSED on anything else
/// (including another merge's abort).
#[test]
fn merge_abort_window_reads_the_coordinate() {
  use crate::endpoint::MergeWindow;
  // Open: the parked entry is the last committed thing.
  let (ep, log, _stable, _k) = make_parked_target(1);
  assert_eq!(ep.merge_abort_window(&log), MergeWindow::Open);

  // Abort: the coordinate holds THIS merge's abort (same source, same freeze generation).
  let (mut ep, mut log, mut stable, k) = make_parked_target(1);
  let a = ep
    .propose_merge_entry(
      Instant::ORIGIN,
      &mut log,
      EntryKind::RollbackMerge,
      abort_payload(b"\x2a", 1, 1),
    )
    .unwrap();
  assert_eq!(a, k.next());
  ack_through(&mut ep, &mut log, &mut stable, a);
  assert!(
    ep.pending_merge().is_some(),
    "the park still blocks the drain"
  );
  assert_eq!(ep.merge_abort_window(&log), MergeWindow::Abort);

  // Closed: a DIFFERENT merge's abort at the coordinate is just another window-closing entry.
  let (mut ep, mut log, mut stable, k) = make_parked_target(1);
  let a = ep
    .propose_merge_entry(
      Instant::ORIGIN,
      &mut log,
      EntryKind::RollbackMerge,
      abort_payload(b"\x2c", 9, 1),
    )
    .unwrap();
  assert_eq!(a, k.next());
  ack_through(&mut ep, &mut log, &mut stable, a);
  assert_eq!(ep.merge_abort_window(&log), MergeWindow::Closed);

  // Closed: an ordinary entry at the coordinate.
  let (mut ep, mut log, mut stable, k) = make_parked_target(1);
  let cmd = bytes::Bytes::from_static(b"w");
  let w = ep
    .propose(Instant::ORIGIN, &mut log, &stable, &cmd)
    .unwrap();
  assert_eq!(w, k.next());
  ack_through(&mut ep, &mut log, &mut stable, w);
  assert_eq!(ep.merge_abort_window(&log), MergeWindow::Closed);
}

/// The seal: a parked LEADER whose log still ENDS at the parked entry appends exactly one
/// no-op at the coordinate (idempotent per park; anything already above `k` skips it), so a
/// quiet target cannot hold every replica's window open forever.
#[test]
fn merge_seal_appends_once_on_the_leader() {
  let (mut ep, mut log, _stable, k) = make_parked_target(0);
  assert_eq!(log.last_index(), k);
  assert!(ep.ensure_merge_seal(Instant::ORIGIN.into(), &mut log));
  assert_eq!(log.last_index(), k.next(), "one no-op at the coordinate");
  assert!(
    !ep.ensure_merge_seal(Instant::ORIGIN.into(), &mut log),
    "already sealed: idempotent per park"
  );
  assert_eq!(log.last_index(), k.next());
}
