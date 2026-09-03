use super::*;
use crate::{
  AppendEntries, Config, Entry, EntryKind, Index, Instant, Message, PrepareMergePayload, Term,
  VoteResponse, endpoint::Cover,
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
  assert_eq!(ep.freeze_pending(), Some(idx), "armed at append");
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
  assert_eq!(ep.freeze_pending(), Some(Index::new(1)));
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
  assert_eq!(ep.freeze_pending(), Some(Index::new(2)));

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
    ep.freeze_pending(),
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
    ep.freeze_pending(),
    None,
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
    ep.freeze_pending(),
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
  assert_eq!(ep.freeze_pending(), Some(Index::new(1)));
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
    ep.freeze_pending(),
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
  assert_eq!(ep.freeze_pending(), Some(Index::new(1)));

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
    ep.freeze_pending(),
    None,
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
/// the lineage bumped to the minted gen, the pending kill subsumed, `Event::MergeFrozen` emitted with the post-freeze lineage.
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
  assert_eq!(ep.freeze_pending(), None, "pending subsumed into frozen");
  let mut frozen_gen = None;
  while let Some(ev) = ep.poll_event() {
    if let crate::Event::MergeFrozen(e) = ev {
      frozen_gen = Some(e.gen_after());
    }
  }
  assert_eq!(
    frozen_gen,
    Some(1),
    "Event::MergeFrozen surfaced carrying the post-freeze lineage"
  );
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
  let mut thaw_gen = None;
  while let Some(ev) = ep.poll_event() {
    if let crate::Event::MergeRolledBack(e) = ev {
      thaw_gen = Some(e.gen_after());
    }
  }
  assert_eq!(
    thaw_gen,
    Some(2),
    "Event::MergeRolledBack surfaced carrying the post-thaw lineage"
  );
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
    ep.freeze_pending(),
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
    assert_eq!(ep.freeze_pending(), None);
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
/// Without the fence the capture below lands, compaction erases the abort entry, and a later
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
  assert!(ep.owes_live_thaw());
  ep.clear_abandoned(&bytes::Bytes::from_static(b"\x2a"));
  assert!(!ep.owes_live_thaw());
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
  let (_source, obligation) = ep
    .abandoned_obligations()
    .first()
    .cloned()
    .expect("replay re-derived the abandoned obligation from the surviving entry");
  assert_eq!(
    obligation.generation, 4,
    "the abandoned freeze generation survived the restart"
  );
  assert_eq!(
    obligation.abort_index,
    Index::new(1),
    "the fence boundary re-derives to the replayed entry's index"
  );
  assert!(
    obligation.cover == Cover::None && !obligation.discharged,
    "a replayed abort re-derives an UNCOVERED, LIVE obligation — its entry is in the log"
  );
}

/// THE ABORT INSTALL MARK (the fence family, install edition): a snapshot install re-baselines a
/// follower's log to a LEADER's boundary — a floor-advance NO local fenced capture produced —
/// discarding an abort entry at-or-below it. That entry is the `abandoned` obligation's only restart
/// re-derivation, and the boundary proves only that the transferring leader's own fence had lifted
/// there — which it does on a host-local escape as readily as on a thaw, with the source possibly
/// still frozen beside this holder. So the install MARKS the obligation install-covered and KEEPS
/// it, live: generation and abort index intact, still driven, still read by every live-obligation
/// gate, and disposed of only on a global fact (`note_abort_covered` is the single authority; the
/// container tests pin the disposal). What the mark does change is the FENCE: the entry the fence
/// protected is gone, so an install-covered record fences nothing, and a LATER `maybe_snapshot`
/// captures past the install with the record still standing.
#[test]
fn snapshot_install_marks_the_covered_abort_relay() {
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
  let source = bytes::Bytes::from_static(b"\x2a");
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
  assert_eq!(
    ep.abandoned_record(&source).map(|m| m.cover),
    Some(Cover::None),
    "a freshly applied abort is uncovered — its entry is in the log"
  );

  // The target leader's post-abort snapshot lands (boundary 5 > commit 2 — a non-redundant install).
  // The re-baseline discards the abort entry; the obligation it covered is marked and kept.
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
  assert!(
    ep.owes_live_thaw(),
    "the covered obligation is RETAINED across the install — the boundary proves no thaw"
  );
  let record = ep.abandoned_record(&source).expect("retained");
  assert_eq!(
    (
      record.generation,
      record.abort_index,
      record.cover,
      record.discharged
    ),
    (3, Index::new(2), Cover::Install, false),
    "MARKED install-covered, still live, generation and abort index intact"
  );
  assert!(
    !ep.abort_relay_fences(Index::new(1_000)),
    "an install-covered record fences nothing — the entry the fence protected is gone"
  );

  // END TO END: the fence really is down. Append and apply two entries ABOVE the boundary
  // (threshold 1); the storage crank captures at 7 with the record still standing.
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
  assert_eq!(
    stable.snapshot().map(|(m, _)| m.last_index()),
    Some(Index::new(7)),
    "the later capture proceeds past the install — the retained record does not fence it"
  );
  assert!(
    ep.owes_live_thaw(),
    "and the record still stands after the capture — disposal is the container's, on a global fact"
  );
}

/// SYMMETRY (the negative pin): a covering install MARKS `abandoned` only where its boundary spans
/// the abort entry, and it REMOVES nothing. An obligation whose abort entry sits ABOVE the boundary
/// stays unmarked — the install proves nothing about THAT freeze, and its re-delivered entry
/// re-applies to re-derive it (the `abort_index <= boundary` test) — so it keeps fencing, while the
/// install-covered one, its entry gone, fences no more. The real install path never carries an
/// above-boundary abort (a non-redundant install re-baselines strictly above
/// `commit >= applied >= abort_index`); this pins the mark predicate directly so a refactor cannot
/// silently mark an uncovered obligation — or drop a covered one.
#[test]
fn install_marks_only_the_covered_abort_relays() {
  let (mut ep, _log, _stable) = make_merge_follower();
  let covered = bytes::Bytes::from_static(b"\x2a");
  let uncovered = bytes::Bytes::from_static(b"\x2b");
  // COVERED: boundary 5 spans the abort entry at 3 → marked, retained, no longer fencing.
  ep.note_abandoned(covered.clone(), 1, Index::new(3));
  // UNCOVERED: boundary 5 does NOT span the abort entry at 8 → unmarked, retained, still fencing.
  ep.note_abandoned(uncovered.clone(), 1, Index::new(8));
  ep.note_abort_covered(Index::new(5), Cover::Install);
  assert_eq!(
    ep.abandoned_obligations().len(),
    2,
    "the mark removes nothing: both obligations are retained"
  );
  assert_eq!(
    ep.abandoned_record(&covered).map(|m| m.cover),
    Some(Cover::Install),
    "the covered obligation (abort_index 3) is marked"
  );
  assert_eq!(
    ep.abandoned_record(&uncovered).map(|m| m.cover),
    Some(Cover::None),
    "the uncovered obligation (abort_index 8) is not"
  );
  assert!(
    !ep.abort_relay_fences(Index::new(4)),
    "the install-covered obligation no longer fences its own abort entry's boundary"
  );
  assert!(
    ep.abort_relay_fences(Index::new(8)),
    "the uncovered obligation still fences"
  );
  assert!(
    ep.owes_live_thaw()
      && ep
        .abandoned_obligations()
        .iter()
        .all(|(_, m)| !m.discharged),
    "both are still LIVE — a cover retires nothing"
  );
  // A later boundary marks what it now covers; re-marking is a no-op.
  ep.note_abort_covered(Index::new(9), Cover::Install);
  assert!(
    ep.abandoned_obligations()
      .iter()
      .all(|(_, m)| m.cover == Cover::Install),
    "a later boundary marks what it now covers and re-marking is a no-op"
  );
  assert_eq!(ep.abandoned_obligations().len(), 2, "still nothing removed");
  assert!(
    !ep.abort_relay_fences(Index::new(1_000)),
    "neither fences now"
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
  let (_, a) = obligations
    .iter()
    .find(|(s, _)| *s == bytes::Bytes::from_static(b"\x2a"))
    .expect("source 2a still tracked");
  assert_eq!(
    (a.generation, a.abort_index),
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

/// The LAST-WINS overwrite RESETS the cover and discharge marks, with no clear in between — the one
/// path where a stale mark could reach a live obligation. A holder whose install covered `(S, g)`
/// still carries that record while its log-behind local S reads gen `g`; S thaws and re-freezes
/// elsewhere, and the abort for `(S, g')` applies here ABOVE the install boundary. That entry is in
/// the log — a live replay source naming a freeze nothing has proven past — so the record it
/// overwrites must be uncovered and live: were the install mark to survive, the new entry's fence
/// would be missing; were a discharge mark to survive, every live-obligation gate would skip a
/// freeze that is very much alive.
#[test]
fn a_fresh_abort_overwrites_a_covered_obligation_uncovered() {
  let (mut ep, _log, _stable) = make_merge_follower();
  let source = bytes::Bytes::from_static(b"\x2a");
  ep.note_abandoned(source.clone(), 1, Index::new(3));
  ep.note_abort_covered(Index::new(5), Cover::Install);
  assert_eq!(
    ep.abandoned_record(&source).map(|m| m.cover),
    Some(Cover::Install),
    "the install's boundary (5) covered the abort entry (3)"
  );
  // The fresh abort for the re-frozen incarnation applies above the boundary — no clear between.
  ep.note_abandoned(source.clone(), 2, Index::new(9));
  let m = ep
    .abandoned_record(&source)
    .expect("the source is still tracked");
  assert_eq!(
    m.cover,
    Cover::None,
    "the overwrite reset the mark: the new abort entry is a live replay source"
  );
  assert_eq!(
    (m.generation, m.abort_index),
    (2, Index::new(9)),
    "the re-freeze's generation and abort index won"
  );
  assert!(
    !ep.abort_relay_fences(Index::new(8)) && ep.abort_relay_fences(Index::new(9)),
    "the fence now keys on the new entry alone"
  );
  // The same for a DISCHARGED record: a global proof retired (S, 2); S re-freezes at 3 and its
  // abort applies at 12 — the overwrite is live again.
  ep.note_discharged(&source);
  assert!(
    !ep.owes_live_thaw() && ep.abandoned_record(&source).is_some_and(|m| m.discharged),
    "discharged: kept as the witness trigger, owed by no live gate"
  );
  ep.note_abandoned(source.clone(), 3, Index::new(12));
  let m = ep.abandoned_record(&source).expect("still tracked");
  assert!(
    !m.discharged && ep.owes_live_thaw() && m.generation == 3,
    "the overwrite reset the discharge mark: the new freeze is live"
  );
}

/// THE FENCE BY COVER KIND: an ADOPT-covered record keeps fencing captures (the kept log still
/// carries its abort entry — the record's only restart re-derivation), an INSTALL-covered one
/// fences nothing (its entry is already gone), the cover is an ORDERED UPGRADE (an install past an
/// adopt-covered record lifts the fence; an adopt after an install never brings it back), and a
/// DISCHARGED record KEEPS fencing unless install-covered — until the witness applies it is the only
/// future witness trigger, which compacting its entry and crashing would lose. The skip lives inside
/// `abort_relay_fences` alone, so every capture site — and the absorb's three-way classification —
/// agrees.
#[test]
fn an_install_cover_lifts_the_fence_and_an_adopt_cover_keeps_it() {
  let source = bytes::Bytes::from_static(b"\x2a");
  let fresh = |cover: Cover| {
    let (mut ep, _log, _stable) = make_merge_follower();
    ep.note_abandoned(source.clone(), 1, Index::new(3));
    ep.note_abort_covered(Index::new(5), cover);
    ep
  };
  // ADOPT keeps fencing.
  let ep = fresh(Cover::Adopt);
  assert_eq!(
    ep.abandoned_record(&source).map(|m| m.cover),
    Some(Cover::Adopt)
  );
  assert!(
    ep.abort_relay_fences(Index::new(5)),
    "an adopt-covered record still fences: its entry is in the kept log"
  );
  // INSTALL lifts.
  let ep = fresh(Cover::Install);
  assert!(
    !ep.abort_relay_fences(Index::new(1_000)),
    "an install-covered record fences nothing: its entry is gone"
  );
  assert!(
    ep.owes_live_thaw(),
    "and it is still live — lifting the fence is not disposal"
  );
  // The UPGRADE: adopt, then an install whose boundary covers it → Install, the fence lifts.
  let mut ep = fresh(Cover::Adopt);
  ep.note_abort_covered(Index::new(6), Cover::Install);
  assert_eq!(
    ep.abandoned_record(&source).map(|m| m.cover),
    Some(Cover::Install),
    "an install past an adopt-covered record upgrades the mark"
  );
  assert!(
    !ep.abort_relay_fences(Index::new(1_000)),
    "and lifts the fence"
  );
  // NO DOWNGRADE: install, then an adopt → still Install.
  let mut ep = fresh(Cover::Install);
  ep.note_abort_covered(Index::new(6), Cover::Adopt);
  assert_eq!(
    ep.abandoned_record(&source).map(|m| m.cover),
    Some(Cover::Install),
    "an adopt never downgrades an install cover — the discarded entry does not come back"
  );
  assert!(!ep.abort_relay_fences(Index::new(1_000)));
  // DISCHARGED keeps fencing: until the witness applies, the record is the only future trigger.
  let mut ep = fresh(Cover::Adopt);
  ep.note_discharged(&source);
  assert!(
    ep.abort_relay_fences(Index::new(5)),
    "a discharged record keeps fencing its entry: the only future witness trigger while a \
     non-observer leads"
  );
  assert!(
    !ep.owes_live_thaw() && ep.abandoned_matches(&source, 1),
    "owed by no live gate, yet held — the belt and the witness apply read it"
  );
  // ... unless install-covered: the entry is already gone, so there is nothing left to fence.
  let mut ep = fresh(Cover::Install);
  ep.note_discharged(&source);
  assert!(
    !ep.abort_relay_fences(Index::new(1_000)),
    "an install-covered discharged record fences nothing — its entry is already gone"
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

/// No-spin pin: a parked `CommitMerge` apply stop is `Waiting`, never `BudgetCut`, so `handle_storage`
/// must NOT report `MorePending` while parked — the container's per-crank merge service resolves the
/// park, and folding it into MorePending would busy-spin the driver against that external wait. A parked
/// crank SETTLES to `Drained` even though `applied < commit`.
#[test]
fn parked_merge_stop_does_not_report_more_pending() {
  use crate::StorageProgress;
  let (mut ep, mut log, mut stable, k) = make_parked_target(2);
  assert_eq!(ep.applied_index(), Index::new(k.get() - 1), "parked at k-1");
  assert!(
    ep.applied_index() < ep.commit_index(),
    "the parked entry keeps applied below commit"
  );
  // The cranks must settle to Drained despite applied < commit — a parked (Waiting) stop that spun
  // MorePending would loop here until the guard trips.
  let mut progress = StorageProgress::MorePending;
  let mut cranks = 0u32;
  while progress == StorageProgress::MorePending {
    progress = ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
    cranks += 1;
    assert!(
      cranks < 100,
      "a parked merge stop must not spin MorePending — it is Waiting, never BudgetCut"
    );
  }
  assert!(ep.pending_merge().is_some(), "still parked");
  assert_eq!(
    progress,
    StorageProgress::Drained,
    "the parked crank reports Drained, not MorePending"
  );
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
/// lineage bumped — and RETURNS the `Merged` payload WITHOUT queuing any event. Emission is the
/// container's job via `emit_merged`, gated on the forced absorb capture staging; the resolve
/// itself surfaces nothing, so no durable-union claim can leak ahead of the capture. The drain
/// resumes on the next crank.
#[test]
fn resolve_pending_merge_absorbs_and_resumes() {
  let (mut ep, mut log, mut stable, k) = make_parked_target(2);
  let before = ep.state_machine().count();
  let mut source = CountSm::default();
  for i in 0..3 {
    let _ = crate::StateMachine::apply(&mut source, Index::new(i + 1), bytes::Bytes::new());
  }
  let merged = ep.resolve_pending_merge(source);
  assert!(!ep.is_poisoned());
  assert_eq!(ep.applied_index(), k, "the parked entry applied");
  assert!(ep.pending_merge().is_none());
  assert_eq!(
    ep.state_machine().count(),
    before + 3,
    "the union folded in"
  );
  assert_eq!(ep.shape_gen(), 1, "lineage bumped to target_gen_after");
  // The payload is RETURNED, not queued: the resolve emits nothing (poll drains empty) so the
  // container can withhold the event on a failed capture. The endpoint here is not poisoned, so
  // a leaked event WOULD surface — the pre-gate code failed exactly this assertion.
  let m = merged.expect("a successful absorb returns the Merged payload");
  assert_eq!(m.index(), k);
  assert_eq!(m.source().as_ref(), b"\x2a");
  assert!(
    ep.poll_event().is_none(),
    "resolve queues no event ahead of emit_merged"
  );
  // `emit_merged` is the surfacing seam the container calls once the capture stages: exactly one
  // `Event::Merged` then drains.
  ep.emit_merged(m);
  let mut merged_events = 0;
  while let Some(ev) = ep.poll_event() {
    if let crate::Event::Merged(me) = ev {
      merged_events += 1;
      assert_eq!(me.index(), k);
      assert_eq!(me.source().as_ref(), b"\x2a");
    }
  }
  assert_eq!(
    merged_events, 1,
    "emit_merged surfaces the event exactly once"
  );
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

/// A group that owes an aborted-merge thaw deliberately does NOT fence conf changes: a voter may
/// join it (re-deriving the obligation), and if that obligation becomes a local dead end the resolve
/// arm's drivability belt drops it at the absorb — so the join never wedges (the container world test
/// `a_dead_end_obligation_does_not_wedge_a_co_hosted_absorb` pins that end to end). Fencing joins on
/// an obligation holder would forbid growing a target that is legitimately aborting an inbound merge.
#[test]
fn an_outstanding_thaw_obligation_does_not_fence_conf_changes() {
  use crate::{ConfChange, ConfChangeType};
  let (mut ep, mut log, mut stable) = make_three_voter_leader();
  // A TARGET-side abort at the live mint applies and records one durable `abandoned` obligation.
  let a = ep
    .propose_merge_entry(
      Instant::ORIGIN,
      &mut log,
      EntryKind::RollbackMerge,
      abort_payload(b"\x2a", 1, 1),
    )
    .unwrap();
  ack_through(&mut ep, &mut log, &mut stable, a);
  assert!(ep.owes_live_thaw(), "the abort recorded an obligation");
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}
  assert!(
    ep.propose_conf_change(
      Instant::ORIGIN,
      &mut log,
      &stable,
      ConfChange::new(ConfChangeType::AddNode, 4u64, bytes::Bytes::new()),
    )
    .is_ok(),
    "an outstanding abort obligation does not fence a conf change — the belt drops a dead end at absorb"
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
/// Without the leg `absorb_capture_blocked` returns false, the container's resolve arm captures,
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
  // The fence held the park, nothing compacted — the abort entry is RETAINED, so a crash-restart
  // re-derives `abandoned` (see `restart_re_derives_the_abort_relay`) and the source stays thawable.
  // Without the leg the arm resolves+captures and the deferred compaction crosses `abort_at`,
  // erasing the obligation's only restart source and consuming the park.
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
  assert!(ep.owes_live_thaw());
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

/// THE MERGE REPLAY FENCE, absorb-capture edition: a target that FROZE as another merge's source
/// (its own `PrepareMerge` applied) and then parked a `CommitMerge` above the freeze reaches the
/// resolve arm frozen-and-parked. The forced absorb capture at the park would compact the
/// `PrepareMerge` — the freeze's only restart re-derivation — so a crash would restart this
/// replica UNFROZEN while the claiming target still holds a parked absorb of it at the freeze
/// boundary; the fold itself would also advance state that claim already pinned. The shared
/// fence set refuses, holding the park until the freeze dies by protocol (the claimant absorbs
/// this whole group, or a thaw arrives wholesale via a superseding install).
#[test]
fn a_live_freeze_blocks_the_forced_absorb_capture() {
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();
  log.force_append(&[
    Entry::new(
      Term::new(1),
      Index::new(1),
      EntryKind::PrepareMerge,
      prepare_payload(b"\x2b", 1),
    ),
    Entry::new(
      Term::new(1),
      Index::new(2),
      EntryKind::CommitMerge,
      commit_payload(b"\x2a", Index::new(5), 1, 2),
    ),
  ]);
  stable.force_state(Term::new(1), Some(1u64), Index::new(2));
  let mut ep = Endpoint::restart(
    cfg.clone(),
    Instant::ORIGIN,
    7,
    CountSm::default(),
    1,
    &mut log,
    &mut stable,
  );
  assert!(!ep.is_poisoned());
  assert!(ep.is_frozen(), "the freeze applied below the park");
  let pending = ep.pending_merge().expect("parked above its own freeze");
  let k = pending.at();
  assert_eq!(k, Index::new(2));

  // Model the container's resolve arm EXACTLY: it resolves + forces the capture ONLY when the
  // capture is not blocked. The freeze leg must hold the park.
  if !ep.absorb_capture_blocked() {
    ep.resolve_pending_merge(CountSm::default());
    assert!(ep.capture_absorb_snapshot(&log, &mut stable));
  }
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  assert!(
    ep.pending_merge().is_some(),
    "the park holds: an absorb into a frozen target would fold and compact across the claim"
  );
  assert!(ep.is_frozen(), "the freeze survives the held park");
  assert!(
    log.first_index() <= Index::new(1),
    "the PrepareMerge stays replayable: it is the freeze's only restart re-derivation"
  );

  // Leg isolation: the identical shape minus the freeze resolves freely — the hold above is the
  // freeze leg alone, not the park or the payload shape.
  let mut log2 = VecLog::default();
  let mut stable2 = AsyncStable::default();
  log2.force_append(&[
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
      // target_gen_after = 1: no freeze bumped this control's lineage, and the park's guard is
      // exact-increment — the ep1 payload's 2 would no-op here instead of parking.
      commit_payload(b"\x2a", Index::new(5), 1, 1),
    ),
  ]);
  stable2.force_state(Term::new(1), Some(1u64), Index::new(2));
  let mut ep2 = Endpoint::restart(
    cfg,
    Instant::ORIGIN,
    7,
    CountSm::default(),
    1,
    &mut log2,
    &mut stable2,
  );
  assert!(ep2.pending_merge().is_some());
  assert!(
    !ep2.absorb_capture_blocked(),
    "no freeze, no hold: the fence is the freeze leg, not the park"
  );
  ep2.resolve_pending_merge(CountSm::default());
  assert!(ep2.capture_absorb_snapshot(&log2, &mut stable2));
}

/// The freeze leg's APPEND-OBSERVED half is BOUNDARY-aware: an uncommitted `PrepareMerge`
/// accepted ABOVE the park does NOT fence the absorb — the fold compacts only at-or-below its
/// boundary, so the freeze entry survives replay untouched, and holding the earlier fold on it
/// is a restart-replay circular wait (the park below is exactly what keeps that freeze from
/// applying). A capture AT-OR-PAST the pending freeze's own index still refuses — its
/// compaction would erase the entry whose replay is the freeze's only restart derivation — and
/// a §5.3 conflict truncation that removes the entry releases that refusal too.
#[test]
fn a_pending_freeze_fences_only_at_or_past_its_own_index() {
  let (mut ep, mut log, mut stable) = make_follower();
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
  assert!(!ep.absorb_capture_blocked(), "nothing fences yet");

  // An UNCOMMITTED PrepareMerge lands above the park: the append-observed kill arms.
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(1),
      1u64,
      Index::new(2),
      Term::new(1),
      std::vec![Entry::new(
        Term::new(1),
        Index::new(3),
        EntryKind::PrepareMerge,
        prepare_payload(b"\x2c", 1),
      )],
      Index::new(2),
    )),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  assert_eq!(ep.freeze_pending(), Some(Index::new(3)));
  assert!(
    !ep.absorb_capture_blocked(),
    "a pending freeze ABOVE the park boundary leaves the earlier fold free"
  );
  assert!(
    ep.capture_blocked_at(Index::new(3)),
    "a capture at the freeze's own index would erase its replay — still refused"
  );

  // A new leader's conflicting append truncates the freeze away: the kill releases and even a
  // capture at that index is free — no over-block once the freeze no longer exists in the log.
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
        encode_cmd(b"b"),
      )],
      Index::new(2),
    )),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  assert_eq!(
    ep.freeze_pending(),
    None,
    "the truncation released the kill"
  );
  assert!(!ep.capture_blocked_at(Index::new(3)));
  assert!(!ep.absorb_capture_blocked());
  ep.resolve_pending_merge(CountSm::default());
  assert!(
    ep.capture_absorb_snapshot(&log, &mut stable),
    "the capture stages once no freeze is live"
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

/// A snapshot install that re-baselines PAST an applied freeze clears the whole applied-freeze
/// quartet, not just the append-observed pending flag — a replica that applied the freeze,
/// partitioned, and installs a boundary past the thaw must derive NOT-frozen, exactly as a plain
/// restart from the same durable state would (install and restart must agree).
///
/// Clearing only the append-observed pending flag leaves `frozen`/`frozen_for`/`freeze_index` set,
/// so the replica stays frozen forever — captures freeze-fenced, proposes/reads/transfers refused if
/// elected, its stale claim blocking the claimed target's removal.
#[test]
fn snapshot_install_clears_an_applied_freeze() {
  use crate::{InstallSnapshot, SnapshotMeta, conf::ConfState};
  let (mut ep, mut log, mut stable) = make_follower();
  // The follower APPLIES a freeze: Normal@1 + PrepareMerge@2 committed (leader_commit = 2).
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
          EntryKind::PrepareMerge,
          prepare_payload(b"\x2b", 1),
        ),
      ],
      Index::new(2),
    )),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  assert!(ep.is_frozen(), "the freeze applied");
  assert_eq!(ep.freeze_index(), Some(Index::new(2)));
  assert_eq!(
    ep.frozen_for().map(|t| t.as_ref().to_vec()),
    Some(b"\x2b".to_vec())
  );
  assert!(ep.merge_freeze_active(), "the capture fence is armed");

  // A boundary PAST the freeze (and past any thaw) installs — the partitioned replica catches up
  // wholesale on a leader that already resolved the freeze it once applied.
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
  assert!(!ep.is_frozen(), "the install cleared the applied freeze");
  assert_eq!(ep.freeze_index(), None, "no lingering boundary");
  assert_eq!(ep.frozen_for(), None, "no lingering claim");
  assert!(
    !ep.merge_freeze_active(),
    "the capture is no longer freeze-fenced"
  );
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
  assert!(ep.owes_live_thaw());
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
  assert!(!ep.owes_live_thaw(), "no abandoned obligation");
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

/// A 3-voter FOLLOWER (node 1) rebuilt at restart PARKED at the `CommitMerge`@2, with one entry
/// committed-but-unapplied ABOVE the park (the gap the campaign guard scans): no-op@1 +
/// CommitMerge@2 + `gap_kind`@3, hard state at commit=3. The reconcile applies the no-op, parks
/// at 1, and leaves index 3 committed above the held apply — the exact shape
/// `merge_park_membership_superseded` judges.
fn restart_parked_follower_with_gap(
  pre_vote: bool,
  gap_kind: EntryKind,
  gap_payload: bytes::Bytes,
) -> (Endpoint<u64, CountSm>, VecLog, AsyncStable) {
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_pre_vote(pre_vote);
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();
  log.force_append(&[
    Entry::new(
      Term::new(1),
      Index::new(1),
      EntryKind::Empty,
      bytes::Bytes::new(),
    ),
    Entry::new(
      Term::new(1),
      Index::new(2),
      EntryKind::CommitMerge,
      commit_payload(b"\x2a", Index::new(7), 1, 1),
    ),
    Entry::new(Term::new(1), Index::new(3), gap_kind, gap_payload),
  ]);
  stable.force_state(Term::new(1), Some(1u64), Index::new(3));
  let mut ep = Endpoint::restart(
    cfg,
    Instant::ORIGIN,
    7,
    CountSm::default(),
    1,
    &mut log,
    &mut stable,
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  assert!(ep.role().is_follower(), "a restarted replica is a follower");
  assert!(ep.pending_merge().is_some(), "parked at the CommitMerge");
  assert_eq!(
    ep.applied_index(),
    Index::new(1),
    "the park holds apply at k-1"
  );
  assert_eq!(
    ep.commit_index(),
    Index::new(3),
    "commit runs past the park"
  );
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}
  (ep, log, stable)
}

/// A committed conf-change payload (AddNode 4) for the gap entry — the membership supersession.
fn add4_payload() -> bytes::Bytes {
  use crate::{ConfChange, ConfChangeType};
  let v2 = ConfChange::new(ConfChangeType::AddNode, 4u64, bytes::Bytes::new()).into_v2();
  let mut buf = Vec::new();
  crate::wire::encode_conf_change_v2(&v2, &mut buf);
  bytes::Bytes::from(buf)
}

/// The campaign guard, `become_candidate` path: a parked replica whose committed-but-unapplied
/// gap holds a `ConfChange` is running on a SUPERSEDED voter set (membership is apply-time), so
/// an election timeout must not make it a candidate — a win on the stale configuration could
/// truncate entries the real configuration committed.
#[test]
fn a_parked_replica_with_a_superseded_voter_set_does_not_campaign() {
  let (mut ep, mut log, mut stable) =
    restart_parked_follower_with_gap(false, EntryKind::ConfChange, add4_payload());
  let deadline = ep.poll_timeout().expect("election timer armed");
  ep.handle_timeout(deadline, &mut log, &mut stable);
  assert!(
    ep.role().is_follower(),
    "the superseded parked replica must not become a candidate"
  );
  assert_eq!(
    ep.term(),
    Term::new(1),
    "no term bump — no campaign started"
  );
  while let Some(out) = ep.poll_message() {
    assert!(
      !matches!(out.message(), Message::RequestVote(_)),
      "no vote request may leave a superseded parked replica"
    );
  }
}

/// The campaign guard, `become_pre_candidate` path: the same superseded park must not even
/// PROBE — a pre-vote quorum on the stale set would walk straight into the real campaign.
#[test]
fn a_parked_replica_with_a_superseded_voter_set_does_not_pre_campaign() {
  let (mut ep, mut log, mut stable) =
    restart_parked_follower_with_gap(true, EntryKind::ConfChange, add4_payload());
  let deadline = ep.poll_timeout().expect("election timer armed");
  ep.handle_timeout(deadline, &mut log, &mut stable);
  assert!(
    ep.role().is_follower(),
    "the superseded parked replica must not become a pre-candidate"
  );
  assert_eq!(
    ep.term(),
    Term::new(1),
    "pre-vote never bumps the term, and none was probed"
  );
  while let Some(out) = ep.poll_message() {
    assert!(
      !matches!(out.message(), Message::RequestVote(_)),
      "no pre-vote probe may leave a superseded parked replica"
    );
  }
}

/// The guard fails CLOSED: a park gap the log cannot serve (a cold read) cannot be proven free of
/// membership changes, so the campaign refuses this pass — and a warm retry over the same PLAIN
/// gap re-evaluates and campaigns, proving the refusal was the unreadable range, not the content.
#[test]
fn an_unreadable_park_gap_refuses_the_campaign_until_a_warm_retry() {
  use crate::testkit::FailTermLog;
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut log = FailTermLog::default();
  let mut stable = AsyncStable::default();
  log.force_append(&[
    Entry::new(
      Term::new(1),
      Index::new(1),
      EntryKind::Empty,
      bytes::Bytes::new(),
    ),
    Entry::new(
      Term::new(1),
      Index::new(2),
      EntryKind::CommitMerge,
      commit_payload(b"\x2a", Index::new(7), 1, 1),
    ),
    Entry::new(
      Term::new(1),
      Index::new(3),
      EntryKind::Normal,
      bytes::Bytes::from_static(b"w"),
    ),
  ]);
  stable.force_state(Term::new(1), Some(1u64), Index::new(3));
  let mut ep = Endpoint::restart(
    cfg,
    Instant::ORIGIN,
    7,
    CountSm::default(),
    1,
    &mut log,
    &mut stable,
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  assert!(ep.pending_merge().is_some(), "parked at the CommitMerge");
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  log.return_cold_on_read();
  let deadline = ep.poll_timeout().expect("election timer armed");
  ep.handle_timeout(deadline, &mut log, &mut stable);
  assert!(
    ep.role().is_follower(),
    "an unprovable gap fails closed — no campaign this pass"
  );
  assert_eq!(ep.term(), Term::new(1));

  // The range becomes resident: the next timeout re-evaluates the plain gap and campaigns.
  log.clear_cold_on_read();
  let deadline = ep.poll_timeout().expect("election timer re-armed");
  ep.handle_timeout(deadline, &mut log, &mut stable);
  assert!(
    ep.role().is_candidate(),
    "a warm retry over a plain gap campaigns normally"
  );
  assert_eq!(ep.term(), Term::new(2), "the real campaign bumped the term");
}

/// The guard must not over-fire: a park over PLAIN entries (no membership change in the
/// committed-but-unapplied gap) leaves the voter set current, so the parked replica still
/// campaigns — a group whose only up-to-date replicas are parked mid-merge can still elect.
#[test]
fn a_park_over_plain_entries_still_campaigns() {
  let (mut ep, mut log, mut stable) =
    restart_parked_follower_with_gap(false, EntryKind::Normal, bytes::Bytes::from_static(b"w"));
  let deadline = ep.poll_timeout().expect("election timer armed");
  ep.handle_timeout(deadline, &mut log, &mut stable);
  assert!(
    ep.role().is_candidate(),
    "a plain-gap park campaigns — the guard keys on membership supersession only"
  );
  assert_eq!(ep.term(), Term::new(2), "the campaign bumped the term");
  let mut targets: Vec<u64> = Vec::new();
  while let Some(out) = ep.poll_message() {
    if matches!(out.message(), Message::RequestVote(_)) {
      targets.push(out.to());
    }
  }
  targets.sort();
  assert_eq!(
    targets,
    std::vec![2u64, 3],
    "votes requested from both peers"
  );
}

/// The chained shape's adopt: a target frozen as ANOTHER merge's source (its own `PrepareMerge`
/// applied below the park) whose thaw sits committed-but-unapplied in the skipped range. The
/// adopt clears the quartet unconditionally — the totality argument in the correct direction: a
/// same-group sender is capture-fenced for its freeze's whole life, so a blob at this boundary
/// PROVES the freeze was thawed at-or-before it — and re-derives `freeze_pending` against the
/// KEPT tail rather than blanket-clearing off a discard that did not happen. Exiting the adopt
/// still frozen would strand the replica forever: captures fenced, proposals refused if
/// elected, its stale claim blocking the claimant's teardown.
#[test]
fn the_adopt_thaws_a_frozen_and_parked_target() {
  use crate::{InstallSnapshot, SnapshotMeta, conf::ConfState};
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();
  log.force_append(&[
    Entry::new(
      Term::new(1),
      Index::new(1),
      EntryKind::PrepareMerge,
      prepare_payload(b"\x2b", 1),
    ),
    Entry::new(
      Term::new(1),
      Index::new(2),
      EntryKind::CommitMerge,
      commit_payload(b"\x2a", Index::new(5), 1, 2),
    ),
    Entry::new(
      Term::new(1),
      Index::new(3),
      EntryKind::RollbackMerge,
      rollback_payload(2),
    ),
    Entry::new(
      Term::new(1),
      Index::new(4),
      EntryKind::Normal,
      encode_cmd(b"a"),
    ),
  ]);
  stable.force_state(Term::new(1), Some(1u64), Index::new(4));
  let mut ep = Endpoint::restart(
    cfg,
    Instant::ORIGIN,
    7,
    CountSm::default(),
    1,
    &mut log,
    &mut stable,
  );
  assert!(ep.is_frozen(), "the freeze applied below the park");
  assert!(ep.pending_merge().is_some(), "parked above its own freeze");

  // The container's resolver would classify this park unresolvable (source unhosted) after
  // advancing the crossing walk; the endpoint test mirrors both steps directly.
  ep.advance_crossing_scan(&log);
  ep.note_merge_park_unresolvable(true);
  let meta = SnapshotMeta::new(
    Index::new(4),
    Term::new(1),
    ConfState::from_voters(std::vec![1u64, 2, 3]),
  )
  .with_shape_gen(2);
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    2u64,
    Message::InstallSnapshot(InstallSnapshot::new(
      Term::new(1),
      2u64,
      meta,
      encode_snapshot(9),
    )),
  );
  assert!(!ep.is_poisoned());
  assert!(ep.pending_merge().is_none(), "the adopt cleared the park");
  assert!(
    !ep.is_frozen(),
    "the boundary proves the freeze thawed — exiting frozen would strand this replica forever"
  );
  assert_eq!(
    ep.freeze_pending(),
    None,
    "re-derived against the kept tail: no freeze above"
  );
  assert_eq!(ep.applied_index(), Index::new(4));
  assert!(
    ep.abandoned_obligations().is_empty(),
    "the adopt mints no obligation of its own: this log carries an unfreeze, never an abort"
  );
  assert!(
    log.first_index() <= Index::new(1),
    "the log is KEPT — the adopt discards nothing"
  );
}

/// A parked OBSERVER (non-voter) of `{1,2,3}`: `Normal@1` + `CommitMerge@2`, both committed, so the
/// apply drain stops at 1 and the park stands at 2. Non-voter by construction, so its election
/// timer never campaigns — every `handle_timeout` in the advertisement tests is pure cadence.
/// Returns `(ep, log, stable, boundary)`.
fn make_parked_observer() -> (Endpoint<u64, CountSm>, VecLog, AsyncStable, Index) {
  use core::time::Duration;
  let cfg = Config::try_new_observer(
    4u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();
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
      commit_payload(b"\x2a", Index::new(5), 1, 1),
    ),
  ]);
  stable.force_state(Term::new(1), None, Index::new(2));
  let mut ep = Endpoint::restart(
    cfg,
    Instant::ORIGIN,
    7,
    CountSm::default(),
    1,
    &mut log,
    &mut stable,
  );
  assert!(!ep.is_poisoned());
  assert_eq!(ep.applied_index(), Index::new(1), "the drain parked at k-1");
  assert_eq!(
    ep.pending_merge().map(PendingMergeApply::at),
    Some(Index::new(2)),
    "the park stands at the CommitMerge"
  );
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}
  (ep, log, stable, Index::new(2))
}

/// Deliver a bare heartbeat from leader 2 and return the boundary its response advertised.
fn beat_and_read_boundary(
  ep: &mut Endpoint<u64, CountSm>,
  log: &mut VecLog,
  stable: &mut AsyncStable,
  at: Instant,
) -> Index {
  ep.handle_message(
    at,
    log,
    stable,
    2u64,
    Message::Heartbeat(crate::Heartbeat::new(
      Term::new(1),
      2u64,
      Index::new(2),
      bytes::Bytes::new(),
    )),
  );
  let mut boundary = None;
  while let Some(out) = ep.poll_message() {
    if let Message::HeartbeatResponse(hbr) = out.message() {
      assert_eq!(out.to(), 2u64, "the response goes to the leader that beat");
      boundary = Some(hbr.stuck_boundary());
    }
  }
  boundary.expect("the heartbeat drew a response")
}

/// A replica whose park the container classified locally unresolvable STAMPS the boundary on the
/// heartbeat response it already owed — the leader's only view of a wedge it is structurally blind
/// to, since the park sits above a fully replicated log and stalls nothing but the local apply
/// drain. The stamp ceases the moment the classification clears.
#[test]
fn an_unresolvable_park_stamps_its_boundary_on_the_heartbeat_response() {
  let (mut ep, mut log, mut stable, boundary) = make_parked_observer();

  assert_eq!(
    beat_and_read_boundary(&mut ep, &mut log, &mut stable, Instant::ORIGIN),
    Index::ZERO,
    "a park nobody has classified advertises nothing"
  );

  ep.note_merge_park_unresolvable(true);
  assert_eq!(
    beat_and_read_boundary(&mut ep, &mut log, &mut stable, Instant::ORIGIN),
    boundary,
    "the classified park advertises its own boundary"
  );

  ep.note_merge_park_unresolvable(false);
  assert_eq!(
    beat_and_read_boundary(&mut ep, &mut log, &mut stable, Instant::ORIGIN),
    Index::ZERO,
    "the advertisement ceases with the classification"
  );
}

/// The unsolicited belt: a hinted replica whose leader has stopped beating altogether — a quiesced
/// leader emits nothing, so the stamped carrier above never fires — still puts the boundary in
/// front of that leader, once per election timeout and no faster. Every emission pins `lease_round`
/// to 0 and `lease_support` to ZERO: the leader's lease accounting credits a round-matching
/// response as a FRESH support promise, and a leader that has stopped beating holds one round open
/// arbitrarily long, so echoing a remembered round would float a `LeaseBased` lease on support this
/// replica never promised at that time.
#[test]
fn a_hinted_replica_advertises_on_a_slow_tick_without_inbound_heartbeats() {
  use core::time::Duration;
  let period = Duration::from_millis(1000);
  let (mut ep, mut log, mut stable, boundary) = make_parked_observer();

  // One beat seats the leader and charges the first period; then the leader goes silent.
  ep.note_merge_park_unresolvable(true);
  assert_eq!(
    beat_and_read_boundary(&mut ep, &mut log, &mut stable, Instant::ORIGIN),
    boundary
  );

  // Collect this tick's unsolicited advertisements (there is no other traffic to confuse them).
  fn tick(
    ep: &mut Endpoint<u64, CountSm>,
    log: &mut VecLog,
    stable: &mut AsyncStable,
    at: Instant,
  ) -> Vec<(u64, crate::HeartbeatResponse<u64>)> {
    ep.handle_timeout(at, log, stable);
    let mut out = Vec::new();
    while let Some(m) = ep.poll_message() {
      let to = m.to();
      if let Message::HeartbeatResponse(hbr) = m.message() {
        out.push((to, hbr.clone()));
      }
    }
    out
  }

  assert!(
    tick(&mut ep, &mut log, &mut stable, Instant::ORIGIN + period / 2).is_empty(),
    "the belt does not fire inside a period the beat already charged"
  );

  let due = tick(&mut ep, &mut log, &mut stable, Instant::ORIGIN + period);
  assert_eq!(due.len(), 1, "exactly one advertisement per period");
  let (to, hbr) = &due[0];
  assert_eq!(*to, 2u64, "addressed to the known leader");
  assert_eq!(hbr.stuck_boundary(), boundary);
  assert_eq!(hbr.lease_round(), 0, "never echo a remembered round");
  assert_eq!(
    hbr.lease_support(),
    Duration::ZERO,
    "promise no support the leader could bank"
  );
  assert!(
    hbr.context().is_empty(),
    "a non-empty context is a ReadIndex token; this answers no read"
  );

  assert!(
    tick(
      &mut ep,
      &mut log,
      &mut stable,
      Instant::ORIGIN + period + period / 2
    )
    .is_empty(),
    "the emission charged its own period"
  );
  assert_eq!(
    tick(
      &mut ep,
      &mut log,
      &mut stable,
      Instant::ORIGIN + period + period
    )
    .len(),
    1,
    "and the next period fires again"
  );

  ep.note_merge_park_unresolvable(false);
  assert!(
    tick(
      &mut ep,
      &mut log,
      &mut stable,
      Instant::ORIGIN + period + period + period
    )
    .is_empty(),
    "the belt ceases with the hint"
  );
}

/// A parked LEADER never advertises: it is the party the cure is addressed to, so telling itself
/// is noise. And a replica with no known leader has nobody to tell — it stays silent until contact
/// (or its own campaign's outcome) seats one.
#[test]
fn the_advertisement_needs_a_leader_that_is_not_this_replica() {
  let (mut ep, mut log, mut stable, _) = make_parked_observer();
  ep.note_merge_park_unresolvable(true);
  assert_eq!(ep.leader(), None, "a fresh restart knows no leader");
  ep.handle_timeout(Instant::ORIGIN, &mut log, &mut stable);
  assert!(
    ep.poll_message().is_none(),
    "a leaderless replica advertises to nobody"
  );

  let (mut ep, mut log, mut stable, k) = make_parked_target(2);
  assert!(ep.role().is_leader());
  ep.note_merge_park_unresolvable(true);
  assert_eq!(
    ep.merge_park_unresolvable(),
    Some(k),
    "the leader's own park is classified too"
  );
  ep.handle_timeout(Instant::ORIGIN, &mut log, &mut stable);
  while let Some(m) = ep.poll_message() {
    assert!(
      !matches!(m.message(), Message::HeartbeatResponse(_)),
      "a leader advertises nothing to itself"
    );
  }
}

/// A leader with a snapshot-threshold-1 config, three committed commands, and a durable capture
/// covering them — the eligible cure sender.
fn make_capturing_leader() -> (Endpoint<u64, CountSm>, VecLog, AsyncStable) {
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
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
  for i in 0..3u8 {
    let cmd = bytes::Bytes::copy_from_slice(&[i]);
    let _ = ep.propose(d, &mut log, &stable, &cmd).unwrap();
  }
  ack_through(&mut ep, &mut log, &mut stable, Index::new(4));
  // Two cranks: capture submitted, then completed durable + compacted.
  ep.handle_storage(d, &mut log, &mut stable);
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(stable.snapshot().is_some(), "the covering blob is durable");
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}
  (ep, log, stable)
}

/// A stable store whose paged snapshot read FAULTS — the cure sender's fatal-read seam.
struct ChunkErrStable(AsyncStable);

#[derive(Debug)]
struct ChunkErr;

impl core::fmt::Display for ChunkErr {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.write_str("chunk read fault")
  }
}

impl core::error::Error for ChunkErr {}

impl crate::StableStore for ChunkErrStable {
  type NodeId = u64;
  type Error = ChunkErr;

  fn hard_state(&self) -> crate::HardState<u64> {
    self.0.hard_state()
  }

  fn submit_write(&mut self, id: crate::OpId, hs: crate::HardState<u64>) {
    self.0.submit_write(id, hs)
  }

  fn submit_snapshot(&mut self, id: crate::OpId, meta: crate::SnapshotMeta<u64>, data: Bytes) {
    self.0.submit_snapshot(id, meta, data)
  }

  fn snapshot(&self) -> Option<(crate::SnapshotMeta<u64>, Bytes)> {
    self.0.snapshot()
  }

  fn durable_snapshot(&self) -> Option<crate::SnapshotMeta<u64>> {
    self.0.durable_snapshot()
  }

  fn snapshot_chunk(
    &self,
    _offset: u64,
    _len: u64,
  ) -> Option<Result<(crate::SnapshotMeta<u64>, u64, crate::SnapshotChunkRead), ChunkErr>> {
    Some(Err(ChunkErr))
  }

  fn accept_snapshot_chunk(
    &mut self,
    meta: &crate::SnapshotMeta<u64>,
    total_len: u64,
    offset: u64,
    data: &Bytes,
  ) -> Result<u64, ChunkErr> {
    self
      .0
      .accept_snapshot_chunk(meta, total_len, offset, data)
      .map_err(|e| match e {})
  }

  fn take_staged_snapshot(&mut self, meta: &crate::SnapshotMeta<u64>) -> Option<Bytes> {
    self.0.take_staged_snapshot(meta)
  }

  fn discard_snapshot_staging(&mut self) {
    self.0.discard_snapshot_staging()
  }

  fn poll(&mut self) -> Option<Result<crate::StableDone, ChunkErr>> {
    self.0.poll().map(|r| r.map_err(|e| match e {}))
  }

  fn has_pending(&self) -> bool {
    self.0.has_pending()
  }
}

/// A FAULTING cure read fail-stops the leader instead of retrying silently forever: the
/// advertised follower is match-caught-up and deliberately stays in `Replicate`, so ordinary
/// replication never exercises this read — a swallowed fault would park that follower
/// indefinitely with no observable cause. The store contract makes the error fatal
/// (`SnapshotRead`), exactly as the ordinary send path treats it.
#[test]
fn a_faulting_cure_read_fail_stops_the_leader() {
  use crate::HeartbeatResponse;
  let (mut ep, mut log, stable) = make_capturing_leader();
  let mut stable = ChunkErrStable(stable);
  let d = Instant::ORIGIN;
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::HeartbeatResponse(
      HeartbeatResponse::new(Term::new(1), 2u64, bytes::Bytes::new())
        .with_stuck_boundary(Index::new(3)),
    ),
  );
  assert!(ep.has_cure_debts(), "the advertisement is the mint");
  while ep.poll_message().is_some() {}
  ep.handle_timeout(
    d + core::time::Duration::from_millis(150),
    &mut log,
    &mut stable,
  );
  assert!(ep.is_poisoned(), "the fatal read fail-stops");
  assert_eq!(ep.poison_reason(), Some(PoisonReason::SnapshotRead));
  while let Some(out) = ep.poll_message() {
    assert!(
      !matches!(out.message(), Message::InstallSnapshot(_)),
      "no cure blob rides a faulting read"
    );
  }
}

/// The cure-send arc on the leader: an advertised boundary from a TRACKED peer mints the debt
/// and the covering blob goes out immediately — `Progress` untouched, so the peer keeps
/// replicating throughout — the cooldown suppresses a duplicate, and completed evidence at-or-
/// past the boundary discharges. Nothing weaker discharges: the advertisement re-mints, and
/// evidence is the only exit.
#[test]
fn an_advertised_park_mints_a_cure_debt_and_offers_the_covering_blob() {
  use crate::{HeartbeatResponse, SnapshotResponse};
  let (mut ep, mut log, mut stable) = make_capturing_leader();
  let d = Instant::ORIGIN;
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::HeartbeatResponse(
      HeartbeatResponse::new(Term::new(1), 2u64, bytes::Bytes::new())
        .with_stuck_boundary(Index::new(3)),
    ),
  );
  assert!(ep.has_cure_debts(), "the advertisement is the mint");
  // Every send rides the sweep's scheduler; the first offer lands on the next leader tick.
  while ep.poll_message().is_some() {}
  ep.handle_timeout(
    d + core::time::Duration::from_millis(150),
    &mut log,
    &mut stable,
  );
  let mut offers = 0;
  let mut boundary = Index::ZERO;
  while let Some(out) = ep.poll_message() {
    if let Message::InstallSnapshot(is) = out.message() {
      offers += 1;
      boundary = is.snapshot().last_index();
    }
  }
  assert_eq!(
    offers, 1,
    "the sweep offers once eligible, one tick after the mint"
  );
  assert!(
    boundary >= Index::new(3),
    "the blob covers the advertised park"
  );
  assert!(
    ep.peer_progress(&2u64).is_some(),
    "Progress untouched: the peer stays tracked and replicating through the transfer"
  );

  // The cooldown: a re-advertisement inside the window re-mints but does not re-send.
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::HeartbeatResponse(
      HeartbeatResponse::new(Term::new(1), 2u64, bytes::Bytes::new())
        .with_stuck_boundary(Index::new(3)),
    ),
  );
  ep.handle_timeout(
    d + core::time::Duration::from_millis(300),
    &mut log,
    &mut stable,
  );
  let mut resent = 0;
  while let Some(out) = ep.poll_message() {
    if matches!(out.message(), Message::InstallSnapshot(_)) {
      resent += 1;
    }
  }
  assert_eq!(
    resent, 0,
    "one blob per cooldown, however often the peer advertises"
  );
  assert!(ep.has_cure_debts());

  // Completed evidence at-or-past the boundary discharges; the debt is gone and stays gone.
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::SnapshotResponse(SnapshotResponse::new(
      Term::new(1),
      2u64,
      false,
      Index::new(4),
    )),
  );
  assert!(!ep.has_cure_debts(), "evidence discharges");
}

/// Eligibility defers, never drops: a PLAUSIBLE advertised boundary (at-or-below this leader's
/// commit) whose blob coverage has not caught up yet mints the debt and sends nothing — the
/// leader's own later capture makes it eligible and the sweep re-drives it. An IMPLAUSIBLE
/// boundary (above the leader's commit — a committed park cannot sit there) never mints at all:
/// its debt could never become eligible and would only pin the group awake until expiry.
#[test]
fn an_uncovered_cure_debt_defers_without_spending() {
  use crate::HeartbeatResponse;
  // A leader that has never captured (default threshold): every boundary is uncovered.
  let (mut ep, mut log, mut stable) = make_three_voter_leader();
  let d = Instant::ORIGIN;
  for i in 0..2u8 {
    let cmd = bytes::Bytes::copy_from_slice(&[i]);
    let _ = ep.propose(d, &mut log, &stable, &cmd).unwrap();
  }
  ack_through(&mut ep, &mut log, &mut stable, Index::new(3));
  while ep.poll_message().is_some() {}
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::HeartbeatResponse(
      HeartbeatResponse::new(Term::new(1), 2u64, bytes::Bytes::new())
        .with_stuck_boundary(Index::new(2)),
    ),
  );
  assert!(
    ep.has_cure_debts(),
    "a plausible boundary mints regardless of blob coverage"
  );
  let mut offers = 0;
  while let Some(out) = ep.poll_message() {
    if matches!(out.message(), Message::InstallSnapshot(_)) {
      offers += 1;
    }
  }
  assert_eq!(
    offers, 0,
    "no blob covers the boundary yet — the offer defers, the debt stands"
  );

  // The implausible twin: a boundary above this leader's commit is refused at the mint.
  let (mut ep2, mut log2, mut stable2) = make_capturing_leader();
  ep2.handle_message(
    d,
    &mut log2,
    &mut stable2,
    2u64,
    Message::HeartbeatResponse(
      HeartbeatResponse::new(Term::new(1), 2u64, bytes::Bytes::new())
        .with_stuck_boundary(Index::new(400)),
    ),
  );
  assert!(
    !ep2.has_cure_debts(),
    "a committed park cannot sit above the leader's commit — nothing mints"
  );
}

/// Ack `upto` from `peer`, raising its `Progress` to the leader's log tip — the caught-up
/// condition the transfer's immediate-`TimeoutNow` arm reads.
fn ack_peer_through(
  ep: &mut Endpoint<u64, CountSm>,
  log: &mut VecLog,
  stable: &mut AsyncStable,
  peer: u64,
  upto: Index,
) {
  use crate::AppendResponse;
  ep.handle_message(
    Instant::ORIGIN,
    log,
    stable,
    peer,
    Message::AppendResponse(AppendResponse::new(
      Term::new(1),
      peer,
      false,
      Index::ZERO,
      Term::ZERO,
      upto,
    )),
  );
  ep.handle_storage(Instant::ORIGIN, log, stable);
}

/// Tell this leader that `peer` is itself wedged on a park, minting the cure debt that excludes it
/// from the handoff candidates.
fn advertise_stuck(
  ep: &mut Endpoint<u64, CountSm>,
  log: &mut VecLog,
  stable: &mut AsyncStable,
  peer: u64,
  boundary: Index,
) {
  use crate::HeartbeatResponse;
  ep.handle_message(
    Instant::ORIGIN,
    log,
    stable,
    peer,
    Message::HeartbeatResponse(
      HeartbeatResponse::new(Term::new(1), peer, bytes::Bytes::new()).with_stuck_boundary(boundary),
    ),
  );
  while ep.poll_message().is_some() {}
}

/// Drain the outbox, returning every `TimeoutNow` recipient in emission order.
fn drain_timeout_now_targets(ep: &mut Endpoint<u64, CountSm>) -> std::vec::Vec<u64> {
  let mut targets = std::vec::Vec::new();
  while let Some(out) = ep.poll_message() {
    if matches!(out.message(), Message::TimeoutNow(_)) {
      targets.push(out.to());
    }
  }
  targets
}

/// The parked LEADER's only exit: nobody installs a cure blob to a leader, so it hands leadership
/// to the highest-matched voter that is not itself advertising a park. Exactly one forced handoff
/// per term — the second tick, still in the same term with the attempt in flight, arms nothing.
#[test]
fn a_parked_leader_hands_off_to_a_curable_peer_once_per_term() {
  let (mut ep, mut log, mut stable, k) = make_parked_target(2);
  // Peer 2 alone acked through the park boundary (`make_parked_target`'s ack), so it is the
  // uniquely highest-matched voter; peer 3 has matched nothing.
  ep.note_merge_park_unresolvable(true);
  assert_eq!(
    ep.merge_park_unresolvable(),
    Some(k),
    "the leader is parked"
  );

  ep.handle_timeout(Instant::ORIGIN, &mut log, &mut stable);
  assert_eq!(
    drain_timeout_now_targets(&mut ep),
    std::vec![2u64],
    "the caught-up, non-advertising voter takes the token"
  );
  assert_eq!(ep.transfer.lead_transferee, Some(2u64));

  // Same term, attempt still in flight: nothing more is armed and no second campaigner is
  // authorized.
  ep.handle_timeout(Instant::ORIGIN, &mut log, &mut stable);
  assert!(
    drain_timeout_now_targets(&mut ep).is_empty(),
    "one forced handoff per term"
  );
  assert_eq!(ep.transfer.lead_transferee, Some(2u64));
}

/// An advertising peer is wedged the same way this leader is, so seating it would move the wedge
/// rather than cure it. With both voters caught up, the debt alone decides the target — in either
/// direction, so the exclusion carries the choice rather than a tie-break.
#[test]
fn an_advertising_voter_is_never_the_handoff_target() {
  for (wedged, expected) in [(3u64, 2u64), (2u64, 3u64)] {
    let (mut ep, mut log, mut stable, k) = make_parked_target(2);
    ack_peer_through(&mut ep, &mut log, &mut stable, 3u64, k);
    while ep.poll_message().is_some() {}
    advertise_stuck(&mut ep, &mut log, &mut stable, wedged, k);
    ep.note_merge_park_unresolvable(true);

    ep.handle_timeout(Instant::ORIGIN, &mut log, &mut stable);
    assert_eq!(
      drain_timeout_now_targets(&mut ep),
      std::vec![expected],
      "peer {wedged} advertises a park of its own and cannot be the exit"
    );
  }
}

/// Every voter wedged ⇒ nothing is armed. Churning leadership between hosts that are all
/// uncurable buys no progress and pays the proposal freeze every term; the group stays
/// degraded-alive under the container's blocked-park signal instead.
#[test]
fn a_wholly_wedged_group_arms_no_handoff() {
  let (mut ep, mut log, mut stable, k) = make_parked_target(2);
  ack_peer_through(&mut ep, &mut log, &mut stable, 3u64, k);
  while ep.poll_message().is_some() {}
  advertise_stuck(&mut ep, &mut log, &mut stable, 2u64, k);
  advertise_stuck(&mut ep, &mut log, &mut stable, 3u64, k);
  ep.note_merge_park_unresolvable(true);

  ep.handle_timeout(Instant::ORIGIN, &mut log, &mut stable);
  assert!(
    drain_timeout_now_targets(&mut ep).is_empty(),
    "no candidate, no handoff"
  );
  assert_eq!(
    ep.transfer.lead_transferee, None,
    "nothing armed means no proposal freeze and no lease revocation"
  );
  assert!(ep.role().is_leader(), "the wedged leader stays seated");
}

/// The leg is park-gated, not merely leader-gated: an unparked leader (or one whose park the
/// resolver can still land locally) never spends a handoff.
#[test]
fn an_unhinted_leader_arms_no_handoff() {
  let (mut ep, mut log, mut stable, _k) = make_parked_target(2);
  assert!(
    ep.pending_merge().is_some() && ep.merge_park_unresolvable().is_none(),
    "parked, but the resolver has not called it unresolvable"
  );
  ep.handle_timeout(Instant::ORIGIN, &mut log, &mut stable);
  assert!(drain_timeout_now_targets(&mut ep).is_empty());
  assert_eq!(ep.transfer.lead_transferee, None);
}

/// A CHUNK at a covered boundary must never enter the adopt: every install class reaches the
/// redundancy arm, and a chunk's payload is a fragment whose decode would poison a live voter
/// on a message class that was inert there before the adopt existed. The chunk falls through
/// to the plain redundant handling and the park stands untouched.
#[test]
fn a_chunked_install_never_adopts_a_hinted_park() {
  use crate::{InstallSnapshot, SnapshotMeta, conf::ConfState};
  let (mut ep, mut log, mut stable) = make_follower();
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
        Entry::new(
          Term::new(1),
          Index::new(3),
          EntryKind::Normal,
          encode_cmd(b"b")
        ),
      ],
      Index::new(3),
    )),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  assert!(ep.pending_merge().is_some(), "parked");
  ep.note_merge_park_unresolvable(true);

  let meta = SnapshotMeta::new(
    Index::new(3),
    Term::new(1),
    ConfState::from_voters(std::vec![1u64, 2]),
  );
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::InstallSnapshot(InstallSnapshot::new_chunk(
      Term::new(1),
      1u64,
      meta,
      bytes::Bytes::from_static(&[0xDE, 0xAD]),
      0,
      1_000_000,
    )),
  );
  assert!(
    !ep.is_poisoned(),
    "a fragment must never reach the adopt's decode"
  );
  assert!(
    ep.pending_merge().is_some(),
    "the chunk took the plain redundant path; the park stands for the whole-blob cure"
  );
}

/// The cure ledger expires on advertisement silence REGARDLESS of role or of the courtesy
/// ledger's state: a standing debt holds quiesce eligibility on both sides of the pump, and a
/// peer that resolved its park some other way must not pin the group awake forever.
#[test]
fn a_silent_advertiser_expires_its_cure_debt() {
  use crate::HeartbeatResponse;
  use core::time::Duration;
  let (mut ep, mut log, mut stable) = make_capturing_leader();
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    2u64,
    Message::HeartbeatResponse(
      HeartbeatResponse::new(Term::new(1), 2u64, bytes::Bytes::new())
        .with_stuck_boundary(Index::new(3)),
    ),
  );
  assert!(ep.has_cure_debts());
  while ep.poll_message().is_some() {}
  // Four election timeouts of silence: the sweep's expiry half runs on the tick, no courtesy
  // debt anywhere, and the ledger drains.
  let later = Instant::ORIGIN + Duration::from_millis(4_000);
  let _ = ep.poll_timeout();
  ep.handle_timeout(later, &mut log, &mut stable);
  assert!(
    !ep.has_cure_debts(),
    "silence is resolution: the expiry half is gated by neither role nor the courtesy ledger"
  );
}

/// The cure's aggregate traffic bound survives ledger churn: per-peer cooldowns die with an
/// eviction, so past the cap every re-mint would arrive immediately due — the GLOBAL send gate
/// is what keeps a rotating advertiser population at one blob per half election timeout from
/// this leader, instead of one per advertisement.
#[test]
fn a_rotating_advertiser_population_is_globally_rate_bounded() {
  use crate::HeartbeatResponse;
  use core::time::Duration;
  let voters: std::vec::Vec<u64> = (1..=70).collect();
  let cfg = Config::try_new(
    1u64,
    voters,
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
  ep.handle_storage(d, &mut log, &mut stable);
  for peer in 2..=36u64 {
    ep.handle_message(
      d,
      &mut log,
      &mut stable,
      peer,
      Message::VoteResponse(VoteResponse::new(Term::new(1), peer, false, false)),
    );
  }
  assert!(ep.role().is_leader());
  ep.handle_storage(d, &mut log, &mut stable);
  let cmd = bytes::Bytes::from_static(b"c");
  let _ = ep.propose(d, &mut log, &stable, &cmd).unwrap();
  // Quorum-ack the command so a covering capture exists.
  {
    use crate::AppendResponse;
    ep.handle_storage(d, &mut log, &mut stable);
    for peer in 2..=70u64 {
      ep.handle_message(
        d,
        &mut log,
        &mut stable,
        peer,
        Message::AppendResponse(AppendResponse::new(
          Term::new(1),
          peer,
          false,
          Index::ZERO,
          Term::ZERO,
          Index::new(2),
        )),
      );
    }
    ep.handle_storage(d, &mut log, &mut stable);
    ep.handle_storage(d, &mut log, &mut stable);
  }
  assert!(stable.snapshot().is_some(), "a covering blob exists");
  while ep.poll_message().is_some() {}

  // Seventy distinct advertisers in one beat window: the ledger churns past its cap, every
  // re-mint is immediately due per-peer — and exactly ONE blob leaves.
  for peer in 2..=70u64 {
    ep.handle_message(
      d,
      &mut log,
      &mut stable,
      peer,
      Message::HeartbeatResponse(
        HeartbeatResponse::new(Term::new(1), peer, bytes::Bytes::new())
          .with_stuck_boundary(Index::new(2)),
      ),
    );
  }
  ep.handle_timeout(
    d + core::time::Duration::from_millis(150),
    &mut log,
    &mut stable,
  );
  let mut blobs = 0;
  while let Some(out) = ep.poll_message() {
    if matches!(out.message(), Message::InstallSnapshot(_)) {
      blobs += 1;
    }
  }
  assert_eq!(
    blobs, 1,
    "one blob per half election timeout, whatever the advertiser population does"
  );

  // Sustained churn is starvation-free: every continuously advertising peer is EVENTUALLY
  // offered a cure — the immutable admission order keys eviction, the cursor rotates service,
  // and every send goes through the scheduler, so no advertisement pattern can monopolize the
  // global gate.
  let mut served: std::collections::BTreeSet<u64> = std::collections::BTreeSet::new();
  let mut now_ms: u64 = 1000;
  for _ in 0..600u32 {
    now_ms += 500;
    let t = Instant::ORIGIN + core::time::Duration::from_millis(now_ms);
    for peer in 2..=70u64 {
      ep.handle_message(
        t,
        &mut log,
        &mut stable,
        peer,
        Message::HeartbeatResponse(
          HeartbeatResponse::new(Term::new(1), peer, bytes::Bytes::new())
            .with_stuck_boundary(Index::new(2)),
        ),
      );
    }
    ep.handle_timeout(t, &mut log, &mut stable);
    while let Some(out) = ep.poll_message() {
      if matches!(out.message(), Message::InstallSnapshot(_)) {
        served.insert(out.to());
      }
    }
  }
  assert_eq!(
    served.len(),
    69,
    "every advertiser is served under sustained over-cap churn: {served:?}"
  );
}

/// A follower (node 1, peer 2 leading at term 1) holding `entries` delivered by `AppendEntries`
/// with NOTHING committed: the pending kill armed by kind, nothing applied.
fn follower_with_appended(
  entries: Vec<Entry>,
) -> (
  Endpoint<u64, CountSm>,
  crate::testkit::FailTermLog,
  NoopStable,
) {
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 42, CountSm::default());
  let mut log = crate::testkit::FailTermLog::default();
  let mut stable = NoopStable::default();
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(1),
      2u64,
      Index::ZERO,
      Term::ZERO,
      entries,
      Index::ZERO,
    )),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  (ep, log, stable)
}

/// The leader's commit index reaches the follower — an empty `AppendEntries` after `last` — and
/// the follower's message-path apply drain runs on it.
fn deliver_commit(
  ep: &mut Endpoint<u64, CountSm>,
  log: &mut crate::testkit::FailTermLog,
  stable: &mut NoopStable,
  last: Index,
  commit: Index,
) {
  ep.handle_message(
    Instant::ORIGIN,
    log,
    stable,
    2u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(1),
      2u64,
      last,
      Term::new(1),
      std::vec![],
      commit,
    )),
  );
}

/// A term-`term` `Normal` entry at `index` carrying one `CountSm` command.
fn normal_entry(term: u64, index: u64) -> Entry {
  let mut buf = Vec::new();
  crate::Data::encode(&bytes::Bytes::from_static(b"c"), &mut buf);
  Entry::new(
    Term::new(term),
    Index::new(index),
    EntryKind::Normal,
    bytes::Bytes::from(buf),
  )
}

/// A term-`term` `PrepareMerge` at `index` naming target bytes `target` with `source_gen_after`.
fn freeze_entry(term: u64, index: u64, target: &'static [u8], source_gen_after: u64) -> Entry {
  Entry::new(
    Term::new(term),
    Index::new(index),
    EntryKind::PrepareMerge,
    prepare_payload(target, source_gen_after),
  )
}

/// Deliver one `AppendEntries` from peer 2 at `term` — `entries` after `(prev, prev_term)`, the
/// commit advertised at `commit` — and let the follower's message-path work run.
#[allow(clippy::too_many_arguments)]
fn follower_deliver(
  ep: &mut Endpoint<u64, CountSm>,
  log: &mut crate::testkit::FailTermLog,
  stable: &mut NoopStable,
  term: u64,
  prev: Index,
  prev_term: u64,
  entries: Vec<Entry>,
  commit: Index,
) {
  ep.handle_message(
    Instant::ORIGIN,
    log,
    stable,
    2u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(term),
      2u64,
      prev,
      Term::new(prev_term),
      entries,
      commit,
    )),
  );
}

/// The endpoint's queued freezes, lowest first.
fn queue(ep: &Endpoint<u64, CountSm>) -> Vec<Index> {
  ep.freeze_queue().collect()
}

/// The ordered `PrepareMerge` indices in `(applied, last]`, read off the log by a scan the product
/// never runs — the ground truth of the freeze queue invariant.
fn expected_queue<L: crate::LogStore>(log: &L, applied: Index) -> Vec<Index> {
  let last = log.last_index();
  let from = applied.next().max(log.first_index());
  if from > last {
    return Vec::new();
  }
  match log.entries(from..last.next(), u64::MAX) {
    Ok(crate::EntriesRead::Ready(c)) => c
      .iter()
      .filter(|e| e.kind() == EntryKind::PrepareMerge)
      .map(|e| e.index())
      .collect(),
    _ => panic!("the ground-truth scan must read"),
  }
}

/// THE FOLD READS NO SUFFIX PAGE. The pending state a freeze fold leaves behind comes from the
/// append-maintained freeze queue, never from a walk of the suffix above the entry, so a page the
/// store has not made resident — here the entry two above, in an uncommitted tail — neither defers
/// nor poisons the fold. Earlier folds re-derived the pending state by scanning that suffix: a
/// paged store then fail-stopped a HEALTHY replica mid-backlog, and the deferral that replaced the
/// poison handed a bounded cache evicting alternately a livelock. Now the fold completes on the
/// apply fetch alone — frozen for the named target, the queued freeze above it the pending state,
/// exactly one `MergeFrozen` — whether the page above is cold or faulting; the drain's own
/// cold-page deferral still governs the entries it applies.
#[test]
fn a_freeze_fold_reads_no_suffix_page() {
  let entries = || {
    std::vec![
      freeze_entry(1, 1, b"\x2b", 1),
      normal_entry(1, 2),
      freeze_entry(1, 3, b"\x2c", 2),
    ]
  };

  // A COLD page above the freeze.
  let (mut ep, mut log, mut stable) = follower_with_appended(entries());
  assert_eq!(
    ep.freeze_pending(),
    Some(Index::new(1)),
    "the kill is armed by kind at append"
  );
  log.cold_entries_at(Some(Index::new(2)));
  deliver_commit(&mut ep, &mut log, &mut stable, Index::new(3), Index::new(1));
  assert!(
    ep.poison_reason().is_none(),
    "a page above the freeze is never read by the fold"
  );
  assert!(
    ep.is_frozen()
      && ep.frozen_for() == Some(&bytes::Bytes::from_static(b"\x2b"))
      && ep.applied_index() == Index::new(1)
      && ep.shape_gen() == 1,
    "the fold completed on the apply fetch alone"
  );
  assert_eq!(
    ep.freeze_pending(),
    Some(Index::new(3)),
    "the queue supplies the pending state"
  );
  assert_eq!(
    core::iter::from_fn(|| ep.poll_event())
      .filter(|e| matches!(e, crate::Event::MergeFrozen(_)))
      .count(),
    1,
    "exactly one MergeFrozen"
  );

  // A FAULTING page above the freeze: equally untouched.
  let (mut ep, mut log, mut stable) = follower_with_appended(entries());
  log.fail_entries_at(Some(Index::new(2)));
  deliver_commit(&mut ep, &mut log, &mut stable, Index::new(3), Index::new(1));
  assert!(
    ep.poison_reason().is_none() && ep.is_frozen() && ep.freeze_pending() == Some(Index::new(3)),
    "a faulting page above the freeze is never read by the fold either"
  );
}

/// THE SOURCE-ROLE LINEAGE GUARD, in the re-election shape that needs it. Node 1 applied a freeze
/// for target A (gen 1); a committed `Unfreeze(2)` and a newer `PrepareMerge(→B, 3)` sit unapplied
/// behind a cold page. It wins leadership: its thaw dedup re-seats to none-in-flight while its
/// live state still shows the OLD freeze, so the target's obligation drive appends a DUPLICATE
/// `Unfreeze(2)` above the newer freeze. Once the page warms the drain applies the original thaw,
/// the newer freeze, and then the duplicate — which, unguarded, cleared the NEWER freeze with a
/// stale generation and left the target replicas to resolve the g+2 `CommitMerge` on opposite
/// sides. A thaw applies only while frozen and only at its minted generation, the successor of the
/// freeze it releases: the duplicate is a silent no-op (no state change, no event) and the source
/// ends FROZEN for B at gen 3.
#[test]
fn a_stale_unfreeze_is_a_no_op_after_a_cold_tail_re_election() {
  use crate::{AppendResponse, RollbackMergePayload};
  use core::time::Duration;
  let unfreeze = |source_gen_after: u64| {
    let p = RollbackMergePayload::unfreeze(source_gen_after);
    let mut buf = Vec::new();
    crate::wire::encode_rollback_merge_payload(&p, &mut buf);
    bytes::Bytes::from(buf)
  };
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 42, CountSm::default());
  let mut log = crate::testkit::FailTermLog::default();
  let mut stable = AsyncStable::default();
  let d = Instant::ORIGIN;
  let deliver = |ep: &mut Endpoint<u64, CountSm>,
                 log: &mut crate::testkit::FailTermLog,
                 stable: &mut AsyncStable,
                 prev: u64,
                 entries: Vec<Entry>,
                 commit: u64| {
    ep.handle_message(
      d,
      log,
      stable,
      2u64,
      Message::AppendEntries(AppendEntries::new(
        Term::new(1),
        2u64,
        Index::new(prev),
        if prev == 0 { Term::ZERO } else { Term::new(1) },
        entries,
        Index::new(commit),
      )),
    );
    ep.handle_storage(d, log, stable);
  };
  // The old freeze, applied: frozen for A at gen 1.
  deliver(
    &mut ep,
    &mut log,
    &mut stable,
    0,
    std::vec![freeze_entry(1, 1, b"\x2b", 1)],
    1,
  );
  assert!(
    ep.is_frozen() && ep.shape_gen() == 1,
    "frozen for A at gen 1"
  );
  while ep.poll_event().is_some() {}
  // The committed tail behind a cold page: the thaw, then the newer freeze.
  log.cold_entries_at(Some(Index::new(2)));
  deliver(
    &mut ep,
    &mut log,
    &mut stable,
    1,
    std::vec![
      normal_entry(1, 2),
      Entry::new(
        Term::new(1),
        Index::new(3),
        EntryKind::RollbackMerge,
        unfreeze(2)
      ),
      freeze_entry(1, 4, b"\x2c", 3),
    ],
    4,
  );
  assert!(
    ep.is_frozen() && ep.applied_index() == Index::new(1) && ep.poison_reason().is_none(),
    "parked below the cold page, still at the old freeze"
  );
  assert_eq!(ep.freeze_pending(), Some(Index::new(4)));

  // Leadership at term 2: the thaw dedup re-seats to none in flight.
  let t = ep.poll_timeout().unwrap();
  ep.handle_timeout(t, &mut log, &mut stable);
  ep.handle_storage(t, &mut log, &mut stable);
  ep.handle_message(
    t,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResponse(VoteResponse::new(Term::new(2), 2u64, false, false)),
  );
  assert!(ep.role().is_leader(), "won leadership at term 2");
  assert_eq!(
    ep.thaw_in_flight(),
    None,
    "the dedup re-seated: this leader knows of no thaw in flight"
  );
  ep.handle_storage(t, &mut log, &mut stable);
  // The drive's duplicate thaw, minted against the live (old) freeze: gen 2 again, above the
  // newer freeze the leader has not applied.
  let dup = ep
    .propose_merge_entry(t, &mut log, EntryKind::RollbackMerge, unfreeze(2))
    .expect("the leader appends the duplicate thaw");
  ep.handle_storage(t, &mut log, &mut stable);
  ep.handle_message(
    t,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendResponse(AppendResponse::new(
      Term::new(2),
      2u64,
      false,
      Index::ZERO,
      Term::ZERO,
      dup,
    )),
  );
  ep.handle_storage(t, &mut log, &mut stable);
  assert!(ep.commit_index() >= dup, "the duplicate committed");

  // The page warms: the drain applies the original thaw, the newer freeze, then the duplicate.
  log.cold_entries_at(None);
  ep.handle_storage(t, &mut log, &mut stable);
  assert_eq!(
    ep.applied_index(),
    dup,
    "the drain applied through the duplicate"
  );
  assert!(ep.poison_reason().is_none());
  assert!(
    ep.is_frozen()
      && ep.frozen_for() == Some(&bytes::Bytes::from_static(b"\x2c"))
      && ep.shape_gen() == 3
      && ep.freeze_index() == Some(Index::new(4)),
    "frozen for B at gen 3: the stale thaw was a no-op"
  );
  let (mut thaws, mut freezes) = (0, 0);
  while let Some(ev) = ep.poll_event() {
    match ev {
      crate::Event::MergeRolledBack(_) => thaws += 1,
      crate::Event::MergeFrozen(_) => freezes += 1,
      _ => {}
    }
  }
  assert_eq!(
    (thaws, freezes),
    (1, 1),
    "one thaw and one freeze — the duplicate emitted nothing"
  );
}

/// THE FREEZE QUEUE INVARIANT: after every append, truncation, apply and restore of a live
/// endpoint the queue equals the ordered set of `PrepareMerge` indices in `(applied, last]` — the
/// ground truth `expected_queue` reads off the log by a scan the product never runs. Append queues
/// every freeze of the suffix in order; a conflict truncation above a queued freeze retracts only
/// what it overwrote and the replacement's own freeze re-queues; a truncation below one retracts
/// everything at-or-above it; the fold pops its own index and the next is the pending state; a
/// re-baseline clears and the re-delivered freeze re-queues; a restart rebuilds from the surviving
/// suffix. A REFUSED freeze clears the queue whole — the poison halts the drain, so nothing queued
/// above it ever applies: a poisoned endpoint breaks the invariant in the empty direction, by
/// design.
#[test]
fn the_freeze_queue_tracks_the_unapplied_suffix_exactly() {
  use core::time::Duration;
  fn assert_invariant(ep: &Endpoint<u64, CountSm>, log: &crate::testkit::FailTermLog, stage: &str) {
    assert_eq!(
      queue(ep),
      expected_queue(log, ep.applied_index()),
      "{stage}: the queue is the ordered PrepareMerge set of (applied, last]"
    );
  }
  let at = |indices: &[u64]| indices.iter().map(|i| Index::new(*i)).collect::<Vec<_>>();

  // APPEND: three freezes among ordinary entries, nothing committed.
  let (mut ep, mut log, mut stable) = follower_with_appended(std::vec![
    freeze_entry(1, 1, b"a", 1),
    normal_entry(1, 2),
    freeze_entry(1, 3, b"b", 2),
    normal_entry(1, 4),
    freeze_entry(1, 5, b"c", 3),
  ]);
  assert_invariant(&ep, &log, "after the append");
  assert_eq!(queue(&ep), at(&[1, 3, 5]));

  // TRUNCATION ABOVE a queued freeze: a newer leader's suffix replaces 4 and up — 5 leaves, 1 and
  // 3 stand — and the replacement's own freeze re-queues.
  follower_deliver(
    &mut ep,
    &mut log,
    &mut stable,
    2,
    Index::new(3),
    1,
    std::vec![normal_entry(2, 4)],
    Index::ZERO,
  );
  assert_invariant(&ep, &log, "after a truncation above a queued freeze");
  assert_eq!(queue(&ep), at(&[1, 3]));
  follower_deliver(
    &mut ep,
    &mut log,
    &mut stable,
    2,
    Index::new(4),
    2,
    std::vec![freeze_entry(2, 5, b"c", 3)],
    Index::ZERO,
  );
  assert_invariant(&ep, &log, "after the replacement suffix's freeze");
  assert_eq!(queue(&ep), at(&[1, 3, 5]));

  // TRUNCATION BELOW a queued freeze: a suffix from 2 overwrites 3 and 5 together; only the new 3
  // re-queues.
  follower_deliver(
    &mut ep,
    &mut log,
    &mut stable,
    3,
    Index::new(1),
    1,
    std::vec![normal_entry(3, 2), freeze_entry(3, 3, b"b", 2)],
    Index::ZERO,
  );
  assert_invariant(&ep, &log, "after a truncation below a queued freeze");
  assert_eq!(queue(&ep), at(&[1, 3]));

  // THE FOLD: the freeze at 1 applies and leaves; 3 is the pending state.
  follower_deliver(
    &mut ep,
    &mut log,
    &mut stable,
    3,
    Index::new(3),
    3,
    std::vec![],
    Index::new(1),
  );
  assert!(ep.is_frozen() && ep.applied_index() == Index::new(1));
  assert_invariant(&ep, &log, "after the fold");
  assert_eq!(queue(&ep), at(&[3]));
  assert_eq!(ep.freeze_pending(), Some(Index::new(3)));

  // RESTORE: a re-baseline discards the suffix and the queue with it; the re-delivered freeze
  // re-queues (the install path calls exactly these two seams).
  crate::LogStore::restore(&mut log, Index::new(3), Term::new(3));
  ep.note_freeze_rebaselined();
  assert_invariant(&ep, &log, "after the re-baseline");
  assert!(queue(&ep).is_empty());
  follower_deliver(
    &mut ep,
    &mut log,
    &mut stable,
    3,
    Index::new(3),
    3,
    std::vec![freeze_entry(3, 4, b"d", 4)],
    Index::ZERO,
  );
  assert_invariant(&ep, &log, "after the re-delivery");
  assert_eq!(queue(&ep), at(&[4]));

  // THE REFUSAL: a refused freeze at the front and a valid one queued above it — the poison
  // clears the queue whole.
  let (mut ep, mut log, mut stable) = follower_with_appended(std::vec![
    freeze_entry(1, 1, b"", 1),
    freeze_entry(1, 2, b"e", 2),
  ]);
  assert_eq!(queue(&ep), at(&[1, 2]));
  deliver_commit(&mut ep, &mut log, &mut stable, Index::new(2), Index::new(1));
  assert_eq!(ep.poison_reason(), Some(PoisonReason::MergeDecode));
  assert!(
    queue(&ep).is_empty() && !ep.merge_freeze_active(),
    "a refused freeze clears the queue whole: nothing above it ever applies"
  );

  // RESTART: the boot rebuild collects every freeze above the replayed prefix.
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut vlog = VecLog::default();
  vlog.force_append(&[
    freeze_entry(1, 1, b"a", 1),
    normal_entry(1, 2),
    freeze_entry(1, 3, b"b", 2),
    normal_entry(1, 4),
    freeze_entry(1, 5, b"c", 3),
  ]);
  let mut vstable = AsyncStable::default();
  vstable.force_state(Term::new(1), None, Index::new(1));
  let ep = Endpoint::restart(
    cfg,
    Instant::ORIGIN,
    42,
    CountSm::default(),
    1,
    &mut vlog,
    &mut vstable,
  );
  assert!(ep.is_frozen() && ep.poison_reason().is_none());
  assert_eq!(queue(&ep), at(&[3, 5]), "the boot scan rebuilt the queue");
  assert_eq!(queue(&ep), expected_queue(&vlog, ep.applied_index()));
}

/// A BOUNDED CACHE THAT EVICTS ALTERNATELY cannot stall the fold: the fold reads no suffix page
/// at all, so it completes on the apply fetch alone — one read — where a fold that re-derived its
/// pending state by walking the suffix found a cold page on every attempt and never completed
/// (each attempt's fetch warmed the very page the walk then found cold).
#[test]
fn a_freeze_fold_survives_an_alternately_cold_cache() {
  let (mut ep, mut log, mut stable) = follower_with_appended(std::vec![
    freeze_entry(1, 1, b"a", 1),
    normal_entry(1, 2),
    freeze_entry(1, 3, b"b", 2),
  ]);
  log.alternate_cold_on_read();
  let before = log.observed_entries_calls();
  deliver_commit(&mut ep, &mut log, &mut stable, Index::new(3), Index::new(1));
  // A few more cranks — more than a driver would spend before calling the replica stuck.
  for _ in 0..4 {
    ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  }
  assert!(
    ep.is_frozen() && ep.poison_reason().is_none(),
    "the fold completed against an alternately cold cache"
  );
  assert_eq!(ep.freeze_pending(), Some(Index::new(3)));
  assert_eq!(
    log.observed_entries_calls() - before,
    1,
    "exactly one read — the apply fetch; the fold touched no suffix page"
  );
}

/// THE ADOPT READS NO PAGE FOR THE QUEUE. The freeze queue is exact and the log is kept, so the
/// post-boundary queue is the queue's own entries above the boundary — a retain, not a scan. A
/// scan there restarted from the boundary after every cold page, and against a one-page cache the
/// adopt never completed: the park it was to clear stood, and with apply pinned by it nothing
/// shrank. The kept tail here holds a queued freeze above the boundary: the adopt performs zero
/// entry reads and leaves that freeze pending.
#[test]
fn the_adopt_keeps_the_queue_above_the_boundary_without_a_read() {
  use crate::{InstallSnapshot, SnapshotMeta, conf::ConfState};
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut log = crate::testkit::FailTermLog::default();
  let mut stable = AsyncStable::default();
  log.force_append(&[
    Entry::new(
      Term::new(1),
      Index::new(1),
      EntryKind::PrepareMerge,
      prepare_payload(b"\x2b", 1),
    ),
    Entry::new(
      Term::new(1),
      Index::new(2),
      EntryKind::CommitMerge,
      commit_payload(b"\x2a", Index::new(5), 1, 2),
    ),
    Entry::new(
      Term::new(1),
      Index::new(3),
      EntryKind::PrepareMerge,
      prepare_payload(b"\x2c", 3),
    ),
  ]);
  stable.force_state(Term::new(1), Some(1u64), Index::new(2));
  let mut ep = Endpoint::restart(
    cfg,
    Instant::ORIGIN,
    7,
    CountSm::default(),
    1,
    &mut log,
    &mut stable,
  );
  assert!(
    ep.is_frozen() && ep.pending_merge().is_some(),
    "frozen below the park, parked"
  );
  assert_eq!(
    ep.freeze_pending(),
    Some(Index::new(3)),
    "the freeze above the park is queued"
  );
  ep.advance_crossing_scan(&log);
  ep.note_merge_park_unresolvable(true);
  let meta = SnapshotMeta::new(
    Index::new(2),
    Term::new(1),
    ConfState::from_voters(std::vec![1u64, 2, 3]),
  )
  .with_shape_gen(2);
  let before = log.observed_entries_calls();
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    2u64,
    Message::InstallSnapshot(InstallSnapshot::new(
      Term::new(1),
      2u64,
      meta,
      encode_snapshot(9),
    )),
  );
  assert!(
    !ep.is_poisoned() && ep.pending_merge().is_none() && !ep.is_frozen(),
    "the adopt cleared the park and the freeze quartet"
  );
  assert_eq!(ep.applied_index(), Index::new(2));
  assert_eq!(
    ep.freeze_pending(),
    Some(Index::new(3)),
    "the queued freeze above the boundary stands"
  );
  assert_eq!(
    log.observed_entries_calls() - before,
    0,
    "zero entry reads: the queue was kept, never rescanned"
  );
}
