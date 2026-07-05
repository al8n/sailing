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
