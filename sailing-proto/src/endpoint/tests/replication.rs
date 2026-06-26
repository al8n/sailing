use super::*;
use crate::{
  HeartbeatResponse,
  endpoint::MAX_READ_BATCH_ENTRIES,
  testkit::{AsyncStable, CountSm, FailTermLog, NoopLog, NoopStable, VecLog},
};

/// Regression (the follower must also reject IMPORTING the reserved sentinel index): the
/// leader reserves u64::MAX, but a malformed/version-skewed AppendEntries with prev_log_index
/// == u64::MAX - 1 carrying an entry at u64::MAX must be REJECTED, not imported — an entry committed
/// there is unreadable by the half-open apply/replication ranges (committed but never applied). The
/// contiguity validation derives the expected position via the same `next_log_index` choke-point, so
/// it poisons instead of appending.
///
/// MUTATION: revert the contiguity loop to bare `checked_add(1)` → the follower imports the entry at
/// the sentinel u64::MAX.
#[test]
fn append_entries_at_sentinel_index_poisons_not_imports() {
  use crate::{
    AppendEntries, Config, Entry, EntryKind, Index, Instant, LogStore as _, Message, Term,
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
  let mut stable = NoopStable::default();
  let d = Instant::ORIGIN;
  // Follower's log re-baselined to the ceiling: last_index == u64::MAX - 1, boundary term 1.
  log.restore(Index::new(u64::MAX - 1), Term::new(1));
  assert_eq!(log.last_index(), Index::new(u64::MAX - 1));

  // A leader (node 1, term 1) sends an entry at the reserved sentinel index u64::MAX. prev_log_index
  // == u64::MAX - 1 matches the follower's boundary, so the consistency check passes and the
  // contiguity validation is reached.
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    1u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(1),
      1u64,
      Index::new(u64::MAX - 1),
      Term::new(1),
      std::vec![Entry::new(
        Term::new(1),
        Index::new(u64::MAX),
        EntryKind::Empty,
        bytes::Bytes::new()
      )],
      Index::ZERO,
    )),
  );

  assert!(
    matches!(
      ep.poison_reason(),
      Some(crate::PoisonReason::NonContiguousAppend)
    ),
    "an entry at the reserved sentinel index must poison, not be imported; got {:?}",
    ep.poison_reason()
  );
  assert_eq!(
    log.last_index(),
    Index::new(u64::MAX - 1),
    "nothing appended at the sentinel index"
  );
  assert!(
    ep.poll_message().is_none(),
    "a poisoned node sends no AppendResponse"
  );
}

#[test]
fn quorum_makes_a_leader_and_heartbeats_follow() {
  use crate::{Config, Instant, Message, Term, VoteResponse};
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, Noop);
  let mut log = NoopLog;
  let mut stable = NoopStable::default();

  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable); // become candidate, term 1, self-vote
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {} // drain RequestVotes
  assert!(ep.role().is_candidate());

  // one more grant = quorum (2 of 3)
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResponse(VoteResponse::new(Term::new(1), 2u64, false, false)),
  );
  assert!(ep.role().is_leader());
  // it should be broadcasting heartbeats to peers
  let mut hb = 0;
  while let Some(o) = ep.poll_message() {
    if matches!(o.message(), Message::Heartbeat(_)) {
      hb += 1;
    }
  }
  assert_eq!(hb, 2);
  // leader event surfaced
  assert!(matches!(ep.poll_event(), Some(Event::LeaderChanged(_))));
}

#[test]
fn become_leader_appends_noop_and_inits_progress() {
  use crate::{Config, Instant, Message, Term, VoteResponse};
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = NoopStable::default();
  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable); // candidate
  ep.handle_storage(d, &mut log, &mut stable);
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResponse(VoteResponse::new(Term::new(1), 2u64, false, false)),
  );
  assert!(ep.role().is_leader());
  assert_eq!(log.last_index(), Index::new(1)); // no-op at index 1
  let crate::EntriesRead::Ready(entries) =
    log.entries(Index::new(1)..Index::new(2), u64::MAX).unwrap()
  else {
    panic!("a resident store never returns Pending");
  };
  assert!(entries[0].kind().is_empty());
}

#[test]
fn propose_appends_and_replicates() {
  use crate::{Config, Instant, Message, Term, VoteResponse};
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
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
  while ep.poll_message().is_some() {} // drain no-op AppendEntries
  while ep.poll_event().is_some() {} // drain LeaderChanged

  let idx = ep
    .propose(d, &mut log, &stable, &bytes::Bytes::from_static(b"cmd"))
    .unwrap();
  assert_eq!(idx, Index::new(2)); // after the no-op at 1
  let mut appends = 0;
  while let Some(o) = ep.poll_message() {
    if let Message::AppendEntries(ae) = o.message()
      && !ae.entries().is_empty()
    {
      appends += 1;
    }
  }
  assert_eq!(appends, 2); // to peers 2 and 3
}

#[test]
fn follower_appends_and_rejects_gap() {
  use crate::{AppendEntries, Config, Entry, EntryKind, Index, Instant, Message, Term};
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
  let mut stable = NoopStable::default();

  // matching append at index 1 (prev=0) — fresh entry, ack deferred until durable
  let e1 = Entry::new(
    Term::new(1),
    Index::new(1),
    EntryKind::Normal,
    bytes::Bytes::from_static(b"a"),
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
      std::vec![e1],
      Index::ZERO,
    )),
  );
  // No ack yet — append-before-ack: wait for durability.
  assert!(
    ep.poll_message().is_none(),
    "no ack before append is durable"
  );
  // Drain storage (VecLog completes synchronously on poll) → ack emitted.
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  let r = ep.poll_message().unwrap();
  assert!(
    matches!(r.message(), Message::AppendResponse(a) if !a.reject() && a.match_index()==Index::new(1))
  );
  assert_eq!(log.last_index(), Index::new(1));

  // gap: prev_log_index=5 we don't have → reject immediately (no append, no deferral)
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(1),
      1u64,
      Index::new(5),
      Term::new(1),
      std::vec![],
      Index::ZERO,
    )),
  );
  let r = ep.poll_message().unwrap();
  assert!(matches!(r.message(), Message::AppendResponse(a) if a.reject()));
}

/// Regression (AppendEntries entry contiguity): a follower MUST reject an AppendEntries
/// whose entries are not positionally contiguous from `prev_log_index`. The handler computes
/// `last_new` (the commit ceiling and the ack match) positionally but keys conflict detection,
/// the truncation boundary, and the store append off each entry's embedded `index()`. A malformed
/// or version-skewed message — here `prev_log_index=0` with a single entry whose embedded index is
/// `2` (a gap at index 1) and `leader_commit=1` — would otherwise advance commit to 1 and ack
/// index 1 while the store holds the entry at index 2, desyncing the log from the acked match. A
/// correct leader never sends this, so it is fatal corruption: the node poisons
/// (`NonContiguousAppend`) and, via the egress halt, emits nothing.
///
/// MUTATION: revert the contiguity loop (restore the positional `last_new` and drop the per-entry
/// index check) → the follower trusts the gap, so `is_poisoned()` is false and it appends/acks.
#[test]
fn non_contiguous_append_poisons() {
  use crate::{AppendEntries, Config, Entry, EntryKind, Index, Instant, Message, Term};
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = NoopStable::default();

  // prev_log_index=0 is consistent against the empty log, but the single entry's embedded index
  // is 2 — a gap at index 1. Positional `last_new` would be 1; the embedded index is 2.
  let gap = Entry::new(
    Term::new(1),
    Index::new(2),
    EntryKind::Normal,
    bytes::Bytes::from_static(b"x"),
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
      std::vec![gap],
      Index::new(1),
    )),
  );

  assert!(
    ep.is_poisoned(),
    "non-contiguous entries must poison the follower"
  );
  assert_eq!(
    ep.poison_reason().map(|r| r.as_str()),
    Some("non_contiguous_append"),
  );
  // Nothing appended, commit not advanced, and the egress halt suppresses any ack.
  assert_eq!(log.last_index(), Index::ZERO, "no entry appended");
  assert_eq!(ep.commit_index(), Index::ZERO, "commit not advanced");
  assert!(ep.poll_message().is_none(), "poisoned node emits no ack");
}

/// Regression (persist-before-ack on the immediate-ack path): a DUPLICATE `AppendEntries` for
/// entries that exist only in the follower's visible-but-unflushed (in-flight) tail must NOT be
/// acked as durable. `VecLog::submit_append` makes an entry visible immediately but releases its
/// `LogDone::Appended` only on `handle_storage`; if the duplicate's immediate `AppendResponse`
/// reported the in-flight index, the leader could count a phantom replica and commit an entry a
/// crash would lose (a non-quorum-durable commit). The immediate ack is clamped to
/// `durable_index`, so the duplicate reports the prior durable watermark (here `0`); the deferred
/// `FollowerAck` reports the full match once the append flushes.
///
/// MUTATION: revert the edit so the immediate `else` sends `last_new` unclamped → the first
/// assertion (duplicate acks `0`) fails because the duplicate over-acks the in-flight index.
#[test]
fn duplicate_append_does_not_ack_in_flight_tail() {
  use crate::{AppendEntries, Config, Entry, EntryKind, Index, Instant, Message, Term};
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

  let e1 = Entry::new(
    Term::new(1),
    Index::new(1),
    EntryKind::Normal,
    bytes::Bytes::from_static(b"a"),
  );

  // Establish term 1 as a DURABLE term first (a follower must not ack under a non-durable term).
  // A term-1 heartbeat (no entries) adopts the term; draining storage makes the term write durable
  // WITHOUT flushing a log tail, leaving `durable_index` at 0 for the persist-before-ack check below.
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
      std::vec![],
      Index::ZERO,
    )),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  assert_eq!(
    ep.durable.durable_index,
    Index::ZERO,
    "the term-1 heartbeat made the term durable without flushing a tail"
  );

  // First AppendEntries carries a NEW entry at index 1 → the follower appends it in-flight
  // (visible in VecLog) and registers a deferred FollowerAck. We deliberately do NOT drain
  // `handle_storage`, so the append's LogDone::Appended is still pending and `durable_index`
  // stays at ZERO.
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
      std::vec![e1.clone()],
      Index::ZERO,
    )),
  );
  assert!(
    ep.poll_message().is_none(),
    "fresh append: ack deferred until durable (no immediate ack)"
  );
  assert_eq!(
    log.last_index(),
    Index::new(1),
    "entry 1 is visible in the log"
  );

  // DUPLICATE AppendEntries for the SAME entry. Index 1 already matches (same index+term), so
  // nothing is appended → the immediate-ack `else` branch fires. Persist-before-ack: the match
  // must be clamped to the durable watermark (0), NOT the in-flight index 1.
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
      std::vec![e1.clone()],
      Index::ZERO,
    )),
  );
  let dup = ep
    .poll_message()
    .expect("duplicate emits an immediate AppendResponse");
  match dup.message() {
    Message::AppendResponse(a) => {
      assert!(!a.reject(), "duplicate is a success ack, not a reject");
      assert_eq!(
        a.match_index(),
        Index::ZERO,
        "persist-before-ack: the duplicate must report the durable watermark (0), \
           not the in-flight index 1"
      );
    }
    other => panic!("expected AppendResponse, got {other:?}"),
  }

  // Now drain storage → the deferred FollowerAck for index 1 fires → the follower reports the
  // full match (1) once the entry is genuinely durable.
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  let acked = ep
    .poll_message()
    .expect("the deferred FollowerAck fires after the append flushes");
  assert!(
    matches!(acked.message(), Message::AppendResponse(a) if !a.reject() && a.match_index() == Index::new(1)),
    "after flush the FollowerAck reports the full match (1)"
  );
}

/// Regression (`durable_index` advances independently of the `pending` ack action): a
/// follower's `FollowerAck` is CLEARED by a higher-term message before its append flushes, yet
/// the append still became durable — so `durable_index` must rise when the completion arrives.
///
/// Setup: the follower appends entry 1 in-flight (FollowerAck pending, durable still 0). A
/// higher-term AppendEntries clears `pending` (term change wipes it). `handle_storage` then
/// drains the original append's completion — which advances `durable_index` to 1 via the
/// unconditional advance, even though no `pending` action survives. A later duplicate's
/// immediate ack must report 1, proving the watermark advanced.
///
/// MUTATION: revert FIX 2 so the advance lives only inside the `FollowerAck`/`LeaderAppend`
/// arms. Then the cleared-pending completion hits the `_` arm, `durable_index` stays at 0, and
/// the duplicate clamps to `min(1, 0) = 0` — the assertion (duplicate acks 1) FAILS.
#[test]
fn durable_index_advances_after_term_cleared_follower_ack() {
  use crate::{AppendEntries, Config, Entry, EntryKind, Index, Instant, Message, Term};
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

  let e1 = Entry::new(
    Term::new(1),
    Index::new(1),
    EntryKind::Normal,
    bytes::Bytes::from_static(b"a"),
  );
  // Append entry 1 in-flight (FollowerAck pending; durable stays 0 — not drained).
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
      std::vec![e1.clone()],
      Index::ZERO,
    )),
  );
  while ep.poll_message().is_some() {}
  assert_eq!(
    ep.durable.durable_index,
    Index::ZERO,
    "append in-flight, not yet durable"
  );

  // Higher-term heartbeat (term 2) from the same leader clears `pending` (term change).
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(2),
      1u64,
      Index::new(1),
      Term::new(1),
      std::vec![],
      Index::ZERO,
    )),
  );
  while ep.poll_message().is_some() {}
  assert!(
    ep.pending.is_empty(),
    "term change must have cleared the pending FollowerAck"
  );

  // Drain the ORIGINAL append's completion. It became durable, so the watermark must rise to 1
  // even though no pending action survives.
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  assert_eq!(
    ep.durable.durable_index,
    Index::new(1),
    "the completed append advanced durable_index independently of the cleared pending"
  );

  // A duplicate of entry 1 (now at term 2's consistency) immediate-acks the now-durable 1.
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(2),
      1u64,
      Index::ZERO,
      Term::ZERO,
      std::vec![e1],
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
        Index::new(1),
        "the immediate ack reports the now-durable index 1, not a stale-low 0"
      );
    }
    other => panic!("expected AppendResponse, got {other:?}"),
  }
}

#[test]
fn quorum_ack_commits_and_applies() {
  use crate::{AppendResponse, Config, Index, Instant, Message, Term, VoteResponse};
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = NoopStable::default();
  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable);
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResponse(VoteResponse::new(Term::new(1), 2u64, false, false)),
  );
  // Drain storage so the no-op LeaderAppend fires (advances self match_index to 1).
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  let idx = ep
    .propose(d, &mut log, &stable, &bytes::Bytes::from_static(b"x"))
    .unwrap(); // index 2
  // Drain storage so the LeaderAppend for index 2 fires (advances self match_index to 2).
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}

  // peer 2 acks up to idx 2 → quorum (self match=2 + peer2 match=2) → commit + apply
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
      idx,
    )),
  );
  // Applied event for the Normal entry at idx 2 (the no-op at 1 is an Empty entry, not Applied)
  let applied: Vec<_> = core::iter::from_fn(|| ep.poll_event()).collect();
  assert!(
    applied
      .iter()
      .any(|e| matches!(e, Event::Applied(a) if a.index()==idx))
  );
}

/// Regression: a stale/duplicate AppendEntries must NOT truncate already-committed entries.
/// Raft §5.3: only delete-and-append from the first *conflicting* entry.
#[test]
fn stale_append_entries_does_not_erase_committed_entries() {
  use crate::{AppendEntries, Config, Entry, EntryKind, Index, Instant, Message, Term};
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
  let mut stable = NoopStable::default();

  // Feed 3 entries from leader 1, leader_commit=3 → follower appends and commits all three.
  // Payloads are Data-encoded (`encode_cmd`) so the committed entries decode as the SM's
  // `Command` and apply cleanly — an undecodable committed entry now (correctly) poisons.
  let e1 = Entry::new(
    Term::new(1),
    Index::new(1),
    EntryKind::Normal,
    encode_cmd(b"a"),
  );
  let e2 = Entry::new(
    Term::new(1),
    Index::new(2),
    EntryKind::Normal,
    encode_cmd(b"b"),
  );
  let e3 = Entry::new(
    Term::new(1),
    Index::new(3),
    EntryKind::Normal,
    encode_cmd(b"c"),
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
      std::vec![e1, e2, e3],
      Index::new(3),
    )),
  );
  // Fresh entries → ack deferred until durable; drain storage to release it.
  assert!(ep.poll_message().is_none(), "no ack before append durable");
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  // Must reply success with match_index=3.
  let r = ep.poll_message().unwrap();
  assert!(
    matches!(r.message(), Message::AppendResponse(a) if !a.reject() && a.match_index() == Index::new(3)),
    "expected success match_index=3 after full append"
  );
  assert_eq!(log.last_index(), Index::new(3), "log must hold 3 entries");

  // Now feed a stale/duplicate AppendEntries carrying only entry 1 (a short prefix already
  // present). Under the old code this would have truncated entries 2 and 3.
  let e1_dup = Entry::new(
    Term::new(1),
    Index::new(1),
    EntryKind::Normal,
    encode_cmd(b"a"),
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
      std::vec![e1_dup],
      Index::new(3),
    )),
  );
  // Must still reply success (last_new = prev(0) + len(1) = 1).
  let r2 = ep.poll_message().unwrap();
  assert!(
    matches!(r2.message(), Message::AppendResponse(a) if !a.reject()),
    "stale duplicate must still be accepted"
  );
  // Entries 2 and 3 must still be in the log — the stale message must not have erased them.
  assert_eq!(
    log.last_index(),
    Index::new(3),
    "stale AppendEntries must not truncate entries 2 and 3"
  );
}

#[test]
fn single_node_leader_commits_after_storage_drain() {
  use crate::{Config, Instant};
  use core::time::Duration;
  // 1-voter cluster: quorum == 1, so a lone node self-elects immediately.
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 42, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = NoopStable::default();

  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable); // self-elects (quorum=1)
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(ep.role().is_leader());

  // The no-op LeaderAppend is still in pending — commit has NOT advanced yet.
  // Drain storage: the no-op append completes → self match advances → commit advances.
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}

  // Now propose a Normal entry and drain storage so it commits.
  ep.propose(d, &mut log, &stable, &bytes::Bytes::from_static(b"cmd"))
    .unwrap();
  ep.handle_storage(d, &mut log, &mut stable);

  // Applied event for the Normal entry must have been emitted.
  let events: Vec<_> = core::iter::from_fn(|| ep.poll_event()).collect();
  assert!(
    events.iter().any(|e| matches!(e, Event::Applied(_))),
    "a single-node leader must commit after handle_storage drains"
  );
}

/// A cold (`EntriesRead::Pending`) committed-range read at apply time DEFERS: the node applies nothing
/// this pass and retries on the next pump (the `cold_read_defers` wedge counter bumps), and crucially
/// does NOT poison — a cold read is not a fault (unlike an `Err`). Single-node leader: commit advances
/// on append durability without any log read, so apply is the only reader and it defers cleanly.
#[test]
fn apply_defers_on_cold_read_without_poisoning() {
  use crate::{Config, Instant};
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 42, CountSm::default());
  let mut log = FailTermLog::default();
  let mut stable = NoopStable::default();

  // Self-elect (quorum == 1), drain so the no-op commits and applies.
  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable);
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(ep.role().is_leader());
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {} // drain the no-op's Applied event
  assert_eq!(
    ep.cold_read_defers(),
    0,
    "no cold defers before arming the fault"
  );

  // Arm the cold read, then propose + drain so commit advances over the new entry; apply reads it cold.
  log.return_cold_on_read();
  ep.propose(d, &mut log, &stable, &bytes::Bytes::from_static(b"cmd"))
    .unwrap();
  ep.handle_storage(d, &mut log, &mut stable);
  ep.handle_timeout(d, &mut log, &mut stable);

  assert!(
    ep.poison_reason().is_none(),
    "a cold read is not a fault — apply must defer, never poison"
  );
  assert!(
    ep.cold_read_defers() > 0,
    "the apply cold-read defer must bump the wedge counter"
  );
  assert!(
    !core::iter::from_fn(|| ep.poll_event()).any(|e| matches!(e, Event::Applied(_))),
    "the proposed entry must NOT apply while the read is cold (deferred)"
  );
}

/// A cold (`EntriesRead::Pending`) replication read DEFERS the send: `maybe_send_append` returns
/// without emitting an AppendEntries and WITHOUT poisoning. This is the one real behavior change — an
/// `Err` here is fatal (poison), but a cold read retries on the next pump, so a lagging follower whose
/// evicted range is being fetched no longer fail-stops the leader.
#[test]
fn replication_defers_on_cold_read_without_poisoning() {
  use crate::{Config, Index, Instant, Message, Term};
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut log = FailTermLog::default();
  let mut stable = NoopStable::default();

  // Elect node 1 leader (peer 2 votes) so it appends a no-op at index 1.
  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable);
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResponse(crate::VoteResponse::new(Term::new(1), 2u64, false, false)),
  );
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(ep.role().is_leader());
  ep.handle_storage(d, &mut log, &mut stable); // the no-op append fires
  while ep.poll_message().is_some() {} // drain the initial broadcast

  // Point follower 2 at index 1 (Probe) so a send must read entries(1..2), then make that read cold.
  if let Some(p) = ep.tracker.progress_mut(&2u64) {
    p.become_probe();
    p.set_next_index(Index::new(1));
  }
  log.return_cold_on_read();
  ep.maybe_send_append(crate::Now::monotonic(Instant::ORIGIN), 2u64, &log, &stable);

  assert!(
    ep.poison_reason().is_none(),
    "a cold replication read is not a fault — defer, never poison"
  );
  assert!(
    !core::iter::from_fn(|| ep.poll_message())
      .any(|m| matches!(m.message(), Message::AppendEntries(_))),
    "a cold replication read must defer the send — no AppendEntries emitted"
  );
}

/// The storage-ready re-drive for a deferred apply: a cold (`EntriesRead::Pending`) apply read leaves
/// `applied < commit` with NO LogDone to re-trigger apply via `on_log_appended`. When the store later
/// makes the range resident and signals storage-ready — which the driver services by calling
/// `handle_storage` with NO new completion — apply MUST re-pump, else an idle/single-node leader stalls
/// silently. Without the re-drive in `handle_storage`, the proposed entry would never apply.
#[test]
fn cold_apply_is_redriven_by_handle_storage_without_a_completion() {
  use crate::{Config, Instant};
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 42, CountSm::default());
  let mut log = FailTermLog::default();
  let mut stable = NoopStable::default();

  // Become leader, commit + apply the no-op (applied catches up).
  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable);
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(ep.role().is_leader());
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  // Arm cold, propose + drain: commit advances (the proposal's LogDone drains) but every apply read is
  // cold, so applied stays behind — NO Applied event yet, and no poison.
  log.return_cold_on_read();
  ep.propose(d, &mut log, &stable, &bytes::Bytes::from_static(b"cmd"))
    .unwrap();
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(
    !core::iter::from_fn(|| ep.poll_event()).any(|e| matches!(e, Event::Applied(_))),
    "the entry must NOT apply while the read is cold"
  );

  // The cold range becomes resident; the store signals storage-ready → `handle_storage` runs again with
  // NO new LogDone. The re-drive must apply the now-resident entry.
  log.clear_cold_on_read();
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(
    ep.poison_reason().is_none(),
    "the re-drive must apply cleanly, never poison"
  );
  assert!(
    core::iter::from_fn(|| ep.poll_event()).any(|e| matches!(e, Event::Applied(_))),
    "handle_storage with no LogDone must re-pump the deferred apply once the cold read resolves"
  );
}

/// A COLD/disk store may return `Ready(Owned(..))`, materialising the range. Apply must (a) iterate the
/// OWNED slice correctly and (b) request a BOUNDED `max_bytes` (the 1 MiB cap, never `u64::MAX`), so a
/// node catching up after a large committed backlog cannot force an O(backlog) materialisation in one read.
#[test]
fn apply_reads_are_byte_capped_and_handle_owned_entries() {
  use crate::{Config, Instant};
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 42, CountSm::default());
  let mut log = FailTermLog::default();
  let mut stable = NoopStable::default();
  log.return_owned_on_read(); // every read materialises OWNED — the cold/disk store shape

  // Become leader, propose, and drive apply over the OWNED read path.
  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable);
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(ep.role().is_leader());
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  ep.propose(d, &mut log, &stable, &bytes::Bytes::from_static(b"cmd"))
    .unwrap();
  ep.handle_storage(d, &mut log, &mut stable);

  assert!(
    ep.poison_reason().is_none(),
    "apply over an OWNED cold read must not poison"
  );
  assert!(
    core::iter::from_fn(|| ep.poll_event()).any(|e| matches!(e, Event::Applied(_))),
    "the entry must apply via the owned cold-read path"
  );
  assert_eq!(
    log.observed_max_bytes(),
    1 << 20,
    "apply must request a BOUNDED max_bytes (the 1 MiB cap), never u64::MAX — else an owned cold store \
     could materialise the whole committed backlog in one read"
  );
}

/// A large backlog of ZERO-payload committed entries (no-ops / empty / conf — common in Raft) must NOT
/// let an owned cold store materialise the whole backlog in one read: the payload-byte cap charges 0 for
/// them, so the CORE bounds the requested range WIDTH (entry count) at `MAX_READ_BATCH_ENTRIES`. Driven
/// via the restart replay (the lease-floor scans AND the apply replay) over an owned store, asserting
/// every committed-range read stays within the cap and the cap actually fires.
#[test]
fn owned_zero_payload_backlog_reads_are_count_bounded() {
  use crate::{Config, Entry, EntryKind, Index, Instant, Term};
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  // A committed backlog LARGER than the cap, every entry zero-payload (the byte cap charges 0 bytes).
  let n = MAX_READ_BATCH_ENTRIES + 200;
  let mut log = FailTermLog::default();
  log.return_owned_on_read(); // materialise OWNED — the cold/disk store shape
  let entries: Vec<Entry> = (1..=n)
    .map(|i| {
      Entry::new(
        Term::new(1),
        Index::new(i),
        EntryKind::Empty,
        bytes::Bytes::new(),
      )
    })
    .collect();
  log.force_append(&entries);
  let mut stable = NoopStable::default();
  stable.force_state(Term::new(1), Some(1u64), Index::new(n)); // commit = n
  let ep = Endpoint::restart(
    cfg,
    Instant::ORIGIN,
    1,
    CountSm::default(),
    1,
    &mut log,
    &mut stable,
  );

  assert!(
    ep.poison_reason().is_none(),
    "restart + apply over a large owned zero-payload backlog must succeed"
  );
  assert!(
    log.observed_max_range_width() <= MAX_READ_BATCH_ENTRIES,
    "every committed-range read must request at most MAX_READ_BATCH_ENTRIES indices ({} requested) — else \
     an owned store could materialise the whole zero-payload backlog in one read",
    log.observed_max_range_width()
  );
  assert_eq!(
    log.observed_max_range_width(),
    MAX_READ_BATCH_ENTRIES,
    "the entry-count cap must actually FIRE for a backlog larger than it (non-vacuous coverage)"
  );
}

/// A follower must not send AppendResponse until the new log entries are durable.
/// Uses `VecLog` which enqueues `LogDone::Appended` on `submit_append`, released on `poll`.
#[test]
fn follower_ack_waits_for_durable_append() {
  use crate::{AppendEntries, Config, Entry, EntryKind, Index, Instant, Message, Term};
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
  let mut stable = NoopStable::default();

  let e1 = Entry::new(
    Term::new(1),
    Index::new(1),
    EntryKind::Normal,
    bytes::Bytes::from_static(b"a"),
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
      std::vec![e1],
      Index::ZERO,
    )),
  );
  // append-before-ack: no AppendResponse yet (the append isn't durable)
  assert!(
    ep.poll_message().is_none(),
    "no ack before append is durable"
  );
  // drain storage → the append completes → AppendResponse(success) is emitted
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  let r = ep.poll_message().unwrap();
  assert!(
    matches!(r.message(), Message::AppendResponse(a) if !a.reject() && a.match_index()==Index::new(1)),
    "AppendResponse(success, match=1) must be emitted after handle_storage"
  );
}

/// Regression: a leader's heartbeat must advertise a commit index CLAMPED to each peer's
/// match index, never the leader's full `commit`. A bare heartbeat carries no prev-log
/// check, so a lagging follower with a divergent, uncommitted tail (e.g. a crashed ex-leader
/// whose durable log holds an orphan entry whose index == its last_index) would otherwise
/// commit+apply that stale entry on `min(hb.commit, last_index)`. Etcd's `min(committed,
/// pr.Match)` rule. Without this clamp the cluster loses a committed entry / applies a
/// phantom one.
#[test]
fn heartbeat_commit_is_clamped_to_peer_match() {
  use crate::{AppendResponse, Config, Index, Instant, Message, Term, VoteResponse};
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = NoopStable::default();

  // Elect node 1 leader (term 1) and let its no-op append become durable (commit→1).
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
  ep.handle_storage(d, &mut log, &mut stable); // no-op (index 1) becomes durable
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  // Propose two Normal entries (indices 2 and 3) and make them durable on the leader.
  ep.propose(d, &mut log, &stable, &bytes::Bytes::from_static(b"x"))
    .unwrap();
  ep.propose(d, &mut log, &stable, &bytes::Bytes::from_static(b"y"))
    .unwrap();
  ep.handle_storage(d, &mut log, &mut stable); // leader self-match → 3
  while ep.poll_message().is_some() {}

  // Peer 2 acks up to index 3 → quorum (leader match=3 + peer2 match=3) → commit advances to 3.
  // Peer 3 NEVER acks: its progress match_index stays at the post-election default (0/1).
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
      Index::new(3),
    )),
  );
  // Commit must have advanced to 3: the two Normal entries (idx 2, 3) are now applied.
  let applied: Vec<_> = core::iter::from_fn(|| ep.poll_event())
    .filter(|e| matches!(e, Event::Applied(_)))
    .collect();
  assert_eq!(
    applied.len(),
    2,
    "leader must have committed+applied indices 2 and 3 via the peer-2 quorum"
  );
  // Drain any replication traffic produced by the commit advance.
  while ep.poll_message().is_some() {}

  // Fire the heartbeat timer → broadcast_heartbeat to peers 2 and 3.
  let hb_deadline = ep.poll_timeout().unwrap();
  ep.handle_timeout(hb_deadline, &mut log, &mut stable);
  ep.handle_storage(hb_deadline, &mut log, &mut stable);

  // Collect the heartbeat advertised to the LAGGING peer 3.
  let mut hb_to_3: Option<Index> = None;
  while let Some(out) = ep.poll_message() {
    if out.to() == 3u64
      && let Message::Heartbeat(hb) = out.message()
    {
      hb_to_3 = Some(hb.commit());
    }
  }
  let advertised = hb_to_3.expect("a heartbeat must be sent to peer 3");
  // Peer 3's match index is far below the leader's commit (3). The heartbeat must be clamped.
  assert!(
    advertised < Index::new(3),
    "heartbeat to a lagging peer must be clamped below the leader commit (got {advertised:?})"
  );
}

/// A leader in Replicate mode with a window of 2 in-flight messages must stop sending
/// once both slots are occupied, and resume after an ack frees a slot.
#[test]
fn leader_paces_by_inflight_window() {
  use crate::{AppendResponse, Config, Index, Instant, Message, Term, VoteResponse};
  use core::time::Duration;

  // window = 2, no byte cap, unbounded per-msg size
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_max_inflight_msgs(2)
  .unwrap()
  .with_max_size_per_msg(u64::MAX);

  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = NoopStable::default();

  // Elect node 1 as leader.
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
  // Drain no-op append messages and storage.
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  // Transition peer 2 to Replicate by simulating it acking the no-op (index 1).
  // This calls become_replicate() on the progress, enabling the inflight window.
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
      Index::new(1), // ack no-op at index 1
    )),
  );
  // Drain any triggered sends (the become_replicate ack may trigger maybe_send_append).
  while ep.poll_message().is_some() {}

  // Propose 5 entries. With window=2 and Replicate mode, peer 2 should receive at most
  // 2 AppendEntries before the window fills.
  for i in 0u8..5 {
    let _ = ep
      .propose(d, &mut log, &stable, &bytes::Bytes::copy_from_slice(&[i]))
      .unwrap();
    ep.handle_storage(d, &mut log, &mut stable);
  }

  // Collect all non-empty AppendEntries sent to peer 2.
  let mut appends_to_2: usize = 0;
  let mut last_sent_index = Index::ZERO;
  while let Some(out) = ep.poll_message() {
    if out.to() == 2u64
      && let Message::AppendEntries(ae) = out.message()
      && !ae.entries().is_empty()
    {
      appends_to_2 += 1;
      if let Some(last) = ae.entries().last() {
        last_sent_index = last.index();
      }
    }
  }
  // With window=2 the leader must have stopped pipelining after 2 in-flight messages.
  assert!(
    appends_to_2 <= 2,
    "leader sent {appends_to_2} AppendEntries but window=2"
  );
  assert!(appends_to_2 > 0, "leader must send at least one batch");

  // Free the window: peer 2 acks through the last sent index.
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
      last_sent_index,
    )),
  );
  // After the ack, the leader should pipeline more entries (entries 5 and beyond).
  let mut resumed = false;
  while let Some(out) = ep.poll_message() {
    if out.to() == 2u64
      && let Message::AppendEntries(ae) = out.message()
      && !ae.entries().is_empty()
    {
      resumed = true;
    }
  }
  assert!(
    resumed,
    "leader must resume sending after ack frees the window"
  );
}

/// A SINGLE-entry ack from a peer already in Replicate must NOT
/// rewind `next_index` or reset the in-flight window. The old code called
/// `become_replicate()` unconditionally on every successful ack, which rewound
/// `next_index` to `match.next()` and reset the whole `Inflights` window — so the next
/// `maybe_send_append` re-sent the already-in-flight tail and the window cap never tripped.
///
/// Setup (window = 2, one entry per message so each send is observable):
///   peer 2 in Replicate at match=1, next=2; propose 4 entries (indexes 2..=5).
///   The window fills after entries 2 and 3 are pipelined (inflight = {2, 3}, next = 4);
///   entries 4 and 5 are held back (paused). Now ack ONLY index 2.
///
/// Expected (NEW): match advances to 2, slot for 2 frees, the peer STAYS in Replicate,
///   next stays 4 (never rewinds), and exactly ONE *new* entry (index 4) is pipelined —
///   the still-in-flight entry 3 is NOT re-sent. Final next = 5.
/// Old behaviour (BUG): become_replicate rewinds next to match.next() = 3 and clears the
///   window, so the post-ack send re-transmits index 3 (already in flight) and next ends
///   at 4 — strictly less than the NEW path's 5, and a wasted re-send of an in-flight entry.
#[test]
fn single_ack_does_not_rewind_replicate_window() {
  use crate::{AppendResponse, Config, Index, Instant, Message, Term, VoteResponse};
  use core::time::Duration;

  // window = 2, exactly one entry per AppendEntries (max_size_per_msg = 1 byte; each
  // command below is 1 byte) so every send carries a single, identifiable entry.
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_max_inflight_msgs(2)
  .unwrap()
  .with_max_size_per_msg(1);

  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = NoopStable::default();

  // Elect node 1 as leader.
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
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  // Move peer 2 into Replicate by acking the no-op (index 1). This is the legitimate
  // Probe -> Replicate transition (must still happen — preserved by the fix).
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
  assert!(
    ep.tracker.progress(&2u64).unwrap().state().is_replicate(),
    "peer 2 must be in Replicate after acking the no-op (Probe -> Replicate preserved)"
  );

  // Propose 4 entries (indexes 2..=5). With window = 2 the leader pipelines exactly two
  // (indexes 2 and 3) and then pauses; indexes 4 and 5 are held back.
  for i in 0u8..4 {
    let _ = ep
      .propose(d, &mut log, &stable, &bytes::Bytes::copy_from_slice(&[i]))
      .unwrap();
    ep.handle_storage(d, &mut log, &mut stable);
  }
  while ep.poll_message().is_some() {}

  // Snapshot the pipeline position: window is full at {2, 3}, next sits at 4.
  let next_before = ep.tracker.progress(&2u64).unwrap().next_index();
  assert_eq!(
    next_before,
    Index::new(4),
    "peer 2 should be pipelined to next=4 (entries 2,3 in flight) before the ack"
  );

  // Deliver a SINGLE-entry ack of just index 2 (the first in-flight index). This frees
  // exactly one slot; entry 3 is STILL in flight.
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
      Index::new(2), // ack ONLY index 2
    )),
  );

  // Collect the AppendEntries (and their entry indexes) the leader emits after the ack.
  let mut appends_after: usize = 0;
  let mut min_sent_index = Index::new(u64::MAX);
  let mut max_sent_index = Index::ZERO;
  while let Some(out) = ep.poll_message() {
    if out.to() == 2u64
      && let Message::AppendEntries(ae) = out.message()
      && !ae.entries().is_empty()
    {
      appends_after += 1;
      for e in ae.entries() {
        if e.index() < min_sent_index {
          min_sent_index = e.index();
        }
        if e.index() > max_sent_index {
          max_sent_index = e.index();
        }
      }
    }
  }

  let next_after = ep.tracker.progress(&2u64).unwrap().next_index();
  let match_after = ep.tracker.progress(&2u64).unwrap().match_index();

  // (1) The peer stayed in Replicate (no spurious become_replicate / state churn).
  assert!(
    ep.tracker.progress(&2u64).unwrap().state().is_replicate(),
    "peer 2 must remain in Replicate after a single-entry ack"
  );

  // (2) match_index advanced monotonically to the acked index.
  assert_eq!(
    match_after,
    Index::new(2),
    "match must advance to the acked index 2"
  );

  // (3) next_index is monotonic non-decreasing — it must NOT rewind below its pre-ack
  //     value. The old unconditional become_replicate() rewound it to match.next() = 3.
  assert!(
    next_after >= next_before,
    "next_index rewound: was {} now {} (the bug rewinds to match.next())",
    next_before.get(),
    next_after.get()
  );

  // (4) The window cap is respected: freeing one slot lets the leader send at most ONE
  //     new entry. It must be a *fresh* entry (index 4), NOT a re-send of the entry that
  //     is still in flight (index 3). The old code re-sent index 3 because the window was
  //     reset and next rewound to 3.
  assert!(
    appends_after <= 1,
    "expected at most one new AppendEntries after freeing one slot, got {appends_after}"
  );
  if appends_after > 0 {
    assert!(
      min_sent_index > Index::new(3),
      "leader re-sent in-flight entry {} (still in flight) instead of a fresh entry; \
         min_sent={} max_sent={}",
      min_sent_index.get(),
      min_sent_index.get(),
      max_sent_index.get()
    );
  }

  // (5) Net effect: the freed slot advanced the pipeline by exactly one fresh entry
  //     (index 4), so next reaches 5. The bug leaves next stuck at 4 (re-sent 3 -> next 4).
  assert_eq!(
    next_after,
    Index::new(5),
    "after freeing one slot the leader should pipeline exactly one fresh entry (index 4), \
       leaving next=5; the bug re-sends in-flight index 3 and leaves next=4"
  );
}

/// A divergent follower's reject carries a term hint that lets the leader skip a whole
/// conflicting term instead of backing off one entry at a time.
///
/// Scenario:
///   Leader log:   1@1 2@1 3@2 4@2 5@3
///   Follower log: 1@1 2@1 3@3 4@3   (diverges at index 3: has term-3 entries)
///
/// The leader (optimistically in Replicate, next=6) sends AppendEntries(prev=5@3, entries=[]).
/// The follower rejects: prev=5, but follower only has 4 entries; last_index=4, so hint is:
///   reject_hint_term = term(4) = 3 (on follower log)
///   reject_hint_index = first index where term==3 on follower = 3
///
/// Leader's find_conflict_by_term(index=3, term=3):
///   leader log term(3) = 2 < 3 → stop immediately at 3
///   → next_index = 3 (skip the whole stale term-3 region in one step)
#[test]
fn divergent_follower_resyncs_fast_via_term_skip() {
  use crate::{
    AppendEntries, AppendResponse, Config, Entry, EntryKind, Index, Instant, Message, Term,
    VoteResponse,
  };
  use core::time::Duration;

  // === Follower side: test the reject-hint computation ===
  // Node 2 is the follower with log [1@1, 2@1, 3@3, 4@3].
  let follower_cfg = Config::try_new(
    2u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut follower = Endpoint::new(follower_cfg, Instant::ORIGIN, 7, CountSm::default());
  let mut follower_log = VecLog::default();
  let mut follower_stable = NoopStable::default();

  // Seed follower log with [1@1, 2@1, 3@3, 4@3].
  follower_log.force_append(&[
    Entry::new(
      Term::new(1),
      Index::new(1),
      EntryKind::Normal,
      bytes::Bytes::from_static(b"a"),
    ),
    Entry::new(
      Term::new(1),
      Index::new(2),
      EntryKind::Normal,
      bytes::Bytes::from_static(b"b"),
    ),
    Entry::new(
      Term::new(3),
      Index::new(3),
      EntryKind::Normal,
      bytes::Bytes::from_static(b"c"),
    ),
    Entry::new(
      Term::new(3),
      Index::new(4),
      EntryKind::Normal,
      bytes::Bytes::from_static(b"d"),
    ),
  ]);

  // Leader sends AppendEntries(prev_index=4, prev_term=2) — inconsistency at prev.
  // Follower has term(4)=3 ≠ 2 → reject.
  follower.handle_message(
    Instant::ORIGIN,
    &mut follower_log,
    &mut follower_stable,
    1u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(3),
      1u64,
      Index::new(4), // prev_log_index
      Term::new(2),  // prev_log_term (leader has 4@2, follower has 4@3)
      std::vec![],
      Index::ZERO,
    )),
  );

  // The follower must reject with the etcd two-sided term-skip hint.
  // hint_index_raw = min(prev_log_index=4, last_index=4) = 4
  // find_conflict_by_term(follower_log, 4, ceiling=prev_log_term=2):
  //   term(4)=3 > 2 → 3; term(3)=3 > 2 → 2; term(2)=1 ≤ 2 → stop at 2
  // hint_index=2, hint_term=term(2)=1
  let response = follower
    .poll_message()
    .expect("follower must send AppendResponse(reject)");
  let ar = match response.message() {
    Message::AppendResponse(r) => *r,
    other => panic!("expected AppendResponse, got {other:?}"),
  };
  assert!(ar.reject(), "follower must reject the inconsistent append");
  // Etcd two-sided hint: walk from min(prev=4, last=4)=4 down while term > prev_log_term=2.
  // Stops at index 2 (term=1 ≤ 2).
  assert_eq!(
    ar.reject_hint_index(),
    Index::new(2),
    "hint index must be 2 (find_conflict_by_term walks below all term-3 entries)"
  );
  assert_eq!(
    ar.reject_hint_term(),
    Term::new(1),
    "hint term must be 1 (term at index 2 on follower)"
  );

  // === Leader side: test that find_conflict_by_term jumps next_index in one step ===
  // Node 1 is the leader with log [1@1, 2@1, 3@1, 4@1, 5@1] in term 1.
  // (We keep term=1 throughout so the leader doesn't step down.)
  // The reject hint (from follower's two-sided form) is (index=2, term=1).
  // Leader find_conflict_by_term(2, ceiling=1): term(2)=1 ≤ 1 → stop at 2 → next=2 → prev=1.
  let leader_cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut leader = Endpoint::new(leader_cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut leader_log = VecLog::default();
  let mut leader_stable = NoopStable::default();

  // Elect node 1 as leader (term=1, noop at index 1).
  let d = leader.poll_timeout().unwrap();
  leader.handle_timeout(d, &mut leader_log, &mut leader_stable);
  leader.handle_storage(d, &mut leader_log, &mut leader_stable);
  leader.handle_message(
    d,
    &mut leader_log,
    &mut leader_stable,
    2u64,
    Message::VoteResponse(VoteResponse::new(Term::new(1), 2u64, false, false)),
  );
  assert!(leader.role().is_leader());
  leader.handle_storage(d, &mut leader_log, &mut leader_stable);
  while leader.poll_message().is_some() {}
  while leader.poll_event().is_some() {}

  // Force-seed the leader log with 4 more entries so total = [1@1, 2@1, 3@1, 4@1, 5@1].
  // All term-1 entries. The follower will hint term=3 (its divergent term), which is
  // higher than any term on the leader's log. find_conflict_by_term(index=3, term=3)
  // will walk back: leader term(3)=1 ≤ 3 → stop at 3 → next_index = 3.
  leader_log.force_append(&[
    Entry::new(
      Term::new(1),
      Index::new(2),
      EntryKind::Normal,
      bytes::Bytes::from_static(b"b"),
    ),
    Entry::new(
      Term::new(1),
      Index::new(3),
      EntryKind::Normal,
      bytes::Bytes::from_static(b"c"),
    ),
    Entry::new(
      Term::new(1),
      Index::new(4),
      EntryKind::Normal,
      bytes::Bytes::from_static(b"d"),
    ),
    Entry::new(
      Term::new(1),
      Index::new(5),
      EntryKind::Normal,
      bytes::Bytes::from_static(b"e"),
    ),
  ]);

  // Simulate peer 2 acking index 1 (noop) → transitions to Replicate.
  leader.handle_message(
    d,
    &mut leader_log,
    &mut leader_stable,
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
  // Drain any pipelined sends triggered by the ack.
  while leader.poll_message().is_some() {}

  // Now simulate receiving the two-sided reject hint from peer 2:
  //   reject=true, reject_hint_index=2, reject_hint_term=1
  // find_conflict_by_term(leader_log, 2, ceiling=1): term(2)=1 ≤ 1 → conflict = 2.
  // etcd MaybeDecrTo: next = min(rejected_prev, conflict+1) = min(5, 3) = 3, prev_log_index = 2.
  // (The leader's index 2 is term 1 — exactly the follower's hint term — so probing prev=2 lands in
  // ONE round-trip; the old naive decrement would step back one slot per reject.)
  leader.handle_message(
    d,
    &mut leader_log,
    &mut leader_stable,
    2u64,
    Message::AppendResponse(AppendResponse::new(
      Term::new(1),
      2u64,
      true,          // reject
      Index::new(2), // reject_hint_index (etcd two-sided form)
      Term::new(1),  // reject_hint_term
      Index::ZERO,
    )),
  );

  // The leader should now send AppendEntries with prev_log_index = 2 (next_index = 3) — the
  // etcd `min(rejected, conflict+1)` jump. If the old naive decrement were used, prev would step
  // back only one slot per reject (a much higher prev_log_index here).
  let mut found_correct_prev = false;
  while let Some(out) = leader.poll_message() {
    if out.to() == 2u64
      && let Message::AppendEntries(ae) = out.message()
      && ae.prev_log_index() == Index::new(2)
    {
      found_correct_prev = true;
    }
  }
  assert!(
    found_correct_prev,
    "leader must jump next_index to 3 (prev=2) via two-sided term-skip hint, not step back one-by-one"
  );
}

/// A deeply-divergent follower — its WHOLE log conflicts, so its reject hint bottoms out at the
/// `(0,0)` form — must be re-synced in ONE round-trip: the leader jumps `next_index` straight to 1
/// (etcd `Progress.MaybeDecrTo`'s `min(rejected, hint+1)`), NOT decrement one index per reject. The
/// naive one-at-a-time walk is O(entries) round-trips; under the simulator's instant delivery that
/// is thousands of reject cycles compressed into a single tick, making a run pathologically slow.
/// (The symptom was a >350s run that the jump cuts to ~20s.)
///
/// Before fix: a `(0,0)` hint took the `conflict == 0` branch and stepped `next_index` back by one.
#[test]
fn deeply_divergent_follower_jumps_to_one_not_decrement() {
  use crate::{AppendResponse, Entry, EntryKind, Index, Message, Term};

  let (mut leader, mut log, mut stable, d) = make_three_node_leader();
  // Give the leader a 5-entry log [1@1 .. 5@1] (index 1 is the elected no-op).
  log.force_append(&[
    Entry::new(
      Term::new(1),
      Index::new(2),
      EntryKind::Normal,
      bytes::Bytes::from_static(b"b"),
    ),
    Entry::new(
      Term::new(1),
      Index::new(3),
      EntryKind::Normal,
      bytes::Bytes::from_static(b"c"),
    ),
    Entry::new(
      Term::new(1),
      Index::new(4),
      EntryKind::Normal,
      bytes::Bytes::from_static(b"d"),
    ),
    Entry::new(
      Term::new(1),
      Index::new(5),
      EntryKind::Normal,
      bytes::Bytes::from_static(b"e"),
    ),
  ]);
  // Peer 2 had been replicating, so its next_index is high (here 6). A deep-divergence reject
  // arrives: with a one-index decrement the leader would step to next=5 (prev=4).
  leader
    .tracker
    .progress_mut(&2u64)
    .unwrap()
    .set_next_index(Index::new(6));
  while leader.poll_message().is_some() {}

  leader.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendResponse(AppendResponse::new(
      Term::new(1),
      2u64,
      true,        // reject
      Index::ZERO, // reject_hint_index = 0  ┐ the follower's whole log conflicts:
      Term::ZERO,  // reject_hint_term  = 0  ┘ the `(0,0)` bottomed-out hint
      Index::ZERO,
    )),
  );

  // The leader must probe at prev_log_index = 0 (next_index jumped straight to 1) in ONE step.
  let mut prev = None;
  while let Some(out) = leader.poll_message() {
    if out.to() == 2u64
      && let Message::AppendEntries(ae) = out.message()
    {
      prev = Some(ae.prev_log_index());
    }
  }
  assert_eq!(
    prev,
    Some(Index::ZERO),
    "a (0,0) deep-divergence reject must jump next_index to 1 (prev=0) in one step, not decrement"
  );
}

/// A peer in Probe mode that has stalled (msg_app_flow_paused set because only a partial
/// batch was sent due to the byte cap) must resume replication when a HeartbeatResponse arrives.
#[test]
fn heartbeat_response_resumes_stalled_probe() {
  use crate::{Config, Instant, Message, Term, VoteResponse};
  use core::time::Duration;

  // max_size_per_msg=0 means exactly 1 entry per AppendEntries.
  // With multiple entries in the log, each send is a partial batch → probe pauses.
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_max_size_per_msg(0); // 0 = one entry per message

  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = NoopStable::default();

  // Elect node 1 as leader.
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
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  // Propose TWO more entries so the log has [noop@1, cmd1@2, cmd2@3].
  // With max_size_per_msg=0 (1 entry/msg), the probe from become_leader already sent
  // noop@1 alone. Since log.last_index()=1 and we sent to index 1 → not partial → no pause.
  // Now we add cmd1@2. After propose, maybe_send_append sends from next=1 (Probe unchanged):
  //   entries=[noop@1, cmd1@2], capped to 1 → sends [noop@1], last_sent=1, last_index=2 → partial → PAUSED.
  let _ = ep
    .propose(d, &mut log, &stable, &bytes::Bytes::from_static(b"cmd1"))
    .unwrap();
  ep.handle_storage(d, &mut log, &mut stable);
  let _ = ep
    .propose(d, &mut log, &stable, &bytes::Bytes::from_static(b"cmd2"))
    .unwrap();
  ep.handle_storage(d, &mut log, &mut stable);
  // Drain all messages from the propose phase (probe fires on first propose, then pauses).
  while ep.poll_message().is_some() {}

  // Probe is now paused (partial batch was sent: noop@1 sent, but cmd1@2/cmd2@3 remain).
  // A new propose would call maybe_send_append → paused → no send.
  let _ = ep
    .propose(d, &mut log, &stable, &bytes::Bytes::from_static(b"cmd3"))
    .unwrap();
  ep.handle_storage(d, &mut log, &mut stable);
  let mut probe_blocked = true;
  while let Some(out) = ep.poll_message() {
    if out.to() == 2u64
      && let Message::AppendEntries(_) = out.message()
    {
      probe_blocked = false;
    }
  }
  assert!(
    probe_blocked,
    "while probe is paused, a new propose must NOT trigger an AppendEntries to peer 2"
  );

  // A HeartbeatResponse from peer 2 must clear msg_app_flow_paused and call
  // maybe_send_append so the stalled probe resumes immediately.
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::HeartbeatResponse(HeartbeatResponse::new(
      Term::new(1),
      2u64,
      bytes::Bytes::new(),
    )),
  );
  let mut resumed = false;
  while let Some(out) = ep.poll_message() {
    if out.to() == 2u64
      && let Message::AppendEntries(_) = out.message()
    {
      resumed = true;
    }
  }
  assert!(
    resumed,
    "HeartbeatResponse must clear the probe pause and trigger an AppendEntries to peer 2"
  );
}

/// The per-message size cap must bound ZERO-BYTE entries: a long run of empty/no-op entries
/// (each `data().len() == 0`) must NOT bypass `max_size_per_msg`. With the old `entry_size`
/// (data-bytes only), a zero-byte entry cost 0, the packing budget never decreased, and a lagging peer
/// behind such a run would make the leader clone+send the WHOLE suffix in one AppendEntries — a
/// flow-control bypass / OOM risk. With the per-entry overhead, `max_size_per_msg = 0` packs exactly
/// one entry per message even for zero-byte entries.
///
/// MUTATION: revert `entry_size` to `e.data().len()` → the single send carries the whole zero-byte run.
#[test]
fn append_cap_bounds_zero_byte_entry_suffix() {
  use crate::{Config, Entry, EntryKind, Index, Instant, Message, Term, VoteResponse};
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_max_size_per_msg(0); // 0 = at most one entry per AppendEntries
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = NoopStable::default();

  // Elect node 1 leader (no-op@1).
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
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  // Append a long run of ZERO-BYTE (Empty / no-op) entries: indices 2..=51, term 1.
  let zero: Vec<_> = (2u64..=51)
    .map(|i| {
      Entry::new(
        Term::new(1),
        Index::new(i),
        EntryKind::Empty,
        bytes::Bytes::new(),
      )
    })
    .collect();
  log.force_append(&zero);

  // A HeartbeatResponse from the lagging peer 2 resumes replication → maybe_send_append packs from peer
  // 2's next index. The cap must bound the send to ONE zero-byte entry, not the whole 50-entry run.
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::HeartbeatResponse(HeartbeatResponse::new(
      Term::new(1),
      2u64,
      bytes::Bytes::new(),
    )),
  );
  let mut sent = None;
  while let Some(out) = ep.poll_message() {
    if out.to() == 2u64
      && let Message::AppendEntries(ae) = out.message()
    {
      sent = Some(ae.entries().len());
    }
  }
  let n = sent.expect("an AppendEntries was sent to peer 2");
  assert_eq!(
    n, 1,
    "max_size_per_msg=0 must cap the zero-byte suffix at ONE entry, not the whole run"
  );
}

// ---- Fix 1 regression: empty appends must NOT consume the inflight window ----

/// A caught-up Replicate peer triggers an empty AppendEntries on every HeartbeatResponse.
/// Before the fix, each call to `sent_entries` added a zero-byte inflight slot that was
/// never freed (no ack for empty sends), so after `max_inflight_msgs` heartbeat-responses
/// the window filled and newly proposed entries were silently not delivered.
///
/// This test uses a small window (4 slots), delivers many HeartbeatResponses (more than 4),
/// then proposes a new entry and asserts that an AppendEntries carrying it IS emitted.
#[test]
fn empty_appends_do_not_wedge_inflight_window() {
  use crate::{AppendResponse, Config, Index, Instant, Message, Term, VoteResponse};
  use core::time::Duration;

  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_max_inflight_msgs(4)
  .unwrap()
  .with_max_size_per_msg(u64::MAX);

  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = NoopStable::default();

  // Elect node 1 as leader.
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
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  // Transition peer 2 to Replicate by acking the no-op (index 1).
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

  // Deliver 10 HeartbeatResponses from peer 2 (each triggers an empty AppendEntries for a
  // caught-up peer). With window=4 and the bug, only 4 responses suffice to wedge the window.
  for _ in 0..10 {
    ep.handle_message(
      d,
      &mut log,
      &mut stable,
      2u64,
      Message::HeartbeatResponse(HeartbeatResponse::new(
        Term::new(1),
        2u64,
        bytes::Bytes::new(),
      )),
    );
    while ep.poll_message().is_some() {}
  }

  // Now propose a new entry. The leader must emit an AppendEntries carrying it to peer 2.
  let _idx = ep
    .propose(d, &mut log, &stable, &bytes::Bytes::from_static(b"new"))
    .unwrap();
  ep.handle_storage(d, &mut log, &mut stable);

  let mut delivered = false;
  while let Some(out) = ep.poll_message() {
    if out.to() == 2u64
      && let Message::AppendEntries(ae) = out.message()
      && !ae.entries().is_empty()
    {
      delivered = true;
    }
  }
  assert!(
    delivered,
    "after 10 heartbeat-responses the inflight window must not be wedged; proposed entry must be delivered to peer 2"
  );
}

// ---- Fix 2 regression: lagging-follower hint is O(terms) not O(entries) ----

/// A follower that is simply behind (prev_log_index > last_index) must emit a reject hint
/// whose term is meaningful so the leader can jump in one step.
///
/// Scenario: follower log [1..=2]@term1, leader sends AppendEntries(prev=20@term1).
/// - Old hint: (last_index.next()=3, Term::ZERO) → leader walks to index 0, falls back
///   to one-step decrement → O(entries) round-trips to converge.
/// - New hint (etcd two-sided): hint_index_raw=min(20,2)=2,
///   find_conflict_by_term(log, 2, ceiling=term1): term(2)=1 ≤ 1 → stop at 2
///   → hint=(2, term1). Leader's find_conflict_by_term(2, term1)=2 → next=2 → converges
///   on the very next send.
///
/// Verification: check the follower's hint_term is non-zero (meaningful), and that a
/// leader receiving it jumps to next=3 (prev=2) in one step — not to index 0.
#[test]
fn lagging_follower_hint_is_two_sided() {
  use crate::{
    AppendEntries, AppendResponse, Config, Entry, EntryKind, Index, Instant, Message, Term,
    VoteResponse,
  };
  use core::time::Duration;

  // Follower has [1@1, 2@1]; receives AppendEntries(prev=20, prev_term=1).
  let follower_cfg = Config::try_new(
    2u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut follower = Endpoint::new(follower_cfg, Instant::ORIGIN, 7, CountSm::default());
  let mut follower_log = VecLog::default();
  let mut follower_stable = NoopStable::default();
  follower_log.force_append(&[
    Entry::new(
      Term::new(1),
      Index::new(1),
      EntryKind::Normal,
      bytes::Bytes::from_static(b"a"),
    ),
    Entry::new(
      Term::new(1),
      Index::new(2),
      EntryKind::Normal,
      bytes::Bytes::from_static(b"b"),
    ),
  ]);

  follower.handle_message(
    Instant::ORIGIN,
    &mut follower_log,
    &mut follower_stable,
    1u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(1),
      1u64,
      Index::new(20), // prev_log_index far past follower's last (2)
      Term::new(1),   // prev_log_term
      std::vec![],
      Index::ZERO,
    )),
  );

  let response = follower.poll_message().expect("follower must reject");
  let ar = match response.message() {
    Message::AppendResponse(r) => *r,
    other => panic!("expected AppendResponse, got {other:?}"),
  };
  assert!(ar.reject(), "follower must reject (prev=20 > last=2)");
  // Two-sided hint: hint_index_raw=min(20,2)=2; find_conflict_by_term(log, 2, ceiling=1):
  // term(2)=1 ≤ 1 → stop → hint_index=2, hint_term=1 (NOT Term::ZERO as in the old code).
  assert_eq!(
    ar.reject_hint_index(),
    Index::new(2),
    "hint index must be 2 (follower's last index, walk stops immediately at ceiling)"
  );
  assert_ne!(
    ar.reject_hint_term(),
    Term::ZERO,
    "hint term must NOT be ZERO for a simply-lagging follower (old bug: always emitted ZERO)"
  );
  assert_eq!(
    ar.reject_hint_term(),
    Term::new(1),
    "hint term must be 1 (the term at the follower's last index)"
  );

  // Leader has [1..20]@term1. Receives reject hint (2, term1).
  // find_conflict_by_term(leader_log, 2, ceiling=1): term(2)=1 ≤ 1 → stop at 2 → next=2.
  // This gives prev=1 on the follow-up send — O(1) not O(entries).
  let leader_cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_max_size_per_msg(u64::MAX);
  let mut leader = Endpoint::new(leader_cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut leader_log = VecLog::default();
  let mut leader_stable = NoopStable::default();

  let d = leader.poll_timeout().unwrap();
  leader.handle_timeout(d, &mut leader_log, &mut leader_stable);
  leader.handle_storage(d, &mut leader_log, &mut leader_stable);
  leader.handle_message(
    d,
    &mut leader_log,
    &mut leader_stable,
    2u64,
    Message::VoteResponse(VoteResponse::new(Term::new(1), 2u64, false, false)),
  );
  assert!(leader.role().is_leader());
  leader.handle_storage(d, &mut leader_log, &mut leader_stable);
  while leader.poll_message().is_some() {}
  while leader.poll_event().is_some() {}

  // Force-seed indices 2..=20 so leader has [1..20]@term1.
  let extra: Vec<_> = (2u64..=20)
    .map(|i| {
      Entry::new(
        Term::new(1),
        Index::new(i),
        EntryKind::Normal,
        bytes::Bytes::from_static(b"x"),
      )
    })
    .collect();
  leader_log.force_append(&extra);

  // Peer 2 acks noop (index 1) → Replicate, next=2.
  leader.handle_message(
    d,
    &mut leader_log,
    &mut leader_stable,
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
  // Drain the pipelined sends after the ack (sends indices 2..=20 in one batch, then
  // records 1 inflight slot in Replicate).
  while leader.poll_message().is_some() {}

  // Inject the two-sided reject hint (2, term1) from the follower.
  // With the old hint (3, ZERO), the leader walks to index 0 and falls back to cur_next-1.
  // With the new hint (2, 1), find_conflict_by_term(leader_log, 2, 1)=2 → next=2, prev=1.
  leader.handle_message(
    d,
    &mut leader_log,
    &mut leader_stable,
    2u64,
    Message::AppendResponse(AppendResponse::new(
      Term::new(1),
      2u64,
      true,          // reject
      Index::new(2), // hint_index from two-sided follower
      Term::new(1),  // hint_term: NON-ZERO so leader can land in one step
      Index::ZERO,
    )),
  );

  // The leader must send AppendEntries with prev_log_index ≤ 2 (next_index ≤ 3).
  // If the old code were used with hint=(2, 0), it would fall back to cur_next-1 = 20
  // (because find_conflict_by_term walks to 0 with ceiling=0 → safe_next = cur_next-1).
  let mut found_low_prev = false;
  while let Some(out) = leader.poll_message() {
    if out.to() == 2u64
      && let Message::AppendEntries(ae) = out.message()
    {
      // With two-sided hint the leader jumps to next=2 → prev=1.
      if ae.prev_log_index() <= Index::new(2) {
        found_low_prev = true;
      }
    }
  }
  assert!(
    found_low_prev,
    "leader must jump to prev ≤ 2 via the two-sided hint (O(1) round-trip), not back off one-by-one"
  );
}

/// The deferred ack must not over-ack a durable-but-DIVERGENT tail. A follower holds a durable
/// tail through index 10 (from an old leader), but a NEW higher-term leader proves consistency only
/// through index 8. The success ack is deferred (term not yet durable); when it flushes it must report
/// the leader-proven match (8), NOT the follower's durable_index (10) — otherwise the leader would
/// count this follower for entries 9-10 it never replicated and could commit without a real quorum.
///
/// MUTATION: flush `self.ack_watermark()` instead of `proven.min(self.ack_watermark())` in
/// `flush_term_gated_acks` (i.e. drop the proven cap) → the flushed match is 10.
#[test]
fn deferred_ack_does_not_over_ack_divergent_tail() {
  use crate::{AppendEntries, Entry, EntryKind, Index, Instant, Message, Term};
  let (mut ep, mut log, mut stable) = make_follower();

  // Durable divergent tail: entries 1..=10 at term 1, durable through 10. (`force_append` writes the
  // VecLog directly; mirror `durable_index` so the follower's durable state is self-consistent.)
  let entries: Vec<_> = (1u64..=10)
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
  ep.durable.durable_index = Index::new(10);

  // A NEW leader at a higher term (5) sends a heartbeat-shaped AppendEntries proving only through 8
  // (prev=8, term 1 matches; no entries). The follower adopts term 5 (not yet durable) → the success
  // ack DEFERS with proven match = last_new = 8.
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(5),
      1u64,
      Index::new(8),
      Term::new(1),
      std::vec![],
      Index::ZERO,
    )),
  );
  assert!(
    !ep.term_is_durable(),
    "term 5 is adopted but not yet durable"
  );
  assert!(
    ep.poll_message().is_none(),
    "the success ack is deferred under the non-durable term"
  );

  // Complete the term write → flush the deferred ack.
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  let acks: Vec<_> = core::iter::from_fn(|| ep.poll_message())
    .filter(|o| matches!(o.message(), Message::AppendResponse(a) if !a.reject()))
    .collect();
  assert_eq!(
    acks.len(),
    1,
    "the deferred ack is flushed once term 5 is durable"
  );
  let m = match acks[0].message() {
    Message::AppendResponse(a) => a.match_index(),
    _ => unreachable!(),
  };
  assert_eq!(
    m,
    Index::new(8),
    "the flushed ack must report only the leader-proven match (8), NOT the durable-but-divergent \
       tail (10)"
  );
}

/// Regression (AppendResponse success match is bounded by the leader's log): a sender-authentic
/// but malformed/version-skewed voter that reports a `match_index` ABOVE the leader's own
/// `log.last_index()` must be ignored. Accepting it would corrupt the peer's `Progress`
/// (`maybe_update` never lowers a match again) and push `maybe_advance_commit`'s quorum candidate
/// past the log — a FALSE commit of an entry only the leader holds. Here the leader's log reaches
/// index 1; peer 2 reports match 1000. The over-ack is dropped: peer 2's match stays 0, commit stays
/// 0, and the leader is not poisoned.
///
/// MUTATION: delete the `match_within_log` guard in `on_append_response`'s success branch → peer 2's
/// match jumps to 1000 and commit advances to 1 (a non-quorum-durable false commit).
#[test]
fn append_response_over_ack_above_log_is_ignored() {
  use crate::{Index, Instant, Message, Term, VoteResponse};
  use core::time::Duration;
  let cfg = crate::Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
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
  ep.handle_storage(d, &mut log, &mut stable); // flush the no-op (index 1) durably
  while ep.poll_message().is_some() {}
  assert_eq!(log.last_index(), Index::new(1));

  // Peer 2 reports an impossible match far above the leader's log.
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendResponse(crate::AppendResponse::new(
      Term::new(1),
      2u64,
      false,
      Index::ZERO,
      Term::ZERO,
      Index::new(1000),
    )),
  );

  assert!(!ep.is_poisoned(), "over-ack must not poison the leader");
  assert_eq!(
    ep.tracker.progress(&2u64).unwrap().match_index(),
    Index::ZERO,
    "over-ack must be ignored — peer match must not be corrupted past the leader's log"
  );
  assert_eq!(
    ep.commit_index(),
    Index::ZERO,
    "no false commit from a single peer's over-ack"
  );
}

/// Regression (AppendResponse reject hint is clamped to the leader's log): a peer-supplied
/// `reject_hint_index` is clamped to `log.last_index()` before the term-skip walk, so the walk only
/// ever reads indexes the leader actually holds. The `FailTermLog` is armed to fail `term()` ONLY
/// at the out-of-range hint (`u64::MAX`); with the clamp the walk starts at `min(hint, last=5)=5`
/// and never touches that index, so the leader is not poisoned. Without the clamp the walk reads
/// `term(u64::MAX)` → `Err` → poison: a single malformed reject would halt the whole leader.
/// (`LogStore::term` is allowed to error on an out-of-range index — `VecLog` happens to be total,
/// which is why this needs a strict store to exercise.)
///
/// MUTATION: drop the `min(_, log.last_index())` clamp → the walk reads `term(u64::MAX)`, the armed
/// failure fires, and the leader poisons.
#[test]
fn append_response_reject_hint_beyond_log_does_not_poison() {
  use crate::{
    AppendResponse, Config, Entry, EntryKind, Index, Instant, Message, Term, VoteResponse,
  };
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut leader = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut log = FailTermLog::default();
  let mut stable = NoopStable::default();

  let d = leader.poll_timeout().unwrap();
  leader.handle_timeout(d, &mut log, &mut stable);
  leader.handle_storage(d, &mut log, &mut stable);
  leader.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResponse(VoteResponse::new(Term::new(1), 2u64, false, false)),
  );
  assert!(leader.role().is_leader());
  leader.handle_storage(d, &mut log, &mut stable);
  // Seed durable term-1 entries so last_index = 5.
  log.force_append(&[
    Entry::new(
      Term::new(1),
      Index::new(2),
      EntryKind::Empty,
      bytes::Bytes::new(),
    ),
    Entry::new(
      Term::new(1),
      Index::new(3),
      EntryKind::Empty,
      bytes::Bytes::new(),
    ),
    Entry::new(
      Term::new(1),
      Index::new(4),
      EntryKind::Empty,
      bytes::Bytes::new(),
    ),
    Entry::new(
      Term::new(1),
      Index::new(5),
      EntryKind::Empty,
      bytes::Bytes::new(),
    ),
  ]);
  while leader.poll_message().is_some() {}

  // Fail term() ONLY at the out-of-range hint index; in-range terms (1..=5) stay readable.
  log.fail_term_at(Some(Index::new(u64::MAX)));
  leader.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendResponse(AppendResponse::new(
      Term::new(1),
      2u64,
      true,
      Index::new(u64::MAX),
      Term::new(1),
      Index::ZERO,
    )),
  );

  assert!(
    !leader.is_poisoned(),
    "an out-of-range reject hint must be clamped to the leader's log, not poison it"
  );
  // The peer was re-probed within the leader's log.
  let pr = leader.peer_progress(&2u64).expect("peer 2 tracked");
  assert!(
    pr.next_index <= Index::new(6),
    "next_index stays within the leader's log"
  );
}

/// Regression (`HardState.commit` is fenced by the durable log): a follower commits over a
/// visible-but-not-yet-durable tail (`commit=7`, `durable_index=5`), then a higher-term message
/// steps it down and persists hard state. The persisted commit MUST be fenced to the durable log
/// (`durable_commit() = min(commit, durable_index) = 5`), never raw `self.commit=7` — otherwise a
/// crash leaves `HardState.commit > durable log`, which restart would have to silently lower
/// (discarding the persisted commitment).
///
/// MUTATION: revert any commit-stamp site to `.with_commit(self.commit)` / `committed_persisted =
/// self.commit` → the persisted commit jumps to 7, above the durable log at 5.
#[test]
fn commit_persist_is_fenced_by_durable_index() {
  use crate::{
    AppendEntries, Config, Entry, EntryKind, Index, Instant, Message, RequestVote, Term,
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
  let mut stable = NoopStable::default();
  let d = Instant::ORIGIN;

  // Become a follower at term 2 with a durable log [1..=5], commit=5.
  let mut e = Vec::new();
  for i in 1u64..=5 {
    e.push(Entry::new(
      Term::new(2),
      Index::new(i),
      EntryKind::Empty,
      bytes::Bytes::new(),
    ));
  }
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
      e,
      Index::new(5),
    )),
  );
  ep.handle_storage(d, &mut log, &mut stable); // make [1..=5] durable → durable_index=5, committed_persisted=5
  assert_eq!(ep.commit, Index::new(5));
  assert_eq!(ep.durable.durable_index, Index::new(5));

  // Append [6,7] and commit to 7, but DO NOT run handle_storage — the tail stays visible-but-not-
  // durable, so durable_index stays at 5 while commit advances to 7.
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(2),
      2u64,
      Index::new(5),
      Term::new(2),
      std::vec![
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
  assert_eq!(
    ep.commit,
    Index::new(7),
    "commit advanced over the visible tail"
  );
  assert_eq!(
    ep.durable.durable_index,
    Index::new(5),
    "tail not yet durable"
  );
  assert_eq!(
    ep.durable_commit(),
    Index::new(5),
    "durable_commit fences to the durable log"
  );

  // A higher-term message steps the node down and persists hard state — the commit it persists must
  // be the FENCED value (5), not raw self.commit (7).
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    3u64,
    Message::RequestVote(RequestVote::new(
      Term::new(3),
      3u64,
      Index::new(7),
      Term::new(2),
      false,
      false,
    )),
  );
  assert_eq!(
    ep.durable.committed_persisted,
    Index::new(5),
    "persisted commit must be fenced to the durable log (5), not the over-committed 7"
  );
}

/// CheckQuorum/PreVote step-down nudge: a node that ADVANCED its term during a partition (here to
/// term 8) and then receives a STALE-term Heartbeat from a node still claiming leadership at a
/// lower term (3) must reply with an AppendResponse at ITS OWN higher term — the stale leader adopts
/// it and steps down, breaking the wedge where it can neither replicate to us (our term is higher)
/// nor be unseated by us (we are too far behind to win an election). Mirrors etcd's `m.Term <
/// r.Term` MsgAppResp branch; only fires when CheckQuorum or PreVote is enabled (plain Raft relies
/// on the disruptive higher-term campaign instead).
///
/// Before fix: the stale-term branch silently `return`ed for every non-pre-vote message, so NO
/// response was sent and the lower-term leader never learned it was stale — a permanent livelock.
#[test]
fn stale_term_heartbeat_forces_leader_step_down() {
  use crate::{Config, Index, Instant, Message, Term};
  use core::time::Duration;

  let make = |pre_vote: bool| {
    let mut cfg = Config::try_new(
      2u64,
      std::vec![1u64, 2u64, 3u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
    if pre_vote {
      cfg = cfg.with_pre_vote(true);
    }
    // Node 2 manually advanced to term 8 (as if it campaigned during a partition and is now far
    // behind a leader that stayed at the lower term 3).
    let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 7, Noop);
    ep.term = Term::new(8);
    ep
  };
  let stale_heartbeat = || {
    Message::Heartbeat(crate::Heartbeat::new(
      Term::new(3), // stale: 3 < our 8
      1u64,         // a node still claiming leadership at the stale term
      Index::ZERO,
      bytes::Bytes::new(),
    ))
  };

  // pre_vote ON: the stale heartbeat must provoke an AppendResponse at OUR term (8).
  let mut ep = make(true);
  let mut log = NoopLog;
  let mut stable = NoopStable::default();
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    stale_heartbeat(),
  );
  let response = ep
    .poll_message()
    .expect("pre_vote on: must reply to a stale-term heartbeat to force the stale leader down");
  assert_eq!(
    response.to(),
    1u64,
    "the nudge must go back to the stale leader"
  );
  match response.message() {
    Message::AppendResponse(ar) => assert_eq!(
      ar.term(),
      Term::new(8),
      "the nudge must carry OUR higher term so the stale leader adopts it and steps down"
    ),
    other => panic!("expected AppendResponse (step-down nudge), got {other:?}"),
  }
  assert_eq!(
    ep.term(),
    Term::new(8),
    "must NOT adopt the stale lower term"
  );

  // Neither mode: the same stale heartbeat is silently dropped (plain-Raft behavior preserved).
  let mut ep2 = make(false);
  ep2.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    stale_heartbeat(),
  );
  assert!(
    ep2.poll_message().is_none(),
    "without check_quorum/pre_vote a stale heartbeat is silently dropped (no nudge)"
  );
}

/// Write-amplification invariant: once the floor is durable, steady-state heartbeats
/// at a stable term add NO HardState write.
///
/// MUTATION: drop the early-return in `ensure_term_durable` (write on every message) → steady-state
/// heartbeats each submit a HardState write.
#[test]
fn steady_state_heartbeats_add_no_hardstate_write() {
  use core::time::Duration;
  let et = Duration::from_millis(1000);
  let (mut ep, mut log, mut stable) = enforcing_follower(et);
  let now = crate::Instant::ORIGIN;
  // First heartbeat establishes term 5 + the floor; drain both writes to durability.
  let _ = follower_advertised_support(&mut ep, &mut log, &mut stable, now, 5, 1);
  ep.handle_storage(now, &mut log, &mut stable);
  while stable.pending_writes() > 0 {
    ep.handle_storage(now, &mut log, &mut stable);
  }
  // Steady-state heartbeats at the SAME term: no new HardState writes.
  for r in 2..8 {
    let _ = follower_advertised_support(&mut ep, &mut log, &mut stable, now, 5, r);
  }
  assert_eq!(
    stable.pending_writes(),
    0,
    "steady-state heartbeats must add no HardState write (the floor is a process-lifetime constant)"
  );
}

/// Side-effect-free fail-stop on a higher-term malformed AppendEntries. Adopting a higher
/// term no longer persists the step-down BEFORE the handler validates: a higher-term AppendEntries
/// with a non-contiguous suffix poisons (`NonContiguousAppend`) WITHOUT first writing the adopted
/// term to stable, so a restart cannot recover into a term the node never validly entered.
///
/// MUTATION: restore the eager `submit_write` in `handle_message`'s higher-term branch (or drop the
/// `ensure_term_durable` deferral) → the durable term becomes the malformed message's term (5).
#[test]
fn higher_term_malformed_append_poisons_without_persisting_term() {
  use crate::{
    AppendEntries, Config, Entry, EntryKind, Index, Instant, Message, PoisonReason, Term,
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
  let mut stable = NoopStable::default();
  let mut log = VecLog::default();
  assert_eq!(
    stable.hard_state().term(),
    Term::ZERO,
    "baseline durable term is 0"
  );

  // Higher-term (5) AppendEntries: prev_log_index=0 passes the consistency check on the empty log,
  // but the suffix is non-contiguous (first entry at index 2, not the expected 1).
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(5),
      1u64,
      Index::ZERO,
      Term::ZERO,
      std::vec![Entry::new(
        Term::new(5),
        Index::new(2),
        EntryKind::Empty,
        bytes::Bytes::new(),
      )],
      Index::ZERO,
    )),
  );

  assert!(
    ep.is_poisoned(),
    "a non-contiguous higher-term append must poison"
  );
  assert_eq!(ep.poison_reason(), Some(PoisonReason::NonContiguousAppend));
  assert_eq!(
    stable.hard_state().term(),
    Term::ZERO,
    "the adopted term must NOT be persisted before the fail-stop (side-effect-free)"
  );
}

/// Catch-up replication PIPELINES: a single success ack from a lagging follower must trigger a
/// window-fill of follow-up batches (the pump), not one byte-capped batch per ack round-trip.
/// Without the pump a follower catching up over a 3 MiB backlog moves at max_size_per_msg
/// (1 MiB) per RTT while the 256-slot inflight window sits idle.
#[test]
fn ack_pumps_multiple_batches_to_a_lagging_follower() {
  use crate::{AppendResponse, Config, Index, Instant, Message, Term, VoteResponse};
  use core::time::Duration;

  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, Noop);
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
  while ep.poll_message().is_some() {}

  // A ~3 MiB backlog: 30 proposals of 100 KiB (max_size_per_msg is the 1 MiB default, so the
  // backlog spans ~3 byte-capped batches).
  let payload = bytes::Bytes::from(std::vec![0u8; 100 * 1024]);
  for _ in 0..30 {
    ep.propose(d, &mut log, &stable, &payload).unwrap();
    ep.handle_storage(d, &mut log, &mut stable);
  }
  while ep.poll_message().is_some() {} // discard the optimistic per-propose sends

  // Rewind the peer to index 1 via a reject (hint 0): Probe state, whole backlog unsent.
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendResponse(AppendResponse::new(
      Term::new(1),
      2u64,
      true,
      Index::ZERO,
      Term::ZERO,
      Index::ZERO,
    )),
  );
  // The probe sends exactly ONE byte-capped batch; note where it ends.
  let probe: Vec<_> = core::iter::from_fn(|| ep.poll_message()).collect();
  let probe_batches: Vec<_> = probe
    .iter()
    .filter_map(|o| match o.message() {
      Message::AppendEntries(ae) if o.to() == 2u64 => Some(ae),
      _ => None,
    })
    .collect();
  assert_eq!(probe_batches.len(), 1, "Probe sends a single batch");
  let probe_end = probe_batches[0].entries().last().unwrap().index();

  // ONE success ack of the probe batch: Probe -> Replicate, and the pump must fill the window
  // with the REST of the backlog (>= 2 further byte-capped batches), not a single batch.
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
      probe_end,
    )),
  );
  let pumped: Vec<_> = core::iter::from_fn(|| ep.poll_message()).collect();
  let batches: Vec<_> = pumped
    .iter()
    .filter_map(|o| match o.message() {
      Message::AppendEntries(ae) if o.to() == 2u64 => Some(ae),
      _ => None,
    })
    .collect();
  assert!(
    batches.len() >= 2,
    "one ack must pump multiple follow-up batches (got {})",
    batches.len()
  );
  // And the pump must cover the whole backlog up to the leader's last index.
  let last_sent = batches
    .iter()
    .filter_map(|ae| ae.entries().last())
    .map(|e| e.index())
    .max()
    .unwrap();
  assert_eq!(
    last_sent,
    log.last_index(),
    "the pump fills the window to the end of the backlog"
  );
}

// on_heartbeat fail-stops if apply_committed self-poisons: it must NOT raise the durable lease-support
// floor (or otherwise act) after a fatal committed-range read on a dead node. (Egress is poison-suppressed,
// so the lease-support floor is the observable discriminator.)
#[test]
fn on_heartbeat_fail_stops_when_apply_committed_poisons() {
  use crate::{
    AppendEntries, Config, Entry, EntryKind, Heartbeat, Index, Instant, Message, PoisonReason, Term,
  };
  use core::time::Duration;
  let cfg = Config::try_new(
    2u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_check_quorum(true);
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 7, CountSm::default());
  let mut log = FailTermLog::default();
  let mut stable = NoopStable::default();
  let d = Instant::ORIGIN;
  let entries: Vec<Entry> = (1u64..=3)
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
      entries,
      Index::new(3),
    )),
  );
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  // Force applied behind commit so the next on_heartbeat re-applies the committed range.
  ep.applied = Index::ZERO;
  assert!(ep.poison_reason().is_none());
  let floor_before = ep.durable.lease_support_floor;

  // Arm the fatal committed-range read; apply_committed reads entries(1..) and poisons (LogRead).
  log.fail_entries_at(Some(Index::new(1)));
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    1u64,
    Message::Heartbeat(Heartbeat::new(
      Term::new(1),
      1u64,
      Index::new(3),
      bytes::Bytes::new(),
    )),
  );
  assert_eq!(ep.poison_reason(), Some(PoisonReason::LogRead));
  assert_eq!(
    ep.durable.lease_support_floor, floor_before,
    "on_heartbeat must not raise the lease-support floor after apply_committed poisons"
  );
  assert!(
    ep.poll_message().is_none(),
    "no HeartbeatResponse on a dead node"
  );
}
