//! The missed-completion reconciliation legs: a lost `LogDone`/`StableDone` heals off the store's
//! durable probe once that store is quiescent, and a `None`-answering store keeps the documented stall.
use super::{super::*, *};
use crate::{
  RequestVote, StorageProgress,
  testkit::{AsyncStable, CountSm, SwallowLog, SwallowStable, VecLog},
};
use core::time::Duration;

const ELECTION_TIMEOUT: Duration = Duration::from_millis(1000);
const HEARTBEAT: Duration = Duration::from_millis(100);

/// Elect node 1 as a single-voter leader on the given stores and drain to quiescence: the no-op at
/// index 1 is committed, applied and persisted, and both completion queues are empty.
fn elect_single_node_leader<L, S>(log: &mut L, stable: &mut S) -> (Endpoint<u64, CountSm>, Instant)
where
  L: LogStore,
  S: StableStore<NodeId = u64>,
{
  let cfg = Config::try_new(1u64, std::vec![1u64], ELECTION_TIMEOUT, HEARTBEAT).unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 42, CountSm::default());
  let at = ep.poll_timeout().expect("a timer is armed");
  ep.handle_timeout(at, log, stable);
  // The self-vote write, the no-op append and the commit persist each need their own crank.
  for _ in 0..8 {
    if ep.handle_storage(at, log, stable).is_drained() {
      break;
    }
  }
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}
  assert!(ep.role().is_leader(), "node 1 leads its single-voter group");
  assert_eq!(ep.commit, Index::new(1), "the no-op committed");
  assert_eq!(
    ep.durable.durable_index,
    Index::new(1),
    "the no-op append is durable"
  );
  assert_eq!(
    ep.durable.durable_commit_index,
    Index::new(1),
    "the commit watermark reached stable storage"
  );
  assert!(ep.pending_is_empty(), "no deferred action is outstanding");
  (ep, at)
}

/// Propose one command on a leader that is about to lose its `Appended` completion, then drain once.
/// The append IS durable in the store; only the completion vanishes.
fn drive_swallowed_leader_append(
  answer_probe: bool,
) -> (Endpoint<u64, CountSm>, SwallowLog, AsyncStable) {
  let mut log = SwallowLog::default();
  let mut stable = AsyncStable::default();
  let (mut ep, at) = elect_single_node_leader(&mut log, &mut stable);
  log.answer_probe(answer_probe);
  log.swallow_next_appended(1);
  let cmd = bytes::Bytes::from_static(b"x");
  ep.propose(at, &mut log, &stable, &cmd)
    .expect("the leader accepts the proposal");
  ep.flush_appends(at, &log, &stable);
  assert_eq!(
    log.last_index(),
    Index::new(2),
    "the proposal is visible at index 2"
  );
  ep.handle_storage(at, &mut log, &mut stable);
  (ep, log, stable)
}

/// (a) A leader whose `Appended` completion is swallowed cannot advance `durable_index` — its own match,
/// and so the commit the match carries, park forever. With the durable-frontier probe the leg folds the
/// evidence and discharges the `LeaderAppend` through the SAME release the completion would have run, in
/// the same `handle_storage` call that swallowed it.
///
/// MUTATION: delete the `reconcile_durable_index` call in `handle_storage` → commit stays at 1 and the
/// `LeaderAppend` stays pending, exactly as the `None`-answering control below.
#[test]
fn swallowed_append_completion_heals_off_the_durable_frontier() {
  let (ep, log, _stable) = drive_swallowed_leader_append(true);
  assert_eq!(
    log.durable_index(),
    Some(Index::new(2)),
    "the store still holds the append: only the completion was lost"
  );
  assert_eq!(
    ep.durable.durable_index,
    Index::new(2),
    "the leg folded the store's frontier into the persist-before-ack watermark"
  );
  assert_eq!(
    ep.commit,
    Index::new(2),
    "discharging the LeaderAppend advanced the leader's own match and the commit it carries"
  );
  assert_eq!(ep.applied, Index::new(2), "and the commit applied");
  assert!(
    ep.pending_is_empty(),
    "the deferred action released, not merely the watermark"
  );
}

/// (a, control) The same swallowed completion against a `None`-answering log keeps the documented SAFE
/// STALL, unchanged: `durable_index` sits at 1, the leader's match never covers index 2, so commit and
/// applied stay at 1 and the `LeaderAppend` stays parked — no further crank helps.
#[test]
fn swallowed_append_completion_stalls_on_a_none_answering_log() {
  let (mut ep, mut log, mut stable) = drive_swallowed_leader_append(false);
  let at = Instant::ORIGIN + ELECTION_TIMEOUT;
  for _ in 0..4 {
    ep.handle_storage(at, &mut log, &mut stable);
  }
  assert_eq!(
    log.durable_index(),
    None,
    "the control store offers no probe"
  );
  assert_eq!(
    ep.durable.durable_index,
    Index::new(1),
    "without the probe the lost completion still wedges the watermark"
  );
  assert_eq!(ep.commit, Index::new(1), "so the commit never advances");
  assert!(
    !ep.pending_is_empty(),
    "and the LeaderAppend stays parked until a restart re-submits"
  );
}

/// Campaign as a single voter with the self-vote's `Wrote` completion swallowed, then drain once.
fn drive_swallowed_self_vote(
  answer_probe: bool,
) -> (Endpoint<u64, CountSm>, VecLog, SwallowStable) {
  let cfg = Config::try_new(1u64, std::vec![1u64], ELECTION_TIMEOUT, HEARTBEAT).unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 42, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = SwallowStable::default();
  stable.answer_probe(answer_probe);
  stable.swallow_next_wrote(1);
  let at = ep.poll_timeout().expect("a timer is armed");
  ep.handle_timeout(at, &mut log, &mut stable);
  assert!(ep.role().is_candidate(), "the timeout started a campaign");
  ep.handle_storage(at, &mut log, &mut stable);
  (ep, log, stable)
}

/// (b) The candidate's self-vote reached stable storage but its completion vanished, so `Campaign` has
/// nothing to fire on. The durable `(term, vote)` PROVES the self-vote at that term, which is exactly
/// what the pending action waits for — so the leg discharges it and the candidate leads.
///
/// MUTATION: delete the `reconcile_durable_hard_state` call in `handle_storage` → the node stays a
/// candidate forever, as the `None`-answering control below.
#[test]
fn swallowed_self_vote_completion_lets_the_candidate_lead() {
  let (ep, _log, stable) = drive_swallowed_self_vote(true);
  let hs = stable
    .durable_hard_state()
    .expect("the probing store answers");
  assert_eq!(
    (hs.term(), hs.vote()),
    (Term::new(1), Some(1u64)),
    "the self-vote IS durable: only its completion was lost"
  );
  assert!(
    ep.role().is_leader(),
    "the durable self-vote released the campaign"
  );
  assert!(
    ep.pending_stable.is_empty(),
    "the Campaign discharged (what remains is the new leader's own no-op, on the LOG seam)"
  );
}

/// (b, control) Against a `None`-answering store the persist-before-ACT gate keeps its documented stall:
/// the node stays a candidate with its `Campaign` parked, no matter how often storage is cranked.
#[test]
fn swallowed_self_vote_completion_stalls_on_a_none_answering_store() {
  let (mut ep, mut log, mut stable) = drive_swallowed_self_vote(false);
  let at = Instant::ORIGIN + ELECTION_TIMEOUT;
  for _ in 0..4 {
    ep.handle_storage(at, &mut log, &mut stable);
  }
  assert!(
    ep.role().is_candidate(),
    "no evidence, no release — the candidate never leads"
  );
  assert!(!ep.pending_is_empty(), "the Campaign stays parked");
}

/// Grant a vote as a follower with the vote write's `Wrote` completion swallowed, then drain once.
fn drive_swallowed_vote_grant(
  answer_probe: bool,
) -> (Endpoint<u64, CountSm>, VecLog, SwallowStable) {
  let cfg = Config::try_new(
    2u64,
    std::vec![1u64, 2u64, 3u64],
    ELECTION_TIMEOUT,
    HEARTBEAT,
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 7, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = SwallowStable::default();
  stable.answer_probe(answer_probe);
  stable.swallow_next_wrote(1);
  let at = Instant::ORIGIN;
  ep.handle_message(
    at,
    &mut log,
    &mut stable,
    1u64,
    Message::RequestVote(RequestVote::new(
      Term::new(1),
      1u64,
      Index::ZERO,
      Term::ZERO,
      false,
      false,
    )),
  );
  assert!(
    ep.poll_message().is_none(),
    "persist-before-respond: the grant is withheld until the write is durable"
  );
  ep.handle_storage(at, &mut log, &mut stable);
  (ep, log, stable)
}

/// (c) A follower's vote write is durable but its completion vanished, so the withheld `VoteResponse`
/// has nothing to fire on. The durable state carries that EXACT vote at that term, so the leg releases
/// the grant — the §5.1 evidence the gate demands is present, just not as a completion.
///
/// MUTATION: delete the `reconcile_durable_hard_state` call in `handle_storage` → no VoteResponse is
/// ever emitted, as the `None`-answering control below.
#[test]
fn swallowed_vote_write_completion_releases_the_grant() {
  let (mut ep, _log, _stable) = drive_swallowed_vote_grant(true);
  let out = ep.poll_message().expect("the grant went out");
  match out.message() {
    Message::VoteResponse(vr) => {
      assert!(!vr.reject(), "the vote was granted");
      assert_eq!(vr.term(), Term::new(1), "at the term it was cast in");
    }
    other => panic!("expected a VoteResponse, got {other:?}"),
  }
  assert!(ep.pending_is_empty(), "the CastVote action discharged");
}

/// (c, control) With no probe the withheld grant stays withheld — the documented safe stall, since a
/// peer that never saw the grant cannot count it toward a quorum.
#[test]
fn swallowed_vote_write_completion_stalls_on_a_none_answering_store() {
  let (mut ep, mut log, mut stable) = drive_swallowed_vote_grant(false);
  let at = Instant::ORIGIN + HEARTBEAT;
  for _ in 0..4 {
    ep.handle_storage(at, &mut log, &mut stable);
  }
  assert!(
    ep.poll_message().is_none(),
    "no evidence, no grant — the vote response is never emitted"
  );
  assert!(!ep.pending_is_empty(), "the CastVote stays parked");
}

/// Commit a second entry on a single-voter leader, then swallow the `Wrote` completion of the
/// commit-watermark write the tail submits for it.
fn drive_swallowed_commit_write(
  answer_probe: bool,
) -> (Endpoint<u64, CountSm>, VecLog, SwallowStable) {
  let mut log = VecLog::default();
  let mut stable = SwallowStable::default();
  let (mut ep, at) = elect_single_node_leader(&mut log, &mut stable);
  stable.answer_probe(answer_probe);
  stable.swallow_next_wrote(1);
  let cmd = bytes::Bytes::from_static(b"x");
  ep.propose(at, &mut log, &stable, &cmd)
    .expect("the leader accepts the proposal");
  ep.flush_appends(at, &log, &stable);
  // First crank: the append completes, commit advances to 2 and the tail submits the commit write.
  ep.handle_storage(at, &mut log, &mut stable);
  assert_eq!(ep.commit, Index::new(2), "index 2 committed");
  // Second crank: that write's completion is the one swallowed.
  ep.handle_storage(at, &mut log, &mut stable);
  (ep, log, stable)
}

/// (d) The commit-axis floor. A swallowed `Wrote` leaves `durable_commit_index` behind the commit the
/// write actually persisted, which is the crash-surviving evidence `shortcut_ack_ready` demands before a
/// no-blob redundancy ack may go out. The leg folds the floor off `hs.commit()` — the field that proves
/// it — and `flush_shortcut_gated_ack` (driven at the end of the same call) can then release.
///
/// MUTATION: delete the `hs.commit()` fold in `reconcile_durable_hard_state` → the floor stays at 1, as
/// the `None`-answering control below.
#[test]
fn swallowed_commit_write_completion_heals_the_durable_commit_floor() {
  let (ep, _log, stable) = drive_swallowed_commit_write(true);
  assert_eq!(
    stable
      .durable_hard_state()
      .expect("the probing store answers")
      .commit(),
    Index::new(2),
    "the commit IS durable: only its completion was lost"
  );
  assert_eq!(
    ep.durable.durable_commit_index,
    Index::new(2),
    "the leg folded the durable commit floor off the state that proves it"
  );
}

/// (d, control) Without the probe the floor keeps its documented stall at the last completion-proven
/// value, so a shortcut ack would keep waiting rather than go out on evidence a crash could erase.
#[test]
fn swallowed_commit_write_completion_stalls_on_a_none_answering_store() {
  let (mut ep, mut log, mut stable) = drive_swallowed_commit_write(false);
  let at = Instant::ORIGIN + ELECTION_TIMEOUT;
  for _ in 0..4 {
    ep.handle_storage(at, &mut log, &mut stable);
  }
  assert_eq!(
    ep.durable.durable_commit_index,
    Index::new(1),
    "no evidence, no floor advance"
  );
  assert!(
    ep.durable.last_submitted_commit > ep.durable.durable_commit_index,
    "the submitted commit stays unproven, exactly as today"
  );
}

/// Deliver one Heartbeat (leader 1, term `t`, lease round `r`) and return the `lease_support` the
/// follower advertised in its HeartbeatResponse.
fn advertised_support<S>(
  ep: &mut Endpoint<u64, CountSm>,
  log: &mut VecLog,
  stable: &mut S,
  now: Instant,
  t: u64,
  r: u64,
) -> Duration
where
  S: StableStore<NodeId = u64>,
{
  ep.handle_message(
    now,
    log,
    stable,
    1u64,
    Message::Heartbeat(
      crate::Heartbeat::new(Term::new(t), 1u64, Index::ZERO, bytes::Bytes::new())
        .with_lease_round(r),
    ),
  );
  let mut support = None;
  while let Some(out) = ep.poll_message() {
    if let Message::HeartbeatResponse(hr) = out.message() {
      support = Some(hr.lease_support());
    }
  }
  support.expect("the follower produced a HeartbeatResponse")
}

/// Take one heartbeat on an enforcing follower (which raises and persists its lease-support floor),
/// swallow that write's completion, drain, then take a second heartbeat. Returns both advertisements.
fn drive_swallowed_lease_floor(
  answer_probe: bool,
) -> (Duration, Duration, Endpoint<u64, CountSm>, SwallowStable) {
  let cfg = Config::try_new(
    2u64,
    std::vec![1u64, 2u64, 3u64],
    ELECTION_TIMEOUT,
    Duration::from_millis(50),
  )
  .unwrap()
  .with_check_quorum(true)
  .with_read_only(ReadOnlyOption::LeaseBased);
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 7, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = SwallowStable::default();
  stable.answer_probe(answer_probe);
  // Every write in this drive loses its completion, so only the probe can prove the floor durable.
  stable.swallow_next_wrote(8);
  let at = Instant::ORIGIN;
  let first = advertised_support(&mut ep, &mut log, &mut stable, at, 5, 1);
  ep.handle_storage(at, &mut log, &mut stable);
  let second = advertised_support(&mut ep, &mut log, &mut stable, at, 5, 2);
  (first, second, ep, stable)
}

/// (d) The lease-axis floor. The persist-before-ADVERTISE gate holds a follower at ZERO lease support
/// until its floor is durable; a swallowed `Wrote` would hold it at ZERO forever, silently degrading
/// every LeaseBased read on the leader. The leg folds the floor off `hs.lease_support()` — the field
/// that proves it — and the next heartbeat advertises the follower's real election timeout.
///
/// MUTATION: delete the `hs.promised_lease_support()` fold in `reconcile_durable_hard_state` → the
/// second advertisement is ZERO again, as the `None`-answering control below.
#[test]
fn swallowed_lease_floor_completion_releases_the_advertise_gate() {
  let (first, second, ep, _stable) = drive_swallowed_lease_floor(true);
  assert_eq!(
    first,
    Duration::ZERO,
    "a follower advertises ZERO until its floor is durable"
  );
  assert_eq!(
    second, ELECTION_TIMEOUT,
    "the durable floor released the advertise gate"
  );
  assert_eq!(
    ep.durable.durable_lease_support,
    Some(ELECTION_TIMEOUT),
    "the leg folded the durable lease floor off the state that proves it"
  );
}

/// (d, control) Without the probe the gate keeps its documented stall — ZERO forever, which is SAFE
/// (the leader floats no lease on a promise a crash could erase) but never recovers.
#[test]
fn swallowed_lease_floor_completion_stalls_on_a_none_answering_store() {
  let (first, second, ep, _stable) = drive_swallowed_lease_floor(false);
  assert_eq!(first, Duration::ZERO, "pre-durable advertise is ZERO");
  assert_eq!(
    second,
    Duration::ZERO,
    "and stays ZERO: no evidence, no advertisement"
  );
  assert_eq!(
    ep.durable.durable_lease_support, None,
    "the floor is still unproven"
  );
}

/// Bring a follower to a DURABLE term (so each append's ack goes out on its own completion rather than
/// coalescing into the single term-gated slot), then deliver two AppendEntries whose completions the
/// store COALESCES: the first append's `Appended` is dropped, only the second's is delivered.
fn drive_coalesced_appends() -> (Endpoint<u64, CountSm>, SwallowLog, AsyncStable) {
  let cfg = Config::try_new(
    2u64,
    std::vec![1u64, 2u64, 3u64],
    ELECTION_TIMEOUT,
    HEARTBEAT,
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 7, CountSm::default());
  let mut log = SwallowLog::default();
  let mut stable = AsyncStable::default();
  let at = Instant::ORIGIN;
  let append = |prev: u64, prev_term: u64, entries: std::vec::Vec<Entry>| {
    Message::AppendEntries(crate::AppendEntries::new(
      Term::new(1),
      1u64,
      Index::new(prev),
      Term::new(prev_term),
      entries,
      Index::ZERO,
    ))
  };
  let entry = |index: u64| {
    Entry::new(
      Term::new(1),
      Index::new(index),
      crate::EntryKind::Empty,
      bytes::Bytes::new(),
    )
  };

  ep.handle_message(at, &mut log, &mut stable, 1u64, append(0, 0, std::vec![]));
  for _ in 0..4 {
    if ep.handle_storage(at, &mut log, &mut stable).is_drained() {
      break;
    }
  }
  while ep.poll_message().is_some() {}
  assert!(
    ep.term_is_durable(),
    "the adopted term is durable before the two appends"
  );

  // A at index 1, then B at index 2 — both accepted before any drain, so both are in flight.
  ep.handle_message(
    at,
    &mut log,
    &mut stable,
    1u64,
    append(0, 0, std::vec![entry(1)]),
  );
  ep.handle_message(
    at,
    &mut log,
    &mut stable,
    1u64,
    append(1, 1, std::vec![entry(2)]),
  );
  assert_eq!(
    ep.durable.inflight_append_upto.len(),
    2,
    "both appends are in flight"
  );
  assert!(
    ep.poll_message().is_none(),
    "persist-before-ack: each ack waits on its own append's durability"
  );

  // The store coalesces: A's completion is dropped, B's is delivered.
  log.swallow_next_appended(1);
  ep.handle_storage(at, &mut log, &mut stable);
  (ep, log, stable)
}

/// A COALESCING store is the sharp case: B's delivered completion raises `durable_index` to the batch
/// tip and removes only B's record, leaving A's record and its `FollowerAck` behind at an index the
/// probe now merely CONFIRMS. The watermark has nothing new to learn, but the DRAIN does — so the
/// stale-answer no-op must apply to the watermark alone, never to the release. Otherwise A accumulates
/// in `inflight_append_upto` and `pending_log` under ordinary traffic and its ack never goes out.
///
/// MUTATION: restore an `if d <= self.durable.durable_index { return true; }` early return ahead of the
/// drain in `reconcile_durable_index` → A's record and its ack stay parked forever.
#[test]
fn a_coalescing_store_releases_the_earlier_append_it_never_completed() {
  let (mut ep, _log, _stable) = drive_coalesced_appends();
  assert_eq!(
    ep.durable.durable_index,
    Index::new(2),
    "the delivered completion carried the batch tip"
  );
  assert!(
    ep.durable.inflight_append_upto.is_empty(),
    "no in-flight record survives the reconcile"
  );
  assert!(
    ep.pending_is_empty(),
    "and no gated action is left parked behind the lost completion"
  );
  let mut acks = std::vec::Vec::new();
  while let Some(out) = ep.poll_message() {
    if let Message::AppendResponse(a) = out.message()
      && !a.reject()
    {
      acks.push(a.match_index());
    }
  }
  assert_eq!(
    acks,
    std::vec![Index::new(2), Index::new(1)],
    "B acked on its own completion; the reconcile then released A's ack (cumulative, so a lower \
     match after a higher one is exactly what A's own completion would have sent)"
  );
}

/// (e) A store answering BELOW what the core already holds must move no WATERMARK. Both legs fold with
/// `max`, so a conservative clamp (a truncation or re-baseline capped the frontier) or a genuinely
/// older durable state is a provable no-op — asserted across every watermark the legs touch. The log
/// leg's DRAIN still runs (see the coalescing-store regression above); here it releases nothing because
/// the in-flight append sits ABOVE the watermark, which the assertion below pins explicitly.
///
/// The stable half is the sharp one: a NEWER term write is in flight (submitted, fsync not landed) while
/// the store honestly answers the OLDER durable state. Had the leg copied `on_stable_wrote`'s
/// `last_submitted_term` shape onto store evidence, it would declare term 5 durable on the evidence of
/// term 1 and release a persist-before-respond gate a crash still erases.
///
/// MUTATION: fold `self.durable.durable_term = self.durable.last_submitted_term` (the completion-path
/// shape) in `reconcile_durable_hard_state` → `durable_term` jumps to 5 on term-1 evidence.
#[test]
fn a_stale_probe_answer_changes_no_watermark() {
  let mut log = SwallowLog::default();
  let mut stable = SwallowStable::default();
  let (mut ep, at) = elect_single_node_leader(&mut log, &mut stable);

  // An append and a hard-state write are both accepted with their fsync still in flight: nothing is
  // queued to poll, so BOTH gates are open — and both stores honestly answer their PRIOR durable state.
  log.hold_appends(true);
  log.force_durable_index(Some(Index::ZERO));
  stable.hold_writes(true);
  let cmd = bytes::Bytes::from_static(b"x");
  ep.propose(at, &mut log, &stable, &cmd)
    .expect("the leader accepts the proposal");
  ep.flush_appends(at, &log, &stable);
  ep.handle_message(
    at,
    &mut log,
    &mut stable,
    3u64,
    Message::RequestVote(RequestVote::new(
      Term::new(5),
      3u64,
      Index::new(9),
      Term::new(5),
      false,
      false,
    )),
  );
  assert_eq!(ep.term, Term::new(5), "the higher term was adopted");
  assert!(
    ep.durable.last_submitted_term > ep.durable.durable_term,
    "its write is submitted but not proven durable"
  );
  assert!(
    !log.has_pending() && !stable.has_pending(),
    "neither store has a completion to poll: both reconciliation gates are open"
  );
  assert!(
    ep.durable
      .inflight_append_upto
      .iter()
      .all(|(_, upto)| *upto > ep.durable.durable_index),
    "the in-flight append sits ABOVE the watermark, so the drain runs and finds nothing covered — \
     what this test pins is the WATERMARK no-op, not a skipped release"
  );

  let before = (
    ep.durable.durable_index,
    ep.durable.durable_term,
    ep.durable.durable_commit_index,
    ep.durable.durable_lease_support,
    ep.durable.durable_snapshot_index,
    ep.pending_len(),
  );
  ep.handle_storage(at, &mut log, &mut stable);
  assert_eq!(
    (
      ep.durable.durable_index,
      ep.durable.durable_term,
      ep.durable.durable_commit_index,
      ep.durable.durable_lease_support,
      ep.durable.durable_snapshot_index,
      ep.pending_len(),
    ),
    before,
    "a stale/older answer moves no watermark, and the drain finds nothing it covers"
  );
  assert!(
    !ep.term_is_durable(),
    "term 5 is NOT durable on term-1 evidence"
  );

  // Positive control: the real completions land, and the ordinary path advances what the probe would not.
  log.force_durable_index(None);
  log.flush_held_appends();
  for _ in 0..4 {
    if ep.handle_storage(at, &mut log, &mut stable).is_drained() {
      break;
    }
  }
  assert_eq!(
    ep.durable.durable_index,
    Index::new(2),
    "the held append's own completion advanced the watermark"
  );
}

/// (f) Both legs are gated on a QUIESCENT store, not merely on outstanding work: while a completion is
/// still queued the missing one is merely LATE, and releasing ahead of it would act before the store's
/// own accounting arrives. Here a per-queue drain budget leaves the store non-quiescent at the
/// reconciliation point, so neither leg may fire even though its probe answers.
///
/// MUTATION: drop the `log.has_pending()` / `stable.has_pending()` half of either gate → the first crank
/// releases, and the `MorePending` assertions below fail.
#[test]
fn the_legs_hold_while_the_store_still_has_a_queued_completion() {
  let budget = Endpoint::<u64, CountSm>::STORAGE_DRAIN_BUDGET;
  let mut log = SwallowLog::default();
  let mut stable = AsyncStable::default();
  let (mut ep, at) = elect_single_node_leader(&mut log, &mut stable);
  log.swallow_next_appended(1);
  let cmd = bytes::Bytes::from_static(b"x");
  ep.propose(at, &mut log, &stable, &cmd)
    .expect("the leader accepts the proposal");
  ep.flush_appends(at, &log, &stable);
  // Queued BEHIND the swallowed completion, and one longer than a single drain can take: the drain
  // swallows the append's completion, then exits on its budget with the queue still non-empty.
  log.queue_filler_completions(budget + 1);

  assert_eq!(
    ep.handle_storage(at, &mut log, &mut stable),
    StorageProgress::MorePending,
    "the budget cut the drain short: the store is not quiescent"
  );
  assert_eq!(
    ep.durable.durable_index,
    Index::new(1),
    "the leg must NOT fire ahead of a queued completion"
  );
  assert_eq!(ep.commit, Index::new(1), "so no ack releases early");
  assert!(!ep.pending_is_empty(), "the LeaderAppend is still parked");

  // Drain the remainder: the store is quiescent now, and the leg heals the loss.
  for _ in 0..4 {
    if ep.handle_storage(at, &mut log, &mut stable).is_drained() {
      break;
    }
  }
  assert_eq!(
    ep.durable.durable_index,
    Index::new(2),
    "once quiescent, the evidence stands in for the lost completion"
  );
  assert_eq!(ep.commit, Index::new(2), "and the gated commit advances");
}

/// (f, stable seam) The same gate on the hard-state leg: a candidate whose self-vote completion was
/// swallowed must NOT be released while the stable queue still holds completions.
///
/// MUTATION: drop the `stable.has_pending()` half of the gate → the node leads after the first crank.
#[test]
fn the_stable_leg_holds_while_the_store_still_has_a_queued_completion() {
  let budget = Endpoint::<u64, CountSm>::STORAGE_DRAIN_BUDGET;
  let cfg = Config::try_new(1u64, std::vec![1u64], ELECTION_TIMEOUT, HEARTBEAT).unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 42, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = SwallowStable::default();
  stable.swallow_next_wrote(1);
  let at = ep.poll_timeout().expect("a timer is armed");
  ep.handle_timeout(at, &mut log, &mut stable);
  assert!(ep.role().is_candidate(), "the timeout started a campaign");
  // Queued BEHIND the swallowed completion: the drain swallows the self-vote's completion (so the store
  // now answers the durable self-vote), then exits on its budget with the queue still non-empty.
  stable.queue_filler_completions(budget + 1);

  assert_eq!(
    ep.handle_storage(at, &mut log, &mut stable),
    StorageProgress::MorePending,
    "the budget cut the drain short: the store is not quiescent"
  );
  assert!(
    ep.role().is_candidate(),
    "the leg must NOT release the campaign ahead of a queued completion"
  );

  for _ in 0..4 {
    if ep.handle_storage(at, &mut log, &mut stable).is_drained() {
      break;
    }
  }
  assert!(
    ep.role().is_leader(),
    "once quiescent, the durable self-vote releases the campaign"
  );
}
