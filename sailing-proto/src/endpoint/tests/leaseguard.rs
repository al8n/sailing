use super::{super::*, *};
use core::time::Duration;

/// A LeaseGuard leader stamps every entry it appends with its append-time clock; a
/// non-LeaseGuard leader leaves the timestamp at 0 (so the field is absent on the wire).
#[test]
fn leaseguard_leader_stamps_appended_entries() {
  fn proposed_timestamp(read_only: crate::ReadOnlyOption) -> (u64, u64) {
    let mut cfg = Config::try_new(
      1u64,
      std::vec![1u64, 2, 3],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap()
    .with_read_only(read_only);
    if read_only == crate::ReadOnlyOption::LeaseGuard {
      cfg = cfg.with_lease_duration(Duration::from_millis(300));
    }
    let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
    let mut log = crate::testkit::VecLog::default();
    let mut stable = crate::testkit::NoopStable::default();

    // Elect (at the non-zero election deadline) so a stamp is distinguishable from 0.
    let now = ep.poll_timeout().unwrap();
    ep.handle_timeout(now, &mut log, &mut stable);
    ep.handle_storage(now, &mut log, &mut stable);
    ep.handle_message(
      now,
      &mut log,
      &mut stable,
      2u64,
      Message::VoteResp(crate::VoteResp::new(
        crate::Term::new(1),
        2u64,
        false,
        false,
      )),
    );
    assert!(ep.role().is_leader());
    ep.handle_storage(now, &mut log, &mut stable);

    let idx = ep
      .propose(now, &mut log, &stable, &bytes::Bytes::from_static(b"cmd"))
      .expect("leader accepts the proposal");
    // Find the proposed entry in the broadcast AppendEntries and read its timestamp.
    while let Some(out) = ep.poll_message() {
      if let Message::AppendEntries(ae) = out.message()
        && let Some(e) = ae.entries().iter().find(|e| e.index() == idx)
      {
        return (e.timestamp(), now.since_origin().as_nanos() as u64);
      }
    }
    panic!("the proposed entry was not broadcast");
  }

  let (lg_ts, expected) = proposed_timestamp(crate::ReadOnlyOption::LeaseGuard);
  assert!(expected > 0, "the election deadline must be non-zero");
  assert_eq!(
    lg_ts, expected,
    "a LeaseGuard leader stamps the entry with its append-time clock"
  );

  let (safe_ts, _) = proposed_timestamp(crate::ReadOnlyOption::Safe);
  assert_eq!(
    safe_ts, 0,
    "a non-LeaseGuard leader leaves the timestamp at 0"
  );
}

/// Under the FAILOVER tier (`bounded_clock_uncertainty` set), a LeaseGuard leader stamps every entry
/// it appends with the SYNCHRONIZED wall reading carried by `Now::synchronized`. Without the tier the
/// wall stamp stays 0 (absent on the wire) even when a wall is supplied.
#[test]
fn leaseguard_failover_leader_stamps_wall_timestamp() {
  fn proposed_wall(uncertainty: Option<Duration>) -> u64 {
    let mut cfg = Config::try_new(
      1u64,
      std::vec![1u64, 2, 3],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap()
    .with_read_only(crate::ReadOnlyOption::LeaseGuard)
    .with_lease_duration(Duration::from_millis(300))
    .with_clock_drift_bound(Duration::from_millis(50));
    if let Some(u) = uncertainty {
      cfg = cfg.with_bounded_clock_uncertainty(u);
    }
    let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
    let mut log = crate::testkit::VecLog::default();
    let mut stable = crate::testkit::NoopStable::default();

    // Supply the synchronized wall on EVERY call (the failover tier debug_asserts a present wall).
    let mono = ep.poll_timeout().unwrap();
    let now = crate::Now::synchronized(mono, crate::Wall::from_nanos(1_700_000_000_000_000_000));
    ep.handle_timeout(now, &mut log, &mut stable);
    ep.handle_storage(now, &mut log, &mut stable);
    ep.handle_message(
      now,
      &mut log,
      &mut stable,
      2u64,
      Message::VoteResp(crate::VoteResp::new(
        crate::Term::new(1),
        2u64,
        false,
        false,
      )),
    );
    assert!(ep.role().is_leader());
    ep.handle_storage(now, &mut log, &mut stable);

    let idx = ep
      .propose(now, &mut log, &stable, &bytes::Bytes::from_static(b"cmd"))
      .expect("leader accepts the proposal");
    while let Some(out) = ep.poll_message() {
      if let Message::AppendEntries(ae) = out.message()
        && let Some(e) = ae.entries().iter().find(|e| e.index() == idx)
      {
        return e.wall_timestamp();
      }
    }
    panic!("the proposed entry was not broadcast");
  }

  assert_eq!(
    proposed_wall(Some(Duration::from_millis(20))),
    1_700_000_000_000_000_000,
    "the failover tier stamps the entry with the synchronized wall"
  );
  assert_eq!(
    proposed_wall(None),
    0,
    "without the failover tier the wall stamp stays 0 (absent on the wire)"
  );
}

/// A FRESH LeaseGuard cluster's first leader has no inherited entries (`max_lease_window = 0`), so it
/// has no deposed lease to wait out — its no-op commits immediately on a quorum ack, exactly like
/// Safe. (The commit-wait engages only on a real failover; see
/// [`leaseguard_commit_wait_covers_inherited_max_window`].)
#[test]
fn leaseguard_fresh_cluster_has_no_commit_wait() {
  use crate::{AppendResp, Config, Index, Instant, Message, Term, VoteResp};
  use core::time::Duration;

  // Drive a fresh leader to the point where peer 2 has acked the no-op at index 1, returning
  // `(endpoint, election_instant)`. `read_only` selects the mode; LeaseGuard also sets Δ=300ms,
  // ε_drift=50ms (the exact window 300·350/250 = 420ms < the 1000ms election timeout, so valid).
  fn elected_with_quorum_ack(
    read_only: crate::ReadOnlyOption,
  ) -> (
    Endpoint<u64, crate::testkit::CountSm>,
    crate::testkit::VecLog,
    crate::testkit::NoopStable,
    Instant,
  ) {
    let mut cfg = Config::try_new(
      1u64,
      std::vec![1u64, 2, 3],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap()
    .with_read_only(read_only);
    if read_only == crate::ReadOnlyOption::LeaseGuard {
      cfg = cfg
        .with_lease_duration(Duration::from_millis(300))
        .with_clock_drift_bound(Duration::from_millis(50));
    }
    let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
    let mut log = crate::testkit::VecLog::default();
    let mut stable = crate::testkit::NoopStable::default();
    let d = ep.poll_timeout().unwrap();
    ep.handle_timeout(d, &mut log, &mut stable);
    // Self-vote durable first (gates the leader transition), then the peer vote yields a quorum and
    // the node becomes leader and submits its no-op, then a second drain makes the no-op durable
    // (advancing self match_index to 1).
    ep.handle_storage(d, &mut log, &mut stable);
    ep.handle_message(
      d,
      &mut log,
      &mut stable,
      2u64,
      Message::VoteResp(VoteResp::new(Term::new(1), 2u64, false, false)),
    );
    assert!(ep.role().is_leader());
    ep.handle_storage(d, &mut log, &mut stable);
    while ep.poll_message().is_some() {}
    while ep.poll_event().is_some() {}
    // peer 2 acks the no-op at index 1 → quorum (self match=1 + peer2 match=1).
    ep.handle_message(
      d,
      &mut log,
      &mut stable,
      2u64,
      Message::AppendResp(AppendResp::new(
        Term::new(1),
        2u64,
        false,
        Index::ZERO,
        Term::ZERO,
        Index::new(1),
      )),
    );
    (ep, log, stable, d)
  }

  // Both Safe AND a fresh LeaseGuard leader commit immediately: a fresh node inherited no windowed
  // entries, so max_lease_window = 0 and there is no deposed lease to wait out.
  let (safe, ..) = elected_with_quorum_ack(crate::ReadOnlyOption::Safe);
  assert_eq!(
    safe.commit_index(),
    Index::new(1),
    "a Safe leader commits its no-op as soon as a quorum acks"
  );
  let (lg, ..) = elected_with_quorum_ack(crate::ReadOnlyOption::LeaseGuard);
  assert_eq!(
    lg.commit_index(),
    Index::new(1),
    "a fresh LeaseGuard leader has max_lease_window=0, so it also commits immediately (no wait)"
  );
}

/// The LeaseGuard commit-wait covers the MAX inherited lease window (the self-describing cross-leader
/// safety). A node that inherits entries a deposed leader stamped — each carrying that leader's own
/// window — holds its first post-election commit until `now + max(inherited window)`, so any deposed
/// leader's read-lease has provably expired, even under heterogeneous per-node windows (the LARGER
/// of two inherited windows binds, regardless of entry order — no assumption about other configs).
#[test]
fn leaseguard_commit_wait_covers_inherited_max_window() {
  use crate::{
    AppendEntries, AppendResp, Config, Entry, EntryKind, Index, Instant, Message, Term, VoteResp,
  };
  use core::time::Duration;

  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_read_only(crate::ReadOnlyOption::LeaseGuard)
  .with_lease_duration(Duration::from_millis(300))
  .with_clock_drift_bound(Duration::from_millis(50));
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::NoopStable::default();

  // A deposed leader (node 2, term 5) replicated two entries to node 1. The LARGER window (350ms) is
  // on the EARLIER index, so the test pins "wait the MAX", not "wait the last entry's window".
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(5),
      2u64,
      Index::ZERO,
      Term::ZERO,
      std::vec![
        Entry::new(
          Term::new(5),
          Index::new(1),
          EntryKind::Normal,
          bytes::Bytes::from_static(b"a"),
        )
        .with_lease_window(350_000_000),
        Entry::new(
          Term::new(5),
          Index::new(2),
          EntryKind::Normal,
          bytes::Bytes::from_static(b"b"),
        )
        .with_lease_window(200_000_000),
      ],
      Index::ZERO,
    )),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  while ep.poll_message().is_some() {}

  // The deposed leader goes silent; node 1 times out, campaigns (term 6), wins, appends its no-op.
  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable);
  ep.handle_storage(d, &mut log, &mut stable);
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    3u64,
    Message::VoteResp(VoteResp::new(Term::new(6), 3u64, false, false)),
  );
  assert!(ep.role().is_leader());
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  // peer 3 acks the no-op at index 3 → quorum, but the commit-wait holds it (max window = 350ms).
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    3u64,
    Message::AppendResp(AppendResp::new(
      Term::new(6),
      3u64,
      false,
      Index::ZERO,
      Term::ZERO,
      Index::new(3),
    )),
  );
  assert_eq!(
    ep.commit_index(),
    Index::ZERO,
    "held by the commit-wait sized at the MAX inherited lease window"
  );

  // 1ns before now+350ms: still held — proves the wait is the 350ms MAX, not the 200ms or 0.
  ep.handle_timeout(
    d + Duration::from_nanos(350_000_000 - 1),
    &mut log,
    &mut stable,
  );
  assert_eq!(
    ep.commit_index(),
    Index::ZERO,
    "not released before now + max(inherited window)"
  );

  // At now+350ms: the commit-wait fires; the no-op and the inherited entries commit.
  ep.handle_timeout(d + Duration::from_millis(350), &mut log, &mut stable);
  assert_eq!(
    ep.commit_index(),
    Index::new(3),
    "released at now + max(inherited window) = 350ms"
  );
}

/// FAILOVER-tier PRECISE commit-anchor: a successor that inherited a deposed FAILOVER leader's
/// WALL-STAMPED entries lifts its post-election commit-wait as soon as the synchronized wall passes
/// each inherited entry's own `wall_timestamp + lease_window` by `2·ε_unc` — committing FAR sooner
/// than the shipped conservative anchor (which restarts the whole window at THIS election's `now`),
/// because the inherited lease was created well before the election. Pins the `+2·ε_unc` boundary and
/// the absent-wall fallback.
#[test]
fn leaseguard_failover_precise_anchor_lifts_commit_wait_early() {
  use crate::{
    AppendEntries, AppendResp, Config, Entry, EntryKind, Index, Instant, Message, Term, VoteResp,
    Wall,
  };
  use core::time::Duration;

  const S: u64 = 1_700_000_000_000_000_000; // the deposed leader's stamp on the inherited entry
  const W: u64 = 1_500_000_000; // its self-describing window (1500ms — still live at this election)
  const EPS: u64 = 20_000_000; // ε_unc = 20ms
  const DEADLINE: u64 = S + W; // inherited_release_deadline = max(s_e + W_e)
  const THRESHOLD: u64 = DEADLINE + 2 * EPS; // now_wall must EXCEED this to release
  const W_E: u64 = S + 1_000_000_000; // this node's synchronized wall at the election instant `d`

  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_read_only(crate::ReadOnlyOption::LeaseGuard)
  .with_lease_duration(Duration::from_millis(300))
  .with_clock_drift_bound(Duration::from_millis(50))
  .with_bounded_clock_uncertainty(Duration::from_nanos(EPS));
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::NoopStable::default();

  // A deposed FAILOVER leader (node 2, term 5) replicated ONE wall-stamped entry to node 1.
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(5),
      2u64,
      Index::ZERO,
      Term::ZERO,
      std::vec![
        Entry::new(
          Term::new(5),
          Index::new(1),
          EntryKind::Normal,
          bytes::Bytes::from_static(b"a"),
        )
        .with_lease_window(W)
        .with_wall_timestamp(S)
      ],
      Index::ZERO,
    )),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  while ep.poll_message().is_some() {}

  // node 1 campaigns (term 6) under a SYNCHRONIZED wall. `at(off)` = the synchronized clock at
  // `d + off`: mono `d + off`, wall `W_E + off` (the wall advances with mono).
  let d = ep.poll_timeout().unwrap();
  let at =
    |off: u64| crate::Now::synchronized(d + Duration::from_nanos(off), Wall::from_nanos(W_E + off));
  ep.handle_timeout(at(0), &mut log, &mut stable);
  ep.handle_storage(at(0), &mut log, &mut stable);
  ep.handle_message(
    at(0),
    &mut log,
    &mut stable,
    3u64,
    Message::VoteResp(VoteResp::new(Term::new(6), 3u64, false, false)),
  );
  assert!(ep.role().is_leader());
  ep.handle_storage(at(0), &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  // peer 3 acks the no-op at index 2 → quorum, but the commit-wait holds (at election wall
  // W_E = S+1000ms the inherited lease lives until S+1500ms).
  ep.handle_message(
    at(0),
    &mut log,
    &mut stable,
    3u64,
    Message::AppendResp(AppendResp::new(
      Term::new(6),
      3u64,
      false,
      Index::ZERO,
      Term::ZERO,
      Index::new(2),
    )),
  );
  assert_eq!(
    ep.commit_index(),
    Index::ZERO,
    "held at election: the inherited wall-stamped lease is still live"
  );
  // Absent-wall fallback: with NO synchronized wall the precise anchor never fires, even far past the
  // deadline — the shipped conservative anchor governs.
  assert!(
    !ep.precise_release_ready(crate::Now::monotonic(d + Duration::from_secs(10))),
    "an absent wall must not lift the commit-wait early"
  );

  // Drive `maybe_advance_commit` at a chosen wall by appending a fresh proposal and making it durable
  // (`on_log_appended` re-runs the commit gate). These post-election proposals never change the
  // CAPTURED `inherited_release_deadline`, so the precise anchor still keys off the inherited entry.
  // 1ns BEFORE the precise threshold: still held — pins the +2·ε_unc boundary.
  let before = (THRESHOLD - 1) - W_E;
  ep.propose(
    at(before),
    &mut log,
    &stable,
    &bytes::Bytes::from_static(b"x"),
  )
  .expect("a leader appends during the commit-wait");
  ep.handle_storage(at(before), &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  assert_eq!(
    ep.commit_index(),
    Index::ZERO,
    "not released until now_wall exceeds inherited_release_deadline + 2·ε_unc"
  );

  // 1ns PAST the precise threshold: the precise anchor lifts the wait — at d+~540ms, far before the
  // conservative d+1500ms, proving the inherited entry's own wall (not THIS election's now) drove it.
  let after = (THRESHOLD + 1) - W_E;
  assert!(
    after < W,
    "the precise release must beat the conservative anchor d + max_lease_window (else vacuous)"
  );
  ep.propose(
    at(after),
    &mut log,
    &stable,
    &bytes::Bytes::from_static(b"y"),
  )
  .expect("a leader appends during the commit-wait");
  ep.handle_storage(at(after), &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  assert_eq!(
    ep.commit_index(),
    Index::new(2),
    "the precise anchor commits the inherited entry + no-op once now_wall > deadline + 2·ε_unc"
  );
}

/// FAILOVER-tier PRECISE commit-anchor SAFETY: an inherited entry that is LEASE-bearing but
/// WALL-ABSENT (a fail-closed failover stamp) is NOT covered by the wall floor, so the precise anchor
/// must additionally hold until its conservative mono-frame fallback elapses — never skipping a
/// fail-closed lease, even once the synchronized wall has raced far past every WALLED entry's
/// deadline.
#[test]
fn leaseguard_failover_precise_anchor_waits_for_unwalled_failclosed_entry() {
  use crate::{
    AppendEntries, AppendResp, Config, Entry, EntryKind, Index, Instant, Message, Term, VoteResp,
    Wall,
  };
  use core::time::Duration;

  const S: u64 = 1_700_000_000_000_000_000;
  const W_WALLED: u64 = 500_000_000; // the walled entry's window (its wall deadline = S + 500ms)
  const W_UNWALLED: u64 = 1_500_000_000; // the fail-closed entry's window (mono fallback = d + 1500ms)
  const EPS: u64 = 20_000_000;
  const W_E: u64 = S + 1_000_000_000; // election wall — already PAST the walled deadline S + 540ms

  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_read_only(crate::ReadOnlyOption::LeaseGuard)
  .with_lease_duration(Duration::from_millis(300))
  .with_clock_drift_bound(Duration::from_millis(50))
  .with_bounded_clock_uncertainty(Duration::from_nanos(EPS));
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::NoopStable::default();

  // Inherit TWO entries from a deposed failover leader: a WALL-STAMPED one (short window) and a
  // fail-closed WALL-ABSENT one (longer window, wall_timestamp == 0).
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(5),
      2u64,
      Index::ZERO,
      Term::ZERO,
      std::vec![
        Entry::new(
          Term::new(5),
          Index::new(1),
          EntryKind::Normal,
          bytes::Bytes::from_static(b"a"),
        )
        .with_lease_window(W_WALLED)
        .with_wall_timestamp(S),
        Entry::new(
          Term::new(5),
          Index::new(2),
          EntryKind::Normal,
          bytes::Bytes::from_static(b"b"),
        )
        .with_lease_window(W_UNWALLED),
      ],
      Index::ZERO,
    )),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  while ep.poll_message().is_some() {}

  let d = ep.poll_timeout().unwrap();
  let at =
    |off: u64| crate::Now::synchronized(d + Duration::from_nanos(off), Wall::from_nanos(W_E + off));
  ep.handle_timeout(at(0), &mut log, &mut stable);
  ep.handle_storage(at(0), &mut log, &mut stable);
  ep.handle_message(
    at(0),
    &mut log,
    &mut stable,
    3u64,
    Message::VoteResp(VoteResp::new(Term::new(6), 3u64, false, false)),
  );
  assert!(ep.role().is_leader());
  ep.handle_storage(at(0), &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}
  ep.handle_message(
    at(0),
    &mut log,
    &mut stable,
    3u64,
    Message::AppendResp(AppendResp::new(
      Term::new(6),
      3u64,
      false,
      Index::ZERO,
      Term::ZERO,
      Index::new(3),
    )),
  );

  // At election the WALLED entry's wall deadline (S + 540ms) has already passed (wall W_E = S+1000ms),
  // yet the precise anchor must NOT fire — the fail-closed entry's mono fallback (d + 1500ms) governs.
  assert!(
    !ep.precise_release_ready(at(0)),
    "the fail-closed (wall-absent) inherited lease gates the precise release"
  );
  // Even with the wall raced 1400ms past election (S + 2400ms, far past the walled deadline), the
  // mono fallback still holds (d + 1400ms < d + 1500ms): the fail-closed lease is never skipped.
  assert!(
    !ep.precise_release_ready(at(1_400_000_000)),
    "the mono-frame fallback still holds the fail-closed lease before its conservative deadline"
  );
  // Once the mono fallback elapses (d + 1500ms) the precise anchor is satisfied.
  assert!(
    ep.precise_release_ready(at(1_500_000_000)),
    "both the wall floor and the unwalled mono fallback are satisfied at d + 1500ms"
  );
  // And the conservative CommitWait timer at d + 1500ms commits the inherited entries + the no-op.
  ep.handle_timeout(
    d + Duration::from_nanos(1_500_000_000),
    &mut log,
    &mut stable,
  );
  assert_eq!(
    ep.commit_index(),
    Index::new(3),
    "released at the fail-closed entry's conservative mono deadline (no early skip)"
  );
}

/// The LeaseGuard read gate: a leader whose most-recent committed entry is still within the lease
/// window serves a read IMMEDIATELY from the local commit (a `ReadState` with no heartbeat round);
/// once that entry ages past `lease_duration` the read degrades to the always-safe heartbeat round
/// (no immediate `ReadState` — it must await a quorum ack).
#[test]
fn leaseguard_read_serves_live_lease_then_degrades_when_stale() {
  use crate::{AppendResp, Config, Index, Instant, Message, Term, VoteResp};
  use core::time::Duration;

  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_read_only(crate::ReadOnlyOption::LeaseGuard)
  .with_lease_duration(Duration::from_millis(300))
  .with_clock_drift_bound(Duration::from_millis(50));
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::NoopStable::default();

  // Elect at the election deadline `d`; no-op lands at index 1.
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
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}
  // peer 2 acks the no-op (held by the commit-wait), then release the commit-wait at d+1000ms.
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendResp(AppendResp::new(
      Term::new(1),
      2u64,
      false,
      Index::ZERO,
      Term::ZERO,
      Index::new(1),
    )),
  );
  let t1 = d + Duration::from_millis(1000);
  ep.handle_timeout(t1, &mut log, &mut stable);
  assert_eq!(ep.commit_index(), Index::new(1));

  // Propose a FRESH entry at t1 (stamped t1) and commit it via a quorum ack — the lease anchor is
  // now this entry's timestamp, t1, NOT the aged no-op.
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}
  let idx2 = ep
    .propose(t1, &mut log, &stable, &bytes::Bytes::from_static(b"x"))
    .unwrap();
  ep.handle_storage(t1, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  ep.handle_message(
    t1,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendResp(AppendResp::new(
      Term::new(1),
      2u64,
      false,
      Index::ZERO,
      Term::ZERO,
      idx2,
    )),
  );
  assert_eq!(ep.commit_index(), idx2, "the fresh entry commits (no wait)");
  while ep.poll_event().is_some() {}

  // LIVE: a read at t1 (anchor t1 + Δ=300ms > t1) serves immediately — a ReadState with no round.
  ep.read_index(t1, &log, &stable, bytes::Bytes::from_static(b"r1"))
    .unwrap();
  let live: std::vec::Vec<_> = core::iter::from_fn(|| ep.poll_event()).collect();
  assert!(
    live
      .iter()
      .any(|e| matches!(e, crate::Event::ReadState(rs) if rs.index() == idx2)),
    "a live LeaseGuard lease serves the read immediately from the local commit"
  );

  // BOUNDARY: a read at EXACTLY ts + Δ (t1+300ms). The STRICT gate (`ts + Δ > now`) treats the lease
  // as already DEAD at its expiry instant, so the read degrades to the safe round — closing the
  // equal-timestamp race with a successor whose commit-wait releases at `now >= deadline`.
  ep.read_index(
    t1 + Duration::from_millis(300),
    &log,
    &stable,
    bytes::Bytes::from_static(b"rb"),
  )
  .unwrap();
  let at_boundary: std::vec::Vec<_> = core::iter::from_fn(|| ep.poll_event()).collect();
  assert!(
    !at_boundary
      .iter()
      .any(|e| matches!(e, crate::Event::ReadState(_))),
    "the lease is DEAD at exactly ts + lease_duration (strict gate), so the read degrades to Safe"
  );

  // STALE: a read at t1+400ms (anchor t1 + 300ms < t1+400ms) degrades to the safe heartbeat round —
  // no immediate ReadState (it now awaits a quorum HeartbeatResp).
  let stale_now = t1 + Duration::from_millis(400);
  ep.read_index(stale_now, &log, &stable, bytes::Bytes::from_static(b"r2"))
    .unwrap();
  let stale: std::vec::Vec<_> = core::iter::from_fn(|| ep.poll_event()).collect();
  assert!(
    !stale
      .iter()
      .any(|e| matches!(e, crate::Event::ReadState(_))),
    "a stale LeaseGuard lease degrades to the safe round (no immediate ReadState)"
  );
}

/// An INVALID LeaseGuard config (a missing required knob, or Δ+drift not below the election timeout)
/// must DEGRADE TO SAFE: no commit-wait is armed, so the first commit is NOT held, and reads take the
/// safe heartbeat round — never an uncoverable lease fast-path or a missing-knob coerced to zero.
#[test]
fn leaseguard_invalid_config_degrades_to_safe() {
  use crate::{AppendResp, Config, Index, Instant, Message, Term, VoteResp};
  use core::time::Duration;

  // LeaseGuard with lease_duration but NO clock_drift_bound: invalid (Config::validate would reject),
  // so the activation gate `leaseguard_lease_window` returns None and the mode is inert.
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_read_only(crate::ReadOnlyOption::LeaseGuard)
  .with_lease_duration(Duration::from_millis(300));
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::NoopStable::default();
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
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}
  // peer 2 acks the no-op at index 1. With an invalid config NO commit-wait is armed, so the no-op
  // commits IMMEDIATELY at `d` (exactly like Safe), not held to election_timeout.
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendResp(AppendResp::new(
      Term::new(1),
      2u64,
      false,
      Index::ZERO,
      Term::ZERO,
      Index::new(1),
    )),
  );
  assert_eq!(
    ep.commit_index(),
    Index::new(1),
    "an invalid LeaseGuard config arms no commit-wait — the no-op commits immediately like Safe"
  );

  // And a read degrades to the safe heartbeat round: no immediate ReadState (it awaits a quorum ack).
  while ep.poll_event().is_some() {}
  ep.read_index(d, &log, &stable, bytes::Bytes::from_static(b"r"))
    .unwrap();
  let evs: std::vec::Vec<_> = core::iter::from_fn(|| ep.poll_event()).collect();
  assert!(
    !evs.iter().any(|e| matches!(e, crate::Event::ReadState(_))),
    "an invalid LeaseGuard config serves reads via the safe round, not a lease fast-path"
  );
}

/// A successor whose OWN read mode is NOT active LeaseGuard (here `Safe`) must STILL defer its first
/// commit to cover a DEPOSED LeaseGuard leader's lease: the commit-wait keys on the inherited
/// `max_lease_window`, not on whether the successor serves LeaseGuard reads. Otherwise a node rolled
/// to Safe/LeaseBased could commit a new entry while the deposed leader still serves a stale read.
#[test]
fn leaseguard_inherited_window_defers_commit_even_for_a_safe_successor() {
  use crate::{
    AppendEntries, AppendResp, Config, Entry, EntryKind, Index, Instant, Message, Term, VoteResp,
  };
  use core::time::Duration;

  // Node 1 runs SAFE — it serves no lease reads and stamps no windows — but it inherits a windowed
  // entry (window 350ms) from a deposed LeaseGuard leader (node 2, term 5).
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  assert_eq!(cfg.read_only(), crate::ReadOnlyOption::Safe);
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::NoopStable::default();

  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(5),
      2u64,
      Index::ZERO,
      Term::ZERO,
      std::vec![
        Entry::new(
          Term::new(5),
          Index::new(1),
          EntryKind::Normal,
          bytes::Bytes::from_static(b"a"),
        )
        .with_lease_window(350_000_000),
      ],
      Index::ZERO,
    )),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  while ep.poll_message().is_some() {}

  // Node 1 times out, campaigns (term 6), wins, appends its no-op at index 2.
  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable);
  ep.handle_storage(d, &mut log, &mut stable);
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    3u64,
    Message::VoteResp(VoteResp::new(Term::new(6), 3u64, false, false)),
  );
  assert!(ep.role().is_leader());
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  // peer 3 acks the no-op at index 2 → quorum, but the commit-wait holds it despite Safe mode.
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    3u64,
    Message::AppendResp(AppendResp::new(
      Term::new(6),
      3u64,
      false,
      Index::ZERO,
      Term::ZERO,
      Index::new(2),
    )),
  );
  assert_eq!(
    ep.commit_index(),
    Index::ZERO,
    "a Safe successor must still defer commit while a deposed LeaseGuard lease may be live"
  );

  // Released at now + the inherited window (350ms) — proving the wait keyed on the inherited window,
  // not on the successor's own read mode.
  ep.handle_timeout(d + Duration::from_millis(350), &mut log, &mut stable);
  assert_eq!(
    ep.commit_index(),
    Index::new(2),
    "released at now + inherited max_lease_window, independent of the successor's read mode"
  );
}

/// A FATAL log-read fault during the restart `max_lease_window` scan must POISON, not recover with a
/// partial (under-sized) bound — otherwise a successor could under-wait and serve a stale read. The
/// restart recompute is exactly the path that makes the bound self-describing across a crash, so it
/// must fail-stop on a read fault like every other durable read.
#[test]
fn leaseguard_restart_scan_read_fault_poisons() {
  use crate::{Config, Entry, EntryKind, Index, Instant, Term};
  use core::time::Duration;

  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_read_only(crate::ReadOnlyOption::LeaseGuard)
  .with_lease_duration(Duration::from_millis(300))
  .with_clock_drift_bound(Duration::from_millis(50));

  let mut stable = crate::testkit::AsyncStable::default();
  stable.force_state(Term::new(2), None, Index::new(1));
  let mut log = crate::testkit::FailTermLog::default();
  log.force_append(&[Entry::new(
    Term::new(2),
    Index::new(1),
    EntryKind::Normal,
    bytes::Bytes::from_static(b"a"),
  )
  .with_lease_window(350_000_000)]);
  // Reading the entry during the LeaseGuard window scan fails (a fatal storage fault).
  log.fail_entries_at(Some(Index::new(1)));

  let ep = Endpoint::restart(
    cfg,
    Instant::ORIGIN,
    42,
    crate::testkit::CountSm::default(),
    1,
    &mut log,
    &mut stable,
  );
  assert!(
    ep.is_poisoned(),
    "a fatal log-read during the LeaseGuard window scan must poison"
  );
  assert_eq!(ep.poison_reason().map(|r| r.as_str()), Some("log_read"));
}

/// `on_install_snapshot` folds the snapshot's carried lease window into `max_lease_window` at
/// RECEIPT — before the destructive install is deferred to `install_snapshot_now`. Otherwise a
/// follower that times out and is elected while the blob fsync is still pending would size its
/// commit-wait from the stale max, missing a deposed lease on an entry the snapshot subsumes.
#[test]
fn leaseguard_pending_snapshot_folds_window_at_receipt() {
  use crate::{
    Config, Index, InstallSnapshot, Instant, Message, SnapshotMeta, Term, conf::ConfState,
  };
  use core::time::Duration;

  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_read_only(crate::ReadOnlyOption::LeaseGuard)
  .with_lease_duration(Duration::from_millis(300))
  .with_clock_drift_bound(Duration::from_millis(50));
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::AsyncStable::default();

  let meta = SnapshotMeta::new(
    Index::new(5),
    Term::new(1),
    ConfState::from_voters(std::vec![1u64, 2, 3]),
  )
  .with_max_lease_window(350_000_000);
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    2u64,
    Message::InstallSnapshot(InstallSnapshot::new(
      Term::new(1),
      2u64,
      meta,
      super::encode_snapshot(0),
    )),
  );
  // The destructive install is DEFERRED (no `handle_storage`, so the blob is not yet durable and
  // `install_snapshot_now` has NOT run), but the window is folded at receipt.
  assert_eq!(
    ep.max_lease_window, 350_000_000,
    "the snapshot's lease window is folded at receipt, before the deferred install completes"
  );
}

/// Defense-in-depth (schema drift): a DUPLICATE AppendEntries whose entry matches an already-present
/// one by index+term but carries a LARGER lease_window still folds that window into max_lease_window —
/// so a follower whose stored copy lost the field (mixed-version / field-stripped) is not left under-
/// sized at the runtime path. (Durable cross-restart survival is the fresh-cluster contract.)
#[test]
fn leaseguard_duplicate_append_folds_a_newly_visible_window() {
  use crate::{AppendEntries, Config, Entry, EntryKind, Index, Instant, Message, Term};
  use core::time::Duration;

  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_read_only(crate::ReadOnlyOption::LeaseGuard)
  .with_lease_duration(Duration::from_millis(300))
  .with_clock_drift_bound(Duration::from_millis(50));
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::NoopStable::default();

  let ae = |window: u64| {
    Message::AppendEntries(AppendEntries::new(
      Term::new(1),
      2u64,
      Index::ZERO,
      Term::ZERO,
      std::vec![
        Entry::new(
          Term::new(1),
          Index::new(1),
          EntryKind::Normal,
          bytes::Bytes::from_static(b"a"),
        )
        .with_lease_window(window)
      ],
      Index::ZERO,
    ))
  };
  // First copy carries a ZERO window (a field-stripped / pre-upgrade entry).
  ep.handle_message(Instant::ORIGIN, &mut log, &mut stable, 2u64, ae(0));
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  assert_eq!(ep.max_lease_window, 0, "stored copy had a zero window");
  // A DUPLICATE (same index+term) from a LeaseGuard-aware leader carries the real window.
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    2u64,
    ae(350_000_000),
  );
  assert_eq!(
    ep.max_lease_window, 350_000_000,
    "the duplicate's lease_window is folded even though the entry was already present"
  );
}

/// Defense-in-depth (schema drift): a DUPLICATE InstallSnapshot at the same boundary but carrying a
/// LARGER max_lease_window folds that bound BEFORE the duplicate-install guard returns — so a stale or
/// field-stripped local copy is not left under-sized at the runtime path.
#[test]
fn leaseguard_duplicate_snapshot_folds_a_newly_visible_window() {
  use crate::{
    Config, Index, InstallSnapshot, Instant, Message, SnapshotMeta, Term, conf::ConfState,
  };
  use core::time::Duration;

  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_read_only(crate::ReadOnlyOption::LeaseGuard)
  .with_lease_duration(Duration::from_millis(300))
  .with_clock_drift_bound(Duration::from_millis(50));
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::AsyncStable::default();

  let is = |window: u64| {
    Message::InstallSnapshot(InstallSnapshot::new(
      Term::new(1),
      2u64,
      SnapshotMeta::new(
        Index::new(5),
        Term::new(1),
        ConfState::from_voters(std::vec![1u64, 2, 3]),
      )
      .with_max_lease_window(window),
      super::encode_snapshot(0),
    ))
  };
  // First receipt carries a ZERO bound (stale / field-stripped); install is left pending.
  ep.handle_message(Instant::ORIGIN, &mut log, &mut stable, 2u64, is(0));
  assert_eq!(
    ep.max_lease_window, 0,
    "first snapshot carried a zero bound"
  );
  // A DUPLICATE at the same boundary carries the real bound — folded before the duplicate guard returns.
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    2u64,
    is(350_000_000),
  );
  assert_eq!(
    ep.max_lease_window, 350_000_000,
    "the duplicate snapshot's carried bound is folded even though the install is a duplicate"
  );
}

/// The read gate compares ages as Durations, NOT a lossy `u128 → u64` nanos cast: an entry stamped
/// near `u64::MAX` nanoseconds since ORIGIN must read STALE once real time crosses the boundary, not
/// wrap `now` to a small value and stay falsely live (which would let a deposed leader serve stale).
#[test]
fn leaseguard_read_gate_does_not_wrap_near_u64_max_nanos() {
  use crate::{AppendResp, Config, Index, Instant, Message, Term, VoteResp};
  use core::time::Duration;

  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_read_only(crate::ReadOnlyOption::LeaseGuard)
  .with_lease_duration(Duration::from_millis(300))
  .with_clock_drift_bound(Duration::from_millis(50));
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::NoopStable::default();

  // Elect at the (small) election deadline; the no-op commits immediately (fresh cluster, no wait).
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
  ep.handle_storage(d, &mut log, &mut stable);
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendResp(AppendResp::new(
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

  // Propose+commit a fresh entry stamped at ~u64::MAX nanos since ORIGIN.
  let t_huge = Instant::ORIGIN + Duration::from_nanos(u64::MAX - 100);
  let idx = ep
    .propose(t_huge, &mut log, &stable, &bytes::Bytes::from_static(b"x"))
    .unwrap();
  ep.handle_storage(t_huge, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  ep.handle_message(
    t_huge,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendResp(AppendResp::new(
      Term::new(1),
      2u64,
      false,
      Index::ZERO,
      Term::ZERO,
      idx,
    )),
  );
  assert_eq!(ep.commit_index(), idx);
  while ep.poll_event().is_some() {}

  // LIVE at t_huge (age 0 < Δ).
  ep.read_index(t_huge, &log, &stable, bytes::Bytes::from_static(b"r1"))
    .unwrap();
  let live: std::vec::Vec<_> = core::iter::from_fn(|| ep.poll_event()).collect();
  assert!(
    live.iter().any(|e| matches!(e, crate::Event::ReadState(_))),
    "a fresh near-u64::MAX-stamped lease serves immediately"
  );

  // STALE 1s later — `since_origin` now exceeds u64::MAX nanos. A u64 cast would WRAP `now` to a small
  // value and keep `ts + Δ > now` true (falsely live); the Duration age `now − ts = 1s > Δ` is stale.
  let t_past = t_huge + Duration::from_secs(1);
  ep.read_index(t_past, &log, &stable, bytes::Bytes::from_static(b"r2"))
    .unwrap();
  let stale: std::vec::Vec<_> = core::iter::from_fn(|| ep.poll_event()).collect();
  assert!(
    !stale
      .iter()
      .any(|e| matches!(e, crate::Event::ReadState(_))),
    "past the u64::MAX boundary the lease is STALE (no wrap to falsely-live), so the read degrades"
  );
}

/// Build a single-voter LeaseGuard leader (Δ=300ms, ε=50ms) whose become-leader no-op is committed, and
/// return `(endpoint, log, stable, election_instant)`. A 1-voter cluster self-quorums, so the no-op
/// commits without peer acks and the lease is fresh as of the election instant.
fn elected_leaseguard_single_voter() -> (
  Endpoint<u64, crate::testkit::CountSm>,
  crate::testkit::VecLog,
  crate::testkit::NoopStable<u64>,
  Instant,
) {
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_read_only(crate::ReadOnlyOption::LeaseGuard)
  .with_lease_duration(Duration::from_millis(300))
  .with_clock_drift_bound(Duration::from_millis(50));
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::NoopStable::default();

  let t0 = ep.poll_timeout().unwrap();
  ep.handle_timeout(t0, &mut log, &mut stable);
  // Flush the self-vote + the appended no-op to durable storage and self-commit (1-voter quorum).
  ep.handle_storage(t0, &mut log, &mut stable);
  ep.handle_storage(t0, &mut log, &mut stable);
  assert!(
    ep.role().is_leader(),
    "a single-voter cluster elects immediately"
  );
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}
  (ep, log, stable, t0)
}

/// A LeaseGuard read that finds the lease stale degrades to Safe AND records a refresh demand; the
/// leader's next timer tick appends ONE stamped no-op, re-stamping the committed lease so subsequent
/// reads serve fast again. This is the no-op refresh that fixes post-election "dead on arrival" and
/// read-only-workload staleness.
#[test]
fn leaseguard_stale_read_triggers_a_refresh_noop() {
  let (mut ep, mut log, stable, t0) = elected_leaseguard_single_voter();
  let last_after_election = log.last_index();

  // The committed no-op's lease expires after Δ=300ms; a read at +400ms is stale.
  let stale = t0 + Duration::from_millis(400);
  ep.read_index(stale, &log, &stable, bytes::Bytes::from_static(b"r1"))
    .expect("a fresh-context read is accepted");
  // (the stale read degraded to the Safe round and recorded the refresh demand)
  while ep.poll_event().is_some() {}
  assert_eq!(
    log.last_index(),
    last_after_election,
    "the read itself must NOT append anything — reads take an immutable log"
  );

  // The leader's next timer tick consumes the demand and appends ONE stamped refresh no-op.
  let mut stable = stable;
  ep.handle_timeout(stale, &mut log, &mut stable);
  assert!(
    log.last_index() > last_after_election,
    "a stale read must trigger a refresh no-op at the next leader tick"
  );
  let refreshed = log
    .entries(
      last_after_election.next()..log.last_index().next(),
      u64::MAX,
    )
    .unwrap();
  assert_eq!(refreshed.len(), 1, "exactly one refresh no-op is appended");
  assert_eq!(
    refreshed[0].timestamp(),
    u64::try_from(stale.since_origin().as_nanos()).unwrap(),
    "the refresh no-op is stamped at the refresh time, re-stamping the lease fresh"
  );
}

/// An IDLE LeaseGuard leader (no reads) appends NO refresh no-ops even as its lease goes stale — the
/// refresh is reactive (driven by read demand), so a read-free cluster pays zero write amplification.
#[test]
fn leaseguard_idle_leader_does_not_append_refresh_noops() {
  let (mut ep, mut log, mut stable, t0) = elected_leaseguard_single_voter();
  let last = log.last_index();
  // No reads. Fire many leader timer ticks well past the lease window.
  for k in 1..=10u64 {
    let t = t0 + Duration::from_millis(400 * k);
    ep.handle_timeout(t, &mut log, &mut stable);
  }
  assert_eq!(
    log.last_index(),
    last,
    "an idle LeaseGuard leader appends no refresh no-ops — the refresh is read-driven, not a timer"
  );
}

/// A stale LeaseGuard read DURING a leader transfer must NOT trigger a refresh no-op. Appending one
/// would advance `last_index` after `TimeoutNow` was sent, leaving the authorized transferee with a
/// now-stale log so it loses the forced election (especially in a small cluster) — the refresh mirrors
/// `propose`'s leader-transfer write freeze.
#[test]
fn leaseguard_no_refresh_during_leader_transfer() {
  use crate::{AppendResp, Config, Index, Instant, Message, Term, VoteResp};
  use core::time::Duration;

  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_read_only(crate::ReadOnlyOption::LeaseGuard)
  .with_lease_duration(Duration::from_millis(300))
  .with_clock_drift_bound(Duration::from_millis(50));
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::NoopStable::default();

  // Elect and commit the become-leader no-op with peer 2 caught up (so a transfer to 2 is immediate).
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
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendResp(AppendResp::new(
      Term::new(1),
      2u64,
      false,
      Index::ZERO,
      Term::ZERO,
      Index::new(1),
    )),
  );
  assert_eq!(ep.commit_index(), Index::new(1));
  let last = log.last_index();

  // Authorize a transfer to the caught-up peer 2 — this arms `lead_transferee` and sends TimeoutNow.
  ep.transfer_leader(d, &log, &stable, 2u64).unwrap();
  assert_eq!(
    ep.lead_transferee,
    Some(2u64),
    "the transfer arms lead_transferee"
  );

  // A stale LeaseGuard read during the transfer still records a refresh demand (the read itself is
  // safe — the successor's commit-wait covers this leader's lease).
  let stale = d + Duration::from_millis(400);
  ep.read_index(stale, &log, &stable, bytes::Bytes::from_static(b"r1"))
    .unwrap();
  while ep.poll_event().is_some() {}

  // The next timer tick must NOT append a refresh no-op while the transfer is in flight.
  ep.handle_timeout(stale, &mut log, &mut stable);
  assert_eq!(
    log.last_index(),
    last,
    "no refresh no-op may be appended during a leader transfer (it would strand the transferee)"
  );
}
