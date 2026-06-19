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

/// The proactive-refresh read-activity signal: a LeaseGuard read sets `read_since_anchor`; COMMITTING a
/// fresh current-term entry (the leader's own re-anchor) clears it — an un-committed append does NOT; and
/// a step-down clears it. This is the gate that keeps an idle leader (no reads) from proactively refreshing.
#[test]
fn read_since_anchor_set_on_read_cleared_on_commit_and_stepdown() {
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

  // Elect, then commit the election no-op so a current-term anchor exists (a fresh cluster has no
  // commit-wait, so the no-op commits on the first quorum ack).
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
  ep.handle_message(
    now,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendResp(crate::AppendResp::new(
      crate::Term::new(1),
      2u64,
      false,
      crate::Index::ZERO,
      crate::Term::ZERO,
      crate::Index::new(1),
    )),
  );
  while ep.poll_event().is_some() {}
  while ep.poll_message().is_some() {}

  // A fresh leader has no read activity yet — the signal starts unset.
  assert!(!ep.read_since_anchor());

  // A LeaseGuard read (served or degraded) sets it.
  let ctx = bytes::Bytes::from_static(b"r");
  ep.read_index(now, &log, &stable, ctx.clone())
    .expect("leader accepts the read");
  assert!(
    ep.read_since_anchor(),
    "a LeaseGuard read sets read_since_anchor"
  );

  // An un-committed leader append does NOT clear the signal — the lease re-anchors when that entry
  // COMMITS, not when it is appended (clearing at append would let a read in the append->commit window
  // survive into the new anchor; see read_in_refresh_inflight_window_does_not_cause_extra_refresh).
  let last = log.last_index();
  ep.append_leader_noop(crate::Now::monotonic(now), &mut log, last);
  assert!(
    ep.read_since_anchor(),
    "an un-committed append must NOT clear read_since_anchor"
  );
  // Commit the no-op (persist on the leader + a quorum ack) → the committed anchor advances and clears it.
  ep.handle_storage(now, &mut log, &mut stable);
  ep.handle_message(
    now,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendResp(crate::AppendResp::new(
      crate::Term::new(1),
      2u64,
      false,
      crate::Index::ZERO,
      crate::Term::ZERO,
      crate::Index::new(2),
    )),
  );
  while ep.poll_event().is_some() {}
  while ep.poll_message().is_some() {}
  assert!(
    !ep.read_since_anchor(),
    "committing the fresh current-term anchor clears read_since_anchor"
  );

  // Set it once more, then a step-down clears it.
  ep.read_index(now, &log, &stable, ctx)
    .expect("leader accepts the read");
  assert!(ep.read_since_anchor());
  ep.step_down_to_follower(crate::Now::monotonic(now));
  assert!(
    !ep.read_since_anchor(),
    "step-down clears read_since_anchor"
  );
}

/// `lease_near_expiry` (the `OnExpiry` trigger) fires once the anchor is within `margin =
/// 2·heartbeat_interval` of `Δ`. With Δ = 300ms and heartbeat = 100ms, margin = 200ms, so the threshold
/// is age `>= Δ - margin = 100ms`.
#[test]
fn lease_near_expiry_fires_within_margin_of_delta() {
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

  // Elect + commit the election no-op, stamped at `now` (the anchor `lease_near_expiry` reads).
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
  ep.handle_message(
    now,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendResp(crate::AppendResp::new(
      crate::Term::new(1),
      2u64,
      false,
      crate::Index::ZERO,
      crate::Term::ZERO,
      crate::Index::new(1),
    )),
  );
  while ep.poll_event().is_some() {}
  while ep.poll_message().is_some() {}

  let at = |ms: u64| crate::Now::monotonic(now + Duration::from_millis(ms));
  // Age below the 100ms threshold: not near expiry.
  assert!(!ep.lease_near_expiry(at(0), &log));
  assert!(!ep.lease_near_expiry(at(50), &log));
  assert!(!ep.lease_near_expiry(at(99), &log));
  // Age at/after the threshold (the `>=` boundary at 100ms): near expiry, including while still live.
  assert!(ep.lease_near_expiry(at(100), &log));
  assert!(ep.lease_near_expiry(at(150), &log));
  assert!(ep.lease_near_expiry(at(250), &log));
}

/// A `Continuous` leader STOPS appending once reads stop: a read sets `read_since_anchor`, the next
/// heartbeat spends ONE proactive no-op (clearing it), and with no further reads the log stabilizes — an
/// idle cluster converges. (Regression for the VOPR quiesce path: a leader that kept refreshing forever
/// would never let a healed cluster catch up.)
#[test]
fn continuous_refresh_drains_when_reads_stop() {
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_read_only(crate::ReadOnlyOption::LeaseGuard)
  .with_lease_duration(Duration::from_millis(300))
  .with_clock_drift_bound(Duration::from_millis(50))
  .with_lease_refresh(crate::LeaseRefresh::Continuous);
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::NoopStable::default();

  // Self-elect (single voter = self-quorum) and commit the election no-op.
  let t0 = ep.poll_timeout().unwrap();
  ep.handle_timeout(t0, &mut log, &mut stable);
  ep.handle_storage(t0, &mut log, &mut stable);
  assert!(ep.role().is_leader());
  ep.handle_storage(t0, &mut log, &mut stable);
  while ep.poll_event().is_some() {}
  while ep.poll_message().is_some() {}

  // One read sets the activity signal.
  ep.read_index(t0, &log, &stable, bytes::Bytes::from_static(b"r"))
    .expect("leader accepts the read");
  assert!(ep.read_since_anchor());

  // Drive many heartbeats with NO further reads. The first fires one proactive no-op; the rest must not
  // keep growing the log.
  let hb = Duration::from_millis(100);
  let mut t = t0;
  // First heartbeat: one proactive no-op fires and clears the signal.
  t = t + hb;
  ep.handle_timeout(t, &mut log, &mut stable);
  ep.handle_storage(t, &mut log, &mut stable);
  while ep.poll_event().is_some() {}
  while ep.poll_message().is_some() {}
  let after_first = log.last_index();
  // Many more heartbeats with no reads: the log must NOT keep growing.
  for _ in 0..50 {
    t = t + hb;
    ep.handle_timeout(t, &mut log, &mut stable);
    ep.handle_storage(t, &mut log, &mut stable);
    while ep.poll_event().is_some() {}
    while ep.poll_message().is_some() {}
  }
  assert_eq!(
    log.last_index(),
    after_first,
    "a Continuous leader must STOP appending once reads stop (read_since_anchor must drain)"
  );
}

/// A `Continuous` leader is HEARTBEAT-PACED: calling `handle_timeout` repeatedly BEFORE the heartbeat is
/// due appends nothing (the proactive block gates on the heartbeat deadline), and the refresh no-op fires
/// only at the due heartbeat. Guards against an embedder's fixed-tick loop driving caller-rate write
/// amplification past the advertised one-per-heartbeat bound.
#[test]
fn continuous_refresh_is_heartbeat_paced() {
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_read_only(crate::ReadOnlyOption::LeaseGuard)
  .with_lease_duration(Duration::from_millis(300))
  .with_clock_drift_bound(Duration::from_millis(50))
  .with_lease_refresh(crate::LeaseRefresh::Continuous);
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::NoopStable::default();

  // Self-elect (single voter) and commit the election no-op; the heartbeat deadline is now t0 + 100ms.
  let t0 = ep.poll_timeout().unwrap();
  ep.handle_timeout(t0, &mut log, &mut stable);
  ep.handle_storage(t0, &mut log, &mut stable);
  assert!(ep.role().is_leader());
  ep.handle_storage(t0, &mut log, &mut stable);
  while ep.poll_event().is_some() {}
  while ep.poll_message().is_some() {}

  // A read arms read_since_anchor.
  ep.read_index(t0, &log, &stable, bytes::Bytes::from_static(b"r"))
    .expect("leader accepts the read");
  assert!(ep.read_since_anchor());

  // Many handle_timeout calls at t0 — the heartbeat is NOT due (deadline t0 + 100ms), so NO refresh fires
  // even with read_since_anchor set: the rate is bounded by the heartbeat, not the caller's tick rate.
  let before = log.last_index();
  for _ in 0..20 {
    ep.handle_timeout(t0, &mut log, &mut stable);
    ep.handle_storage(t0, &mut log, &mut stable);
    while ep.poll_event().is_some() {}
    while ep.poll_message().is_some() {}
  }
  assert_eq!(
    log.last_index(),
    before,
    "Continuous must NOT refresh before the heartbeat is due (caller-rate ticking)"
  );

  // At the due heartbeat the refresh fires.
  ep.handle_timeout(t0 + Duration::from_millis(100), &mut log, &mut stable);
  ep.handle_storage(t0 + Duration::from_millis(100), &mut log, &mut stable);
  while ep.poll_event().is_some() {}
  while ep.poll_message().is_some() {}
  assert!(
    log.last_index() > before,
    "Continuous refreshes once the heartbeat is due"
  );
}

/// A LeaseGuard read arriving in the WINDOW between a refresh no-op's APPEND and its COMMIT must not
/// survive into the freshly-committed anchor and trigger an extra idle no-op after reads stop. The signal
/// is cleared at the anchor's COMMIT, so the in-window read is consumed and a quiesced leader emits no
/// further refreshes.
#[test]
fn read_in_refresh_inflight_window_does_not_cause_extra_refresh() {
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_read_only(crate::ReadOnlyOption::LeaseGuard)
  .with_lease_duration(Duration::from_millis(300))
  .with_clock_drift_bound(Duration::from_millis(50))
  .with_lease_refresh(crate::LeaseRefresh::Continuous);
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::NoopStable::default();

  // Self-elect (single voter) and commit the election no-op.
  let t0 = ep.poll_timeout().unwrap();
  ep.handle_timeout(t0, &mut log, &mut stable);
  ep.handle_storage(t0, &mut log, &mut stable);
  assert!(ep.role().is_leader());
  ep.handle_storage(t0, &mut log, &mut stable);
  while ep.poll_event().is_some() {}
  while ep.poll_message().is_some() {}

  // A read arms the signal against the committed anchor.
  ep.read_index(t0, &log, &stable, bytes::Bytes::from_static(b"r1"))
    .expect("leader accepts the read");
  assert!(ep.read_since_anchor());
  let anchor = log.last_index();

  // Due heartbeat → the proactive refresh APPENDS a no-op (still pending storage; commit has not moved).
  let hb = t0 + Duration::from_millis(100);
  ep.handle_timeout(hb, &mut log, &mut stable);
  assert_eq!(
    log.last_index(),
    anchor.next(),
    "the due heartbeat appended a refresh no-op"
  );
  assert!(
    ep.read_since_anchor(),
    "the un-committed append must NOT clear the signal"
  );

  // A read in the [append, commit) window re-arms the signal against the still-current OLD anchor.
  ep.read_index(hb, &log, &stable, bytes::Bytes::from_static(b"r2"))
    .expect("leader accepts the in-window read");
  assert!(ep.read_since_anchor());

  // Commit the refresh no-op → the anchor advances → the in-window read is consumed (signal clears).
  ep.handle_storage(hb, &mut log, &mut stable);
  while ep.poll_event().is_some() {}
  while ep.poll_message().is_some() {}
  assert!(
    !ep.read_since_anchor(),
    "committing the new anchor clears the in-window read"
  );

  // Reads stop. Drive several heartbeats (the lease even expires) — NO further no-op fires.
  let after = log.last_index();
  for k in 2..8u64 {
    let t = t0 + Duration::from_millis(100 * k);
    ep.handle_timeout(t, &mut log, &mut stable);
    ep.handle_storage(t, &mut log, &mut stable);
    while ep.poll_event().is_some() {}
    while ep.poll_message().is_some() {}
  }
  assert_eq!(
    log.last_index(),
    after,
    "an in-flight-window read must not survive the commit and cause an extra idle refresh"
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
  // R10: ε_unc ≥ Δ (here ε_unc = Δ = 300ms) is REJECTED by `Config::validate` and reports
  // `failover_tier_active() == false`. The wall stamp is gated on the SAME centralized predicate, so it
  // must stay 0 — otherwise a VALID successor would fold a validation-rejected config's `wall_timestamp`
  // into its `max_wall_plus_window` and trust it as an inherited-read / release horizon (a rejected
  // config seeding the failover tier). The basic-LeaseGuard `lease_window` is unaffected (it needs only a
  // valid window, not the failover tier) — only the failover WALL stamp degrades.
  assert_eq!(
    proposed_wall(Some(Duration::from_millis(300))),
    0,
    "a config with ε_unc ≥ Δ (rejected by validate, failover_tier_active false) must NOT stamp a wall"
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
  assert_eq!(
    ep.precise_releases(),
    0,
    "the precise anchor has not fired while the commit-wait is still held"
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
  assert_eq!(
    ep.precise_releases(),
    1,
    "the precise anchor fired exactly once — the early-release path lifted the commit-wait"
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
  // Once the mono fallback elapses (d + 1500ms) the precise anchor is satisfied (the unwalled fallback
  // `unwalled_commit_wait_until` is NOT inflated — only walled entries gate the inherited serve).
  assert!(
    ep.precise_release_ready(at(1_500_000_000)),
    "both the wall floor and the unwalled mono fallback are satisfied at d + 1500ms"
  );
  // The conservative CommitWait backstop fires once the mono fallback has elapsed; probed at d + 1750ms,
  // UNDER A SYNCHRONIZED WALL (this is an ε_unc failover-tier node — the Option B wall-gate fail-closes a
  // non-armed node's conservative clear on an ABSENT wall, so the contract is to supply the wall). The
  // wall (W_E + 1750ms) is far past every WALLED inherited deadline, so the walled class is released; the
  // unwalled mono fallback (d + 1500ms) has also elapsed, so the backstop commits the inherited entries +
  // the no-op.
  ep.handle_timeout(at(1_750_000_000), &mut log, &mut stable);
  assert_eq!(
    ep.commit_index(),
    Index::new(3),
    "released at the E′-inflated conservative mono deadline (no early skip of the fail-closed lease)"
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

/// FAILOVER inherited-read serve offer (`failover_read_window`): a freshly elected leader holding the
/// post-election commit-wait, whose committed anchor `log[c]` lease is still live on the synchronized
/// wall, offers `Some({ index: c, limbo_upper })` so the application can serve a linearizable read at
/// `c` (after its own per-key limbo check) instead of degrading to Safe. Pins the EXACT lease-live
/// boundary `now_wall + 2·ε_unc < committed_anchor_wall + Δ`, the captured `limbo_upper` (the election
/// tail, EXCLUDING the leader's own no-op), and the absent-wall fail-closed case.
#[test]
fn failover_read_window_offers_inherited_serve_while_lease_live() {
  use crate::{
    AppendEntries, Config, Entry, EntryKind, Index, Instant, Message, Term, VoteResp, Wall,
  };
  use core::time::Duration;

  const S: u64 = 1_700_000_000_000_000_000; // the inherited COMMITTED entry's wall stamp
  const W: u64 = 420_000_000; // its self-describing lease window W_c (the serve HORIZON)
  const EPS: u64 = 20_000_000; // ε_unc = 20ms
  // The serve gate keys on the ENTRY's own window W_c, NOT the config Δ (300ms below) — `now_wall +
  // 2·ε_unc < S + W_c`, i.e. the election-relative offset `off < W_c − 2·ε_unc`. Δ is set distinct from W
  // in the config precisely to prove the gate ignores it (260ms would be the boundary if it used Δ).
  const DELTA: u64 = 300_000_000; // Δ = 300ms (config lease_duration; NOT the serve horizon)
  const LIVE_LIMIT: u64 = W - 2 * EPS; // 380ms — Some strictly below, None at/above

  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_read_only(crate::ReadOnlyOption::LeaseGuard)
  .with_lease_duration(Duration::from_nanos(DELTA))
  .with_clock_drift_bound(Duration::from_millis(50))
  .with_bounded_clock_uncertainty(Duration::from_nanos(EPS));
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::NoopStable::default();

  // The COMMITTED entry is applied, so its payload must be a valid `F::Command` encoding (`Bytes` is
  // length-prefixed) — a raw byte string would poison with NormalEntryDecode on apply.
  let cmd = {
    let mut buf = std::vec::Vec::new();
    <bytes::Bytes as crate::Data>::encode(&bytes::Bytes::from_static(b"a"), &mut buf);
    bytes::Bytes::from(buf)
  };

  // A deposed FAILOVER leader (node 2, term 5) replicated ONE wall-stamped entry to node 1 AND committed
  // it (leader_commit = 1), so node 1's commit index is 1 at election.
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
        Entry::new(Term::new(5), Index::new(1), EntryKind::Normal, cmd)
          .with_lease_window(W)
          .with_wall_timestamp(S)
      ],
      Index::new(1), // leader_commit = 1 → the inherited entry is COMMITTED
    )),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  assert_eq!(
    ep.commit_index(),
    Index::new(1),
    "the inherited entry committed at index 1"
  );

  // node 1 campaigns (term 6) under a SYNCHRONIZED wall whose election instant is S (the lease just
  // started — fully alive). `at(off)` = mono `d + off`, wall `S + off`.
  let d = ep.poll_timeout().unwrap();
  let at =
    |off: u64| crate::Now::synchronized(d + Duration::from_nanos(off), Wall::from_nanos(S + off));
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
  assert_eq!(
    ep.commit_index(),
    Index::new(1),
    "commit held at the inherited index during the wait (the no-op at index 2 is uncommitted)"
  );

  // While the lease is live, the window offers an inherited serve at index 1 with `limbo_upper = 1` (the
  // election tail — the leader's own no-op at index 2 is EXCLUDED, captured before it was appended).
  let w = ep
    .failover_read_window(at(0))
    .expect("an inherited serve is offered while the committed anchor's lease is live");
  assert_eq!(w.index(), Index::new(1), "serve at the committed index c");
  assert_eq!(
    w.limbo_upper(),
    Index::new(1),
    "limbo_upper is the election tail, excluding the leader's own no-op"
  );

  // Exact lease-live boundary: Some strictly below `S + W_c − 2·ε_unc` (= the entry's window, NOT Δ),
  // None at/above (the STRICT gate, the dual of the precise release's strict `>`). LIVE_LIMIT = W − 2·ε =
  // 380ms; had the gate used Δ it would withdraw at 260ms, so a Some here also proves it ignores Δ.
  assert!(
    ep.failover_read_window(at(LIVE_LIMIT - 1)).is_some(),
    "1ns below the W_c lease-live threshold still offers the serve"
  );
  assert!(
    ep.failover_read_window(at(LIVE_LIMIT)).is_none(),
    "at the W_c lease-live threshold the offer is withdrawn (strict gate)"
  );
  // Cross-check the horizon is W_c, not Δ: at S + Δ − 2·ε_unc (the Δ-boundary, 260ms) the serve is STILL
  // offered — it would be withdrawn there if the gate used the successor's config Δ.
  assert!(
    ep.failover_read_window(at(DELTA - 2 * EPS)).is_some(),
    "the gate must key on the entry's window W_c, not the config Δ (still live past the Δ-boundary)"
  );

  // Fail closed with NO synchronized wall (a monotonic-only reading): the offer is withdrawn.
  assert!(
    ep.failover_read_window(crate::Now::monotonic(d)).is_none(),
    "an absent synchronized wall must withdraw the inherited-read offer"
  );

  // Fail closed once this node is POISONED (it has declared itself untrustworthy): no serve window even
  // while the role, commit-wait, and anchors are all still intact and the lease is live.
  ep.poison(crate::PoisonReason::LogRead);
  assert!(
    ep.failover_read_window(at(0)).is_none(),
    "a poisoned node must never advertise an inherited-read serve window"
  );
}

/// FAILOVER inherited-read FAIL-CLOSED: with no committed entry at election (here the inherited entry is
/// uncommitted, so `commit == 0` and `committed_anchor_wall == 0`), the serve gate refuses regardless of
/// the wall — the proto never serves past an unknown anchor; the read degrades to Safe. This is also the
/// compacted-`log[c]` case (`commit < first_index`), which the capture maps to the same `0`.
#[test]
fn failover_read_window_fail_closed_without_committed_anchor() {
  use crate::{
    AppendEntries, Config, Entry, EntryKind, Index, Instant, Message, Term, VoteResp, Wall,
  };
  use core::time::Duration;

  const S: u64 = 1_700_000_000_000_000_000;
  const W: u64 = 420_000_000;
  const EPS: u64 = 20_000_000;

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

  // Inherit a wall-stamped entry but leave it UNCOMMITTED (leader_commit = 0).
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
      Index::ZERO, // leader_commit = 0 → uncommitted, so commit stays 0 at election
    )),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  assert_eq!(ep.commit_index(), Index::ZERO);

  let d = ep.poll_timeout().unwrap();
  let at =
    |off: u64| crate::Now::synchronized(d + Duration::from_nanos(off), Wall::from_nanos(S + off));
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

  // committed_anchor_wall is 0 (no committed entry at election), so the gate fails closed even though a
  // synchronized wall is present and well within any lease window.
  assert!(
    ep.failover_read_window(at(0)).is_none(),
    "with no committed anchor the inherited-read offer is withheld (fail-closed)"
  );
}

/// FAILOVER R2 REGRESSION (E′, in its post-Option-B role) — the conservative MONO commit-wait is INFLATED
/// on the failover tier so it can NOT fire before a walled inherited entry's wall window `s_c + W_c`
/// (which the inherited-read serve duals). The bare `become_leader_mono + max_lease_window` covers only
/// the deposed leader's read-LEASE (`< W_c` in real time under drift); a fast successor could reach it in
/// wall-time BEFORE the serve withdraws and commit past `c` — a stale read. E′ inflates the deadline by
/// `(Δ+ε_drift)/Δ` so the mono timer lands no earlier than the wall window.
///
/// Since Option B (the conservative mono clear is WALL-GATED for the walled class when a wall is present),
/// E′'s job narrowed; R19 narrowed it further: the inflated wait may only SKIP the veto when a synchronized
/// wall AT ELECTION proves it reaches the floor (`wall_proves_floor`: now_wall + max_lease_window ≥
/// max_wall_plus_window + 2·ε_unc). Here max_wall_plus_window = S + W and max_lease_window = W, so the proof
/// needs the election wall ≥ S + 2·ε_unc — i.e. this election is 2·ε_unc AFTER the inherited stamp S (a
/// realistic past-stamp). Once proven at election, the post-election ticks are driven MONOTONIC-only to
/// ISOLATE the inflated deadline: the timer must hold past the OLD `d + W` and release only at the inflated
/// `d + ceil(W·(Δ+ε_drift)/Δ)`. (Absent the proof the node fails closed via the veto, never on E′ alone.)
#[test]
fn failover_conservative_commit_wait_is_inflated_against_the_mono_undercut() {
  use crate::{
    AppendEntries, AppendResp, Config, Entry, EntryKind, Index, Instant, Message, Term, VoteResp,
    Wall,
  };
  use core::time::Duration;

  const S: u64 = 1_700_000_000_000_000_000; // the inherited COMMITTED entry's wall stamp
  // Use a NON-DIVISIBLE inflation: 100ms · (350/300) = 116_666_666.67ns, so the deadline must CEIL to
  // 116_666_667 — a truncating `/` would round to 116_666_666 and (the serve gate being strict and
  // nanosecond-grained) re-open the R2 boundary by 1ns.
  const W: u64 = 100_000_000; // lease_window = max_lease_window = 100ms
  const TRUNC: u64 = 116_666_666; // floor(100ms · 350/300) — the WRONG (truncated) deadline
  const INFLATED: u64 = 116_666_667; // ceil(100ms · 350/300) — the correct E′ deadline
  const EPS: u64 = 20_000_000; // ε_unc

  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000), // election_timeout — the inflated 700ms must stay below it
    Duration::from_millis(100),
  )
  .unwrap()
  .with_read_only(crate::ReadOnlyOption::LeaseGuard)
  .with_lease_duration(Duration::from_millis(300)) // Δ
  .with_clock_drift_bound(Duration::from_millis(50)) // ε_drift → inflation (300+50)/300 = 7/6
  .with_bounded_clock_uncertainty(Duration::from_nanos(EPS));
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::NoopStable::default();

  // Inherit a wall-stamped entry and COMMIT it (leader_commit = 1).
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
          EntryKind::Empty,
          bytes::Bytes::new()
        )
        .with_lease_window(W)
        .with_wall_timestamp(S)
      ],
      Index::new(1),
    )),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  while ep.poll_message().is_some() {}

  // node 1 wins term 6. The ELECTION runs under a synchronized wall (`at_wall`) read at S + 2·ε_unc — far
  // enough past the inherited stamp S that `wall_proves_floor` holds ((S + 2·ε_unc) + W ≥ (S + W) + 2·ε_unc),
  // so this node is E′-INFLATED this term (commit_wait_inflated). The commit-wait TIMER is then driven
  // MONOTONIC-only (`at`, wall absent): an inflated node skips the veto, so the inflated mono deadline alone
  // governs the release — isolating the ceil-rounded E′ deadline we are pinning. (Absent the election proof
  // the node would fail closed via the veto, never release on E′ alone — see the no-ε_unc regression.)
  let d = ep.poll_timeout().unwrap();
  let at_wall = |off: u64| {
    crate::Now::synchronized(
      d + Duration::from_nanos(off),
      Wall::from_nanos(S + 2 * EPS + off),
    )
  };
  let at = |off: u64| crate::Now::monotonic(d + Duration::from_nanos(off));
  ep.handle_timeout(at_wall(0), &mut log, &mut stable);
  ep.handle_storage(at_wall(0), &mut log, &mut stable);
  ep.handle_message(
    at_wall(0),
    &mut log,
    &mut stable,
    3u64,
    Message::VoteResp(VoteResp::new(Term::new(6), 3u64, false, false)),
  );
  assert!(ep.role().is_leader());
  ep.handle_storage(at_wall(0), &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}
  // peer 3 acks the no-op at index 2 → quorum, so commit CAN advance once the wait lifts.
  ep.handle_message(
    at_wall(0),
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
    Index::new(1),
    "commit held at the inherited index during the wait"
  );

  // At the OLD (un-inflated) deadline d + W the conservative timer must NOT yet fire — the wall is absent
  // (wall-gate + precise path both inert), so the commit-wait is still held by the E′ inflation alone.
  ep.handle_timeout(at(W), &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  assert_eq!(
    ep.commit_index(),
    Index::new(1),
    "the conservative timer must NOT fire at the un-inflated d + W (E′ holds it for the wall window)"
  );

  // CEIL boundary: at the TRUNCATED deadline (floor(W · 350/300) = d + 116_666_666ns) the timer must
  // STILL NOT fire — a truncating `/` would have released here, 1ns short of the wall window, re-opening
  // R2. The ceil holds it one nanosecond longer.
  ep.handle_timeout(at(TRUNC), &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  assert_eq!(
    ep.commit_index(),
    Index::new(1),
    "the ceil-rounded deadline holds 1ns past the truncated value (a floor `/` would under-wait here)"
  );

  // At the CEIL-inflated deadline d + ceil(W · (Δ+ε_drift)/Δ) the conservative timer fires and commits.
  ep.handle_timeout(at(INFLATED), &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  assert_eq!(
    ep.commit_index(),
    Index::new(2),
    "the conservative timer fires at the ceil-rounded E′ deadline, committing the inherited entry + no-op"
  );
}

/// FAILOVER R19 regression (the core finding — E′ must NOT under-cover a FUTURE-stamped walled floor): an
/// ε_unc successor with valid lease timing inherits a walled committed entry whose wall stamp `s_c` is in
/// the FUTURE relative to this election (crafted/corrupt — `max_wall_plus_window = s_c + W_c` is a SUM a
/// future `s_c` inflates while `max_lease_window = W_c` stays small). Before R19 the node, being E′-eligible,
/// would set `commit_wait_inflated`, SKIP the wall veto, and clear at the small E′ mono deadline
/// (~W_c·(1+ρ) ≈ 117ms) — committing past `c` ~half a second before the future floor `s_c + W_c`,
/// undercutting a peer's inherited serve on it. R19 gates the veto-skip on `wall_proves_floor` (now_wall +
/// max_lease_window ≥ max_wall_plus_window + 2·ε_unc), which a future `s_c` fails — so the node is NOT
/// inflated, and the wall-gate veto HOLDS the commit-wait until the wall actually passes `s_c + W_c + 2·ε_unc`.
#[test]
fn failover_future_stamp_floor_held_not_e_prime_undercut() {
  use crate::{
    AppendEntries, AppendResp, Config, Entry, EntryKind, Index, Instant, Message, Term, VoteResp,
    Wall,
  };
  use core::time::Duration;

  const T: u64 = 1_700_000_000_000_000_000; // the election wall
  const X: u64 = 500_000_000; // the inherited stamp is 500ms in THIS election's FUTURE (crafted)
  const W: u64 = 100_000_000; // lease_window = max_lease_window = 100ms ⇒ small E′ ≈ 117ms
  const EPS: u64 = 20_000_000; // ε_unc; 2·ε_unc = 40ms
  const S_FUTURE: u64 = T + X; // ⇒ max_wall_plus_window = S_FUTURE + W = T + 600ms
  const FLOOR: u64 = X + W + 2 * EPS; // s_c + W_c + 2·ε_unc, election-relative = 640ms (the release floor)
  const PAST_E_PRIME: u64 = 120_000_000; // 120ms: past the small E′ (~117ms), ≪ the 640ms floor
  const NEAR_FLOOR: u64 = FLOOR - 20_000_000; // 620ms: still BELOW the floor — must stay held
  const RELEASED: u64 = FLOOR + 160_000_000; // 800ms: a mono-due tick with the wall well PAST the floor

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

  // Inherit + commit a walled entry whose wall stamp is in the election's FUTURE.
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
          EntryKind::Empty,
          bytes::Bytes::new()
        )
        .with_lease_window(W)
        .with_wall_timestamp(S_FUTURE)
      ],
      Index::new(1),
    )),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  while ep.poll_message().is_some() {}

  // node 1 wins term 6 under election wall T (so s_c = T + X is 500ms in the future).
  let d = ep.poll_timeout().unwrap();
  let at =
    |off: u64| crate::Now::synchronized(d + Duration::from_nanos(off), Wall::from_nanos(T + off));
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
  assert!(
    !ep.is_poisoned(),
    "a FUTURE stamp is passable (≪ u64::MAX) — the node must HOLD via the veto, not poison"
  );
  ep.handle_storage(at(0), &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}
  // peer 3 acks the no-op at index 2 → quorum, so commit CAN advance once the wait lifts.
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
  assert_eq!(ep.commit_index(), Index::new(1));

  // Past the SMALL E′ deadline (~117ms) but with the wall ≪ the 640ms floor: the commit must NOT clear. The
  // node is NOT inflated (`wall_proves_floor` fails for a future floor), so the wall-gate veto holds. Before
  // R19 the E′ skip would have cleared here — the undercut.
  ep.handle_timeout(at(PAST_E_PRIME), &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  assert_eq!(
    ep.commit_index(),
    Index::new(1),
    "an ε_unc node must HOLD past the small E′ deadline when the floor is a FUTURE wall stamp (no undercut)"
  );

  // Still below the floor (620ms < 640ms): STILL held — the wall has not yet passed s_c + W_c + 2·ε_unc.
  ep.handle_timeout(at(NEAR_FLOOR), &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  assert_eq!(
    ep.commit_index(),
    Index::new(1),
    "held right up to the floor — the wall-gate releases only on a STRICT pass of s_c + W_c + 2·ε_unc"
  );

  // The wall is now well PAST the floor (800ms > 640ms) at a mono-due tick: the wall-gate releases and the
  // commit advances — wall-governed, ~680ms after E′ would have (wrongly) cleared it. No undercut.
  ep.handle_timeout(at(RELEASED), &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  assert_eq!(
    ep.commit_index(),
    Index::new(2),
    "the commit-wait releases only once the wall passes the future floor s_c + W_c + 2·ε_unc"
  );
}

/// FAILOVER R19 SERVE-SIDE regression (Gap-G2 — a FUTURE committed anchor must not over-serve): the serve
/// gate reads the committed anchor `(s_c, W_c)` VERBATIM from `log[commit]` and offers while `now_wall +
/// 2·ε_unc < s_c + W_c`. A crafted/corrupt entry with a FUTURE `s_c` (the serve-side mirror of the release
/// hole) would keep the offer open until the wall reaches the future `s_c + W_c` — long past the real lease.
/// R19 bounds the anchor at election: trusted only when `s_c ≤ now_wall + ε_unc` (not stamped in this
/// election's future) AND `s_c + W_c ≤ max_wall_plus_window`; otherwise it is dropped to `(0, 0)` and the
/// serve REFUSES (the read degrades to Safe; the release side still holds commit via the veto). Here `s_c`
/// is 500ms in the election's future, so the offer that the otherwise-identical live-lease test grants is
/// WITHDRAWN. (Contrast `failover_read_window_offers_inherited_serve_while_lease_live`, where `s_c == the
/// election wall`, which serves.)
#[test]
fn failover_future_committed_anchor_refuses_serve() {
  use crate::{
    AppendEntries, Config, Entry, EntryKind, Index, Instant, Message, Term, VoteResp, Wall,
  };
  use core::time::Duration;

  const T: u64 = 1_700_000_000_000_000_000; // the election wall
  const X: u64 = 500_000_000; // the inherited stamp is 500ms in the election's FUTURE (crafted)
  const W: u64 = 420_000_000; // the entry's window W_c — would offer a long serve if trusted
  const EPS: u64 = 20_000_000; // ε_unc
  const S_FUTURE: u64 = T + X; // crafted future committed-anchor stamp

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

  // The committed entry is applied, so its payload must be a valid length-prefixed `Bytes` encoding.
  let cmd = {
    let mut buf = std::vec::Vec::new();
    <bytes::Bytes as crate::Data>::encode(&bytes::Bytes::from_static(b"a"), &mut buf);
    bytes::Bytes::from(buf)
  };
  // Inherit + commit a walled entry whose wall stamp is in the election's FUTURE.
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
        Entry::new(Term::new(5), Index::new(1), EntryKind::Normal, cmd)
          .with_lease_window(W)
          .with_wall_timestamp(S_FUTURE)
      ],
      Index::new(1),
    )),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  assert_eq!(ep.commit_index(), Index::new(1));

  // node 1 wins term 6 under election wall T (so s_c = T + X is 500ms in the future).
  let d = ep.poll_timeout().unwrap();
  let at =
    |off: u64| crate::Now::synchronized(d + Duration::from_nanos(off), Wall::from_nanos(T + off));
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
  assert!(
    !ep.is_poisoned(),
    "a passable future floor holds via the veto, not poison"
  );
  ep.handle_storage(at(0), &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}
  assert_eq!(
    ep.commit_index(),
    Index::new(1),
    "commit held during the wait — the serve is ARMED this term"
  );

  // The committed anchor was stamped in this election's future (s_c = T + 500ms ≫ now_wall + ε_unc), so R19
  // DROPPED it to (0, 0): the serve REFUSES. Without the drop `now_wall + 2·ε_unc < s_c + W_c` would hold
  // (the live-lease test grants exactly this for a non-future stamp), opening a stale over-serve.
  assert!(
    ep.failover_read_window(at(0)).is_none(),
    "a future committed anchor must be dropped — the inherited serve is refused (no over-serve)"
  );
  // Still refused deep into where the future anchor would have (wrongly) kept it live (well within W_c).
  assert!(
    ep.failover_read_window(at(W - 2 * EPS - 1)).is_none(),
    "the dropped anchor keeps the serve refused across the whole would-be-live window"
  );
}

/// FAILOVER Option B REGRESSION (the cross-successor undercut) — a NON-armed successor that carries a
/// bounded clock uncertainty and inherits a WALL-stamped committed entry must hold its commit-wait until
/// the entry's WALL floor `s_c + W_c + 2·ε_unc`, NOT release at the bare `become_leader_mono +
/// max_lease_window` mono deadline. On a non-armed node that bare deadline is UN-inflated (no E′), so
/// under rate drift it could fire — in wall time — before the floor, letting this successor commit past
/// `c` while an ARMED peer serves an inherited read at `c` (design threats T2/T4: a stale read). The
/// wall-gate keys on `inherited_release_deadline` (folded ungated by read mode), so it fences this
/// non-armed node too. A Safe successor carrying ε_unc is the canonical "misconfig that must still be
/// safe": its conservative bare deadline is `d + W`, but the commit must hold to the wall floor.
#[test]
fn failover_non_armed_successor_holds_commit_to_the_wall_floor() {
  use crate::{
    AppendEntries, AppendResp, Config, Entry, EntryKind, Index, Instant, Message, Term, VoteResp,
    Wall,
  };
  use core::time::Duration;

  const S: u64 = 1_700_000_000_000_000_000; // the inherited COMMITTED entry's wall stamp
  const W: u64 = 100_000_000; // inherited lease_window = max_lease_window = 100ms (the BARE mono wait)
  const EPS: u64 = 20_000_000; // ε_unc = 20ms → wall floor = S + W + 2·ε_unc = S + 140ms
  const FLOOR: u64 = W + 2 * EPS; // 140ms — the wall offset at which `walled_wall_released` flips

  // SAFE mode (read_only != LeaseGuard ⇒ leaseguard_timing() == None ⇒ failover_tier_active() == false ⇒
  // NON-armed: become_leader uses the BARE max_lease_window wait, not the E′ inflation) BUT carrying a
  // bounded clock uncertainty so it CAN evaluate the wall floor — the misconfig that must still be safe.
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_bounded_clock_uncertainty(Duration::from_nanos(EPS));
  assert_eq!(cfg.read_only(), crate::ReadOnlyOption::Safe);
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::NoopStable::default();

  // Inherit a WALL-stamped committed entry (leader_commit = 1) from a deposed failover leader.
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
          EntryKind::Empty,
          bytes::Bytes::new()
        )
        .with_lease_window(W)
        .with_wall_timestamp(S)
      ],
      Index::new(1),
    )),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  while ep.poll_message().is_some() {}

  // node 1 wins term 6 under a synchronized wall tracking real time from S (`at(off)` = mono d+off,
  // wall S+off).
  let d = ep.poll_timeout().unwrap();
  let at =
    |off: u64| crate::Now::synchronized(d + Duration::from_nanos(off), Wall::from_nanos(S + off));
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
  // NOTE: peer 3 has NOT yet acked the no-op at index 2 — so the FIRST ack later (at FLOOR + 1) ADVANCES
  // the match index, which is what re-enters `maybe_advance_commit` (a redundant ack would not). Until
  // then only this leader holds index 2, so commit stays at the inherited index 1.

  // The BARE mono deadline is d + W (100ms). Fire the CommitWait timer there: WITHOUT the wall-gate the
  // commit-wait would clear here (the un-inflated bare deadline). WITH it, the wall (S + 100ms) is still
  // below the floor (S + 140ms), so the clear is VETOED and the timer re-arms — commit holds. This ALSO
  // exercises the §8 wedge tripwire: handle_timeout must complete without panic and leave a
  // strictly-future serviceable deadline.
  ep.handle_timeout(at(W), &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  assert_eq!(
    ep.commit_index(),
    Index::new(1),
    "the bare mono deadline must NOT clear a still-live WALLED inherited lease (Option B wall-gate)"
  );
  assert!(
    ep.poll_timeout().is_some_and(|t| t > at(W).mono()),
    "on a wall-veto the CommitWait timer is re-armed strictly-future (no wedge)"
  );

  // 1ns PAST the wall floor (wall = S + 140ms + 1) peer 3 acks index 2 for the FIRST time — advancing the
  // match index re-enters `maybe_advance_commit`, where the precise path (wall now past the floor) clears
  // the wait and the quorum commits index 2. The release is governed by the WALL floor, not the bare mono
  // deadline that fired 40ms earlier.
  ep.handle_message(
    at(FLOOR + 1),
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
  while ep.poll_message().is_some() {}
  assert_eq!(
    ep.commit_index(),
    Index::new(2),
    "past the wall floor the commit-wait releases (the wall, not the bare mono deadline, governs)"
  );
}

/// FAILOVER Option B — the wall-veto RE-POLL is wedge-safe and eventually releases via the CONSERVATIVE
/// path. When the bare mono deadline is repeatedly due while the wall is still below the floor, each
/// `handle_timeout` must re-arm a strictly-future CommitWait deadline (never tripping the §8 wedge
/// tripwire) rather than clear; once the wall passes the floor the conservative clear (now un-vetoed)
/// commits. Exercises the conservative-after-wall path (distinct from the precise path the boundary test
/// drives) and the repeated re-arm across several ticks.
#[test]
fn failover_wall_veto_repoll_is_wedge_safe_and_releases() {
  use crate::{
    AppendEntries, AppendResp, Config, Entry, EntryKind, Index, Instant, Message, Term, VoteResp,
    Wall,
  };
  use core::time::Duration;

  const S: u64 = 1_700_000_000_000_000_000;
  const W: u64 = 100_000_000; // bare mono wait = 100ms; heartbeat is also 100ms (the re-poll quantum)
  const EPS: u64 = 20_000_000; // floor = S + 140ms
  const HB: u64 = 100_000_000; // heartbeat_interval

  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_nanos(HB),
  )
  .unwrap()
  .with_bounded_clock_uncertainty(Duration::from_nanos(EPS));
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
          EntryKind::Empty,
          bytes::Bytes::new()
        )
        .with_lease_window(W)
        .with_wall_timestamp(S)
      ],
      Index::new(1),
    )),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  while ep.poll_message().is_some() {}

  let d = ep.poll_timeout().unwrap();
  let at =
    |off: u64| crate::Now::synchronized(d + Duration::from_nanos(off), Wall::from_nanos(S + off));
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
      Index::new(2),
    )),
  );
  assert_eq!(ep.commit_index(), Index::new(1));

  // Drive the CommitWait timer repeatedly while the wall is below the floor: the bare deadline d + W, then
  // each re-armed d + W + k·HB. Every fire must veto-and-re-arm (no panic, no commit) — the wall (S + off)
  // stays below the floor S + 140ms only at off = 100ms; at off = 200ms the wall (S + 200ms) is past the
  // floor, so the conservative clear (now un-vetoed) commits. (handle_timeout's debug wedge tripwire would
  // panic if any due CommitWait timer were left un-cleared and un-re-armed.)
  ep.handle_timeout(at(W), &mut log, &mut stable); // off=100ms: wall below floor → veto + re-arm to d+200ms
  while ep.poll_message().is_some() {}
  assert_eq!(
    ep.commit_index(),
    Index::new(1),
    "held at the bare deadline (wall below floor): vetoed and re-armed, not wedged"
  );

  ep.handle_timeout(at(W + HB), &mut log, &mut stable); // off=200ms: wall past floor → conservative commits
  while ep.poll_message().is_some() {}
  assert_eq!(
    ep.commit_index(),
    Index::new(2),
    "once the wall passes the floor, the re-armed conservative deadline clears and commits"
  );
}

/// FAILOVER totality regression (R23 — the wall-veto RE-ARM must not saturate near `Instant::MAX`): the
/// re-poll re-arms `now.mono() + heartbeat`, and `Instant::add` SATURATES. A node whose monotonic clock is
/// within one heartbeat of `Instant::MAX`, still vetoing a walled inherited lease, would re-arm to a CLAMPED
/// `Instant::MAX` — a deadline DUE forever (the §8 serviceable-timer tripwire trips in debug, and a release
/// driver busy-loops, never able to advance the clock past `Instant::MAX` to clear it). The fix re-arms only
/// a REPRESENTABLE strictly-future deadline and otherwise FAIL-STOPS with `CommitWaitUnrepresentable`; a
/// poisoned node holds, never undercutting the walled lease. Needs NO forged metadata — a normal walled
/// inherited entry plus an extreme-but-representable monotonic `Now` (in scope for CFT: a faithful clock).
#[test]
fn failover_wall_veto_repoll_near_instant_max_fails_stop() {
  use crate::{
    AppendEntries, AppendResp, Config, Entry, EntryKind, Index, Instant, Message, Term, VoteResp,
    Wall,
  };
  use core::time::Duration;

  const S: u64 = 1_700_000_000_000_000_000;
  const W: u64 = 100_000_000; // bare mono wait = 100ms
  const EPS: u64 = 20_000_000; // floor = S + 140ms
  const HB: u64 = 100_000_000; // heartbeat — the re-poll quantum that would saturate near MAX

  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_nanos(HB),
  )
  .unwrap()
  .with_bounded_clock_uncertainty(Duration::from_nanos(EPS));
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
          EntryKind::Empty,
          bytes::Bytes::new()
        )
        .with_lease_window(W)
        .with_wall_timestamp(S)
      ],
      Index::new(1),
    )),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  while ep.poll_message().is_some() {}

  // Elect under a live wall (lease just started at S, so the node is NOT inflated → the veto governs).
  let d = ep.poll_timeout().unwrap();
  let at =
    |off: u64| crate::Now::synchronized(d + Duration::from_nanos(off), Wall::from_nanos(S + off));
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
      Index::new(2),
    )),
  );
  assert_eq!(ep.commit_index(), Index::new(1));

  // Drive the CommitWait timer at a monotonic instant within one heartbeat of `Instant::MAX`, with the wall
  // STILL below the floor (S < S + 140ms) so the walled lease vetoes the clear. The re-arm `now.mono() + HB`
  // would saturate to `Instant::MAX` (`MAX − 50ms + 100ms`); the fix FAIL-STOPS instead. `handle_timeout`
  // must NOT panic (no saturated, perpetually-due CommitWait timer left behind).
  let near_max = crate::Now::synchronized(
    Instant::from_origin(Duration::MAX - Duration::from_millis(50)),
    Wall::from_nanos(S),
  );
  ep.handle_timeout(near_max, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  assert!(
    ep.is_poisoned(),
    "a wall-veto re-arm that would saturate near Instant::MAX must FAIL-STOP, not store a due-forever deadline"
  );
  assert_eq!(
    ep.poison_reason(),
    Some(crate::PoisonReason::CommitWaitUnrepresentable)
  );
  assert_eq!(
    ep.commit_index(),
    Index::new(1),
    "a poisoned node must not advance commit past the inherited index"
  );
}

/// FAILOVER observability regression (the silent-wedge counter the architecture review surfaced): a failover
/// (ε_unc) node arms its commit-wait under a wall, then is driven with a WALL-ABSENT `Now` on the release
/// path — a driver that armed the tier but did not supply a wall to `handle_timeout`. The veto holds the
/// commit-wait FAIL-CLOSED (R11) — safe (it never undercuts the walled lease) but otherwise SILENT and, for
/// a persistently wall-absent driver, permanent. `unprovable_floor_holds` counts each such hold so the wedge
/// is observable (and so the VOPR can assert the failover commit-wait does not silently wedge). A
/// wall-PRESENT, not-yet-released hold is NORMAL and is NOT counted.
#[test]
fn failover_unprovable_floor_hold_is_counted() {
  use crate::{
    AppendEntries, AppendResp, Config, Entry, EntryKind, Index, Instant, Message, Term, VoteResp,
    Wall,
  };
  use core::time::Duration;

  const S: u64 = 1_700_000_000_000_000_000;
  const W: u64 = 100_000_000; // bare mono wait = 100ms
  const EPS: u64 = 20_000_000; // floor = S + 140ms
  const HB: u64 = 100_000_000; // heartbeat — the re-poll quantum

  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_nanos(HB),
  )
  .unwrap()
  .with_bounded_clock_uncertainty(Duration::from_nanos(EPS));
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
          EntryKind::Empty,
          bytes::Bytes::new()
        )
        .with_lease_window(W)
        .with_wall_timestamp(S)
      ],
      Index::new(1),
    )),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  while ep.poll_message().is_some() {}

  // Elect under a synchronized wall (the no-op stamps; the node is NOT inflated → the veto governs).
  let d = ep.poll_timeout().unwrap();
  let at =
    |off: u64| crate::Now::synchronized(d + Duration::from_nanos(off), Wall::from_nanos(S + off));
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
      Index::new(2),
    )),
  );
  assert_eq!(ep.commit_index(), Index::new(1));
  assert_eq!(
    ep.unprovable_floor_holds(),
    0,
    "a wall-present election holds nothing unprovably"
  );

  // The driver now drops the wall on the release path: drive WALL-ABSENT due ticks. Each holds fail-closed
  // (R11) and is COUNTED; commit stays pinned (the walled lease is never undercut).
  let mono = |off: u64| crate::Now::monotonic(d + Duration::from_nanos(off));
  for k in 1..=3u64 {
    ep.handle_timeout(mono(W + (k - 1) * HB), &mut log, &mut stable);
    while ep.poll_message().is_some() {}
    assert_eq!(
      ep.commit_index(),
      Index::new(1),
      "wedge-safe: a wall-absent hold never undercuts the walled lease"
    );
    assert_eq!(
      ep.unprovable_floor_holds(),
      k,
      "each wall-absent commit-wait hold is counted — the otherwise-silent wedge is now observable"
    );
  }
}

/// FAILOVER R11 regression (the absent-wall fail-OPEN): a NON-armed Safe+ε_unc successor driven with a
/// MONOTONIC-only `Now` (no synchronized wall) at its bare deadline must FAIL CLOSED — hold the
/// commit-wait — NOT clear via the bare mono path. Without the wall-absent veto, `walled_lease_vetoes_
/// conservative` returned false on an absent wall, so this node would clear at the bare `max_lease_window`
/// (un-inflated, under drift) and commit past `c` while an ARMED peer serves an inherited read at `c` — a
/// stale read (R11). The fix: with ε_unc set, a walled inherited lease, and a passable horizon, an absent
/// wall vetoes for a NON-armed node (no E′ to make the bare mono safe). It releases only once a
/// synchronized wall proves the floor expired. (An armed node keeps its E′ mono fallback — tested
/// separately — so this is non-armed-specific.)
#[test]
fn failover_non_armed_successor_fails_closed_on_absent_wall() {
  use crate::{
    AppendEntries, AppendResp, Config, Entry, EntryKind, Index, Instant, Message, Term, VoteResp,
    Wall,
  };
  use core::time::Duration;

  const S: u64 = 1_700_000_000_000_000_000; // the inherited committed entry's wall stamp
  const W: u64 = 100_000_000; // inherited lease_window = max_lease_window = 100ms (the bare mono wait)
  const EPS: u64 = 20_000_000; // ε_unc = 20ms → wall floor = S + W + 2·ε_unc = S + 140ms

  // SAFE + ε_unc: NON-armed (read_only != LeaseGuard ⇒ failover_tier_active false ⇒ bare wait, no serve),
  // ε_unc-bearing (so the wall-gate applies). A Safe node does NOT stamp wall timestamps, so it can be
  // legitimately driven MONOTONIC — the exact case the absent-wall fail-open exposed.
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_bounded_clock_uncertainty(Duration::from_nanos(EPS));
  assert_eq!(cfg.read_only(), crate::ReadOnlyOption::Safe);
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::NoopStable::default();

  // Inherit a WALL-stamped committed entry (leader_commit = 1) from a deposed failover leader.
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
          EntryKind::Empty,
          bytes::Bytes::new()
        )
        .with_lease_window(W)
        .with_wall_timestamp(S)
      ],
      Index::new(1),
    )),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  while ep.poll_message().is_some() {}

  // node 1 wins term 6 driven MONOTONIC-only (no synchronized wall — a Safe node stamps no wall, so no
  // fail-closed assert). peer 3 has NOT yet acked index 2, so the later ack ADVANCES and re-enters
  // maybe_advance_commit.
  let d = ep.poll_timeout().unwrap();
  let mono = |off: u64| crate::Now::monotonic(d + Duration::from_nanos(off));
  ep.handle_timeout(mono(0), &mut log, &mut stable);
  ep.handle_storage(mono(0), &mut log, &mut stable);
  ep.handle_message(
    mono(0),
    &mut log,
    &mut stable,
    3u64,
    Message::VoteResp(VoteResp::new(Term::new(6), 3u64, false, false)),
  );
  assert!(ep.role().is_leader());
  ep.handle_storage(mono(0), &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  // At the bare mono deadline d + W, with NO wall to prove the inherited walled lease expired, the
  // conservative clear must be VETOED (fail closed) — commit holds. WITHOUT the absent-wall veto this
  // would clear and (with a quorum) commit past c, undercutting an armed peer's serve. Also exercises the
  // §8 wedge tripwire: handle_timeout must not panic and must leave a strictly-future serviceable timer.
  ep.handle_timeout(mono(W), &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  assert_eq!(
    ep.commit_index(),
    Index::new(1),
    "an absent wall must FAIL CLOSED for a non-armed node — the bare mono deadline must NOT clear the lease"
  );
  assert!(
    ep.poll_timeout().is_some_and(|t| t > mono(W).mono()),
    "the vetoed commit-wait re-arms strictly-future (no wedge) even with no wall"
  );

  // Once a SYNCHRONIZED wall past the floor (S + 140ms) is supplied, peer 3's first ack advances index 2
  // and the precise path (wall past floor) releases — the lease is honored, then the commit lands.
  ep.handle_message(
    crate::Now::synchronized(
      d + Duration::from_nanos(W + 2 * EPS + 1),
      Wall::from_nanos(S + W + 2 * EPS + 1),
    ),
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
  while ep.poll_message().is_some() {}
  assert_eq!(
    ep.commit_index(),
    Index::new(2),
    "once a wall proves the floor expired, the held commit-wait releases (wall-governed, not undercut)"
  );
}

/// FAILOVER R19 regression (no-ε_unc LeaseGuard successor — the R12 revert): a LeaseGuard successor that
/// LACKS `bounded_clock_uncertainty` (so it has no synchronized wall to read) but has valid lease timing
/// (Δ, ε_drift) inherits a WALL-stamped committed entry. R12 let it ride the E′-inflated mono wait, claiming
/// E′ covers the wall floor `s_c + W_c` WITHOUT a wall. R19 refuted that: E′ is a window-ONLY mono duration,
/// blind to the wall offset `s_c`, so a crafted/corrupt FUTURE `s_c` outruns it (`max_wall_plus_window` is a
/// `wall + window` SUM). A node with no wall therefore cannot PROVE the floor (`wall_proves_floor` requires
/// ε_unc AND a present wall) and must FAIL CLOSED — it HOLDS the commit-wait indefinitely rather than clear
/// on the unproven E′ deadline, matching the Safe successor (`walled_lease_vetoes_conservative`'s no-ε_unc
/// branch). Reachable only in a transient mixed-ε_unc rollout; recoverable by giving the node ε_unc.
/// Safety over the liveness of a node outside the synchronized-clock contract.
#[test]
fn failover_no_eps_unc_leaseguard_successor_fails_closed() {
  use crate::{
    AppendEntries, AppendResp, Config, Entry, EntryKind, Index, Instant, Message, Term, VoteResp,
  };
  use core::time::Duration;

  const S: u64 = 1_700_000_000_000_000_000;
  const W: u64 = 100_000_000; // inherited lease_window = max_lease_window = 100ms (the bare wait)
  const E_PRIME: u64 = 116_666_667; // ceil(100ms · (300+50)/300) — the E′ deadline

  // LeaseGuard with valid Δ/ε_drift but NO bounded_clock_uncertainty: non-failover-tier (no serve, no
  // wall-gate), yet E′ is computable from the lease timing alone.
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
  assert!(cfg.bounded_clock_uncertainty().is_none());
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::NoopStable::default();

  // Inherit a WALL-stamped committed entry from a deposed failover leader (this node didn't stamp it).
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
          EntryKind::Empty,
          bytes::Bytes::new()
        )
        .with_lease_window(W)
        .with_wall_timestamp(S)
      ],
      Index::new(1),
    )),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  while ep.poll_message().is_some() {}

  // Drive MONOTONIC — this node has no ε_unc and stamps no wall, so a monotonic clock is its normal input.
  let d = ep.poll_timeout().unwrap();
  let mono = |off: u64| crate::Now::monotonic(d + Duration::from_nanos(off));
  ep.handle_timeout(mono(0), &mut log, &mut stable);
  ep.handle_storage(mono(0), &mut log, &mut stable);
  ep.handle_message(
    mono(0),
    &mut log,
    &mut stable,
    3u64,
    Message::VoteResp(VoteResp::new(Term::new(6), 3u64, false, false)),
  );
  assert!(ep.role().is_leader());
  ep.handle_storage(mono(0), &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}
  ep.handle_message(
    mono(0),
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
  assert_eq!(ep.commit_index(), Index::new(1));

  // At the BARE deadline d + W the commit must NOT clear — this node cannot prove the inherited wall floor
  // (no ε_unc, no wall), so it is NOT E′-inflated and the veto's no-ε_unc branch fails closed.
  ep.handle_timeout(mono(W), &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  assert_eq!(
    ep.commit_index(),
    Index::new(1),
    "a no-ε_unc successor cannot prove the floor — it must HOLD, not clear at the bare deadline"
  );
  // It must STILL hold past the (now-defunct) E′ deadline: E′ alone cannot bound an absolute wall floor
  // against a crafted future stamp (R19), and there is no wall to ever release it. The R12 "release at E′
  // without a wall" behavior is RETRACTED — fail-closed is the only sound outcome.
  ep.handle_timeout(mono(E_PRIME), &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  assert_eq!(
    ep.commit_index(),
    Index::new(1),
    "no-ε_unc + no wall = fail-closed: the commit-wait holds past E′ (no unsound release)"
  );
  // Far past E′ it stays held — fail-closed indefinitely until the node is given ε_unc and a wall.
  ep.handle_timeout(mono(10 * E_PRIME), &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  assert_eq!(
    ep.commit_index(),
    Index::new(1),
    "still held far past E′ — a node outside the clock contract never clears the walled floor unaided"
  );
  // The no-ε_unc walled-inheritor hold is the OTHER unprovable-floor entry point (L1 of the architecture
  // review) — it too is counted, so this otherwise-silent wedge is observable.
  assert!(
    ep.unprovable_floor_holds() >= 1,
    "a no-ε_unc walled-inheritor's fail-closed hold is counted (the L1 silent-wedge entry point)"
  );
}

/// FAILOVER R12 regression (no-ε_unc successor, the Safe case): a Safe/LeaseBased successor that inherited
/// a WALLED failover entry but has NEITHER `bounded_clock_uncertainty` (cannot wall-gate) NOR lease timing
/// (cannot E′-inflate — no Δ) genuinely cannot bound the inherited lease in real time. It must FAIL CLOSED
/// — hold its commit-wait rather than clear via the bare mono wait and undercut an armed peer's serve.
/// This wedges a Safe-mode voter that inherited failover entries (a deep misconfiguration: the failover
/// serve assumes a bounded clock cluster-wide); the library holds it safe rather than silently corrupt.
#[test]
fn failover_no_eps_unc_safe_successor_fails_closed() {
  use crate::{
    AppendEntries, AppendResp, Config, Entry, EntryKind, Index, Instant, Message, Term, VoteResp,
  };
  use core::time::Duration;

  const S: u64 = 1_700_000_000_000_000_000;
  const W: u64 = 100_000_000;

  // SAFE, no ε_unc, no lease timing — the default config.
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  assert_eq!(cfg.read_only(), crate::ReadOnlyOption::Safe);
  assert!(cfg.bounded_clock_uncertainty().is_none());
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::NoopStable::default();

  // Inherit a WALL-stamped committed entry from a deposed failover leader.
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
          EntryKind::Empty,
          bytes::Bytes::new()
        )
        .with_lease_window(W)
        .with_wall_timestamp(S)
      ],
      Index::new(1),
    )),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  while ep.poll_message().is_some() {}

  let d = ep.poll_timeout().unwrap();
  let mono = |off: u64| crate::Now::monotonic(d + Duration::from_nanos(off));
  ep.handle_timeout(mono(0), &mut log, &mut stable);
  ep.handle_storage(mono(0), &mut log, &mut stable);
  ep.handle_message(
    mono(0),
    &mut log,
    &mut stable,
    3u64,
    Message::VoteResp(VoteResp::new(Term::new(6), 3u64, false, false)),
  );
  assert!(ep.role().is_leader());
  ep.handle_storage(mono(0), &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}
  ep.handle_message(
    mono(0),
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
  assert_eq!(ep.commit_index(), Index::new(1));

  // It can neither wall-gate (no ε_unc) nor E′-inflate (no Δ), so it FAILS CLOSED — the commit-wait holds
  // at the bare deadline and far beyond it (no bare-mono clear that could undercut a peer's serve). It
  // never panics the §8 wedge tripwire (the held timer is always re-armed strictly-future).
  for off in [W, W + 1_000_000_000, W + 10_000_000_000] {
    ep.handle_timeout(mono(off), &mut log, &mut stable);
    while ep.poll_message().is_some() {}
    assert_eq!(
      ep.commit_index(),
      Index::new(1),
      "a Safe no-ε_unc successor that inherited failover entries FAILS CLOSED — never clears via bare mono"
    );
    assert!(
      ep.poll_timeout().is_some_and(|t| t > mono(off).mono()),
      "the held commit-wait re-arms strictly-future (no §8 wedge tripwire panic)"
    );
  }
}

/// FAILOVER R13 regression (E′ totality — timing-invalid successor): a TIMING-INVALID LeaseGuard config
/// (Δ so large the window `Δ·(Δ+ε)/(Δ−ε)` overflows `u64`, so `leaseguard_timing()` is `None`) must NOT
/// produce a usable E′ wait. The prior E′ helper read the RAW `lease_duration`/`clock_drift_bound` and
/// used SATURATING arithmetic, so a stale/huge Δ with a large inherited `max_lease_window` clamped the
/// product to a PLAUSIBLE-but-too-short `u64`, wrongly set `commit_wait_inflated`, and let the node skip
/// the fail-closed veto and clear at a bare-equivalent deadline — undercutting an armed peer's serve. The
/// fix gates the E′ helper on the VALIDATED `leaseguard_timing()` (and uses checked arithmetic), so a
/// timing-invalid node returns `None` ⇒ not inflated ⇒ (no ε_unc here) FAIL CLOSED.
#[test]
fn failover_timing_invalid_successor_no_e_prime_fails_closed() {
  use crate::{
    AppendEntries, AppendResp, Config, Entry, EntryKind, Index, Instant, Message, Term, VoteResp,
  };
  use core::time::Duration;

  const S: u64 = 1_700_000_000_000_000_000;
  const W: u64 = 100_000_000; // a SMALL inherited window (100ms) — a fireable bare deadline

  // LeaseGuard with a Δ near the u64-ns ceiling: the window `Δ·(Δ+ε)/(Δ−ε)` exceeds u64::MAX, so
  // `leaseguard_commit_wait_ns` / `leaseguard_timing` is None (TIMING-INVALID). A huge election timeout
  // means the PRIOR clamped E′ (~half u64::MAX) would have compared BELOW it and (buggily) set
  // commit_wait_inflated. No ε_unc — so once E′ is correctly unavailable, the node fails closed.
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_secs(10_000_000_000), // election ≫ any clamped E′
    Duration::from_secs(1),
  )
  .unwrap()
  .with_read_only(crate::ReadOnlyOption::LeaseGuard)
  .with_lease_duration(Duration::from_nanos(u64::MAX)) // window overflows u64 ⇒ timing-invalid
  .with_clock_drift_bound(Duration::from_nanos(1));
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::NoopStable::default();

  // Inherit a SMALL wall-stamped committed entry (window 100ms) from a deposed failover leader.
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
          EntryKind::Empty,
          bytes::Bytes::new()
        )
        .with_lease_window(W)
        .with_wall_timestamp(S)
      ],
      Index::new(1),
    )),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  while ep.poll_message().is_some() {}

  let d = ep.poll_timeout().unwrap();
  let mono = |off: u64| crate::Now::monotonic(d + Duration::from_nanos(off));
  ep.handle_timeout(mono(0), &mut log, &mut stable);
  ep.handle_storage(mono(0), &mut log, &mut stable);
  ep.handle_message(
    mono(0),
    &mut log,
    &mut stable,
    3u64,
    Message::VoteResp(VoteResp::new(Term::new(6), 3u64, false, false)),
  );
  assert!(ep.role().is_leader());
  ep.handle_storage(mono(0), &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}
  ep.handle_message(
    mono(0),
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
  assert_eq!(ep.commit_index(), Index::new(1));

  // The timing-invalid node has NO usable E′ (leaseguard_timing None ⇒ failover_inflated_commit_wait None)
  // and no ε_unc — so at the small inherited deadline (100ms) it FAILS CLOSED rather than clear via the
  // prior clamped-E′ shortcut. Past the bare deadline and beyond, commit holds.
  for off in [W, W + 1_000_000_000] {
    ep.handle_timeout(mono(off), &mut log, &mut stable);
    while ep.poll_message().is_some() {}
    assert_eq!(
      ep.commit_index(),
      Index::new(1),
      "a timing-invalid successor must not clear via a clamped E′ — it fails closed without ε_unc"
    );
  }
}

/// FAILOVER R14/R15 regression (deadline-scheduling totality): `Instant::add` SATURATES, so a
/// `become_leader` at a monotonic instant within `commit_wait_window` of `Instant::MAX` would store a
/// `commit_wait_until` clamped at the max — a real wait SHORTER than the window, which would clear the
/// commit-wait early and commit before a deposed leader's lease elapsed. The R14 flag gate
/// (`deadline_exact`) disarms the serve; the R15 fix additionally FAIL-STOPS the whole commit-wait
/// (poison `CommitWaitUnrepresentable`) since even the BARE wait would clear early. Here the election
/// instant is `Instant::MAX − 50ms` and the E′ wait is 116.67ms, so the deadline saturates: the node
/// poisons (and its serve is consequently withheld). Unreachable by any real monotonic clock.
#[test]
fn failover_unrepresentable_commit_deadline_disarms_serve() {
  use crate::{
    AppendEntries, Config, Entry, EntryKind, Index, Instant, Message, Term, VoteResp, Wall,
  };
  use core::time::Duration;

  const S: u64 = 1_700_000_000_000_000_000;
  const W: u64 = 100_000_000; // inherited window 100ms → E′ ≈ 116.67ms > the 50ms headroom below MAX

  // Valid armed failover config (Δ=300ms, ε_drift=50ms, ε_unc=20ms) — it WOULD arm the serve but for the
  // unrepresentable deadline.
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
  .with_bounded_clock_uncertainty(Duration::from_millis(20));
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
          EntryKind::Empty,
          bytes::Bytes::new()
        )
        .with_lease_window(W)
        .with_wall_timestamp(S)
      ],
      Index::new(1),
    )),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  while ep.poll_message().is_some() {}

  // Elect at a monotonic instant 50ms below Instant::MAX, under a live wall (lease just started at S). The
  // election deadline (from ORIGIN) is far in the past relative to this instant, so the campaign fires.
  let near_max = Instant::from_origin(Duration::MAX - Duration::from_millis(50));
  let now = crate::Now::synchronized(near_max, Wall::from_nanos(S));
  ep.handle_timeout(now, &mut log, &mut stable);
  ep.handle_storage(now, &mut log, &mut stable);
  ep.handle_message(
    now,
    &mut log,
    &mut stable,
    3u64,
    Message::VoteResp(VoteResp::new(Term::new(6), 3u64, false, false)),
  );
  assert!(ep.role().is_leader());
  ep.handle_storage(now, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  // The commit-wait deadline `near_max + 116.67ms` saturates at Instant::MAX (a wait < the window), so the
  // node FAIL-STOPS rather than under-wait: it poisons. (A poisoned node's handle_* return early, so it
  // never advances commit — the deposed lease is honored by holding.) The serve is consequently withheld.
  assert!(
    ep.is_poisoned(),
    "an unrepresentable (saturated) commit-wait deadline must FAIL-STOP (poison)"
  );
  assert_eq!(
    ep.poison_reason(),
    Some(crate::PoisonReason::CommitWaitUnrepresentable)
  );
  assert!(
    ep.failover_read_window(now).is_none(),
    "a poisoned node never advertises an inherited serve"
  );
}

/// R15 regression (BASIC-LeaseGuard saturated commit-wait): the saturated-deadline fail-stop is NOT
/// failover-specific — a basic LeaseGuard successor with a WALL-ABSENT inherited window (so
/// `inherited_release_deadline == 0` and there is no walled veto) elected near `Instant::MAX` would, with
/// the saturating `now + max_lease_window`, store a deadline at `Instant::MAX` (a wait shorter than the
/// window) and clear the commit-wait early — committing before the deposed leader's BASIC lease window
/// elapsed (a stale read). The fix fail-stops (poison `CommitWaitUnrepresentable`) for ANY saturated
/// commit-wait, bare or inflated, so the commit cannot advance.
#[test]
fn basic_leaseguard_unrepresentable_commit_deadline_fails_stop() {
  use crate::{
    AppendEntries, AppendResp, Config, Entry, EntryKind, Index, Instant, Message, Term, VoteResp,
  };
  use core::time::Duration;

  // Basic LeaseGuard (valid timing, NO ε_unc — not the failover tier).
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

  // Inherit a WALL-ABSENT windowed entry (lease_window = 100ms, wall_timestamp = 0) — a basic LeaseGuard
  // commit-wait obligation, NO walled lease (inherited_release_deadline stays 0, so no Option B veto).
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
          EntryKind::Empty,
          bytes::Bytes::new()
        )
        .with_lease_window(100_000_000)
      ],
      Index::new(1),
    )),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  while ep.poll_message().is_some() {}

  // Elect near Instant::MAX (monotonic — basic LeaseGuard stamps no wall). The bare commit-wait
  // `near_max + 100ms` saturates.
  let near_max = Instant::from_origin(Duration::MAX - Duration::from_millis(50));
  let now = crate::Now::monotonic(near_max);
  ep.handle_timeout(now, &mut log, &mut stable);
  ep.handle_storage(now, &mut log, &mut stable);
  ep.handle_message(
    now,
    &mut log,
    &mut stable,
    3u64,
    Message::VoteResp(VoteResp::new(Term::new(6), 3u64, false, false)),
  );
  // The saturated bare commit-wait fail-stops: the node poisons.
  assert!(
    ep.is_poisoned(),
    "a saturated BARE (basic LeaseGuard) commit-wait must fail-stop, not clear early"
  );
  assert_eq!(
    ep.poison_reason(),
    Some(crate::PoisonReason::CommitWaitUnrepresentable)
  );

  // A quorum ack at the same (near-MAX) instant must NOT advance the commit past the inherited index —
  // the poisoned node's handle_message returns early, so it never commits before the deposed lease.
  ep.handle_message(
    now,
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
    Index::new(1),
    "a poisoned node must not advance commit (the saturated deadline would have under-waited the lease)"
  );
}

/// FAILOVER R16 regression (non-passable wall horizon, the cross-node fail-open): a BARE-wait ε_unc
/// successor (Safe + ε_unc — no E′ inflation) that inherited a WALLED entry whose horizon
/// `wall_timestamp + lease_window + 2·ε_unc` is NON-PASSABLE (a near-`u64::MAX` wall stamp) must FAIL-STOP.
/// The prior code SKIPPED the wall veto for a non-passable LOCAL max, reasoning "the serve is disarmed for
/// this horizon" — but that only disarms THIS node's serve; another leader that did NOT inherit the
/// near-ceiling tail entry arms a serve on a LOWER, PASSABLE committed anchor at `c`, and this bare
/// successor would skip the veto and commit past `c`, undercutting it. The fix poisons such a successor
/// (`WallHorizonUnrepresentable`), so it can neither serve nor commit. (An E′-inflated successor is exempt
/// — see `failover_unrepresentable_wall_horizon_disarms_serve_without_wedging`, which has valid lease
/// timing and rides E′.)
#[test]
fn failover_non_passable_wall_horizon_bare_successor_fails_stop() {
  use crate::{
    AppendEntries, AppendResp, Config, Entry, EntryKind, Index, Instant, Message, Term, VoteResp,
    Wall,
  };
  use core::time::Duration;

  const S: u64 = 1_700_000_000_000_000_000;
  const W: u64 = 100_000_000;
  const EPS: u64 = 20_000_000;
  // wall + W + 2·ε_unc saturates above u64::MAX ⇒ the horizon is non-passable.
  const HUGE_WALL: u64 = u64::MAX - 1;

  // SAFE + ε_unc: bare wait (no E′ — no Δ), ε_unc-bearing (so it WOULD use the wall-gate).
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_bounded_clock_uncertainty(Duration::from_nanos(EPS));
  assert_eq!(cfg.read_only(), crate::ReadOnlyOption::Safe);
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::NoopStable::default();

  // Inherit a WALL-stamped committed entry with a near-ceiling wall stamp (the non-passable horizon).
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
          EntryKind::Empty,
          bytes::Bytes::new()
        )
        .with_lease_window(W)
        .with_wall_timestamp(HUGE_WALL)
      ],
      Index::new(1),
    )),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  while ep.poll_message().is_some() {}

  let d = ep.poll_timeout().unwrap();
  let at =
    |off: u64| crate::Now::synchronized(d + Duration::from_nanos(off), Wall::from_nanos(S + off));
  ep.handle_timeout(at(0), &mut log, &mut stable);
  ep.handle_storage(at(0), &mut log, &mut stable);
  ep.handle_message(
    at(0),
    &mut log,
    &mut stable,
    3u64,
    Message::VoteResp(VoteResp::new(Term::new(6), 3u64, false, false)),
  );
  // The non-passable inherited wall horizon on a bare ε_unc successor fail-stops.
  assert!(
    ep.is_poisoned(),
    "a bare ε_unc successor with a non-passable inherited wall horizon must FAIL-STOP (not skip the veto)"
  );
  assert_eq!(
    ep.poison_reason(),
    Some(crate::PoisonReason::WallHorizonUnrepresentable)
  );

  // A quorum ack must NOT advance the commit — the poisoned node never undercuts a peer's serve at c.
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
    Index::new(1),
    "a poisoned bare successor must not commit past c (no cross-node undercut)"
  );
}

/// R17 regression (inconsistent recovered lease floors): a crafted/corrupt `SnapshotMeta` can carry a
/// walled release floor (`max_wall_plus_window != 0`) with NO lease-window bound (`max_lease_window == 0`)
/// — live folds can never separate them (a walled entry's window goes to BOTH). With `max_lease_window ==
/// 0`, `failover_inflated_commit_wait` returns `Some(0)`, which (before the fix) marked the node
/// E′-inflated with a ZERO wait and `commit_wait_until == None`, so the first commit passed IMMEDIATELY
/// despite a walled lease to honor. The fix gates `inflated_candidate` on `max_lease_window > 0` AND
/// fail-stops the inconsistency (`InconsistentLeaseFloor`), so the node poisons before any commit.
#[test]
fn inconsistent_lease_floor_snapshot_fails_stop() {
  use crate::{
    AppendResp, Config, Index, InstallSnapshot, Instant, Message, SnapshotMeta, Term, VoteResp,
    conf::ConfState,
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

  // Install a snapshot whose carried floors are INCONSISTENT: a walled release floor but NO window bound.
  let meta = SnapshotMeta::new(
    Index::new(5),
    Term::new(1),
    ConfState::from_voters(std::vec![1u64, 2, 3]),
  )
  .with_max_wall_plus_window(u64::MAX - 1) // a walled release floor...
  .with_max_lease_window(0); // ...but no lease-window bound (the inconsistency)
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
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  while ep.poll_message().is_some() {}

  // Campaign and win term 2. become_leader must FAIL-STOP on the inconsistent floors.
  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable);
  ep.handle_storage(d, &mut log, &mut stable);
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResp(VoteResp::new(Term::new(2), 2u64, false, false)),
  );
  assert!(
    ep.is_poisoned(),
    "inconsistent recovered lease floors (walled floor, zero window) must FAIL-STOP at become_leader"
  );
  assert_eq!(
    ep.poison_reason(),
    Some(crate::PoisonReason::InconsistentLeaseFloor)
  );

  // A quorum ack must NOT advance commit past the snapshot index — the poisoned node never commits with no
  // commit-wait despite the walled inherited lease floor.
  let before = ep.commit_index();
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendResp(AppendResp::new(
      Term::new(2),
      2u64,
      false,
      Index::ZERO,
      Term::ZERO,
      Index::new(7),
    )),
  );
  assert_eq!(
    ep.commit_index(),
    before,
    "a poisoned node must not advance commit (the zero-window floor would have committed immediately)"
  );
}

/// Shared body for the R18 unwalled-floor regressions: install a snapshot whose carried floors violate the
/// `max_unwalled_lease_window <= max_lease_window` invariant, campaign to leader, and assert `become_leader`
/// FAIL-STOPS with `InconsistentLeaseFloor` before any commit can advance. (No `bounded_clock_uncertainty`
/// and a representable `max_lease_window`-sized deadline, so `InconsistentLeaseFloor` is the only poison
/// that can fire — the same minimal LeaseGuard config the R17 walled-floor regression uses.)
fn assert_inconsistent_unwalled_floor_fails_stop(
  max_unwalled_lease_window: u64,
  max_lease_window: u64,
) {
  use crate::{
    AppendResp, Config, Index, InstallSnapshot, Instant, Message, SnapshotMeta, Term, VoteResp,
    conf::ConfState,
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

  // Install a snapshot whose floors violate `max_unwalled_lease_window <= max_lease_window`. A consistent
  // fold can never produce this (the unwalled max is taken over a SUBSET of the same windows); only a
  // crafted/corrupt `SnapshotMeta`, which carries the folded values directly, can.
  let meta = SnapshotMeta::new(
    Index::new(5),
    Term::new(1),
    ConfState::from_voters(std::vec![1u64, 2, 3]),
  )
  .with_max_unwalled_lease_window(max_unwalled_lease_window)
  .with_max_lease_window(max_lease_window);
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
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  while ep.poll_message().is_some() {}

  // Campaign and win term 2. become_leader must FAIL-STOP on the inconsistent unwalled floor.
  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable);
  ep.handle_storage(d, &mut log, &mut stable);
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResp(VoteResp::new(Term::new(2), 2u64, false, false)),
  );
  assert!(
    ep.is_poisoned(),
    "an inconsistent recovered unwalled floor (max_unwalled_lease_window > max_lease_window) must \
     FAIL-STOP at become_leader"
  );
  assert_eq!(
    ep.poison_reason(),
    Some(crate::PoisonReason::InconsistentLeaseFloor)
  );

  // A quorum ack must NOT advance commit — the poisoned node never schedules the larger unwalled deadline.
  let before = ep.commit_index();
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendResp(AppendResp::new(
      Term::new(2),
      2u64,
      false,
      Index::ZERO,
      Term::ZERO,
      Index::new(7),
    )),
  );
  assert_eq!(
    ep.commit_index(),
    before,
    "a poisoned node must not advance commit (the larger unwalled floor would have committed early)"
  );
}

/// R18 regression (unwalled fallback floor exceeds the window bound — zero-window shape): a crafted
/// `SnapshotMeta` carries `max_unwalled_lease_window > 0` with `max_lease_window == 0`. The conservative
/// path schedules NO commit-wait from the zero window (`commit_wait_until == None`), so the first commit
/// would pass IMMEDIATELY while the larger unwalled fallback was only ever placed in
/// `unwalled_commit_wait_until` — the same floor-without-window class as R17, through the unwalled field.
#[test]
fn inconsistent_unwalled_floor_zero_window_fails_stop() {
  assert_inconsistent_unwalled_floor_fails_stop(350_000_000, 0);
}

/// R18 regression (unwalled fallback floor exceeds the window bound — early-commit shape): a crafted
/// `SnapshotMeta` carries `max_unwalled_lease_window > max_lease_window > 0`. The conservative path
/// schedules the SMALLER `max_lease_window` wait and clears it before the precise fallback can force the
/// larger unwalled deadline, so the first commit passes EARLY. In a consistent fold the unwalled max is a
/// subset and must be `<= max_lease_window`.
#[test]
fn inconsistent_unwalled_floor_early_commit_fails_stop() {
  assert_inconsistent_unwalled_floor_fails_stop(400_000_000, 200_000_000);
}

/// Body for the structural fold-consistency fail-stop: install a `SnapshotMeta` carrying the given
/// `(max_lease_window, max_wall_plus_window, max_unwalled_lease_window)` floors under a FAILOVER config
/// (ε_unc + a synchronized wall — the case where `precise_release_ready` runs), campaign to leader, and
/// assert `become_leader` FAIL-STOPS with `InconsistentLeaseFloor` before any commit. Used for the
/// STRUCTURAL contradiction a correct fold can never produce (a nonzero window with no classified floor) —
/// cheap defense-in-depth against a fold bug, NOT against forged metadata (a Byzantine/corrupt-storage
/// concern, out of the CFT threat model).
fn assert_inconsistent_lease_floor_fails_stop(
  max_lease_window: u64,
  max_wall_plus_window: u64,
  max_unwalled_lease_window: u64,
) {
  use crate::{
    AppendResp, Config, Index, InstallSnapshot, Instant, Message, SnapshotMeta, Term, VoteResp,
    Wall, conf::ConfState,
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
  .with_clock_drift_bound(Duration::from_millis(50))
  .with_bounded_clock_uncertainty(Duration::from_millis(20));
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::AsyncStable::default();

  let meta = SnapshotMeta::new(
    Index::new(5),
    Term::new(1),
    ConfState::from_voters(std::vec![1u64, 2, 3]),
  )
  .with_max_lease_window(max_lease_window)
  .with_max_wall_plus_window(max_wall_plus_window)
  .with_max_unwalled_lease_window(max_unwalled_lease_window);
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
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  while ep.poll_message().is_some() {}

  // Campaign and win term 2 under a synchronized wall (so `precise_release_ready` WOULD run). `become_leader`
  // must FAIL-STOP on the inconsistent floors before the vacuous/early release can fire.
  let d = ep.poll_timeout().unwrap();
  let at = |off: u64| {
    crate::Now::synchronized(
      d + Duration::from_nanos(off),
      Wall::from_nanos(1_700_000_000_000_000_000 + off),
    )
  };
  ep.handle_timeout(at(0), &mut log, &mut stable);
  ep.handle_storage(at(0), &mut log, &mut stable);
  ep.handle_message(
    at(0),
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResp(VoteResp::new(Term::new(2), 2u64, false, false)),
  );
  assert!(
    ep.is_poisoned(),
    "a nonzero window with no COVERING floor must FAIL-STOP at become_leader"
  );
  assert_eq!(
    ep.poison_reason(),
    Some(crate::PoisonReason::InconsistentLeaseFloor)
  );

  // A quorum ack must NOT advance commit — without the fix the commit-wait would clear (vacuously, or on the
  // too-small floor's immediate wall expiry) and commit past the snapshot index before the window elapsed.
  let before = ep.commit_index();
  ep.handle_message(
    at(0),
    &mut log,
    &mut stable,
    2u64,
    Message::AppendResp(AppendResp::new(
      Term::new(2),
      2u64,
      false,
      Index::ZERO,
      Term::ZERO,
      Index::new(7),
    )),
  );
  assert_eq!(
    ep.commit_index(),
    before,
    "a poisoned node must not advance commit (the early precise release is foreclosed)"
  );
}

/// Structural fold-consistency fail-stop (a window bound with NO classified floor): `max_lease_window > 0`
/// but BOTH derived floors zero — a contradiction a correct fold can never produce (a nonzero window comes
/// from a walled or unwalled entry, which would set one of the floors). `inherited_release_deadline == 0`
/// and `unwalled_commit_wait_until == None`, so `precise_release_ready` would find both halves vacuously
/// expired and clear the wait immediately. Defense-in-depth against a fold BUG; forged-MAGNITUDE shapes (a
/// too-small nonzero floor) are the out-of-CFT Byzantine class and deliberately not chased.
#[test]
fn inconsistent_lease_floor_no_classified_floor_fails_stop() {
  assert_inconsistent_lease_floor_fails_stop(350_000_000, 0, 0);
}

/// FAILOVER R4 config-behavior change: a LeaseGuard config whose BASE window fits below the election
/// timeout is VALID even when its E′-INFLATED failover wait (`window · (Δ+ε_drift)/Δ`) would exceed it.
/// The runtime inflation keys on `max_lease_window` — the MAX window INHERITED, possibly stamped by
/// ANOTHER node's larger config — which config-time validation cannot bound (there is no cluster-wide
/// config check, design §1). So the over-large case is gated at RUNTIME (`inherited_serve_armed` in
/// `become_leader`: the serve is disarmed and the bare wait is used), NOT rejected here. This supersedes
/// the earlier config-rejection test, which incorrectly checked the LOCAL window as if it bounded the
/// inherited max.
#[test]
fn failover_config_with_inflated_wait_over_election_is_still_valid() {
  use crate::{Config, ReadOnlyOption};
  use core::time::Duration;

  // Δ = 400ms, ε_drift = 100ms → base window = 400·500/300 ≈ 666.7ms (BELOW the 700ms election timeout),
  // but the inflated wait = base · (400+100)/400 ≈ 833ms (ABOVE it). The BASE window governs validity.
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(700), // election_timeout
    Duration::from_millis(100), // heartbeat
  )
  .unwrap()
  .with_read_only(ReadOnlyOption::LeaseGuard)
  .with_lease_duration(Duration::from_millis(400))
  .with_clock_drift_bound(Duration::from_millis(100))
  .with_bounded_clock_uncertainty(Duration::from_millis(50));

  cfg.validate().expect(
    "the base window (~666ms) is below the 700ms election timeout; the failover inflation is gated at \
     runtime, not rejected at config validation",
  );
}

/// FAILOVER R4 finding 1 (heterogeneous-window runtime guard): when this node INHERITS a lease window so
/// large that the E′-inflated conservative wait would NOT fit below the election timeout, the inherited
/// serve is DISARMED (`failover_read_window` returns `None` even though the committed anchor's lease is
/// live), and the commit-wait falls back to the BARE `max_lease_window` (the shipped conservative anchor)
/// so the node still makes progress. The inflation keys on the inherited max — another node's larger
/// config — which config validation cannot bound, so the guard lives at `become_leader`, not config time.
#[test]
fn failover_serve_disarmed_when_inherited_window_inflation_exceeds_election() {
  use crate::{
    AppendEntries, AppendResp, Config, Entry, EntryKind, Index, Instant, Message, Term, VoteResp,
    Wall,
  };
  use core::time::Duration;

  const S: u64 = 1_700_000_000_000_000_000; // the inherited COMMITTED entry's wall stamp
  // INHERITED window W = 900ms (from a hypothetical larger-config node). Bare wait 900ms < 1000ms
  // election (schedulable) BUT inflated = 900 · 350/300 = 1050ms ≥ 1000ms (NOT schedulable) → DISARMED.
  const W: u64 = 900_000_000;
  const EPS: u64 = 20_000_000; // ε_unc = 20ms — the lease is live at the election wall (40ms ≪ 900ms)

  // This node's OWN config: Δ = 300ms, ε_drift = 50ms → base window 300·350/250 = 420ms < 1000ms (VALID).
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000), // election_timeout
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

  // Inherit a wall-stamped entry with the OVERSIZED 900ms window and COMMIT it (leader_commit = 1).
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
          EntryKind::Empty,
          bytes::Bytes::new()
        )
        .with_lease_window(W)
        .with_wall_timestamp(S)
      ],
      Index::new(1),
    )),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  while ep.poll_message().is_some() {}

  // node 1 wins term 6 under a SYNCHRONIZED wall whose election instant is S (the lease is fully alive).
  // The commit-wait is driven UNDER THE WALL throughout — this ε_unc failover-tier node's non-armed
  // (bare) wait is wall-GOVERNED (held below the wall floor S + 940ms, released past it), proving the
  // wait is the bare 900ms and not the inflated 1050ms (which would still be held at d + 1000ms).
  let d = ep.poll_timeout().unwrap();
  let at =
    |off: u64| crate::Now::synchronized(d + Duration::from_nanos(off), Wall::from_nanos(S + off));
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
  assert_eq!(
    ep.commit_index(),
    Index::new(1),
    "commit held at the inherited index during the (bare) wait"
  );

  // The inherited serve is DISARMED: even though the committed anchor's lease is live (now_wall + 2·ε_unc
  // = S + 40ms ≪ S + 900ms), the window is withheld because the E′-inflated wait (1050ms) does not fit
  // below the 1000ms election timeout. The commit-wait IS armed (commit held, role Leader), so the only
  // reason for `None` is the disarm — not a missing anchor or a lifted wait.
  assert!(
    ep.failover_read_window(at(0)).is_none(),
    "an oversized inherited window must DISARM the serve even while the anchor lease is live"
  );

  // peer 3 acks the no-op at index 2 → quorum, so commit CAN advance once the (bare) wait lifts.
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

  // The commit-wait uses the BARE max_lease_window (NOT the E′ inflation — that is the disarm), but it is
  // WALL-GOVERNED (Option B): at the bare deadline d + W (wall S + 900ms, still below the floor
  // S + W + 2·ε_unc = S + 940ms) the conservative clear is VETOED — commit holds — and the timer re-arms.
  // (Driven under the synchronized wall: this is an ε_unc failover-tier node, and an absent wall fails
  // closed for a non-armed node, so the contract is to supply the wall.)
  ep.handle_timeout(at(W), &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  assert_eq!(
    ep.commit_index(),
    Index::new(1),
    "the bare mono deadline does NOT clear a still-live walled lease (held below the wall floor)"
  );
  // Once the wall passes the floor (S + 940ms), the re-armed conservative deadline (d + W + heartbeat =
  // d + 1000ms) clears and commits — the release is governed by the WALL, not the bare d + 900ms mono
  // deadline (which would have under-waited the 940ms wall floor). This proves the wait is NOT the
  // inflated 1050ms (an inflated deadline would still be held at d + 1000ms) AND is wall-gated.
  ep.handle_timeout(at(W + 100_000_000), &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  assert_eq!(
    ep.commit_index(),
    Index::new(2),
    "once the wall passes the floor the re-armed conservative deadline commits (wall-governed release)"
  );
}

/// FAILOVER R4 finding 2 (tier gating): `failover_tier_active` — which gates the wall stamp, the precise
/// commit-anchor, AND the inherited-read serve — is true ONLY for a VALID, ACTIVE LeaseGuard failover
/// tier: LeaseGuard mode with valid timing (a computable lease window) AND a bounded clock-uncertainty.
/// `Endpoint::new` does NOT call `Config::validate`, so a Safe/LeaseBased config (or a timing-invalid
/// LeaseGuard one) carrying `bounded_clock_uncertainty` must NOT activate the tier — it would otherwise
/// serve inherited reads under a config the rest of the crate degrades to Safe.
#[test]
fn failover_tier_active_requires_leaseguard_timing_and_bounded_uncertainty() {
  use crate::{Config, Instant, ReadOnlyOption};
  use core::time::Duration;

  let base = || {
    Config::try_new(
      1u64,
      std::vec![1u64, 2, 3],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap()
  };
  let endpoint =
    |cfg: Config<u64>| Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());

  // (1) LeaseGuard + valid timing + bounded uncertainty → ACTIVE.
  let active = endpoint(
    base()
      .with_read_only(ReadOnlyOption::LeaseGuard)
      .with_lease_duration(Duration::from_millis(300))
      .with_clock_drift_bound(Duration::from_millis(50))
      .with_bounded_clock_uncertainty(Duration::from_millis(20)),
  );
  assert!(
    active.failover_tier_active(),
    "a valid LeaseGuard config with bounded uncertainty activates the failover tier"
  );

  // (2) LeaseGuard + valid timing but NO bounded uncertainty → inactive (cross-node wall comparison needs
  // a bounded ε_unc; without it the serve cannot reason about clock skew).
  let no_unc = endpoint(
    base()
      .with_read_only(ReadOnlyOption::LeaseGuard)
      .with_lease_duration(Duration::from_millis(300))
      .with_clock_drift_bound(Duration::from_millis(50)),
  );
  assert!(
    !no_unc.failover_tier_active(),
    "LeaseGuard without a bounded clock-uncertainty does not activate the failover tier"
  );

  // (3) Safe mode carrying a bounded uncertainty → inactive (no LeaseGuard timing).
  let safe = endpoint(
    base()
      .with_read_only(ReadOnlyOption::Safe)
      .with_bounded_clock_uncertainty(Duration::from_millis(20)),
  );
  assert!(
    !safe.failover_tier_active(),
    "a Safe config must not activate the failover tier even with bounded uncertainty set"
  );

  // (4) LeaseBased mode carrying lease timing + bounded uncertainty → inactive (LeaseGuard timing is
  // `None` for any non-LeaseGuard mode).
  let lease_based = endpoint(
    base()
      .with_read_only(ReadOnlyOption::LeaseBased)
      .with_lease_duration(Duration::from_millis(300))
      .with_clock_drift_bound(Duration::from_millis(50))
      .with_bounded_clock_uncertainty(Duration::from_millis(20)),
  );
  assert!(
    !lease_based.failover_tier_active(),
    "a LeaseBased config must not activate the failover tier"
  );

  // (5) LeaseGuard but TIMING-INVALID (window Δ·(Δ+ε)/(Δ−ε) ≥ election timeout) + bounded uncertainty →
  // inactive: the timing is unschedulable, so `leaseguard_timing` is `None` and the tier degrades to Safe.
  let invalid_timing = endpoint(
    base()
      .with_read_only(ReadOnlyOption::LeaseGuard)
      .with_lease_duration(Duration::from_millis(900)) // window 900·1000/800 = 1125ms ≥ 1000ms election
      .with_clock_drift_bound(Duration::from_millis(100))
      .with_bounded_clock_uncertainty(Duration::from_millis(20)),
  );
  assert!(
    !invalid_timing.failover_tier_active(),
    "a timing-invalid LeaseGuard config (window ≥ election timeout) does not activate the failover tier"
  );

  // (6) LeaseGuard + valid window but ε_unc ≥ Δ → inactive: `Config::validate` REJECTS this
  // (`ε_unc ≥ Δ` makes the cross-node age comparison vacuous), and the runtime tier MUST agree —
  // `failover_tier_active` delegates to the same `Config::failover_tier_valid`, so a config the crate
  // would reject can never activate the tier at runtime (the defect class where `Endpoint::new` does not
  // call `validate`). Here ε_unc = 300ms = Δ.
  let unc_too_large_cfg = base()
    .with_read_only(ReadOnlyOption::LeaseGuard)
    .with_lease_duration(Duration::from_millis(300))
    .with_clock_drift_bound(Duration::from_millis(50))
    .with_bounded_clock_uncertainty(Duration::from_millis(300)); // ε_unc == Δ (≥ Δ)
  assert!(
    unc_too_large_cfg.validate().is_err(),
    "Config::validate must reject ε_unc ≥ Δ"
  );
  let unc_too_large = endpoint(unc_too_large_cfg);
  assert!(
    !unc_too_large.failover_tier_active(),
    "ε_unc ≥ Δ must NOT activate the failover tier at runtime — the same condition validate() rejects"
  );

  // CONSISTENCY: the runtime tier-active predicate agrees with `Config::validate` across these configs —
  // a config that validates as a failover tier is active; one validate rejects (for the failover reason)
  // is inactive. (The valid base config (1) is active and validates; the ε_unc ≥ Δ config is both
  // rejected and inactive.) This is the single-source-of-truth invariant that ends the R4/R9 class.
  let valid_failover = base()
    .with_read_only(ReadOnlyOption::LeaseGuard)
    .with_lease_duration(Duration::from_millis(300))
    .with_clock_drift_bound(Duration::from_millis(50))
    .with_bounded_clock_uncertainty(Duration::from_millis(20));
  assert!(valid_failover.validate().is_ok());
  assert!(endpoint(valid_failover).failover_tier_active());
}

/// FAILOVER R5 regression (arming totality): the E′-inflated conservative wait is computed in `u128`. When
/// the EXACT ceil inflation exceeds `u64::MAX` it is NOT representable as the `Duration::from_nanos` wait
/// `become_leader` schedules — so the serve must FAIL CLOSED. The prior `unwrap_or(u64::MAX)` CLAMP let
/// the arming check compare a too-small value against the election timeout: with an election timeout above
/// `u64::MAX` nanos and an inherited window forcing the exact inflation above `u64::MAX`, the clamp armed
/// the serve while scheduling a wait SHORTER than the E′ bound, re-opening the R2 mono-undercut. The fix
/// returns `None` on overflow → the serve is disarmed and the bare wait is used.
///
/// The magnitudes are deliberately at the `u64` boundary (this is a totality guard, not a realistic
/// deployment): Δ = 2ns, ε_drift = 1ns (inflation factor 3/2), inherited `max_lease_window = u64::MAX`
/// (exact inflation ≈ 1.5·u64::MAX, above `u64::MAX`), and an election timeout of ~1268 years (its nanos
/// exceed `u64::MAX`, so the clamped `u64::MAX` would have compared below it and ARMED — the exact bug).
#[test]
fn failover_serve_disarmed_when_inflation_overflows_u64() {
  use crate::{
    AppendEntries, Config, Entry, EntryKind, Index, Instant, Message, Term, VoteResp, Wall,
  };
  use core::time::Duration;

  const S: u64 = 1_700_000_000_000_000_000; // the inherited COMMITTED entry's wall stamp
  // Election timeout ~1268 years: its `as_nanos()` (≈ 4·10^19) exceeds u64::MAX (≈ 1.84·10^19), so the
  // CLAMPED u64::MAX would have compared BELOW it and (buggily) armed.
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_secs(40_000_000_000), // election_timeout ≫ u64::MAX nanos
    Duration::from_secs(1),              // heartbeat (must be < election_timeout)
  )
  .unwrap()
  .with_read_only(crate::ReadOnlyOption::LeaseGuard)
  .with_lease_duration(Duration::from_nanos(2)) // Δ = 2ns
  .with_clock_drift_bound(Duration::from_nanos(1)) // ε_drift = 1ns → inflation factor 3/2
  .with_bounded_clock_uncertainty(Duration::from_nanos(1));
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::NoopStable::default();

  // Inherit a wall-stamped entry whose window is u64::MAX (exact inflation ceil(u64::MAX · 3/2) overflows
  // u64) and COMMIT it.
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
          EntryKind::Empty,
          bytes::Bytes::new()
        )
        .with_lease_window(u64::MAX)
        .with_wall_timestamp(S)
      ],
      Index::new(1),
    )),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  while ep.poll_message().is_some() {}

  // node 1 wins term 6 under a synchronized wall at S (the lease would be live: now_wall + 2·ε_unc ≪
  // S + u64::MAX).
  let d = ep.poll_timeout().unwrap();
  let at =
    |off: u64| crate::Now::synchronized(d + Duration::from_nanos(off), Wall::from_nanos(S + off));
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
  assert_eq!(
    ep.commit_index(),
    Index::new(1),
    "commit held at the inherited index during the (bare) wait — the wait is armed (role Leader)"
  );

  // The serve is DISARMED: the exact inflation overflows u64, so `failover_inflated_commit_wait` returns
  // None and the serve does not arm — even though the lease is live and the election timeout (1268y) is
  // far above the clamped u64::MAX the prior code would have (buggily) compared against. With the clamp
  // bug this would return Some (an inherited serve backed by a wait ~292 years too short).
  assert!(
    ep.failover_read_window(at(0)).is_none(),
    "an inflation overflowing u64 must DISARM the serve (fail closed), not arm with a clamped wait"
  );
}

/// FAILOVER R19 + R16 regression (a non-passable wall horizon FAILS STOP, even with valid lease timing): an
/// inherited walled entry stamps the wall release threshold `max_wall_plus_window + 2·ε_unc` to EXACTLY
/// `u64::MAX` (`wall = u64::MAX − W − 2·ε_unc`). No `u64` wall can ever exceed that (strict `>`), so the
/// floor is UNPROVABLE by a wall. This node HAS valid lease timing (Δ, ε_drift), so BEFORE R19 it rode the
/// E′-inflated wait to avoid a wedge. R19 gates the E′ veto-skip on a wall-PROOF (`now_wall +
/// max_lease_window ≥ max_wall_plus_window + 2·ε_unc`) that a near-`u64::MAX` floor can NEVER satisfy
/// (`now_wall ≈ S ≪ u64::MAX`), so the node is NOT inflated — the `commit_wait_inflated`-exempt path no
/// longer hides a non-passable floor (Expert-panel finding A8). `become_leader` then FAIL-STOPS via
/// `WallHorizonUnrepresentable`: a floor provable by NEITHER a wall NOR E′ must poison — not wedge, and not
/// clear on an unsound E′ release. The serve is refused too (a poisoned node serves nothing). Unreachable
/// under synchronized clocks (~now ≪ u64::MAX); a pure totality guard.
#[test]
fn failover_unrepresentable_wall_horizon_fails_stop_with_valid_timing() {
  use crate::{
    AppendEntries, AppendResp, Config, Entry, EntryKind, Index, Instant, Message, Term, VoteResp,
    Wall,
  };
  use core::time::Duration;

  // A NORMAL synchronized wall for this node (real nanos-since-epoch). The INHERITED entry carries a
  // near-ceiling wall stamp chosen so that `max_wall_plus_window + 2·ε_unc == u64::MAX` EXACTLY (no
  // saturation): `wall = u64::MAX − W − 2·ε_unc`, so `wall + W = u64::MAX − 2·ε_unc` and `+ 2·ε_unc` lands
  // precisely on `u64::MAX` — the strict-boundary case.
  const S: u64 = 1_700_000_000_000_000_000;
  const W: u64 = 100_000_000; // inherited lease_window = max_lease_window = 100ms (the bare mono wait)
  const EPS: u64 = 20_000_000; // ε_unc; 2·ε_unc = 40ms
  const HUGE_WALL: u64 = u64::MAX - W - 2 * EPS; // ⇒ wall + W + 2·ε_unc == u64::MAX exactly

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

  // Inherit a wall-stamped committed entry whose wall stamp is near the u64 ceiling.
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
          EntryKind::Empty,
          bytes::Bytes::new()
        )
        .with_lease_window(W)
        .with_wall_timestamp(HUGE_WALL)
      ],
      Index::new(1),
    )),
  );
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  while ep.poll_message().is_some() {}

  // node 1 wins term 6 under its OWN normal synchronized wall (it stamps its no-op fine).
  let d = ep.poll_timeout().unwrap();
  let at =
    |off: u64| crate::Now::synchronized(d + Duration::from_nanos(off), Wall::from_nanos(S + off));
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
      Index::new(2),
    )),
  );
  assert_eq!(ep.commit_index(), Index::new(1));

  // The non-passable horizon is provable by NEITHER a wall (no u64 exceeds u64::MAX) NOR E′ (the wall-proof
  // `now_wall + max_lease_window ≥ floor + 2·ε_unc` fails: now_wall ≈ S ≪ u64::MAX). So this node — despite
  // valid lease timing — is NOT E′-inflated, and `become_leader` FAIL-STOPS via WallHorizonUnrepresentable.
  // The inflated-exemption can no longer hide a non-passable floor (panel finding A8).
  assert!(
    ep.is_poisoned(),
    "a non-passable wall horizon must FAIL-STOP — not wedge, not clear on an unsound E′ release"
  );
  assert_eq!(
    ep.poison_reason(),
    Some(crate::PoisonReason::WallHorizonUnrepresentable)
  );
  // The serve is refused — a poisoned node serves no inherited read.
  assert!(
    ep.failover_read_window(at(0)).is_none(),
    "a poisoned successor (unrepresentable horizon) must serve nothing"
  );
  // Commit cannot advance — a poisoned node early-returns from handle_message/handle_timeout, so it holds
  // at the inherited index (a clean fail-stop, no wedge-vs-release ambiguity).
  const E_PRIME: u64 = 116_666_667; // ceil(100ms · 350/300) — the OLD E′ deadline, now never reached
  ep.handle_timeout(at(E_PRIME), &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  assert_eq!(
    ep.commit_index(),
    Index::new(1),
    "a poisoned node never advances commit past the inherited index"
  );
}

/// The serve dispatch and the stamp helpers read the RUNTIME `active_read_mode`, not the static
/// `Config.read_only` — and they move in LOCKSTEP. A LeaseGuard-configured endpoint whose active mode is
/// overridden to Safe degrades both the serve gate (`leaseguard_timing`) and the per-entry stamp
/// (`lease_window_stamp`) together, while the static config is untouched. (Task 2 of the read-mode
/// migration; the migration flips this runtime field apply-time, exercised directly here.)
#[test]
fn active_read_mode_drives_serve_and_stamp_not_config() {
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

  // Active mode seeded from the config (LeaseGuard): serve gate live, stamp carries the exact window.
  assert_eq!(ep.active_read_mode(), crate::ReadOnlyOption::LeaseGuard);
  assert!(
    ep.leaseguard_timing().is_some(),
    "LeaseGuard: serve gate live"
  );
  assert!(
    ep.lease_window_stamp() > 0,
    "LeaseGuard: a window is stamped"
  );

  // Override the RUNTIME mode to Safe; the static config stays LeaseGuard.
  ep.set_active_read_mode_for_test(crate::ReadOnlyOption::Safe);
  // Serve + stamp degrade TOGETHER off the runtime mode.
  assert!(
    ep.leaseguard_timing().is_none(),
    "Safe runtime mode: the serve gate degrades"
  );
  assert_eq!(
    ep.lease_window_stamp(),
    0,
    "Safe runtime mode: no window stamped (serve + stamp move in lockstep)"
  );
}

/// A committed SetReadMode flips the active mode at APPLY (not append) and emits ReadModeChanged.
#[test]
fn read_mode_flips_at_apply_not_append() {
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_lease_duration(Duration::from_millis(300))
  .with_clock_drift_bound(Duration::from_millis(50));
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::NoopStable::default();
  let t0 = ep.poll_timeout().unwrap();
  ep.handle_timeout(t0, &mut log, &mut stable);
  ep.handle_storage(t0, &mut log, &mut stable);
  assert!(ep.role().is_leader());
  ep.handle_storage(t0, &mut log, &mut stable);
  while ep.poll_event().is_some() {}
  while ep.poll_message().is_some() {}

  assert_eq!(ep.active_read_mode(), crate::ReadOnlyOption::Safe);
  let now = crate::Now::monotonic(t0);
  let idx = ep
    .propose_read_mode_change(now, &mut log, &stable, crate::ReadOnlyOption::LeaseGuard)
    .expect("proposed");
  assert_eq!(
    ep.active_read_mode(),
    crate::ReadOnlyOption::Safe,
    "not flipped before the entry commits + applies"
  );
  ep.handle_storage(t0, &mut log, &mut stable);
  assert_eq!(
    ep.active_read_mode(),
    crate::ReadOnlyOption::LeaseGuard,
    "flipped at apply"
  );
  let mut rmc = None;
  while let Some(ev) = ep.poll_event() {
    if let crate::Event::ReadModeChanged(r) = ev {
      rmc = Some(r);
    }
  }
  let rmc = rmc.expect("ReadModeChanged emitted");
  assert_eq!(rmc.mode(), crate::ReadOnlyOption::LeaseGuard);
  assert_eq!(rmc.index(), idx);
}

/// Into-LeaseGuard warm-up: the SetReadMode entry is stamped under the OLD mode (Safe ⇒ ts=0), so it is
/// not a usable anchor — reads degrade to Safe until a fresh stamped current-term entry commits.
#[test]
fn into_leaseguard_warms_up_to_safe() {
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_lease_duration(Duration::from_millis(300))
  .with_clock_drift_bound(Duration::from_millis(50));
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::NoopStable::default();
  let t0 = ep.poll_timeout().unwrap();
  ep.handle_timeout(t0, &mut log, &mut stable);
  ep.handle_storage(t0, &mut log, &mut stable);
  assert!(ep.role().is_leader());
  ep.handle_storage(t0, &mut log, &mut stable);
  while ep.poll_event().is_some() {}
  while ep.poll_message().is_some() {}

  let now = crate::Now::monotonic(t0);
  ep.propose_read_mode_change(now, &mut log, &stable, crate::ReadOnlyOption::LeaseGuard)
    .expect("proposed");
  ep.handle_storage(t0, &mut log, &mut stable);
  assert_eq!(ep.active_read_mode(), crate::ReadOnlyOption::LeaseGuard);
  // The committed anchor is the SetReadMode entry, stamped ts=0 under Safe → no live lease yet.
  assert!(
    !ep.lease_guard_read_live(now, &log),
    "into-LeaseGuard warm-up: no live lease until a fresh stamped anchor"
  );
  // A fresh stamped current-term no-op (now under LeaseGuard) establishes the anchor.
  let last = log.last_index();
  ep.append_leader_noop(now, &mut log, last);
  ep.handle_storage(t0, &mut log, &mut stable);
  assert!(
    ep.lease_guard_read_live(now, &log),
    "lease live once a fresh stamped current-term anchor commits"
  );
}

/// Applying a SetReadMode revokes any live LeaseBased lease (its granting quorum may not match the new
/// mode) — mirror the ConfChange revocation.
#[test]
fn flip_revokes_leasebased_lease() {
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_lease_duration(Duration::from_millis(300))
  .with_clock_drift_bound(Duration::from_millis(50));
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::NoopStable::default();
  let t0 = ep.poll_timeout().unwrap();
  ep.handle_timeout(t0, &mut log, &mut stable);
  ep.handle_storage(t0, &mut log, &mut stable);
  assert!(ep.role().is_leader());
  ep.handle_storage(t0, &mut log, &mut stable);
  while ep.poll_event().is_some() {}
  while ep.poll_message().is_some() {}

  // Simulate a live LeaseBased lease, then flip the mode.
  ep.set_lease_valid_until_for_test(Some(t0 + Duration::from_secs(10)));
  assert!(ep.lease_valid_until_for_test().is_some());
  let now = crate::Now::monotonic(t0);
  ep.propose_read_mode_change(now, &mut log, &stable, crate::ReadOnlyOption::LeaseGuard)
    .expect("proposed");
  ep.handle_storage(t0, &mut log, &mut stable);
  assert_eq!(
    ep.lease_valid_until_for_test(),
    None,
    "the apply-time flip revokes the LeaseBased lease"
  );
}

/// A proposal to a mode this leader is not configured for is rejected at propose time (nothing appended).
#[test]
fn propose_rejects_unconfigured_target() {
  // A Safe leader with NO LeaseGuard knobs and NO check_quorum.
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::NoopStable::default();
  let t0 = ep.poll_timeout().unwrap();
  ep.handle_timeout(t0, &mut log, &mut stable);
  ep.handle_storage(t0, &mut log, &mut stable);
  assert!(ep.role().is_leader());
  ep.handle_storage(t0, &mut log, &mut stable);
  while ep.poll_event().is_some() {}
  while ep.poll_message().is_some() {}

  let now = crate::Now::monotonic(t0);
  assert_eq!(
    ep.propose_read_mode_change(now, &mut log, &stable, crate::ReadOnlyOption::LeaseGuard),
    Err(crate::ProposeError::InvalidReadMode),
    "into-LeaseGuard without a configured lease window is rejected"
  );
  assert_eq!(
    ep.propose_read_mode_change(now, &mut log, &stable, crate::ReadOnlyOption::LeaseBased),
    Err(crate::ProposeError::InvalidReadMode),
    "into-LeaseBased without check_quorum is rejected"
  );
  assert!(
    ep.propose_read_mode_change(now, &mut log, &stable, crate::ReadOnlyOption::Safe)
      .is_ok(),
    "into-Safe always validates"
  );
}

/// Only one read-mode migration may be in flight at a time (mirror pending_conf_index).
#[test]
fn one_read_mode_change_in_flight() {
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_lease_duration(Duration::from_millis(300))
  .with_clock_drift_bound(Duration::from_millis(50));
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::NoopStable::default();
  let t0 = ep.poll_timeout().unwrap();
  ep.handle_timeout(t0, &mut log, &mut stable);
  ep.handle_storage(t0, &mut log, &mut stable);
  assert!(ep.role().is_leader());
  ep.handle_storage(t0, &mut log, &mut stable);
  while ep.poll_event().is_some() {}
  while ep.poll_message().is_some() {}

  let now = crate::Now::monotonic(t0);
  assert!(
    ep.propose_read_mode_change(now, &mut log, &stable, crate::ReadOnlyOption::LeaseGuard)
      .is_ok(),
    "the first migration is accepted"
  );
  assert_eq!(
    ep.propose_read_mode_change(now, &mut log, &stable, crate::ReadOnlyOption::Safe),
    Err(crate::ProposeError::ReadModeChangeInFlight),
    "a second migration before the first applies is rejected"
  );
  ep.handle_storage(t0, &mut log, &mut stable);
  assert!(
    ep.propose_read_mode_change(now, &mut log, &stable, crate::ReadOnlyOption::Safe)
      .is_ok(),
    "a new migration is accepted once the prior one applies"
  );
}

/// become_leader recomputes pending_read_mode_index to the last log index, so an inherited uncommitted
/// SetReadMode in a fresh leader's tail blocks a new migration until it commits-and-applies.
#[test]
fn pending_read_mode_recomputed_at_become_leader() {
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_lease_duration(Duration::from_millis(300))
  .with_clock_drift_bound(Duration::from_millis(50));
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::NoopStable::default();
  // Durable log: a committed Empty at 1, an UNCOMMITTED SetReadMode at 2 (commit = 1).
  log.force_append(&[
    crate::Entry::new(
      crate::Term::new(1),
      crate::Index::new(1),
      crate::EntryKind::Empty,
      bytes::Bytes::new(),
    ),
    crate::Entry::new(
      crate::Term::new(1),
      crate::Index::new(2),
      crate::EntryKind::SetReadMode,
      bytes::Bytes::copy_from_slice(&[crate::ReadOnlyOption::Safe.as_u8()]),
    ),
  ]);
  stable.force_state(crate::Term::new(1), Some(1u64), crate::Index::new(1));
  let mut ep = Endpoint::restart(
    cfg,
    Instant::ORIGIN,
    1,
    crate::testkit::CountSm::default(),
    1,
    &mut log,
    &mut stable,
  );
  // Win the election (single voter), inheriting the uncommitted SetReadMode at index 2.
  let t0 = ep.poll_timeout().unwrap();
  ep.handle_timeout(t0, &mut log, &mut stable);
  ep.handle_storage(t0, &mut log, &mut stable);
  assert!(ep.role().is_leader());
  let now = crate::Now::monotonic(t0);
  assert_eq!(
    ep.propose_read_mode_change(now, &mut log, &stable, crate::ReadOnlyOption::LeaseGuard),
    Err(crate::ProposeError::ReadModeChangeInFlight),
    "the inherited uncommitted SetReadMode blocks a new migration until it applies"
  );
}

/// REGRESSION (codex R1 [high]): a YOUNG leader (now.since_origin() < Δ) into-LeaseGuard must NOT serve
/// off the unstamped migration anchor. The Safe→LeaseGuard SetReadMode is stamped ts=0/window=0 under the
/// OLD Safe mode; the age gate alone (now − 0 < Δ) would serve it, but the window check degrades to Safe
/// until a stamped current-term anchor commits. (The earlier warm-up test only covered now > Δ, missing
/// young / forced-transfer leaders.)
#[test]
fn young_leader_into_leaseguard_degrades_off_unstamped_anchor() {
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_lease_duration(Duration::from_millis(300))
  .with_clock_drift_bound(Duration::from_millis(50));
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::NoopStable::default();
  let t0 = ep.poll_timeout().unwrap();
  ep.handle_timeout(t0, &mut log, &mut stable);
  ep.handle_storage(t0, &mut log, &mut stable);
  assert!(ep.role().is_leader());
  ep.handle_storage(t0, &mut log, &mut stable);
  while ep.poll_event().is_some() {}
  while ep.poll_message().is_some() {}

  let now = crate::Now::monotonic(t0);
  ep.propose_read_mode_change(now, &mut log, &stable, crate::ReadOnlyOption::LeaseGuard)
    .expect("proposed");
  ep.handle_storage(t0, &mut log, &mut stable);
  assert_eq!(ep.active_read_mode(), crate::ReadOnlyOption::LeaseGuard);
  // The committed anchor is the unstamped SetReadMode entry (window=0). At a YOUNG now (since_origin < Δ),
  // where the age gate alone would serve, the read MUST still degrade — the anchor is not LeaseGuard-stamped.
  let young = crate::Now::monotonic(Instant::ORIGIN + Duration::from_millis(5));
  assert!(
    !ep.lease_guard_read_live(young, &log),
    "a young leader must NOT serve off the unstamped migration anchor (window=0)"
  );
}

/// REGRESSION (codex R1 [medium]): applying a SetReadMode must NOT discard in-flight accepted reads
/// (`set_option`, not `reset`). A read accepted before the flip stays pending and confirms under the
/// mode-INDEPENDENT ReadIndex quorum, instead of stranding the caller / a forwarding follower.
#[test]
fn mode_flip_preserves_inflight_accepted_read() {
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_lease_duration(Duration::from_millis(300))
  .with_clock_drift_bound(Duration::from_millis(50));
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
  let mut log = crate::testkit::VecLog::default();
  let mut stable = crate::testkit::NoopStable::default();
  let t0 = ep.poll_timeout().unwrap();
  ep.handle_timeout(t0, &mut log, &mut stable);
  ep.handle_storage(t0, &mut log, &mut stable);
  assert!(ep.role().is_leader());
  ep.handle_storage(t0, &mut log, &mut stable);
  while ep.poll_event().is_some() {}
  while ep.poll_message().is_some() {}

  // A read accepted (added at its commit index) and awaiting its heartbeat quorum.
  ep.inject_pending_read_for_test(bytes::Bytes::from_static(b"r"));
  assert_eq!(
    ep.pending_read_count(),
    1,
    "the read is accepted and pending"
  );
  // Flip the mode; the apply must NOT discard the accepted read.
  let now = crate::Now::monotonic(t0);
  ep.propose_read_mode_change(now, &mut log, &stable, crate::ReadOnlyOption::LeaseGuard)
    .expect("proposed");
  ep.handle_storage(t0, &mut log, &mut stable);
  assert_eq!(ep.active_read_mode(), crate::ReadOnlyOption::LeaseGuard);
  assert_eq!(
    ep.pending_read_count(),
    1,
    "the apply-time flip must preserve the in-flight accepted read (set_option, not reset)"
  );
}
