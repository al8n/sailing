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

/// The LeaseGuard post-election commit-wait: a freshly-elected LeaseGuard leader HOLDS its first
/// commit (its own-term no-op) until `lease_duration + clock_drift_bound` past the election, even
/// with a quorum ack in hand, so any deposed leader's read-lease has provably expired before this
/// leader can commit (and begin serving lease reads). A `Safe` leader has no such wait.
#[test]
fn leaseguard_commit_wait_holds_first_commit_until_deadline() {
  use crate::{AppendResp, Config, Index, Instant, Message, Term, VoteResp};
  use core::time::Duration;

  // Drive a fresh leader to the point where peer 2 has acked the no-op at index 1, returning
  // `(endpoint, election_instant)`. `read_only` selects the mode; LeaseGuard also sets Δ=300ms,
  // ε_drift=50ms (so the wait is 350ms and Δ+ε_drift < the 1000ms election timeout).
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

  // Safe mode: the quorum ack commits the no-op immediately — no wait.
  let (safe, _log, _stable, _d) = elected_with_quorum_ack(crate::ReadOnlyOption::Safe);
  assert_eq!(
    safe.commit_index(),
    Index::new(1),
    "a Safe leader commits its no-op as soon as a quorum acks"
  );

  // LeaseGuard mode: the same quorum ack does NOT commit — the commit-wait holds it.
  let (mut lg, mut log, mut stable, d) = elected_with_quorum_ack(crate::ReadOnlyOption::LeaseGuard);
  assert_eq!(
    lg.commit_index(),
    Index::ZERO,
    "a LeaseGuard leader holds its first commit despite a quorum ack (commit-wait armed)"
  );

  // One nanosecond before the deadline (Δ+ε_drift = 350ms): still held. A timeout here fires the
  // heartbeat but NOT the commit-wait, so commit stays at ZERO.
  let just_before = d + Duration::from_nanos(350_000_000 - 1);
  lg.handle_timeout(just_before, &mut log, &mut stable);
  assert_eq!(
    lg.commit_index(),
    Index::ZERO,
    "the commit-wait must not release one nanosecond before the deadline"
  );

  // At exactly Δ+ε_drift past the election: the commit-wait fires and the no-op commits.
  let deadline = d + Duration::from_millis(350);
  lg.handle_timeout(deadline, &mut log, &mut stable);
  assert_eq!(
    lg.commit_index(),
    Index::new(1),
    "the commit-wait releases the first commit at lease_duration + clock_drift_bound"
  );
}
