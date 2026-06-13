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
