use super::*;
use core::time::Duration;

#[test]
fn quorum_and_voters() {
  let c = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  assert_eq!(c.quorum(), 2);
  assert_eq!(c.voters(), &[1, 2, 3]);
  assert!(c.is_voter(2u64));
  // id must be among voters
  assert!(
    Config::try_new(
      9u64,
      std::vec![1u64, 2],
      Duration::from_millis(1000),
      Duration::from_millis(100)
    )
    .is_err()
  );
}

#[test]
fn config_validation_and_defaults() {
  let c = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  assert_eq!(c.id(), 1u64);
  assert_eq!(c.heartbeat_interval(), Duration::from_millis(100));
  // election timeout must exceed heartbeat interval
  assert!(matches!(
    Config::try_new(
      1u64,
      std::vec![1u64],
      Duration::from_millis(50),
      Duration::from_millis(100)
    ),
    Err(ConfigError::ElectionNotGreaterThanHeartbeat { .. })
  ));
}

#[test]
fn step_down_on_removal_default_and_override() {
  let c = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  assert!(
    c.step_down_on_removal(),
    "step_down_on_removal must default to true"
  );
  let c2 = c.with_step_down_on_removal(false);
  assert!(
    !c2.step_down_on_removal(),
    "with_step_down_on_removal(false) must persist"
  );
  let c3 = c2.with_step_down_on_removal(true);
  assert!(
    c3.step_down_on_removal(),
    "with_step_down_on_removal(true) must persist"
  );
}

#[test]
fn snapshot_threshold_default_and_override() {
  let c = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  assert_eq!(c.snapshot_threshold(), 10_000, "default should be 10_000");
  let c2 = c.with_snapshot_threshold(50);
  assert_eq!(c2.snapshot_threshold(), 50);
}

#[test]
fn flow_control_defaults_and_validation() {
  let c = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  // Defaults: 1 MiB per msg, 256 in-flight msgs, 0 (uncapped) bytes.
  assert_eq!(c.max_size_per_msg(), 1024 * 1024);
  assert_eq!(c.max_inflight_msgs(), 256);
  assert_eq!(c.max_inflight_bytes(), 0);

  // with_* builders work.
  let c2 = c
    .clone()
    .with_max_size_per_msg(512)
    .with_max_inflight_msgs(8)
    .unwrap()
    .with_max_inflight_bytes(4096);
  assert_eq!(c2.max_size_per_msg(), 512);
  assert_eq!(c2.max_inflight_msgs(), 8);
  assert_eq!(c2.max_inflight_bytes(), 4096);

  // ZeroInflight: max_inflight_msgs = 0 is rejected.
  assert!(matches!(
    c.clone().with_max_inflight_msgs(0),
    Err(ConfigError::ZeroInflight)
  ));
}

#[test]
fn pre_vote_default_and_override() {
  let c = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  assert!(!c.pre_vote(), "pre_vote must default to false");
  let c2 = c.with_pre_vote(true);
  assert!(c2.pre_vote(), "with_pre_vote(true) must persist");
  let c3 = c2.with_pre_vote(false);
  assert!(!c3.pre_vote(), "with_pre_vote(false) must persist");
}

#[test]
fn check_quorum_default_and_override() {
  let c = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  assert!(!c.check_quorum(), "check_quorum must default to false");
  let c2 = c.with_check_quorum(true);
  assert!(c2.check_quorum(), "with_check_quorum(true) must persist");
  let c3 = c2.with_check_quorum(false);
  assert!(!c3.check_quorum(), "with_check_quorum(false) must persist");
}

#[test]
fn disable_proposal_forwarding_default_and_override() {
  let c = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  assert!(
    !c.disable_proposal_forwarding(),
    "disable_proposal_forwarding must default to false"
  );
  let c2 = c.with_disable_proposal_forwarding(true);
  assert!(
    c2.disable_proposal_forwarding(),
    "with_disable_proposal_forwarding(true) must persist"
  );
}

#[test]
fn read_only_option_defaults_and_as_str() {
  // Default is Safe.
  let c = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  assert_eq!(c.read_only(), ReadOnlyOption::Safe);
  assert!(c.read_only().is_safe());
  assert_eq!(ReadOnlyOption::Safe.as_str(), "safe");
  assert_eq!(ReadOnlyOption::LeaseBased.as_str(), "lease_based");

  // Builder round-trip.
  let c2 = c.with_read_only(ReadOnlyOption::LeaseBased);
  assert_eq!(c2.read_only(), ReadOnlyOption::LeaseBased);
  assert!(c2.read_only().is_lease_based());
  let c3 = c2.with_read_only(ReadOnlyOption::Safe);
  assert_eq!(c3.read_only(), ReadOnlyOption::Safe);
}

#[test]
fn validate_lease_requires_check_quorum() {
  let base = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();

  // Safe + no check_quorum: always ok.
  assert!(base.clone().validate().is_ok());

  // Safe + check_quorum: ok.
  assert!(base.clone().with_check_quorum(true).validate().is_ok());

  // LeaseBased WITHOUT check_quorum: error.
  assert!(matches!(
    base
      .clone()
      .with_read_only(ReadOnlyOption::LeaseBased)
      .validate(),
    Err(ConfigError::LeaseRequiresCheckQuorum)
  ));

  // LeaseBased WITH check_quorum: ok.
  assert!(
    base
      .clone()
      .with_read_only(ReadOnlyOption::LeaseBased)
      .with_check_quorum(true)
      .validate()
      .is_ok()
  );
}

#[test]
fn leaseguard_config_validation() {
  use crate::{ConfigError, ReadOnlyOption};
  let base = || {
    Config::try_new(
      1u64,
      std::vec![1u64, 2, 3],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap()
  };

  // LeaseGuard without a lease_duration is rejected.
  let c = base().with_read_only(ReadOnlyOption::LeaseGuard);
  assert!(matches!(
    c.validate(),
    Err(ConfigError::LeaseGuardRequiresLeaseDuration)
  ));

  // LeaseGuard also requires a clock_drift_bound (the commit-wait needs it).
  let c = base()
    .with_read_only(ReadOnlyOption::LeaseGuard)
    .with_lease_duration(Duration::from_millis(400));
  assert!(matches!(
    c.validate(),
    Err(ConfigError::LeaseGuardRequiresDriftBound)
  ));

  // The EXACT commit-wait window Δ·(Δ+ε)/(Δ−ε) must be < election timeout.
  let c = base()
    .with_read_only(ReadOnlyOption::LeaseGuard)
    .with_lease_duration(Duration::from_millis(900))
    .with_clock_drift_bound(Duration::from_millis(200)); // 900*1100/700 ≈ 1414 >= 1000
  assert!(matches!(
    c.validate(),
    Err(ConfigError::LeaseTimingTooLong { .. })
  ));
  // The exact ratio binds tighter than the Δ+ε approximation: Δ+ε=900 < 1000, but 600*900/300=1800.
  let c = base()
    .with_read_only(ReadOnlyOption::LeaseGuard)
    .with_lease_duration(Duration::from_millis(600))
    .with_clock_drift_bound(Duration::from_millis(300));
  assert!(matches!(
    c.validate(),
    Err(ConfigError::LeaseTimingTooLong { .. })
  ));
  // clock_drift_bound >= lease_duration is degenerate (rate drift >= 100%) — rejected.
  let c = base()
    .with_read_only(ReadOnlyOption::LeaseGuard)
    .with_lease_duration(Duration::from_millis(200))
    .with_clock_drift_bound(Duration::from_millis(200));
  assert!(matches!(
    c.validate(),
    Err(ConfigError::LeaseTimingTooLong { .. })
  ));

  // The stamped window is the EXACT Δ·(Δ+ε)/(Δ−ε), not the Δ+2ε approximation: Δ=300ms, ε=50ms →
  // 300·350/250 = 420ms (the approximation would be 400ms — a stale-read-unsafe under-wait).
  let c = base()
    .with_read_only(ReadOnlyOption::LeaseGuard)
    .with_lease_duration(Duration::from_millis(300))
    .with_clock_drift_bound(Duration::from_millis(50));
  assert_eq!(c.leaseguard_commit_wait_ns(), Some(420_000_000));

  // A window that overflows the u64 wire field (an absurd multi-century lease) is rejected, NOT
  // silently truncated to a small value.
  let huge = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_secs(u64::MAX),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_read_only(ReadOnlyOption::LeaseGuard)
  .with_lease_duration(Duration::from_secs(20_000_000_000))
  .with_clock_drift_bound(Duration::from_secs(1));
  assert!(matches!(
    huge.validate(),
    Err(ConfigError::LeaseTimingTooLong { .. })
  ));
  assert_eq!(huge.leaseguard_commit_wait_ns(), None);

  // A valid LeaseGuard config — it does NOT require check_quorum (its safety is the commit-wait,
  // not election-prevention). bounded_clock_uncertainty is optional (enables inherited reads).
  let c = base()
    .with_read_only(ReadOnlyOption::LeaseGuard)
    .with_lease_duration(Duration::from_millis(400))
    .with_clock_drift_bound(Duration::from_millis(50));
  assert!(c.validate().is_ok());
  assert_eq!(c.lease_duration(), Some(Duration::from_millis(400)));
  assert_eq!(c.clock_drift_bound(), Some(Duration::from_millis(50)));
  assert_eq!(c.bounded_clock_uncertainty(), None);

  let c = c.with_bounded_clock_uncertainty(Duration::from_millis(10));
  assert!(c.validate().is_ok());
  assert_eq!(
    c.bounded_clock_uncertainty(),
    Some(Duration::from_millis(10))
  );

  // FAILOVER tier validation: bounded_clock_uncertainty requires LeaseGuard AND ε_unc < Δ.
  // (a) the skew bound set without LeaseGuard is rejected (it gates LeaseGuard-only failover paths).
  let bad = base()
    .with_read_only(ReadOnlyOption::Safe)
    .with_bounded_clock_uncertainty(Duration::from_millis(10));
  assert!(matches!(
    bad.validate(),
    Err(ConfigError::BoundedUncertaintyInvalid { .. })
  ));
  // (b) ε_unc ≥ Δ is rejected (it would make the cross-node age comparison vacuous).
  let bad = base()
    .with_read_only(ReadOnlyOption::LeaseGuard)
    .with_lease_duration(Duration::from_millis(300))
    .with_clock_drift_bound(Duration::from_millis(50))
    .with_bounded_clock_uncertainty(Duration::from_millis(300));
  assert!(matches!(
    bad.validate(),
    Err(ConfigError::BoundedUncertaintyInvalid { .. })
  ));

  assert_eq!(ReadOnlyOption::LeaseGuard.as_str(), "lease_guard");
}
