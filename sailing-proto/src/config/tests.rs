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
fn read_only_option_u8_round_trips() {
  for m in [
    ReadOnlyOption::Safe,
    ReadOnlyOption::LeaseBased,
    ReadOnlyOption::LeaseGuard,
  ] {
    assert_eq!(
      ReadOnlyOption::from_u8(m.as_u8()),
      Some(m),
      "round-trips: {m:?}"
    );
  }
  assert_eq!(
    ReadOnlyOption::from_u8(3),
    None,
    "unknown discriminant rejected"
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
fn validate_rejects_election_timeout_near_duration_max() {
  // The per-term randomized election timeout is `election_timeout + Duration::from_millis(jitter)`, a
  // raw `Duration` add that PANICS on overflow; a near-`Duration::MAX` value parsed into a config would
  // take the node down on its first election. `validate` (the parsed-path funnel) rejects it above the
  // `Instant`-safe bound — the SAME bound the constructors now enforce, so we inject the over-bound
  // value past `try_new`'s own check (via the test-only setter) to assert `validate` is also a gate.
  // Falsify: remove the `election_timeout > MAX_ELECTION_TIMEOUT` check in `validate` and it succeeds.
  let mut over = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_secs(1),
    Duration::from_millis(100),
  )
  .unwrap();
  over.set_election_timeout_for_test(MAX_ELECTION_TIMEOUT + Duration::from_secs(1));
  assert!(matches!(
    over.validate(),
    Err(ConfigError::ElectionTimeoutTooLarge { .. })
  ));
  // The bound itself validates (paired with a heartbeat strictly under it).
  let at = Config::try_new(
    1u64,
    std::vec![1u64],
    MAX_ELECTION_TIMEOUT,
    Duration::from_millis(100),
  )
  .unwrap();
  assert!(at.validate().is_ok());
}

#[test]
fn try_new_rejects_election_timeout_above_bound() {
  // `Endpoint::new` arms the election timer WITHOUT calling `validate`, so the `Instant`-safe bound
  // must hold at the programmatic constructor too — otherwise `try_new(election_timeout = huge)`
  // returns `Ok` and the node panics on its first election (the raw `Duration` add overflows).
  // Falsify: drop the `> MAX_ELECTION_TIMEOUT` check in `try_new` and the first two asserts return Ok.
  assert!(matches!(
    Config::try_new(
      1u64,
      std::vec![1u64],
      MAX_ELECTION_TIMEOUT + Duration::from_secs(1),
      Duration::from_millis(100),
    ),
    Err(ConfigError::ElectionTimeoutTooLarge { .. })
  ));
  // A value near `Duration::MAX` (the original panic trigger) is likewise rejected, not constructed.
  assert!(matches!(
    Config::try_new(
      1u64,
      std::vec![1u64],
      Duration::MAX,
      Duration::from_millis(100),
    ),
    Err(ConfigError::ElectionTimeoutTooLarge { .. })
  ));
  // The bound itself, and any realistic timeout, still construct (no behavior change for real inputs).
  assert!(
    Config::try_new(
      1u64,
      std::vec![1u64],
      MAX_ELECTION_TIMEOUT,
      Duration::from_millis(100),
    )
    .is_ok()
  );
  assert!(
    Config::try_new(
      1u64,
      std::vec![1u64],
      Duration::from_secs(1),
      Duration::from_millis(100),
    )
    .is_ok()
  );
}

#[test]
fn try_new_observer_rejects_election_timeout_above_bound() {
  // The observer constructor arms the same election timer (an observer that becomes a voter mid-run
  // campaigns), so it carries the identical `Instant`-safe bound. The observer's `id ∉ voters` by
  // design, so the bound is checked before — and independently of — the voter membership rule.
  // Falsify: drop the `> MAX_ELECTION_TIMEOUT` check in `try_new_observer` and the rejects return Ok.
  assert!(matches!(
    Config::try_new_observer(
      9u64,
      std::vec![1u64, 2],
      MAX_ELECTION_TIMEOUT + Duration::from_secs(1),
      Duration::from_millis(100),
    ),
    Err(ConfigError::ElectionTimeoutTooLarge { .. })
  ));
  assert!(matches!(
    Config::try_new_observer(
      9u64,
      std::vec![1u64, 2],
      Duration::MAX,
      Duration::from_millis(100),
    ),
    Err(ConfigError::ElectionTimeoutTooLarge { .. })
  ));
  // The bound itself, and a realistic timeout, still construct an observer.
  assert!(
    Config::try_new_observer(
      9u64,
      std::vec![1u64, 2],
      MAX_ELECTION_TIMEOUT,
      Duration::from_millis(100),
    )
    .is_ok()
  );
  assert!(
    Config::try_new_observer(
      9u64,
      std::vec![1u64, 2],
      Duration::from_secs(1),
      Duration::from_millis(100),
    )
    .is_ok()
  );
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
  assert_eq!(c.max_size_per_msg(), 1024 * 1024);
  assert_eq!(c.max_inflight_msgs(), 256);
  assert_eq!(c.max_inflight_bytes(), 0); // 0 = uncapped

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
fn validate_lease_refresh_requires_leaseguard() {
  use crate::{ConfigError, LeaseRefresh, ReadOnlyOption};
  let base = || {
    Config::try_new(
      1u64,
      std::vec![1u64, 2, 3],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap()
  };
  // A valid LeaseGuard config: window 300·350/250 = 420ms < 1000ms election timeout.
  let leaseguard = || {
    base()
      .with_read_only(ReadOnlyOption::LeaseGuard)
      .with_lease_duration(Duration::from_millis(300))
      .with_clock_drift_bound(Duration::from_millis(50))
  };

  // The default is Off, and Off validates in every read mode.
  assert_eq!(base().lease_refresh(), LeaseRefresh::Off);
  assert!(base().validate().is_ok());
  assert!(
    base()
      .with_lease_refresh(LeaseRefresh::Off)
      .validate()
      .is_ok()
  );
  assert!(
    leaseguard()
      .with_lease_refresh(LeaseRefresh::Off)
      .validate()
      .is_ok()
  );

  // A proactive mode under LeaseGuard validates.
  assert!(
    leaseguard()
      .with_lease_refresh(LeaseRefresh::OnExpiry)
      .validate()
      .is_ok()
  );
  assert!(
    leaseguard()
      .with_lease_refresh(LeaseRefresh::Continuous)
      .validate()
      .is_ok()
  );

  // A proactive mode outside LeaseGuard (Safe / LeaseBased) is rejected.
  assert!(matches!(
    base().with_lease_refresh(LeaseRefresh::OnExpiry).validate(),
    Err(ConfigError::LeaseRefreshRequiresLeaseGuard)
  ));
  assert!(matches!(
    base()
      .with_read_only(ReadOnlyOption::LeaseBased)
      .with_check_quorum(true)
      .with_lease_refresh(LeaseRefresh::Continuous)
      .validate(),
    Err(ConfigError::LeaseRefreshRequiresLeaseGuard)
  ));
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
  assert_eq!(
    c.leaseguard_commit_wait_ns(c.read_only()),
    Some(420_000_000)
  );

  // A window that overflows the u64 wire field (an absurd multi-century lease) is rejected, NOT
  // silently truncated to a small value. The election_timeout is the largest `Instant`-safe value
  // (`MAX_ELECTION_TIMEOUT`) so the LeaseGuard window check — not the election-bound check — is the
  // gate that fires on the overflowing lease.
  let huge = Config::try_new(
    1u64,
    std::vec![1u64],
    MAX_ELECTION_TIMEOUT,
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
  assert_eq!(huge.leaseguard_commit_wait_ns(huge.read_only()), None);

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

#[cfg(feature = "serde")]
#[test]
fn config_serde_roundtrip_and_partial() {
  // A full round-trip: the required id/voters/timeouts plus several knobs (including an enum and
  // an Option<Duration>) survive a JSON encode/decode.
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_max_size_per_msg(2048)
  .with_snapshot_threshold(50)
  .with_check_quorum(true)
  .with_read_only(ReadOnlyOption::LeaseGuard)
  .with_lease_duration(Duration::from_millis(300))
  .with_clock_drift_bound(Duration::from_millis(50));

  let json = serde_json::to_string(&cfg).unwrap();
  // Durations render as humantime strings, not {"secs":..,"nanos":..}.
  assert!(
    json.contains("\"election_timeout\":\"1s\""),
    "json = {json}"
  );
  assert!(
    json.contains("\"read_only\":\"lease_guard\""),
    "json = {json}"
  );
  assert!(
    json.contains("\"lease_duration\":\"300ms\""),
    "json = {json}"
  );

  let back: Config<u64> = serde_json::from_str(&json).unwrap();
  assert_eq!(back, cfg);
  assert_eq!(back.id(), 1u64);
  assert_eq!(back.voters(), &[1, 2, 3]);
  assert_eq!(back.election_timeout(), Duration::from_millis(1000));
  assert_eq!(back.max_size_per_msg(), 2048);
  assert_eq!(back.read_only(), ReadOnlyOption::LeaseGuard);
  assert_eq!(back.lease_duration(), Some(Duration::from_millis(300)));
  assert_eq!(back.clock_drift_bound(), Some(Duration::from_millis(50)));

  // A PARTIAL config carries ONLY the required `id` / `voters`; the two timeouts AND every knob fall
  // back to their DEFAULT_* (this is what proves the per-knob serde(default), now including the
  // timeouts). The only required serde fields are `id` + `voters`.
  let partial: Config<u64> = serde_json::from_str(r#"{"id":7,"voters":[7,8]}"#).unwrap();
  assert_eq!(partial.id(), 7u64);
  assert_eq!(partial.voters(), &[7, 8]);
  assert_eq!(partial.election_timeout(), DEFAULT_ELECTION_TIMEOUT);
  assert_eq!(partial.heartbeat_interval(), DEFAULT_HEARTBEAT_INTERVAL);
  assert_eq!(partial.max_size_per_msg(), DEFAULT_MAX_SIZE_PER_MSG);
  assert_eq!(partial.max_inflight_msgs(), DEFAULT_MAX_INFLIGHT_MSGS);
  assert_eq!(partial.max_inflight_bytes(), DEFAULT_MAX_INFLIGHT_BYTES);
  assert_eq!(partial.snapshot_threshold(), DEFAULT_SNAPSHOT_THRESHOLD);
  assert_eq!(partial.step_down_on_removal(), DEFAULT_STEP_DOWN_ON_REMOVAL);
  assert_eq!(partial.pre_vote(), DEFAULT_PRE_VOTE);
  assert_eq!(partial.check_quorum(), DEFAULT_CHECK_QUORUM);
  assert_eq!(
    partial.disable_proposal_forwarding(),
    DEFAULT_DISABLE_PROPOSAL_FORWARDING
  );
  assert_eq!(partial.read_only(), DEFAULT_READ_ONLY);
  assert_eq!(partial.lease_refresh(), DEFAULT_LEASE_REFRESH);
  assert_eq!(partial.lease_duration(), DEFAULT_LEASE_DURATION);
  assert_eq!(partial.clock_drift_bound(), DEFAULT_CLOCK_DRIFT_BOUND);
  assert_eq!(
    partial.bounded_clock_uncertainty(),
    DEFAULT_BOUNDED_CLOCK_UNCERTAINTY
  );

  // deny_unknown_fields: a misspelled knob is rejected, not silently dropped.
  assert!(
    serde_json::from_str::<Config<u64>>(
      r#"{"id":1,"voters":[1],"election_timeout":"1s","heartbeat_interval":"100ms","snapshot_treshold":5}"#
    )
    .is_err()
  );
}

#[cfg(feature = "serde")]
#[test]
fn config_serde_rejects_invalid() {
  // A deserialized config is VALIDATED (routed through ConfigCli + Config::validate), so a config
  // file carrying a value the programmatic constructors reject is an Err at deserialize time — it
  // never silently builds a node the engine would choke on.

  // max_inflight_msgs = 0 would stall replication.
  assert!(
    serde_json::from_str::<Config<u64>>(
      r#"{"id":1,"voters":[1],"election_timeout":"1s","heartbeat_interval":"100ms","max_inflight_msgs":0}"#
    )
    .is_err(),
    "max_inflight_msgs = 0 must be rejected"
  );

  // Zero heartbeat_interval.
  assert!(
    serde_json::from_str::<Config<u64>>(
      r#"{"id":1,"voters":[1],"election_timeout":"1s","heartbeat_interval":"0s"}"#
    )
    .is_err(),
    "zero heartbeat_interval must be rejected"
  );

  // election_timeout <= heartbeat_interval wedges elections.
  assert!(
    serde_json::from_str::<Config<u64>>(
      r#"{"id":1,"voters":[1],"election_timeout":"100ms","heartbeat_interval":"100ms"}"#
    )
    .is_err(),
    "election_timeout == heartbeat_interval must be rejected"
  );

  // Empty voter set has no consensus group to bootstrap.
  assert!(
    serde_json::from_str::<Config<u64>>(
      r#"{"id":1,"voters":[],"election_timeout":"1s","heartbeat_interval":"100ms"}"#
    )
    .is_err(),
    "empty voters must be rejected"
  );

  // An invalid read-mode combo: LeaseBased without check_quorum.
  assert!(
    serde_json::from_str::<Config<u64>>(
      r#"{"id":1,"voters":[1],"election_timeout":"1s","heartbeat_interval":"100ms","read_only":"lease_based"}"#
    )
    .is_err(),
    "LeaseBased without check_quorum must be rejected"
  );

  // The same LeaseBased config WITH check_quorum is accepted — the reject is the invariant, not a
  // blanket rejection of the mode.
  assert!(
    serde_json::from_str::<Config<u64>>(
      r#"{"id":1,"voters":[1],"election_timeout":"1s","heartbeat_interval":"100ms","read_only":"lease_based","check_quorum":true}"#
    )
    .is_ok(),
    "LeaseBased WITH check_quorum is valid"
  );

  // An election_timeout near Duration::MAX would overflow the per-term randomized-timeout `Duration`
  // add and panic the first election; the validating funnel rejects it at deserialize time. The
  // literal (~3171 years) stays parseable but is far above the ~49.7-day Instant-safe bound; the exact
  // boundary case lives in the programmatic `validate_rejects_election_timeout_near_duration_max`.
  assert!(
    serde_json::from_str::<Config<u64>>(
      r#"{"id":1,"voters":[1],"election_timeout":"100000000000s","heartbeat_interval":"100ms"}"#
    )
    .is_err(),
    "election_timeout near Duration::MAX must be rejected"
  );

  // A parsed OBSERVER seed (id NOT in voters) is accepted — validate() enforces the universal
  // invariants but does NOT require id ∈ voters.
  let observer: Config<u64> = serde_json::from_str(
    r#"{"id":9,"voters":[1,2,3],"election_timeout":"1s","heartbeat_interval":"100ms"}"#,
  )
  .expect("a parsed observer seed (id ∉ voters) is valid");
  assert_eq!(observer.id(), 9u64);
  assert!(!observer.is_voter(9u64));
}

#[cfg(feature = "serde")]
#[test]
fn read_mode_enums_serde_snake_case() {
  // The enum spellings on the wire match their as_str() (safe/lease_based/lease_guard,
  // off/on_expiry/continuous).
  for (mode, name) in [
    (ReadOnlyOption::Safe, "safe"),
    (ReadOnlyOption::LeaseBased, "lease_based"),
    (ReadOnlyOption::LeaseGuard, "lease_guard"),
  ] {
    let json = serde_json::to_string(&mode).unwrap();
    assert_eq!(json, std::format!("\"{name}\""));
    assert_eq!(serde_json::from_str::<ReadOnlyOption>(&json).unwrap(), mode);
    assert_eq!(name, mode.as_str());
  }
  for (mode, name) in [
    (LeaseRefresh::Off, "off"),
    (LeaseRefresh::OnExpiry, "on_expiry"),
    (LeaseRefresh::Continuous, "continuous"),
  ] {
    let json = serde_json::to_string(&mode).unwrap();
    assert_eq!(json, std::format!("\"{name}\""));
    assert_eq!(serde_json::from_str::<LeaseRefresh>(&json).unwrap(), mode);
    assert_eq!(name, mode.as_str());
  }
}

#[cfg(feature = "clap")]
#[test]
fn config_clap_voters_accepts_a_delimited_list() {
  use clap::Parser;

  #[derive(Parser)]
  struct Cli {
    #[command(flatten)]
    config: Config<u64>,
  }

  // A single comma-delimited `--voter` value splits into the full set via `value_delimiter`. This
  // is the SAME split path `SAILING_VOTERS=1,2,3` takes, so the env-only multi-voter config works
  // (an env var is one string; without the delimiter it would parse as a single bogus id).
  let cli = Cli::try_parse_from([
    "app",
    "--id",
    "2",
    "--voter",
    "1,2,3",
    "--election-timeout",
    "1s",
    "--heartbeat-interval",
    "100ms",
  ])
  .unwrap();
  assert_eq!(cli.config.voters(), &[1, 2, 3]);
  assert_eq!(cli.config.id(), 2u64);
}

#[cfg(feature = "clap")]
#[test]
fn config_clap_composed_parse() {
  use clap::Parser;

  #[derive(Parser)]
  struct Cli {
    #[command(flatten)]
    config: Config<u64>,
  }

  // A composed parse with the required id/voters/timeouts plus a scalar knob and the enums —
  // proves there is no arg-id collision and that id, the repeated --voter, and the knobs parse.
  let cli = Cli::try_parse_from([
    "app",
    "--id",
    "1",
    "--voter",
    "1",
    "--voter",
    "2",
    "--voter",
    "3",
    "--election-timeout",
    "1s",
    "--heartbeat-interval",
    "100ms",
    "--max-size-per-msg",
    "2048",
    "--read-only",
    "lease_guard",
    "--lease-refresh",
    "on_expiry",
    "--lease-duration",
    "300ms",
    "--clock-drift-bound",
    "50ms",
  ])
  .unwrap();
  assert_eq!(cli.config.id(), 1u64);
  assert_eq!(cli.config.voters(), &[1, 2, 3]);
  assert_eq!(cli.config.election_timeout(), Duration::from_secs(1));
  assert_eq!(cli.config.heartbeat_interval(), Duration::from_millis(100));
  assert_eq!(cli.config.max_size_per_msg(), 2048);
  assert_eq!(cli.config.read_only(), ReadOnlyOption::LeaseGuard);
  assert_eq!(cli.config.lease_refresh(), LeaseRefresh::OnExpiry);
  assert_eq!(
    cli.config.lease_duration(),
    Some(Duration::from_millis(300))
  );
  assert_eq!(
    cli.config.clock_drift_bound(),
    Some(Duration::from_millis(50))
  );

  // Unspecified knobs default; only the required id/voters are supplied — the two timeouts now
  // default too (no `--election-timeout` / `--heartbeat-interval` on the line), so they land on
  // their DEFAULT_* exactly like the serde partial config.
  let def = Cli::try_parse_from(["app", "--id", "1", "--voter", "1"]).unwrap();
  assert_eq!(def.config.election_timeout(), DEFAULT_ELECTION_TIMEOUT);
  assert_eq!(def.config.heartbeat_interval(), DEFAULT_HEARTBEAT_INTERVAL);
  assert_eq!(def.config.max_size_per_msg(), DEFAULT_MAX_SIZE_PER_MSG);
  assert_eq!(def.config.snapshot_threshold(), DEFAULT_SNAPSHOT_THRESHOLD);
  assert_eq!(def.config.read_only(), ReadOnlyOption::Safe);
  assert_eq!(def.config.lease_refresh(), LeaseRefresh::Off);
  assert_eq!(def.config.lease_duration(), None);

  // A CLI-built Config bypasses try_new but is validated downstream by validate(): a valid
  // LeaseGuard config (window 300·350/250 = 420ms < 1000ms) passes.
  assert!(cli.config.validate().is_ok());
}

#[cfg(feature = "clap")]
#[test]
fn config_clap_wires_env_and_value_enum() {
  use clap::CommandFactory;

  #[derive(clap::Parser)]
  struct Cli {
    #[command(flatten)]
    config: Config<u64>,
  }

  let cmd = Cli::command();
  // Env is wired (introspect the registered command — never std::env::set_var).
  let id_arg = cmd
    .get_arguments()
    .find(|a| a.get_id().as_str() == "config-id")
    .expect("config-id arg is registered");
  assert_eq!(
    id_arg.get_env().and_then(|e| e.to_str()),
    Some("SAILING_ID")
  );

  let knob = cmd
    .get_arguments()
    .find(|a| a.get_id().as_str() == "config-snapshot-threshold")
    .expect("config-snapshot-threshold arg is registered");
  assert_eq!(
    knob.get_env().and_then(|e| e.to_str()),
    Some("SAILING_SNAPSHOT_THRESHOLD")
  );

  // The read-mode enum exposes its snake_case possible-values (matching as_str()/serde).
  let read_only = cmd
    .get_arguments()
    .find(|a| a.get_id().as_str() == "config-read-only")
    .expect("config-read-only arg is registered");
  let possible: std::vec::Vec<String> = read_only
    .get_possible_values()
    .iter()
    .map(|p| p.get_name().to_string())
    .collect();
  assert!(
    possible.iter().any(|p| p == "lease_based"),
    "possible values = {possible:?}"
  );
  assert!(possible.iter().any(|p| p == "lease_guard"));
}

#[cfg(feature = "clap")]
#[test]
fn config_clap_update_preserves_unoverridden_knobs() {
  use clap::Parser;

  #[derive(Parser)]
  struct Cli {
    #[command(flatten)]
    config: Config<u64>,
  }

  let base = || {
    Config::try_new(
      1u64,
      std::vec![1u64, 2, 3],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap()
    .with_max_size_per_msg(2048)
    .with_snapshot_threshold(50)
  };

  // A partial update (only --check-quorum) leaves the other knobs intact rather than resetting
  // them to clap defaults — the take! ValueSource gate is what makes this hold.
  let mut cli = Cli { config: base() };
  cli
    .try_update_from(["app", "--check-quorum", "true"])
    .expect("update");
  assert!(cli.config.check_quorum(), "the override is applied");
  assert_eq!(
    cli.config.max_size_per_msg(),
    2048,
    "non-default max_size_per_msg survives"
  );
  assert_eq!(
    cli.config.snapshot_threshold(),
    50,
    "non-default snapshot_threshold survives"
  );
  assert_eq!(cli.config.voters(), &[1, 2, 3], "voters survive");
}

#[cfg(feature = "clap")]
#[test]
fn config_clap_update_is_transactional() {
  use clap::Parser;

  #[derive(Parser)]
  struct Cli {
    #[command(flatten)]
    config: Config<u64>,
  }

  // A REJECTED update (an override that makes the config invalid) must leave the original config
  // byte-for-byte unchanged — the override is applied to a candidate clone and only committed after
  // it validates, so a caller that catches the clap error and keeps its config never holds a
  // half-applied invalid `Config`.
  let original = Config::try_new(
    1u64,
    std::vec![1u64, 2, 3],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_max_inflight_msgs(8)
  .unwrap()
  .with_snapshot_threshold(50);

  // (a) a knob-level invalid update: max_inflight_msgs = 0 would stall replication.
  let mut cli = Cli {
    config: original.clone(),
  };
  let err = cli.try_update_from(["app", "--max-inflight-msgs", "0"]);
  assert!(err.is_err(), "max_inflight_msgs = 0 must be rejected");
  assert_eq!(
    cli.config, original,
    "a rejected update leaves the config byte-for-byte unchanged"
  );

  // (b) a cross-field invalid update: an election timeout no longer above the heartbeat.
  let mut cli = Cli {
    config: original.clone(),
  };
  let err = cli.try_update_from(["app", "--election-timeout", "50ms"]);
  assert!(
    err.is_err(),
    "election_timeout <= heartbeat_interval must be rejected"
  );
  assert_eq!(
    cli.config, original,
    "the partially-applied override must not poison the config"
  );

  // A VALID update still applies (the transaction commits on success).
  let mut cli = Cli {
    config: original.clone(),
  };
  cli
    .try_update_from(["app", "--max-inflight-msgs", "16"])
    .expect("a valid update applies");
  assert_eq!(cli.config.max_inflight_msgs(), 16, "the override applied");
  assert_eq!(
    cli.config.snapshot_threshold(),
    50,
    "unoverridden knobs still survive"
  );
}

#[cfg(feature = "clap")]
#[test]
fn config_clap_rejects_invalid() {
  use clap::Parser;

  #[derive(Parser)]
  struct Cli {
    #[command(flatten)]
    config: Config<u64>,
  }

  // A CLI/env-built config is VALIDATED at parse time (FromArgMatches routes through the validating
  // TryFrom), so an invalid combination is a clap parse Err — it never silently builds an unrunnable
  // node. Each case supplies the required id/voters/timeouts and then the single offending knob.
  let base = |extra: &[&str]| {
    let mut args = std::vec![
      "app",
      "--id",
      "1",
      "--voter",
      "1",
      "--election-timeout",
      "1s",
      "--heartbeat-interval",
      "100ms",
    ];
    args.extend_from_slice(extra);
    Cli::try_parse_from(args)
  };

  // max_inflight_msgs = 0 would stall replication.
  assert!(
    base(&["--max-inflight-msgs", "0"]).is_err(),
    "max_inflight_msgs = 0 must be rejected"
  );

  // Zero heartbeat_interval (override the base heartbeat to 0s).
  assert!(
    Cli::try_parse_from([
      "app",
      "--id",
      "1",
      "--voter",
      "1",
      "--election-timeout",
      "1s",
      "--heartbeat-interval",
      "0s",
    ])
    .is_err(),
    "zero heartbeat_interval must be rejected"
  );

  // election_timeout <= heartbeat_interval wedges elections.
  assert!(
    Cli::try_parse_from([
      "app",
      "--id",
      "1",
      "--voter",
      "1",
      "--election-timeout",
      "100ms",
      "--heartbeat-interval",
      "100ms",
    ])
    .is_err(),
    "election_timeout == heartbeat_interval must be rejected"
  );

  // election_timeout near Duration::MAX would overflow the per-term randomized-timeout `Duration` add
  // and panic the first election; clap surfaces the validation failure through its error path. The
  // literal (~3171 years) is far above the ~49.7-day Instant-safe bound (boundary case is programmatic).
  assert!(
    Cli::try_parse_from([
      "app",
      "--id",
      "1",
      "--voter",
      "1",
      "--election-timeout",
      "100000000000s",
      "--heartbeat-interval",
      "100ms",
    ])
    .is_err(),
    "election_timeout near Duration::MAX must be rejected"
  );

  // Empty voter set: supply no --voter at all (the others are present).
  assert!(
    Cli::try_parse_from([
      "app",
      "--id",
      "1",
      "--election-timeout",
      "1s",
      "--heartbeat-interval",
      "100ms",
    ])
    .is_err(),
    "empty voters must be rejected"
  );

  // An invalid read-mode combo: LeaseBased without check_quorum.
  assert!(
    base(&["--read-only", "lease_based"]).is_err(),
    "LeaseBased without check_quorum must be rejected"
  );

  // The same LeaseBased config WITH check_quorum parses — the reject is the invariant, not the mode.
  assert!(
    base(&["--read-only", "lease_based", "--check-quorum", "true"]).is_ok(),
    "LeaseBased WITH check_quorum is valid"
  );

  // A CLI-built OBSERVER seed (id ∉ voters) parses — validate() does not require id ∈ voters.
  let observer = Cli::try_parse_from([
    "app",
    "--id",
    "9",
    "--voter",
    "1",
    "--voter",
    "2",
    "--voter",
    "3",
    "--election-timeout",
    "1s",
    "--heartbeat-interval",
    "100ms",
  ])
  .expect("a CLI observer seed (id ∉ voters) is valid");
  assert_eq!(observer.config.id(), 9u64);
  assert!(!observer.config.is_voter(9u64));
}

#[cfg(feature = "serde")]
#[test]
fn config_serde_id_does_not_require_from_str() {
  use crate::CheapClone;
  use core::fmt::{self, Display};

  // A custom NodeId newtype that is everything a node id needs (Serialize + Deserialize + Ord + Hash
  // + Display + CheapClone) but DELIBERATELY does NOT implement `FromStr`. Because the serde mirror
  // (`ConfigSerde<I>`) is bounded only `I: Deserialize + Clone + PartialEq` — split from clap's
  // `ConfigCli<I>`, which is the one that needs `FromStr` for its value parsers — a `Config<NodeId>`
  // round-trips through serde even though `NodeId: FromStr` does not hold.
  #[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, serde::Serialize, serde::Deserialize,
  )]
  struct NodeId(u64);

  impl Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
      Display::fmt(&self.0, f)
    }
  }

  // The one-line default-bodied impl (`cheap_clone()` = `clone()`); `NodeId` is `Copy` so this is O(1).
  impl CheapClone for NodeId {}

  // NOTE: `NodeId` deliberately has NO `impl FromStr`. That is the whole point — if the serde path
  // still required `FromStr` (as it did before the `ConfigSerde` / `ConfigCli` mirror split), the
  // `serde_json::{to_string,from_str}` calls below would FAIL TO COMPILE for `Config<NodeId>`. This
  // test compiling is the proof that the `serde` feature no longer imposes `FromStr` on the id.

  let cfg = Config::try_new(
    NodeId(1),
    std::vec![NodeId(1), NodeId(2), NodeId(3)],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_snapshot_threshold(42)
  .with_read_only(ReadOnlyOption::LeaseGuard)
  .with_lease_duration(Duration::from_millis(300))
  .with_clock_drift_bound(Duration::from_millis(50));

  // Serialize (direct derive — needs only `I: Serialize`) then deserialize (routes through
  // `ConfigSerde<NodeId>` + the validating `TryFrom` — needs only `I: Deserialize + Clone + PartialEq`).
  let json = serde_json::to_string(&cfg).unwrap();
  let back: Config<NodeId> = serde_json::from_str(&json).unwrap();
  assert_eq!(back, cfg);
  assert_eq!(back.id(), NodeId(1));
  assert_eq!(back.voters(), &[NodeId(1), NodeId(2), NodeId(3)]);
  assert_eq!(back.snapshot_threshold(), 42);
  assert_eq!(back.read_only(), ReadOnlyOption::LeaseGuard);

  // The validating funnel still fires on this id type: an invalid deserialized config is rejected.
  assert!(
    serde_json::from_str::<Config<NodeId>>(
      r#"{"id":1,"voters":[1],"election_timeout":"1s","heartbeat_interval":"100ms","max_inflight_msgs":0}"#
    )
    .is_err(),
    "the serde validation funnel still rejects an invalid config for a non-FromStr id"
  );
}
