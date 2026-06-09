//! Endpoint configuration. Tuning is real `Duration` (not logical ticks); the election
//! timeout is randomized per term from the seeded PRNG inside the `Endpoint`.
use crate::{NodeId, error::ConfigError};
use core::time::Duration;
use std::vec::Vec;

/// Static configuration for an [`crate::Endpoint`]. Holds the initial voter set (dynamic
/// membership via `ConfChange` is M6). `Clone`, not `Copy` (it owns the voter list).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config<I> {
  id: I,
  voters: Vec<I>,
  election_timeout: Duration,
  heartbeat_interval: Duration,
  /// Maximum byte size of entries in a single `AppendEntries`. `u64::MAX` = unbounded;
  /// `0` = one entry per message.
  max_size_per_msg: u64,
  /// Maximum number of in-flight `AppendEntries` per peer. Must be > 0.
  max_inflight_msgs: usize,
  /// Maximum total in-flight bytes per peer (`0` = uncapped).
  max_inflight_bytes: u64,
  /// Number of committed entries between automatic snapshots (etcd's SnapshotCount default).
  snapshot_threshold: usize,
  /// When `true` (default), a leader that is removed or demoted to learner by a committed
  /// `ConfChange` steps down immediately (role → Follower, timers disarmed). Set to `false`
  /// only if the operator explicitly wants the removed leader to keep acting until it hears
  /// from a new leader (unusual; the default is safe).
  step_down_on_removal: bool,
}

impl<I: NodeId> Config<I> {
  /// Construct, validating timeouts and that `id` is among `voters`.
  pub fn try_new(
    id: I,
    voters: Vec<I>,
    election_timeout: Duration,
    heartbeat_interval: Duration,
  ) -> Result<Self, ConfigError> {
    if heartbeat_interval.is_zero() {
      return Err(ConfigError::ZeroHeartbeat);
    }
    if election_timeout <= heartbeat_interval {
      return Err(ConfigError::ElectionNotGreaterThanHeartbeat {
        election: election_timeout,
        heartbeat: heartbeat_interval,
      });
    }
    if !voters.contains(&id) {
      return Err(ConfigError::IdNotAVoter);
    }
    Ok(Self {
      id,
      voters,
      election_timeout,
      heartbeat_interval,
      max_size_per_msg: 1024 * 1024, // 1 MiB default
      max_inflight_msgs: 256,
      max_inflight_bytes: 0,
      snapshot_threshold: 10_000, // etcd default SnapshotCount
      step_down_on_removal: true,
    })
  }

  /// This node's id.
  #[inline(always)]
  pub const fn id(&self) -> I {
    self.id
  }

  /// The voter set.
  #[inline(always)]
  pub fn voters(&self) -> &[I] {
    &self.voters
  }

  /// Whether `id` is a voter.
  #[inline(always)]
  pub fn is_voter(&self, id: I) -> bool {
    self.voters.contains(&id)
  }

  /// Majority quorum size: `n/2 + 1`.
  #[inline(always)]
  pub fn quorum(&self) -> usize {
    self.voters.len() / 2 + 1
  }

  /// The base election timeout (randomized per term at runtime).
  #[inline(always)]
  pub const fn election_timeout(&self) -> Duration {
    self.election_timeout
  }

  /// The leader heartbeat interval.
  #[inline(always)]
  pub const fn heartbeat_interval(&self) -> Duration {
    self.heartbeat_interval
  }

  /// Maximum byte size of entries packed into a single `AppendEntries` (`u64::MAX` = unbounded).
  #[inline(always)]
  pub const fn max_size_per_msg(&self) -> u64 {
    self.max_size_per_msg
  }

  /// Maximum number of in-flight (un-acked) `AppendEntries` per peer.
  #[inline(always)]
  pub const fn max_inflight_msgs(&self) -> usize {
    self.max_inflight_msgs
  }

  /// Maximum total in-flight bytes per peer (`0` = uncapped).
  #[inline(always)]
  pub const fn max_inflight_bytes(&self) -> u64 {
    self.max_inflight_bytes
  }

  /// Override the `max_size_per_msg` knob.
  pub fn with_max_size_per_msg(mut self, v: u64) -> Self {
    self.max_size_per_msg = v;
    self
  }

  /// Override the `max_inflight_msgs` knob. Returns `Err(ConfigError::ZeroInflight)` if `v == 0`.
  pub fn with_max_inflight_msgs(mut self, v: usize) -> Result<Self, ConfigError> {
    if v == 0 {
      return Err(ConfigError::ZeroInflight);
    }
    self.max_inflight_msgs = v;
    Ok(self)
  }

  /// Override the `max_inflight_bytes` knob.
  pub fn with_max_inflight_bytes(mut self, v: u64) -> Self {
    self.max_inflight_bytes = v;
    self
  }

  /// Number of committed entries between automatic snapshots.
  #[inline(always)]
  pub const fn snapshot_threshold(&self) -> usize {
    self.snapshot_threshold
  }

  /// Override the `snapshot_threshold` knob.
  pub fn with_snapshot_threshold(mut self, v: usize) -> Self {
    self.snapshot_threshold = v;
    self
  }

  /// Whether a leader that loses its voter status (removed or demoted to learner) should
  /// step down immediately when the `ConfChange` is applied. Defaults to `true`.
  #[inline(always)]
  pub const fn step_down_on_removal(&self) -> bool {
    self.step_down_on_removal
  }

  /// Override the `step_down_on_removal` knob.
  pub fn with_step_down_on_removal(mut self, v: bool) -> Self {
    self.step_down_on_removal = v;
    self
  }
}

#[cfg(test)]
mod tests {
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
}
