//! Endpoint configuration. Tuning is real `Duration` (not logical ticks); the election
//! timeout is randomized per term from the seeded PRNG inside the `Endpoint`.
use crate::error::ConfigError;
use core::time::Duration;

/// Static configuration for an [`crate::Endpoint`]. Generic param carries no bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config<I> {
  id: I,
  election_timeout: Duration,
  heartbeat_interval: Duration,
}

impl<I: Copy> Config<I> {
  /// Construct, validating that `election_timeout > heartbeat_interval > 0`.
  pub fn try_new(
    id: I,
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
    Ok(Self { id, election_timeout, heartbeat_interval })
  }

  /// This node's id.
  #[inline(always)]
  pub const fn id(&self) -> I {
    self.id
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
}

#[cfg(test)]
mod tests {
  use super::*;
  use core::time::Duration;

  #[test]
  fn config_validation_and_defaults() {
    let c =
      Config::try_new(1u64, Duration::from_millis(1000), Duration::from_millis(100)).unwrap();
    assert_eq!(c.id(), 1u64);
    assert_eq!(c.heartbeat_interval(), Duration::from_millis(100));
    // election timeout must exceed heartbeat interval
    assert!(matches!(
      Config::try_new(1u64, Duration::from_millis(50), Duration::from_millis(100)),
      Err(ConfigError::ElectionNotGreaterThanHeartbeat { .. })
    ));
  }
}
