//! Public error types for the core.

/// Why constructing a [`crate::Config`] failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ConfigError {
  /// `election_timeout` was not strictly greater than `heartbeat_interval`.
  #[error("election timeout ({election:?}) must exceed heartbeat interval ({heartbeat:?})")]
  ElectionNotGreaterThanHeartbeat {
    /// The rejected election timeout.
    election: core::time::Duration,
    /// The heartbeat interval it must exceed.
    heartbeat: core::time::Duration,
  },
  /// `heartbeat_interval` was zero.
  #[error("heartbeat interval must be non-zero")]
  ZeroHeartbeat,
}
