//! Public error types for the core.

/// Why a proposal was rejected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ProposeError<I> {
  /// This node is not the leader; redirect to `leader` if known.
  #[error("not the leader")]
  NotLeader {
    /// The believed current leader, if known.
    leader: Option<I>,
  },
  /// A previous configuration change is still in flight (not yet applied). Only one
  /// `ConfChange` entry may be pending at a time — propose another after the first is
  /// committed and applied.
  #[error("a conf change is already in flight")]
  ConfChangeInFlight,
  /// A leader transfer is in progress; the leader is not accepting new proposals until
  /// the transfer completes or times out.
  #[error("a leader transfer is in progress")]
  LeaderTransferInProgress,
  /// The proposed configuration change is invalid for the current configuration (e.g. leaving a
  /// joint config while not in one, or an overlapping change). It was rejected at propose time —
  /// nothing was appended — rather than being committed and then poisoning the node on apply.
  #[error("the configuration change is invalid for the current configuration")]
  InvalidConfChange,
  /// The node has entered the permanent poisoned state (a fatal storage/apply error) and accepts no
  /// new work. The proposal was NOT appended or persisted; inspect `poison_reason()`.
  #[error("the node is poisoned and accepts no new proposals")]
  Poisoned,
  /// The log index space is exhausted (`last_index == u64::MAX`): no new entry can be allocated a
  /// strictly-greater index without aliasing the existing one. Unreachable by legitimate appends
  /// (2^64 entries); reachable only from a crafted or corrupt recovered log. Nothing was appended.
  #[error("the log index space is exhausted")]
  LogIndexExhausted,
}

/// Why a leader-transfer request was rejected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum TransferError<I> {
  /// This node is not the leader; a transfer can only be initiated by the current leader.
  #[error("not the leader")]
  NotLeader {
    /// The believed current leader, if known.
    leader: Option<I>,
  },
  /// The target node is not a voter in the current configuration and therefore cannot be
  /// elected leader.
  #[error("transfer target is not a voter")]
  NotAVoter,
  /// The target node is the current leader — no transfer needed.
  #[error("transfer target is already the leader")]
  AlreadyLeader,
  /// The node has entered the permanent poisoned state and accepts no new work. The transfer was
  /// NOT initiated; inspect `poison_reason()`.
  #[error("the node is poisoned and cannot initiate a transfer")]
  Poisoned,
}

/// Why a [`read_index`](crate::Endpoint::read_index) request could not be issued.
///
/// A `read_index` that returns `Ok(())` has been accepted onto a confirmation path (the
/// leader's heartbeat-quorum round, an immediate lease confirmation, or a forward to the
/// known leader); the eventual [`Event::ReadState`](crate::Event::ReadState) (locally) or
/// [`ReadIndexResp`](crate::ReadIndexResp) (when forwarded) is the only acknowledgement.
/// An `Err` means **no** such acknowledgement will ever arrive for this call, so the caller
/// must not block waiting for one.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ReadIndexError {
  /// This node is a follower with no known leader to forward the read to, so the request
  /// cannot be confirmed. Retry once a leader is known.
  #[error("no known leader to confirm the read")]
  NoLeader,
  /// This node is a follower and `disable_proposal_forwarding` is set, so the read cannot be
  /// forwarded to the leader. Issue the read on (or redirect it to) the leader directly.
  #[error("proposal forwarding is disabled; cannot forward the read to the leader")]
  ForwardingDisabled,
  /// A read with this exact `context` is already in flight. The `context` is the sole
  /// correlator between a request and its eventual `ReadState`/`ReadIndexResp`, so two
  /// concurrent reads MUST use distinct contexts (including the empty context). Wait for the
  /// in-flight read to confirm, or reissue with a unique context.
  #[error("a read with this context is already in flight")]
  DuplicateContext,
  /// This follower already has the maximum number of forwarded reads awaiting a `ReadIndexResp`
  /// (back-pressure). The read was NOT accepted; retry after some in-flight reads confirm, or once a
  /// leader/term change clears the backlog. Forwarded reads are never silently evicted, so an
  /// already-accepted read is never stranded.
  #[error("too many forwarded reads are already in flight")]
  TooManyInFlight,
  /// This node is poisoned (a fatal storage/log fault left its commit/applied view untrustworthy),
  /// so it suppresses all event emission. A read cannot be confirmed — no
  /// [`Event::ReadState`](crate::Event::ReadState) will ever arrive — so the request is rejected
  /// rather than silently accepted onto a path that never completes.
  #[error("node is poisoned; reads cannot be confirmed")]
  Poisoned,
}

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
  /// The configured `id` was not present in the voter set.
  #[error("id is not among the configured voters")]
  IdNotAVoter,
  /// `read_only = LeaseGuard` but no `lease_duration` was set.
  #[error("the LeaseGuard read mode requires a lease_duration")]
  LeaseGuardRequiresLeaseDuration,
  /// `read_only = LeaseGuard` but no `clock_drift_bound` was set (the commit-wait needs it).
  #[error("the LeaseGuard read mode requires a clock_drift_bound")]
  LeaseGuardRequiresDriftBound,
  /// The LeaseGuard commit-wait window `lease_duration·(lease_duration + clock_drift_bound) /
  /// (lease_duration − clock_drift_bound)` is invalid: `clock_drift_bound >= lease_duration`, the
  /// window overflows the `u64` wire field, or it is not strictly less than the election timeout (so
  /// a stale lease could outlive a new leader's election, or a fresh leader could never commit).
  #[error(
    "the LeaseGuard commit-wait window for lease_duration ({lease:?}) and clock_drift_bound ({drift:?}) is invalid (must have drift < lease and window < election timeout {election:?})"
  )]
  LeaseTimingTooLong {
    /// The configured lease window.
    lease: core::time::Duration,
    /// The configured clock-drift bound.
    drift: core::time::Duration,
    /// The election timeout it must stay under.
    election: core::time::Duration,
  },
  /// `max_inflight_msgs` was zero.
  #[error("max_inflight_msgs must be greater than zero")]
  ZeroInflight,
  /// `ReadOnlyOption::LeaseBased` requires `check_quorum = true` (the lease safety depends on
  /// the leader knowing it still holds a quorum; without CheckQuorum that guarantee is absent).
  #[error("ReadOnlyOption::LeaseBased requires check_quorum to be enabled")]
  LeaseRequiresCheckQuorum,
  /// `bounded_clock_uncertainty` (the LeaseGuard failover tier's synchronized-clock skew bound) was
  /// set without `read_only = LeaseGuard`, or it was not strictly less than `lease_duration`.
  #[error(
    "bounded_clock_uncertainty ({uncertainty:?}) requires read_only = LeaseGuard and must be < lease_duration ({lease:?})"
  )]
  BoundedUncertaintyInvalid {
    /// The configured bounded clock uncertainty (the failover skew bound).
    uncertainty: core::time::Duration,
    /// The configured lease duration it must be under (`None` if unset).
    lease: Option<core::time::Duration>,
  },
}
