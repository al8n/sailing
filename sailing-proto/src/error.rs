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
  /// `max_inflight_msgs` was zero.
  #[error("max_inflight_msgs must be greater than zero")]
  ZeroInflight,
  /// `ReadOnlyOption::LeaseBased` requires `check_quorum = true` (the lease safety depends on
  /// the leader knowing it still holds a quorum; without CheckQuorum that guarantee is absent).
  #[error("ReadOnlyOption::LeaseBased requires check_quorum to be enabled")]
  LeaseRequiresCheckQuorum,
}
