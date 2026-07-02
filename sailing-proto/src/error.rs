//! Public error types for the core.
use core::time::Duration;

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
  /// A previous read-mode migration is still in flight (not yet applied). Only one `SetReadMode` entry
  /// may be pending at a time — propose another after the first is committed and applied.
  #[error("a read-mode change is already in flight")]
  ReadModeChangeInFlight,
  /// The proposed read mode requires knobs this leader lacks: into-LeaseGuard needs a valid lease window
  /// (`lease_duration` + `clock_drift_bound`), into-LeaseBased needs `check_quorum`. Rejected at propose
  /// time — nothing appended — rather than committed and then degrading to Safe everywhere.
  #[error("the target read mode requires knobs this node lacks")]
  InvalidReadMode,
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

  /// The proposed entry is too large to ever fit in one transport frame. Accepting it would append a
  /// committed log entry that no `AppendEntries` could carry, so every follower's connection would
  /// close on each resend and replication would wedge cluster-wide. `size` is the entry's worst-case
  /// wire cost and `max` the per-frame entry budget, in bytes. Nothing was appended.
  #[error("the proposed entry is too large for one transport frame ({size} > {max} bytes)")]
  EntryTooLarge {
    /// The entry's worst-case encoded wire cost, in bytes.
    size: usize,
    /// The per-frame entry budget, in bytes.
    max: usize,
  },
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
/// [`ReadIndexResponse`](crate::ReadIndexResponse) (when forwarded) is the only acknowledgement.
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
  /// correlator between a request and its eventual `ReadState`/`ReadIndexResponse`, so two
  /// concurrent reads MUST use distinct contexts (including the empty context). Wait for the
  /// in-flight read to confirm, or reissue with a unique context.
  #[error("a read with this context is already in flight")]
  DuplicateContext,
  /// This follower already has the maximum number of forwarded reads awaiting a `ReadIndexResponse`
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
    election: Duration,
    /// The heartbeat interval it must exceed.
    heartbeat: Duration,
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
    lease: Duration,
    /// The configured clock-drift bound.
    drift: Duration,
    /// The election timeout it must stay under.
    election: Duration,
  },
  /// `max_inflight_msgs` was zero.
  #[error("max_inflight_msgs must be greater than zero")]
  ZeroInflight,
  /// `max_size_per_msg` was zero (which caps every `AppendEntries` at a single entry — a throughput
  /// footgun). The per-frame cap is enforced independently, so this is a sanity floor, not the frame
  /// bound.
  #[error("max_size_per_msg must be greater than zero")]
  ZeroMaxSizePerMsg,
  /// `snapshot_threshold` was zero. The snapshot trigger fires when `applied - first_index >=
  /// threshold`, so a zero threshold matches on every applied index and captures a full snapshot on
  /// every storage drain — a perpetual snapshot/compaction loop.
  #[error("snapshot_threshold must be greater than zero")]
  ZeroSnapshotThreshold,
  /// `snapshot_chunk_bytes` was zero (which would livelock on empty chunks) or exceeded the frame-safe
  /// maximum (which would produce an unsendable wire frame).
  #[error("snapshot_chunk_bytes must be in 1..={max} (got {value})")]
  SnapshotChunkBytesOutOfRange {
    /// The configured chunk size.
    value: u64,
    /// The frame-safe maximum (the configured `snapshot_chunk_bytes` upper bound).
    max: u64,
  },
  /// The voter set was empty. A config with no voters has no consensus group to bootstrap; the
  /// programmatic constructors reach this only via [`crate::Config::try_new`]'s `id ∈ voters`
  /// rejection, but a parsed (serde / clap) config could otherwise carry an empty `voters`.
  #[error("the voter set must not be empty")]
  EmptyVoters,
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
    uncertainty: Duration,
    /// The configured lease duration it must be under (`None` if unset).
    lease: Option<Duration>,
  },
  /// `lease_refresh` was set to a proactive mode (`OnExpiry` / `Continuous`) without
  /// `read_only = LeaseGuard`. Safe and LeaseBased reads have no per-entry timestamp anchor to refresh,
  /// so the knob is meaningless there.
  #[error("a proactive lease_refresh requires read_only = LeaseGuard")]
  LeaseRefreshRequiresLeaseGuard,
  /// `election_timeout` exceeds the `Instant`-safe bound. The per-term randomized timeout is
  /// `election_timeout + Duration::from_millis(rng % election_timeout_ms)` (a raw `Duration` add that
  /// PANICS on overflow), so a value near `Duration::MAX` parsed from a config would take the node down
  /// on its first election. Rejected above [`crate::Config`]'s `Instant`-safe election bound so the
  /// randomized draw (`< 2 · election_timeout`) can never overflow.
  #[error("election_timeout ({election:?}) must be at most the Instant-safe bound ({max:?})")]
  ElectionTimeoutTooLarge {
    /// The rejected election timeout.
    election: Duration,
    /// The `Instant`-safe maximum it must not exceed.
    max: Duration,
  },
}
