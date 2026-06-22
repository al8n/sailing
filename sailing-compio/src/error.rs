//! The driver's typed error surface.

/// Why a [`Handle`](crate::Handle) operation did not produce a committed result.
///
/// Every variant is actionable by the caller; none is a silent drop. Raft loses uncommitted
/// proposals on leadership changes by design — the driver SURFACES that ([`Self::Superseded`])
/// rather than hiding it behind transparent retries it cannot make exactly-once.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum DriverError<I> {
  /// This node is not the leader; redirect to the hinted peer (when known) and retry there.
  #[error("not the leader (hint: {leader:?})")]
  NotLeader {
    /// The leader this node currently believes in, if any.
    leader: Option<I>,
  },
  /// The proposal was accepted into the log but a leadership change made its commitment
  /// unknowable before an `Applied` arrived — it may or may not survive. The caller decides
  /// whether to retry (the operation is NOT exactly-once across this boundary).
  #[error("leadership changed before the proposal's outcome was known")]
  Superseded,
  /// The submit budget (in-flight count or bytes) is exhausted; retry after in-flight
  /// operations complete.
  #[error("submit budget exhausted")]
  Busy,
  /// The consensus endpoint rejected the operation outright (a conf change already in flight,
  /// an invalid change, log exhaustion, forwarding disabled, …). Carries the proto's own
  /// description. A fail-stop is NOT in this bucket — it has its own variant
  /// ([`Self::Poisoned`]).
  #[error("rejected: {reason}")]
  Rejected {
    /// The endpoint's stated reason.
    reason: std::string::String,
  },
  /// The consensus endpoint fail-stopped (poisoned): an unrecoverable storage or apply fault
  /// made continuing unsafe. Everything parked fails with this, the driver exits its run loop,
  /// and the NODE must be restarted (possibly re-provisioned) by the operator — there is no
  /// in-process recovery from a poison by design.
  #[error("the consensus endpoint fail-stopped (poisoned)")]
  Poisoned,
  /// The driver is shutting down (or already gone); no further operations will commit.
  #[error("driver is shutting down")]
  ShuttingDown,
}

/// Why a driver `bind` did not start. Distinct from [`DriverError`] (a per-operation outcome): these
/// are one-time STARTUP faults, surfaced loudly rather than degrading silently.
#[derive(Debug, thiserror::Error)]
pub enum BindError {
  /// The OS socket bind failed.
  #[error("socket bind failed: {0}")]
  Io(#[from] std::io::Error),
  /// The raft `Config` is invalid (e.g. `ε_unc >= lease_duration`, a missing drift bound). The driver
  /// validates UP FRONT, so a misconfigured failover tier is a loud startup error rather than a
  /// silent fall back to Safe.
  #[error("invalid raft config: {0}")]
  Config(#[from] sailing_proto::ConfigError),
  /// The [`DriverConfig`](crate::DriverConfig) is invalid (e.g. a `max_inflight` whose `+ 1`
  /// command-channel sizing would trip `futures_channel`'s `MAX_BUFFER` assert, an over-bound redial).
  /// A programmatic `DriverConfig` is not validated at construction, so `bind` validates it UP FRONT —
  /// a loud startup error rather than a panic deeper in the channel/queue arithmetic.
  #[error("invalid driver config: {0}")]
  DriverConfig(#[from] crate::DriverConfigError),
  /// The `Config` is a valid LeaseGuard FAILOVER tier (`bounded_clock_uncertainty` set) but the
  /// driver's wall source `W` does not supply a wall (the default [`Monotonic`](crate::Monotonic)) —
  /// the failover tier would silently never fire. Bind via `bind_with_wall_clock` with a synchronized
  /// source such as [`NtpDisciplinedClock`](crate::NtpDisciplinedClock) (or remove
  /// `bounded_clock_uncertainty`).
  #[error(
    "the Config is a valid LeaseGuard failover tier but the driver's wall source supplies no wall \
     (the Monotonic default); use bind_with_wall_clock with NtpDisciplinedClock, or remove \
     bounded_clock_uncertainty"
  )]
  MissingWallSource,
}

/// Why a parsed [`DriverConfig`](crate::DriverConfig) is invalid.
///
/// The serde / clap parse paths route through [`DriverConfig::validate`](crate::DriverConfig::validate)
/// so a config-file or CLI/env value that would wedge a bounded queue or overflow a channel-capacity
/// computation is rejected at PARSE time rather than building a driver that panics or hot-loops at
/// run time. Each variant names the offending knob and the bound it violates. A
/// programmatically-constructed `DriverConfig` is NOT validated (it is the embedder's responsibility),
/// which is why the drivers ALSO harden the capacity arithmetic against extreme values.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum DriverConfigError {
  /// `max_inflight` is zero. The in-flight submit budget would admit nothing, so every submit is
  /// [`DriverError::Busy`] — the driver can never make progress.
  #[error("max_inflight must be non-zero")]
  ZeroMaxInflight,
  /// `max_inflight` is `usize::MAX`. The command channel is sized at `max_inflight + 1`, which would
  /// overflow `usize`; the cap is rejected so the addition can never wrap.
  #[error(
    "max_inflight must be less than usize::MAX (the command channel is sized at max_inflight + 1)"
  )]
  MaxInflightOverflow,
  /// `max_inflight` is at or above the channel-capacity ceiling. Both drivers size the command
  /// channel at `max_inflight + 1` via `futures_channel::mpsc::channel`, which ASSERTS the buffer is
  /// strictly below its `MAX_BUFFER` (≈ `usize::MAX / 4`) — rejecting `usize::MAX` alone is not enough,
  /// because a value like `usize::MAX / 2` clears that check yet still trips the channel's assert and
  /// panics `bind`. Capped at [`MAX_CHANNEL_CAPACITY`](crate::MAX_CHANNEL_CAPACITY) − 1 so the `+ 1`
  /// stays under the ceiling.
  #[error(
    "max_inflight + 1 must be at most the channel-capacity ceiling (futures_channel's MAX_BUFFER)"
  )]
  MaxInflightAboveChannelCeiling,
  /// `events_cap` is at or above the channel-capacity ceiling. The events tail is sized via a bounded
  /// channel; a capacity at/above the ceiling would trip the channel implementation's limit.
  #[error("events_cap must be at most the channel-capacity ceiling")]
  EventsCapAboveChannelCeiling,
  /// `recv_cap` is at or above the channel-capacity ceiling. The QUIC datagram channel is sized from
  /// it; a capacity at/above the ceiling would trip the channel implementation's limit.
  #[error("recv_cap must be at most the channel-capacity ceiling")]
  RecvCapAboveChannelCeiling,
  /// `inbound_cap` is at or above the channel-capacity ceiling. The stream drivers' shared inbound
  /// channel is sized from it; a capacity at/above the ceiling would trip the channel implementation's
  /// limit.
  #[error("inbound_cap must be at most the channel-capacity ceiling")]
  InboundCapAboveChannelCeiling,
  /// `accept_cap` is at or above the channel-capacity ceiling. The accept channel is sized from it; a
  /// capacity at/above the ceiling would trip the channel implementation's limit.
  #[error("accept_cap must be at most the channel-capacity ceiling")]
  AcceptCapAboveChannelCeiling,
  /// `cmd_budget` is zero. The per-iteration command-drain budget would drain nothing, stalling
  /// every submit behind the run loop.
  #[error("cmd_budget must be non-zero")]
  ZeroCmdBudget,
  /// `events_cap` is zero. The events tail is a bounded channel; a zero capacity cannot hold an
  /// event.
  #[error("events_cap must be non-zero")]
  ZeroEventsCap,
  /// `recv_cap` is zero. The QUIC datagram channel is bounded; a zero capacity cannot hold a
  /// datagram, so the recv task can never hand one to the run loop.
  #[error("recv_cap must be non-zero")]
  ZeroRecvCap,
  /// `inbound_cap` is zero. The stream drivers' shared inbound frame channel is bounded; a zero
  /// capacity cannot hold a frame.
  #[error("inbound_cap must be non-zero")]
  ZeroInboundCap,
  /// `accept_cap` is zero. The accept channel is bounded; a zero capacity cannot hold an accepted
  /// connection.
  #[error("accept_cap must be non-zero")]
  ZeroAcceptCap,
  /// `max_pending_bytes` is zero. The in-flight submit byte budget would reject every non-empty
  /// payload as [`DriverError::Busy`].
  #[error("max_pending_bytes must be non-zero")]
  ZeroMaxPendingBytes,
  /// `max_outbound_backlog` is zero. The per-connection outbound byte budget would close a
  /// connection on its first enqueued byte.
  #[error("max_outbound_backlog must be non-zero")]
  ZeroMaxOutboundBacklog,
  /// `max_conns` is zero. The stream driver's accept admission would refuse every connection.
  #[error("max_conns must be non-zero")]
  ZeroMaxConns,
  /// `max_failover_limbo_bytes` is zero. Every failover query's limbo scan would exceed the cap and
  /// fall back, defeating the inherited-read tier.
  #[error("max_failover_limbo_bytes must be non-zero")]
  ZeroMaxFailoverLimboBytes,
  /// `redial_base` is zero. A zero initial backoff turns a failed dial into a hot retry loop.
  #[error("redial_base must be non-zero")]
  ZeroRedialBase,
  /// `redial_cap` is zero. A zero backoff ceiling turns redials into a hot retry loop.
  #[error("redial_cap must be non-zero")]
  ZeroRedialCap,
  /// `redial_base` exceeds `redial_cap`. The backoff ceiling must not be below the floor, or the
  /// doubled-then-clamped backoff is incoherent.
  #[error("redial_base must be <= redial_cap")]
  RedialBaseAboveCap,
  /// `redial_base` exceeds the `Instant`-safe redial bound. A redial backoff is doubled, jittered (up
  /// to +25%), and ADDED to a `std::time::Instant` to schedule the next attempt; a value near
  /// `Duration::MAX` overflows that `Duration`/`Instant` math and panics at the first redial. Capped at
  /// [`MAX_REDIAL_BACKOFF`](crate::MAX_REDIAL_BACKOFF) so `2 · cap + jitter` and the `Instant` addition
  /// can never overflow.
  #[error("redial_base must be at most the Instant-safe redial bound")]
  RedialBaseTooLarge,
  /// `redial_cap` exceeds the `Instant`-safe redial bound. The cap is the largest backoff the doubling
  /// reaches, so it is the value the jitter + `Instant` math must stay safe for; a value near
  /// `Duration::MAX` overflows and panics. Capped at [`MAX_REDIAL_BACKOFF`](crate::MAX_REDIAL_BACKOFF).
  #[error("redial_cap must be at most the Instant-safe redial bound")]
  RedialCapTooLarge,
}
