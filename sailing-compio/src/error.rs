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
