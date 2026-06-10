//! Endpoint configuration. Tuning is real `Duration` (not logical ticks); the election
//! timeout is randomized per term from the seeded PRNG inside the `Endpoint`.
use crate::{NodeId, error::ConfigError};
use core::time::Duration;
use std::vec::Vec;

/// How linearizable read-only queries are satisfied.
///
/// `Safe` (the default) issues a heartbeat round to confirm leadership before serving the
/// read. `LeaseBased` skips the round-trip by relying on the election-timeout lease — it
/// requires [`Config::check_quorum`] to be enabled (validated by [`Config::validate`]).
#[derive(
  Debug, Clone, Copy, PartialEq, Eq, Default, derive_more::Display, derive_more::IsVariant,
)]
pub enum ReadOnlyOption {
  /// Confirm leadership via a heartbeat quorum before serving each read (default, always safe).
  #[default]
  Safe,
  /// Use the election-timeout lease to confirm leadership without a round-trip.
  ///
  /// **Requires** [`Config::check_quorum`] = `true`; [`Config::validate`] enforces this.
  LeaseBased,
}

impl ReadOnlyOption {
  /// The stable snake_case name.
  #[inline(always)]
  pub const fn as_str(self) -> &'static str {
    match self {
      Self::Safe => "safe",
      Self::LeaseBased => "lease_based",
    }
  }
}

/// Static configuration for an [`crate::Endpoint`]. Holds the initial voter set (dynamic
/// membership is via `ConfChange`). `Clone`, not `Copy` (it owns the voter list).
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
  /// Enable the PreVote extension (§9.6 of the Raft thesis). A node probes for a quorum
  /// of "would-grant" responses before incrementing its term. Prevents a partitioned node
  /// from inflating the cluster term when it rejoins. Default: `false`.
  pre_vote: bool,
  /// Enable CheckQuorum. A leader that does not hear from a quorum of peers within an
  /// election timeout steps down. Pairs with `ReadOnlyOption::LeaseBased`. Default: `false`.
  check_quorum: bool,
  /// When `true`, a follower that receives a `Propose` request does not forward it to the
  /// leader; it returns `NotLeader` immediately. Default: `false`.
  disable_proposal_forwarding: bool,
  /// How linearizable read-only queries are satisfied. Default: [`ReadOnlyOption::Safe`].
  read_only: ReadOnlyOption,
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
      pre_vote: false,
      check_quorum: false,
      disable_proposal_forwarding: false,
      read_only: ReadOnlyOption::Safe,
    })
  }

  /// Construct a configuration for a **joining (observer) node** whose own id is NOT yet
  /// among the current voter set. Used when adding a new node mid-run: the bootstrap voter
  /// seed is the *existing* cluster's voter list, which does not include the joining node's
  /// id. This makes `is_voter(new_id) = false` in the new node's initial Tracker, so it
  /// cannot campaign and cannot disrupt an existing election.
  ///
  /// Differs from [`Self::try_new`] only by skipping the `id ∈ voters` validation.
  pub fn try_new_observer(
    id: I,
    current_voters: Vec<I>,
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
    // Intentionally do NOT check `current_voters.contains(&id)` — the joining node
    // is not a voter in the bootstrap seed by design.
    Ok(Self {
      id,
      voters: current_voters,
      election_timeout,
      heartbeat_interval,
      max_size_per_msg: 1024 * 1024, // 1 MiB default
      max_inflight_msgs: 256,
      max_inflight_bytes: 0,
      snapshot_threshold: 10_000, // etcd default SnapshotCount
      step_down_on_removal: true,
      pre_vote: false,
      check_quorum: false,
      disable_proposal_forwarding: false,
      read_only: ReadOnlyOption::Safe,
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
  #[must_use]
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
  #[must_use]
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
  #[must_use]
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
  #[must_use]
  pub fn with_step_down_on_removal(mut self, v: bool) -> Self {
    self.step_down_on_removal = v;
    self
  }

  /// Whether the PreVote extension is enabled.
  ///
  /// When `true`, a node probes for a quorum of "would-grant" responses before
  /// incrementing its term, preventing a partitioned node from inflating the cluster term.
  #[inline(always)]
  pub const fn pre_vote(&self) -> bool {
    self.pre_vote
  }

  /// Override the `pre_vote` knob.
  #[must_use]
  pub fn with_pre_vote(mut self, v: bool) -> Self {
    self.pre_vote = v;
    self
  }

  /// Whether CheckQuorum is enabled.
  ///
  /// When `true`, a leader that does not hear from a quorum of peers within an election
  /// timeout steps down. Required by [`ReadOnlyOption::LeaseBased`].
  #[inline(always)]
  pub const fn check_quorum(&self) -> bool {
    self.check_quorum
  }

  /// Override the `check_quorum` knob.
  #[must_use]
  pub fn with_check_quorum(mut self, v: bool) -> Self {
    self.check_quorum = v;
    self
  }

  /// Whether proposal forwarding from followers to the leader is disabled.
  ///
  /// When `true`, a follower that receives a `Propose` returns `NotLeader` immediately
  /// rather than forwarding to the leader.
  #[inline(always)]
  pub const fn disable_proposal_forwarding(&self) -> bool {
    self.disable_proposal_forwarding
  }

  /// Override the `disable_proposal_forwarding` knob.
  #[must_use]
  pub fn with_disable_proposal_forwarding(mut self, v: bool) -> Self {
    self.disable_proposal_forwarding = v;
    self
  }

  /// How linearizable read-only queries are satisfied.
  #[inline(always)]
  pub const fn read_only(&self) -> ReadOnlyOption {
    self.read_only
  }

  /// Override the `read_only` knob.
  #[must_use]
  pub fn with_read_only(mut self, v: ReadOnlyOption) -> Self {
    self.read_only = v;
    self
  }

  /// Validate cross-field invariants that cannot be checked at construction time.
  ///
  /// Currently enforces: `ReadOnlyOption::LeaseBased` requires `check_quorum = true`.
  /// (Lease-based reads are only safe when CheckQuorum guarantees the election-timeout
  /// lease is fresh; without it a stale leader could serve a read after losing quorum.)
  ///
  /// Call this after building a `Config` via the builder chain; `try_new` and
  /// `try_new_observer` do **not** call it automatically so that callers have a chance to
  /// set all knobs first.
  pub fn validate(&self) -> Result<(), ConfigError> {
    if self.read_only == ReadOnlyOption::LeaseBased && !self.check_quorum {
      return Err(ConfigError::LeaseRequiresCheckQuorum);
    }
    Ok(())
  }
}

#[cfg(test)]
mod tests;
