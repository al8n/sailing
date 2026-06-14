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
/// `LeaseGuard` is the commit-anchored lease (the LeaseGuard protocol): the leader serves a
/// read while its last committed entry is younger than [`Config::lease_duration`]. It requires
/// `lease_duration` AND a [`Config::clock_drift_bound`] (the local-timer drift the commit-wait
/// needs); a [`Config::bounded_clock_uncertainty`] additionally enables inherited-lease reads.
/// It does NOT need `check_quorum` (its safety rests on the commit-wait, not
/// election-prevention).
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
  /// The commit-anchored LeaseGuard lease. **Requires** [`Config::lease_duration`] and
  /// [`Config::clock_drift_bound`]; [`Config::bounded_clock_uncertainty`] enables inherited-lease
  /// reads. The per-entry commit-wait window is the exact `Δ·(Δ+ε)/(Δ−ε)` (covering a slow deposed
  /// leader AND a fast successor; see [`Config::clock_drift_bound`]); [`Config::validate`] requires
  /// `clock_drift_bound < lease_duration` and the window `< election_timeout`.
  ///
  /// **Deployment contract — a fresh-cluster / matched-schema choice.** Cross-leader safety relies on
  /// each entry's self-describing `lease_window` (and each snapshot's `max_lease_window`) being
  /// preserved end to end, so EVERY voter must run a LeaseGuard-aware build and persist those wire
  /// fields. Enabling LeaseGuard on a partially-upgraded cluster, or on storage that strips unknown
  /// proto fields, can leave a successor's commit-wait under-sized (a stale read). The duplicate
  /// AppendEntries / snapshot RUNTIME paths fold a newly-visible window defensively, but durable
  /// survival across a restart of a stripped window is the operator's responsibility — like
  /// `LeaseBased`'s bounded-drift contract, mid-life migration is out of scope (see WIRE.md).
  LeaseGuard,
}

impl ReadOnlyOption {
  /// The stable snake_case name.
  #[inline(always)]
  pub const fn as_str(self) -> &'static str {
    match self {
      Self::Safe => "safe",
      Self::LeaseBased => "lease_based",
      Self::LeaseGuard => "lease_guard",
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
  /// The LeaseGuard lease window Δ: a leader serves a `LeaseGuard` read while its last committed
  /// entry is younger than this. Required when `read_only = LeaseGuard`; ignored otherwise.
  /// Default: `None`.
  lease_duration: Option<Duration>,
  /// The bounded one-sided clock drift ε — the most a SINGLE node's clock may gain OR lose (in real
  /// time) while measuring a `lease_duration` interval; equivalently the rate drift is `ρ = ε/Δ`
  /// (`Δ` = `lease_duration`). REQUIRED for `LeaseGuard`, and must be `< lease_duration`. The
  /// post-election commit-wait window stamped per entry is the EXACT `Δ·(Δ+ε)/(Δ−ε) = Δ·(1+ρ)/(1−ρ)`
  /// (≈ `lease_duration + 2·clock_drift_bound` for small drift): it provably covers BOTH a slow
  /// deposed leader (whose lease lasts up to real time `Δ/(1−ρ)`) AND a fast successor (whose local
  /// wait finishes at real time `window/(1+ρ)`), so the successor commits only after the deposed
  /// leader's read-lease has expired. Needs only local clocks with bounded drift, no cross-node sync.
  /// Default: `None`.
  clock_drift_bound: Option<Duration>,
  /// The bounded cross-node clock-UNCERTAINTY (skew). OPTIONAL: `Some(ε)` enables LeaseGuard's
  /// FAILOVER tier — the precise commit-anchor (and, later, inherited-lease reads) compares in-log
  /// wall timestamps ACROSS leaders, so it requires each node's synchronized wall to stay within ε of
  /// true cluster-epoch time. That one convention supplies both terms of the anchor's `2·ε` margin: a
  /// deposed leader's stamp lags real time by ≤ ε, and a successor's evaluation leads it by ≤ ε.
  /// `None` = the new leader simply waits out the prior lease on local clocks (safe, less available).
  /// Default: `None`.
  bounded_clock_uncertainty: Option<Duration>,
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
      lease_duration: None,
      clock_drift_bound: None,
      bounded_clock_uncertainty: None,
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
      lease_duration: None,
      clock_drift_bound: None,
      bounded_clock_uncertainty: None,
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

  /// The LeaseGuard lease window Δ (required when `read_only = LeaseGuard`).
  #[inline(always)]
  pub const fn lease_duration(&self) -> Option<Duration> {
    self.lease_duration
  }

  /// Set the LeaseGuard lease window Δ.
  #[must_use]
  pub fn with_lease_duration(mut self, v: Duration) -> Self {
    self.lease_duration = Some(v);
    self
  }

  /// The bounded clock-drift ε for the commit-wait (required for `LeaseGuard`).
  #[inline(always)]
  pub const fn clock_drift_bound(&self) -> Option<Duration> {
    self.clock_drift_bound
  }

  /// Set the bounded clock-drift ε (required for `LeaseGuard`).
  #[must_use]
  pub fn with_clock_drift_bound(mut self, v: Duration) -> Self {
    self.clock_drift_bound = Some(v);
    self
  }

  /// The bounded cross-node clock-uncertainty (`Some` enables LeaseGuard inherited-lease reads).
  #[inline(always)]
  pub const fn bounded_clock_uncertainty(&self) -> Option<Duration> {
    self.bounded_clock_uncertainty
  }

  /// Set the bounded cross-node clock-uncertainty (enables LeaseGuard inherited-lease reads).
  #[must_use]
  pub fn with_bounded_clock_uncertainty(mut self, v: Duration) -> Self {
    self.bounded_clock_uncertainty = Some(v);
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
    if self.read_only == ReadOnlyOption::LeaseGuard {
      // The single source of truth for the LeaseGuard timing — same computation used to STAMP the
      // per-entry window and to gate the read fast-path, so validation, stamping, and liveness never
      // diverge. Propagates the specific error (missing knob / timing too long).
      self.leaseguard_window_result()?;
    }
    // The LeaseGuard FAILOVER tier (`bounded_clock_uncertainty` set) only makes sense under
    // `LeaseGuard` — it gates the inherited-lease reads + the precise commit-anchor — and the skew
    // bound must be a real fraction of the lease (`ε_unc < Δ`); `ε_unc ≥ Δ` would make the cross-node
    // age comparison vacuous. (When `read_only == LeaseGuard`, the check above already proved
    // `lease_duration` is `Some`; the `is_none_or` only fires for a misordered non-LeaseGuard config.)
    if let Some(unc) = self.bounded_clock_uncertainty
      && (self.read_only != ReadOnlyOption::LeaseGuard
        || self.lease_duration.is_none_or(|d| unc >= d))
    {
      return Err(ConfigError::BoundedUncertaintyInvalid {
        uncertainty: unc,
        lease: self.lease_duration,
      });
    }
    Ok(())
  }

  /// The EXACT LeaseGuard commit-wait window in nanoseconds for a valid config, or the specific
  /// [`ConfigError`] if the timing is invalid. Assumes `read_only == LeaseGuard` (validate-gated).
  ///
  /// `W = Δ·(Δ+ε)/(Δ−ε)` = `Δ·(1+ρ)/(1−ρ)` for the rate drift `ρ = clock_drift_bound/lease_duration`
  /// (`Δ` = lease_duration, `ε` = clock_drift_bound). This is the smallest wait that, with the strict
  /// read gate, provably outlasts a SLOW deposed leader's lease (real time `Δ/(1−ρ)`) as measured by
  /// a FAST successor (whose local wait finishes at real time `W/(1+ρ)`): `W/(1+ρ) ≥ Δ/(1−ρ)`. A flat
  /// `Δ + 2ε` is first-order correct but a hair short (a 2ρ² term), so the exact ratio is required.
  /// Computed in `u128` and rounded UP (never short), then range-checked to fit the `u64`
  /// `Entry.lease_window` field AND stay below the election timeout (the liveness bound). Requires
  /// `ε < Δ` (the rate drift below 100%).
  fn leaseguard_window_result(&self) -> Result<u64, ConfigError> {
    let lease = self
      .lease_duration
      .ok_or(ConfigError::LeaseGuardRequiresLeaseDuration)?;
    let drift = self
      .clock_drift_bound
      .ok_or(ConfigError::LeaseGuardRequiresDriftBound)?;
    let too_long = || ConfigError::LeaseTimingTooLong {
      lease,
      drift,
      election: self.election_timeout,
    };
    let (d, e) = (lease.as_nanos(), drift.as_nanos());
    // ε < Δ (denominator `Δ − ε` must be positive); else the window diverges / the config is degenerate.
    let denom = d.checked_sub(e).filter(|&x| x > 0).ok_or_else(too_long)?;
    // W = Δ·(Δ+ε)/(Δ−ε), in u128 (no truncation), rounded UP. `checked_mul` rejects an absurd Δ near
    // the u128 ceiling rather than wrapping.
    let w = d
      .checked_mul(d + e)
      .map(|num| num.div_ceil(denom))
      .ok_or_else(too_long)?;
    // Must fit the u64 wire field AND stay strictly below the election timeout (so a fresh leader
    // commits before a follower could depose it mid-wait).
    if w > u64::MAX as u128 || w >= self.election_timeout.as_nanos() {
      return Err(too_long());
    }
    Ok(w as u64)
  }

  /// The EXACT LeaseGuard commit-wait window (nanos) when the mode is ACTIVE and the config is valid,
  /// else `None`. The single source of truth that the per-entry stamp, the read fast-path, and the
  /// commit-wait all gate on.
  pub(crate) fn leaseguard_commit_wait_ns(&self) -> Option<u64> {
    if self.read_only != ReadOnlyOption::LeaseGuard {
      return None;
    }
    self.leaseguard_window_result().ok()
  }
}

#[cfg(test)]
mod tests;
