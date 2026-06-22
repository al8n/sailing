//! Endpoint configuration. Tuning is real `Duration` (not logical ticks); the election
//! timeout is randomized per term from the seeded PRNG inside the `Endpoint`.
use crate::{CheapClone, error::ConfigError};
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
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
#[cfg_attr(feature = "clap", derive(clap::ValueEnum))]
#[cfg_attr(feature = "clap", value(rename_all = "snake_case"))]
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
  /// `LeaseBased`'s bounded-drift contract, mid-life WIRE-FORMAT migration (retrofitting these fields
  /// onto a running cluster) is out of scope (see WIRE.md). Changing the read MODE itself on a running
  /// cluster (Safe ↔ LeaseBased ↔ LeaseGuard) IS supported when the target's knobs are pre-provisioned —
  /// see [`Endpoint::propose_read_mode_change`](crate::Endpoint::propose_read_mode_change).
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

  /// The canonical wire discriminant — the byte a `SetReadMode` log entry carries as its payload.
  #[inline(always)]
  pub const fn as_u8(self) -> u8 {
    match self {
      Self::Safe => 0,
      Self::LeaseBased => 1,
      Self::LeaseGuard => 2,
    }
  }

  /// Parse the canonical wire discriminant; `None` for an unknown byte.
  #[inline(always)]
  pub const fn from_u8(b: u8) -> Option<Self> {
    match b {
      0 => Some(Self::Safe),
      1 => Some(Self::LeaseBased),
      2 => Some(Self::LeaseGuard),
      _ => None,
    }
  }
}

/// When (if ever) a LeaseGuard leader proactively re-anchors its read lease with a no-op, so reads do not
/// pay a Safe round after the lease ages out. The re-anchor fires ONLY when reads are active since the
/// current anchor, so an idle leader never amplifies writes. Only meaningful under
/// [`ReadOnlyOption::LeaseGuard`]; [`Config::validate`] rejects a proactive mode in any other read mode.
#[derive(
  Debug, Clone, Copy, PartialEq, Eq, Default, derive_more::Display, derive_more::IsVariant,
)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
#[cfg_attr(feature = "clap", derive(clap::ValueEnum))]
#[cfg_attr(feature = "clap", value(rename_all = "snake_case"))]
pub enum LeaseRefresh {
  /// Demand-driven only (DEFAULT): a stale read triggers one no-op at the next heartbeat. The first read
  /// after the lease ages pays one Safe round. Byte-identical to pre-feature behavior.
  #[default]
  Off,
  /// Proactive, read-gated: if a read occurred since the current anchor AND the anchor is within a margin
  /// of expiry (`2 · heartbeat_interval`, to cover the no-op's commit round-trip), append one no-op before
  /// it dies. While reads flow this re-anchors roughly once per `lease_duration − 2 · heartbeat_interval`,
  /// so the LOW-AMPLIFICATION regime requires `lease_duration` to be well above `2 · heartbeat_interval`;
  /// as the lease shrinks toward that margin the rate climbs and OnExpiry degenerates toward `Continuous`
  /// (a no-op every heartbeat).
  OnExpiry,
  /// Refresh every heartbeat while reads are recent — keeps the lease maximally fresh at the cost of up to
  /// one no-op per heartbeat under continuous reads (operator-accepted write amplification).
  Continuous,
}

impl LeaseRefresh {
  /// The stable snake_case name.
  #[inline(always)]
  pub const fn as_str(self) -> &'static str {
    match self {
      Self::Off => "off",
      Self::OnExpiry => "on_expiry",
      Self::Continuous => "continuous",
    }
  }
}

/// Default [`Config::max_size_per_msg`]: 1 MiB of entries packed per `AppendEntries`.
pub const DEFAULT_MAX_SIZE_PER_MSG: u64 = 1024 * 1024;
/// Default [`Config::max_inflight_msgs`]: up to 256 un-acked `AppendEntries` per peer.
pub const DEFAULT_MAX_INFLIGHT_MSGS: usize = 256;
/// Default [`Config::max_inflight_bytes`]: `0` = the in-flight byte budget is uncapped.
pub const DEFAULT_MAX_INFLIGHT_BYTES: u64 = 0;
/// Default [`Config::snapshot_threshold`]: etcd's `SnapshotCount` (committed entries between snapshots).
pub const DEFAULT_SNAPSHOT_THRESHOLD: usize = 10_000;
/// Default [`Config::step_down_on_removal`]: a leader removed/demoted by a committed `ConfChange`
/// steps down immediately.
pub const DEFAULT_STEP_DOWN_ON_REMOVAL: bool = true;
/// Default [`Config::pre_vote`]: the PreVote extension is off.
pub const DEFAULT_PRE_VOTE: bool = false;
/// Default [`Config::check_quorum`]: CheckQuorum is off.
pub const DEFAULT_CHECK_QUORUM: bool = false;
/// Default [`Config::disable_proposal_forwarding`]: followers forward proposals to the leader.
pub const DEFAULT_DISABLE_PROPOSAL_FORWARDING: bool = false;
/// Default [`Config::read_only`]: the always-safe heartbeat-confirmed read mode.
pub const DEFAULT_READ_ONLY: ReadOnlyOption = ReadOnlyOption::Safe;
/// Default [`Config::lease_refresh`]: demand-driven only (byte-identical to pre-feature behavior).
pub const DEFAULT_LEASE_REFRESH: LeaseRefresh = LeaseRefresh::Off;
/// Default [`Config::lease_duration`]: no LeaseGuard lease window configured.
pub const DEFAULT_LEASE_DURATION: Option<Duration> = None;
/// Default [`Config::clock_drift_bound`]: no bounded clock-drift configured.
pub const DEFAULT_CLOCK_DRIFT_BOUND: Option<Duration> = None;
/// Default [`Config::bounded_clock_uncertainty`]: no bounded cross-node clock-uncertainty configured.
pub const DEFAULT_BOUNDED_CLOCK_UNCERTAINTY: Option<Duration> = None;
/// Default [`Config::election_timeout`]: 1s (10× the heartbeat — etcd's standard ratio). Exceeds
/// [`DEFAULT_HEARTBEAT_INTERVAL`], so the parsed-path `election_timeout > heartbeat_interval`
/// invariant holds for a config that defaults both.
pub const DEFAULT_ELECTION_TIMEOUT: Duration = Duration::from_secs(1);
/// Default [`Config::heartbeat_interval`]: 100ms (etcd's standard heartbeat tick).
pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(100);

/// `Instant`-safe ceiling for [`Config::election_timeout`].
///
/// The per-term randomized election timeout is drawn as `election_timeout +
/// Duration::from_millis(rng % election_timeout_ms)` — a RAW `core::time::Duration` add (its `Add`
/// `expect`s on overflow, unlike the crate's saturating [`Instant`](crate::Instant) add), so the draw
/// can reach just under `2 · election_timeout`. A value near `Duration::MAX` parsed from a serde/clap
/// config therefore overflows that add and PANICS the node on its first election. Bounding
/// `election_timeout` to this ceiling keeps `2 · election_timeout` (the draw's supremum) far below
/// `Duration::MAX`. Because [`Config::validate`] also requires `election_timeout > heartbeat_interval`,
/// this transitively bounds `heartbeat_interval` too.
///
/// Mirrors the QUIC transport's `MAX_TIMER_MILLIS` (`u32::MAX` ms ≈ 49.7 days): comfortably below any
/// `Instant`/`Duration` overflow threshold yet far above any realistic election timeout (the default is
/// 1 s). It is the `Instant`-safety bound, not a policy cap.
pub const MAX_ELECTION_TIMEOUT: Duration = Duration::from_millis(u32::MAX as u64);

// The default election timeout must itself satisfy the new upper bound, or a config that defaults it
// would be rejected by `validate`.
const _: () = assert!(DEFAULT_ELECTION_TIMEOUT.as_nanos() <= MAX_ELECTION_TIMEOUT.as_nanos());

// The two defaulted timeouts must, on their own, satisfy `validate`'s
// `election_timeout > heartbeat_interval`, so a serde/clap config that supplies neither is valid. This
// compile-time check pins that relationship to the consts (and, the consts not being publicly
// re-exported, keeps them referenced in the always-built path — every other `DEFAULT_*` is kept alive
// by the constructors, but the timeouts arrive there as parameters, not as these consts).
const _: () = assert!(DEFAULT_ELECTION_TIMEOUT.as_nanos() > DEFAULT_HEARTBEAT_INTERVAL.as_nanos());

// `serde(default = "…")` needs a function PATH, not a const, so each non-`Option` value knob's
// default is wrapped to return the single-source-of-truth `DEFAULT_*` const. The `Option<Duration>`
// knobs use the bare `serde(default)` (`None`) instead, and the enum knobs use `serde(default)`
// (their `#[default]` variant), so neither needs a wrapper. The clap mirror reuses the consts
// directly via `default_value_t`. Gated on `serde` so the default build stays warning-free.
#[cfg(feature = "serde")]
const fn default_max_size_per_msg() -> u64 {
  DEFAULT_MAX_SIZE_PER_MSG
}
#[cfg(feature = "serde")]
const fn default_max_inflight_msgs() -> usize {
  DEFAULT_MAX_INFLIGHT_MSGS
}
#[cfg(feature = "serde")]
const fn default_max_inflight_bytes() -> u64 {
  DEFAULT_MAX_INFLIGHT_BYTES
}
#[cfg(feature = "serde")]
const fn default_snapshot_threshold() -> usize {
  DEFAULT_SNAPSHOT_THRESHOLD
}
#[cfg(feature = "serde")]
const fn default_step_down_on_removal() -> bool {
  DEFAULT_STEP_DOWN_ON_REMOVAL
}
#[cfg(feature = "serde")]
const fn default_pre_vote() -> bool {
  DEFAULT_PRE_VOTE
}
#[cfg(feature = "serde")]
const fn default_check_quorum() -> bool {
  DEFAULT_CHECK_QUORUM
}
#[cfg(feature = "serde")]
const fn default_disable_proposal_forwarding() -> bool {
  DEFAULT_DISABLE_PROPOSAL_FORWARDING
}
#[cfg(feature = "serde")]
const fn default_election_timeout() -> Duration {
  DEFAULT_ELECTION_TIMEOUT
}
#[cfg(feature = "serde")]
const fn default_heartbeat_interval() -> Duration {
  DEFAULT_HEARTBEAT_INTERVAL
}

/// Static configuration for an [`crate::Endpoint`]. Holds the initial voter set (dynamic
/// membership is via `ConfChange`). `Clone`, not `Copy` (it owns the voter list).
///
/// A config parsed from serde or clap is VALIDATED at parse time, so it can never carry a value the
/// programmatic constructors reject (see [`Self::validate`]). A parsed config is a VOTER when
/// `id ∈ voters` and an OBSERVER otherwise; both are accepted (the `id ∈ voters` rule is
/// voter-only — see [`Self::validate`]). The two parse paths use separate `serde` / `clap` mirrors,
/// so a custom `NodeId` that is `Deserialize` but not `FromStr` deserializes fine.
///
/// `serde` (re)serializes every knob — `id` / `voters` directly, the two `Duration` timeouts as
/// humantime strings via `humantime-serde`, and every tuning knob — so a partial config file carrying
/// only the required `id` / `voters` deserializes with the timeouts AND every knob at their
/// single-source-of-truth `DEFAULT_*` values. `clap` exposes the same knobs as CLI flags + `SAILING_*`
/// env vars.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
// The explicit `bound(deserialize = …)` is required: without it serde infers only `I: Deserialize`,
// leaving `Config<I>: TryFrom<ConfigSerde<I>>` (which the `try_from` below needs) unsatisfied.
// `Serialize` is a direct derive (it needs only `I: Serialize`); `Deserialize` routes through the
// `ConfigSerde<I>` mirror so its impl carries only that mirror's parse bounds.
#[cfg_attr(
  feature = "serde",
  serde(
    try_from = "ConfigSerde<I>",
    bound(deserialize = "I: serde::Deserialize<'de> + Clone + PartialEq")
  )
)]
pub struct Config<I> {
  id: I,
  voters: Vec<I>,
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
  election_timeout: Duration,
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
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
  /// How linearizable read-only queries are satisfied — the GENESIS default and knob source. The LIVE
  /// serving mode is recovered from replicated state and migrates at runtime via a committed `SetReadMode`
  /// (see [`Endpoint::propose_read_mode_change`](crate::Endpoint::propose_read_mode_change)); this field
  /// stays the immutable construction seed. Default: [`ReadOnlyOption::Safe`].
  read_only: ReadOnlyOption,
  /// When (if ever) a LeaseGuard leader proactively re-anchors its read lease with a no-op so reads do
  /// not pay a Safe round after the lease ages. Only meaningful under `LeaseGuard`. Default:
  /// [`LeaseRefresh::Off`] (demand-driven only — byte-identical to pre-feature behavior).
  lease_refresh: LeaseRefresh,
  /// The LeaseGuard lease window Δ: a leader serves a `LeaseGuard` read while its last committed
  /// entry is younger than this. Required when `read_only = LeaseGuard`; ignored otherwise.
  /// Default: `None`.
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
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
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
  clock_drift_bound: Option<Duration>,
  /// The bounded cross-node clock-UNCERTAINTY (skew). OPTIONAL: `Some(ε)` enables LeaseGuard's
  /// FAILOVER tier — the precise commit-anchor (and, later, inherited-lease reads) compares in-log
  /// wall timestamps ACROSS leaders, so it requires each node's synchronized wall to stay within ε of
  /// true cluster-epoch time. That one convention supplies both terms of the anchor's `2·ε` margin: a
  /// deposed leader's stamp lags real time by ≤ ε, and a successor's evaluation leads it by ≤ ε.
  /// `None` = the new leader simply waits out the prior lease on local clocks (safe, less available).
  /// Default: `None`.
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
  bounded_clock_uncertainty: Option<Duration>,
}

impl<I: PartialEq> Config<I> {
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
    // The per-term randomized timeout is a raw `Duration` add (`election_timeout + jitter`, jitter
    // `< election_timeout`); a value near `Duration::MAX` overflows it and panics the first election.
    // `Endpoint::new` does NOT call `validate`, so the bound must also live here, at the construction
    // boundary, to keep `2 · election_timeout` `Instant`-safe.
    if election_timeout > MAX_ELECTION_TIMEOUT {
      return Err(ConfigError::ElectionTimeoutTooLarge {
        election: election_timeout,
        max: MAX_ELECTION_TIMEOUT,
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
      max_size_per_msg: DEFAULT_MAX_SIZE_PER_MSG,
      max_inflight_msgs: DEFAULT_MAX_INFLIGHT_MSGS,
      max_inflight_bytes: DEFAULT_MAX_INFLIGHT_BYTES,
      snapshot_threshold: DEFAULT_SNAPSHOT_THRESHOLD,
      step_down_on_removal: DEFAULT_STEP_DOWN_ON_REMOVAL,
      pre_vote: DEFAULT_PRE_VOTE,
      check_quorum: DEFAULT_CHECK_QUORUM,
      disable_proposal_forwarding: DEFAULT_DISABLE_PROPOSAL_FORWARDING,
      read_only: DEFAULT_READ_ONLY,
      lease_refresh: DEFAULT_LEASE_REFRESH,
      lease_duration: DEFAULT_LEASE_DURATION,
      clock_drift_bound: DEFAULT_CLOCK_DRIFT_BOUND,
      bounded_clock_uncertainty: DEFAULT_BOUNDED_CLOCK_UNCERTAINTY,
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
    // Same `Instant`-safe bound as `try_new` (see there): the per-term randomized timeout's raw
    // `Duration` add would overflow for an `election_timeout` near `Duration::MAX`, and `Endpoint::new`
    // never calls `validate`.
    if election_timeout > MAX_ELECTION_TIMEOUT {
      return Err(ConfigError::ElectionTimeoutTooLarge {
        election: election_timeout,
        max: MAX_ELECTION_TIMEOUT,
      });
    }
    // Intentionally do NOT check `current_voters.contains(&id)` — the joining node
    // is not a voter in the bootstrap seed by design.
    Ok(Self {
      id,
      voters: current_voters,
      election_timeout,
      heartbeat_interval,
      max_size_per_msg: DEFAULT_MAX_SIZE_PER_MSG,
      max_inflight_msgs: DEFAULT_MAX_INFLIGHT_MSGS,
      max_inflight_bytes: DEFAULT_MAX_INFLIGHT_BYTES,
      snapshot_threshold: DEFAULT_SNAPSHOT_THRESHOLD,
      step_down_on_removal: DEFAULT_STEP_DOWN_ON_REMOVAL,
      pre_vote: DEFAULT_PRE_VOTE,
      check_quorum: DEFAULT_CHECK_QUORUM,
      disable_proposal_forwarding: DEFAULT_DISABLE_PROPOSAL_FORWARDING,
      read_only: DEFAULT_READ_ONLY,
      lease_refresh: DEFAULT_LEASE_REFRESH,
      lease_duration: DEFAULT_LEASE_DURATION,
      clock_drift_bound: DEFAULT_CLOCK_DRIFT_BOUND,
      bounded_clock_uncertainty: DEFAULT_BOUNDED_CLOCK_UNCERTAINTY,
    })
  }

  /// Whether `id` is a voter.
  #[inline(always)]
  pub fn is_voter(&self, id: I) -> bool {
    self.voters.contains(&id)
  }
}

impl<I: CheapClone> Config<I> {
  /// This node's id.
  #[inline(always)]
  pub fn id(&self) -> I {
    self.id.cheap_clone()
  }
}

impl<I> Config<I> {
  /// The voter set.
  #[inline(always)]
  pub const fn voters(&self) -> &[I] {
    self.voters.as_slice()
  }

  /// Majority size of the configured SEED voter set (`n/2 + 1`) — a convenience accessor, NOT the live
  /// consensus quorum. The quorum that actually gates commit/elections is derived by the joint-aware
  /// `Tracker`/`MajorityConfig` from the CURRENT (possibly joint) configuration, which handles an empty
  /// voter set correctly. For an empty observer seed this returns a degenerate `1`; such a node cannot
  /// commit anything until it is reconfigured into a real voter set.
  #[inline(always)]
  pub const fn quorum(&self) -> usize {
    self.voters.len() / 2 + 1
  }

  /// The base election timeout (randomized per term at runtime).
  #[inline(always)]
  pub const fn election_timeout(&self) -> Duration {
    self.election_timeout
  }

  /// Test-only: overwrite `election_timeout` BYPASSING the [`MAX_ELECTION_TIMEOUT`] bound the public
  /// constructors enforce. Some FAILOVER tests must reach a RUNTIME comparison that keys on a value
  /// `election_timeout` can never legally hold (e.g. an inherited E′ inflation vs an election timeout
  /// above `u64::MAX` nanos) — a deliberately unconstructible config used only to exercise that branch.
  /// Production code never sets the field except through the bounded constructors.
  #[cfg(test)]
  pub(crate) fn set_election_timeout_for_test(&mut self, v: Duration) -> &mut Self {
    self.election_timeout = v;
    self
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

  /// Override the `max_size_per_msg` knob (consuming).
  #[inline(always)]
  #[must_use]
  pub const fn with_max_size_per_msg(mut self, v: u64) -> Self {
    self.set_max_size_per_msg(v);
    self
  }

  /// Override the `max_size_per_msg` knob in place.
  #[inline(always)]
  pub const fn set_max_size_per_msg(&mut self, v: u64) -> &mut Self {
    self.max_size_per_msg = v;
    self
  }

  /// Override the `max_inflight_msgs` knob (consuming). Returns `Err(ConfigError::ZeroInflight)` if
  /// `v == 0`.
  ///
  /// Not `const`: the fallible path would drop the owned-`Vec` `Config` in its error arm, and a
  /// destructor cannot run in a `const fn`. The in-place [`Self::set_max_inflight_msgs`] (which only
  /// borrows) is `const`.
  #[inline(always)]
  pub fn with_max_inflight_msgs(mut self, v: usize) -> Result<Self, ConfigError> {
    self.set_max_inflight_msgs(v)?;
    Ok(self)
  }

  /// Override the `max_inflight_msgs` knob in place. Returns `Err(ConfigError::ZeroInflight)` if
  /// `v == 0`.
  #[inline(always)]
  pub const fn set_max_inflight_msgs(&mut self, v: usize) -> Result<&mut Self, ConfigError> {
    if v == 0 {
      return Err(ConfigError::ZeroInflight);
    }
    self.max_inflight_msgs = v;
    Ok(self)
  }

  /// Override the `max_inflight_bytes` knob (consuming).
  #[inline(always)]
  #[must_use]
  pub const fn with_max_inflight_bytes(mut self, v: u64) -> Self {
    self.set_max_inflight_bytes(v);
    self
  }

  /// Override the `max_inflight_bytes` knob in place.
  #[inline(always)]
  pub const fn set_max_inflight_bytes(&mut self, v: u64) -> &mut Self {
    self.max_inflight_bytes = v;
    self
  }

  /// Number of committed entries between automatic snapshots.
  #[inline(always)]
  pub const fn snapshot_threshold(&self) -> usize {
    self.snapshot_threshold
  }

  /// Override the `snapshot_threshold` knob (consuming).
  #[inline(always)]
  #[must_use]
  pub const fn with_snapshot_threshold(mut self, v: usize) -> Self {
    self.set_snapshot_threshold(v);
    self
  }

  /// Override the `snapshot_threshold` knob in place.
  #[inline(always)]
  pub const fn set_snapshot_threshold(&mut self, v: usize) -> &mut Self {
    self.snapshot_threshold = v;
    self
  }

  /// Whether a leader that loses its voter status (removed or demoted to learner) should
  /// step down immediately when the `ConfChange` is applied. Defaults to `true`.
  #[inline(always)]
  pub const fn step_down_on_removal(&self) -> bool {
    self.step_down_on_removal
  }

  /// Override the `step_down_on_removal` knob (consuming).
  #[inline(always)]
  #[must_use]
  pub const fn with_step_down_on_removal(mut self, v: bool) -> Self {
    self.set_step_down_on_removal(v);
    self
  }

  /// Override the `step_down_on_removal` knob in place.
  #[inline(always)]
  pub const fn set_step_down_on_removal(&mut self, v: bool) -> &mut Self {
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

  /// Override the `pre_vote` knob (consuming).
  #[inline(always)]
  #[must_use]
  pub const fn with_pre_vote(mut self, v: bool) -> Self {
    self.set_pre_vote(v);
    self
  }

  /// Override the `pre_vote` knob in place.
  #[inline(always)]
  pub const fn set_pre_vote(&mut self, v: bool) -> &mut Self {
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

  /// Override the `check_quorum` knob (consuming).
  #[inline(always)]
  #[must_use]
  pub const fn with_check_quorum(mut self, v: bool) -> Self {
    self.set_check_quorum(v);
    self
  }

  /// Override the `check_quorum` knob in place.
  #[inline(always)]
  pub const fn set_check_quorum(&mut self, v: bool) -> &mut Self {
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

  /// Override the `disable_proposal_forwarding` knob (consuming).
  #[inline(always)]
  #[must_use]
  pub const fn with_disable_proposal_forwarding(mut self, v: bool) -> Self {
    self.set_disable_proposal_forwarding(v);
    self
  }

  /// Override the `disable_proposal_forwarding` knob in place.
  #[inline(always)]
  pub const fn set_disable_proposal_forwarding(&mut self, v: bool) -> &mut Self {
    self.disable_proposal_forwarding = v;
    self
  }

  /// The GENESIS read mode (the construction default + knob source) — NOT the live serving mode after a
  /// runtime migration, which is [`Endpoint::active_read_mode`](crate::Endpoint::active_read_mode).
  #[inline(always)]
  pub const fn read_only(&self) -> ReadOnlyOption {
    self.read_only
  }

  /// Override the `read_only` knob (consuming).
  #[inline(always)]
  #[must_use]
  pub const fn with_read_only(mut self, v: ReadOnlyOption) -> Self {
    self.set_read_only(v);
    self
  }

  /// Override the `read_only` knob in place.
  #[inline(always)]
  pub const fn set_read_only(&mut self, v: ReadOnlyOption) -> &mut Self {
    self.read_only = v;
    self
  }

  /// When (if ever) a LeaseGuard leader proactively re-anchors its read lease. Default:
  /// [`LeaseRefresh::Off`].
  #[inline(always)]
  pub const fn lease_refresh(&self) -> LeaseRefresh {
    self.lease_refresh
  }

  /// Override the `lease_refresh` knob (consuming; only meaningful under [`ReadOnlyOption::LeaseGuard`];
  /// [`Self::validate`] rejects a proactive mode in any other read mode).
  #[inline(always)]
  #[must_use]
  pub const fn with_lease_refresh(mut self, v: LeaseRefresh) -> Self {
    self.set_lease_refresh(v);
    self
  }

  /// Override the `lease_refresh` knob in place (only meaningful under [`ReadOnlyOption::LeaseGuard`];
  /// [`Self::validate`] rejects a proactive mode in any other read mode).
  #[inline(always)]
  pub const fn set_lease_refresh(&mut self, v: LeaseRefresh) -> &mut Self {
    self.lease_refresh = v;
    self
  }

  /// The LeaseGuard lease window Δ (required when `read_only = LeaseGuard`).
  #[inline(always)]
  pub const fn lease_duration(&self) -> Option<Duration> {
    self.lease_duration
  }

  /// Set the LeaseGuard lease window Δ (consuming).
  #[inline(always)]
  #[must_use]
  pub const fn with_lease_duration(mut self, v: Duration) -> Self {
    self.set_lease_duration(v);
    self
  }

  /// Set the LeaseGuard lease window Δ in place.
  #[inline(always)]
  pub const fn set_lease_duration(&mut self, v: Duration) -> &mut Self {
    self.lease_duration = Some(v);
    self
  }

  /// The bounded clock-drift ε for the commit-wait (required for `LeaseGuard`).
  #[inline(always)]
  pub const fn clock_drift_bound(&self) -> Option<Duration> {
    self.clock_drift_bound
  }

  /// Set the bounded clock-drift ε (consuming; required for `LeaseGuard`).
  #[inline(always)]
  #[must_use]
  pub const fn with_clock_drift_bound(mut self, v: Duration) -> Self {
    self.set_clock_drift_bound(v);
    self
  }

  /// Set the bounded clock-drift ε in place (required for `LeaseGuard`).
  #[inline(always)]
  pub const fn set_clock_drift_bound(&mut self, v: Duration) -> &mut Self {
    self.clock_drift_bound = Some(v);
    self
  }

  /// The bounded cross-node clock-uncertainty (`Some` enables LeaseGuard inherited-lease reads).
  #[inline(always)]
  pub const fn bounded_clock_uncertainty(&self) -> Option<Duration> {
    self.bounded_clock_uncertainty
  }

  /// Set the bounded cross-node clock-uncertainty (consuming; enables LeaseGuard inherited-lease reads).
  #[inline(always)]
  #[must_use]
  pub const fn with_bounded_clock_uncertainty(mut self, v: Duration) -> Self {
    self.set_bounded_clock_uncertainty(v);
    self
  }

  /// Set the bounded cross-node clock-uncertainty in place (enables LeaseGuard inherited-lease reads).
  #[inline(always)]
  pub const fn set_bounded_clock_uncertainty(&mut self, v: Duration) -> &mut Self {
    self.bounded_clock_uncertainty = Some(v);
    self
  }

  /// The SINGLE validating builder both parse mirrors funnel through: direct-construct the private
  /// fields from the already-parsed parts, then run [`Self::validate`]. Keeping the construction +
  /// validation in one place is what stops the two mirrors (serde's `ConfigSerde`, clap's `ConfigCli`)
  /// from drifting into duplicated — and divergent — validation logic; each one's `TryFrom` is just a
  /// field-by-field forward to here. Returns the specific [`ConfigError`] for an invalid combination.
  #[cfg(any(feature = "serde", feature = "clap"))]
  #[allow(clippy::too_many_arguments)]
  pub(crate) fn from_parts_validated(
    id: I,
    voters: Vec<I>,
    election_timeout: Duration,
    heartbeat_interval: Duration,
    max_size_per_msg: u64,
    max_inflight_msgs: usize,
    max_inflight_bytes: u64,
    snapshot_threshold: usize,
    step_down_on_removal: bool,
    pre_vote: bool,
    check_quorum: bool,
    disable_proposal_forwarding: bool,
    read_only: ReadOnlyOption,
    lease_refresh: LeaseRefresh,
    lease_duration: Option<Duration>,
    clock_drift_bound: Option<Duration>,
    bounded_clock_uncertainty: Option<Duration>,
  ) -> Result<Self, ConfigError> {
    let cfg = Self {
      id,
      voters,
      election_timeout,
      heartbeat_interval,
      max_size_per_msg,
      max_inflight_msgs,
      max_inflight_bytes,
      snapshot_threshold,
      step_down_on_removal,
      pre_vote,
      check_quorum,
      disable_proposal_forwarding,
      read_only,
      lease_refresh,
      lease_duration,
      clock_drift_bound,
      bounded_clock_uncertainty,
    };
    cfg.validate()?;
    Ok(cfg)
  }

  /// Validate the COMPLETE set of UNIVERSAL config invariants — the checks that must hold for ANY
  /// config, voter or observer alike. This is the single, authoritative invariant set that the
  /// parsed-config paths (serde `Deserialize` and clap `FromArgMatches`) route through, so a
  /// config produced by a file/CLI/env CANNOT carry a value the engine would choke on (a `0`
  /// `max_inflight_msgs` stalling replication, a zero `heartbeat_interval`, an `election_timeout`
  /// that does not exceed it, or an empty voter set).
  ///
  /// Enforces, in order:
  /// - non-zero `heartbeat_interval` ([`ConfigError::ZeroHeartbeat`]);
  /// - `election_timeout > heartbeat_interval` ([`ConfigError::ElectionNotGreaterThanHeartbeat`]);
  /// - non-zero `max_inflight_msgs` ([`ConfigError::ZeroInflight`]);
  /// - non-empty `voters` ([`ConfigError::EmptyVoters`]);
  /// - `ReadOnlyOption::LeaseBased` requires `check_quorum = true`
  ///   ([`ConfigError::LeaseRequiresCheckQuorum`]) — lease-based reads are only safe when
  ///   CheckQuorum keeps the election-timeout lease fresh; without it a stale leader could serve a
  ///   read after losing quorum;
  /// - the LeaseGuard timing + failover-tier invariants (the per-entry commit-wait window and the
  ///   `bounded_clock_uncertainty` bound).
  ///
  /// This deliberately does NOT require `id ∈ voters`: that is voter-role-specific (an OBSERVER's
  /// `id` is `∉ voters` by design — see [`Self::try_new_observer`]). A parsed config is a VOTER
  /// when `id ∈ voters` and an OBSERVER otherwise; both must satisfy the universal invariants
  /// above, so the `id ∈ voters` check stays in [`Self::try_new`] (the voter constructor), not here.
  ///
  /// `try_new` / `try_new_observer` do **not** call this automatically (so a builder chain can set
  /// every knob first); the parsed paths DO, rejecting an invalid config at parse/deserialize time.
  pub fn validate(&self) -> Result<(), ConfigError> {
    if self.heartbeat_interval.is_zero() {
      return Err(ConfigError::ZeroHeartbeat);
    }
    if self.election_timeout <= self.heartbeat_interval {
      return Err(ConfigError::ElectionNotGreaterThanHeartbeat {
        election: self.election_timeout,
        heartbeat: self.heartbeat_interval,
      });
    }
    // The per-term randomized timeout is a raw `Duration` add (`election_timeout + jitter`, jitter
    // `< election_timeout`); a value near `Duration::MAX` would overflow it and panic the first
    // election. Bound it `Instant`-safe so `2 · election_timeout` can never overflow.
    if self.election_timeout > MAX_ELECTION_TIMEOUT {
      return Err(ConfigError::ElectionTimeoutTooLarge {
        election: self.election_timeout,
        max: MAX_ELECTION_TIMEOUT,
      });
    }
    if self.max_inflight_msgs == 0 {
      return Err(ConfigError::ZeroInflight);
    }
    if self.voters.is_empty() {
      return Err(ConfigError::EmptyVoters);
    }
    if self.read_only == ReadOnlyOption::LeaseBased && !self.check_quorum {
      return Err(ConfigError::LeaseRequiresCheckQuorum);
    }
    // A proactive `lease_refresh` re-anchors the per-entry LeaseGuard timestamp; Safe and LeaseBased have
    // no such anchor, so the knob is meaningless there — reject rather than silently ignore.
    if self.lease_refresh != LeaseRefresh::Off && self.read_only != ReadOnlyOption::LeaseGuard {
      return Err(ConfigError::LeaseRefreshRequiresLeaseGuard);
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
    // age comparison vacuous. Reject via the SAME predicate the runtime activates on
    // ([`failover_tier_valid`](Self::failover_tier_valid)) so a rejected config can never silently
    // activate the failover tier at runtime (validation and `failover_tier_active` share one source of
    // truth). The window timing was already validated above, so for a LeaseGuard config the only
    // remaining failover condition is `ε_unc < Δ`; a non-LeaseGuard config carrying `ε_unc` is rejected
    // because `failover_tier_valid` requires LeaseGuard mode.
    if let Some(unc) = self.bounded_clock_uncertainty
      && !self.failover_tier_valid(self.read_only)
    {
      return Err(ConfigError::BoundedUncertaintyInvalid {
        uncertainty: unc,
        lease: self.lease_duration,
      });
    }
    Ok(())
  }

  /// Whether this config is a VALID, ACTIVE LeaseGuard FAILOVER tier — the SINGLE source of truth shared
  /// by [`validate`](Self::validate) (which surfaces the specific [`ConfigError`]) and the runtime
  /// `failover_tier_active` (which gates the inherited serve, the precise commit-anchor, and the
  /// synchronized-wall stamp). All three conditions: `LeaseGuard` read mode, a COMPUTABLE commit-wait
  /// window (Δ and ε_drift present with `ε_drift < Δ` and the window below the election timeout — the
  /// same [`leaseguard_commit_wait_ns`](Self::leaseguard_commit_wait_ns) check stamping uses), AND a
  /// bounded clock-uncertainty that is a real fraction of the lease (`ε_unc < Δ`, else the cross-node age
  /// comparison is vacuous). Keeping validation and the runtime gate on ONE predicate is what stops a
  /// config the crate would reject from activating the failover tier (the class of defect where
  /// `Endpoint::new` does not call `validate`).
  pub(crate) fn failover_tier_valid(&self, mode: ReadOnlyOption) -> bool {
    mode == ReadOnlyOption::LeaseGuard
      && self.leaseguard_commit_wait_ns(mode).is_some()
      && matches!(
        (self.bounded_clock_uncertainty, self.lease_duration),
        (Some(unc), Some(d)) if unc < d
      )
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
    // NOTE: the FAILOVER-tier E′ inflation of the conservative commit-wait (`max_lease_window · (1+ρ)`)
    // must also fit below the election timeout, but it is gated at RUNTIME (`inherited_serve_armed` in
    // `become_leader`), NOT here: the wait keys on `max_lease_window` — the MAX window INHERITED from
    // entries possibly stamped by another node's larger config — which config-time validation cannot
    // bound (there is no cluster-wide config check, §1). A node whose inflated wait would exceed its
    // election timeout simply does not arm the inherited serve that term (falling back to the shipped
    // bare wait); it is not an invalid config.
    Ok(w as u64)
  }

  /// The EXACT LeaseGuard commit-wait window (nanos) when the mode is ACTIVE and the config is valid,
  /// else `None`. The single source of truth that the per-entry stamp, the read fast-path, and the
  /// commit-wait all gate on.
  pub(crate) fn leaseguard_commit_wait_ns(&self, mode: ReadOnlyOption) -> Option<u64> {
    if mode != ReadOnlyOption::LeaseGuard {
      return None;
    }
    self.leaseguard_window_result().ok()
  }

  /// Whether THIS node may PROPOSE migrating to `mode` — i.e. it holds the target mode's required knobs.
  /// Into-LeaseGuard needs a valid commit-wait window (Δ + ε present, ε < Δ, window < election timeout);
  /// into-LeaseBased needs `check_quorum` (a non-enforcing follower cannot uphold the lease). Safe always
  /// validates. A straggler lacking the knobs safely degrades to Safe, so only the PROPOSER is checked
  /// (spec §7); the migration entry carries only the mode discriminant — knobs are pre-provisioned.
  pub(crate) fn read_mode_change_valid(&self, mode: ReadOnlyOption) -> bool {
    match mode {
      ReadOnlyOption::Safe => true,
      ReadOnlyOption::LeaseBased => self.check_quorum,
      ReadOnlyOption::LeaseGuard => self.leaseguard_window_result().is_ok(),
    }
  }
}

// The two optional layers each get their OWN private parse mirror, because their bound requirements
// differ and a shared mirror over-constrains one of them:
//
//  * `ConfigSerde<I>` (serde only) carries the serde field attrs (`default = "fn"` / `humantime_serde`
//    / `deny_unknown_fields`) and is bounded ONLY `I: Clone + PartialEq` (+ what the `Deserialize`
//    derive itself needs). It deserializes `id` / `voters` directly, so it needs no `FromStr`.
//  * `ConfigCli<I>` (clap only) carries the `#[arg(...)]` attrs and is bounded
//    `I: FromStr + Clone + Send + Sync + 'static` — the value-parser bounds clap's `derive(Args)`
//    needs (clap parses every arg from a string, so the `id` type must be `FromStr`); `clap::Args`
//    cannot derive on `Config<I>` itself because `value_parser!` cannot resolve a parser for an
//    unbounded generic, and putting `I: FromStr` on `Config` would cascade that bound onto the whole
//    engine.
//
// Splitting them is what keeps the serde path from inheriting clap's `FromStr` requirement — a custom
// `NodeId` that is `Deserialize` but NOT `FromStr` deserializes fine. Both mirrors funnel through the
// shared validating builder [`Config::from_parts_validated`] via their respective `TryFrom`.

/// Serde-only parse mirror for [`Config`]. NOT part of the public API — it exists only to carry the
/// serde per-knob deserialize defaults / humantime adapters and is always converted to a validated
/// [`Config`] via [`TryFrom`]. Bounded only `I: Clone + PartialEq` (it deserializes `id` / `voters`
/// directly — no `FromStr`), so a custom `NodeId` that impls `Deserialize` but not `FromStr` works.
#[cfg(feature = "serde")]
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields, bound(deserialize = "I: serde::Deserialize<'de>"))]
struct ConfigSerde<I>
where
  I: Clone + PartialEq,
{
  id: I,
  voters: Vec<I>,
  #[serde(default = "default_election_timeout", with = "humantime_serde")]
  election_timeout: Duration,
  #[serde(default = "default_heartbeat_interval", with = "humantime_serde")]
  heartbeat_interval: Duration,
  #[serde(default = "default_max_size_per_msg")]
  max_size_per_msg: u64,
  #[serde(default = "default_max_inflight_msgs")]
  max_inflight_msgs: usize,
  #[serde(default = "default_max_inflight_bytes")]
  max_inflight_bytes: u64,
  #[serde(default = "default_snapshot_threshold")]
  snapshot_threshold: usize,
  #[serde(default = "default_step_down_on_removal")]
  step_down_on_removal: bool,
  #[serde(default = "default_pre_vote")]
  pre_vote: bool,
  #[serde(default = "default_check_quorum")]
  check_quorum: bool,
  #[serde(default = "default_disable_proposal_forwarding")]
  disable_proposal_forwarding: bool,
  #[serde(default)]
  read_only: ReadOnlyOption,
  #[serde(default)]
  lease_refresh: LeaseRefresh,
  #[serde(default, with = "humantime_serde")]
  lease_duration: Option<Duration>,
  #[serde(default, with = "humantime_serde")]
  clock_drift_bound: Option<Duration>,
  #[serde(default, with = "humantime_serde")]
  bounded_clock_uncertainty: Option<Duration>,
}

#[cfg(feature = "serde")]
impl<I> TryFrom<ConfigSerde<I>> for Config<I>
where
  I: Clone + PartialEq,
{
  type Error = ConfigError;

  fn try_from(c: ConfigSerde<I>) -> Result<Self, Self::Error> {
    Self::from_parts_validated(
      c.id,
      c.voters,
      c.election_timeout,
      c.heartbeat_interval,
      c.max_size_per_msg,
      c.max_inflight_msgs,
      c.max_inflight_bytes,
      c.snapshot_threshold,
      c.step_down_on_removal,
      c.pre_vote,
      c.check_quorum,
      c.disable_proposal_forwarding,
      c.read_only,
      c.lease_refresh,
      c.lease_duration,
      c.clock_drift_bound,
      c.bounded_clock_uncertainty,
    )
  }
}

/// Clap-only parse mirror for [`Config`]. NOT part of the public API — it exists only to carry the clap
/// `Args` derive (which needs the `I: FromStr` value-parser bounds `Config` must not impose) and the
/// per-knob `#[arg(...)]` attributes, and is always converted to a validated [`Config`] via [`TryFrom`].
#[cfg(feature = "clap")]
use core::str::FromStr;

#[cfg(feature = "clap")]
#[derive(clap::Args)]
struct ConfigCli<I>
where
  I: FromStr + Clone + Send + Sync + 'static,
  <I as FromStr>::Err: std::error::Error + Send + Sync + 'static,
{
  #[arg(id = "config-id", long = "id", env = "SAILING_ID")]
  id: I,
  #[arg(
    id = "config-voters",
    long = "voter",
    env = "SAILING_VOTERS",
    value_delimiter = ','
  )]
  voters: Vec<I>,
  #[arg(
    id = "config-election-timeout",
    long = "election-timeout",
    env = "SAILING_ELECTION_TIMEOUT",
    value_parser = humantime::parse_duration,
    default_value = "1s"
  )]
  election_timeout: Duration,
  #[arg(
    id = "config-heartbeat-interval",
    long = "heartbeat-interval",
    env = "SAILING_HEARTBEAT_INTERVAL",
    value_parser = humantime::parse_duration,
    default_value = "100ms"
  )]
  heartbeat_interval: Duration,
  #[arg(
    id = "config-max-size-per-msg",
    long = "max-size-per-msg",
    env = "SAILING_MAX_SIZE_PER_MSG",
    default_value_t = DEFAULT_MAX_SIZE_PER_MSG
  )]
  max_size_per_msg: u64,
  #[arg(
    id = "config-max-inflight-msgs",
    long = "max-inflight-msgs",
    env = "SAILING_MAX_INFLIGHT_MSGS",
    default_value_t = DEFAULT_MAX_INFLIGHT_MSGS
  )]
  max_inflight_msgs: usize,
  #[arg(
    id = "config-max-inflight-bytes",
    long = "max-inflight-bytes",
    env = "SAILING_MAX_INFLIGHT_BYTES",
    default_value_t = DEFAULT_MAX_INFLIGHT_BYTES
  )]
  max_inflight_bytes: u64,
  #[arg(
    id = "config-snapshot-threshold",
    long = "snapshot-threshold",
    env = "SAILING_SNAPSHOT_THRESHOLD",
    default_value_t = DEFAULT_SNAPSHOT_THRESHOLD
  )]
  snapshot_threshold: usize,
  // The bool knobs take an explicit `true` / `false` VALUE (`ArgAction::Set`) rather than the
  // derive's default flag (`SetTrue`) action: `step_down_on_removal` DEFAULTS to `true`, which a
  // presence-only flag could never turn off. `Set` keeps every bool settable both ways from its
  // `DEFAULT_*` (the one deviation sailing's true-defaulting bool forces over the memberlist mirror,
  // whose bools all default `false`).
  #[arg(
    id = "config-step-down-on-removal",
    long = "step-down-on-removal",
    env = "SAILING_STEP_DOWN_ON_REMOVAL",
    action = clap::ArgAction::Set,
    default_value_t = DEFAULT_STEP_DOWN_ON_REMOVAL
  )]
  step_down_on_removal: bool,
  #[arg(
    id = "config-pre-vote",
    long = "pre-vote",
    env = "SAILING_PRE_VOTE",
    action = clap::ArgAction::Set,
    default_value_t = DEFAULT_PRE_VOTE
  )]
  pre_vote: bool,
  #[arg(
    id = "config-check-quorum",
    long = "check-quorum",
    env = "SAILING_CHECK_QUORUM",
    action = clap::ArgAction::Set,
    default_value_t = DEFAULT_CHECK_QUORUM
  )]
  check_quorum: bool,
  #[arg(
    id = "config-disable-proposal-forwarding",
    long = "disable-proposal-forwarding",
    env = "SAILING_DISABLE_PROPOSAL_FORWARDING",
    action = clap::ArgAction::Set,
    default_value_t = DEFAULT_DISABLE_PROPOSAL_FORWARDING
  )]
  disable_proposal_forwarding: bool,
  // The enum knobs render their default as the `ValueEnum`'s snake_case possible-value (matching
  // `as_str()` / serde) — a literal `default_value`, not `default_value_t`, so the default does not
  // depend on the enums' `Display` (which yields the CamelCase variant name, not the wire spelling).
  #[arg(
    id = "config-read-only",
    long = "read-only",
    env = "SAILING_READ_ONLY",
    default_value = "safe",
    value_enum
  )]
  read_only: ReadOnlyOption,
  #[arg(
    id = "config-lease-refresh",
    long = "lease-refresh",
    env = "SAILING_LEASE_REFRESH",
    default_value = "off",
    value_enum
  )]
  lease_refresh: LeaseRefresh,
  #[arg(
    id = "config-lease-duration",
    long = "lease-duration",
    env = "SAILING_LEASE_DURATION",
    value_parser = humantime::parse_duration
  )]
  lease_duration: Option<Duration>,
  #[arg(
    id = "config-clock-drift-bound",
    long = "clock-drift-bound",
    env = "SAILING_CLOCK_DRIFT_BOUND",
    value_parser = humantime::parse_duration
  )]
  clock_drift_bound: Option<Duration>,
  #[arg(
    id = "config-bounded-clock-uncertainty",
    long = "bounded-clock-uncertainty",
    env = "SAILING_BOUNDED_CLOCK_UNCERTAINTY",
    value_parser = humantime::parse_duration
  )]
  bounded_clock_uncertainty: Option<Duration>,
}

#[cfg(feature = "clap")]
impl<I> TryFrom<ConfigCli<I>> for Config<I>
where
  I: FromStr + Clone + Send + Sync + 'static + PartialEq,
  <I as FromStr>::Err: std::error::Error + Send + Sync + 'static,
{
  type Error = ConfigError;

  fn try_from(c: ConfigCli<I>) -> Result<Self, Self::Error> {
    Self::from_parts_validated(
      c.id,
      c.voters,
      c.election_timeout,
      c.heartbeat_interval,
      c.max_size_per_msg,
      c.max_inflight_msgs,
      c.max_inflight_bytes,
      c.snapshot_threshold,
      c.step_down_on_removal,
      c.pre_vote,
      c.check_quorum,
      c.disable_proposal_forwarding,
      c.read_only,
      c.lease_refresh,
      c.lease_duration,
      c.clock_drift_bound,
      c.bounded_clock_uncertainty,
    )
  }
}

#[cfg(feature = "clap")]
#[cfg_attr(docsrs, doc(cfg(feature = "clap")))]
const _: () = {
  use clap::{ArgMatches, Args, Command, Error, FromArgMatches, parser::ValueSource};

  // Map a parse-time [`ConfigError`] to a clap value-validation error so an invalid CLI/env config
  // surfaces through clap's own error path (exit code, formatted message) rather than building an
  // unrunnable node.
  fn config_err(e: ConfigError) -> Error {
    Error::raw(clap::error::ErrorKind::ValueValidation, e)
  }

  impl<I> Args for Config<I>
  where
    I: FromStr + Clone + Send + Sync + 'static + PartialEq,
    <I as FromStr>::Err: std::error::Error + Send + Sync + 'static,
  {
    fn augment_args(cmd: Command) -> Command {
      ConfigCli::<I>::augment_args(cmd)
    }

    fn augment_args_for_update(cmd: Command) -> Command {
      ConfigCli::<I>::augment_args_for_update(cmd)
    }
  }

  impl<I> FromArgMatches for Config<I>
  where
    I: FromStr + Clone + Send + Sync + 'static + PartialEq,
    <I as FromStr>::Err: std::error::Error + Send + Sync + 'static,
  {
    fn from_arg_matches(m: &ArgMatches) -> Result<Self, Error> {
      // Parse the mirror, then route through the VALIDATING `TryFrom` so an invalid CLI/env config
      // is rejected at parse time, not silently built.
      let cli = ConfigCli::<I>::from_arg_matches(m)?;
      Config::try_from(cli).map_err(config_err)
    }

    fn update_from_arg_matches(&mut self, m: &ArgMatches) -> Result<(), Error> {
      // TRANSACTIONAL update: apply every override to a `candidate` CLONE, validate the candidate,
      // and commit it back to `self` only on success. A rejected update (e.g. `--max-inflight-msgs 0`
      // or `--heartbeat-interval 0s`) leaves `self` byte-for-byte unchanged, so a caller that catches
      // the clap error and keeps its config can never end up holding a half-applied invalid `Config`.
      let mut candidate = self.clone();
      // Apply ONLY operator-supplied overrides — args whose value came from the command line or an
      // env var, not a clap default. A bare derived update treats every `default_value` arg as
      // present and would reset unset fields back to their clap defaults.
      macro_rules! take {
        ($id:literal, $field:ident, $ty:ty) => {
          if matches!(
            m.value_source($id),
            Some(ValueSource::CommandLine) | Some(ValueSource::EnvVariable)
          ) {
            if let Some(v) = m.get_one::<$ty>($id) {
              candidate.$field = v.clone();
            }
          }
        };
      }
      macro_rules! take_opt {
        ($id:literal, $field:ident, $ty:ty) => {
          if matches!(
            m.value_source($id),
            Some(ValueSource::CommandLine) | Some(ValueSource::EnvVariable)
          ) {
            candidate.$field = m.get_one::<$ty>($id).cloned();
          }
        };
      }
      macro_rules! take_vec {
        ($id:literal, $field:ident, $ty:ty) => {
          if matches!(
            m.value_source($id),
            Some(ValueSource::CommandLine) | Some(ValueSource::EnvVariable)
          ) {
            if let Some(vs) = m.get_many::<$ty>($id) {
              candidate.$field = vs.cloned().collect();
            }
          }
        };
      }
      take!("config-id", id, I);
      take_vec!("config-voters", voters, I);
      take!("config-election-timeout", election_timeout, Duration);
      take!("config-heartbeat-interval", heartbeat_interval, Duration);
      take!("config-max-size-per-msg", max_size_per_msg, u64);
      take!("config-max-inflight-msgs", max_inflight_msgs, usize);
      take!("config-max-inflight-bytes", max_inflight_bytes, u64);
      take!("config-snapshot-threshold", snapshot_threshold, usize);
      take!("config-step-down-on-removal", step_down_on_removal, bool);
      take!("config-pre-vote", pre_vote, bool);
      take!("config-check-quorum", check_quorum, bool);
      take!(
        "config-disable-proposal-forwarding",
        disable_proposal_forwarding,
        bool
      );
      take!("config-read-only", read_only, ReadOnlyOption);
      take!("config-lease-refresh", lease_refresh, LeaseRefresh);
      take_opt!("config-lease-duration", lease_duration, Duration);
      take_opt!("config-clock-drift-bound", clock_drift_bound, Duration);
      take_opt!(
        "config-bounded-clock-uncertainty",
        bounded_clock_uncertainty,
        Duration
      );
      // Validate before committing, so a rejected update leaves `self` untouched (see above).
      candidate.validate().map_err(config_err)?;
      *self = candidate;
      Ok(())
    }
  }
};
#[cfg(test)]
mod tests;
