//! Multi-group reactor drivers: ONE `Send` task hosting N co-located Raft groups over a shared
//! transport (a [`MultiStreamCoordinator`](sailing_proto::MultiStreamCoordinator) or
//! [`MultiQuicCoordinator`](sailing_proto::MultiQuicCoordinator)) and a shared storage engine
//! ([`GroupEngine`](sailing_proto::GroupEngine)) with one batched durability barrier per crank.
//!
//! The single-group run loops generalize, they do not change shape: the socket/timer/command
//! plumbing is the single drivers' verbatim, the per-operation state
//! ([`Routing`](sailing_driver::shared::Routing)) is the single-group type instantiated PER GROUP,
//! and the one armed deadline folds every group's earliest consensus deadline in. Groups arrive
//! and leave at runtime through [`MultiCommand`](sailing_driver::MultiCommand) lifecycle commands
//! — the drivers bind EMPTY — and the drivers surface the placement TRIGGERS (unknown-group
//! solicitations, removed-self conf changes) on the
//! [`MultiHandle::lifecycle`](sailing_driver::MultiHandle::lifecycle) tail: the placement policy
//! itself — where a group should live, when to tear a removed replica down — is explicitly the
//! embedder's, whether a PD-style external placer or a Cockroach-style local one.

mod quic;
mod stream;

pub use quic::MultiReactorQuicDriver;
pub use stream::MultiReactorStreamDriver;

use std::sync::{
  Arc,
  atomic::{AtomicU64, Ordering},
};

use crate::DriverError;

/// How many times one group's `handle_storage` is re-driven within a single crank while it
/// reports `MorePending`; the remainder rides the next crank (the crank keeps the storage-redrive
/// deadline immediate), so no single group's completion backlog can monopolize a pass.
pub(crate) const STORAGE_REDRIVES: usize = 4;

/// The uniform verdict for an operation addressed to a group this host does not carry.
pub(crate) fn no_such_group<I>() -> DriverError<I> {
  DriverError::Rejected {
    reason: "no such group on this host".to_string(),
  }
}

/// Flatten a typed lifecycle/config refusal onto the driver's `Rejected` surface — the same
/// flatten the proto's propose/transfer/read errors get at the single-group boundary.
pub(crate) fn rejected<I>(e: impl core::fmt::Display) -> DriverError<I> {
  DriverError::Rejected {
    reason: e.to_string(),
  }
}

/// Whether `conf` still names `me` in ANY membership role — voter in either joint half, learner,
/// or incoming learner. The complement is the REMOVED-SELF trigger: a committed configuration
/// that no longer names the host anywhere (during a joint phase the outgoing half still counts,
/// so a leaving member fires only on the final, fully-departed configuration).
pub(crate) fn conf_names<I: Ord>(conf: &sailing_proto::ConfState<I>, me: &I) -> bool {
  conf.voters().contains(me)
    || conf.voters_outgoing().contains(me)
    || conf.learners().contains(me)
    || conf.learners_next().contains(me)
}

/// A group's last OBSERVED consensus state and the instant it last changed — the idle clock the
/// quiesce sweep reads. State observation (rather than dispatch counting) is the activity signal
/// because an idle group's heartbeat exchange dispatches forever without changing any of these.
pub(crate) struct GroupActivity {
  pub(crate) term: sailing_proto::Term,
  pub(crate) commit: sailing_proto::Index,
  pub(crate) applied: sailing_proto::Index,
  pub(crate) at: std::time::Instant,
}

/// Whether a group's CONSENSUS state is quiesce-eligible on the leader: this node leads it in the
/// `Safe` read mode (the lease modes are ineligible in v1 — a LeaseGuard/LeaseBased leader must
/// keep renewing through heartbeat rounds), every voter in BOTH joint halves has matched the
/// leader's own durable last index, and that index is fully committed AND applied — nothing is in
/// flight anywhere. The leader's own [`peer_progress`](sailing_proto::Endpoint::peer_progress)
/// match is the durable-last read (public surface): it equals the log's last index exactly when
/// every local append is durable, an extra conservatism consistent with idleness. Learners are
/// deliberately NOT gated on (a catching-up learner's appends are response-driven, not
/// timer-driven, so quiescence does not stall it); a learner-aware refinement is a seam.
///
/// The driver adds its own conditions on top: no parked driver work for the group and a full
/// election timeout of observed inactivity.
pub(crate) fn group_idle<I, F>(ep: &sailing_proto::Endpoint<I, F>) -> bool
where
  I: sailing_proto::NodeId,
  F: sailing_proto::StateMachine,
{
  if ep.is_poisoned()
    || !ep.role().is_leader()
    || !matches!(ep.active_read_mode(), sailing_proto::ReadOnlyOption::Safe)
  {
    return false;
  }
  let commit = ep.commit_index();
  if commit != ep.applied_index() {
    return false;
  }
  let Some(mine) = ep.peer_progress(&ep.id()) else {
    return false;
  };
  let last = mine.match_index;
  if last != commit {
    return false;
  }
  let conf = ep.conf_state();
  conf
    .voters()
    .iter()
    .chain(conf.voters_outgoing().iter())
    .all(|v| ep.peer_progress(v).is_some_and(|p| p.match_index == last))
}

/// Cross-thread observability for a multi driver: the driver republishes the shared engine's
/// [`flushes`](sailing_proto::GroupEngine::flushes) /
/// [`ops_flushed`](sailing_proto::GroupEngine::ops_flushed) counters after every storage crank —
/// so a test or operator thread can watch the fsync-amortization ratio (operations per barrier)
/// while the driver task exclusively owns the engine — plus the QUIESCED-group gauge the driver's
/// scheduler maintains (idle groups whose timers it neither arms nor sweeps), published after
/// every quiesce sweep. Obtain it from the driver BEFORE spawning `run()` (e.g.
/// `driver.engine_metrics()`); clones share the counters.
#[derive(Clone, Default)]
pub struct EngineMetrics {
  inner: Arc<EngineMetricsInner>,
}

#[derive(Default)]
struct EngineMetricsInner {
  flushes: AtomicU64,
  ops_flushed: AtomicU64,
  quiesced_groups: AtomicU64,
}

impl EngineMetrics {
  /// Durability barriers run so far (every [`GroupEngine::flush`](sailing_proto::GroupEngine::flush)
  /// call counts, including one that released nothing).
  #[must_use]
  pub fn flushes(&self) -> u64 {
    self.inner.flushes.load(Ordering::Acquire)
  }

  /// Total storage operations completed across every barrier so far. `ops_flushed / flushes` is
  /// the cross-group batch factor the shared engine exists for.
  #[must_use]
  pub fn ops_flushed(&self) -> u64 {
    self.inner.ops_flushed.load(Ordering::Acquire)
  }

  /// The number of hosted groups currently QUIESCED (idle: the driver neither arms nor sweeps
  /// their consensus deadlines; any traffic, a local command, or a connection loss wakes them).
  #[must_use]
  pub fn quiesced_groups(&self) -> u64 {
    self.inner.quiesced_groups.load(Ordering::Acquire)
  }

  /// Publish the engine's current counters (driver-side, once per crank).
  pub(crate) fn record(&self, flushes: u64, ops_flushed: u64) {
    self.inner.flushes.store(flushes, Ordering::Release);
    self.inner.ops_flushed.store(ops_flushed, Ordering::Release);
  }

  /// Publish the quiesced-group gauge (driver-side, once per quiesce sweep).
  pub(crate) fn record_quiesced(&self, quiesced: u64) {
    self
      .inner
      .quiesced_groups
      .store(quiesced, Ordering::Release);
  }
}
