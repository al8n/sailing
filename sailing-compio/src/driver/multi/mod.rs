//! Multi-group compio drivers: one `!Send` thread-pinned task hosting N co-located Raft groups
//! over a shared transport ([`MultiStreamCoordinator`](sailing_proto::MultiStreamCoordinator))
//! and a shared storage engine ([`GroupEngine`](sailing_proto::GroupEngine)) with one batched
//! durability barrier per crank.
//!
//! This is the proactor port of the reactor multi drivers — full feature parity ON ONE CORE:
//! the multi crank (the engine-flush storage step, the per-group completion drains, the
//! quiesce-aware aggregate deadline fold, the per-group pump tails), the lifecycle mechanics
//! (tombstones, unknown-group surfacing, removed-self), and the group factory all run the
//! reactor loop's logic verbatim over compio idioms (thread-pinned `Rc`/`lochan` plumbing in
//! place of the `Send` future's `Arc`/`flume`). The single-group compio driver's
//! socket/timer/command plumbing is reused unchanged; only the consensus-facing steps fan out
//! per group. Scale-out ACROSS cores is the sharded host's job — K of these drivers, one per
//! core, each a complete plane.
//!
//! Groups arrive and leave at runtime through [`MultiCommand`](sailing_driver::MultiCommand)
//! lifecycle commands — the driver binds EMPTY — and the driver surfaces the placement TRIGGERS
//! (unknown-group solicitations, removed-self conf changes) on the
//! [`MultiHandle::lifecycle`](sailing_driver::MultiHandle::lifecycle) tail: the placement policy
//! itself is explicitly the embedder's. A registered
//! [`GroupFactory`](sailing_driver::GroupFactory) (`with_group_factory`) moves the admission
//! EDGE into the driver crank — a solicited group the factory recognizes materializes
//! hands-free — while the decisions stay wherever the embedder's policy lives.

mod sharded;
mod stream;
#[cfg(test)]
mod tests;

pub use sharded::{ShardMap, ShardRecordLayers, ShardedCompioHost, ShardedMultiHandle, SpawnError};
pub use stream::CompioMultiStreamDriver;

use std::sync::{
  Arc,
  atomic::{AtomicU64, Ordering},
};

use sailing_proto::{Event, StateMachine};

use crate::{DriverConfig, DriverError};

use sailing_driver::{LifecycleEvent, shared::InflightBudget};

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

/// Whether a factory blueprint's seed config names the soliciting peer `from` — the SENDER leg
/// of the factory admission edge, enforced by the drivers fail-closed BEFORE the factory's
/// build (resource) phase is asked for a state machine. A legitimate solicitation is
/// INITIAL-SHAPED consensus traffic — a campaigner's vote request or a leader's first-contact
/// heartbeat — so its sender is by construction a member of the group it solicits; a blueprint
/// that does not name the solicitor is either a stale catalog view (membership moved on: refuse
/// here, the signal falls to the lifecycle tail for the embedder's manual path) or an
/// unauthorized cross-domain solicitation from a peer that merely shares transport
/// authentication (refuse, period). The seed [`Config`](sailing_proto::Config) is a simple
/// single configuration whose only membership set is the voter list — a joining host's own
/// learner shape is its OWN id's absence from the voters (`try_new_observer`), never a remote
/// learner list, and only voters campaign or lead — so "names" is exactly voter-list
/// containment.
pub(crate) fn blueprint_names<I: PartialEq>(
  blueprint: &sailing_driver::GroupBlueprint<I>,
  from: &I,
) -> bool {
  blueprint.config_ref().voters().contains(from)
}

/// A pre-read copy of ONE id's lineage, taken from the engine BEFORE its stores are lent out:
/// the coordinator's restore-time floor check and the engine's `(log, stable)` handles cannot
/// borrow the engine simultaneously, and the copy is exact for the single id the call admits
/// (monotone lineage cannot move under a serial driver task).
pub(crate) struct FloorSnapshot {
  pub(crate) floor: u64,
  pub(crate) lineage: u64,
}

impl<G> sailing_proto::FloorStore<G> for FloorSnapshot {
  fn floor(&self, _gid: &G) -> u64 {
    self.floor
  }

  fn lineage(&self, _gid: &G) -> u64 {
    self.lineage
  }
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
/// keep renewing through heartbeat rounds), NO tracked peer is lagging
/// ([`has_lagging_peer`](sailing_proto::Endpoint::has_lagging_peer) — the exact complement of the
/// heartbeat-response append pump condition plus the snapshot-recovery resend, over EVERY tracked
/// peer, learners included: a peer that is probing, receiving a snapshot, or matched short of the
/// leader's durable last index still draws catch-up traffic, so the final flagged round would not
/// settle to the bare `Heartbeat` + `HeartbeatResponse` the wake classification absorbs — and
/// since catch-up is response-driven and a quiesced leader stops beating, a behind-and-down peer
/// would otherwise stall until an unrelated wake), and the leader's own durable last index is
/// fully committed AND applied — nothing is in flight anywhere. The leader's own
/// [`peer_progress`](sailing_proto::Endpoint::peer_progress) match is the durable-last read
/// (public surface): it equals the log's last index exactly when every local append is durable,
/// an extra conservatism consistent with idleness. Voter completeness is implied: every voter in
/// BOTH joint halves sits in the lagging sweep, so eligibility means every voter — and every
/// learner — has matched the leader's durable last index.
///
/// The driver adds its own conditions on top: no parked driver work for the group and a full
/// election timeout of observed inactivity.
pub(crate) fn group_idle<I, F>(ep: &sailing_proto::Endpoint<I, F>) -> bool
where
  I: sailing_proto::NodeId,
  F: StateMachine,
{
  if ep.is_poisoned()
    || !ep.role().is_leader()
    || !matches!(ep.active_read_mode(), sailing_proto::ReadOnlyOption::Safe)
    || ep.has_lagging_peer()
  {
    return false;
  }
  let commit = ep.commit_index();
  if commit != ep.applied_index() {
    return false;
  }
  ep.peer_progress(&ep.id())
    .is_some_and(|mine| mine.match_index == commit)
}

/// The fan-in tails a multi driver publishes into, bundled so CO-LOCATED drivers can SHARE one
/// set: the group-stamped events channel, the lifecycle channel, and the host-wide
/// [`InflightBudget`]. A single-plane `bind` creates its own from the [`DriverConfig`] caps and
/// returns a standard `MultiHandle` over it; the sharded host creates ONE and hands every plane
/// a clone, so K planes fan into one events tail, one lifecycle tail, and one in-flight bound by
/// construction — no merge task anywhere.
pub(crate) struct SharedTails<G, I, F>
where
  F: StateMachine,
{
  pub(crate) events_tx: flume::Sender<(G, Event<I, F::Response>)>,
  pub(crate) events_rx: flume::Receiver<(G, Event<I, F::Response>)>,
  pub(crate) lifecycle_tx: flume::Sender<LifecycleEvent<G, I>>,
  pub(crate) lifecycle_rx: flume::Receiver<LifecycleEvent<G, I>>,
  pub(crate) budget: InflightBudget,
}

impl<G, I, F> SharedTails<G, I, F>
where
  F: StateMachine,
{
  /// Build one set of tails from the config's caps (the same sizing a single-group `bind` uses:
  /// bounded, dropped-on-full events/lifecycle; the budget is the binding in-flight bound).
  pub(crate) fn new(driver_cfg: &DriverConfig) -> Self {
    let (events_tx, events_rx) = flume::bounded(driver_cfg.events_cap);
    let (lifecycle_tx, lifecycle_rx) = flume::bounded(driver_cfg.events_cap);
    let budget = InflightBudget::new(driver_cfg.max_inflight, driver_cfg.max_pending_bytes);
    Self {
      events_tx,
      events_rx,
      lifecycle_tx,
      lifecycle_rx,
      budget,
    }
  }
}

impl<G, I, F> Clone for SharedTails<G, I, F>
where
  F: StateMachine,
{
  fn clone(&self) -> Self {
    Self {
      events_tx: self.events_tx.clone(),
      events_rx: self.events_rx.clone(),
      lifecycle_tx: self.lifecycle_tx.clone(),
      lifecycle_rx: self.lifecycle_rx.clone(),
      budget: self.budget.clone(),
    }
  }
}

/// Cross-thread observability for a multi driver: the driver republishes the shared engine's
/// [`flushes`](sailing_proto::GroupEngine::flushes) /
/// [`ops_flushed`](sailing_proto::GroupEngine::ops_flushed) counters after every storage crank —
/// so a test or operator thread can watch the fsync-amortization ratio (operations per barrier)
/// while the driver task exclusively owns the engine — plus the QUIESCED-group gauge the driver's
/// scheduler maintains (idle groups whose timers it neither arms nor sweeps), published after
/// every quiesce sweep. Obtain it from the driver BEFORE spawning `run()` (e.g.
/// `driver.engine_metrics()`); clones share the counters. The counters are `Arc`-shared atomics
/// even though the driver is `!Send`: the DRIVER stays pinned to its thread, the metrics handle
/// is what crosses.
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
