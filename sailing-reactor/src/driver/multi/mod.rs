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
//! embedder's, whether a PD-style external placer or a Cockroach-style local one. A registered
//! [`GroupFactory`](sailing_driver::GroupFactory) (`with_group_factory`) moves the admission
//! EDGE into the driver crank — a solicited group the factory recognizes materializes
//! hands-free, CockroachDB's `getOrCreateReplica` shape — while the decisions stay wherever the
//! embedder's policy lives.

mod quic;
mod stream;
#[cfg(test)]
mod tests;

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

/// Force pre-vote + check-quorum on for a RESHAPE-BORN group's config. A reshaping id's steady-state
/// membership churn is exactly where an ignorant removed voter would otherwise depose a live leader.
/// Applied UNCONDITIONALLY at the split-child birth path (a split child is reshape-born by
/// construction) and, via [`reshape_born_factory_config`], to the reshape/rejoin subset of factory
/// materializations. Independent of the seed config's flags; embedder `with_group` groups and fresh
/// day-0 factory births keep their configured (etcd-parity) defaults, so single-group deployments and
/// the VOPR corpus are untouched.
pub(crate) fn reshape_born_prevention<I>(
  config: sailing_proto::Config<I>,
) -> sailing_proto::Config<I> {
  config.with_pre_vote(true).with_check_quorum(true)
}

/// The factory-materialization gate for [`reshape_born_prevention`]: force prevention only on the
/// RESHAPE/rejoin subset, so a fresh day-0 materialization keeps the caller's config byte-for-byte.
/// Two legs: `generation > 0` (a recreated/reshaped incarnation) OR an OBSERVER-shaped blueprint (the
/// host's own id absent from the seed voters). Observer-shape ⇔ fork-born is enforced in-repo — a
/// full-voter blueprint for a fork-born id fuses committed histories (the loopback regression
/// `full_voter_blueprint_for_a_fork_born_id_fuses_histories`) — so a legitimate fork-born blueprint is
/// always the observer shape, and generation alone would miss it: fork children are born at
/// generation 0, which conflates incarnation with reshape-born-ness. The flags persist across the
/// observer-to-voter promotion (a conf change swaps membership, not the config knobs), so forcing at
/// materialization is sufficient. Accepted over-reach, the conservative direction: an observer-shaped
/// blueprint is always treated as reshape-born, so a hypothetical day-0 learner materialization
/// inherits prevention.
pub(crate) fn reshape_born_factory_config<I: sailing_proto::NodeId>(
  generation: u64,
  config: sailing_proto::Config<I>,
) -> sailing_proto::Config<I> {
  if generation > 0 || !config.is_voter(config.id()) {
    reshape_born_prevention(config)
  } else {
    config
  }
}

/// Map the proto's split-propose error, preserving the redirect hint and the poison verdict
/// exactly as [`map_propose_err`](crate::driver::map_propose_err) does for plain proposals.
pub(crate) fn map_split_err<I: core::fmt::Debug>(
  e: sailing_proto::SplitError<I>,
) -> DriverError<I> {
  match e {
    sailing_proto::SplitError::NotLeader { leader } => DriverError::NotLeader { leader },
    sailing_proto::SplitError::Propose(inner) => crate::driver::map_propose_err(inner),
    other => DriverError::Rejected {
      reason: format!("{other:?}"),
    },
  }
}

/// Map the proto's merge-verb error — the split mapping's sibling, same redirect and poison
/// preservation.
pub(crate) fn map_merge_err<I: core::fmt::Debug>(
  e: sailing_proto::MergeError<I>,
) -> DriverError<I> {
  match e {
    sailing_proto::MergeError::NotLeader { leader } => DriverError::NotLeader { leader },
    sailing_proto::MergeError::Propose(inner) => crate::driver::map_propose_err(inner),
    other => DriverError::Rejected {
      reason: format!("{other:?}"),
    },
  }
}

/// The election-jitter seed a driver mints for a RELAYED fork's child (no embedder call supplies
/// one): an FNV-1a fold of the host's node id, so co-located children fold further per group
/// (the container's `group_seed`) while REPLICAS of one child draw distinct jitter — the same
/// decorrelation embedders get by passing their node id as the seed.
pub(crate) fn host_seed<I: sailing_proto::Data>(host: Option<&I>) -> u64 {
  let Some(host) = host else { return 0 };
  let mut buf = std::vec::Vec::new();
  host.encode(&mut buf);
  let mut h = 0xcbf2_9ce4_8422_2325_u64;
  for b in &buf {
    h ^= u64::from(*b);
    h = h.wrapping_mul(0x0000_0100_0000_01b3);
  }
  h
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

/// A pre-read copy of TWO ids' lineage records — the merge verbs' floor seam (the engine is
/// lent to the propose as `(log, stable)`, so the coordinator's per-call floor reads come from
/// this snapshot). Exact for the two ids a merge names; any other id answers the no-fence zero.
pub(crate) struct PairFloors<G> {
  entries: [(G, u64, u64); 2],
}

impl<G: sailing_proto::GroupId> PairFloors<G> {
  /// Snapshot `source` and `target`'s floors + lineage off the engine.
  pub(crate) fn snapshot<I>(
    engine: &sailing_proto::GroupEngine<G, I>,
    source: &G,
    target: &G,
  ) -> Self {
    let read = |gid: &G| {
      (
        gid.cheap_clone(),
        engine.group_floor(gid),
        engine.group_gen(gid),
      )
    };
    Self {
      entries: [read(source), read(target)],
    }
  }
}

impl<G: PartialEq> sailing_proto::FloorStore<G> for PairFloors<G> {
  fn floor(&self, gid: &G) -> u64 {
    self
      .entries
      .iter()
      .find(|(g, _, _)| g == gid)
      .map_or(0, |(_, floor, _)| *floor)
  }

  fn lineage(&self, gid: &G) -> u64 {
    self
      .entries
      .iter()
      .find(|(g, _, _)| g == gid)
      .map_or(0, |(_, _, lineage)| *lineage)
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
  F: sailing_proto::StateMachine,
{
  if ep.is_poisoned()
    || !ep.role().is_leader()
    || !matches!(ep.active_read_mode(), sailing_proto::ReadOnlyOption::Safe)
    || ep.has_lagging_peer()
    // A parked merge is resolved by the per-crank service, which a quiesced group would never
    // reach. Structurally redundant today (a park always leaves commit > applied, which the
    // caught-up check below already refuses) — kept explicit so quiesce eligibility never
    // silently inherits that coupling.
    || ep.pending_merge().is_some()
    // A committed removal's farewell is re-driven on a bounded blind budget; the removed peer's ack is
    // unobservable (its Progress is pruned), so a quiesced group would strand the remaining shots until
    // unrelated traffic woke it. Ineligible until the budget drains — leader-gated, but the budget PARKS
    // across a demotion and re-arms on re-election, so the shots can span MULTIPLE leaderships before it
    // drains (still bounded — at most the original budget per removal across all terms).
    || ep.has_pending_farewells()
    // An outstanding capture debt is pending durability work only the per-crank service drives;
    // a quiesced group would strand the union's capture until unrelated traffic woke the pump.
    || ep.capture_debt().is_some()
    // A standing merge-cure debt is a wedged peer the pump predicate cannot see (it is not
    // log-lagging); quiescing would silence the very cadence its cure rides.
    || ep.has_cure_debts()
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

/// Cross-thread observability for a multi driver: the driver republishes the shared engine's
/// [`barriers`](sailing_proto::GroupEngine::barriers) /
/// [`ops_batched`](sailing_proto::GroupEngine::ops_batched) counters after every storage crank —
/// so a test or operator thread can watch the batch ratio (operations per barrier)
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
  barriers: AtomicU64,
  ops_batched: AtomicU64,
  quiesced_groups: AtomicU64,
  fenced_frames_dropped: AtomicU64,
}

impl EngineMetrics {
  /// Batch barriers run so far (every [`GroupEngine::flush`](sailing_proto::GroupEngine::flush)
  /// call counts, including one that released nothing).
  #[must_use]
  pub fn barriers(&self) -> u64 {
    self.inner.barriers.load(Ordering::Acquire)
  }

  /// Total storage operations completed across every barrier so far. `ops_batched / barriers` is
  /// the cross-group batch factor the shared engine exists for.
  #[must_use]
  pub fn ops_batched(&self) -> u64 {
    self.inner.ops_batched.load(Ordering::Acquire)
  }

  /// The number of hosted groups currently QUIESCED (idle: the driver neither arms nor sweeps
  /// their consensus deadlines; any traffic, a local command, or a connection loss wakes them).
  #[must_use]
  pub fn quiesced_groups(&self) -> u64 {
    self.inner.quiesced_groups.load(Ordering::Acquire)
  }

  /// Inbound frames the demux GENERATION FENCE has dropped: a sender speaking for an incarnation
  /// this host's durable admission floor has retired (WIRE.md §6). Purely observational — nothing
  /// consumes it, and the sender is never answered — but a CLIMBING value names a husk that is
  /// still running and still sending, which the embedder's catalog reap is what finally stops.
  #[must_use]
  pub fn fenced_frames_dropped(&self) -> u64 {
    self.inner.fenced_frames_dropped.load(Ordering::Acquire)
  }

  /// Publish the engine's current counters (driver-side, once per crank).
  pub(crate) fn record(&self, barriers: u64, ops_batched: u64) {
    self.inner.barriers.store(barriers, Ordering::Release);
    self.inner.ops_batched.store(ops_batched, Ordering::Release);
  }

  /// Publish the coordinator's fence-drop counter (driver-side, once per crank).
  pub(crate) fn record_fenced(&self, fenced: u64) {
    self
      .inner
      .fenced_frames_dropped
      .store(fenced, Ordering::Release);
  }

  /// Publish the quiesced-group gauge (driver-side, once per quiesce sweep).
  pub(crate) fn record_quiesced(&self, quiesced: u64) {
    self
      .inner
      .quiesced_groups
      .store(quiesced, Ordering::Release);
  }
}
