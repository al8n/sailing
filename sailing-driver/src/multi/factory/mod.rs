//! Driver-side auto-materialization of solicited groups: the embedder registers a
//! [`GroupFactory`] with a multi-group driver, and the driver materializes a fresh replica —
//! a [`GroupBlueprint`]: config + seed + state machine — whenever gated unknown-group traffic
//! solicits a group id the factory recognizes.

use core::fmt;

use sailing_proto::Config;

/// What a [`GroupFactory`] materializes: the complete admission payload for ONE fresh replica —
/// the group's consensus [`Config`] (its node id must be the host identity latched by the first
/// admitted group), its election-jitter seed, and its INITIAL state machine. The replica learns
/// replicated state through the ordinary catch-up path (append or snapshot) from the soliciting
/// group's leader; a blueprint never carries recovered state (see the CREATE-only rule on
/// [`GroupFactory`]).
pub struct GroupBlueprint<I, F> {
  config: Config<I>,
  seed: u64,
  fsm: F,
}

impl<I, F> GroupBlueprint<I, F> {
  /// Assemble a blueprint from the group's consensus config, its election-jitter seed, and its
  /// initial state machine.
  #[inline(always)]
  pub const fn new(config: Config<I>, seed: u64, fsm: F) -> Self {
    Self { config, seed, fsm }
  }

  /// Borrow the group's consensus config.
  #[inline(always)]
  pub const fn config_ref(&self) -> &Config<I> {
    &self.config
  }

  /// The group's election-jitter seed (folded with the group id by the container, so co-located
  /// groups never draw correlated jitter).
  #[inline(always)]
  pub const fn seed(&self) -> u64 {
    self.seed
  }

  /// Borrow the group's initial state machine.
  #[inline(always)]
  pub const fn fsm_ref(&self) -> &F {
    &self.fsm
  }

  /// Consume and return the `(config, seed, fsm)` triple — the exact argument shape of the
  /// drivers' create path.
  #[inline(always)]
  pub fn into_parts(self) -> (Config<I>, u64, F) {
    (self.config, self.seed, self.fsm)
  }
}

// Hand-written so debuggability never hinges on the state machine: an FSM is rarely `Debug`,
// and the blueprint's identifying content is the config + seed anyway (the elided field renders
// as `..`).
impl<I, F> fmt::Debug for GroupBlueprint<I, F>
where
  I: fmt::Debug,
{
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("GroupBlueprint")
      .field("config", &self.config)
      .field("seed", &self.seed)
      .finish_non_exhaustive()
  }
}

/// The embedder's group-materialization hook — the CockroachDB `getOrCreateReplica` / TiKV
/// `maybe_create_peer` shape: a replica MATERIALIZES on first gated consensus contact, with no
/// per-replica application intervention, while membership DECISIONS still ride ordinary conf
/// changes proposed wherever the embedder's policy lives.
///
/// A multi-group driver with a registered factory consults it on every polled unknown-group
/// signal, BEFORE the signal reaches the lifecycle tail. `Some(blueprint)` admits the group on
/// this host in the same crank — engine storage + coordinator endpoint + driver routing, the
/// exact `create_group` command path — and consumes the signal (no
/// [`LifecycleEvent::UnknownGroup`](crate::LifecycleEvent::UnknownGroup) is emitted; the
/// soliciting peer's retry completes the join). `None` DECLINES: the signal falls through to
/// the lifecycle tail exactly as on a factory-less driver. A driver with no factory registered
/// behaves exactly as before, byte for byte.
///
/// **The factory IS the placement brain's admission edge.** The driver's only pre-filters are
/// the coordinator's: initial-shaped kinds (a vote request, pre-vote included, or a
/// first-contact heartbeat carrying commit 0), an authenticated sender, a group neither hosted
/// nor tombstoned, and the 64-group signal cap. The factory MUST therefore validate the group
/// id against the embedder's catalog and decline ids it cannot vouch for: a `Some` is a real
/// resource commitment (storage, an endpoint, routing state), so a factory that admits
/// unrecognized ids lets a single buggy valid-cert peer soliciting garbage ids materialize
/// unbounded groups across cranks.
///
/// **CREATE-only.** A blueprint materializes a FRESH replica: the state machine is the initial
/// one, and the replica learns state via the ordinary snapshot/append catch-up. Recovering a
/// group that has durable local state is boot-time embedder work through
/// [`MultiCommand::RestoreGroup`](crate::MultiCommand::RestoreGroup) — a factory must never be
/// the restore path (materializing fresh over forgotten durable state is exactly the
/// log-regression hazard the membership-level rejoin recipe exists to avoid).
///
/// **Tombstone interplay.** A tombstoned id never reaches the factory (the coordinator never
/// enqueues its signals), a removal racing an already-queued signal purges it, and the residual
/// polled-then-removed interleaving fails CLOSED at admission (`Retired`) — the driver then
/// surfaces the signal on the lifecycle tail as unplaceable rather than creating silently.
/// Re-admission stays the deliberate two-act
/// [`clear_tombstone`](crate::MultiHandle::clear_tombstone) + create: the factory never
/// overrides a tombstone.
pub trait GroupFactory<G, I, F> {
  /// Decide whether to materialize `group`, solicited by the authenticated peer `from`.
  /// `Some(blueprint)` commits this host to admitting the group; `None` declines and the
  /// signal surfaces on the lifecycle tail as today.
  fn materialize(&mut self, group: &G, from: &I) -> Option<GroupBlueprint<I, F>>;
}

impl<G, I, F, T> GroupFactory<G, I, F> for T
where
  T: FnMut(&G, &I) -> Option<GroupBlueprint<I, F>>,
{
  fn materialize(&mut self, group: &G, from: &I) -> Option<GroupBlueprint<I, F>> {
    self(group, from)
  }
}

/// The trait-object form the multi drivers store: `Send` because a driver's `run()` future
/// migrates across a work-stealing runtime's threads (the factory is only ever CALLED from the
/// one driver task, so no `Sync` is required), `'static` because the driver task owns it.
pub type BoxedGroupFactory<G, I, F> = Box<dyn GroupFactory<G, I, F> + Send>;
