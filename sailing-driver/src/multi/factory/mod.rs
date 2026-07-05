//! Driver-side auto-materialization of solicited groups: the embedder registers a
//! [`GroupFactory`] with a multi-group driver, and the driver materializes a fresh replica
//! whenever gated unknown-group traffic solicits a group id the factory recognizes — in two
//! phases. The cheap [`materialize`](GroupFactory::materialize) consultation yields a
//! [`GroupBlueprint`] (config + seed); only after the driver's sender gate admits that
//! blueprint does [`build`](GroupFactory::build) construct the group's initial state machine.
//! [`factory_fn`] assembles a factory from the two phases as plain closures.

use sailing_proto::Config;

/// What [`GroupFactory::materialize`] returns: the CHEAP half of the admission payload for one
/// fresh replica — the group's consensus [`Config`] (its node id must be the host identity
/// latched by the first admitted group) and its election-jitter seed. The initial state machine
/// is deliberately absent: the driver requests it from [`GroupFactory::build`] only after this
/// blueprint passed the sender gate, so a refused or declined solicitation never constructs
/// one. The replica learns replicated state through the ordinary catch-up path (append or
/// snapshot) from the soliciting group's leader; a blueprint never carries recovered state (see
/// the CREATE-only rule on [`GroupFactory`]).
#[derive(Debug)]
pub struct GroupBlueprint<I> {
  config: Config<I>,
  seed: u64,
  generation: u64,
}

impl<I> GroupBlueprint<I> {
  /// Assemble a blueprint from the group's consensus config and its election-jitter seed. The
  /// incarnation defaults to 0 — every factory that never reshapes ids is a gen-0 world; a
  /// reshaping embedder's catalog supplies the real incarnation via
  /// [`with_gen`](Self::with_gen).
  #[inline(always)]
  pub const fn new(config: Config<I>, seed: u64) -> Self {
    Self {
      config,
      seed,
      generation: 0,
    }
  }

  /// Set the id's incarnation under the single-incarnation contract — the catalog-supplied
  /// generation the driver checks against the id's persisted admission floor before building
  /// anything, and records in its engine on admission. `u64::MAX` is NOT a working incarnation
  /// — it is the reserved merged-tombstone sentinel (`sailing_proto::MERGED_FLOOR`), and the
  /// driver's pre-build gate refuses a blueprint carrying it at any floor.
  #[inline(always)]
  #[must_use]
  pub const fn with_gen(mut self, generation: u64) -> Self {
    self.generation = generation;
    self
  }

  /// The id's incarnation this blueprint materializes (0 unless the embedder reshapes ids).
  #[inline(always)]
  pub const fn generation(&self) -> u64 {
    self.generation
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

  /// Consume and return the `(config, seed)` pair — the blueprint's contribution to the
  /// drivers' create path (the state machine arrives separately, from
  /// [`GroupFactory::build`]).
  #[inline(always)]
  pub fn into_parts(self) -> (Config<I>, u64) {
    (self.config, self.seed)
  }
}

/// The embedder's group-materialization hook — the CockroachDB `getOrCreateReplica` / TiKV
/// `maybe_create_peer` shape: a replica MATERIALIZES on first gated consensus contact, with no
/// per-replica application intervention, while membership DECISIONS still ride ordinary conf
/// changes proposed wherever the embedder's policy lives.
///
/// A multi-group driver with a registered factory consults it on every polled unknown-group
/// signal, BEFORE the signal reaches the lifecycle tail. `Some(blueprint)` — provided its seed
/// config names the soliciting peer (the sender leg below) and [`build`](Self::build) then
/// yields the initial state machine — admits the group on this host in the same crank: engine
/// storage + coordinator endpoint + driver routing, the exact `create_group` command path, and
/// consumes the signal (no [`LifecycleEvent::UnknownGroup`](crate::LifecycleEvent::UnknownGroup)
/// is emitted; the soliciting peer's retry completes the join). `None` DECLINES: the signal
/// falls through to the lifecycle tail exactly as on a factory-less driver. A driver with no
/// factory registered behaves exactly as before, byte for byte.
///
/// **The factory IS the placement brain's admission edge — and admission has two legs.** The
/// coordinator's pre-filters are shape-only: initial-shaped kinds (a vote request, pre-vote
/// included, or a first-contact heartbeat carrying commit 0), an authenticated sender, a group
/// neither hosted nor tombstoned, and the 64-group signal cap. The GROUP-ID leg is the
/// factory's: validate the id against the embedder's catalog and decline ids it cannot vouch
/// for — a `Some` is a real resource commitment (storage, an endpoint, routing state), so a
/// factory that admits unrecognized ids lets a single buggy valid-cert peer soliciting garbage
/// ids materialize unbounded groups across cranks. The SENDER leg is the driver's, enforced
/// fail-closed on every returned blueprint: a legitimate solicitation is initial-shaped
/// consensus traffic — a campaigner's vote request or a leader's first-contact heartbeat — so
/// its sender is by construction a member of the group it solicits, and a blueprint whose seed
/// config does not name the solicitor in its voter list is REFUSED (no create; the signal
/// falls to the lifecycle tail for the embedder's manual path). A blueprint failing that check
/// is either a stale catalog view (membership moved on) or an unauthorized cross-domain
/// solicitation from a peer that merely shares transport authentication — in a deployment
/// where authentication spans more than one placement domain, any valid-cert peer could
/// otherwise solicit catalog-known ids and force allocation on hosts its groups do not touch.
/// A factory may additionally consult `from` to decline early (skipping a doomed
/// materialization), but the driver's refusal never depends on it. **The legs order around the
/// resource phase:** the driver enforces the sender leg on the blueprint BEFORE it calls
/// [`build`](Self::build), so even a `from`-blind factory cannot be made to construct a state
/// machine (or open the resources one carries) for an unauthorized solicitor — the most a
/// refused solicitation can extract, at its retry cadence, is the cheap catalog consultation.
///
/// **Fork-born ids materialize as observers, never voting empties.** A blueprint for an id the
/// catalog knows to be FORK-BORN (a split child — any id whose authoritative baseline is a
/// manufactured snapshot) MUST use the observer shape: the host's own id ABSENT from the seed
/// config's voter list ([`Config::try_new_observer`]), so the materialized empty still GRANTS
/// votes but cannot campaign — the fork holder's manufactured log wins the only possible
/// election, the forced snapshot (the fork compacts through its baseline, so a zero-progress
/// peer's catch-up is structurally an install) delivers the fork content, and its boundary
/// configuration is what promotes the joiner to voter. WHY: a full-voter empty is promotable
/// with a virgin election timer, and an empty quorum's first commit lands exactly on the
/// manufactured baseline's coordinate — log-matching cannot distinguish the two histories, so
/// divergent committed state fuses silently instead of failing loudly. The references'
/// message-created replicas are structurally non-promotable until the log teaches them their
/// own membership; this is sailing's equivalent rule. Fresh factory-BOOTSTRAPPED ids (day-0
/// groups someone created explicitly) keep full-voter blueprints — the distinction is the
/// CATALOG's to make, and the catalog is the split registry.
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
  /// The CHEAP phase: decide whether to admit `group`, solicited by the authenticated peer
  /// `from`, returning the blueprint (config + seed) if the embedder's catalog vouches for the
  /// id. This phase must do no expensive work — no I/O, no resource acquisition, no
  /// state-machine construction (that is [`build`](Self::build)) — because it runs for every
  /// polled solicitation, admitted or not. A `Some` commits nothing by itself: the driver
  /// refuses a blueprint whose seed config does not name `from` among its voters (the sender
  /// leg of the admission edge) and surfaces the signal on the lifecycle tail instead. `None`
  /// declines and the signal surfaces on the lifecycle tail as today.
  fn materialize(&mut self, group: &G, from: &I) -> Option<GroupBlueprint<I>>;

  /// The RESOURCE phase: construct `group`'s initial state machine. The driver calls this at
  /// most once per accepted solicitation, immediately after a `Some` from
  /// [`materialize`](Self::materialize) for the same group within the same drain step, and
  /// never for a refused or declined solicitation — expensive work (opening storage, catalog
  /// I/O, large allocation) belongs here, structurally unreachable to a solicitor the sender
  /// gate turned away. `None` aborts the materialization: the driver surfaces the solicitation
  /// on the lifecycle tail exactly like a create failure and survives.
  fn build(&mut self, group: &G) -> Option<F>;
}

/// Assemble a [`GroupFactory`] from its two phases as plain closures: `seed` backs
/// [`GroupFactory::materialize`] — the cheap catalog consultation returning the blueprint —
/// and `build` backs [`GroupFactory::build`] — the resource phase, which the driver invokes
/// only after its sender gate admitted the blueprint `seed` returned.
///
/// # Example
///
/// ```
/// use core::time::Duration;
///
/// use sailing_driver::{GroupBlueprint, GroupFactory, factory_fn};
/// use sailing_proto::Config;
///
/// let mut factory = factory_fn(
///   // The cheap phase: consult the catalog, return config + seed.
///   |group: &u64, _from: &u64| {
///     (*group == 7).then(|| {
///       let config = Config::try_new(
///         2u64,
///         vec![1, 2],
///         Duration::from_millis(1000),
///         Duration::from_millis(100),
///       )
///       .expect("a valid seed config");
///       GroupBlueprint::new(config, 0x5eed)
///     })
///   },
///   // The resource phase: runs only for solicitations the driver admitted.
///   |_group: &u64| Some(Vec::<u8>::new()),
/// );
/// assert!(factory.materialize(&7, &1).is_some());
/// assert!(factory.build(&7).is_some());
/// ```
#[inline(always)]
pub const fn factory_fn<S, B>(seed: S, build: B) -> FnFactory<S, B> {
  FnFactory { seed, build }
}

/// The closure-backed [`GroupFactory`] returned by [`factory_fn`]: `S` is the cheap
/// materialize phase, `B` the driver-gated build phase.
pub struct FnFactory<S, B> {
  seed: S,
  build: B,
}

impl<G, I, F, S, B> GroupFactory<G, I, F> for FnFactory<S, B>
where
  S: FnMut(&G, &I) -> Option<GroupBlueprint<I>>,
  B: FnMut(&G) -> Option<F>,
{
  fn materialize(&mut self, group: &G, from: &I) -> Option<GroupBlueprint<I>> {
    (self.seed)(group, from)
  }

  fn build(&mut self, group: &G) -> Option<F> {
    (self.build)(group)
  }
}

/// The trait-object form the multi drivers store: `Send` because a driver's `run()` future
/// migrates across a work-stealing runtime's threads (the factory is only ever CALLED from the
/// one driver task, so no `Sync` is required), `'static` because the driver task owns it.
pub type BoxedGroupFactory<G, I, F> = Box<dyn GroupFactory<G, I, F> + Send>;

#[cfg(test)]
mod tests;
