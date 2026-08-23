//! Multi-Raft: hosting many single-group [`Endpoint`]s in one process.
//!
//! [`MultiRaft`] is the Sans-I/O super-state-machine — a container of independent single-group
//! [`Endpoint`]s keyed by [`GroupId`]. You drive it exactly like an [`Endpoint`] (inject peer
//! messages, timer deadlines, and storage completions; drain outbound messages and events), except
//! every call is addressed to a group and the output and scheduling surface is aggregated across
//! all groups. The consensus core stays completely group-unaware; this layer owns only the routing
//! and the aggregate drains.
//!
//! See `MULTI_RAFT.md` for the architecture and the phased roadmap. Groups are created and
//! removed at runtime over caller-injected per-group storage (each group is handed its own
//! `LogStore`/`StableStore` per call, mirroring [`Endpoint`]); `MultiRaft` itself stays the PURE
//! container — the dynamic-lifecycle mechanics around it (removal tombstones, unknown-group
//! surfacing, removed-self notification) live in the multi coordinators and the drivers, and the
//! placement POLICY — where a group should live, when to tear a removed replica down — is
//! explicitly the embedder's (no auto-create, no auto-teardown). The group-tagged wire (the
//! multi-group coordinators) and the shared storage engine ([`GroupEngine`] — every co-located
//! group's stores behind ONE batched durability barrier) consume this surface without reshaping
//! it. Every client-facing routing method of the single-group [`Endpoint`] — propose, conf
//! changes (v1/v2), read-index, leader transfer, read-mode migration — has a group-keyed
//! delegate here.

mod engine;
pub use engine::{EngineLog, EngineStable, EngineStorageError, GroupEngine, MultiEngine};

mod group_id;
pub use group_id::GroupId;

use crate::{
  CommitMergePayload, ConfChange, ConfChangeType, ConfChangeV2, ConfState, Config,
  CreateGroupError, Data, Endpoint, EntryKind, Event, ForkId, HardState, Index, Instant, LogStore,
  MergeError, Message, NodeId, Now, OpId, Outgoing, PoisonReason, PrepareMergePayload, Prng,
  ProposeError, ReadIndexError, ReadOnlyOption, RemoveError, RollbackMergePayload, SnapshotMeta,
  SplitError, SplitPayload, StableStore, StateMachine, StorageProgress, Term,
  ThawDischargedPayload, TransferError, endpoint::MergeWindow,
};
use bytes::Bytes;
use cheap_clone::CheapClone;
use core::time::Duration;
use std::{
  collections::{BTreeMap, BTreeSet, VecDeque},
  vec::Vec,
};

/// Read-only lineage lookup consulted at group admission (and, from M4/M5 on, at the
/// split/merge propose-time gates). [`GroupEngine`] implements it over its lineage records — the
/// driver hands its engine in at every admission call; the deterministic sim brings an in-memory
/// impl; a coordinator-embedder that brings its own storage brings its own impl, whose
/// durability is that storage's job. [`NoFloors`] is the gen-0 convenience: every lookup is 0,
/// which makes admission behave exactly as it did before floors existed (no fence).
///
/// # The single floor authority
///
/// This trait is the ONE canonical lineage accessor for the whole system. Every door that fences on
/// a floor — a fresh create, a restore, a coordinator's factory admission, the merge service's
/// propose-time gate, the fork relay's peek, and the atomic install's own internal re-check — reads
/// it and nothing else. [`MultiEngine`] deliberately carries no second reader: it is a SUPERTRAIT
/// bound on this one precisely so an engine has exactly one place to answer from. Two accessors with
/// no requirement that they agree is a contract hole, and the doors are split across enough call
/// sites that a divergent pair would fence some and admit others.
///
/// # Freshest read (NORMATIVE)
///
/// Both methods MUST answer with the FRESHEST value the implementation holds — a floor or lineage
/// written this crank fences before its barrier lands, so an implementation that stages writes must
/// answer `max(durable, staged)` rather than durable-only.
///
/// Reading ahead of the barrier is safe because both values are monotone: they only ever grow, so
/// early visibility can only refuse admissions that durability would refuse too, never admit what it
/// would fence. Answering durable-only is NOT safe, and the failure is concrete: a host that drains
/// several commands per storage crank can stage a removal's floor and then, in the same batch,
/// process a create for that id — a durable-only read still sees the pre-removal floor and admits
/// the retired incarnation, after which the flush persists the fence beside a live endpoint already
/// standing below it.
pub trait FloorStore<G> {
  /// The admission floor for `gid` (0 = never floored). Freshest — see the trait's normative
  /// clause.
  fn floor(&self, gid: &G) -> u64;
  /// The id's current incarnation/shape counter (0 = unreshaped). Freshest, on the same
  /// monotonicity argument as [`floor`](Self::floor).
  fn lineage(&self, gid: &G) -> u64;
}

/// The no-fence store: a gen-0 world (all of today's embedders and tests). Both lookups return
/// 0, so no incarnation is ever below its floor and the volatile tombstone remains the only
/// re-admission gate.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NoFloors;

impl<G> FloorStore<G> for NoFloors {
  fn floor(&self, _gid: &G) -> u64 {
    0
  }

  fn lineage(&self, _gid: &G) -> u64 {
    0
  }
}

/// The facts a fork RELAY needs that the container cannot see for itself: whether the caller's
/// storage already holds the child id, and that id's admission floor. The container asks at the
/// moment it would install a committed fork, and maps the answers to a verdict itself
/// ([`MultiRaft::peek_yieldable_fork`]) — the gate reports, it does not decide.
///
/// [`NoHold`] is the answer of a caller with no storage to consult.
///
/// # Cost contract
///
/// This is consulted O(parked × yields) per relay drain — every parked parent is re-examined at
/// the top of every drain — so both methods MUST be in-memory and allocation-free. A durable
/// engine caches its lineage and floor maps and answers from the cache; reaching storage here
/// would put an I/O per parked fork on every crank.
pub trait ForkGate<G> {
  /// Whether the caller's storage already holds `gid`'s stores. Occupancy is not provenance: a
  /// `true` says only that the id is spoken for, which is why it HOLDS the fork rather than
  /// refusing it.
  fn contains_group(&self, gid: &G) -> bool;

  /// `gid`'s admission floor (0 = never floored) — the same value the coordinators' admission
  /// gates read, so a fork and a create answer to one fence.
  fn floor(&self, gid: &G) -> u64;
}

/// The install's gate: the ENGINE's own facts, widened by whatever the engine cannot answer.
///
/// The install cannot take a live `ForkGate` from the caller — the gate would hold `&E` while the
/// install needs `&mut E` to mint an epoch and resolve stores, and both borrows are live across the
/// call. So the container composes the gate itself from the engine seam it is already given, and
/// `extra` carries only the facts outside the engine: the coordinators' volatile tombstone set,
/// `NoHold` everywhere else. The composition is exactly the coordinators': occupancy is either
/// source saying yes, and the floor is the higher of the two.
struct EngineForkGate<'a, E, X, I> {
  engine: &'a E,
  extra: &'a X,
  _node: core::marker::PhantomData<I>,
}

impl<G, I, E, X> ForkGate<G> for EngineForkGate<'_, E, X, I>
where
  G: GroupId,
  I: NodeId,
  E: MultiEngine<G, I>,
  X: ForkGate<G>,
{
  fn contains_group(&self, gid: &G) -> bool {
    self.engine.contains_group(gid) || self.extra.contains_group(gid)
  }

  fn floor(&self, gid: &G) -> u64 {
    // Through `FloorStore` — the one canonical accessor — so this internal re-check answers to the
    // same fence, at the same freshness, as the create and factory doors outside.
    FloorStore::floor(self.engine, gid).max(self.extra.floor(gid))
  }
}

/// The gate of a caller holding no storage of its own: nothing is occupied and nothing is
/// floored, which is the relay drain's behavior before the gate existed. The [`NoFloors`] idiom,
/// for the relay.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NoHold;

impl<G> ForkGate<G> for NoHold {
  fn contains_group(&self, _gid: &G) -> bool {
    false
  }

  fn floor(&self, _gid: &G) -> u64 {
    0
  }
}

/// Why a parent's head fork is parked. One mechanism, two causes: the resolution triggers, the
/// re-examination sweep, the one-shot cue and its purge are identical, so only the diagnosis
/// differs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParkCause {
  /// The child id is already HOSTED here and its token is not this fork's (arm (c)).
  HostedChild,
  /// The caller's gate says the id is momentarily spoken for: occupied stores, or a tombstone
  /// mid-rejoin. NOT a committed-consumed source — an outstanding absorb debt, or a park whose
  /// abort window has latched CLOSED, is a verdict about the fork rather than a window that can
  /// close, so those abandon it TERMINALLY instead of holding.
  Blocked,
}

/// Per-group storage a `MultiStreamCoordinator` uses to drive each group's endpoint when inbound
/// bytes span multiple groups. The caller implements it over its own per-group store table.
///
/// CONTRACT: resolution must be STABLE (the same group always resolves to the same stores) and
/// NON-ALIASING (two groups must never share a store — a shared log is a safety violation).
/// Returning `Some` for a group the `MultiRaft` does not host is a harmless per-message drop;
/// returning `None` for a hosted group starves it.
pub trait GroupStores<G, L, S> {
  /// The `(log, stable)` stores for `group`, or `None` if this host has no storage for it — an
  /// inbound message for an unknown group is then dropped (the sender retries on its own cadence).
  fn stores(&mut self, group: &G) -> Option<(&mut L, &mut S)>;
}

/// The admission floor a merge writes for the absorbed id (the merge milestone is its ONLY
/// writer): `u64::MAX` fences every future incarnation, so a merged-away id never returns.
/// Consequently `u64::MAX` is NOT a usable working generation — it is reserved as this fence,
/// and [`floor_admits`] refuses it at ANY floor. That reservation is what makes this fence
/// terminal BY CONSTRUCTION: the floor leg alone (`generation >= floor`) would wave the
/// sentinel generation through its own fence, since `u64::MAX < u64::MAX` is false.
///
/// EMBEDDER CONTRACT: write `MERGED_FLOOR` for an id ONLY once its lineage is TERMINALLY resolved
/// (absorbed away — the driver folds it from a [`MergeResolution::Merged`] or
/// [`MergeResolution::Retired`]). A floor claiming an UNRESOLVED lineage was always a safety
/// violation (it buries a live incarnation below an admission it can never clear); it is now also
/// actively DESTRUCTIVE — the per-crank service treats a hosted FROZEN source at this floor as a
/// dead husk and DISSOLVES it (`Retired`), so a premature `MERGED_FLOOR` tears down a live merge
/// source. The terminal floor must be re-persisted CO-BARRIERED with the source teardown a merge
/// resolution folds: dropping the stores while the floor is only STAGED (not durable) and crashing
/// would re-admit the id below its generation.
///
/// CONSENSUS-GRADE WARNING: a false terminal floor no longer costs merely ONE local replica. The
/// per-crank service reads this floor on a source's DEAD (unhosted) TARGET to authorize the source
/// minting its OWN thaw — a COMMITTED entry every replica applies (see the dead-target derivation in
/// [`service_merge_applies`](crate::MultiRaft::service_merge_applies)). Writing `MERGED_FLOOR` for a
/// lineage that is NOT terminally resolved can therefore unfreeze a source out from under a target
/// that never released it and DIVERGE that target's replicas — a safety violation, full stop. The
/// floor is a GLOBAL fact about a resolved lineage; it is never a knob to break a wedge by hand.
pub const MERGED_FLOOR: u64 = u64::MAX;

/// The floor-admission predicate every admission gate applies — the multi coordinators'
/// create/restore checks and the drivers' factory pre-build gate: `generation` is admissible
/// against `floor` iff it clears the floor AND is not the reserved [`MERGED_FLOOR`] sentinel
/// (never a working incarnation). The second leg makes a `MERGED_FLOOR` fence refuse EVERY
/// generation, the sentinel itself included.
pub const fn floor_admits(floor: u64, generation: u64) -> bool {
  generation < MERGED_FLOOR && generation >= floor
}

/// The floor-INDEPENDENT admission leg of [`floor_admits`], enforced by the CORE group
/// constructors so the reserved terminal [`MERGED_FLOOR`] sentinel is refused at creation — not
/// only at the coordinators' floor validator. A group born AT the sentinel would read as
/// merged-away to every downstream consumer (a `MERGED_FLOOR` floor admits nothing, `next_lineage`
/// never mints it), so no working incarnation may occupy it, whatever floor a caller supplies.
const fn validate_working_generation(generation: u64) -> Result<(), CreateGroupError> {
  if generation < MERGED_FLOOR {
    Ok(())
  } else {
    Err(CreateGroupError::ReservedGeneration)
  }
}

/// The refusing form of [`floor_admits`] the coordinators' admission methods report through:
/// the reserved sentinel under a lower floor maps to [`CreateGroupError::ReservedGeneration`]
/// (the caller supplied a generation that can never work), everything else to
/// [`CreateGroupError::BelowFloor`] — under a `MERGED_FLOOR` fence the fence itself stays the
/// truthful terminal verdict at every generation, sentinel included. Gated with its only
/// consumers, the transport coordinators (`quic` implies `tcp`).
#[cfg(feature = "tcp")]
pub(crate) const fn validate_floor(floor: u64, generation: u64) -> Result<(), CreateGroupError> {
  if floor_admits(floor, generation) {
    Ok(())
  } else if generation == MERGED_FLOOR && floor < MERGED_FLOOR {
    Err(CreateGroupError::ReservedGeneration)
  } else {
    Err(CreateGroupError::BelowFloor { floor })
  }
}

/// The next lineage generation after `gen`, or `None` when the mint would reach the reserved
/// [`MERGED_FLOOR`] terminal (or overflow `u64`). Every shape-move mint — a split's
/// `parent_gen_after`, a merge freeze/absorb/abort/thaw's generation — passes through here so a
/// working generation stays STRICTLY below the sentinel: the value that enters the log can never be
/// read downstream as merged-away, and the counter can never wrap past the terminal to `0`. The
/// mint analogue of [`floor_admits`]'s admission leg.
pub(crate) fn next_lineage(generation: u64) -> Option<u64> {
  generation.checked_add(1).filter(|n| *n < MERGED_FLOOR)
}

/// The engine IS the drivers' floor store: lookups delegate to the freshest-read lineage
/// accessors, so a floor staged this crank already fences before the barrier lands (monotone-max
/// staging makes that early visibility safe).
impl<G, I> FloorStore<G> for GroupEngine<G, I>
where
  G: Ord,
{
  fn floor(&self, gid: &G) -> u64 {
    self.group_floor(gid)
  }

  fn lineage(&self, gid: &G) -> u64 {
    self.group_gen(gid)
  }
}

/// The log index of the synthetic snapshot baseline every forked group boots at. Index 1, not 0:
/// meta index 0 is the no-snapshot sentinel, and the baseline must push `first_index` to 2 so a
/// zero-progress joiner (match 0, next 1) satisfies `next < first_index` and is structurally
/// forced onto the snapshot path — an uncompacted fork would LOG-WALK the joiner, replaying only
/// post-fork entries onto its EMPTY state machine (silent, permanent divergence from the
/// preloaded replicas).
///
/// The empty receiver is not merely the intended shape — it is ENFORCED: the receive path admits a
/// token-bearing snapshot only onto a replica with no committed content (the fork-provenance gate),
/// because this coordinate is the most collision-prone in any log (essentially every group's first
/// entry lands at index 1, term 1) and coordinate proofs certify content only within one lineage.
pub const FORK_BASE_INDEX: Index = Index::new(1);

/// The term of the fork baseline entry. Term 1, not 0: a well-formed store never holds an entry
/// above its own durable term, so the manufactured HardState carries this term and the baseline
/// meta claims it — the exact shape a real snapshot install leaves behind.
pub const FORK_BASE_TERM: Term = Term::new(1);

/// Manufacture the fork baseline in a child's fresh stores — the exact store shape a completed
/// snapshot install leaves behind, so [`Endpoint::restart`] recovers it with the install
/// machinery's own validation: HardState at the baseline term (commit at the boundary), the
/// AUTHORITATIVE blob persisted at `(FORK_BASE_INDEX, FORK_BASE_TERM)` under the boot voters,
/// and the log re-baselined so `first_index == 2` (blob-then-rebaseline, the install order).
///
/// The write ids ride the PRIOR boot epoch (`boot_epoch - 1`): restart seeds the endpoint's op
/// counter at `first_of_epoch(boot_epoch)`, so a completion from these manufacture-time writes is
/// epoch-strictly-below every id the child ever mints — it can never alias a live op in the
/// pending maps or falsely satisfy a `>=` durability watermark (the OpId epoch-major contract).
/// Same-epoch ids would collide with the child's first ops: a vote write pending at
/// `(boot_epoch, 0)` would see OUR completion and ack a not-yet-durable vote. That is exactly
/// what `boot_epoch == 0` would produce — the saturating subtraction has no prior epoch to land
/// in — so the fork constructors refuse it ([`validate_fork_boot_epoch`]) before reaching here;
/// the subtraction never actually saturates.
#[allow(clippy::too_many_arguments)]
fn write_fork_baseline<I, L, S>(
  config: &Config<I>,
  snapshot: Bytes,
  generation: u64,
  read_only: Option<ReadOnlyOption>,
  fork_id: Option<ForkId>,
  boot_epoch: u64,
  log: &mut L,
  stable: &mut S,
) where
  I: NodeId,
  L: LogStore,
  S: StableStore<NodeId = I>,
{
  let opid = OpId::first_of_epoch(boot_epoch.saturating_sub(1));
  // The manufactured hard state records the child's lineage from birth: restart reconciliation
  // compares it against the durable snapshot's token, and a baseline written WITHOUT it would read
  // as a token-less log beside a token-bearing snapshot — the exact ambiguity the record exists to
  // remove.
  stable.submit_write(
    opid,
    HardState::initial()
      .with_term(FORK_BASE_TERM)
      .with_commit(FORK_BASE_INDEX)
      .with_lineage(fork_id.clone()),
  );
  let conf = ConfState::from_voters(config.voters().iter().map(CheapClone::cheap_clone));
  // The baseline meta carries the child's own lineage (its incarnation under the unified
  // counter, absent at 0), the inherited read mode when the parent had a committed migration, and
  // the child's fork PROVENANCE token — exactly as a real install's meta would: the restart boot
  // below then recovers all three, and the token survives every later snapshot/restart/transfer so
  // the parent's parked fork can resolve REDUNDANT only against an exact match.
  let mut meta =
    SnapshotMeta::new(FORK_BASE_INDEX, FORK_BASE_TERM, conf).with_shape_gen(generation);
  if let Some(mode) = read_only {
    meta = meta.with_read_only(mode);
  }
  if let Some(fork_id) = fork_id {
    meta = meta.with_fork_id(fork_id);
  }
  stable.submit_snapshot(opid.next(), meta, snapshot);
  log.restore(FORK_BASE_INDEX, FORK_BASE_TERM);
}

/// Force pre-vote + check-quorum on for a RESHAPE-BORN group's config — the ONE derivation every
/// fork-child birth applies, so every replica of a split child boots with byte-identical knobs.
/// A reshaping id's steady-state membership churn is exactly where an ignorant removed voter would
/// otherwise depose a live leader. Independent of the seed config's flags: embedder-created groups
/// and fresh day-0 factory births keep their configured (etcd-parity) defaults.
#[must_use]
pub fn reshape_born_prevention<I>(config: Config<I>) -> Config<I> {
  config.with_pre_vote(true).with_check_quorum(true)
}

/// Mint a child's [`ForkId`] from one committed split's coordinates — the single source of the
/// provenance token, minted identically at the in-container fork INSTALL (into the manufactured
/// baseline's meta) and at the parked-fork REDUNDANT check. Every input is a property of the committed
/// split entry, so the token is replica-identical: the parent id (its canonical `Data` encoding),
/// the parent's lineage after the split (`parent_gen_after`), the split entry's `(index, term)`,
/// and the child's already-canonical id bytes and incarnation.
pub(crate) fn mint_fork_id<G: GroupId>(
  parent: &G,
  parent_gen_after: u64,
  split_index: Index,
  split_term: Term,
  child_bytes: Bytes,
  child_gen: u64,
) -> ForkId {
  let mut parent_bytes = Vec::new();
  parent.encode(&mut parent_bytes);
  ForkId::new(
    Bytes::from(parent_bytes),
    parent_gen_after,
    split_index,
    split_term,
    child_bytes,
    child_gen,
  )
}

/// The fork constructors' boot-epoch admission check, run BEFORE any store write: a fork must
/// boot at epoch >= 1 because [`write_fork_baseline`] issues the manufactured baseline's store
/// writes at the PRIOR epoch. At `boot_epoch == 0` there is no prior epoch — the baseline ids
/// would collapse into epoch 0, the very epoch [`Endpoint::restart`] seeds the child's own op
/// counter with, so a queued baseline `Wrote(0, 0)` completion could release a live vote or
/// campaign action whose durability it does not prove (leadership on a phantom durable
/// self-vote). Restart already CONTRACTS epochs strictly above every prior incarnation; a fork
/// additionally reserves the prior epoch for its baseline, so this floor is enforced, not
/// merely documented.
const fn validate_fork_boot_epoch(boot_epoch: u64) -> Result<(), CreateGroupError> {
  if boot_epoch == 0 {
    return Err(CreateGroupError::InvalidBootEpoch);
  }
  Ok(())
}

/// The fork constructors' used-storage admission check, run BEFORE any store write:
/// [`write_fork_baseline`] OVERWRITES whatever the stores hold, so a fork is only ever written
/// over VIRGIN stores — no visible hard state, no snapshot slot, no log content (an empty,
/// never-re-baselined log). A used incarnation's stores here mean a replayed fork raced the
/// child's real durable progress (a crash restored the parent while the child stayed unhosted);
/// overwriting would destroy that progress, so the fork refuses and the child reaches this host
/// by restore instead. The legitimate crash-BEFORE-flush replay is untouched: nothing of the
/// child ever became durable, the stores are virgin, and re-materialization stays idempotent.
fn validate_virgin_stores<L, S>(log: &L, stable: &S) -> Result<(), CreateGroupError>
where
  L: LogStore,
  S: StableStore,
  S::NodeId: PartialEq,
{
  let used = stable.hard_state() != HardState::initial()
    || stable.snapshot().is_some()
    || log.last_index() > Index::ZERO
    || log.first_index() > Index::new(1);
  if used {
    return Err(CreateGroupError::StorageInUse);
  }
  Ok(())
}

/// One resolved parked merge from a [`MultiRaft::service_merge_applies`] crank — what the
/// DRIVER folds into its storage engine and lifecycle teardown. The container already did the
/// consensus-side work (the absorb or the deterministic abort, the events, the source
/// endpoint's removal on a merge); the driver owns the storage half: persist
/// `floor(source) = `[`MERGED_FLOOR`] and drop the source's stores for a `Merged` (and, minus the
/// capture, for a `Retired`), nothing for an `Aborted` (the source group is still live — its log
/// settled the race).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeResolution<G> {
  /// The target ABSORBED the source: the source endpoint is gone from this container, the
  /// target's forced absorb capture is staged, and the source id must now be floored terminally
  /// and its stores dropped — behind the SAME barrier the capture rides.
  Merged {
    /// The absorbed source group.
    source: G,
    /// The absorbing target group.
    target: G,
  },
  /// The target ABSORBED the source but a standing replay fence (a parked fork's durability
  /// barrier, an undischarged abort obligation) DEFERRED the forced capture: the union is applied
  /// and serving, the source endpoint is gone from this container, and the consumed source's
  /// stores remain the union's ONLY restart derivation until the debt discharges. The driver
  /// folds the `CaptureFailed` routing half WITHOUT the poison or the restart demand: fail the
  /// source's parked routing typed (its callers would hang forever on the removed endpoint's
  /// completions), drain the routing's completion-panic latch, clear the source's volatile
  /// per-group maps — and PRESERVE the source's stores and floor untouched: no floor write, no
  /// store teardown, no tombstone. `Merged { source, target }` follows from a later crank once
  /// the fence lifts and the capture stages, with its exact contract; `CaptureFailed` follows a
  /// capture fault instead. A crash in the window re-parks against the restored source (the
  /// boundary's `CommitMerge` cannot compact away first — compaction past it requires exactly
  /// the capture still owed).
  Absorbed {
    /// The absorbed source group, whose stores and floor the driver MUST keep until `Merged`.
    source: G,
    /// The absorbing target group, now unparked and carrying the capture debt.
    target: G,
  },
  /// The parked commit resolved as a deterministic NO-OP (the source's log settled the race, or
  /// the commit was a replayed duplicate). Both groups remain exactly as they were.
  Aborted {
    /// The named source group.
    source: G,
    /// The target group whose parked apply no-op'd.
    target: G,
  },
  /// A hosted FROZEN source at the TERMINAL [`MERGED_FLOOR`] — the husk of a lineage that was
  /// absorbed away ELSEWHERE (its target caught up via a snapshot install and never parked here) —
  /// was DISSOLVED locally. There is no absorb and so no capture: the driver folds the SAME source
  /// half as `Merged` MINUS the capture — re-persist `floor(source) = `[`MERGED_FLOOR`] (a monotone
  /// no-op when already durable, but MANDATORY co-barriered with the teardown: a `FloorStore` may
  /// serve a STAGED floor, and dropping the stores off it then crashing before the flush would
  /// re-admit the id below its gen) and drop the source's stores.
  Retired {
    /// The dissolved husk's source group.
    source: G,
  },
  /// The absorb reached the point of NO RETURN — the source endpoint was consumed and its state
  /// machine extracted — but the union could not be made durable: the target's FSM REFUSED the
  /// absorb (a deterministic [`MergeUnsupported`](crate::PoisonReason::MergeUnsupported) fail-stop)
  /// or the forced capture FAULTED. The target is POISONED and no teardown is safe.
  ///
  /// The crucial asymmetry versus `Merged`/`Retired`: the source id is gone from the container, yet
  /// its stores and floor MUST be PRESERVED — they are the union's ONLY surviving copy, and the
  /// documented recovery is a restart that re-parks the merge against the restored source. Flooring
  /// or dropping them would bury the union behind a torn-down source no durable target snapshot
  /// covers. The driver folds this by FAILING the source's parked routing with a typed error (its
  /// callers park on the removed endpoint's oneshots and would otherwise hang forever, since the
  /// queued events that would have answered them vanished with the endpoint) and surfacing a
  /// lifecycle signal so the embedder restarts. The driver mirrors the `Merged` latch discipline:
  /// draining the source routing's completion latch fail-stops the TARGET when it fired, because the
  /// target absorbed the source's FSM.
  CaptureFailed {
    /// The consumed source group, whose stores and floor the driver MUST keep for the restart.
    source: G,
    /// The poisoned absorbing target.
    target: G,
  },
}

/// Why a merge is standing still on this host — see [`MergeBlocked`]. Every variant names a
/// condition that outlives a crank; the transient waits a merge passes through on its way to
/// resolving (a staged capture draining, the abort window still open) are deliberately unnamed
/// and never signalled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum MergeBlockedCause {
  /// The source is NOT hosted here and its floor is non-terminal, so no local fold can ever land:
  /// the union must arrive as a post-merge snapshot from the resolved quorum. This is the shape
  /// the cure advertisement addresses, and the one a placement brain can also resolve directly by
  /// giving the source a host.
  SourceUnhosted,
  /// The source is hosted but has not yet applied its freeze, so the absorb has nothing to fold.
  /// Ordinarily transient — the source's own replication closes it — but a source left
  /// leaderless and under-hosted stays here.
  SourceBehind,
  /// The park itself is locally unresolvable, but a committed `CommitMerge` ABOVE it names a
  /// source hosted on this host: an adopting install would cross that entry without resolving
  /// it, leaving the hosted replica a live-voting husk of an absorbed-away (or stale-no-op)
  /// lineage — so the cure advertisement is withheld, outcome-blind and fail-closed, and the
  /// park waits on the hosted replica's own lifecycle or the propagated terminal floor.
  CrossedHostedSource,
  /// A staged fork holds the merge. Two shapes, one exit — the fork conflict's resolution:
  /// the union is absorbed and serving while its durability capture waits on the TARGET's own fork
  /// barrier, or the park has not folded at all because the SOURCE still owes a staged fork whose
  /// child baseline is not yet locally durable (consuming it would destroy that child's only local
  /// derivation). The second shape has no local exit when the fork's child id IS the merge target:
  /// the fork waits on the occupant, the occupant is parked on this absorb, and the absorb waits on
  /// the fork. That composition needs embedder action, which is why it is signalled rather than
  /// silently resolved.
  ForkFence,
  /// The union is absorbed and serving; its durability capture waits on an undischarged abort
  /// obligation. The named source's thaw is the exit.
  AbortFence,
  /// A live merge freeze holds the capture: this group is itself a source pinned by a claiming
  /// target. The thaw — or this group's own dissolution into the claimant — is the exit.
  Frozen,
}

/// A merge held on this host by a STRUCTURAL cause, surfaced once per transition — see
/// [`MultiRaft::poll_merge_blocked`].
///
/// An OBSERVATION, never a command: nothing is torn down, nothing waits on consumption, and the
/// container re-derives the same verdict every crank whether or not anyone reads this. It exists
/// because both held shapes are otherwise INVISIBLE from outside — a parked target is not
/// log-lagging and its apply stall is purely local, and a debt-holding target looks entirely
/// healthy while its conf changes are fenced and the consumed source's id stays un-reusable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeBlocked<G> {
  /// The absorbing target — the group whose apply drain (or capture) is held.
  pub target: G,
  /// The named source group.
  pub source: G,
  /// The held coordinate: the parked `CommitMerge`'s index while the park stands, the absorb
  /// boundary the capture owes once the park has been deferred into a debt.
  pub boundary: Index,
  /// What is holding it.
  pub cause: MergeBlockedCause,
}

/// The outcome of one head-fork examination (see `MultiRaft::drain_to_yieldable`): the parent's
/// queue was empty (or the parent gone / poisoned on a corrupt child id), the head fork was
/// consumed by a resolution arm, it parked on a hosted-child conflict, or it is yieldable — still
/// staged, with the install's plan decided.
// Transient: matched and consumed on the stack within one drain step, never stored — and the
// relay path's cost contract is allocation-free, so boxing the plan to even the variants out would
// put a heap allocation on every relayed fork.
#[allow(clippy::large_enum_variant)]
enum HeadFork<G, I> {
  Empty,
  Resolved,
  Parked,
  Yield(YieldPlan<G, I>),
}

/// Everything the container needs to install a head fork, decided by the examine and carrying NO
/// part of the forked half: the partition stays in the staged queue until the install pops it.
/// This is what makes the yield atomic — the decision travels, the data never does.
struct YieldPlan<G, I> {
  child: G,
  child_gen: u64,
  parent_gen_after: u64,
  split_index: Index,
  split_term: Term,
  child_bytes: Bytes,
  config: Config<I>,
}

/// A BORROWED look at the head fork the relay would install right now — the pair it names, the
/// split coordinates, and the boot config the container derived for it. It carries no forked state
/// and no capability: possessing one authorizes nothing, because the install is a separate call
/// that re-decides everything from the named parent's own staged queue.
///
/// [`parent`](Self::parent) and [`child`](Self::child) are the install's two arguments. Reading
/// them out and passing them back is not ceremony: it is what makes the install decide on the pair
/// the caller was shown rather than on whatever a second global drain would reach, so a caller that
/// substituted either id gets `NotYieldable` instead of somebody else's fork.
///
/// It borrows the container, so a caller must drop it before installing — which is the point. The
/// old design handed the partition itself across that boundary and every layer it crossed had to
/// re-implement the container's bookkeeping; this hands over a decision to look at, and the fork
/// never leaves home.
pub struct ForkView<'a, G, I> {
  parent: G,
  plan: YieldPlan<G, I>,
  /// The view keeps the container borrowed for as long as it lives. That is the whole mechanism:
  /// a caller must DROP the view before it can call the install, so there is no window in which a
  /// decision is held while the container moves on beneath it.
  _borrow: core::marker::PhantomData<&'a ()>,
}

impl<G, I> ForkView<'_, G, I> {
  /// The parent group whose committed split produced this fork — the install's first argument.
  #[must_use]
  pub const fn parent(&self) -> &G {
    &self.parent
  }

  /// The child group id this fork would materialize — the install's second argument.
  #[must_use]
  pub const fn child(&self) -> &G {
    &self.plan.child
  }

  /// The child's incarnation under the unified lineage counter.
  #[must_use]
  pub const fn child_gen(&self) -> u64 {
    self.plan.child_gen
  }

  /// The parent's lineage counter after this split.
  #[must_use]
  pub const fn parent_gen_after(&self) -> u64 {
    self.plan.parent_gen_after
  }

  /// The split entry's index in the parent's log — the fork durability barrier's anchor.
  #[must_use]
  pub const fn split_index(&self) -> Index {
    self.plan.split_index
  }

  /// The child's boot config as the container derived it: the parent's local tuning under the
  /// voter set the committed split fixed. The container applies
  /// [`reshape_born_prevention`] to this at install, so a caller validating it should validate the
  /// same transform.
  #[must_use]
  pub const fn config(&self) -> &Config<I> {
    &self.plan.config
  }
}

/// The install's front half: either everything the write needs, or the outcome to return.
#[allow(clippy::large_enum_variant)]
enum PrepareOutcome<G, I> {
  Ready {
    parent: G,
    plan: YieldPlan<G, I>,
    epoch: u64,
    config: Config<I>,
  },
  Done(InstallOutcome<G, I>),
}

/// Where one relay drain ended.
#[allow(clippy::large_enum_variant)]
enum DrainOutcome<G, I> {
  /// A head fork is ready to install, staged and un-consumed.
  Yieldable { parent: G, plan: YieldPlan<G, I> },
  /// Nothing yieldable, but the drain ABANDONED a head fork on the way: a barrier resolved and a
  /// refusal queued, so the caller should come round again.
  Refused,
  /// Nothing yieldable and a parent is parked — the child id is spoken for.
  Held,
  /// Nothing staged anywhere.
  Empty,
}

/// What one [`install_yieldable_fork`](MultiRaft::install_yieldable_fork) attempt did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InstallOutcome<G, I> {
  /// The child was materialized here. The caller mirrors the two lineage records and arms its
  /// durability barrier; `config` is the boot config the container actually installed.
  Installed {
    /// The parent group whose split this was.
    parent: G,
    /// The child group now hosted here.
    child: G,
    /// The child's incarnation.
    child_gen: u64,
    /// The parent's lineage after the split — the caller's durable relay-guard mirror.
    parent_gen_after: u64,
    /// The split entry's index in the parent's log — the barrier anchor to lift when durable.
    split_index: Index,
    /// The boot config the child was installed with.
    config: Config<I>,
  },
  /// The named parent's head fork took a RESOLUTION arm during this attempt: abandoned deliberately
  /// (its barrier resolved and the refusal queued for
  /// [`poll_split_refusal`](MultiRaft::poll_split_refusal)), or folded as redundant. Nothing
  /// installed this time, but the queue moved — a drain should come round again.
  Refused,
  /// The child id is spoken for right now (occupied stores, a tombstone, a hosted twin). The fork
  /// stays STAGED with its blob, its fence and its reservation intact.
  Held,
  /// The named parent's head fork names a DIFFERENT child: a caller that substituted an id, or a
  /// pair the container is not about to install. The fork stays staged.
  NotYieldable,
  /// The named parent has no staged fork to examine.
  Empty,
}

/// The create/restore admission check shared by every group constructor: group-id uniqueness, the
/// wire bound on the encoded group id, and agreement with the LATCHED host identity (a multi-Raft
/// host is one physical node for its whole lifetime — the transport authenticates exactly one
/// identity per connection, and live connections outlast group removal, so the check must not
/// relax when the map empties).
fn validate_new_group<G, I, F, R>(
  groups: &BTreeMap<G, Endpoint<I, F, R>>,
  host_id: &Option<I>,
  gid: &G,
  config: &Config<I>,
) -> Result<(), CreateGroupError>
where
  G: GroupId,
  I: NodeId,
  F: StateMachine,
{
  if groups.contains_key(gid) {
    return Err(CreateGroupError::Exists);
  }
  let mut encoded = Vec::new();
  gid.encode(&mut encoded);
  if encoded.is_empty() || encoded.len() > crate::wire::MAX_GROUP_ID_LEN {
    return Err(CreateGroupError::InvalidGroupId);
  }
  if let Some(host) = host_id
    && *host != config.id()
  {
    return Err(CreateGroupError::NodeIdMismatch);
  }
  // A gid named as the absorbed source of an outstanding capture debt admits NOTHING: its
  // preserved stores are the union's only restart derivation, and any admission here — an
  // embedder create, a restore over those very stores, a fork materialization, a solicited
  // factory build (whose driver gate consults this same refusal transitively) — would revive a
  // husk beside the already-absorbed union. Self-releasing at the debt's discharge. The WHOLE
  // chain: an inherited debt names a source absorbed transitively, whose stores are pinned
  // exactly the same way.
  if groups.values().any(|ep| {
    ep.capture_debt_chain()
      .any(|m| m.source().as_ref() == encoded.as_slice())
  }) {
    return Err(CreateGroupError::AbsorbPending);
  }
  // THE SECOND LEG, and the earlier one in the merge's life: a hosted target PARKED on a
  // `CommitMerge` that names this gid as its absorbed source, whose abort window has LATCHED
  // CLOSED. Closed means `k + 1` is committed and is not this merge's abort, so no abort can ever
  // contest it again — the consumption is decided, and this id is spoken for by a union that has
  // not yet been able to materialize here. A bare or OPEN park NEVER qualifies: it is undecided,
  // rollback deliberately races a parked commit, and an abort resurrects the source.
  //
  // Why refusing admission is sound, twice over. (a) A latched-Closed park already authorizes the
  // resolver's `Resolve` arm to DESTROY the source endpoint outright; refusing to ADMIT one is
  // strictly weaker than the destruction the same evidence already licenses. (b) The park stops
  // the target's apply drain at `k - 1`, so every fork staged on that endpoint carries a split
  // index below `k` and the absorbed union SUBSUMES its blob — nothing is lost by refusing, and no
  // later re-split can stage while the park stands.
  //
  // ORDERING DEPENDENCY, load-bearing: this leg answers `true` even when the source is still
  // HOSTED, and it is correct only because the `Exists` check above runs first. Lifting this
  // predicate out of `validate_new_group` and consulting it standalone would refuse a live,
  // hosted, still-abortable id.
  if groups.values().any(|ep| {
    ep.pending_merge()
      .is_some_and(|p| p.window_closed() && p.source_bytes().as_ref() == encoded.as_slice())
  }) {
    return Err(CreateGroupError::AbsorbPending);
  }
  Ok(())
}

/// A container of single-group [`Endpoint`]s multiplexed by [`GroupId`].
///
/// Generic over the group id `G`, the node id `I`, the application state machine `F`, and the
/// election RNG `R` (defaulting to the deterministic [`Prng`], as [`Endpoint`] does). See the
/// module-level documentation for the driving model and `MULTI_RAFT.md` for the architecture.
pub struct MultiRaft<G, I, F, R = Prng>
where
  F: StateMachine,
{
  groups: BTreeMap<G, Endpoint<I, F, R>>,
  /// Groups that may have a pending outbound message to drain (see [`poll_message`](Self::poll_message)).
  /// Enqueued after every dispatch and removed lazily once the group's message queue is exhausted.
  dirty_msgs: VecDeque<G>,
  /// Membership mirror of `dirty_msgs`: holds exactly the queued gids, so `mark_dirty` enqueues a
  /// group at most once while it stays queued (the bound the queue relies on under interleaving).
  dirty_msgs_set: BTreeSet<G>,
  /// Groups that may have a pending event to drain (see [`poll_event`](Self::poll_event)).
  dirty_events: VecDeque<G>,
  /// Membership mirror of `dirty_events`, kept exact with it at every push and pop.
  dirty_events_set: BTreeSet<G>,
  /// Groups that may have a staged pending fork to relay (see
  /// [`peek_yieldable_fork`](Self::peek_yieldable_fork)).
  dirty_forks: VecDeque<G>,
  /// Membership mirror of `dirty_forks`, kept exact with it at every push and pop.
  dirty_forks_set: BTreeSet<G>,
  /// Parents whose HEAD fork is PARKED on a hosted-child conflict (see
  /// [`peek_yieldable_fork`](Self::peek_yieldable_fork)): the fork stays staged (blob retained, the
  /// snapshot fence armed, the relay guard unmoved) and is re-examined at the top of every relay
  /// drain — the resolution triggers are CHILD-side (removal, catch-up), so no parent dispatch
  /// re-marks these. Membership doubles as the conflict-signal dedupe: one
  /// [`poll_split_conflict`](Self::poll_split_conflict) signal per park episode.
  parked: BTreeMap<G, ParkCause>,
  /// Pending `(parent, child)` split-conflict signals, pushed once per park episode and HELD
  /// until consumed: a driver publishing on a bounded tail peeks
  /// ([`peek_split_conflict`](Self::peek_split_conflict)), publishes, and consumes only on
  /// acceptance ([`poll_split_conflict`](Self::poll_split_conflict)), so backpressure defers
  /// the episode's only cue instead of erasing it. Every arm that ends a park purges its
  /// still-queued signal ([`unpark`](Self::unpark)) — delivered after resolution it would be
  /// stale — so queued signals always name currently-parked parents.
  conflicts: VecDeque<(G, G)>,
  /// Pending `(parent, child)` split-REFUSAL signals: a committed fork the relay abandoned
  /// deliberately, because the refusal is a verdict about the fork itself and no host state will
  /// ever change it. Peek/consume-on-accept like [`conflicts`](Self::conflicts) — a refusal is
  /// one-shot too — but never purged: the abandonment already happened and the embedder is owed
  /// the news whatever else resolves.
  refusals: VecDeque<(G, G)>,
  /// Pending `(parent, lineage)` relay-guard advances the caller must MIRROR into its durable
  /// lineage record. Pushed only by the removal-time fork abandonment, whose volatile guard bump
  /// would otherwise be lost on a crash and let the replayed split re-stage the very fork the
  /// removal killed. Every other advance already rides a durable write the caller makes anyway
  /// (a relayed fork's child registration), so this queue exists for that one arm.
  guard_advances: VecDeque<(G, u64)>,
  /// The last [`MergeBlockedCause`] signalled per target — the EDGE that makes
  /// [`poll_merge_blocked`](Self::poll_merge_blocked) fire once per transition instead of once per
  /// crank. An entry lives exactly as long as the target's park or capture debt does (dropped at
  /// the top of the next [`service_merge_applies`](Self::service_merge_applies) once neither
  /// stands, and at removal), so a target that gets held again later signals afresh.
  merge_blocked_seen: BTreeMap<G, (MergeBlockedCause, G, Index)>,
  /// Every `note_merge_blocked` ATTEMPT of the CURRENT service crank (the edge dedupe absorbs
  /// repeats downstream) — the end-of-crank retirement's evidence that a cause was still
  /// derived this crank. Drained there; meaningful only within one `service_merge_applies`.
  merge_blocked_attempts: BTreeMap<G, (MergeBlockedCause, G, Index)>,
  /// Pending structural-hold observations, drained by
  /// [`poll_merge_blocked`](Self::poll_merge_blocked). Best-effort like the poison tail: the
  /// container's own re-derivation, not this queue, is what keeps the hold resolving, so a
  /// dropped signal costs the embedder a notification and nothing else.
  merge_blocked: VecDeque<MergeBlocked<G>>,
  /// Groups seen poisoned since their CURRENT hosting began — the dedupe that makes
  /// [`poll_poisoned`](Self::poll_poisoned) fire once per poisoning per hosted incarnation. An id's
  /// entry is cleared at removal, so a re-admitted id that poisons again re-signals as a fresh
  /// incarnation.
  poisoned_seen: BTreeSet<G>,
  /// Groups whose fail-stop has not yet surfaced on the aggregate lifecycle tail: pushed once when a
  /// crank leaves an endpoint poisoned (a storage/apply fault) and drained by
  /// [`poll_poisoned`](Self::poll_poisoned). An OBSERVATION riding a best-effort tail — a removal
  /// purges any still-queued signal, so a delivered id names a currently poisoned, hosted group.
  poisoned_pending: VecDeque<G>,
  /// The RELAY-TIME lineage view, one entry per admitted id: seeded at every admission (0 at
  /// genesis create, the DURABLE restored lineage at restore/fork) and bumped to
  /// `parent_gen_after` when a fork is relayed. This is the replay guard: a fork whose bump is
  /// at-or-below this view was already relayed (a same-gen retry duplicate, or a tail replayed
  /// under a snapshot that already carries the bump) and folds to a resolved no-op. Deliberately
  /// DISTINCT from the endpoints' live `shape_gen` (bumped at APPLY, before the relay — guarding
  /// on it would drop every first relay).
  lineage: BTreeMap<G, u64>,
  /// The host's node identity, latched by the FIRST admitted group and retained for the
  /// container's whole lifetime — including across [`remove_group`](Self::remove_group) emptying
  /// the map. A multi-Raft host is one physical node: live transport connections stay
  /// authenticated under this id, so a later admission must never change it (see
  /// [`CreateGroupError::NodeIdMismatch`]).
  host_id: Option<I>,
}

// No `I` (or `R`) bound: the container and drain surface neither keys by, encodes, nor clones a
// node id — the delegated `Endpoint` polls live in its bound-free block.
impl<G, I, F, R> MultiRaft<G, I, F, R>
where
  G: GroupId,
  F: StateMachine,
{
  /// An empty host with no groups.
  #[must_use]
  pub fn new() -> Self {
    Self {
      groups: BTreeMap::new(),
      dirty_msgs: VecDeque::new(),
      dirty_msgs_set: BTreeSet::new(),
      dirty_events: VecDeque::new(),
      dirty_events_set: BTreeSet::new(),
      dirty_forks: VecDeque::new(),
      dirty_forks_set: BTreeSet::new(),
      parked: BTreeMap::new(),
      conflicts: VecDeque::new(),
      refusals: VecDeque::new(),
      guard_advances: VecDeque::new(),
      merge_blocked_seen: BTreeMap::new(),
      merge_blocked_attempts: BTreeMap::new(),
      merge_blocked: VecDeque::new(),
      poisoned_seen: BTreeSet::new(),
      poisoned_pending: VecDeque::new(),
      lineage: BTreeMap::new(),
      host_id: None,
    }
  }

  /// The number of hosted groups.
  #[must_use]
  pub fn len(&self) -> usize {
    self.groups.len()
  }

  /// Whether no groups are hosted.
  #[must_use]
  pub fn is_empty(&self) -> bool {
    self.groups.is_empty()
  }

  /// Whether a group with `gid` is hosted.
  #[must_use]
  pub fn contains_group(&self, gid: &G) -> bool {
    self.groups.contains_key(gid)
  }

  /// The host's node identity — latched by the first admitted group and retained across group
  /// removals ([`CreateGroupError::NodeIdMismatch`] enforces it). `None` until any group has ever
  /// been admitted.
  #[must_use]
  pub fn host_id(&self) -> Option<&I> {
    self.host_id.as_ref()
  }

  /// A shared reference to one group's [`Endpoint`], for observability (role, term, commit,
  /// applied index, leader, poison, the state machine). `None` if no such group.
  #[must_use]
  pub fn group(&self, gid: &G) -> Option<&Endpoint<I, F, R>> {
    self.groups.get(gid)
  }

  /// A mutable reference to one group's [`Endpoint`] — test-only, for reconstructing durable-derived
  /// endpoint state (e.g. a replay-re-derived merge obligation) a unit test cannot otherwise reach.
  #[cfg(test)]
  pub(crate) fn group_mut(&mut self, gid: &G) -> Option<&mut Endpoint<I, F, R>> {
    self.groups.get_mut(gid)
  }

  /// The hosted group ids, ascending.
  pub fn group_ids(&self) -> impl Iterator<Item = &G> {
    self.groups.keys()
  }

  /// Remove and return a group's [`Endpoint`]. Stale drain-queue entries for it are skipped on the
  /// next poll. This is the dynamic-lifecycle teardown seam: the container stays PURE (no
  /// tombstone here — the multi coordinators tombstone the id so the wire's stragglers drop
  /// silently). The host identity is NOT cleared — even removing the last group leaves it
  /// latched, so a re-created group must carry the same node id (live transport connections stay
  /// authenticated under it).
  ///
  /// This is also the SINGLE choke point where a merge SOURCE leaves the container, so it PURGES
  /// the removed id's outstanding thaw obligation from every target — see the inline note.
  ///
  /// REFUSES every UNRESOLVED merge participant, making the "do not remove a merge participant"
  /// contract STRUCTURAL: the group is left FULLY intact (endpoint, stores, obligations) and the
  /// refusal is a no-op the caller retries. Each refusal is self-clearing once the merge resolves.
  /// The five legs are the CLOSED product of the choreography's participant states — the set
  /// `{holder} ∪ {source: freeze-pending | frozen} ∪ {target: parked | claimed-applied |
  /// claimed-pending} ∪ {named-as-source-by-a-park}` — so no in-flight role can slip the gate (the
  /// pending-`CommitMerge` windows need no leg of their own: the barrier holds the source `Frozen`
  /// and the park `MergeParked` throughout):
  ///
  /// - [`OwesThaw`](RemoveError::OwesThaw) — the group owes an aborted upstream source its thaw
  ///   ([`has_abandoned`](Endpoint::has_abandoned)); its own log is that obligation's only replay
  ///   source. Discharged by the per-crank thaw pass (or a floor on the owed source).
  /// - [`Frozen`](RemoveError::Frozen) — the group is a merge SOURCE mid-freeze
  ///   ([`merge_freeze_active`](Endpoint::merge_freeze_active)); its target parks against this exact
  ///   freeze, so tearing it down strands the park. Roll the merge back first (abort → thaw).
  /// - [`MergeParked`](RemoveError::MergeParked) — the group is a TARGET parked on a `CommitMerge`
  ///   ([`pending_merge`](Endpoint::pending_merge)); removing the decider strands the frozen source.
  ///   Let the merge resolve (absorb or abort) first.
  /// - [`SpokenFor`](RemoveError::SpokenFor) — another hosted endpoint's park names this group as
  ///   its source (scanned as the thaw pass scans), covering the window before this group's own
  ///   replica has observed its freeze.
  /// - [`Claimed`](RemoveError::Claimed) — another hosted SOURCE names this group as its TARGET
  ///   (applied `frozen_for`, or an append-pending `PrepareMerge` decoded from the source's log)
  ///   before this group has parked its `CommitMerge` — the mirror of `SpokenFor`, covering the
  ///   pre-park window `MergeParked` cannot. Roll the naming merge back first (this group is hosted
  ///   pre-park, so `rollback_merge` on it thaws the source), then the removal admits.
  ///
  /// The DESIGNED CATALOG ESCAPE is deliberately NOT gated: removing an OWED source (a frozen source
  /// a hosted target still owes a thaw AT the generation the source is live at) ADMITS — the purge
  /// below binds every holder's obligation to this incarnation and the driver floors the id, the
  /// recovery path for a genuinely-dead frozen participant. `Frozen` steps aside for exactly it, and
  /// GENERATION-EXACTLY: a spent obligation the source already thawed past and re-froze above names
  /// a dead incarnation, so it can never carry a newly-frozen source out from under its new target.
  ///
  /// The container stays PURE (no tombstone here — the multi coordinators tombstone the id). The
  /// host identity is NOT cleared — even removing the last group leaves it latched, so a re-created
  /// group must carry the same node id (live transport connections stay authenticated under it).
  /// This is also the SINGLE choke point where a merge SOURCE leaves the container, so it PURGES the
  /// removed id's outstanding thaw obligation from every target — see `remove_group_inner`. `Ok(None)`
  /// when no such group is hosted (removing an absent id is a no-op).
  pub fn remove_group<L, S, St>(
    &mut self,
    gid: &G,
    stores: &mut St,
  ) -> Result<Option<Endpoint<I, F, R>>, RemoveError>
  where
    St: GroupStores<G, L, S>,
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    // THE PARTICIPANT GATE, refused BEFORE any teardown so the group stays fully intact. Leg 1: a
    // hosted holder of an undischarged target-role thaw obligation cannot leave — its log is the
    // sole replay source that runs the outstanding thaw, so destroying it wedges the owed source
    // frozen forever. Self-clearing off the per-crank thaw pass (or a floor on the owed source).
    if self.groups.get(gid).is_some_and(Endpoint::has_abandoned) {
      return Err(RemoveError::OwesThaw);
    }
    // An OWED source — one a hosted target still owes a thaw AT the generation this source is live
    // at — is the designed catalog escape: the purge in `remove_group_inner` binds the obligation
    // to this incarnation and the driver floors the id, so a genuinely-dead frozen source stays
    // removable. Leg 2 steps aside for exactly it. GENERATION-EXACT by the campaign rule: a
    // cross-referenced authorization holds only for the precise incarnation it names — a spent
    // obligation the source already thawed past and re-froze above escapes NOTHING, so a freshly
    // frozen source cannot ride a stale record out from under its new target's forming park.
    let owed_source = self.some_target_owes_thaw_to(gid);
    // Leg 2: a merge SOURCE whose freeze is ACTIVE (applied `Frozen`, or an appended-but-unapplied
    // `PrepareMerge`) — its claimed target parks against this exact freeze, so tearing it down
    // strands the park with no source left to absorb or abort against. Suppressed for the escape.
    if !owed_source
      && self
        .groups
        .get(gid)
        .is_some_and(Endpoint::merge_freeze_active)
    {
      return Err(RemoveError::Frozen);
    }
    // Leg 3: a TARGET parked on a committed `CommitMerge` — removing the decider strands the frozen
    // source it claims, with no park left to complete the absorb or relay the abort.
    if self
      .groups
      .get(gid)
      .is_some_and(|ep| ep.pending_merge().is_some())
    {
      return Err(RemoveError::MergeParked);
    }
    // Leg 3b: a TARGET holding an outstanding capture debt — the fence-deferred absorb's window.
    // The debt's discharge is the only path that floors and tears down the consumed source, so
    // removing the holder strands that source's stores forever (un-floored, `Retired`-unreachable).
    if self
      .groups
      .get(gid)
      .is_some_and(|ep| ep.capture_debt().is_some())
    {
      return Err(RemoveError::OwesCapture);
    }
    // Leg 4: any OTHER hosted endpoint's park names gid as its source — the cross-endpoint leg,
    // covering the window where gid's own replica has not observed its own freeze yet.
    if self.park_names_source(gid) {
      return Err(RemoveError::SpokenFor);
    }
    // Leg 5: a CLAIMED TARGET pre-park — the last leg of the participant lattice. Another hosted
    // SOURCE names gid as its merge target (applied-frozen `frozen_for`, or an append-pending
    // `PrepareMerge` whose decoded claim is gid) while gid has NOT parked its own `CommitMerge` yet,
    // so legs 3/4 both miss it. Removing gid strands that source frozen for a missing target: its
    // absorb AND its abort both ride gid's log, so neither can be proposed once gid is gone, and the
    // source's own removal then refuses `Frozen` (it owes no thaw). Roll the naming merge back first
    // (gid is hosted pre-park, so `rollback_merge(gid, source)` rides gid's log and thaws the
    // source), after which this removal admits. Gated on gid being HOSTED, like legs 1-3: this leg
    // protects a live TARGET replica from teardown, so an absent gid is a plain `Ok(None)` no-op —
    // the merge Resolve arm's own ungated source removal has already left, and re-refusing that
    // cleanup would wedge the resolution. (Leg 4 is the sole unhosted-gid refusal: it guards a
    // departed SOURCE's id from tombstoning while a park still names it.)
    if self.groups.contains_key(gid) && self.some_source_claims_target(gid, stores) {
      return Err(RemoveError::Claimed);
    }
    Ok(self.remove_group_inner(gid))
  }

  /// The UNGATED teardown behind [`remove_group`](Self::remove_group)'s participant gate: unpark,
  /// remove, and PURGE the removed source's outstanding thaw obligation from every target. The
  /// merge-absorb Resolve arm calls this DIRECTLY — that path IS the choreography resolving, so the
  /// public gate's participant refusals (a frozen source, a park naming it) all describe the very
  /// in-flight merge and would wedge the absorb they exist to protect.
  fn remove_group_inner(&mut self, gid: &G) -> Option<Endpoint<I, F, R>> {
    // A consumed endpoint's outstanding capture debt is the CALLER's to surface: the public
    // remove refuses it (`OwesCapture`), and the two resolver teardowns — the Resolve arm and
    // the husk dissolve — inherit it into their own resolutions, discharging it on the same
    // barrier that covers the consumed state machine (which has carried the prior union since
    // that absorb applied). The debt is HOST-LOCAL, so a foreign-led merge can legally deliver
    // a debt-holder here for consumption; dropping the record instead would strand the prior
    // source's preserved stores as a restorable orphan beside state already transitively
    // absorbed.
    // A parked PARENT's staged forks die with its endpoint (removal is the embedder's explicit
    // destruction of this replica), so the park bookkeeping — a still-queued conflict signal
    // included — dies too. Removing a parked fork's CHILD needs nothing here: the next relay
    // drain re-examines the park and materializes.
    self.unpark(gid);
    // A poison signal names a hosted incarnation: an un-consumed one dies with the endpoint (stale
    // after teardown), and clearing `poisoned_seen` lets a re-admitted id that poisons again signal
    // afresh as the new incarnation it is.
    self.poisoned_seen.remove(gid);
    self.poisoned_pending.retain(|g| g != gid);
    // Same rule for the structural-hold observation, on both roles: a target that is gone holds
    // nothing, and a source that is gone is not the hold anyone can act on. Clearing the edge lets
    // a re-admitted id that gets held again signal afresh.
    self.merge_blocked_seen.remove(gid);
    // A remembered observation NAMING the removed gid clears too, so the successor cause (the
    // next hosted crossing, the underlying unhosted source) can signal afresh.
    self.merge_blocked_seen.retain(|_, (_, src, _)| src != gid);
    self
      .merge_blocked
      .retain(|b| &b.target != gid && &b.source != gid);
    let removed = self.groups.remove(gid);
    // PURGE-ON-REMOVAL: a source leaving the container takes every target's outstanding thaw
    // obligation for it along with it, binding the obligation to the incarnation SYNCHRONOUSLY —
    // race-free with the removal and independent of whatever floor the embedder does or does not
    // persist. Under the unified lineage counter this is the FAST PATH, not the safety line: a
    // floored recreate admits (and seeds) strictly above every generation an obligation could
    // name, so a stale record can never match a fresh incarnation's freeze anyway. It remains the
    // whole story for a NoFloors embedder (whose recreates may re-mint a removed incarnation's
    // generations — the documented cost of opting out of floors, #22 reopened across
    // incarnations). The merge Resolve arm removes a source too. By then the source owes nothing TO
    // it (it thawed past any earlier abort before becoming free to merge, discharging every target's
    // `abandoned[self]`). As a former TARGET it may still OWE a thaw, but only a LOCALLY-UNDRIVABLE
    // one: `prepare_merge`'s `SourceOwesThaw` gate refuses a source that owes at propose, and the
    // Resolve arm HOLDS the absorb while a DRIVABLE obligation (its owed target hosted here) stands —
    // so what survives to the dissolve is a dead-end obligation this replica can neither drive nor
    // observe, which the belt DROPS by design (a co-hosting replica drives that thaw; dropping it
    // here strands nothing). The purge then clears whatever such residue the source still carries.
    if removed.is_some() && self.groups.values().any(Endpoint::has_abandoned) {
      let mut source_key = Vec::new();
      gid.encode(&mut source_key);
      let source_key = Bytes::from(source_key);
      for ep in self.groups.values_mut() {
        ep.clear_abandoned(&source_key);
      }
    }
    self.abandon_fork_this_removal_descends_from(gid, removed.as_ref());
    removed
  }

  /// TERMINALLY abandon a staged fork the replica just torn down PROVABLY descends from — the
  /// removal-time arm of the fork relay's provenance discipline.
  ///
  /// The hazard: a host that STAGES a fork for child `C`, then removes its own `C` and consents to
  /// `C` again, offers the held fork a clean slate to land on. What lands is the dead
  /// incarnation's baseline, and its inherited parent cells then coexist with whatever the new
  /// incarnation became elsewhere. A relay that holds indefinitely makes that shape deterministic
  /// rather than a one-batch race, which is why the hold needs this arm beside it.
  ///
  /// THE DISCRIMINATOR IS PROVENANCE, never occupancy. Only a removed endpoint carrying THIS
  /// fork's exact [`ForkId`] is evidence that the embedder just ended the story this very fork
  /// began — transfers and restarts carry the token, so a baseline that arrived by snapshot counts
  /// exactly as one materialized here. A token-less or differently-tokened removal is a SQUATTER
  /// leaving: it says nothing about the fork, which keeps its blob and its hold and lands when the
  /// id is consented to again. Getting this backwards would destroy a legitimate child partition's
  /// only local copy on the strength of an unrelated id's teardown.
  ///
  /// THE SCAN COVERS THE WHOLE STAGED QUEUE, and the reason is snapshot transfer. A fork behind
  /// the head does NOT need this host to have relayed it for its child to be hosted here: a
  /// sibling replica can materialize that later child and transfer its token-bearing baseline
  /// over, so the child is hosted-here-and-then-removed while an earlier fork is still parked in
  /// front of it. Scanning only the head leaves that fork condemned by nothing; once the earlier
  /// fork resolves and consent lifts the tombstone, it heads and resurrects the removed
  /// incarnation.
  ///
  /// ORDERED DEFERRAL is what reaches it without breaking the queue. A head hit is consumed
  /// immediately; a hit BELOW the head is MARKED and left exactly where it is, because consuming
  /// it out of turn would either reorder the FIFO the replay guard depends on or advance that
  /// guard past forks still staged in front of it. The drain then consumes a marked fork the
  /// instant it reaches the head — before examining it — so the abandonment lands in apply order
  /// and every guard advance is still a head advance.
  fn abandon_fork_this_removal_descends_from(
    &mut self,
    gid: &G,
    removed: Option<&Endpoint<I, F, R>>,
  ) {
    let Some(token) = removed.and_then(Endpoint::fork_id) else {
      return;
    };
    let mut child_key = Vec::new();
    gid.encode(&mut child_key);
    let child_key = Bytes::from(child_key);
    // Byte-compare the child id FIRST: minting allocates, and the relay path's cost contract is
    // allocation-free, so only a fork that actually names this child pays for a token.
    let hit = self.groups.iter().find_map(|(parent, ep)| {
      let head = ep.peek_pending_fork().map(|f| f.index);
      ep.staged_forks().find_map(|fork| {
        if fork.child_bytes != child_key {
          return None;
        }
        let minted = mint_fork_id(
          parent,
          fork.parent_gen_after,
          fork.index,
          fork.split_term,
          fork.child_bytes.clone(),
          fork.child_gen,
        );
        (minted == token).then(|| {
          (
            parent.cheap_clone(),
            fork.index,
            fork.parent_gen_after,
            head == Some(fork.index),
          )
        })
      })
    });
    let Some((parent, split_index, parent_gen_after, at_head)) = hit else {
      return;
    };
    if !at_head {
      // Condemned in place. Nothing else moves: the fork keeps its blob, its barrier and its
      // position, and the guard stays put — advancing it now would fold the forks staged AHEAD of
      // this one into duplicates and drop them.
      //
      // THE MARK IS VOLATILE, and the crash window it leaves is real and unbounded: a restart
      // replays the split entry, re-stages the fork with no memory of the condemnation, and the
      // fork can then materialize a child the embedder had removed. Nothing here narrows that —
      // the window closes only when the fork reaches the head and is consumed.
      //
      // IT IS ALSO NOT LOSS. The marked fork's OWN barrier keeps the parent from snapshotting past
      // its split, so the replay derivation survives indefinitely however long the mark waits; the
      // hazard is a resurrection, never a missing partition.
      //
      // WHY NO DURABLE MIRROR. Every CONSUMPTION persists its advance
      // ([`advance_relay_guard`](Self::advance_relay_guard)), but this is not a consumption: the
      // guard is a single monotone scalar and the queue is strictly FIFO, so recording this fork's
      // bump would claim the forks staged in front of it as well and fold them to duplicates on
      // replay. Contiguity is what forbids the mirror, not an oversight. A per-fork durable record
      // would carry it — that is the declined schema, and a condemnation floor was declined too
      // (it would repeal a gen-0 id's rejoin entitlement and kill the two-forks-one-child sibling
      // release).
      //
      // WHO IS EXPOSED. A FLOORED embedder is immune by construction: removing a child at
      // generation n > 0 writes floor n + 1 under the ack's own barrier, so the replayed fork dies
      // `BelowFloor` at the gate. The residual is exactly the gen-0/unfloored case, and it belongs
      // to the incarnation-epoch work tracked in #125, which owns every replay-resurrection path.
      if let Some(ep) = self.groups.get_mut(&parent) {
        ep.mark_fork_abandoned(split_index);
      }
      return;
    }
    // Deliberate abandonment, exactly as the `Terminal` verdict performs it: the barrier resolves
    // (the parent must not stay fenced for a fork that will never land) and the embedder is told.
    if let Some(ep) = self.groups.get_mut(&parent)
      && let Some((fork, _fsm)) = ep.pop_pending_fork()
    {
      ep.resolve_fork(fork.index);
    }
    // The guard advance the `Redundant` arm makes for the same reason: this head fork is finished
    // with, so a replayed split entry re-staging it must fold to a resolved no-op instead of
    // resurrecting it. Legal precisely BECAUSE it is the head — no earlier staged fork of this
    // parent is left behind for the advance to skip over.
    //
    // Volatile and durable together, monotone — see
    // [`advance_relay_guard`](Self::advance_relay_guard) for why a consumption owes both halves.
    self.advance_relay_guard(&parent, parent_gen_after);
    // The sweep's Resolved arm, mirrored: leaving the parent parked would strand it (its
    // resolution triggers are child-side), and leaving the stale conflict signal queued would
    // suppress the cue its NEXT child is owed.
    self.unpark(&parent);
    if self.dirty_forks_set.insert(parent.cheap_clone()) {
      self.dirty_forks.push_back(parent.cheap_clone());
    }
    self.refusals.push_back((parent, gid.cheap_clone()));
  }

  /// Advance `gid`'s relay guard past a fork this call CONSUMED — the volatile record and the
  /// caller's durable mirror together, because a consumption owes both.
  ///
  /// The in-memory record folds a replayed fork to a no-op for this process's lifetime; the queued
  /// advance ([`poll_relay_guard_advance`](Self::poll_relay_guard_advance)) is what carries the same
  /// verdict across a CRASH, because the caller mirrors it into its durable lineage record beside
  /// the writes it was already making. A consumption that moved only the volatile half re-stages its
  /// fork on the next restart and acts on it again — reinstalling a baseline the embedder's removal
  /// had ended.
  ///
  /// MONOTONE, like every other writer of this record: the guard legitimately runs AHEAD of the
  /// staged forks (a restore raises it from the durable mirror), so a consumption may only raise it.
  ///
  /// THE BOUNDARY LINE, for anyone adding an arm to the relay. Every CONSUMPTION that moves the
  /// guard persists its advance — that is this function, and it is uniform. A verdict that is
  /// RE-DERIVABLE from a durable fact needs no mirror at all: `Resolve` re-derives from the guard
  /// itself, `Terminal` from the floor, and a replay simply reaches the same answer again. The
  /// below-head condemnation mark is the unique arm that is NEITHER — not a consumption, and not
  /// re-derivable — and it is deliberately volatile, tracked in #125 (see the `!at_head` arm in
  /// `abandon_fork_this_removal_descends_from` for why contiguity forbids the mirror there).
  fn advance_relay_guard(&mut self, gid: &G, parent_gen_after: u64) {
    let guard = self
      .lineage
      .entry(gid.cheap_clone())
      .or_insert(parent_gen_after);
    *guard = (*guard).max(parent_gen_after);
    self
      .guard_advances
      .push_back((gid.cheap_clone(), parent_gen_after));
  }

  /// Consume `gid`'s condemned HEAD fork — the deferred half of the removal-time abandonment,
  /// performing there what a head hit performs at the removal itself: the fork is popped, its
  /// durability barrier resolved, the guard advanced past it (legal now, and only now, because it
  /// is the head), and the refusal surfaced. Reported as `Resolved`, so the caller re-examines this
  /// same parent and the next staged fork takes its turn.
  fn consume_abandoned_head_fork(&mut self, gid: &G) -> HeadFork<G, I> {
    let Some(ep) = self.groups.get_mut(gid) else {
      return HeadFork::Empty;
    };
    let Some((fork, _fsm)) = ep.pop_pending_fork() else {
      return HeadFork::Empty;
    };
    ep.resolve_fork(fork.index);
    self.advance_relay_guard(gid, fork.parent_gen_after);
    // The child id the mark named. Undecodable is unreachable — the mark was set by matching this
    // very encoding against a removed group's id — so the refusal is simply skipped rather than
    // guessed at.
    if let Ok(child) = G::decode_exact(fork.child_bytes.clone()) {
      self.refusals.push_back((gid.cheap_clone(), child));
    }
    HeadFork::Resolved
  }

  /// Whether any OTHER hosted target owes `gid` (a merge source) a thaw for the EXACT freeze
  /// incarnation `gid` is live at — the teardown gate's read for recognizing an OWED FROZEN source
  /// (the designed escape `Frozen` steps aside for). GENERATION-EXACT: a holder's obligation
  /// abandons one specific freeze generation, and the escape holds ONLY while `gid`'s live
  /// `shape_gen` still equals it. A delivered-but-undischarged obligation (the thaw already minted
  /// past it, so `expected < shape_gen`) — or one whose source has since re-frozen ABOVE it for a
  /// fresh merge — names a spent incarnation, not the freeze under removal, and must not suppress
  /// `Frozen` over the newly-frozen source. An unhosted `gid` has no live generation to escape and
  /// answers `false` (leg 2 is a no-op for it anyway). The purge in `remove_group_inner` stays
  /// id-wide by contrast: a departing incarnation voids ALL its obligations, stale ones included.
  fn some_target_owes_thaw_to(&self, gid: &G) -> bool {
    let Some(live_gen) = self.groups.get(gid).map(Endpoint::shape_gen) else {
      return false;
    };
    let mut key = Vec::new();
    gid.encode(&mut key);
    let key = Bytes::from(key);
    self
      .groups
      .iter()
      .any(|(g, ep)| g != gid && ep.owes_thaw_for(&key) == Some(live_gen))
  }

  /// Whether any OTHER hosted endpoint's parked `CommitMerge` names `gid` as its source — scanned
  /// exactly as the thaw pass scans hosted parks. The cross-endpoint teardown leg (`SpokenFor`).
  fn park_names_source(&self, gid: &G) -> bool {
    let mut key = Vec::new();
    gid.encode(&mut key);
    let key = Bytes::from(key);
    self.groups.iter().any(|(g, ep)| {
      g != gid
        && (ep.pending_merge().is_some_and(|p| p.source_bytes() == key)
          // The debt window: the park was consumed by a fence-deferred absorb, but the named
          // source's stores remain the union's only restart derivation until the discharge —
          // the naming outlives the park exactly that long. Inherited debts name sources whose
          // stores are pinned the same way.
          || ep.debt_names_source(&key))
    })
  }

  /// Whether any of `tgid`'s standing abort obligations names a source hosted-and-frozen on
  /// this container — the cure-advertisement gate's abort leg: an adopt clears obligations at
  /// the boundary, and clearing one whose live frozen counterparty sits RIGHT HERE would erase
  /// the only drive for its thaw (a host-local proof of nothing — the source strands frozen).
  /// Unhosted or hosted-but-unfrozen counterparties have no local strand to protect, and the
  /// boundary-proof clear is exactly right for them.
  fn obligation_names_hosted_unadvanced(&self, tgid: &G) -> bool {
    let Some(tep) = self.groups.get(tgid) else {
      return false;
    };
    tep
      .abandoned_obligations()
      .into_iter()
      .any(|(sb, expected, _)| {
        G::decode_exact(sb)
          .ok()
          .and_then(|source| self.groups.get(&source))
          // Withhold unless the hosted counterparty has provably advanced PAST the owed
          // generation. Freeze-active includes PENDING (the thaw path deliberately preserves
          // an obligation while its hosted source is still freeze-pending), and a hosted
          // source merely AT-OR-BELOW the owed generation is one delayed PrepareMerge away
          // from freezing at it — an adopt's boundary clear would then erase the freeze's
          // only local thaw driver in the same inbound batch. Only `shape_gen > expected` —
          // the same past-proof the thaw pass discharges on — makes the clear safe.
          .is_some_and(|sep| sep.merge_freeze_active() || sep.shape_gen() <= expected)
      })
  }

  /// A gid was just admitted (create / restore / fork): any hosted target whose park names it
  /// as the absorbed source is no longer locally unresolvable — the fold the hint declared
  /// impossible is now merely pending catch-up — so the advertisement must stop BEFORE the next
  /// resolver crank re-derives it. The gap this closes is admission-to-receipt: a cure blob
  /// dispatched in the same iteration would otherwise adopt against the previous crank's hint
  /// and clear a park over the freshly admitted source, stranding it as an orphan husk at a
  /// non-terminal floor. The placement recovery the blocked-park signal invites stays safe
  /// exactly because of this clear.
  fn clear_unresolvable_hints_naming(&mut self, gid: &G) {
    let mut key = Vec::new();
    gid.encode(&mut key);
    let key = Bytes::from(key);
    for ep in self.groups.values_mut() {
      // EVERY naming clears: a park whose own source just got a host is merely pending
      // catch-up; a park whose CROSSING set names the admitted gid must stop advertising
      // before a same-iteration cure delivery adopts across a now-hosted crossing; and a park
      // whose ABANDONED obligations name it must re-gate against the restored counterparty's
      // freeze state before an adopt's boundary clear could erase a live obligation.
      if ep.pending_merge().is_some_and(|p| p.source_bytes() == key)
        || ep.crossing_sources().contains(&key)
        || ep
          .abandoned_obligations()
          .into_iter()
          .any(|(sb, _, _)| sb == key)
      {
        ep.note_merge_park_unresolvable(false);
      }
    }
  }

  /// Whether any hosted target's outstanding capture debt names `gid` as its absorbed source —
  /// the debt-window naming the lifecycle surfaces consult: the id's preserved stores are the
  /// absorbed union's only restart derivation until the discharge, so nothing may re-host,
  /// re-materialize, or tombstone it meanwhile. The wire demux treats a debt-named id like a
  /// tombstone (frames drop silently, no unknown-group advisory is minted), and the factory's
  /// pre-build gate refuses it.
  pub fn debt_names(&self, gid: &G) -> bool {
    let mut key = Vec::new();
    gid.encode(&mut key);
    let key = Bytes::from(key);
    self.groups.values().any(|ep| ep.debt_names_source(&key))
  }

  /// Whether `source` owes a LOCALLY-DRIVABLE target-role thaw — the shared residual belt of the
  /// absorb Resolve arm and the husk dissolve. A source-side teardown drops the source's endpoint
  /// (and its outstanding obligations), so it must HOLD while THIS replica can still drive one, or the
  /// dropped obligation would strand the upstream source frozen forever. DRIVABLE = the owed target is
  /// HOSTED here (this replica can append that thaw and observe its lineage advance). An owed target
  /// NOT hosted here is a local DEAD END — a co-hosting replica drives it — so it does not hold, and
  /// dropping it here strands nothing (`prepare_merge`'s `SourceOwesThaw` gate refuses the common case
  /// at propose; this belt covers the abort a source applied AS A TARGET that materialized an
  /// obligation below its own freeze). An owed id whose committed bytes will NOT decode is
  /// committed-corrupt: POISON the source (the `MergeDecode` fail-stop every host reaches
  /// deterministically) and HOLD, never authorizing a teardown some other host would fail-stop on. The
  /// poison makes this a side-effecting read (`&mut self`).
  fn owes_a_drivable_thaw(&mut self, source: &G) -> bool {
    let obligations = self
      .groups
      .get(source)
      .map(Endpoint::abandoned_obligations)
      .unwrap_or_default();
    let mut drivable = false;
    for (owed, _, _) in obligations {
      match G::decode_exact(owed) {
        Ok(owed) if self.groups.contains_key(&owed) => drivable = true,
        // Decodable but not hosted here: a local dead-end — a co-hosting replica drives it.
        Ok(_) => {}
        Err(_) => {
          if let Some(sep) = self.groups.get_mut(source) {
            sep.poison(PoisonReason::MergeDecode);
          }
          self.note_if_poisoned(source);
          drivable = true;
        }
      }
    }
    drivable
  }

  /// Whether any OTHER hosted endpoint is a merge SOURCE that names `gid` as its TARGET — the
  /// claimed-target teardown leg (`Claimed`), covering the pre-park window where a source has frozen
  /// toward `gid` but `gid` has not yet parked its `CommitMerge` (legs 3/4 read `gid`'s own park; a
  /// park-naming-source read the mirror direction). Two sub-legs, one per freeze phase:
  ///
  /// - APPLIED — the source's [`frozen_for`](Endpoint::frozen_for) claim decodes to `gid`. A pure
  ///   in-memory read, exact once the freeze applies.
  /// - APPEND-PENDING — the source's freeze is only append-observed (freeze-pending, not yet
  ///   applied), so its claim is still undecoded in-memory; decode it from the source's own log with
  ///   [`scan_freeze_claim`](Endpoint::scan_freeze_claim). Gated on freeze-pending so the bounded
  ///   walk runs ONLY for a source mid-freeze, never on an idle group's suffix. A read/decode fault
  ///   REFUSES (returns `true`): the gate treats a claim it cannot rule out as present rather than
  ///   risk removing a target out from under a source it could not inspect. Off the append hot path —
  ///   this decode is paid per (rare) removal, not per append.
  fn some_source_claims_target<L, S, St>(&self, gid: &G, stores: &mut St) -> bool
  where
    St: GroupStores<G, L, S>,
    L: LogStore,
  {
    // APPLIED leg (in-memory, stores-free — shared with the conf-change fence): a folded freeze's
    // target claim is decoded in `frozen_for`.
    if self.some_source_applied_claims_target(gid) {
      return true;
    }
    // APPEND-PENDING leg: the freeze is observed only at append (its payload undecoded until
    // apply), so read the claim off the source's log. A faulted scan/decode fails closed.
    let mut target_key = Vec::new();
    gid.encode(&mut target_key);
    let target_key = Bytes::from(target_key);
    for (g, ep) in self.groups.iter() {
      if g == gid {
        continue;
      }
      if ep.merge_freeze_active() && !ep.is_frozen() {
        let Some((log, _)) = stores.stores(g) else {
          continue;
        };
        match Endpoint::<I, F, R>::scan_freeze_claim(&*log, ep.applied_index()) {
          Ok(Some(claim)) => {
            if claim == target_key {
              return true;
            }
          }
          Ok(None) => {}
          Err(_) => return true,
        }
      }
    }
    false
  }

  /// Whether any OTHER hosted source's APPLIED freeze names `gid` as its merge target — the
  /// in-memory (stores-free) APPLIED sub-leg of [`some_source_claims_target`], used by the teardown
  /// gate (which must refuse tearing down ANY claimed target, aborted or not — its endpoint carries
  /// the source's thaw obligation).
  fn some_source_applied_claims_target(&self, gid: &G) -> bool {
    let mut target_key = Vec::new();
    gid.encode(&mut target_key);
    let target_key = Bytes::from(target_key);
    self
      .groups
      .iter()
      .any(|(g, ep)| g != gid && ep.frozen_for().is_some_and(|t| *t == target_key))
  }

  /// Whether any OTHER hosted source's APPLIED freeze names `gid` as its merge target for a merge
  /// `gid` has NOT yet aborted — the conf-change fence's read. The hazard is a voter change moving
  /// `gid`'s voters WHOLLY OFF the frozen source's hosts, after which the merge can neither commit
  /// (`VoterSetsDiffer`) nor abort (`SourceMissing`): the source strands frozen with no release
  /// valve. Growing the set (an add) never moves voters off — the abort stays reachable — but is
  /// still fenced here while the claim is UNRESOLVED, both because a diverged voter set already
  /// blocks the commit and to keep the reshape from racing the merge. Once `gid` ABORTS the merge
  /// (it holds `abandoned[source]`), the source thaws off that durable obligation regardless of
  /// `gid`'s voter set, so voter changes become safe and this returns false — the exemption the
  /// dead-end-obligation world test turns on. Stores-free (in-memory `frozen_for`/`abandoned`), so
  /// the conf-change delegates, holding only `gid`'s own log, consult it directly.
  fn some_source_claims_target_unresolved(&self, gid: &G) -> bool {
    let mut target_key = Vec::new();
    gid.encode(&mut target_key);
    let target_key = Bytes::from(target_key);
    let target = self.groups.get(gid);
    self.groups.iter().any(|(g, ep)| {
      if g == gid || !ep.frozen_for().is_some_and(|t| *t == target_key) {
        return false;
      }
      let mut source_key = Vec::new();
      g.encode(&mut source_key);
      let source_key = Bytes::from(source_key);
      // Exempt a claim `gid` has already aborted: the source thaws off `abandoned[source]`, not off
      // `gid`'s voters, so moving them cannot strand it.
      target.is_none_or(|t| t.owes_thaw_for(&source_key).is_none())
    })
  }

  /// End `gid`'s park episode: leave the parked set and purge any still-queued conflict
  /// signal. Every arm that resolves a park routes through here, so an UNDELIVERED signal (one
  /// a full driver tail deferred) dies with its episode — delivered afterwards it would be
  /// stale, capable of goading the embedder into removing the very child the resolution just
  /// materialized or blessed.
  fn unpark(&mut self, gid: &G) {
    // Signals are queued only while their parent is parked (the queue invariant this helper
    // maintains), so a no-op removal proves there is nothing to purge.
    if self.parked.remove(gid).is_some() {
      self.conflicts.retain(|(parent, _)| parent != gid);
    }
  }

  /// The next outbound message from any group, stamped with its group id. Drain fully between
  /// drives (the per-group queues are unbounded, as [`Endpoint::poll_message`] is).
  pub fn poll_message(&mut self) -> Option<(G, Outgoing<I>)> {
    while let Some(gid) = self.dirty_msgs.front().map(CheapClone::cheap_clone) {
      match self.groups.get_mut(&gid).and_then(Endpoint::poll_message) {
        Some(msg) => return Some((gid, msg)),
        None => {
          self.dirty_msgs.pop_front();
          self.dirty_msgs_set.remove(&gid);
        }
      }
    }
    None
  }

  /// The next application event from any group, stamped with its group id.
  pub fn poll_event(&mut self) -> Option<(G, Event<I, F::Response>)> {
    while let Some(gid) = self.dirty_events.front().map(CheapClone::cheap_clone) {
      match self.groups.get_mut(&gid).and_then(Endpoint::poll_event) {
        Some(ev) => return Some((gid, ev)),
        None => {
          self.dirty_events.pop_front();
          self.dirty_events_set.remove(&gid);
        }
      }
    }
    None
  }

  /// Enqueue a group for output draining after a dispatch. Membership-deduped against each queue's
  /// companion set, so at most one entry per group per queue is live between drains — interleaved
  /// dispatches across many groups (A,B,A,B,…) never grow a queue past the count of distinct dirty
  /// groups, and a group re-marked while still queued is not re-enqueued (its pending visit drains
  /// everything staged since). The post-dispatch choke point also latches a poison signal: a crank
  /// that fail-stopped its endpoint surfaces once on [`poll_poisoned`](Self::poll_poisoned).
  fn mark_dirty(&mut self, gid: &G) {
    if self.dirty_msgs_set.insert(gid.cheap_clone()) {
      self.dirty_msgs.push_back(gid.cheap_clone());
    }
    if self.dirty_events_set.insert(gid.cheap_clone()) {
      self.dirty_events.push_back(gid.cheap_clone());
    }
    if self.dirty_forks_set.insert(gid.cheap_clone()) {
      self.dirty_forks.push_back(gid.cheap_clone());
    }
    self.note_if_poisoned(gid);
  }

  /// Latch a one-shot poison signal for `gid` when its endpoint is fail-stopped: the aggregate
  /// mirror of [`Endpoint::is_poisoned`], surfaced once per poisoning per hosted incarnation via
  /// [`poll_poisoned`](Self::poll_poisoned). Called at the post-dispatch choke point (catching a
  /// crank that self-poisons) and beside every container-driven `poison` on a path that does not
  /// route through it; `poisoned_seen` makes the overlap idempotent.
  fn note_if_poisoned(&mut self, gid: &G) {
    if self.groups.get(gid).is_some_and(Endpoint::is_poisoned)
      && self.poisoned_seen.insert(gid.cheap_clone())
    {
      self.poisoned_pending.push_back(gid.cheap_clone());
    }
  }

  /// Fail-stop the addressed group because a user QUERY closure panicked mid-read against its state
  /// machine, then LATCH the signal for [`poll_poisoned`](Self::poll_poisoned). A driver caught the
  /// unwind (keeping its plane and every co-located group alive) and routes here so this group joins
  /// the poison surface: the closure borrows only `&F`, but interior mutability could have torn
  /// replicated state, so fail-stop beats risking silent divergence from replicas that never ran it.
  /// A no-op if the group is not hosted. The explicit `note_if_poisoned` latches this driver-invoked
  /// poison exactly as the post-dispatch choke point latches a crank that self-poisons.
  pub fn fail_stop_query_panicked(&mut self, gid: &G) {
    if let Some(ep) = self.groups.get_mut(gid) {
      ep.fail_stop_query_panicked();
    }
    self.note_if_poisoned(gid);
  }

  /// Fail-stop EVERY hosted group because a completion caught a user-closure(-drop) panic this
  /// container cannot ATTRIBUTE to one — the verdict a refusal addressed to a group this host does
  /// NOT carry reports (the drivers' not-hosted `query`/`failover_query` arms).
  ///
  /// Being handed no state machine does not bound what the closure TOUCHED. A query closure is
  /// `Send + 'static` and captures whatever it likes; [`StateMachine`] imposes no ownership or
  /// isolation constraint, so a guard captured for the MISSING group can alias state a HOSTED group's
  /// replicated FSM shares — tear it in `Drop`, and panic there, inside the completion's `catch_unwind`.
  /// A container cannot see what a closure captured, so the tear could be in ANY hosted group: there is
  /// no group to name, and naming none leaves a torn group serving.
  ///
  /// So consensus safety outranks availability and the whole PLANE fail-stops. Each group poisons on
  /// its own account and surfaces through [`poll_poisoned`](Self::poll_poisoned), so the embedder learns
  /// what happened to every one of them; each fails its parked work with the typed verdict, and recovery
  /// runs from durable state after a restart. A plane whose groups all fail-stop is recoverable; a group
  /// serving divergent committed state is not. The trigger is a panicking `Drop` — already an
  /// abort-level Rust anti-pattern — so a closure that honors the `query` contract never pays this.
  pub fn fail_stop_plane_unattributable_panic(&mut self) {
    for (gid, ep) in &mut self.groups {
      ep.fail_stop_query_panicked();
      // `note_if_poisoned`, inlined: the endpoint is already in hand, and the `groups` borrow this
      // walk holds is exactly what would forbid the helper's second lookup.
      if ep.is_poisoned() && self.poisoned_seen.insert(gid.cheap_clone()) {
        self.poisoned_pending.push_back(gid.cheap_clone());
      }
    }
  }

  /// The group's lineage counter under the unified per-id scheme (incarnation ⊔ shape), as this
  /// container knows it: the LIVE endpoint counter when hosted (it includes every applied
  /// split), else the relay-time view (a removed id's last relayed bump). `0` for an id never
  /// admitted or reshaped.
  #[must_use]
  pub fn group_gen(&self, gid: &G) -> u64 {
    let live = self.groups.get(gid).map(Endpoint::shape_gen).unwrap_or(0);
    let relayed = self.lineage.get(gid).copied().unwrap_or(0);
    live.max(relayed)
  }

  /// Whether a split IN FLIGHT on this host reserves `gid` as its child id: some hosted group
  /// has a proposed-but-unapplied split naming it (the leader's propose→apply window), or a
  /// committed fork naming it staged in the relay queue — parked and held conflicts included.
  /// This fences the OTHER admission doors: the coordinators refuse embedder create/restore of a
  /// reserved id and the drivers' factory pre-build gate declines it, closing the window the
  /// propose-time `ChildExists` check cannot see. The FORK path does not consult it and must not —
  /// a committed fork's materialization is the split CLAIMING its own id, not another door asking
  /// for it, and the id is reserved by that very fork while it is staged. An id admitted anyway
  /// (before the split arrived) is the parked-conflict case
  /// [`peek_yieldable_fork`](Self::peek_yieldable_fork) holds safe.
  ///
  /// THE COVER IS CONTIGUOUS because the fork never leaves the container: the staged leg runs from
  /// the committed split right up to the pop inside
  /// [`install_yieldable_fork`](Self::install_yieldable_fork), and the pop is followed by the
  /// child's own admission with nothing in between. There is no yield window to reserve across, so
  /// there is no third leg — the reservation is derived from live consensus state alone.
  ///
  /// TWO COMMITTED FORKS naming one child need no fence between them either: the first installs,
  /// and the second finds the child hosted and parks on it under provenance — its token differs,
  /// so it is held, never discarded.
  ///
  /// Purely derived from live consensus state, so it releases by construction at every
  /// resolution: a stale-mint apply ends the propose window, a resolution arm consumes the staged
  /// fork, an install consumes it, and a park keeps it held until the conflict resolves.
  #[must_use]
  pub fn split_reserved(&self, gid: &G) -> bool {
    let mut bytes = Vec::new();
    gid.encode(&mut bytes);
    self.groups.values().any(|ep| ep.split_reserves(&bytes))
  }

  /// Whether `gid` holds a staged fork whose replay derivation a covering install already retired:
  /// the blob is the child's only local copy AND is process-lifetime-only — act before restarting
  /// this node.
  ///
  /// The observability seam for the held-fork durability window. A fork the relay HOLDS (its child
  /// id spoken for) stays queued, and a peer's covering snapshot install crosses this host's fences
  /// by doctrine: the restore has already discarded the split entry, so
  /// `Endpoint::note_fork_barrier_rebaselined` clears the queued fork's barrier while keeping the
  /// entry. What remains is an in-memory blob that does not survive a crash. Pair this with the
  /// [`poll_split_conflict`](Self::poll_split_conflict) cue: the cue says a fork is held, this says
  /// its last crash-surviving derivation is gone.
  ///
  /// DERIVED, zero new state: `fork_obligations_standing() && !fork_barrier_standing()`. The
  /// conjunction is sound because exactly ONE code path clears a still-QUEUED fork's barrier —
  /// `note_fork_barrier_rebaselined`, which retains the entry on purpose. Every other site that
  /// releases a barrier POPS the fork first (`resolve_fork` lifts the barrier for a fork already
  /// out of the queue), so it moves both predicates together and can never leave this gap open.
  /// The state is self-clearing: the hold's resolution consumes the fork and drops obligations, and
  /// a fresh capture re-arms the barrier.
  #[must_use]
  pub fn fork_derivation_volatile(&self, gid: &G) -> bool {
    self
      .groups
      .get(gid)
      .is_some_and(|ep| ep.fork_obligations_standing() && !ep.fork_barrier_standing())
  }
}

// The aggregate scheduling surface, split from the block above because `Endpoint::poll_timeout`
// carries the full node-id bound.
impl<G, I, F, R> MultiRaft<G, I, F, R>
where
  G: GroupId,
  I: NodeId,
  F: StateMachine,
{
  /// The earliest serviceable timer deadline across all groups, or `None` if no group has one.
  ///
  /// This is the pure-core convenience: an `O(N)` minimum. The Phase-3 reactor keeps an aggregate
  /// timing wheel over [`deadlines`](Self::deadlines) instead, waking only the due group.
  #[must_use]
  pub fn poll_timeout(&self) -> Option<Instant> {
    self
      .groups
      .values()
      .filter_map(Endpoint::poll_timeout)
      .min()
  }

  /// Each group's next serviceable deadline — the reactor's input for building its timing wheel.
  pub fn deadlines(&self) -> impl Iterator<Item = (G, Instant)> + '_ {
    self
      .groups
      .iter()
      .filter_map(|(gid, ep)| ep.poll_timeout().map(|d| (gid.cheap_clone(), d)))
  }

  /// The relay drain shared by [`peek_yieldable_fork`](Self::peek_yieldable_fork) and the install,
  /// over a caller-supplied [`ForkGate`] — the facts about the CALLER's storage that decide
  /// whether a fork can be installed at all.
  ///
  /// The gate is consulted at the would-be yield and its answers are mapped HERE: a child id
  /// below its admission floor (or at the reserved terminal) is a verdict about the fork, which
  /// no host state will change, so the fork is abandoned deliberately — popped, its barrier
  /// resolved, the refusal queued for [`poll_split_refusal`](Self::poll_split_refusal). A
  /// COMMITTED-CONSUMED child id is the same kind of verdict and takes the same exit: an
  /// outstanding capture debt names it, or a hosted park whose abort window latched CLOSED is
  /// absorbing it, and in either case the union subsumes whatever the fork carries. Occupied
  /// stores are not a verdict about anything: they say the id is spoken for, so the fork HOLDS,
  /// staged at the head with its blob, its fence, and its reservation intact, on the same
  /// machinery a hosted-child conflict parks on. The distinction is load-bearing — a consumed
  /// source's stores are RETAINED, so treating that as occupancy would hold the fork on the very
  /// state the absorb is waiting to release.
  ///
  /// A yieldable head fork is reported, never consumed: the install pops it, and only after every
  /// remaining check has passed. `Refused` outranks `Held` on the way out because the queue MOVED —
  /// a caller that stopped there would sit on a drain that has more to give.
  fn drain_to_yieldable(&mut self, gate: &impl ForkGate<G>) -> DrainOutcome<G, I> {
    let refusals_before = self.refusals.len();
    let mut held = false;
    // Parked parents first: their resolution triggers are child-side, so the dirty queue —
    // marked only by parent dispatches — cannot be relied on to revisit them. Skip the scan and
    // its allocation entirely when nothing is parked (the overwhelmingly common case).
    if !self.parked.is_empty() {
      let parked: Vec<G> = self.parked.keys().map(CheapClone::cheap_clone).collect();
      for gid in parked {
        match self.examine_head_fork(&gid, gate) {
          HeadFork::Empty => {
            self.unpark(&gid);
          }
          HeadFork::Resolved => {
            // Arm (b): the head fork resolved as redundant — later forks of this parent flow
            // through the ordinary drain below.
            self.unpark(&gid);
            if self.dirty_forks_set.insert(gid.cheap_clone()) {
              self.dirty_forks.push_back(gid);
            }
          }
          HeadFork::Parked => held = true,
          HeadFork::Yield(plan) => {
            // Arm (a): the squatter is gone and the fork is ready to install normally.
            self.unpark(&gid);
            if self.dirty_forks_set.insert(gid.cheap_clone()) {
              self.dirty_forks.push_back(gid.cheap_clone());
            }
            return DrainOutcome::Yieldable { parent: gid, plan };
          }
        }
      }
    }
    while let Some(gid) = self.dirty_forks.front().map(CheapClone::cheap_clone) {
      // A parked parent's queue is owned by the sweep above (head-of-line by design).
      if self.parked.contains_key(&gid) {
        self.dirty_forks.pop_front();
        self.dirty_forks_set.remove(&gid);
        continue;
      }
      match self.examine_head_fork(&gid, gate) {
        HeadFork::Empty => {
          self.dirty_forks.pop_front();
          self.dirty_forks_set.remove(&gid);
        }
        HeadFork::Parked => {
          held = true;
          self.dirty_forks.pop_front();
          self.dirty_forks_set.remove(&gid);
        }
        // Re-examine the same parent: its next staged fork (if any) is now at the head.
        HeadFork::Resolved => {}
        HeadFork::Yield(plan) => return DrainOutcome::Yieldable { parent: gid, plan },
      }
    }
    // Nothing to install. A refusal queued on the way outranks a park: the queue MOVED, so the
    // caller should come round again rather than stop.
    if self.refusals.len() != refusals_before {
      DrainOutcome::Refused
    } else if held {
      DrainOutcome::Held
    } else {
      DrainOutcome::Empty
    }
  }

  /// The install's shared front half — module-private because both terminal arms live in this
  /// module: decide, then make room. Everything up to — but not
  /// including — the pop lives here, because the pop is the point of no return and the two RNG
  /// arms must each own it.
  ///
  /// THE ORDERING IS LOAD-BEARING (and was the design's second blocking defect). Occupancy is a
  /// PEEK-TIME fact: the examine reads it from the engine BEFORE anything is created, because
  /// minting a boot epoch requires the child's storage to exist already, and re-reading occupancy
  /// afterwards would find the storage this very install just made and hold the fork against
  /// itself, forever. So: examine on the pristine engine, then `add_group` + `next_boot_epoch`
  /// only once the verdict is Yield, then stores, then the pre-checks. The install's own occupancy
  /// leg is `validate_virgin_stores` and nothing else.
  ///
  /// Every failure rolls back the storage it created; a caller can retry without accumulating
  /// half-made groups.
  fn prepare_fork_install<E, X>(
    &mut self,
    parent: &G,
    child: &G,
    engine: &mut E,
    extra: &X,
  ) -> PrepareOutcome<G, I>
  where
    E: MultiEngine<G, I>,
    X: ForkGate<G>,
    I: Data,
  {
    let plan = {
      let gate = EngineForkGate {
        engine: &*engine,
        extra,
        _node: core::marker::PhantomData,
      };
      // THE PAIR PIN, and it is why this examines ONE parent instead of re-running the drain. The
      // drain walks the parked sweep and then the dirty queue, and its own arms MUTATE that walk —
      // the sweep's yieldable arm unparks and re-dirties the parent it found before handing it
      // back. So a second drain starts from a different place than the first, and with two parks
      // releasing on one crank it legitimately reaches the OTHER parent: the peek advises
      // `(7, 200)` and a re-draining install for 200 answers `NotYieldable` on a perfectly legal
      // state. Examining the named parent directly removes the disagreement by construction — the
      // peek's effectful drain already consumed everything ahead of this head, so the named
      // parent's head IS the fork the peek described, and a head that changed underneath is
      // re-examined honestly here (a condemned or resolved head takes its ordinary arm; a head
      // that does not yield THIS child is not this call's business).
      match self.examine_head_fork(parent, &gate) {
        HeadFork::Yield(plan) => plan,
        // The named parent has nothing staged at all — the ordinary end of a driver's loop, and
        // what a second install for an already-consumed fork sees.
        HeadFork::Empty => return PrepareOutcome::Done(InstallOutcome::Empty),
        // Its head took a RESOLUTION arm instead of yielding: deliberately abandoned, or folded as
        // redundant. Unreachable in practice — the peek's own drain would have consumed it and
        // moved on rather than describing it — but the queue MOVED, so the honest answer is the
        // one that sends a draining caller round again.
        HeadFork::Resolved => return PrepareOutcome::Done(InstallOutcome::Refused),
        HeadFork::Parked => return PrepareOutcome::Done(InstallOutcome::Held),
      }
    };
    // The child leg of the same pin: nothing downstream would catch a mismatch, because virgin
    // stores are virgin whoever owns them, so an unpinned install would write a manufactured
    // baseline into another group's storage. The fork stays staged either way.
    if plan.child != *child {
      return PrepareOutcome::Done(InstallOutcome::NotYieldable);
    }
    let parent = parent.cheap_clone();
    let added = engine.add_group(child.cheap_clone());
    let Some(epoch) = engine.next_boot_epoch(child) else {
      if added {
        engine.remove_group(child);
      }
      return PrepareOutcome::Done(InstallOutcome::Held);
    };
    let config = reshape_born_prevention(plan.config.clone());
    let refusal = validate_fork_boot_epoch(epoch)
      .and_then(|()| validate_working_generation(plan.child_gen))
      .and_then(|()| validate_new_group(&self.groups, &self.host_id, child, &config))
      .and_then(|()| match engine.stores(child) {
        Some((log, stable)) => validate_virgin_stores(log, stable),
        None => Err(CreateGroupError::StorageInUse),
      });
    if let Err(e) = refusal {
      // EVERY refusal here HOLDS, and the important one is `StorageInUse`: a squatting incarnation
      // can be removed, so the fact can change — and abandoning on it would destroy the child
      // partition's only local copy, which is precisely the loss the hold arm exists to prevent.
      // The others are unreachable (the examine just decided this same state and said Yield), so
      // they are loud in a test run and conservative in release.
      debug_assert!(
        matches!(e, CreateGroupError::StorageInUse),
        "a fork install pre-check refused what the examine had just admitted: {e:?}"
      );
      if added {
        engine.remove_group(child);
      }
      return PrepareOutcome::Done(InstallOutcome::Held);
    }
    PrepareOutcome::Ready {
      parent,
      plan,
      epoch,
      config,
    }
  }

  /// LOOK at the head fork the relay would install right now, without consuming it.
  ///
  /// This is the whole relay drain — the driver runs it every crank, BEFORE its storage crank, so
  /// the same crank's engine flush covers the materialization. It decodes the typed child id,
  /// applies the replay guard, and rebuilds the child's config from the parent's local tuning under
  /// the fork's voter set. A fork folds to a RESOLVED no-op (its barrier contribution released,
  /// nothing to install) when: the bump is at-or-below the relay guard (a retry duplicate / an
  /// already-covered replay), or this host is not in the fork's voter set (a parent LEARNER applies
  /// the split — its parent half shrinks identically — but does not place the child; the embedder
  /// adds it by conf change later if wanted). A committed child id that does not decode as `G`
  /// poisons the parent (`SplitDecode` — committed-corrupt, the apply-arm's own decode class) and
  /// drops its remaining staged forks.
  ///
  /// A fork whose child id is ALREADY HOSTED here is PARKED, never dropped: the parent's
  /// `fsm.split` already ran at apply — the parent SHRANK — so the staged blob is the partition's
  /// only local copy, and the pre-park behavior (resolve as a no-op) silently lost it whenever the
  /// child was admitted between the propose-time `ChildExists` gate and this relay (the
  /// coordinators' reservation narrows that window, but a child admitted BEFORE the split applied
  /// remains reachable). Parked means: the fork stays queued at the head (its parent's later forks
  /// wait behind it — relaying past it would advance the replay guard over it and fold it to a
  /// duplicate), the relay guard does not advance, the snapshot fence does not lift, and one
  /// `(parent, child)` conflict signal surfaces via
  /// [`poll_split_conflict`](Self::poll_split_conflict). Every drain re-examines parked forks first
  /// and resolves by exactly one of: (a) the hosted child is REMOVED — the fork materializes
  /// normally; (b) the hosted child carries THIS split's exact [`ForkId`] — the provenance token a
  /// sibling replica's manufactured baseline (or the child's own later snapshot) installed here, so
  /// the child IS this fork already materialized: it resolves as redundant (fence lifts, guard
  /// advances, blob discarded — now safe); (c) the hosted child carries a DIFFERENT token or none —
  /// an independently-created group, a squatter, or a recreation that merely occupies the id — so
  /// it stays PARKED, however far its own commits pushed applied-index or lineage. The ForkId match
  /// is what makes the discard safe: progress alone cannot distinguish the real fork from an
  /// unrelated child, and treating a threshold crossing as proof of materialization discarded the
  /// fork blob for an unrelated child and lost the child partition. Parking is a conservative HOLD
  /// whose exits are (a) and (b): the conflict signal is the embedder's cue, and a genuinely-reshaped
  /// twin still resolves the moment its token arrives here (the token is fixed at the split, so
  /// later reshaping never changes it).
  ///
  /// WHAT THE FENCE ACTUALLY HOLDS, because the difference decides how long the embedder has. The
  /// standing fork barrier holds this host's OWN capture: while it stands, nothing local compacts
  /// past the split entry, so the fork's LOG replay derivation survives a restart. It does not hold
  /// a PEER. An install crosses every local fence by doctrine — `log.restore` has already discarded
  /// the split entry, so a barrier below the boundary protects nothing and could only wedge every
  /// later capture (`Endpoint::note_fork_barrier_rebaselined`, driven from the install path in
  /// `endpoint/snapshot.rs`) — and a covering install therefore clears the parked fork's barrier
  /// while KEEPING its queue entry. Past that point the staged blob is the child's only local
  /// derivation and is PROCESS-LIFETIME-ONLY: it survives any wait, but not a restart of this node.
  /// [`fork_derivation_volatile`](MultiRaft::fork_derivation_volatile) reports exactly that state.
  ///
  /// EVERY ONE OF THOSE ARMS RUNS HERE, because they are how the queue reaches a yieldable fork at
  /// all. The single thing this call does not do is the yield: the fork stays STAGED, with its
  /// blob, its durability barrier and its reservation intact, and what comes back is a borrowed
  /// [`ForkView`] — a decision to look at, not a capability to hold. Pair it with
  /// [`install_yieldable_fork`](MultiRaft::install_yieldable_fork), which re-decides from the same
  /// staged queue on the same crank. Nothing between the two can lose the partition, because the
  /// partition never moved.
  pub fn peek_yieldable_fork(&mut self, gate: &impl ForkGate<G>) -> Option<ForkView<'_, G, I>> {
    match self.drain_to_yieldable(gate) {
      DrainOutcome::Yieldable { parent, plan } => Some(ForkView {
        parent,
        plan,
        _borrow: core::marker::PhantomData,
      }),
      _ => None,
    }
  }

  /// Examine (and where possible resolve or plan the install of) `gid`'s HEAD staged fork — the
  /// one shared arm evaluation both relay-drain phases run. The head fork is consumed only on a
  /// resolution; a park and a yieldable verdict both leave it staged.
  fn examine_head_fork(&mut self, gid: &G, gate: &impl ForkGate<G>) -> HeadFork<G, I> {
    // TAKE BEFORE EXAMINE. A fork a removal condemned below the head is consumed the moment it
    // reaches the head, ahead of any verdict: it was already judged, by the teardown of the very
    // incarnation it produced, and the gate can only re-decide it wrongly — the id it names is
    // free again, so every arm here would let it land.
    if self
      .groups
      .get(gid)
      .is_some_and(Endpoint::head_fork_is_abandoned)
    {
      return self.consume_abandoned_head_fork(gid);
    }
    enum Verdict<G> {
      Poison,
      /// Resolve the head fork's barrier and consume it (duplicate / non-member arms).
      Resolve,
      /// Park on a hosted-child conflict (arm (c)), surfacing the signal on a fresh park.
      Park(G),
      /// Resolve as redundant — the hosted twin provably carries the fork data (arm (b)).
      Redundant,
      /// No conflict: advance the guard and yield (the config rebuild may still refuse).
      Yield(G),
      /// The id is SPOKEN FOR — occupied caller storage, or container state no install can pass
      /// through. Park on the second cause; the fork stays staged.
      Hold(G),
      /// A verdict about the FORK, true here forever: abandon it deliberately.
      Terminal(G),
    }
    let verdict = {
      let Some(ep) = self.groups.get(gid) else {
        return HeadFork::Empty;
      };
      let Some(fork) = ep.peek_pending_fork() else {
        return HeadFork::Empty;
      };
      if let Ok(child) = G::decode_exact(fork.child_bytes.clone()) {
        let in_voters = self
          .host_id
          .as_ref()
          .is_some_and(|host| fork.voters.contains(host));
        if !in_voters || fork.parent_gen_after <= self.lineage.get(gid).copied().unwrap_or(0) {
          Verdict::Resolve
        } else if let Some(hosted) = self.groups.get(&child) {
          // Arm (b) is a PROVENANCE decision, not a progress one: the hosted child IS this fork
          // materialized iff it carries THIS split's exact ForkId — the token a sibling replica's
          // manufactured baseline (or the child's own later snapshot) transferred here. Only a
          // child that installed this fork's baseline has it; an independently-created group, a
          // squatter, or a recreation at the id carries a DIFFERENT token or none, however far its
          // own commits pushed applied-index or lineage. A match resolves redundant (the fork
          // already exists in the id's history — fence lifts, blob safely discarded); anything else
          // PARKS (arm (c)): the staged blob is the child partition's only local copy, so the
          // standing fence holds it until the conflict resolves. Progress alone can never authorize
          // the discard — that mistaking an unrelated child for the fork is the data-loss defect
          // this closes.
          let this_fork_id = mint_fork_id(
            gid,
            fork.parent_gen_after,
            fork.index,
            fork.split_term,
            fork.child_bytes.clone(),
            fork.child_gen,
          );
          if hosted.fork_id() == Some(this_fork_id) {
            Verdict::Redundant
          } else {
            Verdict::Park(child)
          }
        } else {
          // ARM ORDER IS AN INVARIANT. The gate is consulted only HERE, at the would-be yield,
          // strictly after the hosted-child branch above. A hosted child always has engine
          // stores, so asking the gate first would swallow every park into a stores-hold and
          // make the ForkId redundant exit above unreachable — a legitimately-arrived twin would
          // wedge its parent forever.
          //
          // The verdict split is not about which refusals we happened to enumerate. A refusal
          // about the FORK — a generation the child can never clear, an id that cannot encode, a
          // config no host would accept — is true here forever, so abandoning is the honest
          // answer. Everything else says only that the id is momentarily spoken for, and the
          // fork is the partition's only local copy: it HOLDS. `CreateGroupError` is
          // `#[non_exhaustive]`, so the compiler itself demands the default arm below, and the
          // default is HOLD — enumeration is the fast path, never the safety boundary.
          //
          // The split RESERVATION needs no check on either side. It fences the other admission
          // doors, and the fork path does not consult it — so there is nothing here to refuse
          // against, and nothing to hold for: a sibling fork naming this same child resolves by
          // install-then-park, not by fencing.
          // Checking it here would block the fork on itself.
          let floor = gate.floor(&child);
          match ep.config().with_voter_set(fork.voters.clone()) {
            // The child's boot config is rebuilt from a voter set FIXED at the split, so a
            // config the ordinary admission validation refuses is refused here forever (A8).
            Err(_) => Verdict::Terminal(child),
            Ok(child_config) => {
              let refusal = validate_working_generation(fork.child_gen)
                .and_then(|()| {
                  // The reserved-terminal leg of `floor_admits` is already spent: the working-
                  // generation check above refused it, so a failure here is the floor itself.
                  if floor_admits(floor, fork.child_gen) {
                    Ok(())
                  } else {
                    Err(CreateGroupError::BelowFloor { floor })
                  }
                })
                .and_then(|()| {
                  validate_new_group(&self.groups, &self.host_id, &child, &child_config)
                });
              match refusal {
                // ONE PREDICATE, ONE VERDICT: the absorb refusal is `validate_new_group`'s alone.
                // The occupancy guard below deliberately no longer re-tests it — two copies of one
                // condition reaching opposite verdicts is how the id ended up held forever.
                Ok(()) if gate.contains_group(&child) => Verdict::Hold(child),
                Ok(()) => Verdict::Yield(child),
                // ABANDONMENT NEEDS PERMANENCE, not merely a refusal. A floor alone does not
                // authorize destroying fork content; a MONOTONE floor does, because it is the only
                // refusal class that provably can never lift at this host. Every refusal that CAN
                // lift — occupied stores, a tombstone, a config the caller may fix — HOLDS. These
                // four cannot: a generation is fixed at the split, an id that does not encode never
                // will, and a node-id mismatch is about this host, not this moment.
                //
                // Evidence note for anyone extending this list: a green VOPR is NOT evidence for
                // the below-floor abandonment. A wholly-refused child never registers a
                // `SplitRecord`, so the conservation walk never judges the pair — the transition
                // pin (`a_held_fork_terminalizes_when_its_floor_rises`) and the deliberate-abandon
                // unit are the evidence that exists.
                Err(
                  CreateGroupError::ReservedGeneration
                  | CreateGroupError::BelowFloor { .. }
                  | CreateGroupError::InvalidGroupId
                  | CreateGroupError::NodeIdMismatch,
                ) => Verdict::Terminal(child),
                // A COMMITTED-CONSUMED child id — but abandoning a fork DESTROYS its blob, so the
                // verdict is Terminal only where the loss is provably nothing. The subsumption
                // argument is narrow: it holds when THIS parent is itself the absorbing target and
                // the fork was staged BELOW the park coordinate, because the park stops this
                // endpoint's apply drain at `k - 1`, so such a fork predates the freeze and the
                // absorbed union contains its half. Both facts live on the endpoint being examined
                // — no cross-endpoint reasoning, and nothing here consults another group's state.
                //
                // For an UNRELATED parent whose committed split merely names the same id, the
                // union cannot contain that child-half at all: Terminal there is data loss. Those
                // HOLD, on the fail-safe default — an undecidable-locally fork wedges
                // diagnosably and never drops.
                //
                // The refusal must stay an Err-arm verdict either way: routing it through the `Ok`
                // guard would let the consumed source's own RETAINED STORES re-assert occupancy,
                // which is the ring this closes — a held fork's barrier suppresses the cure
                // advertisement that delivers the covering snapshot, so nothing folds, no floor is
                // written, and the stores are never released.
                //
                // ASYMMETRY WITH THE ADMISSION DOORS, deliberate: `validate_new_group` refuses this
                // id outright for every door, because a door holds no blob — refusing there loses
                // nothing and is the safe strengthening. This arm holds the partition's only
                // local copy, so its scope is exactly the subsumption proof.
                Err(CreateGroupError::AbsorbPending) => {
                  let park = ep.pending_merge();
                  let names_this_child = park.is_some_and(|p| {
                    p.window_closed() && p.source_bytes().as_ref() == fork.child_bytes.as_ref()
                  });
                  let below_the_park = park.is_some_and(|p| fork.index < p.at());
                  // AT-or-ABOVE is UNREACHABLE, and the reason is the same drain-stop the
                  // subsumption rests on: the park holds this endpoint's apply at `k - 1`, and a
                  // fork is staged only when its `Split` entry APPLIES, so no split at or past `k`
                  // can have staged while this park stands — on the live path or on a restore
                  // replay, which parks at the same coordinate and stops there too. Asserted rather
                  // than tested because the state cannot be constructed through any public door.
                  debug_assert!(
                    !names_this_child || below_the_park,
                    "a fork staged at or past its own target's park coordinate"
                  );
                  let subsumed = names_this_child && below_the_park;
                  if subsumed {
                    Verdict::Terminal(child)
                  } else {
                    Verdict::Hold(child)
                  }
                }
                Err(_) => Verdict::Hold(child),
              }
            }
          }
        }
      } else {
        Verdict::Poison
      }
    };
    match verdict {
      Verdict::Poison => {
        if let Some(ep) = self.groups.get_mut(gid) {
          ep.poison(PoisonReason::SplitDecode);
          while ep.pop_pending_fork().is_some() {}
        }
        self.note_if_poisoned(gid);
        HeadFork::Empty
      }
      Verdict::Resolve => {
        if let Some(ep) = self.groups.get_mut(gid)
          && let Some((fork, _fsm)) = ep.pop_pending_fork()
        {
          ep.resolve_fork(fork.index);
        }
        HeadFork::Resolved
      }
      Verdict::Redundant => {
        // REDUNDANT IS A CONSUMPTION, and every consumption that moves the guard persists its
        // advance. The blob is discarded here because the child already carries this split's
        // baseline; if only the volatile guard moved, a crash before a parent snapshot covered the
        // split would replay the fork against a stale durable guard, and after the embedder removed
        // the child and consented to its re-admission the fork would install that very baseline
        // again. The queued advance is what makes the discard survive the restart that follows it.
        let consumed = self.groups.get_mut(gid).and_then(|ep| {
          let (fork, _fsm) = ep.pop_pending_fork()?;
          ep.resolve_fork(fork.index);
          Some(fork.parent_gen_after)
        });
        if let Some(parent_gen_after) = consumed {
          self.advance_relay_guard(gid, parent_gen_after);
        }
        HeadFork::Resolved
      }
      Verdict::Park(child) => {
        // Map-insert is the dedupe: one conflict signal per park episode, re-armed only after
        // a resolution removes the parent from the parked set.
        if self
          .parked
          .insert(gid.cheap_clone(), ParkCause::HostedChild)
          .is_none()
        {
          self.conflicts.push_back((gid.cheap_clone(), child));
        }
        HeadFork::Parked
      }
      Verdict::Hold(child) => {
        // The SECOND CAUSE on the one mechanism: the fork stays staged at the head, so its blob
        // survives, its barrier stays outstanding, its child id stays reserved, and this
        // parent's later forks stay structurally behind it. The parked sweep re-examines it
        // every drain — the resolution triggers are all caller-side or child-side, so nothing
        // re-marks this parent as dirty.
        if self
          .parked
          .insert(gid.cheap_clone(), ParkCause::Blocked)
          .is_none()
        {
          self.conflicts.push_back((gid.cheap_clone(), child));
        }
        HeadFork::Parked
      }
      Verdict::Terminal(child) => {
        // DELIBERATE abandonment: the blob is dropped knowingly, which is only ever sound for a
        // verdict about the fork itself. The barrier resolves — the parent must not stay fenced
        // for a fork that can never land here — and the refusal is queued for the embedder.
        if let Some(ep) = self.groups.get_mut(gid)
          && let Some((fork, _fsm)) = ep.pop_pending_fork()
        {
          ep.resolve_fork(fork.index);
        }
        self.refusals.push_back((gid.cheap_clone(), child));
        HeadFork::Resolved
      }
      Verdict::Yield(child) => {
        // THE YIELD ARM CONSUMES NOTHING. It decides, and the decision is all that travels: the
        // fork stays STAGED at the head with its blob, its barrier and its reservation, and the
        // install pops it only once every check has passed. Everything a caller could act on is
        // derived here so the install can re-decide identically a moment later, on the same crank.
        //
        // Rebuild the child's boot config: the parent's local tuning under the fork's voter set.
        // The voter-membership check above makes `IdNotAVoter` unreachable; the arm is defensive
        // (resolve rather than wedge the queue).
        let config = self.groups.get(gid).and_then(|ep| {
          let voters = ep.peek_pending_fork()?.voters.clone();
          ep.config().with_voter_set(voters).ok()
        });
        let Some(plan) = self.groups.get(gid).and_then(|ep| {
          let fork = ep.peek_pending_fork()?;
          Some(YieldPlan {
            child: child.cheap_clone(),
            child_gen: fork.child_gen,
            parent_gen_after: fork.parent_gen_after,
            split_index: fork.index,
            split_term: fork.split_term,
            child_bytes: fork.child_bytes.clone(),
            config: config.clone()?,
          })
        }) else {
          // The config rebuild failed — unreachable, since the verdict above rebuilt this same
          // config and turned a failure into `Terminal`. Kept as the defensive resolve it always
          // was: consume the fork rather than wedge the queue, and tell the embedder. It is a
          // consumption that moves the guard, so it persists the advance like every other one.
          let Some(ep) = self.groups.get_mut(gid) else {
            return HeadFork::Empty;
          };
          let Some((fork, _fsm)) = ep.pop_pending_fork() else {
            return HeadFork::Empty;
          };
          ep.resolve_fork(fork.index);
          let parent_gen_after = fork.parent_gen_after;
          self.advance_relay_guard(gid, parent_gen_after);
          self.refusals.push_back((gid.cheap_clone(), child));
          return HeadFork::Resolved;
        };
        HeadFork::Yield(plan)
      }
    }
  }

  /// The next `(parent, child)` SPLIT-CONFLICT signal, left queued — the DELIVERED-BEFORE-
  /// CONSUMED half for a driver publishing on a bounded tail: peek, publish, and only on
  /// acceptance consume via [`poll_split_conflict`](Self::poll_split_conflict). The signal is
  /// one-shot per park episode, so consuming it ahead of a refusable send would let a
  /// momentarily-full tail erase the embedder's only cue while the fence stands and the child
  /// id stays reserved; peeking leaves it queued for the next drain instead.
  #[must_use]
  pub fn peek_split_conflict(&self) -> Option<(G, G)> {
    self
      .conflicts
      .front()
      .map(|(parent, child)| (parent.cheap_clone(), child.cheap_clone()))
  }

  /// Drain the next `(parent, child)` SPLIT-CONFLICT signal: a committed fork PARKED because
  /// its child id is already hosted here (see
  /// [`peek_yieldable_fork`](Self::peek_yieldable_fork)).
  /// One signal per park episode, held until consumed HERE. A synchronous embedder (the sim
  /// worlds) consumes directly — its consumption IS delivery; a driver publishing on a BOUNDED
  /// tail must [`peek_split_conflict`](Self::peek_split_conflict) first and consume only after
  /// the tail accepts, so backpressure defers the one-shot cue rather than erasing it. A park
  /// that resolves before consumption purges its queued signal (a later delivery would be
  /// stale); the parked fork itself, not this signal, is the load-bearing state. The embedder
  /// resolves by removing the hosted child (the fork then materializes) or by letting the twin
  /// catch up (the fork then resolves as redundant); until then the parent's snapshot fence
  /// holds its replay source.
  pub fn poll_split_conflict(&mut self) -> Option<(G, G)> {
    self.conflicts.pop_front()
  }

  /// Whether `gid`'s fork relay still owes something: its head fork is PARKED (held), or a
  /// conflict/refusal signal naming it is queued and no driver has taken it yet.
  ///
  /// A driver reads this as a liveness obligation. A parent in this state must not be allowed to
  /// go quiet — the doctrine a merge park already follows: the cue is the embedder's only prompt
  /// to clear a standing hold, and a quiesced parent with its cue still queued would wait for
  /// unrelated traffic to deliver it.
  #[must_use]
  pub fn fork_relay_pending(&self, gid: &G) -> bool {
    self.parked.contains_key(gid)
      || self.conflicts.iter().any(|(parent, _)| parent == gid)
      || self.refusals.iter().any(|(parent, _)| parent == gid)
  }

  /// The next `(parent, child)` SPLIT-REFUSAL signal, left queued — the delivered-before-consumed
  /// half, exactly as [`peek_split_conflict`](Self::peek_split_conflict). A refusal says the
  /// relay ABANDONED a committed fork because no host state could ever let it land: the blob is
  /// gone and the parent's fence is released, so this is the embedder's only record that a
  /// child it was promised will never arrive by this route.
  #[must_use]
  pub fn peek_split_refusal(&self) -> Option<(G, G)> {
    self
      .refusals
      .front()
      .map(|(parent, child)| (parent.cheap_clone(), child.cheap_clone()))
  }

  /// Drain the next `(parent, child)` SPLIT-REFUSAL signal. Unlike a park's conflict cue this is
  /// never purged: the abandonment already happened, so the news stays owed however the rest of
  /// the id's story turns out.
  pub fn poll_split_refusal(&mut self) -> Option<(G, G)> {
    self.refusals.pop_front()
  }

  /// Drain the next `(parent, lineage)` relay-guard advance owed to the caller's DURABLE lineage
  /// record. Only the removal-time fork abandonment queues one: its guard bump is the sole advance
  /// with no durable write of its own to ride, and losing it to a crash would let the parent's
  /// replayed split re-stage the abandoned fork against the very clean slate the removal made.
  /// Drain it beside the removal's floor write, under the same barrier.
  pub fn poll_relay_guard_advance(&mut self) -> Option<(G, u64)> {
    self.guard_advances.pop_front()
  }

  /// The next group whose endpoint FAIL-STOPPED — a storage/apply fault poisoned it — reported once
  /// per poisoning per hosted incarnation. An OBSERVATION riding a best-effort tail, not a command:
  /// the poisoned endpoint stays hosted with its consensus frozen and its parked work failing typed
  /// verdicts, and NOTHING is torn down — the embedder decides whether to inspect, remove, or
  /// replace the group. A removal purges any still-queued signal, so a delivered id always names a
  /// currently poisoned, hosted group; a re-admitted id that poisons again signals afresh.
  pub fn poll_poisoned(&mut self) -> Option<G> {
    self.poisoned_pending.pop_front()
  }

  /// The next merge held by a STRUCTURAL cause on this host, reported ONCE PER TRANSITION of a
  /// target's cause rather than once per crank — see [`MergeBlocked`].
  ///
  /// An OBSERVATION on a best-effort tail: the hold's resolution is driven by
  /// [`service_merge_applies`](Self::service_merge_applies) re-deriving it every crank, never by
  /// this queue, so a dropped signal costs a notification and nothing else. What the embedder does
  /// with it is cause-specific and always OUT of the consensus path: give an unhosted source a
  /// host, resolve a split conflict, remove a group whose thaw will never come. Doing nothing is
  /// also a valid answer for the causes that lift on their own.
  pub fn poll_merge_blocked(&mut self) -> Option<MergeBlocked<G>> {
    self.merge_blocked.pop_front()
  }

  /// The queue's head WITHOUT consuming it — the drivers' delivered-before-consumed read. The
  /// hold itself is re-derived every crank, but the once-per-TRANSITION dedupe is not: a
  /// signal popped ahead of a full lifecycle tail would never re-enqueue while its cause
  /// stands, silencing a hold that needs embedder action. Peek, publish, and consume only on
  /// acceptance.
  pub fn peek_merge_blocked(&self) -> Option<MergeBlocked<G>> {
    self.merge_blocked.front().cloned()
  }

  /// Queue one [`MergeBlocked`] iff `cause` DIFFERS from the last one signalled for `target`. The
  /// container re-derives every hold on every crank, so without this edge the queue would grow by
  /// one entry per crank for as long as the hold stands; with it, a stable hold costs exactly one
  /// signal and a hold that changes shape costs one more.
  fn note_merge_blocked(
    &mut self,
    target: &G,
    source: &G,
    boundary: Index,
    cause: MergeBlockedCause,
  ) {
    // The FULL observation identity dedups — cause, named source, and boundary — never the
    // cause alone: with hosted crossings A then B, removing A must let B's observation through,
    // or the placement layer holds a wedge with no actionable identity for it.
    let observation = (cause, source.cheap_clone(), boundary);
    self.merge_blocked_attempts.insert(
      target.cheap_clone(),
      (cause, source.cheap_clone(), boundary),
    );
    if self.merge_blocked_seen.get(target) == Some(&observation) {
      return;
    }
    self
      .merge_blocked_seen
      .insert(target.cheap_clone(), observation);
    // A superseded observation still QUEUED (a full lifecycle tail held it undelivered) is
    // replaced, not delivered first: the embedder would act on the stale identity — the old
    // crossing, the old cause — before ever seeing the current one.
    self.merge_blocked.retain(|b| &b.target != target);
    self.merge_blocked.push_back(MergeBlocked {
      target: target.cheap_clone(),
      source: source.cheap_clone(),
      boundary,
      cause,
    });
  }

  /// Resolve the fork staged at exactly `split_index` on `parent`: the driver reports the
  /// child's baseline flush-durable behind its engine barrier (or a relayed fork it abandoned),
  /// and the parent's snapshot fence over that index releases. Exact-index semantics — see
  /// [`ForkView::split_index`]; resolving one fork never frees an older, still-pending one.
  pub fn lift_fork_barrier(&mut self, parent: &G, split_index: Index) {
    if let Some(ep) = self.groups.get_mut(parent) {
      ep.resolve_fork(split_index);
    }
  }

  /// Raise `gid`'s relay-time replay guard to at least `lineage` — the restore arms' seam for
  /// the DURABLE lineage record the host's engine keeps beside the group's stores. The restore
  /// constructors seed the guard from the restored snapshot meta alone, but that meta can LAG
  /// what this host already materialized: a driver flushes the parent's lineage record together
  /// with each fork's child baseline (one barrier), while the parent's next snapshot — the only
  /// thing that folds the bump into the meta — may never have happened before the crash. A
  /// parent restored in that window replays its split entries and re-stages their forks; under
  /// the meta-only seed the container would relay them again, and materializing one against an
  /// unhosted child would aim a manufactured baseline at the child's REAL durable progress.
  /// Feeding the engine's record here folds those already-durable forks to resolved no-ops
  /// instead. Monotone (never lowers) and a no-op for an unhosted `gid`, so a lineage-less
  /// floor store leaves the snapshot-seeded guard exactly as it was.
  pub fn raise_relay_guard(&mut self, gid: &G, lineage: u64) {
    if !self.groups.contains_key(gid) {
      return;
    }
    if let Some(guard) = self.lineage.get_mut(gid) {
      *guard = (*guard).max(lineage);
    }
  }
}

// Default-`Prng` constructors, mirroring `Endpoint::new`/`restart` (which are `Prng`-only; the
// generic-RNG entry points live on `Endpoint`'s and this type's `*_with_rng` family).
impl<G, I, F> MultiRaft<G, I, F, Prng>
where
  G: GroupId,
  I: NodeId,
  F: StateMachine,
{
  /// Create a fresh group (Follower, term 0, empty log view). The group's election RNG is seeded by
  /// `seed` folded with `gid`, so co-located groups draw decorrelated election-timeout jitter
  /// (identical jitter would correlate their elections into a host-wide storm).
  ///
  /// `generation` is the id's ADMITTED incarnation under the unified lineage counter (0 at
  /// genesis; a floor-validated recreate passes the same value its coordinator admission checked
  /// and its driver records in the engine). It SEEDS the endpoint's lineage counter, so every
  /// generation this incarnation ever mints (splits, merge freezes/thaws/absorbs) lies strictly
  /// above its floor — a recreate can never repeat a predecessor's generations, which is what
  /// binds gen-keyed state (a stale merge-abort obligation above all) to exactly one incarnation.
  ///
  /// # Errors
  /// [`CreateGroupError::Exists`] if a group with `gid` is already hosted,
  /// [`CreateGroupError::NodeIdMismatch`] if `config`'s id differs from the hosted groups' shared
  /// node id, and [`CreateGroupError::InvalidGroupId`] if `gid`'s encoding is outside the wire
  /// bound (1..=1024 bytes). Hosted groups are untouched in every case; on `Err` the moved-in
  /// `fsm` is dropped — pre-check [`contains_group`](Self::contains_group) to preserve it.
  pub fn create_group(
    &mut self,
    gid: G,
    generation: u64,
    config: Config<I>,
    now: impl Into<Now>,
    seed: u64,
    fsm: F,
  ) -> Result<(), CreateGroupError> {
    validate_new_group(&self.groups, &self.host_id, &gid, &config)?;
    validate_working_generation(generation)?;
    self.host_id.get_or_insert(config.id());
    let mut ep = Endpoint::new(config, now, group_seed(seed, &gid), fsm);
    ep.seed_lineage(generation);
    // Every admission reseeds the relay-time lineage view (a stale entry from an earlier
    // same-uptime incarnation must not shadow this admission) — at the ADMITTED generation,
    // exactly where the endpoint's own counter starts.
    self.lineage.insert(gid.cheap_clone(), generation);
    self.clear_unresolvable_hints_naming(&gid);
    self.groups.insert(gid, ep);
    Ok(())
  }

  /// Recover a group from durable storage, replaying its committed tail into the state machine.
  /// Replay surfaces NO events (mirroring [`Endpoint::restart`], which deliberately clears them —
  /// replay is not new work); the restore MAY leave one pending stable write (a grown lease
  /// floor), drained by the driver's normal `handle_storage` cadence. Same `gid`-folded seeding as
  /// [`create_group`](Self::create_group).
  ///
  /// # Errors
  /// The same admission checks as [`create_group`](Self::create_group) — see
  /// [`CreateGroupError`]. Refusal happens BEFORE any store is read.
  #[allow(clippy::too_many_arguments)]
  pub fn restore_group<L, S>(
    &mut self,
    gid: G,
    config: Config<I>,
    now: impl Into<Now>,
    seed: u64,
    fsm: F,
    boot_epoch: u64,
    log: &mut L,
    stable: &mut S,
  ) -> Result<(), CreateGroupError>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
    F::Command: Data,
    F::Snapshot: Data,
    F::Error: core::error::Error,
    I: Data,
  {
    validate_new_group(&self.groups, &self.host_id, &gid, &config)?;
    self.host_id.get_or_insert(config.id());
    let ep = Endpoint::restart(
      config,
      now,
      group_seed(seed, &gid),
      fsm,
      boot_epoch,
      log,
      stable,
    );
    // Seed the relay guard from the DURABLE lineage (the restored snapshot meta's `shape_gen`),
    // NOT the live counter: the restart replay may have re-staged a not-yet-materialized fork,
    // re-bumping the live counter — the guard must relay that fork again, not drop it.
    self
      .lineage
      .insert(gid.cheap_clone(), ep.restored_lineage());
    self.clear_unresolvable_hints_naming(&gid);
    self.groups.insert(gid.cheap_clone(), ep);
    // The dirty marks cover the replayed forks (message/event replay is deliberately cleared by
    // `Endpoint::restart`, so those two queues mark empty).
    self.mark_dirty(&gid);
    Ok(())
  }

  /// Recover a group from a pre-format store, wrapping [`Endpoint::restart_migrating`] — the
  /// ONE-TIME upgrade path for a node that persisted no lease-support floor. See that method for
  /// the `assume_prior_lease_support` contract.
  ///
  /// # Errors
  /// The same admission checks as [`create_group`](Self::create_group) — see
  /// [`CreateGroupError`]. Refusal happens BEFORE any store is read.
  #[allow(clippy::too_many_arguments)]
  pub fn restore_group_migrating<L, S>(
    &mut self,
    gid: G,
    config: Config<I>,
    now: impl Into<Now>,
    seed: u64,
    fsm: F,
    boot_epoch: u64,
    assume_prior_lease_support: Option<Duration>,
    log: &mut L,
    stable: &mut S,
  ) -> Result<(), CreateGroupError>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
    F::Command: Data,
    F::Snapshot: Data,
    F::Error: core::error::Error,
    I: Data,
  {
    validate_new_group(&self.groups, &self.host_id, &gid, &config)?;
    self.host_id.get_or_insert(config.id());
    let ep = Endpoint::restart_migrating(
      config,
      now,
      group_seed(seed, &gid),
      fsm,
      boot_epoch,
      assume_prior_lease_support,
      log,
      stable,
    );
    self
      .lineage
      .insert(gid.cheap_clone(), ep.restored_lineage());
    self.clear_unresolvable_hints_naming(&gid);
    self.groups.insert(gid.cheap_clone(), ep);
    self.mark_dirty(&gid);
    Ok(())
  }

  /// Create a group born from LOCALLY-FORKED state: a manufactured snapshot install. The
  /// baseline — meta `(`[`FORK_BASE_INDEX`]`, `[`FORK_BASE_TERM`]`)`, the caller's `snapshot`
  /// blob, the log compacted through the boundary — is written into the FRESH `log`/`stable`
  /// first, and the group then boots through the [`Endpoint::restart`] path, inheriting its
  /// boundary validation, poison discipline, and applied/commit derivation wholesale. Because
  /// `first_index` starts at 2, a zero-progress joiner added later is structurally forced onto
  /// the snapshot path and receives exactly the persisted blob — never a log walk onto its empty
  /// state machine.
  ///
  /// **The blob is authoritative.** `fsm` is the restore VESSEL — restart overwrites it from the
  /// blob, so a caller-supplied fsm/blob mismatch cannot diverge replicas (the blob wins
  /// identically everywhere; their equality is an efficiency contract, not a safety one). A blob
  /// that fails boundary/decode/restore POISONS the group exactly as a corrupt durable snapshot
  /// would at crash-restart — construction still returns `Ok`, and co-hosted groups are
  /// untouched.
  ///
  /// A fork is a LOCAL act by an already-authorized replica: it is never solicited over the
  /// wire, and no factory path reaches it — a catalog that marks ids fork-born should DECLINE
  /// solicitations for them. Same `boot_epoch` contract as
  /// [`restore_group`](Self::restore_group) (strictly above every prior incarnation of this
  /// group on this node — a re-fork after removal is a later incarnation), with its floor
  /// ENFORCED here: `boot_epoch == 0` is refused, because the manufactured baseline's store
  /// writes ride the prior epoch and epoch 0 has none — the baseline's completions would land
  /// in the child's own first live epoch and could release a vote/campaign action they do not
  /// prove durable. The stores must be VIRGIN, and that too is ENFORCED
  /// ([`CreateGroupError::StorageInUse`]): the baseline overwrites whatever the stores hold, so
  /// a fork over a used incarnation's storage — a replayed split racing the child's real
  /// durable progress after a parent-only restore — would destroy that progress; only the
  /// crash-before-flush replay, whose stores hold nothing durable, may re-fork (idempotently).
  /// On a stable store whose `hard_state()` lags submitted writes to a durability barrier, the
  /// child boots at the store's PRIOR durable term (the baseline meta alone drives the
  /// applied/commit derivation, so the boot is unchanged otherwise) and the manufactured term
  /// becomes durable at the next barrier — the crash-recovery shape is the spec'd one either
  /// way.
  ///
  /// # Errors
  /// The same admission checks as [`create_group`](Self::create_group) — see
  /// [`CreateGroupError`] — plus [`CreateGroupError::InvalidBootEpoch`] when `boot_epoch == 0`
  /// (a fork's baseline needs the prior epoch to itself) and
  /// [`CreateGroupError::StorageInUse`] when the stores already hold state (a fork never
  /// overwrites used storage). Refusal happens BEFORE any store write.
  ///
  /// # The reservation fence
  ///
  /// THIS IS THE PUBLIC, CALLER-DRIVEN DOOR: the caller chooses the id, the blob and the token, so
  /// an id an in-flight or staged split owns is refused here ([`CreateGroupError::SplitReserved`]),
  /// in BOTH windows — between propose and apply, and while the committed fork is staged. Without
  /// it a caller could install a group of its own making at a reserved child id; the genuine fork
  /// then finds the id hosted, and resolves REDUNDANT against a token the caller supplied, which
  /// discards the child partition's only local copy. The relay's own materialization is not this
  /// door at all — it never leaves the container: the pair
  /// [`peek_yieldable_fork`](Self::peek_yieldable_fork) /
  /// [`install_yieldable_fork`](Self::install_yieldable_fork) installs the child in place, from the
  /// staged queue, and so has nothing to reserve against.
  #[allow(clippy::too_many_arguments)]
  pub fn create_group_from_fork<L, S>(
    &mut self,
    gid: G,
    generation: u64,
    config: Config<I>,
    now: impl Into<Now>,
    seed: u64,
    fsm: F,
    snapshot: Bytes,
    read_only: Option<ReadOnlyOption>,
    boot_epoch: u64,
    log: &mut L,
    stable: &mut S,
  ) -> Result<(), CreateGroupError>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
    F::Command: Data,
    F::Snapshot: Data,
    F::Error: core::error::Error,
    I: Data,
  {
    if self.split_reserved(&gid) {
      return Err(CreateGroupError::SplitReserved);
    }
    // NO CALLER PROVENANCE. This door installs TOKEN-LESS, always: a [`ForkId`] is mintable from
    // public split coordinates, so accepting one here would let a caller stamp its own content
    // with a genuine fork's identity — and the relay's redundant-fork exit, seeing an exact match,
    // would then discard the real partition as already-materialized. Provenance enters through the
    // in-container install or a wire snapshot install, both of which take it from the container's
    // own record rather than a caller's.
    self.create_group_from_fork_unreserved(
      gid, generation, config, now, seed, fsm, snapshot, read_only, None, boot_epoch, log, stable,
    )
  }

  /// INSTALL `parent`'s head fork for `child`, here, without it ever leaving the container.
  ///
  /// The caller names the PAIR and lends its engine; the container re-decides everything from that
  /// parent's own staged queue, makes the storage, and — only once every check has passed — pops
  /// the partition straight into the manufactured baseline. Pair with
  /// [`peek_yieldable_fork`](Self::peek_yieldable_fork), whose [`ForkView`] carries exactly the two
  /// ids this call wants: the peek ADVISES, the install DECIDES, and it decides on the named pair
  /// rather than on whatever a second global drain would happen to reach.
  ///
  /// THE FORK NEVER LEAVES HOME, and that is the security argument. The earlier design handed the
  /// forked half out as a capability object and asked every layer it crossed to keep the
  /// container's books: the value had to be unforgeable, immutable, un-retargetable, reserved
  /// across the hand-out window, released if the holder gave up, and impossible to present twice —
  /// six obligations, each of which failed at least once under review. Here there is no window in
  /// which the forked half exists outside the container, so there is nothing to forge, mutate,
  /// mis-target, race, double-present, drop in a caller's precheck, or leak. The whole class is
  /// gone rather than guarded.
  ///
  /// MEMBERSHIP COMES FROM THE COMMITTED SPLIT, never from a caller: the config installed here is
  /// the parent's local tuning under the parent's voter set AT THE SPLIT ENTRY, identical on every
  /// replica because the split rode the parent's totally-ordered log — the only divergence is
  /// [`reshape_born_prevention`], applied here so it is applied identically everywhere. A
  /// caller-supplied config would let two hosts install the same child id, blob and token under
  /// DISJOINT sole-voter sets and then commit conflicting histories under one identity, and no
  /// gate downstream could catch it, because every other input matches.
  ///
  /// THIS DOOR SKIPS THE SPLIT RESERVATION, and only this one may: the reservation fences the
  /// OTHER admission doors from squatting an id a split owns, and this call IS that split claiming
  /// its own id — the fork's own parent reserves the child while it is staged, so consulting the
  /// predicate here would refuse the very admission it exists to protect.
  ///
  /// `extra` carries the facts the engine cannot answer — a coordinator's volatile tombstone set;
  /// [`NoHold`] for a caller with none.
  pub fn install_yieldable_fork<E, X>(
    &mut self,
    parent: &G,
    child: &G,
    engine: &mut E,
    extra: &X,
    now: impl Into<Now>,
    seed: u64,
  ) -> InstallOutcome<G, I>
  where
    E: MultiEngine<G, I>,
    X: ForkGate<G>,
    F::Command: Data,
    F::Snapshot: Data,
    F::Error: core::error::Error,
    I: Data,
  {
    let (parent, plan, epoch, config) =
      match self.prepare_fork_install(parent, child, engine, extra) {
        PrepareOutcome::Ready {
          parent,
          plan,
          epoch,
          config,
        } => (parent, plan, epoch, config),
        PrepareOutcome::Done(outcome) => return outcome,
      };
    // BOTH ARMS ROLL THE STORAGE BACK. `prepare_fork_install` created the child's group, so an
    // exit here must undo it exactly as its own refusal paths do — a leaked half-made group would
    // read as occupancy at the next drain and hold this very fork against itself.
    let Some((log, stable)) = engine.stores(child) else {
      engine.remove_group(child);
      return InstallOutcome::Held;
    };
    // THE POP is the point of no return: the forked half is obtainable only by consuming it, so
    // from here on there is no rollback — which is exactly why every check ran first. The only
    // steps that may stand between it and the baseline write are the two below: a token mint that
    // is a pure function of data already in hand, and the infallible host-id latch. Nothing that
    // can fail, and nothing whose verdict could have changed since the examine, may join them.
    let popped = self
      .groups
      .get_mut(&parent)
      .and_then(Endpoint::pop_pending_fork);
    let Some((fork, fsm)) = popped else {
      engine.remove_group(child);
      return InstallOutcome::Empty;
    };
    let fork_id = mint_fork_id(
      &parent,
      plan.parent_gen_after,
      plan.split_index,
      plan.split_term,
      plan.child_bytes.clone(),
      plan.child_gen,
    );
    self.host_id.get_or_insert(config.id());
    write_fork_baseline(
      &config,
      fork.blob,
      plan.child_gen,
      fork.read_only,
      Some(fork_id),
      epoch,
      log,
      stable,
    );
    let ep = Endpoint::restart(
      config.clone(),
      now,
      group_seed(seed, child),
      fsm,
      epoch,
      log,
      stable,
    );
    // The parent's relay guard advances past this fork, and the child's opens at its restored
    // lineage — the same two records the yield used to make before the install could fail.
    self
      .lineage
      .insert(parent.cheap_clone(), plan.parent_gen_after);
    self
      .lineage
      .insert(child.cheap_clone(), ep.restored_lineage());
    self.clear_unresolvable_hints_naming(child);
    self.groups.insert(child.cheap_clone(), ep);
    self.mark_dirty(child);
    InstallOutcome::Installed {
      parent,
      child: child.cheap_clone(),
      child_gen: plan.child_gen,
      parent_gen_after: plan.parent_gen_after,
      split_index: plan.split_index,
      config,
    }
  }

  /// The reservation-free fork install, reachable only from inside this crate — the shared write
  /// half of the public token-less door and of the wire relay's own materialization. Consulting the
  /// reservation here would refuse the very admission it protects: a committed fork's own parent
  /// reserves the child id while the fork is staged, so the split could never claim it.
  #[allow(clippy::too_many_arguments)]
  pub(crate) fn create_group_from_fork_unreserved<L, S>(
    &mut self,
    gid: G,
    generation: u64,
    config: Config<I>,
    now: impl Into<Now>,
    seed: u64,
    fsm: F,
    snapshot: Bytes,
    read_only: Option<ReadOnlyOption>,
    fork_id: Option<ForkId>,
    boot_epoch: u64,
    log: &mut L,
    stable: &mut S,
  ) -> Result<(), CreateGroupError>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
    F::Command: Data,
    F::Snapshot: Data,
    F::Error: core::error::Error,
    I: Data,
  {
    validate_fork_boot_epoch(boot_epoch)?;
    validate_working_generation(generation)?;
    validate_new_group(&self.groups, &self.host_id, &gid, &config)?;
    validate_virgin_stores(log, stable)?;
    self.host_id.get_or_insert(config.id());
    // `generation` (the child's incarnation under the unified lineage counter), the inherited
    // `read_only` provenance, and the child's `fork_id` PROVENANCE token ride the baseline meta,
    // so the restart boot below — and every later restart from the child's own stores — recovers
    // all three exactly as it would from a real install (absent at 0 / `None`: byte-identical to
    // the pre-reshaping baseline).
    write_fork_baseline(
      &config, snapshot, generation, read_only, fork_id, boot_epoch, log, stable,
    );
    let ep = Endpoint::restart(
      config,
      now,
      group_seed(seed, &gid),
      fsm,
      boot_epoch,
      log,
      stable,
    );
    self
      .lineage
      .insert(gid.cheap_clone(), ep.restored_lineage());
    self.clear_unresolvable_hints_naming(&gid);
    self.groups.insert(gid.cheap_clone(), ep);
    self.mark_dirty(&gid);
    Ok(())
  }
}

// Caller-supplied-RNG constructors, mirroring `Endpoint`'s `*_with_rng` family. The seed-taking
// constructors fold the group id into the seed automatically; here the CALLER owns cross-group
// decorrelation — supply a distinctly-seeded RNG per group, or co-located groups draw correlated
// election jitter.
impl<G, I, F, R> MultiRaft<G, I, F, R>
where
  G: GroupId,
  I: NodeId,
  F: StateMachine,
  R: rand::Rng,
{
  /// Create a fresh group driven by a caller-supplied RNG (see [`Endpoint::new_with_rng`]);
  /// `generation` seeds the lineage counter exactly as on
  /// [`create_group`](MultiRaft::create_group).
  ///
  /// # Errors
  /// The admission checks of [`CreateGroupError`], as on the seed-taking constructors.
  pub fn create_group_with_rng(
    &mut self,
    gid: G,
    generation: u64,
    config: Config<I>,
    now: impl Into<Now>,
    rng: R,
    fsm: F,
  ) -> Result<(), CreateGroupError> {
    validate_new_group(&self.groups, &self.host_id, &gid, &config)?;
    validate_working_generation(generation)?;
    self.host_id.get_or_insert(config.id());
    let mut ep = Endpoint::new_with_rng(config, now, rng, fsm);
    ep.seed_lineage(generation);
    self.lineage.insert(gid.cheap_clone(), generation);
    self.clear_unresolvable_hints_naming(&gid);
    self.groups.insert(gid, ep);
    Ok(())
  }

  /// Recover a group from durable storage with a caller-supplied RNG (see
  /// [`Endpoint::restart_with_rng`]).
  ///
  /// # Errors
  /// The admission checks of [`CreateGroupError`]; refusal happens BEFORE any store is read.
  #[allow(clippy::too_many_arguments)]
  pub fn restore_group_with_rng<L, S>(
    &mut self,
    gid: G,
    config: Config<I>,
    now: impl Into<Now>,
    rng: R,
    fsm: F,
    boot_epoch: u64,
    log: &mut L,
    stable: &mut S,
  ) -> Result<(), CreateGroupError>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
    F::Command: Data,
    F::Snapshot: Data,
    F::Error: core::error::Error,
    I: Data,
  {
    validate_new_group(&self.groups, &self.host_id, &gid, &config)?;
    self.host_id.get_or_insert(config.id());
    let ep = Endpoint::restart_with_rng(config, now, rng, fsm, boot_epoch, log, stable);
    self
      .lineage
      .insert(gid.cheap_clone(), ep.restored_lineage());
    self.clear_unresolvable_hints_naming(&gid);
    self.groups.insert(gid.cheap_clone(), ep);
    self.mark_dirty(&gid);
    Ok(())
  }

  /// Recover a group from a pre-format store with a caller-supplied RNG (see
  /// [`Endpoint::restart_migrating_with_rng`]).
  ///
  /// # Errors
  /// The admission checks of [`CreateGroupError`]; refusal happens BEFORE any store is read.
  #[allow(clippy::too_many_arguments)]
  pub fn restore_group_migrating_with_rng<L, S>(
    &mut self,
    gid: G,
    config: Config<I>,
    now: impl Into<Now>,
    rng: R,
    fsm: F,
    boot_epoch: u64,
    assume_prior_lease_support: Option<Duration>,
    log: &mut L,
    stable: &mut S,
  ) -> Result<(), CreateGroupError>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
    F::Command: Data,
    F::Snapshot: Data,
    F::Error: core::error::Error,
    I: Data,
  {
    validate_new_group(&self.groups, &self.host_id, &gid, &config)?;
    self.host_id.get_or_insert(config.id());
    let ep = Endpoint::restart_migrating_with_rng(
      config,
      now,
      rng,
      fsm,
      boot_epoch,
      assume_prior_lease_support,
      log,
      stable,
    );
    self
      .lineage
      .insert(gid.cheap_clone(), ep.restored_lineage());
    self.clear_unresolvable_hints_naming(&gid);
    self.groups.insert(gid.cheap_clone(), ep);
    self.mark_dirty(&gid);
    Ok(())
  }

  /// Create a group from locally forked state with a caller-supplied RNG (see
  /// [`create_group_from_fork`](Self::create_group_from_fork) for the manufactured-install
  /// contract and [`Endpoint::restart_with_rng`] for the RNG one).
  ///
  /// # Errors
  /// The admission checks of [`CreateGroupError`], including
  /// [`CreateGroupError::InvalidBootEpoch`] when `boot_epoch == 0` (a fork's baseline needs the
  /// prior epoch to itself) and [`CreateGroupError::StorageInUse`] over non-virgin stores;
  /// refusal happens BEFORE any store write.
  #[allow(clippy::too_many_arguments)]
  pub fn create_group_from_fork_with_rng<L, S>(
    &mut self,
    gid: G,
    generation: u64,
    config: Config<I>,
    now: impl Into<Now>,
    rng: R,
    fsm: F,
    snapshot: Bytes,
    read_only: Option<ReadOnlyOption>,
    boot_epoch: u64,
    log: &mut L,
    stable: &mut S,
  ) -> Result<(), CreateGroupError>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
    F::Command: Data,
    F::Snapshot: Data,
    F::Error: core::error::Error,
    I: Data,
  {
    // The same public door: the same fence, and the same token-less install — see
    // [`create_group_from_fork`](Self::create_group_from_fork).
    if self.split_reserved(&gid) {
      return Err(CreateGroupError::SplitReserved);
    }
    self.create_group_from_fork_with_rng_unreserved(
      gid, generation, config, now, rng, fsm, snapshot, read_only, None, boot_epoch, log, stable,
    )
  }

  /// The RNG-carrying twin of
  /// [`install_yieldable_fork`](MultiRaft::install_yieldable_fork) — same contract, same atomicity,
  /// same pair pin, the caller's RNG in place of the seed fold.
  pub fn install_yieldable_fork_with_rng<E, X>(
    &mut self,
    parent: &G,
    child: &G,
    engine: &mut E,
    extra: &X,
    now: impl Into<Now>,
    rng: R,
  ) -> InstallOutcome<G, I>
  where
    E: MultiEngine<G, I>,
    X: ForkGate<G>,
    F::Command: Data,
    F::Snapshot: Data,
    F::Error: core::error::Error,
    I: Data,
  {
    let (parent, plan, epoch, config) =
      match self.prepare_fork_install(parent, child, engine, extra) {
        PrepareOutcome::Ready {
          parent,
          plan,
          epoch,
          config,
        } => (parent, plan, epoch, config),
        PrepareOutcome::Done(outcome) => return outcome,
      };
    let Some((log, stable)) = engine.stores(child) else {
      engine.remove_group(child);
      return InstallOutcome::Held;
    };
    // THE POP is the point of no return here too — see the seed-taking twin for the ordering rule,
    // for what may stand between it and the baseline write, and for the storage rollback.
    let popped = self
      .groups
      .get_mut(&parent)
      .and_then(Endpoint::pop_pending_fork);
    let Some((fork, fsm)) = popped else {
      engine.remove_group(child);
      return InstallOutcome::Empty;
    };
    let fork_id = mint_fork_id(
      &parent,
      plan.parent_gen_after,
      plan.split_index,
      plan.split_term,
      plan.child_bytes.clone(),
      plan.child_gen,
    );
    self.host_id.get_or_insert(config.id());
    write_fork_baseline(
      &config,
      fork.blob,
      plan.child_gen,
      fork.read_only,
      Some(fork_id),
      epoch,
      log,
      stable,
    );
    let ep = Endpoint::restart_with_rng(config.clone(), now, rng, fsm, epoch, log, stable);
    self
      .lineage
      .insert(parent.cheap_clone(), plan.parent_gen_after);
    self
      .lineage
      .insert(child.cheap_clone(), ep.restored_lineage());
    self.clear_unresolvable_hints_naming(child);
    self.groups.insert(child.cheap_clone(), ep);
    self.mark_dirty(child);
    InstallOutcome::Installed {
      parent,
      child: child.cheap_clone(),
      child_gen: plan.child_gen,
      parent_gen_after: plan.parent_gen_after,
      split_index: plan.split_index,
      config,
    }
  }

  /// The RNG-carrying twin of
  /// [`create_group_from_fork_unreserved`](Self::create_group_from_fork_unreserved), crate-private
  /// for the same reason.
  #[allow(clippy::too_many_arguments)]
  pub(crate) fn create_group_from_fork_with_rng_unreserved<L, S>(
    &mut self,
    gid: G,
    generation: u64,
    config: Config<I>,
    now: impl Into<Now>,
    rng: R,
    fsm: F,
    snapshot: Bytes,
    read_only: Option<ReadOnlyOption>,
    fork_id: Option<ForkId>,
    boot_epoch: u64,
    log: &mut L,
    stable: &mut S,
  ) -> Result<(), CreateGroupError>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
    F::Command: Data,
    F::Snapshot: Data,
    F::Error: core::error::Error,
    I: Data,
  {
    validate_fork_boot_epoch(boot_epoch)?;
    validate_working_generation(generation)?;
    validate_new_group(&self.groups, &self.host_id, &gid, &config)?;
    validate_virgin_stores(log, stable)?;
    self.host_id.get_or_insert(config.id());
    write_fork_baseline(
      &config, snapshot, generation, read_only, fork_id, boot_epoch, log, stable,
    );
    let ep = Endpoint::restart_with_rng(config, now, rng, fsm, boot_epoch, log, stable);
    self
      .lineage
      .insert(gid.cheap_clone(), ep.restored_lineage());
    self.clear_unresolvable_hints_naming(&gid);
    self.groups.insert(gid.cheap_clone(), ep);
    self.mark_dirty(&gid);
    Ok(())
  }
}

// The per-group driving surface. Delegates to the group then marks it for an output drain. Each
// method mirrors the same-named `Endpoint` method and returns `None` when no group `gid` is hosted.
// The `F::Command`/`F::Error` bounds are the apply-path bounds the `Endpoint` driving methods carry.
impl<G, I, F, R> MultiRaft<G, I, F, R>
where
  G: GroupId,
  I: NodeId,
  F: StateMachine,
  R: rand::Rng,
  F::Command: Data,
  F::Error: core::error::Error,
{
  /// Route an inbound peer message to `gid`. `None` if no such group.
  #[must_use = "`None` means no group with this id is hosted — the call did nothing"]
  pub fn handle_message<L, S>(
    &mut self,
    gid: &G,
    now: impl Into<Now>,
    log: &mut L,
    stable: &mut S,
    from: I,
    msg: Message<I>,
  ) -> Option<()>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
    F::Snapshot: Data,
  {
    // RECEIPT-TIME REVALIDATION of the cure-admission hint, at the one edge where sibling
    // state is readable before the adopt consumes it. The hint is derived at resolver cranks,
    // but its sibling-reading gate legs can go stale WITHIN a message batch — a source group's
    // append arms `freeze_pending` at append-OBSERVATION, so a source append followed by this
    // target's cure blob, with no crank between, would adopt and erase the live thaw
    // obligation the freeze-active predicate protects. The endpoint-local legs (the walk's
    // watermark, the fork barrier) cannot stale through siblings and are not re-run.
    if matches!(msg, Message::InstallSnapshot(_))
      && self
        .groups
        .get(gid)
        .is_some_and(|ep| ep.pending_merge().is_some())
    {
      let hosted_crossing = self.groups.get(gid).is_some_and(|t| {
        t.crossing_sources().iter().any(|b| {
          G::decode_exact(b.clone())
            .map(|g| self.groups.contains_key(&g))
            .unwrap_or(true)
        })
      });
      if (hosted_crossing || self.obligation_names_hosted_unadvanced(gid))
        && let Some(ep) = self.groups.get_mut(gid)
      {
        ep.note_merge_park_unresolvable(false);
        // Drop the WHOLE message, not just the hint — and key the gate on the PARK, not the
        // hint: the hint is edge-consumed at the first refusal, while the sibling condition
        // outlives it, and any later install's ordinary DESTRUCTIVE completion would supersede
        // the park and clear covered obligations — exactly what the sibling state proved
        // unsafe (a hosted crossing the blob would husk, or a live frozen counterparty whose
        // thaw drive the clear would erase). Silent, like every receipt refusal: the paced
        // resend re-drives once the sibling hold resolves through its own lifecycle.
        return Some(());
      }
    }
    self
      .groups
      .get_mut(gid)?
      .handle_message(now, log, stable, from, msg);
    self.mark_dirty(gid);
    Some(())
  }

  /// Fire `gid`'s due timers. `None` if no such group.
  #[must_use = "`None` means no group with this id is hosted — the call did nothing"]
  pub fn handle_timeout<L, S>(
    &mut self,
    gid: &G,
    now: impl Into<Now>,
    log: &mut L,
    stable: &mut S,
  ) -> Option<()>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
    F::Snapshot: Data,
  {
    self.groups.get_mut(gid)?.handle_timeout(now, log, stable);
    self.mark_dirty(gid);
    Some(())
  }

  /// Drain `gid`'s storage completions. `None` if no such group, else that group's
  /// [`StorageProgress`] (`MorePending` asks to be re-driven without sleeping).
  #[must_use = "`None` means no group with this id is hosted — the call did nothing"]
  pub fn handle_storage<L, S>(
    &mut self,
    gid: &G,
    now: impl Into<Now>,
    log: &mut L,
    stable: &mut S,
  ) -> Option<StorageProgress>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
    F::Snapshot: Data,
  {
    let progress = self.groups.get_mut(gid)?.handle_storage(now, log, stable);
    self.mark_dirty(gid);
    Some(progress)
  }

  /// Propose a command on `gid`, which must be the leader. `None` if no such group, else the
  /// append result. Call
  /// [`flush_appends`](Self::flush_appends) for the group once after a burst of proposals.
  #[must_use = "`None` means no group with this id is hosted — the call did nothing"]
  pub fn propose<L, S>(
    &mut self,
    gid: &G,
    now: impl Into<Now>,
    log: &mut L,
    stable: &S,
    cmd: &F::Command,
  ) -> Option<Result<Index, ProposeError<I>>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let result = self.groups.get_mut(gid)?.propose(now, log, stable, cmd);
    self.mark_dirty(gid);
    Some(result)
  }

  /// Flush `gid`'s coalesced replication fan-out (once per drive, after a propose burst). `None`
  /// if no such group.
  #[must_use = "`None` means no group with this id is hosted — the call did nothing"]
  pub fn flush_appends<L, S>(
    &mut self,
    gid: &G,
    now: impl Into<Now>,
    log: &L,
    stable: &S,
  ) -> Option<()>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    self.groups.get_mut(gid)?.flush_appends(now, log, stable);
    self.mark_dirty(gid);
    Some(())
  }

  /// Propose a membership change (single-step) on `gid`, which must be the leader. `None` if no
  /// such group. As with [`propose`](Self::propose), call [`flush_appends`](Self::flush_appends)
  /// for the group once after a burst.
  #[must_use = "`None` means no group with this id is hosted — nothing was proposed"]
  pub fn propose_conf_change<L, S>(
    &mut self,
    gid: &G,
    now: impl Into<Now>,
    log: &mut L,
    stable: &S,
    cc: ConfChange<I>,
  ) -> Option<Result<Index, ProposeError<I>>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    if !self.groups.contains_key(gid) {
      return None;
    }
    // The container-level claimed-target fence: another hosted source's APPLIED freeze names `gid`
    // as its merge target for an UNRESOLVED merge, and this change would MOVE `gid`'s voters. Off the
    // source's hosts the source strands frozen (`commit_merge` then refuses `VoterSetsDiffer`,
    // `rollback_merge` `SourceMissing`, with no release valve). A learner-only change keeps the voter
    // sets aligned and is left to the merge's own `LearnersPresent` re-check; a claim `gid` already
    // aborted is exempt (the source thaws off its obligation). The endpoint's own `merge_conf_fence`
    // cannot see this cross-group claim, so surface the same class.
    if conf_change_moves_voters(cc.ty()) && self.some_source_claims_target_unresolved(gid) {
      return Some(Err(ProposeError::MergeInFlight));
    }
    let result = self
      .groups
      .get_mut(gid)?
      .propose_conf_change(now, log, stable, cc);
    self.mark_dirty(gid);
    Some(result)
  }

  /// Propose a membership change (joint-consensus capable) on `gid`, which must be the leader.
  /// `None` if no such group. As with [`propose`](Self::propose), call
  /// [`flush_appends`](Self::flush_appends) for the group once after a burst.
  #[must_use = "`None` means no group with this id is hosted — nothing was proposed"]
  pub fn propose_conf_change_v2<L, S>(
    &mut self,
    gid: &G,
    now: impl Into<Now>,
    log: &mut L,
    stable: &S,
    cc: ConfChangeV2<I>,
  ) -> Option<Result<Index, ProposeError<I>>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    if !self.groups.contains_key(gid) {
      return None;
    }
    // The container-level claimed-target fence (see [`propose_conf_change`]): another hosted
    // source's applied freeze names `gid` as its target for an UNRESOLVED merge, and this change
    // moves `gid`'s voters — a learner-only change, or a claim `gid` already aborted, is exempt.
    if cc
      .changes()
      .iter()
      .any(|c| conf_change_moves_voters(c.ty()))
      && self.some_source_claims_target_unresolved(gid)
    {
      return Some(Err(ProposeError::MergeInFlight));
    }
    let result = self
      .groups
      .get_mut(gid)?
      .propose_conf_change_v2(now, log, stable, cc);
    self.mark_dirty(gid);
    Some(result)
  }

  /// Propose a cluster-wide read-mode migration on `gid`, which must be the leader. `None` if no
  /// such group. As with [`propose`](Self::propose), call [`flush_appends`](Self::flush_appends)
  /// for the group once after a burst.
  #[must_use = "`None` means no group with this id is hosted — nothing was proposed"]
  pub fn propose_read_mode_change<L, S>(
    &mut self,
    gid: &G,
    now: impl Into<Now>,
    log: &mut L,
    stable: &S,
    mode: ReadOnlyOption,
  ) -> Option<Result<Index, ProposeError<I>>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let result = self
      .groups
      .get_mut(gid)?
      .propose_read_mode_change(now, log, stable, mode);
    self.mark_dirty(gid);
    Some(result)
  }

  /// Propose a group SPLIT on `gid` (the parent): a committed `Split` entry deterministically
  /// forks `child` out of the parent's state machine on every replica. Leader-only, on the
  /// parent's own log; the payload carries the child id (G-free on the wire), `child_gen` (the
  /// child's incarnation — normally 0), the parent's bumped lineage (computed here: the live
  /// counter + 1, the replay-guard/idempotence anchor), and the embedder's opaque
  /// `instruction`, bounded by the single-frame append check. `None` if no such group.
  ///
  /// Gate order: poisoned → leader → joint config → split in flight → hosted child → child-id
  /// wire bound → the ordinary admin append (whose refusals pass through as
  /// [`SplitError::Propose`]). `BelowFloor` is produced by the COORDINATOR delegators through
  /// their floor seam, and `CrossPlane` by a sharded host's handle — the container stays floor-
  /// and plane-free.
  #[must_use = "`None` means no group with this id is hosted — nothing was proposed"]
  #[allow(clippy::too_many_arguments)]
  pub fn propose_split<L, S>(
    &mut self,
    gid: &G,
    now: impl Into<Now>,
    log: &mut L,
    // Vestigial, as on the whole propose family: kept so the delegators thread `&stable`.
    _stable: &S,
    child: &G,
    child_gen: u64,
    instruction: Bytes,
  ) -> Option<Result<Index, SplitError<I>>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    {
      let ep = self.groups.get(gid)?;
      if ep.is_poisoned() {
        return Some(Err(SplitError::Propose(ProposeError::Poisoned)));
      }
      if !ep.role().is_leader() {
        return Some(Err(SplitError::NotLeader {
          leader: ep.leader(),
        }));
      }
      // The parent's FSM cannot split: a committed `Split` against it would poison every replica at
      // apply (`SplitUnsupported`). Refuse at propose so nothing is appended. `supports_split` is
      // type-constant, so the leader's answer holds for the whole group.
      if !ep.state_machine().supports_split() {
        return Some(Err(SplitError::Unsupported));
      }
      // A frozen (or freezing) parent must not fork: the split would mutate the FSM above the
      // freeze boundary, breaking the absorb's nothing-above-the-freeze determinism — the same
      // gate class as propose/conf-change/read, applied to the one admin verb that had slipped
      // it (the split's own lineage guard already no-ops the pre-freeze in-flight shapes).
      if ep.merge_freeze_active() {
        return Some(Err(SplitError::Propose(ProposeError::Frozen)));
      }
      // A joint parent would hand the child an ambiguous bootstrap voter set: refuse at
      // propose (the one-line rule that removes the hairiest interleaving).
      if ep.conf_state().is_joint() {
        return Some(Err(SplitError::JointConfig));
      }
      // One split in flight at a time: the mint below reads the LIVE counter, which bumps only
      // when the earlier split APPLIES — a second mint before then duplicates it, and the
      // duplicate can only no-op at the apply-time lineage guard (its child's half would
      // otherwise be given up by the parent and never materialized). See
      // [`SplitError::SplitInFlight`] for the self-healing derivation.
      if ep.split_in_flight() {
        return Some(Err(SplitError::SplitInFlight));
      }
    }
    // A hosted child id (the parent's own included) can never be forked into existence here.
    if self.groups.contains_key(child) {
      return Some(Err(SplitError::ChildExists));
    }
    // Refuse an out-of-bound child encoding BEFORE it can be committed: every replica's relay
    // decode would otherwise poison the parent on a committed entry.
    let mut child_bytes = Vec::new();
    child.encode(&mut child_bytes);
    if child_bytes.is_empty() || child_bytes.len() > crate::wire::MAX_GROUP_ID_LEN {
      return Some(Err(SplitError::InvalidChild));
    }
    let ep = self.groups.get_mut(gid)?;
    // The bump is computed from the LIVE counter, whose sole bump site is the earlier split's
    // APPLY — the in-flight gate above holds new proposals until then, so consecutive mints
    // chain instead of duplicating. The mint stops strictly below the reserved `MERGED_FLOOR`
    // terminal: at the ceiling the split is refused here, before any append (unreachable short of
    // log-index exhaustion, every bump consuming a log index).
    let Some(parent_gen_after) = next_lineage(ep.shape_gen()) else {
      return Some(Err(SplitError::LineageExhausted));
    };
    let child_bytes = Bytes::from(child_bytes);
    let payload = SplitPayload::new(
      child_bytes.clone(),
      child_gen,
      parent_gen_after,
      instruction,
    );
    let mut buf = Vec::new();
    crate::wire::encode_split_payload(&payload, &mut buf);
    let result = ep
      .propose_split_entry(now, log, child_bytes, Bytes::from(buf))
      .map_err(SplitError::Propose);
    self.mark_dirty(gid);
    Some(result)
  }

  /// Propose a merge FREEZE on `source` (the group that will be absorbed): a committed
  /// `PrepareMerge` freezes it on every replica so `target` can absorb it at the boundary.
  /// Leader-proposed on the SOURCE's own log, resolved through the callers' `stores` seam —
  /// the same seam the teardown gate reads, because one refusal here
  /// ([`SourceClaimedAsTarget`](MergeError::SourceClaimedAsTarget)) must decode a co-hosted
  /// claimant's append-pending claim from ITS log. The preconditions are checked against the
  /// LOCAL replicas (colocation makes them representative; every parked apply re-checks the
  /// facts that matter from its own log): identical voter sets, neither carrying learners,
  /// both non-joint, same active read mode, no membership change in flight on either side, the
  /// source not already frozen or freezing, and the source not itself the claimed target of
  /// another in-flight merge. `None` if no group `source` is hosted (or the seam cannot
  /// resolve its stores — the starvation case of the [`GroupStores`] contract; nothing was
  /// proposed either way).
  ///
  /// Floor refusals (`MergeError::BelowFloor`) are the COORDINATOR delegators' leg through
  /// their per-call floor seam, and `CrossPlane` the sharded handle's — the container stays
  /// floor- and plane-free, exactly as it is for splits.
  ///
  /// DIRECTION: a claim must point strictly DOWN the fixed total order over ids — the source must
  /// encode STRICTLY ABOVE the target ([`DirectionInverted`](MergeError::DirectionInverted)), so
  /// the encoding-minimal id of any pair is the target/survivor. Orient each pair (source = the
  /// encoding-larger side) before proposing; this makes a claim cycle unconstructible.
  ///
  /// ADMISSION IS OPTIMISTICALLY CONCURRENT. These propose gates are TRUTHFUL LOCAL REFUSALS about
  /// the state THIS replica observes, not a distributed serializer. Admission reads the target's
  /// state from LOCAL replicas, so overlapping admissions at different leaders are SAFE but may
  /// resolve deterministically AGAINST you (the losing claim aborts through the normal path). Serialize
  /// your OWN admissions if you like, as an optimization — but never treat a refusal error as a
  /// mutual-exclusion primitive, and NEVER write `MERGED_FLOOR` to break a wedge by hand (see the
  /// consensus-grade warning on [`MERGED_FLOOR`](crate::MERGED_FLOOR)). The direction rule and the
  /// dead-target self-thaw make the two liveness wedges (claim cycles, dead-target strands) impossible
  /// and self-healing respectively, with no embedder serialization required.
  #[must_use = "`None` means no group with this id is hosted — nothing was proposed"]
  pub fn prepare_merge<L, S, St>(
    &mut self,
    source: &G,
    now: impl Into<Now>,
    stores: &mut St,
    target: &G,
  ) -> Option<Result<Index, MergeError<I>>>
  where
    St: GroupStores<G, L, S>,
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    if !self.groups.contains_key(source) {
      return None;
    }
    if source == target {
      return Some(Err(MergeError::SelfMerge));
    }
    // THE DIRECTION RULE — a constant-vs-constant STRUCTURAL refusal, so it sits with the earliest
    // gates. A merge claim must point strictly DOWN the fixed total order over ids: the source must
    // encode STRICTLY ABOVE the target (their canonical `Data` encodings compared as byte strings).
    // Every edge then strictly decreases one total order, so a claim cycle (A→B→…→A) is
    // UNCONSTRUCTIBLE — the property that keeps concurrently-admitted freezes at different leaders
    // from deadlocking every release valve `AlreadyFrozen`. The ids never move, so this verdict is
    // truthful ahead of every state-dependent gate; the embedder orients each pair before proposing.
    // Both encodings are reused below (the target for the claim payload; the pair to compare here).
    let mut source_bytes = Vec::new();
    source.encode(&mut source_bytes);
    let mut target_bytes = Vec::new();
    target.encode(&mut target_bytes);
    if source_bytes <= target_bytes {
      return Some(Err(MergeError::DirectionInverted));
    }
    let Some(tep) = self.groups.get(target) else {
      return Some(Err(MergeError::TargetMissing));
    };
    // A frozen (or freezing) target is being dissolved itself — it can absorb nothing.
    if tep.merge_freeze_active() {
      return Some(Err(MergeError::AlreadyFrozen));
    }
    // The target's FSM cannot absorb: a committed `CommitMerge` against it would poison every replica
    // at apply (`MergeUnsupported`), so freezing the source now would only strand it. Refuse at
    // propose. `supports_absorb` is type-constant and colocation makes the local target authoritative.
    if !tep.state_machine().supports_absorb() {
      return Some(Err(MergeError::Unsupported));
    }
    // A target owing outstanding thaws to already-aborted sources is NOT refused here: `abandoned`
    // is a per-source collection, so a fresh freeze toward this target adds an independent obligation
    // (fan-in) that its own later abort records under its own key — nothing is dropped. The absorb
    // itself is still serialized by `AlreadyPending` (one `CommitMerge` in flight/parked at a time).
    // The SAME-source hazard — re-committing a merge whose own abort this target already applied,
    // which would park at the dead freeze generation the thaw pass then drives past — is fenced at
    // the absorb by `commit_merge`'s `TargetOwesThaw` gate (and its apply-time belt), not here.
    let target_conf = tep.conf_state();
    let target_mode = tep.active_read_mode();
    let target_conf_in_flight = tep.conf_change_in_flight();
    let sep = self.groups.get(source).expect("checked hosted above");
    if sep.is_poisoned() {
      return Some(Err(MergeError::Propose(ProposeError::Poisoned)));
    }
    if !sep.role().is_leader() {
      return Some(Err(MergeError::NotLeader {
        leader: sep.leader(),
      }));
    }
    if sep.merge_freeze_active() {
      return Some(Err(MergeError::AlreadyFrozen));
    }
    // A source mid-SPLIT must not freeze: the freeze mints `source_gen_after` from the source's
    // live `shape_gen`, but the appended-unapplied `Split` below it applies first and bumps that
    // counter — so the freeze's generation COLLIDES with the split's on the one lineage counter,
    // and every gen-keyed reader (the absorb's source-gen check, the abort-relay's incarnation
    // gate) can no longer tell the two moves apart. The exact dual of `propose_split` refusing a
    // freezing parent ([`SplitError::Frozen`]): split and freeze are mutually exclusive on a group.
    // TRANSIENT — the split applies, then the same freeze mints from the post-split counter.
    //
    // The STAGED-FORK leg outlives that apply: until the child's baseline is locally durable, this
    // source carries the child's only local derivation — its log, or (once a rebaseline has retired
    // the log's replay) the queued fork's in-memory blob — and the absorb that follows a freeze
    // DESTROYS the endpoint holding both. The source's split machinery is simply not finished, so
    // the same refusal covers it. Host-local, and therefore only half a door: a sibling that
    // already flushed the child's baseline sees no obligation and can commit the freeze from there,
    // which is why the resolver's own fork holds — not this refusal — are the actual guarantee.
    // Transient the same way: the obligation clears when the driver reports the child's baseline
    // durable.
    if sep.split_in_flight() || sep.fork_obligations_standing() {
      return Some(Err(MergeError::SplitInFlight));
    }
    // NB: a source with a target-role abort still in flight is deliberately NOT fenced here. Unlike
    // the split (which materializes a child and must stay mutually exclusive with a freeze), the
    // freeze fold is a monotone MAX — never a stale-aborting lineage guard — so a freeze whose
    // generation collides with the in-flight abort's still applies, and the abort's `abandoned`
    // obligation is honored DOWNSTREAM: the absorbing target's Resolve arm HOLDS the absorb until
    // the thaw pass discharges it (see `a_late_obligation_holds_the_absorb_until_the_thaw_discharges`).
    // Only the TARGET side (`commit_merge`), whose absorb rides a STRICT lineage guard that
    // stale-aborts and strands, needs the abort fence.
    // A source that is itself mid-ABSORB (a CommitMerge in flight or parked as a target)
    // must finish that first: freezing it would mint a source generation the pending absorb
    // is about to move, and the two verbs' entries would race on one counter.
    if sep.commit_merge_in_flight() || sep.pending_merge().is_some() || sep.capture_debt().is_some()
    {
      // The debt leg: an absorbed-but-uncaptured union still owes its durability capture, and
      // freezing the holder would let a claimant absorb and tear it down with the debt live —
      // the consumed prior source's stores would strand as an unreachable orphan.
      return Some(Err(MergeError::AlreadyPending));
    }
    // A source still owing a target-role thaw must discharge it before dissolving. It applied an
    // abort as a TARGET, whose durable `abandoned` obligation the per-crank thaw pass discharges
    // from THIS endpoint; the Resolve arm removes the source endpoint, so freezing it here races
    // that removal against the obligation's discharge — a source torn down mid-thaw takes the
    // obligation with it and strands the upstream source frozen forever. Sits with `AlreadyPending`
    // among the target-role-residue gates (both refuse a source still entangled in a prior merge as
    // a target). TRANSIENT like `SourceBarrierPending`: the thaw pass clears it within a few cranks,
    // then the same freeze admits.
    if sep.has_abandoned() {
      return Some(Err(MergeError::SourceOwesThaw));
    }
    let source_conf = sep.conf_state();
    if source_conf.is_joint() || target_conf.is_joint() {
      return Some(Err(MergeError::JointConfig));
    }
    if sep.conf_change_in_flight() || target_conf_in_flight {
      return Some(Err(MergeError::ConfChangeInFlight));
    }
    if source_conf.voters() != target_conf.voters() {
      return Some(Err(MergeError::VoterSetsDiffer));
    }
    // Aligned replica sets, learners included: the absorb hands off on VOTER replicas only, and a
    // live merge parks on the target's voter hosts — a target-learner host would park forever. The
    // non-joint gate above empties `learners_next`, so the stable learner set is the whole of it.
    if !source_conf.learners().is_empty() || !target_conf.learners().is_empty() {
      return Some(Err(MergeError::LearnersPresent));
    }
    if sep.active_read_mode() != target_mode {
      return Some(Err(MergeError::ReadModesDiffer));
    }
    // The mint reads the live counter; it bumps only when THIS freeze applies, and a second
    // freeze cannot be proposed while this one is pending or applied (AlreadyFrozen above). It
    // stops strictly below the reserved `MERGED_FLOOR` terminal — at the ceiling the freeze is
    // refused here, before any append (unreachable short of log-index exhaustion).
    let Some(source_gen_after) = next_lineage(sep.shape_gen()) else {
      return Some(Err(MergeError::LineageExhausted));
    };
    // THE CLAIMED-TARGET SOURCE-ROLE GATE — the propose-time twin of the teardown lattice's
    // `Claimed` leg (`remove_group`), refusing the same participant state at the other door it
    // could slip through. A co-hosted source's freeze — applied `frozen_for`, or an
    // append-pending `PrepareMerge` decoded from its log, FAIL-CLOSED on unreadable ranges —
    // names THIS source as its target. Freezing it anyway lets a later absorb dissolve it, and
    // the claimant's release verbs BOTH ride the dissolved group's log (`commit_merge` and
    // `rollback_merge` are target-proposed), so the claimant strands frozen with no release
    // valve. Equal-voter-set pairing makes the claim locally visible wherever this propose can
    // run: the claimant's own freeze passed `VoterSetsDiffer` against this group, so every host
    // of this group co-hosts it. LAST among the refusals, exactly as the teardown leg sits: the
    // scan is fail-closed and TRANSIENT, so it must mask no structural (or terminal) verdict —
    // a caller told to wait for the claiming choreography must be able to trust that retrying
    // after its resolution can admit.
    if self.some_source_claims_target(source, stores) {
      return Some(Err(MergeError::SourceClaimedAsTarget));
    }
    // `target_bytes` is the encoding computed for the direction rule above — the claim payload.
    let payload = PrepareMergePayload::new(Bytes::from(target_bytes), source_gen_after);
    let mut buf = Vec::new();
    crate::wire::encode_prepare_merge_payload(&payload, &mut buf);
    // A hosted source whose stores the seam cannot resolve is the contract's starvation case:
    // nothing can be appended, which is exactly the `None` the absent-group arm reports.
    let (log, _) = stores.stores(source)?;
    let ep = self.groups.get_mut(source).expect("checked hosted above");
    let result = ep
      .propose_merge_entry(now, log, EntryKind::PrepareMerge, Bytes::from(buf))
      .map_err(MergeError::Propose);
    self.mark_dirty(source);
    Some(result)
  }

  /// Propose the merge ABSORB on `target`: a committed `CommitMerge` parks every target
  /// replica's apply until its LOCAL source replica is frozen-applied at the boundary, then the
  /// per-crank [`service_merge_applies`](Self::service_merge_applies) resolves it. Leader-
  /// proposed on the TARGET's own log, only once the LOCAL source replica is already
  /// frozen-applied (the cheap local gate; every replica's park re-checks the same facts). The
  /// carried `source_gen_after` is read off the frozen source, so the parked applies' decision
  /// inputs are fully log-determined. `None` if no group `target` is hosted.
  #[must_use = "`None` means no group with this id is hosted — nothing was proposed"]
  pub fn commit_merge<L, S>(
    &mut self,
    target: &G,
    now: impl Into<Now>,
    log: &mut L,
    // Vestigial, as on the whole propose family: kept so the delegators thread `&stable`.
    _stable: &S,
    source: &G,
  ) -> Option<Result<Index, MergeError<I>>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    if !self.groups.contains_key(target) {
      return None;
    }
    if source == target {
      return Some(Err(MergeError::SelfMerge));
    }
    let Some(sep) = self.groups.get(source) else {
      return Some(Err(MergeError::SourceMissing));
    };
    let source_conf = sep.conf_state();
    let source_mode = sep.active_read_mode();
    let source_frozen_ready =
      sep.is_frozen() && sep.freeze_index().is_some_and(|f| sep.applied_index() >= f);
    // The all-source-voters freeze barrier (the CRDB `waitForApplication` shape). Dissolution
    // rides the committed `CommitMerge` uniformly on every host, so it must not be proposed until
    // EVERY source voter has MATCHED the freeze boundary. A committed `CommitMerge` then certifies
    // the whole voter set holds the freeze `(F, freeze_term)` in its own log; a straggler later
    // cut off from the source leader self-advances on that identity (the parked-service leg)
    // instead of being orphaned when the other hosts floor and dismantle the source around it.
    // The barrier is observable only on the source LEADER's tracker (a follower's holds no peer
    // match), so a non-leader local source defers here as well — colocation puts the source
    // leader on the absorbing target's leader, exactly as it certifies the barrier at propose.
    let source_barrier_met = sep.role().is_leader()
      && sep
        .freeze_index()
        .is_some_and(|f| sep.peers_matched_through(f));
    let source_claim_mismatch = {
      let mut target_bytes = Vec::new();
      target.encode(&mut target_bytes);
      sep.frozen_for().is_none_or(|t| *t != target_bytes)
    };
    let freeze_index = sep.freeze_index();
    let freeze_term = sep.freeze_term();
    let source_gen_after = sep.shape_gen();
    let tep = self.groups.get(target).expect("checked hosted above");
    if tep.is_poisoned() {
      return Some(Err(MergeError::Propose(ProposeError::Poisoned)));
    }
    if !tep.role().is_leader() {
      return Some(Err(MergeError::NotLeader {
        leader: tep.leader(),
      }));
    }
    if tep.commit_merge_in_flight() || tep.pending_merge().is_some() || tep.capture_debt().is_some()
    {
      // The debt leg keeps the one-absorb-at-a-time posture across a fence-deferred capture:
      // the prior union's durability is still owed, and a second absorb would chain debts the
      // discharge pass and the restart re-park are not shaped for.
      return Some(Err(MergeError::AlreadyPending));
    }
    // A frozen (or freezing) target must not absorb: the CommitMerge would land above its own
    // freeze boundary and mutate the FSM there — the absorb determinism its own merge's
    // target depends on.
    if tep.merge_freeze_active() {
      return Some(Err(MergeError::AlreadyFrozen));
    }
    // The target's FSM cannot absorb: a committed `CommitMerge` against it would poison every replica
    // at apply (`MergeUnsupported`). Refuse at propose so the entry is never appended. `supports_absorb`
    // is type-constant, so the target leader's answer holds for the whole group.
    if !tep.state_machine().supports_absorb() {
      return Some(Err(MergeError::Unsupported));
    }
    // THE LINEAGE-SERIALIZATION FENCE. `target_gen_after` is minted below from the target's LIVE
    // `shape_gen`, and the apply-time guard admits the CommitMerge ONLY at exactly that mint. A
    // stale mint — a lineage move that applied on the target's own log between this propose and the
    // CommitMerge's apply — makes the parked apply no-op and emit `MergeAborted` WITHOUT recording
    // the source's thaw obligation: a permanently stranded frozen source. The target's `shape_gen`
    // has a CLOSED set of apply-time writers; every one that could be in flight below the CommitMerge
    // is gated so none can stale the mint:
    //   PrepareMerge-freeze        → `merge_freeze_active`                 (AlreadyFrozen, above)
    //   CommitMerge-absorb         → `commit_merge_in_flight`/`pending_merge` (AlreadyPending, above)
    //   RollbackMerge target-abort → `rollback_in_flight`                  (RollbackInFlight, below)
    //   RollbackMerge source-thaw  → only ever on a FROZEN group           (AlreadyFrozen, above)
    //   Split                      → `split_in_flight`                     (here)
    //   ConfChange                 → not a `shape_gen` write, but its apply races the voter
    //                                comparison            → `conf_change_in_flight` (ConfChangeInFlight, below)
    //   snapshot-install           → monotone-max on the same lineage that already accounts for a
    //                                committed CommitMerge, so it cannot regress the mint (no gate)
    // The SPLIT: an appended-unapplied `Split` applies first, bumps `shape_gen`, strands the absorb.
    // Both here are TRANSIENT — self-clearing once the reshaping move applies.
    if tep.split_in_flight() {
      return Some(Err(MergeError::SplitInFlight));
    }
    if tep.rollback_in_flight() {
      return Some(Err(MergeError::RollbackInFlight));
    }
    // The local readiness gate: the source must be frozen-applied at its boundary. `>=` rather
    // than `==` deliberately — a post-freeze election lands an FSM-no-op above the boundary on
    // every replica, and only FSM-no-ops can follow a surviving freeze, so applied-past-F is
    // the SAME state as applied-at-F (an equality gate would wedge every merge whose source
    // ever re-elected while frozen).
    if !source_frozen_ready {
      return Some(Err(MergeError::SourceNotReady));
    }
    if source_claim_mismatch {
      // The freeze names ONE absorbing target for its whole generation; a commit from any
      // other target could only park and abort at the service's claim leg — refuse it here
      // with the truthful verdict instead.
      return Some(Err(MergeError::SourceClaimed));
    }
    // The target must not already owe THIS exact source incarnation an aborted-merge thaw. A prior
    // abort of this very merge committed+applied on the target — recording `abandoned[source] ==
    // source_gen_after` — while the source is still frozen at that generation (its relayed thaw not
    // yet applied). Parking a re-proposed commit at the aborted gen would wedge every replica
    // forever: the per-crank thaw pass drives the source PAST it, so the park could never observe
    // frozen-at-expected again (the apply-time belt catches the same hazard for an order the gate
    // cannot see). GENERATION-EXACT — a spent obligation the source already thawed past and re-froze
    // above names a dead incarnation and must not refuse a fresh legitimate merge; the discharge
    // clears the record, so the re-freeze admits.
    let source_owed_key = {
      let mut b = Vec::new();
      source.encode(&mut b);
      Bytes::from(b)
    };
    if tep.owes_thaw_for(&source_owed_key) == Some(source_gen_after) {
      return Some(Err(MergeError::TargetOwesThaw));
    }
    let target_conf = tep.conf_state();
    if source_conf.is_joint() || target_conf.is_joint() {
      return Some(Err(MergeError::JointConfig));
    }
    if tep.conf_change_in_flight() {
      return Some(Err(MergeError::ConfChangeInFlight));
    }
    if source_conf.voters() != target_conf.voters() {
      return Some(Err(MergeError::VoterSetsDiffer));
    }
    // The same replica-set-alignment gate as `prepare_merge`, re-checked defensively at the
    // absorb: a learner that landed on either side after the freeze would strand this merge on a
    // learner host. Non-joint above empties `learners_next`, so the stable learner set is all of it.
    if !source_conf.learners().is_empty() || !target_conf.learners().is_empty() {
      return Some(Err(MergeError::LearnersPresent));
    }
    if source_mode != tep.active_read_mode() {
      return Some(Err(MergeError::ReadModesDiffer));
    }
    // The all-source-voters freeze barrier fires LAST, after the structural gates: a malformed
    // merge earns its structural refusal, while this transient one clears as the frozen source
    // replicates. Dissolving before every voter matches the boundary would orphan a lagging voter
    // once the source leader is lost (the committed CommitMerge must certify the whole voter set
    // holds the freeze); retry once it catches up, or roll the merge back if a voter is gone.
    if !source_barrier_met {
      return Some(Err(MergeError::SourceBarrierPending));
    }
    // The absorb mint stops strictly below the reserved `MERGED_FLOOR` terminal — at the ceiling
    // the absorb is refused here, before any append.
    let Some(target_gen_after) = next_lineage(tep.shape_gen()) else {
      return Some(Err(MergeError::LineageExhausted));
    };
    let mut source_bytes = Vec::new();
    source.encode(&mut source_bytes);
    let payload = CommitMergePayload::new(
      Bytes::from(source_bytes),
      freeze_index.expect("frozen-ready implies a boundary"),
      // The boundary's log identity, read off the SAME frozen-applied observation that gated
      // this propose: it certifies (freeze_index, freeze_term) committed in the source, which
      // is what lets a parked host later advance a stranded local source replica on identity.
      freeze_term.expect("frozen-ready implies a recorded freeze term"),
      source_gen_after,
      target_gen_after,
    );
    let mut buf = Vec::new();
    crate::wire::encode_commit_merge_payload(&payload, &mut buf);
    let ep = self.groups.get_mut(target).expect("checked hosted above");
    let result = ep
      .propose_merge_entry(now, log, EntryKind::CommitMerge, Bytes::from(buf))
      .map_err(MergeError::Propose);
    self.mark_dirty(target);
    Some(result)
  }

  /// Propose the merge ABORT on `target`: a committed target-side `RollbackMerge` abandons the
  /// merge of `source` into it, TOTALLY ORDERED against `CommitMerge` on the target's own log —
  /// landing below the commit it kills it at the commit's own lineage guard (parks never form);
  /// landing at the coordinate right after a parked commit it un-parks every replica aborted;
  /// landing later it no-ops (the merge already resolved — the abort's mint is stale by then).
  /// The applied abort records a durable `abandoned` obligation, and the per-crank container
  /// service ([`service_merge_applies`](Self::service_merge_applies)) DERIVES the source-side
  /// `RollbackMerge` from it on the source's own log — never an independent source decision, and a
  /// drive lost to churn is re-derived from the still-committed obligation (crash-durable via the
  /// abort entry's replay). The release valve — there is deliberately no timeout-based
  /// auto-unfreeze. `None` if no group `target` is hosted.
  ///
  /// The gates are best-effort truthfulness (the apply-time lineage guard is the decider): the
  /// TARGET leader proposes; the LOCAL source must exist and be frozen or freezing (the mint
  /// names its freeze generation); a frozen target refuses (its own dissolution outranks —
  /// aborting through it would bump its lineage above its own boundary). A target mid-split or with
  /// a DIFFERENT source's abort already in flight defers (`SplitInFlight`/`RollbackInFlight`) so an
  /// unapplied lineage move cannot stale this abort's generation mint into an obligation-less no-op
  /// that strands the aborted source — while an in-flight or parked commit of the SAME merge is
  /// deliberately RACED, not fenced (see the fence comment below and #22).
  #[must_use = "`None` means no group with this id is hosted — nothing was proposed"]
  pub fn rollback_merge<L, S>(
    &mut self,
    target: &G,
    now: impl Into<Now>,
    log: &mut L,
    // Vestigial, as on the whole propose family: kept so the delegators thread `&stable`.
    _stable: &S,
    source: &G,
  ) -> Option<Result<Index, MergeError<I>>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    if !self.groups.contains_key(target) {
      return None;
    }
    if source == target {
      return Some(Err(MergeError::SelfMerge));
    }
    if !self.groups.contains_key(source) {
      return Some(Err(MergeError::SourceMissing));
    }
    let tep = self.groups.get(target).expect("checked hosted above");
    if tep.is_poisoned() {
      return Some(Err(MergeError::Propose(ProposeError::Poisoned)));
    }
    if !tep.role().is_leader() {
      return Some(Err(MergeError::NotLeader {
        leader: tep.leader(),
      }));
    }
    if tep.merge_freeze_active() {
      return Some(Err(MergeError::AlreadyFrozen));
    }
    // The abort names an APPLIED freeze: its generation and claim are read off the frozen
    // source. A merely pending freeze refuses — unreadable claim, and a freeze that never
    // commits self-heals through truncation rather than through an abort.
    let sep = self.groups.get(source).expect("checked hosted above");
    if !sep.is_frozen() {
      return Some(Err(MergeError::NotFrozen));
    }
    let mut target_bytes = Vec::new();
    target.encode(&mut target_bytes);
    if sep.frozen_for().is_none_or(|t| *t != target_bytes) {
      // Only the claimed target may abort the merge — a foreign abort's relayed thaw would
      // move the source's counter under the claimed target's parked commit (the wedge the
      // claim exists to prevent).
      return Some(Err(MergeError::SourceClaimed));
    }
    let source_gen_after = sep.shape_gen();
    let mut source_bytes = Vec::new();
    source.encode(&mut source_bytes);
    // THE LINEAGE-SERIALIZATION FENCE — the abort's proposer leg, completing the trio with
    // `commit_merge` and `prepare_merge`. `target_gen_after` is minted below from the target's LIVE
    // `shape_gen`, and the abort's apply-time guard records the source's `abandoned` thaw obligation
    // ONLY at exactly that mint (the strict `== shape_gen + 1` arm). A stale mint — a lineage move
    // that applied on the target's own log between this propose and the abort's apply — makes the
    // abort SILENTLY no-op WITHOUT recording `abandoned`, stranding the aborted source frozen forever.
    //
    // The full PROPOSERS × MOVES matrix. The moves are the CLOSED set of apply-time `shape_gen`
    // writers (every `self.split.shape_gen =` site — Split ×2, PrepareMerge-freeze, CommitMerge-absorb,
    // RollbackMerge-abort, RollbackMerge-thaw, snapshot-install; ConfChange writes no `shape_gen`).
    // The strict lineage-MINTING proposers each mint from a live counter and admit only at the mint:
    //   commit_merge  (target absorb) — gated: Split, RollbackMerge-abort, CommitMerge/pending, freeze,
    //                                   ConfChange (its absorb compares voter/read state)
    //   rollback_merge (target abort) — gated HERE (this fence)
    //   prepare_merge (source freeze) — the freeze fold is MONOTONE-MAX, not a stale-aborting guard,
    //                                   so it fences only Split (counter collision), never an abort
    //   propose_split                 — self-gates (it IS the split)
    // Against THIS proposer (the target abort), each move is fenced / raced / exempt:
    //   PrepareMerge-freeze         → a freezing target is refused `AlreadyFrozen` above — a frozen
    //                                 target cannot abort at all (its own dissolution outranks)
    //   RollbackMerge source-thaw   → only ever on a FROZEN group → `AlreadyFrozen` above (as above;
    //                                 `rollback_in_flight` is set for thaws too but never reaches here)
    //   RollbackMerge target-abort  → `rollback_in_flight` → FENCE `RollbackInFlight` (below). THE
    //                                 FAN-IN STRAND: two sources frozen into one target, abort A
    //                                 un-drained then abort B mint the SAME gen; A applies + records
    //                                 `abandoned[A]`, B stale-no-ops without `abandoned[B]` → B stranded.
    //   Split                       → `split_in_flight` → FENCE `SplitInFlight` (below): an appended-
    //                                 unapplied `Split` applies first, bumps `shape_gen`, stales the abort.
    //   CommitMerge-absorb          → `commit_merge_in_flight`/`pending_merge` → SAME-MERGE-EXACT race
    //                                 (the match below): racing an in-flight or PARKED commit of the
    //                                 SAME merge is this verb's whole PURPOSE (#22), so it is allowed;
    //                                 a CROSS-source commit is FENCED `AlreadyPending` (else B's abort
    //                                 lands at A's `k+1`, A absorbs, B strands frozen). The PARKED
    //                                 source is compared in memory (`pending_merge`); the IN-FLIGHT
    //                                 one is DECODED from the `CommitMerge` at `pending_commit_index`
    //                                 (`pending_commit_source`, fail-closed to a defer on a cold or
    //                                 undecodable read). Both mint from the shared base and the
    //                                 target's own log totally-orders them to ONE winner — landing below
    //                                 the commit kills it at the commit's OWN guard (parks never form),
    //                                 landing after a parked commit un-parks every replica ABORTED off
    //                                 that one coordinate. Fencing the SAME merge would deadlock the
    //                                 release valve (a parked commit that could never complete could
    //                                 then never be aborted).
    //   ConfChange                  → writes no `shape_gen`, AND the abort compares no voter/read state
    //                                 (unlike the absorb) → irrelevant here, no gate.
    //   snapshot-install            → monotone-max on the same lineage that already accounts for any
    //                                 committed abort → cannot regress the mint → no gate.
    // Both fences are TRANSIENT and self-clearing (`> applied`): re-propose once the reshaping applies.
    if tep.split_in_flight() {
      return Some(Err(MergeError::SplitInFlight));
    }
    if tep.rollback_in_flight() {
      return Some(Err(MergeError::RollbackInFlight));
    }
    // THE COMMIT RACE IS SAME-MERGE-EXACT. Racing an in-flight/parked `CommitMerge` at k+1 is this
    // verb's purpose (#22) — but ONLY for THIS merge. A CROSS-source commit (fan-in: T committing
    // source A while B is aborted) would land B's abort at A's k+1: `merge_abort_window` reads a
    // different-source rollback there as `Closed`, A absorbs and bumps the lineage, and B's abort
    // then stale-no-ops WITHOUT recording `abandoned[B]` — B stranded, and A's release valve
    // consumed. So allow the race ONLY when the racing commit names this exact
    // `(source, source_gen_after)`: a PARKED commit exposes it in memory (`pending_merge`); an
    // IN-FLIGHT one not yet parked is DECODED off the `CommitMerge` at `pending_commit_index`
    // (`pending_commit_source`, whose cold/undecodable read fails closed to a defer). A commit of
    // any OTHER source — or one this abort cannot read — is refused `AlreadyPending`.
    match tep.pending_merge() {
      Some(park)
        if park.source_bytes().as_ref() == source_bytes.as_slice()
          && park.source_gen_after() == source_gen_after => {}
      Some(_) => return Some(Err(MergeError::AlreadyPending)),
      // In flight but not yet parked: no in-memory park to compare, so DECODE the appended
      // `CommitMerge`'s source off the log at `pending_commit_index`. Same source → race it (the
      // #22 in-flight race); any other source, or a read/decode the abort cannot resolve, → defer.
      None if tep.commit_merge_in_flight() => match tep.pending_commit_source(log) {
        Some((in_flight_source, in_flight_gen))
          if in_flight_source.as_ref() == source_bytes.as_slice()
            && in_flight_gen == source_gen_after => {}
        _ => return Some(Err(MergeError::AlreadyPending)),
      },
      None => {}
    }
    // The mint reads the target's live counter, now serialized behind the fences above (the commit
    // race SAME-MERGE-EXACT per the match above). It stops strictly below the reserved `MERGED_FLOOR`
    // terminal — at the ceiling the abort is refused here, before any append.
    let Some(target_gen_after) = next_lineage(tep.shape_gen()) else {
      return Some(Err(MergeError::LineageExhausted));
    };
    let payload = RollbackMergePayload::abort(
      Bytes::from(source_bytes),
      source_gen_after,
      target_gen_after,
    );
    let mut buf = Vec::new();
    crate::wire::encode_rollback_merge_payload(&payload, &mut buf);
    let ep = self.groups.get_mut(target).expect("checked hosted above");
    let result = ep
      .propose_merge_entry(now, log, EntryKind::RollbackMerge, Bytes::from(buf))
      .map_err(MergeError::Propose);
    self.mark_dirty(target);
    Some(result)
  }

  /// Propose the SOURCE-side thaw on `source` — the relay leg of a committed target-side abort,
  /// an INTERNAL helper driven ONLY by [`service_merge_applies`](Self::service_merge_applies) from
  /// a target's durable `abandoned` obligation. It is deliberately NOT public and has no coordinator
  /// delegator: the thaw is fully service-driven, so there is no external path that could move a
  /// frozen source's counter out from under a target that never abandoned it (the #22 cross-log
  /// race). Belt-and-suspenders, the invariant is also baked in below: the append is REFUSED unless
  /// the `claimed_by` target hosts a matching `abandoned` obligation for `(source, expected_gen)`,
  /// so `unfreeze(source) ⟹ ∃ committed target-abort(source, expected_gen)` holds structurally.
  /// Retirement is DELIVERY-BASED, not append-based: the accept arm APPENDS the thaw
  /// but returns a non-terminal `Ok`, and the relay is retired only once the source lineage is
  /// OBSERVED past the freeze (`StaleThaw`, `seen > expected`) — a thaw appended then truncated by a
  /// leadership loss leaves `seen == expected`, so the relay is RETAINED and a new source leader
  /// re-appends until it commits. The terminal-dedupe (`StaleThaw` and the never-frozen/foreign-claim
  /// verdicts) is checked BEFORE leadership, so EVERY host retires on the observed advance — including
  /// a follower that never leads (closing the stale-relay-forever hazard). Other verdicts retain and
  /// requeue: a source still catching up answers `SourceBehindFreeze` (INCLUDING a committed-but-
  /// unapplied freeze, `is_frozen() == false` yet not thawed); a non-leader replica `NotLeader`. The
  /// accept arm is IDEMPOTENT — a thaw already appended-and-unapplied on this leader RETAINS without
  /// re-appending. `None` if no group `source` is hosted.
  ///
  /// THE INCARNATION GATE (`expected_gen`): because a relay is RETAINED and requeued across
  /// source-leader churn (a `NotLeader`/`None` outcome), it can survive while another host lands
  /// the original thaw and the same source→target pair FREEZES AGAIN at a new generation. Minting
  /// the thaw from the source's live `shape_gen` would then let the stale relay thaw that new
  /// freeze — one with no matching target-side abort — aborting the new merge out of order (or
  /// moving the source past the new parked commit's expected generation, which the service path
  /// only debug-asserts before wedging). So the thaw is bound to the freeze generation the abort
  /// abandoned: it appends only when the source's current lineage EQUALS `expected_gen` (`<` is a
  /// source leader still SHORT of the abandoned freeze — a committed-but-unapplied freeze, or an
  /// older one its apply has not rolled through yet — `SourceBehindFreeze`, retried while a freeze
  /// is active; `>` is a source already advanced past this incarnation — `StaleThaw`, terminally
  /// dropped). The `frozen_for == claimed_by` claim identifies the source→target PAIR; `expected_gen`
  /// identifies the INCARNATION within it.
  #[must_use = "`None` means no group with this id is hosted — nothing was proposed"]
  fn propose_merge_unfreeze<L, S>(
    &mut self,
    source: &G,
    now: impl Into<Now>,
    log: &mut L,
    // Vestigial, as on the whole propose family: kept so the service threads `&stable`.
    _stable: &S,
    claimed_by: &G,
    expected_gen: u64,
  ) -> Option<Result<Index, MergeError<I>>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let ep = self.groups.get(source)?;
    if ep.is_poisoned() {
      return Some(Err(MergeError::Propose(ProposeError::Poisoned)));
    }
    // EXHAUSTIVE relay classification over every frozen-source state this (source, claimed_by,
    // expected_gen) relay can observe. `seen` is the source's live lineage; `expected_gen` is the
    // freeze incarnation the abort abandoned. The whole prefix is LEADERSHIP-INDEPENDENT — it runs
    // identically on a leader or a follower — and leadership is required ONLY to append the thaw or
    // read the claim, LAST. INCARNATION is decided FIRST (a pure `shape_gen` compare, readable
    // without the claim, which only an APPLIED freeze exposes):
    //
    //   poisoned                                                -> Poisoned           (terminal)
    //   seen >  expected                                        -> StaleThaw          (terminal)
    //   unhosted (no endpoint)                                  -> None               (transient)
    //   seen <  expected & a freeze is active (pending|applied) -> SourceBehindFreeze  (transient)
    //   seen <  expected & no freeze active                     -> NotFrozen           (terminal)
    //   seen == expected & not frozen-applied                   -> NotFrozen           (terminal)
    //   -- require leadership from here --
    //   not leader                                              -> NotLeader          (transient)
    //   seen == expected & frozen-applied & claim mismatch      -> SourceClaimed       (terminal)
    //   seen == expected & frozen-applied & claim match         -> APPEND, then RETAIN (non-terminal)
    //
    // Terminal-dedupe precedes the leadership check so retirement is DELIVERY-BASED, not
    // append-based: a relay is retired ONLY by observing the source past the frozen incarnation
    // (`seen > expected` — the thaw committed+applied, or a re-freeze moved past). Every host that
    // observes the delivered thaw drops its own relay, INCLUDING a follower that never leads (the
    // stale-relay-forever hazard when `NotLeader` shadowed the dedupe); and the accept arm's append
    // is only an ATTEMPT — a thaw appended then truncated leaves `seen == expected`, so the relay is
    // RETAINED and a new source leader re-appends until it commits.
    //
    // A source LEADER always holds the committed freeze (a leader has every committed entry), so
    // while its apply trails `expected` it is freeze-active (`merge_freeze_active`) at an EARLIER
    // incarnation and WILL reach `expected`; reading the freeze-pending signal keeps a
    // committed-but-unapplied leader (`freeze_pending` set, `is_frozen() == false`, `seen < expected`)
    // transient `SourceBehindFreeze` rather than dropping it into a permanent frozen-source wedge.
    let seen = ep.shape_gen();
    if seen > expected_gen {
      // THE RETIREMENT. Advanced past the abandoned incarnation — the thaw is durably delivered
      // (committed+applied) or the source re-froze the SAME pair for a fresh merge. A spent
      // authorization, terminal even with a new freeze active now (thawing that one would abort the
      // fresh merge out of order). Leadership-independent: a follower that applied the committed
      // thaw retires its relay here without ever leading.
      return Some(Err(MergeError::StaleThaw {
        expected: expected_gen,
        seen,
      }));
    }
    if seen < expected_gen {
      if ep.merge_freeze_active() {
        // Behind the abandoned incarnation with a freeze active: a committed-but-unapplied freeze
        // at `expected` (the wedge above), or an OLDER freeze the apply drain rolls through on the
        // way to `expected`. The source will catch up — retain and retry (transient).
        return Some(Err(MergeError::SourceBehindFreeze {
          expected: expected_gen,
          seen,
        }));
      }
      // Behind with NO freeze pending and none applied: genuinely nothing to thaw. Unreachable for
      // a source leader holding the committed freeze; a defensive terminal drop, recovered by
      // re-proposing the target-side abort.
      return Some(Err(MergeError::NotFrozen));
    }
    // seen == expected_gen: the exact abandoned incarnation. `shape_gen` reaches `expected` ONLY by
    // applying that freeze, so a group AT this lineage that is NOT frozen-applied has already
    // thawed it (or never froze) — terminal `NotFrozen`, never a catch-up (a re-freeze mints
    // strictly above `expected`, landing in the `seen > expected` arm).
    if !ep.is_frozen() {
      return Some(Err(MergeError::NotFrozen));
    }
    // Frozen-applied at exactly `expected`. Only a leader appends the thaw or reads the claim as
    // authoritative — so the leadership gate sits HERE, after the terminal-dedupe above. A follower
    // frozen at the exact incarnation retains (transient) and later retires by OBSERVING the leader's
    // committed thaw (the `seen > expected` arm), never by leading.
    if !ep.role().is_leader() {
      return Some(Err(MergeError::NotLeader {
        leader: ep.leader(),
      }));
    }
    // The claim is authoritative for THIS incarnation. A claim mismatch at an EARLIER incarnation
    // fell into `SourceBehindFreeze` above (a catch-up, not a foreign claim on the abandoned
    // freeze), so this compares the right generation's claim.
    let mut claimed_bytes = Vec::new();
    claimed_by.encode(&mut claimed_bytes);
    if ep.frozen_for().is_none_or(|t| *t != claimed_bytes) {
      // The exact incarnation is claimed by a DIFFERENT target — a relay riding a foreign target's
      // abort must not thaw it; the claimed target's parked commit gates on this counter staying
      // put.
      return Some(Err(MergeError::SourceClaimed));
    }
    // THE DERIVED-FROM-ABORT GATE (structural safety): a source thaw is legal ONLY as the downstream
    // consequence of a committed target-side abort. REQUIRE the claimed target to host a matching
    // `abandoned` obligation for exactly `(source, expected_gen)` before appending; absent it, refuse
    // with NO append — so a frozen source's counter can never move out from under a target that never
    // abandoned it (the #22 cross-log race). The source-side claim above proves the freeze names this
    // target; this proves the target OWES this freeze's thaw — the two legs together make the
    // invariant `unfreeze(source) ⟹ ∃ committed target-abort(source, expected_gen)` intrinsic.
    // Structurally satisfied whenever the container service issues the drive (it derives the call FROM
    // this obligation), so this never fires in production; the belt to the private-only helper.
    let mut source_key = Vec::new();
    source.encode(&mut source_key);
    let source_key = Bytes::from(source_key);
    if self
      .groups
      .get(claimed_by)
      .is_none_or(|t| !t.abandoned_matches(&source_key, expected_gen))
    {
      return Some(Err(MergeError::UnbackedThaw));
    }
    // ACCEPT: frozen at exactly `expected`, claimed by this relay's target, and this host leads. The
    // append is a BEST-EFFORT ATTEMPT, NOT the retirement — this returns a non-terminal `Ok` and the
    // relay is retained until the source lineage is OBSERVED past the freeze (the `seen > expected`
    // arm). IDEMPOTENT: if a thaw is already appended-and-unapplied on this leader, RETAIN and wait
    // rather than append a duplicate every crank until the first commits.
    if let Some(idx) = ep.thaw_in_flight() {
      return Some(Ok(idx));
    }
    // The thaw is minted at `expected_gen + 1` — written against the BOUND incarnation, not the
    // live counter — so it advances exactly the abandoned freeze's generation. It stops strictly
    // below the reserved `MERGED_FLOOR` terminal: a freeze at the ceiling leaves the source frozen
    // (fail-closed) rather than wrapping the counter past the terminal — an unreachable boundary
    // (the freeze that set `expected_gen` already consumed the log index that would exhaust first).
    let Some(source_gen_after) = next_lineage(expected_gen) else {
      return Some(Err(MergeError::LineageExhausted));
    };
    let payload = RollbackMergePayload::unfreeze(source_gen_after);
    let mut buf = Vec::new();
    crate::wire::encode_rollback_merge_payload(&payload, &mut buf);
    let ep = self.groups.get_mut(source).expect("checked hosted above");
    let result = ep
      .propose_merge_entry(now, log, EntryKind::RollbackMerge, Bytes::from(buf))
      .map_err(MergeError::Propose);
    if let Ok(index) = &result {
      ep.note_thaw_appended(*index);
    }
    self.mark_dirty(source);
    Some(result)
  }

  /// Mint a source's OWN thaw for a TERMINALLY-FLOORED, no-longer-hosted target — the SECOND
  /// legitimate thaw derivation. The first ([`propose_merge_unfreeze`](Self::propose_merge_unfreeze))
  /// rides a committed TARGET-side abort; this one rides the target's DEATH. Both mint the same
  /// unfreeze entry on the source's own log with the same incarnation discipline — leader-only,
  /// bound to the FREEZE generation (never the blind live counter), `thaw_in_flight`-idempotent —
  /// and differ ONLY in the authorization gate: `propose_merge_unfreeze`'s derived-from-abort gate
  /// ([`UnbackedThaw`](MergeError::UnbackedThaw)) is DELIBERATELY LEFT UNWEAKENED; the dead-target
  /// authorization is a wholly SEPARATE private path with its own gate, driven only from the
  /// service arm where the target's unhosted-and-terminally-floored fact is read.
  ///
  /// The caller supplies the decoded dead `target` and has already established it is unhosted here
  /// and reads [`MERGED_FLOOR`](crate::MERGED_FLOOR). This re-checks everything readable without the
  /// floor seam: the source is frozen-applied (so its `shape_gen` IS the freeze generation to bind
  /// to), its claim names `target`, this host leads, and — the FAIL-SAFE BELT — no hosted target's
  /// parked commit still names the source ([`SourceAbsorbParked`](MergeError::SourceAbsorbParked)): a
  /// live park means an absorb of this source may still be resolving locally, so moving the counter
  /// underneath it is refused. Private and service-driven only (no coordinator delegator): no
  /// external path can forge a dead-target thaw. `None` if no group `source` is hosted.
  #[must_use = "`None` means no group with this id is hosted — nothing was proposed"]
  fn propose_dead_target_thaw<L, S>(
    &mut self,
    source: &G,
    now: impl Into<Now>,
    log: &mut L,
    // Vestigial, as on the whole propose family: kept so the service threads `&stable`.
    _stable: &S,
    target: &G,
  ) -> Option<Result<Index, MergeError<I>>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let ep = self.groups.get(source)?;
    if ep.is_poisoned() {
      return Some(Err(MergeError::Propose(ProposeError::Poisoned)));
    }
    // Frozen-APPLIED is the incarnation anchor: `shape_gen` reaches the freeze generation ONLY by
    // applying that freeze, so a frozen-applied source sits AT it — bind the mint to it, never the
    // live counter. A source not frozen-applied has nothing to thaw.
    if !ep.is_frozen() {
      return Some(Err(MergeError::NotFrozen));
    }
    let freeze_gen = ep.shape_gen();
    // Only a leader appends the thaw (mirrors the relay). A follower waits and later retires by
    // OBSERVING the committed thaw its leader minted.
    if !ep.role().is_leader() {
      return Some(Err(MergeError::NotLeader {
        leader: ep.leader(),
      }));
    }
    // The freeze CLAIM must name exactly this dead target — the claim is authoritative for the
    // incarnation, and a thaw riding a foreign claim must never move this source (mirrors
    // `propose_merge_unfreeze`'s `SourceClaimed` leg).
    let mut target_bytes = Vec::new();
    target.encode(&mut target_bytes);
    if ep.frozen_for().is_none_or(|t| *t != target_bytes) {
      return Some(Err(MergeError::SourceClaimed));
    }
    // THE FAIL-SAFE BELT (the same `park_names_source` read the husk dissolve and teardown gate
    // use): refuse while any hosted target's parked commit still names this source. A live park may
    // be resolving an absorb of this source on this host RIGHT NOW, and the park gates on the
    // source counter staying put — minting a thaw underneath it would race that resolution.
    if self.park_names_source(source) {
      return Some(Err(MergeError::SourceAbsorbParked));
    }
    // IDEMPOTENT: a thaw already appended-and-unapplied retains its index rather than piling on a
    // duplicate every crank (the twin of the relay's `thaw_in_flight` guard).
    if let Some(idx) = ep.thaw_in_flight() {
      return Some(Ok(idx));
    }
    // Minted at `freeze_gen + 1` against the BOUND freeze incarnation, stopping strictly below the
    // reserved `MERGED_FLOOR` terminal (fail-closed at the ceiling) — exactly as the relay does.
    let Some(source_gen_after) = next_lineage(freeze_gen) else {
      return Some(Err(MergeError::LineageExhausted));
    };
    let payload = RollbackMergePayload::unfreeze(source_gen_after);
    let mut buf = Vec::new();
    crate::wire::encode_rollback_merge_payload(&payload, &mut buf);
    let ep = self.groups.get_mut(source).expect("checked hosted above");
    let result = ep
      .propose_merge_entry(now, log, EntryKind::RollbackMerge, Bytes::from(buf))
      .map_err(MergeError::Propose);
    if let Ok(index) = &result {
      ep.note_thaw_appended(*index);
    }
    self.mark_dirty(source);
    Some(result)
  }

  /// Append a `ThawDischarged` WITNESS on `target`'s own log — the discharge-observing leg of the
  /// thaw pass, driven ONLY by [`service_merge_applies`](Self::service_merge_applies) when the
  /// obligation holder LEADS and holds a GLOBALLY-valid proof the named source is discharged (its
  /// mint predicate lives at the call site, where the source's counter/floor is read). Not public and
  /// has no coordinator delegator: like the source-side thaw, the witness is fully service-driven, so
  /// no external path can forge one. The append rides `propose_merge_entry` with NO freeze gate — a
  /// holder may itself be a frozen source and must still discharge its obligations mid-freeze, and the
  /// witness is FSM-non-mutating so it is legal above a surviving freeze. IDEMPOTENT per obligation
  /// holder: a witness already appended-and-unapplied on this leader RETAINS its index rather than
  /// piling on a duplicate every crank (the exact twin of the thaw relay's `thaw_in_flight` guard).
  /// `None` if no group `target` is hosted; a non-leader falls out of `propose_merge_entry` as
  /// `NotLeader` (the outcome is discarded — the discharge is decided by the committed apply, not this
  /// append).
  #[must_use = "`None` means no group with this id is hosted — nothing was proposed"]
  fn propose_thaw_witness<L>(
    &mut self,
    target: &G,
    source_bytes: &Bytes,
    generation: u64,
    now: impl Into<Now>,
    log: &mut L,
  ) -> Option<Result<Index, MergeError<I>>>
  where
    L: LogStore,
  {
    let ep = self.groups.get(target)?;
    if let Some(idx) = ep.witness_in_flight() {
      return Some(Ok(idx));
    }
    let payload = ThawDischargedPayload::new(source_bytes.clone(), generation);
    let mut buf = Vec::new();
    crate::wire::encode_thaw_discharged_payload(&payload, &mut buf);
    let ep = self.groups.get_mut(target).expect("checked hosted above");
    let result = ep
      .propose_merge_entry(now, log, EntryKind::ThawDischarged, Bytes::from(buf))
      .map_err(MergeError::Propose);
    if let Ok(index) = &result {
      ep.note_witness_appended(*index);
    }
    self.mark_dirty(target);
    Some(result)
  }

  /// Resolve every parked `CommitMerge` that the TARGET's log and local facts now decide —
  /// called ONCE PER CRANK by every driver, after the per-group apply drains.
  ///
  /// The ABORT side is decided by target-log order alone: the park's **abort window** is the
  /// single committed coordinate `k + 1` right after the parked entry. Until something commits
  /// there EVERY arm holds (resolving while the coordinate is undecided would race an abort
  /// landing at it — one host absorbed, another aborted, committed divergence; the proven
  /// cross-log race, one log removed), and the target LEADER seals a quiet window with a no-op
  /// so it cannot stay open forever. A committed matching abort there un-parks ABORTED on
  /// every replica; anything else closes the window for good — from then on the park waits
  /// only on the local source gate. NEVER a live read of the source's mutable state on the
  /// abort side: the source's counter cannot move while the park stands (the thaw is relayed
  /// by the abort itself, which this very park blocks above `k`).
  ///
  /// With the window closed the arms are: **resolve** (source hosted, frozen at the expected
  /// gen FOR THIS TARGET, applied past the boundary, and the target free to stage its absorb
  /// capture) — the source endpoint is removed, its state machine absorbed, and the forced
  /// capture staged through `stores` so the union's durability anchor rides the SAME barrier
  /// as the driver's floor/teardown; **abort** (the frozen source's claim names a DIFFERENT
  /// target — log-pinned for the freeze's whole generation, so identical on every replica; or
  /// source absent WITH the terminal floor — a replayed duplicate, the union already here);
  /// **keep parked** otherwise (a behind source keeps replicating while frozen, entirely
  /// independent of this target; an absent source without the floor waits for the resolved
  /// quorum's post-merge snapshot, whose install supersedes the park).
  ///
  /// Liveness needs exactly one guarantee from the driver: a parked target is never
  /// quiesce-eligible, so this service keeps being reached. The park waits on a local monotone
  /// condition (the local source's applied reaching a fixed committed index); the abort valve
  /// races the seal for the window, and once sealed the merge is as decided as any committed
  /// entry — a source wedged below its boundary forever is the group-is-dead liveness class,
  /// recovered at the embedder's catalog like any dead group.
  ///
  /// Returns the crank's resolutions for the DRIVER to fold: floor + teardown for `Merged`,
  /// nothing for `Aborted`.
  pub fn service_merge_applies<L, S, St>(
    &mut self,
    now: impl Into<Now>,
    stores: &mut St,
  ) -> Vec<MergeResolution<G>>
  where
    St: crate::GroupStores<G, L, S> + FloorStore<G>,
    L: LogStore,
    S: StableStore<NodeId = I>,
    F::Snapshot: Data,
  {
    let now: Now = now.into();
    let mut resolutions = Vec::new();
    let parked: Vec<G> = self
      .groups
      .iter()
      .filter(|(_, ep)| ep.pending_merge().is_some())
      .map(|(gid, _)| gid.cheap_clone())
      .collect();
    for tgid in parked {
      let Some(tep) = self.groups.get(&tgid) else {
        continue;
      };
      let Some(pending) = tep.pending_merge() else {
        continue;
      };
      let expected = pending.source_gen_after();
      let boundary = pending.freeze_index();
      let freeze_term = pending.freeze_term();
      // The park's OWN coordinate (the `CommitMerge`'s index), distinct from the SOURCE's freeze
      // index above: it keys this target's capture fence and is the boundary a cure blob must
      // cover, so it is what an observer of the hold needs.
      let park_at = pending.at();
      let source_bytes = pending.source_bytes();
      let Ok(source) = G::decode_exact(source_bytes) else {
        // A committed source id that does not decode as G is committed-corrupt — the split
        // relay's own decode class, fail-stopped identically.
        if let Some(tep) = self.groups.get_mut(&tgid) {
          tep.poison(PoisonReason::MergeDecode);
        }
        self.note_if_poisoned(&tgid);
        continue;
      };
      let mut locally_unresolvable = false;
      enum Verdict {
        Resolve,
        Abort,
        Wait,
        /// The source sits below its freeze generation: wait, but first try THE IDENTITY LEG —
        /// if the local source log contains `(freeze_index, freeze_term)`, advance its
        /// commit/apply to the boundary so a later crank resolves.
        AdvanceSource,
      }
      // THE ABORT WINDOW, first and unconditionally: no arm below may fire until the target's
      // own log has decided the `k + 1` coordinate (see the method doc). The read is of this
      // group's committed content — identical bytes on every replica.
      let window = match stores.stores(&tgid) {
        Some((log, stable)) => {
          let w = tep.merge_abort_window(&*log);
          if w == MergeWindow::Open {
            // Seal a quiet window (leader-only, idempotent per park), flushing the fan-out
            // inline so sealing rides this crank rather than the next heartbeat cadence.
            if let Some(tep) = self.groups.get_mut(&tgid)
              && tep.ensure_merge_seal(now, log)
            {
              tep.flush_appends(now, &*log, &*stable);
              self.mark_dirty(&tgid);
            }
          }
          w
        }
        None => continue,
      };
      // LATCH THE CLOSED VERDICT before the arms run. Hoisted to here rather than into the
      // `Closed` arm below because that arm holds an immutable `self.groups` borrow for the whole
      // source lookup; taking the mutable endpoint borrow here is borrow-clean and reads the same
      // window this crank just computed. Monotone while this park lives, and it dies with the park
      // at every resolution site by construction — the flag is a field of the park itself.
      if window == MergeWindow::Closed
        && let Some(tep) = self.groups.get_mut(&tgid)
      {
        tep.latch_merge_window_closed();
      }
      let verdict = match window {
        MergeWindow::Abort => Verdict::Abort,
        MergeWindow::Open | MergeWindow::Stall => Verdict::Wait,
        MergeWindow::Closed => match self.groups.get(&source) {
          Some(sep) => {
            let seen = sep.shape_gen();
            if seen == expected {
              // gen == expected implies the freeze applied (that apply is the counter's only
              // path to this value) — frozen and applied-past-boundary ride along; the explicit
              // checks document the gate and catch a broken counter in debug.
              debug_assert!(sep.is_frozen() && sep.applied_index() >= boundary);
              let mut tgid_bytes = Vec::new();
              tgid.encode(&mut tgid_bytes);
              if sep.frozen_for().is_none_or(|t| *t != tgid_bytes) {
                // The freeze is a CLAIM by exactly one target, pinned for this whole
                // generation on the source's log: a park under a foreign claim can never
                // absorb, and every replica sees the same claim — abort deterministically
                // (two targets naming one frozen source is the same committed-divergence
                // class as the cross-log rollback, and the claim is what closes it).
                Verdict::Abort
              } else {
                // Dissolve. No resolve-last discipline is needed here: `commit_merge` only
                // proposes the `CommitMerge` once EVERY source voter has matched the freeze
                // boundary (the admission barrier), so a committed `CommitMerge` certifies the
                // whole voter set holds the freeze in its own log. Tearing down this host's
                // source can therefore never orphan a straggler — a peer cut off from the
                // source leader self-advances on the freeze identity (the `AdvanceSource` leg)
                // from its own log, needing neither a live source leader nor a quorum.
                Verdict::Resolve
              }
            } else {
              // Behind the expectation: still catching up (its own replication keeps running
              // while frozen). PAST it is structurally unreachable while parked — the thaw is
              // relayed only by the abort entry this park blocks above `k`, and a replayed
              // commit against an already-moved counter no-ops at its own lineage guard
              // before ever parking — so a moved counter here is a broken-counter bug, not an
              // abort signal: hold rather than diverge. Waiting is not always enough, though:
              // replication only catches this replica up WHILE THE SOURCE HAS A LEADER, and
              // the boundary MATCH the pace leg waited for is not commit KNOWLEDGE — a lost
              // final heartbeat legally strands a source follower with the freeze in its log
              // but `commit` below it, right as the last absorb consumes the source's quorum
              // (leaderless, under-hosted, unelectable). The identity leg is that shape's only
              // exit, and it is sound on log identity alone (see
              // `advance_commit_on_freeze_identity` for the argument).
              debug_assert!(
                seen < expected,
                "a parked commit observed the source PAST its freeze generation"
              );
              Verdict::AdvanceSource
            }
          }
          // Absent WITH the terminal floor: this host already absorbed the source (the floor
          // lands in the same barrier as an absorb) — the commit is a replayed duplicate and
          // the union is already in the target; no-op past it. Absent WITHOUT the floor: this
          // replica never held the source (lifecycle churn tore it down, or the replica
          // joined after the source dissolved) and the union is NOT materializable here — it
          // must WAIT for the resolved quorum's post-merge snapshot, whose install supersedes
          // the park (the forced capture compacts the leader through the absorb, so a parked
          // straggler is structurally on the snapshot path). Aborting instead would skip the
          // union on this replica alone — silent, permanent divergence from every replica
          // that absorbed.
          None => {
            if stores.floor(&source) == crate::MERGED_FLOOR {
              Verdict::Abort
            } else {
              locally_unresolvable = true;
              Verdict::Wait
            }
          }
        },
      };
      // THE CURE-ADVERTISEMENT HINT, re-derived every crank this park is examined: only the
      // locally-unresolvable Wait above (source unhosted, floor non-terminal — no local fold can
      // ever land) may set it, and only with both gate legs clear: no fork durability barrier
      // stands (an adopting install's restart takes the Compact arm at its boundary, destroying
      // a staged fork's only replay derivation), and no abort obligation names a hosted-and-
      // frozen source (the adopt's boundary clear would erase the only drive for that thaw —
      // exactly the host-local-proof clear the witness rules forbid). Every other verdict, and a
      // standing gate leg, clears it — the hint never outlives the shape that justified it, and
      // a parked replica applies nothing, so a clear gate leg cannot re-arm within the episode.
      if locally_unresolvable
        && let Some((tlog, _)) = stores.stores(&tgid)
        && let Some(tep) = self.groups.get_mut(&tgid)
      {
        tep.advance_crossing_scan(&*tlog);
        // The walk fail-stops on committed-corrupt content (a payload the parked drain could
        // never reach to poison itself) — latch it like every other in-service poison.
        if tep.is_poisoned() {
          self.note_if_poisoned(&tgid);
          continue;
        }
      }
      // The crossing leg is OUTCOME-BLIND by design: an adopt over a crossing whose source is
      // hosted here would leave that replica a live-voting husk of a lineage the blob absorbed
      // — or a stale no-op only the full apply machinery's lineage guard can classify, a
      // re-derivation no scan can soundly make — so ANY hosted crossing withholds the hint and
      // the park waits (its exit is the hosted replica's own lifecycle, or the propagated
      // terminal floor). A decode failure withholds too: fail-closed.
      let mut crossing_decode_corrupt = false;
      let hosted_crossing: Option<G> = if locally_unresolvable {
        self.groups.get(&tgid).and_then(|t| {
          t.crossing_sources()
            .iter()
            .find_map(|b| match G::decode_exact(b.clone()) {
              Ok(g) if self.groups.contains_key(&g) => Some(g),
              // A committed source id that does not decode as G is committed-corrupt — the
              // resolver's own park-decode fail-stop class. Anything short of poisoning here
              // disagrees with the receipt edge's fail-closed read and loops the park through
              // advertise-then-refuse forever, shipping whole blobs at a wedge no cure can fix.
              Err(_) => {
                crossing_decode_corrupt = true;
                None
              }
              Ok(_) => None,
            })
        })
      } else {
        None
      };
      if crossing_decode_corrupt {
        if let Some(tep) = self.groups.get_mut(&tgid) {
          tep.poison(PoisonReason::MergeDecode);
        }
        self.note_if_poisoned(&tgid);
        continue;
      }
      let crossing_blocks = locally_unresolvable
        && self
          .groups
          .get(&tgid)
          .is_some_and(|t| !t.crossing_scan_current())
        || hosted_crossing.is_some();
      let advertise = locally_unresolvable
        && !crossing_blocks
        && self.groups.get(&tgid).is_some_and(|t| {
          // The walk must have reached this crank's committed frontier with every payload
          // decoded: an advertisement off a partial or corrupt walk would authorize an adopt
          // across entries never examined — fail-closed.
          t.crossing_scan_current() && !t.fork_barrier_standing()
        })
        && !self.obligation_names_hosted_unadvanced(&tgid);
      // ONE composed cause per crank: a hosted crossing outranks and SUPPRESSES the generic
      // unhosted-source signal (the actionable identity is the crossing's — its lifecycle is
      // what releases this wedge — carried with the PARK coordinate; the edge-dedup then holds
      // it to one emission per transition).
      if let Some(crossing) = hosted_crossing.as_ref() {
        self.note_merge_blocked(
          &tgid,
          crossing,
          park_at,
          MergeBlockedCause::CrossedHostedSource,
        );
      }
      if let Some(tep) = self.groups.get_mut(&tgid) {
        tep.note_merge_park_unresolvable(advertise);
      }
      match verdict {
        Verdict::Wait => {
          // Only the STRUCTURAL wait is signalled — and only when no hosted crossing outranks
          // it (the composed cause above carries the actionable identity then). The other
          // `Wait` — an abort window still undecided — is the merge's ordinary decision
          // latency and closes with the next committed coordinate.
          if locally_unresolvable && hosted_crossing.is_none() {
            self.note_merge_blocked(&tgid, &source, park_at, MergeBlockedCause::SourceUnhosted);
          }
        }
        Verdict::AdvanceSource => {
          self.note_merge_blocked(&tgid, &source, park_at, MergeBlockedCause::SourceBehind);
          // The committed CommitMerge proves `(boundary, freeze_term)` committed in the source
          // — its proposer stamped the pair from a source observed frozen-applied AT the
          // boundary — and log matching carries that identity: a local source log CONTAINING
          // the pair holds the committed freeze and its exact prefix, so raising its commit to
          // the boundary is the ordinary leader-to-follower knowledge transfer with the
          // (possibly dead) source leader's say-so riding this target's log instead. The
          // endpoint method verifies the pair against the LOCAL log and refuses everything
          // else — a divergent or short log keeps waiting; never an advance on index alone.
          // The freeze then applies through the normal drain and the NEXT crank resolves.
          if let Some((slog, _)) = stores.stores(&source)
            && let Some(sep) = self.groups.get_mut(&source)
          {
            if sep.advance_commit_on_freeze_identity(&*slog, boundary, freeze_term) {
              self.mark_dirty(&source);
            }
            // The identity read can poison the source (a fatal log-term fault) while
            // answering false — latch it like every other in-service fail-stop, or the
            // post-service drain has nothing to surface.
            self.note_if_poisoned(&source);
          }
        }
        Verdict::Abort => {
          if let Some(tep) = self.groups.get_mut(&tgid) {
            tep.resolve_pending_merge_aborted();
          }
          self.mark_dirty(&tgid);
          resolutions.push(MergeResolution::Aborted {
            source,
            target: tgid,
          });
        }
        Verdict::Resolve => {
          // The fence classification decides the arm's shape. `Hold` (a staged capture/install
          // draining within cranks, or a live freeze whose pinned claim the fold itself would
          // advance) keeps the park. `Defer` — only a REPLAY fence stands (a parked fork's
          // barrier, an undischarged abort obligation) — absorbs NOW and records the capture as
          // a debt: the fold is safe, only its compaction must wait for the fence, and holding
          // instead would wedge the park for the fence's whole embedder-timescale life (the
          // abort fence's clearing witness can even ride an entry the park itself keeps from
          // applying). `Clear` is the one-crank absorb + capture + floor + teardown barrier.
          let block = match self.groups.get(&tgid) {
            Some(tep) => tep.absorb_capture_block(),
            None => continue,
          };
          if block == crate::endpoint::AbsorbCaptureBlock::Hold {
            // A held park's cause is worth naming only when it is the FREEZE — a chained shape
            // (this target is itself a claimed source) that lifts on another group's protocol
            // timescale. The other `Hold` leg is a staged capture/install draining within cranks.
            if self
              .groups
              .get(&tgid)
              .and_then(|tep| tep.capture_fence_at(park_at))
              == Some(crate::endpoint::CaptureFence::Frozen)
            {
              self.note_merge_blocked(&tgid, &source, park_at, MergeBlockedCause::Frozen);
            }
            continue;
          }
          // THE RESIDUAL BELT, gated to LOCAL DRIVABILITY (see `owes_a_drivable_thaw`). HOLD the park
          // while the source still owes a thaw THIS replica can drive, so the thaw pass discharges it
          // FIRST — dissolving would drop the obligation and strand the upstream source frozen. A
          // dead-end obligation (owed target not hosted here) does NOT hold: a co-hosting replica
          // drives it, so absorbing here strands nothing. A parked target is never quiesce-eligible,
          // so a genuinely-held park keeps being reached until it resolves.
          if self.owes_a_drivable_thaw(&source) {
            continue;
          }
          // THE STAGED-FORK LEG. The obligation is HOST-LOCAL — a sibling whose staged child's
          // baseline already flushed sees none at all — so a freeze of this source can be proposed
          // and committed elsewhere while it still stands HERE, and the local propose-time refusal
          // never ran. Consuming the holder destroys the staged child's ONLY local derivation (the
          // `Split` entry, or the queued fork's blob once a rebaseline retired the entry), and an
          // under-replicated child plus a crash on this host would then lose it outright. So HOLD
          // the park. Unlike the abort fence — whose clearing witness can ride ABOVE the park,
          // which is why that one chains debts rather than waits — this clears when the DRIVER
          // reports the CHILD's own baseline durable, independent of anything the park blocks: the
          // hold is live, and the first crank after the release resolves. A parked target is never
          // quiesce-eligible, so the park keeps being reached until then.
          //
          // The ONE composition with no local release is a fork whose child id IS this target: the
          // fork waits on the occupant, the occupant is `MergeParked` on this very absorb, and the
          // absorb waits here. Signalling it is deliberate — before the hold this shape silently
          // DROPPED the split-away half at consumption, and a loud wedge an embedder can see is the
          // strictly better failure until a release protocol for it exists.
          if self
            .groups
            .get(&source)
            .is_some_and(Endpoint::fork_obligations_standing)
          {
            self.note_merge_blocked(&tgid, &source, park_at, MergeBlockedCause::ForkFence);
            continue;
          }
          // THE INHERITED-DEBT LEG. A capture debt is HOST-LOCAL — this replica's fences
          // deferred the capture while siblings captured cleanly — so a foreign-led freeze can
          // legally commit this source's consumption while its debt still stands HERE: the
          // propose-time `AlreadyPending` refusal ran on a debt-less replica. Consuming the
          // holder must not drop the held `Merged`s — the ONLY permission that terminally
          // floors each earlier absorbed source — and the debts cannot discharge in place: a
          // merge source is FROZEN, and a frozen endpoint never captures. So the whole chain
          // discharges INTO THIS ABSORB's own barrier — the forced capture covers the source's
          // state machine, which has carried every prior union since its absorb applied — and
          // a `Defer` INHERITS the chain onto this target's own minted debt instead of holding
          // the park: an abort fence's clearing witness can ride ABOVE the park, so holding
          // here would be a circular wait (the fence lifts only once the park resolves). A
          // committed-corrupt debt source id fail-stops the holder — the resolver's uniform
          // decode rule — validated for the WHOLE chain before anything is consumed.
          let mut inherited_sources: std::vec::Vec<G> = std::vec::Vec::new();
          {
            let mut corrupt = false;
            if let Some(sep) = self.groups.get(&source) {
              for m in sep.capture_debt_chain() {
                match G::decode_exact(m.source()) {
                  Ok(prior) => inherited_sources.push(prior),
                  Err(_) => {
                    corrupt = true;
                    break;
                  }
                }
              }
            }
            if corrupt {
              if let Some(sep) = self.groups.get_mut(&source) {
                sep.poison(PoisonReason::MergeDecode);
              }
              self.note_if_poisoned(&source);
              self.mark_dirty(&source);
              continue;
            }
          }
          // The β hold above proved the source owes no thaw; this absorb IS the merge resolving, so
          // it dissolves the source through the UNGATED inner teardown. The public gate's participant
          // refusals — the source is `Frozen`, and this target's own park names it (`SpokenFor`) —
          // all describe THIS in-flight merge and would wedge the absorb they exist to protect.
          // `None` (a source already gone) holds the park exactly as a busy target does.
          let Some(mut sep) = self.remove_group_inner(&source) else {
            continue;
          };
          let inherited_debts = sep.take_capture_debts();
          let fsm = sep.into_state_machine();
          let Some(tep) = self.groups.get_mut(&tgid) else {
            // Unreachable: `tgid` was iterated from `self.groups` and only `source` (a DISTINCT id)
            // was removed above. The assert pins the post-removal invariant a future refactor could
            // break — the source endpoint is already consumed, so any post-removal path that fails
            // to push a resolution strands the source's parked work (the capture-failed hole).
            debug_assert!(
              false,
              "the merge target vanished between iteration and resolution"
            );
            continue;
          };
          let merged = tep.resolve_pending_merge(fsm);
          // An FSM that refuses the absorb POISONED the target (deterministic on every
          // replica — MergeUnsupported is the SplitUnsupported class). Nothing was absorbed:
          // surfacing `Merged` here would have the driver floor the source terminally and
          // tear its stores down, destroying the union's only copy behind a fail-stop. Emit
          // `CaptureFailed` instead — the source endpoint is already CONSUMED, so the driver
          // must fail its stranded routing (its callers would hang forever) while PRESERVING
          // its stores and floor, and a restart re-parks against the restored source.
          if tep.is_poisoned() {
            self.mark_dirty(&tgid);
            // Latch for `poll_poisoned` alongside the typed resolution, as at every other
            // in-service fail-stop.
            self.note_if_poisoned(&tgid);
            resolutions.push(MergeResolution::CaptureFailed {
              source,
              target: tgid,
            });
            continue;
          }
          if block == crate::endpoint::AbsorbCaptureBlock::Defer {
            // A replay fence deferred the capture: the union lives in memory and the CONSUMED
            // source's intact stores remain its only restart derivation, so surface `Absorbed`
            // — the driver fails the source's stranded routing but PRESERVES its stores and
            // floor (the `CaptureFailed` half, minus the poison) — and hold the `Merged` as the
            // target's capture debt. The per-crank debt pass stages the capture once the fence
            // lifts (or adopts any other capture/install at-or-past the boundary) and only THEN
            // surfaces `Merged`, the floor + teardown permission. A crash meanwhile re-parks
            // against the restored source: the boundary's `CommitMerge` cannot have compacted
            // away, since compaction past it requires exactly the capture the debt still owes.
            if let Some(m) = merged
              && let Some(tep) = self.groups.get_mut(&tgid)
            {
              tep.mint_capture_debt(m);
              // The consumed source's own chain rides the minted debt: one covering capture
              // discharges them all (the fold just absorbed carries every prior union).
              tep.adopt_inherited_debts(inherited_debts);
              self.mark_dirty(&tgid);
              // The resolution rides ONLY the minted debt: an `Absorbed` with no debt behind it
              // would be a permanent orphan carrying none of the lifecycle fences.
              resolutions.push(MergeResolution::Absorbed {
                source,
                target: tgid,
              });
            } else {
              debug_assert!(false, "a defer without a foldable park");
              self.mark_dirty(&tgid);
            }
            continue;
          }
          // The absorb happened in memory; the union is durable ONLY once the forced capture
          // STAGES its snapshot/compaction. Emit `Merged` — the driver's permission to floor the
          // source terminally and drop its stores — solely on a staged capture. A `snapshot()` or
          // log fault poisons and stages nothing. A store gone between the window read and here
          // (unreachable through a stable seam within one crank) faults the same way.
          let staged = match stores.stores(&tgid) {
            Some((log, stable)) => self
              .groups
              .get_mut(&tgid)
              .is_some_and(|tep| tep.capture_absorb_snapshot(log, stable)),
            None => {
              if let Some(tep) = self.groups.get_mut(&tgid) {
                tep.poison(PoisonReason::SnapshotCapture);
              }
              false
            }
          };
          self.mark_dirty(&tgid);
          if staged {
            // The capture staged: the union is durable, so `Merged` may now surface. The event
            // rides ONLY this arm — a withheld resolution (a failed capture below) drains none.
            if let Some(m) = merged
              && let Some(tep) = self.groups.get_mut(&tgid)
            {
              tep.emit_merged(m);
            }
            resolutions.push(MergeResolution::Merged {
              source,
              target: tgid.cheap_clone(),
            });
            // The consumed source's debt chain discharges into the SAME staged barrier: the
            // capture covers the source's state machine, which has carried every prior union
            // since its absorb applied. Resolutions only — the holder that would have carried
            // the app-visible events is consumed (the `Retired` asymmetry).
            for prior in inherited_sources {
              resolutions.push(MergeResolution::Merged {
                source: prior,
                target: tgid.cheap_clone(),
              });
            }
          } else {
            // The capture faulted (or the stores vanished): the target is POISONED and no union
            // teardown is safe. The source endpoint is already CONSUMED, so its parked routing is
            // stranded — emit `CaptureFailed` so the driver fails those callers typed while
            // PRESERVING the source's stores and floor, and a restart re-parks against the restored
            // source rather than losing the union behind a floored, torn-down source. Inherited
            // debt records die un-surfaced here, and soundly: the preserved source stores
            // replay their own CommitMerge entries on restart and re-derive them.
            self.note_if_poisoned(&tgid);
            resolutions.push(MergeResolution::CaptureFailed {
              source,
              target: tgid,
            });
          }
        }
      }
    }
    // THE MERGE-ABORT THAW PASS: drive every hosted target's durable `abandoned` obligations to
    // thaw their named sources — the durable-derived mirror of the commit side above (both re-derive
    // resolution from committed endpoint state each crank, no volatile relay). The source rollback
    // is a DOWNSTREAM CONSEQUENCE of the target's committed abort, NEVER a source-local decision:
    // a frozen source with a live leader but NO committed target-abort naming it is never reached
    // here (nothing sets its target's obligation), so it stays frozen and waits. Each obligation is
    // driven and discharged INDEPENDENTLY (per source key), so a target that aborted a fan-in of
    // sources thaws every one of them — none is dropped.
    let abandoned_targets: Vec<G> = self
      .groups
      .iter()
      .filter(|(_, ep)| ep.has_abandoned())
      .map(|(gid, _)| gid.cheap_clone())
      .collect();
    for tgid in abandoned_targets {
      // Snapshot the target's obligations before mutating the container: the drive below re-borrows
      // `self` mutably, and a per-source discharge removes only its own key, so the snapshot stays a
      // faithful worklist for this crank.
      let obligations = match self.groups.get(&tgid) {
        Some(tep) => tep.abandoned_obligations(),
        None => continue,
      };
      for (source_bytes, expected, _abort_index) in obligations {
        let source_key = source_bytes.clone();
        let Ok(source) = G::decode_exact(source_bytes) else {
          // A committed source id that does not decode as G is committed-corrupt — the park
          // decode's fail-stop class. The target is poisoned; stop draining its obligations.
          if let Some(tep) = self.groups.get_mut(&tgid) {
            tep.poison(PoisonReason::MergeDecode);
          }
          self.note_if_poisoned(&tgid);
          break;
        };
        // DISCHARGE by OBSERVING the source, never by classifying the thaw's append outcome: the
        // abandoned freeze is gen `expected`, so a source advanced PAST it committed its unfreeze (or
        // re-froze past it) — the obligation is delivered. The LOCAL clear takes the hosted live
        // counter past `expected`, the PERSISTED engine lineage past it, or — for an UNHOSTED or a
        // hosted-but-UNFROZEN source — a removal floor that no longer admits `expected` (both persisted
        // legs OUTLIVE the source's teardown). Clearing lifts the target's compaction fence over the
        // abort entry, so it releases only once the source is proven past `expected` — crash-durable
        // via the abort entry's replay.
        //
        // WHY the floor leg is FENCED OFF a FROZEN hosted source. A hosted incarnation at a LOWER gen
        // than `expected` that is NOT frozen is a legal SQUATTER — the id was removed (floored past
        // `expected`) and recreated below that non-terminal floor at a fresh gen — and its live counter
        // must NOT SHADOW the durable proof that the NAMED (dead) incarnation is discharged; the floor
        // advanced past `expected` at the removal and floors are MONOTONE, so `!floor_admits` fires. But
        // a hosted source that is FROZEN is LIVE-and-owing whatever its generation: a recreated id can
        // refreeze at a gen the OLD removal floor still sits above, and that fresh freeze is a REAL
        // obligation only the source-side thaw DRIVE (below) may clear — the floor must never
        // short-circuit it, or the source strands frozen forever (the merge-freeze wedge). A frozen
        // source already PAST `expected` still discharges via `seen_past`.
        //
        // TWO predicates from one lookup. LOCAL discharge is what THIS replica can observe directly.
        // The WITNESS mint predicate is STRICTLY STRONGER: a witness is a COMMITTED claim every replica
        // applies, so it may rest ONLY on a GLOBALLY-valid proof — a hosted replica's applied lineage
        // past `expected` (committed ⇒ global), the persisted engine lineage past it (the driver's
        // global mirror of the source's committed counter), or the TERMINAL `MERGED_FLOOR` (the source
        // was absorbed away — global). NEVER a non-terminal floor, and NEVER a lower-gen squatter's
        // counter: both are HOST-LOCAL facts ("THIS host stopped hosting at/below `expected`", set by
        // the local removal ceiling), so witnessing either would clear a LIVE obligation on a
        // co-hosting holder whose source is still frozen — killing the thaw drive and stranding the
        // source frozen forever.
        let (local_discharged, global_proof) = match self.groups.get(&source) {
          Some(sep) => {
            let seen_past = sep.shape_gen() > expected;
            let local = seen_past
              || stores.lineage(&source) > expected
              || (!sep.is_frozen() && !crate::floor_admits(stores.floor(&source), expected));
            (local, seen_past)
          }
          None => {
            let lineage_past = stores.lineage(&source) > expected;
            let floor = stores.floor(&source);
            (
              lineage_past || !crate::floor_admits(floor, expected),
              lineage_past || floor == crate::MERGED_FLOOR,
            )
          }
        };
        // THE OBSERVER LEADER DEFERS ITS CLEAR TO THE WITNESS APPLY. An UNPARKED holder that LEADS with
        // a global proof appends the witness once (idempotent) and lets the committed apply clear the
        // map — uniformly on every replica, this leader included — so a replica that can never LOCALLY
        // observe the source (the dead-end class) clears off the same committed entry. Keeping the map
        // entry alive is the re-append trigger under leader churn (a truncated witness is re-observed
        // and re-appended by the next leader still holding it). A holder whose ONLY proof is a
        // non-terminal floor mints nothing and takes the local clear below; a non-leader likewise.
        //
        // A PARKED holder is EXCLUDED from the defer: its apply drain is stopped at the absorb boundary,
        // so a witness above the park could NEVER apply there, and a standing obligation whose abort
        // entry sits at/below that boundary FENCES the holder's OWN in-flight absorb
        // (`abort_relay_fences`) — deferring would deadlock the absorb on a witness the park blocks. It
        // takes the local clear instead (the pre-witness behavior), which lifts that fence; a co-parked
        // non-observing follower resolves on the absorb's forced-snapshot path (`note_abort_rebaselined`
        // supersedes its park), not the witness. RESIDUAL: while a NON-observer leads an unparked holder,
        // un-observing followers keep the ghost; it self-heals on the first observer-led term.
        let holder_defers = self
          .groups
          .get(&tgid)
          .is_some_and(|t| t.role().is_leader() && t.pending_merge().is_none());
        if holder_defers && global_proof {
          if let Some((tlog, _)) = stores.stores(&tgid) {
            let _ = self.propose_thaw_witness(&tgid, &source_key, expected, now, tlog);
          }
          continue;
        }
        if local_discharged {
          if let Some(tep) = self.groups.get_mut(&tgid) {
            tep.clear_abandoned(&source_key);
          }
          continue;
        }
        // Not discharged: DRIVE the source-side thaw, best-effort and idempotent per crank. Only the
        // source LEADER appends (others answer a transient refusal and later retire by OBSERVING the
        // committed thaw); the incarnation gate binds the mint to `expected`, so a stale re-derivation
        // cannot thaw a re-frozen pair, and the derived-from-abort gate re-verifies THIS obligation
        // still backs it. The return is DISCARDED — discharge is decided by the direct observation
        // above on a later crank, not by this append's outcome.
        if let Some((slog, sstable)) = stores.stores(&source) {
          let _ = self.propose_merge_unfreeze(&source, now, slog, sstable, &tgid, expected);
        }
      }
    }
    // THE FLOOR-AUTHORITATIVE HUSK DISSOLVE, ordered AFTER the resolver loop AND the thaw pass so a
    // witness has already cleared any of a husk's UNDRIVABLE obligations before the belt reads them.
    // A hosted FROZEN source at the TERMINAL `MERGED_FLOOR` is the husk of a lineage absorbed away
    // ELSEWHERE (its target caught up via a snapshot install and never parked here): tombstones refuse
    // recreation, so no later same-id incarnation can ever exist and the floor is AUTHORITATIVE that
    // this frozen replica is dead. Otherwise it is unremovable (`Frozen`), blocks its claimed target's
    // removal (`Claimed`), and is capture-fenced forever. Dissolve it LOCALLY; a poisoned husk is left
    // for its own fail-stop.
    // The source ids NAMED by any co-hosted target's still-standing park, collected in ONE pass
    // here (exact because the resolver + thaw passes above already ran) rather than re-scanning
    // every group per husk — the husk×park product was O(N²) under merge-heavy reshaping. A frozen
    // husk holds no park itself, so it never self-contributes; membership is the exact predicate
    // `park_names_source` computes for a husk. A removals-only staleness is conservative: dissolving
    // husks (frozen sources) never mutates a target's park, so the set stays valid across the loop.
    let park_named_sources: BTreeSet<Bytes> = self
      .groups
      .values()
      .filter_map(|ep| ep.pending_merge().map(|p| p.source_bytes()))
      .collect();
    let husks: Vec<G> = self
      .groups
      .iter()
      .filter(|(gid, ep)| {
        ep.is_frozen() && !ep.is_poisoned() && stores.floor(gid) == crate::MERGED_FLOOR
      })
      .map(|(gid, _)| gid.cheap_clone())
      .collect();
    for gid in husks {
      // THE PARK GATE: a co-hosted target's park may still NAME this husk as its source (that target
      // has not parked-resolved the absorb here). Dissolving first would hand that park's resolver a
      // MANUFACTURED absence + `MERGED_FLOOR` — its absent-source arm resolves Abort and SKIPS the
      // union = committed divergence from every host that absorbed. HOLD; the resolver absorbs the
      // husk normally (source hosted → Resolve) on this same host.
      let mut husk_key = Vec::new();
      gid.encode(&mut husk_key);
      if park_named_sources.contains(&Bytes::from(husk_key)) {
        continue;
      }
      // THE BELT (shared with the absorb Resolve arm): a husk that still owes a LOCALLY-DRIVABLE thaw
      // must not dissolve — dropping the obligation strands the upstream source. A decode-corrupt owed
      // id poisons the husk and holds.
      if self.owes_a_drivable_thaw(&gid) {
        continue;
      }
      // THE STAGED-FORK LEG (the Resolve arm's, for the husk): the obligation is HOST-LOCAL, so the
      // claimant that absorbed this lineage elsewhere — and wrote the terminal floor this dissolve
      // keys on — never saw it. Retiring destroys the staged child's only local derivation, losing
      // an under-replicated child to a crash here. It clears on the CHILD's own baseline flush,
      // which nothing this dissolve blocks can delay, so the hold is live: the first crank after
      // the release retires the husk. SILENT, unlike the Resolve arm's twin — a husk holds no park
      // to key an observation on, and a conflicting fork already cued its own signal.
      if self
        .groups
        .get(&gid)
        .is_some_and(Endpoint::fork_obligations_standing)
      {
        continue;
      }
      // THE INHERITED-DEBT LEG (the Resolve arm's, for the husk): the debt is HOST-LOCAL, so a
      // foreign-led freeze can husk a debtor here while the replicas that captured cleanly never
      // owed. Retiring would drop the held `Merged` — the earlier absorbed source's only
      // terminal-floor permission — so it discharges into the SAME evidence this dissolve keys
      // on: the propagated `MERGED_FLOOR` was co-barriered with the claimant's durable capture
      // of this husk's state machine, which has carried the prior union since that absorb
      // applied. (A claimant that itself deferred writes no terminal floor until its own debt
      // discharges, so the floor gate serializes the chain by construction.) A committed-corrupt
      // debt source id fail-stops the husk instead of retiring it.
      let mut inherited_sources: std::vec::Vec<G> = std::vec::Vec::new();
      {
        let mut corrupt = false;
        if let Some(ep) = self.groups.get(&gid) {
          for m in ep.capture_debt_chain() {
            match G::decode_exact(m.source()) {
              Ok(prior) => inherited_sources.push(prior),
              Err(_) => {
                corrupt = true;
                break;
              }
            }
          }
        }
        if corrupt {
          if let Some(ep) = self.groups.get_mut(&gid) {
            ep.poison(PoisonReason::MergeDecode);
          }
          self.note_if_poisoned(&gid);
          self.mark_dirty(&gid);
          continue;
        }
      }
      // Dissolve through the UNGATED inner teardown (the public gate's `Frozen`/`Claimed` refusals all
      // describe this very dead lineage). The driver folds the SAME source half as `Merged` MINUS the
      // capture — the co-barriered terminal-floor re-write keeps a crash from re-admitting the id.
      if self.remove_group_inner(&gid).is_some() {
        self.mark_dirty(&gid);
        for prior in inherited_sources {
          resolutions.push(MergeResolution::Merged {
            source: prior,
            target: gid.cheap_clone(),
          });
        }
        resolutions.push(MergeResolution::Retired { source: gid });
      }
    }
    // THE DEAD-TARGET THAW DERIVATION, ordered LAST (after the resolver loop, the thaw pass, and the
    // husk dissolve so those own every case they can). A hosted FROZEN source whose claimed target is
    // (i) NOT hosted here AND (ii) reads the TERMINAL `MERGED_FLOOR` is STRANDED: the target dissolved
    // (a legal chain S→T→U absorbed T into U), and BOTH of the source's release verbs — `commit_merge`
    // and `rollback_merge` — ride the dead target's log, so no external verb can ever thaw it. The
    // source self-heals by minting its OWN thaw.
    //
    // THE HUSK-MINORITY LEMMA (why this can never race a live commit into divergence). A committed
    // `CommitMerge(S→T)` at coordinate k lives durably on a target QUORUM; every k-holding T replica
    // PARKS at k−1, and a restart re-parks. A T replica SKIPS k only when an install supersedes its
    // park, and every superseding shape leaves S's post-success electorate exactly as consumed as a
    // local absorb would have: a log-behind straggler's install (the pre-cure population) is
    // sub-quorum by its own trigger, and the parked-union ADOPT — which deliberately reaches
    // log-matched, even quorum-many, T replicas — fires ONLY where the resolver classified the park
    // locally unresolvable, whose defining condition is that NO S replica is hosted there at all. So
    // no skip, of either shape, ever adds a surviving S husk (a vote) to the success world's S
    // electorate; the hosts whose S replicas survive un-consumed are exactly those where the
    // absorb is still locally pending or locally impossible-by-hosting, and their husks retire
    // off the propagated terminal floor once it arrives. S's voter set
    // is FROZEN-TIME-FIXED (a frozen source refuses conf changes), dissolved T replicas are
    // tombstoned NON-voters, and this mint is LEADER-only. Therefore in the success world an S
    // leader can never even APPEND this thaw (the surviving-husk electorate cannot elect), let
    // alone commit it — the divergence is unconstructible. In the
    // commit-ABORTED world the source's drivable-thaw belt (the absorb Resolve arm / husk belt) thaws
    // S off the target's `abandoned` obligation before T could ever dissolve. In the
    // commit-NEVER-EXISTED world (the genuine chain strand) this thaw is exactly correct: no commit is
    // owed, and the terminal floor on the vanished target is authoritative that its lineage is dead.
    // The consensus-grade cost of a FALSE terminal floor (writing `MERGED_FLOOR` for an unresolved
    // lineage) is spelled out on [`MERGED_FLOOR`] — it can now mint a COMMITTED thaw and diverge a
    // target's replicas, so it is a safety violation, full stop.
    let frozen_sources: Vec<G> = self
      .groups
      .iter()
      .filter(|(_, ep)| ep.is_frozen() && !ep.is_poisoned())
      .map(|(gid, _)| gid.cheap_clone())
      .collect();
    for sgid in frozen_sources {
      // Re-read under the mutating loop; decode the applied claim to its target (a committed-corrupt
      // claim is the park/husk decode class — poison and skip).
      let Some(sep) = self.groups.get(&sgid) else {
        continue;
      };
      let Some(claim) = sep.frozen_for().cloned() else {
        continue;
      };
      let target = match G::decode_exact(claim) {
        Ok(t) => t,
        Err(_) => {
          if let Some(sep) = self.groups.get_mut(&sgid) {
            sep.poison(PoisonReason::MergeDecode);
          }
          self.note_if_poisoned(&sgid);
          continue;
        }
      };
      // (i) A hosted target is NOT dead — the resolver (park) or the husk dissolve owns it.
      if self.groups.contains_key(&target) {
        continue;
      }
      // (ii) THE TERMINAL-FLOOR-ONLY TRIGGER. A non-terminal floor is a HOST-LOCAL fact (this host
      // merely stopped hosting the target at/below some generation) and must NEVER mint — the same
      // two-predicate discipline as the `ThawDischarged` witness mint, which likewise rests a
      // COMMITTED entry only on the globally-valid `MERGED_FLOOR`, never a local removal ceiling.
      if stores.floor(&target) != crate::MERGED_FLOOR {
        continue;
      }
      // Derive the source's own thaw. The mint re-checks leadership, the incarnation, the claim, and
      // the park belt; its outcome is DISCARDED — a later crank observes the committed thaw advance
      // the source's lineage past the freeze, exactly as the abort-relay drive is observation-retired.
      if let Some((slog, sstable)) = stores.stores(&sgid) {
        let _ = self.propose_dead_target_thaw(&sgid, now, slog, sstable, &target);
      }
    }
    // THE CAPTURE-DEBT PASS: discharge fence-deferred absorb captures. A debt's union is applied
    // and serving, but its durability capture waited on a replay fence — the consumed source's
    // intact stores are still the union's only restart derivation. Once the fence lifts (this
    // pass runs after every fence-lifting pass in the crank, so a lifted fence feeds it
    // immediately), or
    // a capture at-or-past the boundary is already staged (the threshold capture shares the same
    // fence set, so either producer is co-barriered with this crank's flush), stage the forced
    // capture and surface the held `Merged` — the driver's floor + teardown permission. A
    // capture fault poisons and surfaces `CaptureFailed` exactly as the one-crank arm: stores
    // preserved, restart re-parks.
    let debtors: Vec<G> = self
      .groups
      .iter()
      // A poisoned debtor already surfaced its CaptureFailed during the faulting attempt;
      // re-entering every crank would re-emit it unboundedly and busy-spin the plane. The debt
      // stays minted so its lifecycle fences keep holding.
      .filter(|(_, ep)| ep.capture_debt().is_some() && !ep.is_poisoned())
      .map(|(gid, _)| gid.cheap_clone())
      .collect();
    for tgid in debtors {
      let Some(tep) = self.groups.get(&tgid) else {
        continue;
      };
      let Some(debt) = tep.capture_debt() else {
        continue;
      };
      let boundary = debt.index();
      let Ok(source) = G::decode_exact(debt.source()) else {
        // The same bytes decoded at the defer; a failure here is the committed-corrupt class.
        if let Some(tep) = self.groups.get_mut(&tgid) {
          tep.poison(PoisonReason::MergeDecode);
        }
        self.note_if_poisoned(&tgid);
        continue;
      };
      let already_staged = tep
        .pending_compact_boundary()
        .is_some_and(|b| b >= boundary)
        // A COMPLETED install (or capture) at-or-past the boundary is the after-durable form of
        // the same evidence — the soundest producer of the three; without this leg an install
        // into a debt-holder would orphan the debt (the restore discards the boundary's
        // CommitMerge, so a crash then loses the volatile debt with no re-park left). Durability
        // ALONE is not it, though: a completion-time redundant install raises the durable index
        // while deliberately keeping the log, and discharging on that evidence would strand the
        // membership fence on a compaction no threshold will trigger — so the leg also requires
        // the fence itself to have released (a real restore compacts past the absorb point and
        // satisfies both).
        || (tep.durable_snapshot_covers(boundary)
          && match stores.stores(&tgid) {
            Some((log, _)) => !tep.merge_conf_fence(&*log),
            None => false,
          });
      if !already_staged {
        // The fence is keyed at the CAPTURE POINT — current `applied`, where the forced capture
        // stamps and its compaction lands — never the absorb boundary: the debt window is
        // embedder-timescale, and a Split or a target-role abort applied INSIDE it sits above
        // the boundary, invisible to a boundary-keyed leg, while the compaction at `applied`
        // would erase exactly the replay entry that fence exists to keep. The boundary stays
        // the COVERAGE key (the staged/durable producers above) — coverage is monotone; the
        // fence is not.
        let capture_at = tep.applied_index();
        if tep.capture_blocked_at(capture_at) {
          // The debt window's own signal: post-defer the group LOOKS healthy — unparked,
          // committing, serving the union — while its conf changes are fenced and the consumed
          // source's id stays un-reusable. Name the fence that is standing, or nothing at all
          // when only a transient is (it drains within cranks).
          if let Some(fence) = tep.capture_fence_at(capture_at) {
            let cause = match fence {
              crate::endpoint::CaptureFence::Frozen => MergeBlockedCause::Frozen,
              crate::endpoint::CaptureFence::Fork => MergeBlockedCause::ForkFence,
              crate::endpoint::CaptureFence::Abort => MergeBlockedCause::AbortFence,
            };
            self.note_merge_blocked(&tgid, &source, boundary, cause);
          }
          continue;
        }
        let staged = match stores.stores(&tgid) {
          Some((log, stable)) => self
            .groups
            .get_mut(&tgid)
            .is_some_and(|tep| tep.capture_absorb_snapshot(log, stable)),
          None => {
            if let Some(tep) = self.groups.get_mut(&tgid) {
              tep.poison(PoisonReason::SnapshotCapture);
            }
            false
          }
        };
        if !staged {
          // Latch for `poll_poisoned` alongside the typed resolution: the poisoned debtor is
          // filtered from every later pass, so without the latch the fail-stop never surfaces
          // once cure traffic stops.
          self.note_if_poisoned(&tgid);
          self.mark_dirty(&tgid);
          resolutions.push(MergeResolution::CaptureFailed {
            source,
            target: tgid,
          });
          continue;
        }
      }
      let mut inherited: std::vec::Vec<crate::Merged> = std::vec::Vec::new();
      if let Some(tep) = self.groups.get_mut(&tgid)
        && let Some(m) = tep.discharge_capture_debt()
      {
        tep.emit_merged(m);
        inherited = tep.take_inherited_debts();
      }
      self.mark_dirty(&tgid);
      resolutions.push(MergeResolution::Merged {
        source,
        target: tgid.cheap_clone(),
      });
      // Inherited debts discharge WITH the own debt — the same covering capture proves every
      // prior union durable (the fold has carried each since its absorb applied). Resolutions
      // only: their app-visible events fired where the original holders captured cleanly. The
      // ids decoded when the chain was validated at inheritance; a failure here is the same
      // committed-corrupt fail-stop.
      for m in inherited {
        match G::decode_exact(m.source()) {
          Ok(prior) => resolutions.push(MergeResolution::Merged {
            source: prior,
            target: tgid.cheap_clone(),
          }),
          Err(_) => {
            if let Some(tep) = self.groups.get_mut(&tgid) {
              tep.poison(PoisonReason::MergeDecode);
            }
            self.note_if_poisoned(&tgid);
            break;
          }
        }
      }
    }
    // THE ADOPT-CAPTURE PASS: an adopt owes one forced capture, threshold-independent (the
    // adopt persisted no blob — see the obligation's doc). Same fence discipline as every
    // capture producer; a fenced crank just retries. Runs AFTER the debt pass so a coexisting
    // debt services first under its own CaptureFailed contract — the debt's staged capture (at
    // applied, at-or-past this obligation's boundary) then discharges this flag in the same
    // crank, and a faulting shared capture surfaces the debt's typed resolution instead of
    // vanishing behind the poison filter.
    let adopt_owers: Vec<G> = self
      .groups
      .iter()
      .filter(|(_, ep)| ep.adopt_capture_owed() && !ep.is_poisoned())
      .map(|(gid, _)| gid.cheap_clone())
      .collect();
    for gid in adopt_owers {
      let (discharged, blocked) = {
        let Some(ep) = self.groups.get(&gid) else {
          continue;
        };
        let applied = ep.applied_index();
        // Discharged only when BOTH halves of the obligation hold, or a staged capture will
        // produce them together. Durable coverage ALONE is neither: a completion-time
        // redundant install raises the durable index while deliberately keeping the log, so
        // it satisfies cure-serving while the membership fence still awaits a compaction no
        // threshold will trigger — the exact wedge this obligation exists to cure. The
        // durable leg therefore also requires the fence itself to have released.
        let discharged = ep.pending_compact_boundary().is_some_and(|b| b >= applied)
          || (ep.durable_snapshot_covers(applied)
            && match stores.stores(&gid) {
              Some((log, _)) => !ep.merge_conf_fence(&*log),
              None => false,
            });
        (discharged, ep.capture_blocked_at(applied))
      };
      if discharged {
        if let Some(ep) = self.groups.get_mut(&gid) {
          ep.clear_adopt_capture_owed();
        }
        continue;
      }
      if blocked {
        continue;
      }
      let staged = match stores.stores(&gid) {
        Some((log, stable)) => self
          .groups
          .get_mut(&gid)
          .is_some_and(|ep| ep.capture_absorb_snapshot(log, stable)),
        None => {
          if let Some(ep) = self.groups.get_mut(&gid) {
            ep.poison(PoisonReason::SnapshotCapture);
          }
          false
        }
      };
      if staged {
        if let Some(ep) = self.groups.get_mut(&gid) {
          ep.clear_adopt_capture_owed();
        }
      } else {
        // The capture faulted: no consumed source stands behind this obligation, so no
        // CaptureFailed routing contract applies — the poison surface is the whole signal,
        // and it must latch here or an idle adopter's fail-stop stays invisible until
        // unrelated traffic next touches the group.
        self.note_if_poisoned(&gid);
      }
      self.mark_dirty(&gid);
    }
    // THE OBSERVATION RETIREMENT, at the END of the crank so a hold resolved by ANY pass above
    // never outlives it: the seen edge and the QUEUE both retain only what THIS crank still
    // derived. Every pass re-attempts a standing cause each crank (the edge dedupe absorbs the
    // repeats), so absence here IS resolution or cause drift — and a queued undelivered signal
    // for either would prompt embedder action against a hold that no longer exists (re-hosting
    // an absorbed source beside the cured union, chasing a crossing that moved on). The drivers
    // drain immediately after this returns, so nothing later re-checks.
    let attempts = core::mem::take(&mut self.merge_blocked_attempts);
    self
      .merge_blocked_seen
      .retain(|g, obs| attempts.get(g) == Some(obs));
    self.merge_blocked.retain(|b| {
      attempts
        .get(&b.target)
        .is_some_and(|(c, s, i)| *c == b.cause && *s == b.source && *i == b.boundary)
    });
    resolutions
  }

  /// Initiate a linearizable read on `gid`; the resulting `ReadState` surfaces via
  /// [`poll_event`](Self::poll_event) stamped with the group. `None` if no such group.
  #[must_use = "`None` means no group with this id is hosted — no read was initiated"]
  pub fn read_index<L, S>(
    &mut self,
    gid: &G,
    now: impl Into<Now>,
    log: &L,
    stable: &S,
    context: Bytes,
  ) -> Option<Result<(), ReadIndexError>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let result = self
      .groups
      .get_mut(gid)?
      .read_index(now, log, stable, context);
    self.mark_dirty(gid);
    Some(result)
  }

  /// Begin transferring `gid`'s leadership to `to`. `None` if no such group.
  #[must_use = "`None` means no group with this id is hosted — no transfer was initiated"]
  pub fn transfer_leader<L, S>(
    &mut self,
    gid: &G,
    now: impl Into<Now>,
    log: &L,
    stable: &S,
    to: I,
  ) -> Option<Result<(), TransferError<I>>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let result = self
      .groups
      .get_mut(gid)?
      .transfer_leader(now, log, stable, to);
    self.mark_dirty(gid);
    Some(result)
  }
}

impl<G, I, F, R> Default for MultiRaft<G, I, F, R>
where
  G: GroupId,
  F: StateMachine,
{
  fn default() -> Self {
    Self::new()
  }
}

/// Whether a single conf-change operation MOVES the voter set — `AddNode` (adds or promotes a
/// voter) or `RemoveNode` (may drop a voter). `AddLearnerNode` never touches the voter set. The
/// claimed-target conf fence keys on this: only a voter move can strand a frozen source by carrying
/// the target's voters off its hosts; a learner change leaves the sets aligned for the absorb.
const fn conf_change_moves_voters(ty: ConfChangeType) -> bool {
  matches!(ty, ConfChangeType::AddNode | ConfChangeType::RemoveNode)
}

/// Fold the group id into the base election seed so co-located groups draw distinct
/// election-timeout jitter. FNV-1a over the id's [`Data`] encoding, perturbed by the base seed.
fn group_seed<G: GroupId>(base: u64, gid: &G) -> u64 {
  let mut buf = Vec::new();
  gid.encode(&mut buf);
  let mut h = 0xcbf2_9ce4_8422_2325_u64 ^ base;
  for b in &buf {
    h ^= u64::from(*b);
    h = h.wrapping_mul(0x0000_0100_0000_01b3);
  }
  h
}

#[cfg(test)]
mod tests;
