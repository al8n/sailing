//! The multi-group cross-thread submission surface: [`MultiHandle`], its per-group
//! [`GroupHandle`] projection, the [`MultiCommand`]s they send a multi-group driver, and the
//! [`GroupFactory`] auto-materialization hook a driver consults on unknown-group solicitations.
//!
//! The single-group [`Handle`](crate::Handle)/[`Command`](crate::Command) pair is untouched (it is
//! shared with every single-group driver); this module is the ADDITIVE group-keyed sibling. Every
//! per-group operation is the single-group operation plus the group id it addresses, and the
//! multi-only surface — group lifecycle (create/restore/remove), the group-stamped events tail,
//! and the [`LifecycleEvent`] tail feeding the embedder's placement brain (unknown-group
//! solicitations, removed-self conf changes; the policy is the embedder's, never the driver's) —
//! lives on [`MultiHandle`] itself. The budget, shutdown-coalescing, and teardown-completion
//! machinery are reused as-is: ONE [`InflightBudget`] spans every group projection (co-located
//! groups share the host's memory, so they share its in-flight bound), one shutdown flag dedups the
//! request across every clone, and one shared teardown receiver fans the driver's fd-release signal
//! out to every waiter.

mod factory;

pub use factory::{BoxedGroupFactory, FnFactory, GroupBlueprint, GroupFactory, factory_fn};

use std::sync::{
  Arc,
  atomic::{AtomicBool, Ordering},
};

use futures_channel::oneshot;
use futures_util::{FutureExt, future::Shared};
use sailing_proto::{
  ConfChange, ConfChangeV2, Config, Data, Entry, Event, FailoverReadWindow, GroupId, Index,
  ReadOnlyOption,
};

use crate::{
  DriverError, Status,
  shared::{FailoverComplete, InflightBudget, QueryComplete, ReservationGuard},
};

/// A control message from a [`MultiHandle`]/[`GroupHandle`] to a multi-group driver task.
///
/// The single-group [`Command`](crate::Command) variants each gain the group id they address, and
/// the group lifecycle rides alongside them on the same channel — so creation, removal, and the
/// operations they race against are serialized by the ONE driver task, never against each other.
pub enum MultiCommand<G, I, F>
where
  F: sailing_proto::StateMachine,
{
  /// Propose a command on one group; answer `reply` with the committed apply response.
  Submit {
    /// The addressed group.
    group: G,
    /// The typed command (the driver passes it to the group's propose by reference).
    cmd: F::Command,
    /// Answered when the entry APPLIES (or with the supersede/teardown error).
    reply: oneshot::Sender<Result<F::Response, DriverError<I>>>,
    /// The owning budget reservation: moved into the group's pending entry on drain, or dropped
    /// still-queued by a teardown — never released manually.
    reservation: ReservationGuard,
  },
  /// Propose a single-step membership change on one group; answer `reply` with the applied index.
  Conf {
    /// The addressed group.
    group: G,
    /// The change.
    cc: ConfChange<I>,
    /// Answered when the change APPLIES (`ConfChanged` at its index).
    reply: oneshot::Sender<Result<Index, DriverError<I>>>,
    /// The owning budget reservation.
    reservation: ReservationGuard,
  },
  /// Propose a joint-consensus membership change on one group; answer `reply` with the applied
  /// index.
  ConfV2 {
    /// The addressed group.
    group: G,
    /// The change.
    cc: ConfChangeV2<I>,
    /// Answered when the change APPLIES.
    reply: oneshot::Sender<Result<Index, DriverError<I>>>,
    /// The owning budget reservation.
    reservation: ReservationGuard,
  },
  /// A linearizable query against one group's state machine: the driver runs that group's
  /// `read_index`, parks the completion until the confirmed read index is applied, then calls it
  /// with `Ok(&F)` ON the driver thread.
  Query {
    /// The addressed group.
    group: G,
    /// The single completion (see [`ParkedQuery`](crate::shared::ParkedQuery)).
    complete: QueryComplete<I, F>,
    /// The owning budget reservation.
    reservation: ReservationGuard,
  },
  /// A failover inherited-read query against one group (see
  /// [`Handle::failover_query`](crate::Handle::failover_query) for the single-group contract; the
  /// serve window is per group).
  FailoverWindow {
    /// The addressed group.
    group: G,
    /// The single completion (see [`FailoverComplete`]).
    complete: FailoverComplete<I, F>,
    /// The owning budget reservation (zero-byte).
    reservation: ReservationGuard,
  },
  /// Begin transferring one group's leadership; answer `reply` with the immediate accept/reject.
  Transfer {
    /// The addressed group.
    group: G,
    /// The transfer target.
    to: I,
    /// Answered with the group's immediate verdict (the transfer itself is asynchronous).
    reply: oneshot::Sender<Result<(), DriverError<I>>>,
    /// The owning budget reservation (zero-byte).
    reservation: ReservationGuard,
  },
  /// Migrate one group's cluster-wide read mode; answer `reply` with the leader's immediate
  /// verdict (the proposed log index).
  SetReadMode {
    /// The addressed group.
    group: G,
    /// The target read mode.
    mode: ReadOnlyOption,
    /// Answered with the group's immediate verdict: the proposed log index, or the rejection.
    reply: oneshot::Sender<Result<Index, DriverError<I>>>,
    /// The owning budget reservation (zero-byte).
    reservation: ReservationGuard,
  },
  /// Propose a group SPLIT on one parent group (see `MultiRaft::propose_split`): a committed
  /// `Split` entry deterministically forks `child` out of the parent's state machine on every
  /// replica, and the drivers materialize the staged forks behind their engine barriers. Answer
  /// `reply` with the leader's immediate verdict (the proposed log index).
  ProposeSplit {
    /// The parent group (the split rides its log; leader-only).
    group: G,
    /// The child group id to fork out.
    child: G,
    /// The child id's incarnation under the single-incarnation contract (0 unless the embedder
    /// reshapes ids; checked against the child id's persisted admission floor at propose AND at
    /// every replica's materialization edge).
    child_gen: u64,
    /// The embedder's opaque partition instruction, handed to `StateMachine::split` on every
    /// replica; bounded by the single-frame append check.
    instruction: bytes::Bytes,
    /// Answered with the leader's immediate verdict: the proposed log index, or the rejection.
    reply: oneshot::Sender<Result<Index, DriverError<I>>>,
    /// The owning budget reservation, sized to the instruction bytes.
    reservation: ReservationGuard,
  },
  /// Snapshot one group's runtime [`Status`]; answer `reply` with the status read off that group's
  /// live endpoint ON the driver thread. `Err(Rejected)` when no such group is hosted — the
  /// group-keyed surface has a miss case the single-group one does not.
  Status {
    /// The addressed group.
    group: G,
    /// Answered with the status snapshot, or the no-such-group rejection.
    reply: oneshot::Sender<Result<Status<I>, DriverError<I>>>,
    /// The owning budget reservation (zero-byte).
    reservation: ReservationGuard,
  },
  /// Create a fresh group on this host. The driver admits it into its shared storage engine and
  /// the multi coordinator together; a refusal (duplicate id, host-identity mismatch, invalid
  /// group-id encoding, a tombstoned id awaiting its explicit clear, invalid config) answers
  /// `reply` with [`DriverError::Rejected`] carrying the typed refusal's description.
  CreateGroup {
    /// The new group's id.
    gid: G,
    /// The group's consensus config (its node id must match the host identity latched by the
    /// first admitted group).
    config: Config<I>,
    /// The group's election-jitter seed (folded with the group id by the container).
    seed: u64,
    /// The group's state machine.
    fsm: F,
    /// The id's incarnation under the single-incarnation contract; 0 unless the embedder
    /// reshapes ids. Checked against the id's persisted admission floor and recorded in the
    /// host's engine on admission.
    generation: u64,
    /// Answered with the admission verdict.
    reply: oneshot::Sender<Result<(), DriverError<I>>>,
    /// The owning budget reservation (zero-byte): lifecycle commands are budgeted like every
    /// other operation so handle clones cannot amplify unbudgeted commands into the channel.
    reservation: ReservationGuard,
  },
  /// Create a group born from LOCALLY-FORKED state — a manufactured snapshot install over the
  /// engine's fresh stores (see `MultiRaft::create_group_from_fork`). The DRIVER supplies the
  /// boot epoch, exactly as on [`RestoreGroup`](Self::RestoreGroup).
  CreateGroupFromFork {
    /// The new group's id.
    gid: G,
    /// The group's consensus config.
    config: Config<I>,
    /// The group's election-jitter seed.
    seed: u64,
    /// The restore vessel: the state machine the authoritative `snapshot` blob is decoded into.
    fsm: F,
    /// The authoritative preloaded-state blob, persisted as the group's baseline snapshot.
    snapshot: bytes::Bytes,
    /// The id's incarnation under the single-incarnation contract; 0 unless the embedder
    /// reshapes ids.
    generation: u64,
    /// Answered with the admission verdict.
    reply: oneshot::Sender<Result<(), DriverError<I>>>,
    /// The owning budget reservation, sized to the snapshot blob: the byte leg of the budget is
    /// the drivers' memory gate, and the blob rides an unbounded channel until consumed.
    reservation: ReservationGuard,
  },
  /// Recover a group from the host's shared storage engine. The DRIVER supplies the boot epoch
  /// (the engine's per-group monotonic counter), so the caller passes none.
  RestoreGroup {
    /// The recovered group's id.
    gid: G,
    /// The group's consensus config.
    config: Config<I>,
    /// The group's election-jitter seed.
    seed: u64,
    /// The group's (pre-replay) state machine.
    fsm: F,
    /// The id's incarnation, supplied by the embedder's catalog — the cross-restart incarnation
    /// authority; 0 unless the embedder reshapes ids.
    generation: u64,
    /// Answered with the admission verdict.
    reply: oneshot::Sender<Result<(), DriverError<I>>>,
    /// The owning budget reservation (zero-byte).
    reservation: ReservationGuard,
  },
  /// Remove a group: its endpoint, its storage, and its parked client work (failed with the
  /// teardown error) are torn down together. Answer `reply` with whether the group was hosted.
  RemoveGroup {
    /// The removed group's id.
    gid: G,
    /// Answered with whether a group with this id was hosted.
    reply: oneshot::Sender<bool>,
    /// The owning budget reservation (zero-byte).
    reservation: ReservationGuard,
  },
  /// Lift a group id's tombstone — the EXPLICIT re-admission consent a removal demands before
  /// the id can be created/restored again (see [`MultiHandle::clear_tombstone`]). Answer `reply`
  /// with whether a tombstone existed.
  ClearTombstone {
    /// The tombstoned group's id.
    gid: G,
    /// Answered with whether a tombstone existed for this id.
    reply: oneshot::Sender<bool>,
    /// The owning budget reservation (zero-byte).
    reservation: ReservationGuard,
  },
  /// Ask the driver to stop. Same contract as the single-group
  /// [`Command::Shutdown`](crate::Command::Shutdown): a plain signal, completion observed through
  /// the shared teardown channel.
  Shutdown,
}

/// A group-lifecycle observation from a multi-group driver — the PLACEMENT-BRAIN feed. sailing
/// ships the lifecycle MECHANICS only: no auto-create, no auto-teardown — the embedder decides
/// (a PD-style external placer and a Cockroach-style local policy are both built on exactly
/// these triggers). Rides its own bounded tail, [`MultiHandle::lifecycle`], best-effort like the
/// events tail.
///
/// Events are OBSERVATIONS captured at emission, riding a best-effort asynchronous tail: by the
/// time one is consumed, the host's lifecycle may have moved on (the group created, removed, or
/// tombstoned since), so the placement brain must order its consumption of the tail against its
/// own lifecycle mutations. The tombstone-refusal rule makes the dangerous stale case fail
/// closed at the driver: an `UnknownGroup` consumed AFTER the embedder removed the group cannot
/// resurrect it — the naive re-create is rejected ([`DriverError::Rejected`]), because
/// re-admission always requires the two deliberate acts,
/// [`MultiHandle::clear_tombstone`] then create/restore.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum LifecycleEvent<G, I> {
  /// An authenticated peer sent well-formed, INITIAL-SHAPED consensus traffic (a vote request,
  /// or a first-contact heartbeat) for a group this host neither carries nor has tombstoned.
  /// The embedder decides placement: create/restore the group on this host (the soliciting
  /// peer's retry then completes the join) or ignore the signal (the driver keeps dropping the
  /// group's frames). Recurs after each poll while the solicitation continues.
  UnknownGroup {
    /// The unhosted group.
    group: G,
    /// The authenticated peer soliciting it.
    from: I,
  },
  /// A committed configuration change REMOVED this host from `group`'s membership — no longer a
  /// voter in either joint half, a learner, or an incoming learner. NO auto-teardown: between
  /// this event and the application's [`MultiHandle::remove_group`] the replica keeps running
  /// Raft, harmlessly (the committed change already excluded it from every quorum, so it can
  /// only campaign into the void). The application drains its reads, then removes the group.
  RemovedSelf {
    /// The group whose committed membership no longer names this host.
    group: G,
  },
  /// A committed SPLIT materialized on this host: `child` was forked out of `parent`, its
  /// baseline is flush-durable behind the engine barrier, and the parent's snapshot fence over
  /// the split index has been lifted. Fired on EVERY replica that materializes the fork (the
  /// typed-G surface of the proto's G-free `Event::SplitApplied`).
  SplitApplied {
    /// The parent group the split rode.
    parent: G,
    /// The freshly-materialized child group.
    child: G,
  },
}

/// A cheaply-cloneable, `Send + Sync` handle to a MULTI-GROUP driver: group lifecycle, the
/// group-stamped events tail, and shutdown live here; per-group operations live on the
/// [`GroupHandle`] projection minted by [`group`](Self::group).
///
/// The bound is the single-group [`Handle`](crate::Handle)'s, shared across EVERY group: one
/// [`InflightBudget`] spans all clones and all group projections (the command channel is unbounded;
/// the budget is the bound), and `shutdown` is coalesced across every clone exactly as on the
/// single-group handle.
pub struct MultiHandle<G, I, F>
where
  F: sailing_proto::StateMachine,
{
  /// The command sender shared with every [`GroupHandle`] projection.
  commands: flume::Sender<MultiCommand<G, I, F>>,
  /// The best-effort events tail, each event stamped with its originating group.
  events: flume::Receiver<(G, Event<I, F::Response>)>,
  /// The best-effort group-lifecycle tail (see [`LifecycleEvent`]).
  lifecycle: flume::Receiver<LifecycleEvent<G, I>>,
  budget: InflightBudget,
  /// Shared across every clone: set once by the first `shutdown` to COALESCE all later (and
  /// concurrent) shutdown requests to a single enqueued [`MultiCommand::Shutdown`].
  shutdown: Arc<AtomicBool>,
  /// The COMPLETION half of shutdown (see [`Handle`](crate::Handle) — the same
  /// fired-after-fd-release contract, fanned to every clone through the `Shared` receiver).
  teardown: Shared<oneshot::Receiver<()>>,
}

impl<G, I, F> Clone for MultiHandle<G, I, F>
where
  F: sailing_proto::StateMachine,
{
  fn clone(&self) -> Self {
    Self {
      commands: self.commands.clone(),
      events: self.events.clone(),
      lifecycle: self.lifecycle.clone(),
      budget: self.budget.clone(),
      shutdown: self.shutdown.clone(),
      teardown: self.teardown.clone(),
    }
  }
}

impl<G, I, F> MultiHandle<G, I, F>
where
  G: GroupId + Send,
  I: sailing_proto::NodeId + Send,
  F: sailing_proto::StateMachine,
  F::Command: Data + Send,
  F::Response: Send,
{
  /// Build a multi-group handle. `teardown` is the receiver half of the driver's
  /// teardown-completion oneshot, stored as a `Shared` so every clone awaits the one signal;
  /// `lifecycle` is the driver's group-lifecycle tail (see [`lifecycle`](Self::lifecycle)).
  pub fn new(
    commands: flume::Sender<MultiCommand<G, I, F>>,
    events: flume::Receiver<(G, Event<I, F::Response>)>,
    lifecycle: flume::Receiver<LifecycleEvent<G, I>>,
    budget: InflightBudget,
    teardown: oneshot::Receiver<()>,
  ) -> Self {
    Self {
      commands,
      events,
      lifecycle,
      budget,
      shutdown: Arc::new(AtomicBool::new(false)),
      teardown: teardown.shared(),
    }
  }

  /// Project this handle onto ONE group: the returned [`GroupHandle`] shares this handle's command
  /// channel and budget (a clone each) plus the group id, so per-group operations read exactly
  /// like the single-group [`Handle`](crate::Handle)'s.
  #[must_use]
  pub fn group(&self, group: G) -> GroupHandle<G, I, F> {
    GroupHandle {
      group,
      commands: self.commands.clone(),
      budget: self.budget.clone(),
    }
  }

  /// Enqueue one command, mapping a closed channel to [`DriverError::ShuttingDown`]. The budget
  /// reservation rides INSIDE `cmd`, so a rejected enqueue drops it here and nothing leaks.
  fn send(&self, cmd: MultiCommand<G, I, F>) -> Result<(), DriverError<I>> {
    send(&self.commands, cmd)
  }

  /// Create a fresh group on the host and await the admission verdict.
  ///
  /// `config`'s node id must match the host identity latched by the FIRST group ever admitted (a
  /// multi-Raft host is one physical node); the first admission latches it. `generation` is the
  /// id's incarnation under the single-incarnation contract — pass 0 unless the embedder
  /// reshapes ids; a reshaping embedder's catalog supplies it, and an incarnation below the id's
  /// persisted admission floor is refused. A refusal — duplicate group id, identity mismatch, an
  /// out-of-bound group-id encoding, an under-floor incarnation, a tombstoned id whose removal
  /// [`clear_tombstone`](Self::clear_tombstone) has not consented to reversing, or an invalid
  /// `config` — resolves [`DriverError::Rejected`] carrying the refusal's description.
  pub async fn create_group(
    &self,
    gid: G,
    config: Config<I>,
    seed: u64,
    fsm: F,
    generation: u64,
  ) -> Result<(), DriverError<I>> {
    let reservation = self.budget.try_reserve(0)?;
    let (tx, rx) = oneshot::channel();
    self.send(MultiCommand::CreateGroup {
      gid,
      config,
      seed,
      fsm,
      generation,
      reply: tx,
      reservation,
    })?;
    rx.await.map_err(|_| DriverError::ShuttingDown)?
  }

  /// Create a group born from LOCALLY-FORKED state and await the admission verdict: the driver
  /// manufactures the snapshot baseline — `snapshot` is the AUTHORITATIVE preloaded-state blob,
  /// `fsm` the vessel it is decoded into — in the group's fresh engine stores and boots through
  /// the restart path, so a later zero-progress joiner is forced onto the snapshot path (never
  /// a log walk over its empty state machine). The boot epoch comes from the engine, as on
  /// [`restore_group`](Self::restore_group); admission (tombstone, floor, `generation`) and the
  /// refusal surface are exactly [`create_group`](Self::create_group)'s. A fork is a local act
  /// by an already-authorized replica — never solicited over the wire, never factory-built.
  ///
  /// The blob counts against `max_pending_bytes` exactly like a proposal payload — a fork is the
  /// largest single allocation a caller can push at a driver — so an over-budget snapshot
  /// resolves [`DriverError::Busy`] before anything is queued, and the bytes stay reserved until
  /// the driver consumes the command.
  pub async fn create_group_from_fork(
    &self,
    gid: G,
    config: Config<I>,
    seed: u64,
    fsm: F,
    snapshot: bytes::Bytes,
    generation: u64,
  ) -> Result<(), DriverError<I>> {
    let reservation = self.budget.try_reserve(snapshot.len())?;
    let (tx, rx) = oneshot::channel();
    self.send(MultiCommand::CreateGroupFromFork {
      gid,
      config,
      seed,
      fsm,
      snapshot,
      generation,
      reply: tx,
      reservation,
    })?;
    rx.await.map_err(|_| DriverError::ShuttingDown)?
  }

  /// Recover a group from the host's shared storage engine and await the admission verdict. The
  /// driver derives the boot epoch from the engine's per-group counter, so restarts never reuse an
  /// incarnation's operation ids. `generation` comes from the embedder's catalog — the
  /// cross-restart incarnation authority (pass 0 unless the embedder reshapes ids): a restore
  /// that lies about it collapses two incarnations into one identity for every gen-keyed
  /// observer, voiding exactly what the admission floor exists to distinguish.
  pub async fn restore_group(
    &self,
    gid: G,
    config: Config<I>,
    seed: u64,
    fsm: F,
    generation: u64,
  ) -> Result<(), DriverError<I>> {
    let reservation = self.budget.try_reserve(0)?;
    let (tx, rx) = oneshot::channel();
    self.send(MultiCommand::RestoreGroup {
      gid,
      config,
      seed,
      fsm,
      generation,
      reply: tx,
      reservation,
    })?;
    rx.await.map_err(|_| DriverError::ShuttingDown)?
  }

  /// Remove a group, awaiting whether it was hosted. The group's parked operations fail with
  /// [`DriverError::ShuttingDown`] (the group-scoped teardown verdict); co-hosted groups are
  /// untouched. The removal TOMBSTONES the id: straggler frames drop silently and a
  /// create/restore of the id is rejected until [`clear_tombstone`](Self::clear_tombstone)
  /// explicitly consents to re-admission.
  pub async fn remove_group(&self, gid: G) -> Result<bool, DriverError<I>> {
    let reservation = self.budget.try_reserve(0)?;
    let (tx, rx) = oneshot::channel();
    self.send(MultiCommand::RemoveGroup {
      gid,
      reply: tx,
      reservation,
    })?;
    rx.await.map_err(|_| DriverError::ShuttingDown)
  }

  /// Lift `gid`'s tombstone, awaiting whether one existed — the EXPLICIT re-admission consent,
  /// and the only way a removed id becomes creatable again: rejoining an id is always the two
  /// deliberate acts, `clear_tombstone` then [`create_group`](Self::create_group) /
  /// [`restore_group`](Self::restore_group) — so a stale [`LifecycleEvent::UnknownGroup`]
  /// consumed after the removal can never resurrect the id on its own.
  pub async fn clear_tombstone(&self, gid: G) -> Result<bool, DriverError<I>> {
    let reservation = self.budget.try_reserve(0)?;
    let (tx, rx) = oneshot::channel();
    self.send(MultiCommand::ClearTombstone {
      gid,
      reply: tx,
      reservation,
    })?;
    rx.await.map_err(|_| DriverError::ShuttingDown)
  }

  /// Propose a SPLIT of `parent`: fork `child` out of its state machine (see
  /// [`GroupHandle::propose_split`] — this is the same command addressed without minting a
  /// projection).
  pub async fn propose_split(
    &self,
    parent: G,
    child: G,
    child_gen: u64,
    instruction: bytes::Bytes,
  ) -> Result<Index, DriverError<I>> {
    let reservation = self.budget.try_reserve(instruction.len())?;
    let (tx, rx) = oneshot::channel();
    self.send(MultiCommand::ProposeSplit {
      group: parent,
      child,
      child_gen,
      instruction,
      reply: tx,
      reservation,
    })?;
    rx.await.map_err(|_| DriverError::ShuttingDown)?
  }

  /// The best-effort events tail, every event stamped with its originating group. Bounded and
  /// dropped-on-full: an observer that falls behind loses events, never slows consensus.
  pub fn events(&self) -> &flume::Receiver<(G, Event<I, F::Response>)> {
    &self.events
  }

  /// The best-effort group-LIFECYCLE tail (see [`LifecycleEvent`]): unknown-group placement
  /// signals and removed-self notifications, bounded and dropped-on-full exactly like
  /// [`events`](Self::events) — a lagging observer loses signals, never slows the driver. A
  /// dropped `UnknownGroup` recurs (the soliciting peer retries on its own cadence); a dropped
  /// `RemovedSelf` does not, but remains derivable from [`GroupHandle::status`] (a conf state
  /// that no longer names this host).
  pub fn lifecycle(&self) -> &flume::Receiver<LifecycleEvent<G, I>> {
    &self.lifecycle
  }

  /// Ask the driver to stop and await teardown completion — the single-group
  /// [`Handle::shutdown`](crate::Handle::shutdown) contract verbatim: idempotent, coalesced across
  /// every clone, and resolved only after the driver's fd-release barrier.
  pub async fn shutdown(&self) -> Result<(), DriverError<I>> {
    if !self.shutdown.swap(true, Ordering::AcqRel)
      && let Err(flume::TrySendError::Full(_)) = self.commands.try_send(MultiCommand::Shutdown)
    {
      return Err(DriverError::Busy);
    }
    match self.teardown.clone().await {
      Ok(()) => Ok(()),
      Err(_aborted) => Err(DriverError::ShuttingDown),
    }
  }
}

/// The per-group projection of a [`MultiHandle`]: the single-group operation surface addressed to
/// ONE group. Cheap to mint ([`MultiHandle::group`]) and to clone — a channel handle, the shared
/// budget, and the group id — and `Send + Sync` like its parent.
///
/// The budget is the PARENT's: every projection of every clone draws on the one host-wide
/// in-flight bound, so no per-group fan-out can multiply the host's memory ceiling.
pub struct GroupHandle<G, I, F>
where
  F: sailing_proto::StateMachine,
{
  group: G,
  commands: flume::Sender<MultiCommand<G, I, F>>,
  budget: InflightBudget,
}

impl<G, I, F> Clone for GroupHandle<G, I, F>
where
  G: GroupId,
  F: sailing_proto::StateMachine,
{
  fn clone(&self) -> Self {
    Self {
      group: self.group.cheap_clone(),
      commands: self.commands.clone(),
      budget: self.budget.clone(),
    }
  }
}

/// Enqueue one command on `commands`, mapping a closed channel to
/// [`DriverError::ShuttingDown`] (and the defensively-dead `Full` arm to [`DriverError::Busy`] —
/// the channel is unbounded; the budget is the real backpressure gate).
fn send<G, I, F>(
  commands: &flume::Sender<MultiCommand<G, I, F>>,
  cmd: MultiCommand<G, I, F>,
) -> Result<(), DriverError<I>>
where
  F: sailing_proto::StateMachine,
{
  commands.try_send(cmd).map_err(|e| {
    if matches!(e, flume::TrySendError::Disconnected(_)) {
      DriverError::ShuttingDown
    } else {
      DriverError::Busy
    }
  })
}

impl<G, I, F> GroupHandle<G, I, F>
where
  G: GroupId + Send,
  I: sailing_proto::NodeId + Send,
  F: sailing_proto::StateMachine,
  F::Command: Data + Send,
  F::Response: Send,
{
  /// The group this projection addresses.
  pub fn group_id(&self) -> &G {
    &self.group
  }

  /// Enqueue one command (see the free [`send`]).
  fn send(&self, cmd: MultiCommand<G, I, F>) -> Result<(), DriverError<I>> {
    send(&self.commands, cmd)
  }

  /// Propose a command on this group and await its committed apply response — the group-keyed
  /// [`Handle::submit`](crate::Handle::submit): same budget accounting (the encoded payload
  /// length), same error surface, plus [`DriverError::Rejected`] when the group is not hosted.
  pub async fn submit(&self, cmd: F::Command) -> Result<F::Response, DriverError<I>> {
    // Measure the payload for the byte budget (one extra encode, as on the single-group path).
    let mut measure = Vec::new();
    cmd.encode(&mut measure);
    let reservation = self.budget.try_reserve(measure.len())?;
    let (tx, rx) = oneshot::channel();
    self.send(MultiCommand::Submit {
      group: self.group.cheap_clone(),
      cmd,
      reply: tx,
      reservation,
    })?;
    rx.await.map_err(|_| DriverError::ShuttingDown)?
  }

  /// Run a linearizable query against this group's state machine and await its result — the
  /// group-keyed [`Handle::query`](crate::Handle::query): the closure runs ON the driver thread
  /// once the group's read index is confirmed AND applied.
  pub async fn query<Out, Q>(&self, f: Q) -> Result<Out, DriverError<I>>
  where
    Out: Send + 'static,
    Q: FnOnce(&F) -> Out + Send + 'static,
  {
    let reservation = self.budget.try_reserve(0)?;
    let (tx, rx) = oneshot::channel();
    let complete = Box::new(move |res: Result<&F, DriverError<I>>| {
      let _ = tx.send(res.map(f));
    });
    self.send(MultiCommand::Query {
      group: self.group.cheap_clone(),
      complete,
      reservation,
    })?;
    rx.await.map_err(|_| DriverError::ShuttingDown)?
  }

  /// Run an inherited-read query during this group's post-election failover commit-wait — the
  /// group-keyed [`Handle::failover_query`](crate::Handle::failover_query). Resolves `Ok(None)`
  /// whenever the group has no serve window (including on a monotonic-only multi host, whose
  /// failover tier is inert); the caller falls back to [`query`](Self::query).
  pub async fn failover_query<Out, Q>(&self, f: Q) -> Result<Option<Out>, DriverError<I>>
  where
    Out: Send + 'static,
    Q: FnOnce(&F, &[Entry], FailoverReadWindow) -> Option<Out> + Send + 'static,
  {
    let reservation = self.budget.try_reserve(0)?;
    let (tx, rx) = oneshot::channel();
    let complete = Box::new(move |res: crate::shared::FailoverOutcome<'_, I, F>| {
      let _ = tx.send(res.map(|opt| opt.and_then(|(fsm, limbo, win)| f(fsm, limbo, win))));
    });
    self.send(MultiCommand::FailoverWindow {
      group: self.group.cheap_clone(),
      complete,
      reservation,
    })?;
    rx.await.map_err(|_| DriverError::ShuttingDown)?
  }

  /// Propose a single-step membership change on this group and await its APPLIED index.
  pub async fn conf_change(&self, cc: ConfChange<I>) -> Result<Index, DriverError<I>>
  where
    I: Data,
  {
    let reservation = self.budget.try_reserve(0)?;
    let (tx, rx) = oneshot::channel();
    self.send(MultiCommand::Conf {
      group: self.group.cheap_clone(),
      cc,
      reply: tx,
      reservation,
    })?;
    rx.await.map_err(|_| DriverError::ShuttingDown)?
  }

  /// Propose a joint-consensus membership change on this group and await its APPLIED index.
  pub async fn conf_change_v2(&self, cc: ConfChangeV2<I>) -> Result<Index, DriverError<I>>
  where
    I: Data,
  {
    let reservation = self.budget.try_reserve(0)?;
    let (tx, rx) = oneshot::channel();
    self.send(MultiCommand::ConfV2 {
      group: self.group.cheap_clone(),
      cc,
      reply: tx,
      reservation,
    })?;
    rx.await.map_err(|_| DriverError::ShuttingDown)?
  }

  /// Begin transferring this group's leadership to `to`, awaiting the group's IMMEDIATE verdict.
  pub async fn transfer_leader(&self, to: I) -> Result<(), DriverError<I>> {
    let reservation = self.budget.try_reserve(0)?;
    let (tx, rx) = oneshot::channel();
    self.send(MultiCommand::Transfer {
      group: self.group.cheap_clone(),
      to,
      reply: tx,
      reservation,
    })?;
    rx.await.map_err(|_| DriverError::ShuttingDown)?
  }

  /// Migrate this group's cluster-wide read mode, awaiting the leader's IMMEDIATE verdict (the
  /// proposed log index; the migration takes effect apply-time once the entry commits).
  pub async fn set_read_mode(&self, mode: ReadOnlyOption) -> Result<Index, DriverError<I>> {
    let reservation = self.budget.try_reserve(0)?;
    let (tx, rx) = oneshot::channel();
    self.send(MultiCommand::SetReadMode {
      group: self.group.cheap_clone(),
      mode,
      reply: tx,
      reservation,
    })?;
    rx.await.map_err(|_| DriverError::ShuttingDown)?
  }

  /// Propose a SPLIT of this group: fork `child` out of its state machine by the embedder's
  /// opaque `instruction`, awaiting the leader's IMMEDIATE verdict (the proposed log index; the
  /// fork itself materializes on every replica when the entry commits, surfacing as
  /// [`LifecycleEvent::SplitApplied`]). Refusals — not leader, a joint-config parent, a hosted
  /// or floored child id, an out-of-bound child encoding — resolve typed
  /// [`DriverError::NotLeader`]/[`DriverError::Rejected`], with nothing appended.
  pub async fn propose_split(
    &self,
    child: G,
    child_gen: u64,
    instruction: bytes::Bytes,
  ) -> Result<Index, DriverError<I>> {
    let reservation = self.budget.try_reserve(instruction.len())?;
    let (tx, rx) = oneshot::channel();
    self.send(MultiCommand::ProposeSplit {
      group: self.group.cheap_clone(),
      child,
      child_gen,
      instruction,
      reply: tx,
      reservation,
    })?;
    rx.await.map_err(|_| DriverError::ShuttingDown)?
  }

  /// Snapshot this group's runtime [`Status`], read off the group's live endpoint ON the driver
  /// thread. `Err(Rejected)` when no such group is hosted.
  pub async fn status(&self) -> Result<Status<I>, DriverError<I>> {
    let reservation = self.budget.try_reserve(0)?;
    let (tx, rx) = oneshot::channel();
    self.send(MultiCommand::Status {
      group: self.group.cheap_clone(),
      reply: tx,
      reservation,
    })?;
    rx.await.map_err(|_| DriverError::ShuttingDown)?
  }
}

#[cfg(test)]
mod tests;
