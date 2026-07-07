//! The committed-split state the endpoint stages between the deterministic apply point and the
//! driver's materialization crank: the pending forks (with their apply-derived recovery blobs),
//! the fork durability barrier over the parent's snapshots, and the group's lineage counter.
use super::*;
use std::collections::BTreeSet;

/// One committed split, staged at apply for the multi container to relay. Deliberately F-FREE —
/// the forked state-machine half rides beside the queue (see [`SplitState::forked_fsms`]) — so a
/// container can inspect a fork without touching the half, and G-FREE (the child id stays its
/// canonical `Data` encoding until the container decodes it).
pub(crate) struct PendingFork<I> {
  /// The child group id's canonical `Data` encoding (1..=1024 bytes — validated at payload
  /// decode).
  pub child_bytes: Bytes,
  /// The child id's incarnation, from the committed entry.
  pub child_gen: u64,
  /// The parent's lineage counter after this split — the container's replay-guard anchor.
  pub parent_gen_after: u64,
  /// The child's recovery blob, derived AT APPLY from the just-forked half (`encode(child
  /// .snapshot())`), so blob and half correspond by construction. Persisted as the child's
  /// authoritative baseline by the manufactured snapshot install; never carried by the entry.
  pub blob: Bytes,
  /// The parent's VOTER set at the split entry (both-halves-identical by the total log order) —
  /// the child's colocated-by-construction bootstrap membership.
  pub voters: Vec<I>,
  /// The parent's ACTIVE read mode at the split entry when a committed migration set it
  /// (`None` for a never-migrated parent): the child inherits it through its baseline snapshot
  /// meta, exactly as a restart recovers a migrated mode from replicated state.
  pub read_only: Option<ReadOnlyOption>,
  /// The split entry's log index — the fork durability barrier's anchor.
  pub index: Index,
}

/// The endpoint-resident split state. One instance per endpoint, defaulted empty; restart seeds
/// the lineage from the recovered snapshot meta.
pub(crate) struct SplitState<I, F> {
  /// Committed-but-not-yet-relayed forks, in apply (log) order.
  pub(crate) pending_forks: VecDeque<PendingFork<I>>,
  /// The forked halves, index-aligned with [`pending_forks`](Self::pending_forks).
  pub(crate) forked_fsms: VecDeque<F>,
  /// THE FORK DURABILITY BARRIER: the split indexes whose forks are not yet RESOLVED (durable
  /// behind the driver's engine barrier, or dropped by the container as a duplicate/no-op).
  /// While any is outstanding, `maybe_snapshot` refuses to capture at `applied >= min` — a
  /// parent snapshot at-or-past a split would compact the entry whose replay is the staged
  /// child's only recovery source, so a correlated crash could lose the child outright. Each
  /// fork is resolved INDIVIDUALLY ([`Endpoint::resolve_fork`]): resolving a newer fork (a
  /// dropped duplicate) must never free an older, still-unflushed one.
  pub(crate) outstanding: BTreeSet<Index>,
  /// The group's LINEAGE counter — one unified monotone per-id value for incarnation and shape
  /// (seeded from the admission generation / recovered snapshot meta; bumped to
  /// `parent_gen_after` when a split applies). Stamped into every snapshot meta this endpoint
  /// captures.
  pub(crate) shape_gen: u64,
  /// The lineage recovered from DURABLE state at boot (admission generation ⊔ the restored
  /// snapshot meta), BEFORE any restart replay re-bumped [`shape_gen`](Self::shape_gen): the
  /// container seeds its replay guard from this, so a fork re-staged by restart replay is
  /// relayed again rather than dropped as a duplicate.
  pub(crate) restored_lineage: u64,
  /// Log index of the most recently appended (not-yet-applied) `Split` entry — the ONE-IN-FLIGHT
  /// propose gate (`> applied` ⇒ a split is in flight), mirroring `pending_conf_index` /
  /// `pending_read_mode_index` exactly: `propose_split` mints `parent_gen_after` from the live
  /// counter, which bumps only at apply, so a second mint before the first applies would carry
  /// the SAME bump and lose at the apply-time lineage guard. Derived state, never a sticky flag:
  /// `become_leader` re-seats it at the log's last index (an inherited unapplied split — or any
  /// tail — conservatively holds the gate until applied absorbs the accession point), and a
  /// deposed proposer's truncated entry cannot wedge anything because the gate is only ever
  /// consulted on the leader path, behind that re-seat. On restart, ZERO is acceptable exactly
  /// as on `pending_conf_index`: the gate turns permissive, and the apply-time lineage guard
  /// remains the safety floor for a stale mint.
  pub(crate) pending_split_index: Index,
  /// The encoded child id of the split this leader proposed and has not yet applied — the
  /// PROPOSE-window leg of [`Endpoint::split_reserves`]. Empty ⇔ no id reserved (an empty
  /// encoding is never a legal group id): cleared by `become_leader`'s conservative re-seat,
  /// whose inherited tail holds the propose GATE without knowing any child id — the tail's own
  /// apply stages (and thereby reserves) whatever forks it carries. Self-releasing like the
  /// index it rides beside: once `applied` absorbs `pending_split_index`, the window is over
  /// and these bytes are inert.
  pub(crate) pending_split_child: Bytes,
}

impl<I, F> SplitState<I, F> {
  pub(crate) fn new(lineage: u64) -> Self {
    Self {
      pending_forks: VecDeque::new(),
      forked_fsms: VecDeque::new(),
      outstanding: BTreeSet::new(),
      shape_gen: lineage,
      restored_lineage: lineage,
      pending_split_index: Index::ZERO,
      pending_split_child: Bytes::new(),
    }
  }
}

impl<I: fmt::Debug, F> fmt::Debug for SplitState<I, F> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("SplitState")
      .field("pending_forks", &self.pending_forks.len())
      .field("outstanding", &self.outstanding)
      .field("shape_gen", &self.shape_gen)
      .field("restored_lineage", &self.restored_lineage)
      .finish_non_exhaustive()
  }
}

impl<I, F, R> Endpoint<I, F, R>
where
  F: StateMachine,
{
  /// The oldest staged fork, without consuming it — the container's examine-before-take seam:
  /// a fork whose child id is already hosted PARKS at the head of this queue (re-examined every
  /// relay crank, never popped) so its blob survives until the conflict resolves.
  pub(crate) fn peek_pending_fork(&self) -> Option<&PendingFork<I>> {
    self.split.pending_forks.front()
  }

  /// Pop the oldest staged fork with its forked state-machine half (apply order — the FIFO the
  /// container's replay guard relies on). The two queues are pushed together at the apply arm,
  /// so they pop together. Popping does NOT resolve the fork's durability barrier — only
  /// [`resolve_fork`](Self::resolve_fork) does.
  pub(crate) fn pop_pending_fork(&mut self) -> Option<(PendingFork<I>, F)> {
    let fork = self.split.pending_forks.pop_front()?;
    let fsm = self
      .split
      .forked_fsms
      .pop_front()
      .expect("the fork and half queues are pushed together");
    Some((fork, fsm))
  }

  /// Resolve the fork staged at exactly `index`: its baseline is behind the local engine
  /// barrier, or the container dropped it as a duplicate / hosted-child no-op. Deliberately
  /// EXACT, not at-or-below: a dropped NEWER fork must not free an older, still-unflushed
  /// fork's barrier (the snapshot fence is the minimum outstanding index).
  pub(crate) fn resolve_fork(&mut self, index: Index) {
    self.split.outstanding.remove(&index);
  }

  /// The group's lineage counter (incarnation ⊔ shape), LIVE: one unified monotone per-id value,
  /// bumped by every applied split and merge entry. Public so an embedder (and the absorbing
  /// side of a merge) can read the counter the merge choreography's `*_gen_after` fields compare
  /// against.
  pub fn shape_gen(&self) -> u64 {
    self.split.shape_gen
  }

  /// The lineage recovered from durable state at boot, BEFORE any restart replay re-bumped the
  /// live counter — the container's replay-guard seed.
  pub(crate) fn restored_lineage(&self) -> u64 {
    self.split.restored_lineage
  }

  /// Seed the lineage counter at group ADMISSION — the container's create path calls this with
  /// the ADMITTED generation (the same value the coordinator's floor check validated and the
  /// driver records in its engine), so a created incarnation's counter starts where its
  /// admission says it lives rather than resetting to 0. Without the seed a floor-validated
  /// recreate re-mints its predecessor's generations, and every gen-keyed observer (a stale
  /// merge-abort obligation above all) collapses the two incarnations into one. Both the live
  /// counter and the restored view seed together: a fresh group has no durable forks, so the
  /// relay guard has nothing older to trust.
  pub(crate) fn seed_lineage(&mut self, generation: u64) {
    self.split.shape_gen = generation;
    self.split.restored_lineage = generation;
  }

  /// Whether a proposed `Split` entry is still UNAPPLIED — the container's one-in-flight propose
  /// gate (see [`SplitState::pending_split_index`] for the self-healing derivation).
  pub(crate) fn split_in_flight(&self) -> bool {
    self.split.pending_split_index > self.applied
  }

  /// Whether THIS endpoint's in-flight split machinery reserves the encoded child id
  /// `child_bytes` — the per-group leg of the coordinators' split reservation. Two windows,
  /// both derived state (releasing needs no bookkeeping): the PROPOSE window (this leader
  /// appended a split naming the id and has not applied it — over as soon as `applied` absorbs
  /// the entry, stale-mint no-op included) and the STAGED window (a committed fork naming the
  /// id sits in the relay queue, parked forks included — over when the relay pops it: a
  /// resolution arm consumes it, or a yield hands it to the driver, whose materialization runs
  /// under this very predicate and must not be refused by its own fork).
  pub(crate) fn split_reserves(&self, child_bytes: &[u8]) -> bool {
    (self.split_in_flight()
      && !self.split.pending_split_child.is_empty()
      && self.split.pending_split_child.as_ref() == child_bytes)
      || self
        .split
        .pending_forks
        .iter()
        .any(|f| f.child_bytes.as_ref() == child_bytes)
  }
}

impl<I, F, R> Endpoint<I, F, R>
where
  I: NodeId,
  F: StateMachine,
  R: rand::Rng,
  F::Command: crate::Data,
  F::Error: core::error::Error,
{
  /// Append a `Split` admin entry on the leader (the container's `propose_split` calls this
  /// after its split-specific gates pass). Mirrors the `SetReadMode` propose plumbing: the
  /// entry is appended durable-pending under the current term with the standard lease stamps,
  /// fan-out deferred to `flush_appends`. Unlike `SetReadMode`, the payload is variable-size
  /// (child id + instruction), so the single-frame bound is enforced exactly as `propose` does
  /// — a committed entry no `AppendEntries` could carry would wedge replication cluster-wide.
  pub(crate) fn propose_split_entry<L>(
    &mut self,
    now: impl Into<Now>,
    log: &mut L,
    child_bytes: Bytes,
    payload: Bytes,
  ) -> Result<Index, crate::ProposeError<I>>
  where
    L: LogStore,
  {
    use crate::ProposeError;
    let now: Now = now.into();
    if self.poison.poisoned {
      return Err(ProposeError::Poisoned);
    }
    if !self.role.is_leader() {
      return Err(ProposeError::NotLeader {
        leader: self.leader.cheap_clone(),
      });
    }
    // A leader transfer is in progress: no new proposals until it completes or times out.
    if self.transfer.lead_transferee.is_some() {
      return Err(ProposeError::LeaderTransferInProgress);
    }
    // Allocate a fresh, usable index (see `next_log_index`): refuse at the ceiling rather than alias.
    let Some(index) = Self::next_log_index(log.last_index()) else {
      return Err(ProposeError::LogIndexExhausted);
    };
    let entry = crate::Entry::new(self.term, index, crate::EntryKind::Split, payload)
      .with_timestamp(self.lease_stamp(now.mono()))
      .with_lease_window(self.lease_window_stamp())
      .with_wall_timestamp(self.lease_wall_stamp(now));
    // Refuse an entry no single frame could carry BEFORE it enters the log (see `propose`): the
    // instruction is the embedder's partition rule, bounded only by this check.
    let cost = crate::wire::entry_frame_cost(&entry);
    if cost > crate::wire::APPEND_FRAME_ENTRY_BUDGET {
      return Err(ProposeError::EntryTooLarge {
        size: cost,
        max: crate::wire::APPEND_FRAME_ENTRY_BUDGET,
      });
    }
    let opid = self.mint_op_id();
    self.submit_append(log, opid, core::slice::from_ref(&entry));
    self.push_pending(opid, Pending::LeaderAppend { upto: index });
    // Stage the append for the next `flush_appends` (see `replication_pending`).
    self.replication_pending = true;
    // One split in flight at a time (mirror `pending_read_mode_index`): the mint above read the
    // live counter, which bumps only when THIS entry applies — a second mint before then would
    // duplicate it and no-op at the apply-time lineage guard. The child id rides beside the
    // index so admission can refuse it for exactly as long as the gate holds (`split_reserves`).
    self.split.pending_split_index = index;
    self.split.pending_split_child = child_bytes;
    // The entry was ALREADY appended (durable-pending) — Ok(index) even if a later flush
    // self-poisons, exactly as `propose` reasons: it WILL commit via the durable log.
    Ok(index)
  }
}
