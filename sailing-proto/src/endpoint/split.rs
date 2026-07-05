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
}

impl<I, F> SplitState<I, F> {
  pub(crate) fn new(lineage: u64) -> Self {
    Self {
      pending_forks: VecDeque::new(),
      forked_fsms: VecDeque::new(),
      outstanding: BTreeSet::new(),
      shape_gen: lineage,
      restored_lineage: lineage,
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

  /// The group's lineage counter (incarnation ⊔ shape), LIVE: includes every applied split.
  pub(crate) fn shape_gen(&self) -> u64 {
    self.split.shape_gen
  }

  /// The lineage recovered from durable state at boot, BEFORE any restart replay re-bumped the
  /// live counter — the container's replay-guard seed.
  pub(crate) fn restored_lineage(&self) -> u64 {
    self.split.restored_lineage
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
    // The entry was ALREADY appended (durable-pending) — Ok(index) even if a later flush
    // self-poisons, exactly as `propose` reasons: it WILL commit via the durable log.
    Ok(index)
  }
}
