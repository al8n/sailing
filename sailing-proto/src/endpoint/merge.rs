//! The endpoint-resident merge state: the append-observed freeze, the applied `Frozen` fold, and
//! the parked `CommitMerge` apply the container resolves from local facts.
//!
//! The lease SAFETY gate moves to APPEND observation (`freeze_pending`): every lease-serve and
//! lease-formation gate fails closed from the moment a `PrepareMerge` entry ENTERS the local log
//! — the proposing leader appends before it replicates, and every lease is served leader-side, so
//! the total order `emit(read) < append(freeze) < commit < apply < absorb < accept(write)` holds
//! with NO commit-wait and NO cross-node clock anywhere. The remaining freeze semantics stay
//! apply-time (the membership-apply-time doctrine's shape).
use super::*;

/// One committed `CommitMerge`, parked at the target's apply until the container resolves it: the
/// endpoint alone CANNOT apply it — the absorbed half lives in another group's endpoint, which
/// only the container holds. `applied` stays at `at - 1` while parked; everything else about the
/// target keeps running (elections, replication, reads confirm at the commit index and serve once
/// the resolution advances `applied`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingMergeApply {
  /// The absorbed (source) group id's canonical `Data` encoding (G-free, like the entry).
  source_bytes: Bytes,
  /// The source's freeze boundary: the local source replica must be frozen-applied at (or past —
  /// only FSM-no-ops can follow a surviving freeze) this index before the absorb can resolve.
  freeze_index: Index,
  /// The gen the source's freeze set — the resolution's comparator: a source whose live counter
  /// moved PAST it was rolled back, and the parked apply aborts deterministically.
  source_gen_after: u64,
  /// The target's lineage counter after the absorb.
  target_gen_after: u64,
  /// The `CommitMerge` entry's own index `k` (the drain parked at `k - 1`).
  at: Index,
}

impl PendingMergeApply {
  /// The absorbed (source) group id's canonical `Data` encoding (an O(1) shared handle).
  #[inline(always)]
  pub fn source_bytes(&self) -> Bytes {
    self.source_bytes.clone()
  }

  /// The source's freeze boundary.
  #[inline(always)]
  pub const fn freeze_index(&self) -> Index {
    self.freeze_index
  }

  /// The gen the source's freeze set (the resolution comparator).
  #[inline(always)]
  pub const fn source_gen_after(&self) -> u64 {
    self.source_gen_after
  }

  /// The target's lineage counter after the absorb.
  #[inline(always)]
  pub const fn target_gen_after(&self) -> u64 {
    self.target_gen_after
  }

  /// The parked `CommitMerge` entry's log index.
  #[inline(always)]
  pub const fn at(&self) -> Index {
    self.at
  }
}

/// The endpoint-resident merge state. One instance per endpoint, defaulted inert; every field is
/// DERIVED from the log and re-derivable at restart (`freeze_pending` from the unapplied suffix,
/// `frozen`/`freeze_index` from replaying the applied prefix, the park from re-encountering its
/// entry), so nothing here is persisted. The lineage counter deliberately does NOT live here —
/// incarnation and shape share ONE monotone per-id counter (`SplitState::shape_gen`).
#[derive(Debug, Default)]
pub(crate) struct MergeState {
  /// The LOWEST unresolved `PrepareMerge` index in the local log, observed at APPEND (leader
  /// propose-append / follower append-accept; a kind check only — never a payload decode on the
  /// hot path). While set, every lease-serve and lease-formation gate fails closed. Cleared by a
  /// conflict truncation covering it, by a log re-baseline discarding it (snapshot install), by
  /// its entry applying (subsumed into `frozen`), or by a `RollbackMerge` applying (which
  /// re-derives it — a later freeze may still sit in the unapplied suffix). Restart re-derives it
  /// from one bounded kind-only pass over the unapplied suffix, so a committed-but-unapplied
  /// freeze is re-armed before the replica can win an election and form a fresh lease; an
  /// election itself never clears it (log-derived state — a new leader inherits it with the log).
  pub(crate) freeze_pending: Option<Index>,
  /// Whether a committed `PrepareMerge` has APPLIED and no later `RollbackMerge` has undone it:
  /// the full freeze — proposals, conf changes, transfers, and reads refuse typed; heartbeats,
  /// appends, elections, and snapshot sends run UNCHANGED (the group must stay live to propagate
  /// its own freeze and survive leader crashes).
  pub(crate) frozen: bool,
  /// The applied `PrepareMerge` entry's index while frozen — the boundary the absorbing target
  /// gates on. `None` exactly when `frozen` is false.
  pub(crate) freeze_index: Option<Index>,
  /// The parked `CommitMerge` (target side), `Some` while the apply drain is stopped at
  /// `at - 1`. Written ONLY by the park arm and the two container resolutions.
  pub(crate) pending_apply: Option<PendingMergeApply>,
}

impl<I, F, R> Endpoint<I, F, R>
where
  F: StateMachine,
{
  /// Whether this group is FROZEN by an applied `PrepareMerge` (and not since rolled back): it
  /// refuses proposals/conf changes/transfers/reads typed, while replication and elections run
  /// unchanged. See [`freeze_index`](Self::freeze_index) for the boundary.
  pub fn is_frozen(&self) -> bool {
    self.merge.frozen
  }

  /// The applied `PrepareMerge` entry's index while frozen (`None` when not frozen) — the
  /// boundary an absorbing target's parked `CommitMerge` gates on.
  pub fn freeze_index(&self) -> Option<Index> {
    self.merge.freeze_index
  }

  /// The parked `CommitMerge` awaiting the container's resolution, if any. While `Some`, the
  /// apply drain is stopped at [`PendingMergeApply::at`]` - 1` and the group must never be
  /// treated as idle (a parked merge is resolved by the per-crank service, which a quiesced
  /// group would never reach).
  pub fn pending_merge(&self) -> Option<&PendingMergeApply> {
    self.merge.pending_apply.as_ref()
  }

  /// Whether merge state kills lease serving and formation RIGHT NOW: a pending (append-observed)
  /// freeze or the applied `Frozen` state. Folded into every lease-serve gate, the CheckQuorum
  /// renewal, and the proactive-refresh triggers — one predicate, so the serve and formation
  /// sides can never disagree about a freeze.
  pub(crate) fn merge_lease_killed(&self) -> bool {
    self.merge.freeze_pending.is_some() || self.merge.frozen
  }

  /// Observe a `PrepareMerge` entering the local log at `index` — the APPEND-time lease kill.
  /// Keeps the LOWEST unresolved index: the clear-by-truncation predicate compares against the
  /// first freeze still in the log, and any higher duplicate dies with the same (or a later)
  /// truncation.
  pub(crate) fn note_freeze_appended(&mut self, index: Index) {
    if self.merge.freeze_pending.is_none_or(|cur| index < cur) {
      self.merge.freeze_pending = Some(index);
    }
  }

  /// A §5.3 conflict truncation overwrote `[truncate_from, ..]`: a pending freeze at-or-above it
  /// no longer exists in the log, so the append-observed kill releases. (A truncation strictly
  /// above the pending index leaves it standing — the freeze entry survived.)
  pub(crate) fn note_freeze_truncated(&mut self, truncate_from: Index) {
    if self
      .merge
      .freeze_pending
      .is_some_and(|fp| truncate_from <= fp)
    {
      self.merge.freeze_pending = None;
    }
  }

  /// A snapshot install re-baselined the log to `boundary`, discarding every entry above it: a
  /// pending freeze above the boundary was discarded with them — clear it; the ordinary append
  /// re-delivery of a still-live freeze re-arms it at accept. (A pending freeze at-or-below the
  /// boundary is structurally impossible — compaction happens only at applied indexes, and an
  /// applied `PrepareMerge` already cleared the pending state into `frozen` — so the clear is
  /// total rather than conditional; a stale flag surviving here would kill leases forever on a
  /// node whose freeze entry no longer exists.)
  pub(crate) fn note_freeze_rebaselined(&mut self) {
    self.merge.freeze_pending = None;
  }

  /// One bounded, kind-only pass over the UNAPPLIED suffix `(applied, last]` for the lowest
  /// `PrepareMerge` — the restart re-derivation of [`MergeState::freeze_pending`] and the
  /// re-derivation a `RollbackMerge` apply runs (a LATER freeze may already sit above it in the
  /// suffix). FAIL-STOP on any read fault, like the restart lease-floor scans: under-deriving
  /// the kill would let a restarted replica win an election and serve a lease inside a pending
  /// freeze — a stale read.
  pub(crate) fn scan_freeze_pending<L: LogStore>(
    log: &L,
    applied: Index,
  ) -> Result<Option<Index>, PoisonReason> {
    let last = log.last_index();
    let mut idx = applied.next();
    while idx <= last {
      let read_end = last
        .next()
        .min(Index::new(idx.get().saturating_add(MAX_READ_BATCH_ENTRIES)));
      let chunk = match log.entries(idx..read_end, 1 << 20) {
        Ok(EntriesRead::Ready(c)) if !c.is_empty() => c,
        _ => return Err(PoisonReason::LogRead),
      };
      for e in chunk.iter() {
        if e.kind() == EntryKind::PrepareMerge {
          return Ok(Some(e.index()));
        }
      }
      idx = chunk
        .last()
        .map(|e| e.index().next())
        .ok_or(PoisonReason::LogRead)?;
    }
    Ok(None)
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
  /// Append one merge admin entry (`PrepareMerge`/`CommitMerge`/`RollbackMerge`) on the leader —
  /// the container's merge verbs call this after their merge-specific gates pass. Mirrors
  /// `propose_split_entry`: appended durable-pending under the current term with the standard
  /// lease stamps, fan-out deferred to `flush_appends`, the single-frame bound enforced (the
  /// payload carries one bounded group tag, so the bound is unreachable in practice but kept for
  /// uniformity). A `PrepareMerge` sets the append-observed lease kill HERE — the leader appends
  /// before it replicates, which is what puts `append(freeze)` after every lease read this
  /// leader ever served.
  // The expectation self-expires the moment the container's merge verbs land (an unfulfilled
  // `expect` is a lint error), so it cannot outlive its reason.
  #[cfg_attr(not(test), expect(dead_code))]
  pub(crate) fn propose_merge_entry<L>(
    &mut self,
    now: impl Into<Now>,
    log: &mut L,
    kind: EntryKind,
    payload: Bytes,
  ) -> Result<Index, crate::ProposeError<I>>
  where
    L: LogStore,
  {
    use crate::ProposeError;
    debug_assert!(matches!(
      kind,
      EntryKind::PrepareMerge | EntryKind::CommitMerge | EntryKind::RollbackMerge
    ));
    let now: Now = now.into();
    if self.poison.poisoned {
      return Err(ProposeError::Poisoned);
    }
    if !self.role.is_leader() {
      return Err(ProposeError::NotLeader {
        leader: self.leader.cheap_clone(),
      });
    }
    if self.transfer.lead_transferee.is_some() {
      return Err(ProposeError::LeaderTransferInProgress);
    }
    let Some(index) = Self::next_log_index(log.last_index()) else {
      return Err(ProposeError::LogIndexExhausted);
    };
    let entry = crate::Entry::new(self.term, index, kind, payload)
      .with_timestamp(self.lease_stamp(now.mono()))
      .with_lease_window(self.lease_window_stamp())
      .with_wall_timestamp(self.lease_wall_stamp(now));
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
    self.replication_pending = true;
    if kind == EntryKind::PrepareMerge {
      self.note_freeze_appended(index);
    }
    Ok(index)
  }
}
