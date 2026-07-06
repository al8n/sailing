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
  /// The freeze entry's TERM — with `freeze_index`, the committed freeze's log identity (the
  /// carried [`CommitMergePayload::freeze_term`](crate::CommitMergePayload::freeze_term); zero
  /// when the entry carried none).
  freeze_term: Term,
  /// The gen the source's freeze set — the resolution's comparator: a source whose live counter
  /// moved PAST it was rolled back, and the parked apply aborts deterministically.
  source_gen_after: u64,
  /// The target's lineage counter after the absorb.
  target_gen_after: u64,
  /// The `CommitMerge` entry's own index `k` (the drain parked at `k - 1`).
  at: Index,
}

impl PendingMergeApply {
  /// Record a park (the apply arm's constructor).
  pub(crate) const fn new_parked(
    source_bytes: Bytes,
    freeze_index: Index,
    freeze_term: Term,
    source_gen_after: u64,
    target_gen_after: u64,
    at: Index,
  ) -> Self {
    Self {
      source_bytes,
      freeze_index,
      freeze_term,
      source_gen_after,
      target_gen_after,
      at,
    }
  }

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

  /// The freeze entry's term (the boundary's log identity; zero when the entry carried none).
  #[inline(always)]
  pub const fn freeze_term(&self) -> Term {
    self.freeze_term
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
  /// The applied `PrepareMerge` entry's TERM while frozen — with `freeze_index`, the freeze's
  /// log identity. Stamped into `CommitMergePayload` at the target's propose so a parked host
  /// can later prove its local source log holds the committed freeze (see
  /// [`Endpoint::advance_commit_on_freeze_identity`]). Same lifecycle as `freeze_index`: set at
  /// apply, cleared by the thaw, re-derived by replay (the capture fence keeps the freeze entry
  /// replayable for as long as the freeze lives).
  pub(crate) freeze_term: Option<Term>,
  /// The TARGET id the applied freeze named (its `Data` encoding, from the `PrepareMerge`
  /// payload), retained while frozen: the freeze is a CLAIM by exactly one target, and both the
  /// `commit_merge` propose gate and the park's resolve arm compare against it — without the
  /// claim, two targets naming one frozen source would race per host for the absorb (the same
  /// committed-divergence class as the cross-log rollback). Immutable for the freeze's whole
  /// generation, so reading it off the live source is host-order-independent; `None` exactly
  /// when `frozen` is false. Re-derived by replay like `frozen` itself (the merge replay fence
  /// keeps the freeze entry replayable for as long as the freeze lives).
  pub(crate) frozen_for: Option<Bytes>,
  /// The parked `CommitMerge` (target side), `Some` while the apply drain is stopped at
  /// `at - 1`. Written ONLY by the park arm and the two container resolutions.
  pub(crate) pending_apply: Option<PendingMergeApply>,
  /// Log index of the most recently appended (not-yet-applied) `CommitMerge` on THIS leader —
  /// the in-flight leg of the target's membership fence (`> applied` ⇒ a commit-merge is in
  /// flight), mirroring `pending_split_index` exactly: derived state, re-seated conservatively
  /// at `become_leader`, never a sticky flag.
  pub(crate) pending_commit_index: Index,
  /// Log index of the SOURCE-role `RollbackMerge` (thaw) this leader last appended and not yet
  /// applied (`> applied` ⇒ a thaw is in flight) — the abort-relay's IDEMPOTENT-APPEND guard:
  /// the relay is retained across cranks until the source lineage is observed past the freeze,
  /// so without this the accept arm would append a fresh thaw every crank until the first
  /// commits. Derived, self-releasing via `> applied`. Unlike the fences above it re-seats to
  /// ZERO at `become_leader`, NOT to `last`: a fresh source leader must be free to re-drive a
  /// thaw the previous leader appended but never committed (a truncated thaw would otherwise
  /// wedge the source frozen forever); the guard only suppresses THIS leader's own duplicate.
  pub(crate) thaw_pending_index: Index,
  /// The resolved absorb's `CommitMerge` index, set by `resolve_pending_merge` — the membership
  /// fence's compaction leg: the target refuses conf changes until `first_index` passes it (the
  /// forced absorb capture compacts through it within a crank), so a replica added post-merge
  /// can never be LOG-WALKED across the absorb (it would park with no local source and no
  /// floor, and no-op past the union — silent divergence; the fork milestone's (1,1) lesson).
  /// Never cleared: the check compares against the live `first_index`, so it self-releases
  /// permanently once the capture's compaction lands.
  pub(crate) absorb_index: Option<Index>,
  /// Source-unfreeze relays staged by TARGET-side abort applies, drained by the container
  /// (mirroring the fork relay): each records the abort entry's named source, and the driver
  /// proposes the SOURCE-side `RollbackMerge` on that group's own log. Replay re-stages them
  /// (the relay's restart derivation); a duplicate proposal dies at the source's own incarnation
  /// gate (`StaleThaw` once the source has thawed past the recorded generation), and a relay lost
  /// to lifecycle churn is recovered by re-proposing the target-side abort (a fresh entry, a fresh
  /// relay) — documented on `rollback_merge`.
  pub(crate) pending_aborts: VecDeque<AbortRelay>,
}

/// One staged source-unfreeze relay: a TARGET-side abort applied here, and the source it names
/// must now be thawed on its own log. G-free like the entry (the container decodes the typed id
/// when it relays).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AbortRelay {
  /// The frozen source group id's canonical `Data` encoding.
  pub(crate) source_bytes: Bytes,
  /// The freeze generation the abort abandoned (observability; the source-side mint re-reads
  /// the live frozen state at propose).
  pub(crate) source_gen_after: u64,
}

/// A parked commit's ABORT-WINDOW verdict — the target-log half of the park's resolution rule.
/// The window is the single log coordinate `k + 1` (the entry after the parked commit): until
/// something COMMITS there the merge outcome is still contestable and no arm may resolve (an
/// absorb taken while the coordinate is undecided would race an abort landing at it — one host
/// absorbed, another aborted, committed divergence); once committed, the coordinate's content
/// is immutable and identical on every replica, so the verdict below is a pure log function.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MergeWindow {
  /// Nothing committed at `k + 1` yet: hold every arm; the leader seals the window with a
  /// no-op so a quiet target cannot hold it open forever.
  Open,
  /// The committed `k + 1` is THIS merge's abort: the park resolves aborted — on every
  /// replica, because the coordinate is committed-log content.
  Abort,
  /// The committed `k + 1` is anything else: no abort can ever contest this merge again (a
  /// later abort no-ops at its lineage guard once the absorb bumps the counter), so the park
  /// may wait on the source gate and resolve.
  Closed,
  /// The coordinate is committed but not readable this crank (a cold store, a benign
  /// not-yet-visible read): hold, retry next crank.
  Stall,
}

impl<I, F, R> Endpoint<I, F, R>
where
  F: StateMachine,
{
  /// Evaluate the parked commit's abort window (see [`MergeWindow`]). Reads only THIS group's
  /// committed log — never the source's mutable state — so every replica evaluates the same
  /// bytes. The coordinate cannot have been compacted while parked: compaction happens at
  /// applied indexes, the park holds `applied` at `k - 1`, and an install past the park
  /// supersedes it entirely.
  pub(crate) fn merge_abort_window<L: LogStore>(&self, log: &L) -> MergeWindow {
    let Some(pending) = self.merge.pending_apply.as_ref() else {
      debug_assert!(false, "window read without a parked CommitMerge");
      return MergeWindow::Stall;
    };
    let coord = pending.at().next();
    if self.commit < coord {
      return MergeWindow::Open;
    }
    let read = match log.entries(coord..coord.next(), 1 << 20) {
      Ok(EntriesRead::Ready(e)) if !e.is_empty() => e,
      // Committed but not readable this pass (cold or briefly invisible): benign, retried —
      // exactly the apply drain's own treatment of the same read. A genuinely faulted store
      // is poisoned by that drain when it reaches the coordinate; the window never poisons.
      _ => return MergeWindow::Stall,
    };
    let entry = &read[0];
    if entry.kind() != EntryKind::RollbackMerge {
      return MergeWindow::Closed;
    }
    match crate::wire::decode_rollback_merge_payload(entry.data_bytes()) {
      Ok(p)
        if !p.is_unfreeze()
          && p.source_bytes() == pending.source_bytes()
          && p.source_gen_after() == pending.source_gen_after() =>
      {
        MergeWindow::Abort
      }
      // A different merge's abort (or the source-role shape, unreachable on a target's log)
      // closes the window like any other entry; a corrupt payload is the apply drain's poison
      // to raise when it reaches the coordinate — the window's verdict is still deterministic
      // (same committed bytes everywhere).
      _ => MergeWindow::Closed,
    }
  }
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

  /// The applied `PrepareMerge` entry's term while frozen (`None` when not frozen) — the
  /// boundary's log identity, stamped into the `CommitMerge` the absorbing target proposes.
  pub(crate) fn freeze_term(&self) -> Option<Term> {
    self.merge.freeze_term
  }

  /// The parked `CommitMerge` awaiting the container's resolution, if any. While `Some`, the
  /// apply drain is stopped at [`PendingMergeApply::at`]` - 1` and the group must never be
  /// treated as idle (a parked merge is resolved by the per-crank service, which a quiesced
  /// group would never reach).
  pub fn pending_merge(&self) -> Option<&PendingMergeApply> {
    self.merge.pending_apply.as_ref()
  }

  /// The TARGET id this frozen source's freeze named (`None` when not frozen) — the claim the
  /// `commit_merge` gate and the park's resolve arm verify, so exactly one target can ever
  /// absorb a given freeze generation.
  pub(crate) fn frozen_for(&self) -> Option<&Bytes> {
    self.merge.frozen_for.as_ref()
  }

  /// Pop the next staged source-unfreeze relay (a TARGET-side abort applied on this endpoint),
  /// for the container's relay drain.
  pub(crate) fn pop_pending_abort(&mut self) -> Option<AbortRelay> {
    self.merge.pending_aborts.pop_front()
  }

  /// Re-stage a source-unfreeze relay onto this target's queue — the container's REQUEUE of a
  /// relay whose thaw could not land yet (a leaderless source, or one unreachable this crank). FIFO
  /// with any others still queued; the container re-marks this target for the next abort drain, so
  /// a committed abort keeps thawing its source across source-leader churn instead of being lost
  /// to the one consume the old drain allowed.
  pub(crate) fn stage_abort_relay(&mut self, source_bytes: Bytes, source_gen_after: u64) {
    self.merge.pending_aborts.push_back(AbortRelay {
      source_bytes,
      source_gen_after,
    });
  }

  /// Whether a merge freeze is ACTIVE right now: a pending (append-observed) freeze or the
  /// applied `Frozen` state. ONE predicate for both of the freeze's early gates — the lease
  /// gates (serving and formation die at append observation) and the propose-family gates (an
  /// entry accepted above the freeze would diverge or lose the absorbed union) — so no two
  /// sites can ever disagree about a freeze.
  pub(crate) fn merge_freeze_active(&self) -> bool {
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
  F: StateMachine,
{
  /// Consume this endpoint into its state machine — the absorb extraction: the container
  /// removes the frozen source from its map and hands the FSM to the target's
  /// [`resolve_pending_merge`](Self::resolve_pending_merge). Everything else about the endpoint
  /// (its stores outlive it until the driver's teardown) is dropped; the log-derived state is
  /// re-derivable should a crash rewind the resolution.
  pub fn into_state_machine(self) -> F {
    self.fsm
  }

  /// Whether the absorb's forced snapshot capture would be REFUSED right now — the capture
  /// shares `maybe_snapshot`'s busy/fence set (a capture or install already staged, or a fork
  /// barrier at-or-below the absorb point). The container's resolve arm holds the park while
  /// this is true, so the absorb and its durability capture always land in the SAME crank.
  pub(crate) fn absorb_capture_blocked(&self) -> bool {
    let Some(pending) = self.merge.pending_apply.as_ref() else {
      return false;
    };
    self.snapshot.pending_compact.is_some()
      || self.snapshot.pending_install.is_some()
      || self
        .split
        .outstanding
        .first()
        .is_some_and(|cap| *cap <= pending.at())
  }

  /// Resolve the parked `CommitMerge` by ABSORBING the extracted source state machine: fold it
  /// in, mark the parked entry applied, bump the lineage, and surface `Event::Merged`. The
  /// ONLY writer of a successful resolution; the caller (the container's per-crank service)
  /// verified the source was frozen-applied at the boundary with the expected gen, so the
  /// absorbed state is identical on every replica — log-matching plus deterministic apply up
  /// to the boundary, with nothing FSM-mutating above a surviving freeze.
  ///
  /// An FSM whose `absorb` returns `false` (the defaulted unsupported verdict) poisons —
  /// deterministic on every replica, mirroring `SplitUnsupported`: never a silent skip that
  /// diverges absorbed replicas from refusing ones.
  pub(crate) fn resolve_pending_merge(&mut self, source_fsm: F) {
    let Some(pending) = self.merge.pending_apply.take() else {
      debug_assert!(false, "resolve without a parked CommitMerge");
      return;
    };
    if !self.fsm.absorb(source_fsm) {
      self.poison(PoisonReason::MergeUnsupported);
      return;
    }
    self.applied = pending.at();
    self.split.shape_gen = self.split.shape_gen.max(pending.target_gen_after());
    self.merge.absorb_index = Some(pending.at());
    self
      .outputs
      .events
      .push_back(crate::Event::Merged(crate::Merged::new(
        pending.at(),
        pending.source_bytes(),
      )));
  }

  /// Resolve the parked `CommitMerge` as a deterministic NO-OP: the source's log settled the
  /// race against this commit (a rollback moved its lineage past the expected gen), or the
  /// entry is a replayed/duplicate commit for an already-absorbed source. Advances past the
  /// parked entry WITHOUT touching the state machine or the lineage and surfaces
  /// `Event::MergeAborted`; the drain resumes on the next storage crank.
  pub(crate) fn resolve_pending_merge_aborted(&mut self) {
    let Some(pending) = self.merge.pending_apply.take() else {
      debug_assert!(false, "abort-resolve without a parked CommitMerge");
      return;
    };
    self.applied = pending.at();
    self
      .outputs
      .events
      .push_back(crate::Event::MergeAborted(crate::MergeAborted::new(
        pending.at(),
        pending.source_bytes(),
      )));
  }

  /// Whether a membership change is still in flight (appended, not yet applied) — the merge
  /// verbs' precondition read (the propose gate itself lives in `propose_conf_change_v2`).
  pub(crate) fn conf_change_in_flight(&self) -> bool {
    self.pending_conf_index > self.applied
  }

  /// Whether a `CommitMerge` proposed by THIS leader is still unapplied — the target-side
  /// one-at-a-time gate's in-flight leg (the parked leg is `pending_merge()`).
  pub(crate) fn commit_merge_in_flight(&self) -> bool {
    self.merge.pending_commit_index > self.applied
  }

  /// The index of a SOURCE-role `RollbackMerge` (thaw) this leader has appended but not yet
  /// applied, if any — the abort-relay's idempotent-append guard. `Some` means the accept arm
  /// must RETAIN and wait rather than append a duplicate; the retained relay retires only once
  /// the thaw commits, applies, and the source lineage is observed past the freeze.
  pub(crate) fn thaw_in_flight(&self) -> Option<Index> {
    (self.merge.thaw_pending_index > self.applied).then_some(self.merge.thaw_pending_index)
  }

  /// Record a SOURCE-role thaw this leader just appended at `index` — arms
  /// [`thaw_in_flight`](Self::thaw_in_flight) so the relay's next crank does not pile on a
  /// duplicate `RollbackMerge` while this one is in flight.
  pub(crate) fn note_thaw_appended(&mut self, index: Index) {
    self.merge.thaw_pending_index = index;
  }

  /// The target-side membership fence: whether a conf change must refuse because a merge is in
  /// flight (proposed, parked, or absorbed-but-not-yet-compacted). Adding a replica in any of
  /// those windows lets it be LOG-WALKED across the absorb point — it parks there with no local
  /// source and no floor, no-ops past the union, and silently diverges (the fork milestone's
  /// log-walk lesson). The three legs release on their own: apply absorbs the in-flight index,
  /// resolution consumes the park, and the absorb capture's compaction moves `first_index`
  /// past the absorb point within a crank.
  pub(crate) fn merge_conf_fence<L: LogStore>(&self, log: &L) -> bool {
    self.merge.pending_commit_index > self.applied
      || self.merge.pending_apply.is_some()
      || self
        .merge
        .absorb_index
        .is_some_and(|k| log.first_index() <= k)
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
  /// The absorb's FORCED snapshot capture, run by the container immediately after
  /// [`resolve_pending_merge`](Self::resolve_pending_merge) with the target's stores in hand:
  /// capture at `applied` (the absorb point) regardless of the snapshot threshold, so the
  /// union's durability anchor and the source's teardown ride the SAME barrier — a crash either
  /// rewinds to re-park (nothing durable moved) or restarts the target at a boundary past the
  /// absorb (never a parked apply whose source was already destroyed). The caller checked
  /// [`absorb_capture_blocked`](Self::absorb_capture_blocked) before resolving.
  ///
  /// Returns whether the capture actually STAGED `pending_compact` — the union's durable anchor.
  /// The container's resolve arm emits `Merged` (the driver's permission to floor the source and
  /// drop its stores) ONLY on `true`: a `snapshot()` or log fault poisons and returns `false`, so
  /// an absorb whose union could not be anchored fail-stops with the source's stores intact rather
  /// than authorizing a teardown over a union no durable target snapshot covers.
  #[must_use]
  pub(crate) fn capture_absorb_snapshot<L, S>(&mut self, log: &L, stable: &mut S) -> bool
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
    F::Snapshot: crate::Data,
  {
    if self.poison.poisoned {
      return false;
    }
    debug_assert!(
      self.snapshot.pending_compact.is_none() && self.snapshot.pending_install.is_none(),
      "the resolve arm holds the park while a capture/install is staged"
    );
    let snap = match self.fsm.snapshot() {
      Ok(s) => s,
      Err(_) => {
        self.poison(PoisonReason::SnapshotCapture);
        return false;
      }
    };
    use crate::Data as _;
    let mut data = std::vec::Vec::new();
    snap.encode(&mut data);
    let Some(last_term) = self.log_term(log, self.applied) else {
      // The absorb point must be readable to stamp the snapshot meta; a fault here leaves the
      // union unanchored — fail-stop rather than stage nothing and let the caller emit Merged.
      self.poison(PoisonReason::LogRead);
      return false;
    };
    let mut meta = crate::SnapshotMeta::new(self.applied, last_term, self.conf_state())
      .with_max_lease_window(self.lease_guard.max_lease_window)
      .with_max_wall_plus_window(self.lease_guard.max_wall_plus_window)
      .with_max_unwalled_lease_window(self.lease_guard.max_unwalled_lease_window)
      .with_shape_gen(self.split.shape_gen);
    if self.reads.read_mode_migrated {
      meta = meta.with_read_only(self.reads.active_read_mode);
    }
    let opid = self.mint_op_id();
    self.submit_snapshot(stable, opid, meta, bytes::Bytes::from(data));
    self.snapshot.pending_compact = Some((opid, self.applied));
    true
  }

  /// Seal a parked commit's abort window: while the window is OPEN and this leader's log still
  /// ENDS at the parked entry, append one no-op so the `k + 1` coordinate gets decided — a
  /// quiet target would otherwise hold every replica's park open forever (client traffic and
  /// election no-ops seal it incidentally; this is the guaranteed leg). Idempotent per park:
  /// once anything sits above `k` the append is skipped, and a truncated seal is replaced by
  /// the successor leader's own election no-op. Returns whether a no-op was appended (the
  /// caller flushes the fan-out inline so sealing never waits on heartbeat cadence).
  pub(crate) fn ensure_merge_seal<L: LogStore>(&mut self, now: Now, log: &mut L) -> bool {
    if !self.role.is_leader() {
      return false;
    }
    let Some(pending) = self.merge.pending_apply.as_ref() else {
      return false;
    };
    if log.last_index() != pending.at() {
      return false;
    }
    self.append_leader_noop(now, log, pending.at()).is_some()
  }

  /// Advance this SOURCE replica's commit (and apply) to the freeze boundary on LOG IDENTITY —
  /// the parked-target service's leaderless-source leg. Returns whether the local log proved
  /// the identity (idempotently true once past the boundary); on any miss it changes nothing.
  ///
  /// Soundness: the caller holds a COMMITTED `CommitMerge` carrying `(boundary, freeze_term)`,
  /// and its proposer stamped that pair from a source it observed frozen-applied at the
  /// boundary — so the freeze entry `(boundary, freeze_term)` is committed in the source group.
  /// If this replica's log holds an entry with that exact index and term, the Log Matching
  /// property makes it (and its whole prefix) byte-identical to the committed one, so raising
  /// `commit` here is the same knowledge transfer as a leader's commit index riding an
  /// AppendEntries — with the dead source leader's say-so carried by the target's log instead
  /// of a heartbeat that may never come: match alone is not commit KNOWLEDGE, and a source
  /// follower stranded at `commit < boundary` after the absorb consumed the rest of the quorum
  /// (leaderless, under-hosted, unelectable) would otherwise park its host forever. Never on
  /// index alone: a divergent entry at the boundary (wrong term) proves nothing about the
  /// prefix and MUST keep waiting. A LeaseGuard commit-wait needs no re-check here: the freeze
  /// in the advanced range kills lease serving itself, so the early advance can never surface
  /// a lease read.
  pub(crate) fn advance_commit_on_freeze_identity<L>(
    &mut self,
    log: &L,
    boundary: Index,
    freeze_term: Term,
  ) -> bool
  where
    L: LogStore,
    F::Snapshot: crate::Data,
  {
    if self.poison.poisoned || freeze_term == Term::ZERO {
      return false;
    }
    if log.last_index() < boundary {
      return false;
    }
    let Some(t) = self.log_term(log, boundary) else {
      return false;
    };
    if t != freeze_term {
      return false;
    }
    if self.commit < boundary {
      self.commit = boundary;
    }
    if self.applied < self.commit {
      self.apply_committed(log);
    }
    true
  }

  /// THE MERGE-PARK SAFETY PIN: whether a parked apply has left this replica's voter set
  /// SUPERSEDED — a committed `ConfChange` sits above the parked `CommitMerge`, unapplied. The
  /// park freezes apply at `k - 1` while the committed log races ahead; membership is apply-time,
  /// so the tracker this replica would count a quorum (or an election) against is the STALE
  /// pre-park configuration. Winning a campaign on that superseded set — or advancing commit
  /// against it as a leader — truncates entries the real (shrunk/grown) configuration already
  /// committed (a below-floor read). Callers must refuse to campaign / count quorum when this
  /// holds. Returns false when NOT parked (apply tracks commit tracks membership — current by
  /// construction) or when nothing committed-but-unapplied changes the voter set. Fail-closed on
  /// an unreadable range: a config that cannot be proven current is treated as superseded.
  pub(crate) fn merge_park_membership_superseded<L: LogStore>(&self, log: &L) -> bool {
    if self.merge.pending_apply.is_none() {
      return false;
    }
    let end = self.commit.next();
    let mut cursor = self.applied.next();
    while cursor < end {
      let batch = match log.entries(cursor..end, 1 << 20) {
        Ok(EntriesRead::Ready(b)) if !b.is_empty() => b,
        // A cold/short/empty read cannot prove the committed-unapplied range holds no config
        // change → fail closed (refuse to campaign this pass; a warm retry re-evaluates).
        _ => return true,
      };
      for entry in &*batch {
        if entry.kind() == EntryKind::ConfChange {
          return true;
        }
      }
      cursor = match batch.last() {
        Some(e) => e.index().next(),
        None => return true,
      };
    }
    false
  }

  /// Whether every tracked peer (voters AND learners, both joint halves) has MATCHED the log
  /// through `boundary` — meaningful on the LEADER (its tracker holds the peers' proven match).
  /// The merge's all-source-voters freeze barrier (the CRDB `waitForApplication` shape) reads it
  /// off the frozen SOURCE leader at `commit_merge` admission: the absorb is not proposed until
  /// every source voter provably holds the freeze, so the committed `CommitMerge` that dissolves
  /// the source certifies the whole voter set already has it. A voter left below the boundary
  /// would otherwise be orphaned once the source leader is lost — the other hosts floor and
  /// dismantle the source around it, and its co-located target parks with no way to advance or
  /// be snapshotted past its local source.
  pub(crate) fn peers_matched_through(&self, boundary: Index) -> bool {
    let me = self.config.id();
    self
      .tracker
      .progress_map()
      .iter()
      .all(|(peer, pr)| *peer == me || pr.match_index() >= boundary)
  }

  /// Append one merge admin entry (`PrepareMerge`/`CommitMerge`/`RollbackMerge`) on the leader —
  /// the container's merge verbs call this after their merge-specific gates pass. Mirrors
  /// `propose_split_entry`: appended durable-pending under the current term with the standard
  /// lease stamps, fan-out deferred to `flush_appends`, the single-frame bound enforced (the
  /// payload carries one bounded group tag, so the bound is unreachable in practice but kept for
  /// uniformity). A `PrepareMerge` sets the append-observed lease kill HERE — the leader appends
  /// before it replicates, which is what puts `append(freeze)` after every lease read this
  /// leader ever served.
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
    if kind == EntryKind::CommitMerge {
      self.merge.pending_commit_index = index;
    }
    Ok(index)
  }
}
