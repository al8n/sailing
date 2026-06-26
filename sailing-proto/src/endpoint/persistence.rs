use super::*;
use crate::{AppendResponse, HardState, LogDone, SnapshotResponse, StableDone, StorageProgress};

impl<I, F, R> Endpoint<I, F, R>
where
  I: NodeId,
  F: StateMachine,
  R: rand::Rng,
{
  /// The match-ack ceiling: the highest log index a follower may report as matched. EVERY outbound
  /// follower success-ack clamps to this single bound (persist-before-ack) — centralizing it is what
  /// guarantees no ack site can drift, since an over-ack lets the leader count a phantom replica and
  /// commit an entry a crash would lose.
  ///
  /// This is the follower's true durable RECOVERABLE prefix: `max(durable_index, durable_snapshot_index)`
  /// — the durable log tip OR a durable snapshot boundary, whichever is higher. Normally they coincide
  /// (a completed install re-baselines the log, so `durable_index == boundary`, and a snapshot always
  /// sits at/below the durable log tip). They DIVERGE only when a deferred install is dropped as stale
  /// the blob is durable but the log was not re-baselined, so `durable_index` stays below the
  /// boundary while a crash would `reconcile_restart_log::Restore` to the snapshot — so acking the
  /// boundary is honest, and NOT acking it would pin the leader in `ProgressState::Snapshot`. Acking a
  /// durable snapshot boundary is also phantom-safe: a snapshot's boundary is already quorum-committed
  /// (a leader only snapshots committed state), so the leader cannot newly-commit it.
  pub(crate) fn ack_watermark(&self) -> Index {
    core::cmp::max(
      self.durable.durable_index,
      self.durable.durable_snapshot_index,
    )
  }

  /// The commit watermark that is actually backed by the DURABLE log: `min(commit, durable_index)`.
  /// EVERY `HardState.commit` persist stamps THIS, never raw `self.commit`, so a crash can never leave
  /// a durable commit above the durable log — a state restart would otherwise have to silently lower
  /// (discarding the persisted commitment, hiding a durability-ordering bug). The leader's `commit` is
  /// already durable (it counts only durable matches), so this fences only the follower window where
  /// `commit` advances over a visible-but-not-yet-durable tail; that suffix is re-synced after a crash
  /// (Leader Completeness), so persisting only the durable prefix loses no committed entry. Monotonic
  /// non-decreasing (`commit` is monotonic; `durable_index` only ever resets upward-relative-to-commit
  /// on §5.3 truncation and snapshot install), so it never regresses the persisted watermark.
  ///
  /// A follower snapshot install needs no special fence here: it is DEFERRED until the blob is
  /// durable, so `commit` and `durable_index` are both advanced to the boundary by `install_snapshot_now`
  /// only AFTER the blob lands — `min(commit, durable_index)` is then a boundary the durable snapshot
  /// already backs, never above durable storage.
  pub(crate) fn durable_commit(&self) -> Index {
    core::cmp::min(self.commit, self.durable.durable_index)
  }

  /// Storage-submission choke-point: a poisoned node must never persist new work. Routing every
  /// `submit_*` through these wrappers makes "poisoned ⇒ no durable write" hold BY CONSTRUCTION,
  /// for any caller — public API, handler, or future code — not just the ones that remember to check.
  /// Together with the egress emit-halt (`poll_*` return `None` when poisoned) this means a poisoned
  /// node can neither persist nor emit, regardless of which entry point is exercised.
  pub(crate) fn submit_append<L: LogStore>(
    &mut self,
    log: &mut L,
    id: OpId,
    entries: &[crate::Entry],
  ) {
    if self.poison.poisoned {
      return;
    }
    log.submit_append(id, entries);
    // Raise the self-describing LeaseGuard commit-wait bound over these entries' stamped lease
    // windows (each carries its appending leader's own exact window). The choke-point for EVERY append — leader
    // propose/no-op/conf-change AND follower replication — so a node always bounds any deposed
    // leader's lease on an entry it holds. Monotonic in memory; recomputed from durable state at
    // restart. `0` (non-LeaseGuard / inactive-config entries) never raises it.
    for e in entries {
      self.lease_guard.max_lease_window = self.lease_guard.max_lease_window.max(e.lease_window());
      // The precise-anchor release floor: the per-entry wall stamp PLUS its window (paired per
      // entry, never the max stamp with a different entry's window). ONLY a real (non-zero) wall AND a
      // real (non-zero) lease window contribute — the exact dual of the `max_unwalled_lease_window` fold
      // below (`lease_window > 0 && wall_timestamp == 0`). An ABSENT wall (`wall_timestamp == 0`:
      // non-failover LeaseGuard, or a fail-closed failover entry) folds NOTHING here; a wall WITHOUT a
      // lease window (a degenerate `lease_window == 0` walled entry — never produced by a valid failover
      // leader, since `lease_window_stamp` and `lease_wall_stamp` both fire only on the active tier, but
      // possible from an arbitrary inbound wire entry) is not a lease horizon and is skipped, so THIS
      // floor stays `0` outside the failover tier and never folds a non-lease wall. `saturating_add` is
      // defensive — `wall_timestamp` is nanos-since-epoch + a small window, never overflowing u64.
      if e.wall_timestamp() != 0 && e.lease_window() > 0 {
        self.lease_guard.max_wall_plus_window = self
          .lease_guard
          .max_wall_plus_window
          .max(e.wall_timestamp().saturating_add(e.lease_window()));
      }
      // The mono-frame fallback bound for inherited entries that are LEASE-bearing but WALL-ABSENT.
      // Gated by the ENTRY property (`lease_window > 0 && wall_timestamp == 0`) — the exact dual of the
      // wall floor above (`wall_timestamp != 0`) — NOT by the local failover tier: an entry-property gate
      // folds the same value on every node that holds the entry, so the floor stays complete across
      // heterogeneous per-node tiers (a per-node-config gate could not). On a non-failover LeaseGuard
      // cluster this equals `max_lease_window` (every entry is wall-absent), but is inert there — the
      // sole consumer, `precise_release_ready`, returns false off-tier. Safe/LeaseBased keep it 0
      // (`lease_window` is 0).
      if e.lease_window() > 0 && e.wall_timestamp() == 0 {
        self.lease_guard.max_unwalled_lease_window = self
          .lease_guard
          .max_unwalled_lease_window
          .max(e.lease_window());
      }
    }
    // Track this append's last index independently of `pending` so `on_log_appended` can advance
    // `durable_index` unconditionally when the completion fires (see the field comment).
    if let Some(last) = entries.last() {
      self.durable.inflight_append_upto.insert(id, last.index());
    }
  }

  /// Scrub state that references log entries ABOVE `boundary` — used wherever the log tail is discarded
  /// (a §5.3 conflict truncation OR a snapshot install). A queued success `AppendResponse` or a pending
  /// `FollowerAck` for an index past `boundary` would otherwise over-ack an entry the node no longer
  /// stores, letting the leader count a phantom replica toward commit.
  pub(crate) fn scrub_acks_above(&mut self, boundary: Index) {
    self.outputs.outgoing.retain(|o| {
      !matches!(o.message(), Message::AppendResponse(a) if !a.reject() && a.match_index() > boundary)
    });
    self.pending.retain(|_, p| match p {
      Pending::FollowerAck { match_index, .. } => *match_index <= boundary,
      _ => true,
    });
    // a DEFERRED success ack also caps its proven match to the new boundary — the discarded
    // tail can no longer back it. (The flush already clamps by `ack_watermark()`, which regresses here,
    // so this is defense-in-depth that keeps the stored extent honest.)
    if let Some((to, term, proven)) = self.durable.term_gated_append_ack.as_ref()
      && *proven > boundary
    {
      let capped = Some((to.cheap_clone(), *term, boundary));
      self.durable.term_gated_append_ack = capped;
    }
    if let Some((to, term, proven)) = self.durable.term_gated_snapshot_ack.as_ref()
      && *proven > boundary
    {
      let capped = Some((to.cheap_clone(), *term, boundary));
      self.durable.term_gated_snapshot_ack = capped;
    }
  }

  /// Fold every MONOTONE durable safety floor onto a HardState about to be written — the single extension
  /// point for the choke-point floor-preservation invariant. `submit_write` calls this so EVERY write
  /// preserves the floor regardless of which builder produced `hs` or what `stable.hard_state()` returned:
  /// the `StableStore` trait documents `hard_state()` as LAST-DURABLE, so a conforming store can hand a
  /// writer (vote grant, commit watermark, campaign) a STALE floor while a raise is still in flight — a
  /// later write rebuilt from it would then ERASE the durable promise. Folding the IN-MEMORY raised floor
  /// here (NOT a re-read value) makes the durable floor monotone by construction (`raise` never lowers it,
  /// and also upgrades a legacy `Unrecorded` record to `Recorded` — the self-heal). A future monotone field
  /// `F` adds exactly ONE line here: `.with_f(hs.f().max(self.f_floor))`.
  pub(crate) fn stamp_floors(&self, hs: HardState<I>) -> HardState<I> {
    let raised = hs.lease_support().raise(self.durable.lease_support_floor);
    hs.with_lease_support(raised)
  }

  pub(crate) fn submit_write<S: StableStore<NodeId = I>>(
    &mut self,
    stable: &mut S,
    id: OpId,
    hard_state: HardState<I>,
  ) {
    if self.poison.poisoned {
      return;
    }
    // fold every monotone durable safety floor onto this write at the choke-point (see `stamp_floors`),
    // so the durable floor is preserved by construction regardless of the builder or what `hard_state()`
    // returned.
    let hard_state = self.stamp_floors(hard_state);
    // remember the FIRST write that carries each newly-higher term, so `term_is_durable` can tell
    // when `self.term` has actually reached stable storage. Terms are monotonic and all HardState writes
    // carry the current term, so the first write at a higher term establishes that term's durability.
    if hard_state.term() > self.durable.last_submitted_term {
      self.durable.last_submitted_term = hard_state.term();
      self.durable.term_persist_opid = id;
    }
    // the same watermark for the monotone-increasing lease-support floor (MAGNITUDE), so
    // `on_stable_wrote` can tell when a newly-RAISED floor has reached stable storage and the follower may
    // begin advertising its real lease support (the persist-before-advertise gate in `on_heartbeat`).
    if hard_state.lease_support().promised() > self.durable.last_submitted_lease_support {
      self.durable.last_submitted_lease_support = hard_state.lease_support().promised();
      self.durable.lease_support_persist_opid = id;
    }
    stable.submit_write(id, hard_state);
  }

  /// Whether `self.term`'s HardState write has reached stable storage. A follower must not RESPOND to an
  /// RPC under a term that is not yet durable (Raft §5.1), so the success-ack paths gate on this and
  /// defer (via `term_gated_*_ack`) until [`on_stable_wrote`] flushes them. True trivially for the
  /// initial/recovered term (whose watermarks are seeded so the comparison holds) — only a freshly
  /// ADOPTED term (in memory, write still in flight) reads false.
  #[inline]
  pub(crate) fn term_is_durable(&self) -> bool {
    self.durable.durable_term >= self.term
  }

  /// Flush any term-gated success ack once `self.term` is durable (called from `on_stable_wrote` after
  /// a `Wrote` completion may have advanced the durability watermark). A tag from a superseded term is
  /// dropped (the leader/term has since changed); a current-term tag is sent with its match clamped to
  /// `proven.min(ack_watermark())`. `proven` is the highest extent the leader's RPC(s) actually MATCHED
  /// on this follower (so the flush can never over-ack a durable-but-DIVERGENT tail the current leader
  /// never replicated), and `ack_watermark()` is the live durability cap (so it never
  /// reports a since-truncated index either).
  pub(crate) fn flush_term_gated_acks(&mut self) {
    if matches!(self.durable.term_gated_snapshot_ack, Some((_, t, _)) if t != self.term) {
      self.durable.term_gated_snapshot_ack = None;
    }
    if matches!(self.durable.term_gated_append_ack, Some((_, t, _)) if t != self.term) {
      self.durable.term_gated_append_ack = None;
    }
    if !self.term_is_durable() {
      return;
    }
    let (term, me) = (self.term, self.config.id());
    if let Some((to, _, proven)) = self.durable.term_gated_snapshot_ack.take() {
      let match_index = proven.min(self.ack_watermark());
      self.send(
        to,
        Message::SnapshotResponse(SnapshotResponse::new(
          term,
          me.cheap_clone(),
          false,
          match_index,
        )),
      );
    }
    if let Some((to, _, proven)) = self.durable.term_gated_append_ack.take() {
      let match_index = proven.min(self.ack_watermark());
      self.send(
        to,
        Message::AppendResponse(AppendResponse::new(
          term,
          me,
          false,
          Index::ZERO,
          Term::ZERO,
          match_index,
        )),
      );
    }
  }

  /// Emit a SUCCESS `AppendResponse` if `self.term` is durable; otherwise DEFER it (persist-before-
  /// respond) until [`flush_term_gated_acks`] releases it. `proven` is the extent this AppendEntries
  /// actually matched on the follower (`last_new` / the deferred-append match) — NOT pre-clamped to
  /// durability. This fn applies `proven.min(ack_watermark())` both on the immediate send and (via the
  /// stored `proven`) at flush, so a deferred ack can NEVER over-ack a durable-but-divergent tail the
  /// leader did not replicate. Deferral keeps the MAX proven extent for this `(leader, term)` —
  /// acks are cumulative, and the durability clamp is re-applied at flush.
  pub(crate) fn send_or_gate_append_ack(&mut self, to: I, proven: Index) {
    if self.term_is_durable() {
      let (term, me) = (self.term, self.config.id());
      let match_index = proven.min(self.ack_watermark());
      self.send(
        to,
        Message::AppendResponse(AppendResponse::new(
          term,
          me,
          false,
          Index::ZERO,
          Term::ZERO,
          match_index,
        )),
      );
    } else {
      let proven = match self.durable.term_gated_append_ack.as_ref() {
        Some((prev_to, prev_term, prev)) if *prev_to == to && *prev_term == self.term => {
          (*prev).max(proven)
        }
        _ => proven,
      };
      self.durable.term_gated_append_ack = Some((to, self.term, proven));
    }
  }

  /// Emit a SUCCESS `SnapshotResponse` if `self.term` is durable; otherwise DEFER it (persist-before-
  /// respond). `proven` (the snapshot boundary / committed match) is clamped to `ack_watermark()` on
  /// send and at flush — the snapshot analogue of [`send_or_gate_append_ack`].
  pub(crate) fn send_or_gate_snapshot_ack(&mut self, to: I, proven: Index) {
    if self.term_is_durable() {
      let (term, me) = (self.term, self.config.id());
      let match_index = proven.min(self.ack_watermark());
      self.send(
        to,
        Message::SnapshotResponse(SnapshotResponse::new(term, me, false, match_index)),
      );
    } else {
      let proven = match self.durable.term_gated_snapshot_ack.as_ref() {
        Some((prev_to, prev_term, prev)) if *prev_to == to && *prev_term == self.term => {
          (*prev).max(proven)
        }
        _ => proven,
      };
      self.durable.term_gated_snapshot_ack = Some((to, self.term, proven));
    }
  }

  /// Persist the adopted `(term, vote)` if it is not already durable — the lazy term step-down write.
  ///
  /// Replaces the eager pre-dispatch term persist. A higher-term step-down is made durable only
  /// AFTER the message-specific READ-ONLY validation has passed — so a fail-stop during that validation
  /// (a corrupt `RequestVote`/`AppendEntries`/`InstallSnapshot`) leaves NO premature term/vote write,
  /// i.e. the fail-stop is side-effect-free — and BEFORE any log entry or snapshot from that term
  /// reaches its store. The entry/snapshot handlers call this just before their own durable write
  /// (term-before-entries / term-before-snapshot); everything else is covered by the post-dispatch
  /// catch-all in `handle_message`. NOTE this submission ORDER is DEFENSE-IN-DEPTH, NOT the §5.1 safety
  /// mechanism: the term/vote (`StableStore`) and the log (`LogStore`) are INDEPENDENT durable stores with
  /// no cross-store barrier, so their relative fsync order is not guaranteed. §5.1 safety — a node holding
  /// an entry/snapshot from term T must never vote twice in T (→ two leaders) — is enforced instead by the
  /// persist-before-RESPOND gates (a vote grant / append ack is WITHHELD until `term_is_durable()` / the
  /// stable `Wrote`), which hold under ANY cross-store fsync skew (a grant/ack lost-because-not-durable was
  /// never observed by a peer, plus quorum overlap). A disk `StableStore`/`LogStore` implementer therefore
  /// needs NO ordering barrier between the two stores.
  ///
  /// Idempotent via the durable `HardState` read: a same-term message, a pre-vote (which never adopts a
  /// term), or a handler that already persisted the step-down (a vote grant) does NOT double-write.
  pub(crate) fn ensure_term_durable<S: StableStore<NodeId = I>>(&mut self, stable: &mut S) {
    if self.poison.poisoned {
      return;
    }
    let durable = stable.hard_state();
    // also force a write when the in-memory lease-support floor has outrun the durable one — a fresh
    // node that adopted its term via AppendEntries (no term change here) then bumped the floor on its first
    // enforcing Heartbeat would otherwise early-return and never persist the promise it is about to advertise.
    if durable.term() == self.term
      && durable.vote() == self.voted_for
      && durable.promised_lease_support() == self.durable.lease_support_floor
    {
      return;
    }
    let opid = self.mint_op_id();
    // The lease-support floor is stamped by the `submit_write` choke-point (`raise`), so this builder need
    // not carry it — and relying on the choke-point also UPGRADES a legacy `Unrecorded` durable record to
    // `Recorded` on this write (self-heal).
    let hs = durable
      .with_term(self.term)
      .with_vote(self.voted_for.cheap_clone())
      .with_commit(self.durable_commit());
    self.submit_write(stable, opid, hs);
    self.durable.committed_persisted = self.durable_commit();
  }
}
impl<I, F, R> Endpoint<I, F, R>
where
  I: NodeId,
  F: StateMachine,
  R: rand::Rng,
  F::Command: Data,
  F::Error: core::error::Error,
{
  /// Max storage completions processed per `handle_storage` call before yielding to the driver's run
  /// loop: an unbounded drain lets a degraded store's endless completion stream trap the driver inside
  /// one call, starving commands/timers/accept/peer-I/O. The budget is PER-QUEUE — the log and stable
  /// completion streams each get their own, so a flood of one cannot starve the other's durable
  /// progress (e.g. a log flood must not block the vote/leadership completions). The un-processed
  /// remainder stays queued (`poll()` is a stateful FIFO — nothing is dropped or reordered) and is
  /// re-driven next call. Mirrors the reactor `IO_BUDGET` /
  /// `APPLY_READ_MAX_BYTES` precedent. `pub(crate)` only so the bounded-drain test can assert the
  /// per-call poll count against it.
  pub(crate) const STORAGE_DRAIN_BUDGET: usize = 256;

  /// Drain storage completions (append-before-ack / persist-vote).
  ///
  /// Returns [`StorageProgress::MorePending`] when a completion is still queued at either store after
  /// this call — a per-queue budget cut its drain short, or the fixed tail submitted / compacted into a
  /// queue whose drain already exited — so the driver re-drives without sleeping (no single call
  /// monopolizes the run loop). The verdict is derived from the stores' actual queued state via
  /// [`LogStore::has_pending`] / [`StableStore::has_pending`], so every post-drain enqueue is caught
  /// uniformly.
  pub fn handle_storage<L, S>(
    &mut self,
    now: impl Into<Now>,
    log: &mut L,
    stable: &mut S,
  ) -> StorageProgress
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
    F::Snapshot: Data,
  {
    let now: Now = now.into();
    if self.poison.poisoned {
      return StorageProgress::Drained;
    }
    self.debug_assert_queues_drained();
    let mut log_budget = Self::STORAGE_DRAIN_BUDGET;
    while log_budget > 0 {
      match log.poll() {
        Some(Ok(LogDone::Appended(opid))) => {
          self.on_log_appended(now, log, stable, opid);
          log_budget -= 1;
        }
        Some(Ok(LogDone::Compacted(_))) => log_budget -= 1,
        Some(Err(_)) => {
          self.poison(PoisonReason::LogPoll);
          return StorageProgress::Drained;
        }
        None => break,
      }
    }
    let mut stable_budget = Self::STORAGE_DRAIN_BUDGET;
    while stable_budget > 0 {
      match stable.poll() {
        Some(Ok(StableDone::Wrote(opid))) => {
          self.on_stable_wrote(now, log, stable, opid);
          stable_budget -= 1;
        }
        Some(Ok(StableDone::SnapshotWritten(opid))) => {
          // Deferred compaction: fire only after the snapshot is durable.
          // This mirrors append-before-ack: the log is never compacted before the
          // snapshot backing it is safely on stable storage.
          if let Some((pid, up_to)) = self.snapshot.pending_compact
            && pid == opid
          {
            log.compact(up_to);
            self.snapshot.pending_compact = None;
          }
          // a DEFERRED follower install whose blob just became durable — run the destructive
          // install body NOW (SM restore, commit/applied advance, the log re-baseline, membership, ack).
          // Until this completion the blob was not durable, so nothing destructive had touched the
          // durable log; running it here makes the re-baseline strictly AFTER the blob is durable,
          // closing the orphan window by construction.
          if matches!(&self.snapshot.pending_install, Some((pid, ..)) if *pid == opid) {
            let (_pid, meta, snap, leader) = self
              .snapshot
              .pending_install
              .take()
              .expect("checked Some above");
            self.install_snapshot_now(log, meta, snap, leader);
            // Completion-time install can poison (log_term, SnapshotRestore, the log re-baseline) →
            // fail-stop the storage handler before any further drain / compaction / reclaim on a dead node.
            if self.poison.poisoned {
              return StorageProgress::Drained;
            }
          }
          stable_budget -= 1;
        }
        Some(Err(_)) => {
          self.poison(PoisonReason::StablePoll);
          return StorageProgress::Drained;
        }
        None => break,
      }
    }

    // Reconcile a deferred compaction whose `SnapshotWritten` completion was missed or coalesced
    // by the store: if the DURABLE snapshot already covers `up_to`, the blob IS safely persisted, so
    // the deferred compaction is safe even though we never observed the specific completion. Without
    // this, a single dropped completion would wedge `pending_compact`, and the `is_some()` guard in
    // `maybe_snapshot` would stop ALL future snapshots and compaction, growing the log unbounded.
    //
    // This is a NO-OP on the happy path: the poll-drain loop above clears `pending_compact` when the
    // completion arrives, so the `if let` does not match. It can only fire when a completion was
    // genuinely missed AND the durable snapshot already covers `up_to` — so it can never compact
    // ahead of a durable snapshot (safety preserved). It runs before `maybe_snapshot` so a node that
    // was wedged can snapshot again in this same call. (Keyed on `durable_snapshot()` — the
    // fsync'd slot — NOT `snapshot()`, the submit-visible slot, for uniformity with the install fallback.)
    if let Some((_pid, up_to)) = self.snapshot.pending_compact
      && matches!(stable.durable_snapshot(), Some(m) if m.last_index() >= up_to)
    {
      log.compact(up_to);
      self.snapshot.pending_compact = None;
    }
    // same missed/coalesced-completion fallback for a DEFERRED install — if the DURABLE snapshot
    // already covers the pending boundary, the blob is durable, so run the install now (else a single
    // dropped `SnapshotWritten` would wedge `pending_install` forever, the follower never installing).
    // Durable evidence ONLY (`durable_snapshot()`): firing on the visible (pre-fsync) `snapshot()` slot
    // would re-baseline the log ahead of a non-durable blob — the exact orphan this fix prevents.
    if let Some((_pid, meta, ..)) = &self.snapshot.pending_install {
      // IDENTITY-aware, not merely boundary `>=`: a same-boundary supersede can leave a SUPERSEDED
      // snapshot's blob durable while the replacement is still in flight; firing on that evidence would
      // install the replacement decoded snapshot on a blob that is NOT its own (ack/rebaseline on a
      // non-durable blob, and a crash recovers the superseded identity). Require the durable slot to be
      // THIS pending install's own snapshot.
      if matches!(stable.durable_snapshot(), Some(m) if m.identity_eq(meta)) {
        let (_pid, meta, snap, leader) = self
          .snapshot
          .pending_install
          .take()
          .expect("checked Some above");
        self.install_snapshot_now(log, meta, snap, leader);
        // The deferred (missed-completion) install can poison the same ways → fail-stop before reclaim.
        if self.poison.poisoned {
          return StorageProgress::Drained;
        }
      }
    }

    // Reclaim an abandoned chunked receive whose boundary the now-advanced recoverable prefix has passed
    // (a snapshot/AppendEntries race where the log caught up first), freeing its staging buffer rather than
    // pinning it until a future supersede or restart. A fatal term-read in the Log-Matching proof poisons
    // the node → fail-stop the storage handler immediately (mirrors the poisoned-entry guard at the top).
    if !self.reclaim_stale_snapshot_recv(log, stable) {
      return StorageProgress::Drained;
    }

    // Re-drive a deferred apply. A cold (`EntriesRead::Pending`) committed-range read leaves
    // `applied < commit` with NO `LogDone` to re-trigger apply through `on_log_appended`, so the store's
    // storage-ready wake (which the driver services by calling `handle_storage`) MUST re-attempt apply
    // here — otherwise an idle or single-node leader whose cold read just resolved would never re-pump
    // apply, a SILENT stall (`applied < commit`, no poison). Idempotent: a no-op when caught up, and a
    // still-cold or not-yet-viewable read simply defers again. (Replication has the periodic heartbeat
    // re-pump; apply did not, which is the gap this closes.)
    if !self.poison.poisoned && self.applied < self.commit {
      self.apply_committed(log);
      self.maybe_flush_deferred_reads(now, log, stable);
    }
    // apply_committed / the deferred-read flush can poison on a fatal log or state read → fail-stop before
    // the snapshot, auto-leave, and commit-persist tail runs on a dead node.
    if self.poison.poisoned {
      return StorageProgress::Drained;
    }

    // After all completions are drained, check whether a new snapshot is warranted.
    self.maybe_snapshot(log, stable);
    // maybe_snapshot can poison on snapshot capture or its log_term(applied) read → fail-stop before the
    // auto-leave / commit-persist tail mutates leader state on a dead node.
    if self.poison.poisoned {
      return StorageProgress::Drained;
    }

    // Auto-leave joint consensus: once the joint config is applied and no conf change is in
    // flight, the leader appends an empty leave-joint entry to transition back to a simple
    // config. Re-evaluated each call so a freshly-elected leader also finishes the job.
    // The condition stops once is_joint() is false — no infinite loop risk.
    if self.role.is_leader()
      && self.tracker.is_joint()
      && self.tracker.auto_leave()
      && self.pending_conf_index <= self.applied
    {
      let leave = crate::ConfChangeV2::leave_joint();
      if self.append_conf_change(now, log, stable, leave).is_none() {
        // Log index space exhausted: the leader cannot append the leave-joint entry and so cannot
        // exit joint consensus. This internal path has no user error channel, and a node whose log is
        // at u64::MAX is in a corrupt/terminal state — fail-stop.
        self.poison(PoisonReason::LogExhausted);
      }
    }
    // The auto-leave append can poison (log index space exhausted) → fail-stop before the commit-persist
    // and election-timer tail runs on a dead node.
    if self.poison.poisoned {
      return StorageProgress::Drained;
    }

    // Persist the advanced commit watermark so a restart recovers it (without this, restart
    // rebuilds an empty/snapshot-only state machine despite a durable committed log).
    // Batched here (runs every driver iteration) rather than on every advance; a crash
    // before this persist only loses a bounded commit suffix that is still in the durable LOG
    // and is re-advanced by the leader on recovery — Leader Completeness guarantees the leader
    // holds those committed entries, so no committed entry is lost, just a brief re-sync.
    // No `Pending` entry: a commit-watermark write owes no ack (like the step-down /
    // become_candidate writes); its completion drains harmlessly through `on_stable_wrote`.
    if !self.poison.poisoned && self.durable_commit() > self.durable.committed_persisted {
      let opid = self.mint_op_id();
      let hs = stable
        .hard_state()
        .with_term(self.term)
        .with_vote(self.voted_for.cheap_clone())
        .with_commit(self.durable_commit());
      self.submit_write(stable, opid, hs);
      self.durable.committed_persisted = self.durable_commit();
    }

    // Invariant restore: a learner promoted to voter by an applied conf-change above may have been
    // left without an election timer; ensure a voter non-leader can always campaign.
    self.reconcile_election_timer(now);

    // Storage-derived progress: MorePending iff a completion is queued for the next poll() at EITHER
    // store, checked AFTER every drain and the whole fixed tail (which can submit / compact into a
    // queue whose drain already exited). Exact by construction — catches every post-drain enqueue
    // uniformly (a budget cut leaves the remainder queued; a post-drain submit's completion; a
    // compact's `LogDone::Compacted`) with no per-site detector. poll() is a FIFO; the remainder
    // re-drives next call.
    if log.has_pending() || stable.has_pending() {
      StorageProgress::MorePending
    } else {
      StorageProgress::Drained
    }
  }

  pub(crate) fn on_log_appended<L: LogStore, S: StableStore<NodeId = I>>(
    &mut self,
    now: Now,
    log: &mut L,
    stable: &S,
    opid: OpId,
  ) {
    // Advance the persist-before-ack watermark for EVERY completed append, regardless of role,
    // term, or whether a `pending` action survived. `pending` is cleared on term changes, and a
    // same-term step-down routes a `LeaderAppend` completion to the `_` arm below — in both cases
    // the entry still became durable, so the watermark must rise here, once, unconditionally.
    // Otherwise the follower clamps a later duplicate/empty AppendEntries to a stale-low watermark
    // and under-acks its durable suffix, wedging replication.
    //
    // Taking the MAX of completed `upto`s is correct because `LogStore::submit_append` guarantees
    // prefix-ordered durability (NORMATIVE): an `Appended` for index `upto` means the entire prefix
    // through `upto` is durable, so this watermark is a true durable-PREFIX bound no matter what
    // order completions arrive in — a later append cannot complete ahead of an earlier index that is
    // still crash-losable.
    if let Some(upto) = self.durable.inflight_append_upto.remove(&opid) {
      self.durable.durable_index = self.durable.durable_index.max(upto);
    }
    match self.pending.remove(&opid) {
      Some(Pending::FollowerAck { to, match_index }) => {
        // `match_index` is the extent this append proved (its `last_new`). `send_or_gate_append_ack`
        // applies the persist-before-ack clamp `proven.min(ack_watermark())` itself — both when sending
        // now and at flush — so a freshly-installed snapshot's in-flight blob (ack_watermark caps at the
        // boundary) and a durable-but-divergent tail (proven caps to what the leader matched) are
        // both respected. Persist-before-RESPOND: if the just-adopted term is not yet durable this
        // DEFERS, released by `flush_term_gated_acks` once `on_stable_wrote` sees the term durable.
        self.send_or_gate_append_ack(to, match_index);
      }
      // Role-gate (defense-in-depth): only a current leader advances its own match index
      // and commit. `pending` is cleared on every term change, so a stale `LeaderAppend`
      // reaching a non-leader is already unreachable — this makes the safety local.
      Some(Pending::LeaderAppend { upto }) if self.role.is_leader() => {
        if let Some(p) = self.tracker.progress_mut(&self.config.id()) {
          p.maybe_update(upto);
        }
        self.maybe_advance_commit(now, log);
        self.apply_committed(log);
        // ReadIndex deferred-flush: if this commit advanced to the first current-term
        // entry, flush any reads that were deferred waiting for it.
        self.maybe_flush_deferred_reads(now, log, stable);
      }
      _ => {} // CastVote completes via stable; unknown/superseded opid → ignore
    }
  }

  pub(crate) fn on_stable_wrote<L: LogStore, S: StableStore<NodeId = I>>(
    &mut self,
    now: Now,
    log: &mut L,
    stable: &mut S,
    opid: OpId,
  ) {
    // a completion at or past the current term's write makes that term durable (stable completions
    // are ordered). Advance `durable_term`, which may now release a term-gated success ack below.
    if self.durable.last_submitted_term > self.durable.durable_term
      && opid >= self.durable.term_persist_opid
    {
      self.durable.durable_term = self.durable.last_submitted_term;
    }
    // the same advance for the lease-support floor — a completion at/past the floor write makes the
    // raised floor durable, releasing the persist-before-advertise gate so `on_heartbeat` may now advertise
    // this node's real lease support.
    if self.durable.last_submitted_lease_support > self.durable.durable_lease_support
      && opid >= self.durable.lease_support_persist_opid
    {
      self.durable.durable_lease_support = self.durable.last_submitted_lease_support;
    }
    match self.pending.remove(&opid) {
      Some(Pending::CastVote { to, term }) => {
        // Only emit the grant if the term hasn't changed and we still hold the vote for `to`.
        // If either condition is false the write was superseded by a term advance; drop silently.
        if term == self.term && self.voted_for.as_ref() == Some(&to) {
          debug_assert!(
            self.voted_for.as_ref() == Some(&to),
            "releasing a CastVote we no longer hold"
          );
          let me = self.config.id();
          self.send(
            to,
            Message::VoteResponse(crate::VoteResponse::new(term, me, false, false)),
          );
        }
      }
      // The candidate's self-vote is now DURABLE. If we are still a candidate at this term and a
      // quorum is already met (single-node now, or peer votes that arrived before this completion),
      // become leader — the self-vote backing the quorum is persisted, so a crash + restart can
      // never replay it as a vote for a different candidate in the same term.
      Some(Pending::Campaign { term })
        if term == self.term
          && self.role.is_candidate()
          && self.tracker.vote_result(&self.votes).is_won() =>
      {
        self.become_leader(now, log, stable);
      }
      _ => {}
    }
    // release any success ack that was deferred because `self.term` was not yet durable. The term
    // may have just become durable via this completion.
    self.flush_term_gated_acks();
  }
}
