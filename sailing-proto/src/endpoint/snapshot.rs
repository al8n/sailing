use super::*;

impl<I, F> Endpoint<I, F>
where
  I: NodeId,
  F: StateMachine,
{
  pub(crate) fn submit_snapshot<S: StableStore<NodeId = I>>(
    &self,
    stable: &mut S,
    id: crate::OpId,
    meta: crate::SnapshotMeta<I>,
    data: Bytes,
  ) {
    if self.poisoned {
      return;
    }
    stable.submit_snapshot(id, meta, data);
  }

  /// Expose `pending_compact` for testing.
  #[cfg(test)]
  pub(crate) fn pending_compact(&self) -> Option<(crate::OpId, Index)> {
    self.pending_compact
  }

  /// Re-send the persisted snapshot to a peer that is stuck in `Snapshot` state.
  ///
  /// A peer in `Snapshot` state is unconditionally paused, so `maybe_send_append`
  /// early-returns for it. It only leaves Snapshot state via `maybe_update(n >= pending)`,
  /// which requires the snapshot to have been DELIVERED (a `SnapshotResp`/`AppendResp`). If
  /// the single `InstallSnapshot` emitted by `maybe_send_append`'s compacted-hole branch is
  /// lost, the leader would never retry and the follower would wedge forever. `on_heartbeat_resp`
  /// calls this each heartbeat round for a peer still behind its pending snapshot index.
  ///
  /// Unlike the `maybe_send_append` branch this does NOT touch progress: the peer is already
  /// `Snapshot(pending)` with the correct pending index, and re-sending the same blob is
  /// idempotent for the follower's install (`on_install_snapshot` is staleness-guarded). If no
  /// snapshot is persisted yet (shouldn't happen once compaction ran) this is a no-op.
  pub(crate) fn resend_snapshot<S: StableStore<NodeId = I>>(&mut self, peer: I, stable: &S) {
    if let Some((meta, data)) = stable.snapshot() {
      let (term, me) = (self.term, self.config.id());
      self.send(
        peer,
        Message::InstallSnapshot(crate::InstallSnapshot::new(term, me, meta, data)),
      );
    }
  }
}
impl<I, F> Endpoint<I, F>
where
  I: NodeId,
  F: StateMachine,
  F::Command: crate::Data,
  F::Error: core::error::Error,
{
  /// Trigger a snapshot if `applied - first_index >= snapshot_threshold`.
  ///
  /// Durability rule: the snapshot is persisted first via `submit_snapshot`; the log is
  /// compacted only after `SnapshotWritten` is received in `handle_storage`. This mirrors
  /// append-before-ack and ensures a crash after compaction but before snapshot durability
  /// cannot lose data.
  pub(crate) fn maybe_snapshot<L, S>(&mut self, log: &L, stable: &mut S)
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
    F::Snapshot: crate::Data,
  {
    if self.pending_compact.is_some() || self.pending_install.is_some() {
      // A snapshot is already being persisted (our own compaction) OR a follower install is deferred
      // and about to re-baseline the log; don't start a leader-side snapshot over it.
      return;
    }
    if self.applied == Index::ZERO {
      // Nothing has been applied yet — nothing to snapshot.
      return;
    }
    if self.applied.get().saturating_sub(log.first_index().get())
      < self.config.snapshot_threshold() as u64
    {
      return;
    }
    let snap = match self.fsm.snapshot() {
      Ok(s) => s,
      Err(_) => {
        self.poison(PoisonReason::SnapshotCapture);
        return;
      }
    };
    use crate::Data as _;
    let mut data = std::vec::Vec::new();
    snap.encode(&mut data);
    let Some(last_term) = self.log_term(log, self.applied) else {
      return;
    };
    // Carry the self-describing LeaseGuard bound: this snapshot will subsume entries whose stamped
    // lease windows are about to leave the live log, so it records the node's current
    // `max_lease_window` (a conservative over-bound — the global max ≥ the compacted prefix's max).
    // A successor that compacts past — or installs — these entries then still covers any deposed
    // leader's lease on a now-unavailable entry.
    let mut meta = crate::SnapshotMeta::new(self.applied, last_term, self.conf_state())
      .with_max_lease_window(self.max_lease_window)
      .with_max_wall_plus_window(self.max_wall_plus_window)
      .with_max_unwalled_lease_window(self.max_unwalled_lease_window);
    // Carry the read mode EXPLICITLY only if a committed SetReadMode has applied (provenance). A
    // non-migrated node leaves it absent, so a restart from this snapshot falls back to the static config
    // — the presence bit then means "a migration was compacted", not merely "whatever mode was active".
    if self.read_mode_migrated {
      meta = meta.with_read_only(self.active_read_mode);
    }
    let opid = self.mint_op_id();
    self.submit_snapshot(stable, opid, meta, bytes::Bytes::from(data));
    // Defer compaction until SnapshotWritten fires.
    self.pending_compact = Some((opid, self.applied));
  }

  /// Receive an `InstallSnapshot` from the current leader (follower path). This only VALIDATES,
  /// persists the term, and submits the blob — it DEFERS the destructive install body (which touches the
  /// log) to `install_snapshot_now` once the blob is durable, so it needs no `LogStore`.
  pub(crate) fn on_install_snapshot<S>(
    &mut self,
    now: crate::Now,
    stable: &mut S,
    is: crate::InstallSnapshot<I>,
  ) where
    S: StableStore<NodeId = I>,
    F::Snapshot: crate::Data,
  {
    // Preamble: mirror on_append_entries — reset to Follower, track leader, re-arm election timer.
    self.role = Role::Follower;
    self.set_leader(Some(is.leader()));
    self.arm_election_timer(now);

    let meta = is.snapshot();

    // Reserved-sentinel guard: a snapshot whose boundary index is the reserved sentinel u64::MAX
    // is malformed — a correct leader never commits/snapshots the sentinel, and installing it
    // would set commit/applied to an index the half-open log ranges cannot represent (and re-baseline
    // `first_index` past the ceiling). Fail-stop on the malformed/version-skewed message before any
    // state mutation. (last_index == MAX - 1 is fine: a snapshot at the ceiling, no entry beyond it.)
    if meta.last_index().get() == u64::MAX {
      self.poison(PoisonReason::LogExhausted);
      return;
    }

    // Fold this snapshot's carried LeaseGuard bound into `max_lease_window` HERE — before EVERY early
    // return below (redundant short-circuit, duplicate-install guard) and before the destructive
    // install is deferred to `install_snapshot_now`. Otherwise a follower elected (a) while the blob
    // fsync is still pending, or (b) after acking a redundant/duplicate snapshot whose carried bound a
    // field-stripped local copy lost, would size its commit-wait from a stale max and miss a deposed
    // lease on an entry the snapshot subsumes (a stale read). Folding a not-yet-validated meta is safe
    // (the bound is just a number; a corrupt snapshot poisons below and an inert node never leads), and
    // monotonic so the later re-folds are harmless idempotent re-raises. (Durable cross-restart
    // survival of a stripped bound is the fresh-cluster / matched-schema contract; see WIRE.md.)
    self.max_lease_window = self.max_lease_window.max(meta.max_lease_window());
    self.max_wall_plus_window = self.max_wall_plus_window.max(meta.max_wall_plus_window());
    // The unwalled fallback bound — folded UNGATED, like `max_lease_window` above. An ENTRY-property
    // floor (every wall-absent lease entry folds itself on every node), so a snapshot's carried value
    // is already complete. A pre-FIELD snapshot (no `max_unwalled` field at all) is a mixed-version
    // case the Labeled handshake rejects.
    self.max_unwalled_lease_window = self
      .max_unwalled_lease_window
      .max(meta.max_unwalled_lease_window());

    // Staleness guard: short-circuit ONLY when the snapshot is ALREADY part of this follower's durable
    // RECOVERABLE prefix — `ack_watermark()` = max(durable log tip, durable snapshot boundary). Such a
    // snapshot is redundant; ack `ack_watermark()` (which already covers it) so the leader can leave
    // Snapshot state. `send_or_gate_snapshot_ack` applies the `commit.min(ack_watermark())` persist-before-
    // ack clamp itself (an async follower can have `commit > durable_index`; replying raw `commit` would
    // over-ack an unrecoverable tail). Persist-before-RESPOND: if `self.term` is not yet durable the
    // ack defers (this path runs no install, so the term write is the post-dispatch catch-all in
    // `handle_message`) and `flush_term_gated_acks` releases it.
    //
    // The snapshot is redundant — and short-circuits — ONLY when its boundary is already covered by BOTH
    // the committed prefix AND the recoverable prefix: `boundary <= min(commit, ack_watermark())`. Both
    // bounds are load-bearing:
    //  - a committed snapshot (`<= commit`) ABOVE `ack_watermark()` is NOT redundant (commit ran
    //    ahead of the durable log over an unflushed tail, no durable snapshot covers the gap). It must
    //    fall through to the DEFERRED install, which makes the boundary durable and RECORDS
    //    `durable_snapshot_index` (`install_snapshot_now`), raising `ack_watermark()` so the leader is not
    //    pinned in `ProgressState::Snapshot`; the completion-time stale re-check there drops the
    //    destructive body since `boundary <= commit`, so commit/applied/log never regress.
    //  - A snapshot ABOVE `commit` (`ack_watermark()` can exceed `commit` when a DIVERGENT uncommitted
    //    durable tail sits above it) is also NOT redundant: it extends/corrects the committed prefix and
    //    must install (re-baselining over the divergent tail). Only `boundary <= commit` is committed
    //    history, which is never divergent — so short-circuiting there cannot skip a needed correction.
    if meta.last_index() <= core::cmp::min(self.commit, self.ack_watermark()) {
      let leader = is.leader();
      self.send_or_gate_snapshot_ack(leader, self.commit);
      return;
    }

    // meta.last_index() > self.commit: a genuinely-newer snapshot. DEFER the destructive install.

    // Duplicate-install guard: while this peer is in Snapshot state the leader resends the same (or an
    // older) snapshot (`on_heartbeat_resp` resend / re-probe). If a deferred install at this boundary or
    // higher is already in flight, do NOT re-decode or mint a SECOND blob op (that would orphan the
    // first in-flight blob); the in-flight install will complete and ack. A strictly-NEWER snapshot
    // falls through and REPLACES it below (the stale opid's `SnapshotWritten` then finds no match — a
    // harmless no-op).
    if matches!(&self.pending_install, Some((_, pmeta, ..)) if pmeta.last_index() >= meta.last_index())
    {
      return;
    }

    // Step 0: validate the snapshot's membership BEFORE any durable op / state mutation.
    // `Tracker::from_conf_state` (in `install_snapshot_now`) copies the ConfState sets verbatim, so a
    // malformed `meta.conf()` — empty voters, learner/voter overlap, bad `learners_next`, non-joint
    // `auto_leave` — would install an impossible configuration (no quorum, vacuous votes). A correct
    // leader never sends one; treat it as fatal corruption and poison here, before any durable write.
    if !meta.conf().is_valid() {
      self.poison(PoisonReason::InvalidConfState);
      return;
    }

    // Step 1: decode the SM snapshot (fail-fast; leave NO partial state). The decoded snapshot is HELD
    // in `pending_install` and applied to the SM only once the blob is durable (`install_snapshot_now`)
    // — NOT here, so a crash before the blob lands leaves the SM and log untouched and recoverable.
    let snap = match <F::Snapshot as crate::Data>::decode_exact(is.data().clone()) {
      Ok(s) => s,
      Err(_) => {
        self.poison(PoisonReason::SnapshotDecode);
        return;
      }
    };

    // Persist the (possibly just-adopted) term now — AFTER the read-only validation (sentinel, conf,
    // decode) and BEFORE the snapshot blob: term-before-snapshot, the snapshot analogue of
    // term-before-entries (see `ensure_term_durable`). A fail-stop in any check above persisted no term
    // write. Idempotent (a same-term install skips). The term write is independently recoverable, so it
    // is correct to make durable now even though the destructive install body is deferred.
    self.ensure_term_durable(stable);

    // Submit the snapshot blob and DEFER the destructive install body (SM restore, commit/applied
    // advance, the `log.restore` re-baseline, membership install, ack) until the blob is durable:
    // the core owns the snapshot-vs-rebaseline ordering, exactly mirroring how `pending_compact` defers
    // `log.compact`. Until `SnapshotWritten` (or `durable_snapshot()` evidence) fires `install_snapshot_now`,
    // this follower stays in its OLD consistent state — so a crash in the window loses only the in-flight
    // blob and restart re-syncs from the UNCHANGED durable log (`reconcile_restart_log` sees the
    // pre-install shape, never the `OrphanedLog` poison). The decoded `snap` and the `leader` ride in
    // `pending_install`; the blob bytes are handed to the store (a refcount bump on `Bytes`).
    let opid = self.mint_op_id();
    self.submit_snapshot(stable, opid, meta.clone(), is.data().clone());
    let leader = is.leader();
    self.pending_install = Some((opid, meta.clone(), snap, leader));
  }

  /// Run the DEFERRED destructive snapshot-install body, once the blob is proven durable (the matching
  /// `StableDone::SnapshotWritten`, or `StableStore::durable_snapshot()` evidence if that completion was
  /// missed). Performing the `log.restore` re-baseline HERE — strictly AFTER the blob is durable — is
  /// what makes the orphan window {re-baseline durable, blob NOT durable} unreachable by construction
  /// the core, not the storage layer, owns the ordering. Called only from `handle_storage`, with
  /// the matching `pending_install` tuple already `take`n out (so a failure leaves no partial deferred
  /// install behind).
  pub(crate) fn install_snapshot_now<L: LogStore>(
    &mut self,
    log: &mut L,
    meta: crate::SnapshotMeta<I>,
    snap: F::Snapshot,
    leader: I,
  ) {
    if self.poisoned {
      return;
    }
    // this runs ONLY once the blob is durable (the matching `SnapshotWritten` or `durable_snapshot()`
    // evidence), so the snapshot boundary is now a durable RECOVERABLE prefix — a crash would
    // `reconcile_restart_log::Restore` to it. Record it BEFORE the stale-drop below, so `ack_watermark()`
    // reflects the boundary even when this install is dropped as stale: otherwise a follower whose
    // in-window appends advanced `commit` over a not-yet-flushed tail (so `durable_index < boundary`)
    // under-acks `durable_index` and pins the leader in `ProgressState::Snapshot` until the tail flushes.
    self.durable_snapshot_index = core::cmp::max(self.durable_snapshot_index, meta.last_index());
    // Raise the self-describing LeaseGuard bound over the snapshot's carried max — BEFORE the
    // stale-drop, like `durable_snapshot_index`, so even a dropped-stale install contributes its
    // bound (the sender held entries this follower may not have all of). Monotonic, so the redundant
    // raise from an already-covered install is harmless.
    self.max_lease_window = self.max_lease_window.max(meta.max_lease_window());
    self.max_wall_plus_window = self.max_wall_plus_window.max(meta.max_wall_plus_window());
    // The unwalled fallback bound — folded UNGATED, like `max_lease_window` above. An ENTRY-property
    // floor (every wall-absent lease entry folds itself on every node), so a snapshot's carried value
    // is already complete. A pre-FIELD snapshot (no `max_unwalled` field at all) is a mixed-version
    // case the Labeled handshake rejects.
    self.max_unwalled_lease_window = self
      .max_unwalled_lease_window
      .max(meta.max_unwalled_lease_window());
    // Completion-time staleness re-check (mirror the receipt-time guard): in-window AppendEntries can
    // have caught this follower up to/past the boundary while the blob was in flight. Installing now
    // would REGRESS committed/applied state, so DROP the deferred install (the durable blob is harmless;
    // a later `maybe_compact`/restart reconciles it). `pending_install` was already taken by the caller.
    if meta.last_index() <= self.commit {
      return;
    }

    // The SM, commit/applied, durable_index and the log re-baseline are all advanced TOGETHER here, with
    // the blob already durable — so `durable_commit()`/`ack_watermark()` need no install-window fence.
    // Step 2: restore the state machine. On failure, poison (deterministic: the durable blob re-enters
    // the install on restart and re-poisons, consistent with `restart_inner`'s SnapshotRestore).
    if self.fsm.restore(snap).is_err() {
      self.poison(PoisonReason::SnapshotRestore);
      return;
    }

    // The re-baseline below discards the log tail; drop any pending log-append acks that referred to
    // now-discarded entries, and abandon any in-flight leader-side compaction (its old `SnapshotWritten`
    // harmlessly finds None). Deferred to HERE, not receipt: the OLD log stayed live — and its in-flight
    // appends valid — throughout the deferral window. Vote-persistence pendings survive (log-independent).
    self
      .pending
      .retain(|_, p| matches!(p, Pending::CastVote { .. }));
    self.pending_compact = None;

    // Step 3: advance commit + applied to the snapshot boundary.
    self.commit = meta.last_index();
    self.applied = meta.last_index();
    // Adopt the active read mode at the snapshot boundary (a SetReadMode compacted into it). The
    // re-baseline discards the stale tail, so this is the boundary mode; subsequent AppendEntries replay
    // any post-snapshot SetReadMode via apply_committed (last-writer-wins by index). A legacy/pre-migration
    // snapshot carries None → keep the current mode (a defensive default — unreachable in a same-version
    // cluster, where the LABEL_VERSION-4 handshake fences a pre-migration peer).
    self.active_read_mode = meta.read_only().unwrap_or(self.active_read_mode);
    // Adopt the snapshot's read-mode provenance (Some ⇒ a migration was compacted at/before the boundary);
    // a None/legacy snapshot keeps the current provenance, consistent with keeping the current mode above.
    self.read_mode_migrated = meta.read_only().is_some() || self.read_mode_migrated;

    // Step 4: re-baseline the log on the now-durable snapshot. Discards the follower's stale/short log;
    // after this call first_index == last_index + 1 and term(last_index) == last_term, so the next
    // AppendEntries(prev=last_index) passes the consistency check. Because the blob is already durable,
    // a crash immediately after this leaves {durable snapshot present, log re-baselined} OR {durable
    // snapshot present, log not-yet-re-baselined} — both of which `reconcile_restart_log` recovers
    // (None/Compact/Restore), NEVER the OrphanedLog poison.
    log.restore(meta.last_index(), meta.last_term());
    // `restore` DISCARDS the prior tail, so the durable boundary IS exactly the snapshot's last index — a
    // hard RESET. `durable_index` and the re-baseline advance together, after the blob is durable, so the
    // boundary is recoverable (no stale-HIGH watermark, no orphan).
    self.durable_index = meta.last_index();
    // The log was replaced wholesale; any in-flight append records refer to discarded entries and must
    // not re-advance `durable_index` when their completions arrive.
    self.inflight_append_upto.clear();
    // Scrub any already-queued success `AppendResp`/`FollowerAck` for an index past the new boundary:
    // reporting it would over-ack an entry this node no longer stores (symmetric with the §5.3 scrub).
    self.scrub_acks_above(meta.last_index());

    // Tripwire: the install just advanced commit/applied to `meta.last_index` and the re-baseline took
    // effect, so the log read-view now reflects the snapshot boundary: first_index == last_index + 1.
    debug_assert_eq!(
      log.first_index().get(),
      meta.last_index().get() + 1,
      "restore must re-baseline first_index to last_index + 1 (read-view consistent with commit/applied)"
    );

    // Step 5: emit the application event.
    self
      .events
      .push_back(crate::Event::SnapshotInstalled(meta.clone()));

    // Step 6: install the membership from the snapshot's ConfState — jump directly to the committed
    // membership at the snapshot point; the Tracker is rebuilt from the snapshot's conf.
    self.tracker = crate::Tracker::from_conf_state(
      meta.conf(),
      meta.last_index(),
      self.config.max_inflight_msgs(),
      self.config.max_inflight_bytes(),
    );

    // Step 7: ack the boundary. `durable_index == boundary` now holds (set above), so the centralized
    // persist-before-ack clamp `proven.min(ack_watermark())` resolves to the boundary — and the boundary
    // is safe to ack, already quorum-committed (last_index <= leader.commit). The leader's
    // `maybe_update(last_index) >= pending_snapshot` transitions the peer out of Snapshot state.
    // Persist-before-RESPOND: `ensure_term_durable` (at receipt) submitted the term write; if it is not
    // yet durable this ack defers, released by `flush_term_gated_acks`. (Acking at completion — not
    // receipt — keeps the leader correctly in Snapshot state while the install is in flight; a follower
    // that crashes mid-window is re-driven by the leader's heartbeat-resend after it restarts.)
    self.send_or_gate_snapshot_ack(leader, meta.last_index());
  }

  /// Receive a `SnapshotResp` from a follower (leader path).
  pub(crate) fn on_snapshot_resp<L, S>(
    &mut self,
    now: crate::Now,
    log: &mut L,
    stable: &S,
    from: I,
    resp: crate::SnapshotResp<I>,
  ) where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    if !self.role.is_leader() {
      return;
    }
    let Some(pr) = self.tracker.progress_mut(&from) else {
      return;
    };
    if resp.reject() {
      // The snapshot was refused (shouldn't happen in the current protocol, but handle
      // defensively): revert to Probe so maybe_send_append re-probes and, if the follower
      // is still below first_index, re-sends the snapshot.
      pr.become_probe();
      // Drop the mutable borrow of `pr` before calling maybe_send_append (which re-borrows
      // self.tracker). The pattern mirrors on_append_resp's reject branch.
      self.maybe_send_append(now, from, log, stable);
    } else {
      // Boundary check (shared with `on_append_resp` via `match_within_log`): a successful snapshot
      // ack must not report a match above the leader's own log, for the same reason — an over-run
      // would corrupt `Progress` and could push the commit candidate off the log and poison the
      // leader. Ignore the malformed ack; the peer stays in Snapshot and is re-probed normally.
      if !Self::match_within_log(resp.match_index(), log) {
        return;
      }
      // Success: maybe_update drives the Snapshot → Probe transition regardless of its return
      // value ("advanced" hint). We resume unconditionally so a peer leaving Snapshot is never
      // left un-poked. Drop `pr` before the self.* calls (borrow discipline mirrors on_append_resp).
      pr.maybe_update(resp.match_index());
      // Re-borrow self for the resume sequence (pr is dropped above).
      self.maybe_advance_commit(now, log);
      self.apply_committed(log);
      self.maybe_flush_deferred_reads(now, log, stable);
      self.maybe_send_append(now, from, log, stable);
    }
  }
}
