use super::*;
use crate::{InstallSnapshot, ProgressState, SnapshotChunkRead, SnapshotMeta};

impl<I, F, R> Endpoint<I, F, R>
where
  I: NodeId,
  F: StateMachine,
  R: rand::Rng,
{
  pub(crate) fn submit_snapshot<S: StableStore<NodeId = I>>(
    &self,
    stable: &mut S,
    id: OpId,
    meta: SnapshotMeta<I>,
    data: Bytes,
  ) {
    if self.poison.poisoned {
      return;
    }
    stable.submit_snapshot(id, meta, data);
  }

  /// Expose `pending_compact` for testing.
  #[cfg(test)]
  pub(crate) fn pending_compact(&self) -> Option<(OpId, Index)> {
    self.snapshot.pending_compact
  }

  /// Re-send the persisted snapshot to a peer that is stuck in `Snapshot` state.
  ///
  /// A peer in `Snapshot` state is unconditionally paused, so `maybe_send_append`
  /// early-returns for it. It only leaves Snapshot state via `maybe_update(n >= pending)`,
  /// which requires the snapshot to have been DELIVERED (a `SnapshotResponse`/`AppendResponse`). If
  /// the single `InstallSnapshot` emitted by `maybe_send_append`'s compacted-hole branch is
  /// lost, the leader would never retry and the follower would wedge forever. `on_heartbeat_response`
  /// calls this each heartbeat round for a peer still behind its pending snapshot index.
  ///
  /// Unlike the `maybe_send_append` branch this does NOT touch progress: the peer is already
  /// `Snapshot(pending)` with the correct pending index, and re-sending the same blob is
  /// idempotent for the follower's install (`on_install_snapshot` is staleness-guarded). If no
  /// snapshot is persisted yet (shouldn't happen once compaction ran) this is a no-op.
  pub(crate) fn resend_snapshot<S: StableStore<NodeId = I>>(&mut self, peer: I, stable: &S) {
    // Resume from the peer's contiguous-staged cursor, not from 0: a lost middle chunk re-sends only
    // the tail. A peer with no cursor (shouldn't happen in Snapshot state) restarts from 0.
    let from = match self.tracker.progress(&peer).map(|p| p.state()) {
      Some(ProgressState::Snapshot { acked_through, .. }) => acked_through,
      _ => 0,
    };
    self.send_snapshot_chunk(peer, stable, from);
  }

  /// Send the snapshot chunk beginning at `from_offset`, bounded by `config.snapshot_chunk_bytes()`.
  /// The chunk always carries the blob's real `total_len`, so the receiver stages it; a snapshot that
  /// fits one chunk is simply `offset 0 .. total` with `is_last() == true`.
  ///
  /// `from_offset` is RECONCILED against the current snapshot boundary: if the local snapshot has
  /// advanced past the peer's in-flight boundary (the leader compacted a newer snapshot mid-transfer),
  /// the supplied cursor belongs to a superseded blob, so the peer is reset to the new boundary and the
  /// stream restarts at offset 0.
  pub(crate) fn send_snapshot_chunk<S: StableStore<NodeId = I>>(
    &mut self,
    peer: I,
    stable: &S,
    from_offset: u64,
  ) {
    // Read THIS chunk from the store (bounded — one chunk, never the whole blob resident). The read also
    // hands back the meta + total_len, so we reconcile the boundary and size the frame from it. A cold
    // store returns `Pending` (we defer and re-drive on storage-ready); an `Err` is a fatal store fault.
    let config_chunk = self
      .config
      .snapshot_chunk_bytes()
      .clamp(1, crate::config::MAX_SNAPSHOT_CHUNK_BYTES);
    let (meta, total, chunk_at_from) = match stable.snapshot_chunk(from_offset, config_chunk) {
      None => return,
      Some(Ok(read)) => read,
      Some(Err(_)) => {
        self.poison(PoisonReason::SnapshotRead);
        return;
      }
    };
    let boundary = meta.last_index();
    // A peer whose `pending` no longer matches the local snapshot is mid-transfer on a now-superseded
    // blob: its monotone `acked_through` would otherwise keep re-sending the NEW blob at the OLD offset
    // (the follower supersedes its staging to the new boundary and acks `acked_through = 0`, which the
    // monotone cursor ignores), wedging it. Reset to the new boundary and restart at 0.
    let from = if matches!(
      self.tracker.progress(&peer).map(|p| p.state()),
      Some(ProgressState::Snapshot { pending, .. }) if pending == boundary
    ) {
      from_offset
    } else {
      if let Some(p) = self.tracker.progress_mut(&peer) {
        p.become_snapshot(boundary);
      }
      0
    };
    let (term, me) = (self.term, self.config.id());
    // `start` clamps `from` to the blob. A STALE resume cursor — a reordered ack from a LARGER superseded
    // snapshot, SET onto a peer whose CURRENT blob is smaller — would otherwise encode `offset > total`,
    // which the receiver's range check rejects as a decode poison of a CORRECT follower. The transmitted
    // offset is therefore `start`, NOT the raw `from`: it always matches the sliced data's position and
    // never exceeds `total` (an over-cursor degenerates to the benign empty tail at `start == total`, which
    // the follower's true-watermark ack then self-corrects). `start` is independent of the chunk size, so
    // it is fixed before the frame budget below.
    let start = from.min(total);
    // Bound the chunk by the ENCODED FRAME size, not just the blob slice. The wire frame also carries the
    // `SnapshotMeta` (its `ConfState` voter set can be large yet legal) plus envelope overhead, and a frame
    // over `MAX_FRAME_BYTES` is REFUSED by the transport — the follower would wedge in catch-up. Size the
    // non-data overhead in closed form (no clone of the potentially huge meta) for THIS exact meta and the
    // chosen offset, reserving the data field's own tag + length-prefix at the largest possible chunk, then
    // size `data` to stay under the limit. A `0` or oversized config value is already rejected at
    // construction; the lower clamp keeps a non-empty chunk (no livelock).
    let overhead = crate::wire::install_snapshot_encoded_len(term, &me, &meta, start, total, 0);
    let data_field_max_self_cost =
      1 + buffa::encoding::varint_len(crate::config::MAX_SNAPSHOT_CHUNK_BYTES);
    // If the metadata alone (a pathologically large but VALID ConfState — `is_valid` checks membership
    // invariants, NOT encoded size) leaves no room for even one data byte under the frame limit, the
    // snapshot is UNSENDABLE on this transport. Do NOT enqueue an oversized frame (the stream transport
    // would close the connection / QUIC would drop it): return without sending. The peer stays in Snapshot
    // — this is a misconfiguration (a membership too large to snapshot), not a transient condition a
    // re-send resolves.
    let frame_budget =
      crate::wire::MAX_FRAME_BYTES.saturating_sub(overhead + data_field_max_self_cost) as u64;
    if frame_budget == 0 {
      return;
    }
    let chunk_len = config_chunk.min(frame_budget);
    // Reuse the chunk already read at `from_offset` when the reconciled offset is unchanged (the common
    // path — ONE store read). A boundary RESET (`start == 0`) or an over-cursor clamp (`start == total`)
    // moved the offset, so re-read at `start`.
    let chunk_read = if start == from_offset {
      chunk_at_from
    } else {
      match stable.snapshot_chunk(start, chunk_len) {
        None => return,
        Some(Ok((_, _, read))) => read,
        Some(Err(_)) => {
          self.poison(PoisonReason::SnapshotRead);
          return;
        }
      }
    };
    let data = match chunk_read {
      // Trim to the frame budget — normally a no-op (`chunk_len >= bytes.len()`); it fires only when a
      // huge meta shrank the budget below the bytes the store returned.
      SnapshotChunkRead::Ready(bytes) => {
        let n = (bytes.len() as u64).min(chunk_len) as usize;
        bytes.slice(0..n)
      }
      // Cold: the bytes aren't resident and the store began fetching. Defer — no progress mutation — and
      // re-drive on the storage-ready seam (the heartbeat `resend_snapshot` also re-drives).
      SnapshotChunkRead::Pending => return,
    };
    self.send(
      peer,
      Message::InstallSnapshot(InstallSnapshot::new_chunk(
        term, me, meta, data, start, total,
      )),
    );
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
    F::Snapshot: Data,
  {
    if self.snapshot.pending_compact.is_some() || self.snapshot.pending_install.is_some() {
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
    use Data as _;
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
    let mut meta = SnapshotMeta::new(self.applied, last_term, self.conf_state())
      .with_max_lease_window(self.lease_guard.max_lease_window)
      .with_max_wall_plus_window(self.lease_guard.max_wall_plus_window)
      .with_max_unwalled_lease_window(self.lease_guard.max_unwalled_lease_window);
    // Carry the read mode EXPLICITLY only if a committed SetReadMode has applied (provenance). A
    // non-migrated node leaves it absent, so a restart from this snapshot falls back to the static config
    // — the presence bit then means "a migration was compacted", not merely "whatever mode was active".
    if self.reads.read_mode_migrated {
      meta = meta.with_read_only(self.reads.active_read_mode);
    }
    let opid = self.mint_op_id();
    self.submit_snapshot(stable, opid, meta, bytes::Bytes::from(data));
    // Defer compaction until SnapshotWritten fires.
    self.snapshot.pending_compact = Some((opid, self.applied));
  }

  /// Reclaim an ABANDONED chunked receive: if the recoverable prefix (`min(commit, ack_watermark())`) has
  /// caught up to or past an in-progress transfer's boundary, the partial is now redundant — free
  /// `snapshot_recv` AND the store's `SnapshotStaging` buffer (a full `total_len` allocation) rather than
  /// pinning it until a future supersede or restart. A no-op when no transfer is in progress or it is still
  /// ahead of the recoverable prefix.
  pub(crate) fn reclaim_stale_snapshot_recv<S: StableStore<NodeId = I>>(&mut self, stable: &mut S) {
    if let Some(r) = &self.snapshot.snapshot_recv
      && r.meta.last_index() <= core::cmp::min(self.commit, self.ack_watermark())
    {
      self.snapshot.snapshot_recv = None;
      stable.discard_snapshot_staging();
    }
  }

  /// Receive an `InstallSnapshot` from the current leader (follower path). This only VALIDATES,
  /// persists the term, and submits the blob — it DEFERS the destructive install body (which touches the
  /// log) to `install_snapshot_now` once the blob is durable, so it needs no `LogStore`.
  pub(crate) fn on_install_snapshot<S>(&mut self, now: Now, stable: &mut S, is: InstallSnapshot<I>)
  where
    S: StableStore<NodeId = I>,
    F::Snapshot: Data,
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

    // Reclaim an abandoned in-progress receive whose boundary the recoverable prefix has already passed:
    // a delayed chunk for it would otherwise short-circuit at the staleness guard below WITHOUT freeing the
    // staging buffer (the leak that pins a full `total_len` allocation).
    self.reclaim_stale_snapshot_recv(stable);

    // Fold this snapshot's carried LeaseGuard bound into `max_lease_window` HERE — before EVERY early
    // return below (redundant short-circuit, duplicate-install guard) and before the destructive
    // install is deferred to `install_snapshot_now`. Otherwise a follower elected (a) while the blob
    // fsync is still pending, or (b) after acking a redundant/duplicate snapshot whose carried bound a
    // field-stripped local copy lost, would size its commit-wait from a stale max and miss a deposed
    // lease on an entry the snapshot subsumes (a stale read). Folding a not-yet-validated meta is safe
    // (the bound is just a number; a corrupt snapshot poisons below and an inert node never leads), and
    // monotonic so the later re-folds are harmless idempotent re-raises. (Durable cross-restart
    // survival of a stripped bound is the fresh-cluster / matched-schema contract; see WIRE.md.)
    self.lease_guard.max_lease_window = self
      .lease_guard
      .max_lease_window
      .max(meta.max_lease_window());
    self.lease_guard.max_wall_plus_window = self
      .lease_guard
      .max_wall_plus_window
      .max(meta.max_wall_plus_window());
    // The unwalled fallback bound — folded UNGATED, like `max_lease_window` above. An ENTRY-property
    // floor (every wall-absent lease entry folds itself on every node), so a snapshot's carried value
    // is already complete. A pre-FIELD snapshot (no `max_unwalled` field at all) is a mixed-version
    // case the Labeled handshake rejects.
    self.lease_guard.max_unwalled_lease_window = self
      .lease_guard
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

    // meta.last_index() > self.commit: a genuinely-newer snapshot.

    // Duplicate-install guard: a deferred install for the SAME snapshot identity (or one at a
    // strictly-NEWER boundary) is completing — do NOT re-stage or re-decode (that would orphan the
    // in-flight blob, or stage a now-stale older snapshot). A DIFFERENT snapshot at the same-or-lower
    // boundary (a different term/conf — a re-snapshot during the fsync window) falls through and
    // SUPERSEDES the partial below; the stale opid's `SnapshotWritten` then finds no match.
    if matches!(
      &self.snapshot.pending_install,
      Some((_, pmeta, ..)) if pmeta.last_index() > meta.last_index() || pmeta.identity_eq(meta)
    ) {
      return;
    }

    let total_len = is.total_len();
    if total_len == 0 {
      // LEGACY single-shot: `data` IS the whole blob — decode + submit directly (the pre-chunking path,
      // byte-identical, no staging; also reached by a 0-byte snapshot from the chunked sender). A genuine
      // pre-chunking peer is otherwise fenced by the handshake.
      //
      // A complete single-shot SUPERSEDES any in-progress chunked receive — apply the SAME leader-aware
      // cleanup as the chunked branch: drop a same-leader LOWER-boundary reorder, else discard the
      // abandoned partial (`snapshot_recv` + store staging) before installing. Without it, a stale
      // `snapshot_recv` would pin its `total_len` staging buffer AND skew the vote-freshness floor.
      if matches!(
        &self.snapshot.snapshot_recv,
        Some(r) if r.sender_term == is.term() && r.meta.last_index() > meta.last_index()
      ) {
        return;
      }
      // Discard any prior store staging UNCONDITIONALLY (not gated on `snapshot_recv.is_some()`): a store
      // that persisted staging across a restart holds it WITHOUT a `snapshot_recv` to track, so a gate would
      // miss the orphan and this install would race a stale higher staging key.
      self.snapshot.snapshot_recv = None;
      stable.discard_snapshot_staging();
      if !meta.conf().is_valid() {
        self.poison(PoisonReason::InvalidConfState);
        return;
      }
      let snap = match <F::Snapshot as Data>::decode_exact(is.data().clone()) {
        Ok(s) => s,
        Err(_) => {
          self.poison(PoisonReason::SnapshotDecode);
          return;
        }
      };
      self.ensure_term_durable(stable);
      let opid = self.mint_op_id();
      self.submit_snapshot(stable, opid, meta.clone(), is.data().clone());
      let leader = is.leader();
      self.snapshot.pending_install = Some((opid, meta.clone(), snap, leader));
      return;
    }

    // CHUNKED transfer (total_len != 0): stage this chunk into the store; DEFER decode + the destructive
    // install until the WHOLE blob is contiguous-staged. The proto holds NO bytes — `snapshot_recv` is
    // coordination only.
    let boundary = meta.last_index();
    // Identify the in-progress transfer by its FULL identity — (sender_term, last_index, last_term, conf,
    // total_len) — NOT just the boundary index. The SENDER TERM is load-bearing: a NEWER leader sending a
    // snapshot with the SAME (last_index, last_term, conf) and length is a DISTINCT capture, not a
    // continuation — appending its chunks into the old leader's staging would MIX bytes from two
    // independently-captured snapshots (the StateMachine contract does not promise byte-identical encodings
    // across leaders for the same applied state). A mismatch routes to the supersede/replace path below.
    // (The LeaseGuard / read-mode bounds are folded ungated above and may legitimately differ between
    // same-boundary snapshots, so they are NOT part of the identity.)
    let continues = matches!(
      &self.snapshot.snapshot_recv,
      Some(r) if r.sender_term == is.term() && r.meta.identity_eq(meta) && r.total_len == total_len
    );
    if !continues {
      // A chunk that does NOT continue the current partial. Drop ONLY a stale reorder — a delayed
      // LOWER-boundary chunk from the SAME leader term (a now-superseded transfer). A chunk from a NEWER
      // leader term REPLACES the partial at ANY boundary: the new leader is authoritative and may
      // legitimately send a LOWER snapshot (`snapshot(K)+log` for a follower below its first index), which
      // boundary ordering ALONE would wrongly drop — wedging the follower. Otherwise BEGIN a new transfer.
      if matches!(
        &self.snapshot.snapshot_recv,
        Some(r) if r.sender_term == is.term() && r.meta.last_index() > boundary
      ) {
        return;
      }
      // Validate the new snapshot's membership BEFORE any durable op (once per transfer identity).
      if !meta.conf().is_valid() {
        self.poison(PoisonReason::InvalidConfState);
        return;
      }
      // Free any prior store staging so this fresh transfer stages from scratch — the store keys staging by
      // boundary/identity and would otherwise drop a lower chunk against a higher stale buffer. Done
      // UNCONDITIONALLY (not gated on `snapshot_recv.is_some()`): a store that persisted staging across a
      // restart has orphaned it WITHOUT a `snapshot_recv` to track, so a gate would miss it.
      stable.discard_snapshot_staging();
      self.snapshot.snapshot_recv = Some(SnapshotRecv {
        meta: meta.clone(),
        total_len,
        contiguous_staged: 0,
        sender_term: is.term(),
      });
    }

    // Validate the chunk byte-range BEFORE staging — a chunk past `total_len` would be silently CLAMPED by
    // the staging accumulator, completing the buffer from a malformed stream and decoding a TRUNCATED
    // prefix instead of fail-stopping. (`offset == total_len` with empty data is the benign stale-cursor
    // self-correction — its `end == total_len` passes.) Checked arithmetic: a `u64` overflow is out of range.
    match is.offset().checked_add(is.data().len() as u64) {
      Some(end) if end <= total_len => {}
      _ => {
        self.poison(PoisonReason::SnapshotDecode);
        return;
      }
    }

    // Stage this chunk. A store staging-capacity error poisons (CFT resource exhaustion → failover).
    let staged = match stable.accept_snapshot_chunk(meta, total_len, is.offset(), is.data()) {
      Ok(s) => s,
      Err(_) => {
        self.poison(PoisonReason::StablePoll);
        return;
      }
    };
    if let Some(r) = &mut self.snapshot.snapshot_recv {
      r.contiguous_staged = staged;
    }

    if staged < total_len {
      // Mid-transfer: a PROGRESS ack carrying the contiguous-staged offset — drives the leader's next
      // chunk but does NOT advance `match_index` (the peer stays in Snapshot state).
      let leader = is.leader();
      self.send_snapshot_progress_ack(leader, staged);
      return;
    }

    // Whole blob staged: CONSUME it (clearing the store's staging buffer), decode once (fail-fast;
    // leave NO partial state), persist the term (term-before-blob), submit, and DEFER the UNCHANGED
    // destructive install until the blob is durable.
    let Some(blob) = stable.take_staged_snapshot(meta) else {
      self.poison(PoisonReason::SnapshotDecode);
      return;
    };
    let snap = match <F::Snapshot as Data>::decode_exact(blob.clone()) {
      Ok(s) => s,
      Err(_) => {
        self.poison(PoisonReason::SnapshotDecode);
        return;
      }
    };
    self.ensure_term_durable(stable);
    let opid = self.mint_op_id();
    self.submit_snapshot(stable, opid, meta.clone(), blob);
    let leader = is.leader();
    self.snapshot.pending_install = Some((opid, meta.clone(), snap, leader));
    self.snapshot.snapshot_recv = None;
  }

  /// Send a mid-transfer PROGRESS ack for a chunked snapshot: carries the contiguous-staged byte offset
  /// (`acked_through`) so the leader sends the next chunk, with `match_index = min(commit, ack_watermark)`
  /// — the persist-before-ack-safe RECOVERABLE watermark, which past the staleness guard is strictly
  /// below the snapshot boundary, so the leader's `maybe_update` does NOT lift the peer out of Snapshot
  /// state (counting a phantom replica before the blob is durably installed). UNGATED (unlike the final
  /// install ack): it makes no NEW durable commitment — the watermark is already durable and
  /// `acked_through` is a transfer-progress hint — so a crash that loses it merely restarts the transfer.
  fn send_snapshot_progress_ack(&mut self, to: I, acked_through: u64) {
    let (term, me) = (self.term, self.config.id());
    let match_index = self.commit.min(self.ack_watermark());
    self.send(
      to,
      Message::SnapshotResponse(
        crate::SnapshotResponse::new(term, me, false, match_index)
          .with_acked_through(acked_through),
      ),
    );
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
    meta: SnapshotMeta<I>,
    snap: F::Snapshot,
    leader: I,
  ) {
    if self.poison.poisoned {
      return;
    }
    // this runs ONLY once the blob is durable (the matching `SnapshotWritten` or `durable_snapshot()`
    // evidence), so the snapshot boundary is now a durable RECOVERABLE prefix — a crash would
    // `reconcile_restart_log::Restore` to it. Record it BEFORE the stale-drop below, so `ack_watermark()`
    // reflects the boundary even when this install is dropped as stale: otherwise a follower whose
    // in-window appends advanced `commit` over a not-yet-flushed tail (so `durable_index < boundary`)
    // under-acks `durable_index` and pins the leader in `ProgressState::Snapshot` until the tail flushes.
    self.durable.durable_snapshot_index =
      core::cmp::max(self.durable.durable_snapshot_index, meta.last_index());
    // Raise the self-describing LeaseGuard bound over the snapshot's carried max — BEFORE the
    // stale-drop, like `durable_snapshot_index`, so even a dropped-stale install contributes its
    // bound (the sender held entries this follower may not have all of). Monotonic, so the redundant
    // raise from an already-covered install is harmless.
    self.lease_guard.max_lease_window = self
      .lease_guard
      .max_lease_window
      .max(meta.max_lease_window());
    self.lease_guard.max_wall_plus_window = self
      .lease_guard
      .max_wall_plus_window
      .max(meta.max_wall_plus_window());
    // The unwalled fallback bound — folded UNGATED, like `max_lease_window` above. An ENTRY-property
    // floor (every wall-absent lease entry folds itself on every node), so a snapshot's carried value
    // is already complete. A pre-FIELD snapshot (no `max_unwalled` field at all) is a mixed-version
    // case the Labeled handshake rejects.
    self.lease_guard.max_unwalled_lease_window = self
      .lease_guard
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
    self.snapshot.pending_compact = None;

    // Step 3: advance commit + applied to the snapshot boundary.
    self.commit = meta.last_index();
    self.applied = meta.last_index();
    // Adopt the active read mode at the snapshot boundary (a SetReadMode compacted into it). The
    // re-baseline discards the stale tail, so this is the boundary mode; subsequent AppendEntries replay
    // any post-snapshot SetReadMode via apply_committed (last-writer-wins by index). A legacy/pre-migration
    // snapshot carries None → keep the current mode (a defensive default — unreachable in a same-version
    // cluster, where the LABEL_VERSION-4 handshake fences a pre-migration peer).
    self.reads.active_read_mode = meta.read_only().unwrap_or(self.reads.active_read_mode);
    // Adopt the snapshot's read-mode provenance (Some ⇒ a migration was compacted at/before the boundary);
    // a None/legacy snapshot keeps the current provenance, consistent with keeping the current mode above.
    self.reads.read_mode_migrated = meta.read_only().is_some() || self.reads.read_mode_migrated;

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
    self.durable.durable_index = meta.last_index();
    // The log was replaced wholesale; any in-flight append records refer to discarded entries and must
    // not re-advance `durable_index` when their completions arrive.
    self.durable.inflight_append_upto.clear();
    // Scrub any already-queued success `AppendResponse`/`FollowerAck` for an index past the new boundary:
    // reporting it would over-ack an entry this node no longer stores (symmetric with the §5.3 scrub).
    self.scrub_acks_above(meta.last_index());

    // Fail-stop tripwire: the install just advanced commit/applied to `meta.last_index`, so the log must
    // now be re-baselined EXACTLY to that boundary — the full `restore` postcondition (first_index, NO
    // stale suffix, boundary term), checked by `restore_rebaselined` and shared with the restart path. A
    // `LogStore` that violates it leaves a torn read-view (a retained suffix could later campaign and
    // commit a discarded entry), so poison rather than serve off it (a release check, not a debug assert).
    if !super::restore_rebaselined(log, meta.last_index(), meta.last_term()) {
      self.poison(PoisonReason::SnapshotRebaseline);
      return;
    }

    // Step 5: emit the application event.
    self
      .outputs
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

  /// Receive a `SnapshotResponse` from a follower (leader path).
  pub(crate) fn on_snapshot_response<L, S>(
    &mut self,
    now: Now,
    log: &mut L,
    stable: &S,
    from: I,
    response: crate::SnapshotResponse<I>,
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
    if response.reject() {
      // The snapshot was refused (shouldn't happen in the current protocol, but handle
      // defensively): revert to Probe so maybe_send_append re-probes and, if the follower
      // is still below first_index, re-sends the snapshot.
      pr.become_probe();
      // Drop the mutable borrow of `pr` before calling maybe_send_append (which re-borrows
      // self.tracker). The pattern mirrors on_append_response's reject branch.
      self.maybe_send_append(now, from, log, stable);
    } else {
      // Boundary check (shared with `on_append_response` via `match_within_log`): a successful snapshot
      // ack must not report a match above the leader's own log, for the same reason — an over-run
      // would corrupt `Progress` and could push the commit candidate off the log and poison the
      // leader. Ignore the malformed ack; the peer stays in Snapshot and is re-probed normally.
      if !Self::match_within_log(response.match_index(), log) {
        return;
      }
      // Success: maybe_update drives the Snapshot → Probe transition regardless of its return
      // value ("advanced" hint). We resume unconditionally so a peer leaving Snapshot is never
      // left un-poked. Drop `pr` before the self.* calls (borrow discipline mirrors on_append_response).
      pr.maybe_update(response.match_index());
      // Advance the resume cursor from the follower's contiguous watermark, then — if the peer is STILL
      // mid-transfer (a progress ack did not lift it out of Snapshot via maybe_update above) — send the
      // next chunk. A single-chunk snapshot's FINAL ack lifts the peer out of Snapshot, so this no-ops.
      if let Some(pr) = self.tracker.progress_mut(&from) {
        pr.snapshot_acked(response.acked_through());
      }
      if let Some(ProgressState::Snapshot { acked_through, .. }) =
        self.tracker.progress(&from).map(|p| p.state())
      {
        self.send_snapshot_chunk(from.cheap_clone(), stable, acked_through);
        self.snapshot.snapshot_resend_after.insert(
          from.cheap_clone(),
          now.mono() + self.config.election_timeout(),
        );
      }
      // Re-borrow self for the resume sequence (pr is dropped above).
      self.maybe_advance_commit(now, log);
      self.apply_committed(log);
      self.maybe_flush_deferred_reads(now, log, stable);
      self.maybe_send_append(now, from, log, stable);
    }
  }
}
