use super::*;
use crate::{InstallSnapshot, ProgressState, SnapshotChunkRead, SnapshotMeta};

/// The outcome of a snapshot-chunk send attempt. A FATAL store error, a benign TRANSIENT deferral, and
/// a PERMANENT unsendable frame are DISTINCT outcomes — all three emit no `InstallSnapshot`, but
/// conflating the poison with a defer let a poisoned node keep mutating state (advance commit, compact)
/// in the same dispatch, and conflating the permanent wedge with a transient defer hides a config that
/// can never replicate. Callers match exhaustively.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use = "a ChunkSend::Poisoned must fail-stop the caller; Sent/Deferred/Unsendable drive resend-pacing"]
pub(crate) enum ChunkSend {
  /// An `InstallSnapshot` was emitted — arm the resend-pacing deadline.
  Sent,
  /// No chunk went out for a TRANSIENT reason (cold `Pending`, nothing persisted yet) — clear the
  /// deadline so the next heartbeat retries immediately; a retry is expected to make progress.
  Deferred,
  /// No chunk went out because the snapshot METADATA alone leaves no room for a data byte under the
  /// frame limit — the config is too large to snapshot on this transport. Retrying cannot help (the
  /// meta never shrinks), so the peer stays wedged in `Snapshot`; a diagnostic counter is bumped so
  /// the wedge is visible. Native `propose_conf_change` refuses such a membership up front
  /// (`MembershipTooLargeToSnapshot`), so this only guards a config that predates the gate.
  Unsendable,
  /// A FATAL store error poisoned the node — the caller MUST bail (no further work this dispatch).
  Poisoned,
}

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

  /// Whether `conf` still names this node in ANY membership role — voter in either joint half,
  /// learner, or incoming learner. Its complement is REMOVED-SELF, and it is deliberately the same
  /// predicate the drivers' lifecycle mirror applies to a `ConfChanged`: a removal carried by a
  /// snapshot must be indistinguishable downstream from one carried by a log entry. During a joint
  /// phase the OUTGOING half still counts, so a leaving member reads as named until the final,
  /// fully-departed configuration.
  pub(crate) fn conf_names_self(&self, conf: &crate::ConfState<I>) -> bool {
    self.conf_names(conf, &self.config.id())
  }

  /// Whether `conf` names `id` in ANY membership role — the general form of
  /// [`conf_names_self`](Self::conf_names_self), used by the courtesy path to ask the same
  /// question about the peer it is about to serve.
  pub(crate) fn conf_names(&self, conf: &crate::ConfState<I>, id: &I) -> bool {
    conf.voters().contains(id)
      || conf.voters_outgoing().contains(id)
      || conf.learners().contains(id)
      || conf.learners_next().contains(id)
  }

  /// Every id `conf` names in ANY role, across BOTH joint halves — the full membership dimension.
  /// Deliberately not the voter set: a voter demoted to learner is still a member, and reading a
  /// demotion as a removal would fire a removal event (and, downstream, mint a courtesy debt) for
  /// a replica that is still very much part of the group.
  pub(crate) fn conf_members(conf: &crate::ConfState<I>) -> std::collections::BTreeSet<I> {
    let mut out = std::collections::BTreeSet::new();
    for set in [
      conf.voters(),
      conf.voters_outgoing(),
      conf.learners(),
      conf.learners_next(),
    ] {
      out.extend(set.iter().map(CheapClone::cheap_clone));
    }
    out
  }

  /// THE MEMBERSHIP TRANSITION an admitted install performs: the ids this replica's own APPLIED
  /// configuration names that the installed one does not.
  ///
  /// This is the difference between ABSENCE and REMOVAL, and the whole install-time membership
  /// story rests on it. A snapshot whose ConfState simply does not mention a replica says nothing
  /// on its own — it may be HISTORY, a capture from before that replica was ever admitted, which a
  /// fresh joiner or a fork-born observer receives as ordinary catch-up. Only a peer the receiver
  /// itself counted as a member a moment ago, and the installed configuration does not, has
  /// actually been removed. The prior side is the receiver's own applied state, which is the
  /// apply-time membership doctrine's authority, and an admitted install only ever advances, so
  /// that prior state always sits at or below the boundary.
  pub(crate) fn install_removed_members(
    &self,
    installed: &crate::ConfState<I>,
  ) -> std::collections::BTreeSet<I> {
    let installed = Self::conf_members(installed);
    Self::conf_members(&self.tracker.conf_state())
      .into_iter()
      .filter(|id| !installed.contains(id))
      .collect()
  }

  /// Expose `pending_compact` for testing.
  #[cfg(test)]
  pub(crate) fn pending_compact(&self) -> Option<(OpId, Index)> {
    self
      .snapshot
      .pending_compact
      .as_ref()
      .map(|(pid, m)| (*pid, m.last_index()))
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
  pub(crate) fn resend_snapshot<S: StableStore<NodeId = I>>(
    &mut self,
    peer: I,
    stable: &S,
  ) -> ChunkSend {
    // Resume from the peer's contiguous-staged cursor, not from 0: a lost middle chunk re-sends only
    // the tail. A peer with no cursor (shouldn't happen in Snapshot state) restarts from 0. Propagates
    // the `ChunkSend` outcome so the caller arms resend-pacing on `Sent`, clears it on `Deferred`, and
    // bails on `Poisoned`.
    let from = match self.tracker.progress(&peer).map(|p| p.state()) {
      Some(ProgressState::Snapshot { acked_through, .. }) => acked_through,
      _ => 0,
    };
    self.send_snapshot_chunk(peer, stable, from)
  }

  /// THE COURTESY SNAPSHOT (#95, the compacted removed peer). A leader that hears from a peer it
  /// OWES a courtesy debt offers that peer ONE whole-blob `InstallSnapshot` carrying the
  /// post-removal ConfState. The peer installs it, applies the excluding membership, surfaces its
  /// own removal and disarms — self-removal by APPLYING committed state, never by a bare
  /// assertion, so the safety envelope is untouched: this path only ever ships a snapshot the
  /// leader itself holds as durable committed state.
  ///
  /// AUTHORIZATION IS THE DEBT, NOT THE SENDER. The only peers this can ever offer to are those in
  /// [`courtesy_owed`](Endpoint::courtesy_owed), which is minted solely at this replica's own
  /// apply-time committed removals. A sender that is merely unknown to the tracker — a
  /// never-member, a peer of another group, a fresh identity — is owed nothing and gets nothing:
  /// no offer, no state change, no reply. Contact cannot mint a debt, so no amount of traffic (or
  /// of distinct identities) grows this leader's work or its egress. That also takes sender-identity
  /// binding at the transport OUT of this path's blast radius: it is a real hardening question, but
  /// the courtesy path no longer trusts sender identity for authorization at all — an attacker who
  /// forges an identity gains exactly what that identity is owed, which for anyone this group never
  /// removed is nothing.
  ///
  /// It is the complement of the farewell retry, not a duplicate of it. The retry re-delivers the
  /// removal as an append and fails exactly when the peer's suffix has been COMPACTED below
  /// `first_index` — and that class implies a durable snapshot exists at or past the compaction
  /// point, which is precisely what this sends. Both maps are populated at the same fold, so the
  /// composition is a lookup: the retry OWNS the peer while its budget lasts (checked below) and
  /// this takes over afterwards.
  ///
  /// DEFERS — no send, no offer spent, no cooldown started, debt left armed — when any leg says so:
  /// - the peer is still inside `pending_farewells` (the cheaper cure owns it);
  /// - the debt's cooldown has not elapsed;
  /// - no durable snapshot exists yet (nothing has been compacted, so the retry's append arm can
  ///   still reach the peer and there is no gap to close);
  /// - the durable snapshot PREDATES the removal, by index or by still naming the peer in its
  ///   ConfState (see the eligibility gate below);
  /// - the store defers the read (cold blob) — the peer's next contact re-drives it;
  /// - the blob does not fit ONE frame. v1 is whole-blob only: a chunked courtesy would need ack
  ///   routing for a peer with no `Progress` to route through.
  ///
  /// Two residuals are left to the embedder's catalog reap, by the golden architecture's own
  /// assignment: an oversized blob, and a leader that never captures a snapshot past the removal
  /// at all. Both leave the peer ignorant-but-alive, the safe failure direction.
  pub(crate) fn maybe_send_courtesy_snapshot<S: StableStore<NodeId = I>>(
    &mut self,
    now: Now,
    peer: &I,
    stable: &S,
  ) {
    // THE AUTHORIZATION GATE. Absence from the tracker is what an unknown, a never-member and a
    // departed member all look like; only the debt distinguishes them, and only a committed
    // removal mints one.
    let Some(debt) = self.courtesy_owed.get(peer) else {
      return;
    };
    debug_assert!(self.role.is_leader() && self.tracker.progress(peer).is_none());
    // THE INHERITED-TAIL GATE (see `Endpoint::term_start_index`): a fresh leader's applied
    // configuration is stale by its inherited tail, which may already have re-admitted this very
    // peer, so no cure may be derived from it until that tail applies. Suppression spends nothing;
    // the apply fold's re-add edge then prunes any debt the tail voided.
    if self.applied < self.term_start_index {
      return;
    }
    let (removal_index, next_at, last_seen_term) =
      (debt.removal_index, debt.next_at, debt.last_seen_term);
    // The farewell retry owns the peer while its blind budget lasts: firing both would ship a whole
    // blob alongside a cheap append that is very likely to land.
    if self.pending_farewells.contains_key(peer) {
      return;
    }
    if next_at.is_some_and(|at| now.mono() < at) {
      return;
    }
    // THE FUTILITY GATE. Every message this endpoint sends carries THIS leader's term, and the
    // receiver's universal stale-term pre-pass discards anything below its own before a handler
    // ever runs — so an offer stamped at or below the term the peer has been seen carrying is
    // provably dead on arrival. Emitting it anyway would convert the budget into pure waste and,
    // three wasted offers later, evict the debt: the very peer the cure exists for would end up
    // owed nothing, and its next campaign would find a leader with no answer. Suppressing spends
    // NOTHING — no offer, no cooldown — so the debt survives intact for a leader that can pay it.
    //
    // Two alternatives this gate replaces, so nobody re-litigates them: stamping the peer's term
    // would forge a term this leader does not hold, and a receiver-side exception for term-stale
    // removal proofs re-opens the staleness wound the incarnation gate closed — a partitioned
    // leader deposed before a RE-ADD, still holding the debt and an old excluding snapshot, could
    // tear down a peer the CURRENT committed configuration includes.
    //
    // CONVERGENCE, and its cost, stated plainly. Under the etcd-parity defaults an ignorant removed
    // peer's campaign carries a real higher term and DEPOSES this leader. That deposition is
    // self-healing: the live members re-elect (the removed peer cannot win a quorum of a
    // configuration it is not in), whoever wins holds the debt by the universal mint, and its
    // first-tick PROACTIVE offer is term-valid by construction because a fresh leader's term
    // exceeds the term its electorate accepted — so every deposition is followed within a heartbeat
    // of the re-election by a fresh, valid offer. The cure lands on the FIRST DELIVERED offer, and
    // the peer can never campaign again after it.
    //
    // Under persistent targeted loss of every courtesy frame the group degrades to one self-healing
    // deposition per election-timeout window, with the debt RETAINED throughout — the one thing it
    // can never degrade to is an uncured peer with no standing cure, which is precisely what a
    // charge-on-enqueue budget produced. Under pre-vote — which every reshape-born group defaults
    // to — the peer never inflates a term at all, its probes reach the leader as ordinary traffic,
    // and the contact-triggered offer cures it with ZERO depositions.
    //
    // Suppressing the peer's traffic to avoid even that one deposition was tried and withdrawn: no
    // local state can license muting another replica's reconciliation, because a leader with stale
    // configuration history cannot distinguish a departed peer from a re-added one, and every
    // variant of the idea composed into a wedge topology. Suppressing our OWN sends — this gate,
    // the eligibility gate, the inherited-tail gate, the throttle — is always safe; that asymmetry
    // is the design rule.
    // The deliverability boundary is `>=`, not `>`: the receiver drops only what is STRICTLY below
    // its own term, so an offer at exactly the peer's term still lands. That one term matters — it
    // is the whole pre-vote case, where a peer announces itself without ever inflating and a
    // same-term leader can cure it immediately.
    if last_seen_term.is_some_and(|seen| self.term < seen) {
      return;
    }
    // Read the whole blob in one bounded call. `MAX_SNAPSHOT_CHUNK_BYTES` is the store contract's
    // per-call ceiling, so a blob larger than it cannot ride the whole-blob shape anyway.
    let Some(read) = stable.snapshot_chunk(0, crate::config::MAX_SNAPSHOT_CHUNK_BYTES) else {
      return; // nothing persisted yet
    };
    let Ok((meta, total, chunk)) = read else {
      // A fatal store read on a COURTESY path must not poison a healthy leader: this is a liveness
      // favour to a peer that is already gone, and the ordinary replication paths will surface a
      // genuine store fault on their own next read.
      return;
    };
    // THE ELIGIBILITY GATE: the blob must actually CARRY the removal. A capture taken before the
    // removal committed re-baselines the peer onto a configuration that still names it — leaving it
    // ignorant, still armed to campaign, and now at the cost of a whole snapshot. Both legs are
    // checked because either alone can be satisfied by an unrelated capture: the boundary must
    // cover the removal's index, and the ConfState it installs must be one this peer is out of.
    // Ineligible DEFERS rather than spends — the debt stays armed and a later post-removal capture
    // enables it. A leader that never captures past the removal never offers; that residual is the
    // catalog reap's, the same class as the oversized-blob skip below.
    if meta.last_index() < removal_index || self.conf_names(meta.conf(), peer) {
      return;
    }
    let SnapshotChunkRead::Ready(blob) = chunk else {
      return; // cold: the peer's next contact re-drives the offer
    };
    if blob.len() as u64 != total {
      return; // the store could not hand back the whole blob in one call
    }
    // ONE frame, sized exactly as the chunked sender sizes its own — including the transport's
    // group-demux header, so the multi-group wire bound is respected too. The legacy whole-blob
    // shape is `total_len == 0`, so it is sized at that value.
    let (term, me) = (self.term, self.config.id());
    let encoded =
      crate::wire::install_snapshot_encoded_len(term, &me, &meta, 0, 0, blob.len() as u64);
    if encoded.saturating_add(crate::wire::GROUP_HEADER_RESERVE) > crate::wire::MAX_FRAME_BYTES {
      return;
    }
    // Record the offer and start the cooldown. Nothing is SPENT here: enqueueing a frame is not
    // delivering it, so an offer that is lost in flight must leave the debt exactly as it found it.
    // The cooldown is the rate bound, not a budget — a retained debt costs one whole-blob frame per
    // peer per cooldown on a capped map, the same cost class as heartbeating a tracked peer that
    // has died, which a Raft leader does for as long as the peer stays in the configuration.
    //
    // THE DISCHARGE FLOOR IS SET ONCE PER GENERATION, never raised by a retry. Every offer this
    // generation makes carries a boundary at or past the removal index with a ConfState that
    // excludes the peer, so installing ANY of them cures the peer — which makes the EARLIEST
    // boundary offered the correct floor. Raising it to each retry's newer blob would discard the
    // valid evidence of a delayed ack for an earlier offer, leaving a cured peer owed forever. Only
    // a re-removal moves the floor, and it does so by replacing the whole debt.
    let interval = COURTESY_COOLDOWN_TIMEOUTS * self.config.election_timeout();
    if let Some(debt) = self.courtesy_owed.get_mut(peer) {
      debt.offered_index.get_or_insert(meta.last_index());
      debt.next_at = Some(now.mono() + interval);
    }
    self.send(
      peer.cheap_clone(),
      Message::InstallSnapshot(InstallSnapshot::new(term, me, meta, blob)),
    );
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
  ) -> ChunkSend {
    // Read THIS chunk from the store (bounded — one chunk, never the whole blob resident). The read also
    // hands back the meta + total_len, so we reconcile the boundary and size the frame from it. A cold
    // store returns `Pending` (we defer and re-drive on storage-ready); an `Err` is a fatal store fault.
    // Returns a `ChunkSend`: `Sent` when an `InstallSnapshot` was EMITTED (caller arms resend-pacing),
    // `Deferred` for a benign no-send (cold `Pending`, nothing persisted, unsendable frame — caller clears
    // pacing so the next heartbeat retries), or `Poisoned` on a FATAL store error (caller MUST bail).
    let config_chunk = self
      .config
      .snapshot_chunk_bytes()
      .clamp(1, crate::config::MAX_SNAPSHOT_CHUNK_BYTES);
    let (meta, total, chunk_at_from) = match stable.snapshot_chunk(from_offset, config_chunk) {
      None => return ChunkSend::Deferred,
      Some(Ok(read)) => read,
      Some(Err(_)) => {
        self.poison(PoisonReason::SnapshotRead);
        return ChunkSend::Poisoned;
      }
    };
    let boundary = meta.last_index();
    // A peer whose snapshot IDENTITY (boundary + total) no longer matches the local snapshot is mid-transfer
    // on a now-superseded blob: keeping its resume cursor would re-send the NEW blob at the OLD offset (or,
    // for a SMALLER new blob, clamp the cursor past the end and emit a stale empty tail). Reset to the new
    // identity — `become_snapshot` clears the cursor to 0 and records the new `total` — and restart at 0.
    // Matching the TOTAL (not just the boundary) is load-bearing: a newer same-boundary capture is a distinct
    // blob whose byte stream differs, so the old cursor is meaningless against it.
    let from = if matches!(
      self.tracker.progress(&peer).map(|p| p.state()),
      Some(ProgressState::Snapshot { pending, total: t, .. }) if pending == boundary && t == total
    ) {
      from_offset
    } else {
      if let Some(p) = self.tracker.progress_mut(&peer) {
        p.become_snapshot(boundary, total);
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
    // — this is a misconfiguration (a membership too large to snapshot), NOT a transient condition a
    // re-send resolves, so it is a distinct `Unsendable` outcome (the propose-time
    // `MembershipTooLargeToSnapshot` gate normally prevents ever reaching it).
    // Also reserve the transport's group-demux header (prepended to every frame payload) so a
    // maximum-size chunk plus a group tag still fits under the frame limit.
    let frame_budget = crate::wire::MAX_FRAME_BYTES
      .saturating_sub(overhead + data_field_max_self_cost + crate::wire::GROUP_HEADER_RESERVE)
      as u64;
    if frame_budget == 0 {
      // Bump the diagnostic so a permanently-wedged peer is visible; retrying cannot shrink the meta.
      self.snapshot.unsendable_meta_frames = self.snapshot.unsendable_meta_frames.saturating_add(1);
      return ChunkSend::Unsendable;
    }
    let chunk_len = config_chunk.min(frame_budget);
    // Reuse the chunk already read at `from_offset` when the reconciled offset is unchanged (the common
    // path — ONE store read). A boundary RESET (`start == 0`) or an over-cursor clamp (`start == total`)
    // moved the offset, so re-read at `start`.
    let chunk_read = if start == from_offset {
      chunk_at_from
    } else {
      match stable.snapshot_chunk(start, chunk_len) {
        None => return ChunkSend::Deferred,
        // The re-read MUST be the same snapshot: a store that swapped the blob between the two reads would
        // otherwise pair the FIRST read's meta/total with the SECOND read's bytes. Reject the inconsistency
        // rather than send a frame whose meta and data disagree.
        Some(Ok((meta2, total2, read))) => {
          if total2 != total || !meta2.identity_eq(&meta) {
            self.poison(PoisonReason::SnapshotRead);
            return ChunkSend::Poisoned;
          }
          read
        }
        Some(Err(_)) => {
          self.poison(PoisonReason::SnapshotRead);
          return ChunkSend::Poisoned;
        }
      }
    };
    let data = match chunk_read {
      SnapshotChunkRead::Ready(bytes) => {
        // `start <= total` (clamped above), so this never underflows.
        let remaining = total - start;
        // Validate the store's chunk against the `snapshot_chunk` contract — the sender now TRUSTS this API
        // (it no longer locally slices a resident blob). `Ready(empty)` is EOF-ONLY (`start == total`): an
        // in-range empty chunk would have the leader emit an empty NON-final InstallSnapshot, which the
        // follower stages as nothing and re-acks the same cursor — an infinite-resend wedge. A
        // store-contract violation is fatal.
        if remaining > 0 && bytes.is_empty() {
          self.poison(PoisonReason::SnapshotRead);
          return ChunkSend::Poisoned;
        }
        // Trim to BOTH the frame budget AND the remaining blob length. The frame-budget clamp is normally a
        // no-op (it fires only when a huge meta shrank the budget below the bytes the store returned); the
        // `remaining` clamp rejects a store that returned bytes PAST `total_len`, which would otherwise
        // forward an out-of-range chunk the follower's range check decode-poisons as a CORRECT follower.
        // Compute the min in u64, THEN narrow: `remaining`/`chunk_len` can exceed `usize` on a 32-bit target
        // (a bounded streaming store with a > 4 GiB blob), so a lossy `as usize` could truncate the bound
        // BELOW the returned non-empty `bytes` — yielding n == 0 and recreating the in-range-empty wedge.
        // The min is <= bytes.len() (a real usize), so the final cast is lossless.
        let n = (bytes.len() as u64).min(chunk_len).min(remaining) as usize;
        bytes.slice(0..n)
      }
      // Cold: the bytes aren't resident and the store began fetching. Defer — no progress mutation — and
      // re-drive on the storage-ready seam (the heartbeat `resend_snapshot` also re-drives).
      SnapshotChunkRead::Pending => return ChunkSend::Deferred,
    };
    self.send(
      peer,
      Message::InstallSnapshot(InstallSnapshot::new_chunk(
        term, me, meta, data, start, total,
      )),
    );
    ChunkSend::Sent
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
    if self.poison.poisoned {
      return;
    }
    // The shared capture busy/fence set, keyed at this capture's boundary (`applied`): a staged
    // capture/install, the fork durability barrier, the abort replay fence, and the merge replay
    // fence. The per-leg arguments live on `capture_blocked_at` — ONE predicate for every capture
    // producer (this threshold capture, the forced absorb capture, and any future site), so no
    // site can drift by carrying a partial set. A snapshot INSTALL is the one floor-advance that
    // legitimately crosses these fences — it re-baselines to a LEADER's boundary and CLEARS what
    // that boundary covers (`note_freeze_rebaselined`, `note_abort_rebaselined`) instead of
    // leaning on them.
    if self.capture_blocked_at(self.applied) {
      return;
    }
    if self.applied == Index::ZERO {
      // Nothing has been applied yet — nothing to snapshot.
      return;
    }
    // Floor the threshold at 1: `Config::validate` rejects a zero `snapshot_threshold`, but
    // `Endpoint::new` does not validate, and a zero threshold would make `x < 0` always false and
    // capture a full snapshot on every drain — a perpetual snapshot/compaction loop.
    if self.applied.get().saturating_sub(log.first_index().get())
      < (self.config.snapshot_threshold() as u64).max(1)
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
      .with_max_unwalled_lease_window(self.lease_guard.max_unwalled_lease_window)
      // The lineage counter rides every meta (absent at 0), so a restore knows the group's
      // shape/incarnation without replaying the compacted split entries.
      .with_shape_gen(self.split.shape_gen);
    // Carry the read mode EXPLICITLY only if a committed SetReadMode has applied (provenance). A
    // non-migrated node leaves it absent, so a restart from this snapshot falls back to the static config
    // — the presence bit then means "a migration was compacted", not merely "whatever mode was active".
    if self.reads.read_mode_migrated {
      meta = meta.with_read_only(self.reads.active_read_mode);
    }
    // Preserve fork PROVENANCE: a forked child's own snapshots must keep carrying its ForkId, so a
    // child that compacted past its manufactured baseline — or a restart from this snapshot — still
    // reports its origin. Absent for a non-fork group.
    if let Some(fork_id) = &self.split.fork_id {
      meta = meta.with_fork_id(fork_id.clone());
    }
    let opid = self.mint_op_id();
    // The capture's meta rides `pending_compact`: the missed-completion fallback fires only on the
    // durable slot holding THIS capture (identity, lineage included) — bare boundary coverage would
    // let a foreign blob's durability compact this node's own log.
    let pc_meta = meta.clone();
    self.submit_snapshot(stable, opid, meta, bytes::Bytes::from(data));
    // Defer compaction until SnapshotWritten fires.
    self.snapshot.pending_compact = Some((opid, pc_meta));
  }

  /// Reclaim an ABANDONED chunked receive: if the recoverable prefix (`min(commit, ack_watermark())`) has
  /// caught up to or past an in-progress transfer's boundary, the partial is now redundant — free
  /// `snapshot_recv` AND the store's `SnapshotStaging` buffer (a full `total_len` allocation) rather than
  /// pinning it until a future supersede or restart. A no-op when no transfer is in progress or it is still
  /// ahead of the recoverable prefix.
  ///
  /// Returns `false` if the Log-Matching proof read hit a FATAL `term` error (the node is now poisoned via
  /// `log_term`): the caller MUST bail immediately rather than continue handler work on a poisoned node.
  #[must_use]
  /// The ONE snapshot-coverage proof: is `meta`'s snapshot already COVERED by this replica's OWN
  /// history? Shared by the receipt-time redundancy short-circuit, the completion-time re-check, and
  /// the staged-receive reclaim — every "may I treat this snapshot as already-held?" decision asks
  /// HERE, so the lineage clause cannot be forgotten at any one of them.
  ///
  /// The LINEAGE clause comes first and is decisive: `(index, term)` is a content-identity only WITHIN
  /// one lineage (Log Matching §5.3), so a snapshot whose token differs from this replica's — either
  /// direction, `None` included — is NEVER covered, no matter what the local coordinates claim. A
  /// colliding replica that independently committed the boundary coordinates must not treat the fork's
  /// real state as "already present" while holding different bytes: the leader would advance `match`
  /// on a false ack and replicate later entries over permanent divergence.
  ///
  /// Within the lineage, two arms:
  /// - `boundary <= committed_bound` — the caller's committed/recoverable bound (receipt and reclaim
  ///   pass `min(commit, ack_watermark())`; the completion re-check passes bare `commit`, its durable
  ///   evidence already established);
  /// - `boundary <= durable_index` AND the durable entry at `boundary` carries the snapshot's term —
  ///   Log Matching: the durable `[first..=boundary]` IS the snapshot's prefix entry-for-entry, the
  ///   case the committed bound misses on an async follower whose durable log outran `commit`.
  ///
  /// Returns `None` when the Log-Matching term read hit a fatal storage error — the node is already
  /// poisoned via `log_term`, and the caller must bail without acting.
  fn covered_by_local_history<L: LogStore>(
    &mut self,
    log: &L,
    meta: &SnapshotMeta<I>,
    committed_bound: Index,
  ) -> Option<bool> {
    if meta.fork_id() != self.split.fork_id.as_ref() {
      return Some(false);
    }
    if meta.last_index() <= committed_bound {
      return Some(true);
    }
    if meta.last_index() <= self.durable.durable_index {
      return self
        .log_term(log, meta.last_index())
        .map(|t| t == meta.last_term());
    }
    Some(false)
  }

  pub(crate) fn reclaim_stale_snapshot_recv<L: LogStore, S: StableStore<NodeId = I>>(
    &mut self,
    log: &L,
    stable: &mut S,
  ) -> bool {
    // Free the staging buffer whenever the staged snapshot is COVERED by this replica's own history —
    // the same proof as the ack-path short-circuit (`covered_by_local_history`, lineage clause
    // included: a cross-lineage transfer staged on a then-pristine joiner is never "already held" by
    // content this replica gained since, so it is pinned, not silently freed — the conflict stays
    // visible). The committed-only bound alone missed the case where in-window appends made the
    // durable log match through `boundary` while `commit` stayed lower — a later duplicate chunk would
    // then ack the leader out of Snapshot but strand the full `total_len` allocation. Taken out and
    // restored around the proof: `covered_by_local_history` needs `&mut self` for the poisoning term
    // read.
    let Some(r) = self.snapshot.snapshot_recv.take() else {
      return true;
    };
    let bound = core::cmp::min(self.commit, self.ack_watermark());
    match self.covered_by_local_history(log, &r.meta, bound) {
      // Fatal term read: poisoned via `log_term`. Restore the state the caller found and bail.
      None => {
        self.snapshot.snapshot_recv = Some(r);
        false
      }
      Some(true) => {
        stable.discard_snapshot_staging();
        true
      }
      Some(false) => {
        self.snapshot.snapshot_recv = Some(r);
        true
      }
    }
  }

  /// Receive an `InstallSnapshot` from the current leader (follower path). This VALIDATES, persists the
  /// term, and submits the blob — it DEFERS the destructive install body (which touches the log) to
  /// `install_snapshot_now` once the blob is durable. It reads the `LogStore` only to recognize a snapshot
  /// already covered by this follower's durable log (the Log-Matching short-circuit below), so a follower
  /// whose durable log outran `commit` does not waste a full transfer on a snapshot it already holds.
  pub(crate) fn on_install_snapshot<L, S>(
    &mut self,
    now: Now,
    log: &L,
    stable: &mut S,
    is: InstallSnapshot<I>,
  ) where
    L: LogStore,
    S: StableStore<NodeId = I>,
    F::Snapshot: Data,
  {
    if self.poison.poisoned {
      return;
    }
    // THE INCARNATION GATE (the churn cure's install leg — MULTI_RAFT.md, membership churn), run
    // BEFORE the follower preamble so a refusal leaves state and storage byte-unchanged — not even
    // a leader binding or a re-armed timer. An EXCLUDING ConfState (this node in no role) is a
    // REMOVAL directive; carried by a snapshot whose generation is BELOW this replica's committed
    // one, it speaks for an incarnation that is already gone, and a stale view must never reap a
    // live replica. Equal ADMITS, always: within one incarnation the excluding snapshot is exactly
    // the legitimate cure for a removed peer, and rejecting equal would recreate the
    // membership-apply-time staleness the vote path warns about.
    // A leader can never legitimately trip this on a group member: it holds every committed entry,
    // so its captured generation is at least the receiver's, and a conf-change removal does not
    // move the generation at all.
    if !self.conf_names_self(is.snapshot().conf())
      && is.snapshot().shape_gen() < self.split.shape_gen
    {
      self.snapshot.refused_stale_removal_installs = self
        .snapshot
        .refused_stale_removal_installs
        .saturating_add(1);
      return;
    }
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
    // staging buffer (the leak that pins a full `total_len` allocation). A fatal term-read in the proof
    // poisons the node → bail before any further handler work.
    if !self.reclaim_stale_snapshot_recv(log, stable) {
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

    // FORK-PROVENANCE gate: this replica's lineage token rides every snapshot of its own lineage
    // — the manufactured fork baseline carries it, own captures re-stamp it (the forced absorb
    // capture included), and a sibling twin's transfer repeats it — and the handshake fences
    // pre-token peers, so absence is meaningful: an authenticated leader shipping a token-less
    // (or other-token) DESTRUCTIVE install is foreign lineage for this id, not an older version.
    // Landing it would replace this child's state wholesale while the keep-if-set adoption below
    // retains the token — the replica would impersonate the fork on foreign state (a parked
    // parent could resolve its fork redundant against it), and a restart would re-derive
    // provenance from the foreign durable meta and silently shed the token. REFUSE at receipt —
    // BEFORE the redundancy short-circuit below (which on an already-committed OR Log-Matching
    // boundary would ack a foreign leader out of Snapshot and discard this replica's staging
    // WITHOUT ever consulting provenance), and before any staging — so a foreign leader is never
    // released, the foreign blob never becomes durable, and restart re-derivation stays coherent
    // with the live token. The pre-gate steps left above are provenance-neutral: the lease-bound
    // fold is a monotone raise (only lengthens commit-wait), and `reclaim_stale_snapshot_recv`
    // frees only THIS replica's own already-recoverable staging (no ack, no foreign effect).
    // Refusal, not fail-stop: the sender is authenticated, merely mis-lineaged — dropping the
    // message leaves it pinned in Snapshot state, the same standing-conflict posture as a parked
    // fork, resolved by placement (removal / the genuine twin's transfer). A matching token
    // re-installs freely (the twin retransfer leg); a token-less self adopts — but only from
    // NOTHING (the second leg below).
    //
    // EXACT match only — no cross-mint supersession arm exists, by design. A genuine re-mint of
    // the same child (a later split episode) can never repeat this token's coordinates: every
    // committed split bumps the parent's lineage counter (the apply arm demands
    // `parent_gen_after == shape_gen + 1`), so a re-mint carries a strictly higher parent
    // incarnation — and the coordinator's admission floor retires the stale incarnation, tearing
    // its replicas down, BEFORE the re-mint is admissible at all. A replica still advertising a
    // stale mint against a re-minted lineage is therefore a lifecycle breach resolved by
    // placement, never by letting an authenticated leader destructively replace a token-bearing
    // replica — which is exactly the loss this gate exists to prevent.
    if let Some(existing) = &self.split.fork_id
      && meta.fork_id() != Some(existing)
    {
      self.snapshot.refused_cross_lineage_installs = self
        .snapshot
        .refused_cross_lineage_installs
        .saturating_add(1);
      return;
    }
    // A token-less self ADOPTS a lineage only from NOTHING. `(last_index, last_term, conf)` is a
    // content-identity only WITHIN one lineage (Log Matching), so every coordinate proof downstream
    // — the redundancy short-circuit, the reclaim proof, the progress-ack watermark, restart's
    // log-vs-snapshot reconciliation — is sound only when snapshot and receiver share a lineage. A
    // populated replica that accepted a foreign baseline would hold two lineages' artifacts in one
    // durable state (its own committed log and the foreign blob); no later predicate can untangle
    // that, because the log carries no token. So the invariant is held where it is still cheap:
    // a token-bearing snapshot lands only on a replica with NO committed content — empty log,
    // nothing committed, and a durable slot that is either empty or holds THIS SAME LINEAGE. The
    // kin-slot arm is load-bearing: an adoption interrupted after the blob's fsync but before the
    // install (a crash, or a role flip dropping the deferred body) leaves the blob durable with the
    // log still virgin, and the leader must be able to COMPLETE the adoption. It keys on lineage,
    // NOT exact identity: a leader that snapshotted at a different boundary than the orphaned blob
    // — the norm after a leadership change, since replicas compact at their own points — sends its
    // own snapshot (possibly at a LOWER index, plus the tail it will replicate afterward), and from
    // a single latest-snapshot slot it cannot resend the exact orphan. On a virgin receiver that
    // replacement is harmless: nothing is committed, so the incoming install just re-baselines onto
    // its own boundary. Requiring exact identity here would refuse every retry and wedge the joiner
    // out of the group. A DIFFERENT-lineage orphan still blocks — the same standing conflict a
    // populated squatter poses, resolved by placement (remove, tombstone, recreate pristine), never
    // by destructive replacement. The manufactured fork baseline's contract has always been the
    // zero-progress joiner (see `FORK_BASE_INDEX`); this enforces it, uniformly on relay and wire.
    if self.split.fork_id.is_none() && meta.fork_id().is_some() {
      let pristine_or_kin = log.last_index() == Index::ZERO
        && log.first_index() == Index::new(1)
        && self.commit == Index::ZERO
        && stable
          .durable_snapshot()
          .is_none_or(|m| m.fork_id() == meta.fork_id());
      if !pristine_or_kin {
        self.snapshot.refused_cross_lineage_installs = self
          .snapshot
          .refused_cross_lineage_installs
          .saturating_add(1);
        return;
      }
    }

    // Redundancy short-circuit: skip the staging+install entirely when this follower ALREADY holds the
    // snapshot's prefix durably, and ack a position at/above the boundary so the leader advances `match`
    // and leaves `ProgressState::Snapshot`. Redundant two ways:
    //  - `boundary <= min(commit, ack_watermark())` — covered by BOTH the committed prefix AND the durable
    //    RECOVERABLE prefix (`ack_watermark()` = max(durable log tip, durable snapshot boundary)). A
    //    committed snapshot ABOVE `ack_watermark()` is NOT here (commit ran ahead of the durable log over
    //    an unflushed tail); it falls through to the deferred install, which records `durable_snapshot_index`
    //    and whose completion-time re-check drops the destructive body since `boundary <= commit`.
    //  - `boundary <= durable_index` AND the durable log entry at `boundary` carries the snapshot's term —
    //    Log Matching (§5.3): our durable `[first..=boundary]` IS the snapshot's prefix entry-for-entry, so
    //    the snapshot is redundant even though `boundary > commit`. This is the case the committed-only bound
    //    misses: an async follower whose durable log outran `commit` would otherwise STAGE and slowly transfer
    //    a snapshot it already holds (its leader-side `match` is stale-low, so the leader keeps re-sending) —
    //    wasting a whole transfer and pinning the peer in Snapshot. A DIFFERENT term at the boundary is a
    //    DIVERGENT durable tail → NOT redundant → fall through and install (re-baseline over the tail).
    // THE TWO ARMS ANSWER DIFFERENTLY. At or below `commit` there is nothing to advance and nothing
    // to apply, by definition, so the ack is truthful on emission — unchanged. Above `commit`, the
    // arm RAISES commit to the boundary, applies through it, PERSISTS the raise, and gates the ack on
    // that write landing (and on `applied` reaching the boundary).
    //
    // Why a shortcut is available at all: a snapshot at a boundary is proof that boundary is
    // committed — captures are committed prefixes by construction — and Log Matching makes the local
    // durable prefix through it that same prefix, so this replica proves the boundary from evidence
    // it already holds. That is the coordinate-is-commit-evidence rule the wire-echo work settled for
    // sub-`first_index` coordinates on the append path.
    //
    // Why not install instead: `LogStore::restore` is a TOTAL discard by contract, so re-baselining
    // here would destroy the durable, Log-Matching, already-ACKED tail above the boundary. The
    // restart path deliberately does the opposite in exactly this shape — `reconcile_restart_log`
    // answers `Compact(n)`, "preserving the committed tail above `N`" — so this arm IS the runtime
    // half of that parity.
    //
    // Why the persist: the raise alone is volatile. `HardState` carries `commit` and restart
    // recomputes `commit = min(hs.commit(), log.last_index()).max(applied)`, so a DURABLE commit at
    // the boundary plus the durable log is crash-surviving evidence equal to a durable blob. Without
    // the persist, an ack-then-crash reboots at a stale commit with the entry unapplied — a peer
    // revived as a voter that nobody owes a cure.
    // Ack `max(commit, boundary)` (clamped to `ack_watermark()` inside `send_or_gate_snapshot_ack`, which an
    // async follower needs since it can have `commit > durable_index`): for the committed case that is
    // `commit`; for the Log-Matching case the boundary is durable + consistent and exceeds `commit`, so
    // acking it lifts the leader's `match` past `pending`. Persist-before-RESPOND: a non-durable term defers
    // the ack (this path runs no install; the term write is the post-dispatch catch-all in `handle_message`)
    // and `flush_term_gated_acks` releases it.
    // The shared coverage proof (`covered_by_local_history`) decides — lineage clause first: past the
    // door gate this reaches a cross-lineage meta only on a pristine adopter (never covered — a virgin
    // log covers nothing, and the clause keeps that true even if content lands mid-adoption). A fatal
    // `term` Err in the Log-Matching arm is a STORAGE FAILURE, not a mismatch: the node is poisoned and
    // this handler bails — never silently "not redundant", which would STAGE a transfer of a snapshot
    // the follower already holds.
    let bound = core::cmp::min(self.commit, self.ack_watermark());
    let redundant = match self.covered_by_local_history(log, meta, bound) {
      Some(r) => r,
      None => return,
    };
    if redundant {
      // Apply the SAME leader-aware staged-receive cleanup as the supersede path BEFORE acking, so the ack
      // never lifts the leader out of Snapshot while an abandoned partial stays staged (the `reclaim` above
      // only frees a staged receive whose OWN boundary is recoverable — NOT a higher-boundary one a newer
      // leader's redundant lower snapshot supersedes). A same-leader HIGHER-boundary staged receive means
      // this redundant snapshot is a stale LOWER-boundary reorder of an in-flight authoritative transfer:
      // keep it and don't ack a regressive boundary over it.
      if matches!(
        &self.snapshot.snapshot_recv,
        Some(r) if r.sender_term == is.term() && r.meta.last_index() > meta.last_index()
      ) {
        return;
      }
      // Otherwise the staged receive (if any) is moot or superseded → discard its buffer. Unconditional
      // (not gated on `snapshot_recv.is_some()`): a store that persisted staging across a restart holds it
      // WITHOUT a `snapshot_recv` to track.
      self.snapshot.snapshot_recv = None;
      stable.discard_snapshot_staging();
      let leader = is.leader();
      // THE WORK SPLITS ON `commit`; THE ACK DOES NOT. Only the above-commit case has anything to do
      // — raise, apply, persist — but BOTH cases leave through the one gate below, because
      // `self.commit` is a VOLATILE classifier and cannot decide whether an ack is truthful.
      //
      // The at-or-below branch used to ack immediately, and that was unsound the moment this arm
      // began raising commit: a DUPLICATE `InstallSnapshot` arriving inside the gate window — commit
      // already raised to the boundary, its HardState write still in flight or apply still short —
      // classifies as at-or-below against the raised value and would take the term-only path,
      // handing the sender a discharge for state a crash erases. Routing every no-blob ack through
      // the same three legs closes it by construction rather than by another branch.
      //
      // For a genuinely old boundary this is not a behavior change: `durable_commit_index` and
      // `applied` are both already past it, so the gate releases in the same crank the immediate
      // send would have used. What is retired is the claim that the old path was byte-identical —
      // it was, except in exactly the window that made it wrong.
      //
      // ABOVE COMMIT: raise, apply, PERSIST, and gate the ack on that persist landing.
      //
      // A SNAPSHOT AT A BOUNDARY IS PROOF THAT BOUNDARY IS COMMITTED — a capture is a committed
      // prefix by construction, so a sender only offers coordinates it has committed. With Log
      // Matching (§5.3), which is precisely what this arm proved entry-for-entry through `boundary`,
      // the local prefix through it IS that committed prefix. So this replica can prove the boundary
      // LOCALLY, which is the whole reason a shortcut is available here at all. It is the same
      // coordinate-is-commit-evidence doctrine the wire-echo work settled on the append path for
      // sub-`first_index` coordinates.
      //
      // WHY NOT JUST INSTALL. Because installing would be the destructive answer to a question this
      // replica has already answered. `LogStore::restore` is a TOTAL discard by contract
      // (`restore_rebaselined` fail-stops any store that keeps a suffix), so re-baselining here would
      // throw away the durable, Log-Matching, already-ACKED tail above the boundary — the
      // non-quorum-durable-commit hole. The restart path deliberately does the opposite in exactly
      // this shape: `reconcile_restart_log` returns `Compact(n)`, "preserving the committed tail
      // above `N`". This arm IS that parity at runtime; turning it into an install would make the
      // install path contradict the restart path.
      //
      // WHAT MAKES IT RESTART-STABLE. Not the raise — that is volatile — but the DURABLE commit
      // behind it. `HardState` carries `commit`, and restart recomputes
      // `commit = min(hs.commit(), log.last_index()).max(applied)`, so a persisted commit at the
      // boundary plus the durable log is evidence every bit as crash-surviving as a durable blob,
      // and strictly cheaper. The ack therefore waits for the write to LAND (and for apply to reach
      // the boundary); the two-sided crash argument is at `send_or_gate_shortcut_snapshot_ack`.
      //
      // Trusting the sender's boundary at all is the same trust class as installing its blob would
      // be — a forged boundary buys no more than a forged snapshot, and AUTHENTICATION is the
      // boundary of the model (the R1-era adjudication). The guards above are untouched: only a
      // redundancy the existing predicate ALREADY accepts — same lineage, token-legal, term-matched
      // at `boundary` — reaches this.
      if meta.last_index() > self.commit {
        self.commit = meta.last_index();
        if self.applied < self.commit {
          self.apply_committed(log);
        }
        // `apply_committed` can self-poison on a fatal committed-range read / decode / FSM apply —
        // fail-stop before acking rather than answering success from a dead node.
        if self.poison.poisoned {
          return;
        }
        self.persist_commit_floor(stable);
      }
      self.send_or_gate_shortcut_snapshot_ack(leader, meta.last_index());
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

    // SLOT MONOTONICITY: the store keeps ONE latest snapshot, so a submit is destructive — it
    // REPLACES whatever the slot holds. An inbound snapshot strictly below the slot's boundary must
    // therefore never reach `submit_snapshot`: the slot may be the only baseline for a prefix the
    // log has already compacted (a local capture at N compacts through N; a stale leader with an
    // older compaction point then legitimately offers M < N because this replica's ack watermark
    // honestly lagged), and replacing it with M leaves (M, N] recoverable NOWHERE — a crash then
    // restarts into an orphaned log. The VISIBLE slot boundary, tracked endpoint-side so no store
    // read is needed: the max of the durable boundary and both submitted-awaiting-fsync boundaries
    // (a submitted-but-unfsynced higher capture must not be clobbered either — store completions
    // are FIFO, so a later lower submit would end up the durable slot). Checked HERE, after the
    // coverage short-circuit — which already answers most stale offers with a redundant ack once
    // the watermark covers them — because in the capture-fsync window the watermark does NOT yet
    // cover the visible slot, and this drop is the only guard. Silent, no ack: the boundary may not
    // be recoverable yet, so acking would over-claim; the sender's heartbeat-paced resend re-drives,
    // and once the capture fsyncs the retry resolves redundant at the coverage arm. Strictly LOWER
    // only — an equal-boundary different-identity snapshot is the documented re-snapshot supersede.
    let visible_slot = self
      .durable
      .durable_snapshot_index
      .max(
        self
          .snapshot
          .pending_compact
          .as_ref()
          .map_or(Index::ZERO, |(_, m)| m.last_index()),
      )
      .max(
        self
          .snapshot
          .pending_install
          .as_ref()
          .map_or(Index::ZERO, |(_, m, ..)| m.last_index()),
      );
    if visible_slot > meta.last_index() {
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

    // An empty chunk delivers no bytes, so it must NEVER begin, replace, or extend a staging buffer — it can
    // only be a stale-cursor artifact. Handle it BEFORE the continuation/supersede/stage logic so a malformed
    // or version-skewed leader cannot pin a `total_len` staging allocation or replace the active
    // `snapshot_recv` with a no-progress one. A correct sender emits empty data ONLY at EOF
    // (`offset == total_len`); any other offset is malformed and fail-stops.
    if is.data().is_empty() {
      if is.offset() != total_len {
        self.poison(PoisonReason::SnapshotDecode);
        return;
      }
      // EOF: re-ack the follower's TRUE contiguous watermark — the matching transfer's staged length, or 0
      // when no transfer matches (a not-yet-started or superseded identity has staged nothing for this blob).
      // NEVER drop: the leader sets its resume cursor from this ack (`snapshot_acked` is not monotone), so a
      // true-watermark ack re-syncs a leader whose cursor ran ahead and RESTARTS a stranded transfer at 0
      // rather than letting it resend the same empty tail forever. Allocates nothing and starts nothing.
      let staged = match &self.snapshot.snapshot_recv {
        Some(r)
          if r.sender_term == is.term() && r.meta.identity_eq(meta) && r.total_len == total_len =>
        {
          r.contiguous_staged
        }
        _ => 0,
      };
      self.send_snapshot_progress_ack(is.leader(), staged, meta.last_index());
      return;
    }

    let boundary = meta.last_index();
    // Identify the in-progress transfer by its FULL identity — (sender_term, last_index, last_term, conf,
    // fork_id, total_len) — NOT just the boundary index. Two independent keys bound byte-mixing, and each
    // covers a collision the other cannot see:
    //
    // The SENDER TERM bounds the cross-LEADER recapture WITHIN one lineage: a NEWER leader sending a snapshot
    // with the same coordinate and length is a DISTINCT capture, not a continuation — appending its chunks
    // into the old leader's staging would MIX bytes from two independently-captured snapshots (the
    // StateMachine contract does not promise byte-identical encodings across leaders for the same applied
    // state). A SAME-term recapture at the same boundary is impossible by construction — a leader snapshots
    // only its own monotone `applied`, `maybe_snapshot` is single-flight, and compaction advances
    // `first_index` PAST the boundary — so within a lineage `sender_term` discriminates exactly the recapture.
    //
    // The LINEAGE TOKEN (carried by `identity_eq`) bounds the cross-LINEAGE collision, which the term CANNOT:
    // `(last_index, last_term, conf)` is not a content-identity across a fork boundary, so a manufactured fork
    // baseline and a colliding tokenless (or differently-forked) snapshot are DIFFERENT bytes — and nothing
    // stops them arriving under the SAME sender term. Keyed on the term alone, their chunks would combine into
    // one Frankenstein blob spanning two lineages.
    //
    // Both keys are checked HERE in the core BEFORE staging — NOT in the store's staging key, which omits the
    // term (`discard_snapshot_staging` runs before the first accept of any differing term). A mismatch routes
    // to the supersede/replace path below. (The LeaseGuard / read-mode bounds are folded ungated above and may
    // legitimately differ between same-boundary snapshots, so they are NOT part of the identity.)
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
      // Retire a superseded COMPLETED install NOW, before staging the replacement. This fresh transfer
      // passed the duplicate-install guard, so any `pending_install` is at a same-or-lower boundary AND a
      // DIFFERENT identity — this snapshot supersedes it. Left live, its in-flight `SnapshotWritten` would
      // complete `install_snapshot_now` for the STALE snapshot while THIS replacement is still partial,
      // restoring/acking superseded metadata. The single-shot path retires it by overwriting in the same
      // dispatch; a chunked replacement (which defers its own install) must clear it explicitly. The
      // orphaned durable blob is harmless — a later `maybe_compact`/restart reconciles it.
      self.snapshot.pending_install = None;
      self.snapshot.snapshot_recv = Some(SnapshotRecv {
        meta: meta.clone(),
        total_len,
        contiguous_staged: 0,
        sender_term: is.term(),
      });
    }

    // Validate the NON-EMPTY chunk's byte-range BEFORE staging — a chunk past `total_len` would be silently
    // CLAMPED by the staging accumulator, completing the buffer from a malformed stream and decoding a
    // TRUNCATED prefix instead of fail-stopping. (Empty chunks are fully handled above, so `data.len() >= 1`
    // here.) Checked arithmetic: a `u64` overflow is out of range.
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
      // chunk but can never advance `match_index` to the boundary (the peer stays in Snapshot state).
      let leader = is.leader();
      self.send_snapshot_progress_ack(leader, staged, boundary);
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
  /// (`acked_through`) so the leader sends the next chunk, with `match_index` = the persist-before-ack-safe
  /// RECOVERABLE watermark `min(commit, ack_watermark)` CLAMPED strictly below the transfer's boundary.
  ///
  /// The clamp is what keeps a progress ack from lifting the peer out of Snapshot state on the leader
  /// (`maybe_update` exits at `match >= pending`, counting a phantom replica before the blob is durably
  /// installed). The watermark alone is NOT strictly below the boundary for every transfer that stages:
  /// the receipt-time redundancy short-circuit guarantees it only for a snapshot COVERED by this replica's
  /// own history — a snapshot from a different lineage legitimately stages at a boundary the local
  /// coordinates appear to cover, so the invariant must hold by value, not by handler ordering. UNGATED
  /// (unlike the final install ack): it makes no NEW durable commitment — the watermark is already durable
  /// and `acked_through` is a transfer-progress hint — so a crash that loses it merely restarts the
  /// transfer.
  pub(crate) fn send_snapshot_progress_ack(&mut self, to: I, acked_through: u64, boundary: Index) {
    let (term, me) = (self.term, self.config.id());
    let below_boundary = Index::new(boundary.get().saturating_sub(1));
    let match_index = self.commit.min(self.ack_watermark()).min(below_boundary);
    self.send(
      to,
      Message::SnapshotResponse(
        crate::SnapshotResponse::new(term, me, false, match_index)
          .with_acked_through(acked_through)
          .with_progress(true),
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
  pub(crate) fn install_snapshot_now<L: LogStore, S: StableStore<NodeId = I>>(
    &mut self,
    log: &mut L,
    stable: &mut S,
    meta: SnapshotMeta<I>,
    snap: F::Snapshot,
    leader: I,
  ) where
    F::Snapshot: Data,
  {
    if self.poison.poisoned {
      return;
    }
    // Only a FOLLOWER installs a snapshot (etcd parity). A deferred install can complete after this node
    // became a candidate or leader — its blob fsync outliving an election it won on a longer, visible
    // log. Running the re-baseline below would then discard the log tail the election was counted on
    // (Leader Completeness), and the `pending_stable` retain further down would delete a live
    // `Pending::Campaign` self-vote (so `self_vote_durable()` would wrongly report the self-vote durable,
    // reopening a same-term double vote). A winner already holds every committed entry through the
    // boundary (the up-to-date check + Leader Completeness), so the snapshot is redundant with entries it
    // has — drop it. The blob stays durable in the store, and both exits stay coherent: a
    // genuinely-behind candidate that reverts to follower re-fetches from the leader (its ack never
    // advanced; a same-identity retransfer passes the provenance gate against the leftover slot), and
    // a restart reconciles lineage-first — a same-lineage blob resolves by the ordinary log-vs-boundary
    // arms, a cross-lineage one is an unadopted leftover restart IGNORES (never restored under a log
    // that outvoted it). Skipping the `durable_snapshot_index` raise below is deliberate here: for a
    // leader it would claim local durability at a boundary the real log has not yet reached.
    if !self.role.is_follower() {
      return;
    }
    // A lineage-ADOPTING install (token-less self, token-bearing snapshot) lands only on the
    // content-emptiness the door gate demanded — re-checked HERE because admission was deferred
    // behind the blob's fsync while the gate's check ran at receipt, and the window between them
    // admits appends. Any content that filled the window is ANOTHER lineage's: this replica's own
    // lineage cannot reach it (a fork member's log starts past the manufactured baseline, so it
    // cannot append below the boundary to a zero-progress joiner — the joiner is structurally
    // forced onto the snapshot path), so re-baselining over it would silently destroy a foreign
    // group's durably-acked entries — replacement, where the doctrine demands placement. REFUSE:
    // count and drop; the filled log stands, the conflict stays visible (every retransfer now
    // refuses at receipt — the receiver is populated), and the sender resolves by placement. The
    // `durable_snapshot_index` raise below is skipped with the same reasoning as the role gate's
    // skip: a refused adoption's boundary is NOT recoverable (restart ignores the unadopted
    // leftover), so raising the watermark would over-claim the recoverable prefix.
    if self.split.fork_id.is_none()
      && meta.fork_id().is_some()
      && !(log.last_index() == Index::ZERO && self.commit == Index::ZERO)
    {
      self.snapshot.refused_cross_lineage_installs = self
        .snapshot
        .refused_cross_lineage_installs
        .saturating_add(1);
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
    // Completion-time redundancy re-check (the LAST line of defense before the destructive `log.restore`).
    // In-window AppendEntries can have caught this follower up to/PAST the boundary while the blob was in
    // flight — and the durable log tip can outrun the in-memory `commit` (an async follower makes entries
    // durable, and acks them, before it learns they are committed). DROP the deferred install (keep the
    // durable log) when the follower's durable log ALREADY holds the snapshot's committed prefix, proven
    // two ways:
    //   (a) `boundary <= commit` — committed history is never divergent, so the durable log holds it; OR
    //   (b) `boundary <= durable_index` AND the durable entry at `boundary` carries the snapshot's term —
    //       Log Matching (§5.3): same index+term ⇒ our durable `[first..=boundary]` IS the snapshot's
    //       prefix entry-for-entry, so installing would ONLY destroy durably-acked entries above it (the
    //       non-quorum-durable-commit hole: re-baselining over a longer, consistent, already-acked tail).
    // A DIFFERENT term at the boundary (or a boundary above the durable tip) ⇒ a divergent / short durable
    // tail ⇒ NOT redundant ⇒ fall through and re-baseline. The restart path enforces the identical
    // invariant in `reconcile_restart_log`; this brings the runtime install path to parity. A fatal `term`
    // Err is a STORAGE FAILURE, not a mismatch: it funnels through `log_term`, which poisons
    // (`PoisonReason::LogTerm`) and returns `None`. Treating an Err as "not redundant" would instead fall
    // through to the destructive `log.restore` on unreadable state — discarding a durable tail that may
    // actually match the snapshot prefix (the very hole this guard closes).
    // The shared coverage proof (`covered_by_local_history`), with bare `commit` as the committed
    // bound — durable evidence is already established here (the blob is durable and
    // `durable_snapshot_index` was raised above). The lineage clause is defensive at this site: a
    // lineage-ADOPTING install reaching here proved the receiver still content-empty above, so its
    // coordinates cover nothing — the clause simply keeps the answer exact if either gate ever
    // weakens (foreign coverage must never read as "already durable"). A fatal `term` Err poisons
    // and bails (treating it as "not redundant" would `log.restore` over unreadable state).
    let redundant = match self.covered_by_local_history(log, &meta, self.commit) {
      Some(r) => r,
      None => return,
    };
    if redundant {
      // Release the leader from `ProgressState::Snapshot` NOW, without waiting for a heartbeat resend: a
      // follower that caught up while the blob was fsyncing must ack a position at/above the boundary
      // immediately (mirror the receipt-time short-circuit). `durable_snapshot_index` was raised to the
      // boundary above, so `ack_watermark()` already covers it; ack `max(commit, boundary)` (clamped to
      // `ack_watermark()` inside `send_or_gate_snapshot_ack`) so the leader's `match` advances past
      // `pending`. Persist-before-RESPOND: a non-durable term defers the ack via the term-gated queue.
      //
      // THE SAME WORK-SPLIT-THEN-ONE-GATE AS THE RECEIPT-TIME ARM, for the same reasons (see there).
      // Above commit — arm (b), where the durable log outran commit — the boundary is locally proven
      // committed, so raise, apply and persist rather than re-baselining over a durable Log-Matching
      // tail the restart path would have preserved. At or below commit there is no work. Either way
      // this is a NO-BLOB ack, so it leaves through the gate: volatile `commit` cannot classify a
      // duplicate arriving inside the window as already-truthful.
      if meta.last_index() > self.commit {
        self.commit = meta.last_index();
        if self.applied < self.commit {
          self.apply_committed(log);
        }
        if self.poison.poisoned {
          return;
        }
        self.persist_commit_floor(stable);
      }
      self.send_or_gate_shortcut_snapshot_ack(leader, meta.last_index());
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
    self.pending_log.clear();
    self
      .pending_stable
      .retain(|(_, p)| matches!(p, Pending::CastVote { .. }));
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
    // Adopt the installed meta's lineage (monotone), so this replica's OWN later snapshots keep
    // carrying it — a straggler that installed a post-split parent snapshot, then leads and
    // compacts, must not drop the fold.
    self.split.shape_gen = self.split.shape_gen.max(meta.shape_gen());
    // Adopt the installed meta's fork PROVENANCE: a sibling replica's manufactured fork baseline (or
    // a forked child's own snapshot) carries the child's ForkId, so a child materialized ONLY via
    // snapshot transfer — never through the local fork constructor — still reports its origin, and
    // the parent's parked fork then resolves REDUNDANT against exactly this token. Keep-if-set: a
    // later non-fork snapshot must not erase an established provenance.
    if let Some(fork_id) = meta.fork_id() {
      self.split.fork_id = Some(fork_id.clone());
    }

    // Step 4: re-baseline the log on the now-durable snapshot. Discards the follower's stale/short log;
    // after this call first_index == last_index + 1 and term(last_index) == last_term, so the next
    // AppendEntries(prev=last_index) passes the consistency check. Because the blob is already durable,
    // a crash immediately after this leaves {durable snapshot present, log re-baselined} OR {durable
    // snapshot present, log not-yet-re-baselined} — both of which `reconcile_restart_log` recovers
    // (None/Compact/Restore), NEVER the OrphanedLog poison.
    log.restore(meta.last_index(), meta.last_term());
    // The re-baseline discarded every entry above the boundary — a pending merge freeze among
    // them no longer exists in this log, so the append-observed kill releases (re-armed at
    // accept if the freeze is still live and re-delivered).
    self.note_freeze_rebaselined();
    // The re-baseline also crossed the APPLIED freeze, so clear the whole applied-freeze quartet,
    // not just the append-observed pending flag above. No conforming snapshot boundary can sit
    // INSIDE a live freeze: every source-side capture is fenced by `merge_freeze_active` for the
    // freeze's whole life, the forced absorb capture is target-side, and a fork is refused on a
    // freezing parent — so any install boundary proves every freeze it covers already RESOLVED (the
    // same totality argument `note_freeze_rebaselined` makes for the pending flag). Leaving the
    // quartet set would strand this replica frozen FOREVER — captures fenced, proposes/reads/conf/
    // transfers refused if elected, its stale `frozen_for` blocking the claimed target's removal —
    // while a plain restart from the same durable state derives NOT-frozen (install and restart must
    // agree). The sole other clear site is the thaw apply arm.
    self.merge.frozen = false;
    self.merge.freeze_index = None;
    self.merge.freeze_term = None;
    self.merge.frozen_for = None;
    // A parked CommitMerge is SUPERSEDED by the install: at-or-below the boundary, the blob IS
    // the union (the target leader's forced absorb capture sits past every resolution, so a
    // log-behind straggler is caught up wholesale without ever touching its local source);
    // above the boundary, the replay re-encounters the entry and re-parks from log-fixed data.
    // A stale park kept here would wedge the drain forever below a boundary it cannot re-reach.
    self.merge.pending_apply = None;
    // The re-baseline discarded every abort entry at-or-below the boundary — the ONLY restart
    // re-derivation of the `abandoned` obligation. The install sits past the committed+applied abort,
    // proving the source thawed past the abandoned freeze (the capturing leader's own service drove
    // it), so a covered obligation is MOOT; clear it here or `abort_relay_fences` would stay stuck on
    // a boundary the install already crossed (the source thawed and gone, so the service could never
    // observe it advance to discharge it) — a permanent capture wedge. An obligation above the
    // boundary is retained (see the helper).
    self.note_abort_rebaselined(meta.last_index());
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

    // The membership transition this install performs, computed while the tracker still holds the
    // PRIOR applied configuration (step 6 replaces it below).
    let removed_members = self.install_removed_members(meta.conf());

    // Step 5: emit the application event.
    self
      .outputs
      .events
      .push_back(crate::Event::SnapshotInstalled(meta.clone()));
    // A removal this replica just APPLIED — committed state that arrived as a snapshot instead of a
    // log entry. Surface it through the very event the log-applied `ConfChange` fold emits, so the
    // embedder's removed-self path fires identically whichever way the removal reached us; an
    // install alone would silently disarm campaigning (the rebuilt tracker makes
    // `is_voter(self)` false) and leave the application unaware it must tear the replica down.
    //
    // Keyed on the TRANSITION, never on mere absence from the installed ConfState (see
    // `install_removed_members`). A fresh joiner or a fork-born observer routinely installs a
    // HISTORICAL snapshot — one captured before it was ever admitted — whose ConfState cannot name
    // it; reading that as removal would tell a replica that is mid-join to tear itself down, while
    // the entries after the boundary were about to admit it normally. A membership-neutral install,
    // and any install that only re-roles this replica, keep the single-event shape.
    if removed_members.contains(&self.config.id()) {
      self
        .outputs
        .events
        .push_back(crate::Event::ConfChanged(crate::ConfChanged::new(
          meta.last_index(),
          meta.conf().clone(),
        )));
    }

    // Step 6: install the membership from the snapshot's ConfState — jump directly to the committed
    // membership at the snapshot point; the Tracker is rebuilt from the snapshot's conf.
    self.tracker = crate::Tracker::from_conf_state(
      meta.conf(),
      meta.last_index(),
      self.config.max_inflight_msgs(),
      self.config.max_inflight_bytes(),
    );
    // Mirror the log-applied re-add prune (see the ConfChange arm of `apply_committed`): this snapshot's
    // ConfState is a fresh source of committed-membership truth, and it may RE-ADMIT a peer whose stale
    // removal is PARKED here — a follower can carry a parked map across a demotion. Drop every entry the
    // rebuilt tracker now tracks, so a later re-election never re-arms an obsolete removal against a
    // CURRENT voter. Keep only still-untracked targets — the same `tracker.progress(peer).is_none()`
    // predicate the log-applied prune and the `become_leader` reconcile use.
    let tracker = &self.tracker;
    self
      .pending_farewells
      .retain(|peer, _| tracker.progress(peer).is_none());
    // The install edge reconciles the courtesy debts in BOTH directions, because a snapshot's
    // ConfState is committed-membership truth about every peer at once:
    //
    //   PRUNE the re-admitted — a peer this ConfState names is a member again and is owed nothing.
    self
      .courtesy_owed
      .retain(|peer, _| tracker.progress(peer).is_none());
    //   MINT the newly-removed — every peer the transition dropped. Without this a removal learned
    // ONLY by snapshot (a replica offline across the whole conf change, caught up wholesale)
    // leaves no debt anywhere on that replica, so if it later leads it owes the departed peer
    // nothing, adopts its next campaign in the term pre-pass, and the disruption cycle recurs at
    // it — exactly the gap the universal log-apply mint closes for the replay path.
    //
    // The boundary is the removal index used, and it is the conservative choice in the only
    // direction that matters: the removal committed at or below it, and THIS snapshot is by
    // construction eligible to serve as the cure (its index IS the boundary and its ConfState
    // excludes the peer), so a debt minted here can always be paid. An existing debt is replaced
    // only by a strictly LATER boundary, matching the re-removal rule, and a replacement restarts
    // the budget. Self is never minted: this replica does not owe itself a courtesy.
    let me = self.config.id();
    let boundary = meta.last_index();
    let newly_removed: std::vec::Vec<I> = removed_members
      .iter()
      .filter(|peer| **peer != me)
      .map(CheapClone::cheap_clone)
      .collect();
    for peer in newly_removed {
      self.note_courtesy_debt_at_boundary(&peer, boundary);
    }

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

  /// Receive a `SnapshotResponse` from a follower (leader path), or from a peer this replica owes
  /// a courtesy cure — that one discharge is honored on ANY role, ahead of the leader gate.
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
    F::Snapshot: Data,
  {
    if self.poison.poisoned {
      return;
    }
    // THE COURTESY DEBT'S ONLY EVIDENCE-BASED DISCHARGE, routed ahead of BOTH the role gate and the
    // tracker lookup below — a removed peer has no `Progress`, so its response would otherwise die
    // at that bail, which is exactly why the old design had no delivery evidence to reason from.
    // An install ack (not a reject, not a mid-transfer progress ack) at or past the EARLIEST
    // boundary offered for this removal generation is proof the peer installed a configuration
    // that excludes it, so the debt is discharged.
    //
    // ROLE-INDEPENDENT, because the debt is: the universal mint parks one on every replica, and a
    // step-down between the offer and its ack must not throw the evidence away — the cure would
    // then be re-offered forever to a peer that already installed it. Evidence is honored wherever
    // the debt lives.
    //
    // A FLOOR MUST EXIST. An ack that arrives before this generation's first offer discharges
    // nothing: it can only be evidence of some earlier generation's blob, which may still name the
    // peer. Leaving the debt standing is the safe direction — at worst it costs one more offer,
    // and the install is idempotent.
    //
    // The routing is NARROW: evict and RETURN. On no role does an owed sender's response reach
    // tracker processing, and nothing else here reads an untracked sender — the only statement
    // before this point is the poison bail, and every statement after it goes through the
    // `progress_mut` lookup that an owed peer cannot satisfy.
    //
    // CFT, stated at the site: this trusts an authenticated peer's own report about its own state,
    // which is exactly what the crash-fault model licenses — the peer is honest or it is crashed.
    // A peer that acked without installing has chosen to stay disarmed, which costs only itself:
    // the ack cannot move any commit index (the responder is not in the configuration and holds no
    // `Progress`), so a lie here buys nothing but its own ignorance.
    if !response.reject()
      && !response.progress()
      && self.courtesy_owed.get(&from).is_some_and(|debt| {
        debt
          .offered_index
          .is_some_and(|floor| response.match_index() >= floor)
      })
    {
      self.courtesy_owed.remove(&from);
      return;
    }
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
      // A PROGRESS ack is match-inert BY TYPE: it drives the resume cursor and chunk pacing below,
      // but never touches `match_index` — so it cannot exit Snapshot state or feed the commit
      // quorum. Only an install/redundancy ack (a durable log-state assertion by the follower) may.
      // Enforced HERE, on the leader, so the rule holds regardless of the peer's arithmetic — a
      // version-skewed or malformed peer cannot mint replication progress from a transfer that has
      // not durably installed.
      if !response.progress() {
        // Boundary check (shared with `on_append_response` via `match_within_log`): a successful
        // snapshot ack must not report a match above the leader's own log, for the same reason — an
        // over-run would corrupt `Progress` and could push the commit candidate off the log and
        // poison the leader. Ignore the malformed ack; the peer stays in Snapshot and is re-probed
        // normally.
        if !Self::match_within_log(response.match_index(), log) {
          return;
        }
        // maybe_update drives the Snapshot → Probe transition regardless of its return value
        // ("advanced" hint). We resume unconditionally so a peer leaving Snapshot is never left
        // un-poked. Drop `pr` before the self.* calls (borrow discipline mirrors on_append_response).
        pr.maybe_update(response.match_index());
      }
      // Advance the resume cursor from the follower's contiguous watermark, then — if the peer is STILL
      // mid-transfer (a progress ack leaves Snapshot state untouched by construction) AND this
      // ack MOVED the cursor — send the next chunk. A single-chunk snapshot's FINAL ack lifts the peer
      // out of Snapshot via maybe_update above, so this no-ops.
      //
      // The moved-gate is the snapshot sibling of the append path's advance gate (`on_append_response`
      // pumps only when `maybe_update` ADVANCED the match): a DUPLICATED ack echoing the same watermark
      // must not send the chunk again. Without it every network-duplicated chunk or ack raises the
      // stream's in-flight multiplicity FOREVER (the follower re-acks each duplicate chunk at its true
      // watermark, each such ack re-sent the chunk, and further duplications compound) — an unbounded
      // per-transfer message storm under a lossy/duplicating network. Suppressing the same-cursor pump
      // loses no liveness: a lost next-chunk stalls the cursor, and the heartbeat-paced
      // `resend_snapshot` (on_heartbeat_response) re-sends FROM the stalled cursor within one election
      // timeout. A CHANGED cursor pumps regardless of direction — a regression to a lower watermark (a
      // follower that restarted its staging) legitimately resumes the stream from the new position.
      let cursor_before = match self.tracker.progress(&from).map(|p| p.state()) {
        Some(ProgressState::Snapshot { acked_through, .. }) => Some(acked_through),
        _ => None,
      };
      if let Some(pr) = self.tracker.progress_mut(&from) {
        pr.snapshot_acked(response.acked_through());
      }
      if let Some(ProgressState::Snapshot { acked_through, .. }) =
        self.tracker.progress(&from).map(|p| p.state())
        && cursor_before != Some(acked_through)
      {
        // Pump the NEXT chunk. Arm resend-pacing on a real send; CLEAR it on a benign defer so the next
        // heartbeat retries immediately (merely not re-arming is insufficient HERE — the prior pump left a
        // FUTURE deadline, and the peer, with no new chunk to ack, sends no further progress ack to re-drive
        // it, so a lingering future deadline would suppress the retry for up to a full election timeout); and
        // BAIL on a fatal store error — a poisoned node must not fall through to commit/apply below.
        match self.send_snapshot_chunk(from.cheap_clone(), stable, acked_through) {
          ChunkSend::Sent => {
            self.snapshot.snapshot_resend_after.insert(
              from.cheap_clone(),
              now.mono() + self.config.election_timeout(),
            );
          }
          ChunkSend::Deferred | ChunkSend::Unsendable => {
            self.snapshot.snapshot_resend_after.remove(&from);
          }
          ChunkSend::Poisoned => return,
        }
      }
      // Re-borrow self for the resume sequence (pr is dropped above).
      self.maybe_advance_commit(now, log);
      self.apply_committed(log);
      // maybe_advance_commit / apply_committed can self-poison → fail-stop before the deferred-read flush
      // and the append pump (both no-op on a poisoned node, but the explicit bail keeps "no work after
      // poison" airtight rather than relying on those downstream entry guards).
      if self.poison.poisoned {
        return;
      }
      self.maybe_flush_deferred_reads(now, log, stable);
      self.maybe_send_append(now, from.cheap_clone(), log, stable);
      // maybe_flush_deferred_reads / maybe_send_append can self-poison → fail-stop before the transfer
      // tail attempts a TimeoutNow on a dead node (mirrors on_append_response).
      if self.poison.poisoned {
        return;
      }
      // Leader transfer: a transferee that caught up via this snapshot must trigger the handoff too —
      // its match jumps to last_index on the snapshot ack and never advances again, so the append-path
      // trigger would never fire.
      self.maybe_hand_off_to_transferee(&from, log);
    }
  }
}
