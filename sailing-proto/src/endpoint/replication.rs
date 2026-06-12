use super::*;

impl<I, F> Endpoint<I, F>
where
  I: NodeId,
  F: StateMachine,
{
  pub(crate) fn arm_heartbeat_timer(&mut self, now: Instant) {
    self.heartbeat_deadline = Some(now + self.config.heartbeat_interval());
    // Callers that need to clear election_deadline (e.g. become_leader when check_quorum is
    // false) do so explicitly; we do NOT touch election_deadline here so the CQ timer
    // (set by become_leader when check_quorum is true) is not clobbered on each heartbeat.
  }

  pub(crate) fn broadcast_heartbeat(&mut self, now: Instant) {
    // Start a FRESH CheckQuorum lease round: bump the round, record its SEND instant, and clear the
    // per-round ack set, so the read lease (`lease_valid_until`) is renewed only by HeartbeatResp
    // echoing THIS round and is bounded by this round's send time. A stale/duplicated
    // earlier-round response then cannot keep an isolated leader's lease alive, and a delayed
    // current-round response cannot extend it past the quorum's election window.
    self.lease_round += 1;
    self.lease_round_start = now;
    self.lease_acks.clear();
    // the contributing quorum's min support resets to the leader's OWN election_timeout (its self
    // support); each enforcing ack this round mins it down so a shorter-timeout voter caps the lease.
    self.lease_min_support = self.config.election_timeout();
    let (term, me, lease_round) = (self.term, self.config.id(), self.lease_round);
    // Carry the last-pending-read context so followers can echo it back, giving the
    // leader the acks it needs to confirm outstanding safe reads.  An empty context
    // means there are no pending reads (the echo is harmless either way).
    let ctx = self
      .read_only
      .last_pending_request_ctx()
      .cloned()
      .unwrap_or_default();
    for peer in self.peers().collect::<std::vec::Vec<_>>() {
      // Clamp the advertised commit to this peer's known match index. A heartbeat carries
      // no prev-log check, so the follower can only safely commit up to the prefix it has
      // proven (via a consistency-checked AppendEntries) matches ours. Telling a peer to
      // commit past its match index lets a freshly-restarted node with a divergent,
      // uncommitted tail commit+apply a stale entry (the etcd `min(committed, pr.Match)`
      // rule). Default to ZERO if progress is unknown.
      let peer_commit = self
        .tracker
        .progress(&peer)
        .map(|pr| core::cmp::min(self.commit, pr.match_index()))
        .unwrap_or(Index::ZERO);
      self.send(
        peer,
        Message::Heartbeat(
          crate::Heartbeat::new(term, me, peer_commit, ctx.clone()).with_lease_round(lease_round),
        ),
      );
    }
  }

  /// Broadcast a heartbeat to all peers carrying a specific `context`.
  ///
  /// Used by the ReadIndex Safe path to kick off a dedicated heartbeat round that
  /// proves the leader is still reachable by a quorum.
  pub(crate) fn broadcast_heartbeat_with_ctx(&mut self, _now: Instant, ctx: Bytes) {
    // Carry the CURRENT lease round (do NOT bump — only the periodic `broadcast_heartbeat` opens a new
    // round) so responses to this read-path heartbeat also count toward the lease.
    let (term, me, lease_round) = (self.term, self.config.id(), self.lease_round);
    for peer in self.peers().collect::<std::vec::Vec<_>>() {
      let peer_commit = self
        .tracker
        .progress(&peer)
        .map(|pr| core::cmp::min(self.commit, pr.match_index()))
        .unwrap_or(Index::ZERO);
      self.send(
        peer,
        Message::Heartbeat(
          crate::Heartbeat::new(term, me, peer_commit, ctx.clone()).with_lease_round(lease_round),
        ),
      );
    }
  }

  /// The cap-unit "size" of one entry: a FIXED per-entry overhead (Term 8 + Index 8 + EntryKind 1)
  /// PLUS the payload bytes. Charging a NONZERO per-entry cost — not just `data().len()` — is what makes
  /// `max_size_per_msg` actually bound the per-send entry COUNT (and thus the cloned `Vec<Entry>`):
  /// otherwise a long run of zero-byte entries (no-ops, or commands whose encoding is empty) would each
  /// cost 0, the budget would never decrease, and the packing loop would clone+send the WHOLE suffix in
  /// one message regardless of the cap — a flow-control bypass / OOM risk. Mirrors etcd's
  /// `limitSize`, which charges each entry's full encoded `Size()` (never zero).
  #[inline(always)]
  pub(crate) fn entry_size(e: &crate::Entry) -> u64 {
    const ENTRY_METADATA_SIZE: u64 = 17;
    ENTRY_METADATA_SIZE + e.data().len() as u64
  }

  /// Fill `peer`'s in-flight window: send byte-capped append batches back-to-back until the window
  /// pauses, the peer catches up, or the state forbids sending (etcd's
  /// `for r.maybeSendAppend(...) {}` loop). A single [`Self::maybe_send_append`] sends ONE batch;
  /// without the pump, catch-up replication would move one batch per ack round-trip while the
  /// configured in-flight window (256 messages by default) sat idle — a throughput ceiling of
  /// `max_size_per_msg`/RTT on high-latency links.
  ///
  /// Terminates by construction: each sent batch optimistically advances `next_index`
  /// (`sent_entries`), so every iteration either advances `next_index` or exits. The FIRST
  /// iteration always delegates (no pre-guard): when the peer is "caught up" by `next_index` but
  /// its `match` is stale (acks lost in transit), `maybe_send_append` sends the EMPTY append whose
  /// success ack refreshes `match` and unclamps the heartbeat commit — suppressing that send wedges
  /// a healed follower's commit forever (caught by the VOPR quiesce oracle, seed 3).
  pub(crate) fn pump_appends<L: LogStore, S: StableStore<NodeId = I>>(
    &mut self,
    now: Instant,
    peer: I,
    log: &L,
    stable: &S,
  ) {
    loop {
      let Some(pr) = self.tracker.progress(&peer) else {
        return;
      };
      if pr.is_paused() {
        return;
      }
      let before = pr.next_index();
      self.maybe_send_append(now, peer, log, stable);
      let Some(pr) = self.tracker.progress(&peer) else {
        return;
      };
      if pr.next_index() == before {
        return; // empty append sent / snapshot hand-off / halt — one message, stop
      }
    }
  }

  pub(crate) fn maybe_send_append<L: LogStore, S: StableStore<NodeId = I>>(
    &mut self,
    now: Instant,
    peer: I,
    log: &L,
    stable: &S,
  ) {
    let Some(pr) = self.tracker.progress(&peer).cloned() else {
      return;
    };
    // Respect the in-flight window — if paused, don't send.
    if pr.is_paused() {
      return;
    }

    // If the entries this peer needs have been compacted into a snapshot
    // (next_index strictly below first_index), an AppendEntries cannot carry a valid
    // prev_log_term across the compaction boundary — send the snapshot instead.
    // At next_index == first_index the normal path still works: prev_index == offset
    // whose boundary term is retained.
    if pr.next_index().get() < log.first_index().get() {
      if let Some((meta, data)) = stable.snapshot() {
        let (term, me) = (self.term, self.config.id());
        let pending = meta.last_index();
        self.send(
          peer,
          Message::InstallSnapshot(crate::InstallSnapshot::new(term, me, meta, data)),
        );
        if let Some(p) = self.tracker.progress_mut(&peer) {
          p.become_snapshot(pending);
        }
        // Arm the resend-pacing deadline AT the send: the map entry means "an InstallSnapshot for
        // this peer's current install window went out at deadline − election_timeout". This (a)
        // stops `on_heartbeat_resp` from re-sending the very blob this call just emitted (the
        // heartbeat pump can be what triggers the initial install, in the SAME response handling),
        // and (b) overwrites any stale deadline left over from a previous install window (the peer
        // may have exited Snapshot via `maybe_update` without a heartbeat observation to clean up).
        self
          .snapshot_resend_after
          .insert(peer, now + self.config.election_timeout());
      }
      // No snapshot persisted yet → nothing to send; retry later.
      return;
    }

    let next = pr.next_index();
    let prev_index = Index::new(next.get().saturating_sub(1));
    let prev_term = if prev_index == Index::ZERO {
      Term::ZERO
    } else {
      match self.log_term(log, prev_index) {
        Some(t) => t,
        None => return,
      }
    };
    let end = log.last_index().next();
    // Read the BORROWED suffix slice (no allocation) and apply the byte cap on the slice, cloning
    // ONLY the capped prefix below. A lagging follower must never force the whole suffix to be
    // cloned: the configured `max_size_per_msg` bounds the per-send allocation. `max_bytes` is also
    // passed to the store so an implementation that honours it can return a shorter slice.
    let max_bytes = self.config.max_size_per_msg();
    let slice: &[crate::Entry] = if next < end {
      // A replicable-range read failure is fatal, same policy as `apply_committed`'s LogRead: poison
      // rather than silently shipping an empty AppendEntries that stalls the follower forever.
      match log.entries(next..end, max_bytes) {
        Ok(s) => s,
        Err(_) => {
          self.poison(PoisonReason::LogRead);
          return;
        }
      }
    } else {
      &[]
    };

    // Cap at max_size_per_msg bytes, but always send at least one entry.
    let entries = if slice.is_empty() || max_bytes == u64::MAX {
      slice.to_vec()
    } else {
      let mut budget = max_bytes;
      let mut count = 0usize;
      for e in slice {
        let sz = Self::entry_size(e);
        if count == 0 {
          // always include at least one entry regardless of size
          count += 1;
          budget = budget.saturating_sub(sz);
        } else if sz <= budget {
          count += 1;
          budget -= sz;
        } else {
          break;
        }
      }
      slice[..count].to_vec()
    };

    // Compute the last index and total bytes for sent_entries.
    let last_sent = if entries.is_empty() {
      prev_index
    } else {
      entries.last().unwrap().index()
    };
    let bytes_sent: u64 = entries.iter().map(Self::entry_size).sum();
    let entries_len = entries.len();
    // Whether we sent a partial batch (capped below last_index). In Probe mode we only
    // pause the window when we're holding back entries due to the byte cap — if we sent
    // everything available there is nothing left to throttle and pausing would block the
    // next propose from being pipelined.
    let sent_partial = last_sent < log.last_index();

    let (term, me, commit) = (self.term, self.config.id(), self.commit);
    self.send(
      peer,
      Message::AppendEntries(crate::AppendEntries::new(
        term, me, prev_index, prev_term, entries, commit,
      )),
    );

    // Record the send so the window tracks in-flight messages.
    // For Probe: only pause when we sent a partial batch (byte-capped); a full send leaves
    // nothing to throttle and pausing would stall subsequent proposes unnecessarily.
    // For Replicate: only record non-empty sends — an empty AppendEntries (heartbeat probe
    // for a caught-up peer) must NOT consume an inflight slot. Empty sends carry no entries
    // so there is nothing for the peer to ack; the slot would never be freed, and after
    // max_inflight_msgs heartbeat-resp cycles the window fills and newly proposed entries
    // are silently not delivered. (etcd guards SentEntries on len(entries) > 0.)
    let is_empty = bytes_sent == 0 && entries_len == 0;
    if let Some(p) = self.tracker.progress_mut(&peer) {
      if (!is_empty && p.state().is_replicate()) || sent_partial {
        p.sent_entries(last_sent, bytes_sent);
      }
    }
  }

  pub(crate) fn maybe_advance_commit<L: LogStore>(&mut self, log: &L) {
    // Delegate to the Tracker's joint-quorum committed index. For a simple (non-joint)
    // config this is identical to the old sorted-match logic:
    //   old: matches.sort(); candidate = matches[n - (n/2+1)]
    //   new: MajorityConfig::committed_index does exactly that sort+pick internally.
    // A degenerate Tracker with the static seed (voters = config seed, outgoing empty,
    // no learners) returns the same value.
    let candidate = self.tracker.quorum_committed();
    // §5.4.2: only commit an entry from the CURRENT term by counting replicas.
    let current_term = self
      .log_term(log, candidate)
      .map(|t| t == self.term)
      .unwrap_or(false);
    if candidate > self.commit && current_term {
      self.commit = candidate;
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
  /// Propose a command on the leader. Returns the assigned index, or `NotLeader`.
  /// Takes `cmd` by reference (encoding only borrows; the caller keeps it to retry).
  pub fn propose<L, S>(
    &mut self,
    now: Instant,
    log: &mut L,
    stable: &S,
    cmd: &F::Command,
  ) -> Result<Index, crate::ProposeError<I>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    if self.poisoned {
      return Err(crate::ProposeError::Poisoned);
    }
    if !self.role.is_leader() {
      return Err(crate::ProposeError::NotLeader {
        leader: self.leader,
      });
    }
    // A leader transfer is in progress: stop accepting new entries so the target can
    // catch up to a fixed last_index and receive TimeoutNow.
    if self.lead_transferee.is_some() {
      return Err(crate::ProposeError::LeaderTransferInProgress);
    }
    // Allocate a fresh, usable log index (see `next_log_index`): refuse rather than alias-and-truncate
    // at the saturated ceiling or allocate the unreadable sentinel `u64::MAX`.
    let Some(index) = Self::next_log_index(log.last_index()) else {
      return Err(crate::ProposeError::LogIndexExhausted);
    };
    use crate::Data as _;
    let mut buf = std::vec::Vec::new();
    cmd.encode(&mut buf);
    let entry = crate::Entry::new(
      self.term,
      index,
      crate::EntryKind::Normal,
      bytes::Bytes::from(buf),
    );
    // Self-match advance is deferred until the append is durable (on_log_appended).
    let opid = self.mint_op_id();
    self.submit_append(log, opid, core::slice::from_ref(&entry));
    self
      .pending
      .insert(opid, Pending::LeaderAppend { upto: index });
    for peer in self.peers().collect::<std::vec::Vec<_>>() {
      self.maybe_send_append(now, peer, log, stable);
    }
    Ok(index)
  }

  pub(crate) fn on_heartbeat<L: LogStore>(
    &mut self,
    now: Instant,
    log: &mut L,
    hb: crate::Heartbeat<I>,
  ) {
    self.role = Role::Follower;
    self.set_leader(Some(hb.leader()));
    self.arm_election_timer(now);
    // Advance commit from heartbeat and apply any newly committed entries.
    let new_commit = core::cmp::min(hb.commit(), log.last_index());
    if new_commit > self.commit {
      self.commit = new_commit;
    }
    // Always attempt to apply: a follower's `applied` can lag `commit` even when commit does NOT
    // advance this round — e.g. a previously-committed entry was not yet in the durable read view
    // (the benign empty-read break in `apply_committed`) when commit last advanced. If we only
    // applied on a commit advance, an idle (commit-stable) follower would stay wedged with
    // `applied < commit` forever. Applying whenever `applied < commit` is idempotent (a no-op when
    // already caught up) and closes that wedge.
    if self.applied < self.commit {
      self.apply_committed(log);
    }
    let (term, me) = (self.term, self.config.id());
    // Echo the heartbeat's context back to the leader (lets the leader count this follower's ack
    // toward a pending safe read; empty context is a normal heartbeat) AND echo the lease round so the
    // leader can confirm this is a FRESH response to its current CheckQuorum round.
    let ctx = Bytes::copy_from_slice(hb.context());
    // self-validating lease: advertise how long THIS follower will uphold the leader's read-lease
    // window. We will refuse to help elect a new leader for one election_timeout (we just re-armed our
    // election timer above, and we enforce `in_lease` + the post-restart vote fence) IFF we actually run
    // that enforcement — i.e. `check_quorum || pre_vote`. A non-enforcing follower advertises ZERO so the
    // leader does not count it toward the lease quorum (closes the heterogeneous-cooperation hole);
    // sending our OWN election_timeout (not the leader's) lets the leader bound the lease by the quorum's
    // real support even under heterogeneous timeouts.
    // persist-before-ADVERTISE: a lease-support advertisement is a PROMISE to uphold the leader's
    // lease for one election_timeout that this node must keep even across a crash (the post-restart vote
    // fence). So we advertise our real `election_timeout` ONLY once that promise is DURABLE — i.e. the
    // durable lease-support floor covers it. We bump the in-memory floor here (the advertise site); the
    // post-dispatch `ensure_term_durable` persists it, and `on_stable_wrote` then advances
    // `durable_lease_support`. Until durable, advertise ZERO: the leader counts ZERO (does not float a
    // lease on a promise a crash could erase), so the read silently degrades to Safe. This is the lease
    // sibling of the term-before-respond ack gating.
    let lease_support = if self.config.check_quorum() || self.config.pre_vote() {
      let this_run = self.config.election_timeout();
      if self.lease_support_floor < Some(this_run) {
        self.lease_support_floor = Some(this_run);
      }
      if self.durable_lease_support >= Some(this_run) {
        this_run
      } else {
        core::time::Duration::ZERO
      }
    } else {
      core::time::Duration::ZERO
    };
    self.send(
      hb.leader(),
      Message::HeartbeatResp(
        crate::HeartbeatResp::new(term, me, ctx)
          .with_lease_round(hb.lease_round())
          .with_lease_support(lease_support),
      ),
    );
  }

  pub(crate) fn on_append_entries<L: LogStore, S: StableStore<NodeId = I>>(
    &mut self,
    now: Instant,
    log: &mut L,
    stable: &mut S,
    ae: crate::AppendEntries<I>,
  ) {
    self.role = Role::Follower;
    self.set_leader(Some(ae.leader()));
    self.arm_election_timer(now);

    // Log-consistency check at prev_log_index/term. A fatal term-read poisons via `log_term` and
    // produces `None` → not consistent → reject path; the poisoned node's later dispatch no-ops.
    let consistent = ae.prev_log_index() == Index::ZERO
      || (ae.prev_log_index() <= log.last_index()
        && self
          .log_term(log, ae.prev_log_index())
          .map(|t| t == ae.prev_log_term())
          .unwrap_or(false));
    // A fatal term-read inside the consistency check poisoned us; stop before emitting any reply —
    // a poisoned node must not send (the later dispatch no-ops, but this in-flight handler must too).
    if self.poisoned {
      return;
    }

    let (term, me) = (self.term, self.config.id());
    if !consistent {
      // etcd's two-sided reject hint — uniform for both the
      // term-mismatch and the simply-behind case. This makes the hint O(terms) rather
      // than O(entries): start from min(prev_log_index, last_index) on the FOLLOWER's log
      // and walk down while the term exceeds the leader's prev_log_term. The resulting
      // hint_term is meaningful even when the follower is merely behind, so the leader's
      // find_conflict_by_term lands in one round-trip instead of walking to index 0 and
      // falling back to a one-step decrement. (etcd's uniform findConflictByTerm path.)
      let last_index = log.last_index();
      let hint_index_raw = core::cmp::min(ae.prev_log_index(), last_index);
      // A fatal term-read inside the conflict walk poisons; short-circuit before sending — a
      // poisoned node must not emit a reject hint computed from a fabricated index.
      let Some(hint_index) = self.find_conflict_by_term(log, hint_index_raw, ae.prev_log_term())
      else {
        return;
      };
      let hint_term = match self.log_term(log, hint_index) {
        Some(t) => t,
        None => return,
      };
      self.send(
        ae.leader(),
        Message::AppendResp(crate::AppendResp::new(
          term,
          me,
          true,
          hint_index,
          hint_term,
          Index::ZERO,
        )),
      );
      return;
    }

    // Raft §5.3: only delete-and-re-append from the first *conflicting* entry.
    // Entries that already match (same index, same term) are left untouched so that a
    // stale or duplicate AppendEntries never erases already-committed entries.
    let entries = ae.entries();
    // Validate the suffix is positionally contiguous from `prev_log_index` BEFORE trusting any
    // embedded `entry.index()`. A correct leader always sends a contiguous run starting at
    // `prev_log_index + 1`; conflict detection, the §5.3 truncation boundary, and the store append
    // all key off the embedded index, while `last_new` (the commit ceiling and the ack match) is the
    // positional last. If the two disagree — a malformed or version-skewed message with a gap, a
    // duplicate, or an out-of-range index — the follower could commit/ack an index its store never
    // holds at that position. Deriving `last_new` from the validated running index (checked, so a
    // near-`u64::MAX` prev cannot wrap) makes positional == embedded BY CONSTRUCTION; on any
    // mismatch a correct peer could never produce, poison and abort rather than desync the log from
    // the acked match (the same fatal-corruption class as `CommittedTruncation`).
    let mut last_new = ae.prev_log_index();
    for entry in entries {
      // Derive the next position via the SAME allocation choke-point the leader uses, so the follower
      // REJECTS an imported entry at the reserved sentinel index u64::MAX (or a near-MAX wrap): a
      // correct leader never allocates it, and an entry committed there would be unreadable by
      // the half-open apply/replication ranges — committed but never applied. Same
      // fatal-corruption class as a gap, a duplicate, or an out-of-range index.
      let Some(expected) = Self::next_log_index(last_new) else {
        self.poison(PoisonReason::NonContiguousAppend);
        return;
      };
      if entry.index() != expected {
        self.poison(PoisonReason::NonContiguousAppend);
        return;
      }
      last_new = expected;
    }
    let mut appended_opid: Option<crate::OpId> = None;
    if !entries.is_empty() {
      let mut conflict_at: Option<usize> = None;
      for (i, entry) in entries.iter().enumerate() {
        let idx = entry.index();
        let matches_existing = if idx <= log.last_index() {
          match self.log_term(log, idx) {
            Some(t) => t == entry.term(),
            // Fatal term-read: poisoned; abort rather than mis-classify as a conflict.
            None => return,
          }
        } else {
          false
        };
        if !matches_existing {
          conflict_at = Some(i);
          break;
        }
      }
      if let Some(i) = conflict_at {
        // A conflict at or below our commit would rewrite a committed entry — impossible in correct
        // Raft. Treat it as fatal corruption: poison and abort rather than truncate durable state.
        if entries[i].index() <= self.commit {
          self.poison(PoisonReason::CommittedTruncation);
          return;
        }
        // All read-only consistency/contiguity/truncation checks have passed; the durable phase begins
        // here. Persist the (possibly just-adopted) term BEFORE appending its entries — term-before-
        // entries (see `ensure_term_durable`), preserving the submission order the old eager step-down
        // write had. Placed AFTER validation, so a malformed append fail-stops with no term write.
        // Idempotent for a same-term append (the term is already durable).
        self.ensure_term_durable(stable);
        // §5.3 truncation invalidates any success-ack — already QUEUED in `outgoing` (the immediate
        // pure-duplicate ack) or still PENDING as a deferred FollowerAck — whose match index lies in
        // the range being overwritten. Those entries are gone, so reporting them is an OVER-ACK: it
        // advances the leader's match for this peer past what the peer durably holds and can drive a
        // commit the peer cannot back. This arises in the async fsync window when a follower acks a
        // suffix and a conflicting AppendEntries (e.g. a reordered/duplicate one) truncates it before
        // the ack leaves the outgoing queue. The new suffix's own ack is registered below.
        let truncate_from = entries[i].index();
        // boundary = truncate_from - 1, so `> boundary` is exactly `>= truncate_from`: scrub every
        // queued success ack / pending FollowerAck whose match index lies in the overwritten range.
        self.scrub_acks_above(Index::new(truncate_from.get() - 1));
        // The truncated tail is no longer durable; regress the watermark below it (truncate_from >= 1).
        self.durable_index = self.durable_index.min(Index::new(truncate_from.get() - 1));
        // Drop in-flight append records the truncation supersedes: those entries are overwritten,
        // so their (possibly still-pending) completions must NOT re-advance `durable_index` into
        // the truncated range. The new suffix's own record is added by `submit_append` below.
        self
          .inflight_append_upto
          .retain(|_, upto| *upto < truncate_from);
        let opid = self.mint_op_id();
        self.submit_append(log, opid, &entries[i..]);
        appended_opid = Some(opid);
        // Apply-time membership (etcd, spec §9): a follower does NOT fold appended ConfChanges into
        // its tracker. The configuration changes only when those entries commit-and-apply
        // (apply_committed), so the tracker is never ahead of the committed log — no truncation
        // revert is needed, and `conf_state()` always means the committed voter set.
      }
      // else: every entry already present (pure duplicate) — append nothing.
    }

    // Commit advance and apply proceed independently of the local ack (committed entries
    // are durable on a quorum elsewhere; on restart the SM is rebuilt from durable log).
    let new_commit = core::cmp::min(ae.leader_commit(), last_new);
    if new_commit > self.commit {
      self.commit = new_commit;
    }
    // Always attempt to apply when `applied < commit` (not only on a commit advance): apply can lag
    // commit via the benign empty-read break in `apply_committed` (the committed entry was not yet
    // in the durable read view when commit advanced), and an idle follower would otherwise stay
    // wedged. Idempotent when already caught up.
    if self.applied < self.commit {
      self.apply_committed(log);
    }

    if let Some(opid) = appended_opid {
      // A new suffix was submitted — defer the ack until the append is durable.
      self.pending.insert(
        opid,
        Pending::FollowerAck {
          to: ae.leader(),
          match_index: last_new,
        },
      );
    } else {
      // Nothing was appended (heartbeat or pure duplicate) — ack immediately, but clamp the reported
      // match to `ack_watermark()` (persist-before-ack on the immediate path). In steady state
      // `last_new <= durable_index`, so the clamp is a no-op for genuine heartbeats and already-durable
      // duplicates. The hazards it closes: (a) a duplicate AppendEntries for entries present only in our
      // visible-but-unflushed (in-flight) tail would otherwise ack them as durable; (b) during a pending
      // snapshot install the watermark caps at the snapshot boundary, since the re-based log above it has
      // no durable baseline yet. Either over-ack lets the leader count a phantom replica and commit an
      // entry a crash loses. When the tail/blob flushes, the deferred FollowerAck or next heartbeat
      // reports the higher match.
      // `last_new` is the extent this (empty/duplicate) RPC proved; `send_or_gate_append_ack` applies the
      // persist-before-ack clamp `last_new.min(ack_watermark())` itself (so an in-flight tail/blob and a
      // durable-but-divergent tail are both respected). Persist-before-RESPOND: defer if
      // `self.term` (possibly just adopted from a higher-term heartbeat) is not yet durable.
      let leader = ae.leader();
      self.send_or_gate_append_ack(leader, last_new);
    }
  }

  /// Handle a `HeartbeatResp` from a peer.
  ///
  /// A HeartbeatResp from a peer:
  /// 1. Clears the peer's probe pause (so stalled replication resumes).
  /// 2. Frees one in-flight slot on a full Replicate window (etcd FreeFirstOne).
  /// 3. If the response carries a non-empty context, records the ack for the
  ///    corresponding pending read-index request and confirms any reads that have
  ///    reached a voter quorum.
  pub(crate) fn on_heartbeat_resp<L: LogStore, S: StableStore<NodeId = I>>(
    &mut self,
    now: Instant,
    from: I,
    log: &L,
    stable: &S,
    resp: crate::HeartbeatResp<I>,
  ) {
    if !self.role.is_leader() {
      return;
    }
    // Renew the LeaseBased read lease ONLY from a FRESH response to the CURRENT CheckQuorum round.
    // A HeartbeatResp echoing `self.lease_round` proves `from` was reachable for THIS round — not via a
    // stale or duplicated earlier-round message (which carries a different round and is ignored here).
    // Bound the renewed lease by the round's SEND instant (`lease_round_start`), NOT this
    // response's receipt time: followers reset their election timers when they RECEIVED this round
    // (≈ its send instant), so the lease must expire by `lease_round_start + election_timeout`.
    // Measuring from a (possibly delayed) response would extend the lease past the quorum's election
    // window, letting an isolated leader serve a stale read. Computing from `lease_round_start` is
    // idempotent per round, so a duplicate current-round response cannot extend the same round's
    // deadline. The separate `recent_active`/`election_deadline` CheckQuorum step-down signal is
    // unchanged.
    // self-validating lease: count this ack toward the lease quorum ONLY if the follower advertises
    // that it ENFORCES the lease window (`lease_support > 0` — it runs `in_lease` + the post-restart vote
    // fence). A non-enforcing voter cannot keep the lease alive, so a heterogeneous/misconfigured cluster
    // simply fails to form a lease and `do_leader_read` degrades to Safe (closes the cooperation hole).
    // The lease deadline is bounded by the MIN support across the contributing quorum (`lease_min_support`,
    // min'd here, seeded to the leader's own election_timeout each round), so a voter with a SHORTER
    // election_timeout caps the lease at its real election window — the leader never out-lives a supporter.
    if resp.lease_round() == self.lease_round && resp.lease_support() > core::time::Duration::ZERO {
      self.lease_acks.insert(from);
      self.lease_min_support = self.lease_min_support.min(resp.lease_support());
      let me = self.config.id();
      let fresh: BTreeMap<I, bool> = self
        .tracker
        .ids()
        .into_iter()
        .filter(|id| self.tracker.is_voter(id))
        .map(|id| (id, id == me || self.lease_acks.contains(&id)))
        .collect();
      if self.tracker.vote_result(&fresh).is_won() {
        // Re-set every contributing ack: `lease_min_support` only shrinks within a round, so this never
        // EXTENDS the lease past a supporter's window (a later, shorter-support ack lowers it).
        self.lease_valid_until = Some(self.lease_round_start + self.lease_min_support);
      }
    }
    if let Some(pr) = self.tracker.progress_mut(&from) {
      pr.clear_probe_pause();
      // etcd FreeFirstOne: free one inflight slot so a Replicate peer whose in-flight window
      // was lost (e.g. a healed partition, dropped MsgApps) can resume on the next heartbeat
      // round instead of wedging until an unrelated proposal triggers a send.
      pr.free_inflight_on_heartbeat();
    }
    self.pump_appends(now, from, log, stable);

    // Liveness fix: if this peer is still in Snapshot state and has NOT yet
    // caught up to its pending snapshot index, RE-SEND the snapshot. The single
    // `InstallSnapshot` emitted by maybe_send_append's compacted-hole branch may have been
    // dropped; a Snapshot-state peer is unconditionally paused so the pump above sends it
    // nothing, and it only leaves Snapshot state once the snapshot is delivered and acked
    // (maybe_update). Without this resend a dropped InstallSnapshot wedges the follower forever.
    //
    // BACKOFF: a deferred install legitimately spans many heartbeat intervals (the follower
    // fsyncs the blob before acking), and ReadIndex Safe rounds elicit extra responses — so an
    // unconditional per-response resend would re-transmit the full blob dozens of times per
    // install. The per-peer countdown spaces resends roughly one election timeout apart: a
    // genuinely dropped blob is still retried within one election timeout (liveness preserved),
    // without the per-round egress amplification. (Read state/pending/match via an immutable
    // borrow into locals, drop the borrow, then act — mirrors on_append_resp's re-borrow.)
    let resend = match self.tracker.progress(&from) {
      Some(pr) => match pr.state() {
        crate::ProgressState::Snapshot(pending) => pr.match_index() < pending,
        _ => false,
      },
      None => false,
    };
    if resend {
      // TIME-based pacing (response COUNT is the wrong clock: ReadIndex Safe rounds elicit extra
      // responses, which would accelerate a count-based pacer arbitrarily): at most one blob per
      // election timeout. The deadline is armed at every InstallSnapshot SEND — by
      // `maybe_send_append`'s compacted-hole branch when the install window opens (possibly via
      // the pump a few lines up, in THIS response handling) and re-armed here on each resend — so
      // "due" always means "a full election timeout has passed since the blob last went out".
      // A genuinely dropped blob is therefore retried within one election timeout of its send
      // (liveness preserved). The `is_none_or` arm is a backstop for a Snapshot-state peer with no
      // armed deadline, which no current path produces.
      let due = self
        .snapshot_resend_after
        .get(&from)
        .is_none_or(|&after| now >= after);
      if due {
        self
          .snapshot_resend_after
          .insert(from, now + self.config.election_timeout());
        self.resend_snapshot(from, stable);
      }
    } else {
      // Observed out of Snapshot state: drop the pacing entry. (A peer that exits via
      // `maybe_update` keeps its entry until this observation — harmless, since the resend is
      // gated on Snapshot state above, and a NEW install window re-arms the deadline at send.)
      self.snapshot_resend_after.remove(&from);
    }

    // ReadIndex Safe path: if the resp carries a context, record the ack and check quorum.
    let ctx = resp.context();
    if ctx.is_empty() {
      return;
    }
    let ctx_bytes = Bytes::copy_from_slice(ctx);
    self.read_only.recv_ack(from, ctx);
    // Quorum check: the ack set (including the self-ack seeded at add_request) must
    // form a voter quorum across the joint config.  Reuse vote_result machinery:
    // treat each voter as "granted" iff its id is in the ack set.
    let quorum_reached = {
      let acks = self
        .read_only
        .acks_for(ctx_bytes.as_ref())
        .cloned()
        .unwrap_or_default();
      // vote_result(|id| Some(acks.contains(id))).is_won() covers both joint halves.
      let votes: BTreeMap<I, bool> = self
        .tracker
        .ids()
        .into_iter()
        .filter(|id| self.tracker.is_voter(id))
        .map(|id| (id, acks.contains(&id)))
        .collect();
      self.tracker.vote_result(&votes).is_won()
    };
    if quorum_reached {
      let confirmed = self.read_only.advance(ctx_bytes.as_ref());
      let (term, me) = (self.term, self.config.id());
      for st in confirmed {
        let (context, req_from, index) = st.into_parts();
        match req_from {
          None => {
            // Local leader read — emit ReadState event.
            self.emit_read_state(index, context);
          }
          Some(follower) => {
            // Forwarded read — reply ReadIndexResp to the originating follower.
            self.send(
              follower,
              Message::ReadIndexResp(crate::ReadIndexResp::new(term, me, index, context, false)),
            );
          }
        }
      }
    }
  }

  /// Walk the leader's log downward from `index` until we find an entry whose term is
  /// `<= term` (or we hit the beginning). This mirrors etcd's `findConflictByTerm` and
  /// lets the leader skip a whole divergent term in one round-trip on reject.
  ///
  /// Returns `None` if a fatal term-read poisoned the node mid-walk: the hint index it would
  /// otherwise return is fabricated (the search never completed), so callers must short-circuit
  /// rather than mutate peer progress or send on it. A normal exit returns `Some(index)`.
  pub(crate) fn find_conflict_by_term<L: LogStore>(
    &mut self,
    log: &L,
    mut index: Index,
    term: Term,
  ) -> Option<Index> {
    while index > Index::ZERO {
      // A fatal term-read poisoned the node (inside `log_term`): propagate `None` so the caller
      // short-circuits rather than acting on a fabricated index the incomplete search would return.
      let t = self.log_term(log, index)?;
      if t <= term {
        break;
      }
      index = Index::new(index.get() - 1);
    }
    Some(index)
  }

  /// The boundary check on a peer's reported `match_index` from a SUCCESSFUL response: it must not
  /// exceed the leader's own `log.last_index()`. The leader only ever sent entries it holds, so no
  /// honest peer can durably hold more; a higher value is malformed or version-skewed input. Both
  /// `on_append_resp` and `on_snapshot_resp` gate their success path on this so the invariant lives
  /// in ONE place. Accepting an over-run would (a) corrupt the peer's `Progress` (`maybe_update`
  /// trusts the value verbatim, never lowering it again) and (b) let `maybe_advance_commit`'s quorum
  /// candidate run past the log, where `log_term` reads an out-of-range index and POISONS the leader
  /// on impossible input — turning one malformed follower ack into a leader-wide halt. An associated
  /// fn (no `self`) so callers can check it while a `Progress` borrow is live.
  pub(crate) fn match_within_log(match_index: Index, log: &impl LogStore) -> bool {
    match_index <= log.last_index()
  }

  pub(crate) fn on_append_resp<L: LogStore, S: StableStore<NodeId = I>>(
    &mut self,
    now: Instant,
    log: &mut L,
    stable: &S,
    from: I,
    resp: crate::AppendResp<I>,
  ) {
    if !self.role.is_leader() {
      return;
    }
    let Some(pr) = self.tracker.progress_mut(&from) else {
      return;
    };
    if resp.reject() {
      // Use the term-skip hint to jump next_index forward in one step.
      // find_conflict_by_term walks the leader's log from reject_hint_index downward
      // until we find an entry whose term ≤ reject_hint_term (the follower's conflicting
      // term). This lets the leader skip a whole conflicting term in O(terms) round-trips.
      // Clamp the PEER-SUPPLIED hint to the leader's own log before the term-skip walk. An honest
      // follower's hint is meaningful only within the leader's log; an out-of-range value (malformed,
      // version-skewed, or a follower whose divergent tail is longer than ours) would otherwise make
      // `find_conflict_by_term` read a non-existent index — poisoning the leader via `log_term` — or,
      // at `u64::MAX`, overflow the `conflict + 1` jump below. Mirrors the follower-side hint clamp in
      // `on_append_entries` (`min(prev_log_index, last_index)`).
      let hint_index = core::cmp::min(resp.reject_hint_index(), log.last_index());
      let hint_term = resp.reject_hint_term();
      let cur_next = pr.next_index();
      // Compute the conflict index before re-borrowing self.tracker.progress mutably. A fatal
      // term-read mid-walk poisons and returns `None`; short-circuit before mutating peer progress
      // or sending — a poisoned node must neither advance `next_index` nor emit an AppendEntries.
      let Some(conflict) = self.find_conflict_by_term(log, hint_index, hint_term) else {
        return;
      };
      // etcd `Progress.MaybeDecrTo`: jump next_index to `min(rejected_prev, conflict+1)`, floored at
      // 1 — NOT a one-index decrement. The jump makes catch-up of a deeply-divergent follower O(terms)
      // round-trips instead of O(entries): a `(0,0)` hint (the follower's WHOLE log conflicts, so
      // `find_conflict_by_term` bottomed out at 0) jumps straight to index 1 in a single step rather
      // than walking down one index per reject. The one-index decrement is recovered automatically
      // for a stale/unhelpful hint (`conflict >= cur_next` ⇒ `conflict+1 > rejected_prev` ⇒ the `min`
      // picks `rejected_prev = cur_next-1`). (The O(entries) walk was pathologically slow —
      // thousands of reject round-trips compressed into each instant-delivery tick.)
      let rejected_prev = cur_next.get().saturating_sub(1);
      let safe_next = Index::new(core::cmp::max(
        core::cmp::min(rejected_prev, conflict.get().saturating_add(1)),
        1,
      ));
      // Re-acquire progress to update (prior `pr` reference dropped implicitly by this point).
      if let Some(p) = self.tracker.progress_mut(&from) {
        p.become_probe();
        p.set_next_index(safe_next);
      }
      self.maybe_send_append(now, from, log, stable);
    } else {
      // Boundary check (shared with `on_snapshot_resp` via `match_within_log`): a successful ack must
      // not report a match above the leader's own log. An over-run is malformed/version-skewed input —
      // ignore the whole ack rather than corrupt this peer's `Progress` or let the commit candidate
      // run off the log and poison the leader.
      if !Self::match_within_log(resp.match_index(), log) {
        return;
      }
      // Capture the state BEFORE maybe_update so we can guard the Probe -> Replicate
      // transition. etcd's MsgAppResp handler only switches Probe -> Replicate
      // on the first successful ack.
      let state_before = pr.state();
      if pr.maybe_update(resp.match_index()) {
        // etcd 3-way switch: only transition Probe -> Replicate here. For a peer ALREADY in
        // Replicate, maybe_update already advanced match/next and freed the acked inflight
        // slot via free_le; calling become_replicate() again would rewind next_index to
        // match.next() and reset the whole inflight window, defeating the flow control and
        // re-sending the in-flight tail on every ack. For Snapshot, maybe_update already
        // performed the Snapshot -> Probe transition when the peer caught up past pending, so
        // there is nothing to do here either.
        match state_before {
          crate::ProgressState::Probe => {
            // Re-acquire progress (prior `pr` borrow ended at maybe_update above), mirroring
            // the reject-branch re-borrow idiom.
            if let Some(p) = self.tracker.progress_mut(&from) {
              p.become_replicate();
            }
          }
          crate::ProgressState::Replicate | crate::ProgressState::Snapshot(_) => {}
        }
        self.maybe_advance_commit(log);
        self.apply_committed(log);
        self.maybe_flush_deferred_reads(now, log, stable);
        self.pump_appends(now, from, log, stable); // fill the peer's inflight window if still behind
        // Leader transfer: if this peer just caught up to last_index, send TimeoutNow.
        if self.lead_transferee == Some(from) {
          let peer_match = self
            .tracker
            .progress(&from)
            .map(|p| p.match_index())
            .unwrap_or(crate::Index::ZERO);
          if peer_match == log.last_index() {
            let (term, me) = (self.term, self.config.id());
            self.send(from, Message::TimeoutNow(crate::TimeoutNow::new(term, me)));
            // a forced campaign is now authorized for this term — disable LeaseBased reads for the
            // rest of it (the forced campaign can elect a new leader at any later point, even post-abort).
            self.forced_handoff_this_term = true;
          }
        }
      }
    }
  }
}
