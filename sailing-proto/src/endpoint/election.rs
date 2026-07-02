use super::*;
use crate::{RequestVote, VoteResponse};
use core::error::Error;

impl<I, F, R> Endpoint<I, F, R>
where
  I: NodeId,
  F: StateMachine,
  R: rand::Rng,
{
  pub(crate) fn arm_election_timer(&mut self, now: Now) {
    let t = crate::prng::election_timeout(&mut self.rng, self.config.election_timeout());
    self.election_deadline = Some(now.mono() + t);
    self.heartbeat_deadline = None;
  }

  /// Re-establish the election-timer INVARIANT, by construction, at every public-entry boundary:
  ///
  /// > a node that is a VOTER and is NOT the leader must hold an armed `election_deadline`.
  ///
  /// Otherwise it can never campaign, and a cluster whose voters are ALL in that state wedges
  /// leaderless forever. The hazard arises because the Sans-I/O design DISARMS a non-voter's
  /// deadline (so the event-driven sim clock can advance past a node that must not campaign) — and
  /// several transitions can leave a node a voter without a timer: adopting a higher term and
  /// stepping down on a RESPONSE message (no handler re-arm), and a learner→voter promotion applied
  /// with no current leader to heartbeat it. Rather than remember to arm at each such site (a
  /// fragility that already caused two distinct livelock bugs), we enforce the
  /// invariant centrally here, after the entry point has finished mutating role/term/membership.
  ///
  /// This is a SAFETY NET, not a reset: it arms ONLY when the deadline is currently absent, so it
  /// never postpones an already-running timer (resetting a live timer on every higher-term adoption
  /// regressed liveness under an adversarial schedule). The legitimate resets — leader contact (heartbeat/append/snapshot),
  /// granting a vote, starting a campaign, a CheckQuorum step-down — remain explicit at their own
  /// sites and set a fresh deadline; this no-ops for them. Leaders are skipped (a leader owns its
  /// heartbeat timer, and with CheckQuorum it repurposes `election_deadline` for the quorum check);
  /// non-voters are skipped (they must not campaign).
  ///
  /// Mirrors the guarantee etcd gets for free from its always-incrementing `electionElapsed` counter
  /// (every node ticks, so a voter always eventually campaigns); we reconstruct it for the
  /// deadline-based model without giving up the event-driven clock skip for non-voters.
  pub(crate) fn reconcile_election_timer(&mut self, now: Now) {
    if !self.role.is_leader()
      && self.election_deadline.is_none()
      && self.tracker.is_voter(&self.config.id())
    {
      self.arm_election_timer(now);
    }
  }

  /// Step down to Follower at the SAME term (no term bump): used by CheckQuorum when the
  /// leader can no longer reach a quorum. (The self-removal step-down is separate and
  /// inlined in `apply_committed` — it disarms the election timer because a removed
  /// non-voter must never campaign, the opposite of this helper.)
  ///
  /// Sets `role = Follower`, clears `leader` and `heartbeat_deadline`, and arms the election
  /// timer so the node will eventually campaign again (with PreVote, non-disruptively).
  pub(crate) fn step_down_to_follower(&mut self, now: Now) {
    self.role = Role::Follower;
    self.set_leader(None);
    self.heartbeat_deadline = None;
    // Drop all pending reads — a stepped-down node is no longer the leader and
    // cannot confirm any outstanding read requests.
    self.reads.read_only.reset(self.reads.active_read_mode);
    // A stepped-down node no longer serves LeaseGuard reads, so drop any pending lease-refresh demand
    // (only a leader appends the refresh no-op; a re-election re-stamps the lease via its own no-op).
    self.lease_guard.lease_refresh_wanted = false;
    // ...and the proactive-refresh read-activity signal (a re-election starts its own anchor afresh).
    self.lease_guard.read_since_anchor = false;
    self.reads.pending_reads.clear();
    // Abort any in-progress leader transfer — leadership is changing, the transfer is moot.
    self.transfer.lead_transferee = None;
    self.transfer.transfer_deadline = None;
    // The partitioned former leader arms the election timer; once it heals and
    // pre-vote/real vote succeeds it can campaign again without disrupting the cluster.
    self.arm_election_timer(now);
  }

  /// Whether this candidate's self-vote (the term+vote hard-state write from `become_candidate`) is
  /// already durable — i.e. no `Campaign` completion for the current term is still pending.
  /// `become_leader` must never fire on a quorum that counts an un-durable self-vote: a crash before
  /// the write lands would restart with no recorded vote and could grant a different candidate the
  /// same term.
  pub(crate) fn self_vote_durable(&self) -> bool {
    !self
      .pending_stable
      .iter()
      .any(|(_, p)| matches!(p, Pending::Campaign { term } if *term == self.term))
  }

  /// Whether the post-restart vote-suppression fence currently blocks GRANTING a vote (LeaseBased
  /// crash-safety). `None`/expired fence ⇒ not fenced. A forced leader-transfer bypasses it: the current
  /// leader is voluntarily handing off (relinquishing its lease), so granting cannot strand a live lease
  /// — mirrors the `in_lease` bypass. Only ever armed under `ReadOnlyOption::LeaseBased` (see `restart`).
  #[inline]
  pub(crate) fn lease_vote_fenced(&self, now: Now, force: bool) -> bool {
    !force
      && self
        .durable
        .lease_vote_fence_until
        .is_some_and(|d| now.mono() < d)
  }

  pub(crate) fn on_request_vote<L: LogStore, S: StableStore<NodeId = I>>(
    &mut self,
    now: Now,
    log: &mut L,
    stable: &mut S,
    rv: RequestVote<I>,
  ) {
    // INTENTIONAL (do NOT add a `tracker.is_voter(rv.candidate())` gate here): vote granting is
    // membership-AGNOSTIC, matching etcd's `Step`. Election safety comes from one-vote-per-term
    // (`voted_for`) + log-up-to-date (`log_ok`) + quorum overlap across configurations — a
    // candidate's membership is not part of that proof. A removed node that has not yet applied its
    // own removal can briefly campaign, but if it wins it necessarily holds every committed entry
    // (it won on log freshness), cannot share a term with another leader (one-vote-per-term), and
    // steps down the instant it applies its removal — no committed entry is lost. That disruption is
    // already bounded by PreVote + the lease check (`in_lease`) + the promotable-campaign guard
    // (`become_candidate`/`become_pre_candidate` require `is_voter(self)`), the same mitigations
    // etcd uses. Gating on `tracker.is_voter` would instead couple vote-granting to APPLY-TIME
    // membership (which lags): a freshly-added voter, in the window after its addition commits but
    // before a peer applies it, would be wrongly rejected — breaking legitimate config-change
    // elections. Membership-agnostic is the correct, golden choice.
    let Some((mut my_index, mut my_term)) = self.last_log(log) else {
      // Storage error reading our own last-log term: we cannot safely compare freshness, so poison
      // rather than fabricate `Term::ZERO` and risk granting a vote to a staler candidate.
      self.poison(PoisonReason::LogTerm);
      return;
    };
    // vote-freshness floor: while a snapshot install is DEFERRED (`pending_install`), our durable
    // read-view is still the OLD short log, but the snapshot boundary is already quorum-committed (a
    // leader only snapshots committed state). Floor our advertised freshness at the boundary so we never
    // grant a vote to a candidate whose log is BEHIND that committed prefix — vanilla deferral would
    // otherwise report stale-LOW freshness and could help elect a leader missing committed entries
    // (a Leader-Completeness violation, strictly worse than the orphan the deferral closes). One local floor on
    // the comparison pair only; sound because the boundary is committed, so it never understates our
    // true committed freshness.
    if let Some((_, meta, ..)) = &self.snapshot.pending_install
      && (meta.last_term(), meta.last_index()) > (my_term, my_index)
    {
      my_index = meta.last_index();
      my_term = meta.last_term();
    }
    // The SAME floor for an in-progress CHUNKED receive: chunks accepted into `snapshot_recv` advance our
    // accepted boundary for a committed snapshot LONG before the whole blob completes into `pending_install`.
    // Without this floor, the multi-chunk receive window advertises stale-LOW freshness (the old short log)
    // and could help elect a candidate behind the committed snapshot boundary we have already accepted from
    // the leader — the exact stale-low class the `pending_install` floor closes, reopened by chunking.
    if let Some(r) = &self.snapshot.snapshot_recv
      && (r.meta.last_term(), r.meta.last_index()) > (my_term, my_index)
    {
      my_index = r.meta.last_index();
      my_term = r.meta.last_term();
    }
    let log_ok = (rv.last_log_term(), rv.last_log_index()) >= (my_term, my_index);

    // Pre-vote path: a completely separate branch — NO durable state is changed.
    if rv.pre_vote() {
      // Grant iff ALL of:
      // (a) candidate's log is up-to-date (same §5.4.1 check)
      // (b) advertised term >= our term (etcd: stale-term pre-vote is rejected outright;
      //     the reject reply carries self.term so the pre-candidate learns it is behind).
      //     When rv.term() == self.term, also require we haven't voted for someone else
      //     (etcd canVote); when rv.term() > self.term, the above is trivially satisfied.
      // (c) lease check: we have NOT heard from a current leader within the election timeout
      //     (election timer healthy and we know a leader → refuse; lease is open otherwise)
      let term_ok = rv.term() >= self.term
        && (rv.term() > self.term
          || self.voted_for.is_none()
          || self.voted_for == Some(rv.candidate()));
      let lease_open =
        !(self.leader.is_some() && self.election_deadline.is_some_and(|d| d > now.mono()));
      // (d) post-restart fence: a restarted node under LeaseBased withholds even its PRE-vote during
      //     the fence window, so a lease it may have acked before crashing cannot be undermined by a
      //     fresh election (a forced transfer bypasses — see `lease_vote_fenced`).
      let grant =
        log_ok && term_ok && lease_open && !self.lease_vote_fenced(now, rv.leader_transfer());
      let me = self.config.id();
      // On grant: reply at the advertised term so the pre-candidate counts it for this
      // round; on reject: reply at self.term so the pre-candidate learns our (possibly
      // higher) term. Do NOT touch self.term, self.voted_for, or self.pending.
      let response_term = if grant { rv.term() } else { self.term };
      self.send(
        rv.candidate(),
        Message::VoteResponse(VoteResponse::new(response_term, me, true, !grant)),
      );
      return;
    }

    // Real vote path.
    let can_vote = self.voted_for.is_none() || self.voted_for == Some(rv.candidate());
    // post-restart fence: a restarted node under LeaseBased withholds its real vote during the fence
    // window (a forced leader-transfer bypasses — see `lease_vote_fenced`), so a lease it may have acked
    // before crashing cannot be undermined by electing a new leader inside the old lease window. The
    // higher term is still ADOPTED in `handle_message` (always safe); only the GRANT is withheld.
    if can_vote && log_ok && !self.lease_vote_fenced(now, rv.leader_transfer()) {
      self.voted_for = Some(rv.candidate());
      self.arm_election_timer(now);
      // Persist (term, vote); the VoteResponse(grant) is owed once the write is DURABLE.
      // Stamp the current commit too: we read-modify `hard_state()` then override fields, so
      // without this the write would carry a possibly-stale `hard_state().commit` and could
      // REGRESS the durable commit below a value the handle_storage choke-point already wrote.
      // `self.commit` is monotonic, so stamping it keeps the durable commit monotonic.
      let opid = self.mint_op_id();
      let hs = stable
        .hard_state()
        .with_term(self.term)
        .with_vote(self.voted_for.cheap_clone())
        .with_commit(self.durable_commit());
      self.submit_write(stable, opid, hs);
      self.durable.committed_persisted = self.durable_commit();
      self.push_pending(
        opid,
        Pending::CastVote {
          to: rv.candidate(),
          term: self.term,
        },
      );
    } else {
      // A rejection needs no durability guarantee — send immediately.
      let (term, me) = (self.term, self.config.id());
      self.send(
        rv.candidate(),
        Message::VoteResponse(VoteResponse::new(term, me, false, true)),
      );
    }
  }

  pub(crate) fn on_vote_response<L: LogStore, S: StableStore<NodeId = I>>(
    &mut self,
    now: Now,
    log: &mut L,
    stable: &mut S,
    vr: VoteResponse<I>,
  ) where
    F::Command: Data,
    // `become_candidate`/`become_leader` live in the `apply_committed` impl block, which is
    // gated on this bound (the fatal apply error must be inspectable, design spec §6.3).
    F::Error: Error,
  {
    if vr.pre_vote() {
      // Pre-vote response: only count if we are still a PreCandidate.
      if !self.role.is_pre_candidate() {
        return; // stale: we already advanced or stepped down
      }
      // Record the ballot for the pre-vote round.
      self.votes.insert(vr.from(), !vr.reject());
      if self.tracker.vote_result(&self.votes).is_won() {
        // Pre-vote quorum: NOW start the real campaign (bumps term, persists, broadcasts).
        self.become_candidate(now, log, stable, false);
      }
      // No quorum yet (or lost): stay PreCandidate; election timeout retries.
      return;
    }

    // Real vote path: only count if we are currently a Candidate.
    if !self.role.is_candidate() || vr.term() != self.term {
      return;
    }
    // Record the ballot: true = grant, false = reject.
    // `vr.reject()` is false when the vote was granted.
    self.votes.insert(vr.from(), !vr.reject());
    // Become leader on a quorum ONLY if our own self-vote is already durable; otherwise defer to
    // on_stable_wrote, which re-checks the quorum once the self-vote write completes. Leading on a
    // quorum that includes an un-durable self-vote would break election safety under async storage.
    if self.tracker.vote_result(&self.votes).is_won() && self.self_vote_durable() {
      self.become_leader(now, log, stable);
    }
    // Lost or Pending: stay candidate; the election timeout retries (preserves election liveness).
  }
}
impl<I, F, R> Endpoint<I, F, R>
where
  I: NodeId,
  F: StateMachine,
  R: rand::Rng,
  F::Command: Data,
  F::Error: Error,
{
  /// Start a real election campaign.
  ///
  /// `transfer` must be `true` when called from `on_timeout_now` (leader-transfer path):
  /// it sets `leader_transfer: true` on the broadcast `RequestVote` so that granters bypass
  /// their CheckQuorum/PreVote lease check (the `!force` guard).  For normal elections
  /// (election-timeout path, pre-vote quorum reached) pass `transfer = false`.
  pub(crate) fn become_candidate<L: LogStore, S: StableStore<NodeId = I>>(
    &mut self,
    now: Now,
    log: &mut L,
    stable: &mut S,
    transfer: bool,
  ) {
    // Defensive guard: a non-voter (learner or removed node) must never campaign.
    // The handle_timeout gate is the primary check; this guard closes any other call sites.
    if !self.tracker.is_voter(&self.config.id()) {
      return;
    }
    // Election safety at term exhaustion: `Term::next()` SATURATES at u64::MAX, so a node already at the
    // maximum term cannot advance. Campaigning anyway would clear `voted_for` and record a self-vote in
    // the SAME term — a SECOND vote in a term we may already have voted in — breaking one-vote-per-term
    // (two leaders possible at u64::MAX). A max term is unreachable by legitimate increments (2^64
    // elections) but reachable from a crafted/corrupt max-term message or recovered hard state, so we
    // must not assume `next()` strictly advances. Refuse to campaign rather than violate safety: the
    // node stays a follower at u64::MAX with its existing ballot intact (it can still vote-follow); it
    // simply cannot initiate an election. `voted_for`/`pending` are cleared ONLY after a strict advance.
    let next_term = self.term.next();
    if next_term == self.term {
      return;
    }
    // Read the last-log coordinate FIRST (read-only): the vote request needs it, and a fatal term-read
    // must fail-stop BEFORE any term/vote mutation or the durable self-vote write — so the campaign
    // fail-stop is side-effect-free (no durable self-vote left in a term we never actually campaigned
    // in). Mirrors `on_request_vote`, which reads `last_log` before it grants/persists.
    let Some((last_index, last_term)) = self.last_log(log) else {
      self.poison(PoisonReason::LogTerm);
      return;
    };
    self.term = next_term;
    // All pending work from the previous term is now stale (spec §7). Clear before recording
    // the self-vote below so old completions that arrive later are harmlessly ignored.
    self.pending_log.clear();
    self.pending_stable.clear();
    self.role = Role::Candidate;
    self.set_leader(None);
    self.voted_for = Some(self.config.id());
    // Record self-vote in the ballot map (true = grant).
    self.votes.clear();
    self.votes.insert(self.config.id(), true);
    // Persist (term, self-vote). No Pending entry — a candidate doesn't owe an ack.
    // Stamp the current commit too (see on_request_vote): a read-modify of `hard_state()`
    // must not write back a stale `commit` that regresses the durable watermark.
    let opid = self.mint_op_id();
    let hs = stable
      .hard_state()
      .with_term(self.term)
      .with_vote(self.voted_for.cheap_clone())
      .with_commit(self.durable_commit());
    self.submit_write(stable, opid, hs);
    self.durable.committed_persisted = self.durable_commit();
    // Defer acting on the self-vote until it is DURABLE (persist-before-act, symmetric with the
    // follower `CastVote` path): `become_leader` fires from `on_stable_wrote` (single-node now, or
    // once peer votes arrive) only after this write's `StableDone::Wrote`.
    self.push_pending(opid, Pending::Campaign { term: self.term });
    self.arm_election_timer(now);

    let (term, me) = (self.term, self.config.id());
    // Send RequestVote only to VOTER peers (not learners). Learners don't participate in
    // elections; sending them a RequestVote wastes bandwidth and may confuse their state.
    // Replication still goes to all peers (learners get AppendEntries from become_leader).
    let voter_peers: Vec<_> = self.peers().filter(|p| self.tracker.is_voter(p)).collect();
    for peer in voter_peers {
      self.send(
        peer,
        Message::RequestVote(RequestVote::new(
          term,
          me.cheap_clone(),
          last_index,
          last_term,
          false,
          transfer,
        )),
      );
    }
    // Do NOT become leader here even on a single-node self-vote quorum: the self-vote write above is
    // not yet durable. `on_stable_wrote` fires `become_leader` once `StableDone::Wrote` confirms it.
  }

  /// Begin a pre-vote probe: set `role = PreCandidate`, cast a self pre-vote, and broadcast
  /// `RequestVote{pre_vote:true, term: self.term.next()}` to voter peers WITHOUT bumping
  /// `self.term`, persisting anything, or clearing `voted_for`.
  ///
  /// The advertised term is `self.term.next()` — the term we *would* use in a real campaign.
  /// It is NOT adopted here; only `become_candidate` (reached on a pre-vote quorum) adopts it.
  ///
  /// Returns `true` if the pre-vote quorum is already satisfied (single-node fast path), so
  /// the caller can immediately proceed to `become_candidate`.
  pub(crate) fn become_pre_candidate<L: LogStore>(&mut self, now: Now, log: &L) -> bool {
    // Non-voter guard (mirrors become_candidate for defense-in-depth).
    if !self.tracker.is_voter(&self.config.id()) {
      return false;
    }
    // Term exhaustion (mirrors become_candidate): a pre-vote advertises `self.term.next()`, which
    // SATURATES at u64::MAX. At the max term a successful pre-vote could not lead to a real campaign
    // (`become_candidate` refuses to advance there), so don't probe at all — stay put.
    if self.term.next() == self.term {
      return false;
    }
    self.role = Role::PreCandidate;
    self.set_leader(None);
    // Clear the ballot and record self pre-vote.
    self.votes.clear();
    self.votes.insert(self.config.id(), true);
    // Arm the election timer so a failed pre-vote retries on the next timeout.
    self.arm_election_timer(now);

    let advertised_term = self.term.next(); // proposed, not adopted
    let Some((last_index, last_term)) = self.last_log(log) else {
      self.poison(PoisonReason::LogTerm);
      return false;
    };
    let me = self.config.id();
    let voter_peers: Vec<_> = self.peers().filter(|p| self.tracker.is_voter(p)).collect();
    for peer in voter_peers {
      self.send(
        peer,
        Message::RequestVote(RequestVote::new(
          advertised_term,
          me.cheap_clone(),
          last_index,
          last_term,
          true,  // pre_vote
          false, // leader_transfer
        )),
      );
    }
    // Return whether the pre-vote quorum is already won (single-node cluster fast path:
    // self-vote = quorum). The caller must call become_candidate if this returns true.
    self.tracker.vote_result(&self.votes).is_won()
  }

  /// Append THIS leader's stamped empty (no-op) entry at the next free index after `last`, tracked as a
  /// `LeaderAppend` so its durability advances the leader's own match. The entry carries the LeaseGuard
  /// `timestamp` + `lease_window` stamps (both `0` / proto-omitted outside LeaseGuard). Used by
  /// `become_leader` (to commit prior-term entries, §5.4.2) and by the LeaseGuard lease refresh (to
  /// re-stamp a stale committed lease under a read-only workload). Returns the appended index, or `None`
  /// after poisoning when the log is at the index ceiling (`next_log_index` cannot allocate a fresh,
  /// non-aliased index — a corrupt/terminal node).
  pub(crate) fn append_leader_noop<L: LogStore>(
    &mut self,
    now: Now,
    log: &mut L,
    last: Index,
  ) -> Option<Index> {
    if self.poison.poisoned {
      return None;
    }
    let Some(noop_index) = Self::next_log_index(last) else {
      self.poison(PoisonReason::LogExhausted);
      return None;
    };
    let noop = crate::Entry::new(
      self.term,
      noop_index,
      crate::EntryKind::Empty,
      bytes::Bytes::new(),
    )
    .with_timestamp(self.lease_stamp(now.mono()))
    .with_lease_window(self.lease_window_stamp())
    .with_wall_timestamp(self.lease_wall_stamp(now));
    let opid = self.mint_op_id();
    self.submit_append(log, opid, core::slice::from_ref(&noop));
    self.push_pending(opid, Pending::LeaderAppend { upto: noop_index });
    // Mark the append for the next coalesced `flush_appends`, like every other propose-family mutation.
    // Without this a lease-refresh no-op would replicate only when a heartbeat RESPONSE next triggers a
    // pump (~1 RTT later), eating into the proactive-refresh margin. `become_leader` fans out explicitly
    // regardless, so this is harmless there.
    self.replication_pending = true;
    Some(noop_index)
  }

  /// The committed anchor `log[commit]` at election — its EXACT `(wall_timestamp, lease_window)` =
  /// `(s_c, W_c)` — for the inherited-read SERVE gate ([`failover_read_window`](Self::failover_read_window)).
  /// Stale-HIGH here would serve past a dead lease (UNSAFE), so both are exact-or-fail-closed: `(0, 0)`
  /// when there is no committed entry yet or `log[commit]` has been compacted into the snapshot
  /// (`commit < first_index` — reading it would be out of the live-log domain), in which case the serve
  /// gate refuses and the read degrades to Safe. A genuine log-read fault poisons (the fail-stop
  /// discipline of `lease_guard_read_live`). Both fields come from the SAME entry in one fetch — the
  /// window `W_c` is the entry's OWN self-describing horizon, so the serve dovetails with the release
  /// floor on the shared entry for any per-node config. (Recovering the boundary wall from the snapshot
  /// for the compacted case is a deferred liveness follow-on; fail-closed is always sound.)
  fn committed_anchor_at_election<L: LogStore>(&mut self, log: &L) -> (u64, u64) {
    if self.commit < log.first_index() {
      return (0, 0);
    }
    match log.entries(self.commit..self.commit.next(), u64::MAX) {
      Ok(EntriesRead::Ready(s)) => s
        .first()
        .map(|e| (e.wall_timestamp(), e.lease_window()))
        .unwrap_or((0, 0)),
      // Cold anchor: fail closed (the serve gate refuses, degrades to Safe), same as the absent case.
      Ok(EntriesRead::Pending) => (0, 0),
      Err(_) => {
        self.poison(PoisonReason::LogRead);
        (0, 0)
      }
    }
  }

  /// The E′-INFLATED post-election conservative commit-wait (nanos) ON the failover tier:
  /// `ceil(max_lease_window · (Δ + ε_drift)/Δ) = max_lease_window · (1+ρ)`. The inflation makes the MONO
  /// deadline cover the inherited entry's drift-padded window `W_c` (the LENGTH) in REAL time even at the
  /// fastest admissible clock rate. That alone does NOT bound the absolute wall floor `s_c + W_c`: a mono
  /// wait is blind to the wall offset `s_c`, so a crafted/corrupt FUTURE `s_c` can outrun it (a crafted future wall stamp). The
  /// caller therefore only lets this inflation SKIP the wall veto behind a synchronized-wall proof
  /// (`wall_proves_floor`); absent that, the veto governs. `(1+ρ)` uses THIS (successor) node's own config
  /// (the node whose clock runs this wait).
  ///
  /// `None` OFF the failover tier (no bounded uncertainty / non-LeaseGuard / degenerate `Δ = 0`) AND —
  /// CRUCIALLY — `None` when the EXACT ceil inflation exceeds `u64::MAX`: that wait is NOT representable
  /// as the `Duration::from_nanos` schedule `become_leader` uses, so it must FAIL CLOSED. Clamping it to
  /// `u64::MAX` (the prior behavior) would let `become_leader` ARM the serve while scheduling a wait
  /// SHORTER than the E′ bound — re-opening the mono-undercut-under-drift under a (pathological but constructible)
  /// election timeout above `u64::MAX` nanos. On `None` the caller uses the bare `max_lease_window` and
  /// does not arm the inherited serve. Ceil division (rounding UP only ever over-waits — safe); the value
  /// must also stay `< election_timeout` (checked by the caller, the failover deployment contract).
  fn failover_inflated_commit_wait(&self) -> Option<u64> {
    // E′ needs ONLY the lease timing (Δ, ε_drift) — NOT `bounded_clock_uncertainty`. The inflated mono
    // deadline `max_lease_window·(Δ+ε_drift)/Δ` lands, even at the fastest admissible rate `(1+ρ)`, no
    // earlier than `s_c + W_c` in REAL time, so it covers the inherited walled lease's wall floor WITHOUT
    // a synchronized wall. This is what lets a LeaseGuard successor that lacks ε_unc (and so cannot
    // wall-gate) still hold safely against undercutting a peer's inherited-read serve: it uses the
    // E′-inflated wait. (The SERVE still requires ε_unc — see `inherited_serve_armed`; only the
    // commit-wait safety is available without it.)
    // Gate on the VALIDATED LeaseGuard timing (`leaseguard_timing` ⟹ `leaseguard_commit_wait_ns` is
    // `Some`: `ε_drift < Δ`, and the window `Δ·(Δ+ε)/(Δ−ε)` fits `u64` and is below the election timeout)
    // — NOT the raw config knobs. A timing-INVALID or Safe node carrying stale `lease_duration` /
    // `clock_drift_bound` must NOT reach the E′ proof with unbounded values; it returns `None` and the
    // caller's veto fails closed. Needs NO `bounded_clock_uncertainty` — E′ is a pure lease-timing bound.
    let (delta, drift) = self.leaseguard_timing()?;
    let delta_ns = delta.as_nanos();
    if delta_ns == 0 {
      return None;
    }
    // CHECKED arithmetic (NOT saturating): the safety bound needs the deadline ≥ `max_lease_window·(1+ρ)`
    // EXACTLY. A `saturating_mul` overflow would clamp the product and divide back to a PLAUSIBLE but
    // too-SHORT `u64`, wrongly setting `commit_wait_inflated` and skipping the fail-closed veto — a
    // successor clearing early and undercutting a serve. So any overflowing add/mul FAILS the proof
    // (`None`). CEILING division (rounding UP only ever over-waits — safe against the strict mono-undercut boundary).
    let sum = delta_ns.checked_add(drift.as_nanos())?;
    let inflated = u128::from(self.lease_guard.max_lease_window)
      .checked_mul(sum)?
      .div_ceil(delta_ns);
    // EXACT, never clamped: a value above `u64::MAX` is unschedulable as a `from_nanos` wait → fail
    // closed (`None`) so the caller does not arm a wait backed by a too-short bound.
    u64::try_from(inflated).ok()
  }

  pub(crate) fn become_leader<L: LogStore, S: StableStore<NodeId = I>>(
    &mut self,
    now: Now,
    log: &mut L,
    stable: &mut S,
  ) {
    // A node removed (or demoted to learner) by an applied conf change must NEVER assume leadership —
    // even if it tallied a quorum from the NEW configuration before its own removal applied (the grants
    // come from voters that no longer include it). Step down to follower instead of leading. `is_voter`
    // checks BOTH joint halves, so a node still in the outgoing half keeps leading to shepherd the
    // joint → simple transition. This is the airtight chokepoint: both win paths (`on_vote_response`
    // and the `Campaign` completion) funnel through here.
    if self.config.step_down_on_removal() && !self.tracker.is_voter(&self.config.id()) {
      self.role = Role::Follower;
      self.set_leader(None);
      self.heartbeat_deadline = None;
      self.election_deadline = None;
      return;
    }
    // Clear the leader-scoped replication dirty flags: a `propose`/read that set them in a term this
    // node was then deposed out of must not trigger a spurious broadcast or refresh no-op in THIS fresh
    // leadership. The election no-op below re-sets `replication_pending` for its own append, and the
    // explicit fan-out follows, so nothing legitimate is lost.
    self.replication_pending = false;
    self.lease_guard.lease_refresh_wanted = false;
    self.lease_guard.read_since_anchor = false;
    self.role = Role::Leader;
    self.set_leader(Some(self.config.id()));
    // Reset read-index state from the previous term (stale pending reads must not
    // be confirmed against the new term's commit index).
    self.reads.read_only.reset(self.reads.active_read_mode);
    self.reads.pending_reads.clear();
    // a fresh leader holds NO read lease until a quorum freshly acks its first CheckQuorum
    // round. Reset the lease round/ack set and clear the deadline, so no LeaseBased read can be
    // served until `on_heartbeat_response` confirms a fresh current-round quorum.
    self.check_quorum_lease.lease_round = 0;
    self.check_quorum_lease.lease_acks.clear();
    self.check_quorum_lease.lease_valid_until = None;
    // Fresh leadership starts fresh snapshot-resend pacing (the per-peer deadlines belong to the
    // previous leadership's transfer windows).
    self.snapshot.snapshot_resend_after.clear();
    // Clear any in-progress leader transfer — becoming the leader means the transfer
    // target (us) has won; the previous leader's transfer state is irrelevant.
    self.transfer.lead_transferee = None;
    self.transfer.transfer_deadline = None;
    // a fresh leader term has authorized no forced handoff yet, so the LeaseBased read shortcut is
    // available again once a fresh quorum lease forms. (A `TimeoutNow` sent later this term re-arms it.)
    self.transfer.forced_handoff_this_term = false;
    // Clear the candidate/follower election_deadline unconditionally; it will be re-armed
    // below only if check_quorum is enabled. Without this clear, a CQ-disabled leader would
    // inherit the stale candidate election_deadline (arm_heartbeat_timer no longer clears it).
    self.election_deadline = None;
    self.arm_heartbeat_timer(now);

    // Re-initialize Progress for every tracked member via reset_progress, then mark
    // self as fully caught-up. reset_progress covers voters (both joint halves) ∪
    // learners ∪ learners_next so no member is missing a Progress — a missing voter
    // Progress reads match_index = ZERO and would silently block commit advancement.
    let last = log.last_index();
    // A newly-elected leader may have inherited an uncommitted ConfChange in its log tail.
    // Conservatively block new conf changes until it has committed+applied that whole tail
    // (etcd becomeLeader: "set pendingConfIndex to the last index in the log"). Without this,
    // the one-in-flight guard (pending_conf_index > applied) is ZERO on a fresh leader and a
    // second conf change could stack onto an inherited one, wedging apply on the joint dispatch.
    self.pending_conf_index = last;
    // Mirror for read-mode migrations: a fresh leader may inherit an uncommitted SetReadMode in its tail;
    // block a new mode proposal until the whole inherited tail commits-and-applies (spec §9).
    self.reads.pending_read_mode_index = last;
    self.tracker.reset_progress(
      last.next(),
      self.config.max_inflight_msgs(),
      self.config.max_inflight_bytes(),
    );
    // Self is fully caught up: advance own match_index to last.
    if let Some(p) = self.tracker.progress_mut(&self.config.id()) {
      p.maybe_update(last);
    }

    // CheckQuorum: mark the leader's own Progress as active (it is always reachable to
    // itself) and arm the election_deadline for the first CheckQuorum window.
    if self.config.check_quorum() {
      if let Some(p) = self.tracker.progress_mut(&self.config.id()) {
        p.set_recent_active(true);
      }
      // Use the base election_timeout (not randomized) for the CheckQuorum interval, matching
      // etcd's behavior (checkQuorumActive is checked every electionTimeout ticks).
      self.election_deadline = Some(now.mono() + self.config.election_timeout());
    }

    // LeaseGuard commit-wait: arm the post-election deferred-commit window whenever this node
    // INHERITED a nonzero lease window — REGARDLESS of this node's own read mode. The wait covers a
    // DEPOSED leader's still-live read-lease, which is independent of whether THIS successor serves
    // LeaseGuard reads: a node rolled to Safe/LeaseBased (or holding an invalid LeaseGuard config)
    // that committed new entries while a deposed LeaseGuard leader's lease was still live would
    // recreate the stale read. TWO conservative bounds, with no cross-node clock comparison and no
    // assumption about any other node's config:
    //   • TIME anchor = THIS election's `now` — a lower bound on every inherited entry's creation
    //     time (this node replicated them all before winning the election).
    //   • WINDOW bound = `max_lease_window` — the MAX of every inherited entry's SELF-DESCRIBING
    //     `lease_window` (its appending leader's own exact `Δ·(Δ+ε)/(Δ−ε)`). So whatever window any
    //     deposed leader actually used is carried in the entries it created and covered exactly, even
    //     under heterogeneous per-node config. `0` on a cluster that never ran LeaseGuard (every
    //     `lease_window` is 0) ⇒ no wait, so Safe/LeaseBased clusters are unaffected.
    // (`leaseguard_timing` gates STAMPING new entries and serving lease reads, NOT this wait.)
    // FAILOVER-tier E′ inflation, gated on the SERVE being ARMED this term. The inherited-read SERVE
    // (`failover_read_window`) duals the PRECISE wall floor `s_c + W_c + 2·ε_unc`, so for it to be
    // linearizable EVERY electable successor's commit-past-c must be ≥ that floor — including via THIS
    // conservative MONO deadline. The bare `now.mono() + max_lease_window` covers only a deposed leader's
    // read-LEASE (`Δ_D/(1−ρ)` real), SHORTER than the window `W_c` the serve keys on, so under rate drift
    // it can fire BEFORE the serve withdraws (the mono-undercut under drift). Inflating by `(Δ+ε_drift)/Δ = (1+ρ)` makes the mono
    // deadline land no earlier than `s_c + W_c` even at the fastest rate — PROVIDED `s_c` is a real PAST
    // stamp (`s_c ≤` this election's wall). A crafted/corrupt FUTURE `s_c` breaks that (a crafted future stamp: E′ is a window-
    // only mono duration, blind to the wall offset), so the inflation is ADDITIONALLY gated below on a
    // synchronized-wall proof (`wall_proves_floor`); absent that proof the node holds via the veto, never on
    // E′ alone.
    //
    // BUT the inflated wait keys on `max_lease_window` — the MAX window INHERITED, possibly stamped by
    // ANOTHER node's larger config — which config validation cannot bound (no cluster-wide config check).
    // So ARM the serve (and the inflation) ONLY when a valid active failover tier is configured AND the
    // EXACT inflated wait both fits a schedulable `u64` nanos (`failover_inflated_commit_wait` is `Some`)
    // AND stays strictly below the election timeout (else the first failover commit could not land before
    // a follower deposes the leader). The schedulability gate is load-bearing: an exact inflation above
    // `u64::MAX` cannot be scheduled as a `from_nanos` wait, so arming on a clamped value would back the
    // serve with a too-short wait and re-open the mono-undercut. When NOT armed, use the bare shipped wait and serve no
    // inherited reads (no serve ⇒ no mono-undercut ⇒ no inflation needed): liveness preserved, safety fail-closed.
    let inflated = self.failover_inflated_commit_wait();
    // The wall horizon `max_wall_plus_window + 2·ε_unc` (the release floor the serve duals, ≥ the serve's
    // own `s_c + W_c`) must be PASSABLE by a u64 wall reading. A near-`u64::MAX` inherited wall stamp makes
    // it non-passable: no wall could reach it, so the serve would never withdraw AND the wall-gated
    // release would wedge. Fail closed — disarm the serve; the conservative mono release then governs
    // UNVETOED (`walled_lease_vetoes_conservative` skips the veto for the SAME non-passable horizon),
    // terminating with no serve to undercut. Both the arm and the veto key on the ONE shared predicate
    // `failover_horizon_passable` over `max_wall_plus_window` (= the captured `inherited_release_deadline`)
    // with the SAME u64 ε conversion, so they can never disagree at the strict `u64::MAX` boundary.
    let horizon_passable = self.config.bounded_clock_uncertainty().is_some_and(|eps| {
      let eps_ns = u64::try_from(eps.as_nanos()).unwrap_or(u64::MAX);
      Self::failover_horizon_passable(self.lease_guard.max_wall_plus_window, eps_ns)
    });
    // The E′ inflation FITS this term iff it is computable (valid Δ/ε_drift, no overflow) and stays below
    // the election timeout.
    let e_prime_fits =
      inflated.is_some_and(|w| u128::from(w) < self.config.election_timeout().as_nanos());
    // CRAFTED-FUTURE-WALL-STAMP — the E′ MONO wait may only SKIP the wall veto (clear the commit-wait without ever consulting a
    // wall) when a synchronized wall reading AT THIS ELECTION proves it reaches the absolute walled release
    // floor `max_wall_plus_window`. E′ is sized from `max_lease_window` (a window-ONLY bound) and carries NO
    // wall-offset information, so a crafted/corrupt `max_wall_plus_window` — a `wall_stamp + lease_window`
    // SUM whose reach a FUTURE stamp inflates independently of `max_lease_window` — can outrun it. The proof,
    // in u128 (the sums exceed `u64`): `now_wall + max_lease_window ≥ max_wall_plus_window + 2·ε_unc` (one
    // ε_unc lifts `now_wall` to a real-time lower bound on the election instant; one is the peer-serve slack
    // the serve dual at `read_index.rs` burns). It REQUIRES ε_unc AND a present wall. This RETRACTS the prior
    // "E′ covers the floor in REAL time WITHOUT a synchronized wall" claim: a mono duration cannot bound an
    // absolute wall floor against a crafted stamp, so a node lacking ε_unc or a wall — or whose floor outruns
    // E′ — cannot inflate and is held FAIL-CLOSED by the veto (`walled_lease_vetoes_conservative`: the wall-
    // gate for an ε_unc node with a wall, the no-ε_unc fail-closed branch otherwise).
    let wall_proves_floor = self
      .config
      .bounded_clock_uncertainty()
      .map(|eps| u64::try_from(eps.as_nanos()).unwrap_or(u64::MAX))
      .is_some_and(|eps_ns| {
        !now.wall().is_absent()
          && u128::from(now.wall().as_nanos()) + u128::from(self.lease_guard.max_lease_window)
            >= u128::from(self.lease_guard.max_wall_plus_window) + 2 * u128::from(eps_ns)
      });
    // Use the E′-INFLATED commit-wait (which SKIPS the wall veto) only when it fits, this node inherited
    // WALLED entries (`max_wall_plus_window != 0`, so a peer's serve could exist to undercut), the window
    // bound is positive, AND the wall-proof holds. With no walled inherited entries (basic LeaseGuard / Safe)
    // the bare shipped wait is used (no serve risk). A node that inherited walled entries but cannot
    // wall-PROVE the floor (no ε_unc, no wall, or a floor that outruns E′) gets the bare wait here and is held
    // FAIL-CLOSED by the veto. `max_lease_window > 0` is REQUIRED: a zero window makes
    // `failover_inflated_commit_wait` return `Some(0)`, which would mark the node E′-inflated with a ZERO wait
    // and bypass the fail-stop below.
    let inflated_candidate = e_prime_fits
      && self.lease_guard.max_wall_plus_window != 0
      && self.lease_guard.max_lease_window > 0
      && wall_proves_floor;
    let armed_candidate = self.failover_tier_active()
      && self.lease_guard.max_lease_window > 0
      && horizon_passable
      && e_prime_fits;
    let commit_wait_window = inflated
      .filter(|_| inflated_candidate)
      .unwrap_or(self.lease_guard.max_lease_window);
    // `Instant::add` SATURATES (`now.mono() + window` clamps at `Instant::MAX`). A monotonic instant
    // within `commit_wait_window` of the ceiling would store a deadline at the saturated max — a real wait
    // SHORTER than the window. Such a too-short wait must NOT be treated as E′-inflated or serve-armed: the
    // inflated flag suppresses the wall/absent-wall veto, so a clamped wait would clear early and undercut
    // a peer's still-live serve. Only honor the inflation/serve when the deadline is EXACTLY representable
    // (`since_origin() + window` does not overflow `Duration`); otherwise the candidate falls into the
    // fail-closed veto path (a BARE wait with the veto, which holds until the wall proves the floor or
    // fails closed). Astronomically unreachable (`now.mono()` ≈ `Duration::MAX` ≈ 5.8·10¹¹ years), kept
    // TOTAL for any input. The other commit-wait deadlines that saturate (`unwalled_commit_wait_until`, the
    // veto re-arm) only ever clamp LATER (`now >= MAX` is then ~never), holding LONGER — safe.
    let deadline_exact = now
      .mono()
      .since_origin()
      .checked_add(Duration::from_nanos(commit_wait_window))
      .is_some();
    self.lease_guard.commit_wait_inflated = inflated_candidate && deadline_exact;
    // The SERVE additionally requires a valid active failover tier (ε_unc) and a passable horizon.
    self.lease_guard.inherited_serve_armed = armed_candidate && deadline_exact;
    // If there is a commit-wait to schedule but its deadline is NOT exactly representable, the stored
    // `now.mono() + window` saturates to `Instant::MAX` — a wait SHORTER than the window that would clear
    // the commit-wait early and commit before a deposed leader's lease window elapsed (a stale read, basic
    // LeaseGuard AND failover, regardless of the now-suppressed flags). The deadline cannot be scheduled,
    // so FAIL-STOP: poison. A poisoned node's `handle_message`/`handle_timeout` return early, so it never
    // advances commit — it holds rather than under-wait. Unreachable by any real monotonic clock.
    if self.lease_guard.max_lease_window > 0 && !deadline_exact {
      self.poison(PoisonReason::CommitWaitUnrepresentable);
    }
    // A BARE-wait ε_unc successor (no E′ inflation) relies SOLELY on the wall-gate to bound an inherited
    // WALLED lease. If that lease's horizon is NON-PASSABLE (`max_wall_plus_window + 2·ε_unc > u64::MAX`),
    // no `u64` wall can ever prove it expired — the gate can never fire. Skipping the gate would let the
    // bare mono wait clear early and undercut ANOTHER leader's serve on a LOWER, passable committed anchor
    // (Raft can place a near-`u64::MAX` tail entry on only some voters, so a non-passable LOCAL max does
    // not prove no peer is serving). So FAIL-STOP. An E′-INFLATED successor (`commit_wait_inflated`) is
    // exempt — its mono wait covers the floor WITHOUT the wall, so a non-passable wall horizon is harmless
    // to it. A real synchronized wall is ≪ `u64::MAX`; a non-passable inherited stamp is a crafted entry.
    if self.config.bounded_clock_uncertainty().is_some()
      && !self.lease_guard.commit_wait_inflated
      && self.lease_guard.max_wall_plus_window != 0
      && !horizon_passable
    {
      self.poison(PoisonReason::WallHorizonUnrepresentable);
    }
    // INHERITED-LEASE FLOOR fold-consistency — cheap DEFENSE-IN-DEPTH against a BUG in OUR OWN fold (the
    // `submit_append` / restart-scan / snapshot-install folds emitting internally-inconsistent floors), NOT a
    // defense against forged metadata. THREAT MODEL: this library is CRASH-FAULT-TOLERANT — a recovered
    // `SnapshotMeta` comes from a CORRECT leader over reliable (checksummed) storage, so its floors are
    // FAITHFUL. A forged-but-internally-consistent floor (a too-small or too-low value) requires a Byzantine
    // leader or corrupt storage — OUT OF SCOPE, and in any case undetectable here (a forged-LOW floor is
    // indistinguishable from a real long-expired entry without the per-entry provenance the snapshot compacts
    // away; staleness from it further requires a DIVERGENT peer holding the real high floor, i.e. non-CFT).
    // We therefore only fail-stop the STRUCTURAL contradictions a correct fold can never produce (every
    // window-bearing entry is walled — folding into `max_wall_plus_window` — or unwalled — folding into
    // `max_unwalled_lease_window`):
    //   `max_wall_plus_window != 0` ⟹ `max_lease_window > 0`        (a walled floor implies a window bound)
    //   `max_unwalled_lease_window ≤ max_lease_window`              (the unwalled fallback is dominated)
    //   `max_lease_window > 0` ⟹ a floor is classified              (a window bound implies a fold floor)
    // A violation means OUR fold is buggy (or, out of scope, the meta is forged) — fail-stop rather than arm
    // a commit-wait/serve off self-contradictory state. We deliberately do NOT chase forged-magnitude shapes
    // (a too-small nonzero floor) — that is the Byzantine/corrupt-storage class, outside CFT.
    if (self.lease_guard.max_wall_plus_window != 0 && self.lease_guard.max_lease_window == 0)
      || self.lease_guard.max_unwalled_lease_window > self.lease_guard.max_lease_window
      || (self.lease_guard.max_lease_window > 0
        && self.lease_guard.max_wall_plus_window == 0
        && self.lease_guard.max_unwalled_lease_window == 0)
    {
      self.poison(PoisonReason::InconsistentLeaseFloor);
    }
    self.lease_guard.commit_wait_until = (self.lease_guard.max_lease_window > 0)
      .then(|| now.mono() + Duration::from_nanos(commit_wait_window));
    // FAILOVER-tier PRECISE commit-anchor (consumed by `maybe_advance_commit`'s precise early-release).
    // Pin, immutable for this term: the WALL-frame release floor = `max_wall_plus_window` (max over
    // WALLED inherited entries of `wall_timestamp + lease_window`), and the MONO-frame fallback deadline
    // = `now + max_unwalled_lease_window` for any WALL-ABSENT (fail-closed) inherited lease entry. Both
    // inert (`0` / `None`) on a cluster with no such inherited entry, so off-tier the shipped
    // conservative anchor above governs unchanged.
    self.lease_guard.inherited_release_deadline = self.lease_guard.max_wall_plus_window;
    self.lease_guard.unwalled_commit_wait_until = (self.lease_guard.max_unwalled_lease_window > 0)
      .then(|| now.mono() + Duration::from_nanos(self.lease_guard.max_unwalled_lease_window));
    // FAILOVER-tier INHERITED-READ serve anchors (consumed by `failover_read_window`). Pinned ONCE here,
    // immutable for the term — `log.last_index()` and `commit` both drift during the term, so neither
    // may stand in later (§4). `limbo_upper` = the election tail (captured BEFORE the no-op below, which
    // would otherwise inflate it by one); the per-key limbo region the app checks is `(commit,
    // limbo_upper]`. `committed_anchor_wall`/`committed_anchor_window` = the EXACT `(wall, lease_window)`
    // of `log[commit]` (the entry both this leader and every electable higher-term leader hold); the
    // serve gate keys on the entry's OWN window so it dovetails with the release floor for any per-node
    // config. `(0, 0)` fail-closed when `log[commit]` is compacted / absent — the gate then refuses,
    // never serving past a dead lease.
    self.lease_guard.limbo_upper = last;
    let (anchor_wall, anchor_window) = self.committed_anchor_at_election(log);
    // SERVE-SIDE DUAL (the future committed-anchor hazard): the committed anchor is read VERBATIM from `log[commit]`; a
    // crafted/corrupt entry could carry a FUTURE `wall_timestamp`, making `inherited_lease_live` (which
    // serves while `now_wall + 2·ε_unc < s_c + W_c`) offer the inherited read far past the real lease — the
    // serve-side mirror of the release hole. Trust the anchor only when a synchronized wall AT THIS ELECTION
    // proves it was NOT stamped in this successor's future (`s_c ≤ now_wall + ε_unc`) AND it does not exceed
    // the release floor this successor actually enforces (`s_c + W_c ≤ max_wall_plus_window`; equality holds
    // for a sound fold since `log[c]` folds into the floor). Otherwise FAIL-CLOSED: drop the anchor to
    // `(0, 0)` so the serve refuses (the read degrades to Safe; the release side still holds commit via the
    // veto). u128 — the sums exceed `u64`. A no-ε_unc node never serves (`failover_tier_active` gates the
    // serve), so this only constrains the ε_unc serve path; an absent election wall fails closed here too.
    let anchor_trustworthy = self.config.bounded_clock_uncertainty().is_some_and(|eps| {
      let eps_ns = u64::try_from(eps.as_nanos()).unwrap_or(u64::MAX);
      !now.wall().is_absent()
        && u128::from(anchor_wall) <= u128::from(now.wall().as_nanos()) + u128::from(eps_ns)
        && u128::from(anchor_wall) + u128::from(anchor_window)
          <= u128::from(self.lease_guard.max_wall_plus_window)
    });
    let (anchor_wall, anchor_window) = if anchor_trustworthy {
      (anchor_wall, anchor_window)
    } else {
      (0, 0)
    };
    self.lease_guard.committed_anchor_wall = anchor_wall;
    self.lease_guard.committed_anchor_window = anchor_window;

    // Append the new leader's no-op entry (lets it commit prior-term entries, §5.4.2).
    // Self-match advance is deferred until the append is durable (on_log_appended). A log at the index
    // ceiling is corrupt/terminal — `append_leader_noop` poisons and returns `None`; fail-stop.
    if self.append_leader_noop(now, log, last).is_none() {
      return;
    }

    // (`set_leader` above emitted `LeaderChanged(Some(self))` — a candidate's leader belief is
    // always `None`, so the transition always fires.)

    // Broadcast heartbeats and kick off replication to peers.
    self.broadcast_heartbeat(now);
    for peer in self.peers().collect::<Vec<_>>() {
      self.maybe_send_append(now, peer, log, stable);
    }
  }
}
