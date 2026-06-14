use super::*;

impl<I, F> Endpoint<I, F>
where
  I: NodeId,
  F: StateMachine,
{
  // --- PRIVATE HELPERS (no Data bound) ---

  pub(crate) fn arm_election_timer(&mut self, now: crate::Now) {
    let t = self.prng.election_timeout(self.config.election_timeout());
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
  pub(crate) fn reconcile_election_timer(&mut self, now: crate::Now) {
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
  pub(crate) fn step_down_to_follower(&mut self, now: crate::Now) {
    self.role = Role::Follower;
    self.set_leader(None);
    self.heartbeat_deadline = None;
    // Drop all pending reads — a stepped-down node is no longer the leader and
    // cannot confirm any outstanding read requests.
    self.read_only.reset(self.config.read_only());
    // A stepped-down node no longer serves LeaseGuard reads, so drop any pending lease-refresh demand
    // (only a leader appends the refresh no-op; a re-election re-stamps the lease via its own no-op).
    self.lease_refresh_wanted = false;
    self.pending_reads.clear();
    // Abort any in-progress leader transfer — leadership is changing, the transfer is moot.
    self.lead_transferee = None;
    self.transfer_deadline = None;
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
      .pending
      .values()
      .any(|p| matches!(p, Pending::Campaign { term } if *term == self.term))
  }

  /// Whether the post-restart vote-suppression fence currently blocks GRANTING a vote (LeaseBased
  /// crash-safety). `None`/expired fence ⇒ not fenced. A forced leader-transfer bypasses it: the current
  /// leader is voluntarily handing off (relinquishing its lease), so granting cannot strand a live lease
  /// — mirrors the `in_lease` bypass. Only ever armed under `ReadOnlyOption::LeaseBased` (see `restart`).
  #[inline]
  pub(crate) fn lease_vote_fenced(&self, now: crate::Now, force: bool) -> bool {
    !force && self.lease_vote_fence_until.is_some_and(|d| now.mono() < d)
  }

  pub(crate) fn on_request_vote<L: LogStore, S: StableStore<NodeId = I>>(
    &mut self,
    now: crate::Now,
    log: &mut L,
    stable: &mut S,
    rv: crate::RequestVote<I>,
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
    if let Some((_, meta, ..)) = &self.pending_install
      && (meta.last_term(), meta.last_index()) > (my_term, my_index)
    {
      my_index = meta.last_index();
      my_term = meta.last_term();
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
      let resp_term = if grant { rv.term() } else { self.term };
      self.send(
        rv.candidate(),
        Message::VoteResp(crate::VoteResp::new(resp_term, me, true, !grant)),
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
      // Persist (term, vote); the VoteResp(grant) is owed once the write is DURABLE.
      // Stamp the current commit too: we read-modify `hard_state()` then override fields, so
      // without this the write would carry a possibly-stale `hard_state().commit` and could
      // REGRESS the durable commit below a value the handle_storage choke-point already wrote.
      // `self.commit` is monotonic, so stamping it keeps the durable commit monotonic.
      let opid = self.mint_op_id();
      let hs = stable
        .hard_state()
        .with_term(self.term)
        .with_vote(self.voted_for)
        .with_commit(self.durable_commit());
      self.submit_write(stable, opid, hs);
      self.committed_persisted = self.durable_commit();
      self.pending.insert(
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
        Message::VoteResp(crate::VoteResp::new(term, me, false, true)),
      );
    }
  }

  pub(crate) fn on_vote_resp<L: LogStore, S: StableStore<NodeId = I>>(
    &mut self,
    now: crate::Now,
    log: &mut L,
    stable: &mut S,
    vr: crate::VoteResp<I>,
  ) where
    F::Command: crate::Data,
    // `become_candidate`/`become_leader` live in the `apply_committed` impl block, which is
    // gated on this bound (the fatal apply error must be inspectable, design spec §6.3).
    F::Error: core::error::Error,
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
impl<I, F> Endpoint<I, F>
where
  I: NodeId,
  F: StateMachine,
  F::Command: crate::Data,
  F::Error: core::error::Error,
{
  /// Start a real election campaign.
  ///
  /// `transfer` must be `true` when called from `on_timeout_now` (leader-transfer path):
  /// it sets `leader_transfer: true` on the broadcast `RequestVote` so that granters bypass
  /// their CheckQuorum/PreVote lease check (the `!force` guard).  For normal elections
  /// (election-timeout path, pre-vote quorum reached) pass `transfer = false`.
  pub(crate) fn become_candidate<L: LogStore, S: StableStore<NodeId = I>>(
    &mut self,
    now: crate::Now,
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
    self.pending.clear();
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
      .with_vote(self.voted_for)
      .with_commit(self.durable_commit());
    self.submit_write(stable, opid, hs);
    self.committed_persisted = self.durable_commit();
    // Defer acting on the self-vote until it is DURABLE (persist-before-act, symmetric with the
    // follower `CastVote` path): `become_leader` fires from `on_stable_wrote` (single-node now, or
    // once peer votes arrive) only after this write's `StableDone::Wrote`.
    self
      .pending
      .insert(opid, Pending::Campaign { term: self.term });
    self.arm_election_timer(now);

    let (term, me) = (self.term, self.config.id());
    // Send RequestVote only to VOTER peers (not learners). Learners don't participate in
    // elections; sending them a RequestVote wastes bandwidth and may confuse their state.
    // Replication still goes to all peers (learners get AppendEntries from become_leader).
    let voter_peers: std::vec::Vec<_> = self.peers().filter(|p| self.tracker.is_voter(p)).collect();
    for peer in voter_peers {
      self.send(
        peer,
        Message::RequestVote(crate::RequestVote::new(
          term, me, last_index, last_term, false, transfer,
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
  pub(crate) fn become_pre_candidate<L: LogStore>(&mut self, now: crate::Now, log: &L) -> bool {
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
    let voter_peers: std::vec::Vec<_> = self.peers().filter(|p| self.tracker.is_voter(p)).collect();
    for peer in voter_peers {
      self.send(
        peer,
        Message::RequestVote(crate::RequestVote::new(
          advertised_term,
          me,
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
    now: crate::Now,
    log: &mut L,
    last: crate::Index,
  ) -> Option<crate::Index> {
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
    self
      .pending
      .insert(opid, Pending::LeaderAppend { upto: noop_index });
    Some(noop_index)
  }

  pub(crate) fn become_leader<L: LogStore, S: StableStore<NodeId = I>>(
    &mut self,
    now: crate::Now,
    log: &mut L,
    stable: &mut S,
  ) {
    self.role = Role::Leader;
    self.set_leader(Some(self.config.id()));
    // Reset read-index state from the previous term (stale pending reads must not
    // be confirmed against the new term's commit index).
    self.read_only.reset(self.config.read_only());
    self.pending_reads.clear();
    // a fresh leader holds NO read lease until a quorum freshly acks its first CheckQuorum
    // round. Reset the lease round/ack set and clear the deadline, so no LeaseBased read can be
    // served until `on_heartbeat_resp` confirms a fresh current-round quorum.
    self.lease_round = 0;
    self.lease_acks.clear();
    self.lease_valid_until = None;
    // Fresh leadership starts fresh snapshot-resend pacing (the per-peer deadlines belong to the
    // previous leadership's transfer windows).
    self.snapshot_resend_after.clear();
    // Clear any in-progress leader transfer — becoming the leader means the transfer
    // target (us) has won; the previous leader's transfer state is irrelevant.
    self.lead_transferee = None;
    self.transfer_deadline = None;
    // a fresh leader term has authorized no forced handoff yet, so the LeaseBased read shortcut is
    // available again once a fresh quorum lease forms. (A `TimeoutNow` sent later this term re-arms it.)
    self.forced_handoff_this_term = false;
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
    self.commit_wait_until = (self.max_lease_window > 0)
      .then(|| now.mono() + core::time::Duration::from_nanos(self.max_lease_window));
    // FAILOVER-tier PRECISE commit-anchor (consumed by `maybe_advance_commit`'s precise early-release).
    // Pin, immutable for this term: the WALL-frame release floor = `max_wall_plus_window` (max over
    // WALLED inherited entries of `wall_timestamp + lease_window`), and the MONO-frame fallback deadline
    // = `now + max_unwalled_lease_window` for any WALL-ABSENT (fail-closed) inherited lease entry. Both
    // inert (`0` / `None`) on a cluster with no such inherited entry, so off-tier the shipped
    // conservative anchor above governs unchanged.
    self.inherited_release_deadline = self.max_wall_plus_window;
    self.unwalled_commit_wait_until = (self.max_unwalled_lease_window > 0)
      .then(|| now.mono() + core::time::Duration::from_nanos(self.max_unwalled_lease_window));

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
    for peer in self.peers().collect::<std::vec::Vec<_>>() {
      self.maybe_send_append(now, peer, log, stable);
    }
  }
}
