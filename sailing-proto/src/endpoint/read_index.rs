use super::*;

impl<I, F> Endpoint<I, F>
where
  I: NodeId,
  F: StateMachine,
  F::Command: crate::Data,
  F::Error: core::error::Error,
{
  // ─── ReadIndex helpers ────────────────────────────────────────────────────────

  /// Whether the leader has committed an entry in its current term.
  ///
  /// A newly-elected leader cannot confirm reads against a commit index whose entry is from
  /// a prior term (§5.4.2).  It must wait until its no-op append is committed before
  /// confirming any reads.
  pub(crate) fn has_current_term_commit<L: LogStore>(&mut self, log: &L) -> bool {
    self
      .log_term(log, self.commit)
      .map(|t| t == self.term)
      .unwrap_or(false)
  }

  /// Confirm all pending reads in `pending_reads` by registering them with `read_only` and
  /// broadcasting the heartbeat round (Safe) or confirming immediately (LeaseBased).
  ///
  /// Called once the leader first commits an entry in its current term.
  pub(crate) fn flush_deferred_reads<L: LogStore, S: StableStore<NodeId = I>>(
    &mut self,
    now: crate::Now,
    log: &L,
    _stable: &S,
  ) {
    if self.pending_reads.is_empty() {
      return;
    }
    let deferred = core::mem::take(&mut self.pending_reads);
    for (ctx, from) in deferred {
      self.do_leader_read(now, log, ctx, from);
    }
  }

  /// Called after `maybe_advance_commit` to flush any deferred read requests once the
  /// leader has committed its first current-term entry.
  pub(crate) fn maybe_flush_deferred_reads<L: LogStore, S: StableStore<NodeId = I>>(
    &mut self,
    now: crate::Now,
    log: &L,
    stable: &S,
  ) {
    if self.pending_reads.is_empty() {
      return;
    }
    if !self.role.is_leader() {
      return;
    }
    if !self.has_current_term_commit(log) {
      return;
    }
    self.flush_deferred_reads(now, log, stable);
  }

  /// THE single source of truth for LeaseBased read safety. A leader
  /// may serve a `LeaseBased` read from its local commit WITHOUT a per-read heartbeat round ONLY when no
  /// other node can be (or become) leader before this lease expires. That holds iff ALL of:
  ///
  ///   1. `check_quorum` is enabled — the lease invariant is only maintained under CheckQuorum (a leader
  ///      that loses quorum contact steps down within an election timeout).
  ///   2. a FRESH quorum lease is live (`lease_valid_until > now`). The lease is renewed in
  ///      `on_heartbeat_resp` ONLY by a HeartbeatResp echoing the CURRENT `lease_round` and is
  ///      bounded by the round's SEND time, not response receipt — so a stale/duplicated/delayed
  ///      response can neither keep an isolated leader's lease alive nor over-extend it. SELF-VALIDATING:
  ///      a contributing ack must ALSO advertise that it enforces the lease window
  ///      (`lease_support > 0`), and the deadline is bounded by the quorum's MIN advertised support — so a
  ///      voter that does not run `in_lease`+the vote fence, or that runs a SHORTER `election_timeout`,
  ///      cannot prop up the lease; the read silently degrades to Safe instead of trusting an unenforced
  ///      or over-long window. (The CheckQuorum `recent_active`/`election_deadline` step-down signal is
  ///      deliberately NOT reused here — it is set by ANY inbound current-term message and is thus
  ///      spoofable by stale/duplicated traffic.)
  ///   3. no leader transfer is in progress (`lead_transferee.is_none()`): an active transfer
  ///      authorizes the transferee to campaign FORCED, so this leader may not be the only one.
  ///   4. no forced handoff was authorized this term (`!forced_handoff_this_term`): once a
  ///      `TimeoutNow` is sent, the authorized forced campaign (or its already-sent forced `RequestVote`s)
  ///      can elect a new leader at ANY later point this term under unbounded message delay — even after
  ///      the transfer aborts and `lead_transferee` clears. Lease reads stay off until re-election.
  ///
  /// The LEASE WINDOW is upheld on the FOLLOWER side by two complementary mechanisms, so a new leader
  /// cannot be elected while a lease the followers granted is still live:
  ///   - `in_lease` (in `handle_message`): a follower that has heard from its current leader within the
  ///     election timeout ignores a disruptive higher-term vote request; and
  ///   - the post-restart vote fence (`lease_vote_fenced`, armed in `restart`): a RESTARTED
  ///     follower, which may have acked a lease it has since forgotten, refuses to grant votes until the
  ///     promise expires. The fence is sized by the DURABLE lease-support floor (`HardState.lease_support`,
  ///     persisted before the advertisement via the `on_heartbeat` gate), so it honors the pre-crash promise
  ///     even if the node restarts under a weaker config (shorter `election_timeout` / enforcement disabled).
  ///
  /// A FORCED leader-transfer vote bypasses both (the current leader voluntarily relinquished its lease,
  /// clearing it in `transfer_leader` and disabling its own lease reads via conditions 3–4).
  ///
  /// A committed MEMBERSHIP CHANGE also revokes the lease: the lease's safety rests on the granting
  /// quorum OVERLAPPING any new-leader quorum (a shared voter's `in_lease`/vote-fence blocks the
  /// disruptive vote), which holds only WITHIN a single configuration. `apply_committed` clears
  /// `lease_valid_until` on a ConfChange so reads degrade to Safe until a fresh quorum re-confirms the
  /// lease under the new config — a config whose quorum is disjoint from the old one cannot inherit it.
  ///
  /// RESIDUAL CAVEAT (IRREDUCIBLE for ALL lease reads — etcd's included — and PROVEN unremovable by a
  /// multi-expert Raft panel: a lease infers a non-event from elapsed time, which no logical/epoch/HLC
  /// machinery can discharge): bounded clock-RATE drift, plus the non-Byzantine honesty of voters. The
  /// self-validating renewal (condition 2) closed the COOPERATION/heterogeneity vector by construction, and
  /// the durable lease-support floor closed the CONFIG-DRIFT-across-restart vector (a node restarting
  /// under weaker config granting a vote inside a live lease), so these are the ONLY residuals that remain.
  /// If this leader's clock runs slow relative to the followers'
  /// election timers, a follower could time out and elect a new leader before this lease expires.
  /// Deployments that cannot bound clock drift MUST use `ReadOnlyOption::Safe` (the default), whose
  /// per-read heartbeat round needs no timing assumption.
  #[inline]
  pub(crate) fn lease_read_available(&self, now: crate::Now) -> bool {
    self.config.check_quorum()
      && self.lead_transferee.is_none()
      && !self.forced_handoff_this_term
      && self.lease_valid_until.is_some_and(|d| d > now.mono())
  }

  /// Confirm a read immediately at `index` (a lease fast-path — no heartbeat round): emit
  /// `Event::ReadState` for a local read, or reply `ReadIndexResp` to the forwarding follower.
  /// Shared by the LeaseBased and LeaseGuard immediate-serve paths.
  fn emit_or_reply_read(&mut self, index: Index, context: Bytes, from: Option<I>) {
    match from {
      None => self.emit_read_state(index, context),
      Some(follower) => {
        let (term, me) = (self.term, self.config.id());
        self.send(
          follower,
          Message::ReadIndexResp(crate::ReadIndexResp::new(term, me, index, context, false)),
        );
      }
    }
  }

  /// LeaseGuard same-leader read gate ("the log is the lease"): the leader's most-recent committed
  /// entry is still within the lease window Δ on the leader's OWN monotonic clock. Fails CLOSED —
  /// degrade to the safe heartbeat round — on an inactive/invalid config (see
  /// [`leaseguard_timing`](Self::leaseguard_timing)) or an unreadable/absent anchor.
  pub(crate) fn lease_guard_read_live<L: LogStore>(&mut self, now: crate::Now, log: &L) -> bool {
    let Some((delta, _drift)) = self.leaseguard_timing() else {
      return false;
    };
    // Read the committed anchor entry ONCE for both its term and timestamp. The lease is live only if
    // the anchor is a CURRENT-TERM entry — so THIS leader stamped it, making the ts/now comparison
    // same-clock (no skew) — AND still within Δ. The current-term check keeps the no-skew guarantee
    // LOCAL here rather than relying solely on the caller's `has_current_term_commit` gate. A storage
    // read failure poisons and fails closed (an absent index answers `Ok` empty, not `Err`).
    let (term, ts) = match log.entries(self.commit..self.commit.next(), u64::MAX) {
      Ok(s) => match s.first() {
        Some(e) => (e.term(), e.timestamp()),
        None => return false,
      },
      Err(_) => {
        self.poison(PoisonReason::LogRead);
        return false;
      }
    };
    if term != self.term {
      return false;
    }
    // The lease is live iff the entry's age is below Δ — equivalently `ts + Δ > now`. STRICT (`<`):
    // dead at the EXACT expiry instant, so a deposed leader stops serving strictly before a successor
    // (whose commit-wait releases at `now >= deadline`) can commit, closing the equal-timestamp race
    // (matches LeaseBased's strict `deadline > now`). Computed as DURATIONS (u128 internally), never a
    // lossy `u128 → u64` cast: a huge `Instant` cannot wrap `now` to a small value and keep the gate
    // falsely live. `saturating_sub` floors a future-stamped (clock-non-monotone) anchor at age 0.
    now
      .mono()
      .since_origin()
      .saturating_sub(core::time::Duration::from_nanos(ts))
      < delta
  }

  /// FAILOVER inherited-read lease-live gate: the committed anchor `log[c]` (captured at election as
  /// [`committed_anchor_wall`](Self::committed_anchor_wall) `= s_c` and
  /// [`committed_anchor_window`](Self::committed_anchor_window) `= W_c`) is provably still un-overwritten
  /// on the SYNCHRONIZED wall, accounting for bounded cross-node uncertainty ε_unc:
  ///
  /// ```text
  /// SERVE iff now_wall + ε_unc < committed_anchor_wall + W_c − ε_unc   ⟺   now_wall + 2·ε_unc < s_c + W_c
  /// ```
  ///
  /// This is the strict DUAL of the SINGLE shared horizon predicate
  /// [`walled_wall_released`](Self::walled_wall_released): serve while
  /// `now_wall + 2·ε_unc < deadline`, exactly when a successor inheriting `log[c]` would NOT yet be
  /// released on the SAME wall floor (`now_wall > deadline + 2·ε_unc`), with `deadline = s_c + W_c`. The
  /// horizon is the entry's OWN self-describing window `W_c`, NOT this successor's config Δ, AND it is
  /// the SAME formula (`wall + lease_window`) the precise release folds — so the serve can never use a
  /// different window than the release (the config-Δ-over-serve class: using config Δ over-serves past the release under
  /// heterogeneous config; the deposed leader's own Δ is irrelevant, only a successor's release-on-`W_c`
  /// governs freshness). The matching mono-frame undercut (under drift) is closed on the release side by the E′
  /// inflation in `become_leader` (so every electable successor's CONSERVATIVE release is also
  /// `≥ s_c + W_c`). Fails CLOSED: no synchronized wall this tick, no captured anchor (`s_c = 0`), the
  /// anchor is not lease-bearing (`W_c = 0`), or the failover tier is inactive. STRICT `<`. `u128`
  /// (wall + window nanos exceed `u64`), never a lossy cast.
  fn inherited_lease_live(&self, now: crate::Now) -> bool {
    if !self.failover_tier_active()
      || now.wall().is_absent()
      || self.committed_anchor_wall == 0
      || self.committed_anchor_window == 0
    {
      return false;
    }
    let Some(eps_unc) = self.config.bounded_clock_uncertainty() else {
      return false; // unreachable given `failover_tier_active`, but keeps the binding total
    };
    let now_wall = now.wall().as_nanos() as u128;
    let two_eps = 2 * eps_unc.as_nanos();
    now_wall + two_eps < self.committed_anchor_wall as u128 + self.committed_anchor_window as u128
  }

  /// Whether a VALID, ACTIVE LeaseGuard FAILOVER tier is configured. Delegates to the SINGLE source of
  /// truth [`Config::failover_tier_valid`](crate::Config::failover_tier_valid) — the SAME predicate
  /// `Config::validate` rejects on — so a config the crate would reject can never activate the tier at
  /// runtime (`Endpoint::new` does not call `validate`). Requires LeaseGuard mode, a computable
  /// commit-wait window, AND a bounded clock-uncertainty that is a real fraction of the lease
  /// (`ε_unc < Δ`): `bounded_clock_uncertainty` merely being `Some` is NOT enough (a Safe/LeaseBased,
  /// timing-invalid, or `ε_unc ≥ Δ` config must degrade to Safe, not serve). Gates the synchronized-wall
  /// stamp, the precise commit-anchor, AND the inherited-read serve.
  pub(crate) fn failover_tier_active(&self) -> bool {
    self.config.failover_tier_valid()
  }

  /// The FAILOVER inherited-read offer: while this freshly elected leader holds the post-election
  /// commit-wait under the LeaseGuard failover tier AND the committed anchor's lease is provably live,
  /// `Some({ index: c, limbo_upper })` authorizes the application to serve a linearizable read on the
  /// committed prefix at `c` — the SOLE LeaseGuard serve against a PRIOR-term commit index — instead of
  /// degrading to Safe, PROVIDED it first confirms its key was not written in the limbo region
  /// `(c, limbo_upper]` (the proto stays key-agnostic; the application owns the command format). The
  /// limbo check AND this lease-live offer are together the linearizability substitute for the
  /// current-term-commit gate; the application serves at `c` once `applied >= c`.
  ///
  /// `None` once the commit-wait lifts (the leader then serves normally), off the failover tier, when the
  /// inherited serve was not ARMED this term (the E′-inflated commit-wait would not fit below the
  /// election timeout — see `become_leader`), when the anchor lease has expired,
  /// or when this node is POISONED — in every such case the application must fall back to a normal read.
  /// Pure read-only observer (`&self`, no log): the anchors were captured at `become_leader`.
  pub fn failover_read_window(
    &self,
    now: impl Into<crate::Now>,
  ) -> Option<crate::FailoverReadWindow> {
    // A poisoned node has declared itself untrustworthy (it suppresses all messages/events and rejects
    // the normal read/propose paths); it must never advertise a serve window either. `poison` leaves
    // `role`/`commit_wait_until`/the anchors intact, so this guard is load-bearing, not redundant.
    // `inherited_serve_armed` (captured at election) folds in BOTH the valid-active-failover-tier check
    // AND that the E′-inflated commit-wait fits below the election timeout — so an unvalidated config or
    // an over-large inherited window degrades to Safe rather than serving.
    if self.poisoned
      || self.role != Role::Leader
      || self.commit_wait_until.is_none()
      || !self.inherited_serve_armed
    {
      return None;
    }
    if !self.inherited_lease_live(now.into()) {
      return None;
    }
    Some(crate::FailoverReadWindow::new(
      self.commit,
      self.limbo_upper,
    ))
  }

  /// Core leader read logic: register the read and broadcast / confirm.
  pub(crate) fn do_leader_read<L: LogStore>(
    &mut self,
    now: crate::Now,
    log: &L,
    context: Bytes,
    from: Option<I>,
  ) {
    let commit = self.commit;
    match self.config.read_only() {
      crate::ReadOnlyOption::Safe => {
        self.do_safe_read(now, context, from);
      }
      crate::ReadOnlyOption::LeaseBased => {
        // Serve from the local commit WITHOUT a round-trip iff the full lease-read invariant holds (see
        // `lease_read_available` — the single source of truth for LeaseBased safety). Otherwise degrade
        // to the Safe heartbeat round, which re-confirms a quorum before emitting; degrading is silent
        // and always safe.
        if self.lease_read_available(now) {
          self.emit_or_reply_read(commit, context, from);
        } else {
          // Degrade to the FULL Safe read path — including the single-node self-quorum fast-path —
          // so a one-voter leader still completes the read immediately instead of waiting forever for
          // a peer that does not exist. Sharing `do_safe_read` keeps the degradation behaviourally
          // identical to the Safe config (the old partial copy only `add_request`'d and
          // broadcast, stranding single-node degraded reads until a term/leadership reset).
          self.do_safe_read(now, context, from);
        }
      }
      crate::ReadOnlyOption::LeaseGuard => {
        // "The log is the lease": serve from the local commit WITHOUT a round-trip iff the leader's
        // most-recent committed entry is still within the lease window on the leader's OWN monotonic
        // clock (see `lease_guard_read_live`). The caller already gated on `has_current_term_commit`,
        // so the anchor is a current-term entry THIS leader stamped — anchor and `now` share one
        // clock, no skew assumption; the cross-leader safety (a fresh leader cannot serve before a
        // deposed lease expires) is upheld by the commit-wait. A stale lease (idle / read-only
        // workload) degrades to the always-safe heartbeat round — never a stale serve.
        if self.lease_guard_read_live(now, log) {
          self.emit_or_reply_read(commit, context, from);
        } else {
          // Stale lease: degrade THIS read to the always-safe heartbeat round, and (only under an
          // active LeaseGuard config) record a refresh demand. The leader's next heartbeat tick appends
          // ONE stamped no-op to re-commit and re-stamp the lease, so subsequent reads serve fast again
          // — fixing the post-election "lease dead on arrival" and read-only-workload staleness without
          // ever changing the read gate or commit-wait. (An inactive/invalid config or a poisoned read
          // also lands here; `leaseguard_timing().is_some()` keeps the demand to genuine staleness.)
          if self.leaseguard_timing().is_some() {
            self.lease_refresh_wanted = true;
          }
          self.do_safe_read(now, context, from);
        }
      }
    }
  }

  /// The Safe linearizable-read confirmation path: register the read against the heartbeat-ack
  /// tracker, then either confirm immediately when the leader's own self-ack already wins quorum (a
  /// single-voter cluster has no peers to answer) or broadcast a heartbeat round to gather acks.
  ///
  /// Shared by the `Safe` read-only config AND the `LeaseBased` degradation fallback so single-node
  /// completion holds for both: the lease-unavailable fallback MUST run the self-quorum fast-path,
  /// not merely register-and-broadcast, or a one-voter leader's read would never emit `ReadState`.
  pub(crate) fn do_safe_read(&mut self, now: crate::Now, context: Bytes, from: Option<I>) {
    let me = self.config.id();
    let commit = self.commit;
    // Register the read and seed the heartbeat round with its INTERNAL token (not the user
    // `context`): the token is unique per round, so a stale/duplicated HeartbeatResp echoing an
    // earlier round's token can never confirm this read — the linearizability hazard when a user
    // reuses a `context` after an earlier read with it completed.
    let round = self.read_only.add_request(commit, context, from, me);
    // Single-node cluster fast-path: self-ack is already a quorum.
    let single_node_quorum = {
      let acks = self
        .read_only
        .acks_for(round.as_ref())
        .cloned()
        .unwrap_or_default();
      let votes: BTreeMap<I, bool> = self
        .tracker
        .ids()
        .into_iter()
        .filter(|id| self.tracker.is_voter(id))
        .map(|id| (id, acks.contains(&id)))
        .collect();
      self.tracker.vote_result(&votes).is_won()
    };
    if single_node_quorum {
      let confirmed = self.read_only.advance(round.as_ref());
      let (term, me2) = (self.term, me);
      for st in confirmed {
        let (context, req_from, index) = st.into_parts();
        match req_from {
          None => {
            self.emit_read_state(index, context);
          }
          Some(follower) => {
            self.send(
              follower,
              Message::ReadIndexResp(crate::ReadIndexResp::new(term, me2, index, context, false)),
            );
          }
        }
      }
    } else {
      self.broadcast_heartbeat_with_ctx(now, round);
    }
  }

  /// Initiate a linearizable read.
  ///
  /// The `context` correlates this request with the eventual [`Event::ReadState`](crate::Event::ReadState)
  /// (locally) or [`ReadIndexResp`](crate::ReadIndexResp) (when forwarded), so it should identify the
  /// read uniquely AMONG IN-FLIGHT reads: reusing a `context` that is already in flight (including the
  /// **empty** context for two concurrent reads) returns [`crate::ReadIndexError::DuplicateContext`],
  /// since the prior read's single confirmation would otherwise be the only acknowledgement for both.
  /// Reuse AFTER a prior read with the same context has completed is safe: the leader's heartbeat-quorum
  /// proof keys on an internal, never-reused round token (not the `context`), so a stale or duplicated
  /// `HeartbeatResp` from the earlier read can never confirm the later one.
  ///
  /// `Ok(())` means the read was accepted onto a confirmation path; the caller should wait for
  /// the matching `ReadState`/`ReadIndexResp`. An `Err` means **no** acknowledgement will ever
  /// arrive for this call, so the caller must not block on one.
  ///
  /// - **Leader, `ReadOnlySafe`:** records the read at the current commit index and
  ///   broadcasts a heartbeat round.  Once a voter quorum acks the round, emits
  ///   `Event::ReadState`.  If no current-term commit exists yet, defers until it does.
  /// - **Leader, `ReadOnlyLeaseBased`:** confirms immediately from `commit` when
  ///   `check_quorum` is also enabled (relies on the CheckQuorum lease).  If
  ///   `check_quorum` is disabled the request degrades to the Safe path so the
  ///   misconfiguration is safe rather than silently non-linearizable.
  /// - **Follower:** forwards a `ReadIndex` message to the known leader.  Returns
  ///   [`crate::ReadIndexError::NoLeader`] if no leader is known, or
  ///   [`crate::ReadIndexError::ForwardingDisabled`] if `disable_proposal_forwarding` is set.
  /// - **Candidate / PreCandidate:** returns [`crate::ReadIndexError::NoLeader`] (no leader to confirm).
  ///
  /// A poisoned node returns `Ok(())` without effect (it is inert; the driver should already be
  /// stopping on `poison_reason()`).
  pub fn read_index<L, S>(
    &mut self,
    now: impl Into<Now>,
    log: &L,
    _stable: &S,
    context: Bytes,
  ) -> Result<(), crate::ReadIndexError>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let now: crate::Now = now.into();
    // A poisoned node suppresses `poll_event`, so no `ReadState` can ever be emitted. Returning
    // `Ok(())` here would violate the `read_index` contract ("accepted onto a confirmation path"):
    // the promised acknowledgement never arrives and the caller blocks forever. Reject up front,
    // before any state change, so the caller learns no confirmation is coming.
    if self.poisoned {
      return Err(crate::ReadIndexError::Poisoned);
    }
    match self.role {
      Role::Leader => {
        // Reject a context that is already in flight (deferred or registered) so the caller
        // is not left waiting forever for a confirmation that the prior read already owns.
        if self.read_context_in_flight(&context) {
          return Err(crate::ReadIndexError::DuplicateContext);
        }
        // Leader-side read back-pressure: a partitioned leader (no current-term commit, or no
        // heartbeat-ack quorum) must not accumulate reads without bound. Cap the combined in-flight
        // backlog — deferred (`pending_reads`) plus confirming (`read_only`) — and reject beyond it.
        if self.leader_reads_at_capacity() {
          return Err(crate::ReadIndexError::TooManyInFlight);
        }
        // Current-term-commit gate.
        if !self.has_current_term_commit(log) {
          // Defer until the no-op commits.
          self.pending_reads.push((context, None));
          return Ok(());
        }
        self.do_leader_read(now, log, context, None);
        Ok(())
      }
      Role::Follower => {
        // Forward to the leader if known and forwarding is not disabled.
        if self.config.disable_proposal_forwarding() {
          return Err(crate::ReadIndexError::ForwardingDisabled);
        }
        let Some(leader) = self.leader else {
          return Err(crate::ReadIndexError::NoLeader);
        };
        // Follower-side duplicate-context guard (mirror of the leader's `read_context_in_flight`):
        // a context already forwarded and awaiting its `ReadIndexResp` owns the completion path;
        // reject the duplicate rather than forward it again (unbounded re-forward / silent coalesce).
        if self.forwarded_reads.contains_context(&context) {
          return Err(crate::ReadIndexError::DuplicateContext);
        }
        // Back-pressure at capacity: reject the NEW read rather than evict an already-accepted one
        // (eviction would strand the evicted read and let a reused context complete the wrong one).
        if self.forwarded_reads.is_full() {
          return Err(crate::ReadIndexError::TooManyInFlight);
        }
        // Record before forwarding and forward by the INTERNAL token, NOT the user context: the leader
        // echoes whatever we send as the `ReadIndexResp` context, so correlating on a unique token
        // means a stale/duplicated response from an earlier forward (even of the same user context)
        // cannot complete a later read. `read_index` already returned early if poisoned, so this never
        // desyncs from the suppressed `send` below.
        let token = self.forwarded_reads.push(context);
        let (term, me) = (self.term, self.config.id());
        self.send(
          leader,
          Message::ReadIndex(crate::ReadIndex::new(term, me, token)),
        );
        Ok(())
      }
      Role::Candidate | Role::PreCandidate => {
        // No leader to confirm reads.
        Err(crate::ReadIndexError::NoLeader)
      }
    }
  }

  /// Whether a LOCAL (leader-application) read with this exact `context` is already in flight on the
  /// leader — either deferred awaiting the first current-term commit (`pending_reads`) or registered
  /// with the heartbeat-ack tracker (`read_only`). Used by [`Self::read_index`]'s leader path to
  /// surface [`crate::ReadIndexError::DuplicateContext`] before any side effect. FORWARDED reads are
  /// EXCLUDED: their stored `context` is the forwarding follower's per-follower token (a different
  /// namespace that collides across followers, each starting at 0), and the follower owns its own
  /// user-context dedup.
  pub(crate) fn read_context_in_flight(&self, context: &Bytes) -> bool {
    self
      .pending_reads
      .iter()
      .any(|(ctx, from)| from.is_none() && ctx == context)
      || self.read_only.context_in_flight(context.as_ref())
  }

  /// Whether the leader's combined in-flight read backlog (deferred `pending_reads` + confirming
  /// `read_only`) has reached [`MAX_LEADER_READS`]. A read is in one or the other, never both, so
  /// their sum is the live count.
  pub(crate) fn leader_reads_at_capacity(&self) -> bool {
    self.pending_reads.len() + self.read_only.len() >= MAX_LEADER_READS
  }

  /// Leader receives a forwarded `ReadIndex` from a follower.
  pub(crate) fn on_read_index<L: LogStore, S: StableStore<NodeId = I>>(
    &mut self,
    now: crate::Now,
    log: &L,
    _stable: &S,
    ri: crate::ReadIndex<I>,
  ) {
    if !self.role.is_leader() {
      return;
    }
    // `ri.context()` is the forwarding follower's per-read TOKEN (not a user context); the leader keeps
    // it opaque and echoes it in the `ReadIndexResp` so the follower can correlate.
    let context = Bytes::copy_from_slice(ri.context());
    let from = ri.from();
    // No leader-side duplicate-context guard on the forwarded path: the FORWARDING FOLLOWER owns the
    // dedup of its own user contexts and sends a unique per-read token, and the leader's read tracker
    // keys on its OWN round token, so distinct forwards never collide even when followers reuse token
    // VALUES (each follower's token sequence starts at 0). A network-duplicated `ReadIndex` is harmless:
    // the leader confirms it again, but the follower's token-keyed `forwarded_reads` drops the redundant
    // `ReadIndexResp`. Unbounded growth is bounded by the capacity check below, not by a dedup.
    // Leader-side read back-pressure (same bound as the local path): at capacity we decline the
    // forwarded read rather than grow the backlog without limit. We MUST tell the follower so it can
    // clear its `forwarded_reads` entry — a bare drop would strand that entry until an unrelated
    // term/leader change, leaving the originator blocked forever and the follower's slot consumed
    // (eventually failing later reads with `TooManyInFlight`). The rejecting reply carries no usable
    // index (`Index::ZERO`); the follower re-issues once the leader drains.
    if self.leader_reads_at_capacity() {
      let (term, me) = (self.term, self.config.id());
      self.send(
        from,
        Message::ReadIndexResp(crate::ReadIndexResp::new(
          term,
          me,
          Index::ZERO,
          context,
          true,
        )),
      );
      return;
    }
    // Current-term-commit gate (same as the local path).
    if !self.has_current_term_commit(log) {
      self.pending_reads.push((context, Some(from)));
      return;
    }
    self.do_leader_read(now, log, context, Some(from));
  }

  /// The single `ReadState`-emission choke-point. A poisoned node must NOT complete a read: its
  /// commit/applied view is no longer trustworthy, so confirming a linearizable read against it
  /// would hand the application a stale-or-wrong index. Every `Event::ReadState` push — the local
  /// leader read (Safe single-node and quorum-confirmed paths, LeaseBased) and the follower's
  /// validated `ReadIndexResp` completion — routes through here so the poison check lives in one
  /// place. Mirrors `send`'s central emit-halt for the event channel.
  pub(crate) fn emit_read_state(&mut self, index: Index, context: Bytes) {
    if self.poisoned {
      return;
    }
    self
      .events
      .push_back(crate::Event::ReadState(crate::ReadState::new(
        index, context,
      )));
  }

  /// Follower receives a `ReadIndexResp` from the leader.
  ///
  /// Only a FOLLOWER awaiting THIS forwarded read, from its CURRENT leader, may complete it: an
  /// unsolicited / stale / wrong-leader / already-completed response is rejected without emitting a
  /// `ReadState`. Without the membership check, a spoofed or duplicate resp could complete a read the
  /// node never forwarded (or re-complete one it already did), surfacing a confirmation the
  /// application would treat as linearizable. The response's correlator is the follower's INTERNAL
  /// token (echoed by the leader), NOT the user context, so a stale/duplicated response from an
  /// earlier forward — even of a since-reused user context — finds no matching in-flight read and is
  /// dropped. `remove_by_token` doubles as the already-completed guard: `None` once consumed.
  pub(crate) fn on_read_index_resp(&mut self, from: I, resp: crate::ReadIndexResp<I>) {
    let token = resp.context();
    // Only a follower awaiting a forward from its CURRENT leader may complete it, and the leader is
    // identified by the ENVELOPE sender `from` (the transport peer) — never the self-reported
    // `resp.from()`, which a wrong peer could forge to the leader's id. Membership is
    // checked BEFORE consuming the token, so a spoofed / wrong-leader response never clears a real
    // in-flight slot.
    if self.role != Role::Follower || self.leader != Some(from) || resp.from() != from {
      return;
    }
    // `remove_by_token` is the authoritative clear of the in-flight slot AND the already-completed /
    // stale guard: `None` rejects an unsolicited / stale / already-completed token. It runs for BOTH
    // outcomes — a rejecting response (leader at read back-pressure capacity) clears the strand exactly
    // like a confirming one, but must NOT emit a `ReadState` (its `index` is meaningless). Clearing
    // here lets the originator re-issue the same user context (it is no longer a duplicate).
    let Some(context) = self.forwarded_reads.remove_by_token(token) else {
      return;
    };
    if resp.reject() {
      return;
    }
    self.emit_read_state(resp.index(), context);
  }
}
