//! The Sans-I/O Raft core. M0 is a no-op skeleton: it owns state and exposes the
//! `handle_*`/`poll_*` surface. M1 fills in leader election. M2 adds log replication.
use crate::{
  Config, Event, Index, Instant, LogStore, Message, NodeId, Outgoing, Prng, StableStore,
  StateMachine, Term,
};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

/// The role of a node in its current term.
#[derive(Debug, Clone, Copy, PartialEq, Eq, derive_more::Display, derive_more::IsVariant)]
#[display("{}", self.as_str())]
pub enum Role {
  /// Replicates from a leader; starts an election on timeout.
  Follower,
  /// Probing for votes before incrementing the term (PreVote).
  PreCandidate,
  /// Standing for election in the current term.
  Candidate,
  /// Replicating to followers.
  Leader,
}

impl Role {
  /// The stable snake_case name.
  #[inline(always)]
  pub const fn as_str(&self) -> &'static str {
    match self {
      Self::Follower => "follower",
      Self::PreCandidate => "pre_candidate",
      Self::Candidate => "candidate",
      Self::Leader => "leader",
    }
  }
}

/// What the core owes once a storage write completes.
#[derive(Debug, Clone, Copy)]
enum Pending<I> {
  /// Emit `VoteResp(grant)` to `to` once the term+vote write is durable.
  /// `term` records the term at which the vote was cast so stale completions can be
  /// detected and dropped if the term has since advanced.
  CastVote { to: I, term: crate::Term },
  /// Emit `AppendResp(success, match_index)` to `to` once the log append is durable.
  FollowerAck { to: I, match_index: Index },
  /// Advance the leader's own `match_index` to `upto` (and re-check commit) once durable.
  LeaderAppend { upto: Index },
}

/// The Sans-I/O Raft state machine for one node.
///
/// `I` is unbounded on the struct; `I: NodeId` belongs only on the `impl` blocks that
/// need it. `F: StateMachine` is the documented "bounds that gate storage shape" exception
/// (§8): the struct stores `Event<I, F::Response>`, which cannot be named without it.
#[derive(Debug)]
pub struct Endpoint<I, F>
where
  F: StateMachine,
{
  config: Config<I>,
  fsm: F,
  role: Role,
  term: Term,
  voted_for: Option<I>,
  leader: Option<I>,
  commit: Index,
  applied: Index,
  prng: Prng,
  votes_granted: BTreeSet<I>,
  election_deadline: Option<Instant>,
  heartbeat_deadline: Option<Instant>,
  outgoing: VecDeque<Outgoing<I>>,
  events: VecDeque<Event<I, F::Response>>,
  progress: BTreeMap<I, crate::Progress>,
  /// Monotonically minted id for every storage submission.
  next_op_id: crate::OpId,
  /// Outstanding write → deferred action.
  pending: BTreeMap<crate::OpId, Pending<I>>,
  /// Sticky fatal error: once set, all `handle_*` are no-ops.
  poisoned: bool,
  /// In-flight snapshot write: `(opid, up_to)`. Compaction is deferred until the snapshot
  /// is durable (crash-safe: we never compact before the snapshot write completes).
  ///
  /// Completion contract: this field relies on the `StableStore` guarantee that every
  /// `submit_snapshot` call eventually yields a `SnapshotWritten` completion. If that
  /// completion never arrives (e.g. a store implementation that silently drops snapshots),
  /// the `is_some()` guard in `maybe_snapshot` would permanently block future snapshots
  /// (the node wedges). A store error poisons the node via `handle_storage`, and `restart`
  /// resets this field to `None`.
  pending_compact: Option<(crate::OpId, Index)>,
}

// ─── Pure-accessor / construction impl (no `F::Command` bound needed) ───────────────────────────

impl<I, F> Endpoint<I, F>
where
  I: NodeId,
  F: StateMachine,
{
  /// Create a fresh node (status Follower, term 0, empty log view).
  /// Arms the election timer immediately.
  pub fn new(config: Config<I>, now: Instant, seed: u64, fsm: F) -> Self {
    let mut ep = Self {
      config,
      fsm,
      role: Role::Follower,
      term: Term::ZERO,
      voted_for: None,
      leader: None,
      commit: Index::ZERO,
      applied: Index::ZERO,
      prng: Prng::new(seed),
      votes_granted: BTreeSet::new(),
      election_deadline: None,
      heartbeat_deadline: None,
      outgoing: VecDeque::new(),
      events: VecDeque::new(),
      progress: BTreeMap::new(),
      next_op_id: crate::OpId::ZERO,
      pending: BTreeMap::new(),
      poisoned: false,
      pending_compact: None,
    };
    ep.arm_election_timer(now);
    ep
  }

  /// This node's id.
  #[inline(always)]
  pub const fn id(&self) -> I {
    self.config.id()
  }

  /// The current role.
  #[inline(always)]
  pub const fn role(&self) -> Role {
    self.role
  }

  /// The current term.
  #[inline(always)]
  pub const fn term(&self) -> Term {
    self.term
  }

  /// The believed leader, if any.
  #[inline(always)]
  pub const fn leader(&self) -> Option<I> {
    self.leader
  }

  /// The application state machine (read-only access for agreement checks).
  #[inline]
  pub const fn state_machine(&self) -> &F {
    &self.fsm
  }

  /// Next outbound message, if any.
  #[inline]
  pub fn poll_message(&mut self) -> Option<Outgoing<I>> {
    self.outgoing.pop_front()
  }

  /// Next application event, if any.
  #[inline]
  pub fn poll_event(&mut self) -> Option<Event<I, F::Response>> {
    self.events.pop_front()
  }

  /// The earliest armed deadline (election for followers/candidates, heartbeat for leaders).
  #[inline]
  pub fn poll_timeout(&self) -> Option<Instant> {
    match self.role {
      Role::Leader => self.heartbeat_deadline,
      _ => self.election_deadline,
    }
  }

  /// Mint a unique, monotonically-increasing operation id for a storage submission.
  fn mint_op_id(&mut self) -> crate::OpId {
    let id = self.next_op_id;
    self.next_op_id = self.next_op_id.next();
    id
  }

  /// Enter the permanent failed state (a fatal storage/apply error). Every subsequent
  /// `handle_*` becomes a no-op; the driver should surface this and stop.
  fn poison(&mut self) {
    self.poisoned = true;
  }

  /// Whether this node has hit an unrecoverable error.
  #[inline(always)]
  pub const fn is_poisoned(&self) -> bool {
    self.poisoned
  }

  // --- PRIVATE HELPERS (no Data bound) ---

  fn arm_election_timer(&mut self, now: Instant) {
    let t = self.prng.election_timeout(self.config.election_timeout());
    self.election_deadline = Some(now + t);
    self.heartbeat_deadline = None;
  }

  fn arm_heartbeat_timer(&mut self, now: Instant) {
    self.heartbeat_deadline = Some(now + self.config.heartbeat_interval());
    self.election_deadline = None;
  }

  fn send(&mut self, to: I, msg: Message<I>) {
    self.outgoing.push_back(Outgoing::new(to, msg));
  }

  fn peers(&self) -> impl Iterator<Item = I> + '_ {
    let me = self.config.id();
    self
      .config
      .voters()
      .iter()
      .copied()
      .filter(move |&p| p != me)
  }

  fn last_log(&self, log: &impl LogStore) -> (Index, Term) {
    let li = log.last_index();
    let lt = log.term(li).unwrap_or(Term::ZERO);
    (li, lt)
  }

  /// Build the current `ConfState` from the config voter set.
  fn conf_state(&self) -> crate::ConfState<I> {
    crate::ConfState::from_voters(self.config.voters().iter().copied())
  }

  /// Expose `pending_compact` for testing.
  #[cfg(test)]
  pub(crate) fn pending_compact(&self) -> Option<(crate::OpId, Index)> {
    self.pending_compact
  }

  fn broadcast_heartbeat(&mut self, _now: Instant) {
    let (term, me) = (self.term, self.config.id());
    for peer in self.peers().collect::<std::vec::Vec<_>>() {
      // Clamp the advertised commit to this peer's known match index. A heartbeat carries
      // no prev-log check, so the follower can only safely commit up to the prefix it has
      // proven (via a consistency-checked AppendEntries) matches ours. Telling a peer to
      // commit past its match index lets a freshly-restarted node with a divergent,
      // uncommitted tail commit+apply a stale entry (the etcd `min(committed, pr.Match)`
      // rule). Default to ZERO if progress is unknown.
      let peer_commit = self
        .progress
        .get(&peer)
        .map(|pr| core::cmp::min(self.commit, pr.match_index()))
        .unwrap_or(Index::ZERO);
      self.send(
        peer,
        Message::Heartbeat(crate::Heartbeat::new(
          term,
          me,
          peer_commit,
          bytes::Bytes::new(),
        )),
      );
    }
  }

  /// Byte size of one entry (data length only — the transport framing adds its own overhead
  /// but we use data bytes as the cap unit, matching etcd's `limitSize` convention).
  #[inline(always)]
  fn entry_size(e: &crate::Entry) -> u64 {
    e.data().len() as u64
  }

  fn maybe_send_append<L: LogStore, S: StableStore<NodeId = I>>(
    &mut self,
    peer: I,
    log: &L,
    stable: &S,
  ) {
    let Some(pr) = self.progress.get(&peer).cloned() else {
      return;
    };
    // M4 Task 4: respect the in-flight window — if paused, don't send.
    if pr.is_paused() {
      return;
    }

    // M5 Task 5: if the entries this peer needs have been compacted into a snapshot
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
        if let Some(p) = self.progress.get_mut(&peer) {
          p.become_snapshot(pending);
        }
      }
      // No snapshot persisted yet → nothing to send; retry later.
      return;
    }

    let next = pr.next_index();
    let prev_index = Index::new(next.get().saturating_sub(1));
    let prev_term = if prev_index == Index::ZERO {
      Term::ZERO
    } else {
      log.term(prev_index).unwrap_or(Term::ZERO)
    };
    let end = log.last_index().next();
    let all_entries = if next < end {
      log
        .entries(next..end, u64::MAX)
        .map(<[_]>::to_vec)
        .unwrap_or_default()
    } else {
      std::vec::Vec::new()
    };

    // M4 Task 4: cap at max_size_per_msg bytes, but always send at least one entry.
    let max_bytes = self.config.max_size_per_msg();
    let entries = if all_entries.is_empty() || max_bytes == u64::MAX {
      all_entries
    } else {
      let mut budget = max_bytes;
      let mut count = 0usize;
      for e in &all_entries {
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
      all_entries[..count].to_vec()
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

    // M4 Task 4: record the send so the window tracks in-flight messages.
    // For Probe: only pause when we sent a partial batch (byte-capped); a full send leaves
    // nothing to throttle and pausing would stall subsequent proposes unnecessarily.
    // For Replicate: only record non-empty sends — an empty AppendEntries (heartbeat probe
    // for a caught-up peer) must NOT consume an inflight slot. Empty sends carry no entries
    // so there is nothing for the peer to ack; the slot would never be freed, and after
    // max_inflight_msgs heartbeat-resp cycles the window fills and newly proposed entries
    // are silently not delivered. (etcd guards SentEntries on len(entries) > 0.)
    let is_empty = bytes_sent == 0 && entries_len == 0;
    if let Some(p) = self.progress.get_mut(&peer) {
      if (!is_empty && p.state().is_replicate()) || sent_partial {
        p.sent_entries(last_sent, bytes_sent);
      }
    }
  }

  fn maybe_advance_commit<L: LogStore>(&mut self, log: &L) {
    let mut matches: std::vec::Vec<Index> = self
      .progress
      .values()
      .map(crate::Progress::match_index)
      .collect();
    matches.sort_unstable();
    // highest index replicated on >= quorum nodes
    let q = self.config.quorum();
    if matches.len() < q {
      return;
    }
    let candidate = matches[matches.len() - q];
    // §5.4.2: only commit an entry from the CURRENT term by counting replicas.
    let current_term = log.term(candidate).map(|t| t == self.term).unwrap_or(false);
    if candidate > self.commit && current_term {
      self.commit = candidate;
    }
  }

  fn on_request_vote<L: LogStore, S: StableStore<NodeId = I>>(
    &mut self,
    now: Instant,
    log: &mut L,
    stable: &mut S,
    rv: crate::RequestVote<I>,
  ) {
    let (my_index, my_term) = self.last_log(log);
    let log_ok = (rv.last_log_term(), rv.last_log_index()) >= (my_term, my_index);
    let can_vote = self.voted_for.is_none() || self.voted_for == Some(rv.candidate());
    if can_vote && log_ok {
      self.voted_for = Some(rv.candidate());
      self.arm_election_timer(now);
      // Persist (term, vote); the VoteResp(grant) is owed once the write is DURABLE.
      let opid = self.mint_op_id();
      let hs = stable
        .hard_state()
        .with_term(self.term)
        .with_vote(self.voted_for);
      stable.submit_write(opid, hs);
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

  fn on_vote_resp<L: LogStore, S: StableStore<NodeId = I>>(
    &mut self,
    now: Instant,
    log: &mut L,
    stable: &mut S,
    vr: crate::VoteResp<I>,
  ) where
    F::Command: crate::Data,
  {
    if !self.role.is_candidate() || vr.term() != self.term {
      return;
    }
    if !vr.reject() {
      self.votes_granted.insert(vr.from());
      if self.votes_granted.len() >= self.config.quorum() {
        self.become_leader(now, log, stable);
      }
    }
  }
}

// ─── Full replication impl (F::Command: Data required for apply_committed) ──────────────────────

impl<I, F> Endpoint<I, F>
where
  I: NodeId,
  F: StateMachine,
  F::Command: crate::Data,
{
  /// Drain storage completions. (M3+: append-before-ack / persist-vote.)
  pub fn handle_storage<L, S>(&mut self, _now: Instant, log: &mut L, stable: &mut S)
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
    F::Snapshot: crate::Data,
  {
    if self.poisoned {
      return;
    }
    while let Some(done) = log.poll() {
      match done {
        Ok(crate::LogDone::Appended(opid)) => self.on_log_appended(log, opid),
        Ok(crate::LogDone::Compacted(_)) => {}
        Err(_) => {
          self.poison();
          return;
        }
      }
    }
    while let Some(done) = stable.poll() {
      match done {
        Ok(crate::StableDone::Wrote(opid)) => self.on_stable_wrote(opid),
        Ok(crate::StableDone::SnapshotWritten(opid)) => {
          // Deferred compaction: fire only after the snapshot is durable.
          // This mirrors append-before-ack: the log is never compacted before the
          // snapshot backing it is safely on stable storage.
          if let Some((pid, up_to)) = self.pending_compact {
            if pid == opid {
              log.compact(up_to);
              self.pending_compact = None;
            }
          }
        }
        Err(_) => {
          self.poison();
          return;
        }
      }
    }
    // After all completions are drained, check whether a new snapshot is warranted.
    self.maybe_snapshot(log, stable);
  }

  /// Trigger a snapshot if `applied - first_index >= snapshot_threshold`.
  ///
  /// Durability rule: the snapshot is persisted first via `submit_snapshot`; the log is
  /// compacted only after `SnapshotWritten` is received in `handle_storage`. This mirrors
  /// append-before-ack and ensures a crash after compaction but before snapshot durability
  /// cannot lose data.
  fn maybe_snapshot<L, S>(&mut self, log: &L, stable: &mut S)
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
    F::Snapshot: crate::Data,
  {
    if self.pending_compact.is_some() {
      // A snapshot is already being persisted; don't start another.
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
        self.poison();
        return;
      }
    };
    use crate::Data as _;
    let mut data = std::vec::Vec::new();
    snap.encode(&mut data);
    let last_term = match log.term(self.applied) {
      Ok(t) => t,
      Err(_) => {
        self.poison();
        return;
      }
    };
    let meta = crate::SnapshotMeta::new(self.applied, last_term, self.conf_state());
    let opid = self.mint_op_id();
    stable.submit_snapshot(opid, meta, bytes::Bytes::from(data));
    // Defer compaction until SnapshotWritten fires.
    self.pending_compact = Some((opid, self.applied));
  }

  /// Rebuild a node from durable storage after a crash. If a durable snapshot exists,
  /// restores the state machine from it first, then replays only the post-snapshot
  /// committed tail `(snapshot.last_index .. commit]`. Without a snapshot, replays the
  /// full committed log from index 1 (the M3 behavior). Returns in `Follower` with the
  /// election timer armed.
  ///
  /// A corrupt durable snapshot poisons the node (no partial state is applied).
  pub fn restart<L, S>(
    config: Config<I>,
    now: Instant,
    seed: u64,
    fsm: F,
    log: &mut L,
    stable: &mut S,
  ) -> Self
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
    F::Snapshot: crate::Data,
  {
    let hs = stable.hard_state();
    let mut fsm = fsm;
    let mut applied = Index::ZERO;
    let mut poisoned = false;
    // Restore from a durable snapshot first: the compacted log no longer holds entries
    // <= meta.last_index, so the SM baseline comes from the snapshot; we then replay only
    // the durable post-snapshot committed tail.
    if let Some((meta, data)) = stable.snapshot() {
      match <F::Snapshot as crate::Data>::decode(&data) {
        Ok((_, snap)) => {
          if fsm.restore(snap).is_err() {
            poisoned = true;
          } else {
            applied = meta.last_index();
            // M6 installs meta.conf() (dynamic membership); M5 has fixed config voters.
          }
        }
        Err(_) => poisoned = true,
      }
    }
    // Never trust commit beyond the durable log; never below the snapshot baseline.
    let commit = core::cmp::min(hs.commit(), log.last_index()).max(applied);
    let mut ep = Self {
      config,
      fsm,
      role: Role::Follower,
      term: hs.term(),
      voted_for: hs.vote(),
      leader: None,
      commit,
      applied,
      prng: Prng::new(seed),
      votes_granted: BTreeSet::new(),
      election_deadline: None,
      heartbeat_deadline: None,
      next_op_id: crate::OpId::ZERO,
      pending: BTreeMap::new(),
      poisoned,
      pending_compact: None,
      progress: BTreeMap::new(),
      outgoing: VecDeque::new(),
      events: VecDeque::new(),
    };
    // Replay the durable committed tail (applied..commit] into the restored SM. Skip if the
    // snapshot restore failed (the SM is in an unknown state and the node is poisoned).
    if !ep.poisoned {
      ep.apply_committed(log);
    }
    ep.events.clear();
    ep.arm_election_timer(now);
    ep
  }

  #[cfg(test)]
  pub(crate) fn mint_op_id_for_test(&mut self) -> crate::OpId {
    self.mint_op_id()
  }

  fn on_log_appended<L: LogStore>(&mut self, log: &mut L, opid: crate::OpId) {
    match self.pending.remove(&opid) {
      Some(Pending::FollowerAck { to, match_index }) => {
        let (term, me) = (self.term, self.config.id());
        self.send(
          to,
          Message::AppendResp(crate::AppendResp::new(
            term,
            me,
            false,
            Index::ZERO,
            Term::ZERO,
            match_index,
          )),
        );
      }
      // Role-gate (defense-in-depth): only a current leader advances its own match index
      // and commit. `pending` is cleared on every term change, so a stale `LeaderAppend`
      // reaching a non-leader is already unreachable — this makes the safety local.
      Some(Pending::LeaderAppend { upto }) if self.role.is_leader() => {
        if let Some(p) = self.progress.get_mut(&self.config.id()) {
          p.maybe_update(upto);
        }
        self.maybe_advance_commit(log);
        self.apply_committed(log);
      }
      _ => {} // CastVote completes via stable; unknown/superseded opid → ignore
    }
  }

  fn on_stable_wrote(&mut self, opid: crate::OpId) {
    if let Some(Pending::CastVote { to, term }) = self.pending.remove(&opid) {
      // Only emit the grant if the term hasn't changed and we still hold the vote for `to`.
      // If either condition is false the write was superseded by a term advance; drop silently.
      if term == self.term && self.voted_for == Some(to) {
        debug_assert!(
          self.voted_for == Some(to),
          "releasing a CastVote we no longer hold"
        );
        let me = self.config.id();
        self.send(
          to,
          Message::VoteResp(crate::VoteResp::new(term, me, false, false)),
        );
      }
    }
  }

  /// Feed an inbound message. Runs the universal term pre-pass then dispatches.
  pub fn handle_message<L, S>(
    &mut self,
    now: Instant,
    log: &mut L,
    stable: &mut S,
    from: I,
    msg: Message<I>,
  ) where
    L: LogStore,
    S: StableStore<NodeId = I>,
    F::Snapshot: crate::Data,
  {
    if self.poisoned {
      return;
    }
    // Universal term handling (Raft §5.1): a higher term forces us to a follower.
    if msg.term() > self.term {
      self.term = msg.term();
      self.role = Role::Follower;
      self.voted_for = None;
      self.leader = None;
      // All pending work from the old term is now stale (spec §7). Drop it before any new
      // grant is recorded below — a fresh CastVote added by on_request_vote will survive.
      self.pending.clear();
      // Persist the new term and cleared vote. Stepping down owes no ack, so no Pending entry.
      let opid = self.mint_op_id();
      let hs = stable.hard_state().with_term(self.term).with_vote(None);
      stable.submit_write(opid, hs);
    }
    // Drop messages from a stale term (a CheckQuorum nudge is added in M7).
    if msg.term() < self.term {
      return;
    }
    #[allow(unreachable_patterns)] // `_ => {}` is a forward-compat guard for M7 variants
    match msg {
      Message::RequestVote(rv) => self.on_request_vote(now, log, stable, rv),
      Message::VoteResp(vr) => self.on_vote_resp(now, log, stable, vr),
      Message::Heartbeat(hb) => self.on_heartbeat(now, log, hb),
      Message::AppendEntries(ae) => self.on_append_entries(now, log, ae),
      Message::AppendResp(r) => self.on_append_resp(now, log, stable, from, r),
      Message::HeartbeatResp(_) => self.on_heartbeat_resp(from, log, stable),
      Message::InstallSnapshot(is) => self.on_install_snapshot(now, log, stable, is),
      Message::SnapshotResp(r) => self.on_snapshot_resp(now, log, stable, from, r),
      _ => {}
    }
  }

  /// Fire due timers (election for followers/candidates, heartbeat for leaders).
  pub fn handle_timeout<L, S>(&mut self, now: Instant, log: &mut L, stable: &mut S)
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    if self.poisoned {
      return;
    }
    match self.role {
      Role::Leader => {
        if self.heartbeat_deadline.is_some_and(|d| d <= now) {
          self.broadcast_heartbeat(now);
          self.arm_heartbeat_timer(now);
        }
      }
      _ => {
        if self.election_deadline.is_some_and(|d| d <= now) {
          self.become_candidate(now, log, stable);
        }
      }
    }
  }

  /// Propose a command on the leader. Returns the assigned index, or `NotLeader`.
  /// Takes `cmd` by reference (encoding only borrows; the caller keeps it to retry).
  pub fn propose<L, S>(
    &mut self,
    _now: Instant,
    log: &mut L,
    stable: &mut S,
    cmd: &F::Command,
  ) -> Result<Index, crate::ProposeError<I>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    if !self.role.is_leader() {
      return Err(crate::ProposeError::NotLeader {
        leader: self.leader,
      });
    }
    use crate::Data as _;
    let mut buf = std::vec::Vec::new();
    cmd.encode(&mut buf);
    let index = log.last_index().next();
    let entry = crate::Entry::new(
      self.term,
      index,
      crate::EntryKind::Normal,
      bytes::Bytes::from(buf),
    );
    // Self-match advance is deferred until the append is durable (on_log_appended).
    let opid = self.mint_op_id();
    log.submit_append(opid, core::slice::from_ref(&entry));
    self
      .pending
      .insert(opid, Pending::LeaderAppend { upto: index });
    for peer in self.peers().collect::<std::vec::Vec<_>>() {
      self.maybe_send_append(peer, log, stable);
    }
    Ok(index)
  }

  /// Apply all entries that have been committed but not yet applied.
  fn apply_committed<L: LogStore>(&mut self, log: &L) {
    while self.applied < self.commit {
      let idx = self.applied.next();
      let entry = match log.entries(idx..idx.next(), u64::MAX) {
        Ok(s) => match s.first() {
          Some(e) => e.clone(),
          None => break,
        },
        Err(_) => break, // M3: a read error here becomes a sticky fatal error
      };
      match entry.kind() {
        crate::EntryKind::Normal => {
          let cmd = match <F::Command as crate::Data>::decode(entry.data()) {
            Ok((_, c)) => c,
            Err(_) => break, // M3: corrupt-log decode error → sticky fatal
          };
          match self.fsm.apply(idx, cmd) {
            Ok(resp) => self
              .events
              .push_back(crate::Event::Applied(crate::Applied::new(idx, resp))),
            Err(_) => break, // M3: apply error → sticky fatal
          }
        }
        crate::EntryKind::Empty => {} // no-op: just advance applied
        crate::EntryKind::ConfChange => {} // M6: membership applied here
      }
      self.applied = idx;
    }
  }

  fn become_candidate<L: LogStore, S: StableStore<NodeId = I>>(
    &mut self,
    now: Instant,
    log: &mut L,
    stable: &mut S,
  ) {
    self.term = self.term.next();
    // All pending work from the previous term is now stale (spec §7). Clear before recording
    // the self-vote below so old completions that arrive later are harmlessly ignored.
    self.pending.clear();
    self.role = Role::Candidate;
    self.leader = None;
    self.voted_for = Some(self.config.id());
    self.votes_granted.clear();
    self.votes_granted.insert(self.config.id());
    // Persist (term, self-vote). No Pending entry — a candidate doesn't owe an ack.
    let opid = self.mint_op_id();
    let hs = stable
      .hard_state()
      .with_term(self.term)
      .with_vote(self.voted_for);
    stable.submit_write(opid, hs);
    self.arm_election_timer(now);

    let (last_index, last_term) = self.last_log(log);
    let (term, me) = (self.term, self.config.id());
    for peer in self.peers().collect::<std::vec::Vec<_>>() {
      self.send(
        peer,
        Message::RequestVote(crate::RequestVote::new(
          term, me, last_index, last_term, false, false,
        )),
      );
    }
    // single-node cluster: self-vote already a quorum
    if self.votes_granted.len() >= self.config.quorum() {
      self.become_leader(now, log, stable);
    }
  }

  fn become_leader<L: LogStore, S: StableStore<NodeId = I>>(
    &mut self,
    now: Instant,
    log: &mut L,
    stable: &mut S,
  ) {
    self.role = Role::Leader;
    self.leader = Some(self.config.id());
    self.arm_heartbeat_timer(now);

    // Initialize Progress for every voter (self included; self is fully caught up).
    let last = log.last_index();
    self.progress.clear();
    let max_inflight_msgs = self.config.max_inflight_msgs();
    let max_inflight_bytes = self.config.max_inflight_bytes();
    for v in self.config.voters().to_vec() {
      let mut p = crate::Progress::new(last.next(), max_inflight_msgs, max_inflight_bytes);
      if v == self.config.id() {
        p.maybe_update(last);
      }
      self.progress.insert(v, p);
    }

    // Append the new leader's no-op entry (lets it commit prior-term entries, §5.4.2).
    // Self-match advance is deferred until the append is durable (on_log_appended).
    let noop_index = last.next();
    let noop = crate::Entry::new(
      self.term,
      noop_index,
      crate::EntryKind::Empty,
      bytes::Bytes::new(),
    );
    let opid = self.mint_op_id();
    log.submit_append(opid, core::slice::from_ref(&noop));
    self
      .pending
      .insert(opid, Pending::LeaderAppend { upto: noop_index });

    self
      .events
      .push_back(crate::Event::LeaderChanged(crate::LeaderChanged::new(
        self.term,
        Some(self.config.id()),
      )));

    // Broadcast heartbeats (M1 contract) and kick off replication to peers.
    self.broadcast_heartbeat(now);
    for peer in self.peers().collect::<std::vec::Vec<_>>() {
      self.maybe_send_append(peer, log, stable);
    }
  }

  fn on_heartbeat<L: LogStore>(&mut self, now: Instant, log: &mut L, hb: crate::Heartbeat<I>) {
    let changed = self.leader != Some(hb.leader());
    self.role = Role::Follower;
    self.leader = Some(hb.leader());
    self.arm_election_timer(now);
    if changed {
      self
        .events
        .push_back(crate::Event::LeaderChanged(crate::LeaderChanged::new(
          self.term,
          Some(hb.leader()),
        )));
    }
    // Advance commit from heartbeat and apply any newly committed entries.
    let new_commit = core::cmp::min(hb.commit(), log.last_index());
    if new_commit > self.commit {
      self.commit = new_commit;
      self.apply_committed(log);
    }
    let (term, me) = (self.term, self.config.id());
    self.send(
      hb.leader(),
      Message::HeartbeatResp(crate::HeartbeatResp::new(term, me, bytes::Bytes::new())),
    );
  }

  fn on_append_entries<L: LogStore>(
    &mut self,
    now: Instant,
    log: &mut L,
    ae: crate::AppendEntries<I>,
  ) {
    let changed = self.leader != Some(ae.leader());
    self.role = Role::Follower;
    self.leader = Some(ae.leader());
    self.arm_election_timer(now);
    if changed {
      self
        .events
        .push_back(crate::Event::LeaderChanged(crate::LeaderChanged::new(
          self.term,
          Some(ae.leader()),
        )));
    }

    // Log-consistency check at prev_log_index/term.
    let consistent = ae.prev_log_index() == Index::ZERO
      || (ae.prev_log_index() <= log.last_index()
        && log
          .term(ae.prev_log_index())
          .map(|t| t == ae.prev_log_term())
          .unwrap_or(false));

    let (term, me) = (self.term, self.config.id());
    if !consistent {
      // M4 Task 5 (updated): etcd's two-sided reject hint — uniform for both the
      // term-mismatch and the simply-behind case. This makes the hint O(terms) rather
      // than O(entries): start from min(prev_log_index, last_index) on the FOLLOWER's log
      // and walk down while the term exceeds the leader's prev_log_term. The resulting
      // hint_term is meaningful even when the follower is merely behind, so the leader's
      // find_conflict_by_term lands in one round-trip instead of walking to index 0 and
      // falling back to a one-step decrement. (etcd's uniform findConflictByTerm path.)
      let last_index = log.last_index();
      let hint_index_raw = core::cmp::min(ae.prev_log_index(), last_index);
      let hint_index = self.find_conflict_by_term(log, hint_index_raw, ae.prev_log_term());
      let hint_term = log.term(hint_index).unwrap_or(Term::ZERO);
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
    let last_new = Index::new(ae.prev_log_index().get() + entries.len() as u64);
    let mut appended_opid: Option<crate::OpId> = None;
    if !entries.is_empty() {
      let mut conflict_at: Option<usize> = None;
      for (i, entry) in entries.iter().enumerate() {
        let idx = entry.index();
        let matches_existing =
          idx <= log.last_index() && log.term(idx).map(|t| t == entry.term()).unwrap_or(false);
        if !matches_existing {
          conflict_at = Some(i);
          break;
        }
      }
      if let Some(i) = conflict_at {
        // Safety tripwire: a conflict at/below our commit means a committed entry would be
        // rewritten — that must be impossible in correct Raft.
        debug_assert!(
          entries[i].index().get() > self.commit.get(),
          "AppendEntries would truncate a committed entry"
        );
        let opid = self.mint_op_id();
        log.submit_append(opid, &entries[i..]);
        appended_opid = Some(opid);
      }
      // else: every entry already present (pure duplicate) — append nothing.
    }

    // Commit advance and apply proceed independently of the local ack (committed entries
    // are durable on a quorum elsewhere; on restart the SM is rebuilt from durable log).
    let new_commit = core::cmp::min(ae.leader_commit(), last_new);
    if new_commit > self.commit {
      self.commit = new_commit;
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
      // Nothing was appended (heartbeat or pure duplicate) — entries already durable, ack now.
      self.send(
        ae.leader(),
        Message::AppendResp(crate::AppendResp::new(
          term,
          me,
          false,
          Index::ZERO,
          Term::ZERO,
          last_new,
        )),
      );
    }
  }

  /// M4 Task 6: a HeartbeatResp from a peer clears its probe pause and kicks off a new
  /// send. This allows a stalled `Probe` peer (whose partial-batch send was never acked)
  /// to resume replication on the next heartbeat round rather than waiting indefinitely.
  ///
  /// LIVENESS (deferred to a later milestone): a Replicate peer whose entire in-flight window
  /// is dropped with no further acks won't be re-probed by heartbeats — etcd sends an empty
  /// MsgApp when Replicate && Inflights.full(). Revisit with snapshots.
  fn on_heartbeat_resp<L: LogStore, S: StableStore<NodeId = I>>(
    &mut self,
    from: I,
    log: &L,
    stable: &S,
  ) {
    if !self.role.is_leader() {
      return;
    }
    if let Some(pr) = self.progress.get_mut(&from) {
      pr.clear_probe_pause();
    }
    self.maybe_send_append(from, log, stable);
  }

  /// Walk the leader's log downward from `index` until we find an entry whose term is
  /// `<= term` (or we hit the beginning). This mirrors etcd's `findConflictByTerm` and
  /// lets the leader skip a whole divergent term in one round-trip on reject.
  fn find_conflict_by_term<L: LogStore>(&self, log: &L, mut index: Index, term: Term) -> Index {
    while index > Index::ZERO {
      let t = log.term(index).unwrap_or(Term::ZERO);
      if t <= term {
        break;
      }
      index = Index::new(index.get() - 1);
    }
    index
  }

  fn on_append_resp<L: LogStore, S: StableStore<NodeId = I>>(
    &mut self,
    _now: Instant,
    log: &mut L,
    stable: &S,
    from: I,
    resp: crate::AppendResp<I>,
  ) {
    if !self.role.is_leader() {
      return;
    }
    let Some(pr) = self.progress.get_mut(&from) else {
      return;
    };
    if resp.reject() {
      // M4 Task 5: use the term-skip hint to jump next_index forward in one step.
      // find_conflict_by_term walks the leader's log from reject_hint_index downward
      // until we find an entry whose term ≤ reject_hint_term (the follower's conflicting
      // term). This lets the leader skip a whole conflicting term in O(terms) round-trips.
      let hint_index = resp.reject_hint_index();
      let hint_term = resp.reject_hint_term();
      let cur_next = pr.next_index();
      // Compute the conflict index before re-borrowing self.progress mutably.
      let conflict = self.find_conflict_by_term(log, hint_index, hint_term);
      // next_index must be at least 1 and must not advance past the current next on reject.
      let safe_next = if conflict == Index::ZERO || conflict >= cur_next {
        Index::new(cur_next.get().saturating_sub(1).max(1))
      } else {
        conflict
      };
      // Re-acquire progress to update (prior `pr` reference dropped implicitly by this point).
      if let Some(p) = self.progress.get_mut(&from) {
        p.become_probe();
        p.set_next_index(safe_next);
      }
      self.maybe_send_append(from, log, stable);
    } else if pr.maybe_update(resp.match_index()) {
      pr.become_replicate();
      self.maybe_advance_commit(log);
      self.apply_committed(log);
      self.maybe_send_append(from, log, stable); // keep the pipeline moving if still behind
    }
  }

  /// M5-U2c: receive an `InstallSnapshot` from the current leader (follower path).
  fn on_install_snapshot<L, S>(
    &mut self,
    now: Instant,
    log: &mut L,
    stable: &mut S,
    is: crate::InstallSnapshot<I>,
  ) where
    L: LogStore,
    S: StableStore<NodeId = I>,
    F::Snapshot: crate::Data,
  {
    // Preamble: mirror on_append_entries — reset to Follower, track leader, re-arm election timer.
    let changed = self.leader != Some(is.leader());
    self.role = Role::Follower;
    self.leader = Some(is.leader());
    self.arm_election_timer(now);
    if changed {
      self
        .events
        .push_back(crate::Event::LeaderChanged(crate::LeaderChanged::new(
          self.term,
          Some(is.leader()),
        )));
    }

    let meta = is.snapshot();
    let (term, me) = (self.term, self.config.id());

    // Staleness guard: if last_index <= commit we are already at or ahead of the snapshot.
    // Installing it would REGRESS committed/applied state — absolutely forbidden.
    // Ack anyway with match_index = self.commit so the leader can transition the peer out
    // of Snapshot state (maybe_update(commit) >= pending since leader only sends snapshots
    // whose last_index <= leader.commit, so commit >= pending holds).
    if meta.last_index() <= self.commit {
      self.send(
        is.leader(),
        Message::SnapshotResp(crate::SnapshotResp::new(term, me, false, self.commit)),
      );
      return;
    }

    // meta.last_index() > self.commit: proceed with installation.

    // Step 1: decode the SM snapshot. On failure, poison and return — leave NO partial state.
    let snap = match <F::Snapshot as crate::Data>::decode(is.data()) {
      Ok((_, s)) => s,
      Err(_) => {
        self.poison();
        return;
      }
    };

    // Step 2: restore the state machine. On failure, poison and return — leave NO partial state.
    if self.fsm.restore(snap).is_err() {
      self.poison();
      return;
    }

    // From here on the SM is in the snapshot state; all mutations below are safe.

    // A snapshot install discards the log tail; drop any pending log-append acks that
    // referred to now-discarded entries (a disk LogStore may have enqueued their
    // completions). Vote-persistence pendings are unrelated to the log and must survive.
    self
      .pending
      .retain(|_, p| matches!(p, Pending::CastVote { .. }));

    // A node installing a snapshot as a follower abandons any leader-side compaction it
    // had in flight (the deferred compact would target a now-superseded index); the old
    // SnapshotWritten completion will harmlessly find None.
    self.pending_compact = None;

    // Step 3: advance commit + applied to the snapshot boundary.
    self.commit = meta.last_index();
    self.applied = meta.last_index();

    // Step 4: re-baseline the log. Discards the follower's stale/short log (entries beyond
    // last_index were uncommitted since commit < last_index before this install — the leader
    // will re-replicate them if needed). After this call: first_index == last_index + 1,
    // term(last_index) == last_term — the NEXT AppendEntries(prev=last_index) passes the
    // consistency check without a reject-loop.
    //
    // The install updates in-memory state and the log read-view immediately (so commit/applied
    // and the log stay mutually consistent for apply_committed). The snapshot blob is persisted
    // via submit_snapshot (deferred completion). Local crash recovery does NOT rely on intra-call
    // ordering: if the process crashes before the blob is durable, restart-from-snapshot (M5-U3)
    // finds no durable snapshot and the node re-syncs from the leader. Acking before the blob is
    // durable is safe because meta.last_index <= leader.commit — those entries are already
    // quorum-committed, so this ack cannot advance the cluster commit.
    log.restore(meta.last_index(), meta.last_term());

    // Step 5: persist the snapshot for restart recovery (deferred; see comment above).
    let opid = self.mint_op_id();
    stable.submit_snapshot(opid, meta.clone(), is.data().clone());

    // Step 6: emit the application event.
    self
      .events
      .push_back(crate::Event::SnapshotInstalled(meta.clone()));

    // Step 7: M6 will wire dynamic membership from meta.conf() here. For now (M5, fixed
    // config), the conf field is informational only — skip it.

    // Step 8: ack to the leader with match_index = last_index, signalling successful install.
    // The leader's maybe_update(last_index) >= pending_snapshot transitions the peer out of
    // Snapshot state and resumes normal replication.
    self.send(
      is.leader(),
      Message::SnapshotResp(crate::SnapshotResp::new(term, me, false, meta.last_index())),
    );
  }

  /// M5-U2c: receive a `SnapshotResp` from a follower (leader path).
  fn on_snapshot_resp<L, S>(
    &mut self,
    _now: Instant,
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
    let Some(pr) = self.progress.get_mut(&from) else {
      return;
    };
    if resp.reject() {
      // The snapshot was refused (shouldn't happen in the current protocol, but handle
      // defensively): revert to Probe so maybe_send_append re-probes and, if the follower
      // is still below first_index, re-sends the snapshot.
      pr.become_probe();
      // Drop the mutable borrow of `pr` before calling maybe_send_append (which re-borrows
      // self.progress). The pattern mirrors on_append_resp's reject branch.
      self.maybe_send_append(from, log, stable);
    } else {
      // Success: maybe_update drives the Snapshot → Probe transition regardless of its return
      // value ("advanced" hint). We resume unconditionally so a peer leaving Snapshot is never
      // left un-poked. Drop `pr` before the self.* calls (borrow discipline mirrors on_append_resp).
      pr.maybe_update(resp.match_index());
      // Re-borrow self for the resume sequence (pr is dropped above).
      self.maybe_advance_commit(log);
      self.apply_committed(log);
      self.maybe_send_append(from, log, stable);
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::Instant;
  use core::time::Duration;

  struct Noop;

  impl crate::StateMachine for Noop {
    type Command = bytes::Bytes;
    type Response = ();
    type Snapshot = ();
    type Error = core::convert::Infallible;

    fn apply(&mut self, _: crate::Index, _: bytes::Bytes) -> Result<(), Self::Error> {
      Ok(())
    }

    fn snapshot(&self) -> Result<(), Self::Error> {
      Ok(())
    }

    fn restore(&mut self, _: ()) -> Result<(), Self::Error> {
      Ok(())
    }
  }

  #[test]
  fn endpoint_constructs_and_polls_empty() {
    let cfg = crate::Config::try_new(
      1u64,
      std::vec![1u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
    let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 42, Noop);
    assert_eq!(ep.id(), 1u64);
    assert!(ep.poll_message().is_none());
    assert!(ep.poll_event().is_none());
    // M1: election timer is armed immediately on construction
    assert!(ep.poll_timeout().is_some());
  }

  #[test]
  fn election_timer_is_armed_after_construction() {
    let cfg = crate::Config::try_new(
      1u64,
      std::vec![1u64, 2, 3],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
    let ep = Endpoint::new(cfg, Instant::ORIGIN, 42, Noop);
    // a fresh follower has an election deadline in (now, now + 2*base]
    let d = ep.poll_timeout().expect("election timer armed");
    assert!(d > crate::Instant::ORIGIN);
  }

  #[test]
  fn election_timeout_starts_a_campaign() {
    use crate::{Config, Instant, Message};
    use core::time::Duration;
    let cfg = Config::try_new(
      1u64,
      std::vec![1u64, 2, 3],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
    let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 42, Noop);
    let mut log = crate::testkit::NoopLog;
    let mut stable = crate::testkit::NoopStable::default();
    let deadline = ep.poll_timeout().unwrap();
    ep.handle_timeout(deadline, &mut log, &mut stable);
    assert!(ep.role().is_candidate());
    assert_eq!(ep.term(), crate::Term::new(1));
    // two RequestVotes (to peers 2 and 3), each in term 1
    let mut targets = std::vec::Vec::new();
    while let Some(out) = ep.poll_message() {
      assert!(matches!(out.message(), Message::RequestVote(_)));
      targets.push(out.to());
    }
    targets.sort();
    assert_eq!(targets, std::vec![2u64, 3u64]);
  }

  #[test]
  fn follower_grants_then_rejects_second_candidate() {
    use crate::{Config, Index, Instant, Message, RequestVote, Term};
    use core::time::Duration;
    let cfg = Config::try_new(
      2u64,
      std::vec![1u64, 2, 3],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
    let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, Noop);
    let mut log = crate::testkit::NoopLog;
    // Use AsyncStable so that the VoteResp(grant) is released on handle_storage.
    let mut stable = crate::testkit::AsyncStable::default();

    // candidate 1 in term 1, empty log — grant is deferred behind durability
    ep.handle_message(
      Instant::ORIGIN,
      &mut log,
      &mut stable,
      1u64,
      Message::RequestVote(RequestVote::new(
        Term::new(1),
        1u64,
        Index::ZERO,
        Term::ZERO,
        false,
        false,
      )),
    );
    // Grant is withheld until the hard-state write is durable.
    assert!(ep.poll_message().is_none(), "no grant before durability");
    // Drain storage → hard-state write completes → grant emitted.
    ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
    let vr = ep.poll_message().unwrap();
    assert!(matches!(vr.message(), Message::VoteResp(v) if !v.reject() && v.from()==2));
    assert_eq!(ep.term(), Term::new(1));

    // candidate 3 in the SAME term — already voted for 1, reject sent immediately
    ep.handle_message(
      Instant::ORIGIN,
      &mut log,
      &mut stable,
      3u64,
      Message::RequestVote(RequestVote::new(
        Term::new(1),
        3u64,
        Index::ZERO,
        Term::ZERO,
        false,
        false,
      )),
    );
    let vr = ep.poll_message().unwrap();
    assert!(matches!(vr.message(), Message::VoteResp(v) if v.reject()));
  }

  #[test]
  fn quorum_makes_a_leader_and_heartbeats_follow() {
    use crate::{Config, Instant, Message, Term, VoteResp};
    use core::time::Duration;
    let cfg = Config::try_new(
      1u64,
      std::vec![1u64, 2, 3],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
    let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, Noop);
    let mut log = crate::testkit::NoopLog;
    let mut stable = crate::testkit::NoopStable::default();

    let d = ep.poll_timeout().unwrap();
    ep.handle_timeout(d, &mut log, &mut stable); // become candidate, term 1, self-vote
    while ep.poll_message().is_some() {} // drain RequestVotes
    assert!(ep.role().is_candidate());

    // one more grant = quorum (2 of 3)
    ep.handle_message(
      d,
      &mut log,
      &mut stable,
      2u64,
      Message::VoteResp(VoteResp::new(Term::new(1), 2u64, false, false)),
    );
    assert!(ep.role().is_leader());
    // it should be broadcasting heartbeats to peers
    let mut hb = 0;
    while let Some(o) = ep.poll_message() {
      if matches!(o.message(), Message::Heartbeat(_)) {
        hb += 1;
      }
    }
    assert_eq!(hb, 2);
    // leader event surfaced
    assert!(matches!(
      ep.poll_event(),
      Some(crate::Event::LeaderChanged(_))
    ));
  }

  // --- M2 tests ---

  #[test]
  fn become_leader_appends_noop_and_inits_progress() {
    use crate::{Config, Instant, Message, Term, VoteResp};
    use core::time::Duration;
    let cfg = Config::try_new(
      1u64,
      std::vec![1u64, 2, 3],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
    let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
    let mut log = crate::testkit::VecLog::default();
    let mut stable = crate::testkit::NoopStable::default();
    let d = ep.poll_timeout().unwrap();
    ep.handle_timeout(d, &mut log, &mut stable); // candidate
    ep.handle_message(
      d,
      &mut log,
      &mut stable,
      2u64,
      Message::VoteResp(VoteResp::new(Term::new(1), 2u64, false, false)),
    );
    assert!(ep.role().is_leader());
    assert_eq!(log.last_index(), crate::Index::new(1)); // no-op at index 1
    assert!(
      log
        .entries(crate::Index::new(1)..crate::Index::new(2), u64::MAX)
        .unwrap()[0]
        .kind()
        .is_empty()
    );
  }

  #[test]
  fn propose_appends_and_replicates() {
    use crate::{Config, Instant, Message, Term, VoteResp};
    use core::time::Duration;
    let cfg = Config::try_new(
      1u64,
      std::vec![1u64, 2, 3],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
    let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
    let mut log = crate::testkit::VecLog::default();
    let mut stable = crate::testkit::NoopStable::default();
    let d = ep.poll_timeout().unwrap();
    ep.handle_timeout(d, &mut log, &mut stable);
    ep.handle_message(
      d,
      &mut log,
      &mut stable,
      2u64,
      Message::VoteResp(VoteResp::new(Term::new(1), 2u64, false, false)),
    );
    while ep.poll_message().is_some() {} // drain no-op AppendEntries
    while ep.poll_event().is_some() {} // drain LeaderChanged

    let idx = ep
      .propose(d, &mut log, &mut stable, &bytes::Bytes::from_static(b"cmd"))
      .unwrap();
    assert_eq!(idx, crate::Index::new(2)); // after the no-op at 1
    let mut appends = 0;
    while let Some(o) = ep.poll_message() {
      if let Message::AppendEntries(ae) = o.message() {
        if !ae.entries().is_empty() {
          appends += 1;
        }
      }
    }
    assert_eq!(appends, 2); // to peers 2 and 3
  }

  #[test]
  fn follower_appends_and_rejects_gap() {
    use crate::{AppendEntries, Config, Entry, EntryKind, Index, Instant, Message, Term};
    use core::time::Duration;
    let cfg = Config::try_new(
      2u64,
      std::vec![1u64, 2, 3],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
    let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
    let mut log = crate::testkit::VecLog::default();
    let mut stable = crate::testkit::NoopStable::default();

    // matching append at index 1 (prev=0) — fresh entry, ack deferred until durable
    let e1 = Entry::new(
      Term::new(1),
      Index::new(1),
      EntryKind::Normal,
      bytes::Bytes::from_static(b"a"),
    );
    ep.handle_message(
      Instant::ORIGIN,
      &mut log,
      &mut stable,
      1u64,
      Message::AppendEntries(AppendEntries::new(
        Term::new(1),
        1u64,
        Index::ZERO,
        Term::ZERO,
        std::vec![e1],
        Index::ZERO,
      )),
    );
    // No ack yet — append-before-ack: wait for durability.
    assert!(
      ep.poll_message().is_none(),
      "no ack before append is durable"
    );
    // Drain storage (VecLog completes synchronously on poll) → ack emitted.
    ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
    let r = ep.poll_message().unwrap();
    assert!(
      matches!(r.message(), Message::AppendResp(a) if !a.reject() && a.match_index()==Index::new(1))
    );
    assert_eq!(log.last_index(), Index::new(1));

    // gap: prev_log_index=5 we don't have → reject immediately (no append, no deferral)
    ep.handle_message(
      Instant::ORIGIN,
      &mut log,
      &mut stable,
      1u64,
      Message::AppendEntries(AppendEntries::new(
        Term::new(1),
        1u64,
        Index::new(5),
        Term::new(1),
        std::vec![],
        Index::ZERO,
      )),
    );
    let r = ep.poll_message().unwrap();
    assert!(matches!(r.message(), Message::AppendResp(a) if a.reject()));
  }

  #[test]
  fn quorum_ack_commits_and_applies() {
    use crate::{AppendResp, Config, Index, Instant, Message, Term, VoteResp};
    use core::time::Duration;
    let cfg = Config::try_new(
      1u64,
      std::vec![1u64, 2, 3],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
    let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
    let mut log = crate::testkit::VecLog::default();
    let mut stable = crate::testkit::NoopStable::default();
    let d = ep.poll_timeout().unwrap();
    ep.handle_timeout(d, &mut log, &mut stable);
    ep.handle_message(
      d,
      &mut log,
      &mut stable,
      2u64,
      Message::VoteResp(VoteResp::new(Term::new(1), 2u64, false, false)),
    );
    // Drain storage so the no-op LeaderAppend fires (advances self match_index to 1).
    ep.handle_storage(d, &mut log, &mut stable);
    while ep.poll_message().is_some() {}
    while ep.poll_event().is_some() {}

    let idx = ep
      .propose(d, &mut log, &mut stable, &bytes::Bytes::from_static(b"x"))
      .unwrap(); // index 2
    // Drain storage so the LeaderAppend for index 2 fires (advances self match_index to 2).
    ep.handle_storage(d, &mut log, &mut stable);
    while ep.poll_message().is_some() {}

    // peer 2 acks up to idx 2 → quorum (self match=2 + peer2 match=2) → commit + apply
    ep.handle_message(
      d,
      &mut log,
      &mut stable,
      2u64,
      Message::AppendResp(AppendResp::new(
        Term::new(1),
        2u64,
        false,
        Index::ZERO,
        Term::ZERO,
        idx,
      )),
    );
    // Applied event for the Normal entry at idx 2 (the no-op at 1 is an Empty entry, not Applied)
    let applied: std::vec::Vec<_> = core::iter::from_fn(|| ep.poll_event()).collect();
    assert!(
      applied
        .iter()
        .any(|e| matches!(e, crate::Event::Applied(a) if a.index()==idx))
    );
  }

  /// Regression: a stale/duplicate AppendEntries must NOT truncate already-committed entries.
  /// Raft §5.3: only delete-and-append from the first *conflicting* entry.
  #[test]
  fn stale_append_entries_does_not_erase_committed_entries() {
    use crate::{AppendEntries, Config, Entry, EntryKind, Index, Instant, Message, Term};
    use core::time::Duration;

    let cfg = Config::try_new(
      2u64,
      std::vec![1u64, 2, 3],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
    let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
    let mut log = crate::testkit::VecLog::default();
    let mut stable = crate::testkit::NoopStable::default();

    // Feed 3 entries from leader 1, leader_commit=3 → follower appends and commits all three.
    let e1 = Entry::new(
      Term::new(1),
      Index::new(1),
      EntryKind::Normal,
      bytes::Bytes::from_static(b"a"),
    );
    let e2 = Entry::new(
      Term::new(1),
      Index::new(2),
      EntryKind::Normal,
      bytes::Bytes::from_static(b"b"),
    );
    let e3 = Entry::new(
      Term::new(1),
      Index::new(3),
      EntryKind::Normal,
      bytes::Bytes::from_static(b"c"),
    );
    ep.handle_message(
      Instant::ORIGIN,
      &mut log,
      &mut stable,
      1u64,
      Message::AppendEntries(AppendEntries::new(
        Term::new(1),
        1u64,
        Index::ZERO,
        Term::ZERO,
        std::vec![e1, e2, e3],
        Index::new(3),
      )),
    );
    // Fresh entries → ack deferred until durable; drain storage to release it.
    assert!(ep.poll_message().is_none(), "no ack before append durable");
    ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
    // Must reply success with match_index=3.
    let r = ep.poll_message().unwrap();
    assert!(
      matches!(r.message(), Message::AppendResp(a) if !a.reject() && a.match_index() == Index::new(3)),
      "expected success match_index=3 after full append"
    );
    assert_eq!(log.last_index(), Index::new(3), "log must hold 3 entries");

    // Now feed a stale/duplicate AppendEntries carrying only entry 1 (a short prefix already
    // present). Under the old code this would have truncated entries 2 and 3.
    let e1_dup = Entry::new(
      Term::new(1),
      Index::new(1),
      EntryKind::Normal,
      bytes::Bytes::from_static(b"a"),
    );
    ep.handle_message(
      Instant::ORIGIN,
      &mut log,
      &mut stable,
      1u64,
      Message::AppendEntries(AppendEntries::new(
        Term::new(1),
        1u64,
        Index::ZERO,
        Term::ZERO,
        std::vec![e1_dup],
        Index::new(3),
      )),
    );
    // Must still reply success (last_new = prev(0) + len(1) = 1).
    let r2 = ep.poll_message().unwrap();
    assert!(
      matches!(r2.message(), Message::AppendResp(a) if !a.reject()),
      "stale duplicate must still be accepted"
    );
    // Entries 2 and 3 must still be in the log — the stale message must not have erased them.
    assert_eq!(
      log.last_index(),
      Index::new(3),
      "stale AppendEntries must not truncate entries 2 and 3"
    );
  }

  // --- M3 Task 5: restart test ---

  /// Encode a Bytes command through the Data codec (as propose does internally).
  fn encode_cmd(b: &[u8]) -> bytes::Bytes {
    use crate::Data;
    let mut buf = std::vec::Vec::new();
    bytes::Bytes::copy_from_slice(b).encode(&mut buf);
    bytes::Bytes::from(buf)
  }

  #[test]
  fn restart_replays_committed_log() {
    use crate::{Config, Entry, EntryKind, Index, Instant, Term};
    use core::time::Duration;
    let cfg = Config::try_new(
      1u64,
      std::vec![1u64, 2, 3],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
    let mut log = crate::testkit::VecLog::default();
    let mut stable = crate::testkit::NoopStable::default();
    // Seed the stores as if a prior incarnation had committed 2 Normal entries.
    log.force_append(&[
      Entry::new(
        Term::new(1),
        Index::new(1),
        EntryKind::Normal,
        encode_cmd(b"a"),
      ),
      Entry::new(
        Term::new(1),
        Index::new(2),
        EntryKind::Normal,
        encode_cmd(b"b"),
      ),
    ]);
    stable.force_state(Term::new(1), Some(1u64), Index::new(2)); // term=1, vote=1, commit=2

    let ep = Endpoint::restart(
      cfg,
      Instant::ORIGIN,
      7,
      crate::testkit::CountSm::default(),
      &mut log,
      &mut stable,
    );
    assert_eq!(ep.term(), Term::new(1));
    assert_eq!(ep.state_machine().count(), 2); // both committed entries replayed
    assert!(ep.role().is_follower());
    // election timer must be armed
    assert!(ep.poll_timeout().is_some());
  }

  // --- M3 extra: single-node leader commits after storage drain ---

  #[test]
  fn single_node_leader_commits_after_storage_drain() {
    use crate::{Config, Instant};
    use core::time::Duration;
    // 1-voter cluster: quorum == 1, so a lone node self-elects immediately.
    let cfg = Config::try_new(
      1u64,
      std::vec![1u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
    let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 42, crate::testkit::CountSm::default());
    let mut log = crate::testkit::VecLog::default();
    let mut stable = crate::testkit::NoopStable::default();

    let d = ep.poll_timeout().unwrap();
    ep.handle_timeout(d, &mut log, &mut stable); // self-elects (quorum=1)
    assert!(ep.role().is_leader());

    // The no-op LeaderAppend is still in pending — commit has NOT advanced yet.
    // Drain storage: the no-op append completes → self match advances → commit advances.
    ep.handle_storage(d, &mut log, &mut stable);
    while ep.poll_message().is_some() {}

    // Now propose a Normal entry and drain storage so it commits.
    ep.propose(d, &mut log, &mut stable, &bytes::Bytes::from_static(b"cmd"))
      .unwrap();
    ep.handle_storage(d, &mut log, &mut stable);

    // Applied event for the Normal entry must have been emitted.
    let events: std::vec::Vec<_> = core::iter::from_fn(|| ep.poll_event()).collect();
    assert!(
      events.iter().any(|e| matches!(e, crate::Event::Applied(_))),
      "a single-node leader must commit after handle_storage drains"
    );
  }

  // --- M3 tests ---

  #[test]
  fn op_ids_are_minted_distinctly() {
    let cfg = crate::Config::try_new(
      1u64,
      std::vec![1u64, 2, 3],
      core::time::Duration::from_millis(1000),
      core::time::Duration::from_millis(100),
    )
    .unwrap();
    let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
    let a = ep.mint_op_id_for_test();
    let b = ep.mint_op_id_for_test();
    assert_ne!(a, b);
    assert_eq!(b.get(), a.get() + 1);
  }

  /// Task 3 (M3): A granted vote must be withheld until the HardState write is durable.
  /// Uses `AsyncStable` which releases completions only on `poll`.
  #[test]
  fn vote_grant_waits_for_durable_hard_state() {
    use crate::{Config, Index, Instant, Message, RequestVote, Term};
    use core::time::Duration;
    let cfg = Config::try_new(
      2u64,
      std::vec![1u64, 2, 3],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
    let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
    let mut log = crate::testkit::VecLog::default();
    let mut stable = crate::testkit::AsyncStable::default();

    ep.handle_message(
      Instant::ORIGIN,
      &mut log,
      &mut stable,
      1u64,
      Message::RequestVote(RequestVote::new(
        Term::new(1),
        1u64,
        Index::ZERO,
        Term::ZERO,
        false,
        false,
      )),
    );
    assert!(
      ep.poll_message().is_none(),
      "no grant before the vote is durable"
    );
    // Drain storage → HardState write completes → grant emitted.
    ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
    assert!(
      matches!(ep.poll_message().unwrap().message(), Message::VoteResp(v) if !v.reject()),
      "grant must be emitted after handle_storage"
    );
  }

  /// Regression (M3): A vote grant for term N must NOT be emitted when storage drains
  /// if the node has since advanced to a higher term. Without the fix two grants would be
  /// emitted — one to candidate 1 (term 5, stale) and one to candidate 3 (term 6) — both
  /// stamped term 6, giving two leaders.
  #[test]
  fn deferred_vote_does_not_leak_across_term_bump() {
    use crate::{Config, Index, Instant, Message, RequestVote, Term};
    use core::time::Duration;

    let cfg = Config::try_new(
      2u64,
      std::vec![1u64, 2, 3],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
    let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
    let mut log = crate::testkit::VecLog::default();
    // AsyncStable: writes complete only when handle_storage / poll is called.
    let mut stable = crate::testkit::AsyncStable::default();

    // Step 1: candidate 1 requests a vote in term 5. Follower grants it (deferred).
    ep.handle_message(
      Instant::ORIGIN,
      &mut log,
      &mut stable,
      1u64,
      Message::RequestVote(RequestVote::new(
        Term::new(5),
        1u64,
        Index::ZERO,
        Term::ZERO,
        false,
        false,
      )),
    );
    // Grant is withheld — storage not yet drained.
    assert!(
      ep.poll_message().is_none(),
      "no grant before durability (term 5)"
    );

    // Step 2: candidate 3 arrives in term 6. Term pre-pass bumps term, clears pending.
    // on_request_vote then grants 3 and enqueues a NEW CastVote{to:3, term:6}.
    ep.handle_message(
      Instant::ORIGIN,
      &mut log,
      &mut stable,
      3u64,
      Message::RequestVote(RequestVote::new(
        Term::new(6),
        3u64,
        Index::ZERO,
        Term::ZERO,
        false,
        false,
      )),
    );
    assert!(
      ep.poll_message().is_none(),
      "no grant before durability (term 6)"
    );

    // Step 3: drain all storage completions (both op1 from term-5 grant and op2 from
    // term-6 step-down write, plus op3 from term-6 grant, all complete here).
    ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);

    // Step 4: collect all VoteResp messages.
    let mut grants: std::vec::Vec<(u64, u64)> = std::vec::Vec::new(); // (from, to/candidate)
    while let Some(out) = ep.poll_message() {
      if let Message::VoteResp(vr) = out.message() {
        if !vr.reject() {
          // out.to() is the candidate we're replying to
          grants.push((vr.from(), out.to()));
        }
      }
    }

    // There must be AT MOST one grant, and if present it must be to candidate 3 (term 6).
    assert!(
      grants.len() <= 1,
      "double-vote bug: got {} grants (expected at most 1): {:?}",
      grants.len(),
      grants
    );
    if let Some(&(_from, to)) = grants.first() {
      assert_eq!(
        to, 3u64,
        "grant must be to candidate 3 (term-6 vote), not candidate 1 (stale term-5 vote)"
      );
    }
    // There must be exactly one grant (to candidate 3).
    assert_eq!(
      grants.len(),
      1,
      "expected exactly one grant (to candidate 3)"
    );
  }

  /// Task 4 (M3): A follower must not send AppendResp until the new log entries are durable.
  /// Uses `VecLog` which enqueues `LogDone::Appended` on `submit_append`, released on `poll`.
  #[test]
  fn follower_ack_waits_for_durable_append() {
    use crate::{AppendEntries, Config, Entry, EntryKind, Index, Instant, Message, Term};
    use core::time::Duration;
    let cfg = Config::try_new(
      2u64,
      std::vec![1u64, 2, 3],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
    let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
    let mut log = crate::testkit::VecLog::default();
    let mut stable = crate::testkit::NoopStable::default();

    let e1 = Entry::new(
      Term::new(1),
      Index::new(1),
      EntryKind::Normal,
      bytes::Bytes::from_static(b"a"),
    );
    ep.handle_message(
      Instant::ORIGIN,
      &mut log,
      &mut stable,
      1u64,
      Message::AppendEntries(AppendEntries::new(
        Term::new(1),
        1u64,
        Index::ZERO,
        Term::ZERO,
        std::vec![e1],
        Index::ZERO,
      )),
    );
    // append-before-ack: no AppendResp yet (the append isn't durable)
    assert!(
      ep.poll_message().is_none(),
      "no ack before append is durable"
    );
    // drain storage → the append completes → AppendResp(success) is emitted
    ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
    let r = ep.poll_message().unwrap();
    assert!(
      matches!(r.message(), Message::AppendResp(a) if !a.reject() && a.match_index()==Index::new(1)),
      "AppendResp(success, match=1) must be emitted after handle_storage"
    );
  }

  /// Regression: a leader's heartbeat must advertise a commit index CLAMPED to each peer's
  /// match index, never the leader's full `commit`. A bare heartbeat carries no prev-log
  /// check, so a lagging follower with a divergent, uncommitted tail (e.g. a crashed ex-leader
  /// whose durable log holds an orphan entry whose index == its last_index) would otherwise
  /// commit+apply that stale entry on `min(hb.commit, last_index)`. Etcd's `min(committed,
  /// pr.Match)` rule. Without this clamp the cluster loses a committed entry / applies a
  /// phantom one (caught by the holistic-review chaos probe as UNSOUND-COMMIT).
  #[test]
  fn heartbeat_commit_is_clamped_to_peer_match() {
    use crate::{AppendResp, Config, Index, Instant, Message, Term, VoteResp};
    use core::time::Duration;
    let cfg = Config::try_new(
      1u64,
      std::vec![1u64, 2, 3],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
    let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
    let mut log = crate::testkit::VecLog::default();
    let mut stable = crate::testkit::NoopStable::default();

    // Elect node 1 leader (term 1) and let its no-op append become durable (commit→1).
    let d = ep.poll_timeout().unwrap();
    ep.handle_timeout(d, &mut log, &mut stable);
    ep.handle_message(
      d,
      &mut log,
      &mut stable,
      2u64,
      Message::VoteResp(VoteResp::new(Term::new(1), 2u64, false, false)),
    );
    assert!(ep.role().is_leader());
    ep.handle_storage(d, &mut log, &mut stable); // no-op (index 1) becomes durable
    while ep.poll_message().is_some() {}
    while ep.poll_event().is_some() {}

    // Propose two Normal entries (indices 2 and 3) and make them durable on the leader.
    ep.propose(d, &mut log, &mut stable, &bytes::Bytes::from_static(b"x"))
      .unwrap();
    ep.propose(d, &mut log, &mut stable, &bytes::Bytes::from_static(b"y"))
      .unwrap();
    ep.handle_storage(d, &mut log, &mut stable); // leader self-match → 3
    while ep.poll_message().is_some() {}

    // Peer 2 acks up to index 3 → quorum (leader match=3 + peer2 match=3) → commit advances to 3.
    // Peer 3 NEVER acks: its progress match_index stays at the post-election default (0/1).
    ep.handle_message(
      d,
      &mut log,
      &mut stable,
      2u64,
      Message::AppendResp(AppendResp::new(
        Term::new(1),
        2u64,
        false,
        Index::ZERO,
        Term::ZERO,
        Index::new(3),
      )),
    );
    // Commit must have advanced to 3: the two Normal entries (idx 2, 3) are now applied.
    let applied: std::vec::Vec<_> = core::iter::from_fn(|| ep.poll_event())
      .filter(|e| matches!(e, crate::Event::Applied(_)))
      .collect();
    assert_eq!(
      applied.len(),
      2,
      "leader must have committed+applied indices 2 and 3 via the peer-2 quorum"
    );
    // Drain any replication traffic produced by the commit advance.
    while ep.poll_message().is_some() {}

    // Fire the heartbeat timer → broadcast_heartbeat to peers 2 and 3.
    let hb_deadline = ep.poll_timeout().unwrap();
    ep.handle_timeout(hb_deadline, &mut log, &mut stable);

    // Collect the heartbeat advertised to the LAGGING peer 3.
    let mut hb_to_3: Option<Index> = None;
    while let Some(out) = ep.poll_message() {
      if out.to() == 3u64 {
        if let Message::Heartbeat(hb) = out.message() {
          hb_to_3 = Some(hb.commit());
        }
      }
    }
    let advertised = hb_to_3.expect("a heartbeat must be sent to peer 3");
    // Peer 3's match index is far below the leader's commit (3). The heartbeat must be clamped.
    assert!(
      advertised < Index::new(3),
      "heartbeat to a lagging peer must be clamped below the leader commit (got {advertised:?})"
    );
  }

  // ---- M4 Task 4: leader pacing ----

  /// A leader in Replicate mode with a window of 2 in-flight messages must stop sending
  /// once both slots are occupied, and resume after an ack frees a slot.
  #[test]
  fn leader_paces_by_inflight_window() {
    use crate::{AppendResp, Config, Index, Instant, Message, Term, VoteResp};
    use core::time::Duration;

    // window = 2, no byte cap, unbounded per-msg size
    let cfg = Config::try_new(
      1u64,
      std::vec![1u64, 2u64, 3u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap()
    .with_max_inflight_msgs(2)
    .unwrap()
    .with_max_size_per_msg(u64::MAX);

    let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
    let mut log = crate::testkit::VecLog::default();
    let mut stable = crate::testkit::NoopStable::default();

    // Elect node 1 as leader.
    let d = ep.poll_timeout().unwrap();
    ep.handle_timeout(d, &mut log, &mut stable);
    ep.handle_message(
      d,
      &mut log,
      &mut stable,
      2u64,
      Message::VoteResp(VoteResp::new(Term::new(1), 2u64, false, false)),
    );
    assert!(ep.role().is_leader());
    // Drain no-op append messages and storage.
    ep.handle_storage(d, &mut log, &mut stable);
    while ep.poll_message().is_some() {}
    while ep.poll_event().is_some() {}

    // Transition peer 2 to Replicate by simulating it acking the no-op (index 1).
    // This calls become_replicate() on the progress, enabling the inflight window.
    ep.handle_message(
      d,
      &mut log,
      &mut stable,
      2u64,
      Message::AppendResp(AppendResp::new(
        Term::new(1),
        2u64,
        false,
        Index::ZERO,
        Term::ZERO,
        Index::new(1), // ack no-op at index 1
      )),
    );
    // Drain any triggered sends (the become_replicate ack may trigger maybe_send_append).
    while ep.poll_message().is_some() {}

    // Propose 5 entries. With window=2 and Replicate mode, peer 2 should receive at most
    // 2 AppendEntries before the window fills.
    for i in 0u8..5 {
      let _ = ep
        .propose(
          d,
          &mut log,
          &mut stable,
          &bytes::Bytes::copy_from_slice(&[i]),
        )
        .unwrap();
      ep.handle_storage(d, &mut log, &mut stable);
    }

    // Collect all non-empty AppendEntries sent to peer 2.
    let mut appends_to_2: usize = 0;
    let mut last_sent_index = Index::ZERO;
    while let Some(out) = ep.poll_message() {
      if out.to() == 2u64 {
        if let Message::AppendEntries(ae) = out.message() {
          if !ae.entries().is_empty() {
            appends_to_2 += 1;
            if let Some(last) = ae.entries().last() {
              last_sent_index = last.index();
            }
          }
        }
      }
    }
    // With window=2 the leader must have stopped pipelining after 2 in-flight messages.
    assert!(
      appends_to_2 <= 2,
      "leader sent {appends_to_2} AppendEntries but window=2"
    );
    assert!(appends_to_2 > 0, "leader must send at least one batch");

    // Free the window: peer 2 acks through the last sent index.
    ep.handle_message(
      d,
      &mut log,
      &mut stable,
      2u64,
      Message::AppendResp(AppendResp::new(
        Term::new(1),
        2u64,
        false,
        Index::ZERO,
        Term::ZERO,
        last_sent_index,
      )),
    );
    // After the ack, the leader should pipeline more entries (entries 5 and beyond).
    let mut resumed = false;
    while let Some(out) = ep.poll_message() {
      if out.to() == 2u64 {
        if let Message::AppendEntries(ae) = out.message() {
          if !ae.entries().is_empty() {
            resumed = true;
          }
        }
      }
    }
    assert!(
      resumed,
      "leader must resume sending after ack frees the window"
    );
  }

  // ---- M4 Task 5: term-skip reject hint ----

  /// A divergent follower's reject carries a term hint that lets the leader skip a whole
  /// conflicting term instead of backing off one entry at a time.
  ///
  /// Scenario:
  ///   Leader log:   1@1 2@1 3@2 4@2 5@3
  ///   Follower log: 1@1 2@1 3@3 4@3   (diverges at index 3: has term-3 entries)
  ///
  /// The leader (optimistically in Replicate, next=6) sends AppendEntries(prev=5@3, entries=[]).
  /// The follower rejects: prev=5, but follower only has 4 entries; last_index=4, so hint is:
  ///   reject_hint_term = term(4) = 3 (on follower log)
  ///   reject_hint_index = first index where term==3 on follower = 3
  ///
  /// Leader's find_conflict_by_term(index=3, term=3):
  ///   leader log term(3) = 2 < 3 → stop immediately at 3
  ///   → next_index = 3 (skip the whole stale term-3 region in one step)
  #[test]
  fn divergent_follower_resyncs_fast_via_term_skip() {
    use crate::{
      AppendEntries, AppendResp, Config, Entry, EntryKind, Index, Instant, Message, Term, VoteResp,
    };
    use core::time::Duration;

    // === Follower side: test the reject-hint computation ===
    // Node 2 is the follower with log [1@1, 2@1, 3@3, 4@3].
    let follower_cfg = Config::try_new(
      2u64,
      std::vec![1u64, 2u64, 3u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
    let mut follower = Endpoint::new(
      follower_cfg,
      Instant::ORIGIN,
      7,
      crate::testkit::CountSm::default(),
    );
    let mut follower_log = crate::testkit::VecLog::default();
    let mut follower_stable = crate::testkit::NoopStable::default();

    // Seed follower log with [1@1, 2@1, 3@3, 4@3].
    follower_log.force_append(&[
      Entry::new(
        Term::new(1),
        Index::new(1),
        EntryKind::Normal,
        bytes::Bytes::from_static(b"a"),
      ),
      Entry::new(
        Term::new(1),
        Index::new(2),
        EntryKind::Normal,
        bytes::Bytes::from_static(b"b"),
      ),
      Entry::new(
        Term::new(3),
        Index::new(3),
        EntryKind::Normal,
        bytes::Bytes::from_static(b"c"),
      ),
      Entry::new(
        Term::new(3),
        Index::new(4),
        EntryKind::Normal,
        bytes::Bytes::from_static(b"d"),
      ),
    ]);

    // Leader sends AppendEntries(prev_index=4, prev_term=2) — inconsistency at prev.
    // Follower has term(4)=3 ≠ 2 → reject.
    follower.handle_message(
      Instant::ORIGIN,
      &mut follower_log,
      &mut follower_stable,
      1u64,
      Message::AppendEntries(AppendEntries::new(
        Term::new(3),
        1u64,
        Index::new(4), // prev_log_index
        Term::new(2),  // prev_log_term (leader has 4@2, follower has 4@3)
        std::vec![],
        Index::ZERO,
      )),
    );

    // The follower must reject with the etcd two-sided term-skip hint.
    // hint_index_raw = min(prev_log_index=4, last_index=4) = 4
    // find_conflict_by_term(follower_log, 4, ceiling=prev_log_term=2):
    //   term(4)=3 > 2 → 3; term(3)=3 > 2 → 2; term(2)=1 ≤ 2 → stop at 2
    // hint_index=2, hint_term=term(2)=1
    let resp = follower
      .poll_message()
      .expect("follower must send AppendResp(reject)");
    let ar = match resp.message() {
      Message::AppendResp(r) => *r,
      other => panic!("expected AppendResp, got {other:?}"),
    };
    assert!(ar.reject(), "follower must reject the inconsistent append");
    // Etcd two-sided hint: walk from min(prev=4, last=4)=4 down while term > prev_log_term=2.
    // Stops at index 2 (term=1 ≤ 2).
    assert_eq!(
      ar.reject_hint_index(),
      Index::new(2),
      "hint index must be 2 (find_conflict_by_term walks below all term-3 entries)"
    );
    assert_eq!(
      ar.reject_hint_term(),
      Term::new(1),
      "hint term must be 1 (term at index 2 on follower)"
    );

    // === Leader side: test that find_conflict_by_term jumps next_index in one step ===
    // Node 1 is the leader with log [1@1, 2@1, 3@1, 4@1, 5@1] in term 1.
    // (We keep term=1 throughout so the leader doesn't step down.)
    // The reject hint (from follower's two-sided form) is (index=2, term=1).
    // Leader find_conflict_by_term(2, ceiling=1): term(2)=1 ≤ 1 → stop at 2 → next=2 → prev=1.
    let leader_cfg = Config::try_new(
      1u64,
      std::vec![1u64, 2u64, 3u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
    let mut leader = Endpoint::new(
      leader_cfg,
      Instant::ORIGIN,
      1,
      crate::testkit::CountSm::default(),
    );
    let mut leader_log = crate::testkit::VecLog::default();
    let mut leader_stable = crate::testkit::NoopStable::default();

    // Elect node 1 as leader (term=1, noop at index 1).
    let d = leader.poll_timeout().unwrap();
    leader.handle_timeout(d, &mut leader_log, &mut leader_stable);
    leader.handle_message(
      d,
      &mut leader_log,
      &mut leader_stable,
      2u64,
      Message::VoteResp(VoteResp::new(Term::new(1), 2u64, false, false)),
    );
    assert!(leader.role().is_leader());
    leader.handle_storage(d, &mut leader_log, &mut leader_stable);
    while leader.poll_message().is_some() {}
    while leader.poll_event().is_some() {}

    // Force-seed the leader log with 4 more entries so total = [1@1, 2@1, 3@1, 4@1, 5@1].
    // All term-1 entries. The follower will hint term=3 (its divergent term), which is
    // higher than any term on the leader's log. find_conflict_by_term(index=3, term=3)
    // will walk back: leader term(3)=1 ≤ 3 → stop at 3 → next_index = 3.
    leader_log.force_append(&[
      Entry::new(
        Term::new(1),
        Index::new(2),
        EntryKind::Normal,
        bytes::Bytes::from_static(b"b"),
      ),
      Entry::new(
        Term::new(1),
        Index::new(3),
        EntryKind::Normal,
        bytes::Bytes::from_static(b"c"),
      ),
      Entry::new(
        Term::new(1),
        Index::new(4),
        EntryKind::Normal,
        bytes::Bytes::from_static(b"d"),
      ),
      Entry::new(
        Term::new(1),
        Index::new(5),
        EntryKind::Normal,
        bytes::Bytes::from_static(b"e"),
      ),
    ]);

    // Simulate peer 2 acking index 1 (noop) → transitions to Replicate.
    leader.handle_message(
      d,
      &mut leader_log,
      &mut leader_stable,
      2u64,
      Message::AppendResp(AppendResp::new(
        Term::new(1),
        2u64,
        false,
        Index::ZERO,
        Term::ZERO,
        Index::new(1),
      )),
    );
    // Drain any pipelined sends triggered by the ack.
    while leader.poll_message().is_some() {}

    // Now simulate receiving the two-sided reject hint from peer 2:
    //   reject=true, reject_hint_index=2, reject_hint_term=1
    // find_conflict_by_term(leader_log, 2, ceiling=1): term(2)=1 ≤ 1 → stop at 2
    // safe_next = 2, prev_log_index = 1.
    // With the OLD code (naive decrement from cur_next): next would step back only one slot.
    // With the NEW code (two-sided hint): next_index jumps to 2 in one round-trip.
    leader.handle_message(
      d,
      &mut leader_log,
      &mut leader_stable,
      2u64,
      Message::AppendResp(AppendResp::new(
        Term::new(1),
        2u64,
        true,          // reject
        Index::new(2), // reject_hint_index (etcd two-sided form)
        Term::new(1),  // reject_hint_term
        Index::ZERO,
      )),
    );

    // The leader should now send AppendEntries with prev_log_index = 1 (next_index = 2).
    // If the old naive decrement were used, it would send with a much higher prev_log_index.
    let mut found_correct_prev = false;
    while let Some(out) = leader.poll_message() {
      if out.to() == 2u64 {
        if let Message::AppendEntries(ae) = out.message() {
          if ae.prev_log_index() == Index::new(1) {
            found_correct_prev = true;
          }
        }
      }
    }
    assert!(
      found_correct_prev,
      "leader must jump next_index to 2 (prev=1) via two-sided term-skip hint, not step back one-by-one"
    );
  }

  // ---- M4 Task 6: heartbeat response resumes a stalled probe ----

  /// A peer in Probe mode that has stalled (msg_app_flow_paused set because only a partial
  /// batch was sent due to the byte cap) must resume replication when a HeartbeatResp arrives.
  #[test]
  fn heartbeat_resp_resumes_stalled_probe() {
    use crate::{Config, Instant, Message, Term, VoteResp};
    use core::time::Duration;

    // max_size_per_msg=0 means exactly 1 entry per AppendEntries.
    // With multiple entries in the log, each send is a partial batch → probe pauses.
    let cfg = Config::try_new(
      1u64,
      std::vec![1u64, 2u64, 3u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap()
    .with_max_size_per_msg(0); // 0 = one entry per message

    let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
    let mut log = crate::testkit::VecLog::default();
    let mut stable = crate::testkit::NoopStable::default();

    // Elect node 1 as leader.
    let d = ep.poll_timeout().unwrap();
    ep.handle_timeout(d, &mut log, &mut stable);
    ep.handle_message(
      d,
      &mut log,
      &mut stable,
      2u64,
      Message::VoteResp(VoteResp::new(Term::new(1), 2u64, false, false)),
    );
    assert!(ep.role().is_leader());
    ep.handle_storage(d, &mut log, &mut stable);
    while ep.poll_message().is_some() {}
    while ep.poll_event().is_some() {}

    // Propose TWO more entries so the log has [noop@1, cmd1@2, cmd2@3].
    // With max_size_per_msg=0 (1 entry/msg), the probe from become_leader already sent
    // noop@1 alone. Since log.last_index()=1 and we sent to index 1 → not partial → no pause.
    // Now we add cmd1@2. After propose, maybe_send_append sends from next=1 (Probe unchanged):
    //   entries=[noop@1, cmd1@2], capped to 1 → sends [noop@1], last_sent=1, last_index=2 → partial → PAUSED.
    let _ = ep
      .propose(
        d,
        &mut log,
        &mut stable,
        &bytes::Bytes::from_static(b"cmd1"),
      )
      .unwrap();
    ep.handle_storage(d, &mut log, &mut stable);
    let _ = ep
      .propose(
        d,
        &mut log,
        &mut stable,
        &bytes::Bytes::from_static(b"cmd2"),
      )
      .unwrap();
    ep.handle_storage(d, &mut log, &mut stable);
    // Drain all messages from the propose phase (probe fires on first propose, then pauses).
    while ep.poll_message().is_some() {}

    // Probe is now paused (partial batch was sent: noop@1 sent, but cmd1@2/cmd2@3 remain).
    // A new propose would call maybe_send_append → paused → no send.
    let _ = ep
      .propose(
        d,
        &mut log,
        &mut stable,
        &bytes::Bytes::from_static(b"cmd3"),
      )
      .unwrap();
    ep.handle_storage(d, &mut log, &mut stable);
    let mut probe_blocked = true;
    while let Some(out) = ep.poll_message() {
      if out.to() == 2u64 {
        if let Message::AppendEntries(_) = out.message() {
          probe_blocked = false;
        }
      }
    }
    assert!(
      probe_blocked,
      "while probe is paused, a new propose must NOT trigger an AppendEntries to peer 2"
    );

    // Task 6: a HeartbeatResp from peer 2 must clear msg_app_flow_paused and call
    // maybe_send_append so the stalled probe resumes immediately.
    ep.handle_message(
      d,
      &mut log,
      &mut stable,
      2u64,
      Message::HeartbeatResp(crate::HeartbeatResp::new(
        Term::new(1),
        2u64,
        bytes::Bytes::new(),
      )),
    );
    let mut resumed = false;
    while let Some(out) = ep.poll_message() {
      if out.to() == 2u64 {
        if let Message::AppendEntries(_) = out.message() {
          resumed = true;
        }
      }
    }
    assert!(
      resumed,
      "HeartbeatResp must clear the probe pause and trigger an AppendEntries to peer 2"
    );
  }

  // ---- Fix 1 regression: empty appends must NOT consume the inflight window ----

  /// A caught-up Replicate peer triggers an empty AppendEntries on every HeartbeatResp.
  /// Before the fix, each call to `sent_entries` added a zero-byte inflight slot that was
  /// never freed (no ack for empty sends), so after `max_inflight_msgs` heartbeat-resps
  /// the window filled and newly proposed entries were silently not delivered.
  ///
  /// This test uses a small window (4 slots), delivers many HeartbeatResps (more than 4),
  /// then proposes a new entry and asserts that an AppendEntries carrying it IS emitted.
  #[test]
  fn empty_appends_do_not_wedge_inflight_window() {
    use crate::{AppendResp, Config, Index, Instant, Message, Term, VoteResp};
    use core::time::Duration;

    let cfg = Config::try_new(
      1u64,
      std::vec![1u64, 2u64, 3u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap()
    .with_max_inflight_msgs(4)
    .unwrap()
    .with_max_size_per_msg(u64::MAX);

    let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
    let mut log = crate::testkit::VecLog::default();
    let mut stable = crate::testkit::NoopStable::default();

    // Elect node 1 as leader.
    let d = ep.poll_timeout().unwrap();
    ep.handle_timeout(d, &mut log, &mut stable);
    ep.handle_message(
      d,
      &mut log,
      &mut stable,
      2u64,
      Message::VoteResp(VoteResp::new(Term::new(1), 2u64, false, false)),
    );
    assert!(ep.role().is_leader());
    ep.handle_storage(d, &mut log, &mut stable);
    while ep.poll_message().is_some() {}
    while ep.poll_event().is_some() {}

    // Transition peer 2 to Replicate by acking the no-op (index 1).
    ep.handle_message(
      d,
      &mut log,
      &mut stable,
      2u64,
      Message::AppendResp(AppendResp::new(
        Term::new(1),
        2u64,
        false,
        Index::ZERO,
        Term::ZERO,
        Index::new(1),
      )),
    );
    while ep.poll_message().is_some() {}

    // Deliver 10 HeartbeatResps from peer 2 (each triggers an empty AppendEntries for a
    // caught-up peer). With window=4 and the bug, only 4 resps suffice to wedge the window.
    for _ in 0..10 {
      ep.handle_message(
        d,
        &mut log,
        &mut stable,
        2u64,
        Message::HeartbeatResp(crate::HeartbeatResp::new(
          Term::new(1),
          2u64,
          bytes::Bytes::new(),
        )),
      );
      while ep.poll_message().is_some() {}
    }

    // Now propose a new entry. The leader must emit an AppendEntries carrying it to peer 2.
    let _idx = ep
      .propose(d, &mut log, &mut stable, &bytes::Bytes::from_static(b"new"))
      .unwrap();
    ep.handle_storage(d, &mut log, &mut stable);

    let mut delivered = false;
    while let Some(out) = ep.poll_message() {
      if out.to() == 2u64 {
        if let Message::AppendEntries(ae) = out.message() {
          if !ae.entries().is_empty() {
            delivered = true;
          }
        }
      }
    }
    assert!(
      delivered,
      "after 10 heartbeat-resps the inflight window must not be wedged; proposed entry must be delivered to peer 2"
    );
  }

  // ---- Fix 2 regression: lagging-follower hint is O(terms) not O(entries) ----

  /// A follower that is simply behind (prev_log_index > last_index) must emit a reject hint
  /// whose term is meaningful so the leader can jump in one step.
  ///
  /// Scenario: follower log [1..=2]@term1, leader sends AppendEntries(prev=20@term1).
  /// - Old hint: (last_index.next()=3, Term::ZERO) → leader walks to index 0, falls back
  ///   to one-step decrement → O(entries) round-trips to converge.
  /// - New hint (etcd two-sided): hint_index_raw=min(20,2)=2,
  ///   find_conflict_by_term(log, 2, ceiling=term1): term(2)=1 ≤ 1 → stop at 2
  ///   → hint=(2, term1). Leader's find_conflict_by_term(2, term1)=2 → next=2 → converges
  ///   on the very next send.
  ///
  /// Verification: check the follower's hint_term is non-zero (meaningful), and that a
  /// leader receiving it jumps to next=3 (prev=2) in one step — not to index 0.
  #[test]
  fn lagging_follower_hint_is_two_sided() {
    use crate::{
      AppendEntries, AppendResp, Config, Entry, EntryKind, Index, Instant, Message, Term, VoteResp,
    };
    use core::time::Duration;

    // --- Follower side: verify the hint ----------------------------------------
    // Follower has [1@1, 2@1]; receives AppendEntries(prev=20, prev_term=1).
    let follower_cfg = Config::try_new(
      2u64,
      std::vec![1u64, 2u64, 3u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
    let mut follower = Endpoint::new(
      follower_cfg,
      Instant::ORIGIN,
      7,
      crate::testkit::CountSm::default(),
    );
    let mut follower_log = crate::testkit::VecLog::default();
    let mut follower_stable = crate::testkit::NoopStable::default();
    follower_log.force_append(&[
      Entry::new(
        Term::new(1),
        Index::new(1),
        EntryKind::Normal,
        bytes::Bytes::from_static(b"a"),
      ),
      Entry::new(
        Term::new(1),
        Index::new(2),
        EntryKind::Normal,
        bytes::Bytes::from_static(b"b"),
      ),
    ]);

    follower.handle_message(
      Instant::ORIGIN,
      &mut follower_log,
      &mut follower_stable,
      1u64,
      Message::AppendEntries(AppendEntries::new(
        Term::new(1),
        1u64,
        Index::new(20), // prev_log_index far past follower's last (2)
        Term::new(1),   // prev_log_term
        std::vec![],
        Index::ZERO,
      )),
    );

    let resp = follower.poll_message().expect("follower must reject");
    let ar = match resp.message() {
      Message::AppendResp(r) => *r,
      other => panic!("expected AppendResp, got {other:?}"),
    };
    assert!(ar.reject(), "follower must reject (prev=20 > last=2)");
    // Two-sided hint: hint_index_raw=min(20,2)=2; find_conflict_by_term(log, 2, ceiling=1):
    // term(2)=1 ≤ 1 → stop → hint_index=2, hint_term=1 (NOT Term::ZERO as in the old code).
    assert_eq!(
      ar.reject_hint_index(),
      Index::new(2),
      "hint index must be 2 (follower's last index, walk stops immediately at ceiling)"
    );
    assert_ne!(
      ar.reject_hint_term(),
      Term::ZERO,
      "hint term must NOT be ZERO for a simply-lagging follower (old bug: always emitted ZERO)"
    );
    assert_eq!(
      ar.reject_hint_term(),
      Term::new(1),
      "hint term must be 1 (the term at the follower's last index)"
    );

    // --- Leader side: verify the one-step jump ----------------------------------
    // Leader has [1..20]@term1. Receives reject hint (2, term1).
    // find_conflict_by_term(leader_log, 2, ceiling=1): term(2)=1 ≤ 1 → stop at 2 → next=2.
    // This gives prev=1 on the follow-up send — O(1) not O(entries).
    let leader_cfg = Config::try_new(
      1u64,
      std::vec![1u64, 2u64, 3u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap()
    .with_max_size_per_msg(u64::MAX);
    let mut leader = Endpoint::new(
      leader_cfg,
      Instant::ORIGIN,
      1,
      crate::testkit::CountSm::default(),
    );
    let mut leader_log = crate::testkit::VecLog::default();
    let mut leader_stable = crate::testkit::NoopStable::default();

    let d = leader.poll_timeout().unwrap();
    leader.handle_timeout(d, &mut leader_log, &mut leader_stable);
    leader.handle_message(
      d,
      &mut leader_log,
      &mut leader_stable,
      2u64,
      Message::VoteResp(VoteResp::new(Term::new(1), 2u64, false, false)),
    );
    assert!(leader.role().is_leader());
    leader.handle_storage(d, &mut leader_log, &mut leader_stable);
    while leader.poll_message().is_some() {}
    while leader.poll_event().is_some() {}

    // Force-seed indices 2..=20 so leader has [1..20]@term1.
    let extra: std::vec::Vec<_> = (2u64..=20)
      .map(|i| {
        Entry::new(
          Term::new(1),
          Index::new(i),
          EntryKind::Normal,
          bytes::Bytes::from_static(b"x"),
        )
      })
      .collect();
    leader_log.force_append(&extra);

    // Peer 2 acks noop (index 1) → Replicate, next=2.
    leader.handle_message(
      d,
      &mut leader_log,
      &mut leader_stable,
      2u64,
      Message::AppendResp(AppendResp::new(
        Term::new(1),
        2u64,
        false,
        Index::ZERO,
        Term::ZERO,
        Index::new(1),
      )),
    );
    // Drain the pipelined sends after the ack (sends indices 2..=20 in one batch, then
    // records 1 inflight slot in Replicate).
    while leader.poll_message().is_some() {}

    // Inject the two-sided reject hint (2, term1) from the follower.
    // With the old hint (3, ZERO), the leader walks to index 0 and falls back to cur_next-1.
    // With the new hint (2, 1), find_conflict_by_term(leader_log, 2, 1)=2 → next=2, prev=1.
    leader.handle_message(
      d,
      &mut leader_log,
      &mut leader_stable,
      2u64,
      Message::AppendResp(AppendResp::new(
        Term::new(1),
        2u64,
        true,          // reject
        Index::new(2), // hint_index from two-sided follower
        Term::new(1),  // hint_term: NON-ZERO so leader can land in one step
        Index::ZERO,
      )),
    );

    // The leader must send AppendEntries with prev_log_index ≤ 2 (next_index ≤ 3).
    // If the old code were used with hint=(2, 0), it would fall back to cur_next-1 = 20
    // (because find_conflict_by_term walks to 0 with ceiling=0 → safe_next = cur_next-1).
    let mut found_low_prev = false;
    while let Some(out) = leader.poll_message() {
      if out.to() == 2u64 {
        if let Message::AppendEntries(ae) = out.message() {
          // With two-sided hint the leader jumps to next=2 → prev=1.
          if ae.prev_log_index() <= Index::new(2) {
            found_low_prev = true;
          }
        }
      }
    }
    assert!(
      found_low_prev,
      "leader must jump to prev ≤ 2 via the two-sided hint (O(1) round-trip), not back off one-by-one"
    );
  }

  // ---- M5 Task 4: snapshot threshold + deferred compaction ----

  /// Helper: elect a single-node leader, drain the no-op, and apply `n` Normal entries.
  /// Returns the endpoint with `applied == n + 1` (no-op + n commands, all committed).
  fn make_single_node_leader_with_entries(
    n: usize,
    threshold: usize,
  ) -> (
    Endpoint<u64, crate::testkit::CountSm>,
    crate::testkit::VecLog,
    crate::testkit::AsyncStable,
  ) {
    use crate::{Config, Instant};
    use core::time::Duration;
    let cfg = Config::try_new(
      1u64,
      std::vec![1u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap()
    .with_snapshot_threshold(threshold);

    let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 42, crate::testkit::CountSm::default());
    let mut log = crate::testkit::VecLog::default();
    let mut stable = crate::testkit::AsyncStable::default();

    let d = ep.poll_timeout().unwrap();
    ep.handle_timeout(d, &mut log, &mut stable); // self-elects
    assert!(ep.role().is_leader());
    // Drain no-op (LeaderAppend for index 1).
    ep.handle_storage(d, &mut log, &mut stable);
    while ep.poll_message().is_some() {}
    while ep.poll_event().is_some() {}

    // Propose and commit `n` Normal entries one at a time.
    for i in 0..n {
      let cmd = bytes::Bytes::copy_from_slice(&[i as u8]);
      let _ = ep.propose(d, &mut log, &mut stable, &cmd).unwrap();
      // Drain storage each time to let the self-append complete (quorum=1: auto-commits).
      ep.handle_storage(d, &mut log, &mut stable);
      while ep.poll_message().is_some() {}
      while ep.poll_event().is_some() {}
    }
    (ep, log, stable)
  }

  /// After applying past `snapshot_threshold`, a single `handle_storage` call should:
  /// 1. Submit a snapshot to stable (readable via `stable.snapshot().is_some()`).
  /// 2. Set `pending_compact` to the deferred (opid, applied) pair.
  ///
  /// The log is NOT yet compacted — the SnapshotWritten completion hasn't fired.
  #[test]
  fn snapshot_submitted_and_pending_compact_set() {
    // threshold=3 means we snapshot once applied - first_index >= 3.
    // After no-op (idx 1) + 3 Normal entries (idx 2,3,4), applied=4, first_index=1 → gap=3.
    let (ep, log, stable) = make_single_node_leader_with_entries(3, 3);

    // snapshot was persisted in stable
    assert!(
      stable.snapshot().is_some(),
      "stable must hold the persisted snapshot"
    );
    // pending_compact is set (snapshot write in flight, compaction deferred)
    assert!(
      ep.pending_compact().is_some(),
      "pending_compact must be set while snapshot write is in flight"
    );
    // log is NOT yet compacted (compaction deferred until SnapshotWritten)
    assert_eq!(
      log.first_index(),
      Index::new(1),
      "log must not be compacted before SnapshotWritten fires"
    );
  }

  /// After the `SnapshotWritten` completion fires (second `handle_storage`), the deferred
  /// compaction executes: `log.first_index()` advances and `pending_compact` is cleared.
  #[test]
  fn deferred_compact_fires_on_snapshot_written() {
    let (mut ep, mut log, mut stable) = make_single_node_leader_with_entries(3, 3);

    // Drain the SnapshotWritten completion → deferred compact fires.
    ep.handle_storage(crate::Instant::ORIGIN, &mut log, &mut stable);

    // Log is now compacted: first_index advanced past the initial first_index.
    assert!(
      log.first_index() > Index::new(1),
      "first_index must advance after SnapshotWritten fires (got {:?})",
      log.first_index()
    );
    // pending_compact cleared
    assert!(
      ep.pending_compact().is_none(),
      "pending_compact must be None after compaction fires"
    );
  }

  /// While `pending_compact` is set, `maybe_snapshot` must not fire again (idempotence guard).
  #[test]
  fn maybe_snapshot_does_not_refire_while_pending() {
    let (mut ep, mut log, mut stable) = make_single_node_leader_with_entries(3, 3);

    // At this point pending_compact is Some. Drain again without clearing the completion —
    // but since AsyncStable enqueues SnapshotWritten only once, calling handle_storage again
    // before any new completion simply runs maybe_snapshot again. The guard must prevent a
    // second submit_snapshot.
    let snap_count_before = stable.snapshot().map(|_| 1usize).unwrap_or(0);

    // Call handle_storage again — no new completion available yet (already drained above),
    // so maybe_snapshot runs again. With the guard it must be a no-op.
    ep.handle_storage(crate::Instant::ORIGIN, &mut log, &mut stable);
    // We shouldn't have gotten a SECOND snapshot submission — check pending_compact is still set.
    // (It won't be cleared because there's no new SnapshotWritten completion.)
    // The stable still has exactly one snapshot (no double-submit).
    let snap_count_after = stable.snapshot().map(|_| 1usize).unwrap_or(0);
    assert_eq!(
      snap_count_before, snap_count_after,
      "maybe_snapshot must not re-fire while pending_compact is set"
    );
  }

  // ---- M5 Task 5: send InstallSnapshot to lagging follower ----

  /// Helper: build a 3-voter leader (node 1) with a compacted log.
  /// Returns the endpoint, a VecLog compacted up to `offset` with the snapshot persisted
  /// in an AsyncStable, and the stable store.
  ///
  /// Log after setup: entries [offset+1 ..= offset+n_tail], first_index = offset + 1.
  /// Stable holds a snapshot with last_index = offset.
  fn make_leader_with_compacted_log(
    offset: u64,
    n_tail: usize,
  ) -> (
    Endpoint<u64, crate::testkit::CountSm>,
    crate::testkit::VecLog,
    crate::testkit::AsyncStable,
  ) {
    use crate::{
      Config, Entry, EntryKind, Index, Instant, Message, Term, VoteResp, conf::ConfState,
    };
    use core::time::Duration;

    let cfg = Config::try_new(
      1u64,
      std::vec![1u64, 2u64, 3u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap()
    .with_max_size_per_msg(u64::MAX);

    let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
    let mut log = crate::testkit::VecLog::default();
    let mut stable = crate::testkit::AsyncStable::default();

    // Elect node 1 as leader.
    let d = ep.poll_timeout().unwrap();
    ep.handle_timeout(d, &mut log, &mut stable);
    ep.handle_message(
      d,
      &mut log,
      &mut stable,
      2u64,
      Message::VoteResp(VoteResp::new(Term::new(1), 2u64, false, false)),
    );
    assert!(ep.role().is_leader());
    ep.handle_storage(d, &mut log, &mut stable);
    while ep.poll_message().is_some() {}
    while ep.poll_event().is_some() {}

    // Compact the log up to `offset`: seed boundary entries then compact.
    if offset > 0 {
      // Force the log to have entries 1..=offset+n_tail to give compact() something to drop.
      let all: std::vec::Vec<Entry> = (1u64..=offset + n_tail as u64)
        .map(|i| {
          Entry::new(
            Term::new(1),
            Index::new(i),
            EntryKind::Normal,
            bytes::Bytes::from_static(b"x"),
          )
        })
        .collect();
      log.force_append(&all);
      // Compact up to offset, retaining entries [offset+1 ..= offset+n_tail].
      log.compact(Index::new(offset));
    }

    // Persist a snapshot with last_index = offset in stable.
    let meta = crate::SnapshotMeta::new(
      Index::new(offset),
      Term::new(1),
      ConfState::from_voters(std::vec![1u64, 2u64, 3u64]),
    );
    let data = bytes::Bytes::from_static(b"snap-data");
    stable.submit_snapshot(crate::OpId::new(99), meta, data);
    // Drain the SnapshotWritten completion so stable.snapshot() is readable.
    while stable.poll().is_some() {}

    (ep, log, stable)
  }

  /// Test 1: sends InstallSnapshot when next_index < first_index.
  #[test]
  fn sends_install_snapshot_on_compacted_hole() {
    use crate::{Index, Message};

    let offset = 5u64;
    let (mut ep, log, stable) = make_leader_with_compacted_log(offset, 2);

    // Set peer 2's progress so next_index = 3 < first_index = 6.
    let far_behind = Index::new(3);
    if let Some(p) = ep.progress.get_mut(&2u64) {
      p.become_probe();
      p.set_next_index(far_behind);
    }

    // Call maybe_send_append; it should detect next_index < first_index and send snapshot.
    ep.maybe_send_append(2u64, &log, &stable);

    // Exactly one outgoing message to peer 2 must be InstallSnapshot.
    let msgs: std::vec::Vec<_> = core::iter::from_fn(|| ep.poll_message()).collect();
    let snap_msgs: std::vec::Vec<_> = msgs
      .iter()
      .filter(|o| o.to() == 2u64 && matches!(o.message(), Message::InstallSnapshot(_)))
      .collect();
    assert_eq!(
      snap_msgs.len(),
      1,
      "exactly one InstallSnapshot must be sent to peer 2"
    );

    let snap_msg = match snap_msgs[0].message() {
      Message::InstallSnapshot(s) => s,
      _ => unreachable!(),
    };
    // The snapshot must match what stable holds (last_index = offset).
    assert_eq!(
      snap_msg.snapshot().last_index(),
      Index::new(offset),
      "InstallSnapshot must carry the persisted snapshot's last_index"
    );

    // Peer 2's progress must now be in Snapshot state with pending = offset.
    let pr = ep.progress.get(&2u64).unwrap();
    assert!(
      pr.state().is_snapshot(),
      "peer 2 must be in Snapshot state after sending InstallSnapshot"
    );
    if let crate::ProgressState::Snapshot(pending) = pr.state() {
      assert_eq!(
        pending,
        Index::new(offset),
        "Snapshot pending index must equal the snapshot's last_index"
      );
    }
  }

  /// Test 2: no broken AppendEntries (prev_log_term == ZERO) for compacted peer.
  #[test]
  fn no_broken_append_entries_for_compacted_peer() {
    use crate::{Index, Message, Term};

    let offset = 5u64;
    let (mut ep, log, stable) = make_leader_with_compacted_log(offset, 2);

    // Peer 2 is far behind (next_index < first_index).
    if let Some(p) = ep.progress.get_mut(&2u64) {
      p.become_probe();
      p.set_next_index(Index::new(3));
    }

    ep.maybe_send_append(2u64, &log, &stable);

    // Must NOT see any AppendEntries with prev_log_term == ZERO for this peer.
    while let Some(out) = ep.poll_message() {
      if out.to() == 2u64 {
        if let Message::AppendEntries(ae) = out.message() {
          assert_ne!(
            ae.prev_log_term(),
            Term::ZERO,
            "a broken AppendEntries with prev_log_term=ZERO must not be sent to a compacted peer"
          );
        }
      }
    }
  }

  /// Test 3: after becoming Snapshot-state, peer is paused (no spam).
  #[test]
  fn snapshot_state_peer_is_paused_no_second_send() {
    use crate::Index;

    let offset = 5u64;
    let (mut ep, log, stable) = make_leader_with_compacted_log(offset, 2);

    // Set peer 2 far behind.
    if let Some(p) = ep.progress.get_mut(&2u64) {
      p.become_probe();
      p.set_next_index(Index::new(3));
    }

    // First call: sends the snapshot and transitions peer to Snapshot state.
    ep.maybe_send_append(2u64, &log, &stable);
    while ep.poll_message().is_some() {} // drain

    // Second call: peer is now paused (Snapshot state), must send nothing.
    ep.maybe_send_append(2u64, &log, &stable);
    let msgs: std::vec::Vec<_> = core::iter::from_fn(|| ep.poll_message()).collect();
    assert!(
      msgs.is_empty(),
      "a second maybe_send_append to a Snapshot-state peer must emit nothing (paused)"
    );
  }

  /// Test 4: a peer at next_index == first_index gets a normal AppendEntries (not a snapshot).
  #[test]
  fn normal_append_at_boundary_not_snapshot() {
    use crate::{Index, Message};

    let offset = 5u64;
    let (mut ep, log, stable) = make_leader_with_compacted_log(offset, 2);
    // first_index = offset + 1 = 6; set next_index = 6 (the boundary).
    let first = log.first_index();
    assert_eq!(first, Index::new(offset + 1));

    if let Some(p) = ep.progress.get_mut(&2u64) {
      p.become_probe();
      p.set_next_index(first); // exactly at boundary
    }

    ep.maybe_send_append(2u64, &log, &stable);

    let msgs: std::vec::Vec<_> = core::iter::from_fn(|| ep.poll_message()).collect();

    // Must NOT send an InstallSnapshot.
    let snap_count = msgs
      .iter()
      .filter(|o| o.to() == 2u64 && matches!(o.message(), Message::InstallSnapshot(_)))
      .count();
    assert_eq!(
      snap_count, 0,
      "must NOT send InstallSnapshot when next_index == first_index"
    );

    // Must send an AppendEntries (normal path — prev_index = offset, boundary term retained).
    let ae_count = msgs
      .iter()
      .filter(|o| o.to() == 2u64 && matches!(o.message(), Message::AppendEntries(_)))
      .count();
    assert_eq!(
      ae_count, 1,
      "must send a normal AppendEntries when next_index == first_index"
    );

    // And the prev_log_term must be the boundary term (Term::new(1)), NOT ZERO.
    for out in &msgs {
      if out.to() == 2u64 {
        if let Message::AppendEntries(ae) = out.message() {
          assert_ne!(
            ae.prev_log_term(),
            crate::Term::ZERO,
            "AppendEntries at the compaction boundary must carry the boundary term, not ZERO"
          );
        }
      }
    }
  }

  // ---- M5-U2c: InstallSnapshot receive + SnapshotResp ----

  /// Encode a `u64` snapshot value into a `Bytes` blob (the wire format used by CountSm).
  fn encode_snapshot(v: u64) -> bytes::Bytes {
    use crate::Data as _;
    let mut buf = std::vec::Vec::new();
    v.encode(&mut buf);
    bytes::Bytes::from(buf)
  }

  /// Build a follower endpoint (node 2 in a 3-voter cluster, term 1) with an empty log.
  fn make_follower() -> (
    Endpoint<u64, crate::testkit::CountSm>,
    crate::testkit::VecLog,
    crate::testkit::AsyncStable,
  ) {
    use crate::{Config, Instant};
    use core::time::Duration;
    let cfg = Config::try_new(
      2u64,
      std::vec![1u64, 2u64, 3u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
    let ep = Endpoint::new(cfg, Instant::ORIGIN, 7, crate::testkit::CountSm::default());
    let log = crate::testkit::VecLog::default();
    let stable = crate::testkit::AsyncStable::default();
    (ep, log, stable)
  }

  /// Test 1: a behind follower installs the snapshot and acks correctly.
  #[test]
  fn install_snapshot_on_behind_follower() {
    use crate::{Index, Instant, Message, Term, conf::ConfState};

    let (mut ep, mut log, mut stable) = make_follower();

    // Build a snapshot: SM state = 42 (CountSm::count = 42), last_index=10, last_term=4.
    let snap_value: u64 = 42;
    let snap_data = encode_snapshot(snap_value);
    let meta = crate::SnapshotMeta::new(
      Index::new(10),
      Term::new(4),
      ConfState::from_voters(std::vec![1u64, 2u64, 3u64]),
    );
    let is = crate::InstallSnapshot::new(Term::new(1), 1u64, meta.clone(), snap_data.clone());

    // Follower commit starts at 0 (< 10) → install path.
    ep.handle_message(
      Instant::ORIGIN,
      &mut log,
      &mut stable,
      1u64,
      Message::InstallSnapshot(is),
    );

    // SM must be restored to the snapshot state.
    assert_eq!(
      ep.state_machine().count() as u64,
      snap_value,
      "state machine must be restored to the snapshot value"
    );

    // commit and applied must both equal last_index.
    assert_eq!(
      ep.commit,
      Index::new(10),
      "commit must equal meta.last_index()"
    );
    assert_eq!(
      ep.applied,
      Index::new(10),
      "applied must equal meta.last_index()"
    );

    // Log must be re-baselined: first_index == 11, term(10) == 4.
    assert_eq!(
      log.first_index(),
      Index::new(11),
      "first_index must be last_index + 1"
    );
    assert_eq!(
      log.last_index(),
      Index::new(10),
      "last_index must equal meta.last_index()"
    );
    assert_eq!(
      log.term(Index::new(10)).unwrap(),
      Term::new(4),
      "term(last_index) must equal last_term after restore"
    );
    // No entries exist above last_index.
    assert!(
      log
        .entries(Index::new(11)..Index::new(11), u64::MAX)
        .unwrap()
        .is_empty(),
      "entries(11..11) must be empty after restore"
    );

    // Exactly one SnapshotInstalled event must be emitted.
    let events: std::vec::Vec<_> = core::iter::from_fn(|| ep.poll_event()).collect();
    let installed: std::vec::Vec<_> = events
      .iter()
      .filter(|e| e.is_snapshot_installed())
      .collect();
    assert_eq!(
      installed.len(),
      1,
      "exactly one SnapshotInstalled event must be emitted"
    );
    assert_eq!(
      installed[0].unwrap_snapshot_installed_ref().last_index(),
      Index::new(10)
    );

    // Exactly one SnapshotResp must be sent to the leader (node 1) with reject=false,
    // match_index=10.
    let msgs: std::vec::Vec<_> = core::iter::from_fn(|| ep.poll_message()).collect();
    let snap_resps: std::vec::Vec<_> = msgs
      .iter()
      .filter(|o| o.to() == 1u64 && matches!(o.message(), Message::SnapshotResp(_)))
      .collect();
    assert_eq!(
      snap_resps.len(),
      1,
      "exactly one SnapshotResp must be sent to the leader"
    );
    let sr = match snap_resps[0].message() {
      Message::SnapshotResp(r) => r,
      _ => unreachable!(),
    };
    assert!(
      !sr.reject(),
      "SnapshotResp must not be a rejection on successful install"
    );
    assert_eq!(
      sr.match_index(),
      Index::new(10),
      "match_index must equal meta.last_index()"
    );

    // stable must have a snapshot persisted (submit_snapshot was called).
    assert!(
      stable.snapshot().is_some(),
      "stable store must hold the persisted snapshot after install"
    );

    // Election timer must be re-armed (poll_timeout is Some).
    assert!(
      ep.poll_timeout().is_some(),
      "election timer must be re-armed after receiving a snapshot"
    );
  }

  /// Test 2: a stale snapshot (last_index <= commit) is a no-op ack, SM not touched.
  #[test]
  fn stale_snapshot_does_not_install() {
    use crate::{Entry, EntryKind, Index, Instant, Message, Term, conf::ConfState};

    let (mut ep, mut log, mut stable) = make_follower();

    // Seed the follower log with 15 entries so commit can be set to 15.
    let entries: std::vec::Vec<_> = (1u64..=15)
      .map(|i| {
        Entry::new(
          Term::new(1),
          Index::new(i),
          EntryKind::Normal,
          bytes::Bytes::from_static(b"x"),
        )
      })
      .collect();
    log.force_append(&entries);
    // Manually advance commit to 15 (the follower has committed up to 15).
    ep.commit = Index::new(15);
    ep.applied = Index::new(15);
    // SM count is arbitrary (doesn't matter — must not change).
    let sm_count_before = ep.state_machine().count();

    // Try to install a snapshot with last_index=10 (< commit=15): stale.
    let snap_data = encode_snapshot(7u64);
    let meta = crate::SnapshotMeta::new(
      Index::new(10),
      Term::new(4),
      ConfState::from_voters(std::vec![1u64, 2u64, 3u64]),
    );
    let is = crate::InstallSnapshot::new(Term::new(1), 1u64, meta, snap_data);
    ep.handle_message(
      Instant::ORIGIN,
      &mut log,
      &mut stable,
      1u64,
      Message::InstallSnapshot(is),
    );

    // SM must NOT have been restored.
    assert_eq!(
      ep.state_machine().count(),
      sm_count_before,
      "SM must not be restored for a stale snapshot"
    );
    // commit must be unchanged.
    assert_eq!(
      ep.commit,
      Index::new(15),
      "commit must not regress for a stale snapshot"
    );

    // No SnapshotInstalled event.
    let events: std::vec::Vec<_> = core::iter::from_fn(|| ep.poll_event())
      .filter(|e| e.is_snapshot_installed())
      .collect();
    assert!(
      events.is_empty(),
      "no SnapshotInstalled event for a stale snapshot"
    );

    // Must still send a SnapshotResp with reject=false and match_index = self.commit.
    let msgs: std::vec::Vec<_> = core::iter::from_fn(|| ep.poll_message()).collect();
    let snap_resps: std::vec::Vec<_> = msgs
      .iter()
      .filter(|o| o.to() == 1u64 && matches!(o.message(), Message::SnapshotResp(_)))
      .collect();
    assert_eq!(
      snap_resps.len(),
      1,
      "stale snapshot must still send a SnapshotResp"
    );
    let sr = match snap_resps[0].message() {
      Message::SnapshotResp(r) => r,
      _ => unreachable!(),
    };
    assert!(!sr.reject(), "stale snapshot ack must have reject=false");
    assert_eq!(
      sr.match_index(),
      Index::new(15),
      "match_index must be self.commit (so leader leaves Snapshot state)"
    );
  }

  /// Test 3: malformed snapshot data poisons the node; no partial state is applied.
  #[test]
  fn malformed_snapshot_data_poisons_node() {
    use crate::{Index, Instant, Message, Term, conf::ConfState};

    let (mut ep, mut log, mut stable) = make_follower();

    // Bad data: too short to decode a u64 (only 3 bytes).
    let bad_data = bytes::Bytes::from_static(b"bad");
    let meta = crate::SnapshotMeta::new(
      Index::new(10),
      Term::new(4),
      ConfState::from_voters(std::vec![1u64, 2u64, 3u64]),
    );
    let is = crate::InstallSnapshot::new(Term::new(1), 1u64, meta, bad_data);
    ep.handle_message(
      Instant::ORIGIN,
      &mut log,
      &mut stable,
      1u64,
      Message::InstallSnapshot(is),
    );

    // Node must be poisoned.
    assert!(
      ep.is_poisoned(),
      "node must be poisoned after a malformed snapshot"
    );

    // commit and applied must NOT have been touched (no partial state).
    assert_eq!(
      ep.commit,
      Index::ZERO,
      "commit must not be modified on decode failure"
    );
    assert_eq!(
      ep.applied,
      Index::ZERO,
      "applied must not be modified on decode failure"
    );

    // All subsequent handle_message calls are no-ops.
    let good_data = encode_snapshot(1u64);
    let meta2 = crate::SnapshotMeta::new(
      Index::new(10),
      Term::new(4),
      ConfState::from_voters(std::vec![1u64, 2u64, 3u64]),
    );
    let is2 = crate::InstallSnapshot::new(Term::new(1), 1u64, meta2, good_data);
    ep.handle_message(
      Instant::ORIGIN,
      &mut log,
      &mut stable,
      1u64,
      Message::InstallSnapshot(is2),
    );
    // commit still zero — poisoned node ignores everything.
    assert_eq!(
      ep.commit,
      Index::ZERO,
      "poisoned node must ignore subsequent messages"
    );
    // No messages or events emitted.
    assert!(
      ep.poll_message().is_none(),
      "poisoned node must not emit messages"
    );
  }

  /// Test 4: leader processes a successful SnapshotResp — peer leaves Snapshot state.
  #[test]
  fn leader_processes_snapshot_resp_success_and_reject() {
    use crate::{Index, Instant, Message, Term, VoteResp};
    use core::time::Duration;

    // Build a 3-voter leader (node 1).
    let cfg = crate::Config::try_new(
      1u64,
      std::vec![1u64, 2u64, 3u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
    let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
    let mut log = crate::testkit::VecLog::default();
    let mut stable = crate::testkit::AsyncStable::default();

    let d = ep.poll_timeout().unwrap();
    ep.handle_timeout(d, &mut log, &mut stable);
    ep.handle_message(
      d,
      &mut log,
      &mut stable,
      2u64,
      Message::VoteResp(VoteResp::new(Term::new(1), 2u64, false, false)),
    );
    assert!(ep.role().is_leader());
    ep.handle_storage(d, &mut log, &mut stable);
    while ep.poll_message().is_some() {}
    while ep.poll_event().is_some() {}

    // Manually put peer 2 into Snapshot(10) state.
    if let Some(p) = ep.progress.get_mut(&2u64) {
      p.become_snapshot(Index::new(10));
    }
    assert!(ep.progress.get(&2u64).unwrap().state().is_snapshot());

    // --- Reject case: become_probe, then maybe_send_append re-enters probe ---
    ep.handle_message(
      d,
      &mut log,
      &mut stable,
      2u64,
      Message::SnapshotResp(crate::SnapshotResp::new(
        Term::new(1),
        2u64,
        true, // reject
        Index::new(10),
      )),
    );
    // After reject the peer must have transitioned to Probe.
    assert!(
      ep.progress.get(&2u64).unwrap().state().is_probe(),
      "reject SnapshotResp must transition peer to Probe"
    );

    // --- Success case: peer has been put back in Snapshot(10). ---
    if let Some(p) = ep.progress.get_mut(&2u64) {
      p.become_snapshot(Index::new(10));
    }
    // Drain any messages from the probe that was triggered by the reject.
    while ep.poll_message().is_some() {}

    ep.handle_message(
      d,
      &mut log,
      &mut stable,
      2u64,
      Message::SnapshotResp(crate::SnapshotResp::new(
        Term::new(1),
        2u64,
        false, // success
        Index::new(10),
      )),
    );
    // maybe_update(10) >= pending_snapshot(10) → Probe; match_index == 10.
    let pr = ep.progress.get(&2u64).unwrap();
    assert!(
      pr.state().is_probe(),
      "success SnapshotResp must transition peer out of Snapshot state"
    );
    assert_eq!(
      pr.match_index(),
      Index::new(10),
      "match_index must be 10 after successful SnapshotResp"
    );
  }

  // ---- M5-U2c: LogStore::restore unit tests ----

  /// After `restore(10, 4)` on a VecLog with arbitrary prior content, the log has the
  /// expected re-baseline invariants.
  #[test]
  fn veclog_restore_rebaselines_correctly() {
    use crate::{Entry, EntryKind, Index, Term};

    let mut log = crate::testkit::VecLog::default();

    // Seed with entries 1..=5 at term 1.
    let entries: std::vec::Vec<_> = (1u64..=5)
      .map(|i| {
        Entry::new(
          Term::new(1),
          Index::new(i),
          EntryKind::Normal,
          bytes::Bytes::new(),
        )
      })
      .collect();
    log.submit_append(crate::OpId::new(1), &entries);
    let _ = log.poll(); // drain completion

    // Restore to last_index=10, last_term=4 (simulating a received snapshot).
    log.restore(Index::new(10), Term::new(4));

    assert_eq!(
      log.first_index(),
      Index::new(11),
      "first_index must be last_index + 1"
    );
    assert_eq!(
      log.last_index(),
      Index::new(10),
      "last_index must equal the snapshot boundary"
    );
    assert_eq!(
      log.term(Index::new(10)).unwrap(),
      Term::new(4),
      "term(last_index) must equal last_term"
    );
    // No entries above last_index.
    assert!(
      log
        .entries(Index::new(11)..Index::new(11), u64::MAX)
        .unwrap()
        .is_empty(),
      "entries(11..11) must be empty after restore"
    );
    // No stale completions should leak out.
    assert!(log.poll().is_none(), "no pending completions after restore");
  }

  /// After `restore` a subsequent `submit_append` of index 11 works correctly.
  #[test]
  fn veclog_submit_append_after_restore() {
    use crate::{Entry, EntryKind, Index, Term};

    let mut log = crate::testkit::VecLog::default();

    // Seed and restore to last_index=10, last_term=4.
    log.restore(Index::new(10), Term::new(4));

    // Append index 11 at term 5.
    let e = Entry::new(
      Term::new(5),
      Index::new(11),
      EntryKind::Normal,
      bytes::Bytes::from_static(b"next"),
    );
    log.submit_append(crate::OpId::new(1), core::slice::from_ref(&e));
    let _ = log.poll(); // drain

    assert_eq!(
      log.last_index(),
      Index::new(11),
      "last_index must be 11 after appending entry 11"
    );
    assert_eq!(
      log.term(Index::new(11)).unwrap(),
      Term::new(5),
      "term(11) must be 5"
    );
    // Boundary term still accessible.
    assert_eq!(
      log.term(Index::new(10)).unwrap(),
      Term::new(4),
      "boundary term must be retained"
    );
  }

  // ---- M5-U3: restore-from-snapshot on restart ----

  /// Build a `CountSm` snapshot blob for the given count value.
  fn encode_count_snapshot(count: u64) -> bytes::Bytes {
    use crate::Data as _;
    let mut buf = std::vec::Vec::new();
    count.encode(&mut buf);
    bytes::Bytes::from(buf)
  }

  /// Test 1: restart with a durable snapshot + post-snapshot committed tail.
  /// SM must reflect snapshot-baseline PLUS replayed entries 6 and 7.
  /// applied==7, commit==7, not poisoned.
  #[test]
  fn restart_restores_snapshot_then_replays_tail() {
    use crate::{Config, Entry, EntryKind, Index, Instant, Term, conf::ConfState};
    use core::time::Duration;

    let cfg = Config::try_new(
      1u64,
      std::vec![1u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();

    // Build a durable stable: snapshot at last_index=5, last_term=2, SM count=10.
    let mut stable = crate::testkit::AsyncStable::default();
    let snap_count: u64 = 10;
    let snap_data = encode_count_snapshot(snap_count);
    let meta = crate::SnapshotMeta::new(
      Index::new(5),
      Term::new(2),
      ConfState::from_voters(std::vec![1u64]),
    );
    stable.submit_snapshot(crate::OpId::new(1), meta, snap_data);
    // Drain the SnapshotWritten completion so stable.snapshot() is readable.
    while stable.poll().is_some() {}

    // Set HardState: term=2, commit=7 (two entries past the snapshot).
    stable.force_state(Term::new(2), None, Index::new(7));

    // Build a durable log: compacted to baseline 5, entries 6 and 7 present.
    let mut log = crate::testkit::VecLog::default();
    // Restore the log to the snapshot baseline (offset=5, compacted_term=2).
    log.restore(Index::new(5), Term::new(2));
    // Force-append entries 6 and 7 (post-snapshot tail).
    // Entry data must be length-prefixed (the CountSm uses Bytes::decode, which requires
    // an 8-byte LE length prefix followed by the raw payload).
    log.force_append(&[
      Entry::new(
        Term::new(2),
        Index::new(6),
        EntryKind::Normal,
        encode_cmd(b"cmd6"),
      ),
      Entry::new(
        Term::new(2),
        Index::new(7),
        EntryKind::Normal,
        encode_cmd(b"cmd7"),
      ),
    ]);

    // Restart the node.
    let ep = Endpoint::restart(
      cfg,
      Instant::ORIGIN,
      42,
      crate::testkit::CountSm::default(),
      &mut log,
      &mut stable,
    );

    // SM must be the snapshot baseline (10) + 2 replayed entries = 12.
    assert_eq!(
      ep.state_machine().count() as u64,
      snap_count + 2,
      "SM must equal snapshot baseline + 2 replayed tail entries"
    );
    assert_eq!(ep.applied, Index::new(7), "applied must be 7");
    assert_eq!(ep.commit, Index::new(7), "commit must be 7");
    assert!(!ep.is_poisoned(), "node must not be poisoned");
  }

  /// Test 2: restart with snapshot only, no post-snapshot tail.
  /// SM == snapshot state, applied==commit==5.
  #[test]
  fn restart_restores_snapshot_no_tail() {
    use crate::{Config, Index, Instant, Term, conf::ConfState};
    use core::time::Duration;

    let cfg = Config::try_new(
      1u64,
      std::vec![1u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();

    let mut stable = crate::testkit::AsyncStable::default();
    let snap_count: u64 = 7;
    let snap_data = encode_count_snapshot(snap_count);
    let meta = crate::SnapshotMeta::new(
      Index::new(5),
      Term::new(2),
      ConfState::from_voters(std::vec![1u64]),
    );
    stable.submit_snapshot(crate::OpId::new(1), meta, snap_data);
    while stable.poll().is_some() {}
    stable.force_state(Term::new(2), None, Index::new(5));

    // Log baseline = 5, no entries above it.
    let mut log = crate::testkit::VecLog::default();
    log.restore(Index::new(5), Term::new(2));

    let ep = Endpoint::restart(
      cfg,
      Instant::ORIGIN,
      42,
      crate::testkit::CountSm::default(),
      &mut log,
      &mut stable,
    );

    assert_eq!(
      ep.state_machine().count() as u64,
      snap_count,
      "SM must equal the snapshot state"
    );
    assert_eq!(ep.applied, Index::new(5), "applied must be 5");
    assert_eq!(ep.commit, Index::new(5), "commit must be 5");
    assert!(!ep.is_poisoned(), "node must not be poisoned");
  }

  /// Test 3: no snapshot (regression) — M3 replay-from-1 still works when
  /// stable.snapshot() is None and the log starts at 1.
  #[test]
  fn restart_no_snapshot_replays_from_one() {
    use crate::{Config, Entry, EntryKind, Index, Instant, Term};
    use core::time::Duration;

    let cfg = Config::try_new(
      1u64,
      std::vec![1u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();

    // No snapshot.
    let mut stable = crate::testkit::AsyncStable::default();
    stable.force_state(Term::new(1), None, Index::new(3));

    // Durable log: entries 1,2,3 at term 1.
    // Entry data must be length-prefixed (Bytes::decode requires 8-byte LE length prefix).
    let mut log = crate::testkit::VecLog::default();
    log.force_append(&[
      Entry::new(
        Term::new(1),
        Index::new(1),
        EntryKind::Empty,
        bytes::Bytes::new(),
      ),
      Entry::new(
        Term::new(1),
        Index::new(2),
        EntryKind::Normal,
        encode_cmd(b"a"),
      ),
      Entry::new(
        Term::new(1),
        Index::new(3),
        EntryKind::Normal,
        encode_cmd(b"b"),
      ),
    ]);

    let ep = Endpoint::restart(
      cfg,
      Instant::ORIGIN,
      42,
      crate::testkit::CountSm::default(),
      &mut log,
      &mut stable,
    );

    // 2 Normal entries applied (entry 1 is Empty/noop).
    assert_eq!(ep.state_machine().count(), 2, "two Normal entries applied");
    assert_eq!(ep.applied, Index::new(3), "applied must be 3");
    assert_eq!(ep.commit, Index::new(3), "commit must be 3");
    assert!(!ep.is_poisoned(), "node must not be poisoned");
  }

  /// Test 4: corrupt durable snapshot data poisons the node; no partial apply.
  #[test]
  fn restart_corrupt_snapshot_poisons_node() {
    use crate::{Config, Index, Instant, Term, conf::ConfState};
    use core::time::Duration;

    let cfg = Config::try_new(
      1u64,
      std::vec![1u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();

    let mut stable = crate::testkit::AsyncStable::default();
    // Store garbage — too short to decode a u64 count.
    let bad_data = bytes::Bytes::from_static(b"\x01\x02\x03");
    let meta = crate::SnapshotMeta::new(
      Index::new(5),
      Term::new(2),
      ConfState::from_voters(std::vec![1u64]),
    );
    stable.submit_snapshot(crate::OpId::new(1), meta, bad_data);
    while stable.poll().is_some() {}
    stable.force_state(Term::new(2), None, Index::new(7));

    let mut log = crate::testkit::VecLog::default();
    log.restore(Index::new(5), Term::new(2));

    let ep = Endpoint::restart(
      cfg,
      Instant::ORIGIN,
      42,
      crate::testkit::CountSm::default(),
      &mut log,
      &mut stable,
    );

    assert!(
      ep.is_poisoned(),
      "node must be poisoned after corrupt snapshot"
    );
    // Applied must not have advanced past the snapshot boundary (no partial apply).
    assert_eq!(
      ep.state_machine().count(),
      0,
      "SM must be empty after corrupt snapshot (no partial apply)"
    );
  }
}
