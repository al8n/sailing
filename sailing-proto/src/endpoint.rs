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
#[allow(dead_code)] // variants filled in M3-U2
enum Pending<I> {
  /// Emit `VoteResp(grant)` to `to` once the term+vote write is durable.
  CastVote { to: I },
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
  #[allow(dead_code)] // preserved for Endpoint::restart (M3-U3)
  seed: u64,
  votes_granted: BTreeSet<I>,
  election_deadline: Option<Instant>,
  heartbeat_deadline: Option<Instant>,
  outgoing: VecDeque<Outgoing<I>>,
  events: VecDeque<Event<I, F::Response>>,
  progress: BTreeMap<I, crate::Progress>,
  /// Monotonically minted id for every storage submission.
  #[allow(dead_code)] // read via mint_op_id; non-test callers added in M3-U2
  next_op_id: crate::OpId,
  /// Outstanding write → deferred action (filled in M3-U2; empty until then).
  pending: BTreeMap<crate::OpId, Pending<I>>,
  /// Sticky fatal error: once set, all `handle_*` are no-ops.
  poisoned: bool,
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
      seed,
      votes_granted: BTreeSet::new(),
      election_deadline: None,
      heartbeat_deadline: None,
      outgoing: VecDeque::new(),
      events: VecDeque::new(),
      progress: BTreeMap::new(),
      next_op_id: crate::OpId::ZERO,
      pending: BTreeMap::new(),
      poisoned: false,
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
  #[allow(dead_code)] // used in M3-U2 when Pending entries are created
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

  fn persist_hard_state<S: StableStore<NodeId = I>>(&mut self, stable: &mut S) {
    let hs = stable
      .hard_state()
      .with_term(self.term)
      .with_vote(self.voted_for);
    stable.submit_write(crate::OpId::ZERO, hs);
  }

  fn broadcast_heartbeat(&mut self, _now: Instant) {
    let (term, me, commit) = (self.term, self.config.id(), self.commit);
    for peer in self.peers().collect::<std::vec::Vec<_>>() {
      self.send(
        peer,
        Message::Heartbeat(crate::Heartbeat::new(term, me, commit, bytes::Bytes::new())),
      );
    }
  }

  fn maybe_send_append<L: LogStore>(&mut self, peer: I, log: &L) {
    let Some(pr) = self.progress.get(&peer).copied() else {
      return;
    };
    let next = pr.next_index();
    let prev_index = Index::new(next.get().saturating_sub(1));
    let prev_term = if prev_index == Index::ZERO {
      Term::ZERO
    } else {
      log.term(prev_index).unwrap_or(Term::ZERO)
    };
    let end = log.last_index().next();
    let entries = if next < end {
      log
        .entries(next..end, u64::MAX)
        .map(<[_]>::to_vec)
        .unwrap_or_default()
    } else {
      std::vec::Vec::new()
    };
    let (term, me, commit) = (self.term, self.config.id(), self.commit);
    self.send(
      peer,
      Message::AppendEntries(crate::AppendEntries::new(
        term, me, prev_index, prev_term, entries, commit,
      )),
    );
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
    let grant = can_vote && log_ok;
    if grant {
      self.voted_for = Some(rv.candidate());
      self.persist_hard_state(stable);
      self.arm_election_timer(now);
    }
    let (term, me) = (self.term, self.config.id());
    self.send(
      rv.candidate(),
      Message::VoteResp(crate::VoteResp::new(term, me, false, !grant)),
    );
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
        Ok(crate::StableDone::SnapshotWritten(_)) => {}
        Err(_) => {
          self.poison();
          return;
        }
      }
    }
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
      Some(Pending::LeaderAppend { upto }) => {
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
    if let Some(Pending::CastVote { to }) = self.pending.remove(&opid) {
      let (term, me) = (self.term, self.config.id());
      self.send(
        to,
        Message::VoteResp(crate::VoteResp::new(term, me, false, false)),
      );
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
      self.persist_hard_state(stable);
    }
    // Drop messages from a stale term (a CheckQuorum nudge is added in M7).
    if msg.term() < self.term {
      return;
    }
    match msg {
      Message::RequestVote(rv) => self.on_request_vote(now, log, stable, rv),
      Message::VoteResp(vr) => self.on_vote_resp(now, log, stable, vr),
      Message::Heartbeat(hb) => self.on_heartbeat(now, log, hb),
      Message::AppendEntries(ae) => self.on_append_entries(now, log, ae),
      Message::AppendResp(r) => self.on_append_resp(now, log, from, r),
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
    _stable: &mut S,
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
    log.submit_append(crate::OpId::ZERO, core::slice::from_ref(&entry));
    if let Some(p) = self.progress.get_mut(&self.config.id()) {
      p.maybe_update(index);
    }
    for peer in self.peers().collect::<std::vec::Vec<_>>() {
      self.maybe_send_append(peer, log);
    }
    self.maybe_advance_commit(log);
    self.apply_committed(log);
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
    self.role = Role::Candidate;
    self.leader = None;
    self.voted_for = Some(self.config.id());
    self.votes_granted.clear();
    self.votes_granted.insert(self.config.id());
    self.persist_hard_state(stable);
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
    _stable: &mut S,
  ) {
    self.role = Role::Leader;
    self.leader = Some(self.config.id());
    self.arm_heartbeat_timer(now);

    // Initialize Progress for every voter (self included; self is fully caught up).
    let last = log.last_index();
    self.progress.clear();
    for v in self.config.voters().to_vec() {
      let mut p = crate::Progress::new(last.next());
      if v == self.config.id() {
        p.maybe_update(last);
      }
      self.progress.insert(v, p);
    }

    // Append the new leader's no-op entry (lets it commit prior-term entries, §5.4.2).
    let noop_index = last.next();
    let noop = crate::Entry::new(
      self.term,
      noop_index,
      crate::EntryKind::Empty,
      bytes::Bytes::new(),
    );
    log.submit_append(crate::OpId::ZERO, core::slice::from_ref(&noop));
    if let Some(p) = self.progress.get_mut(&self.config.id()) {
      p.maybe_update(noop_index);
    }

    self
      .events
      .push_back(crate::Event::LeaderChanged(crate::LeaderChanged::new(
        self.term,
        Some(self.config.id()),
      )));

    // Broadcast heartbeats (M1 contract) and kick off replication to peers.
    self.broadcast_heartbeat(now);
    for peer in self.peers().collect::<std::vec::Vec<_>>() {
      self.maybe_send_append(peer, log);
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
      // M2: simple hint = our last_index (M4 adds term-skip). Leader will back off.
      self.send(
        ae.leader(),
        Message::AppendResp(crate::AppendResp::new(
          term,
          me,
          true,
          log.last_index(),
          Term::ZERO,
          Index::ZERO,
        )),
      );
      return;
    }

    // Raft §5.3: only delete-and-re-append from the first *conflicting* entry.
    // Entries that already match (same index, same term) are left untouched so that a
    // stale or duplicate AppendEntries never erases already-committed entries.
    let entries = ae.entries();
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
        log.submit_append(crate::OpId::ZERO, &entries[i..]);
      }
      // else: every entry already present (pure duplicate) — append nothing.
    }
    let last_new = Index::new(ae.prev_log_index().get() + ae.entries().len() as u64);

    // Advance commit (min with what we actually hold) and apply.
    let new_commit = core::cmp::min(ae.leader_commit(), last_new);
    if new_commit > self.commit {
      self.commit = new_commit;
      self.apply_committed(log);
    }
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

  fn on_append_resp<L: LogStore>(
    &mut self,
    _now: Instant,
    log: &mut L,
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
      pr.decrement(); // M4: use the term-skip hint instead
      self.maybe_send_append(from, log);
    } else if pr.maybe_update(resp.match_index()) {
      pr.become_replicate();
      self.maybe_advance_commit(log);
      self.apply_committed(log);
      self.maybe_send_append(from, log); // keep the pipeline moving if still behind
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
    let mut stable = crate::testkit::NoopStable::default();

    // candidate 1 in term 1, empty log — should be granted
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
    let vr = ep.poll_message().unwrap();
    assert!(matches!(vr.message(), Message::VoteResp(v) if !v.reject() && v.from()==2));
    assert_eq!(ep.term(), Term::new(1));

    // candidate 3 in the SAME term — already voted for 1, reject
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

    // matching append at index 1 (prev=0)
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
    let r = ep.poll_message().unwrap();
    assert!(
      matches!(r.message(), Message::AppendResp(a) if !a.reject() && a.match_index()==Index::new(1))
    );
    assert_eq!(log.last_index(), Index::new(1));

    // gap: prev_log_index=5 we don't have → reject
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
    while ep.poll_message().is_some() {}
    while ep.poll_event().is_some() {}
    let idx = ep
      .propose(d, &mut log, &mut stable, &bytes::Bytes::from_static(b"x"))
      .unwrap(); // index 2
    while ep.poll_message().is_some() {}

    // peer 2 acks up to idx 2 → quorum (self + peer2) → commit + apply
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
    // Applied event for the Normal entry at idx 2 (the no-op at 1 is skipped)
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
}
