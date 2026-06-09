//! The Sans-I/O Raft core. M0 is a no-op skeleton: it owns state and exposes the
//! `handle_*`/`poll_*` surface. M1 fills in leader election.
use crate::{
  Config, Event, Index, Instant, LogStore, Message, NodeId, Outgoing, Prng, StableStore,
  StateMachine, Term,
};
use std::collections::{BTreeSet, VecDeque};

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

/// The Sans-I/O Raft state machine for one node.
///
/// `I` is unbounded on the struct; `I: NodeId` belongs only on the `impl` blocks that
/// need it. `F: StateMachine` is the documented "bounds that gate storage shape" exception
/// (§8): the struct stores `Event<I, F::Response>`, which cannot be named without it.
#[derive(Debug)]
// M0 skeleton: some fields are written in `new` but not yet read — M2 fills them in.
// `expect` (not `allow`): once M2 reads these fields it becomes a stale-lint error, forcing removal.
#[expect(dead_code)]
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
}

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

  // --- INPUTS ---

  /// Feed an inbound message. Runs the universal term pre-pass then dispatches.
  pub fn handle_message<L, S>(
    &mut self,
    now: Instant,
    log: &mut L,
    stable: &mut S,
    _from: I,
    msg: Message<I>,
  ) where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
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
      Message::Heartbeat(hb) => self.on_heartbeat(now, hb),
      // AppendEntries/AppendResp/HeartbeatResp: M2 fills these; ignore in M1.
      _ => {}
    }
  }

  /// Fire due timers (election for followers/candidates, heartbeat for leaders).
  pub fn handle_timeout<L, S>(&mut self, now: Instant, log: &mut L, stable: &mut S)
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
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

  /// Drain storage completions. (M3+: append-before-ack / persist-vote.)
  pub fn handle_storage<L, S>(&mut self, _now: Instant, _log: &mut L, _stable: &mut S)
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
  }

  // --- OUTPUTS ---

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

  // --- PRIVATE HELPERS ---

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
    // M1: write synchronously; M3 introduces OpId/pending + deferral.
    let hs = stable
      .hard_state()
      .with_term(self.term)
      .with_vote(self.voted_for);
    stable.submit_write(crate::OpId::ZERO, hs);
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
    _log: &mut L,
    _stable: &mut S,
  ) {
    self.role = Role::Leader;
    self.leader = Some(self.config.id());
    self.arm_heartbeat_timer(now);
    self.broadcast_heartbeat(now);
    self
      .events
      .push_back(crate::Event::LeaderChanged(crate::LeaderChanged::new(
        self.term,
        Some(self.config.id()),
      )));
    // M2: append an Empty no-op entry here and initialize the Progress map.
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
      self.arm_election_timer(now); // granting resets our election timer
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
  ) {
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

  fn on_heartbeat(&mut self, now: Instant, hb: crate::Heartbeat<I>) {
    // term == self.term here (pre-pass handled >, and < returned early)
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
    let (term, me) = (self.term, self.config.id());
    self.send(
      hb.leader(),
      Message::HeartbeatResp(crate::HeartbeatResp::new(term, me, bytes::Bytes::new())),
    );
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::Instant;
  use core::time::Duration;

  struct Noop;

  impl crate::StateMachine for Noop {
    type Command = ();
    type Response = ();
    type Snapshot = ();
    type Error = core::convert::Infallible;

    fn apply(&mut self, _: crate::Index, _: ()) -> Result<(), Self::Error> {
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
}
