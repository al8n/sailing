//! The Sans-I/O Raft core. M0 is a no-op skeleton: it owns state and exposes the
//! `handle_*`/`poll_*` surface, but the methods do nothing yet (filled in M1–M3).
use crate::{
  Config, Event, Index, Instant, LogStore, Message, NodeId, Outgoing, StableStore, StateMachine,
  Term,
};
use std::collections::VecDeque;

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
// M0 skeleton: several fields are written in `new` but not yet read — M1 fills them in.
// `expect` (not `allow`): once M1 reads these fields it becomes a stale-lint error, forcing removal.
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
  /// Deterministic PRNG seed for randomized election timeouts (used in M1).
  seed: u64,
  election_deadline: Option<Instant>,
  outgoing: VecDeque<Outgoing<I>>,
  events: VecDeque<Event<I, F::Response>>,
}

impl<I, F> Endpoint<I, F>
where
  I: NodeId,
  F: StateMachine,
{
  /// Create a fresh node (status Follower, term 0, empty log view).
  /// M0: no timer is armed; `election_deadline` starts `None` (M1 arms it).
  pub fn new(config: Config<I>, _now: Instant, seed: u64, fsm: F) -> Self {
    Self {
      config,
      fsm,
      role: Role::Follower,
      term: Term::ZERO,
      voted_for: None,
      leader: None,
      commit: Index::ZERO,
      applied: Index::ZERO,
      seed,
      election_deadline: None,
      outgoing: VecDeque::new(),
      events: VecDeque::new(),
    }
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

  // --- INPUTS (no-ops in M0) ---

  /// Feed an inbound message. (M1+: dispatch by term + role.)
  pub fn handle_message<L, S>(
    &mut self,
    _now: Instant,
    _log: &mut L,
    _stable: &mut S,
    _from: I,
    _msg: Message<I>,
  ) where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
  }

  /// Fire due timers. (M1+: election/heartbeat.)
  pub fn handle_timeout<L, S>(&mut self, _now: Instant, _log: &mut L, _stable: &mut S)
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
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

  /// The earliest armed deadline, if any. (M1+: serviceable-timer filter.)
  #[inline]
  pub fn poll_timeout(&self) -> Option<Instant> {
    self.election_deadline
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
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
    let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 42, Noop);
    assert_eq!(ep.id(), 1u64);
    assert!(ep.poll_message().is_none());
    assert!(ep.poll_event().is_none());
    // no timers armed yet (M1 arms the election timer)
    assert!(ep.poll_timeout().is_none());
  }
}
