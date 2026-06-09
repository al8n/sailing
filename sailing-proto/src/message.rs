//! Raft RPC messages. Payloads are named structs; `Message<I>` wraps them as newtype
//! variants (no multi-field enum variants). Types only — behavior lands in M1–M3.
use crate::{Entry, Index, Term, conf::ConfState};
use bytes::Bytes;
use std::vec::Vec;

/// AppendEntries / heartbeat-with-entries (log replication).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntries<I> {
  term: Term,
  leader: I,
  prev_log_index: Index,
  prev_log_term: Term,
  entries: Vec<Entry>,
  leader_commit: Index,
}

impl<I: Copy> AppendEntries<I> {
  /// Construct.
  #[allow(clippy::too_many_arguments)]
  pub fn new(
    term: Term,
    leader: I,
    prev_log_index: Index,
    prev_log_term: Term,
    entries: Vec<Entry>,
    leader_commit: Index,
  ) -> Self {
    Self {
      term,
      leader,
      prev_log_index,
      prev_log_term,
      entries,
      leader_commit,
    }
  }

  /// The message term.
  #[inline(always)]
  pub const fn term(&self) -> Term {
    self.term
  }

  /// The leader node id.
  #[inline(always)]
  pub const fn leader(&self) -> I {
    self.leader
  }

  /// The index immediately preceding the new entries.
  #[inline(always)]
  pub const fn prev_log_index(&self) -> Index {
    self.prev_log_index
  }

  /// The term of `prev_log_index`.
  #[inline(always)]
  pub const fn prev_log_term(&self) -> Term {
    self.prev_log_term
  }

  /// The entries to append (may be empty for heartbeat-with-entries).
  #[inline(always)]
  pub fn entries(&self) -> &[Entry] {
    &self.entries
  }

  /// The leader's committed index.
  #[inline(always)]
  pub const fn leader_commit(&self) -> Index {
    self.leader_commit
  }
}

/// Response to AppendEntries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendResp<I> {
  term: Term,
  from: I,
  reject: bool,
  reject_hint_index: Index,
  reject_hint_term: Term,
  match_index: Index,
}

impl<I: Copy> AppendResp<I> {
  /// Construct.
  #[allow(clippy::too_many_arguments)]
  pub const fn new(
    term: Term,
    from: I,
    reject: bool,
    reject_hint_index: Index,
    reject_hint_term: Term,
    match_index: Index,
  ) -> Self {
    Self {
      term,
      from,
      reject,
      reject_hint_index,
      reject_hint_term,
      match_index,
    }
  }

  /// The respondent's current term.
  #[inline(always)]
  pub const fn term(&self) -> Term {
    self.term
  }

  /// The sender's node id.
  #[inline(always)]
  pub const fn from(&self) -> I {
    self.from
  }

  /// Whether the append was rejected.
  #[inline(always)]
  pub const fn reject(&self) -> bool {
    self.reject
  }

  /// The follower's hint index on rejection (for fast log backtrack).
  #[inline(always)]
  pub const fn reject_hint_index(&self) -> Index {
    self.reject_hint_index
  }

  /// The follower's hint term on rejection.
  #[inline(always)]
  pub const fn reject_hint_term(&self) -> Term {
    self.reject_hint_term
  }

  /// The highest index the follower has durably appended (on success).
  #[inline(always)]
  pub const fn match_index(&self) -> Index {
    self.match_index
  }
}

/// RequestVote (carries `pre_vote` for PreVote and `leader_transfer` for forced campaigns).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestVote<I> {
  term: Term,
  candidate: I,
  last_log_index: Index,
  last_log_term: Term,
  pre_vote: bool,
  leader_transfer: bool,
}

impl<I: Copy> RequestVote<I> {
  /// Construct.
  #[allow(clippy::too_many_arguments)]
  pub const fn new(
    term: Term,
    candidate: I,
    last_log_index: Index,
    last_log_term: Term,
    pre_vote: bool,
    leader_transfer: bool,
  ) -> Self {
    Self {
      term,
      candidate,
      last_log_index,
      last_log_term,
      pre_vote,
      leader_transfer,
    }
  }

  /// The candidate's term.
  #[inline(always)]
  pub const fn term(&self) -> Term {
    self.term
  }

  /// The candidate's node id.
  #[inline(always)]
  pub const fn candidate(&self) -> I {
    self.candidate
  }

  /// The candidate's last log index.
  #[inline(always)]
  pub const fn last_log_index(&self) -> Index {
    self.last_log_index
  }

  /// The candidate's last log term.
  #[inline(always)]
  pub const fn last_log_term(&self) -> Term {
    self.last_log_term
  }

  /// Whether this is a PreVote (does not increment term).
  #[inline(always)]
  pub const fn pre_vote(&self) -> bool {
    self.pre_vote
  }

  /// Whether this is a leader-transfer-triggered campaign.
  #[inline(always)]
  pub const fn leader_transfer(&self) -> bool {
    self.leader_transfer
  }
}

/// Response to RequestVote / PreVote.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VoteResp<I> {
  term: Term,
  from: I,
  pre_vote: bool,
  reject: bool,
}

impl<I: Copy> VoteResp<I> {
  /// Construct.
  pub const fn new(term: Term, from: I, pre_vote: bool, reject: bool) -> Self {
    Self {
      term,
      from,
      pre_vote,
      reject,
    }
  }

  /// The respondent's current term.
  #[inline(always)]
  pub const fn term(&self) -> Term {
    self.term
  }

  /// The sender's node id.
  #[inline(always)]
  pub const fn from(&self) -> I {
    self.from
  }

  /// Whether this is a PreVote response.
  #[inline(always)]
  pub const fn pre_vote(&self) -> bool {
    self.pre_vote
  }

  /// Whether the vote was denied.
  #[inline(always)]
  pub const fn reject(&self) -> bool {
    self.reject
  }
}

/// Heartbeat (carries `context` for the ReadIndex round).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Heartbeat<I> {
  term: Term,
  leader: I,
  commit: Index,
  context: Bytes,
}

impl<I: Copy> Heartbeat<I> {
  /// Construct.
  pub fn new(term: Term, leader: I, commit: Index, context: Bytes) -> Self {
    Self {
      term,
      leader,
      commit,
      context,
    }
  }

  /// The leader's term.
  #[inline(always)]
  pub const fn term(&self) -> Term {
    self.term
  }

  /// The leader's node id.
  #[inline(always)]
  pub const fn leader(&self) -> I {
    self.leader
  }

  /// The leader's committed index.
  #[inline(always)]
  pub const fn commit(&self) -> Index {
    self.commit
  }

  /// Opaque context bytes for the ReadIndex round (empty when not used).
  #[inline(always)]
  pub fn context(&self) -> &[u8] {
    &self.context
  }
}

/// Response to Heartbeat.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatResp<I> {
  term: Term,
  from: I,
  context: Bytes,
}

impl<I: Copy> HeartbeatResp<I> {
  /// Construct.
  pub fn new(term: Term, from: I, context: Bytes) -> Self {
    Self {
      term,
      from,
      context,
    }
  }

  /// The respondent's current term.
  #[inline(always)]
  pub const fn term(&self) -> Term {
    self.term
  }

  /// The sender's node id.
  #[inline(always)]
  pub const fn from(&self) -> I {
    self.from
  }

  /// Opaque context bytes echoed from the heartbeat (empty when not used).
  #[inline(always)]
  pub fn context(&self) -> &[u8] {
    &self.context
  }
}

/// Metadata describing a snapshot (the logical "header" without the raw blob).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotMeta<I> {
  last_index: Index,
  last_term: Term,
  conf: ConfState<I>,
}

impl<I: crate::NodeId> SnapshotMeta<I> {
  /// Construct.
  pub fn new(last_index: Index, last_term: Term, conf: ConfState<I>) -> Self {
    Self {
      last_index,
      last_term,
      conf,
    }
  }

  /// The last log index covered by this snapshot.
  #[inline(always)]
  pub const fn last_index(&self) -> Index {
    self.last_index
  }

  /// The term of `last_index`.
  #[inline(always)]
  pub const fn last_term(&self) -> Term {
    self.last_term
  }

  /// The cluster configuration at the snapshot point.
  #[inline(always)]
  pub fn conf(&self) -> &ConfState<I> {
    &self.conf
  }
}

/// Leader → follower: install a snapshot (follower is too far behind to catch up via entries).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstallSnapshot<I> {
  term: Term,
  leader: I,
  snapshot: SnapshotMeta<I>,
  data: Bytes,
}

impl<I: crate::NodeId> InstallSnapshot<I> {
  /// Construct.
  pub fn new(term: Term, leader: I, snapshot: SnapshotMeta<I>, data: Bytes) -> Self {
    Self {
      term,
      leader,
      snapshot,
      data,
    }
  }

  /// The leader's current term.
  #[inline(always)]
  pub const fn term(&self) -> Term {
    self.term
  }

  /// The leader's node id.
  #[inline(always)]
  pub fn leader(&self) -> I
  where
    I: Copy,
  {
    self.leader
  }

  /// Snapshot metadata (last covered index/term + conf).
  #[inline(always)]
  pub fn snapshot(&self) -> &SnapshotMeta<I> {
    &self.snapshot
  }

  /// The raw snapshot blob.
  #[inline(always)]
  pub fn data(&self) -> &Bytes {
    &self.data
  }
}

/// Follower → leader: acknowledgement of an `InstallSnapshot`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotResp<I> {
  term: Term,
  from: I,
  reject: bool,
  match_index: Index,
}

impl<I: Copy> SnapshotResp<I> {
  /// Construct.
  pub const fn new(term: Term, from: I, reject: bool, match_index: Index) -> Self {
    Self {
      term,
      from,
      reject,
      match_index,
    }
  }

  /// The respondent's current term.
  #[inline(always)]
  pub const fn term(&self) -> Term {
    self.term
  }

  /// The sender's node id.
  #[inline(always)]
  pub const fn from(&self) -> I {
    self.from
  }

  /// Whether the snapshot was rejected (stale or follower is already ahead).
  #[inline(always)]
  pub const fn reject(&self) -> bool {
    self.reject
  }

  /// The follower's match index after applying the snapshot (on success).
  #[inline(always)]
  pub const fn match_index(&self) -> Index {
    self.match_index
  }
}

/// The full Raft message set. `#[non_exhaustive]` for forward-compat; derive variant
/// predicates + unwrap accessors per §2.
///
/// `TimeoutNow`, `ReadIndex`, `ReadIndexResp` are added as new newtype variants in their
/// milestones (M7); `#[non_exhaustive]` makes that additive.
#[derive(
  Debug, Clone, PartialEq, Eq, derive_more::IsVariant, derive_more::Unwrap, derive_more::TryUnwrap,
)]
#[unwrap(ref, ref_mut)]
#[try_unwrap(ref, ref_mut)]
#[non_exhaustive]
pub enum Message<I> {
  /// Log replication / heartbeat-with-entries.
  AppendEntries(AppendEntries<I>),
  /// AppendEntries response.
  AppendResp(AppendResp<I>),
  /// Vote / PreVote request.
  RequestVote(RequestVote<I>),
  /// Vote / PreVote response.
  VoteResp(VoteResp<I>),
  /// Leader heartbeat.
  Heartbeat(Heartbeat<I>),
  /// Heartbeat response.
  HeartbeatResp(HeartbeatResp<I>),
  /// Leader → follower: install a snapshot (follower is too far behind).
  InstallSnapshot(InstallSnapshot<I>),
  /// Follower → leader: acknowledgement of an `InstallSnapshot`.
  SnapshotResp(SnapshotResp<I>),
}

/// A typed message addressed to a peer. The driver frames + sends it; the sim moves it
/// as a value over the typed-message bus.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outgoing<I> {
  to: I,
  message: Message<I>,
}

impl<I: Copy> Outgoing<I> {
  /// Construct.
  pub const fn new(to: I, message: Message<I>) -> Self {
    Self { to, message }
  }

  /// The recipient.
  #[inline(always)]
  pub const fn to(&self) -> I {
    self.to
  }

  /// The message.
  #[inline(always)]
  pub const fn message(&self) -> &Message<I> {
    &self.message
  }

  /// Consume into `(to, message)`.
  #[inline(always)]
  pub fn into_parts(self) -> (I, Message<I>) {
    (self.to, self.message)
  }
}

impl<I: crate::NodeId> Message<I> {
  /// The term carried by this message (every variant carries one).
  pub fn term(&self) -> crate::Term {
    match self {
      Self::AppendEntries(m) => m.term(),
      Self::AppendResp(m) => m.term(),
      Self::RequestVote(m) => m.term(),
      Self::VoteResp(m) => m.term(),
      Self::Heartbeat(m) => m.term(),
      Self::HeartbeatResp(m) => m.term(),
      Self::InstallSnapshot(m) => m.term(),
      Self::SnapshotResp(m) => m.term(),
    }
  }
}

#[cfg(test)]
mod term_test {
  use super::*;

  #[test]
  fn message_term() {
    let m = Message::Heartbeat(Heartbeat::new(
      crate::Term::new(5),
      1u64,
      crate::Index::ZERO,
      bytes::Bytes::new(),
    ));
    assert_eq!(m.term(), crate::Term::new(5));
  }

  #[test]
  fn install_snapshot_message_term() {
    use crate::conf::ConfState;
    let meta = SnapshotMeta::new(
      crate::Index::new(10),
      crate::Term::new(3),
      ConfState::new(std::vec![1u64, 2u64, 3u64]),
    );
    let snap = InstallSnapshot::new(
      crate::Term::new(7),
      1u64,
      meta,
      bytes::Bytes::from_static(b"snap"),
    );
    let m = Message::InstallSnapshot(snap);
    assert_eq!(m.term(), crate::Term::new(7));
  }

  #[test]
  fn snapshot_resp_message_term() {
    let resp = SnapshotResp::new(crate::Term::new(4), 2u64, false, crate::Index::new(10));
    let m = Message::SnapshotResp(resp);
    assert_eq!(m.term(), crate::Term::new(4));
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn message_construct_and_classify() {
    let rv = RequestVote::new(
      Term::new(2),
      1u64,
      Index::new(5),
      Term::new(1),
      false,
      false,
    );
    let m = Message::RequestVote(rv);
    assert!(m.is_request_vote());
    assert_eq!(m.try_unwrap_request_vote().unwrap().term(), Term::new(2));

    let out = Outgoing::new(
      3u64,
      Message::Heartbeat(Heartbeat::new(
        Term::new(2),
        1u64,
        Index::new(4),
        bytes::Bytes::new(),
      )),
    );
    assert_eq!(out.to(), 3u64);
    assert!(out.message().is_heartbeat());
  }

  #[test]
  fn snapshot_meta_accessors() {
    use crate::conf::ConfState;
    let voters = std::vec![1u64, 2u64, 3u64];
    let conf = ConfState::new(voters.clone());
    let meta = SnapshotMeta::new(Index::new(42), Term::new(5), conf);
    assert_eq!(meta.last_index(), Index::new(42));
    assert_eq!(meta.last_term(), Term::new(5));
    assert_eq!(meta.conf().voters(), voters.as_slice());
  }

  #[test]
  fn install_snapshot_accessors() {
    use crate::conf::ConfState;
    let meta = SnapshotMeta::new(
      Index::new(10),
      Term::new(3),
      ConfState::new(std::vec![1u64]),
    );
    let data = bytes::Bytes::from_static(b"payload");
    let snap = InstallSnapshot::new(Term::new(7), 1u64, meta.clone(), data.clone());
    assert_eq!(snap.term(), Term::new(7));
    assert_eq!(snap.leader(), 1u64);
    assert_eq!(snap.snapshot().last_index(), meta.last_index());
    assert_eq!(snap.data(), &data);

    let m = Message::InstallSnapshot(snap);
    assert!(m.is_install_snapshot());
  }

  #[test]
  fn snapshot_resp_accessors() {
    let resp = SnapshotResp::new(Term::new(4), 2u64, false, Index::new(10));
    assert_eq!(resp.term(), Term::new(4));
    assert_eq!(resp.from(), 2u64);
    assert!(!resp.reject());
    assert_eq!(resp.match_index(), Index::new(10));

    let m = Message::SnapshotResp(resp);
    assert!(m.is_snapshot_resp());
  }
}
