//! Raft RPC messages. Payloads are named structs; `Message<I>` wraps them as newtype
//! variants (no multi-field enum variants). Types only — behavior lives elsewhere.
use crate::{Data, DecodeError, Entry, Index, NodeId, Term, conf::ConfState};
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

/// Heartbeat (carries `context` for the ReadIndex round and `lease_round` for the CheckQuorum lease).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Heartbeat<I> {
  term: Term,
  leader: I,
  commit: Index,
  context: Bytes,
  lease_round: u64,
}

impl<I: Copy> Heartbeat<I> {
  /// Construct. `lease_round` defaults to 0; the leader sets it via [`Self::with_lease_round`].
  pub fn new(term: Term, leader: I, commit: Index, context: Bytes) -> Self {
    Self {
      term,
      leader,
      commit,
      context,
      lease_round: 0,
    }
  }

  /// Set the per-round CheckQuorum lease token (builder). The follower echoes it in
  /// [`HeartbeatResp`] so the leader confirms a quorum responded to THIS round, not a stale one.
  #[inline(always)]
  pub fn with_lease_round(mut self, lease_round: u64) -> Self {
    self.lease_round = lease_round;
    self
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

  /// The per-round CheckQuorum lease token (0 when the lease is not in use).
  #[inline(always)]
  pub const fn lease_round(&self) -> u64 {
    self.lease_round
  }
}

/// Response to Heartbeat.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatResp<I> {
  term: Term,
  from: I,
  context: Bytes,
  lease_round: u64,
  lease_support: core::time::Duration,
}

impl<I: Copy> HeartbeatResp<I> {
  /// Construct. `lease_round` defaults to 0 and `lease_support` to ZERO; the follower echoes the
  /// heartbeat's round via [`Self::with_lease_round`] and advertises its lease support via
  /// [`Self::with_lease_support`].
  pub fn new(term: Term, from: I, context: Bytes) -> Self {
    Self {
      term,
      from,
      context,
      lease_round: 0,
      lease_support: core::time::Duration::ZERO,
    }
  }

  /// Echo the heartbeat's per-round CheckQuorum lease token (builder).
  #[inline(always)]
  pub fn with_lease_round(mut self, lease_round: u64) -> Self {
    self.lease_round = lease_round;
    self
  }

  /// Advertise how long this follower will UPHOLD the leader's read-lease window (builder) — i.e. how
  /// long it will refuse to help elect a new leader after receiving this round's heartbeat. A follower
  /// that does not enforce the lease (neither `check_quorum` nor `pre_vote`) advertises `ZERO`, so the
  /// leader does not count it toward the lease quorum (the self-validating lease). A non-zero value is
  /// the follower's own `election_timeout`, letting the leader bound the lease by the quorum's actual
  /// support even under heterogeneous `election_timeout`.
  #[inline(always)]
  pub fn with_lease_support(mut self, lease_support: core::time::Duration) -> Self {
    self.lease_support = lease_support;
    self
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

  /// The per-round CheckQuorum lease token echoed from the heartbeat (0 when not in use).
  #[inline(always)]
  pub const fn lease_round(&self) -> u64 {
    self.lease_round
  }

  /// How long this follower will uphold the leader's read-lease window (ZERO if it does not enforce the
  /// lease, so the leader must NOT count it toward the lease quorum). See [`Self::with_lease_support`].
  #[inline(always)]
  pub const fn lease_support(&self) -> core::time::Duration {
    self.lease_support
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

/// Leader → follower: "campaign immediately" signal used during leader transfer.
///
/// The recipient bypasses PreVote and any lease check (this is an authorized handoff) and
/// immediately calls for a real election. Carries the sender's term so the recipient can
/// accept it through the normal term pre-pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeoutNow<I> {
  term: Term,
  leader: I,
}

impl<I: Copy> TimeoutNow<I> {
  /// Construct.
  pub const fn new(term: Term, leader: I) -> Self {
    Self { term, leader }
  }

  /// The sending leader's current term.
  #[inline(always)]
  pub const fn term(&self) -> Term {
    self.term
  }

  /// The sending leader's node id.
  #[inline(always)]
  pub const fn leader(&self) -> I {
    self.leader
  }
}

/// Follower → leader: forward a linearizable read request.
///
/// A follower that receives a read request from a client forwards it to the known leader
/// using this message. The leader processes it (confirming its leadership via a heartbeat
/// round for `ReadOnlySafe`, or via the lease for `ReadOnlyLeaseBased`) and replies with
/// a [`ReadIndexResp`].
///
/// `term` is set to the sender's current term so the message is not dropped by the leader's
/// term pre-pass (which drops any message whose term is less than the leader's).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadIndex<I> {
  term: Term,
  from: I,
  context: Bytes,
}

impl<I: Copy> ReadIndex<I> {
  /// Construct.
  pub fn new(term: Term, from: I, context: Bytes) -> Self {
    Self {
      term,
      from,
      context,
    }
  }

  /// The sender's current term.
  #[inline(always)]
  pub const fn term(&self) -> Term {
    self.term
  }

  /// The sender's node id.
  #[inline(always)]
  pub const fn from(&self) -> I {
    self.from
  }

  /// Opaque application context identifying this read request.
  #[inline(always)]
  pub fn context(&self) -> &[u8] {
    &self.context
  }
}

/// Leader → follower: the confirmed read index for a forwarded read request.
///
/// After the leader confirms its leadership (heartbeat round or lease), it replies to the
/// follower with the current committed index. The follower surfaces a `ReadState` once
/// its `applied` index reaches `index`.
///
/// `term` is set to the sender's (leader's) current term so the message is not dropped by
/// the follower's term pre-pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadIndexResp<I> {
  term: Term,
  from: I,
  index: Index,
  context: Bytes,
  reject: bool,
}

impl<I: Copy> ReadIndexResp<I> {
  /// Construct.
  ///
  /// `reject` is `true` when the leader is at its read back-pressure capacity and is declining
  /// this forwarded read: the follower must clear its corresponding `forwarded_reads` entry (so
  /// the read can be re-issued) and must NOT surface a `ReadState`, since `index` is meaningless
  /// on a rejection.
  pub fn new(term: Term, from: I, index: Index, context: Bytes, reject: bool) -> Self {
    Self {
      term,
      from,
      index,
      context,
      reject,
    }
  }

  /// The sender's (leader's) current term.
  #[inline(always)]
  pub const fn term(&self) -> Term {
    self.term
  }

  /// The sender's node id.
  #[inline(always)]
  pub const fn from(&self) -> I {
    self.from
  }

  /// The confirmed committed index the follower must wait for before serving the read.
  #[inline(always)]
  pub const fn index(&self) -> Index {
    self.index
  }

  /// Opaque application context echoed from the original [`ReadIndex`] request.
  #[inline(always)]
  pub fn context(&self) -> &[u8] {
    &self.context
  }

  /// Whether the leader DECLINED this forwarded read (it was at read back-pressure capacity).
  /// A rejecting response carries no usable `index`; the follower must clear the forwarded-read
  /// entry for `context` and not emit a `ReadState`.
  #[inline(always)]
  pub const fn reject(&self) -> bool {
    self.reject
  }
}

/// The full Raft message set. `#[non_exhaustive]` for forward-compat; derive variant
/// predicates + unwrap accessors per §2.
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
  /// Leader → transfer target: campaign immediately (leader transfer).
  TimeoutNow(TimeoutNow<I>),
  /// Follower → leader: forward a linearizable read request.
  ReadIndex(ReadIndex<I>),
  /// Leader → follower: confirmed read index for a forwarded read request.
  ReadIndexResp(ReadIndexResp<I>),
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
  /// The term carried by this message.
  ///
  /// Every variant carries a term field. For [`TimeoutNow`], [`ReadIndex`], and
  /// [`ReadIndexResp`] the term is the sender's current term — it is included so that the
  /// receiver's term pre-pass (`msg.term() < self.term → drop`) does not accidentally drop
  /// these messages. Callers must set the term to the sender's current term when
  /// constructing these messages.
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
      Self::TimeoutNow(m) => m.term(),
      Self::ReadIndex(m) => m.term(),
      Self::ReadIndexResp(m) => m.term(),
    }
  }

  /// The sender id carried by this message. Every variant records the node that sent it, so the
  /// receiver can reject any message whose self-reported sender disagrees with the transport peer
  /// it actually arrived from — closing payload-sender spoofing for every message type at once.
  pub fn from(&self) -> I {
    match self {
      // Leader/candidate-originated messages name their sender as `leader`/`candidate`.
      Self::AppendEntries(m) => m.leader(),
      Self::RequestVote(m) => m.candidate(),
      Self::Heartbeat(m) => m.leader(),
      Self::InstallSnapshot(m) => m.leader(),
      Self::TimeoutNow(m) => m.leader(),
      // Responses and forwarded reads carry an explicit `from`.
      Self::AppendResp(m) => m.from(),
      Self::VoteResp(m) => m.from(),
      Self::HeartbeatResp(m) => m.from(),
      Self::SnapshotResp(m) => m.from(),
      Self::ReadIndex(m) => m.from(),
      Self::ReadIndexResp(m) => m.from(),
    }
  }
}

// ─── Wire codec (`Data`) ──────────────────────────────────────────────────────
// A leading tag byte selects the variant; each field encodes via its own `Data` impl.
// Variable-length fields (`Bytes`, `Vec<Entry>`, the `ConfState` sets) route through
// the bounds-checked `decode_len`, so no length prefix can drive an oversized allocation.
//
// The NORMATIVE byte-level format (tag table, field orders, canonicality rules, the frame and
// hello layouts) is pinned in `sailing-proto/WIRE.md`; any change here updates that document, the
// golden vectors in `message/tests.rs`, and the transport hello version in the same commit.

impl<I: NodeId> Data for SnapshotMeta<I> {
  fn encode(&self, buf: &mut Vec<u8>) {
    self.last_index.encode(buf);
    self.last_term.encode(buf);
    self.conf.encode(buf);
  }

  fn decode(cur: &mut crate::data::ByteCursor) -> Result<Self, DecodeError> {
    let last_index = <Index>::decode(cur)?;
    let last_term = <Term>::decode(cur)?;
    let conf = <ConfState<I>>::decode(cur)?;
    Ok(Self::new(last_index, last_term, conf))
  }
}

impl<I: NodeId> Data for AppendEntries<I> {
  fn encode(&self, buf: &mut Vec<u8>) {
    self.term.encode(buf);
    self.leader.encode(buf);
    self.prev_log_index.encode(buf);
    self.prev_log_term.encode(buf);
    self.entries.encode(buf);
    self.leader_commit.encode(buf);
  }

  fn decode(cur: &mut crate::data::ByteCursor) -> Result<Self, DecodeError> {
    let term = <Term>::decode(cur)?;
    let leader = <I>::decode(cur)?;
    let prev_log_index = <Index>::decode(cur)?;
    let prev_log_term = <Term>::decode(cur)?;
    let entries = <Vec<Entry>>::decode(cur)?;
    let leader_commit = <Index>::decode(cur)?;
    Ok(Self::new(
      term,
      leader,
      prev_log_index,
      prev_log_term,
      entries,
      leader_commit,
    ))
  }
}

impl<I: NodeId> Data for AppendResp<I> {
  fn encode(&self, buf: &mut Vec<u8>) {
    self.term.encode(buf);
    self.from.encode(buf);
    self.reject.encode(buf);
    self.reject_hint_index.encode(buf);
    self.reject_hint_term.encode(buf);
    self.match_index.encode(buf);
  }

  fn decode(cur: &mut crate::data::ByteCursor) -> Result<Self, DecodeError> {
    let term = <Term>::decode(cur)?;
    let from = <I>::decode(cur)?;
    let reject = <bool>::decode(cur)?;
    let reject_hint_index = <Index>::decode(cur)?;
    let reject_hint_term = <Term>::decode(cur)?;
    let match_index = <Index>::decode(cur)?;
    Ok(Self::new(
      term,
      from,
      reject,
      reject_hint_index,
      reject_hint_term,
      match_index,
    ))
  }
}

impl<I: NodeId> Data for RequestVote<I> {
  fn encode(&self, buf: &mut Vec<u8>) {
    self.term.encode(buf);
    self.candidate.encode(buf);
    self.last_log_index.encode(buf);
    self.last_log_term.encode(buf);
    self.pre_vote.encode(buf);
    self.leader_transfer.encode(buf);
  }

  fn decode(cur: &mut crate::data::ByteCursor) -> Result<Self, DecodeError> {
    let term = <Term>::decode(cur)?;
    let candidate = <I>::decode(cur)?;
    let last_log_index = <Index>::decode(cur)?;
    let last_log_term = <Term>::decode(cur)?;
    let pre_vote = <bool>::decode(cur)?;
    let leader_transfer = <bool>::decode(cur)?;
    Ok(Self::new(
      term,
      candidate,
      last_log_index,
      last_log_term,
      pre_vote,
      leader_transfer,
    ))
  }
}

impl<I: NodeId> Data for VoteResp<I> {
  fn encode(&self, buf: &mut Vec<u8>) {
    self.term.encode(buf);
    self.from.encode(buf);
    self.pre_vote.encode(buf);
    self.reject.encode(buf);
  }

  fn decode(cur: &mut crate::data::ByteCursor) -> Result<Self, DecodeError> {
    let term = <Term>::decode(cur)?;
    let from = <I>::decode(cur)?;
    let pre_vote = <bool>::decode(cur)?;
    let reject = <bool>::decode(cur)?;
    Ok(Self::new(term, from, pre_vote, reject))
  }
}

impl<I: NodeId> Data for Heartbeat<I> {
  fn encode(&self, buf: &mut Vec<u8>) {
    self.term.encode(buf);
    self.leader.encode(buf);
    self.commit.encode(buf);
    self.context.encode(buf);
    self.lease_round.encode(buf);
  }

  fn decode(cur: &mut crate::data::ByteCursor) -> Result<Self, DecodeError> {
    let term = <Term>::decode(cur)?;
    let leader = <I>::decode(cur)?;
    let commit = <Index>::decode(cur)?;
    let context = <Bytes>::decode(cur)?;
    let lease_round = <u64>::decode(cur)?;
    Ok(Self::new(term, leader, commit, context).with_lease_round(lease_round))
  }
}

impl<I: NodeId> Data for HeartbeatResp<I> {
  fn encode(&self, buf: &mut Vec<u8>) {
    self.term.encode(buf);
    self.from.encode(buf);
    self.context.encode(buf);
    self.lease_round.encode(buf);
    self.lease_support.as_secs().encode(buf);
    (self.lease_support.subsec_nanos() as u64).encode(buf);
  }

  fn decode(cur: &mut crate::data::ByteCursor) -> Result<Self, DecodeError> {
    let term = <Term>::decode(cur)?;
    let from = <I>::decode(cur)?;
    let context = <Bytes>::decode(cur)?;
    let lease_round = <u64>::decode(cur)?;
    let secs = <u64>::decode(cur)?;
    let nanos = <u64>::decode(cur)?;
    if nanos >= 1_000_000_000 {
      return Err(DecodeError::Invalid("duration nanos"));
    }
    let lease_support = core::time::Duration::new(secs, nanos as u32);
    Ok(
      Self::new(term, from, context)
        .with_lease_round(lease_round)
        .with_lease_support(lease_support),
    )
  }
}

impl<I: NodeId> Data for InstallSnapshot<I> {
  fn encode(&self, buf: &mut Vec<u8>) {
    self.term.encode(buf);
    self.leader.encode(buf);
    self.snapshot.encode(buf);
    self.data.encode(buf);
  }

  fn decode(cur: &mut crate::data::ByteCursor) -> Result<Self, DecodeError> {
    let term = <Term>::decode(cur)?;
    let leader = <I>::decode(cur)?;
    let snapshot = <SnapshotMeta<I>>::decode(cur)?;
    let data = <Bytes>::decode(cur)?;
    Ok(Self::new(term, leader, snapshot, data))
  }
}

impl<I: NodeId> Data for SnapshotResp<I> {
  fn encode(&self, buf: &mut Vec<u8>) {
    self.term.encode(buf);
    self.from.encode(buf);
    self.reject.encode(buf);
    self.match_index.encode(buf);
  }

  fn decode(cur: &mut crate::data::ByteCursor) -> Result<Self, DecodeError> {
    let term = <Term>::decode(cur)?;
    let from = <I>::decode(cur)?;
    let reject = <bool>::decode(cur)?;
    let match_index = <Index>::decode(cur)?;
    Ok(Self::new(term, from, reject, match_index))
  }
}

impl<I: NodeId> Data for TimeoutNow<I> {
  fn encode(&self, buf: &mut Vec<u8>) {
    self.term.encode(buf);
    self.leader.encode(buf);
  }

  fn decode(cur: &mut crate::data::ByteCursor) -> Result<Self, DecodeError> {
    let term = <Term>::decode(cur)?;
    let leader = <I>::decode(cur)?;
    Ok(Self::new(term, leader))
  }
}

impl<I: NodeId> Data for ReadIndex<I> {
  fn encode(&self, buf: &mut Vec<u8>) {
    self.term.encode(buf);
    self.from.encode(buf);
    self.context.encode(buf);
  }

  fn decode(cur: &mut crate::data::ByteCursor) -> Result<Self, DecodeError> {
    let term = <Term>::decode(cur)?;
    let from = <I>::decode(cur)?;
    let context = <Bytes>::decode(cur)?;
    Ok(Self::new(term, from, context))
  }
}

impl<I: NodeId> Data for ReadIndexResp<I> {
  fn encode(&self, buf: &mut Vec<u8>) {
    self.term.encode(buf);
    self.from.encode(buf);
    self.index.encode(buf);
    self.context.encode(buf);
    self.reject.encode(buf);
  }

  fn decode(cur: &mut crate::data::ByteCursor) -> Result<Self, DecodeError> {
    let term = <Term>::decode(cur)?;
    let from = <I>::decode(cur)?;
    let index = <Index>::decode(cur)?;
    let context = <Bytes>::decode(cur)?;
    let reject = <bool>::decode(cur)?;
    Ok(Self::new(term, from, index, context, reject))
  }
}

impl<I: NodeId> Data for Message<I> {
  fn encode(&self, buf: &mut Vec<u8>) {
    match self {
      Self::AppendEntries(m) => {
        buf.push(0);
        m.encode(buf);
      }
      Self::AppendResp(m) => {
        buf.push(1);
        m.encode(buf);
      }
      Self::RequestVote(m) => {
        buf.push(2);
        m.encode(buf);
      }
      Self::VoteResp(m) => {
        buf.push(3);
        m.encode(buf);
      }
      Self::Heartbeat(m) => {
        buf.push(4);
        m.encode(buf);
      }
      Self::HeartbeatResp(m) => {
        buf.push(5);
        m.encode(buf);
      }
      Self::InstallSnapshot(m) => {
        buf.push(6);
        m.encode(buf);
      }
      Self::SnapshotResp(m) => {
        buf.push(7);
        m.encode(buf);
      }
      Self::TimeoutNow(m) => {
        buf.push(8);
        m.encode(buf);
      }
      Self::ReadIndex(m) => {
        buf.push(9);
        m.encode(buf);
      }
      Self::ReadIndexResp(m) => {
        buf.push(10);
        m.encode(buf);
      }
    }
  }

  fn decode(cur: &mut crate::data::ByteCursor) -> Result<Self, DecodeError> {
    match cur.take_u8()? {
      0 => Ok(Self::AppendEntries(AppendEntries::<I>::decode(cur)?)),
      1 => Ok(Self::AppendResp(AppendResp::<I>::decode(cur)?)),
      2 => Ok(Self::RequestVote(RequestVote::<I>::decode(cur)?)),
      3 => Ok(Self::VoteResp(VoteResp::<I>::decode(cur)?)),
      4 => Ok(Self::Heartbeat(Heartbeat::<I>::decode(cur)?)),
      5 => Ok(Self::HeartbeatResp(HeartbeatResp::<I>::decode(cur)?)),
      6 => Ok(Self::InstallSnapshot(InstallSnapshot::<I>::decode(cur)?)),
      7 => Ok(Self::SnapshotResp(SnapshotResp::<I>::decode(cur)?)),
      8 => Ok(Self::TimeoutNow(TimeoutNow::<I>::decode(cur)?)),
      9 => Ok(Self::ReadIndex(ReadIndex::<I>::decode(cur)?)),
      10 => Ok(Self::ReadIndexResp(ReadIndexResp::<I>::decode(cur)?)),
      _ => Err(DecodeError::Invalid("Message tag")),
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
  fn timeout_now_message_term() {
    let tn = TimeoutNow::new(crate::Term::new(8), 2u64);
    assert_eq!(tn.term(), crate::Term::new(8));
    assert_eq!(tn.leader(), 2u64);
    let m = Message::TimeoutNow(tn);
    assert_eq!(m.term(), crate::Term::new(8));
    assert!(m.is_timeout_now());
  }

  #[test]
  fn read_index_message_term() {
    let ri = ReadIndex::new(crate::Term::new(6), 3u64, bytes::Bytes::from_static(b"ctx"));
    assert_eq!(ri.term(), crate::Term::new(6));
    assert_eq!(ri.from(), 3u64);
    assert_eq!(ri.context(), b"ctx");
    let m = Message::ReadIndex(ri);
    assert_eq!(m.term(), crate::Term::new(6));
    assert!(m.is_read_index());
  }

  #[test]
  fn read_index_resp_message_term() {
    let rir = ReadIndexResp::new(
      crate::Term::new(7),
      1u64,
      crate::Index::new(42),
      bytes::Bytes::from_static(b"ctx"),
      false,
    );
    assert_eq!(rir.term(), crate::Term::new(7));
    assert_eq!(rir.from(), 1u64);
    assert_eq!(rir.index(), crate::Index::new(42));
    assert_eq!(rir.context(), b"ctx");
    assert!(!rir.reject());
    let m = Message::ReadIndexResp(rir);
    assert_eq!(m.term(), crate::Term::new(7));
    assert!(m.is_read_index_resp());

    // A rejecting response carries the flag through the value (the wire form for these messages
    // is the struct itself; the bool round-trips by construction).
    let rejected = ReadIndexResp::new(
      crate::Term::new(7),
      1u64,
      crate::Index::ZERO,
      bytes::Bytes::from_static(b"ctx"),
      true,
    );
    assert!(rejected.reject());
    assert_eq!(rejected, rejected.clone());
  }

  #[test]
  fn install_snapshot_message_term() {
    use crate::conf::ConfState;
    let meta = SnapshotMeta::new(
      crate::Index::new(10),
      crate::Term::new(3),
      ConfState::from_voters(std::vec![1u64, 2u64, 3u64]),
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
mod tests;
