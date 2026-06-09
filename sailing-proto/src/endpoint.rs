//! The Sans-I/O Raft core. M0 is a no-op skeleton: it owns state and exposes the
//! `handle_*`/`poll_*` surface. M1 fills in leader election. M2 adds log replication.
use crate::{
  Config, Event, Index, Instant, LogStore, Message, NodeId, Outgoing, Prng, ReadOnly, StableStore,
  StateMachine, Term,
};
use bytes::Bytes;
use std::collections::{BTreeMap, VecDeque};

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

/// The independent timers an `Endpoint` arms.
///
/// `poll_timeout` filters to the ones the current `(role, state)` will actually service in
/// `handle_timeout` — the §8 timer-wedge defense. Returning a deadline the current state
/// will not service would leave the driver sleeping to a deadline that `handle_timeout`
/// ignores, re-arms nothing, and triggers a busy-wakeup loop / wedge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, derive_more::Display, derive_more::IsVariant)]
#[display("{}", self.as_str())]
pub enum TimerKind {
  /// Follower/candidate election timeout; also the CheckQuorum interval on a leader with
  /// `check_quorum` enabled.
  Election,
  /// Leader heartbeat interval.
  Heartbeat,
  /// Leader transfer abort window (one election timeout after transfer start).
  Transfer,
}

impl TimerKind {
  /// All timer kinds in a fixed order.
  pub const ALL: [TimerKind; 3] = [
    TimerKind::Election,
    TimerKind::Heartbeat,
    TimerKind::Transfer,
  ];

  /// The stable snake_case name.
  pub const fn as_str(&self) -> &'static str {
    match self {
      Self::Election => "election",
      Self::Heartbeat => "heartbeat",
      Self::Transfer => "transfer",
    }
  }
}

/// The CLASS of unrecoverable failure that poisoned a node.
///
/// Once a node is poisoned every `handle_*` is a no-op (see [`Endpoint::is_poisoned`]); this
/// enum records *why* so a driver can surface a diagnosis instead of a bare "node is dead".
/// It captures the kind of fault — a corrupt snapshot vs. an FSM bug vs. a storage read error
/// — not the underlying error value (the variants are unit-only so the type stays `no_std`-
/// friendly and `Copy`). The first cause wins: a later poison never overwrites the original
/// (see [`Endpoint::poison_reason`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, derive_more::Display, derive_more::IsVariant)]
#[display("{}", self.as_str())]
#[non_exhaustive]
pub enum PoisonReason {
  /// A committed-range log read (`LogStore::entries`) failed during apply.
  LogRead,
  /// `LogStore::poll` yielded a storage error.
  LogPoll,
  /// `StableStore::poll` yielded a storage error.
  StablePoll,
  /// `LogStore::term` failed while preparing a snapshot.
  LogTerm,
  /// A committed `Normal` entry's payload failed to decode as `F::Command`.
  NormalEntryDecode,
  /// `StateMachine::apply` returned an error for a committed entry.
  Apply,
  /// `StateMachine::snapshot` returned an error while capturing state.
  SnapshotCapture,
  /// A committed `ConfChange` entry's payload failed to decode as `ConfChangeV2`.
  ConfChangeDecode,
  /// The `Changer` rejected a committed, validly-decoded `ConfChange`.
  ConfChangeApply,
  /// A snapshot blob failed to decode as `F::Snapshot` (install or restart).
  SnapshotDecode,
  /// `StateMachine::restore` failed while installing a snapshot (install or restart).
  SnapshotRestore,
}

impl PoisonReason {
  /// The stable snake_case name.
  pub const fn as_str(&self) -> &'static str {
    match self {
      Self::LogRead => "log_read",
      Self::LogPoll => "log_poll",
      Self::StablePoll => "stable_poll",
      Self::LogTerm => "log_term",
      Self::NormalEntryDecode => "normal_entry_decode",
      Self::Apply => "apply",
      Self::SnapshotCapture => "snapshot_capture",
      Self::ConfChangeDecode => "conf_change_decode",
      Self::ConfChangeApply => "conf_change_apply",
      Self::SnapshotDecode => "snapshot_decode",
      Self::SnapshotRestore => "snapshot_restore",
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
  /// The last `commit` value durably written to `HardState`. The commit watermark is
  /// persisted (batched) by the `handle_storage` choke-point whenever `self.commit` exceeds
  /// this, and stamped into every term/vote write so a stale read-back can never regress the
  /// durable commit. Without persisting it, a crash with no snapshot loses the commit
  /// watermark and `restart` rebuilds an empty/snapshot-only state machine despite a durable
  /// committed log (review C1). Init `Index::ZERO` in `new`; init to the recovered commit in
  /// `restart` (so the choke-point doesn't immediately re-persist an unchanged value).
  committed_persisted: Index,
  prng: Prng,
  /// Per-voter ballot: `true` = grant, `false` = reject. Absent IDs have not voted yet.
  /// Replaces the old `votes_granted: BTreeSet<I>` — the joint quorum needs the full
  /// ballot (grants *and* rejections), not just the grant set.
  votes: BTreeMap<I, bool>,
  election_deadline: Option<Instant>,
  heartbeat_deadline: Option<Instant>,
  outgoing: VecDeque<Outgoing<I>>,
  events: VecDeque<Event<I, F::Response>>,
  /// Runtime membership: joint voter config, learner sets, and per-peer `Progress`.
  /// Replaces the old `progress: BTreeMap<I, crate::Progress>` and static-voter quorum.
  tracker: crate::Tracker<I>,
  /// Monotonically minted id for every storage submission.
  next_op_id: crate::OpId,
  /// Outstanding write → deferred action.
  pending: BTreeMap<crate::OpId, Pending<I>>,
  /// Sticky fatal error: once set, all `handle_*` are no-ops. The fast-path flag checked by
  /// every `handle_*` guard; the cause is recorded separately in `poison_reason`.
  poisoned: bool,
  /// The CLASS of the *first* fatal failure that poisoned this node, or `None` if healthy.
  /// First-cause-wins: a later poison never clobbers the original diagnosis. Surfaced to the
  /// driver via `poison_reason()` so an operator can distinguish (e.g.) a corrupt snapshot
  /// from an FSM bug from a disk read error.
  poison_reason: Option<PoisonReason>,
  /// In-flight snapshot write: `(opid, up_to)`. Compaction is deferred until the snapshot
  /// is durable (crash-safe: we never compact before the snapshot write completes).
  ///
  /// Completion contract: the normal path clears this field when the matching `SnapshotWritten`
  /// completion drains through `handle_storage`'s poll loop. If that completion is dropped or
  /// coalesced by a store (so it never arrives), `handle_storage` instead RECONCILES this field
  /// against the durable snapshot (review I9): once `StableStore::snapshot()` reports a persisted
  /// snapshot whose `last_index >= up_to`, the blob is durable, the deferred compaction is
  /// performed, and this field is cleared — so a missed completion can no longer wedge future
  /// snapshots. A store error still poisons the node via `handle_storage`, and `restart` resets
  /// this field to `None`.
  pending_compact: Option<(crate::OpId, Index)>,
  /// Log index of the most recently appended (not-yet-applied) `ConfChange` entry.
  ///
  /// Initialized to `Index::ZERO` in both `new` and `restart`. On restart, ZERO is acceptable
  /// for M6 — a more precise scan of the durable log to find any pending ConfChange entry is a
  /// future refinement. If a ConfChange entry is in the log but not yet applied after restart,
  /// the one-in-flight guard will be permissive (ZERO <= applied), but correctness is maintained
  /// because the entry will still be applied exactly once in `apply_committed`.
  pending_conf_index: Index,
  /// ReadIndex tracking (pending reads, heartbeat-ack sets, confirmed read states).
  read_only: ReadOnly<I>,
  /// Deferred read requests that arrived before the leader has committed an entry in its
  /// current term.  Flushed once `maybe_advance_commit` advances `self.commit` to a
  /// current-term entry.
  ///
  /// Each element is `(context, from)` matching `add_request`'s signature.
  pending_reads: std::vec::Vec<(Bytes, Option<I>)>,
  /// Target of an in-progress leader transfer, or `None` if no transfer is active.
  ///
  /// Set by `transfer_leader`; cleared on any leadership change (term bump step-down,
  /// `step_down_to_follower`, `become_leader`) and on `transfer_deadline` expiry.
  lead_transferee: Option<I>,
  /// When to abort a stalled leader transfer (abort window = one election timeout).
  ///
  /// Armed when `lead_transferee` is set; cleared together with `lead_transferee`.
  transfer_deadline: Option<Instant>,
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
    // Bootstrap the Tracker from the static seed voter set. Read the needed config
    // values BEFORE moving `config` into the struct literal below.
    let cs = crate::ConfState::from_voters(config.voters().iter().copied());
    let tracker = crate::Tracker::from_conf_state(
      &cs,
      Index::ZERO,
      config.max_inflight_msgs(),
      config.max_inflight_bytes(),
    );
    let read_only_opt = config.read_only();
    // A cross-field misconfiguration (e.g. `LeaseBased` without `check_quorum`) is handled by
    // degradation, not rejection: the `LeaseBased` read path falls back to the Safe heartbeat
    // round when `check_quorum` is off, so construction stays infallible and the same in all
    // build profiles. `Config::validate()` is available for callers who want to opt into a
    // strict pre-flight check.
    let mut ep = Self {
      config,
      fsm,
      role: Role::Follower,
      term: Term::ZERO,
      voted_for: None,
      leader: None,
      commit: Index::ZERO,
      applied: Index::ZERO,
      committed_persisted: Index::ZERO,
      prng: Prng::new(seed),
      votes: BTreeMap::new(),
      election_deadline: None,
      heartbeat_deadline: None,
      outgoing: VecDeque::new(),
      events: VecDeque::new(),
      tracker,
      next_op_id: crate::OpId::ZERO,
      pending: BTreeMap::new(),
      poisoned: false,
      poison_reason: None,
      pending_compact: None,
      pending_conf_index: Index::ZERO,
      read_only: ReadOnly::new(read_only_opt),
      pending_reads: std::vec::Vec::new(),
      lead_transferee: None,
      transfer_deadline: None,
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

  /// The earliest deadline the current `(role, state)` will ACTUALLY service in
  /// `handle_timeout` (the §8 timer-wedge defense).
  ///
  /// Only deadlines that `serviceable_now` considers active for the current role+state are
  /// candidates; the minimum of those is returned.  A driver that feeds `poll_timeout` back
  /// into `handle_timeout` is guaranteed to make progress: every returned deadline will be
  /// re-armed to a strictly-future instant (or cleared) by the dispatch, so the loop never
  /// busy-spins on a stale deadline.
  #[inline]
  pub fn poll_timeout(&self) -> Option<Instant> {
    TimerKind::ALL
      .iter()
      .filter(|&&k| self.serviceable_now(k))
      .filter_map(|&k| self.deadline_of(k))
      .min()
  }

  /// Mint a unique, monotonically-increasing operation id for a storage submission.
  fn mint_op_id(&mut self) -> crate::OpId {
    let id = self.next_op_id;
    self.next_op_id = self.next_op_id.next();
    id
  }

  /// Enter the permanent failed state (a fatal storage/apply error). Every subsequent
  /// `handle_*` becomes a no-op; the driver should surface this and stop.
  ///
  /// `reason` records the CLASS of failure. First-cause-wins: if the node was already
  /// poisoned, the original `reason` is preserved so the diagnosis is not clobbered by a
  /// downstream failure.
  fn poison(&mut self, reason: PoisonReason) {
    self.poisoned = true;
    self.poison_reason.get_or_insert(reason);
  }

  /// Whether this node has hit an unrecoverable error.
  #[inline(always)]
  pub const fn is_poisoned(&self) -> bool {
    self.poisoned
  }

  /// The CLASS of the first fatal failure that poisoned this node, or `None` if healthy.
  ///
  /// First-cause-wins: this is the *original* diagnosis, never overwritten by a later poison.
  /// Pairs with [`is_poisoned`](Self::is_poisoned) (the fast boolean check) to let a driver
  /// surface *why* a node died (a corrupt snapshot vs. an FSM bug vs. a storage read error).
  #[inline(always)]
  pub const fn poison_reason(&self) -> Option<PoisonReason> {
    self.poison_reason
  }

  // --- PRIVATE HELPERS (no Data bound) ---

  fn arm_election_timer(&mut self, now: Instant) {
    let t = self.prng.election_timeout(self.config.election_timeout());
    self.election_deadline = Some(now + t);
    self.heartbeat_deadline = None;
  }

  /// Step down to Follower at the SAME term (no term bump): used by CheckQuorum when the
  /// leader can no longer reach a quorum. (The U6 self-removal step-down is separate and
  /// inlined in `apply_committed` — it disarms the election timer because a removed
  /// non-voter must never campaign, the opposite of this helper.)
  ///
  /// Sets `role = Follower`, clears `leader` and `heartbeat_deadline`, and arms the election
  /// timer so the node will eventually campaign again (with PreVote, non-disruptively).
  fn step_down_to_follower(&mut self, now: Instant) {
    self.role = Role::Follower;
    self.leader = None;
    self.heartbeat_deadline = None;
    // Drop all pending reads — a stepped-down node is no longer the leader and
    // cannot confirm any outstanding read requests.
    self.read_only.reset(self.config.read_only());
    self.pending_reads.clear();
    // Abort any in-progress leader transfer — leadership is changing, the transfer is moot.
    self.lead_transferee = None;
    self.transfer_deadline = None;
    // The partitioned former leader arms the election timer; once it heals and
    // pre-vote/real vote succeeds it can campaign again without disrupting the cluster.
    self.arm_election_timer(now);
  }

  fn arm_heartbeat_timer(&mut self, now: Instant) {
    self.heartbeat_deadline = Some(now + self.config.heartbeat_interval());
    // Callers that need to clear election_deadline (e.g. become_leader when check_quorum is
    // false) do so explicitly; we do NOT touch election_deadline here so the CQ timer
    // (set by become_leader when check_quorum is true) is not clobbered on each heartbeat.
  }

  /// The armed deadline for the given timer kind, regardless of whether it is serviceable now.
  fn deadline_of(&self, kind: TimerKind) -> Option<Instant> {
    match kind {
      TimerKind::Election => self.election_deadline,
      TimerKind::Heartbeat => self.heartbeat_deadline,
      TimerKind::Transfer => self.transfer_deadline,
    }
  }

  /// Whether the current `(role, state)` will service `kind` in `handle_timeout`.
  ///
  /// This is the exact mirror of `handle_timeout`'s dispatch conditions:
  /// - `Heartbeat`: the leader always services its heartbeat deadline.
  /// - `Election`: the leader services it only when `check_quorum` is enabled (CheckQuorum
  ///   tick); a follower/candidate services it only when it is a voter (non-voters never
  ///   campaign, so their election timer firing is a silent no-op — we should not surface it).
  /// - `Transfer`: the leader services it only when a leader transfer is in progress.
  fn serviceable_now(&self, kind: TimerKind) -> bool {
    match kind {
      TimerKind::Heartbeat => self.role.is_leader(),
      TimerKind::Election => {
        if self.role.is_leader() {
          self.config.check_quorum()
        } else {
          self.tracker.is_voter(&self.config.id())
        }
      }
      TimerKind::Transfer => self.role.is_leader() && self.lead_transferee.is_some(),
    }
  }

  fn send(&mut self, to: I, msg: Message<I>) {
    self.outgoing.push_back(Outgoing::new(to, msg));
  }

  fn peers(&self) -> impl Iterator<Item = I> {
    let me = self.config.id();
    // Iterate all tracked IDs (voters both halves ∪ learners ∪ learners_next), excluding self.
    // The leader replicates to learners too; quorum is still computed over voters only
    // (tracker.quorum_committed / tracker.vote_result read only the voter halves).
    self.tracker.ids().into_iter().filter(move |&p| p != me)
  }

  fn last_log(&self, log: &impl LogStore) -> (Index, Term) {
    let li = log.last_index();
    let lt = log.term(li).unwrap_or(Term::ZERO);
    (li, lt)
  }

  /// Build the current `ConfState` from the runtime membership (Tracker).
  ///
  /// This reflects the live configuration so snapshots and restarts carry the correct
  /// membership, not just the static bootstrap seed from `Config.voters`.
  fn conf_state(&self) -> crate::ConfState<I> {
    self.tracker.conf_state()
  }

  /// Expose `pending_compact` for testing.
  #[cfg(test)]
  pub(crate) fn pending_compact(&self) -> Option<(crate::OpId, Index)> {
    self.pending_compact
  }

  fn broadcast_heartbeat(&mut self, _now: Instant) {
    let (term, me) = (self.term, self.config.id());
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
        Message::Heartbeat(crate::Heartbeat::new(term, me, peer_commit, ctx.clone())),
      );
    }
  }

  /// Broadcast a heartbeat to all peers carrying a specific `context`.
  ///
  /// Used by the ReadIndex Safe path to kick off a dedicated heartbeat round that
  /// proves the leader is still reachable by a quorum.
  fn broadcast_heartbeat_with_ctx(&mut self, _now: Instant, ctx: Bytes) {
    let (term, me) = (self.term, self.config.id());
    for peer in self.peers().collect::<std::vec::Vec<_>>() {
      let peer_commit = self
        .tracker
        .progress(&peer)
        .map(|pr| core::cmp::min(self.commit, pr.match_index()))
        .unwrap_or(Index::ZERO);
      self.send(
        peer,
        Message::Heartbeat(crate::Heartbeat::new(term, me, peer_commit, ctx.clone())),
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
    let Some(pr) = self.tracker.progress(&peer).cloned() else {
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
        if let Some(p) = self.tracker.progress_mut(&peer) {
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
    if let Some(p) = self.tracker.progress_mut(&peer) {
      if (!is_empty && p.state().is_replicate()) || sent_partial {
        p.sent_entries(last_sent, bytes_sent);
      }
    }
  }

  /// Liveness fix (review I1): re-send the persisted snapshot to a peer that is stuck in
  /// `Snapshot` state.
  ///
  /// A peer in `Snapshot` state is unconditionally paused, so `maybe_send_append`
  /// early-returns for it. It only leaves Snapshot state via `maybe_update(n >= pending)`,
  /// which requires the snapshot to have been DELIVERED (a `SnapshotResp`/`AppendResp`). If
  /// the single `InstallSnapshot` emitted by `maybe_send_append`'s compacted-hole branch is
  /// lost, the leader would never retry and the follower would wedge forever. `on_heartbeat_resp`
  /// calls this each heartbeat round for a peer still behind its pending snapshot index.
  ///
  /// Unlike the `maybe_send_append` branch this does NOT touch progress: the peer is already
  /// `Snapshot(pending)` with the correct pending index, and re-sending the same blob is
  /// idempotent for the follower's install (`on_install_snapshot` is staleness-guarded). If no
  /// snapshot is persisted yet (shouldn't happen once compaction ran) this is a no-op.
  fn resend_snapshot<S: StableStore<NodeId = I>>(&mut self, peer: I, stable: &S) {
    if let Some((meta, data)) = stable.snapshot() {
      let (term, me) = (self.term, self.config.id());
      self.send(
        peer,
        Message::InstallSnapshot(crate::InstallSnapshot::new(term, me, meta, data)),
      );
    }
  }

  fn maybe_advance_commit<L: LogStore>(&mut self, log: &L) {
    // Delegate to the Tracker's joint-quorum committed index. For a simple (non-joint)
    // config this is identical to the old sorted-match logic:
    //   old: matches.sort(); candidate = matches[n - (n/2+1)]
    //   new: MajorityConfig::committed_index does exactly that sort+pick internally.
    // A degenerate Tracker with the static seed (voters = config seed, outgoing empty,
    // no learners) returns the same value — M0–M5 tests are therefore unaffected.
    let candidate = self.tracker.quorum_committed();
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
      let lease_open = !(self.leader.is_some() && self.election_deadline.is_some_and(|d| d > now));
      let grant = log_ok && term_ok && lease_open;
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

    // Real vote path (unchanged from M1–M6).
    let can_vote = self.voted_for.is_none() || self.voted_for == Some(rv.candidate());
    if can_vote && log_ok {
      self.voted_for = Some(rv.candidate());
      self.arm_election_timer(now);
      // Persist (term, vote); the VoteResp(grant) is owed once the write is DURABLE.
      // Stamp the current commit too: we read-modify `hard_state()` then override fields, so
      // without this the write would carry a possibly-stale `hard_state().commit` and could
      // REGRESS the durable commit below a value the handle_storage choke-point already wrote.
      // `self.commit` is monotonic, so stamping it keeps the durable commit monotonic (C1).
      let opid = self.mint_op_id();
      let hs = stable
        .hard_state()
        .with_term(self.term)
        .with_vote(self.voted_for)
        .with_commit(self.commit);
      stable.submit_write(opid, hs);
      self.committed_persisted = self.commit;
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
    if self.tracker.vote_result(&self.votes).is_won() {
      self.become_leader(now, log, stable);
    }
    // Lost or Pending: stay candidate; the election timeout retries (preserves M1 liveness).
  }
}

// ─── Full replication impl (F::Command: Data required for apply_committed) ──────────────────────

impl<I, F> Endpoint<I, F>
where
  I: NodeId,
  F: StateMachine,
  F::Command: crate::Data,
  // The fatal apply/snapshot error must be inspectable so a poisoned node's cause can be
  // surfaced (design spec §6.3). `core::error::Error` is stable in core (no_std-OK).
  F::Error: core::error::Error,
{
  /// Drain storage completions. (M3+: append-before-ack / persist-vote.)
  pub fn handle_storage<L, S>(&mut self, now: Instant, log: &mut L, stable: &mut S)
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
        Ok(crate::LogDone::Appended(opid)) => self.on_log_appended(now, log, stable, opid),
        Ok(crate::LogDone::Compacted(_)) => {}
        Err(_) => {
          self.poison(PoisonReason::LogPoll);
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
          self.poison(PoisonReason::StablePoll);
          return;
        }
      }
    }

    // Reconcile a deferred compaction whose `SnapshotWritten` completion was missed or coalesced
    // by the store (review I9): if the durable snapshot already covers `up_to`, the blob IS safely
    // persisted, so the deferred compaction is safe even though we never observed the specific
    // completion. Without this, a single dropped completion would wedge `pending_compact`, and the
    // `is_some()` guard in `maybe_snapshot` would stop ALL future snapshots and compaction, growing
    // the log unbounded.
    //
    // This is a NO-OP on the happy path: the poll-drain loop above clears `pending_compact` when the
    // completion arrives, so the `if let` does not match. It can only fire when a completion was
    // genuinely missed AND the durable snapshot already covers `up_to` — so it can never compact
    // ahead of a durable snapshot (safety preserved). It runs before `maybe_snapshot` so a node that
    // was wedged can snapshot again in this same call.
    if let Some((_pid, up_to)) = self.pending_compact {
      if let Some((meta, _data)) = stable.snapshot() {
        if meta.last_index() >= up_to {
          log.compact(up_to);
          self.pending_compact = None;
        }
      }
    }

    // After all completions are drained, check whether a new snapshot is warranted.
    self.maybe_snapshot(log, stable);

    // Auto-leave joint consensus: once the joint config is applied and no conf change is in
    // flight, the leader appends an empty leave-joint entry to transition back to a simple
    // config. Re-evaluated each call so a freshly-elected leader also finishes the job.
    // The condition stops once is_joint() is false — no infinite loop risk.
    if self.role.is_leader()
      && self.tracker.is_joint()
      && self.tracker.auto_leave()
      && self.pending_conf_index <= self.applied
    {
      let leave = crate::ConfChangeV2::leave_joint();
      self.append_conf_change(log, stable, leave);
    }

    // Persist the advanced commit watermark so a restart recovers it (without this, restart
    // rebuilds an empty/snapshot-only state machine despite a durable committed log — review
    // C1). Batched here (runs every driver iteration) rather than on every advance; a crash
    // before this persist only loses a bounded commit suffix that is still in the durable LOG
    // and is re-advanced by the leader on recovery — Leader Completeness guarantees the leader
    // holds those committed entries, so no committed entry is lost, just a brief re-sync.
    // No `Pending` entry: a commit-watermark write owes no ack (like the step-down /
    // become_candidate writes); its completion drains harmlessly through `on_stable_wrote`.
    if !self.poisoned && self.commit > self.committed_persisted {
      let opid = self.mint_op_id();
      let hs = stable
        .hard_state()
        .with_term(self.term)
        .with_vote(self.voted_for)
        .with_commit(self.commit);
      stable.submit_write(opid, hs);
      self.committed_persisted = self.commit;
    }
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
        self.poison(PoisonReason::SnapshotCapture);
        return;
      }
    };
    use crate::Data as _;
    let mut data = std::vec::Vec::new();
    snap.encode(&mut data);
    let last_term = match log.term(self.applied) {
      Ok(t) => t,
      Err(_) => {
        self.poison(PoisonReason::LogTerm);
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
    let mut poison_reason: Option<PoisonReason> = None;
    // Bootstrap tracker from the static seed first; may be overridden below if a
    // durable snapshot carries a more recent ConfState.
    let seed_cs = crate::ConfState::from_voters(config.voters().iter().copied());
    let mut tracker = crate::Tracker::from_conf_state(
      &seed_cs,
      Index::ZERO,
      config.max_inflight_msgs(),
      config.max_inflight_bytes(),
    );
    // Restore from a durable snapshot first: the compacted log no longer holds entries
    // <= meta.last_index, so the SM baseline comes from the snapshot; we then replay only
    // the durable post-snapshot committed tail.
    if let Some((meta, data)) = stable.snapshot() {
      match <F::Snapshot as crate::Data>::decode(&data) {
        Ok((_, snap)) => {
          if fsm.restore(snap).is_err() {
            poisoned = true;
            poison_reason = Some(PoisonReason::SnapshotRestore);
          } else {
            applied = meta.last_index();
            // M6: install the durable membership from the snapshot's ConfState.
            // This supersedes the bootstrap seed from Config.voters.
            // (Replaying ConfChange log entries to further refine membership is U5's job.)
            tracker = crate::Tracker::from_conf_state(
              &meta.conf().clone(),
              meta.last_index(),
              config.max_inflight_msgs(),
              config.max_inflight_bytes(),
            );
          }
        }
        Err(_) => {
          poisoned = true;
          poison_reason = Some(PoisonReason::SnapshotDecode);
        }
      }
    }
    // Never trust commit beyond the durable log; never below the snapshot baseline.
    let commit = core::cmp::min(hs.commit(), log.last_index()).max(applied);
    let read_only_opt = config.read_only();
    // Misconfiguration is handled by degradation, not rejection (see `Endpoint::new`); restart
    // construction stays infallible and identical across build profiles.
    let mut ep = Self {
      config,
      fsm,
      role: Role::Follower,
      term: hs.term(),
      voted_for: hs.vote(),
      leader: None,
      commit,
      applied,
      // Recovered commit is already durable in HardState — seed `committed_persisted` to it so
      // the handle_storage choke-point doesn't immediately re-persist an unchanged value (C1).
      committed_persisted: commit,
      prng: Prng::new(seed),
      votes: BTreeMap::new(),
      election_deadline: None,
      heartbeat_deadline: None,
      next_op_id: crate::OpId::ZERO,
      pending: BTreeMap::new(),
      poisoned,
      poison_reason,
      pending_compact: None,
      // On restart, ZERO is acceptable — see the field-level comment on pending_conf_index.
      pending_conf_index: Index::ZERO,
      tracker,
      outgoing: VecDeque::new(),
      events: VecDeque::new(),
      read_only: ReadOnly::new(read_only_opt),
      pending_reads: std::vec::Vec::new(),
      lead_transferee: None,
      transfer_deadline: None,
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

  fn on_log_appended<L: LogStore, S: StableStore<NodeId = I>>(
    &mut self,
    now: Instant,
    log: &mut L,
    stable: &S,
    opid: crate::OpId,
  ) {
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
        if let Some(p) = self.tracker.progress_mut(&self.config.id()) {
          p.maybe_update(upto);
        }
        self.maybe_advance_commit(log);
        self.apply_committed(log);
        // ReadIndex deferred-flush: if this commit advanced to the first current-term
        // entry, flush any reads that were deferred waiting for it.
        self.maybe_flush_deferred_reads(now, log, stable);
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
    // Exception (PreVote anti-disruption): pre-vote traffic carries an *advertised* term
    // that has not been adopted — do NOT step down or adopt it.
    if msg.term() > self.term {
      let is_prevote_req = matches!(&msg, Message::RequestVote(rv) if rv.pre_vote());
      let is_prevote_resp = matches!(&msg, Message::VoteResp(vr) if vr.pre_vote());
      if is_prevote_req || is_prevote_resp {
        // A PreVote request/response carries an *advertised* future term — the candidate
        // has only proposed it, not adopted it. Fall through without adopting the term,
        // stepping down, or persisting. The anti-disruption guarantee: a partitioned node's
        // pre-votes can never raise the cluster term.
      } else {
        // CheckQuorum / PreVote follower lease: a follower that has heard from its current
        // leader within the election timeout ignores a disruptive higher-term REAL vote
        // request unless the campaign is a forced leader-transfer. (A higher-term PRE-vote
        // already took the exemption branch above and is lease-checked separately inside
        // `on_request_vote`.) This prevents a partitioned node that has been campaigning
        // from raising the cluster term when it rejoins and followers still have a live
        // leader (etcd inLease).
        //
        // The check: (check_quorum OR pre_vote) AND we know a leader AND our election timer
        // is still healthy (i.e. we heard from the leader within the election timeout window).
        // A leader_transfer campaign ALWAYS bypasses the lease (it's an authorized handoff).
        if let Message::RequestVote(rv) = &msg {
          let force = rv.leader_transfer();
          let in_lease = (self.config.check_quorum() || self.config.pre_vote())
            && self.leader.is_some()
            && self.election_deadline.is_some_and(|d| d > now);
          if !force && in_lease {
            // We've heard from our leader recently; ignore this challenger.
            // Do NOT adopt the term, do NOT grant, do NOT reply.
            return;
          }
        }

        // All other higher-term messages: adopt term, step down to follower.
        self.term = msg.term();
        self.role = Role::Follower;
        self.voted_for = None;
        self.leader = None;
        // All pending work from the old term is now stale (spec §7). Drop it before any new
        // grant is recorded below — a fresh CastVote added by on_request_vote will survive.
        self.pending.clear();
        // Drop all ReadIndex state too: a stale read confirmation must never leak across a term
        // change. Mirrors `step_down_to_follower` / `become_leader` (read confirmation is
        // leader-gated, so this is robustness, not a behavior change).
        self.read_only.reset(self.config.read_only());
        self.pending_reads.clear();
        // Abort any in-progress leader transfer — leadership is changing.
        self.lead_transferee = None;
        self.transfer_deadline = None;
        // Persist the new term and cleared vote. Stepping down owes no ack, so no Pending entry.
        // Stamp the current commit too (see on_request_vote): a read-modify of `hard_state()`
        // must not write back a stale `commit` that regresses the durable watermark (C1).
        let opid = self.mint_op_id();
        let hs = stable
          .hard_state()
          .with_term(self.term)
          .with_vote(None)
          .with_commit(self.commit);
        stable.submit_write(opid, hs);
        self.committed_persisted = self.commit;
      }
    }
    // Drop messages from a stale term — with one caveat for pre-vote requests:
    // a pre-vote whose advertised term < self.term is routed to on_request_vote, which
    // rejects it and replies at self.term (etcd: MsgPreVoteResp{Reject:true, Term:r.Term})
    // so the pre-candidate learns it is behind. Only silently drop non-pre-vote stale messages.
    if msg.term() < self.term {
      let is_prevote_req = matches!(&msg, Message::RequestVote(rv) if rv.pre_vote());
      if !is_prevote_req {
        return;
      }
      // Fall through: on_request_vote rejects (rv.term() < self.term fails the term_ok check).
    }

    // CheckQuorum: while the leader, any inbound message from a known peer proves that peer
    // is reachable. Mark it active so it counts toward the next quorum_active check.
    // We do this AFTER the term pre-pass (so a higher-term message that steps us down doesn't
    // mark a peer active on the stale term's leader) and only if we're still the leader.
    if self.role.is_leader() {
      if let Some(pr) = self.tracker.progress_mut(&from) {
        pr.set_recent_active(true);
      }
    }

    #[allow(unreachable_patterns)] // `_ => {}` is a forward-compat guard for future variants
    match msg {
      Message::RequestVote(rv) => self.on_request_vote(now, log, stable, rv),
      Message::VoteResp(vr) => self.on_vote_resp(now, log, stable, vr),
      Message::Heartbeat(hb) => self.on_heartbeat(now, log, hb),
      Message::AppendEntries(ae) => self.on_append_entries(now, log, ae),
      Message::AppendResp(r) => self.on_append_resp(now, log, stable, from, r),
      Message::HeartbeatResp(hr) => self.on_heartbeat_resp(from, log, stable, hr),
      Message::ReadIndex(ri) => self.on_read_index(now, log, stable, ri),
      Message::ReadIndexResp(r) => self.on_read_index_resp(r),
      Message::InstallSnapshot(is) => self.on_install_snapshot(now, log, stable, is),
      Message::SnapshotResp(r) => self.on_snapshot_resp(now, log, stable, from, r),
      Message::TimeoutNow(tn) => self.on_timeout_now(now, log, stable, tn),
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
        // Leader transfer abort: if the transfer deadline has passed without the target
        // taking over, abort the transfer and resume accepting proposals.
        if self.lead_transferee.is_some() && self.transfer_deadline.is_some_and(|d| d <= now) {
          self.lead_transferee = None;
          self.transfer_deadline = None;
        }
        // CheckQuorum: the leader uses the otherwise-idle election_deadline to run a
        // periodic quorum-activity check every election_timeout. If fewer than a quorum of
        // voters have been recently active (no message from them this window), the leader is
        // likely partitioned from the majority — step down so we stop serving stale reads
        // and allow a reachable node to be elected.
        if self.config.check_quorum() && self.election_deadline.is_some_and(|d| d <= now) {
          if !self.tracker.quorum_active() {
            self.step_down_to_follower(now);
          } else {
            // Quorum still reachable: reset the activity window and re-arm for the next check.
            let me = self.config.id();
            self.tracker.reset_recent_active(me);
            self.election_deadline = Some(now + self.config.election_timeout());
          }
        }
      }
      _ => {
        if self.election_deadline.is_some_and(|d| d <= now) {
          // A learner or removed node must never start an election.
          // Clear the deadline so `poll_timeout` returns `None` for this node and
          // the sim's clock can advance past it. Non-voters do not re-arm — they
          // resume their election timer only when a heartbeat arrives (which calls
          // `arm_election_timer`).
          self.election_deadline = None;
          if self.tracker.is_voter(&self.config.id()) {
            if self.config.pre_vote() {
              let won = self.become_pre_candidate(now, log);
              if won {
                // Single-node pre-vote quorum: skip straight to the real campaign.
                self.become_candidate(now, log, stable, false);
              }
            } else {
              self.become_candidate(now, log, stable, false);
            }
          }
          // else: non-voter — timer expires silently; deadline cleared above.
        }
      }
    }
    // Wedge tripwire: after all dispatch, no serviceable timer must still be armed-and-due.
    // If this fires, `serviceable_now` has diverged from the actual dispatch (a branch acted
    // on a timer but forgot to re-arm it to a future instant or clear it).
    debug_assert!(
      TimerKind::ALL
        .iter()
        .all(|&k| { !(self.serviceable_now(k) && self.deadline_of(k).is_some_and(|d| d <= now)) }),
      "handle_timeout left a serviceable timer armed-and-due (serviceable_now diverged from dispatch)"
    );
  }

  /// Append a `ConfChangeV2` entry to the log and replicate it to all peers.
  ///
  /// Internal helper shared by `propose_conf_change_v2` and the auto-leave path.
  /// Mirrors `propose`'s deferred-append + `LeaderAppend` + replicate pattern exactly.
  ///
  /// Requires `I: crate::Data` because the ConfChangeV2 encodes node ids.
  fn append_conf_change<L, S>(
    &mut self,
    log: &mut L,
    stable: &S,
    cc: crate::ConfChangeV2<I>,
  ) -> Index
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    use crate::Data as _;
    let mut buf = std::vec::Vec::new();
    cc.encode(&mut buf);
    let index = log.last_index().next();
    let entry = crate::Entry::new(
      self.term,
      index,
      crate::EntryKind::ConfChange,
      bytes::Bytes::from(buf),
    );
    let opid = self.mint_op_id();
    log.submit_append(opid, core::slice::from_ref(&entry));
    self
      .pending
      .insert(opid, Pending::LeaderAppend { upto: index });
    self.pending_conf_index = index;
    for peer in self.peers().collect::<std::vec::Vec<_>>() {
      self.maybe_send_append(peer, log, stable);
    }
    index
  }

  /// Propose a v1 (single-op) configuration change on the leader.
  ///
  /// Normalises the v1 input to a [`ConfChangeV2`] via [`ConfChange::into_v2`] and delegates
  /// to [`propose_conf_change_v2`][Self::propose_conf_change_v2].
  ///
  /// Returns the assigned log index on success, or an error if:
  /// - this node is not the leader (`NotLeader`), or
  /// - a previous conf-change entry is still pending (`ConfChangeInFlight`).
  pub fn propose_conf_change<L, S>(
    &mut self,
    now: Instant,
    log: &mut L,
    stable: &S,
    cc: crate::ConfChange<I>,
  ) -> Result<Index, crate::ProposeError<I>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    self.propose_conf_change_v2(now, log, stable, cc.into_v2())
  }

  /// Propose a v2 (possibly multi-op / joint-consensus) configuration change on the leader.
  ///
  /// **Safety invariants:**
  /// - Changes apply at commit time, not at append time (Tracker is ONLY updated in
  ///   `apply_committed`).
  /// - Only one conf-change entry may be in flight at a time (`pending_conf_index > applied`
  ///   causes `ConfChangeInFlight`).
  pub fn propose_conf_change_v2<L, S>(
    &mut self,
    _now: Instant,
    log: &mut L,
    stable: &S,
    cc: crate::ConfChangeV2<I>,
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
    // A leader transfer is in progress: no membership changes mid-transfer either.
    if self.lead_transferee.is_some() {
      return Err(crate::ProposeError::LeaderTransferInProgress);
    }
    // One change in flight at a time: refuse if a ConfChange entry is not yet applied.
    if self.pending_conf_index > self.applied {
      return Err(crate::ProposeError::ConfChangeInFlight);
    }
    let index = self.append_conf_change(log, stable, cc);
    Ok(index)
  }

  /// Propose a command on the leader. Returns the assigned index, or `NotLeader`.
  /// Takes `cmd` by reference (encoding only borrows; the caller keeps it to retry).
  pub fn propose<L, S>(
    &mut self,
    _now: Instant,
    log: &mut L,
    stable: &S,
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
    // A leader transfer is in progress: stop accepting new entries so the target can
    // catch up to a fixed last_index and receive TimeoutNow.
    if self.lead_transferee.is_some() {
      return Err(crate::ProposeError::LeaderTransferInProgress);
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
  ///
  /// Every unrecoverable fault here POISONS the node (it does not silently stall): a poisoned
  /// node is inert (`handle_*` are no-ops) and the driver surfaces `poison_reason()` and stops.
  /// This matches the policy of `on_install_snapshot` and the ConfChange Changer-reject arm.
  /// A bare `break` is used ONLY for the benign "committed entry not yet readable" case (the
  /// log slice is empty), which is transient and retried on the next `handle_*`.
  fn apply_committed<L: LogStore>(&mut self, log: &L) {
    while self.applied < self.commit {
      let idx = self.applied.next();
      let entry = match log.entries(idx..idx.next(), u64::MAX) {
        Ok(s) => match s.first() {
          Some(e) => e.clone(),
          // Benign: the committed entry is not yet in the read view. Retry next tick.
          None => break,
        },
        // A committed-range read failed. A healthy LogStore never fails this read, so treat it
        // as unrecoverable: poison rather than silently stall applied behind commit.
        Err(_) => {
          self.poison(PoisonReason::LogRead);
          break;
        }
      };
      match entry.kind() {
        crate::EntryKind::Normal => {
          let cmd = match <F::Command as crate::Data>::decode(entry.data()) {
            Ok((_, c)) => c,
            // A committed entry whose payload won't decode is corrupt/unrecoverable → poison.
            Err(_) => {
              self.poison(PoisonReason::NormalEntryDecode);
              break;
            }
          };
          match self.fsm.apply(idx, cmd) {
            Ok(resp) => self
              .events
              .push_back(crate::Event::Applied(crate::Applied::new(idx, resp))),
            // An FSM apply error is fatal (the SM diverges from the committed log) → poison.
            Err(_) => {
              self.poison(PoisonReason::Apply);
              break;
            }
          }
        }
        crate::EntryKind::Empty => {} // no-op: just advance applied
        crate::EntryKind::ConfChange => {
          // Decode the ConfChangeV2 payload. On failure: unrecoverable → poison (mirror Normal).
          let cc = match <crate::ConfChangeV2<I> as crate::Data>::decode(entry.data()) {
            Ok((_, c)) => c,
            Err(_) => {
              self.poison(PoisonReason::ConfChangeDecode);
              break;
            }
          };
          // Dispatch to the Changer using the etcd rules:
          //   empty changes + Auto transition  → leave_joint
          //   transition != Auto OR >1 change   → enter_joint (auto_leave = transition != Explicit)
          //   else (1 change, Auto transition)  → simple
          let changer = crate::tracker::confchange::Changer::new(
            log.last_index(),
            self.config.max_inflight_msgs(),
            self.config.max_inflight_bytes(),
          );
          let result = if cc.changes().is_empty()
            && cc.transition() == crate::ConfChangeTransition::Auto
          {
            changer.leave_joint(&self.tracker)
          } else if cc.transition() != crate::ConfChangeTransition::Auto || cc.changes().len() > 1 {
            let auto_leave = cc.transition() != crate::ConfChangeTransition::Explicit;
            changer.enter_joint(&self.tracker, auto_leave, cc.changes())
          } else {
            changer.simple(&self.tracker, cc.changes())
          };
          match result {
            Ok(new_tracker) => {
              self.tracker = new_tracker;
              let conf = self.tracker.conf_state();
              self
                .events
                .push_back(crate::Event::ConfChanged(crate::ConfChanged::new(
                  idx, conf,
                )));
              // U6: a leader that this change removed (or demoted to learner) is no longer a
              // voter in the new configuration and must stop acting as leader.
              // `is_voter()` checks BOTH joint halves, so during a joint phase where we are
              // still in the outgoing config we keep leading (we must shepherd the joint →
              // simple transition). We only step down once removed from BOTH halves.
              // The step-down is at the SAME term (no term bump): this is a leader yielding
              // to its own removal, not losing an election.
              if self.role.is_leader()
                && self.config.step_down_on_removal()
                && !self.tracker.is_voter(&self.config.id())
              {
                self.role = Role::Follower;
                self.leader = None;
                self.heartbeat_deadline = None;
                // Do NOT arm the election timer: a non-voter must not campaign (see
                // handle_timeout / become_candidate guards). Leaving election_deadline
                // disarmed is the right choice — a removed/demoted node has no business
                // holding an election timer.
                self.election_deadline = None;
                // Abort any in-progress leader transfer — the leader is being removed.
                self.lead_transferee = None;
                self.transfer_deadline = None;
              }
              // If an in-flight leader transfer's target was removed or demoted by this conf
              // change, abort it (the target can no longer be elected, and proposals must not
              // stay blocked until the transfer deadline). Mirrors etcd's abortLeaderTransfer
              // on conf-change apply.
              if self
                .lead_transferee
                .is_some_and(|t| !self.tracker.is_voter(&t))
              {
                self.lead_transferee = None;
                self.transfer_deadline = None;
              }
            }
            // A committed, validly-decoded ConfChange that the Changer rejects is an
            // unrecoverable logic violation (e.g. an overlapping change that should have
            // been prevented upstream). Poison so the failure is detectable rather than
            // a silent apply stall.
            Err(_) => {
              self.poison(PoisonReason::ConfChangeApply);
              break;
            }
          }
          // Do NOT call F::apply for ConfChange entries — they advance `applied` only.
        }
      }
      self.applied = idx;
    }
  }

  /// Start a real election campaign.
  ///
  /// `transfer` must be `true` when called from `on_timeout_now` (leader-transfer path):
  /// it sets `leader_transfer: true` on the broadcast `RequestVote` so that granters bypass
  /// their CheckQuorum/PreVote lease check (U3's `!force` guard).  For normal elections
  /// (election-timeout path, pre-vote quorum reached) pass `transfer = false`.
  fn become_candidate<L: LogStore, S: StableStore<NodeId = I>>(
    &mut self,
    now: Instant,
    log: &mut L,
    stable: &mut S,
    transfer: bool,
  ) {
    // Defensive guard: a non-voter (learner or removed node) must never campaign.
    // The handle_timeout gate is the primary check; this guard closes any other call sites.
    if !self.tracker.is_voter(&self.config.id()) {
      return;
    }
    self.term = self.term.next();
    // All pending work from the previous term is now stale (spec §7). Clear before recording
    // the self-vote below so old completions that arrive later are harmlessly ignored.
    self.pending.clear();
    self.role = Role::Candidate;
    self.leader = None;
    self.voted_for = Some(self.config.id());
    // Record self-vote in the ballot map (true = grant).
    self.votes.clear();
    self.votes.insert(self.config.id(), true);
    // Persist (term, self-vote). No Pending entry — a candidate doesn't owe an ack.
    // Stamp the current commit too (see on_request_vote): a read-modify of `hard_state()`
    // must not write back a stale `commit` that regresses the durable watermark (C1).
    let opid = self.mint_op_id();
    let hs = stable
      .hard_state()
      .with_term(self.term)
      .with_vote(self.voted_for)
      .with_commit(self.commit);
    stable.submit_write(opid, hs);
    self.committed_persisted = self.commit;
    self.arm_election_timer(now);

    let (last_index, last_term) = self.last_log(log);
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
    // single-node cluster fast-path: self-vote already a quorum under joint config.
    if self.tracker.vote_result(&self.votes).is_won() {
      self.become_leader(now, log, stable);
    }
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
  fn become_pre_candidate<L: LogStore>(&mut self, now: Instant, log: &L) -> bool {
    // Non-voter guard (mirrors become_candidate for defense-in-depth).
    if !self.tracker.is_voter(&self.config.id()) {
      return false;
    }
    self.role = Role::PreCandidate;
    self.leader = None;
    // Clear the ballot and record self pre-vote.
    self.votes.clear();
    self.votes.insert(self.config.id(), true);
    // Arm the election timer so a failed pre-vote retries on the next timeout.
    self.arm_election_timer(now);

    let advertised_term = self.term.next(); // proposed, not adopted
    let (last_index, last_term) = self.last_log(log);
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

  fn become_leader<L: LogStore, S: StableStore<NodeId = I>>(
    &mut self,
    now: Instant,
    log: &mut L,
    stable: &mut S,
  ) {
    self.role = Role::Leader;
    self.leader = Some(self.config.id());
    // Reset read-index state from the previous term (stale pending reads must not
    // be confirmed against the new term's commit index).
    self.read_only.reset(self.config.read_only());
    self.pending_reads.clear();
    // Clear any in-progress leader transfer — becoming the leader means the transfer
    // target (us) has won; the previous leader's transfer state is irrelevant.
    self.lead_transferee = None;
    self.transfer_deadline = None;
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
      self.election_deadline = Some(now + self.config.election_timeout());
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
    // Echo the heartbeat's context back to the leader.  This lets the leader count
    // this follower's ack toward the quorum needed to confirm a pending safe read.
    // An empty context is a normal heartbeat; the echo is harmless.
    let ctx = Bytes::copy_from_slice(hb.context());
    self.send(
      hb.leader(),
      Message::HeartbeatResp(crate::HeartbeatResp::new(term, me, ctx)),
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

  /// M4 Task 6 + liveness fix + M7-U4 ReadIndex acks.
  ///
  /// A HeartbeatResp from a peer:
  /// 1. Clears the peer's probe pause (so stalled replication resumes).
  /// 2. Frees one in-flight slot on a full Replicate window (etcd FreeFirstOne).
  /// 3. If the response carries a non-empty context, records the ack for the
  ///    corresponding pending read-index request and confirms any reads that have
  ///    reached a voter quorum.
  fn on_heartbeat_resp<L: LogStore, S: StableStore<NodeId = I>>(
    &mut self,
    from: I,
    log: &L,
    stable: &S,
    resp: crate::HeartbeatResp<I>,
  ) {
    if !self.role.is_leader() {
      return;
    }
    if let Some(pr) = self.tracker.progress_mut(&from) {
      pr.clear_probe_pause();
      // etcd FreeFirstOne: free one inflight slot so a Replicate peer whose in-flight window
      // was lost (e.g. a healed partition, dropped MsgApps) can resume on the next heartbeat
      // round instead of wedging until an unrelated proposal triggers a send.
      pr.free_inflight_on_heartbeat();
    }
    self.maybe_send_append(from, log, stable);

    // Liveness fix (review I1): if this peer is still in Snapshot state and has NOT yet
    // caught up to its pending snapshot index, RE-SEND the snapshot. The single
    // `InstallSnapshot` emitted by maybe_send_append's compacted-hole branch may have been
    // dropped; a Snapshot-state peer is unconditionally paused so maybe_send_append above
    // sends it nothing, and it only leaves Snapshot state once the snapshot is delivered and
    // acked (maybe_update). Without this resend a dropped InstallSnapshot wedges the follower
    // forever. Re-send each heartbeat round until it acks past `pending` and `maybe_update`
    // transitions it to Probe. (Read state/pending/match via an immutable borrow into locals,
    // drop the borrow, then call resend_snapshot — mirrors on_append_resp's re-borrow.)
    let resend = match self.tracker.progress(&from) {
      Some(pr) => match pr.state() {
        crate::ProgressState::Snapshot(pending) => pr.match_index() < pending,
        _ => false,
      },
      None => false,
    };
    if resend {
      self.resend_snapshot(from, stable);
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
        match st.req_from {
          None => {
            // Local leader read — emit ReadState event.
            self
              .events
              .push_back(crate::Event::ReadState(crate::ReadState::new(
                st.index, st.context,
              )));
          }
          Some(follower) => {
            // Forwarded read — reply ReadIndexResp to the originating follower.
            self.send(
              follower,
              Message::ReadIndexResp(crate::ReadIndexResp::new(term, me, st.index, st.context)),
            );
          }
        }
      }
    }
  }

  // ─── ReadIndex helpers ────────────────────────────────────────────────────────

  /// Whether the leader has committed an entry in its current term.
  ///
  /// A newly-elected leader cannot confirm reads against a commit index whose entry is from
  /// a prior term (§5.4.2).  It must wait until its no-op append is committed before
  /// confirming any reads.
  fn has_current_term_commit<L: LogStore>(&self, log: &L) -> bool {
    log
      .term(self.commit)
      .map(|t| t == self.term)
      .unwrap_or(false)
  }

  /// Confirm all pending reads in `pending_reads` by registering them with `read_only` and
  /// broadcasting the heartbeat round (Safe) or confirming immediately (LeaseBased).
  ///
  /// Called once the leader first commits an entry in its current term.
  fn flush_deferred_reads<L: LogStore, S: StableStore<NodeId = I>>(
    &mut self,
    now: Instant,
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
  fn maybe_flush_deferred_reads<L: LogStore, S: StableStore<NodeId = I>>(
    &mut self,
    now: Instant,
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

  /// Core leader read logic: register the read and broadcast / confirm.
  fn do_leader_read<L: LogStore>(
    &mut self,
    now: Instant,
    _log: &L,
    context: Bytes,
    from: Option<I>,
  ) {
    let me = self.config.id();
    let commit = self.commit;
    match self.config.read_only() {
      crate::ReadOnlyOption::Safe => {
        self
          .read_only
          .add_request(commit, context.clone(), from, me);
        // Single-node cluster fast-path: self-ack is already a quorum.
        let single_node_quorum = {
          let acks = self
            .read_only
            .acks_for(context.as_ref())
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
          let confirmed = self.read_only.advance(context.as_ref());
          let (term, me2) = (self.term, me);
          for st in confirmed {
            match st.req_from {
              None => {
                self
                  .events
                  .push_back(crate::Event::ReadState(crate::ReadState::new(
                    st.index, st.context,
                  )));
              }
              Some(follower) => {
                self.send(
                  follower,
                  Message::ReadIndexResp(crate::ReadIndexResp::new(
                    term, me2, st.index, st.context,
                  )),
                );
              }
            }
          }
        } else {
          self.broadcast_heartbeat_with_ctx(now, context);
        }
      }
      crate::ReadOnlyOption::LeaseBased => {
        // LeaseBased is sound only when CheckQuorum is also enabled: CheckQuorum's
        // periodic heartbeat round proves the leader still holds a quorum lease,
        // which is the precondition for skipping the per-read heartbeat.
        //
        // If check_quorum is disabled the lease invariant is not maintained, so
        // confirming immediately would be a linearizability hazard (a partitioned
        // leader could serve stale reads). Degrade silently to the Safe path
        // (heartbeat-quorum round) so any misconfiguration is safe rather than
        // silently incorrect.
        let use_lease = self.config.check_quorum();
        if use_lease {
          match from {
            None => {
              self
                .events
                .push_back(crate::Event::ReadState(crate::ReadState::new(
                  commit, context,
                )));
            }
            Some(follower) => {
              let (term, me2) = (self.term, me);
              self.send(
                follower,
                Message::ReadIndexResp(crate::ReadIndexResp::new(term, me2, commit, context)),
              );
            }
          }
        } else {
          // Degrade to Safe: record the request and broadcast a heartbeat round.
          self
            .read_only
            .add_request(commit, context.clone(), from, me);
          self.broadcast_heartbeat_with_ctx(now, context);
        }
      }
    }
  }

  /// Initiate a linearizable read.
  ///
  /// The `context` MUST uniquely identify each in-flight read — it is the **sole** correlator
  /// between this request and the eventual [`Event::ReadState`](crate::Event::ReadState)
  /// (locally) or [`ReadIndexResp`](crate::ReadIndexResp) (when forwarded to the leader).
  /// Reusing a `context` that is already in flight (including the **empty** context for two
  /// concurrent reads) returns [`ReadIndexError::DuplicateContext`]; the prior read's single
  /// confirmation would otherwise be the only acknowledgement for both calls.
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
  ///   [`ReadIndexError::NoLeader`] if no leader is known, or
  ///   [`ReadIndexError::ForwardingDisabled`] if `disable_proposal_forwarding` is set.
  /// - **Candidate / PreCandidate:** returns [`ReadIndexError::NoLeader`] (no leader to confirm).
  ///
  /// A poisoned node returns `Ok(())` without effect (it is inert; the driver should already be
  /// stopping on `poison_reason()`).
  pub fn read_index<L, S>(
    &mut self,
    now: Instant,
    log: &L,
    _stable: &S,
    context: Bytes,
  ) -> Result<(), crate::ReadIndexError>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    if self.poisoned {
      return Ok(());
    }
    match self.role {
      Role::Leader => {
        // Reject a context that is already in flight (deferred or registered) so the caller
        // is not left waiting forever for a confirmation that the prior read already owns.
        if self.read_context_in_flight(&context) {
          return Err(crate::ReadIndexError::DuplicateContext);
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
        let (term, me) = (self.term, self.config.id());
        self.send(
          leader,
          Message::ReadIndex(crate::ReadIndex::new(term, me, context)),
        );
        Ok(())
      }
      Role::Candidate | Role::PreCandidate => {
        // No leader to confirm reads.
        Err(crate::ReadIndexError::NoLeader)
      }
    }
  }

  /// Whether a read with this exact `context` is already in flight on the leader — either
  /// deferred awaiting the first current-term commit (`pending_reads`) or registered with the
  /// heartbeat-ack tracker (`read_only`). Used by [`Self::read_index`] to surface
  /// [`crate::ReadIndexError::DuplicateContext`] before any side effect.
  fn read_context_in_flight(&self, context: &Bytes) -> bool {
    self.pending_reads.iter().any(|(ctx, _)| ctx == context)
      || self.read_only.acks_for(context.as_ref()).is_some()
  }

  /// Leader receives a forwarded `ReadIndex` from a follower.
  fn on_read_index<L: LogStore, S: StableStore<NodeId = I>>(
    &mut self,
    now: Instant,
    log: &L,
    _stable: &S,
    ri: crate::ReadIndex<I>,
  ) {
    if !self.role.is_leader() {
      return;
    }
    let context = Bytes::copy_from_slice(ri.context());
    let from = ri.from();
    // Current-term-commit gate (same as the local path).
    if !self.has_current_term_commit(log) {
      self.pending_reads.push((context, Some(from)));
      return;
    }
    self.do_leader_read(now, log, context, Some(from));
  }

  /// Follower receives a `ReadIndexResp` from the leader.
  fn on_read_index_resp(&mut self, resp: crate::ReadIndexResp<I>) {
    let ctx = Bytes::copy_from_slice(resp.context());
    self
      .events
      .push_back(crate::Event::ReadState(crate::ReadState::new(
        resp.index(),
        ctx,
      )));
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
      // M4 Task 5: use the term-skip hint to jump next_index forward in one step.
      // find_conflict_by_term walks the leader's log from reject_hint_index downward
      // until we find an entry whose term ≤ reject_hint_term (the follower's conflicting
      // term). This lets the leader skip a whole conflicting term in O(terms) round-trips.
      let hint_index = resp.reject_hint_index();
      let hint_term = resp.reject_hint_term();
      let cur_next = pr.next_index();
      // Compute the conflict index before re-borrowing self.tracker.progress mutably.
      let conflict = self.find_conflict_by_term(log, hint_index, hint_term);
      // next_index must be at least 1 and must not advance past the current next on reject.
      let safe_next = if conflict == Index::ZERO || conflict >= cur_next {
        Index::new(cur_next.get().saturating_sub(1).max(1))
      } else {
        conflict
      };
      // Re-acquire progress to update (prior `pr` reference dropped implicitly by this point).
      if let Some(p) = self.tracker.progress_mut(&from) {
        p.become_probe();
        p.set_next_index(safe_next);
      }
      self.maybe_send_append(from, log, stable);
    } else {
      // Capture the state BEFORE maybe_update so we can guard the Probe -> Replicate
      // transition (review I5). etcd's MsgAppResp handler only switches Probe -> Replicate
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
        self.maybe_send_append(from, log, stable); // keep the pipeline moving if still behind
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
          }
        }
      }
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
        self.poison(PoisonReason::SnapshotDecode);
        return;
      }
    };

    // Step 2: restore the state machine. On failure, poison and return — leave NO partial state.
    if self.fsm.restore(snap).is_err() {
      self.poison(PoisonReason::SnapshotRestore);
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
    // `restore` re-baselines the read-view IMMEDIATELY (synchronous), keeping the log mutually
    // consistent with the commit/applied we just advanced (apply_committed reads it synchronously).
    // The snapshot blob is persisted separately via submit_snapshot (deferred completion). The
    // restore-vs-blob durability window is governed by the NORMATIVE durability-ordering contract on
    // `LogStore::restore` (review I8): a disk-backed log must not make the re-baseline durable ahead
    // of the blob, and otherwise must rely on restart re-sync. We do NOT rely on intra-call
    // ordering: if the process crashes before the blob is durable, restart-from-snapshot (M5-U3)
    // finds no durable snapshot and re-syncs from the leader — and with commit persistence (review
    // C1) the restart recovers the real commit watermark, so the re-sync resumes from the right
    // point. Acking before the blob is durable is safe because meta.last_index <= leader.commit —
    // those entries are already quorum-committed, so this ack cannot advance the cluster commit.
    log.restore(meta.last_index(), meta.last_term());

    // Tripwire (review I8): the install just advanced commit/applied to `meta.last_index` and the
    // re-baseline must have taken effect, so the log read-view now reflects the snapshot boundary:
    // first_index == last_index + 1. This documents and cheaply checks the synchronous-read-view
    // invariant that the deferred-blob durability contract depends on.
    debug_assert_eq!(
      log.first_index().get(),
      meta.last_index().get() + 1,
      "restore must re-baseline first_index to last_index + 1 (read-view consistent with commit/applied)"
    );

    // Step 5: persist the snapshot for restart recovery (deferred; see comment above).
    let opid = self.mint_op_id();
    stable.submit_snapshot(opid, meta.clone(), is.data().clone());

    // Step 6: emit the application event.
    self
      .events
      .push_back(crate::Event::SnapshotInstalled(meta.clone()));

    // Step 7 (M6): install the membership from the snapshot's ConfState. A follower
    // installing a snapshot jumps directly to the committed membership at that point;
    // the Tracker is rebuilt from the snapshot's conf, superseding the prior config.
    self.tracker = crate::Tracker::from_conf_state(
      meta.conf(),
      meta.last_index(),
      self.config.max_inflight_msgs(),
      self.config.max_inflight_bytes(),
    );

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
    now: Instant,
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
    let Some(pr) = self.tracker.progress_mut(&from) else {
      return;
    };
    if resp.reject() {
      // The snapshot was refused (shouldn't happen in the current protocol, but handle
      // defensively): revert to Probe so maybe_send_append re-probes and, if the follower
      // is still below first_index, re-sends the snapshot.
      pr.become_probe();
      // Drop the mutable borrow of `pr` before calling maybe_send_append (which re-borrows
      // self.tracker). The pattern mirrors on_append_resp's reject branch.
      self.maybe_send_append(from, log, stable);
    } else {
      // Success: maybe_update drives the Snapshot → Probe transition regardless of its return
      // value ("advanced" hint). We resume unconditionally so a peer leaving Snapshot is never
      // left un-poked. Drop `pr` before the self.* calls (borrow discipline mirrors on_append_resp).
      pr.maybe_update(resp.match_index());
      // Re-borrow self for the resume sequence (pr is dropped above).
      self.maybe_advance_commit(log);
      self.apply_committed(log);
      self.maybe_flush_deferred_reads(now, log, stable);
      self.maybe_send_append(from, log, stable);
    }
  }

  // ─── Leader transfer ──────────────────────────────────────────────────────────

  /// Initiate a graceful leader transfer to `to`.
  ///
  /// The leader stops accepting proposals, catches `to` up to its log, then sends it a
  /// `TimeoutNow` so it campaigns immediately (bypassing PreVote and the lease).  The
  /// cluster experiences at most one election timeout of unavailability.
  ///
  /// Returns `Ok(())` on success (transfer initiated or already targeting `to`).
  /// Returns `Err(TransferError::NotLeader)` if this node is not the current leader.
  /// Returns `Err(TransferError::NotAVoter)` if `to` is not a voter.
  /// Returns `Err(TransferError::AlreadyLeader)` if `to == self.id()`.
  pub fn transfer_leader<L, S>(
    &mut self,
    now: Instant,
    log: &L,
    stable: &S,
    to: I,
  ) -> Result<(), crate::TransferError<I>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    if !self.role.is_leader() {
      return Err(crate::TransferError::NotLeader {
        leader: self.leader,
      });
    }
    if to == self.config.id() {
      return Err(crate::TransferError::AlreadyLeader);
    }
    if !self.tracker.is_voter(&to) {
      return Err(crate::TransferError::NotAVoter);
    }
    // Already targeting this node — idempotent, just return Ok.
    if self.lead_transferee == Some(to) {
      return Ok(());
    }
    // Arm the transfer: stop accepting proposals, start the deadline window.
    self.lead_transferee = Some(to);
    self.transfer_deadline = Some(now + self.config.election_timeout());

    // If the target is already caught up, send TimeoutNow immediately.
    let target_match = self
      .tracker
      .progress(&to)
      .map(|p| p.match_index())
      .unwrap_or(crate::Index::ZERO);
    if target_match == log.last_index() {
      let (term, me) = (self.term, self.config.id());
      self.send(to, Message::TimeoutNow(crate::TimeoutNow::new(term, me)));
    } else {
      // Target is lagging: kick replication so it catches up.
      // TimeoutNow will be sent from on_append_resp once match_index == last_index.
      self.maybe_send_append(to, log, stable);
    }
    Ok(())
  }

  /// Receive a `TimeoutNow` from the current leader (transfer target path).
  ///
  /// The target campaigns immediately as a REAL candidate (bypassing PreVote and the lease),
  /// with `leader_transfer: true` on its `RequestVote` broadcast.  If this node is not a
  /// voter it ignores the message (etcd: removed/learner nodes silently drop TimeoutNow).
  fn on_timeout_now<L, S>(
    &mut self,
    now: Instant,
    log: &mut L,
    stable: &mut S,
    _tn: crate::TimeoutNow<I>,
  ) where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    // A non-voter cannot be elected; ignore.
    if !self.tracker.is_voter(&self.config.id()) {
      return;
    }
    // Campaign immediately as a REAL candidate (transfer=true):
    // - Does NOT do a PreVote phase even if config.pre_vote() is on.
    // - Sets leader_transfer=true on every RequestVote so granters bypass their lease.
    self.become_candidate(now, log, stable, true);
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
      .propose(d, &mut log, &stable, &bytes::Bytes::from_static(b"cmd"))
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
      .propose(d, &mut log, &stable, &bytes::Bytes::from_static(b"x"))
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
    // Payloads are Data-encoded (`encode_cmd`) so the committed entries decode as the SM's
    // `Command` and apply cleanly — an undecodable committed entry now (correctly) poisons.
    let e1 = Entry::new(
      Term::new(1),
      Index::new(1),
      EntryKind::Normal,
      encode_cmd(b"a"),
    );
    let e2 = Entry::new(
      Term::new(1),
      Index::new(2),
      EntryKind::Normal,
      encode_cmd(b"b"),
    );
    let e3 = Entry::new(
      Term::new(1),
      Index::new(3),
      EntryKind::Normal,
      encode_cmd(b"c"),
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
      encode_cmd(b"a"),
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

  /// Review C1 regression: a node that commits+applies entries [1..N] through the REAL path
  /// (self-elect → propose → handle_storage drains the append, advances commit, applies, AND
  /// now persists the commit watermark to HardState) must, after a `restart` from the SAME
  /// stores with NO snapshot, recover `commit == N`, `applied == N`, and a state machine that
  /// reflects all N applied entries — NOT an empty SM.
  ///
  /// FAILS ON OLD CODE: without the handle_storage commit-persist (and the with_commit stamps),
  /// the durable HardState.commit stays Index::ZERO for the node's life, so restart computes
  /// `commit = min(0, last_index).max(0) = 0`, the replay loop (0..0] is empty, and the
  /// restarted node recovers commit=0 with an EMPTY state machine despite the durable log
  /// holding all N committed entries.
  #[test]
  fn restart_recovers_commit_persisted_via_real_path() {
    use crate::{Config, Index, Instant};
    use core::time::Duration;
    // 1-voter cluster: quorum == 1, so a lone node self-elects and commits on storage drain.
    let cfg = Config::try_new(
      1u64,
      std::vec![1u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
    let mut log = crate::testkit::VecLog::default();
    // AsyncStable enqueues a Wrote completion for every submit_write, so handle_storage also
    // drains the commit-watermark completion (verifying it passes harmlessly through
    // on_stable_wrote with no Pending entry). Both testkit stores persist synchronously, so
    // the durable HardState reflects each write immediately.
    let mut stable = crate::testkit::AsyncStable::default();
    let mut ep = Endpoint::new(
      cfg.clone(),
      Instant::ORIGIN,
      7,
      crate::testkit::CountSm::default(),
    );

    // Self-elect (quorum == 1) and let the no-op LeaderAppend commit.
    let d = ep.poll_timeout().unwrap();
    ep.handle_timeout(d, &mut log, &mut stable);
    assert!(ep.role().is_leader(), "lone voter must self-elect");
    ep.handle_storage(d, &mut log, &mut stable);
    while ep.poll_message().is_some() {}
    while ep.poll_event().is_some() {}

    // Propose N Normal entries through the real path; drain storage after each so it commits
    // and applies (and, with the fix, persists the advanced commit watermark). The command
    // bytes are irrelevant to CountSm (it just counts applies); use fixed distinct payloads.
    let cmds: [&[u8]; 4] = [b"c0", b"c1", b"c2", b"c3"];
    const N: usize = 4;
    for cmd in cmds {
      ep.propose(d, &mut log, &stable, &bytes::Bytes::copy_from_slice(cmd))
        .unwrap();
      ep.handle_storage(d, &mut log, &mut stable);
      while ep.poll_message().is_some() {}
      while ep.poll_event().is_some() {}
    }
    assert!(!ep.is_poisoned(), "node must not be poisoned");
    // SM reflects N applied Normal entries (the leader's term-start no-op is Empty, not counted).
    assert_eq!(
      ep.state_machine().count(),
      N,
      "live leader must have applied all N proposed entries"
    );
    // The durable HardState.commit must now reflect the advanced watermark (the fix). The log
    // holds the no-op at index 1 plus N Normal entries, so commit == N + 1.
    let expected_commit = Index::new(N as u64 + 1);
    assert_eq!(
      stable.hard_state().commit(),
      expected_commit,
      "handle_storage must persist the advanced commit watermark into HardState (C1)"
    );

    // Restart from the SAME log + stable with NO snapshot.
    let restarted = Endpoint::restart(
      cfg,
      Instant::ORIGIN,
      9,
      crate::testkit::CountSm::default(),
      &mut log,
      &mut stable,
    );
    assert!(
      !restarted.is_poisoned(),
      "restarted node must not be poisoned"
    );
    assert_eq!(
      restarted.commit, expected_commit,
      "restart must recover the durable commit watermark, not collapse to applied/0 (C1)"
    );
    assert_eq!(
      restarted.applied, expected_commit,
      "restart must replay the committed tail so applied catches up to commit (C1)"
    );
    assert_eq!(
      restarted.state_machine().count(),
      N,
      "restarted SM must reflect all N committed entries, not be empty (C1)"
    );
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
    ep.propose(d, &mut log, &stable, &bytes::Bytes::from_static(b"cmd"))
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
    ep.propose(d, &mut log, &stable, &bytes::Bytes::from_static(b"x"))
      .unwrap();
    ep.propose(d, &mut log, &stable, &bytes::Bytes::from_static(b"y"))
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
        .propose(d, &mut log, &stable, &bytes::Bytes::copy_from_slice(&[i]))
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

  /// Review I5 regression: a SINGLE-entry ack from a peer already in Replicate must NOT
  /// rewind `next_index` or reset the in-flight window. The old code called
  /// `become_replicate()` unconditionally on every successful ack, which rewound
  /// `next_index` to `match.next()` and reset the whole `Inflights` window — so the next
  /// `maybe_send_append` re-sent the already-in-flight tail and the window cap never tripped.
  ///
  /// Setup (window = 2, one entry per message so each send is observable):
  ///   peer 2 in Replicate at match=1, next=2; propose 4 entries (indexes 2..=5).
  ///   The window fills after entries 2 and 3 are pipelined (inflight = {2, 3}, next = 4);
  ///   entries 4 and 5 are held back (paused). Now ack ONLY index 2.
  ///
  /// Expected (NEW): match advances to 2, slot for 2 frees, the peer STAYS in Replicate,
  ///   next stays 4 (never rewinds), and exactly ONE *new* entry (index 4) is pipelined —
  ///   the still-in-flight entry 3 is NOT re-sent. Final next = 5.
  /// Old behaviour (BUG): become_replicate rewinds next to match.next() = 3 and clears the
  ///   window, so the post-ack send re-transmits index 3 (already in flight) and next ends
  ///   at 4 — strictly less than the NEW path's 5, and a wasted re-send of an in-flight entry.
  #[test]
  fn single_ack_does_not_rewind_replicate_window() {
    use crate::{AppendResp, Config, Index, Instant, Message, Term, VoteResp};
    use core::time::Duration;

    // window = 2, exactly one entry per AppendEntries (max_size_per_msg = 1 byte; each
    // command below is 1 byte) so every send carries a single, identifiable entry.
    let cfg = Config::try_new(
      1u64,
      std::vec![1u64, 2u64, 3u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap()
    .with_max_inflight_msgs(2)
    .unwrap()
    .with_max_size_per_msg(1);

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

    // Move peer 2 into Replicate by acking the no-op (index 1). This is the legitimate
    // Probe -> Replicate transition (must still happen — preserved by the fix).
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
    assert!(
      ep.tracker.progress(&2u64).unwrap().state().is_replicate(),
      "peer 2 must be in Replicate after acking the no-op (Probe -> Replicate preserved)"
    );

    // Propose 4 entries (indexes 2..=5). With window = 2 the leader pipelines exactly two
    // (indexes 2 and 3) and then pauses; indexes 4 and 5 are held back.
    for i in 0u8..4 {
      let _ = ep
        .propose(d, &mut log, &stable, &bytes::Bytes::copy_from_slice(&[i]))
        .unwrap();
      ep.handle_storage(d, &mut log, &mut stable);
    }
    while ep.poll_message().is_some() {}

    // Snapshot the pipeline position: window is full at {2, 3}, next sits at 4.
    let next_before = ep.tracker.progress(&2u64).unwrap().next_index();
    assert_eq!(
      next_before,
      Index::new(4),
      "peer 2 should be pipelined to next=4 (entries 2,3 in flight) before the ack"
    );

    // Deliver a SINGLE-entry ack of just index 2 (the first in-flight index). This frees
    // exactly one slot; entry 3 is STILL in flight.
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
        Index::new(2), // ack ONLY index 2
      )),
    );

    // Collect the AppendEntries (and their entry indexes) the leader emits after the ack.
    let mut appends_after: usize = 0;
    let mut min_sent_index = Index::new(u64::MAX);
    let mut max_sent_index = Index::ZERO;
    while let Some(out) = ep.poll_message() {
      if out.to() == 2u64 {
        if let Message::AppendEntries(ae) = out.message() {
          if !ae.entries().is_empty() {
            appends_after += 1;
            for e in ae.entries() {
              if e.index() < min_sent_index {
                min_sent_index = e.index();
              }
              if e.index() > max_sent_index {
                max_sent_index = e.index();
              }
            }
          }
        }
      }
    }

    let next_after = ep.tracker.progress(&2u64).unwrap().next_index();
    let match_after = ep.tracker.progress(&2u64).unwrap().match_index();

    // (1) The peer stayed in Replicate (no spurious become_replicate / state churn).
    assert!(
      ep.tracker.progress(&2u64).unwrap().state().is_replicate(),
      "peer 2 must remain in Replicate after a single-entry ack"
    );

    // (2) match_index advanced monotonically to the acked index.
    assert_eq!(
      match_after,
      Index::new(2),
      "match must advance to the acked index 2"
    );

    // (3) next_index is monotonic non-decreasing — it must NOT rewind below its pre-ack
    //     value. The old unconditional become_replicate() rewound it to match.next() = 3.
    assert!(
      next_after >= next_before,
      "next_index rewound: was {} now {} (the bug rewinds to match.next())",
      next_before.get(),
      next_after.get()
    );

    // (4) The window cap is respected: freeing one slot lets the leader send at most ONE
    //     new entry. It must be a *fresh* entry (index 4), NOT a re-send of the entry that
    //     is still in flight (index 3). The old code re-sent index 3 because the window was
    //     reset and next rewound to 3.
    assert!(
      appends_after <= 1,
      "expected at most one new AppendEntries after freeing one slot, got {appends_after}"
    );
    if appends_after > 0 {
      assert!(
        min_sent_index > Index::new(3),
        "leader re-sent in-flight entry {} (still in flight) instead of a fresh entry; \
         min_sent={} max_sent={}",
        min_sent_index.get(),
        min_sent_index.get(),
        max_sent_index.get()
      );
    }

    // (5) Net effect: the freed slot advanced the pipeline by exactly one fresh entry
    //     (index 4), so next reaches 5. The bug leaves next stuck at 4 (re-sent 3 -> next 4).
    assert_eq!(
      next_after,
      Index::new(5),
      "after freeing one slot the leader should pipeline exactly one fresh entry (index 4), \
       leaving next=5; the bug re-sends in-flight index 3 and leaves next=4"
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
      .propose(d, &mut log, &stable, &bytes::Bytes::from_static(b"cmd1"))
      .unwrap();
    ep.handle_storage(d, &mut log, &mut stable);
    let _ = ep
      .propose(d, &mut log, &stable, &bytes::Bytes::from_static(b"cmd2"))
      .unwrap();
    ep.handle_storage(d, &mut log, &mut stable);
    // Drain all messages from the propose phase (probe fires on first propose, then pauses).
    while ep.poll_message().is_some() {}

    // Probe is now paused (partial batch was sent: noop@1 sent, but cmd1@2/cmd2@3 remain).
    // A new propose would call maybe_send_append → paused → no send.
    let _ = ep
      .propose(d, &mut log, &stable, &bytes::Bytes::from_static(b"cmd3"))
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
      .propose(d, &mut log, &stable, &bytes::Bytes::from_static(b"new"))
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
      let _ = ep.propose(d, &mut log, &stable, &cmd).unwrap();
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

  /// Like `make_single_node_leader_with_entries`, but the stable store is armed to DROP the
  /// `SnapshotWritten` completion of the threshold-crossing snapshot while still making the blob
  /// durable. Models a store that coalesces/loses the completion (review I9). After this returns,
  /// `pending_compact` is `Some`, the durable snapshot is readable, but no completion is queued.
  fn make_single_node_leader_dropping_snapshot_completion(
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
    // The threshold is crossed exactly once during the drive, so the only `submit_snapshot` is
    // the one whose completion we want dropped — arming at the start is sufficient and precise.
    stable.drop_next_snapshot_completion();

    let d = ep.poll_timeout().unwrap();
    ep.handle_timeout(d, &mut log, &mut stable); // self-elects
    assert!(ep.role().is_leader());
    ep.handle_storage(d, &mut log, &mut stable); // drain no-op
    while ep.poll_message().is_some() {}
    while ep.poll_event().is_some() {}

    for i in 0..n {
      let cmd = bytes::Bytes::copy_from_slice(&[i as u8]);
      let _ = ep.propose(d, &mut log, &stable, &cmd).unwrap();
      ep.handle_storage(d, &mut log, &mut stable);
      while ep.poll_message().is_some() {}
      while ep.poll_event().is_some() {}
    }
    (ep, log, stable)
  }

  /// Review I9: a dropped `SnapshotWritten` completion must NOT permanently wedge `pending_compact`
  /// (and thus all future snapshots/compaction). `handle_storage` reconciles `pending_compact`
  /// against the durable snapshot: once the persisted snapshot covers `up_to`, the deferred
  /// compaction is performed and the field cleared, even though the completion was never seen.
  ///
  /// FAILS ON OLD CODE (no reconciliation): `pending_compact` stays `Some`, `first_index` never
  /// advances, and the `is_some()` guard in `maybe_snapshot` wedges every future snapshot.
  #[test]
  fn dropped_snapshot_completion_reconciled_against_durable_snapshot() {
    // threshold=3: after no-op (idx 1) + 3 entries (idx 2,3,4), applied=4, first_index=1 → gap=3,
    // so a snapshot is submitted — but its completion is dropped by the armed store.
    let (mut ep, mut log, mut stable) = make_single_node_leader_dropping_snapshot_completion(3, 3);

    // Precondition: the snapshot blob IS durable, but pending_compact is stuck (no completion),
    // and the log was NOT compacted (the deferred compact never ran).
    assert!(
      stable.snapshot().is_some(),
      "the durable snapshot blob must be persisted even though the completion was dropped"
    );
    assert!(
      ep.pending_compact().is_some(),
      "pending_compact must still be set (the SnapshotWritten completion was dropped)"
    );
    assert_eq!(
      log.first_index(),
      Index::new(1),
      "log must not be compacted yet (no completion drained the deferred compact)"
    );

    // Drive handle_storage again. There is NO SnapshotWritten completion to drain, so on OLD code
    // this would be a no-op and the node would stay wedged. The I9 reconciliation must instead
    // notice the durable snapshot covers `up_to`, perform the compaction, and clear pending_compact.
    ep.handle_storage(crate::Instant::ORIGIN, &mut log, &mut stable);

    assert!(
      ep.pending_compact().is_none(),
      "I9: pending_compact must be reconciled to None against the durable snapshot"
    );
    assert!(
      log.first_index() > Index::new(1),
      "I9: the deferred compaction must run via reconciliation (first_index advanced, got {:?})",
      log.first_index()
    );

    // The node is no longer wedged: keep applying until the gap past the (new) first_index reaches
    // the threshold again, and a NEW snapshot must fire (pending_compact set for the fresh point).
    // After reconciliation first_index == 5 (compacted up_to=4); applied must reach 8 for gap >= 3.
    let first_index_after_reconcile = log.first_index();
    let d = crate::Instant::ORIGIN;
    for i in 0..4usize {
      let cmd = bytes::Bytes::copy_from_slice(&[100 + i as u8]);
      let _ = ep.propose(d, &mut log, &stable, &cmd).unwrap();
      ep.handle_storage(d, &mut log, &mut stable);
      while ep.poll_message().is_some() {}
      while ep.poll_event().is_some() {}
    }
    assert!(
      ep.pending_compact().is_some(),
      "I9: after reconciliation the node can snapshot again (not wedged)"
    );
    // And draining the (this time delivered) completion compacts further, proving end-to-end health.
    ep.handle_storage(d, &mut log, &mut stable);
    assert!(
      ep.pending_compact().is_none(),
      "the follow-up snapshot's completion clears pending_compact normally"
    );
    assert!(
      log.first_index() > first_index_after_reconcile,
      "the follow-up compaction advances first_index further (got {:?})",
      log.first_index()
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
    if let Some(p) = ep.tracker.progress_mut(&2u64) {
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
    let pr = ep.tracker.progress(&2u64).unwrap();
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
    if let Some(p) = ep.tracker.progress_mut(&2u64) {
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
    if let Some(p) = ep.tracker.progress_mut(&2u64) {
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

    if let Some(p) = ep.tracker.progress_mut(&2u64) {
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

  // ---- Review I1: heartbeat-driven snapshot resend (no wedge on dropped InstallSnapshot) ----

  /// Helper: drive `make_leader_with_compacted_log` peer 2 into Snapshot state and DROP the
  /// resulting InstallSnapshot (clear the outgoing queue), simulating the §11 message loss.
  /// Returns the leader, log, stable, and the snapshot's pending index (= offset).
  fn wedged_snapshot_follower(
    offset: u64,
    n_tail: usize,
  ) -> (
    Endpoint<u64, crate::testkit::CountSm>,
    crate::testkit::VecLog,
    crate::testkit::AsyncStable,
    crate::Index,
  ) {
    use crate::Index;

    let (mut ep, log, stable) = make_leader_with_compacted_log(offset, n_tail);

    // Peer 2 far behind: next_index < first_index = offset + 1.
    if let Some(p) = ep.tracker.progress_mut(&2u64) {
      p.become_probe();
      p.set_next_index(Index::new(2));
    }

    // First send: emits the InstallSnapshot and moves peer 2 into Snapshot(offset).
    ep.maybe_send_append(2u64, &log, &stable);
    assert!(
      ep.tracker.progress(&2u64).unwrap().state().is_snapshot(),
      "peer 2 must be in Snapshot state after the first send"
    );

    // DROP the InstallSnapshot — simulate the loss by clearing the outgoing queue.
    while ep.poll_message().is_some() {}

    (ep, log, stable, Index::new(offset))
  }

  /// Review I1 regression: a HeartbeatResp from a peer still stuck in Snapshot state (its
  /// InstallSnapshot was dropped) must RE-SEND the InstallSnapshot, carrying the same meta.
  ///
  /// FAILS-ON-OLD: without the resend hook the HeartbeatResp produces NO InstallSnapshot
  /// (maybe_send_append early-returns on the paused Snapshot peer), so the follower wedges.
  #[test]
  fn heartbeat_resend_snapshot_to_wedged_follower() {
    use crate::{Index, Instant, Message, Term};

    let offset = 5u64;
    let (mut ep, mut log, mut stable, pending) = wedged_snapshot_follower(offset, 2);
    assert_eq!(pending, Index::new(offset));

    // Peer 2 is still in Snapshot(offset) with match_index = 0 < pending: it has NOT received
    // the snapshot. Deliver a HeartbeatResp (empty context — no ReadIndex involvement).
    ep.handle_message(
      Instant::ORIGIN,
      &mut log,
      &mut stable,
      2u64,
      Message::HeartbeatResp(crate::HeartbeatResp::new(
        Term::new(1),
        2u64,
        bytes::Bytes::new(),
      )),
    );

    // A NEW InstallSnapshot to peer 2 must be emitted (the resend), carrying the same meta.
    let msgs: std::vec::Vec<_> = core::iter::from_fn(|| ep.poll_message()).collect();
    let snap_msgs: std::vec::Vec<_> = msgs
      .iter()
      .filter(|o| o.to() == 2u64 && matches!(o.message(), Message::InstallSnapshot(_)))
      .collect();
    assert_eq!(
      snap_msgs.len(),
      1,
      "a HeartbeatResp from a wedged Snapshot-state follower must RE-SEND exactly one InstallSnapshot"
    );
    let resent = match snap_msgs[0].message() {
      Message::InstallSnapshot(s) => s,
      _ => unreachable!(),
    };
    assert_eq!(
      resent.snapshot().last_index(),
      pending,
      "the resent InstallSnapshot must carry the same snapshot meta (last_index = pending)"
    );

    // Peer 2 remains in Snapshot(pending) — the resend does not change progress state.
    let pr = ep.tracker.progress(&2u64).unwrap();
    assert!(pr.state().is_snapshot(), "peer 2 stays in Snapshot state");
    if let crate::ProgressState::Snapshot(p) = pr.state() {
      assert_eq!(
        p, pending,
        "pending snapshot index is unchanged by the resend"
      );
    }
  }

  /// Review I1: the resend STOPS once the follower acks past its pending snapshot index.
  /// After a SnapshotResp (match >= pending) the peer leaves Snapshot state (→ Probe), so a
  /// subsequent HeartbeatResp must NOT emit another InstallSnapshot (no infinite resend / spam).
  #[test]
  fn no_snapshot_resend_after_follower_catches_up() {
    use crate::{Instant, Message, Term};

    let offset = 5u64;
    let (mut ep, mut log, mut stable, pending) = wedged_snapshot_follower(offset, 2);

    // First heartbeat round while wedged: resend fires (sanity — same as the test above).
    ep.handle_message(
      Instant::ORIGIN,
      &mut log,
      &mut stable,
      2u64,
      Message::HeartbeatResp(crate::HeartbeatResp::new(
        Term::new(1),
        2u64,
        bytes::Bytes::new(),
      )),
    );
    let resent = core::iter::from_fn(|| ep.poll_message())
      .filter(|o| o.to() == 2u64 && matches!(o.message(), Message::InstallSnapshot(_)))
      .count();
    assert_eq!(resent, 1, "resend fires while the follower is still wedged");

    // The follower finally receives a snapshot and acks at pending (SnapshotResp success).
    ep.handle_message(
      Instant::ORIGIN,
      &mut log,
      &mut stable,
      2u64,
      Message::SnapshotResp(crate::SnapshotResp::new(Term::new(1), 2u64, false, pending)),
    );
    // It must have left Snapshot state (maybe_update(pending) → Probe).
    assert!(
      !ep.tracker.progress(&2u64).unwrap().state().is_snapshot(),
      "after acking at pending the follower must leave Snapshot state"
    );
    while ep.poll_message().is_some() {} // drain anything the catch-up emitted

    // A subsequent HeartbeatResp must NOT emit another InstallSnapshot (resend has stopped).
    ep.handle_message(
      Instant::ORIGIN,
      &mut log,
      &mut stable,
      2u64,
      Message::HeartbeatResp(crate::HeartbeatResp::new(
        Term::new(1),
        2u64,
        bytes::Bytes::new(),
      )),
    );
    let after = core::iter::from_fn(|| ep.poll_message())
      .filter(|o| o.to() == 2u64 && matches!(o.message(), Message::InstallSnapshot(_)))
      .count();
    assert_eq!(
      after, 0,
      "once the follower has caught up, no further InstallSnapshot may be re-sent (no spam)"
    );
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
    if let Some(p) = ep.tracker.progress_mut(&2u64) {
      p.become_snapshot(Index::new(10));
    }
    assert!(ep.tracker.progress(&2u64).unwrap().state().is_snapshot());

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
      ep.tracker.progress(&2u64).unwrap().state().is_probe(),
      "reject SnapshotResp must transition peer to Probe"
    );

    // --- Success case: peer has been put back in Snapshot(10). ---
    if let Some(p) = ep.tracker.progress_mut(&2u64) {
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
    let pr = ep.tracker.progress(&2u64).unwrap();
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
  ///
  /// Updated for review C1 to drive the REAL commit-persist path (a live single-node leader)
  /// instead of `force_state`-injecting the durable commit. This makes the no-snapshot restart
  /// suite genuinely exercise the handle_storage commit-watermark write: the live leader's
  /// `commit` reaches HardState only because of the fix, and the restart reads it back.
  #[test]
  fn restart_no_snapshot_replays_from_one() {
    use crate::{Config, Index, Instant};
    use core::time::Duration;

    let cfg = Config::try_new(
      1u64,
      std::vec![1u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();

    // No snapshot. Drive a live single-node leader so commit advances and is persisted to
    // HardState by the handle_storage choke-point (no force_state injection).
    let mut stable = crate::testkit::AsyncStable::default();
    let mut log = crate::testkit::VecLog::default();
    let mut ep = Endpoint::new(
      cfg.clone(),
      Instant::ORIGIN,
      42,
      crate::testkit::CountSm::default(),
    );

    let d = ep.poll_timeout().unwrap();
    ep.handle_timeout(d, &mut log, &mut stable); // self-elect (quorum == 1)
    assert!(ep.role().is_leader());
    ep.handle_storage(d, &mut log, &mut stable); // no-op LeaderAppend at index 1 commits
    while ep.poll_message().is_some() {}
    while ep.poll_event().is_some() {}

    // Propose 2 Normal entries (indices 2 and 3); drain storage so each commits and applies.
    for b in [b"a".as_slice(), b"b".as_slice()] {
      ep.propose(d, &mut log, &stable, &bytes::Bytes::copy_from_slice(b))
        .unwrap();
      ep.handle_storage(d, &mut log, &mut stable);
      while ep.poll_message().is_some() {}
      while ep.poll_event().is_some() {}
    }
    assert_eq!(
      ep.state_machine().count(),
      2,
      "two Normal entries applied pre-restart"
    );
    assert_eq!(
      ep.commit,
      Index::new(3),
      "commit must reach 3 (no-op + 2 Normal)"
    );
    // The fix: commit watermark is durable, so restart can recover it.
    assert_eq!(stable.hard_state().commit(), Index::new(3));

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

  // ── M6-U5: propose_conf_change + apply-at-commit tests ────────────────────────────────────────

  /// Helper: build a single-node leader (node 1) with a VecLog + NoopStable, and drain storage
  /// so the no-op entry at index 1 is committed and applied. Returns (ep, log, stable, d).
  fn make_single_node_leader() -> (
    Endpoint<u64, crate::testkit::CountSm>,
    crate::testkit::VecLog,
    crate::testkit::NoopStable,
    crate::Instant,
  ) {
    use core::time::Duration;
    let cfg = crate::Config::try_new(
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
    // Drain so the no-op at index 1 commits and applies.
    ep.handle_storage(d, &mut log, &mut stable);
    while ep.poll_event().is_some() {}
    while ep.poll_message().is_some() {}
    (ep, log, stable, d)
  }

  /// Test 1: One-in-flight refusal.
  /// A second `propose_conf_change` before the first is applied → `ConfChangeInFlight`.
  /// After apply, a new one is accepted.
  #[test]
  fn conf_change_in_flight_refusal() {
    use crate::{ConfChange, ConfChangeType, ProposeError};
    let (mut ep, mut log, mut stable, d) = make_single_node_leader();

    // First conf-change: AddNode(2). Should succeed.
    let cc1 = ConfChange::new(ConfChangeType::AddNode, 2u64, bytes::Bytes::new());
    let idx1 = ep
      .propose_conf_change(d, &mut log, &stable, cc1)
      .expect("first conf change must be accepted");
    assert!(idx1 > crate::Index::ZERO);

    // Second conf-change before first is applied: must be refused.
    let cc2 = ConfChange::new(ConfChangeType::AddNode, 3u64, bytes::Bytes::new());
    let err = ep
      .propose_conf_change(d, &mut log, &stable, cc2.clone())
      .expect_err("second conf change must be refused while first is in flight");
    assert_eq!(
      err,
      ProposeError::ConfChangeInFlight,
      "expected ConfChangeInFlight error"
    );

    // Drive the first conf-change to committed+applied (single-node cluster: self-quorum).
    ep.handle_storage(d, &mut log, &mut stable);
    while ep.poll_event().is_some() {}
    while ep.poll_message().is_some() {}

    // Now a new conf-change is accepted.
    let cc3 = ConfChange::new(ConfChangeType::AddNode, 3u64, bytes::Bytes::new());
    let idx3 = ep.propose_conf_change(d, &mut log, &stable, cc3);
    assert!(idx3.is_ok(), "conf change must be accepted after apply");
  }

  /// Test 2: Simple AddNode applies at commit time.
  ///
  /// Invariants verified:
  /// - Tracker is updated ONLY at apply time (not at propose time).
  /// - `Event::ConfChanged` is emitted carrying the new `ConfState`.
  /// - `F::apply` is NOT called for the ConfChange entry (SM apply-count unchanged).
  #[test]
  fn simple_add_node_applies_at_commit() {
    use crate::{ConfChange, ConfChangeType};
    let (mut ep, mut log, mut stable, d) = make_single_node_leader();

    let sm_count_before = ep.state_machine().count();

    // Propose AddNode(2) — must NOT immediately change the Tracker.
    let cc = ConfChange::new(ConfChangeType::AddNode, 2u64, bytes::Bytes::new());
    let _idx = ep
      .propose_conf_change(d, &mut log, &stable, cc)
      .expect("propose AddNode must succeed");

    // Tracker must still only have voter 1 — not yet at commit time.
    assert!(
      !ep.tracker.is_voter(&2u64),
      "AddNode must NOT take effect before commit"
    );

    // Drive to committed+applied (single-node: self-quorum on storage drain).
    ep.handle_storage(d, &mut log, &mut stable);

    // Now the Tracker must have node 2 as a voter.
    assert!(
      ep.tracker.is_voter(&2u64),
      "AddNode must take effect after apply"
    );

    // SM apply-count must NOT have increased (ConfChange does not call F::apply).
    assert_eq!(
      ep.state_machine().count(),
      sm_count_before,
      "F::apply must NOT be called for a ConfChange entry"
    );

    // An Event::ConfChanged must have been emitted.
    let events: std::vec::Vec<_> = core::iter::from_fn(|| ep.poll_event()).collect();
    let conf_changed: std::vec::Vec<_> = events.iter().filter(|e| e.is_conf_changed()).collect();
    assert!(
      !conf_changed.is_empty(),
      "Event::ConfChanged must be emitted when AddNode is applied"
    );
    // The ConfState must contain voter 2.
    if let crate::Event::ConfChanged(cc_ev) = conf_changed[0] {
      assert!(
        cc_ev.conf().is_voter(&2u64),
        "ConfChanged event must carry a ConfState with voter 2"
      );
    }
  }

  /// Test 3: Simple RemoveNode applies at commit time.
  #[test]
  fn simple_remove_node_applies_at_commit() {
    use crate::{ConfChange, ConfChangeType};
    // Start with a 2-voter cluster (1, 2), single-node leader at id=1.
    use core::time::Duration;
    let cfg = crate::Config::try_new(
      1u64,
      std::vec![1u64, 2u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
    let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 42, crate::testkit::CountSm::default());
    let mut log = crate::testkit::VecLog::default();
    let mut stable = crate::testkit::NoopStable::default();

    let d = ep.poll_timeout().unwrap();
    ep.handle_timeout(d, &mut log, &mut stable); // become candidate
    // Self-vote is enough if quorum=1 among {1,2} with only self-vote — but actually 2-voter
    // quorum=2. We need to hand-grant ourselves leadership via a VoteResp.
    use crate::{Message, Term, VoteResp};
    ep.handle_message(
      d,
      &mut log,
      &mut stable,
      2u64,
      Message::VoteResp(VoteResp::new(Term::new(1), 2u64, false, false)),
    );
    assert!(ep.role().is_leader(), "node 1 must be leader");
    ep.handle_storage(d, &mut log, &mut stable);
    while ep.poll_event().is_some() {}
    while ep.poll_message().is_some() {}

    // Also need to advance commit for the no-op entry. The 2-voter quorum requires peer ack.
    // Simulate peer 2 acking the no-op.
    use crate::{AppendResp, Index};
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
    while ep.poll_event().is_some() {}
    while ep.poll_message().is_some() {}

    // Node 2 must be a voter initially.
    assert!(
      ep.tracker.is_voter(&2u64),
      "node 2 must be a voter before remove"
    );

    // Propose RemoveNode(2).
    let cc = ConfChange::new(ConfChangeType::RemoveNode, 2u64, bytes::Bytes::new());
    let _idx = ep
      .propose_conf_change(d, &mut log, &stable, cc)
      .expect("propose RemoveNode must succeed");

    // Not yet applied — node 2 still a voter.
    assert!(
      ep.tracker.is_voter(&2u64),
      "RemoveNode must NOT take effect before commit"
    );

    // Drive to commit: need quorum. Peer 2 acks the ConfChange entry at index 2.
    ep.handle_storage(d, &mut log, &mut stable); // leader self-match → 2
    // Peer 2 acks up to index 2 → quorum of {1,2} → commit.
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
        Index::new(2), // ack ConfChange at index 2
      )),
    );

    // Node 2 must now be gone from voters.
    assert!(
      !ep.tracker.is_voter(&2u64),
      "RemoveNode must take effect after apply"
    );

    // ConfChanged event.
    let events: std::vec::Vec<_> = core::iter::from_fn(|| ep.poll_event()).collect();
    assert!(
      events.iter().any(|e| e.is_conf_changed()),
      "Event::ConfChanged must be emitted when RemoveNode is applied"
    );
  }

  /// Test 4: Non-leader refused.
  #[test]
  fn non_leader_conf_change_is_refused() {
    use crate::{ConfChange, ConfChangeType, ProposeError};
    use core::time::Duration;
    let cfg = crate::Config::try_new(
      2u64,
      std::vec![1u64, 2u64, 3u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
    let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
    let mut log = crate::testkit::VecLog::default();
    let stable = crate::testkit::NoopStable::default();

    assert!(ep.role().is_follower());
    let cc = ConfChange::new(ConfChangeType::AddNode, 4u64, bytes::Bytes::new());
    let err = ep
      .propose_conf_change(Instant::ORIGIN, &mut log, &stable, cc)
      .expect_err("follower must refuse propose_conf_change");
    assert!(
      matches!(err, ProposeError::NotLeader { .. }),
      "expected NotLeader error, got {err:?}"
    );
  }

  // ── Review findings C1/I1 regression tests ────────────────────────────────────────────────────

  /// Regression: a freshly-elected leader must not accept a new ConfChange while an inherited
  /// one is uncommitted (review finding C1).
  ///
  /// Scenario: node 2 is a follower that receives a ConfChange entry from leader 1 but the
  /// entry is NOT committed (leader_commit stays at 0). Node 2 then wins an election and
  /// becomes leader. Its log contains an uncommitted ConfChange at index 2 (the inherited tail).
  /// The one-in-flight guard must fire and refuse a second ConfChange proposal.
  ///
  /// On the OLD code (before Fix C1): `pending_conf_index` was ZERO on a fresh leader, so
  /// `ZERO > applied` is false and the second ConfChange was wrongly accepted → Ok(_).
  /// On the FIXED code: `become_leader` sets `pending_conf_index = last_index` (= 2), so
  /// `2 > applied(0)` is true → Err(ConfChangeInFlight).
  #[test]
  fn inherited_uncommitted_conf_change_blocks_new_proposal() {
    use crate::{
      AppendEntries, ConfChange, ConfChangeType, Entry, EntryKind, Index, Message, ProposeError,
      Term, VoteResp,
    };
    use core::time::Duration;

    // Node 2 is a follower in a 3-voter cluster {1, 2, 3}.
    let cfg = crate::Config::try_new(
      2u64,
      std::vec![1u64, 2u64, 3u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
    let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 7, crate::testkit::CountSm::default());
    let mut log = crate::testkit::VecLog::default();
    let mut stable = crate::testkit::NoopStable::default();

    // Step 1: Leader 1 (term 1) sends node 2 an AppendEntries carrying:
    //   - index 1: the leader's no-op (Empty entry)
    //   - index 2: a ConfChange entry (AddNode 4)
    // leader_commit = 0 → neither entry is committed on node 2.
    use crate::Data as _;
    let cc_payload = {
      let cc = ConfChange::new(ConfChangeType::AddNode, 4u64, bytes::Bytes::new()).into_v2();
      let mut buf = std::vec::Vec::new();
      cc.encode(&mut buf);
      bytes::Bytes::from(buf)
    };
    let noop = Entry::new(
      Term::new(1),
      Index::new(1),
      EntryKind::Empty,
      bytes::Bytes::new(),
    );
    let conf_entry = Entry::new(
      Term::new(1),
      Index::new(2),
      EntryKind::ConfChange,
      cc_payload,
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
        std::vec![noop, conf_entry],
        Index::ZERO, // leader_commit = 0: nothing committed
      )),
    );
    // Drain the deferred append completion so entries are in the log.
    ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
    while ep.poll_message().is_some() {}

    // Verify: log holds entries at indices 1 and 2; applied and commit are still 0.
    assert_eq!(
      log.last_index(),
      Index::new(2),
      "follower log must hold both entries"
    );
    assert_eq!(ep.applied, Index::ZERO, "nothing applied yet");
    assert_eq!(ep.commit, Index::ZERO, "nothing committed yet");

    // Step 2: A term advance causes node 2 to become a candidate in term 2 and win.
    // Simulate: election timeout fires, node 2 becomes candidate (term 2), then receives a
    // grant from node 3 → quorum (self + 3) → become_leader.
    let d = ep.poll_timeout().unwrap();
    ep.handle_timeout(d, &mut log, &mut stable); // become candidate, term 2
    assert!(ep.role().is_candidate());
    while ep.poll_message().is_some() {}

    // Node 3 grants the vote → quorum reached → become_leader.
    ep.handle_message(
      d,
      &mut log,
      &mut stable,
      3u64,
      Message::VoteResp(VoteResp::new(Term::new(2), 3u64, false, false)),
    );
    assert!(ep.role().is_leader(), "node 2 must be leader after quorum");

    // Step 3: Now call propose_conf_change(AddNode(5)).
    // The inherited tail (index 2: uncommitted ConfChange) must block this.
    // Fix C1 sets pending_conf_index = last (= 2) in become_leader; applied = 0;
    // so 2 > 0 is true → ConfChangeInFlight.
    let cc_new = ConfChange::new(ConfChangeType::AddNode, 5u64, bytes::Bytes::new());
    let result = ep.propose_conf_change(d, &mut log, &stable, cc_new);
    assert_eq!(
      result,
      Err(ProposeError::ConfChangeInFlight),
      "a freshly-elected leader must refuse a new ConfChange while an inherited one is \
       uncommitted (review finding C1)"
    );
  }

  /// Regression: a committed ConfChange that the Changer rejects must poison the node
  /// rather than silently stalling apply (review finding I1).
  ///
  /// Scenario: node 2 (follower) receives an AppendEntries that carries a leave-joint
  /// ConfChange entry and commits it (leader_commit covers it). The node is NOT in joint
  /// config, so Changer::leave_joint returns Err. Fix I1 adds `self.poison()` in that
  /// branch so the failure is observable rather than a silent apply stall.
  #[test]
  fn changer_error_at_apply_poisons_node() {
    use crate::{AppendEntries, Entry, EntryKind, Index, Message, Term};
    use core::time::Duration;

    // Node 2 is a follower in a 3-voter cluster {1, 2, 3}.
    let cfg = crate::Config::try_new(
      2u64,
      std::vec![1u64, 2u64, 3u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
    let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 7, crate::testkit::CountSm::default());
    let mut log = crate::testkit::VecLog::default();
    let mut stable = crate::testkit::NoopStable::default();

    // Build a leave-joint ConfChange payload. The node is not in joint config, so
    // when this entry commits the Changer will return Err(NotInJointConfig).
    use crate::Data as _;
    let leave_payload = {
      let cc = crate::ConfChangeV2::<u64>::leave_joint();
      let mut buf = std::vec::Vec::new();
      cc.encode(&mut buf);
      bytes::Bytes::from(buf)
    };

    // Leader 1 (term 1) sends two entries: a no-op and the bad leave-joint ConfChange.
    // leader_commit = 2 forces the follower to commit and apply both entries immediately.
    let noop = Entry::new(
      Term::new(1),
      Index::new(1),
      EntryKind::Empty,
      bytes::Bytes::new(),
    );
    let leave_entry = Entry::new(
      Term::new(1),
      Index::new(2),
      EntryKind::ConfChange,
      leave_payload,
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
        std::vec![noop, leave_entry],
        Index::new(2), // leader_commit = 2: both entries committed
      )),
    );
    // Drain the deferred append completion so apply_committed runs with the durable entries.
    ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);

    // The Changer must have rejected leave_joint (not in joint) → node poisoned (Fix I1).
    assert!(
      ep.is_poisoned(),
      "node must be poisoned when Changer rejects a committed ConfChange at apply time \
       (review finding I1)"
    );
  }

  // ── M6-U6: leader step-down on self-removal/demotion ─────────────────────────────────────────

  /// Helper: elect node 1 as leader of a 3-voter cluster {1, 2, 3}, drive the no-op to
  /// committed+applied, then return (ep, log, stable, d).
  fn make_three_node_leader() -> (
    Endpoint<u64, crate::testkit::CountSm>,
    crate::testkit::VecLog,
    crate::testkit::NoopStable,
    crate::Instant,
  ) {
    use crate::{Message, Term, VoteResp};
    use core::time::Duration;
    let cfg = crate::Config::try_new(
      1u64,
      std::vec![1u64, 2u64, 3u64],
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
    // Drain storage: no-op LeaderAppend fires → self match → commit advances.
    ep.handle_storage(d, &mut log, &mut stable);
    // Need peer ack to commit the no-op in a 3-voter cluster (quorum=2).
    use crate::{AppendResp, Index};
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
        crate::Term::ZERO,
        Index::new(1),
      )),
    );
    while ep.poll_event().is_some() {}
    while ep.poll_message().is_some() {}
    (ep, log, stable, d)
  }

  /// Test U6-1: A leader that removes itself (RemoveNode(self)) steps down immediately when
  /// the ConfChange is committed+applied.
  ///
  /// Invariants:
  /// - role → Follower (same term, no term bump)
  /// - leader → None
  /// - heartbeat_deadline → None (no longer heartbeating)
  /// - election_deadline → None (non-voter must not campaign)
  /// - is_voter(self) == false in the new Tracker
  #[test]
  fn leader_steps_down_on_self_removal() {
    use crate::{AppendResp, ConfChange, ConfChangeType, Index, Message, Term};

    let (mut ep, mut log, mut stable, d) = make_three_node_leader();
    let self_id = ep.id();
    let term_before = ep.term();

    // Propose RemoveNode(self).
    let cc = ConfChange::new(ConfChangeType::RemoveNode, self_id, bytes::Bytes::new());
    let idx = ep
      .propose_conf_change(d, &mut log, &stable, cc)
      .expect("RemoveNode(self) must be accepted");

    // Not yet committed: leader must still be leader.
    assert!(
      ep.role().is_leader(),
      "leader must not step down before commit"
    );

    // Drive to commit: leader self-match via storage drain, then peer 2 acks.
    ep.handle_storage(d, &mut log, &mut stable);
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

    // After apply: leader must have stepped down.
    assert!(
      ep.role().is_follower(),
      "leader must step down after RemoveNode(self) is applied"
    );
    assert_eq!(
      ep.leader(),
      None,
      "leader field must be cleared after step-down"
    );
    assert!(
      ep.heartbeat_deadline.is_none(),
      "heartbeat_deadline must be None after step-down"
    );
    assert!(
      ep.election_deadline.is_none(),
      "election_deadline must be None: a non-voter must not campaign"
    );
    // Step-down is at the same term (no bump).
    assert_eq!(ep.term(), term_before, "step-down must not bump the term");
    // The new Tracker must not have self as a voter.
    assert!(
      !ep.tracker.is_voter(&self_id),
      "self must not be a voter after RemoveNode(self) is applied"
    );
  }

  /// Test U6-2: A leader demoted to learner (AddLearnerNode(self)) also steps down.
  #[test]
  fn leader_steps_down_on_demotion_to_learner() {
    use crate::{AppendResp, ConfChange, ConfChangeType, Index, Message, Term};

    let (mut ep, mut log, mut stable, d) = make_three_node_leader();
    let self_id = ep.id();
    let term_before = ep.term();

    // Propose AddLearnerNode(self) — demotes the current leader to learner.
    let cc = ConfChange::new(ConfChangeType::AddLearnerNode, self_id, bytes::Bytes::new());
    let idx = ep
      .propose_conf_change(d, &mut log, &stable, cc)
      .expect("AddLearnerNode(self) must be accepted");

    // Not yet committed: leader must still be leader.
    assert!(
      ep.role().is_leader(),
      "leader must not step down before commit"
    );

    // Drive to commit.
    ep.handle_storage(d, &mut log, &mut stable);
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

    // After apply: leader stepped down; self is now a learner (not a voter).
    assert!(
      ep.role().is_follower(),
      "leader must step down after AddLearnerNode(self) is applied"
    );
    assert_eq!(ep.leader(), None, "leader field must be cleared");
    assert!(
      ep.heartbeat_deadline.is_none(),
      "heartbeat_deadline must be None"
    );
    assert!(
      ep.election_deadline.is_none(),
      "election_deadline must be None"
    );
    assert_eq!(ep.term(), term_before, "step-down must not bump the term");
    assert!(
      !ep.tracker.is_voter(&self_id),
      "self must not be a voter after demotion to learner"
    );
    assert!(
      ep.tracker.is_learner(&self_id),
      "self must be a learner after AddLearnerNode(self)"
    );
  }

  /// Test U6-3: A non-voter (learner) that has an election timer fire must NOT become a
  /// candidate. The term must not change and the role must stay Follower.
  #[test]
  fn non_voter_does_not_campaign_on_timeout() {
    use core::time::Duration;

    // Node 4 is a learner in {voters: [1,2,3], learners: [4]}.
    // We bootstrap as if 4 is a voter (Config requirement) then manually adjust the Tracker.
    let cfg = crate::Config::try_new(
      4u64,
      std::vec![1u64, 2u64, 3u64, 4u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
    let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 99, crate::testkit::CountSm::default());
    let mut log = crate::testkit::VecLog::default();
    let mut stable = crate::testkit::NoopStable::default();

    // Demote node 4 to learner in the Tracker by rebuilding it from a ConfState that has
    // node 4 as a learner, not a voter.
    let learner_cs = crate::ConfState::new([1u64, 2u64, 3u64], [4u64], [], [], false);
    ep.tracker = crate::Tracker::from_conf_state(&learner_cs, crate::Index::ZERO, 256, 0);

    // Sanity: node 4 is NOT a voter.
    assert!(!ep.tracker.is_voter(&4u64), "node 4 must not be a voter");
    assert!(ep.tracker.is_learner(&4u64), "node 4 must be a learner");

    let term_before = ep.term();

    // Arm the election deadline to now (expired).
    ep.election_deadline = Some(Instant::ORIGIN);

    // Fire handle_timeout at now (deadline expired).
    ep.handle_timeout(Instant::ORIGIN, &mut log, &mut stable);

    // Non-voter must NOT have started an election.
    assert!(
      ep.role().is_follower(),
      "non-voter must remain a follower after election timeout"
    );
    assert_eq!(
      ep.term(),
      term_before,
      "non-voter must not bump the term on election timeout"
    );
    // No RequestVote messages emitted.
    assert!(
      ep.poll_message().is_none(),
      "non-voter must not send RequestVote"
    );
  }

  /// Test U6-4: With `step_down_on_removal = false`, a leader that removes itself keeps
  /// the Leader role (the operator has opted out of the default behavior).
  #[test]
  fn step_down_disabled_leader_keeps_role_after_self_removal() {
    use crate::{AppendResp, ConfChange, ConfChangeType, Index, Message, Term};
    use core::time::Duration;

    let cfg = crate::Config::try_new(
      1u64,
      std::vec![1u64, 2u64, 3u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap()
    .with_step_down_on_removal(false); // opt out

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
      Message::VoteResp(crate::VoteResp::new(Term::new(1), 2u64, false, false)),
    );
    assert!(ep.role().is_leader());
    ep.handle_storage(d, &mut log, &mut stable);
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
    while ep.poll_event().is_some() {}
    while ep.poll_message().is_some() {}

    // Propose and apply RemoveNode(self).
    let cc = ConfChange::new(ConfChangeType::RemoveNode, 1u64, bytes::Bytes::new());
    let idx = ep
      .propose_conf_change(d, &mut log, &stable, cc)
      .expect("RemoveNode(self) must be accepted");
    ep.handle_storage(d, &mut log, &mut stable);
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

    // With step_down_on_removal=false, the leader must keep the Leader role.
    assert!(
      ep.role().is_leader(),
      "leader must keep leadership when step_down_on_removal=false"
    );
  }

  /// Test U6-5: Joint phase — a leader still present in the outgoing joint half must NOT
  /// step down mid-joint (it must shepherd the joint → simple transition).
  ///
  /// We use `enter_joint` with `auto_leave=false` (Explicit transition) so the leader stays
  /// in a joint config where the outgoing half still contains self. `is_voter` checks BOTH
  /// halves, so the leader remains a voter and must NOT step down.
  #[test]
  fn joint_phase_leader_keeps_role_while_still_in_outgoing_half() {
    use crate::{AppendResp, ConfChangeType, Index, Message, Term};
    use core::time::Duration;

    // 3-voter cluster {1, 2, 3}. We propose a joint change that replaces node 3 with node 4
    // via enter_joint (Explicit transition). Node 1 (leader) is still in both the incoming
    // AND outgoing half → is_voter(1) == true → must not step down.
    let cfg = crate::Config::try_new(
      1u64,
      std::vec![1u64, 2u64, 3u64],
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
      Message::VoteResp(crate::VoteResp::new(Term::new(1), 2u64, false, false)),
    );
    assert!(ep.role().is_leader());
    ep.handle_storage(d, &mut log, &mut stable);
    // Commit the no-op via peer 2.
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
    while ep.poll_event().is_some() {}
    while ep.poll_message().is_some() {}

    // Propose an Explicit joint change: add node 4, remove node 3. Node 1 stays in BOTH
    // incoming and outgoing halves, so is_voter(1) == true throughout.
    let ccv2 = crate::ConfChangeV2::new(
      crate::ConfChangeTransition::Explicit,
      std::vec![
        crate::ConfChangeSingle::new(ConfChangeType::AddNode, 4u64),
        crate::ConfChangeSingle::new(ConfChangeType::RemoveNode, 3u64),
      ],
      bytes::Bytes::new(),
    );
    let idx = ep
      .propose_conf_change_v2(d, &mut log, &stable, ccv2)
      .expect("joint conf change must be accepted");

    // Drive to commit: storage drain + peer 2 ack.
    ep.handle_storage(d, &mut log, &mut stable);
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

    // We are now in joint config. Node 1 is still in both halves → is_voter(1) == true.
    assert!(
      ep.tracker.is_joint(),
      "cluster must be in joint configuration"
    );
    assert!(
      ep.tracker.is_voter(&1u64),
      "node 1 must still be a voter in the joint config (outgoing half)"
    );
    // Leader must NOT have stepped down.
    assert!(
      ep.role().is_leader(),
      "leader must not step down mid-joint when still a voter in the outgoing half"
    );
  }

  // ─── M7-U2: PreVote tests ─────────────────────────────────────────────────────────────────────

  /// Test 1: A PreCandidate that loses pre-vote stays at the SAME term.
  /// A node with pre_vote=true times out → PreCandidate; peers reject (they have a live leader)
  /// → the node does NOT advance to Candidate, and self.term is UNCHANGED.
  #[test]
  fn pre_candidate_loses_stays_at_same_term() {
    use crate::{Config, Instant, Message, Term, VoteResp};
    use core::time::Duration;

    let cfg = Config::try_new(
      1u64,
      std::vec![1u64, 2u64, 3u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap()
    .with_pre_vote(true);

    let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 42, Noop);
    let mut log = crate::testkit::NoopLog;
    let mut stable = crate::testkit::NoopStable::default();

    // Trigger the election timer — with pre_vote enabled, node becomes PreCandidate.
    let deadline = ep.poll_timeout().unwrap();
    ep.handle_timeout(deadline, &mut log, &mut stable);
    assert!(ep.role().is_pre_candidate(), "must become PreCandidate");
    assert_eq!(
      ep.term(),
      Term::ZERO,
      "term must NOT be bumped during pre-vote"
    );

    // Drain the RequestVote{pre_vote:true, term:1} messages to peers 2 and 3.
    let mut pre_vote_msgs: std::vec::Vec<u64> = std::vec::Vec::new();
    while let Some(out) = ep.poll_message() {
      match out.message() {
        Message::RequestVote(rv) => {
          assert!(rv.pre_vote(), "must be a pre-vote request");
          assert_eq!(
            rv.term(),
            Term::new(1),
            "advertised term must be self.term.next()"
          );
          pre_vote_msgs.push(out.to());
        }
        other => panic!("unexpected message: {other:?}"),
      }
    }
    pre_vote_msgs.sort();
    assert_eq!(
      pre_vote_msgs,
      std::vec![2u64, 3u64],
      "must send pre-vote to both peers"
    );

    // Peers reject: they have a live leader (simulate by sending reject responses at self.term=0).
    // A pre-vote reject carries the responder's term (self.term = 0 here since this is a fresh
    // cluster test; the key invariant is the pre-candidate does NOT advance to Candidate).
    for peer in [2u64, 3u64] {
      ep.handle_message(
        deadline,
        &mut log,
        &mut stable,
        peer,
        Message::VoteResp(VoteResp::new(
          Term::ZERO,
          peer,
          true, /* pre_vote */
          true, /* reject */
        )),
      );
    }

    // Must still be PreCandidate (or return to Follower), NOT Candidate, and term must be 0.
    assert!(
      !ep.role().is_candidate(),
      "pre-candidate that loses must NOT become a real Candidate"
    );
    assert_eq!(
      ep.term(),
      Term::ZERO,
      "term must be unchanged after failed pre-vote"
    );
  }

  /// Test 2: A partitioned node's pre-vote requests do NOT cause grantors to adopt the higher
  /// advertised term. A follower that receives RequestVote{pre_vote:true, term: self.term+5}
  /// must NOT adopt term+5; its term remains unchanged.
  #[test]
  fn pre_vote_request_does_not_raise_granter_term() {
    use crate::{Config, Index, Instant, Message, RequestVote, Term};
    use core::time::Duration;

    // Follower node 2 with pre_vote=false (it's a stable cluster peer).
    let cfg = Config::try_new(
      2u64,
      std::vec![1u64, 2u64, 3u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
    let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 7, Noop);
    let mut log = crate::testkit::NoopLog;
    let mut stable = crate::testkit::NoopStable::default();

    // Establish a live leader so the lease check blocks the grant.
    // Feed a heartbeat from leader 1 in term 3 — this sets leader=Some(1) and re-arms timer.
    ep.handle_message(
      Instant::ORIGIN,
      &mut log,
      &mut stable,
      1u64,
      Message::Heartbeat(crate::Heartbeat::new(
        Term::new(3),
        1u64,
        crate::Index::ZERO,
        bytes::Bytes::new(),
      )),
    );
    while ep.poll_message().is_some() {} // drain HeartbeatResp
    assert_eq!(
      ep.term(),
      Term::new(3),
      "term from heartbeat must be adopted"
    );
    assert_eq!(ep.leader(), Some(1u64), "leader must be known");

    // Now a partitioned node 1 (pre-candidate) sends a pre-vote request at term+5 = 8.
    ep.handle_message(
      Instant::ORIGIN,
      &mut log,
      &mut stable,
      1u64, // from
      Message::RequestVote(RequestVote::new(
        Term::new(8), // advertised future term (pre_vote)
        1u64,         // candidate
        Index::ZERO,
        Term::ZERO,
        true,  // pre_vote
        false, // leader_transfer
      )),
    );

    // The node must NOT have adopted term 8.
    assert_eq!(
      ep.term(),
      Term::new(3),
      "pre-vote request must NOT cause the receiver to adopt the advertised term"
    );

    // A response must have been sent (reject, since live leader + healthy election timer).
    let resp = ep.poll_message().expect("must send a VoteResp");
    match resp.message() {
      Message::VoteResp(vr) => {
        assert!(vr.pre_vote(), "response must be a pre-vote response");
        assert!(
          vr.reject(),
          "must reject (live leader + healthy election timer)"
        );
        // Rejection carries self.term (3), not the advertised 8.
        assert_eq!(
          vr.term(),
          Term::new(3),
          "reject response must carry self.term, not the advertised term"
        );
      }
      other => panic!("expected VoteResp, got {other:?}"),
    }
  }

  /// Test 3: A successful pre-vote quorum transitions to a real Candidate with a term bump
  /// and a real RequestVote{pre_vote:false} broadcast.
  #[test]
  fn successful_pre_vote_quorum_starts_real_campaign() {
    use crate::{Config, Instant, Message, Term, VoteResp};
    use core::time::Duration;

    let cfg = Config::try_new(
      1u64,
      std::vec![1u64, 2u64, 3u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap()
    .with_pre_vote(true);

    let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 42, Noop);
    let mut log = crate::testkit::NoopLog;
    let mut stable = crate::testkit::NoopStable::default();

    // Fire election → PreCandidate.
    let deadline = ep.poll_timeout().unwrap();
    ep.handle_timeout(deadline, &mut log, &mut stable);
    assert!(ep.role().is_pre_candidate());
    assert_eq!(ep.term(), Term::ZERO, "term must not bump during pre-vote");
    while ep.poll_message().is_some() {} // drain pre-vote RequestVote msgs

    // Peer 2 grants the pre-vote. Node has no live leader (election timer expired), log
    // is at ZERO (same as ours) → grant. The response carries the advertised term (1).
    ep.handle_message(
      deadline,
      &mut log,
      &mut stable,
      2u64,
      Message::VoteResp(VoteResp::new(
        Term::new(1),
        2u64,
        true,  /* pre_vote */
        false, /* grant */
      )),
    );

    // Pre-vote quorum reached (self + peer2 = 2/3 → majority).
    // Node must now be a real Candidate with term bumped to 1.
    assert!(
      ep.role().is_candidate(),
      "must advance to real Candidate after pre-vote quorum"
    );
    assert_eq!(
      ep.term(),
      Term::new(1),
      "term must be bumped on real campaign"
    );

    // Must broadcast real RequestVote{pre_vote:false} to peers.
    let mut real_vote_targets: std::vec::Vec<u64> = std::vec::Vec::new();
    while let Some(out) = ep.poll_message() {
      if let Message::RequestVote(rv) = out.message() {
        assert!(!rv.pre_vote(), "real campaign must send pre_vote=false");
        assert_eq!(
          rv.term(),
          Term::new(1),
          "real RequestVote must carry the new term"
        );
        real_vote_targets.push(out.to());
        // Note: other message types (empty-append from become_candidate) are ignored here.
      }
    }
    real_vote_targets.sort();
    assert_eq!(
      real_vote_targets,
      std::vec![2u64, 3u64],
      "real campaign must broadcast to both voter peers"
    );
  }

  /// Test 4: An up-to-date check still applies to pre-votes. A pre-candidate with a STALE log
  /// is rejected even if the lease is open (no live leader).
  #[test]
  fn pre_vote_rejected_for_stale_log() {
    use crate::{Config, Entry, EntryKind, Index, Instant, Message, RequestVote, Term};
    use core::time::Duration;

    // Follower node 2 with a fresh log (entries up to index 5@term3).
    let cfg = Config::try_new(
      2u64,
      std::vec![1u64, 2u64, 3u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
    let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 7, crate::testkit::CountSm::default());
    let mut log = crate::testkit::VecLog::default();
    let mut stable = crate::testkit::NoopStable::default();

    // Seed log with 5 entries in term 3 so our last_log = (5, 3).
    log.force_append(&[
      Entry::new(
        Term::new(3),
        Index::new(1),
        EntryKind::Normal,
        bytes::Bytes::new(),
      ),
      Entry::new(
        Term::new(3),
        Index::new(2),
        EntryKind::Normal,
        bytes::Bytes::new(),
      ),
      Entry::new(
        Term::new(3),
        Index::new(3),
        EntryKind::Normal,
        bytes::Bytes::new(),
      ),
      Entry::new(
        Term::new(3),
        Index::new(4),
        EntryKind::Normal,
        bytes::Bytes::new(),
      ),
      Entry::new(
        Term::new(3),
        Index::new(5),
        EntryKind::Normal,
        bytes::Bytes::new(),
      ),
    ]);

    // No leader known — lease is open. Election timer is expired (use Instant::ORIGIN as now).
    // Pre-vote from node 1 with a STALE log (last_log_index=2, last_log_term=1 < our 5@3).
    // This violates the up-to-date check → must be rejected.
    ep.handle_message(
      Instant::ORIGIN,
      &mut log,
      &mut stable,
      1u64,
      Message::RequestVote(RequestVote::new(
        Term::new(4), // advertised term (pre_vote)
        1u64,
        Index::new(2), // stale last_log_index
        Term::new(1),  // stale last_log_term
        true,          // pre_vote
        false,
      )),
    );

    let resp = ep.poll_message().expect("must reply to pre-vote");
    match resp.message() {
      Message::VoteResp(vr) => {
        assert!(vr.pre_vote(), "must be a pre-vote response");
        assert!(
          vr.reject(),
          "must reject pre-vote from a stale-log candidate"
        );
      }
      other => panic!("expected VoteResp, got {other:?}"),
    }
    // The receiver's term must be unchanged (pre-vote never changes term).
    assert_eq!(
      ep.term(),
      Term::ZERO,
      "pre-vote must not change receiver term"
    );
  }

  /// Test 5: Term pre-pass exemption. A follower receiving RequestVote{pre_vote:true, term:T+5}
  /// does NOT adopt T+5. Its term is unchanged, and it replies (grant or reject) immediately
  /// without persisting. Specifically: voted_for is not set, and the response is immediate
  /// (not deferred behind a storage write).
  #[test]
  fn term_pre_pass_exemption_for_pre_vote_request() {
    use crate::{Config, Index, Instant, Message, RequestVote, Term};
    use core::time::Duration;

    let cfg = Config::try_new(
      2u64,
      std::vec![1u64, 2u64, 3u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
    // Use AsyncStable to confirm that NO storage write is issued for a pre-vote response.
    let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 7, Noop);
    let mut log = crate::testkit::NoopLog;
    let mut stable = crate::testkit::AsyncStable::default();

    // Node 2 is at term=0, no known leader, election timer just expired (now=ORIGIN).
    // Receive a pre-vote request at term+5 = 5 from node 1.
    // Log is empty (NoopLog) → log_ok passes (last_log=(0,0) == candidate's).
    // Lease check: no leader known → lease open.
    // term_ok: rv.term()=5 > self.term=0 → passes.
    // All conditions pass → grant.
    ep.handle_message(
      Instant::ORIGIN,
      &mut log,
      &mut stable,
      1u64,
      Message::RequestVote(RequestVote::new(
        Term::new(5), // advertised term (T+5)
        1u64,
        Index::ZERO,
        Term::ZERO,
        true, // pre_vote
        false,
      )),
    );

    // CRITICAL: term must NOT have been adopted.
    assert_eq!(
      ep.term(),
      Term::ZERO,
      "pre-vote request must NOT cause receiver to adopt the advertised term T+5"
    );
    // CRITICAL: voted_for must NOT have been set.
    assert!(ep.voted_for.is_none(), "pre-vote must NOT set voted_for");

    // Response must be IMMEDIATE (no persist needed) — it is already in the outgoing queue.
    let resp = ep
      .poll_message()
      .expect("response must be sent immediately, without waiting for storage");
    match resp.message() {
      Message::VoteResp(vr) => {
        assert!(vr.pre_vote(), "must be a pre-vote response");
        // Grant: log_ok + term_ok + lease_open all pass.
        assert!(!vr.reject(), "must grant (log ok, term ok, lease open)");
        // Reply term is the advertised term on grant.
        assert_eq!(
          vr.term(),
          Term::new(5),
          "grant reply must carry the advertised term rv.term()"
        );
      }
      other => panic!("expected VoteResp, got {other:?}"),
    }

    // No storage write must have been submitted (pre-vote grants no-persist invariant).
    // Drain all pending storage → if a write was submitted, AsyncStable would yield it.
    ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
    // No additional messages should appear (a CastVote would have produced a VoteResp here).
    assert!(
      ep.poll_message().is_none(),
      "no additional VoteResp after handle_storage — pre-vote must not persist"
    );
  }

  // ─── N1: stale-term pre-vote rejection (etcd PreVote fidelity) ───────────────────────────────

  /// Regression N1: a follower at term 5 with no voted_for and no live leader receives a
  /// pre-vote whose advertised term (3) is BELOW its own term.
  ///
  /// Expected (etcd semantics):
  /// - Reply: VoteResp{ pre_vote: true, reject: true, term: 5 } (granter's term in reject)
  /// - self.term stays 5
  /// - voted_for stays None
  ///
  /// No durable state is touched (pre-vote path).
  ///
  /// Before fix: the `voted_for.is_none()` disjunct in the old `term_ok` incorrectly
  /// GRANTED this stale pre-vote (reject: false). The fix adds `rv.term() >= self.term` as
  /// a required conjunct so a stale advertised term is rejected regardless of voted_for.
  #[test]
  fn stale_term_pre_vote_is_rejected() {
    use crate::{Config, Index, Instant, Message, RequestVote, Term};
    use core::time::Duration;

    // Node 2 is a follower at term 5 with no voted_for and no live leader.
    let cfg = Config::try_new(
      2u64,
      std::vec![1u64, 2u64, 3u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
    let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 7, Noop);
    let mut log = crate::testkit::NoopLog;
    let mut stable = crate::testkit::NoopStable::default();

    // Manually set term to 5 (no voted_for, no leader, election timer expired).
    ep.term = Term::new(5);

    // Negative case: stale pre-vote (advertised term 3 < our term 5), up-to-date log.
    // Must be rejected: rv.term() < self.term fails the term_ok >= check.
    ep.handle_message(
      Instant::ORIGIN, // election timer at ORIGIN, so deadline <= now → lease open
      &mut log,
      &mut stable,
      1u64,
      Message::RequestVote(RequestVote::new(
        Term::new(3), // stale advertised term
        1u64,
        Index::ZERO,
        Term::ZERO,
        true, // pre_vote
        false,
      )),
    );

    let resp = ep.poll_message().expect("must reply to stale pre-vote");
    match resp.message() {
      Message::VoteResp(vr) => {
        assert!(vr.pre_vote(), "response must be a pre-vote response");
        assert!(
          vr.reject(),
          "stale-term pre-vote (term 3 < our term 5) must be rejected (N1)"
        );
        assert_eq!(
          vr.term(),
          Term::new(5),
          "reject reply must carry self.term (5) so the pre-candidate learns it is behind"
        );
      }
      other => panic!("expected VoteResp, got {other:?}"),
    }
    // No state mutation: term and voted_for are unchanged.
    assert_eq!(
      ep.term(),
      Term::new(5),
      "self.term must remain 5 after stale pre-vote"
    );
    assert!(ep.voted_for.is_none(), "voted_for must remain None");

    // Positive case: pre-vote with advertised term 6 (> 5), up-to-date log, lease open.
    // Must be granted.
    ep.handle_message(
      Instant::ORIGIN,
      &mut log,
      &mut stable,
      1u64,
      Message::RequestVote(RequestVote::new(
        Term::new(6), // rv.term() > self.term → term_ok passes
        1u64,
        Index::ZERO,
        Term::ZERO,
        true, // pre_vote
        false,
      )),
    );

    let resp2 = ep.poll_message().expect("must reply to valid pre-vote");
    match resp2.message() {
      Message::VoteResp(vr) => {
        assert!(vr.pre_vote(), "response must be a pre-vote response");
        assert!(
          !vr.reject(),
          "pre-vote at term 6 > 5, up-to-date, lease open → must grant"
        );
        assert_eq!(
          vr.term(),
          Term::new(6),
          "grant reply must carry the advertised term (6)"
        );
      }
      other => panic!("expected VoteResp, got {other:?}"),
    }
    // Still no state mutation after grant either.
    assert_eq!(
      ep.term(),
      Term::new(5),
      "self.term must remain 5 after granted pre-vote"
    );
    assert!(
      ep.voted_for.is_none(),
      "voted_for must remain None after granted pre-vote"
    );
  }

  // ─── CheckQuorum (M7-U3) tests ────────────────────────────────────────────────────────────────

  /// Helper: build a Config with check_quorum=true for a cluster of `voters` with 1s/100ms.
  fn cq_config(id: u64, voters: std::vec::Vec<u64>) -> crate::Config<u64> {
    crate::Config::try_new(
      id,
      voters,
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap()
    .with_check_quorum(true)
  }

  /// Test CQ-1: A leader isolated from a quorum steps down when the CheckQuorum deadline fires.
  ///
  /// Setup: leader of a 3-node cluster. No `recent_active` peers (neither peer 2 nor peer 3
  /// has sent any messages). At the CheckQuorum deadline, `quorum_active` is false → step down
  /// to Follower (same term, leader=None).
  ///
  /// Conversely: with a quorum active (peer 2 marked), the leader stays and resets the window.
  #[test]
  fn check_quorum_isolated_leader_steps_down() {
    let cfg = cq_config(1, std::vec![1u64, 2, 3]);
    let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, Noop);
    let mut log = crate::testkit::NoopLog;
    let mut stable = crate::testkit::NoopStable::default();

    // Become leader via the normal election path.
    let d = ep.poll_timeout().unwrap();
    ep.handle_timeout(d, &mut log, &mut stable); // → Candidate
    ep.handle_message(
      d,
      &mut log,
      &mut stable,
      2u64,
      Message::VoteResp(crate::VoteResp::new(
        crate::Term::new(1),
        2u64,
        false,
        false,
      )),
    );
    assert!(
      ep.role().is_leader(),
      "should be leader after winning election"
    );
    let leader_term = ep.term();

    // Drain all outbound messages (heartbeats, AppendEntries).
    while ep.poll_message().is_some() {}
    while ep.poll_event().is_some() {}

    // The CheckQuorum election_deadline was armed in become_leader.
    // It should be Some (check_quorum is true).
    let cq_deadline = ep
      .election_deadline
      .expect("CQ election_deadline must be armed");

    // No messages received from peers → recent_active is false for peers 2 and 3.
    // Fire the CheckQuorum tick.
    ep.handle_timeout(cq_deadline, &mut log, &mut stable);

    // CRITICAL: step down at the SAME term (no term bump).
    assert!(
      ep.role().is_follower(),
      "isolated leader must step down to Follower"
    );
    assert_eq!(
      ep.term(),
      leader_term,
      "step-down must be same-term (no bump)"
    );
    assert!(
      ep.leader().is_none(),
      "leader field must be None after step-down"
    );
    // heartbeat_deadline must be cleared; election timer must be armed (for eventual re-campaign).
    assert!(
      ep.heartbeat_deadline.is_none(),
      "heartbeat_deadline must be cleared after step-down"
    );
    assert!(
      ep.election_deadline.is_some(),
      "election timer must be armed after step-down"
    );
  }

  /// Test CQ-2: With a quorum active, the leader stays and resets the window.
  #[test]
  fn check_quorum_active_quorum_stays_leader() {
    let cfg = cq_config(1, std::vec![1u64, 2, 3]);
    let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, Noop);
    let mut log = crate::testkit::NoopLog;
    let mut stable = crate::testkit::NoopStable::default();

    // Become leader.
    let d = ep.poll_timeout().unwrap();
    ep.handle_timeout(d, &mut log, &mut stable);
    ep.handle_message(
      d,
      &mut log,
      &mut stable,
      2u64,
      Message::VoteResp(crate::VoteResp::new(
        crate::Term::new(1),
        2u64,
        false,
        false,
      )),
    );
    assert!(ep.role().is_leader());
    while ep.poll_message().is_some() {}
    while ep.poll_event().is_some() {}

    let cq_deadline = ep
      .election_deadline
      .expect("CQ election_deadline must be armed");

    // Simulate a HeartbeatResp from peer 2 (marks peer 2 active). Use a time before the
    // CheckQuorum deadline (base + election_timeout / 2 is safely before cq_deadline).
    let before_cq = Instant::ORIGIN + Duration::from_millis(1);
    ep.handle_message(
      before_cq,
      &mut log,
      &mut stable,
      2u64,
      Message::HeartbeatResp(crate::HeartbeatResp::new(
        crate::Term::new(1),
        2u64,
        bytes::Bytes::new(),
      )),
    );
    while ep.poll_message().is_some() {}

    // Peer 2 active + self active = 2 of 3 = quorum. Fire CheckQuorum tick.
    ep.handle_timeout(cq_deadline, &mut log, &mut stable);

    // Must remain leader.
    assert!(
      ep.role().is_leader(),
      "leader with active quorum must remain leader"
    );
    // The CheckQuorum window must have been reset (election_deadline re-armed for next window).
    let new_cq_deadline = ep.election_deadline.expect("CQ deadline must be re-armed");
    assert!(
      new_cq_deadline > cq_deadline,
      "re-armed CQ deadline must be in the future"
    );
    // After the reset, peers should be inactive again (except self).
    assert!(
      ep.tracker
        .progress(&2u64)
        .map(|p| !p.recent_active())
        .unwrap_or(false),
      "peer 2 recent_active must be reset to false"
    );
    assert!(
      ep.tracker
        .progress(&1u64)
        .map(|p| p.recent_active())
        .unwrap_or(false),
      "self recent_active must remain true"
    );
  }

  /// Test CQ-3: `recent_active` is set when the leader receives a message from a peer.
  ///
  /// A leader receiving an AppendResp/HeartbeatResp from a peer marks that peer active.
  #[test]
  fn check_quorum_recent_active_set_on_inbound_message() {
    let cfg = cq_config(1, std::vec![1u64, 2, 3]);
    let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, Noop);
    let mut log = crate::testkit::VecLog::default();
    let mut stable = crate::testkit::NoopStable::default();

    // Become leader.
    let d = ep.poll_timeout().unwrap();
    ep.handle_timeout(d, &mut log, &mut stable);
    ep.handle_message(
      d,
      &mut log,
      &mut stable,
      2u64,
      Message::VoteResp(crate::VoteResp::new(
        crate::Term::new(1),
        2u64,
        false,
        false,
      )),
    );
    assert!(ep.role().is_leader());
    // Drain storage (noop write for leader).
    ep.handle_storage(d, &mut log, &mut stable);
    while ep.poll_message().is_some() {}

    // Initially peer 2 is NOT active.
    assert!(
      !ep
        .tracker
        .progress(&2u64)
        .map(|p| p.recent_active())
        .unwrap_or(true),
      "peer 2 must start inactive"
    );

    // Receive a HeartbeatResp from peer 2.
    ep.handle_message(
      d,
      &mut log,
      &mut stable,
      2u64,
      Message::HeartbeatResp(crate::HeartbeatResp::new(
        crate::Term::new(1),
        2u64,
        bytes::Bytes::new(),
      )),
    );

    // Peer 2 must now be active.
    assert!(
      ep.tracker
        .progress(&2u64)
        .map(|p| p.recent_active())
        .unwrap_or(false),
      "peer 2 must be marked active after HeartbeatResp"
    );
  }

  /// Test CQ-4: Follower lease ignores a disruptive vote request.
  ///
  /// A follower with check_quorum=true, a live leader, and a healthy election timer (deadline
  /// in the future) receives `RequestVote{term: self.term+2, leader_transfer: false}` → it
  /// does NOT adopt the term, does NOT grant, term unchanged.
  ///
  /// With `leader_transfer=true` (forced) → it IS NOT ignored (proceeds normally: adopts
  /// the higher term and steps down, would eventually vote or reject based on log).
  #[test]
  fn check_quorum_follower_lease_blocks_disruptive_vote() {
    use crate::{Config, Index, Instant, Message, RequestVote, Term};
    use core::time::Duration;

    let cfg = Config::try_new(
      2u64,
      std::vec![1u64, 2u64, 3u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap()
    .with_check_quorum(true);

    // "now" is well within the election timer window so deadline > now.
    let base = Instant::ORIGIN;
    let mut ep = Endpoint::new(cfg, base, 7, Noop);
    let mut log = crate::testkit::NoopLog;
    let mut stable = crate::testkit::NoopStable::default();

    // The follower must believe it has a live leader. Receive a Heartbeat from leader 1
    // to set leader=Some(1) and arm the election timer.
    ep.handle_message(
      base,
      &mut log,
      &mut stable,
      1u64,
      Message::Heartbeat(crate::Heartbeat::new(
        Term::new(1),
        1u64,
        Index::ZERO,
        bytes::Bytes::new(),
      )),
    );
    // Drain the HeartbeatResp.
    while ep.poll_message().is_some() {}
    while ep.poll_event().is_some() {}

    assert_eq!(ep.term(), Term::new(1));
    assert_eq!(ep.leader(), Some(1u64));
    // election_deadline must be in the future (healthy lease).
    let deadline = ep.election_deadline.expect("election timer must be armed");
    assert!(deadline > base, "election deadline must be in the future");

    // --- Case A: non-forced RequestVote at higher term while lease is active ---
    // Simulate a small time advance that is still within the lease window.
    let now_in_lease = base + Duration::from_millis(50); // well before deadline
    ep.handle_message(
      now_in_lease,
      &mut log,
      &mut stable,
      3u64,
      Message::RequestVote(RequestVote::new(
        Term::new(3), // term+2
        3u64,
        Index::ZERO,
        Term::ZERO,
        false, // real vote, NOT pre_vote
        false, // NOT leader_transfer
      )),
    );

    // CRITICAL: term must NOT be adopted (lease blocked the message before the step-down).
    assert_eq!(
      ep.term(),
      Term::new(1),
      "follower lease must block term adoption from disruptive vote"
    );
    // No response sent (we returned early).
    assert!(
      ep.poll_message().is_none(),
      "no reply must be sent while lease blocks disruptive vote"
    );

    // --- Case B: forced (leader_transfer) RequestVote at higher term ---
    // leader_transfer bypasses the lease; this IS processed normally.
    ep.handle_message(
      now_in_lease,
      &mut log,
      &mut stable,
      3u64,
      Message::RequestVote(RequestVote::new(
        Term::new(5), // higher term
        3u64,
        Index::ZERO,
        Term::ZERO,
        false, // real vote
        true,  // leader_transfer → bypass lease
      )),
    );

    // The forced campaign bypasses the lease: the term IS adopted.
    assert_eq!(
      ep.term(),
      Term::new(5),
      "forced leader_transfer vote must bypass lease and adopt the higher term"
    );
  }

  /// Test CQ-5: `check_quorum=false` default → no CheckQuorum tick, no lease ignore.
  ///
  /// With the default config (check_quorum=false):
  /// - A leader's election_deadline is NOT armed (no CheckQuorum window).
  /// - A follower does NOT block a higher-term vote request (no lease protection).
  #[test]
  fn check_quorum_disabled_preserves_m1_m6_behavior() {
    use crate::{Config, Index, Instant, Message, RequestVote, Term};
    use core::time::Duration;

    // --- Part 1: Leader has no CQ election_deadline when check_quorum=false ---
    let cfg_leader = Config::try_new(
      1u64,
      std::vec![1u64, 2u64, 3u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
    // check_quorum defaults to false
    let mut ep = Endpoint::new(cfg_leader, Instant::ORIGIN, 1, Noop);
    let mut log = crate::testkit::NoopLog;
    let mut stable = crate::testkit::NoopStable::default();
    let d = ep.poll_timeout().unwrap();
    ep.handle_timeout(d, &mut log, &mut stable);
    ep.handle_message(
      d,
      &mut log,
      &mut stable,
      2u64,
      Message::VoteResp(crate::VoteResp::new(Term::new(1), 2u64, false, false)),
    );
    assert!(ep.role().is_leader(), "should be leader");
    // With check_quorum=false, election_deadline must NOT be armed (arm_heartbeat_timer clears it).
    assert!(
      ep.election_deadline.is_none(),
      "check_quorum=false: election_deadline must not be armed for leader"
    );

    // --- Part 2: Follower with no check_quorum does NOT block higher-term vote ---
    let cfg_follower = Config::try_new(
      2u64,
      std::vec![1u64, 2u64, 3u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
    // check_quorum=false AND pre_vote=false
    let base = Instant::ORIGIN;
    let mut ep2 = Endpoint::new(cfg_follower, base, 7, Noop);
    let mut log2 = crate::testkit::NoopLog;
    let mut stable2 = crate::testkit::NoopStable::default();

    // Give the follower a live leader via Heartbeat.
    ep2.handle_message(
      base,
      &mut log2,
      &mut stable2,
      1u64,
      Message::Heartbeat(crate::Heartbeat::new(
        Term::new(1),
        1u64,
        Index::ZERO,
        bytes::Bytes::new(),
      )),
    );
    while ep2.poll_message().is_some() {}
    while ep2.poll_event().is_some() {}
    assert_eq!(ep2.term(), Term::new(1));
    assert_eq!(ep2.leader(), Some(1u64));

    // A higher-term real vote (non-forced) arrives while the lease *would* apply — but
    // check_quorum=false AND pre_vote=false → lease is NOT active → term IS adopted.
    let now_in_lease = base + Duration::from_millis(50);
    ep2.handle_message(
      now_in_lease,
      &mut log2,
      &mut stable2,
      3u64,
      Message::RequestVote(RequestVote::new(
        Term::new(3),
        3u64,
        Index::ZERO,
        Term::ZERO,
        false, // real vote
        false, // not forced
      )),
    );
    // Without check_quorum or pre_vote, the lease block is inactive → term IS adopted.
    assert_eq!(
      ep2.term(),
      Term::new(3),
      "check_quorum=false: higher-term vote must be processed normally (no lease block)"
    );
  }

  // ── M7-U4: ReadIndex tests ─────────────────────────────────────────────────────────────────────

  /// Helper: elect node 1 leader in a 3-voter cluster, drain the no-op so the leader has
  /// a committed current-term entry.  Returns (ep, log, stable, now).
  fn make_leader_with_current_term_commit() -> (
    Endpoint<u64, crate::testkit::CountSm>,
    crate::testkit::VecLog,
    crate::testkit::NoopStable,
    crate::Instant,
  ) {
    use core::time::Duration;
    let cfg = crate::Config::try_new(
      1u64,
      std::vec![1u64, 2u64, 3u64],
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
      Message::VoteResp(crate::VoteResp::new(
        crate::Term::new(1),
        2u64,
        false,
        false,
      )),
    );
    assert!(ep.role().is_leader());
    // Drain storage: no-op LeaderAppend fires → self match_index advances.
    ep.handle_storage(d, &mut log, &mut stable);
    // Peer 2 acks the no-op → quorum (self + peer2) → commit advances to 1.
    ep.handle_message(
      d,
      &mut log,
      &mut stable,
      2u64,
      Message::AppendResp(crate::AppendResp::new(
        crate::Term::new(1),
        2u64,
        false,
        crate::Index::ZERO,
        crate::Term::ZERO,
        crate::Index::new(1),
      )),
    );
    while ep.poll_event().is_some() {}
    while ep.poll_message().is_some() {}
    (ep, log, stable, d)
  }

  /// Test 1: Safe read confirmed only after a heartbeat quorum.
  ///
  /// A 3-node leader (with a current-term commit) calls `read_index(ctx)` →
  /// broadcasts heartbeats with ctx; NO `ReadState` until a quorum of `HeartbeatResp`
  /// arrive; after the quorum, exactly one `Event::ReadState` is emitted.
  #[test]
  fn safe_read_confirmed_after_heartbeat_quorum() {
    let (mut ep, mut log, mut stable, d) = make_leader_with_current_term_commit();
    let ctx = bytes::Bytes::from_static(b"read_1");

    ep.read_index(d, &log, &stable, ctx.clone())
      .expect("leader with a current-term commit must accept the read");

    // The leader should have broadcast heartbeats carrying ctx.
    let mut ctx_hb_count = 0usize;
    while let Some(out) = ep.poll_message() {
      if let Message::Heartbeat(hb) = out.message() {
        if hb.context() == ctx.as_ref() {
          ctx_hb_count += 1;
        }
      }
    }
    assert_eq!(
      ctx_hb_count, 2,
      "leader must broadcast 2 heartbeats with ctx (to peers 2 and 3)"
    );

    // No ReadState yet (need quorum = 2/3 voters, leader already counted itself = 1).
    assert!(
      ep.poll_event().is_none(),
      "ReadState must not be emitted before a quorum of heartbeat acks"
    );

    // One HeartbeatResp with ctx from peer 2 → quorum reached (self + peer2 = 2/3).
    ep.handle_message(
      d,
      &mut log,
      &mut stable,
      2u64,
      Message::HeartbeatResp(crate::HeartbeatResp::new(
        crate::Term::new(1),
        2u64,
        ctx.clone(),
      )),
    );
    while ep.poll_message().is_some() {}

    // ReadState must be emitted now.
    let events: std::vec::Vec<_> = core::iter::from_fn(|| ep.poll_event()).collect();
    assert_eq!(
      events.len(),
      1,
      "exactly one ReadState must be emitted after quorum; got: {:?}",
      events
    );
    let rs = match &events[0] {
      crate::Event::ReadState(rs) => rs.clone(),
      other => panic!("expected ReadState, got {:?}", other),
    };
    assert_eq!(rs.context().as_ref(), ctx.as_ref(), "context must match");
    assert_eq!(
      rs.index(),
      crate::Index::new(1),
      "index must be the commit at receipt"
    );
  }

  /// Test 2: Stale leader (partitioned from quorum) cannot confirm a read.
  ///
  /// The leader calls `read_index` but only gets heartbeat acks from itself (no quorum)
  /// → no `ReadState` is emitted.
  #[test]
  fn stale_leader_cannot_confirm_read() {
    let (mut ep, log, stable, d) = make_leader_with_current_term_commit();
    let ctx = bytes::Bytes::from_static(b"stale_read");

    ep.read_index(d, &log, &stable, ctx.clone())
      .expect("leader must accept the read (it just cannot confirm without a quorum)");
    while ep.poll_message().is_some() {}
    // No heartbeat acks arrive (partitioned).
    // No ReadState must be emitted.
    let events: std::vec::Vec<_> = core::iter::from_fn(|| ep.poll_event()).collect();
    assert!(
      events.is_empty(),
      "stale/partitioned leader must not emit ReadState without a heartbeat quorum"
    );
  }

  /// Test 3: LeaseBased confirms immediately.
  ///
  /// With `read_only=LeaseBased` + `check_quorum=true`, `read_index` emits ReadState
  /// from `commit` without waiting for heartbeats.
  #[test]
  fn lease_based_confirms_immediately() {
    use core::time::Duration;
    let cfg = crate::Config::try_new(
      1u64,
      std::vec![1u64, 2u64, 3u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap()
    .with_read_only(crate::ReadOnlyOption::LeaseBased)
    .with_check_quorum(true);

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
      Message::VoteResp(crate::VoteResp::new(
        crate::Term::new(1),
        2u64,
        false,
        false,
      )),
    );
    assert!(ep.role().is_leader());
    ep.handle_storage(d, &mut log, &mut stable);
    ep.handle_message(
      d,
      &mut log,
      &mut stable,
      2u64,
      Message::AppendResp(crate::AppendResp::new(
        crate::Term::new(1),
        2u64,
        false,
        crate::Index::ZERO,
        crate::Term::ZERO,
        crate::Index::new(1),
      )),
    );
    while ep.poll_event().is_some() {}
    while ep.poll_message().is_some() {}

    let ctx = bytes::Bytes::from_static(b"lease_read");
    ep.read_index(d, &log, &stable, ctx.clone())
      .expect("LeaseBased + check_quorum leader must accept the read");

    // No heartbeats should have been sent for the read round.
    let mut hb_with_ctx = false;
    while let Some(out) = ep.poll_message() {
      if let Message::Heartbeat(hb) = out.message() {
        if hb.context() == ctx.as_ref() {
          hb_with_ctx = true;
        }
      }
    }
    assert!(
      !hb_with_ctx,
      "LeaseBased must NOT broadcast read-heartbeats"
    );

    // ReadState must be emitted immediately.
    let ev = ep
      .poll_event()
      .expect("LeaseBased must emit ReadState immediately");
    assert!(ev.is_read_state(), "expected ReadState event");
    let rs = ev.unwrap_read_state_ref();
    assert_eq!(rs.index(), crate::Index::new(1));
    assert_eq!(rs.context().as_ref(), ctx.as_ref());
  }

  /// Test: LeaseBased without check_quorum degrades to Safe (all build profiles).
  ///
  /// A leader configured `read_only=LeaseBased` but `check_quorum=false` must
  /// NOT confirm the read immediately.  It must behave like Safe: broadcast a
  /// heartbeat round and wait for a quorum of acks before emitting ReadState.
  /// Construction is infallible and behaves identically in debug and release — the
  /// combination is handled by degradation, not rejection.
  #[test]
  fn lease_based_without_check_quorum_degrades_to_safe() {
    use core::time::Duration;

    // Build a leader with LeaseBased but check_quorum=false (the unsafe combination).
    let cfg = crate::Config::try_new(
      1u64,
      std::vec![1u64, 2u64, 3u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap()
    .with_read_only(crate::ReadOnlyOption::LeaseBased)
    .with_check_quorum(false);

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
      Message::VoteResp(crate::VoteResp::new(
        crate::Term::new(1),
        2u64,
        false,
        false,
      )),
    );
    assert!(ep.role().is_leader());
    ep.handle_storage(d, &mut log, &mut stable);
    ep.handle_message(
      d,
      &mut log,
      &mut stable,
      2u64,
      Message::AppendResp(crate::AppendResp::new(
        crate::Term::new(1),
        2u64,
        false,
        crate::Index::ZERO,
        crate::Term::ZERO,
        crate::Index::new(1),
      )),
    );
    while ep.poll_event().is_some() {}
    while ep.poll_message().is_some() {}

    let ctx = bytes::Bytes::from_static(b"degraded_lease_read");
    ep.read_index(d, &log, &stable, ctx.clone())
      .expect("leader must accept the read (degraded LeaseBased → Safe)");

    // Must NOT emit ReadState immediately (would be linearizability hazard).
    assert!(
      ep.poll_event().is_none(),
      "LeaseBased without check_quorum must NOT confirm immediately — no ReadState yet"
    );

    // Must have broadcast a heartbeat with ctx (Safe path).
    let mut hb_with_ctx = false;
    while let Some(out) = ep.poll_message() {
      if let Message::Heartbeat(hb) = out.message() {
        if hb.context() == ctx.as_ref() {
          hb_with_ctx = true;
        }
      }
    }
    assert!(
      hb_with_ctx,
      "LeaseBased without check_quorum must fall back to Safe and broadcast a heartbeat round"
    );

    // After a quorum of HeartbeatResp acks, ReadState is emitted.
    ep.handle_message(
      d,
      &mut log,
      &mut stable,
      2u64,
      Message::HeartbeatResp(crate::HeartbeatResp::new(
        crate::Term::new(1),
        2u64,
        ctx.clone(),
      )),
    );
    while ep.poll_message().is_some() {}

    let ev = ep
      .poll_event()
      .expect("ReadState must be emitted once heartbeat quorum acks");
    assert!(ev.is_read_state(), "expected ReadState");
    let rs = ev.unwrap_read_state_ref();
    assert_eq!(rs.index(), crate::Index::new(1));
    assert_eq!(rs.context().as_ref(), ctx.as_ref());
  }

  /// Test 4: Follower-forwarded read.
  ///
  /// A follower calls `read_index(ctx)` → sends `ReadIndex` to the leader.
  /// The leader confirms (heartbeat quorum) and replies `ReadIndexResp`.
  /// The follower emits `Event::ReadState`.
  #[test]
  fn follower_forwarded_read() {
    use core::time::Duration;

    // Set up a follower (node 2) pointing to leader 1.
    let follower_cfg = crate::Config::try_new(
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

    // Give the follower a heartbeat so it knows about leader 1.
    follower.handle_message(
      Instant::ORIGIN,
      &mut follower_log,
      &mut follower_stable,
      1u64,
      Message::Heartbeat(crate::Heartbeat::new(
        crate::Term::new(1),
        1u64,
        crate::Index::ZERO,
        bytes::Bytes::new(),
      )),
    );
    while follower.poll_message().is_some() {}
    while follower.poll_event().is_some() {}

    // Follower calls read_index: should forward ReadIndex to leader 1.
    let ctx = bytes::Bytes::from_static(b"fwd_read");
    follower
      .read_index(
        Instant::ORIGIN,
        &follower_log,
        &follower_stable,
        ctx.clone(),
      )
      .expect("follower with a known leader must forward the read");

    let msg = follower
      .poll_message()
      .expect("follower must send ReadIndex to leader");
    assert_eq!(msg.to(), 1u64);
    assert!(msg.message().is_read_index(), "message must be ReadIndex");

    // Now simulate the leader confirming and replying with ReadIndexResp.
    follower.handle_message(
      Instant::ORIGIN,
      &mut follower_log,
      &mut follower_stable,
      1u64,
      Message::ReadIndexResp(crate::ReadIndexResp::new(
        crate::Term::new(1),
        1u64,
        crate::Index::new(5),
        ctx.clone(),
      )),
    );

    // Follower must emit ReadState.
    let ev = follower
      .poll_event()
      .expect("follower must emit ReadState on ReadIndexResp");
    assert!(ev.is_read_state());
    let rs = ev.unwrap_read_state_ref();
    assert_eq!(rs.index(), crate::Index::new(5));
    assert_eq!(rs.context().as_ref(), ctx.as_ref());
  }

  /// Test 5: FIFO confirmation + index correctness.
  ///
  /// Two reads in order confirm in order; each ReadState.index is the commit recorded
  /// at that read's receipt (never less than a prior read's index).
  #[test]
  fn fifo_confirmation_and_index_correctness() {
    let (mut ep, mut log, mut stable, d) = make_leader_with_current_term_commit();

    let ctx_a = bytes::Bytes::from_static(b"read_a");
    let ctx_b = bytes::Bytes::from_static(b"read_b");

    // Both reads are at commit=1 (nothing new committed between them).
    ep.read_index(d, &log, &stable, ctx_a.clone())
      .expect("first read (ctx_a) must be accepted");
    ep.read_index(d, &log, &stable, ctx_b.clone())
      .expect("second read (ctx_b, distinct context) must be accepted");
    while ep.poll_message().is_some() {}

    // Peer 2 acks with ctx_b (the last pending context from broadcast_heartbeat).
    // This should advance through both ctx_a and ctx_b (FIFO).
    ep.handle_message(
      d,
      &mut log,
      &mut stable,
      2u64,
      Message::HeartbeatResp(crate::HeartbeatResp::new(
        crate::Term::new(1),
        2u64,
        ctx_b.clone(),
      )),
    );
    while ep.poll_message().is_some() {}

    // Both reads should now be confirmed.
    let events: std::vec::Vec<_> = core::iter::from_fn(|| ep.poll_event()).collect();
    let read_states: std::vec::Vec<_> = events
      .iter()
      .filter_map(|e| {
        if let crate::Event::ReadState(rs) = e {
          Some(rs.clone())
        } else {
          None
        }
      })
      .collect();

    assert_eq!(
      read_states.len(),
      2,
      "both reads must be confirmed; got {} ReadStates",
      read_states.len()
    );
    // FIFO: ctx_a before ctx_b.
    assert_eq!(
      read_states[0].context().as_ref(),
      ctx_a.as_ref(),
      "first confirmed must be ctx_a"
    );
    assert_eq!(
      read_states[1].context().as_ref(),
      ctx_b.as_ref(),
      "second confirmed must be ctx_b"
    );
    // Index correctness: both are at commit=1.
    assert_eq!(read_states[0].index(), crate::Index::new(1));
    assert_eq!(read_states[1].index(), crate::Index::new(1));
  }

  /// Test 6: No-current-term-commit defers.
  ///
  /// A freshly-elected leader whose no-op hasn't committed yet defers a read until
  /// the no-op commits, then confirms it.
  #[test]
  fn no_current_term_commit_defers_read() {
    use core::time::Duration;
    let cfg = crate::Config::try_new(
      1u64,
      std::vec![1u64, 2u64, 3u64],
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
      Message::VoteResp(crate::VoteResp::new(
        crate::Term::new(1),
        2u64,
        false,
        false,
      )),
    );
    assert!(ep.role().is_leader());
    // Do NOT drain storage or advance commit yet.
    while ep.poll_message().is_some() {}
    while ep.poll_event().is_some() {}

    // read_index called before the no-op is committed → must be DEFERRED.
    let ctx = bytes::Bytes::from_static(b"deferred_read");
    ep.read_index(d, &log, &stable, ctx.clone())
      .expect("leader must accept the read (deferred until the no-op commits)");

    // No heartbeats with ctx should have been sent (deferred).
    let mut ctx_hb = false;
    while let Some(out) = ep.poll_message() {
      if let Message::Heartbeat(hb) = out.message() {
        if hb.context() == ctx.as_ref() {
          ctx_hb = true;
        }
      }
    }
    assert!(
      !ctx_hb,
      "deferred read must NOT broadcast a heartbeat round before no-op commits"
    );

    // No ReadState yet.
    assert!(
      ep.poll_event().is_none(),
      "deferred read must NOT emit ReadState before no-op commits"
    );

    // Now drain storage → no-op LeaderAppend fires → self match advances.
    ep.handle_storage(d, &mut log, &mut stable);
    while ep.poll_event().is_some() {} // drain LeaderChanged etc.

    // Peer 2 acks the no-op → commit=1 in current term → deferred read gets flushed.
    ep.handle_message(
      d,
      &mut log,
      &mut stable,
      2u64,
      Message::AppendResp(crate::AppendResp::new(
        crate::Term::new(1),
        2u64,
        false,
        crate::Index::ZERO,
        crate::Term::ZERO,
        crate::Index::new(1),
      )),
    );
    while ep.poll_event().is_some() {} // drain Applied for no-op (it's Empty, so none)

    // The deferred read should now have been flushed → leader broadcasts heartbeat with ctx.
    let mut ctx_hb_after = false;
    while let Some(out) = ep.poll_message() {
      if let Message::Heartbeat(hb) = out.message() {
        if hb.context() == ctx.as_ref() {
          ctx_hb_after = true;
        }
      }
    }
    assert!(
      ctx_hb_after,
      "deferred read must broadcast heartbeats after no-op commits"
    );

    // Peer 2 acks the heartbeat → quorum → ReadState emitted.
    ep.handle_message(
      d,
      &mut log,
      &mut stable,
      2u64,
      Message::HeartbeatResp(crate::HeartbeatResp::new(
        crate::Term::new(1),
        2u64,
        ctx.clone(),
      )),
    );
    while ep.poll_message().is_some() {}

    let events: std::vec::Vec<_> = core::iter::from_fn(|| ep.poll_event()).collect();
    let read_states: std::vec::Vec<_> = events
      .iter()
      .filter_map(|e| {
        if let crate::Event::ReadState(rs) = e {
          Some(rs.clone())
        } else {
          None
        }
      })
      .collect();
    assert_eq!(
      read_states.len(),
      1,
      "exactly one ReadState must be emitted after deferred read is confirmed"
    );
    assert_eq!(
      read_states[0].index(),
      crate::Index::new(1),
      "index must be commit at receipt"
    );
    assert_eq!(read_states[0].context().as_ref(), ctx.as_ref());
  }

  // ─── M7-U5: leader transfer tests ────────────────────────────────────────────

  /// Elect node 1 as leader and return (ep, log, stable) ready for transfer tests.
  /// The log has the no-op at index 1 committed; peer 2's match_index is caught up.
  fn setup_leader_with_peer2_caught_up() -> (
    Endpoint<u64, crate::testkit::CountSm>,
    crate::testkit::VecLog,
    crate::testkit::NoopStable,
  ) {
    use core::time::Duration;
    let cfg = crate::Config::try_new(
      1u64,
      std::vec![1u64, 2u64, 3u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
    let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
    let mut log = crate::testkit::VecLog::default();
    let mut stable = crate::testkit::NoopStable::default();

    // Elect node 1.
    let d = ep.poll_timeout().unwrap();
    ep.handle_timeout(d, &mut log, &mut stable);
    ep.handle_message(
      d,
      &mut log,
      &mut stable,
      2u64,
      Message::VoteResp(crate::VoteResp::new(
        crate::Term::new(1),
        2u64,
        false,
        false,
      )),
    );
    assert!(ep.role().is_leader());
    // Drain the no-op append from storage so self-match advances.
    ep.handle_storage(d, &mut log, &mut stable);
    // Peer 2 acks the no-op (index 1) → match_index=1.
    ep.handle_message(
      d,
      &mut log,
      &mut stable,
      2u64,
      Message::AppendResp(crate::AppendResp::new(
        crate::Term::new(1),
        2u64,
        false,
        crate::Index::ZERO,
        crate::Term::ZERO,
        crate::Index::new(1),
      )),
    );
    while ep.poll_message().is_some() {}
    while ep.poll_event().is_some() {}
    (ep, log, stable)
  }

  /// Test 1: transfer_leader to a caught-up follower sends TimeoutNow immediately.
  /// When peer 2 receives TimeoutNow it becomes a real Candidate (even with pre_vote=true)
  /// and broadcasts RequestVote{leader_transfer:true, pre_vote:false}.
  #[test]
  fn transfer_to_caught_up_follower_sends_timeout_now_immediately() {
    use core::time::Duration;
    let (mut leader, log, stable) = setup_leader_with_peer2_caught_up();
    // Peer 2 is caught up (match=1, last_index=1): transfer_leader should send TimeoutNow now.
    leader
      .transfer_leader(Instant::ORIGIN, &log, &stable, 2u64)
      .expect("transfer should succeed");

    // Exactly one TimeoutNow to peer 2 must be in the outgoing queue.
    let mut tn_count = 0;
    while let Some(out) = leader.poll_message() {
      if out.to() == 2u64 {
        if let Message::TimeoutNow(_) = out.message() {
          tn_count += 1;
        }
      }
    }
    assert_eq!(tn_count, 1, "exactly one TimeoutNow must be sent to peer 2");

    // Now simulate peer 2 receiving TimeoutNow (with pre_vote=true config, should still
    // do a REAL campaign bypassing PreVote).
    let cfg2 = crate::Config::try_new(
      2u64,
      std::vec![1u64, 2u64, 3u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap()
    .with_pre_vote(true);
    let mut follower = Endpoint::new(cfg2, Instant::ORIGIN, 7, crate::testkit::CountSm::default());
    let mut flog = crate::testkit::VecLog::default();
    let mut fstable = crate::testkit::NoopStable::default();

    // Deliver the TimeoutNow (term=1, from leader=1).
    follower.handle_message(
      Instant::ORIGIN,
      &mut flog,
      &mut fstable,
      1u64,
      Message::TimeoutNow(crate::TimeoutNow::new(crate::Term::new(1), 1u64)),
    );

    // Peer 2 must be a REAL Candidate (not PreCandidate) at term 2.
    assert!(
      follower.role().is_candidate(),
      "TimeoutNow must produce a real Candidate even when pre_vote=true"
    );
    assert_eq!(
      follower.term(),
      crate::Term::new(2),
      "candidate term must be bumped to 2"
    );

    // The RequestVote broadcasts must have pre_vote=false and leader_transfer=true.
    let mut rv_count = 0;
    while let Some(out) = follower.poll_message() {
      if let Message::RequestVote(rv) = out.message() {
        assert!(
          !rv.pre_vote(),
          "TimeoutNow-triggered campaign must be a REAL vote (pre_vote=false)"
        );
        assert!(
          rv.leader_transfer(),
          "TimeoutNow-triggered campaign must set leader_transfer=true"
        );
        rv_count += 1;
      }
    }
    assert!(rv_count > 0, "peer 2 must broadcast RequestVote messages");
  }

  /// Test 2: transfer_leader to a LAGGING follower does NOT send TimeoutNow yet.
  /// TimeoutNow is sent only when on_append_resp brings the target to last_index.
  #[test]
  fn transfer_to_lagging_follower_waits_for_catch_up() {
    use core::time::Duration;
    let cfg = crate::Config::try_new(
      1u64,
      std::vec![1u64, 2u64, 3u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
    let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, crate::testkit::CountSm::default());
    let mut log = crate::testkit::VecLog::default();
    let mut stable = crate::testkit::NoopStable::default();

    // Elect node 1.
    let d = ep.poll_timeout().unwrap();
    ep.handle_timeout(d, &mut log, &mut stable);
    ep.handle_message(
      d,
      &mut log,
      &mut stable,
      2u64,
      Message::VoteResp(crate::VoteResp::new(
        crate::Term::new(1),
        2u64,
        false,
        false,
      )),
    );
    assert!(ep.role().is_leader());
    // Drain storage (no-op append).
    ep.handle_storage(d, &mut log, &mut stable);
    while ep.poll_message().is_some() {}
    while ep.poll_event().is_some() {}

    // Propose a second entry (index 2) to create lag for peer 2.
    ep.propose(d, &mut log, &stable, &bytes::Bytes::from_static(b"x"))
      .unwrap();
    ep.handle_storage(d, &mut log, &mut stable);
    while ep.poll_message().is_some() {}
    // log.last_index() == 2, but peer 2 match_index == 0 (has NOT acked yet).

    // Initiate transfer to peer 2 (it is lagging).
    ep.transfer_leader(d, &log, &stable, 2u64)
      .expect("transfer should succeed");

    // Must NOT have sent a TimeoutNow yet.
    let mut tn_sent = false;
    while let Some(out) = ep.poll_message() {
      if out.to() == 2u64 {
        if let Message::TimeoutNow(_) = out.message() {
          tn_sent = true;
        }
      }
    }
    assert!(!tn_sent, "TimeoutNow must NOT be sent to a lagging peer");

    // Now simulate peer 2 catching up: ack at match_index=2 (last_index).
    ep.handle_message(
      d,
      &mut log,
      &mut stable,
      2u64,
      Message::AppendResp(crate::AppendResp::new(
        crate::Term::new(1),
        2u64,
        false,
        crate::Index::ZERO,
        crate::Term::ZERO,
        crate::Index::new(2), // caught up to last_index=2
      )),
    );

    // Now TimeoutNow MUST have been sent.
    let mut tn_after = false;
    while let Some(out) = ep.poll_message() {
      if out.to() == 2u64 {
        if let Message::TimeoutNow(_) = out.message() {
          tn_after = true;
        }
      }
    }
    assert!(
      tn_after,
      "TimeoutNow must be sent to peer 2 after it catches up"
    );
  }

  /// Test 3: proposals are refused during transfer and accepted again after abort.
  #[test]
  fn proposals_refused_during_transfer_allowed_after_abort() {
    use core::time::Duration;
    let (mut ep, mut log, mut stable) = setup_leader_with_peer2_caught_up();

    // Initiate transfer.
    ep.transfer_leader(Instant::ORIGIN, &log, &stable, 2u64)
      .unwrap();

    // Normal propose must be refused.
    let err = ep
      .propose(
        Instant::ORIGIN,
        &mut log,
        &stable,
        &bytes::Bytes::from_static(b"x"),
      )
      .unwrap_err();
    assert!(
      matches!(err, crate::ProposeError::LeaderTransferInProgress),
      "propose must fail with LeaderTransferInProgress during transfer"
    );

    // Conf-change propose must also be refused.
    let cc_err = ep
      .propose_conf_change(
        Instant::ORIGIN,
        &mut log,
        &stable,
        crate::ConfChange::new(crate::ConfChangeType::RemoveNode, 3u64, bytes::Bytes::new()),
      )
      .unwrap_err();
    assert!(
      matches!(cc_err, crate::ProposeError::LeaderTransferInProgress),
      "propose_conf_change must fail with LeaderTransferInProgress during transfer"
    );

    // Advance time past the transfer deadline.
    let deadline = Instant::ORIGIN + Duration::from_millis(1001); // > election_timeout (1000ms)
    ep.handle_timeout(deadline, &mut log, &mut stable);
    while ep.poll_message().is_some() {}

    // After abort, propose must succeed again.
    let ok = ep.propose(
      deadline,
      &mut log,
      &stable,
      &bytes::Bytes::from_static(b"after_abort"),
    );
    assert!(
      ok.is_ok(),
      "propose must succeed after transfer abort; got {ok:?}"
    );
  }

  /// Test 4: transfer aborts after election timeout with no completion.
  #[test]
  fn transfer_aborts_on_deadline() {
    use core::time::Duration;
    let (mut ep, mut log, mut stable) = setup_leader_with_peer2_caught_up();

    ep.transfer_leader(Instant::ORIGIN, &log, &stable, 2u64)
      .unwrap();
    // lead_transferee must be set.
    assert!(ep.lead_transferee.is_some());

    // Fire handle_timeout BEFORE the deadline → still in transfer.
    let before_deadline = Instant::ORIGIN + Duration::from_millis(500);
    ep.handle_timeout(before_deadline, &mut log, &mut stable);
    while ep.poll_message().is_some() {}
    assert!(
      ep.lead_transferee.is_some(),
      "transfer must still be active before deadline"
    );

    // Fire handle_timeout AFTER the deadline → transfer aborted.
    let after_deadline = Instant::ORIGIN + Duration::from_millis(1001);
    ep.handle_timeout(after_deadline, &mut log, &mut stable);
    while ep.poll_message().is_some() {}
    assert!(
      ep.lead_transferee.is_none(),
      "transfer must be aborted after deadline"
    );
    assert!(
      ep.transfer_deadline.is_none(),
      "transfer_deadline must be cleared after abort"
    );

    // Proposals must be accepted again.
    let ok = ep.propose(
      after_deadline,
      &mut log,
      &stable,
      &bytes::Bytes::from_static(b"resumed"),
    );
    assert!(ok.is_ok(), "propose must succeed after abort");
  }

  /// Test 5: TimeoutNow bypasses PreVote + lease (check_quorum=true, pre_vote=true).
  /// The recipient becomes a REAL Candidate (not PreCandidate), bumps its term, and sends
  /// RequestVote{leader_transfer:true}. A follower receiving that RequestVote grants it
  /// even though the election timer is still healthy (lease bypassed by leader_transfer flag).
  #[test]
  fn timeout_now_bypasses_prevote_and_lease() {
    use core::time::Duration;

    // Node 2 is the transfer target: pre_vote=true, check_quorum=true.
    let cfg2 = crate::Config::try_new(
      2u64,
      std::vec![1u64, 2u64, 3u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap()
    .with_pre_vote(true)
    .with_check_quorum(true);
    let mut target = Endpoint::new(cfg2, Instant::ORIGIN, 7, crate::testkit::CountSm::default());
    let mut tlog = crate::testkit::VecLog::default();
    let mut tstable = crate::testkit::NoopStable::default();

    // Arm a healthy election timer (simulating a live leader heartbeat was recently received).
    target.arm_election_timer(Instant::ORIGIN);

    // Set leader so lease check would normally block a vote.
    target.leader = Some(1u64);

    // Deliver TimeoutNow.
    target.handle_message(
      Instant::ORIGIN,
      &mut tlog,
      &mut tstable,
      1u64,
      Message::TimeoutNow(crate::TimeoutNow::new(crate::Term::new(1), 1u64)),
    );

    // Must be a REAL Candidate (not PreCandidate) despite pre_vote=true.
    assert!(
      target.role().is_candidate(),
      "TimeoutNow must produce Candidate, not PreCandidate"
    );
    assert_eq!(
      target.term(),
      crate::Term::new(2),
      "term must be bumped to 2"
    );

    // All RequestVote messages must have leader_transfer=true and pre_vote=false.
    let mut rv_count = 0;
    while let Some(out) = target.poll_message() {
      if let Message::RequestVote(rv) = out.message() {
        assert!(
          rv.leader_transfer(),
          "RequestVote from TimeoutNow must have leader_transfer=true"
        );
        assert!(
          !rv.pre_vote(),
          "RequestVote from TimeoutNow must have pre_vote=false"
        );
        rv_count += 1;
      }
    }
    assert!(rv_count > 0, "target must broadcast RequestVote messages");

    // Node 3 (a follower with a live leader and healthy election timer) receives the
    // RequestVote{leader_transfer:true}: the lease must NOT block it — it should grant.
    let cfg3 = crate::Config::try_new(
      3u64,
      std::vec![1u64, 2u64, 3u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap()
    .with_pre_vote(true)
    .with_check_quorum(true);
    let mut follower3 = Endpoint::new(
      cfg3,
      Instant::ORIGIN,
      42,
      crate::testkit::CountSm::default(),
    );
    let mut fl3 = crate::testkit::VecLog::default();
    let mut fs3 = crate::testkit::AsyncStable::default();

    // Give follower3 a live leader + healthy election timer (same-term as the RequestVote).
    // A real vote from term 2 would normally be blocked by the lease in on_handle_message
    // (RequestVote with term=2 > self.term=1 → term pre-pass would first update term to 2
    // and step down, then on_request_vote grants since voted_for is now None).
    // The CRITICAL test: leader_transfer=true in the higher-term path means the lease guard
    // in the term pre-pass is bypassed, so the request reaches on_request_vote normally.
    follower3.leader = Some(1u64);
    // Make the election timer healthy so the in-lease condition fires if we didn't force it.
    follower3.election_deadline = Some(Instant::ORIGIN + Duration::from_millis(500));

    follower3.handle_message(
      Instant::ORIGIN,
      &mut fl3,
      &mut fs3,
      2u64,
      Message::RequestVote(crate::RequestVote::new(
        crate::Term::new(2), // higher term
        2u64,
        crate::Index::ZERO,
        crate::Term::ZERO,
        false, // real vote
        true,  // leader_transfer — must bypass lease
      )),
    );
    // Drain storage (AsyncStable releases CastVote completion on handle_storage).
    follower3.handle_storage(Instant::ORIGIN, &mut fl3, &mut fs3);

    // follower3 must have granted the vote (not rejected it due to the lease).
    let mut granted = false;
    while let Some(out) = follower3.poll_message() {
      if let Message::VoteResp(vr) = out.message() {
        if !vr.reject() {
          granted = true;
        }
      }
    }
    assert!(
      granted,
      "follower3 must grant the leader-transfer RequestVote despite live leader + healthy timer"
    );
  }

  /// Test 6: transfer_leader to a learner/non-voter is rejected with NotAVoter.
  #[test]
  fn transfer_to_learner_rejected() {
    use core::time::Duration;
    // Create a cluster where node 4 is a learner (not a voter).
    let cfg = crate::Config::try_new(
      1u64,
      std::vec![1u64, 2u64, 3u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
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
      Message::VoteResp(crate::VoteResp::new(
        crate::Term::new(1),
        2u64,
        false,
        false,
      )),
    );
    assert!(ep.role().is_leader());

    // Node 4 is not in the voter set — transfer must fail with NotAVoter.
    let err = ep.transfer_leader(d, &log, &stable, 4u64).unwrap_err();
    assert!(
      matches!(err, crate::TransferError::NotAVoter),
      "transfer to non-voter must fail with NotAVoter; got {err:?}"
    );

    // Transferring to self must fail with AlreadyLeader.
    let err2 = ep.transfer_leader(d, &log, &stable, 1u64).unwrap_err();
    assert!(
      matches!(err2, crate::TransferError::AlreadyLeader),
      "transfer to self must fail with AlreadyLeader; got {err2:?}"
    );

    // Non-leader can't initiate transfer at all.
    let cfg_follower = crate::Config::try_new(
      2u64,
      std::vec![1u64, 2u64, 3u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
    let mut follower = Endpoint::new(
      cfg_follower,
      Instant::ORIGIN,
      5,
      crate::testkit::CountSm::default(),
    );
    let err3 = follower
      .transfer_leader(d, &log, &stable, 3u64)
      .unwrap_err();
    assert!(
      matches!(err3, crate::TransferError::NotLeader { .. }),
      "non-leader transfer_leader must fail with NotLeader; got {err3:?}"
    );
  }

  /// Test 7 (review I-1): Removing the transfer target via a conf change aborts the in-flight
  /// transfer immediately — proposals must resume without waiting for the deadline.
  ///
  /// Scenario: node 1 is leader of {1, 2, 3}; transfer to node 2 is in flight; then
  /// RemoveNode(2) is committed+applied. After apply:
  ///   - `lead_transferee` must be `None`
  ///   - `transfer_deadline` must be `None`
  ///   - a subsequent `propose` must SUCCEED (not `LeaderTransferInProgress`)
  #[test]
  fn transfer_aborted_when_transferee_removed_by_conf_change() {
    use crate::{AppendResp, ConfChange, ConfChangeType, Index, Message, ProposeError, Term};
    use core::time::Duration;

    let (mut ep, mut log, mut stable, d) = make_three_node_leader();
    // `d` is the Instant at which the election fired (the value returned by poll_timeout
    // before the election).  All time offsets are anchored to `d` so that the
    // transfer_deadline arithmetic (deadline = now + election_timeout = d + 1000ms) is
    // consistent regardless of what randomised value poll_timeout produced.

    // Start leader transfer to node 2 (caught-up: match=1, last=1 → TimeoutNow sent now).
    ep.transfer_leader(d, &log, &stable, 2u64)
      .expect("transfer_leader must succeed");
    assert!(
      ep.lead_transferee == Some(2u64),
      "lead_transferee must be Some(2) after transfer_leader"
    );
    // Drain the outgoing TimeoutNow.
    while ep.poll_message().is_some() {}

    // Proposals must be blocked while the transfer is in flight.
    let blocked = ep
      .propose(d, &mut log, &stable, &bytes::Bytes::from_static(b"blocked"))
      .unwrap_err();
    assert!(
      matches!(blocked, ProposeError::LeaderTransferInProgress),
      "propose must fail with LeaderTransferInProgress during transfer; got {blocked:?}"
    );

    // Strategy: abort the in-flight transfer via its deadline (so we can re-issue
    // propose_conf_change without the LeaderTransferInProgress guard firing), propose
    // RemoveNode(2), then re-start the transfer to node 2 (still a voter at that point),
    // and finally commit+apply the RemoveNode.  The fix must abort the re-started transfer
    // when the conf-change is applied, well before its own deadline.

    // Advance time past `d + election_timeout` to trigger the deadline abort.
    let past_first_deadline = d + Duration::from_millis(1001); // > election_timeout (1000ms)
    ep.handle_timeout(past_first_deadline, &mut log, &mut stable);
    while ep.poll_message().is_some() {}
    assert!(
      ep.lead_transferee.is_none(),
      "deadline abort must clear lead_transferee"
    );

    // Propose RemoveNode(2) (no transfer in flight — allowed).
    let cc = ConfChange::new(ConfChangeType::RemoveNode, 2u64, bytes::Bytes::new());
    let cc_idx = ep
      .propose_conf_change(past_first_deadline, &mut log, &stable, cc)
      .expect("propose_conf_change(RemoveNode(2)) must succeed");
    // Drain self-match (leader writes the ConfChange entry).
    ep.handle_storage(past_first_deadline, &mut log, &mut stable);
    while ep.poll_message().is_some() {}

    // Re-start a transfer to node 2 (still a voter until the conf change is applied).
    ep.transfer_leader(past_first_deadline, &log, &stable, 2u64)
      .expect("transfer_leader to node 2 (still a voter) must succeed");
    assert!(
      ep.lead_transferee == Some(2u64),
      "lead_transferee must be node 2 for the re-started transfer"
    );
    while ep.poll_message().is_some() {}

    // Proposals must be blocked again (new transfer in flight).
    let blocked2 = ep
      .propose(
        past_first_deadline,
        &mut log,
        &stable,
        &bytes::Bytes::from_static(b"blocked2"),
      )
      .unwrap_err();
    assert!(
      matches!(blocked2, ProposeError::LeaderTransferInProgress),
      "propose must be blocked by re-started transfer; got {blocked2:?}"
    );

    // Commit the RemoveNode(2): peer 3 acks up to cc_idx (quorum = leader + peer 3 = 2/3).
    // Leader self-match already happened via handle_storage above.
    ep.handle_message(
      past_first_deadline,
      &mut log,
      &mut stable,
      3u64,
      Message::AppendResp(AppendResp::new(
        Term::new(1),
        3u64,
        false,
        Index::ZERO,
        Term::ZERO,
        cc_idx,
      )),
    );
    while ep.poll_message().is_some() {}
    while ep.poll_event().is_some() {}

    // After the conf change applies: the transfer must have been aborted immediately.
    assert!(
      ep.lead_transferee.is_none(),
      "lead_transferee must be None after the transferee is removed by conf change (review I-1)"
    );
    assert!(
      ep.transfer_deadline.is_none(),
      "transfer_deadline must be None after transfer aborted on conf-change apply"
    );

    // Proposals must resume immediately — no need to wait for the transfer deadline.
    let ok = ep.propose(
      past_first_deadline,
      &mut log,
      &stable,
      &bytes::Bytes::from_static(b"resumed"),
    );
    assert!(
      ok.is_ok(),
      "propose must succeed immediately after transferee is removed; got {ok:?}"
    );
  }

  // ──────────────────────────────────────────────────────────────────────────────────────────
  // M7-U6: serviceable-timer filter (timer-wedge defense)
  // ──────────────────────────────────────────────────────────────────────────────────────────

  /// U6-T1: `serviceable_now` mirrors the `handle_timeout` dispatch exactly.
  ///
  /// - Follower: Heartbeat not serviceable; Election serviceable iff voter.
  /// - Leader (no CQ, no transfer): only Heartbeat serviceable.
  /// - Leader (CQ, no transfer): Heartbeat + Election serviceable.
  /// - Leader (CQ + transfer): Heartbeat + Election + Transfer serviceable.
  #[test]
  fn serviceable_now_mirrors_dispatch() {
    use core::time::Duration;

    // --- Follower (voter) ---
    let cfg = crate::Config::try_new(
      1u64,
      std::vec![1u64, 2u64, 3u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
    let ep = Endpoint::new(cfg, Instant::ORIGIN, 42, Noop);
    assert!(ep.role().is_follower());
    assert!(
      !ep.serviceable_now(TimerKind::Heartbeat),
      "follower: Heartbeat not serviceable"
    );
    assert!(
      ep.serviceable_now(TimerKind::Election),
      "follower voter: Election serviceable"
    );
    assert!(
      !ep.serviceable_now(TimerKind::Transfer),
      "follower: Transfer not serviceable"
    );

    // --- Follower (non-voter / observer) ---
    // Use try_new_observer: node 99 joins an existing cluster {1,2,3} as an observer.
    // Its id is not in the voter seed so is_voter(99) = false in its Tracker.
    let cfg_nv = crate::Config::try_new_observer(
      99u64,
      std::vec![1u64, 2u64, 3u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
    let ep_nv = Endpoint::new(cfg_nv, Instant::ORIGIN, 13, Noop);
    // Node 99 is not in the voter set {1,2,3} so is_voter(99) = false.
    assert!(ep_nv.role().is_follower());
    assert!(
      !ep_nv.serviceable_now(TimerKind::Election),
      "non-voter: Election NOT serviceable"
    );
    assert!(
      !ep_nv.serviceable_now(TimerKind::Heartbeat),
      "non-voter: Heartbeat not serviceable"
    );
    assert!(
      !ep_nv.serviceable_now(TimerKind::Transfer),
      "non-voter: Transfer not serviceable"
    );

    // --- Leader (no check_quorum, no transfer) ---
    let (ep_l, log_leader, stable_leader, _) = make_three_node_leader();
    assert!(ep_l.role().is_leader());
    assert!(!ep_l.config.check_quorum());
    assert!(ep_l.lead_transferee.is_none());
    assert!(
      ep_l.serviceable_now(TimerKind::Heartbeat),
      "leader: Heartbeat serviceable"
    );
    assert!(
      !ep_l.serviceable_now(TimerKind::Election),
      "leader (no CQ): Election NOT serviceable"
    );
    assert!(
      !ep_l.serviceable_now(TimerKind::Transfer),
      "leader (no transfer): Transfer not serviceable"
    );

    // --- Leader (check_quorum=true, no transfer) ---
    let cfg_cq = crate::Config::try_new(
      1u64,
      std::vec![1u64, 2u64, 3u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap()
    .with_check_quorum(true);
    let mut ep_cq = Endpoint::new(
      cfg_cq,
      Instant::ORIGIN,
      1,
      crate::testkit::CountSm::default(),
    );
    let mut log_cq = crate::testkit::VecLog::default();
    let mut stable_cq = crate::testkit::NoopStable::default();
    let d_cq = ep_cq.poll_timeout().unwrap();
    ep_cq.handle_timeout(d_cq, &mut log_cq, &mut stable_cq);
    ep_cq.handle_message(
      d_cq,
      &mut log_cq,
      &mut stable_cq,
      2u64,
      crate::Message::VoteResp(crate::VoteResp::new(
        crate::Term::new(1),
        2u64,
        false,
        false,
      )),
    );
    assert!(ep_cq.role().is_leader());
    assert!(ep_cq.config.check_quorum());
    assert!(
      ep_cq.serviceable_now(TimerKind::Heartbeat),
      "leader CQ: Heartbeat serviceable"
    );
    assert!(
      ep_cq.serviceable_now(TimerKind::Election),
      "leader CQ: Election serviceable (CheckQuorum tick)"
    );
    assert!(
      !ep_cq.serviceable_now(TimerKind::Transfer),
      "leader CQ (no transfer): Transfer not serviceable"
    );

    // --- Leader (check_quorum=true, transfer in progress) ---
    let ep_cq_log_ref = &log_cq;
    let ep_cq_stable_ref = &stable_cq;
    ep_cq
      .transfer_leader(d_cq, ep_cq_log_ref, ep_cq_stable_ref, 2u64)
      .expect("transfer_leader must succeed");
    assert!(ep_cq.lead_transferee.is_some());
    assert!(
      ep_cq.serviceable_now(TimerKind::Transfer),
      "leader CQ + transfer: Transfer serviceable"
    );
    let _ = (ep_l, log_leader, stable_leader);
  }

  /// U6-T2: `poll_timeout` never surfaces a non-serviceable deadline.
  ///
  /// - A Follower with a stale heartbeat_deadline set returns its election_deadline only.
  /// - A non-voter follower returns `None` even if election_deadline is armed.
  /// - A Leader without check_quorum returns only heartbeat (not election).
  /// - A Leader with check_quorum returns min(heartbeat, election).
  /// - A Leader with transfer returns min(heartbeat, election[if CQ], transfer).
  #[test]
  fn poll_timeout_only_surfaces_serviceable_deadlines() {
    use core::time::Duration;

    let election_timeout = Duration::from_millis(1000);
    let heartbeat_interval = Duration::from_millis(100);

    // --- Follower: stale heartbeat_deadline set, should NOT appear in poll_timeout ---
    let cfg = crate::Config::try_new(
      1u64,
      std::vec![1u64, 2u64, 3u64],
      election_timeout,
      heartbeat_interval,
    )
    .unwrap();
    let mut ep_f = Endpoint::new(cfg, Instant::ORIGIN, 42, Noop);
    // Defensively set a stale heartbeat_deadline (should not be serviceable for a follower).
    let stale_hb = Instant::ORIGIN + Duration::from_millis(50);
    ep_f.heartbeat_deadline = Some(stale_hb);
    let election_dl = ep_f.election_deadline.expect("election timer armed");
    let pt = ep_f
      .poll_timeout()
      .expect("poll_timeout must be Some for voter follower");
    assert_eq!(
      pt, election_dl,
      "follower poll_timeout must return election_deadline only"
    );
    assert_ne!(
      pt, stale_hb,
      "follower poll_timeout must NOT return heartbeat_deadline"
    );

    // --- Non-voter: election_deadline armed but not serviceable → poll_timeout returns None ---
    let cfg_nv = crate::Config::try_new_observer(
      99u64,
      std::vec![1u64, 2u64, 3u64], // 99 is not in the voter set
      election_timeout,
      heartbeat_interval,
    )
    .unwrap();
    let ep_nv = Endpoint::new(cfg_nv, Instant::ORIGIN, 7, Noop);
    assert!(
      ep_nv.election_deadline.is_some(),
      "election_deadline is armed on construction"
    );
    assert!(
      ep_nv.poll_timeout().is_none(),
      "non-voter poll_timeout must be None even with election_deadline armed"
    );

    // --- Leader (no CQ): poll_timeout returns heartbeat, NOT election ---
    let (ep_l, _log_l, _stable_l, _d_l) = make_three_node_leader();
    assert!(!ep_l.config.check_quorum());
    // The leader has no election_deadline (cleared on become_leader when CQ=false).
    assert!(ep_l.election_deadline.is_none());
    let hb_dl = ep_l.heartbeat_deadline.expect("heartbeat_deadline armed");
    let pt_l = ep_l
      .poll_timeout()
      .expect("leader poll_timeout must be Some");
    assert_eq!(
      pt_l, hb_dl,
      "leader (no CQ) poll_timeout must return heartbeat_deadline"
    );

    // --- Leader (CQ): poll_timeout returns min(heartbeat, election) ---
    let cfg_cq = crate::Config::try_new(
      1u64,
      std::vec![1u64, 2u64, 3u64],
      election_timeout,
      heartbeat_interval,
    )
    .unwrap()
    .with_check_quorum(true);
    let mut ep_cq = Endpoint::new(
      cfg_cq,
      Instant::ORIGIN,
      1,
      crate::testkit::CountSm::default(),
    );
    let mut log_cq = crate::testkit::VecLog::default();
    let mut stable_cq = crate::testkit::NoopStable::default();
    let d_cq = ep_cq.poll_timeout().unwrap();
    ep_cq.handle_timeout(d_cq, &mut log_cq, &mut stable_cq);
    ep_cq.handle_message(
      d_cq,
      &mut log_cq,
      &mut stable_cq,
      2u64,
      crate::Message::VoteResp(crate::VoteResp::new(
        crate::Term::new(1),
        2u64,
        false,
        false,
      )),
    );
    assert!(ep_cq.role().is_leader());
    let hb = ep_cq.heartbeat_deadline.expect("heartbeat armed");
    let el = ep_cq.election_deadline.expect("election (CQ) armed");
    let pt_cq = ep_cq
      .poll_timeout()
      .expect("CQ leader poll_timeout must be Some");
    assert_eq!(
      pt_cq,
      hb.min(el),
      "CQ leader poll_timeout must be min(hb, el)"
    );

    // --- Leader (CQ + transfer): poll_timeout includes transfer ---
    ep_cq
      .transfer_leader(d_cq, &log_cq, &stable_cq, 2u64)
      .expect("transfer_leader must succeed");
    let tr = ep_cq.transfer_deadline.expect("transfer_deadline armed");
    let pt_cq_tr = ep_cq
      .poll_timeout()
      .expect("CQ+transfer leader poll_timeout must be Some");
    assert_eq!(
      pt_cq_tr,
      hb.min(el).min(tr),
      "CQ+transfer leader poll_timeout must be min(hb, el, tr)"
    );
    let _ = ep_l;
  }

  /// U6-T3: `handle_timeout` → `poll_timeout` makes progress (no busy-wakeup wedge).
  ///
  /// For each role/state, arm the relevant deadline(s) to `now` (or just past it), call
  /// `handle_timeout(now)`, and assert that `poll_timeout` afterwards is either `None` or
  /// strictly `> now` — the serviced timer was re-armed to a future instant or cleared.
  #[test]
  fn handle_timeout_makes_progress_no_wedge() {
    use core::time::Duration;
    let now = Instant::ORIGIN + Duration::from_millis(5000);

    // --- Follower voter: election timer fires → campaign → election re-armed to future ---
    let cfg_f = crate::Config::try_new(
      1u64,
      std::vec![1u64, 2u64, 3u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
    let mut ep_f = Endpoint::new(cfg_f, now, 42, Noop);
    // Force the election deadline to exactly `now` (due).
    ep_f.election_deadline = Some(now);
    let mut log_f = crate::testkit::NoopLog;
    let mut stable_f = crate::testkit::NoopStable::default();
    ep_f.handle_timeout(now, &mut log_f, &mut stable_f);
    // After: either poll_timeout is None (single-node immediate leader) or > now.
    if let Some(next_dl) = ep_f.poll_timeout() {
      assert!(
        next_dl > now,
        "follower: poll_timeout after timeout must be > now, got {next_dl:?}"
      );
    }

    // --- Non-voter follower: election timer fires silently → poll_timeout becomes None ---
    let cfg_nv = crate::Config::try_new_observer(
      99u64,
      std::vec![1u64, 2u64, 3u64], // 99 is not in the voter set
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
    let mut ep_nv = Endpoint::new(cfg_nv, now, 7, Noop);
    ep_nv.election_deadline = Some(now);
    let mut log_nv = crate::testkit::NoopLog;
    let mut stable_nv = crate::testkit::NoopStable::default();
    ep_nv.handle_timeout(now, &mut log_nv, &mut stable_nv);
    assert!(
      ep_nv.poll_timeout().is_none(),
      "non-voter: poll_timeout must be None after silent expiry"
    );
    assert!(
      ep_nv.election_deadline.is_none(),
      "non-voter: election_deadline must be cleared after handle_timeout"
    );

    // --- Leader (no CQ): heartbeat fires → re-armed to future ---
    let (mut ep_l, mut log_leader, mut stable_leader, _) = make_three_node_leader();
    assert!(!ep_l.config.check_quorum());
    // Force heartbeat deadline to now.
    ep_l.heartbeat_deadline = Some(now);
    ep_l.handle_timeout(now, &mut log_leader, &mut stable_leader);
    while ep_l.poll_message().is_some() {}
    let pt_l = ep_l
      .poll_timeout()
      .expect("leader: poll_timeout must be Some after heartbeat fires");
    assert!(
      pt_l > now,
      "leader: poll_timeout after heartbeat must be > now, got {pt_l:?}"
    );

    // --- Leader (CQ): both heartbeat and election fire, both re-armed ---
    let cfg_cq = crate::Config::try_new(
      1u64,
      std::vec![1u64, 2u64, 3u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap()
    .with_check_quorum(true);
    let mut ep_cq = Endpoint::new(
      cfg_cq,
      Instant::ORIGIN,
      1,
      crate::testkit::CountSm::default(),
    );
    let mut log_cq = crate::testkit::VecLog::default();
    let mut stable_cq = crate::testkit::NoopStable::default();
    let d_cq = ep_cq.poll_timeout().unwrap();
    ep_cq.handle_timeout(d_cq, &mut log_cq, &mut stable_cq);
    ep_cq.handle_message(
      d_cq,
      &mut log_cq,
      &mut stable_cq,
      2u64,
      crate::Message::VoteResp(crate::VoteResp::new(
        crate::Term::new(1),
        2u64,
        false,
        false,
      )),
    );
    assert!(ep_cq.role().is_leader());
    // Force both timers to now.
    ep_cq.heartbeat_deadline = Some(now);
    ep_cq.election_deadline = Some(now);
    ep_cq.handle_timeout(now, &mut log_cq, &mut stable_cq);
    while ep_cq.poll_message().is_some() {}
    // After: either stepped down (quorum inactive) or both timers re-armed to future.
    if let Some(pt_cq) = ep_cq.poll_timeout() {
      assert!(
        pt_cq > now,
        "CQ leader: poll_timeout after timeout must be > now, got {pt_cq:?}"
      );
    }
    // No serviceable-and-due timer must remain (the debug_assert also guards this).
    for &k in &TimerKind::ALL {
      let still_due = ep_cq.serviceable_now(k) && ep_cq.deadline_of(k).is_some_and(|d| d <= now);
      assert!(
        !still_due,
        "CQ leader: timer {k} is still serviceable-and-due after handle_timeout"
      );
    }

    // --- Leader (transfer): transfer deadline fires → cleared ---
    let (mut ep_tr, mut log_tr, mut stable_tr, d_tr) = make_three_node_leader();
    ep_tr
      .transfer_leader(d_tr, &log_tr, &stable_tr, 2u64)
      .expect("transfer_leader must succeed");
    while ep_tr.poll_message().is_some() {}
    // Force transfer deadline to now.
    ep_tr.transfer_deadline = Some(now);
    ep_tr.heartbeat_deadline = Some(now + Duration::from_millis(100)); // not due
    ep_tr.handle_timeout(now, &mut log_tr, &mut stable_tr);
    while ep_tr.poll_message().is_some() {}
    assert!(
      ep_tr.lead_transferee.is_none(),
      "transfer abort: lead_transferee must be cleared"
    );
    assert!(
      ep_tr.transfer_deadline.is_none(),
      "transfer abort: transfer_deadline must be cleared"
    );
    assert!(
      !ep_tr.serviceable_now(TimerKind::Transfer),
      "transfer abort: Transfer no longer serviceable"
    );
  }

  // ── Review C2/I3: fatal apply_committed errors poison (no silent stall) + carry a cause ──────

  /// A state machine whose `apply` returns `Err` for a sentinel command. `Error` is a real
  /// `core::error::Error` (the §6.3 bound). Used to exercise the `PoisonReason::Apply` path.
  #[derive(Debug, Default)]
  struct FailSm;

  /// Apply failure for `FailSm`. Implements `core::error::Error` (available under both std and
  /// no_std) so it satisfies the `apply_committed` bound without pulling in `std` — keeps the
  /// test module compiling under `--no-default-features --features alloc`.
  #[derive(Debug)]
  struct FailSmError;

  impl core::fmt::Display for FailSmError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
      f.write_str("apply failed")
    }
  }

  impl core::error::Error for FailSmError {}

  impl crate::StateMachine for FailSm {
    type Command = Bytes;
    type Response = usize;
    type Snapshot = u64;
    type Error = FailSmError;

    fn apply(&mut self, _index: Index, cmd: Bytes) -> Result<usize, Self::Error> {
      // Sentinel: a single 0xFF byte means "fail". Any other payload applies successfully.
      if cmd.as_ref() == [0xFFu8] {
        return Err(FailSmError);
      }
      Ok(cmd.len())
    }

    fn snapshot(&self) -> Result<u64, Self::Error> {
      Ok(0)
    }

    fn restore(&mut self, _snapshot: u64) -> Result<(), Self::Error> {
      Ok(())
    }
  }

  /// Encode `payload` as a `Normal` entry's `data` using the `Bytes` codec (length-prefixed),
  /// so `<F::Command as Data>::decode` reads it back as the SM command.
  fn normal_entry(term: u64, index: u64, payload: &[u8]) -> crate::Entry {
    use crate::Data as _;
    let mut buf = std::vec::Vec::new();
    bytes::Bytes::copy_from_slice(payload).encode(&mut buf);
    crate::Entry::new(
      Term::new(term),
      Index::new(index),
      crate::EntryKind::Normal,
      bytes::Bytes::from(buf),
    )
  }

  /// Regression (review C2 + I3): a committed Normal entry whose `StateMachine::apply` returns
  /// `Err` must POISON the node with `PoisonReason::Apply` — not silently stall apply — and the
  /// poisoned node must be inert (all `handle_*` are no-ops).
  ///
  /// FAILS-ON-OLD: with the bare `break` (no `self.poison()`), `is_poisoned()` stays `false`,
  /// `applied` stays stuck behind `commit`, and the node keeps serving — so all three asserts
  /// (poisoned, reason, inertness) fail.
  #[test]
  fn failing_fsm_apply_poisons_node() {
    use crate::{AppendEntries, Index, Message, Term};
    use core::time::Duration;

    // Node 2 is a follower in a 3-voter cluster {1, 2, 3}.
    let cfg = crate::Config::try_new(
      2u64,
      std::vec![1u64, 2u64, 3u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
    let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 7, FailSm);
    let mut log = crate::testkit::VecLog::default();
    let mut stable = crate::testkit::NoopStable::default();

    // Leader 1 (term 1) sends one Normal entry carrying the 0xFF sentinel; leader_commit = 1
    // forces the follower to commit and apply it. FailSm::apply will return Err.
    let bad = normal_entry(1, 1, &[0xFFu8]);
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
        std::vec![bad],
        Index::new(1), // leader_commit = 1: the entry is committed
      )),
    );
    // Drain the deferred append completion so apply_committed runs with the durable entry.
    ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);

    // The FSM apply failed → node poisoned, with the precise cause.
    assert!(
      ep.is_poisoned(),
      "node must be poisoned when StateMachine::apply errors (review C2)"
    );
    assert_eq!(
      ep.poison_reason(),
      Some(crate::PoisonReason::Apply),
      "poison_reason must record the apply failure (review I3)"
    );
    // applied is stuck at the pre-apply watermark (the failing entry was never applied).
    assert_eq!(
      ep.applied,
      Index::ZERO,
      "the failing entry must not advance applied"
    );

    // The poisoned node is inert: subsequent handle_* are no-ops.
    let outgoing_before = ep.outgoing.len();
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
        std::vec![normal_entry(1, 2, b"ok")],
        Index::new(2),
      )),
    );
    ep.handle_timeout(
      Instant::ORIGIN + Duration::from_secs(10),
      &mut log,
      &mut stable,
    );
    ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
    assert_eq!(
      ep.outgoing.len(),
      outgoing_before,
      "a poisoned node must emit nothing on subsequent handle_*"
    );
    assert_eq!(
      ep.poison_reason(),
      Some(crate::PoisonReason::Apply),
      "poison_reason is first-cause-wins and must not change"
    );
  }

  /// Regression (review C2 + I3): a committed Normal entry whose `data` does NOT decode as the
  /// SM's `Command` must POISON the node with `PoisonReason::NormalEntryDecode`.
  ///
  /// FAILS-ON-OLD: with the bare `break` the decode error silently stalls apply —
  /// `is_poisoned()` stays `false` and `applied` is stuck behind `commit`.
  #[test]
  fn corrupt_normal_entry_poisons_node() {
    use crate::{AppendEntries, Index, Message, Term};
    use core::time::Duration;

    let cfg = crate::Config::try_new(
      2u64,
      std::vec![1u64, 2u64, 3u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap();
    let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 7, crate::testkit::CountSm::default());
    let mut log = crate::testkit::VecLog::default();
    let mut stable = crate::testkit::NoopStable::default();

    // A Normal entry whose data is a single byte. `<Bytes as Data>::decode` needs an 8-byte
    // u64 length prefix, so this decodes as UnexpectedEof → corrupt-log decode error.
    let corrupt = crate::Entry::new(
      Term::new(1),
      Index::new(1),
      crate::EntryKind::Normal,
      bytes::Bytes::from_static(&[0x01u8]),
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
        std::vec![corrupt],
        Index::new(1), // leader_commit = 1: the corrupt entry is committed
      )),
    );
    ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);

    assert!(
      ep.is_poisoned(),
      "node must be poisoned when a committed Normal entry fails to decode (review C2)"
    );
    assert_eq!(
      ep.poison_reason(),
      Some(crate::PoisonReason::NormalEntryDecode),
      "poison_reason must record the decode failure (review I3)"
    );
    assert_eq!(
      ep.applied,
      Index::ZERO,
      "the undecodable entry must not advance applied"
    );
  }

  /// `PoisonReason` follows the unit-enum convention (snake_case `as_str` + Display + predicates).
  #[test]
  fn poison_reason_as_str_display_and_predicate() {
    use crate::PoisonReason;
    assert_eq!(PoisonReason::Apply.as_str(), "apply");
    assert_eq!(
      PoisonReason::NormalEntryDecode.as_str(),
      "normal_entry_decode"
    );
    assert_eq!(PoisonReason::SnapshotRestore.as_str(), "snapshot_restore");
    assert!(PoisonReason::LogRead.is_log_read());
    assert!(!PoisonReason::LogRead.is_apply());
  }
}
