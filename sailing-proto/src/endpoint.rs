//! The Sans-I/O Raft core. It owns the consensus state and exposes the
//! `handle_*`/`poll_*` surface; leader election and log replication run through it.
use crate::{
  Config, Event, Index, Instant, LogStore, Message, NodeId, Now, Outgoing, Prng, ReadOnly,
  StableStore, StateMachine, Term,
};
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

// The `impl Endpoint` surface is split across these submodules by concern; each holds
// `impl` blocks that operate on the `Endpoint` defined here.
mod election;
mod membership;
mod persistence;
mod read_index;
mod read_mode;
mod replication;
mod restart;
mod snapshot;
mod transfer;

/// The max ENTRY COUNT a single committed-range read requests (apply, replication, the restart scans).
/// The store's byte cap is PAYLOAD-only, so a backlog of zero-payload entries (no-ops, empty/conf) would
/// let an owned store materialize O(backlog) structs despite it; bounding the requested range WIDTH caps
/// the count regardless of payload. The caller's loop re-reads the remainder.
pub(crate) const MAX_READ_BATCH_ENTRIES: u64 = 8192;

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

/// A read-only snapshot of the leader's replication progress for one peer — the observable subset of
/// the internal per-peer `Progress`. Returned by [`Endpoint::peer_progress`] for status /
/// observability (mirrors the per-peer `Progress` of etcd's `RawNode.Status`); meaningful only while
/// this node is the leader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerProgress {
  /// Highest log index known to be replicated on the peer.
  pub match_index: Index,
  /// Next log index the leader will send to the peer.
  pub next_index: Index,
  /// The peer's flow-control state (probe / replicate / snapshot).
  pub state: crate::ProgressState,
  /// Whether sending to the peer is currently paused (a probe is outstanding, the inflight window is
  /// full, or an `InstallSnapshot` is in flight).
  pub paused: bool,
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
  /// LeaseGuard post-election commit-wait: a newly-elected `ReadOnlyOption::LeaseGuard` leader
  /// holds its first commit until any deposed leader's read-lease has provably expired. The driver
  /// wakes at this deadline to retry the commit (the quorum match may already be satisfied — only
  /// the clock is pending), so the new leader's first commit lands promptly rather than at the next
  /// ack/heartbeat.
  CommitWait,
}

impl TimerKind {
  /// All timer kinds in a fixed order.
  pub const ALL: [TimerKind; 4] = [
    TimerKind::Election,
    TimerKind::Heartbeat,
    TimerKind::Transfer,
    TimerKind::CommitWait,
  ];

  /// The stable snake_case name.
  pub const fn as_str(&self) -> &'static str {
    match self {
      Self::Election => "election",
      Self::Heartbeat => "heartbeat",
      Self::Transfer => "transfer",
      Self::CommitWait => "commit_wait",
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
  /// A committed `SetReadMode` entry's payload failed to decode as a `ReadOnlyOption`.
  SetReadModeDecode,
  /// The `Changer` rejected a committed, validly-decoded `ConfChange`.
  ConfChangeApply,
  /// A snapshot blob failed to decode as `F::Snapshot` (install or restart).
  SnapshotDecode,
  /// `StateMachine::restore` failed while installing a snapshot (install or restart).
  SnapshotRestore,
  /// An AppendEntries conflict at or below the commit index would rewrite committed/applied log
  /// state — impossible in correct Raft; treated as fatal corruption.
  CommittedTruncation,
  /// An AppendEntries carried entries that are not positionally contiguous from `prev_log_index`
  /// (a gap, a duplicate, or an out-of-range embedded index) — a correct leader always sends a
  /// contiguous suffix, so this is fatal corruption (malformed or version-skewed input).
  NonContiguousAppend,
  /// A snapshot (install or restart) carried a `ConfState` that violates the core membership
  /// invariants (empty voters, learner/voter overlap, bad `learners_next`, non-joint `auto_leave`).
  /// Installing it verbatim would corrupt the membership tracker; a correct leader never sends one.
  InvalidConfState,
  /// On a snapshot INSTALL, `LogStore::restore` did not re-baseline the log to the snapshot boundary
  /// (`first_index() != last_index() + 1` afterward), so the log read-view is inconsistent with the
  /// commit/applied watermarks the install just advanced — every later AppendEntries consistency check
  /// and committed-entry fetch would read a wrong boundary. A conforming store re-baselines on `restore`,
  /// so this is a storage-contract violation or a buggy store; fail-stop rather than serve off a torn
  /// boundary (a release-mode promotion of what was a debug-only tripwire).
  SnapshotRebaseline,
  /// On restart the durable log is re-baselined past index 1 (`first_index() > 1`) but no durable
  /// snapshot exists to baseline the discarded prefix — committed entries below `first_index` are
  /// unrecoverable. A conforming `LogStore` orders the `restore` re-baseline durability AFTER the
  /// snapshot blob, so this is a durability-contract violation or disk corruption; fail-stop rather
  /// than bootstrap from the static config and serve a log whose committed prefix is gone.
  OrphanedLog,
  /// Recovered LeaseGuard floors are STRUCTURALLY self-contradictory — a cheap defense-in-depth fail-stop
  /// against a BUG in our own fold (CFT model: a `SnapshotMeta` from a correct leader over reliable storage
  /// is FAITHFUL, so this never fires in correct operation; forged-but-consistent floors are a Byzantine /
  /// corrupt-storage concern, out of scope). One of the three invariants a correct fold ALWAYS maintains is
  /// violated: a walled floor with no window bound (`max_wall_plus_window != 0` but `max_lease_window == 0`);
  /// the unwalled fallback exceeding the window bound (`max_unwalled_lease_window > max_lease_window`); or a
  /// window bound with no classified floor (`max_lease_window > 0` yet BOTH derived floors zero). Each makes
  /// the post-election commit-wait under-honor an inherited lease (a floor outliving the wait, or
  /// `precise_release_ready` vacuously clearing it), so the node fail-stops rather than arm a commit-wait /
  /// serve off self-contradictory state. Forged-MAGNITUDE shapes (a too-small nonzero floor) are deliberately
  /// not chased — that is the out-of-CFT Byzantine class. Regardless of read mode or ε_unc.
  InconsistentLeaseFloor,
  /// A BARE-wait ε_unc successor (no E′ inflation — e.g. Safe/LeaseBased carrying
  /// `bounded_clock_uncertainty`) inherited a WALLED entry whose wall horizon `wall_timestamp +
  /// lease_window + 2·ε_unc` is NON-PASSABLE (exceeds `u64::MAX`), so no `u64` wall reading can ever prove
  /// it expired. Such a node relies on the wall-gate to bound the inherited lease, but the gate can never
  /// fire; skipping it would let the bare mono wait clear early and undercut ANOTHER leader's inherited
  /// serve on a LOWER, passable committed anchor (Raft can split a near-`u64::MAX` tail entry onto only
  /// some voters). The horizon is unrepresentable, so fail-stop rather than under-wait. A real
  /// synchronized wall is `≈ 1.7·10¹⁸` ns (≪ `u64::MAX` ≈ year 2554); a non-passable inherited stamp is a
  /// crafted/corrupt entry, fail-stop like [`NonContiguousAppend`](Self::NonContiguousAppend). (An
  /// E′-INFLATED successor is unaffected — its mono wait covers the floor without the wall.)
  WallHorizonUnrepresentable,
  /// The post-election commit-wait deadline is unrepresentable: `now.mono()` is within the commit-wait
  /// window of `Instant::MAX`, so `now.mono() + window` SATURATES to a deadline SHORTER than the window.
  /// Storing it would clear the commit-wait early and commit before a deposed leader's lease window
  /// elapsed (a stale-read break — basic LeaseGuard AND the failover serve). The deadline cannot be
  /// scheduled correctly, so fail-stop rather than under-wait. Unreachable by any real monotonic clock
  /// (`Instant::MAX` ≈ 5.8·10¹¹ years), reachable only from a crafted/absurd `Now`, like
  /// [`LogExhausted`](Self::LogExhausted).
  CommitWaitUnrepresentable,
  /// The log index space is exhausted: `last_index == u64::MAX`, so no new entry (leader no-op,
  /// auto-leave-joint) can be allocated a strictly-greater index. Appending at the saturated index
  /// would truncate-and-replace the existing (possibly committed) entry there — a log-matching/apply
  /// safety break. Unreachable by legitimate appends (2^64 entries); reachable only from a crafted or
  /// corrupt recovered log, so fail-stop. (User proposals get `ProposeError::LogIndexExhausted`
  /// instead; this poison is for the internal append paths that have no error channel.)
  LogExhausted,
  /// On restart the durable `HardState.lease_support` is [`crate::LeaseSupport::Unrecorded`] (a genuine
  /// pre-`lease_support`/legacy record) but the caller supplied no `assume_prior_lease_support` bound. The
  /// prior LeaseBased promise is then UNKNOWN and possibly larger than this run's `election_timeout`, so no
  /// finite post-restart vote fence is provably safe — fail-stop rather than silently under-fence and grant
  /// a vote inside an old leader's still-live lease. Recover via `restart_migrating(assume_prior =
  /// the pre-upgrade election_timeout, or Some(ZERO) if it never enforced)`. Native nodes never hit this
  /// (genesis is `Recorded`), so it only guards a pre-format upgrade done via plain `restart`.
  LegacyLeaseUnrecoverable,
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
      Self::SetReadModeDecode => "set_read_mode_decode",
      Self::ConfChangeApply => "conf_change_apply",
      Self::SnapshotDecode => "snapshot_decode",
      Self::SnapshotRestore => "snapshot_restore",
      Self::CommittedTruncation => "committed_truncation",
      Self::NonContiguousAppend => "non_contiguous_append",
      Self::InvalidConfState => "invalid_conf_state",
      Self::SnapshotRebaseline => "snapshot_rebaseline",
      Self::OrphanedLog => "orphaned_log",
      Self::InconsistentLeaseFloor => "inconsistent_lease_floor",
      Self::WallHorizonUnrepresentable => "wall_horizon_unrepresentable",
      Self::CommitWaitUnrepresentable => "commit_wait_unrepresentable",
      Self::LogExhausted => "log_exhausted",
      Self::LegacyLeaseUnrecoverable => "legacy_lease_unrecoverable",
    }
  }
}

/// The full [`LogStore::restore`] postcondition for re-baselining the log to snapshot boundary
/// `(n, term)`: `first_index == n + 1` (the prefix is discarded), `last_index == n` (NO stale suffix is
/// retained above `n`), and `term(n) == term` (the boundary term). A store that violates ANY of these
/// leaves a torn read-view — a retained suffix above `n` could later be advertised by a campaign and a
/// current-term commit could apply an entry the snapshot was meant to discard — so the snapshot install
/// AND the restart `Restore` path both fail-stop ([`PoisonReason::SnapshotRebaseline`]) when this returns
/// `false`. A `term(n)` read error also fails the check (a faulty store, not a healthy boundary).
///
/// **Scope (CFT, defense-in-depth).** This is a best-effort fail-stop on the two IN-BAND re-baseline paths
/// — the snapshot install and the restart `Restore` action — against an obvious `LogStore` contract bug or
/// a bug in our own fold; a conforming store always passes. It is NOT a complete defense against Byzantine
/// / corrupt storage. In particular the restart `None` path legitimately KEEPS an uncommitted tail above a
/// snapshot boundary (a snapshot at `n` followed by not-yet-committed replication — standard Raft), and a
/// suffix-retaining store that crashed between `restore` and this check would land on exactly that shape,
/// indistinguishable from the legitimate one at the durable level. Discriminating them would wrongly
/// discard a correct store's valid uncommitted tail, so a fully contract-violating store is the
/// out-of-CFT corrupt-storage class — as for [`PoisonReason::InconsistentLeaseFloor`] and the failover
/// forged-floor decision — and is not exhaustively chased.
pub(crate) fn restore_rebaselined<L: LogStore>(log: &L, n: Index, term: Term) -> bool {
  log.first_index() == n.next() && log.last_index() == n && log.term(n).ok() == Some(term)
}

/// The derived in-memory lease-safety state produced by [`reconcile_durable`] from the durable record + the
/// post-restart config: the floor to seed and the post-restart vote-fence WINDOW (the caller adds `now`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DerivedLeaseSafety {
  /// The in-memory `lease_support_floor` to seed (the max lease window this incarnation will back).
  lease_support_floor: Option<core::time::Duration>,
  /// The vote-fence WINDOW (`None` = no fence); the caller arms `now + window`.
  fence_window: Option<core::time::Duration>,
}

/// The outcome of [`reconcile_durable`]: either a safe derived state, or a fail-stop for a legacy record
/// whose prior promise cannot be safely bounded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LeaseReconcile {
  /// Safe to proceed with this derived lease-safety state.
  Ok(DerivedLeaseSafety),
  /// A legacy `Unrecorded` record with no operator-supplied `assume_prior` bound — the prior promise is
  /// unknown/unbounded, so no finite fence is provably safe. Fail-stop ([`PoisonReason::LegacyLeaseUnrecoverable`]).
  Poison,
}

/// Decide the post-restart lease-safety state from the DURABLE lease-support record + the post-restart
/// config, branching on the recovered PROVENANCE ([`crate::LeaseSupport`]). PURE and total — the lease-axis
/// sibling of [`reconcile_restart_log`], exhaustively case-testable in isolation.
///
/// - `Recorded(d)` (a current-format node's authoritative record): trust it. The floor is `d.max(this_run)`
///   — a recorded promise dominates a config shrink (the config-drift fix), and this run's own future
///   acks are covered. `assume_prior` is IGNORED: a native record is authoritative, never over-fenced on
///   operator input. `Recorded(None)` genuinely means "promised nothing" (the persist-before-advertise gate
///   guarantees a native node never advertised more than its durable floor), so it fences only by `this_run`.
/// - `Unrecorded` (a genuine pre-format/legacy decode): the prior promise is UNKNOWN and possibly LARGER
///   than `election_timeout`, so no finite fence is provably safe. With an operator-supplied
///   `assume_prior` bound (via `restart_migrating`; `Some(ZERO)` asserts "never promised") the floor is
///   `this_run.max(assume_prior)` — safe. WITHOUT one (plain `restart`), fail-stop with `Poison` rather than
///   silently under-fence: `this_run` is NOT an upper bound on the unknown old promise, and persisting it
///   would also erase the `Unrecorded` provenance and defeat a later `restart_migrating`. (Native nodes
///   never reach this — genesis is `Recorded` — so it only guards a pre-format upgrade done via plain restart.)
///
/// A genuine-ZERO floor (`Some(0)`) is filtered out of the fence window (a recorded no-op promise).
/// `this_run` = the lease window THIS incarnation will advertise = `election_timeout` iff it enforces.
fn reconcile_durable(
  recovered: crate::LeaseSupport,
  enforcing: bool,
  election_timeout: core::time::Duration,
  assume_prior: Option<core::time::Duration>,
) -> LeaseReconcile {
  let this_run = if enforcing {
    Some(election_timeout)
  } else {
    None
  };
  let lease_support_floor = match recovered {
    crate::LeaseSupport::Recorded(d) => d.max(this_run),
    // Legacy record with a known operator bound: safe. Without one: unbounded → fail-stop.
    crate::LeaseSupport::Unrecorded if assume_prior.is_some() => this_run.max(assume_prior),
    crate::LeaseSupport::Unrecorded => return LeaseReconcile::Poison,
  };
  let fence_window = lease_support_floor.filter(|d| !d.is_zero());
  LeaseReconcile::Ok(DerivedLeaseSafety {
    lease_support_floor,
    fence_window,
  })
}

/// The action [`Endpoint::restart`] must take to make the durable LOG consistent with the durable
/// SNAPSHOT — the output of [`reconcile_restart_log`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RestartLogAction {
  /// Already consistent (no snapshot + an uncompacted log, or the log already compacted exactly to
  /// the snapshot boundary). No log mutation.
  None,
  /// Complete a deferred LOCAL-snapshot compaction: `compact(N)` drops `[..=N]` while PRESERVING the
  /// committed tail above `N` for replay.
  Compact(Index),
  /// Complete an interrupted INSTALL re-baseline: `restore(N, term)` discards the (behind/divergent,
  /// uncommitted) log and baselines at the snapshot.
  Restore(Index, Term),
  /// The durable state cannot be reconciled without discarding a committed entry — fail-stop.
  Poison(PoisonReason),
}

/// Decide how `restart` reconciles the durable log with the durable snapshot, enforcing ONE safety
/// invariant: a committed entry is NEVER discarded — committed `[1..=commit]` is
/// `snapshot[1..=N] ++ log[N+1..=commit]`, so if the durable state implies a committed entry must be
/// dropped, the result is [`RestartLogAction::Poison`].
///
/// This is a PURE, total function over the durable shape so it is exhaustively case-testable in
/// isolation (every snapshot/log/commit combination maps to exactly one action by construction,
/// rather than by ad-hoc cases). Two normal crash windows — a deferred `compact` and an interrupted
/// install `restore` — both leave `first_index <= N` but must recover differently; the discriminator
/// is whether the log still holds the snapshot boundary entry, gated on whether a committed tail sits
/// above the snapshot.
///
/// Inputs:
/// - `snap`: the durable snapshot `(N, last_term)`, or `None`.
/// - `committed_in_log`: the highest committed index actually present in the log, i.e.
///   `min(hard_state.commit, log.last_index())`.
/// - `first_index` / `last_index`: the durable log bounds.
/// - `boundary_term`: `Some(Ok(t))` if the boundary index `N` is in the log (term `t`);
///   `Some(Err(()))` if reading it failed; `None` if `N` is not in the log. Only consulted when the
///   log spans `N`.
fn reconcile_restart_log(
  snap: Option<(Index, Term)>,
  committed_in_log: Index,
  first_index: Index,
  last_index: Index,
  boundary_term: Option<Result<Term, ()>>,
) -> RestartLogAction {
  // Log-validity precondition (snapshot-independent): a valid log is contiguous, so
  // `first_index <= last_index + 1` (equal when empty/baselined). A larger gap — e.g. a
  // partially-persisted re-baseline that advanced `first_index` to `N + 1` while `last_index` stayed
  // below `N` — is a structurally-impossible shape (store corruption); fail-stop. This also makes the
  // `first_index == N + 1` branch below total: a valid log with `first_index == N + 1` necessarily
  // has `last_index >= N`, so reaching that branch can never mean "behind the snapshot".
  if first_index > last_index.next() {
    return RestartLogAction::Poison(PoisonReason::OrphanedLog);
  }
  let Some((n, t)) = snap else {
    // No durable snapshot: every committed entry must come from the log, so nothing may be compacted
    // away. A compacted log (`first_index > 1`) with no snapshot has lost its committed prefix.
    return if first_index > Index::new(1) {
      RestartLogAction::Poison(PoisonReason::OrphanedLog)
    } else {
      RestartLogAction::None
    };
  };
  if first_index > n.next() {
    // Compacted PAST the snapshot: `[N+1 .. first_index-1]` has no baseline — committed prefix gone.
    RestartLogAction::Poison(PoisonReason::OrphanedLog)
  } else if last_index < n {
    // Log entirely below `N` (interrupted install re-baseline): it holds no entry above the snapshot,
    // so `committed_in_log <= last_index < N` — nothing committed is lost. Re-baseline.
    RestartLogAction::Restore(n, t)
  } else {
    // The snapshot boundary `N` is materialized in the log — either as a live entry
    // (`first_index <= N <= last_index`) or as the compacted baseline (`first_index == N + 1`, so
    // `N == first_index - 1`). Both expose a readable boundary term, which we VALIDATE against the
    // snapshot before trusting the log to continue from it: a disagreeing boundary means the log and
    // snapshot are from different histories (corruption / stale snapshot). (The already-compacted
    // `first_index == N + 1` case previously trusted this blindly.)
    match boundary_term {
      Some(Ok(bt)) if bt == t => {
        // Boundary matches — the log is a valid continuation of the snapshot. If it is already
        // compacted exactly to the boundary (`first_index == N + 1`) it is consistent (no mutation);
        // otherwise compact to drop the snapshotted prefix while PRESERVING the committed tail above
        // `N`.
        if first_index == n.next() {
          RestartLogAction::None
        } else {
          RestartLogAction::Compact(n)
        }
      }
      Some(Ok(_)) => {
        // Boundary term MISMATCHES the snapshot. If a committed entry sits AT OR ABOVE `N`
        // (`committed_in_log >= N`), then the boundary index `N` itself is already committed (equality)
        // or a committed entry sits above it — so the disagreeing term means the committed history
        // diverges at a committed point, impossible in correct Raft → fatal corruption; poison rather
        // than discard or OVERWRITE the committed boundary with snapshot metadata from a different
        // history. Only when `committed_in_log < N` are the divergent entries (the boundary included)
        // uncommitted locally — re-baseline to the snapshot. (Equality is a committed boundary too;
        // `>` would re-baseline a committed `N` onto a different history. `committed_in_log >= n` ⇔
        // `hs.commit >= n` here, since `last_index >= n` in this branch.)
        if committed_in_log >= n {
          RestartLogAction::Poison(PoisonReason::OrphanedLog)
        } else {
          RestartLogAction::Restore(n, t)
        }
      }
      _ => {
        // `Err` (fatal boundary term-read fault — never an excuse to truncate) or `None` (caller
        // contract violation: the boundary `N` is provably materialized in the log here). Poison.
        RestartLogAction::Poison(PoisonReason::LogTerm)
      }
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
  /// The candidate's term+self-vote hard-state write is in flight. Until it is durable the self-vote
  /// must NOT be acted on: `become_leader` (single-node now, or once peer votes arrive) fires from
  /// `on_stable_wrote`/`on_vote_resp` only after this completes. `term` guards a stale completion that
  /// arrives after the term advanced. This makes the candidate's self-vote persist-before-act,
  /// symmetric with the follower's `CastVote` — otherwise a node could lead in a term on an un-durable
  /// self-vote, crash, restart with no recorded vote, and grant another candidate the same term.
  Campaign { term: crate::Term },
}

/// Cap on the number of distinct read contexts a follower may hold in-flight to its leader at once
/// (the [`ForwardedReads`] set). A follower inserts a context before forwarding and removes it only
/// on the matching `ReadIndexResp`; if the request or its response is dropped while the leader stays
/// stable, distinct retry contexts would otherwise accumulate without bound. At the cap the oldest
/// in-flight context is evicted FIFO. Kept independent of `max_inflight_msgs` (the leader's per-peer
/// replication window) because the two limits are unrelated; 256 is the same generous default.
const MAX_FORWARDED_READS: usize = 256;

/// Upper bound on a LEADER's combined in-flight read backlog — deferred reads awaiting the
/// current-term no-op (`pending_reads`) plus reads awaiting heartbeat-quorum confirmation
/// (`read_only`). A partitioned leader never drains this backlog, so without the cap a spammy or
/// looping client could drive unbounded `Bytes` retention. Beyond the cap a local read is rejected
/// with `TooManyInFlight` and a forwarded read is dropped (the follower can re-issue).
const MAX_LEADER_READS: usize = 256;

/// The reads this node (as a FOLLOWER) has forwarded to its current leader and is still awaiting a
/// `ReadIndexResp` for. Each is keyed by an INTERNAL token (NOT the application context) — the
/// follower-side mirror of the leader's round-token fix: the token is what travels in the forwarded
/// `ReadIndex`/`ReadIndexResp`, so a stale or duplicated response echoing an earlier forward's token
/// can never complete a LATER read that reused the same user context. The user context rides alongside
/// for the `DuplicateContext` in-flight guard and for the emitted `ReadState`. Backed by a `VecDeque`
/// (FIFO) and bounded at [`MAX_FORWARDED_READS`] via BACK-PRESSURE (a full set rejects the new read
/// rather than evicting an accepted one). The cap is small, so linear scans are cheaper than the
/// bookkeeping a separate index would need.
///
/// The token is `boot_epoch (8 bytes) || counter (8 bytes)`. `counter` is unique WITHIN an incarnation;
/// `boot_epoch` (durable, app-provided via [`Endpoint::restart`], strictly increasing per restart) makes
/// tokens unique ACROSS restarts. Without it a same-term restart resets `counter` to 0, and a delayed
/// pre-crash `ReadIndexResp` could complete a post-restart read at a stale index — a linearizability
/// break under a transport that redelivers pre-crash messages.
#[derive(Debug, Default)]
struct ForwardedReads {
  /// `(internal token, user context)` in forward order.
  order: VecDeque<(Bytes, Bytes)>,
  /// This incarnation's durable boot epoch — the high 8 bytes of every token (cross-restart uniqueness).
  boot_epoch: u64,
  /// Monotonic source of the low 8 bytes — unique WITHIN this incarnation.
  next_token: u64,
}

impl ForwardedReads {
  /// Construct for an incarnation whose durable, app-provided boot epoch is `boot_epoch`.
  fn new(boot_epoch: u64) -> Self {
    Self {
      order: VecDeque::new(),
      boot_epoch,
      next_token: 0,
    }
  }

  /// Whether the user `context` is currently in flight (the duplicate-context guard).
  fn contains_context(&self, context: &Bytes) -> bool {
    self.order.iter().any(|(_, c)| c == context)
  }

  /// Whether the in-flight set is at capacity. The follower applies BACK-PRESSURE here rather than
  /// evicting: silently dropping an already-accepted read (one `read_index` returned `Ok` for) would
  /// strand it forever, and after eviction the reused context could complete the WRONG read with a
  /// stale index. So a full set rejects the NEW read instead of evicting an old one.
  fn is_full(&self) -> bool {
    self.order.len() >= MAX_FORWARDED_READS
  }

  /// Record a NEW forwarded read for user `context` and return its fresh internal token (sent to the
  /// leader as the `ReadIndex` context and echoed back in the `ReadIndexResp`). The caller has already
  /// verified `!contains_context(context)` (dedup) AND `!is_full()` (back-pressure).
  fn push(&mut self, context: Bytes) -> Bytes {
    let mut buf = [0u8; 16];
    buf[..8].copy_from_slice(&self.boot_epoch.to_be_bytes());
    buf[8..].copy_from_slice(&self.next_token.to_be_bytes());
    self.next_token += 1;
    let token = Bytes::copy_from_slice(&buf);
    self.order.push_back((token.clone(), context));
    token
  }

  /// Remove the forwarded read identified by `token` (the echoed correlator), returning its user
  /// context if present. `None` means unsolicited / stale / already-completed — doubling as the
  /// already-completed guard in `on_read_index_resp`.
  fn remove_by_token(&mut self, token: &[u8]) -> Option<Bytes> {
    let pos = self.order.iter().position(|(t, _)| t.as_ref() == token)?;
    self.order.remove(pos).map(|(_, ctx)| ctx)
  }

  /// Drop every in-flight read (term change / leader loss): reads forwarded to a now-stale
  /// leader must be re-issued to the new one, not block on a confirmation that will never come.
  fn clear(&mut self) {
    self.order.clear();
  }

  /// Current number of in-flight reads. Test-only (bound assertion).
  #[cfg(test)]
  fn len(&self) -> usize {
    self.order.len()
  }
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
  /// committed log. Init `Index::ZERO` in `new`; init to the recovered commit in
  /// `restart` (so the choke-point doesn't immediately re-persist an unchanged value).
  committed_persisted: Index,
  /// Highest log index durably persisted (an append's LogDone::Appended fired). Every outbound
  /// AppendResp match is clamped to this so a follower never reports an index only in its
  /// visible-but-unflushed tail (persist-before-ack on the immediate-ack path too).
  durable_index: Index,
  /// The highest DURABLE snapshot boundary this node holds — a SEPARATE durability watermark from
  /// the durable log tip (`durable_index`). It matters only when a deferred install is DROPPED as stale
  /// (in-window appends advanced `commit` past the boundary over a not-yet-flushed tail): the blob is
  /// durable but the log was NOT re-baselined, so `durable_index` stays below the boundary even though a
  /// crash would `reconcile_restart_log::Restore` to this snapshot. `ack_watermark()` takes the MAX of
  /// the two, so the follower honestly acks its true recoverable prefix and the leader is not pinned in
  /// `ProgressState::Snapshot`. Monotone (a durable snapshot sits at a committed, hence permanent, index);
  /// volatile (init ZERO — after restart the reconciled log already covers any durable snapshot).
  durable_snapshot_index: Index,
  /// `Some((blob_opid, meta, decoded_snap, leader))` while a FOLLOWER snapshot install is DEFERRED —
  /// its blob has been submitted (`submit_snapshot`) but is not yet durable. The destructive
  /// install body (SM restore, `commit`/`applied` advance, the `log.restore` re-baseline, membership
  /// install, the success ack) is held here and run by `install_snapshot_now` ONLY once the blob is
  /// durable — the matching `SnapshotWritten`, or `StableStore::durable_snapshot()` evidence covering
  /// the boundary if that completion was missed. Until then the follower stays in its OLD consistent
  /// state, so a crash in the window loses only the in-flight blob and restart re-syncs from the
  /// UNCHANGED durable log (no orphaned re-baseline → no `OrphanedLog` poison). This is the snapshot
  /// analogue of `pending_compact` deferring `log.compact` until the blob is durable — the core owns
  /// the ordering rather than the storage layer (the audited golden fix). Holds the DECODED snapshot
  /// (move-out-and-replace on supersede, never `Clone` — `F::Snapshot` has only a `Data` bound); a
  /// separate field, so a higher-term step-down's `self.pending.clear()` does NOT drop it (a boundary
  /// that is already quorum-committed stays valid across a pure term bump). `restart` resets it to `None`.
  pending_install: Option<(crate::OpId, crate::SnapshotMeta<I>, F::Snapshot, I)>,
  prng: Prng,
  /// Per-voter ballot: `true` = grant, `false` = reject. Absent IDs have not voted yet.
  /// Replaces the old `votes_granted: BTreeSet<I>` — the joint quorum needs the full
  /// ballot (grants *and* rejections), not just the grant set.
  votes: BTreeMap<I, bool>,
  election_deadline: Option<Instant>,
  heartbeat_deadline: Option<Instant>,
  /// LeaseGuard post-election commit-wait deadline (`None` in every other mode and on every
  /// non-leader). Set at [`become_leader`](Self::become_leader) to `now + max_lease_window` (the bare
  /// conservative bound; ONLY when `become_leader` sets [`commit_wait_inflated`](Self::commit_wait_inflated)
  /// — a failover node that PROVED the wall floor at election — is the window instead E′-INFLATED to
  /// `max_lease_window·(1+ρ)`; otherwise the wait stays bare and a still-live walled lease is held by
  /// `walled_lease_vetoes_conservative` until the wall proves expiry or the node fails closed) — two
  /// CONSERVATIVE bounds (see that method): the TIME anchor `now` is ≥ every inherited entry's
  /// creation time, and the WINDOW bound `max_lease_window` is the MAX self-describing
  /// [`Entry::lease_window`](crate::Entry::lease_window) over the inherited entries, so it covers ANY
  /// deposed leader's lease (each entry carries its own leader's exact `Δ·(Δ+ε)/(Δ−ε)`), even under
  /// heterogeneous per-node config, with NO cross-node clock comparison. `maybe_advance_commit` HOLDS
  /// the first post-election commit while `now < commit_wait_until`; once the deadline passes the gate
  /// is lifted for good (cleared in `maybe_advance_commit`) until the next election.
  ///
  /// On the FAILOVER tier the mono clear is additionally WALL-GATED for the WALLED inherited class
  /// (`walled_lease_vetoes_conservative`): a due mono deadline does not clear a still-live walled
  /// inherited lease (whose wall floor `s_c + W_c` a peer's inherited-read serve duals); on such a veto
  /// this field is RE-ARMED to a strictly-future mono instant (one heartbeat) so the wedge tripwire holds
  /// and the leader re-tests the wall. The E′ inflation in `become_leader` remains as the mono backstop
  /// for the WALL-ABSENT transient (when the wall-gate cannot evaluate and falls back to mono).
  commit_wait_until: Option<Instant>,
  /// The MAX LeaseGuard commit-wait window (`Entry::lease_window`, nanos) over every entry this node
  /// has ever held — the SELF-DESCRIBING cross-leader safety bound. `become_leader` sizes its
  /// commit-wait at `now + max_lease_window`, which covers ANY deposed leader's lease on an inherited
  /// entry (each carries its own leader's exact `Δ·(Δ+ε)/(Δ−ε)`), no assumption about other configs.
  /// Monotonically non-decreasing in memory (a stale-HIGH value is safe — it only over-waits): raised
  /// on `submit_append` over appended entries and on snapshot install over `SnapshotMeta`; carried
  /// through compaction (into the created `SnapshotMeta`) and recomputed at restart from the durable
  /// log + restored snapshot. `0` in non-LeaseGuard clusters (every `lease_window` is `0`) ⇒ no wait.
  max_lease_window: u64,
  /// The MAX per-entry `wall_timestamp + lease_window` over every entry this node has ever held —
  /// the FAILOVER-tier precise commit-anchor's release floor (consumed by a later PR). The
  /// synchronized-wall analogue of [`max_lease_window`](Self::max_lease_window): paired PER ENTRY
  /// (never the max stamp with a different entry's window), monotonic in memory, folded on
  /// `submit_append` + snapshot install, carried through compaction, recomputed at restart. `0`
  /// outside the failover tier (every `wall_timestamp` is `0`).
  max_wall_plus_window: u64,
  /// The MAX `lease_window` over every entry this node has ever held that is LEASE-bearing but
  /// WALL-ABSENT (`lease_window > 0`, `wall_timestamp == 0`) — the failover precise commit-anchor's
  /// mono-frame fallback bound for wall-absent inherited leases. Folded by the ENTRY property (the exact
  /// dual of [`max_wall_plus_window`](Self::max_wall_plus_window)'s `wall_timestamp != 0`), NOT the local
  /// tier, so it is complete BY CONSTRUCTION across heterogeneous per-node tiers — every wall-absent lease
  /// entry folds itself on every node. Equals [`max_lease_window`](Self::max_lease_window) in a
  /// non-failover LeaseGuard cluster, but inert there (the consumer `precise_release_ready` is off-tier);
  /// `0` for Safe/LeaseBased. Monotonic in memory, folded on `submit_append` + snapshot install, carried
  /// through compaction, recomputed at restart (snapshot carry ⊔ live scan, like `max_lease_window`).
  max_unwalled_lease_window: u64,
  /// FAILOVER-tier precise commit-anchor — the WALL-frame release floor, captured ONCE at
  /// [`become_leader`](Self::become_leader) as the then-current
  /// [`max_wall_plus_window`](Self::max_wall_plus_window) (max over WALLED inherited entries of
  /// `wall_timestamp + lease_window`) and immutable for the term. `maybe_advance_commit` lifts the
  /// post-election commit-wait early once the successor's synchronized wall passes this floor by
  /// `2·ε_unc`. `0` ⇒ no walled inherited entry ⇒ the wall gate is vacuously satisfied; the shipped
  /// conservative anchor still governs off-tier or when the leader holds no synchronized wall.
  inherited_release_deadline: u64,
  /// FAILOVER-tier precise commit-anchor — the MONO-frame fallback deadline for any WALL-ABSENT
  /// (fail-closed) inherited lease entry, captured ONCE at [`become_leader`](Self::become_leader) as
  /// `now + max_unwalled_lease_window` and immutable for the term. The precise early-release ALSO
  /// requires this deadline to pass, so a fail-closed entry (uncovered by the wall floor) still waits
  /// out its lease on the conservative mono bound. `None` ⇒ no such entry ⇒ that half of the gate is
  /// satisfied.
  unwalled_commit_wait_until: Option<Instant>,
  /// FAILOVER-tier observability: how many times the PRECISE commit-anchor (not the conservative
  /// mono deadline) lifted the post-election commit-wait over this node's lifetime. Pure in-memory
  /// metric — never persisted, never on the wire, reset to `0` on construction and restart, and read
  /// only via [`precise_releases`](Self::precise_releases). Lets an operator (and the randomized
  /// tester) confirm the failover early-release path is actually being exercised rather than always
  /// deferring to the conservative anchor; `0` outside the failover tier.
  precise_releases: u64,
  /// FAILOVER-tier observability: how many times the post-election commit-wait HELD because the inherited
  /// walled lease floor was UNPROVABLE this tick — either NO synchronized wall on the release path (a driver
  /// that armed the failover tier but did not supply a wall to `handle_timeout`/`handle_storage` here), or
  /// NO bounded clock-uncertainty to wall-gate (a node outside the synchronized-clock contract that
  /// inherited walled entries). Such a hold is FAIL-CLOSED and SAFE — it never undercuts a peer's inherited
  /// serve — but it does NOT self-resolve until a wall is supplied or the node is reconfigured, so it would
  /// otherwise be a SILENT permanent commit-wait wedge. Pure in-memory metric — never persisted, never on
  /// the wire, reset to `0` on construction and restart, read only via
  /// [`unprovable_floor_holds`](Self::unprovable_floor_holds). It climbs ONLY while an inherited WALLED
  /// commit-wait holds unprovably, so it is `0` with no inherited walled lease and for a healthy failover
  /// node whose wall is always supplied — but it CAN be nonzero on a no-ε_unc node (which is OUTSIDE the
  /// active failover tier) that inherited walled entries from an ε_unc leader. A steadily climbing value
  /// flags a misconfigured driver or a heterogeneous-ε_unc cluster (the silent-wedge class the architecture
  /// review surfaced). A wall-PRESENT, not-yet-released
  /// hold is NORMAL (it lifts when the wall passes the floor) and is NOT counted here.
  unprovable_floor_holds: u64,
  /// Cold-read wedge observability counter (sibling of [`unprovable_floor_holds`](Self::unprovable_floor_holds)).
  /// Bumped each time `apply_committed` defers on an [`EntriesRead::Pending`](crate::EntriesRead::Pending)
  /// cold read. A healthy resident store never returns `Pending`, so this stays `0`; a steadily climbing
  /// value flags a store wedging on a range it never makes resident (the cold-read liveness obligation
  /// broken). Pure in-memory metric — never persisted, never on the wire, reset to `0` on construction and
  /// restart, read only via [`cold_read_defers`](Self::cold_read_defers). Not a poison.
  cold_read_defers: u64,
  /// FAILOVER-tier inherited-read serve anchor — the election TAIL, captured ONCE at
  /// [`become_leader`](Self::become_leader) as `log.last_index()` BEFORE the leader's own no-op (which
  /// would otherwise inflate it). Immutable for the term (`log.last_index()` drifts as the leader
  /// proposes, so it may NOT be recomputed live). With the committed index `c`, the limbo region the
  /// application checks before an inherited serve is `(c, limbo_upper]`. `Index::ZERO` until the first
  /// election; meaningful only while this node leads and holds the post-election commit-wait.
  limbo_upper: Index,
  /// FAILOVER-tier inherited-read serve anchor — the EXACT `wall_timestamp` of `log[c]` (the committed
  /// entry at election, shared by this leader and every electable higher-term leader), captured ONCE at
  /// [`become_leader`](Self::become_leader). The [`failover_read_window`](Self::failover_read_window)
  /// lease-live gate keys on it; stale-HIGH would serve past a dead lease (UNSAFE), so it is
  /// exact-or-`0` (fail-closed when `log[c]` is absent/compacted — the gate then refuses). `0` outside
  /// the failover tier and until the first election.
  committed_anchor_wall: u64,
  /// FAILOVER-tier inherited-read serve HORIZON — the EXACT `lease_window` (`W_c`) of the SAME committed
  /// anchor entry `log[c]`, captured ONCE at [`become_leader`](Self::become_leader) from the same fetch
  /// as [`committed_anchor_wall`](Self::committed_anchor_wall). The serve gate is `now_wall + 2·ε_unc <
  /// committed_anchor_wall + committed_anchor_window`: the horizon is the entry's OWN self-describing
  /// window, NOT this successor's config `lease_duration` — exactly like the release floor
  /// ([`inherited_release_deadline`](Self::inherited_release_deadline)), so serve and release dovetail on
  /// the same shared entry's window for ANY per-node config (a successor configured with a longer lease
  /// than the entry's creator can NOT over-serve past the release). `0` (fail-closed, refuse) when the
  /// anchor is absent/compacted or not lease-bearing.
  committed_anchor_window: u64,
  /// FAILOVER-tier inherited-read SERVE armed-this-term flag, captured ONCE at
  /// [`become_leader`](Self::become_leader): `true` iff a VALID active failover tier is configured AND
  /// the E′-inflated conservative commit-wait (`max_lease_window · (1+ρ)`, ceil) fits strictly below the
  /// election timeout. When `false` the leader uses the bare (shipped) conservative wait and
  /// [`failover_read_window`](Self::failover_read_window) returns `None` — no inherited serve, so no mono-undercut risk
  /// to inflate against. This is the RUNTIME liveness gate config validation cannot be: the wait keys on
  /// `max_lease_window`, the MAX window INHERITED (possibly stamped by another node's larger config),
  /// unknown at config time. `false` outside the failover tier and until the first election.
  inherited_serve_armed: bool,
  /// Whether THIS term's post-election commit-wait is the E′-INFLATED window (vs the bare
  /// `max_lease_window`), captured ONCE at [`become_leader`](Self::become_leader): `true` iff a computable
  /// E′ inflation (`max_lease_window·(Δ+ε_drift)/Δ`) FITS below the election timeout AND this node
  /// inherited WALLED entries (`max_wall_plus_window != 0`). The E′ inflation needs ONLY the lease timing
  /// (Δ, ε_drift), NOT `bounded_clock_uncertainty` — so a LeaseGuard successor that lacks ε_unc still gets
  /// an E′-safe wait (it covers the walled wall floor in REAL time without a synchronized wall). The veto
  /// [`walled_lease_vetoes_conservative`](Self::walled_lease_vetoes_conservative) does NOT veto an
  /// inflated wait (E′ already makes the conservative clear safe); a BARE wait with inherited walled
  /// entries must instead prove the floor via the wall, else fail closed. DISTINCT from
  /// `inherited_serve_armed` (which additionally requires ε_unc for the SERVE). `false` with no walled
  /// inherited entries (basic LeaseGuard / Safe), and until the first election.
  commit_wait_inflated: bool,
  /// LeaseGuard lease-refresh demand: set when a LeaseGuard read finds the lease stale (and so degrades
  /// to the Safe round), consumed at the next leader heartbeat tick, which appends ONE stamped no-op to
  /// re-commit and re-stamp the lease so subsequent reads serve fast again. A flag (not a count): the
  /// refresh is rate-limited to one in-flight no-op (the heartbeat only appends when the log is fully
  /// committed). Never set outside LeaseGuard, so it cannot perturb Safe/LeaseBased. Reset on
  /// step-down/restart (only a leader acts on it, and a stale read re-sets it as needed).
  lease_refresh_wanted: bool,
  /// Whether a LeaseGuard read has been served (or degraded) since the current committed anchor — the
  /// gate for the proactive [`crate::LeaseRefresh`] modes. Set by any leader LeaseGuard read, cleared
  /// when a fresh current-term entry COMMITS (the committed anchor advances, re-anchoring the lease) and
  /// on step-down/restart. Cleared at the COMMIT, not the append: a read landing between a refresh no-op's
  /// append and its commit must not survive into the new anchor and fire one idle no-op after reads stop.
  /// A leader with NO reads since its anchor never proactively refreshes (no idle write amplification).
  read_since_anchor: bool,
  outgoing: VecDeque<Outgoing<I>>,
  events: VecDeque<Event<I, F::Response>>,
  /// Runtime membership: joint voter config, learner sets, and per-peer `Progress`.
  /// Replaces the old `progress: BTreeMap<I, crate::Progress>` and static-voter quorum.
  tracker: crate::Tracker<I>,
  /// Monotonically minted id for every storage submission.
  next_op_id: crate::OpId,
  /// Outstanding write → deferred action.
  pending: BTreeMap<crate::OpId, Pending<I>>,
  /// Per-append last-index, keyed by the submission's `OpId`, for EVERY in-flight log append —
  /// independent of `pending`. `durable_index` must advance on every `LogDone::Appended`, but
  /// `pending` is cleared on term changes and a same-term step-down routes a `LeaderAppend`
  /// completion to the `_` arm; in both cases the entry still became durable. Keeping the upto
  /// here lets `on_log_appended` advance the watermark unconditionally (role/term-independent),
  /// so a follower never under-acks its durable suffix on a later duplicate/empty AppendEntries.
  /// Entry is recorded in `submit_append`, removed (consumed into the watermark) in
  /// `on_log_appended`, pruned on §5.3 truncation, and cleared on snapshot restore. Init empty
  /// in `new` and `restart`.
  inflight_append_upto: BTreeMap<crate::OpId, Index>,
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
  /// against the durable snapshot: once `StableStore::snapshot()` reports a persisted
  /// snapshot whose `last_index >= up_to`, the blob is durable, the deferred compaction is
  /// performed, and this field is cleared — so a missed completion can no longer wedge future
  /// snapshots. A store error still poisons the node via `handle_storage`, and `restart` resets
  /// this field to `None`.
  pending_compact: Option<(crate::OpId, Index)>,
  /// Per-peer deadline before which the full `InstallSnapshot` blob is NOT re-sent to a
  /// `Snapshot`-state peer. A deferred install legitimately takes many heartbeat intervals (blob
  /// fsync + apply), so resending on EVERY response would re-transmit a large snapshot tens of
  /// times per install — and response COUNT is the wrong clock entirely (ReadIndex Safe rounds
  /// elicit extra responses, accelerating a count-based pacer arbitrarily). The deadline is
  /// time-based: at most one resend per election timeout, regardless of response rate; a genuinely
  /// dropped blob is still retried within one election timeout of the next response (liveness).
  /// Entries clear when the peer leaves `Snapshot` state and on leadership change.
  snapshot_resend_after: BTreeMap<I, Instant>,
  /// Term-before-respond durability. The highest `Term` whose HardState write has reached stable
  /// storage — `term_is_durable()` is simply `durable_term >= self.term`. Seeded to the initial/recovered
  /// term (trivially durable: it came from durable HardState or is the bootstrap term), then advanced in
  /// [`on_stable_wrote`] when an adopted term's write completes. The core observes the stable seam's
  /// completions, so it enforces currentTerm-before-respond itself rather than delegating the ordering to
  /// the storage layer.
  durable_term: crate::Term,
  /// The highest `Term` ever submitted to the `StableStore` (via `submit_write`). Paired with
  /// `term_persist_opid` so [`on_stable_wrote`] can recognise when the current term's write completes and
  /// advance `durable_term` (a freshly-adopted term is set in memory before its write is even submitted).
  last_submitted_term: crate::Term,
  /// The `OpId` of the FIRST HardState write that carried `last_submitted_term`. When a `Wrote` completion
  /// with `opid >= term_persist_opid` arrives (stable completions are ordered), that term is durable.
  term_persist_opid: crate::OpId,
  /// Persist-before-ADVERTISE for the lease promise; exact mirror of the term machinery above.
  /// The in-memory lease-support floor: the max lease window this node will uphold this incarnation =
  /// `max(recovered durable floor, this run's own election_timeout if it enforces)`. Bumped at most ONCE
  /// per incarnation (election_timeout is process-constant); persisted to `HardState.lease_support` so the
  /// restart vote fence honors the PRE-CRASH promise regardless of post-restart config. `None` = no
  /// enforcing promise (fresh / legacy / never-enforced); `Some(d)` = a real promise of `d`.
  lease_support_floor: Option<core::time::Duration>,
  /// The highest lease-support floor ever submitted to the `StableStore` (paired with
  /// `lease_support_persist_opid`, exactly like `last_submitted_term`/`term_persist_opid`).
  last_submitted_lease_support: Option<core::time::Duration>,
  /// The highest lease-support floor whose HardState write has reached stable storage. A follower must NOT
  /// advertise its real `lease_support` until `durable_lease_support >= Some(this_run)` — otherwise a crash
  /// in the fsync window erases a promise the leader already counted toward a live lease (persist-before-
  /// advertise, the lease sibling of the term-before-respond gate). Seeded to the recovered durable floor.
  durable_lease_support: Option<core::time::Duration>,
  /// The `OpId` of the FIRST HardState write that carried `last_submitted_lease_support`. When a `Wrote`
  /// completion with `opid >= lease_support_persist_opid` arrives, that floor is durable (advanced in
  /// [`on_stable_wrote`], releasing the persist-before-advertise gate in `on_heartbeat`).
  lease_support_persist_opid: crate::OpId,
  /// A SUCCESS `AppendResp` deferred until `self.term` is durable — `(leader, term, proven)`
  /// where `proven` is the highest log index the leader's RPC(s) actually MATCHED on this follower. A
  /// follower must not RESPOND to an AppendEntries under a term whose HardState write is not yet durable
  /// (Raft §5.1: persist `currentTerm` before responding to RPCs). Flushed in `on_stable_wrote` as
  /// `proven.min(ack_watermark())` — `proven` caps to what the leader matched (never over-ack a durable-
  /// but-divergent tail) and `ack_watermark()` caps to durability. A superseded-term tag
  /// is dropped; same-`(leader, term)` deferrals keep the MAX proven extent (acks are cumulative).
  term_gated_append_ack: Option<(I, crate::Term, Index)>,
  /// A SUCCESS `SnapshotResp` deferred until `self.term` is durable — the snapshot analogue of
  /// `term_gated_append_ack` (`proven` = the snapshot boundary / committed match).
  term_gated_snapshot_ack: Option<(I, crate::Term, Index)>,
  /// Log index of the most recently appended (not-yet-applied) `ConfChange` entry.
  ///
  /// Initialized to `Index::ZERO` in both `new` and `restart`. On restart, ZERO is acceptable
  /// — a more precise scan of the durable log to find any pending ConfChange entry is a
  /// future refinement. If a ConfChange entry is in the log but not yet applied after restart,
  /// the one-in-flight guard will be permissive (ZERO <= applied), but correctness is maintained
  /// because the entry will still be applied exactly once in `apply_committed`.
  pending_conf_index: Index,
  /// The index of the last appended `SetReadMode` entry — the one-in-flight guard for read-mode
  /// migrations (mirror [`pending_conf_index`](Self::pending_conf_index)). `> applied` ⇒ a migration is
  /// still in flight; recomputed to `last` at `become_leader` so an inherited uncommitted SetReadMode in a
  /// fresh leader's tail blocks a new proposal until it commits-and-applies.
  pending_read_mode_index: Index,
  /// ReadIndex tracking (pending reads, heartbeat-ack sets, confirmed read states).
  read_only: ReadOnly<I>,
  /// The ACTIVE read mode the serve dispatch + stamp helpers consult. Seeded from `config.read_only()`,
  /// then overwritten apply-time when a committed `SetReadMode` entry applies (a mid-life migration);
  /// recovered from replicated state (snapshot ⊔ tail-replay) on restart. `Config.read_only` stays the
  /// immutable genesis default + knob source — only this active mode migrates. The fold floors stay
  /// mode-INDEPENDENT, so a mid-flip / migrated-away node is strictly conservative on the commit-wait.
  active_read_mode: crate::ReadOnlyOption,
  /// Whether a committed `SetReadMode` has EVER applied on this node (read-mode provenance). Gates whether
  /// `maybe_snapshot` carries an EXPLICIT `SnapshotMeta.read_only`: a migrated node records the mode (so
  /// restart recovers it), a NON-migrated node leaves it absent (so restart falls back to the static
  /// config rather than pinning the active mode across a config edit). Recovered at restart from the
  /// snapshot's presence ⊔ the committed-tail replay; adopted from the snapshot's presence on install.
  read_mode_migrated: bool,
  /// Deferred read requests that arrived before the leader has committed an entry in its
  /// current term.  Flushed once `maybe_advance_commit` advances `self.commit` to a
  /// current-term entry.
  ///
  /// Each element is `(context, from)` matching `add_request`'s signature.
  pending_reads: std::vec::Vec<(Bytes, Option<I>)>,
  /// Read contexts this node (as a FOLLOWER) has forwarded to its current leader and is still
  /// awaiting a `ReadIndexResp` for. The follower-side mirror of the leader's
  /// `read_context_in_flight` guard: a duplicate forward for an in-flight context is rejected with
  /// `DuplicateContext` instead of being silently coalesced (or unboundedly re-forwarded), so the
  /// originator is never left waiting on a confirmation the first forward already owns. Removed on
  /// the matching `ReadIndexResp`, FIFO-evicted at [`MAX_FORWARDED_READS`] (so dropped reads cannot
  /// grow it without bound), and cleared wholesale on any term change or leader change (a read
  /// forwarded to a now-stale leader must not block re-issuing it to the new one).
  forwarded_reads: ForwardedReads,
  /// The leader's CURRENT CheckQuorum lease round (bumped on every heartbeat broadcast). Carried in
  /// each `Heartbeat` and echoed in `HeartbeatResp`, it is what makes the LeaseBased read lease
  /// FRESH: only a `HeartbeatResp` echoing this exact round counts toward renewing the lease, so a
  /// stale or duplicated earlier-round response cannot keep an isolated leader's lease alive
  /// round response cannot keep an isolated leader's lease alive. Meaningful only while leader.
  lease_round: u64,
  /// The instant the CURRENT `lease_round`'s heartbeat was SENT (set in `broadcast_heartbeat` when the
  /// round is bumped). The lease is renewed to `lease_round_start + election_timeout`, NOT
  /// `response_receipt + election_timeout`: followers reset their election timers when they RECEIVED
  /// this round (≈ its send time), so the lease must expire by then — measuring from a (possibly
  /// delayed) response would over-extend the lease past the quorum's election window.
  lease_round_start: Instant,
  /// Voters that ENFORCE the lease and have acked the CURRENT `lease_round` (the leader counts itself
  /// implicitly). Cleared on every heartbeat broadcast (each round must be freshly re-confirmed). When
  /// this set plus self forms a voter quorum, the read lease (`lease_valid_until`) is renewed. A
  /// non-enforcing follower (HeartbeatResp `lease_support == 0`) is NOT inserted here, so it cannot keep
  /// the lease alive.
  lease_acks: BTreeSet<I>,
  /// The MINIMUM lease-support duration advertised across the contributing quorum this round
  /// (reset to the leader's OWN `election_timeout` when the round is bumped in `broadcast_heartbeat`,
  /// then min'd with each enforcing ack's `lease_support`). The lease is renewed to `lease_round_start +
  /// lease_min_support`, so a voter with a SHORTER `election_timeout` (heterogeneous config) caps the
  /// lease at its actual support — the leader never out-lives the quorum's real election window.
  lease_min_support: core::time::Duration,
  /// The read lease deadline for `ReadOnlyOption::LeaseBased`: the leader may serve a read from its
  /// local commit WITHOUT a per-read round-trip while `now < lease_valid_until`. Renewed to
  /// `lease_round_start + election_timeout` only when a quorum FRESHLY acks the current `lease_round` —
  /// NOT from the (spoofable) `recent_active`/`election_deadline` CheckQuorum step-down signal, and NOT
  /// from response-receipt time. `None` until the first fresh quorum confirmation. (The residual
  /// clock-drift assumption common to all lease reads remains — see `do_leader_read`.)
  lease_valid_until: Option<Instant>,
  /// Post-restart vote-suppression fence (LeaseBased crash-safety). A node that crashed may have
  /// acked a leader's read-lease just before crashing; on restart that in-memory promise is gone, so
  /// without a fence it could grant a vote to a new candidate and elect a new leader WHILE the old
  /// leader is still inside its (unexpired) lease window — letting the old leader serve a stale
  /// LeaseBased read. While `now < lease_vote_fence_until` a restarted node REFUSES to grant votes
  /// (real and pre), UNLESS the request is a forced leader-transfer (mirrors the `in_lease` bypass).
  /// `restart` sizes it as `now + the DURABLE lease-support floor` (`HardState.lease_support`,
  /// monotone-max'd with this run's own support), so it honors the PRE-CRASH promise regardless of the
  /// post-restart config — closing config drift (a restart with a shorter `election_timeout` or
  /// enforcement disabled) by construction. `None` only when no enforcing promise is on record and this
  /// run enforces nothing. Safety rests on `now >= the pre-crash ack instants` (follower clock
  /// monotonicity across restart) — the irreducible clock residual common to all lease reads.
  lease_vote_fence_until: Option<Instant>,
  /// Whether a forced leader handoff (`TimeoutNow`) was emitted during the CURRENT leader term.
  /// Sending `TimeoutNow` authorizes the transferee to campaign FORCED (bypassing the follower/restart
  /// lease fences), and under Raft's unbounded message delay that forced campaign — or its already-sent
  /// forced `RequestVote`s — can elect a new leader at ANY later point this term, even AFTER the transfer
  /// aborts (`lead_transferee` is cleared on the deadline). So once set, this leader MUST NOT serve a
  /// LeaseBased read for the rest of the term (`do_leader_read` Safe-degrades); it regains the lease
  /// shortcut only on re-election (a fresh term resets this in `become_leader`). Meaningful only while
  /// leader. (The `lead_transferee` gate covers the lagging-transfer window BEFORE `TimeoutNow` is
  /// sent; this flag covers everything AFTER it, including post-abort.)
  forced_handoff_this_term: bool,
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
  pub fn new(config: Config<I>, now: impl Into<Now>, seed: u64, fsm: F) -> Self {
    let now: crate::Now = now.into();
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
      durable_index: Index::ZERO,
      durable_snapshot_index: Index::ZERO,
      pending_install: None,
      prng: Prng::new(seed),
      votes: BTreeMap::new(),
      election_deadline: None,
      heartbeat_deadline: None,
      // Set fresh at each `become_leader` (LeaseGuard only); a non-leader never gates commit.
      commit_wait_until: None,
      // Fresh node, empty log: no inherited lease window to cover yet. Raised as entries arrive.
      max_lease_window: 0,
      max_wall_plus_window: 0,
      max_unwalled_lease_window: 0,
      // Precise failover commit-anchor captures — armed only at become_leader.
      inherited_release_deadline: 0,
      unwalled_commit_wait_until: None,
      precise_releases: 0,
      unprovable_floor_holds: 0,
      cold_read_defers: 0,
      // Inherited-read serve anchors — armed only at become_leader.
      limbo_upper: Index::ZERO,
      committed_anchor_wall: 0,
      committed_anchor_window: 0,
      inherited_serve_armed: false,
      commit_wait_inflated: false,
      // No read has found the lease stale yet (set by a degraded LeaseGuard read; only a leader acts).
      lease_refresh_wanted: false,
      // No read since the (not-yet-existent) anchor — the proactive-refresh gate starts clear.
      read_since_anchor: false,
      // The active read mode starts as the genesis config default; a committed SetReadMode migrates it.
      active_read_mode: read_only_opt,
      read_mode_migrated: false,
      outgoing: VecDeque::new(),
      events: VecDeque::new(),
      tracker,
      next_op_id: crate::OpId::ZERO,
      pending: BTreeMap::new(),
      inflight_append_upto: BTreeMap::new(),
      poisoned: false,
      poison_reason: None,
      pending_compact: None,
      snapshot_resend_after: BTreeMap::new(),
      // fresh node at Term::ZERO — trivially "durable" (nothing to persist), so
      // `term_is_durable()` is true and acks are never spuriously deferred at startup.
      durable_term: Term::ZERO,
      last_submitted_term: Term::ZERO,
      term_persist_opid: crate::OpId::ZERO,
      // a fresh node has made no lease promise. `new()` has no StableStore, so the floor is recorded
      // lazily on the first enforcing heartbeat (bumped in `on_heartbeat`, persisted by the post-dispatch
      // `ensure_term_durable`); until then `durable_lease_support` is None and the advertise gate emits ZERO.
      lease_support_floor: None,
      last_submitted_lease_support: None,
      durable_lease_support: None,
      lease_support_persist_opid: crate::OpId::ZERO,
      term_gated_append_ack: None,
      term_gated_snapshot_ack: None,
      pending_conf_index: Index::ZERO,
      pending_read_mode_index: Index::ZERO,
      read_only: ReadOnly::new(read_only_opt),
      pending_reads: std::vec::Vec::new(),
      // Fresh node: boot epoch 0. A later restart provides a strictly-higher epoch, so this
      // incarnation's forwarded-read tokens can never collide with a post-restart incarnation's.
      forwarded_reads: ForwardedReads::new(0),
      lease_round: 0,
      lease_round_start: now.mono(),
      lease_acks: BTreeSet::new(),
      lease_min_support: core::time::Duration::ZERO,
      lease_valid_until: None,
      // A fresh node never acked any leader's read-lease, so no post-restart vote fence.
      lease_vote_fence_until: None,
      // No forced handoff authorized yet.
      forced_handoff_this_term: false,
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

  /// The LeaseGuard append-timestamp to stamp on a new leader entry: the leader's clock (nanos
  /// since its ORIGIN) when `read_only = LeaseGuard`, else `0` (the field is then absent on the
  /// wire and ignored). Cross-node comparability is the deployment's synchronized-origin
  /// assumption (see WIRE.md); within the sim and a single node it is the monotonic clock.
  ///
  /// `0` if `since_origin` exceeds the `u64` wire field (a >584-year incarnation): the read gate then
  /// measures the entry's age as `now − 0` (huge) and degrades to Safe — FAIL-CLOSED, never wrapping
  /// the truncated timestamp to a falsely-fresh value.
  pub(crate) fn lease_stamp(&self, now: Instant) -> u64 {
    if self.active_read_mode == crate::ReadOnlyOption::LeaseGuard {
      u64::try_from(now.since_origin().as_nanos()).unwrap_or(0)
    } else {
      0
    }
  }

  /// The LeaseGuard commit-wait window (the exact `Δ·(Δ+ε)/(Δ−ε)`, nanos; see
  /// [`Config::clock_drift_bound`]) this leader stamps into every entry it appends — how long a
  /// successor must wait, from a lower bound on the entry's creation, to cover THIS leader's
  /// read-lease on it. `0` (proto-omitted) when
  /// LeaseGuard is inactive/invalid: such a leader serves no lease reads, so no successor need wait
  /// for it (a `0` contribution to the inherited max is correct, not just safe).
  pub(crate) fn lease_window_stamp(&self) -> u64 {
    // The EXACT commit-wait window `Δ·(Δ+ε)/(Δ−ε)`; the single source of truth lives in `Config` so
    // stamping, the read gate, and validation never diverge. `0` when LeaseGuard is inactive/invalid.
    self
      .config
      .leaseguard_commit_wait_ns(self.active_read_mode)
      .unwrap_or(0)
  }

  /// The LeaseGuard FAILOVER wall stamp for a new leader entry: the leader's SYNCHRONIZED wall
  /// reading (nanos since the cluster epoch) when the failover tier is ACTIVE, else `0` (absent on the
  /// wire). Distinct from [`lease_stamp`](Self::lease_stamp), which reads the per-node MONOTONIC clock
  /// for the same-leader gate; this is the CROSS-LEADER wall the inherited-read / precise-anchor tier
  /// compares.
  ///
  /// Gated on the centralized [`Config::failover_tier_valid`](crate::Config::failover_tier_valid) (the
  /// SAME predicate the runtime `failover_tier_active`, the serve/arming, and `Config::validate` use —
  /// called directly here as this impl block's bounds do not include the `failover_tier_active`
  /// convenience) — NOT the raw `bounded_clock_uncertainty` option. Otherwise a config the crate REJECTS
  /// (e.g. valid timing but
  /// `ε_unc ≥ Δ`) would still emit nonzero `wall_timestamp`s, which a VALID successor would fold into
  /// `max_wall_plus_window` and trust as an inherited-read / release horizon — a rejected config seeding
  /// the failover tier. FAIL-CLOSED: if the tier is active but the caller supplied no wall
  /// (`Now::monotonic`), this returns `0` and the read path degrades to Safe — never a falsely-fresh
  /// stamp. The `debug_assert` makes that misconfiguration LOUD in test/debug builds.
  pub(crate) fn lease_wall_stamp(&self, now: crate::Now) -> u64 {
    if self.config.failover_tier_valid(self.active_read_mode) {
      debug_assert!(
        !now.wall().is_absent(),
        "LeaseGuard failover tier is active but the caller supplied no synchronized wall (Now::monotonic)"
      );
      now.wall().as_nanos()
    } else {
      0
    }
  }

  /// The validated LeaseGuard `(lease_duration Δ, clock_drift_bound ε)` when the mode is ACTIVE, else
  /// `None`. Active means the config yields a valid commit-wait window — both knobs present, `ε < Δ`,
  /// and the exact window `Δ·(Δ+ε)/(Δ−ε)` fits the `u64` field AND is below the election timeout (the
  /// single check lives in [`Config::leaseguard_commit_wait_ns`]). The mode serves lease reads,
  /// stamps the per-entry window, and arms the commit-wait ONLY when active; an invalid/incomplete
  /// config DEGRADES TO SAFE — a missing knob is never coerced to zero, no lease fast-path runs.
  ///
  /// The `window < election_timeout` bound is a LIVENESS guard (a fresh leader commits before a
  /// follower could depose it). Cross-leader SAFETY (covering a deposed leader's lease) rests on the
  /// per-entry SELF-DESCRIBING window (a successor waits the inherited MAX), needing no assumption
  /// about any other node's config. Gated HERE, not only in the optional `Config::validate`, so an
  /// unvalidated config degrades to Safe. Returns `(Δ, ε)` for the read gate's same-leader check.
  pub(crate) fn leaseguard_timing(&self) -> Option<(core::time::Duration, core::time::Duration)> {
    self
      .config
      .leaseguard_commit_wait_ns(self.active_read_mode)?;
    Some((
      self.config.lease_duration()?,
      self.config.clock_drift_bound()?,
    ))
  }

  /// The read mode CURRENTLY IN EFFECT — the last applied `SetReadMode` mode, or the `Config` default if
  /// none. The serve dispatch and the stamp helpers consult this, NOT the immutable `Config.read_only`.
  #[inline(always)]
  pub const fn active_read_mode(&self) -> crate::ReadOnlyOption {
    self.active_read_mode
  }

  #[cfg(test)]
  pub(crate) fn set_active_read_mode_for_test(&mut self, mode: crate::ReadOnlyOption) {
    self.active_read_mode = mode;
  }

  #[cfg(test)]
  pub(crate) fn lease_valid_until_for_test(&self) -> Option<Instant> {
    self.lease_valid_until
  }

  #[cfg(test)]
  pub(crate) fn set_lease_valid_until_for_test(&mut self, until: Option<Instant>) {
    self.lease_valid_until = until;
  }

  #[cfg(test)]
  pub(crate) fn inject_pending_read_for_test(&mut self, context: bytes::Bytes) {
    let leader = self.config.id();
    let _ = self
      .read_only
      .add_request(self.commit, context, None, leader);
  }

  #[cfg(test)]
  pub(crate) fn pending_read_count(&self) -> usize {
    self.read_only.pending_len()
  }

  /// The SINGLE leader-belief mutation point. Assigns the new belief and, exactly when the
  /// identity changes, clears reads forwarded to the previous leader (the forward target is
  /// gone; re-issue against the new belief) and emits [`LeaderChanged`](crate::LeaderChanged)
  /// carrying the CURRENT term — so callers adopting a term alongside the leader must bump
  /// `self.term` FIRST. `None` transitions are emitted like any other: a campaign start, a
  /// check-quorum step-down, a higher-term adoption, and a leader's self-removal all make a
  /// known leader unknown, and an embedder routing on the hint must hear about it rather than
  /// infer it from silence. A higher-term message from a leader therefore surfaces an ordered
  /// PAIR in one drain — `(term, None)` at adoption, then `(term, Some(sender))` from the
  /// handler — the honest transition sequence, deduplicated only on identity.
  pub(crate) fn set_leader(&mut self, leader: Option<I>) {
    if self.leader == leader {
      return;
    }
    self.leader = leader;
    self.forwarded_reads.clear();
    self
      .events
      .push_back(crate::Event::LeaderChanged(crate::LeaderChanged::new(
        self.term, leader,
      )));
  }

  /// The current commit index — the highest log index this node believes is committed
  /// (durably replicated to a quorum and safe to apply).
  ///
  /// Read-only observability accessor: exposes the in-memory `commit` watermark for
  /// verification harnesses (the simulator's per-tick safety oracles read it to check
  /// commit monotonicity, quorum-durability, and the recovered-commit durability
  /// invariant). The proto never mutates state through this; it is a pure observer.
  #[inline(always)]
  pub const fn commit_index(&self) -> Index {
    self.commit
  }

  /// How many times the FAILOVER-tier PRECISE commit-anchor lifted the post-election commit-wait on
  /// this node (vs the conservative mono deadline) over its lifetime. Read-only observability (see
  /// [`commit_index`](Self::commit_index)); never persisted, reset to `0` on restart, `0` outside the
  /// failover tier. The randomized tester reads it to confirm the early-release path is non-vacuous.
  #[inline(always)]
  pub const fn precise_releases(&self) -> u64 {
    self.precise_releases
  }

  /// How many times the post-election commit-wait HELD because the inherited walled lease floor was
  /// UNPROVABLE — no synchronized wall on the release path, or no bounded clock-uncertainty to wall-gate.
  /// Read-only observability (see [`commit_index`](Self::commit_index)); never persisted, reset to `0` on
  /// restart, `0` with no inherited walled lease and for a healthy failover node whose wall is always
  /// supplied — but nonzero on a no-ε_unc node that inherited walled entries (outside the active failover
  /// tier). Such a hold is FAIL-CLOSED and
  /// SAFE but does NOT self-resolve, so a steadily climbing value flags an otherwise-silent commit-wait
  /// wedge — a driver that armed the failover tier without supplying a wall to every release path, or a
  /// node outside the synchronized-clock contract that inherited walled entries. The randomized tester
  /// reads it to assert the failover commit-wait does not silently wedge under a faithful clock.
  #[inline(always)]
  pub const fn unprovable_floor_holds(&self) -> u64 {
    self.unprovable_floor_holds
  }

  /// Cold-read wedge counter: how many times `apply_committed` deferred on a cold
  /// ([`EntriesRead::Pending`](crate::EntriesRead::Pending)) committed-range read. `0` for a
  /// fully-resident store; a steadily climbing value flags a store that returns `Pending` for a range
  /// it never makes resident (the cold-read liveness obligation broken). Diagnostic only — a cold read
  /// is not a fault, so it never poisons.
  #[inline(always)]
  pub const fn cold_read_defers(&self) -> u64 {
    self.cold_read_defers
  }

  /// The current applied index — the highest log index this node has applied to its
  /// state machine. Always `applied <= commit_index()`.
  ///
  /// Read-only observability accessor (see [`commit_index`](Self::commit_index)). Used by
  /// verification harnesses to relate the state machine's progress to the commit watermark.
  #[inline(always)]
  pub const fn applied_index(&self) -> Index {
    self.applied
  }

  /// The application state machine (read-only access for agreement checks).
  #[inline]
  pub const fn state_machine(&self) -> &F {
    &self.fsm
  }

  /// Next outbound message, if any.
  ///
  /// **Driver drain obligation.** The endpoint queues outbound messages and application events in
  /// unbounded `VecDeque`s and tracks in-flight storage ops in unbounded maps; nothing inside the pure
  /// state machine bounds them (a Sans-I/O core applies backpressure nowhere). A driver MUST drain
  /// `poll_message` and [`poll_event`](Self::poll_event) — and call
  /// [`handle_storage`](Self::handle_storage) so storage ops complete — between dispatches, or these
  /// queues grow without bound under a fast input rate, a slow store, or a stalled poll loop. A debug
  /// build trips an assertion at the next dispatch if they run away.
  #[inline]
  pub fn poll_message(&mut self) -> Option<Outgoing<I>> {
    // A poisoned node emits nothing — not even already-queued messages. The emit-halt must live at
    // the EGRESS (here), not only at `send`'s enqueue: a handler can queue a message (e.g. a leader
    // broadcasts heartbeats) and then hit a fatal op later in the SAME dispatch, and those queued
    // messages must never reach the wire from a dead node.
    if self.poisoned {
      return None;
    }
    self.outgoing.pop_front()
  }

  /// Next application event, if any.
  #[inline]
  pub fn poll_event(&mut self) -> Option<Event<I, F::Response>> {
    // Same egress emit-halt as `poll_message`: a poisoned node completes no reads and surfaces no
    // events (a queued `ReadState` from before a mid-dispatch poison must not leak).
    if self.poisoned {
      return None;
    }
    self.events.pop_front()
  }

  /// Debug-only tripwire for the driver's drain obligation (see [`poll_message`](Self::poll_message)):
  /// the unbounded work queues — outbound messages, application events, and the in-flight storage-op maps
  /// — should never approach this in correct operation, so crossing it signals a driver that is not
  /// draining `poll_message` / `poll_event` or not calling `handle_storage`. Checked at each dispatch
  /// start, so it measures what a correct driver should already have drained. Far above any legitimate
  /// burst (peers × batches, or in-flight storage ops); a no-op in release (`debug_assert!`).
  #[inline]
  fn debug_assert_queues_drained(&self) {
    const TRIPWIRE: usize = 1 << 20;
    debug_assert!(
      self.outgoing.len() < TRIPWIRE
        && self.events.len() < TRIPWIRE
        && self.pending.len() < TRIPWIRE
        && self.inflight_append_upto.len() < TRIPWIRE,
      "endpoint work queues exceeded the drain tripwire (outgoing={}, events={}, pending={}, \
       inflight={}) — a driver MUST drain poll_message/poll_event and call handle_storage between \
       dispatches (see poll_message)",
      self.outgoing.len(),
      self.events.len(),
      self.pending.len(),
      self.inflight_append_upto.len(),
    );
  }

  /// Test-only: drain all pending events, returning whether ANY was an `Event::ReadState`.
  /// Used by the poison / read-validation regressions to assert a read did (not) complete.
  #[cfg(test)]
  fn poll_all_events_any_read_state(&mut self) -> bool {
    let mut any = false;
    while let Some(e) = self.poll_event() {
      any |= e.is_read_state();
    }
    any
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
    // A poisoned node has nothing to service; returning `None` also avoids a busy-loop where a
    // driver re-feeds a stale deadline that `handle_timeout` no-ops without re-arming.
    if self.poisoned {
      return None;
    }
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

  /// The armed deadline for the given timer kind, regardless of whether it is serviceable now.
  fn deadline_of(&self, kind: TimerKind) -> Option<Instant> {
    match kind {
      TimerKind::Election => self.election_deadline,
      TimerKind::Heartbeat => self.heartbeat_deadline,
      TimerKind::Transfer => self.transfer_deadline,
      TimerKind::CommitWait => self.commit_wait_until,
    }
  }

  /// Whether the current `(role, state)` will service `kind` in `handle_timeout`.
  ///
  /// This is the exact mirror of `handle_timeout`'s dispatch conditions:
  /// - A POISONED node services NOTHING — `handle_timeout` (like every `handle_*`) early-returns on
  ///   poison. Surfacing a deadline a poisoned node will never act on wedges the event-driven driver:
  ///   it advances `now` to that deadline, the timeout fires as a no-op, the deadline stays due, and
  ///   the clock can never advance past it — freezing the WHOLE cluster (no other node's timer can
  ///   fire). A poisoned node is revived only by an external `restart`, not by a timer (a poisoned,
  ///   already-removed voter that froze `now` would starve every election).
  /// - `Heartbeat`: the leader always services its heartbeat deadline.
  /// - `Election`: the leader services it only when `check_quorum` is enabled (CheckQuorum
  ///   tick); a follower/candidate services it only when it is a voter (non-voters never
  ///   campaign, so their election timer firing is a silent no-op — we should not surface it).
  /// - `Transfer`: the leader services it only when a leader transfer is in progress.
  fn serviceable_now(&self, kind: TimerKind) -> bool {
    if self.poisoned {
      return false;
    }
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
      // Only a leader with an armed post-election commit-wait services it. `commit_wait_until` is
      // set to `Some` exclusively in LeaseGuard mode (at `become_leader`), so this is implicitly
      // gated on the mode — a Safe/LeaseBased leader never arms it and never surfaces the timer.
      TimerKind::CommitWait => self.role.is_leader() && self.commit_wait_until.is_some(),
    }
  }

  /// The single outbound choke-point. A poisoned node emits NOTHING: a fatal fault can strike
  /// mid-handler (e.g. `apply_committed` poisons inside `on_heartbeat`/`on_append_entries`, after
  /// which the handler would otherwise still queue a `HeartbeatResp`/`AppendResp`), and a poisoned
  /// node that keeps replying to peers — acking entries it can no longer guarantee, granting reads it
  /// cannot confirm — is a safety hazard, not merely a dead node. Suppressing centrally here covers
  /// every message kind (HeartbeatResp/AppendResp/AppendEntries/VoteResp/ReadIndex(Resp)/…) without a
  /// per-handler guard. `poison()` only sets a flag and emits no event, so this drops the message
  /// silently; the driver surfaces the fault via `poison_reason()`.
  fn send(&mut self, to: I, msg: Message<I>) {
    if self.poisoned {
      return;
    }
    self.outgoing.push_back(Outgoing::new(to, msg));
  }

  fn peers(&self) -> impl Iterator<Item = I> {
    let me = self.config.id();
    // Iterate all tracked IDs (voters both halves ∪ learners ∪ learners_next), excluding self.
    // The leader replicates to learners too; quorum is still computed over voters only
    // (tracker.quorum_committed / tracker.vote_result read only the voter halves).
    self.tracker.ids().into_iter().filter(move |&p| p != me)
  }

  /// The single term-read choke-point. `LogStore::term` returning `Err` is a FATAL storage failure
  /// (per the trait contract) — never "absent" — so every term read in the core funnels through here:
  /// on `Err` the node poisons (`PoisonReason::LogTerm`) and returns `None`, and the caller
  /// short-circuits. This replaces the scattered `log.term(idx).unwrap_or(<default>)` reads, each of
  /// which silently swallowed a fatal error into a fabricated default — the defect class behind the
  /// `last_log` and `on_append_entries` term reads. An index that legitimately has no
  /// entry (index 0, out of range, compacted) is the store's job to answer with `Ok`; `Err` is
  /// reserved for I/O failure, and there is exactly one correct response to that: poison.
  fn log_term<L: LogStore>(&mut self, log: &L, idx: Index) -> Option<Term> {
    match log.term(idx) {
      Ok(t) => Some(t),
      Err(_) => {
        self.poison(PoisonReason::LogTerm);
        None
      }
    }
  }

  /// Our log's `(last_index, last_term)` for the §5.4.1 up-to-date comparison, or `None` on a genuine
  /// storage error reading the last term of a NON-empty log (the node is poisoned via
  /// [`Self::log_term`]).
  ///
  /// An empty log (`last_index == 0`) legitimately has last term `0`. A term-READ FAILURE on a
  /// non-empty log poisons rather than fabricating a stale `Term::ZERO` (which could make us grant a
  /// vote to a candidate whose log is actually staler than ours — a leader-completeness hazard).
  fn last_log(&mut self, log: &impl LogStore) -> Option<(Index, Term)> {
    let li = log.last_index();
    if li == Index::ZERO {
      return Some((Index::ZERO, Term::ZERO));
    }
    self.log_term(log, li).map(|lt| (li, lt))
  }

  /// The leader's replication [`PeerProgress`] for `peer` (its match/next index, flow-control state,
  /// and whether it is paused), or `None` if `peer` is not a tracked member. A pure read of internal
  /// state for status / observability; only meaningful while this node is the leader.
  pub fn peer_progress(&self, peer: &I) -> Option<PeerProgress> {
    self.tracker.progress(peer).map(|p| PeerProgress {
      match_index: p.match_index(),
      next_index: p.next_index(),
      state: p.state(),
      paused: p.is_paused(),
    })
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
  #[cfg(test)]
  pub(crate) fn mint_op_id_for_test(&mut self) -> crate::OpId {
    self.mint_op_id()
  }

  /// Feed an inbound message. Runs the universal term pre-pass then dispatches.
  pub fn handle_message<L, S>(
    &mut self,
    now: impl Into<Now>,
    log: &mut L,
    stable: &mut S,
    from: I,
    msg: Message<I>,
  ) where
    L: LogStore,
    S: StableStore<NodeId = I>,
    F::Snapshot: crate::Data,
  {
    let now: crate::Now = now.into();
    if self.poisoned {
      return;
    }
    self.debug_assert_queues_drained();
    // Sender-authenticity choke-point: reject any message whose self-reported sender disagrees with
    // the transport peer it actually arrived from. This single check closes payload-sender spoofing
    // for EVERY message type — past this point vote tallies, append acks, and read confirmations may
    // all trust `*.from()` because it provably equals the transport sender.
    if msg.from() != from {
      return;
    }
    // Universal term handling (Raft §5.1): a higher term forces us to a follower.
    // Exception (PreVote anti-disruption): pre-vote traffic that carries an *advertised* term
    // that has not been adopted — do NOT step down or adopt it.
    if msg.term() > self.term {
      // A pre-vote REQUEST, and a GRANTED pre-vote response, both echo the pre-candidate's
      // *advertised* future term (it has only proposed it; it adopts only once a quorum grants) —
      // adopting it would let a partitioned node's pre-votes raise the cluster term, the very
      // disruption PreVote exists to prevent. But a REJECTED pre-vote response carries the
      // responder's REAL current term (`on_request_vote`: `resp_term = if grant { rv.term() } else
      // { self.term }`), so a pre-candidate that is genuinely behind MUST adopt it. Otherwise, with
      // no third node to bump its term — a 2-voter cluster, or any pair where the peer self-voted at
      // a higher term — it stays a stale PreCandidate forever, re-proposing a term the peer keeps
      // rejecting: a livelock. (Matches etcd, which skips the term bump only for `MsgPreVote` and a
      // granted `MsgPreVoteResp`.)
      let advertised_prevote = matches!(&msg, Message::RequestVote(rv) if rv.pre_vote())
        || matches!(&msg, Message::VoteResp(vr) if vr.pre_vote() && !vr.reject());
      if advertised_prevote {
        // Fall through without adopting the term, stepping down, or persisting.
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
            && self.election_deadline.is_some_and(|d| d > now.mono());
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
        self.set_leader(None);
        // All pending work from the old term is now stale (spec §7). Drop it before any new
        // grant is recorded below — a fresh CastVote added by on_request_vote will survive.
        // (`pending_install` is a SEPARATE field, so it survives this clear — a deferred install
        // whose boundary is already quorum-committed stays valid across a pure term bump; the
        // completion-time staleness re-check in `install_snapshot_now` is the final guard.)
        self.pending.clear();
        // Drop all ReadIndex state too: a stale read confirmation must never leak across a term
        // change. Mirrors `step_down_to_follower` / `become_leader` (read confirmation is
        // leader-gated, so this is robustness, not a behavior change).
        self.read_only.reset(self.active_read_mode);
        self.pending_reads.clear();
        // (Reads forwarded under the old term/leader were cleared by `set_leader` above.)
        // Abort any in-progress leader transfer — leadership is changing.
        self.lead_transferee = None;
        self.transfer_deadline = None;
        // The term step-down is NOT persisted here. Persisting before dispatch would put the
        // term/vote write AHEAD of the message handler's read-only fatal validation, so a fail-stop in
        // that handler (a corrupt RequestVote/AppendEntries/InstallSnapshot) would leave a premature
        // durable term write. Instead `ensure_term_durable` makes the step-down durable AFTER the
        // handler validates and BEFORE any entry/snapshot from this term — called by the entry/snapshot
        // handlers themselves (term-before-entries) and, as the catch-all for the rest, after the
        // dispatch below. Stepping down owes no ack, so there is still no Pending entry.
        // NOTE: a voter that steps down here on a higher-term RESPONSE (whose handler does not
        // re-arm) would be left without an election timer — `reconcile_election_timer`, run at the
        // end of this entry point, restores the invariant. We deliberately do NOT arm inline (that
        // would reset an already-running Follower timer on every higher-term adoption — regressed
        // liveness under an adversarial schedule).
      }
    }
    // Drop messages from a stale term — with two caveats.
    if msg.term() < self.term {
      let is_prevote_req = matches!(&msg, Message::RequestVote(rv) if rv.pre_vote());
      if !is_prevote_req {
        // CheckQuorum / PreVote step-down nudge (etcd): a stale-term Heartbeat or AppendEntries
        // means the sender BELIEVES it is the leader but is behind our term — we advanced (e.g.
        // campaigned) during a partition, then rejoined. It can never replicate to us (we reject
        // its lower-term entries), and we may be too far behind to win an election ourselves, so a
        // silent drop wedges us BOTH forever. Reply with an AppendResp at OUR (higher) term: the
        // stale leader sees the higher term and steps down, and the ensuing election lifts the
        // cluster to our term so the winner can finally replicate to us. Only when CheckQuorum or
        // PreVote is enabled (it is the mechanism those modes rely on; plain Raft has the disruptive
        // higher-term campaign instead). Mirrors etcd's `case m.Term < r.Term` MsgAppResp branch.
        let nudge_step_down = (self.config.check_quorum() || self.config.pre_vote())
          && matches!(&msg, Message::Heartbeat(_) | Message::AppendEntries(_));
        if nudge_step_down {
          // Only `term` (ours, strictly higher) is meaningful — the stale leader adopts it and
          // steps down in its own term pre-pass BEFORE the response body is ever inspected, so the
          // reject hints / match_index are immaterial (sent zeroed).
          let me = self.config.id();
          self.send(
            from,
            Message::AppendResp(crate::AppendResp::new(
              self.term,
              me,
              true,
              Index::ZERO,
              Term::ZERO,
              Index::ZERO,
            )),
          );
        }
        return;
      }
      // Pre-vote request: fall through to on_request_vote, which rejects it (rv.term() < self.term
      // fails the term_ok check) and replies at self.term (etcd: MsgPreVoteResp{Reject:true,
      // Term:r.Term}) so the pre-candidate learns it is behind.
    }

    // CheckQuorum: while the leader, any inbound message from a known peer proves that peer
    // is reachable. Mark it active so it counts toward the next quorum_active check.
    // We do this AFTER the term pre-pass (so a higher-term message that steps us down doesn't
    // mark a peer active on the stale term's leader) and only if we're still the leader.
    if self.role.is_leader()
      && let Some(pr) = self.tracker.progress_mut(&from)
    {
      pr.set_recent_active(true);
    }

    #[allow(unreachable_patterns)] // `_ => {}` is a forward-compat guard for future variants
    match msg {
      Message::RequestVote(rv) => self.on_request_vote(now, log, stable, rv),
      Message::VoteResp(vr) => self.on_vote_resp(now, log, stable, vr),
      Message::Heartbeat(hb) => self.on_heartbeat(now, log, hb),
      Message::AppendEntries(ae) => self.on_append_entries(now, log, stable, ae),
      Message::AppendResp(r) => self.on_append_resp(now, log, stable, from, r),
      Message::HeartbeatResp(hr) => self.on_heartbeat_resp(now, from, log, stable, hr),
      Message::ReadIndex(ri) => self.on_read_index(now, log, stable, ri),
      Message::ReadIndexResp(r) => self.on_read_index_resp(from, r),
      Message::InstallSnapshot(is) => self.on_install_snapshot(now, stable, is),
      Message::SnapshotResp(r) => self.on_snapshot_resp(now, log, stable, from, r),
      Message::TimeoutNow(tn) => self.on_timeout_now(now, log, stable, tn),
      _ => {}
    }

    // Catch-all term-step-down persist: make a just-adopted higher term durable for the handlers
    // that did NOT persist it themselves — a heartbeat, a response, a non-granting RequestVote, a
    // stale-snapshot ack. Runs AFTER the handler's read-only validation, so a fail-stop above persisted
    // nothing (side-effect-free). Idempotent: a no-op for same-term/pre-vote messages and for a grant /
    // entry-append / snapshot-install handler that already persisted the step-down (no double-write).
    self.ensure_term_durable(stable);

    // Invariant restore: a higher-term step-down on a RESPONSE message (handled above) or a
    // conf-change applied by this message may have left a voter without an election timer. The
    // early `return`s above (stale-term drop, in-lease ignore) change neither role nor membership,
    // so they cannot break the invariant and need no reconcile.
    self.reconcile_election_timer(now);
  }

  /// Fire due timers (election for followers/candidates, heartbeat for leaders).
  pub fn handle_timeout<L, S>(&mut self, now: impl Into<Now>, log: &mut L, stable: &mut S)
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let now: crate::Now = now.into();
    if self.poisoned {
      return;
    }
    self.debug_assert_queues_drained();
    match self.role {
      Role::Leader => {
        // Whether the heartbeat is DUE this tick. BOTH lease-refresh blocks below gate on it, so the
        // refresh rate is bounded by the heartbeat cadence no matter how often the embedder calls
        // `handle_timeout` — a fixed-tick caller (ticking faster than `heartbeat_interval`) cannot drive
        // proactive no-ops at caller rate.
        let heartbeat_due = self.heartbeat_deadline.is_some_and(|d| d <= now.mono());
        if heartbeat_due {
          self.broadcast_heartbeat(now);
          self.arm_heartbeat_timer(now);
        }
        // LeaseGuard lease refresh: a recent read found the lease stale and degraded to Safe. Consume
        // the demand (clearing the flag rate-limits to one in-flight no-op) and append ONE stamped no-op
        // iff: the config is active; NO leader transfer is in progress (a refresh would advance
        // last_index AFTER TimeoutNow was sent, stranding the authorized transferee with a now-stale log
        // so it loses the forced election — mirror `propose`'s LeaderTransferInProgress freeze); the log
        // is FULLY committed (last==commit, so nothing pending re-stamps the lease and we never stack a
        // second refresh no-op); and the committed lease is STILL stale (an intervening client write may
        // already have re-stamped it). Replication carries it to the quorum; once it commits, subsequent
        // reads serve fast for Δ. On a fully idle cluster (no reads) the flag is never set ⇒ no write amp.
        if heartbeat_due && self.lease_refresh_wanted {
          self.lease_refresh_wanted = false;
          let last = log.last_index();
          if self.leaseguard_timing().is_some()
            && self.lead_transferee.is_none()
            && last == self.commit
            && !self.lease_guard_read_live(now, log)
          {
            self.append_leader_noop(now, log, last);
          }
        }
        // LeaseGuard PROACTIVE refresh (`LeaseRefresh::OnExpiry` / `Continuous`): re-anchor the lease
        // BEFORE it expires so reads never pay a Safe round — but ONLY when a read has occurred since the
        // current anchor (`read_since_anchor`), so an idle leader refreshes nothing (no write amp). The
        // CHEAP guards (mode active, a read since the anchor, valid timing, no transfer, log fully
        // committed) come FIRST — only then the storage-reading `lease_near_expiry` (which poisons on a log
        // fault) — so a heartbeat never fail-stops on that anchor read during a leader transfer or with an
        // append already in flight, where the `last == commit` / `lead_transferee` guard would suppress the
        // refresh anyway. The `last == commit` guard also makes this mutually exclusive with the demand
        // block above (if it appended, `last != commit` here, so we never stack two).
        let mode = self.config.lease_refresh();
        if heartbeat_due
          && mode != crate::LeaseRefresh::Off
          && self.read_since_anchor
          && self.leaseguard_timing().is_some()
          && self.lead_transferee.is_none()
          && log.last_index() == self.commit
        {
          let fire = match mode {
            crate::LeaseRefresh::Continuous => true,
            crate::LeaseRefresh::OnExpiry => self.lease_near_expiry(now, log),
            crate::LeaseRefresh::Off => false, // unreachable — gated by `mode != Off` above
          };
          if fire {
            self.append_leader_noop(now, log, self.commit);
          }
        }
        // LeaseGuard commit-wait: the post-election deferred-commit window has elapsed. Retry the
        // commit (and apply + flush deferred reads) now, so the new leader's first commit lands as
        // soon as any deposed leader's lease has expired rather than at the next ack/heartbeat.
        // `maybe_advance_commit` clears `commit_wait_until` (the deadline has passed), so the
        // CommitWait timer is left non-serviceable and the wedge tripwire below stays satisfied.
        if self.commit_wait_until.is_some_and(|d| d <= now.mono()) {
          self.maybe_advance_commit(now, log);
          self.apply_committed(log);
          self.maybe_flush_deferred_reads(now, log, stable);
        }
        // Leader transfer abort: if the transfer deadline has passed without the target
        // taking over, abort the transfer and resume accepting proposals.
        if self.lead_transferee.is_some() && self.transfer_deadline.is_some_and(|d| d <= now.mono())
        {
          self.lead_transferee = None;
          self.transfer_deadline = None;
        }
        // CheckQuorum: the leader uses the otherwise-idle election_deadline to run a
        // periodic quorum-activity check every election_timeout. If fewer than a quorum of
        // voters have been recently active (no message from them this window), the leader is
        // likely partitioned from the majority — step down so we stop serving stale reads
        // and allow a reachable node to be elected.
        if self.config.check_quorum() && self.election_deadline.is_some_and(|d| d <= now.mono()) {
          if !self.tracker.quorum_active() {
            self.step_down_to_follower(now);
          } else {
            // Quorum still reachable: reset the activity window and re-arm for the next check.
            let me = self.config.id();
            self.tracker.reset_recent_active(me);
            self.election_deadline = Some(now.mono() + self.config.election_timeout());
          }
        }
      }
      _ => {
        if self.election_deadline.is_some_and(|d| d <= now.mono()) {
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
    // Invariant restore (defense-in-depth; the campaign branch above already arms a voter whose
    // timer fired). Arms to a FUTURE deadline, so it never trips the wedge tripwire below.
    self.reconcile_election_timer(now);
    // Wedge tripwire: after all dispatch, no serviceable timer must still be armed-and-due.
    // If this fires, `serviceable_now` has diverged from the actual dispatch (a branch acted
    // on a timer but forgot to re-arm it to a future instant or clear it).
    debug_assert!(
      TimerKind::ALL.iter().all(|&k| {
        !(self.serviceable_now(k) && self.deadline_of(k).is_some_and(|d| d <= now.mono()))
      }),
      "handle_timeout left a serviceable timer armed-and-due (serviceable_now diverged from dispatch)"
    );
  }

  /// The next FRESH log index to allocate after `last`, or `None` if the index space is exhausted.
  ///
  /// The SINGLE choke-point for allocating a new log slot (leader no-op, `propose`, conf-change). It
  /// reserves `u64::MAX` as a non-allocatable sentinel: every committed entry must be readable via the
  /// half-open ranges `[i, i.next())` (apply) and `[.., last.next())` (replication), which require
  /// `i.next() > i`, i.e. `i < u64::MAX`. Allocating `u64::MAX` would make those ranges empty, so the
  /// entry could be committed yet never applied or replicated — a wedge. `Index::next()` also
  /// saturates at the ceiling, which would alias `last_index` and truncate-replace the existing entry.
  /// So the usable index space is `[1, u64::MAX - 1]`; allocation fails once `last >= MAX - 1`.
  fn next_log_index(last: Index) -> Option<Index> {
    last.checked_next().filter(|i| i.get() != u64::MAX)
  }

  /// Apply all entries that have been committed but not yet applied.
  ///
  /// Every unrecoverable fault here POISONS the node (it does not silently stall): a poisoned
  /// node is inert (`handle_*` are no-ops) and the driver surfaces `poison_reason()` and stops.
  /// This matches the policy of `on_install_snapshot` and the ConfChange Changer-reject arm.
  /// A bare `break` is used ONLY for the benign "committed entry not yet readable" case (the
  /// log slice is empty), which is transient and retried on the next `handle_*`.
  fn apply_committed<L: LogStore>(&mut self, log: &L) {
    // Bound BOTH the per-pass payload bytes AND the entry COUNT so a COLD/disk store returning
    // `Ready(Owned(..))` materializes a bounded amount per call instead of the whole unapplied backlog (a
    // panic-OOM for a node catching up). The byte cap alone is INSUFFICIENT: it charges PAYLOAD bytes, so a
    // backlog of ZERO-payload entries (no-ops, empty/conf) would slip the whole range through — hence the
    // entry-count cap on the requested range width too. The outer `while` re-reads from the new
    // `applied.next()`, so a short prefix just costs another pass; the in-memory borrowed path is unaffected
    // (a small range comes back in one pass).
    const APPLY_READ_MAX_BYTES: u64 = 1 << 20;
    while self.applied < self.commit {
      // Halt the drain the moment the node poisons (including a poison set EARLIER in the same
      // dispatch, e.g. by a storage completion processed just before this call): once fail-stopped,
      // the user FSM must not be re-invoked with further applies.
      if self.poisoned {
        return;
      }
      // ONE byte-capped range fetch per pass (was one call per index), iterated BY REFERENCE (no
      // per-entry clone). A conforming store returns a CONTIGUOUS prefix starting at `applied.next()`
      // (the LogStore::entries contract), capped at `APPLY_READ_MAX_BYTES`; a short prefix is re-fetched
      // by the outer while. An empty slice is the benign "committed entry not yet in the read view" case
      // → break and retry next tick; an Err is a fatal committed-range read fault → poison.
      // Cap the requested range at MAX_READ_BATCH_ENTRIES indices (the entry-count bound).
      let read_end = self.commit.next().min(Index::new(
        self
          .applied
          .get()
          .saturating_add(MAX_READ_BATCH_ENTRIES + 1),
      ));
      let batch = match log.entries(self.applied.next()..read_end, APPLY_READ_MAX_BYTES) {
        Ok(crate::EntriesRead::Ready(e)) if e.is_empty() => break,
        Ok(crate::EntriesRead::Ready(e)) => e,
        // Present but cold: stop applying this pass and retry on the next pump (the store signals
        // storage-ready when the range lands). A cold defer is not a fault — never poison.
        Ok(crate::EntriesRead::Pending) => {
          self.cold_read_defers = self.cold_read_defers.saturating_add(1);
          break;
        }
        Err(_) => {
          self.poison(PoisonReason::LogRead);
          break;
        }
      };
      for entry in &*batch {
        // The apply index is the entry's OWN identity, never a parallel counter. It must be EXACTLY the
        // next committed index: contiguous from `applied.next()` AND not past `commit`. A non-conforming
        // store that returns a gap/duplicate (`idx != applied.next()`), or a slice OVERLONG past the
        // requested `commit.next()` (`idx > commit`, an UNCOMMITTED entry), would otherwise apply the
        // wrong command at the wrong index or fold an uncommitted entry into the FSM/core state — both an
        // out-of-range embedded index, so fail-stop rather than silently corrupt state.
        let idx = entry.index();
        if idx != self.applied.next() || idx > self.commit {
          self.poison(PoisonReason::NonContiguousAppend);
          break;
        }
        match entry.kind() {
          crate::EntryKind::Normal => {
            // Zero-copy: the command decodes from a shared slice of the entry's payload.
            let cmd = match <F::Command as crate::Data>::decode_exact(entry.data_bytes()) {
              Ok(c) => c,
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
          crate::EntryKind::SetReadMode => {
            // Decode the target mode from the EXACTLY-1-byte payload; unrecoverable on failure → poison
            // (mirror ConfChange's exact decode). An empty, trailing-junk, or out-of-range payload is a
            // non-canonical committed entry → fail-stop, never a silent partial decode. This is the
            // apply-time flip — the SOLE site the active mode changes (commit-before-apply means it never
            // takes effect on an uncommitted tail). NO commit-wait re-arm: the monotone fold floors + the
            // mode-independent become_leader arming already cover a deposed LeaseGuard lease (spec §1).
            let data = entry.data();
            let mode = match (data.len() == 1)
              .then(|| data[0])
              .and_then(crate::ReadOnlyOption::from_u8)
            {
              Some(m) => m,
              None => {
                self.poison(PoisonReason::SetReadModeDecode);
                break;
              }
            };
            self.active_read_mode = mode;
            // A committed SetReadMode has applied — record the read-mode provenance so a snapshot at/after
            // this boundary carries the mode EXPLICITLY. A node that NEVER migrated leaves it absent (None),
            // so a restart falls back to the static config rather than pinning the active mode across a
            // config edit (see `maybe_snapshot`).
            self.read_mode_migrated = true;
            // Update the read-mode option WITHOUT discarding in-flight accepted reads (`set_option`, NOT
            // `reset`): a read already accepted at its commit index stays valid and still confirms under the
            // mode-INDEPENDENT ReadIndex heartbeat quorum — clearing it (as a mid-term `reset` would) strands
            // the caller / a forwarding follower on a read `read_index` already accepted. Still revoke any
            // live LeaseBased lease (its granting quorum may not match the new mode) — mirror ConfChange.
            self.read_only.set_option(mode);
            self.lease_valid_until = None;
            self.lease_acks.clear();
            self
              .events
              .push_back(crate::Event::ReadModeChanged(crate::ReadModeChanged::new(
                idx, mode,
              )));
          }
          crate::EntryKind::ConfChange => {
            // Decode the ConfChangeV2 payload. On failure: unrecoverable → poison (mirror Normal).
            let cc = match crate::wire::decode_conf_change_v2::<I>(entry.data_bytes()) {
              Ok(c) => c,
              Err(_) => {
                self.poison(PoisonReason::ConfChangeDecode);
                break;
              }
            };
            // Dispatch to the Changer using the etcd rules (apply-time, spec §9): a committed ConfChange
            // takes effect on the tracker HERE — `apply_committed` is the SOLE fold site, so each change
            // (including a joint enter/leave) is folded exactly once by construction.
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
            } else if cc.transition() != crate::ConfChangeTransition::Auto || cc.changes().len() > 1
            {
              let auto_leave = cc.transition() != crate::ConfChangeTransition::Explicit;
              changer.enter_joint(&self.tracker, auto_leave, cc.changes())
            } else {
              changer.simple(&self.tracker, cc.changes())
            };
            match result {
              Ok(new_tracker) => {
                self.tracker = new_tracker;
                // Membership-change lease revocation: the LeaseBased read lease is safe only because
                // the lease quorum OVERLAPS any new-leader quorum (one shared voter's `in_lease` blocks the
                // disruptive vote) — and that overlap is guaranteed ONLY within a SINGLE configuration. A
                // committed membership change can produce a new config whose quorum is DISJOINT from the
                // quorum that granted the live lease, so the lease no longer proves "no other leader". Revoke
                // it; `do_leader_read` degrades to Safe until a fresh quorum re-confirms under the new config.
                self.lease_valid_until = None;
                self.lease_acks.clear();
                // Prune resend-pacing deadlines to the new membership. A peer this change REMOVED
                // while still in Snapshot state can never be observed leaving it (its Progress is
                // gone, and a dead peer sends no further responses), so its entry would linger for
                // the rest of the term — and add/remove churn of lagging peers would grow the map
                // past the live peer set. This is the sole fold site for configuration changes, so
                // pruning here bounds the map by the tracked peers by construction.
                let tracker = &self.tracker;
                self
                  .snapshot_resend_after
                  .retain(|id, _| tracker.progress(id).is_some());
              }
              // A committed, validly-decoded ConfChange that the Changer rejects is an unrecoverable
              // logic violation (e.g. an overlapping change that should have been prevented upstream).
              // Poison so the failure is detectable rather than a silent apply stall.
              Err(_) => {
                self.poison(PoisonReason::ConfChangeApply);
                break;
              }
            }
            let conf = self.tracker.conf_state();
            self
              .events
              .push_back(crate::Event::ConfChanged(crate::ConfChanged::new(
                idx, conf,
              )));
            // A leader that this change removed (or demoted to learner) is no longer a voter in the
            // new configuration and must stop acting as leader. `is_voter()` checks BOTH joint halves,
            // so during a joint phase where we are still in the outgoing config we keep leading (we must
            // shepherd the joint → simple transition). We only step down once removed from BOTH halves.
            // The step-down is at the SAME term (no term bump): a leader yielding to its own removal,
            // not losing an election.
            if self.role.is_leader()
              && self.config.step_down_on_removal()
              && !self.tracker.is_voter(&self.config.id())
            {
              self.role = Role::Follower;
              self.set_leader(None);
              self.heartbeat_deadline = None;
              // Do NOT arm the election timer: a non-voter must not campaign (see handle_timeout /
              // become_candidate guards). Leaving election_deadline disarmed is the right choice — a
              // removed/demoted node has no business holding an election timer.
              self.election_deadline = None;
              // Abort any in-progress leader transfer — the leader is being removed.
              self.lead_transferee = None;
              self.transfer_deadline = None;
            }
            // If an in-flight leader transfer's target was removed or demoted by this conf change,
            // abort it (the target can no longer be elected, and proposals must not stay blocked until
            // the transfer deadline). Mirrors etcd's abortLeaderTransfer on conf-change apply.
            if self
              .lead_transferee
              .is_some_and(|t| !self.tracker.is_voter(&t))
            {
              self.lead_transferee = None;
              self.transfer_deadline = None;
            }
            // NOTE: a learner promoted to voter by this change may be left without an election timer (a
            // non-voter disarms it and never re-arms). `reconcile_election_timer`, run at the end of the
            // public entry point that drove this apply, restores the invariant — no per-site arm needed.
            // Do NOT call F::apply for ConfChange entries — they advance `applied` only.
          }
        }
        self.applied = idx;
      }
    }
  }
}

#[cfg(test)]
mod tests;
