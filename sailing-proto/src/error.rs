//! Public error types for the core.
use core::time::Duration;

/// Why a proposal was rejected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ProposeError<I> {
  /// This node is not the leader; redirect to `leader` if known.
  #[error("not the leader")]
  NotLeader {
    /// The believed current leader, if known.
    leader: Option<I>,
  },
  /// A previous configuration change is still in flight (not yet applied). Only one
  /// `ConfChange` entry may be pending at a time — propose another after the first is
  /// committed and applied.
  #[error("a conf change is already in flight")]
  ConfChangeInFlight,
  /// A previous read-mode migration is still in flight (not yet applied). Only one `SetReadMode` entry
  /// may be pending at a time — propose another after the first is committed and applied.
  #[error("a read-mode change is already in flight")]
  ReadModeChangeInFlight,
  /// The proposed read mode requires knobs this leader lacks: into-LeaseGuard needs a valid lease window
  /// (`lease_duration` + `clock_drift_bound`), into-LeaseBased needs `check_quorum`. Rejected at propose
  /// time — nothing appended — rather than committed and then degrading to Safe everywhere.
  #[error("the target read mode requires knobs this node lacks")]
  InvalidReadMode,
  /// A leader transfer is in progress; the leader is not accepting new proposals until
  /// the transfer completes or times out.
  #[error("a leader transfer is in progress")]
  LeaderTransferInProgress,
  /// The proposed configuration change is invalid for the current configuration (e.g. leaving a
  /// joint config while not in one, or an overlapping change). It was rejected at propose time —
  /// nothing was appended — rather than being committed and then poisoning the node on apply.
  #[error("the configuration change is invalid for the current configuration")]
  InvalidConfChange,
  /// The node has entered the permanent poisoned state (a fatal storage/apply error) and accepts no
  /// new work. The proposal was NOT appended or persisted; inspect `poison_reason()`.
  #[error("the node is poisoned and accepts no new proposals")]
  Poisoned,
  /// The log index space is exhausted (`last_index == u64::MAX`): no new entry can be allocated a
  /// strictly-greater index without aliasing the existing one. Unreachable by legitimate appends
  /// (2^64 entries); reachable only from a crafted or corrupt recovered log. Nothing was appended.
  #[error("the log index space is exhausted")]
  LogIndexExhausted,

  /// A merge naming this group as its TARGET is in flight (a `CommitMerge` proposed, parked, or
  /// absorbed with its compaction still landing), and membership changes are fenced until it
  /// settles: a replica added inside the window could be log-walked across the absorb point and
  /// silently miss the union. The fence releases on its own within a storage crank of the
  /// resolution; re-propose then.
  #[error("a merge into this group is in flight; membership changes are fenced until it settles")]
  MergeInFlight,
  /// The group is FROZEN by a merge — a `PrepareMerge` has entered its log (append-observed) or
  /// applied — and accepts no new entries: anything appended above the freeze would either
  /// diverge the absorbed state across target replicas (each absorbs its LOCAL source at its own
  /// apply progress) or be silently dropped from the union. The freeze is released only by the
  /// merge completing (the group is then absorbed and gone) or an explicit `RollbackMerge` — the
  /// ONE entry proposable while frozen. Nothing was appended.
  #[error("the group is frozen by an in-flight merge")]
  Frozen,
  /// The proposed entry is too large to ever fit in one transport frame. Accepting it would append a
  /// committed log entry that no `AppendEntries` could carry, so every follower's connection would
  /// close on each resend and replication would wedge cluster-wide. `size` is the entry's worst-case
  /// wire cost and `max` the per-frame entry budget, in bytes. Nothing was appended.
  #[error("the proposed entry is too large for one transport frame ({size} > {max} bytes)")]
  EntryTooLarge {
    /// The entry's worst-case encoded wire cost, in bytes.
    size: usize,
    /// The per-frame entry budget, in bytes.
    max: usize,
  },
}

/// Why [`MultiRaft::create_group`](crate::MultiRaft::create_group) /
/// [`restore_group`](crate::MultiRaft::restore_group) — or a multi coordinator's tombstone-aware
/// wrapper of them — refused to admit a group. The already-hosted groups are left untouched in
/// every case; the moved-in inputs (including the state machine) are dropped — pre-check
/// [`contains_group`](crate::MultiRaft::contains_group) when they must be preserved.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum CreateGroupError {
  /// A group with this id is already hosted.
  #[error("a group with this id already exists")]
  Exists,
  /// The group's `Config` node id differs from the host identity latched by the first group ever
  /// admitted. The latch survives group removal — even of the last group — because a multi-Raft
  /// host is ONE physical node whose live transport connections stay authenticated under that
  /// identity: a divergent per-group id's messages would be silently dropped by every receiver's
  /// sender-authenticity check.
  #[error("the group's node id differs from the host's latched identity")]
  NodeIdMismatch,
  /// The group id's `Data` encoding is outside the wire bound (1..=1024 bytes). An empty encoding
  /// is indistinguishable from the single-group frame tag, and an oversized one would produce
  /// frames every receiver rejects at the group header.
  #[error("the group id's encoding is empty or exceeds the wire bound")]
  InvalidGroupId,
  /// The id is TOMBSTONED by a removal (a coordinator-level refusal): creation refuses until an
  /// explicit `clear_tombstone` consents to re-admission — so a stale unknown-group advisory,
  /// consumed after the embedder retired the id, can never resurrect a removed group by prompting
  /// a naive re-create. Mirrors the references' tombstone-refuses-creation rule (TiKV's
  /// `RaftGroupDeletedError`, CockroachDB's tombstone check): re-admission is never implicit.
  #[error("the group id is tombstoned by a removal; clear the tombstone to re-admit it")]
  Retired,
  /// The requested incarnation is below the id's persisted admission floor — a removal or merge
  /// fenced it. Unlike [`Retired`](Self::Retired), no consent call cures this; only a
  /// catalog-supplied incarnation at or above the floor — and below the reserved `u64::MAX`
  /// sentinel — can ever be admitted, so a floor of
  /// [`MERGED_FLOOR`](crate::MERGED_FLOOR) admits nothing and is the terminal verdict this
  /// variant then reports at every generation, the sentinel itself included.
  #[error("the group id's incarnation is below its admission floor ({floor})")]
  BelowFloor {
    /// The id's persisted admission floor: the smallest incarnation that may ever be admitted.
    floor: u64,
  },
  /// The requested incarnation is `u64::MAX` — the merged-tombstone sentinel
  /// ([`MERGED_FLOOR`](crate::MERGED_FLOOR)), never a working incarnation. Admission reserves
  /// it because the floor predicate fences by `generation < floor`: were the sentinel a legal
  /// generation, it would clear even the terminal `MERGED_FLOOR` fence (`u64::MAX < u64::MAX`
  /// is false) and a buggy catalog could resurrect a merged-away id.
  #[error("the group id's incarnation is the reserved merged-floor sentinel (u64::MAX)")]
  ReservedGeneration,
  /// The id is RESERVED as the child of a split in flight on this host (a coordinator-level
  /// refusal): a proposed-but-unapplied split names it, or a committed fork naming it is
  /// staged awaiting materialization — parked conflicts included. Admitting it now would
  /// manufacture the very conflict the relay must then park around (the committed fork cannot
  /// land while a group occupies its id), so admission fails closed until the fork resolves;
  /// the reservation is derived from live consensus state and releases on its own — no
  /// consent call exists or is needed. Retry after the split materializes (the id then refuses
  /// as [`Exists`](Self::Exists)) or is otherwise resolved.
  #[error("the group id is reserved by an in-flight split; retry after the fork resolves")]
  SplitReserved,
  /// A fork constructor was given `boot_epoch == 0`. The manufactured baseline issues its store
  /// writes in the PRIOR boot epoch (`boot_epoch - 1`) so their completions can never alias the
  /// child's own ops; epoch 0 has no prior epoch — the baseline would collapse into epoch 0,
  /// the very epoch the child's op-id counter is seeded with, and a queued baseline write
  /// acknowledgment could then release a live vote or campaign action whose durability it does
  /// not prove. Forks boot at epoch >= 1 so the baseline occupies the prior epoch exclusively,
  /// mirroring the restart contract (boot epochs strictly increase). Refused BEFORE any store
  /// write — the caller's fresh stores are untouched.
  #[error(
    "a fork must boot at epoch >= 1 (epoch 0 would alias the baseline's completions with the child's first live ops)"
  )]
  InvalidBootEpoch,
  /// A fork constructor was handed stores that already hold state (a durable/visible hard state,
  /// a snapshot slot, or log content). The manufactured baseline OVERWRITES whatever the stores
  /// hold, so a fork over a used incarnation's storage would destroy its progress since the fork
  /// — the crash-then-restore-the-parent replay would clobber the child's real durable state
  /// with a fresh baseline. Refused BEFORE any store write; only VIRGIN stores (the legitimate
  /// crash-before-flush replay, where nothing of the child ever became durable) are ever
  /// written.
  #[error("the fork's target stores already hold state; a fork never overwrites used storage")]
  StorageInUse,
}

/// Why [`MultiRaft::propose_split`](crate::MultiRaft::propose_split) — or a coordinator/driver
/// delegator around it — refused to propose a group split. One enum, three producer layers: the
/// container produces the consensus-shaped refusals (`NotLeader`/`JointConfig`/`ChildExists`/
/// `InvalidChild` and the `Propose` passthrough), a coordinator's propose delegator produces
/// `BelowFloor` through its bind-configured floor seam, and a sharded host's handle produces
/// `CrossPlane` before any command crosses a plane boundary. Nothing is appended in every case.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum SplitError<I> {
  /// This node is not the parent group's leader; redirect to `leader` if known. A split rides
  /// the parent's own log, so only its leader can propose one.
  #[error("not the parent group's leader")]
  NotLeader {
    /// The believed current leader of the parent group, if known.
    leader: Option<I>,
  },
  /// The parent is mid-joint-configuration. A split reads the parent's voter set AT its entry as
  /// the child's bootstrap membership; a joint parent would hand the child an ambiguous set, so
  /// the one-line rule is refuse-at-propose — finish (or leave) the joint change first.
  #[error("the parent group is in a joint configuration")]
  JointConfig,
  /// An earlier split on this parent is still in flight (appended, not yet applied) — mirror of
  /// [`ProposeError::ConfChangeInFlight`]. The mint (`parent_gen_after`) reads the live lineage
  /// counter, which bumps only when a split APPLIES, so a second proposal before then would
  /// carry the same mint and deterministically no-op at every replica's apply-time lineage
  /// guard ([`SplitStale`](crate::SplitStale)) — refuse it here instead. Self-healing by
  /// derivation, never a sticky flag: pending-ness is `last proposed split index > applied`,
  /// re-seated at every leadership accession, so an applied split (or a deposed leader's
  /// truncated entry) releases the gate on its own. Re-propose after the in-flight split
  /// applies.
  #[error("an earlier split is still in flight (not yet applied)")]
  SplitInFlight,
  /// A group with the child id is already hosted HERE (including the parent's own id). The
  /// single-incarnation contract makes a hosted id unavailable as a fork target; a committed
  /// split against it would PARK at this host's relay (the fork blob held, the parent fenced)
  /// until the conflict resolves — refuse at propose instead of manufacturing that conflict.
  #[error("a group with the child id is already hosted")]
  ChildExists,
  /// The child id's `Data` encoding is outside the group-tag wire bound (1..=1024 bytes).
  /// Refused at propose because a COMMITTED split whose child cannot decode poisons every
  /// replica of the parent (`SplitDecode`) — a self-inflicted cluster-wide fail-stop.
  #[error("the child group id's encoding is empty or exceeds the wire bound")]
  InvalidChild,
  /// The child incarnation is below the id's persisted admission floor (a coordinator-layer
  /// refusal through its floor seam): a removal or merge fenced the id, so the fork could never
  /// be admitted at materialization. Produced at propose so the entry is never appended.
  #[error("the child id's incarnation is below its admission floor ({floor})")]
  BelowFloor {
    /// The child id's persisted admission floor.
    floor: u64,
  },
  /// The child incarnation is the reserved `u64::MAX` merged-floor sentinel — never a working
  /// incarnation (see [`CreateGroupError::ReservedGeneration`]); a coordinator's floor seam
  /// refuses it at propose exactly as admission would at materialization.
  #[error("the child id's incarnation is the reserved merged-floor sentinel (u64::MAX)")]
  ReservedGeneration,
  /// The child id maps to a different plane than the parent on a sharded (K-plane) host. The
  /// fork must happen inside ONE plane's driver, so a cross-plane child is refused before any
  /// command is sent (v1 constraint; the shard-map override makes any child placeable).
  #[error("the child id maps to a different plane than the parent")]
  CrossPlane,
  /// The parent is frozen by an in-flight merge (dormant until the merge milestone lands: the
  /// freeze machinery is its surface; nothing produces this today).
  #[error("the parent group is frozen by an in-flight merge")]
  Frozen,
  /// The parent's lineage counter is one below the reserved
  /// [`MERGED_FLOOR`](crate::MERGED_FLOOR) terminal, so the split's `parent_gen_after` mint would
  /// reach the sentinel — a generation every downstream reader treats as merged-away, never a
  /// working incarnation. Refused at propose so the unmintable value never enters the log (the
  /// lineage analogue of [`ProposeError::LogIndexExhausted`]); this parent can never split again.
  /// Unreachable short of log-index exhaustion — every mint consumes a log index — so only a
  /// crafted or corrupt lineage counter reaches it. Nothing was appended.
  #[error(
    "the parent group's lineage counter is exhausted (a split cannot mint past the reserved u64::MAX terminal)"
  )]
  LineageExhausted,
  /// The underlying append refused (poisoned / transfer in progress / index space exhausted /
  /// entry too large for one frame). The split-specific gates all passed; the failure is the
  /// ordinary admin-append class, surfaced verbatim.
  #[error(transparent)]
  Propose(#[from] ProposeError<I>),
}

/// Why a merge verb — [`MultiRaft::prepare_merge`](crate::MultiRaft::prepare_merge),
/// [`commit_merge`](crate::MultiRaft::commit_merge), or
/// [`rollback_merge`](crate::MultiRaft::rollback_merge), or a coordinator/driver delegator
/// around them — refused to propose. One enum, three producer layers, the [`SplitError`]
/// precedent verbatim: the container produces the consensus-shaped refusals, a coordinator's
/// delegator produces the floor refusals through its per-call seam, and a sharded host's handle
/// produces `CrossPlane` before any command crosses a plane. Nothing is appended in every case.
/// There is deliberately NO `Unsupported` variant: absorb support is knowable only at apply
/// (the FSM seam is a defaulted method), where an unsupporting state machine fail-stops
/// deterministically rather than diverging.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum MergeError<I> {
  /// This node is not the mutated group's leader; redirect to `leader` if known. Each merge verb
  /// rides exactly one group's log — `prepare_merge` the source's, `commit_merge` and
  /// `rollback_merge` the TARGET's (the abort must be totally ordered against the commit),
  /// the relayed thaw the source's — so only that group's leader can propose it.
  #[error("not the mutated group's leader")]
  NotLeader {
    /// The believed current leader of the mutated group, if known.
    leader: Option<I>,
  },
  /// The source and target are the same group. A group cannot absorb itself.
  #[error("a group cannot merge into itself")]
  SelfMerge,
  /// `prepare_merge`'s target group is not hosted by this container. The preconditions compare
  /// the two groups' LOCAL replicas (voter sets, read modes), so the target must live here —
  /// which colocation guarantees for every legitimate pairing.
  #[error("the merge target is not hosted locally")]
  TargetMissing,
  /// `commit_merge`'s source group is not hosted by this container: there is no local frozen
  /// replica to gate on (or absorb). Same colocation contract as [`TargetMissing`](Self::TargetMissing).
  #[error("the merge source is not hosted locally")]
  SourceMissing,
  /// The two groups' VOTER sets are not identical node sets. Colocation is what makes the
  /// absorb a purely local hand-off on every replica; merge the memberships first.
  #[error("the source and target voter sets differ")]
  VoterSetsDiffer,
  /// A participant's committed configuration still carries LEARNERS. A merge is a purely local
  /// hand-off on every replica, and the driver's relay places children only on VOTER hosts and
  /// parks a live `CommitMerge` only on the target's VOTER hosts — so a target-learner host, even
  /// one that later becomes leader, would park its absorb at `k − 1` forever. Align the replica
  /// sets first: promote or remove the learners on both sides (the CRDB doctrine). Boot-config
  /// observers never enter a committed configuration, so they are unaffected; and the non-joint
  /// gate this refusal sits behind already empties `learners_next`, leaving the stable learner
  /// set as the whole of it.
  #[error("a merge participant's configuration carries learners")]
  LearnersPresent,
  /// One of the groups is mid-joint-configuration; its effective voter set is ambiguous for the
  /// colocation comparison. Finish (or leave) the joint change first.
  #[error("a merge participant is in a joint configuration")]
  JointConfig,
  /// The groups run different ACTIVE read modes. A merged group serves under one mode's
  /// guarantees; migrate one side first (the shipped `SetReadMode` machinery).
  #[error("the source and target read modes differ")]
  ReadModesDiffer,
  /// A membership change is still in flight (appended, not yet applied) on a participant. The
  /// voter-set comparison would race its apply — and a conf entry committing above the freeze
  /// strands the changed membership outside the merge. Re-propose after it applies.
  #[error("a merge participant has a membership change in flight")]
  ConfChangeInFlight,
  /// The source is already frozen (or its freeze is pending in the log). One merge at a time
  /// per source; the standing freeze must resolve or roll back first.
  #[error("the source group is already frozen or freezing")]
  AlreadyFrozen,
  /// A `CommitMerge` is already in flight or parked on the target. One absorb at a time; the
  /// standing one must resolve first.
  #[error("a merge into this target is already in flight or parked")]
  AlreadyPending,
  /// `commit_merge`'s LOCAL source replica is not yet frozen-applied at its freeze boundary —
  /// not frozen at all, or still applying toward it. The propose gate is deliberately local and
  /// cheap (every replica's parked apply re-checks the same facts); retry after the local
  /// source catches up, or roll the merge back.
  #[error("the local source replica is not frozen-applied at its freeze boundary")]
  SourceNotReady,
  /// The all-source-voters freeze barrier is not yet observably met. `commit_merge` refuses to
  /// dissolve the source until EVERY source voter has matched the freeze boundary, so a
  /// committed `CommitMerge` certifies that the whole voter set already holds the freeze — the
  /// dissolution that rides it can then run on every host with no straggler orphaned, even if the
  /// source leader is lost. The barrier is observable only on the source LEADER's tracker, so this
  /// refuses when the local source is a follower (colocate the target's leadership onto the source
  /// leader first) or when a voter still lags the boundary (the frozen source keeps replicating —
  /// retry once it catches up, or roll the merge back if a voter is permanently gone).
  #[error("not every source voter has reached the freeze boundary yet")]
  SourceBarrierPending,
  /// The source still owes an aborted upstream merge its thaw: a merge this endpoint applied as a
  /// TARGET was aborted, recording a durable `abandoned` obligation that its per-crank thaw pass has
  /// not yet discharged. It cannot dissolve as a fresh merge's source — the Resolve arm removes the
  /// source endpoint, and every undischarged obligation would vanish with it, stranding the upstream
  /// source frozen forever. TRANSIENT, exactly like [`SourceBarrierPending`](Self::SourceBarrierPending):
  /// the thaw pass drives each abandoned source past its freeze within a few cranks, discharging the
  /// obligation, after which the same freeze admits.
  #[error("the source still owes an aborted merge its thaw")]
  SourceOwesThaw,
  /// A participant's CURRENT incarnation is below its persisted admission floor (a
  /// coordinator-layer refusal through its floor seam): this replica belongs to a fenced
  /// incarnation — a stale survivor that must not anchor a merge.
  #[error("a merge participant's incarnation is below its admission floor ({floor})")]
  BelowFloor {
    /// The persisted admission floor that fenced the participant.
    floor: u64,
  },
  /// The source and target map to different planes on a sharded (K-plane) host. A merge is a
  /// same-plane operation (the absorb is an in-container hand-off); cross-plane merges are the
  /// same explicit non-goal as cross-plane splits.
  #[error("the source and target map to different planes")]
  CrossPlane,
  /// `rollback_merge`'s local source replica holds no APPLIED freeze — there is nothing to
  /// abort. A merely pending freeze also refuses (its generation and claim are unreadable
  /// until it applies; a freeze that never commits self-heals through truncation instead).
  #[error("the local source replica holds no applied freeze")]
  NotFrozen,
  /// The source's freeze names a DIFFERENT target: the freeze is a CLAIM by exactly one
  /// target, pinned on the source's log for the freeze's whole generation — only that target
  /// may absorb it or abort it (the claim is what makes two targets naming one frozen source
  /// resolve identically on every replica).
  #[error("the source's freeze names a different target")]
  SourceClaimed,
  /// The relayed thaw names freeze generation `expected`, but this source leader's applied
  /// lineage `seen` has not reached it yet — the freeze is committed but not yet folded on a
  /// freshly elected leader. TRANSIENT: the relay retries once the source's apply catches up.
  #[error(
    "the source has not applied the thaw's freeze generation yet (expected {expected}, at {seen})"
  )]
  SourceBehindFreeze {
    /// The freeze generation the relay authorizes a thaw for.
    expected: u64,
    /// The source's current applied lineage, still short of it.
    seen: u64,
  },
  /// The relayed thaw names freeze generation `expected`, but the source's lineage `seen` has
  /// already advanced past it — the freeze was thawed, and the same source→target pair may have
  /// re-frozen for a NEW merge. A relay retained across source-leader churn must bind to the
  /// incarnation it abandoned, never the source's live counter, or it would thaw that later
  /// freeze with no matching target-side abort — aborting the new merge out of order. TERMINAL:
  /// the relay is a spent authorization and is dropped.
  #[error("the thaw's freeze generation is stale (expected {expected}, source at {seen})")]
  StaleThaw {
    /// The freeze generation the relay authorized a thaw for.
    expected: u64,
    /// The source's current lineage, already past it.
    seen: u64,
  },
  /// The CLAIMED target holds NO committed abort obligation for this `(source, expected)`
  /// incarnation — the structural derived-from-abort gate. A source thaw is legal ONLY as the
  /// downstream consequence of a committed target-side abort: the target's durable `abandoned`
  /// record for exactly this source and freeze generation is what authorizes moving the source's
  /// counter. Absent it, appending the thaw would unfreeze a source no target ever abandoned —
  /// the cross-log rollback race the whole abort-derives-thaw path exists to prevent. Unreachable
  /// from the container's own per-crank service, which derives the drive FROM the obligation; the
  /// belt-and-suspenders refusal that makes the invariant intrinsic to the thaw path itself.
  #[error("the claimed target holds no committed abort obligation for this source incarnation")]
  UnbackedThaw,
  /// The mutated group's lineage counter is one below the reserved
  /// [`MERGED_FLOOR`](crate::MERGED_FLOOR) terminal, so this verb's generation mint (a freeze's
  /// `source_gen_after`, an absorb's or abort's `target_gen_after`, or a thaw's `source_gen_after`)
  /// would reach the sentinel — a generation every downstream reader treats as merged-away, never a
  /// working incarnation. Refused at propose so the unmintable value never enters the log (the
  /// lineage analogue of [`ProposeError::LogIndexExhausted`]); this group can never reshape again.
  /// Unreachable short of log-index exhaustion — every mint consumes a log index. Nothing was
  /// appended.
  #[error(
    "the group's lineage counter is exhausted (a merge verb cannot mint past the reserved u64::MAX terminal)"
  )]
  LineageExhausted,
  /// The underlying append refused (poisoned / transfer in progress / index space exhausted /
  /// entry too large). The merge-specific gates all passed; the failure is the ordinary
  /// admin-append class, surfaced verbatim.
  #[error(transparent)]
  Propose(#[from] ProposeError<I>),
}

/// Why a leader-transfer request was rejected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum TransferError<I> {
  /// This node is not the leader; a transfer can only be initiated by the current leader.
  #[error("not the leader")]
  NotLeader {
    /// The believed current leader, if known.
    leader: Option<I>,
  },
  /// The target node is not a voter in the current configuration and therefore cannot be
  /// elected leader.
  #[error("transfer target is not a voter")]
  NotAVoter,
  /// The target node is the current leader — no transfer needed.
  #[error("transfer target is already the leader")]
  AlreadyLeader,
  /// The group is FROZEN by an in-flight merge: its leadership is about to be dissolved into the
  /// absorbing target, so handing it off is refused (a transferee would inherit the same frozen
  /// group). Roll the merge back first if the transfer genuinely matters.
  #[error("the group is frozen by an in-flight merge")]
  Frozen,
  /// The node has entered the permanent poisoned state and accepts no new work. The transfer was
  /// NOT initiated; inspect `poison_reason()`.
  #[error("the node is poisoned and cannot initiate a transfer")]
  Poisoned,
}

/// Why a [`read_index`](crate::Endpoint::read_index) request could not be issued.
///
/// A `read_index` that returns `Ok(())` has been accepted onto a confirmation path (the
/// leader's heartbeat-quorum round, an immediate lease confirmation, or a forward to the
/// known leader); the eventual [`Event::ReadState`](crate::Event::ReadState) (locally) or
/// [`ReadIndexResponse`](crate::ReadIndexResponse) (when forwarded) is the only acknowledgement.
/// An `Err` means **no** such acknowledgement will ever arrive for this call, so the caller
/// must not block waiting for one.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ReadIndexError {
  /// This node is a follower with no known leader to forward the read to, so the request
  /// cannot be confirmed. Retry once a leader is known.
  #[error("no known leader to confirm the read")]
  NoLeader,
  /// This node is a follower and `disable_proposal_forwarding` is set, so the read cannot be
  /// forwarded to the leader. Issue the read on (or redirect it to) the leader directly.
  #[error("proposal forwarding is disabled; cannot forward the read to the leader")]
  ForwardingDisabled,
  /// A read with this exact `context` is already in flight. The `context` is the sole
  /// correlator between a request and its eventual `ReadState`/`ReadIndexResponse`, so two
  /// concurrent reads MUST use distinct contexts (including the empty context). Wait for the
  /// in-flight read to confirm, or reissue with a unique context.
  #[error("a read with this context is already in flight")]
  DuplicateContext,
  /// This follower already has the maximum number of forwarded reads awaiting a `ReadIndexResponse`
  /// (back-pressure). The read was NOT accepted; retry after some in-flight reads confirm, or once a
  /// leader/term change clears the backlog. Forwarded reads are never silently evicted, so an
  /// already-accepted read is never stranded.
  #[error("too many forwarded reads are already in flight")]
  TooManyInFlight,
  /// This node is poisoned (a fatal storage/log fault left its commit/applied view untrustworthy),
  /// so it suppresses all event emission. A read cannot be confirmed — no
  /// [`Event::ReadState`](crate::Event::ReadState) will ever arrive — so the request is rejected
  /// rather than silently accepted onto a path that never completes.
  #[error("node is poisoned; reads cannot be confirmed")]
  Poisoned,
  /// The group is FROZEN by an applied merge freeze: it fails reads CLOSED, typed, rather than
  /// parking them forever — sailing sits below routing, so the embedder re-routes the query to
  /// the absorbing target once the merge resolves (or rolls the merge back).
  #[error("the group is frozen by an in-flight merge; reads fail closed")]
  Frozen,
}

/// Why constructing a [`crate::Config`] failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ConfigError {
  /// `election_timeout` was not strictly greater than `heartbeat_interval`.
  #[error("election timeout ({election:?}) must exceed heartbeat interval ({heartbeat:?})")]
  ElectionNotGreaterThanHeartbeat {
    /// The rejected election timeout.
    election: Duration,
    /// The heartbeat interval it must exceed.
    heartbeat: Duration,
  },
  /// `heartbeat_interval` was zero.
  #[error("heartbeat interval must be non-zero")]
  ZeroHeartbeat,
  /// The configured `id` was not present in the voter set.
  #[error("id is not among the configured voters")]
  IdNotAVoter,
  /// `read_only = LeaseGuard` but no `lease_duration` was set.
  #[error("the LeaseGuard read mode requires a lease_duration")]
  LeaseGuardRequiresLeaseDuration,
  /// `read_only = LeaseGuard` but no `clock_drift_bound` was set (the commit-wait needs it).
  #[error("the LeaseGuard read mode requires a clock_drift_bound")]
  LeaseGuardRequiresDriftBound,
  /// The LeaseGuard commit-wait window `lease_duration·(lease_duration + clock_drift_bound) /
  /// (lease_duration − clock_drift_bound)` is invalid: `clock_drift_bound >= lease_duration`, the
  /// window overflows the `u64` wire field, or it is not strictly less than the election timeout (so
  /// a stale lease could outlive a new leader's election, or a fresh leader could never commit).
  #[error(
    "the LeaseGuard commit-wait window for lease_duration ({lease:?}) and clock_drift_bound ({drift:?}) is invalid (must have drift < lease and window < election timeout {election:?})"
  )]
  LeaseTimingTooLong {
    /// The configured lease window.
    lease: Duration,
    /// The configured clock-drift bound.
    drift: Duration,
    /// The election timeout it must stay under.
    election: Duration,
  },
  /// `max_inflight_msgs` was zero.
  #[error("max_inflight_msgs must be greater than zero")]
  ZeroInflight,
  /// `max_size_per_msg` was zero (which caps every `AppendEntries` at a single entry — a throughput
  /// footgun). The per-frame cap is enforced independently, so this is a sanity floor, not the frame
  /// bound.
  #[error("max_size_per_msg must be greater than zero")]
  ZeroMaxSizePerMsg,
  /// `snapshot_threshold` was zero. The snapshot trigger fires when `applied - first_index >=
  /// threshold`, so a zero threshold matches on every applied index and captures a full snapshot on
  /// every storage drain — a perpetual snapshot/compaction loop.
  #[error("snapshot_threshold must be greater than zero")]
  ZeroSnapshotThreshold,
  /// `snapshot_chunk_bytes` was zero (which would livelock on empty chunks) or exceeded the frame-safe
  /// maximum (which would produce an unsendable wire frame).
  #[error("snapshot_chunk_bytes must be in 1..={max} (got {value})")]
  SnapshotChunkBytesOutOfRange {
    /// The configured chunk size.
    value: u64,
    /// The frame-safe maximum (the configured `snapshot_chunk_bytes` upper bound).
    max: u64,
  },
  /// The voter set was empty. A config with no voters has no consensus group to bootstrap; the
  /// programmatic constructors reach this only via [`crate::Config::try_new`]'s `id ∈ voters`
  /// rejection, but a parsed (serde / clap) config could otherwise carry an empty `voters`.
  #[error("the voter set must not be empty")]
  EmptyVoters,
  /// `ReadOnlyOption::LeaseBased` requires `check_quorum = true` (the lease safety depends on
  /// the leader knowing it still holds a quorum; without CheckQuorum that guarantee is absent).
  #[error("ReadOnlyOption::LeaseBased requires check_quorum to be enabled")]
  LeaseRequiresCheckQuorum,
  /// `bounded_clock_uncertainty` (the LeaseGuard failover tier's synchronized-clock skew bound) was
  /// set without `read_only = LeaseGuard`, or it was not strictly less than `lease_duration`.
  #[error(
    "bounded_clock_uncertainty ({uncertainty:?}) requires read_only = LeaseGuard and must be < lease_duration ({lease:?})"
  )]
  BoundedUncertaintyInvalid {
    /// The configured bounded clock uncertainty (the failover skew bound).
    uncertainty: Duration,
    /// The configured lease duration it must be under (`None` if unset).
    lease: Option<Duration>,
  },
  /// `lease_refresh` was set to a proactive mode (`OnExpiry` / `Continuous`) without
  /// `read_only = LeaseGuard`. Safe and LeaseBased reads have no per-entry timestamp anchor to refresh,
  /// so the knob is meaningless there.
  #[error("a proactive lease_refresh requires read_only = LeaseGuard")]
  LeaseRefreshRequiresLeaseGuard,
  /// `election_timeout` exceeds the `Instant`-safe bound. The per-term randomized timeout is
  /// `election_timeout + Duration::from_millis(rng % election_timeout_ms)` (a raw `Duration` add that
  /// PANICS on overflow), so a value near `Duration::MAX` parsed from a config would take the node down
  /// on its first election. Rejected above [`crate::Config`]'s `Instant`-safe election bound so the
  /// randomized draw (`< 2 · election_timeout`) can never overflow.
  #[error("election_timeout ({election:?}) must be at most the Instant-safe bound ({max:?})")]
  ElectionTimeoutTooLarge {
    /// The rejected election timeout.
    election: Duration,
    /// The `Instant`-safe maximum it must not exceed.
    max: Duration,
  },
}
