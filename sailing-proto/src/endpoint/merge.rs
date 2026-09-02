//! The endpoint-resident merge state: the append-observed freeze, the applied `Frozen` fold, and
//! the parked `CommitMerge` apply the container resolves from local facts.
//!
//! The lease SAFETY gate moves to APPEND observation (`freeze_queue`): every lease-serve and
//! lease-formation gate fails closed from the moment a `PrepareMerge` entry ENTERS the local log
//! — the proposing leader appends before it replicates, and every lease is served leader-side, so
//! the total order `emit(read) < append(freeze) < commit < apply < absorb < accept(write)` holds
//! with NO commit-wait and NO cross-node clock anywhere. The remaining freeze semantics stay
//! apply-time (the membership-apply-time doctrine's shape).
use super::*;
use std::collections::VecDeque;

/// One committed `CommitMerge`, parked at the target's apply until the container resolves it: the
/// endpoint alone CANNOT apply it — the absorbed half lives in another group's endpoint, which
/// only the container holds. `applied` stays at `at - 1` while parked; everything else about the
/// target keeps running (elections, replication, reads confirm at the commit index and serve once
/// the resolution advances `applied`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingMergeApply {
  /// The absorbed (source) group id's canonical `Data` encoding (G-free, like the entry).
  source_bytes: Bytes,
  /// The source's freeze boundary: the local source replica must be frozen-applied at (or past —
  /// only FSM-no-ops can follow a surviving freeze) this index before the absorb can resolve.
  freeze_index: Index,
  /// The freeze entry's TERM — with `freeze_index`, the committed freeze's log identity (the
  /// carried [`CommitMergePayload::freeze_term`](crate::CommitMergePayload::freeze_term); zero
  /// when the entry carried none).
  freeze_term: Term,
  /// The gen the source's freeze set — the resolution's comparator: a source whose live counter
  /// moved PAST it was rolled back, and the parked apply aborts deterministically.
  source_gen_after: u64,
  /// The target's lineage counter after the absorb.
  target_gen_after: u64,
  /// The `CommitMerge` entry's own index `k` (the drain parked at `k - 1`).
  at: Index,
  /// THE LATCHED WINDOW VERDICT: whether this park's abort window was ever observed
  /// [`MergeWindow::Closed`] — i.e. `k + 1` is committed and is not this merge's abort, so no
  /// abort can ever contest it again. Minted `false`, set once by the resolver, monotone for as
  /// long as THIS park lives, and gone with it: the flag rides the park itself rather than the
  /// endpoint's [`MergeState`], so a fan-in target's NEXT park starts undecided instead of
  /// inheriting a verdict about the previous one.
  ///
  /// It is a LATCH because evaluation is not monotone even though the fact is: a cold or briefly
  /// invisible read of the coordinate returns [`MergeWindow::Stall`], so re-reading can flap
  /// Closed → Stall while the committed content it describes cannot change. Callers that must not
  /// flap read the latch.
  ///
  /// VOLATILE, and safely so. A crash re-derives the window from the replayed log, and replay can
  /// only ever be BEHIND the live evaluation, never ahead of it in the unsafe direction: a durable
  /// commit index never recovers above the truth, so a replayed `Closed` is always genuine and a
  /// replayed `Open` merely withholds a verdict this park had already earned. Consumers therefore
  /// treat an unlatched park as undecided, which is the conservative direction.
  window_closed: bool,
}

impl PendingMergeApply {
  /// Record a park (the apply arm's constructor).
  pub(crate) const fn new_parked(
    source_bytes: Bytes,
    freeze_index: Index,
    freeze_term: Term,
    source_gen_after: u64,
    target_gen_after: u64,
    at: Index,
  ) -> Self {
    Self {
      source_bytes,
      freeze_index,
      freeze_term,
      source_gen_after,
      target_gen_after,
      at,
      window_closed: false,
    }
  }

  /// Whether this park's abort window has been observed CLOSED (see
  /// [`window_closed`](Self::window_closed)).
  #[inline(always)]
  pub fn window_closed(&self) -> bool {
    self.window_closed
  }

  /// Latch the CLOSED verdict. Monotone and idempotent; only the resolver calls it, and only from
  /// the branch that just read [`MergeWindow::Closed`] for this very park.
  pub(crate) const fn latch_window_closed(&mut self) {
    self.window_closed = true;
  }

  /// The absorbed (source) group id's canonical `Data` encoding (an O(1) shared handle).
  #[inline(always)]
  pub fn source_bytes(&self) -> Bytes {
    self.source_bytes.clone()
  }

  /// The source's freeze boundary.
  #[inline(always)]
  pub const fn freeze_index(&self) -> Index {
    self.freeze_index
  }

  /// The freeze entry's term (the boundary's log identity; zero when the entry carried none).
  #[inline(always)]
  pub const fn freeze_term(&self) -> Term {
    self.freeze_term
  }

  /// The gen the source's freeze set (the resolution comparator).
  #[inline(always)]
  pub const fn source_gen_after(&self) -> u64 {
    self.source_gen_after
  }

  /// The target's lineage counter after the absorb.
  #[inline(always)]
  pub const fn target_gen_after(&self) -> u64 {
    self.target_gen_after
  }

  /// The parked `CommitMerge` entry's log index.
  #[inline(always)]
  pub const fn at(&self) -> Index {
    self.at
  }
}

/// The resolve arm's fence classification at a parked absorb — see
/// [`Endpoint::absorb_capture_block`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AbsorbCaptureBlock {
  /// No fence: absorb + capture land this crank, one barrier.
  Clear,
  /// A transient (staged capture/install) or a live freeze: the park stays.
  Hold,
  /// Only a replay fence (fork barrier / abort obligation): absorb now, capture as a debt.
  Defer,
}

/// The STRUCTURAL leg holding a target's capture — see [`Endpoint::capture_fence_at`]. Only the
/// legs that stand on an embedder or protocol timescale are named; a transient staged
/// capture/install has no variant, because it resolves on its own within cranks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CaptureFence {
  /// A live merge freeze: this endpoint is itself a source, pinned by a claiming target.
  Frozen,
  /// A staged fork's durability barrier sits at-or-below the boundary.
  Fork,
  /// An undischarged abort obligation's entry sits at-or-below the boundary.
  Abort,
}

/// One queued freeze's READ VERDICT — the value of [`MergeState::claim_cache`]: what a Ready
/// single-entry read of that `PrepareMerge` yields, judged at the commit it was read under.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ClaimRead {
  /// A valid `PrepareMerge`: the target it claims. Fixed for as long as the entry survives.
  Claim(Bytes),
  /// A refused entry read at-or-below `commit`: it poisons this source at apply and nothing
  /// above it ever applies, so the claim walk ends here for good.
  RefusedCommitted,
  /// A refused entry read ABOVE `commit` — indeterminate, since a conflicting append may still
  /// replace it, so the walk fails closed on it. `judged_at` is the commit it was read under;
  /// once `commit` reaches the index the verdict is re-read (it becomes `RefusedCommitted`).
  RefusedAbove { judged_at: Index },
}

/// The endpoint-resident merge state. One instance per endpoint, defaulted inert; every field is
/// DERIVED from the log and re-derivable at restart (`freeze_queue` from the unapplied suffix,
/// `frozen`/`freeze_index` from replaying the applied prefix, the park from re-encountering its
/// entry), so nothing here is persisted. The lineage counter deliberately does NOT live here —
/// incarnation and shape share ONE monotone per-id counter (`SplitState::shape_gen`).
#[derive(Debug, Default)]
pub(crate) struct MergeState {
  /// Every `PrepareMerge` in the UNAPPLIED suffix `(applied, last]`, in log order — the
  /// APPEND-MAINTAINED index of pending freezes. INVARIANT, pinned by the tests: after every
  /// append, truncation, apply and restore of a LIVE endpoint this queue equals the ordered set of
  /// `PrepareMerge` indices in `(applied, last]`. Maintained kind-only on the hot path — never a
  /// payload decode, never a walk of the suffix: the two append arms push every `PrepareMerge` of
  /// the appended suffix in order, a conflict truncation pops from the back while at-or-above the
  /// truncation point, a snapshot re-baseline clears it, the `PrepareMerge` fold pops its own index
  /// (the front), and a restart or a cure adopt REBUILDS it from one scan of the surviving suffix —
  /// the only suffix walks it ever costs. A REFUSED `PrepareMerge` clears it whole: the poison halts
  /// the drain, so nothing queued above the refusal ever applies and no claim can materialize from
  /// it; a poisoned endpoint is the one place the invariant is deliberately broken, in the empty
  /// direction. The FRONT is the lowest pending freeze, the append-observed lease kill: while the
  /// queue is non-empty every lease-serve and lease-formation gate fails closed, and the container's
  /// claim gate reads the queued indices — and only those — to learn which targets the pending
  /// freezes claim. Restart re-derives it before the replica can win an election and form a fresh
  /// lease; an election itself never clears it (log-derived state — a new leader inherits it with
  /// the log).
  pub(crate) freeze_queue: VecDeque<Index>,
  /// The claim gate's per-index READ VERDICTS for the queued freezes — DERIVED state, a pure
  /// function of the surviving entries and `commit`: its keys are a subset of `freeze_queue`, and
  /// each value is what a Ready single-entry read of that entry yields under the recorded commit.
  /// It exists so the gate's walk RESUMES rather than restarts. A bounded cache that pages the
  /// suffix in and out makes a walk that re-reads every queued entry after a cold page livelock —
  /// two required pages and one page of cache: each read evicts the other's — and the gate would
  /// answer a standing refusal forever. With the cache every queued index needs exactly one Ready
  /// read, ever, and a cold page costs only the indices at and above it. Invalidated wherever the
  /// queue changes: a conflict truncation drops the verdicts at-or-above its point (the entry may
  /// come back with a different claim), the fold drops the popped index, a refusal and a
  /// re-baseline empty it with the queue, a cure adopt keeps only the indices above its boundary,
  /// and a boot starts empty. A `RefusedAbove` verdict is re-read once `commit` reaches its index.
  /// ONE reader, [`Endpoint::scan_freeze_claims`], which is also its only filler.
  pub(crate) claim_cache: BTreeMap<Index, ClaimRead>,
  /// Whether a committed `PrepareMerge` has APPLIED and no later `RollbackMerge` has undone it:
  /// the full freeze — proposals, conf changes, transfers, and reads refuse typed; heartbeats,
  /// appends, elections, and snapshot sends run UNCHANGED (the group must stay live to propagate
  /// its own freeze and survive leader crashes).
  pub(crate) frozen: bool,
  /// The applied `PrepareMerge` entry's index while frozen — the boundary the absorbing target
  /// gates on. `None` exactly when `frozen` is false.
  pub(crate) freeze_index: Option<Index>,
  /// The applied `PrepareMerge` entry's TERM while frozen — with `freeze_index`, the freeze's
  /// log identity. Stamped into `CommitMergePayload` at the target's propose so a parked host
  /// can later prove its local source log holds the committed freeze (see
  /// [`Endpoint::advance_commit_on_freeze_identity`]). Same lifecycle as `freeze_index`: set at
  /// apply, cleared by the thaw, re-derived by replay (the capture fence keeps the freeze entry
  /// replayable for as long as the freeze lives).
  pub(crate) freeze_term: Option<Term>,
  /// The TARGET id the applied freeze named (its `Data` encoding, from the `PrepareMerge`
  /// payload), retained while frozen: the freeze is a CLAIM by exactly one target, and both the
  /// `commit_merge` propose gate and the park's resolve arm compare against it — without the
  /// claim, two targets naming one frozen source would race per host for the absorb (the same
  /// committed-divergence class as the cross-log rollback). Immutable for the freeze's whole
  /// generation, so reading it off the live source is host-order-independent; `None` exactly
  /// when `frozen` is false. Re-derived by replay like `frozen` itself (the merge replay fence
  /// keeps the freeze entry replayable for as long as the freeze lives).
  pub(crate) frozen_for: Option<Bytes>,
  /// The parked `CommitMerge` (target side), `Some` while the apply drain is stopped at
  /// `at - 1`. Written ONLY by the park arm and the two container resolutions.
  pub(crate) pending_apply: Option<PendingMergeApply>,
  /// Whether the container's per-crank resolver classified the standing park LOCALLY
  /// UNRESOLVABLE — the source is unhosted here with a non-terminal floor, so no local fold can
  /// ever land — AND the cure-advertisement gate passed (no fork barrier stands, and no abort
  /// obligation names a hosted-and-frozen source whose thaw an adopt would erase). While set,
  /// the follower advertises its park boundary and the receipt-time redundancy arm ADOPTS a
  /// covering blob in place of the impossible fold. Volatile, re-derived every resolver crank;
  /// meaningless without `pending_apply`.
  pub(crate) park_unresolvable: bool,
  /// When the next UNSOLICITED park advertisement is due (see
  /// [`Endpoint::drive_stuck_advertisement`]). `None` means DUE IMMEDIATELY — a freshly-classified
  /// park advertises on the first tick that sees it, rather than waiting out a period. Charged by
  /// BOTH carriers, so a heartbeat-stamped boundary and the unsolicited belt share one cadence;
  /// cleared the moment the hint clears, so a later park never inherits a stale deadline. Volatile
  /// pacing state, meaningless without `park_unresolvable`.
  pub(crate) stuck_advert_next_at: Option<Instant>,
  /// Committed `CommitMerge` entries ABOVE the standing park that an adopting install would
  /// cross without locally resolving — their source id bytes, recorded by an incremental
  /// kind-only walk the resolver advances each crank. The cure-advertisement gate withholds the
  /// hint while ANY of them names a locally hosted group: an adopt would leave that replica a
  /// live-voting husk of a lineage the blob absorbed (or a lineage whose stale no-op only the
  /// full apply machinery can classify), and no scan-side re-derivation of the apply's lineage
  /// guard is sound — so the refusal is deliberately outcome-blind and conservative. Patience
  /// costs liveness only for the composed shape, whose exit stays the hosted replica's own
  /// lifecycle (or the propagated terminal floor). Volatile with the park.
  pub(crate) crossing_sources: Vec<Bytes>,
  /// The crossing walk's high-water mark: entries at-or-below it were already examined, so each
  /// crank scans only the committed delta — O(new entries) amortized for the park's life.
  pub(crate) crossing_scan_upto: Index,
  /// Whether the walk reached this crank's committed frontier — the hint demands it: an
  /// advertisement off a partial walk would authorize an adopt across entries never examined.
  pub(crate) crossing_scan_current: bool,
  /// A successful ADOPT owes one forced capture, serviced by the container independently of
  /// the snapshot threshold: the adopt deliberately persists no blob, so under idle load an
  /// adopter that later leads would have nothing durable covering the boundary — unable to cure
  /// the next parked voter, its absorb membership fence never releasing without compaction.
  /// Volatile: a crash re-parks and the re-cure re-owes.
  pub(crate) adopt_capture_owed: bool,
  /// An absorbed-but-uncaptured union's outstanding durability obligation: the fold ran and the
  /// apply drain resumed, but a standing replay fence (a parked fork's barrier, an undischarged
  /// abort obligation) deferred the forced capture — so the consumed source's stores remain the
  /// union's only restart derivation until a capture (or a superseding install) at-or-past the
  /// absorb boundary is staged. Holds the `Merged` payload the discharge will surface; volatile,
  /// re-derived by the restart re-park (the boundary's `CommitMerge` cannot compact away first —
  /// compaction past it requires exactly the capture whose absence defines the debt). While set:
  /// reshape verbs into this target refuse, the container refuses to re-host or tear down either
  /// named group, and a debt-holding leader is never quiesce-eligible.
  pub(crate) capture_debt: Option<crate::Merged>,
  /// Log index of the most recently appended (not-yet-applied) `CommitMerge` on THIS leader —
  /// the in-flight leg of the target's membership fence (`> applied` ⇒ a commit-merge is in
  /// flight), mirroring `pending_split_index` exactly: derived state, re-seated conservatively
  /// at `become_leader`, never a sticky flag.
  pub(crate) pending_commit_index: Index,
  /// Log index of the most recently appended (not-yet-applied) `RollbackMerge` on THIS leader
  /// (`> applied` ⇒ a rollback is in flight) — `commit_merge`'s LINEAGE fence, the exact analogue
  /// of `pending_commit_index`. A target-role abort applies at its live mint and bumps `shape_gen`,
  /// so an unapplied one BELOW a freshly proposed `CommitMerge` staled its generation mint: the
  /// fan-in strand — a target absorbing one source while a release-valve abort of a DIFFERENT frozen
  /// source sits unapplied on its log makes the absorb no-op at its STRICT lineage guard and strand
  /// the committed source. (`prepare_merge`'s freeze is a monotone max, not a stale-aborting guard,
  /// so it does NOT read this fence — its collision is honored downstream by the Resolve-arm hold.)
  /// Set for EVERY `RollbackMerge` append (the source-role thaw included — harmlessly, since a
  /// thawing source is `frozen` and `commit_merge` refuses it `AlreadyFrozen` first). Derived,
  /// self-releasing via `> applied`, re-seated conservatively to `last` at `become_leader` like the
  /// other fences (a truncated-then-re-elected abort must not wedge the merge verbs).
  pub(crate) pending_rollback_index: Index,
  /// Log index of the SOURCE-role `RollbackMerge` (thaw) this leader last appended and not yet
  /// applied (`> applied` ⇒ a thaw is in flight) — the abort-relay's IDEMPOTENT-APPEND guard:
  /// the relay is retained across cranks until the source lineage is observed past the freeze,
  /// so without this the accept arm would append a fresh thaw every crank until the first
  /// commits. Derived, self-releasing via `> applied`. Unlike the fences above it re-seats to
  /// ZERO at `become_leader`, NOT to `last`: a fresh source leader must be free to re-drive a
  /// thaw the previous leader appended but never committed (a truncated thaw would otherwise
  /// wedge the source frozen forever); the guard only suppresses THIS leader's own duplicate.
  pub(crate) thaw_pending_index: Index,
  /// Log index of the `ThawDischarged` WITNESS this leader last appended (on its own TARGET log) and
  /// not yet applied (`> applied` ⇒ a witness is in flight) — the witness mint's IDEMPOTENT-APPEND
  /// guard, the exact twin of `thaw_pending_index` for the discharge-observing side. Without it the
  /// thaw pass would append a fresh witness every crank while a global proof stands until the first
  /// commits. Derived, self-releasing via `> applied`, and re-seated to ZERO (not `last`) at
  /// `become_leader` like `thaw_pending_index`: a witness appended then truncated before it committed
  /// must be re-appendable by the next observing leader, so a fresh leader starts with none in flight
  /// and the guard suppresses only THIS leader's own duplicate.
  pub(crate) witness_pending_index: Index,
  /// The resolved absorb's `CommitMerge` index, set by `resolve_pending_merge` — the membership
  /// fence's compaction leg: the target refuses conf changes until `first_index` passes it (the
  /// forced absorb capture compacts through it within a crank), so a replica added post-merge
  /// can never be LOG-WALKED across the absorb (it would park with no local source and no
  /// floor, and no-op past the union — silent divergence; the fork milestone's (1,1) lesson).
  /// Never cleared: the check compares against the live `first_index`, so it self-releases
  /// permanently once the capture's compaction lands.
  pub(crate) absorb_index: Option<Index>,
  /// Debts INHERITED from consumed sources that themselves still owed a capture (the chained
  /// shape a foreign-led merge can deliver): they discharge WITH [`MergeState::capture_debt`]
  /// on the same covering capture — the consumed state machine has carried each prior union
  /// since its absorb applied, so one snapshot at-or-past the newest boundary covers them all.
  /// Non-empty only while `capture_debt` is `Some` (inheritance happens only inside a `Defer`,
  /// which always mints).
  pub(crate) inherited_debts: std::vec::Vec<crate::Merged>,
  /// The abandoned merges this TARGET must thaw its sources out of, keyed by source id — one entry
  /// per aborted source, inserted when a target-role abort (`RollbackMerge` at its live mint)
  /// APPLIES here, removed once that source is observed thawed past the abandoned generation (or
  /// floored). A COLLECTION rather than one slot because a target legitimately absorbs many sources
  /// (fan-in): a second source can already be frozen toward it from the window before the first
  /// abort applied, so when its own abort lands a single-slot record would silently drop one
  /// obligation and strand that source frozen forever. DURABLE-DERIVED exactly like `frozen_for`:
  /// every obligation re-set on restart by REPLAYING the target's own committed abort entries, so
  /// each survives a crash in `[abort-committed, unfreeze-committed]` with no new persistence and no
  /// wire change. The per-crank container service ([`crate::MultiRaft::service_merge_applies`])
  /// drives each source-side `RollbackMerge` FROM this map — the source rollback is NEVER an
  /// independent source decision, only the downstream consequence of a committed target abort. The
  /// value is `(the abandoned freeze generation — the thaw's `expected_gen` — and the abort entry's
  /// own index)`; the abort index is the compaction fence boundary ([`Endpoint::abort_relay_fences`]),
  /// because the entry must stay replayable while its obligation is set or a restart past it would
  /// lose the obligation with the source still frozen — a permanent frozen-source wedge. Insert is
  /// LAST-WINS per source: a source re-frozen for a fresh merge (its earlier obligation already
  /// discharged) records the new generation over the spent one — idempotent for a replayed
  /// duplicate, correct for a re-freeze. Written ONLY by the abort apply, the install-clear
  /// ([`Endpoint::note_abort_rebaselined`]), the container's purge when the named source leaves the
  /// host ([`crate::MultiRaft::remove_group`] — so a removed incarnation's obligation can never back
  /// a recreate's thaw), and the service's per-source discharge.
  pub(crate) abandoned: BTreeMap<Bytes, (u64, Index)>,
}

/// A parked commit's ABORT-WINDOW verdict — the target-log half of the park's resolution rule.
/// The window is the single log coordinate `k + 1` (the entry after the parked commit): until
/// something COMMITS there the merge outcome is still contestable and no arm may resolve (an
/// absorb taken while the coordinate is undecided would race an abort landing at it — one host
/// absorbed, another aborted, committed divergence); once committed, the coordinate's content
/// is immutable and identical on every replica, so the verdict below is a pure log function.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MergeWindow {
  /// Nothing committed at `k + 1` yet: hold every arm; the leader seals the window with a
  /// no-op so a quiet target cannot hold it open forever.
  Open,
  /// The committed `k + 1` is THIS merge's abort: the park resolves aborted — on every
  /// replica, because the coordinate is committed-log content.
  Abort,
  /// The committed `k + 1` is anything else: no abort can ever contest this merge again (a
  /// later abort no-ops at its lineage guard once the absorb bumps the counter), so the park
  /// may wait on the source gate and resolve.
  Closed,
  /// The coordinate is committed but not readable this crank (a cold store, a benign
  /// not-yet-visible read): hold, retry next crank.
  Stall,
}

impl<I, F, R> Endpoint<I, F, R>
where
  F: StateMachine,
{
  /// Evaluate the parked commit's abort window (see [`MergeWindow`]). Reads only THIS group's
  /// committed log — never the source's mutable state — so every replica evaluates the same
  /// bytes. The coordinate cannot have been compacted while parked: compaction happens at
  /// applied indexes, the park holds `applied` at `k - 1`, and an install past the park
  /// supersedes it entirely.
  pub(crate) fn merge_abort_window<L: LogStore>(&self, log: &L) -> MergeWindow {
    let Some(pending) = self.merge.pending_apply.as_ref() else {
      debug_assert!(false, "window read without a parked CommitMerge");
      return MergeWindow::Stall;
    };
    let coord = pending.at().next();
    if self.commit < coord {
      return MergeWindow::Open;
    }
    let read = match log.entries(coord..coord.next(), 1 << 20) {
      Ok(EntriesRead::Ready(e)) if !e.is_empty() => e,
      // Committed but not readable this pass (cold or briefly invisible): benign, retried —
      // exactly the apply drain's own treatment of the same read. A genuinely faulted store
      // is poisoned by that drain when it reaches the coordinate; the window never poisons.
      _ => return MergeWindow::Stall,
    };
    let entry = &read[0];
    if entry.kind() != EntryKind::RollbackMerge {
      return MergeWindow::Closed;
    }
    match crate::wire::decode_rollback_merge_payload(entry.data_bytes()) {
      Ok(p)
        if !p.is_unfreeze()
          && p.source_bytes() == pending.source_bytes()
          && p.source_gen_after() == pending.source_gen_after() =>
      {
        MergeWindow::Abort
      }
      // A different merge's abort (or the source-role shape, unreachable on a target's log)
      // closes the window like any other entry; a corrupt payload is the apply drain's poison
      // to raise when it reaches the coordinate — the window's verdict is still deterministic
      // (same committed bytes everywhere).
      _ => MergeWindow::Closed,
    }
  }

  /// The `(source id, freeze generation)` named by the in-flight `CommitMerge` this leader appended
  /// at [`pending_commit_index`](MergeState::pending_commit_index) but has not yet applied —
  /// decoded from that one log entry, the cold-read twin of
  /// [`merge_abort_window`](Self::merge_abort_window) for the pre-park window a `pending_merge` read
  /// cannot see. `rollback_merge`'s cross-source fence reads it: an abort may RACE an in-flight
  /// commit only of the SAME merge (#22), so it compares its own `(source, source_gen_after)`
  /// against this before minting. `None` on a cold/absent read, a non-`CommitMerge` at that index
  /// (a conservative `become_leader` reseat to `last`, or a truncation), or a decode fault — the
  /// caller FAILS CLOSED on every one (defer the abort `AlreadyPending`), never mistaking a source
  /// it cannot read for a match. Meaningful only with a commit in flight
  /// ([`commit_merge_in_flight`](Self::commit_merge_in_flight)); reads only THIS group's own log.
  pub(crate) fn pending_commit_source<L: LogStore>(&self, log: &L) -> Option<(Bytes, u64)> {
    let coord = self.merge.pending_commit_index;
    let read = match log.entries(coord..coord.next(), 1 << 20) {
      Ok(EntriesRead::Ready(e)) if !e.is_empty() => e,
      // Cold, briefly invisible, or empty: the abort cannot rule out a same-source race, so the
      // caller defers — the safe, self-clearing choice (retried once the entry is resident or once
      // the commit parks and the in-memory `pending_merge` arm decides it).
      _ => return None,
    };
    let entry = &read[0];
    if entry.kind() != EntryKind::CommitMerge {
      return None;
    }
    let payload = crate::wire::decode_commit_merge_payload(entry.data_bytes()).ok()?;
    Some((payload.source_bytes(), payload.source_gen_after()))
  }
}

impl<I, F, R> Endpoint<I, F, R>
where
  F: StateMachine,
{
  /// Whether this group is FROZEN by an applied `PrepareMerge` (and not since rolled back): it
  /// refuses proposals/conf changes/transfers/reads typed, while replication and elections run
  /// unchanged. See [`freeze_index`](Self::freeze_index) for the boundary.
  pub fn is_frozen(&self) -> bool {
    self.merge.frozen
  }

  /// The applied `PrepareMerge` entry's index while frozen (`None` when not frozen) — the
  /// boundary an absorbing target's parked `CommitMerge` gates on.
  pub fn freeze_index(&self) -> Option<Index> {
    self.merge.freeze_index
  }

  /// The applied `PrepareMerge` entry's term while frozen (`None` when not frozen) — the
  /// boundary's log identity, stamped into the `CommitMerge` the absorbing target proposes.
  pub(crate) fn freeze_term(&self) -> Option<Term> {
    self.merge.freeze_term
  }

  /// The parked `CommitMerge` awaiting the container's resolution, if any. While `Some`, the
  /// apply drain is stopped at [`PendingMergeApply::at`]` - 1` and the group must never be
  /// treated as idle (a parked merge is resolved by the per-crank service, which a quiesced
  /// group would never reach).
  pub fn pending_merge(&self) -> Option<&PendingMergeApply> {
    self.merge.pending_apply.as_ref()
  }

  /// Latch this park's window verdict as CLOSED — the resolver's write, made where it has just
  /// read [`MergeWindow::Closed`] for this park. No-op when nothing is parked.
  pub(crate) fn latch_merge_window_closed(&mut self) {
    if let Some(pending) = self.merge.pending_apply.as_mut() {
      pending.latch_window_closed();
    }
  }

  /// The TARGET id this frozen source's freeze named (`None` when not frozen) — the claim the
  /// `commit_merge` gate and the park's resolve arm verify, so exactly one target can ever
  /// absorb a given freeze generation.
  pub(crate) fn frozen_for(&self) -> Option<&Bytes> {
    self.merge.frozen_for.as_ref()
  }

  /// The index of the LOWEST unapplied `PrepareMerge` this endpoint observed at append, or `None`
  /// — the append-observed kill, read INDEPENDENTLY of [`is_frozen`](Self::is_frozen): the two
  /// are not exclusive. A lagging replica can hold an APPLIED freeze and a PENDING one at once (a
  /// committed unfreeze and a later freeze waiting behind its apply budget), and the pending one
  /// may name a different target. The container's claim gate keys its pending-claim scan on
  /// exactly this, so that later claim is never hidden behind the applied one — which is why an
  /// earlier freeze's fold re-derives it from the suffix above rather than clearing it.
  pub(crate) fn freeze_pending(&self) -> Option<Index> {
    self.merge.freeze_queue.front().copied()
  }

  /// Every pending freeze, lowest first — the queued `PrepareMerge` indices of the unapplied
  /// suffix. The claim gate reads exactly these entries and nothing else.
  pub(crate) fn freeze_queue(&self) -> impl Iterator<Item = Index> + '_ {
    self.merge.freeze_queue.iter().copied()
  }

  /// The apply drain reached the `PrepareMerge` at `index`: it leaves the unapplied suffix, so it
  /// leaves the queue. It is the front by the invariant — the drain applies in log order and every
  /// lower `PrepareMerge` was popped by its own fold — and the next queued index, if any, is the
  /// pending state from here on. Nothing is read from the log.
  pub(crate) fn pop_applied_freeze(&mut self, index: Index) {
    debug_assert_eq!(
      self.merge.freeze_queue.front(),
      Some(&index),
      "the applied PrepareMerge is the freeze queue's front"
    );
    while let Some(popped) = self.merge.freeze_queue.front().copied() {
      if popped > index {
        break;
      }
      self.merge.freeze_queue.pop_front();
      self.merge.claim_cache.remove(&popped);
    }
  }

  /// A refused `PrepareMerge` poisons this endpoint: nothing queued above it ever applies, so the
  /// queue and its read verdicts empty together — the one deliberate break of the queue invariant,
  /// in the empty direction.
  pub(crate) fn clear_freeze_queue(&mut self) {
    self.merge.freeze_queue.clear();
    self.merge.claim_cache.clear();
  }

  /// Whether this group still owes ANY aborted source a thaw — it applied a merge abort as a TARGET
  /// whose durable `abandoned` obligation the container's per-crank thaw pass has not yet discharged.
  /// The service reads it as the thaw pass's per-crank filter and the removal purge's guard, and the
  /// `prepare_merge` source-side gate refuses to DISSOLVE such a group as a fresh merge's source (its
  /// obligations would vanish with its endpoint, stranding the upstream source frozen). The Resolve
  /// arm is DRIVABILITY-gated by contrast: it holds the absorb only while the obligation's owed
  /// target is hosted HERE (so this replica can still drive that thaw); a locally-undrivable dead-end
  /// obligation is dropped by the dissolve by design, since a co-hosting replica drives it and
  /// absorbing here strands nothing. A group with a DRIVABLE obligation outstanding is a merge
  /// participant the embedder MUST NOT remove; recovery is the embedder's catalog, like any dead group.
  pub fn has_abandoned(&self) -> bool {
    !self.merge.abandoned.is_empty()
  }

  /// Every outstanding thaw obligation this target owes: `(source id bytes, the abandoned freeze
  /// generation, the abort entry's index)` — the durable-derived triggers the container service
  /// reads each crank to drive (and discharge) each source-side thaw independently. Re-derived by
  /// replay like `frozen_for`, so each survives a restart from its committed abort entry.
  pub(crate) fn abandoned_obligations(&self) -> Vec<(Bytes, u64, Index)> {
    self
      .merge
      .abandoned
      .iter()
      .map(|(source, (generation, at))| (source.clone(), *generation, *at))
      .collect()
  }

  /// Whether this target hosts a committed abort obligation for exactly this `(source, generation)`
  /// incarnation — the structural derived-from-abort gate's read: a source thaw appends only when
  /// its claimed target owes precisely this freeze generation's thaw.
  pub(crate) fn abandoned_matches(&self, source: &Bytes, generation: u64) -> bool {
    self
      .merge
      .abandoned
      .get(source)
      .is_some_and(|(g, _)| *g == generation)
  }

  /// Record the merge this target-role abort abandoned — inserted at the abort's apply (and re-set
  /// by its replay), so the source-side thaw is DERIVED from the committed target abort, never an
  /// independent source decision. Keyed by source, so a concurrent fan-in of aborts each keeps its
  /// own obligation. LAST-WINS on a repeat of the same source: a re-frozen source (its earlier
  /// obligation already discharged) records the new generation over the spent one — idempotent for
  /// a replayed duplicate (same value), correct for a re-freeze (the live generation wins). A
  /// still-live earlier obligation is never overwritten here: the source must thaw past it (which
  /// discharges it) before it can re-freeze to a higher generation at all.
  pub(crate) fn note_abandoned(&mut self, source_bytes: Bytes, source_gen_after: u64, at: Index) {
    self
      .merge
      .abandoned
      .insert(source_bytes, (source_gen_after, at));
  }

  /// Discharge one abandoned merge — the container service removes the named source's obligation
  /// once that source is observed thawed past its abandoned generation (the thaw committed+applied
  /// on the source log) or floored, releasing the target's compaction fence over that abort entry.
  pub(crate) fn clear_abandoned(&mut self, source: &Bytes) {
    self.merge.abandoned.remove(source);
  }

  /// The abandoned freeze generation this target owes `source` a thaw for, or `None` when it holds
  /// no such obligation — the value of its `abandoned` entry keyed by exactly that source id (the
  /// freeze INCARNATION the target-role abort abandoned). The container's teardown gate reads it
  /// across hosted endpoints and compares it GENERATION-EXACTLY against the candidate source's live
  /// `shape_gen`: removing an OWED source still frozen AT the abandoned generation is the designed
  /// catalog escape (the removal purge clears every holder's obligation), so the freeze gate steps
  /// aside for exactly it — never for a spent obligation the source has already thawed past and
  /// re-frozen above (that record names a DEAD incarnation, not the live freeze being removed).
  pub(crate) fn owes_thaw_for(&self, source: &Bytes) -> Option<u64> {
    self
      .merge
      .abandoned
      .get(source)
      .map(|(generation, _)| *generation)
  }

  /// Whether a merge freeze is ACTIVE right now: an appended-but-unapplied `PrepareMerge`
  /// (freeze-pending, observed at append) OR the applied [`Frozen`](Self::is_frozen) state. The
  /// superset of [`is_frozen`](Self::is_frozen) that also covers the committed-but-unapplied
  /// window, and the ONE predicate every freeze gate reads — the lease gates (serving and
  /// formation die at append observation), the propose-family gates (an entry accepted above the
  /// freeze would diverge or lose the absorbed union), and the container's teardown gate (a
  /// merge-source removal is refused while its target's park still needs it) — so no two sites can
  /// ever disagree about a freeze.
  #[must_use]
  pub fn merge_freeze_active(&self) -> bool {
    !self.merge.freeze_queue.is_empty() || self.merge.frozen
  }

  /// Observe a `PrepareMerge` entering the local log at `index` — the APPEND-time lease kill, and
  /// the freeze queue's push. Appends enter in increasing index order (a conflicting suffix is
  /// retracted by [`note_freeze_truncated`](Self::note_freeze_truncated) before its replacement is
  /// pushed), so the queue stays sorted and the front stays the lowest pending freeze.
  ///
  /// A KIND check arms it, so nothing about the payload has been judged yet, and a queued freeze
  /// has three releases. A truncation covering the entry and the entry's fold at apply (popped
  /// into `frozen`; the next queued index, if any, is the pending state — the queue already holds
  /// every later freeze, so nothing is re-derived from the log) are the ordinary two. The third is
  /// the apply arm's REFUSAL of the entry — a payload that will not decode, or one naming a
  /// reserved generation — which poisons this endpoint and clears the whole queue with it: a
  /// refused entry creates no merge state, nothing queued above it ever applies, and a kill left
  /// armed on a poisoned replica would present as an active freeze to the container's teardown
  /// gate for the lifetime of a committed entry that never leaves the log.
  pub(crate) fn note_freeze_appended(&mut self, index: Index) {
    debug_assert!(
      self.merge.freeze_queue.back().is_none_or(|b| *b < index),
      "freezes are observed in increasing index order"
    );
    if self.merge.freeze_queue.back().is_none_or(|b| *b < index) {
      self.merge.freeze_queue.push_back(index);
    }
    // A fresh index can carry no verdict: its entry was never read as this queue member.
    self.merge.claim_cache.remove(&index);
  }

  /// A §5.3 conflict truncation overwrote `[truncate_from, ..]`: every queued freeze at-or-above
  /// it no longer exists in the log, so it leaves the queue (the new suffix's own freezes are
  /// pushed by the append that follows). A truncation strictly above every queued index leaves
  /// the queue standing — those freeze entries survived.
  pub(crate) fn note_freeze_truncated(&mut self, truncate_from: Index) {
    while self
      .merge
      .freeze_queue
      .back()
      .is_some_and(|b| *b >= truncate_from)
    {
      self.merge.freeze_queue.pop_back();
    }
    // The overwritten entries may come back with different claims: their verdicts go with them.
    self.merge.claim_cache.retain(|i, _| *i < truncate_from);
  }

  /// A snapshot install re-baselined the log to `boundary`, discarding every entry above it: a
  /// queued freeze above the boundary was discarded with them — clear the queue; the ordinary
  /// append re-delivery of a still-live freeze re-queues it at accept. (A queued freeze at-or-below
  /// the boundary is structurally impossible — compaction happens only at applied indexes, and an
  /// applied `PrepareMerge` already left the queue at its fold — so the clear is total rather than
  /// conditional; a stale entry surviving here would kill leases forever on a node whose freeze
  /// entry no longer exists.)
  pub(crate) fn note_freeze_rebaselined(&mut self) {
    self.merge.freeze_queue.clear();
    self.merge.claim_cache.clear();
  }

  /// A snapshot install re-baselined the log to `boundary`, discarding every abort entry at-or-below
  /// it: drop each obligation whose abort entry the boundary COVERS. The installed snapshot sits past
  /// a committed-and-applied abort (a non-redundant install re-baselines strictly above `commit`, and
  /// an obligation is set at apply, so `abort_index <= applied <= commit < boundary`), so the covered
  /// obligation is already RESOLVED by the envelope §4.5 invariant — covered ⟹ the source was THAWED
  /// (this leader's own service drove the thaw past its abandoned freeze before compacting) OR
  /// ESCAPE-FLOORED (the source was removed and purged, its incarnation fenced by escape⟹terminal-floor
  /// and its frozen remnant torn down by the husk-dissolve pass). It does NOT prove a thaw in
  /// particular: the removal-purge leg never thaws, which is why the earlier "the leader proved the
  /// source thawed" reading was wrong. A covered obligation is MOOT, and its ONLY restart re-derivation
  /// (replaying the abort entry) was just discarded: keeping it would strand `abort_relay_fences` on
  /// a boundary the install already crossed — a permanent target-capture wedge with the source thawed
  /// and gone (the service could never observe it advance to discharge it). An obligation whose abort
  /// entry is ABOVE the boundary is RETAINED: the install does not prove that source past THAT freeze,
  /// and the re-delivered entry re-applies to re-derive it (symmetric with the fence's own
  /// `abort_index <= boundary` test). Mirrors the restart path, whose replay re-derives obligations
  /// only from surviving entries — a below-boundary abort is equally absent there, so runtime install
  /// and restart agree.
  pub(crate) fn note_abort_rebaselined(&mut self, boundary: Index) {
    self
      .merge
      .abandoned
      .retain(|_, (_, abort_index)| *abort_index > boundary);
  }

  /// One bounded, kind-only pass over the UNAPPLIED suffix `(applied, last]` collecting EVERY
  /// `PrepareMerge` index in log order — the REBUILD of [`MergeState::freeze_queue`] at restart,
  /// and the only suffix walk the queue ever costs: every live path maintains it at append, and a
  /// cure adopt keeps the log and so keeps the queue's own entries above its boundary. FAIL-STOP
  /// on any read fault, like the restart lease-floor scans: under-deriving the kill would let a
  /// restarted replica win an election and serve a lease inside a pending freeze — a stale read.
  pub(crate) fn scan_freeze_pending<L: LogStore>(
    log: &L,
    applied: Index,
  ) -> Result<VecDeque<Index>, PoisonReason> {
    let mut queue = VecDeque::new();
    let last = log.last_index();
    let mut idx = applied.next();
    while idx <= last {
      let read_end = last
        .next()
        .min(Index::new(idx.get().saturating_add(MAX_READ_BATCH_ENTRIES)));
      let chunk = match log.entries(idx..read_end, 1 << 20) {
        Ok(EntriesRead::Ready(c)) if !c.is_empty() => c,
        _ => return Err(PoisonReason::LogRead),
      };
      for e in chunk.iter() {
        if e.kind() == EntryKind::PrepareMerge {
          queue.push_back(e.index());
        }
      }
      idx = chunk
        .last()
        .map(|e| e.index().next())
        .ok_or(PoisonReason::LogRead)?;
    }
    Ok(queue)
  }

  /// The TARGET claims of the queued pending freezes ([`freeze_queue`](Self::freeze_queue)),
  /// decoded in log order. The container's claim gate runs it against a freeze-pending SOURCE to
  /// learn which targets its not-yet-applied freezes claim, closing the pre-park window an applied
  /// [`frozen_for`](Self::frozen_for) read cannot see (a payload is undecoded until its freeze
  /// applies). EVERY queued claim, not the first: an apply-starved replica's committed suffix can
  /// hold several freeze cycles (`Unfreeze0, Prepare1, Unfreeze1, Prepare2` behind an applied
  /// freeze for a target 0), and each later freeze's claim stands in turn as the drain catches up,
  /// so a gate that read only the lowest would leave every later target undefended. Run for every
  /// source with a pending freeze, applied-frozen or not.
  ///
  /// Reads EXACTLY the queued entries, one single-entry read each — never a walk of the suffix —
  /// and each of them ONCE: every Ready verdict is cached on this endpoint
  /// ([`MergeState::claim_cache`]) and only the uncached indices are read, so the walk RESUMES
  /// after a cold page instead of restarting. Against a bounded cache that pages the suffix in
  /// and out a restarting walk livelocks (two required pages, one page of cache: each read evicts
  /// the other's) and the gate answers a standing refusal forever; here a cold page costs only the
  /// indices at and above it, and the next attempt makes progress. A queued entry that is not
  /// readable (a cold page, an empty read, a store error) ends the attempt at once with `LogRead`,
  /// which the caller treats as a claim it cannot rule out and refuses on — it retries on its own
  /// cadence, never here.
  ///
  /// Each entry is judged with [`shape_entry_move`](crate::shape_entry_move) — the apply arm's own
  /// admission, run ahead of it — and what a refused entry means depends on whether it is
  /// COMMITTED. At-or-below `commit` it ENDS the walk: a payload that will not decode, or one
  /// naming a reserved generation, is the entry that poisons this source the moment the drain
  /// reaches it, so nothing above it ever applies and only the claims collected below it can still
  /// materialize — reading it, or anything past it, as a claim would fence a target forever against
  /// a source that can never absorb into it. ABOVE `commit` the same entry is indeterminate: a
  /// conflicting append may truncate it and put a VALID freeze at the same index, one that re-arms
  /// the kill and creates the very claim a "no claim" answer would have let the gate act on —
  /// removing the target that freeze names, or freezing it as another merge's source, and
  /// stranding the claimant with no target log for its commit or rollback. So an uncommitted
  /// refused entry stays FAIL-CLOSED (`Err(MergeDecode)`; nothing poisons) until it is truncated
  /// or committed. Off the hot path — appends stay kind-only, and this pays its decodes only per
  /// (rare) removal or freeze proposal.
  pub(crate) fn scan_freeze_claims<L: LogStore>(
    &mut self,
    log: &L,
  ) -> Result<Vec<Bytes>, PoisonReason> {
    let commit = self.commit;
    let mut claims = Vec::new();
    // Walked by value: each step may write the cache.
    let queued: Vec<Index> = self.freeze_queue().collect();
    for idx in queued {
      let cached = match self.merge.claim_cache.get(&idx) {
        // Judged above the commit of its day and committed since: the same bytes now poison this
        // source at apply. Re-read rather than promoted, so no verdict here is ever inferred.
        Some(ClaimRead::RefusedAbove { judged_at }) if commit >= idx => {
          debug_assert!(*judged_at < idx);
          None
        }
        cached => cached.cloned(),
      };
      let verdict = match cached {
        Some(verdict) => verdict,
        None => {
          let verdict = Self::read_claim(log, idx, commit)?;
          self.merge.claim_cache.insert(idx, verdict.clone());
          verdict
        }
      };
      match verdict {
        ClaimRead::Claim(claim) => claims.push(claim),
        ClaimRead::RefusedCommitted => return Ok(claims),
        ClaimRead::RefusedAbove { .. } => return Err(PoisonReason::MergeDecode),
      }
    }
    Ok(claims)
  }

  /// ONE Ready single-entry read of the queued `PrepareMerge` at `idx`, judged under `commit` —
  /// the read [`scan_freeze_claims`](Self::scan_freeze_claims) caches. Anything but a Ready read
  /// of a `PrepareMerge` at exactly `idx` is `LogRead`: the gate cannot rule the claim out.
  fn read_claim<L: LogStore>(
    log: &L,
    idx: Index,
    commit: Index,
  ) -> Result<ClaimRead, PoisonReason> {
    let read = match log.entries(idx..idx.next(), 1 << 20) {
      Ok(EntriesRead::Ready(c)) if !c.is_empty() => c,
      _ => return Err(PoisonReason::LogRead),
    };
    let Some(e) = read
      .iter()
      .find(|e| e.index() == idx && e.kind() == EntryKind::PrepareMerge)
    else {
      return Err(PoisonReason::LogRead);
    };
    Ok(match crate::shape_entry_move(e) {
      // A `Valid` verdict already decoded these bytes, so the decode cannot fail; a store that
      // somehow contradicts it is read as unreadable, never as a claim.
      crate::ShapeMove::Valid(_) => {
        let payload = crate::wire::decode_prepare_merge_payload(e.data_bytes())
          .map_err(|_| PoisonReason::LogRead)?;
        ClaimRead::Claim(payload.target_bytes())
      }
      // Refused: committed, it poisons this source at apply and nothing above it ever applies;
      // uncommitted, a valid freeze can still replace it at this very index, so it is not a
      // "no" — and the verdict remembers the commit it was judged under.
      _ if idx <= commit => ClaimRead::RefusedCommitted,
      _ => ClaimRead::RefusedAbove { judged_at: commit },
    })
  }
}

impl<I, F, R> Endpoint<I, F, R>
where
  F: StateMachine,
{
  /// Consume this endpoint into its state machine — the absorb extraction: the container
  /// removes the frozen source from its map and hands the FSM to the target's
  /// `resolve_pending_merge`. Everything else about the endpoint
  /// (its stores outlive it until the driver's teardown) is dropped; the log-derived state is
  /// re-derivable should a crash rewind the resolution.
  pub fn into_state_machine(self) -> F {
    self.fsm
  }

  /// Whether ANY outstanding obligation FENCES a TARGET capture/compaction at `boundary` — true
  /// when some abort entry sits at-or-below it. That entry's replay is its obligation's ONLY restart
  /// re-derivation (the abort index in the map value), so a capture at-or-past it would compact the
  /// entry and lose the obligation across a restart with the source still frozen — a permanent
  /// frozen-source wedge. The SINGLE predicate every target-capture site shares so none can drift:
  /// `maybe_snapshot` checks it at `applied`, the forced `capture_absorb_snapshot` at the absorb
  /// boundary `pending.at()` (via `absorb_capture_blocked`). A snapshot INSTALL is the one
  /// floor-advance that can cross an abort entry NO local fenced capture produced — it re-baselines to
  /// a LEADER's boundary — so it does not lean on the fence: it CLEARS every covered obligation
  /// (`note_abort_rebaselined`), sound because that boundary proves each covered source thawed past
  /// its abandoned freeze. Every other floor-advance is covered transitively — the deferred
  /// `log.compact` and the restart reconciliation only reach a boundary a fenced capture (or a
  /// clearing install) already produced — so an abort entry leaves the durable log only with its
  /// obligation either fenced or discharged. The fence lifts per source when the service DISCHARGES
  /// its obligation — that source observed thawed past its abandoned generation (its committed thaw
  /// applied) or floored — so the erased entry's replay is by then moot. This is the discharge-gated
  /// durability release: the target keeps each abort entry until its source's unfreeze commits.
  pub(crate) fn abort_relay_fences(&self, boundary: Index) -> bool {
    self
      .merge
      .abandoned
      .values()
      .any(|(_, abort_index)| *abort_index <= boundary)
  }

  /// Whether a TARGET capture/compaction at `boundary` is REFUSED right now — the ONE busy/fence
  /// set every capture producer shares (`maybe_snapshot` at `applied`, the forced absorb capture
  /// at the absorb boundary via [`absorb_capture_block`](Self::absorb_capture_block) (the caller classified the fence and reached `Clear`)), so no
  /// site can drift from the others. The legs:
  ///
  /// - a capture or install is already STAGED (`pending_compact`/`pending_install`) — firing
  ///   another would overwrite the staged operation's identity mid-flight;
  /// - THE FORK DURABILITY BARRIER: a staged fork's only recovery source is re-applying its
  ///   `Split` entry, which dies the moment this endpoint snapshots at-or-past that index (the
  ///   compaction discards the entry) — refuse until every such fork is RESOLVED;
  /// - THE ABORT REPLAY FENCE: an outstanding `abandoned` obligation is re-derivable solely by
  ///   replaying its abort entry — a capture at-or-past it erases the obligation's only restart
  ///   source with the owed source possibly still frozen (see `abort_relay_fences`);
  /// - THE MERGE REPLAY FENCE: an APPLIED freeze holds unconditionally — the fold itself would
  ///   advance state a claiming target pinned at its freeze boundary, and the freeze lifts by
  ///   protocol (the thaw, or this group's own dissolution by the claimant). A PENDING
  ///   `PrepareMerge` fences by BOUNDARY: a capture at-or-past its index compacts the entry
  ///   whose replay is the freeze's only restart derivation, so a crash restarts this replica
  ///   UNFROZEN while the claimant still holds a parked absorb at the freeze boundary — but a
  ///   pending freeze ABOVE the boundary survives the compaction untouched, and holding an
  ///   earlier fold on it is a restart-replay circular wait (the park below is exactly what
  ///   keeps that freeze from ever applying).
  pub(crate) fn capture_blocked_at(&self, boundary: Index) -> bool {
    self.snapshot.pending_compact.is_some()
      || self.snapshot.pending_install.is_some()
      || self
        .split
        .outstanding
        .first()
        .is_some_and(|cap| *cap <= boundary)
      || self.abort_relay_fences(boundary)
      || self.merge.frozen
      || self.freeze_pending().is_some_and(|f| f <= boundary)
  }

  /// Which STRUCTURAL leg of [`capture_blocked_at`](Self::capture_blocked_at) stands at
  /// `boundary` — the observability signal's cause, classified from the same predicate the gate
  /// itself reads so the two cannot drift. The TRANSIENT legs deliberately have no variant: a
  /// staged capture or install drains within cranks and is this endpoint's own business, so
  /// `None` covers both "nothing blocks" and "only a transient does" and the caller signals
  /// neither. Precedence mirrors [`absorb_capture_block`](Self::absorb_capture_block): the freeze
  /// is a HOLD leg and outranks the two replay fences it can stand alongside.
  pub(crate) fn capture_fence_at(&self, boundary: Index) -> Option<CaptureFence> {
    if self.merge.frozen || self.freeze_pending().is_some_and(|f| f <= boundary) {
      Some(CaptureFence::Frozen)
    } else if self
      .split
      .outstanding
      .first()
      .is_some_and(|cap| *cap <= boundary)
    {
      Some(CaptureFence::Fork)
    } else if self.abort_relay_fences(boundary) {
      Some(CaptureFence::Abort)
    } else {
      None
    }
  }

  /// Whether the absorb's forced snapshot capture would be REFUSED right now — the shared
  /// [`capture_blocked_at`](Self::capture_blocked_at) set keyed at the absorb boundary
  /// `pending.at()` (the capture compacts there). The tests' modeling seam for the resolve
  /// arm's gate; production reads the three-way [`absorb_capture_block`](Self::absorb_capture_block).
  #[cfg(test)]
  pub(crate) fn absorb_capture_blocked(&self) -> bool {
    !matches!(self.absorb_capture_block(), AbsorbCaptureBlock::Clear)
  }

  /// The resolve arm's three-way classification of the absorb-capture fence at the park:
  ///
  /// - `Hold`: a staged capture/install (drains within cranks) or a live freeze (the fold itself
  ///   would advance state a claiming target pinned at its freeze boundary, and the freeze lifts
  ///   by protocol — the thaw, or this group's own dissolution by the claimant). The park stays.
  /// - `Defer`: only a REPLAY fence stands — a fork's durability barrier or an undischarged
  ///   abort obligation. The fold is safe NOW; only the capture's compaction must wait for the
  ///   fence. The arm absorbs, unparks, and records the capture as a debt — deferring the park
  ///   instead would wedge it for as long as the fence stands, and the abort fence can even be
  ///   UNDISCHARGEABLE behind the park (its clearing witness rides an entry above the park that
  ///   the park itself keeps from applying).
  /// - `Clear`: absorb + capture land in this crank, as one barrier.
  pub(crate) fn absorb_capture_block(&self) -> AbsorbCaptureBlock {
    let Some(pending) = self.merge.pending_apply.as_ref() else {
      return AbsorbCaptureBlock::Clear;
    };
    // ONE debt at a time is a HOST-LOCAL invariant: the propose-time reshape gates run on the
    // proposing leader, whose own fences may be clear while THIS host's still stand — so a
    // second committed absorb can legally park here mid-window, and deferring it would
    // overwrite the first debt's held `Merged`, stranding that source's stores forever. Hold
    // the park instead: the discharge is independent of it and releases it on its own crank.
    if self.merge.capture_debt.is_some() {
      return AbsorbCaptureBlock::Hold;
    }
    let verdict = if self.snapshot.pending_compact.is_some()
      || self.snapshot.pending_install.is_some()
      || self.merge.frozen
      || self.freeze_pending().is_some_and(|f| f <= pending.at())
    {
      AbsorbCaptureBlock::Hold
    } else if self
      .split
      .outstanding
      .first()
      .is_some_and(|cap| *cap <= pending.at())
      || self.abort_relay_fences(pending.at())
    {
      AbsorbCaptureBlock::Defer
    } else {
      AbsorbCaptureBlock::Clear
    };
    // The two predicates must never drift: Clear here has to mean exactly "the shared fence set
    // is clear at the absorb boundary" — a leg added to one and not the other would fire a
    // blocked capture.
    debug_assert!(
      (verdict == AbsorbCaptureBlock::Clear) == !self.capture_blocked_at(pending.at()),
      "absorb_capture_block drifted from capture_blocked_at"
    );
    verdict
  }

  /// Whether an adopt still owes its forced capture (see [`MergeState::adopt_capture_owed`]).
  pub(crate) fn adopt_capture_owed(&self) -> bool {
    self.merge.adopt_capture_owed
  }

  /// The owed capture staged (or the park died another way) — clear the obligation.
  pub(crate) fn clear_adopt_capture_owed(&mut self) {
    self.merge.adopt_capture_owed = false;
  }

  /// Whether any merge-cure debt stands — the drivers' quiesce-eligibility leg: a wedged peer is
  /// invisible to the pump predicate (it is not log-lagging), so the debt is what keeps the group
  /// awake until the cure lands.
  pub fn has_cure_debts(&self) -> bool {
    !self.cure_owed.is_empty()
  }

  /// The outstanding absorbed-but-uncaptured union obligation, if any: a replay fence deferred
  /// the absorb's forced durability capture, so the consumed source's preserved stores remain
  /// the union's only restart derivation until a capture (or superseding install) at-or-past
  /// the absorb boundary stages. Holds the `Merged` the discharge will surface. While set, the
  /// reshape verbs refuse this group both roles and a leader is never quiesce-eligible.
  pub fn capture_debt(&self) -> Option<&crate::Merged> {
    self.merge.capture_debt.as_ref()
  }

  /// Every debt this endpoint holds — its own, then the inherited chain. The consuming
  /// teardowns validate and surface the WHOLE chain, and the naming scans pin every listed
  /// source's preserved stores.
  pub(crate) fn capture_debt_chain(&self) -> impl Iterator<Item = &crate::Merged> {
    self
      .merge
      .capture_debt
      .iter()
      .chain(self.merge.inherited_debts.iter())
  }

  /// Whether any held debt (own or inherited) names `key` as its absorbed source.
  pub(crate) fn debt_names_source(&self, key: &Bytes) -> bool {
    self.capture_debt_chain().any(|m| m.source() == *key)
  }

  /// Drain the WHOLE chain (own debt first) — the consuming teardowns' take: the caller
  /// surfaces every entry or hands them to the absorbing target.
  pub(crate) fn take_capture_debts(&mut self) -> std::vec::Vec<crate::Merged> {
    let mut chain = std::vec::Vec::with_capacity(1 + self.merge.inherited_debts.len());
    chain.extend(self.merge.capture_debt.take());
    chain.append(&mut self.merge.inherited_debts);
    chain
  }

  /// Adopt a consumed source's drained chain as inherited debts (the `Defer` arm, after its own
  /// mint): they discharge with this endpoint's own debt on the same covering capture.
  pub(crate) fn adopt_inherited_debts(&mut self, debts: std::vec::Vec<crate::Merged>) {
    debug_assert!(
      self.merge.capture_debt.is_some() || debts.is_empty(),
      "inherited debts ride an own debt's discharge"
    );
    self.merge.inherited_debts.extend(debts);
  }

  /// Drain the inherited chain at the own debt's discharge.
  pub(crate) fn take_inherited_debts(&mut self) -> std::vec::Vec<crate::Merged> {
    core::mem::take(&mut self.merge.inherited_debts)
  }

  /// A covering install re-baselined this endpoint: the debt chain is SUPERSEDED, not
  /// discharged — the blob's producer already ran the teardown barrier the held `Merged`s
  /// would have authorized, and the prior sources' terminal floors reach this host by
  /// propagation. Cleared without surfacing anything.
  pub(crate) fn note_debts_rebaselined(&mut self) {
    self.merge.capture_debt = None;
    self.merge.inherited_debts.clear();
  }

  /// The staged (submitted, not yet durability-completed) capture's boundary, if one is in
  /// flight — the debt pass adopts a staged capture at-or-past the absorb boundary as the
  /// debt's own discharge (boundary coverage is monotone in `applied`).
  /// Whether this endpoint's DURABLE snapshot already covers `boundary` — durability
  /// evidence ONLY: a completion-time redundant install raises this index while deliberately
  /// keeping the log, so a caller discharging a compaction-bearing obligation on it must pair
  /// it with the membership fence's own release.
  pub(crate) fn durable_snapshot_covers(&self, boundary: Index) -> bool {
    self.durable.durable_snapshot_index >= boundary
  }

  /// The staged (submitted, not yet durability-completed) capture's boundary, if one is in
  /// flight — the debt pass adopts a staged capture at-or-past the absorb boundary as the
  /// debt's own discharge (boundary coverage is monotone in `applied`).
  pub(crate) fn pending_compact_boundary(&self) -> Option<Index> {
    self
      .snapshot
      .pending_compact
      .as_ref()
      .map(|(_, meta)| meta.last_index())
  }

  /// Record the deferred capture's obligation after a fence-deferred absorb (the resolve arm's
  /// `Defer` leg). The payload is the `Merged` the discharge will surface.
  pub(crate) fn mint_capture_debt(&mut self, merged: crate::Merged) {
    debug_assert!(
      self.merge.capture_debt.is_none(),
      "one absorb at a time: the reshape gates refuse while a debt stands"
    );
    self.merge.capture_debt = Some(merged);
  }

  /// Take the debt for discharge — the caller staged (or observed) a capture at-or-past the
  /// absorb boundary and now surfaces the held `Merged`.
  pub(crate) fn discharge_capture_debt(&mut self) -> Option<crate::Merged> {
    self.merge.capture_debt.take()
  }

  /// Advance the incremental crossing walk over the committed range above the park (see
  /// [`MergeState::crossing_sources`]). Kind-only plus one payload decode per hit; an
  /// unreadable page simply stops this crank's advance (the hint stays withheld — fail-closed).
  pub(crate) fn advance_crossing_scan<L: LogStore>(&mut self, log: &L) {
    let Some(park_at) = self.merge.pending_apply.as_ref().map(PendingMergeApply::at) else {
      return;
    };
    let last = log.last_index().min(self.commit);
    let mut idx = self.merge.crossing_scan_upto.max(park_at).next();
    while idx <= last {
      let read_end = last
        .next()
        .min(Index::new(idx.get().saturating_add(MAX_READ_BATCH_ENTRIES)));
      let chunk = match log.entries(idx..read_end, 1 << 20) {
        Ok(EntriesRead::Ready(c)) if !c.is_empty() => c,
        // Benign transient unreadiness: the walk stays partial and retries next crank.
        Ok(_) => {
          self.merge.crossing_scan_current = false;
          return;
        }
        // A genuine store fault in committed content the parked drain can never reach to
        // poison itself — surface it here or nowhere.
        Err(_) => {
          self.merge.crossing_scan_current = false;
          self.poison(PoisonReason::LogRead);
          return;
        }
      };
      for e in chunk.iter() {
        if e.kind() == EntryKind::CommitMerge {
          match crate::wire::decode_commit_merge_payload(e.data_bytes()) {
            Ok(p) => self.merge.crossing_sources.push(p.source_bytes()),
            // Committed-corrupt content the parked drain can never reach to poison itself —
            // fail-stop HERE, exactly as the drain would have, or the park wedges forever
            // behind a withheld cure while misreporting an unhosted-source hold.
            Err(_) => {
              self.merge.crossing_scan_current = false;
              self.poison(PoisonReason::MergeDecode);
              return;
            }
          }
        }
      }
      match chunk.last() {
        Some(e) => {
          self.merge.crossing_scan_upto = e.index();
          idx = e.index().next();
        }
        None => {
          self.merge.crossing_scan_current = false;
          return;
        }
      }
    }
    self.merge.crossing_scan_current = true;
  }

  /// Whether this crank's walk reached the committed frontier — the hint's completeness leg.
  /// (A payload that fails to decode never reaches here: the walk fail-stops on it, the
  /// committed-corrupt class the parked drain could not raise itself.)
  pub(crate) fn crossing_scan_current(&self) -> bool {
    self.merge.crossing_scan_current
  }

  /// Whether the crossing walk has EXAMINED every entry through `boundary` — the adopt's
  /// admission bind. The commit index is not the right key: a delivery can raise commit between
  /// resolver cranks, and a duplicate arriving before the next crank would then pass a
  /// commit-keyed gate while the walk still stops at the older frontier — adopting across an
  /// interval never examined. The watermark moves only when the walk itself moves.
  pub(crate) fn crossing_walk_covers(&self, boundary: Index) -> bool {
    self.crossing_scan_current()
      && boundary
        <= self.merge.crossing_scan_upto.max(
          // A pristine park (no entries above it yet examined because none exist below the
          // frontier) covers exactly the park coordinate itself.
          self
            .merge
            .pending_apply
            .as_ref()
            .map_or(Index::ZERO, PendingMergeApply::at),
        )
  }

  /// The committed crossings recorded so far (see [`MergeState::crossing_sources`]).
  pub(crate) fn crossing_sources(&self) -> &[Bytes] {
    &self.merge.crossing_sources
  }

  /// The standing park's boundary when the container's resolver classified it locally
  /// unresolvable — the source is unhosted here with a non-terminal floor, so no local fold can
  /// ever land, and the advertisement gate passed: the coordinate this follower advertises for
  /// the cure, and the receipt-time adopt's admission key. Volatile, re-derived every resolver
  /// crank; `None` whenever no park stands.
  pub fn merge_park_unresolvable(&self) -> Option<Index> {
    if self.merge.park_unresolvable {
      self.merge.pending_apply.as_ref().map(PendingMergeApply::at)
    } else {
      None
    }
  }

  /// Record (or clear) the resolver's per-crank unresolvable classification.
  pub(crate) fn note_merge_park_unresolvable(&mut self, unresolvable: bool) {
    self.merge.park_unresolvable = unresolvable;
  }

  /// Resolve the parked `CommitMerge` by ABSORBING the extracted source state machine: fold it
  /// in, mark the parked entry applied, and bump the lineage. Returns the `Merged` payload for
  /// the caller to surface via [`emit_merged`](Self::emit_merged) ONLY once the absorb's forced
  /// durable capture has staged. The event is the driver's permission to floor the source
  /// terminally and drop its stores, so it must never surface on a capture fault that poisons the
  /// target and withholds the resolution. The in-memory mutations here are volatile — they die
  /// with a poison — so it is the EVENT alone that carries an external side effect and must be
  /// gated on a staged capture (mirrors the container's staged-capture arm). Returns `None` when
  /// the absorb is refused: nothing was folded in.
  ///
  /// The ONLY writer of a successful resolution; the caller (the container's per-crank service)
  /// verified the source was frozen-applied at the boundary with the expected gen, so the
  /// absorbed state is identical on every replica — log-matching plus deterministic apply up
  /// to the boundary, with nothing FSM-mutating above a surviving freeze.
  ///
  /// An FSM whose `absorb` returns `false` (the defaulted unsupported verdict) poisons —
  /// deterministic on every replica, mirroring `SplitUnsupported`: never a silent skip that
  /// diverges absorbed replicas from refusing ones.
  pub(crate) fn resolve_pending_merge(&mut self, source_fsm: F) -> Option<crate::Merged> {
    let Some(pending) = self.merge.pending_apply.take() else {
      debug_assert!(false, "resolve without a parked CommitMerge");
      return None;
    };
    self.merge.crossing_sources.clear();
    self.merge.crossing_scan_upto = Index::ZERO;
    self.merge.crossing_scan_current = false;
    if !self.fsm.absorb(source_fsm) {
      self.poison(PoisonReason::MergeUnsupported);
      return None;
    }
    self.applied = pending.at();
    self.split.shape_gen = self.split.shape_gen.max(pending.target_gen_after());
    self.merge.absorb_index = Some(pending.at());
    // The post-absorb counter rides the event — the driver's engine mirror (INV-LINEAGE).
    Some(crate::Merged::new(
      pending.at(),
      pending.source_bytes(),
      self.split.shape_gen,
    ))
  }

  /// Surface the union [`Event::Merged`](crate::Event::Merged) — the driver's permission to floor
  /// the source terminally and drop its stores. The container calls this ONLY once the absorb's
  /// forced capture has staged its snapshot/compaction, so the event never claims a durable union
  /// a capture fault withheld: a poisoned target surfaces nothing.
  pub(crate) fn emit_merged(&mut self, m: crate::Merged) {
    self.outputs.events.push_back(crate::Event::Merged(m));
  }

  /// Resolve the parked `CommitMerge` as a deterministic NO-OP: the source's log settled the
  /// race against this commit (a rollback moved its lineage past the expected gen), or the
  /// entry is a replayed/duplicate commit for an already-absorbed source. Advances past the
  /// parked entry WITHOUT touching the state machine or the lineage and surfaces
  /// `Event::MergeAborted`; the drain resumes on the next storage crank.
  pub(crate) fn resolve_pending_merge_aborted(&mut self) {
    let Some(pending) = self.merge.pending_apply.take() else {
      debug_assert!(false, "abort-resolve without a parked CommitMerge");
      return;
    };
    self.merge.crossing_sources.clear();
    self.merge.crossing_scan_upto = Index::ZERO;
    self.merge.crossing_scan_current = false;
    self.applied = pending.at();
    self
      .outputs
      .events
      .push_back(crate::Event::MergeAborted(crate::MergeAborted::new(
        pending.at(),
        pending.source_bytes(),
        self.split.shape_gen,
      )));
  }

  /// Whether a membership change is still in flight (appended, not yet applied) — the merge
  /// verbs' precondition read (the propose gate itself lives in `propose_conf_change_v2`).
  pub(crate) fn conf_change_in_flight(&self) -> bool {
    self.pending_conf_index > self.applied
  }

  /// Whether a `CommitMerge` proposed by THIS leader is still unapplied — the target-side
  /// one-at-a-time gate's in-flight leg (the parked leg is `pending_merge()`).
  pub(crate) fn commit_merge_in_flight(&self) -> bool {
    self.merge.pending_commit_index > self.applied
  }

  /// Whether a `RollbackMerge` is appended-and-unapplied on this leader's log — the merge verbs'
  /// lineage fence (see [`MergeState::pending_rollback_index`]). A target-role abort here bumps
  /// `shape_gen` when it applies, so `commit_merge`/`prepare_merge` refuse to mint a generation it
  /// would stale until it clears (`> applied`).
  pub(crate) fn rollback_in_flight(&self) -> bool {
    self.merge.pending_rollback_index > self.applied
  }

  /// The index of a SOURCE-role `RollbackMerge` (thaw) this leader has appended but not yet
  /// applied, if any — the abort-relay's idempotent-append guard. `Some` means the accept arm
  /// must RETAIN and wait rather than append a duplicate; the retained relay retires only once
  /// the thaw commits, applies, and the source lineage is observed past the freeze.
  pub(crate) fn thaw_in_flight(&self) -> Option<Index> {
    (self.merge.thaw_pending_index > self.applied).then_some(self.merge.thaw_pending_index)
  }

  /// Record a SOURCE-role thaw this leader just appended at `index` — arms
  /// [`thaw_in_flight`](Self::thaw_in_flight) so the relay's next crank does not pile on a
  /// duplicate `RollbackMerge` while this one is in flight.
  pub(crate) fn note_thaw_appended(&mut self, index: Index) {
    self.merge.thaw_pending_index = index;
  }

  /// The index of a `ThawDischarged` WITNESS this leader has appended (on its own target log) but not
  /// yet applied, if any — the witness mint's idempotent-append guard. `Some` means the thaw pass must
  /// RETAIN rather than append another witness this crank; it retires once the witness commits, applies
  /// (clearing the obligation), and the lifted obligation stops the pass from re-observing it.
  pub(crate) fn witness_in_flight(&self) -> Option<Index> {
    (self.merge.witness_pending_index > self.applied).then_some(self.merge.witness_pending_index)
  }

  /// Record a `ThawDischarged` witness this leader just appended at `index` — arms
  /// [`witness_in_flight`](Self::witness_in_flight) so the thaw pass does not pile on a duplicate
  /// witness while this one is in flight.
  pub(crate) fn note_witness_appended(&mut self, index: Index) {
    self.merge.witness_pending_index = index;
  }

  /// The target-side membership fence: whether a conf change must refuse because a merge is in
  /// flight (proposed, parked, or absorbed-but-not-yet-compacted). Adding a replica in any of
  /// those windows lets it be LOG-WALKED across the absorb point — it parks there with no local
  /// source and no floor, no-ops past the union, and silently diverges (the fork milestone's
  /// log-walk lesson). The three legs release on their own: apply absorbs the in-flight index,
  /// resolution consumes the park, and the absorb capture's compaction moves `first_index`
  /// past the absorb point within a crank.
  ///
  /// An outstanding abort obligation (`has_abandoned`) deliberately does NOT fence here: a voter
  /// joining a group that owes a thaw re-derives that obligation, and if it never hosts the owed
  /// group the obligation is a local dead end — but the drivability belt in the resolve arm drops
  /// exactly such a dead end at the absorb, so the joiner never wedges (the world test
  /// `a_dead_end_obligation_does_not_wedge_a_co_hosted_absorb` pins this). Fencing joins here would
  /// forbid the legitimate growth of a target that is aborting an inbound merge.
  pub(crate) fn merge_conf_fence<L: LogStore>(&self, log: &L) -> bool {
    self.merge.pending_commit_index > self.applied
      || self.merge.pending_apply.is_some()
      || self
        .merge
        .absorb_index
        .is_some_and(|k| log.first_index() <= k)
  }
}

impl<I, F, R> Endpoint<I, F, R>
where
  I: NodeId,
  F: StateMachine,
  R: rand::Rng,
  F::Command: crate::Data,
  F::Error: core::error::Error,
{
  /// The absorb's FORCED snapshot capture, run by the container immediately after
  /// [`resolve_pending_merge`](Self::resolve_pending_merge) with the target's stores in hand:
  /// capture at `applied` (the absorb point) regardless of the snapshot threshold, so the
  /// union's durability anchor and the source's teardown ride the SAME barrier — a crash either
  /// rewinds to re-park (nothing durable moved) or restarts the target at a boundary past the
  /// absorb (never a parked apply whose source was already destroyed). The caller checked
  /// [`absorb_capture_blocked`](Self::absorb_capture_blocked) before resolving.
  ///
  /// Returns whether the capture actually STAGED `pending_compact` — the union's durable anchor.
  /// The container's resolve arm emits `Merged` (the driver's permission to floor the source and
  /// drop its stores) ONLY on `true`: a `snapshot()` or log fault poisons and returns `false`, so
  /// an absorb whose union could not be anchored fail-stops with the source's stores intact rather
  /// than authorizing a teardown over a union no durable target snapshot covers.
  #[must_use]
  pub(crate) fn capture_absorb_snapshot<L, S>(&mut self, log: &L, stable: &mut S) -> bool
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
    F::Snapshot: crate::Data,
  {
    if self.poison.poisoned {
      return false;
    }
    debug_assert!(
      self.snapshot.pending_compact.is_none() && self.snapshot.pending_install.is_none(),
      "the resolve arm holds the park while a capture/install is staged"
    );
    let snap = match self.fsm.snapshot() {
      Ok(s) => s,
      Err(_) => {
        self.poison(PoisonReason::SnapshotCapture);
        return false;
      }
    };
    use crate::Data as _;
    let mut data = std::vec::Vec::new();
    snap.encode(&mut data);
    let Some(last_term) = self.log_term(log, self.applied) else {
      // The absorb point must be readable to stamp the snapshot meta; a fault here leaves the
      // union unanchored — fail-stop rather than stage nothing and let the caller emit Merged.
      self.poison(PoisonReason::LogRead);
      return false;
    };
    let mut meta = crate::SnapshotMeta::new(self.applied, last_term, self.conf_state())
      .with_max_lease_window(self.lease_guard.max_lease_window)
      .with_max_wall_plus_window(self.lease_guard.max_wall_plus_window)
      .with_max_unwalled_lease_window(self.lease_guard.max_unwalled_lease_window)
      .with_shape_gen(self.split.shape_gen);
    if self.reads.read_mode_migrated {
      meta = meta.with_read_only(self.reads.active_read_mode);
    }
    // Preserve fork PROVENANCE exactly as the ordinary capture does: absorbing a source does not
    // change this group's own origin, and this forced capture OVERWRITES the durable meta that a
    // restart re-derives `fork_id` from and that every later snapshot send advertises. Omitting
    // the stamp sheds the token — a fork sibling that missed the absorb then refuses this
    // lineage's every snapshot as foreign (the receipt fork-provenance gate) and never
    // reconverges. Absent for a non-fork target.
    if let Some(fork_id) = &self.split.fork_id {
      meta = meta.with_fork_id(fork_id.clone());
    }
    let opid = self.mint_op_id();
    // As in the ordinary capture: the meta rides `pending_compact` so the missed-completion
    // fallback compacts only on THIS capture's own durability (identity, lineage included).
    let pc_meta = meta.clone();
    self.submit_snapshot(stable, opid, meta, bytes::Bytes::from(data));
    self.snapshot.pending_compact = Some((opid, pc_meta));
    true
  }

  /// Seal a parked commit's abort window: while the window is OPEN and this leader's log still
  /// ENDS at the parked entry, append one no-op so the `k + 1` coordinate gets decided — a
  /// quiet target would otherwise hold every replica's park open forever (client traffic and
  /// election no-ops seal it incidentally; this is the guaranteed leg). Idempotent per park:
  /// once anything sits above `k` the append is skipped, and a truncated seal is replaced by
  /// the successor leader's own election no-op. Returns whether a no-op was appended (the
  /// caller flushes the fan-out inline so sealing never waits on heartbeat cadence).
  pub(crate) fn ensure_merge_seal<L: LogStore>(&mut self, now: Now, log: &mut L) -> bool {
    if !self.role.is_leader() {
      return false;
    }
    let Some(pending) = self.merge.pending_apply.as_ref() else {
      return false;
    };
    if log.last_index() != pending.at() {
      return false;
    }
    self.append_leader_noop(now, log, pending.at()).is_some()
  }

  /// Advance this SOURCE replica's commit (and apply) to the freeze boundary on LOG IDENTITY —
  /// the parked-target service's leaderless-source leg. Returns whether the local log proved
  /// the identity (idempotently true once past the boundary); on any miss it changes nothing.
  ///
  /// Soundness: the caller holds a COMMITTED `CommitMerge` carrying `(boundary, freeze_term)`,
  /// and its proposer stamped that pair from a source it observed frozen-applied at the
  /// boundary — so the freeze entry `(boundary, freeze_term)` is committed in the source group.
  /// If this replica's log holds an entry with that exact index and term, the Log Matching
  /// property makes it (and its whole prefix) byte-identical to the committed one, so raising
  /// `commit` here is the same knowledge transfer as a leader's commit index riding an
  /// AppendEntries — with the dead source leader's say-so carried by the target's log instead
  /// of a heartbeat that may never come: match alone is not commit KNOWLEDGE, and a source
  /// follower stranded at `commit < boundary` after the absorb consumed the rest of the quorum
  /// (leaderless, under-hosted, unelectable) would otherwise park its host forever. Never on
  /// index alone: a divergent entry at the boundary (wrong term) proves nothing about the
  /// prefix and MUST keep waiting. A LeaseGuard commit-wait needs no re-check here: the freeze
  /// in the advanced range kills lease serving itself, so the early advance can never surface
  /// a lease read.
  pub(crate) fn advance_commit_on_freeze_identity<L>(
    &mut self,
    log: &L,
    boundary: Index,
    freeze_term: Term,
  ) -> bool
  where
    L: LogStore,
    F::Snapshot: crate::Data,
  {
    if self.poison.poisoned || freeze_term == Term::ZERO {
      return false;
    }
    if log.last_index() < boundary {
      return false;
    }
    let Some(t) = self.log_term(log, boundary) else {
      return false;
    };
    if t != freeze_term {
      return false;
    }
    if self.commit < boundary {
      self.commit = boundary;
    }
    if self.applied < self.commit {
      self.apply_committed(log);
    }
    true
  }

  /// THE MERGE-PARK SAFETY PIN: whether a parked apply has left this replica's voter set
  /// SUPERSEDED — a committed `ConfChange` sits above the parked `CommitMerge`, unapplied. The
  /// park freezes apply at `k - 1` while the committed log races ahead; membership is apply-time,
  /// so the tracker this replica would count a quorum (or an election) against is the STALE
  /// pre-park configuration. Winning a campaign on that superseded set — or advancing commit
  /// against it as a leader — truncates entries the real (shrunk/grown) configuration already
  /// committed (a below-floor read). Callers must refuse to campaign / count quorum when this
  /// holds. Returns false when NOT parked (apply tracks commit tracks membership — current by
  /// construction) or when nothing committed-but-unapplied changes the voter set. Fail-closed on
  /// an unreadable range: a config that cannot be proven current is treated as superseded.
  pub(crate) fn merge_park_membership_superseded<L: LogStore>(&self, log: &L) -> bool {
    if self.merge.pending_apply.is_none() {
      return false;
    }
    let end = self.commit.next();
    let mut cursor = self.applied.next();
    while cursor < end {
      let batch = match log.entries(cursor..end, 1 << 20) {
        Ok(EntriesRead::Ready(b)) if !b.is_empty() => b,
        // A cold/short/empty read cannot prove the committed-unapplied range holds no config
        // change → fail closed (refuse to campaign this pass; a warm retry re-evaluates).
        _ => return true,
      };
      for entry in &*batch {
        if entry.kind() == EntryKind::ConfChange {
          return true;
        }
      }
      cursor = match batch.last() {
        Some(e) => e.index().next(),
        None => return true,
      };
    }
    false
  }

  /// Whether every tracked peer (voters AND learners, both joint halves) has MATCHED the log
  /// through `boundary` — meaningful on the LEADER (its tracker holds the peers' proven match).
  /// The merge's all-source-voters freeze barrier (the CRDB `waitForApplication` shape) reads it
  /// off the frozen SOURCE leader at `commit_merge` admission: the absorb is not proposed until
  /// every source voter provably holds the freeze, so the committed `CommitMerge` that dissolves
  /// the source certifies the whole voter set already has it. A voter left below the boundary
  /// would otherwise be orphaned once the source leader is lost — the other hosts floor and
  /// dismantle the source around it, and its co-located target parks with no way to advance or
  /// be snapshotted past its local source.
  pub(crate) fn peers_matched_through(&self, boundary: Index) -> bool {
    let me = self.config.id();
    self
      .tracker
      .progress_map()
      .iter()
      .all(|(peer, pr)| *peer == me || pr.match_index() >= boundary)
  }

  /// Append one merge admin entry (`PrepareMerge`/`CommitMerge`/`RollbackMerge`) on the leader —
  /// the container's merge verbs call this after their merge-specific gates pass. Mirrors
  /// `propose_split_entry`: appended durable-pending under the current term with the standard
  /// lease stamps, fan-out deferred to `flush_appends`, the single-frame bound enforced (the
  /// payload carries one bounded group tag, so the bound is unreachable in practice but kept for
  /// uniformity). A `PrepareMerge` sets the append-observed lease kill HERE — the leader appends
  /// before it replicates, which is what puts `append(freeze)` after every lease read this
  /// leader ever served.
  pub(crate) fn propose_merge_entry<L>(
    &mut self,
    now: impl Into<Now>,
    log: &mut L,
    kind: EntryKind,
    payload: Bytes,
  ) -> Result<Index, crate::ProposeError<I>>
  where
    L: LogStore,
  {
    use crate::ProposeError;
    debug_assert!(matches!(
      kind,
      EntryKind::PrepareMerge
        | EntryKind::CommitMerge
        | EntryKind::RollbackMerge
        | EntryKind::ThawDischarged
    ));
    let now: Now = now.into();
    if self.poison.poisoned {
      return Err(ProposeError::Poisoned);
    }
    if !self.role.is_leader() {
      return Err(ProposeError::NotLeader {
        leader: self.leader.cheap_clone(),
      });
    }
    if self.transfer.lead_transferee.is_some() {
      return Err(ProposeError::LeaderTransferInProgress);
    }
    let Some(index) = Self::next_log_index(log.last_index()) else {
      return Err(ProposeError::LogIndexExhausted);
    };
    let entry = crate::Entry::new(self.term, index, kind, payload)
      .with_timestamp(self.lease_stamp(now.mono()))
      .with_lease_window(self.lease_window_stamp())
      .with_wall_timestamp(self.lease_wall_stamp(now));
    let cost = crate::wire::entry_frame_cost(&entry);
    if cost > crate::wire::APPEND_FRAME_ENTRY_BUDGET {
      return Err(ProposeError::EntryTooLarge {
        size: cost,
        max: crate::wire::APPEND_FRAME_ENTRY_BUDGET,
      });
    }
    let opid = self.mint_op_id();
    self.submit_append(log, opid, core::slice::from_ref(&entry));
    self.push_pending(opid, Pending::LeaderAppend { upto: index });
    self.replication_pending = true;
    if kind == EntryKind::PrepareMerge {
      self.note_freeze_appended(index);
    }
    if kind == EntryKind::CommitMerge {
      self.merge.pending_commit_index = index;
    }
    if kind == EntryKind::RollbackMerge {
      self.merge.pending_rollback_index = index;
    }
    Ok(index)
  }
}
