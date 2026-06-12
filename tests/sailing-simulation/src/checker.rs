//! The per-tick safety-oracle suite — the heart of the VOPR's verification power.
//!
//! An oracle that only checks applied-log agreement is "structurally blind" to durability bugs:
//! it can miss a commit-persistence bug — a node that advances commit without the entry on a
//! quorum of durable logs — entirely. This module fixes that: it consolidates the existing oracles
//! and adds the durability/commit class into a suite of
//! **pure functions** that run on EVERY tick.
//!
//! # Design
//!
//! Each oracle is a pure function of a read-only [`ClusterView`] (a snapshot of the cluster's
//! observable state, copied out by [`Cluster::view`](crate::Cluster) at the end of every tick) and
//! returns `Result<(), Violation>`. The oracles that need cross-time history (commit/term
//! monotonicity, the committed-history high-water, no-committed-rewrite) read and update the
//! [`Checker`]'s small per-node state. A [`Violation`] carries the oracle name + a human-readable
//! detail; the driver [`panic`]s with the violation, the cluster SEED, and the current TICK so the
//! VOPR can replay the exact failing run.
//!
//! # The oracle suite (what each catches)
//!
//! | Oracle | Catches |
//! |---|---|
//! | [`agreement`] | divergent applied logs across nodes (State Machine Safety, prefix form) |
//! | [`append_before_ack`] | a node acking an entry it has not durably stored |
//! | [`commit_is_quorum_durable`] | a node advancing commit without the entry on a quorum of durable logs (the heartbeat class) |
//! | [`monotonic_commit`] | a node's commit going backward across ticks (incl. across restart) |
//! | [`no_committed_rewrite`] | a previously-committed `(index→command)` being overwritten (the strongest State Machine Safety check) |
//! | [`term_monotonic`] | a node's term going backward across ticks |
//! | [`durable_prefix`] | a restarted node silently forgetting the committed prefix it durably stored (the headline durability bug) |
//! | [`boundedness`] | per-node bookkeeping growing unboundedly under compaction (a GC/compaction failure) |
//!
//! The suite is a **pure observer**: it never draws from a PRNG and never mutates the simulated
//! nodes/stores, so the run is byte-identical with or without it (determinism preserved).

use std::{
  collections::{BTreeMap, BTreeSet},
  string::String,
  vec::Vec,
};

/// A safety-oracle violation: which oracle tripped and a human-readable detail.
///
/// `oracle` is a stable `&'static str` name (matches the function name) so callers can match on
/// it; `detail` carries the offending node ids / indices / commands for diagnosis. The driver
/// formats this together with the cluster seed + tick into the panic message for VOPR replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
  /// The stable name of the oracle that tripped (e.g. `"agreement"`, `"durable_prefix"`).
  pub oracle: &'static str,
  /// A human-readable description of the violation (offending nodes/indices/commands).
  pub detail: String,
}

impl Violation {
  /// Construct a violation for `oracle` with `detail`.
  fn new(oracle: &'static str, detail: impl Into<String>) -> Self {
    Self {
      oracle,
      detail: detail.into(),
    }
  }
}

impl core::fmt::Display for Violation {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "[{}] {}", self.oracle, self.detail)
  }
}

/// One durable log entry as observed by the checker: `(index, term, command-bytes)`.
///
/// Read from [`MemLog::durable_entries`](crate::MemLog::durable_entries) — the non-faulting seam,
/// so observing it never perturbs the run. Only durable (flushed) entries appear; staged
/// (un-flushed) appends are invisible, exactly as the proto's read view sees them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableEntry {
  /// The entry's 1-based log index.
  pub index: u64,
  /// The entry's term.
  pub term: u64,
  /// The entry's payload bytes.
  pub data: Vec<u8>,
}

/// A read-only snapshot of one node's observable state at a tick boundary.
///
/// Every field is copied out via a PUBLIC accessor (minimized surface): the proto's
/// `commit_index()`/`applied_index()`/`term()`/`role()`/`state_machine()`/`is_poisoned()`, and the
/// sim store's non-faulting durable-read seams. Nothing here reaches into proto internals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeView {
  /// The node id.
  pub id: u64,
  /// Whether the node has been removed from the cluster (its applied log legitimately stops
  /// advancing at removal, so the cross-node oracles skip it).
  pub removed: bool,
  /// Whether the node considers ITSELF a voter in its own committed configuration
  /// (`Endpoint::conf_state().is_voter(id)`). A learner or a freshly-wired joiner is `false` here.
  ///
  /// This is the per-node self-view. The quorum-durability oracle's PRIMARY denominator is the
  /// cluster-wide [authoritative committed voter set](ClusterView::committed_voters) (the leader's
  /// `conf_state().voters()`); this self-report is the FALLBACK population used only when no
  /// authoritative set is available (an empty/all-removed cluster, or a direct-constructed synthetic
  /// view in the oracle teeth tests). Counting voters — not all live nodes — is what keeps the oracle
  /// sound under reconfiguration: only voters ack toward commit, and a node becomes a voter only by
  /// applying the `AddNode` that adds it (which requires a durable log up to that conf-change's index,
  /// hence covering every earlier committed entry).
  pub is_voter: bool,
  /// Whether the node is poisoned (a fatal storage/apply error made it inert). A poisoned node's
  /// watermarks are frozen; the monotonicity oracles still treat it as a normal observation (a
  /// frozen value never regresses), but it is excluded from liveness-flavored checks.
  pub poisoned: bool,
  /// Whether the node currently believes itself leader.
  pub is_leader: bool,
  /// The node's current term (`Endpoint::term()`).
  pub term: u64,
  /// The node's in-memory commit watermark (`Endpoint::commit_index()`).
  pub commit: u64,
  /// The node's in-memory applied watermark (`Endpoint::applied_index()`).
  pub applied: u64,
  /// The node's applied `(index, command)` sequence (`Endpoint::state_machine().applied()`).
  pub applied_log: Vec<(u64, Vec<u8>)>,
  /// The node's DURABLE (fsync'd) log `first_index` (advances after compaction). In async mode this
  /// is the durable snapshot's first index; a submitted-but-unflushed tail is excluded.
  pub durable_first: u64,
  /// The node's DURABLE (fsync'd) log `last_index`. In async mode this is the durable snapshot's
  /// last index — a submitted-but-unflushed append (visible to the proto's reads) is NOT counted
  /// here, since a crash before flush would lose it.
  pub durable_last: u64,
  /// The node's VISIBLE log `last_index` (`Endpoint`-readable state). In async mode this INCLUDES a
  /// submitted-but-unflushed tail (≥ [`durable_last`](Self::durable_last)); in sync mode the two
  /// coincide. The proto applies committed entries from this visible view (a node can only apply
  /// what it can read), so the `applied <= visible_last` sanity bound uses this, while durability
  /// oracles use `durable_last`.
  pub visible_last: u64,
  /// The node's durable entries (`durable_first..=durable_last`), read via the non-faulting seam.
  pub durable_entries: Vec<DurableEntry>,
  /// The `last_index` of the node's durable snapshot (or `0` if none). Entries `<=` this are
  /// covered by the snapshot even though they are compacted out of `durable_entries`.
  pub snapshot_last_index: u64,
  /// The boundary term of the node's durable snapshot (or `0` if none).
  pub snapshot_last_term: u64,
  /// The node's durable `HardState.commit` — the durably-persisted commit watermark. The
  /// durability invariant relates the recovered in-memory `commit` to this.
  pub hardstate_commit: u64,
  /// The number of staged (un-flushed) store writes (fsync window). Bounded-bookkeeping check.
  pub inflight_staged: usize,
  /// The node's restart count ("incarnation") — bumped each time the node crashes and recovers from
  /// durable storage. A change signals a restart boundary at which the commit/term monotonicity
  /// baseline is legitimately reset: the batched commit/term persist can lose an in-memory advance
  /// still in the fsync window on crash, and the restarted node re-derives it.
  pub incarnation: u64,
}

impl NodeView {
  /// Whether this node's durable state (in-memory entries OR snapshot) covers `index` — i.e. the
  /// node has durably stored the committed entry at `index`. Used by the quorum-durability and
  /// committed-rewrite oracles to account for compaction.
  fn durable_covers(&self, index: u64) -> bool {
    index <= self.snapshot_last_index || (index >= self.durable_first && index <= self.durable_last)
  }

  /// The durable term at `index`, accounting for compaction: a snapshotted entry counts as
  /// durable-present at the snapshot boundary term. Returns `None` if the node does not durably
  /// cover `index`.
  fn durable_term(&self, index: u64) -> Option<u64> {
    if index >= self.durable_first && index <= self.durable_last {
      return self
        .durable_entries
        .iter()
        .find(|e| e.index == index)
        .map(|e| e.term);
    }
    // Compacted-away but covered by the snapshot: the only term we can attest is the snapshot
    // boundary's. A committed entry strictly below the boundary was committed before the snapshot,
    // so we treat the boundary term as its durable witness (the committing node's own term lookup
    // does the same via `term(offset)`).
    if index <= self.snapshot_last_index {
      return Some(self.snapshot_last_term);
    }
    None
  }
}

/// A read-only snapshot of the whole cluster at a tick boundary, plus the seed/tick for replay.
#[derive(Debug, Clone)]
pub struct ClusterView {
  /// The cluster seed (for VOPR replay).
  pub seed: u64,
  /// The current tick/step number (for VOPR replay).
  pub tick: u64,
  /// The cluster's REAL committed VOTER set — the authoritative quorum denominator for
  /// `commit_is_quorum_durable`, read from the leader's runtime `conf_state().voters()` (or the
  /// plurality committed config when leaderless). Threading the leader's view (rather than each
  /// node's own `is_voter` self-report combined with the sim's `removed` flag) makes the oracle's
  /// denominator the proto's true committed membership: a node the sim prematurely marked removed —
  /// an accepted-but-never-committed RemoveNode — is still a real committed voter and durable
  /// witness, and a learner is correctly excluded.
  ///
  /// `None` only when the cluster could not derive a committed set (empty / all-removed); the oracle
  /// then falls back to the per-node `is_voter & !removed` population. Direct-constructed synthetic
  /// views (the oracle teeth tests) leave this `None` and rely on that fallback.
  pub committed_voters: Option<BTreeSet<u64>>,
  /// One [`NodeView`] per node, in node-position order.
  pub nodes: Vec<NodeView>,
}

impl ClusterView {
  /// Iterate the non-removed nodes (the cross-node oracles — agreement, no-committed-rewrite — operate
  /// on these: a learner must also agree on the committed prefix it has applied).
  fn live(&self) -> impl Iterator<Item = &NodeView> {
    self.nodes.iter().filter(|n| !n.removed)
  }

  /// Iterate the committed VOTERS — the quorum-durability denominator/witness population.
  ///
  /// When [`committed_voters`](Self::committed_voters) is `Some` (the production path: a leader
  /// exists or a plurality committed config was derived), membership is taken from that authoritative
  /// committed voter set — a node is a voter iff its id is in the set, regardless of its own
  /// `is_voter` self-report or the sim's `removed` flag. This is what makes the oracle independent of
  /// the harness's optimistic membership bookkeeping (a prematurely-`removed` real voter is still
  /// counted; a learner is excluded because it is not in the committed voter set).
  ///
  /// When `committed_voters` is `None` (an empty/all-removed cluster, or a direct-constructed
  /// synthetic view), it falls back to the per-node `!removed && is_voter` self-report.
  fn voters(&self) -> impl Iterator<Item = &NodeView> {
    self
      .nodes
      .iter()
      .filter(move |n| match &self.committed_voters {
        Some(set) => set.contains(&n.id),
        None => !n.removed && n.is_voter,
      })
  }

  /// The number of committed voters — the denominator for the durable-quorum threshold.
  ///
  /// When the authoritative [`committed_voters`](Self::committed_voters) set is known, the count is
  /// its cardinality (the TRUE voter population), not the number of matching `NodeView`s — so a
  /// momentarily-absent voter view can never shrink the quorum threshold and weaken the oracle's
  /// teeth. Otherwise it falls back to counting the per-node voter self-reports.
  fn voter_count(&self) -> usize {
    match &self.committed_voters {
      Some(set) => set.len(),
      None => self.voters().count(),
    }
  }
}

/// The per-tick safety-oracle suite, holding the cross-tick history the monotonicity / committed-
/// history oracles need.
///
/// The driver builds one [`Checker`] per cluster and calls [`check`](Checker::check) at the end of
/// every tick with the freshly-captured [`ClusterView`]; on a violation it panics with seed+tick.
/// The checker is a pure observer of the simulated system (it mutates only its OWN history), so the
/// run is deterministic with or without it.
#[derive(Debug, Default)]
pub struct Checker {
  /// Per-node highest commit watermark ever observed (for [`monotonic_commit`]).
  max_commit_seen: BTreeMap<u64, u64>,
  /// Per-node highest term ever observed (for [`term_monotonic`]).
  max_term_seen: BTreeMap<u64, u64>,
  /// The cluster-wide committed-history high-water: `index -> command`. An entry is recorded the
  /// first time ANY node reports it as applied (applied entries are, by definition, committed).
  /// Used by [`no_committed_rewrite`] (a later conflicting command at a recorded index is a
  /// violation) and to attest the committed prefix for the durability checks.
  committed_hw: BTreeMap<u64, Vec<u8>>,
  /// The highest committed index that was reached under a configuration STRICTLY OLDER than the
  /// current one — raised to the committed high-water each time the authoritative voter set changes.
  /// [`commit_is_quorum_durable`] judges a quorum only for commit indices ABOVE this floor: an entry
  /// committed under a prior config had its quorum defined by that config, so the current voter set
  /// need not durably hold it (a removed voter carried a copy; a freshly-added voter joined later).
  /// Those older entries' safety stays covered by [`agreement`], [`no_committed_rewrite`], and
  /// [`durable_prefix`].
  commit_floor: u64,
  /// The authoritative committed voter set observed on the previous tick — a change signals a
  /// reconfiguration and raises [`commit_floor`](Self::commit_floor).
  last_committed_voters: Option<BTreeSet<u64>>,
  /// Per-node last-observed incarnation ([`NodeView::incarnation`]). When a node's incarnation
  /// changes (it crashed and recovered), its commit/term monotonicity baseline is reset, so a
  /// legitimate watermark drop across the restart boundary is not flagged as a backward step.
  last_incarnation: BTreeMap<u64, u64>,
}

impl Checker {
  /// A fresh checker with empty history.
  pub fn new() -> Self {
    Self::default()
  }

  /// Run the ENTIRE oracle suite against `view`, updating cross-tick history.
  ///
  /// Returns `Err(violation)` on the FIRST oracle that trips (the history-updating oracles update
  /// their state before returning, so a non-fatal observation is still recorded). On `Ok(())` the
  /// cluster satisfied every safety property at this tick.
  ///
  /// Ordering note: the history oracles (`no_committed_rewrite`, `monotonic_commit`,
  /// `term_monotonic`) check the NEW observation against stored history and THEN fold it in, so a
  /// regression is caught at the tick it first appears.
  pub fn check(&mut self, view: &ClusterView) -> Result<(), Violation> {
    // A reconfiguration (the authoritative voter set changed) raises the commit floor to the current
    // committed high-water, so entries committed under the prior config are not re-judged against the
    // new voter set. Only a genuine change of an already-known set counts — the first observation
    // (None -> Some, the initial config) does not raise the floor.
    if let (Some(old), Some(new)) = (&self.last_committed_voters, &view.committed_voters)
      && old != new
    {
      // Raise the floor to the high-water commit among the OLD voter set — the configuration under
      // which everything up to here was committed — INCLUDING any voter this change REMOVES. Using
      // only the NEW voters misses entries a removed voter legitimately committed (under the old
      // quorum) that the survivors have not yet caught up to; those entries would then be wrongly
      // re-judged against the smaller new voter set and flagged as non-quorum-durable. They are
      // safe — already validated against their own config before this change, and still covered by
      // agreement / no_committed_rewrite / durable_prefix. (e.g. leader 0 commits index
      // 375 under {0,2,3}; 0 is then removed → {2,3} with voter 2 still behind, so 375 must stay
      // exempt.) A removed voter is still present in `view.nodes` (isolated, not dropped).
      let hw = view
        .nodes
        .iter()
        .filter(|n| old.contains(&n.id))
        .map(|n| n.commit)
        .max()
        .unwrap_or(0);
      self.commit_floor = self.commit_floor.max(hw);
    }
    if view.committed_voters.is_some() {
      self.last_committed_voters = view.committed_voters.clone();
    }

    // A node that crashed and recovered (its incarnation changed) re-derives its commit/term
    // watermark from durable state — the batched commit/term persist can drop an in-memory advance
    // still in the fsync window on crash. Reset that node's monotonicity baseline so the legitimate
    // drop across the restart boundary is not flagged as a backward step within one incarnation.
    for n in view.nodes.iter() {
      let last = self.last_incarnation.get(&n.id).copied().unwrap_or(0);
      if n.incarnation != last {
        self.max_commit_seen.remove(&n.id);
        self.max_term_seen.remove(&n.id);
        self.last_incarnation.insert(n.id, n.incarnation);
      }
    }

    // Stateless cross-node oracles first.
    agreement(view)?;
    append_before_ack(view)?;
    commit_is_quorum_durable(view, self.commit_floor)?;
    durable_prefix(view)?;
    boundedness(view)?;
    // History oracles (read-then-fold).
    no_committed_rewrite(self, view)?;
    monotonic_commit(self, view)?;
    term_monotonic(self, view)?;
    Ok(())
  }

  /// Run the suite and PANIC on a violation, embedding the seed + tick for exact VOPR replay.
  ///
  /// This is what [`Cluster::tick`](crate::Cluster) calls at the end of every tick.
  pub fn check_or_panic(&mut self, view: &ClusterView) {
    if let Err(v) = self.check(view) {
      panic!(
        "SAFETY ORACLE VIOLATION: {v}\n  seed={} tick={}\n  (replay: run the same scenario / \
         run_vopr(seed, ticks) and inspect tick {})",
        view.seed, view.tick, view.tick,
      );
    }
  }
}

// ─── Cross-node oracles (stateless) ──────────────────────────────────────────────────────────────

/// **agreement** (applied-prefix): for any two non-removed nodes, the shorter applied log is a
/// prefix of the longer — they agree on `(index→command)` at every shared position.
///
/// This is the core State Machine Safety property in prefix form. Removed nodes are skipped (their
/// applied log stopped advancing at removal while the cluster continued).
pub fn agreement(view: &ClusterView) -> Result<(), Violation> {
  let logs: Vec<&[(u64, Vec<u8>)]> = view.live().map(|n| n.applied_log.as_slice()).collect();
  let ids: Vec<u64> = view.live().map(|n| n.id).collect();
  let longest = logs.iter().map(|l| l.len()).max().unwrap_or(0);
  for k in 0..longest {
    // The first node that has position k pins the expected (index, command); any other node that
    // also has position k must match it.
    let mut pinned: Option<(usize, &(u64, Vec<u8>))> = None;
    for (li, l) in logs.iter().enumerate() {
      if let Some(cell) = l.get(k) {
        match pinned {
          None => pinned = Some((li, cell)),
          Some((pi, p)) => {
            if p != cell {
              return Err(Violation::new(
                "agreement",
                std::format!(
                  "applied[{k}] diverges: node {} has (idx={}, cmd={:?}) but node {} has (idx={}, cmd={:?})",
                  ids[pi],
                  p.0,
                  p.1,
                  ids[li],
                  cell.0,
                  cell.1,
                ),
              ));
            }
          }
        }
      }
    }
  }
  Ok(())
}

/// **append-before-ack** (per-tick form): no node has applied beyond its VISIBLE (readable) log —
/// a node can only apply an entry it can read.
///
/// **The real durability invariant is the send-time tripwire** in
/// [`Cluster::schedule_send`](crate::Cluster): a follower sends a non-reject `AppendResp{match}`
/// only after its append is DURABLE (the proto defers the ack to `on_log_appended`, which fires on
/// the flush completion), so `durable_last >= match` holds when the ack is sent. That is where
/// "never ack an entry you have not durably stored" is enforced.
///
/// This per-tick form is a weaker companion sanity check: `applied <= max(visible_last,
/// snapshot_last_index)`. It deliberately uses the VISIBLE last index, NOT the durable one, because
/// the proto legitimately applies committed entries from its visible log BEFORE its own fsync —
/// commit advance/apply proceed independently of the local ack since a committed entry is durable
/// on a QUORUM elsewhere and the local state machine is rebuilt from the durable log on restart
/// (see `Endpoint::on_append_entries`). Bounding `applied` by `durable_last` would therefore
/// false-fire on a leader (or any node) that has applied a committed-but-not-yet-locally-flushed
/// tail. Per-entry quorum durability of every committed index is enforced separately by
/// [`commit_is_quorum_durable`]. (A snapshot-install follower has its applied watermark at the
/// snapshot boundary with the entries compacted out of the log, so the snapshot boundary counts.)
pub fn append_before_ack(view: &ClusterView) -> Result<(), Violation> {
  for n in view.nodes.iter() {
    let visible_high = n.visible_last.max(n.snapshot_last_index);
    if n.applied > visible_high {
      return Err(Violation::new(
        "append_before_ack",
        std::format!(
          "node {} applied={} exceeds its visible log high-water {} (visible_last={}, \
           snapshot_last_index={}) — applied beyond readable storage",
          n.id,
          n.applied,
          visible_high,
          n.visible_last,
          n.snapshot_last_index,
        ),
      ));
    }
  }
  Ok(())
}

/// **commit-is-quorum-durable**: for each committed VOTER, the entry at its `commit` index must be
/// present with the SAME term on a quorum of the VOTERS' DURABLE logs.
///
/// Catches a node that advanced commit without the entry being durably replicated to a quorum (the
/// heartbeat class) — one tick BEFORE [`agreement`] would catch the resulting divergence.
/// Compaction is accounted for: a snapshotted entry counts as durable-present at the snapshot
/// boundary term (see [`NodeView::durable_covers`] / [`NodeView::durable_term`]).
///
/// The committing voter's own durable term at its commit index is the witness term `t`; the oracle
/// then counts how many VOTERS durably hold `(commit, t)`. Fewer than a majority of the EFFECTIVE
/// voter count (the authoritative [`committed_voters`](ClusterView::committed_voters) cardinality minus
/// voters provably on a stale lower-term branch) is a violation. A `commit` of 0 is vacuously fine.
///
/// **Voter-set denominator (reconfiguration soundness):** the quorum is taken over the
/// [authoritative committed voter set](ClusterView::committed_voters) (the leader's
/// `conf_state().voters()`), not all live nodes and not each node's own `is_voter` self-report. Only
/// voters ack toward commit, and a node becomes a voter only by applying the `AddNode` that adds it
/// (which requires its durable log to cover that conf-change's index, hence every earlier committed
/// entry). So a learner or a wired-but-not-yet-voting joiner never inflates the denominator against
/// an entry it could not have witnessed — the false positive an all-live-nodes denominator produced
/// (a 5→6 voter growth). And because the population is the LEADER's committed config,
/// a real committed voter the harness had prematurely marked removed (an accepted-but-never-committed
/// RemoveNode) is still counted as a durable witness, while a behind voter does not crowd it out —
/// the false positive a per-node `is_voter & !removed` denominator produced. A
/// learner's own `commit` watermark is not checked here (it makes no quorum claim; the same entry is
/// checked via the voters), but a learner that holds the entry still does no harm.
pub fn commit_is_quorum_durable(view: &ClusterView, commit_floor: u64) -> Result<(), Violation> {
  for n in view.voters() {
    let c = n.commit;
    if c == 0 {
      continue; // nothing committed
    }
    // The committing voter must itself durably cover its commit index (this is the append-before-
    // ack ordering; a violation here is also a violation, reported precisely).
    let witness_term = match n.durable_term(c) {
      Some(t) => t,
      None => {
        return Err(Violation::new(
          "commit_is_quorum_durable",
          std::format!(
            "node {} has commit={} but does not durably cover it (durable_first={}, \
             durable_last={}, snapshot_last_index={})",
            n.id,
            c,
            n.durable_first,
            n.durable_last,
            n.snapshot_last_index,
          ),
        ));
      }
    };
    // An entry at or below the reconfiguration floor was committed under a configuration older than
    // the current voter set; its quorum was defined by that config, so the current voters need not
    // all hold it. Its safety is covered by agreement / no_committed_rewrite / durable_prefix.
    if c <= commit_floor {
      continue;
    }
    // Quorum DENOMINATOR. Start from the AUTHORITATIVE committed-voter count (`committed_voters.len()`
    // when known — a momentarily-absent voter view must not shrink it and weaken the oracle),
    // then SUBTRACT voters provably on a stale STRICTLY-LOWER-term branch at `c`. Such a voter
    // durably holds a different, older-term entry, so it never acked `(c, witness_term)` — a same-index
    // entry cannot revert to an older term — and it was not in the quorum that committed this entry (the
    // higher-term log will overwrite it). Excluding ONLY lower-term divergence keeps full teeth: a
    // merely-LAGGING voter (no entry at `c`) and a HIGHER-term divergent voter both remain, so a solo /
    // under-replicated commit AND a commit on a LOSING branch still trip. (e.g. a term-3 entry
    // committed under a smaller config while two voters sit on a stale term-2 branch.)
    // A second exclusion handles the deep-churn boundary: a voter whose DURABLE log does not even reach
    // the reconfiguration floor (`durable_last < commit_floor`) is a freshly-added member still catching
    // up to the prior committed config — it could not have witnessed ANY entry above the floor, so it
    // was not in the quorum that committed `c` (which is just above the floor). Counting it demands a
    // phantom ack it could never have given. This is distinct from a merely-LAGGING real voter (which
    // HAS reached the floor and is only missing the latest entries) — that one stays counted, so a true
    // under-replication still trips. (e.g. idx 2056 committed under a small config that
    // then grew to 5 voters, three of which sit far below the floor at 223/490/2034 vs floor 2055.)
    let excluded = view
      .voters()
      .filter(|m| {
        m.durable_term(c).map(|t| t < witness_term).unwrap_or(false)
          || m.durable_last < commit_floor
      })
      .count();
    let effective = view.voter_count().saturating_sub(excluded);
    let quorum = effective / 2 + 1;
    let copies = view
      .voters()
      .filter(|m| m.durable_covers(c) && m.durable_term(c) == Some(witness_term))
      .count();
    if copies < quorum {
      let per_voter: std::vec::Vec<_> = view
        .voters()
        .map(|m| {
          std::format!(
            "n{}(durable_last={} term@{}={:?} covers={})",
            m.id,
            m.durable_last,
            c,
            m.durable_term(c),
            m.durable_covers(c)
          )
        })
        .collect();
      return Err(Violation::new(
        "commit_is_quorum_durable",
        std::format!(
          "node {} committed index {} (term {}) but only {} of {} voter durable logs hold it with \
           that term (quorum needs {})\n  commit_floor={} committed_voters={:?}\n  voters: {}",
          n.id,
          c,
          witness_term,
          copies,
          effective,
          quorum,
          commit_floor,
          view.committed_voters,
          per_voter.join(" "),
        ),
      ));
    }
  }
  Ok(())
}

/// **durable-prefix-after-restart** (the headline crash-recovery oracle), expressed as a per-tick
/// invariant: a node's in-memory `commit` must be `>=` the committed prefix it durably persisted —
/// concretely, `commit >= min(durable HardState.commit, durable last_index)`.
///
/// # Why this catches the commit-persistence bug
///
/// The durable `HardState.commit` is precisely the committed-prefix length the node had
/// **acknowledged and persisted** before any crash. The bug was that `restart` rebuilt an
/// empty / snapshot-only state machine — recovering `commit = 0` — *despite* a durable
/// `HardState.commit > 0` and a durable log covering it. That trips this oracle the instant the
/// restarted node is observed: `commit (=0) < min(hs.commit, durable_last) (= hs.commit > 0)`.
///
/// # Why it never false-positives
///
/// In correct operation a node persists `commit` only AFTER advancing its in-memory `commit`
/// (the `handle_storage` choke-point writes `self.commit`), so the durable `HardState.commit` is
/// always `<=` the in-memory `commit`. After a restart, the recovery formula sets
/// `commit = min(hs.commit, last_index).max(applied)`, whose lower bound is exactly the
/// `min(hs.commit, last_index)` this oracle requires. The `min` with `durable_last` covers the
/// exotic-but-safe case where a crash lost an in-flight LOG write while the commit-watermark write
/// survived (the entries are re-synced from the leader per the `LogStore::restore` contract); the
/// oracle does not demand the lost entries be present, only the prefix the durable log still covers.
pub fn durable_prefix(view: &ClusterView) -> Result<(), Violation> {
  for n in view.nodes.iter() {
    let durable_committed_prefix = n.hardstate_commit.min(n.durable_last);
    if n.commit < durable_committed_prefix {
      return Err(Violation::new(
        "durable_prefix",
        std::format!(
          "node {} recovered/holds commit={} but its durable committed prefix is {} \
           (HardState.commit={}, durable_last={}) — a restart must not silently forget the \
           committed state it durably stored",
          n.id,
          n.commit,
          durable_committed_prefix,
          n.hardstate_commit,
          n.durable_last,
        ),
      ));
    }
  }
  Ok(())
}

/// **boundedness**: per-node bookkeeping stays bounded under compaction.
///
/// A soft structural check that catches a compaction/GC failure: a node's
/// in-memory durable log must not grow unboundedly while snapshots are taken. The window of
/// in-memory entries is `durable_last - durable_first + 1`; under healthy compaction this stays
/// near `commit - first_index`. We assert the in-memory entry COUNT matches the index window (an
/// off-by-one or stale-offset GC bug would desynchronize them) and that staged writes do not
/// accumulate past a small bound (the fsync window holds at most a handful of in-flight writes; an
/// unbounded `staged` means `flush`/`discard` stopped draining).
///
/// The slack bound on staged writes is generous (`1024`) — this is a tripwire for *unbounded*
/// growth (a leak), not a tight resource assertion.
pub fn boundedness(view: &ClusterView) -> Result<(), Violation> {
  const STAGED_SLACK: usize = 1024;
  for n in view.nodes.iter() {
    // The durable entry count must equal the index window [first..=last]. `durable_last == 0`
    // with `durable_first == 1` is the empty-log base case (count 0).
    let window = if n.durable_last >= n.durable_first {
      (n.durable_last - n.durable_first + 1) as usize
    } else {
      0
    };
    if n.durable_entries.len() != window {
      return Err(Violation::new(
        "boundedness",
        std::format!(
          "node {} durable entry count {} disagrees with its index window {} \
           (durable_first={}, durable_last={}) — a compaction/offset GC bug",
          n.id,
          n.durable_entries.len(),
          window,
          n.durable_first,
          n.durable_last,
        ),
      ));
    }
    if n.inflight_staged > STAGED_SLACK {
      return Err(Violation::new(
        "boundedness",
        std::format!(
          "node {} has {} staged (un-flushed) writes, exceeding the {} bound — the fsync window \
           is not draining (a flush/discard leak)",
          n.id,
          n.inflight_staged,
          STAGED_SLACK,
        ),
      ));
    }
  }
  Ok(())
}

// ─── History oracles (read-then-fold the Checker state) ──────────────────────────────────────────

/// **no-committed-rewrite**: once index `i` is committed clusterwide with command `c`, no node ever
/// applies a different command at `i` — the strongest State Machine Safety check.
///
/// The cluster-wide committed high-water is tracked in [`Checker::committed_hw`]: every applied
/// entry is, by definition, committed, so the first command seen applied at an index is recorded;
/// any later DIFFERENT command applied at that index is a violation. Re-applying the SAME command
/// (e.g. a follower replaying its durable log after restart) is fine.
pub fn no_committed_rewrite(checker: &mut Checker, view: &ClusterView) -> Result<(), Violation> {
  // First pass: detect a conflict against the recorded high-water.
  for n in view.nodes.iter() {
    // A removed node's frozen applied log already agreed with the high-water while it was live;
    // skip it so a post-removal cluster that legitimately advanced does not re-compare its stale
    // tail. (Its recorded contributions remain in the high-water.)
    if n.removed {
      continue;
    }
    for (idx, cmd) in n.applied_log.iter() {
      if let Some(prev) = checker.committed_hw.get(idx)
        && prev != cmd
      {
        return Err(Violation::new(
          "no_committed_rewrite",
          std::format!(
            "committed index {idx} was applied as {prev:?} but node {} now applies {cmd:?} — a \
               committed entry was overwritten (State Machine Safety violation)",
            n.id,
          ),
        ));
      }
    }
  }
  // Second pass: fold every applied entry into the high-water (no conflict was found).
  for n in view.nodes.iter() {
    for (idx, cmd) in n.applied_log.iter() {
      checker
        .committed_hw
        .entry(*idx)
        .or_insert_with(|| cmd.clone());
    }
  }
  Ok(())
}

/// **monotonic-commit-per-node**: a (healthy) node's `commit` index never decreases across ticks —
/// within an incarnation AND across a restart.
///
/// The durable commit watermark is persisted, so a restart recovers it and commit
/// must NOT regress even across restart. At a tick boundary a HEALTHY node's in-memory commit is
/// durably persisted (the `handle_storage` choke-point ran to quiescence), so the next incarnation
/// recovers `>=` the last observed value; a regression below a previously-observed HEALTHY commit IS
/// a durability bug.
///
/// **Poisoned exception:** the proto gates commit-persistence on `!poisoned`, so a node that
/// advanced commit in-memory and THEN poisoned (e.g. a committed-range read fault during apply,
/// after advancing commit but before persisting it) holds an in-memory commit that was never made
/// durable. Commit-persistence protects only the DURABLE commit, so that un-persisted advance is
/// legitimately lost on restart and must NOT be used as the regression baseline (see the recording
/// pass).
pub fn monotonic_commit(checker: &mut Checker, view: &ClusterView) -> Result<(), Violation> {
  for n in view.nodes.iter() {
    let prev = checker.max_commit_seen.get(&n.id).copied().unwrap_or(0);
    if n.commit < prev {
      return Err(Violation::new(
        "monotonic_commit",
        std::format!(
          "node {} commit regressed from {} to {} across ticks — the durable commit watermark is \
           persisted, so commit must never go backward (even across restart)",
          n.id,
          prev,
          n.commit,
        ),
      ));
    }
  }
  for n in view.nodes.iter() {
    // Do NOT record a poisoned node's in-memory commit as the monotonic baseline. The proto's
    // commit-persistence choke-point is gated on `!poisoned`, so a node that advanced commit
    // in-memory and THEN poisoned (e.g. on a committed-range read fault during apply, before the
    // persist step) carries an in-memory commit that was never durably persisted. Commit-persistence
    // only guarantees the DURABLE commit is recovered on restart, so that un-persisted advance is
    // legitimately lost when the node restarts — recording it here would make the next (healthy,
    // correctly-recovered) incarnation look like a regression. The regression CHECK above still
    // runs for every node (a poisoned node's commit is frozen and can only sit at/above the
    // healthy baseline, never below it), so this only suppresses the false positive, never a real
    // durability regression on a healthy node.
    if n.poisoned {
      continue;
    }
    let e = checker.max_commit_seen.entry(n.id).or_insert(0);
    *e = (*e).max(n.commit);
  }
  Ok(())
}

/// **term-monotonic-per-node**: a node's `term` never decreases across ticks.
///
/// The current term is persisted in `HardState` before the node acts on it, so a restart recovers
/// the durable term and a term must never regress (within an incarnation or across a restart). A
/// regression would mean a node forgot a term it had already entered — which could let it re-grant
/// a vote or re-accept a stale leader.
pub fn term_monotonic(checker: &mut Checker, view: &ClusterView) -> Result<(), Violation> {
  for n in view.nodes.iter() {
    let prev = checker.max_term_seen.get(&n.id).copied().unwrap_or(0);
    if n.term < prev {
      return Err(Violation::new(
        "term_monotonic",
        std::format!(
          "node {} term regressed from {} to {} across ticks — the term is persisted before use, \
           so it must never go backward (even across restart)",
          n.id,
          prev,
          n.term,
        ),
      ));
    }
  }
  for n in view.nodes.iter() {
    let e = checker.max_term_seen.entry(n.id).or_insert(0);
    *e = (*e).max(n.term);
  }
  Ok(())
}

#[cfg(test)]
mod tests;
