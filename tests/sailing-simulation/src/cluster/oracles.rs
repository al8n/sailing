use super::*;

impl Cluster {
  /// Capture a read-only [`ClusterView`] snapshot for the per-tick safety oracles (the `checker` module).
  ///
  /// Every field is read through a PUBLIC accessor (the proto's
  /// `commit_index()`/`applied_index()`/`term()`/`role()`/`state_machine()`/`is_poisoned()` and the
  /// sim store's non-faulting durable-read seams [`MemLog::durable_entries`] /
  /// `MemStable::hard_state` / `MemStable::snapshot`), so building the view never perturbs the run
  /// (in particular it never triggers the `transient_read` fault on the committed-range
  /// `LogStore::entries` read). The `seed`/`tick` are threaded through for VOPR replay.
  pub fn view(&self) -> ClusterView {
    let nodes = (0..self.nodes.len())
      .map(|i| {
        let id = self.node_ids[i];
        let node = &self.nodes[i];
        let log = &self.logs[i];
        let stable = &self.stables[i];
        // Read the DURABLE (fsync'd) window — in async mode this is the durable snapshot, which
        // EXCLUDES a submitted-but-unflushed tail (visible to `last_index()` but not yet durable).
        // `durable_first`/`durable_last`/`durable_entries` must all come from the same durable
        // snapshot so the boundedness oracle's window stays internally consistent and the
        // quorum-durability oracle observes only fsync'd state. In sync mode these equal the visible
        // state.
        let durable_first = log.durable_first_index().get();
        let durable_last = log.durable_last_index().get();
        // The VISIBLE last index (includes a submitted-but-unflushed tail in async mode). The proto
        // applies committed entries from this visible view, so the apply sanity bound uses it.
        let visible_last = log.last_index().get();
        let durable_entries: std::vec::Vec<DurableEntry> = log
          .durable_entries()
          .iter()
          .map(|e| DurableEntry {
            index: e.index().get(),
            term: e.term().get(),
            data: e.data().to_vec(),
            is_conf_change: e.kind().is_conf_change(),
          })
          .collect();
        // Read the DURABLE (fsync'd) snapshot, NEVER the submit-visible `stable.snapshot()` slot —
        // crediting a visible-but-unflushed blob is exactly the oracle blindness that hid the
        // snapshot-install orphan window. Pairing this with `durable_first/last` above keeps the
        // quorum-durability/boundedness oracles observing a single, internally-consistent fsync'd view.
        let (snapshot_last_index, snapshot_last_term) = match stable.durable_snapshot() {
          Some(meta) => (meta.last_index().get(), meta.last_term().get()),
          None => (0, 0),
        };
        let applied_log: std::vec::Vec<(u64, std::vec::Vec<u8>)> = node
          .state_machine()
          .applied()
          .iter()
          .map(|(idx, cmd)| (idx.get(), cmd.to_vec()))
          .collect();
        // The node's ACTIVE membership (current/joint halves + learners), read ONCE from the proto's
        // runtime `conf_state()` (which folds every applied ConfChange and adopts an installed snapshot's
        // ConfState verbatim). Cloned into the view so the membership-coherence oracle can compare a
        // snapshot-installed node against a log-built peer at the same applied index.
        let cs = node.conf_state();
        NodeView {
          id,
          removed: self.removed.contains(&id),
          // The node's own view of whether it is a voter in its committed configuration. Derived
          // from the proto's runtime `conf_state()` (tracks applied ConfChanges), so a learner or a
          // freshly-wired-but-not-yet-applied joiner reports `false` — the quorum-durability oracle
          // uses this as its denominator population so growth/learners don't inflate the quorum.
          is_voter: cs.is_voter(&id),
          poisoned: node.is_poisoned(),
          is_leader: node.role().is_leader(),
          term: node.term().get(),
          commit: node.commit_index().get(),
          applied: node.applied_index().get(),
          applied_log,
          durable_first,
          durable_last,
          visible_last,
          durable_entries,
          snapshot_last_index,
          snapshot_last_term,
          // Whether this node's membership is SNAPSHOT-DERIVED (it installed a transferred snapshot),
          // from the STICKY lineage flag — NOT the resettable `snapshot_installs` counter, so a node that
          // installed-then-restarted stays marked (its durable snapshot is its membership source). The
          // membership oracle keys "under test" on this AND excludes such nodes as the log-built witness;
          // a LOCAL compaction (durable snapshot but config still log-built) leaves it `false`.
          installed_snapshot: self.snapshot_membership_lineage[i],
          conf_voters: cs.voters().clone(),
          conf_voters_outgoing: cs.voters_outgoing().clone(),
          conf_learners: cs.learners().clone(),
          conf_learners_next: cs.learners_next().clone(),
          conf_auto_leave: cs.auto_leave(),
          conf_changed: self.conf_changed[i],
          hardstate_commit: stable.hard_state().commit().get(),
          inflight_staged: usize::from(log.has_inflight()) + usize::from(stable.has_inflight()),
          incarnation: self.restarts[i],
        }
      })
      .collect();
    ClusterView {
      seed: self.seed,
      tick: self.tick_count,
      // The authoritative committed VOTER set (leader's `conf_state().voters()`, or the plurality
      // committed config when leaderless) — the quorum-durability oracle's denominator/witness
      // population. Threading the leader's view (not each node's self-`is_voter`) makes the oracle
      // independent of the sim's optimistic membership bookkeeping: a node the sim prematurely
      // marked removed (an accepted-but-never-committed RemoveNode) is still a real committed voter
      // and durable witness here, and a learner is correctly excluded. `None` only for an
      // empty/all-removed cluster, where the oracle falls back to per-node `is_voter`.
      committed_voters: {
        let v = self.committed_voters();
        if v.is_empty() { None } else { Some(v) }
      },
      // This tick's committed conf-changes from log-built nodes (recorded in `drain_node_events`), for the
      // membership oracle's step-function reference. Cloned (small — one entry per conf-change this tick).
      committed_transitions: self.pending_transitions.clone(),
      // This tick's new transfer installs (id, boundary), for the oracle's observed-install set.
      new_installs: self.pending_new_installs.clone(),
      nodes,
    }
  }

  /// Run the full per-tick safety-oracle suite against the current cluster state, panicking with
  /// the oracle name + seed + tick on a violation (for exact VOPR replay). Called at the end of
  /// every [`tick`](Self::tick). Exposed so tests can also invoke it at a chosen point.
  pub fn run_oracles(&mut self) {
    let view = self.view();
    self.checker.check_or_panic(&view);
    // The checker folded this view's committed-config transitions; clear the buffer (see `tick`).
    self.pending_transitions.clear();
    self.pending_new_installs.clear();
  }

  /// Run the membership oracle's run-end final pass: compare every observed install's install-time ConfState
  /// against the FINAL, now-stable committed-config history at its boundary, panicking with the oracle name +
  /// seed on a mismatch (for exact VOPR replay). Must run ONCE after the last tick — only then is the history
  /// final, so no install is judged against a value a later overwrite/ambiguation supersedes. Also populates the
  /// [`membership_oracle_comparisons`](Self::membership_oracle_comparisons) /
  /// [`skipped_unwitnessed_installs`](Self::skipped_unwitnessed_installs) counters the report reads.
  pub fn finalize_membership_or_panic(&mut self, seed: u64) {
    if let Err(v) = checker::finalize_membership(&mut self.checker) {
      panic!(
        "SAFETY ORACLE VIOLATION (run-end final pass): {v}\n  seed={seed}\n  (replay: run_vopr-family \
         entry for this seed and inspect the snapshot install at the reported boundary)",
      );
    }
  }

  /// How many membership-coherence comparisons the run-end final pass (`checker::finalize_membership`)
  /// performed; `0` until [`finalize_membership_or_panic`](Self::finalize_membership_or_panic) runs. A sweep
  /// reads this to assert the membership oracle genuinely COMPARED a snapshot-installed node against the
  /// committed-config history — a green run where it only ever skipped would be vacuous coverage.
  pub fn membership_oracle_comparisons(&self) -> u64 {
    self.checker.membership_comparisons()
  }

  /// How many observed installs the run-end final pass could NOT compare due to an incomplete committed-config
  /// HISTORY (boundary beyond the watermark, an unresolved divergence, or an absent reference); `0` until
  /// [`finalize_membership_or_panic`](Self::finalize_membership_or_panic) runs. A sweep asserts this is `0` — on a
  /// converged run every boundary's reference is complete + non-ambiguous. Distinct from
  /// [`kind_unobservable_installs`](Self::kind_unobservable_installs).
  pub fn skipped_unwitnessed_installs(&self) -> u64 {
    self.checker.skipped_unwitnessed_installs()
  }

  /// How many observed installs the run-end final pass SOUNDLY declined because the resolved conf-change index is
  /// committed-FINAL but its committed-log KIND was compacted before any tick observed it (so a genuine ConfChange
  /// cannot be told from a compacted-away superseder). The net declines rather than risk a stale verdict — a
  /// bounded coverage limitation of compaction, not a soundness hole. `0` until the final pass runs.
  pub fn kind_unobservable_installs(&self) -> u64 {
    self.checker.kind_unobservable_installs()
  }

  /// Borrow the [`Violation`](crate::Violation)-or-`Ok` result of running the suite WITHOUT
  /// panicking — for tests that want to assert the suite is green at a point.
  pub fn check_oracles(&mut self) -> Result<(), checker::Violation> {
    let view = self.view();
    let r = self.checker.check(&view);
    // The checker folded this view's committed-config transitions; clear the buffer (see `tick`).
    self.pending_transitions.clear();
    self.pending_new_installs.clear();
    r
  }

  /// True if every non-removed node's applied `(index, command)` sequence agrees as a
  /// prefix of the longest — the core safety property.
  ///
  /// Removed nodes are excluded: their log stopped advancing at the point of removal, but
  /// the rest of the cluster continued, so their suffix would legitimately diverge.
  pub fn agreement_holds(&self) -> bool {
    let logs: std::vec::Vec<&[(sailing_proto::Index, bytes::Bytes)]> = self
      .node_ids
      .iter()
      .enumerate()
      .filter(|(_, id)| !self.removed.contains(id))
      .map(|(i, _)| self.nodes[i].state_machine().applied())
      .collect();
    let longest = logs.iter().map(|l| l.len()).max().unwrap_or(0);
    for k in 0..longest {
      let mut seen: Option<&(sailing_proto::Index, bytes::Bytes)> = None;
      for l in &logs {
        if let Some(cell) = l.get(k) {
          match seen {
            None => seen = Some(cell),
            Some(s) => {
              if s != cell {
                return false;
              }
            }
          }
        }
      }
    }
    true
  }
}
