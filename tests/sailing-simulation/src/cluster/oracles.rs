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
        NodeView {
          id,
          removed: self.removed.contains(&id),
          // The node's own view of whether it is a voter in its committed configuration. Derived
          // from the proto's runtime `conf_state()` (tracks applied ConfChanges), so a learner or a
          // freshly-wired-but-not-yet-applied joiner reports `false` — the quorum-durability oracle
          // uses this as its denominator population so growth/learners don't inflate the quorum.
          is_voter: node.conf_state().is_voter(&id),
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
      nodes,
    }
  }

  /// Run the full per-tick safety-oracle suite against the current cluster state, panicking with
  /// the oracle name + seed + tick on a violation (for exact VOPR replay). Called at the end of
  /// every [`tick`](Self::tick). Exposed so tests can also invoke it at a chosen point.
  pub fn run_oracles(&mut self) {
    let view = self.view();
    self.checker.check_or_panic(&view);
  }

  /// Borrow the [`Violation`](crate::Violation)-or-`Ok` result of running the suite WITHOUT
  /// panicking — for tests that want to assert the suite is green at a point.
  pub fn check_oracles(&mut self) -> Result<(), checker::Violation> {
    let view = self.view();
    self.checker.check(&view)
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
