use super::*;

impl Cluster {
  /// Number of nodes (including removed ones, which are kept in the Vec but isolated).
  pub fn size(&self) -> usize {
    self.nodes.len()
  }

  /// Number of live (non-removed) nodes.
  pub fn live_size(&self) -> usize {
    self.nodes.len() - self.removed.len()
  }

  /// The current virtual time.
  pub fn now(&self) -> Instant {
    self.now
  }

  /// The id of a node that currently believes itself leader, if any.
  pub fn leader(&self) -> Option<u64> {
    self
      .node_ids
      .iter()
      .enumerate()
      .filter(|(_, id)| !self.removed.contains(id))
      .find(|(i, _)| self.nodes[*i].role().is_leader())
      .map(|(_, &id)| id)
  }

  /// The cluster's REAL committed VOTER set — the authoritative committed membership, read from the
  /// proto's runtime `conf_state()` (which tracks every APPLIED `ConfChange`), NOT from any
  /// optimistic propose-time bookkeeping.
  ///
  /// **Source of truth: the current leader's `conf_state().voters()`.** A node's `conf_state()`
  /// reflects only the ConfChanges IT has applied; the committed configuration is the one a quorum
  /// agrees on. The leader has applied every committed entry up to its commit index (it is the most
  /// up-to-date node by construction), so its `conf_state().voters()` IS the committed voter set —
  /// the safe authoritative read whenever a leader exists.
  ///
  /// **No-leader fallback (deterministic):** during an election there is no single authority, so we
  /// return the MOST COMMON `conf_state().voters()` across the live (non-removed) nodes — the
  /// committed config a plurality has applied. Ties break by the smallest voter set under `BTreeSet`'s
  /// total order, so the result is a pure function of the cluster state (no map/iteration-order or
  /// wall-clock nondeterminism). Returns an empty set only for an empty/all-removed cluster.
  pub fn committed_voters(&self) -> BTreeSet<u64> {
    // Source the committed config from the HIGHEST-TERM leader. A stale, partitioned ex-leader at a
    // lower term — or one removed by a committed conf-change it never received, so it never stepped
    // down — can still report `role = Leader` with an OUTDATED voter set. Picking the first such node
    // (as `self.leader()` does) would let the safety oracle re-judge entries committed under the
    // CURRENT config against that stale set, a false positive (e.g. node 1 commits idx 55 under the
    // committed `{0,1,3}` while a stale node still advertises the pre-removal `{0,1,2,3}`). The
    // highest-term leader is the authoritative source of the latest committed membership.
    let authoritative = self
      .node_ids
      .iter()
      .enumerate()
      .filter(|(_, id)| !self.removed.contains(id))
      .filter(|(i, _)| self.nodes[*i].role().is_leader())
      .max_by_key(|(i, _)| self.nodes[*i].term());
    if let Some((i, _)) = authoritative {
      return self.nodes[i]
        .conf_state()
        .voters()
        .iter()
        .copied()
        .collect();
    }
    // No single leader: tally each live node's committed voter set and pick the most common one.
    // `BTreeMap` keyed by the (sorted) voter set keeps the tally deterministic; the fold picks the
    // highest count, breaking ties by the set that is smaller under the map's key ordering.
    let mut tally: BTreeMap<BTreeSet<u64>, usize> = BTreeMap::new();
    for (i, id) in self.node_ids.iter().enumerate() {
      if self.removed.contains(id) {
        continue;
      }
      let voters: BTreeSet<u64> = self.nodes[i]
        .conf_state()
        .voters()
        .iter()
        .copied()
        .collect();
      *tally.entry(voters).or_insert(0) += 1;
    }
    tally
      .into_iter()
      .max_by(|(a_set, a_n), (b_set, b_n)| {
        // Higher count wins; on a tie prefer the set that sorts FIRST (smaller under BTreeSet order)
        // so the choice is deterministic. `max_by` keeps the last maximum, so invert the set
        // comparison to make the first-sorting set the chosen maximum.
        a_n.cmp(b_n).then_with(|| b_set.cmp(a_set))
      })
      .map(|(set, _)| set)
      .unwrap_or_default()
  }

  /// The cluster's REAL committed LEARNER set — the companion to [`committed_voters`](Self::committed_voters),
  /// read from the current leader's runtime `conf_state().learners()` (or, when leaderless, the
  /// learner set of the same plurality committed config `committed_voters` selects, so the two stay
  /// consistent). Used by the VOPR to tell a successfully-committed learner from an orphaned joiner.
  pub fn committed_learners(&self) -> BTreeSet<u64> {
    // Same authoritative-source rule as `committed_voters`: read from the HIGHEST-TERM leader so a
    // stale lower-term ex-leader cannot report an outdated learner set.
    let authoritative = self
      .node_ids
      .iter()
      .enumerate()
      .filter(|(_, id)| !self.removed.contains(id))
      .filter(|(i, _)| self.nodes[*i].role().is_leader())
      .max_by_key(|(i, _)| self.nodes[*i].term());
    if let Some((i, _)) = authoritative {
      return self.nodes[i]
        .conf_state()
        .learners()
        .iter()
        .copied()
        .collect();
    }
    // Leaderless: pick the learner set of the plurality committed config (same selection rule as
    // `committed_voters`, keyed on the voter set so both accessors agree on the chosen config).
    let mut tally: BTreeMap<BTreeSet<u64>, (usize, BTreeSet<u64>)> = BTreeMap::new();
    for (i, id) in self.node_ids.iter().enumerate() {
      if self.removed.contains(id) {
        continue;
      }
      let cs = self.nodes[i].conf_state();
      let voters: BTreeSet<u64> = cs.voters().iter().copied().collect();
      let learners: BTreeSet<u64> = cs.learners().iter().copied().collect();
      let e = tally.entry(voters).or_insert((0, learners));
      e.0 += 1;
    }
    tally
      .into_iter()
      .max_by(|(a_v, (a_n, _)), (b_v, (b_n, _))| a_n.cmp(b_n).then_with(|| b_v.cmp(a_v)))
      .map(|(_, (_, learners))| learners)
      .unwrap_or_default()
  }

  /// How many nodes currently believe themselves leader (among non-removed nodes).
  pub fn leader_count(&self) -> usize {
    self
      .node_ids
      .iter()
      .enumerate()
      .filter(|(_, id)| !self.removed.contains(id))
      .filter(|(i, _)| self.nodes[*i].role().is_leader())
      .count()
  }

  /// The term of node `id`.
  pub fn term_of(&self, id: u64) -> sailing_proto::Term {
    let i = self.node_idx[&id];
    self.nodes[i].term()
  }

  /// The maximum term across all live (non-removed) nodes.
  ///
  /// Used by PreVote tests to assert that an isolated node's campaigns did NOT inflate
  /// the cluster term (with PreVote, the isolated node stays in PreCandidate without bumping
  /// its real term).
  pub fn max_term(&self) -> sailing_proto::Term {
    self
      .node_ids
      .iter()
      .enumerate()
      .filter(|(_, id)| !self.removed.contains(id))
      .map(|(i, _)| self.nodes[i].term())
      .max()
      .unwrap_or(sailing_proto::Term::ZERO)
  }

  /// The id of the current leader — same as `leader()`, a convenience alias so tests
  /// can write `cluster.leader_id()` alongside `term_of`, `max_term`, etc.
  pub fn leader_id(&self) -> Option<u64> {
    self.leader()
  }

  /// The role of node `id`.
  pub fn role_of(&self, id: u64) -> sailing_proto::Role {
    let i = self.node_idx[&id];
    self.nodes[i].role()
  }

  /// All `ReadState`s confirmed for node `id` (ever), in confirmation order.
  ///
  /// Populated by the `tick` inner loop from `Event::ReadState` events drained off the
  /// node's event queue. This list grows monotonically and is never cleared.
  pub fn read_states_of(&self, id: u64) -> &[ReadState] {
    let i = self.node_idx[&id];
    &self.read_states[i]
  }

  /// Every node id the cluster has ever wired, in sorted (deterministic) order. Includes
  /// removed nodes — their histories (e.g. confirmed `ReadState`s) remain observable.
  pub fn node_ids(&self) -> std::vec::Vec<u64> {
    self.node_idx.keys().copied().collect()
  }

  /// The highest commit index any node (including removed ones) currently believes — the
  /// completed-write watermark. An entry committed ANYWHERE is durably replicated to a quorum
  /// and acknowledged, so a linearizable read invoked after this instant must observe it.
  pub fn max_commit(&self) -> sailing_proto::Index {
    self
      .nodes
      .iter()
      .map(|n| n.commit_index())
      .max()
      .unwrap_or(sailing_proto::Index::ZERO)
  }

  /// Node `id`'s commit watermark (the endpoint's belief).
  pub fn commit_index_of(&self, id: u64) -> sailing_proto::Index {
    let i = self.node_idx[&id];
    self.nodes[i].commit_index()
  }

  /// Node `id`'s applied watermark (the endpoint's applied index, NOT the state-machine entry
  /// count — empty/conf entries advance it without a state-machine record). A confirmed read
  /// with `ReadState.index <= applied_index_of(id)` is servable on `id`.
  pub fn applied_index_of(&self, id: u64) -> sailing_proto::Index {
    let i = self.node_idx[&id];
    self.nodes[i].applied_index()
  }

  /// The `first_index()` of node `id`'s durable log (advances after compaction).
  pub fn first_index_of(&self, id: u64) -> sailing_proto::Index {
    let i = self.node_idx[&id];
    self.logs[i].first_index()
  }

  /// Total number of `Event::SnapshotInstalled` events observed for node `id` since
  /// cluster construction (or the last `crash`).
  pub fn snapshot_install_count(&self, id: u64) -> u64 {
    let i = self.node_idx[&id];
    self.snapshot_installs[i]
  }

  /// Total `Event::SnapshotInstalled` events across ALL nodes.
  pub fn total_snapshot_installs(&self) -> u64 {
    self.snapshot_installs.iter().sum()
  }

  /// Total number of `Event::ConfChanged` events observed for node `id` since
  /// cluster construction.
  pub fn conf_changed_count(&self, id: u64) -> u64 {
    let i = self.node_idx[&id];
    self.conf_changed[i]
  }

  /// Total `Event::ConfChanged` events across ALL live (non-removed) nodes.
  pub fn total_conf_changed(&self) -> u64 {
    self
      .node_ids
      .iter()
      .enumerate()
      .filter(|(_, id)| !self.removed.contains(id))
      .map(|(i, _)| self.conf_changed[i])
      .sum()
  }

  /// Length of applied log for node `id`.
  pub fn applied_len_of(&self, id: u64) -> usize {
    let i = self.node_idx[&id];
    self.nodes[i].state_machine().applied().len()
  }

  /// Debug: `(armed, due_now)` for node `id`'s next serviceable timer — `armed=false` means the node
  /// has no timer at all (`poll_timeout()` is None) and so the event-driven clock never wakes it.
  pub fn dbg_timer(&self, id: u64) -> (bool, bool) {
    let i = self.node_idx[&id];
    match self.nodes[i].poll_timeout() {
      Some(d) => (true, d <= self.now),
      None => (false, false),
    }
  }

  /// Node `id`'s applied `(index, command-bytes)` sequence, copied out for cross-run comparison.
  ///
  /// Used by the network-fault determinism test to assert two runs of the same seed produce
  /// byte-identical applied logs. The agreement oracle ([`agreement_holds`](Self::agreement_holds))
  /// compares these prefixes across nodes; this exposes a single node's sequence.
  pub fn applied_entries_of(&self, id: u64) -> AppliedLog {
    let i = self.node_idx[&id];
    self.nodes[i]
      .state_machine()
      .applied()
      .iter()
      .map(|(idx, cmd)| (idx.get(), cmd.to_vec()))
      .collect()
  }

  /// Number of messages DROPPED by the seeded network fault model since construction (a non-vacuity
  /// counter so tests can assert the model actually fired). Excludes partition/isolation drops.
  pub fn net_dropped(&self) -> u64 {
    self.net_dropped
  }

  /// Number of message DUPLICATIONS fired by the seeded network fault model since construction
  /// (counts each fired duplication once, i.e. extra copies pushed). A non-vacuity counter.
  pub fn net_duplicated(&self) -> u64 {
    self.net_duplicated
  }

  /// The VISIBLE `last_index()` of node `id`'s log. In async mode a submitted-but-unflushed append
  /// IS visible here (the proto's submit-then-read contract); use
  /// [`durable_last_index_of`](Self::durable_last_index_of) for the fsync'd window.
  pub fn last_index_of(&self, id: u64) -> sailing_proto::Index {
    let i = self.node_idx[&id];
    self.logs[i].last_index()
  }

  /// Debug: node `id`'s proto-internal membership view — its tracker's incoming/outgoing voters and
  /// learners, plus its visible last log index. Used by VOPR panic diagnostics to spot a config
  /// divergence that stalls commit (e.g. a leader whose tracker still lists a removed node, so the
  /// quorum waits forever on a match that never advances).
  pub fn dbg_membership(&self, id: u64) -> String {
    let i = self.node_idx[&id];
    let cs = self.nodes[i].conf_state();
    std::format!(
      "voters={:?} out={:?} learners={:?} last={}",
      cs.voters(),
      cs.voters_outgoing(),
      cs.learners(),
      self.logs[i].last_index().get(),
    )
  }

  /// Node `id`'s proto-internal VOTER set (both joint halves) from its applied `conf_state`. Used by
  /// the harness to detect a node a current voter still considers a member but the harness has already
  /// isolated (`gone`) — that node must be reinstated, or a divergent-config election / stale-leader
  /// commit deadlocks on it.
  pub fn node_voters(&self, id: u64) -> BTreeSet<u64> {
    let i = self.node_idx[&id];
    let cs = self.nodes[i].conf_state();
    cs.voters()
      .iter()
      .chain(cs.voters_outgoing().iter())
      .copied()
      .collect()
  }

  /// The set of currently network-isolated node ids (VOPR-partitioned `down` ∪ `mark_removed` `gone`).
  /// The calm-window heal uses this to un-isolate EVERY reachable-but-isolated node, not just the ones
  /// the harness still tracks in `st.down` — a node can be `c.isolated` yet absent from `st.down`
  /// (reconcile prunes `st.down` to current voters without un-isolating), which would otherwise leave
  /// it stranded unreachable forever.
  pub fn isolated_nodes(&self) -> BTreeSet<u64> {
    self.isolated.clone()
  }

  /// The DURABLE (fsync'd) `last_index` of node `id`'s log. In async mode this reflects only
  /// flushed (durable) appends — a submitted-but-unflushed append is NOT counted here (it is
  /// visible to [`last_index_of`](Self::last_index_of) but lost on a crash before flush). In sync
  /// mode it equals the visible `last_index`.
  pub fn durable_last_index_of(&self, id: u64) -> sailing_proto::Index {
    let i = self.node_idx[&id];
    self.logs[i].durable_last_index()
  }

  /// Whether node `id` currently has a staged (submitted-but-not-yet-flushed) store write — i.e.
  /// it is sitting inside the fsync-loss window. Always `false` in sync mode. Used by crash-window
  /// tests to assert the crash genuinely lands mid-window (non-vacuity).
  pub fn node_has_inflight(&self, id: u64) -> bool {
    let i = self.node_idx[&id];
    self.logs[i].has_inflight() || self.stables[i].has_inflight()
  }

  /// Whether node `id` is poisoned (a fatal storage/apply error has made it inert). In async mode
  /// a `transient_read` fault that fires on a committed-range read poisons the node via the
  /// proto's poison-on-fatal-read path.
  pub fn is_poisoned(&self, id: u64) -> bool {
    let i = self.node_idx[&id];
    self.nodes[i].is_poisoned()
  }

  /// The [`sailing_proto::PoisonReason`] of node `id`, or `None` if healthy.
  pub fn poison_reason_of(&self, id: u64) -> Option<sailing_proto::PoisonReason> {
    let i = self.node_idx[&id];
    self.nodes[i].poison_reason()
  }

  /// Shortest applied-log length across all non-removed nodes.
  pub fn min_applied_len(&self) -> usize {
    self
      .node_ids
      .iter()
      .enumerate()
      .filter(|(_, id)| !self.removed.contains(id))
      .map(|(i, _)| self.nodes[i].state_machine().applied().len())
      .min()
      .unwrap_or(0)
  }
}
