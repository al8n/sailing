use super::*;

impl Cluster {
  /// Tick until `predicate(self)` holds or `max_steps` elapse; returns whether it held.
  pub fn run_until(&mut self, max_steps: usize, mut predicate: impl FnMut(&Self) -> bool) -> bool {
    for _ in 0..max_steps {
      if predicate(self) {
        return true;
      }
      self.tick();
    }
    predicate(self)
  }

  /// Initiate a linearizable read on the current leader with the given context bytes.
  ///
  /// Calls `Endpoint::read_index` on the leader.  Returns `true` if there is a leader
  /// (the call was made); `false` if no leader is available.
  ///
  /// The leader accepts the read (`read_index` returns `Ok`) for any fresh context; this
  /// helper asserts that, so a reused/duplicate context surfaces as a panic rather than a
  /// silently dropped read.  The confirmed `ReadState` will appear in `read_states_of(leader)`
  /// once a heartbeat-quorum round completes (for `ReadOnlySafe`) or immediately (for
  /// `ReadOnlyLeaseBased`).
  pub fn read_index(&mut self, context: &[u8]) -> bool {
    let leader = match self.leader() {
      Some(l) => l,
      None => return false,
    };
    let i = self.node_idx[&leader];
    let log = &self.logs[i];
    let stable = &self.stables[i];
    self.nodes[i]
      .read_index(
        self.now,
        log,
        stable,
        bytes::Bytes::copy_from_slice(context),
      )
      .expect("leader must accept the read_index for a fresh context");
    true
  }

  /// Initiate a linearizable read on a SPECIFIC node (the VOPR's non-panicking entry): a
  /// follower target exercises the forward path, the leader target the direct path. Returns
  /// whether the node ACCEPTED the read (a `ReadState` with this context may eventually
  /// surface in `read_states_of(node)`). A refusal — no known leader, forwarding capacity,
  /// poison — is a legitimate no-op under faults. A `DuplicateContext` refusal panics: the
  /// caller mints unique contexts, so a duplicate is a harness bug, not weather.
  pub fn read_index_on(&mut self, node: u64, context: &[u8]) -> bool {
    let i = self.node_idx[&node];
    let log = &self.logs[i];
    let stable = &self.stables[i];
    match self.nodes[i].read_index(
      self.now,
      log,
      stable,
      bytes::Bytes::copy_from_slice(context),
    ) {
      Ok(()) => true,
      Err(sailing_proto::ReadIndexError::DuplicateContext) => {
        panic!(
          "read_index_on: duplicate context {context:?} — the caller must mint unique contexts"
        )
      }
      Err(_) => false,
    }
  }

  /// Initiate a leader transfer: ask the current leader to transfer to `to`.
  ///
  /// Returns `Ok(())` if the leader accepted the transfer, or an error if there is no
  /// leader / the transfer was refused (e.g. `to` is not a voter).
  pub fn transfer_leader(&mut self, to: u64) -> Result<(), sailing_proto::TransferError<u64>> {
    let leader = self
      .leader()
      .ok_or(sailing_proto::TransferError::NotLeader { leader: None })?;
    let i = self.node_idx[&leader];
    let log = &mut self.logs[i];
    let stable = &mut self.stables[i];
    self.nodes[i].transfer_leader(self.now, log, stable, to)
  }

  /// Propose `data` on the current leader; returns the assigned index (or `None` if no leader).
  pub fn propose(&mut self, data: &[u8]) -> Option<sailing_proto::Index> {
    let leader = self.leader()?;
    let i = self.node_idx[&leader];
    // Split into disjoint borrows: nodes[i], logs[i], stables[i] are each in a
    // separate Vec, so borrowing them simultaneously is safe.
    let log = &mut self.logs[i];
    let stable = &mut self.stables[i];
    self.nodes[i]
      .propose(self.now, log, stable, &bytes::Bytes::copy_from_slice(data))
      .ok()
  }

  /// Propose a v1 conf-change on the current leader; returns the assigned index (or `None`).
  pub fn propose_conf_change(&mut self, cc: ConfChange<u64>) -> Option<sailing_proto::Index> {
    let leader = self.leader()?;
    let i = self.node_idx[&leader];
    let log = &mut self.logs[i];
    let stable = &mut self.stables[i];
    self.nodes[i]
      .propose_conf_change(self.now, log, stable, cc)
      .ok()
  }

  /// Propose a v2 conf-change on the current leader; returns the assigned index (or `None`).
  pub fn propose_conf_change_v2(&mut self, cc: ConfChangeV2<u64>) -> Option<sailing_proto::Index> {
    let leader = self.leader()?;
    let i = self.node_idx[&leader];
    let log = &mut self.logs[i];
    let stable = &mut self.stables[i];
    self.nodes[i]
      .propose_conf_change_v2(self.now, log, stable, cc)
      .ok()
  }

  /// Add a new **voter** node with `id` mid-run.
  ///
  /// **Bootstrap rule:** the new node's `Endpoint` is constructed with `Config.voters` =
  /// the current live voter set (NOT including `id`). This makes `is_voter(id) = false` in
  /// the new node's own Tracker, so it cannot campaign and cannot disrupt the existing
  /// leader. The new node learns its own membership (voter) by applying the replicated
  /// `ConfChange(AddNode(id))` entry once the leader commits it.
  ///
  /// After wiring the new node into all parallel structures, this method proposes
  /// `AddNode(id)` on the current leader. The leader commits it under the OLD quorum,
  /// updates its Tracker, and replicates the full log (including the ConfChange entry) to
  /// the new node, which applies it and gains voter status in its own view.
  ///
  /// Panics if no leader is available.
  pub fn add_node(&mut self, id: u64) {
    self.wire_new_node(id, false);
    let cc = ConfChange::new(
      sailing_proto::ConfChangeType::AddNode,
      id,
      bytes::Bytes::new(),
    );
    self
      .propose_conf_change(cc)
      .expect("add_node: a leader must be available to propose AddNode");
  }

  /// Add a new **learner** node with `id` mid-run.
  ///
  /// Same bootstrap rule as [`Self::add_node`]: the new node starts as a non-voter observer.
  /// After wiring it into the sim structures, proposes `AddLearnerNode(id)` on the leader.
  ///
  /// Panics if no leader is available.
  pub fn add_learner(&mut self, id: u64) {
    self.wire_new_node(id, false);
    let cc = ConfChange::new(
      sailing_proto::ConfChangeType::AddLearnerNode,
      id,
      bytes::Bytes::new(),
    );
    self
      .propose_conf_change(cc)
      .expect("add_learner: a leader must be available to propose AddLearnerNode");
  }

  /// Remove the node `id` from the cluster.
  ///
  /// Proposes `RemoveNode(id)` on the current leader. The change commits and is applied
  /// by the majority under the current quorum; the node being removed receives the commit
  /// and applies its own removal (gaining the step-down: role → Follower, election timer
  /// disarmed). Once applied, the node is no longer a voter in any tracker.
  ///
  /// **Agreement oracle handling:** the removed node is tracked in `self.removed` so that the
  /// `agreement_holds` and `min_applied_len` oracles skip it — its applied log stopped
  /// advancing after removal and the rest of the cluster legitimately advanced further.
  /// The removed node is also `isolated` so it does not participate in future elections.
  ///
  /// Returns the proposal index. Panics if no leader is available.
  pub fn remove_node(&mut self, id: u64) {
    let cc = ConfChange::new(
      sailing_proto::ConfChangeType::RemoveNode,
      id,
      bytes::Bytes::new(),
    );
    self
      .propose_conf_change(cc)
      .expect("remove_node: a leader must be available to propose RemoveNode");
    // Mark the node as removed so the agreement oracle and min_applied_len skip it.
    // Also isolate it so it does not send spurious RequestVotes after being removed but
    // before applying the conf change in its own view (the step-down fires when the
    // ConfChange is applied; until then, the node is technically still a voter in its own
    // view and its election timer is still armed). Isolation is a simulation convenience
    // — a real cluster would rely on the step-down to stop the removed node from campaigning.
    self.removed.insert(id);
    self.isolated.insert(id);
  }
}
