use super::*;

impl<I, F> Endpoint<I, F>
where
  I: NodeId,
  F: StateMachine,
{
  /// The current committed-configuration membership ([`ConfState`](crate::ConfState)) derived from
  /// the runtime `Tracker`.
  ///
  /// This reflects the LIVE configuration (it tracks every applied `ConfChange`), not just the static
  /// bootstrap seed from `Config.voters`, so snapshots and restarts carry the correct membership.
  /// Exposed (read-only) so a verification harness can derive the true VOTER set — the correct quorum
  /// denominator for a durable-quorum oracle under reconfiguration (a learner / not-yet-applied
  /// joiner is not a voter and must not inflate the quorum). A pure read of internal state.
  pub fn conf_state(&self) -> crate::ConfState<I> {
    self.tracker.conf_state()
  }
}
impl<I, F> Endpoint<I, F>
where
  I: NodeId,
  F: StateMachine,
  F::Command: crate::Data,
  F::Error: core::error::Error,
{
  /// Append a `ConfChangeV2` entry to the log and replicate it to all peers.
  ///
  /// Internal helper shared by `propose_conf_change_v2` and the auto-leave path.
  /// Mirrors `propose`'s deferred-append + `LeaderAppend` + replicate pattern exactly.
  ///
  /// Returns `None` if the log index space is exhausted (`last_index == u64::MAX`) — no entry was
  /// appended (the caller must surface `LogIndexExhausted` or fail-stop). `Some(index)` otherwise.
  ///
  /// Requires `I: crate::Data` because the ConfChangeV2 encodes node ids.
  pub(crate) fn append_conf_change<L, S>(
    &mut self,
    now: Instant,
    log: &mut L,
    stable: &S,
    cc: crate::ConfChangeV2<I>,
  ) -> Option<Index>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    // Allocate a fresh, usable index (see `next_log_index`): refuse at the ceiling rather than
    // alias-and-truncate or allocate the unreadable sentinel `u64::MAX`.
    let index = Self::next_log_index(log.last_index())?;
    use crate::Data as _;
    let mut buf = std::vec::Vec::new();
    cc.encode(&mut buf);
    let entry = crate::Entry::new(
      self.term,
      index,
      crate::EntryKind::ConfChange,
      bytes::Bytes::from(buf),
    );
    let opid = self.mint_op_id();
    self.submit_append(log, opid, core::slice::from_ref(&entry));
    self
      .pending
      .insert(opid, Pending::LeaderAppend { upto: index });
    self.pending_conf_index = index;
    // Apply-time membership (etcd, spec §9): the leader does NOT fold the conf-change into its tracker
    // here. The configuration changes only when the entry is committed-and-applied (apply_committed) —
    // so `conf_state()`/`quorum_committed()` always reflect the COMMITTED voter set, never an
    // uncommitted log tail. At append the leader records only `pending_conf_index` (one in flight).
    for peer in self.peers().collect::<std::vec::Vec<_>>() {
      self.maybe_send_append(now, peer, log, stable);
    }
    Some(index)
  }

  /// Propose a v1 (single-op) configuration change on the leader.
  ///
  /// Normalises the v1 input to a [`crate::ConfChangeV2`] via [`crate::ConfChange::into_v2`] and delegates
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
    now: Instant,
    log: &mut L,
    stable: &S,
    cc: crate::ConfChangeV2<I>,
  ) -> Result<Index, crate::ProposeError<I>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    if self.poisoned {
      return Err(crate::ProposeError::Poisoned);
    }
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
    // Pre-validate against the CURRENT tracker using the SAME Changer dispatch `apply_committed`
    // uses (apply-time membership, spec §9). An invalid change (e.g. `leave_joint` while not in a
    // joint config) must be a REJECTED proposal here, not an `Ok` that replicates and then poisons
    // the node when `apply_committed`'s Changer rejects the committed entry. We DISCARD the
    // resulting tracker — membership still only changes at apply time; this is validation only.
    {
      let changer = crate::tracker::confchange::Changer::new(
        log.last_index(),
        self.config.max_inflight_msgs(),
        self.config.max_inflight_bytes(),
      );
      let result =
        if cc.changes().is_empty() && cc.transition() == crate::ConfChangeTransition::Auto {
          changer.leave_joint(&self.tracker)
        } else if cc.transition() != crate::ConfChangeTransition::Auto || cc.changes().len() > 1 {
          let auto_leave = cc.transition() != crate::ConfChangeTransition::Explicit;
          changer.enter_joint(&self.tracker, auto_leave, cc.changes())
        } else {
          changer.simple(&self.tracker, cc.changes())
        };
      if result.is_err() {
        return Err(crate::ProposeError::InvalidConfChange);
      }
    }
    match self.append_conf_change(now, log, stable, cc) {
      Some(index) => Ok(index),
      None => Err(crate::ProposeError::LogIndexExhausted),
    }
  }
}
