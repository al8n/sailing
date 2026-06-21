use super::*;
use crate::ProposeError;

impl<I, F, R> Endpoint<I, F, R>
where
  I: NodeId,
  F: StateMachine,
  R: rand::Rng,
  F::Command: crate::Data,
  F::Error: core::error::Error,
{
  /// Propose a cluster-wide read-mode migration on the leader. The new mode takes effect APPLY-TIME on
  /// every node (like a `ConfChange`) once this entry commits — see `apply_committed`. The cross-leader
  /// commit-wait safety is unaffected: the monotone fold floors are mode-INDEPENDENT and the
  /// `become_leader` arming never tears them down, so no new barrier is needed (spec §1).
  ///
  /// Returns the assigned log index on success, or an error if:
  /// - this node is not the leader (`NotLeader`);
  /// - the node is poisoned (`Poisoned`);
  /// - a leader transfer is in progress (`LeaderTransferInProgress`);
  /// - a prior migration is still in flight — one at a time (`ReadModeChangeInFlight`);
  /// - this leader lacks the target mode's knobs — Δ + `clock_drift_bound` for LeaseGuard, `check_quorum`
  ///   for LeaseBased (`InvalidReadMode`);
  /// - the log index space is exhausted (`LogIndexExhausted`).
  pub fn propose_read_mode_change<L, S>(
    &mut self,
    now: impl Into<Now>,
    log: &mut L,
    stable: &S,
    mode: crate::ReadOnlyOption,
  ) -> Result<Index, ProposeError<I>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let now: crate::Now = now.into();
    if self.poison.poisoned {
      return Err(ProposeError::Poisoned);
    }
    if !self.role.is_leader() {
      return Err(ProposeError::NotLeader {
        leader: self.leader,
      });
    }
    // A leader transfer is in progress: no new proposals until it completes or times out.
    if self.transfer.lead_transferee.is_some() {
      return Err(ProposeError::LeaderTransferInProgress);
    }
    // One migration in flight at a time: refuse if a SetReadMode entry is not yet applied (mirror
    // `pending_conf_index`). Two stacked flips would otherwise race their apply-time effects.
    if self.reads.pending_read_mode_index > self.applied {
      return Err(ProposeError::ReadModeChangeInFlight);
    }
    // Reject-at-propose if THIS leader lacks the target mode's required knobs (into-LeaseGuard ⇒ a valid
    // commit-wait window; into-LeaseBased ⇒ check_quorum). A straggler that lacks them safely
    // Safe-degrades after the flip applies, so only the PROPOSER must be checked (spec §7).
    if !self.config.read_mode_change_valid(mode) {
      return Err(ProposeError::InvalidReadMode);
    }
    // Allocate a fresh, usable index (see `next_log_index`): refuse at the ceiling rather than alias.
    let Some(index) = Self::next_log_index(log.last_index()) else {
      return Err(ProposeError::LogIndexExhausted);
    };
    // The migration entry is stamped under the CURRENT active mode, so a Safe→LeaseGuard entry is ts=0
    // (the into-LeaseGuard warm-up: it is not a usable anchor until a fresh stamped entry commits). It
    // carries ONLY the target mode discriminant — knobs are pre-provisioned per node (spec §7).
    let entry = crate::Entry::new(
      self.term,
      index,
      crate::EntryKind::SetReadMode,
      bytes::Bytes::copy_from_slice(&[mode.as_u8()]),
    )
    .with_timestamp(self.lease_stamp(now.mono()))
    .with_lease_window(self.lease_window_stamp())
    .with_wall_timestamp(self.lease_wall_stamp(now));
    let opid = self.mint_op_id();
    self.submit_append(log, opid, core::slice::from_ref(&entry));
    self
      .pending
      .insert(opid, Pending::LeaderAppend { upto: index });
    // Apply-time migration (mirror apply-time membership): the mode changes only when the entry is
    // committed-and-applied (`apply_committed`). At append the leader records only the one-in-flight
    // guard; `active_read_mode` does not move yet.
    self.reads.pending_read_mode_index = index;
    for peer in self.peers().collect::<std::vec::Vec<_>>() {
      self.maybe_send_append(now, peer, log, stable);
    }
    Ok(index)
  }
}
