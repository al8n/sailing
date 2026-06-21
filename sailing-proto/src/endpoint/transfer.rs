use super::*;

impl<I, F> Endpoint<I, F>
where
  I: NodeId,
  F: StateMachine,
  F::Command: crate::Data,
  F::Error: core::error::Error,
{
  // ─── Leader transfer ──────────────────────────────────────────────────────────

  /// Initiate a graceful leader transfer to `to`.
  ///
  /// The leader stops accepting proposals, catches `to` up to its log, then sends it a
  /// `TimeoutNow` so it campaigns immediately (bypassing PreVote and the lease).  The
  /// cluster experiences at most one election timeout of unavailability.
  ///
  /// Returns `Ok(())` on success (transfer initiated or already targeting `to`).
  /// Returns `Err(TransferError::NotLeader)` if this node is not the current leader.
  /// Returns `Err(TransferError::NotAVoter)` if `to` is not a voter.
  /// Returns `Err(TransferError::AlreadyLeader)` if `to == self.id()`.
  pub fn transfer_leader<L, S>(
    &mut self,
    now: impl Into<Now>,
    log: &L,
    stable: &S,
    to: I,
  ) -> Result<(), crate::TransferError<I>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let now: crate::Now = now.into();
    if self.poison.poisoned {
      return Err(crate::TransferError::Poisoned);
    }
    if !self.role.is_leader() {
      return Err(crate::TransferError::NotLeader {
        leader: self.leader,
      });
    }
    if to == self.config.id() {
      return Err(crate::TransferError::AlreadyLeader);
    }
    if !self.tracker.is_voter(&to) {
      return Err(crate::TransferError::NotAVoter);
    }
    // Already targeting this node — idempotent, just return Ok.
    if self.lead_transferee == Some(to) {
      return Ok(());
    }
    // Arm the transfer: stop accepting proposals, start the deadline window.
    self.lead_transferee = Some(to);
    self.transfer_deadline = Some(now.mono() + self.config.election_timeout());
    // revoke the LeaseBased read authority for the duration of the transfer. Authorizing a transfer
    // lets the transferee become leader (forced campaign, bypassing the post-restart fence), so this leader must
    // relinquish its lease — otherwise it could keep serving stale LeaseBased reads at its old commit
    // while the transferee commits ahead. `do_leader_read` also gates the lease on `lead_transferee`, so
    // a heartbeat that re-renews `lease_valid_until` during the transfer still cannot be used; this clear
    // is the immediate revocation.
    self.check_quorum_lease.lease_valid_until = None;
    self.check_quorum_lease.lease_acks.clear();

    // If the target is already caught up, send TimeoutNow immediately.
    let target_match = self
      .tracker
      .progress(&to)
      .map(|p| p.match_index())
      .unwrap_or(crate::Index::ZERO);
    if target_match == log.last_index() {
      let (term, me) = (self.term, self.config.id());
      self.send(to, Message::TimeoutNow(crate::TimeoutNow::new(term, me)));
      // a forced campaign is now authorized for this term — disable LeaseBased reads for the rest
      // of it (the forced campaign can elect a new leader at any later point, even after this transfer
      // aborts on the deadline).
      self.forced_handoff_this_term = true;
    } else {
      // Target is lagging: kick replication so it catches up.
      // TimeoutNow will be sent from on_append_resp once match_index == last_index.
      self.maybe_send_append(now, to, log, stable);
    }
    Ok(())
  }

  /// Receive a `TimeoutNow` from the current leader (transfer target path).
  ///
  /// The target campaigns immediately as a REAL candidate (bypassing PreVote and the lease),
  /// with `leader_transfer: true` on its `RequestVote` broadcast.  If this node is not a
  /// voter it ignores the message (etcd: removed/learner nodes silently drop TimeoutNow).
  pub(crate) fn on_timeout_now<L, S>(
    &mut self,
    now: crate::Now,
    log: &mut L,
    stable: &mut S,
    tn: crate::TimeoutNow<I>,
  ) where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    // Authenticate the transfer order: only this node's CURRENT known leader may force a campaign.
    // A forced campaign is deliberately disruptive — it skips PreVote and sets leader_transfer so
    // granters bypass their CheckQuorum/PreVote lease — so a `TimeoutNow` from any other (authentic
    // but non-leader) peer must NOT trigger it, or that peer could provoke a leadership change that
    // the lease was specifically protecting against. `tn.leader()` is the sender, trustworthy by the
    // `handle_message` sender-authenticity choke-point (`msg.from() == from`).
    if self.leader != Some(tn.leader()) {
      return;
    }
    // A non-voter cannot be elected; ignore.
    if !self.tracker.is_voter(&self.config.id()) {
      return;
    }
    // Campaign immediately as a REAL candidate (transfer=true):
    // - Does NOT do a PreVote phase even if config.pre_vote() is on.
    // - Sets leader_transfer=true on every RequestVote so granters bypass their lease.
    self.become_candidate(now, log, stable, true);
  }
}
