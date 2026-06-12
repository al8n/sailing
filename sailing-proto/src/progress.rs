//! Per-peer replication progress (Raft §replication). Probe/Replicate drive normal
//! replication (one-step back-off on reject) with an `Inflights` window + term-skip reject
//! hints; `Snapshot` covers a peer that is receiving an InstallSnapshot.
use crate::{Index, Inflights};

/// How the leader is currently replicating to a peer.
///
/// `Snapshot(pending_snapshot)` carries the last log index covered by the snapshot being
/// sent; the peer stays paused until it acks at or past that index, then transitions back
/// to `Probe`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, derive_more::Display, derive_more::IsVariant)]
#[display("{}", self.as_str())]
pub enum ProgressState {
  /// Last replicated index unknown; send conservatively and narrow on reject.
  Probe,
  /// Steady-state: optimistically advance `next_index` as acks arrive.
  Replicate,
  /// An `InstallSnapshot` is in flight; `pending_snapshot` is its `last_index`.
  Snapshot(Index),
}

impl ProgressState {
  /// Stable snake_case name.
  pub const fn as_str(&self) -> &'static str {
    match self {
      Self::Probe => "probe",
      Self::Replicate => "replicate",
      Self::Snapshot(_) => "snapshot",
    }
  }
}

/// Replication bookkeeping for one peer.
#[derive(Debug, Clone)]
pub struct Progress {
  match_index: Index,
  next_index: Index,
  state: ProgressState,
  inflight: Inflights,
  msg_app_flow_paused: bool,
  /// Set when the leader hears from this peer in the current election window; reset each
  /// CheckQuorum tick. Used by [`crate::Tracker::quorum_active`] to determine whether a
  /// quorum of voters has been recently reachable.
  recent_active: bool,
}

impl Progress {
  /// A fresh peer: nothing acked, send from `next_index`, probing.
  #[inline(always)]
  pub fn new(next_index: Index, max_inflight_msgs: usize, max_inflight_bytes: u64) -> Self {
    Self {
      match_index: Index::ZERO,
      next_index,
      state: ProgressState::Probe,
      inflight: Inflights::new(max_inflight_msgs, max_inflight_bytes),
      msg_app_flow_paused: false,
      recent_active: false,
    }
  }

  /// Highest index known replicated on this peer.
  #[inline(always)]
  pub const fn match_index(&self) -> Index {
    self.match_index
  }

  /// Next index to send to this peer.
  #[inline(always)]
  pub const fn next_index(&self) -> Index {
    self.next_index
  }

  /// Current replication state.
  #[inline(always)]
  pub const fn state(&self) -> ProgressState {
    self.state
  }

  /// Whether the leader should pause sending to this peer.
  ///
  /// - `Probe`: paused after the first send until the peer acks or a heartbeat response
  ///   clears `msg_app_flow_paused`.
  /// - `Replicate`: paused only when the in-flight window is full.
  /// - `Snapshot`: always paused (waiting for the snapshot ack).
  #[inline(always)]
  pub fn is_paused(&self) -> bool {
    match self.state {
      ProgressState::Probe => self.msg_app_flow_paused,
      ProgressState::Replicate => self.inflight.full(),
      ProgressState::Snapshot(_) => true,
    }
  }

  /// Record that entries up to `last` (carrying `bytes`) were sent to this peer.
  pub fn sent_entries(&mut self, last: Index, bytes: u64) {
    match self.state {
      ProgressState::Probe => self.msg_app_flow_paused = true,
      ProgressState::Replicate => {
        self.inflight.add(last, bytes);
        // Optimistically advance next_index past what we just sent.
        if last >= self.next_index {
          self.next_index = last.next();
        }
      }
      ProgressState::Snapshot(_) => {
        // In Snapshot state no entries are sent; this is a no-op.
      }
    }
  }

  /// Enter snapshot-delivery state. The peer stays paused until it acks at or past
  /// `pending_snapshot`, then `maybe_update` transitions it back to `Probe`.
  pub fn become_snapshot(&mut self, pending_snapshot: Index) {
    self.state = ProgressState::Snapshot(pending_snapshot);
    self.inflight.reset();
    self.msg_app_flow_paused = false;
  }

  /// Enter steady-state replication.
  pub fn become_replicate(&mut self) {
    self.state = ProgressState::Replicate;
    self.next_index = self.match_index.next();
    self.msg_app_flow_paused = false;
    self.inflight.reset();
  }

  /// Revert to probing (on a reject or a step-down).
  pub fn become_probe(&mut self) {
    self.state = ProgressState::Probe;
    self.inflight.reset();
    self.msg_app_flow_paused = false;
  }

  /// On a successful ack of index `n`: advance match/next if it moved forward.
  /// Also frees inflights up to `n` and clears the probe pause.
  ///
  /// If in `Snapshot` state and `n >= pending_snapshot`, transitions to `Probe`.
  pub fn maybe_update(&mut self, n: Index) -> bool {
    let updated = n > self.match_index;
    if updated {
      self.match_index = n;
    }
    if self.next_index <= n {
      self.next_index = n.next();
    }
    self.inflight.free_le(n);
    self.msg_app_flow_paused = false;
    // If we were waiting for a snapshot ack and the peer is now caught up, resume.
    if let ProgressState::Snapshot(pending) = self.state
      && n >= pending
    {
      self.become_probe();
    }
    updated
  }

  /// Clear the Probe pause flag without changing state — used by HeartbeatResp so a
  /// stalled Probe peer resumes on the next heartbeat round.
  pub fn clear_probe_pause(&mut self) {
    self.msg_app_flow_paused = false;
  }

  /// Whether this peer has been heard from in the current election window.
  #[inline(always)]
  pub const fn recent_active(&self) -> bool {
    self.recent_active
  }

  /// Set or clear the `recent_active` flag (called on inbound messages from this peer while
  /// we are the leader, and cleared by `Tracker::reset_recent_active` each CheckQuorum tick).
  #[inline(always)]
  pub fn set_recent_active(&mut self, v: bool) -> &mut Self {
    self.recent_active = v;
    self
  }

  /// etcd `FreeFirstOne`: if this peer is in `Replicate` state with a full in-flight window
  /// (because all acks were lost, e.g. during a partition), free the oldest in-flight slot so
  /// the leader can send one new `AppendEntries` per heartbeat round.
  ///
  /// This is called on every `HeartbeatResp` so a healed/partitioned follower resumes
  /// replication on its own via heartbeats instead of waiting for an unrelated client
  /// proposal to trigger a send.
  ///
  /// No-op for `Probe` and `Snapshot` states (they have their own resume mechanisms) and
  /// no-op when the window is not full (normal steady-state; the leader can already send).
  pub fn free_inflight_on_heartbeat(&mut self) {
    if self.state == ProgressState::Replicate && self.inflight.full() {
      self.inflight.free_first_one();
    }
  }

  /// On a reject: back `next_index` off by one (floored at 1) and re-probe.
  #[allow(
    dead_code,
    reason = "exercised by unit tests; kept for the back-off path"
  )]
  pub fn decrement(&mut self) {
    self.next_index = Index::new(self.next_index.get().saturating_sub(1).max(1));
    self.state = ProgressState::Probe;
    self.inflight.reset();
    self.msg_app_flow_paused = false;
  }

  /// Directly set `next_index` (used by the term-skip reject handler after `become_probe`).
  pub fn set_next_index(&mut self, n: Index) -> &mut Self {
    self.next_index = n;
    self
  }
}

#[cfg(test)]
mod tests;
