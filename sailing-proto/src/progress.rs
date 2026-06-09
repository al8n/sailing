//! Per-peer replication progress (Raft §replication). M2 uses Probe/Replicate with naive
//! one-step back-off on reject; M4 adds the `Inflights` window + term-skip reject hints.
use crate::{Index, Inflights};

/// How the leader is currently replicating to a peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, derive_more::Display, derive_more::IsVariant)]
#[display("{}", self.as_str())]
pub enum ProgressState {
  /// Last replicated index unknown; send conservatively and narrow on reject.
  Probe,
  /// Steady-state: optimistically advance `next_index` as acks arrive.
  Replicate,
}

impl ProgressState {
  /// Stable snake_case name.
  pub const fn as_str(&self) -> &'static str {
    match self {
      Self::Probe => "probe",
      Self::Replicate => "replicate",
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
  #[inline(always)]
  pub fn is_paused(&self) -> bool {
    match self.state {
      ProgressState::Probe => self.msg_app_flow_paused,
      ProgressState::Replicate => self.inflight.full(),
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
    }
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
    updated
  }

  /// Clear the Probe pause flag without changing state — used by HeartbeatResp so a
  /// stalled Probe peer resumes on the next heartbeat round (M4 Task 6).
  pub fn clear_probe_pause(&mut self) {
    self.msg_app_flow_paused = false;
  }

  /// On a reject: back `next_index` off by one (floored at 1) and re-probe.
  pub fn decrement(&mut self) {
    self.next_index = Index::new(self.next_index.get().saturating_sub(1).max(1));
    self.state = ProgressState::Probe;
    self.inflight.reset();
    self.msg_app_flow_paused = false;
  }

  /// Directly set `next_index` (used by the term-skip reject handler after `become_probe`).
  pub fn set_next_index(&mut self, n: Index) {
    self.next_index = n;
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::Index;

  #[test]
  fn progress_update_and_decrement() {
    let mut p = Progress::new(Index::new(5), 256, 0); // next=5, match=0, Probe
    assert!(p.maybe_update(Index::new(7))); // ack 7 → match=7, next=8
    assert_eq!(p.match_index(), Index::new(7));
    assert_eq!(p.next_index(), Index::new(8));
    assert!(!p.maybe_update(Index::new(6))); // stale ack → no change
    p.decrement(); // reject → next=7, Probe
    assert_eq!(p.next_index(), Index::new(7));
  }

  #[test]
  fn next_index_floors_at_one() {
    let mut p = Progress::new(Index::new(1), 256, 0);
    p.decrement();
    assert_eq!(p.next_index(), Index::new(1));
  }

  #[test]
  fn pause_semantics() {
    let mut p = Progress::new(crate::Index::new(1), 2, 0); // next=1, inflight cap 2
    assert!(!p.is_paused()); // fresh probe can send
    p.sent_entries(crate::Index::new(1), 10);
    assert!(p.is_paused()); // probe sends one, then pauses until ack/heartbeat-resp
    p.become_replicate();
    p.sent_entries(crate::Index::new(2), 10);
    assert!(!p.is_paused()); // replicate: paused only when the window is full
    p.sent_entries(crate::Index::new(3), 10);
    assert!(p.is_paused()); // window (2) now full
  }
}
