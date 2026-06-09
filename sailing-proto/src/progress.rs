//! Per-peer replication progress (Raft §replication). M2 uses Probe/Replicate with naive
//! one-step back-off on reject; M4 adds the `Inflights` window + term-skip reject hints.
use crate::Index;

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
#[derive(Debug, Clone, Copy)]
pub struct Progress {
  match_index: Index,
  next_index: Index,
  state: ProgressState,
}

impl Progress {
  /// A fresh peer: nothing acked, send from `next_index`, probing.
  #[inline(always)]
  pub fn new(next_index: Index) -> Self {
    Self {
      match_index: Index::ZERO,
      next_index,
      state: ProgressState::Probe,
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

  /// Enter steady-state replication.
  pub fn become_replicate(&mut self) {
    self.state = ProgressState::Replicate;
    self.next_index = self.match_index.next();
  }

  /// On a successful ack of index `n`: advance match/next if it moved forward.
  pub fn maybe_update(&mut self, n: Index) -> bool {
    let updated = n > self.match_index;
    if updated {
      self.match_index = n;
    }
    if self.next_index <= n {
      self.next_index = n.next();
    }
    updated
  }

  /// On a reject: back `next_index` off by one (floored at 1) and re-probe.
  pub fn decrement(&mut self) {
    self.next_index = Index::new(self.next_index.get().saturating_sub(1).max(1));
    self.state = ProgressState::Probe;
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::Index;

  #[test]
  fn progress_update_and_decrement() {
    let mut p = Progress::new(Index::new(5)); // next=5, match=0, Probe
    assert!(p.maybe_update(Index::new(7))); // ack 7 → match=7, next=8
    assert_eq!(p.match_index(), Index::new(7));
    assert_eq!(p.next_index(), Index::new(8));
    assert!(!p.maybe_update(Index::new(6))); // stale ack → no change
    p.decrement(); // reject → next=7, Probe
    assert_eq!(p.next_index(), Index::new(7));
  }

  #[test]
  fn next_index_floors_at_one() {
    let mut p = Progress::new(Index::new(1));
    p.decrement();
    assert_eq!(p.next_index(), Index::new(1));
  }
}
