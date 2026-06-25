//! Per-peer replication progress (Raft §replication). Probe/Replicate drive normal
//! replication (one-step back-off on reject) with an `Inflights` window + term-skip reject
//! hints; `Snapshot` covers a peer that is receiving an InstallSnapshot.
use crate::{Index, Inflights};

/// How the leader is currently replicating to a peer.
///
/// `Snapshot { pending, acked_through }` carries the last log index covered by the snapshot being
/// sent (`pending`) and the highest contiguous byte offset the follower has staged (`acked_through`,
/// the chunk resume cursor); the peer stays paused until it acks at or past `pending`, then
/// transitions back to `Probe`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, derive_more::Display, derive_more::IsVariant)]
#[display("{}", self.as_str())]
pub enum ProgressState {
  /// Last replicated index unknown; send conservatively and narrow on reject.
  Probe,
  /// Steady-state: optimistically advance `next_index` as acks arrive.
  Replicate,
  /// An `InstallSnapshot` is in flight.
  Snapshot {
    /// The snapshot's `last_index` — the peer stays paused until it acks at or past this.
    pending: Index,
    /// The highest contiguous byte offset the follower has staged (the chunk resume cursor).
    acked_through: u64,
  },
}

impl ProgressState {
  /// Stable snake_case name.
  pub const fn as_str(&self) -> &'static str {
    match self {
      Self::Probe => "probe",
      Self::Replicate => "replicate",
      Self::Snapshot { .. } => "snapshot",
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
      ProgressState::Snapshot { .. } => true,
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
      ProgressState::Snapshot { .. } => {
        // In Snapshot state no entries are sent; this is a no-op.
      }
    }
  }

  /// Enter snapshot-delivery state. The peer stays paused until it acks at or past
  /// `pending_snapshot`, then `maybe_update` transitions it back to `Probe`.
  pub fn become_snapshot(&mut self, pending_snapshot: Index) {
    // A peer whose `match_index` already covers the snapshot boundary has those entries durably — it does
    // NOT need the snapshot. Entering Snapshot state here would WEDGE it permanently: it is already
    // `>= pending`, so `resend_snapshot` (which only re-sends to a peer BEHIND `pending`) never fires, and
    // the paused state makes `maybe_send_append` early-return — so no `SnapshotResponse`/`AppendResponse`
    // ever arrives to drive `maybe_update` back out of Snapshot. A caught-up voter stalls forever and the
    // cluster cannot commit. Re-probe from `match_index + 1` instead (resume ordinary append replication).
    // This guards the case where `next_index` has backtracked below `match_index` (a reject hint) and so
    // dipped under `first_index`, making the caller mistake a caught-up peer for one needing a snapshot.
    if self.match_index >= pending_snapshot {
      self.become_probe();
      return;
    }
    self.state = ProgressState::Snapshot {
      pending: pending_snapshot,
      acked_through: 0,
    };
    self.inflight.reset();
    self.msg_app_flow_paused = false;
  }

  /// Set the per-chunk resume cursor for a peer in `Snapshot` state to the follower's reported
  /// contiguous-staged watermark. NOT monotone: a `max` would let a STALE old-boundary `acked_through`
  /// (a reordered ack arriving after a boundary supersede reset the peer to a new boundary) inflate the
  /// new boundary's cursor and wedge the transfer. Tracking the follower's ACTUAL watermark instead
  /// self-corrects — the follower always reports its TRUE contiguous length, so the next ack drives the
  /// cursor to the right place (any out-of-order chunk is retained by the store's staging). No-op if the
  /// peer is not in `Snapshot`.
  pub fn snapshot_acked(&mut self, offset: u64) {
    if let ProgressState::Snapshot { acked_through, .. } = &mut self.state {
      *acked_through = offset;
    }
  }

  /// Enter steady-state replication.
  pub fn become_replicate(&mut self) {
    self.state = ProgressState::Replicate;
    self.next_index = self.match_index.next();
    self.msg_app_flow_paused = false;
    self.inflight.reset();
  }

  /// Revert to probing (on a reject or a step-down), re-probing from `match_index + 1` — etcd's
  /// `BecomeProbe`. Resetting `next_index` here makes the transition correct by construction rather than
  /// resting on every caller to follow with `set_next_index`: a caller with a more precise target (the
  /// append-reject conflict jump) still overrides it, and the snapshot-reject path re-probes — re-sending
  /// the snapshot when `next < first_index` — without depending on a stale pre-snapshot `next_index`.
  pub fn become_probe(&mut self) {
    self.state = ProgressState::Probe;
    self.next_index = self.match_index.next();
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
    if let ProgressState::Snapshot { pending, .. } = self.state
      && n >= pending
    {
      self.become_probe();
    }
    updated
  }

  /// Clear the Probe pause flag without changing state — used by HeartbeatResponse so a
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
  /// This is called on every `HeartbeatResponse` so a healed/partitioned follower resumes
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
