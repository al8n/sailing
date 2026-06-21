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
  let mut p = Progress::new(Index::new(1), 2, 0); // next=1, inflight cap 2
  assert!(!p.is_paused()); // fresh probe can send
  p.sent_entries(Index::new(1), 10);
  assert!(p.is_paused()); // probe sends one, then pauses until ack/heartbeat-response
  p.become_replicate();
  p.sent_entries(Index::new(2), 10);
  assert!(!p.is_paused()); // replicate: paused only when the window is full
  p.sent_entries(Index::new(3), 10);
  assert!(p.is_paused()); // window (2) now full
}

// --- ProgressState::Snapshot ---

#[test]
fn snapshot_state_as_str_and_predicate() {
  assert_eq!(ProgressState::Snapshot(Index::new(10)).as_str(), "snapshot");
  assert!(ProgressState::Snapshot(Index::new(10)).is_snapshot());
  assert!(!ProgressState::Probe.is_snapshot());
  assert!(!ProgressState::Replicate.is_snapshot());
}

#[test]
fn snapshot_state_is_always_paused() {
  let mut p = Progress::new(Index::new(5), 256, 0);
  p.become_snapshot(Index::new(10));
  assert!(p.is_paused());
  assert!(p.state().is_snapshot());
}

#[test]
fn become_snapshot_records_pending_index() {
  let mut p = Progress::new(Index::new(5), 256, 0);
  p.become_snapshot(Index::new(20));
  assert!(p.state().is_snapshot());
  assert!(p.is_paused());
  // pending_snapshot index is stored in the variant
  if let ProgressState::Snapshot(pending) = p.state() {
    assert_eq!(pending, Index::new(20));
  } else {
    panic!("expected Snapshot state");
  }
}

#[test]
fn maybe_update_past_pending_snapshot_becomes_probe() {
  let mut p = Progress::new(Index::new(5), 256, 0);
  p.become_snapshot(Index::new(10));
  // ack at exactly pending_snapshot → transition to Probe
  p.maybe_update(Index::new(10));
  assert!(p.state().is_probe());
  assert!(!p.is_paused());
}

#[test]
fn maybe_update_below_pending_snapshot_stays_in_snapshot() {
  let mut p = Progress::new(Index::new(5), 256, 0);
  p.become_snapshot(Index::new(10));
  // ack below pending_snapshot → stays Snapshot
  p.maybe_update(Index::new(9));
  assert!(p.state().is_snapshot());
  assert!(p.is_paused());
}

#[test]
fn snapshot_state_display() {
  assert_eq!(
    std::format!("{}", ProgressState::Snapshot(Index::new(0))),
    "snapshot"
  );
}

// --- free_inflight_on_heartbeat (etcd FreeFirstOne) ---

#[test]
fn free_inflight_on_heartbeat_replicate_full_frees_one() {
  // Replicate peer with inflight cap=2; fill it then call free_inflight_on_heartbeat.
  let mut p = Progress::new(Index::new(1), 2, 0);
  p.become_replicate();
  p.sent_entries(Index::new(1), 10);
  p.sent_entries(Index::new(2), 20);
  assert!(p.is_paused(), "window full => paused");

  p.free_inflight_on_heartbeat();
  assert!(
    !p.is_paused(),
    "one slot freed => Replicate peer is no longer paused"
  );
  // Calling again on non-full window is a no-op (does not corrupt state).
  p.free_inflight_on_heartbeat();
  assert!(!p.is_paused());
}

#[test]
fn free_inflight_on_heartbeat_probe_noop() {
  // Probe state: free_inflight_on_heartbeat must not touch the probe-pause flag.
  let mut p = Progress::new(Index::new(1), 2, 0);
  p.sent_entries(Index::new(1), 10); // probe pause set
  assert!(p.is_paused());
  p.free_inflight_on_heartbeat(); // no-op for Probe
  assert!(
    p.is_paused(),
    "Probe pause must not be cleared by free_inflight_on_heartbeat"
  );
}

#[test]
fn free_inflight_on_heartbeat_snapshot_noop() {
  // Snapshot state: always paused; free_inflight_on_heartbeat must be a no-op.
  let mut p = Progress::new(Index::new(1), 2, 0);
  p.become_snapshot(Index::new(10));
  assert!(p.is_paused());
  p.free_inflight_on_heartbeat(); // no-op for Snapshot
  assert!(
    p.is_paused(),
    "Snapshot pause must not be cleared by free_inflight_on_heartbeat"
  );
}

/// become_probe re-probes from match+1 (etcd BecomeProbe), even when next_index is stale-HIGH — so the
/// transition is correct by construction and does not rest on the caller to reset it.
#[test]
fn become_probe_resets_next_to_match_plus_one() {
  let mut p = Progress::new(Index::new(100), 256, 0); // next=100, match=0
  assert!(p.maybe_update(Index::new(5))); // match=5; next stays 100 (100 > 5, not advanced)
  assert_eq!(
    p.next_index(),
    Index::new(100),
    "next is stale-high before the reset"
  );
  p.become_probe();
  assert_eq!(
    p.next_index(),
    Index::new(6),
    "become_probe must re-probe from match+1, not retain the stale-high next"
  );
  assert_eq!(p.match_index(), Index::new(5));
}
