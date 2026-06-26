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

#[test]
fn snapshot_state_as_str_and_predicate() {
  assert_eq!(
    ProgressState::Snapshot {
      pending: Index::new(10),
      acked_through: 0,
      total: 256,
    }
    .as_str(),
    "snapshot"
  );
  assert!(
    ProgressState::Snapshot {
      pending: Index::new(10),
      acked_through: 0,
      total: 256,
    }
    .is_snapshot()
  );
  assert!(!ProgressState::Probe.is_snapshot());
  assert!(!ProgressState::Replicate.is_snapshot());
}

#[test]
fn snapshot_state_is_always_paused() {
  let mut p = Progress::new(Index::new(5), 256, 0);
  p.become_snapshot(Index::new(10), 256);
  assert!(p.is_paused());
  assert!(p.state().is_snapshot());
}

#[test]
fn become_snapshot_records_pending_index() {
  let mut p = Progress::new(Index::new(5), 256, 0);
  p.become_snapshot(Index::new(20), 256);
  assert!(p.state().is_snapshot());
  assert!(p.is_paused());
  // pending_snapshot index is stored in the variant
  if let ProgressState::Snapshot { pending, .. } = p.state() {
    assert_eq!(pending, Index::new(20));
  } else {
    panic!("expected Snapshot state");
  }
}

#[test]
fn become_snapshot_at_or_below_match_reprobes_instead_of_wedging() {
  // A peer whose `match_index` already covers the snapshot boundary must NOT enter Snapshot state.
  // Once in Snapshot with `match >= pending` it would wedge forever: `resend_snapshot` only re-sends to
  // a peer BEHIND `pending`, and a paused peer receives no append, so no ack ever arrives to drive
  // `maybe_update` out of Snapshot. Re-probe from `match + 1` and resume append replication instead.
  let mut p = Progress::new(Index::new(5), 256, 0);
  p.maybe_update(Index::new(615)); // confirmed replicated through 615
  p.become_snapshot(Index::new(583), 256); // boundary 583 is already covered by match 615
  assert!(
    p.state().is_probe(),
    "a peer whose match is past the boundary must re-probe, not wedge in Snapshot"
  );
  assert!(!p.is_paused());
  // The boundary-equal case is equally redundant and must also re-probe.
  let mut q = Progress::new(Index::new(5), 256, 0);
  q.maybe_update(Index::new(583));
  q.become_snapshot(Index::new(583), 256);
  assert!(q.state().is_probe());
}

#[test]
fn snapshot_acked_tracks_the_followers_watermark() {
  let mut p = Progress::new(Index::new(1), 256, 0);
  p.become_snapshot(Index::new(10), 256);
  let ProgressState::Snapshot {
    pending,
    acked_through,
    ..
  } = p.state()
  else {
    panic!("expected Snapshot state");
  };
  assert_eq!(pending, Index::new(10));
  assert_eq!(acked_through, 0, "become_snapshot seeds acked_through = 0");
  p.snapshot_acked(64);
  // SET (not max): the cursor tracks the follower's reported contiguous watermark, so a later LOWER
  // value (a stale old-boundary ack after a supersede reset) sets it lower — self-correcting, never
  // inflating the cursor and wedging the transfer.
  p.snapshot_acked(32);
  let ProgressState::Snapshot { acked_through, .. } = p.state() else {
    panic!("expected Snapshot state");
  };
  assert_eq!(acked_through, 32);
}

#[test]
fn maybe_update_past_pending_snapshot_becomes_probe() {
  let mut p = Progress::new(Index::new(5), 256, 0);
  p.become_snapshot(Index::new(10), 256);
  // ack at exactly pending_snapshot → transition to Probe
  p.maybe_update(Index::new(10));
  assert!(p.state().is_probe());
  assert!(!p.is_paused());
}

#[test]
fn maybe_update_below_pending_snapshot_stays_in_snapshot() {
  let mut p = Progress::new(Index::new(5), 256, 0);
  p.become_snapshot(Index::new(10), 256);
  // ack below pending_snapshot → stays Snapshot
  p.maybe_update(Index::new(9));
  assert!(p.state().is_snapshot());
  assert!(p.is_paused());
}

#[test]
fn snapshot_state_display() {
  assert_eq!(
    std::format!(
      "{}",
      ProgressState::Snapshot {
        pending: Index::new(0),
        acked_through: 0,
        total: 0
      }
    ),
    "snapshot"
  );
}

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
  p.become_snapshot(Index::new(10), 256);
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

#[test]
fn snapshot_acked_rejects_a_watermark_past_total() {
  // A delayed ack from a superseded, LARGER same-boundary capture reports a watermark past the CURRENT
  // blob's total. Accepting it would inflate the cursor past `total`, so the next send clamps to the end and
  // emits a stale empty tail that can strand the transfer — it must be rejected.
  let mut p = Progress::new(Index::new(1), 256, 0);
  p.become_snapshot(Index::new(10), 50);
  p.snapshot_acked(40);
  let ProgressState::Snapshot { acked_through, .. } = p.state() else {
    panic!("expected Snapshot state");
  };
  assert_eq!(acked_through, 40, "an in-range watermark is accepted");
  p.snapshot_acked(80); // 80 > total 50 — a stale cross-capture over-ack
  let ProgressState::Snapshot { acked_through, .. } = p.state() else {
    panic!("expected Snapshot state");
  };
  assert_eq!(
    acked_through, 40,
    "a watermark past total is rejected, leaving the cursor unchanged"
  );
}
