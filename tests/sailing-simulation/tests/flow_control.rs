#![allow(missing_docs)]
use sailing_simulation::Cluster;

/// A partitioned follower catches up after healing even under a tight inflight window.
///
/// Scenario: 3-node cluster with `max_inflight_msgs = 4`. Isolate follower 2, propose
/// ~100 entries (commit on the 2-node majority), then heal and assert the whole cluster
/// converges to the same applied history.
///
/// The bounded window (4) means the leader can only have 4 un-acked in-flight
/// `AppendEntries` to the catching-up peer at any one time.  The test proves that
/// bounded flow-control does not block convergence — the follower still catches up.
/// (Window correctness itself is proven by the unit test
/// `empty_appends_do_not_wedge_inflight_window` in `sailing-proto`.)
#[test]
fn lagging_follower_catches_up_bounded() {
  // Small inflight window to exercise the bounded-pacing path.
  let mut c = Cluster::new_with(3, |cfg| {
    cfg
      .with_max_inflight_msgs(4)
      .expect("4 is a valid inflight cap")
  });

  // Elect a leader.
  assert!(
    c.run_until(200, |c| c.leader_count() == 1),
    "a leader must emerge within 200 steps"
  );

  // Isolate one follower before any proposals.
  let leader = c.leader().unwrap();
  let isolated = (0..3u64).find(|&n| n != leader).unwrap();
  c.isolate(isolated);

  // Propose 100 entries; they commit on the leader + other follower quorum.
  for i in 0u32..100 {
    let payload = i.to_le_bytes();
    // The leader may change if it times out while the majority can't hear it;
    // keep trying until we get a slot.
    assert!(
      c.propose(&payload).is_some(),
      "propose {i} must succeed on the current leader"
    );
    c.run_until(30, |_| false);
  }

  // Let the majority stabilise before healing.
  c.run_until(100, |_| false);

  // Heal the partitioned follower.
  c.heal(isolated);

  // The whole cluster must converge: agree on the same applied history and every
  // node must have applied at least 90 entries (100 data + 1 no-op leader entry;
  // the isolated node may miss the very last few during a concurrent election).
  assert!(
    c.run_until(2000, |c| {
      c.agreement_holds() && c.min_applied_len() >= 90
    }),
    "all nodes must converge to >= 90 applied entries after the partition heals"
  );
  assert!(c.agreement_holds(), "agreement must hold after catchup");
}

/// A follower with a divergent tail re-syncs after healing.
///
/// Scenario:
///   1. Elect a leader (term 1). Propose 10 entries — all three nodes replicate them
///      (committed on quorum).
///   2. Isolate follower 2.  While isolated it repeatedly times out, bumping its own term
///      (it cannot win an election alone, but its term advances beyond the leader's).
///   3. Propose 20 more entries on the original leader (quorum = leader + follower 1).
///   4. Heal follower 2.  It now re-discovers the cluster at a stale log position AND a
///      possibly higher term.  The term-skip reject hint must converge it fast.
///
/// The proto unit test `divergent_follower_resyncs_fast_via_term_skip` already verifies
/// the O(terms) exact round-trip count; this test validates end-to-end convergence through
/// the full simulation loop.
#[test]
fn divergent_follower_resyncs_fast() {
  let mut c = Cluster::new(3);

  // Elect a leader.
  assert!(
    c.run_until(200, |c| c.leader_count() == 1),
    "initial leader must emerge"
  );

  // Propose 10 entries and let all three nodes replicate them.
  for i in 0u32..10 {
    c.propose(&i.to_le_bytes())
      .expect("propose must succeed while a leader is present");
    c.run_until(30, |_| false);
  }
  assert!(
    c.run_until(200, |c| c.agreement_holds() && c.min_applied_len() >= 10),
    "all three nodes must agree on >= 10 entries before the partition"
  );

  // Isolate follower 2. It will keep timing out and incrementing its term while cut off,
  // which is the source of the potential divergence: when healed it arrives with a higher
  // term and a stale log, forcing a term-skip re-sync.
  let leader = c.leader().unwrap();
  let isolated = (0..3u64).find(|&n| n != leader).unwrap();
  c.isolate(isolated);

  // Let the isolated follower time out several times (advances term internally).
  c.run_until(300, |_| false);

  // Propose 20 more entries on the quorum (leader + other follower).
  for i in 10u32..30 {
    // The leader may have stepped down if there was disruption; keep proposing.
    if c.propose(&i.to_le_bytes()).is_none() {
      // No leader right now — tick until one emerges, then retry.
      c.run_until(200, |c| c.leader_count() == 1);
      c.propose(&i.to_le_bytes());
    }
    c.run_until(30, |_| false);
  }
  c.run_until(200, |_| false);

  // Heal the isolated follower. It must catch up and agree.
  c.heal(isolated);

  assert!(
    c.run_until(2000, |c| {
      c.agreement_holds() && c.min_applied_len() >= 25
    }),
    "divergent follower must re-sync and all nodes must agree on >= 25 applied entries"
  );
  assert!(
    c.agreement_holds(),
    "agreement must hold after divergent re-sync"
  );
}
