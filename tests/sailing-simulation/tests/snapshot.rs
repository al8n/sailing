#![allow(missing_docs)]
use sailing_proto::Index;
use sailing_simulation::{Cluster, StorageFaults};

/// Low snapshot threshold for simulation tests: triggers compaction after a modest
/// number of proposals so tests run fast without hundreds of entries.
const SNAP_THRESHOLD: usize = 5;

/// A 3-node cluster with a low snapshot threshold takes snapshots and compacts its log
/// after enough entries are committed, while maintaining agreement across all nodes.
///
/// Structural oracles (append-before-ack, one-grant-per-term, agreement) run automatically
/// inside the tick loop's assertion checks — reaching quiescence without a panic proves them.
#[test]
fn leader_snapshots_and_compacts() {
  let mut c = Cluster::new_with(3, |cfg| cfg.with_snapshot_threshold(SNAP_THRESHOLD));

  // Elect a leader.
  assert!(
    c.run_until(200, |c| c.leader_count() == 1),
    "a leader must emerge"
  );

  // Propose enough entries to trigger at least one snapshot cycle.
  // SNAP_THRESHOLD=5: after 10 proposals (threshold applies as applied - first_index >= 5)
  // the leader should snapshot+compact. We propose 20 to be safe.
  for i in 0u32..20 {
    let payload = i.to_le_bytes();
    assert!(c.propose(&payload).is_some(), "propose {i} must succeed");
    c.run_until(30, |_| false);
  }

  // Run to quiescence.
  assert!(
    c.run_until(500, |c| c.agreement_holds() && c.min_applied_len() >= 15),
    "cluster must agree on >= 15 applied entries"
  );

  // Every node's log.first_index() must have advanced past 1 (compaction happened).
  for id in 0..3u64 {
    assert!(
      c.first_index_of(id) > Index::new(1),
      "node {id} log must be compacted (first_index > 1, got {:?})",
      c.first_index_of(id)
    );
  }

  // Agreement oracle (also checked inside the tick loop's structural assertions).
  assert!(
    c.agreement_holds(),
    "agreement must hold after snapshot+compaction"
  );
}

/// A partitioned follower that falls so far behind that the leader's log is compacted past
/// its `next_index` must catch up via `InstallSnapshot` once the partition heals.
///
/// This is the critical test: it asserts that `Event::SnapshotInstalled` actually fired on
/// the lagging follower, proving the catch-up went through `InstallSnapshot` and NOT merely
/// through plain `AppendEntries`.
#[test]
fn lagging_follower_catches_up_via_snapshot() {
  let mut c = Cluster::new_with(3, |cfg| cfg.with_snapshot_threshold(SNAP_THRESHOLD));

  // Elect a leader.
  assert!(
    c.run_until(200, |c| c.leader_count() == 1),
    "a leader must emerge"
  );

  // Identify the leader and one follower to isolate.
  let leader = c.leader().unwrap();
  let isolated = (0..3u64).find(|&n| n != leader).unwrap();

  // Isolate one follower before any proposals.
  c.isolate(isolated);

  // Propose enough entries so the leader snapshots and compacts PAST the isolated
  // follower's current next_index (which is at the leader's first index before proposals,
  // roughly index 1). SNAP_THRESHOLD=5 so ~15 proposals guarantee at least one full cycle.
  for i in 0u32..20 {
    let payload = i.to_le_bytes();
    assert!(c.propose(&payload).is_some(), "propose {i} must succeed");
    c.run_until(30, |_| false);
  }

  // Drain the snapshot cycle on the majority (leader + non-isolated follower).
  c.run_until(200, |_| false);

  // Verify the leader's log is compacted (first_index has advanced).
  assert!(
    c.first_index_of(leader) > Index::new(1),
    "leader log must be compacted before healing (first_index={:?})",
    c.first_index_of(leader)
  );

  // Heal the partition.
  c.heal(isolated);

  // Run until the whole cluster converges.
  assert!(
    c.run_until(2000, |c| {
      c.agreement_holds() && c.min_applied_len() >= 15
    }),
    "lagging follower must catch up and cluster must agree on >= 15 entries"
  );

  // THE CRITICAL ASSERTION: the lagging follower must have received at least one
  // SnapshotInstalled event, proving the catch-up went through InstallSnapshot.
  assert!(
    c.snapshot_install_count(isolated) >= 1,
    "lagging follower (node {isolated}) must have received at least one InstallSnapshot \
     (snapshot_install_count={})",
    c.snapshot_install_count(isolated)
  );

  // Non-vacuity: with the cluster's small `snapshot_chunk_bytes` the catch-up transfer is MULTI-chunk —
  // a delivered InstallSnapshot with offset > 0 is the witness (a single-chunk transfer delivers only
  // offset 0). Guards against the chunked send path silently collapsing back to single-shot.
  assert!(
    c.multi_chunk_deliveries() >= 1,
    "the snapshot catch-up must be MULTI-chunk (a delivered InstallSnapshot with offset > 0); \
     multi_chunk_deliveries={}",
    c.multi_chunk_deliveries()
  );

  assert!(
    c.agreement_holds(),
    "agreement must hold after snapshot catch-up"
  );
}

/// After a node crashes and is restarted from its durable snapshot + committed log tail,
/// it must converge to the same state as the rest of the cluster.
///
/// This is the integration proof of Task 7 (restore-from-snapshot on restart).
#[test]
fn restart_after_snapshot_preserves_state() {
  let mut c = Cluster::new_with(3, |cfg| cfg.with_snapshot_threshold(SNAP_THRESHOLD));

  // Elect a leader.
  assert!(
    c.run_until(200, |c| c.leader_count() == 1),
    "a leader must emerge"
  );

  // Propose enough entries to ensure at least one snapshot+compaction cycle occurs.
  for i in 0u32..20 {
    let payload = i.to_le_bytes();
    assert!(c.propose(&payload).is_some(), "propose {i} must succeed");
    c.run_until(30, |_| false);
  }

  // Let everything quiesce so the snapshot is durable.
  assert!(
    c.run_until(500, |c| c.agreement_holds() && c.min_applied_len() >= 15),
    "cluster must agree on >= 15 entries before crash"
  );

  // Pick a follower to crash (not the leader — we want to test restart from snapshot).
  let leader = c.leader().unwrap();
  let victim = (0..3u64).find(|&n| n != leader).unwrap();

  // Verify the victim's log is compacted (has a durable snapshot to restore from).
  assert!(
    c.first_index_of(victim) > Index::new(1),
    "victim node {victim} log must be compacted before crash (first_index={:?})",
    c.first_index_of(victim)
  );

  // Crash the victim — durable snapshot + log survive; in-memory state is discarded.
  c.crash(victim);

  // Let a few more entries commit while the victim is restarting.
  for i in 20u32..25 {
    c.propose(&i.to_le_bytes());
    c.run_until(30, |_| false);
  }

  // Run to quiescence: the restarted node must rebuild from the snapshot and catch up.
  assert!(
    c.run_until(1000, |c| {
      c.agreement_holds() && c.min_applied_len() >= 15
    }),
    "restarted node must rebuild from snapshot and cluster must agree on >= 15 entries"
  );

  // Agreement oracle.
  assert!(
    c.agreement_holds(),
    "agreement must hold after restart-from-snapshot"
  );
}

/// A COLD snapshot read (the store reports the resident blob as NOT paged in —
/// `SnapshotChunkRead::Pending`) must only DELAY a chunked `InstallSnapshot` transfer, NEVER wedge it:
/// the leader's send DEFERS (emits nothing, mutates no progress) and the heartbeat `resend_snapshot`
/// re-drives it on a later round, so a transient-cold store still completes the transfer.
///
/// This drives the EXACT chunked-snapshot catch-up of `lagging_follower_catches_up_via_snapshot` but
/// with `cold_fetch_per_mille > 0` on every node's stable store, so a fraction of `snapshot_chunk`
/// reads return `Pending` MID-TRANSFER. The far-behind follower must STILL install the snapshot and
/// catch up, the transfer must be MULTI-chunk (small `snapshot_chunk_bytes`), and a cold snapshot read
/// must actually have FIRED (non-vacuity). The per-tick safety-oracle suite (agreement, quorum-durable
/// commit, durable-prefix, …) runs inside every `tick` and panics on any violation, so reaching
/// convergence without a panic proves the deferral preserved safety throughout. A clean single-leader
/// catch-up (one isolated follower, no election churn) keeps the cluster on one quorum the whole time.
#[test]
fn cold_snapshot_read_delays_but_does_not_wedge_catch_up() {
  // ~25% of `snapshot_chunk` reads return Pending — high enough that a multi-chunk transfer hits at
  // least one cold read mid-flight, low enough that the per-heartbeat re-drive completes it briskly.
  const COLD_PER_MILLE: u16 = 250;
  let mut c = Cluster::new_async_with(
    3,
    /* seed */ 0xC01D,
    |cfg| cfg.with_snapshot_threshold(SNAP_THRESHOLD),
  );
  // Arm the cold-snapshot fault on every node (it is rolled on the `&self` `snapshot_chunk` read).
  for id in 0..3u64 {
    let faults = StorageFaults {
      cold_fetch_per_mille: COLD_PER_MILLE,
      ..StorageFaults::none()
    };
    c.set_node_faults(id, faults, 0xC01D ^ id);
  }

  assert!(
    c.run_until(2_000, |c| c.leader_count() == 1),
    "a leader must emerge"
  );
  let leader = c.leader().unwrap();
  let isolated = (0..3u64).find(|&n| n != leader).unwrap();

  // Isolate one follower, then commit far enough that the leader compacts PAST its next index, so the
  // follower can only catch up via a (multi-chunk) InstallSnapshot once healed.
  c.isolate(isolated);
  for i in 0u32..20 {
    let payload = i.to_le_bytes();
    assert!(c.propose(&payload).is_some(), "propose {i} must succeed");
    c.run_until(30, |_| false);
  }
  c.run_until(300, |_| false);
  assert!(
    c.first_index_of(leader) > Index::new(1),
    "leader log must be compacted before healing (first_index={:?})",
    c.first_index_of(leader)
  );

  // Heal and converge. The budget is generous BECAUSE cold reads defer chunks (each re-driven on the
  // next heartbeat) — a cold read may only DELAY the transfer. If the follower never catches up here,
  // the transfer WEDGED (the deferred send was not re-driven) — a real bug, surfaced as this failure.
  c.heal(isolated);
  assert!(
    c.run_until(8_000, |c| c.agreement_holds() && c.min_applied_len() >= 15),
    "the lagging follower must catch up via a cold (deferred) snapshot transfer and the cluster must \
     agree on >= 15 entries — a cold read must DELAY, never WEDGE (snapshot_install_count={}, \
     multi_chunk_deliveries={}, cold_snapshot_reads={})",
    c.snapshot_install_count(isolated),
    c.multi_chunk_deliveries(),
    c.total_cold_snapshot_reads(),
  );

  // The catch-up went through InstallSnapshot, and was MULTI-chunk (small snapshot_chunk_bytes).
  assert!(
    c.snapshot_install_count(isolated) >= 1,
    "the lagging follower (node {isolated}) must have installed at least one snapshot \
     (snapshot_install_count={})",
    c.snapshot_install_count(isolated)
  );
  assert!(
    c.multi_chunk_deliveries() >= 1,
    "the snapshot catch-up must be MULTI-chunk (a delivered InstallSnapshot with offset > 0); \
     multi_chunk_deliveries={}",
    c.multi_chunk_deliveries()
  );

  // NON-VACUITY: a cold snapshot read must actually have fired during the transfer — otherwise the
  // "delays but does not wedge" claim is vacuous (the cold path was never on the critical path).
  assert!(
    c.total_cold_snapshot_reads() > 0,
    "no cold (Pending) snapshot-chunk read fired during the transfer — the cold-snapshot fault never \
     armed on `snapshot_chunk`, so the no-wedge proof is vacuous (cold_snapshot_reads={})",
    c.total_cold_snapshot_reads()
  );

  assert!(
    c.agreement_holds(),
    "agreement must hold after the cold (deferred) snapshot catch-up"
  );

  std::eprintln!(
    "cold-snapshot catch-up: cold_snapshot_reads={} multi_chunk_deliveries={} snapshot_installs={}",
    c.total_cold_snapshot_reads(),
    c.multi_chunk_deliveries(),
    c.snapshot_install_count(isolated),
  );
}
