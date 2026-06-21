//! Integration: a crash INSIDE the fsync window (an in-flight append staged but not yet
//! flushed) loses that in-flight tail, and the node recovers the durable committed prefix
//! (commit persistence, the C1 durable-prefix oracle) and re-syncs the lost tail from the leader
//! to agreement.
//!
//! This is the test that makes append-before-ack / C1 MEANINGFUL under crash: with synchronous
//! stores the window does not exist, so earlier work proved those rules only against a degenerate
//! commit-on-submit store. Here the window is real — the test would FAIL if the proto acted on
//! un-flushed data (it would ack/commit an entry the follower never durably stored) or if
//! `restart` lost the durably-committed prefix.
#![allow(missing_docs)]
use sailing_proto::PoisonReason;
use sailing_simulation::{Cluster, StorageFaults};

/// Drive an async-mode cluster to agreement, then crash a follower while it has a staged-but-
/// unflushed append (the fsync window). The follower must lose only the in-flight tail and
/// re-converge.
#[test]
fn crash_in_fsync_window_loses_inflight_and_recovers() {
  let mut c = Cluster::new_async(3, 0xC0FFEE);
  assert!(
    c.run_until(200, |c| c.leader_count() == 1),
    "a leader should emerge in async mode within 200 steps"
  );

  // Commit a baseline so there is a durable committed prefix to recover after the crash.
  for i in 0..4u32 {
    c.propose(&i.to_le_bytes());
    c.run_until(80, |_| false);
  }
  assert!(
    c.run_until(200, |c| c.agreement_holds() && c.min_applied_len() >= 4),
    "async cluster must agree on the baseline before the crash"
  );

  let leader = c.leader().unwrap();
  let follower = (0..3u64).find(|&n| n != leader).unwrap();

  // Open the fsync window on the follower: propose a fresh entry (the leader STAGES its own
  // append, then on flush replicates it), and drive the cluster — WITHOUT ever flushing the
  // follower — until the follower has STAGED the resulting append and owes an ack on the
  // not-yet-durable completion. The window is now open.
  let idx = c
    .propose(b"in-flight")
    .expect("leader accepts the proposal");
  let staged = c.open_fsync_window(follower, 50);
  assert!(
    staged,
    "the follower must be sitting in the fsync window (staged, un-flushed append) — \
     otherwise the test is vacuous"
  );
  // The in-flight append is VISIBLE to the follower (submit-then-read contract) but NOT yet
  // DURABLE — a crash before flush will lose exactly this un-synced tail.
  assert!(
    c.durable_last_index_of(follower) < idx,
    "the in-flight append must NOT be durable on the follower before the crash"
  );

  // Crash the follower mid-window: discard the staged (un-flushed) append, then restart from the
  // durable stores. The in-flight tail is lost; the durable committed prefix survives.
  c.crash(follower);
  assert!(
    !c.node_has_inflight(follower),
    "the in-flight window must be empty after the crash"
  );

  // The follower re-replicates the lost tail from the leader (Leader Completeness) and re-applies
  // its durable committed prefix (C1), converging with the rest of the cluster.
  for i in 4..8u32 {
    c.propose(&i.to_le_bytes());
    c.run_until(80, |_| false);
  }
  assert!(
    c.run_until(400, |c| c.agreement_holds() && c.min_applied_len() >= 8),
    "the follower crashed mid-fsync-window must rejoin and converge to >= 8 applied entries"
  );
}

/// A baseline async-mode cluster (no crash) must still elect and reach agreement — proving the
/// async stores + the tick flush model do not break ordinary consensus.
#[test]
fn async_cluster_reaches_agreement_without_faults() {
  let mut c = Cluster::new_async(7, 1);
  assert!(
    c.run_until(200, |c| c.leader_count() == 1),
    "async cluster must elect a leader"
  );
  for i in 0..6u32 {
    c.propose(&i.to_le_bytes());
    c.run_until(80, |_| false);
  }
  assert!(
    c.run_until(300, |c| c.agreement_holds() && c.min_applied_len() >= 6),
    "async cluster must agree on >= 6 applied entries"
  );
}

/// Rolling crash of every node in async mode (each crash potentially inside its own fsync window)
/// must not violate agreement or elect two leaders — the double-vote / append-before-ack
/// tripwires in `Cluster::tick` would panic on a violation.
#[test]
fn async_rolling_crash_preserves_agreement() {
  let mut c = Cluster::new_async(3, 0xABCD);
  assert!(
    c.run_until(200, |c| c.leader_count() == 1),
    "async cluster must elect a leader"
  );
  c.propose(b"x");
  c.run_until(80, |_| false);

  for n in 0..3u64 {
    c.crash(n);
    c.run_until(200, |_| false);
  }
  assert!(
    c.run_until(400, |c| c.agreement_holds()),
    "agreement must hold after a rolling crash of every node in async mode"
  );
  assert!(c.leader_count() <= 1, "never two leaders");
}

/// A `transient_read` storage fault must surface as a VALUE (the store `Error`) that the proto
/// treats as fatal, POISONING the node (review C2) — proving the poison-on-read-error path is now
/// reachable through the simulator. It must NEVER panic. With faults installed at 100% on a
/// follower, the follower's next committed-range read fails and poisons it; the rest of the
/// cluster (a healthy majority) is unaffected and keeps agreement.
#[test]
fn transient_read_fault_poisons_a_follower_via_proto() {
  let mut c = Cluster::new_async(3, 0xFEED);
  assert!(
    c.run_until(200, |c| c.leader_count() == 1),
    "async cluster must elect a leader"
  );
  // Commit a baseline so there is a committed tail to re-apply (the read that fails happens in
  // apply_committed when the follower replays committed entries).
  for i in 0..3u32 {
    c.propose(&i.to_le_bytes());
    c.run_until(80, |_| false);
  }
  assert!(
    c.run_until(200, |c| c.min_applied_len() >= 3),
    "baseline must commit before installing faults"
  );

  let leader = c.leader().unwrap();
  let follower = (0..3u64).find(|&n| n != leader).unwrap();
  assert!(!c.is_poisoned(follower), "follower starts healthy");

  // Install an always-firing transient-read fault on the follower, then propose more so the
  // follower must perform a committed-range read (apply_committed) and hit the fault.
  c.set_node_faults(
    follower,
    StorageFaults {
      transient_read_per_mille: 1000,
      ..StorageFaults::none()
    },
    1,
  );
  for i in 3..8u32 {
    c.propose(&i.to_le_bytes());
    c.run_until(80, |_| false);
  }

  assert!(
    c.run_until(300, |c| c.is_poisoned(follower)),
    "the follower must be poisoned by the transient-read fault (review C2 path), not panic"
  );
  assert_eq!(
    c.poison_reason_of(follower),
    Some(PoisonReason::LogRead),
    "poison cause must be the failed committed-range log read"
  );
  // The healthy majority is unaffected and keeps making progress / agreement.
  assert!(
    c.run_until(200, |c| c.leader_count() == 1),
    "the surviving majority keeps a leader despite the poisoned follower"
  );
}
