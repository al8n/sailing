#![allow(missing_docs)]
use sailing_simulation::Cluster;

/// Committed entries must survive a follower crash: after a crash-and-restart the
/// follower re-learns commit from the leader's heartbeats/appends and re-applies its
/// durable log, converging to the same applied history as the rest of the cluster.
#[test]
fn committed_entries_survive_a_follower_crash() {
  let mut c = Cluster::new(3);
  assert!(
    c.run_until(100, |c| c.leader_count() == 1),
    "a leader should emerge within 100 steps"
  );
  // Propose 5 entries and let them replicate.
  for i in 0..5u32 {
    c.propose(&i.to_le_bytes());
    c.run_until(50, |_| false);
  }
  assert!(
    c.run_until(100, |c| c.agreement_holds() && c.min_applied_len() >= 5),
    "cluster must agree on >= 5 applied entries before crash"
  );

  // Crash a follower (not the leader), propose 5 more entries, then check convergence.
  let leader = c.leader().unwrap();
  let follower = (0..3u64).find(|&n| n != leader).unwrap();
  c.crash(follower);
  for i in 5..10u32 {
    c.propose(&i.to_le_bytes());
    c.run_until(50, |_| false);
  }
  assert!(
    c.run_until(300, |c| c.agreement_holds() && c.min_applied_len() >= 10),
    "crashed follower must rejoin and converge to >= 10 applied entries"
  );
}

/// Rolling crash of every node must not violate agreement or produce two leaders.
/// This validates that vote and log are preserved across restarts and the
/// double-vote tripwire in the simulator does not fire.
#[test]
fn restart_preserves_vote_and_log() {
  let mut c = Cluster::new(3);
  assert!(
    c.run_until(100, |c| c.leader_count() == 1),
    "initial leader must emerge"
  );
  c.propose(b"x");
  c.run_until(50, |_| false);

  // Crash every node once in a rolling fashion; after each crash let the cluster run.
  for n in 0..3u64 {
    c.crash(n);
    c.run_until(150, |_| false);
  }
  assert!(
    c.run_until(300, |c| c.agreement_holds()),
    "agreement must hold after rolling crash of all nodes"
  );
  // The double-vote tripwire (in Cluster::tick / drain_storage_all) would have panicked
  // if any node granted its vote twice in the same term; reaching here means no double-vote.
  assert!(c.leader_count() <= 1, "never two leaders");
}
