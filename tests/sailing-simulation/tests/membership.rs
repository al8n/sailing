//! End-to-end membership-change tests for the sailing Raft simulation.
//!
//! Every test runs to quiescence through the tick loop, which fires the structural oracles
//! (append-before-ack, one-grant-per-term) automatically. The agreement oracle is asserted
//! explicitly at the end of each test (and often inline to check invariants mid-scenario).
//!
//! Tests are deterministic: virtual clock + seeded PRNG (no `thread_rng`).
#![allow(missing_docs)]
use bytes::Bytes;
use sailing_proto::{
  ConfChange, ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2,
};
use sailing_simulation::Cluster;

// ── Helpers ──────────────────────────────────────────────────────────────────────────────────────

/// Wait until the cluster has elected a stable leader.
fn wait_for_leader(c: &mut Cluster, msg: &str) -> u64 {
  assert!(c.run_until(400, |c| c.leader_count() == 1), "{msg}");
  c.leader().expect(msg)
}

/// Wait until the cluster has a stable leader and all live nodes have applied at least
/// `min_applied` normal entries and agree.
fn wait_for_quiescence(c: &mut Cluster, min_applied: usize, msg: &str) {
  assert!(
    c.run_until(800, |c| {
      c.leader_count() == 1 && c.agreement_holds() && c.min_applied_len() >= min_applied
    }),
    "{msg}"
  );
}

/// Wait until the leader's conf_changed count has advanced by at least `delta` from `baseline`.
/// Returns the new conf_changed total for the leader.
fn wait_for_conf_change(c: &mut Cluster, baseline: u64, delta: u64, msg: &str) -> u64 {
  assert!(
    c.run_until(800, |c| {
      if let Some(leader) = c.leader() {
        c.conf_changed_count(leader) >= baseline + delta
      } else {
        false
      }
    }),
    "{msg}"
  );
  let leader = c.leader().unwrap();
  c.conf_changed_count(leader)
}

// ── Test 1 ──────────────────────────────────────────────────────────────────────────────────────
/// A 3-node cluster grows to 5 voters one node at a time (3→4→5), with commands proposed
/// between additions. All nodes must end up with the same applied log, and each new node
/// must have fully caught up.
#[test]
fn add_voter_grows_quorum() {
  let mut c = Cluster::new(3);

  // Elect a leader.
  wait_for_leader(&mut c, "initial leader must emerge");

  // Propose some commands before any membership change.
  for i in 0u32..5 {
    let payload = i.to_le_bytes();
    assert!(c.propose(&payload).is_some(), "propose {i} must succeed");
    c.run_until(30, |_| false);
  }
  wait_for_quiescence(&mut c, 5, "initial 3 nodes must agree on >= 5 entries");

  // Record current conf_changed count before adding node 3.
  let leader = wait_for_leader(&mut c, "leader must exist before adding node 3");
  let cc_before_3 = c.conf_changed_count(leader);

  // Add node 3 (3→4).
  c.add_node(3);
  // Wait for the ConfChange to commit (leader's conf_changed advances).
  wait_for_conf_change(&mut c, cc_before_3, 1, "ConfChange for node 3 must commit");
  // Node 3 must catch up.
  assert!(
    c.run_until(800, |c| c.agreement_holds() && c.applied_len_of(3) >= 5),
    "node 3 must join and catch up to >= 5 entries"
  );

  // Propose more commands now that we have 4 voters.
  wait_for_leader(&mut c, "a leader must exist after node 3 joins");
  for i in 5u32..10 {
    let payload = i.to_le_bytes();
    assert!(c.propose(&payload).is_some(), "propose {i} must succeed");
    c.run_until(30, |_| false);
  }
  wait_for_quiescence(&mut c, 9, "4-node cluster must agree on >= 9 entries");

  // Add node 4 (4→5).
  let leader = wait_for_leader(&mut c, "leader must exist before adding node 4");
  let cc_before_4 = c.conf_changed_count(leader);
  c.add_node(4);
  wait_for_conf_change(&mut c, cc_before_4, 1, "ConfChange for node 4 must commit");
  assert!(
    c.run_until(800, |c| c.agreement_holds() && c.applied_len_of(4) >= 5),
    "node 4 must join and catch up to >= 5 entries"
  );

  // Propose more commands with the full 5-voter cluster.
  wait_for_leader(&mut c, "a leader must exist in 5-node cluster");
  for i in 10u32..15 {
    let payload = i.to_le_bytes();
    assert!(c.propose(&payload).is_some(), "propose {i} must succeed");
    c.run_until(30, |_| false);
  }

  // Final quiescence: ALL 5 nodes must agree.
  assert!(
    c.run_until(1000, |c| c.agreement_holds() && c.min_applied_len() >= 10),
    "all 5 nodes must agree on >= 10 applied entries"
  );
  assert!(c.agreement_holds(), "agreement must hold at the end");
}

// ── Test 2 ──────────────────────────────────────────────────────────────────────────────────────
/// A 5-node cluster shrinks down to 3 voters. We remove a follower first, then command
/// more entries, then remove the OLD leader (which triggers step-down + new election).
/// Agreement holds throughout.
#[test]
fn remove_voter_shrinks_quorum() {
  let mut c = Cluster::new(5);

  // Elect a leader.
  let original_leader = wait_for_leader(&mut c, "initial leader must emerge in 5-node cluster");

  // Propose some commands.
  for i in 0u32..5 {
    let payload = i.to_le_bytes();
    assert!(c.propose(&payload).is_some(), "propose {i} must succeed");
    c.run_until(30, |_| false);
  }
  wait_for_quiescence(&mut c, 5, "5-node cluster must agree on >= 5 entries");

  // Find a non-leader follower to remove first.
  let leader_now = wait_for_leader(&mut c, "a leader must exist before first removal");
  let follower_to_remove = (0..5u64).find(|&id| id != leader_now).unwrap();
  let cc_baseline = c.conf_changed_count(leader_now);

  // Remove a follower (5→4).
  c.remove_node(follower_to_remove);
  // Wait for the conf change to be applied by the leader.
  wait_for_conf_change(
    &mut c,
    cc_baseline,
    1,
    "first RemoveNode conf change must apply",
  );
  wait_for_quiescence(
    &mut c,
    5,
    "cluster must stabilise after removing a follower (min_applied)",
  );

  // Propose more commands.
  wait_for_leader(&mut c, "a leader must exist after first removal");
  for i in 5u32..10 {
    let payload = i.to_le_bytes();
    assert!(
      c.propose(&payload).is_some(),
      "propose {i} must succeed after first removal"
    );
    c.run_until(30, |_| false);
  }
  wait_for_quiescence(
    &mut c,
    8,
    "cluster must agree on >= 8 entries after first removal",
  );

  // Find another non-leader, non-removed follower to remove (4→3).
  let leader_now2 = wait_for_leader(&mut c, "a leader must exist before second removal");
  let second_follower_to_remove = (0..5u64)
    .find(|&id| id != leader_now2 && id != follower_to_remove)
    .unwrap();
  let cc_baseline2 = c.conf_changed_count(leader_now2);

  c.remove_node(second_follower_to_remove);
  wait_for_conf_change(
    &mut c,
    cc_baseline2,
    1,
    "second RemoveNode conf change must apply",
  );
  wait_for_quiescence(
    &mut c,
    8,
    "cluster must stabilise after removing second follower",
  );

  // Now remove the CURRENT leader. The leader must step down (U6), and a new leader
  // must emerge among the remaining voters.
  let current_leader = wait_for_leader(&mut c, "a leader must exist before removing leader");
  // The current_leader's tracker was updated after the second removal: voters = {X, Y, current_leader}
  // where X and Y are the remaining two nodes.
  // Propose RemoveNode(current_leader) on it.
  c.remove_node(current_leader);

  // A new leader must emerge (different from the removed one).
  assert!(
    c.run_until(800, |c| { c.leader().is_some_and(|l| l != current_leader) }),
    "a new leader must emerge after removing the current leader"
  );
  assert_ne!(
    c.leader().unwrap(),
    current_leader,
    "new leader must differ"
  );

  // Propose commands under the new leader.
  wait_for_leader(&mut c, "a leader must exist after step-down");
  for i in 10u32..15 {
    let payload = i.to_le_bytes();
    assert!(
      c.propose(&payload).is_some(),
      "propose {i} must succeed under new leader"
    );
    c.run_until(30, |_| false);
  }

  // Final check: the remaining live nodes agree.
  assert!(
    c.run_until(800, |c| c.agreement_holds() && c.min_applied_len() >= 10),
    "remaining cluster must agree on >= 10 entries after full shrink"
  );
  assert!(c.agreement_holds(), "agreement must hold after full shrink");
  let _ = original_leader; // suppress unused warning
}

// ── Test 3 ──────────────────────────────────────────────────────────────────────────────────────
/// A `ConfChangeV2` with `Implicit` transition atomically swaps two nodes (add node 4,
/// remove node 2) in a single joint-consensus round. The cluster enters joint config
/// then auto-leaves once committed. Agreement holds across the entire joint → simple
/// transition.
///
/// Setup: 3-node cluster, then add node 3 (simple), then wire node 4 and propose the V2
/// that atomically adds 4 and removes 2. The V2 step genuinely exercises joint consensus
/// (2 changes + Implicit = enter_joint + auto_leave).
#[test]
fn joint_consensus_replace_two() {
  let mut c = Cluster::new(3);

  // Elect a leader.
  wait_for_leader(&mut c, "initial leader must emerge");

  // Propose some commands.
  for i in 0u32..5 {
    let payload = i.to_le_bytes();
    assert!(c.propose(&payload).is_some(), "propose {i} must succeed");
    c.run_until(30, |_| false);
  }
  wait_for_quiescence(&mut c, 5, "initial 3 nodes must agree");

  // Add node 3 as a simple voter (3→4 voters). This simple change commits normally.
  let leader = wait_for_leader(&mut c, "leader must exist before adding node 3");
  let cc_before_3 = c.conf_changed_count(leader);
  c.add_node(3);
  wait_for_conf_change(&mut c, cc_before_3, 1, "simple AddNode(3) must commit");
  assert!(
    c.run_until(800, |c| c.agreement_holds() && c.applied_len_of(3) >= 5),
    "node 3 must join and catch up"
  );

  // Wire node 4 WITHOUT proposing — the joint V2 below will add it.
  // Node 4 must exist in the sim so it can receive AppendEntries during the joint phase.
  c.wire_joining_node(4);

  // Propose a V2 with Implicit transition: [AddNode(4), RemoveNode(2)].
  // 2 changes + Implicit → enter_joint with auto_leave=true.
  // After this commits, the leader auto-appends a leave-joint entry.
  let leader = wait_for_leader(&mut c, "leader must exist for V2 proposal");
  let cc_before_v2 = c.conf_changed_count(leader);

  let v2 = ConfChangeV2::new(
    ConfChangeTransition::Implicit,
    vec![
      ConfChangeSingle::new(ConfChangeType::AddNode, 4u64),
      ConfChangeSingle::new(ConfChangeType::RemoveNode, 2u64),
    ],
    Bytes::new(),
  );
  let idx = c
    .propose_conf_change_v2(v2)
    .expect("V2 joint conf change must succeed on leader");
  assert!(idx.get() > 0, "V2 proposal must return a valid index");

  // Run to quiescence: enter-joint fires conf_changed(1), auto-leave fires conf_changed(2).
  // The leader must apply 2 conf changes (enter-joint + leave-joint).
  // Node 4 must also have caught up.
  assert!(
    c.run_until(2000, |c| {
      if let Some(leader) = c.leader() {
        let cc = c.conf_changed_count(leader);
        cc >= cc_before_v2 + 2 && c.applied_len_of(4) >= 5 && c.agreement_holds()
      } else {
        false
      }
    }),
    "joint V2 must enter-joint and auto-leave; node 4 must catch up; all nodes agree"
  );

  // After leave-joint, node 2 is removed from incoming voters and the leader's Progress
  // map. The leader no longer replicates to node 2. Mark node 2 as removed in the sim
  // so the agreement oracle and min_applied_len skip it (its log stops advancing here).
  c.mark_removed(2);

  // All remaining live nodes (0, 1, 3, 4) must agree.
  assert!(
    c.run_until(800, |c| c.agreement_holds() && c.min_applied_len() >= 5),
    "live nodes (0,1,3,4) must agree after joint transition"
  );

  // Propose commands after the joint transition.
  wait_for_leader(&mut c, "a leader must exist after joint transition");
  for i in 5u32..10 {
    let payload = i.to_le_bytes();
    assert!(
      c.propose(&payload).is_some(),
      "propose after joint must succeed"
    );
    c.run_until(30, |_| false);
  }

  assert!(
    c.run_until(800, |c| c.agreement_holds() && c.min_applied_len() >= 8),
    "all live nodes (0,1,3,4) must agree on >= 8 entries after joint transition"
  );
  assert!(
    c.agreement_holds(),
    "agreement must hold after joint consensus"
  );
}

// ── Test 4 ──────────────────────────────────────────────────────────────────────────────────────
/// A learner does NOT count for quorum: commands commit without the learner's ack.
/// Then the learner is promoted to voter and must now count for quorum.
#[test]
fn learner_does_not_count_for_quorum() {
  let mut c = Cluster::new(3);

  // Elect a leader.
  wait_for_leader(&mut c, "initial leader must emerge");

  // Propose commands to establish a baseline.
  for i in 0u32..5 {
    let payload = i.to_le_bytes();
    assert!(
      c.propose(&payload).is_some(),
      "initial propose {i} must succeed"
    );
    c.run_until(30, |_| false);
  }
  wait_for_quiescence(&mut c, 5, "initial cluster must agree on >= 5 entries");

  // Add node 3 as a LEARNER.
  let leader = wait_for_leader(&mut c, "a leader must exist before adding learner");
  let cc_before_learner = c.conf_changed_count(leader);
  c.add_learner(3);
  wait_for_conf_change(
    &mut c,
    cc_before_learner,
    1,
    "AddLearnerNode(3) must commit",
  );
  // Wait for the learner to receive replication and catch up.
  assert!(
    c.run_until(800, |c| c.applied_len_of(3) >= 5 && c.agreement_holds()),
    "learner node 3 must join and catch up"
  );

  // NOW isolate the learner and assert the voter quorum (3 voters: 0, 1, 2) still commits.
  // The key proof: commands commit even though the learner cannot ack.
  c.isolate(3);
  wait_for_leader(&mut c, "a leader must exist after isolating learner");

  for i in 5u32..10 {
    let payload = i.to_le_bytes();
    assert!(
      c.propose(&payload).is_some(),
      "propose {i} must succeed without learner (proves learner not in quorum)"
    );
    c.run_until(50, |_| false);
  }

  // The 3-voter quorum (nodes 0, 1, 2) must commit and agree without node 3.
  assert!(
    c.run_until(600, |c| {
      c.applied_len_of(0) >= 10 && c.applied_len_of(1) >= 10 && c.applied_len_of(2) >= 10
    }),
    "3-voter quorum must commit 10 entries without the learner's ack"
  );

  // Heal the learner's partition and let it catch up.
  // A new proposal triggers the leader to replicate the full log to the learner.
  // (The heartbeat mechanism alone can also do this, but a proposal is more direct
  // and ensures immediate replication via `maybe_send_append` for all peers.)
  c.heal(3);
  // Propose one more entry so the leader proactively sends to node 3.
  assert!(
    c.run_until(100, |c| c.leader_count() == 1),
    "a leader must exist to propose after healing"
  );
  c.propose(b"post-heal");
  // The learner must now catch up to all previously committed entries + the new one.
  assert!(
    c.run_until(800, |c| { c.applied_len_of(3) >= 10 }),
    "learner must catch up after healing"
  );

  // Agreement oracle covers non-removed nodes. Node 3 is not removed — verify it converged.
  assert!(
    c.agreement_holds(),
    "agreement must hold after learner catches up"
  );

  // Promote the learner to voter: propose AddNode(3) on an existing learner promotes it.
  let leader = wait_for_leader(&mut c, "a leader must exist for promotion");
  let cc_before_promote = c.conf_changed_count(leader);
  let cc = ConfChange::new(ConfChangeType::AddNode, 3u64, Bytes::new());
  c.propose_conf_change(cc)
    .expect("promote learner to voter must succeed");
  wait_for_conf_change(
    &mut c,
    cc_before_promote,
    1,
    "learner promotion conf change must commit",
  );

  // All nodes (including the newly-promoted voter 3) must agree.
  assert!(
    c.run_until(500, |c| c.agreement_holds() && c.min_applied_len() >= 10),
    "all nodes including promoted voter 3 must agree"
  );

  // After promotion, node 3 is a voter. Verify that with 4 voters, isolating 2 non-leader
  // voters stalls commits (quorum = 3/4; 2/4 cannot commit).
  let leader = wait_for_leader(&mut c, "a leader must exist in the 4-voter cluster");
  let isolated_pair: Vec<u64> = (0..4u64).filter(|&id| id != leader).take(2).collect();
  c.isolate(isolated_pair[0]);
  c.isolate(isolated_pair[1]);

  let applied_before = c.applied_len_of(leader);
  for _ in 0..5 {
    c.propose(b"blocked");
    c.run_until(20, |_| false);
  }

  // Run for a while — commits must NOT advance because 2/4 is below quorum.
  c.run_until(300, |_| false);
  let applied_after = c.applied_len_of(leader);
  assert_eq!(
    applied_before, applied_after,
    "commits must stall when 2 of 4 voters are isolated"
  );

  // Heal and let the cluster converge.
  c.heal(isolated_pair[0]);
  c.heal(isolated_pair[1]);
  assert!(
    c.run_until(600, |c| c.agreement_holds()),
    "cluster must re-converge after healing"
  );
  assert!(
    c.agreement_holds(),
    "agreement must hold after learner promotion"
  );
}

// ── Test 5 ──────────────────────────────────────────────────────────────────────────────────────
/// The current leader is removed from the cluster. It must step down (U6), and a new
/// leader must emerge among the remaining voters.
#[test]
fn remove_leader_steps_down() {
  let mut c = Cluster::new(3);

  // Elect a leader.
  wait_for_leader(&mut c, "initial leader must emerge");

  // Propose a few commands.
  for i in 0u32..5 {
    let payload = i.to_le_bytes();
    assert!(c.propose(&payload).is_some(), "propose {i} must succeed");
    c.run_until(30, |_| false);
  }
  wait_for_quiescence(
    &mut c,
    5,
    "cluster must agree on >= 5 entries before removing leader",
  );

  let old_leader = wait_for_leader(&mut c, "a stable leader must exist before removal");

  // Remove the current leader.
  c.remove_node(old_leader);

  // The old leader must step down AND a new leader must emerge among the remaining 2 voters.
  assert!(
    c.run_until(800, |c| { c.leader().is_some_and(|l| l != old_leader) }),
    "the removed leader must step down and a new leader must emerge"
  );

  let new_leader = c.leader().unwrap();
  assert_ne!(
    new_leader, old_leader,
    "the new leader must be different from the removed one"
  );

  // Propose commands under the new leader.
  for i in 5u32..10 {
    let payload = i.to_le_bytes();
    assert!(
      c.propose(&payload).is_some(),
      "propose {i} must succeed under new leader"
    );
    c.run_until(30, |_| false);
  }

  // The remaining 2 active voters must agree.
  assert!(
    c.run_until(800, |c| c.agreement_holds() && c.min_applied_len() >= 8),
    "remaining cluster must agree on >= 8 entries after leader removal"
  );
  assert!(
    c.agreement_holds(),
    "agreement must hold after leader is removed and new leader is elected"
  );
}

// ── Test 6 ──────────────────────────────────────────────────────────────────────────────────────
/// Regression: a healed follower (learner) resumes replication via heartbeats alone.
///
/// Before the `free_inflight_on_heartbeat` fix (etcd `FreeFirstOne`), a `Replicate` peer
/// whose entire in-flight window was dropped during a partition would stay wedged: on each
/// `HeartbeatResponse` `is_paused()` returned `true` (full window) so `maybe_send_append` sent
/// nothing. The node only recovered when an unrelated client proposal happened to call
/// `maybe_send_append`. This test asserts that a healed node catches up WITHOUT any
/// post-heal `propose` call.
///
/// We force `max_inflight_msgs = 1` so that even a single AppendEntries sent to the
/// isolated learner (and then dropped) fills the window. Without the fix, after healing,
/// every HeartbeatResponse from the learner still sees `is_paused() == true` (full window,
/// no acks ever arrived) and `maybe_send_append` sends nothing — the learner stalls forever.
#[test]
fn healed_follower_catchup_via_heartbeats() {
  // cap = 1 ensures the inflight window fills after the very first send to the isolated
  // learner; this is the minimal condition to reproduce the wedging bug.
  let mut c = Cluster::new_with(3, |cfg| {
    cfg
      .with_max_inflight_msgs(1)
      .expect("1 is a valid inflight cap")
  });

  // Elect a leader.
  wait_for_leader(&mut c, "initial leader must emerge");

  // Propose 5 entries and let voters reach quiescence.
  for i in 0u32..5 {
    let payload = i.to_le_bytes();
    assert!(c.propose(&payload).is_some(), "propose {i} must succeed");
    c.run_until(30, |_| false);
  }
  wait_for_quiescence(
    &mut c,
    5,
    "initial 3-node cluster must agree on >= 5 entries",
  );

  // Add a learner (node 3) and wait for it to join and catch up.
  let leader = wait_for_leader(&mut c, "leader must exist before adding learner");
  let cc_before = c.conf_changed_count(leader);
  c.add_learner(3);
  wait_for_conf_change(&mut c, cc_before, 1, "AddLearnerNode(3) must commit");
  assert!(
    c.run_until(800, |c| c.applied_len_of(3) >= 5 && c.agreement_holds()),
    "learner node 3 must join and catch up to >= 5 entries"
  );

  // Isolate the learner so MsgApp/AppendResponse traffic is dropped (fills its inflight window).
  c.isolate(3);
  wait_for_leader(&mut c, "leader must exist after isolating learner");

  // Commit 5 more entries on the voter quorum while the learner is cut off.
  for i in 5u32..10 {
    let payload = i.to_le_bytes();
    assert!(
      c.propose(&payload).is_some(),
      "propose {i} must succeed without learner (proves learner not in quorum)"
    );
    c.run_until(50, |_| false);
  }
  assert!(
    c.run_until(600, |c| {
      c.applied_len_of(0) >= 10 && c.applied_len_of(1) >= 10 && c.applied_len_of(2) >= 10
    }),
    "3-voter quorum must commit 10 entries without the learner"
  );

  // Heal the partition — the learner is behind by 5 entries.
  // NO further client proposal is issued. Catch-up must happen via heartbeats alone
  // (the `free_inflight_on_heartbeat` fix frees one slot per heartbeat round, letting
  // the leader resend to the stale learner until it is fully caught up).
  c.heal(3);

  assert!(
    c.run_until(2000, |c| {
      c.applied_len_of(3) >= 10 && c.agreement_holds()
    }),
    "healed learner must catch up to >= 10 entries via heartbeats alone (no post-heal propose)"
  );
  assert!(
    c.agreement_holds(),
    "agreement must hold after learner catches up via heartbeats"
  );
}
