//! Simulation proof: PreVote non-disruption guarantee.
//!
//! `rejoining_node_does_not_disrupt` demonstrates the core PreVote invariant:
//! a partitioned follower whose election timer fires repeatedly (entering
//! PreCandidate each time) CANNOT inflate the cluster term because pre-vote
//! requests from an isolated node are always rejected (the two-of-three quorum
//! needed to win pre-vote is unreachable).  When the partition heals the original
//! leader is still the leader and the cluster term has not increased beyond the
//! term established during the initial election.
//!
//! Non-vacuousness proof:
//! - We verify that the isolated node actually entered PreCandidate (its election
//!   timer fired and it attempted to campaign) by asserting its role was
//!   PreCandidate at some point during isolation (checked via `role_of`).
//! - We verify the original leader is STILL the leader after healing.
//! - We verify `max_term()` did NOT increase beyond the leader's election term
//!   (the isolated node's `term()` must still equal the pre-isolation term).
//! - As a baseline contrast: WITHOUT PreVote, an isolated node would promote itself
//!   to Candidate (real term bump) on each election timeout — by the time many
//!   timeouts pass, its term would far exceed the cluster's.  With PreVote it stays
//!   at term T because every pre-vote round is rejected (no quorum).
#![allow(missing_docs)]
use sailing_simulation::Cluster;

/// Wait until the cluster has exactly one leader.
fn wait_for_leader(c: &mut Cluster, msg: &str) -> u64 {
  assert!(c.run_until(400, |c| c.leader_count() == 1), "{msg}");
  c.leader().expect(msg)
}

/// A partitioned node (running PreVote) must NOT disrupt the original leader or
/// inflate the cluster term when it rejoins.
#[test]
fn rejoining_node_does_not_disrupt() {
  // Build a 3-node cluster with PreVote + CheckQuorum enabled on every node.
  // CheckQuorum pairs with PreVote: without it a partitioned node's real-term
  // RequestVote could still raise the cluster term on heal even though we've
  // blocked the pre-vote, because followers that haven't heard from a leader
  // recently would grant the vote.
  let mut c = Cluster::new_with(3, |cfg| cfg.with_pre_vote(true).with_check_quorum(true));

  let initial_leader = wait_for_leader(&mut c, "initial leader must emerge");
  for i in 0u32..5 {
    let payload = i.to_le_bytes();
    assert!(c.propose(&payload).is_some(), "propose {i} must succeed");
    c.run_until(50, |_| false);
  }
  assert!(
    c.run_until(400, |c| c.agreement_holds() && c.min_applied_len() >= 5),
    "cluster must agree on >= 5 entries before isolation"
  );

  let term_before_isolation = c.term_of(initial_leader);

  // Pick a follower to isolate (not the leader).
  let isolated_node = (0..3u64)
    .find(|&id| id != initial_leader)
    .expect("must have a follower");

  // The election timeout is 1 000 ms.  We run for ~15 election timeouts so that,
  // without PreVote, the isolated node would have bumped its term many times.
  c.isolate(isolated_node);

  // Run until we observe the isolated node attempting a pre-vote campaign
  // (role == PreCandidate).  This is the "non-vacuous" proof: the node's timer DID
  // fire and it DID try to campaign — it just could not win a quorum.
  let saw_pre_candidate = c.run_until(10_000, |c| c.role_of(isolated_node).is_pre_candidate());
  assert!(
    saw_pre_candidate,
    "isolated node must have entered PreCandidate (its election timer fired)"
  );

  // Keep running through many more timeouts so the isolated node has ample
  // opportunity to bump its term (it won't, because PreVote holds it back).
  c.run_until(20_000, |_| false);

  // The original leader must still be leader (CheckQuorum keeps the majority
  // reachable, so the leader does not step down; the isolated node cannot
  // disrupt it).
  assert_eq!(
    c.leader(),
    Some(initial_leader),
    "original leader must still be leader during isolation"
  );

  // CORE INVARIANT: the isolated node's real term must NOT have inflated.
  // With PreVote, every election timeout → PreCandidate (no real term bump).
  // The isolated node's term must still equal the pre-isolation cluster term.
  let isolated_term_during = c.term_of(isolated_node);
  assert_eq!(
    isolated_term_during, term_before_isolation,
    "isolated node's term must not have inflated (PreVote held it in PreCandidate)"
  );

  c.heal(isolated_node);

  assert!(
    c.run_until(3_000, |c| c.agreement_holds() && c.min_applied_len() >= 5),
    "cluster must re-converge after healing"
  );

  // The original leader must STILL be the leader (not displaced by the rejoining node).
  // On heal the isolated node sends its remaining pre-vote to the now-reachable peers,
  // but they reject it because they have an active leader (lease still valid).
  // The isolated node then becomes a Follower (no quorum won) and follows the original leader.
  assert_eq!(
    c.leader(),
    Some(initial_leader),
    "original leader must still be leader after healing (no disruption)"
  );

  // CRITICAL: the cluster term must NOT have inflated.
  // max_term() must equal term_before_isolation (no node had a real term bump).
  let max_term_after = c.max_term();
  assert_eq!(
    max_term_after, term_before_isolation,
    "cluster max_term must not have inflated due to the isolated node's campaigns"
  );

  // Commit more entries under the still-original leader to confirm liveness.
  for i in 5u32..10 {
    let payload = i.to_le_bytes();
    assert!(
      c.propose(&payload).is_some(),
      "propose {i} must succeed after healing"
    );
    c.run_until(50, |_| false);
  }
  assert!(
    c.run_until(600, |c| c.agreement_holds() && c.min_applied_len() >= 10),
    "cluster must agree on >= 10 entries after the rejoined node catches up"
  );
  assert!(c.agreement_holds(), "agreement must hold at the end");
}
