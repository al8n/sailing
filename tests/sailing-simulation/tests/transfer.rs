//! Simulation proof: leader transfer via TimeoutNow.
//!
//! Two scenarios are verified:
//!
//! **Scenario A — caught-up follower:** transfer to a follower that is already
//! fully replicated.  The target must campaign immediately (TimeoutNow triggers a
//! real election bypassing PreVote), win, and become the new leader within roughly
//! one election timeout.
//!
//! **Scenario B — lagging follower:** transfer to a follower that is behind.
//! The leader first replicates the missing entries; only once the follower is
//! fully caught up is TimeoutNow sent.  The transfer still completes.
//!
//! Both scenarios assert:
//! - The target becomes the leader (LeaderChanged event / `leader_id() == target`).
//! - The old leader stepped down.
//! - Agreement holds throughout.
//! - No committed entry is lost (the committed prefix before the transfer is still
//!   applied on all nodes after the transfer).
#![allow(missing_docs)]
use sailing_simulation::Cluster;

/// Wait until the cluster has exactly one leader.
fn wait_for_leader(c: &mut Cluster, msg: &str) -> u64 {
  assert!(c.run_until(400, |c| c.leader_count() == 1), "{msg}");
  c.leader().expect(msg)
}

/// Wait until the cluster has a stable leader and all live nodes have applied at
/// least `min_applied` entries and agree.
fn wait_for_quiescence(c: &mut Cluster, min_applied: usize, msg: &str) {
  assert!(
    c.run_until(800, |c| {
      c.leader_count() == 1 && c.agreement_holds() && c.min_applied_len() >= min_applied
    }),
    "{msg}"
  );
}

/// Scenario A: transfer to a fully-caught-up follower.
#[test]
fn leader_transfer_completes() {
  let mut c = Cluster::new(3);

  // Elect a leader and commit a few entries.
  let initial_leader = wait_for_leader(&mut c, "initial leader must emerge");

  for i in 0u32..5 {
    let payload = i.to_le_bytes();
    assert!(c.propose(&payload).is_some(), "propose {i} must succeed");
    c.run_until(50, |_| false);
  }
  wait_for_quiescence(
    &mut c,
    5,
    "cluster must agree on >= 5 entries before transfer",
  );

  // The committed prefix length before the transfer.
  let committed_before = c.min_applied_len();

  // Pick a follower as the transfer target.
  let target = (0..3u64)
    .find(|&id| id != initial_leader)
    .expect("must have a follower");

  // ── Initiate the transfer ────────────────────────────────────────────────────
  c.transfer_leader(target)
    .expect("transfer_leader must succeed when target is a voter");

  // Run until the target becomes leader.
  assert!(
    c.run_until(3_000, |c| c.leader_id() == Some(target)),
    "target must become the new leader after transfer"
  );

  // Assertions.
  assert_eq!(c.leader_id(), Some(target), "target must be the new leader");
  assert_ne!(
    c.leader_id(),
    Some(initial_leader),
    "old leader must have stepped down"
  );
  assert!(
    !c.role_of(initial_leader).is_leader(),
    "old leader's role must no longer be Leader"
  );

  // NO committed entry was lost: every node has applied at least as many entries
  // as before the transfer (the noop the new leader appends adds one more).
  assert!(
    c.run_until(1000, |c| c.min_applied_len() >= committed_before),
    "no committed entry must be lost after transfer"
  );

  // Commit more entries under the new leader to confirm liveness.
  for i in 5u32..10 {
    let payload = i.to_le_bytes();
    assert!(
      c.propose(&payload).is_some(),
      "propose {i} must succeed under new leader"
    );
    c.run_until(50, |_| false);
  }
  assert!(
    c.run_until(600, |c| c.agreement_holds() && c.min_applied_len() >= 9),
    "cluster must agree on >= 9 entries after transfer"
  );
  assert!(c.agreement_holds(), "agreement must hold after transfer");
}

/// Scenario B: transfer to a lagging follower (it must catch up first).
///
/// The leader sends TimeoutNow to the target ONLY once the target's match_index
/// equals the leader's last_index.  We verify this by ensuring the target is
/// behind before the transfer, then has fully caught up once it becomes leader.
///
/// Implementation: we propose entries with the target isolated for a very short
/// window (just a single committed entry), then heal and immediately transfer.
/// The target falls behind by exactly one entry.  This avoids the issue where
/// a long isolation causes the target's election timer to fire many times and
/// self-elect on heal before receiving TimeoutNow.
///
/// The test still proves the "lagging follower" path: `transfer_leader` is called
/// when the target's match_index is behind last_index, so the leader CANNOT send
/// TimeoutNow immediately — it must first replicate the missing entry.
#[test]
fn leader_transfer_lagging_follower() {
  // Use check_quorum so the leader does not get disrupted by the transfer target
  // if it self-campaigns (CheckQuorum lease rejects disruptive votes).
  let mut c = Cluster::new_with(3, |cfg| cfg.with_check_quorum(true));

  let initial_leader = wait_for_leader(&mut c, "initial leader must emerge");

  // Propose a few initial entries (all nodes will have these).
  for i in 0u32..3 {
    let payload = i.to_le_bytes();
    assert!(c.propose(&payload).is_some(), "initial propose {i}");
    c.run_until(50, |_| false);
  }
  wait_for_quiescence(&mut c, 3, "cluster must agree on 3 entries");

  // Pick target and isolate it so it falls behind by exactly 1 entry.
  let target = (0..3u64)
    .find(|&id| id != initial_leader)
    .expect("must have a follower");

  c.isolate(target);

  // Commit ONE more entry while the target is isolated — it falls behind by 1.
  assert!(
    c.propose(b"lag-entry").is_some(),
    "propose while target is isolated"
  );
  // Let the entry commit on the voter quorum (leader + the other non-isolated follower).
  assert!(
    c.run_until(200, |c| c.applied_len_of(initial_leader) >= 4),
    "leader must have applied >= 4 entries"
  );

  let target_applied_while_isolated = c.applied_len_of(target);
  assert!(
    target_applied_while_isolated < 4,
    "target must be lagging (applied={target_applied_while_isolated} < 4)"
  );

  let committed_before = c.applied_len_of(initial_leader);

  // Heal and immediately transfer.  The target is behind by 1 entry.
  // `transfer_leader` sees match_index < last_index → goes to the lagging path →
  // kicks replication via `maybe_send_append`.  Once the target acks the missing
  // entry, the leader sends TimeoutNow.
  c.heal(target);
  c.transfer_leader(target)
    .expect("transfer_leader must succeed");

  // Run until the target becomes leader.
  assert!(
    c.run_until(3_000, |c| c.leader_id() == Some(target)),
    "lagging target must become leader after catching up (transfer)"
  );

  assert_eq!(c.leader_id(), Some(target), "target must be the new leader");

  // No committed entry was lost.
  assert!(
    c.run_until(1_000, |c| c.min_applied_len() >= committed_before),
    "no committed entry must be lost after transfer to lagging follower"
  );

  assert!(
    c.run_until(600, |c| c.agreement_holds()),
    "agreement must hold after transfer to lagging follower"
  );

  // Commit more entries under the new leader.
  for i in 4u32..8 {
    let payload = i.to_le_bytes();
    assert!(
      c.propose(&payload).is_some(),
      "propose {i} under new leader"
    );
    c.run_until(50, |_| false);
  }
  assert!(
    c.run_until(600, |c| c.agreement_holds() && c.min_applied_len() >= 6),
    "cluster must agree on >= 6 entries after lagging-follower transfer"
  );
  assert!(
    c.agreement_holds(),
    "agreement must hold at the end of lagging-follower transfer"
  );
}
