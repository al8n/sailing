use super::*;

/// A healthy 3-node all-voter node view at index `idx` with the given commit/applied, every node
/// holding the same durable log of `(index=i, term=1, cmd=[i as u8])` for `i in 1..=durable_last`.
fn healthy_node(id: u64, commit: u64, durable_last: u64) -> NodeView {
  let durable_entries: Vec<DurableEntry> = (1..=durable_last)
    .map(|i| DurableEntry {
      index: i,
      term: 1,
      data: std::vec![i as u8],
    })
    .collect();
  let applied_log: Vec<(u64, Vec<u8>)> = (1..=commit).map(|i| (i, std::vec![i as u8])).collect();
  NodeView {
    id,
    removed: false,
    is_voter: true,
    poisoned: false,
    is_leader: id == 0,
    term: 1,
    commit,
    applied: commit,
    applied_log,
    durable_first: 1,
    durable_last,
    // A healthy node has no un-flushed tail, so the visible last index equals the durable one.
    visible_last: durable_last,
    durable_entries,
    snapshot_last_index: 0,
    snapshot_last_term: 0,
    hardstate_commit: commit,
    inflight_staged: 0,
    incarnation: 0,
  }
}

/// Build a [`ClusterView`] from `nodes`, deriving the authoritative `committed_voters` set the way
/// the production [`Cluster::view`](crate::Cluster) does: the ids that consider themselves voters
/// in their committed config and are not removed. This exercises the oracle's real authoritative
/// voter-set path (not just the `None` fallback) while keeping every teeth test's voter population
/// exactly what its node self-reports describe.
fn cv(seed: u64, tick: u64, nodes: Vec<NodeView>) -> ClusterView {
  let voters: BTreeSet<u64> = nodes
    .iter()
    .filter(|n| !n.removed && n.is_voter)
    .map(|n| n.id)
    .collect();
  ClusterView {
    seed,
    tick,
    committed_voters: if voters.is_empty() {
      None
    } else {
      Some(voters)
    },
    nodes,
  }
}

/// A healthy, fully-agreed 3-node cluster: every node committed+applied `commit` entries and
/// durably holds `durable_last` entries. Passes the WHOLE suite (the positive baseline that
/// proves no oracle false-positives on a correct snapshot).
fn healthy_cluster(commit: u64, durable_last: u64) -> ClusterView {
  cv(
    1,
    1,
    (0..3)
      .map(|id| healthy_node(id, commit, durable_last))
      .collect(),
  )
}

#[test]
fn healthy_cluster_passes_full_suite() {
  let mut ck = Checker::new();
  // Several ticks of monotonic growth — must stay green (proves no false positives + that the
  // history oracles accept legitimate forward progress).
  for c in 0..=5u64 {
    let view = healthy_cluster(c, c.max(1));
    assert_eq!(ck.check(&view), Ok(()), "healthy commit={c} must pass");
  }
}

#[test]
fn agreement_detects_divergent_applied() {
  // Two nodes disagree on the command applied at index 2.
  let a = healthy_node(0, 3, 3);
  let mut b = healthy_node(1, 3, 3);
  b.applied_log[1] = (2, std::vec![0xFF]); // node 1's applied[index=2] now differs from node 0's
  let view = cv(7, 42, std::vec![a, b, healthy_node(2, 3, 3)]);
  let v = agreement(&view).unwrap_err();
  assert_eq!(v.oracle, "agreement");
  assert!(v.detail.contains("applied[1] diverges"), "{}", v.detail);
}

#[test]
fn append_before_ack_detects_applied_beyond_visible() {
  // A node applied index 5 but its VISIBLE log only reaches 3 (and no snapshot covers it) — it
  // cannot have applied an entry it cannot even read. (`healthy_node` sets visible_last ==
  // durable_last == 3.)
  let mut n = healthy_node(0, 3, 3);
  n.applied = 5;
  n.commit = 5;
  let view = cv(1, 9, std::vec![n]);
  let v = append_before_ack(&view).unwrap_err();
  assert_eq!(v.oracle, "append_before_ack");
  assert!(v.detail.contains("exceeds its visible log"), "{}", v.detail);
}

#[test]
fn append_before_ack_allows_applied_within_visible_unflushed_tail() {
  // The proto legitimately applies committed entries from its VISIBLE log before its own fsync:
  // applied may exceed durable_last as long as it stays within visible_last. This must NOT fire
  // (durability is guaranteed per-entry by commit_is_quorum_durable, and on a quorum elsewhere).
  let mut n = healthy_node(0, 5, 3); // durable_last=3, applied=commit=5
  n.visible_last = 5; // a visible-but-unflushed tail (indices 4,5)
  let view = cv(1, 9, std::vec![n]);
  assert!(
    append_before_ack(&view).is_ok(),
    "applied within the visible (un-flushed) tail is legal"
  );
}

#[test]
fn commit_is_quorum_durable_detects_solo_commit() {
  // Node 0 has commit=5 and durably holds entry 5, but the other two nodes' durable logs only
  // reach 4 — only 1 of 3 durable logs has entry 5, below the quorum of 2. (The heartbeat
  // class: a node advanced commit without quorum-durable replication.)
  let mut n0 = healthy_node(0, 5, 5);
  n0.applied = 4; // keep append-before-ack happy elsewhere; this test calls the oracle directly
  let n1 = healthy_node(1, 4, 4);
  let n2 = healthy_node(2, 4, 4);
  let view = cv(3, 11, std::vec![n0, n1, n2]);
  let v = commit_is_quorum_durable(&view, 0).unwrap_err();
  assert_eq!(v.oracle, "commit_is_quorum_durable");
  assert!(
    v.detail.contains("only 1 of 3 voter durable logs"),
    "{}",
    v.detail
  );
}

#[test]
fn commit_is_quorum_durable_detects_term_mismatch() {
  // A quorum holds index 5, but with a DIFFERENT term than the committing node — not the same
  // committed entry. Must be detected (the heartbeat-commit-of-stale-tail class).
  let mut n0 = healthy_node(0, 5, 5); // node 0 holds (5, term 1) and committed it
  n0.applied = 4;
  let mut n1 = healthy_node(1, 4, 5);
  n1.durable_entries[4].term = 2; // node 1 holds (5, term 2)
  let mut n2 = healthy_node(2, 4, 5);
  n2.durable_entries[4].term = 2; // node 2 holds (5, term 2)
  let view = cv(3, 12, std::vec![n0, n1, n2]);
  let v = commit_is_quorum_durable(&view, 0).unwrap_err();
  assert_eq!(v.oracle, "commit_is_quorum_durable");
  assert!(v.detail.contains("with that term"), "{}", v.detail);
}

#[test]
fn commit_is_quorum_durable_accepts_snapshot_covered_entry() {
  // A node whose commit index is below its snapshot boundary (compacted away) still counts as
  // durable-present at the boundary term — must NOT false-positive.
  let mut nodes = Vec::new();
  for id in 0..3u64 {
    let mut n = healthy_node(id, 6, 8);
    // Compact out 1..=5: snapshot covers index 6 at the boundary; durable entries start at 6.
    n.snapshot_last_index = 5;
    n.snapshot_last_term = 1;
    n.durable_first = 6;
    n.durable_entries.retain(|e| e.index >= 6);
    nodes.push(n);
  }
  let view = cv(1, 1, nodes);
  assert_eq!(commit_is_quorum_durable(&view, 0), Ok(()));
}

#[test]
fn commit_is_quorum_durable_uses_authoritative_voter_set_not_self_view() {
  // Regression for a false positive. The harness had prematurely marked a node
  // `removed` (an accepted-but-never-committed RemoveNode) while it was STILL a real committed
  // voter holding the entry, and had grown a learner. Deriving the quorum from per-node
  // `is_voter & !removed` then under-counted the witnesses and false-fired. With the authoritative
  // committed voter set threaded in, the real quorum is recognized and the oracle stays green.
  //
  // Committed voter set = {0,1,2} (3 voters → quorum 2). Node 1 is the leader committing index 5.
  // Node 0 is a real voter that is simply BEHIND (durable only to 3 — committed off a quorum that
  // did not include it). Node 2 is a real voter that HOLDS index 5 but the harness flagged it
  // `removed=true`. Node 3 is a learner that also holds index 5 but must NOT count. The durable
  // witnesses among the real voters are {1, 2} = 2 ≥ quorum, so this is sound and must pass.
  let mut n0 = healthy_node(0, 3, 3); // behind real voter
  n0.is_voter = true;
  let mut n1 = healthy_node(1, 5, 5); // leader, holds 5
  n1.is_leader = true;
  let mut n2 = healthy_node(2, 5, 5); // real voter holding 5, but harness-`removed`
  n2.removed = true;
  let mut n3 = healthy_node(3, 5, 5); // learner holding 5 (must not count toward the quorum)
  n3.is_voter = false;
  n3.is_leader = false;
  let view = ClusterView {
    seed: 4,
    tick: 336,
    committed_voters: Some(BTreeSet::from([0, 1, 2])),
    nodes: std::vec![n0, n1, n2, n3],
  };
  assert_eq!(
    commit_is_quorum_durable(&view, 0),
    Ok(()),
    "the real {{0,1,2}} voter quorum holds index 5; the oracle must not false-fire on the \
       harness's stale removed/learner bookkeeping"
  );
}

#[test]
fn commit_is_quorum_durable_keeps_teeth_with_authoritative_voter_set() {
  // The flip side: with the SAME authoritative voter set, a commit that is genuinely NOT on a
  // voter quorum must still trip. Voter set = {0,1,2} (quorum 2); node 1 (leader) committed index
  // 5 but only node 1 durably holds it (nodes 0 and 2 reach only index 4), and the learner node 3
  // holding 5 does not count. 1 < 2 → violation. Proves the authoritative-set path did not blunt
  // the oracle.
  let mut n0 = healthy_node(0, 4, 4);
  n0.is_voter = true;
  let mut n1 = healthy_node(1, 5, 5);
  n1.is_leader = true;
  n1.applied = 4;
  let mut n2 = healthy_node(2, 4, 4);
  n2.is_voter = true;
  let mut n3 = healthy_node(3, 5, 5); // learner holds 5 — must not rescue the quorum
  n3.is_voter = false;
  n3.is_leader = false;
  let view = ClusterView {
    seed: 4,
    tick: 1,
    committed_voters: Some(BTreeSet::from([0, 1, 2])),
    nodes: std::vec![n0, n1, n2, n3],
  };
  let v = commit_is_quorum_durable(&view, 0).unwrap_err();
  assert_eq!(v.oracle, "commit_is_quorum_durable");
  assert!(
    v.detail.contains("only 1 of 3 voter durable logs"),
    "{}",
    v.detail
  );
}

#[test]
fn monotonic_commit_detects_regression() {
  let mut ck = Checker::new();
  let up = healthy_cluster(5, 5);
  assert_eq!(monotonic_commit(&mut ck, &up), Ok(()));
  // Now node 0's commit drops 5 -> 3 (e.g. a restart that forgot the durable commit).
  let mut down = healthy_cluster(5, 5);
  down.nodes[0].commit = 3;
  let v = monotonic_commit(&mut ck, &down).unwrap_err();
  assert_eq!(v.oracle, "monotonic_commit");
  assert!(
    v.detail.contains("commit regressed from 5 to 3"),
    "{}",
    v.detail
  );
}

#[test]
fn no_committed_rewrite_detects_conflicting_apply() {
  let mut ck = Checker::new();
  // Tick 1: index 2 committed as 'A'.
  let mut v1 = healthy_cluster(2, 2);
  for n in v1.nodes.iter_mut() {
    n.applied_log[1] = (2, std::vec![b'A']);
  }
  assert_eq!(no_committed_rewrite(&mut ck, &v1), Ok(()));
  // Tick 2: a node applies 'B' at index 2 — a committed entry was overwritten.
  let mut v2 = healthy_cluster(2, 2);
  v2.nodes[0].applied_log[1] = (2, std::vec![b'B']);
  let v = no_committed_rewrite(&mut ck, &v2).unwrap_err();
  assert_eq!(v.oracle, "no_committed_rewrite");
  assert!(v.detail.contains("committed index 2"), "{}", v.detail);
}

#[test]
fn term_monotonic_detects_regression() {
  let mut ck = Checker::new();
  let mut up = healthy_cluster(1, 1);
  for n in up.nodes.iter_mut() {
    n.term = 5;
  }
  assert_eq!(term_monotonic(&mut ck, &up), Ok(()));
  let mut down = healthy_cluster(1, 1);
  for n in down.nodes.iter_mut() {
    n.term = 5;
  }
  down.nodes[1].term = 2; // node 1's term regressed 5 -> 2
  let v = term_monotonic(&mut ck, &down).unwrap_err();
  assert_eq!(v.oracle, "term_monotonic");
  assert!(
    v.detail.contains("term regressed from 5 to 2"),
    "{}",
    v.detail
  );
}

#[test]
fn boundedness_detects_offset_desync() {
  // The durable entry count disagrees with the index window — a compaction/offset GC bug.
  let mut n = healthy_node(0, 3, 3);
  n.durable_entries.pop(); // 2 entries but window [1..=3] says 3
  let view = cv(1, 1, std::vec![n]);
  let v = boundedness(&view).unwrap_err();
  assert_eq!(v.oracle, "boundedness");
  assert!(
    v.detail.contains("disagrees with its index window"),
    "{}",
    v.detail
  );
}

#[test]
fn boundedness_detects_staged_leak() {
  let mut n = healthy_node(0, 3, 3);
  n.inflight_staged = 5000; // unbounded staged writes — flush/discard leak
  let view = cv(1, 1, std::vec![n]);
  let v = boundedness(&view).unwrap_err();
  assert_eq!(v.oracle, "boundedness");
  assert!(v.detail.contains("staged"), "{}", v.detail);
}

#[test]
fn durable_prefix_detects_c1_lost_commit_on_restart() {
  // Scenario: a node had durably committed a prefix of length 5 — its durable HardState.commit is
  // 5 and its durable log holds entries 1..=5. It then crashed and RESTARTED. The bug is that
  // `restart` rebuilt an empty / snapshot-only state machine, recovering commit = 0 DESPITE the
  // durable HardState.commit = 5 and the durable log covering it. The durable-prefix oracle must
  // detect that the recovered commit silently forgot the durably-committed prefix.
  let mut n = healthy_node(0, 0, 5); // recovered commit = 0 (the bug) ...
  n.applied = 0;
  n.applied_log.clear();
  n.hardstate_commit = 5; // ... but the DURABLE committed prefix is 5 (durable_last = 5).
  let view = cv(0xC1, 100, std::vec![n]);
  let v = durable_prefix(&view).unwrap_err();
  assert_eq!(v.oracle, "durable_prefix");
  assert!(
    v.detail.contains("must not silently forget"),
    "{}",
    v.detail
  );
  assert!(
    v.detail.contains("commit=0") && v.detail.contains("durable committed prefix is 5"),
    "{}",
    v.detail
  );
}

#[test]
fn durable_prefix_accepts_correct_recovery() {
  // The CORRECT behavior: restart recovered commit = HardState.commit = 5 (durable log covers
  // it). No violation.
  let n = healthy_node(0, 5, 5); // commit == hardstate_commit == durable_last == 5
  let view = cv(1, 1, std::vec![n]);
  assert_eq!(durable_prefix(&view), Ok(()));
}

#[test]
fn durable_prefix_accepts_resynced_lost_log_tail() {
  // The exotic-but-safe case: a crash lost an in-flight LOG write while the commit-watermark
  // write survived, so durable HardState.commit (5) > durable_last (3). The recovery formula
  // clamps commit to min(hs.commit, durable_last) = 3 and re-syncs the rest from the leader. The
  // oracle requires only that commit covers the prefix the durable LOG still holds (3), so a
  // recovered commit of 3 is accepted.
  let mut n = healthy_node(0, 3, 3);
  n.hardstate_commit = 5; // persisted ahead of the (lost) log tail
  let view = cv(1, 1, std::vec![n]);
  assert_eq!(durable_prefix(&view), Ok(()));
}

#[test]
#[should_panic(expected = "SAFETY ORACLE VIOLATION")]
fn check_or_panic_carries_seed_and_tick() {
  let mut ck = Checker::new();
  let mut v = healthy_cluster(3, 3);
  v.seed = 0xDEAD_BEEF;
  v.tick = 777;
  v.nodes[0].applied_log[1] = (2, std::vec![0xEE]); // diverge → agreement trips
  ck.check_or_panic(&v);
}

#[test]
fn check_or_panic_message_contains_seed_tick() {
  use std::panic;
  let mut ck = Checker::new();
  let mut v = healthy_cluster(3, 3);
  v.seed = 0xABCD_1234;
  v.tick = 999;
  v.nodes[0].applied_log[1] = (2, std::vec![0xEE]);
  let msg = panic::catch_unwind(panic::AssertUnwindSafe(|| ck.check_or_panic(&v)))
    .unwrap_err()
    .downcast::<String>()
    .map(|s| *s)
    .unwrap_or_default();
  assert!(msg.contains("seed=2882343476"), "{msg}"); // 0xABCD_1234
  assert!(msg.contains("tick=999"), "{msg}");
}
