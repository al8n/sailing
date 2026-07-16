use super::*;

/// A healthy 3-node all-voter node view at index `idx` with the given commit/applied, every node
/// holding the same durable log of `(index=i, term=1, cmd=[i as u8])` for `i in 1..=durable_last`.
fn healthy_node(id: u64, commit: u64, durable_last: u64) -> NodeView {
  let durable_entries: Vec<DurableEntry> = (1..=durable_last)
    .map(|i| DurableEntry {
      index: i,
      term: 1,
      data: std::vec![i as u8],
      is_conf_change: false,
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
    // No transferred snapshot; an empty active config is fine — the membership oracle's genesis/history is
    // taken from LOG-BUILT nodes' configs, so the membership-specific tests populate these explicitly.
    installed_snapshot: false,
    conf_voters: BTreeSet::new(),
    conf_voters_outgoing: BTreeSet::new(),
    conf_learners: BTreeSet::new(),
    conf_learners_next: BTreeSet::new(),
    conf_auto_leave: false,
    conf_changed: 0,
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
    positional_agreement: true,
    seed,
    tick,
    committed_voters: if voters.is_empty() {
      None
    } else {
      Some(voters)
    },
    committed_transitions: Vec::new(),
    new_installs: Vec::new(),
    nodes,
  }
}

/// A [`ConfSnapshot`] from voter + learner ids (the other three fields empty) — the common install/reference
/// membership; tests needing `voters_outgoing` / `learners_next` / `auto_leave` mutate the result.
fn conf(voters: &[u64], learners: &[u64]) -> ConfSnapshot {
  ConfSnapshot {
    voters: voters.iter().copied().collect(),
    voters_outgoing: BTreeSet::new(),
    learners: learners.iter().copied().collect(),
    learners_next: BTreeSet::new(),
    auto_leave: false,
  }
}

/// Observe a transfer install `(node id, boundary, install-time ConfState)` in a view — what a
/// `SnapshotInstalled` event feeds the oracle. The install-time ConfState (NOT any node's current config) is
/// the membership the oracle compares against the committed-config reference at the boundary.
fn with_install(
  mut view: ClusterView,
  id: u64,
  boundary: u64,
  install_conf: ConfSnapshot,
) -> ClusterView {
  view.new_installs.push((id, boundary, install_conf));
  view
}

/// Single-tick convenience: RECORD a view's observations, then run the run-end final pass and return its
/// verdict. Multi-tick tests instead call [`record_membership_observation`] per tick and
/// [`finalize_membership`] once, so the verdict is rendered against the FINAL stable history.
fn verdict(ck: &mut Checker, view: &ClusterView) -> Result<(), Violation> {
  record_membership_observation(ck, view);
  finalize_membership(ck)
}

/// Like [`cv`] but seeds this tick's committed conf-change transitions (for the membership oracle's
/// step-function reference) — `(conf-change index, term, ConfState)` triples, as a log-built node's
/// `ConfChanged` events would carry them (the term resolving same-index conflicts).
fn cv_t(
  seed: u64,
  tick: u64,
  transitions: Vec<(u64, u64, sailing_proto::ConfState<u64>)>,
  nodes: Vec<NodeView>,
) -> ClusterView {
  let mut view = cv(seed, tick, nodes);
  view.committed_transitions = transitions
    .iter()
    .map(|(idx, term, cs)| (*idx, *term, ConfSnapshot::from_conf_state(cs)))
    .collect();
  view
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
fn commit_is_quorum_durable_detects_nonquorum_volatile_commit() {
  // A volatile commit that is NOT quorum-durable must trip even though the lagging durable commit alone
  // looks fine. Node 0 has volatile commit=5 while its durable commit watermark is only 4, so it RETAINS
  // index 5 in its durable log WITHOUT having durably committed it. The oracle therefore does not witness
  // node 0's own term for index 5 (it did not choose it) and falls to the holder-quorum fallback, which
  // finds no term durably held by a voter quorum for index 5 (node 0 is the sole holder) → trip. The
  // volatile commit is what the node applies and serves, so checking only the durable commit (4) would
  // miss it.
  let mut n0 = healthy_node(0, 5, 5); // volatile commit=5, durably holds 5
  n0.hardstate_commit = 4; // ...but the durable commit watermark lags (unflushed)
  n0.applied = 4;
  let n1 = healthy_node(1, 4, 4);
  let n2 = healthy_node(2, 4, 4);
  let view = cv(3, 13, std::vec![n0, n1, n2]);
  let v = commit_is_quorum_durable(&view, 0).unwrap_err();
  assert_eq!(v.oracle, "commit_is_quorum_durable");
  assert!(
    v.detail.contains("no term holds a durable quorum"),
    "{}",
    v.detail
  );
}

#[test]
fn commit_is_quorum_durable_accepts_follower_ahead_with_compacted_witness() {
  // The follower-ahead case UNDER COMPACTION (a false positive a naive max-term fallback hits). Node 0's
  // volatile commit is 5 but it has not durably reached 5. The entry IS quorum-durable: node 1 RETAINS
  // (5, term 1) and node 2 COMPACTED past 5 with a higher boundary term (2) — it applied 5 before
  // snapshotting, so it holds the content (the boundary term is not the entry term). Deriving the witness
  // from the boundary term would mis-score node 1 and false-fire; the correct check counts the retained
  // term-1 holder + the compacted cover = 2 of 3, a quorum → no violation.
  let mut n0 = healthy_node(0, 5, 4); // volatile commit=5, durable only to 4 (self-undurable at 5)
  n0.hardstate_commit = 4; // the DURABLE commit watermark lags with the durable log (not corruption)
  n0.applied = 4;
  let n1 = healthy_node(1, 5, 5); // retains (5, term 1)
  let mut n2 = healthy_node(2, 5, 8); // snapshotted past 5: compacted with a higher boundary term
  n2.snapshot_last_index = 6;
  n2.snapshot_last_term = 2;
  n2.durable_first = 7;
  n2.durable_entries.retain(|e| e.index >= 7);
  let view = cv(3, 14, std::vec![n0, n1, n2]);
  assert_eq!(commit_is_quorum_durable(&view, 0), Ok(()));
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
    positional_agreement: true,
    seed: 4,
    tick: 336,
    committed_voters: Some(BTreeSet::from([0, 1, 2])),
    committed_transitions: Vec::new(),
    new_installs: Vec::new(),
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
    positional_agreement: true,
    seed: 4,
    tick: 1,
    committed_voters: Some(BTreeSet::from([0, 1, 2])),
    committed_transitions: Vec::new(),
    new_installs: Vec::new(),
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

/// A freshly fork-materialized child replica: the manufactured `(1, 1)` snapshot install —
/// `commit = 1` covered only by the snapshot boundary, an EMPTY re-baselined log
/// (`durable_first = 2`, `durable_last = 1`), and an empty applied record (the oracle-aligned
/// view strips the fork-inherited baseline cells, which carry parent-log indices).
fn fork_birth_node(id: u64) -> NodeView {
  let mut n = healthy_node(id, 1, 0);
  n.applied_log = Vec::new();
  n.durable_first = 2;
  n.durable_last = 1;
  n.visible_last = 1;
  n.snapshot_last_index = 1;
  n.snapshot_last_term = 1;
  n
}

/// Grow a fork-birth replica past the baseline: it committed, applied, and durably holds the
/// child's OWN first entry `(index 2, term 1)`.
fn fork_grown_node(id: u64) -> NodeView {
  let mut n = fork_birth_node(id);
  n.commit = 2;
  n.applied = 2;
  n.applied_log = std::vec![(2, std::vec![9])];
  n.durable_last = 2;
  n.visible_last = 2;
  n.durable_entries = std::vec![DurableEntry {
    index: 2,
    term: 1,
    data: std::vec![9],
    is_conf_change: false,
  }];
  n.hardstate_commit = 2;
  n
}

/// The fork-birth topology as [`ClusterView`]: the child's committed 4-voter boot config with
/// only the given replicas materialized (absent siblings have no store pair yet, so no view).
fn fork_view(nodes: Vec<NodeView>) -> ClusterView {
  ClusterView {
    positional_agreement: true,
    seed: 7,
    tick: 95,
    committed_voters: Some(BTreeSet::from([3, 4, 5, 6])),
    committed_transitions: Vec::new(),
    new_installs: Vec::new(),
    nodes,
  }
}

#[test]
fn fork_baseline_floor_admits_the_manufactured_install_at_birth() {
  // The fork-birth shape: the child's boot config is a committed 4-voter set, but only ONE
  // replica has materialized — commit=1 (the manufactured baseline) with a single durable
  // holder. An UNSEEDED checker rightly trips the full axiom on it, which is also the pin that
  // a normally-created group (whose registration inserts exactly `Checker::new()`) keeps the
  // full axiom from index 0.
  let view = fork_view(std::vec![fork_birth_node(6)]);
  let v = Checker::new().check(&view).unwrap_err();
  assert_eq!(v.oracle, "commit_is_quorum_durable");
  assert!(
    v.detail.contains("no term holds a durable quorum"),
    "{}",
    v.detail
  );

  // Seeded at the baseline — what fork registration does — the same view is admitted: the
  // baseline's durable witnesses are the PARENT's quorum, invisible to the child-scoped view.
  let mut ck = Checker::new();
  ck.register_fork_baseline(1);
  ck.check(&view)
    .expect("the manufactured baseline is parent-quorum-anchored");

  // And a later, genuinely child-quorum-durable commit above the baseline still passes: all
  // four replicas materialized and 3 of 4 durably hold the child's own entry at index 2.
  let mut nodes: Vec<NodeView> = std::vec![
    fork_grown_node(3),
    fork_grown_node(4),
    fork_grown_node(5),
    fork_birth_node(6),
  ];
  nodes[0].is_leader = true;
  ck.check(&fork_view(nodes))
    .expect("a child-quorum-durable commit above the baseline");
}

#[test]
fn fork_baseline_floor_keeps_teeth_above_the_baseline() {
  // The seeded floor exempts ONLY the baseline. A child whose commit advances past it without
  // child-quorum-durable coverage must still trip — here at the fork-birth topology itself:
  // the sole materialized replica solo-commits index 2 (1 durable copy vs the boot config's
  // quorum of 3).
  let mut ck = Checker::new();
  ck.register_fork_baseline(1);
  let v = ck
    .check(&fork_view(std::vec![fork_grown_node(6)]))
    .unwrap_err();
  assert_eq!(v.oracle, "commit_is_quorum_durable");
  assert!(
    v.detail.contains("only 1 of 4 voter durable logs"),
    "{}",
    v.detail
  );
}

#[test]
fn fork_baseline_floor_keeps_teeth_after_all_replicas_materialize() {
  // All four replicas materialized (each durably at the baseline), one solo-commits index 2.
  // The trip's "1 of 4" pins that the floor's denominator exclusion cannot shrink the child's
  // effective voter set: every materialized replica covers the baseline index by construction
  // (durable_last >= floor), so all four stay in the denominator and 1 < quorum(3) fires.
  let mut ck = Checker::new();
  ck.register_fork_baseline(1);
  let mut nodes: Vec<NodeView> = std::vec![
    fork_grown_node(3),
    fork_birth_node(4),
    fork_birth_node(5),
    fork_birth_node(6),
  ];
  nodes[0].is_leader = true;
  let v = ck.check(&fork_view(nodes)).unwrap_err();
  assert_eq!(v.oracle, "commit_is_quorum_durable");
  assert!(
    v.detail.contains("only 1 of 4 voter durable logs"),
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
fn snapshot_boundary_coherent_accepts_matching_boundary() {
  // Node 0 installed a snapshot at boundary (index 3, term 1); nodes 1 and 2 still RETAIN index 3 as a
  // durable log entry with term 1 (the `healthy_node` log is `(i, term=1)`). The witnessed committed
  // term matches the boundary term, so this must pass.
  let mut n0 = healthy_node(0, 5, 5);
  n0.snapshot_last_index = 3;
  n0.snapshot_last_term = 1; // matches the term every node's log records at index 3
  let view = cv(
    1,
    1,
    std::vec![n0, healthy_node(1, 5, 5), healthy_node(2, 5, 5)],
  );
  assert_eq!(snapshot_boundary_coherent(&view), Ok(()));
}

#[test]
fn snapshot_boundary_coherent_detects_term_mismatch() {
  // Node 0 installed a snapshot claiming boundary term 9 at index 3, but the committed log (nodes 1
  // and 2 still retain index 3 at term 1) says the committed term there is 1 — a corrupt/mis-keyed
  // snapshot boundary that must be caught.
  let mut n0 = healthy_node(0, 5, 5);
  n0.snapshot_last_index = 3;
  n0.snapshot_last_term = 9; // disagrees with the committed term (1) at index 3
  let view = cv(
    2,
    7,
    std::vec![n0, healthy_node(1, 5, 5), healthy_node(2, 5, 5)],
  );
  let v = snapshot_boundary_coherent(&view).unwrap_err();
  assert_eq!(v.oracle, "snapshot_boundary_coherent");
  assert!(
    v.detail.contains("boundary (last_index=3, last_term=9)")
      && v.detail.contains("records term 1"),
    "{}",
    v.detail
  );
}

#[test]
fn snapshot_boundary_coherent_skips_unwitnessable_boundary() {
  // EVERY node has compacted past index 3 (their durable logs start at 4), so no live node retains the
  // committed term at index 3. The boundary is then UNWITNESSABLE: even a boundary term that no log can
  // corroborate must be SKIPPED, never flagged — the soundness rule that keeps the oracle free of false
  // positives under frequent compaction.
  let mut nodes = Vec::new();
  for id in 0..3u64 {
    let mut n = healthy_node(id, 6, 8);
    n.snapshot_last_index = 3;
    n.snapshot_last_term = 9; // a term no retained log can witness — must NOT fire (unwitnessable)
    n.durable_first = 4;
    n.durable_entries.retain(|e| e.index >= 4);
    nodes.push(n);
  }
  let view = cv(3, 11, nodes);
  assert_eq!(
    snapshot_boundary_coherent(&view),
    Ok(()),
    "an index no live node retains in its durable log is unwitnessable and must be skipped, not flagged"
  );
}

#[test]
fn snapshot_boundary_coherent_ignores_uncommitted_tail_witness() {
  // The supersession-race regression: a snapshot boundary (index 4, term 2) is CORRECT — the committed
  // entry at index 4 was elected at term 2. A lagging node still durably holds a STALE UNCOMMITTED entry
  // at index 4 from the pre-supersede term 1 (its commit watermark is only 3, below index 4), which was
  // overwritten by the term-2 committed entry but not yet truncated. That node's durable term at 4 is 1,
  // one below the boundary's 2 — the exact term-off-by-one a superseding snapshot produces. The oracle
  // MUST NOT witness against that uncommitted tail (it is not the committed term); a node that has
  // actually COMMITTED index 4 (n2, commit=5) attests the true committed term 2, so the boundary passes.
  let mut n0 = healthy_node(0, 5, 5); // up-to-date: committed index 4 at term 2 ...
  for e in n0.durable_entries.iter_mut() {
    if e.index >= 4 {
      e.term = 2; // ... the committed term at index 4 is 2
    }
  }
  n0.snapshot_last_index = 4;
  n0.snapshot_last_term = 2; // a CORRECT boundary

  // A laggard durably holding the STALE term-1 entry at index 4, but with commit only 3 (uncommitted).
  let mut laggard = healthy_node(1, 3, 5); // commit=3 < 4; durable still has the old term-1 entry at 4
  laggard.snapshot_last_index = 0;

  // A node that HAS committed index 4 at the true term 2 — the sound witness.
  let mut n2 = healthy_node(2, 5, 5);
  for e in n2.durable_entries.iter_mut() {
    if e.index >= 4 {
      e.term = 2;
    }
  }

  let view = cv(14, 4151, std::vec![n0, laggard, n2]);
  assert_eq!(
    snapshot_boundary_coherent(&view),
    Ok(()),
    "a correct boundary must not be flagged against a laggard's UNCOMMITTED stale tail; only a committed \
     witness attests the committed term"
  );

  // And if NO node has committed index 4 (only the laggard's uncommitted term-1 tail retains it), the
  // index is unwitnessable-as-committed and is SKIPPED — never flagged against the uncommitted term.
  let mut snap_only = healthy_node(0, 3, 5); // commit=3 < 4, carries the boundary, no committed witness
  snap_only.snapshot_last_index = 4;
  snap_only.snapshot_last_term = 2;
  let mut lag2 = healthy_node(1, 3, 5); // commit=3 < 4, durable term-1 tail at 4
  lag2.snapshot_last_index = 0;
  let view2 = cv(14, 4152, std::vec![snap_only, lag2]);
  assert_eq!(
    snapshot_boundary_coherent(&view2),
    Ok(()),
    "with no committed witness for the index, an uncommitted tail must not be used as the reference"
  );
}

#[test]
fn snapshot_boundary_coherent_witnesses_against_own_retained_log() {
  // A node whose snapshot boundary index is still inside its OWN retained durable window is checked
  // against its own log entry at that index — a correctly-built snapshot's boundary term equals exactly
  // that entry's term, so it passes; a corrupted one (term bumped) trips even with a single node.
  let mut ok = healthy_node(0, 5, 5);
  ok.snapshot_last_index = 4;
  ok.snapshot_last_term = 1; // index 4 is retained at term 1 → coherent
  assert_eq!(snapshot_boundary_coherent(&cv(1, 1, std::vec![ok])), Ok(()));

  let mut bad = healthy_node(0, 5, 5);
  bad.snapshot_last_index = 4;
  bad.snapshot_last_term = 7; // its own retained log says term 1 at index 4 → incoherent
  let v = snapshot_boundary_coherent(&cv(1, 1, std::vec![bad])).unwrap_err();
  assert_eq!(v.oracle, "snapshot_boundary_coherent");
}

/// Build a log-built node (never transfer-installed — the sound witness), with an explicit active config.
fn log_node(id: u64, applied: u64, voters: &[u64], learners: &[u64]) -> NodeView {
  let mut n = healthy_node(id, applied, applied.max(1));
  n.installed_snapshot = false;
  n.conf_voters = voters.iter().copied().collect();
  n.conf_learners = learners.iter().copied().collect();
  n
}

/// Mark the witness's retained durable entry at `idx` as a committed ConfChange at `term` — modelling reality
/// (a conf-change index's committed entry IS a ConfChange at the conf-change's term), so `committed_log_kind`
/// carries the EXACT-term ConfChange proof the strict resolver requires to trust the recorded transition.
fn mark_cc(n: &mut NodeView, idx: u64, term: u64) {
  let e = n
    .durable_entries
    .iter_mut()
    .find(|e| e.index == idx)
    .expect("witness must retain a durable entry at the conf-change index");
  e.term = term;
  e.is_conf_change = true;
}

#[test]
fn snapshot_membership_coherent_accepts_matching_config() {
  // A node installed a snapshot embedding membership {0,1,2}; a log-built node (n1) records the committed
  // config {0,1,2} into the history (genesis), and the install matches it. The recorded comparison
  // (membership_comparisons == 1, skipped == 0) proves the oracle genuinely judged, not skipped.
  let mut ck = Checker::new();
  let w = log_node(1, 5, &[0, 1, 2], &[]);
  let view = with_install(cv(1, 1, std::vec![w]), 0, 5, conf(&[0, 1, 2], &[]));
  assert_eq!(verdict(&mut ck, &view), Ok(()));
  assert_eq!(ck.membership_comparisons(), 1);
  assert_eq!(ck.skipped_unwitnessed_installs(), 0);
}

#[test]
fn snapshot_membership_coherent_detects_phantom_voter() {
  // The snapshot installed at n0 still lists voter 3, but the committed config recorded by the log-built n1
  // at the boundary has removed it — a PHANTOM voter that must be caught.
  let mut ck = Checker::new();
  let w = log_node(1, 5, &[0, 1, 2], &[]);
  let view = with_install(cv(7, 9, std::vec![w]), 0, 5, conf(&[0, 1, 2, 3], &[]));
  let v = verdict(&mut ck, &view).unwrap_err();
  assert_eq!(v.oracle, "snapshot_membership_coherent");
  assert!(
    v.detail.contains("phantom voters {3}"),
    "expected phantom voter 3 in the detail: {}",
    v.detail
  );
}

#[test]
fn snapshot_membership_coherent_detects_missing_joiner() {
  // The snapshot installed at n0 is missing voter 2, but the committed config (recorded by n1) has already
  // added it — a MISSING joiner that must be caught.
  let mut ck = Checker::new();
  let w = log_node(1, 5, &[0, 1, 2], &[]);
  let view = with_install(cv(7, 9, std::vec![w]), 0, 5, conf(&[0, 1], &[]));
  let v = verdict(&mut ck, &view).unwrap_err();
  assert_eq!(v.oracle, "snapshot_membership_coherent");
  assert!(
    v.detail.contains("missing joiners {2}"),
    "expected missing joiner 2 in the detail: {}",
    v.detail
  );
}

#[test]
fn snapshot_membership_coherent_detects_learner_divergence() {
  // The voter halves agree but the installed snapshot dropped learner 9 the committed config carries —
  // a mis-keyed ConfState that the full-config comparison still catches.
  let mut ck = Checker::new();
  let w = log_node(1, 5, &[0, 1, 2], &[9]);
  let view = with_install(cv(1, 1, std::vec![w]), 0, 5, conf(&[0, 1, 2], &[]));
  let v = verdict(&mut ck, &view).unwrap_err();
  assert_eq!(v.oracle, "snapshot_membership_coherent");
}

#[test]
fn snapshot_membership_coherent_detects_learners_next_divergence() {
  // EVERY other ConfState field matches; only `learners_next` (the staged outgoing-voter demotions a joint
  // leave applies) diverges. A snapshot that corrupted just this field would leave a wrong demotion staged,
  // so the full-ConfState comparison must catch it — comparing only voters/outgoing/learners would not.
  let mut ck = Checker::new();
  let mut x = conf(&[0, 1, 2], &[]);
  x.voters_outgoing = [0u64, 1, 2, 3].into_iter().collect();
  x.learners_next = [3u64].into_iter().collect();
  let mut w = log_node(1, 5, &[0, 1, 2], &[]);
  w.conf_voters_outgoing = [0u64, 1, 2, 3].into_iter().collect();
  w.conf_learners_next = BTreeSet::new(); // the committed config stages NO demotion
  let view = with_install(cv(1, 1, std::vec![w]), 0, 5, x);
  let v = verdict(&mut ck, &view).unwrap_err();
  assert_eq!(v.oracle, "snapshot_membership_coherent");
  assert!(
    v.detail.contains("learners_next={3}"),
    "expected the diverging learners_next in the detail: {}",
    v.detail
  );
}

#[test]
fn snapshot_membership_coherent_detects_auto_leave_divergence() {
  // EVERY membership set matches; only the `auto_leave` flag (whether the leader auto-appends the
  // leave-joint entry) diverges. A snapshot that flipped just this bit would change joint-exit behaviour
  // while every set looks identical, so the full-ConfState comparison must catch it.
  let mut ck = Checker::new();
  let mut x = conf(&[0, 1, 2], &[]);
  x.voters_outgoing = [0u64, 1].into_iter().collect();
  x.auto_leave = true;
  let mut w = log_node(1, 5, &[0, 1, 2], &[]);
  w.conf_voters_outgoing = [0u64, 1].into_iter().collect();
  w.conf_auto_leave = false; // the committed config does NOT auto-leave
  let view = with_install(cv(1, 1, std::vec![w]), 0, 5, x);
  let v = verdict(&mut ck, &view).unwrap_err();
  assert_eq!(v.oracle, "snapshot_membership_coherent");
  assert!(
    v.detail.contains("auto_leave=true") && v.detail.contains("auto_leave=false"),
    "expected both auto_leave values in the detail: {}",
    v.detail
  );
}

#[test]
fn snapshot_membership_coherent_accepts_matching_joint_config() {
  // Mid joint transition: the log-built node records incoming {0,1,2} + outgoing {0,1}; the install carries
  // the same joint halves — a correctly-installed joint ConfState passes.
  let mut ck = Checker::new();
  let mut x = conf(&[0, 1, 2], &[]);
  x.voters_outgoing = [0u64, 1].into_iter().collect();
  let mut w = log_node(1, 5, &[0, 1, 2], &[]);
  w.conf_voters_outgoing = [0u64, 1].into_iter().collect();
  let view = with_install(cv(1, 1, std::vec![w]), 0, 5, x);
  assert_eq!(verdict(&mut ck, &view), Ok(()));
  assert_eq!(ck.membership_comparisons(), 1);

  // A divergent outgoing half (the recorded committed config stages {0,2}, the install {0,1}) trips —
  // a fresh checker so the history records the (correct) reference {0,2} before the install is checked.
  let mut ck2 = Checker::new();
  let mut x2 = conf(&[0, 1, 2], &[]);
  x2.voters_outgoing = [0u64, 1].into_iter().collect();
  let mut w2 = log_node(1, 5, &[0, 1, 2], &[]);
  w2.conf_voters_outgoing = [0u64, 2].into_iter().collect();
  let view2 = with_install(cv(1, 1, std::vec![w2]), 0, 5, x2);
  let v = verdict(&mut ck2, &view2).unwrap_err();
  assert_eq!(v.oracle, "snapshot_membership_coherent");
}

#[test]
fn snapshot_membership_coherent_skips_when_no_log_built_node() {
  // NO log-built node has ever been observed, so the completeness watermark is 0 and the reference is not
  // certified at any index. Each OBSERVED install is UNWITNESSABLE — counted (observed minus compared), never
  // silently flagged. The sweep's skipped_unwitnessed_installs == 0 assertion is what guards this in a run.
  let mut ck = Checker::new();
  let view = with_install(
    with_install(cv(2, 2, std::vec![]), 0, 5, conf(&[0, 1, 2, 3], &[])),
    1,
    5,
    conf(&[0, 1, 2], &[]),
  );
  assert_eq!(verdict(&mut ck, &view), Ok(()));
  assert_eq!(ck.membership_comparisons(), 0);
  assert_eq!(
    ck.skipped_unwitnessed_installs(),
    2,
    "both observed installs are beyond the (zero) completeness watermark — counted, never flagged"
  );
}

#[test]
fn snapshot_membership_coherent_skips_beyond_the_completeness_watermark() {
  // The only log-built node has reached applied 4, so the history is certified complete only up to 4. The
  // install's boundary is 5 — beyond the watermark, the reference is not certified (a later conf-change could
  // be unrecorded), so the OBSERVED install is counted (never flagged), even though configs differ.
  let mut ck = Checker::new();
  let w = log_node(1, 4, &[0, 1, 2], &[]); // certifies completeness only up to applied 4
  let view = with_install(cv(3, 3, std::vec![w]), 0, 5, conf(&[0, 1, 2, 3], &[]));
  assert_eq!(verdict(&mut ck, &view), Ok(()));
  assert_eq!(ck.membership_comparisons(), 0);
  assert_eq!(ck.skipped_unwitnessed_installs(), 1);
}

#[test]
fn snapshot_membership_coherent_uses_history_when_no_live_witness() {
  // The exhaustion case the history exists to solve: earlier in the run a log-built node recorded the true
  // committed config at applied 5; LATER every live node is snapshot-derived (no live log-built witness),
  // and one carries a corrupt ConfState. The PERSISTENT history still supplies the reference, so the oracle
  // CATCHES the divergence — proving the reference does not exhaust.
  let mut ck = Checker::new();
  // Tick A: a log-built node records the committed config {0,1,2} at applied 5.
  let truth = log_node(0, 5, &[0, 1, 2], &[]);
  assert_eq!(verdict(&mut ck, &cv(1, 1, std::vec![truth])), Ok(()));
  // Tick B: EVERY live node is now snapshot-derived (no log-built node in the view). Two installs carry a
  // phantom voter 3. The history (from tick A) must still catch it.
  let view = with_install(
    with_install(cv(2, 2, std::vec![]), 0, 5, conf(&[0, 1, 2, 3], &[])),
    1,
    5,
    conf(&[0, 1, 2, 3], &[]),
  );
  let v = verdict(&mut ck, &view).unwrap_err();
  assert_eq!(v.oracle, "snapshot_membership_coherent");
  assert!(
    v.detail.contains("phantom voters {3}"),
    "the persistent history must still flag the phantom voter with no live witness: {}",
    v.detail
  );
}

#[test]
fn snapshot_membership_coherent_steps_to_the_config_in_effect_at_an_index() {
  // The step function resolves the config in effect at an index BETWEEN conf-changes, regardless of how big
  // a batch the apply jumped (the reference is keyed by EXACT conf-change index, not per applied index). A
  // log-built node carries one recorded transition — config {0,1,2,3} took effect at index 3 — and has
  // applied to 9 (certifying completeness up to 9).
  let mut ck = Checker::new();
  let cs = sailing_proto::ConfState::from_voters([0u64, 1, 2, 3]);
  let mut a = log_node(0, 9, &[0, 1, 2, 3], &[]);
  a.conf_changed = 1;
  mark_cc(&mut a, 3, 1); // the committed entry at the conf-change index 3 is a ConfChange at term 1
  assert_eq!(
    verdict(&mut ck, &cv_t(1, 1, std::vec![(3, 1, cs)], std::vec![a])),
    Ok(())
  );
  // An install at boundary 6 (between the transition at 3 and applied 9) is checked against the config in
  // effect there ({0,1,2,3}, the greatest transition index <= 6); dropping voter 3 is a missing joiner.
  let view = with_install(cv(2, 2, std::vec![]), 1, 6, conf(&[0, 1, 2], &[]));
  let v = verdict(&mut ck, &view).unwrap_err();
  assert_eq!(v.oracle, "snapshot_membership_coherent");
  assert!(v.detail.contains("missing joiners {3}"), "{}", v.detail);
}

#[test]
fn snapshot_membership_coherent_distinct_boundaries_tracked_independently() {
  // The coverage metric is observed-minus-compared, keyed PER install identity (node, boundary) and PERSISTENT:
  // comparing one boundary on a node must NOT clear a different, still-uncompared boundary on the same node.
  let mut ck = Checker::new();
  // Tick A: no log-built node (watermark 0), so the high install boundary 9 is skipped — observed, uncompared.
  let view_a = with_install(cv(1, 1, std::vec![]), 0, 9, conf(&[0, 1, 2], &[]));
  assert_eq!(verdict(&mut ck, &view_a), Ok(()));
  assert_eq!(ck.skipped_unwitnessed_installs(), 1);
  // Tick B: a log-built node reaches applied 5 (watermark 5, genesis {0,1,2}); a SECOND install on the same
  // node at boundary 5 IS compared (matches). Boundary 9 is still beyond the watermark, so it STAYS counted —
  // comparing boundary 5 did not drop the distinct boundary-9 identity.
  let w = log_node(1, 5, &[0, 1, 2], &[]);
  let view_b = with_install(cv(2, 2, std::vec![w]), 0, 5, conf(&[0, 1, 2], &[]));
  assert_eq!(verdict(&mut ck, &view_b), Ok(()));
  assert!(ck.membership_comparisons() >= 1, "boundary 5 was compared");
  assert_eq!(
    ck.skipped_unwitnessed_installs(),
    1,
    "the still-uncompared boundary 9 stays counted after the distinct boundary 5 is compared"
  );
}

#[test]
fn snapshot_membership_coherent_marks_same_term_divergence_ambiguous_not_poison() {
  // Two log-built nodes report DIFFERENT folds at the SAME (index 4, term 2) — the ConfChanged ConfState is
  // the node's apply-time fold, which an async in-memory apply can transiently diverge. First-writer-win would
  // POISON the reference: an install whose conf happens to equal the first fold would be COMPARED and wrongly
  // PASS. Instead the index is AMBIGUOUS, so the install is SKIPPED (uncompared) — surfaced, never passed.
  let mut ck = Checker::new();
  let conf_a = sailing_proto::ConfState::from_voters([0u64, 1, 2]); // first fold at (4, 2)
  let conf_b = sailing_proto::ConfState::from_voters([0u64, 1, 2, 3]); // divergent fold at (4, 2) -> ambiguous
  let mut w = log_node(1, 9, &[0, 1, 2], &[]);
  w.conf_changed = 1; // a conf-change applied, so the reference at index 6 is the transition, not genesis
  mark_cc(&mut w, 4, 2); // the committed entry at index 4 is a ConfChange at term 2 (proves the kind)
  // The install's conf EQUALS the first fold (conf_a): poisoning would pass it; ambiguity skips it.
  let view = with_install(
    cv_t(
      1,
      1,
      std::vec![(4, 2, conf_a), (4, 2, conf_b)],
      std::vec![w],
    ),
    0,
    6,
    conf(&[0, 1, 2], &[]),
  );
  assert_eq!(
    verdict(&mut ck, &view),
    Ok(()),
    "an ambiguous index is not a hard failure"
  );
  assert_eq!(
    ck.membership_comparisons(),
    0,
    "an install resolving to an ambiguous index is NEVER compared (would be poison)"
  );
  assert_eq!(
    ck.skipped_unwitnessed_installs(),
    1,
    "the install at the ambiguous index stays uncompared (surfaced), not silently passed"
  );
}

#[test]
fn snapshot_membership_coherent_ambiguous_index_disambiguated_by_higher_term() {
  // An ambiguous (index, term) is CLEARED by a strictly-higher-term transition (the later term re-applied the
  // entry — the committed truth), after which the previously-skipped install is compared.
  let mut ck = Checker::new();
  let conf_a = sailing_proto::ConfState::from_voters([0u64, 1, 2]);
  let conf_b = sailing_proto::ConfState::from_voters([0u64, 1, 2, 3]); // divergent at (4, 2) -> ambiguous
  let mut w = log_node(1, 9, &[0, 1, 2], &[]);
  w.conf_changed = 1;
  mark_cc(&mut w, 4, 2); // the committed entry at index 4 is a ConfChange (term 2 here; superseded next tick)
  // Tick 1: index 4 ambiguous -> the install at boundary 6 (resolving to index 4) is observed but skipped. Its
  // install-time conf {0,1,2,3,4} matches the term-3 truth recorded next tick.
  let view1 = with_install(
    cv_t(
      1,
      1,
      std::vec![(4, 2, conf_a), (4, 2, conf_b)],
      std::vec![w],
    ),
    0,
    6,
    conf(&[0, 1, 2, 3, 4], &[]),
  );
  assert_eq!(verdict(&mut ck, &view1), Ok(()));
  assert_eq!(ck.membership_comparisons(), 0);
  assert_eq!(ck.skipped_unwitnessed_installs(), 1);
  // Tick 2: a term-3 transition at index 4 (the committed truth {0,1,2,3,4}) DISAMBIGUATES the history AND a
  // witness retains the term-3 committed ConfChange at index 4 (the exact-term proof strict trust requires); the
  // PERSISTENT install is now compared against that truth (and matches), so it is witnessed.
  let truth = sailing_proto::ConfState::from_voters([0u64, 1, 2, 3, 4]);
  let mut w2 = log_node(1, 9, &[0, 1, 2, 3, 4], &[]);
  w2.conf_changed = 1;
  mark_cc(&mut w2, 4, 3); // the disambiguating term-3 committed ConfChange at index 4
  assert_eq!(
    verdict(
      &mut ck,
      &cv_t(2, 2, std::vec![(4, 3, truth)], std::vec![w2])
    ),
    Ok(())
  );
  assert_eq!(
    ck.membership_comparisons(),
    1,
    "after a higher-term transition disambiguates the index, the install is compared"
  );
  assert_eq!(
    ck.skipped_unwitnessed_installs(),
    0,
    "the disambiguated install is now witnessed (observed minus compared falls to 0)"
  );
}

#[test]
fn snapshot_membership_coherent_catches_corrupt_install_after_current_config_repaired() {
  // The oracle compares the INSTALL-TIME ConfState (fixed at install), NEVER the node's CURRENT config. A
  // snapshot that installed a corrupt membership (phantom voter 3) at boundary 5 must be CAUGHT even after the
  // node's CURRENT config has been repaired to the committed truth by a later ConfChange — a corrupt install
  // does not earn a free pass because the live config later looks fine.
  let mut ck = Checker::new();
  // Tick A: observe the install at boundary 5 with its corrupt install-time conf {0,1,2,3}; no log-built node
  // yet (watermark 0), so it is NOT compared and stays counted.
  let view_a = with_install(cv(1, 1, std::vec![]), 0, 5, conf(&[0, 1, 2, 3], &[]));
  assert_eq!(verdict(&mut ck, &view_a), Ok(()));
  assert_eq!(ck.membership_comparisons(), 0);
  assert_eq!(
    ck.skipped_unwitnessed_installs(),
    1,
    "the corrupt install stays uncompared until the reference at its boundary is complete"
  );
  // Tick B: node 0 is now LOG-BUILT with a CORRECT current config {0,1,2} (the live config was repaired), and
  // it plus a peer certify completeness up to 9 with genesis {0,1,2}. The oracle compares the STORED corrupt
  // install conf {0,1,2,3} (not node 0's repaired current config), so it CATCHES the phantom voter.
  let repaired = log_node(0, 9, &[0, 1, 2], &[]);
  let w = log_node(1, 9, &[0, 1, 2], &[]);
  let v = verdict(&mut ck, &cv(2, 2, std::vec![repaired, w])).unwrap_err();
  assert_eq!(v.oracle, "snapshot_membership_coherent");
  assert!(
    v.detail.contains("phantom voters {3}") && v.detail.contains("boundary 5"),
    "the corrupt install-time conf must be caught despite the repaired current config: {}",
    v.detail
  );
}

#[test]
fn snapshot_membership_coherent_rejudges_install_against_overwritten_reference() {
  // The run-end final pass compares against the FINAL history, never a reference superseded later. A per-tick
  // verdict would bless an install the first time its reference resolved and freeze it; here index 4's reference
  // is FIRST (term 2, conf {0,1,2}) — which the install matches — then OVERWRITTEN by a strictly-higher (term 3,
  // conf {0,1,2,3}). The final pass must re-judge the install against the FINAL {0,1,2,3} and CATCH it, NOT bless
  // it against the stale term-2 value.
  let mut ck = Checker::new();
  let conf_a = sailing_proto::ConfState::from_voters([0u64, 1, 2]); // first reference at (4, 2)
  let conf_b = sailing_proto::ConfState::from_voters([0u64, 1, 2, 3]); // overwrites at (4, 3) — the truth
  let mut w = log_node(1, 9, &[0, 1, 2], &[]);
  w.conf_changed = 1; // the reference at boundary 6 is the transition, not genesis
  mark_cc(&mut w, 4, 2); // the committed entry at index 4 is a ConfChange (term 2 here; overwritten next tick)
  // Tick 1: index 4 = (term 2, {0,1,2}); the install at boundary 6 carries {0,1,2}, which MATCHES this
  // (non-final) reference — a per-tick verdict would compare and bless it here.
  let view1 = with_install(
    cv_t(1, 1, std::vec![(4, 2, conf_a)], std::vec![w]),
    0,
    6,
    conf(&[0, 1, 2], &[]),
  );
  assert_eq!(verdict(&mut ck, &view1), Ok(()));
  // Tick 2: a strictly-higher term-3 transition OVERWRITES index 4 with {0,1,2,3}, and a witness retains the
  // term-3 committed ConfChange at index 4 (the exact-term proof). The run-end final pass now re-judges the
  // install against this FINAL reference and CATCHES the missing joiner 3 — never frozen against the stale term-2.
  let mut w2 = log_node(1, 9, &[0, 1, 2, 3], &[]);
  w2.conf_changed = 1;
  mark_cc(&mut w2, 4, 3); // the overwriting term-3 committed ConfChange at index 4
  let v = verdict(
    &mut ck,
    &cv_t(2, 2, std::vec![(4, 3, conf_b)], std::vec![w2]),
  )
  .unwrap_err();
  assert_eq!(v.oracle, "snapshot_membership_coherent");
  assert!(
    v.detail.contains("missing joiners {3}"),
    "the install must be re-judged against the FINAL overwritten reference, not blessed against the stale one: {}",
    v.detail
  );
}

#[test]
fn snapshot_membership_coherent_skips_install_against_later_ambiguated_reference() {
  // The dual of the overwrite case: index 4's reference is FIRST a clean (term 2, {0,1,2}) the install matches,
  // then a same-term DIVERGENT fold AMBIGUATES it. A per-tick verdict would have blessed the install against the
  // clean value; the run-end final pass instead resolves to the now-ambiguous index and SKIPS the install
  // (counted unwitnessed) — never blessed against an untrusted reference.
  let mut ck = Checker::new();
  let conf_a = sailing_proto::ConfState::from_voters([0u64, 1, 2]); // clean reference at (4, 2)
  let conf_b = sailing_proto::ConfState::from_voters([0u64, 1, 2, 3]); // same-term divergent fold -> ambiguous
  let mut w = log_node(1, 9, &[0, 1, 2], &[]);
  w.conf_changed = 1;
  mark_cc(&mut w, 4, 2); // the committed entry at index 4 is a ConfChange at term 2 (proves the kind)
  // Tick 1: index 4 clean (term 2, {0,1,2}); the install at boundary 6 carries {0,1,2} and matches it.
  let view1 = with_install(
    cv_t(1, 1, std::vec![(4, 2, conf_a)], std::vec![w]),
    0,
    6,
    conf(&[0, 1, 2], &[]),
  );
  assert_eq!(verdict(&mut ck, &view1), Ok(()));
  assert_eq!(
    ck.membership_comparisons(),
    1,
    "the clean reference compares the install on tick 1"
  );
  // Tick 2: a same-(index 4, term 2) DIVERGENT fold ambiguates the index. The final pass resolves the install to
  // the now-ambiguous index and skips it — not blessed against the (no-longer-trusted) term-2 value.
  assert_eq!(
    verdict(&mut ck, &cv_t(2, 2, std::vec![(4, 2, conf_b)], std::vec![])),
    Ok(())
  );
  assert_eq!(
    ck.membership_comparisons(),
    0,
    "the ambiguated index is untrusted, so the final pass compares nothing"
  );
  assert_eq!(
    ck.skipped_unwitnessed_installs(),
    1,
    "the install resolving to the now-ambiguous index is skipped (surfaced), never blessed against the stale value"
  );
}

#[test]
fn snapshot_membership_coherent_tombstones_confchange_superseded_by_higher_term_normal() {
  // The authoritative-source edge: a ConfChange applied in-memory at (index 4, term 2) — recorded in the history
  // via its ConfChanged event — is truncated and SUPERSEDED by a higher-term (term 3) NORMAL entry committed at
  // the SAME index 4, which emits NO ConfChanged event. The committed LOG is authoritative: the config does NOT
  // change at index 4, so finalize_membership tombstones the stale transition and resolves boundary 5 to the
  // PRIOR (genesis) config.
  let conf_x = sailing_proto::ConfState::from_voters([0u64, 1, 2, 3]); // the stale, truncated ConfChange's conf
  // The log-built witness: genesis {0,1,2}, completeness up to 9, with the FINAL committed entry at index 4 a
  // term-3 NORMAL entry (is_conf_change = false) — the higher-term entry that superseded the truncated ConfChange.
  let superseding = DurableEntry {
    index: 4,
    term: 3,
    data: std::vec![0xEE],
    is_conf_change: false,
  };

  // CAUGHT: an install carrying the stale {0,1,2,3} (the truncated ConfChange) at boundary 5 — the tombstone
  // resolves boundary 5 to genesis {0,1,2}, so voter 3 is a phantom.
  let mut ck = Checker::new();
  let mut g = log_node(0, 9, &[0, 1, 2], &[]);
  g.durable_entries[3] = superseding.clone();
  let view = with_install(
    cv_t(1, 1, std::vec![(4, 2, conf_x.clone())], std::vec![g]),
    1,
    5,
    conf(&[0, 1, 2, 3], &[]),
  );
  let v = verdict(&mut ck, &view).unwrap_err();
  assert_eq!(v.oracle, "snapshot_membership_coherent");
  assert!(
    v.detail.contains("phantom voters {3}"),
    "the stale term-2 ConfChange must be tombstoned (config at 5 = genesis), catching voter 3: {}",
    v.detail
  );

  // PASSES (and IS compared): an install carrying the correct post-tombstone {0,1,2} at boundary 5.
  let mut ck2 = Checker::new();
  let mut g2 = log_node(0, 9, &[0, 1, 2], &[]);
  g2.durable_entries[3] = superseding;
  let view2 = with_install(
    cv_t(2, 2, std::vec![(4, 2, conf_x)], std::vec![g2]),
    1,
    5,
    conf(&[0, 1, 2], &[]),
  );
  assert_eq!(verdict(&mut ck2, &view2), Ok(()));
  assert_eq!(
    ck2.membership_comparisons(),
    1,
    "the post-tombstone config is COMPARED, not skipped (the tombstone removes the transition, not the boundary)"
  );
  assert_eq!(ck2.skipped_unwitnessed_installs(), 0);
}

#[test]
fn snapshot_membership_coherent_tombstone_survives_compaction_of_the_superseder() {
  // The committed-log-kind record is PERSISTENT: a higher-term non-ConfChange that superseded a ConfChange at
  // index 4 is captured the tick it is durable and KEPT even after compaction removes it from the retained log,
  // so the tombstone still fires at finalization and a snapshot carrying the stale ConfState is CAUGHT.
  let mut ck = Checker::new();
  // Tick A: the superseding term-3 NORMAL entry at index 4 is durable + committed -> committed_log_kind[4] =
  // (3, false) recorded persistently (genesis {0,1,2}, completeness to 9).
  let mut g = log_node(0, 9, &[0, 1, 2], &[]);
  g.durable_entries[3] = DurableEntry {
    index: 4,
    term: 3,
    data: std::vec![0xEE],
    is_conf_change: false,
  };
  record_membership_observation(
    &mut ck,
    &cv_t(
      1,
      1,
      std::vec![(4, 2, sailing_proto::ConfState::from_voters([0u64, 1, 2, 3]))],
      std::vec![g],
    ),
  );
  // Tick B: index 4 is COMPACTED (the retained committed log no longer holds it), and a node installs at
  // boundary 5 carrying the stale term-2 ConfChange config. The PERSISTENT kind still tombstones the transition,
  // so boundary 5 resolves to genesis {0,1,2} and voter 3 is caught as a phantom.
  let mut comp = log_node(0, 9, &[0, 1, 2], &[]);
  comp.durable_first = 6;
  comp.durable_entries.retain(|e| e.index >= 6);
  comp.snapshot_last_index = 5;
  comp.snapshot_last_term = 3;
  let view = with_install(cv(2, 2, std::vec![comp]), 1, 5, conf(&[0, 1, 2, 3], &[]));
  let v = verdict(&mut ck, &view).unwrap_err();
  assert_eq!(v.oracle, "snapshot_membership_coherent");
  assert!(
    v.detail.contains("phantom voters {3}"),
    "the persistently-recorded superseder kind must tombstone even after compaction: {}",
    v.detail
  );
}

#[test]
fn snapshot_membership_coherent_compacted_kind_at_committed_final_index_is_a_sound_decline() {
  // The soundness net: if the committed-log KIND at a recorded transition's index was compacted unobserved (no
  // standalone-log observation, no same-term snapshot boundary), the transition is NOT trusted — never a
  // fall-through to the stale apply-time ConfChange. When the index is committed-FINAL (covered by a durable
  // snapshot) but its kind is gone, the oracle SOUNDLY DECLINES (it cannot tell a genuine ConfChange from a
  // compacted-away superseder): a kind-unobservable decline, NOT a history-completeness gap.
  let mut ck = Checker::new();
  // A log-built node certifies completeness to 9, its log compacted past index 4 (durable snapshot boundary 5,
  // so index 4 is committed-final) but no snapshot boundary lands on index 4 — its kind is NEVER observed.
  let mut g = log_node(0, 9, &[0, 1, 2], &[]);
  g.durable_first = 6;
  g.durable_entries.retain(|e| e.index >= 6);
  g.snapshot_last_index = 5;
  g.snapshot_last_term = 1;
  // A ConfChange transition recorded at index 4 (from its event), but no committed_log_kind[4] and no boundary at
  // 4. An install carrying the stale config would look like a phantom — but with the kind unknown the oracle must
  // NOT judge it: it is a kind-unobservable decline (counted separately), never compared, never trusted-stale.
  let view = with_install(
    cv_t(
      1,
      1,
      std::vec![(4, 2, sailing_proto::ConfState::from_voters([0u64, 1, 2, 3]))],
      std::vec![g],
    ),
    1,
    5,
    conf(&[0, 1, 2, 3], &[]),
  );
  assert_eq!(verdict(&mut ck, &view), Ok(()));
  assert_eq!(ck.membership_comparisons(), 0);
  assert_eq!(
    ck.kind_unobservable_installs(),
    1,
    "a committed-final index whose kind was compacted is a sound decline, never trusted-stale"
  );
  assert_eq!(
    ck.skipped_unwitnessed_installs(),
    0,
    "a compacted KIND is not a committed-config-history completeness gap"
  );
}

#[test]
fn snapshot_membership_coherent_un_witnessed_snapshot_boundary_does_not_corroborate() {
  // STRICT trust: an UN-independently-witnessed snapshot boundary is NO proof. A ConfChange
  // compacted INTO a snapshot at its own index leaves no standalone log entry (committed_log_kind missing), and
  // the snapshot's boundary term is itself unwitnessed — NO committed retained log entry attests it, so
  // snapshot_boundary_coherent would skip it. The resolver must NOT let such a boundary self-corroborate the
  // missing kind; the install is a sound kind-unobservable decline, never trusted against the transient ConfChange.
  let conf_x = sailing_proto::ConfState::from_voters([0u64, 1, 2, 3]);
  let mut ck = Checker::new();
  // A log-built node compacted past index 4 (committed_log_kind[4] missing) whose durable snapshot boundary is
  // exactly index 4 — but no committed retained log entry anywhere witnesses index 4's term (it is compacted).
  let mut g = log_node(0, 9, &[0, 1, 2, 3], &[]);
  g.conf_changed = 1;
  g.durable_first = 5;
  g.durable_entries.retain(|e| e.index >= 5);
  g.snapshot_last_index = 4;
  g.snapshot_last_term = 2;
  let view = with_install(
    cv_t(1, 1, std::vec![(4, 2, conf_x)], std::vec![g]),
    1,
    5,
    conf(&[0, 1, 2, 3], &[]),
  );
  assert_eq!(verdict(&mut ck, &view), Ok(()));
  assert_eq!(
    ck.membership_comparisons(),
    0,
    "an un-witnessed snapshot boundary is no proof, so nothing is compared via it"
  );
  assert_eq!(
    ck.kind_unobservable_installs(),
    1,
    "the un-witnessed boundary does NOT corroborate — a sound decline, never trusted-stale"
  );
  assert_eq!(ck.skipped_unwitnessed_installs(), 0);
}

#[test]
fn snapshot_membership_coherent_stale_lower_term_record_is_not_trusted() {
  // STRICT trust: a committed-log record at the resolving index with a LOWER term than the
  // recorded ConfChange is stale (the higher-term ConfChange superseded it) and is NOT exact-term proof — the
  // transition must NOT be trusted via it. The install is a sound kind-unobservable decline.
  let mut ck = Checker::new();
  let conf_x = sailing_proto::ConfState::from_voters([0u64, 1, 2, 3]);
  // The witness retains index 4 as a term-1 entry (committed_log_kind[4] = (1, ...)), but the history records a
  // ConfChange at (4, term 2) — a strictly higher term. The lower-term record is no proof of the term-2 entry.
  let mut w = log_node(1, 9, &[0, 1, 2], &[]);
  w.conf_changed = 1;
  let view = with_install(
    cv_t(1, 1, std::vec![(4, 2, conf_x)], std::vec![w]),
    1,
    6,
    conf(&[0, 1, 2, 3], &[]),
  );
  assert_eq!(verdict(&mut ck, &view), Ok(()));
  assert_eq!(ck.membership_comparisons(), 0);
  assert_eq!(
    ck.kind_unobservable_installs(),
    1,
    "a stale lower-term record is not exact-term proof — the ConfChange is not trusted via it"
  );
  assert_eq!(ck.skipped_unwitnessed_installs(), 0);
}

#[test]
fn snapshot_membership_coherent_same_term_non_confchange_tombstones_not_trusts() {
  // STRICT trust: a committed-log record at the resolving index that is a non-ConfChange at the
  // SAME term as the recorded ConfChange is the committed entry there (committed entries are immutable, so the
  // recorded ConfChange transition is stale) — the config does NOT change at that index ⇒ TOMBSTONE (walk past),
  // NEVER trust the stale ConfChange. The install is then compared against the PRIOR (genesis) config.
  let mut ck = Checker::new();
  let conf_x = sailing_proto::ConfState::from_voters([0u64, 1, 2, 3]);
  // A genesis witness (founding config {0,1,2}), and a witness whose committed entry at index 4 is a term-2
  // NON-ConfChange (same term as the recorded ConfChange transition), so committed_log_kind[4] = (2, non-CC).
  let genesis = log_node(0, 9, &[0, 1, 2], &[]); // conf_changed == 0 ⇒ genesis {0,1,2}
  let mut w = log_node(1, 9, &[0, 1, 2], &[]);
  w.conf_changed = 1;
  w.durable_entries[3].term = 2; // index 4 is a term-2 entry, left a NON-ConfChange (is_conf_change == false)
  // The install at boundary 6 carries the stale ConfChange config {0,1,2,3}; the same-term non-ConfChange
  // tombstones the transition, so the reference is genesis {0,1,2} and the install's extra voter 3 is caught.
  let view = with_install(
    cv_t(1, 1, std::vec![(4, 2, conf_x)], std::vec![genesis, w]),
    2,
    6,
    conf(&[0, 1, 2, 3], &[]),
  );
  let v = verdict(&mut ck, &view).unwrap_err();
  assert_eq!(v.oracle, "snapshot_membership_coherent");
  assert!(
    v.detail.contains("phantom voters {3}"),
    "a same-term non-ConfChange tombstones the transition (config unchanged), so the stale install is caught \
     against genesis, never trusted against the stale ConfChange: {}",
    v.detail
  );
}

#[test]
fn snapshot_membership_coherent_same_term_kind_conflict_is_order_independent_decline() {
  // STRICT trust (R11): a same-term committed-log kind conflict is order-INDEPENDENT — here the exact-term
  // ConfChange kind is recorded FIRST, then a same-term NON-ConfChange proof arrives LATER (the reverse of the
  // tombstone regression). The committed entry at a given (index, term) is unique, so a same-term kind conflict
  // means one observation is a transient/buggy artifact ⇒ the record becomes CONFLICTED and the transition is
  // DECLINED: never trusted as a real ConfChange (no false-negative), never tombstoned as a non-ConfChange (no
  // false-positive).
  let mut ck = Checker::new();
  let conf_x = sailing_proto::ConfState::from_voters([0u64, 1, 2, 3]);
  // Tick 1: a witness whose committed entry at index 4 is a term-2 ConfChange — committed_log_kind[4] recorded
  // FIRST as (2, ConfChange), with the matching ConfChange transition.
  let mut w1 = log_node(1, 9, &[0, 1, 2, 3], &[]);
  w1.conf_changed = 1;
  mark_cc(&mut w1, 4, 2);
  record_membership_observation(
    &mut ck,
    &cv_t(1, 1, std::vec![(4, 2, conf_x)], std::vec![w1]),
  );
  // Tick 2: a LATER witness proves the committed entry at (index 4, term 2) is a NON-ConfChange — a same-term
  // kind conflict that must mark index 4 CONFLICTED despite the ConfChange being recorded first.
  let mut w2 = log_node(2, 9, &[0, 1, 2], &[]);
  w2.conf_changed = 1;
  w2.durable_entries[3].term = 2; // index 4 is a term-2 NON-ConfChange (is_conf_change stays false)
  let view = with_install(cv(2, 2, std::vec![w2]), 0, 6, conf(&[0, 1, 2, 3], &[]));
  assert_eq!(verdict(&mut ck, &view), Ok(()));
  assert_eq!(
    ck.membership_comparisons(),
    0,
    "a same-term kind conflict (ConfChange recorded first, non-ConfChange later) is CONFLICTED — never trusted"
  );
  assert_eq!(
    ck.kind_unobservable_installs(),
    1,
    "the conflicted index is a sound decline, independent of observation order"
  );
  assert_eq!(ck.skipped_unwitnessed_installs(), 0);
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

/// The absorb→observers-only→retire shape (seed-2 class): a group absorbs via a forced snapshot,
/// shrinks to observers through a conf-change its snapshot-installed replicas emit no
/// `ConfChanged` for, then retires. The observers-only install lands beyond the completeness
/// watermark against a history that never recorded its config, so the frozen archive counts it
/// unwitnessed forever. `certify_retiring_history` — the one last exact-term walk of the retiring
/// durable logs at archival — records the folded config and raises the watermark through the
/// boundary, so the install is JUDGED (compared), not silently skipped.
#[test]
fn certify_retiring_history_witnesses_the_absorb_observers_retire_install() {
  let mut ck = Checker::new();
  // Pre-absorb: log-built voters {0,1,2}, genesis captured, complete_up_to raised to 5.
  let pre: Vec<NodeView> = (0..3)
    .map(|id| {
      let mut n = healthy_node(id, 5, 5);
      n.conf_voters = [0, 1, 2].into_iter().collect();
      n
    })
    .collect();
  record_membership_observation(&mut ck, &cv(2, 1, pre));

  // The observers-only install: a snapshot lands at boundary 8 with the shrunk config {0}. Its
  // removing conf-change at index 8 was applied only by snapshot-installed replicas, so the
  // event-sourced history never recorded it, and the boundary sits beyond complete_up_to (5).
  let install_view = with_install(cv(2, 2, std::vec![]), 0, 8, conf(&[0], &[]));
  record_membership_observation(&mut ck, &install_view);
  finalize_membership(&mut ck).unwrap();
  assert_eq!(
    ck.skipped_unwitnessed_installs(),
    1,
    "the install lands beyond the completeness watermark against an unrecorded config"
  );

  // Retire: the retiring replicas' durable logs are committed through 8, carrying the
  // observers-only conf-change at index 8, their folded conf_state() = {0}. The world records the
  // committed-log kind, then the retire walk records the config and raises the watermark.
  let retiring: Vec<NodeView> = (0..3)
    .map(|id| {
      let mut n = healthy_node(id, 8, 8);
      n.durable_entries[7].is_conf_change = true; // index 8: the observers-only change
      n.conf_voters = [0].into_iter().collect();
      n.conf_changed = 1;
      n.installed_snapshot = true; // post-absorb: emits no ConfChanged, so record raises nothing
      n
    })
    .collect();
  let retire_view = cv(2, 3, retiring);
  record_membership_observation(&mut ck, &retire_view);
  certify_retiring_history(&mut ck, &retire_view);

  finalize_membership(&mut ck).unwrap();
  assert_eq!(
    ck.skipped_unwitnessed_installs(),
    0,
    "the retire walk certified the history through the boundary"
  );
  assert!(
    ck.membership_comparisons() >= 1,
    "the install was JUDGED against the folded config {{0}}, not merely un-counted"
  );
}

/// Zero-tolerance stays: the retire walk certifies ONLY the gap-free prefix. An install beyond the
/// retiring replicas' durable committed extent has no proof, so it remains unwitnessed — the walk
/// never blanket-blesses, it extends completeness exactly as far as the durable logs prove.
#[test]
fn certify_retiring_history_leaves_a_genuine_gap_unwitnessed() {
  let mut ck = Checker::new();
  let pre: Vec<NodeView> = (0..3)
    .map(|id| {
      let mut n = healthy_node(id, 5, 5);
      n.conf_voters = [0, 1, 2].into_iter().collect();
      n
    })
    .collect();
  record_membership_observation(&mut ck, &cv(7, 1, pre));

  // An install at boundary 30 — far beyond anything the retiring replicas commit.
  let install_view = with_install(cv(7, 2, std::vec![]), 0, 30, conf(&[0], &[]));
  record_membership_observation(&mut ck, &install_view);

  // The retiring replicas commit only through 8: the walk can prove no more.
  let retiring: Vec<NodeView> = (0..3)
    .map(|id| {
      let mut n = healthy_node(id, 8, 8);
      n.conf_voters = [0].into_iter().collect();
      n.installed_snapshot = true;
      n
    })
    .collect();
  let retire_view = cv(7, 3, retiring);
  record_membership_observation(&mut ck, &retire_view);
  certify_retiring_history(&mut ck, &retire_view);

  finalize_membership(&mut ck).unwrap();
  assert_eq!(
    ck.skipped_unwitnessed_installs(),
    1,
    "an install beyond the durable committed extent stays unwitnessed — no false witness"
  );
}

/// Encode a single-change conf-change entry payload for the derivation tests.
fn encode_cc(
  transition: sailing_proto::ConfChangeTransition,
  ty: sailing_proto::ConfChangeType,
  node: u64,
) -> Vec<u8> {
  let cc = sailing_proto::ConfChangeV2::new(
    transition,
    std::vec![sailing_proto::ConfChangeSingle::new(ty, node)],
    bytes::Bytes::new(),
  );
  let mut buf = Vec::new();
  sailing_proto::encode_conf_change_v2(&cc, &mut buf);
  buf
}

/// Set up the fork-born retire shape whose committed conf-change at index 6 the event-sourced history
/// NEVER recorded: genesis {0,1,2}; the install lineage commits `RemoveNode 2 -> {0,1}` at index 6 but
/// emits no `ConfChanged` (snapshot-derived), so `committed_log_kind[6]` is a ConfChange yet its config
/// is unrecorded and the frontier stays at 5; an install lands at boundary 6 with `install_conf`. The
/// retire view is the one lineage-free replica, FROZEN at commit 5 (one short of index 6, so its
/// `conf_snapshot` records nothing there) but still carrying the uncommitted conf-change entry — the
/// sole witness of that config's CONTENT. Returns the checker and the retire view (not yet certified).
fn fork_born_retire_shape(cc6: Vec<u8>, install_conf: ConfSnapshot) -> (Checker, ClusterView) {
  let mut ck = Checker::new();
  let pre: Vec<NodeView> = (0..3)
    .map(|id| {
      let mut n = healthy_node(id, 5, 5);
      n.conf_voters = [0, 1, 2].into_iter().collect();
      n
    })
    .collect();
  record_membership_observation(&mut ck, &cv(0, 1, pre));

  let committed6: Vec<NodeView> = (0..3)
    .map(|id| {
      let mut n = healthy_node(id, 6, 6);
      n.durable_entries[5] = DurableEntry {
        index: 6,
        term: 1,
        data: cc6.clone(),
        is_conf_change: true,
      };
      n.conf_voters = [0, 1].into_iter().collect();
      n.installed_snapshot = true; // snapshot-derived: emits no ConfChanged, never raises the frontier
      n
    })
    .collect();
  record_membership_observation(&mut ck, &cv(0, 2, committed6));

  let install_view = with_install(cv(0, 3, std::vec![]), 0, 6, install_conf);
  record_membership_observation(&mut ck, &install_view);

  let mut frozen = healthy_node(0, 5, 6); // commit 5, durable log reaches the uncommitted index 6
  frozen.durable_entries[5] = DurableEntry {
    index: 6,
    term: 1,
    data: cc6,
    is_conf_change: true,
  };
  frozen.conf_voters = [0, 1, 2].into_iter().collect();
  let retire_view = cv(0, 4, std::vec![frozen]);
  record_membership_observation(&mut ck, &retire_view);
  (ck, retire_view)
}

/// The seed-10 g259 survivor, unit-isolated: the walk must DERIVE the config of a committed
/// conf-change no live replica ever recorded a ConfState for — decoding it from a hosting replica's
/// durable log and folding it. Seeded directly (bypassing `record_membership_observation`, whose own
/// per-tick call would derive it immediately): before the walk index 6's config is absent; after, it
/// is the fold `{0,1,2}` −RemoveNode(2)→ `{0,1}`.
#[test]
fn the_walk_derives_an_unrecorded_committed_conf_change() {
  let mut ck = Checker::new();
  ck.genesis_conf = Some(conf(&[0, 1, 2], &[]));
  ck.complete_up_to = 5;
  ck.committed_log_kind
    .insert(6, (1, CommittedKind::ConfChange));
  let mut frozen = healthy_node(0, 5, 6); // commit 5, durable log carries the uncommitted index 6
  frozen.durable_entries[5] = DurableEntry {
    index: 6,
    term: 1,
    data: encode_cc(
      sailing_proto::ConfChangeTransition::Auto,
      sailing_proto::ConfChangeType::RemoveNode,
      2,
    ),
    is_conf_change: true,
  };
  let view = cv(0, 1, std::vec![frozen]);

  assert!(
    !ck.committed_config_history.contains_key(&6),
    "index 6's config is unrecorded before the walk"
  );
  derive_unrecorded_conf_changes(&mut ck, &view);
  assert_eq!(
    ck.committed_config_history.get(&6).map(|(_, c, _)| c
      .voters
      .iter()
      .copied()
      .collect::<Vec<u64>>()),
    Some(std::vec![0, 1]),
    "the walk decoded RemoveNode 2 and folded genesis {{0,1,2}} -> {{0,1}}"
  );
}

/// End to end: with the config derived PER TICK, an install past the (once-unrecorded) conf-change is
/// WITNESSED against the derived reference at the run-end pass — not skipped, not merely declined.
#[test]
fn a_derived_config_witnesses_the_install() {
  let cc6 = encode_cc(
    sailing_proto::ConfChangeTransition::Auto,
    sailing_proto::ConfChangeType::RemoveNode,
    2,
  );
  let (mut ck, _view) = fork_born_retire_shape(cc6, conf(&[0, 1], &[]));
  finalize_membership(&mut ck).unwrap();
  assert_eq!(
    ck.skipped_unwitnessed_installs(),
    0,
    "the per-tick-derived config witnessed the install"
  );
  assert!(
    ck.membership_comparisons() >= 1,
    "the install was JUDGED against the derived config {{0,1}}, not merely declined"
  );
}

/// Teeth: the derived config is a real REFERENCE, not a blanket bless — an install whose membership
/// does NOT match it still trips the divergence arm. Here the install adopted {0,2} where the derived
/// committed config at the boundary is {0,1}: node 2 is a phantom voter, node 1 a missing joiner.
#[test]
fn a_derived_config_still_trips_a_divergent_install() {
  let cc6 = encode_cc(
    sailing_proto::ConfChangeTransition::Auto,
    sailing_proto::ConfChangeType::RemoveNode,
    2,
  );
  let (mut ck, retire_view) = fork_born_retire_shape(cc6, conf(&[0, 2], &[]));
  certify_retiring_history(&mut ck, &retire_view);
  let v = finalize_membership(&mut ck).unwrap_err();
  assert_eq!(v.oracle, "snapshot_membership_coherent");
  assert!(
    v.detail.contains("phantom voters {2}") && v.detail.contains("missing joiners {1}"),
    "{}",
    v.detail
  );
}

/// Conservatism: a gap conf-change the pure simple fold CANNOT derive (here a JOINT / non-`Auto`
/// change) is NEVER guessed — the walk leaves its config unrecorded. The install past it is then a
/// sound KIND-UNOBSERVABLE decline: its snapshot boundary proves committed, but the config reference
/// is unprovable, so the accounting TOLERATES it (never a false witness, never a hard skip).
#[test]
fn an_undecodable_gap_change_declines_the_install() {
  let cc6 = encode_cc(
    sailing_proto::ConfChangeTransition::Implicit, // joint entry — not a simple fold
    sailing_proto::ConfChangeType::RemoveNode,
    2,
  );
  let (mut ck, _view) = fork_born_retire_shape(cc6, conf(&[0, 1], &[]));
  finalize_membership(&mut ck).unwrap();
  assert_eq!(
    ck.membership_comparisons(),
    0,
    "the joint config was never guessed — the install is not witnessed"
  );
  assert_eq!(
    ck.kind_unobservable_installs(),
    1,
    "the boundary is bridged but its config reference is unprovable — a sound decline"
  );
  assert_eq!(
    ck.skipped_unwitnessed_installs(),
    0,
    "a bridged, unprovable boundary is a tolerated decline, not a hard completeness gap"
  );
}

/// A LIVE all-snapshot-derived group (the merge-target shape, group 110): its log-built witnesses
/// all departed before applying far, freezing `complete_up_to` below the durable-committed extent,
/// so an install whose boundary is committed-final but past the applied frontier was silently
/// skipped and the run-end accounting tripped though the config CONVERGED. `finalize_membership`
/// now runs the gap-free committed-prefix walk itself (retirement is not the only caller), raising
/// the frontier across the committed extent so the install is WITNESSED against the config there.
#[test]
fn finalize_witnesses_a_live_all_snapshot_install_within_the_committed_extent() {
  let mut ck = Checker::new();
  // An early log-built voter set genesis {0,1,2} and the committed-config prefix, but its APPLIED
  // frontier froze at 5 (it departed / became snapshot-derived before applying further). Its durable
  // COMMITTED log still reached 15, so the per-tick recorder persisted committed_log_kind gap-free
  // 1..=15 (all Normal). `complete_up_to` stays 5.
  let mut early = healthy_node(0, 15, 15);
  early.applied = 5;
  early.conf_voters = [0, 1, 2].into_iter().collect();
  record_membership_observation(&mut ck, &cv(6, 1, std::vec![early]));

  // The install: a snapshot lands at boundary 10 embedding the genesis {0,1,2} — beyond the applied
  // watermark (5) but WITHIN the gap-free committed extent (15).
  let install_view = with_install(cv(6, 2, std::vec![]), 1, 10, conf(&[0, 1, 2], &[]));
  record_membership_observation(&mut ck, &install_view);

  finalize_membership(&mut ck).unwrap();
  assert_eq!(
    ck.skipped_unwitnessed_installs(),
    0,
    "the live-finalize walk certifies the committed prefix through the boundary — witnessed, not skipped"
  );
  assert!(
    ck.membership_comparisons() >= 1,
    "the install is JUDGED against the genesis {{0,1,2}}, not merely un-counted"
  );
}

/// Zero-tolerance stays for a live group too: the live walk certifies ONLY the gap-free committed
/// prefix. An install one index PAST the durable-committed extent has no proof, so it remains
/// unwitnessed — the frontier stops at the extent, never blanket-widens.
#[test]
fn finalize_leaves_a_live_boundary_past_the_committed_extent_unwitnessed() {
  let mut ck = Checker::new();
  let mut early = healthy_node(0, 15, 15);
  early.applied = 5;
  early.conf_voters = [0, 1, 2].into_iter().collect();
  record_membership_observation(&mut ck, &cv(6, 1, std::vec![early]));

  // Boundary 16 — one past the gap-free committed extent (15). The walk proves no more than 15.
  let install_view = with_install(cv(6, 2, std::vec![]), 1, 16, conf(&[0, 1, 2], &[]));
  record_membership_observation(&mut ck, &install_view);

  finalize_membership(&mut ck).unwrap();
  assert_eq!(
    ck.skipped_unwitnessed_installs(),
    1,
    "a boundary one past the durable committed extent stays unwitnessed — the frontier stops at the extent"
  );
}

/// The widened frontier must not paper over a real corruption: an install WITHIN the newly-certified
/// committed extent whose ConfState DIVERGES from the config in effect there still trips the verdict.
#[test]
fn finalize_still_trips_a_divergent_live_install_within_the_committed_extent() {
  let mut ck = Checker::new();
  let mut early = healthy_node(0, 15, 15);
  early.applied = 5;
  early.conf_voters = [0, 1, 2].into_iter().collect();
  record_membership_observation(&mut ck, &cv(6, 1, std::vec![early]));

  // The install at boundary 10 (witnessed by the widened frontier) carries a phantom voter 3 the
  // committed genesis {0,1,2} never had — a genuine divergence the walk must still catch.
  let install_view = with_install(cv(6, 2, std::vec![]), 1, 10, conf(&[0, 1, 2, 3], &[]));
  record_membership_observation(&mut ck, &install_view);

  let v = finalize_membership(&mut ck).unwrap_err();
  assert_eq!(v.oracle, "snapshot_membership_coherent");
  assert!(v.detail.contains("phantom voters {3}"), "{}", v.detail);
}
