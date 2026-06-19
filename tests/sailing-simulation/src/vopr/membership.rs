use super::*;

/// Reconcile the VOPR's tracked membership from the cluster's REAL committed state.
///
/// When a leader exists (so `committed_voters()` is authoritative), set `voters`/`learners` to the
/// leader's committed config MINUS the VOPR's `gone` set (a node the VOPR has `mark_removed`'d is
/// isolated and must not re-enter the working set even while a RemoveNode for it is still committing).
/// When there is no leader, KEEP the last-known sets (don't thrash on a transient election).
///
/// Then ABANDON orphans: a wired joiner that is SETTLED (`!conf_in_flight`) and still absent from the
/// committed membership for [`ORPHAN_GRACE_PASSES`] consecutive passes had its AddNode/AddLearner
/// accepted-but-never-committed — `mark_removed` it so it cannot pin `min_applied_len`/quiesce or
/// receive phantom isolation. A wired joiner that DID become a committed member is dropped from
/// `wired` (its streak reset).
///
/// Deterministic: every input (`committed_voters`/`committed_learners`, `leader`) is a pure function
/// of the cluster state, and the orphan grace is driven by a per-node pass counter — no wall-clock /
/// `rand` / map-iteration-order influence.
pub(crate) fn reconcile_membership(c: &mut Cluster, st: &mut VoprState) {
  if c.leader().is_some() {
    let voters = c.committed_voters();
    let learners = c.committed_learners();
    // Regression recovery: a `gone` node the CURRENT leader's committed view STILL lists must rejoin the
    // network. The leader needs that node's vote/ack to make progress, but the harness had isolated it
    // because the leader is a post-restart/partition laggard whose APPLIED config regressed (rebuilt
    // from a stale durable commit, or never learned the removal committed). We trust ONLY the leader's
    // view here, never a non-leader laggard's. Re-admitted as a FULL voter (NOT `removing`) so it
    // counts toward progress and the leader replicates it back into sync — dumping survivors into
    // `removing` emptied the metric.
    let resurrect: Vec<u64> = st
      .gone
      .iter()
      .copied()
      .filter(|g| voters.contains(g))
      .collect();
    for g in resurrect {
      c.reinstate(g);
      st.gone.remove(&g);
    }
    // A victim with an in-flight RemoveNode is isolated only ONCE it has left the leader's committed
    // voter set (its removal committed on the quorum the leader sees). If a later laggard-leader
    // regresses to need it, the resurrect above re-arms it — so isolating on the leader's view is safe.
    let committed_removed: Vec<u64> = st
      .removing
      .iter()
      .copied()
      .filter(|v| !voters.contains(v))
      .collect();
    for v in committed_removed {
      c.mark_removed(v);
      st.gone.insert(v);
      st.removing.remove(&v);
      st.down.remove(&v);
    }
    st.voters = voters.difference(&st.gone).copied().collect();
    st.learners = learners.difference(&st.gone).copied().collect();
  }
  // A VOPR-isolated node that is no longer a voter (e.g. its RemoveNode committed) should leave
  // `down` so it stops being counted — `voters_down` already filters by `voters`, but pruning keeps
  // `down` from growing without bound across a long run.
  st.down
    .retain(|id| st.voters.contains(id) || st.learners.contains(id));

  // Orphan sweep over the wired joiners (sorted order — deterministic).
  let committed_member = |id: &u64| st.voters.contains(id) || st.learners.contains(id);
  let mut abandon: Vec<u64> = Vec::new();
  let mut promoted: Vec<u64> = Vec::new();
  for id in st.wired.iter().copied() {
    if committed_member(&id) {
      promoted.push(id); // its change committed — no longer a pending joiner
      continue;
    }
    if st.conf_in_flight {
      // A change is still in flight; the joiner may yet commit. Reset its streak.
      st.missing_streak.insert(id, 0);
      continue;
    }
    let streak = st.missing_streak.entry(id).or_insert(0);
    *streak += 1;
    if *streak >= ORPHAN_GRACE_PASSES {
      abandon.push(id);
    }
  }
  for id in promoted {
    st.wired.remove(&id);
    st.missing_streak.remove(&id);
  }
  for id in abandon {
    c.mark_removed(id); // accepted-but-never-committed AddNode/AddLearner — abandon the orphan
    st.gone.insert(id);
    st.wired.remove(&id);
    st.missing_streak.remove(&id);
    st.down.remove(&id);
  }
}

/// Refresh the VOPR's one-conf-change-in-flight flag: a proposed change is considered settled once
/// the cluster's `total_conf_changed` advances past the baseline captured at proposal time (the
/// change committed and was applied somewhere) OR there is currently no leader to carry it (we
/// conservatively clear so a re-election does not deadlock conf-changes — the next proposal re-gates
/// on the proto's own `pending_conf_index`, and we issue it via the non-panicking path).
pub(crate) fn refresh_conf_in_flight(c: &Cluster, st: &mut VoprState) {
  if !st.conf_in_flight {
    return;
  }
  if c.total_conf_changed() > st.conf_change_baseline {
    st.conf_in_flight = false;
  }
}

/// Whether every SETTLED voter (committed, not mid-removal) has applied EXACTLY as many entries as
/// the most-advanced one — i.e. the cluster is fully caught up, not merely prefix-consistent. The
/// precondition for the quiesce apply-everywhere equality check. A voter whose removal is in flight is
/// excluded (it is departing and may legitimately lag — see [`VoprState::settled_voters`]).
pub(crate) fn voters_fully_caught_up(c: &Cluster, st: &VoprState) -> bool {
  // The quiesce convergence loop calls `reconcile_membership` every pass, which sets `st.voters` to the
  // leader's COMMITTED voter set minus `gone` (an AddNode's joiner moves out of `wired` into `st.voters`;
  // a committed RemoveNode's victim is dropped; a `gone` node the leader still lists is resurrected). So
  // by the time this check runs, `settled_voters` (= `st.voters` − in-flight removals) is already the
  // correct set to wait for — it includes a newly-committed add and excludes both a committed and an
  // in-flight removal, with no stale-tracking gap.
  let lens: Vec<usize> = st
    .settled_voters()
    .iter()
    .map(|&id| c.applied_len_of(id))
    .collect();
  match (lens.iter().min(), lens.iter().max()) {
    (Some(lo), Some(hi)) => lo == hi,
    _ => true, // no voters (shouldn't happen) → vacuously caught up
  }
}

/// Restart (crash → recover-from-durable) every currently-POISONED live node. A poisoned node is
/// inert forever (its `handle_*` are no-ops) — the proto's deliberate response to an unrecoverable
/// storage read error. Since the VOPR injects `transient_read` faults that trigger exactly that, a
/// poisoned voter is effectively "down" and must be brought back before any liveness assertion. A
/// `crash` resets the poison flag and rebuilds the node from its durable log (the lost apply tail is
/// re-synced from the leader). Called inside the calm window / quiesce AFTER faults are cleared so a
/// restarted node cannot immediately re-poison. Iterates a deterministic (sorted) id order.
pub(crate) fn restart_poisoned(c: &mut Cluster, st: &VoprState, report: &mut VoprReport) {
  for id in st.voters.iter().chain(st.learners.iter()).copied() {
    if c.is_poisoned(id) {
      c.crash(id);
      report.restarts += 1;
    }
  }
}
