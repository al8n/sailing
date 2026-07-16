//! The multi-VOPR action bodies and seeded pick helpers — the single-group shapes lifted
//! group-scoped, plus the lifecycle verbs and the `(link, group)` mute budget.

use super::*;

/// Draw a weighted action from `profile` (deterministic from the master PRNG).
pub(super) fn pick_action(prng: &mut FaultPrng, profile: MultiProfile) -> MultiAction {
  let total: u32 = profile.weights.iter().map(|(_, w)| w).sum();
  let mut pick = (prng.next_u64() % u64::from(total)) as u32;
  for (action, weight) in profile.weights {
    if pick < *weight {
      return *action;
    }
    pick -= *weight;
  }
  MultiAction::ClientLoad // unreachable: the loop always returns
}

/// The `k`-th element (sorted order) of a non-empty slice/set draw, seeded. `None` when empty.
pub(super) fn pick_from(set: &BTreeSet<u64>, prng: &mut FaultPrng) -> Option<u64> {
  if set.is_empty() {
    return None;
  }
  let k = (prng.next_u64() % set.len() as u64) as usize;
  set.iter().nth(k).copied()
}

/// A seed-picked LIVE group, or `None` when none exist (cannot happen — the remove action keeps
/// at least one live group).
pub(super) fn pick_live_group(w: &MultiWorld, prng: &mut FaultPrng) -> Option<u64> {
  let live: BTreeSet<u64> = w.live_groups().into_iter().collect();
  pick_from(&live, prng)
}

/// Whether `gid` keeps a VIABLE quorum under hypothetical extra faults: some candidate leader
/// reaches (bidirectionally, per the group's mutes plus `extra_mute`) a strict majority of the
/// group's committed voters, none of which is isolated (current set plus `extra_isolated`).
pub(super) fn group_has_viable_quorum(
  w: &MultiWorld,
  gid: u64,
  extra_isolated: Option<u64>,
  extra_mute: Option<(u64, u64)>,
) -> bool {
  let voters = w.group_voters(gid);
  if voters.is_empty() {
    return true; // pre-reconcile bootstrap: nothing to protect yet
  }
  let isolated = w.isolated_nodes();
  let down = |n: u64| -> bool { isolated.contains(&n) || extra_isolated.is_some_and(|x| x == n) };
  let muted = |a: u64, b: u64| -> bool {
    w.muted_edges().contains(&(a, b, gid))
      || w.muted_edges().contains(&(b, a, gid))
      || extra_mute.is_some_and(|(f, t)| (f == a && t == b) || (f == b && t == a))
  };
  for &leader in &voters {
    if down(leader) {
      continue;
    }
    let reachable = voters
      .iter()
      .filter(|&&v| v == leader || (!down(v) && !muted(leader, v)))
      .count();
    if reachable * 2 > voters.len() {
      return true;
    }
  }
  false
}

/// Propose 1..=4 gid-tagged keyed-value commands on a live group's leader, keyed from the
/// group's LIVE population (the full domain until a split moves a slice to a child; identical
/// picks to the pre-population form while full).
pub(super) fn client_load(
  w: &mut MultiWorld,
  st: &mut MState,
  prng: &mut FaultPrng,
  report: &mut MultiVoprReport,
) {
  let Some(gid) = pick_live_group(w, prng) else {
    return;
  };
  let k = 1 + (prng.next_u64() % 4) as usize;
  let keys = w.group_keys_of(gid);
  if keys.is_empty() {
    return; // every key handed away — nothing keyed to write here
  }
  for _ in 0..k {
    let key = keys[(st.cmd_counter % keys.len() as u64) as usize];
    let payload = encode_gkv(gid, key, st.cmd_counter);
    if w.propose(gid, &payload).is_some() {
      st.expected.entry(gid).or_default().push(payload);
      st.cmd_counter += 1;
      report.proposals += 1;
    } else {
      break; // momentarily leaderless — a later iteration retries
    }
  }
}

/// Issue 1..=3 linearizable reads on a live group: two thirds leader-direct, one third on
/// another hosting member (the forward path; refusals are legitimate no-ops under faults).
pub(super) fn read_index_load(
  w: &mut MultiWorld,
  reads: &mut MultiReadLedger,
  prng: &mut FaultPrng,
  report: &mut MultiVoprReport,
) {
  let Some(gid) = pick_live_group(w, prng) else {
    return;
  };
  let k = 1 + (prng.next_u64() % 3) as usize;
  for _ in 0..k {
    let leader = w.leader_of(gid);
    let forward = prng.next_u64().is_multiple_of(3);
    let target = if forward {
      let others: BTreeSet<u64> = w
        .hosting_nodes(gid)
        .into_iter()
        .filter(|n| Some(*n) != leader)
        .collect();
      pick_from(&others, prng).or(leader)
    } else {
      leader
    };
    let Some(target) = target else { return };
    let keys = w.group_keys_of(gid);
    if keys.is_empty() {
      return; // every key handed away — no keyed floor to read
    }
    let key = keys[(prng.next_u64() % keys.len() as u64) as usize];
    reads.issue(w, gid, target, key, report);
  }
}

/// Isolate a node — legal only if EVERY live group keeps a viable quorum with it down (the
/// multi generalization of the single-group fault budget).
pub(super) fn partition(w: &mut MultiWorld, prng: &mut FaultPrng, report: &mut MultiVoprReport) {
  let isolated = w.isolated_nodes();
  let candidates: BTreeSet<u64> = w
    .node_list()
    .into_iter()
    .filter(|n| !isolated.contains(n))
    .collect();
  let Some(victim) = pick_from(&candidates, prng) else {
    return;
  };
  if !w
    .live_groups()
    .into_iter()
    .all(|gid| group_has_viable_quorum(w, gid, Some(victim), None))
  {
    return; // would strand some group's quorum — skip
  }
  w.isolate(victim);
  report.partitions += 1;
}

/// Heal one currently isolated node.
pub(super) fn heal_one(w: &mut MultiWorld, prng: &mut FaultPrng, report: &mut MultiVoprReport) {
  if let Some(node) = pick_from(&w.isolated_nodes(), prng) {
    w.heal(node);
    report.heals += 1;
  }
}

/// Crash a seed-picked node (a point event: it restores from durable state immediately, so it
/// never counts against the sustained budget).
///
/// Under an async store mode HALF the crashes (seeded) are armed to land MID-SETTLE on the next
/// tick — after a delivery submitted fresh appends but before the store flush — so they roll back a
/// genuine in-flight fsync window, while the other half stay between-tick (durable-window) crashes:
/// the "both in-flight and durable windows" the brief's crash suite needs. The mid-fsync draw is
/// gated on the async mode so the sync profiles neither draw the extra PRNG nor change behavior.
pub(super) fn crash_one(w: &mut MultiWorld, prng: &mut FaultPrng, report: &mut MultiVoprReport) {
  let nodes: BTreeSet<u64> = w.node_list().into_iter().collect();
  if let Some(victim) = pick_from(&nodes, prng) {
    if w.is_async_stores() && prng.next_u64().is_multiple_of(2) {
      w.arm_mid_fsync_crash(victim);
    } else {
      w.crash(victim);
    }
    report.crashes += 1;
  }
}

/// Mute one directed `(link, group)` edge between two of a live group's hosting nodes, budgeted
/// per group like Partition: the group must keep a viable quorum under its mutes.
pub(super) fn mute_group(w: &mut MultiWorld, prng: &mut FaultPrng, report: &mut MultiVoprReport) {
  let Some(gid) = pick_live_group(w, prng) else {
    return;
  };
  let hosting: Vec<u64> = w.hosting_nodes(gid);
  if hosting.len() < 2 {
    return;
  }
  let from = hosting[(prng.next_u64() % hosting.len() as u64) as usize];
  let to = hosting[(prng.next_u64() % hosting.len() as u64) as usize];
  if from == to || w.muted_edges().contains(&(from, to, gid)) {
    return;
  }
  if !group_has_viable_quorum(w, gid, None, Some((from, to))) {
    return; // the mute would strand the group's quorum — skip
  }
  w.mute_group(from, to, gid);
  report.mutes += 1;
}

/// Unmute one currently muted `(link, group)` edge (seed-picked from the sorted table).
pub(super) fn unmute_group(w: &mut MultiWorld, prng: &mut FaultPrng, report: &mut MultiVoprReport) {
  let edges: Vec<(u64, u64, u64)> = w.muted_edges().iter().copied().collect();
  if edges.is_empty() {
    return;
  }
  let (from, to, gid) = edges[(prng.next_u64() % edges.len() as u64) as usize];
  w.unmute_group(from, to, gid);
  report.unmutes += 1;
}

/// Propose a seed-chosen conf-change on one live group (one in flight PER GROUP): grow with a
/// fresh observer-wired member (voter or learner) or shrink by a viable RemoveNode.
pub(super) fn conf_change(w: &mut MultiWorld, prng: &mut FaultPrng, report: &mut MultiVoprReport) {
  let Some(gid) = pick_live_group(w, prng) else {
    return;
  };
  if w.conf_in_flight_of(gid) || w.leader_of(gid).is_none() {
    return;
  }
  let voters = w.group_voters(gid);
  let learners = w.group_learners(gid);
  let leader = w.leader_of(gid).expect("checked above");
  let joinable: BTreeSet<u64> = w
    .node_list()
    .into_iter()
    .filter(|n| !w.hosts_group(*n, gid))
    .collect();
  let removable: BTreeSet<u64> = if voters.len() > MIN_VOTERS {
    let isolated = w.isolated_nodes();
    voters
      .iter()
      .filter(|&&id| id != leader)
      .filter(|&&id| {
        // The post-removal voter set must keep a reachable majority among non-isolated nodes.
        let remaining: BTreeSet<u64> = voters.iter().copied().filter(|&v| v != id).collect();
        let up = remaining.iter().filter(|v| !isolated.contains(v)).count();
        up * 2 > remaining.len()
      })
      .copied()
      .collect()
  } else {
    BTreeSet::new()
  };

  let roll = prng.next_u64() % 3;
  let did = match roll {
    0 | 1 if !joinable.is_empty() => {
      let node = pick_from(&joinable, prng).expect("non-empty");
      if w.merge_choreography_active(gid) {
        // A merge participant must not grow mid-choreography: a virgin observer wired onto a
        // frozen source or a parked target strands behind the held apply and WORLD-PARKS for the
        // window (the lifecycle-churn discipline, extended from the remove/recreate draws to the
        // grow draw). The node was still drawn off the prng, so non-merge profiles — which never
        // see the predicate true — keep byte-identical schedules.
        false
      } else {
        let ty = if roll == 0 {
          sailing_proto::ConfChangeType::AddNode
        } else {
          sailing_proto::ConfChangeType::AddLearnerNode
        };
        w.wire_group_observer(gid, node);
        let cc = sailing_proto::ConfChange::new(ty, node, bytes::Bytes::new());
        if w.propose_conf_change(gid, cc).is_some() {
          true
        } else {
          // Refused (a racing step-down / in-flight gate): abandon the orphan replica at once so
          // it can never pin the group's quiesce.
          w.abandon_wired(gid, node);
          false
        }
      }
    }
    _ => match pick_from(&removable, prng) {
      Some(victim) => {
        if w.merge_choreography_active(gid) {
          // A merge participant must not SHRINK mid-choreography (the lifecycle-churn discipline,
          // extended from the grow draw to the remove draw): a RemoveNode on a frozen source or a
          // claimed/parked target drops a voter the choreography still counts on — the victim
          // self-removes-and-parks, and a frozen source pushed below quorum goes leaderless
          // forever (the freeze wedge). The victim was still drawn off the prng, so non-merge
          // profiles — which never see the predicate true — keep byte-identical schedules.
          false
        } else {
          let cc = sailing_proto::ConfChange::new(
            sailing_proto::ConfChangeType::RemoveNode,
            victim,
            bytes::Bytes::new(),
          );
          w.propose_conf_change(gid, cc).is_some()
        }
      }
      None => false,
    },
  };
  if did {
    report.conf_changes += 1;
  }
  let _ = learners;
}

/// Ask one group's leader to transfer leadership to a seed-picked other voter.
pub(super) fn transfer_leader(
  w: &mut MultiWorld,
  prng: &mut FaultPrng,
  report: &mut MultiVoprReport,
) {
  let Some(gid) = pick_live_group(w, prng) else {
    return;
  };
  let Some(leader) = w.leader_of(gid) else {
    return;
  };
  let others: BTreeSet<u64> = w
    .group_voters(gid)
    .into_iter()
    .filter(|&v| v != leader)
    .collect();
  let Some(target) = pick_from(&others, prng) else {
    return;
  };
  if w.transfer_group_leader(gid, target) {
    report.transfers += 1;
  }
}

/// Propose a read-mode migration on one live group. The plain multi config carries no lease
/// knobs, so LeaseBased/LeaseGuard targets are legitimately refused (`None`); the Safe target
/// re-affirms and exercises the apply-time flip machinery. Every draw happens regardless, so
/// the stream stays deterministic.
pub(super) fn migrate_read_mode(
  w: &mut MultiWorld,
  prng: &mut FaultPrng,
  report: &mut MultiVoprReport,
) {
  let Some(gid) = pick_live_group(w, prng) else {
    return;
  };
  let mode = match prng.next_u64() % 3 {
    0 => sailing_proto::ReadOnlyOption::Safe,
    1 => sailing_proto::ReadOnlyOption::LeaseBased,
    _ => sailing_proto::ReadOnlyOption::LeaseGuard,
  };
  if w.propose_read_mode_change(gid, mode).is_some() {
    report.read_mode_migrations += 1;
  }
}

/// The most LIVE groups the fuzzer will grow to. Creation past the cap no-ops: an unbounded set
/// makes long runs quadratic (every tick drives every group) without adding schedule diversity —
/// the fuzzer's value is interleaving depth over a bounded working set, as in production hosts.
const MAX_LIVE_GROUPS: usize = 8;

/// Create a fresh group: a monotone gid, a 3..=5-node seed-chosen member subset. No-ops at the
/// [`MAX_LIVE_GROUPS`] cap.
pub(super) fn create_group_action(
  w: &mut MultiWorld,
  st: &mut MState,
  prng: &mut FaultPrng,
  report: &mut MultiVoprReport,
) {
  let nodes = w.node_list();
  if nodes.len() < 3 || w.live_groups().len() >= MAX_LIVE_GROUPS {
    return;
  }
  let want = 3 + (prng.next_u64() % 3) as usize; // 3..=5 members
  let want = want.min(nodes.len());
  let mut pool: BTreeSet<u64> = nodes.into_iter().collect();
  let mut members = BTreeSet::new();
  for _ in 0..want {
    let Some(pick) = pick_from(&pool, prng) else {
      break;
    };
    pool.remove(&pick);
    members.insert(pick);
  }
  let gid = st.next_gid;
  st.next_gid += 1;
  w.create_group(gid, &members);
  report.groups_created += 1;
}

/// Retire a seed-picked live group — never the last live one (the world must keep committing),
/// and never an unresolved merge-choreography participant: the embedder contract keeps a
/// choreography's groups in place until it resolves (this action plays the embedder, exactly as
/// it plays the catalog's tombstone refusal at recreation). Removing a frozen/claimed source
/// orphans every parked commit naming it — the parked replicas are log-complete, so the
/// absent-source arm's snapshot route (built for log-behind stragglers) never fires and the
/// park stands forever; removing a mid-commit target strands its frozen source with no abort
/// relay left to thaw it. The exclusion narrows only merge-profile draws: without merge
/// actions no group ever has choreography state, so other profiles' pick sequences are
/// untouched.
pub(super) fn remove_group_action(
  w: &mut MultiWorld,
  prng: &mut FaultPrng,
  report: &mut MultiVoprReport,
) {
  let live: BTreeSet<u64> = w
    .live_groups()
    .into_iter()
    .filter(|g| !w.merge_choreography_active(*g))
    .collect();
  if live.len() <= 1 {
    return;
  }
  let Some(gid) = pick_from(&live, prng) else {
    return;
  };
  // A refusal that survives the world's superset filter (an append-pending residual after a merge
  // globally resolved) abandons the draw as a retryable no-op; the pick already burned the prng, so
  // the schedule stays deterministic and only a committed teardown counts.
  if w.remove_group(gid) {
    report.groups_removed += 1;
  }
}

/// Propose a SPLIT of a seed-picked live group: mint a fresh child id, pick a split point
/// strictly inside the group's live key population (both sides keep at least one key), and
/// call the world's real `propose_split`. Every landed refusal arm — `NotLeader`,
/// `SplitInFlight`, `JointConfig`, `ChildExists`, `InvalidChild`, a coordinator-tier
/// `SplitReserved`/`BelowFloor`, or a `Propose` passthrough — is a legitimate no-op this tick:
/// the draw stays deterministic and a later iteration retries. The minted id burns
/// unconditionally (single-incarnation ids are never re-offered, accepted or not).
pub(super) fn split_group(w: &mut MultiWorld, st: &mut MState, prng: &mut FaultPrng) {
  let Some(gid) = pick_live_group(w, prng) else {
    return;
  };
  if w.live_groups().len() >= MAX_LIVE_GROUPS {
    return; // the child would push the working set past the cap
  }
  let keys = w.group_keys_of(gid);
  if keys.len() < 2 {
    return; // an interior split point needs at least two live keys
  }
  let j = 1 + (prng.next_u64() % (keys.len() as u64 - 1)) as usize;
  let point = keys[j];
  let child = st.next_gid;
  st.next_gid += 1;
  if let Some(Ok(_)) = w.propose_split(gid, child, point) {
    // The child's integrity expectation: every accepted parent payload for a moved key is
    // reachable in the child's applied record through the fork baseline (filtered by KEY, not
    // tag — a re-split moves a grandparent's cells onward too).
    let moved: Vec<Vec<u8>> = st
      .expected
      .get(&gid)
      .map(|v| {
        v.iter()
          .filter(|p| decode_gkv(p).is_some_and(|(_, k, _)| k >= point))
          .cloned()
          .collect()
      })
      .unwrap_or_default();
    if !moved.is_empty() {
      st.expected.entry(child).or_default().extend(moved);
    }
  }
}

/// Prune the pending-merge book down to entries still awaiting an OBSERVED resolution: the
/// source must still be live (an absorbed source left via merge registration; a rolled-back
/// source that later retired left via lifecycle), and the world must not have drained a
/// `MergeAborted` for the pair past its booking mark. Retiring on the OBSERVATION — never on a
/// rollback's propose-accept — keeps an accepted-but-uncommitted abort's pair drivable at
/// quiesce instead of silently forgotten while its source is still frozen.
pub(super) fn prune_merge_book(w: &MultiWorld, st: &mut MState) {
  st.pending_merges.retain(|s, pm| {
    w.live_groups().contains(s) && w.merge_abort_observations(pm.target, *s) <= pm.abort_mark
  });
}

/// Propose a merge FREEZE of a seed-picked live source into an equal-voter-set live sibling —
/// the harness pairing filter (split children are colocated by construction, so the reshape
/// churn keeps minting mergeable pairs). Every landed refusal arm is a legitimate no-op tick;
/// an accepted freeze enters the pending book the commit/rollback actions draw from.
pub(super) fn prepare_merge_action(
  w: &mut MultiWorld,
  st: &mut MState,
  prng: &mut FaultPrng,
  report: &mut MultiVoprReport,
) {
  prune_merge_book(w, st);
  let live: Vec<u64> = w.live_groups();
  // ORIENT every pair down the id order: the direction rule refuses a claim unless the source's
  // canonical `Data` encoding sorts strictly ABOVE the target's, so pairing arbitrarily would let
  // half of every draw refuse `DirectionInverted` and halve merge coverage. `u64`'s `Data` encoding
  // is `to_le_bytes`, so this compares the exact byte strings the container does. Keeping only the
  // oriented direction of each unordered pair (source encodes above target) covers the same
  // mergeable pairs, one canonical orientation each.
  let candidates: Vec<(u64, u64)> = live
    .iter()
    .flat_map(|&a| live.iter().map(move |&b| (a, b)))
    .filter(|&(source, target)| source.to_le_bytes() > target.to_le_bytes())
    .filter(|&(source, _)| !st.pending_merges.contains_key(&source))
    .filter(|&(source, target)| {
      let sv = w.group_voters(source);
      !sv.is_empty() && sv == w.group_voters(target)
    })
    .collect();
  if candidates.is_empty() {
    return;
  }
  let (source, target) = candidates[(prng.next_u64() % candidates.len() as u64) as usize];
  if let Some(Ok(_)) = w.propose_prepare_merge(source, target) {
    st.pending_merges.insert(
      source,
      PendingMerge {
        target,
        saw_frozen: false,
        abort_mark: w.merge_abort_observations(target, source),
      },
    );
    report.merges_prepared += 1;
  }
}

/// Propose the merge ABSORB for a seed-picked pending freeze. Refusals — leader churn, the
/// local source still catching up to frozen-applied, an earlier commit already parked — no-op;
/// a later draw retries. On acceptance the source's accepted history folds into the target's
/// expected set (the absorb carries it; expected is a superset bound, so folding early is
/// sound even if a rollback later wins the race).
pub(super) fn commit_merge_action(
  w: &mut MultiWorld,
  st: &mut MState,
  prng: &mut FaultPrng,
  report: &mut MultiVoprReport,
) {
  prune_merge_book(w, st);
  let sources: Vec<u64> = st.pending_merges.keys().copied().collect();
  if sources.is_empty() {
    return;
  }
  let source = sources[(prng.next_u64() % sources.len() as u64) as usize];
  let target = st.pending_merges[&source].target;
  if !w.live_groups().contains(&target) {
    // A dead target with a live booked source is the STRANDED class — keep the pair booked so
    // the quiesce teeth can attribute it; dropping it here erased the evidence.
    return;
  }
  if let Some(Ok(_)) = w.propose_commit_merge(target, source) {
    let moved: Vec<Vec<u8>> = st.expected.get(&source).cloned().unwrap_or_default();
    if !moved.is_empty() {
      st.expected.entry(target).or_default().extend(moved);
    }
    report.merges_committed += 1;
  }
}

/// Propose the merge ABORT for a seed-picked pending freeze — deliberately able to RACE an
/// already-accepted commit: the source's log settles the race, and the parked applies must all
/// take the same side (the union verdict and the absorb-determinism check judge the outcome).
/// The book entry is NOT retired on the accept: an abort that never commits leaves the source
/// frozen, so the pair stays booked until the world OBSERVES a resolution (a `MergeAborted`
/// drained past the booking mark, or the source absorbed away) — see [`prune_merge_book`].
pub(super) fn rollback_merge_action(
  w: &mut MultiWorld,
  st: &mut MState,
  prng: &mut FaultPrng,
  report: &mut MultiVoprReport,
) {
  prune_merge_book(w, st);
  let sources: Vec<u64> = st.pending_merges.keys().copied().collect();
  if sources.is_empty() {
    return;
  }
  let source = sources[(prng.next_u64() % sources.len() as u64) as usize];
  let target = st.pending_merges[&source].target;
  if !w.live_groups().contains(&target) {
    // Keep the stranded pair booked for the quiesce teeth (see `commit_merge_action`).
    return;
  }
  if let Some(Ok(_)) = w.propose_rollback_merge(target, source) {
    report.merges_rolled_back += 1;
  }
}

/// Recreate a seed-picked retired gid as the SAME logical group at gen+1.
pub(super) fn recreate_group_action(
  w: &mut MultiWorld,
  prng: &mut FaultPrng,
  report: &mut MultiVoprReport,
) {
  if w.live_groups().len() >= MAX_LIVE_GROUPS {
    return; // recreation also counts against the bounded working set
  }
  // A merged-away gid is fenced by its terminal floor — the catalog (this action) never
  // re-offers it; the world verb enforces the same refusal by panic (harness contract). A gid
  // some live park still NAMES as its source is equally off the menu: recreating it as a
  // fresh incarnation would plant a log that can never satisfy the park's freeze identity —
  // the parked host would wait on it forever, mistaking the newborn for the absorbed.
  let retired: BTreeSet<u64> = w
    .retired_groups()
    .into_iter()
    .filter(|g| !w.is_merged(*g) && !w.merge_choreography_active(*g))
    .collect();
  let Some(gid) = pick_from(&retired, prng) else {
    return;
  };
  w.recreate_group(gid);
  report.groups_recreated += 1;
}

/// Roll a bounded adversarial [`crate::NetworkFaults`] intensity (latency/jitter well under the
/// 100 ms heartbeat; bounded loss the healthy majority re-replicates through).
pub(super) fn roll_network_faults(prng: &mut FaultPrng) -> crate::NetworkFaults {
  let latency = core::time::Duration::from_millis(prng.next_u64() % 9);
  let jitter = core::time::Duration::from_millis(prng.next_u64() % 31);
  let drop_per_mille = (prng.next_u64() % 181) as u32;
  let duplicate_per_mille = (prng.next_u64() % 121) as u32;
  let reorder = prng.next_u64().is_multiple_of(2);
  crate::NetworkFaults {
    latency,
    jitter,
    drop_per_mille,
    duplicate_per_mille,
    reorder,
  }
}

/// Re-roll every live replica's storage-fault intensity (low transient-read / torn-write rates,
/// as the single-group roll bounds them).
pub(super) fn reroll_storage(w: &mut MultiWorld, prng: &mut FaultPrng, seed: u64) {
  for gid in w.live_groups() {
    for node in w.hosting_nodes(gid) {
      let transient_read_per_mille = (prng.next_u64() % 7) as u16;
      let torn_write_per_mille = (prng.next_u64() % 11) as u16;
      let faults = crate::StorageFaults {
        transient_read_per_mille,
        torn_write_per_mille,
        ..crate::StorageFaults::none()
      };
      w.set_replica_faults(
        node,
        gid,
        faults,
        seed.wrapping_add(node).rotate_left(11) ^ gid,
      );
    }
  }
}

/// Clear every live replica's storage faults (the calm-window/quiesce back-off).
pub(super) fn clear_storage_faults(w: &mut MultiWorld, seed: u64) {
  for gid in w.live_groups() {
    for node in w.hosting_nodes(gid) {
      w.set_replica_faults(
        node,
        gid,
        crate::StorageFaults::none(),
        seed.wrapping_add(node),
      );
    }
  }
}
