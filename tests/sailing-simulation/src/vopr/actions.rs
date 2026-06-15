use super::*;

// ─── Action selection ──────────────────────────────────────────────────────────────────────────

/// Draw a weighted action from [`MENU`] using the master PRNG (deterministic from the seed).
pub(crate) fn pick_action(prng: &mut FaultPrng) -> Action {
  let total: u32 = MENU.iter().map(|(_, w)| w).sum();
  let mut pick = (prng.next_u64() % total as u64) as u32;
  for (action, w) in MENU {
    if pick < *w {
      return *action;
    }
    pick -= *w;
  }
  // Unreachable (the loop always returns), but fall back to the dominant action defensively.
  Action::ClientLoad
}

/// Pick the `k`-th element (by sorted order) of a non-empty set, where `k` is a seeded draw. Sorted
/// iteration over a `BTreeSet` is deterministic, so this is reproducible from the seed. Returns
/// `None` for an empty set.
pub(crate) fn pick_from(set: &BTreeSet<u64>, prng: &mut FaultPrng) -> Option<u64> {
  if set.is_empty() {
    return None;
  }
  let k = (prng.next_u64() % set.len() as u64) as usize;
  set.iter().nth(k).copied()
}

// ─── Actions ─────────────────────────────────────────────────────────────────────────────────────

/// Propose 1..=k distinct client commands on the current leader (no-op if momentarily leaderless).
/// Each accepted command is recorded in `proposed` (and counted) so quiesce can verify it applied.
pub(crate) fn client_load(
  c: &mut Cluster,
  st: &mut VoprState,
  prng: &mut FaultPrng,
  report: &mut VoprReport,
) {
  let k = 1 + (prng.next_u64() % 4) as usize; // 1..=4 commands
  for _ in 0..k {
    // Keyed-value payload: round-robin the monotonic counter across NUM_KEYS keys. cmd_counter stays
    // globally monotonic, so each (key, value) is distinct (the `proposed`/quiesce distinctness checks
    // hold) AND per-key values strictly increase (so the latest entry for a key carries its max value).
    let key = (st.cmd_counter % NUM_KEYS as u64) as u16;
    let payload = encode_kv(key, st.cmd_counter);
    if c.propose(&payload).is_some() {
      st.proposed.push(payload);
      st.cmd_counter += 1;
      report.proposals += 1;
    } else {
      // No leader right now — stop the batch; a later iteration retries once a leader re-emerges.
      break;
    }
  }
}

/// Isolate a voter, subject to the fault budget (never take down a quorum). Skips if the budget is
/// exhausted or there is no eligible (live, voter, not-already-down) node.
pub(crate) fn partition(
  c: &mut Cluster,
  st: &mut VoprState,
  prng: &mut FaultPrng,
  report: &mut VoprReport,
) {
  if st.budget_remaining() == 0 {
    return; // taking another voter down would break quorum — skip
  }
  // Eligible victims: voters currently up, not already down, and NOT an in-flight removal victim — a
  // `removing` node is kept network-live so it can ack/vote its own RemoveNode's quorum (under
  // apply-time that removal commits through the OLD config, which still includes the victim).
  let eligible: BTreeSet<u64> = st
    .voters
    .iter()
    .filter(|id| !st.down.contains(id) && !st.removing.contains(id))
    .copied()
    .collect();
  if let Some(victim) = pick_from(&eligible, prng) {
    // Apply-time liveness guard: never isolate a voter if doing so drops the FULL committed config
    // (counting a `removing` victim as the live, quorum-relevant member it still is) below a reachable
    // majority — an in-flight RemoveNode must be able to commit through that quorum. Mirrors
    // `conf_change`'s removable surviving-majority test; closes the compose-with-budget gap where
    // partition + an in-flight removal each pass their own check but together strand the quorum.
    let reachable_after = st
      .voters
      .iter()
      .filter(|v| **v != victim && !st.down.contains(v) && !st.gone.contains(v))
      .count();
    if reachable_after * 2 <= st.voters.len() {
      return; // would strand the committed-config quorum — skip this isolation
    }
    c.isolate(victim);
    st.down.insert(victim);
    report.partitions += 1;
  }
}

/// Heal a currently-isolated node (reconnect it). Skips if nothing is isolated.
pub(crate) fn heal_one(
  c: &mut Cluster,
  st: &mut VoprState,
  prng: &mut FaultPrng,
  report: &mut VoprReport,
) {
  // Only heal nodes the VOPR itself isolated (not removed nodes, which the cluster keeps isolated
  // by design). `down` is exactly the VOPR-isolated set.
  if let Some(node) = pick_from(&st.down.clone(), prng) {
    c.heal(node);
    st.down.remove(&node);
    report.heals += 1;
    report.restarts += 1;
  }
}

/// Crash a node (loses its fsync window; auto-restarts from durable state). A crash is a point event
/// — the node is alive again immediately — so it does NOT count against the sustained fault budget.
/// We still avoid crashing a node that is currently isolated (it would just re-crash a node already
/// out of the quorum), preferring to exercise the recovery path on a participating node.
pub(crate) fn crash_one(
  c: &mut Cluster,
  st: &mut VoprState,
  prng: &mut FaultPrng,
  report: &mut VoprReport,
) {
  // Crash any live (non-removed) node, isolated or not. Crashing a participating voter is the
  // highest-value case (it exercises fsync-loss + recovery while the cluster is making progress),
  // and since a crash auto-restarts, it never sustains an outage past the budget.
  let live = st.live_ids();
  if let Some(victim) = pick_from(&live, prng) {
    c.crash(victim);
    report.crashes += 1;
    report.restarts += 1;
  }
}

/// Propose a seed-chosen conf-change (AddNode / AddLearner / RemoveNode) when viable.
///
/// **Viability + one-in-flight (critical):** the cluster's `add_node`/`remove_node` helpers PANIC if
/// the proto refuses the proposal (no leader, or a conf-change already in flight, which surfaces as
/// `ProposeError::ConfChangeInFlight`). The VOPR therefore (a) only acts when it believes no
/// conf-change is in flight (`conf_in_flight == false`) and a leader exists, and (b) issues the
/// change via the NON-panicking `wire_joining_node` + `propose_conf_change` / `propose_conf_change` +
/// `mark_removed` path, checking the returned `Option` itself. A `RemoveNode` is skipped if it would
/// drop the voter set below [`MIN_VOTERS`] or remove the only surviving quorum (which would poison
/// the leader via the proto's `EmptyVoterSet` apply-time guard).
pub(crate) fn conf_change(
  c: &mut Cluster,
  st: &mut VoprState,
  prng: &mut FaultPrng,
  report: &mut VoprReport,
) {
  if st.conf_in_flight {
    return; // one change in flight at a time (mirrors the proto's pending_conf gate)
  }
  let leader = match c.leader() {
    Some(l) => l,
    None => return, // need a leader to accept the proposal
  };

  // Choose among Add-voter / Add-learner / Remove, gated by viability.
  let can_grow = st.voters.len() + st.learners.len() < MAX_NODES;
  // A removable voter is one that is NOT the leader and whose removal keeps >= MIN_VOTERS voters and
  // keeps a surviving quorum among the still-up voters.
  let removable: BTreeSet<u64> = if st.voters.len() > MIN_VOTERS {
    st.voters
      .iter()
      .filter(|&&id| id != leader)
      .filter(|&&id| {
        // After removing `id`, the voter set is voters \ {id}; require a surviving majority among the
        // up voters (those neither VOPR-isolated nor already cluster-removed). Keeps liveness
        // achievable post-change.
        let remaining: BTreeSet<u64> = st.voters.iter().copied().filter(|&v| v != id).collect();
        let up = remaining
          .iter()
          .filter(|v| !st.down.contains(v) && !st.gone.contains(v))
          .count();
        up * 2 > remaining.len()
      })
      .copied()
      .collect()
  } else {
    BTreeSet::new()
  };

  // Weighted choice: grow (add voter or learner) vs shrink (remove), only among the viable options.
  // On a successful ADD we do NOT optimistically insert the new id into `voters`/`learners` — those
  // are reconciled from the cluster's committed state once the AddNode/AddLearner actually COMMITS.
  // We only record the wired joiner; if its change never commits, the orphan sweep in
  // `reconcile_membership` abandons it.
  let roll = prng.next_u64() % 3;
  let did = match roll {
    0 if can_grow => {
      // AddNode (voter). Wire the node into the sim FIRST (so the replicated AddNode entry can reach
      // it), then propose. If the proposal is refused (e.g. the proto rejects with ConfChangeInFlight
      // because a previous change is still pending despite our flag, or the leader just vanished),
      // the wired node is an ORPHAN observer that never receives the log and would pin
      // `min_applied_len()` at 0 forever — so mark it removed at once. If accepted, it becomes a
      // pending joiner; reconciliation promotes it to a voter when its AddNode commits, or the orphan
      // sweep abandons it if the change never commits.
      let id = st.next_id;
      st.next_id += 1;
      c.wire_joining_node(id);
      let cc = sailing_proto::ConfChange::new(
        sailing_proto::ConfChangeType::AddNode,
        id,
        bytes::Bytes::new(),
      );
      if c.propose_conf_change(cc).is_some() {
        st.wired.insert(id);
        st.missing_streak.insert(id, 0);
        true
      } else {
        c.mark_removed(id); // abandon the orphan so it cannot pin liveness metrics
        st.gone.insert(id);
        false
      }
    }
    1 if can_grow => {
      // AddLearner (same wire-then-reconcile / orphan-abandon handling as AddNode).
      let id = st.next_id;
      st.next_id += 1;
      c.wire_joining_node(id);
      let cc = sailing_proto::ConfChange::new(
        sailing_proto::ConfChangeType::AddLearnerNode,
        id,
        bytes::Bytes::new(),
      );
      if c.propose_conf_change(cc).is_some() {
        st.wired.insert(id);
        st.missing_streak.insert(id, 0);
        true
      } else {
        c.mark_removed(id); // abandon the orphan so it cannot pin liveness metrics
        st.gone.insert(id);
        false
      }
    }
    _ => {
      // RemoveNode (only if a viable victim exists). The victim is kept FULLY LIVE (still voting /
      // replicating) until its removal is observed COMMITTED — `reconcile_membership` isolates it
      // (`mark_removed` + `gone`) once `committed_voters` no longer lists it. Isolating at propose
      // time (the old behavior) made the victim a PHANTOM voter — still in the surviving nodes'
      // committed configs, hence counted toward quorum, yet unreachable — which deadlocks an election
      // if the removal never propagates. Recording it in `removing` is what defers the isolation.
      if let Some(victim) = pick_from(&removable, prng) {
        let cc = sailing_proto::ConfChange::new(
          sailing_proto::ConfChangeType::RemoveNode,
          victim,
          bytes::Bytes::new(),
        );
        if c.propose_conf_change(cc).is_some() {
          st.removing.insert(victim);
          true
        } else {
          false
        }
      } else {
        false
      }
    }
  };

  if did {
    st.conf_in_flight = true;
    st.conf_change_baseline = c.total_conf_changed();
    report.conf_changes += 1;
  }
}

/// Issue 1..=3 linearizable reads. Two thirds of draws target the LEADER (the direct path);
/// one third targets a random OTHER live node (the follower-forward path — and, leaderless,
/// the NoLeader refusal path, a legitimate no-op). Each accepted read records the
/// completed-write floor in the ledger; the per-iteration scan asserts its confirmation.
pub(crate) fn read_index_load(
  c: &mut Cluster,
  st: &VoprState,
  reads: &mut ReadLedger,
  prng: &mut FaultPrng,
  report: &mut VoprReport,
) {
  let k = 1 + (prng.next_u64() % 3) as usize; // 1..=3 reads
  for _ in 0..k {
    let leader = c.leader();
    let forward = prng.next_u64().is_multiple_of(3);
    let target = if forward {
      // A live node that is not the leader, when one exists (sorted pick = deterministic).
      let others: BTreeSet<u64> = st
        .live_ids()
        .into_iter()
        .filter(|id| Some(*id) != leader)
        .collect();
      pick_from(&others, prng).or(leader)
    } else {
      leader.or_else(|| pick_from(&st.live_ids(), prng))
    };
    let Some(target) = target else { return };
    // Each read targets a seed-chosen key in 0..NUM_KEYS (deterministic from the master PRNG).
    let key = (prng.next_u64() % NUM_KEYS as u64) as u16;
    reads.issue(c, target, key, report);
  }
}

/// Ask the leader to transfer leadership to a random other voter. The target may be isolated
/// or lagging — the transfer aborting on its deadline is a legitimate, valuable case; the
/// oracles and calm windows catch anything the handoff breaks.
pub(crate) fn transfer_leader(
  c: &mut Cluster,
  st: &VoprState,
  prng: &mut FaultPrng,
  report: &mut VoprReport,
) {
  let Some(leader) = c.leader() else { return };
  let others: BTreeSet<u64> = st.voters.iter().copied().filter(|&v| v != leader).collect();
  let Some(target) = pick_from(&others, prng) else {
    return;
  };
  if c.transfer_leader(target).is_ok() {
    report.transfers += 1;
  }
}

/// Re-roll the network + per-node storage fault intensities to a new seed-chosen level (an
/// adversarial schedule that shifts over the run). Uses a fresh per-call seed derived from the master
/// PRNG so the schedule stays deterministic.
pub(crate) fn fault_reroll(c: &mut Cluster, st: &VoprState, prng: &mut FaultPrng, seed: u64) {
  let net = roll_network_faults(prng, /* calm */ false);
  let net_seed = prng.next_u64();
  c.set_network_faults(net, net_seed);
  // Re-roll storage faults on every live node (voters + learners), each with its own seed.
  for id in st.voters.iter().chain(st.learners.iter()).copied() {
    let sf = roll_storage_faults(prng);
    c.set_node_faults(id, sf, seed.wrapping_add(id).wrapping_add(prng.next_u64()));
  }
}

// ─── Observation / bookkeeping ───────────────────────────────────────────────────────────────────

/// Fold the current cluster state into the report's running maxima / fault tallies. Called after
/// every batch of ticks. Cheap (reads public accessors only) and never perturbs the run.
pub(crate) fn observe(c: &mut Cluster, _st: &mut VoprState, report: &mut VoprReport) {
  report.max_term_seen = report.max_term_seen.max(c.max_term().get());
  // `net_dropped`/`net_duplicated` are monotonic cluster-wide counters; tracking the latest value
  // captures the total faults fired so far (they only ever grow). Storage faults are not separately
  // counted by the cluster, but a fired storage fault manifests as a poison the quiesce phase clears;
  // the network tallies are the load-bearing non-vacuity signal.
  report.faults_fired = c.net_dropped() + c.net_duplicated();
}
