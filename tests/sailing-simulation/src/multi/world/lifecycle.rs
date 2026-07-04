//! Group lifecycle verbs and the per-group membership reconcile: the harness-side registry
//! (`GroupMeta`), container removal/recreation, per-group conf-changes, and the committed-state
//! reconcile ported from the single-group `VoprState` shape, scoped to one group.

use super::*;

/// The number of consecutive reconcile passes a hosting replica may stay settled-but-absent from
/// its group's committed membership before the harness PARKS it (the catalog-acting-embedder
/// model, state retained). The grace keeps the fast path — the removal entry reaching the victim
/// via the farewell append, which self-removes-and-parks it — the COMMON path, so that class
/// stays exercised; the sweep only parks a victim the farewell never reached (or an orphaned
/// joiner whose add never committed), which would otherwise linger and churn elections forever.
const DEPARTED_GRACE_PASSES: u32 = 3;

/// Harness-side registry entry for one logical group (one per group id, across incarnations).
#[derive(Debug, Default)]
pub(crate) struct GroupMeta {
  /// The group's committed VOTER set, reconciled from the group leader's runtime `conf_state()`.
  pub(crate) voters: BTreeSet<u64>,
  /// The group's committed LEARNER set, reconciled alongside `voters`.
  pub(crate) learners: BTreeSet<u64>,
  /// The incarnation counter: 0 at creation, +1 per recreation. Keys the one-identity oracle
  /// (terms restart across incarnations) — never reused for a different logical group.
  pub(crate) generation: u64,
  /// Whether the group is currently retired (removed and not yet recreated).
  pub(crate) retired: bool,
  /// Whether a conf-change proposed through the world is still unsettled (one in flight PER
  /// GROUP, mirroring the proto's per-group gate).
  pub(crate) conf_in_flight: bool,
  /// The group-wide conf-change apply tally captured when the in-flight change was proposed;
  /// the change is settled once the tally advances past it.
  pub(crate) conf_change_baseline: u64,
  /// Joiners wired as observer replicas whose AddNode/AddLearner has not yet been observed
  /// committed (the orphan-sweep population).
  pub(crate) wired: BTreeSet<u64>,
  /// Per-hosting-replica count of consecutive settled reconcile passes it has been absent from
  /// the committed membership — the departed/orphan grace counter.
  pub(crate) departed_streak: BTreeMap<u64, u32>,
}

impl MultiWorld {
  /// Retire group `gid` everywhere: one final frozen oracle check, then container removal on
  /// every hosting node, store teardown, and a bus purge for the gid.
  ///
  /// The purge models the embedder+coordinator layer, not the container: the PURE container has
  /// no tombstones — in production the multi coordinators tombstone the removed id so wire
  /// stragglers drop silently, and Tier B covers that real code. Here the group-tagged bus
  /// stands in for the wire, so purging its `gid` entries (plus the unhosted-drop on anything
  /// later addressed to the gid) is the same observable semantics.
  pub fn remove_group(&mut self, gid: u64) {
    let meta = self
      .groups
      .get_mut(&gid)
      .unwrap_or_else(|| panic!("remove_group: unknown group {gid}"));
    assert!(!meta.retired, "remove_group: group {gid} already retired");
    meta.retired = true;
    meta.conf_in_flight = false;
    meta.wired.clear();
    meta.departed_streak.clear();
    let generation = meta.generation;

    // One final check against the still-hosted state, then freeze the checker into the archive
    // (its cross-tick history remains inspectable; the live suite stops judging the gid).
    let view = self.group_view(gid);
    let mut checker = self
      .checkers
      .remove(&gid)
      .expect("a live group has a checker");
    checker.check_or_panic(&view);
    self.retired.insert((gid, generation), checker);
    self.pending_transitions.remove(&gid);
    self.pending_new_installs.remove(&gid);

    for node in self.node_ids.clone() {
      if self
        .hosts
        .get(&node)
        .is_some_and(|h| h.contains_group(&gid))
      {
        self.drop_group_replica(gid, node);
      }
    }
    self.bus.retain(|m| m.gid != gid);
  }

  /// Recreate a retired `gid` as the SAME logical group at `generation + 1`: fresh stores, fresh
  /// replicas on the registry's voter set, a fresh checker. Allowed by the single-incarnation
  /// contract (same logical group rejoining); the bumped generation is what keeps the
  /// one-identity oracle sound across the boundary (terms restart).
  pub fn recreate_group(&mut self, gid: u64) {
    let meta = self
      .groups
      .get_mut(&gid)
      .unwrap_or_else(|| panic!("recreate_group: unknown group {gid}"));
    assert!(meta.retired, "recreate_group: group {gid} is not retired");
    meta.retired = false;
    meta.generation += 1;
    meta.learners.clear();
    let voters = meta.voters.clone();
    assert!(
      self.checkers.insert(gid, Checker::new()).is_none(),
      "recreate_group: a retired group has no live checker"
    );
    let voter_vec: Vec<u64> = voters.iter().copied().collect();
    for &node in &voters {
      let config = Config::try_new(
        node,
        voter_vec.clone(),
        Duration::from_millis(1000),
        Duration::from_millis(100),
      )
      .expect("valid multi-world config");
      self.wire_replica(node, gid, config, true);
    }
  }

  /// Wire node `node` as a NON-VOTING observer replica of `gid` (bootstrap voter set = the
  /// group's current committed voters, NOT including `node`, so it cannot campaign). The
  /// precursor to proposing the AddNode/AddLearner that makes it a member; until that commits it
  /// is tracked as a wired joiner for the orphan sweep.
  pub fn wire_group_observer(&mut self, gid: u64, node: u64) {
    let meta = self
      .groups
      .get_mut(&gid)
      .unwrap_or_else(|| panic!("wire_group_observer: unknown group {gid}"));
    assert!(!meta.retired, "wire_group_observer: group {gid} is retired");
    meta.wired.insert(node);
    meta.departed_streak.remove(&node);
    let current_voters: Vec<u64> = meta.voters.iter().copied().collect();
    let config = Config::try_new_observer(
      node,
      current_voters,
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .expect("valid observer config");
    self.wire_replica(node, gid, config, false);
  }

  /// Propose a v1 conf-change on `gid`'s current leader; returns the assigned index (`None`
  /// when leaderless or refused). On acceptance the group's one-in-flight gate arms and the
  /// settle baseline is captured.
  pub fn propose_conf_change(
    &mut self,
    gid: u64,
    cc: sailing_proto::ConfChange<u64>,
  ) -> Option<sailing_proto::Index> {
    let leader = self.leader_of(gid)?;
    let host = self.hosts.get_mut(&leader).expect("leader host exists");
    let log = self.logs.get_mut(&(leader, gid)).expect("leader log");
    let stable = self.stables.get(&(leader, gid)).expect("leader stable");
    let index = host
      .propose_conf_change(&gid, self.now, log, stable, cc)?
      .ok()?;
    let baseline = self.group_conf_changed_total(gid);
    let meta = self.groups.get_mut(&gid).expect("registered group");
    meta.conf_in_flight = true;
    meta.conf_change_baseline = baseline;
    Some(index)
  }

  /// Reconcile `gid`'s registry entry from the group's REAL committed state (the leader's
  /// runtime `conf_state()`), never propose-time bookkeeping — the single-group `VoprState`
  /// reconciliation, scoped per group:
  ///   - keep the last-known sets while leaderless (don't thrash on a transient election);
  ///   - promote a wired joiner once it appears in the committed membership;
  ///   - sweep departed replicas: a hosting node absent from the committed membership for
  ///     [`DEPARTED_GRACE_PASSES`] settled passes is PARKED — delivery-isolated with state
  ///     retained (the ignorant-victim residual and the never-committed orphan; the common case
  ///     self-removes-and-parks via the farewell append first). Parking, never destroying,
  ///     keeps every replica's view whole for the quorum-durability oracle — an ex-member's
  ///     durable log is a real witness while others lag applying the removal, and a
  ///     stale-leader reconcile can even misjudge a CURRENT member;
  ///   - UNPARK / RESURRECT a committed member (the single-group `reinstate` arm): a parked
  ///     member rejoins with its retained state; a torn-down member re-wires as a catching-up
  ///     observer — otherwise its group could never fully converge.
  pub(crate) fn reconcile_membership(&mut self, gid: u64) {
    self.refresh_conf_in_flight(gid);
    if self.groups.get(&gid).is_none_or(|m| m.retired) {
      return;
    }
    if self.leader_of(gid).is_none() {
      return;
    }
    let voters = self.committed_voters_of(gid);
    let learners = self.committed_learners_of(gid);
    let conf_in_flight = {
      let meta = self.groups.get_mut(&gid).expect("registered group");
      meta.voters = voters.clone();
      meta.learners.clone_from(&learners);
      meta
        .wired
        .retain(|n| !voters.contains(n) && !learners.contains(n));
      meta.conf_in_flight
    };

    // The departed/orphan sweep over hosting replicas (sorted, deterministic).
    let hosting: Vec<u64> = self
      .node_ids
      .clone()
      .into_iter()
      .filter(|n| self.hosts[n].contains_group(&gid))
      .collect();
    {
      let parked = self.parked.clone();
      let meta = self.groups.get_mut(&gid).expect("registered group");
      for node in hosting {
        if voters.contains(&node) || learners.contains(&node) || parked.contains(&(node, gid)) {
          meta.departed_streak.remove(&node);
          continue;
        }
        if conf_in_flight {
          // A change is still in flight (this joiner's add, or a removal about to land):
          // don't count the pass — settle first.
          meta.departed_streak.insert(node, 0);
          continue;
        }
        let streak = meta.departed_streak.entry(node).or_insert(0);
        *streak += 1;
        if *streak >= DEPARTED_GRACE_PASSES {
          self.parked.insert((node, gid));
        }
      }
    }

    // Unpark / resurrect committed members. A parked member rejoins with its retained state; a
    // genuinely torn-down member (post self-removal, later re-added) re-wires as a fresh
    // observer whose bootstrap excludes itself, so it cannot campaign until it learns its own
    // membership from the log.
    let members: Vec<u64> = voters.iter().chain(learners.iter()).copied().collect();
    for member in members {
      if self.parked.remove(&(member, gid)) {
        let meta = self.groups.get_mut(&gid).expect("registered group");
        meta.departed_streak.remove(&member);
        continue;
      }
      if self.hosts_group(member, gid) || !self.hosts.contains_key(&member) {
        continue;
      }
      let bootstrap: Vec<u64> = voters.iter().copied().filter(|&v| v != member).collect();
      let config = Config::try_new_observer(
        member,
        bootstrap,
        Duration::from_millis(1000),
        Duration::from_millis(100),
      )
      .expect("valid observer config");
      self.wire_replica(member, gid, config, false);
    }
  }

  /// Settle the per-group one-in-flight gate: the proposed change is settled once the group-wide
  /// conf-change apply tally advances past the baseline, or unconditionally while leaderless (a
  /// re-election clears it conservatively; the proto's own gate re-checks on the next propose).
  fn refresh_conf_in_flight(&mut self, gid: u64) {
    let Some(meta) = self.groups.get(&gid) else {
      return;
    };
    if !meta.conf_in_flight {
      return;
    }
    let settled = self.group_conf_changed_total(gid) > meta.conf_change_baseline
      || self.leader_of(gid).is_none();
    if settled {
      self
        .groups
        .get_mut(&gid)
        .expect("registered group")
        .conf_in_flight = false;
    }
  }

  /// The group-wide conf-change apply tally: the sum of `Event::ConfChanged` drained across the
  /// gid's current hosting replicas. Any committed change applies on the (surviving) members, so
  /// the sum advances past a propose-time baseline exactly when the change settled.
  fn group_conf_changed_total(&self, gid: u64) -> u64 {
    self
      .conf_changed
      .iter()
      .filter(|((_, g), _)| *g == gid)
      .map(|(_, c)| *c)
      .sum()
  }

  /// Tear down node `node`'s replica of `gid`: container removal + store/bookkeeping teardown.
  /// The embedder acting on RemovedSelf / the catalog — the group itself lives on elsewhere.
  pub(crate) fn drop_group_replica(&mut self, gid: u64, node: u64) {
    let host = self.hosts.get_mut(&node).expect("host exists");
    host.remove_group(&gid);
    self.logs.remove(&(node, gid));
    self.stables.remove(&(node, gid));
    self.configs.remove(&(node, gid));
    self.swept.remove(&(node, gid));
    self.conf_changed.remove(&(node, gid));
    self.snapshot_lineage.remove(&(node, gid));
    self.member_view.remove(&(node, gid));
    self.parked.remove(&(node, gid));
    // `restarts` and `read_states` deliberately survive: a later re-wire of the same (node, gid)
    // must bump past the old incarnation, and ledger scan offsets index the monotone vec.
  }

  /// The committed LEARNER set of `gid`, read like [`committed_voters_of`](Self::committed_voters_of)
  /// (the highest-term leader's `conf_state().learners()`; leaderless, the plurality config's
  /// learners keyed by voter set so the two accessors agree on the chosen config).
  fn committed_learners_of(&self, gid: u64) -> BTreeSet<u64> {
    let authoritative = self
      .node_ids
      .iter()
      .filter_map(|&n| self.hosts[&n].group(&gid))
      .filter(|ep| ep.role().is_leader())
      .max_by_key(|ep| ep.term());
    if let Some(ep) = authoritative {
      return ep.conf_state().learners().iter().copied().collect();
    }
    let mut tally: BTreeMap<BTreeSet<u64>, (usize, BTreeSet<u64>)> = BTreeMap::new();
    for &n in &self.node_ids {
      let Some(ep) = self.hosts[&n].group(&gid) else {
        continue;
      };
      let cs = ep.conf_state();
      let voters: BTreeSet<u64> = cs.voters().iter().copied().collect();
      let learners: BTreeSet<u64> = cs.learners().iter().copied().collect();
      let e = tally.entry(voters).or_insert((0, learners));
      e.0 += 1;
    }
    tally
      .into_iter()
      .max_by(|(a_v, (a_n, _)), (b_v, (b_n, _))| a_n.cmp(b_n).then_with(|| b_v.cmp(a_v)))
      .map(|(_, (_, learners))| learners)
      .unwrap_or_default()
  }

  /// Abandon a wired-but-never-committed joiner replica of `gid` on `node`: tear the observer
  /// replica down and unwire it, so an orphan (its AddNode/AddLearner refused or lost) can never
  /// pin the group's quiesce.
  pub(crate) fn abandon_wired(&mut self, gid: u64, node: u64) {
    if self.hosts_group(node, gid) {
      self.drop_group_replica(gid, node);
    }
    if let Some(meta) = self.groups.get_mut(&gid) {
      meta.wired.remove(&node);
      meta.departed_streak.remove(&node);
    }
  }

  /// The number of retired (frozen-archived) checker incarnations — the removal tally.
  #[cfg_attr(
    not(test),
    expect(dead_code, reason = "the multi VOPR report wires this")
  )]
  pub(crate) fn retired_checkers(&self) -> usize {
    self.retired.len()
  }

  /// Group `gid`'s incarnation counter (0 until its first recreation).
  pub(crate) fn generation_of(&self, gid: u64) -> u64 {
    self.groups.get(&gid).map_or(0, |m| m.generation)
  }

  /// Group `gid`'s reconciled committed voter set (the registry view).
  pub(crate) fn group_voters(&self, gid: u64) -> BTreeSet<u64> {
    self
      .groups
      .get(&gid)
      .map(|m| m.voters.clone())
      .unwrap_or_default()
  }

  /// Group `gid`'s reconciled committed learner set (the registry view).
  pub(crate) fn group_learners(&self, gid: u64) -> BTreeSet<u64> {
    self
      .groups
      .get(&gid)
      .map(|m| m.learners.clone())
      .unwrap_or_default()
  }
}

#[cfg(test)]
mod tests;
