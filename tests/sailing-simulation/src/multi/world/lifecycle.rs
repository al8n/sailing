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

/// How many CONSECUTIVE times one gid's [`remove_group`](MultiWorld::remove_group) draw may abandon
/// a fault-free teardown refusal (while `merge_choreography_active` reads false) before the world
/// TRIPS it as a real teardown-gate tie. Generous on purpose: the append-pending residual it
/// tolerates — a co-hosted source's stale log claim after the merge globally resolved — clears
/// within a handful of cranks as the lagging replica catches up, so a transient never approaches the
/// bound; a genuine world-predicate hole (a claim that never clears) refuses forever and still trips.
pub(crate) const TEARDOWN_TIE_BUDGET: usize = 128;

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
  /// The group's LIVE key population: the gkv keys it currently owns out of its per-group
  /// domain. The keyed workload picks only from here, an accepted split moves the at-or-above
  /// slice to the child at PROPOSE time, and the oracle-aligned view record keeps only these
  /// keys' cells. Full domain at creation and recreation (a fresh incarnation owns its whole
  /// keyspace again — its record is empty, so nothing overlaps a live child's inherited
  /// history, and the conservation ledger's incarnation-qualified ids keep old verdicts exact).
  pub(crate) keys: BTreeSet<u16>,
  /// Whether this group was MERGED AWAY (absorbed into a target): terminal — the id's admission
  /// floor is `u64::MAX` in the product, so recreation is refused forever (the harness catalog
  /// mirrors that refusal at [`MultiWorld::recreate_group`]). Implies `retired`.
  pub(crate) merged: bool,
  /// The transitive set of FOREIGN group tags whose cells legitimately ride this group's
  /// record — its tag lineage: a fork child inherits its parent's tag (the baseline cells carry
  /// it) plus the parent's own carried set, and a merge target inherits the source's tag plus
  /// the source's carried set (the absorbed record arrives whole, inherited cells included).
  /// The cross-talk sweep admits exactly these tags; everything else is a leak. Reset at
  /// recreation — a fresh incarnation inherits nothing.
  pub(crate) carried_tags: BTreeSet<u64>,
  /// The length of the fork-inherited applied baseline every replica of THIS incarnation opens
  /// its record with (`0` for a group not born of a fork). GROUP-level on purpose: the baseline
  /// is a property of the incarnation, not of any wiring path — a replica carries it however it
  /// arrived (fork materialization, a transferred snapshot into a fresh observer, a crash
  /// restore from the durable blob). The cross-talk sweep floors its start here; an onward
  /// split only ever SHRINKS the inherited prefix below this count, so the floor never judges
  /// an inherited cell (see `MultiWorld::cross_talk_sweep`). Reset at recreation: a fresh
  /// incarnation inherits nothing.
  pub(crate) fork_baseline: usize,
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
  ///
  /// Returns `true` when the group retired; `false` when the removal was ABANDONED as a retryable
  /// no-op — the container's teardown gate refused a hosting replica even fault-free (a real
  /// embedder retries later). A no-op tears nothing down (the draw is burned but consumes no further
  /// prng), so a caller that counts removals must gate on the return.
  pub fn remove_group(&mut self, gid: u64) -> bool {
    let generation = {
      let meta = self
        .groups
        .get(&gid)
        .unwrap_or_else(|| panic!("remove_group: unknown group {gid}"));
      assert!(!meta.retired, "remove_group: group {gid} already retired");
      meta.generation
    };

    // The removal CEILING — the max shape generation any hosting replica reached — captured while
    // the replicas are still present (they vanish on teardown below); the catalog floor derived
    // from it is applied only once the teardown lands.
    let ceiling = self
      .node_ids
      .iter()
      .filter_map(|&n| self.hosts[&n].group(&gid))
      .map(sailing_proto::Endpoint::shape_gen)
      .max()
      .unwrap_or(0);

    // One final check against the still-hosted state, then one last exact-term walk of the retiring
    // replicas' durable logs (it extends the committed-config history through any install boundary
    // the frozen archive could otherwise never witness — the absorb→observers-only→retire shape,
    // where snapshot-installed replicas emit no ConfChanged). Both read the retiring durable logs,
    // so they run BEFORE teardown and on the FULL view; the checker stays LIVE (not yet archived)
    // until the teardown lands, so a fault-burned attempt re-runs them harmlessly (`check_or_panic`
    // is the per-tick check and `certify_retiring_history` is a monotone, idempotent observer).
    let view = self.group_view(gid);
    {
      let checker = self
        .checkers
        .get_mut(&gid)
        .expect("a live group has a checker");
      checker.check_or_panic(&view);
      crate::checker::certify_retiring_history(checker, &view);
    }

    // Tear every hosting replica down ATOMICALLY: all hosts or none. The container's per-node
    // teardown gate refuses BEFORE any teardown (leaving that replica fully intact) and admits by
    // REMOVING the endpoint, so probe every hosting node here, keeping the world stores until all
    // admit; a refusal on ANY node then RESTORES the already-removed replicas from their retained
    // durable stores (a crash-style rollback). A PARTIAL teardown — dropping some committed voters'
    // durable logs while the group stays live — would strand the survivors below quorum and trip
    // `commit_is_quorum_durable`, so the world never leaves one behind.
    //
    // The scan reads FAULT-FREE: the world plays an embedder with global, non-faulting
    // knowledge, and its removal draws already exclude every participant its superset can see
    // (`merge_choreography_active`, read off `active_freezes`/`durable_entries`, never the fault
    // dice), so a transient fault must not fail-close a claim-free teardown into a partial one. A
    // refusal that SURVIVES suppression is REAL but not necessarily a tie: only the container can
    // tell a genuine claim on `gid` (leg 5, decoded off a co-hosted source's log) from a bystander's
    // unrelated freeze, and the world's in-memory superset is NOT a superset during replication lag
    // — a co-hosted source's stale log can still claim `gid` after the merge globally resolved and
    // the book moved on. That append-pending residual is TRANSIENT (the lagging replica clears it
    // within a handful of cranks), so a fault-free refusal with `merge_choreography_active` false is
    // a retryable no-op, bounded so a genuine non-superset hole still trips.
    let suspended: Vec<((u64, u64), (u16, u16))> = self
      .logs
      .iter_mut()
      .map(|(k, log)| (*k, log.suspend_read_faults()))
      .collect();
    let mut torn_down: Vec<u64> = Vec::new();
    let mut refused = None;
    for node in self.node_ids.clone() {
      if !self
        .hosts
        .get(&node)
        .is_some_and(|h| h.contains_group(&gid))
      {
        continue;
      }
      match self.drop_group_endpoint(gid, node) {
        Ok(()) => torn_down.push(node),
        Err(refusal) => {
          refused = Some((node, refusal));
          break;
        }
      }
    }
    if refused.is_some() {
      // Roll the atomic teardown back — restore every already-removed replica from its retained
      // durable stores, so nothing was dropped.
      for &node in &torn_down {
        self.restore_group_replica(gid, node);
      }
    }
    for (k, rates) in suspended {
      if let Some(log) = self.logs.get_mut(&k) {
        log.restore_read_faults(rates);
      }
    }
    if let Some((node, refusal)) = refused {
      if self.merge_choreography_active(gid) {
        // A legitimately active choreography — the removal DRAW filters these upstream; a direct
        // caller lands here. Not a tie: reset the streak and abandon.
        self.teardown_tie_streak.remove(&gid);
        return false;
      }
      let streak = self.teardown_tie_streak.entry(gid).or_insert(0);
      *streak += 1;
      assert!(
        *streak <= TEARDOWN_TIE_BUDGET,
        "remove_group: tearing down g{gid} on node {node} was refused {refusal:?} fault-free for \
         {streak} consecutive draws with merge_choreography_active=false — a REAL teardown-gate tie \
         the world's superset never covers, not a transient append-pending residual (seed={} \
         tick={})",
        self.seed,
        self.tick_count,
      );
      return false;
    }

    // Every host admitted: reset the tie streak, purge the retained stores, and COMMIT the
    // retirement. The embedder catalog floors what it permanently stops hosting — one past the
    // removal ceiling. A reshaped id that later backs a RE-DERIVED merge-abort thaw obligation (a
    // target replays its still-durable abort entry after a crash, but the source's stores are gone)
    // discharges it off this floor (`!floor_admits(floor, expected)`) while unhosted, instead of
    // wedging the target's compaction fence forever; the obligation is thus discharged before any
    // recreate, so a gen-0 recreate squats harmlessly. A never-reshaped id (ceiling 0) takes NO
    // floor, so the default (no-merge, no-split) profile stays byte-identical.
    self.teardown_tie_streak.remove(&gid);
    for &node in &torn_down {
      self.purge_group_stores(gid, node);
    }
    if ceiling > 0 {
      let floor = self.removal_floors.entry(gid).or_insert(0);
      *floor = (*floor).max(ceiling.saturating_add(1));
    }
    {
      let meta = self
        .groups
        .get_mut(&gid)
        .expect("remove_group: the group was live above");
      meta.retired = true;
      meta.conf_in_flight = false;
      meta.wired.clear();
      meta.departed_streak.clear();
    }
    // Freeze the checker into the archive (its cross-tick history remains inspectable; the live
    // suite stops judging the gid).
    let checker = self
      .checkers
      .remove(&gid)
      .expect("a live group has a checker");
    self.retired.insert((gid, generation), checker);
    self.pending_transitions.remove(&gid);
    self.pending_new_installs.remove(&gid);
    self.bus.retain(|m| m.gid != gid);
    true
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
    assert!(
      !meta.merged,
      "recreate_group: group {gid} was merged away — its floor is terminal (u64::MAX); \
       the catalog never re-admits it"
    );
    assert!(meta.retired, "recreate_group: group {gid} is not retired");
    meta.retired = false;
    meta.generation += 1;
    meta.learners.clear();
    // Cross the incarnation boundary: drop every fork-fence record naming this id as a PARENT. The
    // retirement teardown already cleared the hosting nodes' records; this is the belt on the
    // boundary itself, so the new incarnation can never inherit the old one's coupling (#110).
    self.fork_conflicts.retain(|&(_, parent), _| parent != gid);
    meta.keys = (0..super::super::NUM_KEYS).collect();
    meta.fork_baseline = 0;
    meta.carried_tags.clear();
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
  ///   - keep the last-known sets while leaderless (don't thrash on a transient election) — but
  ///     first COMPLETE an under-hosted group (see [`complete_under_hosted`]
  ///     (Self::complete_under_hosted)) AND still UNPARK its committed members: a quorum mis-parked
  ///     by the sweep is leaderless precisely because its own voters were parked, and nothing else
  ///     re-derives it back to a quorum (`complete_under_hosted` reads a parked replica as
  ///     still-hosting), so the unpark pass must run here too, not only behind a leader;
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
      // Read the committed set for the unpark decision only; leave `meta` at its last-known values
      // (don't thrash on a transient election). Completing the under-hosted set repairs torn-down
      // voters; the unpark pass then re-derives a mis-parked quorum back to hosting.
      let voters = self.committed_voters_of(gid);
      let learners = self.committed_learners_of(gid);
      self.complete_under_hosted(gid);
      self.unpark_committed_members(gid, &voters, &learners);
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
    let choreography_active = self.merge_choreography_active(gid);
    {
      let parked = self.parked.clone();
      let meta = self.groups.get_mut(&gid).expect("registered group");
      for node in hosting {
        if voters.contains(&node) || learners.contains(&node) || parked.contains(&(node, gid)) {
          meta.departed_streak.remove(&node);
          continue;
        }
        if conf_in_flight || choreography_active {
          // A conf-change is still in flight (this joiner's add, or a removal about to land), or a
          // merge choreography still holds this group's participants put: don't count the pass. The
          // contract keeps a frozen/claimed participant in place until it resolves, and the
          // committed-view derivation can transiently misjudge a current member of one — parking a
          // mis-derived voter of a frozen source is the freeze-wedge shape this guards against.
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

    self.unpark_committed_members(gid, &voters, &learners);
  }

  /// Unpark / resurrect the committed members of `gid` (the single-group `reinstate` arm). A
  /// parked member rejoins with its retained state; a genuinely torn-down member (post
  /// self-removal, later re-added) re-wires as a fresh observer whose bootstrap excludes itself,
  /// so it cannot campaign until it learns its own membership from the log. Runs on the LED and
  /// the LEADERLESS paths alike: a quorum mis-parked by the sweep stays leaderless until this
  /// un-parks it back to hosting.
  fn unpark_committed_members(
    &mut self,
    gid: u64,
    voters: &BTreeSet<u64>,
    learners: &BTreeSet<u64>,
  ) {
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

  /// Complete a LEADERLESS group whose hosting replicas do not cover its committed voter set:
  /// wire each missing committed voter as an EMPTY catching-up OBSERVER — the world-side model
  /// of the product's solicitation edge under the factory contract's fork-born rule. In
  /// production a campaigner's vote solicitation reaching a node that does not host the group
  /// triggers the coordinator's factory chain, so a group stranded below hosting quorum is
  /// completed by its own election attempts; the world's bus instead drops unhosted deliveries
  /// silently, and without this arm no repair exists — the resurrect arm above is leader-gated,
  /// and a sub-quorum group can never elect the leader it needs (a fork-born child stranded
  /// when its parent was retired inside the propagation window campaigns at its manufactured
  /// baseline forever).
  ///
  /// The empties boot as OBSERVERS ([`Config::try_new_observer`], self absent from the
  /// bootstrap voters — the resurrect arm's exact wiring): a conforming factory blueprints
  /// fork-born ids this way BECAUSE a full-voter empty is promotable with a virgin election
  /// timer, and an empty quorum's first commit lands on the manufactured `(term 1, index 1)`
  /// baseline coordinate, where log-matching fuses the divergent histories silently. An
  /// observer empty still grants votes, so the holder's manufactured log wins the ONLY possible
  /// election, the zero-progress joiners are structurally forced onto the snapshot route, and
  /// the snapshot's boundary config is what teaches them their own voterhood. Shape-generic on
  /// purpose: the product's solicitation edge does not distinguish fork children from any other
  /// under-hosted group, so neither does this arm.
  ///
  /// Gates, in order (the caller holds the live-registered + leaderless ones):
  ///   - a SOLICITATION WITNESS must exist — an unparked hosting replica whose own config
  ///     lists it as a voter (a campaigner whose solicitations would reach the missing peers);
  ///     a parked holder is delivery-isolated and solicits nothing, so nothing materializes;
  ///   - only committed voters missing from the HOSTING set are wired; a parked replica still
  ///     hosts (state retained), so this arm can never overwrite one with an empty.
  ///
  /// Unreachable while every committed voter hosts: a fully-hosted leaderless group is
  /// ordinary election territory and completes without world help.
  fn complete_under_hosted(&mut self, gid: u64) {
    let witness = self.node_ids.iter().any(|&n| {
      !self.parked.contains(&(n, gid))
        && self.hosts[&n]
          .group(&gid)
          .is_some_and(|ep| ep.conf_state().is_voter(&n))
    });
    if !witness {
      return;
    }
    let voters = self.committed_voters_of(gid);
    let missing: Vec<u64> = voters
      .iter()
      .copied()
      .filter(|&v| self.hosts.contains_key(&v) && !self.hosts_group(v, gid))
      .collect();
    if missing.is_empty() {
      return;
    }
    for node in missing {
      let bootstrap: Vec<u64> = voters.iter().copied().filter(|&v| v != node).collect();
      let config = Config::try_new_observer(
        node,
        bootstrap,
        Duration::from_millis(1000),
        Duration::from_millis(100),
      )
      .expect("valid observer config");
      self.wire_replica(node, gid, config, false);
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
  /// The embedder acting on RemovedSelf / the catalog — the group itself lives on elsewhere. The
  /// internal merge-teardown callers (the husk-dissolve and deferred-capture sweeps) tear down a
  /// source the resolver already settled, where no participant refusal can stand — assert it away.
  pub(crate) fn drop_group_replica(&mut self, gid: u64, node: u64) {
    self
      .try_drop_group_replica(gid, node)
      .expect("the world never tears down an unresolved merge participant");
  }

  /// Tear down node `node`'s replica of `gid`, RETURNING the container's participant refusal rather
  /// than asserting it away — container removal, then (only on admission) the store/bookkeeping
  /// cleanup. The strict [`drop_group_replica`](Self::drop_group_replica) wrapper keeps the assert
  /// for the internal merge-teardown callers, whose sources are past any choreography;
  /// [`remove_group`](Self::remove_group) drives the two halves separately (probe every host, then
  /// commit or roll back) so its multi-host teardown is atomic.
  pub(crate) fn try_drop_group_replica(
    &mut self,
    gid: u64,
    node: u64,
  ) -> Result<(), sailing_proto::RemoveError> {
    self.drop_group_endpoint(gid, node)?;
    self.purge_group_stores(gid, node);
    Ok(())
  }

  /// Remove node `node`'s `gid` ENDPOINT from its container, RETAINING every world store — the
  /// container half of a teardown. A refusal leaves the endpoint FULLY intact (the gate refuses
  /// before any teardown), so the removal verb can probe a host without committing to it; on
  /// admission the endpoint is gone but the durable stores remain, ready either for
  /// [`purge_group_stores`](Self::purge_group_stores) (commit) or
  /// [`restore_group_replica`](Self::restore_group_replica) (rollback).
  fn drop_group_endpoint(&mut self, gid: u64, node: u64) -> Result<(), sailing_proto::RemoveError> {
    let host = self.hosts.get_mut(&node).expect("host exists");
    // The teardown gate reads logs (the Claimed leg), never floors — an empty husk feed. `stores`
    // is the seam the `Claimed` leg reads a freeze-pending source's log through, threaded exactly
    // as the merge pump threads it.
    let no_husks = BTreeSet::new();
    let mut stores = super::merge::NodeStores {
      node,
      logs: &mut self.logs,
      stables: &mut self.stables,
      floored: &self.merge_floors,
      removal_floors: &self.removal_floors,
      husk_floors: &no_husks,
    };
    host.remove_group(&gid, &mut stores)?;
    Ok(())
  }

  /// Drop node `node`'s retained `gid` stores and per-replica bookkeeping — the store half of a
  /// teardown, run once the endpoint removal has committed.
  fn purge_group_stores(&mut self, gid: u64, node: u64) {
    self.logs.remove(&(node, gid));
    self.stables.remove(&(node, gid));
    self.configs.remove(&(node, gid));
    self.swept.remove(&(node, gid));
    self.conf_changed.remove(&(node, gid));
    self.snapshot_lineage.remove(&(node, gid));
    self.member_view.remove(&(node, gid));
    self.parked.remove(&(node, gid));
    // A fork-fence record on `(node, gid)` — `gid` in the PARENT role — is a live coupling fact, not
    // history: tearing this node's `gid` replica down lifts any standing capture fence it held, so the
    // record must go with it (#110). This is the shared teardown chokepoint for both
    // `drop_group_replica` and `remove_group`.
    self.fork_conflicts.remove(&(node, gid));
    // The durable relay lineage is per-incarnation (a real driver's engine drops the group's
    // `group_gen` on teardown): a fresh incarnation of this id must not inherit the retired one's
    // relayed forks, or a later restart's guard would fold its legitimate new forks.
    self.relayed_lineage.remove(&(node, gid));
    // `restarts` and `read_states` deliberately survive: a later re-wire of the same (node, gid)
    // must bump past the old incarnation, and ledger scan offsets index the monotone vec.
  }

  /// Restore node `node`'s `gid` replica from its RETAINED durable stores — the rollback half of an
  /// atomic teardown (see [`remove_group`](Self::remove_group)), rebuilding the endpoint
  /// [`drop_group_endpoint`](Self::drop_group_endpoint) removed while the store was kept. A
  /// crash-style restart: in-flight (unflushed) store windows are discarded and the incarnation
  /// bumps so the checker resets this replica's monotonicity baseline, exactly as
  /// [`crash`](Self::crash) restores a rebooted node. The boot epoch is REUSED (the rollback is not
  /// a reboot; no in-flight message was ever sent to distinguish from).
  fn restore_group_replica(&mut self, gid: u64, node: u64) {
    let epoch = self.boot_epochs.get(&node).copied().unwrap_or(0);
    let config = self.configs[&(node, gid)].clone();
    let now = self.now;
    let seed = self.seed ^ node;
    let log = self
      .logs
      .get_mut(&(node, gid))
      .expect("retained replica log");
    let stable = self
      .stables
      .get_mut(&(node, gid))
      .expect("retained replica stable");
    log.discard_inflight();
    stable.discard_inflight();
    let relayed = self.relayed_lineage.get(&(node, gid)).copied().unwrap_or(0);
    let host = self.hosts.get_mut(&node).expect("host exists");
    host
      .restore_group(gid, config, now, seed, LogSm::new(), epoch, log, stable)
      .unwrap_or_else(|e| {
        panic!("remove_group rollback: restore of group {gid} on node {node}: {e:?}")
      });
    // Restore the container's relay guard from the durable relay lineage (a real driver's
    // `raise_relay_guard(engine.group_gen)` after restore): the restart replay re-stages every
    // already-materialized fork, and the guard must fold them to duplicates rather than re-relay —
    // or, now, PARK them against a co-hosted child that lost its provenance to churn.
    host.raise_relay_guard(&gid, relayed);
    *self.restarts.entry((node, gid)).or_insert(0) += 1;
  }

  /// The committed LEARNER set of `gid`, read like [`committed_voters_of`](Self::committed_voters_of)
  /// (the highest-term leader's `conf_state().learners()`; leaderless, the plurality config's
  /// learners keyed by voter set so the two accessors agree on the chosen config). Parked
  /// replicas are excluded from both paths for the same reason: a reaped zombie's stale config
  /// must never define the group's committed membership.
  pub(super) fn committed_learners_of(&self, gid: u64) -> BTreeSet<u64> {
    let authoritative = self
      .node_ids
      .iter()
      .filter(|&&n| !self.parked.contains(&(n, gid)))
      .filter_map(|&n| self.hosts[&n].group(&gid))
      .filter(|ep| ep.role().is_leader())
      .max_by_key(|ep| ep.term());
    if let Some(ep) = authoritative {
      return ep.conf_state().learners().iter().copied().collect();
    }
    let mut tally: BTreeMap<BTreeSet<u64>, (usize, BTreeSet<u64>)> = BTreeMap::new();
    for &n in &self.node_ids {
      if self.parked.contains(&(n, gid)) {
        continue;
      }
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
