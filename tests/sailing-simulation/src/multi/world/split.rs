//! The split lifecycle verb, the fork pump, and the conservation recorder — the world-side
//! wiring of the reshaping machinery.
//!
//! The world plays the DRIVER's role from the reactor choreography: [`MultiWorld::propose_split`]
//! delegates to the container's gate stack, the per-tick fork pump runs the drivers' own
//! `peek_yieldable_fork` → `install_yieldable_fork` pair (sync stores make each baseline durable at
//! the call, so the barrier lifts immediately — the one-crank engine-flush contract collapsed to
//! its synchronous form), and the FIRST materialization of a child REGISTERS it in the harness
//! catalog (checker, registry entry, key population, the conservation pair).
//!
//! The forked half NEVER leaves the container, so the world reads it where it lands: the installed
//! child's own state machine, pinned lossless against its snapshot round-trip at the install site.
//!
//! # Key populations and the conservation ledger
//!
//! Every group owns a live KEY POPULATION (its un-given-away slice of the per-group gkv key
//! domain). A split's instruction is a split point over the parent's population: keys at or
//! above it move to the child. The population flips AT PROPOSE — the split entry is appended
//! ahead of any later proposal, so every parent-log cell for a moved key sits BELOW the split
//! index and the apply-derived blob carries all of them; a fuzzer write of a moved key after the
//! entry would land above it and falsify the handover, so the flip is what keeps the oracle
//! judging the PRODUCT rather than the workload. A lost split entry (deposed leader) leaves the
//! parent's population conservatively shrunk: the moved keys are PARKED, unroutable from then
//! on, while their cells remain in the parent's record — so a LATER accepted split whose point
//! sits at or below them moves those cells like any others. The conservation ASSIGNMENT
//! therefore follows the instruction rule, not the population snapshot: a registered split's
//! `child_keys` derive from the fork's own record (see `register_split_child`), so a parked
//! key surfacing in a later child is judged as the handover it is.
//!
//! The recorder walks every replica's FULL RAW applied record each sweep and appends each gkv
//! cell once — values are globally unique, so the per-`(group, key)` SET of recorded values
//! dedupes across replicas, crash re-walks, and the child's inherited baseline alike. A set, not
//! a monotone high-water mark: a MERGE folds a source's cells under the TARGET's ledger id where
//! they can sit BELOW the target's own values, and a mark would drop each as stale though it is a
//! distinct cell of a different lineage. The walk trusts membership, never positions either:
//! `LogSm::split` mutates the record non-append-only (moved-key cells vanish record-wide) and a
//! crash restore can resurrect a pre-split state, so a cell can sit below any position an earlier
//! sweep reached — a positional resume watermark skips it forever, punching an interior hole in
//! the recorded history that a later fork baseline exposes as a false conservation verdict. The
//! set needs no resume state at all: a full re-walk re-presents only cells the ledger already
//! admitted. Histories are keyed by an INCARNATION-QUALIFIED ledger id, so a recreated gid's
//! fresh history can never pollute a recorded split pair's verdict. The child's opening history
//! is recorded from its OWN materialized record (the fork blob) — never copied from the parent's
//! — which is what gives [`ConservationLedger::assert_partition`] teeth: a partition bug in the
//! FSM shows up as a parent/child history mismatch, not a tautology.

use super::*;

/// A proposed-but-not-yet-registered split: the parent, the instruction's split point, and the
/// population slice the propose-time flip assigned to the child.
pub(super) struct PendingSplit {
  pub(super) parent: u64,
  /// The instruction's split point — kept so registration can re-derive the ASSIGNED set from
  /// the fork's own record (`>= point`), which the population slice alone under-counts when an
  /// earlier accepted-but-lost split parked keys.
  pub(super) point: u16,
  pub(super) child_keys: BTreeSet<u16>,
}

/// One REGISTERED split (the child materialized somewhere): the conservation verdict's unit.
pub(super) struct SplitRecord {
  /// The parent's incarnation-qualified ledger id AT the split.
  pub(super) parent_led: u64,
  /// The child's incarnation-qualified ledger id (its fork generation).
  pub(super) child_led: u64,
  /// The keys the INSTRUCTION assigned to the child: the propose-time population slice plus
  /// every at-or-above-point key present in the fork's own record (parked keys a lost earlier
  /// split left behind travel with their cells; see `register_split_child`).
  pub(super) child_keys: BTreeSet<u16>,
}

/// Prints replay context if a conservation assert unwinds (the ledger's panics carry the ids and
/// histories but not the seed — this guard adds it without touching the pure seam).
struct ReplayContext {
  seed: u64,
  parent_led: u64,
  child_led: u64,
}

impl Drop for ReplayContext {
  fn drop(&mut self) {
    if std::thread::panicking() {
      std::eprintln!(
        "[conservation] while judging split parent_led={} child_led={} (ledger id = gid + \
         generation * 1_000_000)\n  seed={} (replay: run_multi_vopr(seed, ticks, profile))",
        self.parent_led,
        self.child_led,
        self.seed,
      );
    }
  }
}

impl MultiWorld {
  /// The incarnation-qualified conservation-ledger id: `gid + generation * 1_000_000`. Injective
  /// for this harness's id space (monotone gids from 100, far below the band) and readable in a
  /// panic (generation 1 of g105 prints as 1000105).
  pub(super) fn ledger_id(generation: u64, gid: u64) -> u64 {
    debug_assert!(gid < 1_000_000, "gid {gid} escaped the ledger-id band");
    generation * 1_000_000 + gid
  }

  /// Propose a SPLIT of `parent` at `point`: keys `>= point` of the parent's live population
  /// move to `child` (a NEVER-USED id — the single-incarnation contract; the caller mints it).
  /// Delegates to the container's gate stack on the parent's current leader and returns its
  /// verdict verbatim (`None` while leaderless). On acceptance the parent's population flips
  /// immediately (see the module docs for why propose-time is the sound flip point).
  pub fn propose_split(
    &mut self,
    parent: u64,
    child: u64,
    point: u16,
  ) -> Option<Result<sailing_proto::Index, sailing_proto::SplitError<u64>>> {
    assert!(
      !self.groups.contains_key(&child) && !self.pending_splits.contains_key(&child),
      "propose_split: child id {child} was already used (ids are single-incarnation)"
    );
    let leader = self.leader_of(parent)?;
    let host = self.hosts.get_mut(&leader).expect("leader host exists");
    let log = self.logs.get_mut(&(leader, parent)).expect("leader log");
    let stable = self.stables.get(&(leader, parent)).expect("leader stable");
    let instruction = bytes::Bytes::copy_from_slice(&point.to_le_bytes());
    let result = host.propose_split(&parent, self.now, log, stable, &child, 0, instruction)?;
    if let Ok(idx) = &result {
      let meta = self.groups.get_mut(&parent).expect("registered group");
      let child_keys = meta.keys.split_off(&point);
      // OWNERSHIP moves at acceptance; the fold ANCHORS do not. Everything here is a ROUTING
      // fact: the population shrink above and the tenure bump below hold whether or not the split
      // ever lands, because a proposed-away key is unroutable from this instant either way, and a
      // reacquisition bumps the tenure again regardless. The anchors describe CELLS, a RECORD
      // fact that changes only when `LogSm::split` actually runs — an accepted split can still be
      // deposed and truncated with its cells never leaving, so their retirement waits for the
      // parent-side partition (see [`MultiWorld::pump_forks`]).
      //
      // The same transition ENDS this group's tenure of every instruction-matched key (see
      // [`lifecycle::GroupMeta::key_epochs`]). Over the whole domain slice, not the live set, for
      // the reason the anchor purge just used: the instruction moves cells by key alone, so a
      // parked key's tenure ends here too — and a read invoked before this point must never be
      // judged against a record where the key was handed away and later handed back.
      for key in point..super::super::NUM_KEYS {
        *meta.key_epochs.entry(key).or_default() += 1;
      }
      // Record the fence coordinate (the split entry's parent-log index) so a later parked-fork
      // conflict on this child can be attributed to the index its standing capture fence sits at.
      self.split_fence_index.insert(child, *idx);
      self.pending_splits.insert(
        child,
        PendingSplit {
          parent,
          point,
          child_keys,
        },
      );
    }
    Some(result)
  }

  /// Drain every host's committed, relay-ready forks into materializations — the driver's
  /// fork-drain, played by the world. The world's CATALOG is the relay's gate, exactly as a
  /// driver's engine and coordinator are the product's: a retired id reads as spoken-for and
  /// HOLDS the fork (the container keeps it staged; a later remove/clear releases it), while a
  /// generation below the catalog's floor is a verdict about the fork and is abandoned. The
  /// world therefore keeps no hold state of its own. Returns whether anything happened.
  pub(super) fn pump_forks(&mut self) -> bool {
    let mut progressed = false;
    for node in self.node_ids.clone() {
      // The same diligent-embedder husk feed the merge service's store seam is built with, so both
      // seams answer one floor for this id on this node.
      let husk_floors = self.embedder_husk_floors(node);
      loop {
        let peeked = {
          let gate = NodeGate {
            node,
            logs: &self.logs,
            merge_floors: &self.merge_floors,
            removal_floors: &self.removal_floors,
            host_tombstones: &self.host_tombstones,
            husk_floors: &husk_floors,
          };
          let host = self.hosts.get_mut(&node).expect("host exists");
          host
            .peek_yieldable_fork(&gate)
            .map(|fork| ForkPlan::from_view(&fork))
        };
        let Some(plan) = peeked else {
          break;
        };
        progressed = true;
        // The parent's record has PARTITIONED on this host: the split applied there and this fork
        // is the half it yielded. That — not the proposal — is when the moved keys' fold ANCHORS
        // retire (see [`lifecycle::GroupMeta::fold_baselines`]), because anchors describe cells,
        // and an accepted-but-deposed split is truncated away with its cells never leaving the
        // parent (the lost-split shape the parked-key tests build). Retiring at proposal would
        // strip the anchor off content the record still holds, and a later reacquisition of that
        // key then floors the serve leg below what the invocation legitimately saw. A lost split
        // simply never reaches here, which is exactly right. Both arms below follow the apply, so
        // this sits ahead of the materialize/refuse split: the cells left either way.
        //
        // FIRST confirmation only. The purge is idempotent by itself, but a merge into the parent
        // between two hosts' drains can legitimately anchor a key at or above the point — a
        // reacquisition — and a second pass would strip that new anchor away.
        if !self.partitioned_splits.contains(&plan.child)
          && let Some(point) = self.pending_splits.get(&plan.child).map(|p| p.point)
        {
          self.partitioned_splits.insert(plan.child);
          if let Some(meta) = self.groups.get_mut(&plan.parent) {
            for (_, values) in &mut meta.fold_baselines {
              values.retain(|key, _| *key < point);
            }
            meta.fold_baselines.retain(|(_, values)| !values.is_empty());
          }
        }
        // Both the retirement and the floor arms moved INTO the container, reached through the
        // catalog gate: a retired id holds the fork, a below-floor generation abandons it. What
        // reaches here has passed both, so this is the MATERIALIZE arm — it installs the child
        // HERE, which is exact knowledge that the fork resolved, no inference: clear its fence
        // right at the wire (#110).
        //
        // THE ORDER IS THE PORT. The forked half never leaves the container, so the world can only
        // read it where it LANDS — the installed child's own state machine. Wire first, register
        // the expectations off the landed record second.
        let inherited = self.install_fork_replica(node, &plan, &husk_floors);
        self.register_split_child(&plan, &inherited);
        self.clear_fork_fence(node, plan.parent, plan.child);
      }
      // THE GUARD ADVANCES EVERY CONSUMPTION OWES, mirrored where a real driver mirrors them: in
      // its fork drain, every crank, beside the writes that drain was already making. The world
      // used to drain this queue only at a removal, which was enough while the removal-time
      // abandonment was its only producer — a redundant fold produces one too, and mirroring it a
      // teardown later leaves a restart in between restoring below the guard the container had
      // already moved.
      let advances: Vec<(u64, u64)> = {
        let host = self.hosts.get_mut(&node).expect("host exists");
        core::iter::from_fn(|| host.poll_relay_guard_advance()).collect()
      };
      for (parent, generation) in advances {
        progressed = true;
        let e = self.relayed_lineage.entry((node, parent)).or_insert(0);
        *e = (*e).max(generation);
      }
      // TERMINAL refusals: the container abandoned these deliberately, its own `resolve_fork`
      // already lifting the parent's fence. The world still clears its #110 fence record, which
      // the redundant-fold reconciliation below cannot reach (it keys on a HOSTED child).
      let refused: Vec<(u64, u64)> = {
        let host = self.hosts.get_mut(&node).expect("host exists");
        core::iter::from_fn(|| host.poll_split_refusal()).collect()
      };
      for (parent, child) in refused {
        progressed = true;
        self.split_refused += 1;
        self.clear_fork_fence(node, parent, child);
      }
      // Conflict signals are drained and counted, never acted on: see the field docs for why
      // the world's embedder model leaves a squatter in place. Beyond the count, the standing fence
      // is recorded per `(node, parent)` at the child's split index — a parked fork holds the
      // parent's capture fence there, which a later merge park on the same parent can deadlock
      // behind (issue #110, the fork-fence coupling the quiesce certifies past).
      while let Some((parent, child)) = self
        .hosts
        .get_mut(&node)
        .expect("host exists")
        .poll_split_conflict()
      {
        self.split_conflicts += 1;
        if let Some(&idx) = self.split_fence_index.get(&child) {
          self
            .fork_conflicts
            .entry((node, parent))
            .or_default()
            .insert(idx, child);
        }
      }
    }
    // The REDUNDANT-fold arm (the container resolves a fork whose child is already provenance-matched
    // on the node, WITHOUT yielding it) is invisible to the loops above. Reconcile it here, keyed on
    // the MINT TOKEN — NOT bare hostedness: `Endpoint::fork_id()` is the exact discriminator. A
    // token-BEARING hosted child is a resolved fork (a materialized fork is created with the token; the
    // redundant-fold twin fires precisely because it ALREADY carries the matching token — the catch-up
    // adoption), so its fence has resolved and clears. A token-LESS hosted child is a STANDING SQUATTER
    // — a plain CreateGroup/RecreateGroup incarnation at the fork-child id (token-less by construction),
    // the very #110 mechanism the lifecycle-churn profile builds — whose fence MUST survive. (A
    // different-token child is unconstructible at a recorded conflict: two forks onto one id refuse at
    // propose, and a removed token-bearing child's late fork hits the refuse arm.) The materialize arm
    // already cleared its own fence at the wire, so this leg carries only the redundant fold.
    let mut resolved: Vec<((u64, u64), sailing_proto::Index)> = Vec::new();
    for (&key, idxs) in &self.fork_conflicts {
      for (&idx, &child) in idxs {
        if self.hosts_group(key.0, child)
          && self.hosts[&key.0]
            .group(&child)
            .is_some_and(|ep| ep.fork_id().is_some())
        {
          resolved.push((key, idx));
        }
      }
    }
    for (key, idx) in resolved {
      if let Some(idxs) = self.fork_conflicts.get_mut(&key) {
        idxs.remove(&idx);
        if idxs.is_empty() {
          self.fork_conflicts.remove(&key);
        }
      }
    }
    progressed
  }

  /// Drop the fork-fence record `child`'s split index pinned on `(node, parent)` — the shared per-arm
  /// clear the materialize/refuse arms and the teardown seams all funnel through (#110). A no-op when
  /// the child never had a recorded fence.
  fn clear_fork_fence(&mut self, node: u64, parent: u64, child: u64) {
    if let Some(&idx) = self.split_fence_index.get(&child)
      && let Some(idxs) = self.fork_conflicts.get_mut(&(node, parent))
    {
      idxs.remove(&idx);
      if idxs.is_empty() {
        self.fork_conflicts.remove(&(node, parent));
      }
    }
  }

  /// Register a fork's child in the harness catalog on its FIRST materialization anywhere:
  /// registry entry (voters from the fork's boot config, population from the pending record,
  /// the inherited-baseline length off the fork's own manufactured half), a fresh checker
  /// floored at the fork baseline, and the conservation pair. Later materializations of the
  /// same fork on other nodes find the child registered and only wire their replica.
  ///
  /// Idempotence keys on the INCARNATION `(child, child_gen)`, not on the id: a fork held through
  /// the child's own removal-and-recreation materializes as a DIFFERENT, older incarnation than the
  /// one the registry now holds, and it needs its own expectations rather than the successor's.
  /// Keying on the id alone silently gave it the successor's — the misattribution that reads its
  /// legally inherited cells as a cross-group leak.
  fn register_split_child(
    &mut self,
    fork: &ForkPlan,
    inherited: &[(sailing_proto::Index, bytes::Bytes)],
  ) {
    if self.meta_at(fork.child, fork.child_gen).is_some() {
      // This incarnation's expectations already exist — a sibling node materialized the same fork,
      // or the id moved on and left them archived. Only the replica wiring remains, but the island
      // case needs one more thing: a LIVE checker. The retirement froze this incarnation's, and a
      // frozen archive judges NOTHING, so an island would otherwise be hosted and unjudged.
      self.revive_incarnation_checker(fork.child, fork.child_gen);
      return;
    }
    let Some(pending) = self.pending_splits.remove(&fork.child) else {
      // The proposal record is consumed by the FIRST incarnation this child registers at. A later
      // fork at a DIFFERENT incarnation of the same id has no record of its own left to take, and
      // it is not the shape the conservation pair describes — register its expectations from the
      // fork itself and leave the ledger alone.
      self.register_split_island(fork, inherited);
      return;
    };
    let parent_led = Self::ledger_id(self.generation_of(pending.parent), pending.parent);
    let child_led = Self::ledger_id(fork.child_gen, fork.child);
    // The conservation ASSIGNMENT follows the instruction rule, not the population snapshot:
    // `LogSm::split` moves EXACTLY the at-or-above-point cells of the parent's record, which
    // can include keys the propose-time population no longer carried — an earlier
    // accepted-but-lost split parks its keys, and this split's instruction moves their cells
    // anyway (cascades included: parked cells ride THIS child's record, so an onward fork
    // re-derives them for free; a refused earlier fork took its cells with it, so they never
    // inflate a later assignment). The below-point filter is the arm's teeth — a cell the FSM
    // wrongly moved from below the point stays unassigned and still trips the verdict — and
    // the population slice keeps assigned-but-never-written keys judged. The WRITABLE
    // population stays the propose-time slice: parked keys remain unroutable.
    let mut assigned = pending.child_keys.clone();
    assigned.extend(inherited.iter().filter_map(|(_, cmd)| {
      super::super::decode_gkv(cmd)
        .map(|(_, key, _)| key)
        .filter(|key| *key >= pending.point)
    }));
    let voters: BTreeSet<u64> = fork.voters.iter().copied().collect();
    // The child's GENESIS fold anchor (see [`lifecycle::GroupMeta::fold_baselines`]): the
    // inherited cells keep the PARENT's log indices and the PARENT's tag, so neither the child's
    // index-bounded reconstruction nor its tag filter can reach them — yet they are the child's
    // committed value for every key it inherited. Fold index 0: the baseline is visible from the
    // child's first instant, so no read of it can predate the anchor. Tag-agnostic per-key max
    // over the fork's own manufactured record, for the same monotone-counter reason the merge
    // anchor uses — and, symmetrically with it, over the SPLICED content itself, so the map's
    // keys are exactly what physically arrived here and nothing wider.
    let mut genesis: BTreeMap<u16, u64> = BTreeMap::new();
    for (_, cmd) in inherited {
      if let Some((_, key, value)) = super::super::decode_gkv(cmd) {
        let slot = genesis.entry(key).or_default();
        *slot = (*slot).max(value);
      }
    }
    // The child's TAG LINEAGE: its inherited baseline carries the parent's tag — and whatever
    // foreign tags the parent itself legitimately carried (its own ancestry and absorbs). The
    // baseline floor covers these cells positionally for the child's own sweep; the carried
    // set is what keeps them legitimate when a LATER MERGE moves them above another group's
    // floor (the arrival-path-independence rule, merge edition).
    let carried_tags: BTreeSet<u64> = self
      .groups
      .get(&pending.parent)
      .map(|m| {
        let mut t = m.carried_tags.clone();
        t.insert(pending.parent);
        t
      })
      .unwrap_or_default();
    self.install_incarnation_meta(
      fork.child,
      fork.child_gen,
      lifecycle::GroupMeta {
        voters,
        generation: fork.child_gen,
        carried_tags,
        keys: pending.child_keys.clone(),
        // Every replica of this incarnation opens with the same inherited record, whichever
        // path delivered it: every parent replica manufactures the fork at the same applied
        // prefix (the split entry's log position), and the blob is authoritative at
        // materialization while `LogSm::snapshot()` carries the full record to any
        // snapshot-wired latecomer. Group-level here is what keeps the cross-talk floor
        // independent of HOW a replica arrived (see `cross_talk_sweep` for why the count
        // stays sound once onward splits shrink the inherited prefix).
        fork_baseline: inherited.len(),
        fold_baselines: if genesis.is_empty() {
          Vec::new()
        } else {
          Vec::from([(0, genesis)])
        },
        ..lifecycle::GroupMeta::default()
      },
    );
    // The child's checker anchors its quorum-durability floor at the manufactured baseline:
    // the baseline's durable witnesses are the PARENT's quorum (see
    // `Checker::register_fork_baseline`), so a child materialized ahead of its siblings must
    // not be judged against its own not-yet-existing voter set. Normal creations keep the
    // full axiom — only this fork-registration path seeds the floor.
    let mut checker = Checker::new();
    checker.register_fork_baseline(sailing_proto::FORK_BASE_INDEX.get());
    assert!(
      self
        .checkers
        .insert((fork.child, fork.child_gen), checker)
        .is_none(),
      "register_split_child: child {} gen {} already had a checker",
      fork.child,
      fork.child_gen
    );
    self.splits_applied += 1;
    self.splits.insert(
      fork.child,
      SplitRecord {
        parent_led,
        child_led,
        child_keys: assigned,
      },
    );
  }

  /// Install one incarnation's expectation meta where it belongs: the live registry when it IS the
  /// id's current incarnation, the archive when the id has already moved past it. Routing rather
  /// than a bare insert is what keeps a late fork from overwriting its own successor's registry
  /// entry with the dead incarnation's expectations.
  fn install_incarnation_meta(&mut self, gid: u64, generation: u64, meta: lifecycle::GroupMeta) {
    match self.groups.get(&gid) {
      Some(live) if live.generation != generation => {
        self.meta_archive.insert((gid, generation), meta);
      }
      _ => {
        self.groups.insert(gid, meta);
      }
    }
  }

  /// Give `(gid, generation)` a LIVE checker if it has none — the island's judge. Deliberately
  /// FRESH rather than the frozen archive copy: every replica of a materializing fork boots at the
  /// manufactured baseline, so that is the right quorum-durability anchor, while the frozen
  /// checker's per-node history describes replicas the retirement destroyed. The frozen copy stays
  /// archived and still faces the run-end pass.
  fn revive_incarnation_checker(&mut self, gid: u64, generation: u64) {
    if self.checkers.contains_key(&(gid, generation)) {
      return;
    }
    let mut checker = Checker::new();
    checker.register_fork_baseline(sailing_proto::FORK_BASE_INDEX.get());
    self.checkers.insert((gid, generation), checker);
  }

  /// A fork materializing at an incarnation the world has no proposal record for: the record was
  /// consumed by the FIRST incarnation to register at this id, so this one derives its expectations
  /// from the fork alone. No conservation pair is registered — the partition this fork carries was
  /// already accounted when the ledger's pair was created, and a second pair would demand the same
  /// cells twice.
  fn register_split_island(
    &mut self,
    fork: &ForkPlan,
    inherited: &[(sailing_proto::Index, bytes::Bytes)],
  ) {
    let carried: BTreeSet<u64> = self
      .groups
      .get(&fork.parent)
      .map(|m| {
        let mut t = m.carried_tags.clone();
        t.insert(fork.parent);
        t
      })
      .unwrap_or_default();
    let mut genesis: BTreeMap<u16, u64> = BTreeMap::new();
    let mut keys: BTreeSet<u16> = BTreeSet::new();
    for (_, cmd) in inherited {
      if let Some((_, key, value)) = super::super::decode_gkv(cmd) {
        let slot = genesis.entry(key).or_default();
        *slot = (*slot).max(value);
        keys.insert(key);
      }
    }
    self.install_incarnation_meta(
      fork.child,
      fork.child_gen,
      lifecycle::GroupMeta {
        voters: fork.voters.iter().copied().collect(),
        generation: fork.child_gen,
        carried_tags: carried,
        keys,
        fork_baseline: inherited.len(),
        fold_baselines: if genesis.is_empty() {
          Vec::new()
        } else {
          Vec::from([(0, genesis)])
        },
        ..lifecycle::GroupMeta::default()
      },
    );
    self.revive_incarnation_checker(fork.child, fork.child_gen);
  }

  /// Install one fork on `node` and return the record the child LANDED with — the world's only
  /// view of the forked half, because the half never leaves the container. The container makes the
  /// child's stores and mints its boot epoch inside the call (occupancy is a decision-time fact, so
  /// creating the storage first would hold the fork against its own install); this adds the
  /// per-replica bookkeeping around it and lifts the barrier, which sync stores make durable at the
  /// call. Deliberately NO oracle-view seeding here: the aligned view is a pure content rule
  /// ([`align_record`](Self::align_record)), the cross-talk floor derives from the group
  /// registration record (`GroupMeta::fork_baseline`), and the conservation recorder starts at 0 so
  /// the baseline is observed as the child's opening history — a snapshot-wired latecomer gets the
  /// identical treatment without ever passing through this path.
  fn install_fork_replica(
    &mut self,
    node: u64,
    fork: &ForkPlan,
    husk_floors: &BTreeSet<u64>,
  ) -> Vec<(sailing_proto::Index, bytes::Bytes)> {
    let child = fork.child;
    // Record the parent's DURABLE relay lineage on this node — mirroring a real driver's
    // `engine.set_group_gen(&parent, parent_gen_after)` in its fork drain — so a later restart
    // restores the container's relay guard past this now-materialized fork (see `relayed_lineage`).
    {
      let e = self.relayed_lineage.entry((node, fork.parent)).or_insert(0);
      *e = (*e).max(fork.parent_gen_after);
    }
    let (split_at, installed_config) = {
      let mut engine = NodeEngine {
        node,
        logs: &mut self.logs,
        stables: &mut self.stables,
        boot_epochs: &mut self.boot_epochs,
        merge_floors: &self.merge_floors,
        removal_floors: &self.removal_floors,
        husk_floors,
        store_mode: self.store_mode,
        seed: self.seed,
      };
      // The per-node tombstone is embedder state no storage engine holds, so it rides in as the
      // install's `extra` gate — exactly the coordinators' `retired` augmentation.
      let tombstones = TombstoneGate {
        node,
        host_tombstones: &self.host_tombstones,
      };
      let outcome = self
        .hosts
        .get_mut(&node)
        .expect("host exists")
        .install_yieldable_fork(
          &fork.parent,
          &child,
          &mut engine,
          &tombstones,
          self.now,
          self.seed ^ node,
        );
      match outcome {
        sailing_proto::InstallOutcome::Installed {
          split_index,
          config,
          ..
        } => (split_index, config),
        other => panic!("fork of group {child} on node {node} did not install: {other:?}"),
      }
    };
    // THE CONFIG THE CONTAINER INSTALLED, not the one it derived the plan from: the install applies
    // `reshape_born_prevention` on the way in, so recording the untransformed one would restart this
    // replica with pre-vote and check-quorum OFF while its peers keep them on.
    self.configs.insert((node, child), installed_config);
    self.member_view.insert((node, child), true);
    // A fork-born replica speaks for the incarnation the SPLIT named, not for whatever the registry
    // holds now: a fork that materializes late lands as its own (possibly superseded) incarnation,
    // and binding the registry here would silently promote it into the successor's identity.
    self.replica_gen.insert((node, child), fork.child_gen);
    self.host_tombstones.remove(&(node, child));
    *self.restarts.entry((node, child)).or_insert(0) += 1;
    self
      .hosts
      .get_mut(&node)
      .expect("host exists")
      .lift_fork_barrier(&fork.parent, split_at);

    // THE LANDED HALF, and the pin that makes reading it here equivalent to reading the fork's own
    // copy. The child was restored from `encode(half.snapshot())`, so the two agree exactly as long
    // as `LogSm`'s snapshot round-trip is lossless — and `fork_baseline` is a COUNT over this record
    // while `genesis` is a per-key MAX over it, so a lossy encode would shift the oracle's floors
    // silently instead of failing. Assert it rather than assume it.
    let sm = self
      .hosts
      .get(&node)
      .expect("host exists")
      .group(&child)
      .expect("the fork just installed")
      .state_machine();
    let inherited = sm.applied().to_vec();
    let round_trip = {
      let blob = sailing_proto::StateMachine::snapshot(sm).expect("the sim FSM always snapshots");
      let mut vessel = LogSm::default();
      sailing_proto::StateMachine::restore(&mut vessel, blob)
        .expect("the sim FSM restores its own snapshot");
      vessel.applied().to_vec()
    };
    assert_eq!(
      round_trip, inherited,
      "group {child} on node {node}: the FSM snapshot round-trip is lossy, so the oracle's \
       fork baseline and genesis anchor would silently disagree with the record that landed"
    );
    inherited
  }

  /// The ORACLE-ALIGNED applied record for `(node, gid)`:
  /// [`align_record`](Self::align_record) over the raw record under the group's live key
  /// population. The dropped cells are not unjudged: the conservation ledger compares inherited
  /// cells exact-cell across the handover, and quiesce equality reads the raw records once
  /// every replica converged. For a group that never split this is the raw record verbatim
  /// (own-tagged cells, full domain); an unregistered gid aligns as itself.
  ///
  /// A RETIRED source's live population is emptied at merge resolution, but a lagging husk replica
  /// still hosts its record and stays inside the safety sweep — so align against the TERMINAL
  /// pre-merge population when the live set is empty and one was stashed, else every husk record would
  /// align gkv-empty and its consumer would certify vacuously. The terminal set is the
  /// source's final owned population (post-every-split, pre-merge), so the split-erasure argument is
  /// unchanged: split-away keys stay filtered from every replica's view, pre-split husk replicas
  /// included, and every husk replica of the lineage aligns against the SAME set — views still differ
  /// only by watermark. This heals the aligned consumer (`agreement_holds`' non-absorbed positional
  /// branch for plain-source husks) at one seam.
  ///
  /// The population is `generation`'s OWN — [`meta_at`](Self::meta_at) resolves it live or
  /// archived — never whatever incarnation the registry currently holds. A late fork lands as its
  /// own superseded incarnation beside a live successor, and the two own different key sets:
  /// aligning the island against the successor's population drops cells the island legally owns
  /// and keeps cells it never did, both silently. Callers walking hosts pass the REPLICA's bound
  /// generation ([`replica_gen_of`](Self::replica_gen_of)); callers judging one named incarnation
  /// pass that one, having already filtered the walk to it.
  pub(super) fn aligned_applied(&self, node: u64, gid: u64, generation: u64) -> AppliedLog {
    let raw = self.applied_of(node, gid);
    match self.meta_at(gid, generation) {
      Some(meta) => {
        let population = match &meta.terminal_keys {
          Some(terminal) if meta.keys.is_empty() => terminal,
          _ => &meta.keys,
        };
        Self::align_record(raw, gid, population)
      }
      None => raw,
    }
  }

  /// Align one raw applied record into the oracle's comparison space: keep exactly the group's
  /// OWN cells for keys it still owns. Per cell:
  ///   - a gkv cell tagged with ANOTHER gid is fork-inherited — dropped. The tag test is exact:
  ///     ids are single-incarnation and a child's id is minted strictly after every cell it
  ///     inherits existed, so no ancestor tag can collide with `gid`, and the cross-talk oracle
  ///     pins every own-applied gkv cell to `tag == gid`. (Inherited cells also carry
  ///     PARENT-log indices, which would poison the index-keyed rewrite high-water.)
  ///   - an own-tagged cell whose key left the live population is given away — dropped. The
  ///     population flips at PROPOSE, before any replica can apply that split, so this drops
  ///     from a lagging replica's view exactly the cells `LogSm::split` already removed from an
  ///     ahead replica's record.
  ///   - a non-gkv cell never moves in a split — kept.
  ///
  /// No positional state anywhere is what buys the invariance the agreement oracle needs: a
  /// replica that applied split set S of this incarnation at applied index k holds
  /// `remove_S(baseline) ++ remove_S(own(k))`. The tag test erases `remove_S(baseline)` for
  /// any S, and the population test collapses `remove_S(own(k))` to `filter(own(k))` for any S
  /// (every split in any replica's S flipped the population before that replica could apply
  /// it), so every replica's aligned view is `filter(own(k))` — the views agree at every LOG
  /// INDEX they share, whatever the replication lag or number of onward splits. Under split-lag
  /// the application is index-ordered, so the shorter view is additionally an exact PREFIX of the
  /// longer — the positional relation the non-absorbed [`agreement_holds`](Self::agreement_holds)
  /// branch reads. An ABSORBED lineage whose replicas sit at UNEQUAL watermarks has NO per-index
  /// cross-watermark leg — indices are not cell identities under `LogSm::absorb` (it extends the record
  /// at the source's indices; see `assert_group_safety`), so that content is judged off-band by the
  /// run-end conservation ledger and, at equal watermarks, by `agreement_holds`' sorted absorbed branch.
  pub(super) fn align_record(raw: AppliedLog, gid: u64, population: &BTreeSet<u16>) -> AppliedLog {
    raw
      .into_iter()
      .filter(|(_, cmd)| match super::super::decode_gkv(cmd) {
        Some((tag, key, _)) => tag == gid && population.contains(&key),
        None => true,
      })
      .collect()
  }

  /// Record every replica's applied gkv cells into the conservation ledger (see the module
  /// docs for the dedupe and ordering argument). Cells are recorded under the group HOLDING
  /// them — the payload's gid tag is the cross-talk oracle's business.
  ///
  /// A FULL walk every sweep, on purpose: each value is globally unique, so the per-`(ledger,
  /// key)` recorded-value SET admits each cell exactly once and a re-walk is pure no-op
  /// re-presentation. A set rather than a value watermark because a merge folds a source's cells
  /// under the target's ledger id BELOW the target's own values — a watermark would drop them as
  /// stale though each is a distinct cell of a different lineage. Any positional resume shortcut
  /// has a hole this oracle cannot afford either: a split-apply shifts kept cells below where a
  /// watermark sat within one settle window, and a crash restore resurrects moved cells a
  /// pre-crash sweep never met — both skip real cells forever. The world already pays O(record)
  /// per replica sweep to clone the record, so the walk adds no asymptotic cost.
  ///
  /// ONE INCARNATION per sweep. The ledger id is built from `generation`, and the walk keeps only
  /// the replicas BOUND to it: a late fork's island holds the dead incarnation's cells, and
  /// recording them under the registry's current generation files them as the SUCCESSOR's history.
  /// That is a live false positive, not bookkeeping noise — an onward split off the successor takes
  /// that ledger id as its `parent_led`, and the finalizer then demands the grandchild's record open
  /// with cells the island never handed it. Every hosted replica still faces a sweep: `check_now`
  /// runs one per LIVE checker key, and every incarnation that can host has one (a recreation
  /// installs it, a materializing island revives it).
  pub(super) fn conserve_sweep(&mut self, gid: u64, generation: u64) {
    let led = Self::ledger_id(generation, gid);
    for node in self.node_ids.clone() {
      if !self.hosts[&node].contains_group(&gid) {
        continue;
      }
      if self.replica_gen_of(node, gid) != generation {
        continue;
      }
      let applied = self.applied_of(node, gid);
      for (index, cmd) in &applied {
        if let Some((_, key, value)) = super::super::decode_gkv(cmd) {
          // Record each distinct value once, in first-encounter order: `insert` is true only the
          // first time a value is seen, so full re-walks re-present cells harmlessly, and an
          // absorbed cell folded below the target's own values is recorded rather than shadowed.
          if self
            .cons_recorded
            .entry((led, key))
            .or_default()
            .insert(value)
          {
            self.conservation.record(led, key, *index, value);
          }
        }
      }
    }
  }

  /// Judge every registered split's conservation: the child's recorded history must open with
  /// the parent's full recorded history for each assigned key, exactly one side continues each
  /// key, and no unassigned key ever surfaces in the child. Sound at any quiescent point;
  /// [`run_multi_vopr`](crate::run_multi_vopr) runs it at run end beside the membership verdict.
  /// Incarnation-qualified ledger ids keep the verdict exact even when a recorded pair's gid is
  /// later recreated — the fresh incarnation records under a different id.
  pub fn finalize_conservation_or_panic(&self, seed: u64) {
    for rec in self.splits.values() {
      let ctx = ReplayContext {
        seed,
        parent_led: rec.parent_led,
        child_led: rec.child_led,
      };
      // The split-merge algebra reunifies sides through registered unions; the two exemption
      // sets are the keys a merge re-routed, read off the merge records' transferred POPULATION
      // (`absorbed_keys`). A written-history set would miss a key the absorbing side owned but
      // never wrote and only writes post-absorb — the cross-talk leg's parent-merged-into-child
      // gap. Both are one-hop: a source folds every earlier absorb into the population it hands
      // on, so the transferred set is already transitive.
      //
      // `absorbed` — keys a union carried INTO this child (the child became a merge target): the
      // cross-talk exemption.
      let absorbed: BTreeSet<u16> = self
        .merges
        .iter()
        .filter(|m| m.target_led == rec.child_led)
        .flat_map(|m| m.absorbed_keys.iter().copied())
        .collect();
      // `reacquired` — keys a union re-introduced INTO this parent (the parent became a merge
      // target): the LOSS exemption, symmetric to `absorbed`.
      let reacquired: BTreeSet<u16> = self
        .merges
        .iter()
        .filter(|m| m.target_led == rec.parent_led)
        .flat_map(|m| m.absorbed_keys.iter().copied())
        .collect();
      // `inherited` — the parent-side CELL-level exemption, deliberately population-blind. An
      // absorb copies the source's whole FSM RECORD into the target — including cells for keys
      // OUTSIDE the source's owned population (carried/foreign-tagged cells) — and the sweep
      // records them under the target, so an assigned key's cells can reach this parent through a
      // union whose `absorbed_keys` never named the key (`reacquired` above is blind to them).
      // Collect, per assigned key, every value in ANY inbound union source's record: values are
      // globally unique, so such a value reached the parent's record only via that absorb, and a
      // fresh parent own-write can never match one.
      let mut inherited: BTreeMap<u16, BTreeSet<u64>> = BTreeMap::new();
      for m in self
        .merges
        .iter()
        .filter(|m| m.target_led == rec.parent_led)
      {
        for &k in &rec.child_keys {
          let hist = self.conservation.history(m.source_led, k);
          if !hist.is_empty() {
            inherited
              .entry(k)
              .or_default()
              .extend(hist.iter().map(|&(_, v)| v));
          }
        }
      }
      // `child_inherited` — the CHILD-side CELL-level exemption, the mirror of `inherited`. The
      // same whole-record absorb carries cells of keys OUTSIDE the transferred population into a
      // child that became a merge target (a parked key's cells ride the source's record), and the
      // sweep records them under the child though `absorbed` never named the key. Collect, per key
      // the source recorded, every value in its record: values are globally unique, so such a value
      // reached the child only via that absorb, and a child own-write of an unassigned key can
      // never match one.
      let mut child_inherited: BTreeMap<u16, BTreeSet<u64>> = BTreeMap::new();
      for m in self.merges.iter().filter(|m| m.target_led == rec.child_led) {
        for k in self.conservation.keys_of(m.source_led) {
          let hist = self.conservation.history(m.source_led, k);
          if !hist.is_empty() {
            child_inherited
              .entry(k)
              .or_default()
              .extend(hist.iter().map(|&(_, v)| v));
          }
        }
      }
      self.conservation.assert_partition(
        rec.parent_led,
        rec.child_led,
        &rec.child_keys,
        &absorbed,
        &reacquired,
        &inherited,
        &child_inherited,
      );
      drop(ctx); // no panic: disarm silently
    }
  }
}

/// Everything the world needs off a peeked [`ForkView`](sailing_proto::ForkView), copied out so the
/// container's borrow ends before the install. The forked half is deliberately absent: it never
/// leaves the container, and the world reads it off the child that lands.
struct ForkPlan {
  parent: u64,
  child: u64,
  child_gen: u64,
  parent_gen_after: u64,
  voters: Vec<u64>,
}

impl ForkPlan {
  fn from_view(view: &sailing_proto::ForkView<'_, u64, u64>) -> Self {
    Self {
      parent: *view.parent(),
      child: *view.child(),
      child_gen: view.child_gen(),
      parent_gen_after: view.parent_gen_after(),
      // The VOTERS are all the world needs off the peeked config; the config it RECORDS is the one
      // the install hands back, which is the transformed one the child actually booted with.
      voters: view.config().voters().to_vec(),
    }
  }
}

/// The per-node TOMBSTONE, handed to the install as its `extra` gate: embedder state no storage
/// engine holds, exactly as the coordinators' `retired` set is. A tombstone is a window — a later
/// `clear_tombstone` lifts it — so it HOLDS the fork and never floors it.
struct TombstoneGate<'a> {
  node: u64,
  host_tombstones: &'a BTreeSet<(u64, u64)>,
}

impl sailing_proto::ForkGate<u64> for TombstoneGate<'_> {
  fn contains_group(&self, gid: &u64) -> bool {
    self.host_tombstones.contains(&(self.node, *gid))
  }

  fn floor(&self, _gid: &u64) -> u64 {
    0
  }
}

/// The world's per-node storage as the container's [`MultiEngine`](sailing_proto::MultiEngine)
/// seam — what the fork install makes the child's stores and mints its boot epoch through.
/// Occupancy and floors answer exactly what [`NodeGate`] answers off the same maps for the same id
/// on the same node; only the tombstone is elsewhere (see [`TombstoneGate`]). New storage goes
/// through the world's own [`fresh_stores`](MultiWorld::fresh_stores) chokepoint, so a fork-born
/// child's stores match every other replica's rather than silently reverting to synchronous.
struct NodeEngine<'a> {
  node: u64,
  logs: &'a mut BTreeMap<(u64, u64), crate::MemLog>,
  stables: &'a mut BTreeMap<(u64, u64), crate::MemStable<u64>>,
  /// The world's per-NODE monotone boot-epoch counter: strictly above every prior incarnation on
  /// this node, and `>= 1` as the manufactured baseline requires.
  boot_epochs: &'a mut BTreeMap<u64, u64>,
  merge_floors: &'a BTreeSet<(u64, u64)>,
  removal_floors: &'a BTreeMap<u64, u64>,
  husk_floors: &'a BTreeSet<u64>,
  store_mode: crate::StoreMode,
  seed: u64,
}

impl sailing_proto::GroupStores<u64, crate::MemLog, crate::MemStable<u64>> for NodeEngine<'_> {
  fn stores(&mut self, group: &u64) -> Option<(&mut crate::MemLog, &mut crate::MemStable<u64>)> {
    let log = self.logs.get_mut(&(self.node, *group))?;
    let stable = self.stables.get_mut(&(self.node, *group))?;
    Some((log, stable))
  }
}

impl sailing_proto::FloorStore<u64> for NodeEngine<'_> {
  fn floor(&self, gid: &u64) -> u64 {
    if self.merge_floors.contains(&(self.node, *gid)) || self.husk_floors.contains(gid) {
      return sailing_proto::MERGED_FLOOR;
    }
    self.removal_floors.get(gid).copied().unwrap_or(0)
  }

  fn lineage(&self, _gid: &u64) -> u64 {
    0
  }
}

impl sailing_proto::MultiEngine<u64, u64> for NodeEngine<'_> {
  type Log = crate::MemLog;
  type Stable = crate::MemStable<u64>;

  fn set_snapshot_staging_cap(&mut self, _cap: usize) {}

  fn group_ids(&self) -> impl Iterator<Item = &u64> {
    self
      .logs
      .keys()
      .filter(|(n, _)| *n == self.node)
      .map(|(_, gid)| gid)
  }

  // The world drives its stores directly and re-derives its own lineage records, so the batching
  // metrics, the visibility barrier and the lineage legs are inert here.
  fn barriers(&self) -> u64 {
    0
  }

  fn ops_batched(&self) -> u64 {
    0
  }

  fn has_staged(&self) -> bool {
    false
  }

  fn flush(&mut self) -> usize {
    0
  }

  fn add_group(&mut self, gid: u64) -> bool {
    if self.logs.contains_key(&(self.node, gid)) {
      return false;
    }
    let (log, stable) = MultiWorld::fresh_stores_in(self.store_mode, self.seed, self.node, gid);
    self.logs.insert((self.node, gid), log);
    self.stables.insert((self.node, gid), stable);
    true
  }

  fn remove_group(&mut self, gid: &u64) -> bool {
    self.stables.remove(&(self.node, *gid));
    self.logs.remove(&(self.node, *gid)).is_some()
  }

  fn contains_group(&self, gid: &u64) -> bool {
    self.logs.contains_key(&(self.node, *gid))
  }

  fn next_boot_epoch(&mut self, gid: &u64) -> Option<u64> {
    if !self.logs.contains_key(&(self.node, *gid)) {
      return None;
    }
    let epoch = self.boot_epochs.entry(self.node).or_default();
    *epoch = epoch.checked_add(1)?;
    Some(*epoch)
  }

  fn set_group_floor(&mut self, _gid: &u64, _floor: u64) {}

  fn set_group_gen(&mut self, _gid: &u64, _generation: u64) {}

  fn removal_floor(&self, _gid: &u64) -> u64 {
    0
  }
}

/// One NODE's view, presented as the fork relay's gate — the sim's stand-in for that node's engine
/// plus its coordinator, and node-local for the same reason production is: a relay drain runs on
/// one host and can only see what that host holds.
///
/// - OCCUPANCY is this node's own stores for the id, or THIS NODE's tombstone (the coordinator's
///   own per-host `retired` set, set when this host's removal committed). A tombstone is a window
///   — `clear_tombstone` lifts it — never a verdict, so it HOLDS.
/// - The FLOOR is the id's persisted admission floor as this node knows it, answering EXACTLY what
///   [`NodeStores::floor`](super::merge::NodeStores) answers for the same id on the same node: the
///   reserved terminal for an id merged away here OR surfaced by the diligent-embedder husk feed,
///   else the removal floor a reshaping teardown wrote. One id on one node must not read one floor
///   at the fork gate and another at the merge service — production reads a single engine record
///   through both seams. Deliberately NOT the incarnation counter — a never-reshaped child's
///   removal writes no floor, so its fork holds rather than being abandoned, and a merged-away
///   child terminal-refuses, both exactly as production does off the engine's own record. (The
///   per-node/cluster scale distinction the removal leg draws is unchanged: the terminal legs are
///   per-node facts, the removal floor a cluster-wide one.)
struct NodeGate<'a> {
  node: u64,
  logs: &'a std::collections::BTreeMap<(u64, u64), crate::MemLog>,
  merge_floors: &'a std::collections::BTreeSet<(u64, u64)>,
  removal_floors: &'a std::collections::BTreeMap<u64, u64>,
  host_tombstones: &'a std::collections::BTreeSet<(u64, u64)>,
  husk_floors: &'a std::collections::BTreeSet<u64>,
}

impl sailing_proto::ForkGate<u64> for NodeGate<'_> {
  fn contains_group(&self, gid: &u64) -> bool {
    self.logs.contains_key(&(self.node, *gid)) || self.host_tombstones.contains(&(self.node, *gid))
  }

  fn floor(&self, gid: &u64) -> u64 {
    if self.merge_floors.contains(&(self.node, *gid)) || self.husk_floors.contains(gid) {
      return sailing_proto::MERGED_FLOOR;
    }
    self.removal_floors.get(gid).copied().unwrap_or(0)
  }
}
