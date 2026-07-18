//! The merge lifecycle verbs, the per-tick merge pump, and the merge registration — the
//! world-side wiring of the absorb choreography, playing the DRIVER's role from the reactor
//! crank: the verbs delegate to the container's gate stack on the mutated group's leader, the
//! pump runs `service_merge_applies` on every host each settle pass, and each host's `Merged`
//! resolution folds the driver's storage half (replica teardown; the terminal floor is the
//! catalog's `merged` mark, which [`MultiWorld::recreate_group`] refuses forever).
//!
//! # Registration, populations, and the oracles
//!
//! The FIRST resolution anywhere REGISTERS the merge: the source's key population folds into
//! the target's (writes/reads route to the target from then on — between the freeze and the
//! resolution the frozen source refuses them, deterministic no-ops the actions tolerate), the
//! target's absorbed-tag set inherits the source's tag plus everything IT had absorbed (the
//! cross-talk sweep's legitimacy set), the source's checker is frozen into the retired archive
//! (a group being dismantled host-by-host must not be live-judged against a shrinking view —
//! the fork-transient lesson's shape), and the source's registry entry turns `merged` —
//! terminal, the harness twin of the product's `u64::MAX` floor.
//!
//! # The absorb-determinism oracle
//!
//! Every replica must absorb the IDENTICAL source state: the product argument is that nothing
//! FSM-mutating can follow a surviving freeze, so the extracted FSM at any applied-past-boundary
//! point equals the state at the boundary. The pump checks it DIRECTLY: at each host's `Merged`
//! resolution the target's applied record (applied == the parked index exactly, nothing else
//! ran) is compared byte-for-byte against the record the FIRST resolution registered — any
//! divergence in any replica's absorbed union panics with the host and seed.

use super::*;

/// One registered merge: the union verdict's unit plus the determinism reference.
pub(super) struct MergeRecord {
  /// The absorbed source's incarnation-qualified ledger id.
  pub(super) source_led: u64,
  /// The absorbing target's incarnation-qualified ledger id.
  pub(super) target_led: u64,
  /// The target group id (the union side judged at finalize).
  pub(super) target: u64,
  /// The source's key POPULATION at the merge — the keys this union handed to the target. This
  /// is the target's absorbed-key ENTITLEMENT the conservation partition verdict reads, and it
  /// is what the written-history set (`ConservationLedger::keys_of`) cannot supply: the source
  /// can OWN a key it never wrote, and the target then writes that key for the first time
  /// post-absorb — legitimate, but invisible to a history-only exemption. Already transitive: a
  /// source that itself absorbed earlier merges folded their keys into its population, so this
  /// carries them too (no per-hop walk needed).
  pub(super) absorbed_keys: BTreeSet<u16>,
  /// The target's FULL applied record at the FIRST resolution — the absorb-determinism
  /// reference every later host's resolution is compared against.
  pub(super) resolved_record: AppliedLog,
  /// The committed absorb coordinate (the parked index, identical on every replica of the
  /// target's log) — the diligent-embedder floor feed's eligibility line: a host whose target
  /// replica sits BELOW it has not folded the union yet and must never see the terminal floor.
  pub(super) boundary: sailing_proto::Index,
}

/// Per-node `(log, stable)` resolution over the world's split store maps — the driver's
/// `GroupStores` seam, borrowed for one host's service call.
pub(super) struct NodeStores<'a> {
  pub(super) node: u64,
  pub(super) logs: &'a mut BTreeMap<(u64, u64), MemLog>,
  pub(super) stables: &'a mut BTreeMap<(u64, u64), MemStable<u64>>,
  /// The host's terminal merge floors (recorded when a source's deferred teardown lands).
  pub(super) floored: &'a BTreeSet<(u64, u64)>,
  /// The cluster-wide non-terminal removal floors (one past a reshaped id's removal ceiling).
  pub(super) removal_floors: &'a BTreeMap<u64, u64>,
  /// The diligent-embedder floor feed for THIS node (see
  /// [`MultiWorld::embedder_husk_floors`]): registered-absorbed sources whose local replica is
  /// a husk nothing here still needs, surfaced at the terminal floor so the service's dissolve
  /// arm can reclaim them — the catalog action pin B(a) models with its direct floor write.
  pub(super) husk_floors: &'a BTreeSet<u64>,
}

impl sailing_proto::GroupStores<u64, MemLog, MemStable<u64>> for NodeStores<'_> {
  fn stores(&mut self, group: &u64) -> Option<(&mut MemLog, &mut MemStable<u64>)> {
    let log = self.logs.get_mut(&(self.node, *group))?;
    let stable = self.stables.get_mut(&(self.node, *group))?;
    Some((log, stable))
  }
}

impl sailing_proto::FloorStore<u64> for NodeStores<'_> {
  fn floor(&self, gid: &u64) -> u64 {
    // The catalog's floor is the MAX of three legs: the terminal merge leg THIS host folded (a
    // source it locally resolved), the diligent-embedder husk feed (boundary-aware — see
    // `embedder_husk_floors` for why it must never reach a host still short of the union), and
    // the non-terminal removal leg (a reshaped id the world stopped hosting).
    let terminal = if self.floored.contains(&(self.node, *gid)) || self.husk_floors.contains(gid) {
      sailing_proto::MERGED_FLOOR
    } else {
      0
    };
    terminal.max(self.removal_floors.get(gid).copied().unwrap_or(0))
  }

  fn lineage(&self, _gid: &u64) -> u64 {
    0
  }
}

impl MultiWorld {
  /// Propose the merge FREEZE of `source` into `target` on the source's current leader;
  /// returns the container verdict verbatim (`None` while leaderless). Every refusal arm is a
  /// legitimate no-op tick for the fuzzer. The whole host store map rides along as the
  /// container's `GroupStores` seam — the claimed-target gate reads co-hosted claimants' logs.
  pub fn propose_prepare_merge(
    &mut self,
    source: u64,
    target: u64,
  ) -> Option<Result<sailing_proto::Index, sailing_proto::MergeError<u64>>> {
    let leader = self.leader_of(source)?;
    // The propose path never consults floors (the container is floor-free by design); the
    // seam's husk feed is empty here.
    let no_husks = BTreeSet::new();
    let host = self.hosts.get_mut(&leader).expect("leader host exists");
    let mut stores = NodeStores {
      node: leader,
      logs: &mut self.logs,
      stables: &mut self.stables,
      floored: &self.merge_floors,
      removal_floors: &self.removal_floors,
      husk_floors: &no_husks,
    };
    let verdict = host.prepare_merge(&source, self.now, &mut stores, &target);
    // Record the embedder's freeze intent on acceptance: this source now claims this target, so the
    // choreography predicate keeps the target off the removal draws (the container's `Claimed` gate).
    if matches!(verdict, Some(Ok(_))) {
      self.active_freezes.insert(source, target);
    }
    verdict
  }

  /// Propose the merge ABSORB on the target's current leader; returns the container verdict
  /// verbatim (`None` while leaderless).
  pub fn propose_commit_merge(
    &mut self,
    target: u64,
    source: u64,
  ) -> Option<Result<sailing_proto::Index, sailing_proto::MergeError<u64>>> {
    let leader = self.leader_of(target)?;
    let host = self.hosts.get_mut(&leader).expect("leader host exists");
    let log = self.logs.get_mut(&(leader, target)).expect("leader log");
    let stable = self.stables.get(&(leader, target)).expect("leader stable");
    host.commit_merge(&target, self.now, log, stable, &source)
  }

  /// Propose the merge ABORT on the TARGET's current leader (the abort rides the target's log,
  /// totally ordered against the commit it races); returns the container verdict verbatim
  /// (`None` while leaderless). The source's thaw is the relayed consequence the pump drains.
  pub fn propose_rollback_merge(
    &mut self,
    target: u64,
    source: u64,
  ) -> Option<Result<sailing_proto::Index, sailing_proto::MergeError<u64>>> {
    let leader = self.leader_of(target)?;
    let host = self.hosts.get_mut(&leader).expect("leader host exists");
    let log = self.logs.get_mut(&(leader, target)).expect("leader log");
    let stable = self.stables.get(&(leader, target)).expect("leader stable");
    host.rollback_merge(&target, self.now, log, stable, &source)
  }

  /// Run every host's merge service — the driver's per-crank call, played inside the settle
  /// loop so a park resolved by this very tick's deliveries folds before the tick's oracle
  /// pass. Returns whether anything resolved.
  pub(super) fn pump_merges(&mut self) -> bool {
    let mut progressed = false;
    for node in self.node_ids.clone() {
      let now = self.now;
      let husk_floors = self.embedder_husk_floors(node);
      let resolutions = {
        let host = self.hosts.get_mut(&node).expect("host exists");
        let mut stores = NodeStores {
          node,
          logs: &mut self.logs,
          stables: &mut self.stables,
          floored: &self.merge_floors,
          removal_floors: &self.removal_floors,
          husk_floors: &husk_floors,
        };
        host.service_merge_applies(now, &mut stores)
      };
      for r in resolutions {
        progressed = true;
        match r {
          sailing_proto::MergeResolution::Merged { source, target } => {
            self.merges_resolved += 1;
            let boundary = self
              .hosts
              .get(&node)
              .and_then(|h| h.group(&target))
              .map_or(sailing_proto::Index::ZERO, |ep| ep.applied_index());
            self.register_or_check_merge(node, source, target, boundary);
            // The driver's teardown half — DEFERRED behind the capture's durability, modeling
            // the engine's one-barrier batch: floor, teardown, and the absorb capture land
            // together or not at all. Dropping the source stores at resolution time would make
            // the teardown durable AHEAD of the capture (the world's store drop is immediate),
            // and a crash inside the capture's fsync window would then restart the target
            // parked with its source unrestorable — the abort arm would skip the union and
            // silently diverge. Kept pending until the target's durable snapshot covers the
            // absorb; a crash before that re-parks the restored target, which waits on its
            // extracted source until the resolved quorum's post-merge install supersedes the
            // park — the same install's durability then completes this entry.
            self
              .pending_merge_teardowns
              .push((node, source, target, boundary));
          }
          sailing_proto::MergeResolution::Aborted { .. } => {
            self.merges_aborted += 1;
          }
          sailing_proto::MergeResolution::CaptureFailed { .. } => {
            // The absorb consumed the source endpoint but the union could not be made durable (the
            // target refused the absorb, or its forced capture faulted). UNLIKE `Merged`/`Retired`,
            // PRESERVE the source: do NOT floor it and do NOT drop its replica — its stores hold the
            // union's only copy and a restart re-parks the merge against them. The absorb-capable,
            // non-faulting sim FSM never reaches here; count it so a real occurrence is visible.
            self.merges_capture_failed += 1;
          }
          sailing_proto::MergeResolution::Retired { source } => {
            // A hosted frozen husk of a terminally-absorbed lineage dissolved locally — no absorbing
            // target and no capture. Fold the SAME source half as `Merged` MINUS the capture: record
            // the terminal floor DURABLY and drop the source stores, CO-BARRIERED. Unlike `Merged` this
            // needs no deferral (there is no capture to wait on), but the floor re-write is MANDATORY
            // even when the husk was already at `MERGED_FLOOR` — a crash after the store drop but before
            // a durable floor would re-admit the id below its gen (the resurrection the restore pin
            // checks). The world's `merge_floors` IS the durable terminal floor.
            self.merge_floors.insert((node, source));
            self.drop_group_replica(source, node);
          }
        }
      }
    }
    // The source thaw now rides `service_merge_applies` above (the per-crank container service
    // derives it from each target's durable `abandoned` record and appends the source-side
    // RollbackMerge on the source's own log) — no separate relay drain. The append rides the next
    // tick's `flush_appends` and settles like any other, and the durable obligation re-drives it
    // each crank until the source is observed thawed past the abandoned freeze.
    self.sweep_merge_teardowns();
    progressed
  }

  /// Complete deferred source teardowns whose target capture is now DURABLE on that host — the
  /// world's rendering of the driver's one-barrier batch (see `pump_merges`). Every entry here
  /// came off a `Merged` resolution, and the resolver itself already EXTRACTED the source
  /// endpoint from the host — so completion is decided from the world's own teardown state
  /// (the floor this sweep records), exactly as the product drivers fold the resolutions they
  /// were handed. Re-deriving hosting here instead would always read false post-extraction and
  /// silently drop the whole batch: no floor ever recorded, no source store ever dropped —
  /// absorbed sources left restorable forever and the service's absent-WITH-floor duplicate
  /// arm unreachable.
  fn sweep_merge_teardowns(&mut self) {
    let pending = core::mem::take(&mut self.pending_merge_teardowns);
    for (node, source, target, boundary) in pending {
      if self.merge_floors.contains(&(node, source)) {
        continue; // already landed — a re-staged teardown completes at most once
      }
      let durable = match self.stables.get(&(node, target)) {
        Some(stable) => sailing_proto::StableStore::durable_snapshot(stable)
          .is_some_and(|meta| meta.last_index() >= boundary),
        // The target replica itself was torn down on this host: the barrier this entry waits
        // on can never land, and holding the extracted source's stores hostage to it would
        // leave restorable zombie state behind — complete now (the floor is terminal anyway).
        None => true,
      };
      if durable {
        // The barrier landed: capture + floor + teardown together (the engine batch model).
        self.merge_floors.insert((node, source));
        self.drop_group_replica(source, node);
      } else {
        self
          .pending_merge_teardowns
          .push((node, source, target, boundary));
      }
    }
  }

  /// Fold one host's `Merged` resolution: the FIRST resolution anywhere registers the merge
  /// (populations, absorbed tags, the retired checker, the `merged` mark, the determinism
  /// reference record); every LATER host's resolution is judged against that reference —
  /// byte-for-byte equality of the target's applied record at the resolution point, the direct
  /// form of the absorbed-state determinism argument.
  fn register_or_check_merge(
    &mut self,
    node: u64,
    source: u64,
    target: u64,
    boundary: sailing_proto::Index,
  ) {
    let record = self.applied_of(node, target);
    // Multiset form, like the equal-applied agreement: a crash-restored replica resolving the
    // re-encountered commit presents earlier absorbs at capture-embedded positions, so equal
    // absorbed STATES can order their cells differently. A diverging absorb still trips (the
    // multisets differ); per-key order is the conservation walk's business.
    let sorted = |log: &AppliedLog| {
      let mut v = log.clone();
      v.sort();
      v
    };
    if let Some(rec) = self.merges.iter().find(|m| {
      m.target == target && m.source_led == Self::ledger_id(self.generation_of(source), source)
    }) {
      assert!(
        sorted(&record) == sorted(&rec.resolved_record),
        "ABSORB DIVERGENCE: node {node} resolved the merge of g{source} into g{target} to a \
         different union record than the first resolution\n  first={:?}\n  this={:?}\n  \
         seed={} tick={}",
        rec.resolved_record,
        record,
        self.seed,
        self.tick_count,
      );
      return;
    }
    let source_led = Self::ledger_id(self.generation_of(source), source);
    let target_led = Self::ledger_id(self.generation_of(target), target);
    // Populations fold at the resolution: the frozen window before it refuses writes/reads on
    // the source deterministically, so no accepted traffic can fall between the two owners.
    let (source_keys, source_carried) = {
      let meta = self
        .groups
        .get_mut(&source)
        .unwrap_or_else(|| panic!("merged source {source} is registered"));
      assert!(!meta.merged, "a source merges away at most once");
      meta.retired = true;
      meta.merged = true;
      meta.conf_in_flight = false;
      meta.wired.clear();
      meta.departed_streak.clear();
      // Stash the terminal population before emptying the live set: a lagging husk replica of this
      // retired source stays hosted and inside the safety sweep, but alignment keeps a gkv cell only
      // if the LIVE population owns its key — an emptied live set would blank every husk record. This
      // is the source's final owned population (post-every-split, pre-merge); `aligned_applied` falls
      // back to it so the cross-watermark leg still judges the husk's client content.
      meta.terminal_keys = Some(meta.keys.clone());
      (
        core::mem::take(&mut meta.keys),
        core::mem::take(&mut meta.carried_tags),
      )
    };
    let absorbed_keys = source_keys.clone();
    {
      let meta = self
        .groups
        .get_mut(&target)
        .unwrap_or_else(|| panic!("merge target {target} is registered"));
      meta.keys.extend(source_keys);
      meta.carried_tags.insert(source);
      meta.carried_tags.extend(source_carried);
    }
    // Freeze the source's checker into the retired archive: a group being dismantled
    // host-by-host must not be live-judged against a shrinking replica view (the durable-quorum
    // axiom's witnesses leave with each teardown — the fork-transient lesson's class).
    if let Some(checker) = self.checkers.remove(&source) {
      self
        .retired
        .insert((source, self.generation_of(source)), checker);
    }
    self.pending_transitions.remove(&source);
    self.pending_new_installs.remove(&source);
    self.merges.push(MergeRecord {
      source_led,
      target_led,
      target,
      absorbed_keys,
      resolved_record: record,
      boundary,
    });
  }

  /// Judge every registered merge's union conservation: every value of the source's recorded
  /// history for each absorbed key — minus the split-child exemption below — must appear in the
  /// target's; unordered value containment (`assert_union` checks membership, never position).
  /// Sound at any quiescent point; the multi VOPR runs it at run end beside the split verdict.
  pub fn finalize_merge_conservation_or_panic(&self, seed: u64) {
    for rec in &self.merges {
      // A key this source SPLIT AWAY before the merge left its pre-split cells with the split child,
      // record-wide (the one non-append-only FSM mutation), so a later merge target is not demanded
      // to hold them. Collect, per absorbed key, every value in a registered split child's record for
      // a key this source parented, and exempt the lot from the union demand. Globally-unique values
      // make child-record membership an exact witness that a cell rode the CHILD LINEAGE — not that
      // it departed this source: the child's accumulated record also holds cells the child originated
      // or inherited after the split, so the exemption is a SUPERSET of true departures.
      //
      // Deliberately NOT narrowed by netting returns: the only ledger signal for a return is the
      // value's presence in an absorbed union source's record, and the append-only, dedup-by-value
      // ledger cannot separate a genuine return to the FSM from a value merely INHERITED into that
      // union source's record and never in this source's FSM at the merge — netting over-demands,
      // re-tripping legitimately departed cells. Consequently any cell sitting in both this source's
      // and a matching child's history — returned after departing, or child-born/child-inherited and
      // merged back — stays exempt while riding this merge, and its all-replica loss escapes this
      // demand (disclosed at the VOPR absorbed-coverage note); the split partition verdict still
      // guards the handover.
      let mut departed: BTreeMap<u16, BTreeSet<u64>> = BTreeMap::new();
      for sp in self.splits.values() {
        if sp.parent_led != rec.source_led {
          continue;
        }
        for &k in &rec.absorbed_keys {
          if sp.child_keys.contains(&k) {
            departed.entry(k).or_default().extend(
              self
                .conservation
                .history(sp.child_led, k)
                .iter()
                .map(|&(_, v)| v),
            );
          }
        }
      }
      let ctx = MergeReplayContext {
        seed,
        source_led: rec.source_led,
        target_led: rec.target_led,
      };
      // Judge the keys the source still OWNED at the merge (the transferred population), not
      // every key it ever wrote: a key the source split away BEFORE merging left with that split
      // (its own partition verdict judges the handover) and never rode this union, so the
      // target rightly lacks it — `keys_of` (ledger-retained written keys) would demand it and
      // false-trip. Owned-but-unwritten keys judge vacuously (empty source history).
      self.conservation.assert_union(
        rec.target_led,
        rec.source_led,
        &rec.absorbed_keys,
        &departed,
      );
      drop(ctx); // no panic: disarm silently
    }
  }

  /// Registered merges (first resolutions) across the run — the report's non-vacuity witness.
  pub fn merges_registered(&self) -> u64 {
    self.merges.len() as u64
  }

  /// Per-host `Merged` resolutions across the run.
  pub fn merges_resolved(&self) -> u64 {
    self.merges_resolved
  }

  /// Per-host `Aborted` resolutions across the run.
  pub fn merges_aborted(&self) -> u64 {
    self.merges_aborted
  }

  /// Per-host `CaptureFailed` resolutions across the run (expected zero under the sim FSM).
  pub fn merges_capture_failed(&self) -> u64 {
    self.merges_capture_failed
  }

  /// Whether `gid` has ever ABSORBED another group — the agreement oracle's mode switch (see
  /// `ClusterView::positional_agreement`).
  pub(crate) fn group_absorbed(&self, gid: u64) -> bool {
    self.merges.iter().any(|m| m.target == gid)
  }

  /// Whether `gid` was merged away (the terminal catalog mark).
  pub fn is_merged(&self, gid: u64) -> bool {
    self.groups.get(&gid).is_some_and(|m| m.merged)
  }

  /// Whether `gid` currently REFUSES writes for a merge freeze, read where write authority
  /// lives: the LEADER's replica, falling back to a VOTER-QUORUM read while leaderless (a
  /// frozen majority refuses any would-be leader's fresh load just the same). The calm window's
  /// skip predicate. Deliberately NOT ∃-any-replica: one stale frozen FOLLOWER — a thaw it has
  /// not applied yet — does not stop the leader from accepting load, and skipping the calm
  /// progress demand on its account would blind the window to real livelocks.
  pub fn group_frozen(&self, gid: u64) -> bool {
    if let Some(leader) = self.leader_of(gid) {
      return self.hosts[&leader]
        .group(&gid)
        .is_some_and(sailing_proto::Endpoint::is_frozen);
    }
    let voters = self.group_voters(gid);
    if voters.is_empty() {
      return false;
    }
    let frozen = voters
      .iter()
      .filter(|&&n| {
        self
          .hosts
          .get(&n)
          .and_then(|h| h.group(&gid))
          .is_some_and(sailing_proto::Endpoint::is_frozen)
      })
      .count();
    frozen * 2 > voters.len()
  }

  /// Every hosted replica FROZEN by an applied merge freeze, as `(node, gid)` — the quiesce
  /// teeth's wedge scan. ∃-any deliberately, unlike [`group_frozen`](Self::group_frozen): ANY
  /// replica left frozen at run end is a stranded merge participant, whatever its leader thinks.
  /// World-PARKED replicas are exempt: a removed ex-member's retained witness is no longer a
  /// protocol participant (the `leader_of` rule), so nothing will ever replicate a thaw to it —
  /// its stale frozen view is designed residue, not a wedge.
  pub fn frozen_replicas(&self) -> Vec<(u64, u64)> {
    let mut out = Vec::new();
    for &node in &self.node_ids {
      for gid in self.groups.keys() {
        if !self.parked.contains(&(node, *gid))
          && let Some(ep) = self.hosts[&node].group(gid)
          && ep.is_frozen()
        {
          out.push((node, *gid));
        }
      }
    }
    out
  }

  /// Whether ANY hosting replica of `gid` carries ACTIVE freeze state — applied or still
  /// append-pending. The quiesce drive's work predicate: the wedge scan reads `is_frozen`, but
  /// the drive must also outlive a freeze still settling toward it, or a commit landing in the
  /// drive's last ticks would freeze past the loop and trip the scan unfairly. World-parked
  /// replicas are exempt, as on the scan.
  pub fn group_freeze_seen(&self, gid: u64) -> bool {
    self
      .node_ids
      .iter()
      .filter(|&&n| !self.parked.contains(&(n, gid)))
      .filter_map(|&n| self.hosts[&n].group(&gid))
      .any(sailing_proto::Endpoint::merge_freeze_active)
  }

  /// The target a live freeze of `source` claims, read off the embedder's own freeze record
  /// (`active_freezes`, last-wins) — the quiesce drive's pair lookup when the fuzzer book has
  /// already retired the pair (an abort observation from a previous incarnation of the same
  /// pair can race a re-freeze's booking).
  pub fn claimed_target_of(&self, source: u64) -> Option<u64> {
    self.active_freezes.get(&source).copied()
  }

  /// `Event::MergeAborted` observations drained for `(target, source)` across the run — the
  /// monotone abort clock (see the field): the fuzzer book stamps it at booking and retires the
  /// pair once it moves, the abort-side twin of merge registration.
  pub fn merge_abort_observations(&self, target: u64, source: u64) -> u64 {
    self
      .merge_aborts_observed
      .get(&(target, source))
      .copied()
      .unwrap_or(0)
  }

  /// The diligent-embedder floor feed for `node`: every REGISTERED-absorbed source whose local
  /// replica is a frozen HUSK nothing on this node still needs. The world plays the embedder
  /// pin B(a) models with its direct floor write — a host that never locally resolved the park
  /// (crashed through the capture window, or was superseded by an install) never folds its own
  /// per-node floor, and its husk is otherwise unremovable (`Frozen`), claim-blocking, and
  /// capture-fenced forever; only the service's floor-keyed dissolve arm reclaims it.
  ///
  /// BOUNDARY-AWARE, the load-bearing scope: the registered merge's absorb coordinate rides
  /// [`MergeRecord::boundary`], and a target replica still BELOW it has not folded the union —
  /// it will park and absorb the husk (or install past it). Feeding the terminal floor there
  /// manufactures the absent/duplicate abort arm and skips the union: committed divergence
  /// against every host that absorbed. Eligible only when this node's target replica is gone,
  /// or is past the boundary with no park naming the source.
  fn embedder_husk_floors(&self, node: u64) -> BTreeSet<u64> {
    let mut out = BTreeSet::new();
    let host = &self.hosts[&node];
    for (gid, meta) in &self.groups {
      if !meta.merged {
        continue;
      }
      if !host
        .group(gid)
        .is_some_and(sailing_proto::Endpoint::is_frozen)
      {
        continue;
      }
      let led = Self::ledger_id(self.generation_of(*gid), *gid);
      let Some(rec) = self.merges.iter().find(|m| m.source_led == led) else {
        continue;
      };
      let eligible = match host.group(&rec.target) {
        None => true,
        Some(tep) => {
          let parked_on_it = tep.pending_merge().is_some_and(|p| {
            <u64 as sailing_proto::Data>::decode_exact(p.source_bytes()) == Ok(*gid)
          });
          !parked_on_it && tep.applied_index() >= rec.boundary
        }
      };
      if eligible {
        out.insert(*gid);
      }
    }
    out
  }

  /// Whether any hosting replica of `gid` is a merge TARGET still PARKED on an unresolved absorb
  /// (`pending_merge`) — the quiesce convergence tooth: a park holds the target's apply at the
  /// boundary while its commit races ahead, so its members look "equally applied" and would pass a
  /// naive caught-up check though the group is wedged. A genuinely resolving park clears inside
  /// the healed quiesce ticking; only a PERMANENT one keeps this true to the tick budget and
  /// surfaces the livelock instead of masking it as converged.
  pub fn group_merge_parked(&self, gid: u64) -> bool {
    self
      .node_ids
      .iter()
      .filter_map(|&n| self.hosts[&n].group(&gid))
      .any(|ep| ep.pending_merge().is_some())
  }

  /// The source id a PARKED replica of `gid` (a merge target mid-absorb) names in its
  /// `pending_merge` — the group whose captured state the park is waiting on. `None` when no replica
  /// of `gid` is parked. First-found across replicas (a target parks on one absorb at a time).
  pub(crate) fn parked_source_of(&self, gid: u64) -> Option<u64> {
    for node in &self.node_ids {
      if let Some(ep) = self.hosts[node].group(&gid)
        && let Some(p) = ep.pending_merge()
        && let Ok(source) = <u64 as sailing_proto::Data>::decode_exact(p.source_bytes())
      {
        return Some(source);
      }
    }
    None
  }

  /// Whether `gid`'s committed voter conf keeps a LIVE HOST QUORUM: a strict majority of its
  /// committed voters currently host a replica of `gid`. Read at a fully-healed point (quiesce /
  /// calm window), a voter that does NOT host means reconcile could not resurrect its replica — the
  /// "under-hosted" condition the tracked merge wedge turns on. A conf with no voters is treated as
  /// lacking a quorum (an empty absorbing conf cannot make the absorb durable).
  pub(crate) fn has_live_host_quorum(&self, gid: u64) -> bool {
    let voters = self.group_voters(gid);
    if voters.is_empty() {
      return false;
    }
    let hosting = voters.iter().filter(|&&n| self.hosts_group(n, gid)).count();
    hosting * 2 > voters.len()
  }

  /// A one-line diagnostic of `gid`'s merge-blocking state for the quiesce / wedge panics: whether
  /// it is frozen or parked, the merge counterpart its completion depends on (a parked target's
  /// captured SOURCE, a frozen source's claimed TARGET), and that counterpart's live/hosting status
  /// — the exact facts needed to adjudicate a stuck merge participant against the tracked
  /// under-hosted class.
  pub(crate) fn merge_block_dbg(&self, gid: u64) -> String {
    let mut parts = std::vec![std::format!(
      "g{gid}[frozen={} freeze_seen={} parked={} host_quorum={}]",
      self.group_frozen(gid),
      self.group_freeze_seen(gid),
      self.group_merge_parked(gid),
      self.has_live_host_quorum(gid),
    )];
    if let Some(source) = self.parked_source_of(gid) {
      parts.push(std::format!(
        "parked_on_source g{source}[live={} host_quorum={} voters={:?} hosting={:?}]",
        self.live_groups().contains(&source),
        self.has_live_host_quorum(source),
        self.group_voters(source),
        self.hosting_nodes(source),
      ));
    }
    if let Some(target) = self.claimed_target_of(gid) {
      parts.push(std::format!(
        "claims_target g{target}[live={} host_quorum={}]",
        self.live_groups().contains(&target),
        self.has_live_host_quorum(target),
      ));
    }
    parts.join(" ")
  }

  /// Whether `gid` is wedged in the TRACKED under-hosted parked-absorb completion class (#106): an
  /// unresolved merge participant whose completion is blocked because the absorbing conf lacks a
  /// live host quorum. This is the DOCUMENTED exemption predicate the storm-profile quiesce and calm
  /// windows certify past instead of panicking (see the `exempt_tracked` paths in the multi VOPR
  /// runner) — deliberately NARROW so a NON-merge livelock or a merge wedged for any OTHER reason
  /// still trips the liveness gates as a fresh find.
  ///
  /// It is a per-group membership test against [`tracked_merge_wedge_set`](Self::tracked_merge_wedge_set),
  /// which computes the whole blocked component at once (the tracked class cascades — an under-hosted
  /// source strands the target parked on it, which strands the next source frozen against that
  /// target). Convenient for the calm window and the unit tests; the quiesce loop computes the set
  /// once per pass instead.
  pub(crate) fn tracked_underhosted_merge_wedge(&self, gid: u64) -> bool {
    self.tracked_merge_wedge_set().contains(&gid)
  }

  /// Per-group membership in the fork-fence coupling set (#110) — the calm-window twin of
  /// [`tracked_underhosted_merge_wedge`](Self::tracked_underhosted_merge_wedge). See
  /// [`fork_fence_wedge_set`](Self::fork_fence_wedge_set).
  pub(crate) fn fork_fence_coupled_wedge(&self, gid: u64) -> bool {
    self.fork_fence_wedge_set().contains(&gid)
  }

  /// The full set of groups (live AND retired) wedged in the tracked under-hosted parked-absorb
  /// class (#106): every merge participant transitively blocked by an under-hosted merge conf. The
  /// storm-profile quiesce and calm windows certify these past instead of panicking — deliberately
  /// NARROW (rooted only at a genuine hosting shortfall) so a NON-merge livelock, or a merge wedged
  /// with every relevant conf still hosted, stays a fresh find that trips the liveness gates.
  ///
  /// Computed as a fixpoint over the merge dependency graph:
  /// - **Base (the #106 root).** A merge participant — frozen (`group_frozen`/`group_freeze_seen`) or
  ///   parked (`group_merge_parked`) — whose OWN committed conf lacks a live host quorum. This is the
  ///   observed shape: a source dismantled host-by-host to a husk cannot be captured, so the park on
  ///   it never resolves.
  /// - **Propagate.** A PARKED target whose captured SOURCE (`parked_source_of`) is already blocked;
  ///   or a FROZEN source whose claimed TARGET (`claimed_target_of`) is already blocked (a target
  ///   pinned at another park cannot absorb it). Iterated to a fixpoint over the (bounded) group set.
  ///
  /// A world with no under-hosted merge participant yields the EMPTY set — every merge is then
  /// obliged to converge, and any that does not still trips the gate.
  pub(crate) fn tracked_merge_wedge_set(&self) -> BTreeSet<u64> {
    let participant =
      |g: u64| self.group_frozen(g) || self.group_freeze_seen(g) || self.group_merge_parked(g);
    // Base: merge participants with no live host quorum (the under-hosted husk roots).
    let base: BTreeSet<u64> = self
      .groups
      .keys()
      .copied()
      .filter(|&g| participant(g) && !self.has_live_host_quorum(g))
      .collect();
    self.propagate_merge_block(base)
  }

  /// Propagate a BASE set of blocked merge participants to a fixpoint over the merge dependency
  /// edges: a PARKED target whose captured source is blocked, or a FROZEN source whose claimed
  /// target is blocked, joins the set. Shared by both exemption classes — #106 (under-hosted) and
  /// #110 (fork-fence) differ only in their base root; the cascade closure is identical.
  fn propagate_merge_block(&self, mut blocked: BTreeSet<u64>) -> BTreeSet<u64> {
    let participant =
      |g: u64| self.group_frozen(g) || self.group_freeze_seen(g) || self.group_merge_parked(g);
    loop {
      let mut added = false;
      for g in self.groups.keys().copied() {
        if blocked.contains(&g) || !participant(g) {
          continue;
        }
        let waits_on_blocked = self
          .parked_source_of(g)
          .is_some_and(|s| blocked.contains(&s))
          || self
            .claimed_target_of(g)
            .is_some_and(|t| blocked.contains(&t));
        if waits_on_blocked {
          blocked.insert(g);
          added = true;
        }
      }
      if !added {
        break;
      }
    }
    blocked
  }

  /// Whether a standing fork-fence conflict is recorded on `(node, parent)` at OR BELOW `coord` — the
  /// pure record-lookup + index-comparison leg of the fork-fence coupling (#110). `coord` is the
  /// PARK's own coordinate (see [`fork_fence_coupled_park`](Self::fork_fence_coupled_park)); a fence
  /// ABOVE it sits past the absorb capture and does not deadlock the park — the narrowness.
  pub(crate) fn has_fork_fence_below(
    &self,
    node: u64,
    parent: u64,
    coord: sailing_proto::Index,
  ) -> bool {
    self
      .fork_conflicts
      .get(&(node, parent))
      .is_some_and(|idxs| idxs.keys().any(|&s| s <= coord))
  }

  /// Whether `gid` is a merge TARGET whose park is deadlocked behind a standing fork fence (#110):
  /// on some node hosting a PARKED replica of `gid`, a recorded fork-conflict fence for
  /// `(node, parent == gid)` sits at-or-below that replica's PARK COORDINATE. A drained merge park
  /// pins its applied index at `k-1` (one below the `CommitMerge` entry it waits on), so the park's
  /// own entry index is `applied_index() + 1` — the capture coordinate a standing fence must sit at
  /// or below to deadlock it. The comparison is against the PARK's coordinate, NOT the moving commit:
  /// a parked target's commit races ahead of its pinned apply, and comparing against it would
  /// over-couple a fence sitting between the two. A parked fork holds the parent's capture fence
  /// there, and this absorb cannot proceed above it — the composition deadlock of two individually
  /// sound designs, safety intact. The park being LIVE is a co-condition read from world state, so an
  /// accumulated record never certifies a group whose merge is no longer parked.
  pub(crate) fn fork_fence_coupled_park(&self, gid: u64) -> bool {
    self.node_ids.iter().any(|&n| {
      self.hosts[&n]
        .group(&gid)
        .is_some_and(|ep| ep.pending_merge().is_some())
        && self.has_fork_fence_below(
          n,
          gid,
          sailing_proto::Index::new(self.applied_index_of(n, gid).get() + 1),
        )
    })
  }

  /// The full set of groups wedged in the fork-fence coupling (#110): every merge participant
  /// transitively blocked by a fork-fence-coupled park. Base = the parked targets
  /// [`fork_fence_coupled_park`](Self::fork_fence_coupled_park) identifies; the SAME cascade closure
  /// as [`tracked_merge_wedge_set`](Self::tracked_merge_wedge_set) then folds in the frozen source
  /// held behind a coupled target's stalled park. EMPTY when no coupling stands — every merge is
  /// then obliged to converge.
  pub(crate) fn fork_fence_wedge_set(&self) -> BTreeSet<u64> {
    let base: BTreeSet<u64> = self
      .groups
      .keys()
      .copied()
      .filter(|&g| self.fork_fence_coupled_park(g))
      .collect();
    self.propagate_merge_block(base)
  }

  /// Test-only: inject a standing fork-fence record for `(node, parent)` at `split_index` for pure
  /// BOUNDARY-ARITHMETIC red-proofs. The child is a sentinel (`u64::MAX`) no node ever hosts, so the
  /// pump's redundant-fold reconciliation never clears it — the fence stands until the boundary
  /// assertion reads it. A resolution-arm regression uses
  /// [`inject_fork_conflict_for_child`](Self::inject_fork_conflict_for_child) and drives the arm for
  /// real.
  #[cfg(test)]
  pub(crate) fn inject_fork_conflict(
    &mut self,
    node: u64,
    parent: u64,
    split_index: sailing_proto::Index,
  ) {
    self.inject_fork_conflict_for_child(node, parent, split_index, u64::MAX);
  }

  /// Test-only: inject a standing fork-fence record for `(node, parent)` at `split_index` naming the
  /// real `child` — so a subsequent REAL barrier-resolution arm clears it exactly as an
  /// organically-recorded conflict would: the refuse arm by its split index, or the container's
  /// internal redundant fold via the pump's reconciliation once `child` is hosted on the node. The
  /// world draws only fresh child ids and so cannot itself mint a split conflict; a regression records
  /// the conflict this way and drives the resolution arm through real machinery.
  #[cfg(test)]
  pub(crate) fn inject_fork_conflict_for_child(
    &mut self,
    node: u64,
    parent: u64,
    split_index: sailing_proto::Index,
    child: u64,
  ) {
    self
      .fork_conflicts
      .entry((node, parent))
      .or_default()
      .insert(split_index, child);
  }
}

/// Prints replay context if a union assert unwinds (the ledger's panics carry ids and histories
/// but not the seed).
struct MergeReplayContext {
  seed: u64,
  source_led: u64,
  target_led: u64,
}

impl Drop for MergeReplayContext {
  fn drop(&mut self) {
    if std::thread::panicking() {
      std::eprintln!(
        "[conservation] while judging merge source_led={} -> target_led={} (ledger id = gid + \
         generation * 1_000_000)\n  seed={} (replay: run_multi_vopr(seed, ticks, profile))",
        self.source_led,
        self.target_led,
        self.seed,
      );
    }
  }
}

impl MultiWorld {
  /// Whether `gid` is an UNRESOLVED participant of a merge choreography — the lifecycle
  /// churn's "spoken for" predicate. The remove/recreate action draws consult it the same way
  /// they consult retirement: the world plays the embedder here, and the embedder contract
  /// says a choreography's participants stay put until it resolves — tearing down a frozen or
  /// claimed source strands every parked commit that names it (its replicas are LOG-COMPLETE,
  /// so the install-supersede route, which assumes log-behind stragglers, never fires), and
  /// tearing down a mid-commit target strands its frozen source with no thaw relay left to
  /// ride. Mirrors the fork pump's coordinator-refusal modeling and the removal purge: layers
  /// above the pure container, played truthfully by the world.
  ///
  /// The legs, cheapest truthful reads of the world's own surfaces, kept a SUPERSET of the product
  /// teardown gate so a drawn removal can never trip a refusal the `.expect` would panic on: a
  /// hosting replica with an ACTIVE merge freeze (`merge_freeze_active` — a source from the freeze's
  /// APPEND observation, the exact in-memory state the product's `Frozen` gate reads, until absorbed
  /// or thawed) or parked (a target mid-resolution) or still OWING an aborted source its thaw (a
  /// former target whose `abandoned` obligation the container's thaw pass has not discharged —
  /// tearing it down drops the obligation and strands the upstream source frozen); a merge admin
  /// entry still in a hosting replica's UNAPPLIED durable-log suffix (the accepted-but-not-yet-folded
  /// window — read off the store's raw durable view, so the predicate never touches the faultable
  /// read path and is schedule-inert for every profile that never merges); any live park anywhere
  /// NAMING `gid` as its source; or any still-active freeze CLAIMING `gid` as its target (the
  /// embedder's `active_freezes` record, filtered by whether the source's freeze is still live — the
  /// pre-park mirror of the park-names-source scan, and a superset of the container's `Claimed` gate
  /// across BOTH its windows, applied and append-pending). The world over-excludes an OWED source the
  /// product would ADMIT (its designed catalog escape) — a superset is sound; the world simply never
  /// draws that removal.
  pub fn merge_choreography_active(&self, gid: u64) -> bool {
    for node in &self.node_ids {
      if let Some(ep) = self.hosts[node].group(&gid) {
        if ep.merge_freeze_active() || ep.pending_merge().is_some() || ep.has_abandoned() {
          return true;
        }
        if let Some(log) = self.logs.get(&(*node, gid))
          && unapplied_merge_admin(log, ep.applied_index())
        {
          return true;
        }
      }
    }
    let mut gid_bytes = Vec::new();
    sailing_proto::Data::encode(&gid, &mut gid_bytes);
    for node in &self.node_ids {
      for other in self.groups.keys() {
        if *other != gid
          && let Some(ep) = self.hosts[node].group(other)
          && let Some(p) = ep.pending_merge()
          && p.source_bytes().as_ref() == gid_bytes.as_slice()
        {
          return true;
        }
      }
    }
    // The CLAIMED-TARGET leg (the mirror of the park-names-source scan above): some other source
    // names `gid` as its merge target and its freeze is still ACTIVE — the pre-park window the
    // container's `Claimed` gate refuses. The embedder's own `active_freezes` record supplies the
    // target of each freeze (a park has not formed yet, so nothing in the log points here); the
    // `merge_freeze_active` filter drops a spent record whose source has since thawed or absorbed.
    // Covers BOTH product legs — applied and append-pending — because `merge_freeze_active` is the
    // superset of `frozen` and freeze-pending, so this stays a superset of the product gate.
    for (&src, &tgt) in &self.active_freezes {
      if tgt == gid
        && src != gid
        && self.node_ids.iter().any(|n| {
          self.hosts[n]
            .group(&src)
            .is_some_and(|ep| ep.merge_freeze_active())
        })
      {
        return true;
      }
    }
    false
  }
}

/// Whether the durable log still carries a merge admin entry ABOVE `applied` — the world-side
/// twin of the product's own restart derivation over the unapplied suffix. Deliberately a raw
/// walk of the durable view rather than a `LogStore` read: the trait path rolls the store's
/// fault dice, and a mere lifecycle-draw filter must not perturb the fault schedule of runs it
/// then judges (a not-yet-durable acceptance can slip this view for a sub-tick window, but a
/// freeze that never went durable anywhere can never gate a commit, so nothing can park on it).
fn unapplied_merge_admin(log: &MemLog, applied: sailing_proto::Index) -> bool {
  use sailing_proto::EntryKind;
  log.durable_entries().iter().any(|e| {
    e.index() > applied
      && matches!(
        e.kind(),
        EntryKind::PrepareMerge | EntryKind::CommitMerge | EntryKind::RollbackMerge
      )
  })
}
