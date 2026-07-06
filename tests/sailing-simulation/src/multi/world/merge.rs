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
  /// The target's FULL applied record at the FIRST resolution — the absorb-determinism
  /// reference every later host's resolution is compared against.
  pub(super) resolved_record: AppliedLog,
}

/// Per-node `(log, stable)` resolution over the world's split store maps — the driver's
/// `GroupStores` seam, borrowed for one host's service call.
pub(super) struct NodeStores<'a> {
  pub(super) node: u64,
  pub(super) logs: &'a mut BTreeMap<(u64, u64), MemLog>,
  pub(super) stables: &'a mut BTreeMap<(u64, u64), MemStable<u64>>,
  /// The host's terminal merge floors (recorded when a source's deferred teardown lands).
  pub(super) floored: &'a BTreeSet<(u64, u64)>,
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
    if self.floored.contains(&(self.node, *gid)) {
      sailing_proto::MERGED_FLOOR
    } else {
      0
    }
  }

  fn lineage(&self, _gid: &u64) -> u64 {
    0
  }
}

impl MultiWorld {
  /// Propose the merge FREEZE of `source` into `target` on the source's current leader;
  /// returns the container verdict verbatim (`None` while leaderless). Every refusal arm is a
  /// legitimate no-op tick for the fuzzer.
  pub fn propose_prepare_merge(
    &mut self,
    source: u64,
    target: u64,
  ) -> Option<Result<sailing_proto::Index, sailing_proto::MergeError<u64>>> {
    let leader = self.leader_of(source)?;
    let host = self.hosts.get_mut(&leader).expect("leader host exists");
    let log = self.logs.get_mut(&(leader, source)).expect("leader log");
    let stable = self.stables.get(&(leader, source)).expect("leader stable");
    host.prepare_merge(&source, self.now, log, stable, &target)
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
      let resolutions = {
        let host = self.hosts.get_mut(&node).expect("host exists");
        let mut stores = NodeStores {
          node,
          logs: &mut self.logs,
          stables: &mut self.stables,
          floored: &self.merge_floors,
        };
        host.service_merge_applies(now, &mut stores)
      };
      for r in resolutions {
        progressed = true;
        match r {
          sailing_proto::MergeResolution::Merged { source, target } => {
            self.merges_resolved += 1;
            self.register_or_check_merge(node, source, target);
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
            let boundary = self
              .hosts
              .get(&node)
              .and_then(|h| h.group(&target))
              .map_or(sailing_proto::Index::ZERO, |ep| ep.applied_index());
            self
              .pending_merge_teardowns
              .push((node, source, target, boundary));
          }
          sailing_proto::MergeResolution::Aborted { .. } => {
            self.merges_aborted += 1;
          }
        }
      }
    }
    // The abort-relay drain, the driver's crank leg verbatim: a target-side abort applied on
    // this host must thaw its named source ON THE SOURCE'S OWN LOG. Best-effort per host —
    // only the source-leader host's proposal lands; the rest die typed at the source's gates.
    for node in self.node_ids.clone() {
      let now = self.now;
      let host = self.hosts.get_mut(&node).expect("host exists");
      while let Some(relay) = host.poll_pending_merge_abort() {
        let (Some(log), Some(stable)) = (
          self.logs.get_mut(&(node, relay.source)),
          self.stables.get(&(node, relay.source)),
        ) else {
          continue;
        };
        if host
          .propose_merge_unfreeze(&relay.source, now, log, stable, &relay.target)
          .is_some()
        {
          progressed = true;
        }
      }
    }
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
  fn register_or_check_merge(&mut self, node: u64, source: u64, target: u64) {
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
      (
        core::mem::take(&mut meta.keys),
        core::mem::take(&mut meta.carried_tags),
      )
    };
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
      resolved_record: record,
    });
  }

  /// Judge every registered merge's union conservation: the target's recorded history for each
  /// source key must open with the source's full recorded history (the absorbed baseline).
  /// Sound at any quiescent point; the multi VOPR runs it at run end beside the split verdict.
  pub fn finalize_merge_conservation_or_panic(&self, seed: u64) {
    for rec in &self.merges {
      let ctx = MergeReplayContext {
        seed,
        source_led: rec.source_led,
        target_led: rec.target_led,
      };
      self
        .conservation
        .assert_union(rec.target_led, rec.source_led);
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

  /// Whether `gid` has ever ABSORBED another group — the agreement oracle's mode switch (see
  /// `ClusterView::positional_agreement`).
  pub(crate) fn group_absorbed(&self, gid: u64) -> bool {
    self.merges.iter().any(|m| m.target == gid)
  }

  /// Whether `gid` was merged away (the terminal catalog mark).
  pub fn is_merged(&self, gid: u64) -> bool {
    self.groups.get(&gid).is_some_and(|m| m.merged)
  }

  /// Whether any hosting replica of `gid` reports the applied merge FREEZE — the calm window's
  /// skip predicate: a frozen group refuses writes by design, so demanding fresh client load
  /// from it would misread the covered refusal as a livelock.
  pub fn group_frozen(&self, gid: u64) -> bool {
    self
      .node_ids
      .iter()
      .filter_map(|&n| self.hosts[&n].group(&gid))
      .any(sailing_proto::Endpoint::is_frozen)
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
  /// The legs, cheapest truthful reads of the world's own surfaces: a hosting replica frozen
  /// (a source from freeze-apply until absorbed or thawed) or parked (a target mid-resolution);
  /// a merge admin entry still in a hosting replica's UNAPPLIED durable-log suffix (the
  /// accepted-but-not-yet-folded window — read off the store's raw durable view, so the
  /// predicate never touches the faultable read path and is schedule-inert for every profile
  /// that never merges); or any live park anywhere NAMING `gid` as its source.
  pub fn merge_choreography_active(&self, gid: u64) -> bool {
    for node in &self.node_ids {
      if let Some(ep) = self.hosts[node].group(&gid) {
        if ep.is_frozen() || ep.pending_merge().is_some() {
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
