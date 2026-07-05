//! The split lifecycle verb, the fork pump, and the conservation recorder — the world-side
//! wiring of the reshaping machinery.
//!
//! The world plays the DRIVER's role from the reactor choreography: [`MultiWorld::propose_split`]
//! delegates to the container's gate stack, the per-tick fork pump drains
//! `MultiRaft::poll_pending_fork` into `create_group_from_fork` materializations (sync stores
//! make each baseline durable at the call, so the barrier lifts immediately — the one-crank
//! engine-flush contract collapsed to its synchronous form), and the FIRST materialization of a
//! child REGISTERS it in the harness catalog (checker, registry entry, key population, the
//! conservation pair).
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
//! parent's population conservatively shrunk: parked keys, never a false positive.
//!
//! The recorder walks every replica's FULL RAW applied record each sweep and appends each gkv
//! cell once — values are globally unique and strictly increase per `(group, key)`, so
//! "strictly above the last recorded value" dedupes across replicas, crash re-walks, and the
//! child's inherited baseline alike. The walk trusts VALUES, never positions: `LogSm::split`
//! mutates the record non-append-only (moved-key cells vanish record-wide) and a crash restore
//! can resurrect a pre-split state, so a cell can sit BELOW any position an earlier sweep
//! reached — a positional resume watermark skips it forever, punching an interior hole in the
//! recorded history that a later fork baseline exposes as a false conservation verdict. The
//! value dedupe needs no resume state at all: a full re-walk re-presents only cells the ledger
//! already admitted. Histories are keyed by an INCARNATION-QUALIFIED ledger id, so a recreated
//! gid's fresh history can never pollute a recorded split pair's verdict. The child's opening
//! history is recorded from its OWN materialized record (the fork blob) — never copied from the
//! parent's — which is what gives [`ConservationLedger::assert_partition`] teeth: a partition
//! bug in the FSM shows up as a parent/child history mismatch, not a tautology.

use super::*;

/// A proposed-but-not-yet-registered split: the parent and the key set its instruction assigned
/// to the child.
pub(super) struct PendingSplit {
  pub(super) parent: u64,
  pub(super) child_keys: BTreeSet<u16>,
}

/// One REGISTERED split (the child materialized somewhere): the conservation verdict's unit.
pub(super) struct SplitRecord {
  /// The parent's incarnation-qualified ledger id AT the split.
  pub(super) parent_led: u64,
  /// The child's incarnation-qualified ledger id (its fork generation).
  pub(super) child_led: u64,
  /// The keys the instruction assigned to the child.
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
  fn ledger_id(generation: u64, gid: u64) -> u64 {
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
    if result.is_ok() {
      let meta = self.groups.get_mut(&parent).expect("registered group");
      let child_keys = meta.keys.split_off(&point);
      self
        .pending_splits
        .insert(child, PendingSplit { parent, child_keys });
    }
    Some(result)
  }

  /// Drain every host's committed, relay-ready forks into materializations — the driver's
  /// fork-drain, played by the world, INCLUDING the coordinator-admission gate the product
  /// runs at this edge (floor, then tombstone): a late fork whose child id the catalog has
  /// retired — or outpaced by recreation — resolves REFUSED. Returns whether anything happened.
  pub(super) fn pump_forks(&mut self) -> bool {
    let mut progressed = false;
    for node in self.node_ids.clone() {
      loop {
        let host = self.hosts.get_mut(&node).expect("host exists");
        let Some(fork) = host.poll_pending_fork() else {
          break;
        };
        progressed = true;
        // The pure container cannot see retirement — the coordinators own that refusal, and
        // this catalog check models it. A lagging parent replica can apply the split AFTER the
        // child's registered incarnation was retired (or recreated past the fork's
        // generation); materializing then would squat a replica the lifecycle verbs rightly
        // assume gone. Refusal is the product's arm verbatim: no materialization, the parent's
        // fence lifts (a fork that will never land here must not pin the parent), and the id
        // stays with the ordinary lifecycle — a later recreation admits cleanly.
        if self
          .groups
          .get(&fork.child)
          .is_some_and(|m| m.retired || fork.child_gen < m.generation)
        {
          self.split_refused += 1;
          self
            .hosts
            .get_mut(&node)
            .expect("host exists")
            .lift_fork_barrier(&fork.parent, fork.split_index);
          continue;
        }
        self.register_split_child(&fork);
        self.wire_fork_replica(node, fork);
      }
      // Conflict signals are drained and counted, never acted on: see the field docs for why
      // the world's embedder model leaves a squatter in place.
      while let Some(_pc) = self
        .hosts
        .get_mut(&node)
        .expect("host exists")
        .poll_split_conflict()
      {
        self.split_conflicts += 1;
      }
    }
    progressed
  }

  /// Register a fork's child in the harness catalog on its FIRST materialization anywhere:
  /// registry entry (voters from the fork's boot config, population from the pending record,
  /// the inherited-baseline length off the fork's own manufactured half), a fresh checker
  /// floored at the fork baseline, and the conservation pair. Later materializations of the
  /// same fork on other nodes find the child registered and only wire their replica.
  fn register_split_child(&mut self, fork: &sailing_proto::GroupFork<u64, u64, LogSm>) {
    if self.groups.contains_key(&fork.child) {
      return;
    }
    let pending = self
      .pending_splits
      .remove(&fork.child)
      .unwrap_or_else(|| panic!("fork for child {} without a proposed split", fork.child));
    let parent_led = Self::ledger_id(self.generation_of(pending.parent), pending.parent);
    let child_led = Self::ledger_id(fork.child_gen, fork.child);
    let voters: BTreeSet<u64> = fork.config.voters().iter().copied().collect();
    self.groups.insert(
      fork.child,
      lifecycle::GroupMeta {
        voters,
        generation: fork.child_gen,
        keys: pending.child_keys.clone(),
        // Every replica of this incarnation opens with the same inherited record, whichever
        // path delivered it: every parent replica manufactures the fork at the same applied
        // prefix (the split entry's log position), and the blob is authoritative at
        // materialization while `LogSm::snapshot()` carries the full record to any
        // snapshot-wired latecomer. Group-level here is what keeps the cross-talk floor
        // independent of HOW a replica arrived (see `cross_talk_sweep` for why the count
        // stays sound once onward splits shrink the inherited prefix).
        fork_baseline: fork.fsm.applied().len(),
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
      self.checkers.insert(fork.child, checker).is_none(),
      "register_split_child: child {} already had a checker",
      fork.child
    );
    self.splits_applied += 1;
    self.splits.insert(
      fork.child,
      SplitRecord {
        parent_led,
        child_led,
        child_keys: pending.child_keys,
      },
    );
  }

  /// Materialize one fork on `node`: fresh stores, the manufactured snapshot install through
  /// `create_group_from_fork`, the world's per-replica bookkeeping, and the barrier lift (sync
  /// stores make the baseline durable at the call). Deliberately NO oracle-view seeding here:
  /// the aligned view is a pure content rule ([`align_record`](Self::align_record)), the
  /// cross-talk floor derives from the group registration record (`GroupMeta::fork_baseline`),
  /// and the conservation recorder starts at 0 so the baseline is observed as the child's
  /// opening history — a snapshot-wired latecomer gets the identical treatment without ever
  /// passing through this path.
  fn wire_fork_replica(&mut self, node: u64, fork: sailing_proto::GroupFork<u64, u64, LogSm>) {
    let child = fork.child;
    self.logs.insert((node, child), MemLog::new());
    self.stables.insert((node, child), MemStable::new());
    self.configs.insert((node, child), fork.config.clone());
    self.member_view.insert((node, child), true);
    *self.restarts.entry((node, child)).or_insert(0) += 1;
    // The fork boots at the node's next boot epoch (strictly above every prior incarnation on
    // this node, and >= 1 as the manufactured baseline requires).
    let epoch = {
      let e = self.boot_epochs.entry(node).or_insert(0);
      *e += 1;
      *e
    };
    let log = self.logs.get_mut(&(node, child)).expect("fresh log");
    let stable = self.stables.get_mut(&(node, child)).expect("fresh stable");
    self
      .hosts
      .get_mut(&node)
      .expect("host exists")
      .create_group_from_fork(
        child,
        fork.child_gen,
        fork.config,
        self.now,
        self.seed ^ node,
        fork.fsm,
        fork.blob,
        fork.read_only,
        epoch,
        log,
        stable,
      )
      .unwrap_or_else(|e| panic!("fork of group {child} on node {node}: {e:?}"));
    self
      .hosts
      .get_mut(&node)
      .expect("host exists")
      .lift_fork_barrier(&fork.parent, fork.split_index);
  }

  /// The ORACLE-ALIGNED applied record for `(node, gid)`:
  /// [`align_record`](Self::align_record) over the raw record under the group's live key
  /// population. The dropped cells are not unjudged: the conservation ledger compares inherited
  /// cells exact-cell across the handover, and quiesce equality reads the raw records once
  /// every replica converged. For a group that never split this is the raw record verbatim
  /// (own-tagged cells, full domain); an unregistered gid aligns as itself.
  pub(super) fn aligned_applied(&self, node: u64, gid: u64) -> AppliedLog {
    let raw = self.applied_of(node, gid);
    match self.groups.get(&gid) {
      Some(meta) => Self::align_record(raw, gid, &meta.keys),
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
  /// it), so every replica's aligned view is `filter(own(k))` — views differ across replicas
  /// only by k, an exact prefix relation, whatever the arrival path, replication lag, or
  /// number of onward splits on either side. A genuine divergence inside `own(k)` survives
  /// untouched (live key, own tag) and still trips the oracle.
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
  /// A FULL walk every sweep, on purpose: within one record values are monotone with position
  /// (one global counter, accepted in log order), so the per-`(ledger, key)` value dedupe
  /// admits each cell exactly once and a re-walk is pure no-op re-presentation. Any positional
  /// (or value-anchored) resume shortcut has a hole this oracle cannot afford: a split-apply
  /// shifts kept cells below the watermark within one settle window, and a crash restore
  /// resurrects moved cells a pre-crash sweep never met — both skip real cells forever. The
  /// world already pays O(record) per replica sweep to clone the record, so the walk adds no
  /// asymptotic cost.
  pub(super) fn conserve_sweep(&mut self, gid: u64) {
    let led = Self::ledger_id(self.generation_of(gid), gid);
    for node in self.node_ids.clone() {
      if !self.hosts[&node].contains_group(&gid) {
        continue;
      }
      let applied = self.applied_of(node, gid);
      for (index, cmd) in &applied {
        if let Some((_, key, value)) = super::super::decode_gkv(cmd) {
          match self.cons_last.get(&(led, key)) {
            Some(&last) if value <= last => {}
            _ => {
              self.conservation.record(led, key, *index, value);
              self.cons_last.insert((led, key), value);
            }
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
      self
        .conservation
        .assert_partition(rec.parent_led, rec.child_led, &rec.child_keys);
      drop(ctx); // no panic: disarm silently
    }
  }
}
