//! The deterministic multi-group world: one [`MultiRaft`] container per node over a group-tagged
//! typed bus. See the [module docs](crate::multi).
//!
//! The run loop is the exact analogue of [`Cluster::tick`](crate::Cluster): advance the single
//! global virtual clock to the earliest pending deadline, fire every due `(group, deadline)` on
//! every host, then settle (drain outgoing → deliver due → drain storage) until quiescent at that
//! timestamp. Per-node clock drift and the failover wall are deliberately absent in v1 — the
//! single-group VOPR retains that coverage; the hooks stay reserved here.

use super::oracles::{self, GrantKey};
use crate::{AppliedLog, Checker, DurableEntry, LogSm, MemLog, MemStable, checker};
use core::time::Duration;
use sailing_proto::{Config, Event, Instant, LogStore, Message, MultiRaft, Outgoing, StableStore};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

/// An in-flight group-tagged typed message: `(deliver_at, gid, from, to, message)`.
struct GInFlight {
  deliver_at: Instant,
  gid: u64,
  from: u64,
  to: u64,
  message: Message<u64>,
}

/// A deterministic world of [`MultiRaft`] container hosts. Nodes are empty containers until
/// [`create_group`](Self::create_group) wires a group onto its member nodes; each `(node, group)`
/// replica owns its own [`MemLog`]/[`MemStable`] pair, mirroring per-group stores in production.
pub struct MultiWorld {
  /// The world seed: threaded into every group's election jitter (the container folds the group
  /// id in, so co-located groups draw decorrelated timeouts).
  seed: u64,
  /// The single global virtual clock (every node's groups share its clock, as in production).
  now: Instant,
  /// Node ids in creation order (ascending in every current use; kept explicit for determinism).
  node_ids: Vec<u64>,
  /// One container host per node.
  hosts: BTreeMap<u64, MultiRaft<u64, u64, LogSm>>,
  /// Per-`(node, gid)` log store.
  logs: BTreeMap<(u64, u64), MemLog>,
  /// Per-`(node, gid)` stable store.
  stables: BTreeMap<(u64, u64), MemStable<u64>>,
  /// The group-tagged typed bus.
  bus: VecDeque<GInFlight>,
  /// Fully-partitioned node ids: their outgoing messages are discarded and inbound messages
  /// to/from them are dropped (a node-level partition takes ALL of the node's groups out).
  isolated: BTreeSet<u64>,
  /// Completed [`tick`](Self::tick)s (threaded into oracle panics for replay).
  tick_count: u64,
  /// One safety-oracle suite PER GROUP — unchanged oracle code, parameterized by the per-group
  /// [`ClusterView`](crate::ClusterView) assembled from the group's hosting nodes.
  checkers: BTreeMap<u64, Checker>,
  /// The one-identity tripwire: `(granter, gid, gen, term) → grantee` over every REAL-vote grant
  /// any replica ever sends (see [`oracles::note_grant`]).
  grants: BTreeMap<GrantKey, u64>,
  /// Per-`(node, gid)` count of applied entries already cross-talk-swept (the sweep high-water).
  swept: BTreeMap<(u64, u64), usize>,
  /// Per-`(node, gid)` count of `Event::ConfChanged` drained (the [`NodeView`](crate::NodeView)
  /// `conf_changed` feed and the per-group conf-change settle signal).
  conf_changed: BTreeMap<(u64, u64), u64>,
  /// `(node, gid)` replicas whose membership is SNAPSHOT-DERIVED (a transferred snapshot
  /// installed) — sticky, mirroring the single-group lineage flag.
  snapshot_lineage: BTreeSet<(u64, u64)>,
  /// Per-group committed conf-change transitions observed since the last check (fed to the
  /// membership oracle exactly as `Cluster::pending_transitions` is, then cleared).
  pending_transitions: BTreeMap<u64, Vec<(u64, u64, checker::ConfSnapshot)>>,
  /// Per-group new transfer-snapshot installs observed since the last check (then cleared).
  pending_new_installs: BTreeMap<u64, Vec<(u64, u64, checker::ConfSnapshot)>>,
  /// The harness-side group registry: one [`lifecycle::GroupMeta`] per logical group id, across
  /// incarnations (retirement flips `retired`; recreation bumps `generation`).
  groups: BTreeMap<u64, lifecycle::GroupMeta>,
  /// Frozen checker archive for retired incarnations, keyed `(gid, generation)` — each ran one
  /// final check at removal and keeps its cross-tick history inspectable.
  retired: BTreeMap<(u64, u64), Checker>,
}

impl MultiWorld {
  /// An empty world (no nodes, no groups) at the clock origin.
  pub fn new(seed: u64) -> Self {
    Self {
      seed,
      now: Instant::ORIGIN,
      node_ids: Vec::new(),
      hosts: BTreeMap::new(),
      logs: BTreeMap::new(),
      stables: BTreeMap::new(),
      bus: VecDeque::new(),
      isolated: BTreeSet::new(),
      tick_count: 0,
      checkers: BTreeMap::new(),
      grants: BTreeMap::new(),
      swept: BTreeMap::new(),
      conf_changed: BTreeMap::new(),
      snapshot_lineage: BTreeSet::new(),
      pending_transitions: BTreeMap::new(),
      pending_new_installs: BTreeMap::new(),
      groups: BTreeMap::new(),
      retired: BTreeMap::new(),
    }
  }

  /// Add node `id` as an empty container host (no groups). Panics if the id already exists.
  pub fn add_node(&mut self, id: u64) {
    assert!(
      self.hosts.insert(id, MultiRaft::new()).is_none(),
      "add_node: node {id} already exists"
    );
    self.node_ids.push(id);
  }

  /// Create group `gid` on every node in `voters` (each node id must already exist). Each member
  /// gets a fresh `(node, gid)` store pair and a fresh replica seeded from the world seed (the
  /// container folds `gid` in, so per-group election jitter is decorrelated for free). Panics on
  /// any admission error — a world-construction bug, not weather.
  pub fn create_group(&mut self, gid: u64, voters: &BTreeSet<u64>) {
    assert!(
      !self.groups.contains_key(&gid),
      "create_group: group id {gid} was already used (ids are single-incarnation; a retired \
       logical group rejoins via recreate_group)"
    );
    assert!(
      self.checkers.insert(gid, Checker::new()).is_none(),
      "create_group: group {gid} already exists"
    );
    self.groups.insert(
      gid,
      lifecycle::GroupMeta {
        voters: voters.clone(),
        ..lifecycle::GroupMeta::default()
      },
    );
    let voter_vec: Vec<u64> = voters.iter().copied().collect();
    for &node in voters {
      let config = Config::try_new(
        node,
        voter_vec.clone(),
        Duration::from_millis(1000),
        Duration::from_millis(100),
      )
      .expect("valid multi-world config");
      self.wire_replica(node, gid, config);
    }
  }

  /// Wire one `(node, gid)` replica: fresh stores + container admission under `config`. The shared
  /// seam for group creation (voter bootstrap here; observers and recreation reuse it later).
  fn wire_replica(&mut self, node: u64, gid: u64, config: Config<u64>) {
    let host = self
      .hosts
      .get_mut(&node)
      .unwrap_or_else(|| panic!("wire_replica: node {node} was never added"));
    self.logs.insert((node, gid), MemLog::new());
    self.stables.insert((node, gid), MemStable::new());
    // Per-NODE seed decorrelation: the container folds the GROUP id into the seed (co-located
    // groups on one host draw distinct jitter), but replicas of the SAME group on DIFFERENT nodes
    // need distinct base seeds too — identical streams under the shared global clock would draw
    // identical election timeouts and split votes forever (the single-group harness seeds each
    // Endpoint by node id for the same reason).
    host
      .create_group(gid, config, self.now, self.seed ^ node, LogSm::new())
      .unwrap_or_else(|e| panic!("wire_replica: admission of group {gid} on node {node}: {e:?}"));
  }

  /// The id of a node currently believing itself leader of `gid`, if any (first in node order).
  pub fn leader_of(&self, gid: u64) -> Option<u64> {
    self
      .node_ids
      .iter()
      .find(|&&n| {
        self.hosts[&n]
          .group(&gid)
          .is_some_and(|ep| ep.role().is_leader())
      })
      .copied()
  }

  /// Propose `cmd` on `gid`'s current leader; returns the assigned index (`None` when the group is
  /// momentarily leaderless or the leader refuses).
  pub fn propose(&mut self, gid: u64, cmd: &[u8]) -> Option<sailing_proto::Index> {
    let leader = self.leader_of(gid)?;
    let host = self.hosts.get_mut(&leader).expect("leader host exists");
    let log = self.logs.get_mut(&(leader, gid)).expect("leader log");
    let stable = self.stables.get(&(leader, gid)).expect("leader stable");
    host
      .propose(
        &gid,
        self.now,
        log,
        stable,
        &bytes::Bytes::copy_from_slice(cmd),
      )?
      .ok()
  }

  /// Node `node`'s applied `(index, command-bytes)` sequence for `gid` (empty if the node does not
  /// host the group).
  pub fn applied_of(&self, node: u64, gid: u64) -> AppliedLog {
    self
      .hosts
      .get(&node)
      .and_then(|h| h.group(&gid))
      .map(|ep| {
        ep.state_machine()
          .applied()
          .iter()
          .map(|(idx, cmd)| (idx.get(), cmd.to_vec()))
          .collect()
      })
      .unwrap_or_default()
  }

  /// True if every hosting node's applied sequence for `gid` agrees as a prefix of the longest —
  /// the State Machine Safety core, scoped to one group.
  pub fn agreement_holds(&self, gid: u64) -> bool {
    let logs: Vec<&[(sailing_proto::Index, bytes::Bytes)]> = self
      .node_ids
      .iter()
      .filter_map(|n| self.hosts[n].group(&gid))
      .map(|ep| ep.state_machine().applied())
      .collect();
    let longest = logs.iter().map(|l| l.len()).max().unwrap_or(0);
    for k in 0..longest {
      let mut seen: Option<&(sailing_proto::Index, bytes::Bytes)> = None;
      for l in &logs {
        if let Some(cell) = l.get(k) {
          match seen {
            None => seen = Some(cell),
            Some(s) => {
              if s != cell {
                return false;
              }
            }
          }
        }
      }
    }
    true
  }

  /// Tick until `pred(self)` holds or `max_ticks` elapse; returns whether it held.
  pub fn run_until(&mut self, max_ticks: u32, pred: impl Fn(&Self) -> bool) -> bool {
    for _ in 0..max_ticks {
      if pred(self) {
        return true;
      }
      self.tick();
    }
    pred(self)
  }

  /// Advance the simulation one step (the exact analogue of `Cluster::tick`): advance the global
  /// clock to the earliest pending deadline, fire every due `(host, group)` timer, then settle —
  /// drain outgoing onto the bus, deliver due messages, drain storage completions — until
  /// quiescent at this timestamp. Returns whether any work happened.
  pub fn tick(&mut self) -> bool {
    let mut progressed = false;

    // Step a+b: advance the clock and fire due timers. `poll_timeout` is each container's minimum
    // over its groups; the single global clock needs no per-node folding (drift is reserved).
    let next_timer = self.hosts.values().filter_map(|h| h.poll_timeout()).min();
    let next_msg = self.bus.iter().map(|m| m.deliver_at).min();
    if let Some(target) = [next_timer, next_msg].into_iter().flatten().min() {
      if target > self.now {
        self.now = target;
        progressed = true;
      }
      for node in self.node_ids.clone() {
        let host = self.hosts.get_mut(&node).expect("host exists");
        // Collect first: firing a timer mutates the host, invalidating the `deadlines()` borrow.
        let due: Vec<u64> = host
          .deadlines()
          .filter(|(_, d)| *d <= self.now)
          .map(|(gid, _)| gid)
          .collect();
        for gid in due {
          progressed = true;
          let host = self.hosts.get_mut(&node).expect("host exists");
          let log = self.logs.get_mut(&(node, gid)).expect("replica log");
          let stable = self.stables.get_mut(&(node, gid)).expect("replica stable");
          host
            .handle_timeout(&gid, self.now, log, stable)
            .expect("due group is hosted");
        }
      }
    }

    // Flush each replica's coalesced replication batch ONCE before the settle loop (re-flushing
    // each pass would re-send to a still-Probe peer and the progress flag would never clear).
    for node in self.node_ids.clone() {
      let host = self.hosts.get_mut(&node).expect("host exists");
      let gids: Vec<u64> = host.group_ids().copied().collect();
      for gid in gids {
        let host = self.hosts.get_mut(&node).expect("host exists");
        let log = self.logs.get(&(node, gid)).expect("replica log");
        let stable = self.stables.get(&(node, gid)).expect("replica stable");
        host
          .flush_appends(&gid, self.now, log, stable)
          .expect("hosted group flushes");
      }
    }

    // Step c: drain outgoing → deliver due → drain storage, until quiescent at this timestamp.
    let mut iters = 0u32;
    loop {
      iters += 1;
      assert!(
        iters <= 10_000,
        "MultiWorld::tick inner loop exceeded 10_000 iterations — livelock?"
      );

      let any_new = self.drain_outgoing_all();
      let delivered = self.deliver_due();
      let storage_produced = self.drain_storage_all();
      progressed |= any_new || delivered || storage_produced;

      if !any_new && !delivered && !storage_produced {
        break;
      }
    }

    self.tick_count += 1;
    // The world is quiescent at this timestamp — a consistent observable state. Run the whole
    // per-group oracle suite plus the cross-talk sweep; a violation panics with seed + tick.
    self.check_now();
    progressed
  }

  /// Run the per-group safety-oracle suites and the cross-group cross-talk sweep against the
  /// current state, panicking with the oracle name + seed + tick on a violation. Called at the
  /// end of every [`tick`](Self::tick); exposed so tests can also invoke it at a chosen point.
  pub fn check_now(&mut self) {
    let gids: Vec<u64> = self.checkers.keys().copied().collect();
    for gid in gids {
      let view = self.group_view(gid);
      self
        .checkers
        .get_mut(&gid)
        .expect("checker exists")
        .check_or_panic(&view);
      // The checker folded this view's transitions/installs; clear so the next batch is fresh.
      self.pending_transitions.entry(gid).or_default().clear();
      self.pending_new_installs.entry(gid).or_default().clear();
      self.cross_talk_sweep(gid);
    }
  }

  /// Assert every NEWLY applied entry on every replica of `gid` decodes (when gid-tagged) to
  /// `gid` itself — the O(1)-per-apply cross-group isolation oracle.
  fn cross_talk_sweep(&mut self, gid: u64) {
    for node in self.node_ids.clone() {
      if !self.hosts[&node].contains_group(&gid) {
        continue;
      }
      let applied = self.applied_of(node, gid);
      let hw = self.swept.entry((node, gid)).or_insert(0);
      // A crash-restore can legitimately SHRINK the applied prefix (apply outruns the batched
      // commit persist); clamp, and re-sweeping a replayed suffix is harmless (same entries).
      let start = (*hw).min(applied.len());
      oracles::assert_no_cross_talk(self.seed, self.tick_count, node, gid, &applied[start..]);
      *hw = applied.len();
    }
  }

  /// The group's incarnation (gen) for the one-identity grant key, from the lifecycle registry
  /// (recreation is what moves it).
  fn gen_of(&self, gid: u64) -> u64 {
    self.generation_of(gid)
  }

  /// Assemble the per-group [`ClusterView`](crate::ClusterView) from `gid`'s hosting nodes —
  /// field-for-field the shape `Cluster::view` builds, scoped to one group's replicas and their
  /// `(node, gid)` stores, so the UNCHANGED oracle suite judges each group independently.
  fn group_view(&self, gid: u64) -> checker::ClusterView {
    let mut nodes = Vec::new();
    for &node in &self.node_ids {
      let Some(ep) = self.hosts[&node].group(&gid) else {
        continue;
      };
      let log = &self.logs[&(node, gid)];
      let stable = &self.stables[&(node, gid)];
      let durable_first = log.durable_first_index().get();
      let durable_last = log.durable_last_index().get();
      let visible_last = log.last_index().get();
      let durable_entries: Vec<DurableEntry> = log
        .durable_entries()
        .iter()
        .map(|e| DurableEntry {
          index: e.index().get(),
          term: e.term().get(),
          data: e.data().to_vec(),
          is_conf_change: e.kind().is_conf_change(),
        })
        .collect();
      let (snapshot_last_index, snapshot_last_term) = match stable.durable_snapshot() {
        Some(meta) => (meta.last_index().get(), meta.last_term().get()),
        None => (0, 0),
      };
      let applied_log: Vec<(u64, Vec<u8>)> = ep
        .state_machine()
        .applied()
        .iter()
        .map(|(idx, cmd)| (idx.get(), cmd.to_vec()))
        .collect();
      let cs = ep.conf_state();
      nodes.push(checker::NodeView {
        id: node,
        removed: false,
        is_voter: cs.is_voter(&node),
        poisoned: ep.is_poisoned(),
        is_leader: ep.role().is_leader(),
        term: ep.term().get(),
        commit: ep.commit_index().get(),
        applied: ep.applied_index().get(),
        applied_log,
        durable_first,
        durable_last,
        visible_last,
        durable_entries,
        snapshot_last_index,
        snapshot_last_term,
        installed_snapshot: self.snapshot_lineage.contains(&(node, gid)),
        conf_voters: cs.voters().clone(),
        conf_voters_outgoing: cs.voters_outgoing().clone(),
        conf_learners: cs.learners().clone(),
        conf_learners_next: cs.learners_next().clone(),
        conf_auto_leave: cs.auto_leave(),
        conf_changed: self.conf_changed.get(&(node, gid)).copied().unwrap_or(0),
        hardstate_commit: stable.hard_state().commit().get(),
        inflight_staged: usize::from(log.has_inflight()) + usize::from(stable.has_inflight()),
        incarnation: 0,
      });
    }
    checker::ClusterView {
      seed: self.seed,
      tick: self.tick_count,
      committed_voters: {
        let v = self.committed_voters_of(gid);
        if v.is_empty() { None } else { Some(v) }
      },
      committed_transitions: self
        .pending_transitions
        .get(&gid)
        .cloned()
        .unwrap_or_default(),
      new_installs: self
        .pending_new_installs
        .get(&gid)
        .cloned()
        .unwrap_or_default(),
      nodes,
    }
  }

  /// The group's REAL committed VOTER set, read exactly as `Cluster::committed_voters` reads it:
  /// the HIGHEST-TERM leader among the group's hosting replicas is authoritative; leaderless,
  /// the most common committed voter set across hosting replicas (ties to the first-sorting
  /// set), so the result is a pure function of world state.
  fn committed_voters_of(&self, gid: u64) -> BTreeSet<u64> {
    let authoritative = self
      .node_ids
      .iter()
      .filter_map(|&n| self.hosts[&n].group(&gid).map(|ep| (n, ep)))
      .filter(|(_, ep)| ep.role().is_leader())
      .max_by_key(|(_, ep)| ep.term());
    if let Some((_, ep)) = authoritative {
      return ep.conf_state().voters().iter().copied().collect();
    }
    let mut tally: BTreeMap<BTreeSet<u64>, usize> = BTreeMap::new();
    for &n in &self.node_ids {
      let Some(ep) = self.hosts[&n].group(&gid) else {
        continue;
      };
      let voters: BTreeSet<u64> = ep.conf_state().voters().iter().copied().collect();
      *tally.entry(voters).or_insert(0) += 1;
    }
    tally
      .into_iter()
      .max_by(|(a_set, a_n), (b_set, b_n)| a_n.cmp(b_n).then_with(|| b_set.cmp(a_set)))
      .map(|(set, _)| set)
      .unwrap_or_default()
  }

  /// Drain every host's outgoing `(gid, message)` queue onto the bus (isolated hosts drain to the
  /// void) and its event queue. Returns whether any message reached the bus.
  fn drain_outgoing_all(&mut self) -> bool {
    let mut any_new = false;
    for node in self.node_ids.clone() {
      if self.isolated.contains(&node) {
        let host = self.hosts.get_mut(&node).expect("host exists");
        while host.poll_message().is_some() {}
      } else {
        // Re-borrow the host each iteration: `schedule_send` needs `&mut self`, so the poll
        // borrow cannot be held across it.
        while let Some((gid, out)) = self
          .hosts
          .get_mut(&node)
          .expect("host exists")
          .poll_message()
        {
          any_new = true;
          let (to, message) = Outgoing::into_parts(out);
          self.schedule_send(node, gid, to, message);
        }
      }
      self.drain_host_events(node);
    }
    any_new
  }

  /// THE single event-drain for node `node`'s container: every drain site routes here so no
  /// tracked event is cherry-picked or dropped on any path (the single-group harness's rule).
  ///   - `SnapshotInstalled` → the sticky per-`(node, gid)` snapshot-membership lineage AND the
  ///     group's new-install feed for the membership oracle.
  ///   - `ConfChanged` → the per-`(node, gid)` counter AND (from a LOG-BUILT replica only) the
  ///     group's committed-config transition at its exact index, tagged with the conf-change
  ///     ENTRY's term (a non-faulting log lookup — not the replica's current term).
  ///   - `ConfChanged` whose resulting config no longer lists the replica ITSELF → the replica
  ///     applied its own removal (the farewell append landed): the embedder-on-RemovedSelf
  ///     teardown drops it from the container after the drain.
  fn drain_host_events(&mut self, node: u64) {
    let mut self_removed: Vec<u64> = Vec::new();
    loop {
      let host = self.hosts.get_mut(&node).expect("host exists");
      let Some((gid, ev)) = host.poll_event() else {
        break;
      };
      match ev {
        Event::SnapshotInstalled(meta) => {
          self.snapshot_lineage.insert((node, gid));
          self.pending_new_installs.entry(gid).or_default().push((
            node,
            meta.last_index().get(),
            checker::ConfSnapshot::from_conf_state(meta.conf()),
          ));
        }
        Event::ConfChanged(cc) => {
          *self.conf_changed.entry((node, gid)).or_insert(0) += 1;
          {
            let cs = cc.conf();
            if !cs.voters().contains(&node)
              && !cs.voters_outgoing().contains(&node)
              && !cs.learners().contains(&node)
              && !cs.learners_next().contains(&node)
            {
              self_removed.push(gid);
            }
          }
          if !self.snapshot_lineage.contains(&(node, gid)) {
            let idx = cc.index();
            let entry_term = {
              let commit = self.hosts[&node]
                .group(&gid)
                .expect("event source is hosted")
                .commit_index();
              self.logs[&(node, gid)]
                .committed_entries_no_fault(commit)
                .iter()
                .find(|e| e.index() == idx)
                .map(|e| e.term().get())
                .unwrap_or(0)
            };
            self.pending_transitions.entry(gid).or_default().push((
              idx.get(),
              entry_term,
              checker::ConfSnapshot::from_conf_state(cc.conf()),
            ));
          }
        }
        _ => {}
      }
    }
    // Embedder-on-RemovedSelf teardown, after the drain so it never truncates the event pass.
    for gid in self_removed {
      if self.hosts[&node].contains_group(&gid) {
        self.drop_group_replica(gid, node);
      }
    }
  }

  /// Run the structural send-point oracles on a message `from` is sending for `gid`, then push
  /// it onto the bus (fault-free for now: zero latency, FIFO, exactly once).
  ///
  /// The tripwires run on every SENT message, BEFORE any future drop/duplicate roll, so a
  /// dropped message can never bypass an oracle (the single-group ordering rule):
  ///   (a) append-before-ack — a success `AppendResponse` must not outrun the replica's
  ///       readable `(node, gid)` log;
  ///   (b) one-identity — a REAL-vote grant binds `(granter, gid, gen, term)` to one candidate
  ///       across every replica object this node ever hosts for the group.
  fn schedule_send(&mut self, from: u64, gid: u64, to: u64, message: Message<u64>) {
    if let Message::AppendResponse(a) = &message
      && !a.reject()
    {
      let log = &self.logs[&(from, gid)];
      assert!(
        log.last_index() >= a.match_index(),
        "append-before-ack violated: node {from} group {gid} acked {:?} but last_index is {:?} \
         (durable_last={:?} inflight={})\n  seed={} tick={}",
        a.match_index(),
        log.last_index(),
        log.durable_last_index(),
        log.has_inflight(),
        self.seed,
        self.tick_count,
      );
    }
    if let Message::VoteResponse(vr) = &message
      && !vr.reject()
      && !vr.pre_vote()
    {
      let generation = self.gen_of(gid);
      oracles::note_grant(
        &mut self.grants,
        self.seed,
        self.tick_count,
        (from, gid, generation, vr.term()),
        to,
      );
    }
    self.bus.push_back(GInFlight {
      deliver_at: self.now,
      gid,
      from,
      to,
      message,
    });
  }

  /// Deliver every bus message due at or before `now`. A message to a node that does not host its
  /// group is dropped SILENTLY (the unhosted-drop semantics of the group-tagged wire); a message
  /// with either endpoint isolated is dropped by the partition. Returns whether any delivered.
  fn deliver_due(&mut self) -> bool {
    let mut delivered = false;
    let mut rest: VecDeque<GInFlight> = VecDeque::new();
    while let Some(m) = self.bus.pop_front() {
      if m.deliver_at > self.now {
        rest.push_back(m);
        continue;
      }
      if self.isolated.contains(&m.from) || self.isolated.contains(&m.to) {
        continue; // partition swallows it
      }
      let Some(host) = self.hosts.get_mut(&m.to) else {
        continue; // unknown node id — drop safely
      };
      if !host.contains_group(&m.gid) {
        continue; // unhosted group — silent drop, the connection-level tombstone/demux semantics
      }
      delivered = true;
      let log = self.logs.get_mut(&(m.to, m.gid)).expect("replica log");
      let stable = self
        .stables
        .get_mut(&(m.to, m.gid))
        .expect("replica stable");
      host
        .handle_message(&m.gid, self.now, log, stable, m.from, m.message)
        .expect("hosted group handles");
    }
    self.bus = rest;
    delivered
  }

  /// Drain storage completions for every `(host, group)` and collect any messages they produce
  /// (deferred acks once a staged write flushes). Returns whether new work surfaced.
  fn drain_storage_all(&mut self) -> bool {
    let mut any_new = false;
    for node in self.node_ids.clone() {
      let host = self.hosts.get_mut(&node).expect("host exists");
      let gids: Vec<u64> = host.group_ids().copied().collect();
      for gid in gids {
        let host = self.hosts.get_mut(&node).expect("host exists");
        let log = self.logs.get_mut(&(node, gid)).expect("replica log");
        let stable = self.stables.get_mut(&(node, gid)).expect("replica stable");
        // A budget-bounded drain may leave completions queued (`MorePending`); count that as
        // progress so the settle loop keeps draining until every replica reports `Drained`.
        any_new |= host
          .handle_storage(&gid, self.now, log, stable)
          .expect("hosted group drains")
          .is_more_pending();
      }
    }
    // Collect outgoing produced by completion handlers — same path as the tick outgoing-drain.
    any_new |= self.drain_outgoing_all();
    any_new
  }
}

#[cfg(test)]
mod tests;

mod lifecycle;
