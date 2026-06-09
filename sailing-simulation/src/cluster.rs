//! A deterministic, single-threaded cluster of `Endpoint`s over an in-memory typed-message
//! bus and a virtual clock. M0 wires the loop; M1+ exercises real consensus through it.
use crate::{
  Checker, ClusterView, DurableEntry, LogSm, MemLog, MemStable, NetworkFaults, NodeView,
  StorageFaults, checker, network::NetPrng,
};
use core::time::Duration;
use sailing_proto::{
  ConfChange, ConfChangeV2, Config, Endpoint, Instant, LogStore, Message, Outgoing, ReadState,
  StableStore, Term,
};
use std::{
  collections::{BTreeMap, BTreeSet, VecDeque},
  vec::Vec,
};

/// Per-node snapshot-install tally: incremented each time an `Event::SnapshotInstalled`
/// is drained from that node's event queue during `tick`. Used by snapshot tests to
/// assert that `InstallSnapshot` was genuinely exercised.
type SnapCount = u64;

/// Per-node conf-change tally: incremented each time an `Event::ConfChanged`
/// is drained from that node's event queue during `tick`. Used by membership tests to
/// assert that conf changes were actually applied.
type ConfChangedCount = u64;

type Node = Endpoint<u64, LogSm>;

/// One node's applied log as `(index, command-bytes)` pairs, copied out for cross-run / cross-node
/// comparison (see [`Cluster::applied_entries_of`]). A `Vec<AppliedLog>` is the whole cluster's
/// applied state captured at a point in time.
pub type AppliedLog = Vec<(u64, Vec<u8>)>;

/// An in-flight typed message: `(deliver_at, from, to, message)`.
struct InFlight {
  deliver_at: Instant,
  from: u64,
  to: u64,
  message: Message<u64>,
}

/// A deterministic cluster. Node ids start at 0 and increase monotonically; nodes may
/// be added mid-run. The parallel `Vec`s (nodes/logs/stables/configs/…) are indexed by
/// position; `node_idx` maps id → Vec position for O(log n) lookups.
pub struct Cluster {
  /// Node ids, in Vec order (ids[i] is the id of the node at position i).
  node_ids: Vec<u64>,
  /// Reverse map: id → Vec position.
  node_idx: BTreeMap<u64, usize>,
  nodes: Vec<Node>,
  logs: Vec<MemLog>,
  stables: Vec<MemStable<u64>>,
  /// Config for each node, kept so `crash` can rebuild from durable stores.
  configs: Vec<Config<u64>>,
  bus: VecDeque<InFlight>,
  now: Instant,
  /// Node ids that are fully partitioned: their outgoing messages are dropped and
  /// inbound messages to/from them are dropped. Init empty.
  isolated: BTreeSet<u64>,
  /// Node ids that have been removed from the cluster. The agreement oracle skips
  /// consistency checks for removed nodes' applied-log suffixes beyond the point of removal.
  /// Removed nodes are kept in the Vec structures but are also `isolated` so they don't
  /// receive further messages or participate in elections.
  removed: BTreeSet<u64>,
  /// Double-vote tripwire: maps `(granter, term)` → `grantee`.
  /// A second distinct grantee for the same `(granter, term)` is a fatal bug.
  grants: BTreeMap<(u64, Term), u64>,
  /// Per-node count of `Event::SnapshotInstalled` events drained during `tick`.
  /// Monotonically incremented; reset to zero on `crash`+restart.
  snapshot_installs: Vec<SnapCount>,
  /// Per-node count of `Event::ConfChanged` events drained during `tick`.
  /// Monotonically incremented; never reset.
  conf_changed: Vec<ConfChangedCount>,
  /// Per-node list of `ReadState`s confirmed via `Event::ReadState` during `tick`.
  /// Appended monotonically; never cleared. Index into the outer Vec by node position.
  read_states: Vec<Vec<ReadState>>,
  /// When true, the stores run in [`crate::StoreMode::Async`] (staged writes / fsync-loss window):
  /// `tick` flushes every node's staged writes each step (before draining completions), and a
  /// `crash` that discards in-flight writes loses exactly the un-flushed window. Default false
  /// (synchronous stores, byte-identical to M0–M7).
  async_mode: bool,
  /// Seeded network fault model applied per message at the bus-push point (latency/jitter/drop/
  /// duplicate/reorder). Default [`NetworkFaults::none()`] — a faultless, zero-latency, FIFO bus
  /// byte-identical to M0–M7 + M8-U1. Installed via [`Cluster::set_network_faults`].
  net_faults: NetworkFaults,
  /// Seeded network-fault PRNG, on a stream distinct from the per-node store seeds. Drives every
  /// drop/dup roll and jitter draw, so the same cluster seed yields an identical run. Only consumed
  /// when `net_faults` is non-`none()` (an all-off config touches the PRNG only for the bounded
  /// drop/dup checks, which short-circuit on a `0` rate without a draw — see [`NetPrng`]).
  net_prng: NetPrng,
  /// Per-`(from,to)` last-scheduled `deliver_at`, used to keep deliveries FIFO when
  /// `net_faults.reorder == false`: a message's `deliver_at` is clamped to be ≥ the previous one
  /// for that ordered pair. Empty (and unused) when reorder is on or faults are off.
  net_last_sched: BTreeMap<(u64, u64), Instant>,
  /// Count of messages dropped by the seeded network fault model (non-vacuity counter so tests can
  /// assert the fault model actually fired). Never incremented by partition/isolation drops.
  net_dropped: u64,
  /// Count of messages duplicated by the seeded network fault model (each fired duplication counts
  /// once, i.e. the number of EXTRA copies pushed). Non-vacuity counter.
  net_duplicated: u64,
  /// The per-tick safety-oracle suite (M8-U3). Holds the cross-tick history (commit/term
  /// monotonicity, the committed-history high-water) and runs the WHOLE oracle suite at the end of
  /// every [`tick`](Self::tick); a violation panics with the oracle name + seed + tick for exact
  /// VOPR replay. A pure observer — it never mutates the simulated nodes/stores and never draws a
  /// PRNG, so the run is byte-identical with or without it. See [`crate::checker`].
  checker: Checker,
  /// The cluster construction seed, threaded into the oracle panic for VOPR replay. Captured from
  /// the seed passed to [`new_async`](Self::new_async); `0` for the (seedless) sync constructors.
  seed: u64,
  /// Monotonic count of completed [`tick`](Self::tick)s, threaded into the oracle panic so a
  /// violation pinpoints the exact step to replay.
  tick_count: u64,
}

impl Cluster {
  /// Build an `n`-node cluster (ids `0..n`), each a fresh Follower.
  pub fn new(n: usize) -> Self {
    Self::new_with(n, |cfg| cfg)
  }

  /// Build an `n`-node cluster and apply `configure` to each node's `Config` after
  /// construction. Use this to override flow-control knobs (e.g. `max_inflight_msgs`)
  /// for targeted tests while keeping `new` unchanged.
  pub fn new_with(n: usize, configure: impl Fn(Config<u64>) -> Config<u64>) -> Self {
    Self::new_inner(n, configure, false, 0)
  }

  /// Build an `n`-node cluster whose stores run in [`crate::StoreMode::Async`] (staged writes /
  /// fsync-loss window), seeded with `seed`.
  ///
  /// In async mode `submit_*` stages a write that is made durable only when `tick` flushes it
  /// the next step; a `crash` between submit and the next flush loses that in-flight write (and
  /// the node recovers via re-replication / commit persistence). This is what makes the proto's
  /// durability-ordering rules (append-before-ack, persist-vote-before-grant, deferred-compact,
  /// commit persistence) MEANINGFUL under crash. Storage faults stay off unless installed.
  pub fn new_async(n: usize, seed: u64) -> Self {
    Self::new_inner(n, |cfg| cfg, true, seed)
  }

  /// Shared constructor body. `async_mode` selects [`crate::StoreMode::Async`] stores (seeded with
  /// `seed` for any storage faults); `false` keeps the default synchronous stores so `new` /
  /// `new_with` are byte-identical to M0–M7.
  fn new_inner(
    n: usize,
    configure: impl Fn(Config<u64>) -> Config<u64>,
    async_mode: bool,
    seed: u64,
  ) -> Self {
    let mut nodes = Vec::with_capacity(n);
    let mut logs = Vec::with_capacity(n);
    let mut stables = Vec::with_capacity(n);
    let mut configs = Vec::with_capacity(n);
    let mut node_ids = Vec::with_capacity(n);
    let mut node_idx = BTreeMap::new();
    let voters: Vec<u64> = (0..n as u64).collect();
    for id in 0..n as u64 {
      let base = Config::try_new(
        id,
        voters.clone(),
        Duration::from_millis(1000),
        Duration::from_millis(100),
      )
      .expect("valid config");
      let cfg = configure(base);
      nodes.push(Endpoint::new(
        cfg.clone(),
        Instant::ORIGIN,
        id,
        LogSm::new(),
      ));
      configs.push(cfg);
      // Per-node store seeds derived from the cluster seed + id so each node's fault schedule is
      // distinct yet reproducible from `seed`.
      if async_mode {
        logs.push(MemLog::new_async(seed ^ id));
        stables.push(MemStable::new_async(seed.rotate_left(32) ^ id));
      } else {
        logs.push(MemLog::new());
        stables.push(MemStable::new());
      }
      node_idx.insert(id, id as usize);
      node_ids.push(id);
    }
    let snapshot_installs = vec![0u64; n];
    let conf_changed = vec![0u64; n];
    let read_states = vec![Vec::new(); n];
    Self {
      node_ids,
      node_idx,
      nodes,
      logs,
      stables,
      configs,
      bus: VecDeque::new(),
      now: Instant::ORIGIN,
      isolated: BTreeSet::new(),
      removed: BTreeSet::new(),
      grants: BTreeMap::new(),
      snapshot_installs,
      conf_changed,
      read_states,
      async_mode,
      net_faults: NetworkFaults::none(),
      // Network-fault PRNG seed: derived from the cluster seed on a stream DISTINCT from the
      // per-node store seeds (which use `seed ^ id` / `seed.rotate_left(32) ^ id`), so the network
      // schedule is reproducible yet independent of storage faults. `0x4E_4554` spells "NET".
      net_prng: NetPrng::new(seed.rotate_left(16) ^ 0x004E_4554),
      net_last_sched: BTreeMap::new(),
      net_dropped: 0,
      net_duplicated: 0,
      checker: Checker::new(),
      seed,
      tick_count: 0,
    }
  }

  /// Number of nodes (including removed ones, which are kept in the Vec but isolated).
  pub fn size(&self) -> usize {
    self.nodes.len()
  }

  /// Number of live (non-removed) nodes.
  pub fn live_size(&self) -> usize {
    self.nodes.len() - self.removed.len()
  }

  /// The current virtual time.
  pub fn now(&self) -> Instant {
    self.now
  }

  /// The id of a node that currently believes itself leader, if any.
  pub fn leader(&self) -> Option<u64> {
    self
      .node_ids
      .iter()
      .enumerate()
      .filter(|(_, id)| !self.removed.contains(id))
      .find(|(i, _)| self.nodes[*i].role().is_leader())
      .map(|(_, &id)| id)
  }

  /// Tick until `predicate(self)` holds or `max_steps` elapse; returns whether it held.
  pub fn run_until(&mut self, max_steps: usize, mut predicate: impl FnMut(&Self) -> bool) -> bool {
    for _ in 0..max_steps {
      if predicate(self) {
        return true;
      }
      self.tick();
    }
    predicate(self)
  }

  /// How many nodes currently believe themselves leader (among non-removed nodes).
  pub fn leader_count(&self) -> usize {
    self
      .node_ids
      .iter()
      .enumerate()
      .filter(|(_, id)| !self.removed.contains(id))
      .filter(|(i, _)| self.nodes[*i].role().is_leader())
      .count()
  }

  /// The term of node `id`.
  pub fn term_of(&self, id: u64) -> sailing_proto::Term {
    let i = self.node_idx[&id];
    self.nodes[i].term()
  }

  /// The maximum term across all live (non-removed) nodes.
  ///
  /// Used by PreVote tests to assert that an isolated node's campaigns did NOT inflate
  /// the cluster term (with PreVote, the isolated node stays in PreCandidate without bumping
  /// its real term).
  pub fn max_term(&self) -> sailing_proto::Term {
    self
      .node_ids
      .iter()
      .enumerate()
      .filter(|(_, id)| !self.removed.contains(id))
      .map(|(i, _)| self.nodes[i].term())
      .max()
      .unwrap_or(sailing_proto::Term::ZERO)
  }

  /// The id of the current leader — same as `leader()`, a convenience alias so tests
  /// can write `cluster.leader_id()` alongside `term_of`, `max_term`, etc.
  pub fn leader_id(&self) -> Option<u64> {
    self.leader()
  }

  /// The role of node `id`.
  pub fn role_of(&self, id: u64) -> sailing_proto::Role {
    let i = self.node_idx[&id];
    self.nodes[i].role()
  }

  /// All `ReadState`s confirmed for node `id` (ever), in confirmation order.
  ///
  /// Populated by the `tick` inner loop from `Event::ReadState` events drained off the
  /// node's event queue. This list grows monotonically and is never cleared.
  pub fn read_states_of(&self, id: u64) -> &[ReadState] {
    let i = self.node_idx[&id];
    &self.read_states[i]
  }

  /// Initiate a linearizable read on the current leader with the given context bytes.
  ///
  /// Calls `Endpoint::read_index` on the leader.  Returns `true` if there is a leader
  /// (the call was made); `false` if no leader is available.
  ///
  /// The leader accepts the read (`read_index` returns `Ok`) for any fresh context; this
  /// helper asserts that, so a reused/duplicate context surfaces as a panic rather than a
  /// silently dropped read.  The confirmed `ReadState` will appear in `read_states_of(leader)`
  /// once a heartbeat-quorum round completes (for `ReadOnlySafe`) or immediately (for
  /// `ReadOnlyLeaseBased`).
  pub fn read_index(&mut self, context: &[u8]) -> bool {
    let leader = match self.leader() {
      Some(l) => l,
      None => return false,
    };
    let i = self.node_idx[&leader];
    let log = &self.logs[i];
    let stable = &self.stables[i];
    self.nodes[i]
      .read_index(
        self.now,
        log,
        stable,
        bytes::Bytes::copy_from_slice(context),
      )
      .expect("leader must accept the read_index for a fresh context");
    true
  }

  /// Initiate a leader transfer: ask the current leader to transfer to `to`.
  ///
  /// Returns `Ok(())` if the leader accepted the transfer, or an error if there is no
  /// leader / the transfer was refused (e.g. `to` is not a voter).
  pub fn transfer_leader(&mut self, to: u64) -> Result<(), sailing_proto::TransferError<u64>> {
    let leader = self
      .leader()
      .ok_or(sailing_proto::TransferError::NotLeader { leader: None })?;
    let i = self.node_idx[&leader];
    let log = &mut self.logs[i];
    let stable = &mut self.stables[i];
    self.nodes[i].transfer_leader(self.now, log, stable, to)
  }

  /// Isolate node `id`: drop all messages to and from it (a full two-way partition).
  pub fn isolate(&mut self, id: u64) {
    self.isolated.insert(id);
  }

  /// Heal the partition for node `id`: messages to/from it flow again.
  pub fn heal(&mut self, id: u64) {
    self.isolated.remove(&id);
  }

  /// The `first_index()` of node `id`'s durable log (advances after compaction).
  pub fn first_index_of(&self, id: u64) -> sailing_proto::Index {
    let i = self.node_idx[&id];
    self.logs[i].first_index()
  }

  /// Total number of `Event::SnapshotInstalled` events observed for node `id` since
  /// cluster construction (or the last `crash`).
  pub fn snapshot_install_count(&self, id: u64) -> u64 {
    let i = self.node_idx[&id];
    self.snapshot_installs[i]
  }

  /// Total `Event::SnapshotInstalled` events across ALL nodes.
  pub fn total_snapshot_installs(&self) -> u64 {
    self.snapshot_installs.iter().sum()
  }

  /// Total number of `Event::ConfChanged` events observed for node `id` since
  /// cluster construction.
  pub fn conf_changed_count(&self, id: u64) -> u64 {
    let i = self.node_idx[&id];
    self.conf_changed[i]
  }

  /// Total `Event::ConfChanged` events across ALL live (non-removed) nodes.
  pub fn total_conf_changed(&self) -> u64 {
    self
      .node_ids
      .iter()
      .enumerate()
      .filter(|(_, id)| !self.removed.contains(id))
      .map(|(i, _)| self.conf_changed[i])
      .sum()
  }

  /// Length of applied log for node `id`.
  pub fn applied_len_of(&self, id: u64) -> usize {
    let i = self.node_idx[&id];
    self.nodes[i].state_machine().applied().len()
  }

  /// Node `id`'s applied `(index, command-bytes)` sequence, copied out for cross-run comparison.
  ///
  /// Used by the network-fault determinism test to assert two runs of the same seed produce
  /// byte-identical applied logs. The agreement oracle ([`agreement_holds`](Self::agreement_holds))
  /// compares these prefixes across nodes; this exposes a single node's sequence.
  pub fn applied_entries_of(&self, id: u64) -> AppliedLog {
    let i = self.node_idx[&id];
    self.nodes[i]
      .state_machine()
      .applied()
      .iter()
      .map(|(idx, cmd)| (idx.get(), cmd.to_vec()))
      .collect()
  }

  /// Number of messages DROPPED by the seeded network fault model since construction (a non-vacuity
  /// counter so tests can assert the model actually fired). Excludes partition/isolation drops.
  pub fn net_dropped(&self) -> u64 {
    self.net_dropped
  }

  /// Number of message DUPLICATIONS fired by the seeded network fault model since construction
  /// (counts each fired duplication once, i.e. extra copies pushed). A non-vacuity counter.
  pub fn net_duplicated(&self) -> u64 {
    self.net_duplicated
  }

  /// Crash node `id`: lose all in-memory consensus state and any fsync still in-flight,
  /// but keep the durably-written store contents. The node is immediately restarted from
  /// its durable stores.
  pub fn crash(&mut self, id: u64) {
    let i = self.node_idx[&id];
    self.logs[i].discard_inflight();
    self.stables[i].discard_inflight();
    let cfg = self.configs[i].clone();
    let (log, stable) = (&mut self.logs[i], &mut self.stables[i]);
    self.nodes[i] = Endpoint::restart(cfg, self.now, 0x5EED ^ id, LogSm::new(), log, stable);
    // Reset the snapshot-install counter for the restarted node.
    self.snapshot_installs[i] = 0;
    // Drain any messages left in the bus to/from this node (stale in-flight traffic).
    self.bus.retain(|m| m.from != id && m.to != id);
    // Drop the FIFO bookkeeping for any pair touching this node so the restarted node starts
    // FIFO-fresh (a stale high-water mark would needlessly delay its new traffic). No-op when the
    // network fault model is off (the map is empty).
    self.net_last_sched.retain(|&(f, t), _| f != id && t != id);
  }

  /// The durable `last_index()` of node `id`'s log. In async mode this reflects only flushed
  /// (durable) appends — a staged-but-unflushed append is invisible here.
  pub fn last_index_of(&self, id: u64) -> sailing_proto::Index {
    let i = self.node_idx[&id];
    self.logs[i].last_index()
  }

  /// Whether node `id` currently has a staged (submitted-but-not-yet-flushed) store write — i.e.
  /// it is sitting inside the fsync-loss window. Always `false` in sync mode. Used by crash-window
  /// tests to assert the crash genuinely lands mid-window (non-vacuity).
  pub fn node_has_inflight(&self, id: u64) -> bool {
    let i = self.node_idx[&id];
    self.logs[i].has_inflight() || self.stables[i].has_inflight()
  }

  /// Install a seeded [`StorageFaults`] config on node `id`'s stores (both log and stable),
  /// re-seeding their fault PRNGs from `seed` so the schedule is reproducible. Faults surface as
  /// VALUES (a read returns the store error → the proto poisons; a torn write drops a staged
  /// append) and NEVER panic. Defaults are all-off, so unfaulted nodes are unaffected.
  pub fn set_node_faults(&mut self, id: u64, faults: StorageFaults, seed: u64) {
    let i = self.node_idx[&id];
    self.logs[i].set_faults(faults, seed);
    self.stables[i].set_faults(faults, seed.rotate_left(17));
  }

  /// Install a seeded [`NetworkFaults`] config on the typed-message bus: per-message
  /// latency/jitter/drop/duplicate/reorder applied at the bus-push point in [`tick`](Self::tick),
  /// AFTER the structural oracles run. Faults are deterministic given the cluster seed (the network
  /// PRNG was seeded from it at construction). Defaults are all-off, so `new`/`new_async` keep a
  /// faultless, zero-latency, FIFO bus byte-identical to M0–M7 + M8-U1.
  ///
  /// Re-seeds the network-fault PRNG from `seed` and clears the FIFO bookkeeping so the schedule is
  /// reproducible from the call site (pass the cluster seed, or a fresh seed for a distinct stream).
  /// Isolated/partitioned nodes still drop entirely — that full-partition behavior is independent
  /// of these per-message faults.
  pub fn set_network_faults(&mut self, faults: NetworkFaults, seed: u64) {
    self.net_faults = faults;
    self.net_prng = NetPrng::new(seed);
    self.net_last_sched.clear();
  }

  /// Whether node `id` is poisoned (a fatal storage/apply error has made it inert). In async mode
  /// a `transient_read` fault that fires on a committed-range read poisons the node via the
  /// proto's review-C2 path.
  pub fn is_poisoned(&self, id: u64) -> bool {
    let i = self.node_idx[&id];
    self.nodes[i].is_poisoned()
  }

  /// The [`sailing_proto::PoisonReason`] of node `id`, or `None` if healthy.
  pub fn poison_reason_of(&self, id: u64) -> Option<sailing_proto::PoisonReason> {
    let i = self.node_idx[&id];
    self.nodes[i].poison_reason()
  }

  /// Deterministically drive the cluster until node `keep` is sitting inside the fsync window
  /// (has a staged-but-unflushed append), WITHOUT ever flushing `keep`. Returns `true` once the
  /// window is open, or `false` if it did not open within `max_iters`.
  ///
  /// Each outer iteration mirrors a `tick`: advance virtual time to the next deadline and fire
  /// due timers (so the leader's heartbeat re-replicates a freshly-durable entry), then pump
  /// drain-outgoing → deliver → flush+drain-storage for every node EXCEPT `keep`. `keep` receives
  /// messages (and so STAGES the resulting append) but is never flushed, so its in-flight window
  /// stays open. Pairing this with [`crash`](Self::crash) drops exactly that window (a crash
  /// mid-fsync). The double-vote / append-before-ack tripwires are not evaluated here (this is a
  /// pre-crash setup pump); they run on every real `tick`.
  pub fn open_fsync_window(&mut self, keep: u64, max_iters: usize) -> bool {
    for _ in 0..max_iters {
      if self.node_has_inflight(keep) {
        return true;
      }
      // Advance time to the next deadline and fire due timers (leader heartbeat replicates the
      // durable entry; followers' timers keep their state fresh). `keep`'s timers fire too — a
      // heartbeat will reset its election timer on delivery.
      let next_timer = self.nodes.iter().filter_map(Endpoint::poll_timeout).min();
      let next_msg = self.bus.iter().map(|m| m.deliver_at).min();
      if let Some(target) = [next_timer, next_msg].into_iter().flatten().min() {
        if target > self.now {
          self.now = target;
        }
        for i in 0..self.nodes.len() {
          if self.nodes[i].poll_timeout().is_some_and(|d| d <= self.now) {
            let (log, stable) = (&mut self.logs[i], &mut self.stables[i]);
            self.nodes[i].handle_timeout(self.now, log, stable);
          }
        }
      }
      // Pump: drain outgoing → deliver → flush+drain every node EXCEPT `keep`, until quiescent at
      // this timestamp or the window opens. Mirrors the `tick` inner loop.
      let mut inner = 0u32;
      loop {
        inner += 1;
        assert!(inner <= 10_000, "open_fsync_window inner loop livelock");

        // Drain every non-isolated node's outgoing onto the bus (and discard events — this is a
        // test-only setup pump; the real counters advance in `tick`).
        let mut any_new = false;
        for i in 0..self.nodes.len() {
          let from = self.node_ids[i];
          if self.isolated.contains(&from) {
            while self.nodes[i].poll_message().is_some() {}
          } else {
            while let Some(out) = self.nodes[i].poll_message() {
              any_new = true;
              let (to, message) = Outgoing::into_parts(out);
              self.bus.push_back(InFlight {
                deliver_at: self.now,
                from,
                to,
                message,
              });
            }
          }
          while self.nodes[i].poll_event().is_some() {}
        }

        let delivered = self.deliver_due();
        if self.node_has_inflight(keep) {
          return true;
        }

        // Flush + drain storage for every node EXCEPT `keep`, collecting any messages they produce
        // straight onto the bus (so the loop can detect progress without a deferred iteration).
        let storage_produced = self.flush_drain_collect_except(keep);
        if self.node_has_inflight(keep) {
          return true;
        }

        if !any_new && !delivered && !storage_produced {
          break;
        }
      }
    }
    self.node_has_inflight(keep)
  }

  /// Flush + drain storage for every node whose id is not `keep`, pushing any messages the
  /// completion handlers produce onto the bus. Returns whether any message was produced. Used by
  /// [`open_fsync_window`](Self::open_fsync_window) so it can mirror `tick`'s storage step while
  /// holding one node out of the flush. Isolated nodes' messages are discarded (as in `tick`).
  fn flush_drain_collect_except(&mut self, keep: u64) -> bool {
    for i in 0..self.nodes.len() {
      if self.node_ids[i] == keep {
        continue;
      }
      self.logs[i].flush();
      self.stables[i].flush();
      let (log, stable) = (&mut self.logs[i], &mut self.stables[i]);
      self.nodes[i].handle_storage(self.now, log, stable);
    }
    let mut produced = false;
    for i in 0..self.nodes.len() {
      let from = self.node_ids[i];
      if from == keep {
        continue;
      }
      if self.isolated.contains(&from) {
        while self.nodes[i].poll_message().is_some() {}
      } else {
        while let Some(out) = self.nodes[i].poll_message() {
          produced = true;
          let (to, message) = Outgoing::into_parts(out);
          self.bus.push_back(InFlight {
            deliver_at: self.now,
            from,
            to,
            message,
          });
        }
      }
      while self.nodes[i].poll_event().is_some() {}
    }
    produced
  }

  /// Propose `data` on the current leader; returns the assigned index (or `None` if no leader).
  pub fn propose(&mut self, data: &[u8]) -> Option<sailing_proto::Index> {
    let leader = self.leader()?;
    let i = self.node_idx[&leader];
    // Split into disjoint borrows: nodes[i], logs[i], stables[i] are each in a
    // separate Vec, so borrowing them simultaneously is safe.
    let log = &mut self.logs[i];
    let stable = &mut self.stables[i];
    self.nodes[i]
      .propose(self.now, log, stable, &bytes::Bytes::copy_from_slice(data))
      .ok()
  }

  /// Propose a v1 conf-change on the current leader; returns the assigned index (or `None`).
  pub fn propose_conf_change(&mut self, cc: ConfChange<u64>) -> Option<sailing_proto::Index> {
    let leader = self.leader()?;
    let i = self.node_idx[&leader];
    let log = &mut self.logs[i];
    let stable = &mut self.stables[i];
    self.nodes[i]
      .propose_conf_change(self.now, log, stable, cc)
      .ok()
  }

  /// Propose a v2 conf-change on the current leader; returns the assigned index (or `None`).
  pub fn propose_conf_change_v2(&mut self, cc: ConfChangeV2<u64>) -> Option<sailing_proto::Index> {
    let leader = self.leader()?;
    let i = self.node_idx[&leader];
    let log = &mut self.logs[i];
    let stable = &mut self.stables[i];
    self.nodes[i]
      .propose_conf_change_v2(self.now, log, stable, cc)
      .ok()
  }

  /// Add a new **voter** node with `id` mid-run.
  ///
  /// **Bootstrap rule:** the new node's `Endpoint` is constructed with `Config.voters` =
  /// the current live voter set (NOT including `id`). This makes `is_voter(id) = false` in
  /// the new node's own Tracker, so it cannot campaign and cannot disrupt the existing
  /// leader. The new node learns its own membership (voter) by applying the replicated
  /// `ConfChange(AddNode(id))` entry once the leader commits it.
  ///
  /// After wiring the new node into all parallel structures, this method proposes
  /// `AddNode(id)` on the current leader. The leader commits it under the OLD quorum,
  /// updates its Tracker, and replicates the full log (including the ConfChange entry) to
  /// the new node, which applies it and gains voter status in its own view.
  ///
  /// Panics if no leader is available.
  pub fn add_node(&mut self, id: u64) {
    self.wire_new_node(id, false);
    let cc = ConfChange::new(
      sailing_proto::ConfChangeType::AddNode,
      id,
      bytes::Bytes::new(),
    );
    self
      .propose_conf_change(cc)
      .expect("add_node: a leader must be available to propose AddNode");
  }

  /// Add a new **learner** node with `id` mid-run.
  ///
  /// Same bootstrap rule as [`add_node`]: the new node starts as a non-voter observer.
  /// After wiring it into the sim structures, proposes `AddLearnerNode(id)` on the leader.
  ///
  /// Panics if no leader is available.
  pub fn add_learner(&mut self, id: u64) {
    self.wire_new_node(id, false);
    let cc = ConfChange::new(
      sailing_proto::ConfChangeType::AddLearnerNode,
      id,
      bytes::Bytes::new(),
    );
    self
      .propose_conf_change(cc)
      .expect("add_learner: a leader must be available to propose AddLearnerNode");
  }

  /// Remove the node `id` from the cluster.
  ///
  /// Proposes `RemoveNode(id)` on the current leader. The change commits and is applied
  /// by the majority under the current quorum; the node being removed receives the commit
  /// and applies its own removal (gaining the U6 step-down: role → Follower, election timer
  /// disarmed). Once applied, the node is no longer a voter in any tracker.
  ///
  /// **Agreement oracle handling:** the removed node is tracked in `self.removed` so that the
  /// `agreement_holds` and `min_applied_len` oracles skip it — its applied log stopped
  /// advancing after removal and the rest of the cluster legitimately advanced further.
  /// The removed node is also `isolated` so it does not participate in future elections.
  ///
  /// Returns the proposal index. Panics if no leader is available.
  pub fn remove_node(&mut self, id: u64) {
    let cc = ConfChange::new(
      sailing_proto::ConfChangeType::RemoveNode,
      id,
      bytes::Bytes::new(),
    );
    self
      .propose_conf_change(cc)
      .expect("remove_node: a leader must be available to propose RemoveNode");
    // Mark the node as removed so the agreement oracle and min_applied_len skip it.
    // Also isolate it so it does not send spurious RequestVotes after being removed but
    // before applying the conf change in its own view (the U6 step-down fires when the
    // ConfChange is applied; until then, the node is technically still a voter in its own
    // view and its election timer is still armed). Isolation is a simulation convenience
    // — a real cluster would rely on U6 to stop the removed node from campaigning.
    self.removed.insert(id);
    self.isolated.insert(id);
  }

  /// Wire a brand-new node `id` into the simulation's parallel structures WITHOUT
  /// proposing any conf-change. Use this when the conf-change will be proposed separately
  /// (e.g. a joint [`ConfChangeV2`] that adds this node alongside other changes).
  ///
  /// The node starts as a non-voting observer (bootstrap with the current voter set, not
  /// including `id`) so it cannot campaign.
  pub fn wire_joining_node(&mut self, id: u64) {
    self.wire_new_node(id, false);
  }

  /// Mark node `id` as removed in the simulation's oracle state WITHOUT proposing any
  /// conf-change. Use this after a joint conf change that removes a node, to inform the
  /// `agreement_holds` and `min_applied_len` oracles that the node's applied log is allowed
  /// to diverge from the live cluster. The node is also isolated.
  ///
  /// This is a simulation convenience — it does NOT interact with the Raft protocol.
  pub fn mark_removed(&mut self, id: u64) {
    self.removed.insert(id);
    self.isolated.insert(id);
  }

  /// Wire a brand-new node (not yet in any membership set) into all the cluster's parallel
  /// structures. Does NOT propose any conf-change — call `add_node`/`add_learner` for that.
  fn wire_new_node(&mut self, id: u64, _is_learner: bool) {
    // Gather the current live voter ids to use as the bootstrap seed.
    // The new node starts knowing the current voters but is NOT itself a voter.
    let current_voters: Vec<u64> = self
      .node_ids
      .iter()
      .enumerate()
      .filter(|(_, nid)| !self.removed.contains(nid))
      .map(|(i, _)| self.node_ids[i])
      .collect();

    let base = Config::try_new_observer(
      id,
      current_voters,
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .expect("valid observer config");

    let pos = self.nodes.len();
    self.node_idx.insert(id, pos);
    self.node_ids.push(id);

    let ep = Endpoint::new(base.clone(), self.now, 0x5EED ^ id, LogSm::new());
    self.nodes.push(ep);
    // Honor the cluster's store mode so a node added mid-run matches the rest (async clusters
    // must not silently gain a synchronous store).
    if self.async_mode {
      self.logs.push(MemLog::new_async(0x5EED ^ id));
      self
        .stables
        .push(MemStable::new_async(0x5EED_0000_0000 ^ id));
    } else {
      self.logs.push(MemLog::new());
      self.stables.push(MemStable::new());
    }
    self.configs.push(base);
    self.snapshot_installs.push(0);
    self.conf_changed.push(0);
    self.read_states.push(Vec::new());
  }

  /// Capture a read-only [`ClusterView`] snapshot for the per-tick safety oracles ([`crate::checker`]).
  ///
  /// Every field is read through a PUBLIC accessor (the proto's
  /// `commit_index()`/`applied_index()`/`term()`/`role()`/`state_machine()`/`is_poisoned()` and the
  /// sim store's non-faulting durable-read seams [`MemLog::durable_entries`] /
  /// `MemStable::hard_state` / `MemStable::snapshot`), so building the view never perturbs the run
  /// (in particular it never triggers the `transient_read` fault on the committed-range
  /// `LogStore::entries` read). The `seed`/`tick` are threaded through for VOPR replay.
  pub fn view(&self) -> ClusterView {
    let nodes = (0..self.nodes.len())
      .map(|i| {
        let id = self.node_ids[i];
        let node = &self.nodes[i];
        let log = &self.logs[i];
        let stable = &self.stables[i];
        let durable_first = log.first_index().get();
        let durable_last = log.last_index().get();
        let durable_entries: std::vec::Vec<DurableEntry> = log
          .durable_entries()
          .iter()
          .map(|e| DurableEntry {
            index: e.index().get(),
            term: e.term().get(),
            data: e.data().to_vec(),
          })
          .collect();
        let (snapshot_last_index, snapshot_last_term) = match stable.snapshot() {
          Some((meta, _)) => (meta.last_index().get(), meta.last_term().get()),
          None => (0, 0),
        };
        let applied_log: std::vec::Vec<(u64, std::vec::Vec<u8>)> = node
          .state_machine()
          .applied()
          .iter()
          .map(|(idx, cmd)| (idx.get(), cmd.to_vec()))
          .collect();
        NodeView {
          id,
          removed: self.removed.contains(&id),
          poisoned: node.is_poisoned(),
          is_leader: node.role().is_leader(),
          term: node.term().get(),
          commit: node.commit_index().get(),
          applied: node.applied_index().get(),
          applied_log,
          durable_first,
          durable_last,
          durable_entries,
          snapshot_last_index,
          snapshot_last_term,
          hardstate_commit: stable.hard_state().commit().get(),
          inflight_staged: usize::from(log.has_inflight()) + usize::from(stable.has_inflight()),
        }
      })
      .collect();
    ClusterView {
      seed: self.seed,
      tick: self.tick_count,
      nodes,
    }
  }

  /// Run the full per-tick safety-oracle suite against the current cluster state, panicking with
  /// the oracle name + seed + tick on a violation (for exact VOPR replay). Called at the end of
  /// every [`tick`](Self::tick). Exposed so tests can also invoke it at a chosen point.
  pub fn run_oracles(&mut self) {
    let view = self.view();
    self.checker.check_or_panic(&view);
  }

  /// Borrow the [`Violation`](crate::Violation)-or-`Ok` result of running the suite WITHOUT
  /// panicking — for tests that want to assert the suite is green at a point.
  pub fn check_oracles(&mut self) -> Result<(), checker::Violation> {
    let view = self.view();
    self.checker.check(&view)
  }

  /// True if every non-removed node's applied `(index, command)` sequence agrees as a
  /// prefix of the longest — the core safety property.
  ///
  /// Removed nodes are excluded: their log stopped advancing at the point of removal, but
  /// the rest of the cluster continued, so their suffix would legitimately diverge.
  pub fn agreement_holds(&self) -> bool {
    let logs: std::vec::Vec<&[(sailing_proto::Index, bytes::Bytes)]> = self
      .node_ids
      .iter()
      .enumerate()
      .filter(|(_, id)| !self.removed.contains(id))
      .map(|(i, _)| self.nodes[i].state_machine().applied())
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

  /// Shortest applied-log length across all non-removed nodes.
  pub fn min_applied_len(&self) -> usize {
    self
      .node_ids
      .iter()
      .enumerate()
      .filter(|(_, id)| !self.removed.contains(id))
      .map(|(i, _)| self.nodes[i].state_machine().applied().len())
      .min()
      .unwrap_or(0)
  }

  /// Async mode: flush every node's staged (in-flight) writes to durable state, modeling the
  /// fsync for the in-flight window completing between driver iterations. No-op for sync stores
  /// (their `flush` is a no-op) but only ever called when `async_mode` is set.
  fn flush_all(&mut self) {
    for i in 0..self.nodes.len() {
      self.logs[i].flush();
      self.stables[i].flush();
    }
  }

  /// Drain storage completions for every node and collect any messages they produce.
  /// Returns `true` if any new messages were enqueued onto the bus.
  fn drain_storage_all(&mut self) -> bool {
    let mut any_new = false;
    for i in 0..self.nodes.len() {
      let (log, stable) = (&mut self.logs[i], &mut self.stables[i]);
      self.nodes[i].handle_storage(self.now, log, stable);
    }
    // Collect outgoing messages produced by completion handlers (e.g. deferred acks once a staged
    // append flushes). Same path as the `tick` outgoing-drain: the structural oracles + seeded
    // network faults are applied via `schedule_send`.
    for i in 0..self.nodes.len() {
      let id = self.node_ids[i];
      if self.isolated.contains(&id) {
        while self.nodes[i].poll_message().is_some() {}
      } else {
        while let Some(out) = self.nodes[i].poll_message() {
          any_new = true;
          let (to, message) = Outgoing::into_parts(out);
          self.schedule_send(i, to, message);
        }
      }
      while let Some(ev) = self.nodes[i].poll_event() {
        if ev.is_snapshot_installed() {
          self.snapshot_installs[i] += 1;
        }
        if ev.is_conf_changed() {
          self.conf_changed[i] += 1;
        }
        if let sailing_proto::Event::ReadState(rs) = ev {
          self.read_states[i].push(rs);
        }
      }
    }
    any_new
  }

  /// Run the structural oracles on a message node `i` is SENDING, then apply the seeded
  /// [`NetworkFaults`] and push the resulting `InFlight`(s) onto the bus.
  ///
  /// **Oracle ordering (critical):** the append-before-ack and one-grant-per-term tripwires run on
  /// EVERY sent message, BEFORE the drop/duplicate roll — they audit what the node SENDS regardless
  /// of delivery fate, so a dropped message never bypasses an oracle. (A reorder/dup must likewise
  /// never produce a double-vote or a premature ack; the proto's idempotency must absorb them.)
  ///
  /// **Fault application (seeded, deterministic):**
  /// - **drop:** with probability `drop_per_mille/1000`, do not push (the message is lost).
  /// - **duplicate:** with probability `duplicate_per_mille/1000`, push the SAME message TWICE; each
  ///   copy gets an independent jitter draw, so the copies may arrive at different times.
  /// - **latency+jitter:** `deliver_at = now + latency + U[0, jitter]` (seeded uniform). With
  ///   nonzero jitter messages can be delivered out of order; if `reorder == false`, each (from,to)
  ///   pair's `deliver_at` is clamped to be ≥ the previous one for that pair (FIFO).
  ///
  /// When `net_faults.is_none()` this is byte-identical to the original push (no draw, no clamp,
  /// `deliver_at == now`, exactly one `InFlight`). Returns whether at least one copy was pushed.
  fn schedule_send(&mut self, i: usize, to: u64, message: Message<u64>) -> bool {
    let from = self.node_ids[i];

    // ── Structural assertion (a): append-before-ack ──────────────────────────────
    // A success AppendResp must not outrun the node's durable log.
    if let Message::AppendResp(a) = &message {
      if !a.reject() {
        assert!(
          self.logs[i].last_index() >= a.match_index(),
          "append-before-ack violated: node {from} acked {:?} but durable last_index is {:?}",
          a.match_index(),
          self.logs[i].last_index(),
        );
      }
    }
    // ── Structural assertion (b): one-grant-per-(node,term) ──────────────────────
    // A success VoteResp from `from` in term `T` to candidate `to` must not appear a second time
    // for a different candidate — that would be a double-vote. Holds under reorder+dup: a duplicate
    // grant to the SAME candidate is fine; a grant to a DIFFERENT one in the same term is a bug.
    if let Message::VoteResp(vr) = &message {
      if !vr.reject() {
        let term = vr.term();
        match self.grants.get(&(from, term)) {
          Some(&prev) => assert_eq!(
            prev, to,
            "double-vote bug: node {from} granted vote in term {term:?} to both {prev} and {to}"
          ),
          None => {
            self.grants.insert((from, term), to);
          }
        }
      }
    }

    // Fast path: faults off ⇒ original behavior (zero-latency, FIFO, single push). Keeps M0–M7 +
    // M8-U1 byte-identical and never touches the network PRNG or FIFO map.
    if self.net_faults.is_none() {
      self.bus.push_back(InFlight {
        deliver_at: self.now,
        from,
        to,
        message,
      });
      return true;
    }

    // ── Seeded drop ───────────────────────────────────────────────────────────────
    if self
      .net_prng
      .chance_per_mille(self.net_faults.drop_per_mille)
    {
      self.net_dropped += 1;
      return false; // lost in flight
    }

    // ── Seeded duplicate ──────────────────────────────────────────────────────────
    let copies = if self
      .net_prng
      .chance_per_mille(self.net_faults.duplicate_per_mille)
    {
      self.net_duplicated += 1;
      2
    } else {
      1
    };
    for _ in 0..copies {
      // Each copy gets an independent jitter draw (a dup may overtake its twin).
      let jitter = self.net_prng.jitter_draw(self.net_faults.jitter);
      let mut deliver_at = self.now + self.net_faults.latency + jitter;
      // FIFO clamp: when reorder is disabled, never schedule a message before the previous one for
      // this ordered pair (so jitter delays but never reorders within (from,to)).
      if !self.net_faults.reorder {
        let last = self
          .net_last_sched
          .entry((from, to))
          .or_insert(Instant::ORIGIN);
        if deliver_at < *last {
          deliver_at = *last;
        }
        *last = deliver_at;
      }
      self.bus.push_back(InFlight {
        deliver_at,
        from,
        to,
        message: message.clone(),
      });
    }
    true
  }

  /// Deliver all messages on the bus that are due at or before `self.now`.
  /// Returns `true` if any message was delivered.
  fn deliver_due(&mut self) -> bool {
    let mut delivered = false;
    let mut rest: VecDeque<InFlight> = VecDeque::new();
    while let Some(m) = self.bus.pop_front() {
      if m.deliver_at <= self.now {
        if self.isolated.contains(&m.from) || self.isolated.contains(&m.to) {
          // Drop silently — partition swallows it.
        } else if let Some(&to_idx) = self.node_idx.get(&m.to) {
          delivered = true;
          let (log, stable) = (&mut self.logs[to_idx], &mut self.stables[to_idx]);
          self.nodes[to_idx].handle_message(self.now, log, stable, m.from, m.message);
        }
        // else: message to an unknown id (shouldn't happen, but drop safely)
      } else {
        rest.push_back(m);
      }
    }
    self.bus = rest;
    delivered
  }

  /// Advance the simulation one step. Returns `true` if any work happened.
  ///
  /// A single step:
  ///   a. Advance virtual time to the earliest pending deadline.
  ///   b. Fire all timers due at that time.
  ///   c. Flush outgoing → deliver due → drain storage for all nodes → repeat until
  ///      quiescent at this timestamp (zero-latency bus drains completely before the
  ///      next timer can fire). Panics if the inner loop exceeds 10_000 iterations
  ///      (indicates a livelock bug).
  pub fn tick(&mut self) -> bool {
    let mut progressed = false;

    // Step a+b: advance clock and fire timers.
    let next_timer = self.nodes.iter().filter_map(Endpoint::poll_timeout).min();
    let next_msg = self.bus.iter().map(|m| m.deliver_at).min();
    if let Some(target) = [next_timer, next_msg].into_iter().flatten().min() {
      if target > self.now {
        self.now = target;
        progressed = true;
      }
      for i in 0..self.nodes.len() {
        if self.nodes[i].poll_timeout().is_some_and(|d| d <= self.now) {
          progressed = true;
          let (log, stable) = (&mut self.logs[i], &mut self.stables[i]);
          self.nodes[i].handle_timeout(self.now, log, stable);
        }
      }
    }

    // Step c: flush outgoing → deliver → drain storage → repeat until stable.
    let mut iters = 0u32;
    loop {
      iters += 1;
      assert!(
        iters <= 10_000,
        "Cluster::tick inner loop exceeded 10_000 iterations — livelock?"
      );

      // Drain all node outgoing queues onto the bus.
      // Skip isolated nodes: their outgoing messages are dropped (partition).
      let mut any_new = false;
      for i in 0..self.nodes.len() {
        let id = self.node_ids[i];
        if self.isolated.contains(&id) {
          // Drain and discard so the queue doesn't grow unboundedly.
          while self.nodes[i].poll_message().is_some() {}
        } else {
          while let Some(out) = self.nodes[i].poll_message() {
            any_new = true;
            progressed = true;
            let (to, message) = Outgoing::into_parts(out);
            // Run the structural oracles and apply the seeded network faults, then push onto the
            // bus. The oracles run on every SENT message (inside `schedule_send`), BEFORE the
            // drop/dup roll, so a dropped message never bypasses a tripwire.
            self.schedule_send(i, to, message);
          }
        }
        while let Some(ev) = self.nodes[i].poll_event() {
          progressed = true;
          if ev.is_snapshot_installed() {
            self.snapshot_installs[i] += 1;
          }
          if ev.is_conf_changed() {
            self.conf_changed[i] += 1;
          }
          if let sailing_proto::Event::ReadState(rs) = ev {
            self.read_states[i].push(rs);
          }
        }
      }

      // Deliver all messages due now.
      let delivered = self.deliver_due();
      if delivered {
        progressed = true;
      }

      // Async mode: flush each node's staged (in-flight) writes to durable state BEFORE
      // draining completions — modeling the fsync for the in-flight window completing between
      // driver iterations. A `crash()` that runs `discard_inflight()` WITHOUT a preceding
      // `flush()` therefore loses exactly the staged window. No-op in sync mode.
      if self.async_mode {
        self.flush_all();
      }

      // Drain storage completions for every node (deferred acks produced here
      // will be picked up by the outgoing-drain in the next iteration).
      let storage_produced = self.drain_storage_all();
      if storage_produced {
        progressed = true;
      }

      if !any_new && !delivered && !storage_produced {
        break;
      }
    }

    // The cluster is now quiescent at this timestamp (delivery + storage drained) — a consistent
    // observable state. Advance the tick counter and run the WHOLE per-tick safety-oracle suite
    // (M8-U3). A violation panics with the oracle name + seed + tick for exact VOPR replay. The
    // suite is a pure observer: it reads only public accessors / non-faulting durable seams and
    // never mutates the nodes/stores or draws a PRNG, so the run stays byte-identical and
    // deterministic. (The send-time append-before-ack / one-grant tripwires in `schedule_send`
    // remain as earlier-firing immediate checks; this is the consolidated guarantee.)
    self.tick_count += 1;
    let view = self.view();
    self.checker.check_or_panic(&view);

    progressed
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn three_node_cluster_ticks_and_eventually_elects() {
    let mut c = Cluster::new(3);
    assert_eq!(c.size(), 3);
    // M1: endpoints arm election timers immediately; the cluster should elect a leader.
    let mut found = false;
    for _ in 0..100 {
      c.tick();
      if c.leader().is_some() {
        found = true;
        break;
      }
    }
    assert!(found, "a leader should emerge within 100 ticks");
  }

  /// Drive a cluster to agreement on a batch and return each node's applied (index, command) log.
  fn drive_and_capture(c: &mut Cluster, batch: u32) -> Vec<AppliedLog> {
    assert!(c.run_until(300, |c| c.leader_count() == 1));
    for i in 0..batch {
      c.run_until(100, |c| c.leader_count() == 1);
      c.propose(&i.to_le_bytes());
      c.run_until(60, |_| false);
    }
    assert!(c.run_until(600, |c| c.agreement_holds()
      && c.min_applied_len() >= batch as usize));
    (0..c.size() as u64)
      .map(|n| c.applied_entries_of(n))
      .collect()
  }

  #[test]
  fn faults_off_is_byte_identical_to_baseline() {
    // A cluster with the network fault model installed as `none()` must produce the EXACT same run
    // as a plain `Cluster::new` (no `deliver_at` change, no drops, no extra PRNG influence). This
    // is the M0–M7 / M8-U1 byte-identity invariant, made explicit at the cluster level.
    let baseline = {
      let mut c = Cluster::new(3);
      drive_and_capture(&mut c, 8)
    };
    let with_off_faults = {
      let mut c = Cluster::new(3);
      c.set_network_faults(NetworkFaults::none(), 0xDEAD_BEEF);
      drive_and_capture(&mut c, 8)
    };
    assert_eq!(
      baseline, with_off_faults,
      "an all-off NetworkFaults config must be byte-identical to the faultless bus"
    );
    // And no fault counter moved (nothing was dropped or duplicated).
    let mut c = Cluster::new(3);
    c.set_network_faults(NetworkFaults::none(), 7);
    drive_and_capture(&mut c, 8);
    assert_eq!(c.net_dropped(), 0);
    assert_eq!(c.net_duplicated(), 0);
  }

  #[test]
  fn same_seed_same_run_under_faults() {
    // Cluster-level determinism: identical seed ⇒ identical applied logs AND identical fault tallies.
    let run = |seed: u64| -> (Vec<AppliedLog>, u64, u64) {
      let mut c = Cluster::new(3);
      c.set_network_faults(
        NetworkFaults {
          latency: Duration::from_millis(3),
          jitter: Duration::from_millis(20),
          drop_per_mille: 120,
          duplicate_per_mille: 90,
          reorder: true,
        },
        seed,
      );
      let logs = drive_and_capture(&mut c, 8);
      (logs, c.net_dropped(), c.net_duplicated())
    };
    assert_eq!(
      run(0x1234),
      run(0x1234),
      "same seed ⇒ identical run + tallies"
    );
  }
}
