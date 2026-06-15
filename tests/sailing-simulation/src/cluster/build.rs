use super::*;

impl Cluster {
  /// Build an `n`-node cluster (ids `0..n`), each a fresh Follower.
  pub fn new(n: usize) -> Self {
    Self::new_with(n, |cfg| cfg)
  }

  /// Build an `n`-node cluster and apply `configure` to each node's `Config` after
  /// construction. Use this to override flow-control knobs (e.g. `max_inflight_msgs`)
  /// for targeted tests while keeping `new` unchanged.
  pub fn new_with(n: usize, configure: impl Fn(Config<u64>) -> Config<u64> + 'static) -> Self {
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

  /// Build an async-mode cluster ([`new_async`](Self::new_async)) that also applies `configure` to
  /// every node's bootstrap config — the founders AND any joiner wired in mid-run. Use it to run the
  /// fuzzer under a realistic config (e.g. `pre_vote` + `check_quorum`, which keep a far-behind
  /// freshly-added voter from disrupting a stable leader before it catches up).
  pub fn new_async_with(
    n: usize,
    seed: u64,
    configure: impl Fn(Config<u64>) -> Config<u64> + 'static,
  ) -> Self {
    Self::new_inner(n, configure, true, seed)
  }

  /// Shared constructor body. `async_mode` selects [`crate::StoreMode::Async`] stores (seeded with
  /// `seed` for any storage faults); `false` keeps the default synchronous stores so `new` /
  /// `new_with` are byte-identical to the original synchronous behavior.
  pub(crate) fn new_inner(
    n: usize,
    configure: impl Fn(Config<u64>) -> Config<u64> + 'static,
    async_mode: bool,
    seed: u64,
  ) -> Self {
    let mut nodes = Vec::with_capacity(n);
    let mut logs = Vec::with_capacity(n);
    let mut stables = Vec::with_capacity(n);
    let mut configs = Vec::with_capacity(n);
    let mut node_ids = Vec::with_capacity(n);
    let mut node_idx = BTreeMap::new();
    let node_configure: std::boxed::Box<dyn Fn(Config<u64>) -> Config<u64>> =
      std::boxed::Box::new(configure);
    let voters: Vec<u64> = (0..n as u64).collect();
    for id in 0..n as u64 {
      let base = Config::try_new(
        id,
        voters.clone(),
        Duration::from_millis(1000),
        Duration::from_millis(100),
      )
      .expect("valid config");
      let cfg = node_configure(base);
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
    let restarts = vec![0u64; n];
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
      restarts,
      node_configure,
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
      // No clock drift by default: every node's rate is 1/1, so `now_for` is the identity and the
      // cluster is byte-identical to the original single-clock model. `set_clock_drift` overrides this.
      clock_rate: vec![(1, 1); n],
      drift_policy: std::boxed::Box::new(|_| (1, 1)),
      lease_superseded_serves: 0,
      superseded_read_contexts: std::collections::BTreeSet::new(),
      // No synchronized wall by default: every call carries the monotonic instant only (wall absent),
      // byte-identical to the original, and the precise commit-anchor never fires. `enable_failover_clock`
      // overrides this. Offsets start at zero (a perfectly synchronized wall) for every node.
      failover: false,
      clock_offset: vec![0; n],
      eps_unc_ns: 0,
    }
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
  pub(crate) fn wire_new_node(&mut self, id: u64, _is_learner: bool) {
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
    let base = (self.node_configure)(base);

    let pos = self.nodes.len();
    self.node_idx.insert(id, pos);
    self.node_ids.push(id);
    // Assign this mid-run joiner its clock rate from the drift policy BEFORE reading its local `now`,
    // so a dynamically-added node drifts like the founders (default `(1, 1)` = no drift).
    let rate = validate_rate((self.drift_policy)(id));
    self.clock_rate.push(rate);
    // A mid-run joiner starts at a zero wall offset; it gets its first non-zero offset at the next
    // `resync_offsets` (parallel to `clock_rate`, indexed by Vec position).
    self.clock_offset.push(0);
    let now_pos = self.now_for(pos);

    let ep = Endpoint::new(base.clone(), now_pos, 0x5EED ^ id, LogSm::new());
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
    self.restarts.push(0);
    self.conf_changed.push(0);
    self.read_states.push(Vec::new());
  }
}
