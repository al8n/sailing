use super::*;

impl Cluster {
  /// Isolate node `id`: drop all messages to and from it (a full two-way partition).
  pub fn isolate(&mut self, id: u64) {
    self.isolated.insert(id);
  }

  /// Heal the partition for node `id`: messages to/from it flow again.
  pub fn heal(&mut self, id: u64) {
    self.isolated.remove(&id);
  }

  /// Reverse a [`mark_removed`](Self::mark_removed): make node `id` a reachable participant again and
  /// stop the oracles from skipping it. Used by the harness when a `gone` node turns out to still be
  /// needed — a laggard's APPLIED config (post-restart or post-partition) regressed to list it as a
  /// voter, so it must rejoin the network until its removal is re-applied cluster-wide, or the laggard
  /// (once elected) deadlocks demanding the vote of a node the harness had isolated. `mark_removed`
  /// never destroyed the node's log/endpoint, so reinstating simply un-isolates and un-skips it.
  pub fn reinstate(&mut self, id: u64) {
    self.removed.remove(&id);
    self.isolated.remove(&id);
  }

  /// Crash node `id`: lose all in-memory consensus state and any fsync still in-flight,
  /// but keep the durably-written store contents. The node is immediately restarted from
  /// its durable stores.
  pub fn crash(&mut self, id: u64) {
    let i = self.node_idx[&id];
    self.logs[i].discard_inflight();
    self.stables[i].discard_inflight();
    let cfg = self.configs[i].clone();
    // Strictly-increasing per-restart boot epoch (the harness's durable boot counter): it namespaces
    // the restarted node's forwarded-read tokens so a pre-crash ReadIndexResp cannot complete a
    // post-restart read. `restarts[i]` counts PRIOR restarts, so +1 is unique per incarnation.
    let boot_epoch = self.restarts[i] + 1;
    // The node reboots on its OWN clock (rate persists — a crash does not change hardware clock rate;
    // `clock_rate[i]` stays put). With no drift this is `self.now`, byte-identical to the original.
    let now_i = self.now_for(i);
    let (log, stable) = (&mut self.logs[i], &mut self.stables[i]);
    self.nodes[i] = Endpoint::restart(
      cfg,
      now_i,
      0x5EED ^ id,
      LogSm::new(),
      boot_epoch,
      log,
      stable,
    );
    // Reset the snapshot-install counter for the restarted node.
    self.snapshot_installs[i] = 0;
    self.restarts[i] += 1;
    // Drain any messages left in the bus to/from this node (stale in-flight traffic).
    self.bus.retain(|m| m.from != id && m.to != id);
    // Drop the FIFO bookkeeping for any pair touching this node so the restarted node starts
    // FIFO-fresh (a stale high-water mark would needlessly delay its new traffic). No-op when the
    // network fault model is off (the map is empty).
    self.net_last_sched.retain(|&(f, t), _| f != id && t != id);
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
  /// faultless, zero-latency, FIFO bus byte-identical to the original.
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

  /// Install a per-node clock-drift policy: `policy(id)` returns node `id`'s clock RATE as a
  /// `(num, den)` rational, so its local clock reads `floor(global · num/den)`. Applied to every
  /// CURRENT node and stored so any mid-run joiner gets a rate too. The default is `|_| (1, 1)` (no
  /// drift — a single global clock, byte-identical to the original cluster).
  ///
  /// This is what gives LeaseGuard its cross-leader coverage: a same-clock read gate is blind to a
  /// constant per-node OFFSET, so only differing clock RATES age a deposed leader's lease and a
  /// successor's commit-wait apart in real time — the exact `Δ·(Δ+ε)/(Δ−ε)` margin. Keep every rate
  /// within the configured drift bound ε (`|num/den − 1| ≤ ε/Δ`); a rate outside it would inject MORE
  /// drift than the protocol assumes and could surface a stale read that is the harness's fault, not a
  /// proto bug.
  ///
  /// Call it right after construction (before any tick): it rewrites every founder's rate, which is
  /// sound because the founders were created at `Instant::ORIGIN` (local time 0 under any rate).
  pub fn set_clock_drift(&mut self, policy: impl Fn(u64) -> (u64, u64) + 'static) {
    self.clock_rate = self
      .node_ids
      .iter()
      .map(|&id| validate_rate(policy(id)))
      .collect();
    self.drift_policy = std::boxed::Box::new(policy);
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
      let next_timer = (0..self.nodes.len())
        .filter_map(|i| self.global_timeout(i))
        .min();
      let next_msg = self.bus.iter().map(|m| m.deliver_at).min();
      if let Some(target) = [next_timer, next_msg].into_iter().flatten().min() {
        if target > self.now {
          self.now = target;
        }
        for i in 0..self.nodes.len() {
          if self.global_timeout(i).is_some_and(|d| d <= self.now) {
            let now_i = self.now_for(i);
            let (log, stable) = (&mut self.logs[i], &mut self.stables[i]);
            self.nodes[i].handle_timeout(now_i, log, stable);
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
  pub(crate) fn flush_drain_collect_except(&mut self, keep: u64) -> bool {
    for i in 0..self.nodes.len() {
      if self.node_ids[i] == keep {
        continue;
      }
      self.logs[i].flush();
      self.stables[i].flush();
      let now_i = self.now_for(i);
      let (log, stable) = (&mut self.logs[i], &mut self.stables[i]);
      self.nodes[i].handle_storage(now_i, log, stable);
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
}
