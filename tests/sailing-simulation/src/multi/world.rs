//! The deterministic multi-group world: one [`MultiRaft`] container per node over a group-tagged
//! typed bus. See the [module docs](crate::multi).
//!
//! The run loop is the exact analogue of [`Cluster::tick`](crate::Cluster): advance the single
//! global virtual clock to the earliest pending deadline, fire every due `(group, deadline)` on
//! every host, then settle (drain outgoing → deliver due → drain storage) until quiescent at that
//! timestamp. Per-node clock drift and the failover wall are deliberately absent in v1 — the
//! single-group VOPR retains that coverage; the hooks stay reserved here.

use crate::{AppliedLog, LogSm, MemLog, MemStable};
use core::time::Duration;
use sailing_proto::{Config, Instant, Message, MultiRaft, Outgoing};
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
    progressed
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

  /// Drain (and for now discard) node `node`'s aggregated `(gid, event)` queue so it stays
  /// bounded. The oracle layer grows observers here.
  fn drain_host_events(&mut self, node: u64) {
    let host = self.hosts.get_mut(&node).expect("host exists");
    while host.poll_event().is_some() {}
  }

  /// Push one group-tagged message onto the bus (fault-free: zero latency, FIFO, exactly once).
  fn schedule_send(&mut self, from: u64, gid: u64, to: u64, message: Message<u64>) {
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
