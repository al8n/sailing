//! A deterministic, single-threaded cluster of `Endpoint`s over an in-memory typed-message
//! bus and a virtual clock. M0 wires the loop; M1+ exercises real consensus through it.
use crate::{LogSm, MemLog, MemStable};
use core::time::Duration;
use sailing_proto::{Config, Endpoint, Instant, LogStore, Message, Outgoing, Term};
use std::{
  collections::{BTreeMap, BTreeSet, VecDeque},
  vec::Vec,
};

/// Per-node snapshot-install tally: incremented each time an `Event::SnapshotInstalled`
/// is drained from that node's event queue during `tick`. Used by snapshot tests to
/// assert that `InstallSnapshot` was genuinely exercised.
type SnapCount = u64;

type Node = Endpoint<u64, LogSm>;

/// An in-flight typed message: `(deliver_at, from, to, message)`.
struct InFlight {
  deliver_at: Instant,
  from: u64,
  to: u64,
  message: Message<u64>,
}

/// A deterministic cluster. Node ids are `0..n`.
pub struct Cluster {
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
  /// Double-vote tripwire: maps `(granter, term)` → `grantee`.
  /// A second distinct grantee for the same `(granter, term)` is a fatal bug.
  grants: BTreeMap<(u64, Term), u64>,
  /// Per-node count of `Event::SnapshotInstalled` events drained during `tick`.
  /// Monotonically incremented; reset to zero on `crash`+restart.
  snapshot_installs: Vec<SnapCount>,
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
    let mut nodes = Vec::with_capacity(n);
    let mut logs = Vec::with_capacity(n);
    let mut stables = Vec::with_capacity(n);
    let mut configs = Vec::with_capacity(n);
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
      logs.push(MemLog::new());
      stables.push(MemStable::new());
    }
    let snapshot_installs = vec![0u64; n];
    Self {
      nodes,
      logs,
      stables,
      configs,
      bus: VecDeque::new(),
      now: Instant::ORIGIN,
      isolated: BTreeSet::new(),
      grants: BTreeMap::new(),
      snapshot_installs,
    }
  }

  /// Number of nodes.
  pub fn size(&self) -> usize {
    self.nodes.len()
  }

  /// The current virtual time.
  pub fn now(&self) -> Instant {
    self.now
  }

  /// The id of a node that currently believes itself leader, if any.
  pub fn leader(&self) -> Option<u64> {
    self
      .nodes
      .iter()
      .enumerate()
      .find(|(_, n)| n.role().is_leader())
      .map(|(i, _)| i as u64)
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

  /// How many nodes currently believe themselves leader.
  pub fn leader_count(&self) -> usize {
    self.nodes.iter().filter(|n| n.role().is_leader()).count()
  }

  /// The term of node `i`.
  pub fn term_of(&self, i: usize) -> sailing_proto::Term {
    self.nodes[i].term()
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
    self.logs[id as usize].first_index()
  }

  /// Total number of `Event::SnapshotInstalled` events observed for node `id` since
  /// cluster construction (or the last `crash`).
  pub fn snapshot_install_count(&self, id: u64) -> u64 {
    self.snapshot_installs[id as usize]
  }

  /// Total `Event::SnapshotInstalled` events across ALL nodes.
  pub fn total_snapshot_installs(&self) -> u64 {
    self.snapshot_installs.iter().sum()
  }

  /// Crash node `id`: lose all in-memory consensus state and any fsync still in-flight,
  /// but keep the durably-written store contents. The node is immediately restarted from
  /// its durable stores.
  pub fn crash(&mut self, id: u64) {
    let i = id as usize;
    self.logs[i].discard_inflight();
    self.stables[i].discard_inflight();
    let cfg = self.configs[i].clone();
    let (log, stable) = (&mut self.logs[i], &mut self.stables[i]);
    self.nodes[i] = Endpoint::restart(cfg, self.now, 0x5EED ^ id, LogSm::new(), log, stable);
    // Reset the snapshot-install counter for the restarted node.
    self.snapshot_installs[i] = 0;
    // Drain any messages left in the bus to/from this node (stale in-flight traffic).
    self.bus.retain(|m| m.from != id && m.to != id);
  }

  /// Propose `data` on the current leader; returns the assigned index (or `None` if no leader).
  pub fn propose(&mut self, data: &[u8]) -> Option<sailing_proto::Index> {
    let leader = self.leader()? as usize;
    // Split into disjoint borrows: nodes[leader], logs[leader], stables[leader] are each in a
    // separate Vec, so borrowing them simultaneously is safe.
    let log = &mut self.logs[leader];
    let stable = &mut self.stables[leader];
    self.nodes[leader]
      .propose(self.now, log, stable, &bytes::Bytes::copy_from_slice(data))
      .ok()
  }

  /// True if every node's applied `(index, command)` sequence agrees as a prefix of the
  /// longest — the core safety property.
  pub fn agreement_holds(&self) -> bool {
    let logs: std::vec::Vec<&[(sailing_proto::Index, bytes::Bytes)]> = self
      .nodes
      .iter()
      .map(|n| n.state_machine().applied())
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

  /// Shortest applied-log length across all nodes.
  pub fn min_applied_len(&self) -> usize {
    self
      .nodes
      .iter()
      .map(|n| n.state_machine().applied().len())
      .min()
      .unwrap_or(0)
  }

  /// Drain storage completions for every node and collect any messages they produce.
  /// Returns `true` if any new messages were enqueued onto the bus.
  fn drain_storage_all(&mut self) -> bool {
    let mut any_new = false;
    for i in 0..self.nodes.len() {
      let (log, stable) = (&mut self.logs[i], &mut self.stables[i]);
      self.nodes[i].handle_storage(self.now, log, stable);
    }
    // Collect outgoing messages produced by completion handlers.
    for i in 0..self.nodes.len() {
      if self.isolated.contains(&(i as u64)) {
        while self.nodes[i].poll_message().is_some() {}
      } else {
        while let Some(out) = self.nodes[i].poll_message() {
          any_new = true;
          let (to, message) = Outgoing::into_parts(out);
          // ── Structural assertion (a): append-before-ack ──────────────────────────
          // A success AppendResp must not outrun the node's durable log.
          if let Message::AppendResp(a) = &message {
            if !a.reject() {
              assert!(
                self.logs[i].last_index() >= a.match_index(),
                "append-before-ack violated: node {i} acked {:?} but durable last_index is {:?}",
                a.match_index(),
                self.logs[i].last_index(),
              );
            }
          }
          // ── Structural assertion (b): one-grant-per-(node,term) ──────────────────
          // A success VoteResp from `from` in term `T` to candidate `to` must not
          // appear a second time for a different candidate — that would be a double-vote.
          if let Message::VoteResp(vr) = &message {
            if !vr.reject() {
              let from = i as u64;
              let term = vr.term();
              let grantee = to;
              match self.grants.get(&(from, term)) {
                Some(&prev) => assert_eq!(
                  prev, grantee,
                  "double-vote bug: node {from} granted vote in term {term:?} to both {prev} and {grantee}"
                ),
                None => {
                  self.grants.insert((from, term), grantee);
                }
              }
            }
          }
          self.bus.push_back(InFlight {
            deliver_at: self.now,
            from: i as u64,
            to,
            message,
          });
        }
      }
      while let Some(ev) = self.nodes[i].poll_event() {
        if ev.is_snapshot_installed() {
          self.snapshot_installs[i] += 1;
        }
      }
    }
    any_new
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
        } else {
          delivered = true;
          let to = m.to as usize;
          let (log, stable) = (&mut self.logs[to], &mut self.stables[to]);
          self.nodes[to].handle_message(self.now, log, stable, m.from, m.message);
        }
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
        if self.isolated.contains(&(i as u64)) {
          // Drain and discard so the queue doesn't grow unboundedly.
          while self.nodes[i].poll_message().is_some() {}
        } else {
          while let Some(out) = self.nodes[i].poll_message() {
            any_new = true;
            progressed = true;
            let (to, message) = Outgoing::into_parts(out);
            // ── Structural assertion (a): append-before-ack ──────────────────────
            if let Message::AppendResp(a) = &message {
              if !a.reject() {
                assert!(
                  self.logs[i].last_index() >= a.match_index(),
                  "append-before-ack violated: node {i} acked {:?} but durable last_index is {:?}",
                  a.match_index(),
                  self.logs[i].last_index(),
                );
              }
            }
            // ── Structural assertion (b): one-grant-per-(node,term) ──────────────
            if let Message::VoteResp(vr) = &message {
              if !vr.reject() {
                let from = i as u64;
                let term = vr.term();
                let grantee = to;
                match self.grants.get(&(from, term)) {
                  Some(&prev) => assert_eq!(
                    prev, grantee,
                    "double-vote bug: node {from} granted vote in term {term:?} to both {prev} and {grantee}"
                  ),
                  None => {
                    self.grants.insert((from, term), grantee);
                  }
                }
              }
            }
            self.bus.push_back(InFlight {
              deliver_at: self.now,
              from: i as u64,
              to,
              message,
            });
          }
        }
        while let Some(ev) = self.nodes[i].poll_event() {
          progressed = true;
          if ev.is_snapshot_installed() {
            self.snapshot_installs[i] += 1;
          }
        }
      }

      // Deliver all messages due now.
      let delivered = self.deliver_due();
      if delivered {
        progressed = true;
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
}
