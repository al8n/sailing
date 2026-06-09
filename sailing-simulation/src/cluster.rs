//! A deterministic, single-threaded cluster of `Endpoint`s over an in-memory typed-message
//! bus and a virtual clock. M0 wires the loop; M1+ exercises real consensus through it.
use crate::{LogSm, MemLog, MemStable};
use core::time::Duration;
use sailing_proto::{Config, Endpoint, Instant, Message, Outgoing};
use std::{
  collections::{BTreeSet, VecDeque},
  vec::Vec,
};

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
  bus: VecDeque<InFlight>,
  now: Instant,
  /// Node ids that are fully partitioned: their outgoing messages are dropped and
  /// inbound messages to/from them are dropped. Init empty.
  isolated: BTreeSet<u64>,
}

impl Cluster {
  /// Build an `n`-node cluster (ids `0..n`), each a fresh Follower.
  pub fn new(n: usize) -> Self {
    let mut nodes = Vec::with_capacity(n);
    let mut logs = Vec::with_capacity(n);
    let mut stables = Vec::with_capacity(n);
    let voters: Vec<u64> = (0..n as u64).collect();
    for id in 0..n as u64 {
      let cfg = Config::try_new(
        id,
        voters.clone(),
        Duration::from_millis(1000),
        Duration::from_millis(100),
      )
      .expect("valid config");
      nodes.push(Endpoint::new(cfg, Instant::ORIGIN, id, LogSm::new()));
      logs.push(MemLog::new());
      stables.push(MemStable::new());
    }
    Self {
      nodes,
      logs,
      stables,
      bus: VecDeque::new(),
      now: Instant::ORIGIN,
      isolated: BTreeSet::new(),
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

  /// Advance the simulation one step. Returns `true` if any work happened.
  ///
  /// A single step:
  ///   a. Advance virtual time to the earliest pending deadline.
  ///   b. Fire all timers due at that time.
  ///   c. Flush all outgoing messages to the bus, then deliver all due messages.
  ///      Repeat (c) until stable at this timestamp (zero-latency bus drains completely
  ///      before the next timer can fire).
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
          let (log, stable) = (&mut self.logs[i], &mut self.stables[i]);
          self.nodes[i].handle_storage(self.now, log, stable);
        }
      }
    }

    // Step c: flush outgoing → deliver → repeat until stable at `self.now`.
    loop {
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
            self.bus.push_back(InFlight {
              deliver_at: self.now,
              from: i as u64,
              to,
              message,
            });
          }
        }
        while self.nodes[i].poll_event().is_some() {
          progressed = true;
        }
      }

      // Deliver all messages due now.
      // Drop any message whose `from` or `to` is isolated (full two-way partition).
      let mut delivered = false;
      let mut rest: VecDeque<InFlight> = VecDeque::new();
      while let Some(m) = self.bus.pop_front() {
        if m.deliver_at <= self.now {
          if self.isolated.contains(&m.from) || self.isolated.contains(&m.to) {
            // Drop silently — partition swallows it.
          } else {
            delivered = true;
            progressed = true;
            let to = m.to as usize;
            let (log, stable) = (&mut self.logs[to], &mut self.stables[to]);
            self.nodes[to].handle_message(self.now, log, stable, m.from, m.message);
          }
        } else {
          rest.push_back(m);
        }
      }
      self.bus = rest;

      if !any_new && !delivered {
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
