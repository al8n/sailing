//! A deterministic, single-threaded cluster of `Endpoint`s over an in-memory typed-message
//! bus and a virtual clock. M0 wires the loop; M1+ exercises real consensus through it.
use crate::{LogSm, MemLog, MemStable};
use core::time::Duration;
use sailing_proto::{Config, Endpoint, Instant, Message, Outgoing};
use std::{collections::VecDeque, vec::Vec};

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
}

impl Cluster {
  /// Build an `n`-node cluster (ids `0..n`), each a fresh Follower.
  pub fn new(n: usize) -> Self {
    let mut nodes = Vec::with_capacity(n);
    let mut logs = Vec::with_capacity(n);
    let mut stables = Vec::with_capacity(n);
    for id in 0..n as u64 {
      let cfg = Config::try_new(id, Duration::from_millis(1000), Duration::from_millis(100))
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

  /// Advance the simulation one step. Returns `true` if any work happened (a message was
  /// produced/delivered or a timer fired) — i.e. the cluster is not yet quiescent.
  pub fn tick(&mut self) -> bool {
    let mut progressed = false;

    // 1. Drain outgoing messages from every node into the bus (latency = 0 for M0).
    for i in 0..self.nodes.len() {
      while let Some(out) = self.nodes[i].poll_message() {
        progressed = true;
        let (to, message) = Outgoing::into_parts(out);
        self.bus.push_back(InFlight {
          deliver_at: self.now,
          from: i as u64,
          to,
          message,
        });
      }
      // Drain (and discard, for M0) application events.
      while self.nodes[i].poll_event().is_some() {
        progressed = true;
      }
    }

    // 2. Deliver all messages due now.
    let mut due: Vec<InFlight> = Vec::new();
    let mut rest: VecDeque<InFlight> = VecDeque::new();
    while let Some(m) = self.bus.pop_front() {
      if m.deliver_at <= self.now {
        due.push(m);
      } else {
        rest.push_back(m);
      }
    }
    self.bus = rest;
    for m in due {
      progressed = true;
      let to = m.to as usize;
      let (log, stable) = (&mut self.logs[to], &mut self.stables[to]);
      self.nodes[to].handle_message(self.now, log, stable, m.from, m.message);
    }

    // 3. Advance the clock to the earliest of the next timer / next message, then fire.
    let next_timer = self.nodes.iter().filter_map(Endpoint::poll_timeout).min();
    let next_msg = self.bus.iter().map(|m| m.deliver_at).min();
    if let Some(target) = [next_timer, next_msg].into_iter().flatten().min() {
      if target > self.now {
        self.now = target;
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
    progressed
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn three_node_cluster_ticks_quiescently() {
    let mut c = Cluster::new(3);
    assert_eq!(c.size(), 3);
    // With the M0 no-op endpoint, nothing is produced; tick must not panic and must
    // report no progress.
    for _ in 0..10 {
      let progressed = c.tick();
      assert!(
        !progressed,
        "M0 endpoint is a no-op; no messages or timers expected"
      );
    }
    assert!(c.leader().is_none());
  }
}
