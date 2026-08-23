//! Deterministic multi-container LINEAGE scenarios the shipped [`MultiWorld`](super::MultiWorld)
//! cannot construct.
//!
//! The world mints only FRESH child ids (`propose_split` asserts the child is unused), so its
//! split-conflict signal — a committed fork whose child id is ALREADY hosted — is structurally
//! unreachable there (see the `split_conflicts` field docs). This module hand-builds a small
//! multi-host `MultiRaft` world over the sim's stores and FSM to reach the SQUATTER and
//! EMPTY-JOINER shapes through the REAL machinery: a committed split whose child id is occupied on
//! one host in the child conf, and the child leader's manufactured baseline driven at that host.
//! No message is crafted — the fork baseline, the transfer, and every verdict are produced by the
//! real endpoints.
//!
//! # The two planes at the child id
//!
//! A fork child boots from a manufactured snapshot at `(1, 1)` carrying a `ForkId`, and `(index,
//! term)` certifies content only WITHIN one lineage. Two wire planes can reach a replica squatting
//! the child id:
//!
//!   - the SNAPSHOT plane: the fork-provenance gate admits a token-bearing snapshot only onto a
//!     replica with no committed content (or one bearing the same token) — a populated occupant
//!     REFUSES, silently on the wire, counted locally. Reached whenever the occupant's log
//!     CONTRADICTS the baseline coordinate (its index-1 term differs), because the append probe
//!     then rejects down into compacted territory and the transfer must ship as a snapshot.
//!   - the APPEND plane: coordinates that COINCIDE — and `(1, 1)` is the most collision-prone
//!     coordinate in any log, essentially every group's first entry — let the child leader's
//!     probe MATCH the occupant's prefix and walk in through ordinary appends, which trust
//!     coordinates within a lineage (Log Matching) and never consult the provenance gate. The
//!     committed-truncation fail-stop is what catches that rewrite at the first conflicting
//!     committed entry. The group header carries no incarnation stamp yet (the reserved next wire
//!     field closes this route at the demux); until it does, this file pins the fail-stop as the
//!     enforced behavior at the coincident coordinate.
//!
//! The fixture is intentionally minimal (explicit timers, a stride-seeded fault leg) so every
//! scenario is a pure function of its construction.

#![cfg(test)]

use super::oracles::LineageLedger;
use crate::{LogSm, MemLog, MemStable};
use core::time::Duration;
use sailing_proto::{
  Config, ForkId, Instant, LogStore, Message, MultiRaft, Outgoing, PoisonReason, ProgressState,
};
use std::{
  collections::{BTreeMap, BTreeSet, VecDeque},
  vec::Vec,
};

const ELECTION: Duration = Duration::from_millis(1000);
const HEARTBEAT: Duration = Duration::from_millis(100);

/// One installed snapshot observed while draining events: who installed what lineage at which
/// boundary. The scenarios assert exactly-one adoption off this record, in observation order.
#[derive(Clone)]
pub(super) struct InstallObs {
  pub(super) node: u64,
  pub(super) gid: u64,
  pub(super) lineage: Option<ForkId>,
  pub(super) boundary: u64,
}

/// A minimal deterministic multi-container world: one `MultiRaft` host per node, per-`(node, gid)`
/// stores, an explicit `(from, to, gid, message)` bus, and a shared clock. Deliveries may be
/// dropped or duplicated on a deterministic stride schedule (the per-message fault leg).
pub(super) struct Mini {
  hosts: BTreeMap<u64, MultiRaft<u64, u64, LogSm>>,
  stores: BTreeMap<(u64, u64), (MemLog, MemStable<u64>)>,
  bus: VecDeque<(u64, u64, u64, Message<u64>)>,
  now: Instant,
  boot_epochs: BTreeMap<u64, u64>,
  /// Applied to every config this harness builds (groups and fork children alike) so the
  /// baseline transfer can be forced MULTI-chunk.
  chunk_bytes: Option<u64>,
  /// A delivery whose ordinal is a multiple of `drop_stride` is dropped; of `dup_stride`,
  /// delivered twice. Zero disables the leg.
  drop_stride: u64,
  dup_stride: u64,
  delivery_ordinal: u64,
  /// `(node, gid)` replicas whose timer [`advance`](Self::advance) never fires — a squatter held
  /// at a fixed term so it never campaigns and the child's leadership can rise above it.
  no_fire: BTreeSet<(u64, u64)>,
  /// `(gid, peer)` transfers that delivered at least one non-zero-offset `InstallSnapshot` frame —
  /// the persistent multi-chunk witness (the frame itself is transient).
  multichunk_to: BTreeSet<(u64, u64)>,
  /// Installs drained, in observation order.
  installs: Vec<InstallObs>,
  /// `(node, parent, child)` split-conflict signals drained — the parked-fork witness. The
  /// occupant stays in place (the embedder model is patient observation); the signal count is the
  /// observable that the fork PARKED rather than materializing over it.
  conflicts: Vec<(u64, u64, u64)>,
}

impl Mini {
  pub(super) fn new() -> Self {
    Self {
      hosts: BTreeMap::new(),
      stores: BTreeMap::new(),
      bus: VecDeque::new(),
      now: Instant::ORIGIN,
      boot_epochs: BTreeMap::new(),
      chunk_bytes: None,
      drop_stride: 0,
      dup_stride: 0,
      delivery_ordinal: 0,
      no_fire: BTreeSet::new(),
      multichunk_to: BTreeSet::new(),
      installs: Vec::new(),
      conflicts: Vec::new(),
    }
  }

  /// Force every config this harness builds to a tiny `snapshot_chunk_bytes` so baseline
  /// transfers are multi-chunk.
  pub(super) fn with_chunking(mut self, bytes: u64) -> Self {
    self.chunk_bytes = Some(bytes);
    self
  }

  /// Set the deterministic drop/duplicate stride schedule (0 disables that leg).
  pub(super) fn with_faults(mut self, drop_stride: u64, dup_stride: u64) -> Self {
    self.drop_stride = drop_stride;
    self.dup_stride = dup_stride;
    self
  }

  fn chunked(&self, c: Config<u64>) -> Config<u64> {
    match self.chunk_bytes {
      Some(b) => c.with_snapshot_chunk_bytes(b),
      None => c,
    }
  }

  fn seed_for(&self, node: u64) -> u64 {
    0xA11CE ^ node
  }

  /// Create group `gid` with the given `voters` on the `on` hosts (fresh hosts are added). A host
  /// in `on` but not in `voters` is wired as an observer.
  pub(super) fn create_group(&mut self, gid: u64, voters: &[u64], on: &[u64]) {
    for &node in on {
      let cfg = if voters.contains(&node) {
        self.chunked(Config::try_new(node, voters.to_vec(), ELECTION, HEARTBEAT).unwrap())
      } else {
        self.chunked(Config::try_new_observer(node, voters.to_vec(), ELECTION, HEARTBEAT).unwrap())
      };
      let (seed, now) = (self.seed_for(node), self.now);
      let host = self.hosts.entry(node).or_default();
      host
        .create_group(gid, 0, cfg, now, seed, LogSm::new())
        .expect("admission");
      self
        .stores
        .insert((node, gid), (MemLog::new(), MemStable::new()));
    }
  }

  /// Hold `(node, gid)`'s timer FROZEN — [`advance`](Self::advance) never fires it.
  pub(super) fn freeze_timer(&mut self, node: u64, gid: u64) {
    self.no_fire.insert((node, gid));
  }

  /// Fire `node`'s `gid` timer at its own deadline (explicit calls override a freeze), then
  /// settle.
  pub(super) fn campaign(&mut self, gid: u64, node: u64) {
    let dl = self.hosts[&node]
      .deadlines()
      .find(|(g, _)| *g == gid)
      .map(|(_, d)| d);
    if let Some(dl) = dl {
      self.now = self.now.max(dl);
    }
    let now = self.now;
    let (log, stable) = self.stores.get_mut(&(node, gid)).unwrap();
    self
      .hosts
      .get_mut(&node)
      .unwrap()
      .handle_timeout(&gid, now, log, stable)
      .unwrap();
    self.settle();
  }

  /// Elect `node` as `gid`'s leader by firing only its timer until it leads.
  pub(super) fn elect(&mut self, gid: u64, node: u64) {
    for _ in 0..40 {
      if self.hosts[&node]
        .group(&gid)
        .is_some_and(|e| e.role().is_leader())
      {
        return;
      }
      self.campaign(gid, node);
    }
    panic!("group {gid} node {node} never led");
  }

  /// One world step: advance the clock to the earliest pending deadline, fire every due
  /// `(node, gid)` timer except the frozen ones, then settle. Drives heartbeats, snapshot
  /// re-sends under loss, and re-elections (a child rises in term until it dominates a frozen
  /// squatter).
  pub(super) fn advance(&mut self) {
    // The earliest FIRABLE deadline: frozen replicas' timers never fire, so they must not pin the
    // clock either (a frozen earliest deadline would otherwise stall every other timer forever).
    let no_fire = &self.no_fire;
    if let Some(t) = self
      .hosts
      .iter()
      .flat_map(|(n, h)| {
        h.deadlines()
          .filter(move |(g, _)| !no_fire.contains(&(*n, *g)))
          .map(|(_, d)| d)
      })
      .min()
    {
      self.now = self.now.max(t);
    }
    let nodes: Vec<u64> = self.hosts.keys().copied().collect();
    for node in nodes {
      let due: Vec<u64> = self.hosts[&node]
        .deadlines()
        .filter(|(g, d)| *d <= self.now && !self.no_fire.contains(&(node, *g)))
        .map(|(g, _)| g)
        .collect();
      for g in due {
        let now = self.now;
        let (log, stable) = self.stores.get_mut(&(node, g)).unwrap();
        self
          .hosts
          .get_mut(&node)
          .unwrap()
          .handle_timeout(&g, now, log, stable)
          .unwrap();
      }
    }
    self.settle();
  }

  /// Propose `cmd` on `node`'s `gid` replica (must lead), flush, and settle.
  pub(super) fn propose(&mut self, gid: u64, node: u64, cmd: &[u8]) {
    let now = self.now;
    let bytes = bytes::Bytes::copy_from_slice(cmd);
    {
      let (log, stable) = self.stores.get_mut(&(node, gid)).unwrap();
      let host = self.hosts.get_mut(&node).unwrap();
      host
        .propose(&gid, now, log, stable, &bytes)
        .unwrap()
        .unwrap();
      host.flush_appends(&gid, now, log, stable).unwrap();
    }
    self.settle();
  }

  /// Propose a split of `parent` at `point` into `child` on `leader` (must lead and accept),
  /// flush, settle.
  pub(super) fn propose_split(&mut self, parent: u64, leader: u64, child: u64, point: u16) {
    let now = self.now;
    let instr = bytes::Bytes::copy_from_slice(&point.to_le_bytes());
    {
      let (log, stable) = self.stores.get_mut(&(leader, parent)).unwrap();
      let host = self.hosts.get_mut(&leader).unwrap();
      host
        .propose_split(&parent, now, log, stable, &child, 0, instr)
        .unwrap()
        .unwrap();
      host.flush_appends(&parent, now, log, stable).unwrap();
    }
    self.settle();
  }

  /// The typed refusal of a split whose `child` id this container cannot mint onto, or `None` if
  /// the propose was accepted.
  pub(super) fn propose_split_err(
    &mut self,
    parent: u64,
    leader: u64,
    child: u64,
    point: u16,
  ) -> Option<sailing_proto::SplitError<u64>> {
    let now = self.now;
    let instr = bytes::Bytes::copy_from_slice(&point.to_le_bytes());
    let (log, stable) = self.stores.get_mut(&(leader, parent)).unwrap();
    let host = self.hosts.get_mut(&leader).unwrap();
    host
      .propose_split(&parent, now, log, stable, &child, 0, instr)
      .expect("the leader hosts the parent")
      .err()
  }

  /// Run to quiescence: drain every host (storage, outgoing, events, committed forks), deliver
  /// every queued message under the stride fault schedule, repeat until nothing moves.
  pub(super) fn settle(&mut self) {
    for _ in 0..4_000 {
      let mut progressed = false;
      let nodes: Vec<u64> = self.hosts.keys().copied().collect();
      for node in &nodes {
        if self.drain_node(*node) {
          progressed = true;
        }
      }
      let pending: Vec<(u64, u64, u64, Message<u64>)> = self.bus.drain(..).collect();
      for (from, to, gid, msg) in pending {
        self.delivery_ordinal += 1;
        let ord = self.delivery_ordinal;
        if self.drop_stride != 0 && ord.is_multiple_of(self.drop_stride) {
          continue; // dropped
        }
        let copies = if self.dup_stride != 0 && ord.is_multiple_of(self.dup_stride) {
          2
        } else {
          1
        };
        for _ in 0..copies {
          if !self.hosts.get(&to).is_some_and(|h| h.contains_group(&gid)) {
            continue; // unhosted straggler dropped at the demux
          }
          // The multi-chunk witness: a delivered InstallSnapshot with a non-zero offset.
          if matches!(&msg, Message::InstallSnapshot(s) if s.offset() > 0) {
            self.multichunk_to.insert((gid, to));
          }
          progressed = true;
          let now = self.now;
          let (log, stable) = self.stores.get_mut(&(to, gid)).unwrap();
          self
            .hosts
            .get_mut(&to)
            .unwrap()
            .handle_message(&gid, now, log, stable, from, msg.clone())
            .unwrap();
        }
      }
      if !progressed {
        break;
      }
    }
  }

  /// Drain `node`'s storage completions, outgoing messages (onto the bus), events (recording
  /// installs), committed forks (materializing children), and split-conflict signals (recorded;
  /// the occupant stays in place — patient observation).
  fn drain_node(&mut self, node: u64) -> bool {
    let now = self.now;
    let mut produced = false;
    for _ in 0..4_000 {
      let gids: Vec<u64> = self.hosts[&node].group_ids().copied().collect();
      for g in gids {
        let (log, stable) = self.stores.get_mut(&(node, g)).unwrap();
        while self
          .hosts
          .get_mut(&node)
          .unwrap()
          .handle_storage(&g, now, log, stable)
          .unwrap()
          .is_more_pending()
        {}
      }
      let mut any = false;
      while let Some((g, o)) = self.hosts.get_mut(&node).unwrap().poll_message() {
        any = true;
        produced = true;
        let (to, msg) = Outgoing::into_parts(o);
        self.bus.push_back((node, to, g, msg));
      }
      while let Some((g, ev)) = self.hosts.get_mut(&node).unwrap().poll_event() {
        produced = true;
        if let sailing_proto::Event::SnapshotInstalled(meta) = ev {
          self.installs.push(InstallObs {
            node,
            gid: g,
            lineage: meta.fork_id().cloned(),
            boundary: meta.last_index().get(),
          });
        }
      }
      let mut forked = false;
      while let Some((peeked_parent, child)) = self
        .hosts
        .get_mut(&node)
        .unwrap()
        .peek_yieldable_fork(&sailing_proto::NoHold)
        .map(|fork| (*fork.parent(), *fork.child()))
      {
        forked = true;
        produced = true;
        let seed = self.seed_for(node);
        let no_floors = BTreeSet::new();
        let mut engine = crate::multi::PairEngine {
          node,
          stores: &mut self.stores,
          boot_epochs: &mut self.boot_epochs,
          floored: &no_floors,
        };
        let sailing_proto::InstallOutcome::Installed {
          parent,
          split_index,
          ..
        } = self.hosts.get_mut(&node).unwrap().install_yieldable_fork(
          &peeked_parent,
          &child,
          &mut engine,
          &sailing_proto::NoHold,
          now,
          seed,
        )
        else {
          panic!("fork materialization on node {node}")
        };
        self
          .hosts
          .get_mut(&node)
          .unwrap()
          .lift_fork_barrier(&parent, split_index);
      }
      while let Some((p, c)) = self.hosts.get_mut(&node).unwrap().poll_split_conflict() {
        produced = true;
        self.conflicts.push((node, p, c));
      }
      if !any && !forked {
        break;
      }
    }
    produced
  }

  // ---- accessors ----

  pub(super) fn hosts_group(&self, node: u64, gid: u64) -> bool {
    self
      .hosts
      .get(&node)
      .is_some_and(|h| h.contains_group(&gid))
  }

  pub(super) fn leader_of(&self, gid: u64) -> Option<u64> {
    self
      .hosts
      .iter()
      .filter(|(_, h)| h.group(&gid).is_some_and(|e| e.role().is_leader()))
      .max_by_key(|(_, h)| h.group(&gid).map(|e| e.term()))
      .map(|(id, _)| *id)
  }

  pub(super) fn fork_id(&self, node: u64, gid: u64) -> Option<ForkId> {
    self
      .hosts
      .get(&node)
      .and_then(|h| h.group(&gid))
      .and_then(|e| e.fork_id())
  }

  pub(super) fn refused(&self, node: u64, gid: u64) -> u64 {
    self
      .hosts
      .get(&node)
      .and_then(|h| h.group(&gid))
      .map_or(0, |e| e.refused_cross_lineage_install_count())
  }

  pub(super) fn applied(&self, node: u64, gid: u64) -> Vec<(u64, Vec<u8>)> {
    self
      .hosts
      .get(&node)
      .and_then(|h| h.group(&gid))
      .map(|e| {
        e.state_machine()
          .applied()
          .iter()
          .map(|(i, c)| (i.get(), c.to_vec()))
          .collect()
      })
      .unwrap_or_default()
  }

  pub(super) fn commit(&self, node: u64, gid: u64) -> u64 {
    self
      .hosts
      .get(&node)
      .and_then(|h| h.group(&gid))
      .map_or(0, |e| e.commit_index().get())
  }

  /// The replica's durable log window `(first_index, last_index)`.
  pub(super) fn log_window(&self, node: u64, gid: u64) -> (u64, u64) {
    let (log, _) = &self.stores[&(node, gid)];
    (log.first_index().get(), log.last_index().get())
  }

  /// The durable term at `index` in the replica's log (0 if absent).
  pub(super) fn log_term_at(&self, node: u64, gid: u64, index: u64) -> u64 {
    let (log, _) = &self.stores[&(node, gid)];
    log
      .durable_entries()
      .iter()
      .find(|e| e.index().get() == index)
      .map_or(0, |e| e.term().get())
  }

  pub(super) fn is_poisoned(&self, node: u64, gid: u64) -> bool {
    self
      .hosts
      .get(&node)
      .and_then(|h| h.group(&gid))
      .is_some_and(|e| e.is_poisoned())
  }

  pub(super) fn poison_reason(&self, node: u64, gid: u64) -> Option<PoisonReason> {
    self
      .hosts
      .get(&node)
      .and_then(|h| h.group(&gid))
      .and_then(|e| e.poison_reason())
  }

  /// The leader's match index toward `peer` for `gid` (0 if not tracked / no leader).
  pub(super) fn peer_match(&self, gid: u64, peer: u64) -> u64 {
    let Some(leader) = self.leader_of(gid) else {
      return 0;
    };
    self.hosts[&leader]
      .group(&gid)
      .and_then(|e| e.peer_progress(&peer))
      .map_or(0, |p| p.match_index.get())
  }

  /// Whether the leader is currently driving a `Snapshot` transfer to `peer` for `gid`.
  pub(super) fn peer_in_snapshot(&self, gid: u64, peer: u64) -> bool {
    let Some(leader) = self.leader_of(gid) else {
      return false;
    };
    self.hosts[&leader]
      .group(&gid)
      .and_then(|e| e.peer_progress(&peer))
      .is_some_and(|p| matches!(p.state, ProgressState::Snapshot { .. }))
  }

  /// Every install observed so far, in observation order.
  pub(super) fn installs(&self) -> &[InstallObs] {
    &self.installs
  }

  /// How many split-conflict signals `node` surfaced (its fork parked against a hosted child).
  pub(super) fn conflicts_on(&self, node: u64) -> usize {
    self.conflicts.iter().filter(|(n, _, _)| *n == node).count()
  }

  /// Whether the transfer of `gid` to `peer` ever delivered a non-zero-offset InstallSnapshot
  /// frame — the multi-chunk witness (a single-shot transfer only ever ships offset 0).
  pub(super) fn saw_multichunk_to(&self, gid: u64, peer: u64) -> bool {
    self.multichunk_to.contains(&(gid, peer))
  }
}

/// Stage the shared parent: `g100` on `{0, 1, 2}`, elected on node 0, one committed write per key
/// of the full domain.
fn stage_parent(mut m: Mini) -> Mini {
  m.create_group(100, &[0, 1, 2], &[0, 1, 2]);
  m.elect(100, 0);
  for key in 0u16..8 {
    m.propose(100, 0, &super::encode_gkv(100, key, u64::from(key)));
  }
  m
}

/// Occupy the child id `g200` on node 2 with a POPULATED, token-less squatter whose index-1 term
/// CONTRADICTS the fork baseline's `(1, 1)`: voters `{2, 3}` hosted on node 2 alone burn one
/// campaign against the unhosted peer (the vote request dies at node 3's demux), node 3 then
/// hosts and grants, and the squatter's founding entry lands at `(1, 2)`. The child's own first
/// leadership is ALSO term 2, and an append at the receiver's own term is processed (the squatter
/// steps down and prev-checks), so the child's probe at the baseline coordinate REJECTS down into
/// compacted territory — the transfer is forced onto the snapshot plane, where the
/// fork-provenance gate rules. A lower-term contact would instead be ignored at the demux
/// (etcd-parity defaults), and a term-coincident founding entry would let the probe walk in
/// through the append plane (the camouflage scenario below). Both squatter timers are frozen so
/// its term stays fixed under the child's leadership.
fn stage_term_contradicting_squatter(m: &mut Mini) {
  m.create_group(200, &[2, 3], &[2]);
  m.campaign(200, 2); // term 1: no quorum (the peer is unhosted)
  m.create_group(200, &[2, 3], &[3]);
  m.elect(200, 2); // term 2: node 3 grants; the founding entry is (1, 2)
  m.propose(200, 2, &super::encode_gkv(200, 1, 5001));
  m.propose(200, 2, &super::encode_gkv(200, 2, 5002));
  assert_eq!(m.commit(2, 200), 3, "the squatter committed its content");
  assert!(
    m.log_term_at(2, 200, 1) > 1,
    "the squatter's index-1 term must contradict the fork baseline coordinate"
  );
  m.freeze_timer(2, 200);
  m.freeze_timer(3, 200);
}

/// The squatter's committed keyed content, as `(key, value)` pairs — the untouched-state pin.
fn squatter_cells(m: &Mini) -> Vec<(u16, u64)> {
  m.applied(2, 200)
    .iter()
    .filter_map(|(_, c)| super::decode_gkv(c).map(|(_, k, v)| (k, v)))
    .collect()
}

/// Split the parent into `g200` and drive the child's transfer at node 2 until `resolved` (or
/// panic when the crank budget ends). The split parks node 2's fork against its hosted occupant;
/// the child elects on a real voter and its term rises past any frozen squatter's before the
/// transfer resolves.
fn drive_split_transfer(m: &mut Mini, resolved: impl Fn(&Mini) -> bool) {
  m.propose_split(100, 0, 200, 4);
  // Under a lossy schedule the split's commit needs re-sent rounds before every unoccupied host
  // has applied it and materialized its fork.
  for _ in 0..100 {
    if m.hosts_group(0, 200) && m.hosts_group(1, 200) {
      break;
    }
    m.advance();
  }
  assert!(
    m.hosts_group(0, 200) && m.hosts_group(1, 200),
    "the child materialized on the unoccupied hosts"
  );
  m.elect(200, 0);
  for _ in 0..400 {
    if resolved(m) {
      return;
    }
    m.advance();
  }
  assert!(resolved(m), "the transfer never resolved within the budget");
}

/// SQUATTER-TRANSFER, single-shot: the child leader's baseline meets a POPULATED token-less
/// occupant whose log contradicts the baseline coordinate, so the transfer runs on the snapshot
/// plane and the fork-provenance gate REFUSES — the squatter's log, commit, applied state, and
/// token-less identity are all untouched (placement resolves the conflict, never replacement),
/// the refusal is counted, the occupant never acks toward the child's commit, and the fork on the
/// squatter's host stays PARKED (the conflict signal is the witness).
#[test]
fn squatter_transfer_is_refused_single_shot() {
  let mut m = stage_parent(Mini::new());
  stage_term_contradicting_squatter(&mut m);
  let pre_window = m.log_window(2, 200);
  drive_split_transfer(&mut m, |m| m.refused(2, 200) >= 1);

  // Untouched: same log window, same commit, same cells, no token, no install event, no poison.
  assert_eq!(
    m.log_window(2, 200),
    pre_window,
    "the squatter's log is untouched"
  );
  assert_eq!(m.commit(2, 200), 3, "the squatter's commit is untouched");
  assert_eq!(
    squatter_cells(&m),
    std::vec![(1, 5001), (2, 5002)],
    "the squatter's applied state is untouched"
  );
  assert_eq!(m.fork_id(2, 200), None, "no token adopted");
  assert!(
    m.installs().iter().all(|o| !(o.node == 2 && o.gid == 200)),
    "a refused transfer produces no install on the occupant"
  );
  assert!(!m.is_poisoned(2, 200), "refusal is not a fail-stop");

  // The parked fork: node 2's parent replica surfaced the hosted-child conflict and left the
  // occupant in place.
  assert!(
    m.conflicts_on(2) >= 1,
    "the fork on the squatter's host must park with a conflict signal"
  );

  // No phantom: the child's real quorum agrees byte-for-byte, fresh child load commits on it,
  // and the occupant is never counted toward the child's commit.
  let leader = m.leader_of(200).expect("the child elected");
  assert!(leader == 0 || leader == 1);
  m.propose(200, leader, &super::encode_gkv(200, 5, 900));
  m.advance();
  assert_eq!(
    m.applied(0, 200),
    m.applied(1, 200),
    "the real quorum agrees"
  );
  assert!(
    m.applied(leader, 200)
      .iter()
      .any(|(_, c)| super::decode_gkv(c) == Some((200, 5, 900))),
    "fresh child load commits on the real quorum"
  );
  assert!(
    m.peer_match(200, 2) < m.commit(leader, 200),
    "the occupant never acks toward the child's commit (match {} < commit {})",
    m.peer_match(200, 2),
    m.commit(leader, 200),
  );
  assert_eq!(
    squatter_cells(&m),
    std::vec![(1, 5001), (2, 5002)],
    "the squatter stays untouched under continued child load"
  );
}

/// SQUATTER-TRANSFER, chunked: the identical refusal under a tiny `snapshot_chunk_bytes` — the
/// gate rules the transfer's first frame, so nothing stages regardless of the chunk schedule.
#[test]
fn squatter_transfer_is_refused_chunked() {
  let mut m = stage_parent(Mini::new().with_chunking(4));
  stage_term_contradicting_squatter(&mut m);
  let pre_window = m.log_window(2, 200);
  drive_split_transfer(&mut m, |m| m.refused(2, 200) >= 1);

  assert_eq!(m.log_window(2, 200), pre_window, "untouched log");
  assert_eq!(m.commit(2, 200), 3, "untouched commit");
  assert_eq!(squatter_cells(&m), std::vec![(1, 5001), (2, 5002)]);
  assert_eq!(m.fork_id(2, 200), None, "no token adopted under chunking");
  assert!(
    m.installs().iter().all(|o| !(o.node == 2 && o.gid == 200)),
    "no install effect on the occupant"
  );
  assert!(m.conflicts_on(2) >= 1, "the fork stays parked");
}

/// CAMOUFLAGE-APPEND: the SAME populated squatter shape at the TERM-COINCIDENT coordinate — its
/// founding entry sits at exactly `(1, 1)`, the fork baseline's claim and the most
/// collision-prone coordinate in any log. The child leader's probe then MATCHES the occupant's
/// prefix and the transfer walks in through the APPEND plane, which trusts coordinates within a
/// lineage (Log Matching) and never consults the fork-provenance gate. What stops the rewrite
/// TODAY is the committed-truncation fail-stop: the first child entry conflicting at-or-below the
/// squatter's commit poisons the occupant BEFORE any durable state is truncated. The group header
/// carries no incarnation stamp yet (the reserved next wire field closes this route at the
/// demux); until it does, this pins the fail-stop as the enforced behavior at the coincident
/// coordinate — content preserved, a loud stop, never a silent chimera.
#[test]
fn camouflage_append_at_the_coincident_coordinate_fail_stops() {
  let mut m = stage_parent(Mini::new());
  // Single-voter occupant: its first election wins instantly at term 1 — founding entry (1, 1).
  m.create_group(200, &[2], &[2]);
  m.elect(200, 2);
  m.propose(200, 2, &super::encode_gkv(200, 1, 5001));
  m.propose(200, 2, &super::encode_gkv(200, 2, 5002));
  assert_eq!(m.commit(2, 200), 3);
  assert_eq!(
    m.log_term_at(2, 200, 1),
    1,
    "the camouflage: the squatter's founding entry coincides with the fork baseline coordinate"
  );
  m.freeze_timer(2, 200);
  let pre_window = m.log_window(2, 200);

  drive_split_transfer(&mut m, |m| m.is_poisoned(2, 200));

  // The fail-stop, not the gate: the append plane never consulted the provenance gate (no
  // refusal), and the poison landed before any committed entry was truncated.
  assert_eq!(
    m.poison_reason(2, 200).map(|r| r.as_str()),
    Some("committed_truncation"),
    "the coincident-coordinate walk-in is stopped by the committed-truncation fail-stop"
  );
  assert_eq!(
    m.refused(2, 200),
    0,
    "the append plane bypasses the snapshot-plane gate entirely — the residual this pins"
  );
  assert_eq!(
    m.log_window(2, 200),
    pre_window,
    "the fail-stop aborts BEFORE truncating the squatter's durable log"
  );
  assert_eq!(m.commit(2, 200), 3, "commit untouched");
  assert_eq!(
    squatter_cells(&m),
    std::vec![(1, 5001), (2, 5002)],
    "applied state untouched — a loud stop, never a silent chimera"
  );
  assert_eq!(m.fork_id(2, 200), None, "no token adopted");
}

/// MUST-KEEP, single-shot: an EMPTY occupant at the child id adopts the fork baseline — the
/// pristine-adopter leg of the gate. Exactly one install lands, carrying the token at the
/// baseline boundary, and the joiner converges on the child leader's inherited record.
#[test]
fn empty_joiner_adopts_the_fork_baseline_single_shot() {
  let mut m = stage_parent(Mini::new());
  m.create_group(200, &[2], &[2]);
  m.freeze_timer(2, 200); // pristine: it never self-elects committed content
  drive_split_transfer(&mut m, |m| {
    m.fork_id(2, 200).is_some() && !m.applied(2, 200).is_empty()
  });

  assert_eq!(m.refused(2, 200), 0, "a pristine adopter never refuses");
  assert!(
    m.fork_id(2, 200).is_some(),
    "the joiner adopted the fork token"
  );
  assert_eq!(
    m.applied(2, 200),
    m.applied(0, 200),
    "the joiner converged on the child leader's inherited record"
  );
  assert!(
    m.applied(2, 200)
      .iter()
      .any(|(_, c)| matches!(super::decode_gkv(c), Some((tag, _, _)) if tag == 100)),
    "the baseline delivered the inherited parent-tagged cells"
  );
  let joiner_installs: Vec<&InstallObs> = m
    .installs()
    .iter()
    .filter(|o| o.node == 2 && o.gid == 200)
    .collect();
  assert_eq!(joiner_installs.len(), 1, "exactly one adoption");
  assert!(
    joiner_installs[0].lineage.is_some()
      && joiner_installs[0].boundary >= sailing_proto::FORK_BASE_INDEX.get(),
    "the install carried the fork token at/above the manufactured baseline"
  );
}

/// MUST-KEEP, chunked: the same pristine adoption completes MULTI-chunk (a delivered
/// `InstallSnapshot` with offset > 0 is the witness) under a tiny `snapshot_chunk_bytes`.
#[test]
fn empty_joiner_adopts_the_fork_baseline_chunked() {
  let mut m = stage_parent(Mini::new().with_chunking(4));
  m.create_group(200, &[2], &[2]);
  m.freeze_timer(2, 200);
  drive_split_transfer(&mut m, |m| {
    m.fork_id(2, 200).is_some() && !m.applied(2, 200).is_empty()
  });

  assert!(
    m.fork_id(2, 200).is_some(),
    "the joiner adopted under chunking"
  );
  assert_eq!(
    m.applied(2, 200),
    m.applied(0, 200),
    "the chunked joiner converged on the leader's record"
  );
  assert!(
    m.saw_multichunk_to(200, 2),
    "the baseline transferred MULTI-chunk (an InstallSnapshot with offset > 0 was delivered)"
  );
}

/// SAME-TOKEN-RETRY under LOSS and DUPLICATION: the pristine joiner's baseline transfer survives
/// a deterministic drop/duplicate stride schedule and adopts EXACTLY ONCE — a duplicated or
/// re-sent baseline resolves redundantly against the now-adopted child (no second install event,
/// no state change), and the leader exits Snapshot state (the transfer terminates).
#[test]
fn same_token_retry_adopts_exactly_once_under_loss_and_duplication() {
  let mut m = stage_parent(Mini::new().with_faults(7, 3));
  m.create_group(200, &[2], &[2]);
  m.freeze_timer(2, 200);
  drive_split_transfer(&mut m, |m| {
    m.fork_id(2, 200).is_some() && !m.applied(2, 200).is_empty()
  });

  assert_eq!(
    m.refused(2, 200),
    0,
    "every retried baseline is same-lineage — nothing to refuse"
  );
  let adopted = m.applied(2, 200);
  assert_eq!(adopted, m.applied(0, 200), "adopted the leader's record");

  // Keep the weather blowing: re-sends and duplicates after the adoption must all short-circuit.
  for _ in 0..50 {
    m.advance();
  }
  let installs_on_joiner = m
    .installs()
    .iter()
    .filter(|o| o.node == 2 && o.gid == 200)
    .count();
  assert_eq!(
    installs_on_joiner, 1,
    "exactly one adoption — every duplicate/retransfer resolved redundantly"
  );
  assert_eq!(
    m.applied(2, 200),
    adopted,
    "no duplicate-install effect on the adopted state"
  );
  assert!(
    !m.peer_in_snapshot(200, 2),
    "the leader exited Snapshot toward the joiner — the transfer terminated"
  );
}

/// TWO-TOKEN-RACE, the reachable face: the container admits a child id under the
/// single-incarnation contract, so a SECOND split naming a LIVE gid is refused at propose —
/// before any token is minted — and no path lands two lineages' baselines on one id. The
/// cross-lineage arm such a race WOULD otherwise reach (a foreign lineage's baseline onto a
/// populated child) is exactly the squatter-transfer refusal above.
#[test]
fn a_second_fork_onto_a_live_gid_is_refused_before_minting() {
  let mut m = stage_parent(Mini::new());
  m.create_group(300, &[0, 1, 2], &[0, 1, 2]);
  m.elect(300, 0);
  for key in 0u16..8 {
    m.propose(300, 0, &super::encode_gkv(300, key, u64::from(key)));
  }
  // The first split mints g200 and materializes it everywhere.
  m.propose_split(100, 0, 200, 4);
  assert!(m.hosts_group(0, 200), "the first fork minted the child");
  // A second split from a different parent naming the SAME live child id refuses at propose.
  assert!(
    matches!(
      m.propose_split_err(300, 0, 200, 4),
      Some(sailing_proto::SplitError::ChildExists)
    ),
    "a split onto a live child id must refuse before minting a second lineage"
  );
}

/// The LINEAGE LEDGER over the squatter transfer, fed exactly as the world's per-tick sweep
/// feeds it: installs drained since the previous sweep FIRST (each happened before the content
/// state now visible, so a destructive install is judged against the lineage the replica held
/// BEFORE it), then every hosted replica's applied record under its live `fork_id`. With the
/// receive path holding the fork-provenance gate, the refusal leaves no install to observe and
/// the ledger finalizes GREEN over real content; a receive path that ever LANDED a token-bearing
/// snapshot on this populated token-less occupant surfaces as a CHIMERA at finalize — the
/// mechanical catch for the coordinate-fusion family, independent of the behavioral pins above.
#[test]
fn squatter_transfer_keeps_the_lineage_ledger_green() {
  let mut ledger = LineageLedger::new();
  let mut fed = 0usize;
  let mut m = stage_parent(Mini::new());
  stage_term_contradicting_squatter(&mut m);
  // The squatter's committed token-less lineage goes on record before any transfer can land.
  sweep_ledger(&m, &mut ledger, &mut fed, 0);

  m.propose_split(100, 0, 200, 4);
  m.elect(200, 0);
  let mut tick = 1u64;
  for _ in 0..400 {
    sweep_ledger(&m, &mut ledger, &mut fed, tick);
    tick += 1;
    // Resolution under EITHER receive-path behavior: the gate refuses (counted), or a
    // destructive install lands on the occupant (observed — the chimera finalize trips on).
    if m.refused(2, 200) >= 1 || m.installs().iter().any(|o| o.node == 2 && o.gid == 200) {
      break;
    }
    m.advance();
  }
  sweep_ledger(&m, &mut ledger, &mut fed, tick);

  // The ledger's verdict SPEAKS FIRST: a destructive cross-lineage landing is the chimera it
  // trips on, before any behavioral pin can mask it. Green here, the behavioral pins follow.
  ledger.finalize_or_panic(0);
  assert!(
    ledger.cells_judged() > 0,
    "the ledger judged real committed content (non-vacuous)"
  );
  assert!(
    m.refused(2, 200) >= 1,
    "the transfer must resolve by refusal at the door"
  );
}

/// The ledger's INSTALL leg over the pristine adopter: the joiner's adoption is a real
/// token-bearing install, observed by the chimera detector (non-vacuous) and judged legitimate —
/// a lineage adopted from nothing. Green finalize over a run that installed.
#[test]
fn empty_joiner_adoption_feeds_the_ledger_install_leg() {
  let mut ledger = LineageLedger::new();
  let mut fed = 0usize;
  let mut m = stage_parent(Mini::new());
  m.create_group(200, &[2], &[2]);
  m.freeze_timer(2, 200);
  sweep_ledger(&m, &mut ledger, &mut fed, 0);

  m.propose_split(100, 0, 200, 4);
  m.elect(200, 0);
  let mut tick = 1u64;
  for _ in 0..400 {
    sweep_ledger(&m, &mut ledger, &mut fed, tick);
    tick += 1;
    if m.fork_id(2, 200).is_some() && !m.applied(2, 200).is_empty() {
      break;
    }
    m.advance();
  }
  sweep_ledger(&m, &mut ledger, &mut fed, tick);

  assert!(
    ledger.installs_observed() >= 1,
    "the chimera detector observed the joiner's real install (non-vacuous)"
  );
  assert!(ledger.cells_judged() > 0, "content was judged too");
  ledger.finalize_or_panic(0);
}

/// Feed `ledger` one sweep of the Mini's `g200` state, in the world sweep's order: installs
/// drained since the previous sweep first, then every hosted replica's content under its live
/// `fork_id`. `fed` is the install cursor across sweeps.
fn sweep_ledger(m: &Mini, ledger: &mut LineageLedger, fed: &mut usize, tick: u64) {
  for o in &m.installs()[*fed..] {
    if o.gid == 200 {
      ledger.observe_install(0, tick, (o.node, 200, 0), o.lineage.as_ref(), o.boundary);
    }
  }
  *fed = m.installs().len();
  for node in 0..4u64 {
    if !m.hosts_group(node, 200) {
      continue;
    }
    let lineage = m.fork_id(node, 200);
    ledger.observe_content(
      0,
      tick,
      (node, 200, 0),
      lineage.as_ref(),
      &m.applied(node, 200),
    );
  }
}
