//! The multi-group data-driven interaction harness: the single-group directive style scoped by a
//! mandatory `g=<gid>` argument, executed against [`MultiRaft`] container hosts.
//!
//! Where the single-group [`InteractionEnv`](crate::InteractionEnv) drives bare `Endpoint`s, this
//! harness drives one container per node and per-`(node, group)` stores, so a scenario can show
//! two groups electing independently on one host set, a group retiring while its sibling keeps
//! committing, and the same gid re-admitted fresh after removal (the PURE container has no
//! tombstones — the coordinator layer's tombstone gate is Tier B's to pin). Messages and events
//! render with a `g<gid>` prefix, making the per-group multiplexing visible in the goldens.
//!
//! Scenario files live in `tests/multi_interaction/*.txt` (same datadriven format as the
//! single-group corpus, same `SAILING_REWRITE=1` regeneration).

use crate::{
  LogSm, MemLog, MemStable,
  interaction::{
    parse::{Directive, parse_blocks},
    render::{fmt_data, fmt_set, indent, kind_str, render_event, render_msg, role_str},
  },
};
use core::time::Duration;
use sailing_proto::{Config, Index, Instant, Message, MultiRaft, Outgoing};
use std::{
  collections::{BTreeMap, VecDeque},
  string::{String, ToString},
  vec::Vec,
};

/// The fixed election timeout every harness replica is configured with (immaterial to the
/// scenarios — timers are driven explicitly — but it must exceed [`HEARTBEAT_INTERVAL`]).
const ELECTION_TIMEOUT: Duration = Duration::from_millis(1000);
/// The fixed heartbeat interval every harness replica is configured with.
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(100);

/// The multi-group harness environment: one [`MultiRaft`] host per node, per-`(node, gid)`
/// stores, an explicit `(from, to, gid, message)` bus, and a shared virtual clock.
pub struct MultiInteractionEnv {
  hosts: BTreeMap<u64, MultiRaft<u64, u64, LogSm>>,
  stores: BTreeMap<(u64, u64), (MemLog, MemStable<u64>)>,
  bus: VecDeque<(u64, u64, u64, Message<u64>)>,
  now: Instant,
}

impl Default for MultiInteractionEnv {
  fn default() -> Self {
    Self::new()
  }
}

/// Run a single multi-group data-driven file: execute each directive against a fresh
/// [`MultiInteractionEnv`], comparing rendered output against the recorded expectation, or
/// rewriting the file in place when `SAILING_REWRITE` is set. Panics with a readable diff on any
/// mismatch.
pub fn run_multi_interaction_file(path: &std::path::Path) {
  let content = std::fs::read_to_string(path)
    .unwrap_or_else(|e| panic!("cannot read multi interaction file {}: {e}", path.display()));
  let blocks = parse_blocks(&content);
  let rewrite = std::env::var_os("SAILING_REWRITE").is_some();

  let mut env = MultiInteractionEnv::new();
  let mut rebuilt = String::new();
  let mut failures = Vec::new();

  for b in &blocks {
    let directive = Directive::parse(&b.command);
    let actual = env.exec(&directive);
    let actual = actual.trim_end_matches('\n').to_string();

    for c in &b.comments {
      rebuilt.push_str(c);
      rebuilt.push('\n');
    }
    rebuilt.push_str(&b.command);
    rebuilt.push_str("\n----\n");
    if !actual.is_empty() {
      rebuilt.push_str(&actual);
      rebuilt.push('\n');
    }
    rebuilt.push('\n');

    let expected = b.expected.join("\n");
    if !rewrite && actual.trim_end() != expected.trim_end() {
      failures.push(std::format!(
        "command `{}`:\n--- expected ---\n{}\n--- actual ---\n{}",
        b.command,
        expected,
        actual
      ));
    }
  }

  if rewrite {
    std::fs::write(path, rebuilt).unwrap_or_else(|e| {
      panic!(
        "cannot rewrite multi interaction file {}: {e}",
        path.display()
      )
    });
  } else if !failures.is_empty() {
    panic!(
      "{} multi interaction mismatch(es) in {}:\n\n{}\n\n(run with SAILING_REWRITE=1 to regenerate)",
      failures.len(),
      path.display(),
      failures.join("\n\n")
    );
  }
}

impl MultiInteractionEnv {
  /// A fresh, empty environment (no hosts, empty bus, clock at the origin).
  pub fn new() -> Self {
    Self {
      hosts: BTreeMap::new(),
      stores: BTreeMap::new(),
      bus: VecDeque::new(),
      now: Instant::ORIGIN,
    }
  }

  /// Execute one directive and return its rendered output. Every directive carries a mandatory
  /// `g=<gid>` argument — the group it acts on.
  pub(crate) fn exec(&mut self, d: &Directive) -> String {
    let Some(gid) = d.value::<u64>("g") else {
      return std::format!("{}: missing mandatory g=<gid>\n", d.cmd);
    };
    match d.cmd.as_str() {
      "create-group" => self.create_group(gid, d),
      "remove-group" => self.remove_group(gid),
      "campaign" => self.campaign(gid, d),
      "propose" => self.propose(gid, d),
      "propose-conf-change" => self.propose_conf_change(gid, d),
      "propose-split" => self.propose_split(gid, d),
      "tick-heartbeat" => self.tick(gid, d, HEARTBEAT_INTERVAL),
      "tick-election" => self.tick(gid, d, ELECTION_TIMEOUT),
      "stabilize" => self.stabilize(gid),
      "status" => self.status(gid, d),
      "raft-state" => self.raft_state(gid),
      "raft-log" => self.raft_log(gid, d),
      "applied" => self.applied(gid, d),
      other => std::format!("unknown command: {other}\n"),
    }
  }

  /// The named argument as a set of node ids, accepting both `voters=(1,2,3)` and the bare
  /// `voters=1,2,3` comma form.
  fn id_list(d: &Directive, key: &str) -> Vec<u64> {
    let mut ids: Vec<u64> = d
      .values(key)
      .iter()
      .flat_map(|v| v.split(','))
      .filter_map(|s| s.trim().parse().ok())
      .collect();
    ids.sort_unstable();
    ids.dedup();
    ids
  }

  /// `create-group g=<gid> voters=(a,b,c)` — wire a replica of `gid` on every voter (creating
  /// the container host on first use). The container refuses a duplicate gid on a host; after a
  /// `remove-group` the same gid can be admitted fresh (no container-level tombstones).
  fn create_group(&mut self, gid: u64, d: &Directive) -> String {
    let voters = Self::id_list(d, "voters");
    if voters.is_empty() {
      return "create-group: missing voters=(..)\n".to_string();
    }
    let mut out = String::new();
    for &id in &voters {
      let host = self.hosts.entry(id).or_default();
      let cfg = Config::try_new(id, voters.clone(), ELECTION_TIMEOUT, HEARTBEAT_INTERVAL)
        .expect("valid harness config");
      match host.create_group(gid, 0, cfg, self.now, id, LogSm::new()) {
        Ok(()) => {
          self
            .stores
            .insert((id, gid), (MemLog::new(), MemStable::new()));
          out.push_str(&std::format!(
            "n{id}: created g{gid} voters={}\n",
            fmt_set(&voters)
          ));
        }
        Err(e) => out.push_str(&std::format!("n{id}: create g{gid} rejected: {e:?}\n")),
      }
    }
    out
  }

  /// `remove-group g=<gid>` — the embedder retires the group everywhere: container removal on
  /// every hosting node, store teardown, and a bus purge of the gid's in-flight messages (the
  /// coordinator-layer drop the pure container does not model itself).
  fn remove_group(&mut self, gid: u64) -> String {
    let hosting: Vec<u64> = self
      .hosts
      .iter()
      .filter(|(_, h)| h.contains_group(&gid))
      .map(|(id, _)| *id)
      .collect();
    if hosting.is_empty() {
      return std::format!("g{gid}: not hosted anywhere\n");
    }
    for &id in &hosting {
      self
        .hosts
        .get_mut(&id)
        .expect("host exists")
        .remove_group(&gid)
        .expect("a scenario never tears down a group that still owes a thaw");
      self.stores.remove(&(id, gid));
    }
    let before = self.bus.len();
    self.bus.retain(|(_, _, g, _)| *g != gid);
    std::format!(
      "g{gid} removed on {} ({} in-flight dropped)\n",
      fmt_set(&hosting),
      before - self.bus.len()
    )
  }

  /// `campaign g=<gid> <node>` — advance the clock to that replica's own deadline and fire its
  /// timer so it campaigns; renders the role/term change and the messages it queues.
  fn campaign(&mut self, gid: u64, d: &Directive) -> String {
    let Some(id) = d.positional(0).and_then(|s| s.parse::<u64>().ok()) else {
      return "campaign: missing node id\n".to_string();
    };
    if !self.hosts.get(&id).is_some_and(|h| h.contains_group(&gid)) {
      return std::format!("n{id}: does not host g{gid}\n");
    }
    let deadline = self
      .hosts
      .get(&id)
      .and_then(|h| h.deadlines().find(|(g, _)| *g == gid).map(|(_, dl)| dl));
    if let Some(dl) = deadline
      && dl > self.now
    {
      self.now = dl;
    }
    let before = {
      let ep = self.hosts[&id].group(&gid).expect("hosted");
      (ep.role(), ep.term())
    };
    {
      let now = self.now;
      let host = self.hosts.get_mut(&id).expect("host exists");
      let (log, stable) = self.stores.get_mut(&(id, gid)).expect("replica stores");
      host
        .handle_timeout(&gid, now, log, stable)
        .expect("hosted group fires");
    }
    let mut out = String::new();
    let after = {
      let ep = self.hosts[&id].group(&gid).expect("hosted");
      (ep.role(), ep.term())
    };
    if after != before {
      out.push_str(&std::format!(
        "g{gid} n{id} {} term={}\n",
        role_str(after.0),
        after.1.get()
      ));
    }
    self.drain_node(id, &mut out);
    out
  }

  /// `propose g=<gid> <node> data=<bytes>` — the node proposes a normal command on its `gid`
  /// replica, the replication batch flushes, and the node drains. The keyed form
  /// `key=<k> value=<v>` proposes the gkv payload instead — the keyed sim FSM's split domain,
  /// so a split scenario's handover is visible through the `applied` directive.
  fn propose(&mut self, gid: u64, d: &Directive) -> String {
    let Some(id) = d.positional(0).and_then(|s| s.parse::<u64>().ok()) else {
      return "propose: missing node id\n".to_string();
    };
    let cmd = match (d.value::<u16>("key"), d.value::<u64>("value")) {
      (Some(key), Some(value)) => bytes::Bytes::from(super::encode_gkv(gid, key, value)),
      _ => {
        let data = d.value::<String>("data").unwrap_or_default();
        bytes::Bytes::from(data.into_bytes())
      }
    };
    if !self.hosts.get(&id).is_some_and(|h| h.contains_group(&gid)) {
      return std::format!("n{id}: does not host g{gid}\n");
    }
    let mut out = String::new();
    {
      let now = self.now;
      let host = self.hosts.get_mut(&id).expect("host exists");
      let (log, stable) = self.stores.get_mut(&(id, gid)).expect("replica stores");
      match host.propose(&gid, now, log, stable, &cmd) {
        Some(Ok(idx)) => out.push_str(&std::format!("g{gid} n{id} proposed index={}\n", idx.get())),
        Some(Err(e)) => out.push_str(&std::format!("g{gid} n{id} propose rejected: {e:?}\n")),
        None => out.push_str(&std::format!("n{id}: does not host g{gid}\n")),
      }
      // Flush once at the propose point (a re-driven flush would re-send to a still-Probe peer).
      host
        .flush_appends(&gid, now, log, stable)
        .expect("hosted group flushes");
    }
    self.drain_node(id, &mut out);
    out
  }

  /// `propose-conf-change g=<gid> <leader> <op> <node>` — `op` ∈ {`add`, `remove`,
  /// `addlearner`}. A joining node not yet hosting the gid is first wired as an OBSERVER replica
  /// (bootstrap voters = the leader's committed voters, so it cannot campaign) before the v1
  /// change is proposed on the leader; membership moves only at apply time.
  fn propose_conf_change(&mut self, gid: u64, d: &Directive) -> String {
    let Some(leader) = d.positional(0).and_then(|s| s.parse::<u64>().ok()) else {
      return "propose-conf-change: missing leader id\n".to_string();
    };
    let op = d.positional(1).unwrap_or("").to_string();
    let Some(node) = d.positional(2).and_then(|s| s.parse::<u64>().ok()) else {
      return "propose-conf-change: missing node id\n".to_string();
    };
    let ty = match op.as_str() {
      "add" => sailing_proto::ConfChangeType::AddNode,
      "remove" => sailing_proto::ConfChangeType::RemoveNode,
      "addlearner" => sailing_proto::ConfChangeType::AddLearnerNode,
      other => return std::format!("propose-conf-change: unknown op `{other}`\n"),
    };
    if matches!(op.as_str(), "add" | "addlearner")
      && !self
        .hosts
        .get(&node)
        .is_some_and(|h| h.contains_group(&gid))
    {
      let current_voters: Vec<u64> = self
        .hosts
        .get(&leader)
        .and_then(|h| h.group(&gid))
        .map(|ep| ep.conf_state().voters().iter().copied().collect())
        .unwrap_or_default();
      let cfg =
        Config::try_new_observer(node, current_voters, ELECTION_TIMEOUT, HEARTBEAT_INTERVAL)
          .expect("valid observer config");
      let host = self.hosts.entry(node).or_default();
      host
        .create_group(gid, 0, cfg, self.now, node, LogSm::new())
        .expect("observer admission");
      self
        .stores
        .insert((node, gid), (MemLog::new(), MemStable::new()));
    }
    let cc = sailing_proto::ConfChange::new(ty, node, bytes::Bytes::new());
    let mut out = String::new();
    {
      let now = self.now;
      let host = self.hosts.get_mut(&leader).expect("leader host exists");
      let (log, stable) = self.stores.get_mut(&(leader, gid)).expect("leader stores");
      match host.propose_conf_change(&gid, now, log, stable, cc) {
        Some(Ok(idx)) => out.push_str(&std::format!(
          "g{gid} n{leader} proposed conf-change {op} n{node} at index {}\n",
          idx.get()
        )),
        Some(Err(e)) => {
          out.push_str(&std::format!(
            "g{gid} n{leader} conf-change rejected: {e:?}\n"
          ));
        }
        None => out.push_str(&std::format!("n{leader}: does not host g{gid}\n")),
      }
      host
        .flush_appends(&gid, now, log, stable)
        .expect("hosted group flushes");
    }
    self.drain_node(leader, &mut out);
    out
  }

  /// `propose-split g=<gid> <leader> child=<cid> point=<key>` — the leader proposes forking the
  /// keyed FSM's at-or-above-`point` slice into the fresh group `cid` (gen 0 — harness child ids
  /// are never reused). Renders the accepted index or the typed refusal, then flushes and drains
  /// exactly like `propose`; the committed entry's forks materialize in later drains (see
  /// `drain_node`'s fork pump).
  fn propose_split(&mut self, gid: u64, d: &Directive) -> String {
    let Some(id) = d.positional(0).and_then(|s| s.parse::<u64>().ok()) else {
      return "propose-split: missing node id\n".to_string();
    };
    let Some(child) = d.value::<u64>("child") else {
      return "propose-split: missing child=<gid>\n".to_string();
    };
    let Some(point) = d.value::<u16>("point") else {
      return "propose-split: missing point=<key>\n".to_string();
    };
    if !self.hosts.get(&id).is_some_and(|h| h.contains_group(&gid)) {
      return std::format!("n{id}: does not host g{gid}\n");
    }
    let mut out = String::new();
    {
      let now = self.now;
      let host = self.hosts.get_mut(&id).expect("host exists");
      let (log, stable) = self.stores.get_mut(&(id, gid)).expect("replica stores");
      let instruction = bytes::Bytes::copy_from_slice(&point.to_le_bytes());
      match host.propose_split(&gid, now, log, stable, &child, 0, instruction) {
        Some(Ok(idx)) => out.push_str(&std::format!(
          "g{gid} n{id} proposed split child=g{child} point={point} index={}\n",
          idx.get()
        )),
        Some(Err(e)) => out.push_str(&std::format!("g{gid} n{id} split rejected: {e:?}\n")),
        None => out.push_str(&std::format!("n{id}: does not host g{gid}\n")),
      }
      host
        .flush_appends(&gid, now, log, stable)
        .expect("hosted group flushes");
    }
    self.drain_node(id, &mut out);
    out
  }

  /// `tick-heartbeat g=<gid> <node>` / `tick-election g=<gid> <node>` — advance the shared clock
  /// by one interval and fire that replica's timer.
  fn tick(&mut self, gid: u64, d: &Directive, by: Duration) -> String {
    let Some(id) = d.positional(0).and_then(|s| s.parse::<u64>().ok()) else {
      return "tick: missing node id\n".to_string();
    };
    if !self.hosts.get(&id).is_some_and(|h| h.contains_group(&gid)) {
      return std::format!("n{id}: does not host g{gid}\n");
    }
    let deadline = self.now + by;
    if deadline > self.now {
      self.now = deadline;
    }
    {
      let now = self.now;
      let host = self.hosts.get_mut(&id).expect("host exists");
      let (log, stable) = self.stores.get_mut(&(id, gid)).expect("replica stores");
      host
        .handle_timeout(&gid, now, log, stable)
        .expect("hosted group fires");
    }
    let mut out = String::new();
    self.drain_node(id, &mut out);
    if out.is_empty() {
      out.push_str("ok\n");
    }
    out
  }

  /// `stabilize g=<gid>` — run the world to quiescence FOR ONE GROUP: repeatedly drain every
  /// node (all groups' outgoing reach the bus) but deliver only `gid`'s messages, until nothing
  /// new happens. Other groups' in-flight traffic stays queued — their own `stabilize` moves it,
  /// which is what makes per-group independence visible in a golden.
  fn stabilize(&mut self, gid: u64) -> String {
    let mut out = String::new();
    let mut iters = 0u32;
    loop {
      iters += 1;
      assert!(iters < 10_000, "stabilize livelock");
      let mut progressed = false;

      let ids: Vec<u64> = self.hosts.keys().copied().collect();
      for id in &ids {
        let mut node_out = String::new();
        if self.drain_node(*id, &mut node_out) {
          progressed = true;
        }
        if !node_out.is_empty() {
          out.push_str(&std::format!("> n{id} ready\n"));
          out.push_str(&indent(&node_out));
        }
      }

      let mut kept = VecDeque::new();
      let pending: Vec<(u64, u64, u64, Message<u64>)> = self.bus.drain(..).collect();
      for (from, to, g, msg) in pending {
        if g != gid {
          kept.push_back((from, to, g, msg));
          continue;
        }
        progressed = true;
        let hosted = self.hosts.get(&to).is_some_and(|h| h.contains_group(&g));
        if !hosted {
          // The unhosted-drop semantics of the group-tagged wire, rendered so a golden shows
          // exactly which straggler died at the demux.
          out.push_str(&std::format!(
            "> n{to} drops g{g} {} (unhosted)\n",
            render_msg(from, to, &msg)
          ));
          continue;
        }
        out.push_str(&std::format!(
          "> n{to} recv g{g} {}\n",
          render_msg(from, to, &msg)
        ));
        let now = self.now;
        let host = self.hosts.get_mut(&to).expect("host exists");
        let (log, stable) = self.stores.get_mut(&(to, g)).expect("replica stores");
        host
          .handle_message(&g, now, log, stable, from, msg)
          .expect("hosted group handles");
      }
      self.bus = kept;

      if !progressed {
        break;
      }
    }
    if out.is_empty() {
      out.push_str("ok\n");
    }
    out
  }

  /// `status g=<gid> <node>` — the node's replication progress to each peer of its gid replica.
  fn status(&mut self, gid: u64, d: &Directive) -> String {
    let Some(id) = d.positional(0).and_then(|s| s.parse::<u64>().ok()) else {
      return "status: missing node id\n".to_string();
    };
    let Some(ep) = self.hosts.get(&id).and_then(|h| h.group(&gid)) else {
      return std::format!("n{id}: does not host g{gid}\n");
    };
    let cs = ep.conf_state();
    let mut peers: Vec<u64> = cs
      .voters()
      .iter()
      .chain(cs.learners().iter())
      .copied()
      .filter(|&p| p != id)
      .collect();
    peers.sort_unstable();
    peers.dedup();
    let mut out = String::new();
    for p in peers {
      match ep.peer_progress(&p) {
        Some(pr) => out.push_str(&std::format!(
          "n{p}: match={} next={} {}{}\n",
          pr.match_index.get(),
          pr.next_index.get(),
          pr.state.as_str(),
          if pr.paused { " paused" } else { "" },
        )),
        None => out.push_str(&std::format!("n{p}: (no progress)\n")),
      }
    }
    if out.is_empty() {
      out.push_str(&std::format!("n{id}: no peers\n"));
    }
    out
  }

  /// `raft-state g=<gid>` — one line per HOSTING node: its gid replica's role, term, and
  /// believed leader.
  fn raft_state(&mut self, gid: u64) -> String {
    let mut out = String::new();
    for (id, host) in &self.hosts {
      let Some(ep) = host.group(&gid) else {
        continue;
      };
      out.push_str(&std::format!(
        "n{id}: {} term={} lead={}\n",
        role_str(ep.role()),
        ep.term().get(),
        ep.leader()
          .map(|l| l.to_string())
          .unwrap_or_else(|| "none".to_string()),
      ));
    }
    if out.is_empty() {
      out.push_str(&std::format!("g{gid}: not hosted anywhere\n"));
    }
    out
  }

  /// `raft-log g=<gid> <node>` — the replica's commit/applied watermarks and its durable log.
  fn raft_log(&mut self, gid: u64, d: &Directive) -> String {
    let Some(id) = d.positional(0).and_then(|s| s.parse::<u64>().ok()) else {
      return "raft-log: missing node id\n".to_string();
    };
    let Some(ep) = self.hosts.get(&id).and_then(|h| h.group(&gid)) else {
      return std::format!("n{id}: does not host g{gid}\n");
    };
    use sailing_proto::LogStore;
    let (log, _) = self.stores.get(&(id, gid)).expect("replica stores");
    let mut out = std::format!(
      "n{id}: commit={} applied={} last={}\n",
      ep.commit_index().get(),
      ep.applied_index().get(),
      log.last_index().get(),
    );
    let first = log.first_index();
    let last = log.last_index();
    if last >= first
      && let Ok(sailing_proto::EntriesRead::Ready(entries)) =
        log.entries(first..Index::new(last.get() + 1), u64::MAX)
    {
      for e in entries.iter() {
        out.push_str(&std::format!(
          "  {}/{} {}{}\n",
          e.term().get(),
          e.index().get(),
          kind_str(e.kind()),
          fmt_data(e.data()),
        ));
      }
    }
    out
  }

  /// `applied g=<gid> <node>` — the replica's applied record, gkv cells decoded as
  /// `g<tag> k<key>=<value>` (the tag names the group that ACCEPTED the write, so a fork-inherited
  /// baseline renders parent-tagged) and any other command via the data formatter. The golden's
  /// view of a split handover.
  fn applied(&mut self, gid: u64, d: &Directive) -> String {
    let Some(id) = d.positional(0).and_then(|s| s.parse::<u64>().ok()) else {
      return "applied: missing node id\n".to_string();
    };
    let Some(ep) = self.hosts.get(&id).and_then(|h| h.group(&gid)) else {
      return std::format!("n{id}: does not host g{gid}\n");
    };
    let cells = ep.state_machine().applied();
    let mut out = std::format!("n{id}: applied={}\n", cells.len());
    for (idx, cmd) in cells {
      match super::decode_gkv(cmd) {
        Some((tag, key, value)) => {
          out.push_str(&std::format!("  {} g{tag} k{key}={value}\n", idx.get()));
        }
        None => out.push_str(&std::format!("  {}{}\n", idx.get(), fmt_data(cmd))),
      }
    }
    out
  }

  /// Process node `id`'s storage completions for EVERY hosted group, then drain its outgoing
  /// messages onto the bus and its events, rendering each with the owning group's tag. Returns
  /// whether anything was produced.
  fn drain_node(&mut self, id: u64, out: &mut String) -> bool {
    let now = self.now;
    let mut produced = false;
    let mut guard = 0u32;
    loop {
      guard += 1;
      assert!(guard < 10_000, "drain_node storage livelock");
      let host = match self.hosts.get_mut(&id) {
        Some(h) => h,
        None => return false,
      };
      let gids: Vec<u64> = host.group_ids().copied().collect();
      for g in gids {
        let host = self.hosts.get_mut(&id).expect("host exists");
        let (log, stable) = self.stores.get_mut(&(id, g)).expect("replica stores");
        while host
          .handle_storage(&g, now, log, stable)
          .expect("hosted group drains")
          .is_more_pending()
        {}
      }
      let mut any = false;
      while let Some((g, o)) = self.hosts.get_mut(&id).expect("host exists").poll_message() {
        any = true;
        produced = true;
        let (to, msg) = Outgoing::into_parts(o);
        out.push_str(&std::format!("g{g} {}\n", render_msg(id, to, &msg)));
        self.bus.push_back((id, to, g, msg));
      }
      while let Some((g, ev)) = self.hosts.get_mut(&id).expect("host exists").poll_event() {
        produced = true;
        out.push_str(&std::format!("g{g} {}", render_event(id, &ev)));
      }
      // Materialize committed forks the drains above applied — the harness plays the driver's
      // fork drain. Sync in-memory stores make the child's baseline durable at the call, so the
      // parent's snapshot barrier lifts immediately (the one-crank engine-flush contract in its
      // synchronous form); the env never restarts a node, so every fork is its replica's first
      // boot (epoch 1). A materialization re-runs the loop: the child is a hosted group from
      // this point on and drains like any other.
      let mut forked = false;
      loop {
        let host = self.hosts.get_mut(&id).expect("host exists");
        let Some(fork) = host.poll_pending_fork() else {
          break;
        };
        forked = true;
        produced = true;
        let (parent, child, split_index) = (fork.parent, fork.child, fork.split_index);
        self
          .stores
          .insert((id, child), (MemLog::new(), MemStable::new()));
        let (log, stable) = self.stores.get_mut(&(id, child)).expect("fresh stores");
        let host = self.hosts.get_mut(&id).expect("host exists");
        host
          .create_group_from_fork(
            child,
            fork.child_gen,
            fork.config,
            now,
            id,
            fork.fsm,
            fork.blob,
            fork.read_only,
            1,
            log,
            stable,
          )
          .unwrap_or_else(|e| panic!("fork of g{child} on n{id}: {e:?}"));
        host.lift_fork_barrier(&parent, split_index);
        let inherited = host
          .group(&child)
          .map(|ep| ep.state_machine().applied().len())
          .unwrap_or(0);
        out.push_str(&std::format!(
          "g{child} n{id} forked from g{parent} ({inherited} inherited)\n"
        ));
      }
      while let Some((p, c)) = self
        .hosts
        .get_mut(&id)
        .expect("host exists")
        .poll_split_conflict()
      {
        produced = true;
        out.push_str(&std::format!(
          "n{id}: split conflict parent=g{p} child=g{c} (parked)\n"
        ));
      }
      if !any && !forked {
        break;
      }
    }
    produced
  }
}

#[cfg(test)]
mod tests;
