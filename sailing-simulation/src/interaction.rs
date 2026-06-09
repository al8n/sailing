//! The data-driven interaction harness — sailing's port of etcd's `rafttest`
//! `datadriven` testing approach.
//!
//! # What this is
//!
//! Each scenario is a `tests/interaction/*.txt` file in etcd's data-driven format: a sequence of
//! `command args` directives, each followed by `----` and the directive's expected output. The
//! harness parses a file, runs each directive against a live set of [`Endpoint`]s, renders the
//! resulting messages / events / state to text, and compares it against the recorded expectation.
//! Running with `SAILING_REWRITE=1` regenerates the expectations in place (etcd's `-rewrite`).
//!
//! # Why the goldens differ from etcd's
//!
//! etcd renders its own `Ready` batches and the raft package's internal `INFO` logs. sailing's
//! proto is a no_std Sans-I/O core with **no** internal logging and **no** `Ready` struct — its
//! observable surface is [`Endpoint::poll_message`] (outgoing wire messages), [`Endpoint::poll_event`]
//! (applied / leader-change / conf-change / read-state), and the read accessors. So the *scenarios*
//! (the command scripts) port directly, but the rendered output is sailing-native and regenerated
//! fresh — it is not a copy of etcd's text.
//!
//! # The model
//!
//! The harness owns one `Node` per id (an [`Endpoint`] + its [`MemLog`] + [`MemStable`]) and an
//! explicit in-flight message bus, plus a shared virtual clock. Unlike [`Cluster`](crate::Cluster)
//! (which drives a whole cluster forward one global tick at a time and runs the VOPR oracles), this
//! harness gives **per-node, per-message** control: campaign exactly one node, deliver exactly the
//! messages you choose, process exactly one node's durability/apply step. That explicit control is
//! what lets a scenario construct a precise interleaving (the point of the corpus).

use core::time::Duration;
use std::{
  collections::{BTreeMap, BTreeSet, VecDeque},
  string::{String, ToString},
  vec::Vec,
};

use sailing_proto::{Config, Endpoint, EntryKind, Index, Instant, Message, Role};

use crate::{LogSm, MemLog, MemStable};

/// The fixed election timeout every harness node is configured with. The exact value is immaterial
/// to the scenarios (they drive timers explicitly via `campaign` / `tick-*`), but it must exceed
/// [`HEARTBEAT_INTERVAL`].
const ELECTION_TIMEOUT: Duration = Duration::from_millis(1000);
/// The fixed heartbeat interval every harness node is configured with.
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(100);

/// One node under the harness: its proto [`Endpoint`] plus the two stores it writes through. The
/// `config` is retained so the node can be rebuilt by [`Endpoint::restart`] on `crash`.
struct Node {
  ep: Endpoint<u64, LogSm>,
  log: MemLog,
  stable: MemStable<u64>,
  config: Config<u64>,
}

/// A parsed directive argument: a key with zero-or-more values. A bare positional argument is
/// stored with an empty key; `key=val` is `(key, [val])`; `key=(a,b,c)` is `(key, [a,b,c])`.
#[derive(Debug, Clone)]
struct Arg {
  key: String,
  vals: Vec<String>,
}

/// The parsed form of a single directive line: the command name and its arguments.
#[derive(Debug, Clone)]
struct Directive {
  cmd: String,
  args: Vec<Arg>,
}

impl Directive {
  /// Parse a directive line like `add-nodes 3 voters=(1,2,3) index=2 prevote=true`.
  fn parse(line: &str) -> Self {
    let mut toks = tokenize(line);
    let cmd = toks.next().unwrap_or_default();
    let mut args = Vec::new();
    for tok in toks {
      if let Some((k, v)) = tok.split_once('=') {
        let vals = if let Some(inner) = v.strip_prefix('(').and_then(|s| s.strip_suffix(')')) {
          inner
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToString::to_string)
            .collect()
        } else {
          std::vec![v.to_string()]
        };
        args.push(Arg {
          key: k.to_string(),
          vals,
        });
      } else {
        args.push(Arg {
          key: String::new(),
          vals: std::vec![tok],
        });
      }
    }
    Self { cmd, args }
  }

  /// The `i`-th positional (keyless) argument, if present.
  fn positional(&self, i: usize) -> Option<&str> {
    self
      .args
      .iter()
      .filter(|a| a.key.is_empty())
      .nth(i)
      .and_then(|a| a.vals.first())
      .map(String::as_str)
  }

  /// All values of the named argument (e.g. `voters`), or empty if absent.
  fn values(&self, key: &str) -> &[String] {
    self
      .args
      .iter()
      .find(|a| a.key == key)
      .map(|a| a.vals.as_slice())
      .unwrap_or(&[])
  }

  /// The single value of the named argument, parsed as `T`.
  fn value<T: core::str::FromStr>(&self, key: &str) -> Option<T> {
    self.values(key).first().and_then(|v| v.parse().ok())
  }

  /// A boolean flag: `key` (bare) or `key=true` ⇒ true; absent ⇒ false.
  fn flag(&self, key: &str) -> bool {
    self
      .args
      .iter()
      .any(|a| a.key == key && a.vals.first().map(String::as_str) != Some("false"))
  }
}

/// Split a directive line into whitespace-separated tokens, keeping `(...)` groups intact so a
/// `voters=(1, 2, 3)` value is not split on its inner spaces.
fn tokenize(line: &str) -> std::vec::IntoIter<String> {
  let mut out = Vec::new();
  let mut cur = String::new();
  let mut depth = 0i32;
  for ch in line.trim().chars() {
    match ch {
      '(' => {
        depth += 1;
        cur.push(ch);
      }
      ')' => {
        depth -= 1;
        cur.push(ch);
      }
      c if c.is_whitespace() && depth == 0 => {
        if !cur.is_empty() {
          out.push(core::mem::take(&mut cur));
        }
      }
      c => cur.push(c),
    }
  }
  if !cur.is_empty() {
    out.push(cur);
  }
  out.into_iter()
}

impl InteractionEnv {
  /// A fresh, empty environment (no nodes, empty bus, clock at the origin).
  pub fn new() -> Self {
    Self {
      nodes: BTreeMap::new(),
      bus: VecDeque::new(),
      now: Instant::ORIGIN,
      partitioned: BTreeSet::new(),
    }
  }

  /// Execute one directive and return its rendered output (the text compared against the golden).
  fn exec(&mut self, d: &Directive) -> String {
    match d.cmd.as_str() {
      "add-nodes" => self.add_nodes(d),
      "campaign" => self.campaign(d),
      "stabilize" => self.stabilize(),
      "deliver-msgs" => self.deliver_msgs(d),
      "process-ready" => self.process_ready(d),
      "propose" => self.propose(d),
      "propose-conf-change" => self.propose_conf_change(d),
      "propose-conf-change-v2" => self.propose_conf_change_v2(d),
      "transfer-leadership" => self.transfer_leadership(d),
      "tick-heartbeat" => self.tick(d, HEARTBEAT_INTERVAL),
      "tick-election" => self.tick(d, ELECTION_TIMEOUT),
      "isolate" => self.isolate(d),
      "recover" => self.recover(d),
      "flush" => self.flush(d),
      "crash" => self.crash(d),
      "read-index" => self.read_index(d),
      "raft-state" => self.raft_state(),
      "raft-log" => self.raft_log(d),
      "conf-state" => self.conf_state_cmd(d),
      "status" => self.status(d),
      other => std::format!("unknown command: {other}\n"),
    }
  }

  /// `add-nodes <N> voters=(...) [learners=(...)] [prevote=true] [checkquorum=true]` — create N
  /// fresh voter nodes (ids 1..=N by default, or the `voters` set) with empty logs.
  fn add_nodes(&mut self, d: &Directive) -> String {
    let n: u64 = d.positional(0).and_then(|s| s.parse().ok()).unwrap_or(0);
    let voters: Vec<u64> = if d.values("voters").is_empty() {
      (1..=n).collect()
    } else {
      d.values("voters")
        .iter()
        .filter_map(|s| s.parse().ok())
        .collect()
    };
    let pre_vote = d.flag("prevote");
    let check_quorum = d.flag("checkquorum");
    let async_store = d.flag("async");
    let inflight: usize = d.value("inflight").unwrap_or(256);
    let snap: usize = d.value("snap").unwrap_or(10_000);

    let mut out = String::new();
    for &id in &voters {
      let cfg = Config::try_new(id, voters.clone(), ELECTION_TIMEOUT, HEARTBEAT_INTERVAL)
        .expect("valid harness config")
        .with_pre_vote(pre_vote)
        .with_check_quorum(check_quorum)
        .with_max_inflight_msgs(inflight)
        .expect("valid inflight window")
        .with_snapshot_threshold(snap);
      let ep = Endpoint::new(cfg.clone(), Instant::ORIGIN, id, LogSm::new());
      // Async stores re-open the fsync-in-flight window: a submitted write is visible but not durable
      // until an explicit `flush`, so acks/votes stay deferred until then (persist-before-send).
      let (log, stable) = if async_store {
        (MemLog::new_async(id), MemStable::new_async(id))
      } else {
        (MemLog::new(), MemStable::new())
      };
      self.nodes.insert(
        id,
        Node {
          ep,
          log,
          stable,
          config: cfg,
        },
      );
      out.push_str(&std::format!(
        "n{id}: created voters={} term={} commit={} last={}\n",
        fmt_set(&voters),
        self.nodes[&id].ep.term().get(),
        self.nodes[&id].ep.commit_index().get(),
        self.nodes[&id].log_last(),
      ));
    }
    out
  }

  /// `campaign <id>` — drive node `id`'s election timer so it becomes a candidate, persist its
  /// vote, and emit `RequestVote`s (drained onto the bus). Renders the role/term change and the
  /// messages it queues.
  fn campaign(&mut self, d: &Directive) -> String {
    let id: u64 = match d.positional(0).and_then(|s| s.parse().ok()) {
      Some(id) => id,
      None => return "campaign: missing node id\n".to_string(),
    };
    let (before_role, before_term) = {
      let n = &self.nodes[&id];
      (n.ep.role(), n.ep.term())
    };
    // Advance the clock to this node's election deadline and fire its timer.
    if let Some(deadline) = self.nodes.get(&id).and_then(|n| n.ep.poll_timeout()) {
      if deadline > self.now {
        self.now = deadline;
      }
    }
    {
      let now = self.now;
      let n = self.nodes.get_mut(&id).unwrap();
      n.ep.handle_timeout(now, &mut n.log, &mut n.stable);
    }
    let mut out = String::new();
    let (after_role, after_term) = {
      let n = &self.nodes[&id];
      (n.ep.role(), n.ep.term())
    };
    if after_role != before_role || after_term != before_term {
      out.push_str(&std::format!(
        "n{id} {} term={}\n",
        role_str(after_role),
        after_term.get()
      ));
    }
    // Persist the vote (sync store completes immediately) and release the RequestVotes.
    self.drain_node(id, &mut out);
    let _ = (before_role, before_term);
    out
  }

  /// `propose <id> <data>` — node `id` proposes a normal command, then its durability step runs so
  /// the new entry is appended and replicated.
  fn propose(&mut self, d: &Directive) -> String {
    let id: u64 = match d.positional(0).and_then(|s| s.parse().ok()) {
      Some(id) => id,
      None => return "propose: missing node id\n".to_string(),
    };
    let cmd = bytes::Bytes::from(d.positional(1).unwrap_or("").as_bytes().to_vec());
    let mut out = String::new();
    {
      let now = self.now;
      let n = self.nodes.get_mut(&id).unwrap();
      let _ = n.ep.propose(now, &mut n.log, &n.stable, &cmd);
    }
    self.drain_node(id, &mut out);
    if out.is_empty() {
      out.push_str("ok\n");
    }
    out
  }

  /// `propose-conf-change <leader> <op> <node>` — `op` ∈ {`add`, `remove`, `addlearner`}. For an
  /// `add`/`addlearner` of a not-yet-wired node, the node is first created as an **observer**
  /// (bootstrap voter set = the leader's committed voters, so it cannot campaign or disrupt the
  /// existing leader) before the v1 [`ConfChange`](sailing_proto::ConfChange) is proposed on the
  /// leader. The change takes effect (apply-time) only once committed-and-applied — watch the
  /// `conf-changed` events during the following `stabilize`.
  fn propose_conf_change(&mut self, d: &Directive) -> String {
    let leader: u64 = match d.positional(0).and_then(|s| s.parse().ok()) {
      Some(v) => v,
      None => return "propose-conf-change: missing leader id\n".to_string(),
    };
    let op = d.positional(1).unwrap_or("").to_string();
    let node: u64 = match d.positional(2).and_then(|s| s.parse().ok()) {
      Some(v) => v,
      None => return "propose-conf-change: missing node id\n".to_string(),
    };
    let ty = match op.as_str() {
      "add" => sailing_proto::ConfChangeType::AddNode,
      "remove" => sailing_proto::ConfChangeType::RemoveNode,
      "addlearner" => sailing_proto::ConfChangeType::AddLearnerNode,
      other => return std::format!("propose-conf-change: unknown op `{other}`\n"),
    };
    // A brand-new joining node is wired as an observer so it can receive replication; it gains
    // voter/learner status only by applying the committed ConfChange in its own view.
    if matches!(op.as_str(), "add" | "addlearner") && !self.nodes.contains_key(&node) {
      let current_voters: Vec<u64> = self
        .nodes
        .get(&leader)
        .map(|n| n.ep.conf_state().voters().iter().copied().collect())
        .unwrap_or_default();
      let cfg =
        Config::try_new_observer(node, current_voters, ELECTION_TIMEOUT, HEARTBEAT_INTERVAL)
          .expect("valid observer config");
      let ep = Endpoint::new(cfg.clone(), Instant::ORIGIN, node, LogSm::new());
      self.nodes.insert(
        node,
        Node {
          ep,
          log: MemLog::new(),
          stable: MemStable::new(),
          config: cfg,
        },
      );
    }
    let cc = sailing_proto::ConfChange::new(ty, node, bytes::Bytes::new());
    let mut out = String::new();
    {
      let now = self.now;
      let n = self.nodes.get_mut(&leader).unwrap();
      match n.ep.propose_conf_change(now, &mut n.log, &n.stable, cc) {
        Ok(idx) => out.push_str(&std::format!(
          "n{leader} proposed conf-change {op} n{node} at index {}\n",
          idx.get()
        )),
        Err(e) => out.push_str(&std::format!("n{leader} conf-change rejected: {e:?}\n")),
      }
    }
    self.drain_node(leader, &mut out);
    out
  }

  /// `propose-conf-change-v2 <leader> [transition=auto|implicit|explicit] [add=N…] [remove=N…]
  /// [addlearner=N…]` — propose a joint [`ConfChangeV2`](sailing_proto::ConfChangeV2) bundling zero or
  /// more single changes. With NO changes it is the joint-**leave** entry (for an `explicit`
  /// transition the application writes this itself). Joining nodes are wired as observers first.
  fn propose_conf_change_v2(&mut self, d: &Directive) -> String {
    let leader: u64 = match d.positional(0).and_then(|s| s.parse().ok()) {
      Some(v) => v,
      None => return "propose-conf-change-v2: missing leader id\n".to_string(),
    };
    let transition = match d.value::<String>("transition").as_deref() {
      Some("implicit") => sailing_proto::ConfChangeTransition::Implicit,
      Some("explicit") => sailing_proto::ConfChangeTransition::Explicit,
      _ => sailing_proto::ConfChangeTransition::Auto,
    };
    let mut changes = Vec::new();
    let mut joining = Vec::new();
    for a in &d.args {
      let ty = match a.key.as_str() {
        "add" => sailing_proto::ConfChangeType::AddNode,
        "remove" => sailing_proto::ConfChangeType::RemoveNode,
        "addlearner" => sailing_proto::ConfChangeType::AddLearnerNode,
        _ => continue,
      };
      let node: u64 = match a.vals.first().and_then(|v| v.parse().ok()) {
        Some(v) => v,
        None => continue,
      };
      if matches!(a.key.as_str(), "add" | "addlearner") {
        joining.push(node);
      }
      changes.push(sailing_proto::ConfChangeSingle::new(ty, node));
    }
    // Wire any brand-new joining nodes as observers so they can receive replication.
    let current_voters: Vec<u64> = self
      .nodes
      .get(&leader)
      .map(|n| n.ep.conf_state().voters().iter().copied().collect())
      .unwrap_or_default();
    for node in joining {
      self.nodes.entry(node).or_insert_with(|| {
        let cfg = Config::try_new_observer(
          node,
          current_voters.clone(),
          ELECTION_TIMEOUT,
          HEARTBEAT_INTERVAL,
        )
        .expect("valid observer config");
        Node {
          ep: Endpoint::new(cfg.clone(), Instant::ORIGIN, node, LogSm::new()),
          log: MemLog::new(),
          stable: MemStable::new(),
          config: cfg,
        }
      });
    }
    let n_changes = changes.len();
    let cc = sailing_proto::ConfChangeV2::new(transition, changes, bytes::Bytes::new());
    let mut out = String::new();
    {
      let now = self.now;
      let n = self.nodes.get_mut(&leader).unwrap();
      match n.ep.propose_conf_change_v2(now, &mut n.log, &n.stable, cc) {
        Ok(idx) => {
          let what = if n_changes == 0 {
            "joint-leave".to_string()
          } else {
            std::format!("{n_changes} change(s)")
          };
          out.push_str(&std::format!(
            "n{leader} proposed conf-change-v2 ({what}) at index {}\n",
            idx.get()
          ));
        }
        Err(e) => out.push_str(&std::format!("n{leader} conf-change-v2 rejected: {e:?}\n")),
      }
    }
    self.drain_node(leader, &mut out);
    out
  }

  /// `isolate <id>…` — fully partition the given node(s): every message to or from them is dropped
  /// until `recover`. Use to model a leader losing contact with its followers (check-quorum) or a
  /// minority split.
  fn isolate(&mut self, d: &Directive) -> String {
    let ids: Vec<u64> = (0..)
      .map_while(|i| d.positional(i).and_then(|s| s.parse().ok()))
      .collect();
    if ids.is_empty() {
      return "isolate: missing node id\n".to_string();
    }
    let mut out = String::new();
    for id in ids {
      self.partitioned.insert(id);
      out.push_str(&std::format!("n{id} isolated\n"));
    }
    out
  }

  /// `crash <id>` — model a crash + restart of node `id`: discard its in-flight (submitted-but-not-
  /// yet-durable) store writes, rebuild the `Endpoint` from durable state via [`Endpoint::restart`],
  /// and drop every in-flight message to or from it. The node rejoins from exactly its durable log +
  /// hard state (persistence recovery).
  fn crash(&mut self, d: &Directive) -> String {
    let id: u64 = match d.positional(0).and_then(|s| s.parse().ok()) {
      Some(v) => v,
      None => return "crash: missing node id\n".to_string(),
    };
    let now = self.now;
    if let Some(n) = self.nodes.get_mut(&id) {
      n.log.discard_inflight();
      n.stable.discard_inflight();
      n.ep = Endpoint::restart(
        n.config.clone(),
        now,
        id,
        LogSm::new(),
        &mut n.log,
        &mut n.stable,
      );
    } else {
      return std::format!("n{id}: no such node\n");
    }
    // A crash loses every message in flight to or from the node.
    self.bus.retain(|(from, to, _)| *from != id && *to != id);
    let mut out = std::format!("n{id} crashed and restarted\n");
    self.drain_node(id, &mut out);
    out
  }

  /// `flush <id>…` — make the node(s)' submitted-but-staged store writes durable (the async fsync
  /// completing), releasing the deferred completions, then run their durability/apply step. In sync
  /// mode writes are already durable so this just drains.
  fn flush(&mut self, d: &Directive) -> String {
    let ids: Vec<u64> = (0..)
      .map_while(|i| d.positional(i).and_then(|s| s.parse().ok()))
      .collect();
    if ids.is_empty() {
      return "flush: missing node id\n".to_string();
    }
    let mut out = String::new();
    for id in ids {
      if let Some(n) = self.nodes.get_mut(&id) {
        n.log.flush();
        n.stable.flush();
      }
      let mut node_out = String::new();
      self.drain_node(id, &mut node_out);
      if !node_out.is_empty() {
        out.push_str(&std::format!("> n{id} ready\n"));
        out.push_str(&indent(&node_out));
      }
    }
    if out.is_empty() {
      out.push_str("ok\n");
    }
    out
  }

  /// `recover <id>…` — heal the partition for the given node(s) (re-admit them to the bus).
  fn recover(&mut self, d: &Directive) -> String {
    let ids: Vec<u64> = (0..)
      .map_while(|i| d.positional(i).and_then(|s| s.parse().ok()))
      .collect();
    if ids.is_empty() {
      return "recover: missing node id\n".to_string();
    }
    let mut out = String::new();
    for id in ids {
      self.partitioned.remove(&id);
      out.push_str(&std::format!("n{id} recovered\n"));
    }
    out
  }

  /// `read-index <id> <ctx>` — node `id` requests a linearizable read with opaque context `ctx`. The
  /// leader confirms its leadership (a heartbeat round under the Safe option) and emits a ReadState
  /// event carrying the read's commit index; it surfaces during the following `stabilize`.
  fn read_index(&mut self, d: &Directive) -> String {
    let id: u64 = match d.positional(0).and_then(|s| s.parse().ok()) {
      Some(v) => v,
      None => return "read-index: missing node id\n".to_string(),
    };
    let ctx = bytes::Bytes::from(d.positional(1).unwrap_or("").as_bytes().to_vec());
    let mut out = String::new();
    {
      let now = self.now;
      let n = self.nodes.get_mut(&id).unwrap();
      if let Err(e) = n.ep.read_index(now, &n.log, &n.stable, ctx) {
        out.push_str(&std::format!("n{id} read-index rejected: {e:?}\n"));
      }
    }
    self.drain_node(id, &mut out);
    if out.is_empty() {
      out.push_str("ok\n");
    }
    out
  }

  /// `status <id>` — node `id`'s view of its replication progress to each peer (match / next index,
  /// flow-control state probe|replicate|snapshot, and whether paused). Meaningful while `id` leads.
  fn status(&mut self, d: &Directive) -> String {
    let id: u64 = match d.positional(0).and_then(|s| s.parse().ok()) {
      Some(v) => v,
      None => return "status: missing node id\n".to_string(),
    };
    let n = match self.nodes.get(&id) {
      Some(n) => n,
      None => return std::format!("n{id}: no such node\n"),
    };
    let cs = n.ep.conf_state();
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
      match n.ep.peer_progress(&p) {
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

  /// `conf-state <id>` — node `id`'s committed configuration: its voter and (if any) learner sets.
  fn conf_state_cmd(&mut self, d: &Directive) -> String {
    let id: u64 = match d.positional(0).and_then(|s| s.parse().ok()) {
      Some(v) => v,
      None => return "conf-state: missing node id\n".to_string(),
    };
    match self.nodes.get(&id) {
      Some(n) => {
        let cs = n.ep.conf_state();
        let learners: Vec<u64> = cs.learners().iter().copied().collect();
        if learners.is_empty() {
          std::format!("n{id}: voters={}\n", fmt_conf(&cs))
        } else {
          std::format!(
            "n{id}: voters={} learners={}\n",
            fmt_conf(&cs),
            fmt_set(&learners)
          )
        }
      }
      None => std::format!("n{id}: no such node\n"),
    }
  }

  /// `transfer-leadership from=<a> to=<b>` — node `a` (the current leader) transfers leadership to
  /// `b` (`TimeoutNow` path). Renders the resulting messages.
  fn transfer_leadership(&mut self, d: &Directive) -> String {
    let from: u64 = match d.value("from") {
      Some(v) => v,
      None => return "transfer-leadership: missing from=\n".to_string(),
    };
    let to: u64 = match d.value("to") {
      Some(v) => v,
      None => return "transfer-leadership: missing to=\n".to_string(),
    };
    let mut out = String::new();
    {
      let now = self.now;
      let n = self.nodes.get_mut(&from).unwrap();
      if let Err(e) = n.ep.transfer_leader(now, &n.log, &n.stable, to) {
        out.push_str(&std::format!("n{from} transfer rejected: {e:?}\n"));
      }
    }
    self.drain_node(from, &mut out);
    if out.is_empty() {
      out.push_str("ok\n");
    }
    out
  }

  /// `tick-heartbeat <id>` / `tick-election <id>` — advance node `id`'s virtual clock by one
  /// heartbeat / election interval and fire its timer (a leader broadcasts heartbeats / runs its
  /// check-quorum sweep; a follower or candidate may time out and campaign).
  fn tick(&mut self, d: &Directive, by: Duration) -> String {
    let id: u64 = match d.positional(0).and_then(|s| s.parse().ok()) {
      Some(v) => v,
      None => return "tick: missing node id\n".to_string(),
    };
    {
      let now = self.now;
      let n = self.nodes.get_mut(&id).unwrap();
      let deadline = now + by;
      if deadline > self.now {
        self.now = deadline;
      }
      let now = self.now;
      n.ep.handle_timeout(now, &mut n.log, &mut n.stable);
    }
    let mut out = String::new();
    self.drain_node(id, &mut out);
    if out.is_empty() {
      out.push_str("ok\n");
    }
    out
  }

  /// `stabilize` — run the cluster to quiescence: repeatedly process every node's durability/apply
  /// step, drain its outgoing onto the bus and its events, then deliver every in-flight message,
  /// until nothing new happens. Renders the whole settling sequence.
  fn stabilize(&mut self) -> String {
    let mut out = String::new();
    let mut iters = 0u32;
    loop {
      iters += 1;
      assert!(iters < 10_000, "stabilize livelock");
      let mut progressed = false;

      // Process every node: durability + drain outgoing → bus + drain events.
      let ids: Vec<u64> = self.nodes.keys().copied().collect();
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

      // Deliver every message currently on the bus.
      let pending: Vec<(u64, u64, Message<u64>)> = self.bus.drain(..).collect();
      for (from, to, msg) in pending {
        progressed = true;
        out.push_str(&std::format!(
          "> n{to} recv {}\n",
          render_msg(from, to, &msg)
        ));
        if let Some(n) = self.nodes.get_mut(&to) {
          let now = self.now;
          n.ep
            .handle_message(now, &mut n.log, &mut n.stable, from, msg);
        }
      }

      if !progressed {
        break;
      }
    }
    if out.is_empty() {
      out.push_str("ok\n");
    }
    out
  }

  /// `deliver-msgs [from=..] [to=..]` — deliver the in-flight messages matching the filter (all if
  /// none), stepping each recipient. Renders each delivered message.
  fn deliver_msgs(&mut self, d: &Directive) -> String {
    let from_filter: Option<u64> = d.value("from");
    let to_filter: Option<u64> = d.value("to");
    let mut out = String::new();
    let mut kept = VecDeque::new();
    let pending: Vec<(u64, u64, Message<u64>)> = self.bus.drain(..).collect();
    for (from, to, msg) in pending {
      let matches = from_filter.is_none_or(|f| f == from) && to_filter.is_none_or(|t| t == to);
      if !matches {
        kept.push_back((from, to, msg));
        continue;
      }
      out.push_str(&std::format!(
        "n{from}->n{to} {}\n",
        render_msg(from, to, &msg)
      ));
      if let Some(n) = self.nodes.get_mut(&to) {
        let now = self.now;
        n.ep
          .handle_message(now, &mut n.log, &mut n.stable, from, msg);
      }
    }
    self.bus = kept;
    if out.is_empty() {
      out.push_str("no messages\n");
    }
    out
  }

  /// `process-ready <id>` — run exactly one node's durability/apply step: process storage
  /// completions, drain its outgoing onto the bus, and drain its events. Renders what it produced.
  fn process_ready(&mut self, d: &Directive) -> String {
    let id: u64 = match d.positional(0).and_then(|s| s.parse().ok()) {
      Some(id) => id,
      None => return "process-ready: missing node id\n".to_string(),
    };
    let mut out = String::new();
    self.drain_node(id, &mut out);
    if out.is_empty() {
      out.push_str("ok\n");
    }
    out
  }

  /// `raft-state` — one line per node: its role, term, and believed leader.
  fn raft_state(&mut self) -> String {
    let mut out = String::new();
    for (id, n) in &self.nodes {
      out.push_str(&std::format!(
        "n{id}: {} term={} lead={}\n",
        role_str(n.ep.role()),
        n.ep.term().get(),
        n.ep
          .leader()
          .map(|l| l.to_string())
          .unwrap_or_else(|| "none".to_string()),
      ));
    }
    out
  }

  /// `raft-log <id>` — node `id`'s commit/applied watermarks and its log entries.
  fn raft_log(&mut self, d: &Directive) -> String {
    let id: u64 = match d.positional(0).and_then(|s| s.parse().ok()) {
      Some(id) => id,
      None => return "raft-log: missing node id\n".to_string(),
    };
    let n = match self.nodes.get(&id) {
      Some(n) => n,
      None => return std::format!("n{id}: no such node\n"),
    };
    let mut out = std::format!(
      "n{id}: commit={} applied={} last={}\n",
      n.ep.commit_index().get(),
      n.ep.applied_index().get(),
      n.log_last(),
    );
    use sailing_proto::LogStore;
    let first = n.log.first_index();
    let last = Index::new(n.log_last());
    // Surface compaction: a `first_index` past 1 means everything up to `first-1` is in the snapshot
    // (and gone from the entry list). Only shown when compacted, so uncompacted goldens are unchanged.
    if first.get() > 1 {
      out.push_str(&std::format!(
        "  (snapshot covers <= index {})\n",
        first.get() - 1
      ));
    }
    if last >= first {
      if let Ok(entries) = n.log.entries(first..Index::new(last.get() + 1), u64::MAX) {
        for e in entries {
          out.push_str(&std::format!(
            "  {}/{} {}{}\n",
            e.term().get(),
            e.index().get(),
            kind_str(e.kind()),
            fmt_data(e.data()),
          ));
        }
      }
    }
    out
  }

  /// Process node `id`'s storage completions, then drain its outgoing messages onto the bus and its
  /// events, appending a rendered line for each. Returns whether anything was produced.
  fn drain_node(&mut self, id: u64, out: &mut String) -> bool {
    let now = self.now;
    let mut produced = false;
    let n = match self.nodes.get_mut(&id) {
      Some(n) => n,
      None => return false,
    };
    // Run storage completions to quiescence (a completion can release deferred acks which submit
    // further writes; sync stores complete immediately).
    let mut guard = 0u32;
    loop {
      guard += 1;
      assert!(guard < 10_000, "drain_node storage livelock");
      n.ep.handle_storage(now, &mut n.log, &mut n.stable);
      // Drain outgoing → bus.
      let mut any = false;
      while let Some(o) = n.ep.poll_message() {
        any = true;
        let to = o.to();
        let (_, msg) = sailing_proto::Outgoing::into_parts(o);
        // A full partition drops the message at either endpoint (it never reaches the bus).
        if self.partitioned.contains(&id) || self.partitioned.contains(&to) {
          continue;
        }
        produced = true;
        out.push_str(&std::format!("{}\n", render_msg(id, to, &msg)));
        self.bus.push_back((id, to, msg));
      }
      // Drain events.
      while let Some(ev) = n.ep.poll_event() {
        produced = true;
        out.push_str(&render_event(id, &ev));
      }
      if !any {
        break;
      }
    }
    produced
  }
}

impl Node {
  /// The node's visible log `last_index` as a bare `u64`.
  fn log_last(&self) -> u64 {
    use sailing_proto::LogStore;
    self.log.last_index().get()
  }
}

impl Default for InteractionEnv {
  fn default() -> Self {
    Self::new()
  }
}

/// The harness environment: one `Node` per id, an explicit in-flight message bus, and a shared
/// virtual clock. See the module docs.
pub struct InteractionEnv {
  nodes: BTreeMap<u64, Node>,
  bus: VecDeque<(u64, u64, Message<u64>)>,
  now: Instant,
  /// Fully-partitioned node ids: a message with either endpoint in this set is dropped (the node can
  /// neither send nor receive). `isolate` adds, `recover` removes.
  partitioned: BTreeSet<u64>,
}

// ─────────────────────────── rendering ───────────────────────────

/// Render a wire message as `Kind term=.. <fields>` (the `from->to` prefix is added by the caller).
fn render_msg(_from: u64, _to: u64, msg: &Message<u64>) -> String {
  match msg {
    Message::AppendEntries(m) => std::format!(
      "AppendEntries term={} prev={}/{} commit={} entries=[{}]",
      m.term().get(),
      m.prev_log_term().get(),
      m.prev_log_index().get(),
      m.leader_commit().get(),
      m.entries()
        .iter()
        .map(|e| std::format!(
          "{}/{} {}{}",
          e.term().get(),
          e.index().get(),
          kind_str(e.kind()),
          fmt_data(e.data())
        ))
        .collect::<Vec<_>>()
        .join(", "),
    ),
    Message::AppendResp(m) => {
      if m.reject() {
        std::format!(
          "AppendResp term={} reject hint={}/{}",
          m.term().get(),
          m.reject_hint_term().get(),
          m.reject_hint_index().get(),
        )
      } else {
        std::format!(
          "AppendResp term={} match={}",
          m.term().get(),
          m.match_index().get()
        )
      }
    }
    Message::RequestVote(m) => std::format!(
      "RequestVote term={} last={}/{} prevote={}",
      m.term().get(),
      m.last_log_term().get(),
      m.last_log_index().get(),
      m.pre_vote(),
    ),
    Message::VoteResp(m) => std::format!(
      "VoteResp term={} prevote={} reject={}",
      m.term().get(),
      m.pre_vote(),
      m.reject(),
    ),
    Message::Heartbeat(m) => {
      std::format!(
        "Heartbeat term={} commit={}",
        m.term().get(),
        m.commit().get()
      )
    }
    Message::HeartbeatResp(m) => std::format!("HeartbeatResp term={}", m.term().get()),
    Message::InstallSnapshot(m) => std::format!(
      "InstallSnapshot term={} snap={}/{}",
      m.term().get(),
      m.snapshot().last_term().get(),
      m.snapshot().last_index().get(),
    ),
    Message::SnapshotResp(m) => std::format!(
      "SnapshotResp term={} reject={} match={}",
      m.term().get(),
      m.reject(),
      m.match_index().get(),
    ),
    Message::TimeoutNow(m) => std::format!("TimeoutNow term={}", m.term().get()),
    Message::ReadIndex(m) => std::format!("ReadIndex term={}", m.term().get()),
    Message::ReadIndexResp(m) => {
      std::format!(
        "ReadIndexResp term={} index={}",
        m.term().get(),
        m.index().get()
      )
    }
    _ => "?unknown-message".to_string(),
  }
}

/// Render an [`Event`](sailing_proto::Event) drained from a node's `poll_event`.
fn render_event(id: u64, ev: &sailing_proto::Event<u64, usize>) -> String {
  use sailing_proto::Event;
  match ev {
    Event::Applied(a) => std::format!("n{id} applied index={}\n", a.index().get()),
    Event::LeaderChanged(lc) => std::format!(
      "n{id} leader-changed term={} lead={}\n",
      lc.term().get(),
      lc.leader()
        .map(|l| l.to_string())
        .unwrap_or_else(|| "none".to_string()),
    ),
    Event::SnapshotInstalled(m) => {
      std::format!(
        "n{id} snapshot-installed snap={}/{}\n",
        m.last_term().get(),
        m.last_index().get()
      )
    }
    Event::ConfChanged(cc) => {
      let learners: Vec<u64> = cc.conf().learners().iter().copied().collect();
      if learners.is_empty() {
        std::format!(
          "n{id} conf-changed index={} voters={}\n",
          cc.index().get(),
          fmt_conf(cc.conf())
        )
      } else {
        std::format!(
          "n{id} conf-changed index={} voters={} learners={}\n",
          cc.index().get(),
          fmt_conf(cc.conf()),
          fmt_set(&learners)
        )
      }
    }
    Event::ReadState(rs) => std::format!("n{id} read-state index={}\n", rs.index().get()),
    _ => std::format!("n{id} ?unknown-event\n"),
  }
}

/// Format a `ConfState`'s voter set as `{1,2,3}`.
fn fmt_conf(conf: &sailing_proto::ConfState<u64>) -> String {
  let voters: Vec<u64> = conf.voters().iter().copied().collect();
  let outgoing: Vec<u64> = conf.voters_outgoing().iter().copied().collect();
  if outgoing.is_empty() {
    fmt_set(&voters)
  } else {
    // Joint configuration: the incoming set | the outgoing (old) set still co-deciding.
    std::format!("{}|{}(joint)", fmt_set(&voters), fmt_set(&outgoing))
  }
}

/// Format a slice of ids as `{1,2,3}`.
fn fmt_set(ids: &[u64]) -> String {
  let mut v: Vec<u64> = ids.to_vec();
  v.sort_unstable();
  v.dedup();
  std::format!(
    "{{{}}}",
    v.iter().map(u64::to_string).collect::<Vec<_>>().join(",")
  )
}

/// Format entry payload bytes: empty as ``, printable UTF-8 as `"text"`, else hex.
fn fmt_data(data: &[u8]) -> String {
  if data.is_empty() {
    return String::new();
  }
  match core::str::from_utf8(data) {
    Ok(s) if s.chars().all(|c| !c.is_control()) => std::format!(" \"{s}\""),
    _ => std::format!(
      " 0x{}",
      data
        .iter()
        .map(|b| std::format!("{b:02x}"))
        .collect::<String>()
    ),
  }
}

/// The role as a short stable string.
fn role_str(r: Role) -> &'static str {
  match r {
    Role::Follower => "follower",
    Role::Candidate => "candidate",
    Role::Leader => "leader",
    Role::PreCandidate => "pre-candidate",
  }
}

/// The entry kind as a short stable string.
fn kind_str(k: EntryKind) -> &'static str {
  match k {
    EntryKind::Normal => "normal",
    EntryKind::ConfChange => "conf-change",
    EntryKind::Empty => "empty",
  }
}

/// Indent every non-empty line of `s` by two spaces (for nesting under a `> n{id} ...` header).
fn indent(s: &str) -> String {
  s.lines()
    .map(|l| {
      if l.is_empty() {
        String::new()
      } else {
        std::format!("  {l}")
      }
    })
    .collect::<Vec<_>>()
    .join("\n")
    + "\n"
}

// ─────────────────────── data-driven file runner ───────────────────────

/// One parsed `command / ---- / expected-output` block, with the comment/blank lines that preceded
/// it (preserved verbatim so a rewrite round-trips the file's documentation).
struct Block {
  comments: Vec<String>,
  command: String,
  expected: Vec<String>,
}

/// Parse a data-driven file into its blocks. The format (etcd's `datadriven`, simple variant): any
/// run of `#`-comment / blank lines, then a single-line `command args`, then a `----` line, then the
/// expected-output lines up to the next blank line (or EOF). The harness never emits blank lines
/// inside a block's output, so the simple single-`----` delimiter always suffices.
fn parse_blocks(content: &str) -> Vec<Block> {
  let mut blocks = Vec::new();
  let mut comments = Vec::new();
  let mut lines = content.lines().peekable();
  while let Some(line) = lines.next() {
    let trimmed = line.trim();
    if trimmed.is_empty() {
      // Blank lines are structural separators; the emitter re-inserts exactly one between blocks,
      // so we drop them here rather than carrying them as "comments" (which would double up).
      continue;
    }
    if trimmed.starts_with('#') {
      comments.push(line.to_string());
      continue;
    }
    let command = line.to_string();
    // The next line should be the `----` separator; consume it if present.
    if lines.peek().map(|l| l.trim()) == Some("----") {
      lines.next();
    }
    let mut expected = Vec::new();
    while let Some(l) = lines.peek() {
      if l.trim().is_empty() {
        break;
      }
      expected.push(lines.next().unwrap().to_string());
    }
    blocks.push(Block {
      comments: core::mem::take(&mut comments),
      command,
      expected,
    });
  }
  blocks
}

/// Run a single data-driven interaction file: execute each directive against a fresh
/// [`InteractionEnv`], and either compare the rendered output against the recorded expectation or —
/// when `SAILING_REWRITE` is set in the environment — rewrite the file in place with the freshly
/// rendered output (etcd's `-rewrite`). Panics with a readable diff on any mismatch.
pub fn run_interaction_file(path: &std::path::Path) {
  let content = std::fs::read_to_string(path)
    .unwrap_or_else(|e| panic!("cannot read interaction file {}: {e}", path.display()));
  let blocks = parse_blocks(&content);
  let rewrite = std::env::var_os("SAILING_REWRITE").is_some();

  let mut env = InteractionEnv::new();
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
    std::fs::write(path, rebuilt)
      .unwrap_or_else(|e| panic!("cannot rewrite interaction file {}: {e}", path.display()));
  } else if !failures.is_empty() {
    panic!(
      "{} interaction mismatch(es) in {}:\n\n{}\n\n(run with SAILING_REWRITE=1 to regenerate)",
      failures.len(),
      path.display(),
      failures.join("\n\n")
    );
  }
}
