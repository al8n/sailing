use super::*;

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
  pub(crate) fn exec(&mut self, d: &Directive) -> String {
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
  pub(crate) fn add_nodes(&mut self, d: &Directive) -> String {
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
          boot_epoch: 0,
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
  pub(crate) fn campaign(&mut self, d: &Directive) -> String {
    let id: u64 = match d.positional(0).and_then(|s| s.parse().ok()) {
      Some(id) => id,
      None => return "campaign: missing node id\n".to_string(),
    };
    let (before_role, before_term) = {
      let n = &self.nodes[&id];
      (n.ep.role(), n.ep.term())
    };
    // Advance the clock to this node's election deadline and fire its timer.
    if let Some(deadline) = self.nodes.get(&id).and_then(|n| n.ep.poll_timeout())
      && deadline > self.now
    {
      self.now = deadline;
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
  pub(crate) fn propose(&mut self, d: &Directive) -> String {
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
  pub(crate) fn propose_conf_change(&mut self, d: &Directive) -> String {
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
          boot_epoch: 0,
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
  pub(crate) fn propose_conf_change_v2(&mut self, d: &Directive) -> String {
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
          boot_epoch: 0,
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
  pub(crate) fn isolate(&mut self, d: &Directive) -> String {
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
  pub(crate) fn crash(&mut self, d: &Directive) -> String {
    let id: u64 = match d.positional(0).and_then(|s| s.parse().ok()) {
      Some(v) => v,
      None => return "crash: missing node id\n".to_string(),
    };
    let now = self.now;
    if let Some(n) = self.nodes.get_mut(&id) {
      n.log.discard_inflight();
      n.stable.discard_inflight();
      // Bump the durable boot epoch so this incarnation's forwarded-read tokens are unique vs. any
      // pre-crash ones (a pre-crash ReadIndexResp cannot complete a post-restart read).
      n.boot_epoch += 1;
      n.ep = Endpoint::restart(
        n.config.clone(),
        now,
        id,
        LogSm::new(),
        n.boot_epoch,
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
  pub(crate) fn flush(&mut self, d: &Directive) -> String {
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
  pub(crate) fn recover(&mut self, d: &Directive) -> String {
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
  pub(crate) fn read_index(&mut self, d: &Directive) -> String {
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
  pub(crate) fn status(&mut self, d: &Directive) -> String {
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
  pub(crate) fn conf_state_cmd(&mut self, d: &Directive) -> String {
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
  pub(crate) fn transfer_leadership(&mut self, d: &Directive) -> String {
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
  pub(crate) fn tick(&mut self, d: &Directive, by: Duration) -> String {
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
  pub(crate) fn stabilize(&mut self) -> String {
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
  pub(crate) fn deliver_msgs(&mut self, d: &Directive) -> String {
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
  pub(crate) fn process_ready(&mut self, d: &Directive) -> String {
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
  pub(crate) fn raft_state(&mut self) -> String {
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
  pub(crate) fn raft_log(&mut self, d: &Directive) -> String {
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
    if last >= first
      && let Ok(entries) = n.log.entries(first..Index::new(last.get() + 1), u64::MAX)
    {
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
    out
  }

  /// Process node `id`'s storage completions, then drain its outgoing messages onto the bus and its
  /// events, appending a rendered line for each. Returns whether anything was produced.
  pub(crate) fn drain_node(&mut self, id: u64, out: &mut String) -> bool {
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
