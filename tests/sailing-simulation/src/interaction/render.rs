use super::*;

// ─────────────────────────── rendering ───────────────────────────

/// Render a wire message as `Kind term=.. <fields>` (the `from->to` prefix is added by the caller).
pub(crate) fn render_msg(_from: u64, _to: u64, msg: &Message<u64>) -> String {
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
pub(crate) fn render_event(id: u64, ev: &sailing_proto::Event<u64, usize>) -> String {
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
pub(crate) fn fmt_conf(conf: &sailing_proto::ConfState<u64>) -> String {
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
pub(crate) fn fmt_set(ids: &[u64]) -> String {
  let mut v: Vec<u64> = ids.to_vec();
  v.sort_unstable();
  v.dedup();
  std::format!(
    "{{{}}}",
    v.iter().map(u64::to_string).collect::<Vec<_>>().join(",")
  )
}

/// Format entry payload bytes: empty as ``, printable UTF-8 as `"text"`, else hex.
pub(crate) fn fmt_data(data: &[u8]) -> String {
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
pub(crate) fn role_str(r: Role) -> &'static str {
  match r {
    Role::Follower => "follower",
    Role::Candidate => "candidate",
    Role::Leader => "leader",
    Role::PreCandidate => "pre-candidate",
  }
}

/// The entry kind as a short stable string.
pub(crate) fn kind_str(k: EntryKind) -> &'static str {
  match k {
    EntryKind::Normal => "normal",
    EntryKind::ConfChange => "conf-change",
    EntryKind::Empty => "empty",
  }
}

/// Indent every non-empty line of `s` by two spaces (for nesting under a `> n{id} ...` header).
pub(crate) fn indent(s: &str) -> String {
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
