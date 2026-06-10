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
  /// Monotonic boot epoch, incremented per crash/restart — the durable boot counter the harness feeds
  /// to [`Endpoint::restart`] so forwarded-read tokens are unique across restarts.
  boot_epoch: u64,
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

// Split by concern; re-export the free helpers for the root and siblings.
mod exec;
mod parse;
mod render;
use parse::*;
use render::*;
