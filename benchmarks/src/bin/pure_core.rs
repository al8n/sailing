//! Pure consensus-core throughput benchmark.
//!
//! Drives an N-node sailing-proto cluster directly — no async runtime, no wire serialization, no
//! real I/O — so the measured put/s isolates the Sans-I/O consensus logic itself (append ->
//! replicate -> commit -> apply). A proposal triggers its AppendEntries immediately (the leader
//! fans out on `propose`, not only on a heartbeat tick), so after electing a leader the steady-state
//! loop FREEZES virtual time: no heartbeat/election timers fire, and throughput is bounded purely by
//! how fast the cluster can exchange messages and apply entries.
//!
//! `--batch` is the in-flight depth: `1` is latency-bound (one proposal outstanding at a time); a
//! larger value appends several proposals before draining, the pipelined throughput regime.
//!
//! Contrast with the `parity` bench, which runs the async framework (per-node tasks) to compare
//! against openraft's harness; this one strips all of that to expose the core's raw cost.

use std::{
  collections::BTreeMap,
  time::{Duration, Instant as WallInstant},
};

use bytes::Bytes;
use clap::Parser;
use sailing_benchmark::CountSm;
use sailing_proto::{Config, Endpoint, Event, Instant, Message, Outgoing};
use sailing_simulation::{MemLog, MemStable};

#[derive(Parser, Debug)]
#[command(
  about = "Pure Sans-I/O consensus-core throughput (synchronous, no I/O, no serialization)"
)]
struct Args {
  /// Total number of proposals to commit.
  #[arg(short = 'n', long, default_value_t = 100_000)]
  operations: u64,
  /// Cluster size (1, 3, or 5).
  #[arg(short = 'm', long, default_value_t = 3)]
  members: usize,
  /// In-flight proposal depth: 1 = latency, larger = pipelined throughput.
  #[arg(short = 'b', long, default_value_t = 1)]
  batch: u64,
}

type Node = Endpoint<u64, CountSm>;

fn main() {
  let args = Args::parse();
  assert!(args.batch >= 1, "--batch must be >= 1");
  assert!(args.members >= 1, "--members must be >= 1");
  let n = args.members;

  // Build the cluster: ids 0..n, election timeout 1s, heartbeat 100ms (the simulation's defaults).
  let voters: Vec<u64> = (0..n as u64).collect();
  let ids: Vec<u64> = voters.clone();
  let idx: BTreeMap<u64, usize> = ids.iter().enumerate().map(|(i, id)| (*id, i)).collect();
  let mut nodes: Vec<Node> = Vec::with_capacity(n);
  let mut logs: Vec<MemLog> = Vec::with_capacity(n);
  let mut stables: Vec<MemStable<u64>> = Vec::with_capacity(n);
  for id in 0..n as u64 {
    let cfg = Config::try_new(
      id,
      voters.clone(),
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .expect("valid config");
    // Compaction stays on at the default `snapshot_threshold`: the log holds ~one threshold of
    // entries in steady state, so this measures bounded consensus work. `CountSm`'s O(1) snapshot
    // keeps that compaction cheap (the simulation `LogSm`'s O(n) snapshot is the artifact it avoids).
    nodes.push(Endpoint::new(cfg, Instant::ORIGIN, id, CountSm::new()));
    logs.push(MemLog::new());
    stables.push(MemStable::new());
  }

  // --- Elect a leader: advance virtual time to fire election timers, deliver until one node leads.
  let mut now = Instant::ORIGIN;
  let mut leader_i = None;
  let mut unused = 0u64;
  for _ in 0..100_000 {
    if let Some(next) = (0..n).filter_map(|i| nodes[i].poll_timeout()).min() {
      now = now.max(next);
    }
    for i in 0..n {
      if nodes[i].poll_timeout().is_some_and(|d| d <= now) {
        nodes[i].handle_timeout(now, &mut logs[i], &mut stables[i]);
      }
    }
    drain(
      &mut nodes,
      &mut logs,
      &mut stables,
      &ids,
      &idx,
      now,
      usize::MAX,
      &mut unused,
    );
    if let Some(i) = (0..n).find(|&i| nodes[i].role().is_leader()) {
      leader_i = Some(i);
      break;
    }
  }
  let leader_i = leader_i.expect("no leader elected within 100000 election rounds");
  // Settle the leader's initial no-op (and any single-node self-commit) before timing.
  drain(
    &mut nodes,
    &mut logs,
    &mut stables,
    &ids,
    &idx,
    now,
    leader_i,
    &mut unused,
  );

  // --- Steady state: virtual time frozen, so no timer fires; throughput = pure message exchange.
  let start = WallInstant::now();
  let mut leader_applied = 0u64;
  let mut proposed = 0u64;
  while leader_applied < args.operations {
    let want = args.batch.min(args.operations - proposed);
    for _ in 0..want {
      proposed += 1;
      let cmd = Bytes::copy_from_slice(&proposed.to_le_bytes());
      let _ = nodes[leader_i].propose(now, &mut logs[leader_i], &stables[leader_i], &cmd);
    }
    drain(
      &mut nodes,
      &mut logs,
      &mut stables,
      &ids,
      &idx,
      now,
      leader_i,
      &mut leader_applied,
    );
  }
  let elapsed = start.elapsed();

  let put_s = args.operations as f64 / elapsed.as_secs_f64();
  println!(
    "pure_core  members={} batch={} ops={} elapsed={:.3}s  put/s={:.0}",
    args.members,
    args.batch,
    args.operations,
    elapsed.as_secs_f64(),
    put_s,
  );
}

/// Exchange messages to quiescence: drain every node's outgoing queue and deliver each message,
/// repeating until no node has anything more to send. Drains events too, counting the leader's
/// `Applied` events into `leader_applied` (each is one committed+applied proposal; the no-op `Empty`
/// entry emits no `Applied`, so the count is exactly the committed user proposals).
#[allow(clippy::too_many_arguments)]
fn drain(
  nodes: &mut [Node],
  logs: &mut [MemLog],
  stables: &mut [MemStable<u64>],
  ids: &[u64],
  idx: &BTreeMap<u64, usize>,
  now: Instant,
  leader_i: usize,
  leader_applied: &mut u64,
) {
  let mut guard = 0u64;
  loop {
    guard += 1;
    assert!(
      guard < 50_000_000,
      "drain did not quiesce — consensus stalled"
    );
    let mut progress = false;

    // Process storage completions (persist-before-ack / persist-before-vote): a committed append or a
    // persisted vote only takes effect once its completion is drained here, which may in turn emit acks
    // or advance commit + apply.
    for i in 0..nodes.len() {
      if nodes[i]
        .handle_storage(now, &mut logs[i], &mut stables[i])
        .is_more_pending()
      {
        progress = true;
      }
    }

    let mut outbox: Vec<(u64, u64, Message<u64>)> = Vec::new();
    for i in 0..nodes.len() {
      while let Some(out) = nodes[i].poll_message() {
        let (to, message) = Outgoing::into_parts(out);
        outbox.push((ids[i], to, message));
      }
      while let Some(event) = nodes[i].poll_event() {
        if i == leader_i && matches!(event, Event::Applied(_)) {
          *leader_applied += 1;
        }
      }
    }
    if !outbox.is_empty() {
      progress = true;
    }
    for (from, to, message) in outbox {
      if let Some(&j) = idx.get(&to) {
        nodes[j].handle_message(now, &mut logs[j], &mut stables[j], from, message);
      }
    }

    if !progress {
      break;
    }
  }
}
