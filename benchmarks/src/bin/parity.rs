//! Parity cluster-throughput benchmark — mirrors openraft's `benchmarks/minimal` method so the
//! two numbers are comparable apples-to-apples.
//!
//! Like openraft's harness it runs the *whole* framework, just without real I/O: N concurrent node
//! tasks form the cluster, the "network" is a per-node typed-[`Message`] channel (no serialization,
//! no sockets — the same shortcut openraft takes by calling the peer's `Raft` handle directly), the
//! log and state are in-memory, and the state machine merely counts applied commands. N client
//! tasks each propose to the leader and await the commit+apply of their own write; throughput is the
//! committed put/s over the load window.
//!
//! The measurement assumes a single stable leader for the whole timed window (the no-fault
//! in-process case): clients target one leader captured before timing starts, and a leadership
//! change during the window ABORTS the run loudly rather than silently miscounting it (an election
//! stall inflates elapsed, and an already-accepted entry could otherwise be committed twice).
//!
//! Because sailing-proto is Sans-I/O, a node is NOT self-driving: each node task owns its
//! `Endpoint` + stores and hand-turns the crank — feed an inbound message (or fire a due timer),
//! then pump storage to quiescence (persist-before-ack/-vote), route the produced messages to peer
//! channels, and drain the applied events. This is the async-task analogue of `pure_core`'s
//! synchronous global drain; the difference is only that delivery now hops through channels and
//! tasks instead of an in-loop function call.
//!
//! Contrast with the `pure_core` bench, which strips the async framework entirely to expose the
//! consensus core's raw single-threaded cost.
//!
//! # Multi-group sharding (`-g`)
//!
//! sailing's consensus is serial per Raft *group* (Sans-I/O — one group is roughly one core of
//! consensus work), so it scales throughput the way TiKV/CockroachDB do: by *sharding* into many
//! independent Raft groups, not by parallelizing one group. `-g K` runs K fully-independent groups —
//! each with its own node set, channels, leader, and integrity guards — concurrently on the same
//! runtime, over one shared timed window. `-c` is the per-group client count; `-n` is the TOTAL op
//! budget across all groups (distributed exactly, so the aggregate committed count is exactly `-n`).
//! The reported put/s is the AGGREGATE:
//! every group's committed ops over the single wall clock. It scales ~linearly with K until the
//! runtime's worker threads saturate the cores — give each group roughly one core, so raise `-w` as
//! you raise `-g`. `-g 1` (the default) is exactly the single-group benchmark above.

use std::{
  collections::HashMap,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
  },
  time::{Duration, Instant as WallInstant},
};

use bytes::Bytes;
use clap::Parser;
use sailing_benchmark::CountSm;
use sailing_proto::{Config, Endpoint, Event, Index, Instant, Message, Outgoing};
use sailing_simulation::{MemLog, MemStable};
use tokio::{
  sync::{mpsc, oneshot},
  task::JoinSet,
};

type Node = Endpoint<u64, CountSm>;

/// `current_leader` sentinel: no leader currently known.
const NO_LEADER: u64 = u64::MAX;

/// Upper bound on inbound messages a node task handles before it pumps + yields, so the periodic
/// timer check and the cooperative scheduler are never starved under sustained load while the pump's
/// fixed per-batch cost (one clock read, one quiescence sweep) is still amortized across many ops.
const DRAIN_BUDGET: usize = 1024;

/// How often a node re-checks its consensus timers. Small relative to the 100ms heartbeat and 1s
/// election timeout, so heartbeats/elections fire close to their deadline; the check is also run on
/// every inbound wake, so under load the timer is effectively never late.
const TIMER_TICK: Duration = Duration::from_millis(20);

/// Aborting a run whose timed window saw a leadership change is the only correct response: a
/// throughput number is only meaningful under a single stable leader, and a change mid-window means
/// an election stall (inflated elapsed) plus entries that may commit under both the old and new
/// leader (a miscount). A no-fault in-process cluster never trips this.
const LEADER_CHANGED_MSG: &str =
  "leader changed during the timed benchmark window — run invalid, re-run under a stable leader";

/// Aborting when a node task ends mid-window is the liveness half of the same contract: the cluster
/// must keep all `members` nodes alive for the whole measurement. If one dies, the leader silently
/// commits on the surviving quorum and the harness would report a degraded run under the full
/// `members` label — a bogus N-node number. A no-fault in-process cluster never trips this.
const NODE_DIED_MSG: &str = "a node task exited during the timed benchmark window — run invalid";

#[derive(Parser, Debug)]
#[command(
  about = "Async cluster-throughput benchmark matching openraft's method (typed-message channels, no I/O)"
)]
struct Args {
  /// Number of concurrent client tasks proposing writes, per group.
  #[arg(short = 'c', long, default_value_t = 4096, value_parser = parse_count)]
  clients: u64,
  /// Total number of operations across all clients — and, with `-g`, across all groups. Distributed
  /// exactly: the remainder is spread across the first groups (and, within a group, across its
  /// clients), so the aggregate committed count equals this value exactly, never rounded down.
  #[arg(short = 'n', long, default_value_t = 20_000_000, value_parser = parse_count)]
  operations: u64,
  /// Cluster size per group (1, 3, or 5).
  #[arg(short = 'm', long, default_value_t = 3)]
  members: u64,
  /// Number of independent Raft groups (shards) driven concurrently. sailing's consensus is serial
  /// per group, so aggregate throughput scales by sharding into many groups (the TiKV/CockroachDB
  /// pattern), not by parallelizing one. `-n` is the TOTAL op budget across all groups (distributed
  /// exactly across them); the reported put/s is the aggregate over one shared timed window. `1`
  /// (default) is the single-group benchmark — give each group ~one core, so raise `-w` as you raise
  /// `-g`.
  #[arg(short = 'g', long, default_value_t = 1)]
  groups: u64,
  /// Batch size for writes: 1 = single writes, >1 = pipeline `batch` proposals before awaiting.
  #[arg(short = 'b', long, default_value_t = 1, value_parser = parse_count)]
  batch: u64,
  /// Number of tokio worker threads. At `-g 1` this harness drives each node from one serial task
  /// loop, so extra workers buy no parallelism — they only add cross-thread futex wakeups and
  /// work-stealing migration, so a small fixed count keeps the measurement about consensus
  /// throughput rather than scheduler churn (`-w 2` is typically fastest at `-g 1`). With `-g K` the
  /// K groups run in parallel, so raise `-w` toward one core per group (e.g. `-w 8`). openraft's
  /// harness uses ~16 worker threads — sweep `-w` to compare. Default 4.
  #[arg(short = 'w', long, default_value_t = 4)]
  workers: usize,
}

/// Parse a `u64` with optional `_` separators and a decimal unit suffix (`k`/`m`/`g`), matching
/// openraft's argument parser so the same `-n 20m` style invocations work here.
fn parse_count(s: &str) -> Result<u64, String> {
  let s = s.replace('_', "");
  let (digits, mult) = match s.chars().last() {
    Some('k' | 'K') => (&s[..s.len() - 1], 1_000u64),
    Some('m' | 'M') => (&s[..s.len() - 1], 1_000_000u64),
    Some('g' | 'G') => (&s[..s.len() - 1], 1_000_000_000u64),
    _ => (s.as_str(), 1u64),
  };
  let base: u64 = digits.parse().map_err(|e| format!("{e}"))?;
  base
    .checked_mul(mult)
    .ok_or_else(|| "value overflows u64".to_string())
}

/// What arrives on a node's single inbound channel: a peer's consensus message, or a client's
/// request to propose (the leader records `reply` against the assigned index and fires it on apply).
///
/// The `Message` is carried inline rather than boxed: this is the hot path, and a benchmark must not
/// add a per-message heap allocation that openraft's by-value direct call doesn't have.
#[allow(clippy::large_enum_variant)]
enum Inbound {
  Peer { from: u64, message: Message<u64> },
  Client { reply: oneshot::Sender<()> },
}

/// Why a node task woke: an inbound channel event (`None` = all senders dropped → shut down), or the
/// periodic timer tick. Never queued — it lives only across one `select!`, so its size is irrelevant.
#[allow(clippy::large_enum_variant)]
enum Wake {
  Msg(Option<Inbound>),
  Tick,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
  let args = Args::parse();
  assert!(args.workers >= 1, "--workers must be >= 1");
  eprintln!(
    "parity config: groups={} clients={} operations={} members={} batch={} workers={}",
    args.groups, args.clients, args.operations, args.members, args.batch, args.workers
  );
  // Pin the runtime to a fixed worker count rather than `#[tokio::main]`'s default (one worker per
  // CPU). At `-g 1` this workload's per-node loop is serial, so the extra default workers add
  // cross-thread futex wakeups and work-stealing churn without buying parallelism; a small fixed
  // count keeps the number a read on consensus throughput. With `-g K` the groups run in parallel,
  // so raise `-w` toward one core per group. `-w` lets the operator sweep it either way.
  let rt = tokio::runtime::Builder::new_multi_thread()
    .worker_threads(args.workers)
    .enable_all()
    .build()?;
  rt.block_on(run(args));
  Ok(())
}

async fn run(args: Args) {
  assert!(args.members >= 1, "--members must be >= 1");
  assert!(args.batch >= 1, "--batch must be >= 1");
  assert!(args.clients >= 1, "--clients must be >= 1");
  assert!(args.groups >= 1, "--groups must be >= 1");

  // `-n` is the total op budget across every group, distributed EXACTLY: each group commits `base`
  // ops, and the first `rem` groups commit one extra, so the per-group shares sum to `args.operations`
  // with no rounding loss (the aggregate committed count is exactly `-n`).
  let groups = args.groups;
  assert!(
    args.operations >= groups,
    "operations ({}) too small for groups ({groups}) — each group needs at least one op",
    args.operations,
  );
  let base = args.operations / groups;
  let rem = args.operations % groups;

  // Phase 1 — build and elect every group concurrently, OUTSIDE the timed window. Each group is a
  // fully independent cluster (its own monotonic origin, node set, channels, published-leader cell,
  // and timing flag — nothing is shared across groups); its `setup` task returns once that group
  // holds a single stable leader. Awaiting all of them is the barrier: the timed window below starts
  // only after every group is quiesced on a stable leader, so election churn is startup cost, never
  // throughput. A panic in any setup (e.g. a group that never elects) is FATAL to the whole run.
  let mut setups: JoinSet<GroupReady> = JoinSet::new();
  for group in 0..groups {
    let ops_for_group = base + if group < rem { 1 } else { 0 };
    setups.spawn(setup_group(
      group,
      args.members,
      args.clients,
      args.batch,
      ops_for_group,
    ));
  }
  let mut ready: Vec<GroupReady> = Vec::with_capacity(groups as usize);
  while let Some(res) = setups.join_next().await {
    ready.push(res.expect("a group's setup task panicked — run invalid"));
  }
  // The per-group exact shares sum to `-n` by construction; assert it as a guard against a
  // distribution bug before the timed window relies on it.
  let aggregate_total: u64 = ready.iter().map(|g| g.group_total).sum();
  assert_eq!(
    aggregate_total, args.operations,
    "internal: per-group shares sum to {aggregate_total}, expected -n = {} — distribution bug",
    args.operations,
  );

  // Phase 2 — one shared timed window across ALL groups. Arm every group's timing flag, start a
  // single wall clock, then drive every group's client load concurrently while CENTRALLY policing
  // every group's integrity for the WHOLE window. A group's clients finishing only records that
  // group's committed count: the group's nodes and ALL its guards (stable-leader watcher, node-death
  // monitor, timing flag) stay armed and its nodes keep running until the single global elapsed
  // timestamp is taken — so no finished group goes unguarded, and no group's consensus load vanishes
  // early to inflate the tail. Only after the aggregate window closes do we disarm timing and tear the
  // groups down. Because the K groups run in parallel (one core each, runtime permitting), the
  // aggregate put/s — every group's committed ops over this one window — scales with K.
  for g in &ready {
    g.timing_active.store(true, Ordering::Release);
  }
  let start = WallInstant::now();

  // Split each ready group into its client-load task (which records the group's committed count) and
  // its live guard (node tasks + leader cell + timing flag), kept here so the central window loop can
  // police it for the entire shared window — including after its own clients have finished.
  let mut loads: JoinSet<u64> = JoinSet::new();
  let mut guards: Vec<Guard> = Vec::with_capacity(groups as usize);
  for g in ready {
    let GroupReady {
      group,
      senders,
      leader0,
      nodes,
      current_leader,
      timing_active,
      batch,
      client_ops,
      group_total,
    } = g;
    loads.spawn(run_group_load(
      group,
      senders,
      leader0,
      batch,
      client_ops,
      group_total,
    ));
    guards.push(Guard {
      group,
      leader0,
      nodes,
      current_leader,
      timing_active,
    });
  }

  // Drive the window: drain the per-group load tasks as they finish, and on every idle tick sweep
  // EVERY group's guards. The per-group load tasks stay pending until a whole group's clients finish,
  // so in steady state the loop sweeps roughly every 2ms; a leadership change away from any group's
  // captured leader, or any group's node task ending, is fatal and aborts the whole run. A load-task
  // `JoinError` (panic / cancel) is fatal too — a dead load must never be scored as success.
  let mut sweep = tokio::time::interval(Duration::from_millis(2));
  sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
  let mut observed = 0u64;
  let mut remaining = guards.len();
  while remaining > 0 {
    tokio::select! {
      biased;
      load = loads.join_next() => match load {
        Some(res) => {
          observed += res.expect("a group's client load task panicked or was cancelled — run invalid");
          remaining -= 1;
        }
        None => break,
      },
      _ = sweep.tick() => {
        for guard in &mut guards {
          check_group_integrity(guard);
        }
      }
    }
  }
  let elapsed = start.elapsed();

  // Close the boundary race: a node may have died or leadership moved between the last load completing
  // and the window closing, so sweep every group's guards once more before the number is reported.
  for guard in &mut guards {
    check_group_integrity(guard);
  }

  // The window is closed. Disarm every group's timing flag FIRST — so a peer-channel send that loses
  // the teardown race counts as benign shutdown noise, not a fatal anomaly — then abort all nodes.
  for guard in &mut guards {
    guard.timing_active.store(false, Ordering::Release);
  }
  for guard in &mut guards {
    guard.nodes.abort_all();
  }

  // Each group committed exactly its own share, and the per-group shares sum to `-n` with no rounding
  // loss, so the aggregate equals the user's `-n` exactly on a valid run. Assert against `-n` directly.
  assert_eq!(
    observed, args.operations,
    "groups committed {observed} ops, expected -n = {} — run invalid",
    args.operations,
  );

  // Aggregate throughput: every group's committed ops over the single shared wall clock.
  let put_s = observed as f64 / elapsed.as_secs_f64();
  let millis = elapsed.as_millis().max(1);
  let per_group = put_s / groups as f64;
  println!(
    "parity  groups={} members={} clients={} batch={} ops={} elapsed={:.3}s  put/s={:.0}  \
     op/ms={}  per-group put/s={:.0}",
    groups,
    args.members,
    args.clients,
    args.batch,
    observed,
    elapsed.as_secs_f64(),
    put_s,
    (observed as u128) / millis,
    per_group,
  );
}

/// A group built and quiesced on a single stable leader, ready to join the timed window. Carries the
/// client-load inputs plus the live guard (node tasks + leader cell + timing flag) that the shared
/// window polices for the whole measurement.
struct GroupReady {
  /// This group's index, for diagnostics in abort messages.
  group: u64,
  /// Per-node inbound senders, indexed by node id — this group's private message "network".
  senders: Arc<Vec<mpsc::UnboundedSender<Inbound>>>,
  /// The single leader elected and confirmed stable in phase 1; every client targets it.
  leader0: u64,
  /// The group's live node tasks; in a healthy window none ever ends, so any completion is a death.
  nodes: JoinSet<()>,
  /// The group's published-leader cell, watched during the window for a leadership change.
  current_leader: Arc<AtomicU64>,
  /// Armed for the duration of the window so node tasks treat a dead-peer send as a fatal anomaly.
  timing_active: Arc<AtomicBool>,
  /// Per-write batch (pipeline depth) for this group's clients.
  batch: u64,
  /// Exact per-client op shares (sum == `group_total`); one client task is spawned per entry.
  client_ops: Vec<u64>,
  /// Total ops this group commits on a valid run (`client_ops.iter().sum()`).
  group_total: u64,
}

/// A live group's integrity state, policed centrally for the WHOLE shared window — including after the
/// group's own clients have finished. A leadership change away from `leader0`, or any node task ending
/// (a death), is fatal; the timing flag is disarmed at teardown, before the nodes are aborted.
struct Guard {
  /// This group's index, for diagnostics in abort messages.
  group: u64,
  /// The captured stable leader; any change away from it during the window invalidates the run.
  leader0: u64,
  /// The group's live node tasks; any completion during the window is a node death.
  nodes: JoinSet<()>,
  /// The group's published-leader cell, watched for a leadership change.
  current_leader: Arc<AtomicU64>,
  /// Disarmed once the window closes, before the nodes are aborted.
  timing_active: Arc<AtomicBool>,
}

/// Build one independent group's cluster, spawn its node tasks, and elect + stabilize a single leader
/// — all OUTSIDE any timed window. Returns once the group holds one stable leader, handing the live
/// node `JoinSet` and routing to the caller for the timed phase. Nothing here is shared across groups.
async fn setup_group(
  group: u64,
  members: u64,
  clients: u64,
  batch: u64,
  ops_per_group: u64,
) -> GroupReady {
  // A monotonic origin private to this group, so `now = ORIGIN + origin.elapsed()` advances with real
  // time. This cluster runs no LeaseGuard failover, so the synchronized wall is absent.
  let origin = WallInstant::now();
  let current_leader = Arc::new(AtomicU64::new(NO_LEADER));

  // One unbounded inbound channel per node. Unbounded is deliberate: with a fully-connected message
  // graph, bounded per-node queues can deadlock (every node blocked sending into a full peer queue
  // while its own queue fills). The offered load is bounded by client concurrency, so queues stay
  // bounded in practice.
  let mut receivers = Vec::with_capacity(members as usize);
  let mut senders_vec = Vec::with_capacity(members as usize);
  for _ in 0..members {
    let (tx, rx) = mpsc::unbounded_channel::<Inbound>();
    senders_vec.push(tx);
    receivers.push(rx);
  }
  let senders = Arc::new(senders_vec);

  // True only for the duration of the timed window. The node tasks consult it to decide whether a
  // peer-channel send failure (a dead peer) is a fatal anomaly or just shutdown noise.
  let timing_active = Arc::new(AtomicBool::new(false));

  // Node tasks live in a JoinSet so the timed window can monitor them: in a healthy run they never
  // finish (they loop until aborted after timing), so any completion during the window is a death.
  let mut nodes: JoinSet<()> = JoinSet::new();
  for (id, rx) in receivers.into_iter().enumerate() {
    let senders = senders.clone();
    let current_leader = current_leader.clone();
    let timing_active = timing_active.clone();
    nodes.spawn(run_node(
      id as u64,
      members,
      origin,
      rx,
      senders,
      current_leader,
      timing_active,
    ));
  }

  // Elect AND stabilize a leader before timing: a throughput number is only meaningful under a single
  // stable leader, so we wait for one and confirm it holds across a few heartbeat cycles before this
  // group joins the timed window. Any election churn here is startup cost, not throughput — handling
  // it now (rather than mid-measurement) keeps the timed window clean.
  let elect_deadline = WallInstant::now() + Duration::from_secs(30);
  let leader0 = loop {
    assert!(
      WallInstant::now() < elect_deadline,
      "group {group}: no stable leader elected within 30s"
    );
    let candidate = current_leader.load(Ordering::Acquire);
    if candidate == NO_LEADER {
      tokio::time::sleep(Duration::from_millis(2)).await;
      continue;
    }
    let confirm_until = WallInstant::now() + Duration::from_millis(150);
    let mut held = true;
    while WallInstant::now() < confirm_until {
      tokio::time::sleep(Duration::from_millis(5)).await;
      if current_leader.load(Ordering::Acquire) != candidate {
        held = false;
        break;
      }
    }
    if held {
      break candidate;
    }
  };

  // Distribute this group's exact op budget across its clients: each commits `q` or `q + 1`, with the
  // first `r` clients taking the extra, so the per-client shares sum to `ops_per_group` EXACTLY (no
  // rounding loss — the aggregate over groups is then exactly `-n`). `batch` is only the client's
  // pipeline depth: it fires up to `batch` proposals before awaiting them, with a partial final batch
  // when a share isn't a whole multiple of `batch`, so exactness costs at most a shorter last pipeline.
  let q = ops_per_group / clients;
  let r = ops_per_group % clients;
  let client_ops: Vec<u64> = (0..clients)
    .map(|i| if i < r { q + 1 } else { q })
    .collect();
  let group_total = ops_per_group;
  assert!(
    group_total > 0,
    "group {group}: ops/group ({ops_per_group}) must be > 0"
  );

  GroupReady {
    group,
    senders,
    leader0,
    nodes,
    current_leader,
    timing_active,
    batch,
    client_ops,
    group_total,
  }
}

/// Drive one group's timed client load: spawn one client task per per-client share, await them all,
/// and return the group's committed count (== `group_total` on a valid run). Each client returns only
/// after committing its full share — on any anomaly (leader gone / proposal abandoned) it parks
/// forever, so a short count never returns; the central window loop's guard sweep is what observes the
/// anomaly and aborts the whole run. This function deliberately neither watches for anomalies nor
/// tears the group down: the group's nodes and guards stay armed for the entire shared window.
async fn run_group_load(
  group: u64,
  senders: Arc<Vec<mpsc::UnboundedSender<Inbound>>>,
  leader0: u64,
  batch: u64,
  client_ops: Vec<u64>,
  group_total: u64,
) -> u64 {
  let mut client_handles = Vec::with_capacity(client_ops.len());
  for ops_for_client in client_ops {
    let senders = senders.clone();
    // Each client returns the number of writes it actually committed. It only ever returns after
    // committing its full share (it parks on any anomaly), so the returned count is that share on
    // success — and the group's throughput is the SUM of these, never a configured guess.
    client_handles.push(tokio::spawn(async move {
      let sender = &senders[leader0 as usize];
      let mut done = 0u64;
      while done < ops_for_client {
        let want = batch.min(ops_for_client - done);
        let mut rxs = Vec::with_capacity(want as usize);
        for _ in 0..want {
          let (tx, rx) = oneshot::channel();
          if sender.send(Inbound::Client { reply: tx }).is_err() {
            // The cluster stopped accepting (shutdown / leader gone). Never re-target another leader
            // (that would double-count): park, so the window's guard sweep aborts the run.
            std::future::pending::<()>().await;
          }
          rxs.push(rx);
        }
        for rx in rxs {
          if rx.await.is_err() {
            // leader0 rejected/abandoned this proposal — same reasoning: park and let the run abort.
            std::future::pending::<()>().await;
          }
          done += 1;
        }
      }
      done
    }));
  }

  // The clients only return after committing their full shares (any anomaly parks them), so this sum
  // equals `group_total` by construction on a valid run; assert it as defense-in-depth. A client
  // `JoinError` (panic / cancel) is FATAL — a dead client must never be scored as success.
  let mut observed = 0u64;
  for h in client_handles {
    observed += h
      .await
      .expect("client task panicked or was cancelled — run invalid");
  }
  assert_eq!(
    observed, group_total,
    "group {group}: clients committed {observed} ops, expected {group_total} — run invalid"
  );
  observed
}

/// Police one group for the two anomalies that invalidate a throughput number — leadership leaving the
/// captured `leader0`, or a node task ending (a death) — panicking (which aborts the whole run) on
/// either. Called for every group on every window sweep AND once more after the window closes, so a
/// group stays guarded for the entire shared window, including after its own clients have finished.
fn check_group_integrity(guard: &mut Guard) {
  if guard.current_leader.load(Ordering::Acquire) != guard.leader0 {
    panic!("group {}: {LEADER_CHANGED_MSG}", guard.group);
  }
  // `try_join_next` is non-blocking: `Some` means a node task ended, which during the window can only
  // be a death (a healthy node task loops until aborted after the window).
  if guard.nodes.try_join_next().is_some() {
    panic!("group {}: {NODE_DIED_MSG}", guard.group);
  }
}

/// One cluster node: owns its `Endpoint` + in-memory stores and hand-drives the Sans-I/O crank.
async fn run_node(
  id: u64,
  members: u64,
  origin: WallInstant,
  mut inbound_rx: mpsc::UnboundedReceiver<Inbound>,
  senders: Arc<Vec<mpsc::UnboundedSender<Inbound>>>,
  current_leader: Arc<AtomicU64>,
  timing_active: Arc<AtomicBool>,
) {
  let voters: Vec<u64> = (0..members).collect();
  let cfg = Config::try_new(
    id,
    voters,
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .expect("valid config");
  // Compaction stays on at the default `snapshot_threshold`: the log holds ~one threshold of entries
  // in steady state, so this measures bounded consensus work. `CountSm`'s O(1) snapshot keeps that
  // compaction cheap (the simulation `LogSm`'s O(n) snapshot is the artifact `CountSm` avoids).
  let mut ep: Node = Endpoint::new(cfg, Instant::ORIGIN, id, CountSm::new());
  let mut log = MemLog::new();
  let mut stable = MemStable::<u64>::new();
  // Every client write carries the same small fixed payload — the FSM only counts applies, so the
  // contents are irrelevant to throughput.
  let payload = Bytes::from_static(&[0u8; 8]);

  // Leader-only: maps an accepted proposal's log index to the client waiting on its commit.
  let mut pending: HashMap<Index, oneshot::Sender<()>> = HashMap::new();
  let mut was_leader = false;

  let mut ticker = tokio::time::interval(TIMER_TICK);
  ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

  loop {
    let wake = tokio::select! {
      m = inbound_rx.recv() => Wake::Msg(m),
      _ = ticker.tick() => Wake::Tick,
    };
    let now = Instant::ORIGIN + origin.elapsed();

    match wake {
      Wake::Msg(None) => break, // all senders dropped → shut down
      Wake::Msg(Some(m)) => apply_inbound(
        now,
        &mut ep,
        &mut log,
        &mut stable,
        &mut pending,
        &payload,
        m,
      ),
      Wake::Tick => {}
    }
    // Drain any further queued inbound to amortize the pump over a batch.
    let mut budget = 0usize;
    while budget < DRAIN_BUDGET {
      match inbound_rx.try_recv() {
        Ok(m) => {
          apply_inbound(
            now,
            &mut ep,
            &mut log,
            &mut stable,
            &mut pending,
            &payload,
            m,
          );
          budget += 1;
        }
        Err(_) => break,
      }
    }

    // Fire a consensus timer only when one is actually due (matches `pure_core`).
    if ep.poll_timeout().is_some_and(|d| d <= now) {
      ep.handle_timeout(now, &mut log, &mut stable);
    }

    pump(
      now,
      &mut ep,
      &mut log,
      &mut stable,
      &senders,
      id,
      &mut pending,
      &timing_active,
    );

    let is_leader = ep.role().is_leader();
    if is_leader {
      current_leader.store(id, Ordering::Release);
    } else if was_leader {
      // Stepped down: relinquish the published leadership so the timed-window watcher observes the
      // change and aborts the run. Outstanding `pending` replies are intentionally NOT cancelled —
      // those entries may still commit under the new leader, and cancelling them would invite a
      // client re-propose that double-counts. (A stable benchmark never reaches this branch.)
      let _ = current_leader.compare_exchange(id, NO_LEADER, Ordering::AcqRel, Ordering::Acquire);
    }
    was_leader = is_leader;
  }
}

/// Apply one inbound item: deliver a peer message, or propose a client write (recording its reply
/// against the assigned index, to fire on apply). A rejected propose drops `reply`; the awaiting
/// client then parks and the timed-window watcher aborts the run (a stable leader never rejects).
fn apply_inbound(
  now: Instant,
  ep: &mut Node,
  log: &mut MemLog,
  stable: &mut MemStable<u64>,
  pending: &mut HashMap<Index, oneshot::Sender<()>>,
  payload: &Bytes,
  m: Inbound,
) {
  match m {
    Inbound::Peer { from, message } => ep.handle_message(now, log, stable, from, message),
    Inbound::Client { reply } => {
      // A rejected propose (this node is no longer leader) drops `reply` here; its client observes
      // the cancellation, parks, and the run aborts. A stable leader accepts every proposal.
      if let Ok(index) = ep.propose(now, log, &*stable, payload) {
        pending.insert(index, reply);
      }
    }
  }
}

/// Pump the Sans-I/O crank to local quiescence: complete persistence (persist-before-ack/-vote),
/// route every produced message to its target's inbound channel, and fire client replies as their
/// proposals apply. Loops while storage reports more work or any message was produced.
#[allow(clippy::too_many_arguments)]
fn pump(
  now: Instant,
  ep: &mut Node,
  log: &mut MemLog,
  stable: &mut MemStable<u64>,
  senders: &[mpsc::UnboundedSender<Inbound>],
  id: u64,
  pending: &mut HashMap<Index, oneshot::Sender<()>>,
  timing_active: &AtomicBool,
) {
  // Flush the coalesced batch ONCE before the drain loop; re-flushing each iteration would re-send to
  // a still-Probe peer (a complete send leaves it un-paused with next_index unmoved), wedging the loop.
  ep.flush_appends(now, log, &*stable);
  let mut guard = 0u64;
  loop {
    guard += 1;
    assert!(
      guard < 10_000_000,
      "node {id}: storage/message pump failed to quiesce"
    );
    let mut progress = ep.handle_storage(now, log, stable).is_more_pending();

    while let Some(out) = ep.poll_message() {
      let (to, message) = Outgoing::into_parts(out);
      if let Some(s) = senders.get(to as usize) {
        // A closed peer channel means that node's task is gone. During the timed window that is a
        // fatal anomaly (a silently degraded quorum); panicking ends this node task too, which the
        // main monitor observes as a node death and aborts the run. Outside the window (election or
        // shutdown) a send failure is benign. A healthy in-process run never fails a send.
        if s.send(Inbound::Peer { from: id, message }).is_err()
          && timing_active.load(Ordering::Acquire)
        {
          panic!("node {id}: peer {to}'s channel is closed (peer dead) during the timed window");
        }
      }
      progress = true;
    }

    while let Some(ev) = ep.poll_event() {
      if let Event::Applied(applied) = ev
        && let Some(tx) = pending.remove(&applied.index())
      {
        let _ = tx.send(());
      }
    }

    if !progress {
      break;
    }
  }
}
