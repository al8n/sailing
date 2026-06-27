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
//! Because sailing-proto is Sans-I/O, a node is NOT self-driving: each node task owns its
//! `Endpoint` + stores and hand-turns the crank — feed an inbound message (or fire a due timer),
//! then pump storage to quiescence (persist-before-ack/-vote), route the produced messages to peer
//! channels, and drain the applied events. This is the async-task analogue of `pure_core`'s
//! synchronous global drain; the difference is only that delivery now hops through channels and
//! tasks instead of an in-loop function call.
//!
//! Contrast with the `pure_core` bench, which strips the async framework entirely to expose the
//! consensus core's raw single-threaded cost.

use std::{
  collections::HashMap,
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
  time::{Duration, Instant as WallInstant},
};

use bytes::Bytes;
use clap::Parser;
use sailing_proto::{Config, Endpoint, Event, Index, Instant, Message, Outgoing};
use sailing_simulation::{LogSm, MemLog, MemStable};
use tokio::sync::{mpsc, oneshot};

type Node = Endpoint<u64, LogSm>;

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

#[derive(Parser, Debug)]
#[command(
  about = "Async cluster-throughput benchmark matching openraft's method (typed-message channels, no I/O)"
)]
struct Args {
  /// Number of concurrent client tasks proposing writes.
  #[arg(short = 'c', long, default_value_t = 4096, value_parser = parse_count)]
  clients: u64,
  /// Total number of operations across all clients.
  #[arg(short = 'n', long, default_value_t = 20_000_000, value_parser = parse_count)]
  operations: u64,
  /// Cluster size (1, 3, or 5).
  #[arg(short = 'm', long, default_value_t = 3)]
  members: u64,
  /// Batch size for writes: 1 = single writes, >1 = pipeline `batch` proposals before awaiting.
  #[arg(short = 'b', long, default_value_t = 1, value_parser = parse_count)]
  batch: u64,
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

#[tokio::main]
async fn main() {
  let args = Args::parse();
  eprintln!(
    "parity config: clients={} operations={} members={} batch={}",
    args.clients, args.operations, args.members, args.batch
  );
  run(args).await;
}

async fn run(args: Args) {
  assert!(args.members >= 1, "--members must be >= 1");
  assert!(args.batch >= 1, "--batch must be >= 1");
  assert!(args.clients >= 1, "--clients must be >= 1");
  let members = args.members;

  // A single monotonic origin shared by every node, so `now = ORIGIN + origin.elapsed()` advances
  // with real time. The cluster runs no LeaseGuard failover here, so the synchronized wall is absent.
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

  let mut node_handles = Vec::with_capacity(members as usize);
  for (id, rx) in receivers.into_iter().enumerate() {
    let senders = senders.clone();
    let current_leader = current_leader.clone();
    node_handles.push(tokio::spawn(run_node(
      id as u64,
      members,
      origin,
      rx,
      senders,
      current_leader,
    )));
  }

  // Elect a leader before timing: the election is startup cost, not throughput.
  let elect_deadline = WallInstant::now() + Duration::from_secs(30);
  while current_leader.load(Ordering::Acquire) == NO_LEADER {
    assert!(
      WallInstant::now() < elect_deadline,
      "no leader elected within 30s"
    );
    tokio::time::sleep(Duration::from_millis(2)).await;
  }

  // openraft-identical op accounting: round per-client ops down to a whole number of batches.
  let ops_per_client = args.operations / args.clients / args.batch * args.batch;
  let total = ops_per_client * args.clients;
  assert!(
    total > 0,
    "operations ({}) too small for clients*batch ({}*{})",
    args.operations,
    args.clients,
    args.batch
  );

  let start = WallInstant::now();
  let mut client_handles = Vec::with_capacity(args.clients as usize);
  for _ in 0..args.clients {
    let senders = senders.clone();
    let current_leader = current_leader.clone();
    let batch = args.batch;
    client_handles.push(tokio::spawn(async move {
      let mut done = 0u64;
      while done < ops_per_client {
        let want = batch.min(ops_per_client - done);
        let leader = loop {
          let l = current_leader.load(Ordering::Acquire);
          if l != NO_LEADER {
            break l;
          }
          tokio::time::sleep(Duration::from_millis(1)).await;
        };
        let sender = &senders[leader as usize];

        // Pipeline `want` proposals, then await each commit. A dropped reply (RecvError) means the
        // proposal was rejected (the target is no longer leader) or its leader stepped down before
        // applying — count only the successes and retry the remainder on the re-discovered leader.
        let mut rxs = Vec::with_capacity(want as usize);
        for _ in 0..want {
          let (tx, rx) = oneshot::channel();
          if sender.send(Inbound::Client { reply: tx }).is_err() {
            break; // node task gone (cluster shutting down)
          }
          rxs.push(rx);
        }
        let mut ok = 0u64;
        for rx in rxs {
          if rx.await.is_ok() {
            ok += 1;
          }
        }
        done += ok;
        if ok < want {
          tokio::time::sleep(Duration::from_millis(1)).await;
        }
      }
    }));
  }

  for h in client_handles {
    let _ = h.await;
  }
  let elapsed = start.elapsed();

  for h in &node_handles {
    h.abort();
  }

  let put_s = total as f64 / elapsed.as_secs_f64();
  let millis = elapsed.as_millis().max(1);
  println!(
    "parity  members={} clients={} batch={} ops={} elapsed={:.3}s  put/s={:.0}  op/ms={}",
    members,
    args.clients,
    args.batch,
    total,
    elapsed.as_secs_f64(),
    put_s,
    (total as u128) / millis,
  );
}

/// One cluster node: owns its `Endpoint` + in-memory stores and hand-drives the Sans-I/O crank.
async fn run_node(
  id: u64,
  members: u64,
  origin: WallInstant,
  mut inbound_rx: mpsc::UnboundedReceiver<Inbound>,
  senders: Arc<Vec<mpsc::UnboundedSender<Inbound>>>,
  current_leader: Arc<AtomicU64>,
) {
  let voters: Vec<u64> = (0..members).collect();
  let cfg = Config::try_new(
    id,
    voters,
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .expect("valid config");
  let mut ep: Node = Endpoint::new(cfg, Instant::ORIGIN, id, LogSm::new());
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
    );

    let is_leader = ep.role().is_leader();
    if is_leader {
      current_leader.store(id, Ordering::Release);
    } else if was_leader {
      // Stepped down: relinquish the published leadership and cancel outstanding replies so the
      // waiting clients observe the cancellation and retry on the new leader (rather than hang).
      let _ = current_leader.compare_exchange(id, NO_LEADER, Ordering::AcqRel, Ordering::Acquire);
      pending.clear();
    }
    was_leader = is_leader;
  }
}

/// Apply one inbound item: deliver a peer message, or propose a client write (recording its reply
/// against the assigned index; a rejected propose drops `reply`, signaling the client to retry).
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
      // A rejected propose (not leader / transfer in progress) drops `reply`, signaling the client
      // to retry on the re-discovered leader.
      if let Ok(index) = ep.propose(now, log, &*stable, payload) {
        pending.insert(index, reply);
      }
    }
  }
}

/// Pump the Sans-I/O crank to local quiescence: complete persistence (persist-before-ack/-vote),
/// route every produced message to its target's inbound channel, and fire client replies as their
/// proposals apply. Loops while storage reports more work or any message was produced.
fn pump(
  now: Instant,
  ep: &mut Node,
  log: &mut MemLog,
  stable: &mut MemStable<u64>,
  senders: &[mpsc::UnboundedSender<Inbound>],
  id: u64,
  pending: &mut HashMap<Index, oneshot::Sender<()>>,
) {
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
        let _ = s.send(Inbound::Peer { from: id, message });
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
