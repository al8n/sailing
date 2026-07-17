//! Throughput benchmark for the SHARDED compio host: a 3-node cluster, each node a
//! [`ShardedCompioHost`] of K parallel planes over loopback TCP, driven through one routing
//! [`ShardedMultiHandle`] per node.
//!
//! Every node runs the same uniform [`ShardMap`], so group `g`'s replicas talk `shard(g)` ↔
//! `shard(g)`: the cluster decomposes into K independent per-plane meshes. The bench creates `-g`
//! groups PER PLANE (`-k * -g` total), elects them, then drives `-n` client ops round-robin across
//! every group — one loader thread per group, each pipelining `-b` concurrent submits to its
//! group's leader. It reports the aggregate committed put/s plus a per-plane breakdown (the
//! independent planes' shares of the throughput).
//!
//! `--reshape` additionally runs one split→merge-back cycle on a plane-0 group MID-LOAD (load half,
//! reshape, load the rest), timing the freeze window (PrepareMerge-propose → CommitMerge-applied,
//! wall ms) — the same reshaping-cost probe as `parity --reshape`, here through the real routed
//! handle and its multi-node freeze-barrier choreography (colocate the child's leadership onto the
//! parent's leader, freeze, absorb).
//!
//! The state machine is the split/absorb-capable [`ReshapeSm`] (an O(1) counter): the bench measures
//! throughput and reshaping cost, not conservation, so the counter's cheap snapshot keeps the number
//! a read on consensus + reshape work.
//!
//! Like the `parity` bench this drives the `Send` handles from a plain futures executor — no compio
//! runtime on the caller side, exactly the embedder shape.

use std::{
  collections::BTreeMap,
  net::SocketAddr,
  rc::Rc,
  sync::Arc,
  thread,
  time::{Duration, Instant},
};

use bytes::Bytes;
use clap::Parser;
use futures_executor::block_on;
use futures_util::future::join_all;
use sailing_benchmark::ReshapeSm;
use sailing_compio::{
  AcceptorFactory, DialerFactory, DriverConfig, DriverError, GroupHandle, Node, ShardMap,
  ShardRecordLayers, ShardedCompioHost, ShardedMultiHandle,
};
use sailing_proto::{ClusterId, Config, Data, Index, LabelOptions, Labeled, Passthrough, Role};

type Handle = ShardedMultiHandle<u64, u64, ReshapeSm>;
type Group = GroupHandle<u64, u64, ReshapeSm>;

const ELECTION: Duration = Duration::from_millis(300);
const HEARTBEAT: Duration = Duration::from_millis(60);
/// A generous per-operation retry deadline — the loopback cluster is no-fault, so any stall is a
/// wiring bug worth failing on rather than hanging.
const OP_DEADLINE: Duration = Duration::from_secs(20);

#[derive(Parser, Debug)]
#[command(
  about = "Throughput benchmark for the sharded compio host: a 3-node K-plane cluster over loopback TCP"
)]
struct Args {
  /// Number of parallel planes (shards) per node — each a full multi driver on its own core.
  #[arg(short = 'k', long, default_value_t = 2)]
  planes: usize,
  /// Number of groups mapped to each plane (`planes * groups_per_plane` groups total).
  #[arg(short = 'g', long, default_value_t = 2)]
  groups_per_plane: usize,
  /// Total client operations, distributed exactly across all groups.
  #[arg(short = 'n', long, default_value_t = 200_000, value_parser = parse_count)]
  operations: u64,
  /// Pipeline depth: concurrent submits issued to a group's leader before awaiting the batch.
  #[arg(short = 'b', long, default_value_t = 32)]
  batch: usize,
  /// Run one split→merge-back reshape cycle on a plane-0 group mid-load, timing the freeze window.
  #[arg(long)]
  reshape: bool,
}

/// Parse a `u64` with optional `_` separators and a `k`/`m`/`g` unit suffix (as `parity`).
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

fn cluster() -> ClusterId {
  ClusterId([19; 16])
}

fn encoded(id: u64) -> Vec<u8> {
  let mut v = Vec::new();
  id.encode(&mut v);
  v
}

fn config(id: u64, voters: Vec<u64>) -> Config<u64> {
  Config::try_new(id, voters, ELECTION, HEARTBEAT).expect("valid config")
}

/// Drive one `Send` handle future to completion from a plain thread (no compio runtime caller-side).
fn bo<T>(fut: impl std::future::Future<Output = T>) -> T {
  block_on(fut)
}

/// The per-plane plaintext record layers for node `id` — one `Send + Sync` provider shared across
/// the plane threads, each call building that plane's `Rc` factories ON its thread (ported from the
/// sharded compio integration suite).
fn plain_records(id: u64) -> ShardRecordLayers<u64, Labeled<Passthrough>> {
  let local = encoded(id);
  Arc::new(move |_shard: usize| {
    let dial_local = local.clone();
    let accept_local = local.clone();
    let dialer: DialerFactory<u64, Labeled<Passthrough>> = Rc::new(move |_: &u64| {
      Labeled::dialer(
        Passthrough::new(),
        &LabelOptions {
          cluster: cluster(),
          local_id: dial_local.clone(),
        },
      )
      .map_err(std::io::Error::other)
    });
    let acceptor: AcceptorFactory<Labeled<Passthrough>> = Rc::new(move || {
      Labeled::acceptor(
        Passthrough::new(),
        &LabelOptions {
          cluster: cluster(),
          local_id: accept_local.clone(),
        },
      )
      .map_err(std::io::Error::other)
    });
    Ok((dialer, acceptor))
  })
}

/// Spawn node `id`'s sharded host: K planes at `base` (ports `base .. base + K - 1`), dialing each
/// peer's planes by the same convention, under the cluster-wide uniform `map`.
fn spawn_host(
  id: u64,
  base: SocketAddr,
  peers: Vec<Node<u64, SocketAddr>>,
  map: ShardMap<u64>,
) -> Handle {
  ShardedCompioHost::<u64, u64, ReshapeSm, Labeled<Passthrough>>::new(
    map,
    base,
    peers,
    plain_records(id),
    DriverConfig::default(),
  )
  .spawn()
  .expect("the sharded host spawns")
}

/// Submit one op to a group through whichever node leads (retrying redirects) — used to force each
/// group's initial election before the timed window.
fn elect_group(groups: &[Group]) {
  let deadline = Instant::now() + OP_DEADLINE;
  let mut at = 0usize;
  loop {
    assert!(
      Instant::now() < deadline,
      "no commit within the election deadline"
    );
    match bo(groups[at].submit(Bytes::from_static(b"elect"))) {
      Ok(_) => return,
      Err(DriverError::NotLeader { leader }) => {
        at = leader
          .map(|l| (l - 1) as usize)
          .unwrap_or((at + 1) % groups.len());
        thread::sleep(Duration::from_millis(30));
      }
      Err(DriverError::Superseded) => {}
      Err(_) => {
        at = (at + 1) % groups.len();
        thread::sleep(Duration::from_millis(30));
      }
    }
  }
}

/// The index (0-based) of the node currently LEADING `gid`, waited for.
fn find_leader_idx(handles: &[Handle], gid: u64) -> usize {
  let deadline = Instant::now() + OP_DEADLINE;
  loop {
    assert!(Instant::now() < deadline, "group {gid}: no leader in time");
    for (i, h) in handles.iter().enumerate() {
      if let Ok(status) = bo(h.group(gid).status())
        && status.role == Role::Leader
      {
        return i;
      }
    }
    thread::sleep(Duration::from_millis(20));
  }
}

/// Commit `ops` client ops into `gid`, pipelining `batch` concurrent submits to its leader and
/// re-finding the leader whenever a submit is refused (a no-fault cluster re-finds only on the
/// deliberate reshape leadership move). Returns the committed count (`== ops`).
fn load_group(handles: &[Handle], gid: u64, ops: u64, batch: usize) -> u64 {
  let mut committed = 0u64;
  let mut leader = find_leader_idx(handles, gid);
  while committed < ops {
    let want = (batch as u64).min(ops - committed) as usize;
    let h = handles[leader].group(gid);
    let futs: Vec<_> = (0..want)
      .map(|_| h.submit(Bytes::from_static(b"load")))
      .collect();
    let mut refused = false;
    for r in bo(join_all(futs)) {
      match r {
        Ok(_) => committed += 1,
        Err(DriverError::Superseded) => {}
        Err(_) => refused = true,
      }
    }
    if refused {
      leader = find_leader_idx(handles, gid);
      thread::sleep(Duration::from_millis(5));
    }
  }
  committed
}

/// Drive `shares` (`(gid, ops)`) concurrently — one loader thread per group — and return each
/// group's committed count.
fn run_load(handles: &[Handle], shares: &[(u64, u64)], batch: usize) -> Vec<(u64, u64)> {
  let mut threads = Vec::with_capacity(shares.len());
  for &(gid, ops) in shares {
    let handles = handles.to_vec();
    threads.push(thread::spawn(move || {
      (gid, load_group(&handles, gid, ops, batch))
    }));
  }
  threads
    .into_iter()
    .map(|t| t.join().expect("a loader thread panicked"))
    .collect()
}

/// The node id (1-based) currently leading `gid`.
fn find_leader_node(handles: &[Handle], gid: u64) -> u64 {
  find_leader_idx(handles, gid) as u64 + 1
}

/// Move `gid`'s leadership onto node `to_node` and wait until it settles (the merge freeze-barrier
/// discipline — the source leader must colocate onto the absorbing target's leader).
fn colocate_onto(handles: &[Handle], gid: u64, to_node: u64) {
  let deadline = Instant::now() + OP_DEADLINE;
  loop {
    assert!(
      Instant::now() < deadline,
      "colocation of group {gid} onto node {to_node} never settled"
    );
    let at = find_leader_node(handles, gid);
    if at == to_node {
      return;
    }
    let _ = bo(
      handles[(at - 1) as usize]
        .group(gid)
        .transfer_leader(to_node),
    );
    thread::sleep(Duration::from_millis(40));
  }
}

/// Retry a merge verb across every node until one accepts, failing FAST on the permanent
/// `DirectionInverted` refusal (a property of the id pair, never cleared by retrying).
fn merge_verb(
  handles: &[Handle],
  what: &str,
  mut verb: impl FnMut(&Handle) -> Result<Index, DriverError<u64>>,
) {
  let inverted = format!("{:?}", sailing_proto::MergeError::<u64>::DirectionInverted);
  let deadline = Instant::now() + OP_DEADLINE;
  loop {
    assert!(Instant::now() < deadline, "{what} never accepted");
    for h in handles {
      match verb(h) {
        Ok(_) => return,
        Err(DriverError::Rejected { reason }) if reason == inverted => {
          panic!(
            "{what}: the merge claim is permanently inverted — source must encode above target"
          )
        }
        Err(_) => {}
      }
    }
    thread::sleep(Duration::from_millis(40));
  }
}

/// Whether every node reports `gid` absent (the child has dissolved everywhere).
fn child_gone(handles: &[Handle], gid: u64) -> bool {
  handles
    .iter()
    .all(|h| matches!(bo(h.group(gid).status()), Err(DriverError::Rejected { .. })))
}

/// One split→merge-back cycle on a plane-0 group, through the routed handle: split `parent` into
/// `child`, wait for the child to serve, colocate its leadership onto the parent's leader, then
/// freeze and absorb it back. Returns the freeze window in wall milliseconds.
fn reshape_cycle(handles: &[Handle], parent: u64, child: u64) -> f64 {
  // Split (retry across nodes until the parent's leader accepts).
  {
    let deadline = Instant::now() + OP_DEADLINE;
    'split: loop {
      assert!(Instant::now() < deadline, "no split accepted");
      for h in handles {
        if bo(h.propose_split(parent, child, 0, Bytes::from_static(b"\x01"))).is_ok() {
          break 'split;
        }
      }
      thread::sleep(Duration::from_millis(40));
    }
  }
  // Wait for the child to serve on any node (materialized everywhere before the merge).
  {
    let deadline = Instant::now() + OP_DEADLINE;
    while !handles.iter().any(|h| bo(h.group(child).status()).is_ok()) {
      assert!(Instant::now() < deadline, "the child never materialized");
      thread::sleep(Duration::from_millis(30));
    }
  }
  // Colocate the child (source) leadership onto the parent's (target) leader, then freeze + absorb.
  let parent_leader = find_leader_node(handles, parent);
  colocate_onto(handles, child, parent_leader);

  let freeze_start = Instant::now();
  merge_verb(handles, "the freeze", |h| {
    bo(h.prepare_merge(child, parent))
  });
  merge_verb(handles, "the commit", |h| bo(h.commit_merge(parent, child)));
  let deadline = Instant::now() + OP_DEADLINE;
  while !child_gone(handles, child) {
    assert!(Instant::now() < deadline, "the child never dissolved");
    thread::sleep(Duration::from_millis(20));
  }
  (Instant::now() - freeze_start).as_secs_f64() * 1000.0
}

fn main() {
  let args = Args::parse();
  assert!(args.planes >= 1, "-k (planes) must be >= 1");
  assert!(
    args.groups_per_plane >= 1,
    "-g (groups per plane) must be >= 1"
  );
  assert!(args.batch >= 1, "-b (batch) must be >= 1");
  let k = args.planes;
  let g = args.groups_per_plane;
  let map = ShardMap::<u64>::uniform(k);

  // Pick exactly `g` group ids per plane under the uniform map (the same derivation every node
  // computes), so the load is evenly spread across the K planes.
  let mut per_plane: Vec<Vec<u64>> = vec![Vec::new(); k];
  let mut cand = 1u64;
  while per_plane.iter().any(|v| v.len() < g) {
    let p = map.shard(&cand);
    if per_plane[p].len() < g {
      per_plane[p].push(cand);
    }
    cand += 1;
  }
  let all_gids: Vec<u64> = per_plane.iter().flatten().copied().collect();
  assert!(
    args.operations >= all_gids.len() as u64,
    "operations ({}) too small for {} groups",
    args.operations,
    all_gids.len()
  );

  // The reshape pair: a plane-0 group and a fresh same-plane child encoding strictly above it (the
  // merge direction rule — the child is the dissolving source).
  let reshape = args.reshape.then(|| {
    let parent = per_plane[0][0];
    let mut c = cand;
    let child = loop {
      if map.shard(&c) == 0 && !all_gids.contains(&c) && c.to_le_bytes() > parent.to_le_bytes() {
        break c;
      }
      c += 1;
    };
    (parent, child)
  });

  eprintln!(
    "sharded_host config: nodes=3 planes={k} groups_per_plane={g} groups={} operations={} batch={} reshape={}",
    all_gids.len(),
    args.operations,
    args.batch,
    args.reshape,
  );

  // Three nodes on distinct base ports (planes at base .. base + k - 1); every node runs the same
  // uniform map and dials its peers by the base + shard convention.
  let bases: Vec<SocketAddr> = (0..3u16)
    .map(|i| {
      format!("127.0.0.1:{}", 46000 + i * 1000)
        .parse()
        .expect("addr")
    })
    .collect();
  let handles: Vec<Handle> = (1..=3u64)
    .map(|id| {
      let peers: Vec<Node<u64, SocketAddr>> = (1..=3u64)
        .filter(|&p| p != id)
        .map(|p| Node::new(p, bases[(p - 1) as usize]))
        .collect();
      spawn_host(id, bases[(id - 1) as usize], peers, map.clone())
    })
    .collect();

  // Create every group on every node, then elect each.
  for &gid in &all_gids {
    for (i, h) in handles.iter().enumerate() {
      let id = i as u64 + 1;
      bo(h.create_group(
        gid,
        config(id, vec![1, 2, 3]),
        id * 100_000 + gid,
        ReshapeSm::new(),
        0,
      ))
      .expect("group admission");
    }
  }
  for &gid in &all_gids {
    let groups: Vec<Group> = handles.iter().map(|h| h.group(gid)).collect();
    elect_group(&groups);
  }

  // Distribute the op budget exactly across the groups.
  let num = all_gids.len() as u64;
  let base = args.operations / num;
  let rem = args.operations % num;
  let per_group: Vec<(u64, u64)> = all_gids
    .iter()
    .enumerate()
    .map(|(i, &gid)| (gid, base + u64::from((i as u64) < rem)))
    .collect();

  // The timed window: drive the load (splitting it around one reshape cycle when `--reshape`).
  let mut committed: BTreeMap<u64, u64> = BTreeMap::new();
  let mut record = |part: Vec<(u64, u64)>| {
    for (gid, c) in part {
      *committed.entry(gid).or_default() += c;
    }
  };
  let start = Instant::now();
  let mut freeze_ms = None;
  if let Some((parent, child)) = reshape {
    let shares_a: Vec<(u64, u64)> = per_group.iter().map(|&(gid, o)| (gid, o / 2)).collect();
    let shares_c: Vec<(u64, u64)> = per_group.iter().map(|&(gid, o)| (gid, o - o / 2)).collect();
    record(run_load(&handles, &shares_a, args.batch));
    freeze_ms = Some(reshape_cycle(&handles, parent, child));
    record(run_load(&handles, &shares_c, args.batch));
  } else {
    record(run_load(&handles, &per_group, args.batch));
  }
  let elapsed = start.elapsed();

  // Report: exact total, aggregate put/s, and the per-plane breakdown.
  let total: u64 = committed.values().sum();
  assert_eq!(
    total, args.operations,
    "groups committed {total} ops, expected -n = {} — run invalid",
    args.operations
  );
  let put_s = total as f64 / elapsed.as_secs_f64();
  println!(
    "sharded_host  nodes=3 planes={k} groups_per_plane={g} groups={} ops={total} batch={} \
     elapsed={:.3}s  put/s={:.0}",
    all_gids.len(),
    args.batch,
    elapsed.as_secs_f64(),
    put_s,
  );
  for plane in 0..k {
    let plane_committed: u64 = committed
      .iter()
      .filter(|(gid, _)| map.shard(gid) == plane)
      .map(|(_, c)| *c)
      .sum();
    let plane_put_s = plane_committed as f64 / elapsed.as_secs_f64();
    println!("plane={plane} committed={plane_committed} put_s={plane_put_s:.0}");
  }
  if let Some(w) = freeze_ms {
    println!("reshape freeze_window_ms={w:.3}");
  }

  for h in &handles {
    bo(h.shutdown()).expect("the sharded host tears down");
  }
}
