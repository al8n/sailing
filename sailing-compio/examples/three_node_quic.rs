//! Three sailing nodes over real loopback QUIC, one driver thread each.
//!
//! ```sh
//! cargo run -p sailing-compio --example three_node_quic
//! ```
//!
//! Each node runs the PRODUCTION shape: its own thread, its own compio `Runtime`, its own UDP
//! socket, its own stores — constructed AND run on that thread (a compio socket attaches to the
//! constructing thread's proactor). The `Handle`s are the only objects that cross threads: main
//! collects one per node, routes submits by following `NotLeader` redirect hints, and runs a
//! linearizable query against the leader's state machine without the state machine ever leaving
//! its driver thread.
//!
//! What the embedder supplies, narrated inline:
//! - **Storage** — a `LogStore` + `StableStore` per node. This example uses minimal synchronous
//!   in-memory stores; a real deployment supplies durable implementations honoring the
//!   contracts in `sailing_proto::storage` (completion-means-durable, the normative `term`
//!   domain, prefix-ordered append completions) and wires `DriverConfig::storage_ready` if its
//!   writes complete asynchronously.
//! - **A cluster PKI** — the QUIC transport REQUIRES cluster-private mandatory mTLS; this
//!   example mints a throwaway CA and per-node certs (SAN'd to the form the dialer derives).
//! - **A state machine** — deterministic `apply`/`snapshot`/`restore`; here a counter whose
//!   apply response is the post-apply count.

use std::{net::SocketAddr, time::Duration};

use bytes::Bytes;
use sailing_compio::{CompioQuicDriver, DriverConfig, DriverError, Node};
use sailing_proto::{ClusterId, Config};

#[path = "../tests/common/mod.rs"]
mod common;
use common::{CountSm, MemLog, MemStable, TestCa};

fn main() {
  let cluster = ClusterId([42; 16]);
  let addrs: Vec<SocketAddr> = (0..3)
    .map(|i| format!("127.0.0.1:{}", 47_000 + i).parse().unwrap())
    .collect();

  // One throwaway cluster CA; each node gets a leaf cert minted for the SAN the QUIC dialer
  // derives (node-<id-hex>.<cluster-hex>.sailing).
  let ca = TestCa::new();

  // One driver thread per node — the production scale-out unit. The handle comes back over a
  // plain std channel; everything else stays on the node's thread.
  let (handle_tx, handle_rx) = std::sync::mpsc::channel();
  let mut threads = Vec::new();
  for id in 1u64..=3 {
    let opts = ca.options(id, &cluster);
    let addrs = addrs.clone();
    let handle_tx = handle_tx.clone();
    threads.push(std::thread::spawn(move || {
      compio::runtime::Runtime::new()
        .expect("runtime")
        .block_on(async move {
          let peers: Vec<_> = (1u64..=3)
            .filter(|&p| p != id)
            .map(|p| Node::new(p, addrs[(p - 1) as usize]))
            .collect();
          let config = Config::try_new(
            id,
            vec![1u64, 2, 3],
            Duration::from_millis(300),
            Duration::from_millis(60),
          )
          .expect("config");
          let (driver, handle) = CompioQuicDriver::bind(
            addrs[(id - 1) as usize],
            config,
            id, // election-jitter seed
            CountSm::default(),
            opts,
            cluster,
            peers,
            MemLog::new(),
            MemStable::new(),
            DriverConfig::default(),
          )
          .await
          .expect("bind");
          handle_tx.send((id, handle)).expect("hand back the handle");
          // Drop our sender clone now: main collects the channel to its END, and the iterator
          // only ends when every sender is gone — a clone held for the driver's whole life
          // would park main forever.
          drop(handle_tx);
          // The driver runs until shutdown (or every handle clone drops).
          driver.run().await;
        });
    }));
  }
  drop(handle_tx);

  let mut handles: Vec<_> = handle_rx.iter().collect();
  handles.sort_by_key(|(id, _)| *id);
  let handles: Vec<_> = handles.into_iter().map(|(_, h)| h).collect();

  // Submit a few commands from the MAIN thread, following NotLeader redirects — the cluster
  // elects on its own timers underneath.
  let submit = |payload: &'static [u8]| {
    let mut at = 0usize;
    loop {
      match futures_executor::block_on(handles[at].submit(Bytes::from_static(payload))) {
        Ok(count) => return count,
        Err(DriverError::NotLeader { leader }) => {
          at = leader
            .map(|l| (l - 1) as usize)
            .unwrap_or((at + 1) % handles.len());
          std::thread::sleep(Duration::from_millis(50));
        }
        Err(DriverError::Superseded) => {} // retry: the payload is idempotent here
        Err(e) => panic!("submit failed: {e}"),
      }
    }
  };

  for (i, payload) in [&b"alpha"[..], b"beta", b"gamma"].iter().enumerate() {
    let count = submit(payload);
    println!("committed op {} -> applied count {count}", i + 1);
  }

  // A linearizable query: the closure runs on the serving node's driver thread against its
  // state machine, after a confirmed read index is applied.
  let count = handles
    .iter()
    .find_map(|h| futures_executor::block_on(h.query(|sm: &CountSm| sm.count())).ok())
    .expect("some node serves the read");
  println!("linearizable read -> {count}");
  assert_eq!(count, 3);

  // Orderly teardown: each ack means that node's socket is already rebindable.
  for h in &handles {
    let _ = futures_executor::block_on(h.shutdown());
  }
  for t in threads {
    let _ = t.join();
  }
  println!("done");
}
