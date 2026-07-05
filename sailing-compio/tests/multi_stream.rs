//! Real-socket integration for the MULTI-GROUP compio stream driver on ONE plane: loopback-TCP
//! hosts carrying two Raft groups over shared connections and one shared engine barrier — the
//! compio parity check against the reactor multi loopback basics (elect, commit through
//! redirects, linearizable reads, group removal), driven per the crate's thread-per-core shape
//! (each `!Send` driver constructed and spawned on the test's runtime thread).

mod common;

use std::{net::SocketAddr, rc::Rc, time::Duration};

use bytes::Bytes;
use common::CountSm;
use sailing_compio::{
  CompioMultiStreamDriver, DriverConfig, DriverError, GroupHandle, MultiHandle, Node,
};
use sailing_proto::{ClusterId, Config, Data, LabelOptions, Labeled, Passthrough};

const ELECTION: Duration = Duration::from_millis(300);
const HEARTBEAT: Duration = Duration::from_millis(60);

fn cluster() -> ClusterId {
  ClusterId([13; 16])
}

fn encoded(id: u64) -> Vec<u8> {
  let mut v = Vec::new();
  id.encode(&mut v);
  v
}

fn config(id: u64, voters: Vec<u64>) -> Config<u64> {
  Config::try_new(id, voters, ELECTION, HEARTBEAT).unwrap()
}

/// Plaintext dialer/acceptor factories for node `id` (the single-group suite's, shared here).
fn plain_factories(
  id: u64,
) -> (
  sailing_compio::DialerFactory<u64, Labeled<Passthrough>>,
  sailing_compio::AcceptorFactory<Labeled<Passthrough>>,
) {
  let local = encoded(id);
  let dial_local = local.clone();
  let dialer: sailing_compio::DialerFactory<u64, Labeled<Passthrough>> = Rc::new(move |_: &u64| {
    Labeled::dialer(
      Passthrough::new(),
      &LabelOptions {
        cluster: cluster(),
        local_id: dial_local.clone(),
      },
    )
    .map_err(std::io::Error::other)
  });
  let acceptor: sailing_compio::AcceptorFactory<Labeled<Passthrough>> = Rc::new(move || {
    Labeled::acceptor(
      Passthrough::new(),
      &LabelOptions {
        cluster: cluster(),
        local_id: local.clone(),
      },
    )
    .map_err(std::io::Error::other)
  });
  (dialer, acceptor)
}

/// Submit through whichever node is (or redirects to) the group's leader.
async fn submit_anywhere(groups: &[GroupHandle<u64, u64, CountSm>], payload: &'static [u8]) -> u64 {
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  let mut at = 0usize;
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "no commit within the deadline"
    );
    match groups[at].submit(Bytes::from_static(payload)).await {
      Ok(response) => return response,
      Err(DriverError::NotLeader { leader }) => {
        at = leader
          .map(|l| (l - 1) as usize)
          .unwrap_or((at + 1) % groups.len());
        compio::time::sleep(Duration::from_millis(50)).await;
      }
      Err(DriverError::Superseded) => {}
      Err(DriverError::Rejected { .. }) => {
        at = (at + 1) % groups.len();
        compio::time::sleep(Duration::from_millis(50)).await;
      }
      Err(e) => panic!("unexpected submit error: {e:?}"),
    }
  }
}

/// Query the group through any node that will serve it (sailing forwards follower reads).
async fn query_anywhere(groups: &[GroupHandle<u64, u64, CountSm>]) -> u64 {
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "no query within the deadline"
    );
    for g in groups {
      if let Ok(c) = g.query(|sm: &CountSm| sm.count()).await {
        return c;
      }
    }
    compio::time::sleep(Duration::from_millis(50)).await;
  }
}

/// The single-plane gate: a 2-node compio multi host carries groups 100 and 200 over ONE mesh of
/// shared connections; each group elects, commits through redirects, and serves linearizable
/// reads with the two apply streams strictly isolated; removing one group on one node reports
/// it hosted, fails its later ops with the no-such-group rejection there, and leaves the
/// sibling group committing — the reactor multi loopback basics on the compio driver.
#[compio::test]
async fn two_node_multi_host_commits_and_removes() {
  let addrs: Vec<SocketAddr> = (0..2)
    .map(|i| format!("127.0.0.1:{}", 45_000 + i).parse().unwrap())
    .collect();
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  for id in 1u64..=2 {
    let peers: Vec<_> = (1u64..=2)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (dialer, acceptor) = plain_factories(id);
    let (driver, handle) = CompioMultiStreamDriver::bind(
      addrs[(id - 1) as usize],
      peers,
      dialer,
      acceptor,
      DriverConfig::default(),
    )
    .await
    .expect("the empty multi host binds");
    compio::runtime::spawn(driver.run()).detach();
    handles.push(handle);
  }
  for gid in [100u64, 200] {
    for (i, h) in handles.iter().enumerate() {
      let id = i as u64 + 1;
      h.create_group(
        gid,
        config(id, vec![1, 2]),
        id * 10 + gid,
        CountSm::default(),
        0,
      )
      .await
      .expect("group admission");
    }
  }

  let g100: Vec<_> = handles.iter().map(|h| h.group(100)).collect();
  let g200: Vec<_> = handles.iter().map(|h| h.group(200)).collect();

  // Interleave the two groups' commits: the counts prove per-group apply streams.
  assert_eq!(submit_anywhere(&g100, b"a1").await, 1);
  assert_eq!(submit_anywhere(&g200, b"b1").await, 1);
  assert_eq!(submit_anywhere(&g100, b"a2").await, 2);
  assert_eq!(submit_anywhere(&g200, b"b2").await, 2);
  assert_eq!(submit_anywhere(&g200, b"b3").await, 3);

  // Linearizable reads see exactly their own group's commits — cross-group isolation.
  assert_eq!(
    query_anywhere(&g100).await,
    2,
    "group 100 saw its 2 commits"
  );
  assert_eq!(
    query_anywhere(&g200).await,
    3,
    "group 200 saw its 3 commits"
  );

  // The shared events tail is group-stamped; every stamp names a hosted group.
  let mut stamped = 0;
  while let Ok((g, _ev)) = handles[0].events().try_recv() {
    assert!(g == 100 || g == 200, "a foreign group stamp: {g}");
    stamped += 1;
  }
  assert!(stamped > 0, "the tail observed group-stamped events");

  // Remove group 200 on node 1: the removal reports it hosted, the id answers no-such-group
  // there afterwards, and the sibling group keeps committing across the mesh.
  assert!(
    handles[0].remove_group(200).await.expect("remove resolves"),
    "node 1 hosted group 200"
  );
  match g200[0].status().await {
    Err(DriverError::Rejected { reason }) => {
      assert!(reason.contains("no such group"), "got: {reason}");
    }
    other => panic!("expected the no-such-group rejection, got {other:?}"),
  }
  assert_eq!(submit_anywhere(&g100, b"alive").await, 3);

  // Orderly teardown: each ack means that node's listener is already rebindable.
  for h in &handles {
    h.shutdown().await.expect("the multi host tears down");
  }
}

/// An abandoned fork is VISIBLE on the compio plane too: a committed split whose child id is
/// tombstoned on this host cannot materialize — the drain refuses it, resolves the parent's
/// fence, and surfaces `LifecycleEvent::SplitRefused` on the lifecycle tail. The parent keeps
/// serving on its shrunk half; the child stays unhosted until the embedder acts.
#[compio::test]
async fn refused_fork_surfaces_on_the_lifecycle_tail() {
  use sailing_compio::LifecycleEvent;

  let addr: SocketAddr = "127.0.0.1:45310".parse().unwrap();
  let (dialer, acceptor) = plain_factories(1);
  let (driver, handle) = CompioMultiStreamDriver::<u64, u64, CountSm, _>::bind(
    addr,
    Vec::new(),
    dialer,
    acceptor,
    DriverConfig::default(),
  )
  .await
  .expect("the empty multi host binds");
  compio::runtime::spawn(driver.run()).detach();

  handle
    .create_group(100, config(1, vec![1]), 1, CountSm::default(), 0)
    .await
    .expect("group admission");
  let g100 = handle.group(100);
  for i in 0..3u64 {
    assert_eq!(
      submit_anywhere(std::slice::from_ref(&g100), b"load").await,
      i + 1
    );
  }

  // Tombstone the child id (an unhosted removal still tombstones), then split into it: the
  // propose gate cannot see this host's removal history, so the entry commits and the refusal
  // happens at the materialization edge.
  assert!(!handle.remove_group(300).await.expect("remove resolves"));
  g100
    .propose_split(300, 0, Bytes::from_static(b"\x02"))
    .await
    .expect("the single-voter leader appends the split");

  // The driver shares this thread's runtime: await the tail, never block it.
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  loop {
    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
    assert!(remaining > Duration::ZERO, "no SplitRefused in time");
    match compio::time::timeout(remaining, handle.lifecycle().recv_async()).await {
      Ok(Ok(LifecycleEvent::SplitRefused { parent, child })) => {
        assert_eq!((parent, child), (100, 300), "the typed refusal");
        break;
      }
      Ok(Ok(_)) => {}
      Ok(Err(e)) => panic!("the lifecycle tail closed: {e:?}"),
      Err(_) => panic!("no SplitRefused in time"),
    }
  }

  // The parent's half shrank exactly once and its fence resolved: it keeps committing.
  assert_eq!(query_anywhere(std::slice::from_ref(&g100)).await, 1);
  assert_eq!(
    submit_anywhere(std::slice::from_ref(&g100), b"after").await,
    2
  );
  // The refused child never materialized here.
  assert!(handle.group(300).status().await.is_err());

  handle.shutdown().await.expect("the multi host tears down");
}
