//! Real-socket integration for the MULTI-GROUP compio stream driver on ONE plane: loopback-TCP
//! hosts carrying two Raft groups over shared connections and one shared engine barrier — the
//! compio parity check against the reactor multi loopback basics (elect, commit through
//! redirects, linearizable reads, group removal), driven per the crate's thread-per-core shape
//! (each `!Send` driver constructed and spawned on the test's runtime thread).

mod common;

use std::{
  net::SocketAddr,
  rc::Rc,
  sync::{Arc, atomic::Ordering},
  time::Duration,
};

use bytes::Bytes;
use common::{CountSm, DelegatingEngine, EngineWatch, PRELOAD_TERM, TrapSm, preloaded_engine};
use sailing_compio::{
  CompioMultiStreamDriver, DriverConfig, DriverError, GroupHandle, LifecycleEvent, MultiHandle,
  Node,
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

/// A split into a child id this host has TOMBSTONED is refused at PROPOSE on the compio plane too
/// (the coordinator's #97-1 ChildRetired gate): the fork could never materialize onto a retired id,
/// so the entry is never appended and the parent never shrinks — no data loss. The child stays
/// unhosted; clear-then-recreate is the rejoin path.
#[compio::test]
async fn split_into_a_tombstoned_child_refuses_at_propose() {
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
  // coordinator's ChildRetired gate refuses at PROPOSE, before anything is appended.
  assert!(!handle.remove_group(300).await.expect("remove resolves"));
  let err = g100
    .propose_split(300, 0, Bytes::from_static(b"\x02"))
    .await
    .expect_err("a split into a locally-tombstoned child is refused at propose");
  assert!(
    matches!(&err, DriverError::Rejected { reason } if reason.contains("ChildRetired")),
    "the typed ChildRetired refusal, got {err:?}"
  );

  // The parent never shrank — the split was never appended, so no unit was given away or lost —
  // and it keeps committing.
  assert_eq!(query_anywhere(std::slice::from_ref(&g100)).await, 3);
  assert_eq!(
    submit_anywhere(std::slice::from_ref(&g100), b"after").await,
    4
  );
  // The tombstoned child id stays unhosted.
  assert!(handle.group(300).status().await.is_err());

  handle.shutdown().await.expect("the multi host tears down");
}

/// The compio single-plane merge, end to end: two loaded groups on one 2-node mesh — freeze,
/// parked commit, per-crank resolution — then the target serves the union, the source id
/// answers no-such-group everywhere, and re-admission at any generation refuses on the terminal
/// floor. The clock-free choreography needs nothing from the harness but patience: every
/// resolution input is log-determined, so both nodes converge on their own cranks.
#[compio::test]
async fn merge_absorbs_and_source_never_returns() {
  let addrs: Vec<SocketAddr> = (0..2)
    .map(|i| format!("127.0.0.1:{}", 45_620 + i).parse().unwrap())
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
  assert_eq!(submit_anywhere(&g100, b"a1").await, 1);
  assert_eq!(submit_anywhere(&g100, b"a2").await, 2);
  assert_eq!(submit_anywhere(&g200, b"b1").await, 1);
  assert_eq!(submit_anywhere(&g200, b"b2").await, 2);
  assert_eq!(submit_anywhere(&g200, b"b3").await, 3);

  // Freeze 200 into 100 (retry across nodes for the source leader). The direction rule makes the
  // encoding-minimal id the survivor: group 100 is the target, group 200 the source that dissolves
  // (200's LE encoding sorts strictly above 100's). DirectionInverted is a property of the id pair,
  // never transient, so fail fast on it — `map_merge_err` carries the variant's Debug form.
  let inverted = format!("{:?}", sailing_proto::MergeError::<u64>::DirectionInverted);
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  'freeze: loop {
    assert!(std::time::Instant::now() < deadline, "no freeze accepted");
    for h in &handles {
      match h.prepare_merge(200, 100).await {
        Ok(_) => break 'freeze,
        Err(DriverError::Rejected { reason }) if reason == inverted => {
          panic!("the freeze is permanently inverted — source must encode above target")
        }
        Err(_) => {}
      }
    }
    compio::time::sleep(Duration::from_millis(40)).await;
  }
  // Commit the absorb (retries ride out both leader routing and the local source still
  // catching up to frozen-applied on the target leader's node).
  'commit: loop {
    assert!(std::time::Instant::now() < deadline, "no commit accepted");
    for h in &handles {
      if h.commit_merge(100, 200).await.is_ok() {
        break 'commit;
      }
    }
    compio::time::sleep(Duration::from_millis(40)).await;
  }

  // The union serves from the target once each node's crank resolves its park.
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "the union never served"
    );
    let mut counts = Vec::new();
    for g in &g100 {
      if let Ok(c) = g.query(|sm: &CountSm| sm.count()).await {
        counts.push(c);
      }
    }
    if counts.contains(&5) {
      break;
    }
    compio::time::sleep(Duration::from_millis(40)).await;
  }

  // The source id dies everywhere: status answers no-such-group on both nodes, and
  // re-admission refuses at ANY generation — the floor is terminal.
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "the source never tore down everywhere"
    );
    let mut gone = 0;
    for g in &g200 {
      if matches!(g.status().await, Err(DriverError::Rejected { .. })) {
        gone += 1;
      }
    }
    if gone == handles.len() {
      break;
    }
    compio::time::sleep(Duration::from_millis(40)).await;
  }
  for (i, h) in handles.iter().enumerate() {
    let id = i as u64 + 1;
    match h
      .create_group(200, config(id, vec![1, 2]), 9, CountSm::default(), 9)
      .await
    {
      Err(DriverError::Rejected { reason }) => {
        assert!(
          reason.contains("floor"),
          "node {id}: the refusal must be the terminal floor, got: {reason}"
        );
      }
      other => panic!("node {id}: a merged-away id must never re-admit, got {other:?}"),
    }
  }

  for h in &handles {
    h.shutdown().await.expect("the multi host tears down");
  }
}

/// A restore for a group the host holds NO stored state for fails closed instead of fabricating a
/// blank incarnation: removing a group drops its in-memory stores, so a later restore of that id
/// finds nothing to recover and returns `NoStoredState` rather than a silent empty `Ok`. The id is
/// not resurrected, and the co-hosted live group keeps committing.
#[compio::test]
async fn restore_without_stored_state_fails_closed() {
  let addr: SocketAddr = "127.0.0.1:45330".parse().unwrap();
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
    .expect("the live co-hosted group admits");
  handle
    .create_group(200, config(1, vec![1]), 2, CountSm::default(), 0)
    .await
    .expect("the second group admits");
  let g200 = handle.group(200);
  assert_eq!(submit_anywhere(std::slice::from_ref(&g200), b"x").await, 1);
  assert!(
    handle.remove_group(200).await.expect("remove resolves"),
    "the hosted removal dropped storage"
  );
  assert!(
    handle.clear_tombstone(200).await.expect("clear resolves"),
    "the removal left a tombstone to clear"
  );

  // The tombstone is cleared, so nothing but the ABSENT stores stands between this call and a
  // restore. The removal dropped those in-memory stores, so the restore has nothing to recover:
  // it fails closed rather than silently standing up a blank index-0 incarnation.
  match handle
    .restore_group(200, config(1, vec![1]), 2, CountSm::default(), 0)
    .await
  {
    Err(DriverError::NoStoredState) => {}
    other => panic!("expected NoStoredState for a group with no stored state, got {other:?}"),
  }
  assert!(
    handle.group(200).status().await.is_err(),
    "the group was not resurrected"
  );

  // The co-hosted live group is undisturbed by the refused restore.
  let g100 = handle.group(100);
  assert_eq!(submit_anywhere(std::slice::from_ref(&g100), b"y").await, 1);

  handle.shutdown().await.expect("the multi host tears down");
}

/// A query closure that PANICS is caught at the handle seam — the caller gets `QueryPanicked` and the
/// driver task does NOT unwind. But the caught panic is UNATTRIBUTABLE: the closure captured arbitrary
/// state that can alias ANY hosted group's FSM, so it FAIL-STOPS THE WHOLE PLANE — the group it read
/// against AND every co-located group poison. Each surfaces on the best-effort lifecycle tail; the
/// driver survives (a fail-stop, never an unwind).
#[compio::test]
async fn a_panicking_query_fails_typed_and_the_driver_survives() {
  let addr: SocketAddr = "127.0.0.1:45340".parse().unwrap();
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

  for gid in [100u64, 200] {
    handle
      .create_group(gid, config(1, vec![1]), 1, CountSm::default(), 0)
      .await
      .expect("group admission");
  }
  let g100 = handle.group(100);
  let g200 = handle.group(200);
  assert_eq!(submit_anywhere(std::slice::from_ref(&g100), b"x").await, 1);

  // The panic is caught at the handle seam: the caller gets QueryPanicked, the driver task does
  // NOT unwind.
  match g100
    .query(|_: &CountSm| -> u64 { panic!("boom in query") })
    .await
  {
    Err(DriverError::QueryPanicked) => {}
    other => panic!("expected QueryPanicked, got {other:?}"),
  }

  // The read ran against group 100's FSM, so the caught panic FAIL-STOPS group 100 — it surfaces on
  // the best-effort lifecycle tail as `Poisoned`. Cranking the driver via sibling submits drains the
  // observation. RED before the fail-stop wiring (a caught panic kept the group serving, no poison).
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  let mut seen = false;
  while !seen && std::time::Instant::now() < deadline {
    let _ = g200.submit(Bytes::from_static(b"tick")).await;
    while let Ok(ev) = handle.lifecycle().try_recv() {
      if matches!(ev, LifecycleEvent::Poisoned { group: 100 }) {
        seen = true;
      }
    }
  }
  assert!(
    seen,
    "the query-panicked group fail-stopped and surfaced on the lifecycle tail"
  );

  // Plane-fatal, not group-scoped: the caught panic is unattributable, so the co-located sibling 200
  // fail-stops too — a submit to it now reports `Poisoned`, not a fresh commit.
  match g200.submit(Bytes::from_static(b"z")).await {
    Err(DriverError::Poisoned) => {}
    other => panic!("the sibling must fail-stop on the plane-fatal panic, got {other:?}"),
  }

  handle.shutdown().await.expect("the multi host tears down");
}

/// A storage/apply fault fail-stops a group and the container surfaces it on the aggregate
/// lifecycle tail as `LifecycleEvent::Poisoned` — best-effort, with NO auto-teardown: a sibling
/// group on the same host keeps committing throughout.
#[compio::test]
async fn a_poisoned_group_surfaces_on_the_lifecycle_tail() {
  let addr: SocketAddr = "127.0.0.1:45320".parse().unwrap();
  let (dialer, acceptor) = plain_factories(1);
  let (driver, handle) = CompioMultiStreamDriver::<u64, u64, TrapSm, _>::bind(
    addr,
    Vec::new(),
    dialer,
    acceptor,
    DriverConfig::default(),
  )
  .await
  .expect("the empty multi host binds");
  compio::runtime::spawn(driver.run()).detach();

  for gid in [100u64, 200] {
    handle
      .create_group(gid, config(1, vec![1]), 1, TrapSm::default(), 0)
      .await
      .expect("group admission");
  }
  let g100 = handle.group(100);
  let g200 = handle.group(200);

  // Fail-stop group 100 with a trapped apply: retry until it leads and the BOOM commits+applies.
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "the trapped apply never poisoned"
    );
    match g100.submit(Bytes::from_static(b"BOOM")).await {
      Err(DriverError::Poisoned) => break,
      _ => compio::time::sleep(Duration::from_millis(50)).await,
    }
  }

  // The fail-stop rides the best-effort lifecycle tail. Sibling group 200 keeps committing (no
  // auto-teardown) and each submit cranks the driver, draining the pending observation.
  let mut seen = false;
  while !seen && std::time::Instant::now() < deadline {
    let _ = g200.submit(Bytes::from_static(b"tick")).await;
    while let Ok(ev) = handle.lifecycle().try_recv() {
      if matches!(ev, LifecycleEvent::Poisoned { group: 100 }) {
        seen = true;
      }
    }
  }
  assert!(seen, "the poisoned group surfaced on the lifecycle tail");

  handle.shutdown().await.expect("the multi host tears down");
}

/// FENCE INERTNESS on the compio plane — the reactor suites' sibling, proving the thread-per-core
/// driver publishes the same demux verdict. A retired incarnation's frames are dropped before the
/// endpoint sees them, so the husk can neither depose nor be heard.
///
/// Node 2 reaches a floored, re-admitted incarnation through the public lifecycle path (create at
/// generation 5 → remove, which persists the removal floor → clear the tombstone → create at 6)
/// and hosts a NON-MEMBER observer, so its own term never moves — any term it showed would have to
/// have come from the husk. Node 1 hosts the same gid at the generation the floor retired and
/// campaigns forever, needing a vote it will never be granted.
#[compio::test]
async fn a_retired_incarnations_frames_are_fenced_on_compio() {
  let addrs: Vec<SocketAddr> = (0..2)
    .map(|i| format!("127.0.0.1:{}", 45_350 + i).parse().unwrap())
    .collect();
  let mut handles: Vec<MultiHandle<u64, u64, CountSm>> = Vec::new();
  let mut fenced = None;
  for id in 1u64..=2 {
    let peers: Vec<_> = (1u64..=2)
      .filter(|&p| p != id)
      .map(|p| Node::new(p, addrs[(p - 1) as usize]))
      .collect();
    let (dialer, acceptor) = plain_factories(id);
    let (driver, handle) = CompioMultiStreamDriver::<u64, u64, CountSm, _>::bind(
      addrs[(id - 1) as usize],
      peers,
      dialer,
      acceptor,
      DriverConfig::default(),
    )
    .await
    .expect("the empty multi host binds");
    if id == 2 {
      fenced = Some(driver.engine_metrics());
    }
    compio::runtime::spawn(driver.run()).detach();
    handles.push(handle);
  }
  let fenced = fenced.expect("node 2's metrics");

  let observer = || Config::try_new_observer(2u64, vec![1u64], ELECTION, HEARTBEAT).unwrap();
  handles[1]
    .create_group(100, observer(), 2, CountSm::default(), 5)
    .await
    .expect("first incarnation admits");
  assert!(
    handles[1]
      .remove_group(100)
      .await
      .expect("removal resolves"),
    "node 2 hosted the first incarnation"
  );
  assert!(
    handles[1]
      .clear_tombstone(100)
      .await
      .expect("clear resolves"),
    "the tombstone was set by the removal"
  );
  handles[1]
    .create_group(100, observer(), 2, CountSm::default(), 6)
    .await
    .expect("the successor admits above its floor");

  handles[0]
    .create_group(100, config(1, vec![1, 2]), 1, CountSm::default(), 0)
    .await
    .expect("the husk admits locally");
  let resolved = handles[1].group(100);

  let deadline = std::time::Instant::now() + Duration::from_secs(10);
  while fenced.fenced_frames_dropped() == 0 {
    assert!(
      std::time::Instant::now() < deadline,
      "the husk's frames never reached the fence — the link or the stamp is wrong"
    );
    compio::time::sleep(Duration::from_millis(50)).await;
  }
  compio::time::sleep(Duration::from_millis(600)).await;
  let after = resolved.status().await.expect("status");
  assert_eq!(
    after.term,
    sailing_proto::Term::ZERO,
    "the husk's campaigns never moved the resolved member's term"
  );
  assert!(
    after.leader.is_none(),
    "and never made it follow a retired incarnation"
  );
  assert!(
    fenced.fenced_frames_dropped() > 1,
    "the fence counter keeps naming the husk, beat after beat"
  );

  for h in &handles {
    h.shutdown().await.expect("the multi host tears down");
  }
}

/// Compile-level proof that the ENGINE SEAM is publicly reachable on THIS surface: from a crate
/// that can see only `sailing-compio`'s public API, a foreign `MultiEngine` binds this host. The
/// loopback suites above already prove the host runs, so this is never invoked — what it pins is
/// that `bind_with_engine` exists, is public, and accepts an engine this crate built itself.
#[allow(dead_code)]
async fn a_foreign_engine_binds_this_host(
  addr: SocketAddr,
  watch: std::sync::Arc<EngineWatch>,
) -> (
  CompioMultiStreamDriver<u64, u64, CountSm, Labeled<Passthrough>, DelegatingEngine>,
  MultiHandle<u64, u64, CountSm>,
) {
  let (dialer, acceptor) = plain_factories(1);
  CompioMultiStreamDriver::bind_with_engine(
    addr,
    Vec::new(),
    dialer,
    acceptor,
    DriverConfig::default(),
    DelegatingEngine::new(watch),
  )
  .await
  .expect("the host binds over a foreign engine")
}

/// A host handed an engine that ALREADY holds a group's stores must never build a FRESH endpoint
/// over them: occupied stores are recovered state, and a term-0 endpoint would ack success and
/// then truncate that durable history on its first append. Both creation flavors refuse typed —
/// the fork flavor on the same fact, since occupied child stores mean the child's baseline already
/// exists — the stores come through untouched (they still RESTORE, carrying their own term), and a
/// different id is unaffected.
#[compio::test]
async fn a_preloaded_engine_refuses_fresh_creation_and_keeps_its_stores() {
  // Port 0: this host is PEERLESS, so nothing needs to find it — let the OS pick and keep the
  // suite free of fixed-port collisions.
  let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
  let gid = 100u64;
  let watch = Arc::new(EngineWatch::default());
  let (dialer, acceptor) = plain_factories(1);
  let (driver, handle) = CompioMultiStreamDriver::<u64, u64, CountSm, _, _>::bind_with_engine(
    addr,
    Vec::new(),
    dialer,
    acceptor,
    DriverConfig::default(),
    DelegatingEngine::preloaded(preloaded_engine(&[gid]), watch.clone()),
  )
  .await
  .expect("the host binds over a preloaded engine");
  compio::runtime::spawn(driver.run()).detach();

  let err = handle
    .create_group(gid, config(1, vec![1]), 1, CountSm::default(), 0)
    .await
    .expect_err("a fresh create over recovered stores must refuse");
  assert!(
    matches!(err, DriverError::StoredStateExists),
    "the typed occupied-stores refusal, got {err:?}"
  );

  let err = handle
    .create_group_from_fork(
      gid,
      config(1, vec![1]),
      1,
      CountSm::default(),
      Bytes::from_static(b"child-baseline"),
      0,
    )
    .await
    .expect_err("a fork install over occupied child stores must refuse");
  assert!(
    matches!(err, DriverError::StoredStateExists),
    "the typed occupied-stores refusal, got {err:?}"
  );

  // THE STORES ARE UNTOUCHED. They still restore, and what comes back is what was put in: a
  // create that had overwritten them would have left a term-0 endpoint behind.
  handle
    .restore_group(gid, config(1, vec![1]), 1, CountSm::default(), 0)
    .await
    .expect("the refused creates left the stores recoverable");
  let status = handle
    .group(gid)
    .status()
    .await
    .expect("the restored group answers");
  assert!(
    status.term.get() >= PRELOAD_TERM,
    "the recovered term came from the preloaded stores, got {:?}",
    status.term
  );

  // The refusal is about OCCUPIED STORES, not about creates: a fresh id still admits.
  handle
    .create_group(200, config(1, vec![1]), 1, CountSm::default(), 0)
    .await
    .expect("a fresh id is unaffected");

  handle.shutdown().await.expect("the multi host tears down");
}

/// PERSIST-BEFORE-REPLY across a teardown. A removal stages its admission floor and the removal
/// itself; the `Shutdown` behind it in the SAME bounded drain exits the loop before the crank that
/// would have flushed. Without the teardown barrier a conforming durable engine reboots with the
/// acked removal rolled back — a retired incarnation free to return. The success verdict must
/// therefore be released only behind a barrier that ran, and the loop must not exit owning staged
/// work.
#[compio::test]
async fn a_removal_is_acked_only_behind_the_barrier_that_covers_it() {
  // Port 0: this host is PEERLESS, so nothing needs to find it — let the OS pick and keep the
  // suite free of fixed-port collisions.
  let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
  let gid = 100u64;
  let watch = Arc::new(EngineWatch::default());
  let (dialer, acceptor) = plain_factories(1);
  let (driver, handle) = CompioMultiStreamDriver::<u64, u64, CountSm, _, _>::bind_with_engine(
    addr,
    Vec::new(),
    dialer,
    acceptor,
    DriverConfig::default(),
    DelegatingEngine::new(watch.clone()),
  )
  .await
  .expect("the host binds over a foreign engine");
  compio::runtime::spawn(driver.run()).detach();

  // Generation 3 makes this a RESHAPED id, so its removal floor is non-zero — the write whose
  // loss is a safety rollback rather than a re-runnable inconvenience.
  handle
    .create_group(gid, config(1, vec![1]), 1, CountSm::default(), 3)
    .await
    .expect("group admission");
  let before = watch.barriers.load(Ordering::SeqCst);

  // PRIME THE DRAIN. An idle driver is parked in its select, whose command arm takes exactly ONE
  // command and then cranks — so two queued commands would be handled a crank apart, and the
  // removal would flush normally. This throwaway `Status` absorbs that single wake, leaving the
  // two below to the bounded drain at the top of the NEXT iteration: handled together, with the
  // exit breaking out before any crank. That is the shape a `Shutdown` racing a removal has.
  let probe_handle = handle.group(gid);
  let mut probe = std::pin::pin!(probe_handle.status());
  assert!(
    futures_util::FutureExt::now_or_never(probe.as_mut()).is_none(),
    "the status probe parks, with its command already queued"
  );

  // Now the removal and the `Shutdown`, queued without ever yielding to the driver in between.
  let mut removal = std::pin::pin!(handle.remove_group(gid));
  assert!(
    futures_util::FutureExt::now_or_never(removal.as_mut()).is_none(),
    "the removal parks on its reply, with its command already queued"
  );
  handle.shutdown().await.expect("the multi host tears down");

  let removed = removal
    .await
    .expect("the removal verdict survives the teardown");
  assert!(removed, "the group was hosted");
  assert!(
    watch.barriers.load(Ordering::SeqCst) > before,
    "a barrier ran between the removal and its ack"
  );
  assert!(
    !watch.staged_at_drop.load(Ordering::SeqCst),
    "the loop did not exit owning staged work"
  );
}

/// The teardown barrier's other exit path: the LAST HANDLE DROPPED. The command channel
/// disconnects mid-drain, which leaves the loop exactly as a `Shutdown` does — with a removal's
/// floor staged and no crank left to flush it. Nobody is waiting for the verdict here, which is
/// precisely why the barrier cannot be left to the ack path to trigger.
#[compio::test]
async fn a_disconnected_channel_still_runs_the_teardown_barrier() {
  // Port 0: this host is PEERLESS, so nothing needs to find it — let the OS pick and keep the
  // suite free of fixed-port collisions.
  let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
  let gid = 100u64;
  let watch = Arc::new(EngineWatch::default());
  let (dialer, acceptor) = plain_factories(1);
  let (driver, handle) = CompioMultiStreamDriver::<u64, u64, CountSm, _, _>::bind_with_engine(
    addr,
    Vec::new(),
    dialer,
    acceptor,
    DriverConfig::default(),
    DelegatingEngine::new(watch.clone()),
  )
  .await
  .expect("the host binds over a foreign engine");
  let task = compio::runtime::spawn(driver.run());

  handle
    .create_group(gid, config(1, vec![1]), 1, CountSm::default(), 3)
    .await
    .expect("group admission");
  let before = watch.barriers.load(Ordering::SeqCst);

  // PRIME THE DRAIN. An idle driver is parked in its select, whose command arm takes exactly ONE
  // command and then cranks — so two queued commands would be handled a crank apart, and the
  // removal would flush normally. This throwaway `Status` absorbs that single wake, leaving the
  // two below to the bounded drain at the top of the NEXT iteration: handled together, with the
  // exit breaking out before any crank. That is the shape a `Shutdown` racing a removal has.
  //
  // Both futures and the probe's group projection live in this block and die at its end: the loop
  // exits on a DISCONNECTED channel, so one surviving sender clone would hold it open forever.
  // Nobody awaits the removal verdict here, which is exactly why the barrier cannot be left to
  // the ack path to trigger.
  {
    let probe_handle = handle.group(gid);
    let mut probe = std::pin::pin!(probe_handle.status());
    assert!(
      futures_util::FutureExt::now_or_never(probe.as_mut()).is_none(),
      "the status probe parks, with its command already queued"
    );
    let mut removal = std::pin::pin!(handle.remove_group(gid));
    assert!(
      futures_util::FutureExt::now_or_never(removal.as_mut()).is_none(),
      "the removal parks on its reply, with its command already queued"
    );
  }
  drop(handle);
  // The run loop returns nothing but its completion: awaiting it is how the test observes the
  // teardown, engine drop included.
  let _ = task.await;

  assert!(
    watch.barriers.load(Ordering::SeqCst) > before,
    "the disconnect exit ran its final barrier"
  );
  assert!(
    !watch.staged_at_drop.load(Ordering::SeqCst),
    "the loop did not exit owning staged work"
  );
}

/// The fork wedge on the compio plane — the reactor suite carries the full matrix (the squatter's
/// surviving content and the release half); what this pins here is the anti-loss half, which is
/// the one that cannot be allowed to differ between the two drivers. A committed NON-EMPTY split
/// whose child id is occupied by unrelated stores must be HELD, never abandoned: the parent has
/// already shrunk, so a refusal would lift its fence and drop the partition's only local copy.
#[compio::test]
async fn a_fork_wedges_on_occupied_child_stores() {
  // Port 0: this host is PEERLESS, so nothing needs to find it.
  let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
  let parent = 100u64;
  let child = 300u64;
  let watch = Arc::new(EngineWatch::default());
  let (dialer, acceptor) = plain_factories(1);
  let (driver, handle) = CompioMultiStreamDriver::<u64, u64, CountSm, _, _>::bind_with_engine(
    addr,
    Vec::new(),
    dialer,
    acceptor,
    DriverConfig::default(),
    // The squatter: unrelated stores under the CHILD id.
    DelegatingEngine::preloaded(preloaded_engine(&[child]), watch.clone()),
  )
  .await
  .expect("the host binds over a preloaded engine");
  let lifecycle = handle.lifecycle().clone();
  compio::runtime::spawn(driver.run()).detach();

  handle
    .create_group(parent, config(1, vec![1]), 1, CountSm::default(), 0)
    .await
    .expect("parent admission");
  let p = handle.group(parent);
  for i in 0..5u64 {
    assert_eq!(
      submit_anywhere(std::slice::from_ref(&p), b"unit").await,
      i + 1
    );
  }
  p.propose_split(child, 7, Bytes::from_static(&[2]))
    .await
    .expect("the split commits on the parent");

  // A `SplitRefused` here IS the loss; the hold announces itself with the conflict cue.
  let deadline = std::time::Instant::now() + Duration::from_secs(15);
  let mut held = false;
  while !held {
    assert!(
      std::time::Instant::now() < deadline,
      "the fork reached no verdict in time"
    );
    while let Ok(ev) = lifecycle.try_recv() {
      assert!(
        !matches!(&ev, LifecycleEvent::SplitRefused { parent: pp, child: cc } if *pp == parent && *cc == child),
        "THE LOSS: the fork was ABANDONED over an occupied id — the parent's fence lifted and \
         the partition blob went with it"
      );
      held |= matches!(&ev, LifecycleEvent::SplitConflict { parent: pp, child: cc } if *pp == parent && *cc == child);
    }
    compio::time::sleep(Duration::from_millis(20)).await;
  }
  assert!(
    handle.group(child).status().await.is_err(),
    "no child endpoint was built over the squatter's stores"
  );

  handle.shutdown().await.expect("the multi host tears down");
}
