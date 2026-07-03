use std::task::Poll;

use bytes::Bytes;

use super::*;
use crate::shared::InflightBudget;

/// A counting state machine with a real `Data` command, so `submit`'s byte-budget measurement is
/// exercised (the shutdown-only single-group tests get away with a unit command; these cannot).
struct CountSm;

impl sailing_proto::StateMachine for CountSm {
  type Command = Bytes;
  type Response = u64;
  type Snapshot = u64;
  type Error = std::convert::Infallible;

  fn apply(&mut self, _index: sailing_proto::Index, _cmd: Bytes) -> Result<u64, Self::Error> {
    Ok(0)
  }
  fn snapshot(&self) -> Result<u64, Self::Error> {
    Ok(0)
  }
  fn restore(&mut self, _snapshot: u64) -> Result<(), Self::Error> {
    Ok(())
  }
}

type TestHandle = MultiHandle<u64, u64, CountSm>;
type CmdRx = flume::Receiver<MultiCommand<u64, u64, CountSm>>;
type EventTx = flume::Sender<(u64, Event<u64, u64>)>;
type LifecycleTx = flume::Sender<LifecycleEvent<u64, u64>>;

/// Both the multi handle and its group projection cross threads; a regression in either's
/// `Send + Sync` composition is a compile error here.
const _: fn() = || {
  fn assert_send_sync<T: Send + Sync>() {}
  assert_send_sync::<TestHandle>();
  assert_send_sync::<GroupHandle<u64, u64, CountSm>>();
};

/// The handle under test plus the stub driver's ends: the command receiver, the held event and
/// lifecycle senders (so neither tail is pre-disconnected), and the teardown sender the driver
/// would fire.
fn test_handle(
  budget: InflightBudget,
) -> (TestHandle, CmdRx, EventTx, LifecycleTx, oneshot::Sender<()>) {
  let (cmd_tx, cmd_rx) = flume::unbounded();
  let (event_tx, event_rx) = flume::bounded(4);
  let (lifecycle_tx, lifecycle_rx) = flume::bounded(4);
  let (teardown_tx, teardown_rx) = oneshot::channel();
  let handle = MultiHandle::new(cmd_tx, event_rx, lifecycle_rx, budget, teardown_rx);
  (handle, cmd_rx, event_tx, lifecycle_tx, teardown_tx)
}

fn config() -> sailing_proto::Config<u64> {
  sailing_proto::Config::try_new(
    1u64,
    vec![1u64],
    core::time::Duration::from_millis(1000),
    core::time::Duration::from_millis(100),
  )
  .unwrap()
}

/// A `GroupHandle` stamps EVERY operation with its projected group id — the demux key the driver
/// routes by — while riding the parent's one command channel.
#[test]
fn group_projection_stamps_the_group_id() {
  futures_executor::block_on(async {
    let (handle, cmd_rx, _event_tx, _lifecycle_tx, _teardown_tx) =
      test_handle(InflightBudget::new(8, 64));
    let g7 = handle.group(7);
    assert_eq!(*g7.group_id(), 7);

    // Park a submit (no driver runs; the future stays pending after enqueueing).
    let mut fut = Box::pin(g7.submit(Bytes::from_static(b"x")));
    assert!(matches!(futures_util::poll!(fut.as_mut()), Poll::Pending));
    match cmd_rx.try_recv().expect("the submit was enqueued") {
      MultiCommand::Submit { group, cmd, .. } => {
        assert_eq!(group, 7, "the projection's group id rides the command");
        assert_eq!(&cmd[..], b"x");
      }
      _ => panic!("expected a Submit"),
    }

    // A second projection of the SAME handle addresses a different group over the same channel.
    let g9 = handle.group(9);
    let mut fut9 = Box::pin(g9.status());
    assert!(matches!(futures_util::poll!(fut9.as_mut()), Poll::Pending));
    match cmd_rx.try_recv().expect("the status was enqueued") {
      MultiCommand::Status { group, .. } => assert_eq!(group, 9),
      _ => panic!("expected a Status"),
    }
  });
}

/// ONE budget spans the multi handle and every group projection: reservations taken through one
/// group's projection exhaust the shared bound for every other group AND for the lifecycle
/// commands — no per-group fan-out can multiply the host's in-flight ceiling.
#[test]
fn budget_is_shared_across_group_projections_and_lifecycle() {
  futures_executor::block_on(async {
    let budget = InflightBudget::new(2, 1024);
    let (handle, cmd_rx, _event_tx, _lifecycle_tx, _teardown_tx) = test_handle(budget);
    let g1 = handle.group(1);
    let g2 = handle.group(2);

    // Two in-flight operations through two DIFFERENT projections fill the shared count budget.
    let mut q1 = Box::pin(g1.query(|_sm: &CountSm| 0u64));
    assert!(matches!(futures_util::poll!(q1.as_mut()), Poll::Pending));
    let mut q2 = Box::pin(g2.query(|_sm: &CountSm| 0u64));
    assert!(matches!(futures_util::poll!(q2.as_mut()), Poll::Pending));

    // A third operation — through yet another projection — fails fast, nothing queued.
    match handle.group(3).submit(Bytes::from_static(b"x")).await {
      Err(DriverError::Busy) => {}
      other => panic!("expected Busy on the shared budget, got {other:?}"),
    }
    // Lifecycle commands draw on the SAME budget.
    match handle.create_group(4, config(), 4, CountSm).await {
      Err(DriverError::Busy) => {}
      other => panic!("expected Busy for a lifecycle command too, got {other:?}"),
    }
    // Exactly the two admitted commands reached the channel.
    assert_eq!(cmd_rx.len(), 2);

    // Dropping a parked operation releases its slot (the reservation rides inside the command,
    // which is still queued — drain it) and the budget admits again.
    drop(q1);
    let _ = cmd_rx.try_recv();
    let _ = cmd_rx.try_recv();
    let mut q3 = Box::pin(g1.query(|_sm: &CountSm| 0u64));
    assert!(matches!(futures_util::poll!(q3.as_mut()), Poll::Pending));
  });
}

/// The lifecycle commands ride the shared channel with their typed payloads, and their replies
/// resolve the awaiting caller: `create_group` round-trips an admission verdict, `remove_group`
/// round-trips the was-hosted bool, and `clear_tombstone` round-trips the tombstone-existed bool.
#[test]
fn lifecycle_commands_round_trip_their_replies() {
  futures_executor::block_on(async {
    let (handle, cmd_rx, _event_tx, _lifecycle_tx, _teardown_tx) =
      test_handle(InflightBudget::new(8, 64));

    let mut create = Box::pin(handle.create_group(100, config(), 1, CountSm));
    assert!(matches!(
      futures_util::poll!(create.as_mut()),
      Poll::Pending
    ));
    match cmd_rx.try_recv().expect("the create was enqueued") {
      MultiCommand::CreateGroup {
        gid, seed, reply, ..
      } => {
        assert_eq!(gid, 100);
        assert_eq!(seed, 1);
        reply.send(Ok(())).expect("the caller is awaiting");
      }
      _ => panic!("expected a CreateGroup"),
    }
    create.await.expect("the admission verdict resolves");

    let mut remove = Box::pin(handle.remove_group(100));
    assert!(matches!(
      futures_util::poll!(remove.as_mut()),
      Poll::Pending
    ));
    match cmd_rx.try_recv().expect("the remove was enqueued") {
      MultiCommand::RemoveGroup { gid, reply, .. } => {
        assert_eq!(gid, 100);
        reply.send(true).expect("the caller is awaiting");
      }
      _ => panic!("expected a RemoveGroup"),
    }
    assert!(remove.await.expect("the remove resolves"));

    let mut clear = Box::pin(handle.clear_tombstone(100));
    assert!(matches!(futures_util::poll!(clear.as_mut()), Poll::Pending));
    match cmd_rx.try_recv().expect("the clear was enqueued") {
      MultiCommand::ClearTombstone { gid, reply, .. } => {
        assert_eq!(gid, 100);
        reply.send(true).expect("the caller is awaiting");
      }
      _ => panic!("expected a ClearTombstone"),
    }
    assert!(clear.await.expect("the clear resolves"));
  });
}

/// The multi handle inherits the single-group shutdown contract: many clones' `shutdown()`s
/// coalesce to ONE enqueued `Shutdown`, and every caller resolves only once the shared teardown
/// signal fires.
#[test]
fn shutdown_coalesces_and_awaits_teardown() {
  futures_executor::block_on(async {
    let (handle, cmd_rx, _event_tx, _lifecycle_tx, teardown_tx) =
      test_handle(InflightBudget::new(8, 64));
    let clones: Vec<TestHandle> = (0..4).map(|_| handle.clone()).collect();

    let mut futs: Vec<_> = std::iter::once(&handle)
      .chain(clones.iter())
      .map(|h| Box::pin(h.shutdown()))
      .collect();
    for f in &mut futs {
      assert!(
        matches!(futures_util::poll!(f.as_mut()), Poll::Pending),
        "no shutdown caller may resolve before teardown fires"
      );
    }
    let mut shutdowns = 0;
    while let Ok(cmd) = cmd_rx.try_recv() {
      if matches!(cmd, MultiCommand::Shutdown) {
        shutdowns += 1;
      }
    }
    assert_eq!(shutdowns, 1, "exactly one coalesced Shutdown was enqueued");

    teardown_tx.send(()).expect("receivers are alive");
    for f in &mut futs {
      assert!(f.await.is_ok(), "every caller resolves once teardown fires");
    }
  });
}

/// A dropped driver (disconnected channel) surfaces `ShuttingDown` on every surface — projections
/// and lifecycle alike — and a shutdown whose teardown sender DROPPED (an aborted driver) reports
/// `ShuttingDown` rather than a rebind-safe `Ok`.
#[test]
fn disconnected_driver_is_shutting_down_everywhere() {
  futures_executor::block_on(async {
    let (handle, cmd_rx, _event_tx, _lifecycle_tx, teardown_tx) =
      test_handle(InflightBudget::new(8, 64));
    let group = handle.group(1);
    drop(cmd_rx);

    match group.submit(Bytes::from_static(b"x")).await {
      Err(DriverError::ShuttingDown) => {}
      other => panic!("expected ShuttingDown, got {other:?}"),
    }
    match handle.create_group(2, config(), 2, CountSm).await {
      Err(DriverError::ShuttingDown) => {}
      other => panic!("expected ShuttingDown, got {other:?}"),
    }
    // The teardown sender dropping (driver aborted before its fd-release tail) maps the awaited
    // shutdown to ShuttingDown, not Ok.
    drop(teardown_tx);
    match handle.shutdown().await {
      Err(DriverError::ShuttingDown) => {}
      other => panic!("expected ShuttingDown from an aborted teardown, got {other:?}"),
    }
  });
}

/// The lifecycle tail hands the driver's placement signals to the embedder verbatim and in
/// order, and clones share the one tail — any clone can host the placement brain.
#[test]
fn lifecycle_tail_delivers_signals_to_any_clone() {
  let (handle, _cmd_rx, _event_tx, lifecycle_tx, _teardown_tx) =
    test_handle(InflightBudget::new(8, 64));
  lifecycle_tx
    .try_send(LifecycleEvent::UnknownGroup { group: 7, from: 3 })
    .expect("the tail has room");
  lifecycle_tx
    .try_send(LifecycleEvent::RemovedSelf { group: 9 })
    .expect("the tail has room");
  let clone = handle.clone();
  assert_eq!(
    clone.lifecycle().try_recv().expect("the first signal"),
    LifecycleEvent::UnknownGroup { group: 7, from: 3 }
  );
  assert_eq!(
    clone.lifecycle().try_recv().expect("the second signal"),
    LifecycleEvent::RemovedSelf { group: 9 }
  );
}
