use super::*;

use std::task::Poll;

use crate::shared::InflightBudget;

/// A do-nothing state machine, just enough to parametrize a `Handle`/`Command` for the
/// channel-coalescing tests. No command ever applies — the tests only exercise `shutdown`.
struct NoopSm;

impl sailing_proto::StateMachine for NoopSm {
  type Command = ();
  type Response = ();
  type Snapshot = ();
  type Error = std::convert::Infallible;

  fn apply(
    &mut self,
    _index: sailing_proto::Index,
    _cmd: (),
  ) -> Result<(), std::convert::Infallible> {
    Ok(())
  }
  fn snapshot(&self) -> Result<(), std::convert::Infallible> {
    Ok(())
  }
  fn restore(&mut self, _snapshot: ()) -> Result<(), std::convert::Infallible> {
    Ok(())
  }
}

type TestHandle = Handle<u64, NoopSm>;
type CmdRx = flume::Receiver<Command<u64, NoopSm>>;
type EventTx = flume::Sender<Event<u64, ()>>;

/// The shutdown-coalescing flag is an `Arc<AtomicBool>` and the teardown receiver is a
/// `Shared<oneshot::Receiver<()>>` — both `Send + Sync` — so the cross-thread `Handle` stays
/// `Send + Sync` as documented. A regression here is a compile error.
const _: fn() = || {
  fn assert_send_sync<T: Send + Sync>() {}
  assert_send_sync::<TestHandle>();
};

/// The handle under test, the command RECEIVER (the stub driver's end), the held event SENDER (kept
/// alive only so the handle's event receiver is not pre-disconnected — these tests never use the
/// event tail), and the teardown SENDER the driver would normally hold. The test plays the driver:
/// it fires (or drops) the teardown sender to release every awaiting `shutdown()`, exactly as the
/// real driver does after its `close().await`.
fn test_handle() -> (TestHandle, CmdRx, EventTx, oneshot::Sender<()>) {
  let (cmd_tx, cmd_rx) = flume::unbounded();
  let (event_tx, event_rx) = flume::bounded(1);
  let (teardown_tx, teardown_rx) = oneshot::channel();
  let budget = InflightBudget::new(8, 64);
  let handle = Handle::new(cmd_tx, event_rx, budget, teardown_rx);
  (handle, cmd_rx, event_tx, teardown_tx)
}

fn count_shutdowns(rx: &CmdRx) -> usize {
  let mut n = 0;
  while let Ok(cmd) = rx.try_recv() {
    if matches!(cmd, Command::Shutdown) {
      n += 1;
    }
  }
  n
}

/// The decisive coalescing + completion test: many `Handle` clones call `shutdown()` concurrently;
/// EXACTLY ONE `Command::Shutdown` is enqueued (the request dedup), and — the part the early-return
/// bug got wrong — EVERY caller (the swap winner AND every swap-loser) stays pending until the ONE
/// shared teardown signal fires, then all resolve `Ok`. Holding teardown and asserting the futures
/// are still pending proves a loser does not resolve early.
#[test]
fn shutdown_coalesces_and_every_caller_awaits_teardown() {
  futures_executor::block_on(async {
    let (handle, cmd_rx, _event_tx, teardown_tx) = test_handle();
    let clones: Vec<TestHandle> = (0..8).map(|_| handle.clone()).collect();

    // Nine concurrent shutdown futures (original + 8 clones), driven together.
    let mut futs: Vec<_> = std::iter::once(&handle)
      .chain(clones.iter())
      .map(|h| Box::pin(h.shutdown()))
      .collect();

    // Poll each once: the winner enqueues the single `Shutdown`, every loser skips the enqueue, and
    // ALL of them then park on the (un-fired) shared teardown — none may resolve yet.
    for f in &mut futs {
      assert!(
        matches!(futures_util::poll!(f.as_mut()), Poll::Pending),
        "no shutdown caller may resolve before teardown fires"
      );
    }
    // Exactly ONE `Shutdown` reached the channel despite nine callers — the request dedup. (Drains
    // the channel, so the count is also asserted to stay one.)
    assert_eq!(
      count_shutdowns(&cmd_rx),
      1,
      "the unbounded channel must carry exactly one coalesced Shutdown"
    );

    // The driver releases the fd and fires teardown: now every caller resolves `Ok`.
    teardown_tx
      .send(())
      .expect("the shared receiver is still alive");
    for f in &mut futs {
      assert!(
        f.await.is_ok(),
        "every shutdown caller resolves Ok once teardown completes"
      );
    }
  });
}

/// A losing caller (one that did NOT win the enqueue swap) must not resolve until the shared
/// teardown fires — the immediate-rebind contract. The first call wins the swap and enqueues; a
/// second concurrent call loses it; while teardown is held BOTH are pending; firing teardown
/// resolves both.
#[test]
fn a_losing_caller_waits_for_teardown_not_an_early_ok() {
  futures_executor::block_on(async {
    let (handle, _cmd_rx, _event_tx, teardown_tx) = test_handle();
    let clone = handle.clone();

    let mut winner = Box::pin(handle.shutdown());
    let mut loser = Box::pin(clone.shutdown());

    // Winner polled first wins the swap and enqueues; the loser then loses the swap. Neither may
    // resolve while teardown is held.
    assert!(matches!(
      futures_util::poll!(winner.as_mut()),
      Poll::Pending
    ));
    assert!(
      matches!(futures_util::poll!(loser.as_mut()), Poll::Pending),
      "the swap-loser must block on teardown, not return an early Ok"
    );

    // Fire teardown: both the winner and the loser complete.
    teardown_tx.send(()).expect("receiver alive");
    assert!(winner.await.is_ok());
    assert!(
      loser.await.is_ok(),
      "the loser resolves only after teardown"
    );
  });
}

/// A subsequent shutdown after one has completed is idempotent: it enqueues NOTHING (the flag is
/// already set) and resolves `Ok` immediately off the already-fired shared teardown.
#[test]
fn shutdown_is_idempotent_after_completion() {
  futures_executor::block_on(async {
    let (handle, cmd_rx, _event_tx, teardown_tx) = test_handle();

    // First shutdown: enqueues one `Shutdown`, then completes once the driver fires teardown.
    let mut first = Box::pin(handle.shutdown());
    assert!(matches!(futures_util::poll!(first.as_mut()), Poll::Pending));
    assert_eq!(
      count_shutdowns(&cmd_rx),
      1,
      "the first shutdown enqueues exactly one Shutdown"
    );
    teardown_tx.send(()).expect("receiver alive");
    first.await.unwrap();

    // A SUBSEQUENT shutdown enqueues nothing and resolves at once: the flag is set (no enqueue) and
    // the shared teardown already completed.
    handle.shutdown().await.unwrap();
    assert_eq!(
      count_shutdowns(&cmd_rx),
      0,
      "a second shutdown must not enqueue another Shutdown"
    );
  });
}

/// A DROPPED teardown sender (`Canceled`) is NOT a clean teardown: it means the driver future was
/// aborted before its post-`close()` explicit fire, so the fd-release is unproven. `shutdown()` must
/// surface that as `Err(ShuttingDown)`, never a rebind-safe `Ok` — otherwise a caller could rebind into
/// an `AddrInUse` race after an aborted driver.
#[test]
fn aborted_teardown_is_shutting_down_not_a_rebind_safe_ok() {
  futures_executor::block_on(async {
    let (handle, cmd_rx, _event_tx, teardown_tx) = test_handle();
    // Driver aborted: drop the command receiver AND the teardown sender WITHOUT an explicit fire (the
    // future was dropped before reaching the post-`close()` tail).
    drop(cmd_rx);
    drop(teardown_tx);
    // The dropped sender ⇒ `Canceled` ⇒ the fd-release was NOT proven ⇒ `ShuttingDown`, not `Ok`.
    assert!(matches!(
      handle.shutdown().await,
      Err(DriverError::ShuttingDown)
    ));
    // A second call is consistent: flag set (no enqueue), the same aborted teardown ⇒ Err.
    assert!(matches!(
      handle.shutdown().await,
      Err(DriverError::ShuttingDown)
    ));
  });
}

/// A disconnected channel whose driver has NOT yet signalled teardown still makes the caller WAIT:
/// the contract is "resolve only after the fd is released", so even on a disconnected enqueue the
/// caller parks on teardown until it fires.
#[test]
fn disconnected_enqueue_still_blocks_until_teardown() {
  futures_executor::block_on(async {
    let (handle, cmd_rx, _event_tx, teardown_tx) = test_handle();
    drop(cmd_rx); // the enqueue will see Disconnected
    let mut fut = Box::pin(handle.shutdown());
    // Teardown not yet fired: the disconnected-enqueue caller must NOT resolve early.
    assert!(
      matches!(futures_util::poll!(fut.as_mut()), Poll::Pending),
      "a disconnected enqueue still awaits teardown completion"
    );
    teardown_tx.send(()).expect("receiver alive");
    fut.await.unwrap();
  });
}
