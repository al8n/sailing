//! The cross-thread submission surface: [`Handle`] and the [`Command`]s it sends the driver.

use std::sync::{
  Arc,
  atomic::{AtomicBool, Ordering},
};

use sailing_proto::{ConfChange, ConfChangeV2, Data, Entry, Event, FailoverReadWindow, Index};

use crate::{
  DriverError,
  shared::{InflightBudget, ReservationGuard},
};

/// A control message from a [`Handle`] to the driver task.
///
/// Generic over the node id `I`, the state machine `F` (whose `Command`/`Response` types ride
/// the variants), so the typed payloads cross the thread boundary without serialization.
pub(crate) enum Command<I, F>
where
  F: sailing_proto::StateMachine,
{
  /// Propose a command; answer `reply` with the committed apply response.
  Submit {
    /// The typed command (the driver passes it to `submit_propose` by reference).
    cmd: F::Command,
    /// Answered when the entry APPLIES (or with the supersede/teardown error).
    reply: futures_channel::oneshot::Sender<Result<F::Response, DriverError<I>>>,
    /// The owning budget reservation: moved into the pending entry on drain, or dropped
    /// still-queued by a teardown — never released manually.
    reservation: ReservationGuard,
  },
  /// Propose a single-step membership change; answer `reply` with the applied index.
  Conf {
    /// The change.
    cc: ConfChange<I>,
    /// Answered when the change APPLIES (`ConfChanged` at its index).
    reply: futures_channel::oneshot::Sender<Result<Index, DriverError<I>>>,
    /// The owning budget reservation.
    reservation: ReservationGuard,
  },
  /// Propose a joint-consensus membership change; answer `reply` with the applied index.
  ConfV2 {
    /// The change.
    cc: ConfChangeV2<I>,
    /// Answered when the change APPLIES.
    reply: futures_channel::oneshot::Sender<Result<Index, DriverError<I>>>,
    /// The owning budget reservation.
    reservation: ReservationGuard,
  },
  /// A linearizable query: the driver runs `read_index`, parks the completion until the
  /// confirmed read index is applied, then calls it with `Ok(&F)` ON the driver thread.
  Query {
    /// The single completion (see [`ParkedQuery`](crate::shared::ParkedQuery)).
    complete: crate::shared::QueryComplete<I, F>,
    /// The owning budget reservation.
    reservation: ReservationGuard,
  },
  /// A failover inherited-read query: the driver offers the serve window (when armed and the
  /// inherited lease is provably live), parks until the committed prefix applies, then runs the
  /// closure with the FSM, the limbo entries, and the window — or completes `Ok(None)` when no serve
  /// window is available (the caller falls back to a normal read).
  FailoverWindow {
    /// The single completion (see [`FailoverComplete`](crate::shared::FailoverComplete)).
    complete: crate::shared::FailoverComplete<I, F>,
    /// The owning budget reservation (zero-byte).
    reservation: ReservationGuard,
  },
  /// Begin transferring leadership; answer `reply` with the immediate accept/reject.
  Transfer {
    /// The transfer target.
    to: I,
    /// Answered with the endpoint's immediate verdict (the transfer itself is asynchronous).
    reply: futures_channel::oneshot::Sender<Result<(), DriverError<I>>>,
    /// The owning budget reservation (zero-byte): transfers are budgeted like every other
    /// operation so handle clones cannot amplify unbudgeted commands into the channel.
    reservation: ReservationGuard,
  },
  /// Ask the driver to stop; `ack` is signalled once teardown completes and the socket fd is
  /// fully released (the bound address is immediately rebindable when the ack arrives).
  Shutdown {
    /// Signalled after the run loop exits and the socket is closed.
    ack: futures_channel::oneshot::Sender<()>,
  },
}

/// A cheaply-cloneable, `Send + Sync` handle to submit operations and observe events.
///
/// Cloning is O(1) (a channel handle plus `Arc` clones). All clones share one submit budget —
/// the in-flight count/byte caps apply across all clones, not per clone.
///
/// **The bound, precisely**: the command channel is `flume::unbounded`, so the channel itself is
/// NOT the bound. The BUDGET is: every operation except `shutdown` reserves against it before
/// enqueueing (a transfer reserves a zero-byte slot), so no number of clones can park more than
/// `max_inflight` budgeted commands. `shutdown` is deliberately exempt from the budget — a driver
/// must be stoppable under full load — but is COALESCED instead: a shared atomic flag (cloned across
/// every handle) lets only the FIRST `shutdown` enqueue a `Command::Shutdown`; every later call (and
/// every concurrent racer that lost the swap) returns early without enqueueing. So at most ONE
/// `Shutdown` is ever queued, regardless of clone count or a stalled driver — the unbounded channel
/// cannot be stormed by repeated `shutdown`s.
pub struct Handle<I, F>
where
  F: sailing_proto::StateMachine,
{
  /// The command sender. `flume::Sender::try_send` takes `&self` and is internally synchronized,
  /// so the public `&self` methods enqueue without a wrapping `Mutex`.
  commands: flume::Sender<Command<I, F>>,
  events: flume::Receiver<Event<I, F::Response>>,
  budget: InflightBudget,
  /// Shared across every clone: set once by the first `shutdown` to COALESCE all later (and
  /// concurrent) shutdown requests to a single enqueued `Command::Shutdown`, so the unbounded
  /// command channel can never be flooded with `Shutdown`s under a slow/stalled driver.
  shutdown: Arc<AtomicBool>,
}

impl<I, F> Clone for Handle<I, F>
where
  F: sailing_proto::StateMachine,
{
  fn clone(&self) -> Self {
    // The shared submit BUDGET is the binding bound on in-flight operations; the cloned
    // (unbounded) command sender adds no slack of its own. The shutdown flag is shared (Arc clone)
    // so a `shutdown` on ANY clone coalesces with one on any other.
    Self {
      commands: self.commands.clone(),
      events: self.events.clone(),
      budget: self.budget.clone(),
      shutdown: self.shutdown.clone(),
    }
  }
}

impl<I, F> Handle<I, F>
where
  I: sailing_proto::NodeId + Send,
  F: sailing_proto::StateMachine,
  F::Command: Data + Send,
  F::Response: Send,
{
  pub(crate) fn new(
    commands: flume::Sender<Command<I, F>>,
    events: flume::Receiver<Event<I, F::Response>>,
    budget: InflightBudget,
  ) -> Self {
    Self {
      commands,
      events,
      budget,
      shutdown: Arc::new(AtomicBool::new(false)),
    }
  }

  /// Enqueue one command, mapping a closed channel to [`DriverError::ShuttingDown`]. The budget
  /// reservation rides INSIDE `cmd`, so a rejected enqueue drops it here and nothing leaks.
  fn send(&self, cmd: Command<I, F>) -> Result<(), DriverError<I>> {
    self.commands.try_send(cmd).map_err(|e| {
      if matches!(e, flume::TrySendError::Disconnected(_)) {
        DriverError::ShuttingDown
      } else {
        // Unbounded: `Full` cannot occur, so this arm is defensively dead. The submit BUDGET is
        // the real backpressure signal (exhaustion fails fast as `Busy` before enqueue).
        DriverError::Busy
      }
    })
  }

  /// Propose a command and await its committed apply response.
  ///
  /// The byte cost reserved against the budget is the command's `Data` encoding length (the
  /// same bytes the entry will carry). Fails fast — without queueing — with
  /// [`DriverError::Busy`] when the budget is exhausted; fails with
  /// [`DriverError::NotLeader`] (redirect hint included) when this node cannot accept
  /// proposals; fails with [`DriverError::Superseded`] when a leadership change voided the
  /// outcome before it was known.
  pub async fn submit(&self, cmd: F::Command) -> Result<F::Response, DriverError<I>> {
    // Measure the payload for the byte budget. (One extra encode; the proto's own propose
    // encodes again. An `encoded_len` fast path on `Data` is tracked upstream.)
    let mut measure = Vec::new();
    cmd.encode(&mut measure);
    let reservation = self.budget.try_reserve(measure.len())?;
    let (tx, rx) = futures_channel::oneshot::channel();
    self.send(Command::Submit {
      cmd,
      reply: tx,
      reservation,
    })?;
    rx.await.map_err(|_| DriverError::ShuttingDown)?
  }

  /// Run a linearizable query against the state machine and await its result.
  ///
  /// The closure runs ON the driver thread, against the FSM, only after a `ReadIndex`
  /// confirmation AND the apply watermark covering the confirmed index — the linearizability
  /// point. The FSM never leaves the driver thread; the closure (and its result) cross instead.
  pub async fn query<Out, Q>(&self, f: Q) -> Result<Out, DriverError<I>>
  where
    Out: Send + 'static,
    Q: FnOnce(&F) -> Out + Send + 'static,
  {
    let reservation = self.budget.try_reserve(0)?;
    let (tx, rx) = futures_channel::oneshot::channel();
    let complete = Box::new(move |res: Result<&F, DriverError<I>>| {
      let _ = tx.send(res.map(f));
    });
    self.send(Command::Query {
      complete,
      reservation,
    })?;
    rx.await.map_err(|_| DriverError::ShuttingDown)?
  }

  /// Run an inherited-read query during the post-election failover commit-wait, awaiting its result.
  ///
  /// Under the LeaseGuard failover tier, a freshly elected leader may serve a linearizable read on the
  /// INHERITED committed prefix BEFORE its own commit-wait lifts — provided the read's key was not
  /// written in the limbo region `(index, limbo_upper]` (the proto is key-agnostic; the closure owns
  /// the command format). The closure runs ON the driver thread against the FSM and the limbo entries,
  /// returning `Some(out)` to serve or `None` to decline (e.g. its key IS in limbo). The call resolves
  /// to `Ok(None)` when there is no serve window — off the failover tier, the commit-wait already
  /// lifted (read normally), the inherited lease expired, or not the leader — or the closure declined;
  /// the caller then falls back to [`query`](Self::query). The FSM and the limbo entries never leave the
  /// driver thread; the closure (and its result) cross instead.
  pub async fn failover_query<Out, Q>(&self, f: Q) -> Result<Option<Out>, DriverError<I>>
  where
    Out: Send + 'static,
    Q: FnOnce(&F, &[Entry], FailoverReadWindow) -> Option<Out> + Send + 'static,
  {
    let reservation = self.budget.try_reserve(0)?;
    let (tx, rx) = futures_channel::oneshot::channel();
    let complete = Box::new(move |res: crate::shared::FailoverOutcome<'_, I, F>| {
      let _ = tx.send(res.map(|opt| opt.and_then(|(fsm, limbo, win)| f(fsm, limbo, win))));
    });
    self.send(Command::FailoverWindow {
      complete,
      reservation,
    })?;
    rx.await.map_err(|_| DriverError::ShuttingDown)?
  }

  /// Propose a single-step membership change and await its APPLIED index.
  pub async fn conf_change(&self, cc: ConfChange<I>) -> Result<Index, DriverError<I>>
  where
    I: Data,
  {
    let reservation = self.budget.try_reserve(0)?;
    let (tx, rx) = futures_channel::oneshot::channel();
    self.send(Command::Conf {
      cc,
      reply: tx,
      reservation,
    })?;
    rx.await.map_err(|_| DriverError::ShuttingDown)?
  }

  /// Propose a joint-consensus membership change and await its APPLIED index.
  pub async fn conf_change_v2(&self, cc: ConfChangeV2<I>) -> Result<Index, DriverError<I>>
  where
    I: Data,
  {
    let reservation = self.budget.try_reserve(0)?;
    let (tx, rx) = futures_channel::oneshot::channel();
    self.send(Command::ConfV2 {
      cc,
      reply: tx,
      reservation,
    })?;
    rx.await.map_err(|_| DriverError::ShuttingDown)?
  }

  /// Begin transferring leadership to `to`, awaiting the endpoint's IMMEDIATE verdict (the
  /// transfer itself completes asynchronously through an election).
  pub async fn transfer_leader(&self, to: I) -> Result<(), DriverError<I>> {
    let reservation = self.budget.try_reserve(0)?;
    let (tx, rx) = futures_channel::oneshot::channel();
    self.send(Command::Transfer {
      to,
      reply: tx,
      reservation,
    })?;
    rx.await.map_err(|_| DriverError::ShuttingDown)?
  }

  /// The best-effort events tail (committed entries, leader changes, conf changes, …). Bounded
  /// and dropped-on-full: an observer that falls behind loses events, never slows consensus.
  pub fn events(&self) -> &flume::Receiver<Event<I, F::Response>> {
    &self.events
  }

  /// Ask the driver to stop and await the teardown ack. When the ack arrives the run loop has
  /// exited and the socket fd is fully released — the bound address is immediately rebindable.
  ///
  /// IDEMPOTENT and COALESCED. The shared shutdown flag is swapped once: only the FIRST caller
  /// (across every clone, and across a concurrent race) enqueues a single `Command::Shutdown` and
  /// awaits its teardown ack; every subsequent caller returns `Ok(())` immediately without enqueueing
  /// — so the unbounded command channel can never be flooded with `Shutdown`s. A first-caller enqueue
  /// that finds the channel already disconnected likewise returns `Ok(())`: the driver is already gone,
  /// which is exactly what shutdown wants.
  pub async fn shutdown(&self) -> Result<(), DriverError<I>> {
    // Coalesce: only the caller that flips the flag false→true is responsible for the single enqueue;
    // everyone else (already-shutting-down, or a concurrent racer that lost the swap) is done. The flag
    // stays set on every path, so a later call never re-enqueues.
    if self.shutdown.swap(true, Ordering::AcqRel) {
      return Ok(());
    }
    let (tx, rx) = futures_channel::oneshot::channel();
    // A disconnected channel means the driver already exited — shutdown is satisfied, so map it to
    // `Ok(())` rather than `ShuttingDown`. (`Full` cannot occur on an unbounded channel.)
    if let Err(e) = self.commands.try_send(Command::Shutdown { ack: tx }) {
      return match e {
        flume::TrySendError::Disconnected(_) => Ok(()),
        flume::TrySendError::Full(_) => Err(DriverError::Busy),
      };
    }
    // Only the enqueuer awaits the teardown ack (unchanged behavior): a dropped ack sender (driver
    // gone without signalling) surfaces as `ShuttingDown`.
    rx.await.map_err(|_| DriverError::ShuttingDown)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

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

  /// The shutdown-coalescing flag is an `Arc<AtomicBool>` (`Send + Sync`), so the cross-thread
  /// `Handle` stays `Send + Sync` as documented. A regression here is a compile error.
  const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<TestHandle>();
  };

  /// The handle under test, the command RECEIVER (the stub driver's end), and the held event SENDER
  /// (kept alive only so the handle's event receiver is not pre-disconnected — these tests never use
  /// the event tail).
  fn test_handle() -> (TestHandle, CmdRx, EventTx) {
    let (cmd_tx, cmd_rx) = flume::unbounded();
    let (event_tx, event_rx) = flume::bounded(1);
    let budget = InflightBudget::new(8, 64);
    let handle = Handle::new(cmd_tx, event_rx, budget);
    (handle, cmd_rx, event_tx)
  }

  fn count_shutdowns(rx: &CmdRx) -> usize {
    let mut n = 0;
    while let Ok(cmd) = rx.try_recv() {
      if matches!(cmd, Command::Shutdown { .. }) {
        n += 1;
      }
    }
    n
  }

  #[test]
  fn shutdown_coalesces_to_a_single_command_across_clones() {
    futures_executor::block_on(async {
      let (handle, cmd_rx, _event_tx) = test_handle();
      let clones: Vec<TestHandle> = (0..8).map(|_| handle.clone()).collect();

      // A stub driver: drain the (single) coalesced `Shutdown`, count it, and ack it so the one
      // awaiting caller resolves; the channel then stays empty for the rest of the join.
      let count = std::cell::Cell::new(0usize);
      let driver = async {
        while let Ok(cmd) = cmd_rx.recv_async().await {
          if let Command::Shutdown { ack } = cmd {
            count.set(count.get() + 1);
            let _ = ack.send(());
          }
        }
      };

      // Every handle (original + clones) calls `shutdown`; all must resolve `Ok`. Only the FIRST to
      // win the atomic swap enqueues + awaits the ack (the stub driver, running in the same join,
      // supplies it); the rest return `Ok` immediately without touching the channel — so ordering of
      // the calls does not matter to the coalescing, which the single-channel assertion below proves.
      let callers = async {
        assert!(handle.shutdown().await.is_ok(), "first shutdown must be Ok");
        for c in &clones {
          assert!(
            c.shutdown().await.is_ok(),
            "every later shutdown caller must return Ok"
          );
        }
        // All callers done: drop every sender so the stub driver's recv disconnects and the join
        // completes.
        drop(handle);
        drop(clones);
      };

      futures_util::future::join(driver, callers).await;

      // Exactly ONE `Shutdown` was ever enqueued, regardless of the 9 concurrent callers.
      assert_eq!(
        count.get(),
        1,
        "the unbounded channel must carry at most one coalesced Shutdown"
      );
      // And nothing is left queued.
      assert_eq!(count_shutdowns(&cmd_rx), 0);
    });
  }

  #[test]
  fn shutdown_is_idempotent_after_completion() {
    futures_executor::block_on(async {
      let (handle, cmd_rx, _event_tx) = test_handle();

      // First shutdown: enqueues one `Shutdown`; a stub driver acks it.
      let first = async {
        let count = std::cell::Cell::new(0usize);
        let driver = async {
          while let Ok(cmd) = cmd_rx.recv_async().await {
            if let Command::Shutdown { ack } = cmd {
              count.set(count.get() + 1);
              let _ = ack.send(());
              break;
            }
          }
        };
        let call = async { handle.shutdown().await.unwrap() };
        futures_util::future::join(driver, call).await;
        count.get()
      }
      .await;
      assert_eq!(first, 1, "the first shutdown enqueues exactly one Shutdown");

      // A SUBSEQUENT shutdown on the same handle returns Ok WITHOUT enqueueing anything.
      handle.shutdown().await.unwrap();
      assert_eq!(
        count_shutdowns(&cmd_rx),
        0,
        "a second shutdown must not enqueue another Shutdown"
      );
    });
  }

  #[test]
  fn shutdown_on_disconnected_channel_is_ok() {
    futures_executor::block_on(async {
      let (handle, cmd_rx, _event_tx) = test_handle();
      // Driver already gone: the command receiver is dropped, so the first enqueue sees Disconnected.
      drop(cmd_rx);
      // Shutdown still resolves Ok (the driver is already stopped — exactly what shutdown wants).
      handle.shutdown().await.unwrap();
      // And a second call is still Ok (flag set, early return).
      handle.shutdown().await.unwrap();
    });
  }
}
