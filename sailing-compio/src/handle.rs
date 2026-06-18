//! The cross-thread submission surface: [`Handle`] and the [`Command`]s it sends the driver.

use std::sync::{Mutex, MutexGuard, PoisonError};

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
/// **The bound, precisely**: the command channel's true capacity is its buffer
/// (`max_inflight + 1`) plus one in-flight slot per live sender clone — a property of the
/// bounded mpsc, so the channel alone is NOT the bound. The BUDGET is: every operation except
/// `shutdown` reserves against it before enqueueing (a transfer reserves a zero-byte slot), so
/// no number of clones can park more than `max_inflight` budgeted commands. `shutdown` is
/// deliberately exempt — a driver must be stoppable under full load — and is bounded by being
/// terminal (the first one drained exits the loop).
pub struct Handle<I, F>
where
  F: sailing_proto::StateMachine,
{
  /// The command sender, `Mutex`-wrapped because `futures_channel::mpsc::Sender::try_send`
  /// takes `&mut self` (the sender tracks its own parked state) while the methods here take
  /// `&self`. The lock is held only across a non-blocking `try_send`/`clone` — never across an
  /// await — so callers sharing one clone by reference serialize only the enqueue itself.
  commands: Mutex<futures_channel::mpsc::Sender<Command<I, F>>>,
  events: flume::Receiver<Event<I, F::Response>>,
  budget: InflightBudget,
}

impl<I, F> Clone for Handle<I, F>
where
  F: sailing_proto::StateMachine,
{
  fn clone(&self) -> Self {
    // A cloned sender starts fresh (unparked) and carries its own guaranteed channel slot: the
    // command channel admits its buffer plus one in-flight command per live sender, so each
    // clone widens the queue's slack by one. The shared submit BUDGET is the binding bound on
    // in-flight operations, not that slack.
    let sender = self
      .commands
      .lock()
      .unwrap_or_else(PoisonError::into_inner)
      .clone();
    Self {
      commands: Mutex::new(sender),
      events: self.events.clone(),
      budget: self.budget.clone(),
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
    commands: futures_channel::mpsc::Sender<Command<I, F>>,
    events: flume::Receiver<Event<I, F::Response>>,
    budget: InflightBudget,
  ) -> Self {
    Self {
      commands: Mutex::new(commands),
      events,
      budget,
    }
  }

  /// Lock the command sender. A poisoned lock only means another thread panicked while holding
  /// it; the sender is a plain channel handle whose state cannot be torn by an unwind
  /// mid-`try_send`, so the inner value is taken either way rather than cascading the panic
  /// into every clone.
  fn commands(&self) -> MutexGuard<'_, futures_channel::mpsc::Sender<Command<I, F>>> {
    self.commands.lock().unwrap_or_else(PoisonError::into_inner)
  }

  /// Enqueue one command, mapping a full/closed channel to the right error. The budget
  /// reservation rides INSIDE `cmd`, so a rejected enqueue drops it here and nothing leaks.
  fn send(&self, cmd: Command<I, F>) -> Result<(), DriverError<I>> {
    self.commands().try_send(cmd).map_err(|e| {
      if e.is_disconnected() {
        DriverError::ShuttingDown
      } else {
        // The channel buffer is sized to the submit budget, so a full channel without a
        // disconnected driver means commands are racing ahead of the drain — the same
        // backpressure signal as budget exhaustion.
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
  pub async fn shutdown(&self) -> Result<(), DriverError<I>> {
    let (tx, rx) = futures_channel::oneshot::channel();
    self.send(Command::Shutdown { ack: tx })?;
    rx.await.map_err(|_| DriverError::ShuttingDown)
  }
}
