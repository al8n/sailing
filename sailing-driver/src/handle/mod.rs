//! The cross-thread submission surface: [`Handle`] and the [`Command`]s it sends the driver.

use std::sync::{
  Arc,
  atomic::{AtomicBool, Ordering},
};

use futures_channel::oneshot;
use futures_util::{FutureExt, future::Shared};
use sailing_proto::{
  ConfChange, ConfChangeV2, ConfState, Data, Entry, Event, FailoverReadWindow, Index,
  ReadOnlyOption, Role, Term,
};

use crate::{
  DriverError,
  shared::{InflightBudget, ReservationGuard},
};

/// A control message from a [`Handle`] to the driver task.
///
/// Generic over the node id `I`, the state machine `F` (whose `Command`/`Response` types ride
/// the variants), so the typed payloads cross the thread boundary without serialization.
pub enum Command<I, F>
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
  /// Migrate the cluster-wide read mode; answer `reply` with the leader's immediate verdict (the
  /// proposed log index). The migration takes effect apply-time on every node once the entry commits.
  SetReadMode {
    /// The target read mode.
    mode: ReadOnlyOption,
    /// Answered with the endpoint's immediate verdict: the proposed log index, or the rejection.
    reply: futures_channel::oneshot::Sender<Result<Index, DriverError<I>>>,
    /// The owning budget reservation (zero-byte): migrations are budgeted like every other operation
    /// so handle clones cannot amplify unbudgeted commands into the channel.
    reservation: ReservationGuard,
  },
  /// Ask the driver to stop. A plain signal: every `shutdown()` caller observes COMPLETION through
  /// the shared teardown channel (fired after the fd-release barrier), not a per-command ack, so this
  /// variant carries no reply.
  Shutdown,
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
/// every concurrent racer that lost the swap) skips the enqueue. So at most ONE `Shutdown` is ever
/// queued, regardless of clone count or a stalled driver — the unbounded channel cannot be stormed by
/// repeated `shutdown`s. The flag dedups the REQUEST; COMPLETION is observed separately, through the
/// shared teardown channel below, so a caller that skips the enqueue STILL awaits real teardown.
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
  /// The COMPLETION half of shutdown, separate from the request-dedup flag above. The driver holds
  /// the matching `oneshot::Sender` and fires it AFTER its fd-release barrier (`close().await`), on
  /// every run-loop exit; this `Shared` receiver fans that one signal out to every `Handle` clone, so
  /// EVERY `shutdown()` caller — the enqueuing winner, a swap-loser, and the disconnected-channel path
  /// — resolves only once the socket fd is released. A fired send or the sender dropping (driver gone
  /// without an explicit fire) both mean teardown finished, so both `Ok(())` and `Err(Canceled)` map to
  /// a satisfied shutdown.
  teardown: Shared<oneshot::Receiver<()>>,
}

impl<I, F> Clone for Handle<I, F>
where
  F: sailing_proto::StateMachine,
{
  fn clone(&self) -> Self {
    // The shared submit BUDGET is the binding bound on in-flight operations; the cloned
    // (unbounded) command sender adds no slack of its own. The shutdown flag is shared (Arc clone)
    // so a `shutdown` on ANY clone coalesces with one on any other; the teardown receiver is a
    // `Shared` clone so every clone awaits the SAME single teardown-completion signal.
    Self {
      commands: self.commands.clone(),
      events: self.events.clone(),
      budget: self.budget.clone(),
      shutdown: self.shutdown.clone(),
      teardown: self.teardown.clone(),
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
  /// Build a handle. `teardown` is the receiver half of the driver's teardown-completion oneshot
  /// (the driver keeps the sender and fires it after its fd-release barrier); it is stored as a
  /// `Shared` so every clone awaits the one signal.
  pub fn new(
    commands: flume::Sender<Command<I, F>>,
    events: flume::Receiver<Event<I, F::Response>>,
    budget: InflightBudget,
    teardown: oneshot::Receiver<()>,
  ) -> Self {
    Self {
      commands,
      events,
      budget,
      shutdown: Arc::new(AtomicBool::new(false)),
      teardown: teardown.shared(),
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

  /// Migrate the cluster-wide read mode, awaiting the leader's IMMEDIATE verdict.
  ///
  /// On `Ok(index)` the `SetReadMode` entry was appended at `index`; the migration takes effect
  /// APPLY-TIME on every node once it commits (observe
  /// [`Event::ReadModeChanged`](sailing_proto::Event) on the [`events`](Self::events) tail, or poll
  /// [`status`](Self::status)). Fails [`NotLeader`](DriverError::NotLeader) off the leader, or
  /// [`Rejected`](DriverError::Rejected) when a migration is already in flight or this leader lacks
  /// the target mode's required knobs.
  pub async fn set_read_mode(&self, mode: ReadOnlyOption) -> Result<Index, DriverError<I>> {
    let reservation = self.budget.try_reserve(0)?;
    let (tx, rx) = futures_channel::oneshot::channel();
    self.send(Command::SetReadMode {
      mode,
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

  /// Ask the driver to stop and await teardown completion. On `Ok(())` the run loop has exited and the
  /// socket fd is fully released — the bound address is immediately rebindable. `Err(ShuttingDown)` means
  /// the driver future was ABORTED before its fd-release tail ran (it was dropped, not stopped through
  /// this path), so rebind-safety is NOT proven.
  ///
  /// IDEMPOTENT and COALESCED, with request-dedup and completion-notification SEPARATED. The shared
  /// shutdown flag is swapped once so only the FIRST caller (across every clone, and across a
  /// concurrent race) enqueues a single `Command::Shutdown` — the unbounded command channel can never
  /// be flooded with `Shutdown`s. But EVERY caller — the enqueuing winner, a swap-loser, and a
  /// first-caller whose enqueue finds the channel already disconnected — then awaits the SAME shared
  /// teardown signal the driver fires AFTER its fd-release barrier. So no caller observes `Ok(())`
  /// before the fd is released, and an immediate rebind after ANY `shutdown().await` is safe — not just
  /// after the one call that happened to win the swap.
  pub async fn shutdown(&self) -> Result<(), DriverError<I>> {
    // Coalesce the ENQUEUE only: the caller that flips the flag false→true enqueues the single
    // `Command::Shutdown`; a racer that lost the swap (or an already-shutting-down later call) skips
    // the enqueue. A disconnected channel means the driver already exited — nothing to enqueue. In
    // every case the actual teardown is awaited below, so skipping the enqueue never means returning
    // early. (`Full` cannot occur on an unbounded channel; the defensively-dead arm maps to `Busy`.)
    if !self.shutdown.swap(true, Ordering::AcqRel)
      && let Err(flume::TrySendError::Full(_)) = self.commands.try_send(Command::Shutdown)
    {
      return Err(DriverError::Busy);
    }
    // Every caller awaits the one teardown-completion signal. The driver FIRES it (an explicit send)
    // only AFTER `close().await` — the fd-release barrier — so `Ok(())` proves the fd is released and an
    // immediate rebind is safe. `Err(Canceled)` means the sender DROPPED without that explicit send: the
    // driver future was aborted BEFORE reaching the post-close tail (possibly mid-`close()`), so the
    // fd-release is NOT proven. Surface that as `ShuttingDown` rather than a rebind-safe `Ok`, so a
    // caller never rebinds into an `AddrInUse` race after an aborted teardown.
    match self.teardown.clone().await {
      Ok(()) => Ok(()),
      Err(_aborted) => Err(DriverError::ShuttingDown),
    }
  }
}

#[cfg(test)]
mod tests;
