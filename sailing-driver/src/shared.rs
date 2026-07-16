//! Shared driver state: the submit budget, the pending maps, and the event routing — plus the
//! crate's MEMORY MODEL.
//!
//! Both drivers retain a small fixed set of channels and maps, each EXPLICITLY bounded so a
//! partitioned/slow cluster, a flooding peer, or a caller submitting faster than the cluster
//! commits cannot grow the driver's memory without bound. The complete inventory:
//!
//! | retained state           | bound                                                            |
//! |--------------------------|------------------------------------------------------------------|
//! | `pending` submit map     | `max_inflight` entries AND `max_pending_bytes` of payload (budget) |
//! | parked `query` closures  | same budget (a query reserves one zero-byte submit slot)          |
//! | command channel          | `max_inflight + 1` buffered commands, + one in-flight per live sender (`try_send`) |
//! | events channel           | `events_cap` (bounded best-effort; dropped-on-full)               |
//! | QUIC recv channel        | `recv_cap` datagrams (recv task `send_async` backpressure)        |
//! | stream inbound channel   | `inbound_cap` frames (bridge `send_async` backpressure)           |
//! | stream accept channel    | `accept_cap` sockets (accept task parks; kernel backlog overflows)|
//! | per-conn out-queue       | `max_outbound_backlog` bytes (byte-bounded on enqueue)            |
//! | connection table         | `max_conns` accepts + ≤ peer-book mesh dials (dials never refused) |
//! | dial-ready channel       | live dial count, itself bounded by `max_conns`                    |
//! | storage-ready channel    | drained-to-empty every iteration; carries a unit signal only      |
//!
//! The budget rows are what this module owns ([`InflightBudget`]); the rest are bounded at their
//! construction sites in the drivers and cross-referenced here. The budget closes the submit
//! path: a submit RESERVES before it is queued, so a caller cannot mint pending entries past the
//! cap — it gets [`DriverError::Busy`] instead of growing memory.

use std::{
  collections::BTreeMap,
  sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
  },
};

use sailing_proto::{EntriesRead, Entry, EntryKind, Event, FailoverReadWindow, Index, LogStore};

use crate::DriverError;

/// The shared in-flight submit budget (count AND payload bytes), cloned into every
/// [`Handle`](crate::Handle) — the caps apply across all clones, not per clone.
#[derive(Clone)]
pub struct InflightBudget {
  count: Arc<AtomicUsize>,
  bytes: Arc<AtomicUsize>,
  max_count: usize,
  max_bytes: usize,
}

impl InflightBudget {
  /// A budget admitting `max_count` in-flight slots and `max_bytes` of in-flight submit payload.
  pub fn new(max_count: usize, max_bytes: usize) -> Self {
    Self {
      count: Arc::new(AtomicUsize::new(0)),
      bytes: Arc::new(AtomicUsize::new(0)),
      max_count,
      max_bytes,
    }
  }

  /// Reserve one submit slot carrying `len` payload bytes, or fail with
  /// [`DriverError::Busy`] without queueing anything. The returned guard is the SINGLE owner of
  /// the reservation: it releases on `Drop` wherever the carrying command finally dies — moved
  /// into the pending entry on drain, completed with the reply, or dropped still-queued by a
  /// teardown — so no path can leak budget.
  pub fn try_reserve<I>(&self, len: usize) -> Result<ReservationGuard, DriverError<I>> {
    // Optimistic add + rollback on overshoot: both counters are independent saturating gates,
    // and a failed reservation must leave them exactly as found.
    let prev_count = self.count.fetch_add(1, Ordering::AcqRel);
    if prev_count >= self.max_count {
      self.count.fetch_sub(1, Ordering::AcqRel);
      return Err(DriverError::Busy);
    }
    let prev_bytes = self.bytes.fetch_add(len, Ordering::AcqRel);
    if prev_bytes.saturating_add(len) > self.max_bytes {
      self.bytes.fetch_sub(len, Ordering::AcqRel);
      self.count.fetch_sub(1, Ordering::AcqRel);
      return Err(DriverError::Busy);
    }
    Ok(ReservationGuard {
      budget: self.clone(),
      len,
    })
  }

  #[cfg(test)]
  pub(crate) fn in_flight(&self) -> (usize, usize) {
    (
      self.count.load(Ordering::Acquire),
      self.bytes.load(Ordering::Acquire),
    )
  }
}

/// The owning side of one budget reservation (see [`InflightBudget::try_reserve`]).
pub struct ReservationGuard {
  budget: InflightBudget,
  len: usize,
}

impl Drop for ReservationGuard {
  fn drop(&mut self) {
    self.budget.count.fetch_sub(1, Ordering::AcqRel);
    self.budget.bytes.fetch_sub(self.len, Ordering::AcqRel);
  }
}

/// What a parked operation is waiting for, keyed by log index.
pub enum Pending<I, R> {
  /// A `submit`: completed with the apply response by `Applied` at this index.
  Submit {
    /// Answered with the committed response (or the supersede error).
    reply: futures_channel::oneshot::Sender<Result<R, DriverError<I>>>,
    /// The budget reservation, released when this entry dies (with the reply, or swept).
    _reservation: ReservationGuard,
  },
  /// A `conf_change`: completed by `ConfChanged` at this index.
  Conf {
    /// Answered with the applied index (or the supersede error).
    reply: futures_channel::oneshot::Sender<Result<Index, DriverError<I>>>,
    /// The budget reservation (a conf change reserves a zero-byte slot).
    _reservation: ReservationGuard,
  },
}

/// Whether a query/failover completion DELIVERED its result or caught a user-closure panic. The
/// completion runs the user closure under `catch_unwind`; a caught panic replies `QueryPanicked`
/// to the caller AND reports `Panicked` here, so the invoking driver learns the read ran against —
/// and interior mutability could have TORN — a replicated state machine, and fail-stops rather than
/// serve possibly-divergent state. The `Err`/`Ok(None)` arms do not CALL the closure, but they DROP
/// it unused inside the same `catch_unwind` (`res.map(f)` on an `Err` consumes `f` without invoking
/// it), so a captured guard's `Drop` runs there and can panic too — ANY arm, served or not, can
/// report `Panicked`. WHICH state machine the tear reached is unknowable: the closure captured
/// arbitrary state, which may alias any hosted group's FSM. So on a multi-group host every such
/// report is PLANE-fatal, and the centralized completion latch folds them into ONE fail-stop of
/// every hosted group.
///
/// `#[must_use]` is the ENFORCEMENT. A discarded outcome is a silently dropped fail-stop, so the
/// COMPILER — not a convention, and not a grep that only ever matched one of the two call syntaxes —
/// enumerates every invocation that fails to consume its verdict. Ignoring one is correct in exactly
/// ONE shape, and it must say so at the site: the fail-stop is already decided and a second report
/// cannot change it.
///
/// A completion addressed to a group this host does NOT carry is the shape that LOOKS discardable and
/// is not. Being handed no state machine bounds nothing about what the closure TOUCHED: `Q` is
/// `Send + 'static` and captures whatever it likes, and [`StateMachine`](sailing_proto::StateMachine)
/// imposes no ownership or isolation constraint, so a guard captured for the missing group can alias
/// state a HOSTED group's replicated FSM shares and tear it in `Drop`. A driver cannot see what a
/// closure captured, so such a panic is UNATTRIBUTABLE — it names no group — and the multi drivers
/// treat it as PLANE-FATAL: every hosted group fail-stops, each surfacing its own poison. Consensus
/// safety outranks availability there; a plane of fail-stopped groups restarts from durable state,
/// while a group serving torn committed state cannot be recovered at all.
#[must_use = "a completion reports the fail-stop verdict: `Panicked` means the user closure — or the \
              guard it captured, dropped unused inside the same `catch_unwind` — could have torn SOME \
              hosted group's replicated state machine (the container cannot attribute WHICH), and \
              discarding it leaves a group serving possibly-divergent state"]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionOutcome {
  /// The completion ran to normal delivery — no user-closure panic was caught.
  Delivered,
  /// A user-closure panic was caught; the caller received `QueryPanicked`. On a multi-group host the
  /// driver must fail-stop the WHOLE plane — the closure (or its captured guard's `Drop`) can alias
  /// and tear ANY hosted group's state machine, not only the addressed one, so the tear is
  /// unattributable; a single-group host's one endpoint IS its plane.
  Panicked,
}

impl CompletionOutcome {
  /// Map a completion's caught-panic flag to the outcome: `Panicked` when the `catch_unwind` fired,
  /// else `Delivered`.
  pub(crate) const fn caught(panicked: bool) -> Self {
    if panicked {
      Self::Panicked
    } else {
      Self::Delivered
    }
  }
}

/// The type-erased completion of a linearizable query: called with `Ok(&F)` ON the driver
/// thread to run the query (the result ships through the captured channel), or with the error
/// that voided it — one closure, so the caller keeps full error fidelity across the erasure.
/// Reports whether a user-closure panic was caught (see [`CompletionOutcome`]).
pub type QueryComplete<I, F> =
  Box<dyn FnOnce(Result<&F, DriverError<I>>) -> CompletionOutcome + Send>;

/// The argument a [`FailoverComplete`] receives: the served `(FSM, limbo entries, window)` triple to
/// run the read against, `None` when no serve window is available (the caller falls back to a normal
/// read), or the error that voided the query.
pub type FailoverOutcome<'a, I, F> =
  Result<Option<(&'a F, &'a [Entry], FailoverReadWindow)>, DriverError<I>>;

/// The type-erased completion of a failover inherited-read query. Called ON the driver thread with the
/// [`FailoverOutcome`] — the served triple, `Ok(None)`, or the error that voided it — one closure, so
/// the caller keeps full error fidelity across the erasure. Reports whether a user-closure panic was
/// caught (see [`CompletionOutcome`]).
pub type FailoverComplete<I, F> =
  Box<dyn FnOnce(FailoverOutcome<'_, I, F>) -> CompletionOutcome + Send>;

/// Apply a CHECKED failover inherited read to a [`FailoverOutcome`]: serve the closure's result ONLY
/// when the limbo region is EMPTY (the coarse-but-safe mode), else decline with `Ok(None)`. The closure
/// never has to reason about limbo — a non-empty limbo declines BEFORE it runs, so it cannot serve a
/// stale read by ignoring the region. Shared verbatim by the single- and multi-group `failover_query` so
/// the two cannot drift.
pub(crate) fn apply_checked_failover<I, F, Out>(
  outcome: FailoverOutcome<'_, I, F>,
  f: impl FnOnce(&F, FailoverReadWindow) -> Option<Out>,
) -> Result<Option<Out>, DriverError<I>> {
  outcome
    .map(|opt| opt.and_then(|(fsm, limbo, win)| if limbo.is_empty() { f(fsm, win) } else { None }))
}

/// Apply an UNCHECKED failover inherited read: hand the closure the FSM, the limbo region, and the
/// window; the closure owns the per-key absence proof. The primitive behind `failover_query_unchecked` —
/// a closure that IGNORES the limbo slice can serve stale, which is the documented obligation the API
/// cannot enforce.
pub(crate) fn apply_unchecked_failover<I, F, Out>(
  outcome: FailoverOutcome<'_, I, F>,
  f: impl FnOnce(&F, &[Entry], FailoverReadWindow) -> Option<Out>,
) -> Result<Option<Out>, DriverError<I>> {
  outcome.map(|opt| opt.and_then(|(fsm, limbo, win)| f(fsm, limbo, win)))
}

/// A parked failover inherited-read query: held until the committed prefix applies AND the serve
/// window is (re-)confirmed live, then run against the state machine with the limbo region. The
/// completion is type-erased and carries its own reply channel.
pub struct ParkedFailover<I, F> {
  /// The SINGLE completion (see [`FailoverComplete`]).
  pub complete: FailoverComplete<I, F>,
  /// The budget reservation (a failover query reserves one zero-byte slot).
  pub _reservation: ReservationGuard,
}

/// The limbo region `(window.index(), window.limbo_upper()]` filtered to NORMAL entries — the
/// committed-but-possibly-superseded prefix a failover inherited read's key must be absent from —
/// read under a HARD `max_bytes` cap and FAIL-CLOSED. Shared by both drivers so the bound and the
/// fail-closed rule cannot drift.
///
/// `Err` on a FATAL `LogStore::entries` storage fault (corruption / I/O) — the driver fail-stops
/// (`Poisoned`) rather than hide it. `Ok(None)` is the SAFE fallback to a normal read: the index
/// ceiling, a cap truncation, or an incomplete region — none of which can prove key-absence.
/// `Ok(Some(limbo))` is the complete, in-budget, contiguous NORMAL-entry region to serve (an empty
/// `Vec` when the region is empty). The cap is load-bearing: the post-election limbo can be an
/// arbitrarily large inherited tail and a failover read reserves a zero-byte budget slot, so an
/// unbounded scan would let one read OOM/stall the driver. The region is contiguous and above the
/// commit (never compacted), so a COMPLETE read's last entry reaches `limbo_upper`.
pub fn read_limbo<L: LogStore>(
  log: &L,
  window: &FailoverReadWindow,
  max_bytes: u64,
) -> Result<Option<Vec<Entry>>, L::Error> {
  let Some((lo, hi)) = limbo_bounds(window) else {
    return Ok(None); // index at the u64 ceiling — a safe fallback, not a fault
  };
  if lo >= hi {
    return Ok(Some(Vec::new()));
  }
  // PREFLIGHT the region WIDTH before reading: the `LogStore` byte cap counts PAYLOAD only, so a large
  // inherited limbo tail of ZERO-payload entries would materialize (owned) in full before
  // `contiguous_normal_entries` rejects it on cost — re-opening the OOM the cap is meant to prevent. A
  // region wider than `max_bytes / LIMBO_ENTRY_OVERHEAD` cannot fit the cost budget even at zero payload
  // (each entry charges at least `LIMBO_ENTRY_OVERHEAD`), so FAIL CLOSED here — degrade to the Safe read —
  // rather than read and materialize it. The byte cap still bounds the large-payload case below.
  let max_entries = (max_bytes / LIMBO_ENTRY_OVERHEAD).max(1);
  if hi.get() - lo.get() > max_entries {
    return Ok(None);
  }
  // A `LogStore::entries` error is a FATAL storage fault, NOT a safe fallback — propagate it so the
  // driver fail-stops instead of letting a corrupt committed-range log keep serving normal reads.
  let entries = match log.entries(lo..hi, max_bytes)? {
    EntriesRead::Ready(e) => e,
    // Cold limbo range: fail closed (serve nothing this pass), same as the ceiling fallback — the
    // driver re-pumps when the store signals storage-ready.
    EntriesRead::Pending => return Ok(None),
  };
  Ok(contiguous_normal_entries(&entries, lo, hi, max_bytes))
}

/// The half-open `[lo, hi)` for the inclusive limbo region `(index, limbo_upper]`, or `None` (FAIL
/// CLOSED) when either bound is at the `u64` ceiling — an inclusive `limbo_upper == u64::MAX` cannot be
/// expressed as a half-open upper bound without overflow, so completeness could not be proven and the
/// failover read must fall back.
fn limbo_bounds(window: &FailoverReadWindow) -> Option<(Index, Index)> {
  Some((
    window.index().checked_next()?,
    window.limbo_upper().checked_next()?,
  ))
}

/// A fixed per-entry charge folded into the limbo cost bound, so a COMPLETE region of zero-payload
/// entries is still bounded by COUNT — the `LogStore` byte cap counts payload only and is approximate.
const LIMBO_ENTRY_OVERHEAD: u64 = 64;

/// `Some(normal-filtered)` only when the scanned slice is EXACTLY the contiguous region `[lo, hi)` — it
/// STARTS at `lo`, every entry is adjacent to the next, and it REACHES `hi` — AND its per-entry cost
/// (`LIMBO_ENTRY_OVERHEAD` plus payload) is within `max_bytes`. A truncated, suffix, or gapped slice
/// fails the contiguity check (a tail-only "reaches hi" test would wrongly accept a suffix or a gapped
/// slice that happens to end at `hi`); an oversized region fails the cost check. Any of these means the
/// limbo cannot prove the read's key absent, so FAIL CLOSED to a normal read: `None`.
fn contiguous_normal_entries(
  entries: &[Entry],
  lo: Index,
  hi: Index,
  max_bytes: u64,
) -> Option<Vec<Entry>> {
  let mut expected = lo;
  let mut cost: u64 = 0;
  for e in entries {
    if e.index() != expected {
      return None; // a non-`lo` start or a gap: not the contiguous region
    }
    expected = e.index().checked_next()?;
    cost = cost
      .saturating_add(LIMBO_ENTRY_OVERHEAD)
      .saturating_add(e.data().len() as u64);
    if cost > max_bytes {
      return None;
    }
  }
  if expected != hi {
    return None; // truncated: the scan did not reach limbo_upper
  }
  Some(
    entries
      .iter()
      .filter(|e| e.kind() == EntryKind::Normal)
      .cloned()
      .collect(),
  )
}

/// Serve a batch of parked failover reads against `fsm` and `limbo` for `window`, RE-CHECKING the
/// inherited lease (`still_live`) with a FRESH wall before EACH completion: the limbo scan and every
/// preceding user closure consume wall time and the proto's lease gate is STRICT at the boundary, so a
/// window live when the batch started can expire mid-batch — a completion past expiry must fall back
/// (`Ok(None)`) rather than serve a stale inherited read. Shared by both drivers so the freshness rule
/// cannot drift.
/// Returns whether ANY completion — served OR declined — caught a user-closure(-drop) panic; the
/// caller fail-stops the group the batch read against (interior mutability could have torn its
/// replicated state). The batch stops SERVING at the first caught panic, exactly like the normal
/// query drain: the remaining items complete `Poisoned` — the verdict the caller's fail-stop sweep
/// gives the group's parked siblings — without their user closures ever running against the
/// possibly-torn FSM. A declined completion (`Ok(None)`, past the live window) does not CALL the
/// closure, but it DROPS it unused inside the completion's `catch_unwind`, so a captured guard's
/// `Drop` can report `Panicked` on the decline arm too — this fold captures either arm.
#[must_use]
pub fn serve_failover_batch<I, F>(
  parked: Vec<ParkedFailover<I, F>>,
  fsm: &F,
  limbo: &[Entry],
  window: FailoverReadWindow,
  mut still_live: impl FnMut() -> bool,
) -> bool {
  let mut panicked = false;
  for p in parked {
    if panicked {
      // This `Err` arm does not CALL the user closure, but it DROPS it unused inside the completion's
      // `catch_unwind`, so it CAN report `Panicked` (see [`CompletionOutcome`]). Discarding that report
      // is sound HERE and nowhere else: `panicked` is already latched and is what this function returns,
      // so the caller's fail-stop for this group is already decided and a second report cannot change it.
      let _ = (p.complete)(Err(DriverError::Poisoned));
      continue;
    }
    let outcome = if still_live() {
      (p.complete)(Ok(Some((fsm, limbo, window))))
    } else {
      (p.complete)(Ok(None))
    };
    panicked = outcome == CompletionOutcome::Panicked;
  }
  panicked
}

/// Serve a drained batch of runnable linearizable queries against `fsm`, stopping at the first
/// caught user-closure panic. `take_runnable_queries` already removed the WHOLE batch from routing,
/// so a bare `break` on the first panic would strand the rest — their completions never run, their
/// reply channels close, and the caller's `fail_all` can no longer reach them (a caller gets a
/// closed-channel `ShuttingDown` instead of the true verdict). The remaining drained queries
/// instead complete `Poisoned` WITHOUT running their closures against the possibly-torn FSM — the
/// same verdict the caller's fail-stop gives the group's parked siblings. Returns whether any
/// completion caught a panic; the caller fail-stops the group the batch read against. The
/// normal-query sibling of [`serve_failover_batch`], sharing the drain-the-remainder rule so it
/// cannot drift.
#[must_use]
pub fn serve_query_batch<I, F>(queries: Vec<ParkedQuery<I, F>>, fsm: &F) -> bool {
  let mut panicked = false;
  for q in queries {
    if panicked {
      // This `Err` arm does not CALL the user closure, but it DROPS it unused inside the completion's
      // `catch_unwind`, so it CAN report `Panicked` (see [`CompletionOutcome`]). Discarding that report
      // is sound HERE and nowhere else: `panicked` is already latched and is what this function returns,
      // so the caller's fail-stop for this group is already decided and a second report cannot change it.
      let _ = (q.complete)(Err(DriverError::Poisoned));
      continue;
    }
    panicked = (q.complete)(Ok(fsm)) == CompletionOutcome::Panicked;
  }
  panicked
}

/// A linearizable query's lifecycle: confirmed by `ReadState` (which fixes `ready_at`), then run
/// against the state machine once `applied >= ready_at`. The completion is type-erased and
/// carries its own reply channel; `F` is the driver's state-machine type.
pub struct ParkedQuery<I, F> {
  /// `None` until the matching `ReadState` arrives; then the index the apply watermark must
  /// reach before the closure may run.
  pub ready_at: Option<Index>,
  /// The SINGLE completion (see [`QueryComplete`]).
  pub complete: QueryComplete<I, F>,
  /// The budget reservation (a query reserves one zero-byte slot).
  pub _reservation: ReservationGuard,
}

/// The shared routing state both drivers thread through [`Self::route_event`].
pub struct Routing<I, R, F> {
  /// Submit/conf completions keyed by their log index. Sound ONLY together with the
  /// sweep-on-every-`LeaderChanged` rule below: within one unbroken leadership the index is
  /// unambiguous; across a change, swept entries can never be completed by a recycled index.
  pub pending: BTreeMap<Index, Pending<I, R>>,
  /// Linearizable queries keyed by the read context this driver minted (a monotone counter's
  /// big-endian bytes — never reused within a driver's lifetime).
  pub queries: BTreeMap<u64, ParkedQuery<I, F>>,
  /// The next read-context value.
  pub next_query_ctx: u64,
  /// Parked failover inherited-read queries: served as a batch each pass once the serve window is
  /// confirmed live and the committed prefix has applied (see the drivers' `run_failover_serve`), and
  /// swept by [`Self::fail_all`] on every leadership change, exactly like the parked queries.
  pub failovers: Vec<ParkedFailover<I, F>>,
  /// The apply watermark (highest `Applied`/`ConfChanged` index seen): gates query execution.
  pub applied: Index,
  /// The best-effort events tail (dropped-on-full; never blocks the driver).
  pub events: flume::Sender<Event<I, R>>,
  /// Latched when a Routing completion — a [`Self::fail_all`] sweep or a [`Self::decline_failovers`]
  /// decline — catches a user-closure(-drop) panic. Such a completion never RUNS the user closure
  /// (an `Err`/`Ok(None)` short-circuits it), but it DROPS the closure unused, running any guard the
  /// closure captured; that guard's `Drop` can mutate shared FSM state and panic, caught by the
  /// completion's `catch_unwind`.
  ///
  /// A caught panic means A state machine could be TORN — and WHICH one is unknowable. The closure is
  /// `Send + 'static` and captured whatever it liked; [`StateMachine`](sailing_proto::StateMachine)
  /// imposes no isolation, so the guard can alias state ANY hosted group's replicated FSM shares, not
  /// merely the addressed group's. The tear is UNATTRIBUTABLE, so the verdict is PLANE-FATAL: every
  /// hosted group fail-stops. Read AND cleared by [`Self::take_completion_panicked`], which the driver
  /// routes to that plane fail-stop alongside the serve batches' returned panic — ONE decision fed by
  /// every source. The multi drivers harvest EVERY routing's latch before ANY group serves, so a tear
  /// latched on one group can never be outrun by a sibling's serve.
  completion_panicked: bool,
}

impl<I, R, F> Routing<I, R, F> {
  /// Empty routing forwarding committed events to the best-effort `events` tail.
  pub fn new(events: flume::Sender<Event<I, R>>) -> Self {
    Self {
      pending: BTreeMap::new(),
      queries: BTreeMap::new(),
      next_query_ctx: 1,
      failovers: Vec::new(),
      applied: Index::ZERO,
      events,
      completion_panicked: false,
    }
  }

  /// Mint a fresh, never-reused read context for a query.
  pub fn mint_query_ctx(&mut self) -> u64 {
    let ctx = self.next_query_ctx;
    self.next_query_ctx += 1;
    ctx
  }

  /// Reconcile the driver watermark to the ENDPOINT's applied index, returning whether it moved.
  ///
  /// Some committed entries advance the endpoint's applied index WITHOUT emitting a routed event — a
  /// fresh leader's `Empty` no-op, and the committed prefix a restart replays (whose events are
  /// cleared) — so `route_event` alone leaves `applied` lagging and a read confirmed at such an index
  /// would never become runnable. The driver calls this each pump (after draining events) and runs the
  /// now-runnable queries when it returns `true`. Monotone: the endpoint's applied index only grows and
  /// routed events never exceed it, so this only ever advances the watermark.
  pub fn sync_applied(&mut self, endpoint_applied: Index) -> bool {
    if endpoint_applied > self.applied {
      self.applied = endpoint_applied;
      true
    } else {
      false
    }
  }

  /// Queries whose confirmed read index the apply watermark has reached — popped for execution
  /// against the state machine (the DRIVER runs them; this module only books them).
  pub fn take_runnable_queries(&mut self) -> Vec<ParkedQuery<I, F>> {
    let applied = self.applied;
    let ready: Vec<u64> = self
      .queries
      .iter()
      .filter(|(_, q)| q.ready_at.is_some_and(|at| at <= applied))
      .map(|(ctx, _)| *ctx)
      .collect();
    ready
      .into_iter()
      .filter_map(|ctx| self.queries.remove(&ctx))
      .collect()
  }

  /// Fail EVERYTHING parked with `err`.
  ///
  /// With [`DriverError::Superseded`]: run on EVERY `LeaderChanged` event, unconditionally.
  /// Raft can lose uncommitted entries across a leadership change, and a recycled index could
  /// otherwise complete the WRONG waiter (a later term's entry applying at an index an old
  /// waiter parked on). Sweeping on every change — including this node re-winning — keeps
  /// index-keying sound: pending is non-empty only during one unbroken leadership. Queries are
  /// swept too: their read confirmation belongs to the old leadership's quorum reasoning.
  ///
  /// With [`DriverError::ShuttingDown`]: the teardown sweep.
  pub fn fail_all(&mut self, err: &DriverError<I>)
  where
    I: Clone,
  {
    for (_, p) in std::mem::take(&mut self.pending) {
      match p {
        Pending::Submit { reply, .. } => {
          let _ = reply.send(Err(err.clone()));
        }
        Pending::Conf { reply, .. } => {
          let _ = reply.send(Err(err.clone()));
        }
      }
    }
    // Route the query/failover completions through the centralized invokers so a caught
    // user-closure(-drop) panic is LATCHED, never discarded: a swept completion drops its unused
    // closure, whose captured guard's `Drop` can panic against shared FSM state.
    for (_, q) in std::mem::take(&mut self.queries) {
      self.complete_query(q, Err(err.clone()));
    }
    // TAKING the failovers is load-bearing, not merely a way to iterate them. The multi drivers'
    // leadership-loss backstop calls `fail_all(Superseded)` inside a group's pre-serve step, BEFORE that
    // same group's `run_failover_serve`, and reads the latch this sweep may set only at the END of the
    // step — which reads like a same-group tear-then-serve window. THIS take is what closes it: it empties
    // `failovers`, and `run_failover_serve` early-returns on an empty batch, so a sweep that could have
    // torn an FSM always leaves the serve it precedes with nothing to serve. Drain and empty-check are one
    // invariant spanning two files; iterate this by reference, or drop that check, and the window re-opens
    // inside a single group's step, where no plane-wide phase can see it.
    for p in std::mem::take(&mut self.failovers) {
      self.complete_failover(p, Err(err.clone()));
    }
  }

  /// Complete every parked failover inherited-read with `Ok(None)` — decline to serve, so the caller
  /// falls back to a normal read — routing each through the internal `complete_failover` chokepoint
  /// so a caught drop-panic latches the completion-panic signal. The drivers' failover serve calls
  /// this on its decline arms instead of invoking the completion raw, so no decline can silently
  /// swallow a panic.
  pub fn decline_failovers(&mut self) {
    for p in std::mem::take(&mut self.failovers) {
      self.complete_failover(p, Ok(None));
    }
  }

  /// Take AND clear whether any Routing completion caught a user-closure(-drop) panic since the last
  /// read — the internal `completion_panicked` latch. The driver routes this into its PLANE fail-stop
  /// (the tear is unattributable — see the field), reading it BEFORE any group serves so no read runs
  /// against a state machine an earlier-in-crank completion tore.
  #[must_use]
  pub fn take_completion_panicked(&mut self) -> bool {
    std::mem::replace(&mut self.completion_panicked, false)
  }

  /// Invoke ONE parked query's completion, latching a caught user-closure(-drop) panic. The SOLE
  /// invoker of a query completion outside [`serve_query_batch`]: no Routing method invokes a query
  /// completion raw, so every non-serving completion panic reaches
  /// [`Self::take_completion_panicked`] and thus the plane's fail-stop.
  fn complete_query(&mut self, q: ParkedQuery<I, F>, res: Result<&F, DriverError<I>>) {
    if (q.complete)(res) == CompletionOutcome::Panicked {
      self.completion_panicked = true;
    }
  }

  /// Invoke ONE parked failover completion, latching a caught user-closure(-drop) panic. The SOLE
  /// invoker of a failover completion outside [`serve_failover_batch`] (see [`Self::complete_query`]).
  fn complete_failover(&mut self, p: ParkedFailover<I, F>, res: FailoverOutcome<'_, I, F>) {
    if (p.complete)(res) == CompletionOutcome::Panicked {
      self.completion_panicked = true;
    }
  }
}

/// The latch's other half, which no return type can enforce.
///
/// [`CompletionOutcome`]'s `#[must_use]` makes the COMPILER enumerate every site that invokes a
/// completion and discards its verdict. It cannot see the second way the verdict is lost: a `Routing`
/// whose latch is SET is simply DROPPED, and the driver's fail-stop tail — which reads latches through
/// the routing map — can never read a latch for a group the map no longer holds. [`Self::fail_all`]
/// returns `()`, so the type system has nothing to hold onto; only this assert does.
///
/// A DETACHED `Routing` (a merge fold's source teardown, `remove_group`, the shutdown sweep) sweeps its
/// parked work as it dies, and a swept completion drops its unused closure — whose captured guard's
/// `Drop` can tear a state machine and panic. WHICH state machine is unknowable: the guard aliases
/// whatever the closure captured, which may be state any HOSTED group's FSM shares — a group the dying
/// one never owned, never absorbed, and has no relationship to. There is no heir to reason about and no
/// group to name. So a fired latch on a detached `Routing` means the same thing it means everywhere
/// else: route it to `fail_stop_plane_unattributable_panic()` and stop the WHOLE plane.
///
/// The ONE sound discard is a plane that is TERMINATING — the shutdown sweep, run after the driver's
/// loop has exited. Nothing serves again, so a tear has no serve left to corrupt and there is nothing to
/// fail-stop. That is a property of the plane's lifecycle, not of who inherited an FSM; any detach on a
/// LIVE plane (`remove_group`, a merge fold's `Merged` or `Retired`) is plane-fatal, siblings included.
///
/// Either way the verdict is CONSUMED before the drop. Reaching this assert means a detach site did
/// neither, and a caught panic that could have torn replicated state was silently dropped: call
/// [`Self::take_completion_panicked`] at that site and route the verdict.
impl<I, R, F> Drop for Routing<I, R, F> {
  fn drop(&mut self) {
    debug_assert!(
      !self.completion_panicked,
      "a Routing was dropped with its completion-panic latch SET: a completion caught a \
       user-closure(-drop) panic that could have torn ANY hosted group's replicated state machine, \
       and no group was ever fail-stopped for it. Every site that detaches a Routing must \
       take_completion_panicked() and route the verdict — fail-stop the whole plane (the tear is \
       unattributable), or, only when the plane is terminating and nothing can serve again, drain the \
       latch explicitly and document why no serve is left to corrupt"
    );
  }
}

impl<I: sailing_proto::NodeId, R: Clone, F> Routing<I, R, F> {
  /// Route one coordinator event: complete the matching pending waiter, advance the apply
  /// watermark, mark confirmed queries ready, sweep on leadership changes — and forward a copy
  /// to the best-effort events tail. Returns `true` if the apply watermark advanced (the driver
  /// then runs [`Self::take_runnable_queries`] against the state machine).
  pub fn route_event(&mut self, event: Event<I, R>) -> bool {
    let mut advanced = false;
    match &event {
      Event::Applied(applied) => {
        let index = applied.index();
        if let Some(Pending::Submit { reply, .. }) = self.pending.remove(&index) {
          let _ = reply.send(Ok(applied.response().clone()));
        }
        if index > self.applied {
          self.applied = index;
          advanced = true;
        }
      }
      Event::ConfChanged(cc) => {
        let index = cc.index();
        if let Some(Pending::Conf { reply, .. }) = self.pending.remove(&index) {
          let _ = reply.send(Ok(index));
        }
        if index > self.applied {
          self.applied = index;
          advanced = true;
        }
      }
      Event::LeaderChanged(_) => {
        self.fail_all(&DriverError::Superseded);
      }
      Event::SnapshotInstalled(meta) => {
        // A snapshot install IS an apply: the FSM jumped to `meta.last_index()` without
        // per-entry Applied events. Without this advance, a query confirmed at an index the
        // snapshot covers would stay parked forever (holding its budget reservation).
        if meta.last_index() > self.applied {
          self.applied = meta.last_index();
          advanced = true;
        }
      }
      Event::ReadModeChanged(rmc) => {
        // A committed SetReadMode applied at this index but reports ReadModeChanged, NOT Applied (the
        // sibling of the ConfChanged arm above) — so the watermark must advance HERE too, or a
        // post-migration query confirmed at this index parks forever (no later Applied arrives to lift
        // it). No pending waiter to complete: `set_read_mode` returns the propose verdict immediately
        // and never parks a `Pending` by index.
        if rmc.index() > self.applied {
          self.applied = rmc.index();
          advanced = true;
        }
      }
      // The context is this driver's minted counter (big-endian u64). A context this driver
      // did not mint (an embedder driving read_index through the coordinator directly, or a
      // different width entirely) just passes through to the tail.
      Event::ReadState(rs) if rs.context().len() == 8 => {
        let mut b = [0u8; 8];
        b.copy_from_slice(rs.context());
        if let Some(q) = self.queries.get_mut(&u64::from_be_bytes(b)) {
          q.ready_at = Some(rs.index());
          // The watermark may ALREADY cover the read index; report runnable.
          advanced = true;
        }
      }
      _ => {}
    }
    // Best-effort tail: try_send drops on full — the tail must never block consensus.
    let _ = self.events.try_send(event);
    advanced
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  type R = Routing<u64, u64, ()>;

  fn routing() -> (R, flume::Receiver<Event<u64, u64>>) {
    let (tx, rx) = flume::bounded(8);
    (Routing::new(tx), rx)
  }

  fn park_submit(
    r: &mut R,
    budget: &InflightBudget,
    idx: u64,
  ) -> futures_channel::oneshot::Receiver<Result<u64, DriverError<u64>>> {
    let (tx, rx) = futures_channel::oneshot::channel();
    r.pending.insert(
      Index::new(idx),
      Pending::Submit {
        reply: tx,
        _reservation: budget.try_reserve::<u64>(0).unwrap(),
      },
    );
    rx
  }

  /// How the shared [`MockLog`]'s `entries` behaves. `read_limbo` only ever calls `entries`, so one
  /// configurable fixture covers every scenario its tests need — a fatal fault, an oversized-region
  /// preflight, a complete in-budget region, a cold region — and the trait methods it never calls stay
  /// stubs counted once rather than re-declared per test.
  enum LogMode {
    /// `entries` returns a fatal storage fault (corruption / I/O).
    Fail,
    /// `entries` PANICS: a fixture for the width preflight, which must fail closed BEFORE any read, so
    /// a large owned zero-payload limbo tail is never materialized.
    PanicOnRead,
    /// `entries` returns these entries as a ready borrowed slice.
    Ready(Vec<Entry>),
    /// `entries` returns `Pending` (a cold region the read falls safely back from).
    Pending,
  }

  struct MockLog {
    mode: LogMode,
  }

  impl LogStore for MockLog {
    type Error = ();
    fn first_index(&self) -> Index {
      Index::ZERO
    }
    fn last_index(&self) -> Index {
      Index::new(u64::MAX - 1)
    }
    fn term(&self, _: Index) -> Result<sailing_proto::Term, ()> {
      Ok(sailing_proto::Term::ZERO)
    }
    fn entries(
      &self,
      _: core::ops::Range<Index>,
      _: u64,
    ) -> Result<sailing_proto::EntriesRead<'_>, ()> {
      match &self.mode {
        LogMode::Fail => Err(()),
        LogMode::PanicOnRead => {
          panic!("the preflight must reject an oversized limbo region before reading")
        }
        LogMode::Ready(entries) => Ok(sailing_proto::EntriesRead::Ready(
          sailing_proto::MaybeOwned::Borrowed(entries),
        )),
        LogMode::Pending => Ok(sailing_proto::EntriesRead::Pending),
      }
    }
    fn submit_append(&mut self, _: sailing_proto::OpId, _: &[Entry]) {
      unreachable!()
    }
    fn compact(&mut self, _: Index) {
      unreachable!()
    }
    fn restore(&mut self, _: Index, _: sailing_proto::Term) {
      unreachable!()
    }
    fn poll(&mut self) -> Option<Result<sailing_proto::LogDone, ()>> {
      unreachable!()
    }
    fn has_pending(&self) -> bool {
      false
    }
  }

  #[test]
  fn budget_reserves_and_releases_on_drop() {
    let b = InflightBudget::new(2, 100);
    let g1 = b.try_reserve::<u64>(60).unwrap();
    assert_eq!(b.in_flight(), (1, 60));
    // The byte cap binds before the count cap.
    assert!(matches!(
      b.try_reserve::<u64>(60),
      Err(DriverError::<u64>::Busy)
    ));
    let g2 = b.try_reserve::<u64>(40).unwrap();
    assert_eq!(b.in_flight(), (2, 100));
    // The count cap binds now.
    assert!(matches!(
      b.try_reserve::<u64>(0),
      Err(DriverError::<u64>::Busy)
    ));
    drop(g1);
    drop(g2);
    assert_eq!(b.in_flight(), (0, 0), "drop releases both dimensions");
  }

  #[test]
  fn applied_completes_the_matching_submit_and_advances_the_watermark() {
    let (mut r, _rx) = routing();
    let b = InflightBudget::new(8, 8);
    let mut reply = park_submit(&mut r, &b, 5);
    let advanced = r.route_event(Event::Applied(sailing_proto::Applied::new(
      Index::new(5),
      42,
    )));
    assert!(advanced);
    assert_eq!(reply.try_recv().unwrap().unwrap(), Ok(42));
    assert_eq!(r.applied, Index::new(5));
    assert_eq!(b.in_flight(), (0, 0), "completion released the reservation");
  }

  #[test]
  fn leader_change_supersedes_everything_parked() {
    let (mut r, _rx) = routing();
    let b = InflightBudget::new(8, 8);
    let mut s1 = park_submit(&mut r, &b, 5);
    let mut s2 = park_submit(&mut r, &b, 6);
    r.route_event(Event::LeaderChanged(sailing_proto::LeaderChanged::new(
      sailing_proto::Term::new(3),
      Some(2u64),
    )));
    assert_eq!(
      s1.try_recv().unwrap().unwrap(),
      Err(DriverError::Superseded)
    );
    assert_eq!(
      s2.try_recv().unwrap().unwrap(),
      Err(DriverError::Superseded)
    );
    assert!(r.pending.is_empty());
    assert_eq!(b.in_flight(), (0, 0), "the sweep released the reservations");
    // A LATER Applied at a swept index finds no waiter (the recycled-index hazard).
    let advanced = r.route_event(Event::Applied(sailing_proto::Applied::new(
      Index::new(5),
      9,
    )));
    assert!(advanced, "the watermark still advances");
  }

  #[test]
  fn leader_change_supersedes_parked_failover_reads() {
    let (mut r, _rx) = routing();
    let b = InflightBudget::new(8, 8);
    let (tx, mut rx) = futures_channel::oneshot::channel();
    r.failovers.push(ParkedFailover {
      complete: Box::new(move |res: FailoverOutcome<'_, u64, ()>| {
        // A parked inherited-read MUST be voided on a leadership change — its serve window belonged to
        // the old leadership's reasoning, exactly like a parked query.
        let _ = tx.send(matches!(res, Err(DriverError::Superseded)));
        CompletionOutcome::Delivered
      }),
      _reservation: b.try_reserve::<u64>(0).unwrap(),
    });
    r.route_event(Event::LeaderChanged(sailing_proto::LeaderChanged::new(
      sailing_proto::Term::new(3),
      Some(2u64),
    )));
    assert_eq!(
      rx.try_recv().unwrap(),
      Some(true),
      "the parked failover read was superseded"
    );
    assert!(
      r.failovers.is_empty(),
      "the sweep drained the parked failover"
    );
    assert_eq!(b.in_flight(), (0, 0), "the sweep released the reservation");
  }

  #[test]
  fn limbo_scan_requires_a_complete_contiguous_in_budget_region() {
    use sailing_proto::{EntryKind, Term};
    let e = |idx: u64, kind| Entry::new(Term::new(1), Index::new(idx), kind, bytes::Bytes::new());
    // Region (commit=4, limbo_upper=7]: indices 5,6,7; lo=5, hi=8.
    let lo = Index::new(5);
    let hi = Index::new(8);
    let cap = 1_000_000; // generous: the three small entries fit.
    let full = [
      e(5, EntryKind::Normal),
      e(6, EntryKind::Empty),
      e(7, EntryKind::Normal),
    ];
    // COMPLETE contiguous [5,8) within budget -> serve, keeping only the Normal entries.
    let served =
      contiguous_normal_entries(&full, lo, hi, cap).expect("a complete contiguous limbo serves");
    assert_eq!(served.len(), 2, "only the two Normal entries are kept");
    // TRUNCATED (stops at 6, short of hi) -> fail closed.
    let truncated = [e(5, EntryKind::Normal), e(6, EntryKind::Normal)];
    assert!(contiguous_normal_entries(&truncated, lo, hi, cap).is_none());
    // SUFFIX (starts at 6, not lo=5; ends at hi) -> fail closed; a tail-only check would WRONGLY pass.
    let suffix = [e(6, EntryKind::Normal), e(7, EntryKind::Normal)];
    assert!(
      contiguous_normal_entries(&suffix, lo, hi, cap).is_none(),
      "a suffix that skips lo cannot prove key-absence over the whole region"
    );
    // GAPPED (5 then 7, missing 6; ends at hi) -> fail closed.
    let gapped = [e(5, EntryKind::Normal), e(7, EntryKind::Normal)];
    assert!(
      contiguous_normal_entries(&gapped, lo, hi, cap).is_none(),
      "a gap in the region cannot prove key-absence"
    );
    // EMPTY -> fail closed.
    assert!(contiguous_normal_entries(&[], lo, hi, cap).is_none());
    // COMPLETE but OVER the cap by COUNT (zero-payload entries the LogStore's payload-only cap admits)
    // -> fail closed on the per-entry overhead the payload cap misses.
    assert!(
      contiguous_normal_entries(&full, lo, hi, 100).is_none(),
      "a numerous zero-payload region fails closed on the per-entry cost bound"
    );
  }

  #[test]
  fn limbo_bounds_fails_closed_at_the_index_ceiling() {
    // An inclusive limbo_upper at the u64 ceiling cannot become a half-open upper bound -> fail closed.
    let at_ceiling = FailoverReadWindow::new(Index::new(4), Index::new(u64::MAX));
    assert!(limbo_bounds(&at_ceiling).is_none());
    // A normal window yields the half-open [lo, hi) = (index, limbo_upper] expressed exclusively.
    let normal = FailoverReadWindow::new(Index::new(4), Index::new(7));
    assert_eq!(limbo_bounds(&normal), Some((Index::new(5), Index::new(8))));
  }

  #[test]
  fn read_limbo_surfaces_a_fatal_log_error_not_a_fallback() {
    // A non-empty limbo region forces the log read: a fatal `entries` error MUST surface as `Err` (the
    // driver fail-stops `Poisoned`), NOT collapse to `Ok(None)` — which would hide a corrupt
    // committed-range log behind a normal-read fallback and let the node keep serving.
    let log = MockLog {
      mode: LogMode::Fail,
    };
    let window = FailoverReadWindow::new(Index::new(4), Index::new(7));
    assert!(read_limbo(&log, &window, u64::MAX).is_err());
    // An empty region never touches the log -> `Ok(Some(empty))`, no spurious fault.
    let empty = FailoverReadWindow::new(Index::new(4), Index::new(4));
    assert_eq!(read_limbo(&log, &empty, u64::MAX), Ok(Some(Vec::new())));
  }

  #[test]
  fn read_limbo_preflights_an_oversized_region_without_reading() {
    // `entries` PANICS, so reaching it fails the test: the width preflight must fail closed WITHOUT
    // reading, so a large owned zero-payload inherited limbo tail is never materialized (the OOM a
    // payload-only byte cap would miss). Without the preflight, `read_limbo` would call `entries`.
    let log = MockLog {
      mode: LogMode::PanicOnRead,
    };
    // Budget 640 / LIMBO_ENTRY_OVERHEAD (64) = 10 entries max; a ~1000-index limbo region far exceeds it,
    // so the preflight fails closed (`Ok(None)`) WITHOUT touching the log.
    let window = FailoverReadWindow::new(Index::new(4), Index::new(1003)); // half-open width 999 >> 10
    assert_eq!(
      read_limbo(&log, &window, 640),
      Ok(None),
      "an oversized limbo region must fail closed via the preflight, never reading + materializing it"
    );
  }

  #[test]
  fn failover_batch_falls_back_when_the_lease_expires_mid_batch() {
    use std::sync::atomic::{AtomicU32, Ordering};
    // `FailoverComplete` is `Send`, so the recorders must be too.
    let b = InflightBudget::new(8, 8);
    let served = Arc::new(AtomicU32::new(0));
    let fell_back = Arc::new(AtomicU32::new(0));
    let mut batch = Vec::new();
    for _ in 0..4 {
      let served = served.clone();
      let fell_back = fell_back.clone();
      batch.push(ParkedFailover {
        complete: Box::new(move |res: FailoverOutcome<'_, u64, ()>| {
          match res {
            Ok(Some(_)) => {
              served.fetch_add(1, Ordering::Relaxed);
            }
            Ok(None) => {
              fell_back.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {}
          }
          CompletionOutcome::Delivered
        }),
        _reservation: b.try_reserve::<u64>(0).unwrap(),
      });
    }
    // The inherited lease is live for the first two completions, then expires mid-batch: those two
    // serve, the rest fall back rather than serve a stale read.
    let checks = std::cell::Cell::new(0u32);
    let still_live = || {
      let n = checks.get();
      checks.set(n + 1);
      n < 2
    };
    let window = FailoverReadWindow::new(Index::new(4), Index::new(7));
    assert!(
      !serve_failover_batch(batch, &(), &[], window, still_live),
      "no user closure panicked"
    );
    assert_eq!(served.load(Ordering::Relaxed), 2, "two served while live");
    assert_eq!(
      fell_back.load(Ordering::Relaxed),
      2,
      "the rest fell back on expiry"
    );
  }

  #[test]
  fn failover_batch_stops_serving_at_the_first_caught_panic() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let b = InflightBudget::new(8, 8);
    let second_served = Arc::new(AtomicBool::new(false));
    let second_poisoned = Arc::new(AtomicBool::new(false));
    let mut batch: Vec<ParkedFailover<u64, ()>> = Vec::new();
    // Item 1's user closure panics (caught inside the completion): only the SERVED arm runs the
    // closure, so only it can report the caught panic.
    batch.push(ParkedFailover {
      complete: Box::new(|res: FailoverOutcome<'_, u64, ()>| {
        CompletionOutcome::caught(matches!(res, Ok(Some(_))))
      }),
      _reservation: b.try_reserve::<u64>(0).unwrap(),
    });
    // Item 2 must never be SERVED after the batch caught a panic — a served completion is the
    // only arm that runs the user closure, and the FSM may be torn. It must instead complete
    // with the poisoned-group verdict its parked siblings receive from the caller's fail-stop.
    {
      let served = second_served.clone();
      let poisoned = second_poisoned.clone();
      batch.push(ParkedFailover {
        complete: Box::new(move |res: FailoverOutcome<'_, u64, ()>| {
          match res {
            Ok(Some(_)) => served.store(true, Ordering::Relaxed),
            Err(DriverError::Poisoned) => poisoned.store(true, Ordering::Relaxed),
            _ => {}
          }
          CompletionOutcome::Delivered
        }),
        _reservation: b.try_reserve::<u64>(0).unwrap(),
      });
    }
    let window = FailoverReadWindow::new(Index::new(4), Index::new(7));
    assert!(
      serve_failover_batch(batch, &(), &[], window, || true),
      "the panic indication reaches the caller, which fail-stops the group"
    );
    assert!(
      !second_served.load(Ordering::Relaxed),
      "no later user closure may run against the possibly-torn FSM"
    );
    assert!(
      second_poisoned.load(Ordering::Relaxed),
      "the remaining item completes Poisoned without its closure running"
    );
    assert_eq!(b.in_flight(), (0, 0), "every reservation released");
  }

  #[test]
  fn query_batch_stops_serving_at_the_first_caught_panic() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let b = InflightBudget::new(8, 8);
    let second_served = Arc::new(AtomicBool::new(false));
    let second_poisoned = Arc::new(AtomicBool::new(false));
    let mut batch: Vec<ParkedQuery<u64, ()>> = Vec::new();
    // Item 1's user closure panics (caught inside the completion): only the SERVED arm (`Ok(&fsm)`)
    // runs the closure, so only it can report the caught panic.
    batch.push(ParkedQuery {
      ready_at: None,
      complete: Box::new(|res: Result<&(), DriverError<u64>>| {
        CompletionOutcome::caught(res.is_ok())
      }),
      _reservation: b.try_reserve::<u64>(0).unwrap(),
    });
    // Item 2 must NEVER be served after the batch caught a panic — the FSM may be torn — and must
    // NOT be dropped (a dropped completion closes its channel, the `ShuttingDown`/closed-channel the
    // bug produced). It must complete with the poisoned-group verdict its parked siblings receive.
    {
      let served = second_served.clone();
      let poisoned = second_poisoned.clone();
      batch.push(ParkedQuery {
        ready_at: None,
        complete: Box::new(move |res: Result<&(), DriverError<u64>>| {
          match res {
            Ok(_) => served.store(true, Ordering::Relaxed),
            Err(DriverError::Poisoned) => poisoned.store(true, Ordering::Relaxed),
            Err(_) => {}
          }
          CompletionOutcome::Delivered
        }),
        _reservation: b.try_reserve::<u64>(0).unwrap(),
      });
    }
    assert!(
      serve_query_batch(batch, &()),
      "the panic indication reaches the caller, which fail-stops the group"
    );
    assert!(
      !second_served.load(Ordering::Relaxed),
      "no later user closure may run against the possibly-torn FSM"
    );
    assert!(
      second_poisoned.load(Ordering::Relaxed),
      "the remaining item completes Poisoned without its closure running"
    );
    assert_eq!(b.in_flight(), (0, 0), "every reservation released");
  }

  #[test]
  fn query_becomes_runnable_when_applied_reaches_its_read_index() {
    let (mut r, _rx) = routing();
    let b = InflightBudget::new(8, 8);
    let ctx = r.mint_query_ctx();
    r.queries.insert(
      ctx,
      ParkedQuery {
        ready_at: None,
        complete: Box::new(|_| CompletionOutcome::Delivered),
        _reservation: b.try_reserve::<u64>(0).unwrap(),
      },
    );
    // Unconfirmed: not runnable even as applies advance.
    r.route_event(Event::Applied(sailing_proto::Applied::new(
      Index::new(7),
      0,
    )));
    assert!(r.take_runnable_queries().is_empty());
    // Confirmation at index 9 > applied 7: still not runnable.
    r.route_event(Event::ReadState(sailing_proto::ReadState::new(
      Index::new(9),
      bytes::Bytes::copy_from_slice(&ctx.to_be_bytes()),
    )));
    assert!(r.take_runnable_queries().is_empty());
    // The apply watermark reaches the read index: runnable exactly once.
    r.route_event(Event::Applied(sailing_proto::Applied::new(
      Index::new(9),
      0,
    )));
    let runnable = r.take_runnable_queries();
    assert_eq!(runnable.len(), 1);
    assert!(r.take_runnable_queries().is_empty());
    assert_eq!(
      b.in_flight(),
      (1, 0),
      "the popped query owns its reservation until it is run (or dropped)"
    );
    drop(runnable);
    assert_eq!(
      b.in_flight(),
      (0, 0),
      "dropping the popped query releases it"
    );
  }

  #[test]
  fn snapshot_install_advances_the_watermark_and_releases_queries() {
    let (mut r, _rx) = routing();
    let b = InflightBudget::new(8, 8);
    let ctx = r.mint_query_ctx();
    r.queries.insert(
      ctx,
      ParkedQuery {
        ready_at: Some(Index::new(5)),
        complete: Box::new(|_| CompletionOutcome::Delivered),
        _reservation: b.try_reserve::<u64>(0).unwrap(),
      },
    );
    // The snapshot covers the confirmed read index: the query becomes runnable even though no
    // per-entry Applied ever arrives for the snapshotted range.
    let advanced = r.route_event(Event::SnapshotInstalled(sailing_proto::SnapshotMeta::new(
      Index::new(9),
      sailing_proto::Term::new(2),
      sailing_proto::ConfState::from_voters(std::vec![1u64, 2, 3]),
    )));
    assert!(advanced);
    assert_eq!(r.applied, Index::new(9));
    assert_eq!(r.take_runnable_queries().len(), 1);
  }

  #[test]
  fn read_mode_changed_advances_the_watermark_and_releases_queries() {
    let (mut r, _rx) = routing();
    let b = InflightBudget::new(8, 8);
    let ctx = r.mint_query_ctx();
    r.queries.insert(
      ctx,
      ParkedQuery {
        ready_at: Some(Index::new(5)),
        complete: Box::new(|_| CompletionOutcome::Delivered),
        _reservation: b.try_reserve::<u64>(0).unwrap(),
      },
    );
    // A committed SetReadMode applied at index 5 reports ReadModeChanged, not Applied — yet it MUST
    // advance the watermark, or a query confirmed at 5 parks forever. Without the route_event arm this
    // returns false and the query never runs.
    let advanced = r.route_event(Event::ReadModeChanged(sailing_proto::ReadModeChanged::new(
      Index::new(5),
      sailing_proto::ReadOnlyOption::LeaseBased,
    )));
    assert!(
      advanced,
      "a committed read-mode change advances the apply watermark"
    );
    assert_eq!(r.applied, Index::new(5));
    assert_eq!(
      r.take_runnable_queries().len(),
      1,
      "the post-migration query becomes runnable once the watermark reaches its read index"
    );
  }

  #[test]
  fn sync_applied_tracks_the_endpoint_for_eventless_commits() {
    let (mut r, _rx) = routing();
    let b = InflightBudget::new(8, 8);
    // A query confirmed at index 4 with NO routed event behind it (a fresh leader's Empty no-op, or a
    // restart's replayed prefix): only the endpoint's applied index reflects it.
    let ctx = r.mint_query_ctx();
    r.queries.insert(
      ctx,
      ParkedQuery {
        ready_at: Some(Index::new(4)),
        complete: Box::new(|_| CompletionOutcome::Delivered),
        _reservation: b.try_reserve::<u64>(0).unwrap(),
      },
    );
    assert!(r.take_runnable_queries().is_empty(), "watermark still at 0");
    // Reconciling to the endpoint's applied index advances the watermark and releases the query —
    // without this the query parks forever (no Applied/ConfChanged/ReadModeChanged event ever arrives).
    assert!(r.sync_applied(Index::new(4)), "the watermark moved to 4");
    assert_eq!(r.applied, Index::new(4));
    assert_eq!(r.take_runnable_queries().len(), 1);
    // Monotone + idempotent: a non-greater endpoint index never moves it (and never backward).
    assert!(!r.sync_applied(Index::new(4)));
    assert!(!r.sync_applied(Index::new(2)));
    assert_eq!(r.applied, Index::new(4));
  }

  #[test]
  fn events_tail_is_best_effort() {
    let (tx, rx) = flume::bounded(1);
    let mut r: Routing<u64, u64, ()> = Routing::new(tx);
    for i in 1..=3u64 {
      r.route_event(Event::Applied(sailing_proto::Applied::new(
        Index::new(i),
        i,
      )));
    }
    // Capacity 1: the first event is retained, the overflow dropped, routing never blocked.
    assert_eq!(rx.len(), 1);
    assert_eq!(r.applied, Index::new(3));
  }

  #[test]
  fn conf_changed_completes_the_matching_conf_and_advances_the_watermark() {
    let (mut r, _rx) = routing();
    let b = InflightBudget::new(8, 8);
    let (tx, mut rx) = futures_channel::oneshot::channel();
    r.pending.insert(
      Index::new(6),
      Pending::Conf {
        reply: tx,
        _reservation: b.try_reserve::<u64>(0).unwrap(),
      },
    );
    // A ConfChanged at the parked index completes its waiter with the applied index AND advances the
    // watermark — the membership sibling of the Applied arm (a SetReadMode reports ReadModeChanged, a
    // membership change reports ConfChanged).
    let advanced = r.route_event(Event::ConfChanged(sailing_proto::ConfChanged::new(
      Index::new(6),
      sailing_proto::ConfState::from_voters(std::vec![1u64]),
    )));
    assert!(advanced);
    assert_eq!(rx.try_recv().unwrap().unwrap(), Ok(Index::new(6)));
    assert_eq!(r.applied, Index::new(6));
    assert_eq!(b.in_flight(), (0, 0), "completion released the reservation");
  }

  #[test]
  fn fail_all_supersedes_a_parked_conf_and_query() {
    let (mut r, _rx) = routing();
    let b = InflightBudget::new(8, 8);
    // A parked Conf and a parked query: BOTH must be swept (the Conf arm and the query arm of fail_all,
    // alongside the Submit/failover arms the other tests cover).
    let (conf_tx, mut conf_rx) = futures_channel::oneshot::channel();
    r.pending.insert(
      Index::new(4),
      Pending::Conf {
        reply: conf_tx,
        _reservation: b.try_reserve::<u64>(0).unwrap(),
      },
    );
    let (q_tx, mut q_rx) = futures_channel::oneshot::channel();
    let ctx = r.mint_query_ctx();
    r.queries.insert(
      ctx,
      ParkedQuery {
        ready_at: Some(Index::new(4)),
        complete: Box::new(move |res: Result<&(), DriverError<u64>>| {
          let _ = q_tx.send(matches!(res, Err(DriverError::Superseded)));
          CompletionOutcome::Delivered
        }),
        _reservation: b.try_reserve::<u64>(0).unwrap(),
      },
    );
    r.fail_all(&DriverError::Superseded);
    assert_eq!(
      conf_rx.try_recv().unwrap().unwrap(),
      Err(DriverError::Superseded),
      "the parked conf change was superseded"
    );
    assert_eq!(
      q_rx.try_recv().unwrap(),
      Some(true),
      "the parked query was superseded"
    );
    assert!(r.pending.is_empty() && r.queries.is_empty());
    assert_eq!(
      b.in_flight(),
      (0, 0),
      "the sweep released both reservations"
    );
  }

  /// THE ROOT MECHANISM, pinned executably: an `Err` completion CAN report `Panicked`.
  ///
  /// "An `Err` completion never invokes the user closure, so it cannot panic" is the tempting
  /// inference, and its premise is true while its conclusion is false: `res.map(f)` on an `Err`
  /// CONSUMES `f` without calling it, so `f` — and any guard `f` captured — is DROPPED right there,
  /// INSIDE the completion's `catch_unwind`. A panicking `Drop` is therefore caught and reported
  /// `Panicked` from the very arm that never ran a line of user code. Every refusal, sweep, and decline
  /// arm in the drivers rests on this, which is why none of them may discard its outcome. Built in the
  /// exact shape the handles build a completion, so it is the real mechanism rather than a synthesized
  /// outcome.
  #[test]
  fn an_err_completion_drops_its_unused_closure_and_catches_the_guards_panic() {
    /// A stand-in for a guard whose `Drop` mutates state aliased into the replicated FSM (a `Cell`, a
    /// lock, an `Arc<Mutex<_>>` the closure shares with the state machine) and panics doing so.
    struct PanicOnDrop;

    impl Drop for PanicOnDrop {
      fn drop(&mut self) {
        panic!("the captured guard's Drop ran and tore the state machine");
      }
    }

    /// `Handle::query`'s completion, verbatim in shape: the user closure is applied as `res.map(f)`
    /// under `catch_unwind`, and a caught unwind reports `Panicked`.
    fn completion() -> QueryComplete<u64, u64> {
      let guard = PanicOnDrop;
      Box::new(move |res: Result<&u64, DriverError<u64>>| {
        let f = move |fsm: &u64| {
          // The guard is OWNED by the user closure, so it dies with the closure — whether the closure
          // is CALLED (a served read) or merely DROPPED (every refusal/decline arm).
          let _guard = guard;
          *fsm
        };
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| res.map(f)));
        CompletionOutcome::caught(caught.is_err())
      })
    }

    // The arm the old comments called panic-free: the closure is never CALLED, and the completion still
    // reports `Panicked` — `map` dropped it, it dropped the guard, the guard's `Drop` panicked inside
    // the unwind boundary. A driver that discards this outcome silently drops the fail-stop.
    assert_eq!(
      (completion())(Err(DriverError::Superseded)),
      CompletionOutcome::Panicked,
      "an `Err` refusal DROPS its unused closure inside the catch_unwind: the guard's Drop panics there"
    );
    // The served arm reports it too (the closure runs, then dies) — the outcome is arm-independent.
    assert_eq!(
      (completion())(Ok(&7u64)),
      CompletionOutcome::Panicked,
      "the served arm drops the closure after calling it: same guard, same caught panic"
    );
  }

  #[test]
  fn fail_all_latches_a_caught_completion_panic() {
    // The contract: `fail_all` LATCHES a swept completion's caught panic rather than discarding its
    // `CompletionOutcome`. A swept completion never runs the user closure (the `Err` short-circuits it)
    // yet DROPS it unused, running any guard it captured — whose `Drop` can panic against shared FSM
    // state, caught by the completion's `catch_unwind` and reported `Panicked` (simulated here exactly
    // as `query_batch_stops_serving_at_the_first_caught_panic` does, without a real unwind). A panic
    // dropped here would leave a group LIVE against possibly-torn state, so the latch carries it to the
    // driver's fail-stop — plane-wide on a multi host, since the tear is unattributable.
    let (mut r, _rx) = routing();
    let b = InflightBudget::new(8, 8);
    let ctx = r.mint_query_ctx();
    r.queries.insert(
      ctx,
      ParkedQuery {
        ready_at: None,
        complete: Box::new(|res: Result<&(), DriverError<u64>>| {
          CompletionOutcome::caught(matches!(res, Err(DriverError::Superseded)))
        }),
        _reservation: b.try_reserve::<u64>(0).unwrap(),
      },
    );
    r.fail_all(&DriverError::Superseded);
    assert!(
      r.take_completion_panicked(),
      "fail_all must latch a completion's caught panic, not discard it"
    );
    assert!(!r.take_completion_panicked(), "the take cleared the latch");
  }

  #[test]
  fn decline_failovers_latches_a_caught_completion_panic() {
    // The drivers' failover decline arms (`Ok(None)`, no serve window / safe fallback) route through
    // `decline_failovers`; a declined completion still drops its unused closure, so a guard-drop
    // panic there must reach the fail-stop too — not just the `fail_all` supersede path.
    let (mut r, _rx) = routing();
    let b = InflightBudget::new(8, 8);
    r.failovers.push(ParkedFailover {
      complete: Box::new(|res: FailoverOutcome<'_, u64, ()>| {
        CompletionOutcome::caught(matches!(res, Ok(None)))
      }),
      _reservation: b.try_reserve::<u64>(0).unwrap(),
    });
    r.decline_failovers();
    assert!(r.failovers.is_empty(), "the decline drained the failovers");
    assert!(
      r.take_completion_panicked(),
      "a declined completion's caught panic latches for the group fail-stop"
    );
  }

  #[test]
  fn a_normal_fail_all_does_not_latch_a_panic() {
    // The no-over-reach bound: a WELL-BEHAVED sweep (every completion `Delivered`) must NOT latch —
    // a superseded group is not a poisoned one, so the latch must never fail-stop it.
    let (mut r, _rx) = routing();
    let b = InflightBudget::new(8, 8);
    let ctx = r.mint_query_ctx();
    r.queries.insert(
      ctx,
      ParkedQuery {
        ready_at: None,
        complete: Box::new(|_res: Result<&(), DriverError<u64>>| CompletionOutcome::Delivered),
        _reservation: b.try_reserve::<u64>(0).unwrap(),
      },
    );
    r.failovers.push(ParkedFailover {
      complete: Box::new(|_res: FailoverOutcome<'_, u64, ()>| CompletionOutcome::Delivered),
      _reservation: b.try_reserve::<u64>(0).unwrap(),
    });
    r.fail_all(&DriverError::Superseded);
    assert!(
      !r.take_completion_panicked(),
      "a well-behaved supersede sweep must not latch a panic (no over-fail-stop)"
    );
  }

  #[test]
  fn an_unmatched_event_passes_through_to_the_tail_without_advancing() {
    let (mut r, rx) = routing();
    // A `ReadState` whose context is NOT this driver's 8-byte minted counter (a different width) is no
    // query of ours: it falls through to the catch-all arm — never advancing the watermark — yet is
    // still forwarded to the best-effort tail.
    let advanced = r.route_event(Event::ReadState(sailing_proto::ReadState::new(
      Index::new(9),
      bytes::Bytes::from_static(&[1u8, 2, 3, 4]),
    )));
    assert!(!advanced, "a foreign-width ReadState advances nothing");
    assert_eq!(r.applied, Index::ZERO);
    assert_eq!(rx.len(), 1, "the event still reached the tail");
  }

  #[test]
  fn read_limbo_serves_a_complete_in_budget_region() {
    use sailing_proto::{EntryKind, Term};
    let e = |idx: u64, kind| Entry::new(Term::new(1), Index::new(idx), kind, bytes::Bytes::new());
    // Region (commit=4, limbo_upper=7]: a COMPLETE contiguous [5,8) within budget returns the
    // Normal-only entries — the happy path the failing/panicking/empty fixtures never exercise.
    let log = MockLog {
      mode: LogMode::Ready(std::vec![
        e(5, EntryKind::Normal),
        e(6, EntryKind::Empty),
        e(7, EntryKind::Normal)
      ]),
    };
    let window = FailoverReadWindow::new(Index::new(4), Index::new(7));
    let served = read_limbo(&log, &window, 1_000_000)
      .expect("a readable region is not a fault")
      .expect("a complete contiguous in-budget region serves");
    assert_eq!(served.len(), 2, "only the two Normal entries are kept");
  }

  #[test]
  fn read_limbo_falls_back_on_a_cold_region() {
    // A `Pending` (cold) limbo read is a SAFE fallback to a normal read, never a fault: `Ok(None)`.
    let log = MockLog {
      mode: LogMode::Pending,
    };
    let window = FailoverReadWindow::new(Index::new(4), Index::new(7));
    assert_eq!(read_limbo(&log, &window, u64::MAX), Ok(None));
  }

  #[test]
  fn read_limbo_fails_closed_at_the_index_ceiling_without_reading() {
    // A window whose index is at the u64 ceiling cannot express a half-open upper bound, so `read_limbo`
    // returns the safe fallback BEFORE touching the log (a `Pending`-mode read would otherwise occur).
    let log = MockLog {
      mode: LogMode::Pending,
    };
    let at_ceiling = FailoverReadWindow::new(Index::new(u64::MAX), Index::new(u64::MAX));
    assert_eq!(
      read_limbo(&log, &at_ceiling, u64::MAX),
      Ok(None),
      "the index-ceiling fallback short-circuits before the log read"
    );
  }

  /// The checked/unchecked failover split: with a NON-empty limbo the UNCHECKED wrapper still runs a
  /// limbo-ignoring closure (serving stale — the documented obligation the API cannot enforce), while the
  /// CHECKED wrapper declines regardless of the closure. With an EMPTY limbo the checked wrapper serves.
  #[test]
  fn failover_split_checked_gates_on_empty_limbo() {
    use sailing_proto::Term;

    let fsm = 42u64;
    let win = FailoverReadWindow::new(Index::new(5), Index::new(6));
    let nonempty = [Entry::new(
      Term::ZERO,
      Index::new(6),
      EntryKind::Empty,
      bytes::Bytes::new(),
    )];
    let empty: [Entry; 0] = [];

    // UNCHECKED, non-empty limbo: a limbo-ignoring closure serves — the stale-read hazard the split names.
    let out: FailoverOutcome<'_, u64, u64> = Ok(Some((&fsm, &nonempty, win)));
    assert_eq!(
      apply_unchecked_failover(out, |f: &u64, _limbo, _win| Some(*f)),
      Ok(Some(42))
    );

    // CHECKED, same non-empty limbo: declined regardless of the closure (the coarse-but-safe gate).
    let out: FailoverOutcome<'_, u64, u64> = Ok(Some((&fsm, &nonempty, win)));
    assert_eq!(
      apply_checked_failover(out, |f: &u64, _win| Some(*f)),
      Ok(None)
    );

    // CHECKED, empty limbo: the gate is satisfied, the closure serves.
    let out: FailoverOutcome<'_, u64, u64> = Ok(Some((&fsm, &empty, win)));
    assert_eq!(
      apply_checked_failover(out, |f: &u64, _win| Some(*f)),
      Ok(Some(42))
    );
  }
}
