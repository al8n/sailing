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

use sailing_proto::{Entry, EntryKind, Event, FailoverReadWindow, Index, LogStore};

use crate::DriverError;

// ── submit budget ─────────────────────────────────────────────────────────────

/// The shared in-flight submit budget (count AND payload bytes), cloned into every
/// [`Handle`](crate::Handle) — the caps apply across all clones, not per clone.
#[derive(Clone)]
pub(crate) struct InflightBudget {
  count: Arc<AtomicUsize>,
  bytes: Arc<AtomicUsize>,
  max_count: usize,
  max_bytes: usize,
}

impl InflightBudget {
  pub(crate) fn new(max_count: usize, max_bytes: usize) -> Self {
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
  pub(crate) fn try_reserve<I>(&self, len: usize) -> Result<ReservationGuard, DriverError<I>> {
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
pub(crate) struct ReservationGuard {
  budget: InflightBudget,
  len: usize,
}

impl Drop for ReservationGuard {
  fn drop(&mut self) {
    self.budget.count.fetch_sub(1, Ordering::AcqRel);
    self.budget.bytes.fetch_sub(self.len, Ordering::AcqRel);
  }
}

// ── pending completions ───────────────────────────────────────────────────────

/// What a parked operation is waiting for, keyed by log index.
pub(crate) enum Pending<I, R> {
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

/// The type-erased completion of a linearizable query: called with `Ok(&F)` ON the driver
/// thread to run the query (the result ships through the captured channel), or with the error
/// that voided it — one closure, so the caller keeps full error fidelity across the erasure.
pub(crate) type QueryComplete<I, F> = Box<dyn FnOnce(Result<&F, DriverError<I>>) + Send>;

/// The argument a [`FailoverComplete`] receives: the served `(FSM, limbo entries, window)` triple to
/// run the read against, `None` when no serve window is available (the caller falls back to a normal
/// read), or the error that voided the query.
pub(crate) type FailoverOutcome<'a, I, F> =
  Result<Option<(&'a F, &'a [Entry], FailoverReadWindow)>, DriverError<I>>;

/// The type-erased completion of a failover inherited-read query. Called ON the driver thread with the
/// [`FailoverOutcome`] — the served triple (the closure confirms its key was not written in
/// `(window.index(), window.limbo_upper()]`), `Ok(None)`, or the error that voided it — one closure, so
/// the caller keeps full error fidelity across the erasure.
pub(crate) type FailoverComplete<I, F> = Box<dyn FnOnce(FailoverOutcome<'_, I, F>) + Send>;

/// A parked failover inherited-read query: held until the committed prefix applies AND the serve
/// window is (re-)confirmed live, then run against the state machine with the limbo region. The
/// completion is type-erased and carries its own reply channel.
pub(crate) struct ParkedFailover<I, F> {
  /// The SINGLE completion (see [`FailoverComplete`]).
  pub(crate) complete: FailoverComplete<I, F>,
  /// The budget reservation (a failover query reserves one zero-byte slot).
  pub(crate) _reservation: ReservationGuard,
}

/// The limbo region `(window.index(), window.limbo_upper()]` filtered to NORMAL entries — the
/// committed-but-possibly-superseded prefix a failover inherited read's key must be absent from —
/// read under a HARD `max_bytes` cap and FAIL-CLOSED. Shared by both drivers so the bound and the
/// fail-closed rule cannot drift.
///
/// Returns `None` (the caller falls back to a normal read) when the log read errors OR the `max_bytes`
/// cap TRUNCATES the region. The post-election limbo can be an arbitrarily large inherited tail, and a
/// failover read reserves a zero-byte budget slot, so an unbounded scan would let one read OOM/stall
/// the driver: capping it and refusing a partial limbo (which could not prove the key absent) keeps a
/// read-only query from becoming an availability failure. The region is contiguous and above the commit
/// (never compacted), so a COMPLETE read's last entry reaches `limbo_upper`; a short tail means the cap
/// bit. An empty `Vec` when the region is empty.
pub(crate) fn read_limbo<L: LogStore>(
  log: &L,
  window: &FailoverReadWindow,
  max_bytes: u64,
) -> Option<Vec<Entry>> {
  let (lo, hi) = limbo_bounds(window)?;
  if lo >= hi {
    return Some(Vec::new());
  }
  let entries = log.entries(lo..hi, max_bytes).ok()?;
  contiguous_normal_entries(entries, lo, hi, max_bytes)
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

/// A linearizable query's lifecycle: confirmed by `ReadState` (which fixes `ready_at`), then run
/// against the state machine once `applied >= ready_at`. The completion is type-erased and
/// carries its own reply channel; `F` is the driver's state-machine type.
pub(crate) struct ParkedQuery<I, F> {
  /// `None` until the matching `ReadState` arrives; then the index the apply watermark must
  /// reach before the closure may run.
  pub(crate) ready_at: Option<Index>,
  /// The SINGLE completion (see [`QueryComplete`]).
  pub(crate) complete: QueryComplete<I, F>,
  /// The budget reservation (a query reserves one zero-byte slot).
  pub(crate) _reservation: ReservationGuard,
}

/// The shared routing state both drivers thread through [`route_event`].
pub(crate) struct Routing<I, R, F> {
  /// Submit/conf completions keyed by their log index. Sound ONLY together with the
  /// sweep-on-every-`LeaderChanged` rule below: within one unbroken leadership the index is
  /// unambiguous; across a change, swept entries can never be completed by a recycled index.
  pub(crate) pending: BTreeMap<Index, Pending<I, R>>,
  /// Linearizable queries keyed by the read context this driver minted (a monotone counter's
  /// big-endian bytes — never reused within a driver's lifetime).
  pub(crate) queries: BTreeMap<u64, ParkedQuery<I, F>>,
  /// The next read-context value.
  pub(crate) next_query_ctx: u64,
  /// Parked failover inherited-read queries: served as a batch each pass once the serve window is
  /// confirmed live and the committed prefix has applied (see the drivers' `run_failover_serve`).
  /// Swept by [`Self::fail_all`] on every leadership change, exactly like the parked queries.
  pub(crate) failovers: Vec<ParkedFailover<I, F>>,
  /// The apply watermark (highest `Applied`/`ConfChanged` index seen): gates query execution.
  pub(crate) applied: Index,
  /// The best-effort events tail (dropped-on-full; never blocks the driver).
  pub(crate) events: flume::Sender<Event<I, R>>,
}

impl<I, R, F> Routing<I, R, F> {
  pub(crate) fn new(events: flume::Sender<Event<I, R>>) -> Self {
    Self {
      pending: BTreeMap::new(),
      queries: BTreeMap::new(),
      next_query_ctx: 1,
      failovers: Vec::new(),
      applied: Index::ZERO,
      events,
    }
  }

  /// Mint a fresh, never-reused read context for a query.
  pub(crate) fn mint_query_ctx(&mut self) -> u64 {
    let ctx = self.next_query_ctx;
    self.next_query_ctx += 1;
    ctx
  }

  /// Queries whose confirmed read index the apply watermark has reached — popped for execution
  /// against the state machine (the DRIVER runs them; this module only books them).
  pub(crate) fn take_runnable_queries(&mut self) -> Vec<ParkedQuery<I, F>> {
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
  pub(crate) fn fail_all(&mut self, err: &DriverError<I>)
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
    for (_, q) in std::mem::take(&mut self.queries) {
      (q.complete)(Err(err.clone()));
    }
    for p in std::mem::take(&mut self.failovers) {
      (p.complete)(Err(err.clone()));
    }
  }
}

impl<I: sailing_proto::NodeId, R: Clone, F> Routing<I, R, F> {
  /// Route one coordinator event: complete the matching pending waiter, advance the apply
  /// watermark, mark confirmed queries ready, sweep on leadership changes — and forward a copy
  /// to the best-effort events tail. Returns `true` if the apply watermark advanced (the driver
  /// then runs [`Self::take_runnable_queries`] against the state machine).
  pub(crate) fn route_event(&mut self, event: Event<I, R>) -> bool {
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
  fn query_becomes_runnable_when_applied_reaches_its_read_index() {
    let (mut r, _rx) = routing();
    let b = InflightBudget::new(8, 8);
    let ctx = r.mint_query_ctx();
    r.queries.insert(
      ctx,
      ParkedQuery {
        ready_at: None,
        complete: Box::new(|_| {}),
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
        complete: Box::new(|_| {}),
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
}
