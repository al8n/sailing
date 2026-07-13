use std::task::{Context, Poll};

use agnostic::tokio::TokioRuntime;
use sailing_proto::{ClusterId, Data, LabelOptions, Labeled, Passthrough};

use super::*;

const ELECTION: Duration = Duration::from_millis(100);
const HEARTBEAT: Duration = Duration::from_millis(20);

thread_local! {
  /// When armed, `MergeSm::absorb` REFUSES — the deterministic `MergeUnsupported` fail-stop the merge
  /// resolve arm turns into a `CaptureFailed` resolution (a post-removal failure: the source endpoint
  /// is already consumed). Thread-local so a `#[tokio::test]` running on its own libtest thread arms
  /// it in isolation; a guard disarms it even on panic, so a single-threaded test run stays clean.
  static FAIL_ABSORB: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Arms [`FAIL_ABSORB`] for its lifetime, disarming on drop (panic included).
struct FailAbsorbGuard;

impl FailAbsorbGuard {
  fn arm() -> Self {
    FAIL_ABSORB.with(|f| f.set(true));
    FailAbsorbGuard
  }
}

impl Drop for FailAbsorbGuard {
  fn drop(&mut self) {
    FAIL_ABSORB.with(|f| f.set(false));
  }
}

/// A counter with absorb support: the merged union's total is the two counters' sum.
#[derive(Default)]
struct MergeSm(u64);

impl StateMachine for MergeSm {
  type Command = Bytes;
  type Response = u64;
  type Snapshot = u64;
  type Error = core::convert::Infallible;

  fn apply(&mut self, _: Index, _: Bytes) -> Result<u64, Self::Error> {
    self.0 += 1;
    Ok(self.0)
  }

  fn snapshot(&self) -> Result<u64, Self::Error> {
    Ok(self.0)
  }

  fn restore(&mut self, s: u64) -> Result<(), Self::Error> {
    self.0 = s;
    Ok(())
  }

  fn absorb(&mut self, source: Self) -> bool {
    if FAIL_ABSORB.with(|f| f.get()) {
      return false;
    }
    self.0 += source.0;
    true
  }

  fn supports_absorb(&self) -> bool {
    true
  }
}

type Driver = MultiReactorStreamDriver<TokioRuntime, u64, u64, MergeSm, Labeled<Passthrough>>;

/// Bind one EMPTY peerless host on a loopback port (single-voter groups never touch the wire),
/// stepped manually below — `storage_crank` and `pump` are called crank by crank, which is what
/// makes the one-crank window between a merge's resolution and its barrier observable at all.
async fn bind_host() -> (Driver, MultiHandle<u64, u64, MergeSm>) {
  let mut local = Vec::new();
  1u64.encode(&mut local);
  let dial_local = local.clone();
  let dialer: DialerFactory<u64, Labeled<Passthrough>> = Arc::new(move |_: &u64| {
    Labeled::dialer(
      Passthrough::new(),
      &LabelOptions {
        cluster: ClusterId([7; 16]),
        local_id: dial_local.clone(),
      },
    )
    .map_err(io::Error::other)
  });
  let acceptor: AcceptorFactory<Labeled<Passthrough>> = Arc::new(move || {
    Labeled::acceptor(
      Passthrough::new(),
      &LabelOptions {
        cluster: ClusterId([7; 16]),
        local_id: local.clone(),
      },
    )
    .map_err(io::Error::other)
  });
  Driver::bind(
    "127.0.0.1:0".parse().unwrap(),
    Vec::new(),
    dialer,
    acceptor,
    DriverConfig::default(),
  )
  .await
  .expect("the empty host binds")
}

/// One manual crank: the run loop's post-wake tail (commands, then storage, then pump).
async fn crank(driver: &mut Driver) {
  let now = driver.clock.now();
  while let Ok(cmd) = driver.commands.try_recv() {
    driver.handle_command(now, cmd);
  }
  driver.storage_crank(now);
  driver.pump(now).await;
}

/// Resolve a handle future by interleaving polls with manual cranks (the driver is not spawned).
async fn drive<T>(driver: &mut Driver, fut: impl Future<Output = T>) -> T {
  let mut fut = std::pin::pin!(fut);
  for _ in 0..64 {
    let waker = futures_util::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
      return v;
    }
    crank(driver).await;
  }
  panic!("the command future did not resolve within the crank budget");
}

async fn elect(driver: &mut Driver, gid: u64) {
  let deadline = std::time::Instant::now() + Duration::from_secs(10);
  loop {
    assert!(
      std::time::Instant::now() < deadline,
      "group {gid}: no leader in time"
    );
    let now = driver.clock.now();
    driver.fire_timeouts(now);
    crank(driver).await;
    if driver
      .coord
      .group(&gid)
      .is_some_and(|ep| ep.role().is_leader())
    {
      return;
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
  }
}

fn hosts(driver: &Driver, gid: u64) -> bool {
  driver.engine.group_ids().any(|g| *g == gid)
}

/// A stand-in for a guard whose `Drop` mutates state aliased into a group's replicated FSM (a `Cell`, a
/// lock the closure shares with the state machine) and panics doing so. A query closure that captures
/// one is dropped UNUSED on every arm that does not serve — `res.map(f)` on an `Err` consumes `f`
/// without calling it — so the guard's `Drop` runs INSIDE the completion's `catch_unwind` and the
/// completion reports `Panicked`. This is the mechanism, not a synthesized outcome.
struct PanicOnDrop;

impl Drop for PanicOnDrop {
  fn drop(&mut self) {
    panic!("the captured guard's Drop ran and tore the state machine");
  }
}

/// A parked query's completion in the exact shape the handle builds one (`res.map(f)` under
/// `catch_unwind`), whose user closure captures a [`PanicOnDrop`]. Used where the query must survive,
/// parked, until a sweep completes it — which no public API can arrange, since a leader confirms and
/// serves a real query within the same crank it is issued.
fn drop_panic_completion() -> sailing_driver::shared::QueryComplete<u64, MergeSm> {
  let guard = PanicOnDrop;
  Box::new(
    move |res: Result<&MergeSm, sailing_driver::DriverError<u64>>| {
      let f = move |sm: &MergeSm| {
        let _guard = guard;
        sm.0
      };
      // The handle reports this through the crate-private `CompletionOutcome::caught`; spelled out
      // here because a completion built outside `sailing-driver` cannot reach it.
      if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| res.map(f))).is_err() {
        sailing_driver::shared::CompletionOutcome::Panicked
      } else {
        sailing_driver::shared::CompletionOutcome::Delivered
      }
    },
  )
}

/// The `Merged` event is the application's permission to retire the source's external state —
/// so it must not surface while the absorb's capture, the terminal floor, and the source's
/// removal are only STAGED. The crank that resolves the merge stages those writes AFTER its own
/// `engine.flush()` ran; the next crank's barrier is the first that covers them. Stepping the
/// real crank fns (the in-crate seam `a_parked_merge_blocks_quiescence` steps the container the
/// same way), the resolving crank's pump must deliver NO `Merged` to the app tail, and the crank
/// after the covering barrier exactly one — the fork barrier's `SplitApplied` ordering, mirrored.
#[tokio::test]
async fn a_merged_event_surfaces_only_after_its_barrier() {
  let (mut driver, handle) = bind_host().await;
  for gid in [1u64, 2] {
    let cfg = Config::try_new(1u64, vec![1], ELECTION, HEARTBEAT).unwrap();
    let fut = handle.create_group(gid, cfg, 7, MergeSm::default(), 0);
    drive(&mut driver, fut).await.expect("group admission");
    elect(&mut driver, gid).await;
  }

  // The encoding-minimal id survives: 1 is the target, 2 the source that dissolves.
  drive(&mut driver, handle.prepare_merge(2, 1))
    .await
    .expect("freeze proposed");
  let deadline = std::time::Instant::now() + Duration::from_secs(10);
  while !driver.coord.group(&2).is_some_and(|ep| ep.is_frozen()) {
    assert!(std::time::Instant::now() < deadline, "no freeze in time");
    crank(&mut driver).await;
  }
  drive(&mut driver, handle.commit_merge(1, 2))
    .await
    .expect("commit proposed");

  let events = handle.events();
  let mut merged_before_barrier = 0usize;
  let mut merged_after_barrier = 0usize;
  let mut resolved = false;
  let mut post_resolution_cranks = 0usize;
  for _ in 0..64 {
    let source_hosted = hosts(&driver, 2);
    let now = driver.clock.now();
    driver.storage_crank(now);
    // The source teardown happens synchronously with the resolution, so losing group 2 THIS
    // crank means `service_merge_applies` resolved THIS crank — after this crank's flush.
    let resolved_this_crank = source_hosted && !hosts(&driver, 2);
    driver.pump(now).await;
    let mut merged_this_crank = 0usize;
    while let Ok((g, ev)) = events.try_recv() {
      if matches!(ev, Event::Merged(_)) {
        assert_eq!(g, 1, "the union event is the target's");
        merged_this_crank += 1;
      }
    }
    if resolved_this_crank {
      resolved = true;
      merged_before_barrier += merged_this_crank;
    } else if resolved {
      merged_after_barrier += merged_this_crank;
      post_resolution_cranks += 1;
      // Two extra cranks after the first post-barrier delivery: pin exactly-once.
      if merged_after_barrier > 0 && post_resolution_cranks > 3 {
        break;
      }
    }
  }
  assert!(resolved, "the parked merge never resolved");
  assert_eq!(
    merged_before_barrier, 0,
    "Merged reached the app tail before the barrier covering its snapshot/floor/removal"
  );
  assert_eq!(
    merged_after_barrier, 1,
    "exactly one Merged surfaces once the barrier has covered the staged writes"
  );
}

/// A completion swept by `fail_all` (a `LeaderChanged`) can catch a user-closure(-drop) panic: the
/// swept closure is dropped unused, its captured guard's `Drop` mutates shared FSM state and panics,
/// caught by the handle's `catch_unwind`. Discarding that `Panicked` outcome would leave a group LIVE
/// against possibly-torn state, so `fail_all` latches it and the per-group tail folds the latch. The
/// tear is UNATTRIBUTABLE — the swept closure captured arbitrary aliasing state — so the fold is
/// PLANE-FATAL: every hosted group poisons, the co-located sibling included. The parked query reports
/// the caught-panic outcome directly (as `shared::query_batch_stops_serving_at_the_first_caught_panic`
/// does), avoiding a real unwind.
#[tokio::test]
async fn a_swept_completion_panic_fail_stops_the_whole_plane() {
  let (mut driver, handle) = bind_host().await;
  for gid in [1u64, 2] {
    let cfg = Config::try_new(1u64, vec![1], ELECTION, HEARTBEAT).unwrap();
    let fut = handle.create_group(gid, cfg, 7, MergeSm::default(), 0);
    drive(&mut driver, fut).await.expect("group admission");
    elect(&mut driver, gid).await;
  }
  // Park a query on group 1 that never becomes runnable (`ready_at` stays `None`), so ONLY a
  // `fail_all` sweep can complete it. Its completion reports the caught-panic outcome on the `Err`
  // supersede — the outcome the handle's `catch_unwind` returns when a swept closure's captured
  // guard `Drop` panics.
  let budget = sailing_driver::shared::InflightBudget::new(8, 8);
  {
    let routing = driver.routing.get_mut(&1).expect("group 1 routes");
    let ctx = routing.mint_query_ctx();
    routing.queries.insert(
      ctx,
      sailing_driver::shared::ParkedQuery {
        ready_at: None,
        complete: Box::new(|res: Result<&MergeSm, sailing_driver::DriverError<u64>>| {
          // The outcome the handle's `catch_unwind` returns when a swept closure's captured guard
          // `Drop` panics on the `Err` supersede — `Panicked`; anything else `Delivered`.
          if matches!(res, Err(sailing_driver::DriverError::Superseded)) {
            sailing_driver::shared::CompletionOutcome::Panicked
          } else {
            sailing_driver::shared::CompletionOutcome::Delivered
          }
        }),
        _reservation: budget.try_reserve::<u64>(0).unwrap(),
      },
    );
    // The `LeaderChanged` sweep, invoked exactly as `route_event`'s `LeaderChanged` arm does.
    // (Synthesizing a real single-voter deposition is not cheap; the swept completion path is
    // identical, and the discarded-`Panicked` defect lived entirely in `fail_all`.)
    routing.fail_all(&sailing_driver::DriverError::Superseded);
  }
  // The next crank's per-group tail folds the latched completion panic into a PLANE-FATAL fail-stop.
  crank(&mut driver).await;
  for gid in [1u64, 2] {
    assert!(
      driver.coord.group(&gid).is_some_and(|ep| ep.is_poisoned()),
      "the swept completion's caught panic is unattributable: group {gid} must fail-stop"
    );
  }
  // The co-located sibling can no longer serve — the whole plane fail-stopped, not just group 1.
  match drive(&mut driver, handle.group(2).query(|sm: &MergeSm| sm.0)).await {
    Err(sailing_driver::DriverError::Poisoned) => {}
    other => panic!("the sibling must fail-stop on the plane-fatal panic, got {other:?}"),
  }
}

/// The regression guard: a WELL-BEHAVED `fail_all(Superseded)` sweep (its completion `Delivered`)
/// must NOT poison the group — a superseded group is not a poisoned one. Pins that the latch does not
/// over-fail-stop on the common leadership-change sweep.
#[tokio::test]
async fn a_normal_supersede_sweep_does_not_poison_the_group() {
  let (mut driver, handle) = bind_host().await;
  let cfg = Config::try_new(1u64, vec![1], ELECTION, HEARTBEAT).unwrap();
  let fut = handle.create_group(1u64, cfg, 7, MergeSm::default(), 0);
  drive(&mut driver, fut).await.expect("group admission");
  elect(&mut driver, 1).await;
  let budget = sailing_driver::shared::InflightBudget::new(8, 8);
  {
    let routing = driver.routing.get_mut(&1).expect("group 1 routes");
    let ctx = routing.mint_query_ctx();
    routing.queries.insert(
      ctx,
      sailing_driver::shared::ParkedQuery {
        ready_at: None,
        complete: Box::new(|_res: Result<&MergeSm, sailing_driver::DriverError<u64>>| {
          sailing_driver::shared::CompletionOutcome::Delivered
        }),
        _reservation: budget.try_reserve::<u64>(0).unwrap(),
      },
    );
    routing.fail_all(&sailing_driver::DriverError::Superseded);
  }
  crank(&mut driver).await;
  assert!(
    driver.coord.group(&1).is_some_and(|ep| !ep.is_poisoned()),
    "a well-behaved supersede sweep must not poison the group"
  );
  // The group still commits (it was superseded, not poisoned).
  let g1 = handle.group(1);
  drive(&mut driver, g1.submit(Bytes::from_static(b"y")))
    .await
    .expect("a superseded (not poisoned) group keeps committing");
}

/// The same-crank ordering window: a failover DECLINE processed earlier in a crank latches a caught
/// completion(-drop) panic — its dropped closure's captured guard `Drop` tore the FSM — and the
/// still-parked NORMAL query must NOT then be served against that torn FSM before the tail fail-stops.
/// The tail reads the completion latch BEFORE serving, so a pre-serve latch SKIPS the serve: the parked
/// query completes `Poisoned` from the fail-stop sweep (its served `Ok(&fsm)` arm never runs). The tear
/// is UNATTRIBUTABLE, so the fail-stop is PLANE-FATAL: the co-located sibling poisons too, and because
/// the sorted per-group tail fires the plane fail-stop before it reaches the sibling, the sibling's
/// runnable query is likewise skipped and completes `Poisoned` — not served. Without the pre-serve read
/// the torn group's query is served against the torn FSM. The decline's caught panic is reported
/// directly, as `a_swept_completion_panic_fail_stops_the_whole_plane` does, avoiding a real unwind.
#[tokio::test]
async fn a_pre_serve_completion_panic_skips_the_serve_and_fail_stops_the_plane() {
  use std::sync::atomic::{AtomicBool, Ordering};
  let (mut driver, handle) = bind_host().await;
  for gid in [1u64, 2] {
    let cfg = Config::try_new(1u64, vec![1], ELECTION, HEARTBEAT).unwrap();
    let fut = handle.create_group(gid, cfg, 7, MergeSm::default(), 0);
    drive(&mut driver, fut).await.expect("group admission");
    elect(&mut driver, gid).await;
    // Commit one entry so each endpoint's applied index is past zero: resetting a group's routing
    // watermark to zero below then makes `sync_applied` advance it this crank, which is exactly what
    // sets `run_queries` — the query serve runs only when the watermark moved.
    drive(
      &mut driver,
      handle.group(gid).submit(Bytes::from_static(b"warm")),
    )
    .await
    .expect("warm-up commit");
  }

  let budget = sailing_driver::shared::InflightBudget::new(8, 8);
  let served_1 = Arc::new(AtomicBool::new(false));
  let poisoned_1 = Arc::new(AtomicBool::new(false));
  {
    let routing = driver.routing.get_mut(&1).expect("group 1 routes");
    // A parked failover the crank DECLINES (`failover_read_window` is `None` off the failover tier),
    // whose dropped closure reports `Panicked` — the outcome the handle's `catch_unwind` returns when a
    // decline drops a captured guard whose `Drop` panics. This latches BEFORE the query serve.
    routing
      .failovers
      .push(sailing_driver::shared::ParkedFailover {
        complete: Box::new(
          |res: sailing_driver::shared::FailoverOutcome<'_, u64, MergeSm>| {
            if matches!(res, Ok(None)) {
              sailing_driver::shared::CompletionOutcome::Panicked
            } else {
              sailing_driver::shared::CompletionOutcome::Delivered
            }
          },
        ),
        _reservation: budget.try_reserve::<u64>(0).unwrap(),
      });
    // A RUNNABLE normal query on the SAME group: its served arm (`Ok(&fsm)`) must never run against the
    // FSM the decline tore — it must instead complete `Poisoned` from the fail-stop sweep.
    let served = served_1.clone();
    let poisoned = poisoned_1.clone();
    let ctx = routing.mint_query_ctx();
    routing.queries.insert(
      ctx,
      sailing_driver::shared::ParkedQuery {
        ready_at: Some(Index::ZERO),
        complete: Box::new(
          move |res: Result<&MergeSm, sailing_driver::DriverError<u64>>| {
            match res {
              Ok(_) => served.store(true, Ordering::Relaxed),
              Err(sailing_driver::DriverError::Poisoned) => poisoned.store(true, Ordering::Relaxed),
              Err(_) => {}
            }
            sailing_driver::shared::CompletionOutcome::Delivered
          },
        ),
        _reservation: budget.try_reserve::<u64>(0).unwrap(),
      },
    );
    routing.applied = Index::ZERO;
  }
  // A co-located sibling's runnable query with NO failover decline: under the OLD group-scoped policy it
  // served this crank; under the plane-fatal policy the sorted tail poisons it (group 1 fires the plane
  // fail-stop before the tail reaches group 2), so its runnable query is skipped and completes `Poisoned`.
  let served_2 = Arc::new(AtomicBool::new(false));
  {
    let served = served_2.clone();
    let routing = driver.routing.get_mut(&2).expect("group 2 routes");
    let ctx = routing.mint_query_ctx();
    routing.queries.insert(
      ctx,
      sailing_driver::shared::ParkedQuery {
        ready_at: Some(Index::ZERO),
        complete: Box::new(
          move |res: Result<&MergeSm, sailing_driver::DriverError<u64>>| {
            if res.is_ok() {
              served.store(true, Ordering::Relaxed);
            }
            sailing_driver::shared::CompletionOutcome::Delivered
          },
        ),
        _reservation: budget.try_reserve::<u64>(0).unwrap(),
      },
    );
    routing.applied = Index::ZERO;
  }

  crank(&mut driver).await;

  assert!(
    !served_1.load(Ordering::Relaxed),
    "the parked query must NOT be served against the FSM the same-crank decline tore"
  );
  assert!(
    poisoned_1.load(Ordering::Relaxed),
    "the skipped query completes Poisoned from the group fail-stop instead"
  );
  assert!(
    driver.coord.group(&1).is_some_and(|ep| ep.is_poisoned()),
    "the pre-serve completion panic fail-stopped group 1"
  );
  assert!(
    !served_2.load(Ordering::Relaxed),
    "the co-located sibling's runnable query is skipped too — the plane fail-stopped before its tail ran"
  );
  assert!(
    driver.coord.group(&2).is_some_and(|ep| ep.is_poisoned()),
    "the sibling poisons on the plane-fatal fail-stop"
  );
}

/// The immediate-refusal arm. `read_index` on a group that is not a leader refuses IMMEDIATELY, and the
/// refusal completes with `Err`: it never CALLS the user closure, but it DROPS it unused inside the
/// completion's `catch_unwind`, so a guard the closure captured runs its `Drop` there and a panicking
/// `Drop` is caught and reported `Panicked`. A refusal path that discards that outcome leaves a group
/// LIVE against state the guard could have torn — the reason the outcome is `#[must_use]`, so the
/// compiler (not a convention) enumerates the sites. The guard captured arbitrary state, so the tear is
/// UNATTRIBUTABLE: the refusal fail-stops the WHOLE plane — the addressed group AND its co-located
/// sibling — each surfacing its own `Poisoned` on the lifecycle tail.
#[tokio::test]
async fn an_immediate_refusal_drop_panic_fail_stops_the_whole_plane() {
  let (mut driver, handle) = bind_host().await;
  // Neither group is elected — `crank` never fires timeouts — so each is a fresh follower that knows no
  // leader, and `read_index` refuses with `NoLeader`. That is the HOSTED-group refusal arm, reached with
  // the group fully alive and its state machine fully exposed to the closure.
  for gid in [1u64, 2] {
    let cfg = Config::try_new(1u64, vec![1], ELECTION, HEARTBEAT).unwrap();
    let fut = handle.create_group(gid, cfg, 7, MergeSm::default(), 0);
    drive(&mut driver, fut).await.expect("group admission");
  }

  let guard = PanicOnDrop;
  let refused: Result<u64, _> = drive(
    &mut driver,
    handle.group(1).query(move |sm: &MergeSm| {
      // Owned by this closure, so it dies with it: on the refusal arm the closure is dropped unused,
      // inside the completion's unwind boundary, and the guard's `Drop` panics THERE.
      let _guard = guard;
      sm.0
    }),
  )
  .await;
  assert!(
    matches!(refused, Err(sailing_driver::DriverError::QueryPanicked)),
    "the caller learns its closure panicked, not the read's refusal reason"
  );
  // EVERY hosted group fail-stops: the dropped guard's tear is unattributable, so the refusal is
  // plane-fatal — group 1 the addressed one AND group 2 the co-located sibling.
  for gid in [1u64, 2] {
    assert!(
      driver.coord.group(&gid).is_some_and(|ep| ep.is_poisoned()),
      "group {gid} must fail-stop on the unattributable plane-fatal refusal"
    );
  }
  // The sibling can no longer serve — the whole plane fail-stopped, never a group-scoped stop.
  match drive(&mut driver, handle.group(2).query(|sm: &MergeSm| sm.0)).await {
    Err(sailing_driver::DriverError::Poisoned) => {}
    other => panic!("the co-located sibling must fail-stop, got {other:?}"),
  }
  // Each fail-stop surfaces once on the lifecycle tail — the plane fails LOUDLY, never silently.
  let mut poisoned: Vec<u64> = handle
    .lifecycle()
    .try_iter()
    .filter_map(|ev| match ev {
      LifecycleEvent::Poisoned { group } => Some(group),
      _ => None,
    })
    .collect();
  poisoned.sort_unstable();
  assert_eq!(
    poisoned,
    vec![1, 2],
    "every hosted group surfaces its poison on the lifecycle tail"
  );
}

/// The no-over-fail-stop guard for the immediate-refusal arm: a WELL-BEHAVED closure refused the same way
/// must NOT poison its group. A refusal is not a torn state machine — the caller simply gets the read's refusal
/// reason and the group goes on to elect and commit.
#[tokio::test]
async fn a_normal_immediate_refusal_does_not_poison_the_group() {
  let (mut driver, handle) = bind_host().await;
  let cfg = Config::try_new(1u64, vec![1], ELECTION, HEARTBEAT).unwrap();
  let fut = handle.create_group(1u64, cfg, 7, MergeSm::default(), 0);
  drive(&mut driver, fut).await.expect("group admission");

  let refused: Result<u64, _> =
    drive(&mut driver, handle.group(1).query(|sm: &MergeSm| sm.0)).await;
  assert!(
    matches!(refused, Err(sailing_driver::DriverError::NotLeader { .. })),
    "a well-behaved refusal reports the read's refusal reason"
  );
  assert!(
    driver.coord.group(&1).is_some_and(|ep| !ep.is_poisoned()),
    "a well-behaved refusal must not fail-stop the group"
  );
  elect(&mut driver, 1).await;
  drive(
    &mut driver,
    handle.group(1).submit(Bytes::from_static(b"y")),
  )
  .await
  .expect("a refused (not poisoned) group keeps committing");
}

/// The merge teardown drains the source's DYING latch. The fold DETACHES the source's routing
/// (`self.routing.remove(&source)`) and only then sweeps its parked work, so a completion(-drop) panic in
/// that sweep latches into a `Routing` that is about to be DROPPED — and the pump's per-group tail can
/// never read a latch for a group that no longer routes. Drain it HERE or lose it. The panic is
/// UNATTRIBUTABLE (the swept closure captured arbitrary aliasing state), so the drained latch fires a
/// PLANE-FATAL fail-stop, in the storage crank, ahead of the pump. Here the source is already gone from
/// the container, so the sole remaining hosted group — the absorbing target, which absorbed the source's
/// state machine INTO itself — poisons: a merged target left LIVE would serve a possibly divergent union.
#[tokio::test]
async fn a_merge_teardown_drop_panic_fail_stops_the_absorbing_target() {
  let (mut driver, handle) = bind_host().await;
  for gid in [1u64, 2] {
    let cfg = Config::try_new(1u64, vec![1], ELECTION, HEARTBEAT).unwrap();
    let fut = handle.create_group(gid, cfg, 7, MergeSm::default(), 0);
    drive(&mut driver, fut).await.expect("group admission");
    elect(&mut driver, gid).await;
  }
  // Park a query on the SOURCE that can never become runnable (`ready_at` stays `None`), so only the
  // teardown's `fail_all` sweep can complete it — dropping its closure, and with it the guard.
  let budget = sailing_driver::shared::InflightBudget::new(8, 8);
  {
    let routing = driver.routing.get_mut(&2).expect("the source routes");
    let ctx = routing.mint_query_ctx();
    routing.queries.insert(
      ctx,
      sailing_driver::shared::ParkedQuery {
        ready_at: None,
        complete: drop_panic_completion(),
        _reservation: budget.try_reserve::<u64>(0).unwrap(),
      },
    );
  }

  // 1 absorbs 2: the encoding-minimal id survives, so 2 is the source that dissolves.
  drive(&mut driver, handle.prepare_merge(2, 1))
    .await
    .expect("freeze proposed");
  let deadline = std::time::Instant::now() + Duration::from_secs(10);
  while !driver.coord.group(&2).is_some_and(|ep| ep.is_frozen()) {
    assert!(std::time::Instant::now() < deadline, "no freeze in time");
    crank(&mut driver).await;
  }
  drive(&mut driver, handle.commit_merge(1, 2))
    .await
    .expect("commit proposed");
  let deadline = std::time::Instant::now() + Duration::from_secs(10);
  while hosts(&driver, 2) {
    assert!(
      std::time::Instant::now() < deadline,
      "the parked merge never resolved"
    );
    crank(&mut driver).await;
  }

  assert!(
    driver.coord.group(&1).is_some_and(|ep| ep.is_poisoned()),
    "the source teardown's drop-panic tore state the TARGET absorbed: the target must fail-stop"
  );
  // And it must not SERVE: a fail-stopped target refuses the read rather than answer from a union the
  // guard's `Drop` could have diverged.
  let served: Result<u64, _> = drive(&mut driver, handle.group(1).query(|sm: &MergeSm| sm.0)).await;
  assert!(
    matches!(served, Err(sailing_driver::DriverError::Poisoned)),
    "the fail-stopped target must not serve a read against the possibly-divergent union"
  );
}

/// A fail-stopped group must never SERVE a query that was already confirmed and runnable when the
/// fail-stop landed — the poison gate on the tail's query serve, which the merge teardown is the
/// reachable producer of.
///
/// The target absorbs the source's state machine, so a drop-panic in the source's teardown sweep tore
/// state the TARGET now owns; the fold fail-stops the target for it, in the storage crank, ahead of the
/// pump. But that fail-stop lands on the ENDPOINT, not on the target's routing: the panic latched in the
/// SOURCE's routing, which the fold drained and dropped. The target's own completion latch is therefore
/// CLEAR (asserted below), and `is_poisoned()` is the only thing left standing between a confirmed
/// (`ready_at: Some(..)`, apply watermark reached) parked query and a serve against the possibly-torn
/// absorbed union.
///
/// The tail is driven directly with `run_queries` set, which is the state the pump's own set produces
/// for a group whose routed event advanced its watermark this pass — `route_event` never consults
/// poison. (Through the real pump the state is currently double-covered: a poisoned endpoint also
/// suppresses `poll_event`, so the pump's set cannot name this group and the serve short-circuits on
/// `run_queries` before reaching the poison gate. That suppression is a PROTO property; this gate is
/// the driver's own, and this test pins it independently of the proto's.)
///
/// Without the poison gate the confirmed query is served `Ok(&fsm)` against the absorbed union
/// (`observed` = the merged total) instead of completing `Poisoned`.
#[tokio::test]
async fn a_fail_stopped_merge_target_never_serves_its_confirmed_parked_query() {
  use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
  let (mut driver, handle) = bind_host().await;
  for gid in [1u64, 2] {
    let cfg = Config::try_new(1u64, vec![1], ELECTION, HEARTBEAT).unwrap();
    let fut = handle.create_group(gid, cfg, 7, MergeSm::default(), 0);
    drive(&mut driver, fut).await.expect("group admission");
    elect(&mut driver, gid).await;
    // One commit per group, so the union the target absorbs is a value a served read could OBSERVE
    // (1 + 1 = 2) — a serve against torn state is then visible, not merely inferred.
    drive(
      &mut driver,
      handle.group(gid).submit(Bytes::from_static(b"warm")),
    )
    .await
    .expect("warm-up commit");
  }

  let budget = sailing_driver::shared::InflightBudget::new(8, 8);
  // The SOURCE's parked query: never runnable (`ready_at` stays `None`), so only the teardown's
  // `fail_all` sweep can complete it — dropping its closure unused, and with it the panicking guard.
  {
    let routing = driver.routing.get_mut(&2).expect("the source routes");
    let ctx = routing.mint_query_ctx();
    routing.queries.insert(
      ctx,
      sailing_driver::shared::ParkedQuery {
        ready_at: None,
        complete: drop_panic_completion(),
        _reservation: budget.try_reserve::<u64>(0).unwrap(),
      },
    );
  }
  // The TARGET's parked query: CONFIRMED and RUNNABLE (`ready_at` at or below the watermark), the
  // exact shape a read-index confirmation leaves parked while its apply watermark is already past it.
  let served = Arc::new(AtomicBool::new(false));
  let observed = Arc::new(AtomicU64::new(0));
  let poisoned_verdict = Arc::new(AtomicBool::new(false));
  {
    let routing = driver.routing.get_mut(&1).expect("the target routes");
    let ctx = routing.mint_query_ctx();
    let served = served.clone();
    let observed = observed.clone();
    let verdict = poisoned_verdict.clone();
    routing.queries.insert(
      ctx,
      sailing_driver::shared::ParkedQuery {
        ready_at: Some(Index::ZERO),
        complete: Box::new(
          move |res: Result<&MergeSm, sailing_driver::DriverError<u64>>| {
            match res {
              Ok(sm) => {
                served.store(true, Ordering::Relaxed);
                observed.store(sm.0, Ordering::Relaxed);
              }
              Err(sailing_driver::DriverError::Poisoned) => verdict.store(true, Ordering::Relaxed),
              Err(_) => {}
            }
            sailing_driver::shared::CompletionOutcome::Delivered
          },
        ),
        _reservation: budget.try_reserve::<u64>(0).unwrap(),
      },
    );
    // Pin the driver watermark where nothing can ADVANCE it: an advance is what sets `run_queries`, and
    // the still-healthy target would then serve this query in one of the cranks the merge itself takes.
    // The query has to reach the teardown still parked — that is the state the gate protects — and it
    // stays RUNNABLE throughout (`ready_at` is at the floor, the watermark at the ceiling).
    routing.applied = Index::new(u64::MAX);
  }

  // 1 absorbs 2: the encoding-minimal id survives, so 2 is the source that dissolves.
  drive(&mut driver, handle.prepare_merge(2, 1))
    .await
    .expect("freeze proposed");
  let deadline = std::time::Instant::now() + Duration::from_secs(10);
  while !driver.coord.group(&2).is_some_and(|ep| ep.is_frozen()) {
    assert!(std::time::Instant::now() < deadline, "no freeze in time");
    crank(&mut driver).await;
  }
  drive(&mut driver, handle.commit_merge(1, 2))
    .await
    .expect("commit proposed");

  // Step the crank halves apart and STOP at the resolving storage crank: the fold has torn the source
  // down and fail-stopped the target, and the target's confirmed query is still parked. That is the
  // instant the tail must not serve.
  let deadline = std::time::Instant::now() + Duration::from_secs(10);
  let mut resolved = false;
  for _ in 0..64 {
    assert!(
      std::time::Instant::now() < deadline,
      "the parked merge never resolved"
    );
    let source_hosted = hosts(&driver, 2);
    let now = driver.clock.now();
    while let Ok(cmd) = driver.commands.try_recv() {
      driver.handle_command(now, cmd);
    }
    driver.storage_crank(now);
    if source_hosted && !hosts(&driver, 2) {
      resolved = true;
      break;
    }
    driver.pump(now).await;
  }
  assert!(resolved, "the parked merge never resolved");

  assert!(
    driver.coord.group(&1).is_some_and(|ep| ep.is_poisoned()),
    "the source teardown's drop-panic tore state the TARGET absorbed: the target must fail-stop"
  );
  // The mechanism the gate rests on: the panic latched in the SOURCE's routing, which the fold drained
  // and dropped, so the TARGET's own completion latch never fired. The take is a no-op on a clear latch
  // (and leaves the tail below reading the same `false`) — it asserts that `is_poisoned` is the SOLE
  // barrier here, not a redundant second one behind the completion latch.
  assert!(
    !driver
      .routing
      .get_mut(&1)
      .expect("the target routes")
      .take_completion_panicked(),
    "the target's own completion latch is clear: the poison is the only signal it carries"
  );

  // The tail with `run_queries` SET — a routed event advancing this group's watermark produces exactly
  // this call, and `route_event` does not consult poison.
  driver.pump_group_tail(&1, true);

  assert!(
    !served.load(Ordering::Relaxed),
    "the fail-stopped target served a confirmed parked query against the absorbed union it may have torn"
  );
  assert_eq!(
    observed.load(Ordering::Relaxed),
    0,
    "the query's closure never ran, so it observed no union at all"
  );
  assert!(
    poisoned_verdict.load(Ordering::Relaxed),
    "the confirmed query completes Poisoned from the fail-stop sweep instead of being served"
  );
}

/// The no-over-fail-stop guard for the merge teardown: a WELL-BEHAVED source teardown (its swept
/// completion `Delivered`) must NOT poison the absorbing target. The merge is the common path — it must stay clean,
/// and the target must still serve the union it absorbed.
#[tokio::test]
async fn a_normal_merge_teardown_does_not_poison_the_target() {
  let (mut driver, handle) = bind_host().await;
  for gid in [1u64, 2] {
    let cfg = Config::try_new(1u64, vec![1], ELECTION, HEARTBEAT).unwrap();
    let fut = handle.create_group(gid, cfg, 7, MergeSm::default(), 0);
    drive(&mut driver, fut).await.expect("group admission");
    elect(&mut driver, gid).await;
    // One commit per group, so the absorbed union is observable (1 + 1 = 2).
    drive(
      &mut driver,
      handle.group(gid).submit(Bytes::from_static(b"warm")),
    )
    .await
    .expect("warm-up commit");
  }
  let budget = sailing_driver::shared::InflightBudget::new(8, 8);
  {
    let routing = driver.routing.get_mut(&2).expect("the source routes");
    let ctx = routing.mint_query_ctx();
    routing.queries.insert(
      ctx,
      sailing_driver::shared::ParkedQuery {
        ready_at: None,
        complete: Box::new(|_res: Result<&MergeSm, sailing_driver::DriverError<u64>>| {
          sailing_driver::shared::CompletionOutcome::Delivered
        }),
        _reservation: budget.try_reserve::<u64>(0).unwrap(),
      },
    );
  }

  drive(&mut driver, handle.prepare_merge(2, 1))
    .await
    .expect("freeze proposed");
  let deadline = std::time::Instant::now() + Duration::from_secs(10);
  while !driver.coord.group(&2).is_some_and(|ep| ep.is_frozen()) {
    assert!(std::time::Instant::now() < deadline, "no freeze in time");
    crank(&mut driver).await;
  }
  drive(&mut driver, handle.commit_merge(1, 2))
    .await
    .expect("commit proposed");
  let deadline = std::time::Instant::now() + Duration::from_secs(10);
  while hosts(&driver, 2) {
    assert!(
      std::time::Instant::now() < deadline,
      "the parked merge never resolved"
    );
    crank(&mut driver).await;
  }

  assert!(
    driver.coord.group(&1).is_some_and(|ep| !ep.is_poisoned()),
    "a well-behaved source teardown must not poison the absorbing target"
  );
  let total: u64 = drive(&mut driver, handle.group(1).query(|sm: &MergeSm| sm.0))
    .await
    .expect("the merged target still serves the union it absorbed");
  assert_eq!(total, 2, "the union is both groups' commits");
}

/// The UNATTRIBUTABLE-panic arm. A query addressed to a group this host does NOT carry is refused with
/// `no_such_group` — the completion never CALLS the closure, but it DROPS it unused inside its
/// `catch_unwind`, so a captured guard's `Drop` runs there and can panic. The library handed this
/// closure no state machine, but that bounds nothing: a `Send + 'static` closure can capture a guard
/// aliasing state a HOSTED group's replicated FSM shares (`StateMachine` imposes no isolation), tear it
/// in `Drop`, and panic. The driver cannot see what was captured, so the tear could be in ANY hosted
/// group — the panic names none. Rather than leave some torn group serving silently-divergent committed
/// state, the refusal is PLANE-FATAL: every hosted group fail-stops, surfaces its own poison, and stops
/// serving. The caught panic reports directly, as the sibling completion tests do, avoiding a real unwind.
#[tokio::test]
async fn an_unattributable_refusal_drop_panic_fail_stops_the_whole_plane() {
  let (mut driver, handle) = bind_host().await;
  for gid in [1u64, 2] {
    let cfg = Config::try_new(1u64, vec![1], ELECTION, HEARTBEAT).unwrap();
    let fut = handle.create_group(gid, cfg, 7, MergeSm::default(), 0);
    drive(&mut driver, fut).await.expect("group admission");
    elect(&mut driver, gid).await;
  }

  // A query on group 999 — never created, so NOT hosted: the refusal drops the closure unused, and its
  // captured guard's `Drop` panics inside the completion's unwind boundary.
  let guard = PanicOnDrop;
  let refused: Result<u64, _> = drive(
    &mut driver,
    handle.group(999).query(move |sm: &MergeSm| {
      let _guard = guard;
      sm.0
    }),
  )
  .await;
  assert!(
    matches!(refused, Err(sailing_driver::DriverError::QueryPanicked)),
    "the caller learns its closure panicked, not the group's absence"
  );

  // EVERY hosted group fail-stops — the panic could have torn any of them.
  for gid in [1u64, 2] {
    assert!(
      driver.coord.group(&gid).is_some_and(|ep| ep.is_poisoned()),
      "group {gid} must fail-stop on the unattributable plane-fatal panic"
    );
  }
  // Neither can serve: a fail-stopped group refuses the read rather than answer possibly-torn state.
  for gid in [1u64, 2] {
    let served: Result<u64, _> =
      drive(&mut driver, handle.group(gid).query(|sm: &MergeSm| sm.0)).await;
    assert!(
      matches!(served, Err(sailing_driver::DriverError::Poisoned)),
      "the fail-stopped group {gid} must not serve"
    );
  }
  // Each surfaces its poison ONCE on the lifecycle tail — the plane fails LOUDLY, never silently.
  let mut poisoned: Vec<u64> = handle
    .lifecycle()
    .try_iter()
    .filter_map(|ev| match ev {
      LifecycleEvent::Poisoned { group } => Some(group),
      _ => None,
    })
    .collect();
  poisoned.sort_unstable();
  assert_eq!(
    poisoned,
    vec![1, 2],
    "every hosted group surfaces its poison on the lifecycle tail"
  );
}

/// The no-over-fail-stop guard for the unattributable arm: a WELL-BEHAVED query on a not-hosted group
/// (its refused completion `Delivered`) must NOT fail-stop anything. The caller gets the group-absent
/// rejection and every hosted group keeps serving — a missing group is not a torn plane.
#[tokio::test]
async fn a_normal_unhosted_refusal_does_not_fail_stop_the_plane() {
  let (mut driver, handle) = bind_host().await;
  for gid in [1u64, 2] {
    let cfg = Config::try_new(1u64, vec![1], ELECTION, HEARTBEAT).unwrap();
    let fut = handle.create_group(gid, cfg, 7, MergeSm::default(), 0);
    drive(&mut driver, fut).await.expect("group admission");
    elect(&mut driver, gid).await;
  }
  let refused: Result<u64, _> =
    drive(&mut driver, handle.group(999).query(|sm: &MergeSm| sm.0)).await;
  assert!(
    matches!(refused, Err(sailing_driver::DriverError::Rejected { .. })),
    "a well-behaved unhosted query reports the group's absence"
  );
  for gid in [1u64, 2] {
    assert!(
      driver.coord.group(&gid).is_some_and(|ep| !ep.is_poisoned()),
      "a well-behaved unhosted refusal must not fail-stop group {gid}"
    );
  }
  // Both groups still commit — the plane is untouched.
  for gid in [1u64, 2] {
    drive(
      &mut driver,
      handle.group(gid).submit(Bytes::from_static(b"x")),
    )
    .await
    .expect("a plane with no fail-stop keeps committing");
  }
}

/// A merge whose absorb cannot be made durable CONSUMES the source endpoint before it fails — so the
/// source's parked client work is stranded on oneshots the vanished endpoint can never answer. Without
/// the `CaptureFailed` fold the resolve arm returns NO resolution, the driver never learns the source
/// left, its routing (and the parked oneshot) linger, and the caller hangs FOREVER. The fold fails that
/// routing with a typed error instead — a strictly better answer than an eternal hang — while PRESERVING
/// the source's stores and floor (the union's only copy, restored on the restart the poison forces), and
/// the absorbing target surfaces its poison. The absorb is forced to refuse (the `MergeUnsupported` arm,
/// one of the two post-removal failure paths that fold `CaptureFailed`).
#[tokio::test]
async fn a_capture_failed_merge_fails_the_source_routing_instead_of_hanging() {
  let _fail_absorb = FailAbsorbGuard::arm();
  let (mut driver, handle) = bind_host().await;
  for gid in [1u64, 2] {
    let cfg = Config::try_new(1u64, vec![1], ELECTION, HEARTBEAT).unwrap();
    let fut = handle.create_group(gid, cfg, 7, MergeSm::default(), 0);
    drive(&mut driver, fut).await.expect("group admission");
    elect(&mut driver, gid).await;
  }

  // A PENDING SUBMIT parked on the SOURCE (group 2): a committed-but-unrouted client op — the shape a
  // teardown that consumes the source endpoint strands. Nothing routes an `Applied` to its index, so
  // only the teardown's typed `fail_all` can answer it; without the fix, nothing does and it hangs.
  let budget = sailing_driver::shared::InflightBudget::new(8, 8);
  let (tx, mut rx) =
    futures_channel::oneshot::channel::<Result<u64, sailing_driver::DriverError<u64>>>();
  {
    let routing = driver.routing.get_mut(&2).expect("the source routes");
    routing.pending.insert(
      Index::new(999),
      sailing_driver::shared::Pending::Submit {
        reply: tx,
        _reservation: budget.try_reserve::<u64>(0).unwrap(),
      },
    );
  }

  // 1 absorbs 2: the encoding-minimal id survives, so 2 is the source that dissolves.
  drive(&mut driver, handle.prepare_merge(2, 1))
    .await
    .expect("freeze proposed");
  let deadline = std::time::Instant::now() + Duration::from_secs(10);
  while !driver.coord.group(&2).is_some_and(|ep| ep.is_frozen()) {
    assert!(std::time::Instant::now() < deadline, "no freeze in time");
    crank(&mut driver).await;
  }
  drive(&mut driver, handle.commit_merge(1, 2))
    .await
    .expect("commit proposed");
  // Resolve: the source endpoint is CONSUMED (removed from the container) regardless of the fold, so
  // its disappearance is the fix-independent signal that the resolve arm ran.
  let deadline = std::time::Instant::now() + Duration::from_secs(10);
  while driver.coord.group(&2).is_some() {
    assert!(
      std::time::Instant::now() < deadline,
      "the parked merge never resolved"
    );
    crank(&mut driver).await;
  }
  // A couple more cranks so the target's poison drains onto the lifecycle tail (via `poll_poisoned`).
  crank(&mut driver).await;
  crank(&mut driver).await;

  // THE FIX: the stranded source submit received a TYPED error instead of hanging forever.
  assert!(
    matches!(
      rx.try_recv(),
      Ok(Some(Err(sailing_driver::DriverError::Poisoned)))
    ),
    "the source's parked submit must be failed typed, not left hanging on the vanished endpoint"
  );
  // The source's stores and floor are PRESERVED — the union's only copy, restored on restart.
  assert!(
    hosts(&driver, 2),
    "the source's stores must be preserved, not dropped"
  );
  assert_ne!(
    driver.engine.group_floor(&2),
    sailing_proto::MERGED_FLOOR,
    "the source must NOT be terminally floored — that would bury the union"
  );
  // The absorbing target poisoned and surfaces it.
  assert!(
    driver.coord.group(&1).is_some_and(|ep| ep.is_poisoned()),
    "the absorbing target fail-stops on the failed absorb"
  );
  let lifecycle: Vec<LifecycleEvent<u64, u64>> = handle.lifecycle().try_iter().collect();
  assert!(
    lifecycle.iter().any(|ev| matches!(
      ev,
      LifecycleEvent::MergeCaptureFailed {
        source: 2,
        target: 1
      }
    )),
    "the embedder learns the source is gone and a restart is needed: {lifecycle:?}"
  );
  assert!(
    lifecycle
      .iter()
      .any(|ev| matches!(ev, LifecycleEvent::Poisoned { group: 1 })),
    "the poisoned target surfaces on the lifecycle tail: {lifecycle:?}"
  );
}
