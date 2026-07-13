use std::task::{Context, Poll};

use agnostic::tokio::TokioRuntime;
use sailing_proto::{ClusterId, Data, LabelOptions, Labeled, Passthrough};

use super::*;

const ELECTION: Duration = Duration::from_millis(100);
const HEARTBEAT: Duration = Duration::from_millis(20);

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
/// caught by the handle's `catch_unwind`. Before the fix `fail_all` DISCARDED that `Panicked`
/// outcome and the group stayed LIVE against possibly-torn state; now the panic routes through the
/// group's routing to the per-group fail-stop tail. The panicked group poisons; a co-located sibling
/// keeps committing. The parked query reports the caught-panic outcome directly (as
/// `shared::query_batch_stops_serving_at_the_first_caught_panic` does), avoiding a real unwind.
#[tokio::test]
async fn a_swept_completion_panic_fail_stops_only_its_group() {
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
  // The next crank's per-group tail folds the latched completion panic into the group fail-stop.
  crank(&mut driver).await;
  assert!(
    driver.coord.group(&1).is_some_and(|ep| ep.is_poisoned()),
    "the swept completion's caught panic fail-stopped group 1"
  );
  assert!(
    driver.coord.group(&2).is_some_and(|ep| !ep.is_poisoned()),
    "the co-located sibling was not poisoned"
  );
  // The sibling still accepts and commits a proposal.
  let g2 = handle.group(2);
  drive(&mut driver, g2.submit(Bytes::from_static(b"x")))
    .await
    .expect("the co-located sibling keeps committing");
}

/// The regression guard: a WELL-BEHAVED `fail_all(Superseded)` sweep (its completion `Delivered`)
/// must NOT poison the group — a superseded group is not a poisoned one. Pins that the fix does not
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
/// query completes `Poisoned` from the fail-stop sweep (its served `Ok(&fsm)` arm never runs), the group
/// fail-stops, and a co-located sibling's runnable query still serves normally the same crank. RED
/// before the fix (the query was served against the torn FSM). The decline's caught panic is reported
/// directly, as `a_swept_completion_panic_fail_stops_only_its_group` does, avoiding a real unwind.
#[tokio::test]
async fn a_pre_serve_completion_panic_skips_the_torn_query_serve() {
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
  // A co-located sibling's runnable query with NO failover decline: it must serve normally this crank,
  // proving the skip is scoped to the torn group, not a blanket stall.
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
    served_2.load(Ordering::Relaxed),
    "the co-located sibling's runnable query serves normally the same crank"
  );
  assert!(
    driver.coord.group(&2).is_some_and(|ep| !ep.is_poisoned()),
    "the sibling was not poisoned"
  );
}

/// RED-PROOF 1 — the immediate-refusal arm. `read_index` on a group that is not a leader refuses
/// IMMEDIATELY, and the refusal completes with `Err`: it never CALLS the user closure, but it DROPS it
/// unused inside the completion's `catch_unwind`, so a guard the closure captured runs its `Drop` there
/// and a panicking `Drop` is caught and reported `Panicked`. Every driver's refusal path DISCARDED that
/// outcome (the call is a bare `complete(...)` on a destructured field, which the enforcement grep never
/// matched), leaving the group LIVE against state the guard could have torn. It must fail-stop instead —
/// group-scoped: the co-located sibling keeps committing. RED before: group 1 is not poisoned.
#[tokio::test]
async fn an_immediate_refusal_drop_panic_fail_stops_only_its_group() {
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
  assert!(
    driver.coord.group(&1).is_some_and(|ep| ep.is_poisoned()),
    "the refused query's dropped guard panicked: group 1 must fail-stop, not keep serving torn state"
  );
  assert!(
    driver.coord.group(&2).is_some_and(|ep| !ep.is_poisoned()),
    "the co-located sibling must not be poisoned"
  );
  // The fail-stop surfaces once on the lifecycle tail, exactly as a served read's panic does.
  let poisoned: Vec<u64> = handle
    .lifecycle()
    .try_iter()
    .filter_map(|ev| match ev {
      LifecycleEvent::Poisoned { group } => Some(group),
      _ => None,
    })
    .collect();
  assert_eq!(poisoned, vec![1], "the fail-stopped group surfaces once");
  // The sibling still elects and commits: a group-scoped fail-stop, never a driver stop.
  elect(&mut driver, 2).await;
  drive(
    &mut driver,
    handle.group(2).submit(Bytes::from_static(b"x")),
  )
  .await
  .expect("the co-located sibling keeps committing");
}

/// The no-over-fail-stop guard for RED-PROOF 1: a WELL-BEHAVED closure refused the same way must NOT
/// poison its group. A refusal is not a torn state machine — the caller simply gets the read's refusal
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

/// RED-PROOF 2 — the merge-teardown hole. The merge fold DETACHES the source's routing
/// (`self.routing.remove(&source)`) and only then sweeps its parked work, so a completion(-drop) panic in
/// that sweep latched into a `Routing` that was immediately DROPPED: the latch died with it, and the
/// pump's per-group tail can never read a latch for a group that no longer routes. By that point the
/// proto has already absorbed the source's state machine INTO the target and staged the target's capture,
/// so the state the guard tore is the TARGET's — and the merged target stayed LIVE, serving a possibly
/// divergent union. The fold now keeps the target from `Merged { source, target }` and routes the dying
/// latch to it, in the storage crank, ahead of the pump. RED before: group 1 is not poisoned.
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
/// RED without the poison gate: the confirmed query is served `Ok(&fsm)` against the absorbed union
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

/// The no-over-fail-stop guard for RED-PROOF 2: a WELL-BEHAVED source teardown (its swept completion
/// `Delivered`) must NOT poison the absorbing target. The merge is the common path — it must stay clean,
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
