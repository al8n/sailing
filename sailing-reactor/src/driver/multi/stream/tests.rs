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
