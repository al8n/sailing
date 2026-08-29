use std::{
  io,
  rc::Rc,
  task::{Context, Poll},
};

use sailing_proto::{ClusterId, Config, Data, LabelOptions, Labeled, Passthrough};

use super::*;

const ELECTION: Duration = Duration::from_millis(100);
const HEARTBEAT: Duration = Duration::from_millis(20);

thread_local! {
  /// When armed, `MergeSm::absorb` REFUSES — the deterministic `MergeUnsupported` fail-stop the
  /// merge resolve arm turns into a `CaptureFailed` resolution. Thread-local so a `#[compio::test]`
  /// running on its own libtest thread arms it in isolation; a guard disarms it even on panic.
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

type Driver = CompioMultiStreamDriver<u64, u64, MergeSm, Labeled<Passthrough>>;

/// Bind one EMPTY peerless host on a loopback port (single-voter groups never touch the wire),
/// stepped manually below — `storage_crank` and `pump` are called crank by crank, which is what
/// makes the one-crank window between a merge's resolution and its lifecycle drain observable.
async fn bind_host() -> (Driver, MultiHandle<u64, u64, MergeSm>) {
  let mut local = Vec::new();
  1u64.encode(&mut local);
  let dial_local = local.clone();
  let dialer: DialerFactory<u64, Labeled<Passthrough>> = Rc::new(move |_: &u64| {
    Labeled::dialer(
      Passthrough::new(),
      &LabelOptions {
        cluster: ClusterId([7; 16]),
        local_id: dial_local.clone(),
      },
    )
    .map_err(io::Error::other)
  });
  let acceptor: AcceptorFactory<Labeled<Passthrough>> = Rc::new(move || {
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
    compio::time::sleep(Duration::from_millis(20)).await;
  }
}

/// A fail-stop latched DURING `service_merge_applies` must surface on the lifecycle tail in the
/// SAME storage crank. The crank's pre-storage drain has already run by then, so without the
/// post-service drain the queued `Poisoned` waits for an unrelated wake — invisible exactly when
/// the plane is otherwise idle. The faulting absorb is the constructible member of the class
/// here; the resolution-less member (an owed adopt capture faulting on an idle adopter) is
/// pinned at the container by the proto regression and rides this same drain. The compio twin
/// of the reactor stream regression — the two cranks are maintained in parity, not shared.
#[compio::test]
async fn a_service_latched_poison_surfaces_in_the_same_crank() {
  let _fail_absorb = FailAbsorbGuard::arm();
  let (mut driver, handle) = bind_host().await;
  for gid in [1u64, 2] {
    let cfg = Config::try_new(1u64, vec![1], ELECTION, HEARTBEAT).unwrap();
    let fut = handle.create_group(gid, cfg, 7, MergeSm::default(), 0);
    drive(&mut driver, fut).await.expect("group admission");
    elect(&mut driver, gid).await;
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
  while driver.coord.group(&2).is_some() {
    assert!(
      std::time::Instant::now() < deadline,
      "the parked merge never resolved"
    );
    crank(&mut driver).await;
  }
  // The crank that resolved (and poisoned the target) is the LAST one executed: the poison must
  // already be on the tail, with no further crank granted.
  assert!(
    driver.coord.group(&1).is_some_and(|ep| ep.is_poisoned()),
    "the faulting absorb fail-stopped the target"
  );
  let lifecycle: Vec<LifecycleEvent<u64, u64>> = handle.lifecycle().try_iter().collect();
  assert!(
    lifecycle
      .iter()
      .any(|ev| matches!(ev, LifecycleEvent::Poisoned { group: 1 })),
    "the poison latched in service surfaces in the resolving crank itself: {lifecycle:?}"
  );
}

/// ONE FLOOR AUTHORITY, pinned where the command batch makes it observable.
///
/// The run loop drains SEVERAL commands per storage crank, so a removal's floor is STAGED and not
/// yet durable when the next command in the same batch is handled. Queue remove → clear_tombstone →
/// create at the incarnation the removal just ended, hand all three to the driver before any flush,
/// and the create must REFUSE: every admission door reads [`sailing_proto::FloorStore`], whose
/// contract is the FRESHEST value, so it sees the staged fence.
///
/// A durable-only read admits instead, and the flush then persists the fence beside a live endpoint
/// already standing below it. That divergence was reachable while `MultiEngine` carried its own
/// floor reader beside the `FloorStore` supertrait with nothing requiring the two to agree — the
/// doors are split across both readers. With the seam reduced to one accessor the pair cannot
/// disagree; this pins the doors on it.
///
/// Red-proof: give the create door a durable-only read (drop `lineage_staged` from
/// `GroupEngine::group_floor`, which is what `FloorStore::floor` composes) and the create is
/// ADMITTED into a group id its own removal has already fenced.
#[compio::test]
async fn a_staged_removal_floor_fences_a_create_in_the_same_command_batch() {
  let (mut driver, handle) = bind_host().await;
  let cfg = Config::try_new(1u64, vec![1], ELECTION, HEARTBEAT).unwrap();
  let fut = handle.create_group(1u64, cfg.clone(), 7, MergeSm::default(), 3);
  drive(&mut driver, fut).await.expect("group admission");
  // A RESHAPED id: only a reshape leaves a removal ceiling, and the floor is that opt-in's fence.
  // Seeded directly rather than driven through a split, so the test pins the FLOOR READ and not
  // the shape machinery that produces one.
  driver.engine.set_group_gen(&1u64, 3);
  assert_eq!(
    driver.engine.removal_floor(&1u64),
    4,
    "the removal must have a fence to stage, or this pins nothing"
  );

  // THE BATCH. Three commands queued before the driver sees any of them, then handled with NO
  // storage crank between — the window a durable-only reader answers stale in.
  let waker = futures_util::task::noop_waker();
  let mut cx = Context::from_waker(&waker);
  let mut remove = std::pin::pin!(handle.remove_group(1u64));
  let mut clear = std::pin::pin!(handle.clear_tombstone(1u64));
  let mut create = std::pin::pin!(handle.create_group(1u64, cfg, 7, MergeSm::default(), 3));
  assert!(remove.as_mut().poll(&mut cx).is_pending());
  let _ = clear.as_mut().poll(&mut cx);
  let _ = create.as_mut().poll(&mut cx);

  let now = driver.clock.now();
  let mut drained = 0;
  while let Ok(cmd) = driver.commands.try_recv() {
    driver.handle_command(now, cmd);
    drained += 1;
  }
  assert_eq!(
    drained, 3,
    "all three commands must reach the driver in ONE batch, before any flush"
  );
  assert_eq!(
    sailing_proto::FloorStore::floor(&driver.engine, &1u64),
    4,
    "the removal's floor is staged and readable — durability is still owed"
  );

  // The verdict rides the engine-write ack, so crank once to release it.
  let verdict = drive(&mut driver, create).await;
  let err = verdict.expect_err("the create must be REFUSED off the staged floor");
  let text = err.to_string();
  assert!(
    text.contains("floor"),
    "the refusal must be the admission floor's, not an incidental one: {text}"
  );
  assert!(
    !driver.engine.group_ids().any(|g| *g == 1u64),
    "a refused create must leave the id unhosted"
  );
}

/// Records, at the instant the driver wakes a reply's receiver, whether the plane's in-flight
/// budget has a free slot — the exact question a sharded verb asks next.
struct SlotAtWake {
  budget: sailing_driver::shared::InflightBudget,
  free: std::sync::atomic::AtomicI8,
}

impl futures_util::task::ArcWake for SlotAtWake {
  fn wake_by_ref(this: &std::sync::Arc<Self>) {
    // The probe reservation is released immediately; only its verdict is kept.
    let free = this.budget.try_reserve::<u64>(0).is_ok();
    this
      .free
      .store(i8::from(free), std::sync::atomic::Ordering::SeqCst);
  }
}

/// THE FENCE PREFLIGHT RELEASES ITS SLOT BEFORE IT REPLIES. A sharded verb spends one in-flight
/// slot asking this plane for a fence and then a SECOND on the real command, and the reply is what
/// starts that second reservation. If the arm answered while still holding the first, an admissible
/// operation would race its own preflight and be turned away `Busy` — at `max_inflight = 1` every
/// time, and for the abort that answer leaves a source frozen until something external retries.
/// The wake is the observation point because it is exactly when the caller resumes.
#[compio::test]
async fn the_fence_preflight_frees_its_slot_before_waking_the_caller() {
  let (mut driver, _handle) = bind_host().await;
  let budget = sailing_driver::shared::InflightBudget::new(1, 1024);
  let reservation = budget
    .try_reserve::<u64>(0)
    .expect("the preflight takes the only slot");
  let probe = std::sync::Arc::new(SlotAtWake {
    budget: budget.clone(),
    free: std::sync::atomic::AtomicI8::new(-1),
  });
  let (tx, rx) = futures_channel::oneshot::channel();
  let waker = futures_util::task::waker(probe.clone());
  let mut rx = std::pin::pin!(rx);
  assert!(
    rx.as_mut()
      .poll(&mut Context::from_waker(&waker))
      .is_pending(),
    "the caller parks on the fence reply"
  );

  let now = driver.clock.now();
  driver.handle_command(
    now,
    sailing_driver::MultiCommand::GroupFenced {
      group: 1,
      reply: tx,
      reservation,
    },
  );

  assert_eq!(
    probe.free.load(std::sync::atomic::Ordering::SeqCst),
    1,
    "the preflight's slot must already be free when its reply wakes the caller"
  );
}

/// THE RULE IS THE DISPATCH LOOP'S, NOT THE FENCE ARM'S. `Transfer` stands for every arm that
/// answers inline: it holds a reservation, replies once, and must free the slot first — a caller
/// chaining a second command on this wake would otherwise race the slot it believes it just freed.
/// The fence arm is probed separately because it is the one a sharded verb chains from; this leg
/// proves the ordering is the loop's uniform shape rather than that arm's local fix.
#[compio::test]
async fn a_plain_arm_frees_its_slot_before_waking_the_caller() {
  let (mut driver, _handle) = bind_host().await;
  let budget = sailing_driver::shared::InflightBudget::new(1, 1024);
  let reservation = budget
    .try_reserve::<u64>(0)
    .expect("the command takes the only slot");
  let probe = std::sync::Arc::new(SlotAtWake {
    budget: budget.clone(),
    free: std::sync::atomic::AtomicI8::new(-1),
  });
  let (tx, rx) = futures_channel::oneshot::channel();
  let waker = futures_util::task::waker(probe.clone());
  let mut rx = std::pin::pin!(rx);
  assert!(
    rx.as_mut()
      .poll(&mut Context::from_waker(&waker))
      .is_pending(),
    "the caller parks on the transfer verdict"
  );

  let now = driver.clock.now();
  driver.handle_command(
    now,
    sailing_driver::MultiCommand::Transfer {
      group: 1,
      to: 2,
      reply: tx,
      reservation,
    },
  );

  assert_eq!(
    probe.free.load(std::sync::atomic::Ordering::SeqCst),
    1,
    "the arm's slot must already be free when its reply wakes the caller"
  );
}

/// THE REFUSAL BRANCHES ANSWER TOO. `Submit` parks its reservation only when the propose is
/// ACCEPTED; a floor refusal answers inline, and it is the branch a caller is most likely to chain
/// from — `BelowFloor` names a fenced incarnation, and the recovery is another command. Holding the
/// slot across that reply would meet the recovery with `Busy` at `max_inflight = 1`, from a plane
/// with nothing in flight at all.
#[compio::test]
async fn a_refusal_branch_frees_its_slot_before_waking_the_caller() {
  let (mut driver, handle) = bind_host().await;
  let cfg = Config::try_new(1u64, vec![1], ELECTION, HEARTBEAT).unwrap();
  drive(
    &mut driver,
    handle.create_group(1u64, cfg, 7, MergeSm::default(), 0),
  )
  .await
  .expect("group admission");
  // A fence over the hosted incarnation: floor 7 above generation 0, so every propose is refused
  // `BelowFloor` — the inline-answer branch of an otherwise parking arm.
  driver.engine.set_group_floor(&1u64, 7);

  let budget = sailing_driver::shared::InflightBudget::new(1, 1024);
  let reservation = budget
    .try_reserve::<u64>(0)
    .expect("the submit takes the only slot");
  let probe = std::sync::Arc::new(SlotAtWake {
    budget: budget.clone(),
    free: std::sync::atomic::AtomicI8::new(-1),
  });
  let (tx, rx) = futures_channel::oneshot::channel();
  let waker = futures_util::task::waker(probe.clone());
  let mut rx = std::pin::pin!(rx);
  assert!(
    rx.as_mut()
      .poll(&mut core::task::Context::from_waker(&waker))
      .is_pending(),
    "the caller parks on the submit verdict"
  );

  let now = driver.clock.now();
  driver.handle_command(
    now,
    sailing_driver::MultiCommand::Submit {
      group: 1,
      cmd: Bytes::from_static(b"x"),
      reply: tx,
      reservation,
    },
  );

  assert_eq!(
    probe.free.load(std::sync::atomic::Ordering::SeqCst),
    1,
    "the refusal branch's slot must already be free when its reply wakes the caller"
  );
  // The branch under test really is the floor refusal, not an incidental miss.
  match rx
    .as_mut()
    .poll(&mut core::task::Context::from_waker(&waker))
  {
    Poll::Ready(Ok(Err(e))) => assert!(
      e.to_string().contains("floor"),
      "the inline answer must be the floor refusal: {e}"
    ),
    other => panic!("expected the floor refusal, got {other:?}"),
  }
}
