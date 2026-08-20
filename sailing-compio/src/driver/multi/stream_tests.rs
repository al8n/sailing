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
