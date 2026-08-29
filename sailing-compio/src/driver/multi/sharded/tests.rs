use super::{ShardGuardedFactory, ShardMap, fnv1a, shard_addr};

/// The uniform map is deterministic (no per-process hash state) and stays inside the plane
/// bound — the two properties the cluster-wide-consistency contract rides on.
#[test]
fn uniform_map_is_deterministic_and_bounded() {
  let a = ShardMap::<u64>::uniform(4);
  let b = ShardMap::<u64>::uniform(4);
  for gid in [0u64, 1, 7, 100, 200, 0xFFFF_FFFF_FFFF_FFFF] {
    let shard = a.shard(&gid);
    assert!(shard < 4, "shard {shard} out of bounds for gid {gid}");
    assert_eq!(shard, b.shard(&gid), "two maps disagree on gid {gid}");
    assert_eq!(shard, a.shard(&gid), "one map disagrees with itself");
  }
}

/// FNV-1a matches the published 64-bit test vectors — the fixed-algorithm half of the
/// cross-process determinism contract (the other half is the id's canonical `Data` encoding).
#[test]
fn fnv1a_matches_the_reference_vectors() {
  assert_eq!(fnv1a(b""), 0xcbf2_9ce4_8422_2325);
  assert_eq!(fnv1a(b"a"), 0xaf63_dc4c_8601_ec8c);
  assert_eq!(fnv1a(b"foobar"), 0x8594_4171_f739_67e8);
}

/// A custom mapping is honored — and folded `% shards`, so a wild return value can never index
/// out of the plane vector.
#[test]
fn custom_mapping_is_used_and_folded() {
  let map = ShardMap::with_mapping(2, |gid: &u64| (*gid / 100) as usize);
  assert_eq!(map.shard(&100), 1);
  assert_eq!(map.shard(&200), 0, "2 % 2 folds back into range");
  assert_eq!(map.shard(&500), 1, "5 % 2 folds back into range");
}

/// A zero shard count is clamped to one (the drivers' `cmd_budget.max(1)` convention), keeping
/// `shard()` total.
#[test]
fn zero_shards_clamps_to_one() {
  let map = ShardMap::<u64>::uniform(0);
  assert_eq!(map.shards(), 1);
  assert_eq!(map.shard(&42), 0);
}

/// The shard guard declines a group the map assigns to a DIFFERENT plane before the embedder's
/// factory is ever consulted (its catalog is never probed with wrong-plane ids), and a group
/// mapped to THIS plane flows through both phases untouched.
#[test]
fn shard_guard_declines_wrong_plane_groups_before_the_factory() {
  use core::time::Duration;
  use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
  };

  use sailing_driver::{BoxedGroupFactory, GroupBlueprint, GroupFactory, factory_fn};

  let consulted = Arc::new(AtomicUsize::new(0));
  let counter = consulted.clone();
  let inner: BoxedGroupFactory<u64, u64, Vec<u8>> = Box::new(factory_fn(
    move |group: &u64, _from: &u64| {
      counter.fetch_add(1, Ordering::SeqCst);
      let config = sailing_proto::Config::try_new(
        2u64,
        vec![1, 2],
        Duration::from_millis(1000),
        Duration::from_millis(100),
      )
      .expect("a valid seed config");
      Some(GroupBlueprint::new(config, *group))
    },
    |_group: &u64| Some(Vec::new()),
  ));
  // The identity map over 2 planes (group 0 → plane 0, group 1 → plane 1); the guard sits on
  // plane 0.
  let mut guard = ShardGuardedFactory {
    inner,
    map: ShardMap::with_mapping(2, |g: &u64| *g as usize),
    plane: 0,
  };

  assert!(
    guard.materialize(&1, &7).is_none(),
    "a wrong-plane group is declined"
  );
  assert_eq!(
    consulted.load(Ordering::SeqCst),
    0,
    "the inner factory was never consulted for the wrong-plane group"
  );

  assert!(
    guard.materialize(&0, &7).is_some(),
    "an own-plane group delegates to the inner factory"
  );
  assert_eq!(consulted.load(Ordering::SeqCst), 1);
  assert!(guard.build(&0).is_some(), "build delegates untouched");
}

/// A NONDETERMINISTIC custom map is caught at the decline site: the guard's stability probe trips
/// in debug/test builds, so a map that returns different shards for one group cannot silently slip
/// a group onto the wrong plane. Debug-only — the probe is a `debug_assert`.
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "deterministic")]
fn nondeterministic_map_trips_the_stability_probe() {
  use core::time::Duration;
  use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
  };

  use sailing_driver::{BoxedGroupFactory, GroupBlueprint, GroupFactory, factory_fn};

  let inner: BoxedGroupFactory<u64, u64, Vec<u8>> = Box::new(factory_fn(
    move |group: &u64, _from: &u64| {
      let config = sailing_proto::Config::try_new(
        2u64,
        vec![1, 2],
        Duration::from_millis(1000),
        Duration::from_millis(100),
      )
      .expect("a valid seed config");
      Some(GroupBlueprint::new(config, *group))
    },
    |_group: &u64| Some(Vec::new()),
  ));
  // A map whose shard for one group ALTERNATES between calls — the determinism the guard relies on,
  // violated on purpose.
  let flip = Arc::new(AtomicUsize::new(0));
  let mut guard = ShardGuardedFactory {
    inner,
    map: ShardMap::with_mapping(2, move |_g: &u64| flip.fetch_add(1, Ordering::SeqCst) % 2),
    plane: 0,
  };
  // The two-consult stability probe in `materialize` must trip.
  let _ = guard.materialize(&0, &7);
}

/// The port convention advances the port and preserves the IP; overflow is a `None`, never a
/// wrap (a wrapped port would silently dial an unrelated service).
#[test]
fn shard_addr_advances_the_port_and_refuses_overflow() {
  let base: std::net::SocketAddr = "127.0.0.1:45000".parse().unwrap();
  assert_eq!(shard_addr(base, 0), Some(base));
  assert_eq!(
    shard_addr(base, 3),
    Some("127.0.0.1:45003".parse().unwrap())
  );
  let near_top: std::net::SocketAddr = "127.0.0.1:65535".parse().unwrap();
  assert_eq!(shard_addr(near_top, 0), Some(near_top));
  assert_eq!(shard_addr(near_top, 1), None, "u16 overflow is refused");
}

/// The sharded handle routes a FORK by the shard map, exactly like every other lifecycle verb:
/// the command lands on the mapped plane's channel — payload intact — and the wrong plane never
/// sees anything.
#[test]
fn fork_routes_to_its_owning_plane() {
  use core::time::Duration;

  use sailing_driver::{MultiCommand, MultiHandle, shared::InflightBudget};

  use super::ShardedMultiHandle;

  struct NoopSm;
  impl sailing_proto::StateMachine for NoopSm {
    type Command = bytes::Bytes;
    type Response = u64;
    type Snapshot = u64;
    type Error = std::convert::Infallible;

    fn apply(&mut self, _i: sailing_proto::Index, _c: bytes::Bytes) -> Result<u64, Self::Error> {
      Ok(0)
    }
    fn snapshot(&self) -> Result<u64, Self::Error> {
      Ok(0)
    }
    fn restore(&mut self, _s: u64) -> Result<(), Self::Error> {
      Ok(())
    }
  }

  // Two stub planes: each MultiHandle's command receiver is held so the routing is observable.
  let mut plane_rx = Vec::new();
  let mut shards = Vec::new();
  for _ in 0..2 {
    let (cmd_tx, cmd_rx) = flume::unbounded::<MultiCommand<u64, u64, NoopSm>>();
    let (_event_tx, event_rx) = flume::bounded(4);
    let (_lifecycle_tx, lifecycle_rx) = flume::bounded(4);
    let (teardown_tx, teardown_rx) = futures_channel::oneshot::channel();
    std::mem::forget(teardown_tx);
    shards.push(MultiHandle::new(
      cmd_tx,
      event_rx,
      lifecycle_rx,
      InflightBudget::new(8, 1024),
      teardown_rx,
    ));
    plane_rx.push(cmd_rx);
  }
  let (_ev_tx, events) = flume::bounded(4);
  let (_lc_tx, lifecycle) = flume::bounded(4);
  // The identity map over 2 planes: group 0 → plane 0, group 1 → plane 1.
  let sharded = ShardedMultiHandle {
    shards,
    map: ShardMap::with_mapping(2, |g: &u64| *g as usize),
    events,
    lifecycle,
    metrics: Vec::new(),
  };

  let config = sailing_proto::Config::try_new(
    1u64,
    vec![1],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let blob = bytes::Bytes::from_static(b"forkblob");
  // One poll enqueues the command; the future then parks on the (never-answered) reply.
  let fork = sharded.create_group_from_fork(1, config, 9, NoopSm, blob.clone(), 3);
  assert!(
    futures_util::FutureExt::now_or_never(fork).is_none(),
    "the fork parks awaiting its admission verdict"
  );

  match plane_rx[1]
    .try_recv()
    .expect("the mapped plane got the fork")
  {
    MultiCommand::CreateGroupFromFork {
      gid,
      seed,
      generation,
      snapshot,
      ..
    } => {
      assert_eq!(gid, 1);
      assert_eq!(seed, 9);
      assert_eq!(generation, 3);
      assert_eq!(snapshot, blob, "the blob rides to the owning plane intact");
    }
    _ => panic!("expected a CreateGroupFromFork on the mapped plane"),
  }
  assert!(
    plane_rx[0].try_recv().is_err(),
    "the wrong plane never sees the fork"
  );
}

/// A stub state machine for the two-plane rigs below; nothing is ever applied.
struct StubSm;
impl sailing_proto::StateMachine for StubSm {
  type Command = bytes::Bytes;
  type Response = u64;
  type Snapshot = u64;
  type Error = std::convert::Infallible;

  fn apply(&mut self, _i: sailing_proto::Index, _c: bytes::Bytes) -> Result<u64, Self::Error> {
    Ok(0)
  }
  fn snapshot(&self) -> Result<u64, Self::Error> {
    Ok(0)
  }
  fn restore(&mut self, _s: u64) -> Result<(), Self::Error> {
    Ok(())
  }
}

type StubCommand = sailing_driver::MultiCommand<u64, u64, StubSm>;

/// Two stub planes under the identity map (group 0 → plane 0, group 1 → plane 1), with each
/// plane's command receiver handed back so the test answers the plane's queries itself.
fn two_plane_rig() -> (
  super::ShardedMultiHandle<u64, u64, StubSm>,
  Vec<flume::Receiver<StubCommand>>,
) {
  two_plane_rig_with(8)
}

/// The same rig with an explicit per-plane in-flight slot count.
fn two_plane_rig_with(
  max_inflight: usize,
) -> (
  super::ShardedMultiHandle<u64, u64, StubSm>,
  Vec<flume::Receiver<StubCommand>>,
) {
  use sailing_driver::{MultiHandle, shared::InflightBudget};

  let mut plane_rx = Vec::new();
  let mut shards = Vec::new();
  for _ in 0..2 {
    let (cmd_tx, cmd_rx) = flume::unbounded::<StubCommand>();
    let (_event_tx, event_rx) = flume::bounded(4);
    let (_lifecycle_tx, lifecycle_rx) = flume::bounded(4);
    let (teardown_tx, teardown_rx) = futures_channel::oneshot::channel();
    std::mem::forget(teardown_tx);
    std::mem::forget(_event_tx);
    std::mem::forget(_lifecycle_tx);
    shards.push(MultiHandle::new(
      cmd_tx,
      event_rx,
      lifecycle_rx,
      InflightBudget::new(max_inflight, 1024),
      teardown_rx,
    ));
    plane_rx.push(cmd_rx);
  }
  let (_ev_tx, events) = flume::bounded(4);
  let (_lc_tx, lifecycle) = flume::bounded(4);
  std::mem::forget(_ev_tx);
  std::mem::forget(_lc_tx);
  let sharded = super::ShardedMultiHandle {
    shards,
    map: ShardMap::with_mapping(2, |g: &u64| *g as usize),
    events,
    lifecycle,
    metrics: Vec::new(),
  };
  (sharded, plane_rx)
}

/// Answer the plane's pending fence query with `verdict`, asserting it asked about `group`.
fn answer_fence(rx: &flume::Receiver<StubCommand>, group: u64, verdict: Option<u64>) {
  match rx.try_recv().expect("the plane was asked for its fence") {
    sailing_driver::MultiCommand::GroupFenced {
      group: asked,
      reply,
      ..
    } => {
      assert_eq!(asked, group, "the preflight asked about the wrong id");
      let _ = reply.send(verdict);
    }
    _ => panic!("expected a GroupFenced query before the placement answer"),
  }
}

/// THE LOCAL PARTICIPANT'S FENCE OUTRANKS THE PLACEMENT ANSWER. A caller told only `CrossPlane`
/// remaps the child and retries forever against a parent that can never propose again; the
/// parent's own plane knows the fence, so it reports the fence — with the parent's role in the
/// text, not the child's.
#[test]
fn a_fenced_parent_refuses_before_the_cross_plane_answer() {
  use core::{future::Future, pin::pin, task::Context};

  let (sharded, plane_rx) = two_plane_rig();
  // Parent 0 lives on plane 0; child 1 lives on plane 1 — cross-plane, so the placement answer is
  // the one the preflight must NOT reach.
  let mut split = pin!(sharded.propose_split(0, 1, 5, bytes::Bytes::new()));
  let mut cx = Context::from_waker(core::task::Waker::noop());
  if let core::task::Poll::Ready(answer) = split.as_mut().poll(&mut cx) {
    panic!("the placement answer preempted the parent's fence query: {answer:?}");
  }
  answer_fence(&plane_rx[0], 0, Some(sailing_proto::MERGED_FLOOR));

  match split.as_mut().poll(&mut cx) {
    core::task::Poll::Ready(Err(sailing_driver::DriverError::Rejected { reason })) => {
      assert!(
        reason.contains("parent") && reason.contains("floor"),
        "the refusal names the fenced parent, not a placement problem: {reason}"
      );
      assert!(
        !reason.contains(&sailing_proto::SplitError::<u64>::CrossPlane.to_string()),
        "the placement answer must not shadow the fence: {reason}"
      );
    }
    other => panic!("expected the parent's floor refusal, got {other:?}"),
  }
  assert!(
    plane_rx[0].try_recv().is_err() && plane_rx[1].try_recv().is_err(),
    "a fenced parent proposes nothing on either plane"
  );
}

/// The abort rides the TARGET's log, so the target is the participant this plane owns — and a
/// floored target is refused with its own fence, not with the source's placement.
#[test]
fn a_floored_rollback_target_refuses_before_the_cross_plane_answer() {
  use core::{future::Future, pin::pin, task::Context};

  let (sharded, plane_rx) = two_plane_rig();
  // Target 0 on plane 0, source 1 on plane 1 — the peer is cross-plane and its own floor is
  // invisible from here; the target's is not.
  let mut abort = pin!(sharded.rollback_merge(0, 1));
  let mut cx = Context::from_waker(core::task::Waker::noop());
  if let core::task::Poll::Ready(answer) = abort.as_mut().poll(&mut cx) {
    panic!("the placement answer preempted the target's fence query: {answer:?}");
  }
  answer_fence(&plane_rx[0], 0, Some(7));

  match abort.as_mut().poll(&mut cx) {
    core::task::Poll::Ready(Err(sailing_driver::DriverError::Rejected { reason })) => {
      assert!(
        reason.contains("target") && reason.contains('7'),
        "the refusal names the floored target and its floor: {reason}"
      );
      assert!(
        !reason.contains(&sailing_proto::MergeError::<u64>::CrossPlane.to_string()),
        "the placement answer must not shadow the fence: {reason}"
      );
    }
    other => panic!("expected the target's floor refusal, got {other:?}"),
  }
  assert!(
    plane_rx[0].try_recv().is_err() && plane_rx[1].try_recv().is_err(),
    "a floored target proposes nothing on either plane"
  );
}

/// The preflight does not swallow the placement contract: when the local participant raises no
/// objection, the cross-plane refusal is still the answer — on every gated verb, verbatim. (The
/// peer's own floor is unknowable from this plane; its owning plane's gate judges it on dispatch.)
#[test]
fn a_healthy_local_participant_still_gets_the_cross_plane_answer() {
  use core::{future::Future, pin::pin, task::Context};

  let mut cx = Context::from_waker(core::task::Waker::noop());

  let (sharded, plane_rx) = two_plane_rig();
  let mut split = pin!(sharded.propose_split(0, 1, 5, bytes::Bytes::new()));
  if let core::task::Poll::Ready(answer) = split.as_mut().poll(&mut cx) {
    panic!("the split answered without asking the parent's plane: {answer:?}");
  }
  answer_fence(&plane_rx[0], 0, None);
  match split.as_mut().poll(&mut cx) {
    core::task::Poll::Ready(Err(sailing_driver::DriverError::Rejected { reason })) => {
      assert_eq!(
        reason,
        sailing_proto::SplitError::<u64>::CrossPlane.to_string()
      );
    }
    other => panic!("expected the cross-plane refusal, got {other:?}"),
  }

  let (sharded, plane_rx) = two_plane_rig();
  let mut abort = pin!(sharded.rollback_merge(0, 1));
  if let core::task::Poll::Ready(answer) = abort.as_mut().poll(&mut cx) {
    panic!("the abort answered without asking the target's plane: {answer:?}");
  }
  answer_fence(&plane_rx[0], 0, None);
  match abort.as_mut().poll(&mut cx) {
    core::task::Poll::Ready(Err(sailing_driver::DriverError::Rejected { reason })) => {
      assert_eq!(
        reason,
        sailing_proto::MergeError::<u64>::CrossPlane.to_string()
      );
    }
    other => panic!("expected the cross-plane refusal, got {other:?}"),
  }

  // The freeze routes on its SOURCE, so group 0 is the owned participant here.
  let (sharded, plane_rx) = two_plane_rig();
  let mut freeze = pin!(sharded.prepare_merge(0, 1));
  if let core::task::Poll::Ready(answer) = freeze.as_mut().poll(&mut cx) {
    panic!("the freeze answered without asking the source's plane: {answer:?}");
  }
  answer_fence(&plane_rx[0], 0, None);
  match freeze.as_mut().poll(&mut cx) {
    core::task::Poll::Ready(Err(sailing_driver::DriverError::Rejected { reason })) => {
      assert_eq!(
        reason,
        sailing_proto::MergeError::<u64>::CrossPlane.to_string()
      );
    }
    other => panic!("expected the cross-plane refusal, got {other:?}"),
  }

  let (sharded, plane_rx) = two_plane_rig();
  let mut absorb = pin!(sharded.commit_merge(0, 1));
  if let core::task::Poll::Ready(answer) = absorb.as_mut().poll(&mut cx) {
    panic!("the absorb answered without asking the target's plane: {answer:?}");
  }
  answer_fence(&plane_rx[0], 0, None);
  match absorb.as_mut().poll(&mut cx) {
    core::task::Poll::Ready(Err(sailing_driver::DriverError::Rejected { reason })) => {
      assert_eq!(
        reason,
        sailing_proto::MergeError::<u64>::CrossPlane.to_string()
      );
    }
    other => panic!("expected the cross-plane refusal, got {other:?}"),
  }
}

/// The freeze rides the SOURCE's log, so a floored source is refused before the target's placement
/// — the same precedence the split's parent and the abort's target answer to. The routing id and
/// the owned id coincide here, which is exactly why the placement answer looks like the whole story
/// and is not.
#[test]
fn a_floored_freeze_source_refuses_before_the_cross_plane_answer() {
  use core::{future::Future, pin::pin, task::Context};

  let (sharded, plane_rx) = two_plane_rig();
  let mut freeze = pin!(sharded.prepare_merge(0, 1));
  let mut cx = Context::from_waker(core::task::Waker::noop());
  if let core::task::Poll::Ready(answer) = freeze.as_mut().poll(&mut cx) {
    panic!("the placement answer preempted the source's fence query: {answer:?}");
  }
  answer_fence(&plane_rx[0], 0, Some(sailing_proto::MERGED_FLOOR));

  match freeze.as_mut().poll(&mut cx) {
    core::task::Poll::Ready(Err(sailing_driver::DriverError::Rejected { reason })) => {
      assert!(
        reason.contains("source") && reason.contains("floor"),
        "the refusal names the floored source, not a placement problem: {reason}"
      );
      assert!(
        !reason.contains(&sailing_proto::MergeError::<u64>::CrossPlane.to_string()),
        "the placement answer must not shadow the fence: {reason}"
      );
    }
    other => panic!("expected the source's floor refusal, got {other:?}"),
  }
  assert!(
    plane_rx[0].try_recv().is_err() && plane_rx[1].try_recv().is_err(),
    "a floored source proposes nothing on either plane"
  );
}

/// The absorb rides the TARGET's log, so a floored target is refused before the source's placement.
#[test]
fn a_floored_absorb_target_refuses_before_the_cross_plane_answer() {
  use core::{future::Future, pin::pin, task::Context};

  let (sharded, plane_rx) = two_plane_rig();
  let mut absorb = pin!(sharded.commit_merge(0, 1));
  let mut cx = Context::from_waker(core::task::Waker::noop());
  if let core::task::Poll::Ready(answer) = absorb.as_mut().poll(&mut cx) {
    panic!("the placement answer preempted the target's fence query: {answer:?}");
  }
  answer_fence(&plane_rx[0], 0, Some(11));

  match absorb.as_mut().poll(&mut cx) {
    core::task::Poll::Ready(Err(sailing_driver::DriverError::Rejected { reason })) => {
      assert!(
        reason.contains("target") && reason.contains("11"),
        "the refusal names the floored target and its floor: {reason}"
      );
      assert!(
        !reason.contains(&sailing_proto::MergeError::<u64>::CrossPlane.to_string()),
        "the placement answer must not shadow the fence: {reason}"
      );
    }
    other => panic!("expected the target's floor refusal, got {other:?}"),
  }
  assert!(
    plane_rx[0].try_recv().is_err() && plane_rx[1].try_recv().is_err(),
    "a floored target proposes nothing on either plane"
  );
}

/// Answer the plane's fence query and let the caller run at the point a real driver would wake it.
/// `release_first` is the plane's contract: drop the preflight's reservation, THEN reply.
fn answer_fence_and_resume<T>(
  rx: &flume::Receiver<StubCommand>,
  group: u64,
  release_first: bool,
  poll: &mut dyn FnMut() -> core::task::Poll<T>,
) -> core::task::Poll<T> {
  let (asked, reply, reservation) = match rx.try_recv().expect("the plane was asked for its fence")
  {
    sailing_driver::MultiCommand::GroupFenced {
      group,
      reply,
      reservation,
    } => (group, reply, reservation),
    _ => panic!("expected a GroupFenced query before the verb's own command"),
  };
  assert_eq!(asked, group, "the preflight asked about the wrong id");
  if release_first {
    drop(reservation);
    let _ = reply.send(None);
    poll()
  } else {
    // The hazard, reproduced: the reply wakes the caller while the preflight still holds the slot.
    let _ = reply.send(None);
    let resumed = poll();
    drop(reservation);
    resumed
  }
}

/// THE PREFLIGHT MUST NOT RACE THE COMMAND IT PRECEDES. Every gated sharded verb spends one slot
/// asking the owning plane for a fence and then a SECOND slot on the real command, so the plane has
/// to release the first before the reply wakes the caller. At `max_inflight = 1` the two orders are
/// the whole difference between an admissible operation proceeding and a spurious
/// [`DriverError::Busy`] — and for the abort that answer leaves a source frozen until something
/// external retries.
#[test]
fn a_gated_verb_needs_only_one_slot_at_a_time() {
  use core::{future::Future, pin::pin, task::Context};

  // Group 2 folds onto plane 0 (`2 % 2`), so every leg below is a SAME-plane operation that must
  // reach its plane rather than stop at a placement answer.
  for release_first in [true, false] {
    {
      let (sharded, plane_rx) = two_plane_rig_with(1);
      let mut cx = Context::from_waker(core::task::Waker::noop());
      let mut split = pin!(sharded.propose_split(0, 2, 5, bytes::Bytes::new()));
      assert!(split.as_mut().poll(&mut cx).is_pending());
      let out = answer_fence_and_resume(&plane_rx[0], 0, release_first, &mut || {
        split.as_mut().poll(&mut cx)
      });
      assert_split_leg(out, &plane_rx[0], release_first, "split");
    }
    {
      let (sharded, plane_rx) = two_plane_rig_with(1);
      let mut cx = Context::from_waker(core::task::Waker::noop());
      let mut freeze = pin!(sharded.prepare_merge(0, 2));
      assert!(freeze.as_mut().poll(&mut cx).is_pending());
      let out = answer_fence_and_resume(&plane_rx[0], 0, release_first, &mut || {
        freeze.as_mut().poll(&mut cx)
      });
      assert_split_leg(out, &plane_rx[0], release_first, "freeze");
    }
    {
      let (sharded, plane_rx) = two_plane_rig_with(1);
      let mut cx = Context::from_waker(core::task::Waker::noop());
      let mut abort = pin!(sharded.rollback_merge(0, 2));
      assert!(abort.as_mut().poll(&mut cx).is_pending());
      let out = answer_fence_and_resume(&plane_rx[0], 0, release_first, &mut || {
        abort.as_mut().poll(&mut cx)
      });
      assert_split_leg(out, &plane_rx[0], release_first, "abort");
    }
  }
}

/// One leg's verdict: released-first, the verb reaches its plane and parks on the real reply;
/// reply-first, the same admissible verb is turned away `Busy` by its own preflight.
fn assert_split_leg<T: core::fmt::Debug>(
  out: core::task::Poll<Result<T, sailing_driver::DriverError<u64>>>,
  rx: &flume::Receiver<StubCommand>,
  release_first: bool,
  verb: &str,
) {
  if release_first {
    assert!(
      matches!(out, core::task::Poll::Pending),
      "the {verb} must reach its plane once the slot is free, got {out:?}"
    );
    assert!(
      rx.try_recv().is_ok(),
      "the {verb}'s own command reached the plane"
    );
  } else {
    assert!(
      matches!(
        out,
        core::task::Poll::Ready(Err(sailing_driver::DriverError::Busy))
      ),
      "holding the preflight slot across the reply turns the {verb} away, got {out:?}"
    );
  }
}
