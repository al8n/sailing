use super::*;

/// The blueprint's state-machine slot is deliberately bound-free (it is a dumb admission
/// payload), so a plain non-`Debug`, non-`StateMachine` type exercises it — any tighter bound on
/// the factory surface is a compile error here.
struct OpaqueSm(u8);

fn config() -> Config<u64> {
  Config::try_new(
    1u64,
    vec![1u64],
    core::time::Duration::from_millis(1000),
    core::time::Duration::from_millis(100),
  )
  .unwrap()
}

/// The boxed slot the multi drivers store rides their `Send` run-loop futures across a
/// work-stealing runtime; a regression in the trait object's `Send` composition is a compile
/// error here.
const _: fn() = || {
  fn assert_send<T: Send>() {}
  assert_send::<BoxedGroupFactory<u64, u64, OpaqueSm>>();
};

#[test]
fn blueprint_carries_and_returns_its_parts() {
  let blueprint = GroupBlueprint::new(config(), 7, OpaqueSm(42));
  assert_eq!(blueprint.config_ref().id(), 1);
  assert_eq!(blueprint.seed(), 7);
  assert_eq!(blueprint.fsm_ref().0, 42);

  // `into_parts` hands back exactly the admission triple the drivers' create path takes.
  let (config, seed, fsm) = blueprint.into_parts();
  assert_eq!(config.id(), 1);
  assert_eq!(seed, 7);
  assert_eq!(fsm.0, 42);
}

/// The blanket impl: a plain `FnMut(&G, &I) -> Option<GroupBlueprint>` closure IS a factory —
/// recognizing exactly one catalog id and DECLINING everything else, the admission-edge contract
/// in miniature.
#[test]
fn a_closure_is_a_factory_and_none_declines() {
  let mut factory = |group: &u64, from: &u64| {
    (*group == 7).then(|| GroupBlueprint::new(config(), *from, OpaqueSm(1)))
  };

  let blueprint =
    GroupFactory::materialize(&mut factory, &7, &3).expect("the recognized id materializes");
  assert_eq!(
    blueprint.seed(),
    3,
    "the solicitor's id reached the closure"
  );
  assert!(
    GroupFactory::materialize(&mut factory, &9, &3).is_none(),
    "an unrecognized id declines"
  );
}

/// The exact trait-object form the drivers store: the same closure boxed as a
/// [`BoxedGroupFactory`], with the decline plumbing intact through `dyn` dispatch.
#[test]
fn the_boxed_slot_drives_the_same_closure() {
  let mut boxed: BoxedGroupFactory<u64, u64, OpaqueSm> = Box::new(|group: &u64, _from: &u64| {
    (*group == 7).then(|| GroupBlueprint::new(config(), 1, OpaqueSm(1)))
  });
  assert!(boxed.materialize(&7, &1).is_some());
  assert!(
    boxed.materialize(&9, &1).is_none(),
    "the decline plumbs through the trait object"
  );
}

/// `Debug` requires only `I: Debug`: `OpaqueSm` is deliberately not `Debug` (that this compiles
/// is the pin), and the state machine renders as the `..` elision beside the identifying
/// config + seed.
#[test]
fn debug_elides_the_state_machine() {
  let rendered = format!("{:?}", GroupBlueprint::new(config(), 7, OpaqueSm(0)));
  assert!(rendered.contains("GroupBlueprint"), "got: {rendered}");
  assert!(rendered.contains("seed: 7"), "got: {rendered}");
  assert!(rendered.contains(".."), "got: {rendered}");
}
