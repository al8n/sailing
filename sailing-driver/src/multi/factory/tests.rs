use super::*;

/// The build phase's state-machine slot is deliberately bound-free (it is a dumb admission
/// payload), so a plain non-`Debug`, non-`StateMachine` type exercises it — any tighter bound
/// on the factory surface is a compile error here.
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
  let blueprint = GroupBlueprint::new(config(), 7);
  assert_eq!(blueprint.config_ref().id(), 1);
  assert_eq!(blueprint.seed(), 7);

  // `into_parts` hands back exactly the blueprint's contribution to the drivers' create path
  // (the state machine arrives separately, from the build phase).
  let (config, seed) = blueprint.into_parts();
  assert_eq!(config.id(), 1);
  assert_eq!(seed, 7);
}

/// The [`factory_fn`] combinator round-trip: two plain closures ARE a factory — the seed
/// (materialize) closure recognizing exactly one catalog id and DECLINING everything else, the
/// build closure constructing the state machine only when asked. The admission-edge contract
/// in miniature, in its two-phase shape.
#[test]
fn factory_fn_round_trips_both_phases() {
  let mut factory = factory_fn(
    |group: &u64, from: &u64| (*group == 7).then(|| GroupBlueprint::new(config(), *from)),
    |group: &u64| (*group == 7).then_some(OpaqueSm(1)),
  );

  let blueprint =
    GroupFactory::materialize(&mut factory, &7, &3).expect("the recognized id materializes");
  assert_eq!(
    blueprint.seed(),
    3,
    "the solicitor's id reached the seed closure"
  );
  assert!(
    GroupFactory::materialize(&mut factory, &9, &3).is_none(),
    "an unrecognized id declines"
  );

  let fsm = GroupFactory::build(&mut factory, &7).expect("the recognized id builds");
  assert_eq!(fsm.0, 1);
  assert!(
    GroupFactory::build(&mut factory, &9).is_none(),
    "a build abort plumbs through the combinator"
  );
}

/// The exact trait-object form the drivers store: the same combinator boxed as a
/// [`BoxedGroupFactory`], with both phases — and both phases' decline/abort plumbing — intact
/// through `dyn` dispatch, checking both legs a factory can check (the catalog id AND the
/// solicitor against the group's replica set).
#[test]
fn the_boxed_slot_drives_both_phases() {
  let mut boxed: BoxedGroupFactory<u64, u64, OpaqueSm> = Box::new(factory_fn(
    |group: &u64, from: &u64| (*group == 7 && *from == 1).then(|| GroupBlueprint::new(config(), 1)),
    |group: &u64| (*group == 7).then_some(OpaqueSm(1)),
  ));
  assert!(boxed.materialize(&7, &1).is_some());
  assert!(
    boxed.materialize(&9, &1).is_none(),
    "the decline plumbs through the trait object"
  );
  assert!(
    boxed.materialize(&7, &2).is_none(),
    "a solicitor outside the replica set declines"
  );
  assert!(boxed.build(&7).is_some());
  assert!(
    boxed.build(&9).is_none(),
    "a build abort plumbs through the trait object"
  );
}

/// The blueprint's incarnation defaults to 0 — `new` keeps its signature, so every existing
/// factory is a gen-0 world — and `with_gen` carries the catalog-supplied incarnation of a
/// reshaping embedder.
#[test]
fn blueprint_generation_defaults_to_zero() {
  assert_eq!(GroupBlueprint::new(config(), 9).generation(), 0);
  assert_eq!(GroupBlueprint::new(config(), 9).with_gen(3).generation(), 3);
}

/// `Debug` is the plain derived form now that the blueprint is exactly config + seed — there is
/// no state machine to elide.
#[test]
fn debug_renders_config_and_seed() {
  let rendered = format!("{:?}", GroupBlueprint::new(config(), 7));
  assert!(rendered.contains("GroupBlueprint"), "got: {rendered}");
  assert!(rendered.contains("seed: 7"), "got: {rendered}");
}
