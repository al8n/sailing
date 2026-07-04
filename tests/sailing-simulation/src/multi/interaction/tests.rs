use super::*;

/// Every directive requires the g= scope; a missing one is a rendered error, not a panic, so a
/// malformed scenario file fails its golden comparison legibly.
#[test]
fn missing_group_argument_renders_an_error() {
  let mut env = MultiInteractionEnv::new();
  let out = env.exec(&Directive::parse("campaign 1"));
  assert!(out.contains("missing mandatory g=<gid>"), "got: {out}");
}

/// `voters=` accepts both the parenthesised list and the bare comma form.
#[test]
fn voters_accept_bare_comma_lists() {
  let mut env = MultiInteractionEnv::new();
  let out = env.exec(&Directive::parse("create-group g=7 voters=1,2,3"));
  assert_eq!(out.matches("created g7").count(), 3, "got: {out}");
  assert!(env.hosts[&2].contains_group(&7));
}

/// An end-to-end elect + commit round through the directive engine: the explicit campaign +
/// per-group stabilize drive one deterministic election, and a sibling group stays pristine.
#[test]
fn campaign_and_stabilize_elect_one_group_only() {
  let mut env = MultiInteractionEnv::new();
  env.exec(&Directive::parse("create-group g=100 voters=(1,2,3)"));
  env.exec(&Directive::parse("create-group g=200 voters=(1,2,3)"));
  env.exec(&Directive::parse("campaign g=100 1"));
  env.exec(&Directive::parse("stabilize g=100"));
  let state100 = env.exec(&Directive::parse("raft-state g=100"));
  assert!(state100.contains("n1: leader"), "got: {state100}");
  let state200 = env.exec(&Directive::parse("raft-state g=200"));
  assert!(
    state200.lines().all(|l| l.contains("follower term=0")),
    "the sibling group must be untouched, got: {state200}"
  );
}
