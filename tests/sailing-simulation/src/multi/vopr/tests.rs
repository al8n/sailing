use super::*;

/// The weighted pick is deterministic and covers the whole menu across a seed range.
#[test]
fn pick_action_is_deterministic_and_covers_the_menu() {
  let profile = MultiProfile::default_multi();
  let draw = |seed: u64| -> Vec<MultiAction> {
    let mut p = FaultPrng::new(seed);
    (0..512).map(|_| pick_action(&mut p, profile)).collect()
  };
  assert_eq!(draw(7), draw(7), "same seed ⇒ same action stream");
  let stream = draw(7);
  for (action, _) in profile.0 {
    assert!(
      stream.contains(action),
      "512 draws never hit {action:?} — the weights are broken"
    );
  }
}

/// The viability budget: a 3-voter group survives one directed mute (a live candidate leader
/// still reaches a majority), but a mute that would cut the LAST viable majority is refused.
#[test]
fn group_quorum_viability_counts_mutes_and_isolation() {
  let mut w = MultiWorld::new(31);
  for n in 0..3 {
    w.add_node(n);
  }
  let all: std::collections::BTreeSet<u64> = (0..3).collect();
  w.create_group(100, &all);
  assert!(w.run_until(600, |w| w.leader_of(100).is_some()));
  w.reconcile_membership(100);

  // A single directed mute leaves viable leaders (e.g. node 2 reaches both peers).
  assert!(group_has_viable_quorum(&w, 100, None, Some((0, 1))));
  // Isolating one node still leaves a 2-of-3 majority pair.
  assert!(group_has_viable_quorum(&w, 100, Some(0), None));
  // But with node 0 isolated AND the 1↔2 link muted, no candidate reaches a majority.
  w.isolate(0);
  assert!(!group_has_viable_quorum(&w, 100, None, Some((1, 2))));
}
