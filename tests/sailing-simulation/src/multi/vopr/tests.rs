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
  for (action, _) in profile.weights {
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

/// [`MultiProfile::snapshot_heavy`] must draw a MODERATE compacting threshold — `Some(t)` with `t`
/// in the single-group compacting band `256..=511` — deterministically per seed, and vary it
/// across seeds rather than pin a constant. This is the everyday gate's detector for the DRAW: the
/// snapshot smoke asserts only the default band's group-shape, so `snapshot_heavy` silently
/// returning `None` (or a non-compacting value) would ride a green smoke and only surface at the
/// ignored soak.
#[test]
fn snapshot_heavy_draws_a_deterministic_moderate_threshold() {
  for seed in [0u64, 1, 2, 7, 11, 36, 512, 9_999] {
    let drawn = MultiProfile::snapshot_heavy(seed).snapshot_threshold;
    assert_eq!(
      drawn,
      MultiProfile::snapshot_heavy(seed).snapshot_threshold,
      "seed {seed}: the draw must be a pure function of the seed",
    );
    let t = drawn.expect("snapshot_heavy must set a snapshot_threshold override");
    assert!(
      (256..=511).contains(&t),
      "seed {seed}: threshold {t} escaped the compacting band 256..=511",
    );
  }
  assert_eq!(
    MultiProfile::default_multi().snapshot_threshold,
    None,
    "the default profile must leave the library default in place (byte-identical to no seam)",
  );
  let spread: std::collections::BTreeSet<usize> = (0..64u64)
    .map(|s| {
      MultiProfile::snapshot_heavy(s)
        .snapshot_threshold
        .expect("override")
    })
    .collect();
  assert!(
    spread.len() > 1,
    "snapshot_heavy must spread the threshold across seeds, not pin a constant: {spread:?}",
  );
}

/// Drive all FIVE replica-construction paths the snapshot seam claims, in one world under
/// `threshold`, returning the `snapshot_threshold` each path's freshly-wired replica was built
/// under (read off the retained per-replica config — the config admitted to the container, and the
/// one crash restore rebuilds from). The paths: bootstrap create, recreate-after-remove, non-voting
/// observer, resurrect (a torn-down committed member re-wired by reconcile), and crash restore.
fn thresholds_across_construction_paths(threshold: Option<usize>) -> [(&'static str, usize); 5] {
  let mut w = MultiWorld::new(7);
  w.set_snapshot_threshold(threshold);
  for n in 0..4 {
    w.add_node(n);
  }
  let voters: std::collections::BTreeSet<u64> = (0..3).collect();

  // 1. Bootstrap create.
  w.create_group(100, &voters);
  assert!(
    w.run_until(3_000, |w| w.leader_of(100).is_some()),
    "create: the group must elect",
  );
  let create = w
    .replica_snapshot_threshold(0, 100)
    .expect("a bootstrap voter hosts the group");

  // 2. Recreate the retired gid — fresh replicas on the retained voter set.
  w.remove_group(100);
  w.recreate_group(100);
  assert!(
    w.run_until(3_000, |w| w.leader_of(100).is_some()),
    "recreate: the group must re-elect",
  );
  let recreate = w
    .replica_snapshot_threshold(0, 100)
    .expect("a recreated voter hosts the group");

  // 3. Non-voting observer — a joiner wired before its AddNode.
  w.wire_group_observer(100, 3);
  let observer = w
    .replica_snapshot_threshold(3, 100)
    .expect("the observer hosts the group");

  // 4. Resurrect: tear a committed non-leader voter down; reconcile re-wires it (the committed
  //    membership still lists the member, so it rejoins as a catching-up observer).
  let leader = w.leader_of(100).expect("a leader");
  let victim = *voters
    .iter()
    .find(|&&n| n != leader)
    .expect("a non-leader voter");
  w.drop_group_replica(100, victim);
  assert!(
    !w.hosts_group(victim, 100),
    "the victim replica was torn down"
  );
  w.reconcile_membership(100);
  assert!(
    w.hosts_group(victim, 100),
    "reconcile resurrected the committed member",
  );
  let resurrect = w
    .replica_snapshot_threshold(victim, 100)
    .expect("the resurrected member hosts the group");

  // 5. Crash restore: rebuild a clean, untouched voter from its retained durable config.
  let restart = *voters
    .iter()
    .find(|&&n| n != leader && n != victim)
    .expect("the third voter");
  w.crash(restart);
  assert!(w.hosts_group(restart, 100), "the crashed voter restored");
  let crash_restore = w
    .replica_snapshot_threshold(restart, 100)
    .expect("the restored voter hosts the group");

  [
    ("create", create),
    ("recreate", recreate),
    ("observer", observer),
    ("resurrect", resurrect),
    ("crash-restore", crash_restore),
  ]
}

/// The profile's `snapshot_threshold` override must REACH the replica config on every construction
/// path — create, recreate, observer, resurrect, crash restore — and `None` must leave the library
/// default untouched on each. The snapshot smoke checks only the default band's group-shape, so a
/// construction path silently dropping the override rides a green smoke; this is the everyday
/// gate's structural detector for that plumbing (the soak is otherwise the first to catch it).
#[test]
fn snapshot_threshold_override_reaches_every_construction_path() {
  let heavy = MultiProfile::snapshot_heavy(7)
    .snapshot_threshold
    .expect("snapshot_heavy sets an override");
  for (path, got) in thresholds_across_construction_paths(Some(heavy)) {
    assert_eq!(
      got, heavy,
      "{path}: the snapshot-heavy override must reach the replica config",
    );
  }

  // The library default the untouched (`None`) path must leave in place — derived from a plain
  // config so the assertion tracks the library, not a magic number.
  let library_default = sailing_proto::Config::try_new(
    0u64,
    std::vec![0u64],
    std::time::Duration::from_millis(1000),
    std::time::Duration::from_millis(100),
  )
  .expect("a valid reference config")
  .snapshot_threshold();
  assert!(
    !(256..=511).contains(&library_default),
    "sanity: the library default {library_default} must sit outside the compacting band",
  );
  for (path, got) in thresholds_across_construction_paths(None) {
    assert_eq!(
      got, library_default,
      "{path}: the default profile must leave the library default untouched",
    );
  }
}

/// The reshape menu is the default menu plus ONE added row: same actions at the same weights
/// (the default mix stays the reference point), `Split` present only here, no knob overrides.
/// The default table gaining a `Split` row would break the menu-coverage invariant (every
/// listed row drawable), so its absence there is asserted too.
#[test]
fn reshape_extends_the_default_menu_by_the_split_row() {
  let d = MultiProfile::default_multi();
  let r = MultiProfile::reshape();
  for (action, weight) in d.weights {
    assert!(
      r.weights.contains(&(*action, *weight)),
      "reshape must keep the default row {action:?}@{weight}",
    );
  }
  assert_eq!(r.weights.len(), d.weights.len() + 1);
  assert!(
    r.weights
      .iter()
      .any(|(a, w)| *a == MultiAction::Split && *w > 0),
    "reshape must weight Split in",
  );
  assert!(
    !d.weights.iter().any(|(a, _)| *a == MultiAction::Split),
    "the default menu must NOT list Split (absence is weight zero)",
  );
  assert_eq!(
    r.snapshot_threshold, None,
    "reshape overrides weights only — construction stays library-default",
  );
}

/// The reshape draw covers its whole menu — Split included — deterministically.
#[test]
fn reshape_pick_covers_the_menu() {
  let profile = MultiProfile::reshape();
  let draw = |seed: u64| -> Vec<MultiAction> {
    let mut p = FaultPrng::new(seed);
    (0..512).map(|_| pick_action(&mut p, profile)).collect()
  };
  assert_eq!(draw(7), draw(7), "same seed ⇒ same action stream");
  let stream = draw(7);
  for (action, _) in profile.weights {
    assert!(
      stream.contains(action),
      "512 draws never hit {action:?} — the reshape weights are broken"
    );
  }
}
