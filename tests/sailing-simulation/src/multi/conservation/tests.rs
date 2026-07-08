use super::*;
use std::collections::{BTreeMap, BTreeSet};

fn keyset(keys: &[u16]) -> BTreeSet<u16> {
  keys.iter().copied().collect()
}

/// No departed cells — the common case, and the shape every pre-exemption call site asserts.
fn no_departed() -> BTreeMap<u16, BTreeSet<u64>> {
  BTreeMap::new()
}

/// No union-carried parent cells — the common case, and the shape every pre-exemption partition
/// call site asserts.
fn no_inherited() -> BTreeMap<u16, BTreeSet<u64>> {
  BTreeMap::new()
}

/// The green partition shape: the child starts from the parent's exact baseline for its assigned
/// keys and continues alone; unassigned keys stay parent-only; untouched keys assert nothing.
#[test]
fn partition_holds_for_a_clean_handover() {
  let mut l = ConservationLedger::new();
  // Key 1 stays with the parent (not assigned) and keeps growing there.
  l.record(100, 1, 1, 10);
  l.record(100, 1, 3, 12);
  // Key 2 is assigned to the child: the parent's history is the transferred baseline.
  l.record(100, 2, 2, 11);
  l.record(200, 2, 2, 11);
  l.record(200, 2, 9, 20);
  l.assert_partition(
    100,
    200,
    &keyset(&[2, 7]),
    &BTreeSet::new(),
    &BTreeSet::new(),
    &no_inherited(),
  );
}

/// LOSS: the child's copy of an assigned key stops short of the parent's recorded history — the
/// baseline arrived truncated.
#[test]
#[should_panic(expected = "history LOST")]
fn partition_panics_on_lost_history() {
  let mut l = ConservationLedger::new();
  l.record(100, 2, 2, 11);
  l.record(100, 2, 4, 15);
  l.record(200, 2, 2, 11);
  l.assert_partition(
    100,
    200,
    &keyset(&[2]),
    &BTreeSet::new(),
    &BTreeSet::new(),
    &no_inherited(),
  );
}

/// DUP: after the handover both sides extend the common baseline — the key's history continued
/// in two places at once.
#[test]
#[should_panic(expected = "BOTH sides")]
fn partition_panics_on_a_double_continuation() {
  let mut l = ConservationLedger::new();
  l.record(100, 2, 2, 11);
  l.record(100, 2, 6, 17);
  l.record(200, 2, 2, 11);
  l.record(200, 2, 5, 16);
  l.assert_partition(
    100,
    200,
    &keyset(&[2]),
    &BTreeSet::new(),
    &BTreeSet::new(),
    &no_inherited(),
  );
}

/// CROSS-TALK: a key the instruction never assigned surfaces in the child.
#[test]
#[should_panic(expected = "never assigned")]
fn partition_panics_on_an_unassigned_key_in_the_child() {
  let mut l = ConservationLedger::new();
  l.record(100, 1, 1, 10);
  l.record(200, 1, 3, 9);
  l.assert_partition(
    100,
    200,
    &keyset(&[2]),
    &BTreeSet::new(),
    &BTreeSet::new(),
    &no_inherited(),
  );
}

/// The green union shape (M5's merge): the target absorbed every source key's full history as a
/// prefix and continues alone.
#[test]
fn union_holds_when_the_target_absorbs_the_source() {
  let mut l = ConservationLedger::new();
  l.record(300, 4, 2, 8);
  l.record(300, 4, 5, 9);
  l.record(400, 4, 2, 8);
  l.record(400, 4, 5, 9);
  l.record(400, 4, 11, 14);
  l.record(400, 6, 1, 3); // a target-only key asserts nothing
  l.assert_union(400, 300, &keyset(&[4]), &no_departed());
}

/// Union LOSS: the target's copy of a source key diverges from (or stops short of) the source's
/// recorded history. The key IS in the transferred population (owned at merge), so a target that
/// dropped its history is a genuine absorb failure — it still trips.
#[test]
#[should_panic(expected = "not absorbed")]
fn union_panics_when_source_history_is_dropped() {
  let mut l = ConservationLedger::new();
  l.record(300, 4, 2, 8);
  l.record(300, 4, 5, 9);
  l.record(400, 4, 2, 8);
  l.assert_union(400, 300, &keyset(&[4]), &no_departed());
}

/// The union judges the transferred POPULATION, not every key the source ever wrote. Key 4 was
/// written long ago then SPLIT AWAY before the merge, so the target legitimately never had it
/// (that split's partition verdict judged the handover) and it is absent from the absorbed set —
/// the union holds. A written-history (`keys_of`) sweep would demand key 4 of the target and
/// false-trip.
#[test]
fn union_ignores_a_key_the_source_split_away_before_merging() {
  let mut l = ConservationLedger::new();
  l.record(300, 4, 1, 5); // written, then split away before the merge
  l.record(300, 5, 2, 8); // still owned at the merge
  l.record(400, 5, 2, 8); // the target absorbed only the owned key
  l.assert_union(400, 300, &keyset(&[5]), &no_departed());
}

/// The seed-7 cross-incarnation shape: a recreated source (its own ledger id) hands the target
/// key 5 = [(2, 85)], but the target already recorded key 5 up to value 105 from a PRIOR
/// incarnation it absorbed — so the absorbed cell (value 85, below the target's own max) trails
/// the target's own run rather than leading it. The union finds it as an ordered subsequence and
/// holds; a strict-prefix check (or a value-watermark recorder that dropped the cell) false-trips.
#[test]
fn union_finds_an_absorbed_cell_below_the_targets_own_max() {
  let mut l = ConservationLedger::new();
  l.record(1_000_101, 5, 2, 85); // the recreated source's key 5
  // The target's own key-5 run from a prior incarnation, maxed at 105 ...
  for (index, value) in [(3, 5), (11, 75), (18, 93), (24, 99), (30, 105)] {
    l.record(103, 5, index, value);
  }
  l.record(103, 5, 2, 85); // ... then the absorbed cell, trailing below that max
  l.assert_union(103, 1_000_101, &keyset(&[5]), &no_departed());
}

/// The cross-incarnation union still catches a genuine drop: the source handed key 5 =
/// [(2, 85), (6, 120)], the target absorbed only (2, 85) (below its own max) and never the later
/// (6, 120). The subsequence match fails on the missing cell — a dropped source cell trips even
/// when an earlier absorbed cell is present.
#[test]
#[should_panic(expected = "not absorbed")]
fn union_trips_on_a_dropped_cell_in_a_cross_incarnation_merge() {
  let mut l = ConservationLedger::new();
  l.record(1_000_101, 5, 2, 85);
  l.record(1_000_101, 5, 6, 120); // the source wrote this too ...
  for (index, value) in [(3, 5), (11, 75), (30, 105)] {
    l.record(103, 5, index, value);
  }
  l.record(103, 5, 2, 85); // ... but the target absorbed only (2, 85), never (6, 120)
  l.assert_union(103, 1_000_101, &keyset(&[5]), &no_departed());
}

/// The seed-4 shape: a child that later becomes a merge TARGET carries in the source's keys. The
/// split never assigned them, but a REGISTERED union covers them, so the partition exempts them
/// instead of false-tripping its never-assigned cross-talk leg (the merge's union verdict judges
/// those histories separately).
#[test]
fn partition_exempts_a_key_a_registered_union_carried_in() {
  let mut l = ConservationLedger::new();
  // The split hands key 2 to the child.
  l.record(100, 2, 2, 11);
  l.record(200, 2, 2, 11);
  // Post-birth the child absorbs a source, so its key 5 now surfaces in the child's history.
  l.record(200, 5, 7, 30);
  // Key 5 registered as absorbed ⇒ the partition holds; without the exemption it would trip.
  l.assert_partition(
    100,
    200,
    &keyset(&[2]),
    &keyset(&[5]),
    &BTreeSet::new(),
    &no_inherited(),
  );
}

/// The negative pin: the exemption is EXACT. A key that surfaces in the child but was neither
/// assigned by the split NOR carried in by a registered union is genuine cross-talk — it still
/// trips, even alongside a legitimately absorbed key.
#[test]
#[should_panic(expected = "never assigned")]
fn partition_still_trips_on_a_key_no_union_carried() {
  let mut l = ConservationLedger::new();
  l.record(100, 2, 2, 11);
  l.record(200, 2, 2, 11);
  l.record(200, 5, 7, 30); // absorbed via a union — exempt
  l.record(200, 8, 9, 40); // NOT assigned and NOT absorbed — a real cross-talk leak
  l.assert_partition(
    100,
    200,
    &keyset(&[2]),
    &keyset(&[5]),
    &BTreeSet::new(),
    &no_inherited(),
  );
}

/// The seed-0 LOSS shape closed: the split hands key 2 to the child, then the child MERGES BACK
/// into the parent, which re-acquires key 2 and writes past the child's inherited copy. The
/// child's history is now a PREFIX of the parent's — a registered union re-introduced the key —
/// so the partition holds instead of false-tripping its LOSS leg (the merge's union verdict
/// judges the re-introduced history).
#[test]
fn partition_exempts_a_key_a_union_re_acquired_into_the_parent() {
  let mut l = ConservationLedger::new();
  l.record(100, 2, 2, 11);
  l.record(200, 2, 2, 11);
  // The parent writes key 2 anew after re-absorbing the child: its history grows past the
  // child's inherited copy.
  l.record(100, 2, 9, 20);
  l.record(100, 2, 12, 25);
  l.assert_partition(
    100,
    200,
    &keyset(&[2]),
    &BTreeSet::new(),
    &keyset(&[2]),
    &no_inherited(),
  );
}

/// The seed-3 DUP shape closed: the split hands key 3 to the child, then the PARENT re-acquires
/// key 3 by absorbing a DIFFERENT source. The parent's copy is that source's lineage, disjoint
/// from the child's inherited one (common prefix ZERO), so both sides carry cells past their
/// (empty) common prefix — the DUP shape. Marked re-acquired, the leg exempts it: the two copies
/// are the child's inheritance and the parent's re-acquisition, not a split leak, and the merge's
/// union verdict judges the re-introduced history. The genuine-DUP pin above
/// ([`partition_panics_on_a_double_continuation`], no re-acquiring union) still trips.
#[test]
fn partition_exempts_a_divergent_key_a_union_re_acquired_into_the_parent() {
  let mut l = ConservationLedger::new();
  // The split hands key 3 to the child, which writes its own cell.
  l.record(178, 3, 3, 1353);
  // The parent re-acquires key 3 by absorbing a different source — a disjoint lineage.
  l.record(175, 3, 2, 1563);
  l.record(175, 3, 55, 1579);
  l.assert_partition(
    175,
    178,
    &keyset(&[3]),
    &BTreeSet::new(),
    &keyset(&[3]),
    &no_inherited(),
  );
}

/// The split-eviction exemption (the seed-8 g333→g388 shape): key 7 was written on the source, then
/// SPLIT AWAY to a child (its cells evicted record-wide), then RE-ACQUIRED from another owner and
/// continued. The source now holds the early cells plus the re-acquired run, but the union target
/// absorbed only the re-acquired run — the departed early cells left with the split child. Passing
/// them as `departed` exempts them and the union holds; the split-blind demand would false-trip.
#[test]
fn union_exempts_cells_a_registered_split_carried_away() {
  let mut l = ConservationLedger::new();
  l.record(500, 7, 1, 100); // early — split away to the child
  l.record(500, 7, 3, 101);
  l.record(500, 7, 8, 200); // re-acquired run
  l.record(500, 7, 9, 201);
  // The target absorbed only the re-acquired run (plus a target-only cell of its own).
  l.record(600, 7, 8, 200);
  l.record(600, 7, 9, 201);
  l.record(600, 7, 12, 202);
  let mut departed: BTreeMap<u16, BTreeSet<u64>> = BTreeMap::new();
  departed.insert(7, [100, 101].into_iter().collect());
  l.assert_union(600, 500, &keyset(&[7]), &departed);
}

/// The oracle keeps its teeth. Same shape as the exemption pin, but the target genuinely dropped a
/// LATE re-acquired cell (9, 201) that is NOT departed — a real absorb failure. The exemption softens
/// only the split-evicted cells, so the missing non-departed cell still breaks the subsequence match.
#[test]
#[should_panic(expected = "not absorbed")]
fn union_still_trips_when_a_non_departed_cell_is_dropped() {
  let mut l = ConservationLedger::new();
  l.record(500, 7, 1, 100); // early — split away
  l.record(500, 7, 3, 101);
  l.record(500, 7, 8, 200); // re-acquired run
  l.record(500, 7, 9, 201);
  l.record(600, 7, 8, 200); // the target dropped (9, 201) — a genuine loss, not a departure
  let mut departed: BTreeMap<u16, BTreeSet<u64>> = BTreeMap::new();
  departed.insert(7, [100, 101].into_iter().collect());
  l.assert_union(600, 500, &keyset(&[7]), &departed);
}

/// The seam exempts ONLY what it is handed, and keeps its teeth for everything else — the guarantee a
/// return-aware caller relies on. A cell a caller believes RETURNED to the source (so it withholds it
/// from `departed`) is still demanded, and its absence from the target trips. Here value 100 is left
/// out of the exemption though the source recorded it; the target dropped it, and the union trips.
#[test]
#[should_panic(expected = "not absorbed")]
fn union_still_trips_for_a_cell_the_caller_did_not_exempt() {
  let mut l = ConservationLedger::new();
  l.record(500, 7, 1, 100); // recorded on the source but NOT exempted below
  l.record(500, 7, 3, 101); // exempted — a departed cell
  l.record(500, 7, 8, 200); // an ordinary owned cell the target kept
  l.record(600, 7, 8, 200); // the target dropped the un-exempted cell (1, 100)
  let mut departed: BTreeMap<u16, BTreeSet<u64>> = BTreeMap::new();
  departed.insert(7, [101].into_iter().collect());
  l.assert_union(600, 500, &keyset(&[7]), &departed);
}

/// The carried-record exemption (the seed-8 g127/g185/g215 shape): the parent absorbed a union
/// source whose RECORD carried key-3 cells while key 3 was outside that union's absorbed
/// population, so the sweep recorded them under the parent; a later split assigned key 3 (with
/// those cells) to the child, and the parent's append-only ledger history retains them. The
/// parent's post-prefix cells are all in the union source's history — no own-write witness — so
/// the double-claim leg holds with the exemption; the key-level `reacquired` set (built from
/// absorbed POPULATIONS) is blind to this shape.
#[test]
fn partition_exempts_a_parent_tail_a_unions_record_carried_in() {
  let mut l = ConservationLedger::new();
  // The union source's record carried key-3 cells (key 3 NOT in its absorbed_keys).
  l.record(185, 3, 7, 300);
  l.record(185, 3, 9, 301);
  // The parent: its own prefix, then the carried cells the sweep recorded under it.
  l.record(1_000_127, 3, 2, 11);
  l.record(1_000_127, 3, 7, 300);
  l.record(1_000_127, 3, 9, 301);
  // The child inherited the prefix at fork and continues with its own write.
  l.record(215, 3, 2, 11);
  l.record(215, 3, 12, 400);
  let mut inherited: BTreeMap<u16, BTreeSet<u64>> = BTreeMap::new();
  inherited.insert(3, [300, 301].into_iter().collect());
  l.assert_partition(
    1_000_127,
    215,
    &keyset(&[3]),
    &BTreeSet::new(),
    &BTreeSet::new(),
    &inherited,
  );
}

/// The red assertion kept: the same carried-record shape WITHOUT the exemption is exactly the
/// latent false-trip the exemption closes — the double-claim leg reads the union's cargo as a
/// parent continuation.
#[test]
#[should_panic(expected = "BOTH sides")]
fn partition_trips_on_the_carried_record_shape_without_the_exemption() {
  let mut l = ConservationLedger::new();
  l.record(185, 3, 7, 300);
  l.record(185, 3, 9, 301);
  l.record(1_000_127, 3, 2, 11);
  l.record(1_000_127, 3, 7, 300);
  l.record(1_000_127, 3, 9, 301);
  l.record(215, 3, 2, 11);
  l.record(215, 3, 12, 400);
  l.assert_partition(
    1_000_127,
    215,
    &keyset(&[3]),
    &BTreeSet::new(),
    &BTreeSet::new(),
    &no_inherited(),
  );
}

/// The exemption keeps its teeth: values are globally unique, so a FRESH parent own-write past
/// the common prefix can never appear in any union source's history — with both sides continuing,
/// the double-claim leg still trips even though the inherited cells around it are exempt.
#[test]
#[should_panic(expected = "BOTH sides")]
fn partition_still_trips_on_a_fresh_own_write_on_both_sides() {
  let mut l = ConservationLedger::new();
  l.record(1_000_127, 3, 2, 11);
  l.record(1_000_127, 3, 7, 300); // carried in by the union — exempt
  l.record(1_000_127, 3, 10, 500); // a fresh parent own-write — the double-claim witness
  l.record(215, 3, 2, 11);
  l.record(215, 3, 12, 400);
  let mut inherited: BTreeMap<u16, BTreeSet<u64>> = BTreeMap::new();
  inherited.insert(3, [300].into_iter().collect());
  l.assert_partition(
    1_000_127,
    215,
    &keyset(&[3]),
    &BTreeSet::new(),
    &BTreeSet::new(),
    &inherited,
  );
}

/// The LOSS leg's reduction: the child stopped at the common prefix and the parent's tail is
/// entirely inherited — the union's cargo, not a continuation the child was ever handed — so the
/// common prefix is the parent's effective end and the leg holds.
#[test]
fn partition_loss_leg_excuses_an_entirely_inherited_parent_tail() {
  let mut l = ConservationLedger::new();
  l.record(1_000_127, 3, 2, 11);
  l.record(1_000_127, 3, 7, 300); // the union's cargo — the child rightly never saw it
  l.record(215, 3, 2, 11); // the child stops at the common prefix
  let mut inherited: BTreeMap<u16, BTreeSet<u64>> = BTreeMap::new();
  inherited.insert(3, [300].into_iter().collect());
  l.assert_partition(
    1_000_127,
    215,
    &keyset(&[3]),
    &BTreeSet::new(),
    &BTreeSet::new(),
    &inherited,
  );
}

/// The LOSS leg keeps its teeth: one fresh own-write in the parent's tail is a real continuation
/// the child's copy stops short of — the leg still trips through the surrounding inherited cells.
#[test]
#[should_panic(expected = "history LOST")]
fn partition_loss_leg_still_trips_on_an_own_write_in_the_tail() {
  let mut l = ConservationLedger::new();
  l.record(1_000_127, 3, 2, 11);
  l.record(1_000_127, 3, 7, 300); // inherited — exempt
  l.record(1_000_127, 3, 10, 500); // a fresh own-write the child never received
  l.record(215, 3, 2, 11);
  let mut inherited: BTreeMap<u16, BTreeSet<u64>> = BTreeMap::new();
  inherited.insert(3, [300].into_iter().collect());
  l.assert_partition(
    1_000_127,
    215,
    &keyset(&[3]),
    &BTreeSet::new(),
    &BTreeSet::new(),
    &inherited,
  );
}
