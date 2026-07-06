use super::*;
use std::collections::BTreeSet;

fn keyset(keys: &[u16]) -> BTreeSet<u16> {
  keys.iter().copied().collect()
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
  l.assert_partition(100, 200, &keyset(&[2, 7]), &BTreeSet::new());
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
  l.assert_partition(100, 200, &keyset(&[2]), &BTreeSet::new());
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
  l.assert_partition(100, 200, &keyset(&[2]), &BTreeSet::new());
}

/// CROSS-TALK: a key the instruction never assigned surfaces in the child.
#[test]
#[should_panic(expected = "never assigned")]
fn partition_panics_on_an_unassigned_key_in_the_child() {
  let mut l = ConservationLedger::new();
  l.record(100, 1, 1, 10);
  l.record(200, 1, 3, 9);
  l.assert_partition(100, 200, &keyset(&[2]), &BTreeSet::new());
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
  l.assert_union(400, 300);
}

/// Union LOSS: the target's copy of a source key diverges from (or stops short of) the source's
/// recorded history.
#[test]
#[should_panic(expected = "not absorbed")]
fn union_panics_when_source_history_is_dropped() {
  let mut l = ConservationLedger::new();
  l.record(300, 4, 2, 8);
  l.record(300, 4, 5, 9);
  l.record(400, 4, 2, 8);
  l.assert_union(400, 300);
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
  l.assert_partition(100, 200, &keyset(&[2]), &keyset(&[5]));
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
  l.assert_partition(100, 200, &keyset(&[2]), &keyset(&[5]));
}
