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
  l.assert_partition(100, 200, &keyset(&[2, 7]));
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
  l.assert_partition(100, 200, &keyset(&[2]));
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
  l.assert_partition(100, 200, &keyset(&[2]));
}

/// CROSS-TALK: a key the instruction never assigned surfaces in the child.
#[test]
#[should_panic(expected = "never assigned")]
fn partition_panics_on_an_unassigned_key_in_the_child() {
  let mut l = ConservationLedger::new();
  l.record(100, 1, 1, 10);
  l.record(200, 1, 3, 9);
  l.assert_partition(100, 200, &keyset(&[2]));
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
