//! The instruction-conservation oracle seam for P6 split/merge: a PURE ledger of per-`(group,
//! key)` write histories with the partition (split) and union (merge) assertions. M4 wires the
//! ledger to real split actions and M5 to merges; nothing here touches a world.
//!
//! Cells are opaque `(index, value)` pairs compared exactly; a "handover" means the receiving
//! group's history for a key STARTS WITH the giving group's full recorded history as a prefix
//! (however the wiring materializes it — e.g. a fork snapshot baseline), so conservation is
//! judged on recorded observations alone.

use std::{
  collections::{BTreeMap, BTreeSet},
  vec::Vec,
};

/// A pure per-`(group, key)` history ledger with split/merge conservation assertions.
pub(crate) struct ConservationLedger {
  /// `(gid, key)` → the ordered `(index, value)` cells recorded for that key on that group.
  histories: BTreeMap<(u64, u16), Vec<(u64, u64)>>,
}

impl ConservationLedger {
  /// An empty ledger.
  pub(crate) fn new() -> Self {
    Self {
      histories: BTreeMap::new(),
    }
  }

  /// Append one observed cell to `(gid, key)`'s history.
  pub(crate) fn record(&mut self, gid: u64, key: u16, index: u64, value: u64) {
    self
      .histories
      .entry((gid, key))
      .or_default()
      .push((index, value));
  }

  /// The recorded history for `(gid, key)` (empty if never recorded).
  fn history(&self, gid: u64, key: u16) -> &[(u64, u64)] {
    self
      .histories
      .get(&(gid, key))
      .map(Vec::as_slice)
      .unwrap_or(&[])
  }

  /// Every key recorded under `gid`, ascending.
  fn keys_of(&self, gid: u64) -> BTreeSet<u16> {
    self
      .histories
      .keys()
      .filter(|(g, _)| *g == gid)
      .map(|(_, k)| *k)
      .collect()
  }

  /// Assert the split of `parent` that assigned `child_keys` to `child` CONSERVED every key's
  /// history — each continues in exactly one side:
  ///   - an assigned key's child history must start with the parent's FULL recorded history (the
  ///     transferred baseline; anything shorter or diverging is LOSS);
  ///   - once both sides extend past their common prefix the key continued on BOTH sides (DUP);
  ///   - an unassigned key must never surface in the child at all (CROSS-TALK).
  ///
  /// Panics with the group ids, the key, and both histories on any violation.
  pub(crate) fn assert_partition(&self, parent: u64, child: u64, child_keys: &BTreeSet<u16>) {
    let mut keys = self.keys_of(parent);
    keys.extend(self.keys_of(child));
    for k in keys {
      let p = self.history(parent, k);
      let c = self.history(child, k);
      if !child_keys.contains(&k) {
        assert!(
          c.is_empty(),
          "[conservation] split g{parent}->g{child}: key {k} was never assigned to the child \
           but surfaced there\n  child={c:?}",
        );
        continue;
      }
      let common = p.iter().zip(c.iter()).take_while(|(a, b)| a == b).count();
      assert!(
        !(common < p.len() && common < c.len()),
        "[conservation] split g{parent}->g{child}: key {k} continued on BOTH sides past their \
         common prefix ({common} cells)\n  parent={p:?}\n  child={c:?}",
      );
      assert!(
        common == p.len(),
        "[conservation] split g{parent}->g{child}: key {k} history LOST — the child's copy \
         stops short of the parent's recorded history\n  parent={p:?}\n  child={c:?}",
      );
    }
  }

  /// Assert the merge of `source` into `target` absorbed every source key's FULL history: the
  /// target's copy must start with the source's recorded history as a prefix (M5's shape).
  /// Panics with the group ids, the key, and both histories otherwise.
  #[cfg_attr(
    not(test),
    expect(dead_code, reason = "the merge milestone wires assert_union")
  )]
  pub(crate) fn assert_union(&self, target: u64, source: u64) {
    for k in self.keys_of(source) {
      let s = self.history(source, k);
      let t = self.history(target, k);
      let absorbed = t.len() >= s.len() && &t[..s.len()] == s;
      assert!(
        absorbed,
        "[conservation] merge g{source}->g{target}: key {k} source history not absorbed\n  \
         source={s:?}\n  target={t:?}",
      );
    }
  }
}

#[cfg(test)]
mod tests;
