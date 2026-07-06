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

  /// The recorded history for `(gid, key)` (empty if never recorded). `pub(crate)` so the
  /// world's recorder tests can assert exact recorded contents, not just the verdict.
  pub(crate) fn history(&self, gid: u64, key: u16) -> &[(u64, u64)] {
    self
      .histories
      .get(&(gid, key))
      .map(Vec::as_slice)
      .unwrap_or(&[])
  }

  /// Every key recorded under `gid`, ascending. `pub(crate)` so the world can assemble the set of
  /// keys a registered union carried into a group (the split partition's absorb exemption).
  pub(crate) fn keys_of(&self, gid: u64) -> BTreeSet<u16> {
    self
      .histories
      .keys()
      .filter(|(g, _)| *g == gid)
      .map(|(_, k)| *k)
      .collect()
  }

  /// Assert the split of `parent` that assigned `child_keys` to `child` CONSERVED every key's
  /// history — each continues in exactly one side. The split-merge algebra reunifies sides, so
  /// the two closure sets exempt exactly the keys a REGISTERED union re-routed, symmetrically:
  ///   - `absorbed` — keys a union carried INTO the child (the child became a merge TARGET). An
  ///     unassigned key normally may never surface in the child (CROSS-TALK), but a child that
  ///     absorbs a source legitimately gains the source's whole population — including keys the
  ///     source OWNED but never wrote, which the child then writes for the first time. Those keys
  ///     are exempted here; the merge's own [`assert_union`](Self::assert_union) judges any
  ///     absorbed HISTORY.
  ///   - `reacquired` — keys a union re-introduced INTO the parent (the parent became a merge
  ///     TARGET). An assigned key's child history normally must cover the parent's FULL recorded
  ///     history (a shorter child is LOSS), but once the parent RE-ACQUIRES that key via a merge
  ///     it writes past what the child inherited — its history legitimately grows longer. The
  ///     DUP leg still guards against the two sides DIVERGING (a genuine double-claim trips
  ///     regardless), so the relaxation only forgives the parent extending a prefix the child
  ///     opened; the merge's union verdict judges the re-introduced history.
  ///
  /// Panics with the group ids, the key, and both histories on any violation.
  pub(crate) fn assert_partition(
    &self,
    parent: u64,
    child: u64,
    child_keys: &BTreeSet<u16>,
    absorbed: &BTreeSet<u16>,
    reacquired: &BTreeSet<u16>,
  ) {
    let mut keys = self.keys_of(parent);
    keys.extend(self.keys_of(child));
    for k in keys {
      let p = self.history(parent, k);
      let c = self.history(child, k);
      if !child_keys.contains(&k) {
        if absorbed.contains(&k) {
          continue; // carried in by a registered union — the merge's assert_union judges it
        }
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
        common == p.len() || reacquired.contains(&k),
        "[conservation] split g{parent}->g{child}: key {k} history LOST — the child's copy \
         stops short of the parent's recorded history\n  parent={p:?}\n  child={c:?}",
      );
    }
  }

  /// Assert the merge of `source` into `target` absorbed every OWNED source key's FULL history:
  /// the target's copy must start with the source's recorded history as a prefix (M5's shape).
  /// `absorbed_keys` is the source's key POPULATION at the merge — the keys this union actually
  /// handed over. A key the source WROTE but then SPLIT AWAY before merging is NOT here (it rode
  /// that split, whose partition verdict judges the handover), so it is not demanded of the
  /// target — a written-history (`keys_of`) sweep would false-trip it. An owned key the source
  /// never wrote judges vacuously (its source history is empty). Panics with the group ids, the
  /// key, and both histories otherwise.
  pub(crate) fn assert_union(&self, target: u64, source: u64, absorbed_keys: &BTreeSet<u16>) {
    for &k in absorbed_keys {
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
