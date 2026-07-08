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
  ///     TARGET). The parent's copy of such a key is the ABSORBED lineage, not a continuation of
  ///     the split's assignment, so it need not relate to the child's inherited copy at all: it
  ///     may extend the child's history (which the LOSS leg reads as truncation) or descend from
  ///     a DIFFERENT source entirely (which the DUP leg reads as a double-claim). Both legs
  ///     exempt a re-acquired key; the merge's own [`assert_union`](Self::assert_union) judges the
  ///     re-introduced history. A key no union re-acquired still trips both legs, so a genuine
  ///     double-claim — two sides past a common prefix with no re-acquiring union — is caught.
  ///
  /// `inherited` exempts individual PARENT-side cells, not whole keys. An absorb copies the
  /// source's whole FSM RECORD into the target — including cells for keys OUTSIDE the source's
  /// owned population (carried/foreign-tagged cells) — so cells of an ASSIGNED key can reach the
  /// parent's append-only ledger history through a union whose `absorbed_keys` never named the
  /// key, invisible to the key-level `reacquired` set. The caller supplies, per assigned key, the
  /// values found in any inbound union source's record; on BOTH legs the parent counts as
  /// continuing past the common prefix only on an OWN-WRITE witness — a post-prefix cell whose
  /// value is NOT exempt. Values are globally unique, so a fresh own-write can never appear in
  /// any union source's history (the exemption cannot mask a REAL double-claim: an own-write on
  /// both sides still trips both legs); conversely a value in a union source's history reached
  /// the parent's record only via that absorb. The shared prefix itself is never filtered — an
  /// inherited cell inside it is legitimate common history the child copied at fork, and dropping
  /// it would break the prefix match. The child side is untouched (a union into the CHILD is the
  /// key-level `absorbed` exemption). The LOSS leg applies the same reduction: a parent tail that
  /// is entirely inherited counts as not-continuing (the common prefix is the effective end), so
  /// a child stopping short of only-inherited tail cells does not trip. Accepted residual: an
  /// inherited-then-lost parent tail is excused here — the inbound union's own
  /// [`assert_union`](Self::assert_union) judges that absorb.
  ///
  /// Panics with the group ids, the key, and both histories on any violation.
  pub(crate) fn assert_partition(
    &self,
    parent: u64,
    child: u64,
    child_keys: &BTreeSet<u16>,
    absorbed: &BTreeSet<u16>,
    reacquired: &BTreeSet<u16>,
    inherited: &BTreeMap<u16, BTreeSet<u64>>,
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
      let inh = inherited.get(&k);
      // Own-write witness: the parent continues past the common prefix only via a cell no
      // inbound union's record carries — an inherited-only tail is that union's cargo, not a
      // continuation of the split's assignment.
      let parent_continues = p[common..]
        .iter()
        .any(|&(_, value)| !inh.is_some_and(|s| s.contains(&value)));
      assert!(
        !(parent_continues && common < c.len()) || reacquired.contains(&k),
        "[conservation] split g{parent}->g{child}: key {k} continued on BOTH sides past their \
         common prefix ({common} cells)\n  parent={p:?}\n  child={c:?}",
      );
      assert!(
        !parent_continues || reacquired.contains(&k),
        "[conservation] split g{parent}->g{child}: key {k} history LOST — the child's copy \
         stops short of the parent's recorded history\n  parent={p:?}\n  child={c:?}",
      );
    }
  }

  /// Assert the merge of `source` into `target` absorbed every OWNED source key's FULL history:
  /// the target's recorded history for the key must CONTAIN the source's, in order (an ordered
  /// subsequence). Not a leading prefix: a cross-incarnation merge folds the source's cells under
  /// the target's ledger id where a PRIOR incarnation's cells for that key already sit, so the
  /// absorbed run trails the target's own. The state machine's `absorb` appends the source record
  /// intact, so a faithful absorb leaves those cells as a contiguous in-order run the subsequence
  /// check finds; a dropped, altered, or reordered source cell breaks the match and still trips.
  /// `absorbed_keys` is the source's key POPULATION at the merge — the keys this union actually
  /// handed over. A key the source WROTE but then SPLIT AWAY before merging is NOT here (it rode
  /// that split, whose partition verdict judges the handover), so it is not demanded of the
  /// target — a written-history (`keys_of`) sweep would false-trip it. An owned key the source
  /// never wrote judges vacuously (its source history is empty).
  ///
  /// `departed` exempts individual CELLS, not whole keys. The ledger is append-only save for the ONE
  /// legitimate FSM mutation — `LogSm::split` evicts a moved key's cells record-wide — so cells a
  /// split carried away are demanded of no later target. The caller supplies them per key: values are
  /// globally unique (the fuzzer's command counter), so a value present in a registered split child's
  /// record for `k` is an EXACT departure witness. Exempt values are dropped from the demand before
  /// the subsequence check; the remainder's order is preserved, so a dropped NON-exempt cell still
  /// trips — the seam keeps its teeth for every cell the caller does not hand it. Panics with the
  /// group ids, the key, and both histories otherwise.
  pub(crate) fn assert_union(
    &self,
    target: u64,
    source: u64,
    absorbed_keys: &BTreeSet<u16>,
    departed: &BTreeMap<u16, BTreeSet<u64>>,
  ) {
    for &k in absorbed_keys {
      let s = self.history(source, k);
      let t = self.history(target, k);
      let gone = departed.get(&k);
      let mut ti = t.iter();
      let absorbed = s
        .iter()
        .filter(|&&(_, value)| !gone.is_some_and(|d| d.contains(&value)))
        .all(|cell| ti.by_ref().any(|tc| tc == cell));
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
