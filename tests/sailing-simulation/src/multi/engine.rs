//! The container's [`MultiEngine`] seam for the harnesses that keep their replicas' stores in a
//! plain `(node, gid)` table.
//!
//! The fork install needs the engine itself, not a store pair: it makes the child's storage and
//! mints its boot epoch INSIDE the call, after deciding on the pristine engine, because occupancy
//! is a decision-time fact — storage created first would be read back as "the id is spoken for"
//! and hold the fork against its own install forever.

use std::collections::{BTreeMap, BTreeSet};

use crate::{MemLog, MemStable};

/// One node's slice of a harness store table, presented as the container's storage engine.
///
/// Occupancy is exactly "this node holds stores for the id"; floors come from the harness's own
/// terminal set. The batching metrics and the visibility barrier are no-ops: these harnesses drive
/// the container directly and nothing in it reads them.
pub(crate) struct PairEngine<'a> {
  /// The node whose replicas this engine owns.
  pub(crate) node: u64,
  /// The harness's `(node, gid)` store table.
  pub(crate) stores: &'a mut BTreeMap<(u64, u64), (MemLog, MemStable<u64>)>,
  /// The per-node monotone boot-epoch counter (a fork baseline requires `>= 1`).
  pub(crate) boot_epochs: &'a mut BTreeMap<u64, u64>,
  /// Ids this node has floored terminally.
  pub(crate) floored: &'a BTreeSet<(u64, u64)>,
}

impl sailing_proto::GroupStores<u64, MemLog, MemStable<u64>> for PairEngine<'_> {
  fn stores(&mut self, group: &u64) -> Option<(&mut MemLog, &mut MemStable<u64>)> {
    self
      .stores
      .get_mut(&(self.node, *group))
      .map(|(l, s)| (l, s))
  }
}

impl sailing_proto::FloorStore<u64> for PairEngine<'_> {
  fn floor(&self, gid: &u64) -> u64 {
    if self.floored.contains(&(self.node, *gid)) {
      sailing_proto::MERGED_FLOOR
    } else {
      0
    }
  }

  fn lineage(&self, _gid: &u64) -> u64 {
    0
  }
}

impl sailing_proto::MultiEngine<u64, u64> for PairEngine<'_> {
  type Log = MemLog;
  type Stable = MemStable<u64>;

  fn set_snapshot_staging_cap(&mut self, _cap: usize) {}

  fn group_ids(&self) -> impl Iterator<Item = &u64> {
    self
      .stores
      .keys()
      .filter(|(n, _)| *n == self.node)
      .map(|(_, gid)| gid)
  }

  fn barriers(&self) -> u64 {
    0
  }

  fn ops_batched(&self) -> u64 {
    0
  }

  fn has_staged(&self) -> bool {
    false
  }

  fn flush(&mut self) -> usize {
    0
  }

  fn add_group(&mut self, gid: u64) -> bool {
    if self.stores.contains_key(&(self.node, gid)) {
      return false;
    }
    self
      .stores
      .insert((self.node, gid), (MemLog::new(), MemStable::new()));
    true
  }

  fn remove_group(&mut self, gid: &u64) -> bool {
    self.stores.remove(&(self.node, *gid)).is_some()
  }

  fn contains_group(&self, gid: &u64) -> bool {
    self.stores.contains_key(&(self.node, *gid))
  }

  fn next_boot_epoch(&mut self, gid: &u64) -> Option<u64> {
    if !self.stores.contains_key(&(self.node, *gid)) {
      return None;
    }
    let epoch = self.boot_epochs.entry(self.node).or_default();
    *epoch = epoch.checked_add(1)?;
    Some(*epoch)
  }

  fn set_group_floor(&mut self, _gid: &u64, _floor: u64) {}

  fn set_group_gen(&mut self, _gid: &u64, _generation: u64) {}

  fn removal_floor(&self, _gid: &u64) -> u64 {
    0
  }
}
