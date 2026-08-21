//! A shared in-memory group-storage engine with ONE batched visibility barrier.
//!
//! [`GroupEngine`] hosts every co-located Raft group's log and stable state and lends each group a
//! `(log, stable)` handle pair ([`EngineLog`], [`EngineStable`]) implementing the ordinary
//! single-group [`LogStore`]/[`StableStore`] contracts, with one engine-wide difference: a
//! `submit_*`/`compact` STAGES its completion instead of enqueueing it, and only
//! [`GroupEngine::flush`] — the batch barrier — releases every group's staged completions
//! into that group's poll FIFO. One flush is one batched in-memory visibility barrier covering ALL
//! groups' staged writes — NOT crash durability: the cross-group batching the multi-Raft design
//! exists for. This is the Sans-I/O reference implementation of the Phase-2 storage contract in
//! `MULTI_RAFT.md`; a disk engine mirrors these semantics over group-prefixed keys and a real
//! write batch — the durable form — INCLUDING the per-group lineage records (incarnation gen +
//! admission floor), which outlive `remove_group` and fold at the same barrier.
//!
//! The per-group storage invariants hold BY CONSTRUCTION under a barrier: log durability is
//! prefix-ordered because everything staged becomes durable together, and stable completions
//! release in submit order because each group's staging is a FIFO.
//!
//! **Floors survive exactly what the engine survives.** This engine is in-memory, so its
//! admission floors die with it: durable floors are the disk-engine mirror's obligation, and
//! the embedder's catalog remains the cross-restart incarnation authority until one is in use.

use crate::{
  EntriesRead, Entry, HardState, Index, LogDone, LogStore, MaybeOwned, NodeId, OpId,
  SnapshotChunkRead, SnapshotMeta, SnapshotStaging, StableDone, StableStore, Term,
};
use bytes::Bytes;
use core::{convert::Infallible, ops::Range};
use std::{
  collections::{BTreeMap, VecDeque, btree_map::Entry as MapEntry},
  vec::Vec,
};

/// A fatal fault surfaced by [`EngineStable`] as `Err`, which the consensus core treats as poison
/// — reserved for conditions genuinely terminal for an in-memory store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum EngineStorageError {
  /// A snapshot transfer declared a `total_len` the staging buffer cannot hold: over the
  /// configured cap ([`GroupEngine::set_snapshot_staging_cap`]) or beyond what the allocator
  /// grants. Terminal, not transient — an in-memory store that cannot stage the blob could not
  /// hold its durable slot either (the disk store's disk-full equivalent), and a zero watermark
  /// here would just re-solicit the same unstageable declaration forever.
  #[error("snapshot staging cannot hold the declared blob ({total_len} bytes)")]
  StagingUnallocatable {
    /// The transfer's declared blob length.
    total_len: u64,
  },
}

/// One group's hosted storage. `log` and `stable` are separate fields so [`GroupEngine::stores`]
/// can lend both mutably at once; `boot_epochs` backs [`GroupEngine::next_boot_epoch`].
#[derive(Debug)]
struct GroupStorage<I> {
  log: EngineLog,
  stable: EngineStable<I>,
  boot_epochs: u64,
}

impl<I> GroupStorage<I> {
  fn new(staging_cap: usize) -> Self {
    Self {
      log: EngineLog::new(),
      stable: EngineStable::new(staging_cap),
      boot_epochs: 0,
    }
  }
}

/// A log completion staged behind the engine barrier. An append records its extent (`upto`) so a
/// pre-barrier §5.3 conflict truncation can invalidate a superseded completion: releasing an
/// `Appended` whose extent was truncated away would claim a durable prefix through an index that
/// no longer exists — the normative [`LogStore`] prefix-durability contract forbids it.
#[derive(Debug)]
enum StagedLog {
  /// A `submit_append`'s completion; `upto` is the visible last index at submit time.
  Appended { id: OpId, upto: Index },
  /// A `compact`'s completion (front-boundary — unaffected by suffix truncation).
  Compacted(Index),
}

/// One group id's lineage: the unified incarnation/shape counter and the admission floor. Kept
/// per id, NOT per hosted group — a record must outlive [`GroupEngine::remove_group`], because
/// fencing a removed incarnation's return is the whole point of a floor.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct LineageRecord {
  generation: u64,
  floor: u64,
}

impl LineageRecord {
  /// Fold `other` in field-wise by `max` — the one merge that keeps both staging and durable
  /// slots monotone.
  fn fold(&mut self, other: Self) {
    self.generation = self.generation.max(other.generation);
    self.floor = self.floor.max(other.floor);
  }
}

/// The multi-group storage surface a multi-Raft host drives: every co-located group's
/// `(log, stable)` pair, the lineage records that fence a removed id's return, and ONE batched
/// barrier over all of them. [`GroupEngine`] is the in-memory reference implementation; a durable
/// engine renders the same surface over its own store, and a host generic over this trait accepts
/// either without a line of change.
///
/// NOT object-safe, deliberately. The store handles are ASSOCIATED types — an engine's log and
/// stable handles are its own concrete types — because a host must borrow a group's pair as
/// `(&mut Log, &mut Stable)` across a whole drive call; boxing them would erase exactly the
/// distinction the per-group borrow depends on. A host therefore owns its engine BY VALUE and
/// monomorphizes against it: there is no `dyn MultiEngine`, and none is wanted.
///
/// Per-group store resolution comes from the [`GroupStores`](crate::GroupStores) supertrait and
/// admission lookups from [`FloorStore`](crate::FloorStore), so neither is restated here.
/// CONSTRUCTION stays off the trait: an engine's resources (a heap map, a directory, a
/// write-ahead log) are its own to open, and only the code that knows the concrete engine can
/// open them.
pub trait MultiEngine<G, I>:
  crate::GroupStores<G, Self::Log, Self::Stable> + crate::FloorStore<G>
where
  G: crate::GroupId,
  I: crate::NodeId,
{
  /// The per-group log handle this engine lends.
  type Log: crate::LogStore;
  /// The per-group stable-state handle this engine lends, over the host's node id.
  type Stable: crate::StableStore<NodeId = I>;

  /// Cap every group's chunked-snapshot staging buffer, in bytes. Applies to every current and
  /// future group; a transfer declaring more than the cap fails FATALLY rather than spinning on
  /// a transfer that can never stage.
  fn set_snapshot_staging_cap(&mut self, cap: usize);

  /// The hosted group ids, ascending.
  fn group_ids(&self) -> impl Iterator<Item = &G>;

  /// How many times [`flush`](Self::flush) has run — every call counts, including a barrier that
  /// released nothing. With [`ops_batched`](Self::ops_batched) this is the batch metric.
  #[must_use]
  fn barriers(&self) -> u64;

  /// Total operations completed across every [`flush`](Self::flush) so far.
  #[must_use]
  fn ops_batched(&self) -> u64;

  /// Whether ANY hosted group holds staged-but-unreleased work — the host's exact re-arm signal
  /// for the next barrier. Staged work is invisible to the stores' `has_pending` by contract, so
  /// this is the only honest answer to "is another barrier owed?"; pending lineage writes count.
  #[must_use]
  fn has_staged(&self) -> bool;

  /// THE batch barrier: make every group's staged work VISIBLE at once — and DURABLE, for an
  /// engine with durability to offer — releasing its completions into each owning group's poll
  /// FIFO, and return how many completed. An engine whose barrier can FAIL fail-stops internally
  /// rather than reporting it here: a host has no recovery for a half-applied barrier spanning
  /// every group it hosts, so the count is total by construction.
  fn flush(&mut self) -> usize;

  /// Create EMPTY storage for `gid` — explicit admission. `false` (storage untouched) if the
  /// group is already hosted.
  fn add_group(&mut self, gid: G) -> bool;

  /// Drop `gid`'s hosted storage. The id's LINEAGE — its generation, its floor, and the removal
  /// ceiling behind [`removal_floor`](Self::removal_floor) — is deliberately RETAINED: a fence
  /// exists precisely to outlive the group it fences. `false` if no such group.
  fn remove_group(&mut self, gid: &G) -> bool;

  /// The next boot epoch for `gid` — a per-group monotonic counter (first call returns 1) that
  /// makes each incarnation's [`OpId`]s strictly exceed every prior incarnation's. `None` if no
  /// such group is hosted, or if the counter is exhausted; the increment must never wrap, since a
  /// wrapped epoch folds two incarnations onto one identity.
  #[must_use = "`None` means no such group or an exhausted counter; the returned epoch is the restore_group argument"]
  fn next_boot_epoch(&mut self, gid: &G) -> Option<u64>;

  /// Write `gid`'s admission floor. MONOTONE: a belated lower write must never soften the fence,
  /// which is what makes [`FloorStore`](crate::FloorStore)'s freshest reads safe. Durability is the
  /// next barrier.
  ///
  /// READING the floor back is NOT on this trait, deliberately. The supertrait
  /// [`FloorStore::floor`](crate::FloorStore::floor) is the single canonical accessor, and one
  /// authority is the whole point: a second reader here could answer from durable-only state while
  /// `FloorStore` answered freshest, and the admission doors are split across both — a create that
  /// took the stale answer would admit an incarnation the fence had already condemned.
  fn set_group_floor(&mut self, gid: &G, floor: u64);

  /// Write `gid`'s lineage counter — monotone, freshest-read, and barrier-durable exactly as
  /// [`set_group_floor`](Self::set_group_floor).
  fn set_group_gen(&mut self, gid: &G, generation: u64);

  /// The admission floor a REMOVAL of `gid` must persist — one past the highest generation this
  /// incarnation could have minted on this host, or `0` for an id that never reshaped. Flooring
  /// one past that ceiling discharges every outstanding gen-keyed authorization off the floor and
  /// forces a recreate to admit strictly above it, with NO knowledge of any other group.
  /// OVER-APPROXIMATION IS SAFE and UNDER-approximation is not: a floor may exceed the exact
  /// ceiling (stricter re-admission), never fall below it.
  #[must_use]
  fn removal_floor(&self, gid: &G) -> u64;
}

/// A shared in-memory storage engine: ONE engine hosts EVERY co-located group's replicated log
/// and stable state, keyed by group id, with a single batched visibility barrier
/// ([`flush`](Self::flush)) spanning all of them.
///
/// Per-group handles come from [`stores`](Self::stores) — and, under the `tcp` feature, from the
/// engine's `GroupStores` impl, which the multi-group coordinators resolve inbound frames
/// through. Resolution is STABLE and NON-ALIASING by construction (each group's pair is a
/// disjoint map entry), and an unknown group resolves to `None` — the coordinator's deliberate
/// unhosted-drop path. Groups are admitted explicitly via [`add_group`](Self::add_group),
/// mirroring the container's admission, and torn down via [`remove_group`](Self::remove_group)
/// (the Phase-5 lifecycle seam).
#[derive(Debug)]
pub struct GroupEngine<G, I> {
  groups: BTreeMap<G, GroupStorage<I>>,
  /// Durable lineage records, deliberately SEPARATE from `groups` so they survive
  /// [`remove_group`](Self::remove_group) (floors must be readable before a group exists and
  /// after it is gone).
  lineage: BTreeMap<G, LineageRecord>,
  /// Lineage writes staged since the last barrier, monotone-max folded into `lineage` by
  /// [`flush`](Self::flush).
  lineage_staged: BTreeMap<G, LineageRecord>,
  barriers: u64,
  ops_batched: u64,
  /// The per-group snapshot-staging byte cap (see
  /// [`set_snapshot_staging_cap`](Self::set_snapshot_staging_cap)); allocator-bound by default.
  staging_cap: usize,
}

// No `G` bound: nothing here keys the map. `flush` alone needs `G: Ord + I: Clone` (folding the
// staged lineage records and the visible stable slots into the durable ones), carried on the
// method rather than the block.
impl<G, I> GroupEngine<G, I> {
  /// An empty engine hosting no groups.
  #[must_use]
  pub fn new() -> Self {
    Self {
      groups: BTreeMap::new(),
      lineage: BTreeMap::new(),
      lineage_staged: BTreeMap::new(),
      barriers: 0,
      ops_batched: 0,
      staging_cap: usize::MAX,
    }
  }

  /// Cap one group's chunked-snapshot staging buffer, in bytes (default: allocator-bound —
  /// effectively unlimited). A transfer declaring a `total_len` above the cap fails FATALLY
  /// ([`EngineStorageError::StagingUnallocatable`] — the core poisons that group) rather than
  /// spinning on a transfer that can never stage. Applies to every current and future group.
  pub fn set_snapshot_staging_cap(&mut self, cap: usize) {
    self.staging_cap = cap;
    for storage in self.groups.values_mut() {
      storage.stable.staging_cap = cap;
    }
  }

  /// The number of hosted groups.
  #[must_use]
  pub fn len(&self) -> usize {
    self.groups.len()
  }

  /// Whether no groups are hosted.
  #[must_use]
  pub fn is_empty(&self) -> bool {
    self.groups.is_empty()
  }

  /// The hosted group ids, ascending.
  pub fn group_ids(&self) -> impl Iterator<Item = &G> {
    self.groups.keys()
  }

  /// How many times [`flush`](Self::flush) has run — every call counts, including a barrier that
  /// released nothing. Together with [`ops_batched`](Self::ops_batched) this is the batch metric:
  /// operations per barrier is the cross-group batch factor.
  #[must_use]
  pub const fn barriers(&self) -> u64 {
    self.barriers
  }

  /// Total operations completed across every [`flush`](Self::flush) so far.
  #[must_use]
  pub const fn ops_batched(&self) -> u64 {
    self.ops_batched
  }

  /// Whether ANY hosted group holds staged-but-unreleased work — the driver's exact re-arm signal
  /// for the next barrier. Staged completions are deliberately invisible to the stores'
  /// `has_pending` (the consensus core must not observe pre-barrier work), so a driver deriving
  /// its barrier schedule from `has_pending` — or from a prior flush's release count — would miss
  /// writes staged DURING a completion drain (the core's storage tail submits, e.g., the
  /// commit-watermark HardState write while draining an append completion). Poll this AFTER the
  /// per-group drains: `true` means schedule another barrier immediately. Pending lineage writes
  /// ([`set_group_floor`](Self::set_group_floor)/[`set_group_gen`](Self::set_group_gen)) count
  /// too — a staged floor must reach the barrier like any stable write.
  #[must_use]
  pub fn has_staged(&self) -> bool {
    self
      .groups
      .values()
      .any(|s| s.log.has_staged() || s.stable.has_staged())
      || !self.lineage_staged.is_empty()
  }

  /// THE batch barrier: make every group's staged log appends/compactions and stable
  /// writes/snapshots VISIBLE at once, releasing their completions into each owning group's poll
  /// FIFO. Returns the number of operations completed across all groups.
  ///
  /// One flush is one batched in-memory visibility barrier covering EVERY hosted group's staged
  /// writes — NOT crash durability (this engine is in-memory; the host drivers' "Storage is
  /// in-memory" notes say the same): the cross-group batching a multi-Raft host exists for (a disk
  /// engine renders the same barrier as one write batch + one fsync over group-prefixed keys, and
  /// only that adds durability). The per-group contracts hold trivially under a
  /// barrier: log durability is prefix-ordered (everything staged becomes durable together), and
  /// stable completions release in submit order (each group's staging is a FIFO). Log completions
  /// release in submit order too — the trait permits any order; submit order is the simplest
  /// conforming one. Staged lineage records fold into their durable slots here as well
  /// (field-wise max), one op per folded record.
  pub fn flush(&mut self) -> usize
  where
    G: Ord,
    I: Clone,
  {
    let mut released = 0;
    for storage in self.groups.values_mut() {
      released += storage.log.release_staged();
      released += storage.stable.release_staged();
    }
    while let Some((gid, staged)) = self.lineage_staged.pop_first() {
      self.lineage.entry(gid).or_default().fold(staged);
      released += 1;
    }
    self.barriers += 1;
    self.ops_batched += released as u64;
    released
  }
}

// The map-keyed surface. `Ord` is the only demand lookup makes — the engine neither encodes nor
// clones group ids.
impl<G, I> GroupEngine<G, I>
where
  G: Ord,
{
  /// Create EMPTY storage for `gid` — explicit admission, mirroring the container's: an inbound
  /// frame for a group without storage resolves to `None` (the deliberate unhosted-drop path)
  /// rather than materializing storage implicitly. Returns `false` (hosted storage untouched) if
  /// the group is already present.
  pub fn add_group(&mut self, gid: G) -> bool {
    match self.groups.entry(gid) {
      MapEntry::Occupied(_) => false,
      MapEntry::Vacant(v) => {
        v.insert(GroupStorage::new(self.staging_cap));
        true
      }
    }
  }

  /// Drop `gid`'s storage — log, stable state, staged and released completions, and its
  /// boot-epoch counter (the Phase-5 teardown seam). The id's LINEAGE record (gen + floor) is
  /// deliberately retained: a floor exists precisely to outlive the group it fences. Returns
  /// `false` if no such group.
  pub fn remove_group(&mut self, gid: &G) -> bool {
    self.groups.remove(gid).is_some()
  }

  /// Whether storage for `gid` is hosted.
  #[must_use]
  pub fn contains_group(&self, gid: &G) -> bool {
    self.groups.contains_key(gid)
  }

  /// The `(log, stable)` handles for `gid`, or `None` if no such group. The two handles are
  /// disjoint fields of one map entry, so a driver holds both mutably across one drive call.
  #[must_use = "`None` means no storage for this group is hosted"]
  pub fn stores(&mut self, gid: &G) -> Option<(&mut EngineLog, &mut EngineStable<I>)> {
    self
      .groups
      .get_mut(gid)
      .map(|s| (&mut s.log, &mut s.stable))
  }

  /// The next boot epoch for `gid` — a per-group monotonic counter (first call returns 1). Pass
  /// it to [`MultiRaft::restore_group`](crate::MultiRaft::restore_group) so each incarnation's
  /// [`OpId`]s strictly exceed every prior incarnation's for that group (the epoch-major ordering
  /// the completion plumbing relies on).
  ///
  /// `None` if no such group is hosted, OR if the counter is EXHAUSTED. The increment is CHECKED,
  /// never wrapping: a wrapped epoch restarts at 0 and collides with a live incarnation, folding
  /// two incarnations onto ONE `(group, epoch)` identity for every gen-keyed observer — the same
  /// identity fail-stop class as the [`OpId`]/read-round ceilings. Exhaustion is unreachable in
  /// practice (2^64 restarts of one group), so a caller surfaces the refusal rather than wrapping.
  #[must_use = "`None` means no such group or an exhausted counter; the returned epoch is the restore_group argument"]
  pub fn next_boot_epoch(&mut self, gid: &G) -> Option<u64> {
    let storage = self.groups.get_mut(gid)?;
    let next = storage.boot_epochs.checked_add(1)?;
    storage.boot_epochs = next;
    Some(next)
  }

  /// Seed `gid`'s boot-epoch counter so the exhaustion fail-stop can be exercised without 2^64
  /// calls. Panics if no such group is hosted — a test must admit the group first.
  #[cfg(test)]
  fn set_boot_epochs_for_test(&mut self, gid: &G, boot_epochs: u64) {
    self
      .groups
      .get_mut(gid)
      .expect("group must be hosted to seed its boot epoch")
      .boot_epochs = boot_epochs;
  }

  /// This reference engine's INTERNAL floor accessor — how it composes `max(durable, staged)` to
  /// satisfy [`FloorStore::floor`](crate::FloorStore::floor). Not the seam: every consumer, inside
  /// the container and out, reads the floor through `FloorStore`, which is the single canonical
  /// accessor. Kept public only so this engine's own unit tests can inspect the composition
  /// directly; there is deliberately no counterpart on [`MultiEngine`](crate::MultiEngine).
  #[must_use]
  pub fn group_floor(&self, gid: &G) -> u64 {
    let durable = self.lineage.get(gid).map_or(0, |r| r.floor);
    let staged = self.lineage_staged.get(gid).map_or(0, |r| r.floor);
    durable.max(staged)
  }

  /// This reference engine's INTERNAL lineage accessor, satisfying
  /// [`FloorStore::lineage`](crate::FloorStore::lineage) the same way
  /// [`group_floor`](Self::group_floor) satisfies the floor — and, like it, not the seam.
  #[must_use]
  pub fn group_gen(&self, gid: &G) -> u64 {
    let durable = self.lineage.get(gid).map_or(0, |r| r.generation);
    let staged = self.lineage_staged.get(gid).map_or(0, |r| r.generation);
    durable.max(staged)
  }

  /// Stage `gid`'s admission floor at `floor`. Monotone: the staged slot keeps the MAX of every
  /// write (and [`flush`](Self::flush) folds by max again), so a belated lower write can never
  /// soften the fence, which is what makes the pre-barrier reads above safe. Durability is the
  /// NEXT flush — in the removal-to-flush window the floor is volatile, and the coordinator's
  /// volatile tombstone is what covers that gap.
  pub fn set_group_floor(&mut self, gid: &G, floor: u64)
  where
    G: Clone,
  {
    // A zero write is the absent default: monotone-max makes it unobservable through every
    // read, so staging it would only manufacture a phantom record (and arm the barrier) in a
    // world that never reshaped the id.
    if floor == 0 {
      return;
    }
    let rec = self.lineage_staged.entry(gid.clone()).or_default();
    rec.floor = rec.floor.max(floor);
  }

  /// Stage `gid`'s lineage counter at `generation` — staged, monotone-max, freshest-read, and
  /// barrier-durable exactly as [`set_group_floor`](Self::set_group_floor).
  pub fn set_group_gen(&mut self, gid: &G, generation: u64)
  where
    G: Clone,
  {
    // Zero-write skip as on `set_group_floor`: a gen-0 world stays record-free.
    if generation == 0 {
      return;
    }
    let rec = self.lineage_staged.entry(gid.clone()).or_default();
    rec.generation = rec.generation.max(generation);
  }

  /// The admission floor a REMOVAL of `gid` must persist — one past the id's removal CEILING,
  /// the highest generation this incarnation could have minted on this host: the engine's
  /// mirrored lineage record ⊔ the stores' snapshot-meta lineage ⊔ a shape-kind scan of the
  /// resident log (every `Split`/`PrepareMerge`/`CommitMerge`/`RollbackMerge` entry names the
  /// generation it sets for its OWN group, and every lineage move rides the group's own log or
  /// its snapshot meta — nothing else can mint). `0` (no fence) for an id that never reshaped,
  /// preserving the gen-0 volatile-tombstone rejoin.
  ///
  /// With the drivers' eager mirrors (admission, fork relay, and the merge lineage events) the
  /// record leg already carries the ceiling, so the stores legs are almost always a no-op; they
  /// exist for the crash window where an applied move's mirror never became durable and the
  /// group was never re-hosted (a stores-only removal). Flooring one past the ceiling makes
  /// every outstanding gen-keyed authorization (a merge-abort thaw obligation's `expected`
  /// above all) discharge off the floor and forces a recreate to admit strictly above it — with
  /// NO knowledge of any other group. Over-approximation is safe: a truncated-away suffix entry
  /// only raises the fence, never lowers what durability would demand. The `+ 1` SATURATES: a
  /// ceiling at the highest working generation (one below the reserved terminal) floors at
  /// [`MERGED_FLOOR`](crate::MERGED_FLOOR) — the id is then permanently retired, exactly the
  /// terminal verdict a group that can never reshape again should carry — rather than wrapping
  /// past the terminal to `0`.
  #[must_use]
  pub fn removal_floor(&self, gid: &G) -> u64 {
    let mut ceiling = self.group_gen(gid);
    if let Some(storage) = self.groups.get(gid) {
      let meta_gen = storage
        .stable
        .snapshot
        .as_ref()
        .map_or(0, |(m, _)| m.shape_gen())
        .max(
          storage
            .stable
            .durable_snapshot
            .as_ref()
            .map_or(0, SnapshotMeta::shape_gen),
        );
      ceiling = ceiling.max(meta_gen);
      for entry in &storage.log.entries {
        let minted = match entry.kind() {
          crate::EntryKind::Split => crate::wire::decode_split_payload(entry.data_bytes())
            .map_or(0, |p| p.parent_gen_after()),
          crate::EntryKind::PrepareMerge => {
            crate::wire::decode_prepare_merge_payload(entry.data_bytes())
              .map_or(0, |p| p.source_gen_after())
          }
          crate::EntryKind::CommitMerge => {
            crate::wire::decode_commit_merge_payload(entry.data_bytes())
              .map_or(0, |p| p.target_gen_after())
          }
          crate::EntryKind::RollbackMerge => {
            crate::wire::decode_rollback_merge_payload(entry.data_bytes()).map_or(0, |p| {
              if p.is_unfreeze() {
                p.source_gen_after()
              } else {
                p.target_gen_after()
              }
            })
          }
          _ => 0,
        };
        ceiling = ceiling.max(minted);
      }
    }
    if ceiling == 0 {
      0
    } else {
      ceiling.saturating_add(1)
    }
  }
}

impl<G, I> Default for GroupEngine<G, I> {
  fn default() -> Self {
    Self::new()
  }
}

/// Frame-demux resolution for the multi-group coordinators — and the per-crank merge service's
/// store seam: stable and non-aliasing by construction (disjoint map entries); an unknown group
/// is `None` — the unhosted-drop path.
impl<G, I> crate::GroupStores<G, EngineLog, EngineStable<I>> for GroupEngine<G, I>
where
  G: Ord,
{
  fn stores(&mut self, group: &G) -> Option<(&mut EngineLog, &mut EngineStable<I>)> {
    self
      .groups
      .get_mut(group)
      .map(|s| (&mut s.log, &mut s.stable))
  }
}

/// The reference engine IS a [`MultiEngine`]: every method delegates to the inherent one of the
/// same name, which stays public because an embedder holding a concrete engine reaches for it
/// without importing the trait. The engine's own surface is WIDER than the trait — `new`,
/// [`len`](GroupEngine::len), [`is_empty`](GroupEngine::is_empty), and
/// [`contains_group`](GroupEngine::contains_group) are inherent-only, construction because it is
/// engine-specific and the rest because no host consults them.
impl<G, I> crate::MultiEngine<G, I> for GroupEngine<G, I>
where
  G: crate::GroupId,
  I: NodeId,
{
  type Log = EngineLog;
  type Stable = EngineStable<I>;

  fn set_snapshot_staging_cap(&mut self, cap: usize) {
    Self::set_snapshot_staging_cap(self, cap);
  }

  fn group_ids(&self) -> impl Iterator<Item = &G> {
    Self::group_ids(self)
  }

  fn barriers(&self) -> u64 {
    Self::barriers(self)
  }

  fn ops_batched(&self) -> u64 {
    Self::ops_batched(self)
  }

  fn has_staged(&self) -> bool {
    Self::has_staged(self)
  }

  fn flush(&mut self) -> usize {
    Self::flush(self)
  }

  fn add_group(&mut self, gid: G) -> bool {
    Self::add_group(self, gid)
  }

  fn remove_group(&mut self, gid: &G) -> bool {
    Self::remove_group(self, gid)
  }

  fn next_boot_epoch(&mut self, gid: &G) -> Option<u64> {
    Self::next_boot_epoch(self, gid)
  }

  fn set_group_floor(&mut self, gid: &G, floor: u64) {
    Self::set_group_floor(self, gid, floor);
  }

  fn set_group_gen(&mut self, gid: &G, generation: u64) {
    Self::set_group_gen(self, gid, generation);
  }

  fn removal_floor(&self, gid: &G) -> u64 {
    Self::removal_floor(self, gid)
  }
}

/// The per-group log handle of a [`GroupEngine`]: a fully-resident in-memory [`LogStore`] whose
/// completions are STAGED until the engine's barrier ([`GroupEngine::flush`]).
///
/// The read view (`first_index`/`last_index`/`term`/`entries`) reflects `submit_append` (with
/// conflict-suffix truncation), `compact`, and `restore` IMMEDIATELY — ahead of durability, as
/// the trait requires — while `poll`/`has_pending` see a completion only once a barrier releases
/// it (`has_pending` reports ready-to-poll depth, never staged work). `restore` is fully
/// synchronous and drops every queued completion, staged and released alike: a stale `Appended`
/// for a discarded index would ack entries the log no longer holds.
///
/// Handles are created by [`GroupEngine::add_group`] and borrowed via [`GroupEngine::stores`].
#[derive(Debug)]
pub struct EngineLog {
  entries: Vec<Entry>,
  /// Index before `entries[0]` — the compaction boundary. Starts at [`Index::ZERO`].
  offset: Index,
  /// Term at `offset` (the boundary term retained across compaction and restore).
  compacted_term: Term,
  /// Completions staged by `submit_append`/`compact`, awaiting the engine barrier.
  staged: VecDeque<StagedLog>,
  /// Completions released by a barrier — what `poll`/`has_pending` see.
  ready: VecDeque<LogDone>,
}

impl EngineLog {
  fn new() -> Self {
    Self {
      entries: Vec::new(),
      offset: Index::ZERO,
      compacted_term: Term::ZERO,
      staged: VecDeque::new(),
      ready: VecDeque::new(),
    }
  }

  /// Release every staged completion into the poll FIFO (the barrier), returning how many.
  fn release_staged(&mut self) -> usize {
    let n = self.staged.len();
    self.ready.extend(self.staged.drain(..).map(|s| match s {
      StagedLog::Appended { id, .. } => LogDone::Appended(id),
      StagedLog::Compacted(i) => LogDone::Compacted(i),
    }));
    n
  }

  /// Whether this log holds staged-but-unreleased work (invisible to `has_pending` by contract) —
  /// see [`GroupEngine::has_staged`].
  #[must_use]
  pub fn has_staged(&self) -> bool {
    !self.staged.is_empty()
  }
}

impl LogStore for EngineLog {
  type Error = Infallible;

  fn first_index(&self) -> Index {
    Index::new(self.offset.get() + 1)
  }

  fn last_index(&self) -> Index {
    Index::new(self.offset.get() + self.entries.len() as u64)
  }

  fn term(&self, index: Index) -> Result<Term, Self::Error> {
    if index == self.offset {
      return Ok(self.compacted_term);
    }
    if index < self.offset || index > self.last_index() {
      return Ok(Term::ZERO);
    }
    let pos = (index.get() - self.offset.get() - 1) as usize;
    Ok(self.entries[pos].term())
  }

  fn entries(&self, range: Range<Index>, _max_bytes: u64) -> Result<EntriesRead<'_>, Self::Error> {
    let start = range.start.get();
    let end = range.end.get();
    let offset = self.offset.get();
    let len = self.entries.len() as u64;
    let lo = if start <= offset {
      0usize
    } else {
      (start - offset - 1) as usize
    };
    let hi = if end <= offset {
      0usize
    } else {
      ((end - offset - 1).min(len)) as usize
    };
    let lo = lo.min(self.entries.len());
    let hi = hi.max(lo).min(self.entries.len());
    Ok(EntriesRead::Ready(MaybeOwned::Borrowed(
      &self.entries[lo..hi],
    )))
  }

  fn submit_append(&mut self, id: OpId, entries: &[Entry]) {
    if let Some(first) = entries.first() {
      debug_assert!(
        first.index().get() > self.offset.get(),
        "submit_append below the compaction offset"
      );
      let offset = self.offset.get();
      let fi = first.index().get();
      let from = if fi <= offset + 1 {
        0usize
      } else {
        (fi - offset - 1) as usize
      };
      self.entries.truncate(from);
      // A conflicting append supersedes every staged completion whose extent reaches into the
      // truncated suffix: released, such a completion would claim a durable prefix through an
      // index the barrier no longer makes durable. The survivors' prefix is covered by THIS
      // append's own completion (its extent is at least the truncation point), and the core
      // prunes its records on the same truncation, so a dropped op id never wedges it.
      let fi_idx = first.index();
      self.staged.retain(|s| match s {
        StagedLog::Appended { upto, .. } => *upto < fi_idx,
        StagedLog::Compacted(_) => true,
      });
    }
    self.entries.extend_from_slice(entries);
    let upto = self.last_index();
    self.staged.push_back(StagedLog::Appended { id, upto });
  }

  fn compact(&mut self, up_to: Index) {
    if up_to <= self.offset || self.entries.is_empty() {
      return;
    }
    let last = self.last_index();
    let up_to = if up_to > last { last } else { up_to };
    let boundary_term = self.term(up_to).unwrap_or(Term::ZERO);
    let drain_count = ((up_to.get() - self.offset.get()) as usize).min(self.entries.len());
    self.entries.drain(0..drain_count);
    self.offset = up_to;
    self.compacted_term = boundary_term;
    self.staged.push_back(StagedLog::Compacted(up_to));
  }

  fn restore(&mut self, last_index: Index, last_term: Term) {
    // The whole log is replaced by the snapshot: drop every queued completion (staged AND
    // released) — none may fire for a discarded index — then re-baseline so that
    // first_index() == last_index + 1 and term(last_index) == last_term.
    self.entries.clear();
    self.staged.clear();
    self.ready.clear();
    self.offset = last_index;
    self.compacted_term = last_term;
  }

  fn poll(&mut self) -> Option<Result<LogDone, Self::Error>> {
    self.ready.pop_front().map(Ok)
  }

  fn has_pending(&self) -> bool {
    // Ready-to-poll only: staged (pre-barrier) work has enqueued no completion yet — counting it
    // would make the driver hot-spin on an un-flushed tail.
    !self.ready.is_empty()
  }
}

/// The per-group stable-store handle of a [`GroupEngine`]: durable term/vote/commit metadata plus
/// the snapshot slots, with completions STAGED until the engine's barrier
/// ([`GroupEngine::flush`]).
///
/// This is a strict [`StableStore`]: [`hard_state`](StableStore::hard_state) returns the
/// LAST-DURABLE value — a staged [`submit_write`](StableStore::submit_write) is invisible to it
/// until the barrier — and [`durable_snapshot`](StableStore::durable_snapshot) advances only at
/// the barrier, while [`snapshot`](StableStore::snapshot) is the submit-visible slot. Completions
/// release in submit order, as the trait requires.
///
/// Chunked-snapshot staging (the `accept_snapshot_chunk` trio) is single-slot and independent of
/// the barrier — staging is volatile pre-durability state by contract. Its two capacity bounds
/// part ways: a transfer fragmented past the disjoint-run bound is TRANSIENT — the partial is
/// discarded and a ZERO contiguous watermark restarts the transfer with memory still bounded (an
/// honest sender, one chunk per ack from the contiguous cursor, never fragments) — while a
/// declared `total_len` the buffer cannot hold at all is TERMINAL and surfaces as
/// [`EngineStorageError::StagingUnallocatable`], which the core treats as poison: a store that
/// cannot stage the blob could not hold its durable slot either, and a zero watermark would only
/// re-solicit the same declaration forever.
///
/// Handles are created by [`GroupEngine::add_group`] and borrowed via [`GroupEngine::stores`].
#[derive(Debug)]
pub struct EngineStable<I> {
  /// The submit-visible HardState (what the LAST `submit_write` carried).
  visible: HardState<I>,
  /// The last-durable HardState — what `hard_state()` returns; advances only at the barrier.
  durable: HardState<I>,
  /// The submit-visible snapshot slot (`snapshot()`), set immediately by `submit_snapshot`.
  snapshot: Option<(SnapshotMeta<I>, Bytes)>,
  /// The durable snapshot boundary (`durable_snapshot()`); advances only at the barrier.
  durable_snapshot: Option<SnapshotMeta<I>>,
  /// Completions staged by `submit_write`/`submit_snapshot`, awaiting the engine barrier.
  staged: VecDeque<StableDone>,
  /// Completions released by a barrier — what `poll`/`has_pending` see.
  ready: VecDeque<StableDone>,
  staging: Option<(SnapshotMeta<I>, SnapshotStaging)>,
  /// The staging byte cap this group inherited from the engine (see
  /// [`GroupEngine::set_snapshot_staging_cap`]).
  staging_cap: usize,
}

impl<I> EngineStable<I> {
  fn new(staging_cap: usize) -> Self {
    Self {
      visible: HardState::initial(),
      durable: HardState::initial(),
      snapshot: None,
      durable_snapshot: None,
      staged: VecDeque::new(),
      ready: VecDeque::new(),
      staging: None,
      staging_cap,
    }
  }

  /// Release every staged completion into the poll FIFO (the barrier), returning how many. The
  /// visible slots become the durable values at the same point the completions become pollable —
  /// the two observations of one durability event.
  fn release_staged(&mut self) -> usize
  where
    I: Clone,
  {
    // Every visible-slot move stages a completion, so an empty staging queue means the visible
    // and durable slots already agree.
    if self.staged.is_empty() {
      return 0;
    }
    self.durable = self.visible.clone();
    self.durable_snapshot = self.snapshot.as_ref().map(|(m, _)| m.clone());
    let n = self.staged.len();
    self.ready.append(&mut self.staged);
    n
  }

  /// Whether this store holds staged-but-unreleased work (invisible to `has_pending` by contract)
  /// — see [`GroupEngine::has_staged`].
  #[must_use]
  pub fn has_staged(&self) -> bool {
    !self.staged.is_empty()
  }
}

impl<I: NodeId> StableStore for EngineStable<I> {
  type NodeId = I;
  type Error = EngineStorageError;

  fn hard_state(&self) -> HardState<I> {
    self.durable.clone()
  }

  fn submit_write(&mut self, id: OpId, hard_state: HardState<I>) {
    self.visible = hard_state;
    self.staged.push_back(StableDone::Wrote(id));
  }

  fn submit_snapshot(&mut self, id: OpId, meta: SnapshotMeta<I>, data: Bytes) {
    self.snapshot = Some((meta, data));
    self.staged.push_back(StableDone::SnapshotWritten(id));
  }

  fn snapshot(&self) -> Option<(SnapshotMeta<I>, Bytes)> {
    self.snapshot.clone()
  }

  #[allow(clippy::type_complexity)]
  fn snapshot_chunk(
    &self,
    offset: u64,
    len: u64,
  ) -> Option<Result<(SnapshotMeta<I>, u64, SnapshotChunkRead), Self::Error>> {
    // Fully resident: the blob is `Bytes`, so the default slice is O(1) and never `Pending`.
    self.resident_snapshot_chunk(offset, len)
  }

  fn durable_snapshot(&self) -> Option<SnapshotMeta<I>> {
    self.durable_snapshot.clone()
  }

  fn accept_snapshot_chunk(
    &mut self,
    meta: &SnapshotMeta<I>,
    total_len: u64,
    offset: u64,
    data: &Bytes,
  ) -> Result<u64, Self::Error> {
    let boundary = meta.last_index();
    match &self.staging {
      Some((m, _)) if m.last_index() > boundary => return Ok(0),
      Some((m, s)) if !m.identity_eq(meta) || s.total_len() != total_len => self.staging = None,
      _ => {}
    }
    if self.staging.is_none() {
      // Over the cap, or beyond what the allocator grants: TERMINAL, not transient — a zero
      // watermark would re-solicit the same unstageable declaration forever, while the
      // fragmentation discard below stays a self-correcting restart.
      match SnapshotStaging::new(boundary, total_len, self.staging_cap) {
        Some(s) => self.staging = Some((meta.clone(), s)),
        None => return Err(EngineStorageError::StagingUnallocatable { total_len }),
      }
    }
    let contiguous = self
      .staging
      .as_mut()
      .expect("staging installed above")
      .1
      .accept(offset, data);
    match contiguous {
      Some(c) => Ok(c),
      None => {
        // The disjoint-run cap tripped (an adversarially fragmented transfer): drop the partial
        // so the interval metadata stays bounded; the zero watermark restarts the transfer.
        self.staging = None;
        Ok(0)
      }
    }
  }

  fn take_staged_snapshot(&mut self, meta: &SnapshotMeta<I>) -> Option<Bytes> {
    let complete = matches!(
      &self.staging,
      Some((m, s)) if m.identity_eq(meta) && s.is_complete()
    );
    complete.then(|| {
      let (_, s) = self.staging.take().expect("checked complete above");
      Bytes::from(s.into_vec())
    })
  }

  fn discard_snapshot_staging(&mut self) {
    self.staging = None;
  }

  fn poll(&mut self) -> Option<Result<StableDone, Self::Error>> {
    self.ready.pop_front().map(Ok)
  }

  fn has_pending(&self) -> bool {
    // Ready-to-poll only, exactly as on `EngineLog`: staged (pre-barrier) work is invisible.
    !self.ready.is_empty()
  }
}

#[cfg(test)]
mod tests;
