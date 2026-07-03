//! A shared in-memory group-storage engine with ONE batched durability barrier.
//!
//! [`GroupEngine`] hosts every co-located Raft group's log and stable state and lends each group a
//! `(log, stable)` handle pair ([`EngineLog`], [`EngineStable`]) implementing the ordinary
//! single-group [`LogStore`]/[`StableStore`] contracts, with one engine-wide difference: a
//! `submit_*`/`compact` STAGES its completion instead of enqueueing it, and only
//! [`GroupEngine::flush`] — the durability barrier — releases every group's staged completions
//! into that group's poll FIFO. One flush is one fsync-equivalent covering ALL groups' staged
//! writes: the cross-group fsync amortization the multi-Raft design exists for. This is the
//! Sans-I/O reference implementation of the Phase-2 storage contract in `MULTI_RAFT.md`; a disk
//! engine mirrors these semantics over group-prefixed keys and a real write batch.
//!
//! The per-group storage invariants hold BY CONSTRUCTION under a barrier: log durability is
//! prefix-ordered because everything staged becomes durable together, and stable completions
//! release in submit order because each group's staging is a FIFO.

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

/// Byte cap on one group's chunked-snapshot staging buffer: bounds a forged `total_len` without
/// allocating (the fallible [`SnapshotStaging::new`] refuses anything larger, and the engine
/// reports a zero watermark instead of erroring — see the capacity note on [`EngineStable`]).
const MAX_SNAPSHOT_STAGING_BYTES: usize = 1 << 30;

/// One group's hosted storage. `log` and `stable` are separate fields so [`GroupEngine::stores`]
/// can lend both mutably at once; `boot_epochs` backs [`GroupEngine::next_boot_epoch`].
#[derive(Debug)]
struct GroupStorage<I> {
  log: EngineLog,
  stable: EngineStable<I>,
  boot_epochs: u64,
}

impl<I> GroupStorage<I> {
  fn new() -> Self {
    Self {
      log: EngineLog::new(),
      stable: EngineStable::new(),
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

/// A shared in-memory storage engine: ONE engine hosts EVERY co-located group's replicated log
/// and stable state, keyed by group id, with a single batched durability barrier
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
  flushes: u64,
  ops_flushed: u64,
}

// No `G` bound: nothing here keys the map. `flush` alone needs `I: Clone` (folding the visible
// stable slots into the durable ones), carried on the method rather than the block.
impl<G, I> GroupEngine<G, I> {
  /// An empty engine hosting no groups.
  #[must_use]
  pub fn new() -> Self {
    Self {
      groups: BTreeMap::new(),
      flushes: 0,
      ops_flushed: 0,
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
  /// released nothing. Together with [`ops_flushed`](Self::ops_flushed) this is the amortization
  /// metric: operations per flush is the cross-group batch factor.
  #[must_use]
  pub const fn flushes(&self) -> u64 {
    self.flushes
  }

  /// Total operations completed across every [`flush`](Self::flush) so far.
  #[must_use]
  pub const fn ops_flushed(&self) -> u64 {
    self.ops_flushed
  }

  /// THE durability barrier: make every group's staged log appends/compactions and stable
  /// writes/snapshots durable at once, releasing their completions into each owning group's poll
  /// FIFO. Returns the number of operations completed across all groups.
  ///
  /// One flush is one fsync-equivalent covering EVERY hosted group's staged writes — the
  /// cross-group batching a multi-Raft host exists for (a disk engine renders it as one write
  /// batch + one fsync over group-prefixed keys). The per-group contracts hold trivially under a
  /// barrier: log durability is prefix-ordered (everything staged becomes durable together), and
  /// stable completions release in submit order (each group's staging is a FIFO). Log completions
  /// release in submit order too — the trait permits any order; submit order is the simplest
  /// conforming one.
  pub fn flush(&mut self) -> usize
  where
    I: Clone,
  {
    let mut released = 0;
    for storage in self.groups.values_mut() {
      released += storage.log.release_staged();
      released += storage.stable.release_staged();
    }
    self.flushes += 1;
    self.ops_flushed += released as u64;
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
        v.insert(GroupStorage::new());
        true
      }
    }
  }

  /// Drop `gid`'s storage — log, stable state, staged and released completions, and its
  /// boot-epoch counter (the Phase-5 teardown seam). Returns `false` if no such group.
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
  /// the completion plumbing relies on). `None` if no such group.
  #[must_use = "`None` means no such group; the returned epoch is the restore_group argument"]
  pub fn next_boot_epoch(&mut self, gid: &G) -> Option<u64> {
    let storage = self.groups.get_mut(gid)?;
    storage.boot_epochs += 1;
    Some(storage.boot_epochs)
  }
}

impl<G, I> Default for GroupEngine<G, I> {
  fn default() -> Self {
    Self::new()
  }
}

/// Frame-demux resolution for the multi-group coordinators: stable and non-aliasing by
/// construction (disjoint map entries); an unknown group is `None` — the unhosted-drop path.
#[cfg(feature = "tcp")]
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
/// the barrier — staging is volatile pre-durability state by contract. Capacity exhaustion (a
/// `total_len` beyond the staging byte cap, or a transfer fragmented past the disjoint-run bound)
/// cannot surface as `Err` here (`Error` is [`Infallible`]); the engine instead discards the
/// partial and reports a ZERO contiguous watermark, restarting the transfer with memory still
/// bounded. An honest sender (one chunk per ack, resuming from the contiguous cursor) never
/// approaches either bound.
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
}

impl<I> EngineStable<I> {
  fn new() -> Self {
    Self {
      visible: HardState::initial(),
      durable: HardState::initial(),
      snapshot: None,
      durable_snapshot: None,
      staged: VecDeque::new(),
      ready: VecDeque::new(),
      staging: None,
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
}

impl<I: NodeId> StableStore for EngineStable<I> {
  type NodeId = I;
  type Error = Infallible;

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
      // The cap bounds a forged `total_len` WITHOUT allocating; refusal installs no staging and
      // reports a zero watermark — see the capacity note on the type.
      match SnapshotStaging::new(boundary, total_len, MAX_SNAPSHOT_STAGING_BYTES) {
        Some(s) => self.staging = Some((meta.clone(), s)),
        None => return Ok(0),
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
