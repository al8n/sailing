//! Owned reference stores with a real durability split, and the reference computation of the
//! `durable_index` clamp.
//!
//! These exist for two reasons. They are the SIMPLEST CONFORMING implementations — an author can
//! diff an idea against them — and they are OWNED, so the kit can build one, wrap it, and break the
//! wrapper on purpose to prove a check has teeth.

use bytes::Bytes;
use sailing_proto::{
  EntriesRead, Entry, HardState, Index, LogDone, LogStore, MaybeOwned, OpId, SnapshotChunkRead,
  SnapshotMeta, SnapshotStaging, StableDone, StableStore, Term,
};
use std::{collections::VecDeque, vec::Vec};

/// A fault terminal for a reference store: the declared blob does not fit the staging buffer.
///
/// Terminal rather than transient, on the reference engine's reasoning: a store that cannot stage
/// the blob could not hold its durable slot either, and a zero watermark would re-solicit the same
/// unstageable declaration forever.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StagingUnallocatable {
  /// The transfer's declared blob length.
  pub total_len: u64,
}

impl core::fmt::Display for StagingUnallocatable {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(
      f,
      "snapshot staging cannot hold the declared blob ({} bytes)",
      self.total_len
    )
  }
}

impl core::error::Error for StagingUnallocatable {}

/// A completion held behind the barrier. An append records its EXTENT so a pre-barrier conflict
/// truncation can invalidate it: releasing a completion whose extent was truncated away would claim
/// a durable prefix through an index the log no longer holds.
#[derive(Debug)]
enum Staged {
  Appended { id: OpId, upto: Index },
  Compacted(Index),
}

/// An in-memory [`LogStore`] with an explicit barrier and a CONFORMING
/// [`durable_index`](LogStore::durable_index).
///
/// The probe is the point. `durable_index` is not the store's physical durable tip: it is the
/// highest `N` whose entire VISIBLE prefix `[first_index, N]` is durable, so it CAPS wherever a
/// staged rewrite has made the durable bytes disagree with the visible content. Over-answering
/// manufactures a phantom durable replica — the core acks a match a crash would erase, the leader
/// counts it toward quorum, and a commit lands that no quorum durably holds. Under-answering is
/// always safe: the core folds the probe with `max`.
#[derive(Debug, Default)]
pub struct ProbingLog {
  entries: Vec<Entry>,
  offset: Index,
  boundary_term: Term,
  staged: VecDeque<Staged>,
  ready: VecDeque<LogDone>,
  /// The clamped probe answer: the highest index whose whole visible prefix a barrier has made
  /// durable. Advanced by the barrier, capped by every visible rewrite.
  durable_prefix: Index,
}

impl ProbingLog {
  /// An empty log.
  #[must_use]
  pub fn new() -> Self {
    Self::default()
  }

  /// Make every staged write durable and release its completions.
  pub fn barrier(&mut self) -> usize {
    let n = self.staged.len();
    while let Some(staged) = self.staged.pop_front() {
      match staged {
        Staged::Appended { id, upto } => {
          // The barrier made the whole visible content durable, so the prefix through this
          // append's extent now holds — the ONLY event that may advance the probe.
          self.durable_prefix = self.durable_prefix.max(upto);
          self.ready.push_back(LogDone::Appended(id));
        }
        Staged::Compacted(index) => self.ready.push_back(LogDone::Compacted(index)),
      }
    }
    n
  }

  /// Fold everything REPLAYED from the medium into durable state, manufacturing no completion for
  /// any of it.
  ///
  /// A reopened store owes acknowledgments to nobody: the process that submitted this work is gone,
  /// and every op id it minted died with it. Leaving replay-generated completions in the poll queue
  /// hands the NEW incarnation acknowledgments for writes it never issued — the store-side twin of
  /// the stale-delivery fault, and one a boot epoch cannot fence because the ids were never
  /// current.
  pub fn settle_replayed(&mut self) {
    for staged in self.staged.drain(..) {
      if let Staged::Appended { upto, .. } = staged {
        self.durable_prefix = self.durable_prefix.max(upto);
      }
    }
    self.ready.clear();
  }

  /// Whether the log holds staged, unreleased work.
  #[must_use]
  pub fn has_staged(&self) -> bool {
    !self.staged.is_empty()
  }

  /// The entries currently in view — what a reopen must reproduce for a barrier-covered log.
  #[must_use]
  pub fn view(&self) -> &[Entry] {
    &self.entries
  }

  /// The compaction boundary and its term.
  #[must_use]
  pub const fn boundary(&self) -> (Index, Term) {
    (self.offset, self.boundary_term)
  }
}

impl LogStore for ProbingLog {
  type Error = core::convert::Infallible;

  fn first_index(&self) -> Index {
    Index::new(self.offset.get() + 1)
  }

  fn last_index(&self) -> Index {
    Index::new(self.offset.get() + self.entries.len() as u64)
  }

  fn durable_index(&self) -> Option<Index> {
    Some(self.durable_prefix)
  }

  fn term(&self, index: Index) -> Result<Term, Self::Error> {
    if index == self.offset {
      return Ok(self.boundary_term);
    }
    if index < self.offset || index > self.last_index() {
      return Ok(Term::ZERO);
    }
    let pos = (index.get() - self.offset.get() - 1) as usize;
    Ok(self.entries[pos].term())
  }

  fn entries(
    &self,
    range: core::ops::Range<Index>,
    _max_bytes: u64,
  ) -> Result<EntriesRead<'_>, Self::Error> {
    let offset = self.offset.get();
    let lo = range.start.get().saturating_sub(offset + 1) as usize;
    let hi = range.end.get().saturating_sub(offset + 1) as usize;
    let lo = lo.min(self.entries.len());
    let hi = hi.max(lo).min(self.entries.len());
    Ok(EntriesRead::Ready(MaybeOwned::Borrowed(
      &self.entries[lo..hi],
    )))
  }

  fn submit_append(&mut self, id: OpId, entries: &[Entry]) {
    if let Some(first) = entries.first() {
      let from = first.index().get().saturating_sub(self.offset.get() + 1) as usize;
      if from < self.entries.len() {
        self.entries.truncate(from);
        // THE CLAMP. Everything at and above `first` is now content no barrier has made durable,
        // whatever bytes the medium still holds there.
        self.durable_prefix = self
          .durable_prefix
          .min(Index::new(first.index().get().saturating_sub(1)));
        // A staged completion whose extent reached into the truncated suffix must never fire.
        let cut = first.index();
        self.staged.retain(|s| match s {
          Staged::Appended { upto, .. } => *upto < cut,
          Staged::Compacted(_) => true,
        });
      }
      self.entries.extend_from_slice(entries);
    }
    let upto = self.last_index();
    self.staged.push_back(Staged::Appended { id, upto });
  }

  fn compact(&mut self, up_to: Index) {
    if up_to <= self.offset || self.entries.is_empty() {
      return;
    }
    let up_to = up_to.min(self.last_index());
    let boundary_term = self.term(up_to).unwrap_or(Term::ZERO);
    let drain = ((up_to.get() - self.offset.get()) as usize).min(self.entries.len());
    self.entries.drain(0..drain);
    self.offset = up_to;
    self.boundary_term = boundary_term;
    self.staged.push_back(Staged::Compacted(up_to));
  }

  fn restore(&mut self, last_index: Index, last_term: Term) {
    self.entries.clear();
    self.staged.clear();
    self.ready.clear();
    self.offset = last_index;
    self.boundary_term = last_term;
    // Nothing of the new baseline is durable until the snapshot behind it is, so the probe drops
    // to the floor rather than following the re-baselined view.
    self.durable_prefix = Index::ZERO;
  }

  fn poll(&mut self) -> Option<Result<LogDone, Self::Error>> {
    self.ready.pop_front().map(Ok)
  }

  fn has_pending(&self) -> bool {
    !self.ready.is_empty()
  }
}

/// An in-memory [`StableStore`] with an explicit barrier and a conforming durable/visible split on
/// both the hard state and the snapshot slot.
#[derive(Debug)]
pub struct ProbingStable {
  visible_hard_state: HardState<u64>,
  durable_hard_state: HardState<u64>,
  visible_snapshot: Option<(SnapshotMeta<u64>, Bytes)>,
  durable_snapshot: Option<SnapshotMeta<u64>>,
  staged: VecDeque<StableDone>,
  ready: VecDeque<StableDone>,
  staging: Option<(SnapshotMeta<u64>, SnapshotStaging)>,
  /// The declared size above which a transfer is refused rather than staged.
  staging_cap: usize,
}

impl Default for ProbingStable {
  fn default() -> Self {
    Self::new()
  }
}

impl ProbingStable {
  /// An empty store.
  #[must_use]
  pub fn new() -> Self {
    Self {
      visible_hard_state: HardState::initial(),
      durable_hard_state: HardState::initial(),
      visible_snapshot: None,
      durable_snapshot: None,
      staged: VecDeque::new(),
      ready: VecDeque::new(),
      staging: None,
      staging_cap: usize::MAX,
    }
  }

  /// Bound this store's chunked-snapshot staging buffer, in bytes.
  pub const fn set_staging_cap(&mut self, cap: usize) {
    self.staging_cap = cap;
  }

  /// Make every staged write durable and release its completions. The visible slots become the
  /// durable ones at exactly the point the completions become pollable — one durability event, two
  /// observations of it.
  pub fn barrier(&mut self) -> usize {
    if self.staged.is_empty() {
      return 0;
    }
    self.durable_hard_state = self.visible_hard_state.clone();
    self.durable_snapshot = self.visible_snapshot.as_ref().map(|(m, _)| m.clone());
    let n = self.staged.len();
    self.ready.append(&mut self.staged);
    n
  }

  /// Fold replayed state into the durable slots without manufacturing a completion — see
  /// [`ProbingLog::settle_replayed`].
  pub fn settle_replayed(&mut self) {
    self.durable_hard_state = self.visible_hard_state.clone();
    self.durable_snapshot = self.visible_snapshot.as_ref().map(|(m, _)| m.clone());
    self.staged.clear();
    self.ready.clear();
  }

  /// Whether the store holds staged, unreleased work.
  #[must_use]
  pub fn has_staged(&self) -> bool {
    !self.staged.is_empty()
  }

  /// The blob the durable snapshot slot names, if any — what a reopen must reproduce.
  #[must_use]
  pub fn durable_blob(&self) -> Option<Bytes> {
    let durable = self.durable_snapshot.as_ref()?;
    self
      .visible_snapshot
      .as_ref()
      .filter(|(m, _)| m.identity_eq(durable))
      .map(|(_, b)| b.clone())
  }
}

impl StableStore for ProbingStable {
  type NodeId = u64;
  type Error = StagingUnallocatable;

  fn hard_state(&self) -> HardState<u64> {
    self.durable_hard_state.clone()
  }

  fn durable_hard_state(&self) -> Option<HardState<u64>> {
    Some(self.durable_hard_state.clone())
  }

  fn submit_write(&mut self, id: OpId, hard_state: HardState<u64>) {
    self.visible_hard_state = hard_state;
    self.staged.push_back(StableDone::Wrote(id));
  }

  fn submit_snapshot(&mut self, id: OpId, meta: SnapshotMeta<u64>, data: Bytes) {
    self.visible_snapshot = Some((meta, data));
    self.staged.push_back(StableDone::SnapshotWritten(id));
  }

  fn snapshot(&self) -> Option<(SnapshotMeta<u64>, Bytes)> {
    self.visible_snapshot.clone()
  }

  fn durable_snapshot(&self) -> Option<SnapshotMeta<u64>> {
    self.durable_snapshot.clone()
  }

  fn snapshot_chunk(
    &self,
    offset: u64,
    len: u64,
  ) -> Option<Result<(SnapshotMeta<u64>, u64, SnapshotChunkRead), Self::Error>> {
    self.resident_snapshot_chunk(offset, len)
  }

  fn accept_snapshot_chunk(
    &mut self,
    meta: &SnapshotMeta<u64>,
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
      match SnapshotStaging::new(boundary, total_len, self.staging_cap) {
        Some(s) => self.staging = Some((meta.clone(), s)),
        None => return Err(StagingUnallocatable { total_len }),
      }
    }
    let staging = self.staging.as_mut().expect("installed directly above");
    match staging.1.accept(offset, data) {
      Some(contiguous) => Ok(contiguous),
      None => {
        // The disjoint-run cap tripped: drop the partial so the interval metadata stays bounded;
        // the zero watermark restarts the transfer.
        self.staging = None;
        Ok(0)
      }
    }
  }

  fn take_staged_snapshot(&mut self, meta: &SnapshotMeta<u64>) -> Option<Bytes> {
    let complete = matches!(&self.staging, Some((m, s)) if m.identity_eq(meta) && s.is_complete());
    complete.then(|| {
      let (_, staging) = self.staging.take().expect("checked complete above");
      Bytes::from(staging.into_vec())
    })
  }

  fn discard_snapshot_staging(&mut self) {
    self.staging = None;
  }

  fn poll(&mut self) -> Option<Result<StableDone, Self::Error>> {
    self.ready.pop_front().map(Ok)
  }

  fn has_pending(&self) -> bool {
    !self.ready.is_empty()
  }
}

/// A [`LogSubject`](crate::check::LogSubject) over [`ProbingLog`].
#[derive(Debug, Default)]
pub struct ProbingLogSubject {
  log: ProbingLog,
}

impl crate::check::LogSubject for ProbingLogSubject {
  type Log = ProbingLog;

  fn log(&mut self) -> &mut Self::Log {
    &mut self.log
  }

  fn barrier(&mut self) {
    self.log.barrier();
  }
}

/// A [`StableSubject`](crate::check::StableSubject) over [`ProbingStable`].
#[derive(Debug, Default)]
pub struct ProbingStableSubject {
  stable: ProbingStable,
}

impl crate::check::StableSubject for ProbingStableSubject {
  type Stable = ProbingStable;

  fn stable(&mut self) -> &mut Self::Stable {
    &mut self.stable
  }

  fn barrier(&mut self) {
    self.stable.barrier();
  }

  fn node_id(&self, n: u64) -> u64 {
    n
  }
}
