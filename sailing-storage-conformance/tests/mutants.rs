//! Six deliberately broken implementations, one per failure the kit exists to catch.
//!
//! A suite that has never rejected anything is a suite nobody can trust. Each test here breaks ONE
//! thing — a missing fsync, an over-answering probe, a lineage fold left out of the barrier, a
//! codec that drops a field, a durable reader wired to the visible slot, a journal that frames
//! per operation instead of per barrier — and names the check that must catch it. Every one of
//! these is a mistake a real storage engine has made.

use bytes::Bytes;
use sailing_proto::{
  ConfState, EntriesRead, Entry, FloorStore, GroupEngine, GroupStores, HardState, Index, LogDone,
  LogStore, MultiEngine, OpId, SnapshotChunkRead, SnapshotMeta, StableDone, StableStore, Term,
};
use sailing_storage_conformance::{
  check::{self, Durability, EngineSubject, LogSubject, StableSubject},
  fault::{
    CompletionFaults, CrashClass, JournalEngineSubject, ProbingLog, ProbingStable, ReferenceCodec,
    StagingUnallocatable,
  },
};
use std::collections::BTreeMap;

// ---------------------------------------------------------------------------------------------
// Mutant (i): no fsync.
// ---------------------------------------------------------------------------------------------

/// The engine writes its barrier record and calls sync — but the sync reaches nothing. Everything
/// looks identical until the medium loses its unsynced writes, at which point state two barriers
/// covered is simply gone.
#[test]
fn mutant_no_fsync_fails_the_unsynced_loss_class() {
  let report = check::engine(&mut JournalEngineSubject::never_syncing());
  assert!(
    report.failed("engine/exactly-flush-covered-state-survives"),
    "a store whose sync does not reach the medium must fail the one crash class that proves \
     fsync; the report was {:?}",
    report.violations()
  );
}

// ---------------------------------------------------------------------------------------------
// Mutant (ii): durable_index answers the physical tip past a staged truncation.
// ---------------------------------------------------------------------------------------------

/// A log whose durability probe reports its PHYSICAL durable tip rather than the highest index
/// whose visible prefix is durable. Correct until a conflicting append rewrites content the medium
/// still holds the old bytes for — and then it manufactures a phantom durable replica.
#[derive(Debug, Default)]
struct PhysicalTipLog(ProbingLog);

impl LogStore for PhysicalTipLog {
  type Error = core::convert::Infallible;

  fn first_index(&self) -> Index {
    self.0.first_index()
  }

  fn last_index(&self) -> Index {
    self.0.last_index()
  }

  fn durable_index(&self) -> Option<Index> {
    // THE MUTATION: the visible tip, with no clamp at the last index where the durable bytes and
    // the visible content still agree.
    Some(self.0.last_index())
  }

  fn term(&self, index: Index) -> Result<Term, Self::Error> {
    self.0.term(index)
  }

  fn entries(
    &self,
    range: core::ops::Range<Index>,
    max_bytes: u64,
  ) -> Result<EntriesRead<'_>, Self::Error> {
    self.0.entries(range, max_bytes)
  }

  fn submit_append(&mut self, id: OpId, entries: &[Entry]) {
    self.0.submit_append(id, entries);
  }

  fn compact(&mut self, up_to: Index) {
    self.0.compact(up_to);
  }

  fn restore(&mut self, last_index: Index, last_term: Term) {
    self.0.restore(last_index, last_term);
  }

  fn poll(&mut self) -> Option<Result<LogDone, Self::Error>> {
    self.0.poll()
  }

  fn has_pending(&self) -> bool {
    self.0.has_pending()
  }
}

#[derive(Debug, Default)]
struct PhysicalTipSubject(PhysicalTipLog);

impl LogSubject for PhysicalTipSubject {
  type Log = PhysicalTipLog;

  fn log(&mut self) -> &mut Self::Log {
    &mut self.0
  }

  fn barrier(&mut self) {
    self.0.0.barrier();
  }
}

#[test]
fn mutant_durable_index_answering_the_physical_tip_fails_the_clamp() {
  let report = check::log_store(&mut PhysicalTipSubject::default());
  assert!(
    report.failed("log/durable-index-clamp"),
    "a probe that answers past a staged truncation must be caught; the report was {:?}",
    report.violations()
  );
}

// ---------------------------------------------------------------------------------------------
// Mutant (iii): the lineage fold is dropped from the flush barrier.
// ---------------------------------------------------------------------------------------------

/// An engine whose barrier persists the DATA but leaves the lineage records out of the write
/// batch, clearing the staging anyway. Pre-barrier reads are perfect — the freshest read still
/// answers — and the fence evaporates at the very moment it was supposed to become durable.
#[derive(Debug, Default)]
struct DroppedLineageFold {
  inner: GroupEngine<u64, u64>,
  staged: BTreeMap<u64, (u64, u64)>,
}

impl FloorStore<u64> for DroppedLineageFold {
  fn floor(&self, gid: &u64) -> u64 {
    FloorStore::floor(&self.inner, gid).max(self.staged.get(gid).map_or(0, |r| r.0))
  }

  fn lineage(&self, gid: &u64) -> u64 {
    FloorStore::lineage(&self.inner, gid).max(self.staged.get(gid).map_or(0, |r| r.1))
  }
}

impl GroupStores<u64, sailing_proto::EngineLog, sailing_proto::EngineStable<u64>>
  for DroppedLineageFold
{
  fn stores(
    &mut self,
    group: &u64,
  ) -> Option<(
    &mut sailing_proto::EngineLog,
    &mut sailing_proto::EngineStable<u64>,
  )> {
    GroupStores::stores(&mut self.inner, group)
  }
}

impl MultiEngine<u64, u64> for DroppedLineageFold {
  type Log = sailing_proto::EngineLog;
  type Stable = sailing_proto::EngineStable<u64>;

  fn set_snapshot_staging_cap(&mut self, cap: usize) {
    self.inner.set_snapshot_staging_cap(cap);
  }

  fn group_ids(&self) -> impl Iterator<Item = &u64> {
    self.inner.group_ids()
  }

  fn barriers(&self) -> u64 {
    self.inner.barriers()
  }

  fn ops_batched(&self) -> u64 {
    self.inner.ops_batched()
  }

  fn has_staged(&self) -> bool {
    self.inner.has_staged() || !self.staged.is_empty()
  }

  fn flush(&mut self) -> usize {
    // THE MUTATION: the staging is cleared without ever reaching the durable record.
    let dropped = self.staged.len();
    self.staged.clear();
    self.inner.flush() + dropped
  }

  fn add_group(&mut self, gid: u64) -> bool {
    self.inner.add_group(gid)
  }

  fn remove_group(&mut self, gid: &u64) -> bool {
    self.inner.remove_group(gid)
  }

  fn contains_group(&self, gid: &u64) -> bool {
    self.inner.contains_group(gid)
  }

  fn next_boot_epoch(&mut self, gid: &u64) -> Option<u64> {
    self.inner.next_boot_epoch(gid)
  }

  fn set_group_floor(&mut self, gid: &u64, floor: u64) {
    if floor == 0 {
      return;
    }
    let record = self.staged.entry(*gid).or_default();
    record.0 = record.0.max(floor);
  }

  fn set_group_gen(&mut self, gid: &u64, generation: u64) {
    if generation == 0 {
      return;
    }
    let record = self.staged.entry(*gid).or_default();
    record.1 = record.1.max(generation);
  }

  fn removal_floor(&self, gid: &u64) -> u64 {
    self.inner.removal_floor(gid)
  }
}

#[derive(Debug, Default)]
struct DroppedLineageFoldSubject;

impl EngineSubject for DroppedLineageFoldSubject {
  type Group = u64;
  type NodeId = u64;
  type Engine = DroppedLineageFold;

  fn durability(&self) -> Durability {
    Durability::Volatile
  }

  fn open(&mut self) -> Self::Engine {
    DroppedLineageFold::default()
  }

  fn crash(&mut self, engine: Self::Engine, _class: CrashClass) -> Self::Engine {
    drop(engine);
    DroppedLineageFold::default()
  }

  fn group(&self, n: u64) -> u64 {
    n
  }

  fn node(&self, n: u64) -> u64 {
    n
  }
}

#[test]
fn mutant_lineage_fold_dropped_from_the_barrier_fails_the_engine_suite() {
  let report = check::engine(&mut DroppedLineageFoldSubject);
  assert!(
    report.failed("engine/lineage-fold-rides-the-data-barrier"),
    "a fence that evaporates at the barrier that was meant to persist it must be caught; the \
     report was {:?}",
    report.violations()
  );
}

// ---------------------------------------------------------------------------------------------
// Mutant (iv): serialization strips shape_gen and fork_id.
// ---------------------------------------------------------------------------------------------

/// A codec that persists the STRUCTURE of each shape and drops the provenance riding it: a meta
/// rebuilt from `(last_index, last_term, conf)`, a hard state rebuilt without its founding
/// generation. Nothing errors: the group simply never installs, the chunked transfer restarts on
/// every chunk, and a recreated incarnation comes back founded at zero.
#[derive(Debug, Default)]
struct LineageStrippingCodec;

impl check::Codec for LineageStrippingCodec {
  type NodeId = u64;

  fn encode_hard_state(&self, hs: &HardState<u64>) -> Vec<u8> {
    // THE MUTATION, hard-state side: the founding generation is not persisted, so a restart
    // recovers a counter beneath the generation the incarnation was admitted at.
    ReferenceCodec::encode_hard_state(&hs.clone().with_founding_gen(0))
  }

  fn decode_hard_state(&self, bytes: &[u8]) -> Option<HardState<u64>> {
    ReferenceCodec::decode_hard_state(bytes).ok()
  }

  fn encode_snapshot_meta(&self, meta: &SnapshotMeta<u64>) -> Vec<u8> {
    // THE MUTATION: re-derive the meta from the boundary alone, losing shape_gen and fork_id.
    let stripped = SnapshotMeta::new(meta.last_index(), meta.last_term(), meta.conf().clone())
      .with_max_lease_window(meta.max_lease_window())
      .with_max_wall_plus_window(meta.max_wall_plus_window())
      .with_max_unwalled_lease_window(meta.max_unwalled_lease_window());
    ReferenceCodec::encode_snapshot_meta(&stripped)
  }

  fn decode_snapshot_meta(&self, bytes: &[u8]) -> Option<SnapshotMeta<u64>> {
    ReferenceCodec::decode_snapshot_meta(bytes).ok()
  }

  fn encode_legacy_hard_state(&self, hs: &HardState<u64>) -> Option<Vec<u8>> {
    Some(ReferenceCodec::encode_legacy_hard_state(hs))
  }

  fn node_id(&self, n: u64) -> u64 {
    n
  }
}

#[test]
fn mutant_serialization_stripping_lineage_and_founding_fields_fails_the_fidelity_suite() {
  let report = check::serialization(&LineageStrippingCodec);
  for check in [
    "serde/meta-shape-gen-verbatim",
    "serde/meta-fork-id-verbatim",
    "serde/founding-gen-verbatim",
  ] {
    assert!(
      report.failed(check),
      "a codec that drops the provenance fields must be caught by {check}; the report was {:?}",
      report.violations()
    );
  }
}

// ---------------------------------------------------------------------------------------------
// Mutant (v): durable_snapshot served from the visible slot.
// ---------------------------------------------------------------------------------------------

/// A store whose durable snapshot reader answers from the SUBMIT-VISIBLE slot. The core
/// re-baselines a log against this answer, so the mutation lets a crash orphan the log.
#[derive(Debug, Default)]
struct VisibleSlotDurableSnapshot(ProbingStable);

impl StableStore for VisibleSlotDurableSnapshot {
  type NodeId = u64;
  type Error = StagingUnallocatable;

  fn hard_state(&self) -> HardState<u64> {
    self.0.hard_state()
  }

  fn durable_hard_state(&self) -> Option<HardState<u64>> {
    self.0.durable_hard_state()
  }

  fn submit_write(&mut self, id: OpId, hard_state: HardState<u64>) {
    self.0.submit_write(id, hard_state);
  }

  fn submit_snapshot(&mut self, id: OpId, meta: SnapshotMeta<u64>, data: Bytes) {
    self.0.submit_snapshot(id, meta, data);
  }

  fn snapshot(&self) -> Option<(SnapshotMeta<u64>, Bytes)> {
    self.0.snapshot()
  }

  fn durable_snapshot(&self) -> Option<SnapshotMeta<u64>> {
    // THE MUTATION: the visible slot, which advances at submit rather than at the barrier.
    self.0.snapshot().map(|(meta, _)| meta)
  }

  fn snapshot_chunk(
    &self,
    offset: u64,
    len: u64,
  ) -> Option<Result<(SnapshotMeta<u64>, u64, SnapshotChunkRead), Self::Error>> {
    self.0.snapshot_chunk(offset, len)
  }

  fn accept_snapshot_chunk(
    &mut self,
    meta: &SnapshotMeta<u64>,
    total_len: u64,
    offset: u64,
    data: &Bytes,
  ) -> Result<u64, Self::Error> {
    self.0.accept_snapshot_chunk(meta, total_len, offset, data)
  }

  fn take_staged_snapshot(&mut self, meta: &SnapshotMeta<u64>) -> Option<Bytes> {
    self.0.take_staged_snapshot(meta)
  }

  fn discard_snapshot_staging(&mut self) {
    self.0.discard_snapshot_staging();
  }

  fn poll(&mut self) -> Option<Result<StableDone, Self::Error>> {
    self.0.poll()
  }

  fn has_pending(&self) -> bool {
    self.0.has_pending()
  }
}

#[derive(Debug, Default)]
struct VisibleSlotSubject(VisibleSlotDurableSnapshot);

impl StableSubject for VisibleSlotSubject {
  type Stable = VisibleSlotDurableSnapshot;

  fn stable(&mut self) -> &mut Self::Stable {
    &mut self.0
  }

  fn barrier(&mut self) {
    self.0.0.barrier();
  }

  fn node_id(&self, n: u64) -> u64 {
    n
  }
}

#[test]
fn mutant_durable_snapshot_from_the_visible_slot_fails_the_stable_suite() {
  let report = check::stable_store(&mut VisibleSlotSubject::default());
  assert!(
    report.failed("stable/durable-snapshot-is-never-the-visible-slot"),
    "a durable reader wired to the visible slot must be caught; the report was {:?}",
    report.violations()
  );
}

// ---------------------------------------------------------------------------------------------
// Mutant (vi): half a barrier survives a torn-tail replay.
// ---------------------------------------------------------------------------------------------

/// A journal that frames one record per OPERATION. Every record is individually well-formed and
/// checksum-valid, so a crash between two operations of one barrier leaves half of it durable and
/// recovery replays it — the state the reopened engine reports was never a state the engine was in.
#[test]
fn mutant_half_a_barrier_surviving_a_torn_tail_fails_the_atomicity_check() {
  let report = check::engine(&mut JournalEngineSubject::framing_per_operation());
  assert!(
    report.failed("engine/barrier-is-all-or-nothing-across-a-crash"),
    "a journal that can replay half a barrier must be caught; the report was {:?}",
    report.violations()
  );
}

// ---------------------------------------------------------------------------------------------
// Mutant (vii): one journal record per GROUP instead of one per barrier.
// ---------------------------------------------------------------------------------------------

/// A barrier spans every group the engine hosts. Framing it per group leaves each group's record
/// individually valid, so a cut between two of them reopens with one group at the new barrier and
/// another at the old — a cross-group state the engine was never in.
#[test]
fn mutant_per_group_framing_fails_the_barrier_atomicity_check() {
  let report = check::engine(&mut JournalEngineSubject::framing_per_group());
  assert!(
    report.failed("engine/barrier-is-all-or-nothing-across-a-crash"),
    "a barrier that is not atomic ACROSS GROUPS must be caught; the report was {:?}",
    report.violations()
  );
}

// ---------------------------------------------------------------------------------------------
// Mutant (viii): the journal keeps the shape and drops the payload.
// ---------------------------------------------------------------------------------------------

/// Entry payloads and snapshot blobs are journalled as empty. Indices, terms, kinds, boundaries and
/// lineage all come back correct, so every shape-level check passes and the engine reopens serving
/// content it does not have.
#[test]
fn mutant_losing_entry_payloads_and_snapshot_blobs_fails_the_image_comparison() {
  let report = check::engine(&mut JournalEngineSubject::losing_payloads());
  assert!(
    report.failed("engine/exactly-the-maximal-valid-prefix-survives"),
    "a reopen that reproduces the shape and loses the bytes must be caught; the report was {:?}",
    report.violations()
  );
}

// ---------------------------------------------------------------------------------------------
// Mutant (ix): recovery discards everything on finding a torn tail.
// ---------------------------------------------------------------------------------------------

/// A torn tail is the ordinary end of every crashed log. Treating it as corruption of the WHOLE log
/// throws away every barrier the engine ever acknowledged — the safe-looking failure that loses all
/// the data.
#[test]
fn mutant_discarding_the_whole_log_on_a_torn_tail_fails_the_maximal_prefix_check() {
  let report = check::engine(&mut JournalEngineSubject::discarding_on_tear());
  assert!(
    report.failed("engine/exactly-the-maximal-valid-prefix-survives")
      && report.failed("engine/barrier-is-all-or-nothing-across-a-crash"),
    "recovery that keeps LESS than the maximal valid prefix must be caught; the report was {:?}",
    report.violations()
  );
}

// ---------------------------------------------------------------------------------------------
// Mutant (x): persistence deferred to the completion drain.
// ---------------------------------------------------------------------------------------------

/// The barrier releases its completions and owes the medium the record until something polls. A
/// crash between the flush returning and the first poll loses a barrier the engine already reported
/// done — and nothing polls at all if the driver crashes first.
#[test]
fn mutant_persisting_at_poll_fails_the_crash_without_drain_leg() {
  let report = check::engine(&mut JournalEngineSubject::persisting_at_poll());
  assert!(
    report.failed("engine/durability-precedes-the-barriers-return"),
    "durability must precede the barrier's RETURN, not its drain; the report was {:?}",
    report.violations()
  );
}

// ---------------------------------------------------------------------------------------------
// Mutant (xi): the superseded append completes anyway.
// ---------------------------------------------------------------------------------------------

/// A log that remembers every append it ever accepted and completes them all, truncated suffix
/// included. The released completion claims a durable prefix through an index the log no longer
/// holds — the phantom durable replica, arriving by the completion path instead of the probe.
#[derive(Debug, Default)]
struct RetainsSupersededLog {
  inner: ProbingLog,
  /// Every append accepted since the last barrier, with the extent it reached.
  staged: Vec<(OpId, Index)>,
  superseded: Vec<OpId>,
  extra: Vec<LogDone>,
}

impl RetainsSupersededLog {
  fn barrier(&mut self) {
    self.inner.barrier();
    self.staged.clear();
    self
      .extra
      .extend(self.superseded.drain(..).map(LogDone::Appended));
  }
}

impl LogStore for RetainsSupersededLog {
  type Error = core::convert::Infallible;

  fn first_index(&self) -> Index {
    self.inner.first_index()
  }

  fn last_index(&self) -> Index {
    self.inner.last_index()
  }

  fn durable_index(&self) -> Option<Index> {
    self.inner.durable_index()
  }

  fn term(&self, index: Index) -> Result<Term, Self::Error> {
    self.inner.term(index)
  }

  fn entries(
    &self,
    range: core::ops::Range<Index>,
    max_bytes: u64,
  ) -> Result<EntriesRead<'_>, Self::Error> {
    self.inner.entries(range, max_bytes)
  }

  fn submit_append(&mut self, id: OpId, entries: &[Entry]) {
    // THE MUTATION: an append whose extent a conflicting one truncates away is REMEMBERED and
    // completed at the barrier, instead of being dropped with the entries it claimed.
    if let Some(first) = entries.first() {
      let cut = first.index();
      let (gone, kept): (Vec<_>, Vec<_>) =
        self.staged.drain(..).partition(|(_, upto)| *upto >= cut);
      self.superseded.extend(gone.into_iter().map(|(op, _)| op));
      self.staged = kept;
    }
    self.inner.submit_append(id, entries);
    let upto = self.inner.last_index();
    self.staged.push((id, upto));
  }

  fn compact(&mut self, up_to: Index) {
    self.inner.compact(up_to);
  }

  fn restore(&mut self, last_index: Index, last_term: Term) {
    self.inner.restore(last_index, last_term);
    self.staged.clear();
    self.superseded.clear();
    self.extra.clear();
  }

  fn poll(&mut self) -> Option<Result<LogDone, Self::Error>> {
    self
      .inner
      .poll()
      .or_else(|| (!self.extra.is_empty()).then(|| Ok(self.extra.remove(0))))
  }

  fn has_pending(&self) -> bool {
    self.inner.has_pending() || !self.extra.is_empty()
  }
}

#[derive(Debug, Default)]
struct RetainsSupersededSubject(RetainsSupersededLog);

impl LogSubject for RetainsSupersededSubject {
  type Log = RetainsSupersededLog;

  fn log(&mut self) -> &mut Self::Log {
    &mut self.0
  }

  fn barrier(&mut self) {
    self.0.barrier();
  }
}

#[test]
fn mutant_retaining_a_superseded_completion_fails_the_supersession_check() {
  let report = check::log_store(&mut RetainsSupersededSubject::default());
  assert!(
    report.failed("log/superseded-append-never-completes"),
    "a completion for a truncated-away append must be caught; the report was {:?}",
    report.violations()
  );
}

// ---------------------------------------------------------------------------------------------
// Mutant (xii): every completion is delivered twice.
// ---------------------------------------------------------------------------------------------

/// A log that hands each `Appended` back twice. The core folds every completion into a durability
/// watermark, so a repeat is not a harmless duplicate — it is a second, unearned advance.
#[derive(Debug, Default)]
struct DuplicatingLog {
  inner: ProbingLog,
  repeat: Option<LogDone>,
}

impl LogStore for DuplicatingLog {
  type Error = core::convert::Infallible;

  fn first_index(&self) -> Index {
    self.inner.first_index()
  }

  fn last_index(&self) -> Index {
    self.inner.last_index()
  }

  fn durable_index(&self) -> Option<Index> {
    self.inner.durable_index()
  }

  fn term(&self, index: Index) -> Result<Term, Self::Error> {
    self.inner.term(index)
  }

  fn entries(
    &self,
    range: core::ops::Range<Index>,
    max_bytes: u64,
  ) -> Result<EntriesRead<'_>, Self::Error> {
    self.inner.entries(range, max_bytes)
  }

  fn submit_append(&mut self, id: OpId, entries: &[Entry]) {
    self.inner.submit_append(id, entries);
  }

  fn compact(&mut self, up_to: Index) {
    self.inner.compact(up_to);
  }

  fn restore(&mut self, last_index: Index, last_term: Term) {
    self.inner.restore(last_index, last_term);
    self.repeat = None;
  }

  fn poll(&mut self) -> Option<Result<LogDone, Self::Error>> {
    if let Some(again) = self.repeat.take() {
      return Some(Ok(again));
    }
    match self.inner.poll() {
      // THE MUTATION: hold a copy back and hand it out on the next poll.
      Some(Ok(LogDone::Appended(id))) => {
        self.repeat = Some(LogDone::Appended(id));
        Some(Ok(LogDone::Appended(id)))
      }
      other => other,
    }
  }

  fn has_pending(&self) -> bool {
    self.repeat.is_some() || self.inner.has_pending()
  }
}

#[derive(Debug, Default)]
struct DuplicatingSubject(DuplicatingLog);

impl LogSubject for DuplicatingSubject {
  type Log = DuplicatingLog;

  fn log(&mut self) -> &mut Self::Log {
    &mut self.0
  }

  fn barrier(&mut self) {
    self.0.inner.barrier();
  }
}

#[test]
fn mutant_duplicating_a_completion_fails_the_exactly_once_count() {
  let report = check::log_store(&mut DuplicatingSubject::default());
  assert!(
    report.failed("log/survivor-completes-exactly-once"),
    "a duplicated completion must be caught by a COUNT, which membership would miss; the report \
     was {:?}",
    report.violations()
  );
}

// ---------------------------------------------------------------------------------------------
// Mutant (xiii): persistence keeps the structure and zeroes the self-describing fields.
// ---------------------------------------------------------------------------------------------

/// A store that persists `(index, term, kind, bytes)` and rebuilds everything else from defaults.
/// Indices, terms, kinds, boundaries and lineage records all round-trip, so every structural check
/// passes — and the engine reopens with zeroed lease windows, a promise it never made, and no
/// lineage token. Each of those is silent: an under-sized commit-wait, a post-upgrade restart less
/// safe than the run before it, an install that stalls against its own blob.
#[test]
fn mutant_stripping_self_describing_fields_fails_the_image_comparison() {
  let report = check::engine(&mut JournalEngineSubject::stripping_fields());
  assert!(
    report.failed("engine/exactly-the-maximal-valid-prefix-survives"),
    "a reopen that keeps the shape and drops the self-describing fields must be caught; the \
     report was {:?}",
    report.violations()
  );
}

// ---------------------------------------------------------------------------------------------
// Mutant (xiv): a codec that accepts truncated input.
// ---------------------------------------------------------------------------------------------

/// A codec that answers a DEFAULT-shaped value for input it cannot decode. Complete records still
/// round-trip perfectly, so every fidelity check passes; only a torn blob reveals it, and what it
/// then hands back is a hard state claiming a legacy record, a meta at generation zero, and no
/// lineage token — three claims nobody made.
#[derive(Debug, Default)]
struct TruncationTolerantCodec;

impl check::Codec for TruncationTolerantCodec {
  type NodeId = u64;

  fn encode_hard_state(&self, hs: &HardState<u64>) -> Vec<u8> {
    ReferenceCodec::encode_hard_state(hs)
  }

  fn decode_hard_state(&self, bytes: &[u8]) -> Option<HardState<u64>> {
    // THE MUTATION: a malformed record becomes a value instead of a refusal.
    Some(ReferenceCodec::decode_hard_state(bytes).unwrap_or_else(|_| HardState::initial()))
  }

  fn encode_snapshot_meta(&self, meta: &SnapshotMeta<u64>) -> Vec<u8> {
    ReferenceCodec::encode_snapshot_meta(meta)
  }

  fn decode_snapshot_meta(&self, bytes: &[u8]) -> Option<SnapshotMeta<u64>> {
    Some(
      ReferenceCodec::decode_snapshot_meta(bytes).unwrap_or_else(|_| {
        SnapshotMeta::new(Index::ZERO, Term::ZERO, ConfState::from_voters([1u64]))
      }),
    )
  }

  fn encode_legacy_hard_state(&self, hs: &HardState<u64>) -> Option<Vec<u8>> {
    Some(ReferenceCodec::encode_legacy_hard_state(hs))
  }

  fn node_id(&self, n: u64) -> u64 {
    n
  }
}

#[test]
fn mutant_accepting_truncated_input_fails_the_truncation_sweep() {
  let report = check::serialization(&TruncationTolerantCodec);
  assert!(
    report.failed("serde/truncated-input-never-decodes"),
    "a decoder that builds a value out of a torn record must be caught; the report was {:?}",
    report.violations()
  );
}

// ---------------------------------------------------------------------------------------------
// Mutant (xv): the completion channel that interposes nothing.
// ---------------------------------------------------------------------------------------------

/// The battery's own red-proof: an injector that reports every class and applies none of them.
/// Every named fault class must REJECT it, because each asserts the SIGNATURE of its own fault in
/// the delivered trace. A class that would pass here proves the probe survives a fault that never
/// occurred.
#[derive(Debug, Default)]
struct TransparentInjector;

impl check::CompletionInjector for TransparentInjector {
  fn applied(&self, _requested: CompletionFaults) -> CompletionFaults {
    CompletionFaults::none()
  }
}

#[test]
fn mutant_a_transparent_injector_is_rejected_by_every_fault_class() {
  let log = check::completion_faults_log_with(
    &mut sailing_storage_conformance::fault::ProbingLogSubject::default(),
    &TransparentInjector,
  );
  for check in [
    "completion/reorder-is-observed",
    "completion/duplication-is-observed",
    "completion/loss-is-observed",
    "completion/delay-is-observed",
    "completion/stale-delivery-is-observed",
  ] {
    assert!(
      log.failed(check),
      "{check} must reject a channel that injected nothing; the report was {:?}",
      log.violations()
    );
  }
  let stable = check::completion_faults_stable_with(
    &mut sailing_storage_conformance::fault::ProbingStableSubject::default(),
    &TransparentInjector,
  );
  for check in [
    "completion/reorder-is-observed",
    "completion/duplication-is-observed",
    "completion/loss-is-observed",
    "completion/delay-is-observed",
    "completion/stale-delivery-is-observed",
  ] {
    assert!(
      stable.failed(check),
      "{check} must reject a channel that injected nothing on the stable side too"
    );
  }
}

// ---------------------------------------------------------------------------------------------
// Mutant (xvi): a device that tears without reporting where.
// ---------------------------------------------------------------------------------------------

/// A subject whose medium really is cut, but which never says where its records sit. WHICH
/// barriers survive a cut at a given offset is a fact about the LAYOUT, so without it the suite has
/// no expectation for that — and inventing one ("nothing survived") would record a pass for
/// whichever engines lose everything and a failure for the correct ones.
///
/// What does NOT need the layout is whether a whole barrier survived: a barrier spans every hosted
/// group at once, so the image is one of three complete states or it is none of them. That name is
/// graded here; only the two claims that need the cut's location skip.
#[test]
fn mutant_a_hidden_boundary_skips_only_what_needs_the_layout() {
  let hidden = check::engine(&mut JournalEngineSubject::hiding_its_boundary());
  const DECIDABLE: &str = "engine/barrier-is-all-or-nothing-across-a-crash";
  assert!(
    hidden.passed_check(DECIDABLE),
    "atomicity is decidable from the image alone, so an honest engine is GRADED on it even with \
     no boundary reported; the skips were {:?}",
    hidden.skipped()
  );
  assert!(
    !hidden.skipped().iter().any(|s| s.check == DECIDABLE),
    "and it must not also be reported as skipped"
  );

  // AND THE BROAD NAMES TOO. These two claim something about EVERY crash class, and the clean and
  // unsynced-loss legs settle them long before a torn one is attempted — so a suppressed skip
  // laundered a pass for a property never asked under any torn crash at all, which is the crash
  // class they exist for. Neither may read as covered here, and each must say why.
  for broad in [
    "engine/exactly-the-maximal-valid-prefix-survives",
    "engine/durability-precedes-the-barriers-return",
  ] {
    assert!(
      !hidden.passed_check(broad),
      "{broad} spans every crash class; with the torn legs unaskable it must not read as covered"
    );
    assert!(
      hidden
        .skipped()
        .iter()
        .any(|s| s.check == broad && s.reason.contains("tail_len")),
      "{broad} must be reported as skipped, naming what would make the torn legs run; the skips \
       were {:?}",
      hidden.skipped()
    );
  }
  for name in hidden.passed() {
    assert!(
      !hidden.skipped().iter().any(|s| s.check == *name),
      "{name} is recorded as BOTH passed and skipped"
    );
  }

  // THE TEETH SURVIVED. A subject that DOES report its boundary is still held to the check.
  let known = check::engine(&mut JournalEngineSubject::framing_per_operation());
  assert!(
    known.failed(DECIDABLE),
    "a known-boundary engine that lets half a barrier survive must still fail; the report was {:?}",
    known.violations()
  );
}

// ---------------------------------------------------------------------------------------------
// Mutant (xvii): one group's half of the barrier is abandoned.
// ---------------------------------------------------------------------------------------------

/// An engine that barriers every group but one: the first group's staged work is journalled and
/// released, the other's is neither. Every read on the served group is perfect, so an oracle that
/// drains one group and infers the rest certifies exactly this engine — and the state it publishes
/// is one no single barrier ever produced.
#[test]
fn mutant_abandoning_one_groups_barrier_half_fails_the_cross_group_check() {
  let report = check::engine(&mut JournalEngineSubject::stalling_one_group());
  assert!(
    report.failed("engine/barrier-releases-every-groups-completions"),
    "a barrier that finishes one group's half and abandons another's must be caught; the report \
     was {:?}",
    report.violations()
  );
}

// ---------------------------------------------------------------------------------------------
// Mutant (xviii): the right entries at the right indices, with the wrong content.
// ---------------------------------------------------------------------------------------------

/// A log whose resident reads carry the correct count, the correct indices and the correct
/// boundaries — and stripped payloads with every kind normalised. `first_index`, `last_index` and
/// `term` all agree, so an oracle that checks the shape of a range certifies it. The core replays
/// whatever comes back from `entries`, so what this store hands back is a different log.
#[derive(Debug, Default)]
struct MangledReadLog {
  inner: ProbingLog,
}

impl LogStore for MangledReadLog {
  type Error = core::convert::Infallible;

  fn first_index(&self) -> Index {
    self.inner.first_index()
  }

  fn last_index(&self) -> Index {
    self.inner.last_index()
  }

  fn durable_index(&self) -> Option<Index> {
    self.inner.durable_index()
  }

  fn term(&self, index: Index) -> Result<Term, Self::Error> {
    self.inner.term(index)
  }

  fn entries(
    &self,
    range: core::ops::Range<Index>,
    max_bytes: u64,
  ) -> Result<EntriesRead<'_>, Self::Error> {
    let mangled = match self.inner.entries(range, max_bytes)? {
      // THE MUTATION: the shape is preserved exactly; the content is not.
      EntriesRead::Ready(view) => view
        .iter()
        .map(|e| {
          Entry::new(
            e.term(),
            e.index(),
            sailing_proto::EntryKind::Normal,
            bytes::Bytes::new(),
          )
        })
        .collect::<Vec<_>>(),
      EntriesRead::Pending => return Ok(EntriesRead::Pending),
    };
    Ok(EntriesRead::Ready(sailing_proto::MaybeOwned::Owned(
      mangled.into_boxed_slice(),
    )))
  }

  fn submit_append(&mut self, id: OpId, entries: &[Entry]) {
    self.inner.submit_append(id, entries);
  }

  fn compact(&mut self, up_to: Index) {
    self.inner.compact(up_to);
  }

  fn restore(&mut self, last_index: Index, last_term: Term) {
    self.inner.restore(last_index, last_term);
  }

  fn poll(&mut self) -> Option<Result<LogDone, Self::Error>> {
    self.inner.poll()
  }

  fn has_pending(&self) -> bool {
    self.inner.has_pending()
  }
}

#[derive(Debug, Default)]
struct MangledReadSubject(MangledReadLog);

impl LogSubject for MangledReadSubject {
  type Log = MangledReadLog;

  fn log(&mut self) -> &mut Self::Log {
    &mut self.0
  }

  fn barrier(&mut self) {
    self.0.inner.barrier();
  }
}

#[test]
fn mutant_mangling_resident_reads_fails_the_verbatim_range_check() {
  let report = check::log_store(&mut MangledReadSubject::default());
  assert!(
    report.failed("log/entries-aligned-and-contiguous"),
    "a range read with the right shape and the wrong content must be caught; the report was {:?}",
    report.violations()
  );
}

// ---------------------------------------------------------------------------------------------
// Mutants (xix)-(xx): a completion released ahead of the durability it reports.
// ---------------------------------------------------------------------------------------------

/// Which write class a store releases early, and whether it is telling the truth when it does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EarlyRelease {
  /// A `Wrote` completion is pollable while the durable hard state has not moved.
  PrematureWrote,
  /// A `SnapshotWritten` completion is pollable while the durable slot has not advanced.
  PrematureSnapshotWritten,
  /// HONEST: the hard state really is durable at submit (and says so), while the snapshot stays
  /// staged behind the barrier. A store may be synchronous in one class and asynchronous in the
  /// other, and a faithful suite must accept it.
  MixedSynchronousHardState,
  /// The `Wrote` completion is withheld from the FIRST drain and released on a later pre-barrier
  /// one, still ahead of the durable reader. The same lie as `PrematureWrote`, told one phase late
  /// — invisible to any oracle that classified the store from its first drain alone.
  DelayedPrematureWrote,
  /// Both completions are held to the barrier and then delivered in REVERSE submission order. The
  /// contract makes the completion queue a FIFO in submit order; an unordered membership check
  /// accepts this happily.
  ReversedOrder,
  /// A completion naming an operation the store has not been handed YET — its id and kind match a
  /// write submitted only later, and the real one is then SWALLOWED, so the final sequence is
  /// exactly the two expected completions and membership plus count are both satisfied.
  FutureId,
  /// The snapshot twin, released past ITS classification point: the completion is withheld until
  /// the POST-BARRIER drain — the snapshot class's last look was the second pre-barrier one — while
  /// the durable slot never advances at all.
  DelayedPrematureSnapshotWritten,
}

/// A store that releases one class's completion before the barrier. Under `Premature*` the
/// durable reader has NOT advanced, which is the lie; under `MixedSynchronousHardState` it has.
#[derive(Debug)]
struct EarlyReleaseStable {
  inner: ProbingStable,
  mode: EarlyRelease,
  early: std::collections::VecDeque<StableDone>,
  /// The synchronously-durable hard state, for the honest mixed store.
  settled: Option<HardState<u64>>,
  /// Completions held back until `withhold_drains` drains have ended.
  withheld: std::collections::VecDeque<StableDone>,
  /// How many completed drains must pass before the withheld completions are released — one for
  /// the hard-state class (whose look is the first drain), two for the snapshot class (whose look
  /// is the second).
  withhold_drains: usize,
  /// Drains that have ended so far.
  drains_ended: usize,
  /// The inner queue, reversed, for `ReversedOrder`.
  reversed: std::collections::VecDeque<StableDone>,
}

impl EarlyReleaseStable {
  fn new(mode: EarlyRelease) -> Self {
    Self {
      inner: ProbingStable::new(),
      mode,
      early: std::collections::VecDeque::new(),
      settled: None,
      withheld: std::collections::VecDeque::new(),
      withhold_drains: match mode {
        EarlyRelease::DelayedPrematureSnapshotWritten => 2,
        _ => 1,
      },
      drains_ended: 0,
      reversed: std::collections::VecDeque::new(),
    }
  }
}

impl StableStore for EarlyReleaseStable {
  type NodeId = u64;
  type Error = StagingUnallocatable;

  fn hard_state(&self) -> HardState<u64> {
    self
      .settled
      .clone()
      .unwrap_or_else(|| self.inner.hard_state())
  }

  fn durable_hard_state(&self) -> Option<HardState<u64>> {
    Some(self.hard_state())
  }

  fn submit_write(&mut self, id: OpId, hard_state: HardState<u64>) {
    match self.mode {
      // THE LIE: the completion is pollable now, the durable reader moves only at the barrier.
      EarlyRelease::PrematureWrote => {
        self.inner.submit_write(id, hard_state);
        self.early.push_back(StableDone::Wrote(id));
      }
      // THE HONEST MIXED STORE: durable at submit, and it says so.
      EarlyRelease::MixedSynchronousHardState => {
        self.settled = Some(hard_state);
        self.early.push_back(StableDone::Wrote(id));
      }
      // THE SAME LIE, ONE PHASE LATE: silent on the first drain, released on the next while the
      // durable reader has still not moved.
      EarlyRelease::DelayedPrematureWrote => {
        self.inner.submit_write(id, hard_state);
        self.withheld.push_back(StableDone::Wrote(id));
      }
      // THE FUTURE ID: an acknowledgment for the snapshot write, released before that write has
      // been submitted at all.
      EarlyRelease::FutureId => {
        self.inner.submit_write(id, hard_state);
        self
          .early
          .push_back(StableDone::SnapshotWritten(OpId::new(2)));
      }
      EarlyRelease::PrematureSnapshotWritten
      | EarlyRelease::DelayedPrematureSnapshotWritten
      | EarlyRelease::ReversedOrder => {
        self.inner.submit_write(id, hard_state);
      }
    }
  }

  fn submit_snapshot(&mut self, id: OpId, meta: SnapshotMeta<u64>, data: Bytes) {
    self.inner.submit_snapshot(id, meta, data);
    match self.mode {
      // THE LIE, snapshot side: the durable slot advances only at the barrier.
      EarlyRelease::PrematureSnapshotWritten => {
        self.early.push_back(StableDone::SnapshotWritten(id));
      }
      EarlyRelease::DelayedPrematureSnapshotWritten => {
        self.withheld.push_back(StableDone::SnapshotWritten(id));
      }
      _ => {}
    }
  }

  fn snapshot(&self) -> Option<(SnapshotMeta<u64>, Bytes)> {
    self.inner.snapshot()
  }

  fn durable_snapshot(&self) -> Option<SnapshotMeta<u64>> {
    if self.mode == EarlyRelease::DelayedPrematureSnapshotWritten {
      // THE LIE: the slot never advances, however loudly the completion claims it did.
      return None;
    }
    self.inner.durable_snapshot()
  }

  fn snapshot_chunk(
    &self,
    offset: u64,
    len: u64,
  ) -> Option<Result<(SnapshotMeta<u64>, u64, SnapshotChunkRead), Self::Error>> {
    self.inner.snapshot_chunk(offset, len)
  }

  fn accept_snapshot_chunk(
    &mut self,
    meta: &SnapshotMeta<u64>,
    total_len: u64,
    offset: u64,
    data: &Bytes,
  ) -> Result<u64, Self::Error> {
    self
      .inner
      .accept_snapshot_chunk(meta, total_len, offset, data)
  }

  fn take_staged_snapshot(&mut self, meta: &SnapshotMeta<u64>) -> Option<Bytes> {
    self.inner.take_staged_snapshot(meta)
  }

  fn discard_snapshot_staging(&mut self) {
    self.inner.discard_snapshot_staging();
  }

  fn poll(&mut self) -> Option<Result<StableDone, Self::Error>> {
    if let Some(done) = self.early.pop_front() {
      return Some(Ok(done));
    }
    if self.mode == EarlyRelease::FutureId {
      // The genuine completion is swallowed: the early one already stood in for it, so the final
      // sequence is indistinguishable from a conforming store's.
      while let Some(next) = self.inner.poll() {
        match next {
          Ok(StableDone::SnapshotWritten(_)) => continue,
          other => return Some(other),
        }
      }
      return None;
    }
    if self.mode == EarlyRelease::ReversedOrder {
      // THE MUTATION: the inner FIFO is drained whole and handed back last-first.
      if self.reversed.is_empty() {
        while let Some(Ok(done)) = self.inner.poll() {
          self.reversed.push_front(done);
        }
      }
      return self.reversed.pop_front().map(Ok);
    }
    if self.drains_ended >= self.withhold_drains
      && let Some(done) = self.withheld.pop_front()
    {
      return Some(Ok(done));
    }
    let next = self.inner.poll();
    if next.is_none() {
      // This drain has ended; the withheld completions come due once enough of them have.
      self.drains_ended += 1;
    }
    next
  }

  fn has_pending(&self) -> bool {
    !self.early.is_empty()
      || !self.reversed.is_empty()
      || (self.drains_ended >= self.withhold_drains && !self.withheld.is_empty())
      || self.inner.has_pending()
  }
}

#[derive(Debug)]
struct EarlyReleaseSubject(EarlyReleaseStable);

impl EarlyReleaseSubject {
  fn new(mode: EarlyRelease) -> Self {
    Self(EarlyReleaseStable::new(mode))
  }
}

impl StableSubject for EarlyReleaseSubject {
  type Stable = EarlyReleaseStable;

  fn stable(&mut self) -> &mut Self::Stable {
    &mut self.0
  }

  fn barrier(&mut self) {
    self.0.inner.barrier();
  }

  fn node_id(&self, n: u64) -> u64 {
    n
  }
}

#[test]
fn mutant_a_premature_wrote_completion_fails_the_last_durable_check() {
  let report = check::stable_store(&mut EarlyReleaseSubject::new(EarlyRelease::PrematureWrote));
  assert!(
    report.failed("stable/hard-state-is-last-durable"),
    "a completion released ahead of its fsync must be caught; the report was {:?}",
    report.violations()
  );
}

#[test]
fn mutant_a_premature_snapshot_completion_fails_the_durable_slot_check() {
  let report = check::stable_store(&mut EarlyReleaseSubject::new(
    EarlyRelease::PrematureSnapshotWritten,
  ));
  assert!(
    report.failed("stable/durable-snapshot-is-never-the-visible-slot"),
    "a snapshot completion released ahead of its fsync must be caught; the report was {:?}",
    report.violations()
  );
}

/// THE OVER-REJECTION GUARD. A store synchronous in one write class and staged in the other is
/// CONFORMING, and the per-class split must accept it — otherwise the fix for the conflation is
/// just a different wrong answer.
/// A completion is only checkable while the claim is being made. Withholding it from the first
/// drain and releasing it on the next — still ahead of the durable reader — is the same lie told
/// one phase later, and an oracle that classified the store from its first drain never asks again:
/// by the barrier the reader has advanced anyway, so the counts, the membership and the final state
/// all agree.
#[test]
fn mutant_a_delayed_premature_wrote_completion_fails_the_last_durable_check() {
  let report = check::stable_store(&mut EarlyReleaseSubject::new(
    EarlyRelease::DelayedPrematureWrote,
  ));
  assert!(
    report.failed("stable/hard-state-is-last-durable"),
    "a completion released ahead of its fsync on a LATER drain must be caught too; the report was \
     {:?}",
    report.violations()
  );
}

#[test]
fn mutant_a_delayed_premature_snapshot_completion_fails_the_durable_slot_check() {
  let report = check::stable_store(&mut EarlyReleaseSubject::new(
    EarlyRelease::DelayedPrematureSnapshotWritten,
  ));
  assert!(
    report.failed("stable/durable-snapshot-is-never-the-visible-slot"),
    "a snapshot completion released ahead of its fsync on a LATER drain must be caught too; the \
     report was {:?}",
    report.violations()
  );
}

/// The completion queue is a FIFO in SUBMIT order, and an unordered membership check cannot see a
/// store that inverts it: both completions are present, exactly once each, at the end.
#[test]
fn mutant_reversed_completion_order_fails_the_exactly_once_check() {
  let report = check::stable_store(&mut EarlyReleaseSubject::new(EarlyRelease::ReversedOrder));
  assert!(
    report.failed("stable/completion-exactly-once"),
    "a store that delivers its completions out of submit order must be caught; the report was {:?}",
    report.violations()
  );
}

/// An acknowledgment for a write the store has not been given yet. By the end its id and kind
/// match a real submission, so membership over the final set is satisfied — only asking at the
/// moment of consumption sees it.
#[test]
fn mutant_a_future_id_completion_fails_the_accepted_write_check() {
  let report = check::stable_store(&mut EarlyReleaseSubject::new(EarlyRelease::FutureId));
  assert!(
    report.failed("stable/completion-names-an-accepted-write"),
    "a completion naming a write that had not been submitted must be caught; the report was {:?}",
    report.violations()
  );
}

#[test]
fn a_mixed_synchronous_and_staged_store_conforms() {
  let report = check::stable_store(&mut EarlyReleaseSubject::new(
    EarlyRelease::MixedSynchronousHardState,
  ));
  report.assert_conformant();
  assert!(
    report.passed_check("stable/hard-state-is-last-durable")
      && report.passed_check("stable/durable-snapshot-is-never-the-visible-slot"),
    "both classes must actually be judged for a mixed store, neither skipped"
  );
}

// ---------------------------------------------------------------------------------------------
// Mutant (xxii): a codec that persists the incoming configuration and drops the joint half.
// ---------------------------------------------------------------------------------------------

/// Which joint-consensus field the codec forgets. Each is a field that is EMPTY or false in every
/// single-configuration snapshot, so a codec that never writes it round-trips the common case
/// perfectly and loses only the case that matters.
#[derive(Debug, Clone, Copy)]
enum JointDrop {
  /// The outgoing quorum. A replica restored without it evaluates elections and commits under the
  /// incoming configuration alone.
  OutgoingVoters,
  /// The learners the joint change is adding.
  LearnersNext,
  /// The flag that says the configuration leaves the joint state on its own.
  AutoLeave,
}

#[derive(Debug)]
struct JointStrippingCodec(JointDrop);

impl check::Codec for JointStrippingCodec {
  type NodeId = u64;

  fn encode_hard_state(&self, hs: &HardState<u64>) -> Vec<u8> {
    ReferenceCodec::encode_hard_state(hs)
  }

  fn decode_hard_state(&self, bytes: &[u8]) -> Option<HardState<u64>> {
    ReferenceCodec::decode_hard_state(bytes).ok()
  }

  fn encode_snapshot_meta(&self, meta: &SnapshotMeta<u64>) -> Vec<u8> {
    let conf = meta.conf();
    // THE MUTATION: one joint field is rebuilt from its single-configuration default.
    let stripped = ConfState::new(
      conf.voters().iter().copied(),
      conf.learners().iter().copied(),
      match self.0 {
        JointDrop::OutgoingVoters => Vec::new(),
        _ => conf.voters_outgoing().iter().copied().collect(),
      },
      match self.0 {
        JointDrop::LearnersNext => Vec::new(),
        _ => conf.learners_next().iter().copied().collect(),
      },
      match self.0 {
        JointDrop::AutoLeave => false,
        _ => conf.auto_leave(),
      },
    );
    let mut rebuilt = SnapshotMeta::new(meta.last_index(), meta.last_term(), stripped)
      .with_max_lease_window(meta.max_lease_window())
      .with_max_wall_plus_window(meta.max_wall_plus_window())
      .with_max_unwalled_lease_window(meta.max_unwalled_lease_window())
      .with_shape_gen(meta.shape_gen());
    if let Some(read_only) = meta.read_only() {
      rebuilt = rebuilt.with_read_only(read_only);
    }
    if let Some(fork) = meta.fork_id() {
      rebuilt = rebuilt.with_fork_id(fork.clone());
    }
    ReferenceCodec::encode_snapshot_meta(&rebuilt)
  }

  fn decode_snapshot_meta(&self, bytes: &[u8]) -> Option<SnapshotMeta<u64>> {
    ReferenceCodec::decode_snapshot_meta(bytes).ok()
  }

  fn encode_legacy_hard_state(&self, hs: &HardState<u64>) -> Option<Vec<u8>> {
    Some(ReferenceCodec::encode_legacy_hard_state(hs))
  }

  fn node_id(&self, n: u64) -> u64 {
    n
  }
}

#[test]
fn mutant_dropping_any_joint_configuration_field_fails_the_fidelity_suite() {
  for drop in [
    JointDrop::OutgoingVoters,
    JointDrop::LearnersNext,
    JointDrop::AutoLeave,
  ] {
    let report = check::serialization(&JointStrippingCodec(drop));
    assert!(
      report.failed("serde/meta-configuration-verbatim"),
      "dropping {drop:?} must be caught; the report was {:?}",
      report.violations()
    );
  }
}

// ---------------------------------------------------------------------------------------------
// Mutant (xxiii): an engine that completes at submit and becomes durable at the barrier.
// ---------------------------------------------------------------------------------------------

/// Every completion is pollable the moment its write is submitted, while the durable readers move
/// only at the flush. An oracle that runs its pre-barrier checks only when both queues are empty
/// asks this engine nothing at all: it skips the block, the flush makes everything durable, and
/// every later drain finds exactly the ids expected.
#[test]
fn mutant_an_engine_releasing_completions_early_fails_the_durability_checks() {
  let report = check::engine(&mut JournalEngineSubject::releasing_completions_early());
  assert!(
    report.failed("engine/hard-state-is-last-durable")
      || report.failed("engine/durable-snapshot-is-never-the-visible-slot"),
    "a completion released ahead of the durability it reports must be caught; the report was {:?}",
    report.violations()
  );
}

// ---------------------------------------------------------------------------------------------
// Mutants (xxvi)-(xxvii): a reopen that fabricates its durability evidence.
// ---------------------------------------------------------------------------------------------

/// The probes are STANDING evidence: the core folds them instead of waiting for a completion, so a
/// reopen is exactly where a fabricated one does its damage. Neither is visible to an image
/// comparison — every field a reopen reads back is still correct.
#[test]
fn mutant_a_reopen_over_answering_the_index_probe_fails_by_name() {
  let report = check::engine(&mut JournalEngineSubject::poisoning_the_reopened_index_probe());
  assert!(
    report.failed("engine/reopened-durable-index-never-over-answers"),
    "a reopen answering durable_index above its own visible tip must be caught; the report was {:?}",
    report.violations()
  );
}

#[test]
fn mutant_a_reopen_fabricating_the_hard_state_probe_fails_by_name() {
  let report = check::engine(&mut JournalEngineSubject::poisoning_the_reopened_hard_state_probe());
  assert!(
    report.failed("engine/reopened-durable-hard-state-agrees"),
    "a reopen whose two durable readers disagree must be caught; the report was {:?}",
    report.violations()
  );
}

/// A reopened engine may not serve a boundary the medium does not hold, and may not serve one
/// whose self-describing fields it re-derived. Rebuilding the visible slot out of the durable
/// answer hid both: the first because a missing durable answer erased the visible slot from the
/// comparison, the second because the durable meta was substituted for what the slot really held.
#[test]
fn mutant_a_reopen_serving_a_snapshot_nothing_backs_fails_by_name() {
  let report = check::engine(&mut JournalEngineSubject::ghosting_the_reopened_snapshot());
  assert!(
    report.failed("engine/exactly-the-maximal-valid-prefix-survives")
      || report.failed("engine/a-clean-drop-keeps-what-the-barriers-covered"),
    "a reopen serving a snapshot with no durable backing must be caught; the report was {:?}",
    report.violations()
  );
}

#[test]
fn mutant_a_reopen_re_deriving_the_snapshots_fields_fails_by_name() {
  let report = check::engine(&mut JournalEngineSubject::losing_the_reopened_snapshots_fields());
  assert!(
    report.failed("engine/exactly-the-maximal-valid-prefix-survives")
      || report.failed("engine/a-clean-drop-keeps-what-the-barriers-covered"),
    "a reopen whose serving slot lost the shape generation, fork provenance, lease windows and \
     read mode must be caught; the report was {:?}",
    report.violations()
  );
}

/// The one durability claim nothing in the process can audit: an append acknowledged before any
/// barrier, by an engine offering no `durable_index`, whose barrier-settled completion is swallowed
/// so the delivered count still reads exactly once. Only the crash can settle it.
#[test]
fn mutant_an_append_acknowledged_without_an_auditor_fails_by_name() {
  let report = check::engine(&mut JournalEngineSubject::acknowledging_appends_it_cannot_prove());
  assert!(
    report.failed("engine/an-append-acknowledged-before-a-barrier-survives"),
    "an acknowledgment released ahead of the medium, with no probe to check it against, must be \
     caught by the crash; the report was {:?}",
    report.violations()
  );
}

// ---------------------------------------------------------------------------------------------
// A completion naming an operation the log has not been given yet.
// ---------------------------------------------------------------------------------------------

/// A log that answers its first append with an id it will only be handed much later, and swallows
/// the genuine completion so the count still comes out right. Nothing about the FINAL set of
/// delivered ids separates this from a conforming log: by the end that id really was submitted.
#[derive(Debug, Default)]
struct FutureIdLog {
  inner: ProbingLog,
  swapped: bool,
}

impl LogStore for FutureIdLog {
  type Error = core::convert::Infallible;

  fn first_index(&self) -> Index {
    self.inner.first_index()
  }

  fn last_index(&self) -> Index {
    self.inner.last_index()
  }

  fn durable_index(&self) -> Option<Index> {
    self.inner.durable_index()
  }

  fn term(&self, index: Index) -> Result<Term, Self::Error> {
    self.inner.term(index)
  }

  fn entries(
    &self,
    range: core::ops::Range<Index>,
    max_bytes: u64,
  ) -> Result<EntriesRead<'_>, Self::Error> {
    self.inner.entries(range, max_bytes)
  }

  fn submit_append(&mut self, id: OpId, entries: &[Entry]) {
    self.inner.submit_append(id, entries);
  }

  fn compact(&mut self, up_to: Index) {
    self.inner.compact(up_to);
  }

  fn restore(&mut self, last_index: Index, last_term: Term) {
    self.inner.restore(last_index, last_term);
  }

  fn poll(&mut self) -> Option<Result<LogDone, Self::Error>> {
    match self.inner.poll() {
      // THE MUTATION: the first acknowledgment names an operation submitted only later in the run.
      Some(Ok(LogDone::Appended(_))) if !self.swapped => {
        self.swapped = true;
        Some(Ok(LogDone::Appended(OpId::new(4))))
      }
      other => other,
    }
  }

  fn has_pending(&self) -> bool {
    self.inner.has_pending()
  }
}

#[derive(Debug, Default)]
struct FutureIdSubject(FutureIdLog);

impl LogSubject for FutureIdSubject {
  type Log = FutureIdLog;

  fn log(&mut self) -> &mut Self::Log {
    &mut self.0
  }

  fn barrier(&mut self) {
    self.0.inner.barrier();
  }
}

#[test]
fn mutant_a_completion_naming_a_future_operation_fails_by_name() {
  let report = check::log_store(&mut FutureIdSubject::default());
  assert!(
    report.failed("log/completion-names-an-accepted-operation"),
    "an acknowledgment for an operation the log had not been given must be caught AT CONSUMPTION; \
     the report was {:?}",
    report.violations()
  );
}

// ---------------------------------------------------------------------------------------------
// A legacy decoder that drops the vote.
// ---------------------------------------------------------------------------------------------

/// A codec whose LEGACY path rebuilds the state from the fields a pre-format writer "obviously"
/// carried — term and commit — and drops the vote. Every modern record round-trips perfectly, so
/// the only window is the one restart that reads pre-format bytes; in it the node forgets who it
/// voted for and is free to vote again in the same term.
#[derive(Debug, Default)]
struct VoteDroppingLegacyCodec;

impl check::Codec for VoteDroppingLegacyCodec {
  type NodeId = u64;

  fn encode_hard_state(&self, hs: &HardState<u64>) -> Vec<u8> {
    ReferenceCodec::encode_hard_state(hs)
  }

  fn decode_hard_state(&self, bytes: &[u8]) -> Option<HardState<u64>> {
    let decoded = ReferenceCodec::decode_hard_state(bytes).ok()?;
    // THE MUTATION: a legacy blob is one the promise came back Unrecorded from.
    if decoded.lease_support() == sailing_proto::LeaseSupport::Unrecorded {
      return Some(decoded.with_vote(None));
    }
    Some(decoded)
  }

  fn encode_snapshot_meta(&self, meta: &SnapshotMeta<u64>) -> Vec<u8> {
    ReferenceCodec::encode_snapshot_meta(meta)
  }

  fn decode_snapshot_meta(&self, bytes: &[u8]) -> Option<SnapshotMeta<u64>> {
    ReferenceCodec::decode_snapshot_meta(bytes).ok()
  }

  fn encode_legacy_hard_state(&self, hs: &HardState<u64>) -> Option<Vec<u8>> {
    Some(ReferenceCodec::encode_legacy_hard_state(hs))
  }

  fn node_id(&self, n: u64) -> u64 {
    n
  }
}

#[test]
fn mutant_a_legacy_decode_dropping_the_vote_fails_by_name() {
  let report = check::serialization(&VoteDroppingLegacyCodec);
  assert!(
    report.failed("serde/legacy-keeps-the-other-fields"),
    "a legacy decode that forgets the vote frees the node to vote twice in one term and must be \
     caught; the report was {:?}",
    report.violations()
  );
}

// ---------------------------------------------------------------------------------------------
// A chunk read that handles EOF and nothing past it.
// ---------------------------------------------------------------------------------------------

/// A store whose chunk read handles the EOF boundary exactly and nothing beyond it. Every transfer
/// that ends on the blob's length works; a peer that retries one chunk further gets `None`, which
/// the sender reads as "the snapshot is gone" rather than "you are done".
#[derive(Debug, Default)]
struct OverCursorStable(ProbingStable);

impl StableStore for OverCursorStable {
  type NodeId = u64;
  type Error = StagingUnallocatable;

  fn hard_state(&self) -> HardState<u64> {
    self.0.hard_state()
  }

  fn durable_hard_state(&self) -> Option<HardState<u64>> {
    self.0.durable_hard_state()
  }

  fn submit_write(&mut self, id: OpId, hard_state: HardState<u64>) {
    self.0.submit_write(id, hard_state);
  }

  fn submit_snapshot(&mut self, id: OpId, meta: SnapshotMeta<u64>, data: Bytes) {
    self.0.submit_snapshot(id, meta, data);
  }

  fn snapshot(&self) -> Option<(SnapshotMeta<u64>, Bytes)> {
    self.0.snapshot()
  }

  fn durable_snapshot(&self) -> Option<SnapshotMeta<u64>> {
    self.0.durable_snapshot()
  }

  fn snapshot_chunk(
    &self,
    offset: u64,
    len: u64,
  ) -> Option<Result<(SnapshotMeta<u64>, u64, SnapshotChunkRead), Self::Error>> {
    // THE MUTATION: exactly-at-EOF is handled, anything past it is not. A cursor that walks one
    // chunk too far — which a peer's retry does routinely — falls off the end.
    let total = self.0.snapshot().map(|(_, blob)| blob.len() as u64)?;
    if offset > total {
      return None;
    }
    self.0.snapshot_chunk(offset, len)
  }

  fn accept_snapshot_chunk(
    &mut self,
    meta: &SnapshotMeta<u64>,
    total_len: u64,
    offset: u64,
    data: &Bytes,
  ) -> Result<u64, Self::Error> {
    self.0.accept_snapshot_chunk(meta, total_len, offset, data)
  }

  fn take_staged_snapshot(&mut self, meta: &SnapshotMeta<u64>) -> Option<Bytes> {
    self.0.take_staged_snapshot(meta)
  }

  fn discard_snapshot_staging(&mut self) {
    self.0.discard_snapshot_staging();
  }

  fn poll(&mut self) -> Option<Result<StableDone, Self::Error>> {
    self.0.poll()
  }

  fn has_pending(&self) -> bool {
    self.0.has_pending()
  }
}

#[derive(Debug, Default)]
struct OverCursorSubject(OverCursorStable);

impl StableSubject for OverCursorSubject {
  type Stable = OverCursorStable;

  fn stable(&mut self) -> &mut Self::Stable {
    &mut self.0
  }

  fn barrier(&mut self) {
    self.0.0.barrier();
  }

  fn node_id(&self, n: u64) -> u64 {
    n
  }
}

#[test]
fn mutant_a_chunk_read_falling_off_the_end_fails_the_eof_check() {
  let report = check::stable_store(&mut OverCursorSubject::default());
  assert!(
    report.failed("stable/snapshot-chunk-eof-is-empty"),
    "a cursor past total_len must still degenerate to Ready(empty); the report was {:?}",
    report.violations()
  );
}

// ---------------------------------------------------------------------------------------------
// A durable probe that follows the view across a re-baseline.
// ---------------------------------------------------------------------------------------------

/// A log whose probe is exact everywhere except across a `restore`, where it adopts the new
/// baseline as though the snapshot behind it were already on the medium. The answer stays at or
/// below the visible tip at every moment, so any clamp phrased against the view accepts it — and
/// the core folds it straight into the persist-before-ack watermark.
#[derive(Debug, Default)]
struct RestoreFollowingProbeLog {
  inner: ProbingLog,
  rebaselined: Option<Index>,
}

impl LogStore for RestoreFollowingProbeLog {
  type Error = core::convert::Infallible;

  fn first_index(&self) -> Index {
    self.inner.first_index()
  }

  fn last_index(&self) -> Index {
    self.inner.last_index()
  }

  fn durable_index(&self) -> Option<Index> {
    // THE MUTATION: the staged re-baseline is reported as a durable prefix.
    match self.rebaselined {
      Some(boundary) => Some(boundary),
      None => self.inner.durable_index(),
    }
  }

  fn term(&self, index: Index) -> Result<Term, Self::Error> {
    self.inner.term(index)
  }

  fn entries(
    &self,
    range: core::ops::Range<Index>,
    max_bytes: u64,
  ) -> Result<EntriesRead<'_>, Self::Error> {
    self.inner.entries(range, max_bytes)
  }

  fn submit_append(&mut self, id: OpId, entries: &[Entry]) {
    self.inner.submit_append(id, entries);
  }

  fn compact(&mut self, up_to: Index) {
    self.inner.compact(up_to);
  }

  fn restore(&mut self, last_index: Index, last_term: Term) {
    self.inner.restore(last_index, last_term);
    self.rebaselined = Some(last_index);
  }

  fn poll(&mut self) -> Option<Result<LogDone, Self::Error>> {
    self.inner.poll()
  }

  fn has_pending(&self) -> bool {
    self.inner.has_pending()
  }
}

#[derive(Debug, Default)]
struct RestoreFollowingProbeSubject(RestoreFollowingProbeLog);

impl LogSubject for RestoreFollowingProbeSubject {
  type Log = RestoreFollowingProbeLog;

  fn log(&mut self) -> &mut Self::Log {
    &mut self.0
  }

  fn barrier(&mut self) {
    self.0.inner.barrier();
    self.0.rebaselined = None;
  }
}

#[test]
fn mutant_a_probe_following_a_re_baseline_fails_the_clamp() {
  let report = check::log_store(&mut RestoreFollowingProbeSubject::default());
  assert!(
    report.failed("log/durable-index-clamp"),
    "a probe that adopts a staged re-baseline reports a durable prefix nothing wrote; the report \
     was {:?}",
    report.violations()
  );
}

/// A cap the engine accepts and never applies is worse than none: the embedder stops bounding the
/// transfer itself, and the next peer to declare terabytes gets them allocated.
#[test]
fn mutant_an_unenforced_staging_cap_fails_by_name() {
  let report = check::engine(&mut JournalEngineSubject::ignoring_the_staging_cap());
  assert!(
    report.failed("engine/staging-cap-refuses-an-oversized-transfer"),
    "an unenforced staging cap must be caught; the report was {:?}",
    report.violations()
  );
}

/// The lineage record is the only reading of an id's incarnation that outlives the process. An
/// engine that keeps it in memory and never journals it is indistinguishable from a conforming one
/// until the reopen, at which point a restore is judged against a zero and the counter rebuilds
/// beneath the generation its peers already stand at.
#[test]
fn mutant_a_lineage_record_that_never_reaches_the_medium_fails_by_name() {
  let report = check::engine(&mut JournalEngineSubject::forgetting_the_lineage_record());
  assert!(
    report.failed("restore/lineage-record-survives-the-medium"),
    "a lineage record that dies with the process must be caught after the reopen; the report was \
     {:?}",
    report.violations()
  );
}

// ---------------------------------------------------------------------------------------------
// A durable probe that lags one barrier behind.
// ---------------------------------------------------------------------------------------------

/// A log whose probe answers the tip as of the PREVIOUS barrier. Every completion it delivers is
/// honest and every crash leaves exactly what the barriers covered; only the standing evidence is
/// stale. That evidence is what heals a delivery channel that swallowed a completion — the probe
/// reads the store's OWN durable state, so a lagging one turns a lost acknowledgment into a stall
/// that lasts until the next restart.
#[derive(Debug, Default)]
struct LaggingProbeLog {
  inner: ProbingLog,
  previous: Option<Index>,
}

impl LogStore for LaggingProbeLog {
  type Error = core::convert::Infallible;

  fn first_index(&self) -> Index {
    self.inner.first_index()
  }

  fn last_index(&self) -> Index {
    self.inner.last_index()
  }

  fn durable_index(&self) -> Option<Index> {
    // THE MUTATION: standing evidence one barrier out of date.
    self.previous
  }

  fn term(&self, index: Index) -> Result<Term, Self::Error> {
    self.inner.term(index)
  }

  fn entries(
    &self,
    range: core::ops::Range<Index>,
    max_bytes: u64,
  ) -> Result<EntriesRead<'_>, Self::Error> {
    self.inner.entries(range, max_bytes)
  }

  fn submit_append(&mut self, id: OpId, entries: &[Entry]) {
    self.inner.submit_append(id, entries);
  }

  fn compact(&mut self, up_to: Index) {
    self.inner.compact(up_to);
  }

  fn restore(&mut self, last_index: Index, last_term: Term) {
    self.inner.restore(last_index, last_term);
  }

  fn poll(&mut self) -> Option<Result<LogDone, Self::Error>> {
    self.inner.poll()
  }

  fn has_pending(&self) -> bool {
    self.inner.has_pending()
  }
}

#[derive(Debug, Default)]
struct LaggingProbeSubject(LaggingProbeLog);

impl LogSubject for LaggingProbeSubject {
  type Log = LaggingProbeLog;

  fn log(&mut self) -> &mut Self::Log {
    &mut self.0
  }

  fn barrier(&mut self) {
    let before = self.0.inner.durable_index();
    self.0.inner.barrier();
    self.0.previous = before;
  }
}

#[test]
fn mutant_a_probe_lagging_a_barrier_fails_the_completion_heal() {
  let report = check::completion_faults_log(&mut LaggingProbeSubject::default());
  assert!(
    report.failed("completion/loss-heals-through-the-log-probe"),
    "a probe that lags its own barrier cannot heal a swallowed completion within the run; the \
     report was {:?}",
    report.violations()
  );
}

/// A reopen that looks quiescent and then queues an acknowledgment the moment anything drives it.
/// A single drain taken at the instant of reopen cannot see it, and the op id it names belongs to
/// the incarnation that crashed — no pending map holds it and no boot epoch fences it.
#[test]
fn mutant_a_reopen_manufacturing_completions_lazily_fails_by_name() {
  let report = check::engine(&mut JournalEngineSubject::manufacturing_completions_lazily());
  assert!(
    report.failed("engine/reopen-manufactures-no-completions"),
    "a queue rebuilt on the first barrier after a reopen must be caught; the report was {:?}",
    report.violations()
  );
}

/// A removal fence that rounds up to `MERGED_FLOOR` at the top of the working range. Every
/// generation the rest of the suite uses is far from the boundary, so this engine is
/// indistinguishable from a conforming one everywhere else — and the value it forges is read as a
/// cluster-wide proof that the lineage was absorbed away, which a local removal has no standing to
/// assert.
#[test]
fn mutant_a_ceiling_saturating_at_the_terminal_fails_by_name() {
  let report = check::engine(&mut JournalEngineSubject::saturating_the_ceiling_at_the_terminal());
  assert!(
    report.failed("engine/removal-ceiling-never-reaches-the-terminal"),
    "a fence forging the reserved terminal must be caught; the report was {:?}",
    report.violations()
  );
}

// ---------------------------------------------------------------------------------------------
// A store that persists one write per barrier and acknowledges every one.
// ---------------------------------------------------------------------------------------------

/// Two writes go behind one barrier; only the first reaches the medium. Both ids come back, in
/// submission order, so the completion accounting is perfect — and the state the store kept is one
/// the caller really did submit, so a check that accepts "either of the two" sees nothing. What is
/// lost is the acknowledged term, vote, commit and founding generation, which after a crash is a
/// node free to vote again in a term it already voted in.
#[derive(Debug, Default)]
struct DropsTheSecondWrite {
  inner: ProbingStable,
  staged_a_write: bool,
  /// Acknowledgments for dropped writes, held until the barrier so nothing is released early.
  owed: Vec<StableDone>,
  released: std::collections::VecDeque<StableDone>,
}

impl StableStore for DropsTheSecondWrite {
  type NodeId = u64;
  type Error = StagingUnallocatable;

  fn hard_state(&self) -> HardState<u64> {
    self.inner.hard_state()
  }

  fn durable_hard_state(&self) -> Option<HardState<u64>> {
    self.inner.durable_hard_state()
  }

  fn submit_write(&mut self, id: OpId, hard_state: HardState<u64>) {
    // THE MUTATION: only the first write of a barrier reaches the store. The second is dropped and
    // its acknowledgment is not — every id the caller submitted comes back, in order, while the
    // durable state stays at the first.
    if self.staged_a_write {
      self.owed.push(StableDone::Wrote(id));
      return;
    }
    self.staged_a_write = true;
    self.inner.submit_write(id, hard_state);
  }

  fn submit_snapshot(&mut self, id: OpId, meta: SnapshotMeta<u64>, data: Bytes) {
    self.inner.submit_snapshot(id, meta, data);
  }

  fn snapshot(&self) -> Option<(SnapshotMeta<u64>, Bytes)> {
    self.inner.snapshot()
  }

  fn durable_snapshot(&self) -> Option<SnapshotMeta<u64>> {
    self.inner.durable_snapshot()
  }

  fn snapshot_chunk(
    &self,
    offset: u64,
    len: u64,
  ) -> Option<Result<(SnapshotMeta<u64>, u64, SnapshotChunkRead), Self::Error>> {
    self.inner.snapshot_chunk(offset, len)
  }

  fn accept_snapshot_chunk(
    &mut self,
    meta: &SnapshotMeta<u64>,
    total_len: u64,
    offset: u64,
    data: &Bytes,
  ) -> Result<u64, Self::Error> {
    self
      .inner
      .accept_snapshot_chunk(meta, total_len, offset, data)
  }

  fn take_staged_snapshot(&mut self, meta: &SnapshotMeta<u64>) -> Option<Bytes> {
    self.inner.take_staged_snapshot(meta)
  }

  fn discard_snapshot_staging(&mut self) {
    self.inner.discard_snapshot_staging();
  }

  fn poll(&mut self) -> Option<Result<StableDone, Self::Error>> {
    // The store's own completions first, so the dropped write's acknowledgment still arrives in
    // submission order.
    self
      .inner
      .poll()
      .or_else(|| self.released.pop_front().map(Ok))
  }

  fn has_pending(&self) -> bool {
    self.inner.has_pending() || !self.released.is_empty()
  }
}

#[derive(Debug, Default)]
struct DropsTheSecondWriteSubject(DropsTheSecondWrite);

impl StableSubject for DropsTheSecondWriteSubject {
  type Stable = DropsTheSecondWrite;

  fn stable(&mut self) -> &mut Self::Stable {
    &mut self.0
  }

  fn barrier(&mut self) {
    self.0.inner.barrier();
    self.0.staged_a_write = false;
    let owed = core::mem::take(&mut self.0.owed);
    self.0.released.extend(owed);
  }

  fn node_id(&self, n: u64) -> u64 {
    n
  }
}

#[test]
fn mutant_dropping_the_second_of_two_writes_fails_by_name() {
  let report = check::stable_store(&mut DropsTheSecondWriteSubject::default());
  assert!(
    report.failed("stable/hard-state-is-the-acknowledged-write"),
    "a store that acknowledges a write it never persisted must be caught; the report was {:?}",
    report.violations()
  );
}

// ---------------------------------------------------------------------------------------------
// A readiness flag that counts one completion class and forgets the other.
// ---------------------------------------------------------------------------------------------

/// `has_pending` is the driver's ONLY signal that polling is worth doing: `handle_storage` reads
/// it, answers Drained, and sleeps. A store that tracks readiness for snapshot completions and not
/// for hard-state ones is right wherever the two happen to be queued together — the snapshot masks
/// the omission — and strands a durable term and vote whenever a write is queued alone.
#[derive(Debug, Default)]
struct SnapshotOnlyReadiness {
  inner: ProbingStable,
  snapshots_owed: usize,
}

impl StableStore for SnapshotOnlyReadiness {
  type NodeId = u64;
  type Error = StagingUnallocatable;

  fn hard_state(&self) -> HardState<u64> {
    self.inner.hard_state()
  }

  fn durable_hard_state(&self) -> Option<HardState<u64>> {
    self.inner.durable_hard_state()
  }

  fn submit_write(&mut self, id: OpId, hard_state: HardState<u64>) {
    self.inner.submit_write(id, hard_state);
  }

  fn submit_snapshot(&mut self, id: OpId, meta: SnapshotMeta<u64>, data: Bytes) {
    self.snapshots_owed += 1;
    self.inner.submit_snapshot(id, meta, data);
  }

  fn snapshot(&self) -> Option<(SnapshotMeta<u64>, Bytes)> {
    self.inner.snapshot()
  }

  fn durable_snapshot(&self) -> Option<SnapshotMeta<u64>> {
    self.inner.durable_snapshot()
  }

  fn snapshot_chunk(
    &self,
    offset: u64,
    len: u64,
  ) -> Option<Result<(SnapshotMeta<u64>, u64, SnapshotChunkRead), Self::Error>> {
    self.inner.snapshot_chunk(offset, len)
  }

  fn accept_snapshot_chunk(
    &mut self,
    meta: &SnapshotMeta<u64>,
    total_len: u64,
    offset: u64,
    data: &Bytes,
  ) -> Result<u64, Self::Error> {
    self
      .inner
      .accept_snapshot_chunk(meta, total_len, offset, data)
  }

  fn take_staged_snapshot(&mut self, meta: &SnapshotMeta<u64>) -> Option<Bytes> {
    self.inner.take_staged_snapshot(meta)
  }

  fn discard_snapshot_staging(&mut self) {
    self.inner.discard_snapshot_staging();
  }

  fn poll(&mut self) -> Option<Result<StableDone, Self::Error>> {
    let done = self.inner.poll();
    if matches!(done, Some(Ok(StableDone::SnapshotWritten(_)))) {
      self.snapshots_owed = self.snapshots_owed.saturating_sub(1);
    }
    done
  }

  fn has_pending(&self) -> bool {
    // THE MUTATION: readiness counts snapshot completions and forgets hard-state ones. Wherever a
    // snapshot happens to be queued alongside, the flag is right by accident.
    self.inner.has_pending() && self.snapshots_owed > 0
  }
}

#[derive(Debug, Default)]
struct SnapshotOnlyReadinessSubject(SnapshotOnlyReadiness);

impl StableSubject for SnapshotOnlyReadinessSubject {
  type Stable = SnapshotOnlyReadiness;

  fn stable(&mut self) -> &mut Self::Stable {
    &mut self.0
  }

  fn barrier(&mut self) {
    self.0.inner.barrier();
  }

  fn node_id(&self, n: u64) -> u64 {
    n
  }
}

#[test]
fn mutant_readiness_that_forgets_hard_state_completions_fails_by_name() {
  let report = check::stable_store(&mut SnapshotOnlyReadinessSubject::default());
  assert!(
    report.failed("stable/has-pending-exact"),
    "a readiness flag that forgets a completion class must be caught where nothing masks it; the \
     report was {:?}",
    report.violations()
  );
}

/// One store fault at the TAIL of a drain that already delivered real completions. A
/// `while let Some(Ok(_))` loop ends on it exactly as it ends on an empty queue, so the fault is
/// consumed as the loop's terminating condition and never reaches the report — and the next clean
/// drain records the pass under the very name that should have caught it.
#[test]
fn mutant_a_trailing_one_shot_poll_error_fails_by_name() {
  let report = check::engine(&mut JournalEngineSubject::faulting_once_at_the_tail_of_a_drain());
  assert!(
    report.failed("engine/poll-no-spurious-error"),
    "a store fault at the end of a drain must be reported, not read as quiescence; the report was \
     {:?}",
    report.violations()
  );
}

/// The caller's half of the `set_group_gen` contract, pinned from the caller's side: the kit only
/// ever writes working generations, so the fence that follows one must be a fence. An engine with
/// no release cap over bookkeeping that reached the reserved band answers `MERGED_FLOOR` — a
/// verdict that a lineage was absorbed away cluster-wide, forged by an ordinary local removal.
#[test]
fn mutant_an_uncapped_reserved_ceiling_fails_by_name() {
  let report = check::engine(&mut JournalEngineSubject::folding_an_uncapped_reserved_ceiling());
  assert!(
    report.failed("engine/lineage-record-rejects-the-reserved-band"),
    "a fence forged at the reserved terminal after a legal lineage record must be caught; the \
     report was {:?}",
    report.violations()
  );
}

// ---------------------------------------------------------------------------------------------
// The over-rejection guard: a fully synchronous log with a no-op barrier.
// ---------------------------------------------------------------------------------------------

/// A log that makes every submission durable inside the submitting call and releases its
/// completion there. Its `barrier` is the no-op `LogSubject` explicitly permits, and its probe
/// answers the visible tip because for this store the two are the same thing.
///
/// Nothing here is broken. It is the shape the suite must not reject: a staged-only expectation
/// applied unconditionally told a correct engine it was wrong, which is as much a defect in a
/// conformance suite as letting a broken one through.
#[derive(Debug, Default)]
struct SynchronousLog(ProbingLog);

impl LogStore for SynchronousLog {
  type Error = core::convert::Infallible;

  fn first_index(&self) -> Index {
    self.0.first_index()
  }

  fn last_index(&self) -> Index {
    self.0.last_index()
  }

  fn durable_index(&self) -> Option<Index> {
    self.0.durable_index()
  }

  fn term(&self, index: Index) -> Result<Term, Self::Error> {
    self.0.term(index)
  }

  fn entries(
    &self,
    range: core::ops::Range<Index>,
    max_bytes: u64,
  ) -> Result<EntriesRead<'_>, Self::Error> {
    self.0.entries(range, max_bytes)
  }

  fn submit_append(&mut self, id: OpId, entries: &[Entry]) {
    self.0.submit_append(id, entries);
    self.0.barrier();
  }

  fn compact(&mut self, up_to: Index) {
    self.0.compact(up_to);
    self.0.barrier();
  }

  fn restore(&mut self, last_index: Index, last_term: Term) {
    self.0.restore(last_index, last_term);
    self.0.barrier();
  }

  fn poll(&mut self) -> Option<Result<LogDone, Self::Error>> {
    self.0.poll()
  }

  fn has_pending(&self) -> bool {
    self.0.has_pending()
  }
}

#[derive(Debug, Default)]
struct SynchronousSubject(SynchronousLog);

impl LogSubject for SynchronousSubject {
  type Log = SynchronousLog;

  fn log(&mut self) -> &mut Self::Log {
    &mut self.0
  }

  fn barrier(&mut self) {
    // THE NO-OP the trait permits: everything this store owes is already on the medium.
  }
}

#[test]
fn a_fully_synchronous_no_op_barrier_log_conforms() {
  let report = check::log_store(&mut SynchronousSubject::default());
  report.assert_conformant();
  assert!(
    report.passed_check("log/durable-index-clamp"),
    "the clamp must be JUDGED for a synchronous store, against what it actually claimed durable — \
     not skipped, and not failed for answering honestly"
  );
  for name in report.passed() {
    assert!(
      !report.skipped().iter().any(|s| s.check == *name),
      "{name} is recorded as BOTH passed and skipped"
    );
  }
}

/// An engine that forgets its boot-epoch counter after a TORN tail and nowhere else, over a medium
/// whose boundary it will not name. The clean and unsynced-loss legs leave the epoch rule looking
/// answered, so only a torn leg can catch it — and those are exactly the legs whose IMAGE is
/// unknowable without the boundary. The image needs it; the epoch does not.
#[test]
fn mutant_epochs_rolled_back_only_by_a_torn_tail_fails_by_name() {
  let report = check::engine(&mut JournalEngineSubject::rolling_epochs_back_only_on_a_torn_tail());
  assert!(
    report.failed("engine/boot-epoch-never-repeats-across-a-reopen"),
    "an epoch reissued after a torn crash must be caught even where the image cannot be graded; \
     the report was {:?}",
    report.violations()
  );
}

// ---------------------------------------------------------------------------------------------
// The second over-rejection guard: durability at submit, completions at the barrier.
// ---------------------------------------------------------------------------------------------

/// A log that puts every submission on its medium inside the submitting call — its probe advances
/// there — while scheduling the ACKNOWLEDGMENT for the barrier. Nothing in the `LogStore` contract
/// couples the two: `poll` and `has_pending` describe completion-queue readiness, not durability.
///
/// This is the store an oracle that reads an empty drain as "nothing is durable" misclassifies as
/// staged and then rejects for answering `durable_index` honestly.
#[derive(Debug, Default)]
struct DurableAtSubmitLog {
  inner: ProbingLog,
  /// What this store REPORTS as durable — advanced at submit, never at the barrier.
  durable: Index,
  /// How far the report deliberately lags the medium until the barrier confirms. `durable_index`
  /// may under-answer, and durability is prefix-ordered WITHIN an append's entries as much as
  /// between appends, so a conservative store exposing a shorter prefix is telling the truth.
  lag: u64,
}

impl DurableAtSubmitLog {
  /// After any mutation the medium holds exactly the visible log, because the write went through
  /// synchronously. A re-baseline is the one exception: it leaves a view with no entries in it, so
  /// there is no index at which durable bytes and the visible log agree.
  fn settle(&mut self, rebaselined: bool) {
    self.durable = if rebaselined {
      Index::ZERO
    } else {
      Index::new(self.inner.last_index().get().saturating_sub(self.lag))
    };
  }
}

impl LogStore for DurableAtSubmitLog {
  type Error = core::convert::Infallible;

  fn first_index(&self) -> Index {
    self.inner.first_index()
  }

  fn last_index(&self) -> Index {
    self.inner.last_index()
  }

  fn durable_index(&self) -> Option<Index> {
    Some(self.durable)
  }

  fn term(&self, index: Index) -> Result<Term, Self::Error> {
    self.inner.term(index)
  }

  fn entries(
    &self,
    range: core::ops::Range<Index>,
    max_bytes: u64,
  ) -> Result<EntriesRead<'_>, Self::Error> {
    self.inner.entries(range, max_bytes)
  }

  fn submit_append(&mut self, id: OpId, entries: &[Entry]) {
    self.inner.submit_append(id, entries);
    self.settle(false);
  }

  fn compact(&mut self, up_to: Index) {
    self.inner.compact(up_to);
    self.settle(false);
  }

  fn restore(&mut self, last_index: Index, last_term: Term) {
    self.inner.restore(last_index, last_term);
    self.settle(true);
  }

  fn poll(&mut self) -> Option<Result<LogDone, Self::Error>> {
    self.inner.poll()
  }

  fn has_pending(&self) -> bool {
    self.inner.has_pending()
  }
}

#[derive(Debug, Default)]
struct DurableAtSubmitSubject(DurableAtSubmitLog);

impl LogSubject for DurableAtSubmitSubject {
  type Log = DurableAtSubmitLog;

  fn log(&mut self) -> &mut Self::Log {
    &mut self.0
  }

  fn barrier(&mut self) {
    // The completions are released here — and a conservative store stops holding back its report
    // at the same moment, because a completion IS the claim the probe was lagging behind.
    self.0.inner.barrier();
    self.0.durable = self.0.inner.last_index();
  }
}

#[test]
fn a_store_durable_at_submit_but_completing_at_the_barrier_conforms() {
  let report = check::log_store(&mut DurableAtSubmitSubject::default());
  report.assert_conformant();
  assert!(
    report.passed_check("log/durable-index-clamp"),
    "the clamp must be JUDGED against what the probe actually claims, not against what the \
     completion queue happens to have ready"
  );
  for name in report.passed() {
    assert!(
      !report.skipped().iter().any(|s| s.check == *name),
      "{name} is recorded as BOTH passed and skipped"
    );
  }
}

/// A cut that reaches past the end of the medium removes nothing from it, so what survives is
/// knowable without ever learning where the medium ends. Ungraded with the rest of the torn legs it
/// is where an engine can drop a hosted group unnoticed: no image is compared, and the two
/// membership readers agree with each other when both say "not hosted".
#[test]
fn mutant_dropping_a_group_on_the_no_op_cut_fails_by_name() {
  let report = check::engine(&mut JournalEngineSubject::dropping_a_group_on_the_no_op_cut());
  assert!(
    report.failed("engine/exactly-the-maximal-valid-prefix-survives"),
    "a group lost to a cut that cut nothing must be caught; the report was {:?}",
    report.violations()
  );
}

/// A counter that steps back by exactly ONE, not to zero. The incarnation handed out 1 and 2; the
/// reopen comes back handing out 2 — which clears the FIRST epoch it issued and aliases the second.
/// Comparing against the first epoch alone read that as an advance.
#[test]
fn mutant_epochs_rolled_back_by_one_fails_by_name() {
  let report = check::engine(&mut JournalEngineSubject::rolling_epochs_back_by_one());
  assert!(
    report.failed("engine/boot-epoch-never-repeats-across-a-reopen"),
    "a reopen reissuing the last epoch its predecessor handed out must be caught; the report was \
     {:?}",
    report.violations()
  );
}

/// The third honest regime: a store that persists at submit and REPORTS a shorter prefix than it
/// holds until the barrier confirms. `durable_index` is allowed to under-answer — that is what
/// makes it safe to fold into a watermark ahead of any completion — and durability is
/// prefix-ordered within one append's entries as much as between appends. Demanding the full
/// extent of any store that claimed past the staged boundary rejected exactly this store.
#[test]
fn a_conservatively_lagging_store_conforms() {
  let mut subject = DurableAtSubmitSubject::default();
  subject.0.lag = 2;
  let report = check::log_store(&mut subject);
  report.assert_conformant();
  assert!(
    report.passed_check("log/durable-index-clamp"),
    "a prefix shorter than the store's own medium is an honest answer, and the clamp must judge \
     it rather than reject it"
  );
}

/// An engine that advances its boot-epoch counter in memory and journals it only at the next
/// barrier. An epoch is handed out and USED immediately — a crash between the two forgets it, and
/// the reopen hands the same number out again, so a prior incarnation's retained completion sorts
/// at an id a live write mints.
#[test]
fn mutant_epochs_persisted_only_at_the_barrier_fails_by_name() {
  let report = check::engine(&mut JournalEngineSubject::persisting_epochs_at_the_barrier());
  assert!(
    report.failed("engine/an-issued-epoch-survives-an-unflushed-crash"),
    "an epoch that a crash before the next barrier forgets must be caught; the report was {:?}",
    report.violations()
  );
}

/// A reopened engine that hosts a group whose log it cannot read, on exactly the legs whose IMAGE
/// is unknowable without a medium boundary. Folding `Pending` and `Err` into "no entries" let the
/// image comparison — the only reader of that log — record nothing at all there, while a real
/// restart would poison the replica or wedge on it.
#[test]
fn mutant_an_unreadable_reopened_log_fails_by_name() {
  let report = check::engine(&mut JournalEngineSubject::unreadable_after_a_hidden_torn_cut());
  assert!(
    report.failed("engine/a-reopened-log-is-resident-and-readable"),
    "a hosted group whose log cannot be read must be caught even where the image cannot be \
     graded; the report was {:?}",
    report.violations()
  );
}

/// A CONFORMING engine whose records occupy a different number of bytes on every other
/// incarnation — padding, alignment, a version header. Its state is identical either way; only its
/// offsets move, and nothing in the contract promises they will not.
///
/// Learning the barrier boundaries from a SEPARATE probe engine and then classifying every cut of
/// every other engine with those numbers aims each cut at a region it is not in, and grades the
/// result against the wrong expectation.
#[test]
fn an_engine_with_alternating_record_sizes_conforms() {
  let report = check::engine(&mut JournalEngineSubject::with_alternating_record_sizes());
  report.assert_conformant();
  assert!(
    report.passed_check("engine/exactly-the-maximal-valid-prefix-survives"),
    "the torn legs must still be graded for it, against ITS OWN boundaries"
  );
}

/// A reopened log that answers its FIRST read with `Pending`, as a store fetching lazily does, and
/// queues a dead incarnation's acknowledgment while doing it. The suite's own retry poll is the
/// only reader positioned to see that completion — discarding what the poll returned made the
/// stale acknowledgment vanish, and the drains that run afterwards find an empty queue.
#[test]
fn mutant_manufacturing_on_lazy_recovery_fails_by_name() {
  let report = check::engine(&mut JournalEngineSubject::manufacturing_on_lazy_recovery());
  assert!(
    report.failed("engine/reopen-manufactures-no-completions"),
    "an acknowledgment queued behind a cold read must be caught by the poll that re-drives it; \
     the report was {:?}",
    report.violations()
  );
}

/// A reopened log claiming to retain 1..=3 and answering every read of that range with `Ready` and
/// no entries — on the legs whose image is unknowable without a boundary, where no later
/// comparison exists. A `Ready` response was labelled resident without ever being measured against
/// the range the log itself claimed, and the contract treats an empty in-range read at restart as
/// fatal: the replica cannot answer a peer's `prev_log_index` at all.
#[test]
fn mutant_claiming_a_range_it_serves_nothing_from_fails_by_name() {
  let report = check::engine(&mut JournalEngineSubject::claiming_a_range_it_serves_nothing_from());
  assert!(
    report.failed("engine/a-reopened-log-is-resident-and-readable"),
    "a claimed range served by nothing must be caught; the report was {:?}",
    report.violations()
  );
}

/// A half-barrier replay defect behind a boundary that only appears once a medium exists.
///
/// Choosing landmark cuts versus fixed ones from a `tail_len` sampled BEFORE any leg was opened
/// read `None` and fell back to 0, 1, 16, 64 — offsets that, against records far larger than that,
/// only ever produce pre-first-barrier or no-op outcomes. The cut aimed BETWEEN the two barriers,
/// the one that exposes half a barrier surviving, was never made.
#[test]
fn mutant_a_late_boundary_hiding_half_a_barrier_fails_by_name() {
  let report =
    check::engine(&mut JournalEngineSubject::per_group_framing_boundary_only_after_an_open());
  assert!(
    report.failed("engine/barrier-is-all-or-nothing-across-a-crash"),
    "a barrier that survives in halves must be caught wherever the boundary can be read at the \
     time the cut is aimed; the report was {:?}",
    report.violations()
  );
}

/// A reopened log that answers its FIRST read cold and its next one correctly. Re-driving to an
/// eventual `Ready` certified it — but the core's restart scans are RESIDENT-ONLY: `restart.rs`
/// treats a cold, empty or faulted in-range read during the synchronous lease-floor scan as
/// unretryable and poisons with `PoisonReason::LogRead`, because a retry would under-size the
/// floor. A store that needs a second ask at restart is one the core fail-stops on.
#[test]
fn mutant_a_cold_first_read_after_a_reopen_fails_by_name() {
  let report = check::engine(&mut JournalEngineSubject::cold_on_the_first_read_after_a_reopen());
  assert!(
    report.failed("engine/a-reopened-log-is-resident-and-readable"),
    "a cold read at restart is what production poisons on; the report was {:?}",
    report.violations()
  );
}

/// A reopened log reporting `first_index` 5 with `last_index` 3 — a gap a contiguous log cannot
/// have, and the residue of a partially-persisted re-baseline that lost its committed prefix. The
/// core poisons with `PoisonReason::OrphanedLog` on exactly that pair
/// (`reconcile_restart_log`: `first_index > last_index.next()`).
///
/// Saturating arithmetic made the claimed span zero, so an empty answer satisfied a vacuous
/// contiguity test and the shape read as resident.
#[test]
fn mutant_an_orphaned_reopened_range_fails_by_name() {
  let report = check::engine(&mut JournalEngineSubject::claiming_an_orphaned_range());
  assert!(
    report.failed("engine/a-reopened-log-is-resident-and-readable"),
    "bounds a contiguous log cannot have must be refused before they are read; the report was {:?}",
    report.violations()
  );
}

/// Half a barrier surviving a cut whose location the suite cannot know. A per-group journal cut
/// between the two groups' records leaves one group at the second barrier and the other at the
/// first — and with no boundary reported, WHICH offset that was is unknowable.
///
/// It does not need to be knowable. A barrier spans every hosted group, so whatever a crash left
/// behind is one of exactly three COMPLETE states — nothing, everything through the first barrier,
/// or everything through the second — and a mixture is none of them. Discarding the whole image on
/// those legs threw that away with the part that genuinely needed the cut location.
#[test]
fn mutant_half_a_barrier_behind_a_hidden_boundary_fails_by_name() {
  let report = check::engine(&mut JournalEngineSubject::per_group_framing_with_a_hidden_boundary());
  assert!(
    report.failed("engine/barrier-is-all-or-nothing-across-a-crash"),
    "a mixed image is not one of the three complete states, whatever the cut offset was; the \
     report was {:?}",
    report.violations()
  );
}

// ---------------------------------------------------------------------------------------------
// A store that persists a snapshot's metadata faithfully and its bytes as something else.
// ---------------------------------------------------------------------------------------------

/// Everything read before the barrier is exactly what was submitted. What reaches the medium is
/// the right metadata over the wrong bytes — the blob a restart decodes, and the blob a peer is
/// served during an install. The suite compared the meta after the barrier and never looked at the
/// bytes again, so a store could hand back a correct blob at submit and keep a different one.
#[derive(Debug, Default)]
struct CorruptsTheBlobAtTheBarrier {
  inner: ProbingStable,
  /// Set once the barrier has run: everything read before it was the truth.
  persisted: bool,
}

impl StableStore for CorruptsTheBlobAtTheBarrier {
  type NodeId = u64;
  type Error = StagingUnallocatable;

  fn hard_state(&self) -> HardState<u64> {
    self.inner.hard_state()
  }

  fn durable_hard_state(&self) -> Option<HardState<u64>> {
    self.inner.durable_hard_state()
  }

  fn submit_write(&mut self, id: OpId, hard_state: HardState<u64>) {
    self.inner.submit_write(id, hard_state);
  }

  fn submit_snapshot(&mut self, id: OpId, meta: SnapshotMeta<u64>, data: Bytes) {
    self.inner.submit_snapshot(id, meta, data);
  }

  fn snapshot(&self) -> Option<(SnapshotMeta<u64>, Bytes)> {
    // THE MUTATION: the metadata is kept exactly, and the bytes behind it are not the bytes that
    // were submitted. Everything read before the barrier was correct.
    self.inner.snapshot().map(|(meta, blob)| {
      if self.persisted {
        (meta, Bytes::from(std::vec![0x5au8; blob.len()]))
      } else {
        (meta, blob)
      }
    })
  }

  fn durable_snapshot(&self) -> Option<SnapshotMeta<u64>> {
    self.inner.durable_snapshot()
  }

  fn snapshot_chunk(
    &self,
    offset: u64,
    len: u64,
  ) -> Option<Result<(SnapshotMeta<u64>, u64, SnapshotChunkRead), Self::Error>> {
    let (meta, blob) = self.snapshot()?;
    let total = blob.len() as u64;
    let start = offset.min(total) as usize;
    let end = offset.saturating_add(len).min(total) as usize;
    Some(Ok((
      meta,
      total,
      SnapshotChunkRead::Ready(blob.slice(start..end)),
    )))
  }

  fn accept_snapshot_chunk(
    &mut self,
    meta: &SnapshotMeta<u64>,
    total_len: u64,
    offset: u64,
    data: &Bytes,
  ) -> Result<u64, Self::Error> {
    self
      .inner
      .accept_snapshot_chunk(meta, total_len, offset, data)
  }

  fn take_staged_snapshot(&mut self, meta: &SnapshotMeta<u64>) -> Option<Bytes> {
    self.inner.take_staged_snapshot(meta)
  }

  fn discard_snapshot_staging(&mut self) {
    self.inner.discard_snapshot_staging();
  }

  fn poll(&mut self) -> Option<Result<StableDone, Self::Error>> {
    self.inner.poll()
  }

  fn has_pending(&self) -> bool {
    self.inner.has_pending()
  }
}

#[derive(Debug, Default)]
struct CorruptsTheBlobSubject(CorruptsTheBlobAtTheBarrier);

impl StableSubject for CorruptsTheBlobSubject {
  type Stable = CorruptsTheBlobAtTheBarrier;

  fn stable(&mut self) -> &mut Self::Stable {
    &mut self.0
  }

  fn barrier(&mut self) {
    self.0.inner.barrier();
    self.0.persisted = true;
  }

  fn node_id(&self, n: u64) -> u64 {
    n
  }
}

#[test]
fn mutant_corrupting_the_blob_at_the_barrier_fails_by_name() {
  let report = check::stable_store(&mut CorruptsTheBlobSubject::default());
  assert!(
    report.failed("stable/durable-snapshot-blob-is-verbatim"),
    "a snapshot whose bytes changed on the way to the medium must be caught; the report was {:?}",
    report.violations()
  );
}
