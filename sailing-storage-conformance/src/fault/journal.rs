//! A durable engine over the crash seam — the kit's own conformance subject.
//!
//! [`JournalEngine`] is a TEST FIXTURE, not the production disk engine: it exists so the kit's
//! crash half runs against something that genuinely survives a crash, and so a deliberately broken
//! copy of it can prove those checks have teeth. It is nonetheless a real design — one
//! length-prefixed, CRC'd record per BARRIER, written and synced BEFORE any completion is released,
//! replayed as complete barriers only — because a fixture that cheated would prove nothing.

use crate::fault::{
  codec::{DecodeFault, FieldReader, FieldStep, FieldWriter, ReferenceCodec, crc32},
  probe::{ProbingLog, ProbingStable, StagingUnallocatable},
  vfs::{Device, DeviceError},
};
use bytes::Bytes;
use sailing_proto::{
  Entry, FloorStore, GroupStores, HardState, Index, LogStore, MultiEngine, SnapshotMeta,
  StableStore, Term,
};
use std::{
  cell::RefCell,
  collections::{BTreeMap, btree_map::Entry as MapEntry},
  rc::Rc,
  vec::Vec,
};

/// A fatal fault from a [`JournalEngine`]'s stores.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum JournalStorageError {
  /// The engine's journal device refused a write, so the engine FAIL-STOPPED: a barrier it could
  /// not put on the medium released nothing, and every later poll reports this instead. Terminal by
  /// construction — a host has no recovery for a half-applied barrier spanning every group it hosts.
  DeviceFailed,
  /// A snapshot transfer declared a `total_len` the staging buffer cannot hold.
  StagingUnallocatable {
    /// The transfer's declared blob length.
    total_len: u64,
  },
}

impl core::fmt::Display for JournalStorageError {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self {
      Self::DeviceFailed => f.write_str("the journal device failed; the engine has fail-stopped"),
      Self::StagingUnallocatable { total_len } => write!(
        f,
        "snapshot staging cannot hold the declared blob ({total_len} bytes)"
      ),
    }
  }
}

impl core::error::Error for JournalStorageError {}

impl From<StagingUnallocatable> for JournalStorageError {
  fn from(e: StagingUnallocatable) -> Self {
    Self::StagingUnallocatable {
      total_len: e.total_len,
    }
  }
}

/// When the journal record backing a barrier reaches the medium.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum JournalPersistence {
  /// Written and synced BEFORE `flush` returns, which is the contract: a released completion is a
  /// promise the crash-surviving state already backs it.
  #[default]
  AtBarrier,
  /// Deferred until something polls a completion. Deliberately WRONG, and a real shape — an engine
  /// that treats the poll as the flush point. `flush` returns having released completions for work
  /// no crash would find, so a crash BEFORE the drain loses a barrier that already reported done.
  AtPoll,
}

/// What recovery does with the bytes past the last complete record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum JournalRecovery {
  /// Replay the MAXIMAL VALID PREFIX: every complete, checksum-valid, sequence-contiguous record,
  /// and cut the torn remainder off the medium.
  #[default]
  MaximalValidPrefix,
  /// Discard EVERYTHING on finding a torn tail. Deliberately WRONG: a torn tail is the ordinary
  /// end of every crashed log, so this throws away every barrier the engine ever acknowledged
  /// rather than the one it failed to finish.
  EmptyOnTear,
}

/// How the write-ahead log frames what a barrier writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum JournalFraming {
  /// ONE record per barrier, with one CRC over the whole batch. Recovery replays complete barriers
  /// only, so a crash mid-record loses the whole barrier — which is the point: a barrier is
  /// all-or-nothing across every group it spans.
  #[default]
  PerBarrier,
  /// One record per OPERATION. Deliberately WRONG, and kept because it is the mistake a WAL
  /// actually makes: every record is individually well-framed and CRC-valid, so a crash between
  /// two operations of one barrier leaves HALF A BARRIER durable, and recovery happily replays it.
  PerOperation,
  /// One record per GROUP. Deliberately WRONG in the same way, one level up: a barrier spans every
  /// group the engine hosts, so cutting between two groups' records leaves one group at the new
  /// barrier and another at the old — a cross-group state the engine was never in.
  PerGroup,
}

/// The deliberate defects a [`JournalEngine`] can be built with. Every field defaults to the
/// CONFORMING behaviour; each non-default value is one real mistake, kept so the suites can be
/// shown rejecting it.
#[derive(Debug, Clone, Copy, Default)]
pub struct JournalDefects {
  /// How a barrier is framed on the medium.
  pub framing: JournalFraming,
  /// When the record reaches the medium.
  pub persistence: JournalPersistence,
  /// What recovery keeps after a torn tail.
  pub recovery: JournalRecovery,
  /// Drop entry payloads and snapshot blobs from the journal, keeping only their shape. The bytes
  /// come back empty, so a reopen reports content the engine never held.
  pub lossy_payloads: bool,
  /// Persist only the STRUCTURE of each shape, zeroing the self-describing fields: an entry's
  /// timestamp, lease window and wall timestamp; a hard state's `lease_support` promise, lineage
  /// token and founding generation; a snapshot meta's lease windows, read mode and fork provenance. Indices, terms, kinds
  /// and boundaries all round-trip, so a reopen looks right in every structural respect.
  pub strip_fields: bool,
  /// Keep the operations a refused record decoded BEFORE the refusal, instead of voiding the whole
  /// record. Deliberately WRONG: half a barrier, arriving through the decode path.
  pub partial_records: bool,
  /// After a REOPEN that replayed anything, answer `durable_index` with the top of the index space.
  /// Deliberately WRONG, and invisible to any image comparison: every field a reopen reads back is
  /// still correct.
  pub poison_reopened_durable_index: bool,
  /// After a REOPEN that replayed anything, answer `durable_hard_state` with a fabricated state.
  pub poison_reopened_durable_hard_state: bool,
  /// After a REOPEN, serve a snapshot from a slot nothing durable backs. Deliberately WRONG: the
  /// slot is what a peer's `InstallSnapshot` is answered from, so a boundary with no medium behind
  /// it ships a prefix this replica cannot stand behind after the next crash.
  pub ghost_reopened_snapshot: bool,
  /// After a REOPEN, serve the snapshot's IDENTITY with the fields OUTSIDE it cleared — the shape
  /// generation, lease windows and read mode all gone, the provenance token kept — while the durable
  /// reader still answers the real meta. Deliberately WRONG, and invisible to any comparison that
  /// rebuilds the visible slot out of the durable answer.
  pub reopened_snapshot_loses_fields: bool,
  /// Release every completion at SUBMIT while the durable readers move only at the barrier.
  /// Deliberately WRONG: a completion is a claim of durability, so an engine that hands one over
  /// ahead of its own fsync releases every gate the core fences on it.
  pub early_completions: bool,
  /// Swallow the log completions the barrier genuinely settles, so an early release is the ONLY
  /// acknowledgment an append ever gets. Paired with `early_completions` this keeps the delivered
  /// count exactly right — the evasion an at-consumption oracle must still catch.
  pub suppress_settled_log_completions: bool,
  /// After a REOPEN, rebuild the completion queues on the first barrier rather than at open: the
  /// reopened engine looks quiescent until something drives it, and then hands the new incarnation
  /// acknowledgments whose op ids the crashed one minted. Deliberately WRONG, and invisible to a
  /// single drain taken at the moment of reopen.
  pub manufacture_completions_lazily: bool,
  /// Journal every operation EXCEPT the lineage record, so the record is perfect while the engine
  /// lives and gone the moment it reopens. Deliberately WRONG: the record is the only reading of
  /// an id's incarnation that outlives the process, and a restore judged against a zero rebuilds a
  /// counter beneath the generation peers already stand at.
  pub forget_lineage_records: bool,
  /// After a REAL torn cut, claim `first_index` ABOVE `last_index.next()` — a shape a contiguous
  /// log cannot have — and answer reads of it with `Ready` and nothing. Deliberately WRONG: the
  /// core poisons with `PoisonReason::OrphanedLog` on exactly this pair, because it is the residue
  /// of a partially-persisted re-baseline that lost the committed prefix.
  pub orphaned_range_after_a_real_torn_cut: bool,
  /// After a REAL torn cut, claim a non-empty retained range and answer every read of it with
  /// `Ready` and NO entries. Deliberately WRONG: the contract treats an empty in-range read at
  /// restart as fatal, and a replica whose log says it holds 1..=3 while serving nothing cannot
  /// answer a peer's `prev_log_index` at all.
  pub empty_reads_after_a_real_torn_cut: bool,
  /// Answer the FIRST `entries` read after a reopen with `Pending` and every later one correctly —
  /// a store that fetches its log lazily. Deliberately WRONG at RESTART, whatever it is elsewhere:
  /// the core's restart scans are resident-only and treat a cold in-range read as fail-stop.
  pub cold_first_read_after_a_reopen: bool,
  /// Answer the FIRST `entries` read after a reopen with `Pending`, as a store fetching lazily
  /// does — and queue a prior incarnation's acknowledgment while doing it. Deliberately WRONG: the
  /// completion belongs to op ids the crashed incarnation minted, and the only reader positioned to
  /// see it is whoever consumes the poll that re-drives the cold read.
  pub manufacture_on_lazy_recovery: bool,
  /// After a REAL torn cut — one that removes bytes, not the no-op cut past the end — come back
  /// HOSTING a group whose every `entries` read answers with an error. Deliberately WRONG: a hosted group whose log cannot be read is
  /// a replica the core poisons or wedges on, and a reopen that reports one is not a recovery. The
  /// legs where this bites are exactly the ones whose IMAGE is unknowable without a boundary.
  pub unreadable_after_a_real_torn_cut: bool,
  /// Advance boot epochs in memory and persist them only at the next barrier. Deliberately WRONG:
  /// an epoch is handed out and USED immediately, so a crash before that barrier forgets it and
  /// the reopen hands the same number out again. Every scenario that flushes after issuing hides
  /// it completely.
  pub persist_epochs_at_flush: bool,
  /// Drop a hosted group on the ONE torn cut that reaches past the end of the medium and therefore
  /// cuts nothing. Deliberately WRONG, and invisible wherever that leg goes ungraded: the two
  /// membership readers agree with each other when both say "not hosted", so only comparing the
  /// image against what the barriers wrote can see it.
  pub drop_a_group_on_a_no_op_cut: bool,
  /// Roll every boot-epoch counter back by exactly ONE on reopen. Deliberately WRONG and far
  /// quieter than a reset: an incarnation that handed out 1 and 2 comes back handing out 2, which
  /// exceeds the FIRST epoch it issued and aliases the second.
  pub roll_back_epochs_by_one: bool,
  /// After a TORN-TAIL crash only, reopen with every boot-epoch counter rolled back to zero.
  /// Deliberately WRONG, and invisible to a clean or unsynced-loss reopen: an epoch handed out
  /// twice folds two incarnations onto one identity, so a prior incarnation's retained completion
  /// sorts at the same id a live write mints and releases the gate fencing it.
  pub roll_back_epochs_after_a_torn_tail: bool,
  /// Write one extra, state-neutral record on every other incarnation, so the same logical work
  /// leaves a medium of a DIFFERENT length each time it is opened. Not a defect: padding,
  /// alignment and version headers all do this, and nothing in the contract promises a subject's
  /// byte offsets are stable across opens.
  pub alternate_record_sizes: bool,
  /// Answer ONE `Some(Err(_))` at the tail of a drain that has already delivered real completions,
  /// BEFORE this engine's first barrier, then behave. Deliberately WRONG, and structurally
  /// invisible to a `while let Some(Ok(_))` loop: the error ends the drain exactly as `None` does,
  /// so a store fault is read as quiescence and a driver that would have torn down a sick replica
  /// keeps polling a healthy one. The pre-barrier window is the one a suite's own at-consumption
  /// drains own outright — no shared drain helper runs there — so a fault placed in it is caught
  /// only where the loop that meets it handles the error arm.
  pub trailing_one_shot_poll_error: bool,
  /// Fold the removal ceiling with a bare `saturating_add` and NO release cap, over bookkeeping
  /// that let a lineage record reach the reserved band. `+ 1` there yields `MERGED_FLOOR` itself.
  /// Deliberately WRONG: the caller contract says the record is a working generation, but an
  /// engine that neither validates on the way in nor bounds on the way out turns any drift into a
  /// forged global verdict — and every other generation the suite uses is far from the band, so
  /// nothing else notices.
  pub uncapped_reserved_ceiling: bool,
  /// Fold the removal ceiling by SATURATING at the reserved terminal instead of respecting the
  /// working bound: an engine that reserves one value rather than two believes the highest working
  /// generation is `MERGED_FLOOR - 1`, so a fence at the real top rounds up to the sentinel.
  /// Deliberately WRONG, and correct at every generation the rest of the suite uses — a
  /// `MERGED_FLOOR` floor is read as a GLOBAL proof that the lineage was absorbed away, so forging
  /// one from a LOCAL removal clears a live thaw obligation on every replica.
  pub saturate_ceiling_at_the_terminal: bool,
  /// Accept `set_snapshot_staging_cap` and ignore it. Deliberately WRONG: the declared size comes
  /// from a PEER, so an unenforced cap hands a remote node the allocation decision.
  pub ignore_staging_cap: bool,
  /// Answer `durable_index` with `None`, as an engine that declines the optional probe does. Its
  /// point is what it removes: with no probe there is NO in-process durable reader for the log at
  /// all, so an early acknowledgment can only be audited by crashing.
  pub hide_log_probe: bool,
  /// Leave this group OUT of every barrier: its staged work is neither journalled nor released,
  /// while every other group's half of the same barrier lands. Deliberately WRONG — a barrier
  /// spans every hosted group, and an engine that finishes one group's half and abandons another's
  /// has published a state no single barrier ever produced.
  pub stall_group: Option<u64>,
}

/// One journalled mutation. The set is closed: every way an engine's durable state can move is one
/// of these, which is what makes replay a faithful reconstruction rather than an approximation.
#[derive(Debug, Clone)]
enum Op {
  AddGroup(u64),
  RemoveGroup(u64),
  SetFloor(u64, u64),
  SetGen(u64, u64),
  BootEpoch(u64, u64),
  Append(u64, Vec<Entry>),
  Compact(u64, Index),
  Restore(u64, Index, Term),
  Write(u64, HardState<u64>),
  Snapshot(u64, SnapshotMeta<u64>, Bytes),
}

impl Op {
  /// The group every op names — the key `PerGroup` framing partitions on.
  const fn gid(&self) -> u64 {
    match self {
      Self::AddGroup(g)
      | Self::RemoveGroup(g)
      | Self::SetFloor(g, _)
      | Self::SetGen(g, _)
      | Self::BootEpoch(g, _)
      | Self::Append(g, _)
      | Self::Compact(g, _)
      | Self::Restore(g, _, _)
      | Self::Write(g, _)
      | Self::Snapshot(g, _, _) => *g,
    }
  }

  /// The op with its PAYLOAD dropped — entry data and the snapshot blob — keeping the shape. The
  /// deliberate field/blob loss: a reopen reports a log and a snapshot the engine never held.
  fn stripped(&self) -> Self {
    match self {
      Self::Append(g, entries) => Self::Append(
        *g,
        entries
          .iter()
          .map(|e| Entry::new(e.term(), e.index(), e.kind(), Bytes::new()))
          .collect(),
      ),
      Self::Snapshot(g, meta, _) => Self::Snapshot(*g, meta.clone(), Bytes::new()),
      other => other.clone(),
    }
  }

  /// The op with its SELF-DESCRIBING FIELDS zeroed and its structure intact — the shape a store
  /// persisting only `(index, term, kind, bytes)` produces. Every field dropped here is one whose
  /// loss is silent: an entry's lease window under-sizes a successor's commit-wait, a hard state's
  /// promise reopens the disruptive-vote hole, a meta's fork provenance stalls an install.
  fn field_stripped(&self) -> Self {
    match self {
      Self::Append(g, entries) => Self::Append(
        *g,
        entries
          .iter()
          .map(|e| Entry::new(e.term(), e.index(), e.kind(), e.data_bytes()))
          .collect(),
      ),
      Self::Write(g, hs) => Self::Write(
        *g,
        HardState::initial()
          .with_term(hs.term())
          .with_commit(hs.commit())
          .with_vote(hs.vote()),
      ),
      Self::Snapshot(g, meta, blob) => Self::Snapshot(
        *g,
        SnapshotMeta::new(meta.last_index(), meta.last_term(), meta.conf().clone())
          .with_shape_gen(meta.shape_gen()),
        blob.clone(),
      ),
      other => other.clone(),
    }
  }
}

const OP_ADD_GROUP: u8 = 1;
const OP_REMOVE_GROUP: u8 = 2;
const OP_SET_FLOOR: u8 = 3;
const OP_SET_GEN: u8 = 4;
const OP_BOOT_EPOCH: u8 = 5;
const OP_APPEND: u8 = 6;
const OP_COMPACT: u8 = 7;
const OP_RESTORE: u8 = 8;
const OP_WRITE: u8 = 9;
const OP_SNAPSHOT: u8 = 10;

fn put_u64(buf: &mut Vec<u8>, v: u64) {
  buf.extend_from_slice(&v.to_le_bytes());
}

/// A journalled blob: `[u64 len][bytes]`. SIXTY-FOUR BITS for the same reason the record header is
/// — a snapshot blob reaches four gibibytes, and a `u32` prefix wrapped there, so the operation
/// declared a length far below its own body inside an outer record whose length was correct. The
/// barrier synced, `SnapshotWritten` released, and the reopen followed the truncated INNER boundary
/// and cut away acknowledged state.
fn put_blob(buf: &mut Vec<u8>, blob: &[u8]) {
  buf.extend_from_slice(&crate::fault::codec::declared(blob.len()).to_le_bytes());
  buf.extend_from_slice(blob);
}

fn take_u64(payload: &[u8], at: &mut usize) -> Result<u64, DecodeFault> {
  let end = at.checked_add(8).ok_or(DecodeFault::ImplausibleLength)?;
  let bytes = payload.get(*at..end).ok_or(DecodeFault::ShortField)?;
  let v = u64::from_le_bytes(bytes.try_into().map_err(|_| DecodeFault::ShortField)?);
  *at = end;
  Ok(v)
}

fn take_blob<'a>(payload: &'a [u8], at: &mut usize) -> Result<&'a [u8], DecodeFault> {
  let len_end = at.checked_add(8).ok_or(DecodeFault::ImplausibleLength)?;
  let len_bytes = payload.get(*at..len_end).ok_or(DecodeFault::ShortField)?;
  // A declaration off the medium can name more bytes than this address space holds. Truncating it
  // into one that fits is how a corrupt length becomes a plausible record, so it is REFUSED.
  let len = crate::fault::codec::as_offset(u64::from_le_bytes(
    len_bytes.try_into().map_err(|_| DecodeFault::ShortField)?,
  ))?;
  let end = len_end
    .checked_add(len)
    .ok_or(DecodeFault::ImplausibleLength)?;
  // Length checked against the backing bytes BEFORE anything is sized from it.
  let out = payload
    .get(len_end..end)
    .ok_or(DecodeFault::ImplausibleLength)?;
  *at = end;
  Ok(out)
}

fn encode_op(op: &Op, out: &mut FieldWriter) {
  let mut buf = Vec::new();
  let tag = match op {
    Op::AddGroup(gid) => {
      put_u64(&mut buf, *gid);
      OP_ADD_GROUP
    }
    Op::RemoveGroup(gid) => {
      put_u64(&mut buf, *gid);
      OP_REMOVE_GROUP
    }
    Op::SetFloor(gid, v) => {
      put_u64(&mut buf, *gid);
      put_u64(&mut buf, *v);
      OP_SET_FLOOR
    }
    Op::SetGen(gid, v) => {
      put_u64(&mut buf, *gid);
      put_u64(&mut buf, *v);
      OP_SET_GEN
    }
    Op::BootEpoch(gid, v) => {
      put_u64(&mut buf, *gid);
      put_u64(&mut buf, *v);
      OP_BOOT_EPOCH
    }
    Op::Append(gid, entries) => {
      put_u64(&mut buf, *gid);
      // SIXTY-FOUR BITS, like every other length on this medium. Narrowed to 32 the count wrapped
      // to ZERO at 2^32 entries, and a zero-count append decodes as an EMPTY one while the entry
      // bytes behind it are ignored: acknowledged log data gone at the next reopen, under a record
      // whose own length and checksum both agree.
      buf.extend_from_slice(&crate::fault::codec::declared(entries.len()).to_le_bytes());
      for entry in entries {
        put_blob(&mut buf, &ReferenceCodec::encode_entry(entry));
      }
      OP_APPEND
    }
    Op::Compact(gid, up_to) => {
      put_u64(&mut buf, *gid);
      put_u64(&mut buf, up_to.get());
      OP_COMPACT
    }
    Op::Restore(gid, index, term) => {
      put_u64(&mut buf, *gid);
      put_u64(&mut buf, index.get());
      put_u64(&mut buf, term.get());
      OP_RESTORE
    }
    Op::Write(gid, hs) => {
      put_u64(&mut buf, *gid);
      put_blob(&mut buf, &ReferenceCodec::encode_hard_state(hs));
      OP_WRITE
    }
    Op::Snapshot(gid, meta, blob) => {
      put_u64(&mut buf, *gid);
      put_blob(&mut buf, &ReferenceCodec::encode_snapshot_meta(meta));
      put_blob(&mut buf, blob);
      OP_SNAPSHOT
    }
  };
  out.field(tag, &buf);
}

/// The width of a journalled blob's length prefix — the floor every entry costs, and the number the
/// append preflight must be written against. Named so the gate and the writer cannot drift apart
/// again.
const BLOB_HEADER_LEN: usize = 8;

fn decode_op(tag: u8, payload: &[u8]) -> Result<Op, DecodeFault> {
  let mut at = 0usize;
  let gid = take_u64(payload, &mut at)?;
  let op = match tag {
    OP_ADD_GROUP => Op::AddGroup(gid),
    OP_REMOVE_GROUP => Op::RemoveGroup(gid),
    OP_SET_FLOOR => Op::SetFloor(gid, take_u64(payload, &mut at)?),
    OP_SET_GEN => Op::SetGen(gid, take_u64(payload, &mut at)?),
    OP_BOOT_EPOCH => Op::BootEpoch(gid, take_u64(payload, &mut at)?),
    OP_APPEND => {
      let count_end = at.checked_add(8).ok_or(DecodeFault::ImplausibleLength)?;
      let count_bytes = payload.get(at..count_end).ok_or(DecodeFault::ShortField)?;
      let count = crate::fault::codec::as_offset(u64::from_le_bytes(
        count_bytes
          .try_into()
          .map_err(|_| DecodeFault::ShortField)?,
      ))?;
      at = count_end;
      // REFUSED BEFORE IT SIZES ANYTHING, against the header width that is actually written: every
      // entry costs at least its own eight-byte length prefix, so a count past that bound cannot be
      // honest. The bound said FOUR while the prefixes were widened to eight, which let a count
      // twice the truthful ceiling through the gate.
      let floor = count
        .checked_mul(BLOB_HEADER_LEN)
        .ok_or(DecodeFault::ImplausibleLength)?;
      if floor > payload.len().saturating_sub(at) {
        return Err(DecodeFault::ImplausibleLength);
      }
      // GROWN, NOT RESERVED. `with_capacity(count)` hands a declared number straight to the
      // allocator, so a record that passes the length gate and then fails on its first blob has
      // still made the process pay for every entry it merely CLAIMED. Pushing lets the allocation
      // track the entries that actually decoded.
      let mut entries = Vec::new();
      for _ in 0..count {
        entries.push(ReferenceCodec::decode_entry(take_blob(payload, &mut at)?)?);
      }
      Op::Append(gid, entries)
    }
    OP_COMPACT => Op::Compact(gid, Index::new(take_u64(payload, &mut at)?)),
    OP_RESTORE => Op::Restore(
      gid,
      Index::new(take_u64(payload, &mut at)?),
      Term::new(take_u64(payload, &mut at)?),
    ),
    OP_WRITE => Op::Write(
      gid,
      ReferenceCodec::decode_hard_state(take_blob(payload, &mut at)?)?,
    ),
    OP_SNAPSHOT => {
      let meta = ReferenceCodec::decode_snapshot_meta(take_blob(payload, &mut at)?)?;
      let blob = Bytes::copy_from_slice(take_blob(payload, &mut at)?);
      Op::Snapshot(gid, meta, blob)
    }
    _ => return Err(DecodeFault::UnknownDiscriminant),
  };
  // EXACTLY CONSUMED. A decode that stops short leaves bytes the writer put there unaccounted for,
  // which is the shape a wrapped count takes: the operation reads as complete and the entries
  // behind it are simply ignored. The record's own checksum cannot see that — the bytes are the
  // ones written, they are just not all read.
  if at != payload.len() {
    return Err(DecodeFault::TrailingBytes);
  }
  Ok(op)
}

/// The journal device plus everything write-side about it: the sequence, the framing, and the
/// FAIL-STOP latch. Shared between the engine and the store handles it lends, because a failed
/// barrier has to be visible from both — the engine stops releasing, the stores start poisoning.
#[derive(Debug)]
struct JournalSink<D> {
  device: D,
  seq: u64,
  defects: JournalDefects,
  failed: bool,
  /// Set by the subject when it reopens after a REAL torn cut, for
  /// [`JournalDefects::unreadable_after_a_real_torn_cut`].
  after_a_real_torn_cut: bool,
  /// Whether this engine has run a barrier yet. Only
  /// [`JournalDefects::trailing_one_shot_poll_error`] reads it, to place its fault in the one
  /// window a pre-barrier drain owns.
  flushed: bool,
}

impl<D: Device> JournalSink<D> {
  /// Write `ops` as this framing dictates and SYNC. Latches the fail-stop on any device fault.
  fn write(&mut self, ops: &[Op]) -> Result<(), DeviceError> {
    if self.failed {
      return Err(DeviceError);
    }
    let ops: Vec<Op> = match self.defects.stall_group {
      // The deliberate defect: this group's half never reaches the medium.
      Some(stalled) => ops
        .iter()
        .filter(|op| op.gid() != stalled)
        .cloned()
        .collect(),
      None => ops.to_vec(),
    };
    let ops = ops.as_slice();
    let batches: Vec<Vec<Op>> = match self.defects.framing {
      JournalFraming::PerBarrier => std::vec![ops.to_vec()],
      JournalFraming::PerOperation => ops.iter().map(|op| std::vec![op.clone()]).collect(),
      JournalFraming::PerGroup => {
        let mut by_group: BTreeMap<u64, Vec<Op>> = BTreeMap::new();
        for op in ops {
          by_group.entry(op.gid()).or_default().push(op.clone());
        }
        by_group.into_values().collect()
      }
    };
    let defects = self.defects;
    let write = |sink: &mut Self| -> Result<(), DeviceError> {
      // FRAMED FIRST, ALL OF IT. A record that cannot be framed must stop the barrier before any
      // of it reaches the medium — latching the fail-stop halfway through would leave the device
      // holding part of a barrier that never completes.
      let mut records = Vec::with_capacity(batches.len());
      for (n, batch) in batches.iter().enumerate() {
        let seq = sink.seq.checked_add(n as u64).ok_or(DeviceError)?;
        records.push(frame(seq, batch, defects).ok_or(DeviceError)?);
      }
      for record in records {
        sink.seq += 1;
        sink.device.append(&record).map_err(|_| DeviceError)?;
      }
      sink.device.sync().map_err(|_| DeviceError)
    };
    match write(self) {
      Ok(()) => Ok(()),
      Err(e) => {
        // FAIL-STOP. The barrier is not on the medium, so nothing it covers may become observable
        // — not now and not after any later call.
        self.failed = true;
        Err(e)
      }
    }
  }
}

/// Frame one record: `[u64 payload_len][u64 seq][ops...][u32 crc]`, or `None` when the payload's
/// length cannot be declared.
///
/// EVERY NESTING LAYER IS BUILT HERE, before the caller appends anything: the operation fields, the
/// blobs inside them, and this outer header. A layer that cannot state its own length refuses the
/// whole record, and the caller latches the fail-stop with nothing on the medium — the alternative
/// is a record whose outer length is right and whose inner boundaries are short, which passes its
/// checksum and cuts acknowledged state away at the next reopen.
///
/// The length is 64 bits WIDE ON PURPOSE. Narrowing it to 32 wrapped silently for a barrier past
/// four gibibytes — a snapshot blob reaches that — and the record then declared a length far below
/// its own body: the write succeeded, the completion released, and the reopen discarded the whole
/// thing as an invalid tail. State the engine had acknowledged simply vanished.
///
/// The sequence number is INSIDE the CRC input, so a recycled or stale block cannot pass as a
/// record of this position in the log, and a gap in the sequence ends replay as surely as a bad
/// checksum does.
fn frame(seq: u64, ops: &[Op], defects: JournalDefects) -> Option<Vec<u8>> {
  let mut payload = seq.to_le_bytes().to_vec();
  let mut writer = FieldWriter::default();
  for op in ops {
    let mut op = op.clone();
    if defects.lossy_payloads {
      op = op.stripped();
    }
    if defects.strip_fields {
      op = op.field_stripped();
    }
    encode_op(&op, &mut writer);
  }
  payload.extend_from_slice(&writer.finish());
  let declared = u64::try_from(payload.len()).ok()?;
  let mut record = declared.to_le_bytes().to_vec();
  record.extend_from_slice(&payload);
  record.extend_from_slice(&crc32(&payload).to_le_bytes());
  Some(record)
}

/// Replay every COMPLETE record from the front of `bytes`, stopping at the first that is short,
/// fails its checksum, or breaks the sequence — a torn tail, in each of the three shapes it takes.
fn replay(bytes: &[u8], partial_records: bool) -> (Vec<Op>, u64, u64) {
  let mut ops = Vec::new();
  let mut at = 0usize;
  let mut expected = 0u64;
  // EVERY OFFSET IS CHECKED. The lengths here come off the medium, so a corrupt one can be any
  // value a `u64` holds; an unchecked add would wrap rather than end the replay, and a decoder that
  // wraps reads a record out of bytes belonging to something else. A declared length too large for
  // this platform's address space ends the replay rather than truncating into one that fits.
  while let Some(len_bytes) = at.checked_add(8).and_then(|end| bytes.get(at..end)) {
    let declared = u64::from_le_bytes(match len_bytes.try_into() {
      Ok(a) => a,
      Err(_) => break,
    });
    let Ok(len) = usize::try_from(declared) else {
      break;
    };
    let Some(body) = at.checked_add(8) else {
      break;
    };
    let Some(payload) = body.checked_add(len).and_then(|end| bytes.get(body..end)) else {
      break;
    };
    let Some(crc_bytes) = body
      .checked_add(len)
      .and_then(|start| start.checked_add(4).map(|end| (start, end)))
      .and_then(|(start, end)| bytes.get(start..end))
    else {
      break;
    };
    let stored = u32::from_le_bytes(match crc_bytes.try_into() {
      Ok(a) => a,
      Err(_) => break,
    });
    if crc32(payload) != stored {
      break;
    }
    let Some(seq) = payload
      .get(..8)
      .and_then(|b| b.try_into().ok())
      .map(u64::from_le_bytes)
    else {
      break;
    };
    if seq != expected {
      break;
    }
    // THE RECORD IS DECODED INTO ITS OWN BATCH and committed only once EVERY operation in it has
    // validated. A checksum proves the bytes are the ones written; it proves nothing about whether
    // they decode. Appending straight to the result would leave a record's earlier operations
    // applied when a later one is refused — half a barrier arriving through the decode path
    // instead of the framing one, and invisible to every checksum.
    let mut batch = Vec::new();
    let mut record_ok = true;
    let mut reader = FieldReader::new(&payload[8..]);
    loop {
      match reader.next_field() {
        Ok(FieldStep::Done) => break,
        Ok(FieldStep::Field(tag, op_payload)) => match decode_op(tag, op_payload) {
          Ok(op) => batch.push(op),
          Err(_) => {
            record_ok = false;
            break;
          }
        },
        Err(_) => {
          record_ok = false;
          break;
        }
      }
    }
    if !record_ok {
      if partial_records {
        // The deliberate defect: keep whatever decoded before the refusal.
        ops.extend(batch);
      }
      // `at` still points BEFORE this record, so recovery cuts the medium here and the record is
      // never seen again.
      return (ops, expected, at as u64);
    }
    ops.extend(batch);
    expected += 1;
    let Some(next) = body.checked_add(len).and_then(|e| e.checked_add(4)) else {
      break;
    };
    at = next;
  }
  (ops, expected, at as u64)
}

/// One id's durable lineage: the incarnation counter, the admission floor, and the removal ceiling
/// left behind by stores that are already gone.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct LineageRecord {
  generation: u64,
  floor: u64,
  ceiling: u64,
}

impl LineageRecord {
  fn fold(&mut self, other: Self) {
    self.generation = self.generation.max(other.generation);
    self.floor = self.floor.max(other.floor);
    self.ceiling = self.ceiling.max(other.ceiling);
  }
}

type Batch = Rc<RefCell<Vec<Op>>>;
type Deferred = Rc<RefCell<Vec<Op>>>;
type Sink<D> = Rc<RefCell<JournalSink<D>>>;

/// The per-group log handle a [`JournalEngine`] lends: a [`ProbingLog`] whose mutations also enter
/// the engine's pending barrier batch.
#[derive(Debug)]
pub struct JournalLog<D> {
  inner: ProbingLog,
  gid: u64,
  batch: Batch,
  sink: Sink<D>,
  deferred: Deferred,
  /// Completions released at submit under [`JournalDefects::early_completions`].
  early: std::collections::VecDeque<sailing_proto::LogDone>,
  /// Whether this handle belongs to an engine that REPLAYED state, for the reopen-only probe
  /// defects.
  reopened: bool,
  /// Whether this handle has delivered a real completion, and whether its one-shot fault has
  /// fired — both for [`JournalDefects::trailing_one_shot_poll_error`].
  delivered_any: bool,
  one_shot_fired: bool,
  /// Whether the lazy-recovery answer has been given yet, and whether the acknowledgment it
  /// queued is still owed — both for [`JournalDefects::manufacture_on_lazy_recovery`]. `Cell`,
  /// because `entries` reads through a shared reference.
  lazy_read_answered: core::cell::Cell<bool>,
  lazy_completion_owed: core::cell::Cell<bool>,
}

impl<D: Device> JournalLog<D> {
  fn new(gid: u64, batch: Batch, sink: Sink<D>, deferred: Deferred) -> Self {
    Self {
      delivered_any: false,
      one_shot_fired: false,
      lazy_read_answered: core::cell::Cell::new(false),
      lazy_completion_owed: core::cell::Cell::new(false),
      inner: ProbingLog::new(),
      gid,
      batch,
      sink,
      deferred,
      early: std::collections::VecDeque::new(),
      reopened: false,
    }
  }

  fn barrier(&mut self) -> usize {
    self.inner.barrier()
  }

  fn has_staged(&self) -> bool {
    self.inner.has_staged()
  }
}

impl<D: Device> LogStore for JournalLog<D> {
  type Error = JournalStorageError;

  fn first_index(&self) -> Index {
    let sink = self.sink.borrow();
    if sink.defects.orphaned_range_after_a_real_torn_cut && sink.after_a_real_torn_cut {
      return Index::new(5);
    }
    drop(sink);
    self.inner.first_index()
  }

  fn last_index(&self) -> Index {
    let sink = self.sink.borrow();
    if sink.defects.orphaned_range_after_a_real_torn_cut && sink.after_a_real_torn_cut {
      // Two below `first_index`: a gap a contiguous log cannot have.
      return Index::new(3);
    }
    if sink.defects.empty_reads_after_a_real_torn_cut && sink.after_a_real_torn_cut {
      // The claim the empty read above contradicts.
      return Index::new(3);
    }
    drop(sink);
    self.inner.last_index()
  }

  fn durable_index(&self) -> Option<Index> {
    {
      let sink = self.sink.borrow();
      if sink.defects.orphaned_range_after_a_real_torn_cut && sink.after_a_real_torn_cut {
        // Kept consistent with the faked tip, so the ONLY thing wrong with this reopen is the pair
        // of bounds a contiguous log cannot have.
        return Some(Index::new(3));
      }
    }
    if self.sink.borrow().defects.hide_log_probe {
      return None;
    }
    if self.reopened && self.sink.borrow().defects.poison_reopened_durable_index {
      return Some(Index::new(u64::MAX));
    }
    self.inner.durable_index()
  }

  fn term(&self, index: Index) -> Result<Term, Self::Error> {
    self.inner.term(index).map_err(|e| match e {})
  }

  fn entries(
    &self,
    range: core::ops::Range<Index>,
    max_bytes: u64,
  ) -> Result<sailing_proto::EntriesRead<'_>, Self::Error> {
    let sink = self.sink.borrow();
    if sink.defects.unreadable_after_a_real_torn_cut && sink.after_a_real_torn_cut {
      // THE DEFECT: the group is hosted and its log cannot be read.
      return Err(JournalStorageError::DeviceFailed);
    }
    if (sink.defects.empty_reads_after_a_real_torn_cut
      || sink.defects.orphaned_range_after_a_real_torn_cut)
      && sink.after_a_real_torn_cut
    {
      // THE DEFECT: the range is claimed and the read of it is empty.
      return Ok(sailing_proto::EntriesRead::Ready(
        sailing_proto::MaybeOwned::Borrowed(&[]),
      ));
    }
    let lazy = sink.defects.manufacture_on_lazy_recovery;
    let cold_once = sink.defects.cold_first_read_after_a_reopen;
    drop(sink);
    if cold_once && self.reopened && !self.lazy_read_answered.get() {
      // THE DEFECT: one cold answer, then a perfectly correct one.
      self.lazy_read_answered.set(true);
      return Ok(sailing_proto::EntriesRead::Pending);
    }
    if lazy && self.reopened && !self.lazy_read_answered.get() {
      // THE DEFECT: the first read after a reopen kicks off "recovery", and the acknowledgment it
      // queues belongs to an incarnation that is gone.
      self.lazy_read_answered.set(true);
      self.lazy_completion_owed.set(true);
      return Ok(sailing_proto::EntriesRead::Pending);
    }
    self.inner.entries(range, max_bytes).map_err(|e| match e {})
  }

  fn submit_append(&mut self, id: sailing_proto::OpId, entries: &[Entry]) {
    self
      .batch
      .borrow_mut()
      .push(Op::Append(self.gid, entries.to_vec()));
    self.inner.submit_append(id, entries);
    if self.sink.borrow().defects.early_completions {
      self.early.push_back(sailing_proto::LogDone::Appended(id));
    }
  }

  fn compact(&mut self, up_to: Index) {
    self.batch.borrow_mut().push(Op::Compact(self.gid, up_to));
    self.inner.compact(up_to);
  }

  fn restore(&mut self, last_index: Index, last_term: Term) {
    self
      .batch
      .borrow_mut()
      .push(Op::Restore(self.gid, last_index, last_term));
    self.inner.restore(last_index, last_term);
  }

  fn poll(&mut self) -> Option<Result<sailing_proto::LogDone, Self::Error>> {
    if let Err(e) = settle_deferred(&self.sink, &self.deferred) {
      return Some(Err(e));
    }
    if self.lazy_completion_owed.replace(false) {
      self.delivered_any = true;
      return Some(Ok(sailing_proto::LogDone::Appended(
        sailing_proto::OpId::first_of_epoch(0),
      )));
    }
    if let Some(done) = self.early.pop_front() {
      self.delivered_any = true;
      return Some(Ok(done));
    }
    if self.sink.borrow().defects.suppress_settled_log_completions {
      while self.inner.poll().is_some() {}
      return None;
    }
    let settled = self.inner.poll().map(|r| r.map_err(|e| match e {}));
    match settled {
      Some(done) => {
        self.delivered_any = true;
        Some(done)
      }
      // THE DEFECT: one fault at the TAIL of a drain that already delivered, where a
      // `while let Some(Ok(_))` loop cannot tell it from the end of the queue.
      None
        if self.sink.borrow().defects.trailing_one_shot_poll_error
          && !self.sink.borrow().flushed
          && self.delivered_any
          && !self.one_shot_fired =>
      {
        self.one_shot_fired = true;
        Some(Err(JournalStorageError::DeviceFailed))
      }
      None => None,
    }
  }

  fn has_pending(&self) -> bool {
    if self.lazy_completion_owed.get() {
      return true;
    }
    if self.sink.borrow().defects.suppress_settled_log_completions {
      return self.sink.borrow().failed || !self.early.is_empty();
    }
    self.sink.borrow().failed || !self.early.is_empty() || self.inner.has_pending()
  }
}

/// Write whatever a deferred-persistence engine still owes the medium, and surface a latched
/// fail-stop. A conforming engine owes nothing here — its barrier already wrote.
fn settle_deferred<D: Device>(
  sink: &Sink<D>,
  deferred: &Deferred,
) -> Result<(), JournalStorageError> {
  let owed = core::mem::take(&mut *deferred.borrow_mut());
  if !owed.is_empty() {
    sink
      .borrow_mut()
      .write(&owed)
      .map_err(|_| JournalStorageError::DeviceFailed)?;
  }
  if sink.borrow().failed {
    return Err(JournalStorageError::DeviceFailed);
  }
  Ok(())
}

/// The per-group stable handle a [`JournalEngine`] lends — the [`JournalLog`] of the stable side.
#[derive(Debug)]
pub struct JournalStable<D> {
  inner: ProbingStable,
  gid: u64,
  batch: Batch,
  sink: Sink<D>,
  deferred: Deferred,
  visible_meta_gen: u64,
  durable_meta_gen: u64,
  /// Completions released at submit under [`JournalDefects::early_completions`].
  early: std::collections::VecDeque<sailing_proto::StableDone>,
  /// Whether this handle belongs to an engine that REPLAYED state.
  reopened: bool,
}

impl<D: Device> JournalStable<D> {
  fn new(gid: u64, batch: Batch, sink: Sink<D>, deferred: Deferred) -> Self {
    Self {
      inner: ProbingStable::new(),
      gid,
      batch,
      sink,
      deferred,
      visible_meta_gen: 0,
      durable_meta_gen: 0,
      early: std::collections::VecDeque::new(),
      reopened: false,
    }
  }

  /// Fold replayed state into the durable slots, manufacturing no completion — see
  /// [`ProbingStable::settle_replayed`].
  fn settle_replayed(&mut self) {
    self.inner.settle_replayed();
    self.durable_meta_gen = self.visible_meta_gen;
  }

  fn barrier(&mut self) -> usize {
    let n = self.inner.barrier();
    if n != 0 {
      // The meta leg of the removal ceiling moves with the durable slot: the two are one
      // durability event seen twice.
      self.durable_meta_gen = self.visible_meta_gen;
    }
    n
  }

  fn has_staged(&self) -> bool {
    self.inner.has_staged()
  }

  /// The lineage the CURRENT snapshot slots claim — replaced with the slot, never accumulated, so
  /// a displaced meta stops counting exactly when it stops being what a reader would find.
  fn meta_ceiling(&self) -> u64 {
    self.visible_meta_gen.max(self.durable_meta_gen)
  }
}

impl<D: Device> StableStore for JournalStable<D> {
  type NodeId = u64;
  type Error = JournalStorageError;

  fn hard_state(&self) -> HardState<u64> {
    self.inner.hard_state()
  }

  fn durable_hard_state(&self) -> Option<HardState<u64>> {
    if self.reopened
      && self
        .sink
        .borrow()
        .defects
        .poison_reopened_durable_hard_state
    {
      return Some(HardState::initial().with_term(Term::new(u64::MAX)));
    }
    self.inner.durable_hard_state()
  }

  fn submit_write(&mut self, id: sailing_proto::OpId, hard_state: HardState<u64>) {
    self
      .batch
      .borrow_mut()
      .push(Op::Write(self.gid, hard_state.clone()));
    self.inner.submit_write(id, hard_state);
    if self.sink.borrow().defects.early_completions {
      self.early.push_back(sailing_proto::StableDone::Wrote(id));
    }
  }

  fn submit_snapshot(&mut self, id: sailing_proto::OpId, meta: SnapshotMeta<u64>, data: Bytes) {
    self
      .batch
      .borrow_mut()
      .push(Op::Snapshot(self.gid, meta.clone(), data.clone()));
    self.visible_meta_gen = meta.shape_gen();
    self.inner.submit_snapshot(id, meta, data);
    if self.sink.borrow().defects.early_completions {
      self
        .early
        .push_back(sailing_proto::StableDone::SnapshotWritten(id));
    }
  }

  fn snapshot(&self) -> Option<(SnapshotMeta<u64>, Bytes)> {
    if self.reopened {
      let defects = self.sink.borrow().defects;
      if defects.ghost_reopened_snapshot && self.inner.snapshot().is_none() {
        return Some((
          SnapshotMeta::new(
            Index::new(1),
            Term::new(1),
            sailing_proto::ConfState::from_voters([self.gid]),
          ),
          Bytes::from_static(b"ghost"),
        ));
      }
      if defects.reopened_snapshot_loses_fields {
        // Only the fields OUTSIDE `identity_eq` are dropped, so the slot still passes for the
        // same snapshot: a comparison that rebuilt the visible entry from the durable meta could
        // not see the difference at all.
        return self.inner.snapshot().map(|(meta, blob)| {
          let mut stripped =
            SnapshotMeta::new(meta.last_index(), meta.last_term(), meta.conf().clone());
          if let Some(fork) = meta.fork_id() {
            stripped = stripped.with_fork_id(fork.clone());
          }
          (stripped, blob)
        });
      }
    }
    self.inner.snapshot()
  }

  fn durable_snapshot(&self) -> Option<SnapshotMeta<u64>> {
    self.inner.durable_snapshot()
  }

  fn snapshot_chunk(
    &self,
    offset: u64,
    len: u64,
  ) -> Option<Result<(SnapshotMeta<u64>, u64, sailing_proto::SnapshotChunkRead), Self::Error>> {
    self
      .inner
      .snapshot_chunk(offset, len)
      .map(|r| r.map_err(Into::into))
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
      .map_err(Into::into)
  }

  fn take_staged_snapshot(&mut self, meta: &SnapshotMeta<u64>) -> Option<Bytes> {
    self.inner.take_staged_snapshot(meta)
  }

  fn discard_snapshot_staging(&mut self) {
    self.inner.discard_snapshot_staging();
  }

  fn poll(&mut self) -> Option<Result<sailing_proto::StableDone, Self::Error>> {
    if let Err(e) = settle_deferred(&self.sink, &self.deferred) {
      return Some(Err(e));
    }
    if let Some(done) = self.early.pop_front() {
      return Some(Ok(done));
    }
    self.inner.poll().map(|r| r.map_err(Into::into))
  }

  fn has_pending(&self) -> bool {
    self.sink.borrow().failed || !self.early.is_empty() || self.inner.has_pending()
  }
}

#[derive(Debug)]
struct GroupCell<D> {
  log: JournalLog<D>,
  stable: JournalStable<D>,
}

/// A durable multi-group engine over a [`Device`], with the same one-barrier semantics as the
/// in-tree reference engine and a write-ahead log behind it.
#[derive(Debug)]
pub struct JournalEngine<D> {
  groups: BTreeMap<u64, GroupCell<D>>,
  lineage: BTreeMap<u64, LineageRecord>,
  lineage_staged: BTreeMap<u64, LineageRecord>,
  /// Boot-epoch counters, kept BESIDE the hosted groups so a removal cannot reset one. A counter
  /// that restarted with a re-created id would hand two incarnations the same `(group, epoch)`
  /// identity, which is the collision the epoch exists to prevent.
  boot_epochs: BTreeMap<u64, u64>,
  batch: Batch,
  sink: Sink<D>,
  /// Ops a deferred-persistence engine owes the medium. Empty under the conforming setting.
  deferred: Deferred,
  barriers: u64,
  ops_batched: u64,
  /// The staging bound handed to every group store, current and future.
  staging_cap: usize,
}

impl<D: Device> JournalEngine<D> {
  /// Write one state-neutral record, so this incarnation's medium is a different LENGTH from the
  /// last one's for the same logical work. `BootEpoch(_, 0)` replays as `max(counter, 0)` — it
  /// changes nothing and occupies bytes, which is exactly the variable framing a real engine gets
  /// from padding, alignment or a version header.
  fn write_padding_record(&mut self) {
    let _ = self.sink.borrow_mut().write(&[Op::BootEpoch(1, 0)]);
  }

  /// Open over `device` with the conforming discipline, replaying whatever complete barriers it
  /// holds.
  pub fn open(device: D) -> Self {
    Self::open_with(device, JournalDefects::default())
  }

  /// Open over `device` with a chosen set of deliberate [`JournalDefects`].
  pub fn open_with(device: D, defects: JournalDefects) -> Self {
    let mut device = device;
    // A DEVICE THAT CANNOT BE READ IS NOT AN EMPTY DEVICE. Treating the fault as "no journal" is
    // the worst reading available: recovery would find no records, conclude the whole medium is a
    // torn tail, and truncate an intact journal to nothing. The engine fail-stops instead, on the
    // same latch every other device fault uses.
    let (bytes, mut poisoned) = match device.read_all() {
      Ok(bytes) => (bytes, false),
      Err(_) => (Vec::new(), true),
    };
    let (ops, next_seq, valid_len) = if poisoned {
      (Vec::new(), 0, 0)
    } else {
      replay(&bytes, defects.partial_records)
    };
    let torn = !poisoned && valid_len < device.len();
    // Cut the torn tail off before anything is appended after it. Bytes past the last COMPLETE
    // record are unreadable by construction — replay stops there — so a record written beyond them
    // would be durable and permanently invisible. A cut that FAILS is therefore not recoverable
    // by carrying on: every record this engine went on to acknowledge would sit behind an
    // unreadable tail, durable and permanently invisible.
    if torn && device.truncate(valid_len).is_err() {
      poisoned = true;
    }
    let ops = if poisoned {
      // A fail-stopped engine serves nothing: a partial view of a medium it could not finish
      // recovering is exactly the half-state the latch exists to refuse.
      Vec::new()
    } else {
      match defects.recovery {
        JournalRecovery::MaximalValidPrefix => ops,
        // The deliberate over-reaction: one torn record at the end throws every acknowledged
        // barrier away with it.
        JournalRecovery::EmptyOnTear if torn => Vec::new(),
        JournalRecovery::EmptyOnTear => ops,
      }
    };
    let mut engine = Self {
      groups: BTreeMap::new(),
      lineage: BTreeMap::new(),
      lineage_staged: BTreeMap::new(),
      boot_epochs: BTreeMap::new(),
      batch: Rc::new(RefCell::new(Vec::new())),
      sink: Rc::new(RefCell::new(JournalSink {
        device,
        seq: next_seq,
        defects,
        failed: poisoned,
        after_a_real_torn_cut: false,
        flushed: false,
      })),
      deferred: Rc::new(RefCell::new(Vec::new())),
      barriers: 0,
      ops_batched: 0,
      staging_cap: usize::MAX,
    };
    for op in ops {
      engine.apply(&op);
    }
    // Mark the handles as belonging to a REOPENED engine, so the reopen-only probe defects apply
    // to exactly the incarnation they are about.
    let replayed = !engine.groups.is_empty();
    for cell in engine.groups.values_mut() {
      cell.log.reopened = replayed;
      cell.stable.reopened = replayed;
    }
    // Replay reconstructs DURABLE state, and a reopened store owes acknowledgments to nobody: the
    // process that submitted this work is gone and every op id it minted with it. Settling rather
    // than barriering is what keeps the poll queues EMPTY on the far side of a crash.
    for cell in engine.groups.values_mut() {
      cell.log.inner.settle_replayed();
      cell.stable.settle_replayed();
    }
    while let Some((gid, staged)) = engine.lineage_staged.pop_first() {
      engine.lineage.entry(gid).or_default().fold(staged);
    }
    engine.batch.borrow_mut().clear();
    engine
  }

  /// Apply one replayed operation, bypassing the journal (these bytes are already on the medium).
  fn apply(&mut self, op: &Op) {
    match op {
      Op::AddGroup(gid) => {
        self.insert_group(*gid);
      }
      Op::RemoveGroup(gid) => {
        self.drop_group(gid);
      }
      Op::SetFloor(gid, floor) => self.stage_floor(*gid, *floor),
      Op::SetGen(gid, generation) => self.stage_gen(*gid, *generation),
      Op::BootEpoch(gid, epoch) => {
        let counter = self.boot_epochs.entry(*gid).or_default();
        *counter = (*counter).max(*epoch);
      }
      Op::Append(gid, entries) => {
        if let Some(cell) = self.groups.get_mut(gid) {
          cell
            .log
            .inner
            .submit_append(sailing_proto::OpId::ZERO, entries);
        }
      }
      Op::Compact(gid, up_to) => {
        if let Some(cell) = self.groups.get_mut(gid) {
          cell.log.inner.compact(*up_to);
        }
      }
      Op::Restore(gid, index, term) => {
        if let Some(cell) = self.groups.get_mut(gid) {
          cell.log.inner.restore(*index, *term);
        }
      }
      Op::Write(gid, hs) => {
        if let Some(cell) = self.groups.get_mut(gid) {
          cell
            .stable
            .inner
            .submit_write(sailing_proto::OpId::ZERO, hs.clone());
        }
      }
      Op::Snapshot(gid, meta, blob) => {
        if let Some(cell) = self.groups.get_mut(gid) {
          cell.stable.visible_meta_gen = meta.shape_gen();
          cell
            .stable
            .inner
            .submit_snapshot(sailing_proto::OpId::ZERO, meta.clone(), blob.clone());
        }
      }
    }
  }

  fn insert_group(&mut self, gid: u64) -> bool {
    match self.groups.entry(gid) {
      MapEntry::Occupied(_) => false,
      MapEntry::Vacant(v) => {
        let mut stable = JournalStable::new(
          gid,
          Rc::clone(&self.batch),
          Rc::clone(&self.sink),
          Rc::clone(&self.deferred),
        );
        // A group admitted AFTER the cap was set inherits it; otherwise the bound would silently
        // apply to the groups that happened to exist when the embedder called.
        stable.inner.set_staging_cap(self.staging_cap);
        v.insert(GroupCell {
          log: JournalLog::new(
            gid,
            Rc::clone(&self.batch),
            Rc::clone(&self.sink),
            Rc::clone(&self.deferred),
          ),
          stable,
        });
        true
      }
    }
  }

  fn drop_group(&mut self, gid: &u64) -> bool {
    let Some(cell) = self.groups.remove(gid) else {
      return false;
    };
    // The ceiling the departing stores held is INHERITED by the record: a fence exists to outlive
    // the group it fences, and staging it (rather than folding it straight in) keeps it visible to
    // `has_staged`, so a barrier is still owed for it.
    let inherited = cell.stable.meta_ceiling();
    if inherited != 0 {
      let record = self.lineage_staged.entry(*gid).or_default();
      record.ceiling = record.ceiling.max(inherited);
    }
    true
  }

  fn stage_floor(&mut self, gid: u64, floor: u64) {
    if floor == 0 {
      return;
    }
    let record = self.lineage_staged.entry(gid).or_default();
    record.floor = record.floor.max(floor);
  }

  fn stage_gen(&mut self, gid: u64, generation: u64) {
    if generation == 0 {
      return;
    }
    let record = self.lineage_staged.entry(gid).or_default();
    record.generation = record.generation.max(generation);
  }

  /// Put `ops` on the medium — before anything they cover becomes observable, unless this engine
  /// was built to defer that. Returns whether the write landed.
  fn journal(&mut self, ops: &[Op]) -> bool {
    if self.sink.borrow().defects.persistence == JournalPersistence::AtPoll {
      // The deliberate defect: the record is owed, not written. `flush` returns anyway.
      self.deferred.borrow_mut().extend_from_slice(ops);
      return true;
    }
    self.sink.borrow_mut().write(ops).is_ok()
  }

  /// Whether the engine has FAIL-STOPPED on a device fault. Terminal: no later barrier releases
  /// anything and every store poll reports [`JournalStorageError::DeviceFailed`].
  #[must_use]
  pub fn failed(&self) -> bool {
    self.sink.borrow().failed
  }
}

impl<D> JournalEngine<D> {
  fn floor_of(&self, gid: &u64) -> u64 {
    let durable = self.lineage.get(gid).map_or(0, |r| r.floor);
    let staged = self.lineage_staged.get(gid).map_or(0, |r| r.floor);
    durable.max(staged)
  }

  fn gen_of(&self, gid: &u64) -> u64 {
    let durable = self.lineage.get(gid).map_or(0, |r| r.generation);
    let staged = self.lineage_staged.get(gid).map_or(0, |r| r.generation);
    durable.max(staged)
  }
}

impl<D> FloorStore<u64> for JournalEngine<D> {
  fn floor(&self, gid: &u64) -> u64 {
    self.floor_of(gid)
  }

  fn lineage(&self, gid: &u64) -> u64 {
    self.gen_of(gid)
  }
}

impl<D> GroupStores<u64, JournalLog<D>, JournalStable<D>> for JournalEngine<D> {
  fn stores(&mut self, group: &u64) -> Option<(&mut JournalLog<D>, &mut JournalStable<D>)> {
    self
      .groups
      .get_mut(group)
      .map(|cell| (&mut cell.log, &mut cell.stable))
  }
}

impl<D: Device + core::fmt::Debug> MultiEngine<u64, u64> for JournalEngine<D> {
  type Log = JournalLog<D>;
  type Stable = JournalStable<D>;

  fn set_snapshot_staging_cap(&mut self, cap: usize) {
    // EVERY group, current and future. A cap the engine accepts and does not apply is worse than
    // none: the embedder believes its staging is bounded and the next oversized declaration
    // allocates anyway.
    if self.sink.borrow().defects.ignore_staging_cap {
      return;
    }
    self.staging_cap = cap;
    for cell in self.groups.values_mut() {
      cell.stable.inner.set_staging_cap(cap);
    }
  }

  fn group_ids(&self) -> impl Iterator<Item = &u64> {
    self.groups.keys()
  }

  fn barriers(&self) -> u64 {
    self.barriers
  }

  fn ops_batched(&self) -> u64 {
    self.ops_batched
  }

  fn has_staged(&self) -> bool {
    !self.batch.borrow().is_empty()
      || !self.lineage_staged.is_empty()
      || self
        .groups
        .values()
        .any(|cell| cell.log.has_staged() || cell.stable.has_staged())
  }

  fn flush(&mut self) -> usize {
    self.sink.borrow_mut().flushed = true;
    // DURABLE FIRST. Everything the batch describes reaches the medium and is synced before a
    // single completion is released, because a released completion is a promise the crash-surviving
    // state already backs it.
    let ops = core::mem::take(&mut *self.batch.borrow_mut());
    if !ops.is_empty() && !self.journal(&ops) {
      // The barrier is not on the medium. Release NOTHING — a completion is a promise the
      // crash-surviving state already backs it — and leave the engine fail-stopped.
      self.barriers += 1;
      return 0;
    }
    if self.sink.borrow().failed {
      self.barriers += 1;
      return 0;
    }
    let stalled = self.sink.borrow().defects.stall_group;
    // THE DEFECT: a reopened engine that looked quiescent queues an acknowledgment for the
    // incarnation that crashed, the first time anything drives it.
    if self.sink.borrow().defects.manufacture_completions_lazily
      && let Some(cell) = self.groups.values_mut().next()
    {
      cell.log.early.push_back(sailing_proto::LogDone::Appended(
        sailing_proto::OpId::first_of_epoch(0),
      ));
    }
    let mut released = 0;
    for (gid, cell) in &mut self.groups {
      if Some(*gid) == stalled {
        continue;
      }
      released += cell.log.barrier();
      released += cell.stable.barrier();
    }
    while let Some((gid, staged)) = self.lineage_staged.pop_first() {
      self.lineage.entry(gid).or_default().fold(staged);
      released += 1;
    }
    self.barriers += 1;
    self.ops_batched += released as u64;
    released
  }

  fn add_group(&mut self, gid: u64) -> bool {
    // A fail-stopped engine admits nothing: admission it can never make durable is a promise the
    // crash-surviving state will not back.
    if self.sink.borrow().failed || self.groups.contains_key(&gid) {
      return false;
    }
    self.batch.borrow_mut().push(Op::AddGroup(gid));
    self.insert_group(gid)
  }

  fn remove_group(&mut self, gid: &u64) -> bool {
    if self.sink.borrow().failed || !self.groups.contains_key(gid) {
      return false;
    }
    self.batch.borrow_mut().push(Op::RemoveGroup(*gid));
    self.drop_group(gid)
  }

  fn contains_group(&self, gid: &u64) -> bool {
    self.groups.contains_key(gid)
  }

  fn next_boot_epoch(&mut self, gid: &u64) -> Option<u64> {
    if !self.groups.contains_key(gid) {
      return None;
    }
    let counter = self.boot_epochs.entry(*gid).or_default();
    let next = counter.checked_add(1)?;
    *counter = next;
    // A boot epoch is handed out and USED immediately, so it cannot wait for the next barrier: an
    // epoch a crash forgot would be handed out twice and fold two incarnations onto one identity.
    // It gets its own synced record, whatever the barrier discipline.
    if self.sink.borrow().defects.persist_epochs_at_flush {
      // THE DEFECT: the counter moves in memory and the record waits for a barrier that a crash
      // may never bring.
      self.batch.borrow_mut().push(Op::BootEpoch(*gid, next));
      return Some(next);
    }
    if self
      .sink
      .borrow_mut()
      .write(&[Op::BootEpoch(*gid, next)])
      .is_err()
    {
      return None;
    }
    Some(next)
  }

  fn set_group_floor(&mut self, gid: &u64, floor: u64) {
    if floor == 0 {
      return;
    }
    self.batch.borrow_mut().push(Op::SetFloor(*gid, floor));
    self.stage_floor(*gid, floor);
  }

  fn set_group_gen(&mut self, gid: &u64, generation: u64) {
    if generation == 0 {
      return;
    }
    // THE DEFECT: the record moves in memory and never reaches the journal.
    if !self.sink.borrow().defects.forget_lineage_records {
      self.batch.borrow_mut().push(Op::SetGen(*gid, generation));
    }
    self.stage_gen(*gid, generation);
  }

  fn removal_floor(&self, gid: &u64) -> u64 {
    let mut ceiling = self.gen_of(gid).max(
      self
        .lineage
        .get(gid)
        .map_or(0, |r| r.ceiling)
        .max(self.lineage_staged.get(gid).map_or(0, |r| r.ceiling)),
    );
    if let Some(cell) = self.groups.get(gid) {
      // The SHAPE-ENTRY leg is missing here and cannot be supplied: decoding a lineage move out of
      // an entry's payload needs a codec sailing-proto keeps to itself, so an out-of-tree engine
      // has only the record and the snapshot meta to fold. The in-tree engine folds all three.
      ceiling = ceiling.max(cell.stable.meta_ceiling());
    }
    if ceiling == 0 {
      0
    } else if self.sink.borrow().defects.uncapped_reserved_ceiling && self.gen_of(gid) != 0 {
      // THE MUTATION: a LINEAGE-RECORD ceiling that drifted into the reserved band, folded with no
      // cap. Scoped to the record leg so the snapshot-meta leg — and the id the terminal-ceiling
      // check watches — answers exactly as a conforming engine would.
      ceiling
        .max(sailing_proto::HIGHEST_WORKING_GENERATION)
        .saturating_add(1)
    } else if self.sink.borrow().defects.saturate_ceiling_at_the_terminal
      && ceiling.saturating_add(1) >= sailing_proto::HIGHEST_WORKING_GENERATION
    {
      // THE MUTATION: the top of the working range is rounded up to the terminal.
      sailing_proto::MERGED_FLOOR
    } else {
      ceiling.saturating_add(1)
    }
  }
}

/// An [`EngineSubject`](crate::check::EngineSubject) over [`JournalEngine`]: the kit's durable
/// tier, and the base every crash-half red-proof breaks a copy of.
#[derive(Debug)]
pub struct JournalEngineSubject {
  /// How many incarnations this subject has handed out. Only
  /// [`JournalEngineSubject::with_alternating_record_sizes`] reads it.
  opens: u64,
  vfs: crate::fault::SharedVfs,
  defects: JournalDefects,
  honours_sync: bool,
  /// Whether [`EngineSubject::tail_len`](crate::check::EngineSubject::tail_len) reports the
  /// medium's boundary. False models a subject whose device length the suite cannot see.
  reports_boundary: bool,
  /// Answer the FIRST `tail_len` with `None` and every later one with the real boundary. Nothing
  /// in the trait promises the answer's SHAPE is stable — a subject may not know its medium's
  /// length until it has one to measure — and a suite that samples the capability once, before any
  /// crash-leg medium exists, treats that first answer as the subject's permanent capability.
  /// `Cell`, because `tail_len` reads through a shared reference.
  boundary_after_the_first_ask: core::cell::Cell<bool>,
}

impl Default for JournalEngineSubject {
  fn default() -> Self {
    Self::new()
  }
}

impl JournalEngineSubject {
  /// A conforming subject: one record per barrier, over a device that really syncs.
  #[must_use]
  pub fn new() -> Self {
    Self {
      opens: 0,
      vfs: crate::fault::SharedVfs::new(),
      defects: JournalDefects::default(),
      honours_sync: true,
      reports_boundary: true,
      boundary_after_the_first_ask: core::cell::Cell::new(false),
    }
  }

  /// A subject that TEARS like any other but never reports where its records sit. The suite cannot
  /// name which barriers a given cut leaves behind, so every torn-tail leg is unverifiable for it.
  #[must_use]
  pub fn hiding_its_boundary() -> Self {
    Self {
      reports_boundary: false,
      ..Self::new()
    }
  }

  /// A subject built with a chosen set of deliberate [`JournalDefects`].
  #[must_use]
  pub fn with_defects(defects: JournalDefects) -> Self {
    Self {
      defects,
      ..Self::new()
    }
  }

  /// A subject whose journal device SILENTLY DROPS every sync — the no-fsync red-proof.
  #[must_use]
  pub fn never_syncing() -> Self {
    Self {
      honours_sync: false,
      ..Self::new()
    }
  }

  /// A subject whose journal frames one record per OPERATION instead of per barrier, so a torn
  /// tail can leave half a barrier well-framed and replayable — the barrier-atomicity red-proof.
  #[must_use]
  pub fn framing_per_operation() -> Self {
    Self::with_defects(JournalDefects {
      framing: JournalFraming::PerOperation,
      ..JournalDefects::default()
    })
  }

  /// A subject whose journal frames one record per GROUP, so a cut between two groups' records
  /// leaves one group at the new barrier and another at the old.
  #[must_use]
  pub fn framing_per_group() -> Self {
    Self::with_defects(JournalDefects {
      framing: JournalFraming::PerGroup,
      ..JournalDefects::default()
    })
  }

  /// A subject that acknowledges an append at SUBMIT, swallows the completion its barrier really
  /// settles, and offers no `durable_index` — an early claim with no in-process auditor, and the
  /// delivered count still exactly right.
  #[must_use]
  pub fn acknowledging_appends_it_cannot_prove() -> Self {
    Self::with_defects(JournalDefects {
      early_completions: true,
      suppress_settled_log_completions: true,
      hide_log_probe: true,
      ..JournalDefects::default()
    })
  }

  /// A subject that acknowledges at submit and answers one fault at the tail of the pre-barrier
  /// drain that consumes those acknowledgments.
  #[must_use]
  pub fn faulting_once_at_the_tail_of_a_drain() -> Self {
    Self::with_defects(JournalDefects {
      early_completions: true,
      trailing_one_shot_poll_error: true,
      ..JournalDefects::default()
    })
  }

  /// A subject that hides its medium's boundary and drops a hosted group on the one torn cut that
  /// cuts nothing at all.
  #[must_use]
  pub fn dropping_a_group_on_the_no_op_cut() -> Self {
    Self {
      reports_boundary: false,
      ..Self::with_defects(JournalDefects {
        drop_a_group_on_a_no_op_cut: true,
        ..JournalDefects::default()
      })
    }
  }

  /// A subject whose journal frames one record per GROUP — so a cut between two groups' records
  /// leaves one group at the new barrier and another at the old — and which reports no boundary
  /// until it has been opened. Both halves are legitimate on their own; together they hide the
  /// defect from a suite that samples the boundary capability once, before any medium exists,
  /// because the offset that splits the two groups is far past the fixed spread's reach.
  #[must_use]
  pub fn per_group_framing_boundary_only_after_an_open() -> Self {
    Self {
      boundary_after_the_first_ask: core::cell::Cell::new(true),
      ..Self::with_defects(JournalDefects {
        framing: JournalFraming::PerGroup,
        ..JournalDefects::default()
      })
    }
  }

  /// A subject whose journal frames one record per GROUP and which never reports its boundary. A
  /// cut between the two groups' records leaves one group at the new barrier and the other at the
  /// old — half a barrier — and no fixed offset the suite can aim without a boundary tells it
  /// WHERE that cut landed. What survived is still one of three complete states or it is not.
  #[must_use]
  pub fn per_group_framing_with_a_hidden_boundary() -> Self {
    Self {
      reports_boundary: false,
      ..Self::with_defects(JournalDefects {
        framing: JournalFraming::PerGroup,
        ..JournalDefects::default()
      })
    }
  }

  /// A subject that hides its boundary and, after a real torn cut, reports bounds a contiguous log
  /// cannot have.
  #[must_use]
  pub fn claiming_an_orphaned_range() -> Self {
    Self {
      reports_boundary: false,
      ..Self::with_defects(JournalDefects {
        orphaned_range_after_a_real_torn_cut: true,
        ..JournalDefects::default()
      })
    }
  }

  /// A subject that hides its boundary and, after a real torn cut, claims a retained range it then
  /// serves nothing from.
  #[must_use]
  pub fn claiming_a_range_it_serves_nothing_from() -> Self {
    Self {
      reports_boundary: false,
      ..Self::with_defects(JournalDefects {
        empty_reads_after_a_real_torn_cut: true,
        ..JournalDefects::default()
      })
    }
  }

  /// A subject whose first read after a reopen is cold and whose next one is correct.
  #[must_use]
  pub fn cold_on_the_first_read_after_a_reopen() -> Self {
    Self::with_defects(JournalDefects {
      cold_first_read_after_a_reopen: true,
      ..JournalDefects::default()
    })
  }

  /// A subject whose first read after a reopen answers `Pending` and queues a dead incarnation's
  /// acknowledgment on the way.
  #[must_use]
  pub fn manufacturing_on_lazy_recovery() -> Self {
    Self::with_defects(JournalDefects {
      manufacture_on_lazy_recovery: true,
      ..JournalDefects::default()
    })
  }

  /// A subject whose records occupy a different number of bytes on every other incarnation. Its
  /// state is identical either way; only its offsets move.
  #[must_use]
  pub fn with_alternating_record_sizes() -> Self {
    Self::with_defects(JournalDefects {
      alternate_record_sizes: true,
      ..JournalDefects::default()
    })
  }

  /// A subject that hides its boundary AND cannot read a reopened log after a real torn cut — the
  /// legs whose image is unknowable without that boundary.
  #[must_use]
  pub fn unreadable_after_a_hidden_torn_cut() -> Self {
    Self {
      reports_boundary: false,
      ..Self::with_defects(JournalDefects {
        unreadable_after_a_real_torn_cut: true,
        ..JournalDefects::default()
      })
    }
  }

  /// A subject that advances boot epochs in memory and journals them only at its next barrier.
  #[must_use]
  pub fn persisting_epochs_at_the_barrier() -> Self {
    Self::with_defects(JournalDefects {
      persist_epochs_at_flush: true,
      ..JournalDefects::default()
    })
  }

  /// A subject whose reopened counters step back by exactly one.
  #[must_use]
  pub fn rolling_epochs_back_by_one() -> Self {
    Self::with_defects(JournalDefects {
      roll_back_epochs_by_one: true,
      ..JournalDefects::default()
    })
  }

  /// A subject that hides its medium's boundary AND rolls its boot-epoch counters back after a
  /// torn tail — the two together, because a hidden boundary is what makes a torn leg's IMAGE
  /// unjudgeable, and the epoch rule must survive that.
  #[must_use]
  pub fn rolling_epochs_back_only_on_a_torn_tail() -> Self {
    Self {
      reports_boundary: false,
      ..Self::with_defects(JournalDefects {
        roll_back_epochs_after_a_torn_tail: true,
        ..JournalDefects::default()
      })
    }
  }

  /// A subject offering ONE of the two optional probes: its log declines `durable_index` while its
  /// stable store answers `durable_hard_state`. Not a defect — the probes are independent
  /// capabilities and an engine may offer either — which is exactly why the two batteries' probe
  /// properties cannot share a name.
  #[must_use]
  pub fn offering_only_the_hard_state_probe() -> Self {
    Self::with_defects(JournalDefects {
      hide_log_probe: true,
      ..JournalDefects::default()
    })
  }

  /// A subject whose lineage-record fence is folded with no release cap over a record that reached
  /// the reserved band.
  #[must_use]
  pub fn folding_an_uncapped_reserved_ceiling() -> Self {
    Self::with_defects(JournalDefects {
      uncapped_reserved_ceiling: true,
      ..JournalDefects::default()
    })
  }

  /// A subject whose removal fence rounds up to the reserved terminal at the top of the working
  /// range — the sentinel forgery the product reserves two generations to make impossible.
  #[must_use]
  pub fn saturating_the_ceiling_at_the_terminal() -> Self {
    Self::with_defects(JournalDefects {
      saturate_ceiling_at_the_terminal: true,
      ..JournalDefects::default()
    })
  }

  /// A subject whose reopened engine looks quiescent until its first barrier, and then owes
  /// acknowledgments to an incarnation that is gone.
  #[must_use]
  pub fn manufacturing_completions_lazily() -> Self {
    Self::with_defects(JournalDefects {
      manufacture_completions_lazily: true,
      ..JournalDefects::default()
    })
  }

  /// A subject whose lineage records never reach the medium.
  #[must_use]
  pub fn forgetting_the_lineage_record() -> Self {
    Self::with_defects(JournalDefects {
      forget_lineage_records: true,
      ..JournalDefects::default()
    })
  }

  /// A subject that accepts a staging cap and never applies it.
  #[must_use]
  pub fn ignoring_the_staging_cap() -> Self {
    Self::with_defects(JournalDefects {
      ignore_staging_cap: true,
      ..JournalDefects::default()
    })
  }

  /// A subject whose reopened engine serves a snapshot from a slot nothing durable backs.
  #[must_use]
  pub fn ghosting_the_reopened_snapshot() -> Self {
    Self::with_defects(JournalDefects {
      ghost_reopened_snapshot: true,
      ..JournalDefects::default()
    })
  }

  /// A subject whose reopened engine serves the snapshot's identity with every self-describing
  /// field cleared, while its durable reader still answers the real meta.
  #[must_use]
  pub fn losing_the_reopened_snapshots_fields() -> Self {
    Self::with_defects(JournalDefects {
      reopened_snapshot_loses_fields: true,
      ..JournalDefects::default()
    })
  }

  /// A subject whose reopened engine answers `durable_index` with the top of the index space.
  #[must_use]
  pub fn poisoning_the_reopened_index_probe() -> Self {
    Self::with_defects(JournalDefects {
      poison_reopened_durable_index: true,
      ..JournalDefects::default()
    })
  }

  /// A subject whose reopened engine answers `durable_hard_state` with a fabricated state.
  #[must_use]
  pub fn poisoning_the_reopened_hard_state_probe() -> Self {
    Self::with_defects(JournalDefects {
      poison_reopened_durable_hard_state: true,
      ..JournalDefects::default()
    })
  }

  /// A subject whose completions are pollable at SUBMIT while its durable readers move only at the
  /// barrier.
  #[must_use]
  pub fn releasing_completions_early() -> Self {
    Self::with_defects(JournalDefects {
      early_completions: true,
      ..JournalDefects::default()
    })
  }

  /// A subject that leaves the second group out of every barrier, finishing the first group's half
  /// and abandoning the other's.
  #[must_use]
  pub fn stalling_one_group() -> Self {
    Self::with_defects(JournalDefects {
      stall_group: Some(2),
      ..JournalDefects::default()
    })
  }

  /// A subject whose journal keeps every structural field and zeroes the self-describing ones.
  #[must_use]
  pub fn stripping_fields() -> Self {
    Self::with_defects(JournalDefects {
      strip_fields: true,
      ..JournalDefects::default()
    })
  }

  /// A subject whose recovery keeps the operations decoded before a refused one, instead of
  /// voiding the whole record.
  #[must_use]
  pub fn replaying_partial_records() -> Self {
    Self::with_defects(JournalDefects {
      partial_records: true,
      ..JournalDefects::default()
    })
  }

  /// A subject whose journal drops entry payloads and snapshot blobs, keeping only their shape.
  #[must_use]
  pub fn losing_payloads() -> Self {
    Self::with_defects(JournalDefects {
      lossy_payloads: true,
      ..JournalDefects::default()
    })
  }

  /// A subject whose recovery discards EVERY barrier on finding a torn tail.
  #[must_use]
  pub fn discarding_on_tear() -> Self {
    Self::with_defects(JournalDefects {
      recovery: JournalRecovery::EmptyOnTear,
      ..JournalDefects::default()
    })
  }

  /// A subject that defers its journal write until something polls a completion, so `flush`
  /// returns having released work no crash would find.
  #[must_use]
  pub fn persisting_at_poll() -> Self {
    Self::with_defects(JournalDefects {
      persistence: JournalPersistence::AtPoll,
      ..JournalDefects::default()
    })
  }

  /// The filesystem behind the journal, for a test that wants to look at the medium.
  #[must_use]
  pub fn vfs(&self) -> &crate::fault::SharedVfs {
    &self.vfs
  }

  fn device(&self) -> crate::fault::VfsDevice {
    if self.honours_sync {
      self.vfs.open(WAL_PATH)
    } else {
      self.vfs.open_never_syncing(WAL_PATH)
    }
  }
}

/// The one write-ahead log per engine instance.
const WAL_PATH: &str = "wal";

impl crate::check::EngineSubject for JournalEngineSubject {
  type Group = u64;
  type NodeId = u64;
  type Engine = JournalEngine<crate::fault::VfsDevice>;

  fn durability(&self) -> crate::check::Durability {
    crate::check::Durability::Durable
  }

  fn open(&mut self) -> Self::Engine {
    self.vfs = crate::fault::SharedVfs::new();
    let mut engine = JournalEngine::open_with(self.device(), self.defects);
    self.opens += 1;
    if self.defects.alternate_record_sizes && self.opens.is_multiple_of(2) {
      engine.write_padding_record();
    }
    engine
  }

  fn crash(&mut self, engine: Self::Engine, class: crate::fault::CrashClass) -> Self::Engine {
    // No teardown: the engine goes away with whatever it had not yet journalled.
    drop(engine);
    self.vfs.crash(class);
    let mut reopened = JournalEngine::open_with(self.device(), self.defects);
    self.opens += 1;
    if self.defects.alternate_record_sizes && self.opens.is_multiple_of(2) {
      reopened.write_padding_record();
    }
    let after_a_real_torn_cut = matches!(
      class,
      crate::fault::CrashClass::TornTail { keep_bytes } if keep_bytes != u64::MAX
    );
    reopened.sink.borrow_mut().after_a_real_torn_cut = after_a_real_torn_cut;
    if (self.defects.unreadable_after_a_real_torn_cut
      || self.defects.empty_reads_after_a_real_torn_cut
      || self.defects.orphaned_range_after_a_real_torn_cut)
      && after_a_real_torn_cut
    {
      // THE DEFECT, both halves: the reopen CLAIMS the group — `contains_group` says yes and
      // `stores` lends the pair — and the log behind it answers every read with a fault.
      reopened.insert_group(1);
      // The group it resurrects gets a counter above anything the dead incarnation issued, so the
      // ONLY thing wrong with this reopen is the log nobody can read.
      reopened.boot_epochs.insert(1, u64::MAX / 2);
    }
    if self.defects.roll_back_epochs_after_a_torn_tail
      && matches!(class, crate::fault::CrashClass::TornTail { .. })
    {
      // THE DEFECT: one crash class forgets the counter. Every other reopen is exact.
      for epoch in reopened.boot_epochs.values_mut() {
        *epoch = 0;
      }
    }
    if self.defects.roll_back_epochs_by_one {
      // THE DEFECT: one step back, so the reopen reissues the incarnation's LAST epoch while still
      // clearing the first one it handed out.
      for epoch in reopened.boot_epochs.values_mut() {
        *epoch = epoch.saturating_sub(1);
      }
    }
    if self.defects.drop_a_group_on_a_no_op_cut
      && matches!(
        class,
        crate::fault::CrashClass::TornTail {
          keep_bytes: u64::MAX
        }
      )
    {
      // THE DEFECT: the one cut that removes nothing from the medium removes a group anyway.
      reopened.groups.remove(&1);
    }
    reopened
  }

  fn group(&self, n: u64) -> u64 {
    n
  }

  fn node(&self, n: u64) -> u64 {
    n
  }

  fn tail_len(&self) -> Option<u64> {
    if self.boundary_after_the_first_ask.replace(false) {
      // Asked before there was a medium to measure: this subject says it does not know yet.
      self.boundary_after_the_first_ask.set(false);
      return None;
    }
    self.reports_boundary.then(|| self.vfs.tail_durable_len())
  }
}

#[cfg(test)]
mod tests;
